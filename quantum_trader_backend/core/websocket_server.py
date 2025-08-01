import asyncio
import json
import websockets
import threading
from datetime import datetime
from typing import Set, Dict, Any, Optional
from websockets.server import WebSocketServerProtocol

from config.settings import settings
from utils.logger import get_logger
from core.data_manager import DataManager
from models.dashboard import StreamingUpdate

class WebSocketServer:
    """WebSocket server for real-time data streaming"""
    
    def __init__(self, data_manager: DataManager):
        self.data_manager = data_manager
        self.logger = get_logger("websocket_server")
        
        # Connected clients
        self.clients: Set[WebSocketServerProtocol] = set()
        self.client_info: Dict[WebSocketServerProtocol, Dict] = {}
        
        # Server state
        self.server = None
        self.running = False
        self.broadcast_task = None
        
        # Update frequencies
        self.update_interval = settings.data.update_frequency
        
        self.logger.info("WebSocket server initialized")
    
    async def connection_handler(self, websocket, path):
        """Handle WebSocket connection with proper signature"""
        return await self.register_client(websocket, path)
    
    async def register_client(self, websocket: WebSocketServerProtocol, path: str) -> None:
        """Register a new client connection"""
        try:
            self.clients.add(websocket)
            self.client_info[websocket] = {
                'connected_at': datetime.now(),
                'path': path,
                'remote_address': websocket.remote_address,
                'last_ping': datetime.now()
            }
            
            client_count = len(self.clients)
            self.logger.info(f"Client connected from {websocket.remote_address}. Total clients: {client_count}")
            
            # Send initial dashboard data
            await self.send_initial_data(websocket)
            
            # Handle client messages
            await self.handle_client_messages(websocket)
            
        except websockets.exceptions.ConnectionClosed:
            self.logger.info(f"Client {websocket.remote_address} disconnected")
        except Exception as e:
            self.logger.error(f"Error handling client {websocket.remote_address}: {e}")
        finally:
            await self.unregister_client(websocket)
    
    async def unregister_client(self, websocket: WebSocketServerProtocol) -> None:
        """Unregister a client connection"""
        if websocket in self.clients:
            self.clients.remove(websocket)
            
        if websocket in self.client_info:
            del self.client_info[websocket]
        
        client_count = len(self.clients)
        self.logger.info(f"Client disconnected. Total clients: {client_count}")
    
    async def send_initial_data(self, websocket: WebSocketServerProtocol) -> None:
        """Send initial dashboard data to newly connected client"""
        try:
            dashboard_data = self.data_manager.get_dashboard_data()
            
            message = {
                'type': 'initial_data',
                'data': dashboard_data.to_dict(),
                'timestamp': datetime.now().isoformat()
            }
            
            await websocket.send(json.dumps(message))
            self.logger.debug(f"Sent initial data to {websocket.remote_address}")
            
        except Exception as e:
            self.logger.error(f"Error sending initial data: {e}")
    
    async def handle_client_messages(self, websocket: WebSocketServerProtocol) -> None:
        """Handle incoming messages from client"""
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self.process_client_message(websocket, data)
                except json.JSONDecodeError:
                    await self.send_error(websocket, "Invalid JSON format")
                except Exception as e:
                    self.logger.error(f"Error processing client message: {e}")
                    await self.send_error(websocket, "Error processing message")
                    
        except websockets.exceptions.ConnectionClosed:
            pass
    
    async def process_client_message(self, websocket: WebSocketServerProtocol, data: Dict[str, Any]) -> None:
        """Process specific client message types"""
        message_type = data.get('type')
        
        if message_type == 'ping':
            # Update last ping time
            if websocket in self.client_info:
                self.client_info[websocket]['last_ping'] = datetime.now()
            
            # Send pong response
            await websocket.send(json.dumps({
                'type': 'pong',
                'timestamp': datetime.now().isoformat()
            }))
            
        elif message_type == 'subscribe':
            # Handle subscription requests
            symbols = data.get('symbols', [])
            await self.handle_subscription(websocket, symbols)
            
        elif message_type == 'get_dashboard':
            # Send current dashboard data
            await self.send_initial_data(websocket)
            
        elif message_type == 'acknowledge_alert':
            # Acknowledge an alert
            alert_id = data.get('alert_id')
            if alert_id:
                await self.acknowledge_alert(alert_id)
        
        else:
            await self.send_error(websocket, f"Unknown message type: {message_type}")
    
    async def handle_subscription(self, websocket: WebSocketServerProtocol, symbols: list) -> None:
        """Handle symbol subscription requests"""
        # For now, all clients get all data
        # In the future, this could be used for selective data streaming
        response = {
            'type': 'subscription_confirmed',
            'symbols': symbols,
            'timestamp': datetime.now().isoformat()
        }
        await websocket.send(json.dumps(response))
    
    async def acknowledge_alert(self, alert_id: str) -> None:
        """Acknowledge an alert"""
        # Find and acknowledge the alert
        for alert in self.data_manager.alerts:
            if alert.id == alert_id:
                alert.acknowledged = True
                self.logger.info(f"Alert {alert_id} acknowledged")
                
                # Broadcast alert acknowledgment
                await self.broadcast_update({
                    'type': 'alert_acknowledged',
                    'alert_id': alert_id,
                    'timestamp': datetime.now().isoformat()
                })
                break
    
    async def send_error(self, websocket: WebSocketServerProtocol, error_message: str) -> None:
        """Send error message to client"""
        try:
            error_response = {
                'type': 'error',
                'message': error_message,
                'timestamp': datetime.now().isoformat()
            }
            await websocket.send(json.dumps(error_response))
        except Exception as e:
            self.logger.error(f"Error sending error message: {e}")
    
    async def broadcast_dashboard_update(self) -> None:
        """Broadcast dashboard update to all connected clients"""
        if not self.clients:
            return
        
        try:
            dashboard_data = self.data_manager.get_dashboard_data()
            
            message = {
                'type': 'dashboard_update',
                'data': dashboard_data.to_dict(),
                'timestamp': datetime.now().isoformat()
            }
            
            await self.broadcast_message(message)
            
        except Exception as e:
            self.logger.error(f"Error broadcasting dashboard update: {e}")
    
    async def broadcast_update(self, update_data: Dict[str, Any]) -> None:
        """Broadcast a specific update to all clients"""
        await self.broadcast_message(update_data)
    
    async def broadcast_message(self, message: Dict[str, Any]) -> None:
        """Broadcast message to all connected clients"""
        if not self.clients:
            return
        
        json_message = json.dumps(message)
        disconnected_clients = []
        
        for client in self.clients.copy():
            try:
                await client.send(json_message)
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.append(client)
            except Exception as e:
                self.logger.error(f"Error sending message to client: {e}")
                disconnected_clients.append(client)
        
        # Clean up disconnected clients
        for client in disconnected_clients:
            await self.unregister_client(client)
    
    async def periodic_broadcast(self) -> None:
        """Periodic broadcast of dashboard updates"""
        while self.running:
            try:
                await self.broadcast_dashboard_update()
                await asyncio.sleep(self.update_interval)
            except Exception as e:
                self.logger.error(f"Error in periodic broadcast: {e}")
                await asyncio.sleep(1)  # Short delay before retrying
    
    async def start_server(self) -> None:
        """Start the WebSocket server"""
        try:
            self.running = True
            
            # Start the WebSocket server
            self.server = await websockets.serve(
                self.connection_handler,
                settings.websocket.host,
                settings.websocket.port,
                ping_interval=settings.websocket.ping_interval,
                ping_timeout=settings.websocket.ping_timeout,
                max_size=1024*1024,  # 1MB max message size
                compression=None  # Disable compression for better performance
            )
            
            self.logger.info(f"WebSocket server started on {settings.websocket.host}:{settings.websocket.port}")
            
            # Start periodic broadcast task
            self.broadcast_task = asyncio.create_task(self.periodic_broadcast())
            
            # Keep server running
            await self.server.wait_closed()
            
        except Exception as e:
            self.logger.error(f"Error starting WebSocket server: {e}")
            raise
    
    async def stop_server(self) -> None:
        """Stop the WebSocket server"""
        self.running = False
        
        # Cancel broadcast task
        if self.broadcast_task:
            self.broadcast_task.cancel()
            try:
                await self.broadcast_task
            except asyncio.CancelledError:
                pass
        
        # Close all client connections
        if self.clients:
            await asyncio.gather(
                *[client.close() for client in self.clients],
                return_exceptions=True
            )
        
        # Close server
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        self.logger.info("WebSocket server stopped")
    
    def get_server_stats(self) -> Dict[str, Any]:
        """Get server statistics"""
        return {
            'connected_clients': len(self.clients),
            'running': self.running,
            'update_interval': self.update_interval,
            'clients_info': [
                {
                    'address': str(info['remote_address']),
                    'connected_at': info['connected_at'].isoformat(),
                    'last_ping': info['last_ping'].isoformat(),
                    'path': info['path']
                }
                for info in self.client_info.values()
            ]
        }

class WebSocketManager:
    """Manager for WebSocket server with threading support"""
    
    def __init__(self, data_manager: DataManager):
        self.data_manager = data_manager
        self.websocket_server = WebSocketServer(data_manager)
        self.logger = get_logger("websocket_manager")
        
        self.server_thread = None
        self.loop = None
        self.running = False
    
    def start(self) -> None:
        """Start WebSocket server in separate thread"""
        if self.running:
            self.logger.warning("WebSocket server already running")
            return
        
        self.running = True
        self.server_thread = threading.Thread(target=self._run_server, daemon=True)
        self.server_thread.start()
        
        self.logger.info("WebSocket manager started")
    
    def stop(self) -> None:
        """Stop WebSocket server"""
        if not self.running:
            return
        
        self.running = False
        
        if self.loop and not self.loop.is_closed():
            # Schedule server stop
            asyncio.run_coroutine_threadsafe(
                self.websocket_server.stop_server(),
                self.loop
            )
        
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5)
        
        self.logger.info("WebSocket manager stopped")
    
    def _run_server(self) -> None:
        """Run WebSocket server in event loop"""
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            self.loop.run_until_complete(self.websocket_server.start_server())
            
        except Exception as e:
            self.logger.error(f"WebSocket server error: {e}")
        finally:
            if self.loop and not self.loop.is_closed():
                self.loop.close()
    
    def broadcast_update(self, update_type: str, data: Dict[str, Any]) -> None:
        """Broadcast update to all clients (thread-safe)"""
        if not self.running or not self.loop:
            return
        
        try:
            update_message = {
                'type': update_type,
                'data': data,
                'timestamp': datetime.now().isoformat()
            }
            
            asyncio.run_coroutine_threadsafe(
                self.websocket_server.broadcast_update(update_message),
                self.loop
            )
            
        except Exception as e:
            self.logger.error(f"Error broadcasting update: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get WebSocket server statistics"""
        if not self.running:
            return {'status': 'stopped'}
        
        return {
            'status': 'running',
            'server_stats': self.websocket_server.get_server_stats()
        }