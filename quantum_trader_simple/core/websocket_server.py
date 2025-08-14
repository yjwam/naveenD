import asyncio
import json
import websockets
import threading
from datetime import datetime
from typing import Set, Dict, Any
from websockets.server import WebSocketServerProtocol
from config import Config
from utils.logger import setup_logger, log_error
from core.data_store import DataStore

class WebSocketServer:
    """WebSocket server for real-time data streaming"""
    
    def __init__(self, data_store:DataStore):
        self.data_store = data_store
        self.logger = setup_logger('websocket_server')
        
        # Connected clients
        self.clients: Set[WebSocketServerProtocol] = set()
        self.client_info: Dict[WebSocketServerProtocol, Dict] = {}
        
        # Server state
        self.server = None
        self.running = False
        self.broadcast_task = None
        
        self.logger.info("WebSocket server initialized")
    
    async def register_client(self, websocket: WebSocketServerProtocol, path: str):
        """Handle new client connection"""
        try:
            self.clients.add(websocket)
            self.client_info[websocket] = {
                'connected_at': datetime.now(),
                'path': path,
                'remote_address': websocket.remote_address
            }
            
            client_count = len(self.clients)
            self.logger.info(f"Client connected from {websocket.remote_address}. Total clients: {client_count}")
            
            # Send initial data snapshot
            await self.send_snapshot(websocket)
            
            # Handle client messages
            await self.handle_client_messages(websocket)
            
        except websockets.exceptions.ConnectionClosed:
            self.logger.info(f"Client {websocket.remote_address} disconnected")
        except Exception as e:
            log_error(self.logger, e, f"Error handling client {websocket.remote_address}")
        finally:
            await self.unregister_client(websocket)
    
    async def unregister_client(self, websocket: WebSocketServerProtocol):
        """Remove client connection"""
        if websocket in self.clients:
            self.clients.remove(websocket)
        
        if websocket in self.client_info:
            del self.client_info[websocket]
        
        client_count = len(self.clients)
        self.logger.info(f"Client disconnected. Total clients: {client_count}")
    
    async def send_snapshot(self, websocket: WebSocketServerProtocol):
        """Send initial data snapshot to client"""
        try:
            snapshot = self.data_store.get_snapshot()
            message = {
                'type': 'snapshot',
                'data': snapshot,
                'timestamp': datetime.now().isoformat()
            }
            
            await websocket.send(json.dumps(message))
            self.logger.debug(f"Sent snapshot to {websocket.remote_address}")
            
        except Exception as e:
            log_error(self.logger, e, "Error sending snapshot")
    
    async def handle_client_messages(self, websocket: WebSocketServerProtocol):
        """Handle incoming messages from client"""
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self.process_client_message(websocket, data)
                except json.JSONDecodeError:
                    await self.send_error(websocket, "Invalid JSON format")
                except Exception as e:
                    log_error(self.logger, e, "Error processing client message")
                    await self.send_error(websocket, "Error processing message")
        except websockets.exceptions.ConnectionClosed:
            pass
    
    async def process_client_message(self, websocket: WebSocketServerProtocol, data: Dict[str, Any]):
        """Process specific client message types"""
        message_type = data.get('type')
        
        if message_type == 'ping':
            await websocket.send(json.dumps({
                'type': 'pong',
                'timestamp': datetime.now().isoformat()
            }))
        
        elif message_type == 'get_snapshot':
            await self.send_snapshot(websocket)
        
        elif message_type == 'subscribe':
            symbols = data.get('symbols', [])
            await self.handle_subscription(websocket, symbols)
        
        else:
            await self.send_error(websocket, f"Unknown message type: {message_type}")
    
    async def handle_subscription(self, websocket: WebSocketServerProtocol, symbols: list):
        """Handle symbol subscription requests"""
        response = {
            'type': 'subscription_confirmed',
            'symbols': symbols,
            'timestamp': datetime.now().isoformat()
        }
        await websocket.send(json.dumps(response))
    
    async def send_error(self, websocket: WebSocketServerProtocol, error_message: str):
        """Send error message to client"""
        try:
            error_response = {
                'type': 'error',
                'message': error_message,
                'timestamp': datetime.now().isoformat()
            }
            await websocket.send(json.dumps(error_response))
        except Exception as e:
            log_error(self.logger, e, "Error sending error message")
    
    async def broadcast_update(self, update_data: Dict[str, Any]):
        """Broadcast update to all clients"""
        if not self.clients:
            return
        
        message = {
            'type': 'update',
            'data': update_data,
            'timestamp': datetime.now().isoformat()
        }
        
        await self.broadcast_message(message)
    
    async def broadcast_snapshot(self):
        """Broadcast full snapshot to all clients"""
        if not self.clients:
            return
        
        try:
            snapshot = self.data_store.get_snapshot()
            message = {
                'type': 'snapshot',
                'data': snapshot,
                'timestamp': datetime.now().isoformat()
            }
            
            await self.broadcast_message(message)
            
        except Exception as e:
            log_error(self.logger, e, "Error broadcasting snapshot")
    
    async def broadcast_message(self, message: Dict[str, Any]):
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
                log_error(self.logger, e, f"Error sending message to client")
                disconnected_clients.append(client)
        
        # Clean up disconnected clients
        for client in disconnected_clients:
            await self.unregister_client(client)
    
    async def periodic_broadcast(self):
        """Periodic broadcast of data snapshots"""
        while self.running:
            try:
                await self.broadcast_snapshot()
                await asyncio.sleep(Config.UPDATE_INTERVAL)
            except Exception as e:
                log_error(self.logger, e, "Error in periodic broadcast")
                await asyncio.sleep(1)
    
    async def start_server(self):
        """Start the WebSocket server"""
        try:
            self.running = True
            
            self.server = await websockets.serve(
                self.register_client,
                Config.WEBSOCKET_HOST,
                Config.WEBSOCKET_PORT,
                ping_interval=20,
                ping_timeout=10,
                max_size=1024*1024
            )
            
            self.logger.info(f"WebSocket server started on {Config.WEBSOCKET_HOST}:{Config.WEBSOCKET_PORT}")
            
            # Start periodic broadcast task
            self.broadcast_task = asyncio.create_task(self.periodic_broadcast())
            
            # Keep server running
            await self.server.wait_closed()
            
        except Exception as e:
            log_error(self.logger, e, "Error starting WebSocket server")
            raise
    
    async def stop_server(self):
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
    
    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics"""
        return {
            'connected_clients': len(self.clients),
            'running': self.running,
            'clients_info': [
                {
                    'address': str(info['remote_address']),
                    'connected_at': info['connected_at'].isoformat(),
                    'path': info['path']
                }
                for info in self.client_info.values()
            ]
        }

class WebSocketManager:
    """Manager for WebSocket server with threading support"""
    
    def __init__(self, data_store):
        self.data_store = data_store
        self.websocket_server = WebSocketServer(data_store)
        self.logger = setup_logger('websocket_manager')
        
        self.server_thread = None
        self.loop = None
        self.running = False
    
    def start(self):
        """Start WebSocket server in separate thread"""
        if self.running:
            self.logger.warning("WebSocket server already running")
            return
        
        self.running = True
        self.server_thread = threading.Thread(target=self._run_server, daemon=True)
        self.server_thread.start()
        
        self.logger.info("WebSocket manager started")
    
    def stop(self):
        """Stop WebSocket server"""
        if not self.running:
            return
        
        self.running = False
        
        if self.loop and not self.loop.is_closed():
            asyncio.run_coroutine_threadsafe(
                self.websocket_server.stop_server(),
                self.loop
            )
        
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5)
        
        self.logger.info("WebSocket manager stopped")
    
    def _run_server(self):
        """Run WebSocket server in event loop"""
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            self.loop.run_until_complete(self.websocket_server.start_server())
            
        except Exception as e:
            log_error(self.logger, e, "WebSocket server error")
        finally:
            if self.loop and not self.loop.is_closed():
                self.loop.close()
    
    def broadcast_update(self, update_type: str, data: Dict[str, Any]):
        """Broadcast update to all clients (thread-safe)"""
        if not self.running or not self.loop:
            return
        
        try:
            update_message = {
                'type': update_type,
                'data': data
            }
            
            asyncio.run_coroutine_threadsafe(
                self.websocket_server.broadcast_update(update_message),
                self.loop
            )
            
        except Exception as e:
            log_error(self.logger, e, "Error broadcasting update")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get WebSocket server statistics"""
        if not self.running:
            return {'status': 'stopped'}
        
        return {
            'status': 'running',
            'server_stats': self.websocket_server.get_stats()
        }