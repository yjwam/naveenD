import signal
import sys
import time
import threading
from datetime import datetime
from config import Config
from utils.logger import setup_logger, log_error
from core.data_store import DataStore
from core.ibkr_client import IBKRClient
from core.websocket_server import WebSocketManager
from services.position_service import PositionService
from services.market_service import MarketService
from services.watchlist_service import WatchlistService

class QuantumTraderSimple:
    """Main application class"""
    
    def __init__(self):
        self.logger = setup_logger('main')
        
        # Initialize core components
        self.data_store = DataStore()
        self.ibkr_client = IBKRClient()
        self.websocket_manager = WebSocketManager(self.data_store)
        
        # Initialize services
        self.position_service = PositionService(self.ibkr_client, self.data_store)
        self.market_service = MarketService(self.ibkr_client, self.data_store)
        self.watchlist_service = WatchlistService(self.ibkr_client, self.data_store)
        
        # Application state
        self.running = False
        self.start_time = datetime.now()
        
        # Connection monitoring
        self.connection_monitor_thread = None
        self.last_connection_status = False
        
        self.logger.info("QuantumTrader Simple initialized")
    
    def start(self) -> bool:
        """Start the application"""
        try:
            self.logger.info("Starting QuantumTrader Simple...")
            
            # Connect to IBKR
            self.logger.info("Connecting to IBKR...")
            if not self._connect_to_ibkr():
                self.logger.error("Failed to connect to IBKR")
                return False
            
            # Setup IBKR callbacks
            self._setup_ibkr_callbacks()
            
            # Start services
            self.logger.info("Starting services...")
            self._start_services()
            
            # Start WebSocket server
            self.logger.info("Starting WebSocket server...")
            self.websocket_manager.start()
            
            # Start connection monitoring
            self._start_connection_monitoring()
            
            # Wait for services to initialize
            time.sleep(0.2)
            
            self.running = True
            self.logger.info("QuantumTrader Simple started successfully!")
            
            # Print status
            self._print_startup_status()
            
            return True
            
        except Exception as e:
            log_error(self.logger, e, "Failed to start application")
            return False
    
    def stop(self):
        """Stop the application"""
        self.logger.info("Stopping QuantumTrader Simple...")
        self.running = False
        
        try:
            # Stop services
            self.watchlist_service.stop()
            self.market_service.stop()
            self.position_service.stop()
            
            # Stop WebSocket server
            self.websocket_manager.stop()
            
            # Stop connection monitoring
            if self.connection_monitor_thread and self.connection_monitor_thread.is_alive():
                self.connection_monitor_thread.join(timeout=5)
            
            # Disconnect from IBKR
            self.ibkr_client.disconnect_and_stop()
            
            self.logger.info("QuantumTrader Simple stopped successfully")
            
        except Exception as e:
            log_error(self.logger, e, "Error during shutdown")
    
    def _connect_to_ibkr(self) -> bool:
        """Connect to IBKR with retry logic"""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            self.logger.info(f"IBKR connection attempt {attempt}/{max_attempts}")
            
            if self.ibkr_client.connect_and_run():
                # Verify connection stability
                time.sleep(0.1)
                if self.ibkr_client.is_connected():
                    self.logger.info("IBKR connection established and stable")
                    self.data_store.set_connection_status(True)
                    return True
                else:
                    self.logger.warning("IBKR connection unstable")
            
            if attempt < max_attempts:
                wait_time = attempt * 5
                self.logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
        
        return False
    
    def _start_services(self):
        """Start all services"""
        # Start services with delays
        self.position_service.start()
        time.sleep(0.1)
        
        self.market_service.start()
        time.sleep(0.1)
        
        self.watchlist_service.start()
        
        self.logger.info("All services started")
    
    def _start_connection_monitoring(self):
        """Start connection health monitoring"""
        self.connection_monitor_thread = threading.Thread(
            target=self._monitor_connection_health, 
            daemon=True
        )
        self.connection_monitor_thread.start()
        self.logger.info("Connection monitoring started")
    
    def _monitor_connection_health(self):
        """Monitor IBKR connection health"""
        while self.running:
            try:
                current_status = self.ibkr_client.is_connected()
                
                # Log status changes
                if current_status != self.last_connection_status:
                    if current_status:
                        self.logger.info("✅ IBKR connection restored")
                        self.data_store.set_connection_status(True)
                    else:
                        self.logger.warning("❌ IBKR connection lost")
                        self.data_store.set_connection_status(False)
                
                self.last_connection_status = current_status
                
                # Sleep between checks
                time.sleep(30)
                
            except Exception as e:
                log_error(self.logger, e, "Error in connection monitoring")
                time.sleep(60)
    
    def _setup_ibkr_callbacks(self):
        """Setup callbacks between IBKR client and services"""
        
        def on_connection_status(data: dict):
            status = data['status']
            if status == 'ready':
                self.data_store.set_connection_status(True)
                self.logger.info("IBKR connection ready")
            elif status in ['disconnected', 'error']:
                self.data_store.set_connection_status(False)
                self.logger.warning(f"IBKR connection status: {status}")
        
        # Register connection callback
        self.ibkr_client.register_connection_callback(on_connection_status)
        
        self.logger.info("IBKR callbacks configured")
    
    def _print_startup_status(self):
        """Print startup status information"""
        print("\n" + "="*60)
        print("🚀 QUANTUMTRADER SIMPLE - BACKEND STARTED")
        print("="*60)
        
        # Connection status
        ibkr_status = "✅ Connected" if self.ibkr_client.is_connected() else "❌ Disconnected"
        print(f"📡 IBKR Connection: {ibkr_status}")
        
        print(f"🌐 WebSocket Server: ws://{Config.WEBSOCKET_HOST}:{Config.WEBSOCKET_PORT}")
        print(f"📊 Update Interval: {Config.UPDATE_INTERVAL}s")
        print(f"💼 Position Tracking: Active")
        print(f"📈 Market Data: Active")
        print(f"👁️ Watchlist: Active")
        print("="*60)
        print("🔗 Frontend Connection:")
        print(f"   Connect to: ws://localhost:{Config.WEBSOCKET_PORT}")
        print("="*60)
        print("📝 Services:")
        print(f"   - Position Service: {'✅' if self.position_service.is_running() else '❌'}")
        print(f"   - Market Service: {'✅' if self.market_service.is_running() else '❌'}")
        print(f"   - Watchlist Service: {'✅' if self.watchlist_service.is_running() else '❌'}")
        print("="*60)
        print("🎯 Available Data:")
        print("   - All positions with real-time P&L")
        print("   - ETF prices (SPY, QQQ, VIX, etc.)")
        print("   - Options watchlist with Greeks")
        print("   - Portfolio summary")
        print("="*60)
        print("Use Ctrl+C to stop gracefully")
        print()
    
    def get_status(self) -> dict:
        """Get current application status"""
        uptime = (datetime.now() - self.start_time).total_seconds()
        
        return {
            'running': self.running,
            'uptime_seconds': uptime,
            'start_time': self.start_time.isoformat(),
            'ibkr_connected': self.ibkr_client.is_connected(),
            'websocket_stats': self.websocket_manager.get_stats(),
            'data_summary': {
                'positions_count': len(self.data_store.get_positions()),
                'etfs_count': len(self.data_store.get_etfs()),
                'watchlist_count': len(self.data_store.get_watchlist()),
                'last_update': self.data_store.last_update.isoformat()
            },
            'services': {
                'position': self.position_service.is_running(),
                'market': self.market_service.is_running(),
                'watchlist': self.watchlist_service.is_running()
            }
        }
    
    def run_forever(self):
        """Run the application indefinitely"""
        try:
            while self.running:
                time.sleep(0.1)
                
                # Periodic status logging (every 5 minutes)
                if int(time.time()) % 300 == 0:
                    self._log_periodic_status()
                
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal")
        except Exception as e:
            log_error(self.logger, e, "Unexpected error in main loop")
        finally:
            self.stop()
    
    def _log_periodic_status(self):
        """Log periodic status information"""
        try:
            status = self.get_status()
            uptime_minutes = status['uptime_seconds'] / 60
            
            self.logger.info(f"Status - Uptime: {uptime_minutes:.1f}m, "
                           f"IBKR: {'✅' if status['ibkr_connected'] else '❌'}, "
                           f"Positions: {status['data_summary']['positions_count']}, "
                           f"Clients: {status['websocket_stats'].get('server_stats', {}).get('connected_clients', 0)}")
            
        except Exception as e:
            log_error(self.logger, e, "Error logging periodic status")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger = setup_logger('signal_handler')
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    sys.exit(0)

def main():
    """Main entry point"""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Print startup banner
    print("\n" + "="*60)
    print("🚀 QUANTUMTRADER SIMPLE - INITIALIZING")
    print("="*60)
    print("📋 Configuration:")
    print(f"   IBKR: {Config.IBKR_HOST}:{Config.IBKR_PORT} (Client ID: {Config.IBKR_CLIENT_ID})")
    print(f"   WebSocket: {Config.WEBSOCKET_HOST}:{Config.WEBSOCKET_PORT}")
    print(f"   Update Intervals: {Config.UPDATE_INTERVAL}s / {Config.MARKET_DATA_INTERVAL}s")
    print(f"   Log Level: {Config.LOG_LEVEL}")
    print("="*60)
    
    # Create and start application
    app = QuantumTraderSimple()
    
    if app.start():
        try:
            app.run_forever()
        except KeyboardInterrupt:
            print("\n⚠️  Keyboard interrupt received")
        finally:
            print("\n🛑 Shutting down...")
            app.stop()
            print("✅ Shutdown complete")
    else:
        print("❌ Failed to start QuantumTrader Simple")
        print("\n🔍 Troubleshooting Tips:")
        print("1. Ensure TWS/IB Gateway is running and API is enabled")
        print("2. Check that the correct port is configured (.env file)")
        print("3. Verify client ID is not already in use")
        print("4. Check TWS/Gateway logs for connection issues")
        sys.exit(1)

if __name__ == "__main__":
    main()