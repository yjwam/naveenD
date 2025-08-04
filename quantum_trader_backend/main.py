#!/usr/bin/env python3
"""
QuantumTrader Elite - Main Application Entry Point
Real-time trading dashboard backend with IBKR integration - FIXED VERSION
"""

import signal
import sys
import time
import threading
from datetime import datetime
from typing import Dict, Any

from config.settings import settings
from utils.logger import setup_logging, get_logger
from core.ibkr_client import IBKRClient
from core.data_manager import DataManager
from core.websocket_server import WebSocketManager
from services.market_data_service import MarketDataService
from services.portfolio_service import PortfolioService
from services.options_service import OptionsService
from services.alerts_service import AlertsService

class QuantumTraderApplication:
    """Main application class orchestrating all services - FIXED VERSION"""
    
    def __init__(self):
        # Setup logging
        setup_logging(settings.log_level)
        self.logger = get_logger("main")
        
        # Initialize core components
        self.ibkr_client = IBKRClient()
        self.data_manager = DataManager()
        self.websocket_manager = WebSocketManager(self.data_manager)
        
        # Initialize services
        self.market_data_service = MarketDataService(self.ibkr_client, self.data_manager)
        self.portfolio_service = PortfolioService(self.ibkr_client, self.data_manager)
        self.options_service = OptionsService(self.ibkr_client, self.data_manager)
        self.alerts_service = AlertsService(self.data_manager)
        
        # Application state
        self.running = False
        self.start_time = datetime.now()
        
        # Connection status monitoring
        self.connection_monitor_thread = None
        self.last_connection_status = False
        
        self.logger.info("QuantumTrader Elite initialized")
    
    def start(self) -> bool:
        """Start all application services - FIXED"""
        try:
            self.logger.info("Starting QuantumTrader Elite...")
            
            # Connect to IBKR with proper error handling
            self.logger.info("Connecting to IBKR...")
            connection_success = self._connect_to_ibkr_with_retry()
            
            if not connection_success:
                self.logger.error("Failed to establish stable IBKR connection")
                return False
            
            # Setup data manager callbacks
            self._setup_ibkr_callbacks()
            
            # Start services with proper sequencing
            self.logger.info("Starting services...")
            self._start_services_sequentially()
            
            # Start WebSocket server
            self.logger.info("Starting WebSocket server...")
            self.websocket_manager.start()
            
            # Start connection monitoring
            self._start_connection_monitoring()
            
            # Wait for everything to initialize
            time.sleep(3)
            
            self.running = True
            self.logger.info("QuantumTrader Elite started successfully!")
            
            # Print status
            self._print_startup_status()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start application: {e}")
            return False
    
    def _connect_to_ibkr_with_retry(self) -> bool:
        """Connect to IBKR with retry logic - NEW"""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            self.logger.info(f"IBKR connection attempt {attempt}/{max_attempts}")
            
            if self.ibkr_client.connect_and_run():
                # Verify connection is stable
                self.logger.info("Verifying connection stability...")
                time.sleep(5)  # Wait to see if connection holds
                
                if self.ibkr_client.is_connected():
                    self.logger.info("IBKR connection established and stable")
                    return True
                else:
                    self.logger.warning("IBKR connection was established but became unstable")
            
            if attempt < max_attempts:
                wait_time = attempt * 10  # Progressive backoff
                self.logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
        
        return False
    
    def _start_services_sequentially(self) -> None:
        """Start services in proper sequence with delays - NEW"""
        # Start portfolio service first (fundamental data)
        self.portfolio_service.start()
        time.sleep(2)
        
        # Start market data service (rate-limited)
        self.market_data_service.start()
        time.sleep(2)
        
        # Start options service (depends on market data)
        self.options_service.start()
        time.sleep(1)
        
        # Start alerts service last
        self.alerts_service.start()
        
        self.logger.info("All services started sequentially")
    
    def _start_connection_monitoring(self) -> None:
        """Start connection health monitoring - NEW"""
        self.connection_monitor_thread = threading.Thread(
            target=self._monitor_connection_health, 
            daemon=True
        )
        self.connection_monitor_thread.start()
        self.logger.info("Connection monitoring started")
    
    def _monitor_connection_health(self) -> None:
        """Monitor IBKR connection health - NEW"""
        while self.running:
            try:
                current_status = self.ibkr_client.is_connected()
                
                # Log status changes
                if current_status != self.last_connection_status:
                    if current_status:
                        self.logger.info("✅ IBKR connection restored")
                        self.data_manager.system_status.ibkr_connected = True
                    else:
                        self.logger.warning("❌ IBKR connection lost")
                        self.data_manager.system_status.ibkr_connected = False
                        
                        # Add connection lost alert
                        from models.dashboard import AlertType, AlertLevel
                        self.data_manager.add_alert(
                            alert_type=AlertType.SYSTEM,
                            level=AlertLevel.URGENT,
                            title="IBKR Connection Lost",
                            message="Connection to Interactive Brokers has been lost"
                        )
                
                self.last_connection_status = current_status
                
                # Sleep between checks
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in connection monitoring: {e}")
                time.sleep(60)  # Longer sleep on error
    
    def stop(self) -> None:
        """Stop all application services - FIXED"""
        self.logger.info("Stopping QuantumTrader Elite...")
        self.running = False
        
        try:
            # Stop services in reverse order
            self.alerts_service.stop()
            time.sleep(1)
            
            self.options_service.stop()
            time.sleep(1)
            
            self.market_data_service.stop()
            time.sleep(1)
            
            self.portfolio_service.stop()
            time.sleep(1)
            
            # Stop WebSocket server
            self.websocket_manager.stop()
            time.sleep(1)
            
            # Disconnect from IBKR
            self.ibkr_client.disconnect_and_stop()
            
            # Wait for connection monitor to stop
            if self.connection_monitor_thread and self.connection_monitor_thread.is_alive():
                self.connection_monitor_thread.join(timeout=5)
            
            self.logger.info("QuantumTrader Elite stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
    
    def _setup_ibkr_callbacks(self) -> None:
        """Setup callbacks between IBKR client and data manager - FIXED"""
        
        # Market data callbacks
        def on_market_data_update(data: Dict[str, Any]):
            symbol = data['symbol']
            tick_data = data['data']
            self.data_manager.update_market_data(symbol, tick_data)
        
        def on_greeks_update(data: Dict[str, Any]):
            if data.get('type') == 'greeks':
                symbol = data['symbol']
                greeks_data = data['data']
                self.data_manager.update_greeks_data(symbol, greeks_data)
        
        # Portfolio callbacks
        def on_position_update(data: Dict[str, Any]):
            account_id = data['account']
            self.data_manager.update_position(account_id, data)
        
        def on_account_update(data: Dict[str, Any]):
            account_id = data['account']
            key = data['key']
            value = data['value']
            currency = data['currency']
            self.data_manager.update_account_value(account_id, key, value, currency)
        
        # Connection status callback - IMPROVED
        def on_connection_status(data: Dict[str, Any]):
            status = data['status']
            if status == 'ready':  # Connection is fully ready
                self.data_manager.system_status.ibkr_connected = True
                self.logger.info("IBKR connection is ready for requests")
            elif status in ['disconnected', 'error']:
                self.data_manager.system_status.ibkr_connected = False
                self.logger.warning(f"IBKR connection status: {status}")
        
        # Register callbacks
        self.ibkr_client.register_market_data_callback(on_market_data_update)
        self.ibkr_client.register_market_data_callback(on_greeks_update)
        self.ibkr_client.register_position_callback(on_position_update)
        self.ibkr_client.register_account_callback(on_account_update)
        self.ibkr_client.register_connection_callback(on_connection_status)
        
        self.logger.info("IBKR callbacks configured")
    
    def _print_startup_status(self) -> None:
        """Print startup status information - IMPROVED"""
        print("\n" + "="*60)
        print("🚀 QUANTUMTRADER ELITE - BACKEND STARTED")
        print("="*60)
        
        # Connection status with better checking
        ibkr_status = "✅ Connected & Ready" if self.ibkr_client.is_connected() else "❌ Disconnected"
        print(f"📡 IBKR Connection: {ibkr_status}")
        
        print(f"🌐 WebSocket Server: ws://{settings.websocket.host}:{settings.websocket.port}")
        print(f"📊 Update Frequency: {settings.data.update_frequency}s")
        print(f"⚡ Market Data: Snapshot Mode ({settings.data.max_market_data_subscriptions} max)")
        print(f"💼 Portfolio Tracking: Active")
        print(f"🎯 Options Analytics: Enabled")
        print(f"🚨 Alerts System: Active")
        print("="*60)
        print("🔗 Frontend Connection:")
        print(f"   Connect your frontend to: ws://localhost:{settings.websocket.port}")
        print("="*60)
        print("📝 Monitoring:")
        print("   - Connection health: Every 30s")
        print("   - Market data: Conservative limits")
        print("   - Use Ctrl+C to stop gracefully")
        print()
    
    def get_status(self) -> Dict[str, Any]:
        """Get current application status - ENHANCED"""
        uptime = (datetime.now() - self.start_time).total_seconds()
        
        return {
            'running': self.running,
            'uptime_seconds': uptime,
            'start_time': self.start_time.isoformat(),
            'ibkr_connected': self.ibkr_client.is_connected(),
            'ibkr_ready': getattr(self.ibkr_client.wrapper, 'connection_ready', False),
            'websocket_stats': self.websocket_manager.get_stats(),
            'data_stats': self.data_manager.get_statistics(),
            'services': {
                'market_data': self.market_data_service.is_running(),
                'portfolio': self.portfolio_service.is_running(),
                'options': self.options_service.is_running(),
                'alerts': self.alerts_service.is_running()
            },
            'market_data_status': self.market_data_service.get_subscription_status(),
            'last_connection_check': self.last_connection_status
        }
    
    def run_forever(self) -> None:
        """Run the application indefinitely - IMPROVED"""
        try:
            while self.running:
                time.sleep(1)
                
                # Periodic status logging (every 5 minutes)
                if int(time.time()) % 300 == 0:  # Every 5 minutes
                    self._log_periodic_status()
                
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal")
        except Exception as e:
            self.logger.error(f"Unexpected error in main loop: {e}")
        finally:
            self.stop()
    
    def _log_periodic_status(self) -> None:
        """Log periodic status information - NEW"""
        try:
            status = self.get_status()
            uptime_minutes = status['uptime_seconds'] / 60
            
            self.logger.info(f"Status Update - Uptime: {uptime_minutes:.1f}m, "
                           f"IBKR: {'✅' if status['ibkr_connected'] else '❌'}, "
                           f"Clients: {status['websocket_stats'].get('connected_clients', 0)}, "
                           f"Subscriptions: {status['market_data_status'].get('total_subscriptions', 0)}")
            
            # Log any issues
            if not status['ibkr_connected']:
                self.logger.warning("IBKR connection lost - automatic reconnection in progress")
            
            failed_symbols = status['market_data_status'].get('failed_symbols', [])
            if failed_symbols:
                self.logger.info(f"Failed market data subscriptions: {len(failed_symbols)}")
                
        except Exception as e:
            self.logger.error(f"Error logging periodic status: {e}")

def signal_handler(signum, frame):
    """Handle shutdown signals - IMPROVED"""
    logger = get_logger("signal_handler")
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    sys.exit(0)

def main():
    """Main entry point - IMPROVED"""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Print startup banner
    print("\n" + "="*60)
    print("🚀 QUANTUMTRADER ELITE - INITIALIZING")
    print("="*60)
    print("📋 Configuration:")
    print(f"   IBKR Host: {settings.ibkr.host}:{settings.ibkr.port}")
    print(f"   Client ID: {settings.ibkr.client_id}")
    print(f"   WebSocket: {settings.websocket.host}:{settings.websocket.port}")
    print(f"   Max Subscriptions: {settings.data.max_market_data_subscriptions}")
    print(f"   Snapshot Mode: {settings.data.snapshot_mode}")
    print("="*60)
    
    # Create and start application
    app = QuantumTraderApplication()
    
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
        print("❌ Failed to start QuantumTrader Elite")
        print("\n🔍 Troubleshooting Tips:")
        print("1. Ensure TWS/IB Gateway is running and API is enabled")
        print("2. Check that port 7497 (paper) or 7496 (live) is correct")
        print("3. Verify client ID is not already in use")
        print("4. Check TWS/Gateway logs for connection issues")
        sys.exit(1)

if __name__ == "__main__":
    main()