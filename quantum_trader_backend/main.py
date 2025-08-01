#!/usr/bin/env python3
"""
QuantumTrader Elite - Main Application Entry Point
Real-time trading dashboard backend with IBKR integration
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
    """Main application class orchestrating all services"""
    
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
        
        self.logger.info("QuantumTrader Elite initialized")
    
    def start(self) -> bool:
        """Start all application services"""
        try:
            self.logger.info("Starting QuantumTrader Elite...")
            
            # Connect to IBKR
            self.logger.info("Connecting to IBKR...")
            if not self.ibkr_client.connect_and_run():
                self.logger.error("Failed to connect to IBKR")
                return False
            self.ibkr_client.reqMarketDataType(3)
            self.logger.info("IBKR connection established")
            
            # Start data manager callbacks
            self._setup_ibkr_callbacks()
            
            # Start services
            self.logger.info("Starting services...")
            self.market_data_service.start()
            self.portfolio_service.start()
            self.options_service.start()
            self.alerts_service.start()
            
            # Start WebSocket server
            self.logger.info("Starting WebSocket server...")
            self.websocket_manager.start()
            
            # Wait a moment for everything to initialize
            time.sleep(2)
            
            self.running = True
            self.logger.info("QuantumTrader Elite started successfully!")
            
            # Print status
            self._print_startup_status()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start application: {e}")
            return False
    
    def stop(self) -> None:
        """Stop all application services"""
        self.logger.info("Stopping QuantumTrader Elite...")
        self.running = False
        
        try:
            # Stop services
            self.alerts_service.stop()
            self.options_service.stop()
            self.portfolio_service.stop()
            self.market_data_service.stop()
            
            # Stop WebSocket server
            self.websocket_manager.stop()
            
            # Disconnect from IBKR
            self.ibkr_client.disconnect_and_stop()
            
            self.logger.info("QuantumTrader Elite stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
    
    def _setup_ibkr_callbacks(self) -> None:
        """Setup callbacks between IBKR client and data manager"""
        
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
        
        # Connection status callback
        def on_connection_status(data: Dict[str, Any]):
            status = data['status']
            if status == 'connected':
                self.data_manager.system_status.ibkr_connected = True
            elif status in ['disconnected', 'error']:
                self.data_manager.system_status.ibkr_connected = False
        
        # Register callbacks
        self.ibkr_client.register_market_data_callback(on_market_data_update)
        self.ibkr_client.register_market_data_callback(on_greeks_update)
        self.ibkr_client.register_position_callback(on_position_update)
        self.ibkr_client.register_account_callback(on_account_update)
        self.ibkr_client.register_connection_callback(on_connection_status)
        
        self.logger.info("IBKR callbacks configured")
    
    def _print_startup_status(self) -> None:
        """Print startup status information"""
        print("\n" + "="*60)
        print("🚀 QUANTUMTRADER ELITE - BACKEND STARTED")
        print("="*60)
        print(f"📡 IBKR Connection: {'✅ Connected' if self.ibkr_client.is_connected() else '❌ Disconnected'}")
        print(f"🌐 WebSocket Server: ws://{settings.websocket.host}:{settings.websocket.port}")
        print(f"📊 Update Frequency: {settings.data.update_frequency}s")
        print(f"⚡ Market Data: Real-time")
        print(f"💼 Portfolio Tracking: Active")
        print(f"🎯 Options Analytics: Enabled")
        print(f"🚨 Alerts System: Active")
        print("="*60)
        print("🔗 Frontend Connection:")
        print(f"   Connect your frontend to: ws://localhost:{settings.websocket.port}")
        print("="*60)
        print("📝 Logs: Use Ctrl+C to stop")
        print()
    
    def get_status(self) -> Dict[str, Any]:
        """Get current application status"""
        uptime = (datetime.now() - self.start_time).total_seconds()
        
        return {
            'running': self.running,
            'uptime_seconds': uptime,
            'start_time': self.start_time.isoformat(),
            'ibkr_connected': self.ibkr_client.is_connected(),
            'websocket_stats': self.websocket_manager.get_stats(),
            'data_stats': self.data_manager.get_statistics(),
            'services': {
                'market_data': self.market_data_service.is_running(),
                'portfolio': self.portfolio_service.is_running(),
                'options': self.options_service.is_running(),
                'alerts': self.alerts_service.is_running()
            }
        }
    
    def run_forever(self) -> None:
        """Run the application indefinitely"""
        try:
            while self.running:
                time.sleep(1)
                
                # Periodic health checks
                if not self.ibkr_client.is_connected():
                    self.logger.warning("IBKR connection lost, attempting reconnection...")
                    if self.ibkr_client.reconnect():
                        self.logger.info("IBKR reconnection successful")
                    else:
                        self.logger.error("IBKR reconnection failed")
                
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal")
        except Exception as e:
            self.logger.error(f"Unexpected error in main loop: {e}")
        finally:
            self.stop()

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger = get_logger("signal_handler")
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)

def main():
    """Main entry point"""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and start application
    app = QuantumTraderApplication()
    
    if app.start():
        try:
            app.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            app.stop()
    else:
        print("❌ Failed to start QuantumTrader Elite")
        sys.exit(1)

if __name__ == "__main__":
    main()