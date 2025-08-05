import threading
import time
from datetime import datetime
from typing import Dict, List, Set
from ibapi.contract import Contract

from config.settings import settings
from utils.logger import get_logger
from core.ibkr_client import IBKRClient
from core.data_manager import DataManager

class MarketDataService:
    """Service for managing real-time market data subscriptions - FIXED VERSION"""
    
    def __init__(self, ibkr_client: IBKRClient, data_manager: DataManager):
        self.ibkr_client = ibkr_client
        self.data_manager = data_manager
        self.logger = get_logger("market_data_service")
        
        # Service state
        self.running = False
        self.service_thread = None
        
        # Subscription management - FIXED
        self.subscribed_symbols: Set[str] = set()
        self.symbol_to_req_id: Dict[str, int] = {}
        self.req_id_to_contract: Dict[int, Contract] = {}
        self.failed_symbols: Set[str] = set()  
        self.subscription_retry_count: Dict[str, int] = {}  # NEW: Track retry attempts
        
        # Market indices to track - REDUCED to avoid subscription issues
        self.market_indices = {
            'SPY': self._create_stock_contract('SPY'),
            'QQQ': self._create_stock_contract('QQQ'),
            # Temporarily disable VIX and TNX to avoid subscription issues
            # 'VIX': self._create_index_contract('VIX'),
            # '^TNX': self._create_index_contract('TNX')
        }
        
        # Connection state tracking
        self.last_connection_check = datetime.min
        self.connection_check_interval = 30  # seconds
        
        # Rate limiting - NEW
        self.last_subscription_time = datetime.min
        self.min_subscription_interval = 2  # seconds between subscriptions
        self.max_subscriptions_per_session = getattr(settings.data, 'max_market_data_subscriptions', 5)
        
        self.logger.info("Market data service initialized")
    
    def start(self) -> None:
        """Start the market data service - FIXED"""
        if self.running:
            self.logger.warning("Market data service already running")
            return
        
        self.running = True
        self.service_thread = threading.Thread(target=self._run_service, daemon=True)
        self.service_thread.start()
        
        self.logger.info("Market data service started")
    
    def stop(self) -> None:
        """Stop the market data service - FIXED"""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel all subscriptions gracefully
        self._cancel_all_subscriptions()
        
        if self.service_thread and self.service_thread.is_alive():
            self.service_thread.join(timeout=10)  # Increased timeout
        
        self.logger.info("Market data service stopped")
    
    def _run_service(self) -> None:
        """Main service loop - FIXED"""
        try:
            # Wait for IBKR connection to be ready
            self._wait_for_connection()
            
            # Setup initial subscriptions with delay
            self._setup_initial_subscriptions()
            
            while self.running:
                try:
                    current_time = datetime.now()
                    
                    # Check connection periodically (not too often)
                    if (current_time - self.last_connection_check).total_seconds() >= self.connection_check_interval:
                        self._check_connection_health()
                        self.last_connection_check = current_time
                    
                    self._check_new_subscriptions()
                    
                    # Sleep longer to reduce CPU usage and avoid overwhelming IBKR
                    time.sleep(0.1)  # 10 seconds instead of 5
                    
                except Exception as e:
                    self.logger.error(f"Error in market data service loop: {e}")
                    time.sleep(3)  # Longer sleep on error
                    
        except Exception as e:
            self.logger.error(f"Fatal error in market data service: {e}")
        finally:
            self._cleanup()
    
    def _wait_for_connection(self) -> None:
        """Wait for IBKR connection to be ready - NEW"""
        max_wait = 60  # Maximum wait time in seconds
        start_time = time.time()
        
        while not self.ibkr_client.is_connected() and (time.time() - start_time) < max_wait:
            self.logger.info("Waiting for IBKR connection to be ready...")
            time.sleep(2)
        
        if not self.ibkr_client.is_connected():
            self.logger.error("IBKR connection not ready after waiting")
        else:
            self.logger.info("IBKR connection is ready for market data requests")
    
    def _setup_initial_subscriptions(self) -> None:
        """Setup initial market data subscriptions - FIXED"""
        self.logger.info("Setting up initial market data subscriptions...")
        
        # Wait a bit more after connection
        time.sleep(0.5)
        
        subscription_count = 0
        
        # Subscribe to market indices with proper delays
        for symbol, contract in self.market_indices.items():
            if not self.running:
                break
                
            if subscription_count >= self.max_subscriptions_per_session:
                self.logger.warning(f"Reached maximum subscriptions limit ({self.max_subscriptions_per_session})")
                break
                
            if self._can_make_subscription():
                req_id = self._request_market_data_safe(symbol, contract)
                if req_id != -1:
                    self.subscribed_symbols.add(symbol)
                    self.symbol_to_req_id[symbol] = req_id
                    self.req_id_to_contract[req_id] = contract
                    subscription_count += 1
                    self.logger.info(f"Subscribed to market data for {symbol}")
                    
                    # Wait between subscriptions
                    time.sleep(0.3)
                else:
                    self.logger.warning(f"Failed to subscribe to {symbol}")
                    self.failed_symbols.add(symbol)
                    self.subscription_retry_count[symbol] = 1
        
        self.logger.info(f"Initial subscriptions completed: {len(self.subscribed_symbols)} successful")
    
    def _can_make_subscription(self) -> bool:
        """Check if we can make a new subscription - NEW"""
        now = datetime.now()
        time_since_last = (now - self.last_subscription_time).total_seconds()
        
        return (time_since_last >= self.min_subscription_interval and 
                len(self.subscribed_symbols) < self.max_subscriptions_per_session and
                self.ibkr_client.is_connected())
    
    def _request_market_data_safe(self, symbol: str, contract: Contract) -> int:
        """Request market data with proper error handling - FIXED"""
        if not self.ibkr_client.is_connected():
            return -1
        
        if symbol in self.failed_symbols:
            retry_count = self.subscription_retry_count.get(symbol, 0)
            if retry_count >= 3:  # Max 3 retries per symbol
                return -1
        
        try:
            # Always use snapshots to avoid subscription fees and connection issues
            req_id = self.ibkr_client.request_market_data(symbol, contract, snapshot=True)
            
            if req_id != -1:
                self.last_subscription_time = datetime.now()
                self.logger.info(f"Market data requested for {symbol}", 
                                req_id=req_id, sec_type=contract.secType)
                return req_id
            else:
                self.failed_symbols.add(symbol)
                self.subscription_retry_count[symbol] = self.subscription_retry_count.get(symbol, 0) + 1
                return -1
                
        except Exception as e:
            self.logger.error(f"Error requesting market data for {symbol}: {e}")
            self.failed_symbols.add(symbol)
            self.subscription_retry_count[symbol] = self.subscription_retry_count.get(symbol, 0) + 1
            return -1
    
    def _check_connection_health(self) -> None:
        """Check connection health and reconnect if needed - NEW"""
        if not self.ibkr_client.is_connected():
            self.logger.warning("IBKR connection lost, clearing subscriptions")
            # Clear all subscription tracking
            self.subscribed_symbols.clear()
            self.symbol_to_req_id.clear()
            self.req_id_to_contract.clear()
    
    def _check_new_subscriptions(self) -> None:
        """Check for new symbols that need market data subscriptions - FIXED"""
        if not self._can_make_subscription():
            return
            
        # Get symbols from current positions (limit to avoid overwhelming)
        position_symbols = set()
        for portfolio in self.data_manager.portfolios.values():
            for position in portfolio.positions:
                if (position.symbol not in self.failed_symbols and 
                    position.symbol not in self.subscribed_symbols):
                    position_symbols.add(position.symbol)
                    if len(position_symbols) >= 2:  # Limit to 2 new symbols per check
                        break
            if len(position_symbols) >= 2:
                break
        
        # Subscribe to new symbols (one at a time)
        for symbol in list(position_symbols):  # Only 1 new subscription per check
            if self.subscribe_to_symbol(symbol):
                break  # Only one per cycle
    
    def subscribe_to_symbol(self, symbol: str) -> bool:
        """Subscribe to market data for a specific symbol - FIXED"""
        if (symbol in self.subscribed_symbols or 
            symbol in self.failed_symbols or
            not self._can_make_subscription()):
            return symbol in self.subscribed_symbols
        
        try:
            # Create contract for the symbol (assume stock for now)
            contract = self._create_stock_contract(symbol)
            
            # Request market data
            req_id = self._request_market_data_safe(symbol, contract)
            
            if req_id != -1:
                self.subscribed_symbols.add(symbol)
                self.symbol_to_req_id[symbol] = req_id
                self.req_id_to_contract[req_id] = contract
                
                self.logger.info(f"Subscribed to market data for {symbol}")
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"Error subscribing to {symbol}: {e}")
            self.failed_symbols.add(symbol)
            return False
    
    def unsubscribe_from_symbol(self, symbol: str) -> bool:
        """Unsubscribe from market data for a specific symbol - FIXED"""
        if symbol not in self.subscribed_symbols:
            return True
        
        try:
            req_id = self.symbol_to_req_id.get(symbol)
            if req_id and self.ibkr_client.isConnected():
                self.ibkr_client.cancel_market_data(req_id)
                
                # Clean up tracking
                self.subscribed_symbols.discard(symbol)
                self.symbol_to_req_id.pop(symbol, None)
                self.req_id_to_contract.pop(req_id, None)
                
                self.logger.info(f"Unsubscribed from market data for {symbol}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error unsubscribing from {symbol}: {e}")
            # Still clean up tracking even if cancel failed
            self.subscribed_symbols.discard(symbol)
            self.symbol_to_req_id.pop(symbol, None)
            return False
    
    def _cancel_all_subscriptions(self) -> None:
        """Cancel all market data subscriptions - FIXED"""
        symbols_to_cancel = list(self.subscribed_symbols)
        for symbol in symbols_to_cancel:
            self.unsubscribe_from_symbol(symbol)
            time.sleep(0.5)  # Small delay between cancellations
    
    def _cleanup(self) -> None:
        """Cleanup service resources - FIXED"""
        try:
            self._cancel_all_subscriptions()
            self.subscribed_symbols.clear()
            self.symbol_to_req_id.clear()
            self.req_id_to_contract.clear()
            self.failed_symbols.clear()
            self.subscription_retry_count.clear()
            
            self.logger.info("Market data service cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def _create_stock_contract(self, symbol: str, exchange: str = "SMART") -> Contract:
        """Create a stock contract"""
        return self.ibkr_client.create_stock_contract(symbol, exchange)
    
    def _create_index_contract(self, symbol: str, exchange: str = "CBOE") -> Contract:
        """Create an index contract"""
        return self.ibkr_client.create_index_contract(symbol, exchange)
    
    def get_subscription_status(self) -> Dict[str, any]:
        """Get current subscription status - FIXED"""
        return {
            'subscribed_symbols': list(self.subscribed_symbols),
            'failed_symbols': list(self.failed_symbols),
            'total_subscriptions': len(self.subscribed_symbols),
            'max_subscriptions': self.max_subscriptions_per_session,
            'running': self.running,
            'connection_ready': self.ibkr_client.is_connected(),
            'market_indices': list(self.market_indices.keys()),
            'retry_counts': dict(self.subscription_retry_count),
            'last_subscription_time': self.last_subscription_time.isoformat() if self.last_subscription_time != datetime.min else None
        }
    
    def is_running(self) -> bool:
        """Check if service is running"""
        return self.running and (self.service_thread is not None and self.service_thread.is_alive())
    
    def force_refresh_symbol(self, symbol: str) -> bool:
        """Force refresh market data for a symbol - FIXED"""
        # Remove from failed list and reset retry count
        self.failed_symbols.discard(symbol)
        self.subscription_retry_count.pop(symbol, None)
        
        if symbol in self.subscribed_symbols:
            self.unsubscribe_from_symbol(symbol)
            time.sleep(0.1)
        
        return self.subscribe_to_symbol(symbol)
    
    def retry_failed_symbols(self) -> int:
        """Retry subscribing to failed symbols - FIXED"""
        if not self.failed_symbols or not self._can_make_subscription():
            return 0
        
        # Only retry symbols that haven't exceeded max retries
        retry_symbols = []
        for symbol in list(self.failed_symbols):
            if self.subscription_retry_count.get(symbol, 0) < 3:
                retry_symbols.append(symbol)
            if len(retry_symbols) >= 2:  # Limit retries per call
                break
        
        if not retry_symbols:
            return 0
        
        successful = 0
        for symbol in retry_symbols:
            # Remove from failed list to allow retry
            self.failed_symbols.discard(symbol)
            
            if self.subscribe_to_symbol(symbol):
                successful += 1
            
            time.sleep(0.2)  # Delay between retries
            
            if not self._can_make_subscription():
                break
        
        self.logger.info(f"Retried {len(retry_symbols)} failed symbols, {successful} successful")
        return successful
    
    def get_market_data_stats(self) -> Dict[str, any]:
        """Get market data statistics - FIXED"""
        total_symbols = len(self.data_manager.market_data)
        recent_updates = 0
        
        # Count recent updates (last 30 seconds)
        cutoff_time = datetime.now().timestamp() - 30
        for market_data in self.data_manager.market_data.values():
            if market_data.timestamp.timestamp() > cutoff_time:
                recent_updates += 1
        
        return {
            'total_symbols_tracked': total_symbols,
            'recent_updates_30s': recent_updates,
            'subscribed_symbols': len(self.subscribed_symbols),
            'failed_symbols': len(self.failed_symbols),
            'max_subscriptions': self.max_subscriptions_per_session,
            'connection_ready': self.ibkr_client.is_connected(),
            'last_update': self.data_manager.last_market_update.isoformat() if self.data_manager.last_market_update else None,
            'service_running': self.is_running(),
            'subscription_retry_counts': dict(self.subscription_retry_count)
        }
    
    def emergency_reset(self) -> None:
        """Emergency reset of all subscriptions - NEW"""
        self.logger.warning("Performing emergency reset of market data service")
        
        # Clear all tracking
        self.subscribed_symbols.clear()
        self.symbol_to_req_id.clear()
        self.req_id_to_contract.clear()
        self.failed_symbols.clear()
        self.subscription_retry_count.clear()
        
        # Reset timing
        self.last_subscription_time = datetime.min
        self.last_connection_check = datetime.min
        
        self.logger.info("Emergency reset completed")