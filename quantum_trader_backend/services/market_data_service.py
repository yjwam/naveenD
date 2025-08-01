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
    """Service for managing real-time market data subscriptions"""
    
    def __init__(self, ibkr_client: IBKRClient, data_manager: DataManager):
        self.ibkr_client = ibkr_client
        self.data_manager = data_manager
        self.logger = get_logger("market_data_service")
        
        # Service state
        self.running = False
        self.service_thread = None
        
        # Subscription management
        self.subscribed_symbols: Set[str] = set()
        self.symbol_to_req_id: Dict[str, int] = {}
        self.req_id_to_contract: Dict[int, Contract] = {}
        self.failed_symbols: Set[str] = set()  # Track symbols that failed subscription
        
        # Market indices to always track - use delayed data to avoid subscription issues
        self.market_indices = {
            'SPY': self._create_stock_contract('SPY'),
            'QQQ': self._create_stock_contract('QQQ'),
            'VIX': self._create_index_contract('VIX'),
            '^TNX': self._create_index_contract('TNX')  # 10-Year Treasury
        }
        
        # Tick lists for different security types - minimal to avoid subscription issues
        self.tick_lists = {
            'STK': "233",      # Just RTVolume for stocks
            'IND': "233",      # Just RTVolume for indices
            'OPT': "233,13",   # RTVolume + Greeks for options
            'CASH': "233",     # Just RTVolume for forex
            'DEFAULT': "233"   # Just RTVolume as fallback
        }
        
        self.logger.info("Market data service initialized")
    
    def start(self) -> None:
        """Start the market data service"""
        if self.running:
            self.logger.warning("Market data service already running")
            return
        
        self.running = True
        self.service_thread = threading.Thread(target=self._run_service, daemon=True)
        self.service_thread.start()
        
        self.logger.info("Market data service started")
    
    def stop(self) -> None:
        """Stop the market data service"""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel all subscriptions
        self._cancel_all_subscriptions()
        
        if self.service_thread and self.service_thread.is_alive():
            self.service_thread.join(timeout=5)
        
        self.logger.info("Market data service stopped")
    
    def _run_service(self) -> None:
        """Main service loop"""
        try:
            # Initial setup
            self._setup_initial_subscriptions()
            
            while self.running:
                try:
                    # Check for new symbols to subscribe to (less frequently to avoid spam)
                    self._check_new_subscriptions()
                    
                    # Health check subscriptions
                    self._health_check_subscriptions()
                    
                    # Sleep between checks (longer to reduce load)
                    time.sleep(5)  # Check every 5 seconds instead of market_data_frequency
                    
                except Exception as e:
                    self.logger.error(f"Error in market data service loop: {e}")
                    time.sleep(10)  # Longer sleep on error
                    
        except Exception as e:
            self.logger.error(f"Fatal error in market data service: {e}")
        finally:
            self._cleanup()
    
    def _setup_initial_subscriptions(self) -> None:
        """Setup initial market data subscriptions"""
        self.logger.info("Setting up initial market data subscriptions...")
        
        # Subscribe to market indices with delay between requests
        for symbol, contract in self.market_indices.items():
            if self.ibkr_client.ensure_connection():
                req_id = self._request_market_data_with_proper_ticks(symbol, contract)
                if req_id != -1:
                    self.subscribed_symbols.add(symbol)
                    self.symbol_to_req_id[symbol] = req_id
                    self.req_id_to_contract[req_id] = contract
                    self.logger.info(f"Subscribed to market data for {symbol}")
                else:
                    self.logger.warning(f"Failed to subscribe to {symbol}")
                    self.failed_symbols.add(symbol)
                
                # Add delay between requests to avoid overwhelming IBKR
                time.sleep(1)
        
        self.logger.info(f"Initial subscriptions completed: {len(self.subscribed_symbols)} successful")
    
    def _request_market_data_with_proper_ticks(self, symbol: str, contract: Contract) -> int:
        """Request market data with appropriate tick list for security type"""
        if not self.ibkr_client.ensure_connection():
            return -1
        
        # Skip if already failed before
        if symbol in self.failed_symbols:
            return -1
        
        req_id = self.ibkr_client.get_next_req_id()
        self.ibkr_client.wrapper.req_id_to_symbol[req_id] = symbol
        self.ibkr_client.wrapper.req_id_to_contract[req_id] = contract
        
        # Get appropriate tick list for security type
        sec_type = getattr(contract, 'secType', 'STK')
        tick_list = self.tick_lists.get(sec_type, self.tick_lists['DEFAULT'])
        
        try:
            # Request market data with appropriate tick list
            self.ibkr_client.reqMktData(req_id, contract, tick_list, False, False, [])
            
            self.logger.info(f"Market data requested for {symbol}", 
                            req_id=req_id, sec_type=sec_type, tick_list=tick_list)
            return req_id
            
        except Exception as e:
            self.logger.error(f"Error requesting market data for {symbol}: {e}")
            self.failed_symbols.add(symbol)
            return -1
    
    def _check_new_subscriptions(self) -> None:
        """Check for new symbols that need market data subscriptions"""
        # Get symbols from current positions
        position_symbols = set()
        for portfolio in self.data_manager.portfolios.values():
            for position in portfolio.positions:
                if position.symbol not in self.failed_symbols:  # Skip known failed symbols
                    position_symbols.add(position.symbol)
        
        # Subscribe to new symbols (limit to avoid overwhelming)
        new_symbols = position_symbols - self.subscribed_symbols
        
        # Only process a few new symbols at a time
        for symbol in list(new_symbols)[:3]:  # Max 3 new subscriptions per check
            if self.subscribe_to_symbol(symbol):
                time.sleep(1)  # Delay between requests
    
    def subscribe_to_symbol(self, symbol: str) -> bool:
        """Subscribe to market data for a specific symbol"""
        if symbol in self.subscribed_symbols or symbol in self.failed_symbols:
            return symbol in self.subscribed_symbols
        
        if not self.ibkr_client.ensure_connection():
            return False
        
        try:
            # Create contract for the symbol
            contract = self._create_stock_contract(symbol)
            
            # Request market data
            req_id = self._request_market_data_with_proper_ticks(symbol, contract)
            
            if req_id != -1:
                self.subscribed_symbols.add(symbol)
                self.symbol_to_req_id[symbol] = req_id
                self.req_id_to_contract[req_id] = contract
                
                self.logger.info(f"Subscribed to market data for {symbol}")
                return True
            else:
                self.failed_symbols.add(symbol)
                self.logger.warning(f"Failed to subscribe to market data for {symbol}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error subscribing to {symbol}: {e}")
            self.failed_symbols.add(symbol)
            return False
    
    def unsubscribe_from_symbol(self, symbol: str) -> bool:
        """Unsubscribe from market data for a specific symbol"""
        if symbol not in self.subscribed_symbols:
            return True
        
        try:
            req_id = self.symbol_to_req_id.get(symbol)
            if req_id:
                self.ibkr_client.cancel_market_data(req_id)
                
                # Clean up tracking
                self.subscribed_symbols.discard(symbol)
                self.symbol_to_req_id.pop(symbol, None)
                self.req_id_to_contract.pop(req_id, None)
                
                self.logger.info(f"Unsubscribed from market data for {symbol}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error unsubscribing from {symbol}: {e}")
            return False
    
    def _health_check_subscriptions(self) -> None:
        """Health check for existing subscriptions"""
        # Don't do aggressive health checks to avoid subscription issues
        # Just log status periodically
        if len(self.subscribed_symbols) > 0:
            self.logger.debug(f"Active subscriptions: {len(self.subscribed_symbols)}, "
                            f"Failed symbols: {len(self.failed_symbols)}")
    
    def _cancel_all_subscriptions(self) -> None:
        """Cancel all market data subscriptions"""
        for symbol in list(self.subscribed_symbols):
            self.unsubscribe_from_symbol(symbol)
    
    def _cleanup(self) -> None:
        """Cleanup service resources"""
        self._cancel_all_subscriptions()
        self.logger.info("Market data service cleanup completed")
    
    def _create_stock_contract(self, symbol: str, exchange: str = "SMART") -> Contract:
        """Create a stock contract"""
        return self.ibkr_client.create_stock_contract(symbol, exchange)
    
    def _create_index_contract(self, symbol: str, exchange: str = "CBOE") -> Contract:
        """Create an index contract"""
        return self.ibkr_client.create_index_contract(symbol, exchange)
    
    def _create_forex_contract(self, base: str, quote: str, exchange: str = "IDEALPRO") -> Contract:
        """Create a forex contract"""
        contract = Contract()
        contract.symbol = base
        contract.secType = "CASH"
        contract.currency = quote
        contract.exchange = exchange
        return contract
    
    def get_subscription_status(self) -> Dict[str, any]:
        """Get current subscription status"""
        return {
            'subscribed_symbols': list(self.subscribed_symbols),
            'failed_symbols': list(self.failed_symbols),
            'total_subscriptions': len(self.subscribed_symbols),
            'running': self.running,
            'market_indices': list(self.market_indices.keys()),
            'symbol_mappings': self.symbol_to_req_id.copy()
        }
    
    def is_running(self) -> bool:
        """Check if service is running"""
        return self.running and (self.service_thread is not None and self.service_thread.is_alive())
    
    def force_refresh_symbol(self, symbol: str) -> bool:
        """Force refresh market data for a symbol"""
        # Remove from failed list to retry
        self.failed_symbols.discard(symbol)
        
        if symbol in self.subscribed_symbols:
            self.unsubscribe_from_symbol(symbol)
            time.sleep(0.5)
        
        return self.subscribe_to_symbol(symbol)
    
    def retry_failed_symbols(self) -> int:
        """Retry subscribing to failed symbols"""
        if not self.failed_symbols:
            return 0
        
        retry_symbols = list(self.failed_symbols)[:5]  # Retry max 5 symbols
        self.failed_symbols.clear()  # Clear failed list
        
        successful = 0
        for symbol in retry_symbols:
            if self.subscribe_to_symbol(symbol):
                successful += 1
            time.sleep(1)
        
        self.logger.info(f"Retried {len(retry_symbols)} failed symbols, {successful} successful")
        return successful
    
    def get_market_data_stats(self) -> Dict[str, any]:
        """Get market data statistics"""
        total_symbols = len(self.data_manager.market_data)
        recent_updates = 0
        
        # Count recent updates (last 10 seconds)
        cutoff_time = datetime.now().timestamp() - 10
        for market_data in self.data_manager.market_data.values():
            if market_data.timestamp.timestamp() > cutoff_time:
                recent_updates += 1
        
        return {
            'total_symbols_tracked': total_symbols,
            'recent_updates_10s': recent_updates,
            'subscribed_symbols': len(self.subscribed_symbols),
            'failed_symbols': len(self.failed_symbols),
            'last_update': self.data_manager.last_market_update.isoformat() if self.data_manager.last_market_update else None,
            'service_running': self.is_running()
        }