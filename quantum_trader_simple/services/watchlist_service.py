import csv
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Set
from config import Config
from utils.logger import setup_logger, log_error
from core.ibkr_client import IBKRClient
from core.data_store import DataStore  

class WatchlistService:
    """Service for managing options watchlist"""
    
    def __init__(self, ibkr_client:IBKRClient, data_store:DataStore):
        self.ibkr_client = ibkr_client
        self.data_store = data_store
        self.logger = setup_logger('watchlist_service')
        
        # Service state
        self.running = False
        self.service_thread = None
        
        # Watchlist data
        self.watchlist_symbols = []
        self.watchlist_data = {}
        self.stock_data = {}
        self.option_chains = {}
        
        # Subscription tracking
        self.subscribed_symbols = set()
        self.option_subscriptions = set()
        
        # Update timing
        self.last_watchlist_update = datetime.min
        self.last_stock_update = datetime.min
        
        self.logger.info("Watchlist service initialized")
    
    def start(self):
        """Start the watchlist service"""
        if self.running:
            self.logger.warning("Watchlist service already running")
            return
        
        self.running = True
        self.service_thread = threading.Thread(target=self._run_service, daemon=True)
        self.service_thread.start()
        
        # Setup callbacks
        self._setup_callbacks()
        
        self.logger.info("Watchlist service started")
    
    def stop(self):
        """Stop the watchlist service"""
        if not self.running:
            return
        
        self.running = False
        
        if self.service_thread and self.service_thread.is_alive():
            self.service_thread.join(timeout=5)
        
        self.logger.info("Watchlist service stopped")
    
    def _run_service(self):
        """Main service loop"""
        try:
            # Load watchlist
            time.sleep(0.1)  # Wait for connection
            self._load_watchlist()
            self._subscribe_to_stocks()
            
            while self.running:
                try:
                    current_time = datetime.now()
                    
                    # Update stock data
                    if (current_time - self.last_stock_update).total_seconds() >= 30:
                        self._request_stock_data()
                        self.last_stock_update = current_time
                    
                    # Update watchlist data
                    if (current_time - self.last_watchlist_update).total_seconds() >= 60:
                        self._update_watchlist_options()
                        self.last_watchlist_update = current_time
                    
                    # Update data store
                    self._update_watchlist_store()
                    
                except Exception as e:
                    log_error(self.logger, e, "Error in watchlist service loop")
                    time.sleep(10)
                    
        except Exception as e:
            log_error(self.logger, e, "Fatal error in watchlist service")
    
    def _setup_callbacks(self):
        """Setup IBKR callbacks"""
        self.ibkr_client.register_market_data_callback(self._on_market_data_update)
    
    def _load_watchlist(self):
        """Load watchlist symbols from CSV"""
        try:
            with open(Config.WATCHLIST_FILE, 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    symbol = row['symbol'].strip()
                    enabled = row['enabled'].strip().lower() == 'true'
                    
                    if enabled:
                        self.watchlist_symbols.append(symbol)
            
            self.logger.info(f"Loaded {len(self.watchlist_symbols)} symbols from watchlist: {self.watchlist_symbols}")
            
        except Exception as e:
            log_error(self.logger, e, "Error loading watchlist")
            # Fallback to default symbols
            self.watchlist_symbols = ['AAPL', 'MSFT', 'TSLA', 'GOOG']
    
    def _subscribe_to_stocks(self):
        """Subscribe to stock market data"""
        try:
            for symbol in self.watchlist_symbols:
                if symbol not in self.subscribed_symbols:
                    contract = self.ibkr_client.create_stock_contract(symbol)
                    req_id = self.ibkr_client.request_market_data(symbol, contract, snapshot=True)
                    
                    if req_id != -1:
                        self.subscribed_symbols.add(symbol)
                        self.stock_data[symbol] = {}
                        
                    time.sleep(0.5)  # Rate limiting
                    
        except Exception as e:
            log_error(self.logger, e, "Error subscribing to stocks")
    
    def _request_stock_data(self):
        """Request fresh stock data"""
        try:
            for symbol in self.watchlist_symbols:
                if self.ibkr_client.is_connected():
                    contract = self.ibkr_client.create_stock_contract(symbol)
                    self.ibkr_client.request_market_data(symbol, contract, snapshot=True)
                    time.sleep(0.3)
                    
        except Exception as e:
            log_error(self.logger, e, "Error requesting stock data")
    
    def _update_watchlist_options(self):
        """Update options data for watchlist symbols"""
        try:
            for symbol in self.watchlist_symbols:
                stock_price = self.stock_data.get(symbol, {}).get('last_price', 0)
                
                if stock_price > 0:
                    # Get ATM options
                    call_option, put_option = self._get_atm_options(symbol, stock_price)
                    
                    if call_option or put_option:
                        self.watchlist_data[symbol] = {
                            'stock_price': stock_price,
                            'stock_change': self.stock_data.get(symbol, {}).get('change', 0),
                            'stock_change_pct': self.stock_data.get(symbol, {}).get('change_pct', 0),
                            'options': {
                                'call': call_option,
                                'put': put_option
                            },
                            'last_update': datetime.now().isoformat()
                        }
                    
                    time.sleep(1)  # Rate limiting
                    
        except Exception as e:
            log_error(self.logger, e, "Error updating watchlist options")

    def _get_farthest_expiry(self) -> str:
        """Get farthest expiry date (about 3 months out)"""
        try:
            today = datetime.now().date()
            
            # Find third Friday from now (about 3 months)
            friday_count = 0
            current_date = today
            
            while friday_count < 12:  # Look for 12th Friday (about 3 months)
                current_date += timedelta(days=1)
                if current_date.weekday() == 4:  # Friday
                    friday_count += 1
            
            return current_date.strftime('%Y%m%d')
            
        except Exception as e:
            log_error(self.logger, e, "Error calculating farthest expiry")
            return (datetime.now() + timedelta(days=90)).strftime('%Y%m%d')


    def _get_atm_options(self, symbol: str, stock_price: float) -> tuple:
        """Get ATM call and put options for a symbol"""
        try:
            # Calculate ATM strike (round to nearest 5 or 10)
            if stock_price < 50:
                strike = round(stock_price / 2.5) * 2.5
            elif stock_price < 200:
                strike = round(stock_price / 5) * 5
            else:
                strike = round(stock_price / 10) * 10
            
            # Get nearest expiry (next Friday)
            expiry = self._get_farthest_expiry()
            
            call_option = self._get_option_data(symbol, strike, expiry, 'C')
            put_option = self._get_option_data(symbol, strike, expiry, 'P')
            
            return call_option, put_option
            
        except Exception as e:
            log_error(self.logger, e, f"Error getting ATM options for {symbol}")
            return None, None
    
    def _get_option_data(self, symbol: str, strike: float, expiry: str, right: str) -> Dict:
        """Get option data for specific contract"""
        try:
            option_key = f"{symbol}_{strike}_{expiry}_{right}"
            
            # Create option contract
            contract = self.ibkr_client.create_option_contract(
                symbol=symbol,
                expiry=expiry,
                strike=strike,
                right=right
            )
            
            # Request option data if not already subscribed
            if option_key not in self.option_subscriptions:
                req_id = self.ibkr_client.request_market_data(option_key, contract, snapshot=True)
                if req_id != -1:
                    self.option_subscriptions.add(option_key)
                    time.sleep(0.5)
            
            # Return option data structure (will be populated by market data callback)
            return {
                'strike': strike,
                'expiry': expiry,
                'right': right,
                'price': 0,
                'change': 0,
                'change_pct': 0,
                'volume': 0,
                'greeks': {
                    'delta': 0,
                    'gamma': 0,
                    'theta': 0,
                    'vega': 0,
                    'iv': 0
                },
                'last_update': datetime.now().isoformat()
            }
            
        except Exception as e:
            log_error(self.logger, e, f"Error getting option data for {symbol}")
            return None
    
    def _get_nearest_expiry(self) -> str:
        """Get nearest Friday expiry date"""
        try:
            today = datetime.now().date()
            days_ahead = 4 - today.weekday()  # Friday is 4
            
            if days_ahead <= 0:  # Today is Friday or later
                days_ahead += 7
            
            friday = today + timedelta(days=days_ahead)
            return friday.strftime('%Y%m%d')
            
        except Exception as e:
            log_error(self.logger, e, "Error calculating nearest expiry")
            return (datetime.now() + timedelta(days=7)).strftime('%Y%m%d')
    
    def _on_market_data_update(self, data: Dict):
        """Handle market data update from IBKR"""
        try:
            symbol = data.get('symbol', '')
            tick_data = data.get('data', {})
            
            # Handle Greeks data
            if data.get('type') == 'greeks':
                self._process_option_greeks(data)
                return
            
            # Handle stock data
            if symbol in self.watchlist_symbols:
                if symbol not in self.stock_data:
                    self.stock_data[symbol] = {}
                
                self.stock_data[symbol].update(tick_data)
                self._calculate_stock_changes(symbol)
                
            # Handle option data
            elif '_' in symbol and any(ws in symbol for ws in self.watchlist_symbols):
                self._process_option_data(symbol, tick_data)
                
        except Exception as e:
            log_error(self.logger, e, "Error processing watchlist market data")
    
    def _process_option_greeks(self, data: Dict):
        """Process Greeks data for options"""
        try:
            greeks_data = data.get('data', {})
            symbol_key = greeks_data.get('symbol', '')
            
            if '_' in symbol_key:
                parts = symbol_key.split('_')
                if len(parts) >= 4:
                    base_symbol = parts[0]
                    
                    if base_symbol in self.watchlist_data:
                        # Find matching option and update Greeks
                        watchlist_item = self.watchlist_data[base_symbol]
                        
                        for option_type in ['call', 'put']:
                            option = watchlist_item['options'].get(option_type)
                            if option and self._matches_option(option, parts):
                                option['greeks'] = {
                                    'delta': round(greeks_data.get('delta', 0), 4),
                                    'gamma': round(greeks_data.get('gamma', 0), 4),
                                    'theta': round(greeks_data.get('theta', 0), 4),
                                    'vega': round(greeks_data.get('vega', 0), 4),
                                    'iv': round(greeks_data.get('implied_vol', 0), 4)
                                }
                                break
                                
        except Exception as e:
            log_error(self.logger, e, "Error processing option Greeks")
    
    def _process_option_data(self, option_key: str, tick_data: Dict):
        """Process option market data"""
        try:
            parts = option_key.split('_')
            if len(parts) >= 4:
                base_symbol = parts[0]
                
                if base_symbol in self.watchlist_data:
                    watchlist_item = self.watchlist_data[base_symbol]
                    
                    for option_type in ['call', 'put']:
                        option = watchlist_item['options'].get(option_type)
                        if option and self._matches_option(option, parts):
                            # Update option price data
                            old_price = option.get('price', 0)
                            new_price = tick_data.get('last_price', 0)
                            
                            if new_price > 0:
                                option['price'] = round(new_price, 2)
                                option['volume'] = tick_data.get('volume', 0)
                                
                                # Calculate change
                                if old_price > 0:
                                    change = new_price - old_price
                                    change_pct = (change / old_price) * 100
                                    option['change'] = round(change, 2)
                                    option['change_pct'] = round(change_pct, 2)
                                
                                option['last_update'] = datetime.now().isoformat()
                            break
                            
        except Exception as e:
            log_error(self.logger, e, f"Error processing option data for {option_key}")
    
    def _matches_option(self, option: Dict, key_parts: List[str]) -> bool:
        """Check if option matches key parts"""
        try:
            if len(key_parts) >= 4:
                strike = float(key_parts[1])
                expiry = key_parts[2]
                right = key_parts[3]
                
                return (option.get('strike') == strike and
                        option.get('expiry') == expiry and
                        option.get('right') == right)
        except:
            pass
        return False
    
    def _calculate_stock_changes(self, symbol: str):
        """Calculate stock price changes"""
        try:
            data = self.stock_data.get(symbol, {})
            current_price = data.get('last_price', 0)
            close_price = data.get('close', 0)
            
            if current_price > 0 and close_price > 0:
                change = current_price - close_price
                change_pct = (change / close_price) * 100
                
                data['change'] = round(change, 2)
                data['change_pct'] = round(change_pct, 2)
                
        except Exception as e:
            log_error(self.logger, e, f"Error calculating stock changes for {symbol}")
    
    def _update_watchlist_store(self):
        """Update watchlist data in data store"""
        try:
            if self.watchlist_data:
                self.data_store.update_watchlist(self.watchlist_data)
                
        except Exception as e:
            log_error(self.logger, e, "Error updating watchlist store")
    
    def get_watchlist_data(self) -> Dict:
        """Get current watchlist data"""
        return self.watchlist_data.copy()
    
    def add_symbol(self, symbol: str):
        """Add symbol to watchlist"""
        try:
            if symbol not in self.watchlist_symbols:
                self.watchlist_symbols.append(symbol)
                
                # Subscribe to stock data
                contract = self.ibkr_client.create_stock_contract(symbol)
                req_id = self.ibkr_client.request_market_data(symbol, contract, snapshot=True)
                
                if req_id != -1:
                    self.subscribed_symbols.add(symbol)
                    self.stock_data[symbol] = {}
                
                self.logger.info(f"Added {symbol} to watchlist")
                
        except Exception as e:
            log_error(self.logger, e, f"Error adding {symbol} to watchlist")
    
    def remove_symbol(self, symbol: str):
        """Remove symbol from watchlist"""
        try:
            if symbol in self.watchlist_symbols:
                self.watchlist_symbols.remove(symbol)
                self.subscribed_symbols.discard(symbol)
                self.stock_data.pop(symbol, None)
                self.watchlist_data.pop(symbol, None)
                
                self.logger.info(f"Removed {symbol} from watchlist")
                
        except Exception as e:
            log_error(self.logger, e, f"Error removing {symbol} from watchlist")
    
    def is_running(self) -> bool:
        """Check if service is running"""
        return self.running and (self.service_thread is not None and self.service_thread.is_alive())
    
    def get_watchlist_stats(self) -> Dict:
        """Get watchlist statistics"""
        return {
            'symbols_count': len(self.watchlist_symbols),
            'symbols': self.watchlist_symbols,
            'subscribed_symbols': len(self.subscribed_symbols),
            'option_subscriptions': len(self.option_subscriptions),
            'watchlist_data_count': len(self.watchlist_data),
            'last_update': self.last_watchlist_update.isoformat(),
            'running': self.is_running()
        }