import csv
import threading
import time
from datetime import datetime
from typing import Dict, List, Set, Optional
from config import Config
from utils.logger import setup_logger, log_error
from core.ibkr_client import IBKRClient
from core.data_store import DataStore
from ibapi.contract import Contract, ContractDetails

class WatchlistService:
    """Service for managing options watchlist using IBKR option chains"""
    
    def __init__(self, ibkr_client: IBKRClient, data_store: DataStore):
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
        
        # Contract and option chain data
        self.symbol_contracts = {}  # symbol -> Contract with conId
        self.option_chains = {}     # symbol -> option chain data
        self.option_contracts = {}  # symbol -> {strike_expiry_right: Contract}
        
        # Subscription tracking
        self.subscribed_symbols = set()
        self.option_subscriptions = set()
        
        # Request tracking for callbacks
        self.contract_detail_requests = {}  # req_id -> symbol
        self.option_param_requests = {}     # req_id -> symbol
        self.next_req_id = 5000
        
        # Update timing
        self.last_watchlist_update = datetime.min
        self.last_stock_update = datetime.min
        
        # Setup additional callbacks
        self._setup_contract_callbacks()
        
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
            # Wait for connection and load watchlist
            time.sleep(1)
            self._load_watchlist()
            
            # Get contract details for all symbols first
            self._request_contract_details()
            
            # Wait for contract details to be received
            time.sleep(3)
            
            # Request option parameters for symbols with contract IDs
            self._request_option_parameters()
            
            # Wait for option chains to be received
            time.sleep(5)
            
            # Subscribe to stock data
            self._subscribe_to_stocks()
            
            while self.running:
                try:
                    current_time = datetime.now()
                    
                    # Update stock data periodically
                    if (current_time - self.last_stock_update).total_seconds() >= 1:
                        self._request_stock_data()
                        self.last_stock_update = current_time
                    
                    # Update watchlist options data
                    if (current_time - self.last_watchlist_update).total_seconds() >= 1:
                        self._update_watchlist_options()
                        self.last_watchlist_update = current_time
                    
                    # Update data store
                    self._update_watchlist_store()
                    
                    time.sleep(1)  # Main loop interval
                    
                except Exception as e:
                    log_error(self.logger, e, "Error in watchlist service loop")
                    time.sleep(1)
                    
        except Exception as e:
            log_error(self.logger, e, "Fatal error in watchlist service")
    
    def _setup_callbacks(self):
        """Setup IBKR market data callbacks"""
        self.ibkr_client.register_market_data_callback(self._on_market_data_update)
    
    def _setup_contract_callbacks(self):
        """Setup contract detail and option parameter callbacks"""
        # Add contract detail callback
        self.ibkr_client.wrapper.contractDetails = self._on_contract_details
        self.ibkr_client.wrapper.contractDetailsEnd = self._on_contract_details_end
        
        # Add option parameters callback
        self.ibkr_client.wrapper.securityDefinitionOptionParameter = self._on_security_definition_option_parameter
        self.ibkr_client.wrapper.securityDefinitionOptionParameterEnd = self._on_security_definition_option_parameter_end
    
    def _load_watchlist(self):
        """Load watchlist symbols from CSV"""
        try:
            with open(Config.WATCHLIST_FILE, 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    symbol = row['symbol'].strip().upper()
                    enabled = row['enabled'].strip().lower() == 'true'
                    
                    if enabled:
                        self.watchlist_symbols.append(symbol)
            
            self.logger.info(f"Loaded {len(self.watchlist_symbols)} symbols from watchlist: {self.watchlist_symbols}")
            
        except Exception as e:
            log_error(self.logger, e, "Error loading watchlist")
            # Fallback to default symbols
            self.watchlist_symbols = ['AAPL', 'MSFT', 'TSLA', 'GOOG']
            self.logger.info(f"Using fallback symbols: {self.watchlist_symbols}")
    
    def _request_contract_details(self):
        """Request contract details to get contract IDs for symbols"""
        try:
            if not self.ibkr_client.is_connected():
                self.logger.warning("IBKR not connected, cannot request contract details")
                return
            
            for symbol in self.watchlist_symbols:
                req_id = self._get_next_req_id()
                self.contract_detail_requests[req_id] = symbol
                
                # Create stock contract for contract details request
                contract = Contract()
                contract.symbol = symbol
                contract.secType = "STK"
                contract.currency = "USD"
                contract.exchange = "SMART"
                
                self.ibkr_client.reqContractDetails(req_id, contract)
                self.logger.debug(f"Requested contract details for {symbol} (req_id: {req_id})")
                
                time.sleep(0.5)  # Rate limiting
                
        except Exception as e:
            log_error(self.logger, e, "Error requesting contract details")
    
    def _request_option_parameters(self):
        """Request option parameters for symbols with contract IDs"""
        try:
            if not self.ibkr_client.is_connected():
                self.logger.warning("IBKR not connected, cannot request option parameters")
                return
            
            for symbol, contract in self.symbol_contracts.items():
                if hasattr(contract, 'conId') and contract.conId:
                    req_id = self._get_next_req_id()
                    self.option_param_requests[req_id] = symbol
                    
                    self.ibkr_client.reqSecDefOptParams(
                        req_id,
                        contract.symbol,
                        "",  # futFopExchange
                        contract.secType,
                        contract.conId
                    )
                    
                    self.logger.info(f"Requested option parameters for {symbol} (conId: {contract.conId}, req_id: {req_id})")
                    time.sleep(0.5)  # Rate limiting
                else:
                    self.logger.warning(f"No contract ID available for {symbol}")
                    
        except Exception as e:
            log_error(self.logger, e, "Error requesting option parameters")
    
    def _subscribe_to_stocks(self):
        """Subscribe to stock market data"""
        try:
            for symbol in self.watchlist_symbols:
                if symbol not in self.subscribed_symbols and symbol in self.symbol_contracts:
                    contract = self.symbol_contracts[symbol]
                    req_id = self.ibkr_client.request_market_data(symbol, contract, snapshot=False)
                    
                    if req_id != -1:
                        self.subscribed_symbols.add(symbol)
                        self.stock_data[symbol] = {}
                        self.logger.debug(f"Subscribed to stock data for {symbol}")
                        
                    time.sleep(0.5)  # Rate limiting
                    
        except Exception as e:
            log_error(self.logger, e, "Error subscribing to stocks")
    
    def _request_stock_data(self):
        """Request fresh stock data"""
        try:
            for symbol in self.watchlist_symbols:
                if self.ibkr_client.is_connected() and symbol in self.symbol_contracts:
                    contract = self.symbol_contracts[symbol]
                    self.ibkr_client.request_market_data(symbol, contract, snapshot=True)
                    time.sleep(0.3)
                    
        except Exception as e:
            log_error(self.logger, e, "Error requesting stock data")
    
    def _update_watchlist_options(self):
        """Update options data for watchlist symbols"""
        try:
            for symbol in self.watchlist_symbols:
                stock_price = self.stock_data.get(symbol, {}).get('last_price', 0)
                if symbol in self.option_chains:
                    # Get ATM options from IBKR option chain data
                    call_option, put_option = self._get_atm_options_from_chain(symbol, stock_price)
                    
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
                        
                        self.logger.debug(f"Updated watchlist data for {symbol}")
                    
                    time.sleep(1)  # Rate limiting
                    
        except Exception as e:
            log_error(self.logger, e, "Error updating watchlist options")
    
    def _get_atm_options_from_chain(self, symbol: str, stock_price: float) -> tuple:
        """Get ATM call and put options from IBKR option chain"""
        try:
            chain_data = self.option_chains.get(symbol, {})
            if not chain_data:
                return None, None
            
            # Get strikes and find closest to current price
            strikes = chain_data.get('strikes', [])
            if not strikes:
                return None, None
            
            # Find ATM strike (closest to stock price)
            atm_strike = min(strikes, key=lambda x: abs(x - stock_price))
            
            # Get farthest expiry
            expiries = chain_data.get('expirations', [])
            if not expiries:
                return None, None
            
            # Sort expiries and get the farthest one
            expiries_sorted = sorted(expiries)
            farthest_expiry = expiries_sorted[-1] if expiries_sorted else None
            
            if not farthest_expiry:
                return None, None
            
            # Get call and put option data
            call_option = self._get_option_data(symbol, atm_strike, farthest_expiry, 'C')
            put_option = self._get_option_data(symbol, atm_strike, farthest_expiry, 'P')
            
            self.logger.debug(f"Selected ATM options for {symbol}: Strike={atm_strike}, Expiry={farthest_expiry}")
            
            return call_option, put_option
            
        except Exception as e:
            log_error(self.logger, e, f"Error getting ATM options from chain for {symbol}")
            return None, None
    
    def _get_option_data(self, symbol: str, strike: float, expiry: str, right: str) -> Optional[Dict]:
        """Get option data for specific contract"""
        try:
            option_key = f"{symbol}_{strike}_{expiry}_{right}"
            
            # Create option contract using the base contract
            if symbol not in self.symbol_contracts:
                return None
            
            base_contract = self.symbol_contracts[symbol]
            
            option_contract = Contract()
            option_contract.symbol = symbol
            option_contract.secType = "OPT"
            option_contract.currency = "USD"
            option_contract.exchange = "SMART"
            option_contract.lastTradeDateOrContractMonth = expiry
            option_contract.strike = strike
            option_contract.right = right
            option_contract.multiplier = "100"
            
            # Store the option contract
            if symbol not in self.option_contracts:
                self.option_contracts[symbol] = {}
            self.option_contracts[symbol][f"{strike}_{expiry}_{right}"] = option_contract
            
            # Request option data if not already subscribed
            if option_key not in self.option_subscriptions:
                req_id = self.ibkr_client.request_market_data(option_key, option_contract, snapshot=True)
                if req_id != -1:
                    self.option_subscriptions.add(option_key)
                    self.logger.debug(f"Subscribed to option data: {option_key}")
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
                'bid': 0,
                'ask': 0,
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
            log_error(self.logger, e, f"Error getting option data for {symbol} {strike} {expiry} {right}")
            return None
    
    # IBKR Callback Methods
    def _on_contract_details(self, req_id: int, contract_details: ContractDetails):
        """Handle contract details response"""
        try:
            symbol = self.contract_detail_requests.get(req_id)
            if symbol:
                contract = contract_details.contract
                self.symbol_contracts[symbol] = contract
                self.logger.info(f"Received contract details for {symbol}: conId={contract.conId}")
            
        except Exception as e:
            log_error(self.logger, e, f"Error handling contract details for req_id {req_id}")
    
    def _on_contract_details_end(self, req_id: int):
        """Handle end of contract details"""
        symbol = self.contract_detail_requests.get(req_id)
        if symbol:
            self.logger.debug(f"Contract details completed for {symbol}")
            del self.contract_detail_requests[req_id]
    
    def _on_security_definition_option_parameter(self, req_id: int, exchange: str, 
                                                underlying_con_id: int, trading_class: str,
                                                multiplier: str, expirations: set, strikes: set):
        """Handle option parameters response"""
        try:
            symbol = self.option_param_requests.get(req_id)
            if symbol:
                if symbol not in self.option_chains:
                    self.option_chains[symbol] = {
                        'expirations': set(),
                        'strikes': set(),
                        'exchanges': set(),
                        'multipliers': set()
                    }
                
                # Accumulate data from multiple exchanges
                self.option_chains[symbol]['expirations'].update(expirations)
                self.option_chains[symbol]['strikes'].update(strikes)
                self.option_chains[symbol]['exchanges'].add(exchange)
                self.option_chains[symbol]['multipliers'].add(multiplier)
                
                self.logger.debug(f"Received option params for {symbol} on {exchange}: "
                                f"{len(expirations)} expiries, {len(strikes)} strikes")
            
        except Exception as e:
            log_error(self.logger, e, f"Error handling option parameters for req_id {req_id}")
    
    def _on_security_definition_option_parameter_end(self, req_id: int):
        """Handle end of option parameters"""
        try:
            symbol = self.option_param_requests.get(req_id)
            if symbol:
                chain_data = self.option_chains.get(symbol, {})
                
                # Convert sets to sorted lists for easier processing
                if 'expirations' in chain_data:
                    chain_data['expirations'] = sorted(list(chain_data['expirations']))
                if 'strikes' in chain_data:
                    chain_data['strikes'] = sorted(list(chain_data['strikes']))
                
                self.logger.info(f"Option chain completed for {symbol}: "
                               f"{len(chain_data.get('expirations', []))} expiries, "
                               f"{len(chain_data.get('strikes', []))} strikes")
                
                del self.option_param_requests[req_id]
            
        except Exception as e:
            log_error(self.logger, e, f"Error handling option parameters end for req_id {req_id}")
    
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
                                option['bid'] = round(tick_data.get('bid', 0), 2)
                                option['ask'] = round(tick_data.get('ask', 0), 2)
                                
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
    
    def _get_next_req_id(self) -> int:
        """Get next request ID"""
        req_id = self.next_req_id
        self.next_req_id += 1
        return req_id
    
    # Public Methods
    def get_watchlist_data(self) -> Dict:
        """Get current watchlist data"""
        return self.watchlist_data.copy()
    
    def get_option_chains(self) -> Dict:
        """Get option chain data"""
        return self.option_chains.copy()
    
    def add_symbol(self, symbol: str):
        """Add symbol to watchlist"""
        try:
            symbol = symbol.upper()
            if symbol not in self.watchlist_symbols:
                self.watchlist_symbols.append(symbol)
                
                # Request contract details first
                req_id = self._get_next_req_id()
                self.contract_detail_requests[req_id] = symbol
                
                contract = Contract()
                contract.symbol = symbol
                contract.secType = "STK"
                contract.currency = "USD"
                contract.exchange = "SMART"
                
                self.ibkr_client.reqContractDetails(req_id, contract)
                
                self.logger.info(f"Added {symbol} to watchlist")
                
        except Exception as e:
            log_error(self.logger, e, f"Error adding {symbol} to watchlist")
    
    def remove_symbol(self, symbol: str):
        """Remove symbol from watchlist"""
        try:
            symbol = symbol.upper()
            if symbol in self.watchlist_symbols:
                self.watchlist_symbols.remove(symbol)
                self.subscribed_symbols.discard(symbol)
                self.stock_data.pop(symbol, None)
                self.watchlist_data.pop(symbol, None)
                self.symbol_contracts.pop(symbol, None)
                self.option_chains.pop(symbol, None)
                self.option_contracts.pop(symbol, None)
                
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
            'contracts_count': len(self.symbol_contracts),
            'option_chains_count': len(self.option_chains),
            'subscribed_symbols': len(self.subscribed_symbols),
            'option_subscriptions': len(self.option_subscriptions),
            'watchlist_data_count': len(self.watchlist_data),
            'last_update': self.last_watchlist_update.isoformat(),
            'running': self.is_running()
        }