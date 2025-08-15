import csv
import threading
import time
import yfinance as yf
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional
from config import Config
from utils.logger import setup_logger, log_error
from core.ibkr_client import IBKRClient
from core.data_store import DataStore
from ibapi.contract import Contract, ContractDetails

class WatchlistService:
    """Service for managing options watchlist using yfinance for stock prices and IBKR for options"""
    
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
        
        # Fixed option selections (calculated once, then only price updates)
        self.fixed_option_selections = {}  # symbol -> {strike, expiry, call_contract, put_contract}
        
        # Subscription tracking
        self.option_subscriptions = set()
        
        # Request tracking for callbacks
        self.contract_detail_requests = {}  # req_id -> symbol
        self.option_param_requests = {}     # req_id -> symbol
        self.next_req_id = 5000
        
        # Update timing
        self.last_stock_update = datetime.min
        self.last_option_update = datetime.min
        
        # yfinance tickers
        self.yf_tickers = {}
        
        # Setup additional callbacks
        self._setup_contract_callbacks()
        
        self.logger.info("Watchlist service initialized with yfinance for stock prices")
    
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
            
            # Setup yfinance tickers
            self._setup_yfinance_tickers()
            
            # Get contract details for all symbols first
            self._request_contract_details()
            
            # Wait for contract details to be received
            time.sleep(3)
            
            # Request option parameters for symbols with contract IDs
            self._request_option_parameters()
            
            # Wait for option chains to be received
            time.sleep(5)
            
            self._update_stock_data_yfinance()

            # Calculate fixed option selections (ATM strike + farthest expiry)
            self._calculate_fixed_option_selections()
            
            # Subscribe to option data for fixed selections
            self._subscribe_to_fixed_options()
            
            while self.running:
                try:
                    current_time = datetime.now()
                    
                    # Update stock data from yfinance periodically (every 10 seconds)
                    if (current_time - self.last_stock_update).total_seconds() >= 10:
                        self._update_stock_data_yfinance()
                        self.last_stock_update = current_time
                    
                    # Update option data (every 5 seconds)
                    if (current_time - self.last_option_update).total_seconds() >= 5:
                        self._request_option_data_updates()
                        self.last_option_update = current_time
                    
                    # Update data store
                    self._update_watchlist_store()
                    
                    time.sleep(2)  # Main loop interval
                    
                except Exception as e:
                    log_error(self.logger, e, "Error in watchlist service loop")
                    time.sleep(2)
                    
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
    
    def _setup_yfinance_tickers(self):
        """Setup yfinance ticker objects"""
        try:
            for symbol in self.watchlist_symbols:
                self.yf_tickers[symbol] = yf.Ticker(symbol)
                self.stock_data[symbol] = {}
            
            self.logger.info(f"Setup yfinance tickers for {len(self.yf_tickers)} symbols")
            
        except Exception as e:
            log_error(self.logger, e, "Error setting up yfinance tickers")
    
    def _update_stock_data_yfinance(self):
        """Update stock data using yfinance"""
        try:
            for symbol in self.watchlist_symbols:
                if symbol in self.yf_tickers:
                    ticker = self.yf_tickers[symbol]
                    
                    # Get current price and basic info
                    try:
                        info = ticker.info
                        hist = ticker.history(period="2d", interval="1m")
                        
                        if not hist.empty and info:
                            current_price = hist['Close'].iloc[-1]
                            previous_close = info.get('previousClose', current_price)
                            
                            # Calculate change
                            change = current_price - previous_close
                            change_pct = (change / previous_close) * 100 if previous_close > 0 else 0
                            
                            # Get additional data
                            volume = hist['Volume'].iloc[-1] if not hist.empty else 0
                            high = hist['High'].max() if not hist.empty else current_price
                            low = hist['Low'].min() if not hist.empty else current_price
                            self.logger.info(f"YF: Updating stock data for {symbol}: ${current_price:.2f} (Change: {change_pct:+.2f}%)")
                            self.stock_data[symbol] = {
                                'last_price': round(float(current_price), 2),
                                'previous_close': round(float(previous_close), 2),
                                'change': round(float(change), 2),
                                'change_pct': round(float(change_pct), 2),
                                'volume': int(volume),
                                'high': round(float(high), 2),
                                'low': round(float(low), 2),
                                'last_update': datetime.now().isoformat()
                            }
                            
                            self.logger.info(f"Updated stock data for {symbol}: ${current_price:.2f} ({change_pct:+.2f}%)")
                            
                    except Exception as e:
                        self.logger.warning(f"Failed to get yfinance data for {symbol}: {e}")
                        
        except Exception as e:
            log_error(self.logger, e, "Error updating stock data from yfinance")
    
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
    
    def _calculate_fixed_option_selections(self):
        """Calculate fixed option selections (ATM strike + farthest expiry) - done once"""
        try:
            for symbol in self.watchlist_symbols:
                if symbol in self.option_chains and symbol in self.stock_data:
                    stock_price = self.stock_data[symbol].get('last_price', 0)
                    if stock_price > 0:
                        chain_data = self.option_chains[symbol]
                        strikes = chain_data.get('strikes', [])
                        expiries = chain_data.get('expirations', [])
                        
                        if strikes and expiries:
                            # Find ATM strike (closest to current stock price)
                            atm_strike = min(strikes, key=lambda x: abs(x - stock_price))

                            selected_expiry = sorted(expiries)[-1]
                            
                            # Create option contracts
                            call_contract = self._create_option_contract(symbol, atm_strike, selected_expiry, 'C')
                            put_contract = self._create_option_contract(symbol, atm_strike, selected_expiry, 'P')

                            self.fixed_option_selections[symbol] = {
                                'strike': atm_strike,
                                'expiry': selected_expiry,
                                'call_contract': call_contract,
                                'put_contract': put_contract,
                                'selected_at': datetime.now().isoformat(),
                                'stock_price_at_selection': stock_price
                            }
                            
                            self.logger.info(f"Fixed option selection for {symbol}: "
                                            f"Strike=${atm_strike}, Expiry={selected_expiry}, "
                                            f"Stock=${stock_price:.2f}")
            
        except Exception as e:
            log_error(self.logger, e, "Error calculating fixed option selections")
    
    def _create_option_contract(self, symbol: str, strike: float, expiry: str, right: str) -> Contract:
        """Create option contract"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "OPT"
        contract.currency = "USD"
        contract.exchange = "SMART"
        contract.lastTradeDateOrContractMonth = expiry
        contract.strike = strike
        contract.right = right
        return contract
    
    def _subscribe_to_fixed_options(self):
        """Subscribe to option data for fixed selections"""
        try:
            for symbol, selection in self.fixed_option_selections.items():
                call_contract = selection['call_contract']
                put_contract = selection['put_contract']
                strike = selection['strike']
                expiry = selection['expiry']
                
                # Subscribe to call option
                call_key = f"{symbol}_{strike}_{expiry}_C"
                if call_key not in self.option_subscriptions:
                    req_id = self.ibkr_client.request_option_market_data(call_key, call_contract, snapshot=False)
                    if req_id != -1:
                        self.option_subscriptions.add(call_key)
                        self.logger.debug(f"Subscribed to call option: {call_key}")
                        time.sleep(0.5)
                
                # Subscribe to put option
                put_key = f"{symbol}_{strike}_{expiry}_P"
                if put_key not in self.option_subscriptions:
                    req_id = self.ibkr_client.request_option_market_data(put_key, put_contract, snapshot=False)
                    if req_id != -1:
                        self.option_subscriptions.add(put_key)
                        self.logger.debug(f"Subscribed to put option: {put_key}")
                        time.sleep(0.5)
                        
        except Exception as e:
            log_error(self.logger, e, "Error subscribing to fixed options")
    
    def _request_option_data_updates(self):
        """Request fresh option data for fixed selections"""
        try:
            for symbol, selection in self.fixed_option_selections.items():
                if self.ibkr_client.is_connected():
                    call_contract = selection['call_contract']
                    put_contract = selection['put_contract']
                    strike = selection['strike']
                    expiry = selection['expiry']
                    
                    # Request call option data
                    call_key = f"{symbol}_{strike}_{expiry}_C"
                    self.ibkr_client.request_option_market_data(call_key, call_contract, snapshot=True)
                    time.sleep(0.3)
                    
                    # Request put option data
                    put_key = f"{symbol}_{strike}_{expiry}_P"
                    self.ibkr_client.request_option_market_data(put_key, put_contract, snapshot=True)
                    time.sleep(0.3)
                    
        except Exception as e:
            log_error(self.logger, e, "Error requesting option data updates")
    
    def _update_watchlist_store(self):
        """Update watchlist data in data store"""
        try:
            updated_watchlist = {}
            
            for symbol in self.watchlist_symbols:
                stock_data = self.stock_data.get(symbol, {})
                selection = self.fixed_option_selections.get(symbol, {})
                
                if stock_data and selection:
                    # Get option data from current watchlist_data (updated by market data callbacks)
                    existing_data = self.watchlist_data.get(symbol, {})
                    options_data = existing_data.get('options', {})
                    
                    updated_watchlist[symbol] = {
                        'stock_price': stock_data.get('last_price', 0),
                        'stock_change': stock_data.get('change', 0),
                        'stock_change_pct': stock_data.get('change_pct', 0),
                        'volume': stock_data.get('volume', 0),
                        'high': stock_data.get('high', 0),
                        'low': stock_data.get('low', 0),
                        'previous_close': stock_data.get('previous_close', 0),
                        'options': {
                            'call': options_data.get('call', self._create_empty_option_data(selection, 'C')),
                            'put': options_data.get('put', self._create_empty_option_data(selection, 'P'))
                        },
                        'fixed_selection': {
                            'strike': selection.get('strike', 0),
                            'expiry': selection.get('expiry', ''),
                            'selected_at': selection.get('selected_at', ''),
                            'stock_price_at_selection': selection.get('stock_price_at_selection', 0)
                        },
                        'last_update': datetime.now().isoformat()
                    }
            
            if updated_watchlist:
                self.data_store.update_watchlist(updated_watchlist)
                
        except Exception as e:
            log_error(self.logger, e, "Error updating watchlist store")
    
    def _create_empty_option_data(self, selection: Dict, right: str) -> Dict:
        """Create empty option data structure"""
        return {
            'strike': selection.get('strike', 0),
            'expiry': selection.get('expiry', ''),
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
        """Handle market data update from IBKR (only for options)"""
        try:
            symbol_key = data.get('symbol', '')
            tick_data = data.get('data', {})
            
            # Handle Greeks data
            if data.get('type') == 'greeks':
                self._process_option_greeks(data)
                return
            
            # Handle option data (ignore stock data since we use yfinance)
            if '_' in symbol_key and any(ws in symbol_key for ws in self.watchlist_symbols):
                self._process_option_data(symbol_key, tick_data)
                
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
                    strike = float(parts[1])
                    expiry = parts[2]
                    right = parts[3]
                    
                    if base_symbol in self.watchlist_data:
                        option_type = 'call' if right == 'C' else 'put'
                        
                        if base_symbol not in self.watchlist_data:
                            self.watchlist_data[base_symbol] = {'options': {}}
                        if 'options' not in self.watchlist_data[base_symbol]:
                            self.watchlist_data[base_symbol]['options'] = {}
                        if option_type not in self.watchlist_data[base_symbol]['options']:
                            self.watchlist_data[base_symbol]['options'][option_type] = {}
                        
                        self.watchlist_data[base_symbol]['options'][option_type]['greeks'] = {
                            'delta': round(greeks_data.get('delta', 0), 4),
                            'gamma': round(greeks_data.get('gamma', 0), 4),
                            'theta': round(greeks_data.get('theta', 0), 4),
                            'vega': round(greeks_data.get('vega', 0), 4),
                            'iv': round(greeks_data.get('implied_vol', 0), 4)
                        }
                        
        except Exception as e:
            log_error(self.logger, e, "Error processing option Greeks")
    
    def _process_option_data(self, option_key: str, tick_data: Dict):
        """Process option market data"""
        try:
            parts = option_key.split('_')
            if len(parts) >= 4:
                base_symbol = parts[0]
                strike = float(parts[1])
                expiry = parts[2]
                right = parts[3]
                
                option_type = 'call' if right == 'C' else 'put'
                
                if base_symbol not in self.watchlist_data:
                    self.watchlist_data[base_symbol] = {'options': {}}
                if 'options' not in self.watchlist_data[base_symbol]:
                    self.watchlist_data[base_symbol]['options'] = {}
                if option_type not in self.watchlist_data[base_symbol]['options']:
                    self.watchlist_data[base_symbol]['options'][option_type] = {
                        'strike': strike,
                        'expiry': expiry,
                        'right': right,
                        'greeks': {}
                    }
                
                option_data = self.watchlist_data[base_symbol]['options'][option_type]
                
                # Update option price data
                old_price = option_data.get('price', 0)
                new_price = tick_data.get('last_price', 0)
                
                if new_price > 0:
                    option_data['price'] = round(new_price, 2)
                    option_data['volume'] = tick_data.get('volume', 0)
                    option_data['bid'] = round(tick_data.get('bid', 0), 2)
                    option_data['ask'] = round(tick_data.get('ask', 0), 2)
                    
                    # Calculate change
                    if old_price > 0:
                        change = new_price - old_price
                        change_pct = (change / old_price) * 100
                        option_data['change'] = round(change, 2)
                        option_data['change_pct'] = round(change_pct, 2)
                    
                    option_data['last_update'] = datetime.now().isoformat()
                    
                    self.logger.debug(f"Updated option data for {option_key}: ${new_price:.2f}")
                            
        except Exception as e:
            log_error(self.logger, e, f"Error processing option data for {option_key}")
    
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
    
    def get_fixed_selections(self) -> Dict:
        """Get fixed option selections"""
        return self.fixed_option_selections.copy()
    
    def recalculate_option_selections(self, symbol: str = None):
        """Recalculate option selections for symbol(s) - use sparingly"""
        try:
            symbols_to_update = [symbol] if symbol else self.watchlist_symbols
            
            for sym in symbols_to_update:
                if sym in self.option_chains and sym in self.stock_data:
                    # Cancel existing subscriptions for this symbol
                    for option_key in list(self.option_subscriptions):
                        if option_key.startswith(f"{sym}_"):
                            self.option_subscriptions.discard(option_key)
                    
                    # Remove old selection
                    self.fixed_option_selections.pop(sym, None)
                    
            # Recalculate selections
            self._calculate_fixed_option_selections()
            
            # Re-subscribe to new selections
            self._subscribe_to_fixed_options()
            
            self.logger.info(f"Recalculated option selections for: {symbols_to_update}")
            
        except Exception as e:
            log_error(self.logger, e, f"Error recalculating option selections")
    
    def add_symbol(self, symbol: str):
        """Add symbol to watchlist"""
        try:
            symbol = symbol.upper()
            if symbol not in self.watchlist_symbols:
                self.watchlist_symbols.append(symbol)
                
                # Setup yfinance ticker
                self.yf_tickers[symbol] = yf.Ticker(symbol)
                self.stock_data[symbol] = {}
                
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
                
                # Clean up data
                self.stock_data.pop(symbol, None)
                self.watchlist_data.pop(symbol, None)
                self.symbol_contracts.pop(symbol, None)
                self.option_chains.pop(symbol, None)
                self.fixed_option_selections.pop(symbol, None)
                self.yf_tickers.pop(symbol, None)
                
                # Remove option subscriptions
                for option_key in list(self.option_subscriptions):
                    if option_key.startswith(f"{symbol}_"):
                        self.option_subscriptions.discard(option_key)
                
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
            'fixed_selections_count': len(self.fixed_option_selections),
            'option_subscriptions': len(self.option_subscriptions),
            'yf_tickers_count': len(self.yf_tickers),
            'watchlist_data_count': len(self.watchlist_data),
            'last_stock_update': self.last_stock_update.isoformat(),
            'last_option_update': self.last_option_update.isoformat(),
            'running': self.is_running()
        }