import threading
import time
from datetime import datetime
from typing import Dict, List, Set
from config import Config
from utils.logger import setup_logger, log_error
from core.ibkr_client import IBKRClient
from core.data_store import DataStore

class MarketService:
    """Service for managing market data and ETFs"""
    
    def __init__(self, ibkr_client: IBKRClient, data_store: DataStore):
        self.ibkr_client = ibkr_client
        self.data_store = data_store
        self.logger = setup_logger('market_service')
        
        # Service state
        self.running = False
        self.service_thread = None
        
        # Market data tracking
        self.subscribed_symbols = set()
        self.symbol_to_req_id = {}
        self.req_id_to_symbol = {}
        self.market_data_cache = {}
        
        # ETF contracts
        self.etf_contracts = {}
        self.etf_data = {}
        
        # Update timing
        self.last_market_update = datetime.min
        self.last_etf_update = datetime.min
        
        self.logger.info("Market service initialized")
    
    def start(self):
        """Start the market service"""
        if self.running:
            self.logger.warning("Market service already running")
            return
        
        self.running = True
        self.service_thread = threading.Thread(target=self._run_service, daemon=True)
        self.service_thread.start()
        
        # Setup callbacks
        self._setup_callbacks()
        
        self.logger.info("Market service started")
    
    def stop(self):
        """Stop the market service"""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel all subscriptions
        self._cancel_all_subscriptions()
        
        if self.service_thread and self.service_thread.is_alive():
            self.service_thread.join(timeout=5)
        
        self.logger.info("Market service stopped")
    
    def _run_service(self):
        """Main service loop"""
        try:
            # Initial setup
            time.sleep(0.3)  # Wait for connection
            self._setup_etf_contracts()
            self._subscribe_to_etfs()
            
            while self.running:
                try:
                    current_time = datetime.now()
                    
                    # Update ETF data periodically
                    if (current_time - self.last_etf_update).total_seconds() >= Config.MARKET_DATA_INTERVAL:
                        self._request_etf_data()
                        self.last_etf_update = current_time
                    
                    # Subscribe to position symbols
                    self._subscribe_to_position_symbols()
                    time.sleep(Config.MARKET_DATA_INTERVAL//2)
                    
                    # Update market data in data store
                    self._update_market_data_store()
                    
                except Exception as e:
                    log_error(self.logger, e, "Error in market service loop")
                    time.sleep(1)
                    
        except Exception as e:
            log_error(self.logger, e, "Fatal error in market service")
    
    def _setup_callbacks(self):
        """Setup IBKR callbacks"""
        self.ibkr_client.register_market_data_callback(self._on_market_data_update)
    
    def _setup_etf_contracts(self):
        """Setup ETF contracts"""
        etf_mapping = {
                'SPY': ('SPY', 'STK', 'SMART'),
                'QQQ': ('QQQ', 'STK' ,'NASDAQ'), 
                'VIX': ('VIX', 'IND', 'CBOE'),
                '^IXIC': ('COMP', 'IND', 'NASDAQ'),  # NASDAQ Composite
                '^TNX': ('TNX', 'IND', 'CBOE')     # 10-Year Treasury
            }

        try:
            for symbol, (contract_symbol, sec_type, exchange) in etf_mapping.items():
                if sec_type == 'IND':
                    # Index
                    contract = self.ibkr_client.create_index_contract(contract_symbol, exchange)
                else:
                    # ETF/Stock
                    contract = self.ibkr_client.create_stock_contract(contract_symbol)
                
                self.etf_contracts[symbol] = contract
                
            self.logger.info(f"Setup {len(self.etf_contracts)} ETF contracts")
            
        except Exception as e:
            log_error(self.logger, e, "Error setting up ETF contracts")
    
    def _subscribe_to_etfs(self):
        """Subscribe to ETF market data"""
        try:
            for symbol, contract in self.etf_contracts.items():
                if symbol not in self.subscribed_symbols:
                    req_id = self._subscribe_to_symbol(symbol, contract)
                    if req_id != -1:
                        time.sleep(0.2)  # Rate limiting
                        
        except Exception as e:
            log_error(self.logger, e, "Error subscribing to ETFs")
    
    def _request_etf_data(self):
        """Request fresh ETF data"""
        try:
            for symbol, contract in self.etf_contracts.items():
                if self.ibkr_client.is_connected():
                    req_id = self.ibkr_client.request_market_data(symbol, contract, snapshot=True)
                    if req_id != -1:
                        self.symbol_to_req_id[symbol] = req_id
                        self.req_id_to_symbol[req_id] = symbol
                    time.sleep(0.2)  # Rate limiting
                    
        except Exception as e:
            log_error(self.logger, e, "Error requesting ETF data")
    
    def _subscribe_to_position_symbols(self):
        """Subscribe to market data for position symbols"""
        try:
            positions = self.data_store.get_positions()

            for position in positions:
                symbol = position.get('symbol')

                if position.get('position_type') in ['call', 'put']:
                    contract_details = position.get('contract_details', {})
                    strike = contract_details.get('strike')
                    expiry = contract_details.get('expiry', '').replace('-', '')
                    right = contract_details.get('right')
                    symbol = f"{symbol}_{strike}_{expiry}_{right}"

                if symbol:
                    # For options, also request Greeks
                    if position.get('position_type') in ['call', 'put']:
                        self._subscribe_to_option_greeks(position)
                    else:
                        contract = self.ibkr_client.create_stock_contract(symbol)
                        req_id = self._subscribe_to_symbol(symbol, contract) 

        except Exception as e:
            log_error(self.logger, e, "Error subscribing to position symbols")
    
    def _subscribe_to_option_greeks(self, position: Dict):
        """Subscribe to option Greeks"""
        try:
            contract_details = position.get('contract_details', {})
            if not contract_details:
                return
            symbol = position['symbol']
            strike = contract_details.get('strike')
            expiry = contract_details.get('expiry', '').replace('-', '')
            right = contract_details.get('right')
            multiplier = contract_details.get('multiplier', '100')

            if strike and expiry and right:
                option_contract = self.ibkr_client.create_option_contract(
                    symbol=symbol,
                    expiry=expiry,
                    strike=strike,
                    right=right,
                    multiplier=multiplier,
                    exchange=position.get('exchange', 'SMART')
                )
                option_key = f"{symbol}_{strike}_{expiry}_{right}"
                req_id = self._subscribe_to_option_symbol(option_key, option_contract)
                    
        except Exception as e:
            log_error(self.logger, e, f"Error subscribing to option Greeks for {position.get('symbol')}")
    
    def _subscribe_to_symbol(self, symbol: str, contract) -> int:
        """Subscribe to market data for a symbol"""
        try:
            if not self.ibkr_client.is_connected():
                return -1
            
            req_id = self.ibkr_client.request_market_data(symbol, contract, snapshot=True)
            if req_id != -1:
                self.subscribed_symbols.add(symbol)
                self.symbol_to_req_id[symbol] = req_id
                self.req_id_to_symbol[req_id] = symbol
                self.logger.debug(f"Subscribed to market data for {symbol}")
            
            return req_id
            
        except Exception as e:
            log_error(self.logger, e, f"Error subscribing to {symbol}")
            return -1
        
    def _subscribe_to_option_symbol(self, symbol: str, contract) -> int:
        """Subscribe to market data for a symbol"""
        try:
            if not self.ibkr_client.is_connected():
                return -1
            
            req_id = self.ibkr_client.request_option_market_data(symbol, contract, snapshot=True)
            if req_id != -1:
                self.subscribed_symbols.add(symbol)
                self.symbol_to_req_id[symbol] = req_id
                self.req_id_to_symbol[req_id] = symbol
                self.logger.debug(f"Subscribed to market data for {symbol}")
            
            return req_id
            
        except Exception as e:
            log_error(self.logger, e, f"Error subscribing to {symbol}")
            return -1
    
    def _on_market_data_update(self, data: Dict):
        """Handle market data update from IBKR"""
        try:
            symbol = data.get('symbol')
            req_id = data.get('req_id')
            tick_data = data.get('data', {})

            if symbol not in self.subscribed_symbols:
                return

            if data.get('type') == 'greeks':
                self._process_greeks_data(data)
                return
            
            if symbol and tick_data:
                # Update market data cache
                if symbol not in self.market_data_cache:
                    self.market_data_cache[symbol] = {}
                
                self.market_data_cache[symbol].update(tick_data)
                self.market_data_cache[symbol]['last_update'] = datetime.now().isoformat()
                
                # Calculate change and change percentage
                self._calculate_price_changes(symbol)
                
                self.logger.debug(f"Updated market data for {symbol}")
                
        except Exception as e:
            log_error(self.logger, e, "Error processing market data update")
    
    def _process_greeks_data(self, data: Dict):
        """Process Greeks data for options"""
        try:
            greeks_data = data.get('data', {})
            symbol = greeks_data.get('symbol')
            
            if symbol and any(key in greeks_data for key in ['delta', 'gamma', 'theta', 'vega']):
                # Find matching position and update Greeks
                positions = self.data_store.get_positions()
                
                for position in positions:
                    if (position.get('symbol') == symbol and 
                        position.get('position_type') in ['call', 'put']):
                        
                        position['greeks'] = {
                            'delta': round(greeks_data.get('delta', 0), 4),
                            'gamma': round(greeks_data.get('gamma', 0), 4),
                            'theta': round(greeks_data.get('theta', 0), 4),
                            'vega': round(greeks_data.get('vega', 0), 4),
                            'iv': round(greeks_data.get('implied_vol', 0), 4),
                            'last_update': datetime.now().isoformat()
                        }
                        
                        # Update position in data store
                        self.data_store.update_position(position)
                        break
                
                self.logger.debug(f"Updated Greeks for {symbol}")
                
        except Exception as e:
            log_error(self.logger, e, "Error processing Greeks data")
    
    def _calculate_price_changes(self, symbol: str):
        """Calculate price changes and percentages"""
        try:
            data = self.market_data_cache.get(symbol, {})
            current_price = data.get('last_price', 0)
            close_price = data.get('close', 0)
            
            if current_price > 0 and close_price > 0:
                change = current_price - close_price
                change_pct = (change / close_price) * 100
                
                data['change'] = round(change, 2)
                data['change_pct'] = round(change_pct, 2)
                
        except Exception as e:
            log_error(self.logger, e, f"Error calculating price changes for {symbol}")
    
    def _update_market_data_store(self):
        """Update market data in data store"""
        try:
            # Update ETF data
            etf_data = {}
            etf_mapping = {
                'SPY': ('SPY', 'STK'),
                'QQQ': ('QQQ', 'STK'), 
                'VIX': ('VIX', 'IND'),
                'DXY': ('DXY', 'IND'),
                '^IXIC': ('COMP', 'IND'),  # NASDAQ Composite
                '^TNX': ('TNX', 'IND')     # 10-Year Treasury
            }

            for symbol,(new_symbol,sec) in etf_mapping.items():
                market_data = self.market_data_cache.get(symbol, {})
                if market_data:
                    etf_data[symbol] = {
                        'price': market_data.get('last_price', 0),
                        'change': market_data.get('change', 0),
                        'change_pct': market_data.get('change_pct', 0),
                        'volume': market_data.get('volume', 0),
                        'bid': market_data.get('bid', 0),
                        'ask': market_data.get('ask', 0),
                        'high': market_data.get('high', 0),
                        'low': market_data.get('low', 0),
                        'last_update': market_data.get('last_update', '')
                    }
            
            if etf_data:
                self.data_store.update_etfs(etf_data)
            
            # Update position prices
            self._update_position_prices()
            
        except Exception as e:
            log_error(self.logger, e, "Error updating market data store")
    
    def _update_position_prices(self):
        """Update current prices for positions"""
        try:
            positions = self.data_store.get_positions()
            updated_positions = []
            
            for position in positions:
                symbol = position.get('symbol')

                if position.get('position_type') in ['call', 'put']:
                    contract_details = position.get('contract_details', {})
                    strike = contract_details.get('strike')
                    expiry = contract_details.get('expiry', '').replace('-', '')
                    right = contract_details.get('right')
                    symbol = f"{symbol}_{strike}_{expiry}_{right}"

                market_data = self.market_data_cache.get(symbol, {})

                if market_data and 'last_price' in market_data:
                    old_price = position.get('current_price', 0)
                    new_price = market_data['last_price']
                    
                    if new_price > 0 and new_price != old_price:
                        # Update position with new price
                        position = position.copy()
                        position['current_price'] = round(new_price, 2)
                        
                        # Recalculate market value and P&L
                        quantity = position.get('quantity', 0)
                        avg_cost = position.get('avg_cost', 0)
                        
                        if position.get('position_type') == 'stock':
                            market_value = quantity * new_price
                            unrealized_pnl = (new_price - avg_cost) * quantity
                        else:  # Options/Futures
                            multiplier = 100 if position.get('position_type') in ['call', 'put'] else 1
                            market_value = quantity * new_price * multiplier
                            unrealized_pnl = (new_price - avg_cost) * quantity * multiplier
                        
                        position['market_value'] = round(market_value, 2)
                        position['unrealized_pnl'] = round(unrealized_pnl, 2)
                        
                        if market_value != 0:
                            position['unrealized_pnl_pct'] = round((unrealized_pnl / abs(market_value)) * 100, 2)
                        
                        position['last_update'] = datetime.now().isoformat()
                        updated_positions.append(position)

            if updated_positions:
                # Update positions in data store
                all_positions = self.data_store.get_positions()
                position_dict = {pos['id']: pos for pos in all_positions}
                
                for updated_pos in updated_positions:
                    position_dict[updated_pos['id']] = updated_pos
                
                self.data_store.update_positions(list(position_dict.values()))
                
        except Exception as e:
            log_error(self.logger, e, "Error updating position prices")
    
    def _cancel_all_subscriptions(self):
        """Cancel all market data subscriptions"""
        try:
            for req_id in list(self.req_id_to_symbol.keys()):
                self.ibkr_client.cancel_market_data(req_id)
            
            self.subscribed_symbols.clear()
            self.symbol_to_req_id.clear()
            self.req_id_to_symbol.clear()
            
            self.logger.info("Cancelled all market data subscriptions")
            
        except Exception as e:
            log_error(self.logger, e, "Error cancelling subscriptions")
    
    def get_market_data(self, symbol: str) -> Dict:
        """Get market data for a symbol"""
        return self.market_data_cache.get(symbol, {})
    
    def get_etf_data(self) -> Dict:
        """Get all ETF data"""
        return self.data_store.get_etfs()
    
    def is_running(self) -> bool:
        """Check if service is running"""
        return self.running and (self.service_thread is not None and self.service_thread.is_alive())
    
    def get_subscription_stats(self) -> Dict:
        """Get subscription statistics"""
        return {
            'subscribed_symbols': len(self.subscribed_symbols),
            'symbols': list(self.subscribed_symbols),
            'etf_contracts': len(self.etf_contracts),
            'market_data_cache': len(self.market_data_cache),
            'last_etf_update': self.last_etf_update.isoformat(),
            'running': self.is_running()
        }