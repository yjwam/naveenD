import threading
import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Callable
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import TickerId, BarData
from ibapi.ticktype import TickType, TickTypeEnum

from config.settings import settings
from utils.logger import IBKRLogger
from models.market_data import MarketData, Greeks, MarketDataType
from models.portfolio import Position, Portfolio, AccountType, PositionType

class IBKRWrapper(EWrapper):
    """IBKR API Event Handler - FIXED VERSION"""
    
    def __init__(self, client_instance):
        EWrapper.__init__(self)
        self.client = client_instance
        self.logger = IBKRLogger("ibkr_wrapper")
        
        # Connection state tracking - FIXED
        self.connected = False
        self.connection_ready = False  # NEW: Track when fully initialized
        self.next_valid_order_id = 1
        self.managed_accounts = ""
        
        # Data storage
        self.market_data: Dict[int, Dict] = {}
        self.account_data: Dict[str, Dict] = {}
        self.positions_data: Dict[str, List] = {}
        self.portfolio_data: Dict[str, Dict] = {}
        
        # Request tracking
        self.req_id_to_symbol: Dict[int, str] = {}
        self.req_id_to_contract: Dict[int, Contract] = {}
        
        # Callbacks
        self.callbacks: Dict[str, List[Callable]] = {
            'market_data': [],
            'position_update': [],
            'account_update': [],
            'connection_status': []
        }
        
        # Connection event for synchronization - NEW
        self.connection_event = threading.Event()
    
    def register_callback(self, event_type: str, callback: Callable):
        """Register callback for specific events"""
        if event_type in self.callbacks:
            self.callbacks[event_type].append(callback)
    
    def _trigger_callbacks(self, event_type: str, data: dict):
        """Trigger registered callbacks"""
        for callback in self.callbacks.get(event_type, []):
            try:
                callback(data)
            except Exception as e:
                self.logger.logger.error(f"Callback error: {e}")
    
    # Connection Events - FIXED
    def connectAck(self):
        """Connection acknowledged - FIXED"""
        self.logger.connection_event("connect_ack")
        self.connected = True
        self._trigger_callbacks('connection_status', {'status': 'connected'})
    
    def connectionClosed(self):
        """Connection closed - FIXED"""
        self.logger.connection_event("connection_closed")
        self.connected = False
        self.connection_ready = False
        self.connection_event.clear()
        self._trigger_callbacks('connection_status', {'status': 'disconnected'})
    
    def nextValidId(self, orderId: int):
        """Next valid order ID received - FIXED: This indicates connection is ready"""
        self.next_valid_order_id = orderId  
        self.connection_ready = True  # NOW we're ready for requests
        self.connection_event.set()  # Signal that connection is ready
        self.logger.connection_event("next_valid_id", {"order_id": orderId})
        self._trigger_callbacks('connection_status', {'status': 'ready', 'next_order_id': orderId})
    
    def managedAccounts(self, accountsList: str):
        """Managed accounts received - FIXED"""
        self.managed_accounts = accountsList
        self.logger.connection_event("managed_accounts", {"accounts": accountsList})
    
    # Market Data Events - FIXED to handle delayed data properly
    def tickPrice(self, reqId: TickerId, tickType: int, price: float, attrib):
        """Receive tick price data - FIXED"""
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        
        symbol = self.req_id_to_symbol.get(reqId, f"REQ_{reqId}")
        tick_data = self.market_data[reqId]

        # Handle both live and delayed data
        if tickType in [TickTypeEnum.LAST, TickTypeEnum.DELAYED_LAST]:
            tick_data['last_price'] = price
        elif tickType in [TickTypeEnum.BID, TickTypeEnum.DELAYED_BID]:
            tick_data['bid'] = price
        elif tickType in [TickTypeEnum.ASK, TickTypeEnum.DELAYED_ASK]:
            tick_data['ask'] = price
        elif tickType in [TickTypeEnum.HIGH, TickTypeEnum.DELAYED_HIGH]:
            tick_data['high'] = price
        elif tickType in [TickTypeEnum.LOW, TickTypeEnum.DELAYED_LOW]:
            tick_data['low'] = price
        elif tickType in [TickTypeEnum.CLOSE, TickTypeEnum.DELAYED_CLOSE]:
            tick_data['close'] = price

        # Trigger callback only if we have meaningful data
        if any(key in tick_data for key in ['last_price', 'bid', 'ask']):
            self._trigger_callbacks('market_data', {
                'symbol': symbol,
                'req_id': reqId,
                'tick_type': tickType,
                'price': price,
                'data': tick_data
            })
    
    def tickSize(self, reqId: TickerId, tickType: int, size: int):
        """Receive tick size data - FIXED"""
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        
        symbol = self.req_id_to_symbol.get(reqId, f"REQ_{reqId}")
        tick_data = self.market_data[reqId]

        if tickType in [TickTypeEnum.VOLUME, TickTypeEnum.DELAYED_VOLUME]:
            tick_data['volume'] = size
        elif tickType in [TickTypeEnum.BID_SIZE, TickTypeEnum.DELAYED_BID_SIZE]:
            tick_data['bid_size'] = size
        elif tickType in [TickTypeEnum.ASK_SIZE, TickTypeEnum.DELAYED_ASK_SIZE]:
            tick_data['ask_size'] = size
        
        self._trigger_callbacks('market_data', {
            'symbol': symbol,
            'req_id': reqId,
            'tick_type': tickType,
            'size': size,
            'data': tick_data
        })
    
    def tickOptionComputation(self, reqId: TickerId, tickType: int, 
                         tickAttrib: int, impliedVol: float, delta: float, 
                         optPrice: float, pvDividend: float, gamma: float, 
                         vega: float, theta: float, undPrice: float):
        """Receive option computation data (Greeks) - FIXED"""
        symbol = self.req_id_to_symbol.get(reqId, f"REQ_{reqId}")
        
        # Only process model option computation tick types
        if tickType in [10, 82]:  # Model option computation
            # Validate Greeks data - filter out invalid values
            valid_delta = delta if delta != -2 and abs(delta) <= 1 else 0
            valid_gamma = gamma if gamma != -2 and gamma >= 0 else 0
            valid_theta = theta if theta != -2 else 0
            valid_vega = vega if vega != -2 and vega >= 0 else 0
            valid_iv = impliedVol if impliedVol > 0 and impliedVol != -1 else 0
            
            greeks_data = {
                'symbol': symbol,
                'req_id': reqId,
                'implied_vol': valid_iv,
                'delta': valid_delta,
                'gamma': valid_gamma,
                'vega': valid_vega,
                'theta': valid_theta,
                'option_price': optPrice if optPrice > 0 and optPrice != -1 else 0
            }
            
            self.logger.logger.info(f"Received Greeks for {symbol}: Δ={valid_delta:.3f}, Γ={valid_gamma:.3f}")
            
            self._trigger_callbacks('market_data', {
                'symbol': symbol,
                'type': 'greeks',
                'data': greeks_data
            })
    
    # Portfolio and Account Events - FIXED
    def updatePortfolio(self, contract: Contract, position: float, 
                       marketPrice: float, marketValue: float, 
                       averageCost: float, unrealizedPNL: float, 
                       realizedPNL: float, accountName: str):
        """Update portfolio position - FIXED"""
        symbol = contract.symbol
        
        position_data = {
            'symbol': symbol,
            'contract': contract,
            'position': position,
            'market_price': marketPrice,
            'market_value': marketValue,
            'average_cost': averageCost,
            'unrealized_pnl': unrealizedPNL,
            'realized_pnl': realizedPNL,
            'account': accountName,
            'timestamp': datetime.now()
        }
        
        # Store position data with better deduplication
        if accountName not in self.positions_data:
            self.positions_data[accountName] = []
        
        # Find and update existing position or add new
        position_key = self._get_position_key(contract)
        updated = False
        
        for i, pos in enumerate(self.positions_data[accountName]):
            if self._get_position_key(pos['contract']) == position_key:
                self.positions_data[accountName][i] = position_data
                updated = True
                break
        
        if not updated:
            self.positions_data[accountName].append(position_data)
        
        self._trigger_callbacks('position_update', position_data)
        
        self.logger.portfolio_event(accountName, "position_update", {
            'symbol': symbol,
            'position': position,
            'market_value': marketValue
        })
    
    def _get_position_key(self, contract: Contract) -> str:
        """Generate unique key for position identification"""
        key = f"{contract.symbol}_{contract.secType}"
        if hasattr(contract, 'strike') and contract.strike:
            key += f"_{contract.strike}"
        if hasattr(contract, 'right') and contract.right:
            key += f"_{contract.right}"
        if hasattr(contract, 'lastTradeDateOrContractMonth') and contract.lastTradeDateOrContractMonth:
            key += f"_{contract.lastTradeDateOrContractMonth}"
        return key
    
    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        """Update account values - FIXED"""
        if accountName not in self.account_data:
            self.account_data[accountName] = {}
            
        # Only store important account values to reduce noise
        important_keys = ['CashBalance', 'BuyingPower', 'NetLiquidation', 
                         'GrossPositionValue', 'TotalCashValue', 'AvailableFunds']
        
        if key in important_keys:
            self.account_data[accountName][key] = {
                'value': val,
                'currency': currency,
                'timestamp': datetime.now()
            }
            
            self._trigger_callbacks('account_update', {
                'account': accountName,
                'key': key,
                'value': val,
                'currency': currency
            })
    
    def accountDownloadEnd(self, accountName: str):
        """Account download completed - FIXED"""
        self.logger.portfolio_event(accountName, "account_download_complete")
    
    def positionEnd(self):
        """Position data download completed - FIXED"""
        self.logger.portfolio_event("all", "positions_download_complete")
    
    # Error Handling - FIXED
    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson: str = ""):
        """Handle errors - FIXED with better error categorization"""
        
        # Filter out informational messages that aren't actually errors
        info_codes = [2104, 2106, 2158]  # Market data farm connection OK messages
        if errorCode in info_codes:
            self.logger.logger.info(f"IBKR Info: {errorString} (Code: {errorCode})")
            return
        
        # Connection-related errors
        connection_errors = [502, 503, 504, 1100, 1101, 1102]
        if errorCode in connection_errors:
            self.connected = False
            self.connection_ready = False
            self.connection_event.clear()
            self._trigger_callbacks('connection_status', {
                'status': 'error', 
                'code': errorCode,
                'message': errorString
            })
        
        # Market data errors
        market_data_errors = [200, 354, 10197]
        if errorCode in market_data_errors:
            self.logger.logger.warning(f"Market data error: {errorString} (Code: {errorCode}, ReqId: {reqId})")
            return
        
        # Log other errors normally
        self.logger.error_event(errorCode, errorString, reqId)

class IBKRClient(EClient):
    """Enhanced IBKR Client with proper connection handling - FIXED VERSION"""
    
    def __init__(self):
        self.wrapper = IBKRWrapper(self)
        EClient.__init__(self, self.wrapper)
        
        self.logger = IBKRLogger("ibkr_client")
        self.connection_thread = None
        self.running = False
        
        # Request ID management
        self.next_req_id = 1000
        self._req_id_lock = threading.Lock()
        
        # Connection management - FIXED
        self.max_reconnect_attempts = settings.ibkr.max_reconnect_attempts
        self.reconnect_delay = settings.ibkr.reconnect_delay
        self.reconnect_attempts = 0
        self.connection_timeout = settings.ibkr.timeout
        
        # Connection state
        self._connection_lock = threading.Lock()
        
    def get_next_req_id(self) -> int:
        """Get next available request ID"""
        with self._req_id_lock:
            req_id = self.next_req_id
            self.next_req_id += 1
            return req_id
    
    def connect_and_run(self) -> bool:
        """Connect to IBKR and start message processing - FIXED"""
        with self._connection_lock:
            try:
                # Clean up any existing connection
                if self.isConnected():
                    self.disconnect()
                    time.sleep(2)
                
                self.logger.connection_event("attempting_connection", {
                    "host": settings.ibkr.host,
                    "port": settings.ibkr.port,
                    "client_id": settings.ibkr.client_id
                })
                
                # Clear connection state
                self.wrapper.connected = False
                self.wrapper.connection_ready = False
                self.wrapper.connection_event.clear()
                
                # Connect
                self.connect(
                    settings.ibkr.host,
                    settings.ibkr.port,
                    settings.ibkr.client_id
                )
                
                # Start connection thread
                self.connection_thread = threading.Thread(target=self.run, daemon=True)
                self.connection_thread.start()
                self.running = True
                
                # Wait for connection to be ready (not just connected)
                connection_ready = self.wrapper.connection_event.wait(timeout=self.connection_timeout)
                
                if connection_ready and self.wrapper.connection_ready:
                    self.logger.connection_event("connected_successfully")
                    self.reconnect_attempts = 0
                    # Add startup delay to let connection stabilize
                    time.sleep(settings.startup_delay)
                    return True
                else:
                    self.logger.connection_event("connection_timeout")
                    return False
                    
            except Exception as e:
                self.logger.error_event(-1, f"Connection failed: {e}")
                return False
    
    def disconnect_and_stop(self):
        """Disconnect and stop all operations - FIXED"""
        with self._connection_lock:
            self.running = False
            
            try:
                if self.isConnected():
                    self.disconnect()
                
                if self.connection_thread and self.connection_thread.is_alive():
                    self.connection_thread.join(timeout=5)
                
                self.logger.connection_event("disconnected")
                
            except Exception as e:
                self.logger.error_event(-1, f"Disconnect error: {e}")
    
    def reconnect(self) -> bool:
        """Attempt to reconnect to IBKR - FIXED"""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            self.logger.connection_event("max_reconnect_attempts_reached")
            return False
        
        self.reconnect_attempts += 1
        self.logger.connection_event("attempting_reconnect", 
                                   {"attempt": self.reconnect_attempts})
        
        # Disconnect properly first
        self.disconnect_and_stop()
        
        # Wait before reconnecting
        time.sleep(self.reconnect_delay)
        
        return self.connect_and_run()
    
    def ensure_connection(self) -> bool:
        """Ensure connection is active and ready - FIXED"""
        if not (self.wrapper.connected and self.wrapper.connection_ready and self.isConnected()):
            self.logger.connection_event("connection_check_failed", {
                "connected": self.wrapper.connected,
                "ready": self.wrapper.connection_ready,
                "socket_connected": self.isConnected()
            })
            return self.reconnect()
        return True
    
    # Market Data Methods - FIXED
    def request_market_data(self, symbol: str, contract: Contract, snapshot: bool = True) -> int:
        """Request market data with proper error handling - FIXED"""
        if not self.ensure_connection():
            return -1
        
        try:
            req_id = self.get_next_req_id()
            self.wrapper.req_id_to_symbol[req_id] = symbol
            self.wrapper.req_id_to_contract[req_id] = contract
            
            # Use minimal tick list and snapshots to avoid subscription issues
            tick_list = ""  # Empty for basic data
            
            self.reqMktData(req_id, contract, tick_list, snapshot, False, [])
            
            self.logger.market_data_event(symbol, "market_data_requested", {
                "req_id": req_id, 
                "sec_type": contract.secType,
                "snapshot": snapshot
            })
            return req_id
            
        except Exception as e:
            self.logger.error_event(-1, f"Error requesting market data for {symbol}: {e}")
            return -1
    
    def cancel_market_data(self, req_id: int):
        """Cancel market data subscription - FIXED"""
        try:
            if self.isConnected():
                self.cancelMktData(req_id)
                symbol = self.wrapper.req_id_to_symbol.get(req_id, "Unknown")
                self.logger.market_data_event(symbol, "market_data_cancelled", {"req_id": req_id})
        except Exception as e:
            self.logger.error_event(-1, f"Error cancelling market data: {e}")
    
    def request_option_computation(self, symbol: str, contract: Contract) -> int:
        """Request option Greeks computation - FIXED"""
        if not self.ensure_connection():
            return -1
        
        req_id = self.get_next_req_id()
        self.wrapper.req_id_to_symbol[req_id] = symbol
        self.wrapper.req_id_to_contract[req_id] = contract
        
        # Request live option computation data (NOT snapshot for Greeks)
        # Use specific tick types for Greeks: 100,101,104,105,106
        self.reqMktData(req_id, contract, "", True, False, [])
        
        self.logger.market_data_event(symbol, "greeks_requested", {"req_id": req_id})
        return req_id
    
    # Account and Portfolio Methods - FIXED
    def request_account_updates(self, account_id: str):
        """Request account updates - FIXED"""
        if not self.ensure_connection():
            return
        
        try:
            self.reqAccountUpdates(True, account_id)
            self.logger.portfolio_event(account_id, "account_updates_requested")
        except Exception as e:
            self.logger.error_event(-1, f"Error requesting account updates: {e}")
    
    def cancel_account_updates(self, account_id: str):
        """Cancel account updates"""
        if self.isConnected():
            self.reqAccountUpdates(False, account_id)
            self.logger.portfolio_event(account_id, "account_updates_cancelled")
    
    def request_positions(self):
        """Request all positions - FIXED"""
        if not self.ensure_connection():
            return
        
        try:
            self.reqPositions()
            self.logger.portfolio_event("all", "positions_requested")
        except Exception as e:
            self.logger.error_event(-1, f"Error requesting positions: {e}")
    
    def cancel_positions(self):
        """Cancel positions request"""
        if self.isConnected():
            self.cancelPositions()
    
    # Contract Creation Helpers
    @staticmethod
    def create_stock_contract(symbol: str, exchange: str = "SMART", currency: str = "USD") -> Contract:
        """Create a stock contract"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = exchange
        contract.currency = currency
        return contract
    
    @staticmethod
    def create_option_contract(symbol: str, expiry: str, strike: float, 
                              right: str, exchange: str = "SMART", 
                              currency: str = "USD") -> Contract:
        """Create an option contract"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "OPT"
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiry
        contract.strike = strike
        contract.right = right  # 'C' for Call, 'P' for Put
        contract.multiplier = "100"
        return contract
    
    @staticmethod
    def create_index_contract(symbol: str, exchange: str = "CBOE") -> Contract:
        """Create an index contract"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "IND"
        contract.exchange = exchange
        contract.currency = "USD"
        return contract
    
    def is_connected(self) -> bool:
        """Check if client is connected and ready - FIXED"""
        return (self.wrapper.connected and 
                self.wrapper.connection_ready and 
                self.isConnected())
    
    # Callback Registration
    def register_market_data_callback(self, callback: Callable):
        """Register callback for market data updates"""
        self.wrapper.register_callback('market_data', callback)
    
    def register_position_callback(self, callback: Callable):
        """Register callback for position updates"""
        self.wrapper.register_callback('position_update', callback)
    
    def register_account_callback(self, callback: Callable):
        """Register callback for account updates"""
        self.wrapper.register_callback('account_update', callback)
    
    def register_connection_callback(self, callback: Callable):
        """Register callback for connection status changes"""
        self.wrapper.register_callback('connection_status', callback)