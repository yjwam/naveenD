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
    """IBKR API Event Handler"""
    
    def __init__(self, client_instance):
        EWrapper.__init__(self)
        self.client = client_instance
        self.logger = IBKRLogger("ibkr_wrapper")
        
        # Data storage
        self.market_data: Dict[int, Dict] = {}
        self.account_data: Dict[str, Dict] = {}
        self.positions_data: Dict[str, List] = {}
        self.portfolio_data: Dict[str, Dict] = {}
        
        # Request tracking
        self.req_id_to_symbol: Dict[int, str] = {}
        self.req_id_to_contract: Dict[int, Contract] = {}
        
        # Connection status
        self.connected = False
        self.next_valid_order_id = 1
        
        # Callbacks
        self.callbacks: Dict[str, List[Callable]] = {
            'market_data': [],
            'position_update': [],
            'account_update': [],
            'connection_status': []
        }
    
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
    
    # Connection Events
    def connectAck(self):
        """Connection acknowledged"""
        self.logger.connection_event("connect_ack")
        self.connected = True
        self._trigger_callbacks('connection_status', {'status': 'connected'})
    
    def connectionClosed(self):
        """Connection closed"""
        self.logger.connection_event("connection_closed")
        self.connected = False
        self._trigger_callbacks('connection_status', {'status': 'disconnected'})
    
    def nextValidId(self, orderId: int):
        """Next valid order ID received"""
        self.next_valid_order_id = orderId
        self.logger.connection_event("next_valid_id", {"order_id": orderId})
    
    # Market Data Events
    def tickPrice(self, reqId: TickerId, tickType: int, price: float, attrib):
        """Receive tick price data"""
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        
        symbol = self.req_id_to_symbol.get(reqId, f"REQ_{reqId}")
        
        # Update price data based on tick type
        tick_data = self.market_data[reqId]
        idx2name = TickTypeEnum.idx2name

        if tickType == TickTypeEnum.LAST or tickType == TickTypeEnum.DELAYED_LAST:
            tick_data['last_price'] = price
        elif tickType == TickTypeEnum.BID or tickType == TickTypeEnum.DELAYED_BID:
            tick_data['bid'] = price
        elif tickType == TickTypeEnum.ASK or tickType == TickTypeEnum.DELAYED_ASK:
            tick_data['ask'] = price
        elif tickType == TickTypeEnum.HIGH or tickType == TickTypeEnum.DELAYED_HIGH:
            tick_data['high'] = price
        elif tickType == TickTypeEnum.LOW or tickType == TickTypeEnum.DELAYED_LOW:
            tick_data['low'] = price
        elif tickType == TickTypeEnum.CLOSE or tickType == TickTypeEnum.DELAYED_CLOSE:
            tick_data['close'] = price

        # Trigger market data callback
        self._trigger_callbacks('market_data', {
            'symbol': symbol,
            'req_id': reqId,
            'tick_type': tickType,
            'price': price,
            'data': tick_data
        })
    
    def tickSize(self, reqId: TickerId, tickType: int, size: int):
        """Receive tick size data"""
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        
        symbol = self.req_id_to_symbol.get(reqId, f"REQ_{reqId}")
        tick_data = self.market_data[reqId]

        if tickType == TickTypeEnum.VOLUME or tickType == TickTypeEnum.DELAYED_VOLUME:
            tick_data['volume'] = size
        elif tickType == TickTypeEnum.BID_SIZE or tickType == TickTypeEnum.DELAYED_BID_SIZE:
            tick_data['bid_size'] = size
        elif tickType == TickTypeEnum.ASK_SIZE or tickType == TickTypeEnum.DELAYED_ASK_SIZE:
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
        """Receive option computation data (Greeks) - Fixed parameter signature"""
        symbol = self.req_id_to_symbol.get(reqId, f"REQ_{reqId}")
        
        greeks_data = {
            'symbol': symbol,
            'req_id': reqId,
            'implied_vol': impliedVol if impliedVol != -1 else 0,
            'delta': delta if delta != -2 else 0,
            'gamma': gamma if gamma != -2 else 0,
            'vega': vega if vega != -2 else 0,
            'theta': theta if theta != -2 else 0,
            'option_price': optPrice if optPrice != -1 else 0,
            'underlying_price': undPrice if undPrice != -1 else 0
        }
        
        self._trigger_callbacks('market_data', {
            'symbol': symbol,
            'type': 'greeks',
            'data': greeks_data
        })
    
    # Portfolio and Account Events
    def updatePortfolio(self, contract: Contract, position: float, 
                       marketPrice: float, marketValue: float, 
                       averageCost: float, unrealizedPNL: float, 
                       realizedPNL: float, accountName: str):
        """Update portfolio position"""
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
        
        # Store position data
        if accountName not in self.positions_data:
            self.positions_data[accountName] = []
        
        # Update or add position
        updated = False
        for i, pos in enumerate(self.positions_data[accountName]):
            if (pos['symbol'] == symbol and 
                pos['contract'].strike == contract.strike and
                pos['contract'].lastTradeDateOrContractMonth == contract.lastTradeDateOrContractMonth and
                getattr(pos['contract'], 'right', None) == getattr(contract, 'right', None)):
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
    
    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        """Update account values"""
        if accountName not in self.account_data:
            self.account_data[accountName] = {}
        
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
        """Account download completed"""
        self.logger.portfolio_event(accountName, "account_download_complete")
    
    # Error Handling
    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson: str = ""):
        """Handle errors - Fixed parameter signature"""
        self.logger.error_event(errorCode, errorString, reqId)
        
        # Handle specific error codes
        if errorCode in [502, 503, 504]:  # Connection errors
            self.connected = False
            self._trigger_callbacks('connection_status', {'status': 'error', 'code': errorCode})

class IBKRClient(EClient):
    """Enhanced IBKR Client with automatic reconnection"""
    
    def __init__(self):
        self.wrapper = IBKRWrapper(self)
        EClient.__init__(self, self.wrapper)
        
        self.logger = IBKRLogger("ibkr_client")
        self.connection_thread = None
        self.running = False
        
        # Request ID management
        self.next_req_id = 1000
        self._req_id_lock = threading.Lock()
        
        # Reconnection settings
        self.max_reconnect_attempts = settings.ibkr.max_reconnect_attempts
        self.reconnect_delay = settings.ibkr.reconnect_delay
        self.reconnect_attempts = 0
    
    def get_next_req_id(self) -> int:
        """Get next available request ID"""
        with self._req_id_lock:
            req_id = self.next_req_id
            self.next_req_id += 1
            return req_id
    
    def connect_and_run(self) -> bool:
        """Connect to IBKR and start message processing"""
        try:
            self.connect(
                settings.ibkr.host,
                settings.ibkr.port,
                settings.ibkr.client_id
            )
            
            # Start connection thread
            self.connection_thread = threading.Thread(target=self.run, daemon=True)
            self.connection_thread.start()
            self.running = True
            
            # Wait for connection
            timeout = settings.ibkr.timeout
            start_time = time.time()
            
            while not self.wrapper.connected and (time.time() - start_time) < timeout:
                time.sleep(0.1)
            
            if self.wrapper.connected:
                self.logger.connection_event("connected_successfully")
                self.reconnect_attempts = 0
                return True
            else:
                self.logger.connection_event("connection_timeout")
                return False
                
        except Exception as e:
            self.logger.error_event(-1, f"Connection failed: {e}")
            print(traceback.format_exc())
            return False
    
    def disconnect_and_stop(self):
        """Disconnect and stop all operations"""
        self.running = False
        if self.isConnected():
            self.disconnect()
        
        if self.connection_thread and self.connection_thread.is_alive():
            self.connection_thread.join(timeout=5)
        
        self.logger.connection_event("disconnected")
    
    def reconnect(self) -> bool:
        """Attempt to reconnect to IBKR"""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            self.logger.connection_event("max_reconnect_attempts_reached")
            return False
        
        self.reconnect_attempts += 1
        self.logger.connection_event("attempting_reconnect", 
                                   {"attempt": self.reconnect_attempts})
        
        # Wait before reconnecting
        time.sleep(self.reconnect_delay)
        
        # Disconnect if still connected
        if self.isConnected():
            self.disconnect()
        
        return self.connect_and_run()
    
    def ensure_connection(self) -> bool:
        """Ensure connection is active, reconnect if necessary"""
        if not self.wrapper.connected or not self.isConnected():
            return self.reconnect()
        return True
    
    # Market Data Methods
    def request_market_data(self, symbol: str, contract: Contract) -> int:
        """Request real-time market data for a contract"""
        if not self.ensure_connection():
            return -1
        
        req_id = self.get_next_req_id()
        self.wrapper.req_id_to_symbol[req_id] = symbol
        self.wrapper.req_id_to_contract[req_id] = contract
        
        # Request market data (generic tick list for all data)
        self.reqMktData(req_id, contract, "100,101,104,105,106,107,165,221,225,233,236,258", False, False, [])
        
        self.logger.market_data_event(symbol, "market_data_requested", {"req_id": req_id})
        return req_id
    
    def cancel_market_data(self, req_id: int):
        """Cancel market data subscription"""
        if self.isConnected():
            self.cancelMktData(req_id)
            symbol = self.wrapper.req_id_to_symbol.get(req_id, "Unknown")
            self.logger.market_data_event(symbol, "market_data_cancelled", {"req_id": req_id})
    
    def request_option_computation(self, symbol: str, contract: Contract) -> int:
        """Request option Greeks computation"""
        if not self.ensure_connection():
            return -1
        
        req_id = self.get_next_req_id()
        self.wrapper.req_id_to_symbol[req_id] = symbol
        self.wrapper.req_id_to_contract[req_id] = contract
        
        # Request option computation data
        self.reqMktData(req_id, contract, "100,101,104,105,106,221", False, False, [])
        
        return req_id
    
    # Account and Portfolio Methods
    def request_account_updates(self, account_id: str):
        """Request account updates"""
        if not self.ensure_connection():
            return
        
        self.reqAccountUpdates(True, account_id)
        self.logger.portfolio_event(account_id, "account_updates_requested")
    
    def cancel_account_updates(self, account_id: str):
        """Cancel account updates"""
        if self.isConnected():
            self.reqAccountUpdates(False, account_id)
            self.logger.portfolio_event(account_id, "account_updates_cancelled")
    
    def request_positions(self):
        """Request all positions"""
        if not self.ensure_connection():
            return
        
        self.reqPositions()
        self.logger.portfolio_event("all", "positions_requested")
    
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
    
    # Data Access Methods
    def get_market_data(self, req_id: int) -> Dict:
        """Get current market data for request ID"""
        return self.wrapper.market_data.get(req_id, {})
    
    def get_positions_for_account(self, account_id: str) -> List[Dict]:
        """Get all positions for an account"""
        return self.wrapper.positions_data.get(account_id, [])
    
    def get_account_data(self, account_id: str) -> Dict:
        """Get account data"""
        return self.wrapper.account_data.get(account_id, {})
    
    def is_connected(self) -> bool:
        """Check if client is connected"""
        return self.wrapper.connected and self.isConnected()
    
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