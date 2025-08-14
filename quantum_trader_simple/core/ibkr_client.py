import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Callable
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import TickerId
from ibapi.ticktype import TickTypeEnum
from config import Config
from utils.logger import setup_logger, log_error

class IBKRWrapper(EWrapper):
    """IBKR API Event Handler"""
    
    def __init__(self, client_instance):
        EWrapper.__init__(self)
        self.client = client_instance
        self.logger = setup_logger('ibkr_wrapper')
        
        # Connection state
        self.connected = False
        self.connection_ready = False
        self.next_valid_order_id = 1
        self.managed_accounts = ""
        
        # Data storage
        self.market_data = {}
        self.positions_data = {}
        self.account_data = {}
        
        # Request tracking
        self.req_id_to_symbol = {}
        self.symbol_to_req_id = {}
        
        # Callbacks
        self.callbacks = {
            'market_data': [],
            'position_update': [],
            'account_update': [],
            'connection_status': []
        }
        
        # Connection event
        self.connection_event = threading.Event()
    
    def register_callback(self, event_type: str, callback: Callable):
        """Register callback for events"""
        if event_type in self.callbacks:
            self.callbacks[event_type].append(callback)
    
    def _trigger_callbacks(self, event_type: str, data: dict):
        """Trigger registered callbacks"""
        for callback in self.callbacks.get(event_type, []):
            try:
                callback(data)
            except Exception as e:
                log_error(self.logger, e, f"Callback error for {event_type}")
    
    # Connection Events
    def connectAck(self):
        self.connected = True
        self.logger.info("IBKR connection acknowledged")
        self._trigger_callbacks('connection_status', {'status': 'connected'})
    
    def connectionClosed(self):
        self.connected = False
        self.connection_ready = False
        self.connection_event.clear()
        self.logger.warning("IBKR connection closed")
        self._trigger_callbacks('connection_status', {'status': 'disconnected'})
    
    def nextValidId(self, orderId: int):
        self.next_valid_order_id = orderId
        self.connection_ready = True
        self.connection_event.set()
        self.logger.info(f"Connection ready, next order ID: {orderId}")
        self._trigger_callbacks('connection_status', {'status': 'ready', 'next_order_id': orderId})
    
    def managedAccounts(self, accountsList: str):
        self.managed_accounts = accountsList
        self.logger.info(f"Managed accounts: {accountsList}")
    
    # Market Data Events
    def tickPrice(self, reqId: TickerId, tickType: int, price: float, attrib):
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        
        symbol = self.req_id_to_symbol.get(reqId, f"REQ_{reqId}")
        tick_data = self.market_data[reqId]
        
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
        
        self._trigger_callbacks('market_data', {
            'symbol': symbol,
            'req_id': reqId,
            'tick_type': tickType,
            'price': price,
            'data': tick_data
        })
    
    def tickSize(self, reqId: TickerId, tickType: int, size: int):
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
    
    def tickOptionComputation(self, reqId: TickerId, tickType: int, tickAttrib: int,
                             impliedVol: float, delta: float, optPrice: float,
                             pvDividend: float, gamma: float, vega: float,
                             theta: float, undPrice: float):
        symbol = self.req_id_to_symbol.get(reqId, f"REQ_{reqId}")
        
        if tickType in [TickTypeEnum.MODEL_OPTION, TickTypeEnum.DELAYED_MODEL_OPTION]:  # Model option computation
            greeks_data = {
                'symbol': symbol,
                'req_id': reqId,
                'implied_vol': impliedVol if impliedVol > 0 and impliedVol != -1 else 0,
                'delta': delta if delta != -2 and abs(delta) <= 1 else 0,
                'gamma': gamma if gamma != -2 and gamma >= 0 else 0,
                'vega': vega if vega != -2 and vega >= 0 else 0,
                'theta': theta if theta != -2 else 0,
                'option_price': optPrice if optPrice > 0 and optPrice != -1 else 0
            }
            
            self._trigger_callbacks('market_data', {
                'symbol': symbol,
                'type': 'greeks',
                'data': greeks_data
            })
    
    # Portfolio Events
    def updatePortfolio(self, contract: Contract, position: float, marketPrice: float,
                       marketValue: float, averageCost: float, unrealizedPNL: float,
                       realizedPNL: float, accountName: str):
        position_data = {
            'symbol': contract.symbol,
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
        
        self._trigger_callbacks('position_update', position_data)
    
    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        important_keys = ['CashBalance', 'BuyingPower', 'NetLiquidation', 
                         'GrossPositionValue', 'TotalCashValue', 'AvailableFunds']
        
        if key in important_keys:
            self._trigger_callbacks('account_update', {
                'account': accountName,
                'key': key,
                'value': val,
                'currency': currency
            })
    
    def accountDownloadEnd(self, accountName: str):
        self.logger.info(f"Account download completed for {accountName}")
    
    def positionEnd(self):
        self.logger.info("Position data download completed")
    
    # Error Handling
    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson: str = ""):
        info_codes = [2104, 2106, 2158, 2168]
        if errorCode in info_codes:
            self.logger.debug(f"IBKR Info: {errorString} (Code: {errorCode})")
            return
        
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
        
        self.logger.error(f"IBKR Error {errorCode}: {errorString} (ReqId: {reqId})")

class IBKRClient(EClient):
    """IBKR Client with connection management"""
    
    def __init__(self):
        self.wrapper = IBKRWrapper(self)
        EClient.__init__(self, self.wrapper)
        
        self.logger = setup_logger('ibkr_client')
        self.connection_thread = None
        self.running = False
        
        # Request ID management
        self.next_req_id = 1000
        self._req_id_lock = threading.Lock()
    
    def get_next_req_id(self) -> int:
        with self._req_id_lock:
            req_id = self.next_req_id
            self.next_req_id += 1
            return req_id
    
    def connect_and_run(self) -> bool:
        try:
            
            self.logger.info(f"Connecting to IBKR at {Config.IBKR_HOST}:{Config.IBKR_PORT}")
            
            self.wrapper.connected = False
            self.wrapper.connection_ready = False
            self.wrapper.connection_event.clear()
            
            self.connect(Config.IBKR_HOST, Config.IBKR_PORT, Config.IBKR_CLIENT_ID)

            self.connection_thread = threading.Thread(target=self.run, daemon=True)
            self.connection_thread.start()
            self.running = True
            
            connection_ready = self.wrapper.connection_event.wait(timeout=30)
            
            if connection_ready and self.wrapper.connection_ready:
                self.logger.info("IBKR connection established successfully")
                time.sleep(0.1)  # Allow connection to stabilize
                return True
            else:
                self.logger.error("IBKR connection timeout")
                return False
                
        except Exception as e:
            log_error(self.logger, e, "Connection failed")
            return False
    
    def disconnect_and_stop(self):
        self.running = False
        try:
            if self.isConnected():
                self.disconnect()
            
            if self.connection_thread and self.connection_thread.is_alive():
                self.connection_thread.join(timeout=5)
            
            self.logger.info("IBKR disconnected")
        except Exception as e:
            log_error(self.logger, e, "Disconnect error")
    
    def is_connected(self) -> bool:
        return (self.wrapper.connected and 
                self.wrapper.connection_ready and 
                self.isConnected())
    
    def request_market_data(self, symbol: str, contract: Contract, snapshot: bool = True) -> int:
        if not self.is_connected():
            return -1
        
        try:
            req_id = self.get_next_req_id()
            self.wrapper.req_id_to_symbol[req_id] = symbol
            self.wrapper.symbol_to_req_id[symbol] = req_id

            self.reqMarketDataType(3)
            
            self.reqMktData(req_id, contract, "", snapshot, False, [])
            self.logger.debug(f"Requested market data for {symbol} (ReqId: {req_id})")
            return req_id
        except Exception as e:
            log_error(self.logger, e, f"Error requesting market data for {symbol}")
            return -1
    
    def cancel_market_data(self, req_id: int):
        try:
            if self.isConnected():
                self.cancelMktData(req_id)
        except Exception as e:
            log_error(self.logger, e, "Error cancelling market data")
    
    def request_positions(self):
        if not self.is_connected():
            return
        
        try:
            self.reqPositions()
            self.logger.info("Requested all positions")
        except Exception as e:
            log_error(self.logger, e, "Error requesting positions")
    
    def request_account_updates(self, account_id: str):
        if not self.is_connected():
            return
        
        try:
            self.reqAccountUpdates(True, account_id)
            self.logger.info(f"Requested account updates for {account_id}")
        except Exception as e:
            log_error(self.logger, e, f"Error requesting account updates for {account_id}")
    
    @staticmethod
    def create_stock_contract(symbol: str, exchange: str = "SMART", currency: str = "USD") -> Contract:
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
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "OPT"
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiry
        contract.strike = strike
        contract.right = right
        contract.multiplier = "100"
        return contract
    
    @staticmethod
    def create_index_contract(symbol: str, exchange: str = "SMART") -> Contract:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "IND"
        contract.exchange = exchange
        contract.currency = "USD"
        return contract
    
    def register_market_data_callback(self, callback: Callable):
        self.wrapper.register_callback('market_data', callback)
    
    def register_position_callback(self, callback: Callable):
        self.wrapper.register_callback('position_update', callback)
    
    def register_account_callback(self, callback: Callable):
        self.wrapper.register_callback('account_update', callback)
    
    def register_connection_callback(self, callback: Callable):
        self.wrapper.register_callback('connection_status', callback)