from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum

class MarketDataType(Enum):
    STOCK = "stock"
    INDEX = "index"
    OPTION = "option"
    FUTURE = "future"

@dataclass
class MarketData:
    """Real-time market data for any instrument"""
    symbol: str
    price: float
    bid: float = 0.0
    ask: float = 0.0
    volume: int = 0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0  # Previous close
    change: float = 0.0
    change_percent: float = 0.0
    timestamp: datetime = None
    data_type: MarketDataType = MarketDataType.STOCK
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
            
        # Calculate change if we have close price
        if self.close > 0:
            self.change = self.price - self.close
            self.change_percent = (self.change / self.close) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['data_type'] = self.data_type.value
        return data

@dataclass
class IndexData:
    """Market indices data structure"""
    spy: Optional[MarketData] = None
    qqq: Optional[MarketData] = None
    nasdaq: Optional[MarketData] = None
    vix: Optional[MarketData] = None
    dxy: Optional[MarketData] = None  # Dollar Index
    ten_year: Optional[MarketData] = None  # 10-Year Treasury
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        result = {}
        if self.spy:
            result['SPY'] = self.spy.to_dict()
        if self.qqq:
            result['QQQ'] = self.qqq.to_dict()
        if self.nasdaq:
            result['NASDAQ'] = self.nasdaq.to_dict()
        if self.vix:
            result['VIX'] = self.vix.to_dict()
        if self.dxy:
            result['DXY'] = self.dxy.to_dict()
        if self.ten_year:
            result['10Y'] = self.ten_year.to_dict()
        return result

@dataclass
class OptionChainData:
    """Option chain data for a specific underlying"""
    underlying_symbol: str
    underlying_price: float
    expiry: str
    calls: Dict[float, MarketData]  # Strike -> MarketData
    puts: Dict[float, MarketData]   # Strike -> MarketData
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'underlying_symbol': self.underlying_symbol,
            'underlying_price': self.underlying_price,
            'expiry': self.expiry,
            'calls': {str(strike): data.to_dict() for strike, data in self.calls.items()},
            'puts': {str(strike): data.to_dict() for strike, data in self.puts.items()},
            'timestamp': self.timestamp.isoformat()
        }

@dataclass
class Greeks:
    """Option Greeks"""
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    rho: float = 0.0
    implied_volatility: float = 0.0
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data

@dataclass
class TickData:
    """Individual tick data"""
    symbol: str
    tick_type: int
    value: float
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'symbol': self.symbol,
            'tick_type': self.tick_type,
            'value': self.value,
            'timestamp': self.timestamp.isoformat()
        }