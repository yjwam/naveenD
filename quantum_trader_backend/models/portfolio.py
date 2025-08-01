from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
from .market_data import Greeks

class AccountType(Enum):
    INDIVIDUAL_TAXABLE = "individual_taxable"
    RETIREMENT_TAX_FREE = "retirement_tax_free"

class PositionType(Enum):
    STOCK = "Stock"
    CALL = "Call"
    PUT = "Put"

class Priority(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    URGENT = "URGENT"

class Signal(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_LOSS = "TAKE_LOSS"
    PARTIAL_PROFIT = "PARTIAL_PROFIT"
    ROLL = "ROLL"

@dataclass
class Position:
    """Individual position in portfolio"""
    symbol: str
    account_id: str
    account_type: AccountType
    position_type: PositionType
    quantity: int
    avg_cost: float
    current_price: float
    market_value: float
    unrealized_pnl: float
    realized_pnl: float = 0.0
    day_pnl: float = 0.0
    
    # Options specific fields
    strike_price: Optional[float] = None
    expiry: Optional[str] = None
    option_type: Optional[str] = None  # 'C' or 'P'
    greeks: Optional[Greeks] = None
    
    # Strategy and analysis
    strategy: str = ""
    confidence: int = 50  # 0-100
    signal: Signal = Signal.HOLD
    priority: Priority = Priority.MEDIUM
    notes: str = ""
    
    # Levels for options
    levels: Dict[str, float] = None
    
    # Timestamps
    created_at: datetime = None
    updated_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()
        if self.levels is None:
            self.levels = {}
            
        # Calculate market value if not provided
        if self.market_value == 0:
            if self.position_type == PositionType.STOCK:
                self.market_value = self.quantity * self.current_price
            else:  # Options
                self.market_value = self.quantity * self.current_price * 100  # Options multiplier
        
        # Calculate unrealized P&L if not provided
        if self.unrealized_pnl == 0:
            if self.position_type == PositionType.STOCK:
                self.unrealized_pnl = (self.current_price - self.avg_cost) * self.quantity
            else:  # Options
                self.unrealized_pnl = (self.current_price - self.avg_cost) * self.quantity * 100
    
    def update_price(self, new_price: float) -> None:
        """Update position with new market price"""
        old_market_value = self.market_value
        self.current_price = new_price
        
        if self.position_type == PositionType.STOCK:
            self.market_value = self.quantity * new_price
            self.unrealized_pnl = (new_price - self.avg_cost) * self.quantity
        else:  # Options
            self.market_value = self.quantity * new_price * 100
            self.unrealized_pnl = (new_price - self.avg_cost) * self.quantity * 100
        
        # Calculate day P&L (this would need previous day's close)
        self.day_pnl = self.market_value - old_market_value
        self.updated_at = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['account_type'] = self.account_type.value
        data['position_type'] = self.position_type.value
        data['signal'] = self.signal.value
        data['priority'] = self.priority.value
        data['created_at'] = self.created_at.isoformat()
        data['updated_at'] = self.updated_at.isoformat()
        
        if self.greeks:
            data['greeks'] = self.greeks.to_dict()
        
        return data

@dataclass
class Portfolio:
    """Portfolio for a specific account"""
    account_id: str
    account_type: AccountType
    positions: List[Position]
    cash_balance: float = 0.0
    total_value: float = 0.0
    day_pnl: float = 0.0
    total_pnl: float = 0.0
    buying_power: float = 0.0
    margin_used: float = 0.0
    
    # Performance metrics
    total_pnl_percent: float = 0.0
    day_pnl_percent: float = 0.0
    
    updated_at: datetime = None
    
    def __post_init__(self):
        if self.updated_at is None:
            self.updated_at = datetime.now()
        self.calculate_totals()
    
    def calculate_totals(self) -> None:
        """Calculate portfolio totals"""
        position_value = sum(pos.market_value for pos in self.positions)
        self.total_value = position_value + self.cash_balance
        self.day_pnl = sum(pos.day_pnl for pos in self.positions)
        self.total_pnl = sum(pos.unrealized_pnl + pos.realized_pnl for pos in self.positions)
        
        # Calculate percentages
        if self.total_value > 0:
            self.day_pnl_percent = (self.day_pnl / self.total_value) * 100
            invested_amount = self.total_value - self.total_pnl
            if invested_amount > 0:
                self.total_pnl_percent = (self.total_pnl / invested_amount) * 100
        
        self.updated_at = datetime.now()
    
    def add_position(self, position: Position) -> None:
        """Add a position to the portfolio"""
        # Check if position already exists and update it
        for i, existing_pos in enumerate(self.positions):
            if (existing_pos.symbol == position.symbol and 
                existing_pos.strike_price == position.strike_price and
                existing_pos.expiry == position.expiry and
                existing_pos.option_type == position.option_type):
                self.positions[i] = position
                self.calculate_totals()
                return
        
        # Add new position
        self.positions.append(position)
        self.calculate_totals()
    
    def remove_position(self, symbol: str, strike_price: Optional[float] = None, 
                       expiry: Optional[str] = None, option_type: Optional[str] = None) -> bool:
        """Remove a position from the portfolio"""
        for i, pos in enumerate(self.positions):
            if (pos.symbol == symbol and 
                pos.strike_price == strike_price and
                pos.expiry == expiry and
                pos.option_type == option_type):
                del self.positions[i]
                self.calculate_totals()
                return True
        return False
    
    def get_position(self, symbol: str, strike_price: Optional[float] = None,
                    expiry: Optional[str] = None, option_type: Optional[str] = None) -> Optional[Position]:
        """Get a specific position"""
        for pos in self.positions:
            if (pos.symbol == symbol and 
                pos.strike_price == strike_price and
                pos.expiry == expiry and
                pos.option_type == option_type):
                return pos
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'account_id': self.account_id,
            'account_type': self.account_type.value,
            'positions': [pos.to_dict() for pos in self.positions],
            'summary': {
                'cash_balance': self.cash_balance,
                'total_value': self.total_value,
                'day_pnl': self.day_pnl,
                'day_pnl_percent': self.day_pnl_percent,
                'total_pnl': self.total_pnl,
                'total_pnl_percent': self.total_pnl_percent,
                'buying_power': self.buying_power,
                'margin_used': self.margin_used,
                'position_count': len(self.positions)
            },
            'updated_at': self.updated_at.isoformat()
        }

@dataclass
class PerformanceMetrics:
    """Portfolio performance metrics"""
    total_return: float = 0.0
    annualized_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

@dataclass
class RiskMetrics:
    """Portfolio risk metrics"""
    portfolio_beta: float = 0.0
    portfolio_delta: float = 0.0
    portfolio_gamma: float = 0.0
    portfolio_theta: float = 0.0
    portfolio_vega: float = 0.0
    var_95: float = 0.0  # Value at Risk 95%
    expected_shortfall: float = 0.0
    correlation_to_spy: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)