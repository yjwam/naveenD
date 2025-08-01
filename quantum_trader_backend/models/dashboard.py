from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Any, Optional
from enum import Enum
from .market_data import IndexData
from .portfolio import Portfolio, AccountType, PerformanceMetrics, RiskMetrics

class AlertLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    URGENT = "urgent"

class AlertType(Enum):
    RISK = "risk"
    PROFIT_LOSS = "profit_loss"
    EXPIRATION = "expiration"
    MARKET = "market"
    SYSTEM = "system"

@dataclass
class Alert:
    """System alert/notification"""
    id: str
    type: AlertType
    level: AlertLevel
    title: str
    message: str
    symbol: Optional[str] = None
    account_id: Optional[str] = None
    value: Optional[float] = None
    threshold: Optional[float] = None
    created_at: datetime = None
    expires_at: Optional[datetime] = None
    acknowledged: bool = False
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['type'] = self.type.value
        data['level'] = self.level.value
        data['created_at'] = self.created_at.isoformat()
        if self.expires_at:
            data['expires_at'] = self.expires_at.isoformat()
        return data

@dataclass
class SystemStatus:
    """System status information"""
    ibkr_connected: bool = False
    websocket_clients: int = 0
    last_market_data_update: Optional[datetime] = None
    last_portfolio_update: Optional[datetime] = None
    total_positions: int = 0
    active_alerts: int = 0
    system_uptime: float = 0.0  # seconds
    memory_usage: float = 0.0  # MB
    cpu_usage: float = 0.0  # percentage
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        if self.last_market_data_update:
            data['last_market_data_update'] = self.last_market_data_update.isoformat()
        if self.last_portfolio_update:
            data['last_portfolio_update'] = self.last_portfolio_update.isoformat()
        return data

@dataclass
class DashboardData:
    """Complete dashboard data structure"""
    timestamp: datetime
    market_indices: IndexData
    portfolios: Dict[str, Portfolio]  # account_id -> Portfolio
    alerts: List[Alert]
    performance_metrics: PerformanceMetrics
    risk_metrics: RiskMetrics
    system_status: SystemStatus
    
    # Summary data for quick access
    total_portfolio_value: float = 0.0
    total_day_pnl: float = 0.0
    total_unrealized_pnl: float = 0.0
    total_positions: int = 0
    
    def __post_init__(self):
        self.calculate_summary()
    
    def calculate_summary(self) -> None:
        """Calculate summary statistics across all portfolios"""
        self.total_portfolio_value = sum(p.total_value for p in self.portfolios.values())
        self.total_day_pnl = sum(p.day_pnl for p in self.portfolios.values())
        self.total_unrealized_pnl = sum(p.total_pnl for p in self.portfolios.values())
        self.total_positions = sum(len(p.positions) for p in self.portfolios.values())
    
    def add_alert(self, alert: Alert) -> None:
        """Add an alert to the dashboard"""
        self.alerts.append(alert)
        # Keep only recent alerts (last 100)
        if len(self.alerts) > 100:
            self.alerts = self.alerts[-100:]
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active (non-acknowledged) alerts"""
        now = datetime.now()
        return [alert for alert in self.alerts 
                if not alert.acknowledged and 
                (alert.expires_at is None or alert.expires_at > now)]
    
    def get_portfolio_by_type(self, account_type: AccountType) -> Optional[Portfolio]:
        """Get portfolio by account type (first match)"""
        for portfolio in self.portfolios.values():
            if portfolio.account_type == account_type:
                return portfolio
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        # Organize portfolios by account type for frontend
        portfolios_by_type = {}
        for portfolio in self.portfolios.values():
            account_type_key = portfolio.account_type.value
            if account_type_key not in portfolios_by_type:
                portfolios_by_type[account_type_key] = []
            portfolios_by_type[account_type_key].append(portfolio.to_dict())
        
        return {
            'timestamp': self.timestamp.isoformat(),
            'market_indices': self.market_indices.to_dict(),
            'portfolios': portfolios_by_type,
            'alerts': [alert.to_dict() for alert in self.get_active_alerts()],
            'performance_metrics': self.performance_metrics.to_dict(),
            'risk_metrics': self.risk_metrics.to_dict(),
            'system_status': self.system_status.to_dict(),
            'summary': {
                'total_portfolio_value': self.total_portfolio_value,
                'total_day_pnl': self.total_day_pnl,
                'total_day_pnl_percent': (self.total_day_pnl / self.total_portfolio_value * 100) if self.total_portfolio_value > 0 else 0,
                'total_unrealized_pnl': self.total_unrealized_pnl,
                'total_positions': self.total_positions,
                'active_alerts': len(self.get_active_alerts())
            }
        }

@dataclass
class StreamingUpdate:
    """Lightweight update for streaming to frontend"""
    type: str  # 'market_data', 'portfolio_update', 'alert', etc.
    data: Dict[str, Any]
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'type': self.type,
            'data': self.data,
            'timestamp': self.timestamp.isoformat()
        }