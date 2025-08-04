import os
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class IBKRConfig:
    """IBKR connection configuration"""
    host: str = "127.0.0.1"
    port: int = 7497  # TWS port (7496 for live, 7497 for paper)
    client_id: int = 2
    timeout: int = 10
    max_reconnect_attempts: int = 5
    reconnect_delay: int = 5

@dataclass
class WebSocketConfig:
    """WebSocket server configuration"""
    host: str = "localhost"
    port: int = 8765
    max_connections: int = 100
    ping_interval: int = 20
    ping_timeout: int = 10

@dataclass
class DataConfig:
    """Data management configuration"""
    update_frequency: float = 1.0  # seconds
    market_data_frequency: float = 0.1  # seconds
    greeks_update_frequency: float = 5.0  # seconds
    account_update_frequency: float = 30.0  # seconds
    cache_size: int = 10000
    history_retention_days: int = 30

@dataclass
class AlertConfig:
    """Alert system configuration"""
    max_alerts: int = 1000
    alert_expiry_hours: int = 24
    risk_thresholds: Dict[str, float] = None
    
    def __post_init__(self):
        if self.risk_thresholds is None:
            self.risk_thresholds = {
                "max_position_loss": -0.20,  # 20% loss
                "max_portfolio_loss": -0.10,  # 10% portfolio loss
                "days_to_expiry_warning": 7,  # 7 days
                "high_iv_threshold": 1.0,  # 100% IV
                "low_liquidity_threshold": 10  # Less than 10 volume
            }

@dataclass
class AppSettings:
    """Main application settings"""
    ibkr: IBKRConfig
    websocket: WebSocketConfig
    data: DataConfig
    alerts: AlertConfig
    debug: bool = False
    log_level: str = "INFO"
    
    # Market indices to track
    market_indices: List[str] = None
    
    # Account types
    account_types: List[str] = None
    
    def __post_init__(self):
        if self.market_indices is None:
            self.market_indices = ["SPY", "QQQ", "NASDAQ", "VIX", "DXY", "TNX"]  # TNX = 10Y Treasury
            
        if self.account_types is None:
            self.account_types = ["DU123456", "DU789012"]  # Replace with actual account IDs

# Environment-based configuration
def get_settings() -> AppSettings:
    """Get application settings based on environment"""
    
    # Determine if running in production
    is_production = os.getenv("ENVIRONMENT", "development") == "production"
    
    ibkr_config = IBKRConfig(
        host=os.getenv("IBKR_HOST", "127.0.0.1"),
        port=int(os.getenv("IBKR_PORT", "7496" if is_production else "7497")),
        client_id=int(os.getenv("IBKR_CLIENT_ID", "1")),
        timeout=int(os.getenv("IBKR_TIMEOUT", "10"))
    )
    
    websocket_config = WebSocketConfig(
        host=os.getenv("WS_HOST", "localhost"),
        port=int(os.getenv("WS_PORT", "8765")),
        max_connections=int(os.getenv("WS_MAX_CONNECTIONS", "100"))
    )
    
    data_config = DataConfig(
        update_frequency=float(os.getenv("UPDATE_FREQUENCY", "1.0")),
        market_data_frequency=float(os.getenv("MARKET_DATA_FREQUENCY", "0.1")),
        cache_size=int(os.getenv("CACHE_SIZE", "10000"))
    )
    
    alert_config = AlertConfig(
        max_alerts=int(os.getenv("MAX_ALERTS", "1000"))
    )
    
    return AppSettings(
        ibkr=ibkr_config,
        websocket=websocket_config,
        data=data_config,
        alerts=alert_config,
        debug=os.getenv("DEBUG", "False").lower() == "true",
        log_level=os.getenv("LOG_LEVEL", "INFO")
    )

# Singleton settings instance
settings = get_settings()