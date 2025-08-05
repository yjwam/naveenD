import os
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class IBKRConfig:
    """IBKR connection configuration - FIXED"""
    host: str = "127.0.0.1"
    port: int = 7497  # TWS port (7496 for live, 7497 for paper)
    client_id: int = 1
    timeout: int = 30  # Increased from 10
    max_reconnect_attempts: int = 3  # Reduced from 5
    reconnect_delay: int = 1  # Increased from 5
    connection_check_interval: int = 30  # NEW: How often to check connection health
    request_delay: float = 1.0  # NEW: Delay between requests to avoid overwhelming

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
    """Data management configuration - FIXED"""
    update_frequency: float = 2.0  # Increased from 1.0
    market_data_frequency: float = 5.0  # Increased from 0.1
    greeks_update_frequency: float = 30.0  # Increased from 5.0
    account_update_frequency: float = 60.0  # Increased from 30.0
    cache_size: int = 10000
    history_retention_days: int = 30
    
    # NEW: Market data limits
    max_market_data_subscriptions: int = 50  # Limit concurrent subscriptions
    market_data_retry_limit: int = 3  # Max retries per symbol
    snapshot_mode: bool = True  # Use snapshots instead of streaming

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
    """Main application settings - FIXED"""
    ibkr: IBKRConfig
    websocket: WebSocketConfig
    data: DataConfig
    alerts: AlertConfig
    debug: bool = False
    log_level: str = "INFO"
    
    # Market indices to track - REDUCED to avoid issues
    market_indices: List[str] = None
    
    # Account types
    account_types: List[str] = None
    
    # NEW: Connection management
    startup_delay: int = 1  # Wait time after connection before making requests
    graceful_shutdown_timeout: int = 10  # Time to wait for graceful shutdown
    
    def __post_init__(self):
        if self.market_indices is None:
            # Reduced list to avoid subscription issues
            self.market_indices = ["SPY", "QQQ"]  # Removed VIX, TNX temporarily
            
        if self.account_types is None:
            self.account_types = ["DU123456", "DU789012"]  # Replace with actual account IDs

# Environment-based configuration
def get_settings() -> AppSettings:
    """Get application settings based on environment - FIXED"""
    
    # Determine if running in production
    is_production = os.getenv("ENVIRONMENT", "development") == "production"
    
    # IBKR configuration with better defaults
    ibkr_config = IBKRConfig(
        host=os.getenv("IBKR_HOST", "127.0.0.1"),
        port=int(os.getenv("IBKR_PORT", "7496" if is_production else "7497")),
        client_id=int(os.getenv("IBKR_CLIENT_ID", "1")),
        timeout=int(os.getenv("IBKR_TIMEOUT", "30")),  # Increased default
        max_reconnect_attempts=int(os.getenv("IBKR_MAX_RECONNECT", "3")),  # Reduced
        reconnect_delay=int(os.getenv("IBKR_RECONNECT_DELAY", "10"))  # Increased
    )
    
    websocket_config = WebSocketConfig(
        host=os.getenv("WS_HOST", "localhost"),
        port=int(os.getenv("WS_PORT", "8765")),
        max_connections=int(os.getenv("WS_MAX_CONNECTIONS", "100"))
    )
    
    # Data configuration with conservative defaults
    data_config = DataConfig(
        update_frequency=float(os.getenv("UPDATE_FREQUENCY", "2.0")),  # Slower updates
        market_data_frequency=float(os.getenv("MARKET_DATA_FREQUENCY", "5.0")),  # Much slower
        cache_size=int(os.getenv("CACHE_SIZE", "10000")),
        max_market_data_subscriptions=int(os.getenv("MAX_MARKET_SUBSCRIPTIONS", "5")),
        snapshot_mode=os.getenv("MARKET_DATA_SNAPSHOT_MODE", "true").lower() == "true"
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
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        startup_delay=int(os.getenv("STARTUP_DELAY", "10"))
    )

# Singleton settings instance
settings = get_settings()