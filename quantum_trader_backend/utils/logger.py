import logging
import sys
from datetime import datetime
from typing import Optional
import structlog

def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    """Configure structured logging for the application"""
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.ConsoleRenderer()
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper())),
        logger_factory=structlog.WriteLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structured logger instance"""
    return structlog.get_logger(name)

class IBKRLogger:
    """Custom logger for IBKR API events"""
    
    def __init__(self, name: str = "ibkr"):
        self.logger = get_logger(name)
    
    def connection_event(self, event_type: str, details: dict = None):
        """Log connection events"""
        self.logger.info("IBKR Connection Event", event_type=event_type, details=details or {})
    
    def market_data_event(self, symbol: str, event_type: str, data: dict = None):
        """Log market data events"""
        self.logger.debug("Market Data Event", symbol=symbol, event_type=event_type, data=data or {})
    
    def portfolio_event(self, account: str, event_type: str, data: dict = None):
        """Log portfolio events"""
        self.logger.info("Portfolio Event", account=account, event_type=event_type, data=data or {})
    
    def error_event(self, error_code: int, error_msg: str, req_id: int = -1):
        """Log error events"""
        self.logger.error("IBKR Error", 
                         error_code=error_code, 
                         error_msg=error_msg, 
                         req_id=req_id)
    
    def order_event(self, order_id: int, event_type: str, data: dict = None):
        """Log order events"""
        self.logger.info("Order Event", 
                        order_id=order_id, 
                        event_type=event_type, 
                        data=data or {})