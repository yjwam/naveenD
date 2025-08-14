import logging
import sys
import traceback
from config import Config

def setup_logger(name='quantum_trader'):
    """Setup simple logging"""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(getattr(logging, Config.LOG_LEVEL))
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler
        file_handler = logging.FileHandler('quantum_trader.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def log_error(logger, error, context=""):
    """Log error with traceback"""
    logger.error(f"{context}: {str(error)}")
    logger.error(f"Traceback: {traceback.format_exc()}")