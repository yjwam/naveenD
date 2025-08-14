import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # IBKR Settings
    IBKR_HOST = os.getenv('IBKR_HOST', '127.0.0.1')
    IBKR_PORT = int(os.getenv('IBKR_PORT', '7497'))
    IBKR_CLIENT_ID = int(os.getenv('IBKR_CLIENT_ID', '1'))
    
    # WebSocket Settings
    WEBSOCKET_HOST = os.getenv('WEBSOCKET_HOST', 'localhost')
    WEBSOCKET_PORT = int(os.getenv('WEBSOCKET_PORT', '8765'))
    
    # Update Settings
    UPDATE_INTERVAL = int(os.getenv('UPDATE_INTERVAL', '5'))
    MARKET_DATA_INTERVAL = int(os.getenv('MARKET_DATA_INTERVAL', '10'))
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # ETFs to track
    ETFS = ['SPY', 'QQQ', 'VIX', 'DXY', '^IXIC', '^TNX']
    
    # Watchlist file
    WATCHLIST_FILE = 'data/watchlist.csv'