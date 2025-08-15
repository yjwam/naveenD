import threading
import json
from datetime import datetime
from typing import Dict, List, Any
from utils.logger import setup_logger, log_error

class DataStore:
    """Thread-safe in-memory data storage"""
    
    def __init__(self):
        self.logger = setup_logger('data_store')
        self._lock = threading.RLock()
        
        # Core data structures
        self.positions = {}  # position_id -> position_data
        self.etfs = {}       # symbol -> etf_data
        self.watchlist = {}  # symbol -> watchlist_data
        self.accounts = {}   # account_id -> account_data
        
        # Meta data
        self.last_update = datetime.now()
        self.connection_status = False
        
        self.logger.info("DataStore initialized")
    
    def update_positions(self, positions_data: List[Dict]):
        """Update positions data"""
        with self._lock:
            try:
                new_pos = []
                for position in positions_data:
                    pos_id = position.get('id')
                    if pos_id:
                        new_pos.append(pos_id)
                        if pos_id not in self.positions:
                            self.positions[pos_id] = {}
                        self.positions[pos_id].update(position)

                removed_pos = set(self.positions.keys()) - set(new_pos)
                for pos_id in removed_pos:
                    del self.positions[pos_id]

                self.last_update = datetime.now()
                self.logger.info(f"Updated {len(positions_data)} positions")
            except Exception as e:
                log_error(self.logger, e, "Error updating positions")
    
    def update_position(self, position_data: Dict):
        """Update single position"""
        with self._lock:
            try:
                pos_id = position_data.get('id')
                if pos_id:
                    if pos_id not in self.positions:
                        self.positions[pos_id] = {}
                    self.positions[pos_id].update(position_data)
                    self.last_update = datetime.now()
            except Exception as e:
                log_error(self.logger, e, "Error updating single position")
    
    def update_etfs(self, etf_data: Dict):
        """Update ETF data"""
        with self._lock:
            try:
                self.etfs.update(etf_data)
                self.last_update = datetime.now()
            except Exception as e:
                log_error(self.logger, e, "Error updating ETFs")
    
    def update_watchlist(self, watchlist_data: Dict):
        """Update watchlist data"""
        with self._lock:
            try:
                self.watchlist.update(watchlist_data)
                self.last_update = datetime.now()
                self.logger.debug(f"Updated watchlist: {list(watchlist_data.keys())}")
            except Exception as e:
                log_error(self.logger, e, "Error updating watchlist")
    
    def get_snapshot(self) -> Dict:
        """Get complete data snapshot"""
        with self._lock:
            return {
                'positions': list(self.positions.values()),
                'etfs': self.etfs.copy(),
                'watchlist': self.watchlist.copy(),
                'summary': self._calculate_summary(),
                'last_update': self.last_update.isoformat(),
                'connection_status': self.connection_status
            }
    
    def _calculate_summary(self) -> Dict:
        """Calculate portfolio summary"""
        total_value = 0
        total_pnl = 0
        total_day_pnl = 0
        position_count = len(self.positions)
        
        for position in self.positions.values():
            total_value += position.get('market_value', 0)
            total_pnl += position.get('unrealized_pnl', 0)
            total_day_pnl += position.get('day_pnl', 0)
        
        return {
            'total_value': total_value,
            'total_pnl': total_pnl,
            'total_pnl_pct': (total_pnl / total_value * 100) if total_value > 0 else 0,
            'total_day_pnl': total_day_pnl,
            'total_day_pnl_pct': (total_day_pnl / total_value * 100) if total_value > 0 else 0,
            'position_count': position_count
        }
    
    def set_connection_status(self, status: bool):
        """Update connection status"""
        with self._lock:
            self.connection_status = status
    
    def get_positions(self) -> List[Dict]:
        """Get all positions"""
        with self._lock:
            return list(self.positions.values())
    
    def get_etfs(self) -> Dict:
        """Get ETF data"""
        with self._lock:
            return self.etfs.copy()
    
    def get_watchlist(self) -> Dict:
        """Get watchlist data"""
        with self._lock:
            return self.watchlist.copy()