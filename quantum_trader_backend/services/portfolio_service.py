import threading
import time
from datetime import datetime
from typing import Dict, List, Set

from config.settings import settings
from utils.logger import get_logger
from core.ibkr_client import IBKRClient
from core.data_manager import DataManager

class PortfolioService:
    """Service for managing portfolio and account data"""
    
    def __init__(self, ibkr_client: IBKRClient, data_manager: DataManager):
        self.ibkr_client = ibkr_client
        self.data_manager = data_manager
        self.logger = get_logger("portfolio_service")
        
        # Service state
        self.running = False
        self.service_thread = None
        
        # Account management
        self.tracked_accounts: Set[str] = set()
        self.account_update_requested = False
        self.positions_update_requested = False
        
        # Update timing
        self.last_account_update = datetime.min
        self.last_positions_update = datetime.min
        
        self.logger.info("Portfolio service initialized")
    
    def start(self) -> None:
        """Start the portfolio service"""
        if self.running:
            self.logger.warning("Portfolio service already running")
            return
        
        self.running = True
        self.service_thread = threading.Thread(target=self._run_service, daemon=True)
        self.service_thread.start()
        
        self.logger.info("Portfolio service started")
    
    def stop(self) -> None:
        """Stop the portfolio service"""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel account updates
        self._cancel_account_updates()
        
        if self.service_thread and self.service_thread.is_alive():
            self.service_thread.join(timeout=5)
        
        self.logger.info("Portfolio service stopped")
    
    def _run_service(self) -> None:
        """Main service loop"""
        try:
            # Initial setup
            self._setup_initial_requests()
            
            while self.running:
                try:
                    current_time = datetime.now()
                    
                    # Request account updates periodically
                    if (current_time - self.last_account_update).total_seconds() >= settings.data.account_update_frequency:
                        self._request_account_updates()
                        self.last_account_update = current_time
                    
                    # Request positions updates periodically
                    if (current_time - self.last_positions_update).total_seconds() >= settings.data.account_update_frequency:
                        self._request_positions_update()
                        self.last_positions_update = current_time
                    
                    # Update portfolio calculations
                    self._update_portfolio_calculations()
                    
                    # Sleep between checks
                    time.sleep(0.1)
                    
                except Exception as e:
                    self.logger.error(f"Error in portfolio service loop: {e}")
                    time.sleep(0.5)
                    
        except Exception as e:
            self.logger.error(f"Fatal error in portfolio service: {e}")
        finally:
            self._cleanup()
    
    def _setup_initial_requests(self) -> None:
        """Setup initial account and position requests"""
        self.logger.info("Setting up initial portfolio requests...")
        
        # Wait for connection
        if not self.ibkr_client.ensure_connection():
            self.logger.error("Cannot setup portfolio requests - no IBKR connection")
            return
        
        # Add configured accounts to tracking
        for account_id in settings.account_types:
            self.add_account(account_id)
        
        time.sleep(0.2)  # Allow initial data to flow
        
        # Initial requests
        self._request_positions_update()
        self._request_account_updates()
    
    def add_account(self, account_id: str) -> bool:
        """Add an account to tracking"""
        if account_id in self.tracked_accounts:
            return True
        
        try:
            self.tracked_accounts.add(account_id)
            self.logger.info(f"Added account {account_id} to tracking")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding account {account_id}: {e}")
            return False
    
    def remove_account(self, account_id: str) -> bool:
        """Remove an account from tracking"""
        if account_id not in self.tracked_accounts:
            return True
        
        try:
            # Cancel account updates for this account
            if self.ibkr_client.is_connected():
                self.ibkr_client.cancel_account_updates(account_id)
            
            self.tracked_accounts.discard(account_id)
            
            # Remove from data manager
            if account_id in self.data_manager.portfolios:
                del self.data_manager.portfolios[account_id]
            
            self.logger.info(f"Removed account {account_id} from tracking")
            return True
            
        except Exception as e:
            self.logger.error(f"Error removing account {account_id}: {e}")
            return False
    
    def _request_account_updates(self) -> None:
        """Request account updates for all tracked accounts"""
        if not self.ibkr_client.ensure_connection():
            return
        
        try:
            for account_id in self.tracked_accounts:
                self.ibkr_client.request_account_updates(account_id)
            
            self.account_update_requested = True
            self.logger.debug(f"Requested account updates for {len(self.tracked_accounts)} accounts")
            
        except Exception as e:
            self.logger.error(f"Error requesting account updates: {e}")
    
    def _request_positions_update(self) -> None:
        """Request positions update"""
        if not self.ibkr_client.ensure_connection():
            return
        
        try:
            self.ibkr_client.request_positions()
            self.positions_update_requested = True
            self.logger.debug("Requested positions update")
            
        except Exception as e:
            self.logger.error(f"Error requesting positions update: {e}")
    
    def _update_portfolio_calculations(self) -> None:
        """Update portfolio-level calculations"""
        try:
            for portfolio in self.data_manager.portfolios.values():
                # Recalculate totals
                portfolio.calculate_totals()
                
                # Update market prices for positions
                for position in portfolio.positions:
                    market_data = self.data_manager.market_data.get(position.symbol)
                    if market_data and market_data.price > 0:
                        position.update_price(market_data.price)
                
                # Recalculate after price updates
                portfolio.calculate_totals()
                
        except Exception as e:
            self.logger.error(f"Error updating portfolio calculations: {e}")
    
    def _cancel_account_updates(self) -> None:
        """Cancel all account updates"""
        try:
            if self.ibkr_client.is_connected():
                for account_id in self.tracked_accounts:
                    self.ibkr_client.cancel_account_updates(account_id)
                
                self.ibkr_client.cancel_positions()
            
            self.logger.info("Cancelled all account updates")
            
        except Exception as e:
            self.logger.error(f"Error cancelling account updates: {e}")
    
    def _cleanup(self) -> None:
        """Cleanup service resources"""
        self._cancel_account_updates()
        self.logger.info("Portfolio service cleanup completed")
    
    def get_portfolio_summary(self) -> Dict[str, any]:
        """Get portfolio summary across all accounts"""
        try:
            total_value = 0
            total_day_pnl = 0
            total_unrealized_pnl = 0
            total_positions = 0
            
            account_summaries = {}
            
            for account_id, portfolio in self.data_manager.portfolios.items():
                total_value += portfolio.total_value
                total_day_pnl += portfolio.day_pnl
                total_unrealized_pnl += portfolio.total_pnl
                total_positions += len(portfolio.positions)
                
                account_summaries[account_id] = {
                    'account_type': portfolio.account_type.value,
                    'total_value': portfolio.total_value,
                    'day_pnl': portfolio.day_pnl,
                    'day_pnl_percent': portfolio.day_pnl_percent,
                    'total_pnl': portfolio.total_pnl,
                    'positions_count': len(portfolio.positions),
                    'cash_balance': portfolio.cash_balance,
                    'buying_power': portfolio.buying_power
                }
            
            return {
                'total_value': total_value,
                'total_day_pnl': total_day_pnl,
                'total_day_pnl_percent': (total_day_pnl / total_value * 100) if total_value > 0 else 0,
                'total_unrealized_pnl': total_unrealized_pnl,
                'total_positions': total_positions,
                'accounts': account_summaries,
                'tracked_accounts': len(self.tracked_accounts),
                'last_update': self.data_manager.last_portfolio_update.isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error generating portfolio summary: {e}")
            return {}
    
    def get_account_details(self, account_id: str) -> Dict[str, any]:
        """Get detailed information for a specific account"""
        if account_id not in self.data_manager.portfolios:
            return {}
        
        try:
            portfolio = self.data_manager.portfolios[account_id]
            account_data = self.data_manager.account_data.get(account_id, {})
            
            # Get positions with current market data
            positions_detail = []
            for position in portfolio.positions:
                market_data = self.data_manager.market_data.get(position.symbol)
                
                position_dict = position.to_dict()
                if market_data:
                    position_dict['market_data'] = {
                        'current_price': market_data.price,
                        'change': market_data.change,
                        'change_percent': market_data.change_percent,
                        'volume': market_data.volume,
                        'bid': market_data.bid,
                        'ask': market_data.ask
                    }
                
                positions_detail.append(position_dict)
            
            return {
                'account_id': account_id,
                'portfolio': portfolio.to_dict(),
                'positions_detail': positions_detail,
                'account_values': account_data,
                'last_update': portfolio.updated_at.isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting account details for {account_id}: {e}")
            return {}
    
    def get_position_details(self, account_id: str, symbol: str, 
                           strike_price: float = None, expiry: str = None, 
                           option_type: str = None) -> Dict[str, any]:
        """Get detailed information for a specific position"""
        if account_id not in self.data_manager.portfolios:
            return {}
        
        try:
            portfolio = self.data_manager.portfolios[account_id]
            position = portfolio.get_position(symbol, strike_price, expiry, option_type)
            
            if not position:
                return {}
            
            # Get current market data
            market_data = self.data_manager.market_data.get(symbol)
            
            result = position.to_dict()
            
            if market_data:
                result['current_market_data'] = market_data.to_dict()
            
            # Get Greeks if option
            if position.greeks:
                result['greeks_detail'] = position.greeks.to_dict()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting position details: {e}")
            return {}
    
    def is_running(self) -> bool:
        """Check if service is running"""
        return self.running and (self.service_thread is not None and self.service_thread.is_alive())
    
    def get_service_stats(self) -> Dict[str, any]:
        """Get service statistics"""
        return {
            'running': self.is_running(),
            'tracked_accounts': list(self.tracked_accounts),
            'total_portfolios': len(self.data_manager.portfolios),
            'account_update_requested': self.account_update_requested,
            'positions_update_requested': self.positions_update_requested,
            'last_account_update': self.last_account_update.isoformat(),
            'last_positions_update': self.last_positions_update.isoformat(),
            'last_portfolio_update': self.data_manager.last_portfolio_update.isoformat()
        }
    
    def force_refresh_account(self, account_id: str) -> bool:
        """Force refresh data for a specific account"""
        if account_id not in self.tracked_accounts:
            return False
        
        try:
            if self.ibkr_client.ensure_connection():
                self.ibkr_client.request_account_updates(account_id)
                self.logger.info(f"Forced refresh for account {account_id}")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Error forcing refresh for account {account_id}: {e}")
            return False
    
    def force_refresh_positions(self) -> bool:
        """Force refresh all positions"""
        try:
            if self.ibkr_client.ensure_connection():
                self.ibkr_client.request_positions()
                self.logger.info("Forced refresh for all positions")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Error forcing refresh positions: {e}")
            return False