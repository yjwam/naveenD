import threading
import time
import hashlib
from datetime import datetime
from typing import Dict, List
from utils.logger import setup_logger, log_error
from core.ibkr_client import IBKRClient
from core.data_store import DataStore

class PositionService:
    """Service for managing positions data"""
    
    def __init__(self, ibkr_client:IBKRClient, data_store:DataStore):
        self.ibkr_client = ibkr_client
        self.data_store = data_store
        self.logger = setup_logger('position_service')
        
        # Service state
        self.running = False
        self.service_thread = None
        
        # Position tracking
        self.position_cache = {}
        self.account_cache = {}
        self.last_update = datetime.min
        
        self.logger.info("Position service initialized")
    
    def start(self):
        """Start the position service"""
        if self.running:
            self.logger.warning("Position service already running")
            return
        
        self.running = True
        self.service_thread = threading.Thread(target=self._run_service, daemon=True)
        self.service_thread.start()
        
        # Setup callbacks
        self._setup_callbacks()
        
        self.logger.info("Position service started")
    
    def stop(self):
        """Stop the position service"""
        if not self.running:
            return
        
        self.running = False
        
        if self.service_thread and self.service_thread.is_alive():
            self.service_thread.join(timeout=5)
        
        self.logger.info("Position service stopped")
    
    def _run_service(self):
        """Main service loop"""
        try:
            # Initial position request
            time.sleep(0.2)  # Wait for connection
            self._request_initial_data()
            
            while self.running:
                try:
                    # Periodic position updates
                    if (datetime.now() - self.last_update).total_seconds() >= 30:
                        self._request_positions_update()
                        self.last_update = datetime.now()
                    
                except Exception as e:
                    log_error(self.logger, e, "Error in position service loop")
                    time.sleep(1)
                    
        except Exception as e:
            log_error(self.logger, e, "Fatal error in position service")
    
    def _setup_callbacks(self):
        """Setup IBKR callbacks"""
        self.ibkr_client.register_position_callback(self._on_position_update)
        self.ibkr_client.register_account_callback(self._on_account_update)
    
    def _request_initial_data(self):
        """Request initial positions and account data"""
        try:
            if self.ibkr_client.is_connected():
                self.ibkr_client.request_positions()
                
                # Request account updates for managed accounts
                accounts = self.ibkr_client.wrapper.managed_accounts.split(',')
                for account in accounts:
                    if account.strip():
                        self.ibkr_client.request_account_updates(account.strip())
                
                self.logger.info("Requested initial position and account data")
        except Exception as e:
            log_error(self.logger, e, "Error requesting initial data")
    
    def _request_positions_update(self):
        """Request positions update"""
        try:
            if self.ibkr_client.is_connected():
                self.ibkr_client.request_positions()
                self.logger.debug("Requested positions update")
        except Exception as e:
            log_error(self.logger, e, "Error requesting positions update")
    
    def _on_position_update(self, position_data: Dict):
        """Handle position update from IBKR"""
        try:
            contract = position_data['contract']
            position_qty = position_data['position']
            
            # Skip zero positions
            if position_qty == 0:
                return
            
            # Create position data
            position = self._create_position_data(position_data)
            
            if position:
                # Update cache
                pos_id = position['id']
                self.position_cache[pos_id] = position
                
                # Update data store
                positions_list = list(self.position_cache.values())
                self.data_store.update_positions(positions_list)
                
                self.logger.debug(f"Updated position: {position['symbol']} ({position['position_type']})")
                
        except Exception as e:
            log_error(self.logger, e, "Error processing position update")
    
    def _on_account_update(self, account_data: Dict):
        """Handle account update from IBKR"""
        try:
            account_id = account_data['account']
            key = account_data['key']
            value = account_data['value']
            
            if account_id not in self.account_cache:
                self.account_cache[account_id] = {}
            
            self.account_cache[account_id][key] = value
            
            self.logger.debug(f"Updated account {account_id}: {key} = {value}")
            
        except Exception as e:
            log_error(self.logger, e, "Error processing account update")
    
    def _create_position_data(self, ibkr_data: Dict) -> Dict:
        """Create standardized position data from IBKR data"""
        try:
            contract = ibkr_data['contract']
            position_qty = ibkr_data['position']
            market_price = ibkr_data['market_price']
            market_value = ibkr_data['market_value']
            avg_cost = ibkr_data['average_cost']
            unrealized_pnl = ibkr_data['unrealized_pnl']
            realized_pnl = ibkr_data['realized_pnl']
            account = ibkr_data['account']
            
            # Determine position type
            position_type = self._get_position_type(contract)
            
            # Create unique position ID
            position_id = self._generate_position_id(contract, account)
            
            # Calculate additional metrics
            day_pnl = 0  # Will be calculated later with previous day data
            unrealized_pnl_pct = (unrealized_pnl / abs(market_value) * 100) if market_value != 0 else 0
            day_pnl_pct = 0  # Will be calculated later
            
            position = {
                'id': position_id,
                'symbol': contract.symbol,
                'account': account,
                'position_type': position_type,
                'quantity': int(position_qty),
                'avg_cost': round(avg_cost, 2),
                'current_price': round(market_price, 2),
                'market_value': round(market_value, 2),
                'unrealized_pnl': round(unrealized_pnl, 2),
                'unrealized_pnl_pct': round(unrealized_pnl_pct, 2),
                'realized_pnl': round(realized_pnl, 2),
                'day_pnl': round(day_pnl, 2),
                'day_pnl_pct': round(day_pnl_pct, 2),
                'last_update': datetime.now().isoformat()
            }
            
            # Add contract-specific details
            if position_type in ['call', 'put']:
                position['contract_details'] = {
                    'strike': float(contract.strike) if hasattr(contract, 'strike') else 0,
                    'expiry': self._format_expiry(contract.lastTradeDateOrContractMonth) if hasattr(contract, 'lastTradeDateOrContractMonth') else '',
                    'right': contract.right if hasattr(contract, 'right') else '',
                    'exchange': contract.primaryExchange,
                    'multiplier': contract.multiplier if hasattr(contract, 'multiplier') else '100'
                }
                position['greeks'] = {}  # Will be populated by market service
                
            elif position_type == 'future':
                position['contract_details'] = {
                    'expiry': self._format_expiry(contract.lastTradeDateOrContractMonth) if hasattr(contract, 'lastTradeDateOrContractMonth') else '',
                    'exchange': contract.exchange,
                    'multiplier': contract.multiplier if hasattr(contract, 'multiplier') else '1'
                }
            
            return position
            
        except Exception as e:
            log_error(self.logger, e, "Error creating position data")
            return None
    
    def _get_position_type(self, contract) -> str:
        """Determine position type from contract"""
        if contract.secType == 'STK':
            return 'stock'
        elif contract.secType == 'OPT':
            if hasattr(contract, 'right'):
                return 'call' if contract.right.upper() == 'C' else 'put'
            return 'option'
        elif contract.secType == 'FUT':
            return 'future'
        else:
            return 'other'
    
    def _generate_position_id(self, contract, account: str) -> str:
        """Generate unique position ID"""
        key_parts = [account, contract.symbol, contract.secType]
        
        if hasattr(contract, 'strike') and contract.strike:
            key_parts.append(str(contract.strike))
        
        if hasattr(contract, 'right') and contract.right:
            key_parts.append(contract.right)
        
        if hasattr(contract, 'lastTradeDateOrContractMonth') and contract.lastTradeDateOrContractMonth:
            key_parts.append(contract.lastTradeDateOrContractMonth)
        
        key_string = '_'.join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()[:12]
    
    def _format_expiry(self, expiry_raw: str) -> str:
        """Format expiry date"""
        try:
            if len(expiry_raw) == 8 and expiry_raw.isdigit():
                year = expiry_raw[:4]
                month = expiry_raw[4:6]
                day = expiry_raw[6:8]
                return f"{year}-{month}-{day}"
            return expiry_raw
        except:
            return expiry_raw
    
    def get_positions(self) -> List[Dict]:
        """Get all current positions"""
        return list(self.position_cache.values())
    
    def get_position_by_id(self, position_id: str) -> Dict:
        """Get specific position by ID"""
        return self.position_cache.get(position_id, {})
    
    def get_account_summary(self) -> Dict:
        """Get account summary"""
        return self.account_cache.copy()
    
    def is_running(self) -> bool:
        """Check if service is running"""
        return self.running and (self.service_thread is not None and self.service_thread.is_alive())
    
    def force_refresh(self):
        """Force refresh positions"""
        try:
            self._request_positions_update()
            self.logger.info("Forced position refresh")
        except Exception as e:
            log_error(self.logger, e, "Error forcing position refresh")