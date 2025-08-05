import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional
from ibapi.contract import Contract

from config.settings import settings
from utils.logger import get_logger
from utils.calculations import OptionsCalculator, StrategyAnalyzer, time_to_expiry
from core.ibkr_client import IBKRClient
from core.data_manager import DataManager
from models.portfolio import PositionType, Position
from models.market_data import Greeks

class OptionsService:
    """Service for managing options-specific data and calculations"""
    
    def __init__(self, ibkr_client: IBKRClient, data_manager: DataManager):
        self.ibkr_client = ibkr_client
        self.data_manager = data_manager
        self.logger = get_logger("options_service")
        
        # Service state
        self.running = False
        self.service_thread = None
        
        # Options tracking
        self.options_positions: Dict[str, Dict] = {}  # symbol -> option details
        self.greeks_subscriptions: Dict[str, int] = {}  # option_key -> req_id
        
        # Update timing
        self.last_greeks_update = datetime.min
        self.greeks_update_interval = settings.data.greeks_update_frequency
        
        # Risk-free rate (approximate)
        self.risk_free_rate = 0.05  # 5% - should be updated from market data
        
        self.logger.info("Options service initialized")
    
    def start(self) -> None:
        """Start the options service"""
        if self.running:
            self.logger.warning("Options service already running")
            return
        
        self.running = True
        self.service_thread = threading.Thread(target=self._run_service, daemon=True)
        self.service_thread.start()
        
        self.logger.info("Options service started")
    
    def stop(self) -> None:
        """Stop the options service"""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel all Greeks subscriptions
        self._cancel_all_greeks_subscriptions()
        
        if self.service_thread and self.service_thread.is_alive():
            self.service_thread.join(timeout=5)
        
        self.logger.info("Options service stopped")
    
    def _run_service(self) -> None:
        """Main service loop"""
        try:
            while self.running:
                try:
                    current_time = datetime.now()
                    
                    # Update options positions from portfolios
                    self._update_options_positions()
                    
                    # Request Greeks updates periodically
                    if (current_time - self.last_greeks_update).total_seconds() >= self.greeks_update_interval:
                        self._request_greeks_updates()
                        self.last_greeks_update = current_time
                    
                    # Calculate synthetic Greeks for positions without live data
                    self._calculate_synthetic_greeks()
                    
                    # Update strategy classifications
                    self._update_strategy_classifications()
                    
                    # Check for expiration alerts
                    self._check_expiration_alerts()
                    
                    # Sleep between checks
                    time.sleep(0.1)
                    
                except Exception as e:
                    self.logger.error(f"Error in options service loop: {e}")
                    time.sleep(5)
                    
        except Exception as e:
            self.logger.error(f"Fatal error in options service: {e}")
        finally:
            self._cleanup()
    
    def _update_options_positions(self) -> None:
        """Update tracking of options positions from portfolios"""
        try:
            current_options = {}
            
            for portfolio in self.data_manager.portfolios.values():
                for position in portfolio.positions:
                    if position.position_type in [PositionType.CALL, PositionType.PUT]:
                        option_key = self._get_option_key(position)
                        current_options[option_key] = {
                            'position': position,
                            'symbol': position.symbol,
                            'strike': position.strike_price,
                            'expiry': position.expiry,
                            'option_type': position.option_type,
                            'quantity': position.quantity
                        }
            
            # Update tracking
            self.options_positions = current_options
            
        except Exception as e:
            self.logger.error(f"Error updating options positions: {e}")
    
    def _request_greeks_updates(self) -> None:
        """Request Greeks updates for all options positions - FIXED"""
        if not self.ibkr_client.ensure_connection():
            return
        
        try:
            for option_key, option_data in self.options_positions.items():
                if option_key not in self.greeks_subscriptions:
                    position = option_data['position']
                    
                    # Create proper option contract
                    contract = self._create_proper_option_contract(position)
                    
                    if contract:
                        # Request option computation (Greeks) - not market data
                        req_id = self.ibkr_client.request_option_computation(
                            position.symbol, contract
                        )
                        
                        if req_id != -1:
                            self.greeks_subscriptions[option_key] = req_id
                            self.logger.info(f"Requested Greeks for {option_key}")
                            
                            # Add delay between requests
                            time.sleep(0.5)
                
        except Exception as e:
            self.logger.error(f"Error requesting Greeks updates: {e}")

    def _create_proper_option_contract(self, position: Position) -> Optional[Contract]:
        """Create proper option contract for Greeks subscription - FIXED"""
        try:
            if position.position_type not in [PositionType.CALL, PositionType.PUT]:
                return None
            
            # Format expiry for IBKR (remove slashes, ensure YYYYMMDD format)
            expiry_formatted = position.expiry
            if expiry_formatted and '/' in expiry_formatted:
                # Convert MM/DD/YYYY to YYYYMMDD
                parts = expiry_formatted.split('/')
                if len(parts) == 3:
                    expiry_formatted = f"{parts[2]}{parts[0].zfill(2)}{parts[1].zfill(2)}"
            
            contract = Contract()
            contract.symbol = position.symbol
            contract.secType = "OPT"
            contract.exchange = "SMART"
            contract.currency = "USD"
            contract.lastTradeDateOrContractMonth = expiry_formatted
            contract.strike = position.strike_price
            contract.right = position.option_type  # 'C' or 'P'
            contract.multiplier = "100"
            
            return contract
            
        except Exception as e:
            self.logger.error(f"Error creating option contract for Greeks: {e}")
            return None
    
    def _calculate_synthetic_greeks(self) -> None:
        """Calculate synthetic Greeks for positions without live data"""
        try:
            for option_key, option_data in self.options_positions.items():
                position = option_data['position']
                
                # Skip if we already have live Greeks
                if position.greeks and position.greeks.timestamp:
                    time_since_update = (datetime.now() - position.greeks.timestamp).total_seconds()
                    if time_since_update < 300:  # Less than 5 minutes old
                        continue
                
                # Get underlying price
                underlying_price = 0
                market_data = self.data_manager.market_data.get(position.symbol)
                if market_data:
                    underlying_price = market_data.price
                
                if underlying_price > 0 and position.strike_price and position.expiry:
                    # Calculate time to expiry
                    time_to_exp = time_to_expiry(position.expiry)
                    
                    if time_to_exp > 0:
                        # Estimate implied volatility (or use default)
                        implied_vol = 0.25  # Default 25%
                        if position.greeks and position.greeks.implied_volatility:
                            implied_vol = position.greeks.implied_volatility
                        
                        # Calculate synthetic Greeks
                        synthetic_greeks = OptionsCalculator.calculate_greeks(
                            S=underlying_price,
                            K=position.strike_price,
                            T=time_to_exp,
                            r=self.risk_free_rate,
                            sigma=implied_vol,
                            option_type=position.option_type
                        )
                        
                        # Update position with synthetic Greeks
                        position.greeks = synthetic_greeks
                        
                        self.logger.debug(f"Calculated synthetic Greeks for {option_key}")
            
        except Exception as e:
            self.logger.error(f"Error calculating synthetic Greeks: {e}")
    
    def _update_strategy_classifications(self) -> None:
        """Update strategy classifications for positions"""
        try:
            for portfolio in self.data_manager.portfolios.values():
                # Group positions by underlying symbol
                symbol_positions = {}
                for position in portfolio.positions:
                    symbol = position.symbol
                    if symbol not in symbol_positions:
                        symbol_positions[symbol] = []
                    symbol_positions[symbol].append(position)
                
                # Classify strategies for each underlying
                for symbol, positions in symbol_positions.items():
                    strategy = StrategyAnalyzer.identify_strategy(positions, symbol)
                    
                    # Update strategy for relevant positions
                    for position in positions:
                        if position.position_type != PositionType.STOCK:
                            position.strategy = strategy
            
        except Exception as e:
            self.logger.error(f"Error updating strategy classifications: {e}")
    
    def _check_expiration_alerts(self) -> None:
        """Check for options approaching expiration"""
        try:
            warning_days = settings.alerts.risk_thresholds.get('days_to_expiry_warning', 7)
            
            for option_data in self.options_positions.values():
                position = option_data['position']
                
                if position.expiry:
                    days_to_expiry = time_to_expiry(position.expiry) * 365
                    
                    if 0 < days_to_expiry <= warning_days:
                        # Create expiration alert
                        from models.dashboard import AlertType, AlertLevel
                        
                        self.data_manager.add_alert(
                            alert_type=AlertType.EXPIRATION,
                            level=AlertLevel.WARNING,
                            title=f"{position.symbol} Option Expiring Soon",
                            message=f"{position.symbol} {position.option_type} ${position.strike_price} expires in {days_to_expiry:.0f} days",
                            symbol=position.symbol,
                            account_id=position.account_id,
                            value=days_to_expiry,
                            threshold=warning_days
                        )
            
        except Exception as e:
            self.logger.error(f"Error checking expiration alerts: {e}")
    
    def _create_option_contract(self, option_data: Dict) -> Optional[Contract]:
        """Create option contract from position data"""
        try:
            return self.ibkr_client.create_option_contract(
                symbol=option_data['symbol'],
                expiry=option_data['expiry'].replace('/', ''),  # Remove slashes for IBKR format
                strike=float(option_data['strike']),
                right=option_data['option_type']
            )
        except Exception as e:
            self.logger.error(f"Error creating option contract: {e}")
            return None
    
    def _get_option_key(self, position) -> str:
        """Generate unique key for option position"""
        return f"{position.symbol}_{position.strike_price}_{position.expiry}_{position.option_type}"
    
    def _cancel_all_greeks_subscriptions(self) -> None:
        """Cancel all Greeks subscriptions"""
        try:
            for option_key, req_id in self.greeks_subscriptions.items():
                if self.ibkr_client.is_connected():
                    self.ibkr_client.cancel_market_data(req_id)
            
            self.greeks_subscriptions.clear()
            self.logger.info("Cancelled all Greeks subscriptions")
            
        except Exception as e:
            self.logger.error(f"Error cancelling Greeks subscriptions: {e}")
    
    def _cleanup(self) -> None:
        """Cleanup service resources"""
        self._cancel_all_greeks_subscriptions()
        self.logger.info("Options service cleanup completed")
    
    def get_options_summary(self) -> Dict[str, any]:
        """Get summary of all options positions"""
        try:
            total_options = len(self.options_positions)
            calls_count = 0
            puts_count = 0
            total_delta = 0
            total_gamma = 0
            total_theta = 0
            total_vega = 0
            
            expiring_soon = 0
            warning_days = 7
            
            strategies = {}
            
            for option_data in self.options_positions.values():
                position = option_data['position']
                
                # Count by type
                if position.option_type == 'C':
                    calls_count += 1
                else:
                    puts_count += 1
                
                # Sum Greeks
                if position.greeks:
                    multiplier = position.quantity * 100
                    total_delta += position.greeks.delta * multiplier
                    total_gamma += position.greeks.gamma * multiplier
                    total_theta += position.greeks.theta * multiplier
                    total_vega += position.greeks.vega * multiplier
                
                # Check expiration
                if position.expiry:
                    days_to_expiry = time_to_expiry(position.expiry) * 365
                    if 0 < days_to_expiry <= warning_days:
                        expiring_soon += 1
                
                # Count strategies
                strategy = position.strategy or "Unknown"
                strategies[strategy] = strategies.get(strategy, 0) + 1
            
            return {
                'total_options_positions': total_options,
                'calls_count': calls_count,
                'puts_count': puts_count,
                'portfolio_greeks': {
                    'delta': total_delta,
                    'gamma': total_gamma,
                    'theta': total_theta,
                    'vega': total_vega
                },
                'expiring_soon': expiring_soon,
                'strategies': strategies,
                'greeks_subscriptions': len(self.greeks_subscriptions),
                'last_greeks_update': self.last_greeks_update.isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error generating options summary: {e}")
            return {}
    
    def get_option_details(self, symbol: str, strike: float, expiry: str, option_type: str) -> Dict[str, any]:
        """Get detailed information for a specific option"""
        option_key = f"{symbol}_{strike}_{expiry}_{option_type}"
        
        if option_key not in self.options_positions:
            return {}
        
        try:
            option_data = self.options_positions[option_key]
            position = option_data['position']
            
            # Get underlying market data
            underlying_data = self.data_manager.market_data.get(symbol)
            
            result = {
                'option_key': option_key,
                'position_details': position.to_dict(),
                'underlying_data': underlying_data.to_dict() if underlying_data else None,
                'time_to_expiry_days': time_to_expiry(expiry) * 365 if expiry else 0,
                'greeks_subscription': option_key in self.greeks_subscriptions
            }
            
            # Calculate theoretical price if we have underlying price
            if underlying_data and position.strike_price and position.expiry:
                time_to_exp = time_to_expiry(position.expiry)
                if time_to_exp > 0:
                    implied_vol = 0.25
                    if position.greeks:
                        implied_vol = position.greeks.implied_volatility or 0.25
                    
                    if option_type == 'C':
                        theoretical_price = OptionsCalculator.black_scholes_call(
                            underlying_data.price, position.strike_price, 
                            time_to_exp, self.risk_free_rate, implied_vol
                        )
                    else:
                        theoretical_price = OptionsCalculator.black_scholes_put(
                            underlying_data.price, position.strike_price,
                            time_to_exp, self.risk_free_rate, implied_vol
                        )
                    
                    result['theoretical_price'] = theoretical_price
                    result['price_difference'] = position.current_price - theoretical_price
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting option details: {e}")
            return {}
    
    def is_running(self) -> bool:
        """Check if service is running"""
        return self.running and (self.service_thread is not None and self.service_thread.is_alive())
    
    def get_service_stats(self) -> Dict[str, any]:
        """Get service statistics"""
        return {
            'running': self.is_running(),
            'options_positions_tracked': len(self.options_positions),
            'greeks_subscriptions': len(self.greeks_subscriptions),
            'last_greeks_update': self.last_greeks_update.isoformat(),
            'greeks_update_interval': self.greeks_update_interval,
            'risk_free_rate': self.risk_free_rate
        }