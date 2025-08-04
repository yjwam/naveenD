import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from collections import defaultdict, deque
from dataclasses import asdict

from config.settings import settings
from utils.logger import get_logger
from models.market_data import MarketData, IndexData, Greeks, MarketDataType
from models.portfolio import Portfolio, Position, AccountType, PositionType, Signal, Priority
from models.dashboard import DashboardData, Alert, AlertType, AlertLevel, SystemStatus, PerformanceMetrics, RiskMetrics
from utils.calculations import OptionsCalculator, PortfolioCalculator, StrategyAnalyzer


class DataManager:
    """Centralized data management and processing"""
    
    def __init__(self):
        self.logger = get_logger("data_manager")
        
        # Thread-safe data storage
        self._lock = threading.RLock()
        
        # Market data storage
        self.market_data: Dict[str, MarketData] = {}
        self.greeks_data: Dict[str, Greeks] = {}
        self.historical_data: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        
        # Portfolio data
        self.portfolios: Dict[str, Portfolio] = {}
        self.account_data: Dict[str, Dict] = {}
        
        # System data
        self.alerts: List[Alert] = []
        self.system_status = SystemStatus()
        
        # Performance tracking
        self.performance_data: Dict[str, deque] = defaultdict(lambda: deque(maxlen=252))  # 1 year of daily data
        
        # Request ID mappings
        self.req_id_to_symbol: Dict[int, str] = {}
        self.symbol_to_req_id: Dict[str, int] = {}
        
        # Update frequencies
        self.last_portfolio_update = datetime.now()
        self.last_market_update = datetime.now()
        self.last_greeks_update = datetime.now()
        
        # Market indices mapping
        self.index_symbols = {
            'SPY': 'SPY',
            'QQQ': 'QQQ', 
            'NASDAQ': '^IXIC',
            'VIX': 'VIX',
            'DXY': 'DX-Y.NYB',
            '10Y': '^TNX'
        }
        
        self.logger.info("DataManager initialized")
    
    def update_market_data(self, symbol: str, price_data: Dict[str, Any]) -> None:
        """Update market data for a symbol"""
        with self._lock:
            try:
                # Get existing data or create new
                existing_data = self.market_data.get(symbol)
                
                # Extract price information
                current_price = price_data.get('last_price', 0)
                bid = price_data.get('bid', 0)
                ask = price_data.get('ask', 0)
                volume = price_data.get('volume', 0)
                high = price_data.get('high', 0)
                low = price_data.get('low', 0)
                close = price_data.get('close', 0)
                
                # Calculate change and change percent
                change = 0
                change_percent = 0
                if existing_data and existing_data.close > 0:
                    change = current_price - existing_data.close
                    change_percent = (change / existing_data.close) * 100
                elif close > 0:
                    change = current_price - close
                    change_percent = (change / close) * 100
                
                # Determine data type
                data_type = MarketDataType.INDEX if symbol in self.index_symbols.values() else MarketDataType.STOCK
                
                # Create market data object
                market_data = MarketData(
                    symbol=symbol,
                    price=current_price,
                    bid=bid,
                    ask=ask,
                    volume=volume,
                    high=high,
                    low=low,
                    close=close,
                    change=change,
                    change_percent=change_percent,
                    timestamp=datetime.now(),
                    data_type=data_type
                )
                
                self.market_data[symbol] = market_data
                
                # Store historical data
                self.historical_data[symbol].append({
                    'timestamp': market_data.timestamp,
                    'price': current_price,
                    'volume': volume
                })
                
                self.last_market_update = datetime.now()
                
                self.logger.debug(f"Updated market data for {symbol}: ${current_price:.2f}")
                
            except Exception as e:
                self.logger.error(f"Error updating market data for {symbol}: {e}")
    
    def update_greeks_data(self, symbol: str, greeks_data: Dict[str, Any]) -> None:
        """Update Greeks data for an option - FIXED"""
        with self._lock:
            try:
                # Create Greeks object
                greeks = Greeks(
                    delta=float(greeks_data.get('delta', 0)),
                    gamma=float(greeks_data.get('gamma', 0)),
                    theta=float(greeks_data.get('theta', 0)),
                    vega=float(greeks_data.get('vega', 0)),
                    rho=float(greeks_data.get('rho', 0)),
                    implied_volatility=float(greeks_data.get('implied_vol', 0)),
                    timestamp=datetime.now()
                )
                
                # Store in greeks data cache
                self.greeks_data[symbol] = greeks
                
                # Update all matching option positions with these Greeks
                for portfolio in self.portfolios.values():
                    for position in portfolio.positions:
                        if (position.symbol == symbol and 
                            position.position_type in [PositionType.CALL, PositionType.PUT]):
                            position.greeks = greeks
                            position.updated_at = datetime.now()
                
                self.last_greeks_update = datetime.now()
                
                self.logger.info(f"Updated Greeks for {symbol}: Δ={greeks.delta:.3f}, Γ={greeks.gamma:.3f}")
                
            except Exception as e:
                self.logger.error(f"Error updating Greeks for {symbol}: {e}")
    
    def update_position(self, account_id: str, position_data: Dict[str, Any]) -> None:
        """Update portfolio position - FIXED for proper IBKR data handling"""
        with self._lock:
            try:
                contract = position_data['contract']
                symbol = contract.symbol
                position_quantity = float(position_data.get('position', 0))
                
                self.logger.info(f"Processing position update: {symbol}, secType: {contract.secType}, position: {position_quantity}")
                
                # Handle zero positions (closed positions) - REMOVE from portfolio
                if position_quantity == 0:
                    self.logger.info(f"Position closed for {symbol}, removing from portfolio")
                    self._remove_closed_position(account_id, contract)
                    return
                
                # Determine account type
                account_type = AccountType.INDIVIDUAL_TAXABLE  # Default
                if 'retirement' in account_id.lower() or 'ira' in account_id.lower():
                    account_type = AccountType.RETIREMENT_TAX_FREE
                
                # Create unique position based on IBKR contract data
                position = self._create_position_from_contract(
                    account_id, account_type, contract, position_data
                )
                
                if position:
                    # Get or create portfolio
                    if account_id not in self.portfolios:
                        self.portfolios[account_id] = Portfolio(
                            account_id=account_id,
                            account_type=account_type,
                            positions=[]
                        )
                    
                    # Add/update position in portfolio (fixed logic will handle this)
                    self.portfolios[account_id].add_position(position)
                    self.last_portfolio_update = datetime.now()
                    
                    position_desc = self._get_position_description(position)
                    self.logger.info(f"Updated position: {position_desc} for account {account_id}")
                    
            except Exception as e:
                self.logger.error(f"Error updating position: {e}")
                import traceback
                self.logger.error(traceback.format_exc())

    def _create_position_from_contract(self, account_id: str, account_type: AccountType, 
                                 contract, position_data: Dict[str, Any]) -> Optional[Position]:
        """Create position object from IBKR contract data - FIXED"""
        try:
            symbol = contract.symbol
            position_quantity = float(position_data.get('position', 0))
            avg_cost = float(position_data.get('average_cost', 0))
            market_price = float(position_data.get('market_price', 0))
            market_value = float(position_data.get('market_value', 0))
            unrealized_pnl = float(position_data.get('unrealized_pnl', 0))
            realized_pnl = float(position_data.get('realized_pnl', 0))
            
            # Determine position type and parse contract details
            position_type = PositionType.STOCK
            option_type = None
            strike_price = None
            expiry = None
            
            if contract.secType == 'OPT':
                if hasattr(contract, 'right') and contract.right:
                    if contract.right.upper() == 'C':
                        position_type = PositionType.CALL
                        option_type = 'C'
                    elif contract.right.upper() == 'P':
                        position_type = PositionType.PUT
                        option_type = 'P'
                
                strike_price = float(contract.strike) if hasattr(contract, 'strike') else None
                
                # Parse expiry date properly for options
                if hasattr(contract, 'lastTradeDateOrContractMonth') and contract.lastTradeDateOrContractMonth:
                    expiry_raw = contract.lastTradeDateOrContractMonth
                    expiry = self._format_expiry_date(expiry_raw)
                    
            elif contract.secType == 'FUT':
                position_type = PositionType.FUTURE
                if hasattr(contract, 'lastTradeDateOrContractMonth') and contract.lastTradeDateOrContractMonth:
                    expiry = contract.lastTradeDateOrContractMonth
            
            # Get current market price if not provided
            if market_price == 0:
                market_data = self.market_data.get(symbol)
                if market_data:
                    market_price = market_data.price
                else:
                    market_price = avg_cost  # Fallback
            
            # Calculate market value and P&L if not provided correctly
            if market_value == 0:
                if position_type == PositionType.STOCK:
                    market_value = position_quantity * market_price
                else:  # Options/Futures
                    market_value = position_quantity * market_price * 100
            
            if unrealized_pnl == 0:
                if position_type == PositionType.STOCK:
                    unrealized_pnl = (market_price - avg_cost) * position_quantity
                else:  # Options/Futures
                    unrealized_pnl = (market_price - avg_cost) * position_quantity * 100
            
            # Create position object
            position = Position(
                symbol=symbol,
                account_id=account_id,
                account_type=account_type,
                position_type=position_type,
                quantity=int(position_quantity),
                avg_cost=avg_cost,
                current_price=market_price,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                realized_pnl=realized_pnl,
                day_pnl=0.0,  # Will be calculated
                strike_price=strike_price,
                expiry=expiry,
                option_type=option_type,
                greeks=None,  # Will be updated separately
                strategy="Complex Strategy",
                confidence=50,
                signal=Signal.HOLD,
                priority=Priority.MEDIUM,
                notes="",
                levels={}
            )
            
            return position
            
        except Exception as e:
            self.logger.error(f"Error creating position from contract: {e}")
            return None

    def _format_expiry_date(self, expiry_raw: str) -> str:
        """Format expiry date from IBKR format to readable format"""
        try:
            # Convert IBKR format (YYYYMMDD) to MM/DD/YYYY
            if len(expiry_raw) == 8 and expiry_raw.isdigit():
                year = expiry_raw[:4]
                month = expiry_raw[4:6]
                day = expiry_raw[6:8]
                return f"{month}/{day}/{year}"
            else:
                return expiry_raw
        except:
            return expiry_raw

    def _remove_closed_position(self, account_id: str, contract) -> None:
        """Remove a closed position from portfolio - FIXED"""
        try:
            if account_id not in self.portfolios:
                return
            
            portfolio = self.portfolios[account_id]
            symbol = contract.symbol
            
            # Create a dummy position to use matching logic
            dummy_position = Position(
                symbol=symbol,
                account_id=account_id,
                account_type=portfolio.account_type,
                position_type=PositionType.STOCK,  # Will be corrected below
                quantity=0,
                avg_cost=0,
                current_price=0,
                market_value=0,
                unrealized_pnl=0
            )
            
            # Set correct position type and details
            if contract.secType == 'OPT':
                if hasattr(contract, 'right') and contract.right:
                    if contract.right.upper() == 'C':
                        dummy_position.position_type = PositionType.CALL
                        dummy_position.option_type = 'C'
                    elif contract.right.upper() == 'P':
                        dummy_position.position_type = PositionType.PUT
                        dummy_position.option_type = 'P'
                
                dummy_position.strike_price = float(contract.strike) if hasattr(contract, 'strike') else None
                if hasattr(contract, 'lastTradeDateOrContractMonth'):
                    dummy_position.expiry = self._format_expiry_date(contract.lastTradeDateOrContractMonth)
                    
            elif contract.secType == 'FUT':
                dummy_position.position_type = PositionType.FUTURE
                if hasattr(contract, 'lastTradeDateOrContractMonth'):
                    dummy_position.expiry = contract.lastTradeDateOrContractMonth
            
            # Find and remove matching position
            for i, existing_pos in enumerate(portfolio.positions):
                if portfolio._positions_match(existing_pos, dummy_position):
                    removed_pos = portfolio.positions.pop(i)
                    self.logger.info(f"Removed closed position: {self._get_position_description(removed_pos)}")
                    portfolio.calculate_totals()
                    return
                    
            self.logger.warning(f"Could not find position to remove for {symbol}")
            
        except Exception as e:
            self.logger.error(f"Error removing closed position for {symbol}: {e}")

    def _get_position_description(self, position: Position) -> str:
        """Get human-readable position description"""
        desc = f"{position.symbol}"
        if position.position_type != PositionType.STOCK:
            desc += f" {position.expiry} ${position.strike_price} {position.option_type}"
        desc += f" (qty: {position.quantity})"
        return desc

    def update_account_value(self, account_id: str, key: str, value: str, currency: str) -> None:
        """Update account value"""
        with self._lock:
            try:
                if account_id not in self.account_data:
                    self.account_data[account_id] = {}
                
                self.account_data[account_id][key] = {
                    'value': value,
                    'currency': currency,
                    'timestamp': datetime.now()
                }
                
                # Update portfolio with account data
                if account_id in self.portfolios:
                    portfolio = self.portfolios[account_id]
                    
                    if key == 'CashBalance':
                        portfolio.cash_balance = float(value)
                    elif key == 'BuyingPower':
                        portfolio.buying_power = float(value)
                    elif key == 'GrossPositionValue':
                        pass  # Will be calculated from positions
                    
                    # Recalculate totals
                    portfolio.calculate_totals()
                
                self.logger.debug(f"Updated account value {key}={value} for {account_id}")
                
            except Exception as e:
                self.logger.error(f"Error updating account value: {e}")
    
    def get_index_data(self) -> IndexData:
        """Get market indices data"""
        with self._lock:
            return IndexData(
                spy=self.market_data.get('SPY'),
                qqq=self.market_data.get('QQQ'),
                nasdaq=self.market_data.get('^IXIC'),
                vix=self.market_data.get('VIX'),
                dxy=self.market_data.get('DX-Y.NYB'),
                ten_year=self.market_data.get('^TNX')
            )
    
    def get_dashboard_data(self) -> DashboardData:
        """Get complete dashboard data"""
        with self._lock:
            try:
                # Calculate performance metrics
                performance_metrics = self._calculate_performance_metrics()
                
                # Calculate risk metrics
                risk_metrics = self._calculate_risk_metrics()
                
                # Update system status
                self._update_system_status()
                
                dashboard_data = DashboardData(
                    timestamp=datetime.now(),
                    market_indices=self.get_index_data(),
                    portfolios=self.portfolios.copy(),
                    alerts=self.get_active_alerts(),
                    performance_metrics=performance_metrics,
                    risk_metrics=risk_metrics,
                    system_status=self.system_status
                )
                
                return dashboard_data
                
            except Exception as e:
                self.logger.error(f"Error generating dashboard data: {e}")
                return self._get_empty_dashboard_data()
    
    def add_alert(self, alert_type: AlertType, level: AlertLevel, title: str, 
                  message: str, symbol: str = None, account_id: str = None,
                  value: float = None, threshold: float = None) -> None:
        """Add system alert"""
        with self._lock:
            alert = Alert(
                id=f"{alert_type.value}_{int(time.time())}",
                type=alert_type,
                level=level,
                title=title,
                message=message,
                symbol=symbol,
                account_id=account_id,
                value=value,
                threshold=threshold
            )
            
            self.alerts.append(alert)
            
            # Keep only recent alerts
            if len(self.alerts) > settings.alerts.max_alerts:
                self.alerts = self.alerts[-settings.alerts.max_alerts:]
            
            self.logger.info(f"Added alert: {title}")
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts"""
        with self._lock:
            now = datetime.now()
            return [alert for alert in self.alerts 
                   if not alert.acknowledged and 
                   (alert.expires_at is None or alert.expires_at > now)]
    
    def _calculate_performance_metrics(self) -> PerformanceMetrics:
        """Calculate portfolio performance metrics"""
        # Placeholder implementation
        return PerformanceMetrics()
    
    def _calculate_risk_metrics(self) -> RiskMetrics:
        """Calculate portfolio risk metrics"""
        total_delta = 0
        total_gamma = 0
        total_theta = 0
        total_vega = 0
        
        for portfolio in self.portfolios.values():
            portfolio_greeks = PortfolioCalculator.calculate_portfolio_greeks(portfolio.positions)
            total_delta += portfolio_greeks.delta
            total_gamma += portfolio_greeks.gamma
            total_theta += portfolio_greeks.theta
            total_vega += portfolio_greeks.vega
        
        return RiskMetrics(
            portfolio_delta=total_delta,
            portfolio_gamma=total_gamma,
            portfolio_theta=total_theta,
            portfolio_vega=total_vega
        )
    
    def _update_system_status(self) -> None:
        """Update system status"""
        total_positions = sum(len(p.positions) for p in self.portfolios.values())
        active_alerts = len(self.get_active_alerts())
        
        self.system_status.total_positions = total_positions
        self.system_status.active_alerts = active_alerts
        self.system_status.last_market_data_update = self.last_market_update
        self.system_status.last_portfolio_update = self.last_portfolio_update
    
    def _get_empty_dashboard_data(self) -> DashboardData:
        """Get empty dashboard data as fallback"""
        return DashboardData(
            timestamp=datetime.now(),
            market_indices=IndexData(),
            portfolios={},
            alerts=[],
            performance_metrics=PerformanceMetrics(),
            risk_metrics=RiskMetrics(),
            system_status=SystemStatus()
        )
    
    def cleanup_old_data(self) -> None:
        """Clean up old historical data"""
        with self._lock:
            cutoff_time = datetime.now() - timedelta(days=settings.data.history_retention_days)
            
            # Clean up alerts
            self.alerts = [alert for alert in self.alerts 
                          if alert.created_at > cutoff_time]
            
            self.logger.info("Cleaned up old data")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get data manager statistics"""
        with self._lock:
            return {
                'market_data_symbols': len(self.market_data),
                'total_portfolios': len(self.portfolios),
                'total_positions': sum(len(p.positions) for p in self.portfolios.values()),
                'active_alerts': len(self.get_active_alerts()),
                'last_market_update': self.last_market_update.isoformat(),
                'last_portfolio_update': self.last_portfolio_update.isoformat(),
                'memory_usage_mb': sum(len(str(data)) for data in [
                    self.market_data, self.portfolios, self.alerts
                ]) / 1024 / 1024
            }
        
    def debug_positions(self) -> None:
        """Debug method to check positions"""
        with self._lock:
            self.logger.info(f"=== PORTFOLIO DEBUG ===")
            self.logger.info(f"Total portfolios: {len(self.portfolios)}")
            
            for account_id, portfolio in self.portfolios.items():
                self.logger.info(f"Account {account_id}: {len(portfolio.positions)} positions")
                
                for i, position in enumerate(portfolio.positions):
                    pos_info = f"  {i+1}. {position.symbol} {position.position_type.value}"
                    if position.position_type != PositionType.STOCK:
                        pos_info += f" ${position.strike_price} {position.option_type} {position.expiry}"
                    pos_info += f" qty:{position.quantity} mv:${position.market_value:.2f}"
                    self.logger.info(pos_info)

        def _remove_closed_position(self, account_id: str, contract) -> None:
            """Remove a closed position from portfolio"""
            try:
                if account_id not in self.portfolios:
                    return
                
                portfolio = self.portfolios[account_id]
                symbol = contract.symbol
                
                # For stocks: match by symbol only
                if contract.secType == 'STK':
                    for i, position in enumerate(portfolio.positions):
                        if (position.symbol == symbol and 
                            position.position_type == PositionType.STOCK):
                            removed_pos = portfolio.positions.pop(i)
                            self.logger.info(f"Removed closed stock position: {symbol}")
                            portfolio.calculate_totals()
                            return
                
                # For options: match by symbol, strike, expiry, and type
                elif contract.secType == 'OPT':
                    option_type = None
                    if hasattr(contract, 'right') and contract.right:
                        option_type = contract.right.upper()
                    
                    strike_price = float(contract.strike) if hasattr(contract, 'strike') else None
                    
                    # Parse expiry
                    expiry = None
                    if hasattr(contract, 'lastTradeDateOrContractMonth') and contract.lastTradeDateOrContractMonth:
                        expiry_raw = contract.lastTradeDateOrContractMonth
                        if len(expiry_raw) == 8 and expiry_raw.isdigit():
                            year = expiry_raw[:4]
                            month = expiry_raw[4:6] 
                            day = expiry_raw[6:8]
                            expiry = f"{month}/{day}/{year}"
                        else:
                            expiry = expiry_raw
                    
                    for i, position in enumerate(portfolio.positions):
                        if (position.symbol == symbol and 
                            position.position_type != PositionType.STOCK and
                            position.strike_price == strike_price and
                            position.expiry == expiry and
                            position.option_type == option_type):
                            removed_pos = portfolio.positions.pop(i)
                            self.logger.info(f"Removed closed option position: {symbol} {expiry} ${strike_price} {option_type}")
                            portfolio.calculate_totals()
                            return
                
                # For futures and other instruments
                else:
                    for i, position in enumerate(portfolio.positions):
                        if position.symbol == symbol:
                            # Additional matching for futures
                            if (hasattr(contract, 'lastTradeDateOrContractMonth') and 
                                contract.lastTradeDateOrContractMonth):
                                if position.expiry == contract.lastTradeDateOrContractMonth:
                                    removed_pos = portfolio.positions.pop(i)
                                    self.logger.info(f"Removed closed {contract.secType} position: {symbol}")
                                    portfolio.calculate_totals()
                                    return
                            else:
                                # No expiry matching needed
                                removed_pos = portfolio.positions.pop(i)
                                self.logger.info(f"Removed closed {contract.secType} position: {symbol}")
                                portfolio.calculate_totals()
                                return
                                
            except Exception as e:
                self.logger.error(f"Error removing closed position for {symbol}: {e}")

        def cleanup_empty_portfolios(self) -> None:
            """Remove portfolios with no positions"""
            try:
                empty_accounts = []
                for account_id, portfolio in self.portfolios.items():
                    if len(portfolio.positions) == 0 and portfolio.cash_balance == 0:
                        empty_accounts.append(account_id)
                
                for account_id in empty_accounts:
                    del self.portfolios[account_id]
                    self.logger.info(f"Removed empty portfolio for account {account_id}")
                    
            except Exception as e:
                self.logger.error(f"Error cleaning up empty portfolios: {e}")