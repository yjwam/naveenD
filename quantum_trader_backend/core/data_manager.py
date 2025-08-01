import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from collections import defaultdict, deque
from dataclasses import asdict

from config.settings import settings
from utils.logger import get_logger
from models.market_data import MarketData, IndexData, Greeks, MarketDataType
from models.portfolio import Portfolio, Position, AccountType, PositionType
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
        """Update Greeks data for an option"""
        with self._lock:
            try:
                greeks = Greeks(
                    delta=greeks_data.get('delta', 0),
                    gamma=greeks_data.get('gamma', 0),
                    theta=greeks_data.get('theta', 0),
                    vega=greeks_data.get('vega', 0),
                    rho=greeks_data.get('rho', 0),
                    implied_volatility=greeks_data.get('implied_vol', 0),
                    timestamp=datetime.now()
                )
                
                self.greeks_data[symbol] = greeks
                self.last_greeks_update = datetime.now()
                
                self.logger.debug(f"Updated Greeks for {symbol}: Δ={greeks.delta:.3f}")
                
            except Exception as e:
                self.logger.error(f"Error updating Greeks for {symbol}: {e}")
    
    def update_position(self, account_id: str, position_data: Dict[str, Any]) -> None:
        """Update portfolio position"""
        with self._lock:
            try:
                contract = position_data['contract']
                symbol = contract.symbol
                
                # Determine account type
                account_type = AccountType.INDIVIDUAL_TAXABLE  # Default
                if 'retirement' in account_id.lower() or 'ira' in account_id.lower():
                    account_type = AccountType.RETIREMENT_TAX_FREE
                
                # Determine position type
                position_type = PositionType.STOCK
                if contract.secType == 'OPT':
                    position_type = PositionType.CALL if contract.right == 'C' else PositionType.PUT
                
                # Get current market price
                current_price = position_data.get('market_price', 0)
                if current_price == 0 and symbol in self.market_data:
                    current_price = self.market_data[symbol].price
                
                # Get Greeks if available for options
                greeks = None
                if position_type != PositionType.STOCK:
                    option_symbol = f"{symbol}_{contract.strike}_{contract.expiry}_{contract.right}"
                    greeks = self.greeks_data.get(option_symbol)
                
                # Create position object
                position = Position(
                    symbol=symbol,
                    account_id=account_id,
                    account_type=account_type,
                    position_type=position_type,
                    quantity=int(position_data['position']),
                    avg_cost=position_data.get('average_cost', 0),
                    current_price=current_price,
                    market_value=position_data.get('market_value', 0),
                    unrealized_pnl=position_data.get('unrealized_pnl', 0),
                    realized_pnl=position_data.get('realized_pnl', 0),
                    strike_price=contract.strike if hasattr(contract, 'strike') else None,
                    expiry=contract.lastTradeDateOrContractMonth if hasattr(contract, 'lastTradeDateOrContractMonth') else None,
                    option_type=contract.right if hasattr(contract, 'right') else None,
                    greeks=greeks
                )
                
                # Analyze strategy
                if account_id in self.portfolios:
                    portfolio_positions = self.portfolios[account_id].positions
                    position.strategy = StrategyAnalyzer.identify_strategy(
                        portfolio_positions + [position], symbol
                    )
                
                # Get or create portfolio
                if account_id not in self.portfolios:
                    self.portfolios[account_id] = Portfolio(
                        account_id=account_id,
                        account_type=account_type,
                        positions=[]
                    )
                
                # Add/update position in portfolio
                self.portfolios[account_id].add_position(position)
                
                self.last_portfolio_update = datetime.now()
                
                self.logger.debug(f"Updated position {symbol} for account {account_id}")
                
            except Exception as e:
                self.logger.error(f"Error updating position: {e}")
    
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