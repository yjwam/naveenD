import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Set

from config.settings import settings
from utils.logger import get_logger
from core.data_manager import DataManager
from models.dashboard import Alert, AlertType, AlertLevel
from models.portfolio import PositionType

class AlertsService:
    """Service for monitoring and generating alerts"""
    
    def __init__(self, data_manager: DataManager):
        self.data_manager = data_manager
        self.logger = get_logger("alerts_service")
        
        # Service state
        self.running = False
        self.service_thread = None
        
        # Alert tracking
        self.triggered_alerts: Set[str] = set()  # Track to avoid duplicates
        self.last_alert_check = datetime.min
        
        # Alert thresholds from config
        self.thresholds = settings.alerts.risk_thresholds
        
        self.logger.info("Alerts service initialized")
    
    def start(self) -> None:
        """Start the alerts service"""
        if self.running:
            self.logger.warning("Alerts service already running")
            return
        
        self.running = True
        self.service_thread = threading.Thread(target=self._run_service, daemon=True)
        self.service_thread.start()
        
        self.logger.info("Alerts service started")
    
    def stop(self) -> None:
        """Stop the alerts service"""
        if not self.running:
            return
        
        self.running = False
        
        if self.service_thread and self.service_thread.is_alive():
            self.service_thread.join(timeout=5)
        
        self.logger.info("Alerts service stopped")
    
    def _run_service(self) -> None:
        """Main service loop"""
        try:
            while self.running:
                try:
                    current_time = datetime.now()
                    
                    # Check various alert conditions
                    self._check_position_loss_alerts()
                    self._check_portfolio_loss_alerts()
                    self._check_expiration_alerts()
                    self._check_high_iv_alerts()
                    self._check_liquidity_alerts()
                    self._check_margin_alerts()
                    self._check_system_alerts()
                    
                    # Clean up old triggered alerts
                    self._cleanup_triggered_alerts()
                    
                    self.last_alert_check = current_time
                    
                    # Sleep between checks
                    time.sleep(1)  # Check every 10 seconds
                    
                except Exception as e:
                    self.logger.error(f"Error in alerts service loop: {e}")
                    time.sleep(3)  # Longer sleep on error
                    
        except Exception as e:
            self.logger.error(f"Fatal error in alerts service: {e}")
        finally:
            self._cleanup()
    
    def _check_position_loss_alerts(self) -> None:
        """Check for individual position loss alerts"""
        try:
            max_loss_threshold = self.thresholds.get('max_position_loss', -0.20)
            
            for portfolio in self.data_manager.portfolios.values():
                for position in portfolio.positions:
                    if position.avg_cost > 0:
                        loss_percent = (position.current_price - position.avg_cost) / position.avg_cost
                        
                        if loss_percent <= max_loss_threshold:
                            alert_key = f"position_loss_{position.account_id}_{position.symbol}_{position.strike_price}_{position.expiry}"
                            
                            if alert_key not in self.triggered_alerts:
                                self.data_manager.add_alert(
                                    alert_type=AlertType.PROFIT_LOSS,
                                    level=AlertLevel.CRITICAL,
                                    title=f"Position Loss Alert: {position.symbol}",
                                    message=f"Position {position.symbol} is down {abs(loss_percent)*100:.1f}% from avg cost",
                                    symbol=position.symbol,
                                    account_id=position.account_id,
                                    value=loss_percent,
                                    threshold=max_loss_threshold
                                )
                                
                                self.triggered_alerts.add(alert_key)
                                self.logger.warning(f"Position loss alert triggered for {position.symbol}")
                                
        except Exception as e:
            self.logger.error(f"Error checking position loss alerts: {e}")
    
    def _check_portfolio_loss_alerts(self) -> None:
        """Check for portfolio-level loss alerts"""
        try:
            max_portfolio_loss = self.thresholds.get('max_portfolio_loss', -0.10)
            
            for account_id, portfolio in self.data_manager.portfolios.items():
                if portfolio.total_value > 0:
                    day_loss_percent = portfolio.day_pnl / portfolio.total_value
                    
                    if day_loss_percent <= max_portfolio_loss:
                        alert_key = f"portfolio_loss_{account_id}"
                        
                        if alert_key not in self.triggered_alerts:
                            self.data_manager.add_alert(
                                alert_type=AlertType.RISK,
                                level=AlertLevel.URGENT,
                                title=f"Portfolio Loss Alert",
                                message=f"Portfolio {account_id} is down {abs(day_loss_percent)*100:.1f}% today",
                                account_id=account_id,
                                value=day_loss_percent,
                                threshold=max_portfolio_loss
                            )
                            
                            self.triggered_alerts.add(alert_key)
                            self.logger.warning(f"Portfolio loss alert triggered for {account_id}")
                            
        except Exception as e:
            self.logger.error(f"Error checking portfolio loss alerts: {e}")
    
    def _check_expiration_alerts(self) -> None:
        """Check for options expiration alerts"""
        try:
            warning_days = self.thresholds.get('days_to_expiry_warning', 7)
            
            for portfolio in self.data_manager.portfolios.values():
                for position in portfolio.positions:
                    if position.position_type in [PositionType.CALL, PositionType.PUT] and position.expiry:
                        try:
                            # Parse expiry date
                            if '/' in position.expiry:
                                expiry_date = datetime.strptime(position.expiry, "%m/%d/%Y")
                            else:
                                expiry_date = datetime.strptime(position.expiry, "%Y-%m-%d")
                            
                            days_to_expiry = (expiry_date - datetime.now()).days
                            
                            if 0 <= days_to_expiry <= warning_days:
                                alert_key = f"expiry_{position.account_id}_{position.symbol}_{position.strike_price}_{position.expiry}"
                                
                                if alert_key not in self.triggered_alerts:
                                    level = AlertLevel.URGENT if days_to_expiry <= 1 else AlertLevel.WARNING
                                    
                                    self.data_manager.add_alert(
                                        alert_type=AlertType.EXPIRATION,
                                        level=level,
                                        title=f"Option Expiring: {position.symbol}",
                                        message=f"{position.symbol} {position.option_type} ${position.strike_price} expires in {days_to_expiry} days",
                                        symbol=position.symbol,
                                        account_id=position.account_id,
                                        value=days_to_expiry,
                                        threshold=warning_days
                                    )
                                    
                                    self.triggered_alerts.add(alert_key)
                                    self.logger.info(f"Expiration alert triggered for {position.symbol}")
                                    
                        except ValueError:
                            continue  # Skip invalid date formats
                            
        except Exception as e:
            self.logger.error(f"Error checking expiration alerts: {e}")
    
    def _check_high_iv_alerts(self) -> None:
        """Check for high implied volatility alerts"""
        try:
            high_iv_threshold = self.thresholds.get('high_iv_threshold', 1.0)
            
            for portfolio in self.data_manager.portfolios.values():
                for position in portfolio.positions:
                    if (position.position_type in [PositionType.CALL, PositionType.PUT] and 
                        position.greeks and position.greeks.implied_volatility):
                        
                        iv = position.greeks.implied_volatility
                        
                        if iv >= high_iv_threshold:
                            alert_key = f"high_iv_{position.account_id}_{position.symbol}_{position.strike_price}"
                            
                            if alert_key not in self.triggered_alerts:
                                self.data_manager.add_alert(
                                    alert_type=AlertType.MARKET,
                                    level=AlertLevel.INFO,
                                    title=f"High IV: {position.symbol}",
                                    message=f"{position.symbol} option has high implied volatility: {iv*100:.1f}%",
                                    symbol=position.symbol,
                                    account_id=position.account_id,
                                    value=iv,
                                    threshold=high_iv_threshold
                                )
                                
                                self.triggered_alerts.add(alert_key)
                                self.logger.info(f"High IV alert triggered for {position.symbol}")
                                
        except Exception as e:
            self.logger.error(f"Error checking high IV alerts: {e}")
    
    def _check_liquidity_alerts(self) -> None:
        """Check for low liquidity alerts"""
        try:
            low_liquidity_threshold = self.thresholds.get('low_liquidity_threshold', 10)
            
            for symbol, market_data in self.data_manager.market_data.items():
                if market_data.volume < low_liquidity_threshold:
                    alert_key = f"low_liquidity_{symbol}"
                    
                    # Only alert once per day for liquidity issues
                    if alert_key not in self.triggered_alerts:
                        # Check if we have positions in this symbol
                        has_position = False
                        for portfolio in self.data_manager.portfolios.values():
                            for position in portfolio.positions:
                                if position.symbol == symbol:
                                    has_position = True
                                    break
                            if has_position:
                                break
                        
                        if has_position:
                            self.data_manager.add_alert(
                                alert_type=AlertType.MARKET,
                                level=AlertLevel.WARNING,
                                title=f"Low Liquidity: {symbol}",
                                message=f"{symbol} has low trading volume: {market_data.volume}",
                                symbol=symbol,
                                value=market_data.volume,
                                threshold=low_liquidity_threshold
                            )
                            
                            self.triggered_alerts.add(alert_key)
                            self.logger.info(f"Low liquidity alert triggered for {symbol}")
                            
        except Exception as e:
            self.logger.error(f"Error checking liquidity alerts: {e}")
    
    def _check_margin_alerts(self) -> None:
        """Check for margin and buying power alerts"""
        try:
            for account_id, portfolio in self.data_manager.portfolios.items():
                # Check buying power
                if portfolio.buying_power < 1000:  # Less than $1000 buying power
                    alert_key = f"low_buying_power_{account_id}"
                    
                    if alert_key not in self.triggered_alerts:
                        self.data_manager.add_alert(
                            alert_type=AlertType.RISK,
                            level=AlertLevel.WARNING,
                            title="Low Buying Power",
                            message=f"Account {account_id} has low buying power: ${portfolio.buying_power:,.2f}",
                            account_id=account_id,
                            value=portfolio.buying_power,
                            threshold=1000
                        )
                        
                        self.triggered_alerts.add(alert_key)
                        self.logger.info(f"Low buying power alert triggered for {account_id}")
                
                # Check margin usage (if applicable)
                if portfolio.margin_used > 0:
                    margin_ratio = portfolio.margin_used / portfolio.total_value if portfolio.total_value > 0 else 0
                    
                    if margin_ratio > 0.8:  # Using more than 80% margin
                        alert_key = f"high_margin_{account_id}"
                        
                        if alert_key not in self.triggered_alerts:
                            self.data_manager.add_alert(
                                alert_type=AlertType.RISK,
                                level=AlertLevel.CRITICAL,
                                title="High Margin Usage",
                                message=f"Account {account_id} has high margin usage: {margin_ratio*100:.1f}%",
                                account_id=account_id,
                                value=margin_ratio,
                                threshold=0.8
                            )
                            
                            self.triggered_alerts.add(alert_key)
                            self.logger.warning(f"High margin alert triggered for {account_id}")
                            
        except Exception as e:
            self.logger.error(f"Error checking margin alerts: {e}")
    
    def _check_system_alerts(self) -> None:
        """Check for system-level alerts"""
        try:
            current_time = datetime.now()
            
            # Check IBKR connection
            if not self.data_manager.system_status.ibkr_connected:
                alert_key = "ibkr_disconnected"
                
                if alert_key not in self.triggered_alerts:
                    self.data_manager.add_alert(
                        alert_type=AlertType.SYSTEM,
                        level=AlertLevel.URGENT,
                        title="IBKR Connection Lost",
                        message="Connection to Interactive Brokers has been lost"
                    )
                    
                    self.triggered_alerts.add(alert_key)
                    self.logger.error("IBKR disconnection alert triggered")
            
            # Check data freshness
            if self.data_manager.last_market_update:
                time_since_market_update = (current_time - self.data_manager.last_market_update).total_seconds()
                
                if time_since_market_update > 300:  # 5 minutes without market data update
                    alert_key = "stale_market_data"
                    
                    if alert_key not in self.triggered_alerts:
                        self.data_manager.add_alert(
                            alert_type=AlertType.SYSTEM,
                            level=AlertLevel.WARNING,
                            title="Stale Market Data",
                            message=f"Market data hasn't updated in {time_since_market_update/60:.1f} minutes"
                        )
                        
                        self.triggered_alerts.add(alert_key)
                        self.logger.warning("Stale market data alert triggered")
            
            # Check WebSocket clients
            if self.data_manager.system_status.websocket_clients == 0:
                alert_key = "no_websocket_clients"
                
                # Only alert if we've been running for more than 1 minute
                if (alert_key not in self.triggered_alerts and 
                    self.data_manager.system_status.system_uptime > 60):
                    
                    self.data_manager.add_alert(
                        alert_type=AlertType.SYSTEM,
                        level=AlertLevel.INFO,
                        title="No Frontend Connections",
                        message="No WebSocket clients are currently connected"
                    )
                    
                    self.triggered_alerts.add(alert_key)
                    self.logger.info("No WebSocket clients alert triggered")
                    
        except Exception as e:
            self.logger.error(f"Error checking system alerts: {e}")
    
    def _cleanup_triggered_alerts(self) -> None:
        """Clean up old triggered alerts"""
        try:
            # Reset triggered alerts daily
            if len(self.triggered_alerts) > 1000:  # Prevent memory buildup
                self.triggered_alerts.clear()
                self.logger.info("Cleared triggered alerts cache")
                
        except Exception as e:
            self.logger.error(f"Error cleaning up triggered alerts: {e}")
    
    def _cleanup(self) -> None:
        """Cleanup service resources"""
        self.triggered_alerts.clear()
        self.logger.info("Alerts service cleanup completed")
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge a specific alert"""
        try:
            for alert in self.data_manager.alerts:
                if alert.id == alert_id:
                    alert.acknowledged = True
                    self.logger.info(f"Alert {alert_id} acknowledged")
                    return True
            return False
            
        except Exception as e:
            self.logger.error(f"Error acknowledging alert {alert_id}: {e}")
            return False
    
    def get_alerts_summary(self) -> Dict[str, any]:
        """Get summary of current alerts"""
        try:
            active_alerts = self.data_manager.get_active_alerts()
            
            alerts_by_level = {}
            alerts_by_type = {}
            
            for alert in active_alerts:
                level = alert.level.value
                alert_type = alert.type.value
                
                alerts_by_level[level] = alerts_by_level.get(level, 0) + 1
                alerts_by_type[alert_type] = alerts_by_type.get(alert_type, 0) + 1
            
            return {
                'total_active_alerts': len(active_alerts),
                'alerts_by_level': alerts_by_level,
                'alerts_by_type': alerts_by_type,
                'triggered_alerts_cache': len(self.triggered_alerts),
                'last_alert_check': self.last_alert_check.isoformat(),
                'thresholds': self.thresholds
            }
            
        except Exception as e:
            self.logger.error(f"Error generating alerts summary: {e}")
            return {}
    
    def is_running(self) -> bool:
        """Check if service is running"""
        return self.running and (self.service_thread is not None and self.service_thread.is_alive())
    
    def get_service_stats(self) -> Dict[str, any]:
        """Get service statistics"""
        return {
            'running': self.is_running(),
            'triggered_alerts_count': len(self.triggered_alerts),
            'last_alert_check': self.last_alert_check.isoformat(),
            'alert_thresholds': self.thresholds
        }