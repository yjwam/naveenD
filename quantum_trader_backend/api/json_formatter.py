import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from decimal import Decimal

from models.dashboard import DashboardData
from models.portfolio import Portfolio, Position
from models.market_data import MarketData, IndexData
from utils.logger import get_logger

class JSONFormatter:
    """Formats data for JSON API responses matching frontend expectations"""
    
    def __init__(self):
        self.logger = get_logger("json_formatter")
    
    def format_dashboard_data(self, dashboard_data: DashboardData) -> Dict[str, Any]:
        """Format complete dashboard data for frontend"""
        try:
            formatted_data = {
                'timestamp': dashboard_data.timestamp.isoformat(),
                'market_indices': self._format_market_indices(dashboard_data.market_indices),
                'portfolios': self._format_portfolios(dashboard_data.portfolios),
                'alerts': self._format_alerts(dashboard_data.alerts),
                'performance_metrics': self._format_performance_metrics(dashboard_data.performance_metrics),
                'risk_metrics': self._format_risk_metrics(dashboard_data.risk_metrics),
                'system_status': self._format_system_status(dashboard_data.system_status),
                'summary': self._format_summary(dashboard_data)
            }
            
            return formatted_data
            
        except Exception as e:
            self.logger.error(f"Error formatting dashboard data: {e}")
            return self._get_error_response("Error formatting dashboard data")
    
    def _format_market_indices(self, index_data: IndexData) -> Dict[str, Any]:
        """Format market indices data"""
        formatted_indices = {}
        
        # Map indices to expected frontend format
        index_mapping = {
            'spy': 'SPY',
            'qqq': 'QQQ', 
            'nasdaq': 'NASDAQ',
            'vix': 'VIX',
            'dxy': 'DXY',
            'ten_year': '10Y'
        }
        
        for attr_name, display_name in index_mapping.items():
            market_data = getattr(index_data, attr_name.upper(), None)
            if market_data:
                formatted_indices[display_name] = {
                    'price': self._safe_float(market_data.price),
                    'change': self._safe_float(market_data.change),
                    'change_percent': self._safe_float(market_data.change_percent, 2),
                    'volume': market_data.volume,
                    'bid': self._safe_float(market_data.bid),
                    'ask': self._safe_float(market_data.ask),
                    'high': self._safe_float(market_data.high),
                    'low': self._safe_float(market_data.low),
                    'timestamp': market_data.timestamp.isoformat()
                }
        
        return formatted_indices
    
    def _format_portfolios(self, portfolios: Dict[str, Portfolio]) -> Dict[str, Any]:
        """Format portfolios data organized by account type"""
        formatted_portfolios = {
            'individual_taxable': [],
            'retirement_tax_free': []
        }
        
        for portfolio in portfolios.values():
            account_type_key = portfolio.account_type.value
            
            if account_type_key in formatted_portfolios:
                formatted_portfolio = {
                    'account_id': portfolio.account_id,
                    'positions': self._format_positions(portfolio.positions),
                    'summary': {
                        'total_value': self._safe_float(portfolio.total_value),
                        'day_pnl': self._safe_float(portfolio.day_pnl),
                        'day_pnl_percent': self._safe_float(portfolio.day_pnl_percent, 2),
                        'total_pnl': self._safe_float(portfolio.total_pnl),
                        'total_pnl_percent': self._safe_float(portfolio.total_pnl_percent, 2),
                        'cash_balance': self._safe_float(portfolio.cash_balance),
                        'buying_power': self._safe_float(portfolio.buying_power),
                        'margin_used': self._safe_float(portfolio.margin_used),
                        'position_count': len(portfolio.positions)
                    }
                }
                
                formatted_portfolios[account_type_key].append(formatted_portfolio)
        
        return formatted_portfolios
    
    def _format_positions(self, positions: List[Position]) -> List[Dict[str, Any]]:
        """Format positions data to match frontend table structure - FIXED"""
        formatted_positions = []
        
        for position in positions:
            # Base position data
            formatted_position = {
                'symbol': position.symbol,
                'account_id': position.account_id,
                'account_type': position.account_type.value,
                'position_type': position.position_type.value,
                'quantity': position.quantity,
                'avg_cost': self._safe_float(position.avg_cost, 2),
                'current_price': self._safe_float(position.current_price, 2),
                'market_value': self._safe_float(position.market_value, 2),
                'unrealized_pnl': self._safe_float(position.unrealized_pnl, 2),
                'realized_pnl': self._safe_float(position.realized_pnl, 2),
                'day_pnl': self._safe_float(position.day_pnl, 2),
                'strike_price': self._safe_float(position.strike_price, 2) if position.strike_price else 0,
                'expiry': position.expiry or "",
                'option_type': position.option_type or "0",
                'greeks': position.greeks.to_dict() if position.greeks else None,
                'strategy': position.strategy or "Complex Strategy",
                'confidence': position.confidence,
                'signal': position.signal.value,
                'priority': position.priority.value,
                'notes': position.notes,
                'levels': position.levels or {},
                'created_at': position.created_at.isoformat(),
                'updated_at': position.updated_at.isoformat()
            }
            
            formatted_positions.append(formatted_position)
        
        return formatted_positions
    
    def _format_alerts(self, alerts: List) -> List[Dict[str, Any]]:
        """Format alerts data"""
        formatted_alerts = []
        
        for alert in alerts:
            formatted_alert = {
                'id': alert.id,
                'type': alert.type.value,
                'level': self._map_alert_level(alert.level.value),
                'title': alert.title,
                'message': alert.message,
                'symbol': alert.symbol,
                'account_id': alert.account_id,
                'value': self._safe_float(alert.value) if alert.value else None,
                'threshold': self._safe_float(alert.threshold) if alert.threshold else None,
                'created_at': alert.created_at.isoformat(),
                'acknowledged': alert.acknowledged
            }
            
            formatted_alerts.append(formatted_alert)
        
        return formatted_alerts
    
    def _format_performance_metrics(self, metrics) -> Dict[str, Any]:
        """Format performance metrics"""
        return {
            'total_return': self._safe_float(metrics.total_return, 2),
            'annualized_return': self._safe_float(metrics.annualized_return, 2),
            'sharpe_ratio': self._safe_float(metrics.sharpe_ratio, 2),
            'max_drawdown': self._safe_float(metrics.max_drawdown, 2),
            'win_rate': self._safe_float(metrics.win_rate, 2),
            'profit_factor': self._safe_float(metrics.profit_factor, 2),
            'avg_win': self._safe_float(metrics.avg_win),
            'avg_loss': self._safe_float(metrics.avg_loss),
            'total_trades': metrics.total_trades,
            'winning_trades': metrics.winning_trades,
            'losing_trades': metrics.losing_trades
        }
    
    def _format_risk_metrics(self, metrics) -> Dict[str, Any]:
        """Format risk metrics"""
        return {
            'portfolio_beta': self._safe_float(metrics.portfolio_beta, 2),
            'portfolio_delta': self._safe_float(metrics.portfolio_delta),
            'portfolio_gamma': self._safe_float(metrics.portfolio_gamma),
            'portfolio_theta': self._safe_float(metrics.portfolio_theta),
            'portfolio_vega': self._safe_float(metrics.portfolio_vega),
            'var_95': self._safe_float(metrics.var_95),
            'expected_shortfall': self._safe_float(metrics.expected_shortfall),
            'correlation_to_spy': self._safe_float(metrics.correlation_to_spy, 2)
        }
    
    def _format_system_status(self, status) -> Dict[str, Any]:
        """Format system status"""
        return {
            'ibkr_connected': status.ibkr_connected,
            'websocket_clients': status.websocket_clients,
            'last_market_data_update': status.last_market_data_update.isoformat() if status.last_market_data_update else None,
            'last_portfolio_update': status.last_portfolio_update.isoformat() if status.last_portfolio_update else None,
            'total_positions': status.total_positions,
            'active_alerts': status.active_alerts,
            'system_uptime': self._safe_float(status.system_uptime),
            'memory_usage': self._safe_float(status.memory_usage, 1),
            'cpu_usage': self._safe_float(status.cpu_usage, 1)
        }
    
    def _format_summary(self, dashboard_data: DashboardData) -> Dict[str, Any]:
        """Format dashboard summary"""
        return {
            'total_portfolio_value': self._safe_float(dashboard_data.total_portfolio_value),
            'total_day_pnl': self._safe_float(dashboard_data.total_day_pnl),
            'total_day_pnl_percent': self._safe_float(
                (dashboard_data.total_day_pnl / dashboard_data.total_portfolio_value * 100) 
                if dashboard_data.total_portfolio_value > 0 else 0, 2
            ),
            'total_unrealized_pnl': self._safe_float(dashboard_data.total_unrealized_pnl),
            'total_positions': dashboard_data.total_positions,
            'active_alerts': len(dashboard_data.get_active_alerts())
        }
    
    def _map_alert_level(self, level: str) -> int:
        """Map alert level to numeric value for frontend"""
        level_mapping = {
            'info': 1,
            'warning': 3,
            'critical': 4,
            'urgent': 5
        }
        return level_mapping.get(level.lower(), 1)
    
    def _safe_float(self, value: Any, decimals: int = 2) -> float:
        """Safely convert value to float with specified decimal places"""
        try:
            if value is None:
                return 0.0
            
            if isinstance(value, (int, float, Decimal)):
                return round(float(value), decimals)
            
            if isinstance(value, str):
                return round(float(value), decimals)
            
            return 0.0
            
        except (ValueError, TypeError):
            return 0.0
    
    def _get_error_response(self, message: str) -> Dict[str, Any]:
        """Get standardized error response"""
        return {
            'error': True,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'data': None
        }
    
    def format_streaming_update(self, update_type: str, data: Any) -> str:
        """Format streaming update for WebSocket"""
        try:
            formatted_update = {
                'type': update_type,
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            
            return json.dumps(formatted_update, default=self._json_serializer)
            
        except Exception as e:
            self.logger.error(f"Error formatting streaming update: {e}")
            return json.dumps({
                'type': 'error',
                'message': 'Error formatting update',
                'timestamp': datetime.now().isoformat()
            })
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for complex objects"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif hasattr(obj, 'to_dict'):
            return obj.to_dict()
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        else:
            return str(obj)

# Global formatter instance
json_formatter = JSONFormatter()