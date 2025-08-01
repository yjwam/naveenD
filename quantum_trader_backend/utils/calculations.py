import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import numpy as np
from models.market_data import Greeks
from models.portfolio import Position, PositionType

class OptionsCalculator:
    """Options pricing and Greeks calculations"""
    
    @staticmethod
    def black_scholes_call(S: float, K: float, T: float, r: float, sigma: float) -> float:
        """Black-Scholes call option price"""
        if T <= 0:
            return max(S - K, 0)
        
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        
        call_price = S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
        return max(call_price, 0)
    
    @staticmethod
    def black_scholes_put(S: float, K: float, T: float, r: float, sigma: float) -> float:
        """Black-Scholes put option price"""
        if T <= 0:
            return max(K - S, 0)
        
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        
        put_price = K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)
        return max(put_price, 0)
    
    @staticmethod
    def calculate_greeks(S: float, K: float, T: float, r: float, sigma: float, 
                        option_type: str = 'C') -> Greeks:
        """Calculate option Greeks"""
        if T <= 0:
            return Greeks(delta=1.0 if S > K else 0.0)
        
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        
        # Delta
        if option_type.upper() == 'C':
            delta = norm_cdf(d1)
        else:
            delta = norm_cdf(d1) - 1
        
        # Gamma
        gamma = norm_pdf(d1) / (S * sigma * math.sqrt(T))
        
        # Theta
        if option_type.upper() == 'C':
            theta = (-S * norm_pdf(d1) * sigma / (2 * math.sqrt(T)) 
                    - r * K * math.exp(-r * T) * norm_cdf(d2)) / 365
        else:
            theta = (-S * norm_pdf(d1) * sigma / (2 * math.sqrt(T)) 
                    + r * K * math.exp(-r * T) * norm_cdf(-d2)) / 365
        
        # Vega
        vega = S * norm_pdf(d1) * math.sqrt(T) / 100
        
        # Rho
        if option_type.upper() == 'C':
            rho = K * T * math.exp(-r * T) * norm_cdf(d2) / 100
        else:
            rho = -K * T * math.exp(-r * T) * norm_cdf(-d2) / 100
        
        return Greeks(
            delta=delta,
            gamma=gamma,
            theta=theta,
            vega=vega,
            rho=rho,
            implied_volatility=sigma
        )
    
    @staticmethod
    def implied_volatility(market_price: float, S: float, K: float, T: float, 
                          r: float, option_type: str = 'C') -> float:
        """Calculate implied volatility using Newton-Raphson method"""
        if T <= 0:
            return 0.0
        
        # Initial guess
        sigma = 0.2
        tolerance = 1e-6
        max_iterations = 100
        
        for _ in range(max_iterations):
            if option_type.upper() == 'C':
                price = OptionsCalculator.black_scholes_call(S, K, T, r, sigma)
            else:
                price = OptionsCalculator.black_scholes_put(S, K, T, r, sigma)
            
            diff = price - market_price
            
            if abs(diff) < tolerance:
                return sigma
            
            # Calculate vega for Newton-Raphson
            d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
            vega = S * norm_pdf(d1) * math.sqrt(T)
            
            if vega == 0:
                break
            
            sigma = sigma - diff / vega
            
            if sigma <= 0:
                sigma = 0.01
        
        return max(sigma, 0.01)

class PortfolioCalculator:
    """Portfolio-level calculations and risk metrics"""
    
    @staticmethod
    def calculate_portfolio_greeks(positions: List[Position]) -> Greeks:
        """Calculate portfolio-level Greeks"""
        total_delta = 0.0
        total_gamma = 0.0
        total_theta = 0.0
        total_vega = 0.0
        total_rho = 0.0
        
        for position in positions:
            if position.greeks and position.position_type != PositionType.STOCK:
                multiplier = position.quantity * 100  # Options multiplier
                total_delta += position.greeks.delta * multiplier
                total_gamma += position.greeks.gamma * multiplier
                total_theta += position.greeks.theta * multiplier
                total_vega += position.greeks.vega * multiplier
                total_rho += position.greeks.rho * multiplier
            elif position.position_type == PositionType.STOCK:
                # Stock delta is 1
                total_delta += position.quantity
        
        return Greeks(
            delta=total_delta,
            gamma=total_gamma,
            theta=total_theta,
            vega=total_vega,
            rho=total_rho
        )
    
    @staticmethod
    def calculate_value_at_risk(returns: List[float], confidence_level: float = 0.95) -> float:
        """Calculate Value at Risk (VaR)"""
        if not returns:
            return 0.0
        
        returns_array = np.array(returns)
        return np.percentile(returns_array, (1 - confidence_level) * 100)
    
    @staticmethod
    def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.02) -> float:
        """Calculate Sharpe ratio"""
        if not returns or len(returns) < 2:
            return 0.0
        
        returns_array = np.array(returns)
        excess_returns = returns_array - risk_free_rate / 252  # Daily risk-free rate
        
        if np.std(excess_returns) == 0:
            return 0.0
        
        return np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252)
    
    @staticmethod
    def calculate_max_drawdown(portfolio_values: List[float]) -> float:
        """Calculate maximum drawdown"""
        if len(portfolio_values) < 2:
            return 0.0
        
        values = np.array(portfolio_values)
        peak = np.maximum.accumulate(values)
        drawdown = (values - peak) / peak
        return np.min(drawdown)
    
    @staticmethod
    def calculate_beta(stock_returns: List[float], market_returns: List[float]) -> float:
        """Calculate beta against market"""
        if len(stock_returns) != len(market_returns) or len(stock_returns) < 10:
            return 1.0
        
        stock_array = np.array(stock_returns)
        market_array = np.array(market_returns)
        
        covariance = np.cov(stock_array, market_array)[0, 1]
        market_variance = np.var(market_array)
        
        if market_variance == 0:
            return 1.0
        
        return covariance / market_variance

class StrategyAnalyzer:
    """Options strategy analysis and classification"""
    
    STRATEGY_PATTERNS = {
        'long_call': {'calls': 1, 'puts': 0, 'net_position': 'long'},
        'long_put': {'calls': 0, 'puts': 1, 'net_position': 'long'},
        'covered_call': {'calls': -1, 'puts': 0, 'stock': 100, 'net_position': 'covered'},
        'protective_put': {'calls': 0, 'puts': 1, 'stock': 100, 'net_position': 'protected'},
        'bull_call_spread': {'calls': [1, -1], 'puts': 0, 'strikes': 'ascending'},
        'bear_put_spread': {'calls': 0, 'puts': [1, -1], 'strikes': 'descending'},
        'iron_condor': {'calls': [-1, 1], 'puts': [1, -1], 'strikes': 'symmetric'},
        'straddle': {'calls': 1, 'puts': 1, 'strikes': 'same'},
        'strangle': {'calls': 1, 'puts': 1, 'strikes': 'different'}
    }
    
    @staticmethod
    def identify_strategy(positions: List[Position], underlying_symbol: str) -> str:
        """Identify the options strategy for a given underlying"""
        # Filter positions for the specific underlying
        relevant_positions = [p for p in positions if p.symbol == underlying_symbol]
        
        if not relevant_positions:
            return "No Position"
        
        # Count position types
        calls = [p for p in relevant_positions if p.position_type == PositionType.CALL]
        puts = [p for p in relevant_positions if p.position_type == PositionType.PUT]
        stocks = [p for p in relevant_positions if p.position_type == PositionType.STOCK]
        
        # Simple strategy identification
        if len(calls) == 1 and len(puts) == 0 and len(stocks) == 0:
            return "Long Call" if calls[0].quantity > 0 else "Short Call"
        elif len(calls) == 0 and len(puts) == 1 and len(stocks) == 0:
            return "Long Put" if puts[0].quantity > 0 else "Short Put"
        elif len(calls) == 1 and len(puts) == 0 and len(stocks) == 1:
            return "Covered Call" if calls[0].quantity < 0 and stocks[0].quantity > 0 else "Unknown"
        elif len(calls) == 0 and len(puts) == 1 and len(stocks) == 1:
            return "Protective Put" if puts[0].quantity > 0 and stocks[0].quantity > 0 else "Unknown"
        elif len(calls) == 1 and len(puts) == 1:
            if calls[0].strike_price == puts[0].strike_price:
                return "Straddle"
            else:
                return "Strangle"
        elif len(calls) == 2 and len(puts) == 0:
            return "Bull Call Spread" if any(p.quantity > 0 for p in calls) else "Bear Call Spread"
        elif len(calls) == 0 and len(puts) == 2:
            return "Bear Put Spread" if any(p.quantity > 0 for p in puts) else "Bull Put Spread"
        elif len(calls) == 2 and len(puts) == 2:
            return "Iron Condor"
        else:
            return "Complex Strategy"

# Helper functions
def norm_cdf(x: float) -> float:
    """Cumulative distribution function of standard normal distribution"""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def norm_pdf(x: float) -> float:
    """Probability density function of standard normal distribution"""
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

def time_to_expiry(expiry_str: str) -> float:
    """Calculate time to expiry in years"""
    try:
        # Parse expiry string (format: "MM/DD/YYYY" or "YYYY-MM-DD")
        if '/' in expiry_str:
            expiry_date = datetime.strptime(expiry_str, "%m/%d/%Y")
        else:
            expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d")
        
        now = datetime.now()
        time_diff = (expiry_date - now).total_seconds()
        return max(time_diff / (365.25 * 24 * 3600), 0)  # Years
    except:
        return 0.0