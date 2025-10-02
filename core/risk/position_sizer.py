#!/usr/bin/env python3
"""
Position Sizer - Calculate contract sizes for trading

Adapted from enhanced_progressive_backtester.py
Handles different parameters for Fresh OBs vs Breaker Blocks.

Fresh OB:  40% capital, 20x leverage
Breaker:   30% capital, 10x leverage

Features:
- Lot-based position sizing
- Leverage support
- Minimum order size handling
- Rounded to whole contracts

Usage:
    sizer = PositionSizer()
    
    contracts, actual_value = sizer.calculate_position(
        capital=1000,
        ob_type='fresh',  # or 'breaker'
        entry_price=150.50,
        symbol='SOLUSD'
    )
"""

import math
import json
from typing import Tuple, Dict
from pathlib import Path

from core.utils.logger import get_logger

logger = get_logger('system')


class PositionSizer:
    """
    Calculates position sizes for trading based on capital and OB type
    
    Supports dual parameters:
    - Fresh OB: Higher capital allocation (40%), higher leverage (20x)
    - Breaker Block: Conservative allocation (30%), lower leverage (10x)
    """
    
    def __init__(self, symbols_config_path: str = "config/symbols_config.json"):
        """
        Initialize Position Sizer
        
        Args:
            symbols_config_path: Path to symbols configuration
        """
        self.symbols_config_path = symbols_config_path
        self.symbols_config = self._load_symbols_config()
        
        # Default parameters (from sub_account configs)
        self.parameters = {
            'fresh': {
                'position_size_pct': 0.40,  # 40% of capital
                'leverage': 20               # 20x leverage
            },
            'breaker': {
                'position_size_pct': 0.30,  # 30% of capital
                'leverage': 10               # 10x leverage
            }
        }
        
        logger.info("Position Sizer initialized")
    
    def _load_symbols_config(self) -> Dict:
        """Load symbols configuration"""
        try:
            config_path = Path(self.symbols_config_path)
            if not config_path.exists():
                logger.error(f"Symbols config not found: {config_path}")
                return {}
            
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            return config
            
        except Exception as e:
            logger.error(f"Error loading symbols config: {e}")
            return {}
    
    def get_symbol_specs(self, symbol: str) -> Dict:
        """Get symbol specifications"""
        if symbol not in self.symbols_config:
            raise ValueError(f"Symbol {symbol} not found in config")
        
        return self.symbols_config[symbol]
    
    def calculate_position(self, capital: float, ob_type: str, 
                          entry_price: float, symbol: str) -> Tuple[int, float]:
        """
        Calculate position size in contracts
        
        Args:
            capital: Available capital (USD)
            ob_type: 'fresh' or 'breaker'
            entry_price: Entry price for the trade
            symbol: Trading symbol (e.g., 'SOLUSD')
        
        Returns:
            Tuple of (contracts, actual_position_value)
        
        Example:
            contracts, value = sizer.calculate_position(
                capital=1000,
                ob_type='fresh',
                entry_price=150.50,
                symbol='SOLUSD'
            )
            # Returns: (53, 7976.50) for fresh OB
            # 1000 * 0.40 = 400 capital used
            # 400 * 20 = 8000 buying power
            # 8000 / 150.50 = 53.15 -> floor to 53 contracts
        """
        # Validate inputs
        if ob_type not in ['fresh', 'breaker']:
            raise ValueError(f"Invalid ob_type: {ob_type}. Must be 'fresh' or 'breaker'")
        
        if capital <= 0:
            raise ValueError(f"Capital must be positive: {capital}")
        
        if entry_price <= 0:
            raise ValueError(f"Entry price must be positive: {entry_price}")
        
        # Get symbol specs
        specs = self.get_symbol_specs(symbol)
        qty_per_contract = specs.get('qty_per_contract', 1)
        min_quantity = specs.get('min_quantity', 1)
        
        # Get parameters for this OB type
        params = self.parameters[ob_type]
        position_size_pct = params['position_size_pct']
        leverage = params['leverage']
        
        # Calculate capital to use for this trade
        capital_to_use = capital * position_size_pct
        
        # Apply leverage
        buying_power = capital_to_use * leverage
        
        # Calculate maximum position size (in base currency)
        max_position_size = buying_power / entry_price
        
        # Convert to contracts (round down to whole contracts)
        max_contracts = math.floor(max_position_size / qty_per_contract)
        
        # Ensure minimum quantity
        if max_contracts < min_quantity:
            logger.warning(f"Insufficient capital for minimum order: {max_contracts} < {min_quantity}")
            return 0, 0.0
        
        # Calculate actual position value
        actual_position_value = max_contracts * qty_per_contract * entry_price
        
        # Calculate actual capital used (without leverage)
        actual_capital_used = actual_position_value / leverage
        
        logger.debug(f"Position Sizing ({ob_type.upper()}):")
        logger.debug(f"   Capital: ${capital:.2f}")
        logger.debug(f"   Using: ${capital_to_use:.2f} ({position_size_pct*100:.0f}%)")
        logger.debug(f"   Leverage: {leverage}x")
        logger.debug(f"   Buying Power: ${buying_power:.2f}")
        logger.debug(f"   Entry Price: ${entry_price:.4f}")
        logger.debug(f"   Contracts: {max_contracts}")
        logger.debug(f"   Position Value: ${actual_position_value:.2f}")
        logger.debug(f"   Capital Used: ${actual_capital_used:.2f}")
        
        return max_contracts, actual_position_value
    
    def calculate_liquidation_level(self, entry_price: float, direction: str,
                                    leverage: float, safety_factor: float = 0.95) -> float:
        """
        Calculate liquidation price level
        
        Args:
            entry_price: Entry price
            direction: 'long' or 'short'
            leverage: Leverage used
            safety_factor: Safety factor (0.95 = 5% buffer for fees)
        
        Returns:
            Liquidation price
        
        Example:
            liq = sizer.calculate_liquidation_level(150.0, 'long', 20)
            # Returns: ~142.5 (5% below entry for 20x leverage)
        """
        # Liquidation threshold = (1 / leverage) * safety_factor
        liquidation_threshold = (1.0 / leverage) * safety_factor
        
        if direction == 'long':
            # Long: liquidated if price drops by threshold
            liquidation_price = entry_price * (1 - liquidation_threshold)
        else:  # short
            # Short: liquidated if price rises by threshold
            liquidation_price = entry_price * (1 + liquidation_threshold)
        
        logger.debug(f"Liquidation Level ({direction.upper()}):")
        logger.debug(f"   Entry: ${entry_price:.4f}")
        logger.debug(f"   Leverage: {leverage}x")
        logger.debug(f"   Threshold: {liquidation_threshold*100:.2f}%")
        logger.debug(f"   Liquidation: ${liquidation_price:.4f}")
        
        return liquidation_price
    
    def validate_position_size(self, contracts: int, capital: float, 
                              entry_price: float, symbol: str, 
                              ob_type: str) -> Tuple[bool, str]:
        """
        Validate if position size is within acceptable limits
        
        Args:
            contracts: Number of contracts
            capital: Available capital
            entry_price: Entry price
            symbol: Trading symbol
            ob_type: 'fresh' or 'breaker'
        
        Returns:
            Tuple of (is_valid, reason)
        """
        # Check minimum
        specs = self.get_symbol_specs(symbol)
        min_quantity = specs.get('min_quantity', 1)
        
        if contracts < min_quantity:
            return False, f"Below minimum: {contracts} < {min_quantity}"
        
        # Check capital sufficiency
        params = self.parameters[ob_type]
        leverage = params['leverage']
        
        position_value = contracts * specs['qty_per_contract'] * entry_price
        required_capital = position_value / leverage
        
        if required_capital > capital:
            return False, f"Insufficient capital: needs ${required_capital:.2f}, have ${capital:.2f}"
        
        return True, "Valid"
    
    def update_parameters(self, ob_type: str, position_size_pct: float = None,
                         leverage: float = None):
        """
        Update sizing parameters
        
        Args:
            ob_type: 'fresh' or 'breaker'
            position_size_pct: New position size percentage
            leverage: New leverage
        
        Example:
            sizer.update_parameters('fresh', position_size_pct=0.50, leverage=25)
        """
        if ob_type not in self.parameters:
            raise ValueError(f"Invalid ob_type: {ob_type}")
        
        if position_size_pct is not None:
            if not 0 < position_size_pct <= 1:
                raise ValueError(f"Invalid position_size_pct: {position_size_pct}")
            self.parameters[ob_type]['position_size_pct'] = position_size_pct
            logger.info(f"Updated {ob_type} position_size_pct: {position_size_pct*100:.0f}%")
        
        if leverage is not None:
            if leverage <= 0:
                raise ValueError(f"Invalid leverage: {leverage}")
            self.parameters[ob_type]['leverage'] = leverage
            logger.info(f"Updated {ob_type} leverage: {leverage}x")


# Example usage and testing
if __name__ == "__main__":
    print("\n" + "="*80)
    print("Testing Position Sizer")
    print("="*80 + "\n")
    
    # Create position sizer
    sizer = PositionSizer()
    
    # Test scenarios
    test_capital = 1000  # $1000 capital
    test_price = 150.50  # SOL price
    test_symbol = "SOLUSD"
    
    print("Test Scenario: $1000 capital, SOL @ $150.50\n")
    
    # Test 1: Fresh OB
    print("1. Fresh OB Position:")
    contracts, value = sizer.calculate_position(
        capital=test_capital,
        ob_type='fresh',
        entry_price=test_price,
        symbol=test_symbol
    )
    print(f"   Contracts: {contracts}")
    print(f"   Position Value: ${value:.2f}")
    print(f"   Expected: ~53 contracts, ~$7976 value")
    print()
    
    # Calculate liquidation
    liq_price = sizer.calculate_liquidation_level(test_price, 'long', 20)
    print(f"   Liquidation Price: ${liq_price:.4f}")
    print()
    
    # Test 2: Breaker Block
    print("2. Breaker Block Position:")
    contracts, value = sizer.calculate_position(
        capital=test_capital,
        ob_type='breaker',
        entry_price=test_price,
        symbol=test_symbol
    )
    print(f"   Contracts: {contracts}")
    print(f"   Position Value: ${value:.2f}")
    print(f"   Expected: ~19 contracts, ~$2859 value")
    print()
    
    # Calculate liquidation
    liq_price = sizer.calculate_liquidation_level(test_price, 'long', 10)
    print(f"   Liquidation Price: ${liq_price:.4f}")
    print()
    
    # Test 3: Validation
    print("3. Position Validation:")
    is_valid, reason = sizer.validate_position_size(
        contracts=53,
        capital=test_capital,
        entry_price=test_price,
        symbol=test_symbol,
        ob_type='fresh'
    )
    print(f"   Fresh OB (53 contracts): {is_valid} - {reason}")
    
    is_valid, reason = sizer.validate_position_size(
        contracts=200,
        capital=test_capital,
        entry_price=test_price,
        symbol=test_symbol,
        ob_type='fresh'
    )
    print(f"   Fresh OB (200 contracts): {is_valid} - {reason}")
    print()
    
    # Test 4: Different prices
    print("4. Position Sizing at Different Prices:")
    for price in [100.0, 150.0, 200.0]:
        contracts, value = sizer.calculate_position(
            capital=test_capital,
            ob_type='fresh',
            entry_price=price,
            symbol=test_symbol
        )
        print(f"   @ ${price:.2f}: {contracts} contracts (${value:.2f})")
    print()
    
    # Test 5: Update parameters
    print("5. Testing Parameter Update:")
    print(f"   Current Fresh OB: {sizer.parameters['fresh']}")
    sizer.update_parameters('fresh', position_size_pct=0.50, leverage=25)
    print(f"   Updated Fresh OB: {sizer.parameters['fresh']}")
    
    contracts, value = sizer.calculate_position(
        capital=test_capital,
        ob_type='fresh',
        entry_price=test_price,
        symbol=test_symbol
    )
    print(f"   New position: {contracts} contracts (${value:.2f})")
    print()
    
    print("="*80)
    print("All tests completed!")
    print("="*80 + "\n")