#!/usr/bin/env python3
"""
Position Manager - Track positions and capital per sub-account

Features:
- Track open positions per sub-account
- Update capital after P&L
- Check position limits (max 1 position per symbol per mode)
- Capital tracking per symbol
- Position state persistence

IMPORTANT: Both Mode A and Mode B CAN hold the same symbol simultaneously!
This is NOT a restriction. Each mode manages its own positions independently.

Usage:
    manager = PositionManager("account_1", initial_capital=1000)
    
    # Check if can enter
    can_enter = manager.can_enter_position("SOLUSD")
    
    # Open position
    manager.open_position("SOLUSD", entry_price=150.50, size=53, ...)
    
    # Update after exit
    manager.close_position("SOLUSD", pnl=125.50)
"""

from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import json
from pathlib import Path

from core.utils.logger import get_logger
from core.utils.state_persistence import get_state_manager

logger = get_logger('system')


@dataclass
class Position:
    """Represents an open trading position"""
    symbol: str
    direction: str  # 'long' or 'short'
    entry_price: float
    size: int  # Number of contracts
    entry_time: str
    ob_type: str  # 'fresh' or 'breaker'
    leverage: float
    capital_used: float
    position_value: float
    liquidation_level: float
    
    # OB details
    ob_top: float
    ob_btm: float
    ob_creation_bar: int
    
    # Tracking
    entry_bar: int
    highest_price: Optional[float] = None
    lowest_price: Optional[float] = None
    trailing_sl: Optional[float] = None
    partial_exited: bool = False
    partial_exit_price: Optional[float] = None
    remaining_size: Optional[int] = None  # After partial exit
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return asdict(self)
    
    @staticmethod
    def from_dict(data: Dict) -> 'Position':
        """Create Position from dictionary"""
        return Position(**data)


class PositionManager:
    """
    Manages positions and capital for a sub-account
    
    Tracks:
    - Open positions per symbol
    - Capital per symbol
    - Position history
    - Capital changes (P&L tracking)
    """
    
    def __init__(self, account_name: str, symbols: List[str], 
                 initial_capital_per_symbol: float):
        """
        Initialize Position Manager
        
        Args:
            account_name: Sub-account name (e.g., "account_1", "account_2")
            symbols: List of symbols to track (e.g., ["SOLUSD", "AAVEUSD"])
            initial_capital_per_symbol: Starting capital per symbol (e.g., 100)
        """
        self.account_name = account_name
        self.symbols = symbols
        self.initial_capital_per_symbol = initial_capital_per_symbol
        
        # Initialize capital tracker (per symbol)
        self.capital = {symbol: initial_capital_per_symbol for symbol in symbols}
        self.peak_capital = {symbol: initial_capital_per_symbol for symbol in symbols}
        
        # Open positions (symbol -> Position)
        self.positions: Dict[str, Position] = {}
        
        # Position history (for analysis)
        self.closed_positions: List[Dict] = []
        
        # Statistics
        self.stats = {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_pnl': 0.0,
            'total_fees': 0.0,
            'by_symbol': {symbol: {
                'trades': 0,
                'wins': 0,
                'losses': 0,
                'pnl': 0.0
            } for symbol in symbols}
        }
        
        # State persistence
        self.state_manager = get_state_manager()
        
        logger.info(f"Position Manager initialized: {account_name}")
        logger.info(f"   Symbols: {symbols}")
        logger.info(f"   Capital per symbol: ${initial_capital_per_symbol}")
    
    def get_capital(self, symbol: str) -> float:
        """Get available capital for a symbol"""
        if symbol not in self.capital:
            logger.warning(f"Symbol {symbol} not tracked. Returning 0.")
            return 0.0
        return self.capital[symbol]
    
    def get_total_capital(self) -> float:
        """Get total capital across all symbols"""
        return sum(self.capital.values())
    
    def has_position(self, symbol: str) -> bool:
        """Check if has open position for symbol"""
        return symbol in self.positions
    
    def get_position(self, symbol: str) -> Optional[Position]:
        """Get open position for symbol"""
        return self.positions.get(symbol)
    
    def get_all_positions(self) -> Dict[str, Position]:
        """Get all open positions"""
        return self.positions.copy()
    
    def can_enter_position(self, symbol: str, required_capital: float = 0) -> Tuple[bool, str]:
        """
        Check if can enter a new position for symbol
        
        Args:
            symbol: Trading symbol
            required_capital: Capital needed for the trade
        
        Returns:
            Tuple of (can_enter, reason)
        """
        # Check if symbol is tracked
        if symbol not in self.symbols:
            return False, f"Symbol {symbol} not tracked by this account"
        
        # Check if already has position
        if self.has_position(symbol):
            return False, f"Already has position in {symbol}"
        
        # Check if has enough capital
        available = self.get_capital(symbol)
        if required_capital > 0 and available < required_capital:
            return False, f"Insufficient capital: need ${required_capital:.2f}, have ${available:.2f}"
        
        return True, "OK"
    
    def open_position(self, symbol: str, direction: str, entry_price: float,
                     size: int, ob_type: str, leverage: float, capital_used: float,
                     position_value: float, liquidation_level: float,
                     ob_top: float, ob_btm: float, ob_creation_bar: int,
                     entry_bar: int) -> Position:
        """
        Open a new position
        
        Args:
            symbol: Trading symbol
            direction: 'long' or 'short'
            entry_price: Entry price
            size: Number of contracts
            ob_type: 'fresh' or 'breaker'
            leverage: Leverage used
            capital_used: Actual capital used (without leverage)
            position_value: Total position value
            liquidation_level: Liquidation price
            ob_top: OB top price
            ob_btm: OB bottom price
            ob_creation_bar: Bar index when OB was created
            entry_bar: Bar index of entry
        
        Returns:
            Position object
        
        Raises:
            ValueError: If cannot enter position
        """
        # Validate
        can_enter, reason = self.can_enter_position(symbol, capital_used)
        if not can_enter:
            raise ValueError(f"Cannot open position: {reason}")
        
        # Create position
        position = Position(
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            size=size,
            entry_time=datetime.now().isoformat(),
            ob_type=ob_type,
            leverage=leverage,
            capital_used=capital_used,
            position_value=position_value,
            liquidation_level=liquidation_level,
            ob_top=ob_top,
            ob_btm=ob_btm,
            ob_creation_bar=ob_creation_bar,
            entry_bar=entry_bar,
            remaining_size=size  # Initially full size
        )
        
        # Store position
        self.positions[symbol] = position
        
        # No capital deduction here - capital is "in use" but not lost
        # We'll update capital when position closes with P&L
        
        logger.info(f"Position opened: {self.account_name}")
        logger.info(f"   {symbol} {direction.upper()} {size} @ ${entry_price:.4f}")
        logger.info(f"   Type: {ob_type}, Leverage: {leverage}x")
        logger.info(f"   Capital used: ${capital_used:.2f}")
        logger.info(f"   Liquidation: ${liquidation_level:.4f}")
        
        return position
    
    def update_position(self, symbol: str, **kwargs):
        """
        Update position fields
        
        Args:
            symbol: Trading symbol
            **kwargs: Fields to update
        
        Example:
            manager.update_position("SOLUSD", trailing_sl=148.50, highest_price=152.00)
        """
        if symbol not in self.positions:
            logger.warning(f"Cannot update: no position in {symbol}")
            return
        
        position = self.positions[symbol]
        
        for key, value in kwargs.items():
            if hasattr(position, key):
                setattr(position, key, value)
                logger.debug(f"Updated {symbol} position: {key}={value}")
            else:
                logger.warning(f"Position has no attribute: {key}")
    
    def partial_exit_position(self, symbol: str, exit_price: float, 
                             exit_size: int, pnl: float, fees: float = 0):
        """
        Partial exit of position
        
        Args:
            symbol: Trading symbol
            exit_price: Exit price for partial exit
            exit_size: Number of contracts exited
            pnl: P&L from partial exit
            fees: Fees paid for partial exit
        """
        if symbol not in self.positions:
            logger.warning(f"Cannot partial exit: no position in {symbol}")
            return
        
        position = self.positions[symbol]
        
        # Update position
        position.partial_exited = True
        position.partial_exit_price = exit_price
        position.remaining_size = position.size - exit_size
        
        # Update capital with partial P&L
        self.capital[symbol] += pnl - fees
        
        logger.info(f"Partial exit: {self.account_name}")
        logger.info(f"   {symbol} {exit_size} contracts @ ${exit_price:.4f}")
        logger.info(f"   P&L: ${pnl:+.2f}, Fees: ${fees:.2f}")
        logger.info(f"   Remaining: {position.remaining_size} contracts")
        logger.info(f"   Capital: ${self.capital[symbol]:.2f}")
    
    def close_position(self, symbol: str, exit_price: float, exit_reason: str,
                      pnl: float, fees: float = 0, exit_bar: int = 0) -> Dict:
        """
        Close a position completely
        
        Args:
            symbol: Trading symbol
            exit_price: Exit price
            exit_reason: Reason for exit (e.g., 'take_profit', 'stop_loss')
            pnl: Total P&L (including partial exits)
            fees: Total fees
            exit_bar: Bar index of exit
        
        Returns:
            Closed position summary
        """
        if symbol not in self.positions:
            logger.warning(f"Cannot close: no position in {symbol}")
            return {}
        
        position = self.positions[symbol]
        
        # Calculate final metrics
        pnl_pct = (pnl / position.capital_used) * 100 if position.capital_used > 0 else 0
        
        # Update capital
        self.capital[symbol] += pnl - fees
        
        # Update peak capital
        if self.capital[symbol] > self.peak_capital[symbol]:
            self.peak_capital[symbol] = self.capital[symbol]
        
        # Update statistics
        self.stats['total_trades'] += 1
        self.stats['total_pnl'] += pnl
        self.stats['total_fees'] += fees
        
        if pnl > 0:
            self.stats['winning_trades'] += 1
            self.stats['by_symbol'][symbol]['wins'] += 1
        else:
            self.stats['losing_trades'] += 1
            self.stats['by_symbol'][symbol]['losses'] += 1
        
        self.stats['by_symbol'][symbol]['trades'] += 1
        self.stats['by_symbol'][symbol]['pnl'] += pnl
        
        # Create closed position record
        closed_position = {
            **position.to_dict(),
            'exit_price': exit_price,
            'exit_time': datetime.now().isoformat(),
            'exit_reason': exit_reason,
            'exit_bar': exit_bar,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'fees': fees,
            'capital_after': self.capital[symbol]
        }
        
        # Store in history
        self.closed_positions.append(closed_position)
        
        # Remove from active positions
        del self.positions[symbol]
        
        # Log
        result = "WIN" if pnl > 0 else "LOSS"
        logger.info(f"Position closed: {self.account_name}")
        logger.info(f"   {symbol} {position.direction.upper()} @ ${exit_price:.4f}")
        logger.info(f"   Result: {result} ${pnl:+.2f} ({pnl_pct:+.1f}%)")
        logger.info(f"   Reason: {exit_reason}")
        logger.info(f"   Capital: ${self.capital[symbol]:.2f}")
        
        return closed_position
    
    def get_statistics(self) -> Dict:
        """Get trading statistics"""
        total_capital = self.get_total_capital()
        total_initial = sum(self.initial_capital_per_symbol for _ in self.symbols)
        total_return = ((total_capital - total_initial) / total_initial * 100) if total_initial > 0 else 0
        
        win_rate = (self.stats['winning_trades'] / self.stats['total_trades'] * 100) if self.stats['total_trades'] > 0 else 0
        
        return {
            'account': self.account_name,
            'total_trades': self.stats['total_trades'],
            'winning_trades': self.stats['winning_trades'],
            'losing_trades': self.stats['losing_trades'],
            'win_rate': win_rate,
            'total_pnl': self.stats['total_pnl'],
            'total_fees': self.stats['total_fees'],
            'net_pnl': self.stats['total_pnl'] - self.stats['total_fees'],
            'total_capital': total_capital,
            'initial_capital': total_initial,
            'total_return_pct': total_return,
            'capital_by_symbol': self.capital.copy(),
            'stats_by_symbol': self.stats['by_symbol'].copy(),
            'open_positions': len(self.positions)
        }
    
    def save_state(self):
        """Save position state to disk"""
        state = {
            'account_name': self.account_name,
            'symbols': self.symbols,
            'capital': self.capital,
            'peak_capital': self.peak_capital,
            'positions': {symbol: pos.to_dict() for symbol, pos in self.positions.items()},
            'stats': self.stats,
            'timestamp': datetime.now().isoformat()
        }
        
        # Save using state manager
        filename = f"positions_{self.account_name}.json"
        filepath = Path("data") / self.account_name / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(state, f, indent=2)
        
        logger.debug(f"Saved position state: {filepath}")
    
    def load_state(self) -> bool:
        """Load position state from disk"""
        filename = f"positions_{self.account_name}.json"
        filepath = Path("data") / self.account_name / filename
        
        if not filepath.exists():
            logger.warning(f"No saved state found: {filepath}")
            return False
        
        try:
            with open(filepath, 'r') as f:
                state = json.load(f)
            
            # Restore state
            self.capital = state.get('capital', self.capital)
            self.peak_capital = state.get('peak_capital', self.peak_capital)
            self.stats = state.get('stats', self.stats)
            
            # Restore positions
            positions_data = state.get('positions', {})
            self.positions = {
                symbol: Position.from_dict(pos_data)
                for symbol, pos_data in positions_data.items()
            }
            
            logger.info(f"Loaded position state: {filepath}")
            logger.info(f"   Open positions: {len(self.positions)}")
            logger.info(f"   Total capital: ${self.get_total_capital():.2f}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            return False


# Example usage and testing
if __name__ == "__main__":
    print("\n" + "="*80)
    print("Testing Position Manager")
    print("="*80 + "\n")
    
    # Create position manager
    manager = PositionManager(
        account_name="account_1",
        symbols=["SOLUSD", "AAVEUSD"],
        initial_capital_per_symbol=100
    )
    
    print("1. Initial State:")
    print(f"   Total capital: ${manager.get_total_capital():.2f}")
    print(f"   SOLUSD capital: ${manager.get_capital('SOLUSD'):.2f}")
    print(f"   AAVEUSD capital: ${manager.get_capital('AAVEUSD'):.2f}")
    print()
    
    # Test opening position
    print("2. Opening Position:")
    can_enter, reason = manager.can_enter_position("SOLUSD", required_capital=30)
    print(f"   Can enter: {can_enter} ({reason})")
    
    if can_enter:
        position = manager.open_position(
            symbol="SOLUSD",
            direction="long",
            entry_price=150.50,
            size=53,
            ob_type="fresh",
            leverage=20,
            capital_used=40.0,
            position_value=7976.50,
            liquidation_level=142.50,
            ob_top=150.0,
            ob_btm=148.5,
            ob_creation_bar=100,
            entry_bar=105
        )
        print(f"   Position opened: {position.symbol}")
    print()
    
    # Test checking position
    print("3. Checking Position:")
    has_pos = manager.has_position("SOLUSD")
    print(f"   Has SOLUSD position: {has_pos}")
    
    pos = manager.get_position("SOLUSD")
    if pos:
        print(f"   Direction: {pos.direction}")
        print(f"   Size: {pos.size} contracts")
        print(f"   Entry: ${pos.entry_price:.4f}")
    print()
    
    # Test partial exit
    print("4. Partial Exit:")
    manager.partial_exit_position(
        symbol="SOLUSD",
        exit_price=155.0,
        exit_size=26,  # Exit 50%
        pnl=120.0,
        fees=2.0
    )
    
    pos = manager.get_position("SOLUSD")
    print(f"   Remaining size: {pos.remaining_size}")
    print(f"   Capital after partial: ${manager.get_capital('SOLUSD'):.2f}")
    print()
    
    # Test full close
    print("5. Closing Position:")
    closed = manager.close_position(
        symbol="SOLUSD",
        exit_price=157.0,
        exit_reason="take_profit",
        pnl=180.0,  # Total P&L
        fees=3.0,   # Total fees
        exit_bar=120
    )
    
    print(f"   Position closed")
    print(f"   Final P&L: ${closed['pnl']:+.2f}")
    print(f"   Capital after close: ${manager.get_capital('SOLUSD'):.2f}")
    print()
    
    # Test statistics
    print("6. Statistics:")
    stats = manager.get_statistics()
    print(f"   Total trades: {stats['total_trades']}")
    print(f"   Win rate: {stats['win_rate']:.1f}%")
    print(f"   Net P&L: ${stats['net_pnl']:+.2f}")
    print(f"   Total return: {stats['total_return_pct']:+.1f}%")
    print()
    
    # Test save/load
    print("7. Save/Load State:")
    manager.save_state()
    print("   State saved")
    
    # Create new manager and load
    manager2 = PositionManager(
        account_name="account_1",
        symbols=["SOLUSD", "AAVEUSD"],
        initial_capital_per_symbol=100
    )
    loaded = manager2.load_state()
    print(f"   State loaded: {loaded}")
    print(f"   Capital after load: ${manager2.get_capital('SOLUSD'):.2f}")
    print()
    
    print("="*80)
    print("All tests completed!")
    print("="*80 + "\n")