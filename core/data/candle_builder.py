#!/usr/bin/env python3
"""
Candle Builder - Build OHLCV candles from tick data

Takes real-time tick data and constructs 15-minute candles.
Emits "candle_closed" events when candles are complete.

Features:
- Build 15m candles from ticks
- Handle multiple symbols simultaneously
- Validate candle completeness
- Emit events on candle close
- Track candle state
"""

import asyncio
from typing import Dict, List, Callable, Optional
from datetime import datetime, timedelta
from collections import defaultdict

from core.data.data_validator import DataValidator
from core.utils.logger import get_logger

logger = get_logger('system')


class CandleBuilder:
    """
    Builds OHLCV candles from real-time tick data
    """
    
    def __init__(self, timeframe_minutes: int = 15,
                 on_candle_closed: Optional[Callable[[str, Dict], None]] = None):
        """
        Initialize candle builder
        
        Args:
            timeframe_minutes: Candle timeframe in minutes (default: 15)
            on_candle_closed: Callback when candle closes (symbol, candle)
        """
        self.timeframe_minutes = timeframe_minutes
        self.timeframe_seconds = timeframe_minutes * 60
        self.on_candle_closed = on_candle_closed
        
        # Current candles being built (one per symbol)
        self.current_candles: Dict[str, Dict] = {}
        
        # Completed candles history
        self.completed_candles: Dict[str, List[Dict]] = defaultdict(list)
        
        # Statistics
        self.stats = {
            'ticks_processed': 0,
            'candles_completed': 0,
            'candles_by_symbol': defaultdict(int)
        }
        
        # Data validator
        self.validator = DataValidator(timeframe_minutes=timeframe_minutes)
        
        logger.info(f"Candle builder initialized: {timeframe_minutes}m timeframe")
    
    def _get_candle_start_time(self, timestamp: int) -> int:
        """
        Get the start time of the candle that contains this timestamp
        
        Args:
            timestamp: Unix timestamp
        
        Returns:
            Unix timestamp of candle start
        
        Example:
            If timeframe is 15m and timestamp is 10:07:30,
            returns 10:00:00 (start of 10:00-10:15 candle)
        """
        # Round down to nearest timeframe interval
        return (timestamp // self.timeframe_seconds) * self.timeframe_seconds
    
    def _is_new_candle(self, symbol: str, timestamp: int) -> bool:
        """
        Check if this timestamp belongs to a new candle
        
        Args:
            symbol: Trading symbol
            timestamp: Unix timestamp
        
        Returns:
            True if this starts a new candle
        """
        if symbol not in self.current_candles:
            return True
        
        current_candle = self.current_candles[symbol]
        current_start = current_candle['candle_start']
        new_start = self._get_candle_start_time(timestamp)
        
        return new_start != current_start
    
    def _create_new_candle(self, symbol: str, timestamp: int, price: float) -> Dict:
        """
        Create a new candle
        
        Args:
            symbol: Trading symbol
            timestamp: Unix timestamp
            price: Current price
        
        Returns:
            New candle dictionary
        """
        candle_start = self._get_candle_start_time(timestamp)
        
        candle = {
            'timestamp': candle_start,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': 0.0,  # We don't get volume from mark_price ticks
            'tick_count': 0,
            'candle_start': candle_start,
            'candle_end': candle_start + self.timeframe_seconds
        }
        
        return candle
    
    def _update_candle(self, candle: Dict, price: float):
        """
        Update candle with new tick
        
        Args:
            candle: Current candle dictionary
            price: New price
        """
        # Update OHLC
        candle['high'] = max(candle['high'], price)
        candle['low'] = min(candle['low'], price)
        candle['close'] = price
        candle['tick_count'] += 1
    
    def _finalize_candle(self, candle: Dict) -> Dict:
        """
        Finalize candle for output
        
        Args:
            candle: Candle to finalize
        
        Returns:
            Cleaned candle dictionary
        """
        # Remove internal tracking fields
        finalized = {
            'timestamp': candle['timestamp'],
            'open': candle['open'],
            'high': candle['high'],
            'low': candle['low'],
            'close': candle['close'],
            'volume': candle['volume']
        }
        
        # Validate candle
        is_valid, errors = self.validator.validate_ohlcv_candle(finalized)
        
        if not is_valid:
            logger.warning(f"⚠️  Invalid candle detected: {errors}")
        
        return finalized
    
    def process_tick(self, tick: Dict):
        """
        Process a tick and update candles
        
        Args:
            tick: Tick dictionary with symbol, price, timestamp
        """
        try:
            symbol = tick.get('symbol')
            price = tick.get('price')
            timestamp = tick.get('timestamp')
            
            if not all([symbol, price is not None, timestamp]):
                logger.warning(f"Incomplete tick data: {tick}")
                return
            
            # Convert timestamp to Unix timestamp if needed
            if isinstance(timestamp, str):
                # ISO format timestamp
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                timestamp = int(dt.timestamp())
            elif isinstance(timestamp, datetime):
                timestamp = int(timestamp.timestamp())
            else:
                timestamp = int(timestamp)
            
            # Check if this tick starts a new candle
            if self._is_new_candle(symbol, timestamp):
                # Close previous candle if exists
                if symbol in self.current_candles:
                    self._close_candle(symbol)
                
                # Create new candle
                self.current_candles[symbol] = self._create_new_candle(symbol, timestamp, price)
                
                # Log new candle start
                candle_time = datetime.fromtimestamp(self.current_candles[symbol]['candle_start'])
                logger.debug(f"🕐 New candle started: {symbol} at {candle_time.strftime('%Y-%m-%d %H:%M')}")
            
            # Update current candle
            if symbol in self.current_candles:
                self._update_candle(self.current_candles[symbol], price)
            
            # Update stats
            self.stats['ticks_processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing tick: {e}")
            logger.debug(f"Problematic tick: {tick}")
    
    def _close_candle(self, symbol: str):
        """
        Close and finalize a candle
        
        Args:
            symbol: Trading symbol
        """
        if symbol not in self.current_candles:
            return
        
        candle = self.current_candles[symbol]
        
        # Finalize candle
        finalized_candle = self._finalize_candle(candle)
        
        # Add to completed candles
        self.completed_candles[symbol].append(finalized_candle)
        
        # Update stats
        self.stats['candles_completed'] += 1
        self.stats['candles_by_symbol'][symbol] += 1
        
        # Log candle close
        candle_time = datetime.fromtimestamp(finalized_candle['timestamp'])
        logger.info(f"✅ Candle closed: {symbol} @ {candle_time.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"   O: ${finalized_candle['open']:.4f} | "
                   f"H: ${finalized_candle['high']:.4f} | "
                   f"L: ${finalized_candle['low']:.4f} | "
                   f"C: ${finalized_candle['close']:.4f}")
        logger.info(f"   Ticks: {candle['tick_count']}")
        
        # Call user callback
        if self.on_candle_closed:
            try:
                self.on_candle_closed(symbol, finalized_candle)
            except Exception as e:
                logger.error(f"Error in candle_closed callback: {e}")
    
    def get_current_candles(self) -> Dict[str, Dict]:
        """
        Get current (incomplete) candles
        
        Returns:
            Dictionary mapping symbol to current candle
        """
        return {
            symbol: self._finalize_candle(candle)
            for symbol, candle in self.current_candles.items()
        }
    
    def get_completed_candles(self, symbol: str = None) -> Dict[str, List[Dict]]:
        """
        Get completed candles
        
        Args:
            symbol: Optional symbol filter
        
        Returns:
            Dictionary mapping symbol to list of candles
        """
        if symbol:
            return {symbol: self.completed_candles.get(symbol, [])}
        return dict(self.completed_candles)
    
    def get_stats(self) -> Dict:
        """Get candle builder statistics"""
        return {
            'ticks_processed': self.stats['ticks_processed'],
            'candles_completed': self.stats['candles_completed'],
            'candles_by_symbol': dict(self.stats['candles_by_symbol']),
            'current_candles': len(self.current_candles)
        }
    
    def force_close_all(self):
        """Force close all open candles (useful for shutdown)"""
        logger.info("🛑 Force closing all open candles")
        
        for symbol in list(self.current_candles.keys()):
            self._close_candle(symbol)
        
        self.current_candles.clear()


# Example usage and testing
if __name__ == "__main__":
    import time
    
    print("\n🧪 Testing Candle Builder\n")
    
    def on_candle_closed_handler(symbol: str, candle: Dict):
        """Example candle closed handler"""
        print(f"\n🎉 CANDLE CLOSED: {symbol}")
        print(f"   Timestamp: {datetime.fromtimestamp(candle['timestamp'])}")
        print(f"   OHLC: O={candle['open']:.4f}, H={candle['high']:.4f}, "
              f"L={candle['low']:.4f}, C={candle['close']:.4f}")
        print()
    
    # Create candle builder
    builder = CandleBuilder(
        timeframe_minutes=15,
        on_candle_closed=on_candle_closed_handler
    )
    
    print("Simulating ticks for 15-minute candle...\n")
    
    # Simulate ticks
    base_time = int(time.time())
    base_time = (base_time // 900) * 900  # Round to 15m boundary
    
    # First candle (15 minutes)
    print("📊 Building first candle...")
    for i in range(10):
        tick = {
            'symbol': 'SOLUSD',
            'price': 150.0 + (i * 0.5),  # Price going up
            'timestamp': base_time + (i * 60)  # Every minute
        }
        builder.process_tick(tick)
        print(f"   Tick {i+1}: ${tick['price']:.2f} at {datetime.fromtimestamp(tick['timestamp']).strftime('%H:%M:%S')}")
    
    print("\n   Current candle state:")
    current = builder.get_current_candles()
    if 'SOLUSD' in current:
        c = current['SOLUSD']
        print(f"   OHLC: O={c['open']:.4f}, H={c['high']:.4f}, L={c['low']:.4f}, C={c['close']:.4f}")
    
    # Trigger new candle (15 minutes later)
    print("\n📊 Triggering new candle (15 minutes later)...")
    new_candle_tick = {
        'symbol': 'SOLUSD',
        'price': 155.0,
        'timestamp': base_time + 900  # 15 minutes later
    }
    builder.process_tick(new_candle_tick)
    
    # Second candle
    print("\n📊 Building second candle...")
    for i in range(5):
        tick = {
            'symbol': 'SOLUSD',
            'price': 155.0 - (i * 0.3),  # Price going down
            'timestamp': base_time + 900 + (i * 60)
        }
        builder.process_tick(tick)
        print(f"   Tick {i+1}: ${tick['price']:.2f}")
    
    # Force close remaining candles
    print("\n🛑 Force closing remaining candles...")
    builder.force_close_all()
    
    # Show statistics
    print("\n📊 Statistics:")
    stats = builder.get_stats()
    print(f"   Ticks processed: {stats['ticks_processed']}")
    print(f"   Candles completed: {stats['candles_completed']}")
    print(f"   Candles by symbol: {stats['candles_by_symbol']}")
    
    # Show completed candles
    print("\n📈 Completed Candles:")
    completed = builder.get_completed_candles('SOLUSD')
    for i, candle in enumerate(completed['SOLUSD'], 1):
        print(f"   Candle {i}: {datetime.fromtimestamp(candle['timestamp']).strftime('%H:%M')}")
        print(f"      OHLC: {candle['open']:.4f} / {candle['high']:.4f} / {candle['low']:.4f} / {candle['close']:.4f}")
    
    print("\n✅ Candle builder test complete!")