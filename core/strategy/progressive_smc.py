#!/usr/bin/env python3
"""
Progressive SMC - Real-Time Order Block Detection

Adapted from progressive_smc_simulator.py for real-time event-driven architecture.

Features:
- Processes candles one at a time (event-driven)
- Detects Order Blocks based on BOS/CHoCH
- Tracks OB lifecycle: Fresh → Breaker → Invalidated
- Emits events on OB creation/invalidation
- Maintains state between candles
- Validates against backtest results

Usage:
    smc = ProgressiveSMC(symbol="SOLUSD", timeframe="15m")
    
    # On each new candle
    smc.process_candle(candle)
    
    # Get active OBs
    active_obs = smc.get_active_obs()
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple, Callable
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

from core.utils.logger import get_logger

logger = get_logger('ob_events')


@dataclass
class Structure:
    """Market structure state (BOS/CHoCH tracking)"""
    zn: int = 0           # Zigzag bar index
    zz: float = 0.0       # Zigzag value
    bos: Optional[float] = None     # Break of Structure level
    choch: Optional[float] = None   # Change of Character level
    loc: int = 0          # Structure point location
    temp: int = 0         # Temporary location
    trend: int = 0        # Current trend (1=bull, -1=bear, 0=neutral)
    start: int = 0        # State (0=init, 1=first break, 2=active)
    main: float = 0.0     # Main tracking level
    xloc: int = 0         # Extended location (for sweeps)
    upsweep: bool = False
    dnsweep: bool = False
    txt: str = ""         # Last structure type


@dataclass
class OrderBlock:
    """Order Block representation"""
    bull: bool           # True for bullish OB, False for bearish
    top: float
    btm: float
    avg: float
    loc: int            # Bar index of creation
    css: str            # Color (not used in live trading)
    vol: float          # Volume
    dir: int            # Candle direction (1=green, -1=red)
    move: int = 1
    blPOS: int = 1
    brPOS: int = 1
    xlocbl: int = 0
    xlocbr: int = 0
    isbb: bool = False  # Is breaker block
    bbloc: Optional[int] = None  # Breaker block location
    invalidated: bool = False  # NEW: Track if invalidated
    invalidation_bar: Optional[int] = None  # NEW: When it got invalidated
    
    def get_type(self) -> str:
        """Get OB type as string"""
        if self.invalidated:
            return "invalidated"
        elif self.isbb:
            return "breaker"
        else:
            return "fresh"


class ProgressiveSMC:
    """
    Real-time Order Block detection engine
    
    Processes candles one at a time and maintains OB state.
    """
    
    def __init__(self, symbol: str, timeframe: str = "15m", 
                 on_ob_created: Optional[Callable] = None,
                 on_ob_invalidated: Optional[Callable] = None,
                 on_ob_breaker: Optional[Callable] = None):
        """
        Initialize Progressive SMC engine
        
        Args:
            symbol: Trading symbol (e.g., "SOLUSD")
            timeframe: Candle timeframe (default: "15m")
            on_ob_created: Callback when new OB created
            on_ob_invalidated: Callback when OB invalidated
            on_ob_breaker: Callback when OB becomes breaker
        """
        self.symbol = symbol
        self.timeframe = timeframe
        
        # Event callbacks
        self.on_ob_created = on_ob_created
        self.on_ob_invalidated = on_ob_invalidated
        self.on_ob_breaker = on_ob_breaker
        
        # Pine Script configuration (from backtest)
        self.config = {
            'mslen': 5,                    # Pivot length
            'msmode': 'Adjusted Points',
            'obmode': 'Length',           # OB construction mode
            'obmiti': 'Close',            # Mitigation method
            'buildsweep': True,
            'overlap': True,
            'oblast': 5,
            'len': 5,
        }
        
        # State
        self.current_bar = 0
        self.structure = Structure()
        self.bullish_obs: List[OrderBlock] = []
        self.bearish_obs: List[OrderBlock] = []
        
        # Historical data buffer (needed for pivots and ATR)
        self.candles_buffer: List[Dict] = []
        self.max_buffer_size = 300  # Keep last 300 candles
        
        # Up/down tracking
        self.up = None
        self.dn = None
        
        # Pivot tracking
        self.pivot_highs: List[Tuple[int, float]] = []
        self.pivot_lows: List[Tuple[int, float]] = []
        
        logger.info(f"🚀 Progressive SMC initialized: {symbol} {timeframe}")
    
    def process_candle(self, candle: Dict):
        """
        Process a new candle (main entry point)
        
        Args:
            candle: Dictionary with keys: timestamp, open, high, low, close, volume
        """
        # Validate candle
        required_keys = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        if not all(k in candle for k in required_keys):
            logger.error(f"Invalid candle format: {candle}")
            return
        
        # Add candle to buffer
        self.candles_buffer.append(candle)
        
        # Trim buffer if needed
        if len(self.candles_buffer) > self.max_buffer_size:
            self.candles_buffer.pop(0)
        
        # Skip if not enough data for ATR calculation
        if len(self.candles_buffer) < 200:
            logger.debug(f"Buffering candles: {len(self.candles_buffer)}/200")
            self.current_bar += 1
            return
        
        # Calculate ATR for current candle
        atr = self._calculate_atr()
        if atr is None:
            logger.warning("Failed to calculate ATR")
            self.current_bar += 1
            return
        
        # Add ATR to candle
        candle['atr'] = atr
        
        # Detect pivots
        self._detect_pivots()
        
        # Process market structure
        self._process_market_structure(candle)
        
        # Check OB mitigation/invalidation
        self._check_mitigation(candle)
        
        # Increment bar counter
        self.current_bar += 1
    
    def _calculate_atr(self) -> Optional[float]:
        """Calculate ATR for current candle"""
        if len(self.candles_buffer) < 200:
            return None
        
        try:
            # Use last 200 candles
            recent_candles = self.candles_buffer[-200:]
            
            # Calculate True Range
            tr_list = []
            for i in range(1, len(recent_candles)):
                high = float(recent_candles[i]['high'])
                low = float(recent_candles[i]['low'])
                prev_close = float(recent_candles[i-1]['close'])
                
                tr1 = high - low
                tr2 = abs(high - prev_close)
                tr3 = abs(low - prev_close)
                
                tr = max(tr1, tr2, tr3)
                tr_list.append(tr)
            
            # Average True Range
            base_atr = sum(tr_list) / len(tr_list)
            
            # Apply Pine Script formula: atr / (5/len)
            atr = base_atr / (5 / self.config['len'])
            
            return atr
            
        except Exception as e:
            logger.error(f"Error calculating ATR: {e}")
            return None
    
    def _detect_pivots(self):
        """Detect pivot highs and lows"""
        mslen = self.config['mslen']
        
        # Need enough candles for pivot detection
        if len(self.candles_buffer) < mslen * 2 + 1:
            return
        
        # Check for pivot high at mslen bars back
        center_idx = len(self.candles_buffer) - mslen - 1
        if center_idx >= mslen:
            center_high = self.candles_buffer[center_idx]['high']
            
            is_pivot_high = True
            # Check left side
            for i in range(center_idx - mslen, center_idx):
                if self.candles_buffer[i]['high'] >= center_high:
                    is_pivot_high = False
                    break
            
            # Check right side
            if is_pivot_high:
                for i in range(center_idx + 1, min(center_idx + mslen + 1, len(self.candles_buffer))):
                    if self.candles_buffer[i]['high'] > center_high:
                        is_pivot_high = False
                        break
            
            if is_pivot_high:
                bar_index = self.current_bar - mslen
                self.pivot_highs.append((bar_index, center_high))
        
        # Check for pivot low
        if center_idx >= mslen:
            center_low = self.candles_buffer[center_idx]['low']
            
            is_pivot_low = True
            # Check left side
            for i in range(center_idx - mslen, center_idx):
                if self.candles_buffer[i]['low'] <= center_low:
                    is_pivot_low = False
                    break
            
            # Check right side
            if is_pivot_low:
                for i in range(center_idx + 1, min(center_idx + mslen + 1, len(self.candles_buffer))):
                    if self.candles_buffer[i]['low'] < center_low:
                        is_pivot_low = False
                        break
            
            if is_pivot_low:
                bar_index = self.current_bar - mslen
                self.pivot_lows.append((bar_index, center_low))
    
    def _process_market_structure(self, candle: Dict):
        """Process market structure for current candle"""
        high = float(candle['high'])
        low = float(candle['low'])
        close = float(candle['close'])
        open_price = float(candle['open'])
        
        ms = self.structure
        
        # Initialize up/dn tracking
        if self.up is None:
            self.up = high
        if self.dn is None:
            self.dn = low
        
        crossup = False
        crossdn = False
        
        if high > self.up:
            self.up = high
            self.dn = low
            crossup = True
        
        if low < self.dn:
            self.up = high
            self.dn = low
            crossdn = True
        
        # Reset sweep flags
        ms.upsweep = False
        ms.dnsweep = False
        
        # STATE 0: Initialization
        if ms.start == 0:
            ms.start = 1
            ms.trend = 0
            ms.bos = high
            ms.choch = low
            ms.loc = self.current_bar
            ms.temp = self.current_bar
            ms.main = 0
            ms.xloc = self.current_bar
            return
        
        # STATE 1: Waiting for first break
        if ms.start == 1:
            # Check for sweeps
            if self.config['buildsweep']:
                if low <= ms.choch and close >= ms.choch:
                    ms.dnsweep = True
                    ms.choch = low
                    ms.xloc = self.current_bar
                    return
                
                if high >= ms.bos and close <= ms.bos:
                    ms.upsweep = True
                    ms.bos = high
                    ms.xloc = self.current_bar
                    return
            
            # Check for actual breaks
            if close <= ms.choch:
                # Bearish CHoCH
                ms.txt = "choch"
                self._create_order_block(ms, True)  # Create bullish OB
                
                ms.trend = -1
                ms.choch = ms.bos
                ms.bos = None
                ms.start = 2
                ms.loc = self.current_bar
                ms.main = low
                ms.temp = ms.loc
                ms.xloc = self.current_bar
                
            elif close >= ms.bos:
                # Bullish CHoCH
                ms.txt = "choch"
                self._create_order_block(ms, False)  # Create bearish OB
                
                ms.trend = 1
                ms.choch = ms.choch
                ms.bos = None
                ms.start = 2
                ms.loc = self.current_bar
                ms.main = high
                ms.temp = ms.loc
                ms.xloc = self.current_bar
        
        # STATE 2: Active tracking
        elif ms.start == 2:
            if ms.trend == -1:  # Bearish trend
                # Track main low
                if low <= ms.main:
                    ms.main = low
                    ms.temp = self.current_bar
                
                # Check for BOS formation
                if ms.bos is None:
                    if crossup and close > open_price and self.current_bar > 0:
                        prev_candle = self.candles_buffer[-2]
                        if prev_candle['close'] > prev_candle['open']:
                            ms.bos = ms.main
                            ms.loc = ms.temp
                            ms.xloc = ms.loc
                
                # Check for BOS break
                if ms.bos is not None:
                    if self.config['buildsweep'] and low <= ms.bos and close >= ms.bos:
                        ms.dnsweep = True
                        ms.bos = low
                        ms.xloc = self.current_bar
                        return
                    
                    if close <= ms.bos:
                        ms.txt = "bos"
                        ms.zz = ms.bos
                        ms.zn = self.current_bar
                        self._create_order_block(ms, False)  # Bearish OB
                        
                        ms.xloc = self.current_bar
                        ms.bos = None
                
                # Check for CHoCH (trend change)
                if self.config['buildsweep'] and high >= ms.choch and close <= ms.choch:
                    ms.upsweep = True
                    ms.choch = high
                    ms.xloc = self.current_bar
                    return
                
                if close >= ms.choch:
                    ms.txt = "choch"
                    ms.zz = ms.choch
                    ms.zn = self.current_bar
                    self._create_order_block(ms, True)  # Bullish OB
                    
                    if ms.bos is None:
                        ms.choch = ms.choch
                    else:
                        ms.choch = ms.bos
                    
                    ms.bos = None
                    ms.main = high
                    ms.trend = 1
                    ms.loc = self.current_bar
                    ms.xloc = self.current_bar
                    ms.temp = ms.loc
            
            elif ms.trend == 1:  # Bullish trend
                # Track main high
                if high >= ms.main:
                    ms.main = high
                    ms.temp = self.current_bar
                
                # Check for BOS formation
                if ms.bos is None:
                    if crossdn and close < open_price and self.current_bar > 0:
                        prev_candle = self.candles_buffer[-2]
                        if prev_candle['close'] < prev_candle['open']:
                            ms.bos = ms.main
                            ms.loc = ms.temp
                            ms.xloc = ms.loc
                
                # Check for BOS break
                if ms.bos is not None:
                    if self.config['buildsweep'] and high >= ms.bos and close <= ms.bos:
                        ms.upsweep = True
                        ms.bos = high
                        ms.xloc = self.current_bar
                        return
                    
                    if close >= ms.bos:
                        ms.txt = "bos"
                        ms.zz = ms.bos
                        ms.zn = self.current_bar
                        self._create_order_block(ms, True)  # Bullish OB
                        
                        ms.xloc = self.current_bar
                        ms.bos = None
                
                # Check for CHoCH (trend change)
                if self.config['buildsweep'] and low <= ms.choch and close >= ms.choch:
                    ms.dnsweep = True
                    ms.choch = low
                    ms.xloc = self.current_bar
                    return
                
                if close <= ms.choch:
                    ms.txt = "choch"
                    ms.zz = ms.choch
                    ms.zn = self.current_bar
                    self._create_order_block(ms, False)  # Bearish OB
                    
                    if ms.bos is None:
                        ms.choch = ms.choch
                    else:
                        ms.choch = ms.bos
                    
                    ms.bos = None
                    ms.main = low
                    ms.trend = -1
                    ms.loc = self.current_bar
                    ms.temp = ms.loc
                    ms.xloc = self.current_bar
    
    def _create_order_block(self, ms: Structure, is_bullish: bool):
        """Create a new Order Block"""
        try:
            if is_bullish:
                # Find bullish OB coordinates
                idx = self._find_structure_point(ms, False, False)
                actual_idx = max(0, len(self.candles_buffer) - idx - 1)
                
                if actual_idx >= len(self.candles_buffer):
                    return
                
                candle = self.candles_buffer[actual_idx]
                high_val = float(candle['high'])
                low_val = float(candle['low'])
                atr_val = float(candle.get('atr', 0))
                
                # Calculate coordinates
                if self.config['obmode'] == 'Length':
                    top = high_val if (low_val + atr_val) > high_val else (low_val + atr_val)
                    btm = low_val
                else:
                    top = high_val
                    btm = low_val
                
                # Create OB
                ob = OrderBlock(
                    bull=True,
                    top=top,
                    btm=btm,
                    avg=(top + btm) / 2,
                    loc=self.current_bar - idx,
                    css="bullish",
                    vol=float(candle.get('volume', 0)),
                    dir=1 if candle['close'] > candle['open'] else -1,
                    xlocbl=self.current_bar - idx,
                    xlocbr=self.current_bar - idx
                )
                
                self.bullish_obs.insert(0, ob)
                
                # Emit event
                logger.info(f"🟢 BULLISH OB CREATED: {self.symbol} @ bar {ob.loc}")
                logger.info(f"   Top: ${top:.4f} | Bottom: ${btm:.4f}")
                
                if self.on_ob_created:
                    self.on_ob_created(self.symbol, ob, "bullish")
                
            else:
                # Find bearish OB coordinates
                idx = self._find_structure_point(ms, True, False)
                actual_idx = max(0, len(self.candles_buffer) - idx - 1)
                
                if actual_idx >= len(self.candles_buffer):
                    return
                
                candle = self.candles_buffer[actual_idx]
                high_val = float(candle['high'])
                low_val = float(candle['low'])
                atr_val = float(candle.get('atr', 0))
                
                # Calculate coordinates
                if self.config['obmode'] == 'Length':
                    btm = low_val if (high_val - atr_val) < low_val else (high_val - atr_val)
                    top = high_val
                else:
                    top = high_val
                    btm = low_val
                
                # Create OB
                ob = OrderBlock(
                    bull=False,
                    top=top,
                    btm=btm,
                    avg=(top + btm) / 2,
                    loc=self.current_bar - idx,
                    css="bearish",
                    vol=float(candle.get('volume', 0)),
                    dir=1 if candle['close'] > candle['open'] else -1,
                    xlocbl=self.current_bar - idx,
                    xlocbr=self.current_bar - idx
                )
                
                self.bearish_obs.insert(0, ob)
                
                # Emit event
                logger.info(f"🔴 BEARISH OB CREATED: {self.symbol} @ bar {ob.loc}")
                logger.info(f"   Top: ${top:.4f} | Bottom: ${btm:.4f}")
                
                if self.on_ob_created:
                    self.on_ob_created(self.symbol, ob, "bearish")
        
        except Exception as e:
            logger.error(f"Error creating OB: {e}")
    
    def _find_structure_point(self, ms: Structure, use_max: bool, sweep: bool = False) -> int:
        """Find structure point for OB creation"""
        min_val = 99999999.0
        max_val = 0.0
        idx = 0
        
        loc_to_use = ms.xloc if sweep else ms.loc
        search_range = max(1, self.current_bar - loc_to_use)
        
        if use_max:
            # Find highest point
            for i in range(search_range):
                candle_idx = len(self.candles_buffer) - 1 - i
                if candle_idx >= 0:
                    high_val = self.candles_buffer[candle_idx]['high']
                    if high_val > max_val:
                        max_val = high_val
                        idx = i
        else:
            # Find lowest point
            for i in range(search_range):
                candle_idx = len(self.candles_buffer) - 1 - i
                if candle_idx >= 0:
                    low_val = self.candles_buffer[candle_idx]['low']
                    if low_val < min_val:
                        min_val = low_val
                        idx = i
        
        return idx
    
    def _check_mitigation(self, candle: Dict):
        """Check OB mitigation/invalidation"""
        high = float(candle['high'])
        low = float(candle['low'])
        close = float(candle['close'])
        open_price = float(candle['open'])
        
        # Check bullish OBs
        to_remove_bullish = []
        for i, ob in enumerate(self.bullish_obs):
            if ob.invalidated:
                continue
            
            if not ob.isbb:  # Not yet a breaker block
                # Check mitigation
                if (self.config['obmiti'] == 'Close' and min(close, open_price) < ob.btm) or \
                   (self.config['obmiti'] == 'Wick' and low < ob.btm) or \
                   (self.config['obmiti'] == 'Avg' and low < ob.avg):
                    
                    ob.isbb = True
                    ob.bbloc = self.current_bar
                    
                    logger.info(f"🔄 BULLISH OB → BREAKER: {self.symbol}")
                    logger.info(f"   Bar: {self.current_bar}")
                    
                    if self.on_ob_breaker:
                        self.on_ob_breaker(self.symbol, ob, "bullish")
            
            else:  # Already a breaker - check invalidation
                if (self.config['obmiti'] == 'Close' and max(close, open_price) > ob.top) or \
                   (self.config['obmiti'] == 'Wick' and high > ob.top) or \
                   (self.config['obmiti'] == 'Avg' and high > ob.avg):
                    
                    ob.invalidated = True
                    ob.invalidation_bar = self.current_bar
                    to_remove_bullish.append(i)
                    
                    logger.info(f"❌ BULLISH OB INVALIDATED: {self.symbol}")
                    logger.info(f"   Bar: {self.current_bar}")
                    
                    if self.on_ob_invalidated:
                        self.on_ob_invalidated(self.symbol, ob, "bullish")
        
        # Remove invalidated bullish OBs
        for i in reversed(to_remove_bullish):
            self.bullish_obs.pop(i)
        
        # Check bearish OBs
        to_remove_bearish = []
        for i, ob in enumerate(self.bearish_obs):
            if ob.invalidated:
                continue
            
            if not ob.isbb:  # Not yet a breaker block
                # Check mitigation
                if (self.config['obmiti'] == 'Close' and max(close, open_price) > ob.top) or \
                   (self.config['obmiti'] == 'Wick' and high > ob.top) or \
                   (self.config['obmiti'] == 'Avg' and high > ob.avg):
                    
                    ob.isbb = True
                    ob.bbloc = self.current_bar
                    
                    logger.info(f"🔄 BEARISH OB → BREAKER: {self.symbol}")
                    logger.info(f"   Bar: {self.current_bar}")
                    
                    if self.on_ob_breaker:
                        self.on_ob_breaker(self.symbol, ob, "bearish")
            
            else:  # Already a breaker - check invalidation
                if (self.config['obmiti'] == 'Close' and min(close, open_price) < ob.btm) or \
                   (self.config['obmiti'] == 'Wick' and low < ob.btm) or \
                   (self.config['obmiti'] == 'Avg' and low < ob.avg):
                    
                    ob.invalidated = True
                    ob.invalidation_bar = self.current_bar
                    to_remove_bearish.append(i)
                    
                    logger.info(f"❌ BEARISH OB INVALIDATED: {self.symbol}")
                    logger.info(f"   Bar: {self.current_bar}")
                    
                    if self.on_ob_invalidated:
                        self.on_ob_invalidated(self.symbol, ob, "bearish")
        
        # Remove invalidated bearish OBs
        for i in reversed(to_remove_bearish):
            self.bearish_obs.pop(i)
    
    def get_active_obs(self) -> Dict[str, List[OrderBlock]]:
        """Get active (non-invalidated) OBs"""
        return {
            'bullish': [ob for ob in self.bullish_obs if not ob.invalidated],
            'bearish': [ob for ob in self.bearish_obs if not ob.invalidated]
        }
    
    def get_all_obs(self) -> Dict[str, List[OrderBlock]]:
        """Get all OBs (including invalidated)"""
        return {
            'bullish': self.bullish_obs.copy(),
            'bearish': self.bearish_obs.copy()
        }
    
    def get_market_structure(self) -> Dict:
        """Get current market structure state"""
        return {
            'trend': self.structure.trend,
            'bos_level': self.structure.bos,
            'choch_level': self.structure.choch,
            'last_structure': self.structure.txt
        }


# Example usage and testing
if __name__ == "__main__":
    import json
    from pathlib import Path
    
    print("=" * 80)
    print("🧪 TESTING PROGRESSIVE SMC - REAL-TIME MODE")
    print("=" * 80)
    print()
    
    # Callbacks for events
    def on_ob_created_handler(symbol, ob, direction):
        print(f"  🎉 NEW {direction.upper()} OB: ${ob.btm:.2f} - ${ob.top:.2f}")
    
    def on_ob_breaker_handler(symbol, ob, direction):
        print(f"  🔄 {direction.upper()} OB became BREAKER")
    
    def on_ob_invalidated_handler(symbol, ob, direction):
        print(f"  ❌ {direction.upper()} OB INVALIDATED")
    
    # Create SMC engine
    smc = ProgressiveSMC(
        symbol="SOLUSD",
        timeframe="15m",
        on_ob_created=on_ob_created_handler,
        on_ob_breaker=on_ob_breaker_handler,
        on_ob_invalidated=on_ob_invalidated_handler
    )
    
    # Load historical data
    data_file = Path("data/historical/SOLUSD_15m_2025-04-04_to_2025-10-01.json")
    
    if data_file.exists():
        print("📂 Loading historical data...")
        with open(data_file, 'r') as f:
            data = json.load(f)
        
        candles = data['candles']
        print(f"✅ Loaded {len(candles)} candles")
        print()
        
        # Process candles one by one (simulating real-time)
        print("🔄 Processing candles...")
        print()
        
        for i, candle in enumerate(candles):
            smc.process_candle(candle)
            
            # Print progress every 1000 candles
            if (i + 1) % 1000 == 0:
                active_obs = smc.get_active_obs()
                total_active = len(active_obs['bullish']) + len(active_obs['bearish'])
                print(f"  Bar {i+1}: {total_active} active OBs")
        
        print()
        print("=" * 80)
        print("📊 FINAL RESULTS")
        print("=" * 80)
        
        # Get final state
        active_obs = smc.get_active_obs()
        all_obs = smc.get_all_obs()
        structure = smc.get_market_structure()
        
        print(f"\nActive OBs:")
        print(f"  Bullish: {len(active_obs['bullish'])}")
        print(f"  Bearish: {len(active_obs['bearish'])}")
        
        print(f"\nTotal OBs (including invalidated):")
        print(f"  Bullish: {len(all_obs['bullish'])}")
        print(f"  Bearish: {len(all_obs['bearish'])}")
        
        print(f"\nMarket Structure:")
        print(f"  Trend: {structure['trend']} (1=bull, -1=bear, 0=neutral)")
        print(f"  Last structure: {structure['last_structure']}")
        
        print()
        print("✅ Progressive SMC test complete!")
        
    else:
        print(f"❌ Data file not found: {data_file}")
        print("   Run: python scripts/test_historical_loader.py first")