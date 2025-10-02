#!/usr/bin/env python3
"""
Progressive SMC - Real-Time Order Block Detection (FIXED)

Adapted from progressive_smc_simulator.py with proven logic.
This version correctly detects BOTH bullish and bearish OBs.

Key fixes:
- No buffer limit (handles 6+ months of data)
- Proper OB coordinate adjustment (lines 377-393 from Pine Script)
- Correct structure point finding
- Full event callbacks

Usage:
    smc = ProgressiveSMC(symbol="SOLUSD", timeframe="15m")
    
    # Load 6 months historical
    for candle in historical_candles:
        smc.process_candle(candle)
    
    # Process live candles
    smc.process_candle(new_candle)
    
    # Get active OBs
    active = smc.get_active_obs()
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple, Callable
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


@dataclass
class Structure:
    """Market structure state (replicates Pine Script structure type)"""
    zn: int = 0
    zz: float = 0.0
    bos: Optional[float] = None
    choch: Optional[float] = None
    loc: int = 0
    temp: int = 0
    trend: int = 0
    start: int = 0
    main: float = 0.0
    xloc: int = 0
    upsweep: bool = False
    dnsweep: bool = False
    txt: str = ""


@dataclass
class OrderBlock:
    """Order Block representation"""
    bull: bool
    top: float
    btm: float
    avg: float
    loc: int
    css: str
    vol: float
    dir: int
    move: int = 1
    blPOS: int = 1
    brPOS: int = 1
    xlocbl: int = 0
    xlocbr: int = 0
    isbb: bool = False
    bbloc: Optional[int] = None
    invalidated: bool = False
    invalidation_bar: Optional[int] = None
    
    def get_type(self) -> str:
        if self.invalidated:
            return "invalidated"
        elif self.isbb:
            return "breaker"
        else:
            return "fresh"


class ProgressiveSMC:
    """
    Real-time Order Block detection engine
    Based on proven progressive_smc_simulator.py logic
    """
    
    def __init__(self, symbol: str, timeframe: str = "15m",
                 on_ob_created: Optional[Callable] = None,
                 on_ob_invalidated: Optional[Callable] = None,
                 on_ob_breaker: Optional[Callable] = None):
        """
        Initialize Progressive SMC engine
        
        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            on_ob_created: Callback(symbol, ob, direction)
            on_ob_invalidated: Callback(symbol, ob, direction)
            on_ob_breaker: Callback(symbol, ob, direction)
        """
        self.symbol = symbol
        self.timeframe = timeframe
        
        # Event callbacks
        self.on_ob_created = on_ob_created
        self.on_ob_invalidated = on_ob_invalidated
        self.on_ob_breaker = on_ob_breaker
        
        # Pine Script configuration (exact match)
        self.config = {
            'mslen': 5,
            'msmode': 'Adjusted Points',
            'obmode': 'Length',
            'obmiti': 'Close',
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
        
        # Candle buffer (NO LIMIT - store all historical data)
        self.candles_buffer: List[Dict] = []
        
        # Up/down tracking
        self.up = None
        self.dn = None
        
        # Pivot tracking
        self.pivot_highs: List[Tuple[int, float]] = []
        self.pivot_lows: List[Tuple[int, float]] = []
        
        print(f"Progressive SMC initialized: {symbol} {timeframe}")
        print(f"Buffer limit: UNLIMITED (for historical + live data)")
    
    def process_candle(self, candle: Dict):
        """Process a new candle (main entry point)"""
        # Validate
        required = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        if not all(k in candle for k in required):
            print(f"ERROR: Invalid candle format")
            return
        
        # Add to buffer (NO TRIMMING)
        self.candles_buffer.append(candle)
        
        # Skip if not enough for ATR
        if len(self.candles_buffer) < 200:
            self.current_bar += 1
            return
        
        # Calculate ATR
        atr = self._calculate_atr()
        if atr is None:
            self.current_bar += 1
            return
        
        candle['atr'] = atr
        
        # Detect pivots
        self._detect_pivots()
        
        # Process market structure
        self._process_structure_bar(candle)
        
        # Check mitigation
        self._check_mitigation(candle)
        
        self.current_bar += 1
    
    def _calculate_atr(self) -> Optional[float]:
        """Calculate ATR (exact Pine Script formula)"""
        if len(self.candles_buffer) < 200:
            return None
        
        try:
            recent = self.candles_buffer[-200:]
            
            tr_list = []
            for i in range(1, len(recent)):
                h = float(recent[i]['high'])
                l = float(recent[i]['low'])
                pc = float(recent[i-1]['close'])
                
                tr1 = h - l
                tr2 = abs(h - pc)
                tr3 = abs(l - pc)
                tr_list.append(max(tr1, tr2, tr3))
            
            base_atr = sum(tr_list) / len(tr_list)
            atr = base_atr / (5 / self.config['len'])
            
            return atr
        except Exception as e:
            print(f"ATR calculation error: {e}")
            return None
    
    def _detect_pivots(self):
        """Detect pivot highs and lows"""
        mslen = self.config['mslen']
        
        if len(self.candles_buffer) < mslen * 2 + 1:
            return
        
        center_idx = len(self.candles_buffer) - mslen - 1
        
        # Pivot high
        if center_idx >= mslen:
            center_high = self.candles_buffer[center_idx]['high']
            is_ph = True
            
            for i in range(center_idx - mslen, center_idx):
                if self.candles_buffer[i]['high'] >= center_high:
                    is_ph = False
                    break
            
            if is_ph:
                for i in range(center_idx + 1, min(center_idx + mslen + 1, len(self.candles_buffer))):
                    if self.candles_buffer[i]['high'] > center_high:
                        is_ph = False
                        break
            
            if is_ph:
                self.pivot_highs.append((self.current_bar - mslen, center_high))
        
        # Pivot low
        if center_idx >= mslen:
            center_low = self.candles_buffer[center_idx]['low']
            is_pl = True
            
            for i in range(center_idx - mslen, center_idx):
                if self.candles_buffer[i]['low'] <= center_low:
                    is_pl = False
                    break
            
            if is_pl:
                for i in range(center_idx + 1, min(center_idx + mslen + 1, len(self.candles_buffer))):
                    if self.candles_buffer[i]['low'] < center_low:
                        is_pl = False
                        break
            
            if is_pl:
                self.pivot_lows.append((self.current_bar - mslen, center_low))
    
    def _process_structure_bar(self, candle: Dict):
        """Process market structure (exact Pine Script logic)"""
        high = float(candle['high'])
        low = float(candle['low'])
        close = float(candle['close'])
        open_price = float(candle['open'])
        
        ms = self.structure
        
        # Initialize up/dn
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
        
        ms.upsweep = False
        ms.dnsweep = False
        
        # STATE 0: Initialization
        if ms.start == 0:
            ms = Structure(
                start=1,
                trend=0,
                bos=high,
                choch=low,
                loc=self.current_bar,
                temp=self.current_bar,
                main=0,
                xloc=self.current_bar
            )
            self.structure = ms
            return
        
        # STATE 1: First break
        if ms.start == 1:
            # Sweeps
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
            
            # Bearish CHoCH
            if close <= ms.choch:
                ms.txt = "choch"
                self._create_order_block(ms, True)
                
                ms.trend = -1
                ms.choch = ms.bos
                ms.bos = None
                ms.start = 2
                ms.loc = self.current_bar
                ms.main = low
                ms.temp = ms.loc
                ms.xloc = self.current_bar
                
            # Bullish CHoCH
            elif close >= ms.bos:
                ms.txt = "choch"
                self._create_order_block(ms, False)
                
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
            if ms.trend == -1:  # Bearish
                if low <= ms.main:
                    ms.main = low
                    ms.temp = self.current_bar
                
                # BOS formation
                if ms.bos is None:
                    if crossup and close > open_price and self.current_bar > 0:
                        prev = self.candles_buffer[-2]
                        if prev['close'] > prev['open']:
                            ms.bos = ms.main
                            ms.loc = ms.temp
                            ms.xloc = ms.loc
                
                # BOS break
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
                        self._create_order_block(ms, False)
                        
                        id_idx, high_val, _ = self._find_structure_point(ms, True, False)
                        ms.xloc = self.current_bar
                        ms.bos = None
                        ms.choch = high_val
                        ms.loc = self.current_bar - id_idx
                
                # CHoCH (trend change)
                if self.config['buildsweep'] and high >= ms.choch and close <= ms.choch:
                    ms.upsweep = True
                    ms.choch = high
                    ms.xloc = self.current_bar
                    return
                
                if close >= ms.choch:
                    ms.txt = "choch"
                    ms.zz = ms.choch
                    ms.zn = self.current_bar
                    self._create_order_block(ms, True)
                    
                    id_idx, low_val, _ = self._find_structure_point(ms, False, False)
                    
                    if ms.bos is None:
                        ms.choch = low_val
                    else:
                        ms.choch = ms.bos
                    
                    ms.bos = None
                    ms.main = high
                    ms.trend = 1
                    ms.loc = self.current_bar
                    ms.xloc = self.current_bar
                    ms.temp = ms.loc
            
            elif ms.trend == 1:  # Bullish
                if high >= ms.main:
                    ms.main = high
                    ms.temp = self.current_bar
                
                # BOS formation
                if ms.bos is None:
                    if crossdn and close < open_price and self.current_bar > 0:
                        prev = self.candles_buffer[-2]
                        if prev['close'] < prev['open']:
                            ms.bos = ms.main
                            ms.loc = ms.temp
                            ms.xloc = ms.loc
                
                # BOS break
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
                        self._create_order_block(ms, True)
                        
                        id_idx, low_val, _ = self._find_structure_point(ms, False, False)
                        ms.xloc = self.current_bar
                        ms.bos = None
                        ms.choch = low_val
                        ms.loc = self.current_bar - id_idx
                
                # CHoCH (trend change)
                if self.config['buildsweep'] and low <= ms.choch and close >= ms.choch:
                    ms.dnsweep = True
                    ms.choch = low
                    ms.xloc = self.current_bar
                    return
                
                if close <= ms.choch:
                    ms.txt = "choch"
                    ms.zz = ms.choch
                    ms.zn = self.current_bar
                    self._create_order_block(ms, False)
                    
                    id_idx, high_val, _ = self._find_structure_point(ms, True, False)
                    
                    if ms.bos is None:
                        ms.choch = high_val
                    else:
                        ms.choch = ms.bos
                    
                    ms.bos = None
                    ms.main = low
                    ms.trend = -1
                    ms.loc = self.current_bar
                    ms.temp = ms.loc
                    ms.xloc = self.current_bar
    
    def _find_structure_point(self, ms: Structure, use_max: bool, sweep: bool = False) -> Tuple[int, float, float]:
        """Find structure point (exact Pine Script logic with OB adjustment)"""
        min_val = 99999999.0
        max_val = 0.0
        idx = 0
        
        loc_to_use = ms.xloc if sweep else ms.loc
        search_range = max(1, self.current_bar - loc_to_use)
        
        if use_max:
            # Find highest
            for i in range(search_range):
                if self.current_bar - i >= 0 and self.current_bar - i < len(self.candles_buffer):
                    high_val = self.candles_buffer[self.current_bar - i]['high']
                    low_val = self.candles_buffer[self.current_bar - i]['low']
                    if high_val > max_val:
                        max_val = high_val
                        min_val = low_val
                        idx = i
            
            # CRITICAL: OB mode adjustment (Pine Script lines 377-381)
            if self.config['obmode'] == 'Length' and self.current_bar - idx + 1 < len(self.candles_buffer):
                next_idx = self.current_bar - idx + 1
                if next_idx >= 0 and self.candles_buffer[next_idx]['high'] > self.candles_buffer[self.current_bar - idx]['high']:
                    max_val = self.candles_buffer[next_idx]['high']
                    min_val = self.candles_buffer[next_idx]['low']
                    idx = idx - 1
        
        else:
            # Find lowest
            for i in range(search_range):
                if self.current_bar - i >= 0 and self.current_bar - i < len(self.candles_buffer):
                    low_val = self.candles_buffer[self.current_bar - i]['low']
                    high_val = self.candles_buffer[self.current_bar - i]['high']
                    if low_val < min_val:
                        min_val = low_val
                        max_val = high_val
                        idx = i
            
            # CRITICAL: OB mode adjustment (Pine Script lines 389-393)
            if self.config['obmode'] == 'Length' and self.current_bar - idx + 1 < len(self.candles_buffer):
                next_idx = self.current_bar - idx + 1
                if next_idx >= 0 and self.candles_buffer[next_idx]['low'] < self.candles_buffer[self.current_bar - idx]['low']:
                    max_val = self.candles_buffer[next_idx]['high']
                    min_val = self.candles_buffer[next_idx]['low']
                    idx = idx - 1
        
        return idx, max_val, min_val
    
    def _create_order_block(self, ms: Structure, is_bullish: bool):
        """Create Order Block"""
        try:
            if is_bullish:
                idx, _, _ = self._find_structure_point(ms, False, False)
                actual_idx = self.current_bar - idx
                
                if actual_idx < 0 or actual_idx >= len(self.candles_buffer):
                    return
                
                candle = self.candles_buffer[actual_idx]
                high_val = float(candle['high'])
                low_val = float(candle['low'])
                atr_val = float(candle.get('atr', 0))
                
                if self.config['obmode'] == 'Length':
                    top = high_val if (low_val + atr_val) > high_val else (low_val + atr_val)
                    btm = low_val
                else:
                    top = high_val
                    btm = low_val
                
                ob = OrderBlock(
                    bull=True,
                    top=top,
                    btm=btm,
                    avg=(top + btm) / 2,
                    loc=actual_idx,
                    css="bullish",
                    vol=float(candle.get('volume', 1)),
                    dir=1 if candle['close'] > candle['open'] else -1,
                    xlocbl=actual_idx,
                    xlocbr=actual_idx
                )
                
                self.bullish_obs.insert(0, ob)
                
                if self.on_ob_created:
                    self.on_ob_created(self.symbol, ob, "bullish")
                
            else:
                idx, _, _ = self._find_structure_point(ms, True, False)
                actual_idx = self.current_bar - idx
                
                if actual_idx < 0 or actual_idx >= len(self.candles_buffer):
                    return
                
                candle = self.candles_buffer[actual_idx]
                high_val = float(candle['high'])
                low_val = float(candle['low'])
                atr_val = float(candle.get('atr', 0))
                
                if self.config['obmode'] == 'Length':
                    btm = low_val if (high_val - atr_val) < low_val else (high_val - atr_val)
                    top = high_val
                else:
                    top = high_val
                    btm = low_val
                
                ob = OrderBlock(
                    bull=False,
                    top=top,
                    btm=btm,
                    avg=(top + btm) / 2,
                    loc=actual_idx,
                    css="bearish",
                    vol=float(candle.get('volume', 1)),
                    dir=1 if candle['close'] > candle['open'] else -1,
                    xlocbl=actual_idx,
                    xlocbr=actual_idx
                )
                
                self.bearish_obs.insert(0, ob)
                
                if self.on_ob_created:
                    self.on_ob_created(self.symbol, ob, "bearish")
                    
        except Exception as e:
            print(f"Error creating OB: {e}")
    
    def _check_mitigation(self, candle: Dict):
        """Check OB mitigation/invalidation"""
        high = float(candle['high'])
        low = float(candle['low'])
        close = float(candle['close'])
        open_price = float(candle['open'])
        
        # Bullish OBs
        to_remove = []
        for i, ob in enumerate(self.bullish_obs):
            if ob.invalidated:
                continue
            
            if not ob.isbb:
                if (self.config['obmiti'] == 'Close' and min(close, open_price) < ob.btm) or \
                   (self.config['obmiti'] == 'Wick' and low < ob.btm) or \
                   (self.config['obmiti'] == 'Avg' and low < ob.avg):
                    ob.isbb = True
                    ob.bbloc = self.current_bar
                    
                    if self.on_ob_breaker:
                        self.on_ob_breaker(self.symbol, ob, "bullish")
            else:
                if (self.config['obmiti'] == 'Close' and max(close, open_price) > ob.top) or \
                   (self.config['obmiti'] == 'Wick' and high > ob.top) or \
                   (self.config['obmiti'] == 'Avg' and high > ob.avg):
                    ob.invalidated = True
                    ob.invalidation_bar = self.current_bar
                    to_remove.append(i)
                    
                    if self.on_ob_invalidated:
                        self.on_ob_invalidated(self.symbol, ob, "bullish")
        
        for i in reversed(to_remove):
            self.bullish_obs.pop(i)
        
        # Bearish OBs
        to_remove = []
        for i, ob in enumerate(self.bearish_obs):
            if ob.invalidated:
                continue
            
            if not ob.isbb:
                if (self.config['obmiti'] == 'Close' and max(close, open_price) > ob.top) or \
                   (self.config['obmiti'] == 'Wick' and high > ob.top) or \
                   (self.config['obmiti'] == 'Avg' and high > ob.avg):
                    ob.isbb = True
                    ob.bbloc = self.current_bar
                    
                    if self.on_ob_breaker:
                        self.on_ob_breaker(self.symbol, ob, "bearish")
            else:
                if (self.config['obmiti'] == 'Close' and min(close, open_price) < ob.btm) or \
                   (self.config['obmiti'] == 'Wick' and low < ob.btm) or \
                   (self.config['obmiti'] == 'Avg' and low < ob.avg):
                    ob.invalidated = True
                    ob.invalidation_bar = self.current_bar
                    to_remove.append(i)
                    
                    if self.on_ob_invalidated:
                        self.on_ob_invalidated(self.symbol, ob, "bearish")
        
        for i in reversed(to_remove):
            self.bearish_obs.pop(i)
    
    def get_active_obs(self) -> Dict[str, List[OrderBlock]]:
        """Get active OBs"""
        return {
            'bullish': [ob for ob in self.bullish_obs if not ob.invalidated],
            'bearish': [ob for ob in self.bearish_obs if not ob.invalidated]
        }
    
    def get_all_obs(self) -> Dict[str, List[OrderBlock]]:
        """Get all OBs"""
        return {
            'bullish': self.bullish_obs.copy(),
            'bearish': self.bearish_obs.copy()
        }
    
    def get_market_structure(self) -> Dict:
        """Get market structure"""
        return {
            'trend': self.structure.trend,
            'bos_level': self.structure.bos,
            'choch_level': self.structure.choch,
            'last_structure': self.structure.txt
        }