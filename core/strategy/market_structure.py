#!/usr/bin/env python3
"""
Market Structure Helper - Utilities for BOS/CHoCH analysis

Provides helper functions and utilities for market structure analysis:
- Trend identification
- BOS/CHoCH detection
- Structure level tracking
- Sweep detection

This module provides a clean interface on top of the ProgressiveSMC engine.

Usage:
    from core.strategy.market_structure import MarketStructureHelper
    
    helper = MarketStructureHelper()
    
    # Get trend
    trend = helper.get_trend(smc_engine)
    
    # Check if bullish market
    is_bullish = helper.is_bullish_trend(smc_engine)
"""

from typing import Dict, Optional, Tuple
from enum import Enum

from core.strategy.progressive_smc import ProgressiveSMC, Structure
from core.utils.logger import get_logger

logger = get_logger('ob_events')


class TrendDirection(Enum):
    """Trend direction enumeration"""
    BULLISH = 1
    BEARISH = -1
    NEUTRAL = 0


class StructureType(Enum):
    """Market structure event types"""
    BOS = "bos"           # Break of Structure
    CHOCH = "choch"       # Change of Character
    SWEEP = "sweep"       # Liquidity sweep
    NONE = "none"


class MarketStructureHelper:
    """
    Helper class for market structure analysis
    
    Provides clean interface for querying market structure state.
    """
    
    def __init__(self):
        """Initialize market structure helper"""
        logger.debug("Market Structure Helper initialized")
    
    def get_trend(self, smc: ProgressiveSMC) -> TrendDirection:
        """
        Get current trend direction
        
        Args:
            smc: ProgressiveSMC engine instance
        
        Returns:
            TrendDirection enum
        """
        structure = smc.get_market_structure()
        trend_value = structure.get('trend', 0)
        
        if trend_value == 1:
            return TrendDirection.BULLISH
        elif trend_value == -1:
            return TrendDirection.BEARISH
        else:
            return TrendDirection.NEUTRAL
    
    def is_bullish_trend(self, smc: ProgressiveSMC) -> bool:
        """Check if market is in bullish trend"""
        return self.get_trend(smc) == TrendDirection.BULLISH
    
    def is_bearish_trend(self, smc: ProgressiveSMC) -> bool:
        """Check if market is in bearish trend"""
        return self.get_trend(smc) == TrendDirection.BEARISH
    
    def is_neutral(self, smc: ProgressiveSMC) -> bool:
        """Check if market is neutral (no clear trend)"""
        return self.get_trend(smc) == TrendDirection.NEUTRAL
    
    def get_bos_level(self, smc: ProgressiveSMC) -> Optional[float]:
        """
        Get current BOS (Break of Structure) level
        
        Args:
            smc: ProgressiveSMC engine instance
        
        Returns:
            BOS price level or None if not set
        """
        structure = smc.get_market_structure()
        return structure.get('bos_level')
    
    def get_choch_level(self, smc: ProgressiveSMC) -> Optional[float]:
        """
        Get current CHoCH (Change of Character) level
        
        Args:
            smc: ProgressiveSMC engine instance
        
        Returns:
            CHoCH price level or None if not set
        """
        structure = smc.get_market_structure()
        return structure.get('choch_level')
    
    def get_last_structure_event(self, smc: ProgressiveSMC) -> StructureType:
        """
        Get last structure event type
        
        Args:
            smc: ProgressiveSMC engine instance
        
        Returns:
            StructureType enum
        """
        structure = smc.get_market_structure()
        last_event = structure.get('last_structure', '')
        
        if last_event == 'bos':
            return StructureType.BOS
        elif last_event == 'choch':
            return StructureType.CHOCH
        elif last_event == 'sweep':
            return StructureType.SWEEP
        else:
            return StructureType.NONE
    
    def get_structure_summary(self, smc: ProgressiveSMC) -> Dict:
        """
        Get comprehensive market structure summary
        
        Args:
            smc: ProgressiveSMC engine instance
        
        Returns:
            Dictionary with complete structure info
        """
        structure = smc.get_market_structure()
        trend = self.get_trend(smc)
        
        return {
            'trend': trend.name,
            'trend_value': trend.value,
            'bos_level': structure.get('bos_level'),
            'choch_level': structure.get('choch_level'),
            'last_event': self.get_last_structure_event(smc).value,
            'is_bullish': self.is_bullish_trend(smc),
            'is_bearish': self.is_bearish_trend(smc),
            'is_neutral': self.is_neutral(smc)
        }
    
    def is_counter_trend_setup(self, smc: ProgressiveSMC, ob_direction: str) -> bool:
        """
        Check if OB setup is counter-trend
        
        Args:
            smc: ProgressiveSMC engine instance
            ob_direction: 'bullish' or 'bearish'
        
        Returns:
            True if counter-trend setup
        """
        trend = self.get_trend(smc)
        
        # Bullish OB in bearish trend = counter-trend
        if ob_direction == 'bullish' and trend == TrendDirection.BEARISH:
            return True
        
        # Bearish OB in bullish trend = counter-trend
        if ob_direction == 'bearish' and trend == TrendDirection.BULLISH:
            return True
        
        return False
    
    def get_structure_levels(self, smc: ProgressiveSMC) -> Tuple[Optional[float], Optional[float]]:
        """
        Get key structure levels (BOS and CHoCH)
        
        Args:
            smc: ProgressiveSMC engine instance
        
        Returns:
            Tuple of (bos_level, choch_level)
        """
        bos = self.get_bos_level(smc)
        choch = self.get_choch_level(smc)
        return (bos, choch)
    
    def is_structure_bullish(self, structure: Dict) -> bool:
        """
        Check if structure state indicates bullish conditions
        
        Args:
            structure: Structure dictionary
        
        Returns:
            True if bullish
        """
        return structure.get('trend', 0) == 1
    
    def is_structure_bearish(self, structure: Dict) -> bool:
        """
        Check if structure state indicates bearish conditions
        
        Args:
            structure: Structure dictionary
        
        Returns:
            True if bearish
        """
        return structure.get('trend', 0) == -1
    
    def format_structure_for_log(self, smc: ProgressiveSMC) -> str:
        """
        Format structure information for logging
        
        Args:
            smc: ProgressiveSMC engine instance
        
        Returns:
            Formatted string for logging
        """
        summary = self.get_structure_summary(smc)
        
        trend_emoji = "📈" if summary['is_bullish'] else "📉" if summary['is_bearish'] else "➡️"
        
        lines = [
            f"{trend_emoji} Market Structure:",
            f"  Trend: {summary['trend']}",
            f"  Last Event: {summary['last_event'].upper()}"
        ]
        
        if summary['bos_level']:
            lines.append(f"  BOS Level: ${summary['bos_level']:.4f}")
        
        if summary['choch_level']:
            lines.append(f"  CHoCH Level: ${summary['choch_level']:.4f}")
        
        return "\n".join(lines)


# Singleton instance
_structure_helper = None

def get_structure_helper() -> MarketStructureHelper:
    """
    Get singleton market structure helper instance
    
    Returns:
        MarketStructureHelper instance
    
    Example:
        from core.strategy.market_structure import get_structure_helper
        
        helper = get_structure_helper()
        trend = helper.get_trend(smc_engine)
    """
    global _structure_helper
    if _structure_helper is None:
        _structure_helper = MarketStructureHelper()
    return _structure_helper


# Convenience functions
def get_trend(smc: ProgressiveSMC) -> TrendDirection:
    """Convenience function to get trend"""
    helper = get_structure_helper()
    return helper.get_trend(smc)


def is_bullish_trend(smc: ProgressiveSMC) -> bool:
    """Convenience function to check if bullish"""
    helper = get_structure_helper()
    return helper.is_bullish_trend(smc)


def is_bearish_trend(smc: ProgressiveSMC) -> bool:
    """Convenience function to check if bearish"""
    helper = get_structure_helper()
    return helper.is_bearish_trend(smc)


def is_counter_trend_setup(smc: ProgressiveSMC, ob_direction: str) -> bool:
    """Convenience function to check if counter-trend"""
    helper = get_structure_helper()
    return helper.is_counter_trend_setup(smc, ob_direction)


def get_structure_summary(smc: ProgressiveSMC) -> Dict:
    """Convenience function to get structure summary"""
    helper = get_structure_helper()
    return helper.get_structure_summary(smc)


# Example usage and testing
if __name__ == "__main__":
    import json
    from pathlib import Path
    from core.strategy.progressive_smc import ProgressiveSMC
    
    print("=" * 80)
    print("🧪 TESTING MARKET STRUCTURE HELPER")
    print("=" * 80)
    print()
    
    # Create SMC engine
    smc = ProgressiveSMC(symbol="SOLUSD", timeframe="15m")
    
    # Load historical data
    data_file = Path("data/historical/SOLUSD_15m_2025-04-04_to_2025-10-01.json")
    
    if data_file.exists():
        print("📂 Loading historical data...")
        with open(data_file, 'r') as f:
            data = json.load(f)
        
        candles = data['candles']
        print(f"✅ Loaded {len(candles)} candles")
        print()
        
        # Create helper
        helper = MarketStructureHelper()
        
        print("🔄 Processing candles and tracking structure...")
        print()
        
        # Track structure changes
        last_trend = None
        structure_changes = 0
        
        for i, candle in enumerate(candles):
            smc.process_candle(candle)
            
            # Check for trend changes every 100 candles
            if (i + 1) % 100 == 0:
                current_trend = helper.get_trend(smc)
                
                if last_trend is not None and current_trend != last_trend:
                    structure_changes += 1
                    print(f"  📍 Bar {i+1}: Trend changed from {last_trend.name} → {current_trend.name}")
                
                last_trend = current_trend
        
        print()
        print("=" * 80)
        print("📊 FINAL ANALYSIS")
        print("=" * 80)
        print()
        
        # Get comprehensive summary
        summary = helper.get_structure_summary(smc)
        
        print("Current Market Structure:")
        print(f"  Trend: {summary['trend']}")
        print(f"  Bullish: {summary['is_bullish']}")
        print(f"  Bearish: {summary['is_bearish']}")
        print(f"  Neutral: {summary['is_neutral']}")
        print()
        
        if summary['bos_level']:
            print(f"  BOS Level: ${summary['bos_level']:.4f}")
        
        if summary['choch_level']:
            print(f"  CHoCH Level: ${summary['choch_level']:.4f}")
        
        print(f"\n  Last Event: {summary['last_event'].upper()}")
        print()
        
        print(f"Structure Changes Detected: {structure_changes}")
        print()
        
        # Test convenience functions
        print("Testing Convenience Functions:")
        print(f"  is_bullish_trend(): {is_bullish_trend(smc)}")
        print(f"  is_bearish_trend(): {is_bearish_trend(smc)}")
        print()
        
        # Test counter-trend detection
        print("Counter-Trend Setup Tests:")
        print(f"  Bullish OB in current trend: {is_counter_trend_setup(smc, 'bullish')}")
        print(f"  Bearish OB in current trend: {is_counter_trend_setup(smc, 'bearish')}")
        print()
        
        # Format for logging
        print("Formatted for Logging:")
        print(helper.format_structure_for_log(smc))
        print()
        
        print("✅ Market Structure Helper test complete!")
        
    else:
        print(f"❌ Data file not found: {data_file}")
        print("   Run: python scripts/test_historical_loader.py first")