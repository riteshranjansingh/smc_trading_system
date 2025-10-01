#!/usr/bin/env python3
"""
Order Block Manager - Manages OB state and persistence

Features:
- Manages OBs for multiple symbols
- Integrates with ProgressiveSMC
- Saves/loads OB state to/from JSON
- Provides clean API for execution layer
- Tracks OB age and validity
- Handles OB lifecycle events

Usage:
    manager = OBManager()
    
    # Register SMC engines
    manager.register_symbol("SOLUSD", smc_engine)
    manager.register_symbol("AAVEUSD", smc_engine)
    
    # On candle close
    manager.on_candle_close("SOLUSD", candle)
    
    # Get active OBs
    obs = manager.get_active_obs("SOLUSD")
"""

from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import asdict
import json

from core.strategy.progressive_smc import ProgressiveSMC, OrderBlock
from core.utils.state_persistence import get_state_manager
from core.utils.logger import get_logger

logger = get_logger('ob_events')


class OBManager:
    """
    Manages Order Blocks across multiple symbols
    
    Integrates with ProgressiveSMC and state persistence.
    """
    
    def __init__(self, auto_save: bool = True):
        """
        Initialize OB Manager
        
        Args:
            auto_save: Automatically save state on every update
        """
        self.auto_save = auto_save
        self.state_manager = get_state_manager()
        
        # SMC engines per symbol
        self.smc_engines: Dict[str, ProgressiveSMC] = {}
        
        # Statistics
        self.stats = {
            'obs_created': 0,
            'obs_invalidated': 0,
            'obs_became_breaker': 0,
            'obs_by_symbol': {}
        }
        
        logger.info("📦 OB Manager initialized")
    
    def register_symbol(self, symbol: str, timeframe: str = "15m"):
        """
        Register a symbol for OB tracking
        
        Args:
            symbol: Trading symbol (e.g., "SOLUSD")
            timeframe: Candle timeframe (default: "15m")
        """
        if symbol in self.smc_engines:
            logger.warning(f"Symbol {symbol} already registered")
            return
        
        # Create SMC engine with callbacks
        smc = ProgressiveSMC(
            symbol=symbol,
            timeframe=timeframe,
            on_ob_created=self._on_ob_created,
            on_ob_invalidated=self._on_ob_invalidated,
            on_ob_breaker=self._on_ob_breaker
        )
        
        self.smc_engines[symbol] = smc
        self.stats['obs_by_symbol'][symbol] = {
            'created': 0,
            'invalidated': 0,
            'became_breaker': 0,
            'active': 0
        }
        
        logger.info(f"✅ Registered symbol: {symbol}")
    
    def on_candle_close(self, symbol: str, candle: Dict):
        """
        Process candle close for a symbol
        
        Args:
            symbol: Trading symbol
            candle: Candle data dictionary
        """
        if symbol not in self.smc_engines:
            logger.error(f"Symbol {symbol} not registered")
            return
        
        # Process through SMC engine
        self.smc_engines[symbol].process_candle(candle)
        
        # Auto-save state if enabled
        if self.auto_save:
            self.save_state()
    
    def get_active_obs(self, symbol: str) -> Dict[str, List[OrderBlock]]:
        """
        Get active OBs for a symbol
        
        Args:
            symbol: Trading symbol
        
        Returns:
            Dictionary with 'bullish' and 'bearish' OB lists
        """
        if symbol not in self.smc_engines:
            logger.error(f"Symbol {symbol} not registered")
            return {'bullish': [], 'bearish': []}
        
        return self.smc_engines[symbol].get_active_obs()
    
    def get_all_obs(self, symbol: str) -> Dict[str, List[OrderBlock]]:
        """
        Get all OBs (including invalidated) for a symbol
        
        Args:
            symbol: Trading symbol
        
        Returns:
            Dictionary with 'bullish' and 'bearish' OB lists
        """
        if symbol not in self.smc_engines:
            return {'bullish': [], 'bearish': []}
        
        return self.smc_engines[symbol].get_all_obs()
    
    def get_market_structure(self, symbol: str) -> Dict:
        """
        Get market structure for a symbol
        
        Args:
            symbol: Trading symbol
        
        Returns:
            Dictionary with trend, BOS/CHoCH levels
        """
        if symbol not in self.smc_engines:
            return {}
        
        return self.smc_engines[symbol].get_market_structure()
    
    def check_ob_touch(self, symbol: str, price: float, 
                      penetration_pct: float = 0.20) -> Optional[Dict]:
        """
        Check if price touches any active OB with penetration
        
        Args:
            symbol: Trading symbol
            price: Current price
            penetration_pct: Penetration percentage (default: 20%)
        
        Returns:
            Dictionary with OB info if touched, None otherwise
        """
        active_obs = self.get_active_obs(symbol)
        
        # Check bullish OBs (for long entries)
        for ob in active_obs['bullish']:
            ob_range = ob.top - ob.btm
            penetration = ob_range * penetration_pct
            entry_level = ob.top - penetration
            
            # Price must be below entry level (penetrated into zone)
            if price <= entry_level:
                return {
                    'direction': 'bullish',
                    'ob': ob,
                    'ob_type': ob.get_type(),
                    'entry_level': entry_level,
                    'ob_top': ob.top,
                    'ob_bottom': ob.btm,
                    'penetration_pct': penetration_pct,
                    'price_in_zone': price
                }
        
        # Check bearish OBs (for short entries)
        for ob in active_obs['bearish']:
            ob_range = ob.top - ob.btm
            penetration = ob_range * penetration_pct
            entry_level = ob.btm + penetration
            
            # Price must be above entry level (penetrated into zone)
            if price >= entry_level:
                return {
                    'direction': 'bearish',
                    'ob': ob,
                    'ob_type': ob.get_type(),
                    'entry_level': entry_level,
                    'ob_top': ob.top,
                    'ob_bottom': ob.btm,
                    'penetration_pct': penetration_pct,
                    'price_in_zone': price
                }
        
        return None
    
    def is_ob_invalidated(self, symbol: str, ob: OrderBlock) -> bool:
        """
        Check if an OB is invalidated
        
        Args:
            symbol: Trading symbol
            ob: OrderBlock to check
        
        Returns:
            True if invalidated
        """
        return ob.invalidated
    
    def get_ob_age(self, ob: OrderBlock, current_bar: int) -> int:
        """
        Get age of OB in bars
        
        Args:
            ob: OrderBlock
            current_bar: Current bar index
        
        Returns:
            Age in bars
        """
        return current_bar - ob.loc
    
    def save_state(self):
        """Save OB state to JSON"""
        state = {}
        
        for symbol, smc in self.smc_engines.items():
            active_obs = smc.get_active_obs()
            
            # Convert OBs to serializable format
            state[symbol] = {
                'bullish': [self._ob_to_dict(ob) for ob in active_obs['bullish']],
                'bearish': [self._ob_to_dict(ob) for ob in active_obs['bearish']],
                'market_structure': smc.get_market_structure(),
                'last_updated': datetime.now().isoformat()
            }
            
            # Update stats
            self.stats['obs_by_symbol'][symbol]['active'] = (
                len(active_obs['bullish']) + len(active_obs['bearish'])
            )
        
        # Save using state manager
        success = self.state_manager.save_ob_state(state)
        
        if success:
            logger.debug("💾 OB state saved")
        else:
            logger.error("❌ Failed to save OB state")
    
    def load_state(self) -> bool:
        """
        Load OB state from JSON
        
        Note: This loads for reference only. On startup, we should
        rebuild OBs from 6-month history for accuracy.
        
        Returns:
            True if loaded successfully
        """
        state = self.state_manager.load_ob_state()
        
        if state is None:
            logger.warning("No saved OB state found")
            return False
        
        logger.info(f"📂 Loaded OB state for {len(state)} symbols")
        
        for symbol, data in state.items():
            if symbol in self.smc_engines:
                logger.info(f"  {symbol}: {len(data['bullish'])} bullish, "
                          f"{len(data['bearish'])} bearish OBs")
        
        return True
    
    def get_statistics(self) -> Dict:
        """Get OB statistics"""
        return {
            'total_obs_created': self.stats['obs_created'],
            'total_obs_invalidated': self.stats['obs_invalidated'],
            'total_obs_became_breaker': self.stats['obs_became_breaker'],
            'by_symbol': self.stats['obs_by_symbol'].copy()
        }
    
    def _ob_to_dict(self, ob: OrderBlock) -> Dict:
        """Convert OrderBlock to dictionary"""
        return {
            'bull': ob.bull,
            'top': ob.top,
            'btm': ob.btm,
            'avg': ob.avg,
            'loc': ob.loc,
            'vol': ob.vol,
            'dir': ob.dir,
            'isbb': ob.isbb,
            'bbloc': ob.bbloc,
            'invalidated': ob.invalidated,
            'invalidation_bar': ob.invalidation_bar,
            'ob_type': ob.get_type()
        }
    
    def _on_ob_created(self, symbol: str, ob: OrderBlock, direction: str):
        """Callback when OB is created"""
        self.stats['obs_created'] += 1
        self.stats['obs_by_symbol'][symbol]['created'] += 1
        
        # Log event
        logger.info(f"🎉 OB CREATED: {symbol} {direction.upper()}")
        logger.info(f"   Top: ${ob.top:.4f} | Bottom: ${ob.btm:.4f}")
        logger.info(f"   Type: {ob.get_type()}")
    
    def _on_ob_invalidated(self, symbol: str, ob: OrderBlock, direction: str):
        """Callback when OB is invalidated"""
        self.stats['obs_invalidated'] += 1
        self.stats['obs_by_symbol'][symbol]['invalidated'] += 1
        
        # Log event
        logger.info(f"❌ OB INVALIDATED: {symbol} {direction.upper()}")
        logger.info(f"   Top: ${ob.top:.4f} | Bottom: ${ob.btm:.4f}")
        logger.info(f"   Type: {ob.get_type()}")
    
    def _on_ob_breaker(self, symbol: str, ob: OrderBlock, direction: str):
        """Callback when OB becomes breaker"""
        self.stats['obs_became_breaker'] += 1
        self.stats['obs_by_symbol'][symbol]['became_breaker'] += 1
        
        # Log event
        logger.info(f"🔄 OB → BREAKER: {symbol} {direction.upper()}")
        logger.info(f"   Top: ${ob.top:.4f} | Bottom: ${ob.btm:.4f}")


# Singleton instance
_ob_manager = None

def get_ob_manager() -> OBManager:
    """
    Get singleton OB manager instance
    
    Returns:
        OBManager instance
    
    Example:
        from core.strategy.ob_manager import get_ob_manager
        
        manager = get_ob_manager()
        manager.register_symbol("SOLUSD")
        manager.on_candle_close("SOLUSD", candle)
    """
    global _ob_manager
    if _ob_manager is None:
        _ob_manager = OBManager()
    return _ob_manager


# Example usage and testing
if __name__ == "__main__":
    import json
    from pathlib import Path
    
    print("=" * 80)
    print("🧪 TESTING OB MANAGER")
    print("=" * 80)
    print()
    
    # Create OB manager
    manager = OBManager(auto_save=False)  # Disable auto-save for test
    
    # Register symbols
    print("1. Registering symbols...")
    manager.register_symbol("SOLUSD", "15m")
    manager.register_symbol("AAVEUSD", "15m")
    print()
    
    # Load historical data
    data_file = Path("data/historical/SOLUSD_15m_2025-04-04_to_2025-10-01.json")
    
    if data_file.exists():
        print("2. Loading SOLUSD historical data...")
        with open(data_file, 'r') as f:
            data = json.load(f)
        
        candles = data['candles']
        print(f"   ✅ Loaded {len(candles)} candles")
        print()
        
        # Process candles
        print("3. Processing candles...")
        for i, candle in enumerate(candles):
            manager.on_candle_close("SOLUSD", candle)
            
            # Test OB touch detection every 100 candles
            if (i + 1) % 100 == 0:
                price = float(candle['close'])
                touch = manager.check_ob_touch("SOLUSD", price, penetration_pct=0.20)
                
                if touch:
                    print(f"   📍 Bar {i+1}: Price ${price:.2f} touches "
                          f"{touch['direction']} {touch['ob_type']} OB")
        
        print()
        
        # Get final statistics
        print("=" * 80)
        print("📊 FINAL STATISTICS")
        print("=" * 80)
        
        stats = manager.get_statistics()
        print(f"\nOverall:")
        print(f"  OBs Created: {stats['total_obs_created']}")
        print(f"  OBs Invalidated: {stats['total_obs_invalidated']}")
        print(f"  OBs → Breaker: {stats['total_obs_became_breaker']}")
        
        print(f"\nSOLUSD:")
        sol_stats = stats['by_symbol']['SOLUSD']
        print(f"  Created: {sol_stats['created']}")
        print(f"  Invalidated: {sol_stats['invalidated']}")
        print(f"  Became Breaker: {sol_stats['became_breaker']}")
        print(f"  Currently Active: {sol_stats['active']}")
        
        # Get active OBs
        active_obs = manager.get_active_obs("SOLUSD")
        print(f"\nActive OBs:")
        print(f"  Bullish: {len(active_obs['bullish'])}")
        print(f"  Bearish: {len(active_obs['bearish'])}")
        
        # Show market structure
        structure = manager.get_market_structure("SOLUSD")
        print(f"\nMarket Structure:")
        print(f"  Trend: {structure['trend']} (1=bull, -1=bear)")
        print(f"  Last event: {structure['last_structure']}")
        
        # Test save/load
        print()
        print("4. Testing state persistence...")
        manager.save_state()
        print("   ✅ State saved")
        
        # Create new manager and load
        new_manager = OBManager()
        new_manager.register_symbol("SOLUSD")
        new_manager.load_state()
        print("   ✅ State loaded")
        
        print()
        print("✅ OB Manager test complete!")
        
    else:
        print(f"❌ Data file not found: {data_file}")
        print("   Run: python scripts/test_historical_loader.py first")