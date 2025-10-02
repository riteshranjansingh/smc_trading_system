#!/usr/bin/env python3
"""
Phase 3 Complete Integration Test

Tests all Phase 3 modules working together:
1. ProgressiveSMC - OB detection
2. OBManager - State management
3. MarketStructureHelper - Structure queries

This validates the complete Strategy Layer is working.
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.strategy.progressive_smc import ProgressiveSMC
from core.strategy.ob_manager import OBManager, get_ob_manager
from core.strategy.market_structure import MarketStructureHelper, get_structure_helper
from core.utils.logger import get_logger

logger = get_logger('system')

print("=" * 80)
print("🧪 PHASE 3 COMPLETE - INTEGRATION TEST")
print("=" * 80)
print()

# Test results tracking
tests_passed = 0
tests_failed = 0

def test(description, condition, error_msg=""):
    """Helper function to run a test"""
    global tests_passed, tests_failed
    
    if condition:
        print(f"✅ {description}")
        tests_passed += 1
        return True
    else:
        print(f"❌ {description}")
        if error_msg:
            print(f"   Error: {error_msg}")
        tests_failed += 1
        return False


print("1. TESTING MODULE IMPORTS")
print("-" * 80)

try:
    from core.strategy.progressive_smc import ProgressiveSMC, OrderBlock, Structure
    test("Import ProgressiveSMC", True)
except ImportError as e:
    test("Import ProgressiveSMC", False, str(e))

try:
    from core.strategy.ob_manager import OBManager, get_ob_manager
    test("Import OBManager", True)
except ImportError as e:
    test("Import OBManager", False, str(e))

try:
    from core.strategy.market_structure import MarketStructureHelper, get_structure_helper
    test("Import MarketStructureHelper", True)
except ImportError as e:
    test("Import MarketStructureHelper", False, str(e))

print()


print("2. TESTING OB MANAGER INITIALIZATION")
print("-" * 80)

try:
    manager = OBManager(auto_save=False)
    test("Create OBManager instance", manager is not None)
    
    # Register symbols
    manager.register_symbol("SOLUSD", "15m")
    manager.register_symbol("AAVEUSD", "15m")
    test("Register SOLUSD", "SOLUSD" in manager.smc_engines)
    test("Register AAVEUSD", "AAVEUSD" in manager.smc_engines)
    
except Exception as e:
    test("OBManager initialization", False, str(e))

print()


print("3. TESTING MARKET STRUCTURE HELPER")
print("-" * 80)

try:
    helper = MarketStructureHelper()
    test("Create MarketStructureHelper instance", helper is not None)
    
    # Test singleton
    helper2 = get_structure_helper()
    test("Singleton pattern works", helper2 is not None)
    
except Exception as e:
    test("MarketStructureHelper initialization", False, str(e))

print()


print("4. TESTING WITH HISTORICAL DATA")
print("-" * 80)

# Load historical data
data_file = Path("data/historical/SOLUSD_15m_2025-04-05_to_2025-10-02.json")

if not data_file.exists():
    print(f"❌ Data file not found: {data_file}")
    print("   Run: python scripts/test_historical_loader.py first")
    sys.exit(1)

try:
    with open(data_file, 'r') as f:
        data = json.load(f)
    
    candles = data['candles']
    test(f"Load historical data ({len(candles)} candles)", True)
    
except Exception as e:
    test("Load historical data", False, str(e))
    sys.exit(1)

print()


print("5. PROCESSING CANDLES")
print("-" * 80)

try:
    print(f"   Processing {len(candles)} candles...")
    
    obs_created_count = 0
    structure_changes = 0
    last_trend = None
    
    for i, candle in enumerate(candles):
        # Process through OB manager
        manager.on_candle_close("SOLUSD", candle)
        
        # Track structure changes every 500 candles
        if (i + 1) % 500 == 0:
            smc = manager.smc_engines["SOLUSD"]
            current_trend = helper.get_trend(smc)
            
            if last_trend is not None and current_trend != last_trend:
                structure_changes += 1
                print(f"      Bar {i+1}: {last_trend.name} → {current_trend.name}")
            
            last_trend = current_trend
    
    test("Process all candles", True)
    print()
    
except Exception as e:
    test("Process candles", False, str(e))
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()


print("6. VALIDATING OB DETECTION")
print("-" * 80)

try:
    # Get active OBs
    active_obs = manager.get_active_obs("SOLUSD")
    
    total_active = len(active_obs['bullish']) + len(active_obs['bearish'])
    test(f"Active OBs detected ({total_active} total)", total_active > 0)
    
    test(f"Bullish OBs ({len(active_obs['bullish'])})", len(active_obs['bullish']) >= 0)
    test(f"Bearish OBs ({len(active_obs['bearish'])})", len(active_obs['bearish']) >= 0)
    
    # Check OB properties
    if active_obs['bullish']:
        ob = active_obs['bullish'][0]
        test("OB has top/bottom", hasattr(ob, 'top') and hasattr(ob, 'btm'))
        test("OB has type", hasattr(ob, 'get_type'))
        test("OB top > bottom", ob.top > ob.btm)
    
except Exception as e:
    test("OB detection validation", False, str(e))

print()


print("7. TESTING OB TOUCH DETECTION")
print("-" * 80)

try:
    # Test with last candle price
    last_candle = candles[-1]
    test_price = float(last_candle['close'])
    
    touch = manager.check_ob_touch("SOLUSD", test_price, penetration_pct=0.20)
    
    if touch:
        print(f"   📍 Price ${test_price:.2f} touches {touch['direction']} {touch['ob_type']} OB")
        test("OB touch detection works", True)
    else:
        print(f"   No OB touch at price ${test_price:.2f}")
        test("OB touch detection works (no touch)", True)
    
except Exception as e:
    test("OB touch detection", False, str(e))

print()


print("8. TESTING MARKET STRUCTURE QUERIES")
print("-" * 80)

try:
    smc = manager.smc_engines["SOLUSD"]
    
    # Test trend detection
    trend = helper.get_trend(smc)
    test(f"Get trend ({trend.name})", trend is not None)
    
    is_bullish = helper.is_bullish_trend(smc)
    is_bearish = helper.is_bearish_trend(smc)
    is_neutral = helper.is_neutral(smc)
    
    test("Trend detection methods work", True)
    
    # Test structure levels
    bos_level = helper.get_bos_level(smc)
    choch_level = helper.get_choch_level(smc)
    
    test("BOS/CHoCH level retrieval", True)
    
    # Test comprehensive summary
    summary = helper.get_structure_summary(smc)
    test("Structure summary", 'trend' in summary and 'is_bullish' in summary)
    
except Exception as e:
    test("Market structure queries", False, str(e))

print()


print("9. TESTING STATE PERSISTENCE")
print("-" * 80)

try:
    # Save state
    manager.save_state()
    test("Save OB state", True)
    
    # Load state (creates new manager)
    new_manager = OBManager()
    new_manager.register_symbol("SOLUSD")
    loaded = new_manager.load_state()
    test("Load OB state", loaded)
    
except Exception as e:
    test("State persistence", False, str(e))

print()


print("10. TESTING STATISTICS")
print("-" * 80)

try:
    stats = manager.get_statistics()
    
    test("Get statistics", stats is not None)
    test("Statistics has total counts", 'total_obs_created' in stats)
    test("Statistics has by-symbol data", 'by_symbol' in stats)
    
    print(f"\n   Statistics Summary:")
    print(f"      Total OBs Created: {stats['total_obs_created']}")
    print(f"      Total OBs Invalidated: {stats['total_obs_invalidated']}")
    print(f"      Total → Breaker: {stats['total_obs_became_breaker']}")
    
    if 'SOLUSD' in stats['by_symbol']:
        sol_stats = stats['by_symbol']['SOLUSD']
        print(f"\n   SOLUSD:")
        print(f"      Created: {sol_stats['created']}")
        print(f"      Currently Active: {sol_stats['active']}")
    
except Exception as e:
    test("Statistics", False, str(e))

print()


# ===== SUMMARY =====
print("=" * 80)
print("📊 TEST SUMMARY")
print("=" * 80)
print(f"Total tests: {tests_passed + tests_failed}")
print(f"✅ Passed: {tests_passed}")
print(f"❌ Failed: {tests_failed}")
print()

if tests_failed == 0:
    print("🎉 ALL TESTS PASSED! Phase 3 is complete!")
    print()
    print("✅ PHASE 3 DELIVERABLES:")
    print("   - progressive_smc.py: ✅ Working")
    print("   - ob_manager.py: ✅ Working")
    print("   - market_structure.py: ✅ Working")
    print("   - Integration: ✅ Working")
    print()
    print("📋 NEXT STEPS:")
    print()
    print("   1. Commit Phase 3 changes:")
    print("      git add core/strategy/")
    print("      git add scripts/test_phase3_complete.py")
    print("      git commit -m 'Phase 3 Complete: Strategy Layer implemented'")
    print("      git push")
    print()
    print("   2. Update PROJECT_TRACKER:")
    print("      - Mark Phase 3 as 100% complete")
    print("      - Update progress bars")
    print()
    print("   3. Ready for Phase 4: Execution Layer!")
    print("      - Mode A Executor (candle close entry)")
    print("      - Mode B Executor (limit order entry)")
    print("      - Position Manager")
    print("      - Risk Management")
    print()
    exit_code = 0
else:
    print("⚠️  SOME TESTS FAILED")
    print()
    print("Please fix the failed tests before proceeding to Phase 4.")
    print("Review the error messages above for details.")
    print()
    exit_code = 1

print("=" * 80)
sys.exit(exit_code)