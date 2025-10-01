#!/usr/bin/env python3
"""
Test Candle Builder Standalone

Quick test to verify candle builder works correctly.
Simulates ticks and verifies candles are built properly.
"""

import sys
from pathlib import Path
from datetime import datetime
import time

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.data.candle_builder import CandleBuilder

print("=" * 80)
print("🧪 CANDLE BUILDER TEST")
print("=" * 80)
print()

# Create candle builder
print("1. Creating candle builder (15m timeframe)...")
builder = CandleBuilder(
    timeframe_minutes=15,
    on_candle_closed=lambda symbol, candle: print(
        f"   ✅ Candle closed: {symbol} @ "
        f"{datetime.fromtimestamp(candle['timestamp']).strftime('%H:%M')} - "
        f"OHLC: {candle['open']:.2f}/{candle['high']:.2f}/"
        f"{candle['low']:.2f}/{candle['close']:.2f}"
    )
)
print("   ✅ Candle builder created")
print()

# Get current time aligned to 15m boundary
base_time = int(time.time())
base_time = (base_time // 900) * 900  # Round to 15m

# Test scenario: Build 2 complete candles
print("2. Simulating ticks for first candle...")
print(f"   Start time: {datetime.fromtimestamp(base_time).strftime('%H:%M:%S')}")

# First candle - 10 ticks over 10 minutes
for i in range(10):
    tick = {
        'symbol': 'SOLUSD',
        'price': 150.0 + (i * 0.5),  # Price increasing
        'timestamp': base_time + (i * 60)  # Every minute
    }
    builder.process_tick(tick)
    if i == 0 or i == 9:
        print(f"   Tick {i+1}: ${tick['price']:.2f} @ {datetime.fromtimestamp(tick['timestamp']).strftime('%H:%M:%S')}")

print()

# Check current candle state
print("3. Checking current candle state...")
current = builder.get_current_candles()
if 'SOLUSD' in current:
    c = current['SOLUSD']
    print(f"   ✅ Active candle for SOLUSD:")
    print(f"      OHLC: {c['open']:.2f} / {c['high']:.2f} / {c['low']:.2f} / {c['close']:.2f}")
else:
    print("   ❌ No active candle found!")
print()

# Trigger candle close by starting new candle (15 minutes later)
print("4. Triggering candle close (15 minutes later)...")
new_tick = {
    'symbol': 'SOLUSD',
    'price': 155.0,
    'timestamp': base_time + 900  # +15 minutes
}
builder.process_tick(new_tick)
print()

# Build second candle with different price action
print("5. Building second candle (price decreasing)...")
for i in range(5):
    tick = {
        'symbol': 'SOLUSD',
        'price': 155.0 - (i * 0.3),  # Price decreasing
        'timestamp': base_time + 900 + (i * 60)
    }
    builder.process_tick(tick)

# Add AAVEUSD candle
print()
print("6. Testing multi-symbol (adding AAVEUSD)...")
for i in range(3):
    tick = {
        'symbol': 'AAVEUSD',
        'price': 200.0 + (i * 1.0),
        'timestamp': base_time + (i * 60)
    }
    builder.process_tick(tick)

print("   ✅ Processed AAVEUSD ticks")
print()

# Force close all candles
print("7. Force closing all open candles...")
builder.force_close_all()
print()

# Show statistics
print("=" * 80)
print("📊 FINAL STATISTICS")
print("=" * 80)

stats = builder.get_stats()
print(f"Ticks processed: {stats['ticks_processed']}")
print(f"Candles completed: {stats['candles_completed']}")
print(f"Candles by symbol:")
for symbol, count in stats['candles_by_symbol'].items():
    print(f"   {symbol}: {count} candles")
print()

# Show completed candles
completed = builder.get_completed_candles()
print("📈 COMPLETED CANDLES:")
for symbol, candles in completed.items():
    print(f"\n{symbol} ({len(candles)} candles):")
    for i, candle in enumerate(candles, 1):
        candle_time = datetime.fromtimestamp(candle['timestamp'])
        print(f"   Candle {i} @ {candle_time.strftime('%H:%M')}:")
        print(f"      O: ${candle['open']:.2f} | H: ${candle['high']:.2f} | "
              f"L: ${candle['low']:.2f} | C: ${candle['close']:.2f}")
print()

# Validation
print("=" * 80)
print("✅ VALIDATION")
print("=" * 80)

success = True

if stats['ticks_processed'] >= 18:  # 10 + 5 + 3
    print("✅ All ticks processed")
else:
    print(f"❌ Expected 18 ticks, got {stats['ticks_processed']}")
    success = False

if stats['candles_completed'] >= 2:
    print("✅ Multiple candles completed")
else:
    print(f"❌ Expected at least 2 candles, got {stats['candles_completed']}")
    success = False

if len(stats['candles_by_symbol']) == 2:
    print("✅ Multi-symbol tracking works")
else:
    print(f"❌ Expected 2 symbols, got {len(stats['candles_by_symbol'])}")
    success = False

# Validate OHLC logic
if 'SOLUSD' in completed and len(completed['SOLUSD']) > 0:
    first_candle = completed['SOLUSD'][0]
    if first_candle['high'] >= first_candle['open'] and first_candle['low'] <= first_candle['close']:
        print("✅ OHLC relationships valid")
    else:
        print("❌ Invalid OHLC relationships")
        success = False

print()

if success:
    print("🎉 ALL TESTS PASSED!")
    print()
    print("✅ Candle builder is working correctly!")
    print()
    print("Next: Run full integration test")
    print("   python scripts/test_phase2_complete.py")
else:
    print("❌ SOME TESTS FAILED")
    print("   Check output above for details")

print()
print("=" * 80)

sys.exit(0 if success else 1)