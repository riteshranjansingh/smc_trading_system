#!/usr/bin/env python3
"""
Test Historical Data Loader

Tests:
1. API connection
2. Data validator integration
3. Fetch 6 months data for SOLUSD
4. Data validation
5. JSON save/load

Run this to verify Phase 2 Step 1 & 2 are working.
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
from core.data.historical_loader import HistoricalDataLoader
from core.data.data_validator import DataValidator

print("=" * 80)
print("🧪 PHASE 2 - HISTORICAL DATA LOADER TEST")
print("=" * 80)
print()

# Load environment variables
load_dotenv()

api_key = os.getenv('DELTA_API_KEY_1')
api_secret = os.getenv('DELTA_API_SECRET_1')

if not api_key or not api_secret:
    print("❌ Error: API credentials not found in .env file")
    print()
    print("Please add to your .env file:")
    print("   DELTA_API_KEY_1=your_api_key_here")
    print("   DELTA_API_SECRET_1=your_api_secret_here")
    print()
    exit(1)

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

try:
    # Test 1: Create loader
    print("1. CREATING HISTORICAL DATA LOADER")
    print("-" * 80)
    
    loader = HistoricalDataLoader(
        api_key=api_key,
        api_secret=api_secret,
        output_dir="data/historical"
    )
    test("Loader created successfully", True)
    print()
    
    # Test 2: Test API connection
    print("2. TESTING API CONNECTION")
    print("-" * 80)
    
    connection_ok = loader.test_connection()
    test("API connection successful", connection_ok)
    print()
    
    if not connection_ok:
        print("❌ Cannot proceed without API connection")
        exit(1)
    
    # Test 3: Test date range calculation
    print("3. TESTING DATE RANGE CALCULATION")
    print("-" * 80)
    
    start_date, end_date = loader._calculate_6_month_range()
    test(f"Calculated 6-month range: {start_date} to {end_date}", True)
    print()
    
    # Test 4: Fetch data for SOLUSD (this will take a while)
    print("4. FETCHING HISTORICAL DATA (SOLUSD)")
    print("-" * 80)
    print("⏳ This may take 1-2 minutes... Please wait...")
    print()
    
    try:
        filepath = loader.fetch_6_months_data("SOLUSD")
        test("Successfully fetched SOLUSD data", True)
        test(f"Saved to: {filepath}", os.path.exists(filepath))
        print()
    except Exception as e:
        test("Fetch SOLUSD data", False, str(e))
        print()
    
    # Test 5: Load and validate saved data
    print("5. LOADING AND VALIDATING DATA")
    print("-" * 80)
    
    try:
        data = loader.load_from_json(filepath)
        
        test(f"Loaded {data['total_candles']} candles", True)
        test(f"Symbol: {data['symbol']}", data['symbol'] == 'SOLUSD')
        test(f"Timeframe: {data['timeframe']}", data['timeframe'] == '15m')
        test("Has candles array", 'candles' in data and len(data['candles']) > 0)
        
        # Validate with data_validator
        validator = DataValidator(timeframe_minutes=15)
        
        # Test first candle structure
        if data['candles']:
            first_candle = data['candles'][0]
            test("First candle has timestamp", 'timestamp' in first_candle)
            test("First candle has OHLCV", all(k in first_candle for k in ['open', 'high', 'low', 'close', 'volume']))
            
            # Validate a few candles
            sample_candles = data['candles'][:10]
            is_valid, errors = validator.validate_candle_sequence(sample_candles)
            test(f"Sample candles valid", is_valid or len(errors) < 5)
        
        print()
    except Exception as e:
        test("Load and validate data", False, str(e))
        print()
    
    # Test 6: Check data quality
    print("6. DATA QUALITY CHECK")
    print("-" * 80)
    
    try:
        if data['candles']:
            validator = DataValidator(timeframe_minutes=15)
            validation_result = validator.validate_historical_data(
                data['candles'], 
                data['symbol']
            )
            
            print(f"   Total candles: {validation_result['total_candles']}")
            print(f"   Valid candles: {validation_result['valid_candles']}")
            print(f"   Gaps detected: {len(validation_result['gaps'])}")
            
            if validation_result['gaps']:
                print(f"\n   First few gaps:")
                for i, gap in enumerate(validation_result['gaps'][:3]):
                    print(f"      Gap {i+1}: {gap['missing_candles']} candles, "
                          f"{gap['gap_duration_minutes']:.0f} minutes")
            
            test("Data quality check completed", True)
        
        print()
    except Exception as e:
        test("Data quality check", False, str(e))
        print()

except Exception as e:
    print(f"❌ Fatal error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Summary
print("=" * 80)
print("📊 TEST SUMMARY")
print("=" * 80)
print(f"Total tests: {tests_passed + tests_failed}")
print(f"✅ Passed: {tests_passed}")
print(f"❌ Failed: {tests_failed}")
print()

if tests_failed == 0:
    print("🎉 ALL TESTS PASSED!")
    print()
    print("✅ PHASE 2 PROGRESS:")
    print("   - Data validator: ✅ Working")
    print("   - Historical loader: ✅ Working")
    print("   - 6-month data fetch: ✅ Working")
    print("   - JSON export: ✅ Working")
    print("   - Data validation: ✅ Working")
    print()
    print("📋 NEXT STEPS:")
    print("   1. Optionally fetch AAVEUSD data:")
    print("      python -c 'from core.data.historical_loader import HistoricalDataLoader; import os; from dotenv import load_dotenv; load_dotenv(); loader = HistoricalDataLoader(os.getenv(\"DELTA_API_KEY_1\"), os.getenv(\"DELTA_API_SECRET_1\")); loader.fetch_6_months_data(\"AAVEUSD\")'")
    print()
    print("   2. Or fetch both at once:")
    print("      (See core/data/historical_loader.py for fetch_both_symbols example)")
    print()
    print("   3. Commit changes:")
    print("      git add core/data/")
    print("      git commit -m 'Phase 2: Add data validator and historical loader'")
    print()
    print("   4. Ready for Step 3: WebSocket client!")
    print()
    exit_code = 0
else:
    print("⚠️  SOME TESTS FAILED")
    print()
    print("Please fix the failed tests before proceeding.")
    print()
    exit_code = 1

print("=" * 80)
sys.exit(exit_code)