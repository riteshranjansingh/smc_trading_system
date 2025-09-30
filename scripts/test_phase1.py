#!/usr/bin/env python3
"""
Phase 1 Verification Script

Tests all Phase 1 deliverables:
1. Project structure
2. Configuration files
3. Logging system
4. State persistence
5. Import paths

Run this after completing Phase 1 to verify everything works.
"""

import sys
import os
from pathlib import Path
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

print("=" * 80)
print("🧪 PHASE 1 VERIFICATION TEST")
print("=" * 80)
print()

# Track results
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


print("1. PROJECT STRUCTURE")
print("-" * 80)

# Check required directories
required_dirs = [
    'brokers/delta_exchange',
    'config',
    'core/data',
    'core/execution',
    'core/risk',
    'core/strategy',
    'core/utils',
    'data',
    'logs',
    'scripts',
    'tests'
]

for dir_path in required_dirs:
    full_path = project_root / dir_path
    test(f"Directory exists: {dir_path}", full_path.exists())

print()


print("2. CONFIGURATION FILES")
print("-" * 80)

# Check config files
config_files = [
    'config/sub_account_1.json',
    'config/sub_account_2.json',
    'config/symbols_config.json'
]

for config_file in config_files:
    full_path = project_root / config_file
    exists = full_path.exists()
    test(f"Config file exists: {config_file}", exists)
    
    if exists:
        try:
            with open(full_path, 'r') as f:
                data = json.load(f)
            test(f"  → Valid JSON: {config_file}", True)
        except json.JSONDecodeError as e:
            test(f"  → Valid JSON: {config_file}", False, str(e))

# Check .env file (optional but recommended)
env_file = project_root / '.env'
if env_file.exists():
    print("✅ .env file exists (secrets configured)")
else:
    print("⚠️  .env file not found (optional)")

print()


print("3. CORE FILES")
print("-" * 80)

# Check critical Python files
core_files = [
    'brokers/delta_exchange/api_client.py',
    'core/data/historical_loader.py',
    'core/data/csv_exporter.py',
    'core/utils/logger.py',
    'core/utils/state_persistence.py'
]

for py_file in core_files:
    full_path = project_root / py_file
    test(f"Python file exists: {py_file}", full_path.exists())

print()


print("4. IMPORT TESTS")
print("-" * 80)

# Test imports
try:
    from core.utils.logger import get_logger
    test("Import logger module", True)
except ImportError as e:
    test("Import logger module", False, str(e))

try:
    from core.utils.state_persistence import get_state_manager
    test("Import state_persistence module", True)
except ImportError as e:
    test("Import state_persistence module", False, str(e))

try:
    from brokers.delta_exchange.api_client import DeltaExchangeClient
    test("Import DeltaExchangeClient", True)
except ImportError as e:
    test("Import DeltaExchangeClient", False, str(e))

print()


print("5. LOGGING SYSTEM TEST")
print("-" * 80)

try:
    from core.utils.logger import get_logger, SMCLogger
    
    # Create test logger
    test_logger = get_logger('system')
    test("Create logger instance", test_logger is not None)
    
    # Test log message
    test_logger.info("Phase 1 verification test message")
    test("Logger can write messages", True)
    
    # Test convenience methods
    SMCLogger.log_trade(
        action="TEST_ENTRY",
        symbol="SOLUSD",
        direction="LONG",
        price=150.00,
        size=1.0
    )
    test("Trade logging works", True)
    
    # Check log file created
    log_file = project_root / 'logs' / 'system.log'
    test("Log file created", log_file.exists())
    
except Exception as e:
    test("Logging system test", False, str(e))

print()


print("6. STATE PERSISTENCE TEST")
print("-" * 80)

try:
    from core.utils.state_persistence import StatePersistence
    
    # Create test state manager
    state = StatePersistence(data_dir="data/test")
    test("Create state manager", True)
    
    # Test OB state
    test_obs = {
        'SOLUSD': {
            'bullish': [{'top': 150.0, 'bottom': 148.5}],
            'bearish': []
        }
    }
    
    saved = state.save_ob_state(test_obs)
    test("Save OB state", saved)
    
    loaded_obs = state.load_ob_state()
    test("Load OB state", loaded_obs == test_obs)
    
    # Test positions
    test_positions = {
        'account_1': [{'symbol': 'SOLUSD', 'size': 1.0}],
        'account_2': []
    }
    
    saved = state.save_positions(test_positions)
    test("Save positions", saved)
    
    loaded_pos = state.load_positions()
    test("Load positions", loaded_pos == test_positions)
    
    # Clean up test data
    state.clear_all_state()
    test("Clear test state", True)
    
except Exception as e:
    test("State persistence test", False, str(e))

print()


print("7. REQUIREMENTS.TXT")
print("-" * 80)

req_file = project_root / 'requirements.txt'
test("requirements.txt exists", req_file.exists())

if req_file.exists():
    with open(req_file, 'r') as f:
        requirements = f.read()
    
    # Check for key dependencies
    required_packages = ['requests', 'websockets', 'pandas', 'numpy']
    
    for package in required_packages:
        test(f"  → {package} in requirements", package in requirements.lower())

print()


print("8. GIT REPOSITORY")
print("-" * 80)

git_dir = project_root / '.git'
test("Git repository initialized", git_dir.exists())

gitignore = project_root / '.gitignore'
test(".gitignore exists", gitignore.exists())

if gitignore.exists():
    with open(gitignore, 'r') as f:
        content = f.read()
    test("  → Ignores venv/", 'venv' in content)
    test("  → Ignores .env", '.env' in content)
    test("  → Ignores data/", 'data/' in content or 'data/*' in content)

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
    print("🎉 ALL TESTS PASSED! Phase 1 is complete!")
    print()
    print("✅ PHASE 1 DELIVERABLES:")
    print("   - Project structure created")
    print("   - Configuration files set up")
    print("   - Logging system working")
    print("   - State persistence working")
    print("   - Git repository initialized")
    print()
    print("📋 NEXT STEPS:")
    print("   1. Commit your changes:")
    print("      git add .")
    print("      git commit -m 'Complete Phase 1: Foundation setup'")
    print("      git push")
    print()
    print("   2. Update project tracker (mark Phase 1 as complete)")
    print()
    print("   3. Ready to start Phase 2: Data Layer!")
    print()
    exit_code = 0
else:
    print("⚠️  SOME TESTS FAILED")
    print()
    print("Please fix the failed tests before proceeding to Phase 2.")
    print("Review the error messages above for details.")
    print()
    exit_code = 1

print("=" * 80)
sys.exit(exit_code)