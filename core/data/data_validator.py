#!/usr/bin/env python3
"""
Data Validator - Validates OHLCV data integrity

This module provides validation for:
- OHLCV relationship integrity
- Timestamp sequencing
- Data gaps detection
- Candle completeness

Used throughout the data layer to ensure data quality.
"""

from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates OHLCV data integrity and detects issues"""
    
    def __init__(self, timeframe_minutes: int = 15):
        """
        Initialize validator
        
        Args:
            timeframe_minutes: Expected candle timeframe in minutes (default 15)
        """
        self.timeframe_minutes = timeframe_minutes
        self.expected_interval = timedelta(minutes=timeframe_minutes)
        
    def validate_ohlcv_candle(self, candle: Dict) -> Tuple[bool, List[str]]:
        """
        Validate a single OHLCV candle
        
        Args:
            candle: Dictionary with keys: timestamp, open, high, low, close, volume
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check required fields
        required_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        missing_fields = [f for f in required_fields if f not in candle]
        if missing_fields:
            errors.append(f"Missing required fields: {missing_fields}")
            return False, errors
        
        # Extract values
        try:
            timestamp = candle['timestamp']
            open_price = float(candle['open'])
            high = float(candle['high'])
            low = float(candle['low'])
            close = float(candle['close'])
            volume = float(candle['volume'])
        except (ValueError, TypeError) as e:
            errors.append(f"Invalid numeric values: {e}")
            return False, errors
        
        # Validate OHLCV relationships
        # 1. High must be >= Low
        if high < low:
            errors.append(f"High ({high}) < Low ({low})")
        
        # 2. High must be >= max(Open, Close)
        if high < max(open_price, close):
            errors.append(f"High ({high}) < max(Open={open_price}, Close={close})")
        
        # 3. Low must be <= min(Open, Close)
        if low > min(open_price, close):
            errors.append(f"Low ({low}) > min(Open={open_price}, Close={close})")
        
        # 4. All prices must be positive
        if any(p <= 0 for p in [open_price, high, low, close]):
            errors.append(f"Negative or zero prices detected")
        
        # 5. Volume must be non-negative
        if volume < 0:
            errors.append(f"Negative volume: {volume}")
        
        # 6. Validate timestamp
        if not isinstance(timestamp, (int, float, str, datetime)):
            errors.append(f"Invalid timestamp type: {type(timestamp)}")
        
        is_valid = len(errors) == 0
        return is_valid, errors
    
    def validate_candle_sequence(self, candles: List[Dict]) -> Tuple[bool, List[str]]:
        """
        Validate a sequence of candles for gaps and ordering
        
        Args:
            candles: List of candle dictionaries (must be sorted by timestamp)
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        if not candles:
            errors.append("Empty candle list")
            return False, errors
        
        if len(candles) < 2:
            # Single candle - just validate it
            return self.validate_ohlcv_candle(candles[0])
        
        # Check each candle individually
        for i, candle in enumerate(candles):
            is_valid, candle_errors = self.validate_ohlcv_candle(candle)
            if not is_valid:
                errors.extend([f"Candle {i}: {e}" for e in candle_errors])
        
        # Check sequence ordering and gaps
        for i in range(1, len(candles)):
            prev_candle = candles[i-1]
            curr_candle = candles[i]
            
            # Convert timestamps to datetime for comparison
            prev_time = self._parse_timestamp(prev_candle['timestamp'])
            curr_time = self._parse_timestamp(curr_candle['timestamp'])
            
            if prev_time is None or curr_time is None:
                errors.append(f"Invalid timestamp at position {i}")
                continue
            
            # Check ordering
            if curr_time <= prev_time:
                errors.append(f"Timestamps not in order: {prev_time} >= {curr_time}")
            
            # Check for gaps (missing candles)
            expected_time = prev_time + self.expected_interval
            time_diff = curr_time - prev_time
            
            # Allow small tolerance (1 second) for timestamp precision
            if abs((curr_time - expected_time).total_seconds()) > 1:
                if time_diff > self.expected_interval:
                    missing_candles = int(time_diff.total_seconds() / (self.expected_interval.total_seconds()))
                    errors.append(
                        f"Gap detected: {prev_time} to {curr_time} "
                        f"(~{missing_candles} candles missing)"
                    )
        
        is_valid = len(errors) == 0
        return is_valid, errors
    
    def detect_data_gaps(self, candles: List[Dict]) -> List[Dict]:
        """
        Detect gaps in candle data
        
        Args:
            candles: List of candle dictionaries (must be sorted by timestamp)
            
        Returns:
            List of gap information dictionaries with keys:
            - start_time: When gap starts
            - end_time: When gap ends
            - missing_candles: Number of missing candles
        """
        gaps = []
        
        if len(candles) < 2:
            return gaps
        
        for i in range(1, len(candles)):
            prev_candle = candles[i-1]
            curr_candle = candles[i]
            
            prev_time = self._parse_timestamp(prev_candle['timestamp'])
            curr_time = self._parse_timestamp(curr_candle['timestamp'])
            
            if prev_time is None or curr_time is None:
                continue
            
            expected_time = prev_time + self.expected_interval
            time_diff = curr_time - prev_time
            
            # If gap is larger than expected interval
            if time_diff > self.expected_interval + timedelta(seconds=1):
                missing_candles = int(time_diff.total_seconds() / (self.expected_interval.total_seconds())) - 1
                
                gaps.append({
                    'start_time': prev_time,
                    'end_time': curr_time,
                    'missing_candles': missing_candles,
                    'gap_duration_minutes': time_diff.total_seconds() / 60
                })
        
        return gaps
    
    def validate_historical_data(self, candles: List[Dict], symbol: str) -> Dict:
        """
        Comprehensive validation of historical data
        
        Args:
            candles: List of candle dictionaries
            symbol: Symbol name (for logging)
            
        Returns:
            Dictionary with validation results:
            - is_valid: Overall validity
            - total_candles: Number of candles
            - valid_candles: Number of valid candles
            - gaps: List of gaps detected
            - errors: List of all errors
        """
        logger.info(f"🔍 Validating historical data for {symbol}")
        logger.info(f"   Total candles: {len(candles)}")
        
        # Validate sequence
        is_valid, errors = self.validate_candle_sequence(candles)
        
        # Detect gaps
        gaps = self.detect_data_gaps(candles)
        
        # Count valid candles
        valid_count = 0
        for candle in candles:
            candle_valid, _ = self.validate_ohlcv_candle(candle)
            if candle_valid:
                valid_count += 1
        
        # Log results
        if is_valid:
            logger.info(f"   ✅ All candles valid, no gaps detected")
        else:
            logger.warning(f"   ⚠️  Validation issues found:")
            for error in errors[:5]:  # Show first 5 errors
                logger.warning(f"      - {error}")
            if len(errors) > 5:
                logger.warning(f"      ... and {len(errors) - 5} more errors")
        
        if gaps:
            logger.warning(f"   ⚠️  {len(gaps)} data gaps detected:")
            for gap in gaps[:3]:  # Show first 3 gaps
                logger.warning(
                    f"      Gap: {gap['missing_candles']} candles missing "
                    f"({gap['gap_duration_minutes']:.1f} minutes)"
                )
            if len(gaps) > 3:
                logger.warning(f"      ... and {len(gaps) - 3} more gaps")
        
        return {
            'is_valid': is_valid,
            'total_candles': len(candles),
            'valid_candles': valid_count,
            'gaps': gaps,
            'errors': errors,
            'symbol': symbol
        }
    
    def _parse_timestamp(self, timestamp) -> Optional[datetime]:
        """
        Parse timestamp to datetime object
        
        Args:
            timestamp: Can be int (unix), float (unix), str (ISO), or datetime
            
        Returns:
            datetime object or None if invalid
        """
        try:
            if isinstance(timestamp, datetime):
                return timestamp
            elif isinstance(timestamp, (int, float)):
                # Unix timestamp (seconds)
                return datetime.fromtimestamp(timestamp)
            elif isinstance(timestamp, str):
                # ISO format string
                return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            else:
                return None
        except Exception as e:
            logger.error(f"Error parsing timestamp {timestamp}: {e}")
            return None


# Example usage and tests
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("🧪 Testing Data Validator\n")
    
    validator = DataValidator(timeframe_minutes=15)
    
    # Test 1: Valid candle
    print("Test 1: Valid candle")
    valid_candle = {
        'timestamp': 1696118400,  # Unix timestamp
        'open': 100.0,
        'high': 105.0,
        'low': 99.0,
        'close': 103.0,
        'volume': 1000.0
    }
    is_valid, errors = validator.validate_ohlcv_candle(valid_candle)
    print(f"   Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
    if errors:
        print(f"   Errors: {errors}")
    print()
    
    # Test 2: Invalid candle (high < low)
    print("Test 2: Invalid candle (high < low)")
    invalid_candle = {
        'timestamp': 1696118400,
        'open': 100.0,
        'high': 99.0,  # High < Low (INVALID)
        'low': 101.0,
        'close': 100.5,
        'volume': 1000.0
    }
    is_valid, errors = validator.validate_ohlcv_candle(invalid_candle)
    print(f"   Result: {'✅ PASS' if is_valid else '❌ FAIL (expected)'}")
    print(f"   Errors: {errors}")
    print()
    
    # Test 3: Candle sequence with gap
    print("Test 3: Candle sequence with gap")
    candles_with_gap = [
        {'timestamp': 1696118400, 'open': 100, 'high': 105, 'low': 99, 'close': 103, 'volume': 1000},
        {'timestamp': 1696119300, 'open': 103, 'high': 107, 'low': 102, 'close': 106, 'volume': 1200},
        # Gap here - missing 1696120200 (15 minutes)
        {'timestamp': 1696121100, 'open': 106, 'high': 110, 'low': 105, 'close': 108, 'volume': 1500},
    ]
    is_valid, errors = validator.validate_candle_sequence(candles_with_gap)
    gaps = validator.detect_data_gaps(candles_with_gap)
    print(f"   Result: {'✅ PASS' if is_valid else '❌ FAIL (gap detected)'}")
    print(f"   Gaps found: {len(gaps)}")
    if gaps:
        for gap in gaps:
            print(f"      - {gap['missing_candles']} candles missing, {gap['gap_duration_minutes']:.1f} min gap")
    print()
    
    # Test 4: Comprehensive validation
    print("Test 4: Comprehensive historical data validation")
    test_symbol = "SOLUSD"
    result = validator.validate_historical_data(candles_with_gap, test_symbol)
    print(f"   Symbol: {result['symbol']}")
    print(f"   Total candles: {result['total_candles']}")
    print(f"   Valid candles: {result['valid_candles']}")
    print(f"   Gaps: {len(result['gaps'])}")
    print(f"   Overall valid: {'✅ YES' if result['is_valid'] else '❌ NO'}")
    print()
    
    print("✅ All tests complete!")