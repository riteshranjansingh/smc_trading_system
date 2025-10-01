#!/usr/bin/env python3
"""
Historical Data Loader - Fetch historical OHLCV data from Delta Exchange

Features:
- Fetches 6 months of historical data
- Outputs to JSON format
- Integrates with data_validator for quality checks
- Handles API rate limits with retry logic
- Saves to data/historical/ directory

Usage:
    from core.data.historical_loader import HistoricalDataLoader
    
    loader = HistoricalDataLoader(api_key="...", api_secret="...")
    loader.fetch_6_months_data("SOLUSD")
"""

import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from pathlib import Path

# Import from correct paths
from brokers.delta_exchange.api_client import DeltaExchangeClient
from core.data.data_validator import DataValidator
from core.utils.logger import get_logger

logger = get_logger('system')


class HistoricalDataLoader:
    """
    Fetches and validates historical OHLCV data from Delta Exchange
    """
    
    def __init__(self, api_key: str, api_secret: str, 
                 base_url: str = "https://api.india.delta.exchange",
                 output_dir: str = "data/historical"):
        """
        Initialize historical data loader
        
        Args:
            api_key: Delta Exchange API key
            api_secret: Delta Exchange API secret
            base_url: API base URL (default: India exchange)
            output_dir: Directory to save historical data
        """
        self.client = DeltaExchangeClient(api_key, api_secret, base_url)
        self.validator = DataValidator(timeframe_minutes=15)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Historical data loader initialized")
        logger.info(f"Output directory: {self.output_dir}")
    
    def test_connection(self) -> bool:
        """
        Test API connection
        
        Returns:
            True if connection successful
        """
        success, message = self.client.test_connection()
        
        if success:
            logger.info(f"✅ API connection test successful")
        else:
            logger.error(f"❌ API connection test failed: {message}")
        
        return success
    
    def _convert_date_to_timestamp(self, date_str: str) -> int:
        """Convert date string (YYYY-MM-DD) to Unix timestamp"""
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            return int(dt.timestamp())
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYY-MM-DD. Error: {str(e)}")
    
    def _calculate_6_month_range(self) -> Tuple[str, str]:
        """
        Calculate date range for 6 months of data
        
        Returns:
            Tuple of (start_date, end_date) as strings (YYYY-MM-DD)
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)  # 6 months = ~180 days
        
        return (
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
    
    def _split_date_range(self, start_timestamp: int, end_timestamp: int,
                         max_candles: int = 2000) -> List[Tuple[int, int]]:
        """
        Split large date ranges into smaller chunks to respect API limits
        
        Args:
            start_timestamp: Start timestamp
            end_timestamp: End timestamp
            max_candles: Maximum candles per request (default: 2000)
        
        Returns:
            List of (start, end) timestamp tuples
        """
        # 15m timeframe = 900 seconds per candle
        interval_seconds = 900
        max_duration = max_candles * interval_seconds
        
        chunks = []
        current_start = start_timestamp
        
        while current_start < end_timestamp:
            current_end = min(current_start + max_duration, end_timestamp)
            chunks.append((current_start, current_end))
            current_start = current_end
        
        return chunks
    
    def _fetch_with_retry(self, symbol: str, chunk_start: int, chunk_end: int,
                         max_retries: int = 3) -> List[Dict]:
        """
        Fetch data for a chunk with retry logic
        
        Args:
            symbol: Trading symbol
            chunk_start: Start timestamp
            chunk_end: End timestamp
            max_retries: Maximum retry attempts
        
        Returns:
            List of candle dictionaries
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Add delay for rate limiting
                if retry_count > 0:
                    delay = 2 ** retry_count  # Exponential backoff: 2s, 4s, 8s
                    logger.debug(f"Retry {retry_count}: waiting {delay}s...")
                    time.sleep(delay)
                
                # Fetch data
                candles = self.client.get_historical_candles(
                    symbol=symbol,
                    resolution='15m',  # Fixed to 15m timeframe
                    start_time=chunk_start,
                    end_time=chunk_end
                )
                
                return candles
            
            except Exception as e:
                retry_count += 1
                error_msg = str(e)
                
                # Check for rate limit
                if "rate limit" in error_msg.lower() or "too many requests" in error_msg.lower():
                    if retry_count < max_retries:
                        wait_time = 5 * (2 ** retry_count)  # 10s, 20s, 40s
                        logger.warning(f"Rate limited, waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                
                if retry_count >= max_retries:
                    logger.error(f"Failed after {max_retries} attempts: {error_msg}")
                    raise
        
        return []
    
    def fetch_historical_data(self, symbol: str, start_date: str, end_date: str) -> List[Dict]:
        """
        Fetch historical data for a symbol
        
        Args:
            symbol: Trading symbol (e.g., "SOLUSD")
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Returns:
            List of candle dictionaries with OHLCV data
        """
        logger.info(f"🔄 Fetching historical data: {symbol}")
        logger.info(f"   Date range: {start_date} to {end_date}")
        
        # Convert dates to timestamps
        start_timestamp = self._convert_date_to_timestamp(start_date)
        end_timestamp = self._convert_date_to_timestamp(end_date)
        
        # Split into chunks
        chunks = self._split_date_range(start_timestamp, end_timestamp)
        total_chunks = len(chunks)
        
        logger.info(f"   Fetching {total_chunks} chunks...")
        
        all_candles = []
        
        for i, (chunk_start, chunk_end) in enumerate(chunks):
            try:
                # Log progress
                chunk_date_start = datetime.fromtimestamp(chunk_start).strftime('%Y-%m-%d')
                chunk_date_end = datetime.fromtimestamp(chunk_end).strftime('%Y-%m-%d')
                logger.debug(f"   Chunk {i+1}/{total_chunks}: {chunk_date_start} to {chunk_date_end}")
                
                # Fetch chunk with retry
                candles = self._fetch_with_retry(symbol, chunk_start, chunk_end)
                
                all_candles.extend(candles)
                
                # Small delay between chunks to avoid rate limits
                if i < total_chunks - 1:  # Don't wait after last chunk
                    time.sleep(0.5)
            
            except Exception as e:
                logger.error(f"   Failed to fetch chunk {i+1}: {e}")
                # Continue with other chunks instead of failing completely
        
        if not all_candles:
            raise Exception(f"No data retrieved for {symbol}. Check symbol name and date range.")
        
        # Remove duplicates and sort by timestamp
        unique_candles = {}
        for candle in all_candles:
            timestamp = candle['timestamp']
            if timestamp not in unique_candles:
                unique_candles[timestamp] = candle
        
        sorted_candles = sorted(unique_candles.values(), key=lambda x: x['timestamp'])
        
        logger.info(f"   ✅ Fetched {len(sorted_candles)} candles for {symbol}")
        
        return sorted_candles
    
    def save_to_json(self, candles: List[Dict], symbol: str, 
                    start_date: str, end_date: str) -> str:
        """
        Save candles to JSON file
        
        Args:
            candles: List of candle dictionaries
            symbol: Trading symbol
            start_date: Start date string
            end_date: End date string
        
        Returns:
            Path to saved file
        """
        # Create filename: SOLUSD_15m_2024-04-01_to_2024-10-01.json
        filename = f"{symbol}_15m_{start_date}_to_{end_date}.json"
        filepath = self.output_dir / filename
        
        # Prepare data with metadata
        data = {
            'symbol': symbol,
            'timeframe': '15m',
            'start_date': start_date,
            'end_date': end_date,
            'total_candles': len(candles),
            'fetched_at': datetime.now().isoformat(),
            'candles': candles
        }
        
        # Save to JSON
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"   💾 Saved to: {filepath}")
        
        return str(filepath)
    
    def fetch_6_months_data(self, symbol: str) -> str:
        """
        Fetch 6 months of historical data for a symbol
        
        Args:
            symbol: Trading symbol (e.g., "SOLUSD", "AAVEUSD")
        
        Returns:
            Path to saved JSON file
        """
        logger.info(f"🎯 Fetching 6 months of data for {symbol}")
        
        # Calculate 6-month date range
        start_date, end_date = self._calculate_6_month_range()
        
        logger.info(f"   Calculated range: {start_date} to {end_date}")
        
        # Fetch data
        candles = self.fetch_historical_data(symbol, start_date, end_date)
        
        # Validate data
        logger.info(f"   🔍 Validating data quality...")
        validation_result = self.validator.validate_historical_data(candles, symbol)
        
        if not validation_result['is_valid']:
            logger.warning(f"   ⚠️  Data validation issues found")
            logger.warning(f"       Valid candles: {validation_result['valid_candles']}/{validation_result['total_candles']}")
            logger.warning(f"       Gaps detected: {len(validation_result['gaps'])}")
        else:
            logger.info(f"   ✅ Data validation passed")
        
        # Save to JSON
        filepath = self.save_to_json(candles, symbol, start_date, end_date)
        
        logger.info(f"✅ Successfully fetched and saved {symbol} data")
        
        return filepath
    
    def load_from_json(self, filepath: str) -> Dict:
        """
        Load historical data from JSON file
        
        Args:
            filepath: Path to JSON file
        
        Returns:
            Dictionary with metadata and candles
        """
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        logger.info(f"📂 Loaded {data['total_candles']} candles for {data['symbol']}")
        
        return data
    
    def fetch_both_symbols(self, symbols: List[str] = None) -> Dict[str, str]:
        """
        Fetch 6 months data for multiple symbols
        
        Args:
            symbols: List of symbols (default: ['SOLUSD', 'AAVEUSD'])
        
        Returns:
            Dictionary mapping symbol to filepath
        """
        if symbols is None:
            symbols = ['SOLUSD', 'AAVEUSD']
        
        logger.info(f"🚀 Fetching data for {len(symbols)} symbols")
        
        results = {}
        
        for symbol in symbols:
            try:
                filepath = self.fetch_6_months_data(symbol)
                results[symbol] = filepath
                
                # Delay between symbols to avoid rate limits
                if symbol != symbols[-1]:  # Don't wait after last symbol
                    logger.info(f"   ⏸️  Waiting 2s before next symbol...")
                    time.sleep(2)
            
            except Exception as e:
                logger.error(f"❌ Failed to fetch {symbol}: {e}")
                results[symbol] = None
        
        # Summary
        logger.info(f"\n📊 FETCH SUMMARY")
        logger.info(f"   Total symbols: {len(symbols)}")
        logger.info(f"   Successful: {sum(1 for v in results.values() if v is not None)}")
        logger.info(f"   Failed: {sum(1 for v in results.values() if v is None)}")
        
        return results


# Convenience function
def fetch_historical_data_for_symbols(api_key: str, api_secret: str, 
                                     symbols: List[str] = None) -> Dict[str, str]:
    """
    Convenience function to fetch historical data
    
    Args:
        api_key: Delta Exchange API key
        api_secret: Delta Exchange API secret
        symbols: List of symbols (default: ['SOLUSD', 'AAVEUSD'])
    
    Returns:
        Dictionary mapping symbol to filepath
    
    Example:
        results = fetch_historical_data_for_symbols(
            api_key="your_key",
            api_secret="your_secret"
        )
    """
    loader = HistoricalDataLoader(api_key, api_secret)
    
    # Test connection first
    if not loader.test_connection():
        raise Exception("API connection test failed. Check your credentials.")
    
    return loader.fetch_both_symbols(symbols)


# Example usage and testing
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    print("\n🧪 Testing Historical Data Loader\n")
    
    # Load environment variables
    load_dotenv()
    
    api_key = os.getenv('DELTA_API_KEY_1')
    api_secret = os.getenv('DELTA_API_SECRET_1')
    
    if not api_key or not api_secret:
        print("❌ Error: API credentials not found in .env file")
        print("   Please set DELTA_API_KEY_1 and DELTA_API_SECRET_1")
        exit(1)
    
    try:
        # Create loader
        loader = HistoricalDataLoader(api_key, api_secret, output_dir="data/historical")
        
        # Test connection
        print("1. Testing API connection...")
        if not loader.test_connection():
            raise Exception("API connection failed")
        print()
        
        # Fetch 6 months data for SOLUSD (test with one symbol first)
        print("2. Fetching 6 months data for SOLUSD...")
        filepath = loader.fetch_6_months_data("SOLUSD")
        print(f"   Saved to: {filepath}")
        print()
        
        # Load and verify
        print("3. Loading and verifying data...")
        data = loader.load_from_json(filepath)
        print(f"   Symbol: {data['symbol']}")
        print(f"   Timeframe: {data['timeframe']}")
        print(f"   Total candles: {data['total_candles']}")
        print(f"   Date range: {data['start_date']} to {data['end_date']}")
        print()
        
        # Uncomment to fetch both symbols
        # print("4. Fetching both symbols...")
        # results = loader.fetch_both_symbols(['SOLUSD', 'AAVEUSD'])
        # print(f"   Results: {results}")
        
        print("✅ Historical data loader test complete!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()