import time
from datetime import datetime, timedelta
from typing import List, Dict, Callable, Optional, Tuple
import logging
from .api_client import DeltaExchangeClient
from .csv_exporter import CSVExporter

class DataFetcher:
    def __init__(self, api_key: str, api_secret: str, base_url: str = "https://api.india.delta.exchange"):
        self.client = DeltaExchangeClient(api_key, api_secret, base_url)
        self.exporter = CSVExporter()
        self.logger = logging.getLogger(__name__)

    def test_connection(self) -> Tuple[bool, str]:
        """Test API connection"""
        return self.client.test_connection()

    def get_available_symbols(self) -> List[str]:
        """Get list of available trading symbols"""
        return self.client.get_symbol_list()

    def _convert_date_to_timestamp(self, date_str: str) -> int:
        """Convert date string (YYYY-MM-DD) to Unix timestamp"""
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            return int(dt.timestamp())
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYY-MM-DD. Error: {str(e)}")

    def _split_date_range(self, start_timestamp: int, end_timestamp: int,
                         max_candles: int, timeframe: str) -> List[Tuple[int, int]]:
        """
        Split large date ranges into smaller chunks to respect API limits

        Args:
            start_timestamp: Start timestamp
            end_timestamp: End timestamp
            max_candles: Maximum candles per request (usually 2000)
            timeframe: Timeframe string (1m, 5m, 1h, etc.)

        Returns:
            List of (start, end) timestamp tuples
        """
        # Convert timeframe to seconds
        timeframe_to_seconds = {
            '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600,
            '1d': 86400, '7d': 604800, '30d': 2592000
        }

        if timeframe not in timeframe_to_seconds:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        interval_seconds = timeframe_to_seconds[timeframe]
        max_duration = max_candles * interval_seconds

        chunks = []
        current_start = start_timestamp

        while current_start < end_timestamp:
            current_end = min(current_start + max_duration, end_timestamp)
            chunks.append((current_start, current_end))
            current_start = current_end

        return chunks

    def fetch_historical_data(self, symbol: str, timeframe: str, start_date: str, end_date: str,
                            progress_callback: Optional[Callable[[int, int, str], None]] = None,
                            output_path: Optional[str] = None) -> str:
        """
        Fetch historical data and export to CSV

        Args:
            symbol: Trading symbol (e.g., "BTC-USDT")
            timeframe: Timeframe (1m, 5m, 15m, 1h, 4h, 1d, etc.)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            progress_callback: Optional callback function for progress updates
            output_path: Optional custom output path

        Returns:
            Path to the created CSV file
        """
        try:
            # Validate inputs
            if not symbol or not timeframe or not start_date or not end_date:
                raise ValueError("All parameters (symbol, timeframe, start_date, end_date) are required")

            # Convert dates to timestamps
            start_timestamp = self._convert_date_to_timestamp(start_date)
            end_timestamp = self._convert_date_to_timestamp(end_date)

            if start_timestamp >= end_timestamp:
                raise ValueError("Start date must be before end date")

            # Update output path if provided
            if output_path:
                self.exporter.base_output_path = output_path

            # Split date range into manageable chunks
            max_candles = 2000  # Delta Exchange API limit
            chunks = self._split_date_range(start_timestamp, end_timestamp, max_candles, timeframe)

            if progress_callback:
                progress_callback(0, len(chunks), f"Starting data fetch for {symbol} {timeframe}")

            all_candles = []

            for i, (chunk_start, chunk_end) in enumerate(chunks):
                max_retries = 3
                retry_count = 0
                chunk_success = False

                while retry_count < max_retries and not chunk_success:
                    try:
                        if progress_callback:
                            retry_text = f" (retry {retry_count + 1})" if retry_count > 0 else ""
                            progress_callback(i, len(chunks),
                                            f"Fetching chunk {i+1}/{len(chunks)} for {symbol} {timeframe}{retry_text}")

                        # Add progressive delay for intraday data and retries
                        if i > 0 or retry_count > 0:
                            # Longer delays for intraday data and retries
                            delay = 0.5 if timeframe in ['1m', '3m', '5m', '15m', '30m', '1h'] else 0.1
                            delay = delay * (2 ** retry_count)  # Exponential backoff
                            time.sleep(delay)

                        # Fetch data for this chunk
                        candles = self.client.get_historical_candles(
                            symbol=symbol,
                            resolution=timeframe,
                            start_time=chunk_start,
                            end_time=chunk_end
                        )

                        all_candles.extend(candles)
                        chunk_success = True

                        # Log successful chunk
                        chunk_date_start = datetime.fromtimestamp(chunk_start).strftime('%Y-%m-%d')
                        chunk_date_end = datetime.fromtimestamp(chunk_end).strftime('%Y-%m-%d')
                        self.logger.info(f"Successfully fetched chunk {i+1}/{len(chunks)}: {chunk_date_start} to {chunk_date_end}, {len(candles)} candles")

                    except Exception as e:
                        retry_count += 1
                        error_msg = f"Error fetching chunk {i+1} (attempt {retry_count}): {str(e)}"
                        self.logger.warning(error_msg)

                        # Check if it's a rate limit error
                        if "rate limit" in str(e).lower() or "too many requests" in str(e).lower():
                            if retry_count < max_retries:
                                wait_time = 5 * (2 ** retry_count)  # 10s, 20s, 40s
                                if progress_callback:
                                    progress_callback(i, len(chunks), f"Rate limited, waiting {wait_time}s before retry...")
                                time.sleep(wait_time)
                            continue

                        if retry_count >= max_retries:
                            if progress_callback:
                                progress_callback(i, len(chunks), f"Failed chunk {i+1} after {max_retries} attempts")
                            self.logger.error(f"Failed to fetch chunk {i+1} after {max_retries} attempts: {str(e)}")

                if not chunk_success:
                    # Log the failed chunk details for debugging
                    chunk_date_start = datetime.fromtimestamp(chunk_start).strftime('%Y-%m-%d')
                    chunk_date_end = datetime.fromtimestamp(chunk_end).strftime('%Y-%m-%d')
                    self.logger.error(f"Permanently failed chunk {i+1}: {chunk_date_start} to {chunk_date_end}")

            if not all_candles:
                # Provide detailed info about what failed
                total_chunks = len(chunks)
                failed_chunks = sum(1 for i in range(total_chunks) if i not in [j for j, _ in enumerate(all_candles)])
                raise Exception(f"No data retrieved. Processed {total_chunks} chunks, {failed_chunks} failed. Check symbol name, date range, and logs for details.")

            # Remove duplicates and sort by timestamp
            unique_candles = {}
            for candle in all_candles:
                timestamp = candle['timestamp']
                if timestamp not in unique_candles:
                    unique_candles[timestamp] = candle

            sorted_candles = sorted(unique_candles.values(), key=lambda x: x['timestamp'])

            # Log final statistics
            total_requested_chunks = len(chunks)
            total_candles_received = len(sorted_candles)
            date_range_start = datetime.fromtimestamp(chunks[0][0]).strftime('%Y-%m-%d')
            date_range_end = datetime.fromtimestamp(chunks[-1][1]).strftime('%Y-%m-%d')

            self.logger.info(f"Download summary: {total_candles_received} candles from {total_requested_chunks} chunks ({date_range_start} to {date_range_end})")

            if progress_callback:
                progress_callback(len(chunks), len(chunks),
                                f"Processing {len(sorted_candles)} candles for export...")

            # Validate data quality
            validation_result = self.exporter.validate_data_quality(sorted_candles)
            if not validation_result['valid']:
                self.logger.warning(f"Data quality issues found: {validation_result['issues']}")

            # Export to CSV
            csv_file_path = self.exporter.export_to_csv(
                candles=sorted_candles,
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date
            )

            if progress_callback:
                progress_callback(len(chunks), len(chunks),
                                f"Successfully exported {len(sorted_candles)} candles")

            return csv_file_path

        except Exception as e:
            error_msg = f"Error in fetch_historical_data: {str(e)}"
            self.logger.error(error_msg)
            if progress_callback:
                progress_callback(0, 1, f"Error: {error_msg}")
            raise Exception(error_msg)

    def get_data_summary(self, csv_file_path: str) -> Dict:
        """Get summary of exported data"""
        return self.exporter.get_export_summary(csv_file_path)