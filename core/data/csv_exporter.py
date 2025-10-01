import pandas as pd
import os
from datetime import datetime
from typing import List, Dict
import logging

class CSVExporter:
    def __init__(self, base_output_path: str = "./data"):
        self.base_output_path = base_output_path
        self.logger = logging.getLogger(__name__)

    def _create_directory_structure(self, symbol: str, timeframe: str) -> str:
        """Create organized directory structure for data"""
        # Create path: data/BTC-USDT/1h/
        symbol_path = os.path.join(self.base_output_path, symbol)
        timeframe_path = os.path.join(symbol_path, timeframe)

        os.makedirs(timeframe_path, exist_ok=True)
        return timeframe_path

    def _generate_filename(self, symbol: str, timeframe: str, start_date: str, end_date: str) -> str:
        """Generate standardized filename"""
        # Format: BTC-USDT_1h_2024-01-01_to_2024-12-31.csv
        return f"{symbol}_{timeframe}_{start_date}_to_{end_date}.csv"

    def _convert_timestamps(self, candles: List[Dict]) -> List[Dict]:
        """Convert Unix timestamps to readable datetime"""
        converted_candles = []
        for candle in candles:
            converted_candle = candle.copy()
            # Convert timestamp to datetime
            if 'timestamp' in converted_candle:
                dt = datetime.fromtimestamp(converted_candle['timestamp'])
                converted_candle['datetime'] = dt.strftime('%Y-%m-%d %H:%M:%S')
            converted_candles.append(converted_candle)
        return converted_candles

    def export_to_csv(self, candles: List[Dict], symbol: str, timeframe: str,
                     start_date: str, end_date: str) -> str:
        """
        Export candle data to CSV file

        Args:
            candles: List of candle dictionaries with OHLCV data
            symbol: Trading symbol (e.g., "BTC-USDT")
            timeframe: Timeframe (e.g., "1h")
            start_date: Start date string (YYYY-MM-DD)
            end_date: End date string (YYYY-MM-DD)

        Returns:
            Full path to the created CSV file
        """
        try:
            if not candles:
                raise ValueError("No candle data provided")

            # Create directory structure
            output_dir = self._create_directory_structure(symbol, timeframe)

            # Generate filename
            filename = self._generate_filename(symbol, timeframe, start_date, end_date)
            file_path = os.path.join(output_dir, filename)

            # Convert timestamps to readable format
            processed_candles = self._convert_timestamps(candles)

            # Create DataFrame
            df = pd.DataFrame(processed_candles)

            # Reorder columns for better readability
            column_order = ['datetime', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
            existing_columns = [col for col in column_order if col in df.columns]
            df = df[existing_columns]

            # Sort by timestamp (oldest first)
            if 'timestamp' in df.columns:
                df = df.sort_values('timestamp')

            # Export to CSV
            df.to_csv(file_path, index=False)

            self.logger.info(f"Successfully exported {len(candles)} candles to {file_path}")
            return file_path

        except Exception as e:
            error_msg = f"Error exporting CSV: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def get_export_summary(self, file_path: str) -> Dict:
        """Get summary information about exported file"""
        try:
            if not os.path.exists(file_path):
                return {"error": "File not found"}

            file_size = os.path.getsize(file_path)
            file_size_mb = round(file_size / (1024 * 1024), 2)

            # Read file to get row count
            df = pd.read_csv(file_path)
            row_count = len(df)

            return {
                "file_path": file_path,
                "file_size_mb": file_size_mb,
                "row_count": row_count,
                "created_at": datetime.fromtimestamp(os.path.getctime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            return {"error": str(e)}

    def validate_data_quality(self, candles: List[Dict]) -> Dict:
        """Validate the quality of candle data before export"""
        if not candles:
            return {"valid": False, "issues": ["No data provided"]}

        issues = []

        # Check for required fields
        required_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        sample_candle = candles[0]

        for field in required_fields:
            if field not in sample_candle:
                issues.append(f"Missing required field: {field}")

        # Check for data consistency
        for i, candle in enumerate(candles[:10]):  # Check first 10 candles
            try:
                high = float(candle.get('high', 0))
                low = float(candle.get('low', 0))
                open_price = float(candle.get('open', 0))
                close_price = float(candle.get('close', 0))

                # High should be >= Low
                if high < low:
                    issues.append(f"Candle {i}: High ({high}) < Low ({low})")

                # High should be >= Open and Close
                if high < max(open_price, close_price):
                    issues.append(f"Candle {i}: High price inconsistency")

                # Low should be <= Open and Close
                if low > min(open_price, close_price):
                    issues.append(f"Candle {i}: Low price inconsistency")

            except (ValueError, TypeError):
                issues.append(f"Candle {i}: Invalid price data")

        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "total_candles": len(candles)
        }