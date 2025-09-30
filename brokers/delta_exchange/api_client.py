import requests
import hashlib
import hmac
import time
from typing import Dict, List, Optional, Tuple
import json

class DeltaExchangeClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str = "https://api.india.delta.exchange"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()

    def _generate_signature(self, method: str, endpoint: str, query_string: str = "", payload: str = "") -> Tuple[str, str]:
        """Generate authentication signature for API requests"""
        timestamp = str(int(time.time()))
        message = method + timestamp + endpoint + query_string + payload
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature, timestamp

    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make authenticated API request"""
        url = f"{self.base_url}{endpoint}"

        # Prepare query string and payload for signature
        query_string = ""
        payload = ""

        if method.upper() == "GET" and params:
            # For GET requests, params become query parameters
            query_params = []
            for key, value in sorted(params.items()):  # Sort for consistent ordering
                query_params.append(f"{key}={value}")
            query_string = "?" + "&".join(query_params) if query_params else ""
        elif method.upper() == "POST" and params:
            # For POST requests, params become JSON payload
            payload = json.dumps(params)

        # Generate signature
        signature, timestamp = self._generate_signature(method.upper(), endpoint, query_string, payload)

        # Prepare headers
        headers = {
            'api-key': self.api_key,
            'signature': signature,
            'timestamp': timestamp,
            'User-Agent': 'delta-data-capture/1.0'
        }

        try:
            if method.upper() == "GET":
                response = self.session.get(url, params=params, headers=headers, timeout=30)
            else:
                response = self.session.post(url, json=params, headers=headers, timeout=30)

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            raise Exception(f"API request failed: {str(e)}")

    def test_connection(self) -> Tuple[bool, str]:
        """Test API connection by fetching products"""
        try:
            result = self._make_request("GET", "/v2/products")
            if result.get("success", False):
                return True, "Connection successful"
            else:
                return False, f"API returned error: {result.get('error', 'Unknown error')}"
        except Exception as e:
            return False, str(e)

    def get_products(self) -> List[Dict]:
        """Get all available trading products/symbols"""
        try:
            result = self._make_request("GET", "/v2/products")
            if result.get("success", False):
                return result.get("result", [])
            else:
                raise Exception(f"Failed to fetch products: {result.get('error', 'Unknown error')}")
        except Exception as e:
            raise Exception(f"Error fetching products: {str(e)}")

    def get_historical_candles(self, symbol: str, resolution: str, start_time: int, end_time: int) -> List[Dict]:
        """
        Fetch historical OHLCV candles

        Args:
            symbol: Trading symbol (e.g., "BTCUSD")
            resolution: Timeframe (1m, 5m, 15m, 1h, 4h, 1d, etc.)
            start_time: Start timestamp (Unix timestamp)
            end_time: End timestamp (Unix timestamp)

        Returns:
            List of candle data dictionaries
        """
        try:
            params = {
                'symbol': symbol,
                'resolution': resolution,
                'start': start_time,
                'end': end_time
            }

            # Historical candles endpoint is public (no authentication needed)
            url = f"{self.base_url}/v2/history/candles"
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            result = response.json()

            if result.get("success", False):
                candles = result.get("result", [])
                # Convert to consistent format
                formatted_candles = []
                for candle in candles:
                    # Delta Exchange returns objects with time/open/high/low/close/volume
                    if isinstance(candle, dict) and 'time' in candle:
                        formatted_candles.append({
                            'timestamp': candle['time'],
                            'open': float(candle['open']),
                            'high': float(candle['high']),
                            'low': float(candle['low']),
                            'close': float(candle['close']),
                            'volume': float(candle.get('volume', 0.0))
                        })
                    elif isinstance(candle, list) and len(candle) >= 6:
                        # Fallback for array format
                        formatted_candles.append({
                            'timestamp': candle[0],
                            'open': float(candle[1]),
                            'high': float(candle[2]),
                            'low': float(candle[3]),
                            'close': float(candle[4]),
                            'volume': float(candle[5]) if len(candle) > 5 else 0.0
                        })
                return formatted_candles
            else:
                raise Exception(f"Failed to fetch candles: {result.get('error', 'Unknown error')}")

        except Exception as e:
            raise Exception(f"Error fetching historical candles: {str(e)}")

    def get_symbol_list(self) -> List[str]:
        """Get list of all available trading symbols"""
        try:
            products = self.get_products()
            symbols = []
            for product in products:
                if product.get('symbol'):
                    symbols.append(product['symbol'])
            return sorted(symbols)
        except Exception as e:
            raise Exception(f"Error fetching symbol list: {str(e)}")