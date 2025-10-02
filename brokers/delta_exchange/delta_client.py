#!/usr/bin/env python3
"""
Delta Exchange Trading Client

High-level trading client for Delta Exchange India.
Builds on top of the low-level api_client.py

Features:
- Place market/limit orders
- Cancel orders
- Get positions and balances
- Auto-lookup product_id from symbol
- Proper error handling

Usage:
    from brokers.delta_exchange.api_client import DeltaExchangeClient
    from brokers.delta_exchange.delta_client import DeltaTradingClient
    
    api_client = DeltaExchangeClient(api_key, api_secret)
    trading_client = DeltaTradingClient(api_client)
    
    # Place market order
    order = trading_client.place_market_order("SOLUSD", "buy", 10)
    
    # Place limit order  
    order = trading_client.place_limit_order("SOLUSD", "buy", 10, 150.50)
"""

import json
from typing import Dict, List, Optional, Tuple
from pathlib import Path

from brokers.delta_exchange.api_client import DeltaExchangeClient
from core.utils.logger import get_logger

logger = get_logger('system')


class DeltaTradingClient:
    """
    High-level trading client for Delta Exchange India
    
    Handles order placement, position management, and account queries.
    """
    
    def __init__(self, api_client: DeltaExchangeClient, 
                 symbols_config_path: str = "config/symbols_config.json"):
        """
        Initialize Delta Trading Client
        
        Args:
            api_client: Low-level API client (handles authentication)
            symbols_config_path: Path to symbols configuration file
        """
        self.api_client = api_client
        self.symbols_config_path = symbols_config_path
        
        # Load symbols configuration
        self.symbols_config = self._load_symbols_config()
        
        logger.info("Delta Trading Client initialized")
        logger.info(f"   Loaded config for {len(self.symbols_config)} symbols")
    
    def _load_symbols_config(self) -> Dict:
        """Load symbols configuration from JSON"""
        try:
            config_path = Path(self.symbols_config_path)
            if not config_path.exists():
                logger.error(f"Symbols config not found: {config_path}")
                return {}
            
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            logger.debug(f"Loaded symbols config: {list(config.keys())}")
            return config
            
        except Exception as e:
            logger.error(f"Error loading symbols config: {e}")
            return {}
    
    def get_product_id(self, symbol: str) -> int:
        """
        Get product_id for a symbol
        
        Args:
            symbol: Trading symbol (e.g., "SOLUSD")
        
        Returns:
            Product ID (e.g., 27 for SOLUSD)
        
        Raises:
            ValueError: If symbol not found in config
        """
        if symbol not in self.symbols_config:
            raise ValueError(f"Symbol {symbol} not found in config. "
                           f"Available: {list(self.symbols_config.keys())}")
        
        return self.symbols_config[symbol]['product_id']
    
    def get_symbol_specs(self, symbol: str) -> Dict:
        """
        Get full specifications for a symbol
        
        Args:
            symbol: Trading symbol
        
        Returns:
            Dict with product_id, qty_per_contract, min_quantity, tick_size
        """
        if symbol not in self.symbols_config:
            raise ValueError(f"Symbol {symbol} not found in config")
        
        return self.symbols_config[symbol]
    
    def place_market_order(self, symbol: str, side: str, size: int,
                          reduce_only: bool = False) -> Dict:
        """
        Place a market order
        
        Args:
            symbol: Trading symbol (e.g., "SOLUSD")
            side: "buy" or "sell"
            size: Number of contracts
            reduce_only: If True, order only reduces position (doesn't open new)
        
        Returns:
            Order response dictionary
        
        Example:
            order = client.place_market_order("SOLUSD", "buy", 10)
        """
        try:
            product_id = self.get_product_id(symbol)
            
            # Build order payload
            payload = {
                "product_id": product_id,
                "size": size,
                "side": side,
                "order_type": "market_order",
                "reduce_only": reduce_only
            }
            
            logger.info(f"Placing MARKET order: {symbol} {side.upper()} {size} contracts")
            
            # Place order via API
            result = self.api_client._make_request("POST", "/v2/orders", params=payload)
            
            if result.get("success"):
                order = result.get("result", {})
                order_id = order.get("id")
                logger.info(f"   Order placed: ID={order_id}")
                return order
            else:
                error = result.get("error", "Unknown error")
                logger.error(f"   Order failed: {error}")
                raise Exception(f"Order placement failed: {error}")
        
        except Exception as e:
            logger.error(f"Error placing market order: {e}")
            raise
    
    def place_limit_order(self, symbol: str, side: str, size: int, price: float,
                         post_only: bool = False, reduce_only: bool = False,
                         time_in_force: str = "gtc") -> Dict:
        """
        Place a limit order
        
        Args:
            symbol: Trading symbol (e.g., "SOLUSD")
            side: "buy" or "sell"
            size: Number of contracts
            price: Limit price
            post_only: If True, order is maker-only (won't take liquidity)
            reduce_only: If True, order only reduces position
            time_in_force: "gtc" (good-till-cancel) or "ioc" (immediate-or-cancel)
        
        Returns:
            Order response dictionary
        
        Example:
            order = client.place_limit_order("SOLUSD", "buy", 10, 150.50)
        """
        try:
            product_id = self.get_product_id(symbol)
            specs = self.get_symbol_specs(symbol)
            
            # Round price to tick_size
            tick_size = specs.get('tick_size', 0.01)
            rounded_price = round(price / tick_size) * tick_size
            
            if rounded_price != price:
                logger.debug(f"Price rounded: {price:.4f} -> {rounded_price:.4f} (tick={tick_size})")
            
            # Build order payload
            payload = {
                "product_id": product_id,
                "size": size,
                "side": side,
                "order_type": "limit_order",
                "limit_price": str(rounded_price),  # API expects string
                "time_in_force": time_in_force,
                "post_only": post_only,
                "reduce_only": reduce_only
            }
            
            logger.info(f"Placing LIMIT order: {symbol} {side.upper()} {size} @ ${rounded_price:.4f}")
            
            # Place order via API
            result = self.api_client._make_request("POST", "/v2/orders", params=payload)
            
            if result.get("success"):
                order = result.get("result", {})
                order_id = order.get("id")
                logger.info(f"   Order placed: ID={order_id}")
                return order
            else:
                error = result.get("error", "Unknown error")
                logger.error(f"   Order failed: {error}")
                raise Exception(f"Order placement failed: {error}")
        
        except Exception as e:
            logger.error(f"Error placing limit order: {e}")
            raise
    
    def cancel_order(self, order_id: int) -> bool:
        """
        Cancel an open order
        
        Args:
            order_id: Order ID to cancel
        
        Returns:
            True if cancelled successfully
        
        Example:
            success = client.cancel_order(12345678)
        """
        try:
            logger.info(f"Cancelling order: ID={order_id}")
            
            # Cancel via API
            result = self.api_client._make_request("DELETE", f"/v2/orders/{order_id}")
            
            if result.get("success"):
                logger.info(f"   Order cancelled: ID={order_id}")
                return True
            else:
                error = result.get("error", "Unknown error")
                logger.error(f"   Cancel failed: {error}")
                return False
        
        except Exception as e:
            logger.error(f"Error cancelling order: {e}")
            return False
    
    def cancel_all_orders(self, symbol: Optional[str] = None) -> Dict:
        """
        Cancel all open orders (optionally for a specific symbol)
        
        Args:
            symbol: If provided, cancel only orders for this symbol
        
        Returns:
            Dict with cancellation results
        """
        try:
            params = {}
            if symbol:
                product_id = self.get_product_id(symbol)
                params['product_id'] = product_id
            
            logger.info(f"Cancelling all orders{' for ' + symbol if symbol else ''}")
            
            result = self.api_client._make_request("DELETE", "/v2/orders/all", params=params)
            
            if result.get("success"):
                cancelled = result.get("result", [])
                logger.info(f"   Cancelled {len(cancelled)} orders")
                return result
            else:
                error = result.get("error", "Unknown error")
                logger.error(f"   Cancel all failed: {error}")
                return result
        
        except Exception as e:
            logger.error(f"Error cancelling all orders: {e}")
            raise
    
    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        """
        Get all open orders
        
        Args:
            symbol: If provided, filter by symbol
        
        Returns:
            List of open order dictionaries
        """
        try:
            params = {"state": "open"}
            
            if symbol:
                product_id = self.get_product_id(symbol)
                params['product_id'] = product_id
            
            result = self.api_client._make_request("GET", "/v2/orders", params=params)
            
            if result.get("success"):
                orders = result.get("result", [])
                logger.debug(f"Retrieved {len(orders)} open orders")
                return orders
            else:
                logger.error(f"Failed to get open orders: {result.get('error')}")
                return []
        
        except Exception as e:
            logger.error(f"Error getting open orders: {e}")
            return []
    
    def get_order_status(self, order_id: int) -> Optional[Dict]:
        """
        Get status of a specific order
        
        Args:
            order_id: Order ID
        
        Returns:
            Order dictionary or None if not found
        """
        try:
            result = self.api_client._make_request("GET", f"/v2/orders/{order_id}")
            
            if result.get("success"):
                order = result.get("result", {})
                return order
            else:
                logger.error(f"Failed to get order {order_id}: {result.get('error')}")
                return None
        
        except Exception as e:
            logger.error(f"Error getting order status: {e}")
            return None
    
    def get_positions(self, symbol: Optional[str] = None) -> List[Dict]:
        """
        Get open positions
        
        Args:
            symbol: If provided, filter by symbol
        
        Returns:
            List of position dictionaries
        """
        try:
            # Use /v2/positions/margined for all positions
            result = self.api_client._make_request("GET", "/v2/positions/margined")
            
            if result.get("success"):
                positions = result.get("result", [])
                
                # Filter by symbol if provided
                if symbol:
                    product_id = self.get_product_id(symbol)
                    positions = [p for p in positions if p.get('product_id') == product_id]
                
                # Filter out zero positions
                active_positions = [p for p in positions if float(p.get('size', 0)) != 0]
                logger.debug(f"Retrieved {len(active_positions)} active positions")
                return active_positions
            else:
                logger.error(f"Failed to get positions: {result.get('error')}")
                return []
        
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []

    def get_position(self, symbol: str) -> Optional[Dict]:
        """
        Get position for a specific symbol (real-time data)
        
        Args:
            symbol: Trading symbol
        
        Returns:
            Position dictionary or None if no position
        """
        try:
            product_id = self.get_product_id(symbol)
            params = {"product_id": product_id}
            
            result = self.api_client._make_request("GET", "/v2/positions", params=params)
            
            if result.get("success"):
                position = result.get("result", {})
                # Check if position exists and has size
                if position and float(position.get('size', 0)) != 0:
                    logger.debug(f"Retrieved position for {symbol}: {position.get('size')} contracts")
                    return position
                else:
                    logger.debug(f"No active position for {symbol}")
                    return None
            else:
                logger.error(f"Failed to get position: {result.get('error')}")
                return None
        
        except Exception as e:
            logger.error(f"Error getting position: {e}")
            return None

    
    def close_position(self, symbol: str) -> Dict:
        """
        Close entire position for a symbol using market order
        
        Args:
            symbol: Trading symbol
        
        Returns:
            Order response dictionary
        """
        try:
            position = self.get_position(symbol)
            
            if not position:
                logger.warning(f"No position to close for {symbol}")
                return {"success": False, "error": "No position"}
            
            size = abs(int(position.get('size', 0)))
            current_side = "buy" if float(position.get('size', 0)) > 0 else "sell"
            
            # Close by taking opposite side with reduce_only
            close_side = "sell" if current_side == "buy" else "buy"
            
            logger.info(f"Closing {symbol} position: {current_side.upper()} {size} contracts")
            
            return self.place_market_order(symbol, close_side, size, reduce_only=True)
        
        except Exception as e:
            logger.error(f"Error closing position: {e}")
            raise
    
    def get_account_balance(self) -> Dict:
        """
        Get account balance information
        
        Returns:
            Dict with balance, available_balance, equity, etc.
        """
        try:
            result = self.api_client._make_request("GET", "/v2/wallet/balances")
            
            if result.get("success"):
                balances = result.get("result", [])
                # Get USD balance (Delta India uses USD, not USDT)
                usd_balance = next((b for b in balances if b.get('asset_symbol') == 'USD'), None)
                
                if usd_balance:
                    logger.debug(f"Balance: ${float(usd_balance.get('balance', 0)):.2f}")
                    return usd_balance
                else:
                    logger.warning("USD balance not found")
                    # Log all available balances for debugging
                    logger.debug(f"Available balances: {[b.get('asset_symbol') for b in balances]}")
                    return {}
            else:
                logger.error(f"Failed to get balance: {result.get('error')}")
                return {}
        
        except Exception as e:
            logger.error(f"Error getting account balance: {e}")
            return {}


# Example usage and testing
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    print("\n" + "="*80)
    print("Testing Delta Trading Client")
    print("="*80 + "\n")
    
    # Load environment variables
    load_dotenv()
    
    api_key = os.getenv('DELTA_API_KEY_1')
    api_secret = os.getenv('DELTA_API_SECRET_1')
    
    if not api_key or not api_secret:
        print("Error: API credentials not found in .env file")
        print("   Set DELTA_API_KEY_1 and DELTA_API_SECRET_1")
        exit(1)
    
    try:
        # Create low-level API client
        from brokers.delta_exchange.api_client import DeltaExchangeClient
        api_client = DeltaExchangeClient(api_key, api_secret)
        
        # Create high-level trading client
        trading_client = DeltaTradingClient(api_client)
        
        print("1. Testing connection...")
        success, message = api_client.test_connection()
        print(f"   {message}\n")
        
        if not success:
            exit(1)
        
        # Test symbol lookup
        print("2. Testing symbol lookup...")
        for symbol in ['SOLUSD', 'AAVEUSD']:
            product_id = trading_client.get_product_id(symbol)
            specs = trading_client.get_symbol_specs(symbol)
            print(f"   {symbol}: product_id={product_id}, tick_size={specs['tick_size']}")
        print()
        
        # Get account balance
        print("3. Getting account balance...")
        balance = trading_client.get_account_balance()
        if balance:
            print(f"   Balance: ${float(balance.get('balance', 0)):.2f}")
            print(f"   Available: ${float(balance.get('available_balance', 0)):.2f}")
        print()
        
        # Get open positions
        print("4. Checking open positions...")
        positions = trading_client.get_positions()
        if positions:
            for pos in positions:
                symbol = pos.get('product_symbol')
                size = pos.get('size')
                entry = pos.get('entry_price')
                pnl = pos.get('unrealized_pnl')
                print(f"   {symbol}: {size} @ ${entry} (P&L: ${pnl:+.2f})")
        else:
            print("   No open positions")
        print()
        
        # Get open orders
        print("5. Checking open orders...")
        orders = trading_client.get_open_orders()
        if orders:
            for order in orders:
                symbol = order.get('product_symbol')
                side = order.get('side')
                size = order.get('size')
                price = order.get('limit_price', 'market')
                print(f"   {symbol} {side.upper()} {size} @ {price}")
        else:
            print("   No open orders")
        print()
        
        # Test order placement (COMMENTED OUT - ENABLE FOR REAL TESTING)
        # WARNING: This will place REAL orders!
        """
        print("6. Testing order placement (TESTNET)...")
        
        # Place a limit order (far from market to avoid fill)
        order = trading_client.place_limit_order(
            symbol="SOLUSD",
            side="buy",
            size=1,
            price=1.00,  # Very low price (won't fill)
            post_only=True
        )
        print(f"   Limit order placed: ID={order.get('id')}")
        
        # Wait a moment
        import time
        time.sleep(2)
        
        # Cancel the order
        order_id = order.get('id')
        cancelled = trading_client.cancel_order(order_id)
        print(f"   Order cancelled: {cancelled}")
        """
        
        print("\n" + "="*80)
        print("All tests completed!")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()