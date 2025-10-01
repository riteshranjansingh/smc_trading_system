#!/usr/bin/env python3
"""
Delta Exchange WebSocket Client

Connects to Delta Exchange India WebSocket API for real-time market data.

Features:
- Subscribe to tick data for multiple symbols
- Auto-reconnect on disconnect
- Message parsing and validation
- Event callbacks for tick updates

Delta WebSocket Documentation:
https://docs.india.delta.exchange/
"""

import asyncio
import json
import time
from typing import Dict, List, Callable, Optional
from datetime import datetime
import websockets
from websockets.exceptions import ConnectionClosed

from core.utils.logger import get_logger

logger = get_logger('system')


class DeltaWebSocket:
    """
    WebSocket client for Delta Exchange India
    """
    
    # Delta Exchange India WebSocket URL
    WS_URL = "wss://socket.india.delta.exchange"
    
    def __init__(self, symbols: List[str], on_tick: Callable[[Dict], None]):
        """
        Initialize Delta WebSocket client
        
        Args:
            symbols: List of symbols to subscribe (e.g., ['SOLUSD', 'AAVEUSD'])
            on_tick: Callback function for tick updates
        """
        self.symbols = symbols
        self.on_tick = on_tick
        self.websocket = None
        self.is_running = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5  # seconds
        
        # Track connection state
        self.connected = False
        self.subscribed = False
        self.last_message_time = None
        
        logger.info(f"Delta WebSocket initialized for symbols: {symbols}")
    
    async def connect(self):
        """Connect to Delta Exchange WebSocket"""
        try:
            logger.info(f"🔌 Connecting to Delta WebSocket: {self.WS_URL}")
            
            self.websocket = await websockets.connect(
                self.WS_URL,
                ping_interval=20,  # Send ping every 20s
                ping_timeout=10,   # Wait 10s for pong
                close_timeout=10
            )
            
            self.connected = True
            self.reconnect_attempts = 0
            
            logger.info(f"✅ Connected to Delta WebSocket")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to WebSocket: {e}")
            self.connected = False
            return False
    
    async def subscribe(self):
        """Subscribe to tick data for configured symbols"""
        if not self.connected or not self.websocket:
            logger.error("Cannot subscribe - not connected")
            return False
        
        try:
            # Delta Exchange subscription format
            # Subscribe to mark_price channel for real-time price updates
            for symbol in self.symbols:
                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {
                                "name": "mark_price",
                                "symbols": [symbol]
                            }
                        ]
                    }
                }
                
                await self.websocket.send(json.dumps(subscribe_msg))
                logger.info(f"📡 Subscribed to {symbol}")
            
            self.subscribed = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to subscribe: {e}")
            self.subscribed = False
            return False
    
    async def _handle_message(self, message: str):
        """
        Handle incoming WebSocket message
        
        Args:
            message: Raw message string from WebSocket
        """
        try:
            data = json.loads(message)
            
            # Update last message time
            self.last_message_time = time.time()
            
            # Handle different message types
            msg_type = data.get('type', '')
            
            if msg_type == 'subscriptions':
                # Subscription confirmation
                logger.debug(f"Subscription confirmed: {data}")
                
            elif msg_type == 'mark_price':
                # Price update - this is what we want!
                self._process_tick(data)
                
            elif msg_type == 'error':
                # Error message
                logger.error(f"WebSocket error: {data.get('message', 'Unknown error')}")
                
            else:
                # Unknown message type - log for debugging
                logger.debug(f"Unknown message type: {msg_type}")
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
            logger.debug(f"Raw message: {message}")
        
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    def _process_tick(self, data: Dict):
        """
        Process tick data and call callback
        
        Args:
            data: Parsed tick data from WebSocket
        """
        try:
            # Delta sends data at root level, not nested in 'payload'
            # Two message types: 'mark_price' and 'v2/ticker'
            
            symbol = data.get('symbol', '')
            timestamp = data.get('timestamp')
            
            # Get price based on message type
            if 'price' in data:
                # mark_price message
                price = data.get('price')
            elif 'mark_price' in data:
                # v2/ticker message
                price = data.get('mark_price')
            else:
                logger.debug(f"No price in message: {data.get('type', 'unknown')}")
                return
            
            if not all([symbol, price, timestamp]):
                logger.debug(f"Incomplete tick data - symbol:{symbol}, price:{price}, timestamp:{timestamp}")
                return
            
            # Convert timestamp from microseconds to seconds
            timestamp_seconds = timestamp / 1_000_000
            
            # Create standardized tick format
            tick = {
                'symbol': symbol,
                'price': float(price),
                'timestamp': int(timestamp_seconds),  # Unix timestamp in seconds
                'received_at': datetime.now().isoformat()
            }
            
            # Call user callback
            if self.on_tick:
                self.on_tick(tick)
        
        except Exception as e:
            logger.error(f"Error processing tick: {e}")
            logger.debug(f"Problematic data: {data}")
    
    async def _listen(self):
        """Listen for incoming messages"""
        try:
            async for message in self.websocket:
                await self._handle_message(message)
        
        except ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}")
            self.connected = False
            self.subscribed = False
        
        except Exception as e:
            logger.error(f"Error in listen loop: {e}")
            self.connected = False
            self.subscribed = False
    
    async def _reconnect_loop(self):
        """Reconnection logic with exponential backoff"""
        while self.is_running:
            if not self.connected:
                self.reconnect_attempts += 1
                
                if self.reconnect_attempts > self.max_reconnect_attempts:
                    logger.error(f"❌ Max reconnection attempts reached ({self.max_reconnect_attempts})")
                    logger.error(f"   Stopping WebSocket client")
                    self.is_running = False
                    break
                
                # Exponential backoff: 5s, 10s, 20s, 40s, ...
                delay = self.reconnect_delay * (2 ** (self.reconnect_attempts - 1))
                delay = min(delay, 300)  # Max 5 minutes
                
                logger.info(f"🔄 Reconnection attempt {self.reconnect_attempts}/{self.max_reconnect_attempts}")
                logger.info(f"   Waiting {delay}s before retry...")
                
                await asyncio.sleep(delay)
                
                # Try to reconnect
                if await self.connect():
                    if await self.subscribe():
                        logger.info(f"✅ Reconnected and resubscribed successfully")
                        # Reset reconnect counter on success
                        self.reconnect_attempts = 0
            else:
                # Check if connection is still alive
                if self.last_message_time:
                    time_since_last_msg = time.time() - self.last_message_time
                    
                    # If no message for 60 seconds, assume disconnected
                    if time_since_last_msg > 60:
                        logger.warning(f"⚠️  No messages for {time_since_last_msg:.0f}s - assuming disconnected")
                        self.connected = False
                        self.subscribed = False
                        
                        if self.websocket:
                            try:
                                await self.websocket.close()
                            except:
                                pass
            
            await asyncio.sleep(5)  # Check every 5 seconds
    
    async def start(self):
        """Start WebSocket client"""
        if self.is_running:
            logger.warning("WebSocket client already running")
            return
        
        logger.info("🚀 Starting Delta WebSocket client")
        
        self.is_running = True
        
        # Connect and subscribe
        if not await self.connect():
            logger.error("Failed to connect on startup")
            self.is_running = False
            return
        
        if not await self.subscribe():
            logger.error("Failed to subscribe on startup")
            self.is_running = False
            return
        
        # Start tasks
        listen_task = asyncio.create_task(self._listen())
        reconnect_task = asyncio.create_task(self._reconnect_loop())
        
        logger.info("✅ WebSocket client started")
        
        # Wait for tasks
        try:
            await asyncio.gather(listen_task, reconnect_task)
        except asyncio.CancelledError:
            logger.info("WebSocket tasks cancelled")
        except Exception as e:
            logger.error(f"Error in WebSocket tasks: {e}")
    
    async def stop(self):
        """Stop WebSocket client"""
        logger.info("🛑 Stopping Delta WebSocket client")
        
        self.is_running = False
        self.connected = False
        self.subscribed = False
        
        if self.websocket:
            try:
                await self.websocket.close()
                logger.info("✅ WebSocket connection closed")
            except Exception as e:
                logger.error(f"Error closing WebSocket: {e}")
        
        logger.info("✅ WebSocket client stopped")


# Example usage and testing
if __name__ == "__main__":
    import asyncio
    
    print("\n🧪 Testing Delta WebSocket Client\n")
    
    # Track received ticks
    tick_count = 0
    
    def on_tick_received(tick: Dict):
        """Callback for tick updates"""
        global tick_count
        tick_count += 1
        
        print(f"🔔 Tick #{tick_count}: {tick['symbol']} @ ${tick['price']:.4f}")
        print(f"   Timestamp: {tick['timestamp']}")
        print(f"   Received at: {tick['received_at']}")
        print()
    
    async def test_websocket():
        """Test WebSocket connection"""
        
        # Create WebSocket client
        symbols = ['SOLUSD', 'AAVEUSD']
        ws = DeltaWebSocket(symbols=symbols, on_tick=on_tick_received)
        
        # Start WebSocket
        print("Starting WebSocket client...")
        print("Press Ctrl+C to stop")
        print()
        
        try:
            # Run for 60 seconds or until Ctrl+C
            await asyncio.wait_for(ws.start(), timeout=60)
        except asyncio.TimeoutError:
            print("\n⏰ Test timeout reached (60 seconds)")
        except KeyboardInterrupt:
            print("\n⚠️  Interrupted by user")
        finally:
            await ws.stop()
            print(f"\n📊 Total ticks received: {tick_count}")
    
    # Run test
    try:
        asyncio.run(test_websocket())
    except KeyboardInterrupt:
        print("\nTest stopped")
    
    print("\n✅ WebSocket test complete!")