#!/usr/bin/env python3
"""
WebSocket Client - System wrapper for real-time market data

Abstracts broker-specific WebSocket implementations and provides
a consistent interface for the rest of the system.

Features:
- Wraps Delta Exchange WebSocket
- Feeds ticks to candle builder
- Manages connection lifecycle
- Easy to extend for other brokers
"""

import asyncio
from typing import Dict, List, Callable, Optional
from datetime import datetime

from brokers.delta_exchange.delta_websocket import DeltaWebSocket
from core.utils.logger import get_logger

logger = get_logger('system')


class WebSocketClient:
    """
    Generic WebSocket client for real-time market data
    
    Currently supports:
    - Delta Exchange India
    
    Easy to extend for other brokers in the future.
    """
    
    def __init__(self, broker: str = "delta_exchange_india", 
                 symbols: List[str] = None,
                 on_tick: Optional[Callable[[Dict], None]] = None):
        """
        Initialize WebSocket client
        
        Args:
            broker: Broker name (currently only 'delta_exchange_india')
            symbols: List of symbols to subscribe
            on_tick: Callback for tick updates
        """
        self.broker = broker
        self.symbols = symbols or ['SOLUSD', 'AAVEUSD']
        self.on_tick = on_tick
        self.ws_client = None
        self.is_running = False
        
        # Statistics
        self.stats = {
            'ticks_received': 0,
            'ticks_by_symbol': {},
            'started_at': None,
            'last_tick_time': None
        }
        
        logger.info(f"WebSocket client initialized")
        logger.info(f"   Broker: {broker}")
        logger.info(f"   Symbols: {symbols}")
    
    def _handle_tick(self, tick: Dict):
        """
        Internal tick handler that tracks stats and calls user callback
        
        Args:
            tick: Tick data dictionary
        """
        # Update statistics
        self.stats['ticks_received'] += 1
        self.stats['last_tick_time'] = datetime.now()
        
        symbol = tick.get('symbol', 'UNKNOWN')
        self.stats['ticks_by_symbol'][symbol] = self.stats['ticks_by_symbol'].get(symbol, 0) + 1
        
        # Log periodic stats (every 100 ticks)
        if self.stats['ticks_received'] % 100 == 0:
            logger.debug(f"📊 Stats: {self.stats['ticks_received']} ticks received")
            for sym, count in self.stats['ticks_by_symbol'].items():
                logger.debug(f"   {sym}: {count} ticks")
        
        # Call user callback
        if self.on_tick:
            try:
                self.on_tick(tick)
            except Exception as e:
                logger.error(f"Error in user tick callback: {e}")
    
    async def connect(self) -> bool:
        """
        Connect to WebSocket
        
        Returns:
            True if connected successfully
        """
        if self.broker == "delta_exchange_india":
            # Create Delta WebSocket client
            self.ws_client = DeltaWebSocket(
                symbols=self.symbols,
                on_tick=self._handle_tick
            )
            return True
        else:
            logger.error(f"Unsupported broker: {self.broker}")
            return False
    
    async def start(self):
        """Start WebSocket client and begin receiving ticks"""
        if self.is_running:
            logger.warning("WebSocket client already running")
            return
        
        logger.info("🚀 Starting WebSocket client")
        
        # Connect
        if not await self.connect():
            logger.error("Failed to connect WebSocket client")
            return
        
        self.is_running = True
        self.stats['started_at'] = datetime.now()
        
        # Start broker-specific client
        await self.ws_client.start()
        
        self.is_running = False
        logger.info("WebSocket client stopped")
    
    async def stop(self):
        """Stop WebSocket client"""
        logger.info("🛑 Stopping WebSocket client")
        
        self.is_running = False
        
        if self.ws_client:
            await self.ws_client.stop()
        
        # Log final stats
        logger.info("📊 WebSocket Statistics:")
        logger.info(f"   Total ticks: {self.stats['ticks_received']}")
        logger.info(f"   Started at: {self.stats['started_at']}")
        logger.info(f"   Last tick: {self.stats['last_tick_time']}")
        
        if self.stats['ticks_by_symbol']:
            logger.info(f"   Ticks by symbol:")
            for symbol, count in self.stats['ticks_by_symbol'].items():
                logger.info(f"      {symbol}: {count}")
        
        logger.info("✅ WebSocket client stopped")
    
    def get_stats(self) -> Dict:
        """
        Get WebSocket statistics
        
        Returns:
            Dictionary with statistics
        """
        return self.stats.copy()


# Example usage and testing
if __name__ == "__main__":
    import asyncio
    
    print("\n🧪 Testing Generic WebSocket Client\n")
    
    def on_tick_handler(tick: Dict):
        """Example tick handler"""
        print(f"📈 {tick['symbol']}: ${tick['price']:.4f} at {tick['timestamp']}")
    
    async def test_client():
        """Test WebSocket client"""
        
        # Create client
        client = WebSocketClient(
            broker="delta_exchange_india",
            symbols=['SOLUSD', 'AAVEUSD'],
            on_tick=on_tick_handler
        )
        
        print("Starting WebSocket client for 30 seconds...")
        print("Press Ctrl+C to stop early")
        print()
        
        try:
            # Run for 30 seconds
            await asyncio.wait_for(client.start(), timeout=30)
        except asyncio.TimeoutError:
            print("\n⏰ Test timeout reached")
        except KeyboardInterrupt:
            print("\n⚠️  Interrupted by user")
        finally:
            await client.stop()
            
            # Show stats
            stats = client.get_stats()
            print(f"\n📊 Final Statistics:")
            print(f"   Total ticks: {stats['ticks_received']}")
            print(f"   Duration: {stats['started_at']} to {stats['last_tick_time']}")
    
    try:
        asyncio.run(test_client())
    except KeyboardInterrupt:
        print("\nTest stopped")
    
    print("\n✅ Test complete!")