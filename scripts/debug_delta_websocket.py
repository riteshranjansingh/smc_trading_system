#!/usr/bin/env python3
"""
Debug Delta Exchange WebSocket

This script connects to Delta WebSocket and prints ALL messages
to help us understand the correct format.
"""

import asyncio
import json
import websockets

WS_URL = "wss://socket.india.delta.exchange"

async def debug_websocket():
    """Connect and print all messages"""
    
    print("=" * 80)
    print("🔍 DELTA WEBSOCKET DEBUG")
    print("=" * 80)
    print(f"\nConnecting to: {WS_URL}\n")
    
    try:
        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as websocket:
            print("✅ Connected!\n")
            
            # Try different subscription formats
            print("📡 Trying subscription format 1 (mark_price)...")
            subscribe_msg_1 = {
                "type": "subscribe",
                "payload": {
                    "channels": [
                        {
                            "name": "mark_price",
                            "symbols": ["SOLUSD"]
                        }
                    ]
                }
            }
            await websocket.send(json.dumps(subscribe_msg_1))
            print(f"Sent: {json.dumps(subscribe_msg_1, indent=2)}\n")
            
            # Try ticker channel too
            print("📡 Trying subscription format 2 (v2/ticker)...")
            subscribe_msg_2 = {
                "type": "subscribe",
                "payload": {
                    "channels": [
                        {
                            "name": "v2/ticker",
                            "symbols": ["SOLUSD"]
                        }
                    ]
                }
            }
            await websocket.send(json.dumps(subscribe_msg_2))
            print(f"Sent: {json.dumps(subscribe_msg_2, indent=2)}\n")
            
            print("=" * 80)
            print("📥 RECEIVING MESSAGES (will run for 30 seconds)")
            print("=" * 80)
            print()
            
            # Listen for 30 seconds
            message_count = 0
            
            try:
                async for message in websocket:
                    message_count += 1
                    print(f"\n--- MESSAGE {message_count} ---")
                    
                    try:
                        data = json.loads(message)
                        print(json.dumps(data, indent=2))
                    except json.JSONDecodeError:
                        print(f"Raw message: {message}")
                    
                    print("-" * 40)
                    
                    # Stop after 30 seconds or 20 messages
                    if message_count >= 20:
                        print("\n⏰ Received 20 messages, stopping...")
                        break
                    
            except asyncio.TimeoutError:
                print("\n⏰ 30 seconds timeout")
            
            print(f"\n✅ Total messages received: {message_count}")
            
            if message_count == 0:
                print("\n❌ NO MESSAGES RECEIVED!")
                print("\nPossible reasons:")
                print("   1. Wrong channel name")
                print("   2. Wrong subscription format")
                print("   3. Wrong symbol name")
                print("   4. Market closed?")
                print("\nPlease check Delta Exchange WebSocket docs:")
                print("   https://docs.delta.exchange/#introduction")
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("\nThis will connect to Delta WebSocket and show ALL messages")
    print("Press Ctrl+C to stop\n")
    
    try:
        asyncio.run(debug_websocket())
    except KeyboardInterrupt:
        print("\n\nStopped by user")
    
    print("\n" + "=" * 80)
    print("Share the output above so we can fix the WebSocket implementation!")
    print("=" * 80)