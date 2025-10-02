#!/usr/bin/env python3
"""
Order Manager - Track order lifecycle

Tracks all orders from placement to completion:
- Pending orders (limit orders waiting to fill)
- Filled orders (converted to positions)
- Cancelled orders (OB invalidated, manual cancel)
- Rejected orders (exchange rejection)

Critical for Mode B which places limit orders that may:
1. Fill when price touches entry level
2. Get cancelled if OB invalidates
3. Expire after certain time

Usage:
    manager = OrderManager()
    
    # Place order
    order_id = manager.add_order(symbol, side, size, price, ...)
    
    # Check status
    status = manager.get_order_status(order_id)
    
    # Update when filled
    manager.mark_filled(order_id, fill_price)
    
    # Cancel order
    manager.cancel_order(order_id)
"""

from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
import json
from pathlib import Path

from core.utils.logger import get_logger

logger = get_logger('system')


class OrderStatus(Enum):
    """Order status states"""
    PENDING = "pending"           # Order placed, waiting to fill
    PARTIALLY_FILLED = "partial"  # Partially filled
    FILLED = "filled"             # Fully filled
    CANCELLED = "cancelled"       # Cancelled (by user or OB invalidation)
    REJECTED = "rejected"         # Rejected by exchange
    EXPIRED = "expired"           # Expired (time-based)


class OrderType(Enum):
    """Order types"""
    MARKET = "market"
    LIMIT = "limit"


@dataclass
class Order:
    """Represents a trading order"""
    # Order identification
    order_id: int  # Exchange order ID
    internal_id: str  # Our internal tracking ID
    
    # Order details
    symbol: str
    side: str  # 'buy' or 'sell'
    order_type: OrderType
    size: int  # Number of contracts
    price: Optional[float]  # Limit price (None for market orders)
    
    # Metadata
    account: str  # Which sub-account
    ob_type: str  # 'fresh' or 'breaker'
    ob_id: str  # ID of the OB this order is for
    created_at: str
    
    # Status tracking
    status: OrderStatus = OrderStatus.PENDING
    filled_size: int = 0
    remaining_size: int = 0
    filled_price: Optional[float] = None
    filled_at: Optional[str] = None
    cancelled_at: Optional[str] = None
    cancel_reason: Optional[str] = None
    
    # Exchange info
    exchange_status: Optional[str] = None
    exchange_message: Optional[str] = None
    
    def __post_init__(self):
        """Initialize remaining size"""
        if self.remaining_size == 0:
            self.remaining_size = self.size
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        data = asdict(self)
        # Convert enums to strings
        data['order_type'] = self.order_type.value
        data['status'] = self.status.value
        return data
    
    @staticmethod
    def from_dict(data: Dict) -> 'Order':
        """Create Order from dictionary"""
        # Convert string enums back
        data['order_type'] = OrderType(data['order_type'])
        data['status'] = OrderStatus(data['status'])
        return Order(**data)


class OrderManager:
    """
    Manages order lifecycle across all symbols
    
    Tracks orders from placement to completion.
    Critical for Mode B limit order tracking.
    """
    
    def __init__(self, account_name: str):
        """
        Initialize Order Manager
        
        Args:
            account_name: Sub-account name (e.g., "account_1", "account_2")
        """
        self.account_name = account_name
        
        # Active orders (order_id -> Order)
        self.orders: Dict[int, Order] = {}
        
        # Order history (completed orders)
        self.completed_orders: List[Order] = []
        
        # Internal ID counter
        self.next_internal_id = 1
        
        # Statistics
        self.stats = {
            'total_orders': 0,
            'filled_orders': 0,
            'cancelled_orders': 0,
            'rejected_orders': 0,
            'by_symbol': {}
        }
        
        logger.info(f"Order Manager initialized: {account_name}")
    
    def _generate_internal_id(self) -> str:
        """Generate unique internal order ID"""
        internal_id = f"{self.account_name}_ORDER_{self.next_internal_id}"
        self.next_internal_id += 1
        return internal_id
    
    def add_order(self, order_id: int, symbol: str, side: str, order_type: str,
                  size: int, price: Optional[float], ob_type: str, ob_id: str) -> Order:
        """
        Add a new order to tracking
        
        Args:
            order_id: Exchange order ID
            symbol: Trading symbol
            side: 'buy' or 'sell'
            order_type: 'market' or 'limit'
            size: Number of contracts
            price: Limit price (None for market)
            ob_type: 'fresh' or 'breaker'
            ob_id: ID of the OB this order is for
        
        Returns:
            Order object
        """
        # Create order
        order = Order(
            order_id=order_id,
            internal_id=self._generate_internal_id(),
            symbol=symbol,
            side=side,
            order_type=OrderType(order_type.lower()),
            size=size,
            price=price,
            account=self.account_name,
            ob_type=ob_type,
            ob_id=ob_id,
            created_at=datetime.now().isoformat()
        )
        
        # Store order
        self.orders[order_id] = order
        
        # Update stats
        self.stats['total_orders'] += 1
        if symbol not in self.stats['by_symbol']:
            self.stats['by_symbol'][symbol] = {
                'orders': 0,
                'filled': 0,
                'cancelled': 0
            }
        self.stats['by_symbol'][symbol]['orders'] += 1
        
        logger.info(f"Order added: {order.internal_id}")
        logger.info(f"   {symbol} {side.upper()} {size} @ {price or 'MARKET'}")
        logger.info(f"   Exchange ID: {order_id}")
        
        return order
    
    def get_order(self, order_id: int) -> Optional[Order]:
        """Get order by exchange ID"""
        return self.orders.get(order_id)
    
    def get_orders_by_symbol(self, symbol: str, status: Optional[OrderStatus] = None) -> List[Order]:
        """
        Get orders for a symbol
        
        Args:
            symbol: Trading symbol
            status: Optional status filter
        
        Returns:
            List of orders
        """
        orders = [o for o in self.orders.values() if o.symbol == symbol]
        
        if status:
            orders = [o for o in orders if o.status == status]
        
        return orders
    
    def get_pending_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """
        Get all pending orders
        
        Args:
            symbol: Optional symbol filter
        
        Returns:
            List of pending orders
        """
        pending = [o for o in self.orders.values() if o.status == OrderStatus.PENDING]
        
        if symbol:
            pending = [o for o in pending if o.symbol == symbol]
        
        return pending
    
    def get_orders_by_ob(self, ob_id: str) -> List[Order]:
        """
        Get all orders for a specific OB
        
        Args:
            ob_id: Order Block ID
        
        Returns:
            List of orders
        """
        return [o for o in self.orders.values() if o.ob_id == ob_id]
    
    def mark_filled(self, order_id: int, fill_price: float, fill_size: Optional[int] = None) -> Order:
        """
        Mark order as filled (fully or partially)
        
        Args:
            order_id: Exchange order ID
            fill_price: Fill price
            fill_size: Number of contracts filled (None = full fill)
        
        Returns:
            Updated order
        """
        order = self.get_order(order_id)
        if not order:
            logger.error(f"Order not found: {order_id}")
            return None
        
        # Determine fill size
        if fill_size is None:
            fill_size = order.remaining_size
        
        # Update order
        order.filled_size += fill_size
        order.remaining_size -= fill_size
        order.filled_price = fill_price
        order.filled_at = datetime.now().isoformat()
        
        # Update status
        if order.remaining_size == 0:
            order.status = OrderStatus.FILLED
            logger.info(f"Order FILLED: {order.internal_id}")
        else:
            order.status = OrderStatus.PARTIALLY_FILLED
            logger.info(f"Order PARTIALLY FILLED: {order.internal_id}")
        
        logger.info(f"   Filled: {fill_size}/{order.size} @ ${fill_price:.4f}")
        
        # Move to completed if fully filled
        if order.status == OrderStatus.FILLED:
            self._complete_order(order)
        
        # Update stats
        self.stats['filled_orders'] += 1
        self.stats['by_symbol'][order.symbol]['filled'] += 1
        
        return order
    
    def cancel_order(self, order_id: int, reason: str = "manual") -> bool:
        """
        Cancel an order
        
        Args:
            order_id: Exchange order ID
            reason: Cancellation reason (e.g., 'ob_invalidated', 'manual')
        
        Returns:
            True if cancelled successfully
        """
        order = self.get_order(order_id)
        if not order:
            logger.error(f"Order not found: {order_id}")
            return False
        
        # Update order
        order.status = OrderStatus.CANCELLED
        order.cancelled_at = datetime.now().isoformat()
        order.cancel_reason = reason
        
        logger.info(f"Order CANCELLED: {order.internal_id}")
        logger.info(f"   Reason: {reason}")
        
        # Move to completed
        self._complete_order(order)
        
        # Update stats
        self.stats['cancelled_orders'] += 1
        self.stats['by_symbol'][order.symbol]['cancelled'] += 1
        
        return True
    
    def cancel_orders_by_ob(self, ob_id: str, reason: str = "ob_invalidated") -> int:
        """
        Cancel all orders for a specific OB
        
        Args:
            ob_id: Order Block ID
            reason: Cancellation reason
        
        Returns:
            Number of orders cancelled
        """
        orders = self.get_orders_by_ob(ob_id)
        pending_orders = [o for o in orders if o.status == OrderStatus.PENDING]
        
        cancelled_count = 0
        for order in pending_orders:
            if self.cancel_order(order.order_id, reason):
                cancelled_count += 1
        
        logger.info(f"Cancelled {cancelled_count} orders for OB {ob_id}")
        
        return cancelled_count
    
    def cancel_orders_by_symbol(self, symbol: str, reason: str = "manual") -> int:
        """
        Cancel all pending orders for a symbol
        
        Args:
            symbol: Trading symbol
            reason: Cancellation reason
        
        Returns:
            Number of orders cancelled
        """
        pending_orders = self.get_pending_orders(symbol)
        
        cancelled_count = 0
        for order in pending_orders:
            if self.cancel_order(order.order_id, reason):
                cancelled_count += 1
        
        logger.info(f"Cancelled {cancelled_count} orders for {symbol}")
        
        return cancelled_count
    
    def mark_rejected(self, order_id: int, rejection_reason: str) -> Order:
        """
        Mark order as rejected by exchange
        
        Args:
            order_id: Exchange order ID
            rejection_reason: Reason for rejection
        
        Returns:
            Updated order
        """
        order = self.get_order(order_id)
        if not order:
            logger.error(f"Order not found: {order_id}")
            return None
        
        # Update order
        order.status = OrderStatus.REJECTED
        order.exchange_message = rejection_reason
        
        logger.warning(f"Order REJECTED: {order.internal_id}")
        logger.warning(f"   Reason: {rejection_reason}")
        
        # Move to completed
        self._complete_order(order)
        
        # Update stats
        self.stats['rejected_orders'] += 1
        
        return order
    
    def _complete_order(self, order: Order):
        """
        Move order to completed (internal)
        
        Args:
            order: Order to complete
        """
        # Remove from active orders
        if order.order_id in self.orders:
            del self.orders[order.order_id]
        
        # Add to completed orders
        self.completed_orders.append(order)
    
    def update_from_exchange(self, order_id: int, exchange_status: str, 
                            exchange_data: Dict) -> Order:
        """
        Update order status from exchange data
        
        Args:
            order_id: Exchange order ID
            exchange_status: Status from exchange (e.g., 'open', 'filled', 'cancelled')
            exchange_data: Full order data from exchange
        
        Returns:
            Updated order
        """
        order = self.get_order(order_id)
        if not order:
            logger.warning(f"Order not found: {order_id}")
            return None
        
        # Update exchange status
        order.exchange_status = exchange_status
        
        # Map exchange status to our status
        if exchange_status in ['filled', 'closed']:
            fill_price = float(exchange_data.get('average_fill_price', exchange_data.get('limit_price', 0)))
            filled_size = int(exchange_data.get('size', order.size))
            self.mark_filled(order_id, fill_price, filled_size)
        
        elif exchange_status == 'cancelled':
            self.cancel_order(order_id, reason="exchange_cancelled")
        
        elif exchange_status == 'rejected':
            reason = exchange_data.get('reject_reason', 'Unknown')
            self.mark_rejected(order_id, reason)
        
        return order
    
    def get_statistics(self) -> Dict:
        """Get order statistics"""
        active_count = len(self.orders)
        completed_count = len(self.completed_orders)
        
        return {
            'account': self.account_name,
            'total_orders': self.stats['total_orders'],
            'active_orders': active_count,
            'completed_orders': completed_count,
            'filled_orders': self.stats['filled_orders'],
            'cancelled_orders': self.stats['cancelled_orders'],
            'rejected_orders': self.stats['rejected_orders'],
            'fill_rate': (self.stats['filled_orders'] / self.stats['total_orders'] * 100) 
                        if self.stats['total_orders'] > 0 else 0,
            'by_symbol': self.stats['by_symbol'].copy()
        }
    
    def save_state(self):
        """Save order state to disk"""
        state = {
            'account_name': self.account_name,
            'orders': {oid: order.to_dict() for oid, order in self.orders.items()},
            'completed_orders': [order.to_dict() for order in self.completed_orders],
            'stats': self.stats,
            'next_internal_id': self.next_internal_id,
            'timestamp': datetime.now().isoformat()
        }
        
        # Save to file
        filename = f"orders_{self.account_name}.json"
        filepath = Path("data") / self.account_name / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(state, f, indent=2)
        
        logger.debug(f"Saved order state: {filepath}")
    
    def load_state(self) -> bool:
        """Load order state from disk"""
        filename = f"orders_{self.account_name}.json"
        filepath = Path("data") / self.account_name / filename
        
        if not filepath.exists():
            logger.warning(f"No saved state found: {filepath}")
            return False
        
        try:
            with open(filepath, 'r') as f:
                state = json.load(f)
            
            # Restore orders
            orders_data = state.get('orders', {})
            self.orders = {
                int(oid): Order.from_dict(order_data)
                for oid, order_data in orders_data.items()
            }
            
            # Restore completed orders
            completed_data = state.get('completed_orders', [])
            self.completed_orders = [Order.from_dict(o) for o in completed_data]
            
            # Restore stats
            self.stats = state.get('stats', self.stats)
            self.next_internal_id = state.get('next_internal_id', self.next_internal_id)
            
            logger.info(f"Loaded order state: {filepath}")
            logger.info(f"   Active orders: {len(self.orders)}")
            logger.info(f"   Completed orders: {len(self.completed_orders)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            return False


# Example usage and testing
if __name__ == "__main__":
    print("\n" + "="*80)
    print("Testing Order Manager")
    print("="*80 + "\n")
    
    # Create order manager
    manager = OrderManager("account_2")  # Mode B uses limit orders
    
    print("1. Adding Orders:")
    
    # Add limit order
    order1 = manager.add_order(
        order_id=12345678,
        symbol="SOLUSD",
        side="buy",
        order_type="limit",
        size=10,
        price=150.50,
        ob_type="fresh",
        ob_id="OB_SOLUSD_1"
    )
    print(f"   Order 1: {order1.internal_id}")
    
    # Add another order
    order2 = manager.add_order(
        order_id=12345679,
        symbol="AAVEUSD",
        side="sell",
        order_type="limit",
        size=5,
        price=200.00,
        ob_type="breaker",
        ob_id="OB_AAVEUSD_1"
    )
    print(f"   Order 2: {order2.internal_id}")
    print()
    
    # Check pending orders
    print("2. Checking Pending Orders:")
    pending = manager.get_pending_orders()
    print(f"   Total pending: {len(pending)}")
    for order in pending:
        print(f"   - {order.symbol} {order.side} {order.size} @ ${order.price}")
    print()
    
    # Mark order as filled
    print("3. Filling Order:")
    manager.mark_filled(
        order_id=12345678,
        fill_price=150.50
    )
    
    filled_order = manager.get_order(12345678)
    if not filled_order:  # Order moved to completed
        print("   Order moved to completed orders")
    print()
    
    # Cancel order
    print("4. Cancelling Order:")
    manager.cancel_order(
        order_id=12345679,
        reason="ob_invalidated"
    )
    print()
    
    # Check active orders
    print("5. Active Orders:")
    pending = manager.get_pending_orders()
    print(f"   Pending orders: {len(pending)}")
    print()
    
    # Statistics
    print("6. Statistics:")
    stats = manager.get_statistics()
    print(f"   Total orders: {stats['total_orders']}")
    print(f"   Filled: {stats['filled_orders']}")
    print(f"   Cancelled: {stats['cancelled_orders']}")
    print(f"   Fill rate: {stats['fill_rate']:.1f}%")
    print()
    
    # Save/Load state
    print("7. Save/Load State:")
    manager.save_state()
    print("   State saved")
    
    manager2 = OrderManager("account_2")
    loaded = manager2.load_state()
    print(f"   State loaded: {loaded}")
    stats2 = manager2.get_statistics()
    print(f"   Orders after load: {stats2['total_orders']}")
    print()
    
    print("="*80)
    print("All tests completed!")
    print("="*80 + "\n")