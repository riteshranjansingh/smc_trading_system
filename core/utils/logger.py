"""
SMC Trading System - Centralized Logging Module

Creates separate log files for:
- trades.log: All trade entries, exits, P&L
- ob_events.log: Order block creation, invalidation
- system.log: General system events, startup, shutdown
- errors.log: Errors and exceptions only

All logs also print to console with color coding.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for console output"""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def format(self, record):
        # Add color to level name
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{self.COLORS['RESET']}"
        return super().format(record)


class SMCLogger:
    """
    Centralized logger for the SMC Trading System
    
    Usage:
        logger = SMCLogger.get_logger("trades")
        logger.info("Entered LONG SOLUSD @ 150.25")
    """
    
    _loggers = {}
    _initialized = False
    
    @classmethod
    def initialize(cls, log_dir: str = "logs"):
        """
        Initialize logging system (call once at startup)
        
        Args:
            log_dir: Directory to store log files
        """
        if cls._initialized:
            return
        
        # Create logs directory
        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True)
        
        # Define log files
        cls.log_files = {
            'trades': log_path / 'trades.log',
            'ob_events': log_path / 'ob_events.log',
            'system': log_path / 'system.log',
            'errors': log_path / 'errors.log',
        }
        
        cls._initialized = True
        
        # Create system logger and log initialization
        system_logger = cls.get_logger('system')
        system_logger.info("=" * 80)
        system_logger.info(f"SMC Trading System Started - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        system_logger.info("=" * 80)
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Get or create a logger with the given name
        
        Args:
            name: Logger name ('trades', 'ob_events', 'system', 'errors')
        
        Returns:
            Configured logger instance
        """
        if not cls._initialized:
            cls.initialize()
        
        if name in cls._loggers:
            return cls._loggers[name]
        
        # Create logger
        logger = logging.getLogger(f"smc.{name}")
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()  # Remove any existing handlers
        
        # File handler - detailed logs
        if name in cls.log_files:
            file_handler = logging.FileHandler(cls.log_files[name])
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                '%(asctime)s | %(levelname)-8s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        
        # Console handler - with colors
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = ColoredFormatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # Prevent propagation to root logger
        logger.propagate = False
        
        cls._loggers[name] = logger
        return logger
    
    @classmethod
    def log_trade(cls, action: str, symbol: str, direction: str, 
                  price: float, size: float, **kwargs):
        """
        Convenience method for logging trades
        
        Args:
            action: 'ENTRY', 'EXIT', 'PARTIAL_EXIT'
            symbol: 'SOLUSD', 'AAVEUSD'
            direction: 'LONG', 'SHORT'
            price: Entry/exit price
            size: Position size (contracts)
            **kwargs: Additional info (pnl, reason, etc.)
        """
        logger = cls.get_logger('trades')
        
        msg = f"{action} | {symbol} | {direction} | Price: ${price:.4f} | Size: {size:.2f}"
        
        if 'pnl' in kwargs:
            msg += f" | P&L: ${kwargs['pnl']:+.2f}"
        
        if 'reason' in kwargs:
            msg += f" | Reason: {kwargs['reason']}"
        
        if 'ob_type' in kwargs:
            msg += f" | OB: {kwargs['ob_type']}"
        
        logger.info(msg)
    
    @classmethod
    def log_ob_event(cls, event: str, symbol: str, ob_type: str, 
                     top: float, bottom: float, **kwargs):
        """
        Convenience method for logging Order Block events
        
        Args:
            event: 'CREATED', 'INVALIDATED', 'BREAKER'
            symbol: 'SOLUSD', 'AAVEUSD'
            ob_type: 'fresh', 'breaker'
            top: OB top price
            bottom: OB bottom price
            **kwargs: Additional info
        """
        logger = cls.get_logger('ob_events')
        
        msg = f"{event} | {symbol} | {ob_type.upper()} | Top: ${top:.4f} | Bottom: ${bottom:.4f}"
        
        if 'bar_index' in kwargs:
            msg += f" | Bar: {kwargs['bar_index']}"
        
        if 'direction' in kwargs:
            msg += f" | Direction: {kwargs['direction']}"
        
        logger.info(msg)
    
    @classmethod
    def shutdown(cls):
        """Clean shutdown of logging system"""
        system_logger = cls.get_logger('system')
        system_logger.info("=" * 80)
        system_logger.info(f"SMC Trading System Stopped - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        system_logger.info("=" * 80)
        
        # Close all handlers
        for logger in cls._loggers.values():
            for handler in logger.handlers:
                handler.close()


# Convenience function for getting loggers
def get_logger(name: str = 'system') -> logging.Logger:
    """
    Get a logger instance
    
    Args:
        name: Logger name ('trades', 'ob_events', 'system', 'errors')
    
    Returns:
        Configured logger
    
    Example:
        from core.utils.logger import get_logger
        logger = get_logger('system')
        logger.info("Application started")
    """
    return SMCLogger.get_logger(name)


# Initialize on module import
SMCLogger.initialize()


if __name__ == "__main__":
    # Test the logger
    print("\n🧪 Testing SMC Logger...\n")
    
    # Test different loggers
    system_logger = get_logger('system')
    trade_logger = get_logger('trades')
    ob_logger = get_logger('ob_events')
    error_logger = get_logger('errors')
    
    # Test different log levels
    system_logger.debug("This is a debug message")
    system_logger.info("System initialized successfully")
    system_logger.warning("This is a warning")
    system_logger.error("This is an error")
    
    # Test trade logging
    SMCLogger.log_trade(
        action="ENTRY",
        symbol="SOLUSD",
        direction="LONG",
        price=150.25,
        size=2.0,
        ob_type="fresh"
    )
    
    SMCLogger.log_trade(
        action="EXIT",
        symbol="SOLUSD",
        direction="LONG",
        price=155.50,
        size=2.0,
        pnl=10.50,
        reason="take_profit"
    )
    
    # Test OB event logging
    SMCLogger.log_ob_event(
        event="CREATED",
        symbol="SOLUSD",
        ob_type="fresh",
        top=150.00,
        bottom=148.50,
        bar_index=1234,
        direction="bullish"
    )
    
    print("\n✅ Logger test complete! Check logs/ directory for output files.\n")
    
    SMCLogger.shutdown()