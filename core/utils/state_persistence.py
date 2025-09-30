"""
SMC Trading System - State Persistence Module

Handles saving and loading of:
- Order Blocks (active OBs, creation times, invalidation status)
- Positions (current open positions for both sub-accounts)
- Capital (track capital changes for both accounts)

Uses atomic writes to prevent corruption during crashes.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import tempfile
import shutil
from dataclasses import dataclass, asdict

from core.utils.logger import get_logger

logger = get_logger('system')


class StatePersistence:
    """
    Handles saving and loading system state to/from JSON files
    
    Thread-safe atomic writes prevent corruption during crashes.
    """
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize state persistence
        
        Args:
            data_dir: Directory to store state files
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Define state file paths
        self.ob_state_file = self.data_dir / "ob_state.json"
        self.positions_file = self.data_dir / "positions.json"
        self.capital_file = self.data_dir / "capital.json"
        
        logger.info(f"State persistence initialized: {self.data_dir}")
    
    def _atomic_write(self, filepath: Path, data: Dict) -> bool:
        """
        Atomic write to prevent corruption
        
        Process:
        1. Write to temporary file
        2. Flush to disk
        3. Rename (atomic operation on most systems)
        
        Args:
            filepath: Target file path
            data: Data to write
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create temp file in same directory (for atomic rename)
            temp_fd, temp_path = tempfile.mkstemp(
                dir=filepath.parent,
                prefix=f".{filepath.name}.",
                suffix=".tmp"
            )
            
            # Write to temp file
            with os.fdopen(temp_fd, 'w') as f:
                json.dump(data, f, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
            
            # Atomic rename
            shutil.move(temp_path, filepath)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to write state to {filepath}: {e}")
            # Clean up temp file if it exists
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return False
    
    def _safe_load(self, filepath: Path) -> Optional[Dict]:
        """
        Safely load JSON file
        
        Args:
            filepath: File to load
        
        Returns:
            Loaded data or None if file doesn't exist/is corrupt
        """
        if not filepath.exists():
            logger.debug(f"State file does not exist: {filepath}")
            return None
        
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            logger.debug(f"Loaded state from {filepath}")
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"Corrupt state file {filepath}: {e}")
            # Backup corrupt file
            backup_path = filepath.with_suffix(f".corrupt.{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            shutil.copy(filepath, backup_path)
            logger.warning(f"Backed up corrupt file to {backup_path}")
            return None
        
        except Exception as e:
            logger.error(f"Failed to load state from {filepath}: {e}")
            return None
    
    # ===== ORDER BLOCKS STATE =====
    
    def save_ob_state(self, obs_by_symbol: Dict[str, Dict]) -> bool:
        """
        Save Order Block state
        
        Args:
            obs_by_symbol: Dictionary like:
                {
                    'SOLUSD': {
                        'bullish': [
                            {
                                'top': 150.0,
                                'bottom': 148.5,
                                'creation_bar': 1234,
                                'creation_time': '2025-10-01 12:00:00',
                                'ob_type': 'fresh',
                                'is_breaker': False,
                                'invalidated': False
                            }
                        ],
                        'bearish': [...]
                    },
                    'AAVEUSD': {...}
                }
        
        Returns:
            True if successful
        """
        state = {
            'timestamp': datetime.now().isoformat(),
            'ob_state': obs_by_symbol
        }
        
        success = self._atomic_write(self.ob_state_file, state)
        
        if success:
            total_obs = sum(
                len(obs['bullish']) + len(obs['bearish']) 
                for obs in obs_by_symbol.values()
            )
            logger.debug(f"Saved OB state: {total_obs} active OBs across {len(obs_by_symbol)} symbols")
        
        return success
    
    def load_ob_state(self) -> Optional[Dict[str, Dict]]:
        """
        Load Order Block state
        
        Returns:
            OB state dictionary or None if not found
        """
        state = self._safe_load(self.ob_state_file)
        
        if state and 'ob_state' in state:
            logger.info(f"Loaded OB state from {state['timestamp']}")
            return state['ob_state']
        
        return None
    
    # ===== POSITIONS STATE =====
    
    def save_positions(self, positions: Dict[str, List[Dict]]) -> bool:
        """
        Save current positions for both sub-accounts
        
        Args:
            positions: Dictionary like:
                {
                    'account_1': [
                        {
                            'symbol': 'SOLUSD',
                            'direction': 'LONG',
                            'entry_price': 150.25,
                            'size': 2.0,
                            'entry_time': '2025-10-01 12:00:00',
                            'ob_type': 'fresh',
                            'stop_loss': 148.0,
                            'take_profit': 155.0
                        }
                    ],
                    'account_2': [...]
                }
        
        Returns:
            True if successful
        """
        state = {
            'timestamp': datetime.now().isoformat(),
            'positions': positions
        }
        
        success = self._atomic_write(self.positions_file, state)
        
        if success:
            total_positions = sum(len(pos) for pos in positions.values())
            logger.debug(f"Saved positions: {total_positions} open positions")
        
        return success
    
    def load_positions(self) -> Optional[Dict[str, List[Dict]]]:
        """
        Load current positions
        
        Returns:
            Positions dictionary or None if not found
        """
        state = self._safe_load(self.positions_file)
        
        if state and 'positions' in state:
            logger.info(f"Loaded positions from {state['timestamp']}")
            return state['positions']
        
        return None
    
    # ===== CAPITAL STATE =====
    
    def save_capital(self, capital: Dict[str, float]) -> bool:
        """
        Save capital for both sub-accounts
        
        Args:
            capital: Dictionary like:
                {
                    'account_1': 1050.25,
                    'account_2': 980.50,
                    'total': 2030.75
                }
        
        Returns:
            True if successful
        """
        state = {
            'timestamp': datetime.now().isoformat(),
            'capital': capital
        }
        
        success = self._atomic_write(self.capital_file, state)
        
        if success:
            logger.debug(f"Saved capital: Account 1: ${capital.get('account_1', 0):.2f}, "
                        f"Account 2: ${capital.get('account_2', 0):.2f}")
        
        return success
    
    def load_capital(self) -> Optional[Dict[str, float]]:
        """
        Load capital state
        
        Returns:
            Capital dictionary or None if not found
        """
        state = self._safe_load(self.capital_file)
        
        if state and 'capital' in state:
            logger.info(f"Loaded capital from {state['timestamp']}")
            return state['capital']
        
        return None
    
    # ===== UTILITY METHODS =====
    
    def clear_all_state(self):
        """Clear all state files (useful for reset/testing)"""
        files_removed = 0
        
        for filepath in [self.ob_state_file, self.positions_file, self.capital_file]:
            if filepath.exists():
                filepath.unlink()
                files_removed += 1
                logger.info(f"Removed state file: {filepath}")
        
        logger.warning(f"Cleared all state: {files_removed} files removed")
    
    def backup_state(self, backup_dir: str = "data/backups") -> bool:
        """
        Create backup of all state files
        
        Args:
            backup_dir: Directory to store backups
        
        Returns:
            True if successful
        """
        backup_path = Path(backup_dir)
        backup_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            for filepath in [self.ob_state_file, self.positions_file, self.capital_file]:
                if filepath.exists():
                    backup_file = backup_path / f"{filepath.stem}_{timestamp}{filepath.suffix}"
                    shutil.copy(filepath, backup_file)
                    logger.debug(f"Backed up {filepath.name} to {backup_file}")
            
            logger.info(f"State backup created: {backup_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to backup state: {e}")
            return False
    
    def get_state_info(self) -> Dict[str, Any]:
        """
        Get information about current state files
        
        Returns:
            Dictionary with file sizes and timestamps
        """
        info = {}
        
        for name, filepath in [
            ('ob_state', self.ob_state_file),
            ('positions', self.positions_file),
            ('capital', self.capital_file)
        ]:
            if filepath.exists():
                stat = filepath.stat()
                info[name] = {
                    'exists': True,
                    'size_bytes': stat.st_size,
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                }
            else:
                info[name] = {'exists': False}
        
        return info


# Singleton instance
_state_manager = None

def get_state_manager() -> StatePersistence:
    """
    Get singleton state manager instance
    
    Returns:
        StatePersistence instance
    
    Example:
        from core.utils.state_persistence import get_state_manager
        
        state = get_state_manager()
        state.save_ob_state(obs)
        obs = state.load_ob_state()
    """
    global _state_manager
    if _state_manager is None:
        _state_manager = StatePersistence()
    return _state_manager


if __name__ == "__main__":
    # Test the state persistence
    print("\n🧪 Testing State Persistence...\n")
    
    state = StatePersistence(data_dir="data/test")
    
    # Test OB state
    print("1. Testing OB state...")
    test_obs = {
        'SOLUSD': {
            'bullish': [
                {
                    'top': 150.0,
                    'bottom': 148.5,
                    'creation_bar': 1234,
                    'creation_time': '2025-10-01 12:00:00',
                    'ob_type': 'fresh',
                    'is_breaker': False
                }
            ],
            'bearish': []
        }
    }
    
    state.save_ob_state(test_obs)
    loaded_obs = state.load_ob_state()
    assert loaded_obs == test_obs, "OB state mismatch!"
    print("   ✅ OB state save/load works")
    
    # Test positions
    print("2. Testing positions...")
    test_positions = {
        'account_1': [
            {
                'symbol': 'SOLUSD',
                'direction': 'LONG',
                'entry_price': 150.25,
                'size': 2.0
            }
        ],
        'account_2': []
    }
    
    state.save_positions(test_positions)
    loaded_positions = state.load_positions()
    assert loaded_positions == test_positions, "Positions mismatch!"
    print("   ✅ Positions save/load works")
    
    # Test capital
    print("3. Testing capital...")
    test_capital = {
        'account_1': 1050.25,
        'account_2': 980.50,
        'total': 2030.75
    }
    
    state.save_capital(test_capital)
    loaded_capital = state.load_capital()
    assert loaded_capital == test_capital, "Capital mismatch!"
    print("   ✅ Capital save/load works")
    
    # Test backup
    print("4. Testing backup...")
    state.backup_state(backup_dir="data/test/backups")
    print("   ✅ Backup works")
    
    # Test state info
    print("5. Testing state info...")
    info = state.get_state_info()
    print(f"   State files: {json.dumps(info, indent=2)}")
    print("   ✅ State info works")
    
    # Clean up
    state.clear_all_state()
    print("\n✅ State persistence test complete!\n")