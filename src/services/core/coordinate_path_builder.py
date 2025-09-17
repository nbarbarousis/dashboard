"""
CoordinatePathBuilder Service - Centralized path management for all coordinate operations.

This service handles all coordinate-to-path translations and naming convention 
differences between local/cloud and raw/ML structures.
"""

import logging
from pathlib import Path
from typing import Dict

from src.models import RunCoordinate

logger = logging.getLogger(__name__)


class CoordinatePathBuilder:
    """
    Centralized path management service that handles all coordinate-to-path translations
    and naming convention differences between local/cloud and raw/ML structures.
    """
    
    def __init__(self, raw_root: Path, ml_root: Path, processed_root: Path, bucket_names: Dict[str, str]):
        """
        Initialize path builder with root directories and bucket names.
        
        Args:
            raw_root: Local raw data root directory
            ml_root: Local ML data root directory  
            processed_root: Local processed data root directory
            bucket_names: Dict with 'raw', 'ml' bucket names (processed bucket skipped)
        """
        self.raw_root = Path(raw_root)
        self.ml_root = Path(ml_root)
        self.processed_root = Path(processed_root)
        self.bucket_names = bucket_names
        
        # Log warning about processed bucket
        if 'processed' in bucket_names:
            logger.warning("Processed bucket found in config but cloud processed operations not implemented")
    
    # ========================================================================
    # Local Path Methods
    # ========================================================================
    
    def get_local_raw_coordinate_path(self, coord: RunCoordinate) -> Path:
        """Get coordinate directory for local raw data."""
        return self.raw_root / coord.cid / coord.regionid / coord.fieldid / coord.twid / coord.lbid / coord.timestamp
    
    def get_local_raw_bag_path(self, coord: RunCoordinate, bag_name: str) -> Path:
        """Get specific bag file path for local raw data."""
        return self.get_local_raw_coordinate_path(coord) / bag_name
    
    def get_local_ml_raw_path(self, coord: RunCoordinate) -> Path:
        """Get coordinate directory for local ML data."""
        return self.ml_root / "raw" / coord.cid / coord.regionid / coord.fieldid / coord.twid / coord.lbid / coord.timestamp
    
    def get_local_ml_bag_path(self, coord: RunCoordinate, bag_name: str, file_type: str, filename: str) -> Path:
        """Get specific ML file path (frames/labels)."""
        return self.get_local_ml_raw_path(coord) / bag_name / file_type / filename
    
    def get_local_processed_coordinate_path(self, coord: RunCoordinate) -> Path:
        """Get coordinate directory for local processed data."""
        return self.processed_root / coord.cid / coord.regionid / coord.fieldid / coord.twid / coord.lbid / coord.timestamp
    
    # ========================================================================
    # Cloud Path Methods
    # ========================================================================
    
    def get_cloud_raw_bag_path(self, coord: RunCoordinate, bag_name: str) -> Path:
        """Get cloud raw bag path."""
        return Path(f"{coord.cid}/{coord.regionid}/{coord.fieldid}/{coord.twid}/{coord.lbid}/{coord.timestamp}/rosbag/{bag_name}")
    
    def get_cloud_ml_file_path(self, coord: RunCoordinate, bag_name: str, file_type: str, filename: str) -> Path:
        """Get cloud ML file path."""
        return Path(f"raw/{coord.cid}/{coord.regionid}/{coord.fieldid}/{coord.twid}/{coord.lbid}/{coord.timestamp}/rosbag/{bag_name}/{file_type}/{filename}")
    
    # ========================================================================
    # Name Translation Methods
    # ========================================================================
    
    def translate_bag_name_cloud_to_local(self, cloud_bag_name: str) -> str:
        """
        Convert cloud bag naming to local convention.
        
        Cloud format: "_2025-08-12-08-54-21_0.bag" or "_2025-08-12-08-54-21_0"
        Local format: "rosbag_2025-08-12-08-54-21_0.bag" or "rosbag_2025-08-12-08-54-21_0"
        
        Args:
            cloud_bag_name: Bag name in cloud format
            data_type: 'raw' or 'ml' (for future extensibility)
            
        Returns:
            Bag name in local format
        """
        if cloud_bag_name.startswith('_'):
            return f"rosbag{cloud_bag_name}"
        else:
            logger.warning(f"Cloud bag name doesn't start with '_': {cloud_bag_name}")
            return cloud_bag_name
    
    def translate_bag_name_local_to_cloud(self, local_bag_name: str) -> str:
        """
        Convert local bag naming to cloud convention.
        
        Local format: "rosbag_2025-08-12-08-54-21_0.bag" or "rosbag_2025-08-12-08-54-21_0"
        Cloud format: "_2025-08-12-08-54-21_0.bag" or "_2025-08-12-08-54-21_0"
        
        Args:
            local_bag_name: Bag name in local format
            data_type: 'raw' or 'ml' (for future extensibility)
            
        Returns:
            Bag name in cloud format
        """
        if local_bag_name.startswith('rosbag_'):
            return local_bag_name.replace('rosbag_', '_', 1)
        else:
            logger.warning(f"Local bag name doesn't start with 'rosbag_': {local_bag_name}")
            return local_bag_name
    
    # ========================================================================
    # Utility Methods
    # ========================================================================

    def get_ml_export_tracking_file(self) -> Path:
        """Get path to ML export tracking JSON file."""
        return self.ml_root / "raw" / ".export_tracking.json"
    
    def get_bucket_name(self, bucket_type: str) -> str:
        """Get bucket name for given type."""
        if bucket_type not in self.bucket_names:
            raise ValueError(f"Unknown bucket type: {bucket_type}")
        return self.bucket_names[bucket_type]
    
    def build_local_coordinate_path(self, coord: RunCoordinate, data_type: str) -> Path:
        """
        Build local coordinate path for any data type.
        
        Args:
            coord: RunCoordinate
            data_type: 'raw', 'ml', or 'processed'
            
        Returns:
            Path to coordinate directory
        """
        if data_type == 'raw':
            return self.get_local_raw_coordinate_path(coord)
        elif data_type == 'ml':
            return self.get_local_ml_raw_path(coord)
        elif data_type == 'processed':
            return self.get_local_processed_coordinate_path(coord)
        else:
            raise ValueError(f"Unknown data type: {data_type}")