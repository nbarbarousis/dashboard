"""
Service for rosbag data operations
Handles loading and managing extracted rosbag data
"""
import json
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime
import pandas as pd

from models.data_models import (
    RunCoordinate, ExtractedData, DataStatus
)

logger = logging.getLogger(__name__)


class RosbagService:
    """
    Handles rosbag data operations.
    Manages extracted data loading and status checking.
    """
    
    def __init__(self, raw_root: Path, processed_root: Path):
        """
        Initialize rosbag service
        
        Args:
            raw_root: Root directory for raw rosbag data
            processed_root: Root directory for processed/extracted data
        """
        self.raw_root = Path(raw_root)
        self.processed_root = Path(processed_root)
    
    def check_extraction_status(self, coord: RunCoordinate) -> Dict:
        """
        Check if data has already been extracted for a run
        
        Args:
            coord: Run coordinate
            
        Returns:
            Dictionary with status and file information
        """
        processed_path = self._get_processed_path(coord)
        
        if not processed_path.exists():
            return {
                'status': DataStatus.NOT_DOWNLOADED,
                'path': None,
                'files': {}
            }
        
        # Check for expected output files
        frames_csv = processed_path / "frames.csv"
        detections_csv = processed_path / "detections.csv"
        tracking_csv = processed_path / "tracking.csv"
        metadata_yaml = processed_path / "metadata.yaml"
        
        files = {
            'frames': frames_csv.exists(),
            'detections': detections_csv.exists(),
            'tracking': tracking_csv.exists(),
            'metadata': metadata_yaml.exists()
        }
        
        # Determine overall status
        if all(files.values()):
            status = DataStatus.EXTRACTED
        elif any(files.values()):
            status = DataStatus.DOWNLOADED  # Partial extraction
        else:
            status = DataStatus.DOWNLOADED  # No extraction yet
        
        return {
            'status': status,
            'path': str(processed_path),
            'files': files
        }
    
    def load_extracted_data(self, coord: RunCoordinate) -> Optional[ExtractedData]:
        """
        Load extracted data from disk
        
        Args:
            coord: Run coordinate
            
        Returns:
            ExtractedData object or None if not available
        """
        processed_path = self._get_processed_path(coord)
        
        if not processed_path.exists():
            logger.warning(f"No processed data found at {processed_path}")
            return None
        
        data = ExtractedData()
        
        # Load CSVs
        frames_csv = processed_path / "frames.csv"
        if frames_csv.exists():
            data.frames_df = pd.read_csv(frames_csv)
            logger.debug(f"Loaded {len(data.frames_df)} frames")
        
        detections_csv = processed_path / "detections.csv"
        if detections_csv.exists():
            data.detections_df = pd.read_csv(detections_csv)
            logger.debug(f"Loaded {len(data.detections_df)} detection messages")
        
        tracking_csv = processed_path / "tracking.csv"
        if tracking_csv.exists():
            data.tracking_df = pd.read_csv(tracking_csv)
            logger.debug(f"Loaded {len(data.tracking_df)} tracking messages")
        
        # Load JSON files
        detections_json = processed_path / "detections_full.json"
        if detections_json.exists():
            with open(detections_json, 'r') as f:
                data.detections_json = json.load(f)
        
        tracking_json = processed_path / "tracking_full.json"
        if tracking_json.exists():
            with open(tracking_json, 'r') as f:
                data.tracking_json = json.load(f)
        
        # Load metadata
        metadata_yaml = processed_path / "metadata.yaml"
        if metadata_yaml.exists():
            with open(metadata_yaml, 'r') as f:
                data.metadata = yaml.safe_load(f)
                if data.metadata:
                    data.source_bags = [bag['name'] for bag in data.metadata.get('bags', [])]
        
        data.extraction_time = datetime.now()
        
        return data
    
    def get_available_bags(self, coord: RunCoordinate) -> List[str]:
        """
        Get list of available bag files for a run
        
        Args:
            coord: Run coordinate
            
        Returns:
            List of bag file names
        """
        raw_path = self._get_raw_path(coord)
        
        if not raw_path.exists():
            return []
        
        bags = sorted(raw_path.glob("*.bag"))
        return [bag.name for bag in bags]
    
    def _get_raw_path(self, coord: RunCoordinate) -> Path:
        """Get raw data path for coordinate"""
        return self.raw_root / coord.to_path_str()
    
    def _get_processed_path(self, coord: RunCoordinate) -> Path:
        """Get processed data path for coordinate"""
        return self.processed_root / coord.to_path_str()