"""
Local State Service

This service handles all local filesystem state tracking operations.
It provides atomic operations to check the status of raw, processed, and ML data
for individual coordinates.
"""

import json
import os
from pathlib import Path
from typing import Dict, List
import logging

from src.models import ExtractionDetails, MLExportStatus
from src.models import RunCoordinate
from src.models import ExtractionStatus, LocalRawStatus



logger = logging.getLogger(__name__)


class LocalStateService:
    """
    Pure filesystem operations service for tracking local data state.
    
    This service performs atomic operations to check if data exists locally
    for individual coordinates. It does not perform any modifications,
    only state queries.
    """
    
    def __init__(self, raw_root: Path, processed_root: Path, ml_root: Path):
        """
        Initialize LocalStateService with data root paths.
        
        Args:
            raw_root: Path to raw data directory
            processed_root: Path to processed data directory  
            ml_root: Path to ML data directory
        """
        self.raw_root = Path(raw_root)
        self.processed_root = Path(processed_root)
        self.ml_root = Path(ml_root)
        
        # Ensure paths exist (for safety, but don't create them)
        if not self.raw_root.exists():
            logger.warning(f"Raw data root does not exist: {self.raw_root}")
        if not self.processed_root.exists():
            logger.warning(f"Processed data root does not exist: {self.processed_root}")
        if not self.ml_root.exists():
            logger.warning(f"ML data root does not exist: {self.ml_root}")
    
    # ========================================================================
    # Raw Data Queries (Atomic)
    # ========================================================================
    
    def check_raw_downloaded(self, coord: RunCoordinate) -> LocalRawStatus:
        """
        Check if raw bags are downloaded locally for single coordinate.
        
        Args:
            coord: RunCoordinate to check
            
        Returns:
            LocalRawStatus with download information
        """
        try:
            # Construct path: raw_root/cid/regionid/fieldid/twid/lbid/timestamp/
            coord_path = self._build_coordinate_path(self.raw_root, coord)
            
            if not coord_path.exists():
                return LocalRawStatus(
                    downloaded=False,
                    bag_count=0,
                    bag_names=[],
                    total_size=0,
                    path=None
                )
            
            # Find all .bag files in the coordinate directory
            bag_files = list(coord_path.glob("*.bag"))
            
            if not bag_files:
                return LocalRawStatus(
                    downloaded=False,
                    bag_count=0,
                    bag_names=[],
                    total_size=0,
                    path=str(coord_path)
                )
            
            # Calculate total size and collect names
            total_size = 0
            bag_names = []
            
            for bag_file in bag_files:
                bag_names.append(bag_file.name)  # Always add name
                try:
                    total_size += bag_file.stat().st_size
                except OSError as e:
                    logger.warning(f"Could not stat bag file {bag_file}: {e}")
                    # Continue without adding size but name is already added
            
            return LocalRawStatus(
                downloaded=True,
                bag_count=len(bag_files),
                bag_names=sorted(bag_names),
                total_size=total_size,
                path=str(coord_path)
            )
            
        except Exception as e:
            logger.error(f"Error checking raw download status for {coord}: {e}")
            return LocalRawStatus(
                downloaded=False,
                bag_count=0,
                bag_names=[],
                total_size=0,
                path=None
            )
    
    def check_extracted(self, coord: RunCoordinate) -> ExtractionStatus:
        """
        Check extraction status for single coordinate.
        
        Args:
            coord: RunCoordinate to check
            
        Returns:
            ExtractionStatus with extraction information
        """
        try:
            # Construct path: processed_root/cid/regionid/fieldid/twid/lbid/timestamp/
            coord_path = self._build_coordinate_path(self.processed_root, coord)
            
            if not coord_path.exists():
                return ExtractionStatus(
                    extracted=False,
                    files={},
                    path=None
                )
            
            # Expected extraction files
            expected_files = {
                "frames.csv": False,
                "detections.csv": False,
                "tracking.csv": False,
                "detections.json": False,
                "tracking.json": False,
                "metadata.json": False
            }
            
            # Check for extracted files in all subdirectories
            # (since extraction creates subdirs per bag)
            files_found = {}
            any_files_found = False
            
            for expected_file in expected_files.keys():
                # Look for the file in the coordinate directory and subdirectories
                found_files = list(coord_path.rglob(expected_file))
                if found_files:
                    files_found[expected_file] = True
                    any_files_found = True
                else:
                    files_found[expected_file] = False
            
            return ExtractionStatus(
                extracted=any_files_found,
                files=files_found,
                path=str(coord_path) if any_files_found else None
            )
            
        except Exception as e:
            logger.error(f"Error checking extraction status for {coord}: {e}")
            return ExtractionStatus(
                extracted=False,
                files={},
                path=None
            )
    
    def get_extraction_details(self, coord: RunCoordinate) -> ExtractionDetails:
        """
        Get detailed info about extracted files for single coordinate.
        
        Args:
            coord: RunCoordinate to check
            
        Returns:
            ExtractionDetails with detailed extraction information
        """
        try:
            coord_path = self._build_coordinate_path(self.processed_root, coord)
            
            if not coord_path.exists():
                return ExtractionDetails(
                    exists=False,
                    path=None,
                    subdirectories=[],
                    total_files=0,
                    total_size=0,
                    file_details={}
                )
            
            # Collect information about all files and subdirectories
            subdirectories = []
            file_details = {}
            total_files = 0
            total_size = 0
            
            for item in coord_path.rglob("*"):
                if item.is_file():
                    try:
                        file_size = item.stat().st_size
                        relative_path = str(item.relative_to(coord_path))
                        file_details[relative_path] = {
                            "size": file_size,
                            "type": item.suffix.lower() if item.suffix else "no_extension"
                        }
                        total_files += 1
                        total_size += file_size
                    except OSError as e:
                        logger.warning(f"Could not stat file {item}: {e}")
                
                elif item.is_dir() and item != coord_path:
                    relative_path = str(item.relative_to(coord_path))
                    if relative_path not in subdirectories:
                        subdirectories.append(relative_path)
            
            return ExtractionDetails(
                exists=True,
                path=str(coord_path),
                subdirectories=sorted(subdirectories),
                total_files=total_files,
                total_size=total_size,
                file_details=file_details
            )
            
        except Exception as e:
            logger.error(f"Error getting extraction details for {coord}: {e}")
            return ExtractionDetails(
                exists=False,
                path=None,
                subdirectories=[],
                total_files=0,
                total_size=0,
                file_details={}
            )
    
    def check_ml_exported(self, coord: RunCoordinate) -> MLExportStatus:
        """
        Check if ML samples exist in ML/raw export structure.
        
        This checks the ML/raw directory which contains exported annotated samples
        that were processed through the annotation pipeline and exported via dataset-export.
        
        Args:
            coord: RunCoordinate to check
            
        Returns:
            MLExportStatus with ML export information
        """
        try:
            # Focus on ML/raw structure (exported annotated samples)
            ml_raw_path = self._build_coordinate_path(self.ml_root / "raw", coord)
            
            if not ml_raw_path.exists():
                return MLExportStatus(
                    exists=False,
                    path=None,
                    file_counts={},
                    total_size=0,
                    subdirectories=[],
                    sample_files=[]
                )
            
            # Get file counts and details
            file_counts = self._count_files_in_path(ml_raw_path)
            
            # Get subdirectories and sample files
            subdirs = []
            sample_files = []
            total_size = 0
            
            for item in ml_raw_path.rglob("*"):
                if item.is_dir() and item != ml_raw_path:
                    relative_path = str(item.relative_to(ml_raw_path))
                    if relative_path not in subdirs:
                        subdirs.append(relative_path)
                elif item.is_file():
                    try:
                        file_size = item.stat().st_size
                        total_size += file_size
                        relative_path = str(item.relative_to(ml_raw_path))
                        sample_files.append({
                            "path": relative_path,
                            "size": file_size,
                            "extension": item.suffix.lower() if item.suffix else "no_extension"
                        })
                    except OSError:
                        pass  # Skip files we can't read
            
            return MLExportStatus(
                exists=True,
                path=str(ml_raw_path),
                file_counts=file_counts,
                total_size=total_size,
                subdirectories=sorted(subdirs),
                sample_files=sample_files
            )
            
        except Exception as e:
            logger.error(f"Error checking ML export for {coord}: {e}")
            return MLExportStatus(
                exists=False,
                path=None,
                file_counts={},
                total_size=0,
                subdirectories=[],
                sample_files=[]
            )
    
    def get_export_ids(self) -> List[str]:
        """
        Get available export IDs from ML/raw/.export_tracking.json.
        
        Returns:
            List of export IDs
        """
        try:
            tracking_file = self.ml_root / "raw" / ".export_tracking.json"
            
            if not tracking_file.exists():
                logger.info(f"Export tracking file not found: {tracking_file}")
                return []
            
            with open(tracking_file, 'r') as f:
                tracking_data = json.load(f)
            
            # Extract export IDs from tracking data
            export_ids = []
            if isinstance(tracking_data, dict):
                if "exports" in tracking_data:
                    export_ids = list(tracking_data["exports"].keys())
                else:
                    # If the root level contains export info directly
                    export_ids = [key for key in tracking_data.keys() 
                                if key not in ["metadata", "last_updated", "version"]]
            
            return sorted(export_ids)
            
        except Exception as e:
            logger.error(f"Error reading export tracking file: {e}")
            return []
    
    def get_export_info(self, export_id: str) -> Dict:
        """
        Get detailed export information from ML/raw/.export_tracking.json.
        
        Args:
            export_id: Export ID to get information for
            
        Returns:
            Dictionary with export information
        """
        try:
            tracking_file = self.ml_root / "raw" / ".export_tracking.json"
            
            if not tracking_file.exists():
                return {
                    "exists": False,
                    "error": "Export tracking file not found"
                }
            
            with open(tracking_file, 'r') as f:
                tracking_data = json.load(f)
            
            # Find export info
            export_info = None
            if isinstance(tracking_data, dict):
                if "exports" in tracking_data and export_id in tracking_data["exports"]:
                    export_info = tracking_data["exports"][export_id]
                elif export_id in tracking_data:
                    export_info = tracking_data[export_id]
            
            if export_info is None:
                return {
                    "exists": False,
                    "error": f"Export ID '{export_id}' not found"
                }
            
            return {
                "exists": True,
                "export_id": export_id,
                "info": export_info
            }
            
        except Exception as e:
            logger.error(f"Error getting export info for {export_id}: {e}")
            return {
                "exists": False,
                "error": str(e)
            }
    
    # ========================================================================
    # Helper Methods
    # ========================================================================
    
    def _build_coordinate_path(self, root_path: Path, coord: RunCoordinate) -> Path:
        """
        Build filesystem path from coordinate.
        
        Args:
            root_path: Base path (raw_root, processed_root, etc.)
            coord: RunCoordinate to build path for
            
        Returns:
            Path object for the coordinate
        """
        return root_path / coord.cid / coord.regionid / coord.fieldid / coord.twid / coord.lbid / coord.timestamp
    
    def _count_files_in_path(self, path: Path) -> Dict[str, int]:
        """
        Count files by extension in a given path.
        
        Args:
            path: Path to count files in
            
        Returns:
            Dictionary with file counts by extension
        """
        try:
            file_counts = {}
            total_files = 0
            
            for file_path in path.rglob("*"):
                if file_path.is_file():
                    total_files += 1
                    ext = file_path.suffix.lower() if file_path.suffix else "no_extension"
                    file_counts[ext] = file_counts.get(ext, 0) + 1
            
            file_counts["total"] = total_files
            return file_counts
            
        except Exception as e:
            logger.warning(f"Error counting files in {path}: {e}")
            return {"total": 0, "error": str(e)}
