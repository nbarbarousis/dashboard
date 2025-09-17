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

from src.models import ExtractionDetails, LocalMLStatus
from src.models import RunCoordinate
from src.models import ExtractionStatus, LocalRawStatus
from .coordinate_path_builder import CoordinatePathBuilder

logger = logging.getLogger(__name__)


class LocalStateService:
    """
    Pure filesystem operations service for tracking local data state.
    
    This service performs atomic operations to check if data exists locally
    for individual coordinates. It does not perform any modifications,
    only state queries.
    """
    
    def __init__(self, path_builder: CoordinatePathBuilder):
        """
        Initialize LocalStateService with path builder.
        
        Args:
            path_builder: CoordinatePathBuilder service for all path operations
        """
        self.path_builder = path_builder
        
        # Ensure paths exist (for safety, but don't create them)
        if not self.path_builder.raw_root.exists():
            logger.warning(f"Raw data root does not exist: {self.path_builder.raw_root}")
        if not self.path_builder.processed_root.exists():
            logger.warning(f"Processed data root does not exist: {self.path_builder.processed_root}")
        if not self.path_builder.ml_root.exists():
            logger.warning(f"ML data root does not exist: {self.path_builder.ml_root}")
    
    # ========================================================================
    # Raw Data Queries (Atomic)
    # ========================================================================
    
    def get_raw_status(self, coord: RunCoordinate) -> LocalRawStatus:
        """
        Check if raw bags are downloaded locally for single coordinate.
        
        Args:
            coord: RunCoordinate to check
            
        Returns:
            LocalRawStatus with download information
        """
        try:
            coord_path = self.path_builder.get_local_raw_coordinate_path(coord)
            
            if not coord_path.exists():
                return LocalRawStatus(
                    downloaded=False,
                    bag_count=0,
                    bag_names=[],
                    bag_sizes={},
                    total_size=0
                )
            
            # Find all .bag files in the coordinate directory
            bag_files = list(coord_path.glob("*.bag"))
            
            if not bag_files:
                return LocalRawStatus(
                    downloaded=False,
                    bag_count=0,
                    bag_names=[],
                    bag_sizes={},
                    total_size=0
                )
            
            # Calculate total size and collect names with sizes
            total_size = 0
            bag_names = []
            bag_sizes = {}
            
            for bag_file in bag_files:
                bag_names.append(bag_file.name)
                try:
                    file_size = bag_file.stat().st_size
                    bag_sizes[bag_file.name] = file_size
                    total_size += file_size
                except OSError as e:
                    logger.warning(f"Could not stat bag file {bag_file}: {e}")
                    bag_sizes[bag_file.name] = 0
            
            return LocalRawStatus(
                downloaded=True,
                bag_count=len(bag_files),
                bag_names=sorted(bag_names),
                bag_sizes=bag_sizes,
                total_size=total_size
            )
            
        except Exception as e:
            logger.error(f"Error checking raw download status for {coord}: {e}")
            return LocalRawStatus(
                downloaded=False,
                bag_count=0,
                bag_names=[],
                bag_sizes={},
                total_size=0
            )
    
    # def check_extracted(self, coord: RunCoordinate) -> ExtractionStatus:
    #     """
    #     Check extraction status for single coordinate.
        
    #     Args:
    #         coord: RunCoordinate to check
            
    #     Returns:
    #         ExtractionStatus with extraction information
    #     """
    #     try:
    #         coord_path = self.path_builder.get_local_processed_coordinate_path(coord)
            
    #         if not coord_path.exists():
    #             return ExtractionStatus(
    #                 extracted=False,
    #                 files={},
    #                 path=None
    #             )
            
    #         # Expected extraction files
    #         expected_files = {
    #             "frames.csv": False,
    #             "detections.csv": False,
    #             "tracking.csv": False,
    #             "detections.json": False,
    #             "tracking.json": False,
    #             "metadata.json": False
    #         }
            
    #         # Check for extracted files in all subdirectories
    #         # (since extraction creates subdirs per bag)
    #         files_found = {}
    #         any_files_found = False
            
    #         for expected_file in expected_files.keys():
    #             # Look for the file in the coordinate directory and subdirectories
    #             found_files = list(coord_path.rglob(expected_file))
    #             if found_files:
    #                 files_found[expected_file] = True
    #                 any_files_found = True
    #             else:
    #                 files_found[expected_file] = False
            
    #         return ExtractionStatus(
    #             extracted=any_files_found,
    #             files=files_found,
    #             path=str(coord_path) if any_files_found else None
    #         )
            
    #     except Exception as e:
    #         logger.error(f"Error checking extraction status for {coord}: {e}")
    #         return ExtractionStatus(
    #             extracted=False,
    #             files={},
    #             path=None
    #         )
    
    # def get_extraction_details(self, coord: RunCoordinate) -> ExtractionDetails:
    #     """
    #     Get detailed info about extracted files for single coordinate.
        
    #     Args:
    #         coord: RunCoordinate to check
            
    #     Returns:
    #         ExtractionDetails with detailed extraction information
    #     """
    #     try:
    #         coord_path = self.path_builder.get_local_processed_coordinate_path(coord)
            
    #         if not coord_path.exists():
    #             return ExtractionDetails(
    #                 exists=False,
    #                 path=None,
    #                 subdirectories=[],
    #                 total_files=0,
    #                 total_size=0,
    #                 file_details={}
    #             )
            
    #         # Collect information about all files and subdirectories
    #         subdirectories = []
    #         file_details = {}
    #         total_files = 0
    #         total_size = 0
            
    #         for item in coord_path.rglob("*"):
    #             if item.is_file():
    #                 try:
    #                     file_size = item.stat().st_size
    #                     relative_path = str(item.relative_to(coord_path))
    #                     file_details[relative_path] = {
    #                         "size": file_size,
    #                         "type": item.suffix.lower() if item.suffix else "no_extension"
    #                     }
    #                     total_files += 1
    #                     total_size += file_size
    #                 except OSError as e:
    #                     logger.warning(f"Could not stat file {item}: {e}")
                
    #             elif item.is_dir() and item != coord_path:
    #                 relative_path = str(item.relative_to(coord_path))
    #                 if relative_path not in subdirectories:
    #                     subdirectories.append(relative_path)
            
    #         return ExtractionDetails(
    #             exists=True,
    #             path=str(coord_path),
    #             subdirectories=sorted(subdirectories),
    #             total_files=total_files,
    #             total_size=total_size,
    #             file_details=file_details
    #         )
            
    #     except Exception as e:
    #         logger.error(f"Error getting extraction details for {coord}: {e}")
    #         return ExtractionDetails(
    #             exists=False,
    #             path=None,
    #             subdirectories=[],
    #             total_files=0,
    #             total_size=0,
    #             file_details={}
    #         )
    
    def get_ml_status(self, coord: RunCoordinate) -> LocalMLStatus:
        """
        Check if ML samples exist in ML/raw export structure.
        
        This checks the ML/raw directory which contains exported annotated samples
        that were processed through the annotation pipeline and exported via dataset-export.
        
        Args:
            coord: RunCoordinate to check
            
        Returns:
            LocalMLStatus with ML export information
        """
        try:
            ml_raw_path = self.path_builder.get_local_ml_raw_path(coord)
            
            if not ml_raw_path.exists():
                return LocalMLStatus(
                    downloaded=False,
                    total_samples=0,
                    bag_samples={},
                    bag_files={},
                    file_counts={}
                )
            
            # Get file counts and calculate totals
            file_counts = self._count_files_in_path(ml_raw_path)
            bag_samples = {}
            bag_files = {}
            total_samples = 0
            
            # Build bag structure and calculate totals
            for item in ml_raw_path.rglob("*"):
                if item.is_file():
                    try:
                        file_size = item.stat().st_size
                        
                        # Parse bag structure from path
                        relative_path = item.relative_to(ml_raw_path)
                        path_parts = relative_path.parts
                        
                        # Expected structure: bag_name/file_type/filename
                        if len(path_parts) >= 3:
                            bag_name = path_parts[0]
                            file_type = path_parts[1]  # "frames" or "labels"
                            filename = path_parts[2]
                            
                            if file_type in ['frames', 'labels']:
                                # Initialize bag structures
                                if bag_name not in bag_samples:
                                    bag_samples[bag_name] = {'frame_count': 0, 'label_count': 0}
                                if bag_name not in bag_files:
                                    bag_files[bag_name] = {'frames': {}, 'labels': {}}
                                
                                # Update counts
                                if file_type == 'frames':
                                    bag_samples[bag_name]['frame_count'] += 1
                                elif file_type == 'labels':
                                    bag_samples[bag_name]['label_count'] += 1
                                    total_samples += 1  # Labels represent samples
                                
                                # Store file with size
                                bag_files[bag_name][file_type][filename] = file_size
                                
                    except OSError:
                        pass  # Skip files we can't read
            
            return LocalMLStatus(
                downloaded=total_samples > 0,
                total_samples=total_samples,
                bag_samples=bag_samples,
                bag_files=bag_files,
                file_counts=file_counts
            )
            
        except Exception as e:
            logger.error(f"Error checking ML export for {coord}: {e}")
            return LocalMLStatus(
                downloaded=False,
                total_samples=0,
                bag_samples={},
                bag_files={},
                file_counts={}
            )
        
    # ========================================================================
    # Bulk Discovery Methods
    # ========================================================================
    
    def get_all_raw_statuses(self) -> Dict[RunCoordinate, LocalRawStatus]:
        """
        Walk filesystem to discover all local raw data.
        
        Returns:
            Dictionary mapping RunCoordinate to LocalRawStatus for all found coordinates
        """
        results = {}
        
        if not self.path_builder.raw_root.exists():
            logger.warning(f"Raw data root does not exist: {self.path_builder.raw_root}")
            return results
        
        try:
            # Walk filesystem hierarchy: cid/regionid/fieldid/twid/lbid/timestamp/
            for cid_dir in self.path_builder.raw_root.iterdir():
                if not cid_dir.is_dir():
                    continue
                    
                for regionid_dir in cid_dir.iterdir():
                    if not regionid_dir.is_dir():
                        continue
                        
                    for fieldid_dir in regionid_dir.iterdir():
                        if not fieldid_dir.is_dir():
                            continue
                            
                        for twid_dir in fieldid_dir.iterdir():
                            if not twid_dir.is_dir():
                                continue
                                
                            for lbid_dir in twid_dir.iterdir():
                                if not lbid_dir.is_dir():
                                    continue
                                    
                                for timestamp_dir in lbid_dir.iterdir():
                                    if not timestamp_dir.is_dir():
                                        continue
                                    
                                    # Create coordinate from directory names
                                    coord = RunCoordinate(
                                        cid=cid_dir.name,
                                        regionid=regionid_dir.name,
                                        fieldid=fieldid_dir.name,
                                        twid=twid_dir.name,
                                        lbid=lbid_dir.name,
                                        timestamp=timestamp_dir.name
                                    )
                                    
                                    # Get status for this coordinate
                                    try:
                                        results[coord] = self.get_raw_status(coord)
                                    except Exception as e:
                                        logger.warning(f"Error getting raw status for {coord}: {e}")
                                        # Continue with other coordinates
                                        continue
            
            logger.info(f"Discovered {len(results)} raw coordinates")
            return results
            
        except Exception as e:
            logger.error(f"Error during raw filesystem discovery: {e}")
            return results
    
    def get_all_ml_statuses(self) -> Dict[RunCoordinate, LocalMLStatus]:
        """
        Walk ML/raw filesystem to discover all local ML data.
        
        Returns:
            Dictionary mapping RunCoordinate to LocalMLStatus for all found coordinates
        """
        results = {}
        
        # ML data is in ml_root/raw/ structure
        ml_raw_root = self.path_builder.ml_root / "raw"
        
        if not ml_raw_root.exists():
            logger.warning(f"ML raw data root does not exist: {ml_raw_root}")
            return results
        
        try:
            # Walk filesystem hierarchy: ml_root/raw/cid/regionid/fieldid/twid/lbid/timestamp/
            for cid_dir in ml_raw_root.iterdir():
                if not cid_dir.is_dir():
                    continue
                    
                for regionid_dir in cid_dir.iterdir():
                    if not regionid_dir.is_dir():
                        continue
                        
                    for fieldid_dir in regionid_dir.iterdir():
                        if not fieldid_dir.is_dir():
                            continue
                            
                        for twid_dir in fieldid_dir.iterdir():
                            if not twid_dir.is_dir():
                                continue
                                
                            for lbid_dir in twid_dir.iterdir():
                                if not lbid_dir.is_dir():
                                    continue
                                    
                                for timestamp_dir in lbid_dir.iterdir():
                                    if not timestamp_dir.is_dir():
                                        continue
                                    
                                    # Create coordinate from directory names
                                    coord = RunCoordinate(
                                        cid=cid_dir.name,
                                        regionid=regionid_dir.name,
                                        fieldid=fieldid_dir.name,
                                        twid=twid_dir.name,
                                        lbid=lbid_dir.name,
                                        timestamp=timestamp_dir.name
                                    )
                                    
                                    # Get status for this coordinate
                                    try:
                                        results[coord] = self.get_ml_status(coord)
                                    except Exception as e:
                                        logger.warning(f"Error getting ML status for {coord}: {e}")
                                        # Continue with other coordinates
                                        continue
            
            logger.info(f"Discovered {len(results)} ML coordinates")
            return results
            
        except Exception as e:
            logger.error(f"Error during ML filesystem discovery: {e}")
            return results
        
    # ========================================================================
    # Export Tracking Methods
    # ========================================================================
    
    def get_export_ids(self) -> List[str]:
        """
        Get available export IDs from ML/raw/.export_tracking.json.
        
        Returns:
            List of export IDs
        """
        try:
            # Use path builder for export tracking file path
            tracking_file = self.path_builder.get_ml_export_tracking_file()
            
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
            # Use path builder for export tracking file path
            tracking_file = self.path_builder.get_ml_export_tracking_file()
            
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
