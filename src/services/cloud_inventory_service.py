"""
CloudInventoryService - Master cloud data service with comprehensive caching.

This service handles all cloud storage interactions and maintains a comprehensive
cache of cloud inventory data. ALL coordinate queries return structured model objects.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from google.cloud import storage
from google.api_core import exceptions as gcs_exceptions

from src.models import RunCoordinate
from src.models import CloudRawStatus, CloudMLStatus
from src.models import TimelineData
from src.models import CacheInfo

logger = logging.getLogger(__name__)


class CloudInventoryService:
    """
    Master cloud data service with comprehensive caching.
    ALL coordinate queries return structured model objects for consistency.
    """
    
    def __init__(self, bucket_names: Dict[str, str], cache_file: str):
        """
        Initialize cloud inventory service.
        
        Args:
            bucket_names: Dict with keys 'raw', 'ml', 'processed' and bucket names as values
            cache_file: Path to JSON cache file
        """
        self.bucket_names = bucket_names
        self.cache_file = Path(cache_file)
        self._inventory_cache: Optional[Dict] = None
        self._cache_timestamp: Optional[datetime] = None
        self._is_stale: bool = False
        
        # Initialize GCS client
        self.gcs_client = storage.Client()
        
        # Ensure cache directory exists
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing cache on initialization
        self._load_cache()
    
    # ============================================================================
    # Cache Management
    # ============================================================================
    
    def get_full_inventory(self, force_refresh: bool = False) -> Dict:
        """
        Get complete cloud inventory - raw cache access.
        
        Args:
            force_refresh: Force refresh from cloud even if cache is valid
            
        Returns:
            Dict: Raw cache data structure
        """
        if force_refresh or self._should_refresh_cache():
            logger.info("Refreshing cloud inventory cache")
            self._refresh_inventory()
        
        if self._inventory_cache is None:
            logger.warning("No inventory cache available, returning empty dict")
            return {}
            
        return self._inventory_cache
    
    def mark_stale(self, reason: str = "external_change") -> None:
        """Mark cache stale - simple invalidation."""
        logger.info(f"Marking cache stale: {reason}")
        self._is_stale = True
    
    def get_cache_info(self) -> CacheInfo:
        """Get cache metadata information."""
        if not self.cache_file.exists():
            return CacheInfo(
                last_updated=datetime.min,
                size_bytes=0,
                entry_count=0,
                cache_file=str(self.cache_file)
            )
        
        stat = self.cache_file.stat()
        entry_count = 0
        
        if self._inventory_cache:
            # Count total coordinates across all bucket types
            for bucket_type, bucket_data in self._inventory_cache.items():
                if bucket_type != 'metadata' and isinstance(bucket_data, dict):
                    entry_count += self._count_coordinates_in_bucket(bucket_data)
        
        return CacheInfo(
            last_updated=self._cache_timestamp or datetime.fromtimestamp(stat.st_mtime),
            size_bytes=stat.st_size,
            entry_count=entry_count,
            cache_file=str(self.cache_file)
        )
    
    # ============================================================================
    # Hierarchy Queries - Return simple types (fine for filter dropdowns)
    # ============================================================================
    
    def get_hierarchy_level(self, level: str, parent_path: Tuple[str, ...] = (), bucket_type: str = 'raw') -> List[str]:
        """
        Get available options at hierarchy level from cache.
        
        Args:
            level: Hierarchy level ('cid', 'regionid', 'fieldid', 'twid', 'lbid', 'timestamp')
            parent_path: Parent path components
            bucket_type: Which bucket to query ('raw', 'ml', 'processed')
            
        Returns:
            List[str]: Available options at this level
        """
        inventory = self.get_full_inventory()
        
        if bucket_type not in inventory:
            return []
        
        current_level = inventory[bucket_type]
        
        # Navigate to the specified level
        for path_component in parent_path:
            if not isinstance(current_level, dict) or path_component not in current_level:
                return []
            current_level = current_level[path_component]
        
        if not isinstance(current_level, dict):
            return []
        
        return sorted(list(current_level.keys()))
    
    def path_exists(self, path: Tuple[str, ...], bucket_type: str = 'raw') -> bool:
        """
        Check if specific hierarchy path exists.
        
        Args:
            path: Path components (cid, regionid, fieldid, twid, lbid, timestamp)
            bucket_type: Which bucket to check
            
        Returns:
            bool: True if path exists
        """
        inventory = self.get_full_inventory()
        
        if bucket_type not in inventory:
            return False
        
        current_level = inventory[bucket_type]
        
        for path_component in path:
            if not isinstance(current_level, dict) or path_component not in current_level:
                return False
            current_level = current_level[path_component]
        
        return True
    
    # ============================================================================
    # Coordinate Queries - ALL return models for consistency
    # ============================================================================
    
    def get_raw_status(self, coord: RunCoordinate) -> CloudRawStatus:
        """
        Get raw bag info for single coordinate from cache.
        
        Args:
            coord: RunCoordinate to query
            
        Returns:
            CloudRawStatus: Raw bag information from cloud
        """
        try:
            inventory = self.get_full_inventory()
            
            # Navigate to coordinate
            path_data = self._navigate_to_coordinate(inventory['raw'], coord)
            
            if path_data is None:
                return CloudRawStatus(exists=False, bag_count=0, bag_names=[], total_size=0)
            
            # Extract bag information
            bag_names = path_data.get('bags', [])
            bag_sizes = path_data.get('sizes', [])
            total_size = sum(bag_sizes) if bag_sizes else 0
            
            return CloudRawStatus(
                exists=len(bag_names) > 0,
                bag_count=len(bag_names),
                bag_names=sorted(bag_names),
                total_size=total_size
            )
            
        except Exception as e:
            logger.error(f"Error getting raw bags info for {coord}: {e}")
            return CloudRawStatus(exists=False, bag_count=0, bag_names=[], total_size=0)
    
    def get_ml_status(self, coord: RunCoordinate) -> CloudMLStatus:
        """
        Get ML sample info for single coordinate from cache.
        
        Args:
            coord: RunCoordinate to query
            
        Returns:
            CloudMLStatus: ML sample information from cloud
        """
        try:
            inventory = self.get_full_inventory()
            
            if 'ml' not in inventory:
                return CloudMLStatus(exists=False, total_samples=0, bag_samples={})
            
            # Navigate to coordinate
            path_data = self._navigate_to_coordinate(inventory['ml'], coord)
            
            if path_data is None:
                return CloudMLStatus(exists=False, total_samples=0, bag_samples={})
            
            # Extract ML sample information
            bag_samples = path_data.get('bag_samples', {})
            total_samples = sum(
                bag_data.get('label_count', 0) 
                for bag_data in bag_samples.values() 
                if isinstance(bag_data, dict)
            )
            
            return CloudMLStatus(
                exists=total_samples > 0,
                total_samples=total_samples,
                bag_samples=bag_samples
            )
            
        except Exception as e:
            logger.error(f"Error getting ML samples info for {coord}: {e}")
            return CloudMLStatus(exists=False, total_samples=0, bag_samples={})
    
    # ============================================================================
    # Page-Specific Queries - Return appropriate models
    # ============================================================================
    
    def get_temporal_timeline(self, filters: Dict) -> TimelineData:
        """
        Extract raw timeline data for specific filter path.
        
        Args:
            filters: Filter dict with keys like 'cid', 'regionid', etc.
            
        Returns:
            TimelineData: Raw timeline data that DataStateService processes
        """
        try:
            inventory = self.get_full_inventory()
            
            # Build filter path from filters (excluding timestamp)
            filter_components = []
            for level in ['cid', 'regionid', 'fieldid', 'twid', 'lbid']:
                if level in filters and filters[level]:
                    filter_components.append(filters[level])
                else:
                    break  # Stop at first missing level
            
            # Collect timeline data from both raw and ml buckets
            raw_counts = self._extract_timeline_from_bucket(inventory.get('raw', {}), filter_components)
            ml_counts = self._extract_timeline_from_bucket(inventory.get('ml', {}), filter_components)
            
            # Get all unique timestamps and sort them
            all_timestamps = set(raw_counts.keys()) | set(ml_counts.keys())
            sorted_timestamps = sorted(list(all_timestamps))
            
            # Ensure all timestamps exist in both dicts with 0 as default
            for ts in sorted_timestamps:
                raw_counts.setdefault(ts, 0)
                ml_counts.setdefault(ts, 0)
            
            return TimelineData(
                timestamps=sorted_timestamps,
                raw_counts=raw_counts,
                ml_counts=ml_counts
            )
            
        except Exception as e:
            logger.error(f"Error extracting temporal timeline for filters {filters}: {e}")
            return TimelineData(timestamps=[], raw_counts={}, ml_counts={})
    
    # ============================================================================
    # Private Implementation Methods
    # ============================================================================
    
    def _load_cache(self) -> None:
        """Load cache from disk if it exists."""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r') as f:
                    cache_data = json.load(f)
                    self._inventory_cache = cache_data
                    
                    # Try to get timestamp from metadata or file stat
                    if 'metadata' in cache_data and 'last_updated' in cache_data['metadata']:
                        self._cache_timestamp = datetime.fromisoformat(cache_data['metadata']['last_updated'])
                    else:
                        self._cache_timestamp = datetime.fromtimestamp(self.cache_file.stat().st_mtime)
                        
                logger.info(f"Loaded cache from {self.cache_file}, last updated: {self._cache_timestamp}")
            else:
                logger.info("No existing cache file found")
                
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            self._inventory_cache = None
            self._cache_timestamp = None
    
    def _save_cache(self) -> None:
        """Save cache to disk."""
        try:
            if self._inventory_cache is None:
                return
                
            # Add metadata
            self._inventory_cache['metadata'] = {
                'last_updated': datetime.now().isoformat(),
                'cache_version': '1.0',
                'bucket_names': self.bucket_names
            }
            
            with open(self.cache_file, 'w') as f:
                json.dump(self._inventory_cache, f, indent=2)
                
            self._cache_timestamp = datetime.now()
            self._is_stale = False
            logger.info(f"Cache saved to {self.cache_file}")
            
        except Exception as e:
            logger.error(f"Error saving cache: {e}")
    
    def _should_refresh_cache(self) -> bool:
        """Determine if cache should be refreshed."""
        if self._is_stale:
            return True
            
        if self._inventory_cache is None or self._inventory_cache is {}:
            return True
            
        if self._cache_timestamp is None:
            return True
            
        return False
    
    def _refresh_inventory(self) -> None:
        """Force refresh of complete inventory from cloud."""
        try:
            logger.info("Starting cloud inventory refresh")
            
            new_cache = {}
            
            # Refresh each bucket type
            for bucket_type, bucket_name in self.bucket_names.items():
                logger.info(f"Refreshing {bucket_type} bucket: {bucket_name}")
                bucket_data = self._scan_bucket(bucket_name, bucket_type)
                new_cache[bucket_type] = bucket_data
                logger.info(f"Found {self._count_coordinates_in_bucket(bucket_data)} coordinates in {bucket_type}")
            
            self._inventory_cache = new_cache
            self._save_cache()
            
            logger.info("Cloud inventory refresh complete")
            
        except Exception as e:
            logger.error(f"Error refreshing inventory: {e}")
            # Don't clear existing cache on error
    
    def _scan_bucket(self, bucket_name: str, bucket_type: str) -> Dict:
        """
        Scan a GCS bucket and build hierarchical cache structure.
        
        Args:
            bucket_name: GCS bucket name
            bucket_type: Type of bucket ('raw', 'ml', 'processed')
            
        Returns:
            Dict: Hierarchical cache structure for this bucket
        """
        try:
            bucket = self.gcs_client.bucket(bucket_name)
            bucket_cache = {}
            
            # List all blobs in bucket
            blobs = bucket.list_blobs()
            
            for blob in blobs:
                self._process_blob(blob, bucket_cache, bucket_type)
            
            return bucket_cache
            
        except gcs_exceptions.NotFound:
            logger.error(f"Bucket not found: {bucket_name}")
            return {}
        except Exception as e:
            logger.error(f"Error scanning bucket {bucket_name}: {e}")
            return {}
    
    def _process_blob(self, blob, bucket_cache: Dict, bucket_type: str) -> None:
        """
        Process a single blob and add it to the cache structure.
        
        Args:
            blob: GCS blob object
            bucket_cache: Cache dict to update
            bucket_type: Type of bucket being processed
        """
        try:
            # Parse blob path based on bucket type
            if bucket_type == 'raw':
                self._process_raw_blob(blob, bucket_cache)
            elif bucket_type == 'ml':
                self._process_ml_blob(blob, bucket_cache)
            else:
                # For other bucket types, just track existence
                self._process_generic_blob(blob, bucket_cache)
                
        except Exception as e:
            logger.warning(f"Error processing blob {blob.name}: {e}")
    
    def _process_raw_blob(self, blob, bucket_cache: Dict) -> None:
        """Process blob from raw data bucket."""
        path_parts = blob.name.split('/')
        
        # Expected format: cid/regionid/fieldid/twid/lbid/timestamp/rosbag/<bag_filename>.bag
        if len(path_parts) >= 8 and path_parts[-2] == 'rosbag' and path_parts[-1].endswith('.bag'):
            cid, regionid, fieldid, twid, lbid, timestamp = path_parts[:6]
            filename = path_parts[-1]
            
            # Navigate/create hierarchy
            coord_data = self._ensure_coordinate_path(
                bucket_cache, cid, regionid, fieldid, twid, lbid, timestamp
            )
            
            # Add bag information
            if 'bags' not in coord_data:
                coord_data['bags'] = []
                coord_data['sizes'] = []
            
            coord_data['bags'].append(filename)
            coord_data['sizes'].append(blob.size or 0)
    
    def _process_ml_blob(self, blob, bucket_cache: Dict) -> None:
        """Process blob from ML data bucket."""
        path_parts = blob.name.split('/')
        
        # Expected format: raw/cid/regionid/fieldid/twid/lbid/timestamp/rosbag/bag_name/frames/image.jpg
        # or: raw/cid/regionid/fieldid/twid/lbid/timestamp/rosbag/bag_name/labels/label.txt
        if len(path_parts) >= 11 and path_parts[0] == 'raw' and path_parts[7] == 'rosbag':
            _, cid, regionid, fieldid, twid, lbid, timestamp, _, bag_name, data_type, filename = path_parts[:11]
            
            if data_type in ['frames', 'labels']:
                # Navigate/create hierarchy
                coord_data = self._ensure_coordinate_path(bucket_cache, cid, regionid, fieldid, twid, lbid, timestamp)
                
                # Initialize bag samples structure
                if 'bag_samples' not in coord_data:
                    coord_data['bag_samples'] = {}
                
                if bag_name not in coord_data['bag_samples']:
                    coord_data['bag_samples'][bag_name] = {
                        'frame_count': 0,
                        'label_count': 0,
                        'frame_files': [],
                        'label_files': []
                    }
                
                # Update counts and files
                bag_data = coord_data['bag_samples'][bag_name]
                if data_type == 'frames':
                    bag_data['frame_count'] += 1
                    bag_data['frame_files'].append(filename)
                elif data_type == 'labels':
                    bag_data['label_count'] += 1
                    bag_data['label_files'].append(filename)
    
    def _process_generic_blob(self, blob, bucket_cache: Dict) -> None:
        """Process blob from generic bucket - just track existence."""
        path_parts = blob.name.split('/')
        
        # Try to extract coordinate information from path
        if len(path_parts) >= 6:
            cid, regionid, fieldid, twid, lbid, timestamp = path_parts[:6]
            coord_data = self._ensure_coordinate_path(bucket_cache, cid, regionid, fieldid, twid, lbid, timestamp)
            
            # Track that this coordinate has some data
            if 'files' not in coord_data:
                coord_data['files'] = []
            coord_data['files'].append(blob.name)
    
    def _ensure_coordinate_path(self, cache: Dict, cid: str, regionid: str, fieldid: str, twid: str, lbid: str, timestamp: str) -> Dict:
        """Ensure coordinate path exists in cache and return the leaf data dict."""
        current = cache
        
        for component in [cid, regionid, fieldid, twid, lbid, timestamp]:
            if component not in current:
                current[component] = {}
            current = current[component]
        
        return current
    
    def _navigate_to_coordinate(self, bucket_data: Dict, coord: RunCoordinate) -> Optional[Dict]:
        """Navigate to specific coordinate in bucket data."""
        try:
            current = bucket_data
            
            for component in [coord.cid, coord.regionid, coord.fieldid, coord.twid, coord.lbid, coord.timestamp]:
                if component not in current:
                    return None
                current = current[component]
            
            return current
            
        except Exception:
            return None
    
    def _extract_timeline_from_bucket(self, bucket_data: Dict, filter_components: List[str]) -> Dict[str, int]:
        """Extract timeline counts from bucket data for given filter path."""
        try:
            # Navigate to the filter level
            current = bucket_data
            for component in filter_components:
                if component not in current:
                    return {}
                current = current[component]
            
            # If we're at the timestamp level, return the data
            if len(filter_components) == 5:  # cid/regionid/fieldid/twid/lbid
                # Current should contain timestamp keys
                timeline = {}
                for timestamp, timestamp_data in current.items():
                    if isinstance(timestamp_data, dict):
                        # Count based on bucket type
                        if 'bags' in timestamp_data:  # Raw bucket
                            timeline[timestamp] = len(timestamp_data['bags'])
                        elif 'bag_samples' in timestamp_data:  # ML bucket
                            total_samples = sum(
                                bag_data.get('label_count', 0)
                                for bag_data in timestamp_data['bag_samples'].values()
                                if isinstance(bag_data, dict)
                            )
                            timeline[timestamp] = total_samples
                        else:
                            timeline[timestamp] = 1 if timestamp_data else 0
                
                return timeline
            
            # If we're at a higher level, need to traverse deeper
            timeline = {}
            self._recursive_timeline_extraction(current, timeline, 6 - len(filter_components))
            return timeline
            
        except Exception as e:
            logger.warning(f"Error extracting timeline: {e}")
            return {}
    
    def _recursive_timeline_extraction(self, data: Dict, timeline: Dict[str, int], levels_remaining: int) -> None:
        """Recursively extract timeline data from nested structure."""
        if levels_remaining == 1:  # At timestamp level
            for timestamp, timestamp_data in data.items():
                if isinstance(timestamp_data, dict):
                    if 'bags' in timestamp_data:  # Raw bucket
                        timeline[timestamp] = timeline.get(timestamp, 0) + len(timestamp_data['bags'])
                    elif 'bag_samples' in timestamp_data:  # ML bucket
                        total_samples = sum(
                            bag_data.get('label_count', 0)
                            for bag_data in timestamp_data['bag_samples'].values()
                            if isinstance(bag_data, dict)
                        )
                        timeline[timestamp] = timeline.get(timestamp, 0) + total_samples
        
        elif levels_remaining > 1:
            for key, value in data.items():
                if isinstance(value, dict):
                    self._recursive_timeline_extraction(value, timeline, levels_remaining - 1)
    
    def _count_coordinates_in_bucket(self, bucket_data: Dict) -> int:
        """Count total coordinates in bucket data."""
        count = 0
        
        def count_recursive(data: Dict, depth: int) -> int:
            if depth == 6:  # We're at coordinate level (6 levels deep)
                return 1
            
            total = 0
            for value in data.values():
                if isinstance(value, dict):
                    total += count_recursive(value, depth + 1)
            return total
        
        return count_recursive(bucket_data, 0)