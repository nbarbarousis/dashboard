# src/services/gcs_service.py
"""
Google Cloud Storage service - handles bucket discovery and caching
Pure service layer with no UI dependencies
"""

import logging
import re
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Callable
from collections import defaultdict

from google.cloud import storage

logger = logging.getLogger(__name__)

# Timestamp regex pattern
TS_RE = re.compile(r'^\d{4}[_-]\d{2}[_-]\d{2}T\d{2}:\d{2}:\d{2}Z$')

class GCSService:
    """
    Google Cloud Storage service for bucket discovery and caching.
    UI-agnostic service that discovers bucket structure and caches results.
    """
    
    def __init__(self, bucket_names: Dict[str, str], cache_file: str = "cache/gcs_data.json"):
        """
        Initialize GCS service
        
        Args:
            bucket_names: Dict mapping bucket types ('raw', 'ml') to bucket names
            cache_file: Path to cache file for storing discovery results
        """
        self.bucket_names = bucket_names
        self.cache_file = Path(cache_file)
        self._discovered_data = {}
        self._cache_info = {}
        
        # Ensure cache directory exists
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize GCS client and buckets
        try:
            self.gcs_client = storage.Client()
            self.buckets = {}
            for bucket_type, bucket_name in bucket_names.items():
                self.buckets[bucket_type] = self.gcs_client.bucket(bucket_name)
            logger.info(f"Initialized GCS client for buckets: {list(bucket_names.values())}")
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            raise RuntimeError(f"GCS initialization failed: {e}")
    
    def discover_and_cache(self, force_refresh: bool = False, progress_callback: Optional[Callable[[str], None]] = None) -> Dict:
        """
        Discover bucket data and cache results
        
        Args:
            force_refresh: If True, bypass cache and perform fresh discovery
            progress_callback: Optional callback to report discovery progress
            
        Returns:
            Dictionary containing discovered bucket data
        """
        # Try to load from cache first
        if not force_refresh and self._load_from_cache():
            logger.info("Loaded data from cache")
            if progress_callback:
                progress_callback("Loaded from cache")
            return self._discovered_data
        
        # Perform fresh discovery
        logger.info("Performing fresh bucket discovery...")
        if progress_callback:
            progress_callback("Starting fresh discovery...")
        
        self._discover_fresh_data(progress_callback)
        self._save_to_cache()
        
        return self._discovered_data
    
    def get_cached_data(self) -> Dict:
        """
        Get cached discovery data
        
        Returns:
            Dictionary containing cached bucket data, or empty dict if not cached
        """
        if not self._discovered_data and self._load_from_cache():
            logger.info("Loaded data from cache")
        
        return self._discovered_data
    
    def is_cache_valid(self, max_age_hours: int = 24) -> bool:
        """
        Check if cached data is valid and recent
        
        Args:
            max_age_hours: Maximum age of cache in hours
            
        Returns:
            True if cache is valid and recent, False otherwise
        """
        if not self._cache_info or not self.cache_file.exists():
            return False
        
        try:
            cache_time = datetime.fromisoformat(self._cache_info['timestamp'])
            age = datetime.now() - cache_time
            return age.total_seconds() < (max_age_hours * 3600)
        except Exception:
            return False
    
    def get_cache_info(self) -> Dict:
        """
        Get information about cached data
        
        Returns:
            Dictionary with cache status and metadata
        """
        if not self._cache_info:
            return {'cached': False}
        
        try:
            cache_time = datetime.fromisoformat(self._cache_info['timestamp'])
            age = datetime.now() - cache_time
            
            return {
                'cached': True,
                'timestamp': cache_time,
                'age_minutes': int(age.total_seconds() / 60),
                'age_hours': int(age.total_seconds() / 3600),
                'cache_file': str(self.cache_file),
                'bucket_names': self.bucket_names
            }
        except Exception:
            return {'cached': False}
    
    def clear_cache(self):
        """Clear cached data and remove cache file"""
        self._discovered_data = {}
        self._cache_info = {}
        
        if self.cache_file.exists():
            self.cache_file.unlink()
            logger.info(f"Cache file {self.cache_file} removed")
    
    # Private methods for discovery and caching
    
    def _discover_fresh_data(self, progress_callback: Optional[Callable[[str], None]] = None):
        """Perform fresh discovery of both buckets"""
        
        if progress_callback:
            progress_callback("Discovering raw bucket...")
        logger.info("Discovering raw bucket...")
        self._discovered_data['raw'] = self._discover_raw_bucket()
        
        if progress_callback:
            progress_callback("Discovering ML bucket...")
        logger.info("Discovering ML bucket...")
        self._discovered_data['ml'] = self._discover_ml_bucket()
        
        if progress_callback:
            progress_callback("Discovery complete")
        logger.info("Bucket discovery completed")
    
    def _discover_raw_bucket(self) -> Dict:
        """Discover raw bucket: count .bag files per timestamp"""
        bucket = self.buckets['raw']
        data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))))
        
        clients = self._list_directories(bucket, '')
        logger.info(f"Found {len(clients)} clients in raw bucket")
        
        for client in clients:
            regions = self._list_directories(bucket, f"{client}/")
            
            for region in regions:
                fields = self._list_directories(bucket, f"{client}/{region}/")
                
                for field in fields:
                    tws = self._list_directories(bucket, f"{client}/{region}/{field}/")
                    
                    for tw in tws:
                        lbs = self._list_directories(bucket, f"{client}/{region}/{field}/{tw}/")
                        
                        for lb in lbs:
                            timestamps = self._list_directories(bucket, f"{client}/{region}/{field}/{tw}/{lb}/")
                            valid_timestamps = [ts for ts in timestamps if self._is_valid_timestamp(ts)]
                            
                            for timestamp in valid_timestamps:
                                bag_count = self._count_raw_bags(bucket, client, region, field, tw, lb, timestamp)
                                if bag_count > 0:
                                    data[client][region][field][tw][lb][timestamp] = {'bag_count': bag_count}
        
        return dict(data)
    
    def _discover_ml_bucket(self) -> Dict:
        """Discover ML bucket: count .jpg files per bag per timestamp"""
        bucket = self.buckets['ml']
        data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))))
        
        # Check for 'raw/' prefix
        top_dirs = self._list_directories(bucket, '')
        if 'raw' not in top_dirs:
            logger.warning("No 'raw/' directory found in ML bucket")
            return {}
        
        clients = self._list_directories(bucket, 'raw/')
        logger.info(f"Found {len(clients)} clients in ML bucket")
        
        for client in clients:
            regions = self._list_directories(bucket, f"raw/{client}/")
            
            for region in regions:
                fields = self._list_directories(bucket, f"raw/{client}/{region}/")
                
                for field in fields:
                    tws = self._list_directories(bucket, f"raw/{client}/{region}/{field}/")
                    
                    for tw in tws:
                        lbs = self._list_directories(bucket, f"raw/{client}/{region}/{field}/{tw}/")
                        
                        for lb in lbs:
                            timestamps = self._list_directories(bucket, f"raw/{client}/{region}/{field}/{tw}/{lb}/")
                            valid_timestamps = [ts for ts in timestamps if self._is_valid_timestamp(ts)]
                            
                            for timestamp in valid_timestamps:
                                bag_samples = self._count_ml_samples_per_bag(bucket, client, region, field, tw, lb, timestamp)
                                if bag_samples:
                                    data[client][region][field][tw][lb][timestamp] = {'bag_samples': bag_samples}
        
        return dict(data)
    
    def _count_raw_bags(self, bucket, client, region, field, tw, lb, timestamp) -> int:
        """Count .bag files for a specific timestamp"""
        prefix = f"{client}/{region}/{field}/{tw}/{lb}/{timestamp}/rosbag/"
        return sum(1 for blob in bucket.list_blobs(prefix=prefix) if blob.name.endswith('.bag'))
    
    def _count_ml_samples_per_bag(self, bucket, client, region, field, tw, lb, timestamp) -> Dict[str, int]:
        """Count .jpg files per bag directory for a timestamp"""
        prefix = f"raw/{client}/{region}/{field}/{tw}/{lb}/{timestamp}/rosbag/"
        
        bag_dirs = self._list_directories(bucket, prefix)
        bag_samples = {}
        
        for bag_dir in bag_dirs:
            if bag_dir.startswith('_'):  # ML bag directories start with _
                frames_prefix = f"{prefix}{bag_dir}/frames/"
                jpg_count = sum(1 for blob in bucket.list_blobs(prefix=frames_prefix)
                               if blob.name.endswith('.jpg'))
                if jpg_count > 0:
                    bag_samples[bag_dir] = jpg_count
        
        return bag_samples
    
    def _list_directories(self, bucket, prefix: str) -> List[str]:
        """List directories at prefix level using delimiter"""
        try:
            iterator = bucket.list_blobs(prefix=prefix, delimiter='/')
            prefixes = set()
            
            for page in iterator.pages:
                if hasattr(page, 'prefixes') and page.prefixes:
                    prefixes.update(page.prefixes)
            
            directories = []
            for prefix_path in prefixes:
                if prefix_path.startswith(prefix):
                    dir_name = prefix_path[len(prefix):].rstrip('/')
                    if dir_name:
                        directories.append(dir_name)
            
            return sorted(directories)
            
        except Exception as e:
            logger.error(f"Error listing directories for prefix '{prefix}': {e}")
            return []
    
    def _is_valid_timestamp(self, timestamp: str) -> bool:
        """Check if string matches timestamp format"""
        return bool(TS_RE.match(timestamp))
    
    def _load_from_cache(self) -> bool:
        """Load discovered data from cache file"""
        try:
            if not self.cache_file.exists():
                return False
            
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            # Validate cache structure
            required_keys = ['discovered_data', 'cache_info', 'bucket_names']
            if not all(key in cache_data for key in required_keys):
                logger.warning("Invalid cache structure - missing required keys")
                return False
            
            if cache_data['bucket_names'] != self.bucket_names:
                logger.warning("Cache bucket names don't match current configuration")
                return False
            
            self._discovered_data = cache_data['discovered_data']
            self._cache_info = cache_data['cache_info']
            return True
            
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            return False
    
    def _save_to_cache(self):
        """Save discovered data to cache file"""
        try:
            cache_data = {
                'discovered_data': self._discovered_data,
                'cache_info': {
                    'timestamp': datetime.now().isoformat(),
                    'bucket_names': self.bucket_names,
                    'version': '1.0'
                },
                'bucket_names': self.bucket_names
            }
            
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            self._cache_info = cache_data['cache_info']
            logger.info(f"Cache saved to {self.cache_file}")
            
        except Exception as e:
            logger.error(f"Error saving cache: {e}")
