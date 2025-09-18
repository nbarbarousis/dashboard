# src/services/pages/operations_view_service.py
"""
Simple InventoryViewService:
 - Build inventory once, filter from cache.
"""

import logging
from typing import Dict, List, Optional

from src.models import RunCoordinate, InventoryItem
from ..coordination.data_coordination_service import DataCoordinationService

logger = logging.getLogger(__name__)


class InventoryViewService:

    """Simple service for operations page - builds complete inventory once."""
    
    def __init__(self, data_coordination: DataCoordinationService):
        self.coordination = data_coordination
        # Direct access to cloud/local for bulk operations
        self.cloud = data_coordination.cloud
        self.local = data_coordination.local
        
        # Inventory cache
        self._inventory_items: Optional[List[InventoryItem]] = None
    
    def get_inventory_items(self, filters: Dict) -> List[InventoryItem]:
        """
        Get inventory items matching the filters.
        
        Builds complete inventory once, then filters based on user selection.
        """
        try:
            # Build inventory if not cached
            if self._inventory_items is None:
                self._build_complete_inventory()
            
            # Filter based on user filters
            filtered_items = self._filter_inventory(self._inventory_items, filters)
            
            logger.info(f"Returning {len(filtered_items)} items for filters: {filters}")
            return filtered_items
            
        except Exception as e:
            logger.error(f"Error getting inventory items: {e}")
            return []
    
    def refresh_inventory(self):
        """Force refresh of inventory cache."""
        self._inventory_items = None
    
    def _build_complete_inventory(self):
        """Build complete inventory from all cloud and local data."""
        logger.info("Building complete inventory...")
        
        # Get ALL statuses from both cloud and local
        cloud_raw_statuses = self.cloud.get_all_raw_statuses()
        cloud_ml_statuses = self.cloud.get_all_ml_statuses()
        local_raw_statuses = self.local.get_all_raw_statuses()
        local_ml_statuses = self.local.get_all_ml_statuses()
        
        # Get all unique coordinates
        all_coords = set()
        all_coords.update(cloud_raw_statuses.keys())
        all_coords.update(cloud_ml_statuses.keys())
        all_coords.update(local_raw_statuses.keys())
        all_coords.update(local_ml_statuses.keys())
        
        # Build inventory items
        items = []
        for coord in all_coords:
            item = InventoryItem(
                coord=RunCoordinate.from_path_str(coord),
                cloud_raw_status=cloud_raw_statuses.get(coord),
                local_raw_status=local_raw_statuses.get(coord),
                cloud_ml_status=cloud_ml_statuses.get(coord),
                local_ml_status=local_ml_statuses.get(coord)
            )
            
            # Add sync status tags
            item.raw_sync_status = self._determine_raw_sync_status(item)
            item.ml_sync_status = self._determine_ml_sync_status(item)
            item.raw_issues = self._get_raw_issues(item)
            item.ml_issues = self._get_ml_issues(item)
            
            items.append(item)
        
        self._inventory_items = items
        logger.info(f"Built inventory with {len(items)} total coordinates")
    
    def _determine_raw_sync_status(self, item: InventoryItem) -> str:
        """Determine raw data sync status."""
        cloud_exists = item.cloud_raw_status and item.cloud_raw_status.exists
        local_exists = item.local_raw_status and item.local_raw_status.downloaded
        
        if not cloud_exists and not local_exists:
            return "missing"
        elif cloud_exists and not local_exists:
            return "cloud_only"
        elif local_exists and not cloud_exists:
            return "local_only"
        elif cloud_exists and local_exists:
            # Check if they match
            if self._raw_data_matches(item):
                return "synced"
            else:
                return "mismatch"
        return "unknown"
    
    def _determine_ml_sync_status(self, item: InventoryItem) -> str:
        """Determine ML data sync status."""
        cloud_exists = item.cloud_ml_status and item.cloud_ml_status.exists
        local_exists = item.local_ml_status and item.local_ml_status.downloaded
        
        if not cloud_exists and not local_exists:
            return "missing"
        elif cloud_exists and not local_exists:
            return "cloud_only"
        elif local_exists and not cloud_exists:
            return "local_only"
        elif cloud_exists and local_exists:
            # Check if they match
            if self._ml_data_matches(item):
                return "synced"
            else:
                return "mismatch"
        return "unknown"
    
    def _raw_data_matches(self, item: InventoryItem) -> bool:
        """Check if raw data matches between cloud and local."""
        cloud = item.cloud_raw_status
        local = item.local_raw_status
        
        if not cloud or not local:
            return False
        
        # Check bag count
        if cloud.bag_count != local.bag_count:
            return False
        
        # Check total size
        if cloud.total_size != local.total_size:
            return False
        
        # Check bag names with translation
        cloud_bags_translated = set()
        for cloud_bag in cloud.bag_names:
            local_bag = self.local.path_builder.translate_bag_name_cloud_to_local(cloud_bag)
            cloud_bags_translated.add(local_bag)
        
        if cloud_bags_translated != set(local.bag_names):
            return False
        
        # Check individual bag sizes
        for cloud_bag, cloud_size in cloud.bag_sizes.items():
            local_bag = self.local.path_builder.translate_bag_name_cloud_to_local(cloud_bag)
            if local_bag not in local.bag_sizes or local.bag_sizes[local_bag] != cloud_size:
                return False
        
        return True
    
    def _ml_data_matches(self, item: InventoryItem) -> bool:
        """Check if ML data matches between cloud and local."""
        cloud = item.cloud_ml_status
        local = item.local_ml_status
        
        if not cloud or not local:
            return False
        
        # Check total samples
        if cloud.total_samples != local.total_samples:
            return False
        
        # Translate cloud bag names to local format for comparison
        cloud_bags_translated = {}
        for cloud_bag in cloud.bag_samples.keys():
            local_bag = self.local.path_builder.translate_bag_name_cloud_to_local(cloud_bag)
            cloud_bags_translated[local_bag] = cloud_bag
        
        # Check bag samples structure (using translated names)
        if set(cloud_bags_translated.keys()) != set(local.bag_samples.keys()):
            return False
        
        # Check individual bag sample counts
        for local_bag, cloud_bag in cloud_bags_translated.items():
            if cloud.bag_samples[cloud_bag] != local.bag_samples.get(local_bag):
                return False
        
        # Check file structures (filenames and sizes within each bag)
        for local_bag, cloud_bag in cloud_bags_translated.items():
            if cloud_bag in cloud.bag_files and local_bag in local.bag_files:
                cloud_files = cloud.bag_files[cloud_bag]
                local_files = local.bag_files[local_bag]
                
                for file_type in ['frames', 'labels']:
                    if file_type in cloud_files and file_type in local_files:
                        cloud_type_files = cloud_files[file_type]
                        local_type_files = local_files[file_type]
                        
                        # Check if same files exist
                        if set(cloud_type_files.keys()) != set(local_type_files.keys()):
                            return False
                        
                        # Check file sizes match
                        for filename in cloud_type_files.keys():
                            if cloud_type_files[filename] != local_type_files.get(filename):
                                return False
                    elif file_type in cloud_files or file_type in local_files:
                        # One has the file type, the other doesn't
                        return False
        
        return True
    
    def _get_raw_issues(self, item: InventoryItem) -> List[str]:
        """Get specific raw data issues."""
        issues = []
        cloud = item.cloud_raw_status
        local = item.local_raw_status
        
        if item.raw_sync_status == "cloud_only":
            issues.append("Not downloaded locally")
        elif item.raw_sync_status == "local_only":
            issues.append("Not available in cloud")
        elif item.raw_sync_status == "mismatch" and cloud and local:
            if cloud.bag_count != local.bag_count:
                issues.append(f"Bag count mismatch: cloud={cloud.bag_count}, local={local.bag_count}")
            if cloud.total_size != local.total_size:
                issues.append(f"Size mismatch: cloud={cloud.total_size}, local={local.total_size}")
            # Add more specific issues as needed
        
        return issues
    
    def _get_ml_issues(self, item: InventoryItem) -> List[str]:
        """Get specific ML data issues."""
        issues = []
        cloud = item.cloud_ml_status
        local = item.local_ml_status
        
        if item.ml_sync_status == "cloud_only":
            issues.append("Not downloaded locally")
        elif item.ml_sync_status == "local_only":
            issues.append("Not uploaded to cloud")
        elif item.ml_sync_status == "mismatch" and cloud and local:
            if cloud.total_samples != local.total_samples:
                issues.append(f"Sample count mismatch: cloud={cloud.total_samples}, local={local.total_samples}")
            # Add more specific issues as needed
        
        return issues
    
    def _filter_inventory(self, items: List[InventoryItem], filters: Dict) -> List[InventoryItem]:
        """Filter inventory items based on user filters."""
        filtered = []
        
        for item in items:
            coord = item.coord
            
            # Check each filter level
            if filters.get('cid') and coord.cid != filters['cid']:
                continue
            if filters.get('regionid') and coord.regionid != filters['regionid']:
                continue
            if filters.get('fieldid') and coord.fieldid != filters['fieldid']:
                continue
            if filters.get('twid') and coord.twid != filters['twid']:
                continue
            if filters.get('lbid') and coord.lbid != filters['lbid']:
                continue
            
            filtered.append(item)
        
        return filtered
    def get_simple_metrics(self, items: List[InventoryItem]) -> Dict[str, int]:
        """Get simple 4-value metrics: cloud bags, local bags, cloud samples, local samples."""
        metrics = {
            'cloud_bags': 0,
            'local_bags': 0,
            'cloud_samples': 0,
            'local_samples': 0
        }
        
        for item in items:
            # Cloud bags (raw + ml)
            if item.cloud_raw_status and item.cloud_raw_status.exists:
                metrics['cloud_bags'] += item.cloud_raw_status.bag_count
            
            # Local bags (raw + ml)  
            if item.local_raw_status and item.local_raw_status.downloaded:
                metrics['local_bags'] += item.local_raw_status.bag_count
            
            # Cloud ML samples
            if item.cloud_ml_status and item.cloud_ml_status.exists:
                metrics['cloud_samples'] += item.cloud_ml_status.total_samples
            
            # Local ML samples
            if item.local_ml_status and item.local_ml_status.downloaded:
                metrics['local_samples'] += item.local_ml_status.total_samples
        
        return metrics