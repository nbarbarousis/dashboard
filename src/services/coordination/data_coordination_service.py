"""
DataStateService - Combines cloud and local state for page-specific views.

This implementation focuses on Page 1 (Temporal Coverage) functionality.
"""

import logging
import math
from typing import Dict, List, Optional, Tuple

from src.models import AggregatedTemporalData, LaserBoxStats
from src.models import RunCoordinate
from src.models import TemporalData, CoverageStatistics, TimelineData
from ..core.cloud_inventory_service import CloudInventoryService
from ..core.local_state_service import LocalStateService

logger = logging.getLogger(__name__)


class DataCoordinationService:
    """
    Coordinates cloud and local services for general infrastructure needs.
    
    This service provides the "glue" between cloud and local state that's 
    needed by multiple pages and UI components. It's NOT page-specific.
    """
 
    def __init__(self, cloud_service: CloudInventoryService, local_service: LocalStateService):
        """Initialize with cloud and local services."""
        self.cloud = cloud_service
        self.local = local_service
    
    def get_filter_options(self, level: str, parent_filters: Optional[Dict] = None) -> List[str]:
        """
        Get available options for filter dropdown.
        
        This replaces the logic that was in HierarchicalFilters component.
        
        Args:
            level: Filter level ('cid', 'regionid', 'fieldid', 'twid', 'lbid')
            parent_filters: Already selected parent filters
            
        Returns:
            List of available options for the dropdown
        """
        try:
            # Build parent path from filters
            parent_path = []
            if parent_filters:
                # Order matters for hierarchy
                hierarchy_order = ['cid', 'regionid', 'fieldid', 'twid', 'lbid']
                
                for filter_level in hierarchy_order:
                    if filter_level in parent_filters and parent_filters[filter_level]:
                        parent_path.append(parent_filters[filter_level])
                    else:
                        break  # Stop at first missing level
            
            # Get options from cloud service (cache-based)
            options = self.cloud.get_hierarchy_level(level, tuple(parent_path))
            
            return sorted(options)
            
        except Exception as e:
            logger.error(f"Error getting filter options for {level}: {e}")
            return []
    
    def get_hierarchy_for_filters(self) -> Dict:
        """
        Get the complete hierarchy structure for filter dropdowns.
        
        This provides all available filter options in a nested structure.
        This replaces the logic that was spread across DataService and filters.
        
        Returns:
            Nested dict with hierarchy: {cid: {regionid: {fieldid: {twid: {lbid: [timestamps]}}}}}
        """
        try:
            # Get full inventory from cloud service
            inventory = self.cloud.get_full_inventory()
            
            if 'raw' not in inventory:
                return {}
            
            # The raw bucket data is already in the correct hierarchical format
            # Just return it directly
            return inventory['raw']
            
        except Exception as e:
            logger.error(f"Error getting hierarchy for filters: {e}")
            return {}

    def _build_parent_path(self, filters: Optional[Dict]) -> Tuple[str, ...]:
        """Helper to build hierarchy path from filters."""
        if not filters:
            return ()
        
        path = []
        for level in ['cid', 'regionid', 'fieldid', 'twid', 'lbid']:
            if filters.get(level):
                path.append(filters[level])
            else:
                break
        return tuple(path)
        
