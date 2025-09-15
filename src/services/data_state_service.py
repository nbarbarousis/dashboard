"""
DataStateService - Combines cloud and local state for page-specific views.

This implementation focuses on Page 1 (Temporal Coverage) functionality.
"""

import logging
from typing import Dict, List, Optional

from src.models import RunCoordinate
from src.models import TemporalData, CoverageStatistics, TimelineData
from src.services.cloud_inventory_service import CloudInventoryService
from src.services.local_state_service import LocalStateService

logger = logging.getLogger(__name__)


class DataStateService:
    """
    Combines cloud and local state to provide page-specific data views.
    This is the primary service that UI pages interact with.
    """
    
    def __init__(self, cloud_service: CloudInventoryService, local_service: LocalStateService):
        """Initialize with cloud and local services."""
        self.cloud = cloud_service
        self.local = local_service
    
    # ========================================================================
    # Page 1: Temporal Coverage Support
    # ========================================================================
    
    def get_temporal_coverage_data(self, filters: Dict, expected_samples_per_bag: int = 17) -> TemporalData:
        """
        Get temporal coverage data for plotting.
        
        This method:
        1. Gets timeline data from cloud cache
        2. Calculates gap percentages
        3. Returns structured data for plotting
        
        Args:
            filters: Filter selection from UI with keys 'cid', 'regionid', 'fieldid', 'twid', 'lbid'
            expected_samples_per_bag: Expected ML samples per raw bag
            
        Returns:
            TemporalData model ready for plotting
        """
        try:
            # Validate filters
            required_keys = ['cid', 'regionid', 'fieldid', 'twid', 'lbid']
            if not all(filters.get(k) for k in required_keys):
                logger.warning(f"Incomplete filters provided: {filters}")
                return TemporalData(
                    timestamps=[],
                    raw_bags=[],
                    ml_samples=[],
                    gap_percentages=[],
                    expected_samples_per_bag=expected_samples_per_bag
                )
            
            # Get raw timeline from cloud service
            timeline = self.cloud.get_temporal_timeline(filters)
            
            if not timeline.timestamps:
                return TemporalData(
                    timestamps=[],
                    raw_bags=[],
                    ml_samples=[],
                    gap_percentages=[],
                    expected_samples_per_bag=expected_samples_per_bag
                )
            
            # Calculate gap percentages
            gap_percentages = []
            for timestamp in timeline.timestamps:
                raw_count = timeline.raw_counts.get(timestamp, 0)
                ml_count = timeline.ml_counts.get(timestamp, 0)
                
                if raw_count > 0:
                    expected = raw_count * expected_samples_per_bag
                    actual = ml_count
                    gap_pct = ((expected - actual) / expected) * 100 if expected > 0 else 0
                    gap_percentages.append(max(0, gap_pct))  # Never negative
                else:
                    gap_percentages.append(0)  # No gap if no raw data
            
            # Build lists in correct order
            raw_bags = [timeline.raw_counts.get(ts, 0) for ts in timeline.timestamps]
            ml_samples = [timeline.ml_counts.get(ts, 0) for ts in timeline.timestamps]
            
            return TemporalData(
                timestamps=timeline.timestamps,
                raw_bags=raw_bags,
                ml_samples=ml_samples,
                gap_percentages=gap_percentages,
                expected_samples_per_bag=expected_samples_per_bag
            )
            
        except Exception as e:
            logger.error(f"Error getting temporal coverage data: {e}")
            # Return empty data on error
            return TemporalData(
                timestamps=[],
                raw_bags=[],
                ml_samples=[],
                gap_percentages=[],
                expected_samples_per_bag=expected_samples_per_bag
            )
    
    def get_coverage_statistics(self, filters: Dict, expected_samples_per_bag: int = 17) -> CoverageStatistics:
        """
        Get detailed coverage statistics for summary display.
        
        Args:
            filters: Filter selection from UI
            expected_samples_per_bag: Expected ML samples per raw bag
            
        Returns:
            CoverageStatistics model with summary metrics
        """
        try:
            # Get temporal data first
            temporal_data = self.get_temporal_coverage_data(filters, expected_samples_per_bag)
            
            if not temporal_data.timestamps:
                return CoverageStatistics(
                    total_timestamps=0,
                    total_raw_bags=0,
                    total_ml_samples=0,
                    overall_coverage_pct=0.0,
                    average_gap_pct=0.0,
                    under_labeled_count=0,
                    under_labeled_timestamps=[]
                )
            
            # Calculate statistics
            total_raw = sum(temporal_data.raw_bags)
            total_ml = sum(temporal_data.ml_samples)
            
            # Overall coverage
            total_expected = total_raw * expected_samples_per_bag
            overall_coverage = (total_ml / total_expected * 100) if total_expected > 0 else 0
            
            # Average gap
            avg_gap = sum(temporal_data.gap_percentages) / len(temporal_data.gap_percentages) if temporal_data.gap_percentages else 0
            
            # Find under-labeled timestamps (>20% gap)
            under_labeled = []
            for i, timestamp in enumerate(temporal_data.timestamps):
                gap = temporal_data.gap_percentages[i]
                if gap > 20:  # More than 20% gap
                    under_labeled.append((
                        timestamp,
                        gap,
                        temporal_data.raw_bags[i],
                        temporal_data.ml_samples[i]
                    ))
            
            return CoverageStatistics(
                total_timestamps=len(temporal_data.timestamps),
                total_raw_bags=total_raw,
                total_ml_samples=total_ml,
                overall_coverage_pct=overall_coverage,
                average_gap_pct=avg_gap,
                under_labeled_count=len(under_labeled),
                under_labeled_timestamps=under_labeled
            )
            
        except Exception as e:
            logger.error(f"Error calculating coverage statistics: {e}")
            return CoverageStatistics(
                total_timestamps=0,
                total_raw_bags=0,
                total_ml_samples=0,
                overall_coverage_pct=0.0,
                average_gap_pct=0.0,
                under_labeled_count=0,
                under_labeled_timestamps=[]
            )
    
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