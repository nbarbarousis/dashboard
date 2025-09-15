"""
DataStateService - Combines cloud and local state for page-specific views.

This implementation focuses on Page 1 (Temporal Coverage) functionality.
"""

import logging
from typing import Dict, List, Optional

from src.models import AggregatedTemporalData, LaserBoxStats
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
        2. Calculates coverage percentages (not gaps)
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
                    coverage_percentages=[],
                    expected_samples_per_bag=expected_samples_per_bag
                )
            
            # Get raw timeline from cloud service
            timeline = self.cloud.get_temporal_timeline(filters)
            
            if not timeline.timestamps:
                return TemporalData(
                    timestamps=[],
                    raw_bags=[],
                    ml_samples=[],
                    coverage_percentages=[],
                    expected_samples_per_bag=expected_samples_per_bag
                )
            
            # Calculate coverage percentages (100 - gap)
            coverage_percentages = []
            for timestamp in timeline.timestamps:
                raw_count = timeline.raw_counts.get(timestamp, 0)
                ml_count = timeline.ml_counts.get(timestamp, 0)
                
                if raw_count > 0:
                    expected = raw_count * expected_samples_per_bag
                    actual = ml_count
                    gap_pct = ((expected - actual) / expected) * 100 if expected > 0 else 0
                    gap_pct = max(0, gap_pct)  # Never negative
                    coverage_pct = 100 - gap_pct  # Convert to coverage
                    coverage_percentages.append(coverage_pct)
                else:
                    coverage_percentages.append(100)  # 100% coverage if no raw data expected
            
            # Build lists in correct order
            raw_bags = [timeline.raw_counts.get(ts, 0) for ts in timeline.timestamps]
            ml_samples = [timeline.ml_counts.get(ts, 0) for ts in timeline.timestamps]
            
            return TemporalData(
                timestamps=timeline.timestamps,
                raw_bags=raw_bags,
                ml_samples=ml_samples,
                coverage_percentages=coverage_percentages,  # Now contains coverage percentages
                expected_samples_per_bag=expected_samples_per_bag
            )
            
        except Exception as e:
            logger.error(f"Error getting temporal coverage data: {e}")
            # Return empty data on error
            return TemporalData(
                timestamps=[],
                raw_bags=[],
                ml_samples=[],
                coverage_percentages=[],
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
            
            # Average coverage (temporal_data.coverage_percentages now contains coverage)
            avg_coverage = sum(temporal_data.coverage_percentages) / len(temporal_data.coverage_percentages) if temporal_data.coverage_percentages else 0
            avg_gap = 100 - avg_coverage  # Convert back to gap for stats model
            
            # Find under-labeled timestamps (<80% coverage)
            under_labeled = []
            for i, timestamp in enumerate(temporal_data.timestamps):
                coverage = temporal_data.coverage_percentages[i]  # This is now coverage
                if coverage < 80:  # Less than 80% coverage
                    gap = 100 - coverage  # Convert to gap for storage
                    under_labeled.append((
                        timestamp,
                        gap,
                        temporal_data.raw_bags[i],
                        temporal_data.ml_samples[i]
                    ))
            
            # Sort by coverage (worst first) - convert gap back to coverage for sorting
            under_labeled.sort(key=lambda x: 100 - x[1])  # Sort by coverage ascending
            
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


    def get_temporal_coverage_aggregated(self, filters: Dict, expected_samples_per_bag: int = 17) -> AggregatedTemporalData:
        """
        Get aggregated temporal coverage data when LB = 'All'.
        
        Groups data by date across all laser boxes for the given filters.
        Only includes dates where at least one LB has data.
        
        Args:
            filters: Filter selection (should have lbid=None for aggregated view)
            expected_samples_per_bag: Expected ML samples per raw bag
            
        Returns:
            AggregatedTemporalData model ready for plotting
        """
        try:
            # Get data for all laser boxes
            lb_options = self.get_filter_options(
                'lbid',
                parent_filters={k: v for k, v in filters.items() if k != 'lbid'}
            )
            
            if not lb_options:
                return AggregatedTemporalData(
                    dates=[], raw_bags=[], ml_samples=[], coverage_percentages=[],
                    lb_breakdown={}, contributing_lbs=[], expected_samples_per_bag=expected_samples_per_bag
                )
            
            # Collect data from all laser boxes
            all_data = {}  # date -> {lb -> {bags: int, samples: int}}
            
            for lb_id in lb_options:
                lb_filters = {**filters, 'lbid': lb_id}
                timeline = self.cloud.get_temporal_timeline(lb_filters)
                
                for timestamp in timeline.timestamps:
                    # Extract date from timestamp
                    try:
                        date = timestamp.split('T')[0]  # Get YYYY-MM-DD part
                    except:
                        date = timestamp  # Fallback
                    
                    if date not in all_data:
                        all_data[date] = {}
                    
                    if lb_id not in all_data[date]:
                        all_data[date][lb_id] = {'bags': 0, 'samples': 0}
                    
                    all_data[date][lb_id]['bags'] += timeline.raw_counts.get(timestamp, 0)
                    all_data[date][lb_id]['samples'] += timeline.ml_counts.get(timestamp, 0)
            
            if not all_data:
                return AggregatedTemporalData(
                    dates=[], raw_bags=[], ml_samples=[], coverage_percentages=[],
                    lb_breakdown={}, contributing_lbs=[], expected_samples_per_bag=expected_samples_per_bag
                )
            
            # Sort dates and aggregate
            sorted_dates = sorted(all_data.keys())
            daily_bags = []
            daily_samples = []
            coverage_percentages = []
            
            for date in sorted_dates:
                total_bags = sum(lb_data['bags'] for lb_data in all_data[date].values())
                total_samples = sum(lb_data['samples'] for lb_data in all_data[date].values())
                
                daily_bags.append(total_bags)
                daily_samples.append(total_samples)
                
                # Calculate coverage for this date
                if total_bags > 0:
                    expected = total_bags * expected_samples_per_bag
                    coverage = (total_samples / expected * 100) if expected > 0 else 100
                    coverage_percentages.append(min(100, coverage))
                else:
                    coverage_percentages.append(100)
            
            # Get all contributing LBs
            contributing_lbs = list(set(
                lb_id for date_data in all_data.values() 
                for lb_id in date_data.keys()
            ))
            
            return AggregatedTemporalData(
                dates=sorted_dates,
                raw_bags=daily_bags,
                ml_samples=daily_samples,
                coverage_percentages=coverage_percentages,
                lb_breakdown=all_data,
                contributing_lbs=sorted(contributing_lbs),
                expected_samples_per_bag=expected_samples_per_bag
            )
            
        except Exception as e:
            logger.error(f"Error getting aggregated temporal coverage data: {e}")
            return AggregatedTemporalData(
                dates=[], raw_bags=[], ml_samples=[], coverage_percentages=[],
                lb_breakdown={}, contributing_lbs=[], expected_samples_per_bag=expected_samples_per_bag
            )

    def get_laser_box_statistics(self, filters: Dict, expected_samples_per_bag: int = 17) -> List[LaserBoxStats]:
        """
        Get per-laser box statistics for breakdown table.
        
        Args:
            filters: Filter selection (should have lbid=None for aggregated view)
            expected_samples_per_bag: Expected ML samples per raw bag
            
        Returns:
            List of LaserBoxStats sorted by total bags descending
        """
        try:
            # Get aggregated data first
            agg_data = self.get_temporal_coverage_aggregated(filters, expected_samples_per_bag)
            
            if not agg_data.contributing_lbs:
                return []
            
            lb_stats = []
            
            for lb_id in agg_data.contributing_lbs:
                total_bags = 0
                total_samples = 0
                active_days = 0
                
                # Sum across all dates for this LB
                for date in agg_data.dates:
                    if lb_id in agg_data.lb_breakdown.get(date, {}):
                        lb_data = agg_data.lb_breakdown[date][lb_id]
                        total_bags += lb_data['bags']
                        total_samples += lb_data['samples']
                        if lb_data['bags'] > 0:  # Count as active day if has bags
                            active_days += 1
                
                # Calculate metrics
                coverage_pct = 0
                if total_bags > 0:
                    expected = total_bags * expected_samples_per_bag
                    coverage_pct = (total_samples / expected * 100) if expected > 0 else 0
                
                avg_bags_per_day = total_bags / active_days if active_days > 0 else 0
                avg_samples_per_day = total_samples / active_days if active_days > 0 else 0
                
                lb_stats.append(LaserBoxStats(
                    lb_id=lb_id,
                    total_bags=total_bags,
                    total_samples=total_samples,
                    coverage_pct=coverage_pct,
                    active_days=active_days,
                    avg_bags_per_day=avg_bags_per_day,
                    avg_samples_per_day=avg_samples_per_day
                ))
            
            # Sort by total bags descending
            return sorted(lb_stats, key=lambda x: x.total_bags, reverse=True)
            
        except Exception as e:
            logger.error(f"Error getting laser box statistics: {e}")
            return []
    
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