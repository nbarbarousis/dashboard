# src/services/pages/temporal_coverage_service.py
import logging
import math
from typing import Dict, List, Optional


from src.models import AggregatedTemporalData, CoverageStatistics, LaserBoxStats, TemporalData
from ..coordination.data_coordination_service import DataCoordinationService


logger = logging.getLogger(__name__)


class TemporalCoverageService:
    """
    Pure Page 1 business logic - temporal coverage analysis.
    
    This service ONLY handles temporal coverage calculations and data preparation.
    It gets raw timeline data and does domain-specific processing.
    """
    
    def __init__(self, data_coordination: DataCoordinationService):
        # Only depends on coordination service, not raw cloud/local
        self.coordination = data_coordination
    
    def get_temporal_coverage_data(self, filters: Dict) -> TemporalData:
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
                    )
                
                # Get raw timeline from cloud service
                timeline = self.coordination.cloud.get_temporal_timeline(filters)
                
                if not timeline.timestamps:
                    return TemporalData(
                        timestamps=[],
                        raw_bags=[],
                        ml_samples=[],
                        coverage_percentages=[],
                    )
                
                # Calculate coverage percentages (100 - gap)
                coverage_percentages = []
                for timestamp in timeline.timestamps:
                    raw_count = timeline.raw_counts.get(timestamp, 0)
                    ml_count = timeline.ml_counts.get(timestamp, 0)
                    
                    coverage_percentages.append(
                        self._calculate_coverage(ml_count, raw_count)
                    )
                
                # Build lists in correct order
                raw_bags = [timeline.raw_counts.get(ts, 0) for ts in timeline.timestamps]
                ml_samples = [timeline.ml_counts.get(ts, 0) for ts in timeline.timestamps]
                
                return TemporalData(
                    timestamps=timeline.timestamps,
                    raw_bags=raw_bags,
                    ml_samples=ml_samples,
                    coverage_percentages=coverage_percentages,  # Now contains coverage percentages
                )
                
            except Exception as e:
                logger.error(f"Error getting temporal coverage data: {e}")
                # Return empty data on error
                return TemporalData(
                    timestamps=[],
                    raw_bags=[],
                    ml_samples=[],
                    coverage_percentages=[],
                )

    def get_coverage_statistics(self, filters: Dict) -> CoverageStatistics:
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
            temporal_data = self.get_temporal_coverage_data(filters)
            
            if not temporal_data.timestamps:
                return CoverageStatistics(
                    total_timestamps=0,
                    total_raw_bags=0,
                    total_ml_samples=0,
                    overall_coverage_pct=0.0,
                    under_labeled_count=0,
                    under_labeled_timestamps=[]
                )
            
            # Calculate statistics
            total_raw = sum(temporal_data.raw_bags)
            total_ml = sum(temporal_data.ml_samples)
            
            overall_coverage = self._calculate_coverage(total_ml, total_raw)
            
            # Find under-labeled timestamps (<70% coverage)
            under_labeled = []
            for i, timestamp in enumerate(temporal_data.timestamps):
                coverage = temporal_data.coverage_percentages[i]  # This is now coverage
                if coverage < 70:  # Less than 70% coverage
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
                under_labeled_count=0,
                under_labeled_timestamps=[]
            )


    def get_temporal_coverage_aggregated(self, filters: Dict) -> AggregatedTemporalData:
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
            lb_options = self.coordination.get_filter_options(
                'lbid',
                parent_filters={k: v for k, v in filters.items() if k != 'lbid'}
            )
            
            if not lb_options:
                return AggregatedTemporalData(
                    dates=[], raw_bags=[], ml_samples=[], coverage_percentages=[],
                    lb_breakdown={}, contributing_lbs=[])
            
            # Collect data from all laser boxes
            all_data = {}  # date -> {lb -> {bags: int, samples: int}}
            
            for lb_id in lb_options:
                lb_filters = {**filters, 'lbid': lb_id}
                timeline = self.coordination.cloud.get_temporal_timeline(lb_filters)
                
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
                    lb_breakdown={}, contributing_lbs=[])
            
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
                coverage_percentages.append(
                    self._calculate_coverage(total_samples, total_bags)
                )
            
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
            )
            
        except Exception as e:
            logger.error(f"Error getting aggregated temporal coverage data: {e}")
            return AggregatedTemporalData(
                dates=[], raw_bags=[], ml_samples=[], coverage_percentages=[],
                lb_breakdown={}, contributing_lbs=[]
            )

    def get_laser_box_statistics(self, filters: Dict) -> List[LaserBoxStats]:
        """
        Get per-laser box statistics for breakdown table.
        
        Args:
            filters: Filter selection (should have lbid=None for aggregated view)
            
        Returns:
            List of LaserBoxStats sorted by total bags descending
        """
        try:
            # Get aggregated data first
            agg_data = self.get_temporal_coverage_aggregated(filters)
            
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
                coverage_pct = self._calculate_coverage(total_samples, total_bags)
                
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
        
    def _calculate_coverage(self, actual_ml: int, raw_bags: int, expected_samples_per_bag: Optional[int] = 18) -> float:
        """Calculate coverage using improved method with hard ceiling."""
        if raw_bags == 0:
            return 100.0
        
        expected_samples = raw_bags * expected_samples_per_bag
        if expected_samples == 0:
            return 100.0
        
        ratio = actual_ml / expected_samples  # Don't cap here
        
        # Use "capped" method - makes 100% nearly impossible!
        method = "capped"
        
        if method == "capped":
            # Power method up to 85%, then heavy diminishing returns, max 95%
            if ratio <= 0.85:
                return math.pow(ratio, 0.7) * 100.0
            else:
                base_coverage = math.pow(0.85, 0.7) * 100.0  # ~91%
                remaining = 95.0 - base_coverage  # ~4% headroom
                excess_ratio = ratio - 0.85
                diminished = math.sqrt(excess_ratio / 0.5) if excess_ratio < 0.5 else 1.0
                return base_coverage + (remaining * diminished)
        else:
            # Fallback to power method
            ratio = min(ratio, 1.0)
            return math.pow(ratio, 0.7) * 100.0