# src/services/data_service.py
"""
Data service - handles business logic for filtering and aggregating discovered data
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class DataService:
    """
    Handles business logic for data filtering and aggregation.
    Single responsibility: Transform raw GCS data into UI-ready format.
    """
    
    def __init__(self, gcs_data: Dict):
        """
        Initialize with raw discovered data from GCSService
        
        Args:
            gcs_data: Raw hierarchy data with counts from GCS discovery
        """
        self.raw_data = gcs_data.get('raw', {})
        self.ml_data = gcs_data.get('ml', {})
        
        if not self.raw_data:
            logger.warning("No raw data provided to DataService")
        if not self.ml_data:
            logger.warning("No ML data provided to DataService")
    
    def get_hierarchy_for_filters(self) -> Dict:
        """
        Extract hierarchy structure for filter dropdowns
        
        Returns:
            Nested dict for cascading filter options
        """
        hierarchy = {}
        
        for client in self.raw_data:
            hierarchy[client] = {}
            for region in self.raw_data[client]:
                hierarchy[client][region] = {}
                for field in self.raw_data[client][region]:
                    hierarchy[client][region][field] = {}
                    for tw in self.raw_data[client][region][field]:
                        hierarchy[client][region][field][tw] = {}
                        for lb in self.raw_data[client][region][field][tw]:
                            timestamps = list(self.raw_data[client][region][field][tw][lb].keys())
                            hierarchy[client][region][field][tw][lb] = sorted(timestamps)
        
        return hierarchy
    
    def get_temporal_data(self, filters: Dict, expected_samples_per_bag: int = 85) -> Dict:
        """
        Get temporal coverage data for specific filter selection
        
        Args:
            filters: Dict with keys: client, region, field, tw, lb
            expected_samples_per_bag: Expected number of ML samples per raw bag
            
        Returns:
            Dict with timestamps, raw_bags, ml_samples, gap_percentages for plotting
            
        Raises:
            ValueError: If required filters are missing or no data found
        """
        # Validate all required filters are present
        required_filters = ['client', 'region', 'field', 'tw', 'lb']
        missing_filters = [f for f in required_filters if not filters.get(f)]
        
        if missing_filters:
            raise ValueError(f"Missing required filters: {missing_filters}")
        
        # Extract filter values
        client = filters['client']
        region = filters['region']
        field = filters['field']
        tw = filters['tw']
        lb = filters['lb']
        
        # Get raw data for this path
        raw_timestamp_data = self._get_raw_data_for_path(client, region, field, tw, lb)
        if not raw_timestamp_data:
            raise ValueError(f"No raw data found for {client}/{region}/{field}/{tw}/{lb}")
        
        # Get ML data for this path
        ml_timestamp_data = self._get_ml_data_for_path(client, region, field, tw, lb)
        
        # Build temporal data
        all_timestamps = set(raw_timestamp_data.keys()) | set(ml_timestamp_data.keys())
        sorted_timestamps = self._sort_timestamps(list(all_timestamps))
        
        raw_bags = [raw_timestamp_data.get(ts, {}).get('bag_count', 0) for ts in sorted_timestamps]
        ml_samples = [self._sum_ml_samples(ml_timestamp_data.get(ts, {})) for ts in sorted_timestamps]
        
        # Calculate gap percentages based on expected samples per bag
        gap_percentages = self._calculate_expected_gaps(raw_bags, ml_samples, expected_samples_per_bag)
        
        return {
            'timestamps': sorted_timestamps,
            'raw_bags': raw_bags,
            'ml_samples': ml_samples,
            'gap_percentages': gap_percentages,
            'expected_samples_per_bag': expected_samples_per_bag
        }
    
    def _get_raw_data_for_path(self, client: str, region: str, field: str, tw: str, lb: str) -> Dict:
        """Get raw data for specific hierarchy path"""
        try:
            return self.raw_data[client][region][field][tw][lb]
        except KeyError:
            return {}
    
    def _get_ml_data_for_path(self, client: str, region: str, field: str, tw: str, lb: str) -> Dict:
        """Get ML data for specific hierarchy path"""
        try:
            return self.ml_data[client][region][field][tw][lb]
        except KeyError:
            return {}
    
    def _sum_ml_samples(self, timestamp_data: Dict) -> int:
        """Sum ML samples across all bags for a timestamp"""
        if not timestamp_data or 'bag_samples' not in timestamp_data:
            return 0
        
        bag_samples = timestamp_data['bag_samples']
        return sum(bag_samples.values()) if bag_samples else 0
    
    def _sort_timestamps(self, timestamps: List[str]) -> List[str]:
        """Sort timestamps chronologically"""
        def parse_timestamp(ts):
            try:
                normalized = ts.replace('_', '-')
                return datetime.fromisoformat(normalized.replace('Z', '+00:00'))
            except:
                return datetime.min
        
        return sorted(timestamps, key=parse_timestamp)
    
    def validate_filter_path(self, filters: Dict) -> Optional[str]:
        """
        Validate that a filter path exists in the data
        
        Args:
            filters: Partial or complete filter dict
            
        Returns:
            None if path is valid, error message if invalid
        """
        try:
            data = self.raw_data
            
            if filters.get('client'):
                if filters['client'] not in data:
                    return f"Client '{filters['client']}' not found"
                data = data[filters['client']]
                
                if filters.get('region'):
                    if filters['region'] not in data:
                        return f"Region '{filters['region']}' not found for client '{filters['client']}'"
                    data = data[filters['region']]
                    
                    if filters.get('field'):
                        if filters['field'] not in data:
                            return f"Field '{filters['field']}' not found"
                        data = data[filters['field']]
                        
                        if filters.get('tw'):
                            if filters['tw'] not in data:
                                return f"TW '{filters['tw']}' not found"
                            data = data[filters['tw']]
                            
                            if filters.get('lb'):
                                if filters['lb'] not in data:
                                    return f"LB '{filters['lb']}' not found"
            
            return None  # Path is valid
            
        except Exception as e:
            return f"Error validating path: {e}"
    
    def _calculate_expected_gaps(self, raw_bags: List[int], ml_samples: List[int], expected_samples_per_bag: int) -> List[float]:
        """
        Calculate gap percentages based on expected samples per bag
        
        Args:
            raw_bags: List of raw bag counts per timestamp
            ml_samples: List of ML sample counts per timestamp  
            expected_samples_per_bag: Expected number of samples per bag
            
        Returns:
            List of gap percentages (0-100)
        """
        gap_percentages = []
        
        for raw, ml in zip(raw_bags, ml_samples):
            if raw > 0:
                expected_total_samples = raw * expected_samples_per_bag
                gap_pct = max(0, (expected_total_samples - ml) / expected_total_samples * 100)
            else:
                # No raw bags but have ML samples = 0% gap
                # No raw bags and no ML samples = 100% gap (no data at all)
                gap_pct = 100 if ml == 0 else 0
            
            gap_percentages.append(round(gap_pct, 1))
        
        return gap_percentages
    
    def get_coverage_statistics(self, filters: Dict, expected_samples_per_bag: int = 17) -> Dict:
        """
        Get detailed coverage statistics for the selected filters
        
        Args:
            filters: Filter selection
            expected_samples_per_bag: Expected samples per bag
            
        Returns:
            Dictionary with detailed statistics
        """
        temporal_data = self.get_temporal_data(filters, expected_samples_per_bag)
        
        raw_bags = temporal_data['raw_bags']
        ml_samples = temporal_data['ml_samples']
        gap_percentages = temporal_data['gap_percentages']
        
        total_raw_bags = sum(raw_bags)
        total_ml_samples = sum(ml_samples)
        expected_total_samples = total_raw_bags * expected_samples_per_bag
        
        # Find under-labeled timestamps (high gap percentages)
        under_labeled_threshold = 20  # 20% gap threshold
        under_labeled_timestamps = [
            (temporal_data['timestamps'][i], gap_percentages[i], raw_bags[i], ml_samples[i])
            for i, gap in enumerate(gap_percentages) 
            if gap > under_labeled_threshold and raw_bags[i] > 0
        ]
        
        return {
            'total_timestamps': len(temporal_data['timestamps']),
            'total_raw_bags': total_raw_bags,
            'total_ml_samples': total_ml_samples,
            'expected_total_samples': expected_total_samples,
            'overall_coverage_pct': (total_ml_samples / expected_total_samples * 100) if expected_total_samples > 0 else 0,
            'average_gap_pct': sum(gap_percentages) / len(gap_percentages) if gap_percentages else 0,
            'under_labeled_timestamps': under_labeled_timestamps,
            'under_labeled_count': len(under_labeled_timestamps)
        }