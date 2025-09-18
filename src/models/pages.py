from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from src.models import RunCoordinate, CloudRawStatus, CloudMLStatus, LocalRawStatus, LocalMLStatus

@dataclass
class TemporalData:
    """Model for Page 1 temporal coverage visualization."""
    timestamps: List[str]
    raw_bags: List[int]
    ml_samples: List[int]
    coverage_percentages: List[float]

@dataclass
class AggregatedTemporalData:
    """Model for aggregated temporal data when LB = 'All'."""
    dates: List[str]  # "2024-03-15" format
    raw_bags: List[int]  # Total bags per date across all LBs
    ml_samples: List[int]  # Total samples per date across all LBs
    coverage_percentages: List[float]  # Coverage per date
    lb_breakdown: Dict[str, Dict[str, int]]  # date -> {lb -> {bags: int, samples: int}}
    contributing_lbs: List[str]  # All LBs that contributed data

@dataclass
class LaserBoxStats:
    """Per-laser box statistics for breakdown table."""
    lb_id: str
    total_bags: int
    total_samples: int
    coverage_pct: float
    active_days: int
    avg_bags_per_day: float
    avg_samples_per_day: float

@dataclass
class CoverageStatistics:
    """Model for Page 1 summary metrics."""
    total_timestamps: int
    total_raw_bags: int
    total_ml_samples: int
    overall_coverage_pct: float
    under_labeled_count: int
    under_labeled_timestamps: List[Tuple[str, float, int, int]]


@dataclass
class TimelineData:
    """Raw timeline data extracted from cloud cache."""
    timestamps: List[str]
    raw_counts: Dict[str, int]  # timestamp -> bag_count
    ml_counts: Dict[str, int]   # timestamp -> sample_count

@dataclass
class InventoryItem:
    """Enhanced inventory item with comparison tags"""
    coord: RunCoordinate
    # Raw status (for Raw data views)
    cloud_raw_status: Optional[CloudRawStatus] = None
    local_raw_status: Optional[LocalRawStatus] = None
    # ML status (for ML data views)  
    cloud_ml_status: Optional[CloudMLStatus] = None
    local_ml_status: Optional[LocalMLStatus] = None
    
    # Sync status tags
    raw_sync_status: str = "unknown"  # "synced", "cloud_only", "local_only", "mismatch", "missing"
    ml_sync_status: str = "unknown"   # same values
    raw_issues: List[str] = field(default_factory=list)  # Specific issues for details
    ml_issues: List[str] = field(default_factory=list)   # Specific issues for details
