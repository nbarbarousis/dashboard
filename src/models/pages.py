from dataclasses import dataclass
from typing import Dict, List, Tuple

@dataclass
class TemporalData:
    """Model for Page 1 temporal coverage visualization."""
    timestamps: List[str]
    raw_bags: List[int]
    ml_samples: List[int]
    coverage_percentages: List[float]
    expected_samples_per_bag: int


@dataclass
class CoverageStatistics:
    """Model for Page 1 summary metrics."""
    total_timestamps: int
    total_raw_bags: int
    total_ml_samples: int
    overall_coverage_pct: float
    average_gap_pct: float
    under_labeled_count: int
    under_labeled_timestamps: List[Tuple[str, float, int, int]]


@dataclass
class TimelineData:
    """Raw timeline data extracted from cloud cache."""
    timestamps: List[str]
    raw_counts: Dict[str, int]  # timestamp -> bag_count
    ml_counts: Dict[str, int]   # timestamp -> sample_count
