from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .core import DataStatus, RunCoordinate

@dataclass
class CloudRawStatus:
    """Enhanced cloud raw data status."""
    exists: bool
    bag_count: int
    bag_names: List[str]  # Discovered bag names in cloud format
    bag_sizes: Dict[str, int]  # bag_name -> size_bytes
    total_size: int

@dataclass
class CloudMLStatus:
    """Enhanced cloud ML data status."""
    exists: bool
    total_samples: int
    bag_samples: Dict[str, Dict]  # bag_name -> {frame_count: int, label_count: int}
    bag_files: Dict[str, Dict[str, Dict[str, int]]]  # bag_name -> {frames: {filename: size}, labels: {filename: size}}

@dataclass
class LocalRawStatus:
    """Local raw data status."""
    downloaded: bool  # Keep "downloaded" - makes sense for local raw data
    bag_count: int
    bag_names: List[str]  # Local format bag names
    bag_sizes: Dict[str, int]  # bag_name -> size_bytes  
    total_size: int

@dataclass
class LocalMLStatus:
    """Local ML data status."""
    downloaded: bool  # Match local raw pattern
    total_samples: int
    bag_samples: Dict[str, Dict]  # bag_name -> {frame_count: int, label_count: int}
    bag_files: Dict[str, Dict[str, Dict[str, int]]]  # bag_name -> {frames: {filename: size}, labels: {filename: size}}
    file_counts: Dict[str, int]  # extension -> count (keep for summary info)

# Keep existing models that don't need changes
@dataclass
class ExtractionStatus:
    """Local extraction status for a coordinate."""
    extracted: bool
    files: Dict[str, bool] = field(default_factory=dict)
    path: Optional[str] = None

@dataclass
class ExtractionDetails:
    """Detailed extraction information for a coordinate."""
    exists: bool
    path: Optional[str]
    subdirectories: List[str]
    total_files: int
    total_size: int
    file_details: Dict[str, Dict]  # relative_path -> {size: int, type: str}

@dataclass
class ExportInfo:
    """Export tracking information."""
    exists: bool
    export_id: Optional[str] = None
    info: Optional[Dict] = None
    error: Optional[str] = None

@dataclass
class RunState:
    """Complete state model for individual run."""
    coordinate: 'RunCoordinate'
    cloud_raw_status: CloudRawStatus
    cloud_ml_status: CloudMLStatus
    local_raw_status: LocalRawStatus
    extraction_status: ExtractionStatus
    pipeline_status: 'DataStatus'
    next_action: str  # "download", "extract", "analyze", "complete"
    ready_for_analysis: bool