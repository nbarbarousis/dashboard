from dataclasses import dataclass, field
from typing import Dict, List, Optional

from src.models import DataStatus, RunCoordinate

@dataclass
class CloudRawStatus:
    """Cloud raw data status for a coordinate."""
    exists: bool
    bag_count: int
    bag_names: List[str]
    total_size: int


@dataclass
class CloudMLStatus:
    """Cloud ML data status for a coordinate."""
    exists: bool
    total_samples: int
    bag_samples: Dict[str, Dict]  # bag_name -> {frame_count: int, label_count: int}


@dataclass
class LocalRawStatus:
    """Local raw data status for a coordinate."""
    downloaded: bool
    bag_count: int
    bag_names: List[str]
    total_size: int
    path: Optional[str] = None


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
class LocalMLStatus:
    """Local ML export status for a coordinate."""
    exists: bool
    path: Optional[str]
    file_counts: Dict[str, int]  # extension -> count
    total_size: int
    subdirectories: List[str]
    sample_files: List[Dict]  # List of {path: str, size: int, extension: str}


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
