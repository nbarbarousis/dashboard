# src/models/data_models.py
"""
Data models for the dashboard - defines core data structures
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path
from enum import Enum
from datetime import datetime
import pandas as pd
import plotly.graph_objects as go

class DataStatus(Enum):
    """Status of data availability for a run"""
    NOT_DOWNLOADED = "not_downloaded"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    EXTRACTING = "extracting"
    EXTRACTED = "extracted"
    ANALYZING = "analyzing"
    ANALYZED = "analyzed"
    ERROR = "error"


class ProcessingStatus(Enum):
    """Status of processing operations"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    FAILED = "failed"
    CACHED = "cached"


@dataclass
class RunCoordinate:
    """
    Identifies a specific run/timestamp in the hierarchy.
    Simplified version for dashboard use (not the full BagCoord from pipeline).
    """
    cid: str
    regionid: str
    fieldid: str
    twid: str
    lbid: str
    timestamp: str
    bag_indices: Optional[List[int]] = None
    
    def to_path_tuple(self) -> tuple:
        """Convert to tuple for path construction"""
        return (self.cid, self.regionid, self.fieldid, 
                self.twid, self.lbid, self.timestamp)
    
    def to_path_str(self, separator: str = "/") -> str:
        """Convert to path string"""
        return separator.join(self.to_path_tuple())
    
    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary"""
        return {
            'cid': self.cid,
            'regionid': self.regionid,
            'fieldid': self.fieldid,
            'twid': self.twid,
            'lbid': self.lbid,
            'timestamp': self.timestamp
        }
    
    @classmethod
    def from_filters(cls, filters: Dict, timestamp: str) -> 'RunCoordinate':
        """Create from filter selections"""
        return cls(
            cid=filters.get('client'),
            regionid=filters.get('region'),
            fieldid=filters.get('field'),
            twid=filters.get('tw'),
            lbid=filters.get('lb'),
            timestamp=timestamp
        )


@dataclass
class ExtractedData:
    """Container for extracted rosbag data"""
    frames_df: Optional[pd.DataFrame] = None
    detections_df: Optional[pd.DataFrame] = None
    tracking_df: Optional[pd.DataFrame] = None
    detections_json: Optional[Dict] = None
    tracking_json: Optional[Dict] = None
    metadata: Optional[Dict] = None
    extraction_time: Optional[datetime] = None
    source_bags: List[str] = field(default_factory=list)


@dataclass
class AnalysisMetrics:
    """Computed metrics from rosbag analysis"""
    # FPS metrics
    frame_fps_instant: Optional[List[float]] = None
    frame_fps_rolling: Optional[List[float]] = None

    detection_fps_instant: Optional[List[float]] = None
    detection_fps_rolling: Optional[List[float]] = None

    tracking_fps_instant: Optional[List[float]] = None
    tracking_fps_rolling: Optional[List[float]] = None
    
    # Latency metrics
    detection_latency_ms: Optional[List[float]] = None
    mean_detection_latency_ms: Optional[float] = None
    max_detection_latency_ms: Optional[float] = None
    
    # Detection statistics
    total_detections: Optional[int] = None
    avg_detections_per_frame: Optional[float] = None
    detection_confidence_dist: Optional[Dict] = None
    
    # Tracking statistics
    total_tracks: Optional[int] = None
    avg_track_lifetime: Optional[float] = None
    track_density_dist: Optional[Dict] = None
    
    # Computation metadata
    computation_time: Optional[datetime] = None
    metrics_version: str = "1.0"


@dataclass
class AnalysisPlots:
    """Container for generated analysis plots"""
    fps_figure: go.Figure = None  
    stats_figure: Optional[Dict] = None
    latency_figure: Optional[Dict] = None
    lifecycle_figure: Optional[Dict] = None
    # Additional plots can be added here


@dataclass
class RunAnalysis:
    """Complete analysis result for a single run"""
    coordinate: RunCoordinate
    status: DataStatus
    processing_status: ProcessingStatus = ProcessingStatus.PENDING
    
    # Data stages
    extracted_data: Optional[ExtractedData] = None
    metrics: Optional[AnalysisMetrics] = None
    plots: Optional[AnalysisPlots] = None
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    error_message: Optional[str] = None
    cache_path: Optional[Path] = None
    
    def is_ready_for_analysis(self) -> bool:
        """Check if data is ready for analysis"""
        return (self.status in [DataStatus.EXTRACTED, DataStatus.ANALYZED] and 
                self.extracted_data is not None)
    
    def is_cached(self) -> bool:
        """Check if analysis is cached"""
        return self.processing_status == ProcessingStatus.CACHED and self.cache_path and self.cache_path.exists()


@dataclass
class DownloadJob:
    """Represents a download operation"""
    job_id: str
    coordinate: RunCoordinate
    source_bucket: str
    target_path: Path
    files_to_download: List[str] = field(default_factory=list)
    
    # Progress tracking
    total_files: int = 0
    files_downloaded: int = 0
    total_bytes: int = 0
    bytes_downloaded: int = 0
    
    # Status
    status: ProcessingStatus = ProcessingStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    
    @property
    def progress_percent(self) -> float:
        """Calculate download progress percentage"""
        if self.total_bytes == 0:
            return 0.0
        return (self.bytes_downloaded / self.total_bytes) * 100


@dataclass
class ExtractionJob:
    """Represents an extraction operation"""
    job_id: str
    coordinate: RunCoordinate
    source_path: Path
    output_path: Path
    
    # Configuration
    extract_params: Dict[str, Any] = field(default_factory=dict)
    
    # Progress tracking  
    total_bags: int = 0
    bags_processed: int = 0
    frames_extracted: int = 0
    detections_extracted: int = 0
    
    # Status
    status: ProcessingStatus = ProcessingStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    docker_output: Optional[str] = None


@dataclass
class MLDatasetInfo:
    """Information about ML datasets"""
    dataset_name: str
    export_id: Optional[str] = None
    source_path: Optional[Path] = None
    
    # Statistics
    total_frames: int = 0
    total_labels: int = 0
    total_detections: int = 0
    
    # Location info
    cloud_bucket: Optional[str] = None
    cloud_path: Optional[str] = None
    local_path: Optional[Path] = None
    
    # Metadata
    created_at: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    dataset_type: str = "annotated"  # "annotated", "raw", "augmented"


@dataclass
class ServiceConfig:
    """Configuration for services"""
    # Paths
    raw_root: Path
    processed_root: Path
    cache_root: Path
    ml_root: Path
    
    # Cloud settings
    bucket_names: Dict[str, str]
    
    # Processing settings
    extract_every_n_frames: int = 5
    docker_image_name: str = "rosbag-extractor"
    enable_caching: bool = True
    cache_ttl_hours: int = 24
    
    # Analysis settings
    fps_rolling_window: int = 25
    latency_threshold_ms: float = 100.0
    confidence_threshold: float = 0.5
    
    @classmethod
    def from_dashboard_config(cls, config: Any, user_paths: Dict[str, Path]) -> 'ServiceConfig':
        """Create from dashboard config and user paths"""
        return cls(
            raw_root=user_paths.get('raw_root', Path('data/raw')),
            processed_root=user_paths.get('processed_root', Path('data/processed')),
            cache_root=user_paths.get('cache_root', Path('data/cache')),
            ml_root=user_paths.get('ml_root', Path('data/ML')),
            bucket_names=config.bucket_names,
            extract_every_n_frames=config.extraction_params.get('every', 5) if hasattr(config, 'extraction_params') else 5
        )