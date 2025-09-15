from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import plotly.graph_objects as go

from src.models import DataStatus, ProcessingStatus, RunCoordinate
from src.models import ExtractedData

@dataclass
class AnalysisMetrics:
    """Computed metrics from analysis - pure data container."""
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
    tracking_latency_ms: Optional[List[float]] = None
    mean_tracking_latency_ms: Optional[float] = None
    
    # Detection/tracking stats
    detections_over_time: Optional[List[int]] = None
    detections_confidence_dist: Optional[List[int]] = None
    avg_detection_confidence: Optional[float] = None
    tracks_over_time: Optional[List[int]] = None
    tracks_confidence_dist: Optional[List[int]] = None
    avg_track_confidence: Optional[float] = None
    
    # Tracking statistics
    total_tracks: Optional[int] = None
    avg_track_lifetime: Optional[float] = None
    track_density_dist: Optional[Dict] = None
    track_lifecycles: Optional[pd.DataFrame] = None


@dataclass
class AnalysisPlots:
    """Container for generated Plotly figures."""
    fps_figure: Optional[go.Figure] = None
    stats_figure: Optional[go.Figure] = None
    latency_figure: Optional[go.Figure] = None
    lifecycle_figure: Optional[go.Figure] = None


@dataclass
class RunAnalysis:
    """Complete analysis result for single run."""
    coordinate: 'RunCoordinate'
    status: 'DataStatus'
    processing_status: 'ProcessingStatus' = ProcessingStatus.PENDING
    
    # Data stages
    extracted_data: Optional['ExtractedData'] = None
    metrics: Optional[AnalysisMetrics] = None
    plots: Optional[AnalysisPlots] = None
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    error_message: Optional[str] = None
    cache_path: Optional[Path] = None