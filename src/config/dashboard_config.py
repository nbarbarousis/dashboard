# src/config/dashboard_config.py
"""
Dashboard configuration settings - enhanced version
"""

from dataclasses import dataclass, field
from typing import Dict, Optional
from pathlib import Path


@dataclass
class DashboardConfig:
    """Enhanced configuration for the data dashboard"""
    
    # Cloud bucket configuration
    bucket_names: Dict[str, str]
    
    # Path configurations
    raw_data_path: str = "/home/nikbarb/data-annot-pipeline/data/raw/"
    processed_data_path: str = "/home/nikbarb/data-annot-pipeline/data/processed/"
    cache_path: str = "cache/gcs_data.json"
    ml_data_path: str = "/home/nikbarb/data-annot-pipeline/data/ML/"
    
    # UI settings
    default_page: str = "Temporal Coverage"
    refresh_on_startup: bool = False
    
    # Analytics settings
    enable_analytics: bool = True
    enable_caching: bool = True
    
    # Extraction settings
    extraction_params: Dict = field(default_factory=lambda: {
        'topics': {
            'img_topic': '/sensors/triton_camera_feed/compressed',
            'det_topic': '/weed_detection/bboxes',
            'track_topic': '/multi_object_tracking/track_bboxes'
        }
    })
    
    # Docker settings
    docker_image_name: str = "rosbag-extractor"
    docker_auto_build: bool = False
    
    # Analysis settings
    analysis_params: Dict = field(default_factory=lambda: {
        'fps_rolling_window': 25,
        'latency_threshold_ms': 100.0,
        'confidence_threshold': 0.5,
        'track_density_threshold': 0.5
    })
    
    # Download settings
    download_params: Dict = field(default_factory=lambda: {
        'conflict_resolution': 'skip',  # 'skip', 'overwrite', 'verify'
        'parallel_downloads': 4,
        'chunk_size': 1024 * 1024  # 1MB chunks
    })
    
    # ML settings
    ml_params: Dict = field(default_factory=lambda: {
        'default_export_path': 'data/ML/raw/',
        'default_training_path': 'data/ML/trainings/',
        'enable_ml_upload': True,
        'enable_ml_download': True
    })
    
    def get_user_paths(self) -> Dict[str, Path]:
        """Get user paths as Path objects"""
        return {
            'raw_root': Path(self.raw_data_path),
            'processed_root': Path(self.processed_data_path),
            'cache_root': Path(self.cache_path),
            'ml_root': Path(self.ml_data_path)
        }
    
    def validate(self) -> bool:
        """Validate configuration"""
        # Check required bucket names
        if 'raw' not in self.bucket_names or 'ml' not in self.bucket_names:
            return False
        
        # Check paths are valid
        for path_str in [self.raw_data_path, self.processed_data_path, 
                         self.cache_path, self.ml_data_path]:
            try:
                Path(path_str)
            except Exception:
                return False
        
        return True


# Default configuration
DEFAULT_CONFIG = DashboardConfig(
    bucket_names={
        'raw': 'terra-weeder-deployments-data-raw',
        'ml': 'terra-weeder-deployments-data-ml'
    },
    default_page="Temporal Coverage",
    refresh_on_startup=False,
    enable_analytics=True,
    enable_caching=True,
    raw_data_path="/home/nikbarb/data-annot-pipeline/data/raw/",
    processed_data_path="/home/nikbarb/data-annot-pipeline/data/processed/",
    cache_path="cache/gcs_data.json",
    ml_data_path="/home/nikbarb/data-annot-pipeline/data/ML/"
)