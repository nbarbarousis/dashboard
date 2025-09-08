# src/config/dashboard_config.py
"""
Dashboard configuration settings
"""

from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class DashboardConfig:
    """Configuration for the data dashboard"""
    
    # Cloud bucket configuration
    bucket_names: Dict[str, str]
    
    # UI settings
    default_page: str = "Temporal Coverage"
    refresh_on_startup: bool = False
    
    # Analytics settings
    analytics_root_path: Optional[str] = None
    enable_analytics: bool = True
    
    # Download/upload settings
    default_download_path: str = "data/raw/"
    default_ml_path: str = "data/ML/raw/"
    default_training_path: str = "data/ML/trainings/"

# Default configuration
DEFAULT_CONFIG = DashboardConfig(
    bucket_names={
        'raw': 'terra-weeder-deployments-data-raw',
        'ml': 'terra-weeder-deployments-data-ml'
    },
    default_page="Temporal Coverage",
    refresh_on_startup=False,
    analytics_root_path=None,
    enable_analytics=True,
    default_download_path="data/raw/",
    default_ml_path="data/ML/raw/",
    default_training_path="data/ML/trainings/"
)