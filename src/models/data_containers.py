from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd

@dataclass
class ExtractedData:
    """Container for all extracted rosbag data."""
    frames_df: Optional[pd.DataFrame] = None
    detections_df: Optional[pd.DataFrame] = None
    tracking_df: Optional[pd.DataFrame] = None
    detections_json: Optional[Dict] = None
    tracking_json: Optional[Dict] = None
    metadata: Optional[Dict] = None
    extraction_time: Optional[datetime] = None
    source_bags: List[str] = field(default_factory=list)