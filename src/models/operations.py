from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from .core import ProcessingStatus, RunCoordinate

@dataclass
class DownloadJob:
    """Single coordinate download operation."""
    job_id: str
    coordinate: 'RunCoordinate'
    source_bucket: str
    target_path: Path
    files_to_download: List[str] = field(default_factory=list)
    
    # Progress tracking
    total_files: int = 0
    files_downloaded: int = 0
    total_bytes: int = 0
    bytes_downloaded: int = 0
    
    # Status
    status: 'ProcessingStatus' = ProcessingStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    
    @property
    def progress_percent(self) -> float:
        if self.total_bytes == 0:
            return 0.0
        return (self.bytes_downloaded / self.total_bytes) * 100


@dataclass
class ExtractionJob:
    """Single coordinate extraction operation."""
    job_id: str
    coordinate: 'RunCoordinate'
    source_path: Path
    output_path: Path
    
    # Progress tracking
    total_bags: int = 0
    bags_processed: int = 0
    frames_extracted: int = 0
    
    # Status
    status: 'ProcessingStatus' = ProcessingStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    docker_output: Optional[str] = None


@dataclass
class OperationResult:
    """Generic operation result container."""
    success: bool
    result: Optional[any] = None
    error: Optional[str] = None
    warning: Optional[str] = None
    critical: bool = False