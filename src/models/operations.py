from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .core import ProcessingStatus, RunCoordinate


# @dataclass
# class ExtractionJob:
#     """Single coordinate extraction operation."""
#     job_id: str
#     coordinate: 'RunCoordinate'
#     source_path: Path
#     output_path: Path
    
#     # Progress tracking
#     total_bags: int = 0
#     bags_processed: int = 0
#     frames_extracted: int = 0
    
#     # Status
#     status: 'ProcessingStatus' = ProcessingStatus.PENDING
#     started_at: Optional[datetime] = None
#     completed_at: Optional[datetime] = None
#     error_message: Optional[str] = None
#     docker_output: Optional[str] = None


@dataclass
class TransferJob:
    """Specification for any cloud transfer operation."""
    coordinate: RunCoordinate
    operation_type: str  # "raw_download", "ml_upload", etc.
    selection_criteria: Dict  # Flexible criteria per operation
    conflict_resolution: str = "skip"  # "skip", "overwrite"
    dry_run: bool = True

@dataclass 
class TransferPlan:
    """Detailed plan for any transfer operation."""
    coordinate: RunCoordinate
    files_to_transfer: List[Dict]
    files_to_skip: List[Dict]
    conflicts: List[Dict]
    total_size: int
    total_files: int

@dataclass
class OperationResult:
    """Generic operation result container."""
    success: bool
    result: Optional[any] = None
    error: Optional[str] = None
    warning: Optional[str] = None
    critical: bool = False