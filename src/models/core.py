# Proposed Data Models Organization

# ============================================================================
# src/models/core.py - Core domain models
# ============================================================================

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Tuple

@dataclass
class RunCoordinate:
    """Primary addressing system for all operations."""
    cid: str
    regionid: str
    fieldid: str
    twid: str
    lbid: str
    timestamp: str
    
    def to_path_str(self, separator: str = "/") -> str:
        """Convert coordinate to path string format."""
        return separator.join([
            self.cid, self.regionid, self.fieldid, 
            self.twid, self.lbid, self.timestamp
        ])
    
    def to_path_tuple(self) -> Tuple[str, ...]:
        """Convert coordinate to tuple format."""
        return (self.cid, self.regionid, self.fieldid, self.twid, self.lbid, self.timestamp)
    
    def to_dict(self) -> Dict[str, str]:
        """Convert coordinate to dictionary format."""
        return {
            'cid': self.cid, 'regionid': self.regionid, 'fieldid': self.fieldid,
            'twid': self.twid, 'lbid': self.lbid, 'timestamp': self.timestamp
        }
    
    @classmethod
    def from_filters(cls, filters: Dict[str, str], timestamp: str) -> 'RunCoordinate':
        """Create RunCoordinate from filter dictionary and timestamp."""
        return cls(
            cid=filters.get('cid', ''), regionid=filters.get('regionid', ''),
            fieldid=filters.get('fieldid', ''), twid=filters.get('twid', ''),
            lbid=filters.get('lbid', ''), timestamp=timestamp
        )


class DataStatus(Enum):
    """Standardized status vocabulary across the system."""
    NOT_DOWNLOADED = "not_downloaded"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    EXTRACTING = "extracting"
    EXTRACTED = "extracted"
    ANALYZING = "analyzing"
    ANALYZED = "analyzed"
    ERROR = "error"


class ProcessingStatus(Enum):
    """Operation status tracking."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    FAILED = "failed"
    CACHED = "cached"