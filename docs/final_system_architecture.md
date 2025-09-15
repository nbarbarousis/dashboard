# Final System Architecture Blueprint

## Overview

This document defines the complete technical architecture for the refactored dashboard system. The architecture eliminates batch operations complexity in favor of atomic operations that are simpler to implement, test, and maintain.

## Core Design Principles

1. **Atomic Operations Only**: All operations work on single coordinates, eliminating batch complexity
2. **Models as Contracts**: Data models define all interfaces between components
3. **Single Responsibility Services**: Each service has one clear purpose
4. **Cache-First Architecture**: Comprehensive caching with atomic queries as needed
5. **Fail-Fast Error Handling**: Clear warnings, continue unless critical failure

## System Components

### Data Models Layer

#### Core Domain Models

```python
@dataclass
class RunCoordinate:
    """
    Primary addressing system - every operation uses this to identify data.
    """
    cid: str
    regionid: str
    fieldid: str
    twid: str
    lbid: str
    timestamp: str
    
    def to_path_str(self, separator: str = "/") -> str
    def to_path_tuple(self) -> Tuple[str, ...]
    def to_dict(self) -> Dict[str, str]
    
    @classmethod
    def from_filters(cls, filters: Dict, timestamp: str) -> 'RunCoordinate'

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
```

#### Data Container Models

```python
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

@dataclass
class AnalysisMetrics:
    """Computed metrics from analysis - pure data container."""
    # FPS metrics
    frame_fps_instant: Optional[List[float]] = None
    frame_fps_rolling: Optional[List[float]] = None
    detection_fps_instant: Optional[List[float]] = None
    detection_fps_rolling: Optional[List[float]] = None
    
    # Latency metrics
    detection_latency_ms: Optional[List[float]] = None
    mean_detection_latency_ms: Optional[float] = None
    
    # Detection/tracking stats
    detections_over_time: Optional[List[int]] = None
    tracks_over_time: Optional[List[int]] = None
    total_tracks: Optional[int] = None
    avg_track_lifetime: Optional[float] = None

@dataclass
class AnalysisPlots:
    """Container for generated Plotly figures."""
    fps_figure: Optional[go.Figure] = None
    stats_figure: Optional[go.Figure] = None
    latency_figure: Optional[go.Figure] = None
    lifecycle_figure: Optional[go.Figure] = None
```

#### State Models

```python
@dataclass
class CloudRawStatus:
    """Cloud raw data status for a coordinate."""
    exists: bool
    bag_count: int
    bag_names: List[str]
    total_size: int

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
    files: Dict[str, bool]  # {"frames.csv": True, "detections.csv": True, ...}
    path: Optional[str] = None

@dataclass
class RunState:
    """Complete state model for individual run - central to Page 2."""
    coordinate: RunCoordinate
    cloud_raw_status: CloudRawStatus
    local_raw_status: LocalRawStatus
    extraction_status: ExtractionStatus
    pipeline_status: DataStatus
    next_action: str  # "download", "extract", "analyze", "complete"
    ready_for_analysis: bool
```

#### Operation Models

```python
@dataclass
class DownloadJob:
    """Single coordinate download operation."""
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
        if self.total_bytes == 0:
            return 0.0
        return (self.bytes_downloaded / self.total_bytes) * 100

@dataclass
class ExtractionJob:
    """Single coordinate extraction operation."""
    job_id: str
    coordinate: RunCoordinate
    source_path: Path
    output_path: Path
    
    # Progress tracking
    total_bags: int = 0
    bags_processed: int = 0
    frames_extracted: int = 0
    
    # Status
    status: ProcessingStatus = ProcessingStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    docker_output: Optional[str] = None

@dataclass
class RunAnalysis:
    """Complete analysis result for single run."""
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
```

#### Page-Specific Models

```python
@dataclass
class TemporalData:
    """Model for Page 1 temporal coverage visualization."""
    timestamps: List[str]
    raw_bags: List[int]
    ml_samples: List[int]
    gap_percentages: List[float]
    expected_samples_per_bag: int

@dataclass
class CoverageStatistics:
    """Model for Page 1 summary metrics."""
    total_timestamps: int
    total_raw_bags: int
    total_ml_samples: int
    overall_coverage_pct: float
    average_gap_pct: float
    under_labeled_count: int
    under_labeled_timestamps: List[Tuple[str, float, int, int]]
```

### Services Layer

#### CloudInventoryService

**Purpose**: Master cloud data service with comprehensive caching.

```python
class CloudInventoryService:
    def __init__(self, bucket_names: Dict[str, str], cache_file: str):
        self.bucket_names = bucket_names
        self.cache_file = cache_file
        self._inventory_cache: Optional[Dict] = None
        self._is_stale: Boolean = False
    
    # Cache Management
    def get_full_inventory(self, force_refresh: bool = False) -> Dict:
        """Get complete cloud inventory with all filenames and metadata."""
        
    def refresh_inventory(self) -> None:
        """Force refresh of complete inventory from cloud."""
        
    def get_cache_info(self) -> Dict:
        """Get cache metadata (age, size, last update)."""
    
    def mark_stale(self, reason: str = "external_change")
        """Mark cache stale - will refresh on next access"""
    
    # Hierarchy Queries
    def get_hierarchy_level(self, level: str, parent_path: Tuple[str, ...] = ()) -> List[str]:
        """Get available options at specific hierarchy level."""
        
    def path_exists(self, path: Tuple[str, ...], bucket_type: str = 'raw') -> bool:
        """Check if specific hierarchy path exists."""
    
    # Coordinate Queries (Atomic Only)
    def get_raw_bags_info(self, coord: RunCoordinate) -> CloudRawStatus:
        """Get raw bag info for single coordinate from cache."""
        
    def get_ml_samples_info(self, coord: RunCoordinate) -> Dict:
        """Get ML sample info for single coordinate from cache."""
        
    def get_coordinate_cloud_state(self, coord: RunCoordinate) -> Dict:
        """Get complete cloud state for single coordinate."""
    
    # Temporal Data (Page 1 Specific)
    def get_temporal_data(self, filters: Dict) -> Dict:
        """Extract temporal data for specific filter path from cache."""
```

#### LocalStateService

**Purpose**: Track all local filesystem state.

```python
class LocalStateService:
    def __init__(self, raw_root: Path, processed_root: Path, ml_root: Path):
        self.raw_root = raw_root
        self.processed_root = processed_root
        self.ml_root = ml_root
    
    # Raw Data Queries (Atomic)
    def check_raw_downloaded(self, coord: RunCoordinate) -> LocalRawStatus:
        """Check if raw bags are downloaded locally for single coordinate."""
        
    def check_extracted(self, coord: RunCoordinate) -> ExtractionStatus:
        """Check extraction status for single coordinate."""
        
    def get_extraction_output_info(self, coord: RunCoordinate) -> Dict:
        """Get detailed info about extracted files for single coordinate."""
    
    # ML Data Queries
    def check_ml_exported(self, coord: RunCoordinate) -> Dict:
        """Check if ML samples exist in local export structure."""
        
    def get_export_ids(self) -> List[str]:
        """Get available export IDs from .export_tracking.json."""
        
    def get_export_info(self, export_id: str) -> Dict:
        """Get detailed export information."""
```

#### DataStateService

**Purpose**: Combines cloud and local state for page-specific views.

```python
class DataStateService:
    def __init__(self, cloud_service: CloudInventoryService, local_service: LocalStateService):
        self.cloud = cloud_service
        self.local = local_service
    
    # Page 1: Temporal Coverage
    def get_temporal_coverage_data(self, filters: Dict, expected_samples_per_bag: int = 17) -> TemporalData:
        """Get temporal coverage data for plotting."""
        
    def get_coverage_statistics(self, filters: Dict, expected_samples_per_bag: int = 17) -> CoverageStatistics:
        """Get detailed coverage statistics for summary."""
        
    def get_filter_options(self, level: str, parent_filters: Dict = None) -> List[str]:
        """Get available options for filter dropdown."""
    
    # Page 2: Per-Run Analysis
    def get_complete_run_state(self, coord: RunCoordinate) -> RunState:
        """Get complete state for single run - core of Page 2."""
        
    def get_available_timestamps(self, filters: Dict) -> List[str]:
        """Get available timestamps for filter selection."""
    
    # Page 3: Download Manager  
    def generate_coordinates_from_filters(self, filters: Dict) -> List[RunCoordinate]:
        """Generate coordinate list from filter selection."""
        
    def get_state_overview_data(self, coords: List[RunCoordinate]) -> List[Dict]:
        """Get state data for overview table (iterate atomically)."""
```

#### CloudOperationService

**Purpose**: All cloud operations - atomic only.

```python
class CloudOperationService:
    def __init__(self, cloud_service: CloudInventoryService, local_service: LocalStateService,
                 bucket_names: Dict[str, str], path_manager):
        self.cloud = cloud_service
        self.local = local_service
        self.bucket_names = bucket_names
        self.path_manager = path_manager
        self.active_jobs: Dict[str, Any] = {}
    
    # Raw Downloads (Atomic)
    def plan_raw_download(self, coord: RunCoordinate, 
                         conflict_resolution: str = "skip") -> Dict:
        """Plan raw download operation for single coordinate."""
        
    def execute_raw_download(self, coord: RunCoordinate,
                           conflict_resolution: str = "skip",
                           dry_run: bool = True) -> DownloadJob:
        """Execute raw download for single coordinate."""
    
    # ML Uploads (Atomic)
    def plan_ml_upload(self, export_id: str,
                      conflict_resolution: str = "skip") -> Dict:
        """Plan ML upload operation."""
        
    def execute_ml_upload(self, export_id: str,
                         conflict_resolution: str = "skip",
                         dry_run: bool = True) -> Dict:
        """Execute ML upload."""
    
    # Dataset Fetch (Atomic - works on filter selection)
    def plan_dataset_fetch(self, dataset_name: str, filters: Dict,
                          expected_samples_per_bag: int = 17) -> Dict:
        """Plan dataset fetch operation."""
        
    def execute_dataset_fetch(self, plan: Dict, dry_run: bool = True) -> Dict:
        """Execute dataset fetch operation."""
    
    # Job Management
    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get status of active job."""
        
    def cancel_job(self, job_id: str) -> bool:
        """Cancel active job."""
```

#### ExtractionService

**Purpose**: Docker-based extraction operations.

```python
class ExtractionService:
    def __init__(self, docker_image: str, local_service: LocalStateService):
        self.docker_image = docker_image
        self.local = local_service
    
    def check_extraction_possible(self, coord: RunCoordinate) -> bool:
        """Check if extraction is possible for single coordinate."""
        
    def execute_extraction(self, coord: RunCoordinate) -> ExtractionJob:
        """Execute extraction for single coordinate."""
        
    def get_extraction_progress(self, job_id: str) -> Dict:
        """Get progress of extraction job."""
```

#### AnalyticsService

**Purpose**: Analysis operations with caching.

```python
class AnalyticsService:
    def __init__(self, local_service: LocalStateService, processed_root: Path):
        self.local = local_service
        self.processed_root = processed_root
    
    def check_analysis_possible(self, coord: RunCoordinate) -> bool:
        """Check if analysis is possible (data extracted)."""
        
    def get_cached_analysis(self, coord: RunCoordinate) -> Optional[RunAnalysis]:
        """Get cached analysis if available."""
        
    def analyze_run(self, coord: RunCoordinate, force_refresh: bool = False) -> RunAnalysis:
        """Execute complete analysis pipeline for single run."""
```

### Service Container

```python
@dataclass
class ServiceContainer:
    """Simple service container - no fancy DI."""
    # Core data services
    cloud_inventory: CloudInventoryService
    local_state: LocalStateService
    data_state: DataStateService
    
    # Operation services
    cloud_operations: CloudOperationService
    extraction: ExtractionService
    analytics: AnalyticsService
    
    @classmethod
    def initialize(cls, config: DashboardConfig) -> 'ServiceContainer':
        """Initialize all services with configuration."""
        
    def warm_up(self) -> None:
        """Warm up services (load caches, etc.)."""
```

### Pages Layer

#### Page 1: Temporal Coverage

```python
class TemporalCoveragePage:
    def __init__(self, services: ServiceContainer):
        self.services = services
    
    def render(self) -> None:
        """
        Render temporal coverage analysis page.
        
        Flow:
        1. Get global filters from sidebar
        2. Validate all filters selected
        3. Get temporal data and statistics (2 service calls)
        4. Render plots and summary
        
        Service Dependencies: DataStateService only
        """
```

#### Page 2: Per-Run Analysis

```python
class PerRunAnalysisPage:
    def __init__(self, services: ServiceContainer):
        self.services = services
    
    def render(self) -> None:
        """
        Render per-run analysis with operations.
        
        Flow:
        1. Get available timestamps from filters
        2. User selects specific timestamp
        3. Get complete run state (1 service call)
        4. Render pipeline status with operations
        5. Execute operations as needed
        6. Render analysis if ready
        
        Service Dependencies: DataStateService, CloudOperationService, 
                             ExtractionService, AnalyticsService
        """
```

#### Page 3: Download Manager

```python
class DownloadManagerPage:
    def __init__(self, services: ServiceContainer):
        self.services = services
    
    def render(self) -> None:
        """
        Render download manager operations hub.
        
        Flow:
        1. Generate coordinates from filters
        2. Get state overview (iterate coordinates atomically)
        3. Render state table and summary
        4. Provide operation interfaces in tabs
        5. Execute operations as needed
        
        Service Dependencies: DataStateService, CloudOperationService
        """
```

## Component Interactions

### Service Communication Patterns

**Pattern 1: Page → DataStateService → Core Services**
```python
# Pages only talk to DataStateService
temporal_data = services.data_state.get_temporal_coverage_data(filters)

# DataStateService coordinates with other services
def get_temporal_coverage_data(self, filters: Dict) -> TemporalData:
    cloud_data = self.cloud.get_temporal_data(filters)  # From cache
    # Process and return structured model
    return TemporalData(...)
```

**Pattern 2: Operations → Service Coordination**
```python
# Operations coordinate multiple services
def execute_raw_download(self, coord: RunCoordinate) -> DownloadJob:
    # Check state, execute download, update local cache
    job = self._perform_download(coord)
    self.local.invalidate_cache(coord)  # Update state
    return job
```

**Pattern 3: Model-Driven UI**
```python
# Services return models, UI renders them
run_state = services.data_state.get_complete_run_state(coord)

# UI logic based on model state
if run_state.next_action == "download":
    if st.button("Download"):
        services.cloud_operations.execute_raw_download(coord)
```

### Data Flow Architecture

```
User Input → Page → DataStateService → Core Services → Cache/APIs
                ↓
            Model Objects → UI Rendering
```

### Cache Strategy

**Three-Level Caching**:
1. **Memory Cache**: In service instances (fastest)
2. **Session State**: Survives page refreshes
3. **Disk Cache**: Persists across app restarts

### Error Handling Strategy

**Atomic Operation Errors**:
```python
def execute_operation(self, coord: RunCoordinate) -> OperationResult:
    try:
        result = self._perform_operation(coord)
        return OperationResult(success=True, result=result)
    except CriticalError as e:
        # Stop immediately
        return OperationResult(success=False, error=str(e), critical=True)
    except Warning as e:
        # Log warning, return partial success
        return OperationResult(success=True, warning=str(e), result=partial_result)
```

## Key Architectural Benefits

1. **Simplified Complexity**: No batch operations eliminates coordination complexity
2. **Clear Contracts**: Models define all interfaces between components
3. **Atomic Operations**: Easier to test, debug, and reason about
4. **Focused Services**: Single responsibility principle throughout
5. **Type Safety**: Models catch interface errors early
6. **Cache Efficiency**: Comprehensive cache with atomic queries as needed

## Implementation Constraints

1. **No Batch Operations**: All operations work on single coordinates
2. **Atomic UI Updates**: Pages update one coordinate at a time
3. **Cache-First**: Bulk data comes from cache, operations hit APIs
4. **Model Contracts**: All data flows through structured models
5. **Single Page Navigation**: Standard page-to-page navigation only

This architecture provides a clean, testable, and maintainable foundation for the dashboard while eliminating unnecessary complexity.
