# Cloud Operations Implementation Plan

## Executive Summary

This document outlines the implementation plan for integrating cloud upload/download operations into the dashboard architecture. The solution addresses path translation complexities, eliminates code duplication through template method pattern, and maintains the atomic operations principle established in the current codebase.

## Problem Statement

The existing cloud_download_v2.py and cloud_upload.py scripts contain valuable business logic that needs integration into the dashboard, but face several architectural challenges:

1. **Path Translation Complexity**: Different naming conventions between local/cloud and raw/ML data structures
2. **Code Duplication**: Upload and download operations follow similar patterns with different implementations
3. **Discovery Limitations**: Current services lack bulk discovery capabilities needed for operations
4. **Bag-Level Granularity**: Operations require bag-specific paths not captured in current RunCoordinate model

## Architecture Overview

### Core Design Principles

1. **Separation of Concerns**: Path building, discovery, and operations remain distinct
2. **Template Method Pattern**: Eliminate duplication while preserving operation-specific logic
3. **Atomic Operations**: All operations work on single RunCoordinate objects
4. **Status-Driven Planning**: Operations plan based on discovered source/target states

## Component 1: CoordinatePathBuilder Service

### Purpose
Centralized path management service that handles all coordinate-to-path translations and naming convention differences between local/cloud and raw/ML structures.

### Service Signature

```python
class CoordinatePathBuilder:
    def __init__(self, raw_root: Path, ml_root: Path, processed_root: Path, bucket_names: Dict[str, str]):
        self.raw_root = raw_root
        self.ml_root = ml_root
        self.processed_root = processed_root
        self.bucket_names = bucket_names
    
    # Local Path Methods
    def get_local_raw_coordinate_path(self, coord: RunCoordinate) -> Path:
        """Get coordinate directory for local raw data"""
        return self.raw_root / coord.cid / coord.regionid / coord.fieldid / coord.twid / coord.lbid / coord.timestamp
    
    def get_local_raw_bag_path(self, coord: RunCoordinate, bag_name: str) -> Path:
        """Get specific bag file path for local raw data"""
        return self.get_local_raw_coordinate_path(coord) / bag_name
    
    def get_local_ml_coordinate_path(self, coord: RunCoordinate) -> Path:
        """Get coordinate directory for local ML data"""  
        return self.ml_root / "raw" / coord.cid / coord.regionid / coord.fieldid / coord.twid / coord.lbid / coord.timestamp
    
    def get_local_ml_bag_path(self, coord: RunCoordinate, bag_name: str, file_type: str, filename: str) -> Path:
        """Get specific ML file path (frames/labels)"""
        return self.get_local_ml_coordinate_path(coord) / bag_name / file_type / filename
    
    # Cloud Path Methods  
    def get_cloud_raw_bag_path(self, coord: RunCoordinate, bag_name: str) -> str:
        """Get cloud raw bag path"""
        return f"gs://{self.bucket_names['raw']}/{coord.cid}/{coord.regionid}/{coord.fieldid}/{coord.twid}/{coord.lbid}/{coord.timestamp}/rosbag/{bag_name}"
    
    def get_cloud_ml_file_path(self, coord: RunCoordinate, bag_name: str, file_type: str, filename: str) -> str:
        """Get cloud ML file path"""
        return f"gs://{self.bucket_names['ml']}/raw/{coord.cid}/{coord.regionid}/{coord.fieldid}/{coord.twid}/{coord.lbid}/{coord.timestamp}/rosbag/{bag_name}/{file_type}/{filename}"
    
    # Name Translation Methods
    def translate_bag_name_cloud_to_local(self, cloud_bag_name: str, data_type: str) -> str:
        """Convert cloud bag naming to local convention"""
        # "_2025-08-12-08-54-21_0.bag" → "rosbag_2025-08-12-08-54-21_0.bag"
        # "_2025-08-12-08-54-21_0" → "rosbag_2025-08-12-08-54-21_0"
    
    def translate_bag_name_local_to_cloud(self, local_bag_name: str, data_type: str) -> str:
        """Convert local bag naming to cloud convention"""
        # "rosbag_2025-08-12-08-54-21_0.bag" → "_2025-08-12-08-54-21_0.bag"
        # "rosbag_2025-08-12-08-54-21_0" → "_2025-08-12-08-54-21_0"
```

### Implementation Details

**Path Structure Handling:**
- Encapsulates all differences between local/cloud directory structures
- Handles raw vs ML path differences (ML has additional "raw/" prefix in cloud)
- Manages bag-level vs coordinate-level path building

**Naming Convention Translation:**
- Cloud raw bags: `_stem_index.bag`
- Local raw bags: `rosbag_stem_index.bag`  
- Cloud ML directories: `_stem_index/`
- Local ML directories: `rosbag_stem_index/`

**Integration Points:**
- LocalStateService uses for all path building (replaces `_build_coordinate_path`)
- CloudOperationService uses for source/target path generation
- Future services can use for consistent path management

## Component 2: Enhanced Status Models

### Current Limitations
- CloudMLStatus lacks detailed file information needed for upload operations
- Status models contain redundant path information when coordinate + path builder is sufficient

### Enhanced Models

```python
@dataclass
class CloudRawStatus:
    """Enhanced cloud raw data status"""
    exists: bool
    bag_count: int
    bag_names: List[str]  # Discovered bag names in cloud format
    bag_sizes: Dict[str, int]  # bag_name -> size_bytes
    total_size: int

@dataclass
class CloudMLStatus:  
    """Enhanced cloud ML data status"""
    exists: bool
    total_samples: int
    bag_samples: Dict[str, Dict]  # bag_name -> {frame_count: int, label_count: int}
    bag_files: Dict[str, Dict[str, List[str]]]  # bag_name -> {frames: [filenames], labels: [filenames]}
    
@dataclass
class LocalRawStatus:
    """Local raw data status (path removed - use path_builder)"""
    downloaded: bool
    bag_count: int
    bag_names: List[str]  # Local format bag names
    bag_sizes: Dict[str, int]  # bag_name -> size_bytes  
    total_size: int

@dataclass
class LocalMLStatus:
    """Local ML data status (path removed - use path_builder)"""
    exists: bool
    file_counts: Dict[str, int]  # extension -> count
    total_size: int
    bag_structure: Dict[str, Dict[str, List[str]]]  # bag_name -> {frames: [files], labels: [files]}
```

### Rationale for Changes
- **Removed path fields**: Redundant when coordinate + CoordinatePathBuilder provides same information
- **Added detailed file information**: Operations need to know specific files for conflict detection and planning
- **Added size information**: Essential for progress tracking and conflict resolution

## Component 3: Template Method Pattern Implementation

### Pattern Overview
Common algorithm skeleton with operation-specific implementations for discovery, planning, and execution phases.

### File Organization

```
src/services/cloud_operations/
├── __init__.py
├── base.py              # CloudOperationTemplate
├── raw_download.py      # RawDownloadOperation  
├── ml_upload.py         # MLUploadOperation
├── raw_upload.py        # RawUploadOperation
└── ml_download.py       # MLDownloadOperation
```

### Base Template

```python
from abc import ABC, abstractmethod

class CloudOperationTemplate(ABC):
    """Template for all cloud transfer operations"""
    
    def __init__(self, cloud_service: CloudInventoryService, 
                 local_service: LocalStateService,
                 path_builder: CoordinatePathBuilder):
        self.cloud_service = cloud_service
        self.local_service = local_service  
        self.path_builder = path_builder
    
    def execute_operation(self, coord: RunCoordinate, **options) -> OperationResult:
        """Template method - defines the operation skeleton"""
        try:
            # Phase 1: Discovery
            source_status = self.discover_source_status(coord)
            target_status = self.discover_target_status(coord)
            
            # Phase 2: Planning  
            plan = self.create_transfer_plan(source_status, target_status, options)
            
            # Phase 3: Validation
            if not self.validate_plan(plan):
                return OperationResult(success=False, error="Plan validation failed")
            
            # Phase 4: Execution
            if not options.get('dry_run', True):
                execution_result = self.execute_transfer_plan(plan, options)
                # Phase 5: Post-execution cleanup
                self.post_execution_cleanup(coord, execution_result)
            else:
                execution_result = self.create_dry_run_result(plan)
            
            # Phase 6: Result generation
            return self.generate_operation_result(plan, execution_result, options)
            
        except Exception as e:
            return OperationResult(success=False, error=str(e), critical=True)
    
    # Abstract methods - subclasses must implement
    @abstractmethod
    def discover_source_status(self, coord: RunCoordinate) -> Union[CloudRawStatus, LocalRawStatus, CloudMLStatus, LocalMLStatus]:
        """Discover what exists at source"""
        pass
    
    @abstractmethod  
    def discover_target_status(self, coord: RunCoordinate) -> Union[CloudRawStatus, LocalRawStatus, CloudMLStatus, LocalMLStatus]:
        """Discover what exists at target"""
        pass
    
    @abstractmethod
    def create_transfer_plan(self, source_status, target_status, options: Dict) -> Dict:
        """Create transfer plan based on source/target states"""
        pass
    
    @abstractmethod
    def execute_transfer_plan(self, plan: Dict, options: Dict) -> Dict:
        """Execute the actual transfer operations"""
        pass
    
    # Concrete methods with default implementations
    def validate_plan(self, plan: Dict) -> bool:
        """Validate transfer plan (can be overridden)"""
        return len(plan.get('files_to_transfer', [])) > 0
    
    def post_execution_cleanup(self, coord: RunCoordinate, execution_result: Dict):
        """Post-execution cleanup (invalidate caches, etc.)"""
        # Mark caches as stale after operations complete
        if execution_result.get('success', False):
            self.cloud_service.mark_stale("post_operation_cleanup")
    
    def create_dry_run_result(self, plan: Dict) -> Dict:
        """Create dry run result"""
        return {"dry_run": True, "plan": plan, "success": True}
    
    def generate_operation_result(self, plan: Dict, execution_result: Dict, options: Dict) -> OperationResult:
        """Generate final operation result"""
        return OperationResult(
            success=execution_result.get('success', False),
            result=execution_result,
            error=execution_result.get('error'),
            warning=execution_result.get('warning')
        )
```

### Concrete Implementations

#### Raw Download Operation

```python
class RawDownloadOperation(CloudOperationTemplate):
    """Download raw bags from cloud to local storage"""
    
    def discover_source_status(self, coord: RunCoordinate) -> CloudRawStatus:
        return self.cloud_service.get_raw_status(coord)
    
    def discover_target_status(self, coord: RunCoordinate) -> LocalRawStatus:
        return self.local_service.get_raw_status(coord)
    
    def create_transfer_plan(self, source_status: CloudRawStatus, target_status: LocalRawStatus, options: Dict) -> Dict:
        """Plan which bags to download"""
        files_to_download = []
        conflicts = []
        
        for cloud_bag_name in source_status.bag_names:
            # Translate cloud bag name to expected local name
            local_bag_name = self.path_builder.translate_bag_name_cloud_to_local(cloud_bag_name, "raw")
            
            if local_bag_name in target_status.bag_names:
                # File exists locally - check for conflicts
                cloud_size = source_status.bag_sizes.get(cloud_bag_name, 0)
                local_size = target_status.bag_sizes.get(local_bag_name, 0)
                
                if cloud_size != local_size:
                    conflicts.append({
                        "cloud_bag": cloud_bag_name,
                        "local_bag": local_bag_name,
                        "cloud_size": cloud_size,
                        "local_size": local_size
                    })
                # If sizes match, skip (already downloaded)
            else:
                # File doesn't exist locally - add to download list
                cloud_path = self.path_builder.get_cloud_raw_bag_path(coord, cloud_bag_name)
                local_path = self.path_builder.get_local_raw_bag_path(coord, local_bag_name)
                
                files_to_download.append({
                    "cloud_path": cloud_path,
                    "local_path": local_path,
                    "cloud_bag_name": cloud_bag_name,
                    "local_bag_name": local_bag_name,
                    "size": source_status.bag_sizes.get(cloud_bag_name, 0)
                })
        
        # Handle conflicts based on options
        conflict_resolution = options.get('conflict_resolution', 'skip')
        if conflict_resolution == 'overwrite':
            for conflict in conflicts:
                cloud_path = self.path_builder.get_cloud_raw_bag_path(coord, conflict['cloud_bag'])
                local_path = self.path_builder.get_local_raw_bag_path(coord, conflict['local_bag'])
                files_to_download.append({
                    "cloud_path": cloud_path,
                    "local_path": local_path,
                    "cloud_bag_name": conflict['cloud_bag'],
                    "local_bag_name": conflict['local_bag'],
                    "size": conflict['cloud_size'],
                    "overwrite": True
                })
        
        return {
            "files_to_download": files_to_download,
            "conflicts": conflicts,
            "total_size": sum(f["size"] for f in files_to_download)
        }
    
    def execute_transfer_plan(self, plan: Dict, options: Dict) -> Dict:
        """Execute raw bag downloads using GCS client"""
        from google.cloud import storage
        
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(self.path_builder.bucket_names['raw'])
        
        downloaded_files = []
        failed_files = []
        
        for file_info in plan['files_to_download']:
            try:
                # Extract blob path from cloud path
                cloud_path = file_info['cloud_path']
                blob_path = cloud_path.replace(f"gs://{self.path_builder.bucket_names['raw']}/", "")
                
                # Download file
                blob = bucket.blob(blob_path)
                local_path = file_info['local_path']
                
                # Ensure directory exists
                local_path.parent.mkdir(parents=True, exist_ok=True)
                
                blob.download_to_filename(str(local_path))
                
                downloaded_files.append({
                    "local_path": str(local_path),
                    "size": file_info['size']
                })
                
            except Exception as e:
                failed_files.append({
                    "cloud_path": file_info['cloud_path'],
                    "error": str(e)
                })
        
        return {
            "success": len(failed_files) == 0,
            "downloaded_files": downloaded_files,
            "failed_files": failed_files,
            "total_downloaded": len(downloaded_files),
            "total_failed": len(failed_files)
        }
```

#### ML Upload Operation

```python
class MLUploadOperation(CloudOperationTemplate):
    """Upload ML samples from local to cloud storage"""
    
    def discover_source_status(self, coord: RunCoordinate) -> LocalMLStatus:
        return self.local_service.get_ml_status(coord)
    
    def discover_target_status(self, coord: RunCoordinate) -> CloudMLStatus:
        return self.cloud_service.get_ml_status(coord)
    
    def create_transfer_plan(self, source_status: LocalMLStatus, target_status: CloudMLStatus, options: Dict) -> Dict:
        """Plan which ML files to upload"""
        files_to_upload = []
        conflicts = []
        
        for bag_name, file_types in source_status.bag_structure.items():
            # Translate local bag name to cloud format
            cloud_bag_name = self.path_builder.translate_bag_name_local_to_cloud(bag_name, "ml")
            
            for file_type in ['frames', 'labels']:
                if file_type not in file_types:
                    continue
                    
                for filename in file_types[file_type]:
                    # Check if file exists in cloud
                    cloud_files = target_status.bag_files.get(cloud_bag_name, {}).get(file_type, [])
                    
                    if filename in cloud_files:
                        # File exists - potential conflict
                        conflicts.append({
                            "bag_name": bag_name,
                            "file_type": file_type,
                            "filename": filename
                        })
                    else:
                        # File doesn't exist - add to upload list
                        local_path = self.path_builder.get_local_ml_bag_path(coord, bag_name, file_type, filename)
                        cloud_path = self.path_builder.get_cloud_ml_file_path(coord, cloud_bag_name, file_type, filename)
                        
                        files_to_upload.append({
                            "local_path": local_path,
                            "cloud_path": cloud_path,
                            "bag_name": bag_name,
                            "file_type": file_type,
                            "filename": filename
                        })
        
        # Handle conflicts
        conflict_resolution = options.get('conflict_resolution', 'skip')
        if conflict_resolution == 'overwrite':
            for conflict in conflicts:
                bag_name = conflict['bag_name']
                file_type = conflict['file_type']
                filename = conflict['filename']
                cloud_bag_name = self.path_builder.translate_bag_name_local_to_cloud(bag_name, "ml")
                
                local_path = self.path_builder.get_local_ml_bag_path(coord, bag_name, file_type, filename)
                cloud_path = self.path_builder.get_cloud_ml_file_path(coord, cloud_bag_name, file_type, filename)
                
                files_to_upload.append({
                    "local_path": local_path,
                    "cloud_path": cloud_path,
                    "bag_name": bag_name,
                    "file_type": file_type,
                    "filename": filename,
                    "overwrite": True
                })
        
        return {
            "files_to_upload": files_to_upload,
            "conflicts": conflicts,
            "total_files": len(files_to_upload)
        }
    
    def execute_transfer_plan(self, plan: Dict, options: Dict) -> Dict:
        """Execute ML file uploads using GCS client"""
        from google.cloud import storage
        
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(self.path_builder.bucket_names['ml'])
        
        uploaded_files = []
        failed_files = []
        
        for file_info in plan['files_to_upload']:
            try:
                # Extract blob path from cloud path  
                cloud_path = file_info['cloud_path']
                blob_path = cloud_path.replace(f"gs://{self.path_builder.bucket_names['ml']}/", "")
                
                # Upload file
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(str(file_info['local_path']))
                
                uploaded_files.append({
                    "cloud_path": cloud_path,
                    "local_path": str(file_info['local_path'])
                })
                
            except Exception as e:
                failed_files.append({
                    "local_path": str(file_info['local_path']),
                    "error": str(e)
                })
        
        return {
            "success": len(failed_files) == 0,
            "uploaded_files": uploaded_files,
            "failed_files": failed_files,
            "total_uploaded": len(uploaded_files),
            "total_failed": len(failed_files)
        }
```

## Component 4: Service Container Integration

### Enhanced ServiceContainer

```python
@dataclass
class ServiceContainer:
    # Core data services
    cloud_inventory: CloudInventoryService
    local_state: LocalStateService
    data_state: DataStateService
    
    # Shared utilities
    path_builder: CoordinatePathBuilder
    
    @classmethod
    def initialize(cls, config: DashboardConfig) -> 'ServiceContainer':
        """Initialize all services with configuration"""
        # Initialize path builder
        bucket_names = {
            'raw': config.raw_bucket_name,
            'ml': config.ml_bucket_name
        }
        
        path_builder = CoordinatePathBuilder(
            raw_root=config.raw_data_root,
            ml_root=config.ml_data_root,
            processed_root=config.processed_data_root,
            bucket_names=bucket_names
        )
        
        # Initialize other services (updated to use path_builder)
        cloud_service = CloudInventoryService(bucket_names, str(config.cache_root / "cloud_inventory.json"))
        
        local_service = LocalStateService(
            raw_root=config.raw_data_root,
            processed_root=config.processed_data_root,
            ml_root=config.ml_data_root,
            path_builder=path_builder  # New dependency
        )
        
        data_state_service = DataStateService(cloud_service, local_service)
        
        return cls(
            cloud_inventory=cloud_service,
            local_state=local_service,
            data_state=data_state_service,
            path_builder=path_builder
        )
    
    # Factory methods for operations
    def create_raw_download_operation(self) -> RawDownloadOperation:
        return RawDownloadOperation(self.cloud_inventory, self.local_state, self.path_builder)
    
    def create_ml_upload_operation(self) -> MLUploadOperation:
        return MLUploadOperation(self.cloud_inventory, self.local_state, self.path_builder)
    
    def create_raw_upload_operation(self) -> RawUploadOperation:
        return RawUploadOperation(self.cloud_inventory, self.local_state, self.path_builder)
    
    def create_ml_download_operation(self) -> MLDownloadOperation:  
        return MLDownloadOperation(self.cloud_inventory, self.local_state, self.path_builder)
```

## Component 5: Bulk Discovery Methods

### CloudInventoryService Enhancements

```python
class CloudInventoryService:
    # Existing methods...
    
    def get_all_raw_statuses(self) -> Dict[RunCoordinate, CloudRawStatus]:
        """Get all cloud raw statuses from cache"""
        inventory = self.get_full_inventory()
        results = {}
        
        # Walk cache hierarchy to find all coordinates
        for cid, cid_data in inventory.get('raw', {}).items():
            for regionid, regionid_data in cid_data.items():
                for fieldid, fieldid_data in regionid_data.items():
                    for twid, twid_data in fieldid_data.items():
                        for lbid, lbid_data in twid_data.items():
                            for timestamp, timestamp_data in lbid_data.items():
                                coord = RunCoordinate(cid, regionid, fieldid, twid, lbid, timestamp)
                                results[coord] = self.get_raw_status(coord)
        
        return results
    
    def get_all_ml_statuses(self) -> Dict[RunCoordinate, CloudMLStatus]:
        """Get all cloud ML statuses from cache"""
        # Similar implementation for ML bucket
        pass
```

### LocalStateService Enhancements

```python
class LocalStateService:
    # Existing methods...
    
    def get_all_raw_statuses(self) -> Dict[RunCoordinate, LocalRawStatus]:
        """Walk filesystem to discover all local raw data"""
        results = {}
        
        if not self.raw_root.exists():
            return results
        
        # Walk filesystem hierarchy
        for cid_dir in self.raw_root.iterdir():
            if not cid_dir.is_dir():
                continue
                
            for regionid_dir in cid_dir.iterdir():
                if not regionid_dir.is_dir():
                    continue
                    
                for fieldid_dir in regionid_dir.iterdir():
                    if not fieldid_dir.is_dir():
                        continue
                        
                    for twid_dir in fieldid_dir.iterdir():
                        if not twid_dir.is_dir():
                            continue
                            
                        for lbid_dir in twid_dir.iterdir():
                            if not lbid_dir.is_dir():
                                continue
                                
                            for timestamp_dir in lbid_dir.iterdir():
                                if not timestamp_dir.is_dir():
                                    continue
                                
                                coord = RunCoordinate(
                                    cid=cid_dir.name,
                                    regionid=regionid_dir.name,
                                    fieldid=fieldid_dir.name,
                                    twid=twid_dir.name,
                                    lbid=lbid_dir.name,
                                    timestamp=timestamp_dir.name
                                )
                                
                                results[coord] = self.get_raw_status(coord)
        
        return results
    
    def get_all_ml_statuses(self) -> Dict[RunCoordinate, LocalMLStatus]:
        """Walk ML filesystem to discover all local ML data"""
        # Similar implementation for ML/raw directory
        pass
```

## Implementation Roadmap

### Phase 1: Foundation (Week 1)
**Deliverables:**
- `src/services/coordinate_paths.py` - Complete CoordinatePathBuilder implementation
- Update `LocalStateService.__init__()` to accept `path_builder` parameter
- Update `ServiceContainer.initialize()` to create and inject `path_builder`
- Update `LocalStateService` methods to use `path_builder` instead of `_build_coordinate_path`

**Testing Criteria:**
- All existing LocalStateService functionality works with new path builder
- Path builder correctly translates between naming conventions
- ServiceContainer initializes all dependencies correctly

### Phase 2: Enhanced Models (Week 2)
**Deliverables:**
- Update status models in `src/models/state.py`
- Enhance `CloudInventoryService` to populate detailed file information
- Add bulk discovery methods to both services
- Remove redundant path fields from status models

**Testing Criteria:**
- Status models contain all required file detail information
- Bulk discovery methods return comprehensive coordinate/status mappings
- Path information can be reconstructed using coordinate + path_builder

### Phase 3: Template Implementation (Week 3)
**Deliverables:**
- `src/services/cloud_operations/` package with template and concrete implementations
- Port core logic from existing cloud_download_v2.py and cloud_upload.py scripts
- Integration with ServiceContainer factory methods

**Testing Criteria:**
- Raw download operation works for single coordinates
- ML upload operation works for single coordinates  
- Template pattern eliminates code duplication
- Operations return structured results with proper error handling

### Phase 4: Dashboard Integration (Week 4)
**Deliverables:**
- Integration with Page 2 (Per-Run Analysis) for single coordinate operations
- Integration with Page 3 (Download Manager) for bulk operations overview
- UI components for operation execution and progress display

**Testing Criteria:**
- Operations execute successfully from dashboard UI
- Progress and results display correctly
- Cache invalidation works after operations complete

## Design Justifications

### CoordinatePathBuilder as Shared Service
**Justification:** Path building logic appears in multiple services (LocalStateService, future CloudOperationService). Centralizing prevents duplication and ensures consistent path handling.

**Alternative Considered:** Utility functions
**Why Rejected:** Service approach provides better testability, dependency injection, and encapsulation of configuration.

### Template Method for Operations
**Justification:** Upload and download scripts follow nearly identical patterns (discovery → planning → execution → reporting). Template method eliminates duplication while preserving operation-specific behavior.

**Alternative Considered:** Composition with shared helper classes
**Why Rejected:** Template method provides clearer algorithm structure and better enforces the common workflow.

**Benefits:**
- Enforces consistent algorithm flow across all operations
- Prevents accidentally missing steps (like cache invalidation)
- Reduces code duplication in common parts (error handling, result generation, validation)
- Makes it easier to add new operation types
- Centralizes overall workflow logic

**Potential Drawbacks:**
- Adds complexity through inheritance
- Can be harder to understand flow when split across base and derived classes  
- More rigid - harder to customize overall flow for specific operations

### Bag Information in Status Models
**Justification:** Operations require bag-level granularity that RunCoordinate doesn't provide. Status models naturally contain discovered state information.

**Alternative Considered:** Extending RunCoordinate with bag information
**Why Rejected:** RunCoordinate represents conceptual run identity, not discovered filesystem reality.

### Path Builder Dependency Injection
**Justification:** Explicit dependencies make testing easier and clarify service relationships. Factory methods in ServiceContainer provide convenient initialization.

**Alternative Considered:** Service locator pattern throughout
**Why Rejected:** Hidden dependencies make testing harder and reduce code clarity.

## Conclusion

This implementation plan addresses all identified architectural challenges while maintaining consistency with the existing codebase design principles. The solution provides a clean separation of concerns, eliminates code duplication, and establishes patterns for future cloud operation extensions.

The template method pattern is justified for this use case because:
1. The existing scripts demonstrate a clear common algorithm
2. Consistent error handling and cache management are required
3. Multiple operation types will be added (raw upload, ML download)
4. The workflow is stable and unlikely to require significant customization per operation type