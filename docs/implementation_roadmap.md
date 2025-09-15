# Implementation Roadmap & Execution Plan

## Executive Summary

This document provides the complete step-by-step implementation plan for transforming the dashboard from its current monolithic architecture to the new atomic-operations, model-driven system. The plan is designed for a 4-week big bang migration with clear milestones and deliverables.

**Approach**: Big Bang Migration with Atomic Operations Only  
**Timeline**: 4 weeks  
**Team Size**: Single developer  
**Risk Level**: Medium (comprehensive testing mitigates architecture changes)

## Implementation Strategy

### Core Implementation Principles

1. **Foundation First**: Build data models and core services before UI
2. **Atomic Operations**: No batch operations - keep complexity minimal
3. **Test-Driven**: Each component tested before integration
4. **Cache-First**: Comprehensive cache with atomic queries
5. **Model Contracts**: All data flows through structured models

### Migration Approach

**Big Bang Benefits for This Project**:
- Clean final architecture without legacy code
- No dual-system maintenance overhead
- Clear timeline and deliverables
- Eliminates incremental complexity

**Risk Mitigation**:
- Comprehensive testing at each phase
- Working backups of current system
- Feature parity validation before go-live

## Phase-by-Phase Implementation

### Phase 1: Foundation Layer (Week 1)

**Goal**: Establish data models and core services foundation

#### Week 1.1-1.3: Data Models Implementation

**Tasks**:
```python
# 1. Create all data model classes in models/data_models.py
@dataclass
class RunCoordinate:
    # Complete implementation with all methods

class DataStatus(Enum):
    # All status values

@dataclass  
class CloudRawStatus:
    # All status models

@dataclass
class TemporalData:
    # All page-specific models

# 2. Create model validation and testing
def test_run_coordinate_creation():
    coord = RunCoordinate.from_filters(filters, timestamp)
    assert coord.to_path_str() == "client_001/region_xxx/..."

# 3. Model conversion methods
def test_model_serialization():
    coord = RunCoordinate(...)
    assert coord.to_dict() == expected_dict
```

**Deliverables**:
- `src/models/data_models.py` - Complete model definitions
- `tests/test_models.py` - Comprehensive model tests
- Model validation ensures all required fields and methods work

**Testing Criteria**:
- All models can be instantiated
- All model methods work correctly
- Model serialization/deserialization works
- Type hints are correct and validated

#### Week 1.4-1.7: Core Services Implementation

**Tasks**:
```python
# 1. LocalStateService - Pure filesystem operations
class LocalStateService:
    def check_raw_downloaded(self, coord: RunCoordinate) -> LocalRawStatus:
        # Implementation with actual filesystem checks
        
    def check_extracted(self, coord: RunCoordinate) -> ExtractionStatus:
        # Implementation checking for CSV files
        
# Test with mock filesystem
def test_local_state_detection():
    # Create test directory structure
    # Verify service detects state correctly

# 2. CloudInventoryService - Enhanced caching
class CloudInventoryService:
    def get_full_inventory(self) -> Dict:
        # Implementation with comprehensive cache structure
        
    def get_raw_bags_info(self, coord: RunCoordinate) -> CloudRawStatus:
        # Query cache, return structured model
        
# Test with mock GCS data
def test_cloud_inventory_caching():
    # Mock cloud data
    # Verify cache structure and queries
```

**Deliverables**:
- `src/services/local_state_service.py` - Complete filesystem state tracking
- `src/services/cloud_inventory_service.py` - Enhanced cloud caching
- `tests/test_local_state.py` - Local state tests with temp directories  
- `tests/test_cloud_inventory.py` - Cache tests with mock data

**Testing Criteria**:
- LocalStateService correctly identifies all local states
- CloudInventoryService builds comprehensive cache
- Cache persistence and loading works
- All service methods return correct model types

### Phase 2: Data Aggregation Layer (Week 2)

**Goal**: Build DataStateService and validate with Page 1 migration

#### Week 2.1-2.4: DataStateService Implementation

**Tasks**:
```python
# 1. Core DataStateService methods
class DataStateService:
    def get_temporal_coverage_data(self, filters: Dict, expected_samples: int) -> TemporalData:
        # Extract from cloud cache
        # Calculate gaps
        # Return structured TemporalData model
        
    def get_complete_run_state(self, coord: RunCoordinate) -> RunState:
        # Combine cloud and local state
        # Determine pipeline status and next action
        # Return comprehensive RunState model
        
    def get_filter_options(self, level: str, parent_filters: Dict) -> List[str]:
        # Navigate cache hierarchy
        # Return available options for dropdown
        
# 2. Service integration testing
def test_data_state_integration():
    # Mock both cloud and local services
    # Verify DataStateService correctly combines data
    # Test all page-specific methods
```

**Deliverables**:
- `src/services/data_state_service.py` - Complete data aggregation service
- `tests/test_data_state.py` - Integration tests with mocked dependencies
- Service method signatures match exactly with page requirements

**Testing Criteria**:
- All page-specific methods return correct model types
- Data aggregation logic works correctly
- Error handling for missing data works
- Filter options generation works for all hierarchy levels

#### Week 2.5-2.7: Page 1 Migration

**Tasks**:
```python
# 1. Update temporal_coverage.py to use new services
def render_temporal_coverage(services: ServiceContainer):
    # Replace current gcs_service calls
    temporal_data = services.data_state.get_temporal_coverage_data(filters)
    
    # Replace current DataService calls  
    stats = services.data_state.get_coverage_statistics(filters)
    
    # Keep same plotting logic
    _render_temporal_plots(temporal_data)
    _render_summary_metrics(stats)

# 2. Update hierarchical filters
def render_hierarchical_filters():
    # Use services.data_state.get_filter_options() instead of current logic
    
# 3. Service container integration
services = ServiceContainer.initialize(config)
```

**Deliverables**:
- `src/dashboard/pages/temporal_coverage.py` - Updated to use new services
- `src/dashboard/components/filters.py` - Updated filter components
- Page 1 functionality matches current behavior exactly

**Testing Criteria**:
- Page 1 loads without errors
- Temporal plots render correctly
- Filter dropdowns work and cascade properly  
- Coverage statistics match current calculations
- Performance is same or better than current system

### Phase 3: Operations Layer (Week 3)

**Goal**: Build CloudOperationService and integrate all existing script functionality

#### Week 3.1-3.3: Script Logic Extraction

**Tasks**:
```python
# 1. Extract core logic from cloud_download_v2.py
class CloudOperationService:
    def execute_raw_download(self, coord: RunCoordinate, **options) -> DownloadJob:
        # Port CloudDownloadManagerV2 logic
        # Remove CLI dependencies
        # Work with RunCoordinate instead of manifest files
        # Return structured DownloadJob model
        
    def _create_manifest_from_coord(self, coord: RunCoordinate) -> Dict:
        # Convert RunCoordinate to internal manifest format
        
    def _discover_cloud_files(self, coord: RunCoordinate) -> List[Dict]:
        # Port cloud file discovery logic
        
    def _download_files(self, files: List[Dict], coord: RunCoordinate) -> None:
        # Port actual download logic

# 2. Extract core logic from cloud_upload.py
    def execute_ml_upload(self, export_id: str, **options) -> Dict:
        # Port CloudUploadManager logic
        # Remove CLI dependencies  
        # Return structured result

# 3. Extract core logic from dataset_fetch.py
    def execute_dataset_fetch(self, dataset_name: str, filters: Dict, **options) -> Dict:
        # Port dataset fetch logic
        # Work with filters instead of YAML config
```

**Deliverables**:
- `src/services/cloud_operation_service.py` - All script logic integrated
- Removed CLI dependencies and file I/O from ported logic
- All operations work with RunCoordinate and model objects

**Testing Criteria**:
- Raw download works for single coordinate
- ML upload works with export IDs
- Dataset fetch works with filter selections
- All operations return structured results
- Error handling matches original script behavior

#### Week 3.4-3.7: Enhanced Services Integration

**Tasks**:
```python
# 1. Update ExtractionService for new architecture
class ExtractionService:
    def __init__(self, docker_image: str, local_service: LocalStateService):
        # Integration with LocalStateService for state tracking
        
    def execute_extraction(self, coord: RunCoordinate) -> ExtractionJob:
        # Enhanced extraction with better state management
        # Return structured ExtractionJob model

# 2. Update AnalyticsService for new architecture  
class AnalyticsService:
    def analyze_run(self, coord: RunCoordinate) -> RunAnalysis:
        # Integration with LocalStateService
        # Return structured RunAnalysis model with plots

# 3. Service container integration
@dataclass
class ServiceContainer:
    # All services properly initialized and connected
    
    def warm_up(self):
        # Ensure caches are loaded and services are ready
```

**Deliverables**:
- `src/services/extraction_service.py` - Enhanced extraction service
- `src/services/analytics_service.py` - Enhanced analytics service
- `src/services/service_container.py` - Complete service container
- All services work together through model contracts

**Testing Criteria**:
- Extraction works and updates local state correctly
- Analytics generates all plots and metrics
- Service container initializes all services correctly
- Service interdependencies work without circular imports

### Phase 4: UI Integration & Finalization (Week 4)

**Goal**: Complete Page 2 and Page 3 migration, remove old services, finalize system

#### Week 4.1-4.3: Page 2 Migration

**Tasks**:
```python
# 1. Update per_run_analysis.py
def render_per_run_analysis(services: ServiceContainer):
    # Single comprehensive state query
    run_state = services.data_state.get_complete_run_state(coord)
    
    # Pipeline status with integrated operations
    _render_pipeline_status(run_state, services, coord)
    
    # Integrated analysis rendering
    if run_state.ready_for_analysis:
        analysis = services.analytics.analyze_run(coord)
        _render_analysis_plots(analysis.plots)

# 2. Operations integration in UI
def _execute_download(services, coord):
    job = services.cloud_operations.execute_raw_download(coord, dry_run=False)
    # Handle progress and results

def _execute_extraction(services, coord):
    job = services.extraction.execute_extraction(coord)
    # Handle progress and results
```

**Deliverables**:
- `src/dashboard/pages/per_run_analysis.py` - Complete page with integrated operations
- Pipeline status visual with context-sensitive buttons
- Direct operation execution within UI

**Testing Criteria**:
- Page loads and shows correct pipeline status
- Download, extraction, and analysis operations work within UI
- Progress feedback works correctly
- Page refreshes show updated state after operations

#### Week 4.4-4.6: Page 3 Implementation

**Tasks**:
```python
# 1. Build download_manager.py from scratch
def render_download_manager(services: ServiceContainer):
    # State overview table
    coords = services.data_state.generate_coordinates_from_filters(filters)
    
    # Iterate through coordinates atomically for state
    state_data = []
    for coord in coords:
        state = services.data_state.get_complete_run_state(coord)
        state_data.append(_format_state_for_table(coord, state))
    
    # Operations tabs
    _render_operations_tabs(services)

# 2. Operations interfaces
def _render_raw_download_interface(services):
    # UI for planning and executing downloads
    
def _render_ml_upload_interface(services):
    # UI for ML sample uploads
    
def _render_dataset_fetch_interface(services):
    # UI for dataset creation
```

**Deliverables**:
- `src/dashboard/pages/download_manager.py` - Complete operations hub
- State overview table with all run states
- Operation interfaces for all cloud operations
- Progress tracking and error handling

**Testing Criteria**:
- State overview loads and displays correctly for filter selections
- All operation interfaces work correctly
- Progress feedback works for all operations
- Error handling displays clear messages

#### Week 4.7: System Finalization

**Tasks**:
```python
# 1. Remove old services
# Delete src/services/gcs_service.py
# Delete src/services/data_service.py  
# Update imports throughout codebase

# 2. Update main app initialization
def main():
    # Use ServiceContainer.initialize() instead of old service setup
    services = ServiceContainer.initialize(config)
    services.warm_up()

# 3. Final integration testing
# Test complete user workflows across all pages
# Verify no old service dependencies remain

# 4. Performance validation
# Ensure new system performs as well as old system
# Profile cache performance and optimization if needed
```

**Deliverables**:
- Removed all old service code
- Updated `src/dashboard/app.py` - Complete integration
- Performance validated and optimized
- Full system working end-to-end

**Testing Criteria**:
- Complete user workflows work across all pages
- No errors or broken functionality
- Performance matches or exceeds current system
- All features from current system are preserved

## Implementation Details

### Development Environment Setup

**Repository Structure**:
```
src/
├── models/
│   └── data_models.py          # All model definitions
├── services/
│   ├── cloud_inventory_service.py
│   ├── local_state_service.py  
│   ├── data_state_service.py
│   ├── cloud_operation_service.py
│   ├── extraction_service.py
│   ├── analytics_service.py
│   └── service_container.py
├── dashboard/
│   ├── pages/
│   │   ├── temporal_coverage.py
│   │   ├── per_run_analysis.py
│   │   └── download_manager.py
│   └── components/
│       └── filters.py
└── tests/
    ├── test_models.py
    ├── test_services.py
    └── test_integration.py
```

### Testing Strategy

**Unit Testing**:
```python
# Model testing
def test_run_coordinate_from_filters():
    filters = {'client': 'c1', 'region': 'r1', ...}
    coord = RunCoordinate.from_filters(filters, 'timestamp')
    assert coord.cid == 'c1'

# Service testing with mocks
def test_local_state_service(tmp_path):
    service = LocalStateService(tmp_path, tmp_path, tmp_path)
    # Create test files
    status = service.check_raw_downloaded(coord)
    assert status.downloaded == False

# Integration testing
def test_data_state_service_integration():
    mock_cloud = Mock(spec=CloudInventoryService)
    mock_local = Mock(spec=LocalStateService) 
    service = DataStateService(mock_cloud, mock_local)
    # Test service integration
```

**User Acceptance Testing**:
```python
# Complete workflow testing
def test_user_workflow_download_analyze():
    # 1. User selects filters on Page 1
    # 2. User navigates to Page 2
    # 3. User downloads data
    # 4. User extracts data  
    # 5. User analyzes data
    # 6. Verify all steps work end-to-end
```

### Migration Validation

**Feature Parity Checklist**:
- [ ] Page 1 temporal coverage plots match current system
- [ ] Page 1 filter cascading works identically
- [ ] Page 2 pipeline status shows correct states
- [ ] Page 2 operations execute successfully
- [ ] Page 3 state overview shows all data correctly
- [ ] All cloud operations work as expected
- [ ] Performance is equal or better
- [ ] Error handling works correctly

**Data Validation**:
```python
# Compare old vs new system outputs
def validate_temporal_coverage_data():
    # Run same filters through old and new systems
    old_data = old_data_service.get_temporal_data(filters)
    new_data = services.data_state.get_temporal_coverage_data(filters)
    
    # Verify data matches
    assert old_data.timestamps == new_data.timestamps
    assert old_data.raw_bags == new_data.raw_bags
```

### Performance Considerations

**Cache Optimization**:
- Load comprehensive cache on startup (acceptable cold start)
- Memory cache for fast page navigation
- Atomic queries only when needed

**UI Responsiveness**:
- All page loads under 2 seconds
- Operations provide immediate feedback
- Progress bars for long operations

**Memory Management**:
- Cache size monitoring
- Periodic cache cleanup if needed
- Efficient coordinate iteration for state overview

### Risk Mitigation

**Technical Risks**:
1. **Cache Performance**: Monitor cache size and access patterns
2. **Service Integration**: Comprehensive integration testing
3. **UI State Management**: Thorough user workflow testing

**Migration Risks**:
1. **Feature Regression**: Detailed feature parity validation
2. **Data Consistency**: Validation against current system outputs  
3. **User Experience**: Ensure new system is as intuitive as current

**Mitigation Strategies**:
- Working backup of current system
- Rollback plan if critical issues found
- Staged rollout to validate functionality
- Comprehensive testing at each phase

## Success Criteria

### Technical Success Metrics
- All model contracts implemented and tested
- All services working with correct interfaces  
- All pages rendering and functioning correctly
- No external script dependencies
- Performance equal or better than current system

### User Experience Success Metrics  
- All current functionality preserved
- Intuitive operation execution within UI
- Clear progress feedback and error messages
- Fast page navigation and data loading

### Development Success Metrics
- Clean service boundaries enable easy testing
- Adding new features doesn't require touching unrelated code
- Debugging is easier with focused service responsibilities
- Architecture supports future enhancements

## Post-Implementation Plan

### Immediate Follow-up (Week 5)
- User acceptance testing with actual workflows
- Performance monitoring and optimization
- Bug fixes and refinements
- Documentation updates

### Future Enhancements (Weeks 6+)
- Real-time progress tracking improvements
- Additional operation types as needed
- UI/UX improvements based on usage
- Performance optimizations

This implementation plan provides a clear path from the current monolithic system to the new atomic-operations architecture while minimizing risk and ensuring feature parity.
