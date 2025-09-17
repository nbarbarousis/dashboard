"""
Services package - Clean imports for all services.
"""

# Core services
from .core.cloud_inventory_service import CloudInventoryService
from .core.local_state_service import LocalStateService
from .core.coordinate_path_builder import CoordinatePathBuilder

# Coordination services
from .coordination.data_coordination_service import DataCoordinationService

# Page services
from .pages.temporal_coverage_service import TemporalCoverageService
# from .pages.run_analysis_service import RunAnalysisService        # Future
# from .pages.operations_view_service import OperationsViewService  # Future

# Utility services
from .utils.service_container import ServiceContainer

# Operation services (future)
from .cloud_operations.cloud_operations_service import CloudOperationService

__all__ = [
    # Core
    'CloudInventoryService',
    'LocalStateService', 
    'CoordinatePathBuilder',
    
    # Coordination
    'DataCoordinationService',
    
    # Pages
    'TemporalCoverageService',
    
    # Utils
    'ServiceContainer',

    # Operations
    'CloudOperationService',
]