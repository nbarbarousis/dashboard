"""
Service Container - Manages all services and their initialization.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import logging

from src.models import DashboardConfig
from ..core.cloud_inventory_service import CloudInventoryService
from ..core.local_state_service import LocalStateService
from ..core.coordinate_path_builder import CoordinatePathBuilder
from ..coordination.data_coordination_service import DataCoordinationService
from ..pages.temporal_coverage_service import TemporalCoverageService
from ..pages.inventory_view_service import InventoryViewService  
from ..cloud_operations.cloud_operations_service import CloudOperationService
from ..pages.operations_orchestration_service import OperationsOrchestrationService

logger = logging.getLogger(__name__)


@dataclass
class ServiceContainer:
    """Service container with new architecture."""
    # Core data services
    cloud_inventory: CloudInventoryService
    local_state: LocalStateService
    path_builder: CoordinatePathBuilder
    
    # Coordination layer
    data_coordination: DataCoordinationService
    
    # Page services
    temporal_coverage: TemporalCoverageService
    inventory_view: InventoryViewService
    operations_orchestration: OperationsOrchestrationService
    
    # Operation services
    cloud_operations: CloudOperationService
    
    @classmethod
    def initialize(cls, config: DashboardConfig) -> 'ServiceContainer':
        """Initialize all services with new architecture."""
        try:
            logger.info("Initializing services...")
            
            # 1. Initialize CoordinatePathBuilder first
            bucket_names = {
                'raw': config.raw_bucket_name,
                'ml': config.ml_bucket_name,
                'processed': config.processed_bucket_name
            }
            
            path_builder = CoordinatePathBuilder(
                raw_root=config.raw_data_root,
                ml_root=config.ml_data_root,
                processed_root=config.processed_data_root,
                bucket_names=bucket_names
            )
            
            # 2. Initialize core services
            cloud_service = CloudInventoryService(
                bucket_names=bucket_names,
                cache_file=str(config.cache_root / "cloud_inventory.json")
            )
            
            local_service = LocalStateService(
                path_builder=path_builder
            )
            
            # 3. Initialize coordination layer
            data_coordination = DataCoordinationService(
                cloud_service=cloud_service,
                local_service=local_service
            )
            
            # 4. Initialize page services
            temporal_coverage = TemporalCoverageService(
                data_coordination=data_coordination
            )

            # 5. Initialize operation services
            cloud_operations = CloudOperationService(
                cloud_service=cloud_service,
                local_service=local_service,
                path_builder=path_builder
            )

            # 6. Initialize operations view service
            inventory_view = InventoryViewService(
                data_coordination=data_coordination
            )

            operations_orchestration = OperationsOrchestrationService(
                cloud_operations=cloud_operations,
            )
            
            logger.info("All services initialized successfully")
            
            return cls(
                cloud_inventory=cloud_service,
                local_state=local_service,
                path_builder=path_builder,
                data_coordination=data_coordination,
                temporal_coverage=temporal_coverage,
                cloud_operations=cloud_operations,
                inventory_view=inventory_view,
                operations_orchestration=operations_orchestration
            )
            
        except Exception as e:
            logger.error(f"Failed to initialize services: {e}")
            raise
    
    def warm_up(self) -> None:
        """Warm up services (load caches, etc.)."""
        try:
            logger.info("Warming up services...")
            
            # Ensure cloud inventory is loaded
            inventory = self.cloud_inventory.get_full_inventory()
            if not inventory:
                logger.warning("No cloud inventory data available")
            else:
                logger.info("Cloud inventory loaded")
                
        except Exception as e:
            logger.error(f"Error during service warm-up: {e}")
            raise