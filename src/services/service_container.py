"""
Service Container - Manages all services and their initialization.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import logging

from src.models import DashboardConfig
from src.services.cloud_inventory_service import CloudInventoryService
from src.services.local_state_service import LocalStateService
from src.services.data_state_service import DataStateService

logger = logging.getLogger(__name__)


@dataclass
class ServiceContainer:
    """Simple service container - holds all initialized services."""
    # Core data services
    cloud_inventory: CloudInventoryService
    local_state: LocalStateService
    data_state: DataStateService
    
    # Operation services (to be added later)
    # cloud_operations: CloudOperationService
    # extraction: ExtractionService
    # analytics: AnalyticsService
    
    @classmethod
    def initialize(cls, config: DashboardConfig) -> 'ServiceContainer':
        """
        Initialize all services with configuration.
        
        Args:
            config: Dashboard configuration
            
        Returns:
            ServiceContainer with all services initialized
        """
        try:
            logger.info("Initializing services...")
            
            # 1. Initialize CloudInventoryService
            bucket_names = {
                'raw': config.raw_bucket_name,
                'ml': config.ml_bucket_name,
                'processed': config.raw_bucket_name  # Often same as raw
            }
            
            cloud_service = CloudInventoryService(
                bucket_names=bucket_names,
                cache_file=str(config.cache_root / "cloud_inventory.json")
            )
            
            # 2. Initialize LocalStateService
            local_service = LocalStateService(
                raw_root=config.raw_data_root,
                processed_root=config.processed_data_root,
                ml_root=config.ml_data_root
            )
            
            # 3. Initialize DataStateService (depends on cloud and local)
            data_state_service = DataStateService(
                cloud_service=cloud_service,
                local_service=local_service
            )
            
            # 4. TODO: Initialize operation services
            # cloud_operations = CloudOperationService(...)
            # extraction = ExtractionService(...)
            # analytics = AnalyticsService(...)
            
            logger.info("All services initialized successfully")
            
            return cls(
                cloud_inventory=cloud_service,
                local_state=local_service,
                data_state=data_state_service
            )
            
        except Exception as e:
            logger.error(f"Failed to initialize services: {e}")
            raise
    
    def warm_up(self) -> None:
        """
        Warm up services (load caches, etc.).
        This is called after initialization to ensure data is ready.
        """
        try:
            logger.info("Warming up services...")
            
            # Ensure cloud inventory is loaded
            inventory = self.cloud_inventory.get_full_inventory()
            if not inventory:
                logger.warning("No cloud inventory data available")
            else:
                logger.info("Cloud inventory loaded")
            
            # Future: warm up other services as needed
            
        except Exception as e:
            logger.error(f"Error during service warm-up: {e}")
            raise