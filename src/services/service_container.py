"""
Service Container - Manages all services and their initialization.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import logging

from src.models import RunCoordinate
from src.models import CloudMLStatus
from src.models import DashboardConfig
from src.services.cloud_inventory_service import CloudInventoryService
from src.services.local_state_service import LocalStateService
from src.services.data_state_service import DataStateService
from src.services.coordinate_path_builder import CoordinatePathBuilder

logger = logging.getLogger(__name__)


@dataclass
class ServiceContainer:
    """Simple service container - holds all initialized services."""
    # Core data services
    cloud_inventory: CloudInventoryService
    local_state: LocalStateService
    data_state: DataStateService
    
    # Path management
    path_builder: CoordinatePathBuilder
    
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
            
            # 1. Initialize CoordinatePathBuilder first (other services depend on it)
            bucket_names = {
                'raw': config.raw_bucket_name,
                'ml': config.ml_bucket_name,
                'processed': config.processed_bucket_name  # Often same as raw
            }
            
            path_builder = CoordinatePathBuilder(
                raw_root=config.raw_data_root,
                ml_root=config.ml_data_root,
                processed_root=config.processed_data_root,
                bucket_names=bucket_names
            )
            
            # 2. Initialize CloudInventoryService
            cloud_service = CloudInventoryService(
                bucket_names=bucket_names,
                cache_file=str(config.cache_root / "cloud_inventory.json")
            )
            
            # 3. Initialize LocalStateService with path builder
            local_service = LocalStateService(
                path_builder=path_builder
            )
            
            # 4. Initialize DataStateService (depends on cloud and local)
            data_state_service = DataStateService(
                cloud_service=cloud_service,
                local_service=local_service
            )
            
            # 5. TODO: Initialize operation services
            # cloud_operations = CloudOperationService(...)
            # extraction = ExtractionService(...)
            # analytics = AnalyticsService(...)
            
            logger.info("All services initialized successfully")
            
            return cls(
                cloud_inventory=cloud_service,
                local_state=local_service,
                data_state=data_state_service,
                path_builder=path_builder
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

def get_ml_status(self, coord: RunCoordinate) -> CloudMLStatus:
        """
        Get ML sample info for single coordinate from cache.
        
        Args:
            coord: RunCoordinate to query
            
        Returns:
            CloudMLStatus: ML sample information from cloud
        """
        try:
            inventory = self.get_full_inventory()
            
            if 'ml' not in inventory:
                return CloudMLStatus(
                    exists=False, 
                    total_samples=0, 
                    bag_samples={},
                    bag_files={}
                )
            
            # Navigate to coordinate
            path_data = self._navigate_to_coordinate(inventory['ml'], coord)
            
            if path_data is None:
                return CloudMLStatus(
                    exists=False, 
                    total_samples=0, 
                    bag_samples={},
                    bag_files={}
                )
            
            # Extract ML sample information
            bag_samples_data = path_data.get('bag_samples', {})
            
            # Build bag_samples (counts) and bag_files (file lists)
            bag_samples = {}
            bag_files = {}
            total_samples = 0
            
            for bag_name, bag_data in bag_samples_data.items():
                if isinstance(bag_data, dict):
                    # Extract counts for bag_samples
                    frame_count = bag_data.get('frame_count', 0)
                    label_count = bag_data.get('label_count', 0)
                    
                    bag_samples[bag_name] = {
                        'frame_count': frame_count,
                        'label_count': label_count
                    }
                    
                    # Extract file lists for bag_files
                    bag_files[bag_name] = {
                        'frames': bag_data.get('frame_files', []),
                        'labels': bag_data.get('label_files', [])
                    }
                    
                    total_samples += label_count
            
            return CloudMLStatus(
                exists=total_samples > 0,
                total_samples=total_samples,
                bag_samples=bag_samples,
                bag_files=bag_files
            )
            
        except Exception as e:
            logger.error(f"Error getting ML samples info for {coord}: {e}")
            return CloudMLStatus(
                exists=False, 
                total_samples=0, 
                bag_samples={},
                bag_files={}
            )