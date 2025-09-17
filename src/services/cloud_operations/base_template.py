# src/services/operations/cloud_operation_template.py
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple
import logging
import time

from src.models import (
    RunCoordinate, TransferJob, TransferPlan, OperationResult,
    CloudRawStatus, CloudMLStatus, LocalRawStatus, LocalMLStatus
)
from ..core.cloud_inventory_service import CloudInventoryService
from ..core.local_state_service import LocalStateService
from ..core.coordinate_path_builder import CoordinatePathBuilder

logger = logging.getLogger(__name__)


class CloudOperationTemplate(ABC):
    """
    Abstract template for cloud operations.
    
    Defines the operation algorithm while subclasses implement specifics.
    """
    
    def __init__(
        self,
        cloud_service: CloudInventoryService,
        local_service: LocalStateService,
        path_builder: CoordinatePathBuilder,
        gcs_client
    ):
        self.cloud_service = cloud_service
        self.local_service = local_service  
        self.path_builder = path_builder
        self.gcs_client = gcs_client
    
    def execute_operation(self, job: TransferJob) -> OperationResult:
        """Template method defining the operation algorithm."""
        try:
            logger.info(f"Starting {job.operation_type} for {job.coordinate}")
            
            # Step 1: Discover source state using models
            source_state = self._discover_source_state(job.coordinate)
            
            if not self._validate_source_state(source_state):
                return OperationResult(
                    success=False,
                    error="No source data found"
                )
            
            # Step 2: Discover target state using models
            target_state = self._discover_target_state(job.coordinate)
            
            # Step 3: Apply selection criteria
            filtered_items = self._apply_selection_filter(
                source_state, job.selection_criteria
            )
            
            # Step 4: Create plan (includes conflict detection)
            plan = self._create_operation_plan(
                job, filtered_items, source_state, target_state
            )
            
            logger.info(
                f"Plan: {plan.total_files} files, "
                f"{self._format_size(plan.total_size)}"
            )
                        
            # Step 5: Execute or return dry-run
            if job.dry_run:
                return OperationResult(
                    success=True,
                    result={"plan": plan, "dry_run": True}
                )
            else:
                if not plan.files_to_transfer:
                    return OperationResult(
                        success=True,
                        result={"plan": plan, "message": "No files to transfer"}
                    )
                
                return self._execute_transfer(plan, job)
                
        except Exception as e:
            logger.error(f"Operation failed: {e}", exc_info=True)
            return OperationResult(success=False, error=str(e), critical=True)
    
    # Abstract methods using proper models
    @abstractmethod
    def _discover_source_state(self, coord: RunCoordinate):
        """Return appropriate status model (CloudRawStatus, LocalMLStatus, etc.)"""
        pass
    
    @abstractmethod
    def _discover_target_state(self, coord: RunCoordinate):
        """Return appropriate status model"""
        pass
    
    @abstractmethod
    def _validate_source_state(self, source_state) -> bool:
        """Validate source has data available"""
        pass
    
    @abstractmethod
    def _apply_selection_filter(self, source_state, criteria: Dict) -> List:
        """Apply selection criteria, return list of items to transfer"""
        pass
    
    @abstractmethod
    def _create_operation_plan(
        self, job: TransferJob, filtered_items: List,
        source_state, target_state
    ) -> TransferPlan:
        """Create detailed transfer plan"""
        pass
    
    @abstractmethod
    def _execute_transfer(self, plan: TransferPlan, job: TransferJob) -> OperationResult:
        """Execute the actual transfer"""
        pass
    
    def _format_size(self, size_bytes: int) -> str:
        """Utility: format bytes as human readable"""
        if size_bytes < 1024:
            return f"{size_bytes}B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f}KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f}MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f}GB"
        
    def _decide_file_action(
        self, job: TransferJob, cloud_size: int, local_size: int
    ) -> Tuple[bool, bool]:
        """
        Decide whether to transfer or skip a file based on conflict_resolution:
        - overwrite: always transfer (conflict=True)
        - skip:
            • if sizes match → skip (conflict=False)
            • if sizes differ → transfer (conflict=True)
        Returns:
            (should_transfer, is_conflict)
        """
        if job.conflict_resolution == "overwrite":
            return True, True
        # skip mode
        if cloud_size == local_size:
            return False, True
        # size mismatch under skip → still transfer
        return True, True