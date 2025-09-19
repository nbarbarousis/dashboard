# src/services/pages/operations_orchestration_service.py
"""
Operations Orchestration Service - Handles transfer operation workflows.

This service coordinates between the UI and the CloudOperationService,
handling job creation, dry runs, and execution for both single and bulk operations.
"""

import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from src.models import (
    RunCoordinate, TransferJob, TransferPlan, OperationResult,
    InventoryItem, CloudRawStatus, CloudMLStatus, LocalMLStatus
)
from ..cloud_operations.cloud_operations_service import CloudOperationService

logger = logging.getLogger(__name__)


@dataclass
class OperationDialogData:
    """Data for operation configuration dialog."""
    operation_type: str  # "raw_download", "ml_download", "ml_upload"
    coordinate: RunCoordinate
    available_options: Dict  # What's available for selection
    default_selection: Dict  # Default selection criteria
    supports_partial: bool  # Whether partial selection is supported
    conflict_options: List[str]  # Available conflict resolution options


@dataclass
class BulkOperationPlan:
    """Plan for bulk operations."""
    operation_type: str
    total_items: int
    total_size: int
    individual_jobs: List[TransferJob]
    summary: Dict  # Summary statistics


class OperationsOrchestrationService:
    """
    Orchestrates transfer operations between UI and backend services.
    
    This service handles:
    - Creating appropriate dialog data for operation configuration
    - Building TransferJob specifications from user input
    - Executing dry runs and actual transfers
    - Coordinating bulk operations
    """
    
    def __init__(self, cloud_operations: CloudOperationService):
        self.cloud_operations = cloud_operations
    
    # ========================================================================
    # Single Item Operations
    # ========================================================================
    
    def get_operation_dialog_data(
        self, 
        item: InventoryItem, 
        operation_type: str
    ) -> OperationDialogData:
        """
        Prepare data for operation configuration dialog.
        
        Args:
            item: Inventory item to operate on
            operation_type: Type of operation
            
        Returns:
            OperationDialogData with available options and defaults
        """
        coord = item.coord
        
        if operation_type == "raw_download":
            return self._get_raw_download_dialog_data(item)
        elif operation_type == "ml_download":
            return self._get_ml_download_dialog_data(item)
        elif operation_type == "ml_upload":
            return self._get_ml_upload_dialog_data(item)
        else:
            raise ValueError(f"Unknown operation type: {operation_type}")
    
    def _get_raw_download_dialog_data(self, item: InventoryItem) -> OperationDialogData:
        """Get dialog data for raw download operation."""
        cloud_status = item.cloud_raw_status
        
        available_options = {
            "bags": []
        }
        
        if cloud_status and cloud_status.exists:
            # Create list with indices and names
            available_options["bags"] = [
                {"index": i, "name": name, "size": cloud_status.bag_sizes.get(name, 0)}
                for i, name in enumerate(cloud_status.bag_names)
            ]
        
        return OperationDialogData(
            operation_type="raw_download",
            coordinate=item.coord,
            available_options=available_options,
            default_selection={"all": True},  # Default to downloading all
            supports_partial=True,
            conflict_options=["skip", "overwrite"]
        )
    
    def _get_ml_download_dialog_data(self, item: InventoryItem) -> OperationDialogData:
        """Get dialog data for ML download operation."""
        cloud_status = item.cloud_ml_status
        
        available_options = {
            "bags": [],
            "file_types": ["frames", "labels"]
        }
        
        if cloud_status and cloud_status.exists:
            # Get bag names with sample counts
            available_options["bags"] = [
                {
                    "name": bag_name,
                    "frame_count": info.get("frame_count", 0),
                    "label_count": info.get("label_count", 0)
                }
                for bag_name, info in cloud_status.bag_samples.items()
            ]
        
        return OperationDialogData(
            operation_type="ml_download",
            coordinate=item.coord,
            available_options=available_options,
            default_selection={"all": True, "file_types": ["frames", "labels"]},
            supports_partial=True,
            conflict_options=["skip", "overwrite"]
        )
    
    def _get_ml_upload_dialog_data(self, item: InventoryItem) -> OperationDialogData:
        """Get dialog data for ML upload operation."""
        local_status = item.local_ml_status
        
        available_options = {
            "bags": [],
            "file_types": ["frames", "labels"]
        }
        
        if local_status and local_status.downloaded:
            # Get bag names with sample counts
            available_options["bags"] = [
                {
                    "name": bag_name,
                    "frame_count": info.get("frame_count", 0),
                    "label_count": info.get("label_count", 0)
                }
                for bag_name, info in local_status.bag_samples.items()
            ]
        
        return OperationDialogData(
            operation_type="ml_upload",
            coordinate=item.coord,
            available_options=available_options,
            default_selection={"all": True, "file_types": ["frames", "labels"]},
            supports_partial=True,
            conflict_options=["skip", "overwrite"]
        )
    
    def create_transfer_job(
        self,
        coordinate: RunCoordinate,
        operation_type: str,
        selection_criteria: Dict,
        conflict_resolution: str = "skip"
    ) -> TransferJob:
        """
        Create a TransferJob from user input.
        
        Args:
            coordinate: Run coordinate
            operation_type: Operation type
            selection_criteria: User's selection criteria
            conflict_resolution: Conflict resolution strategy
            
        Returns:
            TransferJob ready for execution
        """
        return TransferJob(
            coordinate=coordinate,
            operation_type=operation_type,
            selection_criteria=selection_criteria,
            conflict_resolution=conflict_resolution,
            dry_run=True  # Always start with dry run
        )
    
    def execute_dry_run(self, job: TransferJob) -> OperationResult:
        """
        Execute a dry run for the transfer job.
        
        Args:
            job: Transfer job specification
            
        Returns:
            OperationResult with dry run plan
        """
        # Ensure dry_run is True
        job.dry_run = True
        return self.cloud_operations.execute_transfer_job(job)
    
    def execute_transfer(self, job: TransferJob) -> OperationResult:
        """
        Execute the actual transfer operation.
        
        Args:
            job: Transfer job specification
            
        Returns:
            OperationResult with execution results
        """
        # Set dry_run to False for actual execution
        logger.info(f"Executing transfer for job: {job}, dry_run=False")
        job.dry_run = False
        return self.cloud_operations.execute_transfer_job(job)
    
    # ========================================================================
    # Bulk Operations
    # ========================================================================
    
    def prepare_bulk_operation(
        self,
        items: List[InventoryItem],
        operation_type: str,
        selection_criteria: Dict,
        conflict_resolution: str = "skip"
    ) -> BulkOperationPlan:
        """
        Prepare a bulk operation plan by breaking into individual jobs.
        
        Args:
            items: List of inventory items to operate on
            operation_type: Type of operation
            selection_criteria: Selection criteria to apply to all items
            conflict_resolution: Conflict resolution strategy
            
        Returns:
            BulkOperationPlan with individual jobs and summary
        """
        individual_jobs = []
        total_size_estimate = 0
        
        for item in items:
            job = TransferJob(
                coordinate=item.coord,
                operation_type=operation_type,
                selection_criteria=selection_criteria,
                conflict_resolution=conflict_resolution,
                dry_run=True
            )
            individual_jobs.append(job)
            
            # Estimate size based on item status
            if operation_type == "raw_download":
                if item.cloud_raw_status and item.cloud_raw_status.exists:
                    if selection_criteria.get("all"):
                        total_size_estimate += item.cloud_raw_status.total_size
                    # For partial selection, we'd need to calculate based on specific bags
                    
            elif operation_type == "ml_download":
                if item.cloud_ml_status and item.cloud_ml_status.exists:
                    # This is a rough estimate - actual size would be calculated during dry run
                    total_size_estimate += sum(
                        sum(files.values())
                        for bag_files in item.cloud_ml_status.bag_files.values()
                        for files in bag_files.values()
                    )
                    
            elif operation_type == "ml_upload":
                if item.local_ml_status and item.local_ml_status.downloaded:
                    # Rough estimate
                    total_size_estimate += sum(
                        sum(files.values())
                        for bag_files in item.local_ml_status.bag_files.values()
                        for files in bag_files.values()
                    )
        
        summary = {
            "total_runs": len(items),
            "estimated_size": total_size_estimate,
            "operation_type": operation_type,
            "selection_criteria": selection_criteria,
            "conflict_resolution": conflict_resolution
        }
        
        return BulkOperationPlan(
            operation_type=operation_type,
            total_items=len(items),
            total_size=total_size_estimate,
            individual_jobs=individual_jobs,
            summary=summary
        )
    
    def execute_bulk_dry_run(self, plan: BulkOperationPlan) -> List[OperationResult]:
        """
        Execute dry runs for all jobs in bulk plan.
        
        Args:
            plan: Bulk operation plan
            
        Returns:
            List of OperationResult for each job
        """
        results = []
        
        for job in plan.individual_jobs:
            job.dry_run = True
            result = self.cloud_operations.execute_transfer_job(job)
            results.append(result)
        
        return results
    
    def execute_bulk_transfer(
        self, 
        plan: BulkOperationPlan,
        progress_callback: Optional[callable] = None
    ) -> List[OperationResult]:
        """
        Execute bulk transfer with optional progress callback.
        
        Args:
            plan: Bulk operation plan
            progress_callback: Optional callback for progress updates
            
        Returns:
            List of OperationResult for each job
        """
        results = []
        total = len(plan.individual_jobs)
        
        for i, job in enumerate(plan.individual_jobs):
            if progress_callback:
                progress_callback(i + 1, total, f"Processing {job.coordinate.to_path_str()}")
            
            job.dry_run = False
            result = self.cloud_operations.execute_transfer_job(job)
            results.append(result)
        
        return results
    
    def get_bulk_summary(self, results: List[OperationResult]) -> Dict:
        """
        Summarize bulk operation results.
        
        Args:
            results: List of operation results
            
        Returns:
            Summary dictionary
        """
        successful = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        
        total_files = 0
        total_bytes = 0
        
        for result in results:
            if result.success and result.result:
                if "summary" in result.result:
                    summary = result.result["summary"]
                    total_files += summary.get("total_files", 0)
                    total_bytes += summary.get("total_bytes", 0)
        
        return {
            "total_jobs": len(results),
            "successful": successful,
            "failed": failed,
            "total_files": total_files,
            "total_bytes": total_bytes,
            "errors": [r.error for r in results if r.error]
        }