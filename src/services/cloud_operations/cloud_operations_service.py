# src/services/operations/cloud_operation_service.py
from typing import Dict, Type
import logging
from google.cloud import storage

from src.models import TransferJob, OperationResult
from ..core.cloud_inventory_service import CloudInventoryService
from ..core.local_state_service import LocalStateService
from ..core.coordinate_path_builder import CoordinatePathBuilder
from .base_template import CloudOperationTemplate
from .raw_download_operation import RawDownloadOperation
from .ml_upload_operation import MLUploadOperation  
from .ml_download_operation import MLDownloadOperation  

logger = logging.getLogger(__name__)


class CloudOperationService:
    """Dispatches cloud operations based on job type."""
    
    OPERATIONS: Dict[str, Type[CloudOperationTemplate]] = {
        "raw_download": RawDownloadOperation,
        "ml_upload": MLUploadOperation,
        "ml_download": MLDownloadOperation  # Placeholder for future implementation
    }
    
    def __init__(
        self,
        cloud_service: CloudInventoryService,
        local_service: LocalStateService,
        path_builder: CoordinatePathBuilder
    ):
        self.cloud_service = cloud_service
        self.local_service = local_service
        self.path_builder = path_builder
        self.gcs_client = storage.Client()
    
    def execute_transfer_job(self, job: TransferJob) -> OperationResult:
        """Execute a transfer job."""
        if job.operation_type not in self.OPERATIONS:
            return OperationResult(
                success=False,
                error=f"Unknown operation: {job.operation_type}"
            )

        logger.info(f"Starting operation: {job.operation_type} for coordinate {job.coordinate}, dry_run={job.dry_run}")
        # Instantiate operation with services
        operation_class = self.OPERATIONS[job.operation_type]
        operation = operation_class(
            self.cloud_service,
            self.local_service,
            self.path_builder,
            self.gcs_client
        )
        
        result = operation.execute_operation(job)
        logger.info(f"Operation result: success={result.success}, error={result.error}")
        return result