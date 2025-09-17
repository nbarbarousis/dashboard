# src/services/operations/raw_download_operation.py
from typing import Dict, List
import time
import logging


from src.models import (
    RunCoordinate, TransferJob, TransferPlan, OperationResult,
    CloudRawStatus, LocalRawStatus
)
from .base_template import CloudOperationTemplate

logger = logging.getLogger(__name__)


class RawDownloadOperation(CloudOperationTemplate):
    """
    Downloads raw rosbags from cloud to local filesystem.
    
    Selection criteria:
    - {"bag_indices": [0, 2, 4]} - specific bags by index
    - {"all": True} - all available bags
    """
    
    def _discover_source_state(self, coord: RunCoordinate) -> CloudRawStatus:
        """Get cloud raw status using model"""
        return self.cloud_service.get_raw_status(coord)
    
    def _discover_target_state(self, coord: RunCoordinate) -> LocalRawStatus:
        """Get local raw status using model"""
        return self.local_service.get_raw_status(coord)
    
    def _validate_source_state(self, source_state: CloudRawStatus) -> bool:
        """Check if cloud has bags"""
        return source_state.exists and source_state.bag_count > 0
    
    def _apply_selection_filter(
        self, source_state: CloudRawStatus, criteria: Dict
    ) -> List[str]:
        """Apply selection to cloud bag names"""
        if criteria.get("all", False):
            return source_state.bag_names
        
        selected = []
        
        if "bag_indices" in criteria:
            for idx in criteria["bag_indices"]:
                if 0 <= idx < len(source_state.bag_names):
                    selected.append(source_state.bag_names[idx])
                else:
                    logger.warning(f"Index {idx} out of range")
        
        return selected
    
    def _create_operation_plan(
        self,
        job: TransferJob,
        filtered_items: List[str],
        source_state: CloudRawStatus,
        target_state: LocalRawStatus
    ) -> TransferPlan:
        files_to_transfer = []
        files_to_skip = []
        conflicts = []

        # map local → cloud
        local_cloud_names = {}
        for local_name in target_state.bag_names:
            cloud_name = self.path_builder.translate_bag_name_local_to_cloud(local_name)
            local_cloud_names[cloud_name] = {
                "local_name": local_name,
                "size": target_state.bag_sizes.get(local_name, 0)
            }

        for cloud_bag_name in filtered_items:
            cloud_size = source_state.bag_sizes.get(cloud_bag_name, 0)
            local_bag_name = self.path_builder.translate_bag_name_cloud_to_local(
                cloud_bag_name
            )

            if cloud_bag_name in local_cloud_names:
                local_info = local_cloud_names[cloud_bag_name]
                local_size = local_info["size"]

                # decide
                should_transfer, is_conflict = self._decide_file_action(
                    job, cloud_size, local_size
                )

                # always record mismatches in the conflicts list
                if is_conflict:
                    reason = "size_mismatch" if cloud_size != local_size else job.conflict_resolution
                    conflicts.append({
                        "name": cloud_bag_name,
                        "cloud_size": cloud_size,
                        "local_size": local_size,
                        "reason": reason
                    })

                if should_transfer:
                    files_to_transfer.append({
                        "cloud_name": cloud_bag_name,
                        "local_name": local_bag_name,
                        "size": cloud_size,
                        "conflict": is_conflict
                    })
                else:
                    files_to_skip.append({
                        "cloud_name": cloud_bag_name,
                        "local_name": local_bag_name,
                        "reason": "exists",
                        "size": cloud_size
                    })

            else:
                # no local copy → always transfer, no conflict flag
                files_to_transfer.append({
                    "cloud_name": cloud_bag_name,
                    "local_name": local_bag_name,
                    "size": cloud_size,
                    "conflict": False
                })

        total_size = sum(f["size"] for f in files_to_transfer)
        return TransferPlan(
            coordinate=job.coordinate,
            files_to_transfer=files_to_transfer,
            files_to_skip=files_to_skip,
            conflicts=conflicts,
            total_size=total_size,
            total_files=len(files_to_transfer)
        )
    
    def _execute_transfer(
        self, plan: TransferPlan, job: TransferJob
    ) -> OperationResult:
        """Execute downloads"""

        bucket_name = self.path_builder.bucket_names['raw']
        bucket = self.gcs_client.bucket(bucket_name)
        
        results = []
        failed = 0
        total_bytes = 0
        start_time = time.time()
        
        for file_spec in plan.files_to_transfer:
            file_start = time.time()
            
            try:
                # Build paths
                cloud_path = self.path_builder.get_cloud_raw_bag_path(
                    plan.coordinate, file_spec['cloud_name']
                )
                local_path = self.path_builder.get_local_raw_bag_path(
                    plan.coordinate, file_spec['local_name']
                )
                
                # Ensure directory exists
                local_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Download
                blob = bucket.blob(str(cloud_path))
                
                logger.info(f"Downloading {file_spec['cloud_name']}")
                blob.download_to_filename(str(local_path))
                
                # Verify
                if local_path.exists():
                    actual_size = local_path.stat().st_size
                    if actual_size != file_spec['size']:
                        raise ValueError(
                            f"Size mismatch: expected {file_spec['size']}, "
                            f"got {actual_size}"
                        )
                
                duration = time.time() - file_start
                total_bytes += file_spec['size']
                
                results.append({
                    'file': file_spec['cloud_name'],
                    'success': True,
                    'duration': duration,
                    'size': file_spec['size']
                })
                
            except Exception as e:
                failed += 1
                logger.error(f"Failed to download {file_spec['cloud_name']}: {e}")
                
                results.append({
                    'file': file_spec['cloud_name'],
                    'success': False,
                    'error': str(e)
                })
                
                # Clean up partial download
                if 'local_path' in locals() and local_path.exists():
                    try:
                        local_path.unlink()
                    except:
                        pass
        
        total_duration = time.time() - start_time
        
        return OperationResult(
            success=failed == 0,
            result={
                'plan': plan,
                'results': results,
                'summary': {
                    'total_files': len(plan.files_to_transfer),
                    'successful': len(plan.files_to_transfer) - failed,
                    'failed': failed,
                    'total_bytes': total_bytes,
                    'total_duration': total_duration,
                    'average_speed_mbps': (total_bytes / (1024*1024)) / total_duration if total_duration > 0 else 0
                }
            },
            warning=f"{failed} files failed" if failed > 0 else None
        )