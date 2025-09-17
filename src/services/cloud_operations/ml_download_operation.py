# src/services/cloud_operations/ml_download_operation.py
from typing import Dict, List
import time
import logging
from pathlib import Path

from src.models import (
    RunCoordinate, TransferJob, TransferPlan, OperationResult,
    CloudMLStatus, LocalMLStatus
)
from .base_template import CloudOperationTemplate

logger = logging.getLogger(__name__)


class MLDownloadOperation(CloudOperationTemplate):
    """
    Downloads ML samples (frames/labels) from cloud ML bucket to local filesystem.
    
    Selection criteria:
    - {"bag_names": ["_2025-08-04-10-33-23_0"]} - specific bags by cloud name
    - {"file_types": ["frames", "labels"]} - all files of certain types
    - {"all": True} - all available ML files
    """
    
    def _discover_source_state(self, coord: RunCoordinate) -> CloudMLStatus:
        """Get cloud ML status using model"""
        return self.cloud_service.get_ml_status(coord)
    
    def _discover_target_state(self, coord: RunCoordinate) -> LocalMLStatus:
        """Get local ML status using model"""
        return self.local_service.get_ml_status(coord)
    
    def _validate_source_state(self, source_state: CloudMLStatus) -> bool:
        """Check if cloud has ML data"""
        return source_state.exists and source_state.total_samples > 0
    
    def _apply_selection_filter(
        self, source_state: CloudMLStatus, criteria: Dict
    ) -> List[Dict]:
        """Apply selection to cloud ML files with support for combined criteria."""
        filtered_files = []
        
        # Determine selected bags
        if criteria.get("all", False):
            selected_bags = list(source_state.bag_files.keys())
        elif "bag_names" in criteria:
            # Note: bag_names here should be in cloud format (with underscore prefix)
            selected_bags = [bag for bag in criteria["bag_names"] 
                           if bag in source_state.bag_files]
        else:
            # No bag selection means no files
            return []
        
        # Determine selected file types (defaults to both if not specified)
        if "file_types" in criteria:
            selected_types = [ft for ft in criteria["file_types"] 
                            if ft in ["frames", "labels"]]
        else:
            selected_types = ["frames", "labels"]
        
        # Build file list based on both selections
        for bag_name in selected_bags:
            bag_data = source_state.bag_files.get(bag_name, {})
            
            for file_type in selected_types:
                if file_type not in bag_data:
                    continue
                    
                for filename, file_size in bag_data[file_type].items():
                    filtered_files.append({
                        'bag_name': bag_name,  # Cloud format
                        'file_type': file_type,
                        'filename': filename,
                        'size': file_size
                    })
        
        return filtered_files
    
    def _create_operation_plan(
        self,
        job: TransferJob,
        filtered_items: List[Dict],
        source_state: CloudMLStatus,
        target_state: LocalMLStatus
    ) -> TransferPlan:
        """Create detailed download plan"""
        files_to_transfer = []
        files_to_skip = []
        conflicts = []

        # Map cloud → local bag names
        cloud_local_names = {}
        for cloud_bag_name in source_state.bag_files.keys():
            local_bag_name = self.path_builder.translate_bag_name_cloud_to_local(cloud_bag_name)
            cloud_local_names[cloud_bag_name] = local_bag_name

        for file_item in filtered_items:
            cloud_bag_name = file_item['bag_name']
            file_type = file_item['file_type']
            filename = file_item['filename']
            cloud_size = file_item['size']
            
            # Get local bag name
            local_bag_name = cloud_local_names[cloud_bag_name]
            
            # Check if file exists locally
            local_exists = False
            local_size = 0
            
            if (local_bag_name in target_state.bag_files and 
                file_type in target_state.bag_files[local_bag_name] and
                filename in target_state.bag_files[local_bag_name][file_type]):
                
                local_exists = True
                local_size = target_state.bag_files[local_bag_name][file_type][filename]

            if not local_exists:
                # File doesn't exist locally - always download, no conflict
                files_to_transfer.append({
                    "cloud_bag_name": cloud_bag_name,
                    "local_bag_name": local_bag_name,
                    "file_type": file_type,
                    "filename": filename,
                    "size": cloud_size,
                    "conflict": False
                })
            else:
                # File exists locally - decide based on conflict resolution
                should_transfer, is_conflict = self._decide_file_action(
                    job, cloud_size, local_size
                )

                # Record conflicts if sizes mismatch
                if cloud_size != local_size:
                    conflicts.append({
                        "filename": filename,
                        "file_type": file_type,
                        "cloud_size": cloud_size,
                        "local_size": local_size,
                        "reason": "size_mismatch"
                    })

                if should_transfer:
                    files_to_transfer.append({
                        "cloud_bag_name": cloud_bag_name,
                        "local_bag_name": local_bag_name,
                        "file_type": file_type,
                        "filename": filename,
                        "size": cloud_size,
                        "conflict": is_conflict
                    })
                else:
                    files_to_skip.append({
                        "cloud_bag_name": cloud_bag_name,
                        "local_bag_name": local_bag_name,
                        "file_type": file_type,
                        "filename": filename,
                        "reason": "already_exists" if cloud_size == local_size else "conflict_skip",
                        "size": cloud_size
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
        bucket_name = self.path_builder.bucket_names['ml']
        bucket = self.gcs_client.bucket(bucket_name)
        
        results = []
        failed = 0
        total_bytes = 0
        start_time = time.time()
        
        for file_spec in plan.files_to_transfer:
            file_start = time.time()
            
            try:
                # Build paths
                cloud_path = self.path_builder.get_cloud_ml_file_path(
                    plan.coordinate, 
                    file_spec['cloud_bag_name'], 
                    file_spec['file_type'], 
                    file_spec['filename']
                )
                
                local_path = self.path_builder.get_local_ml_bag_path(
                    plan.coordinate, 
                    file_spec['local_bag_name'], 
                    file_spec['file_type'], 
                    file_spec['filename']
                )
                
                # Ensure local directory structure exists
                local_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Download from cloud
                blob = bucket.blob(str(cloud_path))
                
                logger.info(f"Downloading {file_spec['filename']} ({file_spec['file_type']})")
                blob.download_to_filename(str(local_path))
                
                # Verify download
                if local_path.exists():
                    actual_size = local_path.stat().st_size
                    if actual_size != file_spec['size']:
                        raise ValueError(
                            f"Size mismatch after download: expected {file_spec['size']}, "
                            f"got {actual_size}"
                        )
                
                duration = time.time() - file_start
                total_bytes += file_spec['size']
                
                results.append({
                    'file': file_spec['filename'],
                    'file_type': file_spec['file_type'],
                    'success': True,
                    'duration': duration,
                    'size': file_spec['size']
                })
                
            except Exception as e:
                failed += 1
                logger.error(f"Failed to download {file_spec['filename']}: {e}")
                
                results.append({
                    'file': file_spec['filename'],
                    'file_type': file_spec['file_type'],
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