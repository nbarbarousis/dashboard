# src/services/download_service.py
"""
Download service - handles downloading rosbags from cloud storage
"""

import logging
import uuid
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from models.data_models import (
    RunCoordinate, DownloadJob, ProcessingStatus
)
from services.gcs_service import GCSService

logger = logging.getLogger(__name__)


class DownloadService:
    """
    Handles download operations from cloud storage.
    Wraps cloud_download.py functionality as a service.
    """
    
    def __init__(self, gcs_service: GCSService, raw_root: Path):
        """
        Initialize download service
        
        Args:
            gcs_service: GCS service for cloud operations
            raw_root: Root directory for raw data storage
        """
        self.gcs_service = gcs_service
        self.raw_root = Path(raw_root)
        self.active_jobs: Dict[str, DownloadJob] = {}
    
    def check_download_status(self, coord: RunCoordinate) -> Dict:
        """
        Check if rosbags are downloaded for a run
        
        Args:
            coord: Run coordinate
            
        Returns:
            Status dictionary
        """
        raw_path = self._get_raw_path(coord)
        
        if not raw_path.exists():
            return {
                'downloaded': False,
                'path': None,
                'bag_count': 0,
                'total_size': 0
            }
        
        # Count bag files
        bags = list(raw_path.glob("rosbag_*.bag"))
        
        if not bags:
            return {
                'downloaded': False,
                'path': str(raw_path),
                'bag_count': 0,
                'total_size': 0
            }
        
        # Calculate total size
        total_size = sum(bag.stat().st_size for bag in bags)
        
        return {
            'downloaded': True,
            'path': str(raw_path),
            'bag_count': len(bags),
            'total_size': total_size,
            'bags': [bag.name for bag in bags]
        }
    
    def create_download_job(self, coord: RunCoordinate,
                          conflict_resolution: str = "skip") -> DownloadJob:
        """
        Create a download job for a run
        
        Args:
            coord: Run coordinate
            conflict_resolution: How to handle existing files
            
        Returns:
            DownloadJob object
        """
        # Check what needs to be downloaded
        cloud_files = self._discover_cloud_files(coord)
        local_status = self.check_download_status(coord)
        
        # Determine files to download
        files_to_download = []
        total_bytes = 0
        
        for cloud_file in cloud_files:
            file_name = cloud_file['name']
            file_size = cloud_file['size']
            
            # Check if file exists locally
            local_path = self._get_raw_path(coord) / file_name
            
            if local_path.exists():
                if conflict_resolution == "skip":
                    continue
                elif conflict_resolution == "overwrite":
                    files_to_download.append(file_name)
                    total_bytes += file_size
            else:
                files_to_download.append(file_name)
                total_bytes += file_size
        
        # Create job
        job = DownloadJob(
            job_id=str(uuid.uuid4()),
            coordinate=coord,
            source_bucket=self.gcs_service.bucket_names.get('raw', ''),
            target_path=self._get_raw_path(coord),
            files_to_download=files_to_download,
            total_files=len(files_to_download),
            total_bytes=total_bytes,
            status=ProcessingStatus.PENDING
        )
        
        # Store in active jobs
        self.active_jobs[job.job_id] = job
        
        return job
    
    def execute_download(self, job: DownloadJob) -> DownloadJob:
        """
        Execute a download job
        
        Args:
            job: Download job to execute
            
        Returns:
            Updated job with results
        """
        job.status = ProcessingStatus.IN_PROGRESS
        job.started_at = datetime.now()
        
        try:
            # Ensure target directory exists
            job.target_path.mkdir(parents=True, exist_ok=True)
            
            # Download each file
            for file_name in job.files_to_download:
                success = self._download_file(
                    job.coordinate, 
                    file_name, 
                    job.target_path
                )
                
                if success:
                    job.files_downloaded += 1
                    # Update bytes_downloaded (would need actual tracking)
                    job.bytes_downloaded = int(
                        (job.files_downloaded / job.total_files) * job.total_bytes
                    )
                else:
                    logger.warning(f"Failed to download {file_name}")
            
            # Update status
            if job.files_downloaded == job.total_files:
                job.status = ProcessingStatus.COMPLETE
                logger.info(f"Download complete: {job.files_downloaded} files")
            else:
                job.status = ProcessingStatus.FAILED
                job.error_message = f"Only downloaded {job.files_downloaded}/{job.total_files} files"
            
        except Exception as e:
            job.status = ProcessingStatus.FAILED
            job.error_message = str(e)
            logger.error(f"Download failed: {e}")
        
        job.completed_at = datetime.now()
        return job
    
    def get_job_status(self, job_id: str) -> Optional[DownloadJob]:
        """Get status of a download job"""
        return self.active_jobs.get(job_id)
    
    def _discover_cloud_files(self, coord: RunCoordinate) -> List[Dict]:
        """
        Discover files in cloud for a run
        
        Args:
            coord: Run coordinate
            
        Returns:
            List of file info dictionaries
        """
        # This would integrate with the cloud_download.py logic
        # For now, return placeholder
        logger.info(f"Discovering cloud files for {coord.timestamp}")
        
        # In production, this would:
        # 1. Use GCS service to list blobs
        # 2. Filter for .bag files
        # 3. Return file info
        
        return []  # Placeholder
    
    def _download_file(self, coord: RunCoordinate, 
                      file_name: str, 
                      target_path: Path) -> bool:
        """
        Download a single file
        
        Args:
            coord: Run coordinate
            file_name: Name of file to download
            target_path: Target directory
            
        Returns:
            True if successful
        """
        # This would integrate with the cloud_download.py logic
        # For now, return placeholder
        logger.info(f"Downloading {file_name} to {target_path}")
        
        # In production, this would:
        # 1. Use GCS client to download blob
        # 2. Save to target_path / file_name
        # 3. Verify download
        
        return True  # Placeholder
    
    def _get_raw_path(self, coord: RunCoordinate) -> Path:
        """Get raw data path for coordinate"""
        return self.raw_root / coord.to_path_str()