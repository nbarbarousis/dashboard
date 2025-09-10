# src/services/rosbag_service.py
"""
Service for rosbag extraction operations
Handles Docker-based extraction and data loading
"""

import subprocess
import json
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime
import pandas as pd
import uuid

from models.data_models import (
    RunCoordinate, ExtractedData, ExtractionJob, 
    ProcessingStatus, DataStatus
)

logger = logging.getLogger(__name__)


class RosbagService:
    """
    Handles rosbag extraction operations.
    Wraps Docker-based extraction and manages extracted data.
    """
    
    def __init__(self, raw_root: Path, processed_root: Path, docker_image: str = "rosbag-extractor"):
        """
        Initialize rosbag service
        
        Args:
            raw_root: Root directory for raw rosbag data
            processed_root: Root directory for processed/extracted data
            docker_image: Name of Docker image for extraction
        """
        self.raw_root = Path(raw_root)
        self.processed_root = Path(processed_root)
        self.docker_image = docker_image
        self._ensure_docker_image()
    
    def _ensure_docker_image(self):
        """Ensure Docker image exists, build if necessary"""
        try:
            # Check if image exists
            result = subprocess.run(
                ["docker", "images", "-q", self.docker_image],
                capture_output=True,
                text=True,
                check=False
            )
            
            if not result.stdout.strip():
                logger.info(f"Docker image {self.docker_image} not found, building...")
                self._build_docker_image()
            else:
                logger.info(f"Docker image {self.docker_image} found")
        except Exception as e:
            logger.error(f"Failed to check Docker image: {e}")
    
    def _build_docker_image(self):
        """Build the Docker image for extraction"""
        # NOTE: In production, this would build from the docker/ directory
        # For now, we assume the image exists or user builds manually
        logger.warning("Docker image build not implemented - please build manually")
    
    def check_extraction_status(self, coord: RunCoordinate) -> Dict:
        """
        Check if data has already been extracted for a run
        
        Args:
            coord: Run coordinate
            
        Returns:
            Dictionary with status and file information
        """
        processed_path = self._get_processed_path(coord)
        
        if not processed_path.exists():
            return {
                'status': DataStatus.NOT_DOWNLOADED,
                'path': None,
                'files': {}
            }
        
        # Check for expected output files
        frames_csv = processed_path / "frames.csv"
        detections_csv = processed_path / "detections.csv"
        tracking_csv = processed_path / "tracking.csv"
        metadata_yaml = processed_path / "metadata.yaml"
        
        files = {
            'frames': frames_csv.exists(),
            'detections': detections_csv.exists(),
            'tracking': tracking_csv.exists(),
            'metadata': metadata_yaml.exists()
        }
        
        # Determine overall status
        if all(files.values()):
            status = DataStatus.EXTRACTED
        elif any(files.values()):
            status = DataStatus.DOWNLOADED  # Partial extraction
        else:
            status = DataStatus.DOWNLOADED  # No extraction yet
        
        return {
            'status': status,
            'path': str(processed_path),
            'files': files
        }
    
    def create_extraction_job(self, coord: RunCoordinate, 
                            force: bool = False,
                            extract_params: Optional[Dict] = None) -> ExtractionJob:
        """
        Create an extraction job for a run
        
        Args:
            coord: Run coordinate
            force: Force re-extraction even if data exists
            extract_params: Optional extraction parameters
            
        Returns:
            ExtractionJob object
        """
        # Check current status
        status = self.check_extraction_status(coord)
        
        if status['status'] == DataStatus.EXTRACTED and not force:
            # Already extracted
            job = ExtractionJob(
                job_id=str(uuid.uuid4()),
                coordinate=coord,
                source_path=self._get_raw_path(coord),
                output_path=self._get_processed_path(coord),
                status=ProcessingStatus.COMPLETE
            )
            return job
        
        # Create new extraction job
        job = ExtractionJob(
            job_id=str(uuid.uuid4()),
            coordinate=coord,
            source_path=self._get_raw_path(coord),
            output_path=self._get_processed_path(coord),
            extract_params=extract_params or {},
            status=ProcessingStatus.PENDING
        )
        
        # Count bags to process
        raw_path = self._get_raw_path(coord)
        if raw_path.exists():
            bags = list(raw_path.glob("*.bag"))
            job.total_bags = len(bags)
        
        return job
    
    def execute_extraction(self, job: ExtractionJob) -> ExtractionJob:
        """
        Execute extraction job using Docker
        
        Args:
            job: Extraction job to execute
            
        Returns:
            Updated job with results
        """
        job.status = ProcessingStatus.IN_PROGRESS
        job.started_at = datetime.now()
        
        try:
            # Ensure output directory exists
            job.output_path.mkdir(parents=True, exist_ok=True)
          
            # Build Docker command
            docker_cmd = [
                "docker", "run", "--rm",
                "-v", f"{job.source_path.parent.absolute()}:/data:ro",
                "--mount", f"type=bind,src={job.output_path.absolute()},dst=/output",
                "-e", f"TIMESTAMP={job.coordinate.timestamp}",
                self.docker_image
            ]
            
            # Run extraction
            logger.info(f"Running extraction for {job.coordinate.timestamp}")
            result = subprocess.run(
                docker_cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            job.docker_output = result.stdout
            job.status = ProcessingStatus.COMPLETE
            job.completed_at = datetime.now()
            
            # Count extracted items
            self._update_extraction_stats(job)
            
            logger.info(f"Extraction completed for {job.coordinate.timestamp}")
            
        except subprocess.CalledProcessError as e:
            job.status = ProcessingStatus.FAILED
            job.error_message = str(e)
            job.docker_output = e.stderr if e.stderr else e.stdout
            logger.error(f"Extraction failed: {e}")
            
        except Exception as e:
            job.status = ProcessingStatus.FAILED
            job.error_message = str(e)
            logger.error(f"Extraction error: {e}")
        
        return job
    
    def load_extracted_data(self, coord: RunCoordinate) -> Optional[ExtractedData]:
        """
        Load extracted data from disk
        
        Args:
            coord: Run coordinate
            
        Returns:
            ExtractedData object or None if not available
        """
        processed_path = self._get_processed_path(coord)
        
        if not processed_path.exists():
            logger.warning(f"No processed data found at {processed_path}")
            return None
        
        data = ExtractedData()
        
        # Load CSVs
        frames_csv = processed_path / "frames.csv"
        if frames_csv.exists():
            data.frames_df = pd.read_csv(frames_csv)
            logger.debug(f"Loaded {len(data.frames_df)} frames")
        
        detections_csv = processed_path / "detections.csv"
        if detections_csv.exists():
            data.detections_df = pd.read_csv(detections_csv)
            logger.debug(f"Loaded {len(data.detections_df)} detection messages")
        
        tracking_csv = processed_path / "tracking.csv"
        if tracking_csv.exists():
            data.tracking_df = pd.read_csv(tracking_csv)
            logger.debug(f"Loaded {len(data.tracking_df)} tracking messages")
        
        # Load JSON files
        detections_json = processed_path / "detections_full.json"
        if detections_json.exists():
            with open(detections_json, 'r') as f:
                data.detections_json = json.load(f)
        
        tracking_json = processed_path / "tracking_full.json"
        if tracking_json.exists():
            with open(tracking_json, 'r') as f:
                data.tracking_json = json.load(f)
        
        # Load metadata
        metadata_yaml = processed_path / "metadata.yaml"
        if metadata_yaml.exists():
            with open(metadata_yaml, 'r') as f:
                data.metadata = yaml.safe_load(f)
                if data.metadata:
                    data.source_bags = [bag['name'] for bag in data.metadata.get('bags', [])]
        
        data.extraction_time = datetime.now()
        
        return data
    
    def get_available_bags(self, coord: RunCoordinate) -> List[str]:
        """
        Get list of available bag files for a run
        
        Args:
            coord: Run coordinate
            
        Returns:
            List of bag file names
        """
        raw_path = self._get_raw_path(coord)
        
        if not raw_path.exists():
            return []
        
        bags = sorted(raw_path.glob("rosbag_*.bag"))
        return [bag.name for bag in bags]
    
    def _get_raw_path(self, coord: RunCoordinate) -> Path:
        """Get raw data path for coordinate"""
        return self.raw_root / coord.to_path_str()
    
    def _get_processed_path(self, coord: RunCoordinate) -> Path:
        """Get processed data path for coordinate"""
        return self.processed_root / coord.to_path_str()
    
    def _update_extraction_stats(self, job: ExtractionJob):
        """Update extraction statistics from output files"""
        try:
            # Count frames
            frames_csv = job.output_path / "frames.csv"
            if frames_csv.exists():
                df = pd.read_csv(frames_csv)
                job.frames_extracted = len(df)
            
            # Count detections
            detections_csv = job.output_path / "detections.csv"
            if detections_csv.exists():
                df = pd.read_csv(detections_csv)
                job.detections_extracted = df['num_detections'].sum() if 'num_detections' in df.columns else len(df)
            
            # Count processed bags
            metadata_yaml = job.output_path / "metadata.yaml"
            if metadata_yaml.exists():
                with open(metadata_yaml, 'r') as f:
                    metadata = yaml.safe_load(f)
                    if metadata and 'bags' in metadata:
                        job.bags_processed = len(metadata['bags'])
                        
        except Exception as e:
            logger.warning(f"Failed to update extraction stats: {e}")