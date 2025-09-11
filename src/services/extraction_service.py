"""
Service for Docker-based rosbag extraction operations
Handles Docker orchestration and container management
"""
import subprocess
import logging
import yaml
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
import uuid
import pandas as pd

from models.data_models import (
    RunCoordinate, ExtractionJob, ProcessingStatus
)

logger = logging.getLogger(__name__)


class ExtractionService:
    """
    Handles Docker-based rosbag extraction operations.
    Manages Docker images, containers, and extraction jobs.
    """
    
    def __init__(self, docker_image: str = "rosbag-extractor", docker_dir: Optional[Path] = None):
        """
        Initialize extraction service
        
        Args:
            docker_image: Name of Docker image for extraction
            docker_dir: Directory containing Dockerfile and scripts
        """
        self.docker_image = docker_image
        self.docker_dir = docker_dir or Path(__file__).parent.parent / "docker"
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
        if not self.docker_dir.exists():
            raise ValueError(f"Docker directory not found: {self.docker_dir}")
        
        dockerfile_path = self.docker_dir / "Dockerfile"
        if not dockerfile_path.exists():
            raise ValueError(f"Dockerfile not found: {dockerfile_path}")
        
        try:
            logger.info(f"Building Docker image {self.docker_image}...")
            result = subprocess.run(
                ["docker", "build", "-t", self.docker_image, str(self.docker_dir)],
                capture_output=True,
                text=True,
                check=True
            )
            logger.info(f"Docker image {self.docker_image} built successfully")
            logger.debug(f"Build output: {result.stdout}")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to build Docker image: {e}")
            logger.error(f"Build stderr: {e.stderr}")
            raise
    
    def create_extraction_job(self, coord: RunCoordinate, 
                            source_path: Path,
                            output_path: Path,
                            extract_params: Optional[Dict] = None) -> ExtractionJob:
        """
        Create an extraction job
        
        Args:
            coord: Run coordinate
            source_path: Path to raw bag files
            output_path: Path for extracted output
            extract_params: Optional extraction parameters
            
        Returns:
            ExtractionJob object
        """
        job = ExtractionJob(
            job_id=str(uuid.uuid4()),
            coordinate=coord,
            source_path=source_path,
            output_path=output_path,
            extract_params=extract_params or {},
            status=ProcessingStatus.PENDING
        )
        
        # Count bags to process
        if source_path.exists():
            bags = list(source_path.glob("*.bag"))
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
            logger.debug(f"Docker command: {' '.join(docker_cmd)}")
            
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