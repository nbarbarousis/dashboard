"""
Comprehensive tests for all data models.

This test suite validates that all models can be instantiated,
all methods work correctly, and serialization/deserialization works.
"""

import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock
import pandas as pd
import plotly.graph_objects as go

from src.models.data_models import (
    # Core domain models
    RunCoordinate, DataStatus, ProcessingStatus,
    
    # Data container models
    ExtractedData, AnalysisMetrics, AnalysisPlots,
    
    # State models
    CloudRawStatus, LocalRawStatus, ExtractionStatus, RunState,
    
    # Operation models
    DownloadJob, ExtractionJob, RunAnalysis,
    
    # Page-specific models
    TemporalData, CoverageStatistics,
    
    # Utility models
    OperationResult, CacheInfo, DashboardConfig
)


class TestRunCoordinate:
    """Test the core RunCoordinate model."""
    
    def test_run_coordinate_creation(self):
        """Test basic RunCoordinate instantiation."""
        coord = RunCoordinate(
            cid="client_001",
            regionid="region_xxx",
            fieldid="field_yyy",
            twid="tw_123",
            lbid="lb_456",
            timestamp="2025_08_12T08:53:56Z"
        )
        
        assert coord.cid == "client_001"
        assert coord.regionid == "region_xxx"
        assert coord.fieldid == "field_yyy"
        assert coord.twid == "tw_123"
        assert coord.lbid == "lb_456"
        assert coord.timestamp == "2025_08_12T08:53:56Z"
    
    def test_run_coordinate_from_filters(self):
        """Test RunCoordinate creation from filters dictionary."""
        filters = {
            'cid': 'client_001',
            'regionid': 'region_xxx',
            'fieldid': 'field_yyy',
            'twid': 'tw_123',
            'lbid': 'lb_456'
        }
        timestamp = "2025_08_12T08:53:56Z"
        
        coord = RunCoordinate.from_filters(filters, timestamp)
        
        assert coord.cid == "client_001"
        assert coord.regionid == "region_xxx"
        assert coord.fieldid == "field_yyy"
        assert coord.twid == "tw_123"
        assert coord.lbid == "lb_456"
        assert coord.timestamp == "2025_08_12T08:53:56Z"
    
    def test_run_coordinate_from_filters_missing_keys(self):
        """Test RunCoordinate creation with missing filter keys."""
        filters = {
            'cid': 'client_001',
            'regionid': 'region_xxx'
            # Missing fieldid, twid, lbid
        }
        timestamp = "2025_08_12T08:53:56Z"
        
        coord = RunCoordinate.from_filters(filters, timestamp)
        
        assert coord.cid == "client_001"
        assert coord.regionid == "region_xxx"
        assert coord.fieldid == ""
        assert coord.twid == ""
        assert coord.lbid == ""
        assert coord.timestamp == "2025_08_12T08:53:56Z"
    
    def test_to_path_str_default_separator(self):
        """Test path string conversion with default separator."""
        coord = RunCoordinate(
            cid="client_001",
            regionid="region_xxx",
            fieldid="field_yyy",
            twid="tw_123",
            lbid="lb_456",
            timestamp="2025_08_12T08:53:56Z"
        )
        
        expected = "client_001/region_xxx/field_yyy/tw_123/lb_456/2025_08_12T08:53:56Z"
        assert coord.to_path_str() == expected
    
    def test_to_path_str_custom_separator(self):
        """Test path string conversion with custom separator."""
        coord = RunCoordinate(
            cid="client_001",
            regionid="region_xxx",
            fieldid="field_yyy",
            twid="tw_123",
            lbid="lb_456",
            timestamp="2025_08_12T08:53:56Z"
        )
        
        expected = "client_001_region_xxx_field_yyy_tw_123_lb_456_2025_08_12T08:53:56Z"
        assert coord.to_path_str("_") == expected
    
    def test_to_path_tuple(self):
        """Test path tuple conversion."""
        coord = RunCoordinate(
            cid="client_001",
            regionid="region_xxx",
            fieldid="field_yyy",
            twid="tw_123",
            lbid="lb_456",
            timestamp="2025_08_12T08:53:56Z"
        )
        
        expected = ("client_001", "region_xxx", "field_yyy", "tw_123", "lb_456", "2025_08_12T08:53:56Z")
        assert coord.to_path_tuple() == expected
    
    def test_to_dict(self):
        """Test dictionary conversion."""
        coord = RunCoordinate(
            cid="client_001",
            regionid="region_xxx",
            fieldid="field_yyy",
            twid="tw_123",
            lbid="lb_456",
            timestamp="2025_08_12T08:53:56Z"
        )
        
        expected = {
            'cid': 'client_001',
            'regionid': 'region_xxx',
            'fieldid': 'field_yyy',
            'twid': 'tw_123',
            'lbid': 'lb_456',
            'timestamp': '2025_08_12T08:53:56Z'
        }
        assert coord.to_dict() == expected
    
    def test_from_path_tuple(self):
        """Test RunCoordinate creation from path tuple."""
        path_tuple = ("client_001", "region_xxx", "field_yyy", "tw_123", "lb_456", "2025_08_12T08:53:56Z")
        
        coord = RunCoordinate.from_path_tuple(path_tuple)
        
        assert coord.cid == "client_001"
        assert coord.regionid == "region_xxx"
        assert coord.fieldid == "field_yyy"
        assert coord.twid == "tw_123"
        assert coord.lbid == "lb_456"
        assert coord.timestamp == "2025_08_12T08:53:56Z"
    
    def test_from_path_tuple_invalid_length(self):
        """Test RunCoordinate creation from invalid path tuple."""
        path_tuple = ("client_001", "region_xxx")  # Too short
        
        with pytest.raises(ValueError, match="Path tuple must have 6 elements"):
            RunCoordinate.from_path_tuple(path_tuple)
    
    def test_str_representation(self):
        """Test string representation."""
        coord = RunCoordinate(
            cid="client_001",
            regionid="region_xxx",
            fieldid="field_yyy",
            twid="tw_123",
            lbid="lb_456",
            timestamp="2025_08_12T08:53:56Z"
        )
        
        expected = "RunCoordinate(client_001/region_xxx/field_yyy/tw_123/lb_456/2025_08_12T08:53:56Z)"
        assert str(coord) == expected
    
    def test_timestamp_formats(self):
        """Test both accepted timestamp formats."""
        filters = {'cid': 'c1', 'regionid': 'r1', 'fieldid': 'f1', 'twid': 't1', 'lbid': 'l1'}
        
        # Test underscore format
        coord1 = RunCoordinate.from_filters(filters, "2025_08_12T08:53:56Z")
        assert coord1.timestamp == "2025_08_12T08:53:56Z"
        
        # Test dash format
        coord2 = RunCoordinate.from_filters(filters, "2025-08-12T08:53:56Z")
        assert coord2.timestamp == "2025-08-12T08:53:56Z"


class TestEnums:
    """Test enum definitions."""
    
    def test_data_status_enum(self):
        """Test DataStatus enum values."""
        assert DataStatus.NOT_DOWNLOADED.value == "not_downloaded"
        assert DataStatus.DOWNLOADING.value == "downloading"
        assert DataStatus.DOWNLOADED.value == "downloaded"
        assert DataStatus.EXTRACTING.value == "extracting"
        assert DataStatus.EXTRACTED.value == "extracted"
        assert DataStatus.ANALYZING.value == "analyzing"
        assert DataStatus.ANALYZED.value == "analyzed"
        assert DataStatus.ERROR.value == "error"
    
    def test_processing_status_enum(self):
        """Test ProcessingStatus enum values."""
        assert ProcessingStatus.PENDING.value == "pending"
        assert ProcessingStatus.IN_PROGRESS.value == "in_progress"
        assert ProcessingStatus.COMPLETE.value == "complete"
        assert ProcessingStatus.FAILED.value == "failed"
        assert ProcessingStatus.CACHED.value == "cached"


class TestDataContainerModels:
    """Test data container models."""
    
    def test_extracted_data_creation(self):
        """Test ExtractedData model creation."""
        data = ExtractedData(
            frames_df=pd.DataFrame({'col1': [1, 2, 3]}),
            detections_df=pd.DataFrame({'col2': [4, 5, 6]}),
            metadata={'key': 'value'},
            source_bags=['bag1.bag', 'bag2.bag']
        )
        
        assert data.frames_df is not None
        assert data.detections_df is not None
        assert data.metadata == {'key': 'value'}
        assert data.source_bags == ['bag1.bag', 'bag2.bag']
    
    def test_extracted_data_defaults(self):
        """Test ExtractedData default values."""
        data = ExtractedData()
        
        assert data.frames_df is None
        assert data.detections_df is None
        assert data.tracking_df is None
        assert data.detections_json is None
        assert data.tracking_json is None
        assert data.metadata is None
        assert data.extraction_time is None
        assert data.source_bags == []
    
    def test_analysis_metrics_creation(self):
        """Test AnalysisMetrics model creation."""
        metrics = AnalysisMetrics(
            frame_fps_instant=[10.0, 15.0, 20.0],
            mean_detection_latency_ms=50.5,
            total_tracks=25
        )
        
        assert metrics.frame_fps_instant == [10.0, 15.0, 20.0]
        assert metrics.mean_detection_latency_ms == 50.5
        assert metrics.total_tracks == 25
    
    def test_analysis_plots_creation(self):
        """Test AnalysisPlots model creation."""
        fig = go.Figure()
        plots = AnalysisPlots(fps_figure=fig)
        
        assert plots.fps_figure is fig
        assert plots.stats_figure is None


class TestStateModels:
    """Test state models."""
    
    def test_cloud_raw_status_creation(self):
        """Test CloudRawStatus model creation."""
        status = CloudRawStatus(
            exists=True,
            bag_count=3,
            bag_names=['bag1.bag', 'bag2.bag', 'bag3.bag'],
            total_size=1024000
        )
        
        assert status.exists is True
        assert status.bag_count == 3
        assert status.bag_names == ['bag1.bag', 'bag2.bag', 'bag3.bag']
        assert status.total_size == 1024000
    
    def test_local_raw_status_creation(self):
        """Test LocalRawStatus model creation."""
        status = LocalRawStatus(
            downloaded=True,
            bag_count=2,
            bag_names=['bag1.bag', 'bag2.bag'],
            total_size=512000,
            path="/local/path"
        )
        
        assert status.downloaded is True
        assert status.bag_count == 2
        assert status.bag_names == ['bag1.bag', 'bag2.bag']
        assert status.total_size == 512000
        assert status.path == "/local/path"
    
    def test_extraction_status_creation(self):
        """Test ExtractionStatus model creation."""
        status = ExtractionStatus(
            extracted=True,
            files={"frames.csv": True, "detections.csv": False},
            path="/output/path"
        )
        
        assert status.extracted is True
        assert status.files == {"frames.csv": True, "detections.csv": False}
        assert status.path == "/output/path"
    
    def test_extraction_status_defaults(self):
        """Test ExtractionStatus default values."""
        status = ExtractionStatus(extracted=False)
        
        assert status.extracted is False
        assert status.files == {}
        assert status.path is None
    
    def test_run_state_creation(self):
        """Test RunState model creation."""
        coord = RunCoordinate("c1", "r1", "f1", "t1", "l1", "2025_08_12T08:53:56Z")
        cloud_status = CloudRawStatus(True, 3, ['b1', 'b2', 'b3'], 1024)
        local_status = LocalRawStatus(False, 0, [], 0)
        extraction_status = ExtractionStatus(False)
        
        run_state = RunState(
            coordinate=coord,
            cloud_raw_status=cloud_status,
            local_raw_status=local_status,
            extraction_status=extraction_status,
            pipeline_status=DataStatus.NOT_DOWNLOADED,
            next_action="download",
            ready_for_analysis=False
        )
        
        assert run_state.coordinate == coord
        assert run_state.cloud_raw_status == cloud_status
        assert run_state.local_raw_status == local_status
        assert run_state.extraction_status == extraction_status
        assert run_state.pipeline_status == DataStatus.NOT_DOWNLOADED
        assert run_state.next_action == "download"
        assert run_state.ready_for_analysis is False


class TestOperationModels:
    """Test operation models."""
    
    def test_download_job_creation(self):
        """Test DownloadJob model creation."""
        coord = RunCoordinate("c1", "r1", "f1", "t1", "l1", "2025_08_12T08:53:56Z")
        job = DownloadJob(
            job_id="job_123",
            coordinate=coord,
            source_bucket="my-bucket",
            target_path=Path("/target/path"),
            total_bytes=1000,
            bytes_downloaded=250
        )
        
        assert job.job_id == "job_123"
        assert job.coordinate == coord
        assert job.source_bucket == "my-bucket"
        assert job.target_path == Path("/target/path")
        assert job.total_bytes == 1000
        assert job.bytes_downloaded == 250
        assert job.status == ProcessingStatus.PENDING
    
    def test_download_job_progress_percent(self):
        """Test DownloadJob progress calculation."""
        coord = RunCoordinate("c1", "r1", "f1", "t1", "l1", "2025_08_12T08:53:56Z")
        
        # Test with data
        job = DownloadJob(
            job_id="job_123",
            coordinate=coord,
            source_bucket="bucket",
            target_path=Path("/path"),
            total_bytes=1000,
            bytes_downloaded=250
        )
        assert job.progress_percent == 25.0
        
        # Test with zero total bytes
        job.total_bytes = 0
        assert job.progress_percent == 0.0
    
    def test_extraction_job_creation(self):
        """Test ExtractionJob model creation."""
        coord = RunCoordinate("c1", "r1", "f1", "t1", "l1", "2025_08_12T08:53:56Z")
        job = ExtractionJob(
            job_id="extract_123",
            coordinate=coord,
            source_path=Path("/source"),
            output_path=Path("/output"),
            total_bags=5,
            bags_processed=2
        )
        
        assert job.job_id == "extract_123"
        assert job.coordinate == coord
        assert job.source_path == Path("/source")
        assert job.output_path == Path("/output")
        assert job.total_bags == 5
        assert job.bags_processed == 2
        assert job.status == ProcessingStatus.PENDING
    
    def test_run_analysis_creation(self):
        """Test RunAnalysis model creation."""
        coord = RunCoordinate("c1", "r1", "f1", "t1", "l1", "2025_08_12T08:53:56Z")
        
        analysis = RunAnalysis(
            coordinate=coord,
            status=DataStatus.ANALYZED,
            processing_status=ProcessingStatus.COMPLETE
        )
        
        assert analysis.coordinate == coord
        assert analysis.status == DataStatus.ANALYZED
        assert analysis.processing_status == ProcessingStatus.COMPLETE
        assert analysis.extracted_data is None
        assert analysis.metrics is None
        assert analysis.plots is None
        assert isinstance(analysis.created_at, datetime)
        assert isinstance(analysis.updated_at, datetime)


class TestPageSpecificModels:
    """Test page-specific models."""
    
    def test_temporal_data_creation(self):
        """Test TemporalData model creation."""
        data = TemporalData(
            timestamps=["2025_08_12T08:53:56Z", "2025_08_12T09:53:56Z"],
            raw_bags=[3, 2],
            ml_samples=[51, 34],
            gap_percentages=[0.0, 15.0],
            expected_samples_per_bag=17
        )
        
        assert data.timestamps == ["2025_08_12T08:53:56Z", "2025_08_12T09:53:56Z"]
        assert data.raw_bags == [3, 2]
        assert data.ml_samples == [51, 34]
        assert data.gap_percentages == [0.0, 15.0]
        assert data.expected_samples_per_bag == 17
    
    def test_coverage_statistics_creation(self):
        """Test CoverageStatistics model creation."""
        stats = CoverageStatistics(
            total_timestamps=100,
            total_raw_bags=300,
            total_ml_samples=4500,
            overall_coverage_pct=88.2,
            average_gap_pct=11.8,
            under_labeled_count=5,
            under_labeled_timestamps=[
                ("2025_08_12T08:53:56Z", 25.0, 3, 38),
                ("2025_08_12T09:53:56Z", 15.0, 2, 29)
            ]
        )
        
        assert stats.total_timestamps == 100
        assert stats.total_raw_bags == 300
        assert stats.total_ml_samples == 4500
        assert stats.overall_coverage_pct == 88.2
        assert stats.average_gap_pct == 11.8
        assert stats.under_labeled_count == 5
        assert len(stats.under_labeled_timestamps) == 2


class TestUtilityModels:
    """Test utility models."""
    
    def test_operation_result_creation(self):
        """Test OperationResult model creation."""
        result = OperationResult(
            success=True,
            result={"data": "value"},
            warning="Minor issue occurred"
        )
        
        assert result.success is True
        assert result.result == {"data": "value"}
        assert result.error is None
        assert result.warning == "Minor issue occurred"
        assert result.critical is False
    
    def test_cache_info_creation(self):
        """Test CacheInfo model creation."""
        now = datetime.now()
        info = CacheInfo(
            last_updated=now,
            size_bytes=1024,
            entry_count=50,
            cache_file="/cache/file.json"
        )
        
        assert info.last_updated == now
        assert info.size_bytes == 1024
        assert info.entry_count == 50
        assert info.cache_file == "/cache/file.json"


class TestConfigurationModels:
    """Test configuration models."""
    
    def test_dashboard_config_creation(self):
        """Test DashboardConfig model creation."""
        config = DashboardConfig(
            raw_data_root=Path("/data/raw"),
            processed_data_root=Path("/data/processed"),
            ml_data_root=Path("/data/ml"),
            cache_root=Path("/cache"),
            raw_bucket_name="raw-bucket",
            ml_bucket_name="ml-bucket",
            extraction_docker_image="extractor:latest"
        )
        
        assert config.raw_data_root == Path("/data/raw")
        assert config.processed_data_root == Path("/data/processed")
        assert config.ml_data_root == Path("/data/ml")
        assert config.cache_root == Path("/cache")
        assert config.raw_bucket_name == "raw-bucket"
        assert config.ml_bucket_name == "ml-bucket"
        assert config.extraction_docker_image == "extractor:latest"
        assert config.expected_samples_per_bag == 17  # Default
        assert config.cache_refresh_hours == 24  # Default
    
    def test_dashboard_config_from_dict(self):
        """Test DashboardConfig creation from dictionary."""
        config_dict = {
            'raw_data_root': '/data/raw',
            'processed_data_root': '/data/processed',
            'ml_data_root': '/data/ml',
            'cache_root': '/cache',
            'raw_bucket_name': 'raw-bucket',
            'ml_bucket_name': 'ml-bucket',
            'extraction_docker_image': 'extractor:latest',
            'expected_samples_per_bag': 20,
            'cache_refresh_hours': 12
        }
        
        config = DashboardConfig.from_dict(config_dict)
        
        assert config.raw_data_root == Path("/data/raw")
        assert config.processed_data_root == Path("/data/processed")
        assert config.ml_data_root == Path("/data/ml")
        assert config.cache_root == Path("/cache")
        assert config.raw_bucket_name == "raw-bucket"
        assert config.ml_bucket_name == "ml-bucket"
        assert config.extraction_docker_image == "extractor:latest"
        assert config.expected_samples_per_bag == 20  # Custom
        assert config.cache_refresh_hours == 12  # Custom
    
    def test_dashboard_config_from_dict_with_defaults(self):
        """Test DashboardConfig creation from dictionary with missing optional values."""
        config_dict = {
            'raw_data_root': '/data/raw',
            'processed_data_root': '/data/processed',
            'ml_data_root': '/data/ml',
            'cache_root': '/cache',
            'raw_bucket_name': 'raw-bucket',
            'ml_bucket_name': 'ml-bucket',
            'extraction_docker_image': 'extractor:latest'
            # Missing optional fields
        }
        
        config = DashboardConfig.from_dict(config_dict)
        
        assert config.expected_samples_per_bag == 17  # Default
        assert config.cache_refresh_hours == 24  # Default


# Integration tests
class TestModelIntegration:
    """Test model integration scenarios."""
    
    def test_complete_run_state_creation(self):
        """Test creating a complete RunState with all dependencies."""
        # Create coordinate
        coord = RunCoordinate.from_filters(
            {'cid': 'client_001', 'regionid': 'region_xxx', 'fieldid': 'field_yyy', 
             'twid': 'tw_123', 'lbid': 'lb_456'},
            "2025_08_12T08:53:56Z"
        )
        
        # Create status models
        cloud_status = CloudRawStatus(
            exists=True,
            bag_count=3,
            bag_names=['bag1.bag', 'bag2.bag', 'bag3.bag'],
            total_size=1024000
        )
        
        local_status = LocalRawStatus(
            downloaded=True,
            bag_count=3,
            bag_names=['bag1.bag', 'bag2.bag', 'bag3.bag'],
            total_size=1024000,
            path="/local/data/path"
        )
        
        extraction_status = ExtractionStatus(
            extracted=True,
            files={
                "frames.csv": True,
                "detections.csv": True,
                "tracking.csv": True
            },
            path="/local/processed/path"
        )
        
        # Create complete run state
        run_state = RunState(
            coordinate=coord,
            cloud_raw_status=cloud_status,
            local_raw_status=local_status,
            extraction_status=extraction_status,
            pipeline_status=DataStatus.EXTRACTED,
            next_action="analyze",
            ready_for_analysis=True
        )
        
        # Verify everything is connected correctly
        assert run_state.coordinate.to_path_str() == "client_001/region_xxx/field_yyy/tw_123/lb_456/2025_08_12T08:53:56Z"
        assert run_state.cloud_raw_status.exists is True
        assert run_state.local_raw_status.downloaded is True
        assert run_state.extraction_status.extracted is True
        assert run_state.pipeline_status == DataStatus.EXTRACTED
        assert run_state.next_action == "analyze"
        assert run_state.ready_for_analysis is True
    
    def test_complete_analysis_workflow(self):
        """Test creating a complete analysis workflow with all models."""
        # Create coordinate
        coord = RunCoordinate("c1", "r1", "f1", "t1", "l1", "2025_08_12T08:53:56Z")
        
        # Create extracted data
        extracted_data = ExtractedData(
            frames_df=pd.DataFrame({'timestamp': [1, 2, 3], 'frame_id': [10, 11, 12]}),
            detections_df=pd.DataFrame({'detection_id': [1, 2], 'confidence': [0.9, 0.8]}),
            metadata={'total_frames': 3, 'duration_seconds': 10.5},
            source_bags=['bag1.bag', 'bag2.bag']
        )
        
        # Create metrics
        metrics = AnalysisMetrics(
            frame_fps_instant=[10.0, 15.0, 12.0],
            detection_fps_instant=[8.0, 12.0, 10.0],
            mean_detection_latency_ms=45.2,
            total_tracks=15,
            avg_track_lifetime=2.3
        )
        
        # Create plots (mock)
        plots = AnalysisPlots(
            fps_figure=go.Figure(),
            stats_figure=go.Figure()
        )
        
        # Create complete analysis
        analysis = RunAnalysis(
            coordinate=coord,
            status=DataStatus.ANALYZED,
            processing_status=ProcessingStatus.COMPLETE,
            extracted_data=extracted_data,
            metrics=metrics,
            plots=plots,
            cache_path=Path("/cache/analysis.pkl")
        )
        
        # Verify complete workflow
        assert analysis.coordinate == coord
        assert analysis.status == DataStatus.ANALYZED
        assert analysis.processing_status == ProcessingStatus.COMPLETE
        assert analysis.extracted_data is not None
        assert analysis.metrics is not None
        assert analysis.plots is not None
        assert len(analysis.extracted_data.source_bags) == 2
        assert analysis.metrics.total_tracks == 15
        assert analysis.plots.fps_figure is not None
