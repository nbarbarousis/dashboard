"""
Comprehensive tests for LocalStateService.

This test suite validates all local filesystem state tracking operations
with both mock directories and real filesystem tests.
"""

import json
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, mock_open


from src.models import RunCoordinate
from src.models import ExtractionStatus, LocalRawStatus
from src.services.local_state_service import LocalStateService


# ============================================================================
# MODULE-LEVEL FIXTURES (accessible to all test classes)
# ============================================================================

@pytest.fixture
def temp_data_dirs():
    """Create temporary directories for testing."""
    temp_dir = Path(tempfile.mkdtemp())
    
    raw_root = temp_dir / "raw"
    processed_root = temp_dir / "processed"
    ml_root = temp_dir / "ML"
    
    # Create directory structure
    raw_root.mkdir(parents=True)
    processed_root.mkdir(parents=True)
    ml_root.mkdir(parents=True)
    (ml_root / "raw").mkdir()
    (ml_root / "annot").mkdir()
    (ml_root / "trainings").mkdir()
    
    yield {
        "temp_dir": temp_dir,
        "raw_root": raw_root,
        "processed_root": processed_root,
        "ml_root": ml_root
    }
    
    # Cleanup
    shutil.rmtree(temp_dir)

@pytest.fixture
def sample_coordinate():
    """Sample coordinate for testing."""
    return RunCoordinate(
        cid="client_001",
        regionid="region_xxx",
        fieldid="field_yyy",
        twid="TW_001",
        lbid="LB_0003",
        timestamp="2025_08_25T09:01:16Z"
    )

@pytest.fixture
def local_state_service(temp_data_dirs):
    """Create LocalStateService with temp directories."""
    return LocalStateService(
        raw_root=temp_data_dirs["raw_root"],
        processed_root=temp_data_dirs["processed_root"],
        ml_root=temp_data_dirs["ml_root"]
    )


# ============================================================================
# TEST CLASSES
# ============================================================================

class TestLocalStateService:
    """Test LocalStateService with mock and real filesystem operations."""
    
    def test_initialization(self, temp_data_dirs):
        """Test LocalStateService initialization."""
        service = LocalStateService(
            raw_root=temp_data_dirs["raw_root"],
            processed_root=temp_data_dirs["processed_root"],
            ml_root=temp_data_dirs["ml_root"]
        )
        
        assert service.raw_root == temp_data_dirs["raw_root"]
        assert service.processed_root == temp_data_dirs["processed_root"]
        assert service.ml_root == temp_data_dirs["ml_root"]
    
    def test_initialization_nonexistent_paths(self):
        """Test initialization with non-existent paths."""
        with patch('src.services.local_state_service.logger') as mock_logger:
            service = LocalStateService(
                raw_root=Path("/nonexistent/raw"),
                processed_root=Path("/nonexistent/processed"),
                ml_root=Path("/nonexistent/ml")
            )
            
            # Should log warnings but not fail
            assert mock_logger.warning.call_count == 3
            assert service.raw_root == Path("/nonexistent/raw")


class TestRawDataQueries:
    """Test raw data query methods."""
    
    def test_check_raw_downloaded_not_exists(self, local_state_service, sample_coordinate):
        """Test check_raw_downloaded when coordinate path doesn't exist."""
        status = local_state_service.check_raw_downloaded(sample_coordinate)
        
        assert isinstance(status, LocalRawStatus)
        assert status.downloaded is False
        assert status.bag_count == 0
        assert status.bag_names == []
        assert status.total_size == 0
        assert status.path is None

    def test_initialization_nonexistent_paths(self):
        """Test initialization with non-existent paths."""
        with patch('src.services.local_state_service.logger') as mock_logger:
            service = LocalStateService(
                raw_root=Path("/nonexistent/raw"),
                processed_root=Path("/nonexistent/processed"),
                ml_root=Path("/nonexistent/ml")
            )
            
            # Should log warnings but not fail
            assert mock_logger.warning.call_count == 3
            assert service.raw_root == Path("/nonexistent/raw")


class TestRawDataQueries:
    """Test raw data query methods."""
    
    def test_check_raw_downloaded_not_exists(self, local_state_service, sample_coordinate):
        """Test check_raw_downloaded when coordinate path doesn't exist."""
        status = local_state_service.check_raw_downloaded(sample_coordinate)
        
        assert isinstance(status, LocalRawStatus)
        assert status.downloaded is False
        assert status.bag_count == 0
        assert status.bag_names == []
        assert status.total_size == 0
        assert status.path is None
    
    def test_check_raw_downloaded_exists_no_bags(self, local_state_service, sample_coordinate, temp_data_dirs):
        """Test check_raw_downloaded when path exists but no bag files."""
        # Create coordinate directory but no bag files
        coord_path = local_state_service._build_coordinate_path(temp_data_dirs["raw_root"], sample_coordinate)
        coord_path.mkdir(parents=True)
        
        status = local_state_service.check_raw_downloaded(sample_coordinate)
        
        assert status.downloaded is False
        assert status.bag_count == 0
        assert status.bag_names == []
        assert status.total_size == 0
        assert status.path == str(coord_path)
    
    def test_check_raw_downloaded_with_bags(self, local_state_service, sample_coordinate, temp_data_dirs):
        """Test check_raw_downloaded with actual bag files."""
        # Create coordinate directory with bag files
        coord_path = local_state_service._build_coordinate_path(temp_data_dirs["raw_root"], sample_coordinate)
        coord_path.mkdir(parents=True)
        
        # Create test bag files
        bag1 = coord_path / "rosbag_2025-08-25-10-28-21_64.bag"
        bag2 = coord_path / "rosbag_2025-08-25-10-35-13_69.bag"
        bag3 = coord_path / "rosbag_2025-08-25-10-25-42_62.bag"
        
        bag1.write_bytes(b"0" * 1000)  # 1KB
        bag2.write_bytes(b"0" * 2000)  # 2KB
        bag3.write_bytes(b"0" * 1500)  # 1.5KB
        
        status = local_state_service.check_raw_downloaded(sample_coordinate)
        
        assert status.downloaded is True
        assert status.bag_count == 3
        assert sorted(status.bag_names) == [
            "rosbag_2025-08-25-10-25-42_62.bag",
            "rosbag_2025-08-25-10-28-21_64.bag", 
            "rosbag_2025-08-25-10-35-13_69.bag"
        ]
        assert status.total_size == 4500  # 1000 + 2000 + 1500
        assert status.path == str(coord_path)
    
    def test_check_raw_downloaded_with_mixed_files(self, local_state_service, sample_coordinate, temp_data_dirs):
        """Test check_raw_downloaded with mixed file types (only counts .bag files)."""
        coord_path = local_state_service._build_coordinate_path(temp_data_dirs["raw_root"], sample_coordinate)
        coord_path.mkdir(parents=True)
        
        # Create bag files and other files
        bag1 = coord_path / "test.bag"
        other_file = coord_path / "test.txt"
        another_file = coord_path / "metadata.json"
        
        bag1.write_bytes(b"0" * 1000)
        other_file.write_text("not a bag file")
        another_file.write_text('{"key": "value"}')
        
        status = local_state_service.check_raw_downloaded(sample_coordinate)
        
        assert status.downloaded is True
        assert status.bag_count == 1
        assert status.bag_names == ["test.bag"]
        assert status.total_size == 1000
    

class TestExtractionQueries:
    """Test extraction query methods."""
    
    def test_check_extracted_not_exists(self, local_state_service, sample_coordinate):
        """Test check_extracted when coordinate path doesn't exist."""
        status = local_state_service.check_extracted(sample_coordinate)
        
        assert isinstance(status, ExtractionStatus)
        assert status.extracted is False
        assert status.files == {}
        assert status.path is None
    
    def test_check_extracted_exists_no_files(self, local_state_service, sample_coordinate, temp_data_dirs):
        """Test check_extracted when path exists but no extraction files."""
        coord_path = local_state_service._build_coordinate_path(temp_data_dirs["processed_root"], sample_coordinate)
        coord_path.mkdir(parents=True)
        
        status = local_state_service.check_extracted(sample_coordinate)
        
        assert status.extracted is False
        expected_files = {
            "frames.csv": False,
            "detections.csv": False,
            "tracking.csv": False,
            "detections.json": False,
            "tracking.json": False,
            "metadata.json": False
        }
        assert status.files == expected_files
        assert status.path is None
    
    def test_check_extracted_with_some_files(self, local_state_service, sample_coordinate, temp_data_dirs):
        """Test check_extracted with some extraction files present."""
        coord_path = local_state_service._build_coordinate_path(temp_data_dirs["processed_root"], sample_coordinate)
        coord_path.mkdir(parents=True)
        
        # Create subdirectory structure (like real extraction)
        bag_subdir = coord_path / "rosbag_2025-08-25-10-28-21_64"
        bag_subdir.mkdir()
        
        # Create some extraction files
        (bag_subdir / "frames.csv").write_text("timestamp,frame_id\n1,100\n")
        (bag_subdir / "detections.json").write_text('{"detections": []}')
        
        status = local_state_service.check_extracted(sample_coordinate)
        
        assert status.extracted is True
        expected_files = {
            "frames.csv": True,
            "detections.csv": False,
            "tracking.csv": False,
            "detections.json": True,
            "tracking.json": False,
            "metadata.json": False
        }
        assert status.files == expected_files
        assert status.path == str(coord_path)
    
    def test_check_extracted_with_all_files(self, local_state_service, sample_coordinate, temp_data_dirs):
        """Test check_extracted with all extraction files present."""
        coord_path = local_state_service._build_coordinate_path(temp_data_dirs["processed_root"], sample_coordinate)
        coord_path.mkdir(parents=True)
        
        # Create subdirectory structure
        bag_subdir = coord_path / "rosbag_2025-08-25-10-28-21_64"
        bag_subdir.mkdir()
        
        # Create all extraction files
        (bag_subdir / "frames.csv").write_text("data")
        (bag_subdir / "detections.csv").write_text("data")
        (bag_subdir / "tracking.csv").write_text("data")
        (bag_subdir / "detections.json").write_text('{}')
        (bag_subdir / "tracking.json").write_text('{}')
        (bag_subdir / "metadata.json").write_text('{}')
        
        status = local_state_service.check_extracted(sample_coordinate)
        
        assert status.extracted is True
        expected_files = {
            "frames.csv": True,
            "detections.csv": True,
            "tracking.csv": True,
            "detections.json": True,
            "tracking.json": True,
            "metadata.json": True
        }
        assert status.files == expected_files
        assert status.path == str(coord_path)
    
    def test_get_extraction_output_info_not_exists(self, local_state_service, sample_coordinate):
        """Test get_extraction_output_info when path doesn't exist."""
        info = local_state_service.get_extraction_output_info(sample_coordinate)
        
        expected = {
            "exists": False,
            "path": None,
            "subdirectories": [],
            "total_files": 0,
            "total_size": 0,
            "file_details": {}
        }
        assert info == expected
    
    def test_get_extraction_output_info_with_files(self, local_state_service, sample_coordinate, temp_data_dirs):
        """Test get_extraction_output_info with actual files."""
        coord_path = local_state_service._build_coordinate_path(temp_data_dirs["processed_root"], sample_coordinate)
        coord_path.mkdir(parents=True)
        
        # Create complex subdirectory structure
        bag1_dir = coord_path / "rosbag_2025-08-25-10-28-21_64"
        bag2_dir = coord_path / "rosbag_2025-08-25-10-35-13_69"
        images_dir = bag1_dir / "images"
        
        bag1_dir.mkdir()
        bag2_dir.mkdir()
        images_dir.mkdir()
        
        # Create various files
        (bag1_dir / "frames.csv").write_bytes(b"0" * 1000)
        (bag1_dir / "detections.json").write_bytes(b"0" * 500)
        (bag2_dir / "tracking.csv").write_bytes(b"0" * 750)
        (images_dir / "image1.jpg").write_bytes(b"0" * 2000)
        (images_dir / "image2.jpg").write_bytes(b"0" * 1500)
        
        info = local_state_service.get_extraction_output_info(sample_coordinate)
        
        assert info["exists"] is True
        assert info["path"] == str(coord_path)
        assert sorted(info["subdirectories"]) == [
            "rosbag_2025-08-25-10-28-21_64",
            "rosbag_2025-08-25-10-28-21_64/images",
            "rosbag_2025-08-25-10-35-13_69"
        ]
        assert info["total_files"] == 5
        assert info["total_size"] == 5750  # 1000 + 500 + 750 + 2000 + 1500
        
        # Check file details
        expected_files = {
            "rosbag_2025-08-25-10-28-21_64/frames.csv",
            "rosbag_2025-08-25-10-28-21_64/detections.json",
            "rosbag_2025-08-25-10-35-13_69/tracking.csv",
            "rosbag_2025-08-25-10-28-21_64/images/image1.jpg",
            "rosbag_2025-08-25-10-28-21_64/images/image2.jpg"
        }
        assert set(info["file_details"].keys()) == expected_files


class TestMLDataQueries:
    """Test ML data query methods."""
    
    def test_check_ml_exported_not_exists(self, local_state_service, sample_coordinate):
        """Test check_ml_exported when ML/raw path doesn't exist."""
        result = local_state_service.check_ml_exported(sample_coordinate)
        
        expected = {
            "exists": False,
            "path": None,
            "file_counts": {},
            "total_size": 0,
            "subdirectories": [],
            "sample_files": []
        }
        assert result == expected

    def test_check_ml_exported_with_data(self, local_state_service, sample_coordinate, temp_data_dirs):
        """Test check_ml_exported with exported samples in ML/raw."""
        # Create ML/raw coordinate path
        ml_raw_path = local_state_service._build_coordinate_path(temp_data_dirs["ml_root"] / "raw", sample_coordinate)
        ml_raw_path.mkdir(parents=True)
        
        # Create sample structure
        export_dir = ml_raw_path / "export_batch_001"
        export_dir.mkdir()
        
        # Create sample files
        (export_dir / "sample_001.jpg").write_bytes(b"0" * 1000)
        (export_dir / "sample_001.json").write_bytes(b"0" * 200)
        (export_dir / "sample_002.jpg").write_bytes(b"0" * 1200)
        (export_dir / "sample_002.json").write_bytes(b"0" * 300)
        
        result = local_state_service.check_ml_exported(sample_coordinate)
        
        assert result["exists"] is True
        assert result["path"] == str(ml_raw_path)
        assert result["total_size"] == 2700  # 1000 + 200 + 1200 + 300
        assert result["subdirectories"] == ["export_batch_001"]
        
        # Check sample files
        sample_files = result["sample_files"]
        assert len(sample_files) == 4
        
        # Find specific files
        jpg_files = [f for f in sample_files if f["extension"] == ".jpg"]
        json_files = [f for f in sample_files if f["extension"] == ".json"]
        
        assert len(jpg_files) == 2
        assert len(json_files) == 2
        assert sum(f["size"] for f in jpg_files) == 2200  # 1000 + 1200
        assert sum(f["size"] for f in json_files) == 500   # 200 + 300
    
    def test_get_export_ids_file_not_exists(self, local_state_service):
        """Test get_export_ids when tracking file doesn't exist."""
        with patch('src.services.local_state_service.logger') as mock_logger:
            export_ids = local_state_service.get_export_ids()
            
            assert export_ids == []
            mock_logger.info.assert_called()
    
    def test_get_export_ids_with_exports_key(self, local_state_service, temp_data_dirs):
        """Test get_export_ids with exports key in JSON."""
        tracking_file = temp_data_dirs["ml_root"] / "raw" / ".export_tracking.json"
        
        tracking_data = {
            "exports": {
                "export_001": {"created": "2025-08-25", "samples": 100},
                "export_002": {"created": "2025-08-26", "samples": 150},
                "export_003": {"created": "2025-08-27", "samples": 200}
            },
            "metadata": {"version": "1.0"}
        }
        
        with open(tracking_file, 'w') as f:
            json.dump(tracking_data, f)
        
        export_ids = local_state_service.get_export_ids()
        
        assert export_ids == ["export_001", "export_002", "export_003"]
    
    def test_get_export_ids_root_level_exports(self, local_state_service, temp_data_dirs):
        """Test get_export_ids with exports at root level."""
        tracking_file = temp_data_dirs["ml_root"] / "raw" / ".export_tracking.json"
        
        tracking_data = {
            "export_001": {"created": "2025-08-25", "samples": 100},
            "export_002": {"created": "2025-08-26", "samples": 150},
            "metadata": {"version": "1.0"},
            "last_updated": "2025-08-27"
        }
        
        with open(tracking_file, 'w') as f:
            json.dump(tracking_data, f)
        
        export_ids = local_state_service.get_export_ids()
        
        # Should exclude metadata keys
        assert export_ids == ["export_001", "export_002"]
    
    def test_get_export_info_file_not_exists(self, local_state_service):
        """Test get_export_info when tracking file doesn't exist."""
        result = local_state_service.get_export_info("export_001")
        
        expected = {
            "exists": False,
            "error": "Export tracking file not found"
        }
        assert result == expected
    
    def test_get_export_info_export_not_found(self, local_state_service, temp_data_dirs):
        """Test get_export_info when export ID doesn't exist."""
        tracking_file = temp_data_dirs["ml_root"] / "raw" / ".export_tracking.json"
        
        tracking_data = {
            "exports": {
                "export_001": {"created": "2025-08-25", "samples": 100}
            }
        }
        
        with open(tracking_file, 'w') as f:
            json.dump(tracking_data, f)
        
        result = local_state_service.get_export_info("export_999")
        
        expected = {
            "exists": False,
            "error": "Export ID 'export_999' not found"
        }
        assert result == expected
    
    def test_get_export_info_success(self, local_state_service, temp_data_dirs):
        """Test get_export_info with successful retrieval."""
        tracking_file = temp_data_dirs["ml_root"] / "raw" / ".export_tracking.json"
        
        tracking_data = {
            "exports": {
                "export_001": {
                    "created": "2025-08-25",
                    "samples": 100,
                    "coordinates": ["client_001/region_xxx/field_yyy"]
                }
            }
        }
        
        with open(tracking_file, 'w') as f:
            json.dump(tracking_data, f)
        
        result = local_state_service.get_export_info("export_001")
        
        assert result["exists"] is True
        assert result["export_id"] == "export_001"
        assert result["info"]["created"] == "2025-08-25"
        assert result["info"]["samples"] == 100


class TestHelperMethods:
    """Test helper methods."""
    
    def test_build_coordinate_path(self, local_state_service, sample_coordinate, temp_data_dirs):
        """Test _build_coordinate_path helper method."""
        path = local_state_service._build_coordinate_path(temp_data_dirs["raw_root"], sample_coordinate)
        
        expected = temp_data_dirs["raw_root"] / "client_001" / "region_xxx" / "field_yyy" / "TW_001" / "LB_0003" / "2025_08_25T09:01:16Z"
        assert path == expected
    
    def test_count_files_in_path_empty(self, local_state_service, temp_data_dirs):
        """Test _count_files_in_path with empty directory."""
        empty_dir = temp_data_dirs["temp_dir"] / "empty"
        empty_dir.mkdir()
        
        counts = local_state_service._count_files_in_path(empty_dir)
        
        assert counts == {"total": 0}
    
    def test_count_files_in_path_with_files(self, local_state_service, temp_data_dirs):
        """Test _count_files_in_path with various file types."""
        test_dir = temp_data_dirs["temp_dir"] / "test_files"
        test_dir.mkdir()
        
        # Create files with different extensions
        (test_dir / "file1.txt").write_text("content")
        (test_dir / "file2.txt").write_text("content")
        (test_dir / "image.jpg").write_text("content")
        (test_dir / "data.csv").write_text("content")
        (test_dir / "no_extension").write_text("content")
        
        # Create subdirectory with more files
        subdir = test_dir / "subdir"
        subdir.mkdir()
        (subdir / "nested.json").write_text("content")
        
        counts = local_state_service._count_files_in_path(test_dir)
        
        assert counts["total"] == 6
        assert counts[".txt"] == 2
        assert counts[".jpg"] == 1
        assert counts[".csv"] == 1
        assert counts[".json"] == 1
        assert counts["no_extension"] == 1
    

class TestRealFilesystemIntegration:
    """Integration tests with real filesystem data."""
    
    def test_with_real_data_structure(self):
        """Test LocalStateService with real data structure."""
        # Test with actual data paths if they exist
        real_data_root = Path("/home/nikbarb/data-annot-pipeline/data")
        
        if real_data_root.exists():
            service = LocalStateService(
                raw_root=real_data_root / "raw",
                processed_root=real_data_root / "processed",
                ml_root=real_data_root / "ML"
            )
            
            # Test with a real coordinate from the filesystem
            coord = RunCoordinate(
                cid="client_001",
                regionid="region_xxx", 
                fieldid="field_xxx",
                twid="TW_001",
                lbid="LB_0003",
                timestamp="2025_08_25T09:01:16Z"
            )
            
            # Check raw status
            raw_status = service.check_raw_downloaded(coord)
            
            # Should find actual bag files
            if raw_status.downloaded:
                assert raw_status.bag_count > 0
                assert len(raw_status.bag_names) > 0
                assert raw_status.total_size > 0
                assert raw_status.path is not None
                
                # All bag files should end with .bag
                for bag_name in raw_status.bag_names:
                    assert bag_name.endswith('.bag')
        else:
            # Skip test if real data doesn't exist
            pytest.skip("Real data directory not available")


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_check_raw_downloaded_exception_handling(self, local_state_service, sample_coordinate):
        """Test check_raw_downloaded handles general exceptions."""
        with patch.object(local_state_service, '_build_coordinate_path', side_effect=Exception("Unexpected error")):
            with patch('src.services.local_state_service.logger') as mock_logger:
                status = local_state_service.check_raw_downloaded(sample_coordinate)
                
                # Should return safe defaults
                assert status.downloaded is False
                assert status.bag_count == 0
                assert status.bag_names == []
                assert status.total_size == 0
                assert status.path is None
                mock_logger.error.assert_called()
    
    def test_check_extracted_exception_handling(self, local_state_service, sample_coordinate):
        """Test check_extracted handles general exceptions."""
        with patch.object(local_state_service, '_build_coordinate_path', side_effect=Exception("Unexpected error")):
            with patch('src.services.local_state_service.logger') as mock_logger:
                status = local_state_service.check_extracted(sample_coordinate)
                
                # Should return safe defaults
                assert status.extracted is False
                assert status.files == {}
                assert status.path is None
                mock_logger.error.assert_called()
    
    def test_get_export_ids_json_decode_error(self, local_state_service, temp_data_dirs):
        """Test get_export_ids handles JSON decode errors."""
        tracking_file = temp_data_dirs["ml_root"] / "raw" / ".export_tracking.json"
        
        # Write invalid JSON
        with open(tracking_file, 'w') as f:
            f.write("{ invalid json content")
        
        with patch('src.services.local_state_service.logger') as mock_logger:
            export_ids = local_state_service.get_export_ids()
            
            assert export_ids == []
            mock_logger.error.assert_called()
    
    def test_get_export_info_json_decode_error(self, local_state_service, temp_data_dirs):
        """Test get_export_info handles JSON decode errors."""
        tracking_file = temp_data_dirs["ml_root"] / "raw" / ".export_tracking.json"
        
        # Write invalid JSON
        with open(tracking_file, 'w') as f:
            f.write("{ invalid json content")
        
        result = local_state_service.get_export_info("export_001")
        
        assert result["exists"] is False
        assert "error" in result
        """Test check_ml_exported with exported samples in ML/raw."""
        # Create ML/raw coordinate path
        ml_raw_path = local_state_service._build_coordinate_path(temp_data_dirs["ml_root"] / "raw", sample_coordinate)
        ml_raw_path.mkdir(parents=True)
        
        # Create sample structure
        export_dir = ml_raw_path / "export_batch_001"
        export_dir.mkdir()
        
        # Create sample files
        (export_dir / "sample_001.jpg").write_bytes(b"0" * 1000)
        (export_dir / "sample_001.json").write_bytes(b"0" * 200)
        (export_dir / "sample_002.jpg").write_bytes(b"0" * 1200)
        (export_dir / "sample_002.json").write_bytes(b"0" * 300)
        
        result = local_state_service.check_ml_exported(sample_coordinate)
        
        assert result["exists"] is True
        assert result["path"] == str(ml_raw_path)
        assert result["total_size"] == 2700  # 1000 + 200 + 1200 + 300
        assert result["subdirectories"] == ["export_batch_001"]
        
        # Check sample files
        sample_files = result["sample_files"]
        assert len(sample_files) == 4
        
        # Find specific files
        jpg_files = [f for f in sample_files if f["extension"] == ".jpg"]
        json_files = [f for f in sample_files if f["extension"] == ".json"]
        
        assert len(jpg_files) == 2
        assert len(json_files) == 2
        assert sum(f["size"] for f in jpg_files) == 2200  # 1000 + 1200
        assert sum(f["size"] for f in json_files) == 500   # 200 + 300
    
    def test_get_export_ids_file_not_exists(self, local_state_service):
        """Test get_export_ids when tracking file doesn't exist."""
        with patch('src.services.local_state_service.logger') as mock_logger:
            export_ids = local_state_service.get_export_ids()
            
            assert export_ids == []
            mock_logger.info.assert_called()
    
    def test_get_export_ids_with_exports_key(self, local_state_service, temp_data_dirs):
        """Test get_export_ids with exports key in JSON."""
        tracking_file = temp_data_dirs["ml_root"] / "raw" / ".export_tracking.json"
        
        tracking_data = {
            "exports": {
                "export_001": {"created": "2025-08-25", "samples": 100},
                "export_002": {"created": "2025-08-26", "samples": 150},
                "export_003": {"created": "2025-08-27", "samples": 200}
            },
            "metadata": {"version": "1.0"}
        }
        
        with open(tracking_file, 'w') as f:
            json.dump(tracking_data, f)
        
        export_ids = local_state_service.get_export_ids()
        
        assert export_ids == ["export_001", "export_002", "export_003"]
    
    def test_get_export_ids_root_level_exports(self, local_state_service, temp_data_dirs):
        """Test get_export_ids with exports at root level."""
        tracking_file = temp_data_dirs["ml_root"] / "raw" / ".export_tracking.json"
        
        tracking_data = {
            "export_001": {"created": "2025-08-25", "samples": 100},
            "export_002": {"created": "2025-08-26", "samples": 150},
            "metadata": {"version": "1.0"},
            "last_updated": "2025-08-27"
        }
        
        with open(tracking_file, 'w') as f:
            json.dump(tracking_data, f)
        
        export_ids = local_state_service.get_export_ids()
        
        # Should exclude metadata keys
        assert export_ids == ["export_001", "export_002"]
    
    def test_get_export_info_file_not_exists(self, local_state_service):
        """Test get_export_info when tracking file doesn't exist."""
        result = local_state_service.get_export_info("export_001")
        
        expected = {
            "exists": False,
            "error": "Export tracking file not found"
        }
        assert result == expected
    
    def test_get_export_info_export_not_found(self, local_state_service, temp_data_dirs):
        """Test get_export_info when export ID doesn't exist."""
        tracking_file = temp_data_dirs["ml_root"] / "raw" / ".export_tracking.json"
        
        tracking_data = {
            "exports": {
                "export_001": {"created": "2025-08-25", "samples": 100}
            }
        }
        
        with open(tracking_file, 'w') as f:
            json.dump(tracking_data, f)
        
        result = local_state_service.get_export_info("export_999")
        
        expected = {
            "exists": False,
            "error": "Export ID 'export_999' not found"
        }
        assert result == expected
    
    def test_get_export_info_success(self, local_state_service, temp_data_dirs):
        """Test get_export_info with valid export ID."""
        tracking_file = temp_data_dirs["ml_root"] / "raw" / ".export_tracking.json"
        
        export_info = {
            "created": "2025-08-25T10:30:00Z",
            "samples": 150,
            "coordinates": ["client_001/region_xxx/field_yyy/TW_001/LB_0003/2025_08_25T09:01:16Z"],
            "total_size": 5000000
        }
        
        tracking_data = {
            "exports": {
                "export_001": export_info
            }
        }
        
        with open(tracking_file, 'w') as f:
            json.dump(tracking_data, f)
        
        result = local_state_service.get_export_info("export_001")
        
        expected = {
            "exists": True,
            "export_id": "export_001",
            "info": export_info
        }
        assert result == expected


class TestHelperMethods:
    """Test helper methods."""
    
    def test_build_coordinate_path(self, local_state_service, sample_coordinate, temp_data_dirs):
        """Test _build_coordinate_path method."""
        path = local_state_service._build_coordinate_path(temp_data_dirs["raw_root"], sample_coordinate)
        
        expected = (
            temp_data_dirs["raw_root"] / 
            "client_001" / "region_xxx" / "field_yyy" / 
            "TW_001" / "LB_0003" / "2025_08_25T09:01:16Z"
        )
        assert path == expected
    
    def test_count_files_in_path_empty(self, local_state_service, temp_data_dirs):
        """Test _count_files_in_path with empty directory."""
        empty_dir = temp_data_dirs["temp_dir"] / "empty"
        empty_dir.mkdir()
        
        counts = local_state_service._count_files_in_path(empty_dir)
        
        assert counts == {"total": 0}
    
    def test_count_files_in_path_with_files(self, local_state_service, temp_data_dirs):
        """Test _count_files_in_path with various file types."""
        test_dir = temp_data_dirs["temp_dir"] / "test_files"
        test_dir.mkdir()
        
        # Create subdirectory with files
        subdir = test_dir / "subdir"
        subdir.mkdir()
        
        # Create files with various extensions
        (test_dir / "file1.csv").write_text("data")
        (test_dir / "file2.json").write_text("data")
        (test_dir / "file3.jpg").write_bytes(b"data")
        (test_dir / "file_no_ext").write_text("data")
        (subdir / "file4.csv").write_text("data")
        (subdir / "file5.txt").write_text("data")
        
        counts = local_state_service._count_files_in_path(test_dir)
        
        expected = {
            "total": 6,
            ".csv": 2,
            ".json": 1,
            ".jpg": 1,
            ".txt": 1,
            "no_extension": 1
        }
        assert counts == expected
    


class TestRealFilesystemIntegration:
    """Integration tests with real filesystem data."""
    
    def test_with_real_data_structure(self):
        """Test LocalStateService with real data structure."""
        # Use the actual data paths
        real_data_root = Path("/home/nikbarb/data-annot-pipeline/data")
        
        if not real_data_root.exists():
            pytest.skip("Real data directory not available")
        
        service = LocalStateService(
            raw_root=real_data_root / "raw",
            processed_root=real_data_root / "processed",
            ml_root=real_data_root / "ML"
        )
        
        # Test with known coordinate from real data
        real_coord = RunCoordinate(
            cid="client_001",
            regionid="region_xxx",
            fieldid="field_xxx",
            twid="TW_001",
            lbid="LB_0003",
            timestamp="2025_08_25T09:01:16Z"
        )
        
        # Test raw data check
        raw_status = service.check_raw_downloaded(real_coord)
        if raw_status.downloaded:
            assert raw_status.bag_count > 0
            assert len(raw_status.bag_names) > 0
            assert raw_status.total_size > 0
            assert raw_status.path is not None
        
        # Test processed data check
        extraction_status = service.check_extracted(real_coord)
        # May or may not be extracted, but should not raise errors
        assert isinstance(extraction_status, ExtractionStatus)
        
        # Test ML data check
        ml_status = service.check_ml_exported(real_coord)
        # May or may not exist, but should not raise errors
        assert isinstance(ml_status, dict)
        assert "exists" in ml_status
        
        # Test export tracking
        export_ids = service.get_export_ids()
        # May be empty but should not raise errors
        assert isinstance(export_ids, list)


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_check_raw_downloaded_exception_handling(self, local_state_service, sample_coordinate):
        """Test error handling in check_raw_downloaded."""
        with patch.object(local_state_service, '_build_coordinate_path', side_effect=Exception("Test error")):
            with patch('src.services.local_state_service.logger') as mock_logger:
                status = local_state_service.check_raw_downloaded(sample_coordinate)
                
                # Should return safe default values
                assert status.downloaded is False
                assert status.bag_count == 0
                assert status.bag_names == []
                assert status.total_size == 0
                assert status.path is None
                
                mock_logger.error.assert_called()
    
    def test_check_extracted_exception_handling(self, local_state_service, sample_coordinate):
        """Test error handling in check_extracted."""
        with patch.object(local_state_service, '_build_coordinate_path', side_effect=Exception("Test error")):
            with patch('src.services.local_state_service.logger') as mock_logger:
                status = local_state_service.check_extracted(sample_coordinate)
                
                # Should return safe default values
                assert status.extracted is False
                assert status.files == {}
                assert status.path is None
                
                mock_logger.error.assert_called()
    
    def test_get_export_ids_json_decode_error(self, local_state_service, temp_data_dirs):
        """Test get_export_ids with invalid JSON."""
        tracking_file = temp_data_dirs["ml_root"] / "raw" / ".export_tracking.json"
        
        # Write invalid JSON
        with open(tracking_file, 'w') as f:
            f.write("invalid json content")
        
        with patch('src.services.local_state_service.logger') as mock_logger:
            export_ids = local_state_service.get_export_ids()
            
            assert export_ids == []
            mock_logger.error.assert_called()
    
    def test_get_export_info_json_decode_error(self, local_state_service, temp_data_dirs):
        """Test get_export_info with invalid JSON."""
        tracking_file = temp_data_dirs["ml_root"] / "raw" / ".export_tracking.json"
        
        # Write invalid JSON
        with open(tracking_file, 'w') as f:
            f.write("invalid json content")
        
        with patch('src.services.local_state_service.logger') as mock_logger:
            result = local_state_service.get_export_info("export_001")
            
            assert result["exists"] is False
            assert "error" in result
            mock_logger.error.assert_called()
