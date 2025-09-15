"""
Comprehensive tests for CloudInventoryService.

Tests cloud data caching, hierarchy queries, and coordinate-specific queries
with mock GCS data and real cache persistence.
"""

import json
import pytest
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from src.models import RunCoordinate
from src.models import CloudRawStatus, CloudMLStatus
from src.models.pages import TimelineData
from src.models.config import CacheInfo
from src.services.cloud_inventory_service import CloudInventoryService

# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def temp_cache_file():
    """Create temporary cache file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        # Initialize with empty JSON instead of leaving empty
        json.dump({}, f)
        cache_path = Path(f.name)
    
    yield str(cache_path)
    
    # Cleanup
    if cache_path.exists():
        cache_path.unlink()

@pytest.fixture
def bucket_names():
    """Sample bucket configuration."""
    return {
        'raw': 'test-raw-bucket',
        'ml': 'test-ml-bucket',
        'processed': 'test-processed-bucket'
    }

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
def mock_gcs_client():
    """Mock GCS client with sample data."""
    mock_client = Mock()
    
    # Mock raw bucket data — now includes the rosbag/ folder
    raw_bucket = Mock()
    raw_blobs = []
    
    # Create properly configured mock blobs
    blob1 = Mock()
    blob1.name = "client_001/region_xxx/field_yyy/TW_001/LB_0003/2025_08_25T09:01:16Z/rosbag/bag1.bag"
    blob1.size = 1000
    
    blob2 = Mock()
    blob2.name = "client_001/region_xxx/field_yyy/TW_001/LB_0003/2025_08_25T09:01:16Z/rosbag/bag2.bag"
    blob2.size = 2000
    
    blob3 = Mock()
    blob3.name = "client_001/region_xxx/field_yyy/TW_001/LB_0003/2025_08_26T10:15:30Z/rosbag/bag3.bag"
    blob3.size = 1500
    
    raw_blobs = [blob1, blob2, blob3]
    raw_bucket.list_blobs.return_value = raw_blobs
    
    # Mock ML bucket data
    ml_bucket = Mock()
    ml_blobs = []
    
    # Create ML mock blobs
    ml_blob1 = Mock()
    ml_blob1.name = "raw/client_001/region_xxx/field_yyy/TW_001/LB_0003/2025_08_25T09:01:16Z/rosbag/_2025-08-25-09-01-16_0/frames/001.jpg"
    ml_blob1.size = 50
    
    ml_blob2 = Mock()
    ml_blob2.name = "raw/client_001/region_xxx/field_yyy/TW_001/LB_0003/2025_08_25T09:01:16Z/rosbag/_2025-08-25-09-01-16_0/labels/001.txt"
    ml_blob2.size = 10
    
    ml_blob3 = Mock()
    ml_blob3.name = "raw/client_001/region_xxx/field_yyy/TW_001/LB_0003/2025_08_25T09:01:16Z/rosbag/_2025-08-25-09-01-16_0/frames/002.jpg"
    ml_blob3.size = 50
    
    ml_blob4 = Mock()
    ml_blob4.name = "raw/client_001/region_xxx/field_yyy/TW_001/LB_0003/2025_08_25T09:01:16Z/rosbag/_2025-08-25-09-01-16_0/labels/002.txt"
    ml_blob4.size = 10
    
    ml_blobs = [ml_blob1, ml_blob2, ml_blob3, ml_blob4]
    ml_bucket.list_blobs.return_value = ml_blobs
    
    def mock_bucket(bucket_name):
        if bucket_name == 'test-raw-bucket':
            return raw_bucket
        elif bucket_name == 'test-ml-bucket':
            return ml_bucket
        else:
            return Mock(list_blobs=Mock(return_value=[]))
    
    mock_client.bucket = mock_bucket
    return mock_client

@pytest.fixture
def cloud_service(bucket_names, temp_cache_file, mock_gcs_client):
    """Create CloudInventoryService with mocked GCS client."""
    with patch('src.services.cloud_inventory_service.storage.Client', return_value=mock_gcs_client):
        service = CloudInventoryService(bucket_names, temp_cache_file)
        return service

# ============================================================================
# TEST CLASSES
# ============================================================================

class TestCloudInventoryService:
    """Test CloudInventoryService initialization and basic functionality."""
    
    def test_initialization(self, bucket_names, temp_cache_file):
        """Test service initialization."""
        with patch('src.services.cloud_inventory_service.storage.Client'):
            service = CloudInventoryService(bucket_names, temp_cache_file)
            
            assert service.bucket_names == bucket_names
            assert service.cache_file == Path(temp_cache_file)
            assert service._inventory_cache is None or service._inventory_cache == {}
            assert service._is_stale is False
    
    def test_initialization_creates_cache_directory(self, bucket_names):
        """Test that cache directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_file = Path(temp_dir) / "subdir" / "cache.json"
            
            with patch('src.services.cloud_inventory_service.storage.Client'):
                service = CloudInventoryService(bucket_names, str(cache_file))
                
                assert cache_file.parent.exists()


class TestCacheManagement:
    """Test cache loading, saving, and management operations."""
    
    def test_get_full_inventory_empty_cache(self, cloud_service):
        """Test get_full_inventory with no existing cache."""
        # Force refresh to populate cache
        inventory = cloud_service.get_full_inventory(force_refresh=True)
        
        assert isinstance(inventory, dict)
        assert 'raw' in inventory
        assert 'ml' in inventory
        assert 'metadata' in inventory
    
    def test_cache_persistence(self, cloud_service):
        """Test that cache is saved and loaded correctly."""
        # Force refresh to create cache
        cloud_service.get_full_inventory(force_refresh=True)
        
        # Create new service instance with same cache file
        with patch('src.services.cloud_inventory_service.storage.Client'):
            new_service = CloudInventoryService(cloud_service.bucket_names, str(cloud_service.cache_file))
            
            # Should load existing cache
            inventory = new_service.get_full_inventory()
            assert 'raw' in inventory
            assert 'metadata' in inventory
    
    def test_mark_stale(self, cloud_service):
        """Test cache invalidation."""
        cloud_service.mark_stale("test reason")
        assert cloud_service._is_stale is True
    
    def test_get_cache_info_no_cache(self, cloud_service):
        """Test cache info when no cache exists."""
        # Delete cache file if it exists
        if cloud_service.cache_file.exists():
            cloud_service.cache_file.unlink()
        
        cache_info = cloud_service.get_cache_info()
        
        assert cache_info.entry_count == 0
        assert cache_info.size_bytes == 0
        assert cache_info.last_updated == datetime.min
    
    def test_get_cache_info_with_cache(self, cloud_service):
        """Test cache info with existing cache."""
        # Create cache
        cloud_service.get_full_inventory(force_refresh=True)
        
        cache_info = cloud_service.get_cache_info()
        
        assert cache_info.entry_count > 0
        assert cache_info.size_bytes > 0
        assert cache_info.last_updated > datetime.min


class TestHierarchyQueries:
    """Test hierarchy navigation and path existence checks."""
    
    def test_get_hierarchy_level_cid(self, cloud_service):
        """Test getting CID level options."""
        cloud_service.get_full_inventory(force_refresh=True)
        
        cids = cloud_service.get_hierarchy_level('cid', (), 'raw')
        
        assert 'client_001' in cids
        assert isinstance(cids, list)
    
    def test_get_hierarchy_level_with_parent_path(self, cloud_service):
        """Test getting hierarchy level with parent path."""
        cloud_service.get_full_inventory(force_refresh=True)
        
        regions = cloud_service.get_hierarchy_level('regionid', ('client_001',), 'raw')
        
        assert 'region_xxx' in regions
    
    def test_get_hierarchy_level_nonexistent_bucket(self, cloud_service):
        """Test hierarchy query on nonexistent bucket."""
        result = cloud_service.get_hierarchy_level('cid', (), 'nonexistent')
        
        assert result == []
    
    def test_path_exists_valid_path(self, cloud_service, sample_coordinate):
        """Test path existence check with valid path."""
        cloud_service.get_full_inventory(force_refresh=True)
        
        path = sample_coordinate.to_path_tuple()
        exists = cloud_service.path_exists(path, 'raw')
        
        assert exists is True
    
    def test_path_exists_invalid_path(self, cloud_service):
        """Test path existence check with invalid path."""
        cloud_service.get_full_inventory(force_refresh=True)
        
        path = ('nonexistent', 'path', 'components')
        exists = cloud_service.path_exists(path, 'raw')
        
        assert exists is False


class TestCoordinateQueries:
    """Test coordinate-specific data queries."""
    
    def test_get_raw_bags_info_exists(self, cloud_service, sample_coordinate):
        """Test getting raw bag info for existing coordinate."""
        cloud_service.get_full_inventory(force_refresh=True)
        
        status = cloud_service.get_raw_bags_info(sample_coordinate)
        
        assert isinstance(status, CloudRawStatus)
        assert status.exists is True
        assert status.bag_count == 2
        assert 'bag1.bag' in status.bag_names
        assert 'bag2.bag' in status.bag_names
        assert status.total_size == 3000  # 1000 + 2000
    
    def test_get_raw_bags_info_not_exists(self, cloud_service):
        """Test getting raw bag info for non-existing coordinate."""
        cloud_service.get_full_inventory(force_refresh=True)
        
        nonexistent_coord = RunCoordinate(
            cid="nonexistent", regionid="xxx", fieldid="yyy",
            twid="TW_999", lbid="LB_999", timestamp="2025_01_01T00:00:00Z"
        )
        
        status = cloud_service.get_raw_bags_info(nonexistent_coord)
        
        assert isinstance(status, CloudRawStatus)
        assert status.exists is False
        assert status.bag_count == 0
        assert status.bag_names == []
        assert status.total_size == 0
    
    def test_get_ml_samples_info_exists(self, cloud_service, sample_coordinate):
        """Test getting ML sample info for existing coordinate."""
        cloud_service.get_full_inventory(force_refresh=True)
        
        status = cloud_service.get_ml_samples_info(sample_coordinate)
        
        assert isinstance(status, CloudMLStatus)
        assert status.exists is True
        assert status.total_samples == 2  # 2 label files
        assert '_2025-08-25-09-01-16_0' in status.bag_samples
        
        bag_data = status.bag_samples['_2025-08-25-09-01-16_0']
        assert bag_data['frame_count'] == 2
        assert bag_data['label_count'] == 2
    
    def test_get_ml_samples_info_not_exists(self, cloud_service):
        """Test getting ML sample info for non-existing coordinate."""
        cloud_service.get_full_inventory(force_refresh=True)
        
        nonexistent_coord = RunCoordinate(
            cid="nonexistent", regionid="xxx", fieldid="yyy",
            twid="TW_999", lbid="LB_999", timestamp="2025_01_01T00:00:00Z"
        )
        
        status = cloud_service.get_ml_samples_info(nonexistent_coord)
        
        assert isinstance(status, CloudMLStatus)
        assert status.exists is False
        assert status.total_samples == 0
        assert status.bag_samples == {}


class TestTimelineQueries:
    """Test temporal timeline data extraction."""
    
    def test_get_temporal_timeline_with_filters(self, cloud_service):
        """Test timeline extraction with specific filters."""
        cloud_service.get_full_inventory(force_refresh=True)
        
        filters = {
            'cid': 'client_001',
            'regionid': 'region_xxx',
            'fieldid': 'field_yyy',
            'twid': 'TW_001',
            'lbid': 'LB_0003'
        }
        
        timeline = cloud_service.get_temporal_timeline(filters)
        
        assert isinstance(timeline, TimelineData)
        assert len(timeline.timestamps) == 2  # Two different timestamps in mock data
        assert '2025_08_25T09:01:16Z' in timeline.timestamps
        assert '2025_08_26T10:15:30Z' in timeline.timestamps
        
        # Check raw counts
        assert timeline.raw_counts['2025_08_25T09:01:16Z'] == 2  # 2 bags
        assert timeline.raw_counts['2025_08_26T10:15:30Z'] == 1   # 1 bag
        
        # Check ML counts
        assert timeline.ml_counts['2025_08_25T09:01:16Z'] == 2    # 2 samples
        assert timeline.ml_counts['2025_08_26T10:15:30Z'] == 0    # No ML data for this timestamp
    
    def test_get_temporal_timeline_partial_filters(self, cloud_service):
        """Test timeline extraction with partial filter path."""
        cloud_service.get_full_inventory(force_refresh=True)
        
        filters = {
            'cid': 'client_001',
            'regionid': 'region_xxx'
        }
        
        timeline = cloud_service.get_temporal_timeline(filters)
        
        assert isinstance(timeline, TimelineData)
        assert len(timeline.timestamps) >= 2
    
    def test_get_temporal_timeline_no_data(self, cloud_service):
        """Test timeline extraction with filters that match no data."""
        cloud_service.get_full_inventory(force_refresh=True)
        
        filters = {
            'cid': 'nonexistent_client'
        }
        
        timeline = cloud_service.get_temporal_timeline(filters)
        
        assert isinstance(timeline, TimelineData)
        assert timeline.timestamps == []
        assert timeline.raw_counts == {}
        assert timeline.ml_counts == {}


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_gcs_bucket_not_found(self, bucket_names, temp_cache_file):
        """Test handling of GCS bucket not found error."""
        mock_client = Mock()
        mock_client.bucket.side_effect = Exception("Bucket not found")
        
        with patch('src.services.cloud_inventory_service.storage.Client', return_value=mock_client):
            service = CloudInventoryService(bucket_names, temp_cache_file)
            
            # Should not crash, should return empty inventory
            inventory = service.get_full_inventory(force_refresh=True)
            assert isinstance(inventory, dict)
    
    def test_cache_file_corruption(self, bucket_names, temp_cache_file):
        """Test handling of corrupted cache file."""
        # Write invalid JSON to cache file
        with open(temp_cache_file, 'w') as f:
            f.write("invalid json content")
        
        with patch('src.services.cloud_inventory_service.storage.Client'):
            service = CloudInventoryService(bucket_names, temp_cache_file)
            
            # Should handle corrupted cache gracefully
            assert service._inventory_cache is None
    
    def test_coordinate_query_with_no_cache(self, cloud_service, sample_coordinate):
        """Test coordinate queries when cache is empty."""
        # Don't refresh cache, so it stays empty
        status = cloud_service.get_raw_bags_info(sample_coordinate)
        
        assert isinstance(status, CloudRawStatus)
        assert status.exists is False


class TestRealDataStructures:
    """Test with realistic cloud data structures."""
    
    def test_complex_ml_structure(self, bucket_names, temp_cache_file):
        """Test with complex ML data structure."""
        mock_client = Mock()
        
        # Create complex ML blob structure
        ml_blobs = []
        for bag_idx in range(3):
            bag_name = f"_2025-08-25-09-01-{16+bag_idx}_0"
            for frame_idx in range(5):
                # Frame files - PROPER Mock setup
                frame_blob = Mock()
                frame_blob.name = f"raw/client_001/region_xxx/field_yyy/TW_001/LB_0003/2025_08_25T09:01:16Z/rosbag/{bag_name}/frames/{frame_idx:06d}.jpg"
                frame_blob.size = 50
                ml_blobs.append(frame_blob)
                
                # Label files (not all frames have labels)
                if frame_idx < 3:
                    label_blob = Mock()
                    label_blob.name = f"raw/client_001/region_xxx/field_yyy/TW_001/LB_0003/2025_08_25T09:01:16Z/rosbag/{bag_name}/labels/{frame_idx:06d}.txt"
                    label_blob.size = 10
                    ml_blobs.append(label_blob)
        
        ml_bucket = Mock()
        ml_bucket.list_blobs.return_value = ml_blobs
        mock_client.bucket.return_value = ml_bucket
        
        with patch('src.services.cloud_inventory_service.storage.Client', return_value=mock_client):
            service = CloudInventoryService(bucket_names, temp_cache_file)
            service.get_full_inventory(force_refresh=True)
            
            coord = RunCoordinate(
                cid="client_001", regionid="region_xxx", fieldid="field_yyy",
                twid="TW_001", lbid="LB_0003", timestamp="2025_08_25T09:01:16Z"
            )
            
            ml_status = service.get_ml_samples_info(coord)
            
            assert ml_status.exists is True
            assert ml_status.total_samples == 9  # 3 bags * 3 labels each
            assert len(ml_status.bag_samples) == 3
            
            # Check individual bag data
            for bag_name, bag_data in ml_status.bag_samples.items():
                assert bag_data['frame_count'] == 5
                assert bag_data['label_count'] == 3

