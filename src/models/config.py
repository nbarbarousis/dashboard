from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

@dataclass
class DashboardConfig:
    """Main dashboard configuration container."""
    # Path configurations
    raw_data_root: Path
    processed_data_root: Path
    ml_data_root: Path
    cache_root: Path
    
    # Cloud configurations
    raw_bucket_name: str
    ml_bucket_name: str
    
    # Docker configurations
    extraction_docker_image: str
    
    # Processing configurations
    expected_samples_per_bag: int = 17
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'DashboardConfig':
        """Create config from dictionary."""
        return cls(
            raw_data_root=Path(config_dict['raw_data_root']),
            processed_data_root=Path(config_dict['processed_data_root']),
            ml_data_root=Path(config_dict['ml_data_root']),
            cache_root=Path(config_dict['cache_root']),
            raw_bucket_name=config_dict['raw_bucket_name'],
            ml_bucket_name=config_dict['ml_bucket_name'],
            extraction_docker_image=config_dict['extraction_docker_image'],
            expected_samples_per_bag=config_dict.get('expected_samples_per_bag', 17),
            cache_refresh_days=config_dict.get('cache_refresh_days', 7)
        )


@dataclass
class CacheInfo:
    """Cache metadata information."""
    last_updated: datetime
    size_bytes: int
    entry_count: int
    cache_file: str