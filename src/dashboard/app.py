# src/dashboard/app.py
"""
Main Streamlit dashboard application - enhanced version with all services
"""

import logging
import streamlit as st
from pathlib import Path
from typing import Dict, Optional

# Local imports
from config.dashboard_config import DEFAULT_CONFIG, DashboardConfig
from models.data_models import ServiceConfig
from services.extraction_service import ExtractionService
from services.gcs_service import GCSService
from services.rosbag_service import RosbagService
from services.analytics_service import AnalyticsService
from services.download_service import DownloadService
from dashboard.utils.session_state import (
    initialize_session_state, get_service, set_service
)
from dashboard.pages import temporal_coverage, per_run_analysis, download_manager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ServiceManager:
    """Manages all dashboard services"""
    
    def __init__(self, config: DashboardConfig):
        """Initialize service manager with configuration"""
        self.config = config
        self.services = {}
        
    def initialize_all_services(self) -> bool:
        """
        Initialize all required services
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get user paths
            user_paths = self.config.get_user_paths()
            
            # Create service configuration
            service_config = ServiceConfig.from_dashboard_config(
                self.config, 
                user_paths
            )
            
            # 1. Initialize GCS service (existing)
            logger.info("Initializing GCS service...")
            gcs_service = GCSService(
                bucket_names=self.config.bucket_names,
                cache_file=self.config.cache_path
            )
            self.services['gcs_service'] = gcs_service
            set_service('gcs_service', gcs_service)

            # Extraction Service
            extraction_service = ExtractionService(
               docker_image="rosbag-extractor"
           )
            set_service('extraction_service', extraction_service)
            
            # 2. Initialize Rosbag service (new)
            logger.info("Initializing Rosbag service...")
            rosbag_service = RosbagService(
                raw_root=service_config.raw_root,
                processed_root=service_config.processed_root,
            )
            self.services['rosbag_service'] = rosbag_service
            set_service('rosbag_service', rosbag_service)
            
            # 3. Initialize Analytics service (new)
            logger.info("Initializing Analytics service...")
            analytics_service = AnalyticsService(
                rosbag_service=rosbag_service,
                processed_root=service_config.processed_root
            )
            self.services['analytics_service'] = analytics_service
            set_service('analytics_service', analytics_service)
            
            # 4. Initialize Download service (new)
            logger.info("Initializing Download service...")
            download_service = DownloadService(
                gcs_service=gcs_service,
                raw_root=service_config.raw_root
            )
            self.services['download_service'] = download_service
            set_service('download_service', download_service)
            
            # 5. Store service config for reference
            set_service('service_config', service_config)
            set_service('user_paths', user_paths)
            
            logger.info("All services initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize services: {e}")
            return False
    
    def get_service(self, name: str):
        """Get a service by name"""
        return self.services.get(name)


class DataOverviewDashboard:
    """Enhanced main dashboard application class"""
    
    def __init__(self, config=None):
        """Initialize dashboard with configuration"""
        self.config = config or DEFAULT_CONFIG
        
        # Validate configuration
        if not self.config.validate():
            raise ValueError("Invalid configuration")
        
        # Initialize service manager
        self.service_manager = ServiceManager(self.config)
        
        # Define available pages
        self.pages = {
            "Temporal Coverage": temporal_coverage,
            "Per-Run Analysis": per_run_analysis,
            "Download Manager": download_manager
        }
    
    def run(self):
        """Main application entry point"""
        # Configure Streamlit page
        st.set_page_config(
            page_title="Data Overview Dashboard",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Initialize session state
        initialize_session_state()
        
        # Initialize services if needed
        if not st.session_state.get('services_initialized', False):
            if self._initialize_services():
                st.session_state.services_initialized = True
            else:
                st.error("Failed to initialize services. Please check configuration.")
                st.stop()
        
        # Discover cloud data if needed
        self._ensure_cloud_data_discovered()
        
        # Render dashboard
        self._render_dashboard()
    
    def _initialize_services(self) -> bool:
        """Initialize all services"""
        return self.service_manager.initialize_all_services()
    
    def _ensure_cloud_data_discovered(self):
        """Ensure cloud data is discovered and cached"""
        gcs_service = get_service('gcs_service')
        
        if gcs_service and not st.session_state.get('data_discovered', False):
            try:                
                # Discover data
                gcs_service.discover_and_cache(
                    force_refresh=self.config.refresh_on_startup,
                    progress_callback=lambda msg: logger.info(f"Discovery: {msg}")
                )
                
                st.session_state.data_discovered = True
                logger.info("GCS data discovery completed")
                
            except Exception as e:
                st.error(f"Failed to discover cloud data: {e}")
                logger.error(f"GCS data discovery failed: {e}")
    
    def _render_dashboard(self):
        """Render the main dashboard interface"""
        # Main title
        st.title("📊 Data Overview Dashboard")
        
        # Page tabs as centered buttons
        pages = list(self.pages.keys())
        cols = st.columns(len(pages))
        for idx, page_name in enumerate(pages):
            if cols[idx].button(page_name, key=f"nav_{page_name.replace(' ', '_')}", use_container_width=True):
                st.session_state.current_page = page_name
                st.rerun()

        st.markdown("")  # spacing


        # Sidebar navigation
        self._render_sidebar()
        
        # Get current page selection
        current_page = st.session_state.get('current_page', self.config.default_page)
        
        # Render selected page
        try:
            page_module = self.pages[current_page]
            gcs_service = get_service('gcs_service')
            
            if gcs_service and gcs_service.get_cached_data():
                page_module.render(gcs_service)
            else:
                st.error("GCS service not available or data not discovered")
                st.info("Try refreshing the page to reinitialize services")
            
        except Exception as e:
            st.error(f"Error rendering page '{current_page}': {e}")
            logger.error(f"Page rendering error: {e}", exc_info=True)
    
    def _render_sidebar(self):
        """Render enhanced sidebar navigation"""
        st.sidebar.title("Navigation")
        
        # Add the 5 hierarchical filters here in 5x1 configuration
        self._render_hierarchical_filters()
        
        st.sidebar.divider()
        
        # Configuration section
        with st.sidebar.expander("Configuration", expanded=False):
            st.text("Raw Bucket:")
            st.caption(f"{self.config.bucket_names['raw']}")
            st.text("ML Bucket:")
            st.caption(f"{self.config.bucket_names['ml']}")
            
            # Path configuration
            st.text("Data Paths:")
            user_paths = get_service('user_paths')
            if user_paths:
                st.caption(f"Raw: {user_paths['raw_root']}")
                st.caption(f"Processed: {user_paths['processed_root']}")
                st.caption(f"Cache: {user_paths['cache_root']}")
        
        st.sidebar.divider()
        
        # Data Status Section
        st.sidebar.subheader("Data Status")
        
        gcs_service = get_service('gcs_service')
        if gcs_service:
            cache_info = gcs_service.get_cache_info()
            
            if cache_info.get('cached', False):
                age_hours = cache_info.get('age_hours', 0)
                age_minutes = cache_info.get('age_minutes', 0)
                
                if age_hours > 0:
                    st.sidebar.success(
                        f"Data cached ({age_hours}h {age_minutes % 60}m ago)"
                    )
                else:
                    st.sidebar.success(f"Data cached ({age_minutes}m ago)")
                
                # Cache timestamp
                cache_time = cache_info.get('timestamp')
                if cache_time:
                    st.sidebar.caption(
                        f"Last updated: {cache_time.strftime('%Y-%m-%d %H:%M')}"
                    )
            else:
                st.sidebar.warning("No cached data")
            
            # Refresh button
            if st.sidebar.button("🔄 Refresh Data", type="primary", use_container_width=True):
                with st.spinner("Refreshing cloud data..."):
                    gcs_service.discover_and_cache(force_refresh=True)
                    st.session_state.data_discovered = True
                st.sidebar.success("Data refreshed!")
                st.rerun()
        else:
            st.sidebar.error("GCS service unavailable")
        
        st.sidebar.divider()
        

    def _render_hierarchical_filters(self):
        """Render the 5 hierarchical filters in sidebar"""
        st.sidebar.subheader("Data Filters")
        
        # Get GCS service and data
        gcs_service = get_service('gcs_service')
        if not gcs_service:
            st.sidebar.error("GCS service not available")
            return
        
        gcs_data = gcs_service.get_cached_data()
        if not gcs_data:
            st.sidebar.warning("No data available")
            return
        
        from services.data_service import DataService
        from dashboard.components.filters import HierarchicalFilters
        
        try:
            data_service = DataService(gcs_data)
            filters = HierarchicalFilters.render_sidebar(data_service)
            
            # Store filters in session state for pages to access
            st.session_state.global_filters = filters
            
        except Exception as e:
            st.sidebar.error(f"Filter error: {e}")

def main():
    """Entry point for the dashboard application"""
    try:
        dashboard = DataOverviewDashboard()
        dashboard.run()
    except Exception as e:
        st.error(f"Dashboard initialization failed: {e}")
        logger.error(f"Dashboard initialization failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()