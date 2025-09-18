"""
Main Streamlit dashboard application - Updated for new architecture
"""
import streamlit as st
import logging
from pathlib import Path
from typing import Dict, Optional

from src.models import DashboardConfig
from src.services import ServiceContainer
from src.dashboard.utils.session_state import initialize_session_state
from src.dashboard.pages import temporal_coverage, operations
from src.dashboard.components.filters import HierarchicalFilters

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataOverviewDashboard:
    """Main dashboard application class using new ServiceContainer architecture"""
    
    def __init__(self, config: DashboardConfig):
        """Initialize dashboard with configuration"""
        self.config = config
        self.services: Optional[ServiceContainer] = None
        
        # Define available pages
        self.pages = {
            "Temporal Coverage": temporal_coverage,
            "Operations": operations
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
            success = self._initialize_services()
            if success:
                st.session_state.services_initialized = True
            else:
                st.error("Failed to initialize services. Please check the logs.")
                return
        else:
            self.services = st.session_state.get('services')
        
        # Render dashboard
        self._render_dashboard()
    
    def _initialize_services(self) -> bool:
        """Initialize all services via ServiceContainer"""
        try:
            with st.spinner("Initializing services..."):
                self.services = ServiceContainer.initialize(self.config)
                self.services.warm_up()
                
                # Store in session state
                st.session_state.services = self.services
                
                st.success("Services initialized successfully!")
                return True
                
        except Exception as e:
            logger.error(f"Service initialization failed: {e}")
            st.error(f"Service initialization failed: {e}")
            return False
    
    def _render_dashboard(self):
        """Render the main dashboard interface"""
        # Main title
        st.title("📊 Data Overview Dashboard")
        
        # Page navigation
        pages = list(self.pages.keys())
        cols = st.columns(len(pages))
        for idx, page_name in enumerate(pages):
            with cols[idx]:
                if st.button(page_name, use_container_width=True):
                    st.session_state.current_page = page_name
        
        st.markdown("")  # spacing
        
        # Sidebar with filters
        self._render_sidebar()
        
        # Get current page selection
        current_page = st.session_state.get('current_page', 'Temporal Coverage')
        
        # Render selected page
        try:
            if current_page in self.pages and self.services:
                self.pages[current_page].render(self.services)
            else:
                st.error(f"Page '{current_page}' not found or services not initialized")
                
        except Exception as e:
            logger.error(f"Error rendering page {current_page}: {e}")
            st.error(f"Error rendering page: {e}")
    
    def _render_sidebar(self):
        """Render sidebar with hierarchical filters using new architecture"""
        st.sidebar.title("Navigation")
        
        # Render hierarchical filters using DataCoordinationService
        self._render_hierarchical_filters()
        
        st.sidebar.divider()
        
        # Data Status Section
        st.sidebar.subheader("Data Status")
        
        if self.services and self.services.cloud_inventory:
            cache_info = self.services.cloud_inventory.get_cache_info()
            
            if cache_info.last_updated:
                from datetime import datetime
                age = datetime.now() - cache_info.last_updated
                hours = int(age.total_seconds() / 3600)
                minutes = int((age.total_seconds() % 3600) / 60)
                
                if hours > 0:
                    st.sidebar.success(f"Data cached ({hours}h {minutes}m ago)")
                else:
                    st.sidebar.success(f"Data cached ({minutes}m ago)")
                
                st.sidebar.caption(
                    f"Last updated: {cache_info.last_updated.strftime('%Y-%m-%d %H:%M')}"
                )
            else:
                st.sidebar.warning("No cached data")
            
            # Refresh button
            if st.sidebar.button("🔄 Refresh Data", type="primary", use_container_width=True):
                with st.spinner("Refreshing cloud data..."):
                    self.services.cloud_inventory.get_full_inventory(force_refresh=True)
                st.sidebar.success("Data refreshed!")
                st.rerun()
        else:
            st.sidebar.error("Services unavailable")
    
    def _render_hierarchical_filters(self):
        """Render the hierarchical filters using DataCoordinationService"""
        st.sidebar.subheader("Data Filters")
        
        if not self.services:
            st.sidebar.error("Services not initialized")
            return
        
        try:
            # Use DataCoordinationService for infrastructure-level filter operations
            filters = HierarchicalFilters.render_sidebar(self.services.data_coordination)
            st.session_state.global_filters = filters
            
        except Exception as e:
            logger.error(f"Filter rendering error: {e}")
            st.sidebar.error(f"Filter error: {e}")

def main():
    """Entry point for the dashboard application"""
    try:
        # Load configuration
        # For now, using a simple config - later can load from file
        config = DashboardConfig(
            raw_data_root=Path("/home/nikbarb/data-annot-pipeline/data/raw"),
            processed_data_root=Path("/home/nikbarb/data-annot-pipeline/data/processed"),
            ml_data_root=Path("/home/nikbarb/data-annot-pipeline/data/ML"),
            cache_root=Path("data/cache"),
            raw_bucket_name="terra-weeder-deployments-data-raw",
            processed_bucket_name="terra-weeder-deployments-data-processed",
            ml_bucket_name="terra-weeder-deployments-data-ml",
            extraction_docker_image="rosbag-extractor",
            expected_samples_per_bag=17
        )
        
        dashboard = DataOverviewDashboard(config)
        dashboard.run()
        
    except Exception as e:
        st.error(f"Dashboard initialization failed: {e}")
        logger.error(f"Dashboard initialization failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()