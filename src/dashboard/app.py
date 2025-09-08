# src/dashboard/app.py
"""
Main Streamlit dashboard application
"""

import logging
import streamlit as st

# Local imports
from config.dashboard_config import DEFAULT_CONFIG
from services.gcs_service import GCSService
from dashboard.utils.session_state import initialize_session_state, get_service, set_service
from dashboard.pages import temporal_coverage, per_run_analysis, download_manager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataOverviewDashboard:
    """Main dashboard application class"""
    
    def __init__(self, config=None):
        """Initialize dashboard with configuration"""
        self.config = config or DEFAULT_CONFIG
        
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
        self._initialize_services()
        
        # Render dashboard
        self._render_dashboard()
    
    def _initialize_services(self):
        """Initialize services once per session"""
        if not st.session_state.get('services_initialized', False):
            try:
                # Initialize GCS service
                gcs_service = GCSService(
                    bucket_names=self.config.bucket_names,
                    cache_file="cache/gcs_data.json"
                )
                set_service('gcs_service', gcs_service)
                
                st.session_state.services_initialized = True
                logger.info("Services initialized successfully")
                
            except Exception as e:
                st.error(f"Failed to initialize services: {e}")
                logger.error(f"Service initialization failed: {e}")
                st.stop()
        
        # Discover cloud data if not done yet
        gcs_service = get_service('gcs_service')
        if gcs_service and not st.session_state.get('data_discovered', False):
            try:
                # Discover data with progress feedback
                def progress_callback(message):
                    # Update a placeholder or just log for now
                    logger.info(f"Discovery progress: {message}")
                
                # Discover data (uses cache if available)
                gcs_service.discover_and_cache(force_refresh=False, progress_callback=progress_callback)
                st.session_state.data_discovered = True
                logger.info("GCS data discovery completed")
            except Exception as e:
                st.error(f"Failed to discover cloud data: {e}")
                logger.error(f"GCS data discovery failed: {e}")
                st.stop()
    
    def _render_dashboard(self):
        """Render the main dashboard interface"""
        # Main title
        st.title("📊 Data Overview Dashboard")
        st.markdown("*Cloud-first data management and analytics interface*")
        
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
            logger.error(f"Page rendering error: {e}")
    
    def _render_sidebar(self):
        """Render sidebar navigation"""
        st.sidebar.title("Navigation")
        
        # Page selection
        selected_page = st.sidebar.selectbox(
            "Select Page",
            list(self.pages.keys()),
            index=list(self.pages.keys()).index(
                st.session_state.get('current_page', self.config.default_page)
            )
        )
        
        # Update current page if changed
        if selected_page != st.session_state.get('current_page'):
            st.session_state.current_page = selected_page
            st.rerun()
        
        st.sidebar.divider()
        
        # Configuration info
        st.sidebar.subheader("Configuration")
        st.sidebar.text("Raw Bucket:")
        st.sidebar.caption(f"{self.config.bucket_names['raw']}")
        st.sidebar.text("ML Bucket:")
        st.sidebar.caption(f"{self.config.bucket_names['ml']}")
        
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
                    st.sidebar.success(f"Data cached ({age_hours}h {age_minutes % 60}m ago)")
                else:
                    st.sidebar.success(f"Data cached ({age_minutes}m ago)")
                
                # Cache timestamp
                cache_time = cache_info.get('timestamp')
                if cache_time:
                    st.sidebar.caption(f"Last updated: {cache_time.strftime('%Y-%m-%d %H:%M')}")
            else:
                st.sidebar.warning("No cached data")
            
            # Refresh button (centered)
            col1, col2, col3 = st.sidebar.columns([1, 2, 1])
            with col2:
                if st.button("🔄 Refresh Data", type="primary", use_container_width=True):
                    with st.spinner("Refreshing cloud data..."):
                        gcs_service.discover_and_cache(force_refresh=True)
                        st.session_state.data_discovered = True
                    st.sidebar.success("Data refreshed!")
                    st.rerun()
        else:
            st.sidebar.error("GCS service unavailable")

def main():
    """Entry point for the dashboard application"""
    dashboard = DataOverviewDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()