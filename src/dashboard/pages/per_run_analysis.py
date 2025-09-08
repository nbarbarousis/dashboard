# src/dashboard/pages/per_run_analysis.py
"""
Per-run analysis page - detailed analytics for individual runs
"""

import streamlit as st
from dashboard.components.filters import HierarchicalFilters
from services.data_service import DataService

def render(gcs_service):
    """
    Render the per-run analysis page
    
    Args:
        gcs_service: GCSService instance
    """
    st.header("Per-Run Analysis")
    st.markdown("*Detailed analytics for individual runs*")
    
    # Get GCS data and initialize data service
    gcs_data = gcs_service.get_cached_data()
    if not gcs_data:
        st.error("GCS data not available")
        return
    
    data_service = DataService(gcs_data)
    
    # Render hierarchical filters
    filters = HierarchicalFilters.render(data_service)
    
    # Get available timestamps for selected hierarchy
    available_timestamps = HierarchicalFilters.get_available_timestamps(filters)
    
    if available_timestamps:
        st.subheader("Available Runs")
        st.write(f"Found {len(available_timestamps)} runs for current selection:")
        
        # Show first few timestamps as examples
        for i, timestamp in enumerate(available_timestamps[:5]):
            st.text(f"• {timestamp}")
        
        if len(available_timestamps) > 5:
            st.text(f"... and {len(available_timestamps) - 5} more")
        
        # TODO: Implement run selection and analytics integration
        st.info("Analytics integration coming soon...")
        
    else:
        st.info("Select filters to view available runs for analysis")