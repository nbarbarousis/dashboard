# src/dashboard/pages/per_run_analysis.py
"""
Per-run analysis page - detailed analytics for individual runs
Enhanced version with full rosbag analysis integration
"""

import streamlit as st
import plotly.graph_objects as go
import json
from datetime import datetime
from typing import Dict, Optional

from dashboard.components.filters import HierarchicalFilters
from dashboard.utils.session_state import get_service
from services.data_service import DataService
from models.data_models import (
    RunCoordinate, DataStatus, ProcessingStatus
)


def render(gcs_service):
    """
    Render the per-run analysis page with full analytics integration
    
    Args:
        gcs_service: GCSService instance
    """
    st.header("Per-Run Analysis")
    st.markdown("*Detailed performance metrics and model behavior for individual runs*")
    
    # Get services
    analytics_service = get_service('analytics_service')
    rosbag_service = get_service('rosbag_service')
    download_service = get_service('download_service')
    extraction_service = get_service('extraction_service')
    
    if not all([analytics_service, rosbag_service, download_service, extraction_service]):
        st.error("Required services not initialized. Please restart the application.")
        return
    
    # Get GCS data for filtering
    gcs_data = gcs_service.get_cached_data()
    if not gcs_data:
        st.error("GCS data not available")
        return

    # Get global filters from sidebar
    filters = st.session_state.get('global_filters', {})
    
    # Check if all required filters are selected
    if not all([filters.get('client'), filters.get('region'), 
               filters.get('field'), filters.get('tw'), filters.get('lb')]):
        st.info("Please select all filter levels in the sidebar")
        return
    
    # Page-specific options on top-left
    col1, col2, col3 = st.columns([2, 2, 2])
    
    # Get available timestamps from filters
    available_timestamps = HierarchicalFilters.get_available_timestamps(filters)
    
    if not available_timestamps:
        st.info("No runs available for the selected filters")
        return
    
    with col1:
        # Timestamp selection
        selected_timestamp = st.selectbox(
            "Select Timestamp",
            available_timestamps,
            format_func=lambda x: _format_timestamp_display(x),
            help="Select the specific run to analyze"
        )
    
    with col2:
        st.metric("Available Runs", len(available_timestamps))
        st.caption(f"For {filters['lb']} in {filters['field']}")
    
    if not selected_timestamp:
        return
    
    # Create coordinate for selected run
    coord = RunCoordinate(
        cid=filters['client'],
        regionid=filters['region'],
        fieldid=filters['field'],
        twid=filters['tw'],
        lbid=filters['lb'],
        timestamp=selected_timestamp
    )
    
    st.divider()
    
    # ========== SECTION 2: DATA PIPELINE STATUS ==========
    with st.container():
        st.subheader("📊 Data Pipeline Status")
        
        # Create status columns
        status_cols = st.columns(4)
        
        # 1. Cloud Discovery Status
        with status_cols[0]:
            st.markdown("**☁️ Cloud Data**")
            # Check if this run exists in cloud
            cloud_exists = _check_cloud_exists(gcs_data, coord)
            if cloud_exists:
                st.success("Available")
                st.caption(f"{cloud_exists['bag_count']} bags")
            else:
                st.error("Not Found")
                st.stop()
        
        # 2. Download Status
        with status_cols[1]:
            st.markdown("**📥 Local Storage**")
            download_status = download_service.check_download_status(coord)
            
            if download_status['downloaded']:
                st.success("Downloaded")
                st.caption(f"{download_status['bag_count']} bags, {_format_size(download_status['total_size'])}")
            else:
                st.warning("Not Downloaded")
                
                if st.button("Download", key="download_btn"):
                    with st.spinner("Creating download job..."):
                        job = download_service.create_download_job(coord)
                        
                    if job.total_files > 0:
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        with st.spinner(f"Downloading {job.total_files} files..."):
                            # Execute download
                            job = download_service.execute_download(job)
                            
                            # Update progress (simplified - real implementation would poll)
                            progress_bar.progress(1.0)
                            status_text.text(f"Downloaded {job.files_downloaded} files")
                        
                        if job.status == ProcessingStatus.COMPLETE:
                            st.success("Download complete!")
                            st.rerun()
                        else:
                            st.error(f"Download failed: {job.error_message}")
                    else:
                        st.info("No files to download")
        
        # 3. Extraction Status
        with status_cols[2]:
            st.markdown("**🔧 Extraction**")
            extraction_status = rosbag_service.check_extraction_status(coord)
            
            if extraction_status['status'] == DataStatus.EXTRACTED:
                st.success("Extracted")
                files = extraction_status.get('files', {})
                extracted_count = sum(1 for v in files.values() if v)
                st.caption(f"{extracted_count} data types")
            else:
                st.warning("Not Extracted")
                
                if download_status['downloaded'] and st.button("Extract", key="extract_btn"):
                    with st.spinner("Creating extraction job..."):
                        source_path = rosbag_service._get_raw_path(coord)
                        output_path = rosbag_service._get_processed_path(coord)
                        job = extraction_service.create_extraction_job(coord, source_path, output_path)
                    
                    if job.status != ProcessingStatus.COMPLETE:
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        with st.spinner(f"Extracting from {job.total_bags} bags..."):
                            # Execute extraction
                            job = extraction_service.execute_extraction(job)
                            
                            # Update progress
                            progress_bar.progress(1.0)
                            status_text.text(f"Extracted {job.frames_extracted} frames")
                        
                        if job.status == ProcessingStatus.COMPLETE:
                            st.success("Extraction complete!")
                            st.rerun()
                        elif job.status == ProcessingStatus.FAILED:
                            st.error(f"Extraction failed: {job.error_message}")
                            if job.docker_output:
                                with st.expander("Docker Output"):
                                    st.code(job.docker_output)
        
        # 4. Analysis Status
        with status_cols[3]:
            st.markdown("**📈 Analysis**")
            # Quick check if analysis is cached
            analysis_cached = _check_analysis_cached(analytics_service, coord)
            
            if analysis_cached:
                st.success("Cached")
                st.caption("Ready to view")
            elif extraction_status['status'] == DataStatus.EXTRACTED:
                st.info("Ready")
                if st.button("Analyze", key="analyze_btn"):
                    st.session_state.run_analysis = True
            else:
                st.warning("Not Ready")
                st.caption("Extract first")
    
    st.divider()
    
    # ========== SECTION 3: RUN ANALYSIS ==========
    
    # Only proceed if data is extracted
    if extraction_status['status'] != DataStatus.EXTRACTED:
        st.info("💡 Extract data to enable analysis")
        return

    # Run analysis if requested
    if st.session_state.get('run_analysis', False) or analysis_cached:
        with st.spinner("Running analysis..." if not analysis_cached else "Loading analysis..."):
            analysis = analytics_service.analyze_run(coord)

        if analysis.status != DataStatus.ANALYZED:
            st.error(f"Analysis failed: {analysis.error_message}")
            return   
        # ========== SECTION 4: ANALYSIS PLOTS ==========
        
        if analysis.plots:
            st.markdown("### 📈 Detailed Analysis")
            
            # Create tabs for different plot categories
            plot_tabs = st.tabs([
                "🎬 FPS Analysis",
                "🎯 Detection Stats",
                "⏱️ Latency Analysis",
                "🔄 Track Lifecycles"
            ])
            
            with plot_tabs[0]:
                if analysis.plots.fps_figure:
                    st.markdown("""
                    **FPS Analysis** shows frame processing performance:
                    - **Top row**: Temporal view of instantaneous and rolling FPS
                    - **Middle row**: Distribution of instantaneous FPS values
                    - **Bottom row**: Distribution of smoothed (25-frame) FPS values
                    """)
                    fig = go.Figure(analysis.plots.fps_figure)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("FPS analysis not available")
            
            with plot_tabs[1]:
                if analysis.plots.stats_figure:
                    st.markdown("""
                    **Detection Statistics** shows model behavior:
                    - **Left**: Detection/track counts over time
                    - **Middle**: Distribution of counts per frame
                    - **Right**: Confidence score distributions
                    """)
                    fig = go.Figure(analysis.plots.stats_figure)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Detection statistics not available")
            
            with plot_tabs[2]:
                if analysis.plots.latency_figure:
                    st.markdown("""
                    **Latency Analysis** shows processing delays:
                    - **Left**: Latency over time (detection pipeline delay)
                    - **Right**: Distribution of latency values
                    """)
                    fig = go.Figure(analysis.plots.latency_figure)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Latency analysis not available")
            
            with plot_tabs[3]:
                if analysis.plots.lifecycle_figure:
                    st.markdown("""
                    **Track Lifecycle Analysis** shows tracking quality:
                    - **Track Span**: Total duration of tracks (including gaps)
                    - **Active Frames**: Actual detection count per track
                    - **Density**: Ratio of active frames to span (1.0 = perfect)
                    - **Confidence vs Span**: Relationship between detection confidence and track persistence
                    """)
                    fig = go.Figure(analysis.plots.lifecycle_figure)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Track lifecycle analysis not available")



# ============================================================================
# Helper Functions
# ============================================================================

def _format_timestamp_display(timestamp: str) -> str:
    """Format timestamp for display"""
    try:
        # Handle both formats: 2024-01-15T10:30:00Z and 2024_01_15T10:30:00Z
        normalized = timestamp.replace('_', '-')
        dt = datetime.fromisoformat(normalized.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return timestamp


def _format_size(size_bytes: int) -> str:
    """Format bytes as human readable string"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def _check_cloud_exists(gcs_data: Dict, coord: RunCoordinate) -> Optional[Dict]:
    """Check if run exists in cloud data"""
    try:
        raw_data = gcs_data.get('raw', {})
        path_data = raw_data
        
        for key in [coord.cid, coord.regionid, coord.fieldid, coord.twid, coord.lbid, coord.timestamp]:
            path_data = path_data.get(key, {})
            if not path_data:
                return None
        
        return path_data
    except:
        return None


def _check_analysis_cached(analytics_service, coord: RunCoordinate) -> bool:
    """Quick check if analysis is cached without loading it"""
    try:
        # Check if cache file exists
        cache_path = analytics_service._get_cache_path(coord)
        return cache_path.exists()
    except:
        return False


def _generate_html_report(analysis) -> str:
    """Generate standalone HTML report with all plots"""
    # This would generate a complete HTML report similar to rosbag-analysis
    # For now, return a simple template
    
    plots_json = {
        'fps': analysis.plots.fps_figure if analysis.plots else {},
        'stats': analysis.plots.stats_figure if analysis.plots else {},
        'latency': analysis.plots.latency_figure if analysis.plots else {},
        'lifecycle': analysis.plots.lifecycle_figure if analysis.plots else {}
    }
    
    html_template = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Run Analysis Report - {analysis.coordinate.timestamp}</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}
            .header {{
                background: white;
                padding: 20px;
                border-radius: 8px;
                margin-bottom: 20px;
            }}
            .plot-container {{
                background: white;
                padding: 20px;
                border-radius: 8px;
                margin-bottom: 20px;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Run Analysis Report</h1>
            <p>Timestamp: {analysis.coordinate.timestamp}</p>
            <p>Location: {analysis.coordinate.cid}/{analysis.coordinate.regionid}/{analysis.coordinate.fieldid}/{analysis.coordinate.tw}</p>
        </div>
        
        <div class="plot-container">
            <h2>FPS Analysis</h2>
            <div id="fps-plot"></div>
        </div>
        
        <div class="plot-container">
            <h2>Detection Statistics</h2>
            <div id="stats-plot"></div>
        </div>
        
        <div class="plot-container">
            <h2>Latency Analysis</h2>
            <div id="latency-plot"></div>
        </div>
        
        <div class="plot-container">
            <h2>Track Lifecycles</h2>
            <div id="lifecycle-plot"></div>
        </div>
        
        <script>
            var fpsData = {json.dumps(plots_json['fps'])};
            var statsData = {json.dumps(plots_json['stats'])};
            var latencyData = {json.dumps(plots_json['latency'])};
            var lifecycleData = {json.dumps(plots_json['lifecycle'])};
            
            if (fpsData.data) Plotly.newPlot('fps-plot', fpsData.data, fpsData.layout);
            if (statsData.data) Plotly.newPlot('stats-plot', statsData.data, statsData.layout);
            if (latencyData.data) Plotly.newPlot('latency-plot', latencyData.data, latencyData.layout);
            if (lifecycleData.data) Plotly.newPlot('lifecycle-plot', lifecycleData.data, lifecycleData.layout);
        </script>
    </body>
    </html>
    """
    
    return html_template