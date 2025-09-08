# src/dashboard/pages/temporal_coverage.py
"""
Temporal coverage page - UI layer only, handles user interactions and display
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from typing import Dict, List

from dashboard.components.filters import HierarchicalFilters
from services.data_service import DataService

def render(gcs_service):
    """
    Render the temporal coverage page
    
    Args:
        gcs_service: GCSService instance
    """
    st.header("Temporal Data Coverage")
    st.markdown("*Cloud data overview and coverage analysis*")
    
    # Ensure GCS data is available
    gcs_data = gcs_service.get_cached_data()
    if not gcs_data:
        st.error("GCS data not available. Please refresh the page.")
        if st.button("🔄 Refresh Data"):
            try:
                gcs_service.discover_and_cache(force_refresh=True)
                st.rerun()
            except Exception as e:
                st.error(f"Failed to refresh data: {e}")
        return
    
    # Initialize data service with discovered data
    try:
        data_service = DataService(gcs_data)
    except Exception as e:
        st.error(f"Failed to initialize data service: {e}")
        return
    
    # Block 1: Filters and Configuration
    with st.container():
        st.subheader("Data Selection & Configuration")
        
        # Create two columns: filters and settings
        filter_col, settings_col = st.columns([3, 1])
        
        with filter_col:
            # Render hierarchical filters
            filters = HierarchicalFilters.render(data_service)
        
        with settings_col:
            st.subheader("Settings")
            # Expected samples per bag configuration
            expected_samples_per_bag = st.number_input(
                "Expected Samples/Bag",
                min_value=1,
                max_value=200,
                value=17,
                step=5,
                help="Expected number of ML samples per raw bag. Adjust to fine-tune gap detection."
            )
        
        # Check if all required filters are selected
        required_filters = ['client', 'region', 'field', 'tw', 'lb']
        selected_filters = {k: v for k, v in filters.items() if v is not None}
        missing_filters = [f for f in required_filters if f not in selected_filters]
        
        if missing_filters:
            st.info(f"Please select: {', '.join(missing_filters)}")
            return
    
    st.divider()
    
    # Block 2: Get data and handle errors
    try:
        temporal_data = data_service.get_temporal_data(selected_filters, expected_samples_per_bag)
        coverage_stats = data_service.get_coverage_statistics(selected_filters, expected_samples_per_bag)
    except ValueError as e:
        st.error(f"Data error: {e}")
        return
    except Exception as e:
        st.error(f"Unexpected error: {e}")
        return
    
    # Block 3: Render plots
    with st.container():
        st.subheader("Temporal Coverage Analysis")
        _render_temporal_plots(temporal_data)
    
    st.divider()
    
    # Block 4: Summary statistics and under-labeled detection
    with st.container():
        st.subheader("Coverage Analysis")
        _render_summary_metrics(temporal_data, coverage_stats, selected_filters)

def _render_temporal_plots(data: Dict):
    """Render the temporal coverage plots"""
    
    if not data['timestamps']:
        st.warning("No data available for the selected filters")
        return
    
    # Format timestamps for display
    display_timestamps = [_format_timestamp(ts) for ts in data['timestamps']]
    
    # Create 3-subplot layout
    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=[
            "Raw Data Coverage (Bags per Timestamp)",
            "ML Data Coverage (Samples per Timestamp)",
            "Annotation Gaps (% Unannotated per Timestamp)"
        ],
        shared_xaxes=True,
        vertical_spacing=0.08
    )
    
    # Plot 1: Raw bags
    fig.add_trace(go.Scatter(
        x=display_timestamps, 
        y=data['raw_bags'],
        mode='markers+lines',
        name='Raw Bags',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8)
    ), row=1, col=1)
    
    # Plot 2: ML samples
    fig.add_trace(go.Scatter(
        x=display_timestamps,
        y=data['ml_samples'],
        mode='markers+lines',
        name='ML Samples', 
        line=dict(color='#ff7f0e', width=3),
        marker=dict(size=8)
    ), row=2, col=1)
    
    # Plot 3: Gap analysis (using calculated gaps)
    gap_percentages = data['gap_percentages']
    
    fig.add_trace(go.Scatter(
        x=display_timestamps,
        y=gap_percentages,
        mode='markers+lines',
        name='Coverage Gap %',
        line=dict(color='#d62728', width=3),
        marker=dict(size=8),
        fill='tonexty' if len(gap_percentages) > 0 else None,
        fillcolor='rgba(214, 39, 40, 0.2)'
    ), row=3, col=1)
    
    # Update layout
    fig.update_layout(
        height=700,
        showlegend=False,
        title_text="Data Coverage Over Time"
    )
    
    # Update y-axis labels
    fig.update_yaxes(title_text="Bag Count", row=1, col=1)
    fig.update_yaxes(title_text="Sample Count", row=2, col=1)
    fig.update_yaxes(title_text="Gap %", row=3, col=1)
    fig.update_xaxes(title_text="Timestamps", row=3, col=1)
    
    st.plotly_chart(fig, use_container_width=True)

def _render_summary_metrics(data: Dict, stats: Dict, filters: Dict):
    """Render summary statistics"""
    
    # Display metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Runs", f"{stats['total_timestamps']:,}")
    
    with col2:
        st.metric("Total Bags", f"{stats['total_raw_bags']:,}")
    
    with col3:
        st.metric("Total Samples", f"{stats['total_ml_samples']:,}")
    
    with col4:
        st.metric(
            "Overall Coverage", 
            f"{stats['overall_coverage_pct']:.1f}%",
            delta=f"{stats['overall_coverage_pct'] - 100:.1f}%" if stats['overall_coverage_pct'] < 100 else None
        )
    
    # Second row with more detailed metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        expected_samples = stats['expected_total_samples']
        st.metric("Expected Samples", f"{expected_samples:,}")
    
    with col2:
        avg_gap = stats['average_gap_pct']
        st.metric("Avg Gap", f"{avg_gap:.1f}%")
    
    with col3:
        samples_per_bag = data.get('expected_samples_per_bag', 85)
        st.metric("Samples/Bag", f"{samples_per_bag}")
    
    with col4:
        under_labeled_count = stats['under_labeled_count']
        st.metric("Under-labeled Runs", f"{under_labeled_count}")
    
    # Show current selection context
    st.caption(f"Showing data for: {filters['client']} → {filters['region']} → {filters['field']} → {filters['tw']} → {filters['lb']}")

def _format_timestamp(timestamp_str: str) -> str:
    """Format timestamp for display on plots"""
    try:
        normalized = timestamp_str.replace('_', '-')
        dt = datetime.fromisoformat(normalized.replace('Z', '+00:00'))
        return dt.strftime('%m-%d %H:%M')
    except Exception:
        return timestamp_str