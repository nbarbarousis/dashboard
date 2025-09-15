"""
Temporal coverage page - Updated to use DataStateService
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from typing import Dict, List

from src.services.service_container import ServiceContainer
from src.models import TemporalData, CoverageStatistics


def render(services: ServiceContainer):
    """
    Render the temporal coverage page using ServiceContainer.
    
    Key changes:
    - Receives ServiceContainer instead of individual services
    - Uses DataStateService for all data operations
    - No more direct GCS or DataService calls
    
    Args:
        services: ServiceContainer instance
    """
    st.header("Temporal Data Coverage")
    st.markdown("*Cloud data overview and coverage analysis*")
    
    # Get global filters from sidebar
    filters = st.session_state.get('global_filters', {})
    
    # Page-specific options
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
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
    required_filters = ['cid', 'regionid', 'fieldid', 'twid', 'lbid']
    missing_filters = [f for f in required_filters if not filters.get(f)]
    
    if missing_filters:
        st.info(f"Please select all filter levels in the sidebar to view temporal coverage")
        return
    
    st.divider()
    
    # Get data using DataStateService
    try:
        # Get temporal data and statistics using the new service
        temporal_data = services.data_state.get_temporal_coverage_data(
            filters, 
            expected_samples_per_bag
        )
        
        coverage_stats = services.data_state.get_coverage_statistics(
            filters, 
            expected_samples_per_bag
        )
        
        # Check if we have data
        if not temporal_data.timestamps:
            st.warning("No data available for the selected filters")
            return
            
    except Exception as e:
        st.error(f"Error retrieving temporal data: {e}")
        return
    
    # Render plots
    with st.container():
        st.subheader("Temporal Coverage Analysis")
        _render_temporal_plots(temporal_data)
    
    st.divider()
    
    # Summary statistics
    with st.container():
        st.subheader("Coverage Analysis")
        _render_summary_metrics(temporal_data, coverage_stats, filters)


def _render_temporal_plots(data: TemporalData):
    """
    Render the temporal coverage plots.
    
    Uses the TemporalData model returned by DataStateService.
    """
    if not data.timestamps:
        st.warning("No data available for plotting")
        return
    
    # Format timestamps for display
    display_timestamps = [_format_timestamp(ts) for ts in data.timestamps]
    
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
        y=data.raw_bags,
        mode='markers+lines',
        name='Raw Bags',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8)
    ), row=1, col=1)
    
    # Plot 2: ML samples
    fig.add_trace(go.Scatter(
        x=display_timestamps,
        y=data.ml_samples,
        mode='markers+lines',
        name='ML Samples', 
        line=dict(color='#ff7f0e', width=3),
        marker=dict(size=8)
    ), row=2, col=1)
    
    # Plot 3: Gap analysis
    fig.add_trace(go.Scatter(
        x=display_timestamps,
        y=data.gap_percentages,
        mode='markers+lines',
        name='Coverage Gap %',
        line=dict(color='#d62728', width=3),
        marker=dict(size=8),
        fill='tonexty' if len(data.gap_percentages) > 0 else None,
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


def _render_summary_metrics(data: TemporalData, stats: CoverageStatistics, filters: Dict):
    """
    Render summary statistics using the CoverageStatistics model.
    """
    # Display metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Runs", f"{stats.total_timestamps:,}")
    
    with col2:
        st.metric("Total Bags", f"{stats.total_raw_bags:,}")
    
    with col3:
        st.metric("Total Samples", f"{stats.total_ml_samples:,}")
    
    with col4:
        st.metric(
            "Overall Coverage", 
            f"{stats.overall_coverage_pct:.1f}%",
            delta=f"{stats.overall_coverage_pct - 100:.1f}%" if stats.overall_coverage_pct < 100 else None
        )
    
    # Second row with more detailed metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        expected_total = stats.total_raw_bags * data.expected_samples_per_bag
        st.metric("Expected Samples", f"{expected_total:,}")
    
    with col2:
        st.metric("Avg Gap", f"{stats.average_gap_pct:.1f}%")
    
    with col3:
        st.metric("Samples/Bag", f"{data.expected_samples_per_bag}")
    
    with col4:
        st.metric("Under-labeled Runs", f"{stats.under_labeled_count}")
    
    # Show current selection context
    st.caption(
        f"Showing data for: {filters['cid']} → {filters['regionid']} → "
        f"{filters['fieldid']} → {filters['twid']} → {filters['lbid']}"
    )
    
    # If there are under-labeled timestamps, show them
    if stats.under_labeled_timestamps:
        with st.expander(f"Under-labeled Timestamps ({stats.under_labeled_count})"):
            for ts, gap, raw, ml in stats.under_labeled_timestamps[:10]:  # Show first 10
                st.text(f"{_format_timestamp(ts)}: {gap:.1f}% gap ({raw} bags, {ml} samples)")
            if stats.under_labeled_count > 10:
                st.text(f"... and {stats.under_labeled_count - 10} more")


def _format_timestamp(timestamp_str: str) -> str:
    """Format timestamp for display on plots"""
    try:
        normalized = timestamp_str.replace('_', '-')
        dt = datetime.fromisoformat(normalized.replace('Z', '+00:00'))
        return dt.strftime('%m-%d %H:%M')
    except Exception:
        return timestamp_str