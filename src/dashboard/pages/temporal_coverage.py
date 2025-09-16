"""
Temporal coverage page - Updated to use DataStateService with aggregated view
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from typing import Dict, List

from src.services.service_container import ServiceContainer
from src.models import TemporalData, CoverageStatistics, AggregatedTemporalData, LaserBoxStats


def render(services: ServiceContainer):
    """
    Render the temporal coverage page using ServiceContainer.
    
    Handles both individual LB view and aggregated "All" view.
    """
    # Get global filters from sidebar
    filters = st.session_state.get('global_filters', {})
    
    # Check if all required filters are selected (except lbid can be None)
    required_filters = ['cid', 'regionid', 'fieldid', 'twid']
    missing_filters = [f for f in required_filters if not filters.get(f)]
    
    if missing_filters:
        st.info(f"Please select all filter levels (except laser box) in the sidebar to view temporal coverage")
        return
    
    st.divider()
    
    # Check if this is aggregated view (LB = "All" or None)
    is_aggregated = filters.get('lbid') is None
    
    if is_aggregated:
        _render_aggregated_view(services, filters)
    else:
        _render_individual_lb_view(services, filters)


def _render_individual_lb_view(services: ServiceContainer, filters: Dict):
    """Render the individual laser box view (existing functionality)."""
    try:
        # Get temporal data and statistics using the existing service
        temporal_data = services.data_state.get_temporal_coverage_data(filters)
        coverage_stats = services.data_state.get_coverage_statistics(filters)
        
        # Check if we have data
        if not temporal_data.timestamps:
            st.warning("No data available for the selected filters")
            return
            
    except Exception as e:
        st.error(f"Error retrieving temporal data: {e}")
        return
    
    # Render plots
    with st.container():
        st.subheader(f"Temporal Coverage Analysis - {filters['lbid']}")
        _render_temporal_plots(temporal_data)
    
    st.divider()
    
    # Summary statistics
    with st.container():
        st.subheader("Coverage Analysis")
        _render_summary_metrics(temporal_data, coverage_stats, filters)


def _render_aggregated_view(services: ServiceContainer, filters: Dict):
    """Render the aggregated view when LB = 'All'."""
    try:
        # Get aggregated data
        agg_data = services.data_state.get_temporal_coverage_aggregated(filters)
        lb_stats = services.data_state.get_laser_box_statistics(filters)
        
        if not agg_data.dates:
            st.warning("No data available for the selected filters")
            return
            
    except Exception as e:
        st.error(f"Error retrieving aggregated temporal data: {e}")
        return
    
    # Top Section: Daily Aggregated Plots
    with st.container():
        st.subheader("Daily Aggregated Coverage - All Laser Boxes")
        _render_aggregated_plots(agg_data)
    
    st.divider()
    
    # Middle Section: Summary Statistics
    with st.container():
        st.subheader("Aggregated Coverage Statistics")
        _render_aggregated_summary(agg_data, filters)
    
    st.divider()
    
    # Bottom Section: Laser Box Breakdown
    with st.container():
        st.subheader("Laser Box Breakdown")
        _render_laser_box_breakdown(lb_stats, agg_data)


def _render_aggregated_plots(data: AggregatedTemporalData):
    """Render aggregated temporal plots for all laser boxes."""
    if not data.dates:
        st.warning("No data available for plotting")
        return
    
    # Create 3-subplot layout
    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=[
            "Raw Data Coverage (Total Bags per Active Day)",
            "ML Data Coverage (Total Samples per Active Day)", 
            "Annotation Coverage (% Coverage per Active Day)"
        ],
        shared_xaxes=True,
        vertical_spacing=0.12
    )
    
    # Plot 1: Daily raw bags (aggregated)
    fig.add_trace(go.Scatter(
        x=data.dates,
        y=data.raw_bags,
        mode='markers+lines',
        name='Total Raw Bags',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8)
    ), row=1, col=1)
    
    # Plot 2: Daily ML samples (aggregated)
    fig.add_trace(go.Scatter(
        x=data.dates,
        y=data.ml_samples,
        mode='markers+lines',
        name='Total ML Samples',
        line=dict(color='#ff7f0e', width=3),
        marker=dict(size=8)
    ), row=2, col=1)
    
    # Plot 3: Daily coverage
    fig.add_trace(go.Scatter(
        x=data.dates,
        y=data.coverage_percentages,
        mode='markers+lines',
        name='Coverage %',
        line=dict(color='#2ca02c', width=3),
        marker=dict(size=8),
        fill='tonexty' if len(data.coverage_percentages) > 0 else None,
        fillcolor='rgba(44, 160, 44, 0.2)'
    ), row=3, col=1)
    
    fig.update_layout(
        height=750,
        showlegend=False,
    )

    fig.update_annotations(font=dict(size=20,weight=400, color="#000"))
    
    # Update y-axis labels
    fig.update_yaxes(title_text="Total Bag Count", row=1, col=1)
    fig.update_yaxes(title_text="Total Sample Count", row=2, col=1)
    fig.update_yaxes(title_text="Coverage %", row=3, col=1)
    fig.update_xaxes(title_text="Date", row=3, col=1)
    
    st.plotly_chart(fig, use_container_width=True)


def _render_aggregated_summary(data: AggregatedTemporalData, filters: Dict):
    """Render summary metrics for aggregated view."""
    # Calculate summary statistics
    total_days = len(data.dates)
    total_bags = sum(data.raw_bags)
    total_samples = sum(data.ml_samples)
    
    # Average daily coverage
    avg_coverage = sum(data.coverage_percentages) / len(data.coverage_percentages) if data.coverage_percentages else 0
    
    # Display metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Active Days", f"{total_days:,}")
    
    with col2:
        st.metric("Total Bags", f"{total_bags:,}")
    
    with col3:
        st.metric("Total Samples", f"{total_samples:,}")
    
    with col4:
        st.metric("Avg Daily Coverage", f"{avg_coverage:.1f}%")
    
    # Second row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Active Laser Boxes", f"{len(data.contributing_lbs)}")
    
    with col2:
        avg_bags_per_day = total_bags / total_days if total_days > 0 else 0
        st.metric("Avg Bags/Day", f"{avg_bags_per_day:.1f}")
    
    with col3:
        avg_samples_per_day = total_samples / total_days if total_days > 0 else 0
        st.metric("Avg Samples/Day", f"{avg_samples_per_day:.1f}")
    
    
    # Show current selection context
    st.caption(
        f"Showing aggregated data for: {filters['cid']} → {filters['regionid']} → "
        f"{filters['fieldid']} → {filters['twid']} → All Laser Boxes"
    )
    


def _render_laser_box_breakdown(lb_stats: List[LaserBoxStats], agg_data: AggregatedTemporalData):
    """Render laser box breakdown with three side-by-side bar charts."""
    if not lb_stats:
        st.warning("No laser box data available")
        return

    # Sort by total bags for consistent display
    sorted_stats = sorted(lb_stats, key=lambda x: x.lb_id)
    lb_names      = [s.lb_id        for s in sorted_stats]
    bag_counts    = [s.total_bags   for s in sorted_stats]
    sample_counts = [s.total_samples for s in sorted_stats]
    active_days   = [s.active_days  for s in sorted_stats]

    # Create a 1x3 subplot
    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=[
            "Active Days per Laser Box",
            "Total Bags per Laser Box",
            "Total Samples per Laser Box",
        ],
        horizontal_spacing=0.1
    )

    # Bar 1: active days
    fig.add_trace(go.Bar(
        x=lb_names, y=active_days,
        marker_color='#2ca02c',
        text=active_days, textposition='outside'
    ), row=1, col=1)

    # Bar 2: total bags
    fig.add_trace(go.Bar(
        x=lb_names, y=bag_counts,
        marker_color='#1f77b4',
        text=bag_counts, textposition='outside'
    ), row=1, col=2)

    # Bar 3: total samples
    fig.add_trace(go.Bar(
        x=lb_names, y=sample_counts,
        marker_color='#ff7f0e',
        text=sample_counts, textposition='outside'
    ), row=1, col=3)


    fig.update_layout(
        height=500,
        showlegend=False,
        margin=dict(l=40, r=40, t=80, b=40)
    )

    # Y-axis titles
    fig.update_yaxes(title_text="Days", row=1, col=1)
    fig.update_yaxes(title_text="Count", row=1, col=2)
    fig.update_yaxes(title_text="Count",  row=1, col=3)

    # X-axis titles
    for c in (1, 2, 3):
        fig.update_xaxes(title_text="Laser Box ID", row=1, col=c)

    fig.update_annotations(font=dict(size=20,weight=400, color="#000"))
    st.plotly_chart(fig, use_container_width=True)


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
            "Annotation Coverage (% Annotated per Timestamp)"
        ],
        shared_xaxes=True,
        vertical_spacing=0.12  # Increased margin
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
    
    # Plot 3: Coverage analysis
    fig.add_trace(go.Scatter(
        x=display_timestamps,
        y=data.coverage_percentages,  
        mode='markers+lines',
        name='Coverage %',
        line=dict(color='#2ca02c', width=3),  # Green for positive metric
        marker=dict(size=8),
        fill='tonexty' if len(data.coverage_percentages) > 0 else None,
        fillcolor='rgba(44, 160, 44, 0.2)'
    ), row=3, col=1)
    
    # Update layout with increased height for better spacing
    fig.update_layout(
        height=750,  # Increased for better spacing
        showlegend=False,
    )

    fig.update_annotations(font=dict(size=20,weight=400, color="#000"))

    fig.update_yaxes(range=[0, 105], row=3)
    
    # Update y-axis labels
    fig.update_yaxes(title_text="Bag Count", row=1, col=1)
    fig.update_yaxes(title_text="Sample Count", row=2, col=1)
    fig.update_yaxes(title_text="Coverage %", row=3, col=1)
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
            )
    
    # Second row with more detailed metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Average Bags/Run", f"{(stats.total_raw_bags / stats.total_timestamps):.1f}" if stats.total_timestamps > 0 else "0")
    
    with col2:
        st.metric("Average Samples/Run", f"{(stats.total_ml_samples / stats.total_timestamps):.1f}" if stats.total_timestamps > 0 else "0")
    
    
    with col3:
        st.metric("Under-labeled Runs", f"{stats.under_labeled_count}")
    
    # Show current selection context
    st.caption(
        f"Showing data for: {filters['cid']} → {filters['regionid']} → "
        f"{filters['fieldid']} → {filters['twid']} → {filters['lbid']}"
    )
    
    # Display worst coverage timestamps (already sorted by service)
    if stats.under_labeled_timestamps:
        with st.expander(f"Worst Coverage Timestamps ({stats.under_labeled_count})"):
            for ts, gap, raw, ml in stats.under_labeled_timestamps[:10]:  # Show first 10
                coverage = 100 - gap
                st.text(f"{_format_timestamp(ts)}: {coverage:.1f}% coverage ({raw} bags, {ml} samples)")
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