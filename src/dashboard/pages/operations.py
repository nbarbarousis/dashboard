"""
Clean Operations Page 
"""

import streamlit as st
from datetime import datetime
from typing import Dict, List

from src.services import ServiceContainer
from src.models import InventoryItem


def render(services: ServiceContainer):
    """Main render function"""
    
    st.title("🔧 Data Operations")
    
    # Get current filters
    filters = st.session_state.get('global_filters', {})
    
    # Check if filters are complete
    required_filters = ['cid', 'regionid', 'fieldid', 'twid', 'lbid']
    missing_filters = [f for f in required_filters if not filters.get(f)]
    
    if missing_filters:
        st.info(f"Please select filters in sidebar: {', '.join(missing_filters)}")
        return
    
    # Main content
    render_inventory_view(services, filters)


def render_inventory_view(services: ServiceContainer, filters: Dict):
    """Render the inventory view with combined cloud/local status and filtering"""
    
    st.subheader("Data Inventory")
    st.caption("Combined view of cloud and local data with status filtering")
    
    # Preserve current selections
    current_data_type = st.session_state.get('operations_data_type', "📦 Raw")
    current_filter = st.session_state.get('operations_filter', "All")
    
    # Controls
    col1, col2, col3 = st.columns([2, 3, 1])
    with col1:
        data_type = st.radio(
            "Data Type:", 
            ["📦 Raw", "🎯 ML"], 
            horizontal=True, 
            key="operations_data_type",
            index=0 if current_data_type == "📦 Raw" else 1
        )
    
    with col2:
        is_raw = "Raw" in data_type
        if is_raw:
            filter_options = ["All", "🟠 Not Downloaded", "🔴 Issues"]
        else:  # ML
            filter_options = ["All", "🟠 Not Downloaded", "🟡 Not Uploaded", "🔴 Issues"]
        
        status_filter = st.radio(
            "Filter:",
            filter_options,
            horizontal=True,
            key="operations_filter",
            index=0 if current_filter not in filter_options else filter_options.index(current_filter)
        )
    
    with col3:
        if st.button("🔄 Refresh", help="Refresh inventory"):
            services.inventory_view.refresh_inventory()
            st.rerun()
    
    # Status legend
    if is_raw:
        st.caption("🟠 Not Downloaded (cloud only) • 🔴 Issues (missing/mismatch/unknown)")
    else:
        st.caption("🟠 Not Downloaded (cloud only) • 🟡 Not Uploaded (local only) • 🔴 Issues (missing/mismatch/unknown)")
    
    st.divider()
    
    # Get data from service
    with st.spinner("Loading inventory..."):
        inventory_items = services.inventory_view.get_inventory_items(filters)
        
        if not inventory_items:
            st.info("No data found for current filters")
            return
    
    # Show combined view with filtering
    show_combined_inventory_view(inventory_items, data_type, status_filter)


def show_combined_inventory_view(items: List[InventoryItem], data_type: str, status_filter: str):
    """Show combined inventory view with status-based filtering and color coding"""
    
    is_raw = "Raw" in data_type
    
    # Sort items by timestamp (latest first)
    sorted_items = sorted(items, key=lambda x: x.coord.timestamp, reverse=True)
    
    # Filter items based on status filter
    filtered_items = filter_items_by_status(sorted_items, is_raw, status_filter)
    
    if not filtered_items:
        st.info(f"No items found for filter: {status_filter}")
        return
    
    # Create table data with status color coding
    table_rows = []
    for item in filtered_items:
        coord = item.coord
        
        # Get status info based on data type
        if is_raw:
            cloud_status = item.cloud_raw_status
            local_status = item.local_raw_status
            sync_status = item.raw_sync_status
            issues = item.raw_issues
        else:
            cloud_status = item.cloud_ml_status
            local_status = item.local_ml_status
            sync_status = item.ml_sync_status
            issues = item.ml_issues
        
        # Format timestamp with date and time
        timestamp_display = format_timestamp_with_time(coord.timestamp)
        
        # Determine row background color and status symbols
        row_color, status_symbols = get_status_display_info(
            cloud_status, local_status, sync_status, is_raw
        )
        
        # Build data columns based on type
        if is_raw:
            if cloud_status and cloud_status.exists:
                bags = f"📦 {cloud_status.bag_count}"
                size = f"💾 {format_size(cloud_status.total_size)}"
            elif local_status and local_status.downloaded:
                bags = f"📦 {local_status.bag_count}"
                size = f"💾 {format_size(local_status.total_size)}"
            else:
                bags = "📦 -"
                size = "💾 -"
            
            data_columns = [bags, size]
        else:  # ML
            total_samples = 0
            bag_count = 0
            
            if cloud_status and cloud_status.exists:
                total_samples = cloud_status.total_samples
                bag_count = len(cloud_status.bag_samples) if cloud_status.bag_samples else 0
            elif local_status and local_status.downloaded:
                total_samples = local_status.total_samples
                bag_count = len(local_status.bag_samples) if local_status.bag_samples else 0
            
            data_columns = [f"📂 {bag_count}", f"🎯 {total_samples}"]
        
        # Add row with color coding
        row_data = [coord.lbid, timestamp_display] + data_columns + [status_symbols]
        table_rows.append((row_data, row_color))
    
    # Headers based on data type
    if is_raw:
        headers = ["LBID", "Date & Time", "Bags", "Size", "Status"]
    else:
        headers = ["LBID", "Date & Time", "Bags", "Samples", "Status"]
    
    # Render colored table
    render_colored_table(table_rows, headers)


def filter_items_by_status(items: List[InventoryItem], is_raw: bool, status_filter: str) -> List[InventoryItem]:
    """Filter items based on selected status filter"""
    
    if status_filter == "All":
        return items
    
    filtered = []
    for item in items:
        sync_status = item.raw_sync_status if is_raw else item.ml_sync_status
        
        if status_filter == "🟠 Not Downloaded":
            if sync_status == "cloud_only":
                filtered.append(item)
        elif status_filter == "🟡 Not Uploaded":
            if sync_status == "local_only":
                filtered.append(item)
        elif status_filter == "🔴 Issues":
            if sync_status in ["missing", "mismatch", "unknown"]:
                filtered.append(item)
    
    return filtered


def get_status_display_info(cloud_status, local_status, sync_status: str, is_raw: bool):
    """Get display color and status symbols for an item"""
    
    # Determine background color based on sync status
    if sync_status == "cloud_only":
        row_color = "orange"
        status_symbols = "🟠 Not Downloaded"
    elif sync_status == "local_only":
        row_color = "yellow" if not is_raw else None
        status_symbols = "🟡 Not Uploaded" if not is_raw else "💻 Local Only"
    elif sync_status in ["missing", "mismatch", "unknown"]:
        row_color = "red"
        status_symbols = f"🔴 {sync_status.title()}"
    elif sync_status == "synced":
        row_color = None
        status_symbols = "✅ Synced"
    else:
        row_color = None
        status_symbols = "❓ Unknown"
    
    return row_color, status_symbols


# Helper functions
def format_timestamp_as_date(timestamp: str) -> str:
    """Format timestamp as just date for cleaner display"""
    try:
        # Handle different timestamp formats
        normalized = timestamp.replace('_', '-').replace('T', ' ')
        if 'Z' in normalized:
            normalized = normalized.replace('Z', '')
        
        # Try parsing
        try:
            dt = datetime.fromisoformat(normalized)
            return dt.strftime('%m-%d')
        except:
            # Fallback - just take first part
            return timestamp.split('T')[0] if 'T' in timestamp else timestamp[:10]
    except:
        return timestamp[:8]  # Fallback


def format_timestamp_with_time(timestamp: str) -> str:
    """Format timestamp to show date and time"""
    try:
        # Handle different timestamp formats
        normalized = timestamp.replace('_', '-').replace('T', ' ')
        if 'Z' in normalized:
            normalized = normalized.replace('Z', '')
        
        # Try parsing
        try:
            dt = datetime.fromisoformat(normalized)
            return dt.strftime('%m-%d %H:%M')
        except:
            # Fallback - extract parts manually
            if 'T' in timestamp:
                date_part, time_part = timestamp.split('T')
                # Extract date (MM-DD format)
                date_formatted = date_part.split('-')[-2:] if '-' in date_part else date_part[4:8] + '-' + date_part[8:10]
                # Extract time (HH:MM format)
                time_formatted = time_part[:5] if ':' in time_part else time_part[:2] + ':' + time_part[2:4]
                return f"{'-'.join(date_formatted[-2:])} {time_formatted}"
            else:
                return timestamp[:8]  # Fallback
    except:
        return timestamp[:12]  # Fallback


def format_size(size_bytes: int) -> str:
    """Format bytes as human readable"""
    if size_bytes >= 1024**3:
        return f"{size_bytes / (1024**3):.1f}GB"
    elif size_bytes >= 1024**2:
        return f"{size_bytes / (1024**2):.1f}MB" 
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f}KB"
    else:
        return f"{size_bytes}B"


def render_colored_table(rows_with_colors: List[tuple], headers: List[str]):
    """Render a table with color-coded rows"""
    
    # Create header HTML
    header_html = ""
    for header in headers:
        header_html += f"<th style='border: 1px solid #ddd; padding: 12px; background-color: #f5f5f5; text-align: left;'>{header}</th>"
    
    # Create rows HTML with color coding
    rows_html = ""
    for row_data, row_color in rows_with_colors:
        # Determine background color style
        if row_color == "orange":
            bg_style = "background-color: #FFF3CD; border-left: 4px solid #FF8C00;"
        elif row_color == "yellow":
            bg_style = "background-color: #FFF3E0; border-left: 4px solid #FFB300;"
        elif row_color == "red":
            bg_style = "background-color: #FFEBEE; border-left: 4px solid #F44336;"
        else:
            bg_style = ""
        
        rows_html += f"<tr style='{bg_style}'>"
        for cell in row_data:
            rows_html += f"<td style='border: 1px solid #ddd; padding: 12px; text-align: left;'>{cell}</td>"
        rows_html += "</tr>"
    
    # Complete table HTML
    table_html = f"""
    <div style='width: 100%; display: flex; justify-content: center; margin: 20px 0;'>
        <table style='
            border-collapse: collapse; 
            width: 80%; 
            max-width: 1000px;
            border: 1px solid #ddd;
            margin: 0 auto;
        '>
            <thead>
                <tr>{header_html}</tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
    """
    
    st.markdown(table_html, unsafe_allow_html=True)