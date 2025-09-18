"""
Enhanced Operations Page with Multi-Selection and Batch Operations
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional

from src.services import ServiceContainer
from src.models import InventoryItem, TransferJob, RunCoordinate


def render(services: ServiceContainer):
    """Main render function"""
    
    st.subheader("🔧 Data Inventory & Operations")
    st.write("")
    
    # Get current filters
    filters = st.session_state.get('global_filters', {})
    
    # Check if filters are complete (excluding lbid which can be "All")
    required_filters = ['cid', 'regionid', 'fieldid', 'twid']
    missing_filters = [f for f in required_filters if not filters.get(f)]
    
    if missing_filters:
        st.info(f"Please select filters in sidebar: {', '.join(missing_filters)}")
        return
    
    # Main content
    render_inventory_view(services, filters)


def render_inventory_view(services: ServiceContainer, filters: Dict):
    """Render the inventory view with data editor and side panel"""
    
    # Three column layout (40%, 30%, 30%)
    col1, col2, col3 = st.columns([0.2, 0.4, 0.1], vertical_alignment="center")

    # First column: Controls with horizontal layout
    with col1:
        with st.container(border=True):
            # Data Type and Filter in same row (horizontal)
            type_col, filter_col, refresh_col = st.columns([0.4, 0.6, 0.1], vertical_alignment="bottom")
            
            with type_col:
                st.markdown("**Data Type:**")
                data_type = st.selectbox(
                    "Data Type:", 
                    ["📦 Raw", "🎯 ML"], 
                    key="operations_data_type",
                    label_visibility="collapsed"
                )
            
            with filter_col:
                is_raw = "Raw" in data_type
                if is_raw:
                    filter_options = ["All", "🟠 Not Downloaded", "🔴 Issues"]
                else:
                    filter_options = ["All", "🟠 Not Downloaded", "🟡 Not Uploaded", "🔴 Issues"]
                
                st.markdown("**Filter:**")
                status_filter = st.selectbox(
                    "Filter:",
                    filter_options,
                    key="operations_filter",
                    label_visibility="collapsed"
                )
            
            with refresh_col:
                if st.button("🔄", help="Refresh inventory", use_container_width=True):
                    services.inventory_view.refresh_inventory()
                    st.rerun()

    # Second column: Metrics with left padding
    with col2:
        with st.spinner("Loading inventory..."):
            inventory_items = services.inventory_view.get_inventory_items(filters)
            
            if inventory_items:
                metrics = services.inventory_view.get_simple_metrics(inventory_items)
                
                # Add some left padding from filters
                _, metrics_container = st.columns([0.3, 0.9])
                with metrics_container:
                    with st.container():
                        # 1x4 horizontal layout
                        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
                        
                        with metric_col1:
                            st.metric("Cloud Bags", f"{metrics['cloud_bags']:,}")
                        
                        with metric_col2:
                            st.metric("Local Bags", f"{metrics['local_bags']:,}")
                        
                        with metric_col3:
                            st.metric("Cloud Samples", f"{metrics['cloud_samples']:,}")
                        
                        with metric_col4:
                            st.metric("Local Samples", f"{metrics['local_samples']:,}")

    # Third column remains empty

    st.divider()
    # Show data editor with side panel
    show_data_editor_view(inventory_items, data_type, status_filter, is_raw, services)


def show_data_editor_view(items: List[InventoryItem], data_type: str, status_filter: str, is_raw: bool, services: ServiceContainer):
    """Show data editor with side panel for details and batch operations"""
    
    # Sort items by timestamp (latest first)
    sorted_items = sorted(items, key=lambda x: x.coord.timestamp, reverse=True)
    
    # Filter items based on status filter
    filtered_items = filter_items_by_status(sorted_items, is_raw, status_filter)
    
    if not filtered_items:
        st.info(f"No items found for filter: {status_filter}")
        return
    
    # Create DataFrame for data editor
    df = create_inventory_dataframe(filtered_items, is_raw)
    
    # Initialize selection state if not exists
    selection_key = f"selection_state_{data_type}_{status_filter}"
    if selection_key not in st.session_state:
        st.session_state[selection_key] = set()
    
    # Convert stored selection back to DataFrame format
    stored_selection = st.session_state[selection_key]
    for i, item in enumerate(filtered_items):
        coord_key = item.coord.to_path_str()
        if coord_key in stored_selection:
            df.at[i, "Select"] = True
    
    # Three column layout: table takes 2 columns, side panel takes 1
    col_table, col_spacer, col_details = st.columns([2, 0.1, 1])
    
    with col_table:
        st.markdown("### 📊 Inventory Table")
        st.caption(f"Showing {len(filtered_items)} items")
        
        # Configure the data editor
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            height=500,  # Fixed height for consistent layout
            column_config={
                "Select": st.column_config.CheckboxColumn(
                    "Select",
                    help="Select rows for batch operations",
                    default=False,
                    width="small"
                ),
                "LB ID": st.column_config.TextColumn("LB ID", width="small"),
                "Timestamp": st.column_config.TextColumn("Date/Time", width="medium"),
                "Status": st.column_config.TextColumn("Status", width="medium"),
                "Data Info": st.column_config.TextColumn("Data", width="medium"),
                "Size/Samples": st.column_config.TextColumn("Size/Samples", width="medium")
            },
            disabled=["LB ID", "Timestamp", "Status", "Data Info", "Size/Samples"],
            key=f"inventory_editor_{data_type}_{status_filter}"
        )
        
        # Update stored selection based on current checkbox states
        current_selection = set()
        for i, row in edited_df.iterrows():
            if row["Select"]:
                coord_key = filtered_items[i].coord.to_path_str()
                current_selection.add(coord_key)
        st.session_state[selection_key] = current_selection
    
    with col_details:
        st.markdown("### 🔍 Selection Panel")
        
        # Find selected rows
        selected_rows = edited_df[edited_df["Select"] == True]
        selected_count = len(selected_rows)
        
        if selected_count == 0:
            st.info("👈 Select rows from the table for details or batch operations")
        elif selected_count == 1:
            # Single selection - show detailed analysis
            selected_idx = selected_rows.index[0]
            selected_item = filtered_items[selected_idx]
            render_single_item_details(selected_item, is_raw, services)
        else:
            # Multiple selection - show batch operations
            selected_indices = selected_rows.index.tolist()
            selected_items = [filtered_items[idx] for idx in selected_indices]
            render_batch_operations(selected_items, is_raw, selected_count, services)


def create_inventory_dataframe(items: List[InventoryItem], is_raw: bool) -> pd.DataFrame:
    """Create DataFrame for the data editor"""
    
    rows = []
    for item in items:
        coord = item.coord
        
        # Get status info based on data type
        if is_raw:
            cloud_status = item.cloud_raw_status
            local_status = item.local_raw_status
            sync_status = item.raw_sync_status
        else:
            cloud_status = item.cloud_ml_status
            local_status = item.local_ml_status
            sync_status = item.ml_sync_status
        
        # Format timestamp
        timestamp_display = format_timestamp_with_time(coord.timestamp)
        
        # Status display with emoji
        status_display = get_status_emoji_text(sync_status, is_raw)
        
        # Data info and size/samples
        if is_raw:
            if cloud_status and cloud_status.exists:
                data_info = f"📦 {cloud_status.bag_count} bags"
                size_info = format_size(cloud_status.total_size)
            elif local_status and local_status.downloaded:
                data_info = f"📦 {local_status.bag_count} bags"
                size_info = format_size(local_status.total_size)
            else:
                data_info = "📦 No data"
                size_info = "-"
        else:  # ML
            if cloud_status and cloud_status.exists:
                bag_count = len(cloud_status.bag_samples) if cloud_status.bag_samples else 0
                data_info = f"📂 {bag_count} bags"
                size_info = f"{cloud_status.total_samples:,} samples"
            elif local_status and local_status.downloaded:
                bag_count = len(local_status.bag_samples) if local_status.bag_samples else 0
                data_info = f"📂 {bag_count} bags"
                size_info = f"{local_status.total_samples:,} samples"
            else:
                data_info = "📂 No data"
                size_info = "-"
        
        rows.append({
            "Select": False,  # Default to not selected
            "LB ID": coord.lbid,
            "Timestamp": timestamp_display,
            "Status": status_display,
            "Data Info": data_info,
            "Size/Samples": size_info
        })
    
    return pd.DataFrame(rows)


def render_single_item_details(item: InventoryItem, is_raw: bool, services: ServiceContainer):
    """Render details panel for a single selected item"""
    
    coord = item.coord
    
    # Header
    st.markdown(f"**{coord.lbid}**")
    st.markdown(f"*{format_timestamp_with_time(coord.timestamp)}*")
    
    st.divider()
    
    # Status section
    if is_raw:
        render_raw_details(item)
    else:
        render_ml_details(item)
    
    st.divider()
    
    # Single item actions
    st.markdown("#### ⚡ Actions")
    render_single_item_actions(item, is_raw, services)


def render_batch_operations(items: List[InventoryItem], is_raw: bool, count: int, services: ServiceContainer):
    """Render batch operations panel for multiple selected items"""
    
    st.markdown(f"**{count} items selected**")
    
    # Group by status to determine available batch operations
    status_groups = {}
    for item in items:
        sync_status = item.raw_sync_status if is_raw else item.ml_sync_status
        if sync_status not in status_groups:
            status_groups[sync_status] = []
        status_groups[sync_status].append(item)
    
    # Show status breakdown
    st.markdown("**Status Breakdown:**")
    for status, status_items in status_groups.items():
        emoji = get_status_emoji_text(status, is_raw)
        st.markdown(f"• {emoji}: {len(status_items)} items")
    
    st.divider()
    
    # Batch operation buttons
    st.markdown("#### ⚡ Batch Operations")
    
    # Only allow batch operations if all items have the same non-issue status
    if len(status_groups) == 1:
        status = list(status_groups.keys())[0]
        
        if status == "cloud_only":
            # All items need downloading
            if is_raw:
                button_text = f"⬇️ Download rosbags from {count} Runs"
                button_help = "Download rosbags from selected runs"
            else:
                button_text = f"⬇️ Download samples from {count} Runs" 
                button_help = "Download samples from selected runs"
            
            if st.button(button_text, help=button_help, use_container_width=True, type="primary"):
                execute_batch_download(items, is_raw, services)
        
        elif status == "local_only" and not is_raw:  # Only for ML
            # All items need uploading
            button_text = f"⬆️ Upload {count} Run Samples"
            button_help = "Upload all selected run samples"
            
            if st.button(button_text, help=button_help, use_container_width=True, type="primary"):
                execute_batch_upload(items, services)
        
        elif status in ["missing", "mismatch", "unknown"]:
            st.warning("⚠️ Cannot perform batch operations on items with issues. Please handle individually.")
        
        elif status == "synced":
            st.info("✅ All selected items are already synced.")
        
        else:
            st.info("No batch operations available for current selection.")
    
    else:
        st.warning("⚠️ Cannot perform batch operations on items with different statuses. Please select items with the same status.")


def render_single_item_actions(item: InventoryItem, is_raw: bool, services: ServiceContainer):
    """Render actions for a single item"""
    
    coord = item.coord
    sync_status = item.raw_sync_status if is_raw else item.ml_sync_status
    cloud_status = item.cloud_raw_status if is_raw else item.cloud_ml_status
    local_status = item.local_raw_status if is_raw else item.local_ml_status
    
    if is_raw:
        # Raw data actions
        download_available = (sync_status in ["cloud_only", "mismatch"] and 
                             cloud_status and cloud_status.exists)
        
        if st.button("⬇️ Download", disabled=not download_available, 
                    help="Download run rosbags", use_container_width=True,
                    key=f"dl_raw_{coord.to_path_str()}"):
            execute_single_download(item, is_raw, services)
    
    else:
        # ML data actions
        download_available = (sync_status in ["cloud_only", "mismatch"] and 
                             cloud_status and cloud_status.exists)
        upload_available = (sync_status in ["local_only", "mismatch"] and 
                           local_status and local_status.downloaded)
        
        if st.button("⬇️ Download", disabled=not download_available,
                    help="Download run samples", use_container_width=True,
                    key=f"dl_ml_{coord.to_path_str()}"):
            execute_single_download(item, is_raw, services)
        
        if st.button("⬆️ Upload", disabled=not upload_available,
                    help="Upload run samples", use_container_width=True,
                    key=f"ul_ml_{coord.to_path_str()}"):
            execute_single_upload(item, services)


def execute_single_download(item: InventoryItem, is_raw: bool, services: ServiceContainer):
    """Execute download for a single item"""
    st.info("🚧 Single download will be implemented")
    # TODO: Create TransferJob and execute via services.cloud_operations


def execute_single_upload(item: InventoryItem, services: ServiceContainer):
    """Execute upload for a single item"""
    st.info("🚧 Single upload will be implemented")
    # TODO: Create TransferJob and execute via services.cloud_operations


def execute_batch_download(items: List[InventoryItem], is_raw: bool, services: ServiceContainer):
    """Execute batch download operation"""
    st.info(f"🚧 Batch download of {len(items)} items will be implemented")
    # TODO: Create multiple TransferJobs or batch TransferJob and execute


def execute_batch_upload(items: List[InventoryItem], services: ServiceContainer):
    """Execute batch upload operation"""
    st.info(f"🚧 Batch upload of {len(items)} items will be implemented")
    # TODO: Create multiple TransferJobs or batch TransferJob and execute


# Keep existing helper functions
def render_raw_details(item: InventoryItem):
    """Render raw data details in compact format"""
    
    cloud_status = item.cloud_raw_status
    local_status = item.local_raw_status
    sync_status = item.raw_sync_status
    issues = item.raw_issues
    
    st.markdown("#### 📊 Raw Data Status")
    
    if sync_status == "synced":
        st.success("✅ Synced")
        if cloud_status and cloud_status.exists:
            st.markdown(f"**Bags**: {cloud_status.bag_count}")
            st.markdown(f"**Size**: {format_size(cloud_status.total_size)}")
    
    elif sync_status == "cloud_only":
        st.warning("🟠 Not Downloaded")
        if cloud_status and cloud_status.exists:
            st.markdown(f"**Cloud Bags**: {cloud_status.bag_count}")
            st.markdown(f"**Cloud Size**: {format_size(cloud_status.total_size)}")
    
    elif sync_status == "local_only":
        st.info("💻 Local Only")
        if local_status and local_status.downloaded:
            st.markdown(f"**Local Bags**: {local_status.bag_count}")
            st.markdown(f"**Local Size**: {format_size(local_status.total_size)}")
    
    elif sync_status == "missing":
        st.error("🔴 Missing")
        st.markdown("Data missing from both local and cloud sources")
        if issues:
            for issue in issues:
                st.markdown(f"• {issue}")
    
    elif sync_status in ["mismatch", "unknown"]:
        st.error(f"🔴 {sync_status.title()}")
        if issues:
            for issue in issues:
                st.markdown(f"• {issue}")
        
        # Show comparison if both exist
        if cloud_status and cloud_status.exists and local_status and local_status.downloaded:
            st.markdown("**Comparison:**")
            st.markdown(f"Cloud: {cloud_status.bag_count} bags, {format_size(cloud_status.total_size)}")
            st.markdown(f"Local: {local_status.bag_count} bags, {format_size(local_status.total_size)}")


def render_ml_details(item: InventoryItem):
    """Render ML data details in compact format"""
    
    cloud_status = item.cloud_ml_status
    local_status = item.local_ml_status
    sync_status = item.ml_sync_status
    issues = item.ml_issues
    
    st.markdown("#### 🎯 ML Data Status")
    
    if sync_status == "synced":
        st.success("✅ Synced")
        if cloud_status and cloud_status.exists:
            bag_count = len(cloud_status.bag_samples) if cloud_status.bag_samples else 0
            st.markdown(f"**Bags**: {bag_count}")
            st.markdown(f"**Samples**: {cloud_status.total_samples:,}")
    
    elif sync_status == "cloud_only":
        st.warning("🟠 Not Downloaded")
        if cloud_status and cloud_status.exists:
            bag_count = len(cloud_status.bag_samples) if cloud_status.bag_samples else 0
            st.markdown(f"**Cloud Bags**: {bag_count}")
            st.markdown(f"**Cloud Samples**: {cloud_status.total_samples:,}")
    
    elif sync_status == "local_only":
        st.info("🟡 Not Uploaded")
        if local_status and local_status.downloaded:
            bag_count = len(local_status.bag_samples) if local_status.bag_samples else 0
            st.markdown(f"**Local Bags**: {bag_count}")
            st.markdown(f"**Local Samples**: {local_status.total_samples:,}")
    
    elif sync_status == "missing":
        st.error("🔴 Missing")
        st.markdown("Data missing from both local and cloud sources")
        if issues:
            for issue in issues:
                st.markdown(f"• {issue}")
    
    elif sync_status in ["mismatch", "unknown"]:
        st.error(f"🔴 {sync_status.title()}")
        if issues:
            for issue in issues:
                st.markdown(f"• {issue}")
        
        # Show comparison if both exist
        if cloud_status and cloud_status.exists and local_status and local_status.downloaded:
            st.markdown("**Comparison:**")
            cloud_bags = len(cloud_status.bag_samples) if cloud_status.bag_samples else 0
            local_bags = len(local_status.bag_samples) if local_status.bag_samples else 0
            st.markdown(f"Cloud: {cloud_bags} bags, {cloud_status.total_samples:,} samples")
            st.markdown(f"Local: {local_bags} bags, {local_status.total_samples:,} samples")


def get_status_emoji_text(sync_status: str, is_raw: bool) -> str:
    """Get emoji and text for status"""
    if sync_status == "cloud_only":
        return "🟠 Not Downloaded"
    elif sync_status == "local_only":
        return "🟡 Not Uploaded" if not is_raw else "💻 Local Only"
    elif sync_status in ["missing", "mismatch", "unknown"]:
        return f"🔴 {sync_status.title()}"
    elif sync_status == "synced":
        return "✅ Synced"
    else:
        return "❓ Unknown"


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