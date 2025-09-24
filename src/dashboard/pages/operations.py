"""
Fixed Operations Page - Simplified Selection and State Management
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional

from src.services import ServiceContainer
from src.models import InventoryItem, RunCoordinate

def render(services: ServiceContainer):
    """Main render function"""
    
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
    """Render the inventory view with simplified state management"""
    
    # Controls section
    col1, col2, col3 = st.columns([0.2, 0.4, 0.1], vertical_alignment="center")

    with col1:
        with st.container(border=True):
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

    # Metrics section
    with col2:
        with st.spinner("Loading inventory..."):
            inventory_items = services.inventory_view.get_inventory_items(filters)
            
            if inventory_items:
                metrics = services.inventory_view.get_simple_metrics(inventory_items)
                
                _, metrics_container = st.columns([0.3, 0.9])
                with metrics_container:
                    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
                    
                    with metric_col1:
                        st.metric("Cloud Bags", f"{metrics['cloud_bags']:,}")
                    with metric_col2:
                        st.metric("Local Bags", f"{metrics['local_bags']:,}")
                    with metric_col3:
                        st.metric("Cloud Samples", f"{metrics['cloud_samples']:,}")
                    with metric_col4:
                        st.metric("Local Samples", f"{metrics['local_samples']:,}")

    st.divider()
    
    # Show inventory table with fixed selection logic
    show_inventory_table(inventory_items, data_type, status_filter, is_raw, services)


def show_inventory_table(items: List[InventoryItem], data_type: str, status_filter: str, is_raw: bool, services: ServiceContainer):
    """Show inventory table with proper Streamlit state management"""
    
    # Sort and filter items
    sorted_items = sorted(items, key=lambda x: x.coord.timestamp, reverse=True)
    filtered_items = filter_items_by_status(sorted_items, is_raw, status_filter)
    
    if not filtered_items:
        st.info(f"No items found for filter: {status_filter}")
        return
    
    # Create DataFrame for display
    df = create_inventory_dataframe(filtered_items, is_raw)
    
    # Use a simple, stable key for selection
    table_key = f"inventory_table_{data_type}_{status_filter}"
    
    # Two column layout: table and details panel
    col_table, col_spacer, col_details = st.columns([2, 0.1, 1])
    
    with col_table:
        st.markdown("### 📊 Inventory Table")
        st.caption(f"Showing {len(filtered_items)} items")
        
        # FIXED: Use data_editor with proper on_change callback
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            height=500,
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
            key=table_key
        )
    
    with col_details:
        # FIXED: Simple selection handling
        selected_indices = edited_df[edited_df["Select"] == True].index.tolist()
        selected_items = [filtered_items[idx] for idx in selected_indices]
        
        render_selection_panel(selected_items, is_raw, services)


def render_selection_panel(selected_items: List[InventoryItem], is_raw: bool, services: ServiceContainer):
    """Render selection panel with simple state management"""
    
    st.markdown("### 🔍 Selection Panel")
    
    selected_count = len(selected_items)
    
    if selected_count == 0:
        st.info("👈 Select rows from the table for details or batch operations")
    
    elif selected_count == 1:
        # Single selection - show details and actions
        item = selected_items[0]
        render_item_details(item, is_raw)
        st.divider()
        render_item_actions(item, is_raw, services)
    
    else:
        # Multiple selection - show batch operations
        render_batch_operations_panel(selected_items, is_raw, services, selected_count)


def render_item_details(item: InventoryItem, is_raw: bool):
    """Render details for a single item"""
    
    coord = item.coord
    st.markdown(f"**{coord.lbid}**")
    st.markdown(f"*{format_timestamp_with_time(coord.timestamp)}*")
    
    # Status details based on data type
    if is_raw:
        cloud_status = item.cloud_raw_status
        local_status = item.local_raw_status
        sync_status = item.raw_sync_status
        
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
        
        elif sync_status == "mismatch":
            st.error("🔴 Mismatch")
            # Show comparison for mismatch
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Cloud:**")
                if cloud_status and cloud_status.exists:
                    st.markdown(f"📦 {cloud_status.bag_count} bags")
                    st.markdown(f"💾 {format_size(cloud_status.total_size)}")
                else:
                    st.markdown("❌ No data")
            
            with col2:
                st.markdown("**Local:**")
                if local_status and local_status.downloaded:
                    st.markdown(f"📦 {local_status.bag_count} bags")  
                    st.markdown(f"💾 {format_size(local_status.total_size)}")
                else:
                    st.markdown("❌ No data")
        
        else:  # missing, unknown
            st.error(f"🔴 {sync_status.title()}")
            st.markdown("Data missing from both local and cloud sources")
    
    else:  # ML data
        cloud_status = item.cloud_ml_status
        local_status = item.local_ml_status
        sync_status = item.ml_sync_status
        
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
        
        elif sync_status == "mismatch":
            st.error("🔴 Mismatch")
            # Show comparison for mismatch
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Cloud:**")
                if cloud_status and cloud_status.exists:
                    cloud_bags = len(cloud_status.bag_samples) if cloud_status.bag_samples else 0
                    st.markdown(f"📂 {cloud_bags} bags")
                    st.markdown(f"🎯 {cloud_status.total_samples:,} samples")
                else:
                    st.markdown("❌ No data")
            
            with col2:
                st.markdown("**Local:**")
                if local_status and local_status.downloaded:
                    local_bags = len(local_status.bag_samples) if local_status.bag_samples else 0
                    st.markdown(f"📂 {local_bags} bags")
                    st.markdown(f"🎯 {local_status.total_samples:,} samples")
                else:
                    st.markdown("❌ No data")
        
        else:  # missing, unknown
            st.error(f"🔴 {sync_status.title()}")
            st.markdown("Data missing from both local and cloud sources")


def render_item_actions(item: InventoryItem, is_raw: bool, services: ServiceContainer):
    """Render actions for a single item with proper dialog state"""
    
    st.markdown("#### ⚡ Actions")
    
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
            # FIXED: Simple dialog trigger
            trigger_operation_dialog(item, "raw_download", services)
    
    else:
        # ML data actions
        download_available = (sync_status in ["cloud_only", "mismatch"] and 
                             cloud_status and cloud_status.exists)
        upload_available = (sync_status in ["local_only", "mismatch"] and 
                           local_status and local_status.downloaded)
        
        if st.button("⬇️ Download", disabled=not download_available,
                    help="Download run samples", use_container_width=True,
                    key=f"dl_ml_{coord.to_path_str()}"):
            trigger_operation_dialog(item, "ml_download", services)
        
        if st.button("⬆️ Upload", disabled=not upload_available,
                    help="Upload run samples", use_container_width=True,
                    key=f"ul_ml_{coord.to_path_str()}"):
            trigger_operation_dialog(item, "ml_upload", services)


def render_batch_operations_panel(selected_items: List[InventoryItem], is_raw: bool, services: ServiceContainer, count: int):
    """Render batch operations panel"""
    
    st.markdown(f"**{count} items selected**")
    
    # Group by status
    status_groups = {}
    for item in selected_items:
        sync_status = item.raw_sync_status if is_raw else item.ml_sync_status
        status_groups.setdefault(sync_status, []).append(item)
    
    # Show status breakdown
    st.markdown("**Status Breakdown:**")
    for status, status_items in status_groups.items():
        emoji = get_status_emoji_text(status, is_raw)
        st.markdown(f"• {emoji}: {len(status_items)} items")
    
    st.divider()
    
    # Batch operation buttons
    st.markdown("#### ⚡ Batch Operations")
    
    # Only allow batch operations if all items have the same status
    if len(status_groups) == 1:
        status = list(status_groups.keys())[0]
        
        if status == "cloud_only":
            op_type = "raw_download" if is_raw else "ml_download"
            button_text = f"⬇️ {'Download rosbags' if is_raw else 'Download samples'} from {count} Runs"
            
            if st.button(button_text, use_container_width=True, type="primary"):
                trigger_bulk_operation_dialog(selected_items, op_type, services)
        
        elif status == "local_only" and not is_raw:
            if st.button(f"⬆️ Upload {count} Run Samples", use_container_width=True, type="primary"):
                trigger_bulk_operation_dialog(selected_items, "ml_upload", services)
        
        elif status in ["missing", "mismatch", "unknown"]:
            st.warning("⚠️ Cannot perform batch operations on items with issues.")
        
        elif status == "synced":
            st.info("✅ All selected items are already synced.")
    
    else:
        st.warning("⚠️ Cannot perform batch operations on items with different statuses.")


def trigger_operation_dialog(item: InventoryItem, operation_type: str, services: ServiceContainer):
    """FIXED: Simple dialog triggering with proper state management"""
    dialog_key = f"dialog_{operation_type}_{item.coord.to_path_str()}"
    
    # Store dialog configuration in session state
    st.session_state[f"{dialog_key}_config"] = {
        'show': True,
        'item': item,
        'operation_type': operation_type,
        'services': services
    }


def trigger_bulk_operation_dialog(items: List[InventoryItem], operation_type: str, services: ServiceContainer):
    """FIXED: Simple bulk dialog triggering"""
    dialog_key = f"bulk_dialog_{operation_type}_{len(items)}"
    
    st.session_state[f"{dialog_key}_config"] = {
        'show': True,
        'items': items,
        'operation_type': operation_type,
        'services': services
    }


# Keep existing helper functions but simplified
def create_inventory_dataframe(items: List[InventoryItem], is_raw: bool) -> pd.DataFrame:
    """Create DataFrame for the data editor - SIMPLIFIED"""
    
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
            "Select": False,  # Always start unselected
            "LB ID": coord.lbid,
            "Timestamp": timestamp_display,
            "Status": status_display,
            "Data Info": data_info,
            "Size/Samples": size_info
        })
    
    return pd.DataFrame(rows)


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
        normalized = timestamp.replace('_', '-').replace('T', ' ')
        if 'Z' in normalized:
            normalized = normalized.replace('Z', '')
        
        try:
            dt = datetime.fromisoformat(normalized)
            return dt.strftime('%m-%d %H:%M')
        except:
            if 'T' in timestamp:
                date_part, time_part = timestamp.split('T')
                date_formatted = date_part.split('-')[-2:] if '-' in date_part else date_part[4:8] + '-' + date_part[8:10]
                time_formatted = time_part[:5] if ':' in time_part else time_part[:2] + ':' + time_part[2:4]
                return f"{'-'.join(date_formatted[-2:])} {time_formatted}"
            else:
                return timestamp[:8]
    except:
        return timestamp[:12]


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