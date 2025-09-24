"""
Properly Separated Dialog System - Following Streamlit Best Practices

Key Principles:
1. STATE MANAGEMENT - Track where we are in the dialog flow
2. UI RENDERING - Show UI based on current state  
3. ACTION EXECUTION - Execute operations and update state

This eliminates the layout constraint and execution state loss issues.
"""

import streamlit as st
from typing import Dict, List, Optional
from dataclasses import dataclass

# ============================================================================
# STATE MANAGEMENT - What phase are we in?
# ============================================================================

@dataclass
class DialogState:
    """Track dialog state across reruns"""
    phase: str = "config"  # "config", "preview", "executing", "complete"
    selection_criteria: Dict = None
    conflict_resolution: str = "skip"
    preview_result: Dict = None
    execution_result: Dict = None
    error_message: str = None

def get_dialog_state(dialog_key: str) -> DialogState:
    """Get or create dialog state"""
    state_key = f"{dialog_key}_state"
    if state_key not in st.session_state:
        st.session_state[state_key] = DialogState()
    return st.session_state[state_key]

def update_dialog_state(dialog_key: str, **kwargs):
    """Update dialog state"""
    state = get_dialog_state(dialog_key)
    for key, value in kwargs.items():
        setattr(state, key, value)

# ============================================================================
# UI RENDERING - Show UI based on current state
# ============================================================================

def render_single_operation_dialog(dialog_key: str, config: Dict):
    """Render single operation dialog with proper state separation"""
    
    item = config['item']
    operation_type = config['operation_type']
    services = config['services']
    
    state = get_dialog_state(dialog_key)
    
    @st.dialog(f"Configure {get_operation_title(operation_type)}", width="large")
    def operation_modal():
        # Render different UI based on current phase
        if state.phase == "config":
            render_config_phase(dialog_key, item, operation_type, services, state)
        elif state.phase == "preview":
            render_preview_phase(dialog_key, item, operation_type, services, state)
        elif state.phase == "executing":
            render_executing_phase(dialog_key, state)
        elif state.phase == "complete":
            render_complete_phase(dialog_key, state)
    
    operation_modal()

def render_config_phase(dialog_key: str, item, operation_type: str, services, state: DialogState):
    """Render the configuration phase UI"""
    
    st.markdown(f"**Run:** {item.coord.lbid} / {format_timestamp_simple(item.coord.timestamp)}")
    
    # Get dialog data
    dialog_data = services.operations_orchestration.get_operation_dialog_data(item, operation_type)
    
    # Selection criteria section
    st.markdown("#### Selection Criteria")
    selection_criteria = render_selection_criteria(dialog_data, dialog_key)
    
    # Conflict resolution section
    st.markdown("#### Conflict Resolution")
    conflict_resolution = st.selectbox(
        "How to handle existing files:",
        dialog_data.conflict_options,
        key=f"{dialog_key}_conflict",
        help="Skip: Keep existing files | Overwrite: Replace existing files"
    )
    
    st.divider()
    
    # Action buttons - FULL WIDTH (not in columns)
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔍 Preview", type="primary", use_container_width=True, key=f"{dialog_key}_preview_btn"):
            if validate_selection(selection_criteria):
                # UPDATE STATE - don't render UI here!
                execute_preview_action(dialog_key, item, operation_type, selection_criteria, conflict_resolution, services)
            else:
                st.error("Please select items to transfer")
    
    with col2:
        if st.button("❌ Cancel", use_container_width=True, key=f"{dialog_key}_cancel_btn"):
            close_dialog(dialog_key)
            st.rerun()

def render_preview_phase(dialog_key: str, item, operation_type: str, services, state: DialogState):
    """Render the preview phase UI - FULL WIDTH"""
    
    st.markdown(f"**Run:** {item.coord.lbid} / {format_timestamp_simple(item.coord.timestamp)}")
    
    # Add padding/margin on top
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Title on the left
    st.markdown("**Preview Results**")
    
    # Show preview results - FULL WIDTH (no column constraints)
    if state.error_message:
        st.error(f"Preview failed: {state.error_message}")
        
        # Back button
        if st.button("🔙 Back to Configuration", use_container_width=True):
            update_dialog_state(dialog_key, phase="config", error_message=None)
            st.rerun()
    
    elif state.preview_result:
        plan = state.preview_result.get("plan")  # This gets a TransferPlan dataclass
        if plan:
            # Metrics - FULL WIDTH
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Files to Transfer", plan.total_files)  # Direct attribute access
            with col2:
                st.metric("Total Size", format_size(plan.total_size))  # Direct attribute access
            with col3:
                st.metric("Conflicts", len(plan.conflicts))  # Direct attribute access
            
            # File details
            if plan.total_files > 0:
                if plan.files_to_transfer:
                    with st.expander(f"📥 Files to Transfer ({len(plan.files_to_transfer)})"):
                        for file_info in plan.files_to_transfer[:5]:
                            filename = file_info.get('filename', file_info.get('cloud_name', 'unknown'))
                            size = format_size(file_info.get('size', 0))
                            st.text(f"• {filename} ({size})")
                        if len(plan.files_to_transfer) > 5:
                            st.text(f"... and {len(plan.files_to_transfer) - 5} more")
                
                st.divider()
                
                # Action buttons
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🔙 Back", use_container_width=True):
                        update_dialog_state(dialog_key, phase="config")
                        st.rerun()
                
                with col2:
                    if st.button("✅ Execute Transfer", type="primary", use_container_width=True):
                        # UPDATE STATE - don't execute here!
                        execute_transfer_action(dialog_key, item, operation_type, state.selection_criteria, state.conflict_resolution, services)
            else:
                st.info("No files to transfer - all files are up to date")
                if st.button("🔙 Back", use_container_width=True):
                    update_dialog_state(dialog_key, phase="config")
                    st.rerun()

def render_executing_phase(dialog_key: str, state: DialogState):
    """Render the executing phase UI"""
    
    st.markdown("**Executing Transfer...**")
    
    with st.spinner("Transfer in progress..."):
        # This will show until execution completes and state changes
        st.info("Please wait while the transfer completes...")

def render_complete_phase(dialog_key: str, state: DialogState):
    """Render the completion phase UI"""
    
    if state.execution_result:
        result = state.execution_result
        if result.get('success'):
            summary = result.get('summary', {})
            st.success(
                f"✅ Transfer complete! "
                f"{summary.get('successful', 0)}/{summary.get('total_files', 0)} files, "
                f"{format_size(summary.get('total_bytes', 0))}"
            )
        else:
            st.error(f"Transfer failed: {result.get('error', 'Unknown error')}")
    else:
        st.error("No execution result available")
    
    if st.button("✅ Close", type="primary", use_container_width=True):
        close_dialog(dialog_key)
        st.rerun()

# ============================================================================
# ACTION EXECUTION - Execute operations and update state
# ============================================================================

def execute_preview_action(dialog_key: str, item, operation_type: str, selection_criteria: Dict, conflict_resolution: str, services):
    """Execute preview action - UPDATE STATE ONLY"""
    
    try:
        # Create job
        job = services.operations_orchestration.create_transfer_job(
            coordinate=item.coord,
            operation_type=operation_type,
            selection_criteria=selection_criteria,
            conflict_resolution=conflict_resolution
        )
        
        # Execute dry run
        result = services.operations_orchestration.execute_dry_run(job)
        
        if result.success:
            # Update state to preview phase
            update_dialog_state(
                dialog_key,
                phase="preview",
                selection_criteria=selection_criteria,
                conflict_resolution=conflict_resolution,
                preview_result=result.result,
                error_message=None
            )
        else:
            # Update state with error
            update_dialog_state(
                dialog_key,
                phase="preview",
                error_message=result.error
            )
    
    except Exception as e:
        update_dialog_state(
            dialog_key,
            phase="preview",
            error_message=str(e)
        )
    
    # Let Streamlit rerun and show new UI
    st.rerun()
def execute_transfer_action(dialog_key: str, item, operation_type: str, selection_criteria: Dict, conflict_resolution: str, services):
    """Execute transfer action - FIXED VERSION"""
    
    try:
        # Execute the entire operation first
        job = services.operations_orchestration.create_transfer_job(
            coordinate=item.coord,
            operation_type=operation_type,
            selection_criteria=selection_criteria,
            conflict_resolution=conflict_resolution
        )
        
        # Execute actual transfer with user feedback
        with st.spinner("Executing transfer..."):
            result = services.operations_orchestration.execute_transfer(job)
        
        # Update state with results
        execution_result = {
            'success': result.success,
            'summary': result.result.get('summary') if result.result else {},
            'error': result.error
        }
        
        update_dialog_state(
            dialog_key,
            phase="complete",
            execution_result=execution_result
        )
    
    except Exception as e:
        update_dialog_state(
            dialog_key,
            phase="complete",
            execution_result={'success': False, 'error': str(e)}
        )
    
    # ONLY rerun after everything is done
    st.rerun()
# ============================================================================
# DIALOG MANAGEMENT
# ============================================================================

def render_operation_dialogs():
    """Render any open operation dialogs"""
    
    # Check for single operation dialogs
    for key in list(st.session_state.keys()):
        if key.endswith('_config') and key.startswith('dialog_'):
            config = st.session_state[key]
            if config.get('show', False):
                render_single_operation_dialog(key.replace('_config', ''), config)
        
        elif key.endswith('_config') and key.startswith('bulk_dialog_'):
            config = st.session_state[key]
            if config.get('show', False):
                render_bulk_operation_dialog(key.replace('_config', ''), config)

def close_dialog(dialog_key: str):
    """Close dialog and clean up state"""
    # Remove config
    config_key = f"{dialog_key}_config"
    if config_key in st.session_state:
        del st.session_state[config_key]
    
    # Remove state
    state_key = f"{dialog_key}_state"
    if state_key in st.session_state:
        del st.session_state[state_key]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def render_selection_criteria(dialog_data, dialog_key: str) -> Dict:
    """Render selection criteria controls"""
    
    if dialog_data.operation_type == "raw_download":
        # Raw download: all or specific bags
        selection_mode = st.radio(
            "Select bags to download:",
            ["All bags", "Specific bags"],
            key=f"{dialog_key}_raw_mode"
        )
        
        if selection_mode == "All bags":
            bags = dialog_data.available_options.get("bags", [])
            if bags:
                total_size = sum(bag["size"] for bag in bags)
                st.info(f"📦 {len(bags)} bags, {format_size(total_size)}")
            return {"all": True}
        else:
            bags = dialog_data.available_options.get("bags", [])
            if bags:
                selected_indices = st.multiselect(
                    "Select bags:",
                    options=list(range(len(bags))),
                    format_func=lambda i: f"[{i}] {bags[i]['name']} ({format_size(bags[i]['size'])})",
                    key=f"{dialog_key}_raw_bags"
                )
                
                if selected_indices:
                    selected_size = sum(bags[i]["size"] for i in selected_indices)
                    st.info(f"📦 {len(selected_indices)} bags selected, {format_size(selected_size)}")
                    return {"bag_indices": selected_indices}
                else:
                    return {}
    
    elif dialog_data.operation_type in ["ml_download", "ml_upload"]:
        # ML operations: file types and bags
        file_types = st.multiselect(
            "File types:",
            options=dialog_data.available_options.get("file_types", []),
            default=["frames", "labels"],
            key=f"{dialog_key}_file_types"
        )
        
        selection_mode = st.radio(
            "Select bags:",
            ["All bags", "Specific bags"],
            key=f"{dialog_key}_ml_mode"
        )
        
        if selection_mode == "All bags":
            bags = dialog_data.available_options.get("bags", [])
            if bags:
                total_frames = sum(bag["frame_count"] for bag in bags)
                total_labels = sum(bag["label_count"] for bag in bags)
                st.info(f"📂 {len(bags)} bags, {total_frames} frames, {total_labels} labels")
            return {"all": True, "file_types": file_types}
        else:
            bags = dialog_data.available_options.get("bags", [])
            if bags:
                bag_names = [bag["name"] for bag in bags]
                selected_names = st.multiselect(
                    "Select bags:",
                    options=bag_names,
                    format_func=lambda name: next(
                        f"{bag['name']} ({bag['frame_count']} frames, {bag['label_count']} labels)"
                        for bag in bags if bag["name"] == name
                    ),
                    key=f"{dialog_key}_ml_bags"
                )
                
                if selected_names:
                    selected_bags = [bag for bag in bags if bag["name"] in selected_names]
                    total_frames = sum(bag["frame_count"] for bag in selected_bags)
                    total_labels = sum(bag["label_count"] for bag in selected_bags)
                    st.info(f"📂 {len(selected_names)} bags, {total_frames} frames, {total_labels} labels")
                    return {"bag_names": selected_names, "file_types": file_types}
                else:
                    return {"file_types": file_types} if file_types else {}
    
    return {}

def validate_selection(selection_criteria: Dict) -> bool:
    """Validate selection criteria"""
    if not selection_criteria:
        return False
    return (
        selection_criteria.get("all") or 
        selection_criteria.get("bag_indices") or 
        selection_criteria.get("bag_names")
    )

def get_operation_title(operation_type: str) -> str:
    """Get human-readable title for operation type"""
    titles = {
        "raw_download": "Download Raw Bags",
        "ml_download": "Download ML Samples", 
        "ml_upload": "Upload ML Samples"
    }
    return titles.get(operation_type, operation_type)

def format_timestamp_simple(timestamp: str) -> str:
    """Simple timestamp formatting"""
    try:
        if 'T' in timestamp:
            date_part, time_part = timestamp.split('T')
            return f"{date_part} {time_part[:5]}"
        return timestamp[:16]
    except:
        return timestamp

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

# ============================================================================
# BULK OPERATIONS (Simplified version)
# ============================================================================

def render_bulk_operation_dialog(dialog_key: str, config: Dict):
    """Render bulk operation dialog with same separation principles"""
    
    items = config['items']
    operation_type = config['operation_type']
    services = config['services']
    
    state = get_dialog_state(dialog_key)
    
    @st.dialog(f"Bulk {get_operation_title(operation_type)}", width="large")
    def bulk_modal():
        # Render different UI based on current phase
        if state.phase == "config":
            render_bulk_config_phase(dialog_key, items, operation_type, services, state)
        elif state.phase == "preview":
            render_bulk_preview_phase(dialog_key, items, operation_type, services, state)
        elif state.phase == "executing":
            render_bulk_executing_phase(dialog_key, state)
        elif state.phase == "complete":
            render_bulk_complete_phase(dialog_key, state)
    
    bulk_modal()


def render_bulk_config_phase(dialog_key: str, items, operation_type: str, services, state: DialogState):
    """Render bulk configuration phase"""
    
    st.markdown(f"**Operating on {len(items)} runs**")
    st.info(f"This will analyze and execute {get_operation_title(operation_type).lower()} for all {len(items)} selected runs")
    
    # Simplified selection for bulk
    st.markdown("#### Conflict Resolution")
    conflict_resolution = st.selectbox(
        "How to handle existing files:",
        ["skip", "overwrite"],
        key=f"{dialog_key}_conflict",
        help="This applies to all runs in the batch"
    )
    
    st.divider()
    
    # Action buttons
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔍 Analyze & Preview", type="primary", use_container_width=True, key=f"{dialog_key}_analyze_btn"):
            execute_bulk_preview_action(dialog_key, items, operation_type, conflict_resolution, services)
    
    with col2:
        if st.button("❌ Cancel", use_container_width=True, key=f"{dialog_key}_cancel_btn"):
            close_dialog(dialog_key)
            st.rerun()


def render_bulk_preview_phase(dialog_key: str, items, operation_type: str, services, state: DialogState):
    """Render bulk preview phase - FULL WIDTH"""
    
    # Add padding/margin on top
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Title on the left
    st.markdown("**Bulk Operation Preview**")
    
    if state.error_message:
        st.error(f"Analysis failed: {state.error_message}")
        if st.button("🔙 Back to Configuration", use_container_width=True):
            update_dialog_state(dialog_key, phase="config", error_message=None)
            st.rerun()
        return
    
    if state.preview_result:
        results = state.preview_result
        
        # Calculate totals - TransferPlan is a dataclass, not dict
        total_files = sum(r.get('plan').total_files for r in results if r.get('success') and r.get('plan'))
        total_size = sum(r.get('plan').total_size for r in results if r.get('success') and r.get('plan'))
        successful_runs = sum(1 for r in results if r.get('success'))
        
        # Overall summary metrics - FULL WIDTH
        st.markdown("#### 📊 Overall Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Selected Runs", len(items))
        with col2:
            st.metric("Valid Runs", successful_runs)
        with col3:
            st.metric("Total Files", total_files)
        with col4:
            st.metric("Total Size", format_size(total_size))
        
        st.divider()
        
        # Detailed breakdown
        st.markdown("#### 📋 Run-by-Run Breakdown")
        
        # Show successful operations
        successful_results = [r for r in results if r.get('success')]
        failed_results = [r for r in results if not r.get('success')]
        
        if successful_results:
            with st.expander(f"✅ Valid Operations ({len(successful_results)})", expanded=True):
                for result in successful_results:
                    item = result['item']
                    plan = result['plan']  # TransferPlan dataclass
                    coord = item.coord
                    
                    # Create a compact display for each run
                    col_coord, col_files, col_size, col_details = st.columns([0.3, 0.15, 0.15, 0.4])
                    
                    with col_coord:
                        st.markdown(f"**{coord.lbid}**")
                        st.caption(format_timestamp_simple(coord.timestamp))
                    
                    with col_files:
                        st.markdown(f"📄 {plan.total_files} files")
                    
                    with col_size:
                        st.markdown(f"💾 {format_size(plan.total_size)}")
                    
                    with col_details:
                        # Show what type of data
                        if operation_type == "raw_download":
                            if item.cloud_raw_status and item.cloud_raw_status.exists:
                                st.markdown(f"📦 {item.cloud_raw_status.bag_count} rosbags from cloud")
                        elif operation_type == "ml_download":
                            if item.cloud_ml_status and item.cloud_ml_status.exists:
                                bag_count = len(item.cloud_ml_status.bag_samples) if item.cloud_ml_status.bag_samples else 0
                                st.markdown(f"🎯 {item.cloud_ml_status.total_samples:,} samples from {bag_count} bags")
                        elif operation_type == "ml_upload":
                            if item.local_ml_status and item.local_ml_status.downloaded:
                                bag_count = len(item.local_ml_status.bag_samples) if item.local_ml_status.bag_samples else 0
                                st.markdown(f"📤 {item.local_ml_status.total_samples:,} samples from {bag_count} bags")
                    
                    # Add conflicts info if any
                    if plan.conflicts:
                        st.caption(f"⚠️ {len(plan.conflicts)} conflicts")
                    
                    st.markdown("")  # spacing
        
        # Show failed operations if any
        if failed_results:
            with st.expander(f"❌ Invalid Operations ({len(failed_results)})"):
                for result in failed_results:
                    item = result['item']
                    coord = item.coord
                    error = result.get('error', 'Unknown error')
                    
                    col_coord, col_error = st.columns([0.3, 0.7])
                    with col_coord:
                        st.markdown(f"**{coord.lbid}**")
                        st.caption(format_timestamp_simple(coord.timestamp))
                    
                    with col_error:
                        st.error(f"Error: {error}")
        
        st.divider()
        
        # Execute button (only if we have valid operations)
        if successful_results and total_files > 0:
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔙 Back", use_container_width=True):
                    update_dialog_state(dialog_key, phase="config")
                    st.rerun()
            with col2:
                if st.button(f"✅ Execute Bulk Transfer ({len(successful_results)} runs, {format_size(total_size)})", 
                            type="primary", use_container_width=True):
                    execute_bulk_transfer_action(dialog_key, successful_results, operation_type, state.conflict_resolution, services)
        elif total_files == 0:
            st.info("✅ All files are already up to date - no transfer needed")
            if st.button("🔙 Back", use_container_width=True):
                update_dialog_state(dialog_key, phase="config")
                st.rerun()
        else:
            st.warning("⚠️ No valid operations to execute")
            if st.button("🔙 Back", use_container_width=True):
                update_dialog_state(dialog_key, phase="config")
                st.rerun()


def render_bulk_executing_phase(dialog_key: str, state: DialogState):
    """Render bulk executing phase"""
    st.markdown("**Executing Bulk Transfer...**")
    with st.spinner("Bulk transfer in progress..."):
        st.info("Please wait while all transfers complete...")


def render_bulk_complete_phase(dialog_key: str, state: DialogState):
    """Render bulk completion phase"""
    if state.execution_result:
        results = state.execution_result
        successful = sum(1 for r in results if r.get('success'))
        failed = len(results) - successful
        
        total_files = sum(r.get('summary', {}).get('total_files', 0) for r in results if r.get('success'))
        total_bytes = sum(r.get('summary', {}).get('total_bytes', 0) for r in results if r.get('success'))
        
        if failed == 0:
            st.success(
                f"🎉 Bulk transfer complete! "
                f"{successful} runs successfully processed, "
                f"{total_files:,} files transferred, "
                f"{format_size(total_bytes)} total"
            )
        else:
            st.warning(
                f"⚠️ Bulk transfer completed with issues: "
                f"{successful} succeeded, {failed} failed"
            )
    else:
        st.error("No execution result available")
    
    if st.button("✅ Close", type="primary", use_container_width=True):
        close_dialog(dialog_key)
        st.rerun()


def execute_bulk_preview_action(dialog_key: str, items, operation_type: str, conflict_resolution: str, services):
    """Execute bulk preview action"""
    try:
        # Execute dry runs for each item
        individual_results = []
        
        for item in items:
            # Create job for this item
            job = services.operations_orchestration.create_transfer_job(
                coordinate=item.coord,
                operation_type=operation_type,
                selection_criteria={"all": True},
                conflict_resolution=conflict_resolution
            )
            
            # Execute dry run
            result = services.operations_orchestration.execute_dry_run(job)
            
            if result.success and result.result:
                plan = result.result.get("plan")
                if plan:
                    individual_results.append({
                        'item': item,
                        'plan': plan,
                        'success': True
                    })
                else:
                    individual_results.append({
                        'item': item,
                        'error': 'No plan generated',
                        'success': False
                    })
            else:
                individual_results.append({
                    'item': item,
                    'error': result.error or 'Unknown error',
                    'success': False
                })
        
        # Update state with results
        update_dialog_state(
            dialog_key,
            phase="preview",
            conflict_resolution=conflict_resolution,
            preview_result=individual_results,
            error_message=None
        )
    
    except Exception as e:
        update_dialog_state(
            dialog_key,
            phase="preview",
            error_message=str(e)
        )
    
    st.rerun()

def execute_bulk_transfer_action(dialog_key: str, successful_results, operation_type: str, conflict_resolution: str, services):
    """Execute bulk transfer action - FIXED VERSION"""
    
    try:
        results = []
        
        # Show progress to user
        with st.spinner(f"Executing bulk transfer for {len(successful_results)} runs..."):
            for result_data in successful_results:
                item = result_data['item']
                
                # Create and execute job
                job = services.operations_orchestration.create_transfer_job(
                    coordinate=item.coord,
                    operation_type=operation_type,
                    selection_criteria={"all": True},
                    conflict_resolution=conflict_resolution
                )
                
                result = services.operations_orchestration.execute_transfer(job)
                
                execution_result = {
                    'success': result.success,
                    'summary': result.result.get('summary') if result.result else {},
                    'error': result.error
                }
                results.append(execution_result)
        
        # Update state with results
        update_dialog_state(
            dialog_key,
            phase="complete",
            execution_result=results
        )
    
    except Exception as e:
        update_dialog_state(
            dialog_key,
            phase="complete",
            execution_result=[{'success': False, 'error': str(e)}]
        )
    
    # ONLY rerun after everything is done
    st.rerun()