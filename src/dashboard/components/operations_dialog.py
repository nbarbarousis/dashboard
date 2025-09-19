"""
Operations dialog components for configuring and executing transfer operations.
"""

import streamlit as st
from typing import Dict, List, Optional, Callable
import logging
from datetime import datetime

from src.models import InventoryItem, TransferJob, OperationResult
from src.services.pages.operations_orchestration_service import (
    OperationsOrchestrationService, OperationDialogData
)

# Configure logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

@st.fragment
def show_operation_dialog(
    item: InventoryItem,
    operation_type: str,
    orchestration_service: OperationsOrchestrationService,
    dialog_key: str = None,
    on_complete: Callable = None
):
    """
    Show configuration dialog for a transfer operation using fragments.
    """
    if not dialog_key:
        dialog_key = f"dialog_{operation_type}_{item.coord.to_path_str()}"
    
    # Check if dialog should be open FIRST
    if dialog_key in st.session_state and not st.session_state[dialog_key].get('is_open', True):
        logger.info(f"Dialog {dialog_key} is closed, not rendering")
        return
    
    # Get dialog data
    dialog_data = orchestration_service.get_operation_dialog_data(item, operation_type)
    
    # Use a more persistent state key
    state_key = f"persistent_{dialog_key}_state"
    if state_key not in st.session_state:
        st.session_state[state_key] = {
            'phase': 'configure',
            'selection_criteria': dialog_data.default_selection.copy(),
            'conflict_resolution': dialog_data.conflict_options[0],
            'dry_run_result': None,
            'job': None,
            'transfer_result': None
        }
    
    state = st.session_state[state_key]
    
    # DEBUG: Log current phase
    logger.info(f"Fragment render: phase={state['phase']}, state_key={state_key}")
    
    with st.container(border=True):
        st.markdown(f"### Configure {_get_operation_title(operation_type)}")
        st.caption(f"Run: {item.coord.lbid} / {item.coord.timestamp}")
        
        # Show phase for debugging
        st.caption(f"Current phase: {state['phase']}")
        
        if state['phase'] == 'configure':
            logger.info("Rendering configure phase")
            _render_configure_phase(dialog_data, dialog_key, state, orchestration_service, item)
        elif state['phase'] == 'dry_run':
            logger.info("Rendering dry_run phase")
            _render_dry_run_phase(state, orchestration_service, dialog_key, on_complete)
        elif state['phase'] == 'execute':
            logger.info("🔥 RENDERING EXECUTE PHASE!!!")
            _render_execute_phase(state, orchestration_service, dialog_key, on_complete)
        elif state['phase'] == 'complete':
            logger.info("Rendering complete phase")
            _render_complete_phase(state, dialog_key, on_complete)
        else:
            logger.error(f"Unknown phase: {state['phase']}")



def _close_dialog(dialog_key):
    """Close dialog and clean up session state"""
    logger.info(f"Closing dialog: {dialog_key}")
    
    # Remove persistent state
    state_key = f"persistent_{dialog_key}_state"
    if state_key in st.session_state:
        del st.session_state[state_key]
        logger.info(f"Deleted persistent state: {state_key}")
    
    # Set dialog closed flag
    if dialog_key in st.session_state:
        st.session_state[dialog_key]['is_open'] = False
        logger.info(f"Set dialog closed: {dialog_key}")
    else:
        # Create the dialog state just to mark it closed
        st.session_state[dialog_key] = {'is_open': False}
        logger.info(f"Created closed dialog state: {dialog_key}")



def _render_complete_phase(state, dialog_key, on_complete):
    """Render the completion phase"""
    
    result = state['transfer_result']
    
    if result.success:
        if result.result and "summary" in result.result:
            summary = result.result["summary"]
            st.success(
                f"✅ Transfer complete! "
                f"{summary['successful']}/{summary['total_files']} files, "
                f"{_format_size(summary['total_bytes'])}"
            )
        else:
            st.success("✅ Transfer complete!")
    else:
        st.error(f"Transfer failed: {result.error}")
    
    if st.button("✅ Close", key=f"{dialog_key}_close", type="primary", use_container_width=True):
        logger.info("Close button clicked")
        _close_dialog(dialog_key)
        if on_complete:
            logger.info("Calling on_complete callback")
            on_complete()
        # Force a rerun to apply the dialog close
        st.rerun()

def _render_configure_phase(dialog_data, dialog_key, state, orchestration_service, item):
    """Render the configuration phase"""
    
    # Selection criteria section
    st.markdown("#### Selection Criteria")
    selection_criteria = _render_selection_criteria(dialog_data, dialog_key, state)
    state['selection_criteria'] = selection_criteria
    
    # Conflict resolution section
    st.markdown("#### Conflict Resolution")
    conflict_resolution = st.selectbox(
        "How to handle existing files:",
        dialog_data.conflict_options,
        index=dialog_data.conflict_options.index(state['conflict_resolution']),
        key=f"{dialog_key}_conflict",
        help="Skip: Keep existing files | Overwrite: Replace existing files"
    )
    state['conflict_resolution'] = conflict_resolution
    
    st.divider()
    
    # Action buttons
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔍 Dry Run", key=f"{dialog_key}_dryrun", type="primary", use_container_width=True):
            # Validate selection
            if not selection_criteria or (
                "all" not in selection_criteria and 
                "bag_indices" not in selection_criteria and 
                "bag_names" not in selection_criteria
            ):
                st.error("Please select items to transfer")
                return
            
            # Create job and execute dry run
            job = orchestration_service.create_transfer_job(
                coordinate=item.coord,
                operation_type=dialog_data.operation_type,
                selection_criteria=selection_criteria,
                conflict_resolution=conflict_resolution
            )
            
            state['job'] = job
            state['phase'] = 'dry_run'
            st.rerun()
    
    with col2:
        if st.button("❌ Cancel", key=f"{dialog_key}_cancel", use_container_width=True):
            logger.info("Cancel button clicked")
            _close_dialog(dialog_key)
            st.rerun()


def _render_dry_run_phase(state, orchestration_service, dialog_key, on_complete):
    """Render the dry run execution phase"""
    
    logger.info(f"In dry_run_phase, current phase: {state['phase']}")
    
    with st.spinner("Running dry run..."):
        if state['dry_run_result'] is None:
            # Execute dry run
            result = orchestration_service.execute_dry_run(state['job'])
            state['dry_run_result'] = result
            st.rerun()
    
    # Show results
    result = state['dry_run_result']
    
    st.divider()
    st.markdown("### Dry Run Results")
    
    if not result.success:
        st.error(f"Dry run failed: {result.error}")
        if st.button("🔙 Back to Configuration", key=f"{dialog_key}_back"):
            state['phase'] = 'configure'
            state['dry_run_result'] = None
            st.rerun()
        return
    
    plan = result.result.get("plan") if result.result else None
    if not plan:
        st.error("No plan generated")
        return
    
    # Show plan summary
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Files to Transfer", plan.total_files)
    
    with col2:
        st.metric("Total Size", _format_size(plan.total_size))
    
    with col3:
        st.metric("Conflicts", len(plan.conflicts))
    
    # Show file details in expanders
    _show_plan_details(plan)
    
    st.divider()
    
    if plan.total_files > 0:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔙 Back", key=f"{dialog_key}_back_from_dry", use_container_width=True):
                state['phase'] = 'configure'
                state['dry_run_result'] = None
                st.rerun()
        
        with col2:
            if st.button("❌ Cancel", key=f"{dialog_key}_cancel_dry", use_container_width=True):
                _close_dialog(dialog_key)
        
        with col3:
            if st.button(
                f"✅ Execute ({plan.total_files} files)",
                key=f"{dialog_key}_execute",
                type="primary",
                use_container_width=True
            ):
                logger.info("🔥 EXECUTE BUTTON CLICKED!")
                logger.info(f"Setting phase to execute, current phase: {state['phase']}")
                state['phase'] = 'execute'
                logger.info(f"Phase set to: {state['phase']}")
                logger.info("About to call st.rerun()")
                st.rerun()
    else:
        st.info("No files to transfer - all files are up to date")
        if st.button("🔙 Back", key=f"{dialog_key}_back_no_files", use_container_width=True):
            state['phase'] = 'configure'
            state['dry_run_result'] = None
            st.rerun()


def _render_execute_phase(state, orchestration_service, dialog_key, on_complete):
    """Render the actual execution phase"""
    
    logger.info("🔥🔥🔥 EXECUTE PHASE CALLED!!!")
    logger.info(f"State keys: {list(state.keys())}")
    logger.info(f"Job exists: {state.get('job') is not None}")
    
    with st.spinner("Executing transfer..."):
        if state['transfer_result'] is None:
            logger.info("About to execute transfer")
            result = orchestration_service.execute_transfer(state['job'])
            logger.info(f"Transfer completed: success={result.success}")
            state['transfer_result'] = result
            state['phase'] = 'complete'
            st.rerun()


def _render_complete_phase(state, dialog_key, on_complete):
    """Render the completion phase"""
    
    result = state['transfer_result']
    
    if result.success:
        if result.result and "summary" in result.result:
            summary = result.result["summary"]
            st.success(
                f"✅ Transfer complete! "
                f"{summary['successful']}/{summary['total_files']} files, "
                f"{_format_size(summary['total_bytes'])}"
            )
        else:
            st.success("✅ Transfer complete!")
    else:
        st.error(f"Transfer failed: {result.error}")
    
    if st.button("✅ Close", key=f"{dialog_key}_close", type="primary", use_container_width=True):
        _close_dialog(dialog_key)
        if on_complete:
            on_complete()




def _show_plan_details(plan):
    """Show plan details in expanders"""
    if plan.files_to_transfer:
        with st.expander(f"📥 Files to Transfer ({len(plan.files_to_transfer)})"):
            for file_info in plan.files_to_transfer[:10]:
                if "filename" in file_info:
                    st.text(f"• {file_info['filename']} ({_format_size(file_info['size'])})")
                else:
                    name = file_info.get('cloud_name', file_info.get('local_name', 'unknown'))
                    st.text(f"• {name} ({_format_size(file_info['size'])})")
            if len(plan.files_to_transfer) > 10:
                st.text(f"... and {len(plan.files_to_transfer) - 10} more")
    
    if plan.files_to_skip:
        with st.expander(f"⏭️ Files to Skip ({len(plan.files_to_skip)})"):
            for file_info in plan.files_to_skip[:10]:
                reason = file_info.get('reason', 'unknown')
                if "filename" in file_info:
                    st.text(f"• {file_info['filename']} - {reason}")
                else:
                    name = file_info.get('cloud_name', file_info.get('local_name', 'unknown'))
                    st.text(f"• {name} - {reason}")
            if len(plan.files_to_skip) > 10:
                st.text(f"... and {len(plan.files_to_skip) - 10} more")
    
    if plan.conflicts:
        with st.expander(f"⚠️ Conflicts ({len(plan.conflicts)})"):
            for conflict in plan.conflicts[:10]:
                name = conflict.get('filename', conflict.get('name', 'unknown'))
                reason = conflict.get('reason', 'unknown')
                st.text(f"• {name} - {reason}")
            if len(plan.conflicts) > 10:
                st.text(f"... and {len(plan.conflicts) - 10} more")


def _get_operation_title(operation_type: str) -> str:
    """Get human-readable title for operation type."""
    titles = {
        "raw_download": "Download Raw Bags",
        "ml_download": "Download ML Samples",
        "ml_upload": "Upload ML Samples"
    }
    return titles.get(operation_type, operation_type)


def _render_selection_criteria(dialog_data: OperationDialogData, dialog_key: str, state: Dict) -> Dict:
    """Render selection criteria controls based on operation type."""
    selection_criteria = state.get('selection_criteria', {})
    
    if dialog_data.operation_type == "raw_download":
        # Raw download: select all or specific bags by index
        default_mode = "All bags" if selection_criteria.get("all", True) else "Specific bags"
        selection_mode = st.radio(
            "Select bags to download:",
            ["All bags", "Specific bags"],
            index=0 if default_mode == "All bags" else 1,
            key=f"{dialog_key}_raw_mode"
        )
        
        if selection_mode == "All bags":
            selection_criteria = {"all": True}
            
            # Show summary
            bags = dialog_data.available_options.get("bags", [])
            if bags:
                total_size = sum(bag["size"] for bag in bags)
                st.info(f"📦 {len(bags)} bags, {_format_size(total_size)}")
        else:
            # Show bag selector
            bags = dialog_data.available_options.get("bags", [])
            if bags:
                # Restore previous selection if exists
                default_selection = selection_criteria.get("bag_indices", [])
                
                selected_indices = st.multiselect(
                    "Select bags:",
                    options=list(range(len(bags))),
                    default=default_selection,
                    format_func=lambda i: f"[{i}] {bags[i]['name']} ({_format_size(bags[i]['size'])})",
                    key=f"{dialog_key}_raw_bags"
                )
                
                if selected_indices:
                    selection_criteria = {"bag_indices": selected_indices}
                    selected_size = sum(bags[i]["size"] for i in selected_indices)
                    st.info(f"📦 {len(selected_indices)} bags selected, {_format_size(selected_size)}")
                else:
                    st.warning("Please select at least one bag")
                    selection_criteria = {}
    
    elif dialog_data.operation_type in ["ml_download", "ml_upload"]:
        # ML operations: select bags and file types
        
        # File type selection - restore previous selection
        default_file_types = selection_criteria.get("file_types", dialog_data.available_options.get("file_types", []))
        file_types = st.multiselect(
            "File types:",
            options=dialog_data.available_options.get("file_types", []),
            default=default_file_types,
            key=f"{dialog_key}_file_types"
        )
        
        # Bag selection
        default_mode = "All bags" if selection_criteria.get("all", True) else "Specific bags"
        selection_mode = st.radio(
            "Select bags:",
            ["All bags", "Specific bags"],
            index=0 if default_mode == "All bags" else 1,
            key=f"{dialog_key}_ml_mode"
        )
        
        if selection_mode == "All bags":
            selection_criteria = {"all": True, "file_types": file_types}
            
            # Show summary
            bags = dialog_data.available_options.get("bags", [])
            if bags:
                total_frames = sum(bag["frame_count"] for bag in bags)
                total_labels = sum(bag["label_count"] for bag in bags)
                st.info(f"📂 {len(bags)} bags, {total_frames} frames, {total_labels} labels")
        else:
            # Show bag selector
            bags = dialog_data.available_options.get("bags", [])
            if bags:
                bag_names = [bag["name"] for bag in bags]
                default_selection = selection_criteria.get("bag_names", [])
                
                selected_names = st.multiselect(
                    "Select bags:",
                    options=bag_names,
                    default=[n for n in default_selection if n in bag_names],
                    format_func=lambda name: next(
                        f"{bag['name']} ({bag['frame_count']} frames, {bag['label_count']} labels)"
                        for bag in bags if bag["name"] == name
                    ),
                    key=f"{dialog_key}_ml_bags"
                )
                
                if selected_names:
                    selection_criteria = {"bag_names": selected_names, "file_types": file_types}
                    selected_bags = [bag for bag in bags if bag["name"] in selected_names]
                    total_frames = sum(bag["frame_count"] for bag in selected_bags)
                    total_labels = sum(bag["label_count"] for bag in selected_bags)
                    st.info(f"📂 {len(selected_names)} bags, {total_frames} frames, {total_labels} labels")
                else:
                    st.warning("Please select at least one bag")
                    selection_criteria = {"file_types": file_types} if file_types else {}
    
    return selection_criteria


def _format_size(size_bytes: int) -> str:
    """Format bytes as human readable."""
    if size_bytes >= 1024**3:
        return f"{size_bytes / (1024**3):.1f}GB"
    elif size_bytes >= 1024**2:
        return f"{size_bytes / (1024**2):.1f}MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f}KB"
    else:
        return f"{size_bytes}B"


# =============================================================================
# Bulk Operations Dialog
# =============================================================================

@st.fragment
def show_bulk_operation_dialog(
    items: List[InventoryItem],
    operation_type: str,
    orchestration_service: OperationsOrchestrationService,
    dialog_key: str = None,
    on_complete: Callable = None
):
    """
    Show configuration dialog for bulk transfer operations using fragments.
    """
    if not dialog_key:
        dialog_key = f"bulk_dialog_{operation_type}_{len(items)}"
    
    # Initialize dialog state in fragment-local session state
    state_key = f"{dialog_key}_fragment_state"
    if state_key not in st.session_state:
        st.session_state[state_key] = {
            'phase': 'configure',  # 'configure', 'analyze', 'execute', 'complete'
            'selection_criteria': {"all": True},
            'conflict_resolution': "skip",
            'bulk_plan': None,
            'bulk_results': None
        }
    
    state = st.session_state[state_key]
    
    with st.container(border=True):
        st.markdown(f"### Bulk {_get_operation_title(operation_type)}")
        st.caption(f"Operating on {len(items)} runs")
        
        if state['phase'] == 'configure':
            _render_bulk_configure_phase(items, dialog_key, state, orchestration_service, operation_type)
        elif state['phase'] == 'analyze':
            _render_bulk_analyze_phase(items, state, orchestration_service, dialog_key, operation_type)
        elif state['phase'] == 'execute':
            _render_bulk_execute_phase(state, orchestration_service, dialog_key)
        elif state['phase'] == 'complete':
            _render_bulk_complete_phase(state, dialog_key, on_complete)


def _render_bulk_configure_phase(items, dialog_key, state, orchestration_service, operation_type):
    """Render bulk configuration phase"""
    
    # For bulk operations, use simplified selection
    st.markdown("#### Selection Criteria")
    st.info(f"This will apply to all files in each of the {len(items)} selected runs")
    selection_criteria = {"all": True}
    state['selection_criteria'] = selection_criteria
    
    # Conflict resolution
    st.markdown("#### Conflict Resolution")
    conflict_resolution = st.selectbox(
        "How to handle existing files:",
        ["skip", "overwrite"],
        index=0 if state['conflict_resolution'] == "skip" else 1,
        key=f"{dialog_key}_conflict",
        help="This applies to all runs in the batch"
    )
    state['conflict_resolution'] = conflict_resolution
    
    st.divider()
    
    # Action buttons
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔍 Analyze", key=f"{dialog_key}_analyze", type="primary", use_container_width=True):
            state['phase'] = 'analyze'
            st.rerun()
    
    with col2:
        if st.button("❌ Cancel", key=f"{dialog_key}_cancel", use_container_width=True):
            _close_dialog(dialog_key)


def _render_bulk_analyze_phase(items, state, orchestration_service, dialog_key, operation_type):
    """Render bulk analysis phase"""
    
    with st.spinner("Analyzing bulk operation..."):
        if state['bulk_plan'] is None:
            plan = orchestration_service.prepare_bulk_operation(
                items=items,
                operation_type=operation_type,
                selection_criteria=state['selection_criteria'],
                conflict_resolution=state['conflict_resolution']
            )
            state['bulk_plan'] = plan
            st.rerun()
    
    plan = state['bulk_plan']
    
    st.divider()
    st.markdown("### Bulk Operation Plan")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Runs", plan.total_items)
    
    with col2:
        st.metric("Estimated Size", _format_size(plan.total_size))
    
    with col3:
        st.metric("Operation", plan.operation_type)
    
    st.divider()
    
    # Action buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔙 Back", key=f"{dialog_key}_back_from_analyze", use_container_width=True):
            state['phase'] = 'configure'
            state['bulk_plan'] = None
            st.rerun()
    
    with col2:
        if st.button("❌ Cancel", key=f"{dialog_key}_cancel_analyze", use_container_width=True):
            _close_dialog(dialog_key)
    
    with col3:
        if st.button(
            f"✅ Execute ({plan.total_items} runs)",
            key=f"{dialog_key}_execute",
            type="primary",
            use_container_width=True
        ):
            state['phase'] = 'execute'
            st.rerun()


def _render_bulk_execute_phase(state, orchestration_service, dialog_key):
    """Render bulk execution phase"""
    
    plan = state['bulk_plan']
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def update_progress(current, total, message):
        progress = current / total
        progress_bar.progress(progress)
        status_text.text(f"[{current}/{total}] {message}")
    
    if state['bulk_results'] is None:
        # Execute bulk transfer
        with st.spinner("Executing bulk transfer..."):
            results = orchestration_service.execute_bulk_transfer(plan, update_progress)
            state['bulk_results'] = results
            state['phase'] = 'complete'
            st.rerun()


def _render_bulk_complete_phase(state, dialog_key, on_complete):
    """Render bulk completion phase"""
    
    results = state['bulk_results']
    
    # Show summary
    summary = {
        'successful': sum(1 for r in results if r.success),
        'failed': sum(1 for r in results if not r.success),
        'total_files': 0,
        'total_bytes': 0,
        'errors': []
    }
    
    for result in results:
        if result.success and result.result and "summary" in result.result:
            result_summary = result.result["summary"]
            summary['total_files'] += result_summary.get('total_files', 0)
            summary['total_bytes'] += result_summary.get('total_bytes', 0)
        elif not result.success:
            summary['errors'].append(result.error)
    
    if summary["failed"] == 0:
        st.success(
            f"✅ Bulk transfer complete! "
            f"{summary['successful']} runs, "
            f"{summary['total_files']} files, "
            f"{_format_size(summary['total_bytes'])}"
        )
    else:
        st.warning(
            f"⚠️ Bulk transfer completed with issues: "
            f"{summary['successful']} succeeded, {summary['failed']} failed"
        )
        
        if summary["errors"]:
            with st.expander("Show errors"):
                for error in summary["errors"]:
                    st.text(f"• {error}")
    
    if st.button("✅ Close", key=f"{dialog_key}_close", type="primary", use_container_width=True):
        _close_dialog(dialog_key)
        if on_complete:
            on_complete()