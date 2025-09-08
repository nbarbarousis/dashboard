# src/dashboard/utils/session_state.py
"""
Session state management utilities for the dashboard
"""

import streamlit as st
from typing import Any, Dict, Optional

def initialize_session_state():
    """Initialize session state with default values"""
    
    # Services initialization flag
    if 'services_initialized' not in st.session_state:
        st.session_state.services_initialized = False
    
    # Data initialization flag
    if 'data_initialized' not in st.session_state:
        st.session_state.data_initialized = False
    
    # Current page tracking
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "Temporal Coverage"
    
    # Filter states
    if 'selected_filters' not in st.session_state:
        st.session_state.selected_filters = {
            'client': None,
            'region': None,
            'field': None,
            'tw': None,
            'lb': None
        }

def get_service(service_name: str) -> Optional[Any]:
    """Get a service instance from session state"""
    return st.session_state.get(service_name)

def set_service(service_name: str, service_instance: Any):
    """Store a service instance in session state"""
    st.session_state[service_name] = service_instance

def get_filters() -> Dict[str, Optional[str]]:
    """Get current filter selections"""
    return st.session_state.get('selected_filters', {})

def update_filter(filter_type: str, value: Optional[str]):
    """Update a specific filter and clear dependent filters"""
    filters = st.session_state.get('selected_filters', {})
    
    # Update the specified filter
    filters[filter_type] = value
    
    # Clear dependent filters when parent changes
    filter_hierarchy = ['client', 'region', 'field', 'tw', 'lb']
    
    if filter_type in filter_hierarchy:
        current_index = filter_hierarchy.index(filter_type)
        # Clear all filters after the current one
        for i in range(current_index + 1, len(filter_hierarchy)):
            filters[filter_hierarchy[i]] = None
    
    st.session_state.selected_filters = filters

def clear_all_filters():
    """Clear all filter selections"""
    st.session_state.selected_filters = {
        'client': None,
        'region': None,
        'field': None,
        'tw': None,
        'lb': None
    }