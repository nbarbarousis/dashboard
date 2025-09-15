"""
Session state management utilities for Streamlit dashboard.

This module provides helper functions for managing Streamlit session state
in a consistent way across the application.
"""

import streamlit as st
from typing import Any, Dict, Optional


def initialize_session_state() -> None:
    """
    Initialize session state with default values.
    
    This should be called once at the start of the app to ensure
    all required session state keys exist.
    """
    # Initialize filter state
    if 'filters' not in st.session_state:
        st.session_state.filters = {
            'cid': None,
            'regionid': None,
            'fieldid': None,
            'twid': None,
            'lbid': None
        }
    
    # Initialize page navigation
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 'Temporal Coverage'
    
    # Initialize service state
    if 'services_initialized' not in st.session_state:
        st.session_state.services_initialized = False
    
    # Initialize global filters (from sidebar)
    if 'global_filters' not in st.session_state:
        st.session_state.global_filters = {}


def get_filters() -> Dict[str, Optional[str]]:
    """
    Get current filter selections.
    
    Returns:
        Dictionary of current filter values
    """
    if 'filters' not in st.session_state:
        initialize_session_state()
    
    return st.session_state.filters.copy()


def update_filter(level: str, value: Optional[str]) -> None:
    """
    Update a specific filter level.
    
    Args:
        level: Filter level to update ('cid', 'regionid', etc.)
        value: New value for the filter (None to clear)
    """
    if 'filters' not in st.session_state:
        initialize_session_state()
    
    st.session_state.filters[level] = value


def clear_all_filters() -> None:
    """Clear all filter selections."""
    st.session_state.filters = {
        'cid': None,
        'regionid': None,
        'fieldid': None,
        'twid': None,
        'lbid': None
    }


def get_service(service_name: str) -> Optional[Any]:
    """
    Get a service from session state.
    
    Args:
        service_name: Name of the service to retrieve
        
    Returns:
        Service instance or None if not found
    """
    return st.session_state.get(service_name)


def set_service(service_name: str, service_instance: Any) -> None:
    """
    Store a service in session state.
    
    Args:
        service_name: Name to store the service under
        service_instance: Service instance to store
    """
    st.session_state[service_name] = service_instance