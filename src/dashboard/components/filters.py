"""
Hierarchical filtering UI components - Updated for new architecture
"""
from typing import Dict, List, Optional
import streamlit as st

from src.services import DataCoordinationService
from src.dashboard.utils.session_state import get_filters, update_filter, clear_all_filters


class HierarchicalFilters:
    """
    Reusable hierarchical filtering component with cascading dependencies.
    Now uses DataCoordinationService for infrastructure-level filter operations.
    """
    
    @staticmethod
    def render_sidebar(data_coordination_service: DataCoordinationService) -> Dict[str, Optional[str]]:
        """
        Render hierarchical filters in sidebar using infrastructure service.
        
        Args:
            data_coordination_service: DataCoordinationService instance
            
        Returns:
            Dictionary of current filter selections
        """
        # Current filters from session state
        current_filters = get_filters()
        
        # Build filters progressively
        selected_filters = {}
        
        # 1. Client Filter
        client_options = data_coordination_service.get_filter_options('cid')
        
        if client_options:
            client_idx = 0
            if current_filters.get('cid') in client_options:
                client_idx = client_options.index(current_filters['cid'])
            
            selected_cid = st.sidebar.selectbox(
                "Client",
                client_options,
                index=client_idx,
                key="sidebar_filter_cid"
            )
            
            if selected_cid != current_filters.get('cid'):
                update_filter('cid', selected_cid)
                st.rerun()
            
            selected_filters['cid'] = selected_cid
        else:
            st.sidebar.selectbox("Client", ["No data"], disabled=True)
            return selected_filters
        
        # 2. Region Filter
        if selected_filters['cid']:
            region_options = data_coordination_service.get_filter_options(
                'regionid', 
                parent_filters={'cid': selected_filters['cid']}
            )
            
            if region_options:
                region_idx = 0
                if current_filters.get('regionid') in region_options:
                    region_idx = region_options.index(current_filters['regionid'])
                
                selected_regionid = st.sidebar.selectbox(
                    "Region",
                    region_options,
                    index=region_idx,
                    key="sidebar_filter_regionid"
                )
                
                if selected_regionid != current_filters.get('regionid'):
                    update_filter('regionid', selected_regionid)
                    st.rerun()
                
                selected_filters['regionid'] = selected_regionid
            else:
                st.sidebar.selectbox("Region", ["No data"], disabled=True)
                return selected_filters
        else:
            st.sidebar.selectbox("Region", ["Select client first"], disabled=True)
            return selected_filters
        
        # 3. Field Filter
        if selected_filters['regionid']:
            field_options = data_coordination_service.get_filter_options(
                'fieldid',
                parent_filters={
                    'cid': selected_filters['cid'],
                    'regionid': selected_filters['regionid']
                }
            )
            
            if field_options:
                field_idx = 0
                if current_filters.get('fieldid') in field_options:
                    field_idx = field_options.index(current_filters['fieldid'])
                
                selected_fieldid = st.sidebar.selectbox(
                    "Field",
                    field_options,
                    index=field_idx,
                    key="sidebar_filter_fieldid"
                )
                
                if selected_fieldid != current_filters.get('fieldid'):
                    update_filter('fieldid', selected_fieldid)
                    st.rerun()
                
                selected_filters['fieldid'] = selected_fieldid
            else:
                st.sidebar.selectbox("Field", ["No data"], disabled=True)
                return selected_filters
        else:
            st.sidebar.selectbox("Field", ["Select region first"], disabled=True)
            return selected_filters
        
        # 4. TW Filter
        if selected_filters['fieldid']:
            tw_options = data_coordination_service.get_filter_options(
                'twid',
                parent_filters={
                    'cid': selected_filters['cid'],
                    'regionid': selected_filters['regionid'],
                    'fieldid': selected_filters['fieldid']
                }
            )
            
            if tw_options:
                tw_idx = 0
                if current_filters.get('twid') in tw_options:
                    tw_idx = tw_options.index(current_filters['twid'])
                
                selected_twid = st.sidebar.selectbox(
                    "TW",
                    tw_options,
                    index=tw_idx,
                    key="sidebar_filter_twid"
                )
                
                if selected_twid != current_filters.get('twid'):
                    update_filter('twid', selected_twid)
                    st.rerun()
                
                selected_filters['twid'] = selected_twid
            else:
                st.sidebar.selectbox("TW", ["No data"], disabled=True)
                return selected_filters
        else:
            st.sidebar.selectbox("TW", ["Select field first"], disabled=True)
            return selected_filters
        
        # 5. Laser Box Filter (with "All" option)
        if selected_filters['twid']:
            lb_options = data_coordination_service.get_filter_options(
                'lbid',
                parent_filters={
                    'cid': selected_filters['cid'],
                    'regionid': selected_filters['regionid'],
                    'fieldid': selected_filters['fieldid'],
                    'twid': selected_filters['twid']
                }
            )
            
            if lb_options:
                # Add "All" option for laser box
                lb_display_options = ["All"] + lb_options
                
                lb_idx = 0
                if current_filters.get('lbid') in lb_options:
                    lb_idx = lb_display_options.index(current_filters['lbid'])
                elif current_filters.get('lbid') is None:
                    lb_idx = 0  # Select "All" if None
                
                selected_lb = st.sidebar.selectbox(
                    "Laser Box",
                    lb_display_options,
                    index=lb_idx,
                    key="sidebar_filter_lbid"
                )
                
                # Convert "All" to None
                lb_value = None if selected_lb == "All" else selected_lb
                
                if lb_value != current_filters.get('lbid'):
                    update_filter('lbid', lb_value)
                    st.rerun()
                
                selected_filters['lbid'] = lb_value
            else:
                st.sidebar.selectbox("Laser Box", ["No data"], disabled=True)
                return selected_filters
        else:
            st.sidebar.selectbox("Laser Box", ["Select TW first"], disabled=True)
            return selected_filters
        
        # 6. Clear All Button
        if st.sidebar.button("Clear All Filters", use_container_width=True):
            clear_all_filters()
            st.rerun()
        
        return selected_filters