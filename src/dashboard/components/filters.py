# src/dashboard/components/filters.py
"""
Hierarchical filtering UI components
"""

import streamlit as st
from typing import Dict, List, Optional

from dashboard.utils.session_state import get_filters, update_filter, clear_all_filters

class HierarchicalFilters:
    """Reusable hierarchical filtering component with cascading dependencies"""
    
    @staticmethod
    def render(data_service) -> Dict[str, Optional[str]]:
        """
        Render cascading hierarchical filters in compact layout
        
        Args:
            data_service: DataService instance
            
        Returns:
            Dictionary of current filter selections
        """
        # Get hierarchy data for filter options
        hierarchy = data_service.get_hierarchy_for_filters()
        
        if not hierarchy:
            st.error("No data available for filters")
            return {}
        
        # Current filters
        current_filters = get_filters()
        
        # Compact two-row layout
        row1_col1, row1_col2, row1_col3 = st.columns([1, 1, 1])
        row2_col1, row2_col2, row2_col3 = st.columns([1, 1, 1])
        
        # Row 1: Client, Region, Field
        with row1_col1:
            clients = list(hierarchy.keys()) if hierarchy else []
            client_options = ["All"] + sorted(clients)
            
            current_client_idx = 0
            if current_filters.get('client') in clients:
                current_client_idx = client_options.index(current_filters['client'])
            
            selected_client = st.selectbox(
                "Client",
                client_options,
                index=current_client_idx,
                key="filter_client"
            )
            
            client_value = None if selected_client == "All" else selected_client
            if client_value != current_filters.get('client'):
                update_filter('client', client_value)
                st.rerun()
        
        with row1_col2:
            if client_value and client_value in hierarchy:
                regions = list(hierarchy[client_value].keys())
                region_options = ["All"] + sorted(regions)
                
                current_region_idx = 0
                if current_filters.get('region') in regions:
                    current_region_idx = region_options.index(current_filters['region'])
                
                selected_region = st.selectbox(
                    "Region",
                    region_options,
                    index=current_region_idx,
                    key="filter_region"
                )
                
                region_value = None if selected_region == "All" else selected_region
                if region_value != current_filters.get('region'):
                    update_filter('region', region_value)
                    st.rerun()
            else:
                st.selectbox("Region", ["All"], disabled=True, key="filter_region_disabled")
                region_value = None
        
        with row1_col3:
            if (client_value and region_value and 
                client_value in hierarchy and region_value in hierarchy[client_value]):
                
                fields = list(hierarchy[client_value][region_value].keys())
                field_options = ["All"] + sorted(fields)
                
                current_field_idx = 0
                if current_filters.get('field') in fields:
                    current_field_idx = field_options.index(current_filters['field'])
                
                selected_field = st.selectbox(
                    "Field",
                    field_options,
                    index=current_field_idx,
                    key="filter_field"
                )
                
                field_value = None if selected_field == "All" else selected_field
                if field_value != current_filters.get('field'):
                    update_filter('field', field_value)
                    st.rerun()
            else:
                st.selectbox("Field", ["All"], disabled=True, key="filter_field_disabled")
                field_value = None
        
        # Row 2: TW, Laser Box, Controls
        with row2_col1:
            if (client_value and region_value and field_value and
                client_value in hierarchy and 
                region_value in hierarchy[client_value] and
                field_value in hierarchy[client_value][region_value]):
                
                tws = list(hierarchy[client_value][region_value][field_value].keys())
                tw_options = ["All"] + sorted(tws)
                
                current_tw_idx = 0
                if current_filters.get('tw') in tws:
                    current_tw_idx = tw_options.index(current_filters['tw'])
                
                selected_tw = st.selectbox(
                    "TW",  # Changed from "Time Window" to "TW"
                    tw_options,
                    index=current_tw_idx,
                    key="filter_tw"
                )
                
                tw_value = None if selected_tw == "All" else selected_tw
                if tw_value != current_filters.get('tw'):
                    update_filter('tw', tw_value)
                    st.rerun()
            else:
                st.selectbox("TW", ["All"], disabled=True, key="filter_tw_disabled")
                tw_value = None
        
        with row2_col2:
            if (client_value and region_value and field_value and tw_value and
                client_value in hierarchy and 
                region_value in hierarchy[client_value] and
                field_value in hierarchy[client_value][region_value] and
                tw_value in hierarchy[client_value][region_value][field_value]):
                
                lbs = list(hierarchy[client_value][region_value][field_value][tw_value].keys())
                lb_options = ["All"] + sorted(lbs)
                
                current_lb_idx = 0
                if current_filters.get('lb') in lbs:
                    current_lb_idx = lb_options.index(current_filters['lb'])
                
                selected_lb = st.selectbox(
                    "Laser Box",
                    lb_options,
                    index=current_lb_idx,
                    key="filter_lb"
                )
                
                lb_value = None if selected_lb == "All" else selected_lb
                if lb_value != current_filters.get('lb'):
                    update_filter('lb', lb_value)
                    st.rerun()
            else:
                st.selectbox("Laser Box", ["All"], disabled=True, key="filter_lb_disabled")
                lb_value = None
        
        with row2_col3:
            # Controls
            if st.button("Clear All", use_container_width=True):
                clear_all_filters()
                st.rerun()
        
        # Build final filter dictionary
        final_filters = {
            'client': client_value,
            'region': region_value,
            'field': field_value,
            'tw': tw_value,
            'lb': lb_value,
            'hierarchy_data': hierarchy
        }
        
        return final_filters
    
    @staticmethod
    def render_sidebar(data_service) -> Dict[str, Optional[str]]:
        """
        Render hierarchical filters in sidebar (5x1 configuration)
        
        Args:
            data_service: DataService instance
            
        Returns:
            Dictionary of current filter selections
        """
        # Get hierarchy data for filter options
        hierarchy = data_service.get_hierarchy_for_filters()
        
        if not hierarchy:
            st.sidebar.error("No data available for filters")
            return {}
        
        # Current filters
        current_filters = get_filters()
        
        # 1. Client Filter
        clients = list(hierarchy.keys()) if hierarchy else []
        client_options = ["All"] + sorted(clients)
        
        current_client_idx = 0
        if current_filters.get('client') in clients:
            current_client_idx = client_options.index(current_filters['client'])
        
        selected_client = st.sidebar.selectbox(
            "Client",
            client_options,
            index=current_client_idx,
            key="sidebar_filter_client"
        )
        
        client_value = None if selected_client == "All" else selected_client
        if client_value != current_filters.get('client'):
            update_filter('client', client_value)
            st.rerun()
        
        # 2. Region Filter
        if client_value and client_value in hierarchy:
            regions = list(hierarchy[client_value].keys())
            region_options = ["All"] + sorted(regions)
            
            current_region_idx = 0
            if current_filters.get('region') in regions:
                current_region_idx = region_options.index(current_filters['region'])
            
            selected_region = st.sidebar.selectbox(
                "Region",
                region_options,
                index=current_region_idx,
                key="sidebar_filter_region"
            )
            
            region_value = None if selected_region == "All" else selected_region
            if region_value != current_filters.get('region'):
                update_filter('region', region_value)
                st.rerun()
        else:
            st.sidebar.selectbox("Region", ["All"], disabled=True, key="sidebar_filter_region_disabled")
            region_value = None
        
        # 3. Field Filter
        if (client_value and region_value and 
            client_value in hierarchy and region_value in hierarchy[client_value]):
            
            fields = list(hierarchy[client_value][region_value].keys())
            field_options = ["All"] + sorted(fields)
            
            current_field_idx = 0
            if current_filters.get('field') in fields:
                current_field_idx = field_options.index(current_filters['field'])
            
            selected_field = st.sidebar.selectbox(
                "Field",
                field_options,
                index=current_field_idx,
                key="sidebar_filter_field"
            )
            
            field_value = None if selected_field == "All" else selected_field
            if field_value != current_filters.get('field'):
                update_filter('field', field_value)
                st.rerun()
        else:
            st.sidebar.selectbox("Field", ["All"], disabled=True, key="sidebar_filter_field_disabled")
            field_value = None
        
        # 4. TW Filter
        if (client_value and region_value and field_value and
            client_value in hierarchy and 
            region_value in hierarchy[client_value] and
            field_value in hierarchy[client_value][region_value]):
            
            tws = list(hierarchy[client_value][region_value][field_value].keys())
            tw_options = ["All"] + sorted(tws)
            
            current_tw_idx = 0
            if current_filters.get('tw') in tws:
                current_tw_idx = tw_options.index(current_filters['tw'])
            
            selected_tw = st.sidebar.selectbox(
                "TW",
                tw_options,
                index=current_tw_idx,
                key="sidebar_filter_tw"
            )
            
            tw_value = None if selected_tw == "All" else selected_tw
            if tw_value != current_filters.get('tw'):
                update_filter('tw', tw_value)
                st.rerun()
        else:
            st.sidebar.selectbox("TW", ["All"], disabled=True, key="sidebar_filter_tw_disabled")
            tw_value = None
        
        # 5. Laser Box Filter
        if (client_value and region_value and field_value and tw_value and
            client_value in hierarchy and 
            region_value in hierarchy[client_value] and
            field_value in hierarchy[client_value][region_value] and
            tw_value in hierarchy[client_value][region_value][field_value]):
            
            lbs = list(hierarchy[client_value][region_value][field_value][tw_value].keys())
            lb_options = ["All"] + sorted(lbs)
            
            current_lb_idx = 0
            if current_filters.get('lb') in lbs:
                current_lb_idx = lb_options.index(current_filters['lb'])
            
            selected_lb = st.sidebar.selectbox(
                "Laser Box",
                lb_options,
                index=current_lb_idx,
                key="sidebar_filter_lb"
            )
            
            lb_value = None if selected_lb == "All" else selected_lb
            if lb_value != current_filters.get('lb'):
                update_filter('lb', lb_value)
                st.rerun()
        else:
            st.sidebar.selectbox("Laser Box", ["All"], disabled=True, key="sidebar_filter_lb_disabled")
            lb_value = None
        
        # 6. Clear All Button
        if st.sidebar.button("Clear All Filters", use_container_width=True):
            clear_all_filters()
            st.rerun()
        
        # Build final filter dictionary
        final_filters = {
            'client': client_value,
            'region': region_value,
            'field': field_value,
            'tw': tw_value,
            'lb': lb_value,
            'hierarchy_data': hierarchy
        }
        
        return final_filters

    @staticmethod
    def get_available_timestamps(filters: Dict) -> List[str]:
        """Extract available timestamps from current filter selection"""
        hierarchy = filters.get('hierarchy_data', {})
        
        if not all([filters.get('client'), filters.get('region'), 
                   filters.get('field'), filters.get('tw'), filters.get('lb')]):
            return []
        
        try:
            timestamps = hierarchy[filters['client']][filters['region']][filters['field']][filters['tw']][filters['lb']]
            return sorted(timestamps)
        except KeyError:
            return []