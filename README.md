src/
├── dashboard/
│   ├── __init__.py
│   ├── app.py                    # Main Streamlit application
│   ├── pages/
│   │   ├── __init__.py
│   │   ├── temporal_coverage.py  # Landing page implementation
│   │   ├── per_run_analysis.py   # Analytics integration page
│   │   └── download_manager.py   # Cloud operations page
│   ├── components/
│   │   ├── __init__.py
│   │   ├── filters.py            # Hierarchical filtering UI
│   │   ├── plots.py              # Reusable plot components
│   │   └── metrics.py            # Summary metric cards
│   └── utils/
│       ├── __init__.py
│       ├── session_state.py      # Session state management
│       └── formatting.py         # Data formatting utilities
├── services/
│   ├── __init__.py
│   ├── cloud_discovery.py        # Core cloud data discovery
│   ├── analytics_service.py      # Analytics integration
│   └── cloud_operations.py       # Download/upload operations
├── models/
│   ├── __init__.py
│   ├── hierarchy.py              # Data hierarchy models
│   └── cloud_data.py             # Cloud data structures
├── config/
│   ├── __init__.py
│   └── dashboard_config.py       # Dashboard configuration
└── cli/
    └── dashboard.py              # CLI entry point

# Root level files
requirements.txt                  # Dashboard dependencies
dashboard_main.py                 # Main entry point
README_dashboard.md               # Dashboard documentation