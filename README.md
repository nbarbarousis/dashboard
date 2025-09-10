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
│   └── utils/
│       ├── __init__.py
│       ├── session_state.py      # Session state management
├── services/
│   ├── __init__.py
│   ├── data_service.py           # Analytics data service
│   ├── gcs_service.py      # Cloud gcs service
├── models/
├── config/
│   ├── __init__.py
│   └── dashboard_config.py       # Dashboard configuration
└── cli/
    └── dashboard.py              # CLI entry point

# Root level files
requirements.txt                  # Dashboard dependencies
dashboard_main.py                 # Main entry point
README.md                         # Dashboard documentation