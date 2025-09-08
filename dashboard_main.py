# ---

# dashboard_main.py (root level)
"""
Data Overview Dashboard - Main Entry Point

Run with: streamlit run dashboard_main.py
"""

import sys
import os
from pathlib import Path

# Add src to Python path
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

# Import and run dashboard
from dashboard.app import main

if __name__ == "__main__":
    # Set up environment
    os.environ['STREAMLIT_THEME_BASE'] = 'light'
    
    # Run dashboard
    main()