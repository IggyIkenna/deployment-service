"""Service status display functions for data status commands.

This module serves as the main entry point for data status display functionality.
The implementation has been split into focused modules:

- data_status_display_dynamic.py: Dynamic service status functionality
- data_status_display_fixed.py: Fixed service status functionality
- data_status_scanning.py: Scanning-related helper functions
- data_status_processing.py: Result processing functions

This file maintains backward compatibility by re-exporting the main functions.
"""

# Import main functions from the specialized modules
from .data_status_display_dynamic import display_dynamic_service_status
from .data_status_display_fixed import display_fixed_service_status

# Re-export the main functions for backward compatibility
__all__ = [
    "display_dynamic_service_status",
    "display_fixed_service_status",
]
