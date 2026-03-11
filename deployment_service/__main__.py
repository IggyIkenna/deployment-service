"""Entry point for python -m deployment_service.cli

Suppresses third-party deprecation warnings from openbb_*, pydantic, etc.
"""

import warnings

# Suppress ALL warnings before any imports
warnings.filterwarnings("ignore")

# Now safe to import
from deployment_service.cli import cli

if __name__ == "__main__":
    cli()
