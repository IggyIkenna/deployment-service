# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""
League configuration shim for sports verification scripts.

Re-exports ``LEAGUE_CLASSIFICATION_DATA`` from the canonical source in UAC.
"""

from __future__ import annotations

from unified_api_contracts.sports import LEAGUE_CLASSIFICATION_DATA as LEAGUE_CLASSIFICATION_DATA

__all__ = ["LEAGUE_CLASSIFICATION_DATA"]
