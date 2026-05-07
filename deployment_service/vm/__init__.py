"""VM-specific CLI + adapters for the UTL lifecycle daemon.

UTL owns the generic heartbeat plumbing (``unified_trading_library.lifecycle``);
this package owns the VM-specific CLI wrapper and the
``DeploymentsRegistry`` <-> ``HeartbeatStore`` adapter.
"""

from .heartbeat_cli import main

__all__ = ["main"]
