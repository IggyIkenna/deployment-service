#!/usr/bin/env python3
"""Thin CLI entry point — delegates to ``deployment_service.vm.heartbeat_cli.main``.

All logic lives in UTL (``unified_trading_library.lifecycle``) + the VM
adapter (``deployment_service.vm.heartbeat_cli``). This shim exists so the
wrapper shell (``vm-exec-with-gcs-tee.sh``) and the GCS-uploaded VM tarball
keep calling the same file path without breakage.
"""

from __future__ import annotations

import sys

from deployment_service.vm.heartbeat_cli import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
