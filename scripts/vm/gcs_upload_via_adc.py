#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Thin CLI entry point — delegates to ``deployment_service.vm.gcs_upload_cli.main``.

Uploads local files to GCS via UTL's ADC-backed StorageClient instead of
shelling out to ``gsutil``/``gcloud storage`` (which resolve credentials from
the CLI's active account, not ADC — broken under an expired WIF token in an
interactive AO slot). See
plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.

Called from ``create-code-tarballs.sh`` in place of bare ``gsutil cp``.
"""

from __future__ import annotations

import sys

from deployment_service.vm.gcs_upload_cli import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
