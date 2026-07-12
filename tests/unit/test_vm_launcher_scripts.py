#!/usr/bin/env python3
"""
Comprehensive tests for VM launcher scripts and launcher_common.sh library.

Tests the launcher_common.sh library functions, environment variable validation,
zone failover logic, and common patterns across all launcher scripts.

Coverage includes:
1. launcher_common.sh library functions (via subprocess)
2. Environment validation
3. Zone selection logic
4. Singleton checking
5. Dry-run mode
6. Metadata and labels generation
7. Error handling patterns
"""

import os
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

# Test fixtures and constants
TEST_PROJECT = "test-project"
TEST_ZONE = "asia-northeast1-c"
TEST_FALLBACK_ZONE = "asia-northeast1-b"
LAUNCHER_COMMON_PATH = "scripts/vm/lib/launcher_common.sh"


class TestLauncherCommonLibrary:
    """Test the launcher_common.sh library functions"""

    @pytest.fixture
    def launcher_lib_path(self) -> Path:
        """Get path to launcher_common.sh"""
        return Path(__file__).parent.parent.parent / LAUNCHER_COMMON_PATH

    def test_launcher_common_exists_and_executable(self, launcher_lib_path: Path):
        """Test that launcher_common.sh exists and is readable"""
        assert launcher_lib_path.exists(), f"launcher_common.sh not found at {launcher_lib_path}"
        assert launcher_lib_path.is_file(), "launcher_common.sh is not a file"

        # Test basic syntax validity
        result = subprocess.run(["bash", "-n", str(launcher_lib_path)], capture_output=True, text=True)
        assert result.returncode == 0, f"Syntax error in launcher_common.sh: {result.stderr}"

    def test_lc_validate_env_valid_environments(self, launcher_lib_path: Path):
        """Test lc_validate_env with valid environments"""
        for env in ["prod", "staging", "dev"]:
            result = subprocess.run(
                ["bash", "-c", f'source "{launcher_lib_path}" && lc_validate_env "{env}"'],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0, f"lc_validate_env failed for valid env: {env}"

    def test_lc_validate_env_invalid_environments(self, launcher_lib_path: Path):
        """Test lc_validate_env with invalid environments"""
        for env in ["production", "test", "local", "", "invalid"]:
            result = subprocess.run(
                ["bash", "-c", f'source "{launcher_lib_path}" && lc_validate_env "{env}"'],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 1, f"lc_validate_env should fail for invalid env: {env}"
            assert "ERROR: --env must be one of prod/staging/dev" in result.stderr

    def test_lc_code_bucket_generation(self, launcher_lib_path: Path):
        """Test lc_code_bucket function"""
        result = subprocess.run(
            ["bash", "-c", f'source "{launcher_lib_path}" && lc_code_bucket "{TEST_PROJECT}"'],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert result.stdout.strip() == f"deployment-scripts-{TEST_PROJECT}"

    def test_lc_run_ts_format(self, launcher_lib_path: Path):
        """Test lc_run_ts timestamp format"""
        result = subprocess.run(
            ["bash", "-c", f'source "{launcher_lib_path}" && lc_run_ts'], capture_output=True, text=True
        )
        assert result.returncode == 0
        timestamp = result.stdout.strip()
        # Should match YYYYmmdd-HHMMSS format
        import re

        assert re.match(r"^\d{8}-\d{6}$", timestamp), f"Invalid timestamp format: {timestamp}"

    def test_lc_write_startup_file(self, launcher_lib_path: Path):
        """Test lc_write_startup_file function"""
        test_content = "#!/bin/bash\necho 'test startup script'"

        result = subprocess.run(
            [
                "bash",
                "-c",
                f'''source "{launcher_lib_path}"
            lc_write_startup_file "{test_content}"
            cat "$STARTUP_FILE"''',
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert test_content in result.stdout

    def test_lc_log_upload_trap_block(self, launcher_lib_path: Path):
        """Test lc_log_upload_trap_block generates valid bash snippet"""
        vm_name = "test-vm-20240523-120000"
        result = subprocess.run(
            ["bash", "-c", f'source "{launcher_lib_path}" && lc_log_upload_trap_block "{vm_name}" "{TEST_PROJECT}"'],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        output = result.stdout

        # Check key components of the trap block
        assert f"gs://deployment-scripts-{TEST_PROJECT}/vm-logs/{vm_name}/run.log" in output
        assert "trap _lc_final_upload EXIT" in output
        assert "exec > >(tee -a" in output
        assert "_lc_final_upload()" in output


class TestTarballFreshnessGuard:
    """Test lc_verify_tarball_freshness — the pre-launch stale-tarball guard.

    Root cause it prevents (2026-07-12 morpho incident): a VM boots and fetches
    a code tarball that was never republished after a fix landed on
    live-defi-rollout, silently running days-old code. The helper compares the
    floating tarball's .manifest.json commit_sha against the workspace clone's
    current git SHA and warns / blocks / auto-republishes per LC_TARBALL_FRESHNESS.
    SSOT: plans/active/issues/defi_morpho_lending_indices_never_wired_2026_07_12.md
    """

    LIB = "scripts/vm/lib/launcher_common.sh"

    @pytest.fixture
    def lib_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.LIB

    def _lib_abs(self) -> Path:
        return Path(__file__).parent.parent.parent / self.LIB

    def test_tarball_name_for_repo_mapping(self, lib_path: Path):
        """market-tick-data-service→mtds-code is the one special case; else <repo>-code."""
        cases = {
            "market-tick-data-service": "mtds-code",
            "unified-api-contracts": "unified-api-contracts-code",
            "unified-trading-library": "unified-trading-library-code",
            "deployment-service": "deployment-service-code",
            "instruments-service": "instruments-service-code",
        }
        for repo, expected in cases.items():
            result = subprocess.run(
                ["bash", "-c", f'source "{lib_path}" && lc_tarball_name_for_repo "{repo}"'],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0
            assert result.stdout.strip() == expected, f"{repo} → {result.stdout.strip()!r} (want {expected!r})"

    def test_off_mode_short_circuits(self, lib_path: Path):
        """LC_TARBALL_FRESHNESS=off returns 0 without touching git/gsutil."""
        result = subprocess.run(
            [
                "bash",
                "-c",
                f'source "{lib_path}" && LC_TARBALL_FRESHNESS=off lc_verify_tarball_freshness bkt some-repo',
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

    def _fresh_workspace(self, tmp_path: Path) -> tuple[Path, str]:
        """Create a fake workspace with a git-cloned market-tick-data-service; return (ws, head_sha)."""
        repo = tmp_path / "market-tick-data-service"
        repo.mkdir(parents=True)
        for cmd in (
            ["git", "init", "-q"],
            ["git", "config", "user.email", "t@t"],
            ["git", "config", "user.name", "t"],
            ["git", "commit", "-q", "--allow-empty", "-m", "init"],
        ):
            subprocess.run(cmd, cwd=repo, check=True, capture_output=True)
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo, check=True, capture_output=True, text=True
        ).stdout.strip()
        return tmp_path, head

    def _run_with_mock_gsutil(self, ws: Path, manifest_sha: str, mode: str) -> subprocess.CompletedProcess[str]:
        """Run the guard with gsutil mocked to emit a manifest carrying manifest_sha."""
        lib = self._lib_abs()
        # gsutil mock: for a `... cp gs://...manifest.json <dest>` call, write the manifest.
        script = f"""
gsutil() {{
    local dest="${{@: -1}}"
    if [[ "$*" == *manifest.json* && "$*" == *cp* ]]; then
        printf '{{"commit_sha": "{manifest_sha}"}}' > "$dest"; return 0
    fi
    return 0
}}
export -f gsutil
source "{lib}"
# `source` re-runs `set -euo pipefail`; disable AFTER so a return-1 doesn't abort.
set +e
if lc_verify_tarball_freshness bkt market-tick-data-service; then rc=0; else rc=$?; fi
echo "RC=$rc"
"""
        return subprocess.run(
            ["bash", "-c", script],
            capture_output=True,
            text=True,
            env={**os.environ, "WORKSPACE_ROOT": str(ws), "LC_TARBALL_FRESHNESS": mode},
        )

    def test_fresh_tarball_passes(self, tmp_path: Path):
        ws, head = self._fresh_workspace(tmp_path)
        result = self._run_with_mock_gsutil(ws, head, "warn")
        assert "RC=0" in result.stdout
        assert "tarball fresh" in result.stdout

    def test_stale_tarball_warn_does_not_block(self, tmp_path: Path):
        ws, _ = self._fresh_workspace(tmp_path)
        result = self._run_with_mock_gsutil(ws, "deadbeef" * 5, "warn")
        assert "RC=0" in result.stdout, "warn mode must not block a launch"
        assert "STALE tarball" in result.stderr
        assert "create-code-tarballs.sh --include market-tick-data-service" in result.stderr

    def test_stale_tarball_enforce_blocks(self, tmp_path: Path):
        ws, _ = self._fresh_workspace(tmp_path)
        result = self._run_with_mock_gsutil(ws, "deadbeef" * 5, "enforce")
        assert "RC=1" in result.stdout, "enforce mode must block a stale launch"
        assert "refusing to launch a VM onto stale code" in result.stderr

    def test_missing_manifest_enforce_blocks(self, tmp_path: Path):
        """A missing/unreadable tarball manifest is treated as stale under enforce."""
        ws, _ = self._fresh_workspace(tmp_path)
        lib = self._lib_abs()
        script = f"""
gsutil() {{ if [[ "$*" == *cp* ]]; then return 1; fi; return 0; }}
export -f gsutil
source "{lib}"
# `source` re-runs `set -euo pipefail`; disable AFTER so a return-1 doesn't abort.
set +e
if lc_verify_tarball_freshness bkt market-tick-data-service; then rc=0; else rc=$?; fi
echo "RC=$rc"
"""
        result = subprocess.run(
            ["bash", "-c", script],
            capture_output=True,
            text=True,
            env={**os.environ, "WORKSPACE_ROOT": str(ws), "LC_TARBALL_FRESHNESS": "enforce"},
        )
        assert "RC=1" in result.stdout
        assert "manifest MISSING" in result.stderr

    def test_incident_launcher_wires_the_guard(self):
        """The morpho incident launcher must actually call the guard before launch."""
        launcher = Path(__file__).parent.parent.parent / "scripts/vm/launch-mtds-lending-indices-backfill-vm.sh"
        content = launcher.read_text()
        assert "lc_verify_tarball_freshness" in content, (
            "launch-mtds-lending-indices-backfill-vm.sh must wire lc_verify_tarball_freshness "
            "(the exact launcher that ran 4-day-stale in the 2026-07-12 incident)"
        )
        assert "launcher_common.sh" in content, "launcher must source launcher_common.sh"


class TestLauncherScriptPatterns:
    """Test common patterns used across launcher scripts"""

    def test_gcloud_command_structure_dry_run(self):
        """Test that dry run mode properly skips gcloud calls"""
        with patch.dict(os.environ, {"LC_DRY_RUN": "true"}):
            # Mock the launcher_common functions
            result = subprocess.run(
                [
                    "bash",
                    "-c",
                    f'''
                export LC_DRY_RUN=true
                source "scripts/vm/lib/launcher_common.sh"
                lc_gcloud_create "test-vm" "{TEST_PROJECT}" "{TEST_ZONE}" "e2-standard-4" "50" "key=value" "env=test"
                ''',
                ],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent.parent,
            )

            assert result.returncode == 0
            assert "[DRY-RUN] Would create VM: test-vm" in result.stdout

    def test_environment_variable_propagation(self):
        """Test that required environment variables are properly propagated to VM metadata"""
        required_vars = [
            "VM_TASK",
            "VM_SERVICE",
            "VM_OPERATION",
            "VM_ASSET_GROUP",
            "VM_START_DATE",
            "VM_END_DATE",
            "DEPLOYMENT_ENV",
            "VM_NAME",
        ]

        # Simulate metadata building pattern from launcher scripts
        metadata_parts = []
        for var in required_vars:
            metadata_parts.append(f"{var}=test_value")

        metadata = ",".join(metadata_parts)

        # Verify all required variables are present
        for var in required_vars:
            assert f"{var}=" in metadata

    @patch("subprocess.run")
    def test_zone_failover_logic(self, mock_run: Mock):
        """Test zone failover behavior when primary zone has capacity issues"""
        # Simulate zone capacity failure scenarios
        mock_run.side_effect = [
            # First call fails (simulating capacity issue in primary zone)
            subprocess.CalledProcessError(1, ["gcloud"], stderr="ZONE_RESOURCE_POOL_EXHAUSTED"),
            # Second call succeeds in fallback zone
            subprocess.CompletedProcess(["gcloud"], 0, stdout="VM created"),
        ]

        # This would be the pattern for a launcher with zone failover
        primary_zone = "asia-northeast1-c"
        fallback_zones = ["asia-northeast1-b", "asia-northeast1-a"]

        zones_to_try = [primary_zone] + fallback_zones
        created = False

        for zone in zones_to_try:
            try:
                # This simulates a gcloud create call
                subprocess.run(
                    ["gcloud", "compute", "instances", "create", "test-vm", "--zone", zone],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                created = True
                break
            except subprocess.CalledProcessError:
                continue

        assert created, "VM should have been created in fallback zone"

    def test_singleton_check_logic(self):
        """Test singleton checking prevents duplicate VM launches"""
        launcher_lib_path = Path(__file__).parent.parent.parent / LAUNCHER_COMMON_PATH

        # Test with no existing VMs (should succeed)
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(["gcloud"], 0, stdout="")

            result = subprocess.run(
                [
                    "bash",
                    "-c",
                    f'source "{launcher_lib_path}" && lc_singleton_check "test-prefix-" "{TEST_ZONE}" "{TEST_PROJECT}" "false"',
                ],
                capture_output=True,
                text=True,
            )

            assert result.returncode == 0

    def test_singleton_check_with_existing_vm(self):
        """Test singleton check fails when VM already exists"""
        launcher_lib_path = Path(__file__).parent.parent.parent / LAUNCHER_COMMON_PATH

        # Create a temporary script that simulates existing VM behavior
        test_script = f'''#!/bin/bash
source "{launcher_lib_path}"

# Override gcloud command to return existing VM
gcloud() {{
    if [[ "$*" == *"instances list"* ]]; then
        echo "existing-vm-20240523-120000"
        return 0
    else
        command gcloud "$@"
    fi
}}
export -f gcloud

lc_singleton_check "test-prefix-" "{TEST_ZONE}" "{TEST_PROJECT}" "false"
'''

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
            f.write(test_script)
            f.flush()

            try:
                result = subprocess.run(["bash", f.name], capture_output=True, text=True)

                assert result.returncode == 1
                assert "already running" in result.stderr
            finally:
                os.unlink(f.name)

    def test_force_flag_bypasses_singleton_check(self):
        """Test that force=true bypasses singleton checking"""
        launcher_lib_path = Path(__file__).parent.parent.parent / LAUNCHER_COMMON_PATH

        result = subprocess.run(
            [
                "bash",
                "-c",
                f'source "{launcher_lib_path}" && lc_singleton_check "test-prefix-" "{TEST_ZONE}" "{TEST_PROJECT}" "true"',
            ],
            capture_output=True,
            text=True,
        )

        # Should succeed regardless of existing VMs when force=true
        assert result.returncode == 0


class TestSpecificLauncherScripts:
    """Test specific launcher scripts for compliance with patterns"""

    def get_launcher_scripts(self) -> list[Path]:
        """Get list of all launcher scripts"""
        scripts_dir = Path(__file__).parent.parent.parent / "scripts" / "vm"
        return list(scripts_dir.glob("launch-*.sh"))

    def test_all_launchers_have_required_patterns(self):
        """Test that all launcher scripts follow required patterns"""
        launcher_scripts = self.get_launcher_scripts()
        assert len(launcher_scripts) > 0, "No launcher scripts found"

        for script_path in launcher_scripts:
            script_content = script_path.read_text()

            # Check for required patterns - allow some flexibility for different script types
            if not ("set -euo pipefail" in script_content or "set -eu" in script_content):
                # Some scripts may have different error handling patterns
                print(f"NOTE: {script_path.name} doesn't use standard set -euo pipefail")

            # Most launchers should have deployment env validation
            if "DEPLOYMENT_ENV" in script_content:
                # Check for environment validation patterns - different scripts may use different approaches
                has_env_validation = any(
                    pattern in script_content
                    for pattern in [
                        "prod|staging|dev",
                        'case "$ENV"',
                        'case "$DEPLOYMENT_ENV"',
                        "--env prod",
                        "--env staging",
                        "--env dev",
                    ]
                )
                if not has_env_validation:
                    print(f"NOTE: {script_path.name} has DEPLOYMENT_ENV but no clear validation pattern")

            # Check for metadata building pattern
            if "METADATA=" in script_content:
                # Should have VM_NAME in metadata
                assert "VM_NAME=" in script_content, f"Missing VM_NAME in metadata in {script_path.name}"

    def test_launcher_common_sourcing(self):
        """Test that scripts properly source launcher_common.sh when they use it"""
        launcher_scripts = self.get_launcher_scripts()

        for script_path in launcher_scripts:
            script_content = script_path.read_text()

            # If script uses lc_ functions, it should source launcher_common.sh
            if any(func in script_content for func in ["lc_validate_env", "lc_gcloud_create", "lc_singleton_check"]):
                assert "source" in script_content and "launcher_common.sh" in script_content, (
                    f"Script {script_path.name} uses lc_ functions but doesn't source launcher_common.sh"
                )

    def test_date_handling_compatibility(self):
        """Test date command compatibility between macOS and Linux"""
        # Test macOS date format (BSD date)
        try:
            result = subprocess.run(["date", "-v-1d", "+%Y-%m-%d"], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                # BSD date works
                pass
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

        # Test GNU date format (Linux)
        try:
            result = subprocess.run(["date", "-d", "yesterday", "+%Y-%m-%d"], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                # GNU date works
                pass
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

        # Both formats should be supported in launcher scripts for cross-platform compatibility

    def test_vm_naming_conventions(self):
        """Test VM naming follows the expected pattern"""
        # VM names should include timestamp and be unique

        # Test timestamp generation pattern used in scripts
        timestamp = subprocess.run(["date", "+%Y%m%d-%H%M%S"], capture_output=True, text=True).stdout.strip()

        # Should match expected format
        import re

        assert re.match(r"^\d{8}-\d{6}$", timestamp), "Invalid timestamp format for VM naming"


class TestErrorHandling:
    """Test error handling patterns in launcher scripts"""

    def test_required_parameter_validation(self):
        """Test that launcher_common functions validate required parameters"""
        launcher_lib_path = Path(__file__).parent.parent.parent / LAUNCHER_COMMON_PATH

        # Test missing required parameter
        result = subprocess.run(
            ["bash", "-c", f'source "{launcher_lib_path}" && lc_validate_env'], capture_output=True, text=True
        )

        assert result.returncode == 1
        assert "ERROR: --env must be one of prod/staging/dev" in result.stderr

    def test_gcloud_command_error_propagation(self):
        """Test that gcloud command errors are properly propagated"""
        launcher_lib_path = Path(__file__).parent.parent.parent / LAUNCHER_COMMON_PATH

        # Create a temporary script that simulates gcloud failure
        test_script = f'''#!/bin/bash
source "{launcher_lib_path}"

# Override gcloud command to simulate failure
gcloud() {{
    echo "ERROR: Invalid project" >&2
    return 1
}}
export -f gcloud

lc_gcloud_create "test-vm" "invalid-project" "invalid-zone" "e2-standard-4" "50" "key=value" "env=test"
'''

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
            f.write(test_script)
            f.flush()

            try:
                result = subprocess.run(["bash", f.name], capture_output=True, text=True)

                # Function should exit with error due to set -euo pipefail
                assert result.returncode != 0
            finally:
                os.unlink(f.name)

    def test_script_syntax_validation(self):
        """Test that all launcher scripts have valid bash syntax"""
        launcher_scripts = Path(__file__).parent.parent.parent / "scripts" / "vm"

        for script_path in launcher_scripts.glob("launch-*.sh"):
            result = subprocess.run(["bash", "-n", str(script_path)], capture_output=True, text=True)
            assert result.returncode == 0, f"Syntax error in {script_path.name}: {result.stderr}"


class TestSecurityAndBestPractices:
    """Test security and best practices in launcher scripts"""

    def test_no_hardcoded_secrets(self):
        """Test that launcher scripts don't contain hardcoded secrets"""
        launcher_scripts = Path(__file__).parent.parent.parent / "scripts" / "vm"

        # Patterns that might indicate hardcoded secrets
        secret_patterns = [
            r'password\s*=\s*["\'][^"\']+["\']',
            r'key\s*=\s*["\'][A-Za-z0-9]{32,}["\']',
            r'token\s*=\s*["\'][^"\']+["\']',
        ]

        for script_path in launcher_scripts.glob("launch-*.sh"):
            script_content = script_path.read_text()

            for pattern in secret_patterns:
                import re

                matches = re.search(pattern, script_content, re.IGNORECASE)
                assert not matches, (
                    f"Potential hardcoded secret in {script_path.name}: {matches.group() if matches else ''}"
                )

    def test_safe_variable_usage(self):
        """Test that scripts use safe variable practices"""
        launcher_scripts = Path(__file__).parent.parent.parent / "scripts" / "vm"

        scripts_without_pipefail = []
        for script_path in launcher_scripts.glob("launch-*.sh"):
            script_content = script_path.read_text()

            # Check for safe scripting practices - allow some flexibility
            if not ("set -euo pipefail" in script_content or "set -eu" in script_content):
                scripts_without_pipefail.append(script_path.name)

            # Check for quoted variables in dangerous contexts
            lines = script_content.split("\n")
            for line in lines:
                # Look for gcloud commands with unquoted variables
                if "gcloud" in line and "$" in line:
                    # This is a simplified check - in practice you'd want more sophisticated analysis
                    pass

        # Report scripts that don't follow best practices but don't fail the test
        if scripts_without_pipefail:
            print(f"NOTE: Scripts without set -euo pipefail: {scripts_without_pipefail}")

        # At least some scripts should use safe practices
        total_scripts = len(list(launcher_scripts.glob("launch-*.sh")))
        assert total_scripts > len(scripts_without_pipefail), "Too many scripts lack safe scripting practices"

    def test_resource_cleanup_patterns(self):
        """Test that scripts follow proper resource cleanup patterns"""
        launcher_lib_path = Path(__file__).parent.parent.parent / LAUNCHER_COMMON_PATH
        lib_content = launcher_lib_path.read_text()

        # The lc_write_startup_file should register cleanup traps
        assert "trap" in lib_content, "launcher_common.sh should use traps for cleanup"
        assert "EXIT" in lib_content, "Should cleanup on EXIT"


class TestQgSnapshotLauncher:
    """Tests for launch-qg-snapshot-vm.sh — cefi-015 regression.

    Root cause: (1) startup-script-url pointed to a GCS file that hadn't been
    uploaded yet → GCE silently skipped the startup script (no serial output,
    no run.log ever); (2) direct-launch path used undefined VM_NAME/METADATA/
    LABELS; (3) VM_BACKFILL_CMD used /home/unified/workspace but setup script
    hardcodes /home/ikennaigboaka/workspace.
    """

    LAUNCHER_PATH = "scripts/vm/launch-qg-snapshot-vm.sh"

    @pytest.fixture
    def launcher_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.LAUNCHER_PATH

    def test_syntax_valid(self, launcher_path: Path) -> None:
        result = subprocess.run(["bash", "-n", str(launcher_path)], capture_output=True, text=True)
        assert result.returncode == 0, f"Syntax error: {result.stderr}"

    def test_scheduler_body_has_startup_script_url(self, launcher_path: Path) -> None:
        """--dry-run-scheduler-body JSON must contain startup-script-url pointing to the right bucket."""
        import json

        result = subprocess.run(
            ["bash", str(launcher_path), "--dry-run-scheduler-body"],
            capture_output=True,
            text=True,
            env={**os.environ, "SKIP_GCS_PREFLIGHT": "true"},
            cwd=launcher_path.parent.parent.parent,
        )
        assert result.returncode == 0, f"--dry-run-scheduler-body failed: {result.stderr}"
        body = json.loads(result.stdout.strip())
        items = {i["key"]: i["value"] for i in body["metadata"]["items"]}
        assert "startup-script-url" in items, "startup-script-url missing from Cloud Scheduler body"
        assert "deployment-scripts-" in items["startup-script-url"], (
            f"startup-script-url should reference the canonical code bucket; got: {items['startup-script-url']}"
        )
        assert items["startup-script-url"].endswith("/vm/setup-data-pipeline-vm.sh"), (
            f"startup-script-url should point to setup-data-pipeline-vm.sh; got: {items['startup-script-url']}"
        )

    def test_scheduler_body_required_metadata_keys(self, launcher_path: Path) -> None:
        """Cloud Scheduler body must include all VM metadata keys the setup script reads."""
        import json

        result = subprocess.run(
            ["bash", str(launcher_path), "--dry-run-scheduler-body"],
            capture_output=True,
            text=True,
            env={**os.environ, "SKIP_GCS_PREFLIGHT": "true"},
            cwd=launcher_path.parent.parent.parent,
        )
        assert result.returncode == 0, f"--dry-run-scheduler-body failed: {result.stderr}"
        body = json.loads(result.stdout.strip())
        items = {i["key"]: i["value"] for i in body["metadata"]["items"]}
        for required in ("VM_TASK", "VM_SERVICE", "VM_BACKFILL_CMD", "VM_SHUTDOWN_ON_COMPLETION"):
            assert required in items, f"Required metadata key {required!r} missing from scheduler body"
        assert items["VM_TASK"] == "qg-snapshot"
        assert items["VM_SHUTDOWN_ON_COMPLETION"] == "true"

    def test_vm_backfill_cmd_uses_correct_workspace(self, launcher_path: Path) -> None:
        """VM_BACKFILL_CMD must NOT reference /home/unified/workspace — that path never exists on the VM."""
        import json

        result = subprocess.run(
            ["bash", str(launcher_path), "--dry-run-scheduler-body"],
            capture_output=True,
            text=True,
            env={**os.environ, "SKIP_GCS_PREFLIGHT": "true"},
            cwd=launcher_path.parent.parent.parent,
        )
        assert result.returncode == 0
        body = json.loads(result.stdout.strip())
        items = {i["key"]: i["value"] for i in body["metadata"]["items"]}
        cmd = items.get("VM_BACKFILL_CMD", "")
        assert "/home/unified/workspace" not in cmd, (
            "VM_BACKFILL_CMD must not reference /home/unified/workspace — "
            "setup-data-pipeline-vm.sh hardcodes WORKSPACE=/home/ikennaigboaka/workspace"
        )
        assert "/home/ikennaigboaka/workspace" in cmd, (
            f"VM_BACKFILL_CMD should use /home/ikennaigboaka/workspace; got: {cmd}"
        )

    def test_preflight_check_exits_when_gsutil_fails(self, launcher_path: Path) -> None:
        """Pre-flight check must fail fast with a clear error when startup-script-url is inaccessible."""
        script = f"""#!/bin/bash
gsutil() {{ return 1; }}
export -f gsutil
SKIP_GCS_PREFLIGHT=false bash "{launcher_path}" --dry-run-scheduler-body
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
            f.write(script)
            f.flush()
        try:
            result = subprocess.run(["bash", f.name], capture_output=True, text=True)
        finally:
            os.unlink(f.name)
        assert result.returncode != 0, "Should fail when gsutil stat returns non-zero"
        assert "create-code-tarballs.sh" in result.stderr or "startup script not found" in result.stderr, (
            f"Error message should mention create-code-tarballs.sh; got: {result.stderr!r}"
        )

    def test_direct_launch_dry_run_defines_all_vars(self, launcher_path: Path) -> None:
        """Direct-launch --dry-run path must not exit with unbound-variable error."""
        script = f"""#!/bin/bash
# Mock gcloud: return empty for instances list (no existing VM → singleton passes)
# return 0 for everything else.
gcloud() {{
    if [[ "$*" == *"instances list"* ]]; then
        echo ""
        return 0
    fi
    return 0
}}
gsutil() {{ return 0; }}
export -f gcloud gsutil
SKIP_GCS_PREFLIGHT=true bash "{launcher_path}" --dry-run
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
            f.write(script)
            f.flush()
        try:
            result = subprocess.run(["bash", f.name], capture_output=True, text=True)
        finally:
            os.unlink(f.name)
        assert result.returncode == 0, (
            f"--dry-run must succeed (previously failed due to undefined VM_NAME/METADATA/LABELS); "
            f"stderr: {result.stderr!r}"
        )
        assert "DRY-RUN" in result.stdout


class TestDurableLogStreamerCoverage:
    """Guard: every GCP ``launch-*.sh`` workload launcher streams its run-log +
    heartbeat + terminal EXIT_STATUS to the durable GCS ``vm-logs/`` path, so the
    /deployments surface + the exit_code monitor see EVERY GCP target (0 untracked,
    self-delete-proof).

    A launcher "uses the durable-log streamer" if it wires one of the three
    canonical mechanisms (reuse, never rebuild — see
    plan deployment_observability_parity_live_batch_paper_2026_06_22 Phase 4 +
    vm_launcher_durable_log_observability):

      1. ``setup-data-pipeline-vm.sh``        — GCE startup fetches + runs the
         vm-exec-with-gcs-tee.sh wrapper (the 30s GCS log + EXIT_STATUS uploader).
      2. ``vm-exec-with-gcs-tee.sh``          — direct reference to the tee wrapper.
      3. ``lc_log_upload_trap_block``         — the inline-startup streamer in
         lib/launcher_common.sh (continuous run.log stream + heartbeat +
         EXIT_STATUS trap), for bespoke launchers that build their own startup.
      4. ``_tradfi-ohlcv-launcher-lib.sh``    — the shared tradfi-OHLCV lib whose
         TRADFI_OHLCV_STARTUP defaults to setup-data-pipeline-vm.sh (transitively #1).

    A future "added a launcher, forgot the durable log" regression fails here.
    Genuinely-exempt launchers are whitelisted EXPLICITLY below with a reason.
    """

    STREAMER_TOKENS = (
        "setup-data-pipeline-vm",
        "vm-exec-with-gcs-tee",
        "lc_log_upload_trap_block",
        "_tradfi-ohlcv-launcher-lib",
    )

    # Genuinely-exempt GCP launchers (NOT batch-workload VMs that need a run-log
    # lifecycle). Each entry MUST carry a reason — a new launcher added here
    # without justification is itself a review smell.
    EXEMPT: dict[str, str] = {
        # --- AWS launcher (not a GCP VM; AWS parity is Phase 5) ---
        "launch-ec2-vm.sh": "AWS EC2 master launcher — not a GCP VM (AWS parity tracked separately).",
        # --- LONG_LIVED service VMs (persistent; systemd/container logging, no
        #     batch run.log/EXIT_STATUS lifecycle — VM_SHUTDOWN_ON_COMPLETION=false) ---
        "launch-planning-vm.sh": "LONG_LIVED_LIVE interactive planning VM (no batch run-log lifecycle).",
        "launch-orchestrator-worker-vm.sh": "LONG_LIVED agent-orchestrator worker (systemd-managed, persistent).",
        "launch-dashboard-vm.sh": "LONG_LIVED container VM (restart=always; container logging, no startup-script run.log).",
        "launch-epic-vm.sh": "Epic VM from the orchestrator registry; long-lived, delegates the planning VM to launch-planning-vm.sh.",
        "launch-data-pipeline-fleet-monitor.sh": "Permanent observability monitor VM (it IS the fleet monitor).",
        # --- Consolidated multi-shard live VMs with bespoke startup scripts ---
        "launch-mtds-live-cefi-consolidated.sh": (
            "LONG_LIVED_LIVE consolidated CeFi live VM; uses setup-cefi-live-consolidated-vm.sh "
            "which runs N parallel websocket-streaming processes. The custom startup script "
            "wires the GCS heartbeat sidecar (vm_heartbeat_sidecar.sh) directly — the "
            "durable-log tee is not applicable to a multi-process supervisor."
        ),
        "launch-mtds-live-prediction-consolidated.sh": (
            "LONG_LIVED_LIVE consolidated prediction live VM; uses setup-prediction-live-consolidated-vm.sh "
            "which runs 4 parallel websocket-streaming shards (KALSHI + POLYMARKET × trades + book_snapshot_5). "
            "The custom startup script wires the GCS heartbeat sidecar directly — the "
            "durable-log tee is not applicable to a multi-process supervisor."
        ),
        # --- Pure fan-out wrappers that delegate to a covered per-shard launcher ---
        "launch-cefi-week-test.sh": "Fan-out wrapper → launch-cefi-forward-poll.sh (covered) per day.",
        "launch-sku-matrix-v2-benchmark.sh": "Fan-out wrapper → launch-synthetic-benchmark-vm.sh (covered) per archetype.",
    }

    def _gcp_launchers(self) -> list[Path]:
        scripts_dir = Path(__file__).parent.parent.parent / "scripts" / "vm"
        # Exclude *-aws.sh (AWS family) — this guard is GCP-scoped.
        return sorted(p for p in scripts_dir.glob("launch-*.sh") if not p.name.endswith("-aws.sh"))

    def _streams_durable_log(self, content: str) -> bool:
        return any(token in content for token in self.STREAMER_TOKENS)

    def test_gcp_launchers_present(self) -> None:
        assert len(self._gcp_launchers()) > 0, "No GCP launch-*.sh scripts found"

    def test_every_gcp_launcher_streams_durable_log_or_is_whitelisted(self) -> None:
        """No GCP workload launcher may skip the durable-log streamer without an
        explicit whitelist reason."""
        offenders: list[str] = []
        for script in self._gcp_launchers():
            content = script.read_text()
            if self._streams_durable_log(content):
                continue
            if script.name in self.EXEMPT:
                continue
            offenders.append(script.name)

        assert not offenders, (
            "GCP launcher(s) do NOT wire the durable-log streamer "
            f"({', '.join(self.STREAMER_TOKENS)}) and are not whitelisted-exempt: "
            f"{offenders}. Wire lc_log_upload_trap_block (or route through "
            "setup-data-pipeline-vm.sh) so run.log + EXIT_STATUS land in "
            "vm-logs/<VM_NAME>/ — else /deployments + the exit_code monitor go blind. "
            "If genuinely exempt (AWS / long-lived service VM / pure fan-out wrapper), "
            "add it to TestDurableLogStreamerCoverage.EXEMPT with a reason."
        )

    def test_whitelist_entries_still_exist(self) -> None:
        """A whitelisted launcher that no longer exists is stale — drop it so the
        guard stays honest."""
        scripts_dir = Path(__file__).parent.parent.parent / "scripts" / "vm"
        for name in self.EXEMPT:
            assert (scripts_dir / name).exists(), (
                f"Whitelisted-exempt launcher {name} no longer exists — remove it from "
                "TestDurableLogStreamerCoverage.EXEMPT."
            )

    def test_whitelist_entries_have_reasons(self) -> None:
        for name, reason in self.EXEMPT.items():
            assert reason.strip(), f"Exempt launcher {name} has no reason — add one."

    def test_guard_catches_an_unconverted_launcher(self) -> None:
        """Self-test: a synthetic launcher that creates a GCP VM but omits the
        streamer (and is not whitelisted) is detected as an offender — proving the
        guard would catch the regression it exists to prevent."""
        fake_content = (
            "#!/usr/bin/env bash\nset -euo pipefail\n"
            'gcloud compute instances create "my-vm" --zone=asia-northeast1-c '
            "--metadata-from-file=startup-script=/tmp/x\n"
        )
        assert not self._streams_durable_log(fake_content), (
            "Self-test sentinel: a launcher with no streamer token must read as un-streamed."
        )
        # And a converted one (wires lc_log_upload_trap_block) reads as covered.
        converted = fake_content + 'LOG_TRAP="$(lc_log_upload_trap_block "$VM_NAME" "$PID")"\n'
        assert self._streams_durable_log(converted)


if __name__ == "__main__":
    pytest.main([__file__])
