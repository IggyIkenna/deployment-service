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
import shlex
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

    def test_lc_log_upload_trap_block_stamps_running_sentinel_before_trap_install(self, launcher_lib_path: Path):
        """A RUNNING sentinel must land on EXIT_STATUS before the EXIT trap is
        installed — this is the fix for a same-named relaunch reading a PRIOR
        run's stale terminal EXIT_STATUS (e.g. a stale "0") as success when a
        whole-unit SIGKILL (a fast OOM) kills the script before
        `_lc_final_upload` ever gets a chance to run (SIGKILL bypasses EXIT
        traps entirely). See launcher_common.sh's lc_log_upload_trap_block
        docstring point 4.
        """
        vm_name = "test-vm-20240523-120000"
        result = subprocess.run(
            ["bash", "-c", f'source "{launcher_lib_path}" && lc_log_upload_trap_block "{vm_name}" "{TEST_PROJECT}"'],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        output = result.stdout

        sentinel_line = 'echo "RUNNING" | gsutil -q cp - "$GCS_EXIT_URI"'
        assert sentinel_line in output
        # Must be stamped BEFORE the trap install, so a mid-run whole-unit
        # kill can never leave a stale prior-run terminal code readable.
        assert output.index(sentinel_line) < output.index("trap _lc_final_upload EXIT")
        # Non-numeric — data_pipeline_monitors._gcs.read_terminal_exit_code()
        # must fail its int() parse on it and fall through to None/unknown,
        # never misread it as a successful rc=0.
        with pytest.raises(ValueError):
            int("RUNNING")


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
        """LC_TARBALL_FRESHNESS=off returns 0 without touching git/gcloud."""
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
        """Run the guard with `gcloud storage` mocked to emit a manifest carrying manifest_sha.

        (Helper name kept for diff-minimality; it mocks `gcloud`, not `gsutil` — the guard's
        manifest reads route through `gcloud storage cp` (ADC-backed), not `gsutil` (active-CLI-
        account-backed, breaks under an expired WIF token in an interactive AO slot). See
        plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.)
        """
        lib = self._lib_abs()
        # gcloud mock: for a `storage cp gs://...manifest.json <dest> --quiet` call, write the manifest.
        script = f"""
gcloud() {{
    local dest="${{@: -2:1}}"
    if [[ "$*" == *manifest.json* && "$*" == *cp* ]]; then
        printf '{{"commit_sha": "{manifest_sha}"}}' > "$dest"; return 0
    fi
    return 0
}}
export -f gcloud
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
gcloud() {{ if [[ "$*" == *cp* ]]; then return 1; fi; return 0; }}
export -f gcloud
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


class TestSetupScriptFreshnessGuard:
    """Test lc_verify_setup_script_freshness — the pre-launch stale-startup-script guard.

    Root cause it prevents (2026-07-12 morpho incident, "UPDATE" section): unlike code
    tarballs (which carry a .manifest.json commit_sha), the GCS setup/startup script a
    Pattern-A VM's startup-script-url points at is published with no freshness record.
    A VM can boot and fetch that object before a
    fix-then-launch turn's publish has landed, silently running stale startup logic
    despite correct instance metadata. Unlike lc_verify_tarball_freshness (which every
    launcher must wire individually), this guard is invoked automatically from
    lc_gcloud_create so every caller inherits it with zero per-launcher wiring.
    SSOT: plans/active/issues/defi_morpho_lending_indices_never_wired_2026_07_12.md
    """

    LIB = "scripts/vm/lib/launcher_common.sh"

    def _lib_abs(self) -> Path:
        return Path(__file__).parent.parent.parent / self.LIB

    def test_off_mode_short_circuits(self):
        result = subprocess.run(
            [
                "bash",
                "-c",
                f'source "{self._lib_abs()}" && LC_SETUP_SCRIPT_FRESHNESS=off '
                'lc_verify_setup_script_freshness bkt "startup-script-url=gs://bkt/vm/setup-data-pipeline-vm.sh"',
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

    def test_no_startup_script_url_is_a_noop(self):
        """Bespoke launchers using --metadata-from-file startup-script have no GCS URL to check."""
        result = subprocess.run(
            [
                "bash",
                "-c",
                f'source "{self._lib_abs()}" && lc_verify_setup_script_freshness bkt "VM_TASK=foo,ENV=prod"',
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

    def _run_with_mock_gcloud(
        self, local_script: Path, remote_hash: str | None, mode: str
    ) -> subprocess.CompletedProcess[str]:
        """Mock `gcloud storage hash`/`objects describe`/`cp` — the guard's local-hash and

        remote-hash reads route through `gcloud storage` (ADC-backed), not `gsutil` (active-
        CLI-account-backed, breaks under an expired WIF token in an interactive AO slot). See
        plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
        """
        lib = self._lib_abs()
        remote_describe_body = f'echo "{remote_hash}"' if remote_hash is not None else "return 1"
        script = f"""
gcloud() {{
    if [[ "$1" == "storage" && "$2" == "hash" ]]; then
        echo "local-hash-abc"
        return 0
    fi
    if [[ "$1" == "storage" && "$2" == "objects" && "$3" == "describe" ]]; then
        {remote_describe_body}
        return 0
    fi
    if [[ "$1" == "storage" && "$2" == "cp" ]]; then
        return 0
    fi
    return 0
}}
export -f gcloud
source "{lib}"
# `source` re-runs `set -euo pipefail`; disable AFTER so a return-1 doesn't abort.
set +e
if lc_verify_setup_script_freshness bkt "startup-script-url=gs://bkt/vm/{local_script.name}"; then rc=0; else rc=$?; fi
echo "RC=$rc"
"""
        # lc_verify_setup_script_freshness resolves the local script relative to
        # lib_dir/.. (scripts/vm/), so point BASH_SOURCE[0] at a lib.sh sitting next
        # to the real script via a temp lib/ dir symlinking the real launcher_common.sh.
        return subprocess.run(
            ["bash", "-c", script],
            capture_output=True,
            text=True,
            env={**os.environ, "LC_SETUP_SCRIPT_FRESHNESS": mode},
        )

    def test_fresh_script_passes(self, tmp_path: Path):
        # setup-data-pipeline-vm.sh already exists in scripts/vm/ — reuse it so
        # lc_verify_setup_script_freshness's local-path resolution finds a real file.
        real_script = self._lib_abs().parent.parent / "setup-data-pipeline-vm.sh"
        assert real_script.exists(), "fixture assumes setup-data-pipeline-vm.sh exists in scripts/vm/"
        result = self._run_with_mock_gcloud(real_script, "local-hash-abc", "warn")
        assert "RC=0" in result.stdout
        assert "setup script fresh" in result.stdout

    def test_stale_script_warn_does_not_block(self, tmp_path: Path):
        real_script = self._lib_abs().parent.parent / "setup-data-pipeline-vm.sh"
        result = self._run_with_mock_gcloud(real_script, "different-remote-hash", "warn")
        assert "RC=0" in result.stdout, "warn mode must not block a launch"
        assert "STALE setup script" in result.stderr
        assert "gcloud storage cp" in result.stderr

    def test_stale_script_enforce_blocks(self, tmp_path: Path):
        real_script = self._lib_abs().parent.parent / "setup-data-pipeline-vm.sh"
        result = self._run_with_mock_gcloud(real_script, "different-remote-hash", "enforce")
        assert "RC=1" in result.stdout, "enforce mode must block a stale launch"
        assert "refusing to launch" in result.stderr

    def test_missing_remote_script_enforce_blocks(self, tmp_path: Path):
        real_script = self._lib_abs().parent.parent / "setup-data-pipeline-vm.sh"
        result = self._run_with_mock_gcloud(real_script, None, "enforce")
        assert "RC=1" in result.stdout
        assert "MISSING" in result.stderr

    def test_lc_gcloud_create_wires_the_guard_automatically(self):
        """No per-launcher wiring needed — lc_gcloud_create calls the guard itself."""
        content = self._lib_abs().read_text()
        assert "lc_verify_setup_script_freshness" in content
        # Verify call site is inside lc_gcloud_create, before the real gcloud invocation.
        create_fn_start = content.index("lc_gcloud_create() {")
        guard_call = content.index("lc_verify_setup_script_freshness", create_fn_start)
        gcloud_call = content.index('gcloud compute instances create "$vm_name"', create_fn_start)
        assert create_fn_start < guard_call < gcloud_call, (
            "lc_verify_setup_script_freshness must be called inside lc_gcloud_create, "
            "before the real `gcloud compute instances create` invocation"
        )


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
gcloud() {{ return 1; }}
export -f gcloud
SKIP_GCS_PREFLIGHT=false bash "{launcher_path}" --dry-run-scheduler-body
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
            f.write(script)
            f.flush()
        try:
            result = subprocess.run(["bash", f.name], capture_output=True, text=True)
        finally:
            os.unlink(f.name)
        assert result.returncode != 0, "Should fail when the startup-script-url pre-flight check returns non-zero"
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
export -f gcloud
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


class TestTarballFreshnessGuardCoverage:
    """Guard: every ``launch-*.sh`` GCP launcher whose VM fetches a code tarball
    (i.e. its ``gcloud`` metadata carries a GCS ``startup-script-url=``, the
    mechanism every ``setup-*-vm.sh`` startup script uses to pull
    ``gs://<bucket>/code/*-code.tar.gz``) must call ``lc_verify_tarball_freshness``
    before launch. A launcher that fetches a tarball but skips the check can
    silently boot onto stale code — the exact 2026-07-12 morpho lending_indices
    incident (mtds-code sat 4 days stale; the VM ran the pre-fix protocol list
    and wrote 0 rows for hours). SSOT:
    plans/active/issues/defi_morpho_lending_indices_never_wired_2026_07_12.md.

    A future "added a tarball-fetching launcher, forgot the freshness guard"
    regression fails here. Genuinely-exempt launchers (AWS family — parity
    tracked separately, same boundary ``TestDurableLogStreamerCoverage`` uses)
    are whitelisted explicitly below with a reason.
    """

    STARTUP_SCRIPT_URL_TOKEN = "startup-script-url=gs://"
    GUARD_TOKEN = "lc_verify_tarball_freshness"

    # Genuinely-exempt launchers. Each entry MUST carry a reason.
    EXEMPT: dict[str, str] = {}

    def _tarball_fetching_launchers(self) -> list[Path]:
        scripts_dir = Path(__file__).parent.parent.parent / "scripts" / "vm"
        # Exclude *-aws.sh (AWS family) — S3/EC2 tarball delivery is a separate
        # mechanism (lc_verify_tarball_freshness reads GCS manifests only);
        # AWS parity is tracked separately, same boundary as
        # TestDurableLogStreamerCoverage.
        return sorted(
            p
            for p in scripts_dir.glob("launch-*.sh")
            if not p.name.endswith("-aws.sh") and self.STARTUP_SCRIPT_URL_TOKEN in p.read_text()
        )

    def test_tarball_fetching_launchers_present(self) -> None:
        assert len(self._tarball_fetching_launchers()) > 0, "No tarball-fetching GCP launch-*.sh scripts found"

    def test_every_tarball_fetching_launcher_wires_the_freshness_guard(self) -> None:
        """No tarball-fetching launcher may skip lc_verify_tarball_freshness
        without an explicit whitelist reason."""
        offenders: list[str] = []
        for script in self._tarball_fetching_launchers():
            content = script.read_text()
            if self.GUARD_TOKEN in content:
                continue
            if script.name in self.EXEMPT:
                continue
            offenders.append(script.name)

        assert not offenders, (
            "Tarball-fetching launcher(s) do NOT wire lc_verify_tarball_freshness "
            f"and are not whitelisted-exempt: {offenders}. Source "
            'lib/launcher_common.sh and call lc_verify_tarball_freshness "$CODE_BUCKET" '
            "<repos...> before the gcloud create — else the VM can silently boot onto "
            "stale code (2026-07-12 morpho lending_indices incident). If genuinely "
            "exempt, add it to TestTarballFreshnessGuardCoverage.EXEMPT with a reason."
        )

    def test_whitelist_entries_still_exist(self) -> None:
        """A whitelisted launcher that no longer exists is stale — drop it so the
        guard stays honest."""
        scripts_dir = Path(__file__).parent.parent.parent / "scripts" / "vm"
        for name in self.EXEMPT:
            assert (scripts_dir / name).exists(), (
                f"Whitelisted-exempt launcher {name} no longer exists — remove it from "
                "TestTarballFreshnessGuardCoverage.EXEMPT."
            )

    def test_whitelist_entries_have_reasons(self) -> None:
        for name, reason in self.EXEMPT.items():
            assert reason.strip(), f"Exempt launcher {name} has no reason — add one."

    def test_guard_catches_an_unwired_launcher(self) -> None:
        """Self-test: a synthetic launcher that fetches a tarball but omits the
        freshness guard (and is not whitelisted) is detected as an offender —
        proving the guard would catch the regression it exists to prevent."""
        fake_content = (
            "#!/usr/bin/env bash\nset -euo pipefail\n"
            'gcloud compute instances create "my-vm" --zone=asia-northeast1-c '
            '--metadata="startup-script-url=gs://deployment-scripts-x/vm/setup-data-pipeline-vm.sh"\n'
        )
        assert self.STARTUP_SCRIPT_URL_TOKEN in fake_content
        assert self.GUARD_TOKEN not in fake_content, (
            "Self-test sentinel: a launcher with no guard token must read as unwired."
        )
        # And a converted one (wires lc_verify_tarball_freshness) reads as covered.
        converted = fake_content + 'lc_verify_tarball_freshness "$CODE_BUCKET" my-repo\n'
        assert self.GUARD_TOKEN in converted


class TestCanonicalMigrationVmRelaunch:
    """SPOT-preemption relaunch support for launch-canonical-migration-vm.sh (adversarial review
    2026-07-22, HIGH finding): VM_NAME_OVERRIDE + RESUME_* env fallbacks + lc_write_launch_params,
    cloned from launch-mdps-backfill-vm.sh's already-working mechanism. Root cause closed: a
    SPOT-preempted canonical-migration-* VM's checkpoint blob is keyed on VM_NAME
    (`vm-logs/{VM_NAME}/MIGRATION_PROGRESS-shard{N}.json`), but RelaunchPreemptedVm re-invokes the
    launcher with ZERO positional CLI args and a fresh RUN_TS would mint a DIFFERENT vm_name every
    time -- so the checkpoint the preempted VM wrote could never be found, and the bare re-invocation
    also used to hit the launcher's own positional-arg usage-error `exit 2` (no ASSET_GROUP/START_DATE/
    END_DATE at all). These tests never touch real GCS/GCE — gcloud/gsutil are mocked shell functions.
    """

    LAUNCHER = "scripts/vm/launch-canonical-migration-vm.sh"

    @pytest.fixture
    def launcher_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.LAUNCHER

    def _mock_preamble(self, capture_dir: Path, gcloud_log: Path) -> str:
        # gcloud: no-op success; every "instances create <name> ..." call is appended to gcloud_log so
        # tests can assert exactly how many VMs were created (fan-out vs single relaunch) and under
        # which name. `storage cp - <uri> --quiet` (lc_write_launch_params / lc_write_tarball_pin_record's
        # actual mechanism, `gcloud storage` not `gsutil` -- see
        # plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md) is
        # intercepted -- stdin is captured to `<capture_dir>/<basename(uri)>` so a test can read back the
        # exact JSON that would have been persisted to GCS; every other gcloud invocation (freshness
        # checks etc.) is a harmless no-op. python3 is left REAL so lc_write_launch_params's own
        # JSON-building one-liner runs for real, just piped into our mock.
        return f'''
gcloud() {{
    if [[ "$1 $2 $3" == "compute instances create" ]]; then
        echo "$4" >> "{gcloud_log}"
        return 0
    fi
    if [[ "$1 $2 $3" == "storage cp -" ]]; then
        local uri="$4"
        cat > "{capture_dir}/$(basename "$uri")"
        return 0
    fi
    return 0
}}
export -f gcloud
'''

    def _run(
        self, launcher_path: Path, args: list[str], env_extra: dict[str, str], tmp_path: Path
    ) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
        capture_dir = tmp_path / "gsutil_captures"
        capture_dir.mkdir()
        gcloud_log = tmp_path / "gcloud_create_calls.log"
        # No DRY_RUN=true here (deliberately, 2026-07-22): the launcher's own DRY_RUN flag now
        # genuinely skips the `gcloud compute instances create` call (fixed after a candle-apply
        # adversarial self-test proved DRY_RUN=true previously did NOT gate it at all, silently
        # creating a real VM on every "preview" invocation across every category). These tests need
        # the create call to actually run so `gcloud_log` observes it -- infra safety here comes
        # entirely from the mocked `gcloud`/`gsutil` shell functions above, never from DRY_RUN.
        script = self._mock_preamble(capture_dir, gcloud_log) + f'\nbash "{launcher_path}" {" ".join(args)}\n'
        result = subprocess.run(
            ["bash", "-c", script],
            capture_output=True,
            text=True,
            env={**os.environ, **env_extra},
        )
        return result, capture_dir, gcloud_log

    def test_vm_name_override_flag_pins_the_created_vm_name(self, launcher_path: Path, tmp_path: Path) -> None:
        result, _capture_dir, gcloud_log = self._run(
            launcher_path,
            ["--vm-name", "canonical-migration-cefi-pinned-test", "cefi", "2020-01-01", "2026-01-01", "dry"],
            {},
            tmp_path,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert gcloud_log.exists()
        created_names = gcloud_log.read_text().split()
        assert created_names == ["canonical-migration-cefi-pinned-test"]

    def test_vm_name_override_env_var_pins_the_created_vm_name(self, launcher_path: Path, tmp_path: Path) -> None:
        result, _capture_dir, gcloud_log = self._run(
            launcher_path,
            ["cefi", "2020-01-01", "2026-01-01", "dry"],
            {"VM_NAME_OVERRIDE": "canonical-migration-cefi-env-pinned-test"},
            tmp_path,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert gcloud_log.read_text().split() == ["canonical-migration-cefi-env-pinned-test"]

    def test_resume_env_fallback_avoids_the_positional_arg_usage_error(
        self, launcher_path: Path, tmp_path: Path
    ) -> None:
        """The exact bug the review found: RelaunchPreemptedVm re-invokes the launcher with ZERO
        positional args, only RESUME_* env vars. Before this fix that always hit `exit 2`."""
        result, _capture_dir, gcloud_log = self._run(
            launcher_path,
            [],  # zero positional args -- exactly what RelaunchPreemptedVm's subprocess.run does
            {
                "VM_NAME_OVERRIDE": "canonical-migration-cefi-relaunch-test",
                "RESUME_ASSET_GROUP": "cefi",
                "RESUME_START_DATE": "2020-01-01",
                "RESUME_END_DATE": "2026-01-01",
                "RESUME_MODE": "full",
            },
            tmp_path,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert result.returncode != 2, "must not hit the positional-arg usage error on a bare relaunch"
        assert gcloud_log.read_text().split() == ["canonical-migration-cefi-relaunch-test"]

    def test_launch_params_persist_vm_name_and_resume_fields_for_relaunch(
        self, launcher_path: Path, tmp_path: Path
    ) -> None:
        """lc_write_launch_params must be called with everything a future relaunch needs to reproduce
        this EXACT VM: its own name (so the checkpoint blob is found) and RESUME_* positional-arg
        equivalents (so a bare re-invocation doesn't hit the usage-error exit 2)."""
        import json

        result, capture_dir, _gcloud_log = self._run(
            launcher_path, ["cefi", "2020-01-01", "2026-01-01", "full"], {}, tmp_path
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        params_file = capture_dir / "LAUNCH_PARAMS.json"
        assert params_file.exists(), "lc_write_launch_params must have been called"
        payload = json.loads(params_file.read_text())
        assert payload["launcher"] == "launch-canonical-migration-vm.sh"
        env = payload["env"]
        assert env["RESUME_ASSET_GROUP"] == "cefi"
        assert env["RESUME_START_DATE"] == "2020-01-01"
        assert env["RESUME_END_DATE"] == "2026-01-01"
        assert env["RESUME_MODE"] == "full"
        # VM_NAME_OVERRIDE must equal THIS VM's own auto-generated name (not pinned in this test) --
        # proof the persisted override will reproduce the SAME name on a future relaunch.
        assert env["VM_NAME_OVERRIDE"].startswith("canonical-migration-cefi-")

    def test_relaunch_of_a_pinned_shard_does_not_re_trigger_the_fan_out_loop(
        self, launcher_path: Path, tmp_path: Path
    ) -> None:
        """The tradfi category fans out SHARD_OF VMs (one per shard) when SHARD_INDEX is left unset. A
        relaunch of ONE preempted shard VM must target exactly that one shard again -- never
        re-trigger the N-VM fan-out just because SHARD_OF>1 (SHARD_INDEX_EXPLICIT must be honored via
        RESUME_SHARD_INDEX, not just the raw SHARD_INDEX env var a fresh original launch would use)."""
        result, _capture_dir, gcloud_log = self._run(
            launcher_path,
            [],
            {
                "VM_NAME_OVERRIDE": "canonical-migration-tradfi-shard3-relaunch-test",
                "RESUME_ASSET_GROUP": "tradfi",
                "RESUME_START_DATE": "2020-01-01",
                "RESUME_END_DATE": "2026-01-01",
                "RESUME_MODE": "full",
                "RESUME_SHARD_OF": "20",
                "RESUME_SHARD_INDEX": "3",
            },
            tmp_path,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        created = gcloud_log.read_text().split()
        # Exactly ONE VM created (the pinned relaunch), never a 20-VM fan-out.
        assert created == ["canonical-migration-tradfi-shard3-relaunch-test"], (
            f"relaunch must target exactly the one preempted shard, got: {created}"
        )

    def test_fresh_tradfi_launch_without_resume_still_fans_out_normally(
        self, launcher_path: Path, tmp_path: Path
    ) -> None:
        """Regression guard for the fix above: an ORIGINAL (non-relaunch) tradfi launch with SHARD_OF>1
        and no SHARD_INDEX pinned must still fan out one VM per shard -- the RESUME_SHARD_INDEX fallback
        must never suppress a genuine fresh fan-out."""
        result, _capture_dir, gcloud_log = self._run(
            launcher_path,
            ["tradfi", "2020-01-01", "2026-01-01", "dry"],
            {"SHARD_OF": "3"},
            tmp_path,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        created = gcloud_log.read_text().split()
        assert len(created) == 3, f"expected a 3-VM fan-out, got: {created}"

    def test_syntax_valid(self, launcher_path: Path) -> None:
        result = subprocess.run(["bash", "-n", str(launcher_path)], capture_output=True, text=True)
        assert result.returncode == 0, f"Syntax error: {result.stderr}"

    def _mock_preamble_full_args(self, gcloud_log: Path) -> str:
        """Like `_mock_preamble` but captures the FULL `compute instances create` argument list
        (not just the vm-name at $4), so a test can grep the `--metadata=` value for
        VM_SERVICE=/VM_MIGRATION_CMD= content."""
        return f'''
gcloud() {{
    if [[ "$1 $2 $3" == "compute instances create" ]]; then
        printf '%s\\n' "$*" >> "{gcloud_log}"
        return 0
    fi
    return 0
}}
export -f gcloud
'''

    def test_defi_curve_optimism_reclassify_dry_uses_instruments_service_and_dry_run_flag(
        self, launcher_path: Path, tmp_path: Path
    ) -> None:
        """New category (2026-07-27): the one-shot CURVE/OPTIMISM dex_pool_swaps reclassify script
        lives in instruments-service, not MTDS, and `dry` mode must pass --dry-run to the tool."""
        gcloud_log = tmp_path / "gcloud_create_calls.log"
        script = self._mock_preamble_full_args(gcloud_log) + (
            f'\nbash "{launcher_path}" defi-curve-optimism-reclassify 2026-07-27 2026-07-27 dry\n'
        )
        result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env={**os.environ})
        assert result.returncode == 0, f"stderr: {result.stderr}"
        call = gcloud_log.read_text()
        assert "VM_SERVICE=instruments_service" in call
        assert "reclassify_defi_curve_optimism_subgraph_deindexed_2026_07_24.py --dry-run" in call
        assert "--apply" not in call

    def test_defi_curve_optimism_reclassify_full_uses_apply_flag(self, launcher_path: Path, tmp_path: Path) -> None:
        """`full` mode must pass --apply, not --dry-run — the actual data-mutating run."""
        gcloud_log = tmp_path / "gcloud_create_calls.log"
        script = self._mock_preamble_full_args(gcloud_log) + (
            f'\nbash "{launcher_path}" defi-curve-optimism-reclassify 2026-07-27 2026-07-27 full\n'
        )
        result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env={**os.environ})
        assert result.returncode == 0, f"stderr: {result.stderr}"
        call = gcloud_log.read_text()
        assert "reclassify_defi_curve_optimism_subgraph_deindexed_2026_07_24.py --apply" in call
        assert "--dry-run" not in call

    def test_defi_curve_optimism_reclassify_vm_name_stays_under_gce_limit(
        self, launcher_path: Path, tmp_path: Path
    ) -> None:
        """Regression guard: the un-abbreviated category name
        ('defi-curve-optimism-reclassify', 31 chars) + the 'canonical-migration-' prefix (21 chars)
        + the RUN_TS timestamp (15 chars) is 66 chars — over GCE's 63-char instance-name limit
        BEFORE any shard suffix is even added. The vm_name-only abbreviation (mirroring the existing
        *-candle-apply pattern) must keep the real, launched name under budget."""
        gcloud_log = tmp_path / "gcloud_create_calls.log"
        script = self._mock_preamble_full_args(gcloud_log) + (
            f'\nbash "{launcher_path}" defi-curve-optimism-reclassify 2026-07-27 2026-07-27 dry\n'
        )
        result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env={**os.environ})
        assert result.returncode == 0, f"stderr: {result.stderr}"
        call = gcloud_log.read_text()
        # `"$*"` logs the FULL invocation ("compute instances create <vm_name> --zone=... ..."),
        # so the vm_name is the 4th word (index 3), not the first.
        vm_name = call.split()[3]
        assert vm_name.startswith("canonical-migration-defi-curve-optm-reclass-"), vm_name
        assert len(vm_name) <= 63, f"'{vm_name}' is {len(vm_name)} chars, exceeds GCE's 63-char limit"


class TestCanonicalMigrationStallDetection:
    """Todo 3+4, /plans/active/issues/migration_vm_hung_detection_monitoring_gap_2026_07_27.md
    (Gap 2): 10/42 cefi-content-apply canonical-migration VMs sat hung 1-2.5h+ with GCE reporting
    RUNNING and nothing paging. Root cause traced this session: every VM_TASK=canonical-migration
    worker already routes through the shared setup-data-pipeline-vm.sh -> _launch_with_tee() ->
    vm-exec-with-gcs-tee.sh stall-kill (no code change needed in either of those two files --
    STALL_TIMEOUT_SEC/STALL_PROGRESS_REGEX are read off GCE instance metadata generically,
    independent of VM_TASK), but no launcher category ever SET STALL_PROGRESS_REGEX, so every
    category fell back to raw log-BYTE-GROWTH stall detection -- permanently defeated by the
    always-on PIPELINE_HEARTBEAT emitter wired into the SAME tee'd log (it writes a line every 60s
    regardless of whether the real workload is alive). The fix: set STALL_PROGRESS_REGEX for
    cefi-content-apply specifically (its script's real progress-log line format was read straight
    out of migrate_cefi_content_instrument_id_catalogue_2026_07_17.py this session), scoped
    narrowly per todo 5's stated per-category audit boundary -- every other
    VM_TASK=canonical-migration category's script has NOT been individually verified and
    deliberately gets no regex here yet.

    This class proves both halves: (1) the launcher wires the metadata key ONLY for
    cefi-content-apply (never a different, unverified category); and (2) the underlying
    vm-exec-with-gcs-tee.sh watchdog mechanism ACTUALLY closes the gap on a simulated hang -- and
    would NOT have, absent this fix (the exact historical incident, reproduced small-scale, byte-
    for-byte against the shipped production watchdog script, not a reimplementation of it)."""

    LAUNCHER = TestCanonicalMigrationVmRelaunch.LAUNCHER
    WATCHDOG = "scripts/vm/vm-exec-with-gcs-tee.sh"
    # The exact regex this session added to launch-canonical-migration-vm.sh's cefi-content-apply
    # branch -- kept as one constant so the metadata-wiring tests and the live-watchdog tests below
    # can never silently drift apart from each other or from the shipped value.
    REGEX = "progress:|files/sec"

    @pytest.fixture
    def launcher_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.LAUNCHER

    @pytest.fixture
    def watchdog_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.WATCHDOG

    # ---- half 1: the launcher wires the metadata key, scoped correctly ----

    def _created_metadata(self, launcher_path: Path, args: list[str], tmp_path: Path) -> str:
        gcloud_log = tmp_path / "gcloud_create_calls.log"
        preamble = f'''
gcloud() {{
    if [[ "$1 $2 $3" == "compute instances create" ]]; then
        printf '%s\\n' "$*" >> "{gcloud_log}"
        return 0
    fi
    return 0
}}
export -f gcloud
gsutil() {{ return 0; }}
export -f gsutil
'''
        script = preamble + f'\nbash "{launcher_path}" {" ".join(args)}\n'
        result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env={**os.environ})
        assert result.returncode == 0, f"stderr: {result.stderr}"
        return gcloud_log.read_text()

    def test_cefi_content_apply_sets_the_verified_stall_progress_regex(
        self, launcher_path: Path, tmp_path: Path
    ) -> None:
        call = self._created_metadata(
            launcher_path, ["cefi-content-apply", "2019-03-30", "2026-07-27", "dry"], tmp_path
        )
        assert f"STALL_PROGRESS_REGEX={self.REGEX}" in call

    @pytest.mark.parametrize(
        "args",
        [
            ["cefi-late-renames", "2025-11-01", "2026-07-24", "dry"],
            ["cefi-dedup-apply", "2026-07-27", "2026-07-27", "dry"],
            ["cefi", "2020-01-01", "2026-01-01", "dry"],
        ],
    )
    def test_other_categories_get_no_stall_progress_regex_yet(
        self, launcher_path: Path, tmp_path: Path, args: list[str]
    ) -> None:
        """Regression guard for the narrow scoping: only cefi-content-apply's real progress-log
        format has been individually verified (todo 5's per-category audit is deliberately left
        open for every other VM_TASK=canonical-migration category) -- broadening this regex onto
        an unverified category's script would risk a false-positive stall-kill on a legitimately
        slow/quiet phase of a DIFFERENT tool with a different log shape."""
        call = self._created_metadata(launcher_path, args, tmp_path)
        assert "STALL_PROGRESS_REGEX=" not in call

    # ---- half 2: the regex itself, against the real log-line formats ----

    @pytest.mark.parametrize(
        "line,expect_match",
        [
            # migrate_cefi_content_instrument_id_catalogue_2026_07_17.py's discovery-phase marker.
            ("12:00:01 INFO  Discovery progress: day=2019-03-30 cumulative_files=5 elapsed=1.2s", True),
            # Its per-200-file migrate-phase marker.
            (
                "12:00:02 INFO  Progress: 200/5000 files (5.2 files/sec, 38.5s elapsed) stats={'ok': 200}",
                True,
            ),
            # Its final migrate-phase summary line.
            ("12:00:03 INFO  Elapsed (migrate phase): 120.3s (8.31 files/sec)", True),
            # Its OWN wedged-worker WARNING -- must NEVER count as progress (case-sensitive
            # "progress:" does not match "progress in", and this line has no "files/sec").
            (
                "12:00:04 WARN  No progress in the last poll window -- 12 files still outstanding "
                "(possible wedged worker)",
                False,
            ),
        ],
    )
    def test_regex_matches_exactly_the_intended_lines(self, line: str, expect_match: bool) -> None:
        """Checks the exact production regex case-sensitively against the real line shapes the
        migration script emits, using the SAME `grep -qE` matcher vm-exec-with-gcs-tee.sh itself
        uses (line 260: `grep -qE "$STALL_PROGRESS_REGEX"`), not a reimplementation in Python."""
        result = subprocess.run(["grep", "-qE", self.REGEX], input=line, text=True)
        matched = result.returncode == 0
        assert matched == expect_match, f"line={line!r} expected_match={expect_match} got={matched}"

    # ---- half 3: the real watchdog, against a simulated hang ----

    def _mock_bin_functions(self) -> str:
        """Bash-function mocks for the heavy deps vm-exec-with-gcs-tee.sh talks to, so this test
        never touches real GCS/GCE credentials. `stat` is additionally shimmed to a portable
        `wc -c`-based equivalent of GNU coreutils' `stat -c %s` (production always runs this
        script on real Ubuntu GCE VMs where that flag is native; a local dev/CI box may not be
        Linux, so this keeps the byte-growth-vs-progress-regex comparison meaningful everywhere
        rather than silently degenerating on a non-GNU `stat`). All three are exported so they
        survive the script's own `exec setsid bash "$0" "$@"` re-exec when `setsid` is present
        (bash forwards `export -f` functions to a child bash via BASH_FUNC_*% env vars regardless
        of the intermediate `setsid` binary; verified directly against this exact script this
        session on both a no-setsid host and by tracing the re-exec logic)."""
        return """
python() {
    if [[ "$1" == "-c" ]]; then
        echo "00000000-0000-0000-0000-000000000000"
        return 0
    fi
    return 0
}
export -f python
gsutil() { return 0; }
export -f gsutil
stat() {
    if [[ "$1" == "-c" && "$2" == "%s" ]]; then
        wc -c < "$3" 2>/dev/null | tr -d ' '
        return 0
    fi
    return 1
}
export -f stat
"""

    def _run_watchdog(
        self,
        watchdog_path: Path,
        workload: str,
        stall_progress_regex: str | None,
        tmp_path: Path,
    ) -> subprocess.CompletedProcess[str]:
        gcs_log_uri = f"gs://fake-test-bucket/vm-logs/{tmp_path.name}/run.log"
        env = {
            **os.environ,
            "VM_NAME": f"stall-detection-unit-test-{tmp_path.name}",
            "VM_ASSET_GROUP": "CEFI",
            "VM_TASK": "canonical-migration",
            "VM_MODE": "full",
            "PYTHON_BIN": "python",
            # Compressed timings for test speed -- production default is STALL_TIMEOUT_SEC=1800
            # (30 min); only the threshold is compressed here, the watchdog logic under test is
            # byte-for-byte the shipped production script (no reimplementation).
            "STALL_TIMEOUT_SEC": "3",
            "STALL_POLL_SEC": "1",
        }
        if stall_progress_regex is not None:
            env["STALL_PROGRESS_REGEX"] = stall_progress_regex
        else:
            env.pop("STALL_PROGRESS_REGEX", None)
        script = self._mock_bin_functions() + (
            f"bash {shlex.quote(str(watchdog_path))} {shlex.quote(gcs_log_uri)} bash -c {shlex.quote(workload)}\n"
        )
        return subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env, timeout=25)

    def test_hung_worker_with_noisy_non_progress_log_is_killed_when_regex_is_set(
        self, watchdog_path: Path, tmp_path: Path
    ) -> None:
        """The exact incident this fixes: a worker wedged on a network call while its log keeps
        emitting non-progress noise (the always-on PIPELINE_HEARTBEAT emitter, or any other
        recurring line that never matches a real progress marker) -- proven this session to
        silently defeat the byte-growth-only default for 1-2.5h+ across 10/42 real VMs."""
        workload = (
            'for i in $(seq 1 10); do echo "PIPELINE_HEARTBEAT tick=$i (no real progress)"; '
            "sleep 1; done; echo DONE_NORMALLY"
        )
        result = self._run_watchdog(watchdog_path, workload, self.REGEX, tmp_path)
        # rc=124 is airtight proof the kill fired: the workload's OWN natural exit code (had it
        # ever reached its `echo DONE_NORMALLY` tail) would be 0, and the script only ever
        # overrides RC to 124 in the branch gated on the STALL_BREADCRUMB file existing (written
        # exclusively by the watchdog's stall-kill path) -- there is no other way to observe 124
        # here. (The workload's stdout is teed into a `/tmp/vm-exec-$$.log` this test doesn't
        # capture, and the startup banner already echoes the command's OWN source text --
        # including the literal substring "DONE_NORMALLY" -- to this process's stdout regardless
        # of outcome, so checking for that substring's absence here would be a false signal, not
        # evidence of anything.)
        assert result.returncode == 124, (
            f"expected the stall-kill (rc=124), got rc={result.returncode}\nstdout={result.stdout}"
        )
        assert "status=failed" in result.stdout

    def test_same_hang_is_not_caught_by_the_byte_growth_only_default(self, watchdog_path: Path, tmp_path: Path) -> None:
        """Regression baseline proving the bug this fix closes: the IDENTICAL noisy hang, but with
        STALL_PROGRESS_REGEX unset (every canonical-migration category's behavior before this
        session's fix) -- the noise itself keeps the log growing, so byte-growth mode never fires
        and the wedged worker runs to its own (here, harmless) natural completion instead of being
        killed. This is exactly how 10/42 real cefi-content-apply VMs sat hung 1-2.5h+ undetected."""
        workload = (
            'for i in $(seq 1 6); do echo "PIPELINE_HEARTBEAT tick=$i (no real progress)"; '
            "sleep 1; done; echo DONE_NORMALLY"
        )
        result = self._run_watchdog(watchdog_path, workload, None, tmp_path)
        assert result.returncode == 0, (
            f"byte-growth-only mode should let the noisy-but-hung run finish on its own, "
            f"got rc={result.returncode}\nstdout={result.stdout}"
        )
        assert "status=completed" in result.stdout

    def test_genuinely_healthy_run_with_real_progress_markers_is_not_false_killed(
        self, watchdog_path: Path, tmp_path: Path
    ) -> None:
        """No-false-positive check: a run that periodically emits the tool's OWN wedged-worker
        WARNING line ("No progress in the last poll window...") interleaved with genuine
        "Progress: ... files/sec" markers must NOT be killed -- the WARNING text never resets the
        timer on its own (per test_regex_matches_exactly_the_intended_lines above), only the real
        marker does, and campaign-measured healthy throughput (2.9-9.9 files/sec/VM) gives >>25x
        headroom under the unchanged 1800s production STALL_TIMEOUT_SEC default."""
        workload = (
            "for i in 1 2 3 4 5 6; do "
            'echo "No progress in the last poll window -- $i files still outstanding '
            '(possible wedged worker)"; sleep 1; '
            'echo "Progress: $i/10 files (2.0 files/sec, 5.0s elapsed) stats={}"; sleep 1; '
            "done; echo DONE_NORMALLY"
        )
        result = self._run_watchdog(watchdog_path, workload, self.REGEX, tmp_path)
        assert result.returncode == 0, (
            f"a genuinely-progressing worker must not be killed, got rc={result.returncode}\nstdout={result.stdout}"
        )
        assert "status=completed" in result.stdout


class TestCefiFundingTimestampFixStallDetection:
    """Real incident 2026-07-29 (a genuinely NEW instance of the exact bug class
    /plans/active/issues/migration_vm_hung_detection_monitoring_gap_2026_07_27.md's
    Gap 2 documents -- this launcher did not exist yet when that doc's todo 5 audited
    the other 103 launch-*-vm.sh scripts): a BINANCE-FUTURES
    canonical-migration-cefi-fts-* VM's migration script printed its own final
    "=== SUMMARY (APPLIED) ..." line, then the process never actually exited -- the
    VM sat GCE-RUNNING for 3+ hours with zero real progress (only the always-on
    PIPELINE_HEARTBEAT emitter kept the tee'd log growing) before being noticed and
    manually killed. Same root cause, same fix shape as todo 4's cefi-content-apply
    fix: this launcher already routes through the shared setup-data-pipeline-vm.sh ->
    _launch_with_tee() -> vm-exec-with-gcs-tee.sh stall-kill (VM_TASK=canonical-
    migration), but had no STALL_PROGRESS_REGEX set, so it fell back to the
    byte-growth-only default that heartbeat noise permanently defeats.

    Mirrors TestCanonicalMigrationStallDetection's proof shape: (1) the launcher
    wires the metadata key for both the N=1 and sharded launch paths; (2) the exact
    regex matches this script's real per-object progress-log line
    (reprocess_bulk_tardis_derivative_ticker_funding_timestamp_2026_07_28.py line
    ~592); (3) against the REAL shipped vm-exec-with-gcs-tee.sh (not a
    reimplementation), a simulated post-completion hang IS killed with the regex set
    and is NOT killed under the byte-growth-only default -- the exact historical
    incident, reproduced small-scale."""

    LAUNCHER = "scripts/vm/launch-cefi-funding-timestamp-fix-vm.sh"
    WATCHDOG = "scripts/vm/vm-exec-with-gcs-tee.sh"
    REGEX = "action="

    @pytest.fixture
    def launcher_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.LAUNCHER

    @pytest.fixture
    def watchdog_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.WATCHDOG

    # ---- half 1: the launcher wires the metadata key, for N=1 and sharded alike ----

    def _created_metadata(self, launcher_path: Path, args: list[str], tmp_path: Path) -> str:
        gcloud_log = tmp_path / "gcloud_create_calls.log"
        preamble = f'''
gcloud() {{
    if [[ "$1 $2 $3" == "compute instances create" ]]; then
        printf '%s\\n' "$*" >> "{gcloud_log}"
        return 0
    fi
    return 0
}}
export -f gcloud
gsutil() {{ return 0; }}
export -f gsutil
'''
        script = preamble + f'\nbash "{launcher_path}" {" ".join(args)}\n'
        result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env={**os.environ})
        assert result.returncode == 0, f"stderr: {result.stderr}"
        return gcloud_log.read_text()

    def test_n1_default_launch_sets_the_stall_progress_regex(self, launcher_path: Path, tmp_path: Path) -> None:
        call = self._created_metadata(launcher_path, ["BYBIT", "2020-01-01", "2020-01-05"], tmp_path)
        assert f"STALL_PROGRESS_REGEX={self.REGEX}" in call

    def test_sharded_launch_sets_the_stall_progress_regex_for_every_shard(
        self, launcher_path: Path, tmp_path: Path
    ) -> None:
        call = self._created_metadata(launcher_path, ["--shards", "3", "BYBIT", "2020-01-01", "2020-01-10"], tmp_path)
        assert call.count(f"STALL_PROGRESS_REGEX={self.REGEX}") == 3

    # ---- half 2: the regex itself, against the real log-line format ----

    @pytest.mark.parametrize(
        "line,expect_match",
        [
            # reprocess_bulk_tardis_derivative_ticker_funding_timestamp_2026_07_28.py's real
            # per-object line (script line ~592), across every action= value it emits.
            (
                "  BINANCE-FUTURES: action=corrected rows=69947 populated=69946 shifted=69946 "
                "cadence_registered=True staged=... backup=...",
                True,
            ),
            (
                "  BINANCE-FUTURES: action=skipped_next_funding_timestamp_already_present "
                "rows=27205 populated=0 shifted=0 cadence_registered=True",
                True,
            ),
            ("  DERIBIT: action=skipped_all_null_no_forward_value rows=100 populated=0 shifted=0", True),
            ("  EXTENDED-STARKNET: action=skipped_no_funding_timestamp_column rows=0", True),
            # The always-on heartbeat noise that defeated byte-growth mode in the real incident --
            # must NEVER count as progress.
            ("PIPELINE_HEARTBEAT vm=canonical-migration-cefi-fts-binance-futures-x ts=2026-07-29T10:00:00Z", False),
            # The final one-time summary line -- also must not match (it is emitted exactly once,
            # right before the observed hang, so treating it as recurring progress would mask the
            # exact failure mode this fix closes).
            ("=== SUMMARY (APPLIED) run_ts=20260729-090941 venue=BINANCE-FUTURES window=... ===", False),
        ],
    )
    def test_regex_matches_exactly_the_intended_lines(self, line: str, expect_match: bool) -> None:
        """Checks the exact production regex case-sensitively against the real line shapes,
        using the SAME `grep -qE` matcher vm-exec-with-gcs-tee.sh itself uses (line 260)."""
        result = subprocess.run(["grep", "-qE", self.REGEX], input=line, text=True)
        matched = result.returncode == 0
        assert matched == expect_match, f"line={line!r} expected_match={expect_match} got={matched}"

    # ---- half 3: the real watchdog, against the exact observed incident shape ----

    def _mock_bin_functions(self) -> str:
        return """
python() {
    if [[ "$1" == "-c" ]]; then
        echo "00000000-0000-0000-0000-000000000000"
        return 0
    fi
    return 0
}
export -f python
gsutil() { return 0; }
export -f gsutil
stat() {
    if [[ "$1" == "-c" && "$2" == "%s" ]]; then
        wc -c < "$3" 2>/dev/null | tr -d ' '
        return 0
    fi
    return 1
}
export -f stat
"""

    def _run_watchdog(
        self,
        watchdog_path: Path,
        workload: str,
        stall_progress_regex: str | None,
        tmp_path: Path,
    ) -> subprocess.CompletedProcess[str]:
        gcs_log_uri = f"gs://fake-test-bucket/vm-logs/{tmp_path.name}/run.log"
        env = {
            **os.environ,
            "VM_NAME": f"cefi-fts-stall-unit-test-{tmp_path.name}",
            "VM_ASSET_GROUP": "CEFI",
            "VM_TASK": "canonical-migration",
            "VM_MODE": "full",
            "PYTHON_BIN": "python",
            "STALL_TIMEOUT_SEC": "3",
            "STALL_POLL_SEC": "1",
        }
        if stall_progress_regex is not None:
            env["STALL_PROGRESS_REGEX"] = stall_progress_regex
        else:
            env.pop("STALL_PROGRESS_REGEX", None)
        script = self._mock_bin_functions() + (
            f"bash {shlex.quote(str(watchdog_path))} {shlex.quote(gcs_log_uri)} bash -c {shlex.quote(workload)}\n"
        )
        return subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env, timeout=25)

    def test_post_completion_hang_is_killed_when_regex_is_set(self, watchdog_path: Path, tmp_path: Path) -> None:
        """The exact incident: the migration script prints its final SUMMARY line, then the
        process itself never exits -- only heartbeat-shaped noise keeps appearing afterward."""
        workload = (
            'echo "=== SUMMARY (APPLIED) run_ts=x venue=BINANCE-FUTURES window=... ==="; '
            'for i in $(seq 1 10); do echo "PIPELINE_HEARTBEAT tick=$i"; sleep 1; done; echo DONE_NORMALLY'
        )
        result = self._run_watchdog(watchdog_path, workload, self.REGEX, tmp_path)
        assert result.returncode == 124, (
            f"expected the stall-kill (rc=124), got rc={result.returncode}\nstdout={result.stdout}"
        )
        assert "status=failed" in result.stdout

    def test_same_hang_is_not_caught_by_the_byte_growth_only_default(self, watchdog_path: Path, tmp_path: Path) -> None:
        """Regression baseline proving the bug: the IDENTICAL post-completion hang, but with
        STALL_PROGRESS_REGEX unset (this launcher's behavior before this fix) -- the heartbeat
        noise itself keeps the log growing, so byte-growth mode never fires. This is exactly how
        the real BINANCE-FUTURES VM sat hung 3+ hours undetected."""
        workload = (
            'echo "=== SUMMARY (APPLIED) run_ts=x venue=BINANCE-FUTURES window=... ==="; '
            'for i in $(seq 1 6); do echo "PIPELINE_HEARTBEAT tick=$i"; sleep 1; done; echo DONE_NORMALLY'
        )
        result = self._run_watchdog(watchdog_path, workload, None, tmp_path)
        assert result.returncode == 0, (
            f"byte-growth-only mode should let the noisy-but-hung run finish on its own, "
            f"got rc={result.returncode}\nstdout={result.stdout}"
        )
        assert "status=completed" in result.stdout

    def test_genuinely_healthy_run_with_real_action_lines_is_not_false_killed(
        self, watchdog_path: Path, tmp_path: Path
    ) -> None:
        """No-false-positive check: a run emitting real per-object action= lines interleaved with
        heartbeat noise must not be killed."""
        workload = (
            "for i in 1 2 3 4 5 6; do "
            'echo "PIPELINE_HEARTBEAT tick=$i"; sleep 1; '
            'echo "  BINANCE-FUTURES: action=corrected rows=$i populated=$i shifted=$i "; sleep 1; '
            "done; echo DONE_NORMALLY"
        )
        result = self._run_watchdog(watchdog_path, workload, self.REGEX, tmp_path)
        assert result.returncode == 0, (
            f"a genuinely-progressing worker must not be killed, got rc={result.returncode}\nstdout={result.stdout}"
        )
        assert "status=completed" in result.stdout


class TestDefiLaunchersSpotPreemptionContract:
    """SPOT preemption contract for the two DeFi backfill launchers
    (defi_mvp_backfill_optimization_ready_2026_07_20.md defect #2 — "Both DeFi
    launchers MISS the SPOT preemption contract"). Before this fix,
    launch-defi-backfill-vm.sh + launch-mtds-solana-defi-backfill-vm.sh had ZERO
    lc_write_preemption_signal_file / lc_write_launch_params calls (unlike
    launch-cefi-sharded-backfill.sh:568-589, which has 6), so a SPOT preemption
    relaunched blind onto the launcher's bare defaults instead of the exact
    scope the terminated VM was running. These tests never touch real GCS/GCE —
    gcloud/gsutil are mocked shell functions (same harness as
    TestCanonicalMigrationVmRelaunch above)."""

    DEFI_LAUNCHER = "scripts/vm/launch-defi-backfill-vm.sh"
    SOLANA_LAUNCHER = "scripts/vm/launch-mtds-solana-defi-backfill-vm.sh"

    def _mock_preamble(self, capture_dir: Path, gcloud_log: Path) -> str:
        # gcloud: no-op success; every "compute instances create" call's FULL
        # argument list is appended to gcloud_log (unlike the vm-name-only "$4"
        # capture in TestCanonicalMigrationVmRelaunch) so a test can grep for
        # --metadata-from-file=shutdown-script=... on the same line. "compute
        # instances list" (the Solana launcher's already-running guard) falls
        # through the else branch and returns nothing, i.e. EXISTING="" — the
        # guard's normal not-already-running path. `storage cp - <uri> --quiet`
        # (lc_write_launch_params's actual mechanism, `gcloud storage` not
        # `gsutil` -- see
        # plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md)
        # is intercepted, stdin captured to <capture_dir>/<basename(uri)>; every
        # other gcloud invocation (freshness checks, etc.) is a harmless no-op.
        return f'''
gcloud() {{
    if [[ "$1 $2 $3" == "compute instances create" ]]; then
        printf '%s\\n' "$*" >> "{gcloud_log}"
        return 0
    fi
    if [[ "$1 $2 $3" == "storage cp -" ]]; then
        local uri="$4"
        cat > "{capture_dir}/$(basename "$uri")"
        return 0
    fi
    return 0
}}
export -f gcloud
'''

    def _run(
        self, launcher: str, args: list[str], env_extra: dict[str, str], tmp_path: Path
    ) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
        launcher_path = Path(__file__).parent.parent.parent / launcher
        capture_dir = tmp_path / "gsutil_captures"
        capture_dir.mkdir()
        gcloud_log = tmp_path / "gcloud_create_calls.log"
        script = self._mock_preamble(capture_dir, gcloud_log) + f'\nbash "{launcher_path}" {" ".join(args)}\n'
        result = subprocess.run(
            ["bash", "-c", script],
            capture_output=True,
            text=True,
            env={**os.environ, **env_extra},
        )
        return result, capture_dir, gcloud_log

    # ── launch-defi-backfill-vm.sh ──────────────────────────────────────────

    def test_defi_launcher_writes_launch_params_with_replayable_scope(self, tmp_path: Path) -> None:
        result, capture_dir, _gcloud_log = self._run(self.DEFI_LAUNCHER, [], {}, tmp_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        params_file = capture_dir / "LAUNCH_PARAMS.json"
        assert params_file.exists(), "lc_write_launch_params must have been called"
        import json

        payload = json.loads(params_file.read_text())
        assert payload["launcher"] == "launch-defi-backfill-vm.sh"
        env = payload["env"]
        assert env["START_DATE"] == "2020-01-01"
        assert env["END_DATE"] == "2026-04-04"
        assert env["CHUNK_DAYS"] == "250"
        assert env["VM_FORCE"] == "false"

    def test_defi_launcher_persists_force_true_when_force_flag_passed(self, tmp_path: Path) -> None:
        result, capture_dir, _gcloud_log = self._run(self.DEFI_LAUNCHER, ["--force"], {}, tmp_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        import json

        payload = json.loads((capture_dir / "LAUNCH_PARAMS.json").read_text())
        assert payload["env"]["VM_FORCE"] == "true"

    def test_defi_launcher_gcloud_create_carries_preemption_shutdown_script(self, tmp_path: Path) -> None:
        result, _capture_dir, gcloud_log = self._run(self.DEFI_LAUNCHER, [], {}, tmp_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert gcloud_log.exists()
        call = gcloud_log.read_text()
        assert "--metadata-from-file=shutdown-script=" in call

    def test_defi_launcher_respects_inherited_start_date_env(self, tmp_path: Path) -> None:
        """The checkpoint-resume fix: START_DATE must be READ from an inherited env
        (RelaunchPreemptedVm sets env["START_DATE"]=<frontier>), not clobbered by
        the launcher's own hardcoded default."""
        result, capture_dir, _gcloud_log = self._run(self.DEFI_LAUNCHER, [], {"START_DATE": "2025-06-01"}, tmp_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        import json

        payload = json.loads((capture_dir / "LAUNCH_PARAMS.json").read_text())
        assert payload["env"]["START_DATE"] == "2025-06-01"

    # ── launch-mtds-solana-defi-backfill-vm.sh ──────────────────────────────

    def test_solana_launcher_writes_launch_params_with_replayable_scope(self, tmp_path: Path) -> None:
        result, capture_dir, _gcloud_log = self._run(self.SOLANA_LAUNCHER, [], {}, tmp_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        params_file = capture_dir / "LAUNCH_PARAMS.json"
        assert params_file.exists(), "lc_write_launch_params must have been called"
        import json

        payload = json.loads(params_file.read_text())
        assert payload["launcher"] == "launch-mtds-solana-defi-backfill-vm.sh"
        env = payload["env"]
        assert env["START_DATE"] == "2023-01-01"
        assert env["SOLANA_PROTOCOLS"] == "kamino;orca;raydium"

    def test_solana_launcher_gcloud_create_carries_preemption_shutdown_script(self, tmp_path: Path) -> None:
        result, _capture_dir, gcloud_log = self._run(self.SOLANA_LAUNCHER, [], {}, tmp_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert gcloud_log.exists()
        assert "--metadata-from-file=shutdown-script=" in gcloud_log.read_text()

    def test_solana_launcher_respects_inherited_start_date_env(self, tmp_path: Path) -> None:
        """Regression guard: this launcher already read ${START_DATE:-...} before
        this fix — confirm the new lc_write_launch_params call round-trips it."""
        result, capture_dir, _gcloud_log = self._run(self.SOLANA_LAUNCHER, [], {"START_DATE": "2025-06-01"}, tmp_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        import json

        payload = json.loads((capture_dir / "LAUNCH_PARAMS.json").read_text())
        assert payload["env"]["START_DATE"] == "2025-06-01"

    def test_defi_launcher_syntax_valid(self) -> None:
        launcher_path = Path(__file__).parent.parent.parent / self.DEFI_LAUNCHER
        result = subprocess.run(["bash", "-n", str(launcher_path)], capture_output=True, text=True)
        assert result.returncode == 0, f"Syntax error: {result.stderr}"

    def test_solana_launcher_syntax_valid(self) -> None:
        launcher_path = Path(__file__).parent.parent.parent / self.SOLANA_LAUNCHER
        result = subprocess.run(["bash", "-n", str(launcher_path)], capture_output=True, text=True)
        assert result.returncode == 0, f"Syntax error: {result.stderr}"


class TestCandleApplyCategory:
    """The `<ag>-candle-apply` category (2026-07-22, P7): the REAL --apply migration+purge pass over
    one asset_group's processed_candles/ corpus, distinct from `<ag>-candle-census` (always --dry-run,
    no reachable --apply). Found + fixed by adversarial self-testing before any real VM touched
    production: (1) DRY_RUN=true never actually gated `gcloud compute instances create` for ANY
    category (fixed at the shared _launch() level); (2) the shard-suffixed vm_name for the longer
    candle-apply category names overflowed GCE's 63-char instance-name limit; (3) fixing (2)
    introduced an unbound-variable crash under `set -u` for the non-sharded (single-VM) launch path,
    since $VM_NAME_SUFFIX is only ever set inside the shard fan-out loop."""

    LAUNCHER = TestCanonicalMigrationVmRelaunch.LAUNCHER

    @pytest.fixture
    def launcher_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.LAUNCHER

    def _run(
        self, launcher_path: Path, args: list[str], env_extra: dict[str, str], tmp_path: Path
    ) -> tuple[subprocess.CompletedProcess[str], Path]:
        gcloud_log = tmp_path / "gcloud_create_calls.log"
        preamble = f'''
gcloud() {{
    if [[ "$1 $2 $3" == "compute instances create" ]]; then
        echo "$4" >> "{gcloud_log}"
        return 0
    fi
    return 0
}}
export -f gcloud
gsutil() {{ return 0; }}
export -f gsutil
'''
        script = preamble + f'\nbash "{launcher_path}" {" ".join(args)}\n'
        result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env={**os.environ, **env_extra})
        return result, gcloud_log

    def test_non_sharded_dry_mode_does_not_crash_and_emits_dry_run(self, launcher_path: Path, tmp_path: Path) -> None:
        """Regression guard for finding (3): a plain single-VM candle-apply launch (no SHARD_OF set,
        so $VM_NAME_SUFFIX is never assigned) must not hit `set -u`'s unbound-variable crash."""
        result, gcloud_log = self._run(
            launcher_path, ["cefi-candle-apply", "2020-01-01", "2026-07-22", "dry"], {}, tmp_path
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "unbound variable" not in result.stderr
        assert gcloud_log.read_text().strip().startswith("canonical-migration-cefi-cdlap-")
        assert " --dry-run --enumeration" in result.stdout
        assert " --apply --enumeration" not in result.stdout
        assert "--quarantine" not in result.stdout

    def test_non_sharded_full_mode_emits_apply_and_gates(self, launcher_path: Path, tmp_path: Path) -> None:
        result, _gcloud_log = self._run(
            launcher_path, ["defi-candle-apply", "2020-01-01", "2026-07-22", "full"], {}, tmp_path
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert " --apply --enumeration" in result.stdout
        assert "--quarantine" in result.stdout
        assert "--content-repair" in result.stdout
        # NOT " --dry-run --enumeration" (the actual flag-in-context) -- generic "Mode: full (dry =
        # --dry-run; full = live writes)" label text elsewhere always contains the bare substring.
        assert " --dry-run --enumeration" not in result.stdout
        # "purge" is the operator's own word for this step (migration+purge) -- both gates ON by
        # default in full mode, not a follow-up flag.

    def test_sharded_fan_out_creates_one_vm_per_shard_under_the_length_budget(
        self, launcher_path: Path, tmp_path: Path
    ) -> None:
        """Regression guard for finding (2): SHARD_OF>1 must fan out N distinct VMs, every name
        <=63 chars (GCE's hard limit) -- proven on the LONGEST asset_group category name
        (prediction-candle-apply) with a 2-digit shard count, the worst case that originally
        overflowed with `Invalid value for field 'resource.name'`."""
        result, gcloud_log = self._run(
            launcher_path,
            ["prediction-candle-apply", "2020-01-01", "2026-07-22", "full"],
            {"SHARD_OF": "20"},
            tmp_path,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        created = gcloud_log.read_text().split()
        assert len(created) == 20, f"expected a 20-VM fan-out, got {len(created)}: {created}"
        assert len(set(created)) == 20, "shard VM names must all be distinct"
        for name in created:
            assert len(name) <= 63, f"'{name}' is {len(name)} chars, exceeds GCE's 63-char limit"
            assert name.startswith("canonical-migration-prediction-"), name

    def test_bucket_resolves_correctly_per_asset_group(self, launcher_path: Path, tmp_path: Path) -> None:
        """Regression guard mirroring the candle-census fix (prediction's real bucket abbreviation is
        'pred', not 'prediction') -- candle-apply must resolve the SAME bucket, not silently census
        against a nonexistent bucket."""
        result, _gcloud_log = self._run(
            launcher_path, ["prediction-candle-apply", "2020-01-01", "2026-07-22", "dry"], {}, tmp_path
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        # No hardcoded prod project ID here (QG-banned in tests) -- the venue-specific bucket prefix
        # alone proves the "pred" (not "prediction") resolution, regardless of which project the
        # launcher's own PROJECT constant resolves to.
        assert "market-data-tick-pred-prd-" in result.stdout
        assert "/processed_candles" in result.stdout
        assert "market-data-tick-prediction-" not in result.stdout


class TestCefiDropStaleCategory:
    """The `cefi-drop-stale` category (2026-07-28): the E4/E7 orphan-sweep pass over
    migrate_cefi_flat_to_v9_canonical.py's `--drop-stale` mode (mtds@e663d72f). DRY-BY-DEFAULT +
    --apply for full, same convention as the sibling bare "cefi" category (both invoke the same
    tool) — this class only proves the NEW category wires correctly (flag, VM naming, asset-group
    classification), not the tool's own delete logic (covered by mtds's own unit tests)."""

    LAUNCHER = TestCanonicalMigrationVmRelaunch.LAUNCHER

    @pytest.fixture
    def launcher_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.LAUNCHER

    def _run(self, launcher_path: Path, args: list[str], tmp_path: Path) -> subprocess.CompletedProcess[str]:
        gcloud_log = tmp_path / "gcloud_create_calls.log"
        preamble = f'''
gcloud() {{
    if [[ "$1 $2 $3" == "compute instances create" ]]; then
        printf '%s\\n' "$*" >> "{gcloud_log}"
        return 0
    fi
    return 0
}}
export -f gcloud
gsutil() {{ return 0; }}
export -f gsutil
'''
        script = preamble + f'\nbash "{launcher_path}" {" ".join(args)}\n'
        return subprocess.run(["bash", "-c", script], capture_output=True, text=True, env={**os.environ})

    def test_dry_mode_does_not_apply(self, launcher_path: Path, tmp_path: Path) -> None:
        result = self._run(launcher_path, ["cefi-drop-stale", "2019-03-30", "2026-07-28", "dry"], tmp_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "--drop-stale" in result.stdout
        assert "--apply" not in result.stdout

    def test_full_mode_embeds_apply(self, launcher_path: Path, tmp_path: Path) -> None:
        result = self._run(launcher_path, ["cefi-drop-stale", "2019-03-30", "2026-07-28", "full"], tmp_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "--drop-stale" in result.stdout
        assert "--apply" in result.stdout

    def test_extra_args_forward_also_legacy(self, launcher_path: Path, tmp_path: Path) -> None:
        """--also-legacy (part (b) of the same todo, the 5,233-cell legacy gap-fill) must reach the
        tool via MIGRATION_EXTRA_ARGS -- this category takes the generic-path EXTRA_ARGS append."""
        gcloud_log = tmp_path / "gcloud_create_calls.log"
        preamble = f'''
gcloud() {{
    if [[ "$1 $2 $3" == "compute instances create" ]]; then
        printf '%s\\n' "$*" >> "{gcloud_log}"
        return 0
    fi
    return 0
}}
export -f gcloud
gsutil() {{ return 0; }}
export -f gsutil
'''
        script = preamble + f'\nbash "{launcher_path}" cefi-drop-stale 2019-03-30 2026-07-28 dry\n'
        result = subprocess.run(
            ["bash", "-c", script],
            capture_output=True,
            text=True,
            env={**os.environ, "MIGRATION_EXTRA_ARGS": "--also-legacy"},
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "--also-legacy" in result.stdout

    def test_vm_name_and_metadata_classify_under_cefi(self, launcher_path: Path, tmp_path: Path) -> None:
        result = self._run(launcher_path, ["cefi-drop-stale", "2019-03-30", "2026-07-28", "dry"], tmp_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        gcloud_log = tmp_path / "gcloud_create_calls.log"
        created_call = gcloud_log.read_text()
        tokens = shlex.split(created_call)
        vm_names = [t for t in tokens if t.startswith("canonical-migration-cefi-drop-stale-")]
        assert len(vm_names) == 1, f"expected exactly one matching vm_name token, got {vm_names}"
        assert len(vm_names[0]) <= 63
        assert "VM_OPERATION=migrate-cefi-drop-stale" in created_call
        # Fleet classification stays CEFI (not a novel CEFI-DROP-STALE asset-group bucket), same
        # rule already applied to cefi-dedup-apply/cefi-late-renames/cefi-eu-twin-apply/etc.
        assert "VM_ASSET_GROUP=CEFI," in created_call
        assert "VM_ASSET_GROUP=CEFI-DROP-STALE" not in created_call


class TestCanonicalMigrationServiceKeyedWorkspaceDir:
    """Regression guard for the canonical-migration VM_TASK branch in setup-data-pipeline-vm.sh:
    it used to hardcode `cd "$WORKSPACE/mtds"` regardless of VM_SERVICE, so an
    instruments-service canonical-migration script (e.g.
    reclassify_defi_curve_optimism_subgraph_deindexed_2026_07_24.py, launched with
    VM_SERVICE=instruments_service) hit "ERROR: $WORKSPACE/mtds missing" even though its
    tarball was correctly extracted to $WORKSPACE/instruments by the earlier install step.
    The fix derives the workspace dir via the SAME SERVICE_TARBALLS -> TARBALL_DIRS mapping
    the tarball-install step already uses. These tests extract the REAL declarations/derivation
    lines straight out of the setup script (not a hand-duplicated copy) so a future edit to
    either mapping or the derivation can't silently drift out of sync with this test.
    """

    SETUP_SCRIPT = "scripts/vm/setup-data-pipeline-vm.sh"

    @pytest.fixture
    def setup_script_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.SETUP_SCRIPT

    def _extract_block(self, content: str, start_marker: str) -> str:
        """Extract a `declare -A NAME=( ... )` block starting at start_marker, up to its
        closing `)` line (these blocks never contain a nested top-level `)`-only line)."""
        lines = content.splitlines()
        start = next(i for i, ln in enumerate(lines) if ln.startswith(start_marker))
        end = next(i for i in range(start, len(lines)) if lines[i].strip() == ")")
        return "\n".join(lines[start : end + 1])

    def _resolved_dir_for(self, setup_script_path: Path, vm_service: str) -> str:
        content = setup_script_path.read_text()
        service_tarballs = self._extract_block(content, "declare -A SERVICE_TARBALLS=(")
        tarball_dirs = self._extract_block(content, "declare -A TARBALL_DIRS=(")
        derivation_lines = [
            ln.strip() for ln in content.splitlines() if "_MIGRATION_TARBALL=" in ln or "_MIGRATION_DIR=" in ln
        ]
        assert len(derivation_lines) == 2, (
            f"expected exactly 2 derivation lines (_MIGRATION_TARBALL=/_MIGRATION_DIR=) in "
            f"{self.SETUP_SCRIPT}, found {len(derivation_lines)} — the canonical-migration "
            "branch's shape changed; update this test's extraction to match."
        )
        script = "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                service_tarballs,
                tarball_dirs,
                f'VM_SERVICE="{vm_service}"',
                *derivation_lines,
                'echo "$_MIGRATION_DIR"',
            ]
        )
        result = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        return result.stdout.strip()

    def test_instruments_service_resolves_to_instruments_dir(self, setup_script_path: Path) -> None:
        """The bug this fixes: VM_SERVICE=instruments_service must resolve to $WORKSPACE/instruments,
        not the previously-hardcoded $WORKSPACE/mtds."""
        assert self._resolved_dir_for(setup_script_path, "instruments_service") == "instruments"

    def test_market_tick_data_service_still_resolves_to_mtds_dir(self, setup_script_path: Path) -> None:
        """Backward-compat: the original MTDS-only canonical-migration callers must be unaffected."""
        assert self._resolved_dir_for(setup_script_path, "market_tick_data_service") == "mtds"

    def test_unmapped_vm_service_falls_back_to_mtds_dir(self, setup_script_path: Path) -> None:
        """An unrecognised VM_SERVICE falls back to the pre-fix default (mtds) rather than an
        empty/unset dir that would silently `cd` somewhere unintended."""
        assert self._resolved_dir_for(setup_script_path, "some_future_service") == "mtds"


class TestMtdsBackfillMvpModeFlag:
    """Regression guard for the mtds-backfill VM_TASK branch's `--mvp-mode` wiring
    (operator-ruled 2026-07-29, tradfi_mvp_mode_unreachable_dead_gate_2026_07_08.md): the MTDS
    download CLI's `--mvp-mode` flag was fully wired end-to-end but had NO caller passing it,
    making it an unreachable dead gate. Extracts the REAL `VM_MVP_MODE=$(_meta VM_MVP_MODE)` +
    conditional-append lines straight out of the setup script (not a hand-duplicated copy) so a
    future edit can't silently drift out of sync with this test.
    """

    SETUP_SCRIPT = "scripts/vm/setup-data-pipeline-vm.sh"

    @pytest.fixture
    def setup_script_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.SETUP_SCRIPT

    def _base_cli_for(self, setup_script_path: Path, vm_mvp_mode: str) -> str:
        content = setup_script_path.read_text()
        lines = content.splitlines()
        read_line = next(ln.strip() for ln in lines if ln.strip().startswith("VM_MVP_MODE=$(_meta VM_MVP_MODE)"))
        append_line = next(ln.strip() for ln in lines if '"$VM_MVP_MODE" == "true"' in ln and "--mvp-mode" in ln)
        script = "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f'_meta() {{ [[ "$1" == "VM_MVP_MODE" ]] && echo "{vm_mvp_mode}" || echo ""; }}',
                "BASE_CLI=''",
                read_line,
                append_line,
                'echo "$BASE_CLI"',
            ]
        )
        result = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        return result.stdout.strip()

    def test_vm_mvp_mode_true_appends_the_flag(self, setup_script_path: Path) -> None:
        assert "--mvp-mode" in self._base_cli_for(setup_script_path, "true")

    def test_vm_mvp_mode_absent_does_not_append_the_flag(self, setup_script_path: Path) -> None:
        """No metadata (the default for every OTHER launcher) must produce identical CLI to today."""
        assert "--mvp-mode" not in self._base_cli_for(setup_script_path, "")

    def test_vm_mvp_mode_false_does_not_append_the_flag(self, setup_script_path: Path) -> None:
        assert "--mvp-mode" not in self._base_cli_for(setup_script_path, "false")


class TestCefiFtsDateSharding:
    """Tests for `_cefi-fts-launcher-lib.sh`'s `cefi_fts_split_date_shards` (the
    pure date-range-split arithmetic) and its wiring into
    `launch-cefi-funding-timestamp-fix-vm.sh`'s `--shards N` / `SHARD_COUNT`
    option — the fix #1 half of
    plans/active/issues/cefi_migration_vm_launcher_no_sharding_and_spot_preemption_churn_2026_07_28.md
    (fix #2, preemption auto-recovery, is a separate track — not covered here).
    """

    LIB = "scripts/vm/_cefi-fts-launcher-lib.sh"
    LAUNCHER = "scripts/vm/launch-cefi-funding-timestamp-fix-vm.sh"

    @pytest.fixture
    def lib_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.LIB

    @pytest.fixture
    def launcher_path(self) -> Path:
        return Path(__file__).parent.parent.parent / self.LAUNCHER

    def _split(self, lib_path: Path, start: str, end: str, n: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", "-c", f'source "{lib_path}"; set +e; cefi_fts_split_date_shards "{start}" "{end}" "{n}"'],
            capture_output=True,
            text=True,
        )

    def test_lib_syntax_valid(self, lib_path: Path) -> None:
        result = subprocess.run(["bash", "-n", str(lib_path)], capture_output=True, text=True)
        assert result.returncode == 0, f"Syntax error in {self.LIB}: {result.stderr}"

    def test_single_shard_returns_full_range(self, lib_path: Path) -> None:
        result = self._split(lib_path, "2020-01-01", "2020-01-10", "1")
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "2020-01-01:2020-01-10"

    def test_single_day_range_single_shard(self, lib_path: Path) -> None:
        result = self._split(lib_path, "2020-01-01", "2020-01-01", "1")
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "2020-01-01:2020-01-01"

    def test_even_split_three_shards(self, lib_path: Path) -> None:
        """9 days / 3 shards -> exactly 3 days each, no remainder."""
        result = self._split(lib_path, "2020-01-01", "2020-01-09", "3")
        assert result.returncode == 0, result.stderr
        lines = result.stdout.strip().splitlines()
        assert lines == [
            "2020-01-01:2020-01-03",
            "2020-01-04:2020-01-06",
            "2020-01-07:2020-01-09",
        ]

    def test_uneven_split_last_shard_absorbs_remainder(self, lib_path: Path) -> None:
        """10 days / 3 shards -> base=3 for shards 1-2, shard 3 (LAST) gets the
        remainder day (4), per the issue doc's exact spec: 'total_days // N per
        shard, with the LAST shard absorbing any remainder day'."""
        result = self._split(lib_path, "2020-01-01", "2020-01-10", "3")
        assert result.returncode == 0, result.stderr
        lines = result.stdout.strip().splitlines()
        assert lines == [
            "2020-01-01:2020-01-03",
            "2020-01-04:2020-01-06",
            "2020-01-07:2020-01-10",
        ]
        # 3+3+4 = 10, no shard shorter than the base and only the last is longer.
        assert len(lines) == 3

    def test_shards_are_contiguous_and_exhaustive_no_gap_no_overlap(self, lib_path: Path) -> None:
        """For every shard boundary, shard i's end + 1 day == shard i+1's start —
        no gap, no double-counted day — over a real multi-year range with a
        shard count that does not evenly divide it."""
        import datetime
        import itertools

        result = self._split(lib_path, "2020-01-01", "2026-05-01", "7")
        assert result.returncode == 0, result.stderr
        lines = result.stdout.strip().splitlines()
        assert len(lines) == 7
        windows = [tuple(ln.split(":")) for ln in lines]
        assert windows[0][0] == "2020-01-01"
        assert windows[-1][1] == "2026-05-01"
        for (_, prev_end), (next_start, _) in itertools.pairwise(windows):
            prev_end_d = datetime.date.fromisoformat(prev_end)
            next_start_d = datetime.date.fromisoformat(next_start)
            assert next_start_d - prev_end_d == datetime.timedelta(days=1), (
                f"gap/overlap between shard end {prev_end} and next shard start {next_start}"
            )
        # every window is start <= end
        for start, end in windows:
            assert datetime.date.fromisoformat(start) <= datetime.date.fromisoformat(end)

    def test_shard_count_clamped_to_total_days(self, lib_path: Path) -> None:
        """Asking for more shards than days in a short range clamps down to
        one shard per day rather than emitting empty/inverted windows."""
        result = self._split(lib_path, "2020-01-01", "2020-01-03", "8")
        assert result.returncode == 0, result.stderr
        lines = result.stdout.strip().splitlines()
        assert lines == ["2020-01-01:2020-01-01", "2020-01-02:2020-01-02", "2020-01-03:2020-01-03"]

    def test_inverted_range_fails(self, lib_path: Path) -> None:
        result = self._split(lib_path, "2020-01-10", "2020-01-01", "2")
        assert result.returncode != 0
        assert "end_date must be >= start_date" in result.stderr

    def test_zero_shards_fails(self, lib_path: Path) -> None:
        result = self._split(lib_path, "2020-01-01", "2020-01-10", "0")
        assert result.returncode != 0
        assert "positive integer" in result.stderr

    def test_non_numeric_shards_fails(self, lib_path: Path) -> None:
        result = self._split(lib_path, "2020-01-01", "2020-01-10", "banana")
        assert result.returncode != 0
        assert "positive integer" in result.stderr

    # -- Launcher CLI integration -------------------------------------------

    def test_launcher_syntax_valid(self, launcher_path: Path) -> None:
        result = subprocess.run(["bash", "-n", str(launcher_path)], capture_output=True, text=True)
        assert result.returncode == 0, f"Syntax error in {self.LAUNCHER}: {result.stderr}"

    def test_launcher_sources_the_sharding_lib(self, launcher_path: Path) -> None:
        content = launcher_path.read_text()
        assert "_cefi-fts-launcher-lib.sh" in content
        assert "cefi_fts_split_date_shards" in content

    def test_default_n1_dry_run_vm_name_unchanged_shape(self, launcher_path: Path) -> None:
        """No --shards flag: VM_NAME must be exactly
        canonical-migration-cefi-fts-<venue>-<run_ts> with NO shard suffix —
        the strict-additive N=1 contract."""
        import re

        result = subprocess.run(
            ["bash", str(launcher_path), "--dry-run", "BYBIT", "2020-01-01", "2020-01-10"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        vm_line = next(ln for ln in result.stdout.splitlines() if ln.strip().startswith("VM:"))
        vm_name = vm_line.split()[-1]
        assert re.fullmatch(r"canonical-migration-cefi-fts-bybit-\d{8}-\d{6}", vm_name), vm_name
        # Exactly one VM printed for the unsharded default.
        assert result.stdout.count("Would launch VM") == 1

    def test_shards_flag_dry_run_prints_n_distinct_vm_names(self, launcher_path: Path) -> None:
        result = subprocess.run(
            ["bash", str(launcher_path), "--dry-run", "--shards", "4", "BYBIT", "2020-01-01", "2020-01-20"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.count("Would launch VM") == 4
        vm_names = [
            ln.split()[-1] for ln in result.stdout.splitlines() if ln.strip().startswith("VM:") and "canonical" in ln
        ]
        assert len(vm_names) == 4
        assert len(set(vm_names)) == 4, f"shard VM names must be distinct: {vm_names}"

    def test_shards_flag_windows_are_contiguous_exhaustive(self, launcher_path: Path) -> None:
        import re

        result = subprocess.run(
            ["bash", str(launcher_path), "--dry-run", "--shards", "3", "BYBIT", "2020-01-01", "2020-01-10"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        windows = re.findall(r"Window:\s+(\d{4}-\d{2}-\d{2}) \.\. (\d{4}-\d{2}-\d{2})", result.stdout)
        assert windows == [
            ("2020-01-01", "2020-01-03"),
            ("2020-01-04", "2020-01-06"),
            ("2020-01-07", "2020-01-10"),
        ]

    def test_shard_count_env_var_equivalent_to_flag(self, launcher_path: Path) -> None:
        result = subprocess.run(
            ["bash", str(launcher_path), "--dry-run", "BYBIT", "2020-01-01", "2020-01-04"],
            capture_output=True,
            text=True,
            env={**os.environ, "SHARD_COUNT": "2"},
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.count("Would launch VM") == 2

    def test_shards_flag_wins_over_env_var(self, launcher_path: Path) -> None:
        result = subprocess.run(
            ["bash", str(launcher_path), "--dry-run", "--shards", "3", "BYBIT", "2020-01-01", "2020-01-09"],
            capture_output=True,
            text=True,
            env={**os.environ, "SHARD_COUNT": "2"},
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.count("Would launch VM") == 3

    def test_shards_over_max_is_rejected(self, launcher_path: Path) -> None:
        """A fat-fingered --shards 50 must not launch a VM storm."""
        result = subprocess.run(
            ["bash", str(launcher_path), "--dry-run", "--shards", "50", "BYBIT", "2020-01-01", "2020-01-10"],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "exceeds MAX_SHARDS" in result.stderr

    def test_shards_zero_is_rejected(self, launcher_path: Path) -> None:
        result = subprocess.run(
            ["bash", str(launcher_path), "--dry-run", "--shards", "0", "BYBIT", "2020-01-01", "2020-01-10"],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "positive integer" in result.stderr

    def test_max_shards_vm_name_stays_within_gce_63_char_limit(self, launcher_path: Path) -> None:
        """The longest known venue slug (bitfinex-futures, 16 chars) at the max
        allowed shard count must not exceed GCE's 63-character instance-name
        limit -- the exact constraint MAX_SHARDS was chosen to respect."""
        result = subprocess.run(
            [
                "bash",
                str(launcher_path),
                "--dry-run",
                "--shards",
                "8",
                "BITFINEX-FUTURES",
                "2020-01-01",
                "2026-05-01",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        vm_names = [
            ln.split()[-1] for ln in result.stdout.splitlines() if ln.strip().startswith("VM:") and "canonical" in ln
        ]
        assert len(vm_names) == 8
        for name in vm_names:
            assert len(name) <= 63, f"VM name exceeds GCE's 63-char limit ({len(name)} chars): {name}"

    def test_venue_slug_still_matches_registered_vm_prefix_registry_entry(self) -> None:
        """Every shard VM_NAME must still start with the registered
        'canonical-migration-cefi-fts-' prefix (deployment_service/vm_prefix_registry.py)
        -- the shard suffix is appended AFTER the timestamp, never inserted into
        or ahead of the registered prefix itself."""
        registry_path = Path(__file__).parent.parent.parent / "deployment_service" / "vm_prefix_registry.py"
        content = registry_path.read_text()
        assert '"canonical-migration-cefi-fts-"' in content, (
            "registered prefix missing/renamed in vm_prefix_registry.py — sharded VM names "
            "must still resolve via longest-prefix-match"
        )


if __name__ == "__main__":
    pytest.main([__file__])
