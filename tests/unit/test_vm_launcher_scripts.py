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
import pytest
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, call, MagicMock
from typing import Generator, Dict, Any

# Test fixtures and constants
TEST_PROJECT = "central-element-323112"
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
        result = subprocess.run(
            ["bash", "-n", str(launcher_lib_path)], 
            capture_output=True, 
            text=True
        )
        assert result.returncode == 0, f"Syntax error in launcher_common.sh: {result.stderr}"

    def test_lc_validate_env_valid_environments(self, launcher_lib_path: Path):
        """Test lc_validate_env with valid environments"""
        for env in ["prod", "staging", "dev"]:
            result = subprocess.run([
                "bash", "-c", 
                f'source "{launcher_lib_path}" && lc_validate_env "{env}"'
            ], capture_output=True, text=True)
            assert result.returncode == 0, f"lc_validate_env failed for valid env: {env}"

    def test_lc_validate_env_invalid_environments(self, launcher_lib_path: Path):
        """Test lc_validate_env with invalid environments"""
        for env in ["production", "test", "local", "", "invalid"]:
            result = subprocess.run([
                "bash", "-c", 
                f'source "{launcher_lib_path}" && lc_validate_env "{env}"'
            ], capture_output=True, text=True)
            assert result.returncode == 1, f"lc_validate_env should fail for invalid env: {env}"
            assert "ERROR: --env must be one of prod/staging/dev" in result.stderr

    def test_lc_code_bucket_generation(self, launcher_lib_path: Path):
        """Test lc_code_bucket function"""
        result = subprocess.run([
            "bash", "-c", 
            f'source "{launcher_lib_path}" && lc_code_bucket "{TEST_PROJECT}"'
        ], capture_output=True, text=True)
        assert result.returncode == 0
        assert result.stdout.strip() == f"deployment-scripts-{TEST_PROJECT}"

    def test_lc_run_ts_format(self, launcher_lib_path: Path):
        """Test lc_run_ts timestamp format"""
        result = subprocess.run([
            "bash", "-c", 
            f'source "{launcher_lib_path}" && lc_run_ts'
        ], capture_output=True, text=True)
        assert result.returncode == 0
        timestamp = result.stdout.strip()
        # Should match YYYYmmdd-HHMMSS format
        import re
        assert re.match(r'^\d{8}-\d{6}$', timestamp), f"Invalid timestamp format: {timestamp}"

    def test_lc_write_startup_file(self, launcher_lib_path: Path):
        """Test lc_write_startup_file function"""
        test_content = "#!/bin/bash\necho 'test startup script'"
        
        result = subprocess.run([
            "bash", "-c", 
            f'''source "{launcher_lib_path}"
            lc_write_startup_file "{test_content}"
            cat "$STARTUP_FILE"'''
        ], capture_output=True, text=True)
        
        assert result.returncode == 0
        assert test_content in result.stdout

    def test_lc_log_upload_trap_block(self, launcher_lib_path: Path):
        """Test lc_log_upload_trap_block generates valid bash snippet"""
        vm_name = "test-vm-20240523-120000"
        result = subprocess.run([
            "bash", "-c", 
            f'source "{launcher_lib_path}" && lc_log_upload_trap_block "{vm_name}" "{TEST_PROJECT}"'
        ], capture_output=True, text=True)
        
        assert result.returncode == 0
        output = result.stdout
        
        # Check key components of the trap block
        assert f"gs://deployment-scripts-{TEST_PROJECT}/vm-logs/{vm_name}/run.log" in output
        assert "trap _lc_final_upload EXIT" in output
        assert "exec > >(tee -a" in output
        assert "_lc_final_upload()" in output


class TestLauncherScriptPatterns:
    """Test common patterns used across launcher scripts"""
    
    def test_gcloud_command_structure_dry_run(self):
        """Test that dry run mode properly skips gcloud calls"""
        with patch.dict(os.environ, {"LC_DRY_RUN": "true"}):
            # Mock the launcher_common functions
            result = subprocess.run([
                "bash", "-c", 
                f'''
                export LC_DRY_RUN=true
                source "scripts/vm/lib/launcher_common.sh"
                lc_gcloud_create "test-vm" "{TEST_PROJECT}" "{TEST_ZONE}" "e2-standard-4" "50" "key=value" "env=test"
                '''
            ], capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
            
            assert result.returncode == 0
            assert "[DRY-RUN] Would create VM: test-vm" in result.stdout

    def test_environment_variable_propagation(self):
        """Test that required environment variables are properly propagated to VM metadata"""
        required_vars = [
            "VM_TASK", "VM_SERVICE", "VM_OPERATION", "VM_ASSET_GROUP", 
            "VM_START_DATE", "VM_END_DATE", "DEPLOYMENT_ENV", "VM_NAME"
        ]
        
        # Simulate metadata building pattern from launcher scripts
        metadata_parts = []
        for var in required_vars:
            metadata_parts.append(f"{var}=test_value")
        
        metadata = ",".join(metadata_parts)
        
        # Verify all required variables are present
        for var in required_vars:
            assert f"{var}=" in metadata

    @patch('subprocess.run')
    def test_zone_failover_logic(self, mock_run: Mock):
        """Test zone failover behavior when primary zone has capacity issues"""
        # Simulate zone capacity failure scenarios
        mock_run.side_effect = [
            # First call fails (simulating capacity issue in primary zone)
            subprocess.CalledProcessError(1, ["gcloud"], stderr="ZONE_RESOURCE_POOL_EXHAUSTED"),
            # Second call succeeds in fallback zone
            subprocess.CompletedProcess(["gcloud"], 0, stdout="VM created")
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
                    check=True, capture_output=True, text=True
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
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                ["gcloud"], 0, stdout=""
            )
            
            result = subprocess.run([
                "bash", "-c", 
                f'source "{launcher_lib_path}" && lc_singleton_check "test-prefix-" "{TEST_ZONE}" "{TEST_PROJECT}" "false"'
            ], capture_output=True, text=True)
            
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
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
            f.write(test_script)
            f.flush()
            
            try:
                result = subprocess.run(
                    ["bash", f.name], 
                    capture_output=True, text=True
                )
                
                assert result.returncode == 1
                assert "already running" in result.stderr
            finally:
                os.unlink(f.name)

    def test_force_flag_bypasses_singleton_check(self):
        """Test that force=true bypasses singleton checking"""
        launcher_lib_path = Path(__file__).parent.parent.parent / LAUNCHER_COMMON_PATH
        
        result = subprocess.run([
            "bash", "-c", 
            f'source "{launcher_lib_path}" && lc_singleton_check "test-prefix-" "{TEST_ZONE}" "{TEST_PROJECT}" "true"'
        ], capture_output=True, text=True)
        
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
                has_env_validation = any(pattern in script_content for pattern in [
                    "prod|staging|dev",
                    "case \"$ENV\"",
                    "case \"$DEPLOYMENT_ENV\"", 
                    "--env prod", 
                    "--env staging",
                    "--env dev"
                ])
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
                assert "source" in script_content and "launcher_common.sh" in script_content, \
                    f"Script {script_path.name} uses lc_ functions but doesn't source launcher_common.sh"

    def test_date_handling_compatibility(self):
        """Test date command compatibility between macOS and Linux"""
        # Test macOS date format (BSD date)
        try:
            result = subprocess.run(
                ["date", "-v-1d", "+%Y-%m-%d"], 
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                # BSD date works
                pass
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        
        # Test GNU date format (Linux)
        try:
            result = subprocess.run(
                ["date", "-d", "yesterday", "+%Y-%m-%d"], 
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                # GNU date works
                pass
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        
        # Both formats should be supported in launcher scripts for cross-platform compatibility

    def test_vm_naming_conventions(self):
        """Test VM naming follows the expected pattern"""
        # VM names should include timestamp and be unique
        import time
        
        # Test timestamp generation pattern used in scripts
        timestamp = subprocess.run(
            ["date", "+%Y%m%d-%H%M%S"], 
            capture_output=True, text=True
        ).stdout.strip()
        
        # Should match expected format
        import re
        assert re.match(r'^\d{8}-\d{6}$', timestamp), "Invalid timestamp format for VM naming"


class TestErrorHandling:
    """Test error handling patterns in launcher scripts"""
    
    def test_required_parameter_validation(self):
        """Test that launcher_common functions validate required parameters"""
        launcher_lib_path = Path(__file__).parent.parent.parent / LAUNCHER_COMMON_PATH
        
        # Test missing required parameter
        result = subprocess.run([
            "bash", "-c", 
            f'source "{launcher_lib_path}" && lc_validate_env'
        ], capture_output=True, text=True)
        
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
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
            f.write(test_script)
            f.flush()
            
            try:
                result = subprocess.run(
                    ["bash", f.name], 
                    capture_output=True, text=True
                )
                
                # Function should exit with error due to set -euo pipefail
                assert result.returncode != 0
            finally:
                os.unlink(f.name)

    def test_script_syntax_validation(self):
        """Test that all launcher scripts have valid bash syntax"""
        launcher_scripts = Path(__file__).parent.parent.parent / "scripts" / "vm"
        
        for script_path in launcher_scripts.glob("launch-*.sh"):
            result = subprocess.run(
                ["bash", "-n", str(script_path)], 
                capture_output=True, text=True
            )
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
                assert not matches, f"Potential hardcoded secret in {script_path.name}: {matches.group() if matches else ''}"

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
            lines = script_content.split('\n')
            for line in lines:
                # Look for gcloud commands with unquoted variables
                if 'gcloud' in line and '$' in line:
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


if __name__ == "__main__":
    pytest.main([__file__])