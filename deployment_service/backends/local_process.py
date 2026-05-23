"""Local process backend — runs services as subprocesses on the developer machine.

Used for Tier 2 (full fleet on localhost). Each service is started as:
  cd <workspace>/<service-repo> && .venv/bin/python -m <module> --operation <op> --mode <mode> --asset-group <cat>

For batch: subprocess runs to completion, exit code = success/failure.
For live: subprocess runs in background, monitored via health endpoint.
"""

from __future__ import annotations

import logging
import signal
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

from ..events import VMEventType
from .base import ComputeBackend, JobInfo, JobStatus

logger = logging.getLogger(__name__)

# Graceful shutdown timeout before SIGKILL (seconds)
_SIGTERM_GRACE_PERIOD = 10

# Default batch timeout (seconds)
_BATCH_TIMEOUT = 600

# Health check timeout (seconds)
_HEALTH_CHECK_TIMEOUT = 5


class _TrackedProcess:
    """Internal bookkeeping for a launched subprocess."""

    __slots__ = (
        "shard_id",
        "process",
        "start_time",
        "end_time",
        "log_path",
        "pid_path",
        "is_live",
        "health_url",
        "error_message",
    )

    def __init__(
        self,
        *,
        shard_id: str,
        process: subprocess.Popen[bytes],
        start_time: datetime,
        log_path: Path,
        pid_path: Path,
        is_live: bool,
        health_url: str | None,
    ) -> None:
        self.shard_id = shard_id
        self.process = process
        self.start_time = start_time
        self.end_time: datetime | None = None
        self.log_path = log_path
        self.pid_path = pid_path
        self.is_live = is_live
        self.health_url = health_url
        self.error_message: str | None = None


class LocalProcessBackend(ComputeBackend):
    """Compute backend that runs services as local subprocesses.

    Designed for Tier 2 local development — full fleet on localhost.

    Args:
        workspace_root: Absolute path to the multi-repo workspace
            (e.g. ``/Users/.../unified-trading-system-repos``).
        port_map: Mapping of ``service-repo-name`` to localhost port.
            Used to build health-check URLs for live-mode services.
        batch_timeout: Max seconds a batch subprocess may run before
            being killed (default 600).
    """

    def __init__(
        self,
        workspace_root: str,
        port_map: dict[str, int] | None = None,
        batch_timeout: int = _BATCH_TIMEOUT,
    ) -> None:
        # ComputeBackend.__init__ expects project_id, region, service_account_email.
        # For local dev these are placeholder values — no cloud interaction.
        super().__init__(
            project_id="local",
            region="localhost",
            service_account_email="local@localhost",
        )
        self._workspace_root = Path(workspace_root)
        self._port_map: dict[str, int] = port_map or {}
        self._batch_timeout = batch_timeout

        # Ensure cache directories exist
        self._cache_root = self._workspace_root / ".local-dev-cache"
        self._pids_dir = self._cache_root / "pids"
        self._logs_dir = self._cache_root / "logs"
        self._pids_dir.mkdir(parents=True, exist_ok=True)
        self._logs_dir.mkdir(parents=True, exist_ok=True)

        # Tracked processes keyed by job_id (= shard_id for local backend)
        self._processes: dict[str, _TrackedProcess] = {}

    # ------------------------------------------------------------------
    # ComputeBackend interface
    # ------------------------------------------------------------------

    @property
    def backend_type(self) -> str:
        return "local_process"

    def deploy_shard(
        self,
        shard_id: str,
        docker_image: str,
        args: list[str],
        environment_variables: dict[str, str],
        compute_config: dict[str, object],
        labels: dict[str, str],
    ) -> JobInfo:
        """Launch a service as a local subprocess.

        ``docker_image`` is ignored — the service is run directly from
        the workspace checkout.  ``compute_config`` may contain:

        * ``service_repo`` (str) — repo directory name (e.g. ``instruments-service``).
          Falls back to ``shard_id`` if absent.
        * ``module`` (str) — Python module to run (e.g. ``instruments_service``).
          Falls back to ``service_repo`` with hyphens replaced by underscores.
        * ``is_live`` (bool) — whether this is a long-running service (default False).
        """
        service_repo = str(compute_config.get("service_repo", shard_id))
        module = str(compute_config.get("module", service_repo.replace("-", "_")))
        is_live = bool(compute_config.get("is_live", False))

        repo_dir = self._workspace_root / service_repo
        venv_python = repo_dir / ".venv" / "bin" / "python"

        if not repo_dir.is_dir():
            msg = f"Service repo not found: {repo_dir}"
            logger.error("%s", msg)
            return JobInfo(
                job_id=shard_id,
                shard_id=shard_id,
                status=JobStatus.FAILED,
                start_time=datetime.now(UTC),
                end_time=datetime.now(UTC),
                error_message=msg,
            )

        if not venv_python.is_file():
            msg = f"Venv python not found: {venv_python}"
            logger.error("%s", msg)
            return JobInfo(
                job_id=shard_id,
                shard_id=shard_id,
                status=JobStatus.FAILED,
                start_time=datetime.now(UTC),
                end_time=datetime.now(UTC),
                error_message=msg,
            )

        # Build command: .venv/bin/python -m <module> <args...>
        cmd: list[str] = [str(venv_python), "-m", module, *args]

        # Merge environment: inherit parent, overlay service-specific vars
        env: dict[str, str] = {
            "CLOUD_PROVIDER": "local",
            **environment_variables,
        }

        log_path = self._logs_dir / f"{shard_id}.log"
        pid_path = self._pids_dir / f"{shard_id}.pid"

        # Determine health URL for live services
        health_url: str | None = None
        if is_live:
            port = self._port_map.get(service_repo)
            if port is not None:
                health_url = f"http://localhost:{port}/health"

        start_time = datetime.now(UTC)
        self._emit_event(shard_id, _JOB_STARTED, f"Launching: {' '.join(cmd)}")

        if is_live:
            return self._launch_live(
                shard_id=shard_id,
                cmd=cmd,
                env=env,
                cwd=repo_dir,
                log_path=log_path,
                pid_path=pid_path,
                start_time=start_time,
                health_url=health_url,
            )

        return self._run_batch(
            shard_id=shard_id,
            cmd=cmd,
            env=env,
            cwd=repo_dir,
            log_path=log_path,
            pid_path=pid_path,
            start_time=start_time,
        )

    def get_status(self, job_id: str) -> JobInfo:
        """Return current status of a tracked subprocess."""
        tracked = self._processes.get(job_id)
        if tracked is None:
            return JobInfo(
                job_id=job_id,
                shard_id=job_id,
                status=JobStatus.UNKNOWN,
                error_message=f"No tracked process for job_id={job_id}",
            )

        status = self._resolve_status(tracked)
        return JobInfo(
            job_id=job_id,
            shard_id=tracked.shard_id,
            status=status,
            start_time=tracked.start_time,
            end_time=tracked.end_time,
            error_message=tracked.error_message,
            logs_url=str(tracked.log_path),
            metadata={"pid": str(tracked.process.pid)},
        )

    def cancel_job(self, job_id: str) -> bool:
        """Send SIGTERM then SIGKILL to a running subprocess."""
        tracked = self._processes.get(job_id)
        if tracked is None:
            return False

        if tracked.process.poll() is not None:
            # Already exited
            return False

        logger.info("Sending SIGTERM to %s (pid=%d)", job_id, tracked.process.pid)
        tracked.process.send_signal(signal.SIGTERM)

        try:
            tracked.process.wait(timeout=_SIGTERM_GRACE_PERIOD)
        except subprocess.TimeoutExpired:
            logger.warning(
                "SIGTERM timeout for %s (pid=%d), sending SIGKILL",
                job_id,
                tracked.process.pid,
            )
            tracked.process.kill()
            tracked.process.wait(timeout=5)

        tracked.end_time = datetime.now(UTC)
        self._emit_event(
            tracked.shard_id,
            _JOB_CANCELLED,
            f"Cancelled job {job_id} (pid={tracked.process.pid})",
        )

        # Clean up PID file
        if tracked.pid_path.exists():
            tracked.pid_path.unlink()

        return True

    def get_logs_url(self, job_id: str) -> str:
        """Return local log file path for the job."""
        tracked = self._processes.get(job_id)
        if tracked is not None:
            return str(tracked.log_path)
        return str(self._logs_dir / f"{job_id}.log")

    def list_jobs(self) -> list[JobInfo]:
        """Return status of all tracked processes."""
        return [self.get_status(job_id) for job_id in self._processes]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _launch_live(
        self,
        *,
        shard_id: str,
        cmd: list[str],
        env: dict[str, str],
        cwd: Path,
        log_path: Path,
        pid_path: Path,
        start_time: datetime,
        health_url: str | None,
    ) -> JobInfo:
        """Start a long-running (live) service as a background subprocess."""
        log_file = log_path.open("w")
        process = subprocess.Popen(
            cmd,
            cwd=cwd,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

        # Write PID file
        pid_path.write_text(str(process.pid))

        tracked = _TrackedProcess(
            shard_id=shard_id,
            process=process,
            start_time=start_time,
            log_path=log_path,
            pid_path=pid_path,
            is_live=True,
            health_url=health_url,
        )
        self._processes[shard_id] = tracked

        logger.info(
            "Live service %s started (pid=%d, log=%s)",
            shard_id,
            process.pid,
            log_path,
        )

        return JobInfo(
            job_id=shard_id,
            shard_id=shard_id,
            status=JobStatus.RUNNING,
            start_time=start_time,
            logs_url=str(log_path),
            metadata={"pid": str(process.pid)},
        )

    def _run_batch(
        self,
        *,
        shard_id: str,
        cmd: list[str],
        env: dict[str, str],
        cwd: Path,
        log_path: Path,
        pid_path: Path,
        start_time: datetime,
    ) -> JobInfo:
        """Run a batch service synchronously to completion."""
        log_file = log_path.open("w")

        process = subprocess.Popen(
            cmd,
            cwd=cwd,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

        # Write PID file while running
        pid_path.write_text(str(process.pid))

        tracked = _TrackedProcess(
            shard_id=shard_id,
            process=process,
            start_time=start_time,
            log_path=log_path,
            pid_path=pid_path,
            is_live=False,
            health_url=None,
        )
        self._processes[shard_id] = tracked

        try:
            process.wait(timeout=self._batch_timeout)
        except subprocess.TimeoutExpired:
            logger.error(
                "Batch job %s timed out after %ds, killing",
                shard_id,
                self._batch_timeout,
            )
            process.kill()
            process.wait(timeout=5)
            tracked.error_message = f"Timed out after {self._batch_timeout}s"
            tracked.end_time = datetime.now(UTC)
            self._emit_event(
                shard_id,
                _JOB_FAILED,
                tracked.error_message,
            )
            # Clean up PID file
            if pid_path.exists():
                pid_path.unlink()
            return JobInfo(
                job_id=shard_id,
                shard_id=shard_id,
                status=JobStatus.FAILED,
                start_time=start_time,
                end_time=tracked.end_time,
                error_message=tracked.error_message,
                logs_url=str(log_path),
                metadata={"pid": str(process.pid)},
            )

        tracked.end_time = datetime.now(UTC)
        log_file.close()

        # Clean up PID file
        if pid_path.exists():
            pid_path.unlink()

        if process.returncode == 0:
            self._emit_event(shard_id, _JOB_COMPLETED, "Batch completed successfully")
            return JobInfo(
                job_id=shard_id,
                shard_id=shard_id,
                status=JobStatus.SUCCEEDED,
                start_time=start_time,
                end_time=tracked.end_time,
                logs_url=str(log_path),
                metadata={"pid": str(process.pid)},
            )

        tracked.error_message = f"Process exited with code {process.returncode}"
        self._emit_event(shard_id, _JOB_FAILED, tracked.error_message)
        return JobInfo(
            job_id=shard_id,
            shard_id=shard_id,
            status=JobStatus.FAILED,
            start_time=start_time,
            end_time=tracked.end_time,
            error_message=tracked.error_message,
            logs_url=str(log_path),
            metadata={"pid": str(process.pid)},
        )

    def _resolve_status(self, tracked: _TrackedProcess) -> JobStatus:
        """Determine the current status of a tracked process."""
        rc = tracked.process.poll()

        if rc is None:
            # Still running
            if tracked.is_live and tracked.health_url:
                return self._check_health(tracked)
            return JobStatus.RUNNING

        # Process has exited — record end time if not already set
        if tracked.end_time is None:
            tracked.end_time = datetime.now(UTC)

        if rc == 0:
            return JobStatus.SUCCEEDED

        if rc < 0:
            # Killed by signal
            sig_name = _signal_name(-rc)
            tracked.error_message = f"Killed by signal {sig_name}"
            return JobStatus.CANCELLED if -rc == signal.SIGTERM else JobStatus.FAILED

        tracked.error_message = f"Exited with code {rc}"
        return JobStatus.FAILED

    def _check_health(self, tracked: _TrackedProcess) -> JobStatus:
        """Probe the health endpoint of a live service."""
        if tracked.health_url is None:
            return JobStatus.RUNNING

        try:
            with urlopen(tracked.health_url, timeout=_HEALTH_CHECK_TIMEOUT) as response:  # nosec B310 — localhost health check, not user-controlled URL
                status: int = getattr(response, "status", getattr(response, "getcode", lambda: 0)())
                if status == 200:
                    return JobStatus.RUNNING
                tracked.error_message = f"Health check returned {status}"
                return JobStatus.FAILED
        except (URLError, OSError):
            # Service may still be starting up
            return JobStatus.RUNNING


# ------------------------------------------------------------------
# Event type constants
# ------------------------------------------------------------------

_JOB_STARTED = VMEventType.JOB_STARTED
_JOB_COMPLETED = VMEventType.JOB_COMPLETED
_JOB_FAILED = VMEventType.JOB_FAILED
_JOB_CANCELLED = VMEventType.JOB_CANCELLED


def _signal_name(signum: int) -> str:
    """Return the signal name for a signal number, or the number as a string."""
    try:
        return signal.Signals(signum).name
    except ValueError:
        return str(signum)
