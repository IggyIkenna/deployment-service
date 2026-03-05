"""
Worker management utilities for deploying shards.

Handles launching shards in parallel or rolling fashion, managing
concurrency limits, retries, and progress tracking.
"""

import logging
import random
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import cast

from deployment_service.backends import ComputeBackend, JobInfo, JobStatus
from deployment_service.deployment_config import DeploymentConfig

from .quota_broker_client import QuotaBrokerClient
from .rate_limiter import RateLimiter
from .state import DeploymentState, DeploymentStatus, ShardState, ShardStatus, StateManager

_config = DeploymentConfig()

logger = logging.getLogger(__name__)


def launch_shards_parallel(
    state: DeploymentState,
    backend: ComputeBackend,
    docker_image: str,
    environment_variables: dict[str, str],
    compute_config: dict[str, object],
    rate_limiter: RateLimiter,
    state_manager: StateManager,
    quota_broker: QuotaBrokerClient | None = None,
    vm_resource_request_fn: object = None,
    max_workers: int = 50,
    venue_overrides: dict[str, dict[str, object]] | None = None,
    compute_type: str = "vm",
    auto_retry_failed: bool = True,
    max_launch_retry_rounds: int = 3,
) -> None:
    """
    Launch all shards in parallel using threading.

    Args:
        state: DeploymentState to update
        backend: ComputeBackend to use
        docker_image: Docker image URL
        environment_variables: Environment variables
        compute_config: Default compute configuration
        rate_limiter: RateLimiter instance for API throttling
        state_manager: StateManager for persisting state
        quota_broker: Optional quota broker client
        vm_resource_request_fn: Function to map VM config to resource requirements
        max_workers: Maximum concurrent API calls
        venue_overrides: Per-venue compute overrides (e.g., COINBASE needs 256GB RAM)
        compute_type: "vm" or "cloud_run" for applying overrides
        auto_retry_failed: If True, automatically retry shards that failed during launch (default True)
        max_launch_retry_rounds: Maximum number of retry rounds for failed launch shards (default 3)
    """
    labels = {
        "service": state.service,
        "deployment_id": state.deployment_id,
    }

    pending_shards = state.pending_shards[:]  # Copy list
    venue_overrides = venue_overrides or {}

    def get_shard_compute_config(shard: ShardState) -> dict[str, object]:
        """Get compute config for shard, applying venue overrides if applicable."""
        venue = cast("str | None", shard.dimensions.get("venue"))
        if venue and venue in venue_overrides:
            venue_config = cast("dict[str, object]", venue_overrides[venue].get(compute_type, {}))
            if venue_config:
                logger.info("[%s] Applying venue override for %s: %s", shard.shard_id, venue, venue_config)
                return {**compute_config, **venue_config}
        return compute_config

    def launch_single_shard(
        shard_with_index: tuple[int, ShardState],
        max_launch_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
    ) -> tuple[ShardState, JobInfo | None]:
        """Launch a single shard with retry logic and exponential backoff.

        Args:
            shard_with_index: Tuple of (index, shard)
            max_launch_retries: Maximum number of retry attempts for transient errors (default 3)
            base_delay: Base delay in seconds for exponential backoff (default 1.0)
            max_delay: Maximum delay in seconds between retries (default 30.0)

        Returns:
            Tuple of (shard, job_info) where job_info is None if all retries failed
        """
        shard_index, shard = shard_with_index
        last_error = None
        lease_id: str | None = None

        # Centralized admission control (optional) - block until granted.
        if quota_broker and quota_broker.enabled():
            shard_compute_config = get_shard_compute_config(shard)
            broker_region: str = str(getattr(backend, "region", "us-central1"))
            resources: dict[str, float]
            ttl_override: int | None
            if compute_type == "vm":
                resources = (
                    cast(dict[str, float], vm_resource_request_fn(shard_compute_config))
                    if callable(vm_resource_request_fn)
                    else {}
                )
                ttl_override = None
            else:
                resources = {"RUNNING_EXECUTIONS": 1.0}
                timeout_s = int(cast(int, (shard_compute_config or {}).get("timeout_seconds", 3600) or 3600))
                ttl_override = max(300, min(timeout_s, 6 * 3600))

            started_wait = time.time()
            max_wait_seconds = _config.broker_max_wait_seconds
            while True:
                admission = quota_broker.acquire(
                    deployment_id=state.deployment_id,
                    shard_id=shard.shard_id,
                    compute_type=compute_type,  # type: ignore[arg-type]
                    region=broker_region,
                    resources=resources,
                    ttl_seconds=ttl_override,
                )
                if admission.granted:
                    lease_id = admission.lease_id
                    shard.quota_lease_id = lease_id
                    shard.quota_denied_reason = None
                    shard.quota_retry_after_seconds = None
                    break

                shard.quota_denied_reason = admission.reason or "denied"
                shard.quota_retry_after_seconds = admission.retry_after_seconds or 30

                if (time.time() - started_wait) > max_wait_seconds:
                    # Give up and mark as launch failure.
                    return (shard, None)

                time.sleep(float(shard.quota_retry_after_seconds))

        for attempt in range(max_launch_retries + 1):
            try:
                # Rate limit to avoid hitting GCP API quotas (create = write operation)
                rate_limiter.acquire()

                # Add SHARD_INDEX and TOTAL_SHARDS to environment for round-robin API key selection
                # This allows services like TheGraphBaseClient to distribute API keys
                # TOTAL_SHARDS is needed because some services validate shard_index < total_shards
                total_shards = len(pending_shards)
                shard_env_vars = {
                    **environment_variables,
                    "SHARD_INDEX": str(shard_index),
                    "TOTAL_SHARDS": str(total_shards),
                }

                shard_compute_config = get_shard_compute_config(shard)
                # Zone distribution for VM: round-robin across zones (shard_index % 3)
                if compute_type == "vm" and hasattr(backend, "_get_zones_for_region"):
                    _get_zones_fn = cast(object, getattr(backend, "_get_zones_for_region", None))
                    _backend_region: str = str(getattr(backend, "region", "us-central1"))
                    zones: list[str] = (
                        cast(list[str], _get_zones_fn(_backend_region))
                        if callable(_get_zones_fn)
                        else [_backend_region]
                    )
                    assigned_zone: str = zones[shard_index % len(zones)]
                    shard_compute_config = {
                        **(shard_compute_config or {}),
                        "zone": assigned_zone,
                    }
                job_info = backend.deploy_shard(
                    shard_id=shard.shard_id,
                    docker_image=docker_image,
                    args=shard.args,
                    environment_variables=shard_env_vars,
                    compute_config=shard_compute_config,
                    labels=labels,
                )

                # Release admission lease on failed launch (best-effort)
                if job_info.status == JobStatus.FAILED:
                    try:
                        if quota_broker and quota_broker.enabled() and (lease_id or shard.quota_lease_id):
                            quota_broker.release(lease_id=str(lease_id or shard.quota_lease_id))
                            shard.quota_lease_id = None
                    except (ConnectionError, TimeoutError) as e:
                        logger.warning("Failed to release quota lease for %s (connection issue): %s", shard.shard_id, e)
                    except (OSError, ValueError, RuntimeError) as e:
                        logger.warning("Failed to release quota lease for %s: %s", shard.shard_id, e)

                # Log success after retries
                if attempt > 0:
                    logger.info(
                        "[LAUNCH_RETRY_SUCCESS] Shard %s launched successfully after %s retry(ies)",
                        shard.shard_id,
                        attempt,
                    )

                return (shard, job_info)

            except (OSError, ValueError, RuntimeError) as e:
                last_error = e
                error_str = str(e).lower()

                # Check if this is a retryable error (SSL, connection, timeout issues)
                is_retryable = any(
                    keyword in error_str
                    for keyword in [
                        "ssl",
                        "ssleof",
                        "connection",
                        "timeout",
                        "temporary",
                        "unavailable",
                        "reset",
                        "broken pipe",
                        "eof occurred",
                        "max retries exceeded",
                        "connectionpool",
                        "httpsconnectionpool",
                    ]
                )

                if is_retryable and attempt < max_launch_retries:
                    # Exponential backoff with jitter
                    delay: float = min(base_delay * (2.0**attempt), max_delay)
                    jitter: float = random.uniform(0, delay * 0.3)  # Up to 30% jitter
                    total_delay: float = delay + jitter

                    logger.warning(
                        "[LAUNCH_RETRY] Shard %s failed (attempt %s/%s), retrying in %.1fs. Error: %s",
                        shard.shard_id,
                        attempt + 1,
                        max_launch_retries + 1,
                        total_delay,
                        e,
                    )
                    time.sleep(total_delay)
                else:
                    # Non-retryable error or max retries reached
                    if attempt >= max_launch_retries:
                        logger.error(
                            "[LAUNCH_FAILED] Shard %s failed after %s attempt(s): %s", shard.shard_id, attempt + 1, e
                        )
                    else:
                        logger.error("[LAUNCH_FAILED] Shard %s failed (non-retryable): %s", shard.shard_id, e)
                    # Release admission lease on ultimate failure (best-effort)
                    try:
                        if quota_broker and quota_broker.enabled() and (lease_id or shard.quota_lease_id):
                            quota_broker.release(lease_id=str(lease_id or shard.quota_lease_id))
                            shard.quota_lease_id = None
                    except (ConnectionError, TimeoutError) as e:
                        logger.warning("Failed to release quota lease for %s (connection issue): %s", shard.shard_id, e)
                    except (OSError, ValueError, RuntimeError) as e:
                        logger.warning("Failed to release quota lease for %s: %s", shard.shard_id, e)
                    return (shard, None)

        # Should not reach here, but just in case
        logger.error("[LAUNCH_FAILED] Shard %s failed after all retries: %s", shard.shard_id, last_error)
        # Release admission lease on ultimate failure (best-effort)
        try:
            if quota_broker and quota_broker.enabled() and (lease_id or shard.quota_lease_id):
                quota_broker.release(lease_id=str(lease_id or shard.quota_lease_id))
                shard.quota_lease_id = None
        except (ConnectionError, TimeoutError) as e:
            logger.warning("Failed to release quota lease for %s (connection issue): %s", shard.shard_id, e)
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Failed to release quota lease for %s: %s", shard.shard_id, e)
        return (shard, None)

    # Launch all shards in parallel using ThreadPoolExecutor with mini-batching
    # Enumerate shards to pass index for round-robin API key selection
    indexed_shards = list(enumerate(pending_shards))

    # Configurable mini-batch size and delay to avoid overwhelming GCP
    # 50 VMs per batch is safe since GCP provisions in parallel (~15-20s for 50)
    # The delay between batches lets GCP's provisioning queue clear
    mini_batch_size = _config.vm_launch_mini_batch_size
    mini_batch_delay_seconds = _config.vm_launch_mini_batch_delay_seconds

    total_mini_batches = (len(indexed_shards) + mini_batch_size - 1) // mini_batch_size
    logger.info(
        "Launching %s shards with %s parallel workers (mini-batches of %s with %ss delay, %s batches)...",
        len(pending_shards),
        max_workers,
        mini_batch_size,
        mini_batch_delay_seconds,
        total_mini_batches,
    )

    completed = 0

    # Process shards in mini-batches to avoid overwhelming GCP
    for mini_batch_idx in range(0, len(indexed_shards), mini_batch_size):
        mini_batch = indexed_shards[mini_batch_idx : mini_batch_idx + mini_batch_size]
        mini_batch_num = (mini_batch_idx // mini_batch_size) + 1

        logger.info(
            "[MINI_BATCH] Launching mini-batch %s/%s (%s shards)...",
            mini_batch_num,
            total_mini_batches,
            len(mini_batch),
        )

        with ThreadPoolExecutor(max_workers=min(max_workers, len(mini_batch))) as executor:
            futures: dict[Future[tuple[ShardState, JobInfo | None]], ShardState] = {
                executor.submit(launch_single_shard, indexed_shard): indexed_shard[1] for indexed_shard in mini_batch
            }

            for future in as_completed(futures):
                shard, job_info = future.result()
                completed += 1

                # Update shard state
                shard.start_time = datetime.now(UTC).isoformat()

                if job_info is None or job_info.status == JobStatus.FAILED:
                    shard.status = ShardStatus.FAILED
                    shard.error_message = job_info.error_message if job_info else "Launch failed"
                    shard.end_time = datetime.now(UTC).isoformat()
                else:
                    shard.status = ShardStatus.RUNNING
                    shard.job_id = job_info.job_id

        # Log and save state after each mini-batch for better UI sync
        running = sum(1 for s in state.shards if s.status == ShardStatus.RUNNING)
        failed = sum(1 for s in state.shards if s.status == ShardStatus.FAILED)
        state_manager.save_state(state)
        logger.info(
            "[MINI_BATCH] Mini-batch %s complete: %s/%s total (running: %s, failed: %s)",
            mini_batch_num,
            completed,
            len(pending_shards),
            running,
            failed,
        )

        # Delay between mini-batches to let GCP provision VMs
        if mini_batch_idx + mini_batch_size < len(indexed_shards):
            logger.debug("[MINI_BATCH] Waiting %ss before next mini-batch...", mini_batch_delay_seconds)
            time.sleep(mini_batch_delay_seconds)

    # Recalculate overall deployment status after launch
    succeeded = sum(1 for s in state.shards if s.status == ShardStatus.SUCCEEDED)
    failed = sum(1 for s in state.shards if s.status == ShardStatus.FAILED)
    running = sum(1 for s in state.shards if s.status == ShardStatus.RUNNING)

    state_manager.save_state(state)
    logger.info(
        "Initial launch complete: %s shards (running: %s, failed: %s, succeeded: %s)",
        len(pending_shards),
        running,
        failed,
        succeeded,
    )

    # Auto-retry failed shards if enabled
    if auto_retry_failed and failed > 0:
        logger.info("[AUTO_RETRY] Starting automatic retry of %s failed launch shards...", failed)

        for retry_round in range(max_launch_retry_rounds):
            # Get currently failed shards
            failed_shards = [s for s in state.shards if s.status == ShardStatus.FAILED]
            if not failed_shards:
                logger.info("[AUTO_RETRY] All shards succeeded, no more retries needed")
                break

            # Exponential backoff between retry rounds
            if retry_round > 0:
                round_delay: float = min(30.0 * (2.0 ** (retry_round - 1)), 120.0)  # 30s, 60s, 120s max
                logger.info("[AUTO_RETRY] Waiting %ss before retry round %s...", round_delay, retry_round + 1)
                time.sleep(round_delay)

            logger.info(
                "[AUTO_RETRY] Retry round %s/%s: retrying %s failed shards...",
                retry_round + 1,
                max_launch_retry_rounds,
                len(failed_shards),
            )

            # Reset failed shards to pending for retry
            for shard in failed_shards:
                shard.status = ShardStatus.PENDING
                shard.error_message = None
                shard.retries += 1

            # Re-enumerate with fresh indices for retry
            indexed_retry_shards = list(enumerate(failed_shards))

            # Use fewer workers for retry to reduce connection pressure
            retry_workers = max(10, max_workers // 3)

            with ThreadPoolExecutor(max_workers=retry_workers) as executor:
                retry_futures: dict[Future[tuple[ShardState, JobInfo | None]], ShardState] = {
                    executor.submit(launch_single_shard, indexed_shard): indexed_shard[1]
                    for indexed_shard in indexed_retry_shards
                }

                for retry_completed, future in enumerate(as_completed(retry_futures), 1):
                    shard, job_info = future.result()

                    shard.start_time = datetime.now(UTC).isoformat()

                    if job_info is None or job_info.status == JobStatus.FAILED:
                        shard.status = ShardStatus.FAILED
                        shard.error_message = job_info.error_message if job_info else "Launch failed"
                        shard.end_time = datetime.now(UTC).isoformat()
                    else:
                        shard.status = ShardStatus.RUNNING
                        shard.job_id = job_info.job_id

                    if retry_completed % 10 == 0 or retry_completed == len(failed_shards):
                        logger.info(
                            "[AUTO_RETRY] Round %s: %s/%s shards processed",
                            retry_round + 1,
                            retry_completed,
                            len(failed_shards),
                        )

            # Recalculate counts after this retry round
            succeeded = sum(1 for s in state.shards if s.status == ShardStatus.SUCCEEDED)
            still_failed = sum(1 for s in state.shards if s.status == ShardStatus.FAILED)
            running = sum(1 for s in state.shards if s.status == ShardStatus.RUNNING)

            state_manager.save_state(state)
            logger.info(
                "[AUTO_RETRY] Round %s complete: running=%s, failed=%s, succeeded=%s",
                retry_round + 1,
                running,
                still_failed,
                succeeded,
            )

            if still_failed == 0:
                break

        # Final count after all retry rounds
        failed = sum(1 for s in state.shards if s.status == ShardStatus.FAILED)
        running = sum(1 for s in state.shards if s.status == ShardStatus.RUNNING)
        succeeded = sum(1 for s in state.shards if s.status == ShardStatus.SUCCEEDED)

        if failed > 0:
            logger.warning("[AUTO_RETRY] %s shards still failed after %s retry rounds", failed, max_launch_retry_rounds)

    # Set final deployment status
    if failed == len(state.shards):
        # All shards failed
        state.status = DeploymentStatus.FAILED
    elif succeeded == len(state.shards):
        # All shards succeeded
        state.status = DeploymentStatus.COMPLETED
    elif failed > 0 and running == 0:
        # Some failed, none running = partial failure / failed
        state.status = DeploymentStatus.FAILED
    elif running > 0:
        # Some still running
        state.status = DeploymentStatus.RUNNING

    state_manager.save_state(state)
    logger.info(
        "Launch phase complete: %s total shards (running: %s, failed: %s, succeeded: %s)",
        len(pending_shards),
        running,
        failed,
        succeeded,
    )


def launch_shards_rolling(
    state: DeploymentState,
    backend: ComputeBackend,
    docker_image: str,
    environment_variables: dict[str, str],
    compute_config: dict[str, object],
    rate_limiter: RateLimiter,
    state_manager: StateManager,
    quota_broker: QuotaBrokerClient | None = None,
    vm_resource_request_fn: object = None,
    max_workers: int = 50,
    max_concurrent: int = 2000,
    venue_overrides: dict[str, dict[str, object]] | None = None,
    compute_type: str = "vm",
    no_wait: bool = False,
    poll_interval: int = 30,
) -> None:
    """
    Launch shards with rolling concurrency - maintains max_concurrent running at any time.

    When total shards > max_concurrent, this method:
    1. Launches initial batch of max_concurrent shards
    2. Monitors for completions
    3. Launches new shards to fill available slots
    4. Continues until all shards are launched and completed

    Args:
        state: DeploymentState to update
        backend: ComputeBackend to use
        docker_image: Docker image URL
        environment_variables: Environment variables
        compute_config: Default compute configuration
        rate_limiter: RateLimiter instance for API throttling
        state_manager: StateManager for persisting state
        quota_broker: Optional quota broker client
        vm_resource_request_fn: Function to map VM config to resource requirements
        max_workers: Maximum concurrent API calls for launching
        max_concurrent: Maximum simultaneously running VMs/jobs
        venue_overrides: Per-venue compute overrides
        compute_type: "vm" or "cloud_run"
        no_wait: If True, return after launching initial batch
        poll_interval: Seconds between monitoring checks
    """
    labels = {
        "service": state.service,
        "deployment_id": state.deployment_id,
    }
    venue_overrides = venue_overrides or {}

    total_shards = len(state.pending_shards)
    logger.info(
        "[ROLLING_LAUNCH] Starting rolling launch: %s total shards, max_concurrent=%s", total_shards, max_concurrent
    )

    def get_shard_compute_config(shard: ShardState) -> dict[str, object]:
        """Get compute config for shard, applying venue overrides if applicable."""
        venue = cast("str | None", shard.dimensions.get("venue"))
        if venue and venue in venue_overrides:
            venue_config = cast("dict[str, object]", venue_overrides[venue].get(compute_type, {}))
            if venue_config:
                return {**compute_config, **venue_config}
        return compute_config

    # Track shard index for environment variables
    shard_index_counter = [0]
    shard_index_lock = threading.Lock()

    def launch_single_shard(shard: ShardState) -> tuple[ShardState, JobInfo | None]:
        """Launch a single shard and return result."""
        lease_id: str | None = None
        try:
            shard_compute_config = get_shard_compute_config(shard)

            # Centralized admission control (optional)
            if quota_broker and quota_broker.enabled():
                broker_region: str = str(getattr(backend, "region", "us-central1"))
                _resources: dict[str, float]
                _ttl_override: int | None
                if compute_type == "vm":
                    _resources = (
                        cast(dict[str, float], vm_resource_request_fn(shard_compute_config))
                        if callable(vm_resource_request_fn)
                        else {}
                    )
                    _ttl_override = None
                else:
                    _resources = {"RUNNING_EXECUTIONS": 1.0}
                    timeout_s = int(cast(int, (shard_compute_config or {}).get("timeout_seconds", 3600) or 3600))
                    _ttl_override = max(300, min(timeout_s, 6 * 3600))

                admission = quota_broker.acquire(
                    deployment_id=state.deployment_id,
                    shard_id=shard.shard_id,
                    compute_type=compute_type,  # type: ignore[arg-type]
                    region=broker_region,
                    resources=_resources,
                    ttl_seconds=_ttl_override,
                )
                if not admission.granted:
                    shard.quota_denied_reason = admission.reason or "denied"
                    shard.quota_retry_after_seconds = admission.retry_after_seconds
                    return (shard, None)

                lease_id = admission.lease_id
                shard.quota_lease_id = lease_id
                shard.quota_denied_reason = None
                shard.quota_retry_after_seconds = None

            # Rate limit (GCP writes)
            rate_limiter.acquire()

            # Get unique shard index
            with shard_index_lock:
                shard_index = shard_index_counter[0]
                shard_index_counter[0] += 1

            # Zone distribution for VM: round-robin across zones (shard_index % 3)
            if compute_type == "vm" and hasattr(backend, "_get_zones_for_region"):
                _get_zones_fn2 = cast(object, getattr(backend, "_get_zones_for_region", None))
                _backend_region2: str = str(getattr(backend, "region", "us-central1"))
                zones2: list[str] = (
                    cast(list[str], _get_zones_fn2(_backend_region2))
                    if callable(_get_zones_fn2)
                    else [_backend_region2]
                )
                assigned_zone2: str = zones2[shard_index % len(zones2)]
                shard_compute_config = {
                    **(shard_compute_config or {}),
                    "zone": assigned_zone2,
                }

            # Add SHARD_INDEX and TOTAL_SHARDS to environment for round-robin API key selection
            shard_env_vars = {
                **environment_variables,
                "SHARD_INDEX": str(shard_index),
                "TOTAL_SHARDS": str(total_shards),
            }

            job_info = backend.deploy_shard(
                shard_id=shard.shard_id,
                docker_image=docker_image,
                args=shard.args,
                environment_variables=shard_env_vars,
                compute_config=shard_compute_config,
                labels=labels,
            )
            return (shard, job_info)
        except (OSError, ValueError, RuntimeError) as e:
            # Release admission lease on launch failure (best-effort)
            try:
                if quota_broker and quota_broker.enabled() and (lease_id or shard.quota_lease_id):
                    quota_broker.release(lease_id=str(lease_id or shard.quota_lease_id))
                    shard.quota_lease_id = None
            except (ConnectionError, TimeoutError) as e2:
                logger.warning("Failed to release quota lease for %s (connection issue): %s", shard.shard_id, e2)
            except (OSError, ValueError, RuntimeError) as e2:
                logger.warning("Failed to release quota lease for %s: %s", shard.shard_id, e2)
            logger.error("[ROLLING_LAUNCH] Failed to launch %s: %s", shard.shard_id, e)
            return (shard, None)

    def check_shard_status(shard: ShardState) -> JobStatus:
        """Check current backend job status for a running shard."""
        if not shard.job_id:
            return JobStatus.UNKNOWN
        try:
            job_info = backend.get_status_with_context(
                shard.job_id,
                deployment_id=state.deployment_id,
                shard_id=shard.shard_id,
            )
            return job_info.status if job_info else JobStatus.UNKNOWN
        except (ConnectionError, TimeoutError) as e:
            # Network issues - treat as unknown and retry next poll
            logger.debug("Connection error checking status for %s: %s", shard.shard_id, e)
            return JobStatus.UNKNOWN
        except (OSError, ValueError, RuntimeError) as e:
            # Other errors - log warning but don't spam in large deployments
            logger.debug("Error checking status for %s: %s", shard.shard_id, e)
            return JobStatus.UNKNOWN

    # Track which shards have been launched
    launched_shard_ids: set[str] = set()

    # Initial launch of up to max_concurrent shards
    pending_to_launch = [s for s in state.pending_shards if s.shard_id not in launched_shard_ids]
    initial_batch = pending_to_launch[:max_concurrent]

    # Configurable mini-batch size and delay to avoid overwhelming GCP
    # 50 VMs per batch is safe since GCP provisions in parallel (~15-20s for 50)
    # The delay between batches lets GCP's provisioning queue clear
    mini_batch_size = _config.vm_launch_mini_batch_size
    mini_batch_delay_seconds = _config.vm_launch_mini_batch_delay_seconds

    logger.info(
        "[ROLLING_LAUNCH] Launching initial batch of %s shards (mini-batches of %s with %ss delay)...",
        len(initial_batch),
        mini_batch_size,
        mini_batch_delay_seconds,
    )

    # Split initial batch into mini-batches to avoid overwhelming GCP
    launched = 0
    for mini_batch_idx in range(0, len(initial_batch), mini_batch_size):
        mini_batch = initial_batch[mini_batch_idx : mini_batch_idx + mini_batch_size]
        mini_batch_num = (mini_batch_idx // mini_batch_size) + 1
        total_mini_batches = (len(initial_batch) + mini_batch_size - 1) // mini_batch_size

        logger.info(
            "[ROLLING_LAUNCH] Launching mini-batch %s/%s (%s shards)...",
            mini_batch_num,
            total_mini_batches,
            len(mini_batch),
        )

        with ThreadPoolExecutor(max_workers=min(max_workers, len(mini_batch))) as executor:
            futures = {executor.submit(launch_single_shard, shard): shard for shard in mini_batch}

            for future in as_completed(futures):
                shard, job_info = future.result()
                launched += 1

                # If broker denied admission, keep shard pending for later retry.
                if job_info is None and shard.quota_denied_reason:
                    continue

                shard.start_time = datetime.now(UTC).isoformat()
                launched_shard_ids.add(shard.shard_id)

                if job_info is None or job_info.status == JobStatus.FAILED:
                    shard.status = ShardStatus.FAILED
                    shard.error_message = job_info.error_message if job_info else "Launch failed"
                    shard.end_time = datetime.now(UTC).isoformat()

                    # Release admission lease on failed launch (best-effort)
                    try:
                        if quota_broker and quota_broker.enabled() and shard.quota_lease_id:
                            quota_broker.release(lease_id=str(shard.quota_lease_id))
                            shard.quota_lease_id = None
                    except (OSError, ValueError, RuntimeError) as e:
                        logger.warning("Failed to release quota lease on failed launch: %s", e)
                else:
                    shard.status = ShardStatus.RUNNING
                    shard.job_id = job_info.job_id

        # Save state after every mini-batch for better UI sync
        state_manager.save_state(state)
        logger.info(
            "[ROLLING_LAUNCH] Mini-batch %s complete: %s/%s total launched",
            mini_batch_num,
            launched,
            len(initial_batch),
        )

        # Delay between mini-batches to let GCP provision VMs
        if mini_batch_idx + mini_batch_size < len(initial_batch):
            logger.debug("[ROLLING_LAUNCH] Waiting %ss before next mini-batch...", mini_batch_delay_seconds)
            time.sleep(mini_batch_delay_seconds)

    logger.info("[ROLLING_LAUNCH] Initial batch complete: %s shards launched", launched)

    if no_wait:
        logger.info("[ROLLING_LAUNCH] no_wait=True, returning after initial batch")
        return

    # Rolling launch loop - monitor and launch more as slots become available
    remaining_to_launch = [s for s in state.pending_shards if s.shard_id not in launched_shard_ids]

    while remaining_to_launch or any(s.status == ShardStatus.RUNNING for s in state.shards):
        unknown_threshold = _config.unknown_status_max_polls

        # Count current running shards
        running_shards = [s for s in state.shards if s.status == ShardStatus.RUNNING]
        running_count = len(running_shards)

        # Check status of running shards
        completed_this_round = 0
        for shard in running_shards:
            status = check_shard_status(shard)

            if status == JobStatus.SUCCEEDED:
                shard.status = ShardStatus.SUCCEEDED
                shard.end_time = datetime.now(UTC).isoformat()
                shard.unknown_polls = 0
                completed_this_round += 1
                # Release quota lease (best-effort)
                try:
                    if quota_broker and quota_broker.enabled() and shard.quota_lease_id:
                        quota_broker.release(lease_id=str(shard.quota_lease_id))
                        shard.quota_lease_id = None
                except (ConnectionError, TimeoutError) as e:
                    logger.warning("Failed to release quota lease for %s (connection issue): %s", shard.shard_id, e)
                except (OSError, ValueError, RuntimeError) as e:
                    logger.warning("Failed to release quota lease for %s: %s", shard.shard_id, e)
            elif status == JobStatus.FAILED:
                shard.status = ShardStatus.FAILED
                shard.end_time = datetime.now(UTC).isoformat()
                shard.unknown_polls = 0
                completed_this_round += 1
                # Release quota lease (best-effort)
                try:
                    if quota_broker and quota_broker.enabled() and shard.quota_lease_id:
                        quota_broker.release(lease_id=str(shard.quota_lease_id))
                        shard.quota_lease_id = None
                except (ConnectionError, TimeoutError) as e:
                    logger.warning("Failed to release quota lease for %s (connection issue): %s", shard.shard_id, e)
                except (OSError, ValueError, RuntimeError) as e:
                    logger.warning("Failed to release quota lease for %s: %s", shard.shard_id, e)
            elif status == JobStatus.CANCELLED:
                shard.status = ShardStatus.CANCELLED
                shard.end_time = datetime.now(UTC).isoformat()
                shard.unknown_polls = 0
                completed_this_round += 1
                # Release quota lease (best-effort)
                try:
                    if quota_broker and quota_broker.enabled() and shard.quota_lease_id:
                        quota_broker.release(lease_id=str(shard.quota_lease_id))
                        shard.quota_lease_id = None
                except (ConnectionError, TimeoutError) as e:
                    logger.warning("Failed to release quota lease for %s (connection issue): %s", shard.shard_id, e)
                except (OSError, ValueError, RuntimeError) as e:
                    logger.warning("Failed to release quota lease for %s: %s", shard.shard_id, e)
            elif status == JobStatus.UNKNOWN:
                shard.unknown_polls = (shard.unknown_polls or 0) + 1
                if shard.unknown_polls >= unknown_threshold:
                    shard.status = ShardStatus.FAILED
                    shard.end_time = datetime.now(UTC).isoformat()
                    shard.error_message = (
                        f"Backend status UNKNOWN for {shard.unknown_polls} polls; marking shard as failed"
                    )
                    completed_this_round += 1
                    # Release quota lease (best-effort)
                    try:
                        if quota_broker and quota_broker.enabled() and shard.quota_lease_id:
                            quota_broker.release(lease_id=str(shard.quota_lease_id))
                            shard.quota_lease_id = None
                    except (OSError, ValueError, RuntimeError) as e:
                        logger.warning("Failed to release quota lease (UNKNOWN status): %s", e)
            else:
                # Any non-UNKNOWN response resets the consecutive UNKNOWN counter.
                shard.unknown_polls = 0

        # Recalculate running after status updates
        running_count = sum(1 for s in state.shards if s.status == ShardStatus.RUNNING)

        # Calculate available slots
        available_slots = max_concurrent - running_count

        # Launch more shards if we have capacity and pending shards
        if available_slots > 0 and remaining_to_launch:
            batch_to_launch = remaining_to_launch[:available_slots]

            logger.info(
                "[ROLLING_LAUNCH] Launching %s more shards (running: %s, available: %s)",
                len(batch_to_launch),
                running_count,
                available_slots,
            )

            # Use mini-batching for subsequent launches too
            for mini_batch_idx in range(0, len(batch_to_launch), mini_batch_size):
                mini_batch = batch_to_launch[mini_batch_idx : mini_batch_idx + mini_batch_size]

                with ThreadPoolExecutor(max_workers=min(max_workers, len(mini_batch))) as executor:
                    futures = {executor.submit(launch_single_shard, shard): shard for shard in mini_batch}

                    for future in as_completed(futures):
                        shard, job_info = future.result()
                        # If broker denied admission, keep shard pending for later retry.
                        if job_info is None and shard.quota_denied_reason:
                            continue

                        launched_shard_ids.add(shard.shard_id)

                        shard.start_time = datetime.now(UTC).isoformat()

                        if job_info is None or job_info.status == JobStatus.FAILED:
                            shard.status = ShardStatus.FAILED
                            shard.error_message = job_info.error_message if job_info else "Launch failed"
                            shard.end_time = datetime.now(UTC).isoformat()

                            # Release admission lease on failed launch (best-effort)
                            try:
                                if quota_broker and quota_broker.enabled() and shard.quota_lease_id:
                                    quota_broker.release(lease_id=str(shard.quota_lease_id))
                                    shard.quota_lease_id = None
                            except (OSError, ValueError, RuntimeError) as e:
                                logger.warning("Failed to release quota lease on failed launch: %s", e)
                        else:
                            shard.status = ShardStatus.RUNNING
                            shard.job_id = job_info.job_id

                # Save state after each mini-batch for better UI sync
                state_manager.save_state(state)

                # Delay between mini-batches to let GCP provision VMs
                if mini_batch_idx + mini_batch_size < len(batch_to_launch):
                    time.sleep(mini_batch_delay_seconds)

            # Update remaining list
            remaining_to_launch = [s for s in state.pending_shards if s.shard_id not in launched_shard_ids]

        # Save state and display progress
        state_manager.save_state(state)

        # Log progress
        succeeded = sum(1 for s in state.shards if s.status == ShardStatus.SUCCEEDED)
        failed = sum(1 for s in state.shards if s.status == ShardStatus.FAILED)
        running = sum(1 for s in state.shards if s.status == ShardStatus.RUNNING)
        pending = sum(1 for s in state.shards if s.status == ShardStatus.PENDING)

        logger.info(
            "[ROLLING_LAUNCH] Progress: running=%s, succeeded=%s, failed=%s, pending=%s, remaining_to_launch=%s",
            running,
            succeeded,
            failed,
            pending,
            len(remaining_to_launch),
        )

        # Exit if all done
        if running == 0 and len(remaining_to_launch) == 0:
            break

        # Wait before next check
        time.sleep(poll_interval)

    # Final status
    succeeded = sum(1 for s in state.shards if s.status == ShardStatus.SUCCEEDED)
    failed = sum(1 for s in state.shards if s.status == ShardStatus.FAILED)

    if failed == len(state.shards) or failed > 0:
        state.status = DeploymentStatus.FAILED
    else:
        state.status = DeploymentStatus.COMPLETED

    state_manager.save_state(state)
    logger.info("[ROLLING_LAUNCH] Complete: succeeded=%s, failed=%s", succeeded, failed)
