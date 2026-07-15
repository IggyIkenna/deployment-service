# Sharding and Data Alignment Spec

<!-- MULTI_AXIS_CORRECTION_2026_05_06 -->

> **Multi-axis correction (2026-05-06)** — per [`data_status_multi_axis_shard_propagation_2026_05_06.plan`](../../unified-trading-pm/plans/active/data_status_multi_axis_shard_propagation_2026_05_06.plan): a column belongs in the **shard atom** ONLY IF it earns it via failure isolation OR memory ceiling OR concurrency orthogonality. Otherwise it's a **display axis** (row-level column for filter/group, NOT a manifest row per value). This refines the per-asset-group shard atoms below:
>
> - **Sports**: shard atom = `(asset_group=sports, venue/source, data_type, league_id, day)`. **`fixture_id` is a row-level column in the parquet, NOT a shard axis** — `(league_id, day)` already bounds the per-day fixture set; per-fixture detail at drill-down comes from reading the parquet, not from a separate manifest row. Avoids 10× manifest inflation.
> - **Prediction**: shard atom = `(asset_group=prediction, venue, data_type=prediction_canonical_question_group, canonical_question_group, day)`. **`market_id` is a row-level column in the parquet, NOT a shard axis** — same rationale. HOURLY (24/day) + DAILY + ELECTION groups all roll up to one manifest row per `(canonical_question_group, day)`; per-market detail at drill-down from parquet.
> - **CeFi options/futures bundles**: bundle root IS a shard axis (memory + concurrency); per-symbol within bundle is parquet row (cluster validation enforces all expected per-bundle clusters covered).
> - **DeFi `chain`** IS a shard axis (independent RPC/subgraph endpoints + failure isolation).
> - **ML / strategy / execution**: new `job_id` v7 manifest column for experiment-keyed services. Same `(model_family, training_period, job_id)` shard atom for ML training; `(strategy_id, job_id)` for strategy; `(strategy_id, instruction_type, job_id)` for execution. Re-running same configs = new `job_id` (audit trail of every experiment version).
> - **instrument_type for instruments-service**: NOT a shard axis (Databento + TARDIS bulk-fetch all instrument_types per venue in one call). Display axis only — row column for filter/group.

**Purpose:** Ensure data status, missing-shards, startup dependency validation, and service upload behavior align with GCS bucketing and the canonical shard atom per asset_group.

**Principle:** "Any data means all data" — a shard either succeeds fully (passes the 4-pillar write-gate at `record_captured`) or fails fully (`record_failed(<typed_reason>)`) or is honestly absent (`record_empty(row_key)`). No partial uploads. No silent NaN placeholder rows. No silent per-schema drops on bundled writes.

**Canonical SSOT for shard atoms + manifest semantics:** [`unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md`](../../unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md). This doc is deployment-service-specific (path templates, alignment matrix, audit table); shard-atom shapes + write-gate contracts live in the manifest SSOT.

---

## 1. Alignment Matrix

| Layer              | Source                                                                                                               | Granularity                                                                  |
| ------------------ | -------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| **GCS paths**      | `dependencies.yaml`, `catalog.py`, schema-change docs                                                                | Per service output path templates                                            |
| **Sharding**       | UAC SSOT (per-asset-group shard-atom matrix below); `configs/sharding.*.yaml` is consumer-side only                  | v6 shard atoms with full granularity per asset_group + per-data_type         |
| **Data status**    | `data_status.py`, `catalog.py` reading manifest at the same shard atom                                               | Same as shard atom; checks down to leaf parquet via deployment-ui drill-down |
| **Missing shards** | `POST /missing-shards`                                                                                               | Compares calculated shards (UAC denominator) vs manifest `captured` rows     |
| **Service upload** | Each service's main/orchestrator routing through `ManifestWriter.record_captured` / `record_empty` / `record_failed` | Must fail entire shard if ANY upload OR write-gate pillar fails              |

---

## 2. Per-Asset-Group Shard Atom Matrix (v6 canonical)

The shard atom MUST be identical across writer atomicity boundary, manifest row key, data-status display rollup, downstream pre-flight gate, and deployment-ui drill-down. Drift between any two = silent correctness bug. Per workspace CLAUDE.md `§ Shard-granularity SSOT (CRITICAL)`:

| Asset group              | Shard atom                                                                                                                                                                                                                             | Bundling                                                                                                                                                        | `empty_confirmed` triggers                                                  |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| **CeFi spot/perp**       | `(asset_group, venue, data_type, instrument_type, instrument_id, day)`                                                                                                                                                                 | per-instrument (~35GB roots)                                                                                                                                    | source returned 200 + zero rows                                             |
| **CeFi options/futures** | `(asset_group, venue, data_type, options_chain\|futures_chain, root, day)` — `quote_asset` + `margin_type` for DERIBIT inverse vs linear                                                                                               | bundled by root with cluster validation MANDATORY (`OPTIONS_CLUSTERS` / `FUTURES_CLUSTERS` registries in UAC)                                                   | per-bundle empty                                                            |
| **TradFi futures**       | `(asset_group=tradfi, venue, data_type, instrument_type, root, day)`                                                                                                                                                                   | bundled, non-trading days pre-skipped via `venue_trading_calendar` → `empty_confirmed`                                                                          | non-trading days (holiday + session close)                                  |
| **TradFi ETFs**          | `(asset_group=tradfi, venue, data_type, instrument_type, instrument_id, day)`                                                                                                                                                          | per-instrument (IBIT, ETHA on NASDAQ)                                                                                                                           | non-trading days                                                            |
| **TradFi options**       | `(asset_group=tradfi, venue, data_type, options_chain, root, day)` — ES.OPT 11-cluster taxonomy + `combo_type` + `leg_weights` for spreads                                                                                             | bundled with cluster validation MANDATORY                                                                                                                       | non-trading + zero-volume strikes                                           |
| **DeFi**                 | `(asset_group=defi, chain, venue/protocol, data_type, instrument_id_or_protocol_id, day)`                                                                                                                                              | `chain` is a first-class v5 axis                                                                                                                                | pre-genesis dates per chain                                                 |
| **Sports per-fixture**   | `(asset_group=sports, source, data_type, league_id, fixture_id, day)` for `ODDS_SNAPSHOT` / `ODDS_MOVEMENT` / `ARBITRAGE` / `FIXTURE_STATS` / `FIXTURE_EVENTS` / `FIXTURE_LINEUPS` / `PLAYER_STATS` / `INJURIES` (when fixture-scoped) | bundled per fixture with `cluster_extractor=lambda row: row["bookmaker"]` for ODDS\_\*; `SPORTS_FIXTURE_CLUSTERS` per league-tier                               | paused-league windows (`KNOWN_COVERAGE_GAPS`) + pre-`SOURCE_COVERAGE_START` |
| **Sports day-aggregate** | `(asset_group=sports, source, data_type, league_id, day)` for `STANDINGS` / `LEAGUES` / `TEAMS` / `REFEREES` / `COACHES` / `ROUNDS`                                                                                                    | per-league-day                                                                                                                                                  | paused-league + pre-launch                                                  |
| **Prediction**           | `(asset_group=prediction, venue, data_type=prediction_canonical_question_group, canonical_question_group, market_id, day)` — bundled by canonical_question_group with per-`market_id` rows                                             | bundled with `cluster_extractor=lambda row: row["market_id"]` and `PREDICTION_GROUPS` registry per cadence (HOURLY=24/day, DAILY=1/day, ELECTION=1 over months) | pre-`market_created_at` + post-`settlement_time` per market lifecycle       |

**Live = batch principle**: same shard atom + same data_types + same fields in live and batch; only the SOURCE differs. Historical writes timestamped with the live-pipeline-equivalent `available_at`.

---

## 3. GCS Path Alignment

All path templates must match across:

- `deployment-service/configs/dependencies.yaml` (outputs.path_template)
- `deployment-service/deployment_service/catalog.py` (SERVICE_GCS_CONFIGS)
- Schema-change docs: `docs/schema-change/02_*.md` through `08_*.md`
- Actual service implementations (`gcs_path_utils.py`, feature writers, MTDS `raw_tick_hive.py` SSOT)

**Hive vocab**: `asset_group=` is canonical for new writes (per `market_tick_data_service/raw_tick_hive.py` SSOT — `RAW_TICK_ASSET_GROUP_HIVE_KEY = "asset_group"`); `category=` is the legacy form (`RAW_TICK_ASSET_GROUP_HIVE_KEY_LEGACY`) preserved on disk without re-keying. Readers must try canonical first then fall back to legacy. Phantom audit regex matches both: `(?:category|asset_group)=`. See workspace CLAUDE.md `§ Asset-group vocabulary` for full rationale.

### Key Paths (Current Implementation)

| Service | Path Template | Notes |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --- | --- | -------- | ---------------------------------------------------------------------------------------------------------------------- |
| **market-tick-data-service** | `raw_tick_data/by_date/day={date}/data_type={data_type}/instrument_type={asset_group}/venue={venue}/{instrument}.parquet` | No `symbol=` in path; filename = `{instrument}.parquet`. CeFi options/futures bundle to `instrument_type=options_chain/...{underlying}.parquet`. |
| **market-tick-data-service** (DeFi) | `raw_tick_data/by_date/day={date}/data_type={data_type}/asset_group=defi/chain={chain}/venue={venue}/{instrument}.parquet` | `chain=` is a first-class v5 axis between `asset_group=defi` and `venue=`. |
| **market-tick-data-service** (sports per-fixture) | `raw_tick_data/by_date/day={date}/data_type={data_type}/asset_group=sports/source={source}/league={league_id}/fixture={fixture_id}/{F}.parquet` | Per-fixture sharding (writegate Phase 2.B). League stays as a higher-level rollup grouping for data-status panel filtering, NOT the shard atom. Writegate Phase 2.B migrates from prior `(bookmaker, league)` grouping. |
| **market-tick-data-service** (prediction) | `raw_tick_data/by_date/day={date}/data_type=prediction_canonical_question_group/asset_group=prediction/venue={venue}/canonical_question_group={cqg}/{market_id}.parquet` | Plan A migration target. Pre-Plan A: per-base_asset shards at `data_type=BTC                                                                                                                                            | ETH | SPX | FOOTBALL | OTHER`. Reconciler script splits per-base_asset parquets into per-canonical_question_group parquets at Plan A Phase 3. |
| **market-data-processing** | `processed_candles/by_date/day={date}/timeframe={tf}/data_type={type}/{asset_group}/{venue}/{instrument}.parquet` | Chain bundles: `options_chain/{venue}/`, `futures_chain/{venue}/`. v6 columns `quote_asset` + `margin_type` carried in row schema for DERIBIT inverse vs linear disambiguation. |
| **features-delta-one** | `by_date/day={date}/feature_group={group}/timeframe={tf}/{instrument}.parquet` | `available_at` column required per row (writegate `assert_available_at_present` guard). |
| **features-calendar** | `calendar/asset_group={asset_group}/by_date/day={date}/features.parquet` or `events.parquet` | Shared bucket. `category=` legacy hive vocab still on disk; reader tries canonical first then falls back. |
| **features-volatility** | `by_date/day={date}/feature_group={group}/timeframe={tf}/{underlying}.parquet` | |
| **features-sports** | `by_date/day={date}/feature_group={group}/league_id={league}/fixture_id={fixture}/{F}.parquet` (per-fixture); aggregate features at day-aggregate path | Per-fixture sharding aligned with MTDS sports per-fixture. `_FETCH_COMPLETED_AT` cache populates per-table `available_at` for 8 reference tables (writegate Phase 2.C). |
| **features-onchain** | `by_date/day={date}/feature_group={group}/timeframe={tf}/chain={chain}/{instrument}.parquet` | `chain=` first-class axis. |

---

## 4. Sharding Dimensions vs Data Status

Data status checks completion at the same granularity as shard dimensions:

| Service                                             | Shard Dimensions (v6 canonical)                                                                                             | Data Status Check                                                                                                                                                     |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **instruments-service**                             | asset_group, venue, [chain], [league_id], date                                                                              | venue × [chain] × [league_id] × date. Per-league pre-flight for sports per-league entities (api_football SFI_PROGRESSIVE_STATS / PLAYER_VALUES).                      |
| **market-tick-data-service** (CeFi spot/perp)       | asset_group, venue, data_type, instrument_type, instrument_id, [quote_asset, margin_type], date                             | venue × instrument_type × data_type × dates × `(quote_asset, margin_type)` for DERIBIT v6 disambiguation                                                              |
| **market-tick-data-service** (CeFi/TradFi bundles)  | asset_group, venue, data_type=options_chain\|futures_chain, root, [quote_asset, margin_type, combo_type, leg_weights], date | venue × instrument_type × data_type × root × dates. **Cluster validation enforced at write** (`expected_root_clusters` mandatory per UAC `BUNDLED_DATA_TYPES`).       |
| **market-tick-data-service** (DeFi)                 | asset_group=defi, chain, venue/protocol, data_type, instrument_id_or_protocol_id, date                                      | chain × protocol × data_type × dates                                                                                                                                  |
| **market-tick-data-service** (sports per-fixture)   | asset_group=sports, source, data_type, league_id, fixture_id, date                                                          | league × source × data_type × fixture × dates. Per-fixture drill-down in deployment-ui.                                                                               |
| **market-tick-data-service** (sports day-aggregate) | asset_group=sports, source, data_type, league_id, date                                                                      | league × source × data_type × dates                                                                                                                                   |
| **market-tick-data-service** (prediction)           | asset_group=prediction, venue, data_type=prediction_canonical_question_group, canonical_question_group, market_id, date     | venue × canonical_question_group × market_id × dates. Lifecycle bounds enforced at write (no ticks before `market_created_at`, no new ticks after `settlement_time`). |
| **market-data-processing**                          | asset_group, venue, data_type, instrument_type, instrument_id\|root, timeframe, date                                        | venue × instrument_type × data_type × timeframe × dates                                                                                                               |
| **features-delta-one**                              | asset_group, feature_group, timeframe, instrument_id, date                                                                  | feature_group × timeframe × dates                                                                                                                                     |
| **features-sports**                                 | asset_group=sports, feature_group, league_id, fixture_id, horizon, date                                                     | feature_group × league × horizon × fixture × dates                                                                                                                    |
| **features-onchain**                                | asset_group=defi, feature_group, chain, timeframe, instrument_id, date                                                      | feature_group × chain × timeframe × dates                                                                                                                             |
| **features-calendar**                               | asset_group, feature_group, date                                                                                            | feature_group × date                                                                                                                                                  |

---

## 5. Shard-Level Failure Behavior — 4-pillar write-gate (post-2026-05-06)

Every `record_captured` call is gated by 4 pillars. Failure of any pillar → `record_failed(<typed_reason>)` instead of writing the parquet. NO partial passes.

| Pillar                                         | Gate                                                                                                                                                                                                    | Failure mode                                                         |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| **Row count > 0**                              | Mandatory unless source response was legitimately empty (then `record_empty`, not `record_captured`).                                                                                                   | `record_failed(EmptyAfterFilterError)`.                              |
| **NaN ratio per column < threshold**           | Per-feature-group thresholds in UAC `nan_thresholds.NAN_RATIO_THRESHOLDS`. Currently inlined per-service; Plan B lifts to UTL `write_gate_helpers.check_nan_ratio`.                                     | `record_failed(NanRatioExceededError(column, observed, threshold))`. |
| **Schema matches contract**                    | Required columns + types match UAC schema declaration. Existing `ParquetSchemaEnforcer`. Includes `available_at` column.                                                                                | `record_failed(SchemaMismatchError)`.                                |
| **Cluster coverage ≥ expected** (BUNDLED only) | For `data_type ∈ BUNDLED_DATA_TYPES`, `expected_root_clusters` + `cluster_extractor` kwargs are MANDATORY (UTL guard raises `MissingClusterValidationError` if absent; QG STEP 5.64 statically checks). | `record_failed(ClusterCoverageError(missing, observed))`.            |

If ANY pillar fails OR ANY upload within a shard fails, the service MUST:

1. Log `FAILED` via `log_event("FAILED", severity="ERROR", details={"reason": "<typed_error_name>", "row_key": "..."})` (UTL).
2. Exit with code 1 (`sys.exit(1)`) at the orchestrator boundary.
3. NOT report success when some uploads succeeded and others failed.

### Three-category empty-output decision (every per-shard adapter)

Per workspace CLAUDE.md `§ Three-category empty-output decision`:

| Path                                | Condition                                                                              | Manifest verb                                                                      | Notes                                                                                                                                                                                |
| ----------------------------------- | -------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **A. Honest absence**               | Source returned 0 ticks for the requested window.                                      | `record_empty(row_key, attempted_at)`                                              | Counts in denominator only.                                                                                                                                                          |
| **B. Upstream timestamp bias**      | Source returned ticks; ALL fall outside the requested day after `interval_idx` filter. | `record_failed(UpstreamTimestampBiasError(observed_dates, expected_day, n_ticks))` | UPSTREAM bug — partition mislabeled at MTDS write-time, OR source replay covered wrong window, OR clock-skew. Paired upstream MTDS partitioner-validation fix at `raw_tick_hive.py`. |
| **C. Mid-process malformed fields** | Rows in window but downstream calc dropped due to NaN/malformed source fields.         | `record_failed(MalformedTickFieldError(field, n_dropped, sample_values))`          | Data-quality bug; sample values surface for triage.                                                                                                                                  |

NO fourth category. NO silent NaN placeholder rows. `_create_empty_output()` is BANNED from `base_adapter` (writegate Phase 2.A deletes it across MDPS' 37 callsites).

### Implementation patterns per service

| Service                            | Pattern                                                                                                                                                                                                                                                                                                                      |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **market-tick-data-service**       | `download_handler.py` + `options_orchestrator.py`: `ErrorWarningCounter` attached to root logger; `shard_success = len(failed_dates) == 0 and error_counter.error_count == 0`. Bundle adapters route through `record_captured` with cluster validation kwargs (writegate Phase 2.B).                                         |
| **market-data-processing-service** | `orchestration_service.py`: `success = len(errors) == 0 and len(processed_timeframes) == len(timeframes)`. Adapters raise `EmptyAfterFilterError` / `UpstreamTimestampBiasError` / `MalformedTickFieldError` per A/B/C decision (writegate Phase 2.A). Holiday early-return writes closed-market candles for all timeframes. |
| **features-delta-one-service**     | `orchestration_service.py` + `feature_writer.py`: `success_count == len(instruments)` (CLI-filtered). Holidays skipped (`success_count == days_attempted`). `available_at` column required per row.                                                                                                                          |
| **features-sports-service**        | `batch_handler.py:238-338` `_stamp_available_at` per-table per-source. 4 stub exports (`fixture_lineups`, `fixture_player_stats`, `coaches`, `rounds`) wired to `record_empty(row_key)` for honest absence (writegate Phase 2.C). `_ensure_timestamp` shim deleted.                                                          |

---

## 6. Unified Events Interface for Failures

**Standard lifecycle events:** `STARTED` / `STOPPED` / `FAILED` are in `STANDARD_LIFECYCLE_EVENTS`. Plus per-shard typed events for the new write-gate pillars:

- `RAW_TICK_PARTITION_MISMATCH` — MTDS partitioner validation rejects a tick whose `timestamp.date() != day_partition_key` (writegate Phase 2.B).
- `CLUSTER_COVERAGE_INSUFFICIENT` — bundle adapter's expected clusters missing at write time.
- `LIFECYCLE_BOUNDS_VIOLATED` — prediction adapter rejected a tick outside `[market_created_at, settlement_time)`.
- `LOOKAHEAD_BIAS_DETECTED` — features-\* compute consumed a row with `available_at > target_ts`.

**Usage:**

- **UCS (unified_trading_library):** `log_event("FAILED", details=str)` — second arg is details string.
- **unified-trading-library:** `log_event("FAILED", severity="ERROR", details={"reason": "...", "shard": "..."})` — details is dict.

For shard-level failures, include:

```python
log_event("FAILED", severity="ERROR", details={
    "reason": "cluster_coverage_insufficient",  # or upstream_timestamp_bias, etc.
    "missing_clusters": ["E1A", "E2A"],
    "observed_clusters": ["ES", "EW1", "EW2"],
    "shard_dimensions": {
        "asset_group": "tradfi",
        "venue": "CME",
        "data_type": "options_chain",
        "root": "ES.OPT",
        "date": "2026-01-01",
    },
})
```

**Per-VM shard isolation events:**

- `MANIFEST_PER_VM_SHARD_WRITE` — written when `MANIFEST_PER_VM_SHARDS=true` is active and a per-VM shard parquet lands at `_index/per_vm/{vm_name}.parquet`.
- `MULTI_WORKER_WITHOUT_SHARD_ISOLATION` — fired by UTL `MultiWorkerWithoutShardIsolationError` runtime guard when multi-process detection fires but envvar isn't set.

---

## 7. Startup Dependency Validation — `DependencyError(fail_fast=True)`

Per workspace CLAUDE.md `§ Honest absence vs fake placeholders`, the dependency boundary between services is the right place to fail-fast on missing upstream data. Categories:

1. **Expected upstream-source gap** (source genuinely doesn't provide data for that key — venue didn't exist on date, instrument delisted, league paused, pre-genesis). Action: `record_empty(row_key, attempted_at)`. NaN downstream is fine.
2. **Unexpected upstream-pipeline gap** (upstream service was supposed to capture but didn't). Action: `DependencyError(fail_fast=True)` at the boundary; resolve by running the upstream backfill, NOT by `--skip-dependency-check`.
3. **Reader / schema-drift bug** (data IS in the upstream bucket but reader can't find it). Action: RAISE LOUD; fix the bug. NEVER silently produce empty placeholder rows.

**Manifest-aware dependency check:** `check_upstream_availability` reads the upstream service's manifest and asserts every expected `row_key` has `capture_status == "captured"` (NOT just any row — `attempted_failed` and `empty_confirmed` are separate states that may indicate honest absence vs unresolved upstream bugs).

**Missing-shards endpoint:** `POST /missing-shards` compares the UAC denominator (per-(asset_group, data_type) start dates clipped by `SOURCE_COVERAGE_START` / `KNOWN_COVERAGE_GAPS` / `venue_trading_calendar`) against manifest `captured` rows at the canonical shard atom.

---

## 8. Per-VM Shard Isolation (workspace rule)

Every multi-worker backfill (multiple chunk processes locally OR multiple GCE VMs writing to the same manifest) MUST set `VM_NAME=<unique>` + `MANIFEST_PER_VM_SHARDS=true` per worker. UTL runtime guard: `ManifestWriter.__init__` raises `MultiWorkerWithoutShardIsolationError` when multi-process detection fires AND per-VM shard isolation isn't set. New base-service.sh QG STEP 5.66 AST-walks launcher scripts that fork multi-process; asserts envvar setting.

Manifest consolidator merges per-VM shards under `_index/per_vm/{vm_name}.parquet` into the canonical `_index/availability_index.parquet` with last-writer-wins on identical row_key.

Reference incident **2026-05-04**: instruments-service `00f6352` + `619a32e` chunk workers without isolation clobbered each other's manifest entries; commits were the per-script fixes; Plan C codifies the workspace rule + QG step.

---

## 9. Schema-Change Docs vs Implementation

Schema-change docs (`docs/schema-change/02_*.md`, etc.) describe migration options. Current implementation: v6 manifest schema (`MANIFEST_SCHEMA_VERSION = 6`) per `unified-trading-library/unified_trading_library/manifest_writer.py`. v6 added `quote_asset`, `margin_type`, `combo_type`, `leg_weights` columns (DERIBIT inverse vs linear disambiguation; multi-leg synthetic instrument metadata).

Path templates use `key=value` hive format. Filename = `{instrument}.parquet` (no `symbol=` in path). See `GCS_AND_SCHEMA.md` for current canonical paths.

**Active schema migrations** (writegate plan + predictions plan):

- Sports per-fixture sharding migration (writegate Phase 2.B + Phase 3.A reconciler) — old `(bookmaker, league)` shard keys flip to `attempted_failed[reason=ShardSchemaMigrated]`; new per-fixture shards write fresh.
- Polymarket canonical_question_group migration (predictions Plan A Phase 3.A reconciler) — old `data_type=<base_asset>` parquets re-grouped by `(canonical_question_group, day)` via UAC classifier with stability hash.
- `category=` → `asset_group=` GCS migration (writegate Phase 3.A) — per-asset_group migration scripts; legacy `category=` fallback reader deleted only after 100% migration verified.

---

## 10. Per-Service Audit: "Any Data Means All Data"

Status as of 2026-05-06 (post-Phase 0 audit synthesis); writegate plan migrations in progress.

| Service                            | Status             | Location                                         | Notes                                                                                                                                                                                                                                              |
| ---------------------------------- | ------------------ | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **instruments-service**            | ✅ Aligned         | `cloud_instrument_storage.py`                    | Returns `all_successful`; sets False on any venue upload failure. Per-league pre-flight for sports SFI_PROGRESSIVE_STATS + PLAYER_VALUES (commit `7bfa877`).                                                                                       |
| **market-tick-data-service**       | ⚠️ In progress     | `download_handler.py`, `options_orchestrator.py` | download: `shard_success = len(failed_dates)==0 and error_count==0`. Bundle adapters: `all_succeeded = successful == total_underlyings`. **Pending writegate Phase 2.B**: cluster validation kwargs at orchestrator.py:1940 (amendment F to user). |
| **market-data-processing-service** | ⚠️ In progress     | `orchestration_service.py`                       | `success = len(errors) == 0 and len(processed_timeframes) == len(timeframes)`. **Pending writegate Phase 2.A**: 37 `_create_empty_output` callsites converted to A/B/C decision tree; `_create_empty_output` deleted from `base_adapter`.          |
| **features-delta-one-service**     | ✅ Aligned         | `orchestration_service.py`, `feature_writer.py`  | orchestration: `success_count == len(instruments)`. feature_writer: `success_count == days_attempted`. **Pending Plan B**: feature_group → required_inputs DAG SSOT lift to UAC for LookaheadBiasError enforcement.                                |
| **features-calendar-service**      | ✅ Aligned         | `batch_handler.py` L197-210                      | `if total_failed > 0: log_event("FAILED", ...); sys.exit(1)`.                                                                                                                                                                                      |
| **features-volatility-service**    | ✅ Aligned         | `batch_handler.py` L199                          | Returns `success_count == len(groups)`.                                                                                                                                                                                                            |
| **features-onchain-service**       | ✅ Aligned         | `batch_handler.py` L187                          | Returns `success_count == len(groups)`. **Pending Plan B**: NaN-ratio gate lift to UTL.                                                                                                                                                            |
| **features-sports-service**        | ⚠️ In progress     | `batch_handler.py:238-338` `_stamp_available_at` | `_stamp_available_at` ~80% wired (writegate Phase 2.C). **Pending**: 4 stub exports (`fixture_lineups`, `fixture_player_stats`, `coaches`, `rounds`) wire to `record_empty(row_key)`; `_FETCH_COMPLETED_AT` cache build for 8 reference tables.    |
| **ml-training-service**            | ⚠️ Different model | Handlers return `HandlerResult(success=...)`     | Shards by instrument/timeframe/target; partial success (N-1 of N models) may be acceptable for training. Verify per-handler.                                                                                                                       |
| **ml-inference-service**           | ✅ Aligned         | `cli/main.py` L168-173                           | `if error_count > 0: log_event("FAILED", ...); sys.exit(1)`.                                                                                                                                                                                       |
| **strategy-service**               | ✅ Aligned         | `batch_handler.py` L294                          | `aggregated["success"] = len(errors) == 0`.                                                                                                                                                                                                        |
| **execution-service**              | N/A                | Live/backtest                                    | Different model; writes results per run.                                                                                                                                                                                                           |

### Active migrations (post-2026-05-06)

1. **Writegate Phase 2.A — MDPS empty-output A/B/C**: 37 callsites converted across `app/adapters/{cefi,defi,tradfi,sports}/`. `_create_empty_output` deleted from `base_adapter`. New typed exceptions raised per A/B/C; orchestrator's `_handle_empty_tick_data` routes to `record_empty` / `record_failed(UpstreamTimestampBiasError)` / `record_failed(MalformedTickFieldError)`.
2. **Writegate Phase 2.B — MTDS cluster validation + per-fixture sharding**: cluster kwargs wired at `orchestrator.py:1940` callsite (Option α — single SSOT, awaiting Ikenna review per amendment F). Sports per-fixture_id shard granularity restored from `(bookmaker, league)` to full v6 spec.
3. **Writegate Phase 2.C — features-sports `available_at` correctness**: 4 stub exports + `_FETCH_COMPLETED_AT` cache + per-table stamping per `UAC.AVAILABILITY_AT_SEMANTICS`.
4. **Predictions Plan A — Polymarket canonical_question_group + lifecycle**: UAC SSOT build + instruments-service lifecycle ingestion + MTDS adapter migration + GCS reconciler.
5. **Plan B — UTL/UAC lift triple**: DAG SSOT + NaN-ratio gate + phantom-audit drift-probe.
6. **Plan C — pre-flight + concurrency hardening**: UTL `check_shard_freshness` tightening + per-VM shard isolation rule + `category=` → `asset_group=` migration runbook.
7. **Plan D — multi-source merge**: SOURCE_PRIORITY extended to multi-entry merge with per-field provenance.

See active plans in `unified-trading-pm/plans/active/` for full execution detail.
