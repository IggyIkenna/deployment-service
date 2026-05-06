<!-- POST_PLAN_BANNER_2026_05_06 -->

> **POST-PLAN REALITY (2026-05-06)** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) BEFORE making code or doc changes informed by this doc. This doc is partially stale: may describe shard atom, GCS path conventions, schema validation, or live-mode behaviour that's evolving (per-fixture sports sharding, canonical_question_group for predictions, cluster validation mandatory at record_captured, available_at column required, per-VM shard isolation rule for concurrent backfills). The post-plan-reality doc lists the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation mandatory at record_captured, per-row write-time `available_at`, prediction lifecycle timing, temporary state must have named successor, per-VM shard isolation, etc.) plus the active plans where the canonical post-plan reality is being implemented. If this doc and the active plans disagree, the plans win. If you find a contradiction the plans don't address, flag to user — don't decide unilaterally.

# Deployment Service — GCS Paths

## Bucket Pattern

State and build artifacts use GCS. Bucket from config or STATE_BUCKET env.

## Path Templates

- Deployment state: `{bucket}/deployment/state/`
- Build logs: via Cloud Build API

Variables: `{bucket}` from config.
