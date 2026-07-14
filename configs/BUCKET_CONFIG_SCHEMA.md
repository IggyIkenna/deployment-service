# Bucket Configuration Schema

This document describes the configuration schema for `configs/bucket_config.yaml`.

**Rewritten 2026-07-14** (`bucket_estate_consolidation_to_sub100_2026_07_13.md`
Deferred #8): `setup-buckets.py` derives every service/data bucket name from the
canonical `configs/cloud-providers.yaml` matrix via the UTL SSOT resolver
(`unified_trading_library.resolve_bucket_name`) — mirroring
`terraform/gcp/canonical_buckets.tf`'s `for_each` derivation. This file is now
ONLY the registry of genuine, hand-managed infra buckets that fall outside
that per-kind matrix. The `aws_bucket_mappings`, `test_buckets`, and
`validation` sections previously documented here were consumed solely by the
old local resolver (`resolve_bucket_name()` / `get_test_bucket_name()` /
`convert_to_aws_bucket_name()`) this rewrite deleted, and have been removed
from both the yaml and this doc.

## Configuration Structure

### `defaults`

Default values for cloud providers (region fallback only — `project_id` here
is a vestigial literal nothing reads; the real project/account ID always comes
from `--project-id` or the environment):

```yaml
defaults:
  gcp:
    project_id: test-project
    region: asia-northeast1
  aws:
    region: ap-northeast-1
```

### `shared_bucket_services` / `service_categories`

Not read by `setup-buckets.py` any more (the old dependencies.yaml-driven
per-service resolver these fed is gone). Kept because
`system-integration-tests/tests/conftest.py`'s own, separate
`dependencies.yaml`-driven bucket enumeration (`required_gcs_buckets` fixture)
still reads them.

```yaml
shared_bucket_services:
  - features-calendar-service
  - alerting-service
  - reconciliation-service

service_categories:
  restricted_categories:
    volatility: [cefi, tradfi] # No DEFI for volatility
  default_categories: [cefi, tradfi, defi, sports, prediction]
```

### `infrastructure_buckets`

The registry of genuine, hand-managed infra buckets `setup-buckets.py`
provisions in addition to the canonical `cloud-providers.yaml` matrix
(terraform-state, deployment-orchestration, build-metadata,
databento-batch-registry, backtest-results, client-reporting-data, events,
unified-deployment-state, ...). These never get a test-tier sibling.

```yaml
infrastructure_buckets:
  gcp:
    - name_template: "terraform-state-{project_id}"
      service: infrastructure
      type: infrastructure
      category: ALL
  aws:
    - name_template: "unified-trading-terraform-state-{project_id}"
      service: infrastructure
      type: infrastructure
      category: ALL
```

### `bucket_settings`

Cloud-specific bucket creation settings — used ONLY for `infrastructure_buckets`
entries now (canonical data buckets get hardcoded settings in
`setup-buckets.py` that mirror `terraform/gcp/canonical_buckets.tf`: STANDARD
storage class, uniform bucket-level access, no versioning, STANDARD->COLDLINE
@ 60 days):

```yaml
bucket_settings:
  gcp:
    storage_class: STANDARD
    uniform_bucket_access: true
    versioning: true
    lifecycle_rules:
      production:
        age_days: 365
        action: SetStorageClass
        storage_class: NEARLINE
      test:
        age_days: 30
        action: SetStorageClass
        storage_class: NEARLINE
  aws:
    versioning: true
    encryption:
      algorithm: AES256
    # ... more settings
```

## Configuration Benefits

1. **Centralized Configuration**: All genuine infra-bucket settings in one place
2. **Maintainability**: Clear structure makes updates easier
3. **Reusability**: Configuration can be shared across tools

## Template Variables

- `{project_id}` - GCP project ID or AWS account ID
