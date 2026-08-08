"""
Bucket IAM per-tier write-isolation negative tests.

plans/active/bucket_iam_write_protection_per_tier_2026_06_09.md P2.3: prove the per-tier
service accounts (deployment-service/terraform/gcp/bucket_iam_per_tier_sa.tf) are actually
IAM-denied a cross-tier write, not just code-level-guarded by the bucket-name resolver.

Terminology note (2026-08-02): the plan's original P2.3 wording ("ENVIRONMENT=staging write
to a *-prod-* bucket") predates the 2026-07-13 operator ruling that permanently retired the
dev/stg bucket tiers workspace-wide (canonical_buckets.tf:44-46 -- "prd + test are the only
provisioned tiers"; uts-stg-sa is left permanently unbound with zero IAM grants, so it isn't
a meaningful non-prod-tier test subject any more -- it would 403 on *everything*, not just
prod, which doesn't exercise the tier-scoping condition at all). uts-test-sa is the ratified
replacement (see issues/bucket_iam_per_tier_dev_stg_retired_ssot_contradiction_2026_07_27.md)
-- these tests use the real -prd-/-test- tier pair instead.

Uses Bucket.test_iam_permissions() (the GCS "would I be allowed to do X" check) rather than
attempting a real object write -- this exercises the exact same IAM Condition CEL evaluation
bucket_iam_per_tier_sa.tf declares, with no side effects, no cleanup needed, and no risk to
the target bucket's content.

These tests impersonate the per-tier service accounts from the ambient identity (ADC), same
self-service impersonation pattern used to live-verify P2.2c
(codex/05-infrastructure/orchestrator-cloud-identity-self-service.md). They self-skip (not
fail) if the ambient identity lacks roles/iam.serviceAccountTokenCreator on a target SA --
matching every other test in this module's "skip, don't fail, on a permission gap" convention
-- so this file works whether or not that impersonation grant is standing in a given
environment.

To run:
    RUN_INTEGRATION=true GCP_PROJECT_ID=<your-gcp-project-id> \
        pytest tests/integration/test_bucket_iam_tier_isolation.py -v
"""

import os

import pytest

pytestmark = pytest.mark.integration

DEFAULT_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "test-project")

# Group A (market-data-tick, cefi) is one of the AG-scoped families both
# uts-prd-sa's and uts-test-sa's IAM conditions cover (bucket_iam_per_tier_sa.tf
# group_a_market_data_tick_ag_prefixes) -- confirmed live to exist in both tiers.
PRD_BUCKET = f"market-data-tick-cefi-prd-{DEFAULT_PROJECT_ID}"
TEST_BUCKET = f"market-data-tick-cefi-test-{DEFAULT_PROJECT_ID}"

WRITE_PERMISSION = "storage.objects.create"

TIER_SA_EMAILS = {
    "prd": f"uts-prd-sa@{DEFAULT_PROJECT_ID}.iam.gserviceaccount.com",
    "test": f"uts-test-sa@{DEFAULT_PROJECT_ID}.iam.gserviceaccount.com",
    "migration": f"uts-migration-sa@{DEFAULT_PROJECT_ID}.iam.gserviceaccount.com",
}


def _impersonated_storage_client(target_sa_email: str):
    """Build a storage.Client authenticating as target_sa_email via impersonation.

    Skips the calling test if the ambient identity can't mint a token for the
    target SA (missing roles/iam.serviceAccountTokenCreator) -- an environment
    gap, not a test failure, matching this module's existing skip convention.
    """
    import google.auth
    import google.auth.transport.requests
    from google.auth import impersonated_credentials
    from google.auth.exceptions import RefreshError
    from google.cloud import storage

    source_credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    target_credentials = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=target_sa_email,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        lifetime=60,
    )
    try:
        target_credentials.refresh(google.auth.transport.requests.Request())
    except RefreshError as e:
        pytest.skip(f"Cannot impersonate {target_sa_email} from ambient identity: {e}")

    return storage.Client(project=DEFAULT_PROJECT_ID, credentials=target_credentials)


def _can_write(target_sa_email: str, bucket_name: str) -> bool:
    """True iff target_sa_email would be allowed WRITE_PERMISSION on bucket_name.

    No object is ever created -- testIamPermissions is a pure permission check.
    """
    client = _impersonated_storage_client(target_sa_email)
    bucket = client.bucket(bucket_name)
    granted = bucket.test_iam_permissions([WRITE_PERMISSION])
    return WRITE_PERMISSION in granted


class TestPrdSaTierIsolation:
    """uts-prd-sa: objectAdmin scoped to *-prd-* buckets only (bucket_iam_per_tier_sa.tf
    uts_prd_objectadmin_group_a/uts_prd_objectadmin_group_b)."""

    @pytest.mark.integration
    def test_prd_sa_can_write_prd_bucket(self):
        assert _can_write(TIER_SA_EMAILS["prd"], PRD_BUCKET) is True

    @pytest.mark.integration
    def test_prd_sa_cannot_write_test_bucket(self):
        assert _can_write(TIER_SA_EMAILS["prd"], TEST_BUCKET) is False


class TestTestSaTierIsolation:
    """uts-test-sa: objectAdmin scoped to *-test-* buckets only (bucket_iam_per_tier_sa.tf
    uts_test_objectadmin_group_a/uts_test_objectadmin_group_b) -- the ratified replacement
    for the retired dev/stg non-prod tier (see module docstring)."""

    @pytest.mark.integration
    def test_test_sa_can_write_test_bucket(self):
        assert _can_write(TIER_SA_EMAILS["test"], TEST_BUCKET) is True

    @pytest.mark.integration
    def test_test_sa_cannot_write_prd_bucket(self):
        assert _can_write(TIER_SA_EMAILS["test"], PRD_BUCKET) is False


class TestMigrationSaCurrentState:
    """uts-migration-sa: the sanctioned cross-tier write exception
    (bucket_iam_write_protection_per_tier_2026_06_09.md's own "Open design decisions").
    P2.2f DONE 2026-08-03: uts-migration-sa now holds roles/storage.objectAdmin
    project-wide (unconditioned -- generic cross-family tool, see bucket_iam_per_tier_sa.tf
    uts_migration_objectadmin comment for why a CEL condition is not achievable here).
    This test confirms the grant is live (positive case -- migration SA CAN write prd)."""

    @pytest.mark.integration
    def test_migration_sa_can_write_prd_bucket(self):
        assert _can_write(TIER_SA_EMAILS["migration"], PRD_BUCKET) is True
