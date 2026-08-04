# ---------------------------------------------------------------------------
# Default compute SA security hardening
# Audit: P3.1 of plans/active/issues/
#        bucket_iam_p2_tier_sa_scope_gap_and_default_compute_sa_overprivilege_2026_07_30.md
#
# The GCP default compute SA (PROJECT_NUMBER-compute@developer.gserviceaccount.com)
# is the identity that runs 49 of the 169 VM launchers still not wired to a
# per-tier SA. It held 29 unconditional project-wide roles — a bigger live
# exposure than the god-SA (unified-trading-sa) that the parent plan was created
# to close (roles/storage.admin alone allows bucket IAM policy rewrites;
# roles/iam.serviceAccountTokenCreator allows impersonating any other SA).
#
# Audit method (2026-08-04):
#   gcloud projects get-iam-policy central-element-323112 \
#     --flatten="bindings[].members" \
#     --filter="bindings.members:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
#     --format="value(bindings.role)" | sort
#
# Full 29-role pre-hardening set:
#   roles/artifactregistry.reader       KEEP — VMs pull images during setup
#   roles/bigquery.admin                REMOVED — dataEditor+jobUser+user is sufficient
#   roles/bigquery.dataEditor           KEEP — VMs write BQ rows
#   roles/bigquery.jobUser              KEEP — VMs run BQ query jobs
#   roles/bigquery.user                 KEEP — BQ dataset metadata (list/get)
#   roles/browser                       REMOVED — no practical use for data VMs
#   roles/cloudbuild.builds.viewer      REMOVED — VMs don't view Cloud Build
#   roles/cloudfunctions.invoker        KEEP — conservative; monitor for removal
#   roles/cloudscheduler.admin          REMOVED — VMs never create/modify schedulers
#   roles/compute.instanceAdmin.v1      KEEP — VMs self-delete on completion
#   roles/container.clusterViewer       REMOVED — VMs never access GKE
#   roles/container.developer           REMOVED — VMs never deploy to GKE
#   roles/dataflow.worker               REMOVED — VMs are not Dataflow workers
#   roles/datastore.user                KEEP — Firestore deployment-registry writes
#   roles/developerconnect.readTokenAccessor  REMOVED — no evidence of use
#   roles/eventarc.eventReceiver        REMOVED — VMs are not Eventarc receivers
#   roles/firebaseauth.admin            REMOVED — VMs never manage Firebase auth
#   roles/iam.serviceAccountTokenCreator  REMOVED — CRITICAL: VMs must not impersonate SAs
#   roles/logging.logWriter             KEEP — VMs write Cloud Logging
#   roles/logging.viewer                REMOVED — VMs write logs, don't view others'
#   roles/pubsub.publisher              KEEP — VMs publish events
#   roles/pubsub.subscriber             KEEP — alerting-relay VM subscribes (alerting_relay_pubsub.tf)
#   roles/run.developer                 REMOVED — VMs never deploy Cloud Run services
#   roles/run.invoker                   KEEP — conservative; some VMs may call Cloud Run endpoints
#   roles/secretmanager.secretAccessor  KEEP — VMs read API keys / credentials from GSM
#   roles/storage.admin                 REMOVED — CRITICAL: objectAdmin is sufficient;
#                                         admin uniquely adds bucket IAM policy writes
#   roles/storage.objectAdmin           KEEP — VMs write data/logs/signals to GCS
#   roles/storage.objectCreator         KEEP — redundant with objectAdmin; harmless
#   roles/storage.objectViewer          KEEP — VMs read tarballs and data from GCS
#
# Removals were applied via gcloud projects remove-iam-policy-binding
# (not Terraform, since these grants pre-dated this repo's IAM management).
# The 13 removed roles and their removal commands are recorded in:
#   scripts/iam/remove_default_compute_sa_overprivilege.sh
#
# The roles/datastore.user grant (added 2026-07-24 for deployment-registry
# Firestore dual-write) is managed in main.tf as
# google_project_iam_member.default_compute_sa_datastore_user — it is NOT
# re-declared here to avoid Terraform state conflicts.
# ---------------------------------------------------------------------------
