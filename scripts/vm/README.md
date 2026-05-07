# VM Deployment Scripts

## Three Deployment Approaches

The system supports three ways to get code + deps onto a GCE VM:

| Approach         | Script                       | When to Use                    | Startup Time                   |
| ---------------- | ---------------------------- | ------------------------------ | ------------------------------ |
| **Tarball**      | `setup-data-pipeline-vm.sh`  | Backfills, ad-hoc runs, dev    | ~3-5 min (install from source) |
| **Docker image** | `backends/vm.py` (VMBackend) | Production batch, auto-scaling | ~1-2 min (pull image)          |
| **SSH manual**   | Direct `gcloud compute ssh`  | Quick one-off debugging        | Immediate (if venv exists)     |

The deployment-UI should offer a toggle between these approaches.

## Per-category launcher quick reference

| Script                                  | Category             | Notes                                                                            |
| --------------------------------------- | -------------------- | -------------------------------------------------------------------------------- |
| `launch-cefi-sharded-backfill.sh`       | CEFI / TRADFI        | Tardis-backed download fleet. Year × venue × heavy/light shards.                 |
| `launch-canonical-migration-vm.sh`      | any                  | Layout-rewrite migration (existing data → canonical partitions). NOT a download. |
| `launch-canonical-smoke-vm.sh`          | CEFI / TRADFI / DEFI | 1-day write to TEST buckets (`IS_TEST_RUN=true`). Safe smoke.                    |
| `launch-mdps-backfill-vm.sh`            | CEFI / TRADFI / DEFI | MDPS candle aggregation. SPORTS/PREDICTION require dep-checker support.          |
| `launch-features-backfill-vm.sh`        | any                  | Per (feature_service × category) cell.                                           |
| `launch-mtds-prediction-backfill-vm.sh` | PREDICTION           | Polymarket-only MTDS download via gamma. Singleton-locked.                       |
| `launch-sfi-forward-poll.sh`            | SPORTS               | SoccerFootball.info reference data via instruments-service. Singleton-locked.    |
| `launch-footystats-forward-poll.sh`     | SPORTS               | FootyStats forward poll for 14 days.                                             |
| `launch-strategy-test-vm.sh`            | n/a                  | Strategy-service smoke.                                                          |

**Singleton locks**: `launch-sfi-forward-poll.sh` and `launch-mtds-prediction-backfill-vm.sh` refuse to launch a duplicate VM in the zone (RapidAPI / gamma rate-limit per shared API key). Pass `--force` as the first arg to bypass.

## Tarball Approach (this directory)

### Step 1: Create and upload tarballs

**Pick the invocation that matches the scope of your change.** The default
(no flags) only re-tars CORE — `unified-api-contracts`, `unified-trading-library`,
`market-tick-data-service`, `deployment-service`. If your change touches any
other service repo, you MUST use `--all` or a category flag, or VMs will run
stale code without any error.

```bash
# CORE only — UAC + UTL + MTDS + deployment-service
# Use this only when your changes are confined to those four repos.
bash scripts/vm/create-code-tarballs.sh

# ALL service repos (CORE + every consumer service) — safest default for
# any multi-repo feature rollout (e.g. Phase B of honest-coverage-metrics).
bash scripts/vm/create-code-tarballs.sh --all

# Category-scoped — CORE + every repo in the category's pipeline
# Categories: CEFI, TRADFI, DEFI, SPORTS, PREDICTION
bash scripts/vm/create-code-tarballs.sh --asset-group SPORTS
bash scripts/vm/create-code-tarballs.sh --asset-group PREDICTION
bash scripts/vm/create-code-tarballs.sh --asset-group DEFI

# Combine category with one-off additions
bash scripts/vm/create-code-tarballs.sh --asset-group SPORTS --include features-calendar-service

# Add specific repos on top of CORE without a category
bash scripts/vm/create-code-tarballs.sh --include instruments-service --include features-sports-service

# Dry run (show what would be created)
bash scripts/vm/create-code-tarballs.sh --all --dry-run

# Custom bucket
bash scripts/vm/create-code-tarballs.sh --all --bucket my-custom-bucket
```

GCS layout after upload (varies by flag):

```
gs://deployment-scripts-central-element-323112/
├── code/
│   ├── unified-api-contracts-code.tar.gz    (always — CORE)
│   ├── unified-trading-library-code.tar.gz  (always — CORE)
│   ├── mtds-code.tar.gz                     (always — CORE)
│   ├── deployment-service-code.tar.gz       (always — CORE)
│   ├── instruments-service-code.tar.gz      (--all / category / --include)
│   ├── market-data-processing-service-code.tar.gz  (--all / category / --include)
│   ├── features-sports-service-code.tar.gz  (--all / SPORTS / --include)
│   ├── features-calendar-service-code.tar.gz  (--all / CEFI / TRADFI / --include)
│   └── ...                                  (one per repo per --all / category)
└── vm/
    └── setup-data-pipeline-vm.sh            (the setup script)
```

**Category → repo mappings** are defined in `create-code-tarballs.sh` (the
`CEFI_REPOS`, `TRADFI_REPOS`, `DEFI_REPOS`, `SPORTS_REPOS`, `PREDICTION_REPOS`
arrays). Edit the script if you add a new service repo to a category.

### Step 2: Launch VM with startup-script

```bash
# Auto-setup: VM boots, fetches tarballs, installs deps, launches task
gcloud compute instances create my-backfill-vm \
  --zone=asia-northeast1-b \
  --machine-type=e2-standard-4 \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --scopes=cloud-platform \
  --metadata=startup-script-url=gs://deployment-scripts-central-element-323112/vm/setup-data-pipeline-vm.sh \
  --metadata=VM_TASK=cefi-backfill,VM_VENUE=DERIBIT,VM_START_DATE=2020-01-01,VM_END_DATE=2020-12-31,VM_CATEGORY=CEFI,VM_OPERATION=download
```

### Step 2 (alternative): Manual SSH setup

```bash
# Create VM without startup script
gcloud compute instances create my-vm --zone=asia-northeast1-b --machine-type=e2-standard-4 \
  --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud --scopes=cloud-platform

# Upload and run setup script
gcloud compute scp scripts/vm/setup-data-pipeline-vm.sh my-vm:/tmp/setup.sh --zone=asia-northeast1-b
gcloud compute ssh my-vm --zone=asia-northeast1-b --command="sudo bash /tmp/setup.sh"

# Launch task manually
gcloud compute ssh my-vm --zone=asia-northeast1-b --command="
  export GCP_PROJECT_ID=central-element-323112 CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false
  nohup /home/ikennaigboaka/venv/bin/python -m market_tick_data_service \
    --operation download --mode batch --asset-group CEFI \
    --venues DERIBIT --start-date 2020-01-01 --end-date 2020-12-31 \
    > /home/ikennaigboaka/logs/backfill.log 2>&1 &
"
```

### VM metadata parameters

| Parameter       | Required        | Example         | Description                       |
| --------------- | --------------- | --------------- | --------------------------------- |
| `VM_TASK`       | For auto-launch | `cefi-backfill` | Task type (used in logging)       |
| `VM_VENUE`      | For auto-launch | `DERIBIT`       | Venue filter                      |
| `VM_START_DATE` | For auto-launch | `2020-01-01`    | Start of date range               |
| `VM_END_DATE`   | For auto-launch | `2020-12-31`    | End of date range                 |
| `VM_CATEGORY`   | Optional        | `CEFI`          | Market category (default: CEFI)   |
| `VM_OPERATION`  | Optional        | `download`      | CLI operation (default: download) |

### What the setup script does

1. Installs Python 3.13 via deadsnakes PPA (UAC requires >=3.13)
2. Installs build-essential + python3.13-dev (C extensions: ckzg, lru-dict)
3. Creates venv at `/home/ikennaigboaka/venv` with Python 3.13
4. Downloads code tarballs from GCS → extracts to `/home/ikennaigboaka/workspace/{uac,utl,mtds}`
5. `pip install -e` all three packages
6. If `VM_TASK` + `VM_VENUE` metadata set → auto-launches the task via `nohup`
7. Writes `READY` to `/tmp/vm_ready` on completion

### Checking VM status

```bash
# Quick process check
gcloud compute ssh my-vm --zone=asia-northeast1-b --command="ps aux | grep python | grep -v grep"

# Log tail
gcloud compute ssh my-vm --zone=asia-northeast1-b --command="tail -20 /home/ikennaigboaka/logs/backfill.log"

# Setup log (if startup-script was used)
gcloud compute ssh my-vm --zone=asia-northeast1-b --command="tail -20 /var/log/vm-setup.log"
```

## Docker Approach (deployment-service backends)

The production VM backend lives at:

| File                                                    | Purpose                                                       |
| ------------------------------------------------------- | ------------------------------------------------------------- |
| `deployment_service/backends/vm.py`                     | VMBackend class — GCE VM with Docker (Container-Optimized OS) |
| `deployment_service/backends/services/vm_lifecycle.py`  | VM create/delete with multi-zone failover                     |
| `deployment_service/backends/services/vm_config.py`     | Config templates, image resolution, zone management           |
| `deployment_service/backends/services/vm_monitoring.py` | GCS status file polling for completion                        |
| `deployment_service/shard_calculator.py`                | Shard distribution across VMs (10K cap)                       |

This approach pulls Docker images from Artifact Registry at runtime. Faster startup,
deterministic deps, but requires building + pushing images first.

## Deployment-UI Integration Plan

The deployment-UI "VM Instance" tab should offer:

1. **Deploy mode toggle**: Docker image | Tarball | SSH
2. **For tarball mode**:
   - "Refresh tarballs" button → runs `create-code-tarballs.sh`
   - Shows last tarball upload timestamp from GCS
   - VM launch form with metadata fields (venue, dates, category, operation)
   - Calls `gcloud compute instances create` with startup-script-url
3. **For Docker mode**:
   - Image picker (from Artifact Registry)
   - Uses existing VMBackend via deployment-service API
4. **For all modes**:
   - VM status dashboard (polls `gcloud compute instances list`)
   - Log viewer (SSH + tail)
   - Stop/delete buttons

## Known Issues & Lessons Learned

- **Must use Python 3.13** — UAC requires >=3.13, Ubuntu 24.04 ships 3.12
- **Must use full venv path** — `nohup python` fails, must use `nohup /home/.../venv/bin/python`
- **build-essential required** — C extensions (ckzg, lru-dict for web3/UTL) fail without it
- **Tarballs must be refreshed** — After code changes, re-run `create-code-tarballs.sh`. **Pick the right scope flag**: bare invocation re-tars only CORE (UAC/UTL/MTDS/deployment-service); use `--all` for any multi-repo feature, or `--asset-group SPORTS|CEFI|TRADFI|DEFI|PREDICTION` to scope tighter. Forgetting the flag means stale code runs on VMs with no error signal.
- **Same region as GCS** — Use asia-northeast1 for fast I/O with data buckets
- **cloud-platform scope** — Required for GCS + Secret Manager access
