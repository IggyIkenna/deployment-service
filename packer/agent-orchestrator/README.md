# agent-orchestrator — Packer AMI build (Phase 9)

Pre-bakes an Ubuntu 24.04 AMI with all `bootstrap_vm.sh` Steps 1-2 deps + a warm
clone of `agent-orchestrator` / `unified-trading-library` / `unified-api-contracts`
/ `unified-trading-pm` + a pre-built `.venv`, so first boot of a new fleet VM
completes in **<5 minutes** instead of the cold-boot ~5-15 min.

## What's baked in

| Layer   | Item                                                                                                                                    | Skipped at first boot         |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| OS deps | `git`, `tmux`, `python3`, `python3-pip`, `python3-yaml`, `curl`, `jq`, `unzip`                                                          | bootstrap STEP 1              |
| Node.js | Node.js 20 (NodeSource)                                                                                                                 | bootstrap STEP 1              |
| CLIs    | Claude Code CLI (`@anthropic-ai/claude-code` global), AWS CLI v2                                                                        | bootstrap STEP 2              |
| Python  | `uv` (in `/usr/local/bin/`)                                                                                                             | bootstrap STEP 4.5            |
| Repos   | `agent-orchestrator`, `unified-trading-library`, `unified-api-contracts`, `unified-trading-pm` warm-cached at `/opt/orchestrator-warm/` | bootstrap STEP 3 rsync seed   |
| venv    | `/opt/orchestrator-warm/agent-orchestrator/.venv` (deps installed)                                                                      | bootstrap STEP 4.5 rsync seed |
| Marker  | `/etc/orchestrator-ami-version` containing the build timestamp                                                                          | bootstrap detection           |

## What still happens at first boot

The cold-only items that depend on the specific VM identity remain in
`bootstrap_vm.sh`:

1. Resolve `VM_NAME` / `VM_ROLE` / `PUBLIC_URL` from instance metadata
2. Fetch per-VM `.env.local` from `ORCHESTRATOR_ENV_LOCAL` secret
3. Fetch `accounts.json` + `backlog.yaml` + per-account `.env` files from the
   creds bucket via `CredsEnvPoller`'s bootstrap step
4. `git pull --ff-only` from `live-defi-rollout` on each repo (catches up
   from the baked snapshot)
5. Re-run `uv pip install -e` (no-op when warm-cache is current)
6. systemd install + enable + start
7. Emit `STARTED` event

These steps total <60s on a well-provisioned m7i.xlarge.

## Build

Prereqs on the build host:

- `packer` ≥ 1.10 ([install](https://developer.hashicorp.com/packer/install))
- AWS credentials for an account with EC2 + EBS + AMI permissions in
  `ap-northeast-1` (matches the fleet region)

Run:

```bash
cd deployment-service/packer/agent-orchestrator

# Initialise the amazon plugin (first run only)
packer init .

# Validate
packer validate .

# Build (~10-15 min: spins a build instance, runs install-deps + warm-cache, snapshots, terminates)
packer build .
# … or with explicit branch:
packer build -var "git_branch=live-defi-rollout" .
```

Output: an AMI named `agent-orchestrator-YYYYMMDD-HHMMSS` tagged
`Project=agent-orchestrator`, `Branch=live-defi-rollout`, `BuildDate=<iso>`.

## Use the AMI

Pass the AMI id to `launch-epic-vm-aws.sh` via env var:

```bash
export AMI_ID=ami-0123456789abcdef
bash deployment-service/scripts/vm/launch-epic-vm-aws.sh --vm-id vm-defi
# Or all 10:
bash deployment-service/scripts/vm/launch-epic-vm-aws.sh --all
```

The launcher prints which AMI it's using:
`[lc_aws_ec2_run] Using operator-supplied AMI: ami-...`. When `AMI_ID` is
unset, it falls back to the latest Canonical Ubuntu 24.04 via SSM (and you
get the cold-boot 5-15 min path).

Inside the VM, verify the prebaked path triggered:

```bash
cat /etc/orchestrator-ami-version    # e.g. 20260528-150300
journalctl -u orchestrator -n 50 | grep PREBAKED   # confirms bootstrap saw the marker
```

## Cadence + cost

The AMI is timestamp-named, never overwritten. Rebuild whenever:

- A meaningful new dep lands in `agent-orchestrator`, UTL, or UAC (cuts cold
  install at runtime)
- Node / claude CLI / Ubuntu base AMI gets a security update
- A new repo gets added to the warm cache

Cost: one m7i.large build run (~$0.20 + EBS snapshot ~few cents/month). AMIs
linger free; their EBS snapshots are ~$0.05/GB/mo (~$2/mo each at ~30GB).
Deregister old AMIs + delete their snapshots quarterly via
`scripts/aws/cleanup-old-orchestrator-amis.sh` (TBD).

## Files

```
packer/agent-orchestrator/
├── agent-orchestrator-ami.pkr.hcl   # HCL2 build definition
├── scripts/
│   ├── install-deps.sh              # System deps + Node.js + claude CLI + AWS CLI + uv
│   └── warm-cache.sh                # Pre-clone repos + build .venv
└── README.md                        # this file
```

## Related

- `agent-orchestrator/scripts/bootstrap_vm.sh` — the first-boot script that
  detects the marker and short-circuits installed steps
- `deployment-service/scripts/vm/launch-epic-vm-aws.sh` — launcher that takes
  the optional `AMI_ID` env var
- `deployment-service/scripts/vm/lib/aws_ec2_launch_lib.sh::lc_aws_ec2_run` —
  the EC2 RunInstances wrapper that resolves the AMI
- `codex/05-infrastructure/agent-orchestrator-worker-topology.md` — fleet
  topology + bootstrap flow overview
- `plans/epics/orchestrator_master.md` Phase 9 — epic context
