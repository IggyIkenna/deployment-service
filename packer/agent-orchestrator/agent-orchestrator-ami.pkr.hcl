// Packer template — agent-orchestrator immutable AMI build (Phase 9)
//
// Pre-bakes everything bootstrap_vm.sh installs on a cold Ubuntu so first-boot
// time drops from ~5-15 min (cold) to <5 min (image already has deps + warm
// repos + venv). bootstrap_vm.sh detects the marker /etc/orchestrator-ami-version
// and skips Steps 1-3 + part of Step 6 when present.
//
// What this image bakes in:
//   - System deps (git, tmux, python3, python3-pip, curl, jq, unzip)
//   - Node.js 20 (NodeSource)
//   - Claude Code CLI (@anthropic-ai/claude-code, global)
//   - AWS CLI v2
//   - uv (Python package manager)
//   - Pre-cloned agent-orchestrator + unified-trading-library + unified-api-contracts
//     at /opt/orchestrator-warm/ on live-defi-rollout (used as cache; bootstrap
//     rsyncs into /home/<operator>/unified-trading-system-repos/ then `git pull`)
//   - Pre-built Python venv at /opt/orchestrator-warm/agent-orchestrator/.venv
//
// What bootstrap_vm.sh still does on first boot:
//   - Fetch per-VM secrets from Secrets Manager (ORCHESTRATOR_ENV_LOCAL,
//     accounts.json, backlog.yaml from creds bucket)
//   - Set VM_ID / VM_ROLE / PUBLIC_URL env vars from instance metadata
//   - Sync warm repos to operator home + `git pull` to LDR-current
//   - Start systemd unit, emit STARTED event
//
// Build:
//   cd deployment-service/packer/agent-orchestrator
//   packer init .
//   packer build -var "git_branch=live-defi-rollout" .
//
// Result: AMI named `agent-orchestrator-<UTC-timestamp>` tagged
//   Project=agent-orchestrator, Branch=live-defi-rollout, BuildDate=<iso>
// Capture the AMI ID from packer output; pass to launch-epic-vm-aws.sh via
// AMI_ID env var or --ami-id flag.

packer {
  required_plugins {
    amazon = {
      version = ">= 1.3.0"
      source  = "github.com/hashicorp/amazon"
    }
  }
}

variable "aws_region" {
  type        = string
  default     = "ap-northeast-1"
  description = "AWS region to build the AMI in (matches fleet region)."
}

variable "instance_type" {
  type        = string
  default     = "m7i.large"
  description = "Build instance type. Smaller than fleet m7i.xlarge; AMI runs anywhere."
}

variable "git_branch" {
  type        = string
  default     = "live-defi-rollout"
  description = "Branch to warm-clone into the AMI. Should match the fleet's runtime branch."
}

variable "ami_name_prefix" {
  type        = string
  default     = "agent-orchestrator"
  description = "Prefix for the resulting AMI name."
}

variable "ssh_username" {
  type    = string
  default = "ubuntu"
}

variable "gh_pat" {
  type        = string
  sensitive   = true
  description = "GitHub PAT (workflow-capable, from Secret Manager via load-gh-token.sh) used to warm-clone the PRIVATE repos. Injected as a git insteadOf credential before warm-cache and SCRUBBED in the cleanup step so it is never baked into the AMI."
}

locals {
  build_timestamp = formatdate("YYYYMMDD-hhmmss", timestamp())
  ami_name        = "${var.ami_name_prefix}-${local.build_timestamp}"
}

// Source AMI: Canonical Ubuntu 24.04 LTS amd64 (latest, fetched at build time)
data "amazon-ami" "ubuntu_24_04" {
  filters = {
    virtualization-type = "hvm"
    name                = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"
    root-device-type    = "ebs"
  }
  owners      = ["099720109477"] // Canonical
  most_recent = true
  region      = var.aws_region
}

source "amazon-ebs" "agent_orchestrator" {
  region        = var.aws_region
  instance_type = var.instance_type
  source_ami    = data.amazon-ami.ubuntu_24_04.id
  ssh_username  = var.ssh_username

  ami_name        = local.ami_name
  ami_description = "agent-orchestrator pre-baked AMI; bootstrap_vm.sh first-boot completes in <5 min"

  // Tagging — propagated to instances launched from this AMI
  tags = {
    Project   = "agent-orchestrator"
    Branch    = var.git_branch
    BuildDate = local.build_timestamp
    Tool      = "packer"
  }
  run_tags = {
    Name      = "packer-build-${local.build_timestamp}"
    Purpose   = "ami-bake"
    Ephemeral = "true"
  }

  // Keep build EBS snapshot reasonably sized
  launch_block_device_mappings {
    device_name           = "/dev/sda1"
    volume_size           = 30
    volume_type           = "gp3"
    delete_on_termination = true
  }
}

build {
  name    = "agent-orchestrator"
  sources = ["source.amazon-ebs.agent_orchestrator"]

  // Step 1: system deps + Node.js 20 + Claude CLI + AWS CLI + uv
  provisioner "shell" {
    script          = "scripts/install-deps.sh"
    execute_command = "echo '${var.ssh_username}' | sudo -S -E bash '{{ .Path }}'"
    environment_vars = [
      "DEBIAN_FRONTEND=noninteractive",
    ]
  }

  // Step 1.5: inject the PAT as a git insteadOf credential SYSTEM-wide
  // (/etc/gitconfig). warm-cache.sh clones the PRIVATE repos via `sudo -S -E bash`,
  // so git runs as root but with HOME=/home/ubuntu preserved (-E) — it would read
  // /home/ubuntu/.gitconfig, NOT /root/.gitconfig. --system is read regardless of
  // HOME. Scrubbed in Step 4 cleanup so the token is never baked into the AMI.
  provisioner "shell" {
    inline = [
      "sudo git config --system url.\"https://x-access-token:${var.gh_pat}@github.com/\".insteadOf \"https://github.com/\"",
    ]
  }

  // Step 2: pre-clone repos + build warm venv
  provisioner "shell" {
    script          = "scripts/warm-cache.sh"
    execute_command = "echo '${var.ssh_username}' | sudo -S -E bash '{{ .Path }}'"
    environment_vars = [
      "GIT_BRANCH=${var.git_branch}",
    ]
  }

  // Step 3: stamp the AMI with marker so bootstrap_vm.sh can skip baked steps
  provisioner "shell" {
    inline = [
      "echo '${local.build_timestamp}' | sudo tee /etc/orchestrator-ami-version",
      "sudo chmod 644 /etc/orchestrator-ami-version",
    ]
  }

  // Step 4: cleanup transient caches before snapshot
  provisioner "shell" {
    inline = [
      // SCRUB the warm-clone PAT — never bake a token into the AMI.
      "sudo git config --system --remove-section url.\"https://x-access-token:${var.gh_pat}@github.com/\" || true",
      "sudo rm -f /root/.gitconfig /home/${var.ssh_username}/.gitconfig /root/.git-credentials || true",
      "sudo apt-get clean",
      "sudo rm -rf /var/lib/apt/lists/*",
      "sudo rm -rf /tmp/* /var/tmp/*",
      "sudo rm -f /root/.bash_history /home/${var.ssh_username}/.bash_history",
      "sudo cloud-init clean --logs || true",
    ]
  }
}
