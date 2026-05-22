#!/usr/bin/env bash
# deploy-ecs-fargate.sh — Register ECS task definitions + create Fargate services + App Runner services
# for the 7 DeFi-live services (Phase 6 of aws_migration_defi_first plan).
#
# Fargate (stateful): execution-service, features-service, strategy-service,
#                     risk-and-exposure-service, position-balance-monitor-service
# App Runner (stateless HTTP): alerting-service, deployment-api
#
# Usage:
#   bash scripts/aws/deploy-ecs-fargate.sh [--dry-run] [--service <name>]
#
# Env:
#   AWS_DEFAULT_REGION (default: ap-northeast-1)
#   AWS_ACCOUNT_ID     (default: 427895769566)

set -euo pipefail

# /usr/local/bin/aws is broken (Homebrew bad interpreter). Use pip-installed v1 via python3.
aws() { python3 -m awscli "$@"; }

AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-427895769566}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
ECR_BASE="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com"
CLUSTER="uts-defi-prod"
VPC_SUBNETS="subnet-852f84cd,subnet-fc09eca6,subnet-5c16a477"
VPC_SG="sg-6ae6142b"  # default VPC SG — allows outbound all, inbound from VPC
DRY_RUN=false
TARGET_SERVICE=""

for arg in "$@"; do
  case $arg in
    --dry-run) DRY_RUN=true ;;
    --service) TARGET_SERVICE="${2:-}"; shift ;;
    --service=*) TARGET_SERVICE="${arg#*=}" ;;
  esac
done

log() { echo "[deploy-ecs-fargate] $*"; }
run() {
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] $*"
  else
    "$@"
  fi
}

# ── Task definition helpers ──────────────────────────────────────────────────

_role_arn() {
  local role_name="$1"
  echo "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${role_name}"
}

_secrets_json() {
  local -a entries=("$@")
  local out="["
  local first=true
  for entry in "${entries[@]}"; do
    local name="${entry%%=*}"
    local path="${entry#*=}"
    [[ "$first" == "true" ]] || out+=","
    out+="{\"name\":\"$name\",\"valueFrom\":\"arn:aws:secretsmanager:${AWS_DEFAULT_REGION}:${AWS_ACCOUNT_ID}:secret:${path}\"}"
    first=false
  done
  out+="]"
  echo "$out"
}

register_fargate_task() {
  local service="$1"
  local family="$2"
  local role_name="$3"
  local cpu="$4"
  local memory="$5"
  local image_tag="$6"
  local env_json="$7"
  local secrets_json="$8"
  # Optional: JSON array of CLI args passed as container command override.
  # Overrides the Dockerfile CMD (all 3 stateful services use CMD ["--help"]
  # which exits immediately without an explicit command override).
  local command_json="${9:-}"

  local role_arn
  role_arn=$(_role_arn "$role_name")
  local image="${ECR_BASE}/${service}:${image_tag}"
  local log_group="/ecs/${family}"

  # Build optional command field
  local command_field=""
  if [[ -n "$command_json" ]]; then
    command_field=",\"command\":${command_json}"
  fi

  log "Registering task definition: $family (image: $image)"
  run aws logs create-log-group --log-group-name "$log_group" --region "$AWS_DEFAULT_REGION" 2>/dev/null || true
  run aws ecs register-task-definition \
    --family "$family" \
    --network-mode awsvpc \
    --requires-compatibilities FARGATE \
    --cpu "$cpu" \
    --memory "$memory" \
    --execution-role-arn "$role_arn" \
    --task-role-arn "$role_arn" \
    --region "$AWS_DEFAULT_REGION" \
    --container-definitions "[{
      \"name\":\"$service\",
      \"image\":\"$image\",
      \"portMappings\":[{\"containerPort\":8080,\"protocol\":\"tcp\"}],
      \"environment\":$env_json,
      \"secrets\":$secrets_json${command_field},
      \"logConfiguration\":{
        \"logDriver\":\"awslogs\",
        \"options\":{
          \"awslogs-group\":\"$log_group\",
          \"awslogs-region\":\"$AWS_DEFAULT_REGION\",
          \"awslogs-stream-prefix\":\"ecs\"
        }
      },
      \"healthCheck\":{
        \"command\":[\"CMD-SHELL\",\"curl -sf http://localhost:8080/health || exit 1\"],
        \"interval\":30,
        \"timeout\":10,
        \"retries\":3,
        \"startPeriod\":60
      }
    }]" \
    --query 'taskDefinition.taskDefinitionArn' --output text
}

create_or_update_ecs_service() {
  local service="$1"
  local family="$2"
  local desired_count="${3:-1}"

  local svc_name="uts-${service}-prod"
  log "Creating/updating ECS service: $svc_name in cluster $CLUSTER"
  # Check if service exists
  local existing
  existing=$(aws ecs describe-services --cluster "$CLUSTER" --services "$svc_name" \
    --region "$AWS_DEFAULT_REGION" --query 'services[0].status' --output text 2>/dev/null || echo "MISSING")
  if [[ "$existing" == "ACTIVE" ]]; then
    log "Service $svc_name exists (ACTIVE) — updating task definition"
    run aws ecs update-service \
      --cluster "$CLUSTER" \
      --service "$svc_name" \
      --task-definition "$family" \
      --desired-count "$desired_count" \
      --region "$AWS_DEFAULT_REGION" \
      --query 'service.serviceArn' --output text
  else
    log "Service $svc_name missing/inactive — creating"
    run aws ecs create-service \
      --cluster "$CLUSTER" \
      --service-name "$svc_name" \
      --task-definition "$family" \
      --desired-count "$desired_count" \
      --launch-type FARGATE \
      --network-configuration "awsvpcConfiguration={subnets=[$VPC_SUBNETS],securityGroups=[$VPC_SG],assignPublicIp=ENABLED}" \
      --region "$AWS_DEFAULT_REGION" \
      --query 'service.serviceArn' --output text
  fi
}

create_or_update_apprunner_service() {
  local service="$1"
  local svc_name="$2"
  local role_name="$3"
  local image_tag="$4"
  local port="$5"
  local cpu="$6"
  local memory="$7"
  local env_json="$8"

  local role_arn
  role_arn=$(_role_arn "$role_name")
  local image="${ECR_BASE}/${service}:${image_tag}"
  log "Creating/updating App Runner service: $svc_name (image: $image)"

  # Check if service exists
  local existing_arn
  existing_arn=$(aws apprunner list-services --region "$AWS_DEFAULT_REGION" \
    --query "ServiceSummaryList[?ServiceName=='$svc_name'].ServiceArn | [0]" \
    --output text 2>/dev/null || echo "None")

  if [[ "$existing_arn" != "None" && -n "$existing_arn" ]]; then
    log "App Runner service $svc_name exists — updating"
    run aws apprunner update-service \
      --service-arn "$existing_arn" \
      --region "$AWS_DEFAULT_REGION" \
      --source-configuration "{
        \"AuthenticationConfiguration\":{\"AccessRoleArn\":\"$role_arn\"},
        \"ImageRepository\":{
          \"ImageIdentifier\":\"$image\",
          \"ImageConfiguration\":{\"Port\":\"$port\",\"RuntimeEnvironmentVariables\":$env_json},
          \"ImageRepositoryType\":\"ECR\"
        }
      }" \
      --query 'Service.ServiceArn' --output text
  else
    log "App Runner service $svc_name missing — creating"
    run aws apprunner create-service \
      --service-name "$svc_name" \
      --region "$AWS_DEFAULT_REGION" \
      --source-configuration "{
        \"AuthenticationConfiguration\":{\"AccessRoleArn\":\"$role_arn\"},
        \"ImageRepository\":{
          \"ImageIdentifier\":\"$image\",
          \"ImageConfiguration\":{\"Port\":\"$port\",\"RuntimeEnvironmentVariables\":$env_json},
          \"ImageRepositoryType\":\"ECR\"
        }
      }" \
      --instance-configuration "{\"Cpu\":\"$cpu\",\"Memory\":\"$memory\",\"InstanceRoleArn\":\"$role_arn\"}" \
      --health-check-configuration "{\"Protocol\":\"HTTP\",\"Path\":\"/health\",\"Interval\":10,\"Timeout\":5,\"HealthyThreshold\":1,\"UnhealthyThreshold\":3}" \
      --query 'Service.ServiceArn' --output text
  fi
}

smoke_health() {
  local url="$1"
  local label="$2"
  log "Smoke /health: $label ($url)"
  local http_code
  http_code=$(curl -sf --max-time 10 -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo "000")
  if [[ "$http_code" == "200" ]]; then
    log "  ✅ $label /health = 200"
    return 0
  else
    log "  ❌ $label /health = $http_code (service may still be starting)"
    return 1
  fi
}

# ── Common env vars ──────────────────────────────────────────────────────────
# ECS format: [{"name":"K","value":"V"},...]
# App Runner format: {"K":"V",...}

_ecs_base_env() {
  # Base env vars as ECS JSON array
  local extras="${1:-}"
  echo "[{\"name\":\"DEPLOYMENT_ENV\",\"value\":\"prod\"},{\"name\":\"CLOUD_PROVIDER\",\"value\":\"aws\"},{\"name\":\"AWS_REGION\",\"value\":\"${AWS_DEFAULT_REGION}\"},{\"name\":\"AWS_ACCOUNT_ID\",\"value\":\"${AWS_ACCOUNT_ID}\"}${extras}]"
}

_apprunner_base_env() {
  # Base env vars as App Runner map format
  local extras="${1:-}"
  echo "{\"DEPLOYMENT_ENV\":\"prod\",\"CLOUD_PROVIDER\":\"aws\",\"AWS_REGION\":\"${AWS_DEFAULT_REGION}\",\"AWS_ACCOUNT_ID\":\"${AWS_ACCOUNT_ID}\"${extras}}"
}

# ── Deploy: execution-service (Fargate) ─────────────────────────────────────

deploy_execution_service() {
  [[ -n "$TARGET_SERVICE" && "$TARGET_SERVICE" != "execution-service" ]] && return 0
  local env_json
  env_json=$(_ecs_base_env ",{\"name\":\"CLIENT_ISOLATION_MODE\",\"value\":\"subprocess\"}")
  local secrets_json
  secrets_json=$(_secrets_json \
    "EXEC_ODUM_BINANCE_CEFI=unified-trading/exec-odum-binance-cefi" \
    "EXEC_ODUM_DERIBIT_CEFI=unified-trading/exec-odum-deribit-cefi" \
    "EXEC_ODUM_OKX_CEFI=unified-trading/exec-odum-okx-cefi" \
    "EXEC_ODUM_BYBIT_CEFI=unified-trading/exec-odum-bybit-cefi" \
    "EXEC_ODUM_HYPERLIQUID_DEFI=unified-trading/exec-odum-hyperliquid-defi" \
    "KMS_KEY_ARN=unified-trading/defi-kms-key-arn")
  register_fargate_task "execution-service" "uts-execution-service-prod" \
    "uts-execution-service-prod" "2048" "4096" "latest" "$env_json" "$secrets_json"
  create_or_update_ecs_service "execution-service" "uts-execution-service-prod"
}

# ── Deploy: features-service (Fargate) ──────────────────────────────────────

deploy_features_service() {
  [[ -n "$TARGET_SERVICE" && "$TARGET_SERVICE" != "features-service" ]] && return 0
  local env_json
  env_json=$(_ecs_base_env ",{\"name\":\"FEATURES_ASSET_GROUPS\",\"value\":\"defi\"},{\"name\":\"FEATURES_FAMILIES\",\"value\":\"onchain\"}")
  local secrets_json
  secrets_json=$(_secrets_json \
    "ALCHEMY_API_KEY=unified-trading/alchemy-api-key" \
    "THEGRAPH_API_KEY=unified-trading/thegraph-api-key")
  # CLI requires --feature-family; Dockerfile CMD ["--help"] exits immediately without this override.
  local command_json='["--feature-family","onchain","--operation","compute","--mode","live","--asset-group","defi","--feature-group","ALL"]'
  register_fargate_task "features-service" "uts-features-service-prod" \
    "uts-features-service-prod" "2048" "4096" "latest" "$env_json" "$secrets_json" "$command_json"
  create_or_update_ecs_service "features-service" "uts-features-service-prod"
}

# ── Deploy: strategy-service (Fargate) ──────────────────────────────────────

deploy_strategy_service() {
  [[ -n "$TARGET_SERVICE" && "$TARGET_SERVICE" != "strategy-service" ]] && return 0
  local env_json
  env_json=$(_ecs_base_env ",{\"name\":\"ACTIVE_ARCHETYPES\",\"value\":\"carry_staked_basis,arbitrage_price_dispersion\"}")
  # CLI requires --operation; Dockerfile CMD ["--help"] exits immediately without this override.
  local command_json='["--operation","trade","--mode","live","--asset-group","defi"]'
  register_fargate_task "strategy-service" "uts-strategy-service-prod" \
    "uts-strategy-service-prod" "2048" "4096" "latest" "$env_json" "[]" "$command_json"
  create_or_update_ecs_service "strategy-service" "uts-strategy-service-prod"
}

# ── Deploy: risk-and-exposure-service (Fargate) ─────────────────────────────

deploy_risk_service() {
  [[ -n "$TARGET_SERVICE" && "$TARGET_SERVICE" != "risk-and-exposure-service" ]] && return 0
  # Check ECR image exists first
  local latest_tag
  latest_tag=$(aws ecr describe-images --repository-name risk-and-exposure-service \
    --region "$AWS_DEFAULT_REGION" \
    --query 'sort_by(imageDetails,&imagePushedAt)[-1].imageTags[0]' \
    --output text 2>/dev/null || echo "None")
  if [[ "$latest_tag" == "None" || -z "$latest_tag" ]]; then
    log "  ⚠️  risk-and-exposure-service: ECR image not yet available — skipping (build in progress)"
    return 0
  fi
  local env_json
  env_json=$(_ecs_base_env)
  register_fargate_task "risk-and-exposure-service" "uts-risk-and-exposure-service-prod" \
    "uts-risk-and-exposure-service-prod" "1024" "2048" "$latest_tag" "$env_json" "[]"
  create_or_update_ecs_service "risk-and-exposure-service" "uts-risk-and-exposure-service-prod"
}

# ── Deploy: position-balance-monitor-service (Fargate or App Runner) ─────────

deploy_pbm_service() {
  [[ -n "$TARGET_SERVICE" && "$TARGET_SERVICE" != "position-balance-monitor-service" ]] && return 0
  local latest_tag
  latest_tag=$(aws ecr describe-images --repository-name position-balance-monitor-service \
    --region "$AWS_DEFAULT_REGION" \
    --query 'sort_by(imageDetails,&imagePushedAt)[-1].imageTags[0]' \
    --output text 2>/dev/null || echo "None")
  if [[ "$latest_tag" == "None" || -z "$latest_tag" ]]; then
    log "  ⚠️  position-balance-monitor-service: ECR image not yet available — skipping (build in progress)"
    return 0
  fi
  local env_json
  env_json=$(_ecs_base_env)
  register_fargate_task "position-balance-monitor-service" "uts-position-balance-monitor-service-prod" \
    "uts-position-balance-monitor-service-prod" "1024" "2048" "$latest_tag" "$env_json" "[]"
  create_or_update_ecs_service "position-balance-monitor-service" "uts-position-balance-monitor-service-prod"
}

# ── Deploy: alerting-service (App Runner) ────────────────────────────────────

deploy_alerting_service() {
  [[ -n "$TARGET_SERVICE" && "$TARGET_SERVICE" != "alerting-service" ]] && return 0
  local env_json
  env_json=$(_apprunner_base_env ",\"SECRETS_SOURCE\":\"aws_sm\"")
  create_or_update_apprunner_service "alerting-service" "uts-alerting-service-prod" \
    "uts-alerting-service-prod" "0.1.0" "8080" "1 vCPU" "2 GB" "$env_json"
}

# ── Deploy: deployment-api (App Runner) ──────────────────────────────────────

deploy_deployment_api() {
  [[ -n "$TARGET_SERVICE" && "$TARGET_SERVICE" != "deployment-api" ]] && return 0
  # deployment-api keeps CLOUD_PROVIDER=gcp at launch (reads GCP manifests; AWS toggle in UI)
  local env_json
  env_json="{\"DEPLOYMENT_ENV\":\"prod\",\"CLOUD_PROVIDER\":\"gcp\",\"AWS_REGION\":\"${AWS_DEFAULT_REGION}\",\"AWS_ACCOUNT_ID\":\"${AWS_ACCOUNT_ID}\"}"
  create_or_update_apprunner_service "deployment-api" "uts-deployment-api-prod" \
    "uts-deployment-api-prod" "0.1.1" "8004" "1 vCPU" "2 GB" "$env_json"
}

# ── Main ─────────────────────────────────────────────────────────────────────

log "=== ECS Fargate + App Runner deployment ==="
log "Cluster: $CLUSTER | Region: $AWS_DEFAULT_REGION | Account: $AWS_ACCOUNT_ID"
log "DRY_RUN: $DRY_RUN | TARGET: ${TARGET_SERVICE:-all}"
log ""

deploy_execution_service
deploy_features_service
deploy_strategy_service
deploy_risk_service
deploy_pbm_service
deploy_alerting_service
deploy_deployment_api

log ""
log "=== Deployment complete (or dry-run) ==="
log "Wait 3-5 min for Fargate tasks to start, then re-run with --service <name> to smoke /health"
log "App Runner services take ~2 min to provision."
log ""
log "Check cluster status: aws ecs list-tasks --cluster $CLUSTER --region $AWS_DEFAULT_REGION"
log "Check App Runner: aws apprunner list-services --region $AWS_DEFAULT_REGION"
