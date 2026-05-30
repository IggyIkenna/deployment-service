# api_host_auto_reboot.tf — Auto-reboot watchdog for central API host
#
# Triggers a Lambda when the CloudWatch alarm "api-host-impaired" enters ALARM
# state (StatusCheckFailed_Instance == 1 for 3/3 consecutive 5-min datapoints).
#
# Ceiling: ≤3 auto-reboots per 24-hour sliding window (SSM-tracked).
# On the 4th trigger the EventBridge rule is self-disabled and an operator
# alert fires — a permanently-broken host cannot infinite-loop on reboots.
#
# Alerts: Slack + Telegram (webhook / bot-token read from SSM at runtime).
# Operator must create these SSM parameters before enabling:
#   /uts/alerts/slack-webhook-url      (SecureString)
#   /uts/alerts/telegram-bot-token     (SecureString)
#   /uts/alerts/telegram-chat-id       (String)
#
# Plan: unified-trading-pm/plans/active/api_host_chronic_impairment_2026_05_29.md § Phase 4
# Instance: i-0c9b283b31d6b5ca7 (central orchestrator API host, m8i.4xlarge)

terraform {
  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

locals {
  api_host_instance_id   = "i-0c9b283b31d6b5ca7"
  api_host_alarm_name    = "api-host-impaired"
  auto_reboot_rule_name  = "uts-api-host-auto-reboot-${var.environment}"
  auto_reboot_lambda_name = "uts-api-host-auto-reboot-${var.environment}"
}

# ---------------------------------------------------------------------------
# Lambda — inline Python, packaged via archive_file
# ---------------------------------------------------------------------------

data "archive_file" "api_host_auto_reboot" {
  type        = "zip"
  output_path = "${path.module}/api_host_auto_reboot.zip"
  source {
    filename = "handler.py"
    content  = <<-PYTHON
      """
      Auto-reboot handler for central API host impairment watchdog.

      Flow:
        1. EventBridge fires when CloudWatch alarm enters ALARM state.
        2. SSM tracks reboot_count + window_start (24-hour sliding window).
        3. count < ceiling  → reboot + increment + Slack/Telegram alert.
        4. count >= ceiling → disable EventBridge rule + critical alert (no reboot).
      """
      from __future__ import annotations

      import json
      import os
      import urllib.request
      from datetime import datetime, timezone

      import boto3

      INSTANCE_ID        = os.environ["INSTANCE_ID"]
      REBOOT_CEILING     = int(os.environ.get("REBOOT_CEILING", "3"))
      REBOOT_WINDOW_HRS  = int(os.environ.get("REBOOT_WINDOW_HOURS", "24"))
      SSM_PARAM_BASE     = os.environ.get("SSM_PARAM_BASE", "/uts/api-host/reboot-ceiling")
      SLACK_SSM          = os.environ.get("SLACK_WEBHOOK_SSM", "")
      TG_TOKEN_SSM       = os.environ.get("TELEGRAM_BOT_TOKEN_SSM", "")
      TG_CHAT_SSM        = os.environ.get("TELEGRAM_CHAT_ID_SSM", "")
      EVENT_RULE_NAME    = os.environ["EVENT_RULE_NAME"]
      AWS_REGION         = os.environ.get("AWS_REGION_NAME", "ap-northeast-1")

      SSM_COUNT  = f"{SSM_PARAM_BASE}/count"
      SSM_WINDOW = f"{SSM_PARAM_BASE}/window-start"


      def _ssm_get(ssm, name: str, default: str = "") -> str:
          try:
              return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
          except Exception:
              return default


      def _ssm_put(ssm, name: str, value: str) -> None:
          ssm.put_parameter(Name=name, Value=value, Type="String", Overwrite=True)


      def _http_post(url: str, payload: dict) -> None:
          data = json.dumps(payload).encode("utf-8")
          req = urllib.request.Request(
              url, data=data, headers={"Content-Type": "application/json"}
          )
          urllib.request.urlopen(req, timeout=10)


      def _alert(ssm, message: str) -> None:
          print(f"[alert] {message}")
          if SLACK_SSM:
              webhook = _ssm_get(ssm, SLACK_SSM)
              if webhook:
                  try:
                      _http_post(webhook, {"text": message})
                  except Exception as exc:
                      print(f"[alert] Slack failed: {exc}")
          if TG_TOKEN_SSM and TG_CHAT_SSM:
              token = _ssm_get(ssm, TG_TOKEN_SSM)
              chat_id = _ssm_get(ssm, TG_CHAT_SSM)
              if token and chat_id:
                  try:
                      _http_post(
                          f"https://api.telegram.org/bot{token}/sendMessage",
                          {"chat_id": chat_id, "text": message},
                      )
                  except Exception as exc:
                      print(f"[alert] Telegram failed: {exc}")


      def lambda_handler(event: dict, context: object) -> dict:
          now = datetime.now(timezone.utc)
          now_iso = now.isoformat()

          ssm    = boto3.client("ssm", region_name=AWS_REGION)
          ec2    = boto3.client("ec2", region_name=AWS_REGION)
          events = boto3.client("events", region_name=AWS_REGION)

          count_str  = _ssm_get(ssm, SSM_COUNT, "0")
          window_str = _ssm_get(ssm, SSM_WINDOW, now_iso)

          count        = int(count_str)
          window_start = datetime.fromisoformat(window_str)

          elapsed_hours = (now - window_start).total_seconds() / 3600
          if elapsed_hours > REBOOT_WINDOW_HRS:
              count = 0
              _ssm_put(ssm, SSM_WINDOW, now_iso)
              _ssm_put(ssm, SSM_COUNT, "0")
              print(f"[ceiling] Window reset after {elapsed_hours:.1f}h")

          print(f"[ceiling] reboot_count={count}/{REBOOT_CEILING} window_start={window_str}")

          if count >= REBOOT_CEILING:
              try:
                  events.disable_rule(Name=EVENT_RULE_NAME)
                  print(f"[ceiling] EventBridge rule {EVENT_RULE_NAME!r} DISABLED")
              except Exception as exc:
                  print(f"[ceiling] Failed to disable rule: {exc}")

              _alert(
                  ssm,
                  f"🚨 Auto-reboot CEILING HIT for {INSTANCE_ID} — "
                  f"{REBOOT_CEILING} reboots in {REBOOT_WINDOW_HRS}h window. "
                  f"Auto-reboot DISABLED. MANUAL INTERVENTION REQUIRED. "
                  f"Re-enable: aws events enable-rule --name {EVENT_RULE_NAME}",
              )
              return {"status": "ceiling_hit", "reboot_count": count}

          ec2.reboot_instances(InstanceIds=[INSTANCE_ID])
          new_count = count + 1
          _ssm_put(ssm, SSM_COUNT, str(new_count))
          if count == 0:
              _ssm_put(ssm, SSM_WINDOW, now_iso)

          _alert(
              ssm,
              f"🔄 Auto-rebooted {INSTANCE_ID} at {now_iso} — impaired for ≥15 min "
              f"(StatusCheckFailed_Instance 3/3 datapoints). "
              f"Reboot {new_count}/{REBOOT_CEILING} in current {REBOOT_WINDOW_HRS}h window.",
          )
          print(f"[reboot] {INSTANCE_ID} rebooted. new_count={new_count}")
          return {"status": "rebooted", "reboot_count": new_count, "timestamp": now_iso}
    PYTHON
  }
}

# ---------------------------------------------------------------------------
# IAM role + policy for Lambda
# ---------------------------------------------------------------------------

resource "aws_iam_role" "api_host_auto_reboot" {
  name = "uts-api-host-auto-reboot-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "api_host_auto_reboot" {
  name = "reboot-ceiling-alerts"
  role = aws_iam_role.api_host_auto_reboot.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RebootInstance"
        Effect = "Allow"
        Action = ["ec2:RebootInstances"]
        Resource = [
          "arn:aws:ec2:${var.aws_region}:${var.aws_account_id}:instance/${local.api_host_instance_id}",
        ]
      },
      {
        Sid    = "RebootCeilingSsm"
        Effect = "Allow"
        Action = ["ssm:GetParameter", "ssm:PutParameter"]
        Resource = [
          "arn:aws:ssm:${var.aws_region}:${var.aws_account_id}:parameter/uts/api-host/reboot-ceiling/*",
        ]
      },
      {
        Sid    = "AlertsSsm"
        Effect = "Allow"
        Action = ["ssm:GetParameter"]
        Resource = [
          "arn:aws:ssm:${var.aws_region}:${var.aws_account_id}:parameter/uts/alerts/*",
        ]
      },
      {
        Sid    = "DisableEventBridgeRule"
        Effect = "Allow"
        Action = ["events:DisableRule"]
        Resource = [
          "arn:aws:events:${var.aws_region}:${var.aws_account_id}:rule/${local.auto_reboot_rule_name}",
        ]
      },
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = [
          "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/lambda/${local.auto_reboot_lambda_name}:*",
        ]
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# Lambda function
# ---------------------------------------------------------------------------

resource "aws_lambda_function" "api_host_auto_reboot" {
  function_name    = local.auto_reboot_lambda_name
  role             = aws_iam_role.api_host_auto_reboot.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  timeout          = 30
  filename         = data.archive_file.api_host_auto_reboot.output_path
  source_code_hash = data.archive_file.api_host_auto_reboot.output_base64sha256

  environment {
    variables = {
      INSTANCE_ID            = local.api_host_instance_id
      REBOOT_CEILING         = "3"
      REBOOT_WINDOW_HOURS    = "24"
      SSM_PARAM_BASE         = "/uts/api-host/reboot-ceiling"
      SLACK_WEBHOOK_SSM      = "/uts/alerts/slack-webhook-url"
      TELEGRAM_BOT_TOKEN_SSM = "/uts/alerts/telegram-bot-token"
      TELEGRAM_CHAT_ID_SSM   = "/uts/alerts/telegram-chat-id"
      EVENT_RULE_NAME        = local.auto_reboot_rule_name
      AWS_REGION_NAME        = var.aws_region
    }
  }

  depends_on = [aws_iam_role_policy.api_host_auto_reboot]
}

# ---------------------------------------------------------------------------
# CloudWatch alarm — 3/3 consecutive 5-min datapoints = 15 min impairment
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "api_host_impaired" {
  alarm_name          = local.api_host_alarm_name
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 3
  datapoints_to_alarm = 3
  metric_name         = "StatusCheckFailed_Instance"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Maximum"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    InstanceId = local.api_host_instance_id
  }

  alarm_description = "API host ${local.api_host_instance_id} — StatusCheckFailed_Instance for ≥15 min (3/3 datapoints). Triggers auto-reboot via EventBridge → Lambda."
}

# ---------------------------------------------------------------------------
# EventBridge rule — fires on CloudWatch Alarm State Change → ALARM
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "api_host_auto_reboot" {
  name        = local.auto_reboot_rule_name
  description = "Trigger auto-reboot Lambda when api-host-impaired alarm enters ALARM state"

  event_pattern = jsonencode({
    source      = ["aws.cloudwatch"]
    "detail-type" = ["CloudWatch Alarm State Change"]
    detail = {
      alarmName = [local.api_host_alarm_name]
      state = {
        value = ["ALARM"]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "api_host_auto_reboot" {
  rule = aws_cloudwatch_event_rule.api_host_auto_reboot.name
  arn  = aws_lambda_function.api_host_auto_reboot.arn
}

resource "aws_lambda_permission" "api_host_auto_reboot_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api_host_auto_reboot.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.api_host_auto_reboot.arn
}
