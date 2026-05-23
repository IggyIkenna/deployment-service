# Layer-0 Deterministic Recovery Scripts

> SSOT directory for the 10 closed-set deterministic recovery actions consumed
> by the Incident Gateway state machine + the LLM Layer-1.5 backup actuator +
> the DART Safety Ops manual-override tab.

Codex SSOT:
- `codex/04-architecture/recovery-defence-in-depth-layers.md` § Layer 0
- `codex/04-architecture/incident-gateway-state-machine.md`

Implementation plan: `plans/active/agent_recovery_controller_layer0_deterministic_2026_05_23.md`.

## The 10 scripts

| Script                          | Action                                                    | Runbook        | Idempotent |
| ------------------------------- | --------------------------------------------------------- | -------------- | ---------- |
| `restart_service.py`            | Cloud Run revision flip / GCE systemctl restart            | RB-INFRA-001   | yes        |
| `restart_container.py`          | Cloud Run revision or Docker restart                       | RB-INFRA-001   | yes        |
| `redeploy_known_good.py`        | Flip Cloud Run traffic to previous revision                | RB-DEPLOY-001  | yes        |
| `resize_machine_after_oom.py`   | `gcloud compute instances set-machine-type`                | RB-INFRA-001   | yes        |
| `failover_feed.py`              | MTDS handler primary → backup feed                          | RB-CONN-001   | yes        |
| `pause_strategy.py`             | strategy-service pause endpoint                            | RB-RISK-004   | yes        |
| `cancel_open_orders.py`         | execution-service cancel-all-orders                         | RB-RECON-002  | **no**     |
| `disable_venue.py`              | circuit-breaker force-open                                  | RB-CONN-001   | yes        |
| `enter_safe_mode.py`            | strategy-service safe-mode entry                            | RB-RISK-004   | yes        |
| `enter_readonly_recon_mode.py`  | service reads but rejects writes                            | RB-CONN-004   | yes        |

## Contract (every script)

Each script MUST:

1. Implement `--dry-run` (returns plan; no side-effects).
2. Emit `AgentActionEvent` with `status=STARTED` before the action.
3. Check `RepeatedRepairLoopDetector` and bail out with `LoopDetected`
   escalation if 3+ identical actions in 15min.
4. Be idempotent where possible (re-running on already-actioned scope is
   a safe no-op).
5. Emit `AgentActionEvent` with `status=SUCCEEDED|FAILED` + recovery
   verification result after the action.
6. Carry runbook_id from the registry (no inline strings).

All scripts use the shared `_common.py` `Layer0Script` base class which
handles arg parsing + emitter wiring + loop-detector check + provenance
tagging. Each script implements only its action-specific `_execute()` method.

## CLI shape

```
python -m deployment_service.scripts.recovery.<action> \
    --reason "Operator override per OOM detection on execution-service" \
    --provenance AUTOMATIC|MANUAL_OPERATOR|LLM_LAYER15 \
    --incident-key <key> \
    [--dry-run] \
    [--service <name> | --venue <name> | --strategy <id> | --vm <name>] ...
```

## Layer-1.5 LLM wrapper

The LLM recovery-audit-signoff agent invokes these scripts via
`deployment-service/scripts/recovery/llm_invoke_layer0.py` (closed-set
wrapper). The wrapper validates `action_type` against `RecoveryScriptRegistry`
and rejects anything outside the registered 10.
