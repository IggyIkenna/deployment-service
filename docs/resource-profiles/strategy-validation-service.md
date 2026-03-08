# Resource Profile: strategy-validation-service

## Deployment Mode

- Mode: batch (validation job before signal forwarding)
- Cloud Run region: asia-northeast1
- Execution: Cloud Run Job

## Resource Allocation

### Batch Mode

| Resource    | Value           | Rationale                                                                                             |
| ----------- | --------------- | ----------------------------------------------------------------------------------------------------- |
| CPU         | 2 vCPU          | Statistical validation tests across signal history; compute-bound for walk-forward significance tests |
| Memory      | 8 Gi            | Loads historical signals + benchmark returns for statistical significance testing                     |
| Timeout     | 86400 s (24 hr) | Maximum — full validation over multi-year signal history                                              |
| Max retries | 3               | Idempotent; retry on transient failures                                                               |

## Cost Estimate

GCP Cloud Run pricing (vCPU-second: $0.00002400, GB-second: $0.00000250).

| Scenario            | Invocations/day | Avg duration    | Est. monthly cost |
| ------------------- | --------------- | --------------- | ----------------- |
| Pre-live validation | 1               | 1800 s (30 min) | ~$1.10            |
| Full signal audit   | 1               | 7200 s (2 hr)   | ~$4.30            |

Assumptions: 2 vCPU + 8 Gi; runs before each strategy deployment or parameter change.

## Data Flow

- **Source:** GCS signals bucket (strategy-service output) + GCS market data (benchmark returns)
- **Sink:** GCS validation reports bucket; gates signal forwarding to execution-service

## Validation Checks

- Statistical significance (t-test, Sharpe ratio with confidence intervals)
- Drawdown limits (maximum drawdown vs configured threshold)
- Overfitting detection (in-sample vs out-of-sample performance degradation)
- Signal consistency (autocorrelation, regime stability)

## Special Requirements

- Validation failure blocks signal forwarding to execution-service — this is a safety gate
- Upstream dependency: strategy-service must have generated signals for the validation period
- Validation thresholds configured in strategy configs stored in Secret Manager

## Source References

- `deployment-service/terraform/services/strategy-validation-service/gcp/variables.tf`
