"""
Architecture reference panel nodes for the topology DAG.

All five reference panels (legend, recovery, circuit breakers,
publish+persist policy, deployment modes summary) are defined here.
"""

import graphviz


def _add_reference_panels(g: graphviz.Digraph) -> None:
    """Add architecture reference panel nodes to the graph."""

    # Legend panel
    g.node(
        "LEGEND",
        label=(
            "LEGEND\\l"
            "--------------------------------------\\l"
            "-- solid blue (2px)    batch flow  B:GCS\\l"
            "-- solid purple (2px)  live flow   L:PubSub\\l"
            "-- dashed orange (2px) co-located  CoLoc:in_memory (same VM)\\l"
            "-- solid cyan (1.5px)  HTTP / SSE  (API->UI or API->svc)\\l"
            "-- dashed red (2.5px)  CIRCUIT BREAKER / kill-switch\\l"
            "-- dashed amber (1.5px) planned (not yet built)\\l"
            "-- dotted green (1px)  Redis sink (exceptional)\\l"
            "--------------------------------------\\l"
            "Sinks badge on node label (no dotted edges to GCS/PubSub):\\l"
            "  'Sinks: GCS + PubSub' = publishes live + persists batch\\l"
            "  'Sinks: GCS only'     = batch/infrequent service\\l"
            "--------------------------------------\\l"
            "Deploy badges:  [VM co-loc]  [VM standalone]\\l"
            "               [CR Svc]  [CR Job]\\l"
        ),
        shape="note",
        style="filled",
        fillcolor="#fffbeb",
        color="#d97706",
        penwidth="1.5",
        fontsize="8",
        fontname="Arial Narrow",
        margin="0.15,0.1",
    )

    # Recovery chains panel
    g.node(
        "RECOVERY",
        label=(
            "RECOVERY CHAINS  (SSOT: sec.13)\\l"
            "--------------------------------------\\l"
            "CeFi crypto:\\l"
            "  1. UMI WS reconnect (exp-backoff 1s-32s, max 10)\\l"
            "  2. Venue REST backfill (<3mo most exchanges)\\l"
            "  3. Tardis replay (~7yr, WS-identical format)\\l"
            "  4. GCS historical (last resort)\\l"
            "TradFi:\\l"
            "  1. Venue reconnect\\l"
            "  2. Databento (7yr, live-identical, CME/Nasdaq/NYSE)\\l"
            "  3. IBKR TWS backfill (6mo tick, rate-limited)\\l"
            "  4. GCS historical\\l"
            "DeFi:\\l"
            "  1. RPC reconnect (The Graph / Alchemy / direct node)\\l"
            "  2. Block replay (blockchain = immutable, full history)\\l"
            "Internal svc (MDPS/features/ML/strategy/risk/PnL):\\l"
            "  concurrent replay GCS + live PubSub; deduplicate by ts\\l"
        ),
        shape="note",
        style="filled",
        fillcolor="#f0f7ff",
        color="#3b82f6",
        penwidth="1.5",
        fontsize="8",
        fontname="Arial Narrow",
        margin="0.15,0.1",
    )

    # Circuit breakers + kill switch panel
    g.node(
        "CIRCUIT",
        label=(
            "CIRCUIT BREAKERS + KILL SWITCH  (SSOT: sec.16)\\l"
            "--------------------------------------\\l"
            "Manual kill switch (human-initiated):\\l"
            "  deployment-api /kill-switch/{svc}/activate  (OAuth)\\l"
            "  -> PubSub: kill-switch-commands\\l"
            "  -> targets: execution-service + strategy-service\\l"
            "  -> state persisted in Secret Manager\\l"
            "Automated circuit breaker (alerting-initiated):\\l"
            "  alerting-service publishes CIRCUIT_BREAKER_OPEN\\l"
            "  -> PubSub: circuit-breaker-commands\\l"
            "  triggers: risk breach | order rejection spike\\l"
            "            balance discrepancy | connectivity loss\\l"
            "  escalation: PubSub -> Slack -> PagerDuty\\l"
            "Reset policy:\\l"
            "  position mismatch  -> auto-reset after recon\\l"
            "  network            -> auto-reset on reconnect\\l"
            "  risk breach        -> MANUAL reset only\\l"
            "  rate limit         -> auto-reset after cooldown\\l"
        ),
        shape="note",
        style="filled",
        fillcolor="#fef2f2",
        color="#dc2626",
        penwidth="1.5",
        fontsize="8",
        fontname="Arial Narrow",
        margin="0.15,0.1",
    )

    # Publish + Persist policy panel
    g.node(
        "PPOLICY",
        label=(
            "PUBLISH + PERSIST POLICY  (SSOT: sec.14)\\l"
            "--------------------------------------\\l"
            "Rule: publish PubSub + persist GCS in PARALLEL\\l"
            "  (not sequentially; GCS write must not block live publish)\\l"
            "Result: small overlap window at live->batch merge point\\l"
            "  -> consumer deduplicates by exchange_timestamp\\l"
            "Timestamps per message:\\l"
            "  exchange_timestamp  = canonical ordering key\\l"
            "  local_timestamp     = latency monitoring\\l"
            "  sequence_number     = gap detection per stream\\l"
            "Switchover (live start):\\l"
            "  1. Subscribe live PubSub (messages queue)\\l"
            "  2. Replay GCS up to last persisted ts (separate thread)\\l"
            "  3. Drain queued PubSub messages\\l"
            "  4. Merge point: live takes over, replay stops\\l"
        ),
        shape="note",
        style="filled",
        fillcolor="#f0fdf4",
        color="#16a34a",
        penwidth="1.5",
        fontsize="8",
        fontname="Arial Narrow",
        margin="0.15,0.1",
    )

    # Deployment modes summary panel
    g.node(
        "DEPLOYSUM",
        label=(
            "DEPLOYMENT MODES  (SSOT: sec.21 / runtime-topology.yaml)\\l"
            "--------------------------------------\\l"
            "VM co-located (always-on):\\l"
            "  MTDH + MDPS + execution-svc  (in_memory transport)\\l"
            "VM standalone (manual/scheduled):\\l"
            "  ml-training-svc  (~2hr runs, Cloud Run timeout N/A)\\l"
            "Cloud Run Svc (always-on):\\l"
            "  strategy | PBM | risk | alerting\\l"
            "Cloud Run Svc (mode-dependent):\\l"
            "  ml-inference | features-d1 | features-vol | features-xI\\l"
            "  batch=scale-to-zero | live=always-on\\l"
            "Cloud Run Job (scale-to-zero):\\l"
            "  instruments | features-calendar | features-onchain\\l"
            "  pnl-attribution | strategy-validation\\l"
            "Cloud Run Svc (auto-scale, OAuth):\\l"
            "  deployment-api :8001 | client-reporting-api :8005\\l"
            "Cloud Run Svc (auto-scale) - UIs:\\l"
            "  ALL UIs serve React static build\\l"
            "  UI and API are SEPARATE services (separate repos/scaling)\\l"
        ),
        shape="note",
        style="filled",
        fillcolor="#f8fafc",
        color="#64748b",
        penwidth="1.5",
        fontsize="8",
        fontname="Arial Narrow",
        margin="0.15,0.1",
    )
