"""
Upper-layer node definitions: L4-L7, API services, UIs, Infrastructure, Storage.

Covers:
- L4 Cross-Instrument + Multi-Timeframe subgraph (_add_l4_nodes)
- L5 ML Pipeline subgraph (_add_l5_nodes)
- L6 Strategy and Execution subgraph (_add_l6_nodes)
- L7 Risk, PnL, Monitoring subgraph (_add_l7_nodes)
- API Services subgraph (_add_api_nodes)
- UI subgraphs — 4 sub-clusters (_add_ui_nodes)
- Infrastructure subgraph (_add_infra_nodes)
- Storage primitives subgraph (_add_storage_nodes)
- add_all_nodes() — top-level orchestrator

L1-L3 + svc()/panel_node() helpers + color palette C:
    See _topology_nodes.py
"""

import graphviz

from ._topology_nodes import svc


def _add_l4_nodes(g: graphviz.Digraph) -> None:
    """Add L4 Cross-Instrument + Multi-Timeframe subgraph nodes."""
    with g.subgraph(name="cluster_l4") as s:
        s.attr(
            label="L4 - Cross-Instrument + Multi-Timeframe  [Shared Data Plane]  (aggregates L3)",
            style="rounded",
            color="#d97706",
            bgcolor="#fffbeb",
        )
        s.node(
            "FCIS",
            **svc(
                "l4",
                "features-cross-instrument-svc  [CR Svc]\\nFDS + FVS event (many-to-one agg per underlying)\\nB: underlying x date | L: underlying\\nSinks: GCS + PubSub (features-cross-instrument-{underlying})",
                tooltip=(
                    "PubSub topic: features-cross-instrument-{underlying}\\n"
                    "Batch dims: underlying x date\\n"
                    "Live trigger: FDS + FVS events (subscribes to ALL instruments of one underlying)\\n"
                    "Key: many-to-one aggregation - subscribes to multiple topics, publishes to one per underlying\\n"
                    "Recovery: subscribe live + replay GCS; maintain in-memory state per underlying"
                ),
            ),
        )
        s.node(
            "FMTS",
            **svc(
                "l4",
                "features-multi-timeframe-svc  [CR Svc]\\nFDS event per timeframe boundary\\nB: cat x venue x inst x tf x date | L: cat x venue x inst x tf\\nSinks: GCS + PubSub (features-mtf-{cat}-{venue}-{inst}-{tf})",
                tooltip=(
                    "repo: features-multi-timeframe-service\\n"
                    "PubSub topic: features-multi-timeframe-{category}-{venue}-{instrument}-{timeframe}\\n"
                    "Features: tf_momentum_alignment, tf_structure_context, tf_vol_compression, tf_session_context\\n"
                    "Timeframes: 5m, 15m, 1h, 4h, 1d\\n"
                    "Category sharding included (unlike FCIS which is cross-category)"
                ),
            ),
        )


def _add_l5_nodes(g: graphviz.Digraph) -> None:
    """Add L5 ML Pipeline subgraph nodes."""
    with g.subgraph(name="cluster_l5") as s:
        s.attr(
            label="L5 - ML Pipeline  [Shared Data Plane]",
            style="rounded",
            color="#d97706",
            bgcolor="#fffbeb",
        )
        s.node(
            "ML",
            **svc(
                "l5",
                "ml-service  [VM standalone]\\ntrain: ~quarterly batch | infer: event-driven (on feature events)\\nB: model x inst x tf x target x cfg | L: model x venue x inst\\nSinks: GCS (model_artifacts_registry + predictions) + PubSub",
                tooltip=(
                    "Consolidated from ml-training-service + ml-inference-service (2026-05-20)\\n"
                    "Train dims: model x instrument x timeframe x target_type x config\\n"
                    "Infer dims: model x venue x instrument x date\\n"
                    "Deploy: standalone VM (~2hr training runs; Cloud Run max timeout insufficient)\\n"
                    "PubSub topic: predictions-{model}-{venue}-{instrument}\\n"
                    "Recovery: re-run batch job from checkpoint; reload model from GCS"
                ),
            ),
        )


def _add_l6_nodes(g: graphviz.Digraph) -> None:
    """Add L6 Strategy and Execution subgraph nodes."""
    with g.subgraph(name="cluster_l6") as s:
        s.attr(
            label="L6 - Strategy and Execution  [Client-Specific Plane]\\nco-located group: MTDH + MDPS + execution on same VM (in_memory transport)",
            style="rounded",
            color="#dc2626",
            bgcolor="#fef2f2",
        )
        s.node(
            "STR",
            **svc(
                "l6",
                "strategy-service  [CR Svc]\\nevent-driven (predictions + mktdata + positions)\\nB: strategy_id x client x date | L: strategy_id x client\\nSinks: GCS + PubSub (signals-{strategy}-{client})",
                tooltip=(
                    "PubSub topic: signals-{strategy_id}-{client}\\n"
                    "Batch dims: strategy_id x client x date\\n"
                    "Live trigger: predictions (ML inference) + market data (MDPS) + positions (PBM) via PubSub\\n"
                    "GAP: position monitoring still internal PositionMonitor (target: subscribe to PBM)\\n"
                    "Recovery: subscribe PBM for initial position snapshot, then ML + MDPS PubSub"
                ),
            ),
        )
        s.node(
            "EXEC",
            **svc(
                "l6",
                "execution-service  [VM co-loc]\\nevent-driven (on trade signals)\\nB: client x subaccount x date | L: client x subaccount\\nRedis: hot order state | Sinks: GCS + PubSub (orders-{client}-{sub}-{venue})",
                tooltip=(
                    "PubSub topic: orders-{client}-{subaccount}-{venue}\\n"
                    "Batch dims: client x subaccount x date\\n"
                    "Live trigger: trade signals from strategy-service via PubSub\\n"
                    "Redis: hot transient order state (NOT persistence - ephemeral)\\n"
                    "Order lifecycle events: ORDER_CREATED, ORDER_UPDATED, ORDER_CANCELLED, ORDER_FILLED, ORDER_REJECTED\\n"
                    "GAP: currently only publishes fills externally (target: full order lifecycle)\\n"
                    "Recovery: query exchange REST for open orders, restore Redis state"
                ),
            ),
        )


def _add_l7_nodes(g: graphviz.Digraph) -> None:
    """Add L7 Risk, PnL, Monitoring subgraph nodes.

    Post-consolidation (strategy_repo_consolidation_2026_05_19.md): position-balance-monitor,
    risk-and-exposure, and pnl-attribution are sub-operations of strategy-service. They are
    represented as a single node (STRAT_L7) with an operation axis breakdown.
    """
    with g.subgraph(name="cluster_l7") as s:
        s.attr(
            label="L7 - Risk, PnL, Monitoring  [Client-Specific Plane]",
            style="rounded",
            color="#db2777",
            bgcolor="#fdf2f8",
        )
        s.node(
            "STRAT_L7",
            **svc(
                "l7",
                "strategy-service  [CR Svc]  --operation {position-recon | risk-monitor | pnl-attribution}\\n"
                "event-driven (fills + mktdata + exec + risk + positions)\\n"
                "position-recon: B: client x venue x date | L: client x venue\\n"
                "risk-monitor:   B: client x date | L: client | VaR, Greeks, DeFi LTV\\n"
                "pnl-attribution: B: client x date | L: client | delta, basis, funding, Greeks dims\\n"
                "Sinks: GCS + PubSub (positions-{client}-{venue} | risk-{client} | pnl-{client})",
                tooltip=(
                    "Consolidated from 3 source repos (strategy_repo_consolidation_2026_05_19.md):\\n"
                    "  --operation position-recon: reconciles fills vs exchange positions (was PBM)\\n"
                    "  --operation risk-monitor: VaR/Greeks/LTV (was risk-and-exposure-service)\\n"
                    "  --operation pnl-attribution: delta/basis/funding/Greeks dims (was pnl-attribution-service)\\n"
                    "PubSub topics: positions-{client}-{venue} | risk-{client} | pnl-{client}\\n"
                    "Recovery: query exchange REST + subscribe MDPS+execution PubSub"
                ),
            ),
        )
        s.node(
            "ALT",
            **svc(
                "l7",
                "alerting-service  [CR Svc]\\nsingleton (no sharding)\\ncloud-logging crash subscription\\ncircuit breakers | Slack | PagerDuty",
                tooltip=(
                    "Singleton -- no sharding, no topic template\\n"
                    "Consumes: ALL lifecycle events from all services (unified events topic)\\n"
                    "Also receives: risk alerts, PBM balance alerts, execution rejection spikes\\n"
                    "Publishes: circuit-breaker-commands (halt execution+strategy) + external notifications\\n"
                    "Recovery: Cloud Run auto-restart + PubSub message retention"
                ),
            ),
        )


def _add_api_nodes(g: graphviz.Digraph) -> None:
    """Add API Services subgraph nodes."""
    with g.subgraph(name="cluster_api") as s:
        s.attr(
            label="API Services  [CR Svc, OAuth, auto-scale]\\nUI -> API (HTTP REST + SSE) -> Service -> Storage  |  APIs never own data",
            style="rounded",
            color="#7c3aed",
            bgcolor="#faf5ff",
        )
        s.node(
            "MDA",
            **svc(
                "api",
                tooltip=(
                    "Port 8003 | OAuth authenticated\\n"
                    "Batch: serves historical order book snapshots and candles via HTTP REST from GCS\\n"
                    "Live: SSE orderbook from MTDH PubSub; SSE candles from MDPS PubSub\\n"
                    "GAP: currently orderbook only (target: orderbook + candles SSE)"
                ),
            ),
        )
        s.node(
            "ERA",
            **svc(
                "api",
                "execution-results-api  :8002  [CR Svc, OAuth]\\nHTTP REST + SSE fills + order lifecycle\\nB: reads execution results (GCS)\\nL: SSE fills (exec-svc PS)",
                tooltip=(
                    "Port 8002 | OAuth authenticated\\n"
                    "Batch: historical execution results from GCS\\n"
                    "Live: SSE fills and order lifecycle events from execution-service PubSub\\n"
                    "Consumed by: trading-analytics-ui, execution-analytics-ui, settlement-ui"
                ),
            ),
        )
        s.node(
            "CRS",
            **svc(
                "api",
                "client-reporting-api  :8005  [CR Job, OAuth]\\nbatch reports + live SSE P&L\\nB: reads PnL+risk+positions (GCS)\\nL: SSE P&L (pnl-svc PS) [target]",
                tooltip=(
                    "Port 8005 | OAuth authenticated\\n"
                    "Batch: historical P&L reports, portfolio summaries, invoices from GCS\\n"
                    "Live target: SSE P&L updates from strategy-service pnl-attribution PubSub\\n"
                    "GAP: batch only currently"
                ),
            ),
        )
        s.node(
            "DEPAPI",
            **svc(
                "api",
                "deployment-api  :8001  [CR Svc, OAuth]\\nHTTP REST + SSE health events\\norchestration + kill-switch\\nReads: deployment-engine",
                tooltip=(
                    "Port 8001 | OAuth authenticated\\n"
                    "Endpoints: deployments, services, config, data-status, service-status, cloud-builds, checklists\\n"
                    "Live: SSE endpoint for health monitoring events (-> unified-trading-system-ui)\\n"
                    "Kill switch: /kill-switch/{service}/activate (OAuth-gated, state in Secret Manager)"
                ),
            ),
        )
        s.node(
            "STRAPI",
            **svc(
                "api",
                "strategy-api  :8004  [CR Svc, OAuth]\\nbacktest results + signal configs\\nB: reads signals (GCS)\\nThin FastAPI gateway over strategy-svc outputs",
                planned=True,
                tooltip=(
                    "PLANNED - new repo: strategy-api\\n"
                    "Port 8004 | OAuth authenticated\\n"
                    "Thin FastAPI gateway over strategy-service batch outputs (signals_backtest_results GCS)"
                ),
            ),
        )
        s.node(
            "UTAPI",
            **svc(
                "api",
                "unified-trading-api  :8030  [CR Svc, OAuth]\\nconsolidated domain HTTP (archived batch-audit-api)\\nB+L: platform routes for unified-trading-system-ui",
                tooltip=(
                    "Port 8030 | OAuth authenticated\\n"
                    "Consolidated API for unified-trading-system-ui (audit, domain surfaces)\\n"
                    "SSOT: unified-trading-pm/configs/runtime-topology.yaml api_services"
                ),
            ),
        )
        s.node(
            "AUTHAPI",
            **svc(
                "api",
                "auth-api  :8200  [CR Svc, OAuth]\\nJWT, persona, OAuth token exchange",
                tooltip=(
                    "Port 8200 | Authentication service\\nLogin flows from unified-trading-system-ui and other UIs"
                ),
            ),
        )


def _add_ui_nodes(g: graphviz.Digraph) -> None:
    """Add UIs subgraph nodes (4 sub-clusters)."""
    with g.subgraph(name="cluster_ui") as s:
        s.attr(
            label="UIs  [CR Svc, auto-scale]  React/TypeScript static build\\nUI and API are SEPARATE Cloud Run Services (separate repos, separate scaling)",
            style="rounded",
            color="#06b6d4",
            bgcolor="#ecfeff",
        )

        with s.subgraph(name="cluster_ui_batch") as b:
            b.attr(
                label="Batch Research (3 tiers)",
                style="rounded,dashed",
                color="#94a3b8",
                bgcolor="#f0fdfa",
            )
            b.node(
                "MLUI",
                **svc(
                    "ui",
                ),
            )
            b.node(
                "STUI",
                **svc(
                    "ui",
                    "strategy-ui  [CR Svc]\\nstrategy backtest + param tuning\\n-> strategy-api :8004",
                    tooltip="Consumes: strategy-api (backtest results, signal configs)",
                ),
            )
            b.node(
                "EXANI",
                **svc(
                    "ui",
                    tooltip=("Content migration (execution-service/visualizer-ui/ extraction) tracked separately"),
                ),
            )

        with s.subgraph(name="cluster_ui_trade") as t:
            t.attr(
                label="Trading + Monitoring",
                style="rounded,dashed",
                color="#94a3b8",
                bgcolor="#f0fdfa",
            )
            t.node(
                "TAUI",
                **svc(
                    "ui",
                    "trading-analytics-ui  [CR Svc]\\nSSE fills + live P&L\\n-> execution-results-api :8002",
                    tooltip="Consumes: execution-results-api (live fills SSE)",
                ),
            )

        with s.subgraph(name="cluster_ui_ops") as o:
            o.attr(label="Ops + Deployment", style="rounded,dashed", color="#94a3b8", bgcolor="#f0fdfa")
            o.node(
                "DEPUI",
                **svc("ui", "deployment-ui  [CR Svc]\\nbatch vs live deploy\\n-> deployment-api :8001"),
            )
            o.node(
                "UTSUI",
                **svc(
                    "ui",
                    "unified-trading-system-ui  [CR Svc]\\n"
                    "health SSE, batch audit, logs, onboarding (consolidated)\\n"
                    "-> deployment-api + unified-trading-api :8030 + auth-api :8200",
                    tooltip=(
                        "Replaces archived: live-health-monitor-ui, batch-audit-ui, logs-dashboard-ui, onboarding-ui.\\n"
                        "Consumes: deployment-api (SSE health, kill switch), unified-trading-api, auth-api"
                    ),
                ),
            )

        with s.subgraph(name="cluster_ui_client") as c:
            c.attr(
                label="Client + Reporting",
                style="rounded,dashed",
                color="#94a3b8",
                bgcolor="#f0fdfa",
            )
            c.node("CRUI", **svc("ui", "client-reporting-ui  [CR Svc]\\n-> client-reporting-api :8005"))
            c.node(
                "SETU",
                **svc(
                    "ui",
                    "settlement-ui  [CR Svc]\\n-> execution-results-api :8002\\n(not yet built)",
                    planned=True,
                ),
            )


def _add_infra_nodes(g: graphviz.Digraph) -> None:
    """Add Infrastructure subgraph nodes."""
    with g.subgraph(name="cluster_infra") as s:
        s.attr(label="Infrastructure", style="rounded", color="#64748b", bgcolor="#f8fafc")
        s.node("DEPENG", **svc("infra", "deployment-engine\\norchestrator + CLI + Terraform"))
        s.node("SIT", **svc("infra", "system-integration-tests\\nL3a smoke + L3b full E2E"))


def _add_storage_nodes(g: graphviz.Digraph) -> None:
    """Add Storage + Transport Primitives subgraph nodes."""
    with g.subgraph(name="cluster_store") as s:
        s.attr(
            label="Storage + Transport Primitives\\n"
            "Rule: ALL services persist to GCS. Publish + persist in PARALLEL.\\n"
            "Consumer deduplicates by timestamp. See publish+persist panel.",
            style="rounded",
            color="#16a34a",
            bgcolor="#f0fdf4",
        )
        s.node(
            "GCS",
            shape="cylinder",
            label=(
                "GCS  (universal persistence sink)\\n"
                "instruments | ticks | candles | features\\n"
                "models | predictions | signals | fills\\n"
                "positions | risk | PnL | audit"
            ),
            fillcolor="#f0fdf4",
            color="#16a34a",
            style="filled",
            penwidth="2",
            fontsize="8",
            fontname="Arial",
            tooltip="Every service writes to GCS regardless of transport mode. In batch: GCS is also the transport. In live: GCS is persistence-only (PubSub is transport).",
        )
        s.node(
            "PS",
            shape="cylinder",
            label=(
                "PubSub  (live event bus)\\n"
                "scope: CROSS_VM\\n"
                "topics: see service node tooltips\\n"
                "retention: configurable (recovery support)"
            ),
            fillcolor="#f0fdf4",
            color="#16a34a",
            style="filled",
            penwidth="2",
            fontsize="8",
            fontname="Arial",
            tooltip="Live transport only -- ephemeral. Data published to PubSub MUST also be persisted to GCS separately. Topic templates per service in node tooltips.",
        )
        s.node(
            "REDIS",
            shape="cylinder",
            label=(
                "Redis  (hot transient state)\\nscope: SAME_VM\\nexec-svc: hot order state\\nMDPS: ~1yr candle cache"
            ),
            fillcolor="#f0fdf4",
            color="#16a34a",
            style="filled",
            penwidth="2",
            fontsize="8",
            fontname="Arial",
            tooltip="NOT persistence, NOT transport. Execution-service: hot order state (ephemeral). MDPS: ~1yr rolling candle window (survives restarts via GCS warmup).",
        )


def add_all_nodes(g: graphviz.Digraph) -> None:
    """Add all layer, API, UI, infra, and storage nodes to *g*."""
    from ._topology_nodes import _add_l1_nodes, _add_l2_nodes, _add_l3_nodes

    _add_l1_nodes(g)
    _add_l2_nodes(g)
    _add_l3_nodes(g)
    _add_l4_nodes(g)
    _add_l5_nodes(g)
    _add_l6_nodes(g)
    _add_l7_nodes(g)
    _add_api_nodes(g)
    _add_ui_nodes(g)
    _add_infra_nodes(g)
    _add_storage_nodes(g)
