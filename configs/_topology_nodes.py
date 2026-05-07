"""
Node helper factories, color palette, and L1-L3 layer subgraph nodes.

Covers:
- Color palette C and PLANNED_COLOR constant
- svc() / panel_node() helper factories
- L1 Data Ingestion subgraph (_add_l1_nodes)
- L2 Market Data Processing subgraph (_add_l2_nodes)
- L3 Feature Computation subgraph (_add_l3_nodes)

Upper layers (L4-L7), API, UI, Infra, Storage and add_all_nodes():
    See _topology_nodes_upper.py
"""

import graphviz

PLANNED_COLOR = "#ca8a04"

C = {
    "l1": ("#dbeafe", "#3b82f6"),
    "l2": ("#e0f2fe", "#0284c7"),
    "l3": ("#dcfce7", "#16a34a"),
    "l4": ("#fef3c7", "#d97706"),
    "l5": ("#fef3c7", "#d97706"),
    "l6": ("#fee2e2", "#dc2626"),
    "l7": ("#fce7f3", "#db2777"),
    "api": ("#f3e8ff", "#7c3aed"),
    "ui": ("#ecfeff", "#06b6d4"),
    "infra": ("#f1f5f9", "#64748b"),
    "store": ("#f0fdf4", "#16a34a"),
    "planned": ("#fefce8", PLANNED_COLOR),
    "panel": ("#f8fafc", "#64748b"),
}


def svc(layer, label, planned=False, rename=None, tooltip=""):
    c = C["planned"] if planned else C[layer]
    lbl = label
    if planned:
        lbl += "\\n(PLANNED)"
    if rename:
        lbl += f"\\n[repo: {rename}]"
    attrs = {
        "label": lbl,
        "style": ("dashed,filled" if planned else "filled"),
        "fillcolor": c[0],
        "color": c[1],
        "penwidth": "2",
        "fontsize": "9",
        "fontname": "Arial",
        "shape": "box",
        "margin": "0.15,0.08",
    }
    if tooltip:
        attrs["tooltip"] = tooltip
    return attrs


def panel_node(name, label):
    """Reference panel styled as a record/note box."""
    return {
        "name": name,
        "label": label,
        "shape": "note",
        "style": "filled",
        "fillcolor": C["panel"][0],
        "color": C["panel"][1],
        "penwidth": "1",
        "fontsize": "8",
        "fontname": "Arial Narrow",
        "margin": "0.15,0.1",
    }


def _add_l1_nodes(g: graphviz.Digraph) -> None:
    """Add L1 Data Ingestion subgraph nodes."""
    with g.subgraph(name="cluster_l1") as s:
        s.attr(
            label="L1 - Data Ingestion  [Shared Data Plane]",
            style="rounded",
            color="#3b82f6",
            bgcolor="#f0f7ff",
        )
        s.node(
            "IS",
            **svc(
                "l1",
                "instruments-service  [CR Job]\\n~15min poll -> PubSub instrument events\\nB: cat x venue x date | L: venue\\nSinks: GCS + PubSub (instrument-events-{venue})",
                tooltip=(
                    "PubSub topic: instrument-events-{venue}\\n"
                    "Batch dims: category x venue x date\\n"
                    "Live trigger: ~15min timer poll\\n"
                    "Recovery: full re-fetch from venue REST APIs on restart"
                ),
            ),
        )
        s.node(
            "MTDH",
            **svc(
                "l1",
                "market-tick-data-service  [VM co-loc]\\ncontinuous WebSocket stream\\nB: cat x venue x inst_type x data_type x date\\nSinks: GCS + PubSub (raw-ticks-{venue}-{inst_type}-{data_type})",
                tooltip=(
                    "PubSub topic: raw-ticks-{venue}-{inst_type}-{data_type}\\n"
                    "Batch dims: category x venue x instrument_type x data_type x date\\n"
                    "Live trigger: continuous WebSocket message arrival\\n"
                    "Recovery: 1) WS reconnect exp-backoff 1s-32s 2) venue REST backfill 3) Tardis replay 4) GCS"
                ),
            ),
        )


def _add_l2_nodes(g: graphviz.Digraph) -> None:
    """Add L2 Market Data Processing subgraph nodes."""
    with g.subgraph(name="cluster_l2") as s:
        s.attr(
            label="L2 - Market Data Processing  [Shared Data Plane]",
            style="rounded",
            color="#0284c7",
            bgcolor="#f0f9ff",
        )
        s.node(
            "MDPS",
            **svc(
                "l2",
                "market-data-processing-service  [VM co-loc]\\n~15s timer (1m/5m/15m/1h/4h/24h on boundaries; clock-aligned)\\nB: cat x venue x inst_type x date x tf | L: venue x inst_type\\nRedis: ~1yr rolling candle window | Sinks: GCS + PubSub (candles-{venue}-{inst_type}-{tf})",
                tooltip=(
                    "PubSub topic: candles-{venue}-{inst_type}-{timeframe}\\n"
                    "Batch dims: category x venue x instrument_type x date x timeframe\\n"
                    "Live trigger: ~15s timer; 1m(x4), 5m(x20), 15m(x60), 1h(x240), 4h(x960), 24h(x5760)\\nClock-alignment: starts at even time boundary since midnight (ensures full candle blocks)\\n"
                    "Redis: ~1yr rolling candle window survives restarts (warmup from GCS on start)\\n"
                    "Recovery: subscribe live PubSub + replay GCS on separate thread; deduplicate by timestamp"
                ),
            ),
        )


def _add_l3_nodes(g: graphviz.Digraph) -> None:
    """Add L3 Feature Computation subgraph nodes."""
    with g.subgraph(name="cluster_l3") as s:
        s.attr(
            label="L3 - Feature Computation  [Shared Data Plane]  (MDPS-event-driven, not timer)",
            style="rounded",
            color="#16a34a",
            bgcolor="#f0fdf4",
        )
        s.node(
            "FCS",
            **svc(
                "l3",
                "features-calendar-svc  [CR Job]\\ndaily 00:05 UTC (Cloud Scheduler)\\nB: cat x date (batch only)\\nSinks: GCS only",
                tooltip=(
                    "Batch only -- no live mode\\nBatch dims: category x date\\nRecovery: re-run batch job"
                ),
            ),
        )
        s.node(
            "FDS",
            **svc(
                "l3",
                "features-delta-one-svc  [CR Svc]\\nMDPS completion event (event-driven)\\nB: cat x venue x feat_cat x date | L: venue x feat_cat\\nSinks: GCS + PubSub (features-{cat}-{venue})",
                tooltip=(
                    "PubSub topic: features-{category}-{venue}\\n"
                    "Batch dims: category x venue x feature_category x date\\n"
                    "Live trigger: MDPS completion PubSub event (NOT a timer)\\n"
                    "Recovery: subscribe live + replay GCS; deduplicate by timestamp"
                ),
            ),
        )
        s.node(
            "FVS",
            **svc(
                "l3",
                "features-volatility-svc  [CR Svc]\\nMDPS completion event (event-driven)\\nB: cat x venue x feat_cat x date | L: venue x feat_cat\\nSinks: GCS + PubSub (features-{cat}-{venue})",
                tooltip=(
                    "PubSub topic: features-{category}-{venue}\\n"
                    "Batch dims: category x venue x feature_category x date\\n"
                    "Live trigger: MDPS completion PubSub event\\n"
                    "Recovery: subscribe live + replay GCS; deduplicate by timestamp"
                ),
            ),
        )
        s.node(
            "FOS",
            **svc(
                "l3",
                "features-onchain-svc  [CR Job]\\nevery 60s (Cloud Scheduler, config-driven)\\nB: protocol x chain x date (batch only)\\nSinks: GCS only",
                tooltip=(
                    "Batch only -- no live mode\\nBatch dims: protocol x chain x date\\nRecovery: re-run batch job"
                ),
            ),
        )
