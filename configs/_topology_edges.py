"""
Pipeline flow edges and reference panel positioning for the topology DAG.

All data-flow edges (L1->L7, API->UI, infra, Redis sinks) and invisible
rank-sink edges for panel positioning are defined here.
"""

import graphviz

# Edge style presets (must stay in sync with generate_topology_svg.py)
PLANNED_COLOR = "#ca8a04"

B = {"color": "#2563eb", "penwidth": "2.0"}  # batch
L = {"color": "#7c3aed", "penwidth": "2.0"}  # live
CO = {"color": "#ea580c", "penwidth": "2.0", "style": "dashed"}  # co-located in_memory
H = {"color": "#06b6d4", "penwidth": "1.5"}  # HTTP/SSE
K = {"color": "#dc2626", "penwidth": "2.5", "style": "dashed"}  # circuit breaker / kill switch
PL = {"color": PLANNED_COLOR, "penwidth": "1.5", "style": "dashed"}  # planned
REDIS = {
    "color": "#16a34a",
    "penwidth": "1.0",
    "style": "dotted",
    "arrowsize": "0.6",
    "constraint": "false",
}


def _add_edges(g: graphviz.Digraph) -> None:
    """Add pipeline flow edges and reference panel positioning to the graph."""

    # L1 internal
    g.edge("IS", "MTDH", label="instruments_universe\\nB:GCS | L:PS", **B)
    g.edge("IS", "FMTS", label="instruments_universe\\nB:GCS | L:PS", **B, constraint="false")

    # L1 -> L2 (co-located group)
    g.edge("MTDH", "MDPS", label="raw_tick_data\\nB:GCS", **B)
    g.edge("MTDH", "MDPS", label="raw_tick_data\\nL:PS", **L)
    g.edge("MTDH", "MDPS", label="raw_tick_data\\nCoLoc:in_memory", **CO)

    # L2 -> L3
    g.edge("MDPS", "FDS", label="processed_candles_ohlcv\\nB:GCS | L:PS", **B)
    g.edge("MDPS", "FVS", label="processed_candles_ohlcv\\nL:PS", **L)

    # L3 -> L4
    g.edge("FDS", "FCIS", label="delta_one_features\\nB:GCS | L:PS", **B)
    g.edge("FVS", "FCIS", label="volatility_features\\nL:PS", **L)
    g.edge("FDS", "FMTS", label="delta_one_features\\nB:GCS | L:PS", **B)

    # L4 -> L5
    g.edge("FCIS", "MLTR", label="cross_instrument_features\\nB:GCS", **B)
    g.edge("FCIS", "MLIN", label="cross_instrument_features\\nL:PS", **L)
    g.edge("FMTS", "MLTR", label="mtf_features  B:GCS", **B, constraint="false")
    g.edge("FMTS", "MLIN", label="mtf_features  L:PS", **L, constraint="false")

    # L3 -> L5 direct (live features to inference)
    g.edge("FDS", "MLIN", label="delta_one_features\\nlive L:PS", **L, constraint="false")
    g.edge("FVS", "MLIN", label="vol_features  L:PS", **L, constraint="false")

    # L5 internal
    g.edge("MLTR", "MLIN", label="model_artifacts_registry\\nB:GCS (~quarterly)", **B)

    # L5 -> L6
    g.edge("MLIN", "STR", label="predictions\\nB:GCS | L:PS", **B)

    # MDPS -> L6 (market data to strategy live)
    g.edge("MDPS", "STR", label="processed_candles\\nL:PS", **L, constraint="false")

    # L6 internal
    g.edge("STR", "EXEC", label="trade_signals\\nB:GCS | L:PS", **B)

    # Co-located market feed to execution
    g.edge("MTDH", "EXEC", label="live market feed\\nCoLoc:in_memory", **CO, constraint="false")

    # L6 -> L7
    g.edge("EXEC", "PBM", label="order_lifecycle_events\\nL:PS", **L)
    g.edge("EXEC", "PNL", label="execution_results\\nB:GCS | L:PS", **B, constraint="false")
    g.edge("PBM", "RAE", label="position_updates\\nB:GCS | L:PS", **B)
    g.edge("RAE", "PNL", label="risk_metrics\\nB:GCS | L:PS", **B)

    # L7 feedback loop (position authority)
    g.edge("PBM", "STR", label="position_snapshots\\nL:PS (authority)", **L, constraint="false")

    # Risk -> alerting (circuit breaker triggers)
    g.edge(
        "RAE",
        "ALT",
        label="risk_alerts (circuit\\nbreaker triggers)  L:PS",
        **L,
        constraint="false",
    )
    g.edge("PBM", "ALT", label="balance_alerts  L:PS", **L, constraint="false")

    # Circuit breakers (alerting -> services)
    g.edge(
        "ALT",
        "EXEC",
        label="CIRCUIT BREAKER HALT\\nkill-switch-commands PS",
        **K,
        constraint="false",
    )
    g.edge("ALT", "STR", label="CIRCUIT BREAKER HALT", **K, constraint="false")

    # Service -> API (data source connections)
    g.edge("EXEC", "ERA", label="execution_results\\nB:GCS | L:PS", **B, constraint="false")
    g.edge("MDPS", "MDA", label="candles\\nB:GCS | L:PS (SSE)", **B, constraint="false")
    g.edge("MTDH", "MDA", label="orderbook_stream\\nL:PS (SSE)", **L, constraint="false")
    g.edge("PNL", "CRS", label="pnl_reports\\nB:GCS | L:PS (SSE)", **B, constraint="false")
    g.edge("DEPENG", "DEPAPI", label="orchestration", **H)
    g.edge("STR", "STRAPI", label="signals_backtest_results\\nB:GCS", **PL, constraint="false")

    # API -> UI (HTTP/SSE)
    g.edge("ERA", "TAUI", label="fills SSE", **H)
    g.edge("ERA", "EXANI", label="exec results", **H)
    g.edge("ERA", "SETU", **H)
    g.edge("MDA", "EXANI", label="mktdata", **H, constraint="false")
    g.edge("MDA", "MLUI", label="feature/candle plots", **H, constraint="false")
    g.edge("STRAPI", "STUI", label="backtest results", **H, style="dashed")
    g.edge("CRS", "CRUI", label="reports", **H)
    g.edge("DEPAPI", "DEPUI", **H)
    g.edge("DEPAPI", "LHMU", label="health SSE", **H)
    g.edge("DEPAPI", "BAUI", **H)
    g.edge("DEPAPI", "LGUI", **H)
    g.edge("DEPAPI", "OBUI", **H)
    g.edge("DEPAPI", "MLUI", label="deploy hook", **H)
    g.edge("DEPAPI", "STUI", label="deploy hook", **H)

    # Infra
    g.edge(
        "DEPAPI",
        "SIT",
        label="post-deploy trigger",
        style="dashed",
        color="#64748b",
        constraint="false",
    )

    # Exceptional Redis sinks (not shown via badge; Redis is not all-services)
    g.edge("EXEC", "REDIS", label="hot order state", **REDIS)
    g.edge("MDPS", "REDIS", label="~1yr candle cache", **REDIS)

    # Reference panel positioning (invisible edges to sink rank)
    with g.subgraph(name="cluster_panels") as p:
        p.attr(
            label="Architecture Reference",
            style="rounded,dashed",
            color="#94a3b8",
            bgcolor="#f9fafb",
            rank="sink",
        )
        p.node("LEGEND")
        p.node("RECOVERY")
        p.node("CIRCUIT")
        p.node("PPOLICY")
        p.node("DEPLOYSUM")

    # Invisible edges to push panels to bottom
    g.edge("ALT", "LEGEND", style="invis", constraint="false")
    g.edge("CRUI", "RECOVERY", style="invis", constraint="false")
    g.edge("REDIS", "CIRCUIT", style="invis", constraint="false")
    g.edge("GCS", "PPOLICY", style="invis", constraint="false")
    g.edge("PS", "DEPLOYSUM", style="invis", constraint="false")
