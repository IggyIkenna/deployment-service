"""Generate RUNTIME_DEPLOYMENT_TOPOLOGY_DAG.svg using Graphviz.

Run: python3 generate_topology_svg.py
Requires: graphviz (brew install graphviz), pip install graphviz

Design principles (from RUNTIME_TOPOLOGY_DECISIONS.md):
- 4-line enriched node labels: name+deploy_badge, trigger, sharding dims, sinks badge
- NO dotted sink edges (GCS/PubSub sink shown as badge on node instead)
- Edge labels carry domain data name + protocol shorthand (B:GCS | L:PS)
- Reference panels (record nodes): legend, recovery, circuit breakers, publish+persist
- Tooltips: full PubSub topic templates + recovery methods per node
- Deployment badges: [VM] [CR Svc] [CR Job]
- API auth: all APIs note [OAuth]

Sub-modules:
    _topology_nodes   — node definitions for all layers, APIs, UIs, infra, storage
    _topology_panels  — architecture reference panel nodes
    _topology_edges   — pipeline flow edges and panel positioning
"""

import logging

import graphviz

from ._topology_edges import _add_edges
from ._topology_nodes_upper import add_all_nodes
from ._topology_panels import _add_reference_panels

logger = logging.getLogger(__name__)

# Re-export colour palette and helpers so existing consumers of
# this module continue to work unchanged.
from ._topology_nodes import PLANNED_COLOR, C, panel_node, svc  # noqa: F401 — re-exports


def build() -> graphviz.Digraph:
    """Build and return the full topology Digraph."""
    g = graphviz.Digraph(
        "topology",
        format="svg",
        engine="dot",
        graph_attr={
            "rankdir": "TB",
            "fontname": "Arial",
            "fontsize": "10",
            "label": (
                "Runtime Deployment Topology DAG v10\\n"
                "7-layer pipeline | L1-L5 shared data plane | L6-L7 client-specific plane\\n"
                "Deploy: [VM]=co-located VM  [CR Svc]=Cloud Run Service  [CR Job]=Cloud Run Job\\n"
                "SSOT: RUNTIME_TOPOLOGY_DECISIONS.md | Machine-readable: runtime-topology.yaml"
            ),
            "labelloc": "t",
            "labeljust": "l",
            "pad": "0.4",
            "nodesep": "0.45",
            "ranksep": "0.65",
            "compound": "true",
            "newrank": "true",
            "bgcolor": "#fafbfc",
            "splines": "polyline",
        },
        node_attr={"fontname": "Arial", "fontsize": "8"},
        edge_attr={"fontname": "Arial", "fontsize": "7"},
    )

    add_all_nodes(g)
    _add_reference_panels(g)
    _add_edges(g)

    return g


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import os

    outdir = os.path.dirname(os.path.abspath(__file__))
    g = build()
    outpath = os.path.join(outdir, "RUNTIME_DEPLOYMENT_TOPOLOGY_DAG")
    g.render(outpath, cleanup=True)
    logger.info("Generated: %s.svg", outpath)
