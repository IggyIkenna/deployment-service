"""Tests for runtime topology validator."""

import json

import yaml

from deployment_service.runtime_topology_validator import validate_runtime_topology


def test_runtime_topology_validation_passes_for_valid_minimum(tmp_path):
    runtime_topology = {
        "deployment_profiles": {
            "distributed": {"allowed_transports": ["gcs", "pubsub"]},
            "co_located_vm": {"allowed_transports": ["gcs", "pubsub", "in_memory"]},
        },
        "storage_systems": {"gcs": {}, "redis": {}, "bigquery": {}},
        "services": {"market-data-processing-service": {}},
        "service_flows": [
            {
                "producer": "market-tick-data-service",
                "consumer": "market-data-processing-service",
                "modes": {
                    "batch": {"transport": "gcs"},
                    "live": {"distributed": {"transport": "pubsub"}, "co_located_vm": {"transport": "in_memory"}},
                },
            }
        ],
        "api_interactions": [
            {
                "caller": "trading-analytics-ui",
                "callee": "execution-results-api",
                "protocol": "sse",
            }
        ],
        "storage_flows": [
            {"actor": "market-data-processing-service", "store": "gcs"},
        ],
    }

    workspace_manifest = {
        "repositories": {
            "market-tick-data-service": {"type": "service"},
            "market-data-processing-service": {"type": "service"},
            "execution-results-api": {"type": "api-service"},
            "trading-analytics-ui": {"type": "ui"},
        }
    }

    runtime_path = tmp_path / "runtime-topology.yaml"
    manifest_path = tmp_path / "workspace-manifest.json"
    runtime_path.write_text(yaml.safe_dump(runtime_topology))
    manifest_path.write_text(json.dumps(workspace_manifest))

    violations = validate_runtime_topology(runtime_path, manifest_path)
    assert violations == []


def test_runtime_topology_validation_fails_on_unknown_entities(tmp_path):
    runtime_topology = {
        "deployment_profiles": {"distributed": {"allowed_transports": ["gcs", "pubsub"]}},
        "storage_systems": {"gcs": {}},
        "services": {"unknown-service": {}},
        "service_flows": [],
        "api_interactions": [],
        "storage_flows": [],
    }
    workspace_manifest = {"repositories": {}}

    runtime_path = tmp_path / "runtime-topology.yaml"
    manifest_path = tmp_path / "workspace-manifest.json"
    runtime_path.write_text(yaml.safe_dump(runtime_topology))
    manifest_path.write_text(json.dumps(workspace_manifest))

    violations = validate_runtime_topology(runtime_path, manifest_path)
    assert any("unknown-service" in item for item in violations)
