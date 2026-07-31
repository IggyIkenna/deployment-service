#!/usr/bin/env python3
# Epic: batch_live_symmetry_master
# Lifecycle: one-off remediation verifier — DELETE after
#   live_event_log_warm_sink_recovery_and_cold_compaction_2026_07_31.md archives.
# Delete-when: plans/active/live_event_log_warm_sink_recovery_and_cold_compaction_2026_07_31.md archived.
"""Diff every live warm-sink-persist-* Pub/Sub subscription's cloud_storage_config
(bucket / filename_prefix / filename_suffix) against the value declared for that
same resource in terraform/gcp/live_event_log/warm_sink.tf.

Confirms each subscription's filename_prefix 1:1-encodes its own topic's
asset_group x data_type label pairing (so no sink can silently write to another
shard's path) and that no two subscriptions declare the same filename_prefix.

Usage:
    python3 scripts/verify_warm_sink_subscription_paths.py [--project central-element-323112]
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

TF_PATH = Path(__file__).resolve().parent.parent / "terraform" / "gcp" / "live_event_log" / "warm_sink.tf"

# var.warm_gcs_bucket — no tfvars file exists for this module; this is the value
# recovered from the live-event-log-compactor Cloud Run job's env at apply time
# (see this script's plan Progress Log, todo 2).
EXPECTED_BUCKET = "central-element-323112-events"

RESOURCE_RE = re.compile(
    r'resource "google_pubsub_subscription" "(?P<resource_name>\w+)" \{(?P<body>.*?)\n\}\n',
    re.DOTALL,
)
NAME_RE = re.compile(r'name\s+=\s+"(?P<name>[^"]+)"')
PREFIX_RE = re.compile(r'filename_prefix\s+=\s+"(?P<prefix>[^"]+)"')
SUFFIX_RE = re.compile(r'filename_suffix\s+=\s+"(?P<suffix>[^"]+)"')
ASSET_GROUP_RE = re.compile(r'asset_group\s+=\s+"(?P<val>[^"]+)"')
DATA_TYPE_RE = re.compile(r'data_type\s+=\s+"(?P<val>[^"]+)"')


def parse_declared(tf_text: str) -> dict[str, dict[str, str]]:
    declared: dict[str, dict[str, str]] = {}
    for match in RESOURCE_RE.finditer(tf_text):
        body = match.group("body")
        name_match = NAME_RE.search(body)
        prefix_match = PREFIX_RE.search(body)
        suffix_match = SUFFIX_RE.search(body)
        asset_group_match = ASSET_GROUP_RE.search(body)
        data_type_match = DATA_TYPE_RE.search(body)
        if not (name_match and prefix_match and suffix_match and asset_group_match and data_type_match):
            raise ValueError(f"resource {match.group('resource_name')!r} is missing a required field")
        declared[name_match.group("name")] = {
            "filename_prefix": prefix_match.group("prefix"),
            "filename_suffix": suffix_match.group("suffix"),
            "asset_group": asset_group_match.group("val"),
            "data_type": data_type_match.group("val"),
        }
    return declared


def fetch_live(project: str) -> dict[str, dict[str, str]]:
    result = subprocess.run(
        [
            "gcloud",
            "pubsub",
            "subscriptions",
            "list",
            "--filter=name:warm-sink",
            f"--project={project}",
            "--format=json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    subs = json.loads(result.stdout)
    live: dict[str, dict[str, str]] = {}
    for sub in subs:
        short_name = sub["name"].rsplit("/", 1)[-1]
        csc = sub.get("cloudStorageConfig", {})
        live[short_name] = {
            # empty string is a meaningful "field absent from the live API response" sentinel here —
            # the diff() below correctly reports it as a mismatch against the (always non-empty)
            # declared tf value, never silently treated as a match.
            "bucket": csc.get("bucket", ""),  # noqa: qg-empty-fallback
            "filename_prefix": csc.get("filenamePrefix", ""),  # noqa: qg-empty-fallback
            "filename_suffix": csc.get("filenameSuffix", ""),  # noqa: qg-empty-fallback
        }
    return live


def diff(declared: dict[str, dict[str, str]], live: dict[str, dict[str, str]]) -> list[str]:
    mismatches: list[str] = []

    missing_live = sorted(set(declared) - set(live))
    extra_live = sorted(set(live) - set(declared))
    if missing_live:
        mismatches.append(f"declared but not live ({len(missing_live)}): {missing_live}")
    if extra_live:
        mismatches.append(f"live but not declared ({len(extra_live)}): {extra_live}")

    seen_prefixes: dict[str, str] = {}
    for name in sorted(set(declared) & set(live)):
        d = declared[name]
        live_sub = live[name]

        if live_sub["bucket"] != EXPECTED_BUCKET:
            mismatches.append(f"{name}: bucket live={live_sub['bucket']!r} expected={EXPECTED_BUCKET!r}")
        if live_sub["filename_prefix"] != d["filename_prefix"]:
            mismatches.append(
                f"{name}: filename_prefix live={live_sub['filename_prefix']!r} declared={d['filename_prefix']!r}"
            )
        if live_sub["filename_suffix"] != d["filename_suffix"]:
            mismatches.append(
                f"{name}: filename_suffix live={live_sub['filename_suffix']!r} declared={d['filename_suffix']!r}"
            )

        # 1:1 pairing: this subscription's own asset_group/data_type must appear
        # in its own declared filename_prefix (warm sink layout is
        # live-events/warm/{asset_group}/{data_type with '-' -> '_'}/).
        expected_fragment = f"/{d['asset_group']}/{d['data_type'].replace('-', '_')}/"
        if expected_fragment not in d["filename_prefix"]:
            mismatches.append(
                f"{name}: declared filename_prefix {d['filename_prefix']!r} does not encode "
                f"asset_group={d['asset_group']!r} data_type={d['data_type']!r}"
            )

        # no two subscriptions may share a filename_prefix (would cross-write).
        if d["filename_prefix"] in seen_prefixes:
            mismatches.append(
                f"{name}: declared filename_prefix {d['filename_prefix']!r} collides with "
                f"{seen_prefixes[d['filename_prefix']]!r}"
            )
        else:
            seen_prefixes[d["filename_prefix"]] = name

    return mismatches


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="central-element-323112")
    args = parser.parse_args()

    declared = parse_declared(TF_PATH.read_text())
    live = fetch_live(args.project)
    mismatches = diff(declared, live)

    print(f"declared subscriptions: {len(declared)}")
    print(f"live subscriptions:     {len(live)}")
    print(f"mismatches:             {len(mismatches)}")
    for line in mismatches:
        print(f"  MISMATCH: {line}")

    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
