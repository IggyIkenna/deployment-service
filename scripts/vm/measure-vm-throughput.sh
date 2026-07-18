#!/usr/bin/env bash
# Epic: data-pipeline observability
# Lifecycle: PERMANENT — the canonical way to measure VM data throughput for ANY asset_group.
# Definitive VM throughput via the Cloud Monitoring network-RX counter.
# Works for ANY asset_group / VM without SSH (IAP is often unavailable).
# Usage: measure_vm_throughput.sh <vm-name> [zone] [project] [start-RFC3339]
set -uo pipefail
VM="${1:?usage: measure_vm_throughput.sh <vm-name> [zone] [project] [startTime]}"
ZONE="${2:-asia-northeast1-c}"
P="${3:-central-element-323112}"
IID=$(gcloud compute instances describe "$VM" --zone="$ZONE" --project="$P" --format='value(id)' 2>/dev/null)
[ -z "$IID" ] && { echo "VM $VM not found in $ZONE"; exit 1; }
START="${4:-$(gcloud compute instances describe "$VM" --zone="$ZONE" --project="$P" --format='value(creationTimestamp)' 2>/dev/null | python3 -c 'import sys,datetime;print(datetime.datetime.fromisoformat(sys.stdin.read().strip()).astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))')}"
END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
TOKEN=$(gcloud auth print-access-token 2>/dev/null)
F="metric.type=\"compute.googleapis.com/instance/network/received_bytes_count\" AND resource.labels.instance_id=\"$IID\""
echo "VM=$VM  window: $START -> $END"
curl -sG "https://monitoring.googleapis.com/v3/projects/$P/timeSeries" \
  -H "Authorization: Bearer $TOKEN" \
  --data-urlencode "filter=$F" \
  --data-urlencode "interval.startTime=$START" \
  --data-urlencode "interval.endTime=$END" \
  --data-urlencode "aggregation.alignmentPeriod=60s" \
  --data-urlencode "aggregation.perSeriesAligner=ALIGN_RATE" 2>/dev/null \
| python3 -c "
import json,sys
d=json.load(sys.stdin)
if 'error' in d: print('API error:', d['error'].get('message','')[:200]); raise SystemExit(1)
pts=sorted((p['interval']['endTime'], float(p['value'].get('doubleValue', p['value'].get('int64Value',0))))
           for s in d.get('timeSeries',[]) for p in s.get('points',[]))
if not pts: print('no data (VM too new? metric lags ~2-3min)'); raise SystemExit(1)
v=[x[1] for x in pts]
print(f'  minutes sampled : {len(pts)}')
print(f'  TOTAL received  : {sum(v)*60/1e9:.2f} GB')
print(f'  MEAN            : {sum(v)/len(v)/1e6:.2f} MB/s   <-- sustained download rate')
print(f'  PEAK minute     : {max(v)/1e6:.2f} MB/s')
print('  per-5min profile:')
for i in range(0,len(pts),5):
    c=v[i:i+5]; print(f'    {pts[i][0][11:16]}  {sum(c)/len(c)/1e6:6.2f} MB/s')
"
