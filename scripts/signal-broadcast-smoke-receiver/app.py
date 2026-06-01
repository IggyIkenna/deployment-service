"""Minimal signal-broadcast smoke receiver.

Accepts POSTs at `/cp/{cp_id}` and logs the signed payload so the live-staging
smoke can grep Cloud Run logs for proof that strategy-service's emitter
reached the receiver with an HMAC signature + idempotency key header.

Throwaway — deployed once, logs once, delete after. Not a production service.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("signal-broadcast-smoke-receiver")

app = FastAPI(title="Signal-Broadcast Smoke Receiver")


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    return {"ok": True}


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "service": "signal-broadcast-smoke-receiver",
        "purpose": "Logs POSTs at /cp/{cp_id} with HMAC + idempotency headers.",
    }


@app.post("/cp/{cp_id}")
async def receive_signal(cp_id: str, request: Request) -> JSONResponse:
    body_bytes = await request.body()
    headers = dict(request.headers)
    auth_header = headers.get("authorization", "")
    idempotency_key = headers.get("idempotency-key", "")
    content_type = headers.get("content-type", "")

    # stdout print is what `gcloud run services logs read` surfaces cleanly.
    print(
        f"[smoke-receiver] cp={cp_id} "
        f"idempotency_key={idempotency_key} "
        f"auth_prefix={auth_header[:24]} "
        f"content_type={content_type} "
        f"body_len={len(body_bytes)}",
        flush=True,
    )
    logger.info(
        "signal-received cp=%s idempotency_key=%s body_len=%d",
        cp_id,
        idempotency_key,
        len(body_bytes),
    )
    return JSONResponse({"ack": True, "cp": cp_id, "idempotency_key": idempotency_key})
