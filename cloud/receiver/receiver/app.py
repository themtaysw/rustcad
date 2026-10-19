import logging
import os
import secrets
from contextlib import asynccontextmanager
from importlib.resources import files
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Path, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from psycopg_pool import ConnectionPool

from receiver.models import Batch, DeviceId, IngestResult, Latest, LatestValue

logger = logging.getLogger("receiver")

INSERT_TELEMETRY = """
INSERT INTO telemetry (device_id, outbox_id, seq, ts, signal, value)
SELECT %(device_id)s::text, %(outbox_id)s::text, *
FROM unnest(%(seq)s::bigint[], %(ts)s::timestamptz[], %(signal)s::text[], %(value)s::float8[])
ON CONFLICT (device_id, outbox_id, seq) DO NOTHING
"""

INSERT_BATCH = """
INSERT INTO batches (
    device_id, outbox_id, batch_id, record_count, inserted_count, dropped_records
)
VALUES (
    %(device_id)s, %(outbox_id)s, %(batch_id)s,
    %(record_count)s, %(inserted_count)s, %(dropped_records)s
)
ON CONFLICT (device_id, outbox_id, batch_id) DO NOTHING
"""

SELECT_LATEST = """
SELECT DISTINCT ON (signal) signal, value, ts, seq
FROM telemetry
WHERE device_id = %s
ORDER BY signal, ts DESC, seq DESC
"""


def create_app(database_url: str | None = None, token: str | None = None) -> FastAPI:
    database_url = database_url or os.environ["DATABASE_URL"]
    token = token or os.environ["RECEIVER_TOKEN"]
    if not token.strip():
        raise RuntimeError("RECEIVER_TOKEN must not be empty")
    pool = ConnectionPool(database_url, open=False)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        pool.open(wait=True, timeout=30)
        with pool.connection() as conn:
            conn.execute(files("receiver").joinpath("schema.sql").read_text())
        yield
        pool.close()

    app = FastAPI(title="telemetry receiver", lifespan=lifespan)

    # The default handler echoes the rejected input back, which fails on NaN and
    # would send whole 10k-record batches back to the device.
    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, exc: RequestValidationError):
        errors = [{"loc": e["loc"], "msg": e["msg"], "type": e["type"]} for e in exc.errors()]
        return JSONResponse({"detail": errors}, status.HTTP_422_UNPROCESSABLE_CONTENT)

    bearer = HTTPBearer(auto_error=False)

    def require_token(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> None:
        if credentials is None or not secrets.compare_digest(
            credentials.credentials.encode(), token.encode()
        ):
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                "invalid or missing bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    @app.get("/health")
    def health():
        try:
            with pool.connection(timeout=2) as conn:
                conn.execute("SELECT 1")
        except Exception:
            logger.exception("database health check failed")
            return JSONResponse(
                {"status": "database unavailable"}, status.HTTP_503_SERVICE_UNAVAILABLE
            )
        return {"status": "ok"}

    @app.post("/ingest", dependencies=[Depends(require_token)])
    def ingest(batch: Batch) -> IngestResult:
        records = batch.records
        with pool.connection() as conn:
            inserted = conn.execute(
                INSERT_TELEMETRY,
                {
                    "device_id": batch.device_id,
                    "outbox_id": batch.outbox_id,
                    "seq": [r.seq for r in records],
                    "ts": [r.ts for r in records],
                    "signal": [r.signal for r in records],
                    "value": [r.value for r in records],
                },
            ).rowcount
            conn.execute(
                INSERT_BATCH,
                {
                    "device_id": batch.device_id,
                    "outbox_id": batch.outbox_id,
                    "batch_id": batch.batch_id,
                    "record_count": len(records),
                    "inserted_count": inserted,
                    "dropped_records": batch.dropped_records,
                },
            )

        if batch.dropped_records:
            logger.warning(
                "device %s reports %d dropped records", batch.device_id, batch.dropped_records
            )
        return IngestResult(
            accepted=len(records), inserted=inserted, duplicates=len(records) - inserted
        )

    @app.get("/devices/{device_id}/latest", dependencies=[Depends(require_token)])
    def latest(device_id: Annotated[DeviceId, Path()]) -> Latest:
        with pool.connection() as conn:
            rows = conn.execute(SELECT_LATEST, (device_id,)).fetchall()
        if not rows:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"no telemetry for device {device_id}")
        return Latest(
            device_id=device_id,
            signals={
                signal: LatestValue(value=value, ts=ts, seq=seq) for signal, value, ts, seq in rows
            },
        )

    return app
