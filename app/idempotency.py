from __future__ import annotations

import sqlite3
from typing import Any, Callable

from fastapi import Request
from fastapi.responses import JSONResponse

from . import db
from .errors import DomainError


def run_idempotent(
    request: Request,
    key: str,
    payload: dict[str, Any],
    operation: Callable[[sqlite3.Connection, int], tuple[int, dict[str, Any]]],
) -> JSONResponse:
    """Serialize a mutation and replay its stored response for key retries.

    ``BEGIN IMMEDIATE`` is acquired before checking either capacity or the key.
    Concurrent requests therefore serialize, and the loser of a race observes
    the winner's committed hold.
    """
    request_fingerprint = db.fingerprint(request.method, request.url.path, payload)
    conn = db.connect(request.app.state.db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        now = db.get_meta(conn, "clock")

        existing = conn.execute(
            "SELECT fingerprint, status_code, body FROM idempotency WHERE key = ?",
            (key,),
        ).fetchone()
        if existing is not None:
            if existing["fingerprint"] != request_fingerprint:
                conn.execute("ROLLBACK")
                return JSONResponse(
                    status_code=409,
                    content={
                        "detail": "Idempotency-Key was already used with a different payload"
                    },
                )
            conn.execute("COMMIT")
            return JSONResponse(
                status_code=int(existing["status_code"]),
                content=db.json.loads(existing["body"]),
            )

        try:
            status_code, body = operation(conn, now)
        except DomainError as exc:
            status_code = exc.status_code
            body = {"detail": exc.detail}

        db.save_idempotent(
            conn, key, request_fingerprint, status_code, body, now
        )
        conn.execute("COMMIT")
        return JSONResponse(status_code=status_code, content=body)
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
