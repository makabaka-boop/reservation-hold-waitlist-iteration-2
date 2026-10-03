from __future__ import annotations

import os
from contextlib import asynccontextmanager, closing
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from . import db
from .idempotency import run_idempotent
from .services import (
    advance_clock,
    apply_reservation,
    cancel_reservation,
    confirm_reservation,
    reschedule_reservation,
    shorten_reservation,
)


class ApplyRequest(BaseModel, extra="forbid"):
    room_id: int = Field(ge=1)
    start_time: int = Field(ge=0)
    end_time: int = Field(ge=1)

    @field_validator("end_time")
    @classmethod
    def valid_interval(cls, value: int, info: Any) -> int:
        start = info.data.get("start_time")
        if start is not None and start >= value:
            raise ValueError("start_time must be before end_time")
        return value


class ConfirmRequest(BaseModel, extra="forbid"):
    pass


class CancelRequest(BaseModel, extra="forbid"):
    pass


class ShortenRequest(BaseModel, extra="forbid"):
    start_time: int = Field(ge=0)
    end_time: int = Field(ge=1)

    @field_validator("end_time")
    @classmethod
    def valid_interval(cls, value: int, info: Any) -> int:
        start = info.data.get("start_time")
        if start is not None and start >= value:
            raise ValueError("start_time must be before end_time")
        return value


class ClockRequest(BaseModel, extra="forbid"):
    target_time: int = Field(ge=0)


class RescheduleRequest(BaseModel, extra="forbid"):
    original_room_id: int = Field(ge=1)
    original_start_time: int = Field(ge=0)
    original_end_time: int = Field(ge=1)
    target_room_id: int = Field(ge=1)
    target_start_time: int = Field(ge=0)
    target_end_time: int = Field(ge=1)

    @field_validator("original_end_time")
    @classmethod
    def original_interval_valid(cls, value: int, info: Any) -> int:
        start = info.data.get("original_start_time")
        if start is not None and start >= value:
            raise ValueError("original_start_time must be before original_end_time")
        return value

    @field_validator("target_end_time")
    @classmethod
    def target_interval_valid(cls, value: int, info: Any) -> int:
        start = info.data.get("target_start_time")
        if start is not None and start >= value:
            raise ValueError("target_start_time must be before target_end_time")
        return value


def _configured_int(name: str, default: int, minimum: int, maximum: int | None = None) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if value < minimum or (maximum is not None and value > maximum):
        if maximum is None:
            allowed = f">= {minimum}"
        else:
            allowed = f"between {minimum} and {maximum}"
        raise RuntimeError(f"{name} must be {allowed}")
    return value


def idempotency_key(
    idempotency_key_header: str | None = Header(default=None, alias="Idempotency-Key"),
) -> str:
    if not idempotency_key_header or not idempotency_key_header.strip():
        raise HTTPException(
            status_code=400, detail="Idempotency-Key header is required"
        )
    key = idempotency_key_header.strip()
    if len(key) > 128:
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key must contain at most 128 characters",
        )
    return key


def create_app(
    *,
    db_path: str | None = None,
    room_count: int | None = None,
    hold_ttl: int | None = None,
) -> FastAPI:
    configured_rooms = room_count if room_count is not None else _configured_int(
        "ROOM_COUNT", 10, 1, 10
    )
    configured_ttl = hold_ttl if hold_ttl is not None else _configured_int(
        "HOLD_TTL", 5, 1
    )
    resolved_db_path = db_path or db.default_db_path()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db.initialize(resolved_db_path, configured_rooms, configured_ttl)
        app.state.db_path = resolved_db_path
        app.state.room_count = configured_rooms
        app.state.hold_ttl = configured_ttl
        yield

    app = FastAPI(title="Integer interval room reservations", lifespan=lifespan)

    @app.get("/health")
    def health(request: Request) -> dict[str, Any]:
        with closing(db.connect(request.app.state.db_path)) as conn:
            return {
                "clock": db.get_meta(conn, "clock"),
                "room_count": db.get_meta(conn, "room_count"),
                "hold_ttl": db.get_meta(conn, "hold_ttl"),
            }

    @app.post("/reservations", status_code=201)
    def create_reservation(
        request: Request,
        payload: ApplyRequest,
        key: str = Depends(idempotency_key),
    ) -> JSONResponse:
        return run_idempotent(
            request,
            key,
            payload.model_dump(),
            lambda conn, now: apply_reservation(
                conn, now, payload.room_id, payload.start_time, payload.end_time
            ),
        )

    @app.get("/reservations")
    def get_reservations(
        request: Request,
        status: str | None = Query(default=None),
        room_id: int | None = Query(default=None, ge=1),
    ) -> JSONResponse:
        allowed = set(db.ALL_STATUSES)
        if status is not None and status not in allowed:
            return JSONResponse(
                status_code=400,
                content={"detail": f"status must be one of {sorted(allowed)}"},
            )
        with closing(db.connect(request.app.state.db_path)) as conn:
            if room_id is not None and room_id > db.get_meta(conn, "room_count"):
                return JSONResponse(
                    status_code=400,
                    content={"detail": "room_id is outside the configured room range"},
                )
            rows = db.list_reservations(conn, status=status, room_id=room_id)
            return JSONResponse(
                status_code=200,
                content={"reservations": [db.serialize_reservation(conn, row) for row in rows]},
            )

    @app.get("/reservations/{reservation_id}")
    def get_reservation(reservation_id: int, request: Request) -> JSONResponse:
        with closing(db.connect(request.app.state.db_path)) as conn:
            row = db.get_reservation(conn, reservation_id)
            if row is None:
                return JSONResponse(
                    status_code=404, content={"detail": "reservation not found"}
                )
            return JSONResponse(
                status_code=200, content=db.serialize_reservation(conn, row)
            )

    @app.post("/reservations/{reservation_id}/confirm")
    def post_confirm(
        reservation_id: int,
        request: Request,
        payload: ConfirmRequest,
        key: str = Depends(idempotency_key),
    ) -> JSONResponse:
        return run_idempotent(
            request,
            key,
            payload.model_dump(),
            lambda conn, now: confirm_reservation(conn, now, reservation_id),
        )

    @app.post("/reservations/{reservation_id}/cancel")
    def post_cancel(
        reservation_id: int,
        request: Request,
        payload: CancelRequest,
        key: str = Depends(idempotency_key),
    ) -> JSONResponse:
        return run_idempotent(
            request,
            key,
            payload.model_dump(),
            lambda conn, now: cancel_reservation(conn, now, reservation_id),
        )

    @app.post("/reservations/{reservation_id}/shorten")
    def post_shorten(
        reservation_id: int,
        request: Request,
        payload: ShortenRequest,
        key: str = Depends(idempotency_key),
    ) -> JSONResponse:
        return run_idempotent(
            request,
            key,
            payload.model_dump(),
            lambda conn, now: shorten_reservation(
                conn, now, reservation_id, payload.start_time, payload.end_time
            ),
        )

    @app.post("/reservations/{reservation_id}/reschedule")
    def post_reschedule(
        reservation_id: int,
        request: Request,
        payload: RescheduleRequest,
        key: str = Depends(idempotency_key),
    ) -> JSONResponse:
        return run_idempotent(
            request,
            key,
            payload.model_dump(),
            lambda conn, now: reschedule_reservation(
                conn,
                now,
                reservation_id,
                payload.original_room_id,
                payload.original_start_time,
                payload.original_end_time,
                payload.target_room_id,
                payload.target_start_time,
                payload.target_end_time,
            ),
        )

    @app.post("/clock/advance")
    def post_clock_advance(
        request: Request,
        payload: ClockRequest,
        key: str = Depends(idempotency_key),
    ) -> JSONResponse:
        return run_idempotent(
            request,
            key,
            payload.model_dump(),
            lambda conn, now: advance_clock(conn, now, payload.target_time),
        )

    return app


app = create_app()
