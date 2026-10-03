from __future__ import annotations

import sqlite3
from typing import Any

from . import db
from .errors import DomainError


def _reservation_or_404(
    conn: sqlite3.Connection, reservation_id: int
) -> sqlite3.Row:
    row = db.get_reservation(conn, reservation_id)
    if row is None:
        raise DomainError(404, "reservation not found")
    return row


def _validate_room(conn: sqlite3.Connection, room_id: int) -> None:
    if room_id < 1 or room_id > db.get_meta(conn, "room_count"):
        raise DomainError(400, "room_id is outside the configured room range")


def _next_enqueue_seq(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(enqueue_seq), 0) + 1 AS next_seq FROM reservations"
    ).fetchone()
    return int(row["next_seq"])


def apply_reservation(
    conn: sqlite3.Connection, now: int, room_id: int, start_time: int, end_time: int
) -> tuple[int, dict[str, Any]]:
    _validate_room(conn, room_id)

    blocked = db.has_overlap(conn, room_id, start_time, end_time)
    status = "waiting" if blocked else "held"
    expires_at = None if blocked else now + db.get_meta(conn, "hold_ttl")
    enqueue_seq = _next_enqueue_seq(conn)
    cursor = conn.execute(
        """
        INSERT INTO reservations (
            room_id, start_time, end_time, status, enqueue_seq,
            expires_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            room_id,
            start_time,
            end_time,
            status,
            enqueue_seq,
            expires_at,
            now,
            now,
        ),
    )
    reservation = db.serialize_reservation(
        conn, _reservation_or_404(conn, int(cursor.lastrowid))
    )
    return (202 if blocked else 201), {"reservation": reservation}


def confirm_reservation(
    conn: sqlite3.Connection, now: int, reservation_id: int
) -> tuple[int, dict[str, Any]]:
    row = _reservation_or_404(conn, reservation_id)
    if row["status"] == "confirmed":
        raise DomainError(409, "reservation is already confirmed")
    if row["status"] != "held":
        raise DomainError(
            409,
            f"only a held reservation can be confirmed, current status is {row['status']}",
        )

    conn.execute(
        """
        UPDATE reservations
        SET status = 'confirmed', expires_at = NULL, updated_at = ?
        WHERE id = ? AND status = 'held'
        """,
        (now, reservation_id),
    )
    reservation = db.serialize_reservation(
        conn, _reservation_or_404(conn, reservation_id)
    )
    return 200, {"reservation": reservation}


def cancel_reservation(
    conn: sqlite3.Connection, now: int, reservation_id: int
) -> tuple[int, dict[str, Any]]:
    row = _reservation_or_404(conn, reservation_id)
    if row["status"] not in ("held", "confirmed", "waiting"):
        raise DomainError(
            409,
            f"reservation with status {row['status']} cannot be canceled",
        )

    released_capacity = row["status"] in ("held", "confirmed")
    conn.execute(
        """
        UPDATE reservations
        SET status = 'canceled', expires_at = NULL, updated_at = ?
        WHERE id = ? AND status IN ('held', 'confirmed', 'waiting')
        """,
        (now, reservation_id),
    )
    reservation = db.serialize_reservation(
        conn, _reservation_or_404(conn, reservation_id)
    )
    promotions = db.promote_waiting(conn, now) if released_capacity else []
    return 200, {"reservation": reservation, "promotions": promotions}


def shorten_reservation(
    conn: sqlite3.Connection,
    now: int,
    reservation_id: int,
    start_time: int,
    end_time: int,
) -> tuple[int, dict[str, Any]]:
    row = _reservation_or_404(conn, reservation_id)
    if row["status"] not in ("held", "confirmed"):
        raise DomainError(
            409,
            f"only an active reservation can be shortened, current status is {row['status']}",
        )
    if start_time < row["start_time"] or end_time > row["end_time"]:
        raise DomainError(400, "the shortened interval must be inside the original interval")
    if start_time >= end_time:
        raise DomainError(400, "start_time must be before end_time")
    if start_time == row["start_time"] and end_time == row["end_time"]:
        raise DomainError(400, "the shortened interval must release at least one time unit")

    conn.execute(
        """
        UPDATE reservations
        SET start_time = ?, end_time = ?, updated_at = ?
        WHERE id = ? AND status IN ('held', 'confirmed')
        """,
        (start_time, end_time, now, reservation_id),
    )
    reservation = db.serialize_reservation(
        conn, _reservation_or_404(conn, reservation_id)
    )
    promotions = db.promote_waiting(conn, now)
    return 200, {"reservation": reservation, "promotions": promotions}


def advance_clock(
    conn: sqlite3.Connection, now: int, target_time: int
) -> tuple[int, dict[str, Any]]:
    if target_time < now:
        raise DomainError(400, "the clock can only move forward")
    result = db.advance_clock(conn, target_time)
    return 200, result
