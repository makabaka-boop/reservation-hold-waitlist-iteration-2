from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from typing import Any


SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id INTEGER NOT NULL,
    start_time INTEGER NOT NULL,
    end_time INTEGER NOT NULL,
    status TEXT NOT NULL,
    enqueue_seq INTEGER NOT NULL,
    expires_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK (room_id >= 1),
    CHECK (start_time < end_time),
    CHECK (status IN ('held', 'confirmed', 'waiting', 'canceled', 'expired'))
);

CREATE INDEX IF NOT EXISTS idx_reservations_active
ON reservations(room_id, start_time, end_time)
WHERE status IN ('held', 'confirmed');

CREATE UNIQUE INDEX IF NOT EXISTS idx_reservations_waiting_seq
ON reservations(enqueue_seq)
WHERE status = 'waiting';

CREATE TABLE IF NOT EXISTS idempotency (
    key TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    body TEXT NOT NULL,
    created_at INTEGER NOT NULL
) WITHOUT ROWID;
"""

ALL_STATUSES = ("held", "confirmed", "waiting", "canceled", "expired")


def default_db_path() -> str:
    return os.environ.get("DB_PATH", str(Path.cwd() / "reservations.db"))


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def initialize(db_path: str, room_count: int, hold_ttl: int) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with closing(connect(db_path)) as conn:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('clock', 0)"
        )
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('room_count', ?)",
            (room_count,),
        )
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('hold_ttl', ?)",
            (hold_ttl,),
        )


def get_meta(conn: sqlite3.Connection, key: str) -> int:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    if row is None:
        raise RuntimeError(f"missing meta value: {key}")
    return int(row["value"])


def set_meta(conn: sqlite3.Connection, key: str, value: int) -> None:
    conn.execute("UPDATE meta SET value = ? WHERE key = ?", (value, key))


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def fingerprint(method: str, path: str, payload: dict[str, Any]) -> str:
    raw = canonical_json({"method": method, "path": path, "payload": payload})
    return sha256(raw.encode("utf-8")).hexdigest()


def queue_position(conn: sqlite3.Connection, reservation_id: int) -> int | None:
    row = conn.execute(
        """
        SELECT 1 + COUNT(*) AS position
        FROM reservations
        WHERE status = 'waiting' AND enqueue_seq < (
            SELECT enqueue_seq FROM reservations WHERE id = ?
        )
        """,
        (reservation_id,),
    ).fetchone()
    return int(row["position"]) if row is not None else None


def serialize_reservation(
    conn: sqlite3.Connection, row: sqlite3.Row
) -> dict[str, Any]:
    result = dict(row)
    result["queue_position"] = (
        queue_position(conn, result["id"]) if result["status"] == "waiting" else None
    )
    return result


def get_reservation(
    conn: sqlite3.Connection, reservation_id: int
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT * FROM reservations WHERE id = ?",
        (reservation_id,),
    ).fetchone()


def has_overlap(
    conn: sqlite3.Connection,
    room_id: int,
    start_time: int,
    end_time: int,
    *,
    exclude_id: int | None = None,
) -> bool:
    sql = """
        SELECT 1
        FROM reservations
        WHERE room_id = ?
          AND status IN ('held', 'confirmed')
          AND start_time < ?
          AND end_time > ?
    """
    params: list[Any] = [room_id, end_time, start_time]
    if exclude_id is not None:
        sql += " AND id != ?"
        params.append(exclude_id)
    sql += " LIMIT 1"
    return conn.execute(sql, params).fetchone() is not None


def promote_waiting(
    conn: sqlite3.Connection, now: int, *, at: int | None = None
) -> list[dict[str, Any]]:
    """Perform one FIFO scan and promote every request that fully fits.

    Waiting rows are considered once in enqueue order. An earlier request that
    does not fit does not block a later, independent request, and no request is
    split around an existing occupation.
    """
    hold_ttl = get_meta(conn, "hold_ttl")
    event_time = now if at is None else at
    promotions: list[dict[str, Any]] = []
    waiting = conn.execute(
        """
        SELECT id, room_id, start_time, end_time, enqueue_seq
        FROM reservations
        WHERE status = 'waiting'
        ORDER BY enqueue_seq
        """
    ).fetchall()

    for candidate in waiting:
        if has_overlap(
            conn,
            candidate["room_id"],
            candidate["start_time"],
            candidate["end_time"],
            exclude_id=candidate["id"],
        ):
            continue
        conn.execute(
            """
            UPDATE reservations
            SET status = 'held',
                expires_at = ?,
                updated_at = ?
            WHERE id = ? AND status = 'waiting'
            """,
            (now + hold_ttl, now, candidate["id"]),
        )
        row = get_reservation(conn, candidate["id"])
        if row is not None:
            promotions.append({"type": "promoted", "at": event_time, **serialize_reservation(conn, row)})
    return promotions


def expire_holds(
    conn: sqlite3.Connection, now: int, *, at: int | None = None
) -> list[dict[str, Any]]:
    event_time = now if at is None else at
    rows = conn.execute(
        """
        SELECT *
        FROM reservations
        WHERE status = 'held' AND expires_at <= ?
        ORDER BY id
        """,
        (now,),
    ).fetchall()
    events: list[dict[str, Any]] = []
    for row in rows:
        conn.execute(
            """
            UPDATE reservations
            SET status = 'expired',
                expires_at = NULL,
                updated_at = ?
            WHERE id = ? AND status = 'held'
            """,
            (now, row["id"]),
        )
        events.append({"type": "expired", "at": event_time, "id": row["id"]})
    return events


def next_hold_expiry(conn: sqlite3.Connection, after: int) -> int | None:
    row = conn.execute(
        """
        SELECT MIN(expires_at) AS next_expiry
        FROM reservations
        WHERE status = 'held' AND expires_at > ?
        """,
        (after,),
    ).fetchone()
    value = row["next_expiry"] if row is not None else None
    return None if value is None else int(value)


def advance_clock(conn: sqlite3.Connection, target: int) -> dict[str, Any]:
    start = get_meta(conn, "clock")
    now = start
    events: list[dict[str, Any]] = []

    while now < target:
        expiry = next_hold_expiry(conn, now)
        if expiry is None or expiry > target:
            now = target
        else:
            now = expiry
        set_meta(conn, "clock", now)
        events.extend(expire_holds(conn, now, at=now))
        events.extend(promote_waiting(conn, now, at=now))

    return {"from_time": start, "to_time": now, "events": events}


def stored_idempotent(
    conn: sqlite3.Connection, key: str
) -> tuple[int, dict[str, Any]] | None:
    row = conn.execute(
        "SELECT status_code, body FROM idempotency WHERE key = ?", (key,)
    ).fetchone()
    if row is None:
        return None
    return int(row["status_code"]), json.loads(row["body"])


def save_idempotent(
    conn: sqlite3.Connection,
    key: str,
    request_fingerprint: str,
    status_code: int,
    body: dict[str, Any],
    now: int,
) -> None:
    conn.execute(
        """
        INSERT INTO idempotency(key, fingerprint, status_code, body, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (key, request_fingerprint, status_code, json.dumps(body, ensure_ascii=False), now),
    )


def list_reservations(
    conn: sqlite3.Connection,
    *,
    status: str | None = None,
    room_id: int | None = None,
) -> list[sqlite3.Row]:
    sql = "SELECT * FROM reservations WHERE 1=1"
    params: list[Any] = []
    if status is not None:
        sql += " AND status = ?"
        params.append(status)
    if room_id is not None:
        sql += " AND room_id = ?"
        params.append(room_id)
    sql += " ORDER BY id"
    return conn.execute(sql, params).fetchall()
