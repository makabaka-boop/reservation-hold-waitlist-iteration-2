from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from threading import Barrier, Thread
from typing import Any
import uuid

import pytest
from fastapi.testclient import TestClient

from app import db as db_module
from app.main import create_app


@pytest.fixture
def client(tmp_path):
    db_path = str(tmp_path / "test.db")
    app = create_app(db_path=db_path, room_count=2, hold_ttl=5)
    with TestClient(app) as test_client:
        yield test_client, db_path


def headers(key: str | None = None) -> dict[str, str]:
    return {"Idempotency-Key": key or str(uuid.uuid4())}


def post_json(client: TestClient, path: str, payload: dict[str, Any], key: str | None = None):
    return client.post(path, json=payload, headers=headers(key))


@dataclass
class RefReservation:
    id: int
    room: int
    start: int
    end: int
    status: str = "held"
    expires_at: int | None = None


@dataclass
class ReferenceModel:
    ttl: int = 5
    now: int = 0
    seq: int = 0
    reservations: dict[int, RefReservation] = field(default_factory=dict)

    def overlaps_active(self, room: int, start: int, end: int, exclude: int | None = None) -> bool:
        return any(
            r.room == room
            and r.status in ("held", "confirmed")
            and r.start < end
            and r.end > start
            and r.id != exclude
            for r in self.reservations.values()
        )

    def apply(self, room: int, start: int, end: int) -> tuple[int, str, int | None]:
        self.seq += 1
        rid = self.seq
        blocked = self.overlaps_active(room, start, end)
        status = "waiting" if blocked else "held"
        expires = None if blocked else self.now + self.ttl
        self.reservations[rid] = RefReservation(rid, room, start, end, status, expires)
        return (202 if blocked else 201), status, rid

    def confirm(self, rid: int) -> int:
        r = self.reservations[rid]
        if r.status != "held":
            return 409
        r.status = "confirmed"
        r.expires_at = None
        return 200

    def cancel(self, rid: int) -> int:
        r = self.reservations[rid]
        if r.status not in ("held", "confirmed", "waiting"):
            return 409
        released = r.status in ("held", "confirmed")
        r.status = "canceled"
        r.expires_at = None
        if released:
            self.promote_scan()
        return 200

    def shorten(self, rid: int, start: int, end: int) -> int:
        r = self.reservations[rid]
        if r.status not in ("held", "confirmed"):
            return 409
        if start < r.start or end > r.end or start >= end or (start == r.start and end == r.end):
            return 400
        r.start, r.end = start, end
        self.promote_scan()
        return 200

    def promote_scan(self) -> None:
        waiting = sorted(
            (r for r in self.reservations.values() if r.status == "waiting"),
            key=lambda r: r.id,
        )
        for candidate in waiting:
            if not self.overlaps_active(
                candidate.room, candidate.start, candidate.end, exclude=candidate.id
            ):
                candidate.status = "held"
                candidate.expires_at = self.now + self.ttl

    def advance_to(self, target: int) -> set[str]:
        changed = False
        while self.now < target:
            expiries = [
                r.expires_at
                for r in self.reservations.values()
                if r.status == "held" and self.now < r.expires_at <= target
            ]
            if not expiries:
                self.now = target
                break
            self.now = min(expiries)
            for r in self.reservations.values():
                if r.status == "held" and r.expires_at <= self.now:
                    r.status = "expired"
                    r.expires_at = None
                    changed = True
            before = {rid: r.status for rid, r in self.reservations.items()}
            self.promote_scan()
            if {rid: r.status for rid, r in self.reservations.items()} != before:
                changed = True
        return set()  # Caller compares all resulting states.

    def expected(self, rid: int) -> dict[str, Any]:
        r = self.reservations[rid]
        waiting_ids = sorted(
            x.id for x in self.reservations.values() if x.status == "waiting"
        )
        position = waiting_ids.index(rid) + 1 if r.status == "waiting" else None
        return {
            "room_id": r.room,
            "start_time": r.start,
            "end_time": r.end,
            "status": r.status,
            "expires_at": r.expires_at,
            "queue_position": position,
        }


def assert_matches_ref(client: TestClient, ref: ReferenceModel, rid: int) -> None:
    data = client.get(f"/reservations/{rid}").json()
    expected = ref.expected(rid)
    for key, value in expected.items():
        assert data[key] == value


def test_reference_model_overlap_expiry_promotion_and_shortening(client):
    http, _ = client
    ref = ReferenceModel(ttl=5)

    # [10, 20) becomes the blocker.
    _, _, blocker = ref.apply(1, 10, 20)
    response = post_json(http, "/reservations", {"room_id": 1, "start_time": 10, "end_time": 20})
    assert response.status_code == 201
    assert response.json()["reservation"]["id"] == blocker
    assert ref.confirm(blocker) == 200
    assert post_json(http, f"/reservations/{blocker}/confirm", {}).status_code == 200

    cases = [
        (1, 10, 20),  # full overlap -> position 1
        (1, 15, 25),  # overlap -> position 2
        (1, 20, 30),  # adjacent half-open interval is free
        (2, 10, 20),  # another room is free
        (1, 0, 10),   # adjacent on the left is free
        (1, 18, 30),  # waits behind the two overlapping requests
        (1, 16, 18),  # fits only after the blocker releases [16,18)
    ]
    ids = []
    for room, start, end in cases:
        expected_status_code, expected_status, rid = ref.apply(room, start, end)
        response = post_json(http, "/reservations", {"room_id": room, "start_time": start, "end_time": end})
        assert response.status_code == expected_status_code
        assert response.json()["reservation"]["status"] == expected_status
        assert response.json()["reservation"]["id"] == rid
        ids.append(rid)

    full, partial, adjacent, other_room, left_adjacent, tail, inner = ids
    assert_matches_ref(http, ref, full)
    assert_matches_ref(http, ref, partial)
    assert_matches_ref(http, ref, adjacent)
    assert_matches_ref(http, ref, other_room)
    assert_matches_ref(http, ref, left_adjacent)
    assert_matches_ref(http, ref, tail)
    assert_matches_ref(http, ref, inner)

    # Advancing below TTL does not expire the held blocker.
    ref.advance_to(4)
    response = post_json(http, "/clock/advance", {"target_time": 4})
    assert response.status_code == 200
    assert_matches_ref(http, ref, blocker)

    # At time 5, the two independent [20,30) / other-room holds expire but the
    # confirmed blocker remains; nothing waiting fits.
    ref.advance_to(5)
    post_json(http, "/clock/advance", {"target_time": 5})
    assert_matches_ref(http, ref, blocker)
    assert_matches_ref(http, ref, full)
    assert_matches_ref(http, ref, partial)

    # Shortening the confirmed blocker to [16,20) releases [10,16). The earlier
    # full request still does not fit, but [15,25) also does not fit: it needs
    # [20,25) as one uninterrupted interval and must not be split into parts.
    assert ref.shorten(blocker, 16, 20) == 200
    response = post_json(
        http, f"/reservations/{blocker}/shorten", {"start_time": 16, "end_time": 20}
    )
    assert response.status_code == 200
    assert response.json()["promotions"] == []
    assert_matches_ref(http, ref, full)
    assert_matches_ref(http, ref, partial)

    # Shortening to [18,20) releases exactly [16,18). The large requests still
    # do not fit; only the complete [16,18) waitlist request is promoted.
    assert ref.shorten(blocker, 18, 20) == 200
    response = post_json(
        http, f"/reservations/{blocker}/shorten", {"start_time": 18, "end_time": 20}
    )
    assert response.status_code == 200
    assert [item["id"] for item in response.json()["promotions"]] == [inner]
    assert_matches_ref(http, ref, full)
    assert_matches_ref(http, ref, partial)
    assert_matches_ref(http, ref, inner)

    # Cancel the remaining [18,20) blocker. The earlier full/partial requests
    # overlap the just-promoted [16,18) hold and are skipped; [18,30) fits as a
    # complete interval and is promoted despite queueing later.
    assert ref.cancel(blocker) == 200
    response = post_json(http, f"/reservations/{blocker}/cancel", {})
    assert response.status_code == 200
    assert [item["id"] for item in response.json()["promotions"]] == [tail]
    assert_matches_ref(http, ref, blocker)
    assert_matches_ref(http, ref, full)
    assert_matches_ref(http, ref, partial)
    assert_matches_ref(http, ref, tail)

    # Releasing [16,18) cannot help full/partial because tail holds [18,30);
    # tail being later in queue does not prevent those intervals from staying
    # waiting. Cancel tail too, then the head is promoted in FIFO order.
    assert ref.cancel(inner) == 200
    response = post_json(http, f"/reservations/{inner}/cancel", {})
    assert response.status_code == 200
    assert response.json()["promotions"] == []
    assert_matches_ref(http, ref, inner)
    assert_matches_ref(http, ref, full)
    assert_matches_ref(http, ref, partial)

    assert ref.cancel(tail) == 200
    response = post_json(http, f"/reservations/{tail}/cancel", {})
    assert response.status_code == 200
    assert [item["id"] for item in response.json()["promotions"]] == [full]
    assert_matches_ref(http, ref, tail)
    assert_matches_ref(http, ref, full)
    assert_matches_ref(http, ref, partial)


def test_concurrent_conflicting_applications_only_one_holds(tmp_path):
    db_path = str(tmp_path / "concurrent.db")
    app = create_app(db_path=db_path, room_count=1, hold_ttl=10)
    barrier = Barrier(8)

    def request(statuses: list[int], bodies: list[dict[str, Any]]) -> None:
        local = TestClient(app)
        barrier.wait()
        response = local.post(
            "/reservations",
            json={"room_id": 1, "start_time": 100, "end_time": 200},
            headers={"Idempotency-Key": str(uuid.uuid4())},
        )
        statuses.append(response.status_code)
        bodies.append(response.json())

    with TestClient(app):
        statuses: list[int] = []
        bodies: list[dict[str, Any]] = []
        threads = [Thread(target=request, args=(statuses, bodies)) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert sorted(statuses) == [201] + [202] * 7
    with TestClient(app) as restarted:
        held = restarted.get("/reservations", params={"status": "held"}).json()["reservations"]
        waiting = restarted.get("/reservations", params={"status": "waiting"}).json()["reservations"]
    assert len(held) == 1
    assert len(waiting) == 7
    assert [item["enqueue_seq"] for item in waiting] == list(range(2, 9))
    assert [item["queue_position"] for item in waiting] == list(range(1, 8))


def test_idempotency_retry_and_payload_change_rejection(client):
    http, _ = client
    key = str(uuid.uuid4())
    payload = {"room_id": 1, "start_time": 50, "end_time": 60}

    first = http.post("/reservations", json=payload, headers={"Idempotency-Key": key})
    second = http.post("/reservations", json=payload, headers={"Idempotency-Key": key})
    changed = http.post(
        "/reservations",
        json={"room_id": 1, "start_time": 50, "end_time": 61},
        headers={"Idempotency-Key": key},
    )

    assert first.status_code == 201
    assert second.status_code == 201
    assert second.json() == first.json()
    assert changed.status_code == 409

    # An idempotent retry still returns the original response even after clock
    # advancement expires the hold.
    post_json(http, "/clock/advance", {"target_time": 6})
    retry = http.post("/reservations", json=payload, headers={"Idempotency-Key": key})
    assert retry.status_code == 201
    assert retry.json() == first.json()
    assert http.get("/reservations/1").json()["status"] == "expired"


def test_state_survives_restart_and_expiry_promotion_advances_in_steps(client):
    http, db_path = client
    blocker = post_json(http, "/reservations", {"room_id": 1, "start_time": 10, "end_time": 20}).json()["reservation"]["id"]
    waiting = post_json(http, "/reservations", {"room_id": 1, "start_time": 10, "end_time": 20}).json()["reservation"]["id"]
    post_json(http, f"/reservations/{blocker}/cancel", {})

    # Canceling a waiting-only request releases no capacity and is idempotent.
    cancel_key = str(uuid.uuid4())
    cancel_waiting = http.post(f"/reservations/{waiting}/cancel", json={}, headers={"Idempotency-Key": cancel_key})
    assert cancel_waiting.status_code == 200
    assert cancel_waiting.json()["promotions"] == []
    assert http.post(f"/reservations/{waiting}/cancel", json={}, headers={"Idempotency-Key": cancel_key}).json() == cancel_waiting.json()

    # Recreate the app/connection pool against the same SQLite file.
    app2 = create_app(db_path=db_path, room_count=2, hold_ttl=5)
    with TestClient(app2) as restarted:
        assert restarted.get("/health").json()["clock"] == 0
        held = post_json(restarted, "/reservations", {"room_id": 1, "start_time": 30, "end_time": 40})
        assert held.status_code == 201
        queued = post_json(restarted, "/reservations", {"room_id": 1, "start_time": 30, "end_time": 40})
        assert queued.status_code == 202
        advance = post_json(restarted, "/clock/advance", {"target_time": 5})
        assert advance.status_code == 200
        events = advance.json()["events"]
        assert [event["type"] for event in events].count("expired") == 1
        assert [event["type"] for event in events].count("promoted") == 1
        assert events[-1]["id"] == queued.json()["reservation"]["id"]
        assert restarted.get(f"/reservations/{queued.json()['reservation']['id']}").json()["status"] == "held"

    app3 = create_app(db_path=db_path, room_count=2, hold_ttl=5)
    with TestClient(app3) as restarted_again:
        assert restarted_again.get("/health").json()["clock"] == 5
        assert restarted_again.get(f"/reservations/{queued.json()['reservation']['id']}").json()["status"] == "held"
        assert restarted_again.get(f"/reservations/{held.json()['reservation']['id']}").json()["status"] == "expired"


def test_boundary_intervals_and_capacity_per_room(client):
    http, _ = client
    first = post_json(http, "/reservations", {"room_id": 1, "start_time": 10, "end_time": 20})
    left = post_json(http, "/reservations", {"room_id": 1, "start_time": 0, "end_time": 10})
    right = post_json(http, "/reservations", {"room_id": 1, "start_time": 20, "end_time": 30})
    touching = post_json(http, "/reservations", {"room_id": 1, "start_time": 19, "end_time": 21})
    assert first.status_code == 201
    assert left.status_code == 201
    assert right.status_code == 201
    assert touching.status_code == 202
    assert touching.json()["reservation"]["queue_position"] == 1
