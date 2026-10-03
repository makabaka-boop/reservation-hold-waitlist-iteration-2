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

    def reschedule(
        self,
        rid: int,
        original_room: int,
        original_start: int,
        original_end: int,
        target_room: int,
        target_start: int,
        target_end: int,
    ) -> tuple[int, list[int]]:
        r = self.reservations.get(rid)
        if r is None:
            return 404, []
        if r.status not in ("held", "confirmed"):
            return 409, []
        if r.status == "held" and r.expires_at is not None and r.expires_at <= self.now:
            return 409, []
        if (
            r.room != original_room
            or r.start != original_start
            or r.end != original_end
        ):
            return 409, []
        if self.overlaps_active(target_room, target_start, target_end, exclude=rid):
            return 409, []
        # One atomic move; expires_at is deliberately preserved.
        r.room, r.start, r.end = target_room, target_start, target_end
        promoted = self.promote_scan()
        return 200, promoted

    def promote_scan(self) -> list[int]:
        promoted: list[int] = []
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
                promoted.append(candidate.id)
        return promoted

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


def reschedule_payload(
    original_room: int,
    original_start: int,
    original_end: int,
    target_room: int,
    target_start: int,
    target_end: int,
) -> dict[str, Any]:
    return {
        "original_room_id": original_room,
        "original_start_time": original_start,
        "original_end_time": original_end,
        "target_room_id": target_room,
        "target_start_time": target_start,
        "target_end_time": target_end,
    }


def assert_all_match_ref(http: TestClient, ref: ReferenceModel) -> None:
    for rid in ref.reservations:
        assert_matches_ref(http, ref, rid)


def test_reschedule_matches_reference_model(client):
    http, _ = client
    ref = ReferenceModel(ttl=5)

    def apply(room: int, start: int, end: int) -> int:
        code, status, rid = ref.apply(room, start, end)
        response = post_json(http, "/reservations", {"room_id": room, "start_time": start, "end_time": end})
        assert response.status_code == code
        assert response.json()["reservation"]["status"] == status
        return rid

    # Room 1 holds A [10,30) (later confirmed) and B [40,50); a waiter for
    # each room plus a waiter only fitting room 2's [10,30).
    rid_a = apply(1, 10, 30)
    assert ref.confirm(rid_a) == 200
    assert post_json(http, f"/reservations/{rid_a}/confirm", {}).status_code == 200
    rid_b = apply(1, 40, 50)
    rid_w1 = apply(1, 10, 30)   # waiting behind A
    rid_w2 = apply(2, 10, 30)   # held in room 2
    rid_w3 = apply(1, 40, 45)   # waiting behind B
    assert_all_match_ref(http, ref)

    def reschedule(
        rid: int,
        o_room: int,
        o_start: int,
        o_end: int,
        t_room: int,
        t_start: int,
        t_end: int,
        key: str | None = None,
    ):
        code, promoted = ref.reschedule(
            rid, o_room, o_start, o_end, t_room, t_start, t_end
        )
        response = post_json(
            http,
            f"/reservations/{rid}/reschedule",
            reschedule_payload(o_room, o_start, o_end, t_room, t_start, t_end),
            key=key,
        )
        assert response.status_code == code, response.text
        if code == 200:
            assert [item["id"] for item in response.json()["promotions"]] == promoted
            assert response.json()["reservation"]["id"] == rid
        return response

    # Move confirmed A room1[10,30) -> room2[10,30): blocked by held w2.
    reschedule(rid_a, 1, 10, 30, 2, 10, 30)
    assert_all_match_ref(http, ref)

    # Adjacent half-open intervals never block: A -> room2[30,50).
    reschedule(rid_a, 1, 10, 30, 2, 30, 50)
    # Moving A out of room 1 releases exactly [10,30): w1 fits and is promoted.
    assert ref.reservations[rid_w1].status == "held"
    assert_all_match_ref(http, ref)

    # Stale original interval is rejected; state and queue unchanged.
    reschedule(rid_a, 1, 10, 30, 1, 10, 30)
    reschedule(rid_a, 2, 30, 50, 1, 0, 10)  # 1,0,10) is adjacent to w1 -> fits
    assert_all_match_ref(http, ref)
    assert ref.reservations[rid_a].status == "confirmed"

    # Overlapping same-room move for confirmed A [0,10) -> [5,15) must not
    # double-release capacity: w1 still holds [10,30) and blocks the target.
    reschedule(rid_a, 1, 0, 10, 1, 5, 15)
    assert_all_match_ref(http, ref)

    # Same-room move to an adjacent interval succeeds and promotes nobody.
    reschedule(rid_a, 1, 0, 10, 1, 30, 40)
    assert_all_match_ref(http, ref)

    # Held B room1[40,50) moves to room2[0,10); w3 fits the released [40,45)
    # in FIFO order and is promoted with expiry = now + ttl.
    assert ref.reservations[rid_b].expires_at == 5
    reschedule(rid_b, 1, 40, 50, 2, 0, 10)
    assert ref.reservations[rid_b].expires_at == 5
    assert ref.reservations[rid_w3].status == "held"
    assert ref.reservations[rid_w3].expires_at == 5
    assert_all_match_ref(http, ref)

    # Non-active reservations cannot be rescheduled even when the target is free.
    rid_w4 = apply(1, 10, 20)  # waiting: overlaps promoted w1 [10,30)
    reschedule(rid_w4, 1, 10, 20, 2, 30, 40)
    assert_all_match_ref(http, ref)


def test_reschedule_expiry_boundary_and_no_hold_extension(client):
    http, _ = client

    held = post_json(http, "/reservations", {"room_id": 1, "start_time": 10, "end_time": 20})
    rid = held.json()["reservation"]["id"]
    assert held.json()["reservation"]["expires_at"] == 5

    # Rescheduling at time 4 keeps the original expiry instant (5): no renewal.
    post_json(http, "/clock/advance", {"target_time": 4})
    response = post_json(
        http,
        f"/reservations/{rid}/reschedule",
        reschedule_payload(1, 10, 20, 1, 20, 30),
    )
    assert response.status_code == 200
    assert response.json()["reservation"]["expires_at"] == 5
    assert response.json()["reservation"]["start_time"] == 20

    # At the exact boundary the hold is expired: the reschedule is rejected
    # and leaves both the reservation and the waiting queue untouched.
    waiter = post_json(http, "/reservations", {"room_id": 2, "start_time": 20, "end_time": 30})
    waiter_id = waiter.json()["reservation"]["id"]
    post_json(http, "/clock/advance", {"target_time": 5})
    response = post_json(
        http,
        f"/reservations/{rid}/reschedule",
        reschedule_payload(1, 20, 30, 2, 30, 40),
    )
    assert response.status_code == 409
    assert http.get(f"/reservations/{rid}").json()["status"] == "expired"
    assert http.get(f"/reservations/{rid}").json()["room_id"] == 1
    assert http.get(f"/reservations/{waiter_id}").json()["status"] == "held"

    # Confirmed reservations ignore the clock entirely.
    confirmed = post_json(http, "/reservations", {"room_id": 1, "start_time": 100, "end_time": 200})
    cid = confirmed.json()["reservation"]["id"]
    post_json(http, f"/reservations/{cid}/confirm", {})
    post_json(http, "/clock/advance", {"target_time": 20})
    response = post_json(
        http,
        f"/reservations/{cid}/reschedule",
        reschedule_payload(1, 100, 200, 2, 100, 200),
    )
    assert response.status_code == 200
    body = response.json()["reservation"]
    assert body["status"] == "confirmed"
    assert body["room_id"] == 2
    assert body["expires_at"] is None


def test_reschedule_idempotent_retry_and_payload_change(client):
    http, _ = client
    held = post_json(http, "/reservations", {"room_id": 1, "start_time": 10, "end_time": 20})
    rid = held.json()["reservation"]["id"]

    payload = reschedule_payload(1, 10, 20, 1, 30, 40)
    key = str(uuid.uuid4())
    first = http.post(f"/reservations/{rid}/reschedule", json=payload, headers={"Idempotency-Key": key})
    second = http.post(f"/reservations/{rid}/reschedule", json=payload, headers={"Idempotency-Key": key})
    changed = http.post(
        f"/reservations/{rid}/reschedule",
        json=reschedule_payload(1, 10, 20, 1, 30, 41),
        headers={"Idempotency-Key": key},
    )
    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json() == first.json()
    assert changed.status_code == 409

    # A new reschedule based on the moved reservation works, but replaying the
    # old key still returns the original stored response.
    third = post_json(
        http,
        f"/reservations/{rid}/reschedule",
        reschedule_payload(1, 30, 40, 2, 50, 60),
    )
    assert third.status_code == 200
    replay = http.post(f"/reservations/{rid}/reschedule", json=payload, headers={"Idempotency-Key": key})
    assert replay.status_code == 200
    assert replay.json() == first.json()
    current = http.get(f"/reservations/{rid}").json()
    assert current["room_id"] == 2
    assert (current["start_time"], current["end_time"]) == (50, 60)

    # A failed reschedule (stale original interval) is also replayed exactly.
    fail_key = str(uuid.uuid4())
    fail_payload = reschedule_payload(1, 10, 20, 1, 0, 5)
    fail_first = http.post(
        f"/reservations/{rid}/reschedule", json=fail_payload, headers={"Idempotency-Key": fail_key}
    )
    fail_retry = http.post(
        f"/reservations/{rid}/reschedule", json=fail_payload, headers={"Idempotency-Key": fail_key}
    )
    assert fail_first.status_code == 409
    assert fail_retry.status_code == 409
    assert fail_retry.json() == fail_first.json()


def test_two_requests_contend_for_same_reservation(tmp_path):
    db_path = str(tmp_path / "contend.db")
    app = create_app(db_path=db_path, room_count=1, hold_ttl=100)
    barrier = Barrier(2)
    results: list[tuple[int, Any]] = []

    def move(
        payload: dict[str, Any],
        outcomes: list[tuple[int, Any]],
    ) -> None:
        local = TestClient(app)
        barrier.wait()
        response = local.post(
            "/reservations/1/reschedule",
            json=payload,
            headers={"Idempotency-Key": str(uuid.uuid4())},
        )
        outcomes.append((response.status_code, response.json()))

    with TestClient(app) as setup:
        created = post_json(setup, "/reservations", {"room_id": 1, "start_time": 10, "end_time": 20})
        rid = created.json()["reservation"]["id"]
        assert rid == 1

        threads = [
            Thread(
                target=move,
                args=(reschedule_payload(1, 10, 20, 1, 20, 30), results),
            ),
            Thread(
                target=move,
                args=(reschedule_payload(1, 10, 20, 1, 30, 40), results),
            ),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert sorted(code for code, _ in results) == [200, 409]
    with TestClient(app) as check:
        body = check.get("/reservations/1").json()
        assert body["status"] == "held"
        moved = [(code, data) for code, data in results if code == 200][0][1]
        assert (body["start_time"], body["end_time"]) == (
            moved["reservation"]["start_time"],
            moved["reservation"]["end_time"],
        )
        assert body["start_time"] in (20, 30)
        assert body["end_time"] == body["start_time"] + 10
