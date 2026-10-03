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
    rooms: int = 2
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
        r = self.reservations[rid]
        if r.status == "held" and r.expires_at is not None and r.expires_at <= self.now:
            return 409, []
        if r.status not in ("held", "confirmed"):
            return 409, []
        if (r.room, r.start, r.end) != (original_room, original_start, original_end):
            return 409, []
        if not (1 <= target_room <= self.rooms):
            return 400, []
        if self.overlaps_active(target_room, target_start, target_end, exclude=rid):
            return 409, []
        r.room, r.start, r.end = target_room, target_start, target_end
        # Held keeps its original deadline; confirmed stays confirmed. The FIFO
        # scan runs only after the move, so capacity still occupied by r (same
        # room, overlapping target) is never released.
        return 200, self.promote_scan()

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


def _move_payload(orig: tuple[int, int, int], target: tuple[int, int, int]) -> dict[str, Any]:
    return {
        "original_room_id": orig[0],
        "original_start_time": orig[1],
        "original_end_time": orig[2],
        "target_room_id": target[0],
        "target_start_time": target[1],
        "target_end_time": target[2],
    }


def _assert_service_matches_ref(http: TestClient, ref: ReferenceModel) -> None:
    rows = http.get("/reservations").json()["reservations"]
    by_id = {row["id"]: row for row in rows}
    assert set(by_id) == set(ref.reservations)
    for rid, r in ref.reservations.items():
        row = by_id[rid]
        assert row["room_id"] == r.room
        assert row["start_time"] == r.start
        assert row["end_time"] == r.end
        assert row["status"] == r.status
        assert row["expires_at"] == r.expires_at
        waiting_ids = sorted(x.id for x in ref.reservations.values() if x.status == "waiting")
        assert row["queue_position"] == (
            waiting_ids.index(rid) + 1 if r.status == "waiting" else None
        )


def test_reschedule_matches_reference_model_across_moves_and_expiry(client):
    http, _ = client
    ref = ReferenceModel(ttl=5, rooms=2)

    def apply_both(room: int, start: int, end: int) -> int:
        expected_code, expected_status, rid = ref.apply(room, start, end)
        response = post_json(
            http, "/reservations", {"room_id": room, "start_time": start, "end_time": end}
        )
        assert response.status_code == expected_code
        assert response.json()["reservation"]["status"] == expected_status
        _assert_service_matches_ref(http, ref)
        return rid

    def confirm_both(rid: int) -> None:
        assert ref.confirm(rid) == 200
        assert post_json(http, f"/reservations/{rid}/confirm", {}).status_code == 200
        _assert_service_matches_ref(http, ref)

    def clock_both(target: int, expected_events: list[tuple[str, int]]) -> None:
        ref.advance_to(target)
        response = post_json(http, "/clock/advance", {"target_time": target})
        assert response.status_code == 200
        events = [(event["type"], event["id"]) for event in response.json()["events"]]
        assert events == expected_events
        _assert_service_matches_ref(http, ref)

    def move_both(
        rid: int,
        orig: tuple[int, int, int],
        target: tuple[int, int, int],
        key: str,
        *,
        apply_to_ref: bool = True,
    ):
        if apply_to_ref:
            expected_code, expected_promotions = ref.reschedule(
                rid, *orig, *target
            )
        response = http.post(
            f"/reservations/{rid}/reschedule",
            json=_move_payload(orig, target),
            headers=headers(key),
        )
        if apply_to_ref:
            assert response.status_code == expected_code
            if expected_code == 200:
                assert [p["id"] for p in response.json()["promotions"]] == expected_promotions
        _assert_service_matches_ref(http, ref)
        return response

    a = apply_both(1, 10, 20)  # 1 held exp 5
    confirm_both(a)            # 1 confirmed
    w = apply_both(1, 10, 20)  # 2 waiting
    other = apply_both(2, 10, 20)  # 3 held exp 5
    neighbor = apply_both(1, 20, 30)  # 4 held exp 5, adjacent

    # Overlapping same-room move is blocked by the other active occupation;
    # neither the reservation nor the waiting queue changes (409 stays retryable).
    blocked = move_both(a, (1, 10, 20), (1, 16, 24), "move-blocked")
    assert blocked.status_code == 409
    replayed_blocked = move_both(
        a, (1, 10, 20), (1, 16, 24), "move-blocked", apply_to_ref=False
    )
    assert replayed_blocked.status_code == 409
    assert replayed_blocked.json() == blocked.json()

    # Same-room move whose target overlaps the old interval but not any other
    # occupation succeeds; the waiter [10,20) is still blocked and must not be
    # promoted by the overlapping portion of the move.
    shrink = move_both(a, (1, 10, 20), (1, 16, 20), "move-shrink")
    assert shrink.status_code == 200
    assert shrink.json()["reservation"]["status"] == "confirmed"

    # Moving the other-room hold into a free adjacent interval succeeds and
    # keeps its original expiry (exp 5, not renewed to now+ttl).
    cross = move_both(other, (2, 10, 20), (1, 0, 10), "move-cross")
    assert cross.status_code == 200
    assert cross.json()["reservation"]["expires_at"] == 5

    # Idempotent replay returns the original response; a changed payload on the
    # same key is rejected without changing state.
    replay = move_both(
        other, (2, 10, 20), (1, 0, 10), "move-cross", apply_to_ref=False
    )
    assert replay.status_code == 200
    assert replay.json() == cross.json()
    changed = http.post(
        f"/reservations/{other}/reschedule",
        json=_move_payload((2, 10, 20), (2, 0, 10)),
        headers=headers("move-cross"),
    )
    assert changed.status_code == 409
    _assert_service_matches_ref(http, ref)

    # Stale original basis: the reservation has already moved to room 1, so the
    # old basis fails even though the requested target interval is free.
    stale = move_both(other, (2, 10, 20), (2, 0, 5), "move-stale")
    assert stale.status_code == 409

    # Repeated reschedules must not renew the hold: move again at t0, then at t4.
    assert move_both(other, (1, 0, 10), (2, 40, 50), "move-away").status_code == 200
    clock_both(4, [])
    renewed = move_both(other, (2, 40, 50), (2, 0, 40), "move-renew-check")
    assert renewed.status_code == 200
    assert renewed.json()["reservation"]["expires_at"] == 5

    # Exact expiry boundary: both holds expire at t5; the waiter is still
    # blocked by the confirmed reservation, so nothing is promoted.
    clock_both(5, [("expired", other), ("expired", neighbor)])

    # A request for an expired hold is stale and changes nothing.
    assert move_both(neighbor, (1, 20, 30), (1, 0, 5), "move-expired").status_code == 409

    # A target-room waiter must not be promoted merely because somebody moves
    # into that room; only genuinely released old capacity promotes FIFO.
    blocker = apply_both(2, 10, 20)   # 5 held exp 10
    target_waiter = apply_both(2, 10, 20)  # 6 waiting
    move = move_both(a, (1, 16, 20), (2, 0, 10), "move-confirmed-out")
    assert move.status_code == 200
    assert [p["id"] for p in move.json()["promotions"]] == [w]
    assert move.json()["reservation"]["status"] == "confirmed"
    # Reservation 6 is still waiting behind the room-2 blocker.
    _assert_service_matches_ref(http, ref)

    # Moving into the room the promoted hold occupies is blocked by it;
    # moving into the adjacent slot works.
    assert move_both(a, (2, 0, 10), (1, 10, 20), "move-into-hold").status_code == 409
    assert move_both(a, (2, 0, 10), (1, 0, 10), "move-adjacent").status_code == 200

    # A no-op move (same room, same interval) succeeds but releases nothing.
    noop = move_both(a, (1, 0, 10), (1, 0, 10), "move-noop")
    assert noop.status_code == 200
    assert noop.json()["promotions"] == []

    # At t10 the room-1 hold (promoted waiter) and room-2 blocker expire; the
    # room-2 waiter is then promoted once.
    clock_both(
        10,
        [("expired", w), ("expired", blocker), ("promoted", target_waiter)],
    )
    assert move_both(w, (1, 10, 20), (1, 0, 5), "move-promoted-expired").status_code == 409

    # The promoted hold keeps its own deadline (15) after moving.
    last = move_both(target_waiter, (2, 10, 20), (2, 0, 10), "move-last")
    assert last.status_code == 200
    assert last.json()["reservation"]["expires_at"] == 15

    # Canceled reservations cannot be rescheduled; bad room/interval rejected.
    assert post_json(http, f"/reservations/{target_waiter}/cancel", {}).status_code == 200
    ref.cancel(target_waiter)
    _assert_service_matches_ref(http, ref)
    assert move_both(target_waiter, (2, 0, 10), (2, 9, 19), "move-canceled").status_code == 409
    bad_room = http.post(
        f"/reservations/{a}/reschedule",
        json=_move_payload((1, 0, 10), (3, 0, 10)),
        headers=headers("move-bad-room"),
    )
    assert bad_room.status_code == 400
    bad_interval = http.post(
        f"/reservations/{a}/reschedule",
        json=_move_payload((1, 0, 10), (1, 30, 20)),
        headers=headers("move-bad-interval"),
    )
    assert bad_interval.status_code == 422
    missing = http.post(
        f"/reservations/{a}/reschedule", json=_move_payload((1, 0, 10), (1, 0, 5))
    )
    assert missing.status_code == 400


def test_two_requests_contend_for_the_same_reservation(tmp_path):
    db_path = str(tmp_path / "contend.db")
    app = create_app(db_path=db_path, room_count=2, hold_ttl=1000)
    barrier = Barrier(4)
    results: list[tuple[str, int, dict[str, Any]]] = []

    def worker(key: str, rid: int, orig: tuple[int, int, int], target: tuple[int, int, int]) -> None:
        local = TestClient(app)
        barrier.wait()
        response = local.post(
            f"/reservations/{rid}/reschedule",
            json=_move_payload(orig, target),
            headers={"Idempotency-Key": key},
        )
        results.append((key, response.status_code, response.json()))

    with TestClient(app) as setup:
        first = setup.post(
            "/reservations",
            json={"room_id": 1, "start_time": 10, "end_time": 20},
            headers={"Idempotency-Key": "create-1"},
        ).json()["reservation"]["id"]
        second = setup.post(
            "/reservations",
            json={"room_id": 1, "start_time": 50, "end_time": 60},
            headers={"Idempotency-Key": "create-2"},
        ).json()["reservation"]["id"]
        third = setup.post(
            "/reservations",
            json={"room_id": 2, "start_time": 70, "end_time": 80},
            headers={"Idempotency-Key": "create-3"},
        ).json()["reservation"]["id"]

        threads = [
            # Two requests reschedule the very same held reservation.
            Thread(target=worker, args=("same-a", first, (1, 10, 20), (2, 0, 10))),
            Thread(target=worker, args=("same-b", first, (1, 10, 20), (1, 20, 30))),
            # Two different reservations fight for one target slot.
            Thread(target=worker, args=("slot-c", second, (1, 50, 60), (1, 0, 5))),
            Thread(target=worker, args=("slot-d", third, (2, 70, 80), (1, 0, 5))),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        by_key = {key: (code, body) for key, code, body in results}

    # Exactly one of the two same-reservation moves wins; the loser sees that
    # the original basis changed and its reservation is not lost or duplicated.
    same_codes = {by_key["same-a"][0], by_key["same-b"][0]}
    assert same_codes == {200, 409}
    winner_key = "same-a" if by_key["same-a"][0] == 200 else "same-b"
    loser_key = "same-b" if winner_key == "same-a" else "same-a"
    winner_target = (2, 0, 10) if winner_key == "same-a" else (1, 20, 30)

    with TestClient(app) as check:
        moved = check.get(f"/reservations/{first}").json()
        assert (moved["room_id"], moved["start_time"], moved["end_time"]) == winner_target
        assert moved["status"] == "held"
        assert moved["expires_at"] == 1000

        second_row = check.get(f"/reservations/{second}").json()
        third_row = check.get(f"/reservations/{third}").json()

    # The slot loser stays at its original room/interval.
    if by_key["slot-c"][0] == 200:
        assert by_key["slot-d"][0] == 409
        assert (third_row["room_id"], third_row["start_time"], third_row["end_time"]) == (2, 70, 80)
    else:
        assert by_key["slot-c"][0] == 409
        assert by_key["slot-d"][0] == 200
        assert (second_row["room_id"], second_row["start_time"], second_row["end_time"]) == (1, 50, 60)

    # Idempotent retries replay the original outcome for winner and loser.
    with TestClient(app) as replay_client:
        winner_retry = replay_client.post(
            f"/reservations/{first}/reschedule",
            json=_move_payload(
                (1, 10, 20), winner_target
            ),
            headers={"Idempotency-Key": winner_key},
        )
        loser_replay_payload = (2, 0, 10) if loser_key == "same-a" else (1, 20, 30)
        loser_retry = replay_client.post(
            f"/reservations/{first}/reschedule",
            json=_move_payload((1, 10, 20), loser_replay_payload),
            headers={"Idempotency-Key": loser_key},
        )
        assert winner_retry.status_code == 200
        assert winner_retry.json() == by_key[winner_key][1]
        assert loser_retry.status_code == 409
        assert loser_retry.json() == by_key[loser_key][1]
