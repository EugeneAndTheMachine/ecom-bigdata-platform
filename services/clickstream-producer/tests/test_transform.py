from datetime import UTC, datetime, timedelta

import pytest

from producer.chaos import ChaosInjector
from producer.reader import parse_event_time
from producer.transform import InvalidEvent, ReplayClock, to_record

T0 = datetime(2019, 10, 1, 0, 0, tzinfo=UTC)


def _row(**over):
    row = {
        "event_time": T0, "event_type": "view", "product_id": 44600062, "category_id": 2103807459595387724,
        "category_code": None, "brand": "shiseido", "price": 35.79, "user_id": 541312140,
        "user_session": "72d76fde-8bb3-4e00-8c23-a032dfed738c",
    }
    row.update(over)
    return row


def test_parse_event_time():
    assert parse_event_time("2019-10-01 00:00:00 UTC") == T0


def test_shift_mode_compresses_time_by_speed():
    wall = datetime(2026, 1, 1, tzinfo=UTC)
    clock = ReplayClock(speed=10, time_mode="shift", wall_start=wall)
    assert clock.event_time(T0) == wall
    assert clock.event_time(T0 + timedelta(seconds=100)) == wall + timedelta(seconds=10)


def test_original_mode_keeps_timestamps():
    clock = ReplayClock(speed=10, time_mode="original")
    rec = to_record(_row(), clock)
    assert rec["event_time"] == T0 and rec["original_event_time"] == T0


@pytest.mark.parametrize("bad, reason", [
    ({"event_type": "click"}, "invalid_event_type"),
    ({"price": -1.0}, "invalid_price"),
    ({"user_session": None}, "missing_user_session"),
])
def test_invalid_rows_go_to_dlq(bad, reason):
    with pytest.raises(InvalidEvent) as exc:
        to_record(_row(**bad), ReplayClock(speed=1))
    assert exc.value.reason == reason


def test_chaos_disabled_is_identity():
    chaos = ChaosInjector(1.0, 0.0, 0.0, 10, enabled=False)
    rec = to_record(_row(), ReplayClock(speed=1))
    assert chaos.process(rec, 0.0) == [rec]


def test_chaos_late_records_are_held_then_released():
    chaos = ChaosInjector(late_ratio=1.0, dup_ratio=0.0, bot_burst_prob=0.0, late_delay_s=60)
    rec = to_record(_row(), ReplayClock(speed=1))
    assert chaos.process(rec, now_ts=0.0) == []
    assert chaos.release_due(now_ts=30.0) == []
    released = chaos.release_due(now_ts=61.0)
    assert len(released) == 1 and released[0]["chaos_tag"] == "late"


def test_chaos_dup_keeps_event_id():
    chaos = ChaosInjector(late_ratio=0.0, dup_ratio=1.0, bot_burst_prob=0.0, late_delay_s=60)
    rec = to_record(_row(), ReplayClock(speed=1))
    a, b = chaos.process(rec, 0.0)
    assert a["event_id"] == b["event_id"] and b["chaos_tag"] == "dup"


def test_bot_burst_is_dense():
    chaos = ChaosInjector(0.0, 0.0, 0.0, 60)
    rec = to_record(_row(), ReplayClock(speed=1))
    burst = chaos.bot_burst(rec)
    assert len(burst) >= 50
    assert {e["chaos_tag"] for e in burst} == {"bot"}
    span = burst[-1]["event_time"] - burst[0]["event_time"]
    assert span <= timedelta(seconds=10)



def test_identical_source_rows_share_event_id():
    clock = ReplayClock(speed=10, wall_start=datetime(2026, 1, 1, tzinfo=UTC))
    a, b = to_record(_row(), clock), to_record(_row(), clock)
    assert a["event_id"] == b["event_id"] and len(a["event_id"]) == 64


def test_price_is_not_part_of_the_id_like_silver_natural_key():
    clock = ReplayClock(speed=10, wall_start=datetime(2026, 1, 1, tzinfo=UTC))
    assert to_record(_row(price=35.79), clock)["event_id"] == to_record(_row(price=99.0), clock)["event_id"]


@pytest.mark.parametrize("change", [
    {"user_session": "other-session"},
    {"event_time": T0 + timedelta(seconds=1)},
    {"product_id": 1},
    {"event_type": "cart"},
])
def test_event_id_changes_with_each_natural_key_field(change):
    clock = ReplayClock(speed=10, wall_start=datetime(2026, 1, 1, tzinfo=UTC))
    base = to_record(_row(), clock)
    assert base["event_id"] != to_record(_row(**change), clock)["event_id"]


def test_producer_restart_creates_new_ids_like_new_event_times():
    first = to_record(_row(), ReplayClock(speed=10, wall_start=datetime(2026, 1, 1, tzinfo=UTC)))
    again = to_record(_row(), ReplayClock(speed=10, wall_start=datetime(2026, 1, 2, tzinfo=UTC)))
    assert first["event_time"] != again["event_time"] and first["event_id"] != again["event_id"]


def test_event_time_is_truncated_to_milliseconds():
    clock = ReplayClock(speed=3, wall_start=datetime(2026, 1, 1, tzinfo=UTC))
    to_record(_row(), clock)                                   # anchors first_original
    rec = to_record(_row(event_time=T0 + timedelta(seconds=1)), clock)   # 1/3 s -> 333333 us
    assert rec["event_time"].microsecond == 333000
