"""迟到数据链路回归测试：确认时刻口径、侧输出与重判。

覆盖题目要求的四事件场景（确认时刻 1000、允许延迟 30s、丢弃阈值 60s）：
  - 1061 发生在确认之后 → 保留待判定，不被误判为迟到/丢弃
  - 970  延迟 30s（等号） → 准时
  - 940  延迟 60s（等号） → 迟到，且只有它进入侧输出
  - 939  延迟 61s → 丢弃

以及：未确认重判落地时间依据、进度补到后确认、侧输出校正、旧分类可复核、
并发推进进度时结果仍绑定同一确认版本。
"""
from __future__ import annotations

import threading

import pytest

from app.services import service_for
from app.store import store


POLICY_ID = 3
SOURCE = "partition-2"
EVENTS = [{"ts": ts} for ts in (1061, 970, 940, 939)]


@pytest.fixture()
def policy():
    entry = store.find("late_data", POLICY_ID)
    entry.update({
        "status": "已生效",
        "关联源": SOURCE,
        "允许延迟秒数": "30",
        "丢弃阈值": "60",
        "侧输出路径": "s3://late-output/",
        "重定向主题": "late_topic",
    })
    # 清理该策略的历史产物，保证用例相互独立
    store._late_events[:] = [e for e in store._late_events if e.get("policy_id") != POLICY_ID]
    store._late_side_output[:] = [e for e in store._late_side_output if e.get("policy_id") != POLICY_ID]
    store._late_rejudges.clear()
    store._late_inputs.pop(POLICY_ID, None)
    return entry


def test_only_earlier_events_get_delay(policy):
    """确认时刻只给更早的记录计算延迟，晚于确认时刻的记录留待判定。"""
    store.update_watermark(SOURCE, {"offset": 1000, "ts": 1})
    result = service_for("late_data").classify_late_events(POLICY_ID, EVENTS)

    assert result["confirmed_at"] == 1000
    assert result["on_time"] == 1       # 970，延迟 30s 等号
    assert result["late"] == 1          # 940，延迟 60s 等号
    assert result["discarded"] == 1     # 939，延迟 61s
    assert result["unconfirmed"] == 1   # 1061，晚于确认时刻
    assert result["discarded"] < 2      # 1061 不能被误丢


def test_side_output_has_only_940(policy):
    """侧输出只包含 940，并关联判定所用的确认时刻。"""
    store.update_watermark(SOURCE, {"offset": 1000, "ts": 1})
    ld = service_for("late_data")
    ld.classify_late_events(POLICY_ID, EVENTS)

    side = ld.query_side_output(POLICY_ID)
    assert [row["event_id"] for row in side["items"]] == ["ts:940.0"]
    assert side["items"][0]["confirmed_at"] == 1000
    assert side["items"][0]["delay"] == 60


def test_confirmation_version_snapshot_is_consistent(policy):
    """并发推进消费进度时，一次分类只对应同一个确认版本。"""
    store.update_watermark(SOURCE, {"offset": 1000, "ts": 1})
    ld = service_for("late_data")
    barrier = threading.Barrier(2)
    captured = []

    def classify():
        barrier.wait()
        captured.append(ld.classify_late_events(POLICY_ID, EVENTS))

    def advance():
        barrier.wait()
        for offset in range(1001, 1006):
            store.update_watermark(SOURCE, {"offset": offset, "ts": offset})

    threads = [threading.Thread(target=classify), threading.Thread(target=advance)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    result = captured[0]
    assert result["confirmation_confirmed"] is True
    rows = ld.query_late_events(POLICY_ID)["items"]
    assert {row["confirmation"]["version"] for row in rows} == {result["confirmation_version"]}
    assert {row["confirmation"]["offset"] for row in rows} == {result["confirmed_at"]}


def test_unconfirmed_rejudge_records_time_basis(policy):
    """未确认的重判先留下当次选择的时间依据，不立即改判定结果。"""
    store.update_watermark(SOURCE, {"offset": 1000, "ts": 1})
    ld = service_for("late_data")
    ld.classify_late_events(POLICY_ID, EVENTS)

    requested = ld.request_rejudge(POLICY_ID, {"confirmed_at": 1030, "operator": "值班员"})
    assert requested["ok"] is True
    assert requested["status"] == "待确认"
    assert requested["basis"]["confirmed_at"] == 1030

    # 进度尚未补到 1030：不能确认，侧输出保持原样
    blocked = ld.confirm_rejudge(POLICY_ID)
    assert blocked["ok"] is False
    assert "尚未补到" in blocked["reason"]
    assert [row["event_id"] for row in ld.query_side_output(POLICY_ID)["items"]] == ["ts:940.0"]


def test_rejudge_after_progress_catches_up(policy):
    """进度补到 1030 后确认重判：引用所选时刻、校正侧输出、保留旧分类。"""
    store.update_watermark(SOURCE, {"offset": 1000, "ts": 1})
    ld = service_for("late_data")
    ld.classify_late_events(POLICY_ID, EVENTS)
    ld.request_rejudge(POLICY_ID, {"confirmed_at": 1030})

    store.update_watermark(SOURCE, {"offset": 1030, "ts": 2})
    confirmed = ld.confirm_rejudge(POLICY_ID)

    assert confirmed["ok"] is True
    assert confirmed["result"]["time_basis"] == 1030
    # 940 由迟到变为丢弃，退出侧输出
    assert confirmed["result"]["side_output_removed"] == ["ts:940.0"]
    assert confirmed["result"]["side_output_added"] == ["ts:970.0"]
    assert [row["event_id"] for row in ld.query_side_output(POLICY_ID)["items"]] == ["ts:970.0"]

    events = {row["event_id"]: row for row in ld.query_late_events(POLICY_ID)["items"]}
    assert events["ts:940.0"]["classification"] == "discarded"
    assert events["ts:940.0"]["original_classification"]["classification"] == "late"
    assert events["ts:940.0"]["confirmation"]["offset"] == 1030


def test_missing_confirmation_keeps_rows_pending(policy):
    """缺少可靠确认时刻时保留待判定行，不判迟到也不丢弃。"""
    store.update_watermark(SOURCE, {"offset": None, "ts": 1})
    result = service_for("late_data").classify_late_events(POLICY_ID, EVENTS)

    assert result["confirmation_confirmed"] is False
    assert result["late"] == 0
    assert result["discarded"] == 0
    assert result["unconfirmed"] == len(EVENTS)
    assert service_for("late_data").query_side_output(POLICY_ID)["items"] == []


def test_request_rejudge_requires_time_basis(policy):
    """重判必须留下当次选择的时间依据。"""
    store.update_watermark(SOURCE, {"offset": 1000, "ts": 1})
    ld = service_for("late_data")
    ld.classify_late_events(POLICY_ID, EVENTS)
    result = ld.request_rejudge(POLICY_ID, {})
    assert result["ok"] is False
    assert "确认时刻" in result["reason"]
