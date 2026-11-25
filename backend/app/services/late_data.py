"""late_data 领域服务：迟到数据处理策略。

业务逻辑：
1. 迟到数据判定：只以已确认的消费进度时刻为基准，事件时刻早于确认时刻才计算延迟
2. 迟到处理链路：watermark → late_data → 侧输出/重定向主题
3. 失败场景：丢弃阈值超限、缺少可靠确认时刻、侧输出未配置、重判未确认
4. 与上下游流水线：消费 watermark 水位；输出被判定为迟到的数据行

跨模块数据流：
  watermark → late_data（已确认的消费进度版本决定迟到窗口）
  late_data → 侧输出/重定向（迟到数据路由到专门主题）
  late_data → 迟到事件清单（分类结果回写，保留旧分类用于复核）

判定口径（修复说明）：
  旧实现用 ``abs(确认时刻 - 事件时刻)`` 计算延迟，导致发生在确认时刻之后的事件
  也被当成迟到甚至丢弃。现在只有 ``事件时刻 <= 确认时刻`` 的记录才计算延迟
  ``确认时刻 - 事件时刻``；晚于确认时刻的事件尚未被确认进度覆盖，保持"待判定"。
  延迟阈值用 ``<=`` 比较，与历史判定的等号口径一致。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LateDataService(BaseModuleService):
    MODULE = "late_data"
    ENTITY = "迟到数据策略"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["策略编号", "关联源", "允许延迟秒数", "处理方式", "侧输出路径", "重定向主题", "丢弃阈值", "最后更新"]
    REQUIRED_FIELDS = ["策略编号", "关联源", "允许延迟秒数"]
    STATUS_ORDER = ["草稿", "待验证", "已生效", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "标记生效": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    PIPELINE = "late_data_handler"
    UPSTREAM_MODULES = ["watermark"]
    DOWNSTREAM_MODULES = []
    PRODUCES_ARTIFACT = "迟到数据记录"

    REJUDGE_ACTIONS = {"发起迟到重判", "确认迟到重判", "取消迟到重判"}

    def __init__(self) -> None:
        # 串行化整个判定过程：读确认进度快照 + 回写分类 + 同步侧输出必须原子，
        # 保证并发推进消费进度时，本次结果只对应同一个确认版本。
        self._lock = store.lock

    def _confirmation_time(self, progress: dict[str, Any]) -> float | None:
        """把消费进度里的确认时刻转成数值；不可靠时返回 None（保留待判定）。"""
        if not progress.get("confirmed"):
            return None
        try:
            value = float(progress["offset"])
        except (TypeError, ValueError, KeyError):
            return None
        if value != value:  # NaN
            return None
        return value

    def _event_timestamp(self, event: dict[str, Any]) -> float | None:
        """读取事件时刻，兼容 ts / event_time / timestamp；不可靠时返回 None。"""
        for field in ("ts", "event_time", "timestamp"):
            if field not in event:
                continue
            raw = event[field]
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                continue
            value = float(raw)
            if value != value:  # NaN
                continue
            return value
        return None

    def _provided_delay(self, event: dict[str, Any], confirmed_at: float | None) -> float | None:
        """仅当事件显式带了 ``delay`` 字段时采用它；否则由确认时刻与事件时刻推导。"""
        if "delay" not in event:
            return None
        raw = event.get("delay")
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            return None
        try:
            delay = float(raw)
        except (TypeError, ValueError):
            return None
        if delay != delay or delay < 0:
            return None
        return delay

    def _current_progress(self, entry: dict[str, Any]) -> dict[str, Any]:
        """取判定所用的消费进度快照。

        该源从未上报过进度时，沿用历史口径以当前墙钟作为确认时刻（落 ``fallback``
        标记）；已上报但确认时刻缺失/不可靠时保持 ``confirmed=False``，事件行留待判定。
        """
        progress = store.watermark_progress(str(entry.get("关联源", "")))
        if not progress.get("has_record"):
            progress = {
                "offset": time.time(),
                "version": progress.get("version"),
                "last_update": None,
                "confirmed": True,
                "has_record": False,
                "fallback": True,
            }
        return progress

    def _classify_event(
        self,
        event: dict[str, Any],
        confirmed_at: float | None,
        allowed_lag: float,
        discard_threshold: float,
    ) -> dict[str, Any]:
        """判定单条事件：on_time / late / discarded / unconfirmed。

        等号口径：delay <= allowed_lag 视为准时；allowed_lag < delay <= discard_threshold
        视为迟到；delay > discard_threshold 丢弃。确认时刻只给更早的记录计算延迟。
        """
        delay = self._provided_delay(event, confirmed_at)
        if delay is None:
            if confirmed_at is None:
                return {"classification": "unconfirmed", "delay": None}
            event_ts = self._event_timestamp(event)
            if event_ts is None:
                return {"classification": "unconfirmed", "delay": None}
            if event_ts > confirmed_at:
                # 发生在消费确认之后：确认进度还没有覆盖到它，不能判迟到/丢弃
                return {"classification": "unconfirmed", "delay": None}
            delay = confirmed_at - event_ts

        if delay <= allowed_lag:
            return {"classification": "on_time", "delay": round(delay, 1)}
        if delay <= discard_threshold:
            return {"classification": "late", "delay": round(delay, 1)}
        return {"classification": "discarded", "delay": round(delay, 1)}

    def _event_key(self, event: dict[str, Any], index: int) -> str:
        for field in ("id", "event_id", "key"):
            if field in event and event[field] is not None:
                return f"{field}:{event[field]}"
        timestamp = self._event_timestamp(event)
        if timestamp is not None:
            return f"ts:{timestamp}"
        return f"idx:{index}"

    def _rejudge_state(self, entry_id: int) -> dict[str, Any] | None:
        record = store.find_rejudge(entry_id)
        if record is None:
            return None
        return {
            "rejudge_id": record["rejudge_id"],
            "status": record["status"],
            "basis": record.get("basis", {}),
            "requested_at": record.get("requested_at"),
            "confirmed_at": record.get("confirmed_at"),
        }

    def _confirmation_snapshot(self, confirmation: dict[str, Any]) -> dict[str, Any]:
        """对外暴露的确认进度快照：只保留确认时刻/版本，不泄漏内部标记。"""
        return {
            "offset": confirmation.get("offset"),
            "version": confirmation.get("version"),
            "confirmed": confirmation.get("confirmed", False),
            "last_update": confirmation.get("last_update"),
        }

    def _apply_classification(
        self,
        entry: dict[str, Any],
        entry_id: int,
        events: list[dict[str, Any]],
        confirmation: dict[str, Any],
        requested_at: float,
    ) -> dict[str, Any]:
        """按一个确认版本完成分类、回写清单并同步侧输出。"""
        allowed_lag = float(str(entry.get("允许延迟秒数", "30")))
        discard_threshold = float(str(entry.get("丢弃阈值", "3600")))
        source_id = str(entry.get("关联源", ""))
        confirmed_at = self._confirmation_time(confirmation)
        confirmation_snapshot = self._confirmation_snapshot(confirmation)
        redirect_topic = entry.get("重定向主题", "")
        side_path = entry.get("侧输出路径", "")

        groups: dict[str, list[dict[str, Any]]] = {
            "on_time": [], "late": [], "discarded": [], "unconfirmed": [],
        }
        classified_rows: list[dict[str, Any]] = []

        for index, event in enumerate(events):
            decision = self._classify_event(event, confirmed_at, allowed_lag, discard_threshold)
            classification = decision["classification"]
            event_id = self._event_key(event, index)
            event_ts = self._event_timestamp(event)

            if classification == "unconfirmed":
                reason = (
                    "缺少可靠确认时刻，保留待判定"
                    if confirmed_at is None
                    else "事件晚于消费确认时刻，尚未进入确认进度，保留待判定"
                )
            elif classification == "late":
                reason = f"超过允许延迟 {allowed_lag}s，未超过丢弃阈值 {discard_threshold}s"
            elif classification == "discarded":
                reason = f"超出丢弃阈值 {discard_threshold}s"
            else:
                reason = "在允许延迟窗口内"

            existing = store.find_late_event(event_id, entry_id)
            row: dict[str, Any] = {
                "event_id": event_id,
                "policy_id": entry_id,
                "source": source_id,
                "event_ts": event_ts,
                "event": {key: value for key, value in event.items() if key != "delay"},
                "classification": classification,
                "delay": decision["delay"],
                "reason": reason,
                "confirmation": confirmation_snapshot,
                "side_output_path": side_path if classification == "late" else "",
                "redirect": redirect_topic if classification == "late" else "",
                "last_classified_at": requested_at,
            }
            if existing is not None:
                row["first_classified_at"] = existing.get("first_classified_at", existing.get("last_classified_at", requested_at))
                if existing.get("classification") != classification:
                    history = list(existing.get("classification_history", []))
                    history.append({
                        "classification": existing.get("classification"),
                        "delay": existing.get("delay"),
                        "reason": existing.get("reason"),
                        "confirmation": existing.get("confirmation"),
                        "classified_at": existing.get("last_classified_at"),
                    })
                    row["classification_history"] = history
                    row["original_classification"] = existing.get("original_classification") or {
                        "classification": existing.get("classification"),
                        "delay": existing.get("delay"),
                        "reason": existing.get("reason"),
                        "confirmation": existing.get("confirmation"),
                        "classified_at": existing.get("last_classified_at"),
                    }
                else:
                    row["classification_history"] = list(existing.get("classification_history", []))
                    if existing.get("original_classification"):
                        row["original_classification"] = existing["original_classification"]
                existing.clear()
                existing.update(row)
                classified_rows.append(existing)
            else:
                row["first_classified_at"] = requested_at
                classified_rows.append(store.record_late_event(row))

            groups[classification].append({
                "event": event,
                "event_id": event_id,
                "delay": decision["delay"],
                "lag": decision["delay"],  # 兼容旧字段名
                "reason": reason,
                "redirect": redirect_topic,
            })

        # 只把本次判定为迟到的行放进侧输出；之前误判进侧输出的行同步失效。
        rows_by_id = {row["event_id"]: row for row in classified_rows}
        store.sync_side_output(entry_id, [
            {
                "event_id": item["event_id"],
                "policy_id": entry_id,
                "source": source_id,
                "event_ts": rows_by_id[item["event_id"]]["event_ts"],
                "event": rows_by_id[item["event_id"]]["event"],
                "delay": item["delay"],
                "reason": item["reason"],
                "confirmation": confirmation_snapshot,
                "side_output_path": side_path,
                "redirect": redirect_topic,
                "written_at": requested_at,
            }
            for item in groups["late"]
        ])

        # 迟到样本回写清单里的分类结果，旧分类保留在 classification_history。
        return {
            "confirmed_at": confirmation_snapshot["offset"],
            "confirmation_version": confirmation_snapshot["version"],
            "confirmation_confirmed": confirmation_snapshot["confirmed"],
            "allowed_lag": allowed_lag,
            "discard_threshold": discard_threshold,
            "on_time": len(groups["on_time"]),
            "late": len(groups["late"]),
            "discarded": len(groups["discarded"]),
            "unconfirmed": len(groups["unconfirmed"]),
            "on_time_samples": groups["on_time"][:3],
            "late_samples": groups["late"][:3],
            "discarded_samples": groups["discarded"][:3],
            "unconfirmed_samples": groups["unconfirmed"][:3],
            "classified_rows": classified_rows,
        }

    def _resolve_events(
        self, entry_id: int, events: list[dict[str, Any]] | None
    ) -> list[dict[str, Any]]:
        if events:
            return events
        return store.last_late_input(entry_id)

    def classify_late_events(
        self, entry_id: int, events: list[dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        """按当前已确认的消费进度判定事件是否迟到。

        失败场景：
        - 超出丢弃阈值 → 直接移除
        - 缺少可靠确认时刻或事件时刻 → 保留待判定，不误丢
        - 侧输出路径未配置 → 迟到行无法写入侧输出，结果里明确标出
        """
        with self._lock:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return {"ok": False, "reason": f"策略 {entry_id} 不存在"}
            if entry.get("status") != "已生效":
                return {"ok": False, "reason": f"策略未生效，当前状态: {entry.get('status')}"}

            event_list = self._resolve_events(entry_id, events)
            if events:
                store.remember_late_input(entry_id, events)

            requested_at = time.time()
            progress = self._current_progress(entry)
            result = self._apply_classification(entry, entry_id, event_list, progress, requested_at)

            late_rows = [row for row in result.pop("classified_rows") if row["classification"] == "late"]
            if late_rows:
                # 迟到行写入流水线；补上主键 id，避免被流水线当成脏行拒收。
                self.produce_artifact(
                    [
                        {
                            "id": row["event_id"],
                            "policy_id": row["policy_id"],
                            "event_ts": row["event_ts"],
                            "delay": row["delay"],
                            "reason": row["reason"],
                            "confirmation_offset": row["confirmation"]["offset"],
                            "confirmation_version": row["confirmation"]["version"],
                        }
                        for row in late_rows
                    ],
                    store.next_late_batch_id(entry_id),
                )

            entry["最后更新"] = time.strftime("%Y-%m-%d %H:%M:%S")
            active_rejudge = store.find_rejudge(entry_id)
            warnings = []
            if active_rejudge and active_rejudge.get("status") == "待确认":
                warnings.append(f"存在未确认的迟到重判 #{active_rejudge['rejudge_id']}，当前结果仍按已确认进度判定")
            if result["late"] and not entry.get("侧输出路径"):
                warnings.append("迟到行已判定，但策略未配置侧输出路径，无法写入侧输出")

            rejected = [item["event"] for item in result["discarded_samples"]]
            return {
                "ok": True,
                "entry_id": entry_id,
                "source": str(entry.get("关联源", "")),
                "allowed_lag": result["allowed_lag"],
                "discard_threshold": result["discard_threshold"],
                "confirmed_at": result["confirmed_at"],
                "confirmation_version": result["confirmation_version"],
                "confirmation_confirmed": result["confirmation_confirmed"],
                "on_time": result["on_time"],
                "late": result["late"],
                "discarded": result["discarded"],
                "unconfirmed": result["unconfirmed"],
                "pending": result["unconfirmed"],
                "side_output": [
                    {"event_id": row["event_id"], "delay": row["delay"], "event": row["event"]}
                    for row in late_rows
                ],
                "side_output_written": bool(late_rows) and bool(entry.get("侧输出路径")),
                "on_time_samples": result["on_time_samples"],
                "late_samples": result["late_samples"],
                "discarded_samples": result["discarded_samples"],
                "unconfirmed_samples": result["unconfirmed_samples"],
                "rejected": rejected,
                "warnings": warnings,
                "rejudge": self._rejudge_state(entry_id),
            }

    def query_side_output(self, entry_id: int) -> dict[str, Any]:
        """侧输出查询入口：只返回当前有效的迟到行，并带上判定所用的确认时刻。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"策略 {entry_id} 不存在"}
        rows = store.side_output_for_policy(entry_id)
        items = [
            {
                "event_id": row["event_id"],
                "event_ts": row.get("event_ts"),
                "delay": row.get("delay"),
                "reason": row.get("reason"),
                "source": row.get("source"),
                "side_output_path": row.get("side_output_path"),
                "redirect": row.get("redirect"),
                "confirmation": row.get("confirmation", {}),
                "confirmed_at": (row.get("confirmation") or {}).get("offset"),
                "confirmation_version": (row.get("confirmation") or {}).get("version"),
            }
            for row in rows
        ]
        return {
            "ok": True,
            "entry_id": entry_id,
            "source": str(entry.get("关联源", "")),
            "total": len(items),
            "items": items,
        }

    def query_late_events(self, entry_id: int) -> dict[str, Any]:
        """迟到事件清单查询入口：返回分类结果与旧分类，供复核。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"策略 {entry_id} 不存在"}
        rows = store.late_events_for_policy(entry_id)
        return {
            "ok": True,
            "entry_id": entry_id,
            "total": len(rows),
            "items": rows,
            "by_classification": {
                name: sum(1 for row in rows if row.get("classification") == name)
                for name in ("on_time", "late", "discarded", "unconfirmed")
            },
        }

    def _preview_side_output(self, entry_id: int, confirmed_at: float) -> dict[str, Any]:
        """按重判所选时刻预演一次分类（不落库），用于登记重判时说明时间依据。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {}
        allowed_lag = float(str(entry.get("允许延迟秒数", "30")))
        discard_threshold = float(str(entry.get("丢弃阈值", "3600")))
        events = store.last_late_input(entry_id)
        decisions = []
        for index, event in enumerate(events):
            decision = self._classify_event(event, confirmed_at, allowed_lag, discard_threshold)
            decisions.append({
                "event_id": self._event_key(event, index),
                "event_ts": self._event_timestamp(event),
                "classification": decision["classification"],
                "delay": decision["delay"],
            })
        return {
            "confirmed_at": confirmed_at,
            "eligible_event_ids": [d["event_id"] for d in decisions if d["classification"] == "late"],
            "by_event": decisions,
        }

    def request_rejudge(self, entry_id: int, basis: dict[str, Any] | None = None) -> dict[str, Any]:
        """发起迟到重判：先落库值班员选择的确认时刻依据，等待补到进度后确认。"""
        with self._lock:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return {"ok": False, "reason": f"策略 {entry_id} 不存在"}

            basis = dict(basis or {})
            confirmed_at = basis.get("confirmed_at", basis.get("time_basis"))
            if confirmed_at is None:
                return {"ok": False, "reason": "重判必须给出所选确认时刻（time_basis / confirmed_at）"}
            try:
                confirmed_at = float(str(confirmed_at))
            except (TypeError, ValueError):
                return {"ok": False, "reason": f"重判时间依据不合法: {basis.get('confirmed_at')}"}
            if confirmed_at != confirmed_at or confirmed_at < 0:
                return {"ok": False, "reason": "重判时间依据不合法: 必须是有限的非负时刻"}

            requested_at = time.time()
            progress = self._current_progress(entry)
            basis = {
                "confirmed_at": confirmed_at,
                "confirmation_version": progress.get("version"),
                "confirmation_confirmed": progress.get("confirmed", False),
                "current_confirmed_at": progress.get("offset"),
                "requested_by": basis.get("operator") or basis.get("requested_by") or "值班员",
                "requested_at": requested_at,
                "note": basis.get("note", ""),
            }
            record = {
                "rejudge_id": store.next_rejudge_id(),
                "policy_id": entry_id,
                "status": "待确认",
                "basis": basis,
                "requested_at": requested_at,
                "confirmed_at": None,
                "result": None,
            }
            store.save_rejudge(record)
            preview = self._preview_side_output(entry_id, confirmed_at)
            return {
                "ok": True,
                "rejudge_id": record["rejudge_id"],
                "policy_id": entry_id,
                "status": record["status"],
                "basis": basis,
                "preview": preview,
                "message": "重判已登记，等待消费进度补到所选确认时刻后确认",
            }

    def confirm_rejudge(self, entry_id: int) -> dict[str, Any]:
        """确认重判：消费进度补到所选时刻后，按该时刻重新分类并校正侧输出。"""
        with self._lock:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return {"ok": False, "reason": f"策略 {entry_id} 不存在"}
            record = store.find_rejudge(entry_id)
            if record is None:
                return {"ok": False, "reason": "没有可确认的迟到重判"}
            if record.get("status") != "待确认":
                return {"ok": False, "reason": f"迟到重判当前状态为 {record.get('status')}，不能重复确认"}

            progress = self._current_progress(entry)
            chosen_at = record["basis"].get("confirmed_at")
            if chosen_at is not None:
                if progress.get("confirmed") and float(progress["offset"]) < float(chosen_at):
                    return {
                        "ok": False,
                        "reason": f"消费进度尚未补到重判时间 {chosen_at}，当前确认时刻 {progress['offset']}",
                    }

            events = store.last_late_input(entry_id)
            if not events:
                return {"ok": False, "reason": "没有可用于重判的迟到事件输入"}

            # 重判以值班员选择的时刻为准；新补到的进度只用于校验其已覆盖该时刻。
            confirmation = dict(progress)
            if chosen_at is not None:
                confirmation["offset"] = chosen_at
                confirmation["confirmed"] = True
            confirmation_label = "所选时刻" if chosen_at is not None else "当前确认时刻"

            before = {row["event_id"]: dict(row) for row in store.late_events_for_policy(entry_id)}
            previous_side = {row["event_id"] for row in store.side_output_for_policy(entry_id)}
            result = self._apply_classification(entry, entry_id, events, confirmation, time.time())
            after_rows = result.pop("classified_rows")
            current_side = {row["event_id"] for row in store.side_output_for_policy(entry_id)}

            added = sorted(current_side - previous_side)
            removed = sorted(previous_side - current_side)
            record.update({
                "status": "已确认",
                "confirmed_at": time.time(),
                "time_label": confirmation_label,
                "result": {
                    "time_basis": result["confirmed_at"],
                    "confirmed_at": result["confirmed_at"],
                    "confirmation_version": result["confirmation_version"],
                    "on_time": result["on_time"],
                    "late": result["late"],
                    "discarded": result["discarded"],
                    "unconfirmed": result["unconfirmed"],
                    "side_output_added": added,
                    "side_output_removed": removed,
                    "by_event": [
                        {
                            "event_id": row["event_id"],
                            "previous": (before.get(row["event_id"]) or {}).get("classification"),
                            "previous_classification": before.get(row["event_id"]),
                            "current": row["classification"],
                            "delay": row["delay"],
                            "reason": row["reason"],
                            "confirmation": row["confirmation"],
                        }
                        for row in after_rows
                    ],
                },
            })
            store.save_rejudge(record)
            entry["最后更新"] = time.strftime("%Y-%m-%d %H:%M:%S")
            return {
                "ok": True,
                "rejudge_id": record["rejudge_id"],
                "policy_id": entry_id,
                "status": record["status"],
                "basis": record["basis"],
                "result": record["result"],
                "side_output": [
                    {"event_id": row["event_id"], "delay": row["delay"], "event": row["event"]}
                    for row in store.side_output_for_policy(entry_id)
                ],
            }

    def cancel_rejudge(self, entry_id: int) -> dict[str, Any]:
        with self._lock:
            record = store.find_rejudge(entry_id)
            if record is None or record.get("status") != "待确认":
                return {"ok": False, "reason": "没有待确认的迟到重判可取消"}
            record["status"] = "已取消"
            record["confirmed_at"] = time.time()
            return {"ok": True, "rejudge_id": record["rejudge_id"], "status": record["status"]}

    def run_action(
        self,
        entry_id: int,
        action: str,
        payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        if action == "发起迟到重判":
            result = self.request_rejudge(entry_id, (payload or {}).get("basis"))
            if not result.get("ok"):
                return None, result.get("reason", "发起迟到重判失败")
            eligible = (result.get("preview") or {}).get("eligible_event_ids", [])
            return store.find(self.MODULE, entry_id), (
                f"已登记迟到重判 #{result['rejudge_id']}，时间依据 {result['basis']['confirmed_at']}，"
                f"待确认后侧输出预计包含 {len(eligible)} 行"
            )
        if action == "确认迟到重判":
            result = self.confirm_rejudge(entry_id)
            if not result.get("ok"):
                return None, result.get("reason", "确认迟到重判失败")
            summary = result["result"]
            return store.find(self.MODULE, entry_id), (
                f"迟到重判 #{result['rejudge_id']} 已确认（时间依据 {summary['time_basis']}，"
                f"版本 {summary['confirmation_version']}），侧输出新增 {len(summary['side_output_added'])} 行、"
                f"移除 {len(summary['side_output_removed'])} 行"
            )
        if action == "取消迟到重判":
            result = self.cancel_rejudge(entry_id)
            if not result.get("ok"):
                return None, result.get("reason", "取消迟到重判失败")
            return store.find(self.MODULE, entry_id), f"迟到重判 #{result['rejudge_id']} 已取消"
        return super().run_action(entry_id, action)

    def query_rejudge(self, entry_id: int) -> dict[str, Any]:
        """重判查询入口：返回当次所选时间依据、当前状态与可复核的旧分类。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"策略 {entry_id} 不存在"}
        record = store.find_rejudge(entry_id)
        if record is None:
            return {"ok": True, "entry_id": entry_id, "total": 0, "rejudge": None}
        return {
            "ok": True,
            "entry_id": entry_id,
            "rejudge": record,
            "total": len(store.all_rejudges()),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["active_policies"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已生效")
        base["pending_rejudges"] = sum(
            1 for r in store.all_rejudges() if r.get("status") == "待确认")
        base["side_output_rows"] = len(store.all_side_output())
        return base
