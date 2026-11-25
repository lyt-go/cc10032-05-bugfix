"""watermark 领域服务：消费水位管理。

业务逻辑：
1. 消费水位追踪：分区偏移量、延迟秒数、积压消息数
2. 水位管理链路：watermark → late_data（水位决定延迟阈值）
3. 失败场景：水位重置冲突、heartbeat丢失检测、延迟告警
4. 与上下游流水线：消费 stream_source 的分区信息；为 late_data 提供水位参数

跨模块数据流：
  stream_source → watermark（获取分区和消费进度）
  watermark → late_data（水位决定延迟数据的判定阈值）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class WatermarkService(BaseModuleService):
    MODULE = "watermark"
    ENTITY = "消费水位"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["分区标识", "消费组", "当前位点", "提交时间", "最大事件时间", "延迟秒数", "积压消息数", "上次心跳"]
    REQUIRED_FIELDS = ["分区标识", "消费组", "当前位点"]
    STATUS_ORDER = ["正常", "延迟", "积压", "已重置"]
    ACTION_RULES = {"标记延迟": "延迟", "标记积压": "积压", "重置水位": "已重置"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "watermark_management"
    UPSTREAM_MODULES = ["stream_source"]
    DOWNSTREAM_MODULES = ["late_data"]
    PRODUCES_ARTIFACT = "水位偏移快照"

    def compute_watermark(self, entry_id: int) -> dict[str, Any]:
        """计算当前水位：结合stream_source数据和分区进度。

        失败场景：
        - 上游断流 → watermark未推进
        - heartbeat超时 → 延迟告警
        - 水位重置不合法 → 冲突检测
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"水位 {entry_id} 不存在"}

        partition = str(entry.get("分区标识", ""))
        # 从stream_source获取当前offset
        wm_data = store.get_watermark(partition)
        current_offset = wm_data["offset"] if wm_data else float(entry.get("当前位点", "0"))
        last_heartbeat = wm_data["ts"] if wm_data else time.time()
        delay = time.time() - last_heartbeat

        # 更新水位
        entry["当前位点"] = str(current_offset)
        entry["提交时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["延迟秒数"] = str(round(max(delay, 0), 1))
        entry["上次心跳"] = time.strftime("%Y-%m-%d %H:%M:%S")

        status = "正常"
        lag = float(str(entry.get("延迟秒数", 0)))
        if lag > 300:
            status = "积压"
        elif lag > 60:
            status = "延迟"
        entry["status"] = status
        entry["pending"] = status in ("延迟", "积压")
        entry["abnormal"] = status == "积压"

        # 同步到store
        store.update_watermark(partition, {"offset": current_offset + 50, "ts": time.time(), "lag_seconds": lag})
        progress = store.watermark_progress(partition)

        return {
            "ok": True,
            "entry_id": entry_id,
            "partition": partition,
            "status": status,
            "lag_seconds": round(lag, 1),
            "current_offset": current_offset + 50,
            "confirmation_version": progress["version"],
        }

    def emit_watermark_for_late_data(self, source_id: str) -> dict[str, Any]:
        """向 late_data 暴露当前已确认进度，用于判定迟到数据。

        确认时刻只随位点真正前进而变更，``confirmation_version`` 用于让下游判定
        与某一次确认对齐；缺少可靠确认时刻时 ``confirmed=False``，下游保留待判定。
        """
        progress = store.watermark_progress(source_id)
        return {
            "ok": True,
            "source": source_id,
            "current_offset": progress["offset"],
            "confirmation_version": progress["version"],
            "confirmed": progress["confirmed"],
            "lag_seconds": (store.get_watermark(source_id) or {}).get("lag_seconds", 0),
            "last_update": progress["last_update"],
            "downstream_consumer": "late_data",
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["watermark_offsets_tracked"] = len(store._watermark_offsets)
        return base