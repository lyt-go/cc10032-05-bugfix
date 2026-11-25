"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据，并支持跨模块数据流水线。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import hashlib
import threading
import time
from typing import Any

from app.seed import SEED_ROWS


class DataPipeline:
    """跨模块数据流水线引擎：支持 输入→转换→版本化产物→下游消费 的完整链路。

    每条流水线定义了源模块、目标模块、转换函数和验证规则。
    产物存储为可版本化的数据快照，支持脏行检测、重复批次拒绝、冲突版本检测、中断恢复。
    """

    def __init__(self, name: str, sources: list[str], target_module: str) -> None:
        self.name = name
        self.sources = sources
        self.target_module = target_module
        self._artifacts: list[dict[str, Any]] = []
        self._versions: list[str] = []  # 版本号列表
        self._dirty_log: list[dict[str, Any]] = []
        self._batch_registry: set[str] = set()  # 防重复批次
        self._last_checkpoint: dict[str, Any] = {}  # 中断恢复点
        self._interrupted = False

    def ingest(self, rows: list[dict[str, Any]], batch_id: str) -> tuple[int, list[str]]:
        """摄入数据行，返回 (成功行数, 脏行详情列表)"""
        if batch_id in self._batch_registry:
            return 0, [f"重复批次 {batch_id} 已存在，拒绝摄入"]
        self._batch_registry.add(batch_id)
        clean_rows = []
        dirty_details = []
        for i, row in enumerate(rows):
            issues = self._validate_row(row, i)
            if issues:
                dirty_details.append(f"行 {i}: {'; '.join(issues)}")
                self._dirty_log.append({"batch": batch_id, "row_index": i, "issues": issues, "row": row, "ts": time.time()})
            else:
                clean_rows.append(row)
        self._artifacts.extend(clean_rows)
        return len(clean_rows), dirty_details

    def transform(self, transform_fn, *args, **kwargs) -> list[dict[str, Any]]:
        """对当前产物执行转换函数"""
        self._save_checkpoint("transform_enter", len(self._artifacts))
        try:
            result = transform_fn(self._artifacts, *args, **kwargs)
            version = self._make_version("transform")
            self._versions.append(version)
            self._save_checkpoint("transform_exit", len(result))
            return result
        except Exception:
            self._interrupted = True
            raise

    def versioned_snapshot(self) -> dict[str, Any]:
        """生成版本化产物快照"""
        version = self._versions[-1] if self._versions else self._make_version("snapshot")
        return {
            "pipeline": self.name,
            "version": version,
            "artifact_count": len(self._artifacts),
            "artifact_sample": self._artifacts[:5],
            "dirty_count": len(self._dirty_log),
            "batch_count": len(self._batch_registry),
            "interrupted": self._interrupted,
            "checkpoint": self._last_checkpoint,
        }

    def health_report(self) -> dict[str, Any]:
        """流水线健康报告：用于外部模块消费和容量材料生成"""
        return {
            "pipeline": self.name,
            "source_modules": self.sources,
            "target_module": self.target_module,
            "total_ingested": sum(1 for a in self._artifacts),
            "dirty_rows": len(self._dirty_log),
            "versions": len(self._versions),
            "batches_processed": len(self._batch_registry),
            "interrupted": self._interrupted,
            "last_artifact_ts": max((a.get("ts", 0) for a in self._artifacts), default=0) if self._artifacts else 0,
        }

    def _validate_row(self, row: dict[str, Any], index: int) -> list[str]:
        issues = []
        for key, val in row.items():
            if val is None or (isinstance(val, str) and not val.strip()):
                issues.append(f"空值字段 '{key}'")
        if any(row.get(k) is None for k in ["id"]):
            issues.append("缺少主键 id")
        return issues

    def _make_version(self, tag: str) -> str:
        ts = str(int(time.time() * 1000))
        raw = f"{self.name}:{tag}:{ts}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def _save_checkpoint(self, stage: str, count: int) -> None:
        self._last_checkpoint = {"stage": stage, "count": count, "ts": time.time()}

    def recover_checkpoint(self) -> dict[str, Any]:
        """从最近检查点恢复"""
        return {
            "pipeline": self.name,
            "interrupted": self._interrupted,
            "last_checkpoint": self._last_checkpoint,
            "artifacts_before_interrupt": len(self._artifacts),
        }


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块数据流水线
        self._pipelines: dict[str, DataPipeline] = {}
        # 全局变更日志（用于 changelog 跨模块追溯）
        self._global_changelog: list[dict[str, Any]] = []
        # Schema 注册表（cross-module schema registry）
        self._schema_registry: dict[str, list[dict[str, Any]]] = {}
        # 水位偏移记录（watermark → late_data 共享）
        self._watermark_offsets: dict[str, dict[str, Any]] = {}
        # 消费进度确认版本：水位每次真正推进才递增；迟到判定一次只取一个版本
        self._watermark_version = 0
        # 全局重入锁：水位版本与迟到判定回写必须原子，避免并发更新串版本
        self._store_lock = threading.RLock()
        # 迟到事件清单（分类结果回写，旧分类保留用于复核）
        self._late_events: list[dict[str, Any]] = []
        # 迟到侧输出（只保存当前有效的迟到行，失效行保留审计标记）
        self._late_side_output: list[dict[str, Any]] = []
        # 迟到重判记录：值班员选择的时间依据先落库，再决定侧输出归属
        self._late_rejudges: dict[int, dict[str, Any]] = {}
        # 每条策略最近一次判定使用的事件输入，供未带事件的补判/重判复用
        self._late_inputs: dict[int, list[dict[str, Any]]] = {}
        self._late_seq = 0
        self._late_batch_seq = 0
        self._late_rejudge_seq = 0
        # 快照存储表（snapshot_mgr → backfill_job 共享）
        self._snapshot_store: dict[str, dict[str, Any]] = {}
        # 血缘图（lineage → table_format 共享）
        self._lineage_graph: dict[str, list[dict[str, Any]]] = {}
        # 归档清单（cold_archive → retention 共享）
        self._archive_manifest: list[dict[str, Any]] = []

    # ---------- 基础 CRUD ----------
    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        # 跨模块流水线状态
        pipeline_stats = []
        for pname, pl in self._pipelines.items():
            pipeline_stats.append(pl.health_report())
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
            {"label": "流水线", "value": len(pipeline_stats)},
        ]
        return {"cards": cards, "modules": modules, "pipelines": pipeline_stats}

    # ---------- 跨模块流水线 ----------
    def create_pipeline(self, name: str, sources: list[str], target_module: str) -> DataPipeline:
        pl = DataPipeline(name, sources, target_module)
        self._pipelines[name] = pl
        return pl

    def pipeline(self, name: str) -> DataPipeline | None:
        return self._pipelines.get(name)

    def all_pipelines(self) -> dict[str, DataPipeline]:
        return dict(self._pipelines)

    # ---------- Schema 注册表 ----------
    def register_schema(self, topic: str, schema_def: dict[str, Any], version: str) -> dict[str, Any]:
        versions = self._schema_registry.setdefault(topic, [])
        entry = {"topic": topic, "definition": schema_def, "version": version, "ts": time.time()}
        versions.append(entry)
        return entry

    def schema_versions(self, topic: str) -> list[dict[str, Any]]:
        return self._schema_registry.get(topic, [])

    def check_schema_compatibility(self, topic: str, new_fields: list[str]) -> dict[str, Any]:
        versions = self.schema_versions(topic)
        if not versions:
            return {"compatible": True, "reason": "新主题，无兼容性问题"}
        latest = versions[-1]
        old_fields = set(latest.get("definition", {}).get("fields", []))
        missing_required = [f for f in old_fields if f not in new_fields]
        if missing_required:
            return {"compatible": False, "reason": f"缺少已有字段: {missing_required}"}
        return {"compatible": True, "reason": "向后兼容"}

    # ---------- 水位记录 ----------
    @property
    def lock(self) -> threading.RLock:
        """跨模块共享的原子操作锁：消费进度推进与迟到判定互斥。"""
        return self._store_lock

    def update_watermark(self, partition: str, offset: dict[str, Any]) -> None:
        """推进消费水位。只有确认位点真正前进才递增确认版本。

        重复提交同一确认时刻（例如并发重放）返回同一版本，迟到判定因此只会
        看到同一份确认时刻；位点回退同样不递增版本，避免把已确认进度改小。
        """
        with self._store_lock:
            previous = self._watermark_offsets.get(partition)
            confirmed = True
            if previous is not None:
                try:
                    confirmed = float(offset.get("offset")) > float(previous.get("offset"))
                except (TypeError, ValueError):
                    # 无法比较时按不使用可靠时刻处理，不递增版本
                    confirmed = False
            if confirmed:
                self._watermark_version += 1
                offset = {**offset, "version": self._watermark_version}
            else:
                keep_version = (previous or {}).get("version", self._watermark_version)
                offset = {**offset, "version": keep_version}
            self._watermark_offsets[partition] = offset

    def get_watermark(self, partition: str) -> dict[str, Any] | None:
        return self._watermark_offsets.get(partition)

    def watermark_progress(self, partition: str) -> dict[str, Any]:
        """读取一个自洽的消费进度快照：确认时刻与确认版本同时定格。

        返回 {offset, version, ts, confirmed}。缺少可靠确认时刻时 confirmed=False，
        调用方不得据此把任何行判成迟到或丢弃。
        """
        with self._store_lock:
            data = self._watermark_offsets.get(partition) or {}
            offset = data.get("offset")
            try:
                offset_val = float(offset)
                reliable = offset_val == offset_val  # 排除 NaN
            except (TypeError, ValueError):
                reliable = False
            return {
                "offset": offset if reliable else None,
                "version": data.get("version", self._watermark_version),
                "last_update": data.get("ts"),
                "confirmed": reliable,
                "has_record": bool(data),
            }

    # ---------- 迟到事件清单 / 侧输出 / 重判 ----------
    def next_late_event_id(self) -> int:
        with self._store_lock:
            self._late_seq += 1
            return self._late_seq

    def next_late_batch_id(self, entry_id: int) -> str:
        with self._store_lock:
            self._late_batch_seq += 1
            return f"LATE_{entry_id}_{self._late_batch_seq}"

    def next_rejudge_id(self) -> int:
        with self._store_lock:
            self._late_rejudge_seq += 1
            return self._late_rejudge_seq

    def late_events_for_policy(self, entry_id: int) -> list[dict[str, Any]]:
        return [e for e in self._late_events if e.get("policy_id") == entry_id]

    def find_late_event(self, event_id: str, policy_id: int | None = None) -> dict[str, Any] | None:
        for event in self._late_events:
            if event.get("event_id") != event_id:
                continue
            if policy_id is not None and event.get("policy_id") != policy_id:
                continue
            return event
        return None

    def record_late_event(self, record: dict[str, Any]) -> dict[str, Any]:
        self._late_events.append(record)
        return record

    def side_output_for_policy(self, entry_id: int) -> list[dict[str, Any]]:
        return [
            row for row in self._late_side_output
            if row.get("policy_id") == entry_id and row.get("active")
        ]

    def all_side_output(self) -> list[dict[str, Any]]:
        return [row for row in self._late_side_output if row.get("active")]

    def sync_side_output(self, entry_id: int, rows: list[dict[str, Any]]) -> None:
        """把某次判定的迟到行写进侧输出，并让不再迟到的旧行失效。

        失效行不删除，保留原判定时刻与失效时间，便于复核"曾经进过侧输出"的记录。
        """
        with self._store_lock:
            keep_ids = {row["event_id"] for row in rows}
            for existing in self._late_side_output:
                if existing.get("policy_id") != entry_id or not existing.get("active"):
                    continue
                if existing.get("event_id") not in keep_ids:
                    existing["active"] = False
                    existing["invalidated_at"] = time.time()
            for row in rows:
                for existing in self._late_side_output:
                    if existing.get("event_id") == row["event_id"]:
                        existing.update(row)
                        existing["active"] = True
                        existing.pop("invalidated_at", None)
                        break
                else:
                    self._late_side_output.append({**row, "active": True})

    def save_rejudge(self, record: dict[str, Any]) -> dict[str, Any]:
        self._late_rejudges[record["rejudge_id"]] = record
        return record

    def find_rejudge(self, entry_id: int) -> dict[str, Any] | None:
        matches = [r for r in self._late_rejudges.values() if r.get("policy_id") == entry_id]
        return max(matches, key=lambda r: r["rejudge_id"]) if matches else None

    def all_rejudges(self) -> list[dict[str, Any]]:
        return [self._late_rejudges[key] for key in sorted(self._late_rejudges)]

    def remember_late_input(self, entry_id: int, events: list[dict[str, Any]]) -> None:
        self._late_inputs[entry_id] = [dict(event) for event in events]

    def last_late_input(self, entry_id: int) -> list[dict[str, Any]]:
        return [dict(event) for event in self._late_inputs.get(entry_id, [])]

    # ---------- 快照存储 ----------
    def save_snapshot(self, snap_id: str, data: dict[str, Any]) -> None:
        self._snapshot_store[snap_id] = data

    def get_snapshot(self, snap_id: str) -> dict[str, Any] | None:
        return self._snapshot_store.get(snap_id)

    # ---------- 变更日志 ----------
    def append_changelog(self, entry: dict[str, Any]) -> None:
        entry["_ts"] = time.time()
        self._global_changelog.append(entry)

    def changelog_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [e for e in self._global_changelog if e.get("目标表") == table_name]

    # ---------- 血缘图 ----------
    def add_lineage_edge(self, upstream: str, downstream: str, edge_data: dict[str, Any]) -> None:
        edges = self._lineage_graph.setdefault(upstream, [])
        edges.append({"downstream": downstream, **edge_data, "ts": time.time()})

    def lineage_downstream(self, table_name: str) -> list[dict[str, Any]]:
        return self._lineage_graph.get(table_name, [])

    # ---------- 归档清单 ----------
    def add_archive_record(self, record: dict[str, Any]) -> None:
        self._archive_manifest.append(record)

    def archive_manifest_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [r for r in self._archive_manifest if r.get("源表名称") == table_name]


store = Store()