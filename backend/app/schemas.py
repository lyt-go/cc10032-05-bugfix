"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None
    # 动作参数（如迟到重判所选确认时刻依据），领域服务按需读取。
    basis: dict[str, Any] = Field(default_factory=dict)


class LateClassifyPayload(BaseModel):
    # 留空时复用该策略最近一次判定的事件输入（供进度补到后复核）
    events: list[dict[str, Any]] = Field(default_factory=list)


class LateRejudgePayload(BaseModel):
    # 值班员当次选择的确认时刻依据；`time_basis` 与 `confirmed_at` 等价。
    confirmed_at: float | None = None
    time_basis: float | None = None
    operator: str | None = None
    note: str = ""
