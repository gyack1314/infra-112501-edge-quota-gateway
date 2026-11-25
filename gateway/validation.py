"""Validation and natural-key helpers."""

from __future__ import annotations

from typing import Any

from .errors import ValidationError

MISSING = object()


def require_int(data: dict[str, Any], key: str, *, minimum: int = 0, required: bool = True) -> int | None:
    value = data.get(key, MISSING)
    if value is MISSING or value is None:
        if required:
            raise ValidationError(f"缺少整数字段 {key}", details={"field": key})
        return None
    if type(value) is not int or isinstance(value, bool):
        raise ValidationError(f"字段 {key} 必须是整数", details={"field": key, "value": value})
    if value < minimum:
        raise ValidationError(
            f"字段 {key} 不能小于 {minimum}", details={"field": key, "value": value, "minimum": minimum}
        )
    return value


def require_str(data: dict[str, Any], key: str, *, required: bool = True) -> str | None:
    value = data.get(key, MISSING)
    if value is MISSING or value is None:
        if required:
            raise ValidationError(f"缺少字符串字段 {key}", details={"field": key})
        return None
    if not isinstance(value, str) or not value:
        raise ValidationError(f"字段 {key} 必须是非空字符串", details={"field": key, "value": value})
    return value


def optional_use(data: dict[str, Any]) -> str | None:
    value = data.get("use")
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValidationError("use 必须是字符串", details={"field": "use", "value": value})
    return value


def fingerprint(maximum: int | None, use: str | None) -> dict[str, Any]:
    return {"maximum": maximum, "use": use}


def ensure_window(starts: int, ends: int) -> None:
    if starts >= ends:
        raise ValidationError("时间窗口必须满足 starts < ends", details={"starts": starts, "ends": ends})
