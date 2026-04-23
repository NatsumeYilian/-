"""从请求中解析 traffic 表过滤条件，生成 SQL 片段与参数。"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from flask import Request


def traffic_filter_sql(
    alias: str, req: Request, json_body: Optional[Dict[str, Any]] = None
) -> Tuple[str, List[Any]]:
    """
    返回 (\" AND ...\", [params])，可接在 WHERE 1=1 之后。
    alias: 表别名，如 't'
    """
    parts: List[str] = []
    params: List[Any] = []
    jb = json_body or {}

    def get(k: str) -> str | None:
        v = req.args.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
        if k in jb and jb[k] is not None and str(jb[k]).strip() != "":
            return str(jb[k]).strip()
        return None

    road = get("road_name")
    if road:
        parts.append(f"{alias}.road_name LIKE %s")
        params.append("%" + road + "%")

    area = get("area")
    if area:
        parts.append(f"{alias}.area LIKE %s")
        params.append("%" + area + "%")

    df = get("date_from")
    if df:
        parts.append(f"{alias}.date >= %s")
        params.append(df)

    dt = get("date_to")
    if dt:
        parts.append(f"{alias}.date <= %s")
        params.append(dt)

    if not parts:
        return "", []
    return " AND " + " AND ".join(parts), params
