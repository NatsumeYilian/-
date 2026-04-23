from __future__ import annotations

import io
import os
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from db_utils import BASE_DIR, MYSQL_DATABASE, get_connection, init_db

# pandas 仅在 load_traffic 内导入，避免 Flask 启动时依赖 numpy/pandas


# 交通流量 Excel 文件查找顺序：
#   1) 环境变量 TRAFFIC_XLSX 指定的绝对路径
#   2) BASE_DIR/traffic_data.xlsx（通用默认文件名）
#   3) BASE_DIR 下任意 .xlsx 文件（取第一个）
_CANDIDATE_XLSX_NAMES = [
    "traffic_data.xlsx",
    "城市道路_逐小时_车辆分类.xlsx",
]


def _resolve_traffic_xlsx() -> str:
    env_path = os.environ.get("TRAFFIC_XLSX")
    if env_path and os.path.isfile(env_path):
        return env_path
    for name in _CANDIDATE_XLSX_NAMES:
        p = os.path.join(BASE_DIR, name)
        if os.path.isfile(p):
            return p
    # 兜底：扫描 BASE_DIR 下的 .xlsx 文件（排除以 ~$ 开头的临时文件）
    try:
        for fn in sorted(os.listdir(BASE_DIR)):
            if fn.lower().endswith(".xlsx") and not fn.startswith("~$"):
                return os.path.join(BASE_DIR, fn)
    except OSError:
        pass
    return os.path.join(BASE_DIR, _CANDIDATE_XLSX_NAMES[0])


TRAFFIC_XLSX = _resolve_traffic_xlsx()


def _guess_time_columns(df: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    由于当前 Excel 为固定格式：
    第 1 列为日期，第 2 列为时间（小时），这里直接按列位置返回，
    避免中文列名编码造成的匹配错误。
    """
    date_col = df.columns[0] if len(df.columns) >= 1 else None
    hour_col = df.columns[1] if len(df.columns) >= 2 else None
    return date_col, hour_col


def _guess_dimension_columns(df: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    road_name = None
    road_type = None
    area = None
    for c in df.columns:
        name = str(c)
        if road_name is None and any(k in name for k in ["道路", "路名", "road", "Road"]):
            road_name = c
        if road_type is None and any(k in name for k in ["类型", "道路等级", "road_type", "RoadType"]):
            road_type = c
        if area is None and any(k in name for k in ["区域", "片区", "zone", "area", "Area"]):
            area = c
    return road_name, road_type, area


def load_traffic() -> Dict[str, Any]:
    import pandas as pd

    if not os.path.exists(TRAFFIC_XLSX):
        raise FileNotFoundError("未找到交通流量数据文件（请检查文件名和目录）")

    df = pd.read_excel(TRAFFIC_XLSX)
    source_rows = int(len(df))

    date_col, hour_col = _guess_time_columns(df)
    road_name_col, road_type_col, area_col = _guess_dimension_columns(df)

    # ---- 优先按列名识别“平均车速/拥堵指数”，避免依赖固定列序 ----
    def pick_col_by_keywords(keywords):
        for c in df.columns:
            name = str(c)
            if any(k in name for k in keywords):
                return c
        return None

    speed_col_kw = pick_col_by_keywords(["平均车速", "车速", "速度", "avg_speed", "speed", "Speed"])
    congestion_col_kw = pick_col_by_keywords(["拥堵指数", "拥堵", "指数", "congestion", "index", "Index"])

    # 自动识别车辆数量列：数值类型且不是时间/维度列
    # 注意：车速/拥堵指数是指标列，不应计入 total_flow 的求和
    exclude_cols = {date_col, hour_col, road_name_col, road_type_col, area_col, speed_col_kw, congestion_col_kw}
    numeric_cols = [c for c in df.columns if c not in exclude_cols and pd.api.types.is_numeric_dtype(df[c])]

    if not numeric_cols:
        raise ValueError("在交通流量表中未找到数值型流量列，请检查 Excel 文件结构。")

    if date_col:
        # 有些 Excel 可能在日期列中掺杂说明文字，如“（内容由AI生成，仅供参考）”
        # 使用 errors='coerce' 将无法解析的日期转为 NaT，并整体过滤掉
        date_series = pd.to_datetime(df[date_col], errors="coerce")
        mask = ~date_series.isna()
        df = df[mask].copy()
        date_series = date_series[mask]
        df["date"] = date_series.dt.strftime("%Y-%m-%d")
    else:
        df["date"] = datetime.today().strftime("%Y-%m-%d")

    if hour_col:
        # 小时列可能是数字或字符串，例如 0-23 或 "00:00-01:00" 等
        def parse_hour(v):
            # 先处理纯数字情况，如果在 0-23 之间直接返回
            try:
                iv = int(v)
                if 0 <= iv <= 23:
                    return iv
            except Exception:
                iv = None

            s = str(v)
            for sep in ["-", "～", "~"]:
                if sep in s:
                    s = s.split(sep)[0]
                    break
            s = s.strip()
            if ":" in s:
                # 形如 "07:00" / "7:30"
                try:
                    return int(s.split(":")[0])
                except Exception:
                    pass
            try:
                iv2 = int(s)
                # 如果是像 737 这种，取前两位近似为小时
                if iv2 > 23:
                    return int(str(iv2)[:2])
                return iv2
            except Exception:
                return 0

        df["hour"] = df[hour_col].apply(parse_hour)
    else:
        df["hour"] = 0

    # 再次保证小时在 0-23 之间
    df["hour"] = df["hour"].astype(int)
    df.loc[(df["hour"] < 0) | (df["hour"] > 23), "hour"] = df["hour"] % 24

    df["ts"] = pd.to_datetime(df["date"] + " " + df["hour"].astype(str) + ":00:00", errors="coerce")
    # 丢弃无法构造时间戳的记录
    df = df[df["ts"].notna()].copy()
    valid_rows = int(len(df))

    df["total_flow"] = df[numeric_cols].sum(axis=1)

    # 尝试拆分几类车辆，如果不存在则为 NULL
    def pick_first_matching(keywords):
        for c in numeric_cols:
            name = str(c)
            if any(k in name for k in keywords):
                return c
        return None

    small_col = pick_first_matching(["小客", "小型车", "客车", "small", "car"])
    large_col = pick_first_matching(["大客", "公交", "coach", "bus"])
    truck_col = pick_first_matching(["货车", "truck", "货运"])

    df["small_vehicle"] = df[small_col] if small_col in df.columns else None
    df["large_vehicle"] = df[large_col] if large_col in df.columns else None
    df["truck"] = df[truck_col] if truck_col in df.columns else None

    df["road_name"] = df[road_name_col] if road_name_col in df.columns else "未知道路"
    df["road_type"] = df[road_type_col] if road_type_col in df.columns else "未分类"
    df["area"] = df[area_col] if area_col in df.columns else "本地"

    # 平均车速 / 拥堵指数：优先按列名关键词，其次再尝试按列序兜底
    speed_col = speed_col_kw
    congestion_col = congestion_col_kw
    if speed_col is None and len(df.columns) > 4:
        speed_col = df.columns[4]
    if congestion_col is None and len(df.columns) > 5:
        congestion_col = df.columns[5]

    df["avg_speed"] = pd.to_numeric(df[speed_col], errors="coerce") if speed_col is not None and speed_col in df.columns else None
    df["congestion_index"] = pd.to_numeric(df[congestion_col], errors="coerce") if congestion_col is not None and congestion_col in df.columns else None

    records = [
        (
            row["ts"].strftime("%Y-%m-%d %H:%M:%S"),
            row["date"],
            int(row["hour"]),
            str(row["road_name"]),
            str(row["road_type"]),
            str(row["area"]),
            float(row["total_flow"]),
            float(row["small_vehicle"]) if row["small_vehicle"] is not None else None,
            float(row["large_vehicle"]) if row["large_vehicle"] is not None else None,
            float(row["truck"]) if row["truck"] is not None else None,
            float(row["avg_speed"]) if row["avg_speed"] is not None and pd.notna(row["avg_speed"]) else None,
            float(row["congestion_index"]) if row["congestion_index"] is not None and pd.notna(row["congestion_index"]) else None,
        )
        for _, row in df.iterrows()
    ]

    conn = get_connection()
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO traffic
        (ts, date, hour, road_name, road_type, area, total_flow, small_vehicle, large_vehicle, truck, avg_speed, congestion_index)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
        records,
    )
    conn.commit()
    conn.close()
    road_names = sorted({str(x) for x in df["road_name"].dropna().astype(str).tolist() if str(x).strip()})
    return {
        "source_rows": source_rows,
        "valid_rows": valid_rows,
        "inserted_rows": len(records),
        "date_from": str(df["date"].min()) if valid_rows else "",
        "date_to": str(df["date"].max()) if valid_rows else "",
        "hour_min": int(df["hour"].min()) if valid_rows else 0,
        "hour_max": int(df["hour"].max()) if valid_rows else 0,
        "road_count": len(road_names),
        "sample_roads": road_names[:5],
    }


def load_all() -> Dict[str, Any]:
    init_db()
    return load_traffic()


def run_cleaning_pipeline() -> Tuple[bool, List[str]]:
    """
    手动执行数据清洗：初始化表结构 → 清空 traffic → 全量导入。
    返回 (是否成功, 日志行列表)，供前端展示。
    """
    lines: List[str] = []

    def log(msg: str) -> None:
        lines.append(msg)

    try:
        log("【步骤 1/3】初始化 MySQL 表结构（traffic / users 等）…")
        init_db()
        log("  → 完成：表结构就绪。")

        log("【步骤 2/3】清空 traffic 并重新从原始 Excel 导入…")
        log(f"  （将读取 Excel：{os.path.basename(TRAFFIC_XLSX)}）")
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM traffic;")
        conn.commit()
        conn.close()
        log("  → 已清空旧业务数据（users 表保留）。")

        buf = io.StringIO()
        summary: Dict[str, Any] = {}
        with redirect_stdout(buf), redirect_stderr(buf):
            summary = load_all()
        extra = buf.getvalue()
        if extra.strip():
            for ln in extra.strip().splitlines():
                lines.append("  | " + ln)

        log("【清洗内容】")
        log(f"  原始记录数: {summary.get('source_rows', 0)}")
        log(f"  有效记录数: {summary.get('valid_rows', 0)}")
        log(f"  入库记录数: {summary.get('inserted_rows', 0)}")
        log(f"  日期范围: {summary.get('date_from', '')} ~ {summary.get('date_to', '')}")
        log(f"  小时范围: {summary.get('hour_min', 0)} ~ {summary.get('hour_max', 0)}")
        log(f"  道路数量: {summary.get('road_count', 0)}")
        sample_roads = summary.get("sample_roads", [])
        if sample_roads:
            log("  示例道路: " + "、".join(sample_roads))

        log("【步骤 3/3】校验数据库文件写入…")
        log("  → 完成：清洗与导入流程结束。")
        return True, lines
    except FileNotFoundError as e:
        log("错误：找不到数据文件 — " + str(e))
        return False, lines
    except Exception as e:
        log("错误：执行异常 — " + str(e))
        return False, lines


if __name__ == "__main__":
    ok, logs = run_cleaning_pipeline()
    for line in logs:
        print(line)
    if ok:
        print(f"结果：成功写入 MySQL 数据库（{MYSQL_DATABASE}）")
    else:
        print("结果：清洗失败，请检查日志。")
