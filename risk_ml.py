"""
拥堵风险预测 — 机器学习：随机森林多分类（RandomForestClassifier）

- 算法：sklearn.ensemble.RandomForestClassifier（集成决策树，对特征非线性组合建模）
- 训练：使用与业务规则一致的自动标注作为监督标签（弱监督），在特征空间上学习；
- 特征：小时、星期、车流量、拥堵指数、平均车速、是否高峰、是否主干道等。
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Sequence, Tuple

import numpy as np

try:
    from sklearn.ensemble import RandomForestClassifier

    _HAS_SKLEARN = True
except ImportError:
    _HAS_SKLEARN = False

LEVELS = ["低风险", "中风险", "高风险"]
NAME_TO_CODE = {"低风险": 0, "中风险": 1, "高风险": 2}
CODE_TO_NAME = {0: "低风险", 1: "中风险", 2: "高风险"}


def _rule_classify(row: dict) -> Tuple[str, int]:
    """
    规则基线：多维加权打分，用于生成训练标签及无 sklearn 时的回退。
    维度：流量 / 拥堵指数 / 车速 / 时段 / 道路等级 / 星期。
    """
    ts = datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S")
    hour = ts.hour
    weekday = ts.weekday()  # 0=周一 … 6=周日
    flow = float(row.get("total_flow") or 0)
    road_type = row.get("road_type") or ""
    ci = row.get("congestion_index")
    sp = row.get("avg_speed")
    ci_val = float(ci) if ci is not None else None
    sp_val = float(sp) if sp is not None else None

    score = 0

    # 1) 流量维度（阈值下调、权重提高，避免大量样本卡在中低档）
    if flow > 700:
        score += 4
    elif flow > 400:
        score += 3
    elif flow > 250:
        score += 2
    elif flow > 120:
        score += 1

    # 2) 拥堵指数（常见范围 0~10）
    if ci_val is not None:
        if ci_val >= 7:
            score += 4
        elif ci_val >= 5:
            score += 3
        elif ci_val >= 3:
            score += 2
        elif ci_val >= 1.5:
            score += 1

    # 3) 平均车速（越慢越堵）
    if sp_val is not None and sp_val > 0:
        if sp_val < 15:
            score += 3
        elif sp_val < 25:
            score += 2
        elif sp_val < 35:
            score += 1

    # 4) 时段：早高峰/晚高峰、夜间
    if 7 <= hour <= 9 or 17 <= hour <= 19:
        score += 2
    elif 10 <= hour <= 16 or 20 <= hour <= 22:
        score += 1

    # 5) 道路等级
    if any(k in road_type for k in ["主干", "主路", "arterial", "快速", "高速"]):
        score += 2
    elif any(k in road_type for k in ["次干", "次路", "secondary"]):
        score += 1

    # 6) 工作日相对周末更易拥堵
    if weekday < 5 and (7 <= hour <= 9 or 17 <= hour <= 19):
        score += 1

    # 阈值：考虑到满分可到 ~16，这里把高风险门槛设为 7，中风险 4
    if score >= 7:
        level = "高风险"
    elif score >= 4:
        level = "中风险"
    else:
        level = "低风险"
    return level, score


def _row_features(row: dict) -> np.ndarray:
    ts = datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S")
    hour = float(ts.hour)
    wd = float(ts.weekday())
    flow = float(row["total_flow"] or 0)
    ci = row.get("congestion_index")
    sp = row.get("avg_speed")
    ci = float(ci) if ci is not None else 0.0
    sp = float(sp) if sp is not None else 0.0
    road_type = row.get("road_type") or ""

    is_peak = 1.0 if (7 <= ts.hour <= 9 or 17 <= ts.hour <= 19) else 0.0
    is_main = 1.0 if any(k in road_type for k in ["主干", "主路", "arterial"]) else 0.0

    return np.array(
        [hour, wd, flow, ci, sp, is_peak, is_main],
        dtype=np.float64,
    )


def _weighted_score_from_proba(proba: np.ndarray, classes: np.ndarray) -> np.ndarray:
    """将各类概率映射为排序用分值。"""
    name_to_w = {"低风险": 2.0, "中风险": 5.0, "高风险": 9.0}
    weights = np.zeros(proba.shape[1], dtype=np.float64)
    for j, c in enumerate(classes):
        label = CODE_TO_NAME.get(int(c), "中风险")
        weights[j] = name_to_w.get(label, 4.0)
    return np.sum(proba * weights, axis=1)


def predict_risk_ml(rows: Sequence[dict]) -> Tuple[List[dict], str]:
    """
    对 rows 每条记录写入 risk_level、risk_score；返回 (rows 原地已更新, 算法说明字符串)。
    """
    rows = list(rows)
    n = len(rows)
    algo = "规则引擎（弱监督标注）"

    if n == 0:
        return rows, algo

    rule_results = [_rule_classify(r) for r in rows]

    if not _HAS_SKLEARN or n < 20:
        for i, r in enumerate(rows):
            lvl, sc = rule_results[i]
            r["risk_level"] = lvl
            r["risk_score"] = sc
        return rows, "规则引擎（样本过少或未安装 scikit-learn，未启用随机森林）"

    X = np.stack([_row_features(r) for r in rows])
    y_names = [rule_results[i][0] for i in range(n)]
    y = np.array([NAME_TO_CODE.get(nm, 1) for nm in y_names], dtype=np.int64)

    uniq = np.unique(y)
    if uniq.size < 2:
        for i, r in enumerate(rows):
            lvl, sc = rule_results[i]
            r["risk_level"] = lvl
            r["risk_score"] = sc
        return rows, "规则引擎（标签仅一类，随机森林跳过）"

    clf = RandomForestClassifier(
        n_estimators=120,
        max_depth=16,
        min_samples_leaf=max(2, min(20, n // 50)),
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    clf.fit(X, y)
    pred = clf.predict(X)
    proba = clf.predict_proba(X)
    scores = _weighted_score_from_proba(proba, clf.classes_)

    for i, r in enumerate(rows):
        code = int(pred[i])
        r["risk_level"] = CODE_TO_NAME.get(code, "中风险")
        r["risk_score"] = round(float(scores[i]), 2)

    algo = (
        "RandomForestClassifier（随机森林，n_estimators=120）："
        "以规则自动标注为训练标签，特征含小时/星期/流量/拥堵指数/车速/高峰/主干道"
    )
    return rows, algo
