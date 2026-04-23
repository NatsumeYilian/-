import os
from datetime import datetime
from functools import wraps

from flask import (
    Flask,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from auth_models import (
    create_user,
    delete_user,
    ensure_default_admin,
    get_user_by_id,
    init_users_table,
    list_users,
    set_user_role,
    update_password,
    update_profile,
    verify_login,
)
from db_utils import BASE_DIR, get_connection, init_db
from data_cleaning import load_all, run_cleaning_pipeline
from risk_ml import predict_risk_ml
from sql_helpers import traffic_filter_sql


def ensure_db():
    """
    确保数据库和基础数据已准备好：
    - 每次启动先执行 init_db()，保证表结构存在
    - 若 traffic 表为空或指标全空，再执行全量 Excel 导入
    """
    init_db()

    need_reload = False
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM traffic;")
        row = cur.fetchone()
        total_cnt = int(row["c"]) if row else 0

        if total_cnt > 0:
            cur.execute(
                "SELECT COUNT(*) AS c FROM traffic WHERE avg_speed IS NOT NULL AND avg_speed > 0;"
            )
            row2 = cur.fetchone()
            speed_ok = int(row2["c"]) if row2 else 0
            cur.execute(
                "SELECT COUNT(*) AS c FROM traffic WHERE congestion_index IS NOT NULL AND congestion_index > 0;"
            )
            row3 = cur.fetchone()
            idx_ok = int(row3["c"]) if row3 else 0
            if speed_ok == 0 and idx_ok == 0:
                need_reload = True

        conn.close()
        if total_cnt == 0:
            need_reload = True
    except Exception:
        need_reload = True

    if need_reload:
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("DELETE FROM traffic;")
            conn.commit()
            conn.close()
        except Exception:
            pass
        load_all()

    init_users_table()
    ensure_default_admin()


app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=os.path.join(BASE_DIR, "static"),
)
app.secret_key = os.environ.get("SECRET_KEY", "dev-traffic-screen-secret-change-me")


@app.before_request
def screen_only_mode():
    # 仅保留数据大屏：屏蔽原系统页面入口，统一跳转到大屏。
    path = request.path or "/"
    if path in ("/", "/screen"):
        return None
    if path.startswith("/api/") or path.startswith("/static/"):
        return None
    if path == "/favicon.ico":
        return None
    return redirect(url_for("dashboard_screen"))


@app.context_processor
def inject_current_user():
    uid = session.get("user_id")
    if not uid:
        return {"current_user": None, "display_name": ""}
    u = get_user_by_id(int(uid))
    if not u:
        return {"current_user": None, "display_name": ""}
    display_name = (u.get("nickname") or "").strip() or (u.get("username") or "")
    return {"current_user": u, "display_name": display_name}


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("user_id"):
            if request.path.startswith("/api/"):
                return jsonify({"error": "未登录", "login": url_for("login")}), 401
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)

    return wrapped


def admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if session.get("role") != "admin":
            abort(403)
        return view(*args, **kwargs)

    return wrapped


@app.route("/")
def home():
    return redirect(url_for("dashboard_screen"))


@app.route("/screen")
def dashboard_screen():
    return render_template("dashboard_screen.html", title="交通流量数据分析大屏")


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("user_id"):
        return redirect(url_for("home"))
    next_url = request.args.get("next") or url_for("home")
    if request.method == "POST":
        username = request.form.get("username") or ""
        password = request.form.get("password") or ""
        u = verify_login(username, password)
        if not u:
            flash("用户名或密码错误", "danger")
        else:
            session["user_id"] = u["id"]
            session["username"] = u["username"]
            session["nickname"] = (u.get("nickname") or u["username"]).strip()
            session["role"] = u["role"]
            return redirect(request.form.get("next") or next_url)
    return render_template("login.html", title="登录", next=next_url)


@app.route("/register", methods=["GET", "POST"])
def register():
    if session.get("user_id"):
        return redirect(url_for("home"))
    if request.method == "POST":
        ok, err = create_user(
            request.form.get("username") or "",
            request.form.get("password") or "",
            request.form.get("email") or "",
            request.form.get("nickname") or "",
        )
        if ok:
            flash("注册成功，请登录", "success")
            return redirect(url_for("login"))
        flash(err, "danger")
    return render_template("register.html", title="注册")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("home"))


@app.route("/profile", methods=["GET", "POST"])
@login_required
def profile():
    uid = int(session["user_id"])
    u = get_user_by_id(uid)
    if not u:
        session.clear()
        return redirect(url_for("login"))
    if request.method == "POST":
        act = request.form.get("action")
        if act == "info":
            update_profile(uid, request.form.get("email") or "", request.form.get("nickname") or "")
            session["nickname"] = (request.form.get("nickname") or "").strip() or session.get("username", "")
            flash("资料已保存", "success")
        elif act == "password":
            p1 = request.form.get("password") or ""
            p2 = request.form.get("password2") or ""
            if len(p1) < 6:
                flash("新密码至少 6 位", "danger")
            elif p1 != p2:
                flash("两次密码不一致", "danger")
            else:
                update_password(uid, p1)
                flash("密码已更新", "success")
        return redirect(url_for("profile"))
    u = get_user_by_id(uid)
    return render_template("profile.html", title="个人资料", user=u)


@app.route("/admin/users", methods=["GET", "POST"])
@login_required
@admin_required
def admin_users():
    q_username = (request.args.get("username") or "").strip()
    q_email = (request.args.get("email") or "").strip()
    q_role = (request.args.get("role") or "").strip()
    if request.method == "POST":
        act = request.form.get("action")
        uid = request.form.get("user_id", type=int)
        if act == "delete" and uid and uid != int(session["user_id"]):
            delete_user(uid)
            flash("已删除用户", "success")
        elif act == "role" and uid:
            set_user_role(uid, request.form.get("role") or "user")
            flash("角色已更新", "success")
        return redirect(
            url_for("admin_users", username=q_username, email=q_email, role=q_role)
        )
    users = list_users(q_username, q_email, q_role)
    return render_template(
        "admin_users.html",
        title="用户管理",
        users=users,
        q={"username": q_username, "email": q_email, "role": q_role},
    )


@app.route("/algorithm")
@login_required
def algorithm_page():
    return render_template("algorithm.html", title="拥堵风险预测")


@app.route("/analysis/<name>")
@login_required
def analysis_page(name):
    titles = {
        "flow": "日交通流量走势",
        "vehicle": "车型流量占比",
        "heatmap": "高峰拥堵风险热力图",
        "speed": "平均车速趋势图",
        "duration": "分时段拥堵时长对比",
        "congestion": "拥堵指数趋势图",
    }
    if name not in titles:
        abort(404)
    return render_template(
        "analysis.html",
        title=titles[name],
        chart_key=name,
    )


@app.route("/data/traffic")
@login_required
def data_traffic_page():
    return render_template("data_traffic.html", title="数据明细")


@app.route("/tools/cleaning")
def tools_cleaning_redirect():
    """旧地址重定向到数据导入。"""
    return redirect(url_for("tools_import"))


@app.route("/tools/import")
@login_required
@admin_required
def tools_import():
    auto_run = request.args.get("auto", "0") == "1"
    return render_template("tools_cleaning.html", title="数据清洗入库", auto_run=auto_run)


@app.route("/tools/import/run", methods=["POST"])
@login_required
@admin_required
def tools_import_run():
    ok, lines = run_cleaning_pipeline()
    return jsonify({"ok": ok, "log": lines})


def _query(sql: str, params=None):
    if params is None:
        params = ()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.route("/api/home_stats")
def api_home_stats():
    """首页顶部悬浮 KPI 数据：记录总数、总流量、道路/区域计数、日期范围。"""
    try:
        rows = _query(
            """
            SELECT
                COUNT(*) AS record_count,
                COALESCE(SUM(total_flow), 0) AS total_flow,
                COUNT(DISTINCT road_name) AS road_count,
                COUNT(DISTINCT area) AS area_count,
                MIN(date) AS date_from,
                MAX(date) AS date_to
            FROM traffic;
            """
        )
        r = rows[0] if rows else {}
        return jsonify({
            "record_count": int(r.get("record_count") or 0),
            "total_flow": float(r.get("total_flow") or 0),
            "road_count": int(r.get("road_count") or 0),
            "area_count": int(r.get("area_count") or 0),
            "date_from": r.get("date_from") or "",
            "date_to": r.get("date_to") or "",
        })
    except Exception:
        return jsonify({
            "record_count": 0,
            "total_flow": 0,
            "road_count": 0,
            "area_count": 0,
            "date_from": "",
            "date_to": "",
        })


@app.route("/api/summary")
def api_summary():
    sql = """
    SELECT
        COUNT(*) AS record_count,
        SUM(total_flow) AS total_flow,
        AVG(total_flow) AS avg_flow,
        MAX(total_flow) AS max_hour_flow
    FROM traffic;
    """
    rows = _query(sql)
    summary = rows[0] if rows else {}
    return jsonify(summary)


@app.route("/api/time_trend")
def api_time_trend():
    # 时间维度：按日期统计总流量（仅返回最近 30 天，按日期升序）
    sql = """
    SELECT date, total_flow
    FROM (
        SELECT date, SUM(total_flow) AS total_flow
        FROM traffic
        GROUP BY date
        ORDER BY date DESC
        LIMIT 30
    ) t
    ORDER BY date;
    """
    rows = _query(sql)
    return jsonify(rows)


# ---------- 新大屏 6 图 API ----------

@app.route("/api/flow_temp_by_month")
def api_flow_temp_by_month():
    """1. 日交通流量走势（多月份切换）：横轴24小时，按小时平均流量，支持1-6月"""
    month = request.args.get("month", type=int, default=1)
    if month is None or month < 1 or month > 6:
        month = 1
    extra_sql, extra_params = traffic_filter_sql("t", request)
    sql = """
    SELECT
        t.hour,
        AVG(t.total_flow) AS flow
    FROM traffic t
    WHERE MONTH(STR_TO_DATE(t.ts, '%%Y-%%m-%%d %%H:%%i:%%s')) = %s
    """ + extra_sql + """
    GROUP BY t.hour
    ORDER BY t.hour;
    """
    rows = _query(sql, (month,) + tuple(extra_params))
    hours = [r["hour"] for r in rows]
    flow = [round(float(r["flow"] or 0), 2) for r in rows]
    return jsonify({"month": month, "hours": hours, "flow": flow})


@app.route("/api/vehicle_type_ratio")
def api_vehicle_type_ratio():
    """2. 车型流量占比：私家车、货车、其他；公交仅在合计>0时返回"""
    extra_sql, extra_params = traffic_filter_sql("t", request)
    sql = """
    SELECT
        COALESCE(SUM(small_vehicle), 0) AS private,
        COALESCE(SUM(large_vehicle), 0) AS bus,
        COALESCE(SUM(truck), 0) AS truck,
        COALESCE(SUM(total_flow), 0) AS total
    FROM traffic t
    WHERE 1=1
    """ + extra_sql + """
    """
    rows = _query(sql, tuple(extra_params))
    r = rows[0] if rows else {}
    total = float(r.get("total") or 0)
    private = float(r.get("private") or 0)
    bus = float(r.get("bus") or 0)
    truck = float(r.get("truck") or 0)
    other = max(0, total - private - bus - truck)
    payload = {
        "私家车": round(private, 2),
        "货车": round(truck, 2),
        "其他": round(other, 2),
    }
    if bus > 0:
        payload["公交"] = round(bus, 2)
    return jsonify(payload)


def _risk_heatmap_payload(request):
    """高峰拥堵风险热力图数据：仅最新 7 天；横轴 24 小时，纵轴日期。"""
    extra_sql, extra_params = traffic_filter_sql("traffic", request)
    sql = """
    SELECT date, hour,
           AVG(COALESCE(congestion_index, total_flow / 100.0)) AS intensity
    FROM traffic
    WHERE 1=1
    """ + extra_sql + """
    GROUP BY date, hour
    ORDER BY date, hour;
    """
    rows = _query(sql, tuple(extra_params))
    dates = sorted(set(r["date"] for r in rows))
    if len(dates) > 7:
        dates = dates[-7:]
    date_set = set(dates)
    hours = list(range(24))
    by_date = {}
    for r in rows:
        d, h = r["date"], r["hour"]
        if d not in date_set:
            continue
        v = round(float(r["intensity"] or 0), 2)
        if d not in by_date:
            by_date[d] = {}
        by_date[d][h] = v
    data = []
    for d in dates:
        data.append([by_date.get(d, {}).get(h, 0) for h in hours])
    return {"dates": dates, "hours": hours, "data": data}


@app.route("/api/risk_heatmap")
def api_risk_heatmap():
    """3. 高峰拥堵风险热力图：仅最新 7 天；横轴 24 小时，纵轴日期，颜色=拥堵强度"""
    payload = _risk_heatmap_payload(request)
    return jsonify(payload)


@app.route("/api/speed_temp_hourly")
def api_speed_temp_hourly():
    """4. 平均车速趋势图：横轴24小时，按小时平均车速，支持按月份或半年对比"""
    month = request.args.get("month", type=int)  # 空=全部半年
    if month is not None and (month < 1 or month > 6):
        month = None

    extra_sql, extra_params = traffic_filter_sql("t", request)
    if month is not None:
        where = "WHERE MONTH(STR_TO_DATE(t.ts, '%%Y-%%m-%%d %%H:%%i:%%s')) = %s" + extra_sql
        params = (month,) + tuple(extra_params)
    else:
        where = "WHERE 1=1" + extra_sql
        params = tuple(extra_params)

    sql = f"""
    SELECT t.hour,
           AVG(t.avg_speed) AS speed,
           AVG(t.total_flow) AS flow,
           AVG(t.congestion_index) AS idx
    FROM traffic t
    {where}
    GROUP BY t.hour
    ORDER BY t.hour;
    """
    rows = _query(sql, params)

    speed_vals = [float(r["speed"]) for r in rows if r.get("speed") is not None]
    has_real_speed = any(v > 0 for v in speed_vals)

    idx_vals = [float(r["idx"]) for r in rows if r.get("idx") is not None]
    flow_vals = [float(r["flow"]) for r in rows if r.get("flow") is not None]
    idx_max = max(idx_vals) if idx_vals else 0
    flow_max = max(flow_vals) if flow_vals else 0

    by_hour = {}
    for r in rows:
        h = int(r["hour"])
        speed_v = None
        if has_real_speed and r.get("speed") is not None:
            speed_v = round(float(r["speed"]), 2)
        else:
            if r.get("idx") is not None and idx_max > 0:
                norm = float(r["idx"]) / idx_max
            elif r.get("flow") is not None and flow_max > 0:
                norm = float(r["flow"]) / flow_max
            else:
                norm = 0
            speed_v = round(60 - 45 * min(1, max(0, norm)), 2)
        by_hour[h] = {"speed": speed_v}

    hours = list(range(24))
    speed = [by_hour.get(h, {}).get("speed", 0) for h in hours]
    return jsonify({"month": month, "hours": hours, "speed": speed})


@app.route("/api/speed_temp_half_year")
def api_speed_temp_half_year():
    """4 半年对比：每月一条曲线，横轴24小时"""
    series = []
    for m in range(1, 7):
        sql = """
        SELECT t.hour, AVG(t.avg_speed) AS speed
        FROM traffic t
        WHERE MONTH(STR_TO_DATE(t.ts, '%%Y-%%m-%%d %%H:%%i:%%s')) = %s AND t.avg_speed IS NOT NULL
        GROUP BY t.hour ORDER BY t.hour;
        """
        rows = _query(sql, (m,))
        speed = [round(float(r["speed"] or 0), 2) for r in rows]
        series.append({"month": m, "name": f"{m}月", "speed": speed})
    return jsonify({"hours": list(range(24)), "series": series})


@app.route("/api/congestion_duration_period")
def api_congestion_duration_period():
    """5. 分时段拥堵时长对比：早高峰7-9、晚高峰17-19、平峰10-16、夜间0-6+22-23，单位分钟"""
    extra_sql, extra_params = traffic_filter_sql("t", request)
    sql_index = """
    SELECT t.hour, AVG(COALESCE(t.congestion_index, 0)) AS avg_index
    FROM traffic t WHERE 1=1
    """ + extra_sql + """
    GROUP BY t.hour ORDER BY t.hour;
    """
    rows = _query(sql_index, tuple(extra_params))
    by_hour_index = {int(r["hour"]): float(r["avg_index"] or 0) for r in rows}
    index_max = max(by_hour_index.values()) if by_hour_index else 0

    if index_max <= 0:
        sql_flow = """
        SELECT t.hour, AVG(t.total_flow) AS avg_flow
        FROM traffic t WHERE 1=1
        """ + extra_sql + """
        GROUP BY t.hour ORDER BY t.hour;
        """
        rows_flow = _query(sql_flow, tuple(extra_params))
        by_hour_flow = {int(r["hour"]): float(r["avg_flow"] or 0) for r in rows_flow}
        flow_max = max(by_hour_flow.values()) if by_hour_flow else 1
        def avg_minutes_from_flow(hours):
            vals = [by_hour_flow.get(h, 0) for h in hours]
            if not vals or flow_max <= 0:
                return 0
            avg_flow = sum(vals) / len(vals)
            return round((avg_flow / flow_max) * 25, 2)
        early = avg_minutes_from_flow([7, 8, 9])
        late = avg_minutes_from_flow([17, 18, 19])
        flat = avg_minutes_from_flow(list(range(10, 17)))
        night = avg_minutes_from_flow(list(range(0, 7)) + [22, 23])
    else:
        def avg_minutes(hours):
            vals = [by_hour_index.get(h, 0) for h in hours]
            return round((sum(vals) / len(vals)) * 5, 2) if vals else 0
        early = avg_minutes([7, 8, 9])
        late = avg_minutes([17, 18, 19])
        flat = avg_minutes(list(range(10, 17)))
        night = avg_minutes(list(range(0, 7)) + [22, 23])

    return jsonify([
        {"period": "早高峰", "minutes": early},
        {"period": "晚高峰", "minutes": late},
        {"period": "平峰", "minutes": flat},
        {"period": "夜间", "minutes": night},
    ])


@app.route("/api/congestion_index_trend")
def api_congestion_index_trend():
    """6. 拥堵指数趋势图：横轴日期，拥堵指数，支持 period=month|last_month|half_year"""
    period = request.args.get("period", "month")
    extra_sql, extra_params = traffic_filter_sql("t", request)
    if period == "last_month":
        sql = """
        SELECT t.date,
               AVG(t.congestion_index) AS idx,
               AVG(t.total_flow) AS flow
        FROM traffic t
        WHERE DATE_FORMAT(STR_TO_DATE(t.date, '%%Y-%%m-%%d'), '%%Y-%%m') = (
            SELECT DATE_FORMAT(DATE_SUB(MAX(STR_TO_DATE(ts, '%%Y-%%m-%%d %%H:%%i:%%s')), INTERVAL 1 MONTH), '%%Y-%%m')
            FROM traffic
        )
        """ + extra_sql + """
        GROUP BY t.date ORDER BY t.date;
        """
    elif period == "half_year":
        sql = """
        SELECT t.date,
               AVG(t.congestion_index) AS idx,
               AVG(t.total_flow) AS flow
        FROM traffic t
        WHERE 1=1
        """ + extra_sql + """
        GROUP BY t.date ORDER BY t.date;
        """
    else:
        sql = """
        SELECT t.date,
               AVG(t.congestion_index) AS idx,
               AVG(t.total_flow) AS flow
        FROM traffic t
        WHERE DATE_FORMAT(STR_TO_DATE(t.date, '%%Y-%%m-%%d'), '%%Y-%%m') = (
            SELECT DATE_FORMAT(MAX(STR_TO_DATE(ts, '%%Y-%%m-%%d %%H:%%i:%%s')), '%%Y-%%m')
            FROM traffic
        )
        """ + extra_sql + """
        GROUP BY t.date ORDER BY t.date;
        """
    rows = _query(sql, tuple(extra_params))
    dates = [r["date"] for r in rows]
    idx_raw = [None if r.get("idx") is None else float(r["idx"]) for r in rows]
    has_real_idx = any((v is not None and v > 0) for v in idx_raw)
    if has_real_idx:
        idx = [round(float(v or 0), 2) for v in idx_raw]
    else:
        flows = [0 if r.get("flow") is None else float(r["flow"] or 0) for r in rows]
        fmax = max(flows) if flows else 0
        if fmax <= 0:
            idx = [0 for _ in flows]
        else:
            idx = [round((f / fmax) * 10, 2) for f in flows]
    return jsonify({"period": period, "dates": dates, "congestion_index": idx})


@app.route("/api/road_type_compare")
def api_road_type_compare():
    """
    空间维度：三条主要道路日均流量对比
    由于原始 road_type 基本只有“未分类”，这里改为按 road_name 维度展示 TOP3 道路的平均流量。
    """
    sql = """
    SELECT road_name, AVG(total_flow) AS avg_flow
    FROM traffic
    GROUP BY road_name
    ORDER BY avg_flow DESC
    LIMIT 3;
    """
    rows = _query(sql)
    return jsonify(rows)


@app.route("/api/area_compare")
def api_area_compare():
    """
    道路维度：三条主要道路累计流量对比
    原始 area 通常只有单一区域，这里同样按 road_name 汇总累计流量。
    """
    sql = """
    SELECT road_name, SUM(total_flow) AS total_flow
    FROM traffic
    GROUP BY road_name
    ORDER BY total_flow DESC
    LIMIT 3;
    """
    rows = _query(sql)
    return jsonify(rows)


@app.route("/api/weekday_compare")
def api_weekday_compare():
    # 时间+空间：工作日/周末流量对比
    sql = """
    SELECT
        (DAYOFWEEK(STR_TO_DATE(ts, '%%Y-%%m-%%d %%H:%%i:%%s')) - 1) AS weekday,
        AVG(total_flow) AS avg_flow
    FROM traffic
    GROUP BY (DAYOFWEEK(STR_TO_DATE(ts, '%%Y-%%m-%%d %%H:%%i:%%s')) - 1)
    ORDER BY weekday;
    """
    rows = _query(sql)
    # 补充中文名称
    weekday_names = ["周日", "周一", "周二", "周三", "周四", "周五", "周六"]
    for r in rows:
        try:
            idx = int(r["weekday"])
            r["weekday_name"] = weekday_names[idx]
        except Exception:
            r["weekday_name"] = "未知"
    return jsonify(rows)


# ============================================================
# 专题分析：丰富化（KPI / 辅助图 / 排行 / 自动洞察）
# ============================================================

def _fmt_num(v, digits=2):
    try:
        if v is None:
            return "—"
        return round(float(v), digits)
    except Exception:
        return "—"


def _hour_label(h):
    try:
        return f"{int(h):02d}:00"
    except Exception:
        return str(h)


@app.route("/api/analysis_kpi/<chart_key>")
def api_analysis_kpi(chart_key):
    """
    为每个专题分析返回 KPI、道路排行与自动洞察。
    返回结构：
    {
        "kpis":   [{label, value, hint, tone}],
        "road_rank": [{name, value, pct}],
        "insights": [text, ...]
    }
    """
    extra_sql, extra_params = traffic_filter_sql("t", request)
    filter_params = tuple(extra_params)

    def q(sql, params=None):
        return _query(sql, params or ())

    kpis, road_rank, insights = [], [], []

    # ---- 道路排行（所有分析页共用） ----
    if chart_key in ("flow", "vehicle"):
        rank_metric = "SUM(t.total_flow)"
        rank_label = "累计流量"
    elif chart_key == "speed":
        rank_metric = "AVG(CASE WHEN t.avg_speed > 0 THEN t.avg_speed ELSE NULL END)"
        rank_label = "平均车速"
    elif chart_key in ("heatmap", "duration", "congestion"):
        rank_metric = "AVG(COALESCE(t.congestion_index, t.total_flow/100.0))"
        rank_label = "平均拥堵强度"
    else:
        rank_metric = "SUM(t.total_flow)"
        rank_label = "累计流量"

    rank_rows = q(
        f"""
        SELECT t.road_name AS name, {rank_metric} AS value
        FROM traffic t WHERE 1=1 {extra_sql}
        GROUP BY t.road_name
        HAVING value IS NOT NULL
        ORDER BY value {"ASC" if chart_key == "speed" else "DESC"}
        LIMIT 6;
        """,
        filter_params,
    )
    vmax = max([float(r.get("value") or 0) for r in rank_rows] + [0.0001])
    road_rank = [
        {
            "name": r["name"] or "未命名",
            "value": _fmt_num(r["value"]),
            "pct": round(float(r.get("value") or 0) / vmax * 100, 1) if vmax > 0 else 0,
            "label": rank_label,
        }
        for r in rank_rows
    ]

    # ---- 按 chart_key 生成 KPI 与洞察 ----
    if chart_key == "flow":
        month = request.args.get("month", type=int, default=1)
        if month < 1 or month > 6:
            month = 1
        sql = f"""
        SELECT t.hour, AVG(t.total_flow) AS avg_flow
        FROM traffic t
        WHERE MONTH(STR_TO_DATE(t.ts, '%%Y-%%m-%%d %%H:%%i:%%s')) = %s {extra_sql}
        GROUP BY t.hour ORDER BY t.hour;
        """
        rows = q(sql, (month,) + filter_params)
        vals = [(int(r["hour"]), float(r["avg_flow"] or 0)) for r in rows]
        if vals:
            peak = max(vals, key=lambda x: x[1])
            low = min(vals, key=lambda x: x[1])
            avg = sum(v for _, v in vals) / max(1, len(vals))
            total = sum(v for _, v in vals)
            kpis = [
                {"label": "小时峰值流量", "value": _fmt_num(peak[1]), "hint": f"出现在 {_hour_label(peak[0])}", "tone": "danger"},
                {"label": "低谷时段流量", "value": _fmt_num(low[1]), "hint": f"出现在 {_hour_label(low[0])}", "tone": "success"},
                {"label": "小时均值流量", "value": _fmt_num(avg), "hint": f"{month}月全日平均", "tone": "primary"},
                {"label": "当月累计流量", "value": _fmt_num(total), "hint": "24 小时累计值", "tone": "info"},
            ]
            peak_band = "早高峰" if 7 <= peak[0] <= 9 else ("晚高峰" if 17 <= peak[0] <= 19 else "其他时段")
            insights = [
                f"{month}月 <b>{_hour_label(peak[0])}</b> 是当日流量最高峰，约 {_fmt_num(peak[1])}，位于<strong>{peak_band}</strong>。",
                f"低谷出现在 <b>{_hour_label(low[0])}</b>，约 {_fmt_num(low[1])}，峰谷比约 <b>{round(peak[1]/max(1,low[1]),1)}x</b>。",
                "建议：优先对该时段道路进行信号灯动态优化与公共交通错峰引导。" if peak[1] > avg * 1.5 else "流量分布较平稳，整体调度压力可控。",
            ]
        else:
            kpis = [{"label": "暂无数据", "value": "—", "hint": "请调整筛选条件", "tone": "info"}]

    elif chart_key == "vehicle":
        rows = q(
            f"""
            SELECT COALESCE(SUM(small_vehicle),0) AS private,
                   COALESCE(SUM(large_vehicle),0) AS bus,
                   COALESCE(SUM(truck),0) AS truck,
                   COALESCE(SUM(total_flow),0) AS total
            FROM traffic t WHERE 1=1 {extra_sql};
            """,
            filter_params,
        )
        r = rows[0] if rows else {}
        total = float(r.get("total") or 0)
        pv = float(r.get("private") or 0)
        bus = float(r.get("bus") or 0)
        tr = float(r.get("truck") or 0)
        other = max(0.0, total - pv - bus - tr)
        def pct(x):
            return round(x / total * 100, 1) if total > 0 else 0
        kpis = [
            {"label": "私家车流量", "value": _fmt_num(pv), "hint": f"占比 {pct(pv)}%", "tone": "primary"},
            {"label": "货车流量", "value": _fmt_num(tr), "hint": f"占比 {pct(tr)}%", "tone": "warning"},
            {"label": "其他车流量", "value": _fmt_num(other), "hint": f"占比 {pct(other)}%", "tone": "success"},
        ]
        if bus > 0:
            kpis.insert(1, {"label": "公交流量", "value": _fmt_num(bus), "hint": f"占比 {pct(bus)}%", "tone": "info"})
        while len(kpis) < 4:
            kpis.append({"label": "—", "value": "—", "hint": "—", "tone": "info"})
        kpis = kpis[:4]
        if total > 0:
            parts = [("私家车", pv), ("货车", tr), ("其他", other)]
            if bus > 0:
                parts.insert(1, ("公交", bus))
            dominant = max(parts, key=lambda x: x[1])
            insights = [
                f"车流结构以 <b>{dominant[0]}</b> 为主，占比约 <b>{pct(dominant[1])}%</b>。",
                f"货车占比 <b>{pct(tr)}%</b>" + ("，建议关注重载通行安全与桥梁承压。" if pct(tr) > 12 else "，整体处于合理水平。"),
                "可通过筛选特定道路，进一步对比主干道与支路的车型构成差异。",
            ]
        else:
            insights = ["当前筛选条件下暂无车型数据。"]

    elif chart_key == "heatmap":
        rows = q(
            f"""
            SELECT t.date, t.hour,
                   AVG(COALESCE(t.congestion_index, t.total_flow/100.0)) AS intensity
            FROM traffic t WHERE 1=1 {extra_sql}
            GROUP BY t.date, t.hour ORDER BY t.date DESC;
            """,
            filter_params,
        )
        if rows:
            dates = sorted({r["date"] for r in rows})[-7:]
            date_set = set(dates)
            filtered = [r for r in rows if r["date"] in date_set]
            peak = max(filtered, key=lambda x: float(x["intensity"] or 0))
            min_r = min(filtered, key=lambda x: float(x["intensity"] or 0))
            avg = sum(float(r["intensity"] or 0) for r in filtered) / max(1, len(filtered))
            hour_agg = {}
            for r in filtered:
                hour_agg.setdefault(int(r["hour"]), []).append(float(r["intensity"] or 0))
            peak_hour = max(hour_agg.items(), key=lambda kv: sum(kv[1]) / len(kv[1]))[0] if hour_agg else 0
            kpis = [
                {"label": "最高风险强度", "value": _fmt_num(peak["intensity"]), "hint": f"{peak['date']} {_hour_label(peak['hour'])}", "tone": "danger"},
                {"label": "最低风险强度", "value": _fmt_num(min_r["intensity"]), "hint": f"{min_r['date']} {_hour_label(min_r['hour'])}", "tone": "success"},
                {"label": "平均强度", "value": _fmt_num(avg), "hint": "近 7 天平均", "tone": "primary"},
                {"label": "高发时段", "value": _hour_label(peak_hour), "hint": "小时均值最大", "tone": "warning"},
            ]
            insights = [
                f"近 7 天风险峰值出现在 <b>{peak['date']} {_hour_label(peak['hour'])}</b>，强度 <b>{_fmt_num(peak['intensity'])}</b>。",
                f"日常高发时段约为 <b>{_hour_label(peak_hour)}</b>，建议提前部署交通疏导力量。",
                f"7 天平均强度 <b>{_fmt_num(avg)}</b>，可作为基线用于异常波动监测。",
            ]
        else:
            kpis = [{"label": "暂无数据", "value": "—", "hint": "请调整筛选条件", "tone": "info"}]

    elif chart_key == "speed":
        month = request.args.get("month", type=int)
        if month is not None and (month < 1 or month > 6):
            month = None
        if month is not None:
            where = f"WHERE MONTH(STR_TO_DATE(t.ts, '%%Y-%%m-%%d %%H:%%i:%%s')) = %s {extra_sql}"
            params = (month,) + filter_params
        else:
            where = f"WHERE 1=1 {extra_sql}"
            params = filter_params
        rows = q(
            f"""
            SELECT t.hour,
                   AVG(CASE WHEN t.avg_speed > 0 THEN t.avg_speed END) AS speed,
                   AVG(t.total_flow) AS flow
            FROM traffic t {where}
            GROUP BY t.hour ORDER BY t.hour;
            """,
            params,
        )
        speeds = [(int(r["hour"]), float(r["speed"])) for r in rows if r.get("speed") is not None]
        if not speeds:
            flows = [(int(r["hour"]), float(r.get("flow") or 0)) for r in rows]
            fmax = max((f for _, f in flows), default=1) or 1
            speeds = [(h, round(60 - 45 * (f / fmax), 2)) for h, f in flows]
        if speeds:
            maxh = max(speeds, key=lambda x: x[1])
            minh = min(speeds, key=lambda x: x[1])
            avg = sum(s for _, s in speeds) / max(1, len(speeds))
            slow_hours = sum(1 for _, s in speeds if s < 30)
            kpis = [
                {"label": "最高平均车速", "value": f"{_fmt_num(maxh[1])} km/h", "hint": _hour_label(maxh[0]), "tone": "success"},
                {"label": "最低平均车速", "value": f"{_fmt_num(minh[1])} km/h", "hint": _hour_label(minh[0]), "tone": "danger"},
                {"label": "全日均值", "value": f"{_fmt_num(avg)} km/h", "hint": (f"{month}月" if month else "半年整体"), "tone": "primary"},
                {"label": "低速时段数", "value": f"{slow_hours}", "hint": "<30 km/h 的小时数", "tone": "warning"},
            ]
            insights = [
                f"最低车速 <b>{_fmt_num(minh[1])} km/h</b> 出现在 <b>{_hour_label(minh[0])}</b>，需重点疏导。",
                (f"有 <b>{slow_hours}</b> 个小时车速低于 30km/h，占比 {round(slow_hours/24*100,1)}%。"
                 if slow_hours else "全时段车速均保持在 30km/h 以上，整体通行顺畅。"),
                f"最高车速 <b>{_fmt_num(maxh[1])} km/h</b> 出现在 <b>{_hour_label(maxh[0])}</b>，通常为夜间低峰。",
            ]
        else:
            kpis = [{"label": "暂无数据", "value": "—", "hint": "请调整筛选条件", "tone": "info"}]

    elif chart_key == "duration":
        rows = q(
            f"""
            SELECT t.hour, AVG(COALESCE(t.congestion_index,0)) AS idx, AVG(t.total_flow) AS flow
            FROM traffic t WHERE 1=1 {extra_sql}
            GROUP BY t.hour ORDER BY t.hour;
            """,
            filter_params,
        )
        by_h = {int(r["hour"]): float(r["idx"] or 0) for r in rows}
        fmax = max([float(r.get("flow") or 0) for r in rows] + [1])
        use_flow = max(by_h.values()) <= 0
        def minutes(hours):
            if use_flow:
                vals = [float(next((r.get("flow") for r in rows if int(r["hour"])==h), 0)) for h in hours]
                return round(sum(vals)/len(vals)/fmax * 25, 2) if vals else 0
            vals = [by_h.get(h, 0) for h in hours]
            return round(sum(vals)/len(vals) * 5, 2) if vals else 0
        early = minutes([7,8,9])
        late = minutes([17,18,19])
        flat = minutes(list(range(10,17)))
        night = minutes(list(range(0,7)) + [22,23])
        kpis = [
            {"label": "早高峰", "value": f"{early} 分", "hint": "07:00–09:00", "tone": "warning"},
            {"label": "晚高峰", "value": f"{late} 分", "hint": "17:00–19:00", "tone": "danger"},
            {"label": "平峰", "value": f"{flat} 分", "hint": "10:00–16:00", "tone": "primary"},
            {"label": "夜间", "value": f"{night} 分", "hint": "22:00–06:00", "tone": "success"},
        ]
        worst = max([("早高峰",early),("晚高峰",late),("平峰",flat),("夜间",night)], key=lambda x: x[1])
        insights = [
            f"<b>{worst[0]}</b>拥堵时长最长，约 <b>{worst[1]}</b> 分钟，应加强信号配时与交警指挥。",
            f"晚高峰 vs 早高峰：{'晚高峰更拥堵' if late>early else '早高峰更拥堵' if early>late else '早晚高峰持平'}。",
            f"平峰时段可承担弹性出行，夜间整体畅通（{night} 分）。",
        ]

    elif chart_key == "congestion":
        period = request.args.get("period", "month")
        if period == "last_month":
            where = f"""WHERE DATE_FORMAT(STR_TO_DATE(t.date,'%%Y-%%m-%%d'),'%%Y-%%m') = (
                SELECT DATE_FORMAT(DATE_SUB(MAX(STR_TO_DATE(ts,'%%Y-%%m-%%d %%H:%%i:%%s')),INTERVAL 1 MONTH),'%%Y-%%m') FROM traffic
            ) {extra_sql}"""
        elif period == "half_year":
            where = f"WHERE 1=1 {extra_sql}"
        else:
            where = f"""WHERE DATE_FORMAT(STR_TO_DATE(t.date,'%%Y-%%m-%%d'),'%%Y-%%m') = (
                SELECT DATE_FORMAT(MAX(STR_TO_DATE(ts,'%%Y-%%m-%%d %%H:%%i:%%s')),'%%Y-%%m') FROM traffic
            ) {extra_sql}"""
        rows = q(
            f"""SELECT t.date, AVG(t.congestion_index) AS idx, AVG(t.total_flow) AS flow
            FROM traffic t {where} GROUP BY t.date ORDER BY t.date;""",
            filter_params,
        )
        series = []
        for r in rows:
            idx = r.get("idx")
            if idx is None or float(idx or 0) == 0:
                flow = float(r.get("flow") or 0)
                idx = flow / 1000.0
            series.append((r["date"], float(idx or 0)))
        if series:
            maxd = max(series, key=lambda x: x[1])
            mind = min(series, key=lambda x: x[1])
            avg = sum(v for _, v in series) / max(1, len(series))
            thr = avg * 1.15
            congest_days = sum(1 for _, v in series if v > thr)
            kpis = [
                {"label": "最拥堵日", "value": _fmt_num(maxd[1]), "hint": maxd[0], "tone": "danger"},
                {"label": "最畅通日", "value": _fmt_num(mind[1]), "hint": mind[0], "tone": "success"},
                {"label": "区间均值", "value": _fmt_num(avg), "hint": f"共 {len(series)} 天", "tone": "primary"},
                {"label": "高拥堵天数", "value": f"{congest_days}", "hint": "超过均值 15%", "tone": "warning"},
            ]
            insights = [
                f"拥堵峰值出现在 <b>{maxd[0]}</b>，指数 <b>{_fmt_num(maxd[1])}</b>，可追溯该日赛事/施工等事件。",
                f"累计 <b>{congest_days}</b> 天拥堵超过基线 15%" + ("，建议重点治理。" if congest_days > len(series)*0.3 else "，整体处于可控范围。"),
                f"区间 {series[0][0]} ~ {series[-1][0]}，平均拥堵指数 <b>{_fmt_num(avg)}</b>。",
            ]
        else:
            kpis = [{"label": "暂无数据", "value": "—", "hint": "请调整筛选条件", "tone": "info"}]

    return jsonify({"kpis": kpis, "road_rank": road_rank, "insights": insights})


@app.route("/api/flow_half_year_overview")
def api_flow_half_year_overview():
    """流量走势副图：1-6 月 24 小时叠加曲线对比"""
    extra_sql, extra_params = traffic_filter_sql("t", request)
    series = []
    for m in range(1, 7):
        sql = f"""
        SELECT t.hour, AVG(t.total_flow) AS flow
        FROM traffic t
        WHERE MONTH(STR_TO_DATE(t.ts,'%%Y-%%m-%%d %%H:%%i:%%s')) = %s {extra_sql}
        GROUP BY t.hour ORDER BY t.hour;
        """
        rows = _query(sql, (m,) + tuple(extra_params))
        by_h = {int(r["hour"]): round(float(r["flow"] or 0), 2) for r in rows}
        series.append({"month": m, "name": f"{m}月", "flow": [by_h.get(h, 0) for h in range(24)]})
    return jsonify({"hours": list(range(24)), "series": series})


@app.route("/api/vehicle_by_road_top")
def api_vehicle_by_road_top():
    """车型副图：TOP 5 道路的车型堆叠（私家车/货车/其他）"""
    extra_sql, extra_params = traffic_filter_sql("t", request)
    rows = _query(
        f"""
        SELECT t.road_name,
               COALESCE(SUM(t.small_vehicle),0) AS pv,
               COALESCE(SUM(t.large_vehicle),0) AS bus,
               COALESCE(SUM(t.truck),0) AS tr,
               COALESCE(SUM(t.total_flow),0) AS total
        FROM traffic t WHERE 1=1 {extra_sql}
        GROUP BY t.road_name
        ORDER BY total DESC
        LIMIT 6;
        """,
        tuple(extra_params),
    )
    roads, pv, bus, tr, other = [], [], [], [], []
    for r in rows:
        roads.append(r["road_name"] or "未命名")
        p = float(r["pv"] or 0)
        b = float(r["bus"] or 0)
        t_ = float(r["tr"] or 0)
        total = float(r["total"] or 0)
        pv.append(round(p, 2))
        bus.append(round(b, 2))
        tr.append(round(t_, 2))
        other.append(round(max(0, total - p - b - t_), 2))
    return jsonify({"roads": roads, "private": pv, "bus": bus, "truck": tr, "other": other})


@app.route("/api/risk_hours_rank")
def api_risk_hours_rank():
    """热力图副图：24 小时平均拥堵强度排行（含日期峰值）"""
    extra_sql, extra_params = traffic_filter_sql("t", request)
    rows = _query(
        f"""
        SELECT t.hour, AVG(COALESCE(t.congestion_index, t.total_flow/100.0)) AS intensity
        FROM traffic t WHERE 1=1 {extra_sql}
        GROUP BY t.hour ORDER BY t.hour;
        """,
        tuple(extra_params),
    )
    hours = [int(r["hour"]) for r in rows]
    intensities = [round(float(r["intensity"] or 0), 2) for r in rows]
    return jsonify({"hours": hours, "intensities": intensities})


@app.route("/api/duration_weekday")
def api_duration_weekday():
    """拥堵时长副图：一周 7 天 × 4 时段 的堆叠柱"""
    extra_sql, extra_params = traffic_filter_sql("t", request)
    rows = _query(
        f"""
        SELECT
            ((DAYOFWEEK(STR_TO_DATE(t.ts,'%%Y-%%m-%%d %%H:%%i:%%s')) + 5) MOD 7) AS weekday_idx,
            t.hour,
            AVG(COALESCE(t.congestion_index, t.total_flow/100.0)) AS idx
        FROM traffic t WHERE 1=1 {extra_sql}
        GROUP BY weekday_idx, t.hour;
        """,
        tuple(extra_params),
    )
    names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

    def bucket(h):
        if 7 <= h <= 9: return 0
        if 17 <= h <= 19: return 1
        if 10 <= h <= 16: return 2
        return 3

    agg = [[[] for _ in range(4)] for _ in range(7)]
    for r in rows:
        w = int(r["weekday_idx"])
        h = int(r["hour"])
        agg[w][bucket(h)].append(float(r["idx"] or 0))

    factor = [5, 5, 5, 5]
    data = []
    for b in range(4):
        bucket_series = []
        for w in range(7):
            vals = agg[w][b]
            v = (sum(vals) / len(vals) * factor[b]) if vals else 0
            bucket_series.append(round(v, 2))
        data.append(bucket_series)
    return jsonify({
        "weekdays": names,
        "series": [
            {"name": "早高峰", "data": data[0]},
            {"name": "晚高峰", "data": data[1]},
            {"name": "平峰",   "data": data[2]},
            {"name": "夜间",   "data": data[3]},
        ],
    })


@app.route("/api/congestion_month_compare")
def api_congestion_month_compare():
    """拥堵指数副图：本月 vs 上月 日均对比"""
    extra_sql, extra_params = traffic_filter_sql("t", request)
    this_rows = _query(
        f"""
        SELECT DAY(STR_TO_DATE(t.date,'%%Y-%%m-%%d')) AS d,
               AVG(t.congestion_index) AS idx, AVG(t.total_flow) AS flow
        FROM traffic t
        WHERE DATE_FORMAT(STR_TO_DATE(t.date,'%%Y-%%m-%%d'),'%%Y-%%m') = (
            SELECT DATE_FORMAT(MAX(STR_TO_DATE(ts,'%%Y-%%m-%%d %%H:%%i:%%s')),'%%Y-%%m') FROM traffic
        ) {extra_sql}
        GROUP BY d ORDER BY d;
        """,
        tuple(extra_params),
    )
    last_rows = _query(
        f"""
        SELECT DAY(STR_TO_DATE(t.date,'%%Y-%%m-%%d')) AS d,
               AVG(t.congestion_index) AS idx, AVG(t.total_flow) AS flow
        FROM traffic t
        WHERE DATE_FORMAT(STR_TO_DATE(t.date,'%%Y-%%m-%%d'),'%%Y-%%m') = (
            SELECT DATE_FORMAT(DATE_SUB(MAX(STR_TO_DATE(ts,'%%Y-%%m-%%d %%H:%%i:%%s')),INTERVAL 1 MONTH),'%%Y-%%m') FROM traffic
        ) {extra_sql}
        GROUP BY d ORDER BY d;
        """,
        tuple(extra_params),
    )

    def to_idx_map(rows):
        out = {}
        for r in rows:
            idx = r.get("idx")
            if idx is None or float(idx or 0) == 0:
                idx = float(r.get("flow") or 0) / 1000.0
            out[int(r["d"])] = round(float(idx or 0), 2)
        return out

    this_map = to_idx_map(this_rows)
    last_map = to_idx_map(last_rows)
    days = sorted(set(list(this_map.keys()) + list(last_map.keys())))
    if not days:
        days = list(range(1, 31))
    return jsonify({
        "days": days,
        "this_month": [this_map.get(d, None) for d in days],
        "last_month": [last_map.get(d, None) for d in days],
    })


@app.route("/api/rule_risk")
def api_rule_risk():
    """
    拥堵风险预测：默认使用 scikit-learn RandomForestClassifier（随机森林多分类）。
    训练标签由规则引擎对样本自动标注（弱监督）；样本过少或无 sklearn 时回退为纯规则。
    """
    extra_sql, extra_params = traffic_filter_sql("t", request)
    sql = """
    SELECT
        t.ts,
        t.road_name,
        t.road_type,
        t.area,
        t.total_flow,
        t.congestion_index,
        t.avg_speed
    FROM traffic t
    WHERE 1=1
    """ + extra_sql + """
    """
    rows = _query(sql, tuple(extra_params))
    rows, algo_note = predict_risk_ml(rows)

    risk_stats = {"高风险": 0, "中风险": 0, "低风险": 0}
    dist_counter = {}
    for r in rows:
        level = r["risk_level"]
        risk_stats[level] += 1

        ts = datetime.strptime(r["ts"], "%Y-%m-%d %H:%M:%S")
        hour = ts.hour
        key = (hour, level)
        dist_counter[key] = dist_counter.get(key, 0) + 1

    high_risk = [r for r in rows if r["risk_level"] == "高风险"]
    examples_source = high_risk if high_risk else rows

    examples = sorted(
        examples_source,
        key=lambda x: x["risk_score"],
        reverse=True,
    )[:15]

    distribution = [
        {"hour": h, "risk_level": level, "count": cnt}
        for (h, level), cnt in sorted(dist_counter.items(), key=lambda x: (x[0][0], x[0][1]))
    ]

    return jsonify({
        "stats": risk_stats,
        "examples": examples,
        "distribution": distribution,
        "algorithm": algo_note,
    })


@app.route("/api/traffic_records", methods=["GET", "POST"])
@login_required
def api_traffic_records():
    if request.method == "GET":
        page = request.args.get("page", 1, type=int)
        page_size = min(request.args.get("page_size", 10, type=int), 100)
        if page < 1:
            page = 1
        extra_sql, extra_params = traffic_filter_sql("t", request)
        count_sql = (
            "SELECT COUNT(*) AS c FROM traffic t WHERE 1=1 " + extra_sql
        )
        total = int(_query(count_sql, tuple(extra_params))[0]["c"])
        offset = (page - 1) * page_size
        sql = (
            """
            SELECT t.* FROM traffic t WHERE 1=1
            """
            + extra_sql
            + """
            ORDER BY t.ts DESC
            LIMIT %s OFFSET %s;
            """
        )
        rows = _query(sql, tuple(extra_params) + (page_size, offset))
        return jsonify(
            {"total": total, "page": page, "page_size": page_size, "rows": rows}
        )

    data = request.get_json(silent=True) or {}
    for k in ("ts", "date", "hour", "road_name"):
        if not str(data.get(k, "")).strip():
            return jsonify({"ok": False, "error": f"缺少字段: {k}"}), 400
    hour = int(data["hour"])
    if hour < 0 or hour > 23:
        return jsonify({"ok": False, "error": "hour 须在 0-23"}), 400
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO traffic
        (ts, date, hour, road_name, road_type, area, total_flow,
         small_vehicle, large_vehicle, truck, avg_speed, congestion_index)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
        (
            str(data["ts"]),
            str(data["date"]),
            hour,
            str(data["road_name"]),
            str(data.get("road_type") or "未分类"),
            str(data.get("area") or ""),
            float(data.get("total_flow") or 0),
            _nullable_float(data.get("small_vehicle")),
            _nullable_float(data.get("large_vehicle")),
            _nullable_float(data.get("truck")),
            _nullable_float(data.get("avg_speed")),
            _nullable_float(data.get("congestion_index")),
        ),
    )
    conn.commit()
    rid = cur.lastrowid
    conn.close()
    return jsonify({"ok": True, "id": rid})


def _nullable_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


@app.route("/api/traffic_records/<int:rid>", methods=["GET", "PUT", "DELETE"])
@login_required
def api_traffic_record_one(rid):
    if request.method == "GET":
        rows = _query("SELECT * FROM traffic WHERE id = %s;", (rid,))
        if not rows:
            return jsonify({"error": "not found"}), 404
        return jsonify(rows[0])

    if request.method == "DELETE":
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM traffic WHERE id = %s;", (rid,))
        conn.commit()
        conn.close()
        return jsonify({"ok": True})

    data = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE traffic SET
            ts = %s, date = %s, hour = %s, road_name = %s, road_type = %s, area = %s,
            total_flow = %s, small_vehicle = %s, large_vehicle = %s, truck = %s,
            avg_speed = %s, congestion_index = %s
        WHERE id = %s;
        """,
        (
            str(data.get("ts", "")),
            str(data.get("date", "")),
            int(data.get("hour", 0)),
            str(data.get("road_name", "")),
            str(data.get("road_type", "未分类")),
            str(data.get("area", "")),
            float(data.get("total_flow") or 0),
            _nullable_float(data.get("small_vehicle")),
            _nullable_float(data.get("large_vehicle")),
            _nullable_float(data.get("truck")),
            _nullable_float(data.get("avg_speed")),
            _nullable_float(data.get("congestion_index")),
            rid,
        ),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


if __name__ == "__main__":
    ensure_db()
    app.run(host="0.0.0.0", port=5000, debug=True)

