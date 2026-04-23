"""用户表与账号逻辑（MySQL）。"""
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pymysql.err import IntegrityError
from werkzeug.security import check_password_hash, generate_password_hash

from db_utils import get_connection


def init_users_table() -> None:
    """users 表由 db_utils.init_db() 一并创建；此处保留接口兼容。"""
    pass


def ensure_default_admin() -> None:
    """若无用户则创建管理员 admin / 123456。"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM users;")
    row = cur.fetchone()
    n = int(row["c"]) if row else 0
    if n == 0:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """
            INSERT INTO users (username, password_hash, email, nickname, role, created_at)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (
                "admin",
                generate_password_hash("123456"),
                "admin@local",
                "管理员",
                "admin",
                now,
            ),
        )
        conn.commit()
    conn.close()


def get_user_by_id(uid: int) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, email, nickname, role, created_at FROM users WHERE id = %s;",
        (uid,),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, password_hash, email, nickname, role, created_at FROM users WHERE username = %s;",
        (username.strip(),),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def create_user(username: str, password: str, email: str = "", nickname: str = "") -> Tuple[bool, str]:
    username = username.strip()
    if len(username) < 3:
        return False, "用户名至少 3 个字符"
    if len(password) < 6:
        return False, "密码至少 6 位"
    conn = get_connection()
    cur = conn.cursor()
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """
            INSERT INTO users (username, password_hash, email, nickname, role, created_at)
            VALUES (%s, %s, %s, %s, 'user', %s);
            """,
            (
                username,
                generate_password_hash(password),
                email.strip() or "",
                nickname.strip() or username,
                now,
            ),
        )
        conn.commit()
        return True, ""
    except IntegrityError:
        return False, "用户名已存在"
    finally:
        conn.close()


def verify_login(username: str, password: str) -> Optional[Dict[str, Any]]:
    u = get_user_by_username(username)
    if not u:
        return None
    if not check_password_hash(u["password_hash"], password):
        return None
    u.pop("password_hash", None)
    return u


def update_profile(uid: int, email: str, nickname: str) -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE users SET email = %s, nickname = %s WHERE id = %s;",
        (email.strip(), nickname.strip(), uid),
    )
    conn.commit()
    conn.close()


def update_password(uid: int, new_password: str) -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE users SET password_hash = %s WHERE id = %s;",
        (generate_password_hash(new_password), uid),
    )
    conn.commit()
    conn.close()


def list_users(username: str = "", email: str = "", role: str = "") -> List[Dict[str, Any]]:
    username = (username or "").strip()
    email = (email or "").strip()
    role = (role or "").strip()

    where = []
    params: List[Any] = []
    if username:
        where.append("username LIKE %s")
        params.append(f"%{username}%")
    if email:
        where.append("email LIKE %s")
        params.append(f"%{email}%")
    if role in ("user", "admin"):
        where.append("role = %s")
        params.append(role)

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, email, nickname, role, created_at FROM users"
        + where_sql
        + " ORDER BY id ASC;",
        tuple(params),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def delete_user(uid: int) -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id = %s;", (uid,))
    conn.commit()
    conn.close()


def set_user_role(uid: int, role: str) -> None:
    if role not in ("user", "admin"):
        role = "user"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET role = %s WHERE id = %s;", (role, uid))
    conn.commit()
    conn.close()
