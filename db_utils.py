"""MySQL 连接与建表（不依赖 pandas/numpy）。"""
from __future__ import annotations

import os
import sys
import traceback
from datetime import datetime
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

import pymysql
from pymysql.cursors import DictCursor
from werkzeug.security import generate_password_hash

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 可通过环境变量覆盖；默认 localhost:3306 root/123456，库名 py_traffic
MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "123456")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "py_traffic")
MYSQL_CHARSET = "utf8mb4"


def get_server_connection():
    """连接到 MySQL 服务（不指定 database）。"""
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        charset=MYSQL_CHARSET,
        cursorclass=DictCursor,
        autocommit=False,
    )


def ensure_database() -> None:
    """若业务库不存在则自动创建。"""
    conn = get_server_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        )
        conn.commit()
    finally:
        conn.close()


def get_connection():
    """返回 DictCursor 连接，键为列名。"""
    ensure_database()
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset=MYSQL_CHARSET,
        cursorclass=DictCursor,
        autocommit=False,
    )


@contextmanager
def get_cursor(commit: bool = True) -> Iterator:
    conn = get_connection()
    try:
        cur = conn.cursor()
        yield conn, cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """创建数据表与索引（不删库；供应用启动调用）。"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS traffic (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ts VARCHAR(32) NOT NULL,
            date VARCHAR(16) NOT NULL,
            hour INT,
            road_name VARCHAR(255),
            road_type VARCHAR(64),
            area VARCHAR(128),
            total_flow DOUBLE,
            small_vehicle DOUBLE,
            large_vehicle DOUBLE,
            truck DOUBLE,
            avg_speed DOUBLE,
            congestion_index DOUBLE,
            INDEX idx_traffic_ts (ts(20)),
            INDEX idx_traffic_date (date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(64) NOT NULL UNIQUE,
            password_hash VARCHAR(255) NOT NULL,
            email VARCHAR(255),
            nickname VARCHAR(128),
            role VARCHAR(16) NOT NULL DEFAULT 'user',
            created_at VARCHAR(32) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    )
    conn.commit()
    conn.close()


def seed_default_admin() -> None:
    """若 users 为空则写入默认管理员 admin / 123456。"""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM users;")
        row = cur.fetchone() or {}
        if int(row.get("c") or 0) == 0:
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
    finally:
        conn.close()


def rebuild_database(with_admin: bool = True) -> None:
    """重建业务库并建表；可选写入默认管理员。"""
    conn = get_server_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS `{MYSQL_DATABASE}`;")
        cur.execute(
            f"CREATE DATABASE `{MYSQL_DATABASE}` "
            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        )
        conn.commit()
    finally:
        conn.close()
    init_db()
    if with_admin:
        seed_default_admin()


def fetch_all(sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    if params is None:
        params = ()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        return list(rows)
    finally:
        conn.close()


def ping_db() -> Dict[str, Any]:
    """连接并返回数据库基本信息，便于命令行自检。"""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DATABASE() AS db_name, VERSION() AS version;")
        row = cur.fetchone() or {}
        return {
            "db_name": row.get("db_name"),
            "version": row.get("version"),
        }
    finally:
        conn.close()


if __name__ == "__main__":
    print("[db_utils] 开始数据库自检...")
    print(
        f"[db_utils] 连接信息: host={MYSQL_HOST}, port={MYSQL_PORT}, user={MYSQL_USER}, db={MYSQL_DATABASE}"
    )
    try:
        if "--rebuild" in sys.argv:
            rebuild_database(with_admin=True)
            print("[db_utils] 已重建数据库并写入默认管理员 admin / 123456。")
        info = ping_db()
        print(f"[db_utils] 连接成功: db={info.get('db_name')}, version={info.get('version')}")
        init_db()
        print("[db_utils] 建表检查完成: traffic/users 已就绪。")
    except Exception as exc:
        print(f"[db_utils] 自检失败: {exc}")
        print(traceback.format_exc())
