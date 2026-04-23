import os
import sqlite3

import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, "traffic.db")


def main():
    conn = sqlite3.connect(DB)

    print("=== area stats ===")
    print(
        pd.read_sql_query(
            "SELECT area, COUNT(*) AS c, SUM(total_flow) AS s FROM traffic GROUP BY area", conn
        )
    )

    print("\n=== road_type stats ===")
    print(
        pd.read_sql_query(
            "SELECT road_type, COUNT(*) AS c, AVG(total_flow) AS avg FROM traffic GROUP BY road_type",
            conn,
        )
    )

    print("\n=== road_name top 10 by total_flow ===")
    print(
        pd.read_sql_query(
            "SELECT road_name, SUM(total_flow) AS s, COUNT(*) AS c "
            "FROM traffic GROUP BY road_name ORDER BY s DESC LIMIT 10",
            conn,
        )
    )

    print("\n=== weekday avg ===")
    print(
        pd.read_sql_query(
            "SELECT strftime('%w', ts) AS w, AVG(total_flow) AS avg "
            "FROM traffic GROUP BY w ORDER BY w",
            conn,
        )
    )

    conn.close()


if __name__ == "__main__":
    main()

