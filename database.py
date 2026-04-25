"""MySQL connections and schema snapshot queries for Preprod and Production."""

from __future__ import annotations

import os
from typing import Any

import mysql.connector
import pandas as pd
from mysql.connector import MySQLConnection
from dotenv import load_dotenv

load_dotenv()

PREPROD_SQL = """
SELECT
    CONCAT(
        LEFT(c.TABLE_SCHEMA, LENGTH(c.TABLE_SCHEMA) - 8),
        '_',
        c.TABLE_NAME,
        '-',
        c.COLUMN_NAME
    ) AS database_tablename_columnname,
    c.COLUMN_NAME AS preprod_column_name,
    c.COLUMN_TYPE AS preprod_column_type,
    c.COLUMN_KEY AS preprod_column_key,
    c.COLLATION_NAME AS preprod_collation_name,
    LEFT(c.TABLE_SCHEMA, LENGTH(c.TABLE_SCHEMA) - 8) AS database_name,
    c.TABLE_NAME AS table_name,
    t.TABLE_ROWS AS table_rows,
    ROUND((t.DATA_LENGTH + t.INDEX_LENGTH) / (1024 * 1024 * 1024), 10) AS size_gb
FROM information_schema.columns c
LEFT JOIN information_schema.tables t
    ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
    AND t.TABLE_NAME = c.TABLE_NAME
WHERE c.TABLE_SCHEMA LIKE '%preprod'
    AND c.TABLE_SCHEMA NOT LIKE 'dmz%%'
    AND c.TABLE_NAME NOT LIKE '%%Xcl_%%'
"""

PROD_SQL = """
SELECT
    CONCAT(
        LEFT(c.TABLE_SCHEMA, LENGTH(c.TABLE_SCHEMA) - 5),
        '_',
        c.TABLE_NAME,
        '-',
        c.COLUMN_NAME
    ) AS database_tablename_columnname,
    c.COLUMN_NAME AS prod_column_name,
    c.COLUMN_TYPE AS prod_column_type,
    c.COLUMN_KEY AS prod_column_key,
    c.COLLATION_NAME AS prod_collation_name,
    LEFT(c.TABLE_SCHEMA, LENGTH(c.TABLE_SCHEMA) - 5) AS database_name,
    c.TABLE_NAME AS table_name,
    t.TABLE_ROWS AS table_rows,
    ROUND((t.DATA_LENGTH + t.INDEX_LENGTH) / (1024 * 1024 * 1024), 10) AS size_gb
FROM information_schema.columns c
LEFT JOIN information_schema.tables t
    ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
    AND t.TABLE_NAME = c.TABLE_NAME
WHERE RIGHT(c.TABLE_SCHEMA, 4) = 'prod'
    AND c.TABLE_NAME NOT LIKE '%%Xcl%%'
    AND c.TABLE_NAME NOT LIKE '%%view2026%%'
    AND c.TABLE_NAME NOT LIKE '%%view2025%%'
"""


def _connect(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str | None = None,
) -> MySQLConnection:
    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
    )


def _read_frame(conn: MySQLConnection, sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, conn)


def fetch_preprod_dataframe() -> pd.DataFrame:
    conn = _connect(
        os.environ["PREPROD_DB_HOST"],
        int(os.environ.get("PREPROD_DB_PORT", "3306")),
        os.environ["PREPROD_DB_USER"],
        os.environ["PREPROD_DB_PASSWORD"],
    )
    try:
        return _read_frame(conn, PREPROD_SQL)
    finally:
        conn.close()


def fetch_prod_dataframe() -> pd.DataFrame:
    conn = _connect(
        os.environ["PROD_DB_HOST"],
        int(os.environ.get("PROD_DB_PORT", "3306")),
        os.environ["PROD_DB_USER"],
        os.environ["PROD_DB_PASSWORD"],
    )
    try:
        return _read_frame(conn, PROD_SQL)
    finally:
        conn.close()


def fetch_both() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (preprod_df, prod_df)."""
    return fetch_preprod_dataframe(), fetch_prod_dataframe()
