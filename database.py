"""MySQL connections and schema snapshot queries for Preprod and Production."""

from __future__ import annotations

import os
from typing import Any

import mysql.connector
import pandas as pd
from mysql.connector import MySQLConnection
from dotenv import load_dotenv

load_dotenv()


def _require_plain_env(key: str) -> str:
    """
    Return an app setting value. Key Vault references must be resolved by Azure
    before the process starts; if the literal @Microsoft.KeyVault(...) string is
    still present, MySQL would treat it as a hostname and fail with a cryptic error.
    """
    if key not in os.environ:
        raise KeyError(key)
    v = os.environ[key].strip()
    if v.startswith("@Microsoft.KeyVault"):
        raise RuntimeError(
            f"App setting {key!r} is still an unresolved Key Vault reference (Azure did not inject the secret).\n\n"
            "Fix: Web App → Identity → enable System-assigned managed identity → Save. "
            "Key vault deployprep → Access control (IAM) → Add role assignment → "
            "role “Key Vault Secrets User” → assign to this Web App’s managed identity → "
            "Restart the Web App.\n\n"
            f"Workaround: set {key!r} to the real hostname, user, or password as a plain Application setting (not a @Microsoft.KeyVault reference)."
        )
    return v


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


def _db_port(env_name: str) -> int:
    """
    MySQL port from env. If unset, non-numeric, or still an unresolved Key Vault
    reference string, default to 3306.
    """
    raw = (os.environ.get(env_name) or "3306").strip()
    if not raw or raw.startswith("@Microsoft.KeyVault"):
        return 3306
    try:
        return int(raw)
    except ValueError:
        return 3306


def fetch_preprod_dataframe() -> pd.DataFrame:
    conn = _connect(
        _require_plain_env("PREPROD_DB_HOST"),
        _db_port("PREPROD_DB_PORT"),
        _require_plain_env("PREPROD_DB_USER"),
        _require_plain_env("PREPROD_DB_PASSWORD"),
    )
    try:
        return _read_frame(conn, PREPROD_SQL)
    finally:
        conn.close()


def fetch_prod_dataframe() -> pd.DataFrame:
    conn = _connect(
        _require_plain_env("PROD_DB_HOST"),
        _db_port("PROD_DB_PORT"),
        _require_plain_env("PROD_DB_USER"),
        _require_plain_env("PROD_DB_PASSWORD"),
    )
    try:
        return _read_frame(conn, PROD_SQL)
    finally:
        conn.close()


def fetch_both() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (preprod_df, prod_df)."""
    return fetch_preprod_dataframe(), fetch_prod_dataframe()


def _connect_oasis_preprod() -> MySQLConnection:
    """Connection for authentication table routing (oasis_preprod)."""
    return _connect(
        _require_plain_env("PREPROD_DB_HOST"),
        _db_port("PREPROD_DB_PORT"),
        _require_plain_env("PREPROD_DB_USER"),
        _require_plain_env("PREPROD_DB_PASSWORD"),
        database="oasis_preprod",
    )


def lookup_signup_candidate(email: str) -> dict[str, Any] | None:
    """
    Return pre-approved user row for signup only if FirstName is NULL.
    """
    sql = """
        SELECT `Id`, `EmailAddress`
        FROM `xcl_deploypreps_users`
        WHERE `EmailAddress` = %s
          AND `FirstName` IS NULL
        LIMIT 1
    """
    conn = _connect_oasis_preprod()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, (email,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def username_exists(username: str) -> bool:
    sql = "SELECT 1 FROM `xcl_deploypreps_users` WHERE `UserName` = %s LIMIT 1"
    conn = _connect_oasis_preprod()
    try:
        cur = conn.cursor()
        cur.execute(sql, (username,))
        return cur.fetchone() is not None
    finally:
        conn.close()


def complete_signup(
    email: str,
    first_name: str,
    last_name: str,
    username: str,
    password_hash: str,
) -> bool:
    """
    Finalize signup only for pre-approved rows (FirstName IS NULL).
    """
    sql = """
        UPDATE `xcl_deploypreps_users`
        SET `FirstName` = %s,
            `LastName` = %s,
            `UserName` = %s,
            `PasswordHash` = %s
        WHERE `EmailAddress` = %s
          AND `FirstName` IS NULL
    """
    conn = _connect_oasis_preprod()
    try:
        cur = conn.cursor()
        cur.execute(sql, (first_name, last_name, username, password_hash, email))
        conn.commit()
        return cur.rowcount == 1
    finally:
        conn.close()


def lookup_login_user(username: str) -> dict[str, Any] | None:
    sql = """
        SELECT `Id`, `UserName`, `PasswordHash`, `Access`
        FROM `xcl_deploypreps_users`
        WHERE LOWER(TRIM(`UserName`)) = LOWER(TRIM(%s))
          AND `PasswordHash` IS NOT NULL
          AND TRIM(`PasswordHash`) <> ''
        ORDER BY `Id` DESC
        LIMIT 1
    """
    conn = _connect_oasis_preprod()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, (username,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()
