"""Pandas-based schema diff: categorize upcoming changes (Preprod vs Production)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

KEY_COL = "database_tablename_columnname"


def _is_blank(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and np.isnan(v):
        return True
    if isinstance(v, str) and not v.strip():
        return True
    return False


def norm_str(v: Any) -> str:
    """Normalize for comparisons (NaN/None/NaT -> '')."""
    if v is None:
        return ""
    if isinstance(v, float) and np.isnan(v):
        return ""
    if isinstance(v, str) and v.strip().lower() in {"nan", "none"}:
        return ""
    if pd.isna(v):
        return ""
    return str(v).strip()


@dataclass
class SchemaDiffResult:
    table1_new_tables: pd.DataFrame
    table2_field_changes: pd.DataFrame
    table3_index_added: pd.DataFrame
    table4_datatype_changes: pd.DataFrame
    preprod_row_count: int
    prod_row_count: int
    error: str | None = None


def _table_stats_for(
    prod: pd.DataFrame,
    pre: pd.DataFrame,
    database_name: str,
    table_name: str,
) -> tuple[Any, Any]:
    """Prefer Prod rows/size; fall back to Preprod when Prod is missing or zero/null."""
    p = prod[
        (prod["database_name"].map(norm_str) == norm_str(database_name))
        & (prod["table_name"].map(norm_str) == norm_str(table_name))
    ]
    prep = pre[
        (pre["database_name"].map(norm_str) == norm_str(database_name))
        & (pre["table_name"].map(norm_str) == norm_str(table_name))
    ]

    def pick_rows_size(frame: pd.DataFrame) -> tuple[Any, Any]:
        if frame.empty:
            return None, None
        rows = frame["table_rows"].max()
        size = frame["size_gb"].max()
        return rows, size

    rows_p, size_p = pick_rows_size(p)
    rows_prep, size_prep = pick_rows_size(prep)

    def is_missing_or_zero_rows(r: Any) -> bool:
        if r is None:
            return True
        if isinstance(r, float) and np.isnan(r):
            return True
        try:
            return int(r) == 0
        except (TypeError, ValueError):
            return _is_blank(r)

    def is_missing_or_zero_size(s: Any) -> bool:
        if s is None:
            return True
        if isinstance(s, float) and np.isnan(s):
            return True
        try:
            return float(s) == 0.0
        except (TypeError, ValueError):
            return _is_blank(s)

    rows_out = rows_p
    if is_missing_or_zero_rows(rows_p):
        rows_out = rows_prep if not is_missing_or_zero_rows(rows_prep) else rows_p

    size_out = size_p
    if is_missing_or_zero_size(size_p):
        size_out = size_prep if not is_missing_or_zero_size(size_prep) else size_p

    return rows_out, size_out


def compute_schema_diff(pre: pd.DataFrame, prod: pd.DataFrame) -> SchemaDiffResult:
    pre = pre.copy()
    prod = prod.copy()

    for col in [KEY_COL, "database_name", "table_name"]:
        if col not in pre.columns or col not in prod.columns:
            return SchemaDiffResult(
                pd.DataFrame(),
                pd.DataFrame(),
                pd.DataFrame(),
                pd.DataFrame(),
                len(pre),
                len(prod),
                error=f"Missing expected column {col!r} in inputs.",
            )

    pre_keys = set(pre[KEY_COL].dropna().astype(str).map(lambda x: norm_str(x)))
    prod_keys = set(prod[KEY_COL].dropna().astype(str).map(lambda x: norm_str(x)))
    pre_keys.discard("")
    prod_keys.discard("")

    prep_table_groups = pre.groupby(["database_name", "table_name"], dropna=False)
    prod_table_groups = prod.groupby(["database_name", "table_name"], dropna=False)

    new_tables: list[dict[str, Any]] = []
    new_tables_set: set[tuple[str, str]] = set()
    removed_tables_set: set[tuple[str, str]] = set()

    for (db, tbl), g in prep_table_groups:
        keys = {norm_str(k) for k in g[KEY_COL].tolist()}
        keys.discard("")
        if not keys:
            continue
        if not (keys & prod_keys):
            db_s, tbl_s = norm_str(db), norm_str(tbl)
            new_tables.append({"new_table": tbl_s, "database": db_s})
            new_tables_set.add((db_s, tbl_s))

    for (db, tbl), g in prod_table_groups:
        keys = {norm_str(k) for k in g[KEY_COL].tolist()}
        keys.discard("")
        if not keys:
            continue
        if not (keys & pre_keys):
            db_s, tbl_s = norm_str(db), norm_str(tbl)
            removed_tables_set.add((db_s, tbl_s))

    new_keys = pre_keys - prod_keys
    removed_keys = prod_keys - pre_keys

    new_columns_by_table: dict[tuple[str, str], list[str]] = {}
    removed_columns_by_table: dict[tuple[str, str], list[str]] = {}

    for k in new_keys:
        row = pre.loc[pre[KEY_COL].map(norm_str) == k].head(1)
        if row.empty:
            continue
        db = norm_str(row.iloc[0]["database_name"])
        tbl = norm_str(row.iloc[0]["table_name"])
        if (db, tbl) in new_tables_set:
            continue
        new_columns_by_table.setdefault((db, tbl), []).append(norm_str(row.iloc[0]["preprod_column_name"]))

    for k in removed_keys:
        row = prod.loc[prod[KEY_COL].map(norm_str) == k].head(1)
        if row.empty:
            continue
        db = norm_str(row.iloc[0]["database_name"])
        tbl = norm_str(row.iloc[0]["table_name"])
        if (db, tbl) in removed_tables_set:
            continue
        removed_columns_by_table.setdefault((db, tbl), []).append(norm_str(row.iloc[0]["prod_column_name"]))

    field_rows: list[dict[str, Any]] = []
    all_partial_tables = set(new_columns_by_table) | set(removed_columns_by_table)
    for db, tbl in sorted(all_partial_tables):
        added = ", ".join(sorted(new_columns_by_table.get((db, tbl), [])))
        removed_cols = ", ".join(sorted(removed_columns_by_table.get((db, tbl), [])))
        rows, size = _table_stats_for(prod, pre, db, tbl)
        field_rows.append(
            {
                "table": tbl,
                "database": db,
                "new_columns_added": added,
                "columns_removed": removed_cols,
                "prod_table_rows": rows,
                "prod_table_size_gb": size,
            }
        )

    merged = pd.merge(
        pre,
        prod,
        on=KEY_COL,
        how="outer",
        suffixes=("_pre", "_prod"),
        indicator=True,
    )

    if "database_name_pre" in merged.columns:
        merged["database_name"] = merged["database_name_pre"].combine_first(merged["database_name_prod"])
    if "table_name_pre" in merged.columns:
        merged["table_name"] = merged["table_name_pre"].combine_first(merged["table_name_prod"])

    both = merged[merged["_merge"] == "both"].copy()
    both["preprod_column_key_n"] = both["preprod_column_key"].map(norm_str)
    both["prod_column_key_n"] = both["prod_column_key"].map(norm_str)
    both["preprod_column_type_n"] = both["preprod_column_type"].map(norm_str)
    both["prod_column_type_n"] = both["prod_column_type"].map(norm_str)

    index_changes = both[both["preprod_column_key_n"] != both["prod_column_key_n"]].copy()
    dtype_changes = both[both["preprod_column_type_n"] != both["prod_column_type_n"]].copy()

    index_rows: list[dict[str, Any]] = []
    for _, r in index_changes.iterrows():
        prep_k = r["preprod_column_key_n"]
        prod_k = r["prod_column_key_n"]
        if _is_blank(prep_k) and not _is_blank(prod_k):
            continue
        if prep_k == prod_k:
            continue
        colname = norm_str(r.get("preprod_column_name", r.get("prod_column_name", "")))
        if _is_blank(colname):
            colname = norm_str(r.get("prod_column_name", ""))
        if _is_blank(prep_k):
            continue
        if _is_blank(prod_k):
            label = f"{colname} → {prep_k}"
        else:
            label = f"{colname}: {prod_k} → {prep_k}"
        index_rows.append(
            {
                "table_name": norm_str(r.get("table_name", "")),
                "database": norm_str(r.get("database_name", "")),
                "index_added": label,
            }
        )

    dtype_rows: list[dict[str, Any]] = []
    for _, r in dtype_changes.iterrows():
        dtype_rows.append(
            {
                "table_name": norm_str(r.get("table_name", "")),
                "database": norm_str(r.get("database_name", "")),
                "column": norm_str(r.get("preprod_column_name", r.get("prod_column_name", ""))),
                "new_datatype_length": norm_str(r.get("preprod_column_type", "")),
            }
        )

    t1 = pd.DataFrame(new_tables)
    if not t1.empty:
        t1 = t1.rename(columns={"new_table": "New Table(s)", "database": "Database"})
    else:
        t1 = pd.DataFrame(columns=["New Table(s)", "Database"])

    t2 = pd.DataFrame(field_rows)
    if not t2.empty:
        t2 = t2.rename(
            columns={
                "table": "Table",
                "database": "Database",
                "new_columns_added": "New Columns Added",
                "columns_removed": "Columns Removed",
                "prod_table_rows": "Prod Table Rows",
                "prod_table_size_gb": "Prod Table Size (GB)",
            }
        )
    else:
        t2 = pd.DataFrame(
            columns=[
                "Table",
                "Database",
                "New Columns Added",
                "Columns Removed",
                "Prod Table Rows",
                "Prod Table Size (GB)",
            ]
        )

    t3 = pd.DataFrame(index_rows)
    if not t3.empty:
        t3 = t3.rename(
            columns={
                "table_name": "Table Name",
                "database": "Database",
                "index_added": "Index Added",
            }
        )
    else:
        t3 = pd.DataFrame(columns=["Table Name", "Database", "Index Added"])

    t4 = pd.DataFrame(dtype_rows)
    if not t4.empty:
        t4 = t4.rename(
            columns={
                "table_name": "Table Name",
                "database": "Database",
                "column": "Column",
                "new_datatype_length": "New DataType / Length",
            }
        )
    else:
        t4 = pd.DataFrame(columns=["Table Name", "Database", "Column", "New DataType / Length"])

    return SchemaDiffResult(
        table1_new_tables=t1,
        table2_field_changes=t2,
        table3_index_added=t3,
        table4_datatype_changes=t4,
        preprod_row_count=len(pre),
        prod_row_count=len(prod),
        error=None,
    )
