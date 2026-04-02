from __future__ import annotations

import duckdb
from pathlib import Path
from typing import Any


def execute_duckdb_sql(context_root: Path, sql: str, *, limit: int = 200) -> dict[str, object]:
    normalized_sql = sql.lstrip().lower()
    if not normalized_sql.startswith(("select", "with", "pragma", "describe", "show")):
        raise ValueError("Only read-only SQL statements are allowed.")

    resolved_root = context_root.resolve()
    conn = duckdb.connect(":memory:")
    try:
        original_dir = Path.cwd()
        import os
        os.chdir(resolved_root)
        try:
            result = conn.execute(sql)
            column_names = [desc[0] for desc in result.description or []]
            rows = result.fetchmany(limit + 1)
        finally:
            os.chdir(original_dir)
    finally:
        conn.close()

    truncated = len(rows) > limit
    limited_rows = rows[:limit]
    return {
        "columns": column_names,
        "rows": [list(row) for row in limited_rows],
        "row_count": len(limited_rows),
        "truncated": truncated,
    }


def data_profile(context_root: Path, relative_path: str) -> dict[str, object]:
    resolved_root = context_root.resolve()
    file_path = (resolved_root / relative_path).resolve()

    if resolved_root not in file_path.parents and file_path != resolved_root:
        raise ValueError(f"Path escapes context dir: {relative_path}")
    if not file_path.exists():
        raise FileNotFoundError(f"Missing context asset: {relative_path}")

    suffix = file_path.suffix.lower()
    conn = duckdb.connect(":memory:")
    try:
        if suffix == ".csv":
            table_ref = f"read_csv_auto('{file_path.as_posix()}')"
        elif suffix == ".json":
            table_ref = f"read_json_auto('{file_path.as_posix()}')"
        elif suffix in (".parquet",):
            table_ref = f"read_parquet('{file_path.as_posix()}')"
        else:
            return {
                "path": relative_path,
                "error": f"Unsupported file type for profiling: {suffix}",
                "file_size": file_path.stat().st_size,
            }

        count_result = conn.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()
        row_count = count_result[0] if count_result else 0

        desc_result = conn.execute(f"DESCRIBE {table_ref}").fetchall()
        columns_info: list[dict[str, Any]] = []
        for row in desc_result:
            columns_info.append({
                "name": row[0],
                "type": row[1],
            })

        null_counts: dict[str, int] = {}
        for col_info in columns_info:
            col_name = col_info["name"]
            try:
                null_result = conn.execute(
                    f'SELECT COUNT(*) FROM {table_ref} WHERE "{col_name}" IS NULL'
                ).fetchone()
                null_counts[col_name] = null_result[0] if null_result else 0
            except Exception:
                null_counts[col_name] = -1

        for col_info in columns_info:
            name = col_info["name"]
            null_count = null_counts.get(name, 0)
            col_info["null_count"] = null_count
            col_info["null_pct"] = round(null_count / row_count * 100, 1) if row_count > 0 else 0.0

        return {
            "path": relative_path,
            "file_size": file_path.stat().st_size,
            "row_count": row_count,
            "column_count": len(columns_info),
            "columns": columns_info,
        }
    except Exception as exc:
        return {
            "path": relative_path,
            "error": str(exc),
            "file_size": file_path.stat().st_size,
        }
    finally:
        conn.close()
