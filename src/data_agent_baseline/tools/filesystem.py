from __future__ import annotations

import csv
import json
from pathlib import Path

from data_agent_baseline.benchmark.schema import PublicTask


def resolve_context_path(task: PublicTask, relative_path: str) -> Path:
    candidate = (task.context_dir / relative_path).resolve()
    context_root = task.context_dir.resolve()
    if context_root not in candidate.parents and candidate != context_root:
        raise ValueError(f"Path escapes context dir: {relative_path}")
    if not candidate.exists():
        raise FileNotFoundError(f"Missing context asset: {relative_path}")
    return candidate


def list_context_tree(task: PublicTask, *, max_depth: int = 4) -> dict[str, object]:
    entries: list[dict[str, object]] = []

    def walk(path: Path, depth: int) -> None:
        if depth > max_depth:
            return
        for child in sorted(path.iterdir(), key=lambda item: (item.is_file(), item.name)):
            rel_path = child.relative_to(task.context_dir).as_posix()
            entries.append(
                {
                    "path": rel_path,
                    "kind": "dir" if child.is_dir() else "file",
                    "size": child.stat().st_size if child.is_file() else None,
                }
            )
            if child.is_dir():
                walk(child, depth + 1)

    walk(task.context_dir, 1)
    return {
        "root": str(task.context_dir),
        "entries": entries,
    }


def read_csv_preview(
    task: PublicTask,
    relative_path: str,
    *,
    max_rows: int = 20,
    offset: int = 0,
) -> dict[str, object]:
    path = resolve_context_path(task, relative_path)
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        rows = list(reader)

    if not rows:
        return {
            "path": relative_path,
            "columns": [],
            "rows": [],
            "row_count": 0,
            "offset": 0,
        }

    header = rows[0]
    data_rows = rows[1:]
    sliced = data_rows[offset : offset + max_rows]
    return {
        "path": relative_path,
        "columns": header,
        "rows": sliced,
        "row_count": len(data_rows),
        "offset": offset,
        "returned": len(sliced),
    }


def read_json_preview(
    task: PublicTask,
    relative_path: str,
    *,
    max_chars: int = 4000,
    schema_only: bool = False,
) -> dict[str, object]:
    path = resolve_context_path(task, relative_path)
    payload = json.loads(path.read_text())

    if schema_only and isinstance(payload, list) and len(payload) > 0:
        first = payload[0]
        schema_info = {}
        if isinstance(first, dict):
            schema_info = {k: type(v).__name__ for k, v in first.items()}
        return {
            "path": relative_path,
            "type": "array",
            "length": len(payload),
            "element_schema": schema_info,
            "sample": payload[:3],
        }

    preview = json.dumps(payload, ensure_ascii=False, indent=2)
    result: dict[str, object] = {
        "path": relative_path,
        "preview": preview[:max_chars],
        "truncated": len(preview) > max_chars,
    }
    if isinstance(payload, list):
        result["type"] = "array"
        result["length"] = len(payload)
    elif isinstance(payload, dict):
        result["type"] = "object"
        result["keys"] = list(payload.keys())[:50]
    return result


def read_doc_preview(
    task: PublicTask,
    relative_path: str,
    *,
    max_chars: int = 4000,
    offset: int = 0,
) -> dict[str, object]:
    path = resolve_context_path(task, relative_path)
    text = path.read_text(errors="replace")
    total_chars = len(text)
    sliced = text[offset : offset + max_chars]
    return {
        "path": relative_path,
        "preview": sliced,
        "total_chars": total_chars,
        "offset": offset,
        "truncated": (offset + max_chars) < total_chars,
    }
