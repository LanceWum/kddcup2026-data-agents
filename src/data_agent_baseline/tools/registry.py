from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from data_agent_baseline.agents.tracing import trace_log
from data_agent_baseline.benchmark.schema import AnswerTable, PublicTask
from data_agent_baseline.tools.duckdb_tools import data_profile, execute_duckdb_sql
from data_agent_baseline.tools.filesystem import (
    list_context_tree,
    read_csv_preview,
    read_doc_preview,
    read_json_preview,
    resolve_context_path,
)
from data_agent_baseline.tools.python_exec import execute_python_code
from data_agent_baseline.tools.sqlite import execute_read_only_sql, inspect_sqlite_schema

EXECUTE_PYTHON_TIMEOUT_SECONDS = 60


@dataclass(frozen=True, slots=True)
class ToolSpec:
    name: str
    description: str
    input_schema: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ToolExecutionResult:
    ok: bool
    content: dict[str, Any]
    is_terminal: bool = False
    answer: AnswerTable | None = None


ToolHandler = Callable[[PublicTask, dict[str, Any]], ToolExecutionResult]


def _list_context(task: PublicTask, action_input: dict[str, Any]) -> ToolExecutionResult:
    max_depth = int(action_input.get("max_depth", 4))
    return ToolExecutionResult(ok=True, content=list_context_tree(task, max_depth=max_depth))


def _read_csv(task: PublicTask, action_input: dict[str, Any]) -> ToolExecutionResult:
    path = str(action_input["path"])
    max_rows = int(action_input.get("max_rows", 20))
    offset = int(action_input.get("offset", 0))
    return ToolExecutionResult(
        ok=True, content=read_csv_preview(task, path, max_rows=max_rows, offset=offset)
    )


def _read_json(task: PublicTask, action_input: dict[str, Any]) -> ToolExecutionResult:
    path = str(action_input["path"])
    max_chars = int(action_input.get("max_chars", 4000))
    schema_only = bool(action_input.get("schema_only", False))
    return ToolExecutionResult(
        ok=True, content=read_json_preview(task, path, max_chars=max_chars, schema_only=schema_only)
    )


def _read_doc(task: PublicTask, action_input: dict[str, Any]) -> ToolExecutionResult:
    path = str(action_input["path"])
    max_chars = int(action_input.get("max_chars", 4000))
    offset = int(action_input.get("offset", 0))
    return ToolExecutionResult(
        ok=True, content=read_doc_preview(task, path, max_chars=max_chars, offset=offset)
    )


def _inspect_sqlite_schema(task: PublicTask, action_input: dict[str, Any]) -> ToolExecutionResult:
    path = resolve_context_path(task, str(action_input["path"]))
    return ToolExecutionResult(ok=True, content=inspect_sqlite_schema(path))


def _execute_context_sql(task: PublicTask, action_input: dict[str, Any]) -> ToolExecutionResult:
    path = resolve_context_path(task, str(action_input["path"]))
    sql = str(action_input["sql"])
    limit = int(action_input.get("limit", 200))
    return ToolExecutionResult(ok=True, content=execute_read_only_sql(path, sql, limit=limit))


def _execute_python(task: PublicTask, action_input: dict[str, Any]) -> ToolExecutionResult:
    code = str(action_input["code"])
    timeout = int(action_input.get("timeout", EXECUTE_PYTHON_TIMEOUT_SECONDS))
    content = execute_python_code(
        context_root=task.context_dir,
        code=code,
        timeout_seconds=timeout,
    )
    return ToolExecutionResult(ok=bool(content.get("success")), content=content)


def _execute_duckdb_sql(task: PublicTask, action_input: dict[str, Any]) -> ToolExecutionResult:
    sql = str(action_input["sql"])
    limit = int(action_input.get("limit", 200))
    content = execute_duckdb_sql(task.context_dir, sql, limit=limit)
    return ToolExecutionResult(ok=True, content=content)


def _data_profile(task: PublicTask, action_input: dict[str, Any]) -> ToolExecutionResult:
    path = str(action_input["path"])
    content = data_profile(task.context_dir, path)
    return ToolExecutionResult(ok="error" not in content, content=content)


def _answer(_: PublicTask, action_input: dict[str, Any]) -> ToolExecutionResult:
    columns = action_input.get("columns")
    rows = action_input.get("rows")
    if not isinstance(columns, list) or not columns or not all(isinstance(item, str) for item in columns):
        raise ValueError("answer.columns must be a non-empty list of strings.")
    if not isinstance(rows, list):
        raise ValueError("answer.rows must be a list.")

    normalized_rows: list[list[Any]] = []
    for row in rows:
        if not isinstance(row, list):
            raise ValueError("Each answer row must be a list.")
        if len(row) != len(columns):
            raise ValueError("Each answer row must match the number of columns.")
        normalized_rows.append(list(row))

    answer = AnswerTable(columns=list(columns), rows=normalized_rows)
    validation_errors = answer.validate()
    if validation_errors:
        raise ValueError(f"Answer validation failed: {'; '.join(validation_errors)}")

    return ToolExecutionResult(
        ok=True,
        content={
            "status": "submitted",
            "column_count": len(columns),
            "row_count": len(normalized_rows),
        },
        is_terminal=True,
        answer=answer,
    )


@dataclass(slots=True)
class ToolRegistry:
    specs: dict[str, ToolSpec]
    handlers: dict[str, ToolHandler]
    python_timeout: int = EXECUTE_PYTHON_TIMEOUT_SECONDS

    def describe_for_prompt(self) -> str:
        lines = []
        for name in sorted(self.specs):
            spec = self.specs[name]
            lines.append(f"- {spec.name}: {spec.description}")
            lines.append(f"  input_schema: {spec.input_schema}")
        return "\n".join(lines)

    def execute(
        self,
        task: PublicTask,
        action: str,
        action_input: dict[str, Any],
        *,
        trace_id: str = "",
    ) -> ToolExecutionResult:
        if action not in self.handlers:
            raise KeyError(f"Unknown tool: {action}")
        if trace_id:
            trace_log(trace_id, "tool", f"Executing tool={action}")
        result = self.handlers[action](task, action_input)
        if trace_id:
            status = "ok" if result.ok else "fail"
            trace_log(trace_id, "tool", f"Tool={action} {status}", level="success" if result.ok else "warn")
        return result


def create_default_tool_registry(*, python_timeout: int = EXECUTE_PYTHON_TIMEOUT_SECONDS) -> ToolRegistry:
    specs = {
        "answer": ToolSpec(
            name="answer",
            description="Submit the final answer table and terminate the task. columns must be a list of column name strings, rows must be a list of lists.",
            input_schema={
                "columns": ["column_name"],
                "rows": [["value_1"]],
            },
        ),
        "data_profile": ToolSpec(
            name="data_profile",
            description="Get a quick overview of a CSV/JSON file: row count, column count, column names and types, null percentages. Use this before writing queries to understand data structure.",
            input_schema={"path": "relative/path/to/file.csv"},
        ),
        "execute_context_sql": ToolSpec(
            name="execute_context_sql",
            description="Run a read-only SQL query against a SQLite/DB file inside context. Returns columns and rows.",
            input_schema={"path": "relative/path/to/file.db", "sql": "SELECT ...", "limit": 200},
        ),
        "execute_duckdb_sql": ToolSpec(
            name="execute_duckdb_sql",
            description=(
                "Run a read-only SQL query using DuckDB. Can directly query CSV/JSON files using "
                "read_csv_auto('path') or read_json_auto('path') in FROM clause. Supports cross-file JOINs. "
                "File paths are relative to context directory. Example: SELECT * FROM read_csv_auto('csv/data.csv') LIMIT 10"
            ),
            input_schema={"sql": "SELECT * FROM read_csv_auto('csv/data.csv') LIMIT 10", "limit": 200},
        ),
        "execute_python": ToolSpec(
            name="execute_python",
            description=(
                "Execute Python code with the task context directory as working directory. "
                "Pre-imported: pandas (pd), numpy (np), duckdb, Path. "
                "Print results to stdout. "
                f"Timeout: {python_timeout} seconds."
            ),
            input_schema={
                "code": "import pandas as pd\ndf = pd.read_csv('csv/data.csv')\nprint(df.head())",
            },
        ),
        "inspect_sqlite_schema": ToolSpec(
            name="inspect_sqlite_schema",
            description="Inspect tables, columns, and row counts in a SQLite/DB file. Use this before writing SQL queries.",
            input_schema={"path": "relative/path/to/file.db"},
        ),
        "list_context": ToolSpec(
            name="list_context",
            description="List all files and directories under the task context directory with their sizes.",
            input_schema={"max_depth": 4},
        ),
        "read_csv": ToolSpec(
            name="read_csv",
            description="Read a preview of a CSV file. Returns column names and up to max_rows data rows. Supports offset for pagination.",
            input_schema={"path": "relative/path/to/file.csv", "max_rows": 20, "offset": 0},
        ),
        "read_doc": ToolSpec(
            name="read_doc",
            description="Read a text document (Markdown, TXT, etc). Supports offset for reading large documents in chunks.",
            input_schema={"path": "relative/path/to/file.md", "max_chars": 4000, "offset": 0},
        ),
        "read_json": ToolSpec(
            name="read_json",
            description="Read a preview of a JSON file. Set schema_only=true for arrays to get structure info without loading all data.",
            input_schema={"path": "relative/path/to/file.json", "max_chars": 4000, "schema_only": False},
        ),
    }
    handlers = {
        "answer": _answer,
        "data_profile": _data_profile,
        "execute_context_sql": _execute_context_sql,
        "execute_duckdb_sql": _execute_duckdb_sql,
        "execute_python": _execute_python,
        "inspect_sqlite_schema": _inspect_sqlite_schema,
        "list_context": _list_context,
        "read_csv": _read_csv,
        "read_doc": _read_doc,
        "read_json": _read_json,
    }
    return ToolRegistry(specs=specs, handlers=handlers, python_timeout=python_timeout)
