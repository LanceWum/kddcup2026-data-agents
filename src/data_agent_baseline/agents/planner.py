from __future__ import annotations

from dataclasses import dataclass

from data_agent_baseline.benchmark.schema import PublicTask

_DIFFICULTY_STEPS = {
    "easy": 12,
    "medium": 20,
    "hard": 28,
    "extreme": 32,
}

_DIFFICULTY_PYTHON_TIMEOUT = {
    "easy": 60,
    "medium": 90,
    "hard": 120,
    "extreme": 120,
}


def _format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes / (1024 * 1024):.1f} MB"


@dataclass(frozen=True, slots=True)
class TaskPlan:
    difficulty: str
    context_summary: str
    knowledge_summary: str | None
    data_modalities: list[str]
    recommended_tools: list[str]
    strategy_hint: str
    adaptive_max_steps: int
    python_timeout: int


class TaskPlanner:
    def analyze(self, task: PublicTask) -> TaskPlan:
        context_dir = task.context_dir
        difficulty = task.difficulty

        file_entries: list[str] = []
        modalities: set[str] = set()
        total_size = 0

        for child in sorted(context_dir.rglob("*")):
            if child.is_file():
                rel = child.relative_to(context_dir).as_posix()
                size = child.stat().st_size
                total_size += size
                file_entries.append(f"  {rel} ({_format_size(size)})")
                suffix = child.suffix.lower()
                if suffix in (".csv",):
                    modalities.add("csv")
                elif suffix in (".json",):
                    modalities.add("json")
                elif suffix in (".db", ".sqlite", ".sqlite3"):
                    modalities.add("db")
                elif suffix in (".md", ".txt", ".doc", ".docx"):
                    modalities.add("doc")

        context_summary = f"Total: {len(file_entries)} files, {_format_size(total_size)}\n"
        context_summary += "\n".join(file_entries) if file_entries else "  (empty)"

        knowledge_summary: str | None = None
        knowledge_path = context_dir / "knowledge.md"
        if knowledge_path.exists():
            try:
                text = knowledge_path.read_text(errors="replace")
                knowledge_summary = text[:2000]
                if len(text) > 2000:
                    knowledge_summary += "\n... (truncated)"
            except Exception:
                knowledge_summary = None

        recommended_tools = self._recommend_tools(modalities, difficulty)
        strategy_hint = self._build_strategy(modalities, difficulty, knowledge_summary is not None)

        return TaskPlan(
            difficulty=difficulty,
            context_summary=context_summary,
            knowledge_summary=knowledge_summary,
            data_modalities=sorted(modalities),
            recommended_tools=recommended_tools,
            strategy_hint=strategy_hint,
            adaptive_max_steps=_DIFFICULTY_STEPS.get(difficulty, 16),
            python_timeout=_DIFFICULTY_PYTHON_TIMEOUT.get(difficulty, 60),
        )

    def _recommend_tools(self, modalities: set[str], difficulty: str) -> list[str]:
        tools = ["list_context"]
        if "csv" in modalities:
            tools.append("read_csv")
            tools.append("execute_duckdb_sql")
        if "json" in modalities:
            tools.append("read_json")
        if "db" in modalities:
            tools.append("inspect_sqlite_schema")
            tools.append("execute_context_sql")
        if "doc" in modalities:
            tools.append("read_doc")
        if difficulty in ("hard", "extreme") or len(modalities) > 1:
            tools.append("execute_python")
        tools.append("answer")
        return tools

    def _build_strategy(self, modalities: set[str], difficulty: str, has_knowledge: bool) -> str:
        parts: list[str] = []
        if has_knowledge:
            parts.append("1. Start by reading knowledge.md for domain context and data dictionary.")
        parts.append(f"{'2' if has_knowledge else '1'}. Use list_context to see all available files.")

        if "db" in modalities:
            parts.append("- For .db files: inspect_sqlite_schema → execute_context_sql.")
        if "csv" in modalities:
            parts.append("- For CSV files: use read_csv for small files, execute_duckdb_sql or execute_python for large files.")
        if "json" in modalities:
            parts.append("- For JSON files: use read_json for small files, execute_python for large files.")
        if "doc" in modalities:
            parts.append("- For documents: read_doc to extract text, then analyze content.")
        if difficulty in ("hard", "extreme"):
            parts.append("- Use execute_python for complex multi-step analysis and data aggregation.")

        return "\n".join(parts)
