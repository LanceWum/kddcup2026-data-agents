from __future__ import annotations

import json

from data_agent_baseline.benchmark.schema import PublicTask


REACT_SYSTEM_PROMPT = """
You are a ReAct-style data agent.

You are solving a task from a public dataset. You may only inspect files inside the task's `context/` directory through the provided tools.

Rules:
1. Use tools to inspect the available context before answering.
2. Base your answer only on information you can observe through the provided tools.
3. The task is complete only when you call the `answer` tool.
4. The `answer` tool must receive a table with `columns` and `rows`.
5. Always return exactly one JSON object with keys `thought`, `action`, and `action_input`.
6. Always wrap that JSON object in exactly one fenced code block that starts with ```json and ends with ```.
7. Do not output any text before or after the fenced JSON block.

Keep reasoning concise and grounded in the observed data.
""".strip()

ENHANCED_SYSTEM_PROMPT = """
You are an expert ReAct-style data analysis agent.

You are solving a task from a data analysis benchmark. You may only inspect files inside the task's `context/` directory through the provided tools.

## Analysis Strategy Rules

1. **Always start by reading `knowledge.md`** if it exists in the context directory. It contains critical domain knowledge, schema descriptions, and data dictionaries that are essential for correctly interpreting the data.
2. **Then use `list_context`** to understand the full directory structure and file sizes before diving into any specific file.
3. **For large CSV/JSON files (>1MB)**, prefer `execute_duckdb_sql` or `execute_python` over `read_csv`/`read_json` to avoid truncated previews. Use SQL aggregations or Python pandas to compute results directly.
4. **For database files (.db/.sqlite)**, always use `inspect_sqlite_schema` first to understand the schema, then use `execute_context_sql` for precise queries.
5. **Use `data_profile`** to quickly understand file structure (row count, column types, basic stats) before writing complex queries.
6. **When combining data from multiple sources**, plan your JOINs carefully. Use `execute_python` or `execute_duckdb_sql` for cross-file analysis.
7. **Verify your results** before submitting. If a query returns unexpected results (empty, too many rows, etc.), re-examine the data and query logic.
8. **Pay attention to data types**: dates may need parsing, numeric columns may contain strings, column names may have spaces or special characters.
9. **For text/document analysis tasks**, read the full document content, extract structured information, then compute the answer.
10. **Always ensure your final answer matches the expected format**: correct column names, appropriate data types, and complete rows.

## Response Format

Always return exactly one JSON object with keys `thought`, `action`, and `action_input`.
Always wrap that JSON object in exactly one fenced code block that starts with ```json and ends with ```.
Do not output any text before or after the fenced JSON block.
Keep reasoning concise and grounded in the observed data.
""".strip()

RESPONSE_EXAMPLES = """
Example response when you need to inspect the context:
```json
{"thought":"I should inspect the available files first.","action":"list_context","action_input":{"max_depth":4}}
```

Example response when you have the final answer:
```json
{"thought":"I have the final result table.","action":"answer","action_input":{"columns":["average_long_shots"],"rows":[["63.5"]]}}
```
""".strip()

DIFFICULTY_HINTS = {
    "easy": (
        "This is an EASY task. The data is in structured files (CSV/JSON) with a knowledge document. "
        "Approach: Read knowledge.md, inspect the data files, then write Python code or SQL to extract the answer directly. "
        "A single well-crafted query or script should suffice."
    ),
    "medium": (
        "This is a MEDIUM task involving structured files, databases, and knowledge documents. "
        "Approach: Read knowledge.md first, then inspect all data sources (CSV/JSON/DB). "
        "You may need Text-to-SQL queries and cross-source data analysis. Plan your JOINs and aggregations carefully."
    ),
    "hard": (
        "This is a HARD task with structured files, databases, data documents, and knowledge documents (~10K-128K tokens). "
        "Approach: Read knowledge.md first. For documents, read them in chunks if needed. "
        "Combine information from unstructured documents with structured data. Reason carefully over the extracted information."
    ),
    "extreme": (
        "This is an EXTREME task with ultra-long document inputs (>128K tokens). "
        "Approach: Read knowledge.md first. For very large documents, use targeted searches and chunked reading. "
        "Use execute_python to process large files programmatically rather than reading them entirely. "
        "Focus on extracting only the relevant information needed to answer the question."
    ),
}


def build_difficulty_hints(difficulty: str) -> str:
    return DIFFICULTY_HINTS.get(difficulty, "")


def build_system_prompt(
    tool_descriptions: str,
    system_prompt: str | None = None,
    *,
    use_enhanced: bool = True,
) -> str:
    if use_enhanced:
        base_prompt = system_prompt or ENHANCED_SYSTEM_PROMPT
    else:
        base_prompt = system_prompt or REACT_SYSTEM_PROMPT
    return (
        f"{base_prompt}\n\n"
        "Available tools:\n"
        f"{tool_descriptions}\n\n"
        f"{RESPONSE_EXAMPLES}\n\n"
        "You must always return a single ```json fenced block containing one JSON object "
        "with keys `thought`, `action`, and `action_input`, and no extra text."
    )


def build_task_prompt(
    task: PublicTask,
    *,
    context_summary: str | None = None,
    difficulty_hint: str | None = None,
) -> str:
    parts = []
    if difficulty_hint:
        parts.append(f"## Task Difficulty\n{difficulty_hint}")
    if context_summary:
        parts.append(f"## Available Context Files\n{context_summary}")
    parts.append(f"## Question\n{task.question}")
    parts.append(
        "All tool file paths are relative to the task context directory. "
        "When you have the final table, call the `answer` tool."
    )
    return "\n\n".join(parts)


def build_observation_prompt(observation: dict[str, object]) -> str:
    rendered = json.dumps(observation, ensure_ascii=False, indent=2)
    return f"Observation:\n{rendered}"
