from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from rich.console import Console

_console = Console(stderr=True)


def generate_trace_id() -> str:
    """Generate a short trace ID using the first 8 characters of a uuid4."""
    return uuid.uuid4().hex[:8]


@dataclass(frozen=True, slots=True)
class TraceContext:
    trace_id: str
    task_id: str
    created_at: str

    @staticmethod
    def create(task_id: str, trace_id: str | None = None) -> TraceContext:
        return TraceContext(
            trace_id=trace_id or generate_trace_id(),
            task_id=task_id,
            created_at=datetime.now(timezone.utc).isoformat(),
        )


def trace_log(
    trace_id: str,
    component: str,
    message: str,
    *,
    step: int | None = None,
    level: str = "info",
) -> None:
    """Structured log output: [trace_id][step_N][component] message."""
    step_tag = f"[step_{step}]" if step is not None else ""
    prefix = f"[{trace_id}]{step_tag}[{component}]"

    style_map = {
        "info": "dim",
        "warn": "yellow",
        "error": "bold red",
        "success": "green",
    }
    style = style_map.get(level, "dim")
    _console.print(f"{prefix} {message}", style=style)
