from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass(slots=True)
class FailureEntry:
    task_id: str
    difficulty: str
    failure_reason: str
    first_seen_run_id: str
    first_seen_at: str
    status: str = "open"
    notes: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> FailureEntry:
        return FailureEntry(**{k: v for k, v in data.items() if k in FailureEntry.__dataclass_fields__})


@dataclass(slots=True)
class FailureRegistry:
    failures: list[FailureEntry] = field(default_factory=list)
    _path: Path | None = None

    def load(self, path: Path) -> None:
        self._path = path
        if not path.exists():
            self.failures = []
            return
        data = json.loads(path.read_text())
        self.failures = [FailureEntry.from_dict(entry) for entry in data.get("failures", [])]

    def save(self, path: Path | None = None) -> None:
        target = path or self._path
        if target is None:
            raise ValueError("No path specified for saving failure registry.")
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {"failures": [f.to_dict() for f in self.failures]}
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")

    def register(
        self,
        task_id: str,
        difficulty: str,
        reason: str,
        run_id: str,
    ) -> bool:
        for entry in self.failures:
            if entry.task_id == task_id and entry.status == "open":
                return False
        self.failures.append(
            FailureEntry(
                task_id=task_id,
                difficulty=difficulty,
                failure_reason=reason,
                first_seen_run_id=run_id,
                first_seen_at=datetime.now(timezone.utc).isoformat(),
                status="open",
            )
        )
        return True

    def import_from_summary(self, summary_path: Path) -> int:
        data = json.loads(summary_path.read_text())
        run_id = data.get("run_id", "unknown")
        count = 0
        for task_info in data.get("tasks", []):
            if not task_info.get("succeeded", False):
                task_id = task_info["task_id"]
                reason = task_info.get("failure_reason", "Unknown failure")
                difficulty = task_info.get("difficulty", "unknown")
                if self.register(task_id, difficulty, reason, run_id):
                    count += 1
        return count

    def mark_resolved(self, task_id: str) -> bool:
        for entry in self.failures:
            if entry.task_id == task_id and entry.status == "open":
                entry.status = "resolved"
                return True
        return False

    def get_open_failures(self) -> list[FailureEntry]:
        return [f for f in self.failures if f.status == "open"]

    def get_task_ids_open(self) -> list[str]:
        return [f.task_id for f in self.failures if f.status == "open"]
