from __future__ import annotations

import json
from dataclasses import dataclass

from data_agent_baseline.config import AppConfig
from data_agent_baseline.harness.failure_registry import FailureRegistry


@dataclass(frozen=True, slots=True)
class RegressionReport:
    run_id: str
    total_tested: int
    newly_fixed: list[str]
    still_failing: list[str]
    newly_broken: list[str]

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "total_tested": self.total_tested,
            "newly_fixed": self.newly_fixed,
            "still_failing": self.still_failing,
            "newly_broken": self.newly_broken,
            "fix_rate": f"{len(self.newly_fixed)}/{self.total_tested}" if self.total_tested > 0 else "0/0",
        }


class RegressionRunner:
    def run(
        self,
        config: AppConfig,
        registry: FailureRegistry,
    ) -> RegressionReport:
        from data_agent_baseline.run.runner import (
            build_model_adapter,
            create_run_output_dir,
            run_single_task,
        )
        from data_agent_baseline.tools.registry import create_default_tool_registry

        open_task_ids = registry.get_task_ids_open()
        if not open_task_ids:
            return RegressionReport(
                run_id="no_failures",
                total_tested=0,
                newly_fixed=[],
                still_failing=[],
                newly_broken=[],
            )

        run_id, run_output_dir = create_run_output_dir(
            config.run.output_dir, run_id=None
        )

        model = build_model_adapter(config)
        tools = create_default_tool_registry()

        newly_fixed: list[str] = []
        still_failing: list[str] = []

        for task_id in open_task_ids:
            try:
                artifact = run_single_task(
                    task_id=task_id,
                    config=config,
                    run_output_dir=run_output_dir,
                    model=model,
                    tools=tools,
                )
                if artifact.succeeded:
                    newly_fixed.append(task_id)
                    registry.mark_resolved(task_id)
                else:
                    still_failing.append(task_id)
            except Exception:
                still_failing.append(task_id)

        report = RegressionReport(
            run_id=run_id,
            total_tested=len(open_task_ids),
            newly_fixed=newly_fixed,
            still_failing=still_failing,
            newly_broken=[],
        )

        report_path = config.harness.harness_dir / f"regression_{run_id}.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2) + "\n")

        return report
