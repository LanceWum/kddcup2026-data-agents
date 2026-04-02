from pathlib import Path
from time import perf_counter

import typer
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from data_agent_baseline.benchmark.dataset import DABenchPublicDataset
from data_agent_baseline.config import load_app_config
from data_agent_baseline.run.runner import TaskRunArtifacts, create_run_output_dir, run_benchmark, run_single_task
from data_agent_baseline.tools.filesystem import list_context_tree

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIGS_DIR = PROJECT_ROOT / "configs"
DATA_DIR = PROJECT_ROOT / "data"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
ARTIFACT_RUNS_DIR = ARTIFACTS_DIR / "runs"
DEFAULT_GOLD_DIR = PROJECT_ROOT / "data" / "public" / "output"

app = typer.Typer(add_completion=False, no_args_is_help=False)
console = Console()


def _status_value(path: Path) -> str:
    return "present" if path.exists() else "missing"


def _format_compact_rate(completed_count: int, elapsed_seconds: float) -> str:
    if completed_count <= 0 or elapsed_seconds <= 0:
        return "rate=0.0 task/min"
    return f"rate={(completed_count / elapsed_seconds) * 60:.1f} task/min"


def _format_last_task(artifact: TaskRunArtifacts | None) -> str:
    if artifact is None:
        return "last=-"
    status = "ok" if artifact.succeeded else "fail"
    return f"last={artifact.task_id} ({status})"


def _build_compact_progress_fields(
    *,
    completed_count: int,
    succeeded_count: int,
    failed_count: int,
    task_total: int,
    max_workers: int,
    elapsed_seconds: float,
    last_artifact: TaskRunArtifacts | None,
) -> dict[str, str]:
    remaining_count = max(task_total - completed_count, 0)
    running_count = min(max_workers, remaining_count)
    queued_count = max(remaining_count - running_count, 0)
    return {
        "ok": str(succeeded_count),
        "fail": str(failed_count),
        "run": str(running_count),
        "queue": str(queued_count),
        "speed": _format_compact_rate(completed_count, elapsed_seconds),
        "last": _format_last_task(last_artifact),
    }


@app.callback()
def cli() -> None:
    """Utilities for working with the local DABench baseline project."""


@app.command()
def status(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="YAML config path."),
) -> None:
    """Show the local project layout and public dataset presence."""
    app_config = load_app_config(config)
    config_path = config.resolve()
    public_dataset = DABenchPublicDataset(app_config.dataset.root_path)

    table = Table(title="DABench Baseline Status")
    table.add_column("Item")
    table.add_column("Path")
    table.add_column("State")

    table.add_row("project_root", str(PROJECT_ROOT), "ready")
    table.add_row("data_dir", str(DATA_DIR), _status_value(DATA_DIR))
    table.add_row("configs_dir", str(CONFIGS_DIR), _status_value(CONFIGS_DIR))
    table.add_row("artifacts_dir", str(ARTIFACTS_DIR), _status_value(ARTIFACTS_DIR))
    table.add_row("runs_dir", str(ARTIFACT_RUNS_DIR), _status_value(ARTIFACT_RUNS_DIR))
    table.add_row("dataset_root", str(app_config.dataset.root_path), _status_value(app_config.dataset.root_path))
    table.add_row("config_path", str(config_path), _status_value(config_path))

    console.print(table)

    if public_dataset.exists:
        console.print(f"Public tasks: {len(public_dataset.list_task_ids())}")
        counts = public_dataset.task_counts()
        if counts:
            rendered_counts = ", ".join(
                f"{difficulty}={count}" for difficulty, count in sorted(counts.items())
            )
            console.print(f"Public task counts: {rendered_counts}")


@app.command("inspect-task")
def inspect_task(
    task_id: str,
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="YAML config path."),
) -> None:
    """Show task metadata and available context files."""
    app_config = load_app_config(config)
    dataset = DABenchPublicDataset(app_config.dataset.root_path)
    task = dataset.get_task(task_id)
    console.print(f"Task: {task.task_id}")
    console.print(f"Difficulty: {task.difficulty}")
    console.print(f"Question: {task.question}")
    context_listing = list_context_tree(task)
    table = Table(title=f"Context Files for {task.task_id}")
    table.add_column("Path")
    table.add_column("Kind")
    table.add_column("Size")
    for entry in context_listing["entries"]:
        table.add_row(str(entry["path"]), str(entry["kind"]), str(entry["size"] or ""))
    console.print(table)


@app.command("run-task")
def run_task_command(
    task_id: str,
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="YAML config path."),
) -> None:
    """Run the ReAct baseline on one task."""
    app_config = load_app_config(config)
    try:
        _, run_output_dir = create_run_output_dir(app_config.run.output_dir, run_id=app_config.run.run_id)
    except (ValueError, FileExistsError) as exc:
        raise typer.BadParameter(str(exc), param_hint="run.run_id") from exc
    artifacts = run_single_task(task_id=task_id, config=app_config, run_output_dir=run_output_dir)

    console.print(f"Run output: {run_output_dir}")
    console.print(f"Task output: {artifacts.task_output_dir}")
    console.print(f"Trace ID: {artifacts.trace_id}")
    if artifacts.prediction_csv_path is not None:
        console.print(f"Prediction CSV: {artifacts.prediction_csv_path}")
    else:
        console.print("Prediction CSV: not generated")
    if artifacts.failure_reason is not None:
        console.print(f"Failure: {artifacts.failure_reason}")


@app.command("run-benchmark")
def run_benchmark_command(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="YAML config path."),
    limit: int | None = typer.Option(None, min=1, help="Maximum number of tasks to run."),
    difficulty: str | None = typer.Option(None, help="Filter tasks by difficulty level (easy/medium/hard/extreme)."),
) -> None:
    """Run the ReAct baseline on multiple tasks from the config selection."""
    app_config = load_app_config(config)
    dataset = DABenchPublicDataset(app_config.dataset.root_path)

    if difficulty:
        tasks_list = dataset.iter_tasks(difficulty=difficulty)
    else:
        tasks_list = dataset.iter_tasks()

    task_total = len(tasks_list)
    if limit is not None:
        task_total = min(task_total, limit)
    effective_workers = app_config.run.max_workers

    progress_columns = [
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[dim]|[/dim]"),
        TextColumn("[green]ok={task.fields[ok]}[/green]"),
        TextColumn("[red]fail={task.fields[fail]}[/red]"),
        TextColumn("[cyan]run={task.fields[run]}[/cyan]"),
        TextColumn("[yellow]queue={task.fields[queue]}[/yellow]"),
        TextColumn("[dim]|[/dim]"),
        TextColumn("{task.fields[speed]}"),
        TextColumn("[dim]| elapsed[/dim]"),
        TimeElapsedColumn(),
        TextColumn("[dim]| eta[/dim]"),
        TimeRemainingColumn(),
        TextColumn("[dim]|[/dim]"),
        TextColumn("{task.fields[last]}"),
    ]
    with Progress(*progress_columns, console=console) as progress:
        progress_task_id = progress.add_task(
            "Benchmark",
            total=task_total,
            completed=0,
            **_build_compact_progress_fields(
                completed_count=0,
                succeeded_count=0,
                failed_count=0,
                task_total=task_total,
                max_workers=effective_workers,
                elapsed_seconds=0.0,
                last_artifact=None,
            ),
        )

        completion_count = 0
        succeeded_count = 0
        failed_count = 0
        start_time = perf_counter()

        def on_task_complete(artifact) -> None:
            nonlocal completion_count, succeeded_count, failed_count
            completion_count += 1
            if artifact.succeeded:
                succeeded_count += 1
            else:
                failed_count += 1
            progress.update(
                progress_task_id,
                completed=completion_count,
                description="Benchmark",
                refresh=True,
                **_build_compact_progress_fields(
                    completed_count=completion_count,
                    succeeded_count=succeeded_count,
                    failed_count=failed_count,
                    task_total=task_total,
                    max_workers=effective_workers,
                    elapsed_seconds=perf_counter() - start_time,
                    last_artifact=artifact,
                ),
            )

        try:
            run_output_dir, artifacts = run_benchmark(
                config=app_config,
                limit=limit,
                progress_callback=on_task_complete,
            )
        except (ValueError, FileExistsError) as exc:
            raise typer.BadParameter(str(exc), param_hint="run.run_id") from exc
        progress.update(
            progress_task_id,
            completed=task_total,
            description="Benchmark",
            refresh=True,
            **_build_compact_progress_fields(
                completed_count=task_total,
                succeeded_count=succeeded_count,
                failed_count=failed_count,
                task_total=task_total,
                max_workers=effective_workers,
                elapsed_seconds=perf_counter() - start_time,
                last_artifact=artifacts[-1] if artifacts else None,
            ),
        )
    console.print(f"Run output: {run_output_dir}")
    console.print(f"Tasks attempted: {len(artifacts)}")
    console.print(f"Succeeded tasks: {sum(1 for item in artifacts if item.succeeded)}")


@app.command("evaluate")
def evaluate_command(
    run_dir: Path = typer.Argument(..., help="Path to a run output directory (e.g. artifacts/runs/<run_id>)."),
    gold_dir: Path = typer.Option(None, help="Path to gold output directory. Defaults to data/public/output."),
) -> None:
    """Evaluate predictions against gold answers."""
    from data_agent_baseline.evaluation.evaluator import evaluate_run

    effective_gold = gold_dir or DEFAULT_GOLD_DIR
    if not run_dir.exists():
        console.print(f"[red]Run directory not found: {run_dir}[/red]")
        raise typer.Exit(1)

    result = evaluate_run(run_dir, effective_gold)

    table = Table(title="Evaluation Results")
    table.add_column("Task ID")
    table.add_column("Passed")
    table.add_column("Message")
    for tr in result.task_results:
        status_str = "[green]PASS[/green]" if tr.passed else "[red]FAIL[/red]"
        table.add_row(tr.task_id, status_str, tr.message)
    console.print(table)
    console.print(f"Total: {result.total} | Passed: {result.passed} | Failed: {result.failed} | Skipped: {result.skipped}")
    console.print(f"Accuracy: {result.accuracy:.1%}")


@app.command("register-failures")
def register_failures_command(
    summary_path: Path = typer.Argument(..., help="Path to summary.json from a benchmark run."),
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="YAML config path."),
) -> None:
    """Import failed tasks from a summary.json into the failure registry."""
    from data_agent_baseline.harness.failure_registry import FailureRegistry

    app_config = load_app_config(config)
    registry = FailureRegistry()
    registry_path = app_config.harness.harness_dir / "failure_registry.json"
    registry.load(registry_path)

    count = registry.import_from_summary(summary_path)
    registry.save(registry_path)

    console.print(f"Imported {count} new failures from {summary_path}")
    console.print(f"Total open failures: {len(registry.get_open_failures())}")
    console.print(f"Registry saved to: {registry_path}")


@app.command("run-regression")
def run_regression_command(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="YAML config path."),
) -> None:
    """Run regression tests on all open failure cases."""
    from data_agent_baseline.harness.failure_registry import FailureRegistry
    from data_agent_baseline.harness.regression import RegressionRunner

    app_config = load_app_config(config)
    registry = FailureRegistry()
    registry_path = app_config.harness.harness_dir / "failure_registry.json"
    registry.load(registry_path)

    open_count = len(registry.get_open_failures())
    if open_count == 0:
        console.print("[green]No open failures to test.[/green]")
        return

    console.print(f"Running regression on {open_count} open failures...")
    runner = RegressionRunner()
    report = runner.run(app_config, registry)

    registry.save(registry_path)

    table = Table(title=f"Regression Report (run_id={report.run_id})")
    table.add_column("Category")
    table.add_column("Count")
    table.add_column("Tasks")
    table.add_row("[green]Newly Fixed[/green]", str(len(report.newly_fixed)), ", ".join(report.newly_fixed) or "-")
    table.add_row("[red]Still Failing[/red]", str(len(report.still_failing)), ", ".join(report.still_failing) or "-")
    table.add_row("[yellow]Newly Broken[/yellow]", str(len(report.newly_broken)), ", ".join(report.newly_broken) or "-")
    console.print(table)
    console.print(f"Fix rate: {len(report.newly_fixed)}/{report.total_tested}")


@app.command("regression-report")
def regression_report_command(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="YAML config path."),
) -> None:
    """Show current failure registry status."""
    from data_agent_baseline.harness.failure_registry import FailureRegistry

    app_config = load_app_config(config)
    registry = FailureRegistry()
    registry_path = app_config.harness.harness_dir / "failure_registry.json"

    if not registry_path.exists():
        console.print("[yellow]No failure registry found. Run register-failures first.[/yellow]")
        return

    registry.load(registry_path)
    open_failures = registry.get_open_failures()
    resolved = [f for f in registry.failures if f.status == "resolved"]

    table = Table(title="Failure Registry")
    table.add_column("Task ID")
    table.add_column("Difficulty")
    table.add_column("Status")
    table.add_column("Failure Reason")
    table.add_column("First Seen")
    for entry in registry.failures:
        status_str = "[green]resolved[/green]" if entry.status == "resolved" else "[red]open[/red]"
        reason = entry.failure_reason[:60] + "..." if len(entry.failure_reason) > 60 else entry.failure_reason
        table.add_row(entry.task_id, entry.difficulty, status_str, reason, entry.first_seen_run_id)
    console.print(table)
    console.print(f"Open: {len(open_failures)} | Resolved: {len(resolved)} | Total: {len(registry.failures)}")


def main() -> None:
    app()
