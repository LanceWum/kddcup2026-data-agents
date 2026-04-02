from __future__ import annotations

import json
import re
from dataclasses import dataclass

from data_agent_baseline.agents.context_manager import ContextWindowManager
from data_agent_baseline.agents.model import ModelAdapter, ModelMessage, ModelStep
from data_agent_baseline.agents.planner import TaskPlan, TaskPlanner
from data_agent_baseline.agents.prompt import (
    build_difficulty_hints,
    build_observation_prompt,
    build_system_prompt,
    build_task_prompt,
)
from data_agent_baseline.agents.runtime import AgentRunResult, AgentRuntimeState, StepRecord
from data_agent_baseline.agents.tracing import generate_trace_id, trace_log
from data_agent_baseline.benchmark.schema import PublicTask
from data_agent_baseline.tools.registry import ToolRegistry

_MAX_PARSE_RETRIES = 2


@dataclass(frozen=True, slots=True)
class ReActAgentConfig:
    max_steps: int = 16
    use_planner: bool = True
    use_enhanced_prompt: bool = True


def _strip_json_fence(raw_response: str) -> str:
    text = raw_response.strip()
    fence_match = re.search(r"```json\s*(.*?)\s*```", text, flags=re.IGNORECASE | re.DOTALL)
    if fence_match is not None:
        return fence_match.group(1).strip()
    generic_fence_match = re.search(r"```\s*(.*?)\s*```", text, flags=re.DOTALL)
    if generic_fence_match is not None:
        return generic_fence_match.group(1).strip()
    return text


def _load_single_json_object(text: str) -> dict[str, object]:
    payload, end = json.JSONDecoder().raw_decode(text)
    remainder = text[end:].strip()
    if remainder:
        cleaned_remainder = re.sub(r"(?:\\[nrt])+", "", remainder).strip()
        if cleaned_remainder:
            raise ValueError("Model response must contain only one JSON object.")
    if not isinstance(payload, dict):
        raise ValueError("Model response must be a JSON object.")
    return payload


def parse_model_step(raw_response: str) -> ModelStep:
    normalized = _strip_json_fence(raw_response)
    payload = _load_single_json_object(normalized)

    thought = payload.get("thought", payload.get("Thought", ""))
    action = payload.get("action", payload.get("Action"))
    action_input = payload.get("action_input", payload.get("Action_input", payload.get("action_Input", {})))

    if not isinstance(thought, str):
        thought = str(thought)
    if not isinstance(action, str) or not action:
        raise ValueError("action must be a non-empty string.")
    if action_input is None:
        action_input = {}
    if not isinstance(action_input, dict):
        raise ValueError("action_input must be a JSON object.")

    return ModelStep(
        thought=thought,
        action=action,
        action_input=action_input,
        raw_response=raw_response,
    )


class ReActAgent:
    def __init__(
        self,
        *,
        model: ModelAdapter,
        tools: ToolRegistry,
        config: ReActAgentConfig | None = None,
        system_prompt: str | None = None,
        planner: TaskPlanner | None = None,
        context_manager: ContextWindowManager | None = None,
    ) -> None:
        self.model = model
        self.tools = tools
        self.config = config or ReActAgentConfig()
        self.system_prompt = system_prompt
        self.planner = planner or (TaskPlanner() if self.config.use_planner else None)
        self.context_manager = context_manager or ContextWindowManager()

    def _build_messages(
        self,
        task: PublicTask,
        state: AgentRuntimeState,
        plan: TaskPlan | None = None,
    ) -> list[ModelMessage]:
        system_content = build_system_prompt(
            self.tools.describe_for_prompt(),
            system_prompt=self.system_prompt,
            use_enhanced=self.config.use_enhanced_prompt,
        )
        messages = [ModelMessage(role="system", content=system_content)]

        context_summary = plan.context_summary if plan else None
        difficulty_hint = build_difficulty_hints(task.difficulty) if plan else None

        task_content = build_task_prompt(
            task,
            context_summary=context_summary,
            difficulty_hint=difficulty_hint,
        )

        if plan and plan.strategy_hint:
            task_content += f"\n\n## Recommended Strategy\n{plan.strategy_hint}"

        if plan and plan.knowledge_summary:
            task_content += f"\n\n## Domain Knowledge (from knowledge.md)\n{plan.knowledge_summary}"

        messages.append(ModelMessage(role="user", content=task_content))

        for step in state.steps:
            messages.append(ModelMessage(role="assistant", content=step.raw_response))
            messages.append(
                ModelMessage(role="user", content=build_observation_prompt(step.observation))
            )
        return messages

    def run(self, task: PublicTask, *, trace_id: str | None = None) -> AgentRunResult:
        effective_trace_id = trace_id or generate_trace_id()
        trace_log(effective_trace_id, "agent", f"Task {task.task_id} started (difficulty={task.difficulty})")

        plan: TaskPlan | None = None
        effective_max_steps = self.config.max_steps

        if self.planner:
            try:
                plan = self.planner.analyze(task)
                effective_max_steps = plan.adaptive_max_steps
                trace_log(effective_trace_id, "planner",
                          f"Plan: modalities={plan.data_modalities}, max_steps={effective_max_steps}")
            except Exception as exc:
                trace_log(effective_trace_id, "planner", f"Planning failed: {exc}", level="warn")

        state = AgentRuntimeState()
        state.plan = plan.strategy_hint if plan else None

        for step_index in range(1, effective_max_steps + 1):
            messages = self._build_messages(task, state, plan)
            messages = self.context_manager.compress_if_needed(messages)

            trace_log(effective_trace_id, "agent", f"Step {step_index}/{effective_max_steps}", step=step_index)

            raw_response = self.model.complete(messages, trace_id=effective_trace_id)

            parse_error_count = 0
            model_step = None
            while parse_error_count <= _MAX_PARSE_RETRIES:
                try:
                    model_step = parse_model_step(raw_response)
                    break
                except Exception as parse_exc:
                    parse_error_count += 1
                    if parse_error_count > _MAX_PARSE_RETRIES:
                        trace_log(effective_trace_id, "agent",
                                  f"Parse failed after {_MAX_PARSE_RETRIES} retries: {parse_exc}", step=step_index, level="error")
                        observation = {"ok": False, "error": f"Parse error: {parse_exc}"}
                        state.steps.append(StepRecord(
                            step_index=step_index,
                            thought="",
                            action="__parse_error__",
                            action_input={},
                            raw_response=raw_response,
                            observation=observation,
                            ok=False,
                            trace_id=effective_trace_id,
                        ))
                        break

                    trace_log(effective_trace_id, "agent",
                              f"Parse retry {parse_error_count}: {parse_exc}", step=step_index, level="warn")
                    retry_messages = messages + [
                        ModelMessage(role="assistant", content=raw_response),
                        ModelMessage(role="user", content=(
                            f"Your response could not be parsed: {parse_exc}\n"
                            "Please respond with exactly one ```json fenced block containing "
                            "a JSON object with keys `thought`, `action`, and `action_input`."
                        )),
                    ]
                    raw_response = self.model.complete(retry_messages, trace_id=effective_trace_id)

            if model_step is None:
                continue

            try:
                tool_result = self.tools.execute(
                    task, model_step.action, model_step.action_input,
                    trace_id=effective_trace_id,
                )
                observation = self.context_manager.truncate_observation({
                    "ok": tool_result.ok,
                    "tool": model_step.action,
                    "content": tool_result.content,
                })
                step_record = StepRecord(
                    step_index=step_index,
                    thought=model_step.thought,
                    action=model_step.action,
                    action_input=model_step.action_input,
                    raw_response=raw_response,
                    observation=observation,
                    ok=tool_result.ok,
                    trace_id=effective_trace_id,
                )
                state.steps.append(step_record)
                if tool_result.is_terminal:
                    state.answer = tool_result.answer
                    trace_log(effective_trace_id, "agent", "Answer submitted", step=step_index, level="success")
                    break
            except Exception as exc:
                observation = {"ok": False, "error": str(exc)}
                state.steps.append(StepRecord(
                    step_index=step_index,
                    thought=model_step.thought if model_step else "",
                    action=model_step.action if model_step else "__error__",
                    action_input=model_step.action_input if model_step else {},
                    raw_response=raw_response,
                    observation=observation,
                    ok=False,
                    trace_id=effective_trace_id,
                ))

        if state.answer is None and state.failure_reason is None:
            state.failure_reason = "Agent did not submit an answer within max_steps."
            trace_log(effective_trace_id, "agent", state.failure_reason, level="error")

        result = AgentRunResult(
            task_id=task.task_id,
            answer=state.answer,
            steps=list(state.steps),
            failure_reason=state.failure_reason,
            trace_id=effective_trace_id,
        )
        trace_log(effective_trace_id, "agent",
                  f"Task {task.task_id} finished: {'succeeded' if result.succeeded else 'failed'}", level="success" if result.succeeded else "error")
        return result
