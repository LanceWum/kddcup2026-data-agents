from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Protocol

from openai import APIConnectionError, APIError, APITimeoutError, OpenAI, RateLimitError

from data_agent_baseline.agents.tracing import trace_log

_LM_STUDIO_PLACEHOLDER_KEY = "lm-studio"


@dataclass(frozen=True, slots=True)
class ModelMessage:
    role: str
    content: str


@dataclass(frozen=True, slots=True)
class ModelStep:
    thought: str
    action: str
    action_input: dict[str, Any]
    raw_response: str


class ModelAdapter(Protocol):
    def complete(self, messages: list[ModelMessage], *, trace_id: str = "") -> str:
        raise NotImplementedError


class OpenAIModelAdapter:
    def __init__(
        self,
        *,
        model: str,
        api_base: str,
        api_key: str,
        temperature: float,
        max_retries: int = 3,
        request_timeout: int = 120,
        connection_timeout: int = 30,
    ) -> None:
        self.model = model
        self.api_base = api_base.rstrip("/")
        self.api_key = api_key or _LM_STUDIO_PLACEHOLDER_KEY
        self.temperature = temperature
        self.max_retries = max_retries
        self.request_timeout = request_timeout
        self.connection_timeout = connection_timeout
        self._client = OpenAI(
            api_key=self.api_key,
            base_url=self.api_base,
            timeout=float(self.request_timeout),
            max_retries=0,
        )

    def complete(self, messages: list[ModelMessage], *, trace_id: str = "") -> str:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                if trace_id:
                    trace_log(trace_id, "model", f"LLM call attempt {attempt}/{self.max_retries} model={self.model}")

                response = self._client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": m.role, "content": m.content} for m in messages],
                    temperature=self.temperature,
                )

                choices = response.choices or []
                if not choices:
                    raise RuntimeError("Model response missing choices.")
                content = choices[0].message.content
                if not isinstance(content, str):
                    raise RuntimeError("Model response missing text content.")

                if trace_id:
                    trace_log(trace_id, "model", f"LLM call succeeded ({len(content)} chars)", level="success")
                return content

            except (APIConnectionError, APITimeoutError) as exc:
                last_error = exc
                if trace_id:
                    trace_log(trace_id, "model", f"Connection/timeout error (attempt {attempt}): {exc}", level="warn")
                if attempt < self.max_retries:
                    backoff = 2 ** (attempt - 1)
                    time.sleep(backoff)
                continue

            except RateLimitError as exc:
                last_error = exc
                if trace_id:
                    trace_log(trace_id, "model", f"Rate limit hit (attempt {attempt}): {exc}", level="warn")
                if attempt < self.max_retries:
                    backoff = 2 ** attempt
                    time.sleep(backoff)
                continue

            except APIError as exc:
                last_error = exc
                if trace_id:
                    trace_log(trace_id, "model", f"API error (attempt {attempt}): {exc}", level="error")
                if attempt < self.max_retries:
                    time.sleep(1)
                continue

        if trace_id:
            trace_log(trace_id, "model", f"All {self.max_retries} attempts failed", level="error")
        raise RuntimeError(f"Model request failed after {self.max_retries} attempts: {last_error}") from last_error


class ScriptedModelAdapter:
    def __init__(self, responses: list[str]) -> None:
        self._responses = list(responses)

    def complete(self, messages: list[ModelMessage], *, trace_id: str = "") -> str:
        del messages
        if not self._responses:
            raise RuntimeError("No scripted model responses remaining.")
        return self._responses.pop(0)
