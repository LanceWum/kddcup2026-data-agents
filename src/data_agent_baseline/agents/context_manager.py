from __future__ import annotations

from data_agent_baseline.agents.model import ModelMessage

_CHARS_PER_TOKEN = 4


class ContextWindowManager:
    def __init__(self, max_tokens: int = 120_000) -> None:
        self.max_tokens = max_tokens

    def estimate_tokens(self, messages: list[ModelMessage]) -> int:
        total_chars = sum(len(m.content) for m in messages)
        return total_chars // _CHARS_PER_TOKEN

    def compress_if_needed(
        self,
        messages: list[ModelMessage],
        *,
        keep_recent_steps: int = 6,
    ) -> list[ModelMessage]:
        estimated = self.estimate_tokens(messages)
        if estimated <= int(self.max_tokens * 0.85):
            return messages

        if len(messages) < 4:
            return messages

        system_msg = messages[0]
        task_msg = messages[1]
        conversation = messages[2:]

        pairs: list[tuple[ModelMessage, ModelMessage]] = []
        i = 0
        while i + 1 < len(conversation):
            pairs.append((conversation[i], conversation[i + 1]))
            i += 2
        if i < len(conversation):
            pairs.append((conversation[i], ModelMessage(role="user", content="")))

        keep_count = min(keep_recent_steps, len(pairs))
        early_pairs = pairs[: len(pairs) - keep_count]
        recent_pairs = pairs[len(pairs) - keep_count :]

        compressed: list[ModelMessage] = [system_msg, task_msg]
        for assistant_msg, user_msg in early_pairs:
            summary = self._summarize_step(assistant_msg.content)
            compressed.append(ModelMessage(role="assistant", content=summary))
            if user_msg.content:
                obs_summary = self._summarize_observation(user_msg.content)
                compressed.append(ModelMessage(role="user", content=obs_summary))

        for assistant_msg, user_msg in recent_pairs:
            compressed.append(assistant_msg)
            if user_msg.content:
                compressed.append(user_msg)

        return compressed

    def truncate_observation(self, observation: dict, *, max_chars: int = 8000) -> dict:
        import json
        rendered = json.dumps(observation, ensure_ascii=False)
        if len(rendered) <= max_chars:
            return observation

        result = dict(observation)
        content = result.get("content")
        if isinstance(content, dict):
            content_str = json.dumps(content, ensure_ascii=False)
            if len(content_str) > max_chars:
                result["content"] = {"_truncated": True, "preview": content_str[:max_chars - 100]}
                result["_note"] = f"Content truncated from {len(content_str)} to {max_chars - 100} chars"
        elif isinstance(content, str) and len(content) > max_chars:
            result["content"] = content[:max_chars - 100]
            result["_note"] = f"Content truncated from {len(content)} to {max_chars - 100} chars"

        return result

    def _summarize_step(self, assistant_content: str) -> str:
        import json
        try:
            import re
            fence = re.search(r"```json\s*(.*?)\s*```", assistant_content, re.DOTALL | re.IGNORECASE)
            if fence:
                payload = json.loads(fence.group(1))
                thought = payload.get("thought", "")[:100]
                action = payload.get("action", "unknown")
                return f'```json\n{{"thought":"{thought}","action":"{action}","action_input":{{}}}}\n```'
        except Exception:
            pass
        return assistant_content[:200] + "..." if len(assistant_content) > 200 else assistant_content

    def _summarize_observation(self, user_content: str) -> str:
        if len(user_content) <= 500:
            return user_content
        return user_content[:400] + "\n... (observation compressed) ..."
