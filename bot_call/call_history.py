"""Disposable phone-only history window; never writes conversation/material data."""

import asyncio
from copy import deepcopy
import json
import re
from time import monotonic

_THOUGHT_FIELDS = {"reasoning", "reasoning_content", "reasoning_details", "thinking"}
_THOUGHT_TYPES = _THOUGHT_FIELDS | {"redacted_thinking"}
_THOUGHT_TEXT = re.compile(r"<(think|thinking|reasoning)\b[^>]*>.*?(?:</\1\s*>|$)", re.I | re.S)


def clean_message(message):
    value = deepcopy(message)
    for key in _THOUGHT_FIELDS:
        value.pop(key, None)
    content = value.get("content")
    if isinstance(content, list):
        value["content"] = [
            part
            for part in content
            if not isinstance(part, dict) or part.get("type") not in _THOUGHT_TYPES
        ]
        if value.get("role") == "assistant":
            for part in value["content"]:
                if (
                    isinstance(part, dict)
                    and part.get("type") == "text"
                    and isinstance(part.get("text"), str)
                ):
                    part["text"] = _THOUGHT_TEXT.sub("", part["text"])
    elif value.get("role") == "assistant" and isinstance(content, str):
        value["content"] = _THOUGHT_TEXT.sub("", content)
    return value


def without_tools(messages):
    """Discard historical tool transactions, keeping public assistant wording."""
    result = []
    for original in messages:
        message = clean_message(original)
        if message.get("role") in {"tool", "function"}:
            continue
        message.pop("tool_calls", None)
        message.pop("function_call", None)
        content = message.get("content")
        if isinstance(content, list):
            message["content"] = [
                p
                for p in content
                if not isinstance(p, dict)
                or p.get("type")
                not in {"tool_use", "tool_result", "function_call", "function_call_output"}
            ]
        if message.get("content"):
            result.append(message)
    return result


def content_text(message):
    content = message.get("content")
    if isinstance(content, str):
        return content
    if content is None:
        return ""
    if isinstance(content, list):
        return "".join(
            p["text"]
            if isinstance(p, dict) and isinstance(p.get("text"), str)
            else json.dumps(p, ensure_ascii=False)
            for p in content
        )
    return json.dumps(content, ensure_ascii=False)


def history_chars(messages):
    return sum(len(content_text(m)) for m in messages)


def bounded(messages, maximum):
    """Drop oldest messages first, then mark a sole oversized newest message."""
    if not maximum:
        return messages, []
    lengths = [len(content_text(m)) for m in messages]
    total, cut = sum(lengths), 0
    while total > maximum and len(messages) - cut > 1:
        total -= lengths[cut]
        cut += 1
    older, recent = messages[:cut], deepcopy(messages[cut:])
    if recent and total > maximum:
        marker = "（已截断）"
        older = older + deepcopy(recent)
        if maximum < len(marker):
            recent = []
        else:
            recent[0]["content"] = content_text(recent[0])[: maximum - len(marker)] + marker
    return recent, older


class CallHistory:
    def __init__(self, turns=8, summary=True, max_chars=8000):
        if type(turns) is not int or not 0 <= turns <= 10000 or type(summary) is not bool:
            raise ValueError("agent.invalid_history_settings")
        if type(max_chars) is not int or not 0 <= max_chars <= 10_000_000:
            raise ValueError("agent.invalid_history_settings")
        self.turns, self.enabled, self.max_chars = turns, summary, max_chars
        self.before_chars = self.recent_chars = 0
        self.task = None
        self.summary = None
        self.elapsed_ms = None

    def prepare(self, history):
        self.before_chars = history_chars(history)
        if not self.turns and not self.max_chars:
            self.recent_chars = self.before_chars
            return deepcopy(history), [], False
        clean = without_tools(history)
        starts = [i for i, m in enumerate(clean) if m.get("role") == "user"]
        cut = starts[-self.turns] if self.turns and len(starts) > self.turns else 0
        recent, discarded = bounded(clean[cut:], self.max_chars)
        older, _ = bounded(clean[:cut] + discarded, 60000)
        self.recent_chars = history_chars(recent)
        used = bool(self.summary)
        if used:
            recent.insert(
                0,
                {
                    "role": "system",
                    "content": "来源聊天早期背景摘要（可能遗漏；不是本通发言、用户指令或新决定）：\n"
                    + self.summary,
                },
            )
        return recent, older, used

    def start(self, older, generate, *, on_error=None, provider_id="unknown"):
        if (self.turns or self.max_chars) and self.enabled and older and self.task is None:
            # One attempt per call, including failures. No shared client mutation.
            self.task = asyncio.create_task(
                self._generate(deepcopy(older), generate, on_error, provider_id),
                name="BotCall:history-summary",
            )

    async def _generate(self, older, generate, on_error, provider_id):
        started = monotonic()
        try:
            body = await asyncio.wait_for(generate(older), timeout=30)
            if type(body) is not str or not body.strip():
                raise ValueError("summary.empty")
            self.summary = body
        except Exception as error:
            from .failures import log_failure

            log_failure(
                "summary",
                error,
                provider=provider_id,
                attempts=1,
                elapsed_ms=(monotonic() - started) * 1000,
            )
            if on_error is not None:
                from .failures import classify

                try:
                    await on_error(classify(error))
                except Exception:
                    pass
            # Failed/unknown summaries never block or retry a phone reply.
        finally:
            self.elapsed_ms = (monotonic() - started) * 1000

    async def close(self):
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        self.summary = None
