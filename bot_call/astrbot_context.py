"""Fixed-cid review import for the verified AstrBot ConversationManager API.

Importing this module loads no AstrBot, Provider, Store or publication module.
Construction belongs on the actual plugin business loop. Storage readback alone
is not a model-input acknowledgement. No worker/storage lifecycle is wired here.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import hashlib
import json

from .astrbot_adapter import LoopMismatch, SnapshotError, _check_messages, _reject_json_constant
from .binding import BindingError, CallBinding, validate_conversation


_SOURCE_PREFIX = "系统导入的本通电话回顾，非用户原话\nBot-Call review source v1\n"


class ContextRejected(ValueError):
    """The fixed target, history or source pair was rejected without overwriting."""


class ContextUncertain(RuntimeError):
    """An import or its readback cannot be confirmed; never blindly retry."""


@dataclass(frozen=True, slots=True)
class ImportReceipt:
    """Only a core-storage readback, never model availability or delivery proof."""

    call_id: str = field(repr=False)
    source_key: str = field(repr=False)
    body_sha256: str = field(repr=False)
    already_present: bool


def _source(binding: CallBinding, key: str, body: str) -> tuple[dict, str]:
    metadata = {
        "call_id": binding.call_id,
        "source_key": key,
        "body_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
    }
    marker = _SOURCE_PREFIX + json.dumps(
        metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return metadata, marker


def _contains_pair(history: list[dict], expected: dict, body: str) -> bool:
    found = False
    for index, message in enumerate(history):
        content = message.get("content")
        # A user quoting a key or even this exact marker is not import evidence.
        if (
            message.get("role") != "system"
            or not isinstance(content, str)
            or not content.startswith(_SOURCE_PREFIX)
        ):
            continue
        try:
            metadata = json.loads(
                content[len(_SOURCE_PREFIX) :], parse_constant=_reject_json_constant
            )
        except (ValueError, RecursionError):
            raise ContextRejected("Malformed review source marker") from None
        if (
            not isinstance(metadata, dict)
            or set(metadata) != set(expected)
            or any(not isinstance(value, str) for value in metadata.values())
        ):
            raise ContextRejected("Malformed review source metadata")
        if (
            metadata["call_id"] != expected["call_id"]
            and metadata["source_key"] != expected["source_key"]
        ):
            continue
        if found or metadata != expected or index + 1 >= len(history):
            raise ContextRejected("Review source conflicts or is incomplete")
        assistant = history[index + 1]
        if assistant.get("role") != "assistant" or assistant.get("content") != body:
            raise ContextRejected("Review source does not match the original body")
        found = True
    return found


class AstrBotContextAdapter:
    """Use only for freshly claimed pending publication steps, on the core loop.

    A trusted optional reader returns the actual model-input list[dict] for the
    fixed binding. It must not echo the expected body or invoke a model. Missing
    or unverified input returns UNCERTAIN. The local attempt fence is transient;
    durable publication running/uncertain states must prevent cross-restart
    retries, especially after trimming/reset removes a source marker.
    """

    def __init__(
        self,
        context,
        *,
        business_loop=None,
        session_lock_manager=None,
        model_input_reader: Callable[[CallBinding], Awaitable[list[dict]]] | None = None,
    ):
        current = asyncio.get_running_loop()
        if business_loop is not None and current is not business_loop:
            raise LoopMismatch("Construct the context adapter on its business loop")
        self._loop = current
        if session_lock_manager is None:
            from .astrbot_compat import session_locks

            session_lock_manager = session_locks()
        if not callable(getattr(session_lock_manager, "acquire_lock", None)):
            raise TypeError("A trusted shared session lock manager is required")
        if model_input_reader is not None and not callable(model_input_reader):
            raise TypeError("A trusted model-input reader must be callable")
        self._context = context
        self._locks = session_lock_manager
        self._model_input_reader = model_input_reader
        self._attempted: set[tuple[str, str, str]] = set()

    def _check_loop(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise LoopMismatch("Context import rejected on a different loop")

    @staticmethod
    def _check_input(binding: CallBinding, key: str, body: str) -> None:
        if not isinstance(binding, CallBinding):
            raise ContextRejected("Expected a trusted fixed CallBinding")
        values = (binding.call_id, binding.conversation_id, binding.platform_id, binding.session_id)
        if any(not isinstance(value, str) or not value or value.isspace() for value in values) or (
            binding.message_type != "FriendMessage"
            or binding.unified_msg_origin
            != f"{binding.platform_id}:{binding.message_type}:{binding.session_id}"
        ):
            raise ContextRejected("Malformed fixed conversation binding")
        if not isinstance(key, str) or key != f"review:{binding.call_id}:context":
            raise ContextRejected("Source key does not match the publication binding")
        if not isinstance(body, str) or not body or body.isspace():
            raise ContextRejected("Expected the original successful review text")

    async def _history(self, binding: CallBinding) -> list[dict]:
        conversation = await self._context.conversation_manager.get_conversation(
            binding.unified_msg_origin, binding.conversation_id, create_if_not_exists=False
        )
        try:
            validate_conversation(
                conversation,
                umo=binding.unified_msg_origin,
                cid=binding.conversation_id,
                platform_id=binding.platform_id,
            )
            raw = getattr(conversation, "history", None)
            if not isinstance(raw, str):
                raise ContextRejected("Conversation history must be a JSON string")
            history = json.loads(raw, parse_constant=_reject_json_constant)
            _check_messages(history, "Invalid conversation message structure")
        except (BindingError, SnapshotError, ValueError, RecursionError):
            raise ContextRejected("Fixed conversation or history was rejected") from None
        return history

    async def import_review(
        self, binding: CallBinding, source_key: str, body: str
    ) -> ImportReceipt:
        """Append a standard system/assistant pair and invalidate tokens once.

        Core-storage readback remains under the same original UMO lock. The
        adapter never calls SQLite/Store, selects the current cid or creates a
        conversation. An absent marker after a local attempt cannot cause a
        second write. This is not a permanent receipt across adapter lifetimes.
        """
        self._check_loop()  # Before context, locks or material access.
        self._check_input(binding, source_key, body)
        write_started = False
        try:
            expected, marker = _source(binding, source_key, body)
            attempt = (binding.unified_msg_origin, binding.conversation_id, source_key)
            async with self._locks.acquire_lock(binding.unified_msg_origin):
                history = await self._history(binding)
                if _contains_pair(history, expected, body):
                    self._attempted.add(attempt)
                    return ImportReceipt(binding.call_id, source_key, expected["body_sha256"], True)
                if attempt in self._attempted:
                    raise ContextUncertain("Review source disappeared after an import attempt")
                self._attempted.add(attempt)
                write_started = True
                await self._context.conversation_manager.update_conversation(
                    binding.unified_msg_origin,
                    binding.conversation_id,
                    history=history
                    + [
                        {"role": "system", "content": marker},
                        {"role": "assistant", "content": body},
                    ],
                    token_usage=0,
                )
                confirmed = await self._history(binding)
                if not _contains_pair(confirmed, expected, body):
                    raise ContextUncertain("Context import readback is unconfirmed")
                return ImportReceipt(binding.call_id, source_key, expected["body_sha256"], False)
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except ContextRejected as error:
            if write_started:
                raise ContextUncertain("Context import readback is unconfirmed") from None
            raise error from None
        except ContextUncertain as error:
            raise error from None
        except Exception:
            raise ContextUncertain("Context import is unconfirmed") from None

    async def make_context_readable(self, binding: CallBinding, source_key: str, body: str):
        """Publication callback; only a trusted model-input match can confirm.

        The model-input reader runs outside the short import lock and receives
        only the fixed binding, not the expected body. No reader means uncertain,
        even when core-storage readback succeeded. No production reader is wired.
        """
        self._check_loop()
        from .publication import Outcome

        try:
            await self.import_review(binding, source_key, body)
            if self._model_input_reader is None:
                return Outcome.UNCERTAIN
            messages = await self._model_input_reader(binding)
            _check_messages(messages, "Invalid model-input message structure")
            expected, _ = _source(binding, source_key, body)
            return (
                Outcome.CONFIRMED if _contains_pair(messages, expected, body) else Outcome.UNCERTAIN
            )
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            return Outcome.UNCERTAIN
