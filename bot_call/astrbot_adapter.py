"""Read-only Pinned AstrBot preparation; importing this module loads no AstrBot."""

import asyncio
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
import json

from .astrbot_compat import provider_settings, using_provider
from .binding import BindingError, CallBinding, EventReader, bind_call, validate_conversation


class LoopMismatch(RuntimeError):
    """The adapter was used outside the captured AstrBot business loop."""


class SnapshotError(ValueError):
    """Conversation, configuration or persona material is malformed."""


class ProviderError(SnapshotError):
    """The original UMO has no usable chat provider selection."""


@dataclass(frozen=True, slots=True)
class CallSnapshot:
    """Detached input material, never a live Provider or a complete Agent request."""

    binding: CallBinding = field(repr=False)
    history: list[dict] = field(repr=False)
    conversation_persona_id: str | None = field(repr=False)
    selected_persona_id: str | None = field(repr=False)
    persona: dict | None = field(repr=False)
    forced_persona_id: str | None = field(repr=False)
    use_webchat_special_default: bool = field(repr=False)
    provider_settings: dict = field(repr=False)
    provider_id: str = field(repr=False)
    provider_max_context_tokens: int = field(repr=False)

    @property
    def prompt(self) -> str | None:
        return self.persona["prompt"] if self.persona is not None else None

    @property
    def begin_dialogs(self) -> list[dict] | None:
        if self.persona is None:
            return None
        return self.persona.get("_begin_dialogs_processed")


def _copy_mapping(value: object, description: str) -> dict:
    if not isinstance(value, Mapping):
        raise SnapshotError(description)
    try:
        # Strip the outer AstrBotConfig/dict subclass; never clone its file handles.
        return deepcopy(dict(value))
    except Exception:
        raise SnapshotError(description) from None


def _check_messages(messages: object, description: str) -> None:
    """Check message envelopes without projecting away tool/vendor content."""
    if not isinstance(messages, list):
        raise SnapshotError(description)
    for message in messages:
        if not isinstance(message, dict):
            raise SnapshotError(description)
        role = message.get("role")
        if not isinstance(role, str) or not role or role.isspace():
            raise SnapshotError(description)
        if "content" not in message and "tool_calls" not in message:
            raise SnapshotError(description)
        if "content" in message and not isinstance(
            message["content"], (str, list, dict, type(None))
        ):
            raise SnapshotError(description)
        content = message.get("content")
        if isinstance(content, list) and any(
            not isinstance(part, dict) or not isinstance(part.get("type"), str) or not part["type"]
            for part in content
        ):
            raise SnapshotError(description)
        calls = message.get("tool_calls")
        if calls is not None and (
            not isinstance(calls, list) or any(not isinstance(call, dict) for call in calls)
        ):
            raise SnapshotError(description)


def _reject_json_constant(value: str):
    raise ValueError("Non-JSON numeric constant")


class AstrBotReadAdapter:
    """Construct in plugin initialization on its actual business event loop.

    An injected manager must be the trusted core session_lock_manager singleton;
    injection exists for integration and isolated tests, not for room-local locks.
    A snapshot intentionally carries only Provider identity/window metadata.
    Returning the live Provider would expose mutable shared configuration.
    """

    def __init__(self, context, *, business_loop=None, session_lock_manager=None):
        current = asyncio.get_running_loop()
        if business_loop is not None and current is not business_loop:
            raise LoopMismatch("Construct the adapter on its business loop")
        self._loop = current
        if session_lock_manager is None:
            # Delayed until actual construction inside the initialized plugin.
            from .astrbot_compat import session_locks

            session_lock_manager = session_locks()
        if not callable(getattr(session_lock_manager, "acquire_lock", None)):
            raise TypeError("A shared session lock manager is required")
        self._locks = session_lock_manager
        self._context = context

    def _check_loop(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise LoopMismatch("Read rejected before accessing context on a different loop")

    async def bind(self, event: EventReader, *, expected=None) -> CallBinding:
        """Read the current cid and verify ownership under the same original UMO lock."""
        self._check_loop()
        umo = event.unified_msg_origin
        if not isinstance(umo, str) or not umo or umo.isspace():
            raise BindingError("Missing event origin")
        async with self._locks.acquire_lock(umo):
            binding = await bind_call(event, self._context.conversation_manager, expected=expected)
            if binding.unified_msg_origin != umo:
                raise BindingError("Event origin changed while binding")
            return binding

    async def permission_metadata(self, binding: CallBinding):
        """Fixed-cid persona/tools metadata only; never decode/copy history."""
        self._check_loop()
        umo = binding.unified_msg_origin
        async with self._locks.acquire_lock(umo):
            conversation = await self._context.conversation_manager.get_conversation(
                umo, binding.conversation_id
            )
            persona_id = validate_conversation(
                conversation, umo=umo, cid=binding.conversation_id, platform_id=binding.platform_id
            )
            config = self._context.get_config(umo=umo)
            settings = provider_settings(config)
            if not isinstance(settings, Mapping):
                raise SnapshotError("Invalid persona metadata")
            (
                selected,
                persona,
                forced,
                web,
            ) = await self._context.persona_manager.resolve_selected_persona(
                umo=umo,
                conversation_persona_id=persona_id,
                platform_name=binding.platform_type,
                provider_settings={"default_personality": settings.get("default_personality")},
            )
            tools = None if persona is None else deepcopy(persona.get("tools"))
            return persona_id, selected, forced, web, tools

    async def refresh(self, binding: CallBinding) -> CallSnapshot:
        """Read the latest fixed cid, detached settings and delegated persona.

        No current-pointer read, SQLite operation, model call, compression,
        conversation creation/switch or history write occurs here.
        """
        self._check_loop()
        if not isinstance(binding, CallBinding):
            raise TypeError("Expected a server-created CallBinding")
        if (
            not binding.conversation_id
            or binding.conversation_id.isspace()
            or not binding.platform_id
            or not binding.session_id
            or binding.message_type != "FriendMessage"
            or binding.unified_msg_origin
            != f"{binding.platform_id}:{binding.message_type}:{binding.session_id}"
        ):
            raise BindingError("Malformed fixed conversation binding")
        umo = binding.unified_msg_origin
        async with self._locks.acquire_lock(umo):
            conversation = await self._context.conversation_manager.get_conversation(
                umo, binding.conversation_id
            )
            conversation_persona = validate_conversation(
                conversation,
                umo=umo,
                cid=binding.conversation_id,
                platform_id=binding.platform_id,
            )
            raw_history = getattr(conversation, "history", None)
            if not isinstance(raw_history, str):
                raise SnapshotError("Conversation history must be a JSON string")
            try:
                history = json.loads(raw_history, parse_constant=_reject_json_constant)
            except (ValueError, RecursionError):
                raise SnapshotError("Invalid conversation history JSON") from None
            _check_messages(history, "Invalid conversation message structure")

            try:
                config = self._context.get_config(umo=umo)
            except Exception:
                raise SnapshotError("Cannot read configuration for the original UMO") from None
            if not isinstance(config, Mapping):
                raise SnapshotError("Invalid per-UMO configuration")
            try:
                settings = _copy_mapping(
                    provider_settings(config), "Invalid per-UMO provider_settings"
                )
            except Exception:
                raise SnapshotError("Invalid per-UMO provider_settings") from None
            # These two fields are mandatory in the core local pipeline config.
            for name in ("max_context_length", "dequeue_context_length"):
                if type(settings.get(name)) is not int:
                    raise SnapshotError("Missing or invalid core context length setting")

            try:
                provider = await using_provider(self._context, umo)
            except Exception:
                raise ProviderError("Cannot resolve the original UMO chat provider") from None
            # Real Context.get_using_provider already enforces isinstance(Provider).
            # Check only the exact read/call surface needed here for trusted fakes.
            if provider is None or not callable(getattr(provider, "text_chat", None)):
                raise ProviderError("No chat provider for the original UMO")
            provider_config = getattr(provider, "provider_config", None)
            if not isinstance(provider_config, Mapping):
                raise ProviderError("Invalid selected provider configuration")
            provider_id = provider_config.get("id")
            window = provider_config.get("max_context_tokens", 0)
            if not isinstance(provider_id, str) or not provider_id or provider_id.isspace():
                raise ProviderError("Selected provider has no stable identifier")
            if type(window) is not int:
                raise ProviderError("Invalid selected provider context window")

            try:
                selection = await self._context.persona_manager.resolve_selected_persona(
                    umo=umo,
                    conversation_persona_id=conversation_persona,
                    platform_name=binding.platform_type,
                    provider_settings=deepcopy(settings),
                )
            except Exception:
                raise SnapshotError("Cannot resolve the original conversation persona") from None
            if not isinstance(selection, tuple) or len(selection) != 4:
                raise SnapshotError("Invalid core persona resolution result")
            selected_id, persona, forced_id, webchat_default = selection
            if (
                (selected_id is not None and not isinstance(selected_id, str))
                or (forced_id is not None and not isinstance(forced_id, str))
                or type(webchat_default) is not bool
            ):
                raise SnapshotError("Invalid core persona resolution metadata")
            if persona is not None:
                persona = _copy_mapping(persona, "Invalid resolved persona")
                if not isinstance(persona.get("prompt"), str):
                    raise SnapshotError("Invalid resolved persona prompt")
                dialogs = persona.get("_begin_dialogs_processed")
                if dialogs is not None:
                    _check_messages(dialogs, "Invalid processed persona begin dialogs")
            return CallSnapshot(
                binding,
                history,
                conversation_persona,
                selected_id,
                persona,
                forced_id,
                webchat_default,
                settings,
                provider_id,
                window,
            )
