"""Read-only adapter behavior with synthetic Context and shared-lock stand-ins."""

import asyncio
from contextlib import asynccontextmanager, redirect_stderr, redirect_stdout
from copy import deepcopy
import io
import json
import logging
import subprocess
import sys
from types import SimpleNamespace
import unittest

from bot_call.astrbot_compat import provider_settings
from bot_call.astrbot_adapter import (
    AstrBotReadAdapter,
    LoopMismatch,
    ProviderError,
    SnapshotError,
)
from bot_call.binding import BindingError


class SharedLocks:
    """Synthetic core-lock stand-in; every accessor checks lock participation."""

    def __init__(self):
        self.locks = {}
        self.held = set()
        self.attempts = []
        self.attempted = asyncio.Event()

    @asynccontextmanager
    async def acquire_lock(self, umo):
        self.attempts.append(umo)
        self.attempted.set()
        lock = self.locks.setdefault(umo, asyncio.Lock())
        async with lock:
            self.held.add(umo)
            try:
                yield
            finally:
                self.held.remove(umo)


class Event:
    def __init__(self, platform="qq-1"):
        self.platform = platform
        self.unified_msg_origin = f"{platform}:FriendMessage:user"

    def get_sender_id(self):
        return "user"

    def get_self_id(self):
        return "bot"

    def get_platform_id(self):
        return self.platform

    def get_platform_name(self):
        return "aiocqhttp"

    def get_message_type(self):
        return SimpleNamespace(value="FriendMessage")

    def get_session_id(self):
        return "user"


class Conversations:
    def __init__(self, locks):
        self.locks = locks
        self.current = {}
        self.items = {}
        self.calls = []

    async def get_curr_conversation_id(self, umo):
        assert umo in self.locks.held
        self.calls.append(("current", umo))
        return self.current.get(umo)

    async def get_conversation(self, umo, cid):
        assert umo in self.locks.held
        self.calls.append(("conversation", umo, cid))
        return self.items.get(cid)

    async def update_conversation(self, *args, **kwargs):
        raise AssertionError("History writes are forbidden")

    async def add_message_pair(self, *args, **kwargs):
        raise AssertionError("History append is forbidden")

    async def new_conversation(self, *args, **kwargs):
        raise AssertionError("Conversation creation is forbidden")

    async def switch_conversation(self, *args, **kwargs):
        raise AssertionError("Conversation switching is forbidden")


class Provider:
    def __init__(self, identifier):
        self.provider_config = {
            "id": identifier,
            "max_context_tokens": 0,
            "key": ["PRIVATE_PROVIDER_KEY"],
            "nested": {"values": ["shared provider setting"]},
        }

    async def text_chat(self, *args, **kwargs):
        raise AssertionError("Real/fake provider calls are forbidden in preparation")

    async def get_text(self, *args, **kwargs):
        raise AssertionError("STT calls are forbidden")

    def get_model(self):
        raise AssertionError("No model-window inference side effects are needed")


class Personas:
    def __init__(self, locks):
        self.locks = locks
        self.results = {}
        self.calls = []
        self.error = None

    async def resolve_selected_persona(
        self, *, umo, conversation_persona_id, platform_name, provider_settings
    ):
        assert umo in self.locks.held
        self.calls.append(
            {
                "umo": umo,
                "conversation_persona_id": conversation_persona_id,
                "platform_name": platform_name,
                "provider_settings": provider_settings,
            }
        )
        if self.error is not None:
            raise self.error
        return self.results[umo]


class Context:
    def __init__(self, locks):
        self.locks = locks
        self.conversation_manager = Conversations(locks)
        self.persona_manager = Personas(locks)
        self.configs = {}
        self.providers = {}
        self.calls = []
        self.config_error = None
        self.provider_error = None

    def add(self, event, cid="A", persona_id=None):
        umo = event.unified_msg_origin
        self.conversation_manager.current[umo] = cid
        self.conversation_manager.items[cid] = SimpleNamespace(
            cid=cid,
            user_id=umo,
            platform_id=event.platform,
            persona_id=persona_id,
            history=json.dumps([{"role": "user", "content": "initial history"}]),
        )
        self.configs[umo] = {
            "provider_settings": {
                "max_context_length": -1,
                "dequeue_context_length": 1,
                "context_limit_reached_strategy": "llm_compress",
                "llm_compress_instruction": "PRIVATE_COMPRESS_PROMPT",
                "llm_compress_keep_recent_ratio": 0.15,
                "llm_compress_provider_id": "compress-provider",
                "fallback_max_context_tokens": 128000,
                "default_personality": "default-" + cid,
                "nested": {"values": [cid]},
            }
        }
        from native_config import migrate

        migrate(self.configs[umo])
        self.providers[umo] = Provider("provider-" + cid)
        self.persona_manager.results[umo] = (
            "selected-" + cid,
            {
                "name": "selected-" + cid,
                "prompt": "PRIVATE_PERSONA_PROMPT",
                "_begin_dialogs_processed": [
                    {
                        "role": "assistant",
                        "content": [
                            {"type": "text", "text": "PRIVATE_BEGIN_DIALOG"},
                        ],
                    }
                ],
                "nested": {"values": [cid]},
            },
            None,
            False,
        )

    def get_config(self, *, umo):
        assert umo in self.locks.held
        self.calls.append(("config", umo))
        if self.config_error:
            raise self.config_error
        return self.configs[umo]

    def get_using_provider(self, *, umo):
        assert umo in self.locks.held
        self.calls.append(("provider", umo))
        if self.provider_error:
            raise self.provider_error
        return self.providers.get(umo)

    async def send_message(self, *args, **kwargs):
        raise AssertionError("Sending is forbidden")


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.locks = SharedLocks()
        self.context = Context(self.locks)
        self.event = Event()
        self.umo = self.event.unified_msg_origin
        self.context.add(self.event)
        self.adapter = AstrBotReadAdapter(
            self.context,
            business_loop=asyncio.get_running_loop(),
            session_lock_manager=self.locks,
        )
        self.binding = await self.adapter.bind(self.event)

    async def test_fixed_target_reads_latest_material_and_rejects_missing_or_wrong_owner(self):
        reader = self.context.conversation_manager
        reader.current[self.umo] = "B"
        reader.items["B"] = SimpleNamespace(history="B stays untouched")
        original = reader.items["A"]
        original.history = json.dumps([{"role": "user", "content": "new QQ text in A"}])
        original.persona_id = "new-persona"
        self.context.persona_manager.results[self.umo] = (
            "forced-A",
            {"prompt": "latest prompt"},
            "forced-A",
            False,
        )
        snapshot = await self.adapter.refresh(self.binding)
        self.assertEqual(snapshot.history[0]["content"], "new QQ text in A")
        self.assertEqual(snapshot.conversation_persona_id, "new-persona")
        self.assertEqual(
            (snapshot.selected_persona_id, snapshot.prompt), ("forced-A", "latest prompt")
        )
        self.assertIsNone(snapshot.binding.persona_id)
        self.assertEqual(sum(call[0] == "current" for call in reader.calls), 1)
        reader.items.pop("A")
        with self.assertRaises(BindingError):
            await self.adapter.refresh(self.binding)
        reader.items["A"] = original
        for name, value in [("user_id", "other"), ("platform_id", "qq-2"), ("cid", "wrong")]:
            saved = getattr(original, name)
            setattr(original, name, value)
            with self.assertRaises(BindingError):
                await self.adapter.refresh(self.binding)
            setattr(original, name, saved)
        self.assertEqual(reader.current[self.umo], "B")
        self.assertEqual(reader.items["B"].history, "B stays untouched")

    async def test_per_umo_routing_and_four_persona_results_are_delegated(self):
        other = Event("qq-2")
        self.context.add(other, cid="C", persona_id="conversation-C")
        other_binding = await self.adapter.bind(other)
        first = await self.adapter.refresh(self.binding)
        second = await self.adapter.refresh(other_binding)
        self.assertEqual((first.provider_id, second.provider_id), ("provider-A", "provider-C"))
        self.assertEqual(second.provider_settings["nested"]["values"], ["C"])
        self.assertEqual(
            self.context.persona_manager.calls[-1],
            {
                "umo": other.unified_msg_origin,
                "conversation_persona_id": "conversation-C",
                "platform_name": "aiocqhttp",
                "provider_settings": provider_settings(
                    self.context.configs[other.unified_msg_origin]
                ),
            },
        )
        for current_id, selected, forced, persona in [
            (None, "core-default", None, {"prompt": "default"}),
            ("", "", None, None),
            ("[%None]", "[%None]", None, None),
            ("old", "core-rule", "core-rule", {"prompt": "forced"}),
        ]:
            self.context.conversation_manager.items["A"].persona_id = current_id
            self.context.persona_manager.results[self.umo] = (selected, persona, forced, False)
            snapshot = await self.adapter.refresh(self.binding)
            self.assertEqual(snapshot.selected_persona_id, selected)
            self.assertEqual(snapshot.forced_persona_id, forced)
            self.assertFalse(snapshot.use_webchat_special_default)
            self.assertEqual(
                self.context.persona_manager.calls[-1]["conversation_persona_id"], current_id
            )
        self.assertTrue(
            all(call[1] in {self.umo, other.unified_msg_origin} for call in self.context.calls)
        )

    async def test_full_detached_snapshot_preserves_tools_policy_and_private_repr(self):
        history = [
            {
                "role": "user" if i % 2 else "assistant",
                "content": f" message {i}\r\n" + "长" * 4500 + " \t",
            }
            for i in range(62)
        ]
        history.extend(
            [
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {"name": "lookup", "arguments": '{"x":1}'},
                        },
                    ],
                    "vendor": {"trace": ["untouched"]},
                },
                {"role": "tool", "tool_call_id": "call-1", "content": "tool result"},
                {
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": "synthetic-image"}},
                        {"type": "text", "text": "Unicode e\u0301🙂\r\n"},
                    ],
                },
                {"role": "checkpoint", "content": {"id": "checkpoint-1"}},
            ]
        )
        self.context.conversation_manager.items["A"].history = json.dumps(history)
        source_config = deepcopy(self.context.configs[self.umo])
        settings = provider_settings(source_config)
        persona = deepcopy(self.context.persona_manager.results[self.umo][1])
        provider_config = deepcopy(self.context.providers[self.umo].provider_config)
        messages = []

        class Capture(logging.Handler):
            def emit(self, record):
                messages.append(record.getMessage())

        root = logging.getLogger()
        old_level, handler = root.level, Capture()
        stdout, stderr = io.StringIO(), io.StringIO()
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)
        try:
            with redirect_stdout(stdout), redirect_stderr(stderr):
                snapshot = await self.adapter.refresh(self.binding)
                representation = repr(snapshot) + repr(self.adapter)
        finally:
            root.removeHandler(handler)
            root.setLevel(old_level)
        self.assertEqual(snapshot.history, history)
        self.assertEqual(snapshot.provider_settings, settings)
        self.assertEqual(snapshot.provider_max_context_tokens, 0)
        self.assertFalse(hasattr(snapshot, "provider"))
        snapshot.history[-4]["vendor"]["trace"].append("local edit")
        snapshot.provider_settings["nested"]["values"].append("local edit")
        snapshot.persona["nested"]["values"].append("local edit")
        snapshot.begin_dialogs[0]["content"][0]["text"] = "local edit"
        self.context.persona_manager.calls[0]["provider_settings"]["nested"]["values"].append(
            "resolver edit"
        )
        fresh = await self.adapter.refresh(self.binding)
        self.assertEqual(fresh.history, history)
        self.assertEqual(fresh.provider_settings, settings)
        self.assertEqual(fresh.persona, persona)
        self.assertEqual(self.context.configs[self.umo], source_config)
        self.assertEqual(self.context.persona_manager.results[self.umo][1], persona)
        self.assertEqual(self.context.providers[self.umo].provider_config, provider_config)
        output = stdout.getvalue() + stderr.getvalue() + "".join(messages) + representation
        for secret in [
            "PRIVATE_PERSONA_PROMPT",
            "PRIVATE_BEGIN_DIALOG",
            "PRIVATE_PROVIDER_KEY",
            "PRIVATE_COMPRESS_PROMPT",
            history[0]["content"],
        ]:
            self.assertNotIn(secret, output)

    async def test_corrupt_material_and_unavailable_provider_fail_without_fallback(self):
        conversation = self.context.conversation_manager.items["A"]
        history = conversation.history
        for raw in ["bad PRIVATE_HISTORY", "{}", '[{"role":"user","content":[42]}]']:
            conversation.history = raw
            with self.assertRaises(SnapshotError) as error:
                await self.adapter.refresh(self.binding)
            self.assertNotIn("PRIVATE_HISTORY", str(error.exception))
        conversation.history = history
        provider = self.context.providers[self.umo]
        for value in [None, object(), SimpleNamespace(text_chat=lambda: None, provider_config=[])]:
            self.context.providers[self.umo] = value
            with self.assertRaises(ProviderError):
                await self.adapter.refresh(self.binding)
        self.context.providers[self.umo] = provider
        self.context.provider_error = ValueError("PRIVATE_PROVIDER_KEY")
        with self.assertRaises(ProviderError) as error:
            await self.adapter.refresh(self.binding)
        self.assertNotIn("PRIVATE_PROVIDER_KEY", str(error.exception))
        self.context.provider_error = None
        config = self.context.configs[self.umo]
        self.context.configs[self.umo] = {"provider_settings": []}
        with self.assertRaises(SnapshotError):
            await self.adapter.refresh(self.binding)
        self.context.configs[self.umo] = config
        self.context.persona_manager.results[self.umo] = (None, None)
        with self.assertRaises(SnapshotError):
            await self.adapter.refresh(self.binding)
        self.assertTrue(all(call[1] == self.umo for call in self.context.calls))

    async def test_shared_lock_serializes_binding_and_refresh_while_other_origin_proceeds(self):
        other = Event("qq-2")
        self.context.add(other, cid="C")
        other_binding = await self.adapter.bind(other)
        reader = self.context.conversation_manager
        reader.items["A-other"] = SimpleNamespace(
            cid="A-other",
            user_id=self.umo,
            platform_id="qq-1",
            persona_id="new",
        )
        before = len(reader.calls)
        async with self.locks.acquire_lock(self.umo):
            self.locks.attempted.clear()
            pending_bind = asyncio.create_task(self.adapter.bind(self.event))
            pending_read = asyncio.create_task(self.adapter.refresh(self.binding))
            await asyncio.wait_for(self.locks.attempted.wait(), 1)
            self.assertFalse(pending_bind.done())
            self.assertFalse(pending_read.done())
            other_snapshot = await asyncio.wait_for(self.adapter.refresh(other_binding), 1)
            self.assertEqual(other_snapshot.provider_id, "provider-C")
            self.assertEqual([call for call in reader.calls[before:] if call[1] == self.umo], [])
            reader.current[self.umo] = "A-other"
            reader.items["A"].history = json.dumps(
                [
                    {"role": "user", "content": "QQ committed inside shared lock"},
                ]
            )
        new_binding, snapshot = await asyncio.wait_for(
            asyncio.gather(pending_bind, pending_read), 1
        )
        self.assertEqual(new_binding.conversation_id, "A-other")
        self.assertEqual(snapshot.binding.conversation_id, "A")
        self.assertEqual(snapshot.history[0]["content"], "QQ committed inside shared lock")

    async def test_wrong_loop_is_rejected_before_any_context_event_or_lock_read(self):
        before = (
            list(self.context.calls),
            list(self.context.conversation_manager.calls),
            list(self.locks.attempts),
        )

        def other_thread():
            async def foreign():
                with self.assertRaises(LoopMismatch):
                    await self.adapter.refresh(self.binding)
                with self.assertRaises(LoopMismatch):
                    await self.adapter.bind(object())

            asyncio.run(foreign())

        await asyncio.to_thread(other_thread)
        self.assertEqual(
            before,
            (self.context.calls, self.context.conversation_manager.calls, self.locks.attempts),
        )


class ImportTests(unittest.TestCase):
    def test_import_loads_neither_astrbot_nor_storage(self):
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                "import sys; import bot_call.astrbot_adapter; "
                "assert not any(n=='astrbot' or n.startswith('astrbot.') for n in sys.modules); "
                "assert 'bot_call.store' not in sys.modules; print('isolated import')",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "isolated import")


if __name__ == "__main__":
    unittest.main()
