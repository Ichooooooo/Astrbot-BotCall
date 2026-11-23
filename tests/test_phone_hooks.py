"""Real registry and Runner; synthetic plugins and services only."""

import asyncio
from types import SimpleNamespace as NS
from dataclasses import replace
from native_astrbot import register_hook
from test_backend import NativeCase, response, TEXT
from bot_call.astrbot_compat import PhoneHooks
from bot_call.observability import CURRENT_TIMING, RoundTiming, timing_summary, timing_row
from bot_call.store import Store


class PhoneHookTests(NativeCase):
    async def test_dispatch_defaults_filter_reserved_priority_stop_errors_and_cancel(self):
        from astrbot.core.pipeline.context_utils import call_event_hook
        from astrbot.core.astr_agent_hooks import MAIN_AGENT_HOOKS

        kind = self.agent_api.event_types.OnLLMRequestEvent
        seen = []

        async def alpha(event, *args):
            seen.append("alpha")

        async def beta(event, *args):
            seen.append("beta")

        async def reserved(event, *args):
            seen.append("reserved")

        register_hook(self, "alpha", kind, alpha, priority=10)
        register_hook(self, "beta", kind, beta)
        register_hook(self, "core", kind, reserved, reserved=True, priority=5)
        event = NS(
            trace=NS(record=lambda *a, **k: None),
            plugins_name=["alpha", "beta"],
            is_stopped=lambda: False,
        )
        await call_event_hook(event, kind)
        baseline = seen[:]
        seen.clear()
        await PhoneHooks().call(event, kind)
        self.assertEqual(seen, baseline)
        seen.clear()
        timing = RoundTiming()
        token = CURRENT_TIMING.set(timing)
        try:
            selected = ["alpha", "core"]
            phone = PhoneHooks(selected)
            selected.append("beta")
            await asyncio.gather(phone.call(event, kind), call_event_hook(event, kind))
        finally:
            CURRENT_TIMING.reset(token)
        self.assertEqual(seen.count("alpha"), 1)
        self.assertEqual(seen.count("beta"), 2)
        self.assertEqual(seen.count("reserved"), 2)
        self.assertEqual(event.plugins_name, ["alpha", "beta"])
        self.assertIsNot(phone.agent_hooks(), MAIN_AGENT_HOOKS)
        rows = [
            dict(timing_row(r), call_id="c", generation_id="g", recorded_at=1) for r in timing.rows
        ]
        self.assertEqual({r["provider_id"] for r in rows}, {"alpha", "beta", "core"})
        self.assertTrue(all(r["model"] == kind.name for r in rows))
        summary = timing_summary(rows)["hooks"]
        self.assertEqual(next(r for r in summary if r["plugin"] == "alpha")["skipped"], 1)
        self.assertTrue(all(r["count"] == 1 for r in summary))

        async def cancelled(event, *args):
            raise asyncio.CancelledError()

        event.plugins_name += ["cancel", "stop"]
        row = register_hook(self, "cancel", kind, cancelled, priority=20)
        with self.assertRaises(asyncio.CancelledError):
            await phone.call(event, kind)
        # Removing a plugin and creating a new call cannot retain old handlers/skip lists.
        from astrbot.core.star.star_handler import star_handlers_registry

        star_handlers_registry.remove(row)
        seen.clear()
        await PhoneHooks().call(event, kind)
        self.assertEqual(seen, baseline)
        stopped = False

        async def stop(event, *args):
            nonlocal stopped
            stopped = True

        register_hook(self, "stop", kind, stop, priority=30)
        event.is_stopped = lambda: stopped
        seen.clear()
        self.assertTrue(await phone.call(event, kind))
        self.assertEqual(seen, [])

    async def test_owned_runner_skips_hooks_keeps_plugin_tool_and_native_side_effects(self):
        event, provider = self.environment()
        events = self.agent_api.event_types
        seen = []
        for kind in (
            events.OnWaitingLLMRequestEvent,
            events.OnLLMRequestEvent,
            events.OnAgentBeginEvent,
            events.OnLLMResponseEvent,
            events.OnAgentDoneEvent,
            events.OnUsingLLMToolEvent,
            events.OnLLMToolRespondEvent,
        ):
            for plugin in ("alpha", "beta"):

                async def record(event, *args, plugin=plugin, kind=kind):
                    seen.append((plugin, kind))
                    if kind == events.OnAgentDoneEvent:
                        self.assertEqual(
                            event.get_extra("_llm_reasoning_content"), "synthetic private reasoning"
                        )
                    if kind == events.OnLLMToolRespondEvent:
                        self.assertIsNone(event.get_result())

                register_hook(self, plugin, kind, record)

        async def tool(event):
            return "synthetic tool response"

        tool.__module__ = "synthetic_plugin.alpha"
        native_tool = self.agent_env["FunctionTool"](
            name="alpha_tool",
            description="synthetic",
            parameters={"type": "object", "properties": {}},
            handler=tool,
        )
        tools = self.agent_env["ToolSet"]([native_tool])
        self.context.get_llm_tool_manager = lambda: NS(
            get_full_tool_set=lambda: tools, get_builtin_tool=lambda *a: None
        )
        self.context.configs[event.unified_msg_origin]["plugin_set"] = ["alpha", "beta"]
        old = self.settings()
        skipped = ["alpha"]
        self.settings = lambda: dict(old, call_skip_plugin_hooks=skipped)

        async def answer(payload):
            if len(provider.calls) == 1:
                self.assertIn("alpha_tool", [t.name for t in payload["func_tool"].tools])
                return response("", role="tool", names=["alpha_tool"], args=[{}], ids=["id"])
            result = response("synthetic final")
            result.reasoning_content = "synthetic private reasoning"
            return result

        provider.responder = answer
        backend, host = await self.backend()
        handle, web = await self.enter(backend, event)
        skipped.append("beta")
        await backend.web_submit(web.token, web.reference, "u", TEXT)
        await backend.drain()
        self.assertEqual((await backend.read_state(handle)).generations[-1].state, "succeeded")
        self.assertEqual({p for p, k in seen}, {"beta"})
        self.assertEqual(
            {k for p, k in seen},
            {
                events.OnWaitingLLMRequestEvent,
                events.OnLLMRequestEvent,
                events.OnAgentBeginEvent,
                events.OnLLMResponseEvent,
                events.OnAgentDoneEvent,
                events.OnUsingLLMToolEvent,
                events.OnLLMToolRespondEvent,
            },
        )
        with Store(self.path / "calls.db") as store:
            rows = [
                dict(r)
                for r in store._db.execute("SELECT * FROM turn_timings WHERE stage LIKE 'hook_%'")
            ]
            self.assertTrue(rows)
            self.assertEqual({r["provider_id"] for r in rows}, {"alpha", "beta"})
            self.assertTrue(all(r["tool"].startswith("synthetic_plugin.") for r in rows))
