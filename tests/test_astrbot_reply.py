"""Native main-Agent transport and byte-preserving read-only legacy contracts."""

import asyncio
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace as NS
import json
import logging
import os
import sys
import importlib.util
from unittest.mock import patch

from test_backend import NativeCase, response, TEXT, BOT, FINAL
from bot_call.astrbot_agent import SourceAgentPolicy, spoken
from bot_call.astrbot_reply import AstrBotReplyAdapter, ReplyNotApplied, ReplyModel, ReplyPolicy
from bot_call.call_policy import CallPolicy
from bot_call.store import Store, StateConflict, Turn
from bot_call.tool_query import canonical, policy_details
from bot_call.native_provider import NATIVE_CONTRACT
from test_astrbot_context import binding


class ReplyTests(NativeCase):
    async def test_runtime_latest_original_background_native_delegation_and_complete_material(self):
        event, provider = self.environment()
        other, other_provider = self.environment("qq-B", "B")
        env = self.agent_env
        Tool = env["FunctionTool"]
        ToolSet = env["ToolSet"]
        hook_requests = []
        from native_astrbot import register_hook

        for kind in (
            env["EventType"].OnLLMRequestEvent,
            env["EventType"].OnLLMResponseEvent,
            env["EventType"].OnAgentDoneEvent,
        ):

            async def check_hook(native_event, *args, kind=kind):
                req = native_event.get_extra("provider_request")
                self.assertIsNotNone(req)
                self.assertEqual(req.conversation.cid, "A")
                if kind == env["EventType"].OnLLMRequestEvent:
                    self.assertIs(req, args[0])
                hook_requests.append(kind)

            register_hook(self, "synthetic_observer", kind, check_hook)
        calls = []
        available = []
        schemas = []

        async def tool_a(event, word):
            calls.append(
                (
                    "A",
                    event.unified_msg_origin,
                    event.get_sender_id(),
                    event.message_obj.message_id,
                    word,
                )
            )
            return "A public report"

        async def tool_b(event, value):
            calls.append(("B", event.unified_msg_origin, event.role, value))
            return "B public report"

        a = Tool(
            name="arbitrary_a",
            description="A",
            parameters={"type": "object", "properties": {"word": {"type": "string"}}},
            handler=tool_a,
        )
        b = Tool(
            name="unrelated_b",
            description="B",
            parameters={"type": "object", "properties": {"value": {"type": "integer"}}},
            handler=tool_b,
        )
        manager = NS(
            get_full_tool_set=lambda: ToolSet(list(available)),
            get_func=lambda name: next((t for t in available if t.name == name), None),
            get_builtin_tool=lambda *a: None,
        )
        self.context.get_llm_tool_manager = lambda: manager
        backend, host = await self.backend()
        handle, web = await self.enter(backend, event)
        record = backend._record(handle)
        bound = record.binding
        initial = self.context.conversation_manager.items["A"].history
        self.context.conversation_manager.current[bound.unified_msg_origin] = "B"
        phases = {}

        async def answer(payload):
            tools = payload.get("func_tool")
            if tools and tools.tools:
                tool = tools.tools[0]
                schemas.append(deepcopy(tool.parameters))
                name = tool.name
                phases[name] = phases.get(name, 0) + 1
                if name == "arbitrary_a" and phases[name] <= 2:
                    return response(
                        "INTERMEDIATE_NOT_SPOKEN",
                        role="tool",
                        names=[name],
                        args=[{"word": "same"}],
                        ids=["opaque-" + str(phases[name])],
                    )
                if name == "unrelated_b" and phases[name] == 1:
                    return response(
                        "INTERMEDIATE_NOT_SPOKEN",
                        role="tool",
                        names=[name],
                        args=[{"value": 7}],
                        ids=["other"],
                    )
            return response("最终自然回复")

        provider.responder = answer
        available[:] = [a]
        await backend.web_submit(web.token, web.reference, "u1", TEXT)
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual(state.turns[-1].body, "最终自然回复")
        self.assertEqual(len(calls), 2)
        with Store(self.path / "calls.db") as metrics:
            rows = [dict(r) for r in metrics._db.execute("SELECT * FROM turn_timings")]
            self.assertEqual(len([r for r in rows if r["stage"] == "llm"]), 3)
            tools = [r for r in rows if r["stage"] == "tool"]
            self.assertEqual([r["tool"] for r in tools], ["arbitrary_a"] * 2)
            self.assertTrue(all(r["elapsed_ms"] >= 0 for r in tools))

        self.assertTrue(
            {
                env["EventType"].OnLLMRequestEvent,
                env["EventType"].OnLLMResponseEvent,
                env["EventType"].OnAgentDoneEvent,
            }
            <= set(hook_requests)
        )
        self.assertEqual(calls[0][3], calls[1][3])  # Same native run may repeat the same operation.
        self.assertTrue(
            all(x[1] == bound.unified_msg_origin and x[2] == bound.sender_id for x in calls)
        )
        first = provider.calls[0]["contexts"]
        text = json.dumps(first, ensure_ascii=False)
        self.assertIn("开头", text)
        self.assertIn("中段", text)
        self.assertIn("结尾", text)
        row = await backend._runtime.agent_journal("read", bound.call_id, "reply:u1")
        reports = json.loads(row["reports_json"])
        self.assertEqual(reports[0], {"kind": "agent_begin"})
        self.assertEqual(sum(x["kind"] == "tool_start" for x in reports), 2)
        self.assertEqual(sum(x["kind"] == "framework_report" for x in reports), 2)
        self.assertNotIn("same", row["reports_json"])  # Args are digests, not saved parameter text.
        before = len(provider.calls)
        await backend.web_submit(web.token, web.reference, "u1", TEXT)
        await backend.drain()
        self.assertEqual(len(provider.calls), before)
        self.assertEqual(len(calls), 2)
        available[:] = [b]
        b.parameters["properties"]["value"]["minimum"] = 0
        self.context.configs[bound.unified_msg_origin]["admins_id"] = [bound.sender_id]
        await backend.web_submit(web.token, web.reference, "u2", "另一个普通请求")
        await backend.drain()
        self.assertEqual((calls[-1][0], calls[-1][2]), ("B", "admin"))
        self.assertTrue(
            any(x.get("properties", {}).get("value", {}).get("minimum") == 0 for x in schemas)
        )
        self.context.configs[bound.unified_msg_origin]["admins_id"] = []
        available.clear()
        await backend.web_submit(web.token, web.reference, "u3", "没有任何工具仍可聊天")
        await backend.drain()
        self.assertEqual(len(calls), 3)
        self.assertEqual(self.context.conversation_manager.items["A"].history, initial)
        self.assertEqual(other_provider.calls, [])
        self.assertTrue(record.policy.is_agent)
        # Source metadata capabilities are copied, not guessed from defaults.
        seen = []
        original = self.agent_api.build

        async def build(**kw):
            seen.append(kw["event"].platform_meta)
            return await original(**kw)

        backend._record(handle).reply.api = replace(self.agent_api, build=build)
        await backend.web_submit(web.token, web.reference, "meta", "检查来源能力")
        await backend.drain()
        original_meta = self.context.platform_manager.platform_insts[0].meta()
        self.assertEqual(seen[0], original_meta)
        self.assertIsNot(seen[0], original_meta)
        self.assertFalse(seen[0].support_proactive_message)
        # Changed persona allowlist is taken by the next native builder.
        available[:] = [a]
        persona = self.context.persona_manager.results[bound.unified_msg_origin][1]
        persona["tools"] = []
        before = len(calls)
        await backend.web_submit(web.token, web.reference, "persona", "普通请求")
        await backend.drain()
        self.assertEqual(len(calls), before)
        persona["tools"] = None
        a.handler_module_path = "fixture.plugin_a"
        env["star_map"]["fixture.plugin_a"] = NS(name="plugin_a", reserved=False)
        cfg = self.context.configs[bound.unified_msg_origin]
        cfg["plugin_set"] = ["another_plugin"]
        await backend.web_submit(web.token, web.reference, "plugin-filter", "原生插件来源过滤")
        await backend.drain()
        self.assertEqual(len(calls), before)
        cfg["plugin_set"] = ["*"]
        cfg["platform_settings"].update(
            enable_id_white_list=True, id_whitelist=["another:FriendMessage:user"]
        )
        before = len(provider.calls)
        await backend.web_submit(web.token, web.reference, "revoked", "来源已撤销")
        await backend.drain()
        self.assertEqual(len(provider.calls), before)
        cfg["platform_settings"]["enable_id_white_list"] = False
        await backend.web_end(web.token, web.reference)
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertIn("系统主Agent框架工具报告", state.review.prompt)
        self.assertIn("不证明业务事务", state.review.prompt)
        self.assertIn("arbitrary_a", state.review.prompt)
        self.assertIn("unrelated_b", state.review.prompt)

    async def test_native_query_single_decision_owned_close_and_schema(self):
        # Retired single-tool policy is read-only; the current Agent is multi-step.
        import bot_call.tool_query as legacy

        self.assertFalse(hasattr(legacy, "NativeTaskQuery"))
        self.assertFalse(hasattr(legacy, "load_native"))
        with Store(self.path / "legacy-gate.db") as store:
            a = binding("legacy")
            store.create_call(a)
            with self.assertRaises(StateConflict):
                store.claim_query(a.call_id, "reply:u", "id")

    async def test_safe_preflight_unknown_response_and_real_cancel_ownership(self):
        event, provider = self.environment()
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        bound = backend._record(handle).binding
        entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.gates.append(release)

        async def stubborn(payload):
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
            return response("迟到回复")

        provider.responder = stubborn
        await backend.web_submit(web.token, web.reference, "held", TEXT)
        await asyncio.wait_for(entered.wait(), 3)
        await backend.web_end(web.token, web.reference)
        self.assertIsNone((await backend._runtime.read_state(bound.call_id)).review)
        self.assertFalse((await backend.shutdown(timeout=0.01)).stopped)
        release.set()
        self.assertTrue((await backend.shutdown(timeout=2)).stopped)
        with Store(self.path / "calls.db") as store:
            row = store.agent_record(bound.call_id, "reply:held")
            self.assertEqual(row["state"], "unknown")
            stages = {r[0] for r in store._db.execute("SELECT stage FROM turn_timings")}
            self.assertTrue({"llm", "run"} <= stages)

            self.assertEqual(len(store.get_transcript(bound.call_id)), 1)
            self.assertIn('"state":"unknown"', store.get_review(bound.call_id).prompt)
            before = row.copy()
            store.recover_agents()
            self.assertEqual(store.agent_record(bound.call_id, "reply:held"), before)
        self.assertEqual(len(provider.calls), 1)

    async def test_actual_native_task_readonly_executor_and_owned_unknown(self):
        # The old task contract is data only and cannot bind to the current Agent.
        event, provider = self.environment()
        host = self.host()
        a = binding("old-native", "qq-A", "A")
        old = ReplyPolicy(
            a, ReplyModel(provider.meta().id, provider.get_model(), 32000, 100, NATIVE_CONTRACT)
        )
        with self.assertRaises(ReplyNotApplied):
            await AstrBotReplyAdapter.bind(
                self.context, old, read_call_state=lambda *a: None, privacy=host.privacy
            )
        self.assertEqual(provider.calls, [])

    async def test_persisted_turn_preflight_protocol_and_native_failures(self):
        event, provider = self.environment()
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        bound = backend._record(handle).binding
        record = backend._record(handle)
        # Before a persisted material/generation match, no native work may run.
        forged = Turn(bound.call_id, 1, "user", "forged", "synthetic", "forged", 1.0, None, 0)
        with self.assertRaises(Exception):
            await record.reply.reply(bound, forged)
        self.assertEqual(provider.calls, [])
        env = self.agent_env
        Tool = env["FunctionTool"]
        ToolSet = env["ToolSet"]
        available = []
        self.context.get_llm_tool_manager = lambda: NS(
            get_full_tool_set=lambda: ToolSet(list(available)),
            get_func=lambda n: next((t for t in available if t.name == n), None),
            get_builtin_tool=lambda *a: None,
        )
        mode = ["direct"]
        entered = []

        async def direct(event):
            await event.send(env["MessageChain"]().message("工具直接文字"))
            return None

        async def failed(event):
            await event.send(env["MessageChain"]().message("更早的片段"))
            raise RuntimeError("synthetic failure")

        async def ordinary(event):
            return "reported"

        available[:] = [
            Tool(
                name="generic",
                description="generic",
                parameters={"type": "object", "properties": {}},
                handler=direct,
            )
        ]
        turns = [0]

        async def answer(payload):
            if not payload.get("func_tool"):
                return response(BOT)
            turns[0] += 1
            if turns[0] % 2 == 1:
                return response(
                    "中间文字", role="tool", names=["generic"], args=[{}], ids=["native"]
                )
            return response("final")

        provider.responder = answer
        await backend.web_submit(web.token, web.reference, "direct", "普通请求")
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual(state.turns[-1].body, "工具直接文字")
        # Earlier direct text cannot mask a subsequent native error.
        available[0].handler = failed
        turns[0] = 0

        async def error_answer(payload):
            turns[0] += 1
            if turns[0] == 1:
                return response("prefix", role="tool", names=["generic"], args=[{}], ids=["bad"])
            return response("native failed", role="err")

        provider.responder = error_answer
        await backend.web_submit(web.token, web.reference, "error", "普通请求")
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual(state.generations[-1].state, "failed")
        self.assertEqual(state.turns[-1].role, "user")
        self.assertEqual(
            (await backend._runtime.agent_journal("read", bound.call_id, "reply:error"))["state"],
            "unknown",
        )
        # Native timeout is a framework error/unknown report, not no-side-effect proof.
        timed_out = asyncio.Event()

        async def timed(event):
            try:
                await asyncio.sleep(1)
            finally:
                timed_out.set()

        available[0].handler = timed
        from native_config import set_setting

        set_setting(self.context.configs[bound.unified_msg_origin], "tool_call_timeout", 1)
        turns[0] = 0
        provider.responder = answer
        await backend.web_submit(web.token, web.reference, "timeout", "合成超时")
        await backend.drain()
        self.assertTrue(timed_out.is_set())
        self.assertEqual(
            (await backend._runtime.agent_journal("read", bound.call_id, "reply:timeout"))["state"],
            "unknown",
        )
        set_setting(self.context.configs[bound.unified_msg_origin], "tool_call_timeout", 60)
        available.clear()
        for i, result in enumerate(
            [
                response(" ", raw=None),
                response("bad", raw={"status": "in_progress"}),
                response("bad", raw={"choices": [{"finish_reason": "length"}]}),
            ]
        ):
            provider.responder = lambda p, result=result: asyncio.sleep(0, result=result)
            await backend.web_submit(web.token, web.reference, "bad-" + str(i), "材料保留")
            await backend.drain()
            self.assertEqual((await backend.read_state(handle)).generations[-1].state, "failed")
        self.assertIn("控制协议", spoken("<tool_call>unsafe</tool_call>"))
        with self.assertRaises(UnicodeError):
            spoken("\ud800")
        with self.assertRaises(ValueError):
            spoken("   ")

        # A generic background report means submitted, never business complete.
        async def fake_background(**kw):
            await asyncio.sleep(0)

        self.enterContext(
            patch.object(
                env["FunctionToolExecutor"], "_execute_background", staticmethod(fake_background)
            )
        )
        available[:] = [
            Tool(
                name="background_generic",
                description="bg",
                parameters={"type": "object", "properties": {}},
                handler=ordinary,
                is_background_task=True,
            )
        ]
        turns[0] = 0

        async def background_answer(payload):
            turns[0] += 1
            if turns[0] == 1:
                return response(
                    "", role="tool", names=["background_generic"], args=[{}], ids=["bg"]
                )
            return response("已提交后台，结果未确认")

        provider.responder = background_answer
        await backend.web_submit(web.token, web.reference, "background", "合成后台请求")
        await backend.drain()
        row = await backend._runtime.agent_journal("read", bound.call_id, "reply:background")
        reports = json.loads(row["reports_json"])
        self.assertTrue(any(r.get("background_submitted") for r in reports))
        # Failed observation is not a fake hard tool gate and yields unknown.
        available[0].is_background_task = False
        turns[0] = 0
        old = record.reply.journal

        async def bad_journal(action, *args):
            if action == "report":
                raise RuntimeError("synthetic persistence failure")
            return await old(action, *args)

        record.reply.journal = bad_journal
        await backend.web_submit(web.token, web.reference, "observation", "合成观察失败")
        await backend.drain()
        self.assertEqual(
            (await backend._runtime.agent_journal("read", bound.call_id, "reply:observation"))[
                "state"
            ],
            "unknown",
        )
        record.reply.journal = old
