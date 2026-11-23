"""Phone input copies and disposable, non-blocking summary ownership."""

import asyncio
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from bot_call.call_history import CallHistory, history_chars
from bot_call.store import Store, _TIMING_SCHEMA
from test_astrbot_context import binding
from test_backend import NativeCase, response
from test_call_model import ChatProvider


def sample():
    return [
        {"role": "user", "content": "old user"},
        {
            "role": "assistant",
            "content": "<think>OLD_HISTORY_THOUGHT</think>old reply",
            "reasoning_content": "OLD_HISTORY_THOUGHT",
        },
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {"id": "old", "type": "function", "function": {"name": "lookup", "arguments": "{}"}}
            ],
        },
        {"role": "tool", "tool_call_id": "old", "content": "old tool result"},
        {"role": "user", "content": "recent user"},
        {
            "role": "assistant",
            "content": [
                {"type": "thinking", "thinking": "OLD_HISTORY_THOUGHT"},
                {"type": "text", "text": "<think>OLD_HISTORY_THOUGHT</think>recent reply"},
            ],
        },
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "new",
                    "type": "function",
                    "function": {
                        "name": "lookup",
                        "arguments": '{"thinking":"legitimate argument"}',
                    },
                }
            ],
        },
        {"role": "tool", "tool_call_id": "new", "content": "new result"},
    ]


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_copy_window_thoughts_and_all_source_tool_pairs_removed(self):
        original = sample()
        saved = deepcopy(original)
        recent, older, used = CallHistory(1).prepare(original)
        self.assertFalse(used)
        self.assertEqual(
            recent,
            [
                {"role": "user", "content": "recent user"},
                {"role": "assistant", "content": [{"type": "text", "text": "recent reply"}]},
            ],
        )
        for value in recent + older:
            self.assertNotIn(value.get("role"), {"tool", "function"})
            self.assertFalse(value.get("tool_calls") or value.get("function_call"))
        self.assertNotIn("OLD_HISTORY_THOUGHT", json.dumps(recent + older))
        self.assertNotIn("old tool result", json.dumps(older))
        self.assertEqual(original, saved)

    async def test_both_limits_zero_preserves_request_bytes_and_never_generates_summary(self):
        original = sample()
        original.append({"role": "user", "content": "中文\n空白  \t" + "长" * 9000})
        saved = json.dumps(original, ensure_ascii=False).encode("utf-8")
        history = CallHistory(0, summary=True, max_chars=0)
        # Even an existing summary must not enter the bypassed request copy.
        history.summary = "must not be injected"
        request, older, used = history.prepare(original)
        self.assertEqual(json.dumps(request, ensure_ascii=False).encode("utf-8"), saved)
        self.assertEqual(json.dumps(original, ensure_ascii=False).encode("utf-8"), saved)
        self.assertEqual(older, [])
        self.assertFalse(used)
        self.assertEqual(history.recent_chars, history.before_chars)
        request[5]["content"][0]["thinking"] = "copy mutation"
        self.assertEqual(json.dumps(original, ensure_ascii=False).encode("utf-8"), saved)

        async def forbidden_generate(older):
            self.fail("full bypass must not generate a summary")

        history.start(original, forbidden_generate)
        self.assertIsNone(history.task)
        await history.close()
        # Either independently enabled limit keeps the source-cleaning behavior.
        for limits in ({"turns": 0, "max_chars": 8000}, {"turns": 8, "max_chars": 0}):
            cleaned, _, _ = CallHistory(**limits).prepare(original)
            encoded = json.dumps(cleaned, ensure_ascii=False)
            self.assertNotIn("OLD_HISTORY_THOUGHT", encoded)
            self.assertFalse(any(m.get("role") == "tool" or m.get("tool_calls") for m in cleaned))

    async def test_large_recent_tool_history_char_budget_summary_bound_and_original_copy(self):
        original = [{"role": "system", "content": "earliest-system"}]
        for i in range(49):
            original.extend(
                [
                    {"role": "user", "content": f"用户{i}"},
                    {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {"id": str(i), "function": {"name": "lookup", "arguments": "{}"}}
                        ],
                    },
                    {"role": "tool", "tool_call_id": str(i), "content": "工具结果" + "大" * 16000},
                    {"role": "assistant", "content": f"公开结论{i}" + "答" * 1100},
                ]
            )
        saved = deepcopy(original)
        h = CallHistory()
        recent, older, _ = h.prepare(original)
        self.assertEqual(len(original), 197)
        self.assertLessEqual(history_chars(recent), 8000)
        self.assertEqual(h.recent_chars, history_chars(recent))
        self.assertNotIn("工具结果", json.dumps(recent + older, ensure_ascii=False))
        self.assertTrue(
            all(not m.get("tool_calls") and m.get("role") != "tool" for m in recent + older)
        )
        self.assertIn("公开结论48", json.dumps(recent, ensure_ascii=False))
        self.assertEqual(original, saved)
        large = [{"role": "user", "content": "长" * 9000}]
        short, _, _ = CallHistory().prepare(large)
        self.assertEqual(history_chars(short), 8000)
        self.assertTrue(short[0]["content"].endswith("（已截断）"))
        full, _, _ = CallHistory(max_chars=0).prepare(large)
        self.assertEqual(full, large)
        zero = CallHistory(0, max_chars=20)
        _, retired, _ = zero.prepare(large)

        async def summarize(retired):
            return "early summary"

        zero.start(retired, summarize)
        await zero.task
        self.assertEqual(zero.summary, "early summary")
        await zero.close()
        history = [{"role": "user", "content": f"old-{i}-" + "很长" * 2000} for i in range(30)] + [
            {"role": "user", "content": "latest"}
        ]
        _, older, _ = CallHistory(1).prepare(history)
        self.assertLessEqual(history_chars(older), 60000)
        self.assertNotIn("old-0-", json.dumps(older))
        self.assertIn("old-29-", json.dumps(older))
        legacy = [
            {"role": "assistant", "content": "公开前缀", "function_call": {"name": "f"}},
            {"role": "function", "name": "f", "content": "旧工具"},
        ]
        kept, _, _ = CallHistory().prepare(legacy)
        self.assertEqual(kept, [{"role": "assistant", "content": "公开前缀"}])

    async def test_summary_pending_failed_success_once_and_close(self):
        for outcome in ("failed", "success"):
            history = CallHistory(1)
            gate = asyncio.Event()
            calls = []

            async def generate(older):
                calls.append(older)
                await gate.wait()
                if outcome == "failed":
                    raise ValueError("synthetic private failure")
                return "summary facts"

            _, older, _ = history.prepare(sample())
            history.start(older, generate)
            await asyncio.sleep(0)
            self.assertFalse(history.prepare(sample())[2])
            history.start(older, generate)
            self.assertEqual(len(calls), 1)
            gate.set()
            await history.task
            recent, _, used = history.prepare(sample())
            self.assertEqual(used, outcome == "success")
            if used:
                self.assertIn("summary facts", recent[0]["content"])
            history.start(older, generate)
            self.assertEqual(len(calls), 1)
            self.assertGreaterEqual(history.elapsed_ms, 0)
            await history.close()
            self.assertIsNone(history.summary)
        for h in (CallHistory(0, False), CallHistory(1, False)):
            h.start(sample(), generate)
            self.assertIsNone(h.task)

    async def test_reload_cleanup_cancels_pending_summary(self):
        entered = asyncio.Event()

        async def generate(older):
            entered.set()
            await asyncio.Event().wait()

        history = CallHistory(1)
        history.start(sample(), generate)
        await asyncio.wait_for(entered.wait(), 1)
        await history.close()
        self.assertTrue(history.task.done())
        self.assertIsNone(history.summary)

    async def test_schema17_upgrade_preserves_old_observations(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "upgrade.db"
            with Store(path) as store:
                store.create_call(binding("old"))
                store._db.execute("DROP TABLE turn_timings")
                store._db.execute(_TIMING_SCHEMA[0])
                store._db.execute(
                    "INSERT INTO turn_timings VALUES ('old','reply:u','llm',0,0,1,'p','m','inherited','unknown',10,2,NULL,123)"
                )
                from schema_fixture import remove_series_columns

                remove_series_columns(store._db)
                from schema_fixture import remove_retry_schema

                remove_retry_schema(store._db)
                store._db.execute("PRAGMA user_version=17")
            with Store(path) as store:
                row = dict(store._db.execute("SELECT * FROM turn_timings").fetchone())
                self.assertEqual(row["prompt_tokens"], 10)
                self.assertEqual(row["recorded_at"], 123)
                self.assertIsNone(row["history_before"])
                store.record_timings(
                    "old",
                    "reply:u",
                    [
                        dict(
                            stage="history",
                            sequence=0,
                            start_ms=0,
                            elapsed_ms=1,
                            history_before=100,
                            history_after=16,
                            history_summary=1,
                        )
                    ],
                )
                observed = dict(
                    store._db.execute("SELECT * FROM turn_timings WHERE stage='history'").fetchone()
                )
                self.assertEqual(
                    (
                        observed["history_before"],
                        observed["history_after"],
                        observed["history_summary"],
                    ),
                    (100, 16, 1),
                )


class HistoryNativeTests(NativeCase):
    async def test_current_call_tool_reports_remain_outside_source_history_budget(self):
        from types import SimpleNamespace as NS

        event, provider = self.environment()
        source = sample()
        for message in source:
            if message.get("role") == "tool":
                message["content"] = "SYNTHETIC_SOURCE_TOOL" * 1000
        original = self.context.conversation_manager.items["A"].history = json.dumps(source)
        settings = self.settings
        self.settings = lambda: dict(
            settings(), call_history_max_chars=80, call_history_summary=False
        )
        result_text = "SYNTHETIC_CALL_TOOL" + "长" * 12000

        async def lookup(event):
            return result_text

        tool = self.agent_env["FunctionTool"](
            name="synthetic_lookup",
            description="synthetic",
            parameters={"type": "object", "properties": {}},
            handler=lookup,
        )
        self.context.get_llm_tool_manager = lambda: NS(
            get_full_tool_set=lambda: self.agent_env["ToolSet"]([tool]),
            get_func=lambda name: tool if name == tool.name else None,
            get_builtin_tool=lambda *a: None,
        )
        calls = []

        async def answer(payload):
            calls.append(True)
            if len(calls) == 1:
                return response("", role="tool", names=[tool.name], args=[{}], ids=["synthetic"])
            return response("简短结论。")

        provider.responder = answer
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "lookup", "查询")
        await backend.drain()
        self.assertEqual(len(provider.calls), 2)
        first_chars = history_chars(provider.calls[0]["contexts"])
        await backend.web_submit(web.token, web.reference, "followup", "继续")
        await backend.drain()
        payload = provider.calls[-1]
        contexts = json.dumps(payload["contexts"], ensure_ascii=False)
        self.assertIn(result_text, contexts)
        self.assertNotIn("SYNTHETIC_SOURCE_TOOL", contexts)
        reports = [
            m for m in payload["contexts"] if "本通先前主Agent框架报告" in str(m.get("content"))
        ]
        self.assertEqual(len(reports), 1)
        self.assertGreater(len(reports[0]["content"]), 12000)
        history = backend._record(handle).reply.history
        self.assertLessEqual(history.recent_chars, 80)
        self.assertIsNone(history.summary)
        self.assertEqual(self.context.conversation_manager.items["A"].history, original)
        self.assertEqual(self.context.conversation_manager.writes, [])
        self.reproduction_counts = dict(
            source_budget=80,
            source_recent_chars=history.recent_chars,
            synthetic_tool_chars=len(result_text),
            first_context_chars=first_chars,
            followup_context_chars=history_chars(payload["contexts"]),
            followup_report_chars=len(reports[0]["content"]),
            summary_used=False,
            original_history_unchanged=True,
        )

    async def test_background_summary_uses_actual_provider_without_blocking_or_history_writes(self):
        event, source_provider = self.environment()
        _, provider = self.environment("qq-B", "B", provider_class=ChatProvider)
        original = self.context.conversation_manager.items["A"].history = json.dumps(sample())
        self.settings = lambda: dict(
            enabled=True,
            public_origin="https://calls.example.test",
            call_history_turns=1,
            call_history_summary=True,
            call_history_max_chars=123,
            call_chat_provider_id=provider.meta().id,
        )
        gate = asyncio.Event()
        summary_calls = []
        native = self.context.llm_generate

        async def generate(**kwargs):
            summary_calls.append(deepcopy(kwargs))
            await gate.wait()
            return response("earlier summary")

        self.context.llm_generate = generate
        backend, host = await self.backend()
        handle, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "u1", "phone first")
        await asyncio.wait_for(backend.drain(), 3)
        self.assertEqual(len(summary_calls), 1)
        self.assertEqual(summary_calls[0]["chat_provider_id"], provider.meta().id)
        self.assertNotIn("OLD_HISTORY_THOUGHT", json.dumps(summary_calls))
        gate.set()
        await backend._record(handle).reply.history.task
        await backend.web_submit(web.token, web.reference, "u2", "phone second")
        await backend.drain()
        payload = provider.calls[-1]
        text = json.dumps(payload["contexts"], ensure_ascii=False)
        self.assertIn("earlier summary", text)
        self.assertIn("phone first", text)
        self.assertNotIn("OLD_HISTORY_THOUGHT", text)
        self.assertIn("PRIVATE_PERSONA_PROMPT", text)
        self.assertEqual(source_provider.calls, [])
        self.assertEqual(self.context.conversation_manager.items["A"].history, original)
        self.assertEqual(self.context.conversation_manager.writes, [])
        self.assertEqual(len(summary_calls), 1)
        with Store(self.path / "calls.db") as store:
            row = store._db.execute(
                "SELECT history_after FROM turn_timings WHERE stage='history_chars' LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertLessEqual(row[0], 123)
        self.context.llm_generate = native
