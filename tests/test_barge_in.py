"""P2 durable protocol and native cancellation; all Providers/tools synthetic."""

import asyncio
import json
import unittest
from types import SimpleNamespace as NS

from test_backend import NativeCase, PublicProvider, response
from bot_call.access import AccessError
from bot_call.barge_in import (
    decide_candidate,
    valid_request,
    CandidateContext,
    DEFAULT_BACKCHANNEL_PHRASES,
    BargeStore,
    REASONS,
    valid_phrases,
)


class CandidateProvider(PublicProvider):
    async def text_chat_stream(self, **payload):
        yield response("这是一句已经生成的公开文字。", chunk=True)
        result = await self.text_chat(**payload)
        yield result


class BargeTests(NativeCase):
    def command(self, uid, action="begin", gid="reply:old", seq=1):
        return dict(
            utterance_id=uid, generation_id=gid, seq=seq, position_ms=137, started=1, action=action
        )

    async def wait_for(self, fn):
        for _ in range(300):
            result = await fn()
            if result:
                return result
            await asyncio.sleep(0.01)
        self.fail("synthetic progress deadline")

    async def test_single_stt_resume_interrupt_late_fences_projection_review_and_migration(self):
        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        call_id = backend._record(handle).binding.call_id
        await backend.web_submit(web.token, web.reference, "old", "初始问题")
        await backend.drain()
        await backend.web_segment(
            web.token, web.reference, "reply:old", 1, dict(revision=1, state="started")
        )
        stt = self.context.stt[event.unified_msg_origin]
        stt.transcriber = lambda _: asyncio.sleep(0, result="")
        row = await backend.web_barge_in(web.token, web.reference, self.command("empty"))
        self.assertEqual(row["decision"], "pending")
        await backend.web_audio(web.token, web.reference, "empty", b"empty", "audio/webm")
        await backend.drain()
        state = await backend.web_state(web.token, web.reference)
        self.assertEqual(state.barge_in[0]["decision"], "resume")
        self.assertEqual(len(provider.calls), 1)
        self.assertNotEqual(state.segments[0]["state"], "stopped")
        await backend.web_audio(web.token, web.reference, "empty", b"empty", "audio/webm")
        self.assertEqual(len(stt.paths), 1, "same-id retry replayed STT")
        stt.transcriber = lambda _: asyncio.sleep(0, result="不对，我补充一个条件")
        await backend.web_barge_in(web.token, web.reference, self.command("real"))
        await backend.web_audio(web.token, web.reference, "real", b"real", "audio/webm")
        await backend.drain()
        state = await backend.web_state(web.token, web.reference)
        real = next(r for r in state.barge_in if r["utterance_id"] == "real")
        self.assertEqual((real["decision"], real["dispatch"]), ("interrupt", "started"))
        self.assertEqual(len(stt.paths), 2)
        self.assertEqual(len(provider.calls), 2)
        old = [r for r in state.segments if r["generation_id"] == "reply:old"]
        self.assertEqual(old[0]["state"], "stopped")
        with self.assertRaises(AccessError):
            await backend.web_segment(web.token, web.reference, "reply:old", 2)
        await backend.web_segment(
            web.token, web.reference, "reply:old", 1, dict(revision=999, state="ended")
        )
        projected = await backend._runtime.agent_journal("projection", call_id)
        partial = next(
            t for t in projected if t.role == "assistant" and t.material_id == "reply:old"
        )
        self.assertEqual(partial.body, "")
        self.assertIn("partial_words=unknown", (await backend.read_state(handle)).material_notes)
        contexts = provider.calls[-1]["contexts"]
        self.assertTrue(
            any(
                m["role"] == "system"
                and "interrupted=true" in m["content"]
                and "tool_reports_remain_valid=true" in m["content"]
                for m in contexts
            )
        )
        self.assertFalse(
            any(m["role"] == "assistant" and "朗读被打断" in m["content"] for m in contexts)
        )
        raw = await backend.read_state(handle)
        original = next(t for t in raw.raw_turns if t.material_id == "reply:old")
        self.assertNotEqual(partial.body, original.body)
        await backend.web_end(web.token, web.reference)
        await backend.drain()
        review = (await backend.read_state(handle)).review
        self.assertIn("朗读被打断，仅部分播放", review.prompt)
        self.assertIn(original.body, review.prompt)
        self.assertEqual(decide_candidate("嗯", CandidateContext("middle")).action, "resume")

    async def test_protocol_rejects_wrong_fields_identity_off_and_silent_after_audio(self):
        event, _ = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        _, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "old", "问题")
        await backend.drain()
        for changes in (
            dict(seq=True),
            dict(position_ms=-1),
            dict(started=True),
            dict(remaining_ms=True),
            dict(remaining_ms=-1),
            dict(remaining_ms=3600001),
            dict(action="resume"),
            dict(extra=1),
        ):
            data = dict(self.command("bad"), **changes)
            self.assertFalse(valid_request(data))
            with self.assertRaises(AccessError):
                await backend.web_barge_in(web.token, web.reference, data)
        await backend.web_barge_in(web.token, web.reference, self.command("silent"))
        row = await backend.web_barge_in(web.token, web.reference, self.command("silent", "silent"))
        self.assertEqual(row["decision"], "resume")
        backend._runtime._barge_in = False
        with self.assertRaises(AccessError):
            await backend.web_barge_in(web.token, web.reference, self.command("off"))

    async def test_timeout_continues_same_expression_and_controls_never_dispatch(self):
        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        _, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "old", "问题")
        await backend.drain()
        await backend.web_barge_in(web.token, web.reference, self.command("long"))
        await backend.web_barge_in(web.token, web.reference, self.command("long", "timeout"))
        stt = self.context.stt[event.unified_msg_origin]
        stt.transcriber = lambda _: asyncio.sleep(0, result="完整的连续表达")
        await backend.web_audio(
            web.token, web.reference, "long", b"part1", "audio/webm", ("long", 1, False)
        )
        await backend.drain()
        self.assertEqual(len(provider.calls), 1)
        await backend.web_audio(
            web.token, web.reference, "long-2", b"part2", "audio/webm", ("long", 2, True)
        )
        await backend.drain()
        self.assertEqual(len(provider.calls), 2)
        state = await backend.web_state(web.token, web.reference)
        self.assertEqual(state.barge_in[0]["closed_reason"], "timeout")
        # Candidate for the new reply, revoked by explicit control.
        await backend.web_barge_in(
            web.token, web.reference, self.command("control", gid="reply:long")
        )
        await backend.web_barge_in(
            web.token, web.reference, self.command("control", "long_speech", gid="reply:long")
        )
        await backend.web_barge_in(
            web.token, web.reference, self.command("control", "cancel", gid="reply:long")
        )
        await backend.web_audio(web.token, web.reference, "control", b"control", "audio/webm")
        await backend.drain()
        self.assertEqual(len(provider.calls), 2)

    async def test_long_speech_seals_output_before_audio_then_transcribes_dispatches_once(self):
        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        _, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "old", "问题")
        await backend.drain()
        stt = self.context.stt[event.unified_msg_origin]
        stt.transcriber = lambda _: asyncio.sleep(0, result="完整长表达，包含停顿后的条件")
        await backend.web_barge_in(web.token, web.reference, self.command("long"))
        for _ in range(2):
            row = await backend.web_barge_in(
                web.token, web.reference, self.command("long", "long_speech")
            )
            self.assertEqual(
                (row["closed_reason"], row["decision"], row["dispatch"]),
                ("long_speech", "pending", "pending"),
            )
        state = await backend.web_state(web.token, web.reference)
        self.assertEqual(state.segments[0]["state"], "stopped")
        self.assertEqual(len(stt.paths), 0)
        self.assertEqual(
            len(provider.calls), 1, "early acoustic confirmation dispatched a partial turn"
        )
        with self.assertRaises(AccessError):
            await backend.web_segment(web.token, web.reference, "reply:old", 2)
        for _ in range(2):
            await backend.web_audio(web.token, web.reference, "long", b"complete", "audio/webm")
            await backend.drain()
        state = await backend.web_state(web.token, web.reference)
        self.assertEqual(
            (
                state.barge_in[0]["decision"],
                state.barge_in[0]["dispatch"],
                state.barge_in[0]["closed_reason"],
            ),
            ("interrupt", "started", "long_speech"),
        )
        self.assertEqual(len(stt.paths), 1)
        self.assertEqual(len(provider.calls), 2)
        with self.assertRaises(AccessError):
            await backend.web_barge_in(
                web.token, web.reference, self.command("long", "long_speech")
            )

    async def test_native_wait_cancellation_tools_finish_before_next_reply(self):
        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        bound = backend._record(handle).binding
        entered, release, completed = asyncio.Event(), asyncio.Event(), []
        Tool, ToolSet = self.agent_env["FunctionTool"], self.agent_env["ToolSet"]

        async def tool(event):
            entered.set()
            try:
                await release.wait()
                completed.append("fact")
                return "已完成一次真实模拟操作"
            except asyncio.CancelledError:
                completed.append("cancelled")
                raise

        available = [
            Tool(
                name="owned_tool",
                description="test",
                parameters={"type": "object", "properties": {}},
                handler=tool,
            )
        ]
        self.context.get_llm_tool_manager = lambda: NS(
            get_full_tool_set=lambda: ToolSet(list(available)),
            get_func=lambda n: available[0] if n == "owned_tool" else None,
            get_builtin_tool=lambda *a: None,
        )
        count = 0

        async def answer(payload):
            nonlocal count
            count += 1
            if count == 1:
                return response(
                    "",
                    role="tool",
                    names=["owned_tool", "owned_tool"],
                    args=[{}, {}],
                    ids=["a", "b"],
                )
            return response("新一轮回复")

        provider.responder = answer
        await backend.web_submit(web.token, web.reference, "old", "工具问题")
        await asyncio.wait_for(entered.wait(), 2)

        async def segment():
            state = await backend.web_state(web.token, web.reference)
            return any(r["seq"] == 1 and r["state"] == "succeeded" for r in state.segments)

        await self.wait_for(segment)
        await backend.web_barge_in(web.token, web.reference, self.command("new"))
        await backend.web_audio(web.token, web.reference, "new", b"new", "audio/webm")

        async def waiting():
            state = await backend.web_state(web.token, web.reference)
            return any(r["dispatch"] == "waiting" for r in state.barge_in)

        await self.wait_for(waiting)
        self.assertEqual(count, 1)
        self.assertEqual(completed, [])
        release.set()
        await backend.drain()
        self.assertEqual(
            completed, ["fact"], "already-running tool cancelled or queued sibling executed"
        )
        self.assertEqual(count, 2, "old Agent requested another model result")
        reports = await backend._runtime.agent_journal("read", bound.call_id, "reply:old")
        self.assertTrue(
            any(r.get("kind") == "framework_report" for r in json.loads(reports["reports_json"]))
        )

    async def test_owned_llm_wait_and_submitted_background_fact_are_not_replayed(self):
        for background in (False, True):
            event, provider = self.environment(
                platform="p" + str(background),
                cid=str(background),
                provider_class=CandidateProvider,
            )
            backend, _ = await self.backend(name="wait-" + str(background) + ".db")
            handle, web = await self.enter(backend, event)
            record = backend._record(handle)
            blocked = asyncio.Event()
            calls = 0
            if background:
                from unittest.mock import patch

                Tool, ToolSet = self.agent_env["FunctionTool"], self.agent_env["ToolSet"]
                submitted = []

                background_entered, background_release, background_done = (
                    asyncio.Event(),
                    asyncio.Event(),
                    asyncio.Event(),
                )
                self.addCleanup(background_release.set)

                async def operation(event):
                    submitted.append("committed")
                    background_entered.set()
                    await background_release.wait()
                    return "后台工具已完成"

                async def wake(**kw):
                    submitted.append("result-observed")
                    background_done.set()

                # Execute native submission, registry ownership and local tool;
                # replace only the final platform/Agent wake-up (external IO).
                self.enterContext(
                    patch.object(
                        self.agent_env["FunctionToolExecutor"],
                        "_wake_main_agent_for_background_result",
                        staticmethod(wake),
                    )
                )
                available = [
                    Tool(
                        name="bg",
                        description="test",
                        parameters={"type": "object", "properties": {}},
                        handler=operation,
                        is_background_task=True,
                    )
                ]
                self.context.get_llm_tool_manager = lambda: NS(
                    get_full_tool_set=lambda: ToolSet(list(available)),
                    get_func=lambda n: available[0] if n == "bg" else None,
                    get_builtin_tool=lambda *a: None,
                )

            async def answer(payload):
                nonlocal calls
                calls += 1
                if background and calls == 1:
                    return response("", role="tool", names=["bg"], args=[{}], ids=["bg"])
                if calls == (2 if background else 1):
                    blocked.set()
                    await asyncio.Event().wait()
                return response("新回复")

            provider.responder = answer
            await backend.web_submit(web.token, web.reference, "old", "问题")
            await asyncio.wait_for(blocked.wait(), 2)
            await backend.web_barge_in(web.token, web.reference, self.command("take"))
            await backend.web_audio(web.token, web.reference, "take", b"new", "audio/webm")
            await backend.drain()
            self.assertEqual(calls, 3 if background else 2)
            self.assertNotIn("reply:old", record.reply.attempts)
            state = await backend.read_state(handle)
            old = next(
                t for t in state.raw_turns if t.role == "assistant" and t.material_id == "reply:old"
            )
            self.assertIn("已经生成", old.body)
            self.assertTrue(
                any(
                    t.material_id == "reply:old" and "部分播放" not in t.body
                    for t in state.projected_turns
                )
            )
            if background:
                await asyncio.wait_for(background_entered.wait(), 2)
                self.assertEqual(submitted, ["committed"])
                self.assertFalse(background_done.is_set(), "old LLM stop cancelled background work")
                background_release.set()
                await asyncio.wait_for(background_done.wait(), 2)
                self.assertEqual(submitted, ["committed", "result-observed"])
                report = await backend._runtime.agent_journal(
                    "read", record.binding.call_id, "reply:old"
                )
                self.assertTrue(
                    any(r.get("background_submitted") for r in json.loads(report["reports_json"]))
                )

    async def test_stop_reading_is_ordinary_interruption(self):
        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "old", "问题")
        await backend.drain()
        await backend.web_barge_in(web.token, web.reference, self.command("stop"))
        await backend.web_barge_in(web.token, web.reference, self.command("stop", "long_speech"))
        self.context.stt[event.unified_msg_origin].transcriber = lambda _: asyncio.sleep(
            0, result="别说了"
        )
        await backend.web_audio(web.token, web.reference, "stop", b"stop", "audio/webm")
        await backend.drain()
        state = await backend.web_state(web.token, web.reference)
        self.assertEqual(
            (
                state.barge_in[0]["decision"],
                state.barge_in[0]["dispatch"],
                state.barge_in[0]["reason"],
            ),
            ("interrupt", "started", "long_speech"),
        )
        self.assertEqual(len(provider.calls), 2)
        self.assertEqual(len(self.context.stt[event.unified_msg_origin].paths), 1)

    async def test_question_really_dispatches_once(self):
        await self._answer_dispatch_case("question_context")

    async def test_boundary_really_dispatches_once(self):
        await self._answer_dispatch_case("boundary")

    async def _answer_dispatch_case(self, kind):
        class QuestionProvider(PublicProvider):
            async def text_chat_stream(self, **payload):
                yield response("你觉得对吗？", chunk=True)
                self.calls.append(payload)
                yield response("你觉得对吗？")

        event, provider = self.environment(
            provider_class=QuestionProvider if kind == "question_context" else CandidateProvider
        )
        backend, _ = await self.backend(name=kind + ".db")
        _, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "old", "问题")
        await backend.drain()
        state = await backend.web_state(web.token, web.reference)
        seq = max(p["seq"] for p in state.segments if p["generation_id"] == "reply:old")
        command = dict(
            self.command("answer", seq=seq),
            remaining_ms=500 if kind == "boundary" else 5000,
        )
        await backend.web_barge_in(web.token, web.reference, command)
        stt = self.context.stt[event.unified_msg_origin]
        stt.transcriber = lambda _: asyncio.sleep(0, result="对")
        await backend.web_audio(web.token, web.reference, "answer", b"answer", "audio/webm")
        await backend.drain()
        row = (await backend.web_state(web.token, web.reference)).barge_in[0]
        self.assertEqual(
            (row["decision"], row["dispatch"], row["reason"]),
            ("interrupt", "started", kind),
        )
        self.assertEqual(len(provider.calls), 2)
        self.assertEqual(len(stt.paths), 1)

    async def test_backchannel_original_projection_review_no_agent_and_next_interrupt(self):
        await self._backchannel_material_case(False)

    async def test_backchannel_series_releases_slot_without_missing_material_note(self):
        await self._backchannel_material_case(True)

    async def _backchannel_material_case(self, series):
        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        call_id = backend._record(handle).binding.call_id
        await backend.web_submit(web.token, web.reference, "old", "初始问题")
        await backend.drain()
        # A known following sentence, or >1s duration, proves mid-reply context.
        command = dict(self.command("assent"), remaining_ms=5000)
        await backend.web_barge_in(web.token, web.reference, command)
        original = "嗯" * 120 + "，嗯。"
        stt = self.context.stt[event.unified_msg_origin]
        stt.transcriber = lambda _: asyncio.sleep(0, result=original)
        await backend.web_audio(
            web.token,
            web.reference,
            "assent",
            b"assent",
            "audio/webm",
            ("assent", 1, True) if series else None,
        )
        await backend.drain()
        state = await backend.read_state(handle)
        observed = (await backend.web_state(web.token, web.reference)).barge_in[0]
        self.assertEqual(
            (observed["decision"], observed["reason"]), ("resume", "backchannel"), observed
        )
        turn = next(t for t in state.raw_turns if t.material_id == "assent")
        self.assertEqual((turn.source, turn.body), ("audio_transcription", original))
        self.assertEqual(
            next(t for t in state.turns if t.material_id == "assent").source, "backchannel"
        )
        self.assertEqual(len(provider.calls), 1)
        if series:
            self.assertNotIn("未完成", state.material_notes)
            self.assertEqual(
                (await backend.web_state(web.token, web.reference)).series[0]["state"], "replied"
            )
        projected = next(t for t in state.projected_turns if t.material_id == "assent")
        self.assertEqual(original, projected.body)
        self.assertIn("purpose=backchannel", state.material_notes)
        self.assertIn("不代表同意、决定或工具授权", state.material_notes)
        row = (await backend.web_state(web.token, web.reference)).barge_in[0]
        self.assertEqual((row["decision"], row["reason"]), ("resume", "backchannel"))
        await backend.web_barge_in(web.token, web.reference, dict(command, action="resumed"))
        await backend.web_audio(
            web.token,
            web.reference,
            "assent",
            b"assent",
            "audio/webm",
            ("assent", 1, True) if series else None,
        )
        self.assertEqual(len(stt.paths), 1)
        stt.transcriber = lambda _: asyncio.sleep(0, result="嗯，但是我想问新问题")
        await backend.web_barge_in(
            web.token, web.reference, dict(self.command("real"), remaining_ms=5000)
        )
        await backend.web_audio(web.token, web.reference, "real", b"real", "audio/webm")
        await backend.drain()
        self.assertEqual(len(provider.calls), 2)
        self.assertIn("用户附和", json.dumps(provider.calls[-1]["contexts"], ensure_ascii=False))
        self.assertIn(original, json.dumps(provider.calls[-1]["contexts"], ensure_ascii=False))
        await backend.web_end(web.token, web.reference)
        await backend.drain()
        review = (await backend.read_state(handle)).review
        self.assertIn('"source":"backchannel"', review.input_json)
        self.assertIn(original, review.input_json)
        self.assertIn("不代表接受建议", review.prompt)

    async def test_native_request_stop_is_not_a_tool_rollback(self):
        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        entered, cancelled = asyncio.Event(), asyncio.Event()
        facts = []

        async def operation(event):
            facts.append("committed")
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        Tool, ToolSet = self.agent_env["FunctionTool"], self.agent_env["ToolSet"]
        tool = Tool(
            name="stop_boundary",
            description="test",
            parameters={"type": "object", "properties": {}},
            handler=operation,
        )
        self.context.get_llm_tool_manager = lambda: NS(
            get_full_tool_set=lambda: ToolSet([tool]),
            get_func=lambda n: tool if n == tool.name else None,
            get_builtin_tool=lambda *a: None,
        )
        provider.responder = lambda payload: asyncio.sleep(
            0, result=response("", role="tool", names=[tool.name], args=[{}], ids=["x"])
        )
        await backend.web_submit(web.token, web.reference, "old", "问题")
        await asyncio.wait_for(entered.wait(), 2)
        attempt, _ = backend._record(handle).reply.attempts["reply:old"]
        attempt.runner.request_stop()  # Deliberate boundary experiment, not P2 policy.
        await asyncio.wait_for(cancelled.wait(), 2)
        await backend.drain()
        self.assertEqual(facts, ["committed"])
        self.assertEqual(len(provider.calls), 1)
        report = await backend._runtime.agent_journal(
            "read", backend._record(handle).binding.call_id, "reply:old"
        )
        self.assertEqual(report["state"], "unknown")

    async def test_cancel_while_waiting_revokes_dispatch_but_preserves_tool_fact(self):
        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        entered, release = asyncio.Event(), asyncio.Event()
        facts = []
        Tool, ToolSet = self.agent_env["FunctionTool"], self.agent_env["ToolSet"]

        async def operation(event):
            entered.set()
            await release.wait()
            facts.append("committed")
            return "已完成"

        tool = Tool(
            name="waiting",
            description="test",
            parameters={"type": "object", "properties": {}},
            handler=operation,
        )
        self.context.get_llm_tool_manager = lambda: NS(
            get_full_tool_set=lambda: ToolSet([tool]),
            get_func=lambda n: tool if n == tool.name else None,
            get_builtin_tool=lambda *a: None,
        )
        provider.responder = lambda _: asyncio.sleep(
            0, result=response("", role="tool", names=[tool.name], args=[{}], ids=["w"])
        )
        await backend.web_submit(web.token, web.reference, "old", "问题")
        await asyncio.wait_for(entered.wait(), 2)
        await backend.web_barge_in(web.token, web.reference, self.command("new"))
        await backend.web_audio(web.token, web.reference, "new", b"new", "audio/webm")

        async def waiting():
            state = await backend.web_state(web.token, web.reference)
            return state.barge_in and state.barge_in[0]["dispatch"] == "waiting"

        await self.wait_for(waiting)
        await backend.web_barge_in(web.token, web.reference, self.command("new", "cancel"))
        release.set()
        await backend.drain()
        self.assertEqual(facts, ["committed"])
        self.assertEqual(len(provider.calls), 1)
        state = await backend.web_state(web.token, web.reference)
        self.assertEqual(state.barge_in[0]["dispatch"], "cancelled")

    async def test_native_executor_failure_does_not_deadlock_queued_tool_guard(self):
        from unittest.mock import patch

        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        entered, release = asyncio.Event(), asyncio.Event()
        attempts = []
        Tool, ToolSet = self.agent_env["FunctionTool"], self.agent_env["ToolSet"]
        tool = Tool(
            name="fails",
            description="test",
            parameters={"type": "object", "properties": {}},
            handler=lambda event: None,
        )
        self.context.get_llm_tool_manager = lambda: NS(
            get_full_tool_set=lambda: ToolSet([tool]),
            get_func=lambda n: tool if n == tool.name else None,
            get_builtin_tool=lambda *a: None,
        )

        async def executor(**kwargs):
            attempts.append("entered")
            entered.set()
            await release.wait()
            raise RuntimeError("synthetic executor outcome unknown")
            yield  # Native async-generator interface.

        self.enterContext(
            patch.object(self.agent_env["FunctionToolExecutor"], "execute", staticmethod(executor))
        )
        count = 0

        async def answer(_):
            nonlocal count
            count += 1
            if count == 1:
                return response(
                    "",
                    role="tool",
                    names=[tool.name, tool.name],
                    args=[{}, {}],
                    ids=["first", "queued"],
                )
            return response("新回复")

        provider.responder = answer
        await backend.web_submit(web.token, web.reference, "old", "问题")
        await asyncio.wait_for(entered.wait(), 2)
        await backend.web_barge_in(web.token, web.reference, self.command("new"))
        await backend.web_audio(web.token, web.reference, "new", b"new", "audio/webm")

        async def waiting():
            state = await backend.web_state(web.token, web.reference)
            return state.barge_in and state.barge_in[0]["dispatch"] == "waiting"

        await self.wait_for(waiting)
        release.set()
        await asyncio.wait_for(backend.drain(), 5)
        self.assertEqual(attempts, ["entered"])
        self.assertEqual(count, 2)
        report = await backend._runtime.agent_journal(
            "read", backend._record(handle).binding.call_id, "reply:old"
        )
        self.assertEqual(report["state"], "unknown")

    async def test_silent_cannot_override_inflight_stt_and_resume_ack_is_not_stop(self):
        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        _, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "old", "问题")
        await backend.drain()
        entered, release = asyncio.Event(), asyncio.Event()

        async def transcribe(_):
            entered.set()
            await release.wait()
            return ""

        self.context.stt[event.unified_msg_origin].transcriber = transcribe
        await backend.web_barge_in(web.token, web.reference, self.command("empty"))
        await backend.web_audio(web.token, web.reference, "empty", b"empty", "audio/webm")
        await entered.wait()
        with self.assertRaises(AccessError):
            await backend.web_barge_in(web.token, web.reference, self.command("empty", "silent"))
        release.set()
        await backend.drain()
        await backend.web_barge_in(web.token, web.reference, self.command("empty", "resumed"))
        state = await backend.web_state(web.token, web.reference)
        self.assertNotEqual(state.segments[0]["state"], "stopped")
        self.assertEqual(state.barge_in[0]["closed_reason"], "resumed")
        self.assertEqual(len(provider.calls), 1)

    async def test_seal_without_audio_tail_uses_one_candidate_and_one_reply(self):
        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        _, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "old", "问题")
        await backend.drain()
        await backend.web_barge_in(web.token, web.reference, self.command("series"))
        await backend.web_audio(
            web.token, web.reference, "series", b"one", "audio/webm", ("series", 1, False)
        )
        await backend.drain()
        self.assertEqual(len(provider.calls), 1)
        for _ in range(2):
            await backend.web_audio_series_control(
                web.token, web.reference, "series", "seal", 1, "no_audio_tail"
            )
            await backend.drain()
        self.assertEqual(len(provider.calls), 2)
        self.assertEqual(len(self.context.stt[event.unified_msg_origin].paths), 1)

    async def test_confirmation_cancels_owned_tts_and_rejects_late_result(self):
        from test_backend import MP3

        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        _, web = await self.enter(backend, event)
        entered, cancelled = asyncio.Event(), asyncio.Event()
        tts = self.context.tts[event.unified_msg_origin]
        path = self.path / "held-tts.mp3"
        path.write_bytes(MP3)
        calls = 0

        async def synthesize(text):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    # An SDK may suppress cancellation and return a late file.
                    return str(path)
            return str(path)

        tts.synthesizer = synthesize
        await backend.web_submit(web.token, web.reference, "old", "问题")
        await asyncio.wait_for(entered.wait(), 2)
        await backend.web_barge_in(web.token, web.reference, self.command("new"))
        await backend.web_audio(web.token, web.reference, "new", b"new", "audio/webm")
        await asyncio.wait_for(cancelled.wait(), 2)
        await backend.drain()
        state = await backend.web_state(web.token, web.reference)
        old = [r for r in state.segments if r["generation_id"] == "reply:old"]
        self.assertEqual(old[0]["state"], "stopped")
        self.assertNotEqual(old[1]["state"], "succeeded")
        with self.assertRaises(AccessError):
            await backend.web_segment(web.token, web.reference, "reply:old", 2)
        self.assertEqual(len(provider.calls), 2)

    async def test_empty_or_cancelled_series_releases_expression_slot_without_replay(self):
        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        _, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "old", "问题")
        await backend.drain()
        stt = self.context.stt[event.unified_msg_origin]
        stt.transcriber = lambda _: asyncio.sleep(0, result="")
        await backend.web_barge_in(web.token, web.reference, self.command("empty-series"))
        await backend.web_audio(
            web.token,
            web.reference,
            "empty-series",
            b"empty",
            "audio/webm",
            ("empty-series", 1, True),
        )
        await backend.drain()
        state = await backend.web_state(web.token, web.reference)
        self.assertEqual(state.barge_in[0]["decision"], "resume")
        self.assertEqual(state.series[0]["state"], "interrupted")
        # No generation was requested for the false alarm, but the next
        # expression can enter normally and does not replay its STT.
        stt.transcriber = lambda _: asyncio.sleep(0, result="下一句话")
        await backend.web_audio(web.token, web.reference, "next", b"next", "audio/webm")
        await backend.drain()
        self.assertEqual(len(provider.calls), 2)
        self.assertEqual(len(stt.paths), 2)


class BargeMigrationTests(unittest.TestCase):
    def test_schema21_additive_upgrade_and_atomic_failure(self):
        import sqlite3
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from unittest.mock import patch
        from bot_call.store import Store
        from test_astrbot_context import binding

        with TemporaryDirectory() as directory:
            path = Path(directory) / "old.db"
            with Store(path) as store:
                store.create_call(binding("old"))
                store.append_user("old", "u", "原文保持", source="synthetic")
                store.claim_generation("old", "reply:u", utterance_id="u", source="fake_reply")
                store.add_segment("old", "reply:u", "完整公开文字。")
                store.complete_generation("old", "reply:u", "完整公开文字。")
                db = store._db
                db.execute("DROP TABLE barge_candidates")
                for column in ("play_started_at", "playback_position_ms", "end_reason"):
                    db.execute("ALTER TABLE speech_segments DROP COLUMN " + column)
                from schema_fixture import remove_retry_schema

                remove_retry_schema(db)
                db.execute("PRAGMA user_version=21")
                before = [tuple(r) for r in db.execute("SELECT * FROM turns")]
                segments = [tuple(r) for r in db.execute("SELECT * FROM speech_segments")]
                old_columns = [r[1] for r in db.execute("PRAGMA table_info(speech_segments)")]
                ddl = db.execute(
                    "SELECT sql FROM sqlite_master WHERE name='speech_segments'"
                ).fetchone()[0]
            with patch("bot_call.store._BARGE_SCHEMA", "INVALID"):
                with self.assertRaises(sqlite3.Error):
                    Store(path)
            with sqlite3.connect(path) as db:
                self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 21)
                self.assertEqual(
                    db.execute(
                        "SELECT sql FROM sqlite_master WHERE name='speech_segments'"
                    ).fetchone()[0],
                    ddl,
                )
            with (
                sqlite3.connect(path) as source,
                sqlite3.connect(Path(directory) / "calls-consistent.sqlite3") as backup,
            ):
                source.backup(backup)
            with Store(path) as store:
                self.assertEqual(store._db.execute("PRAGMA user_version").fetchone()[0], 24)
                self.assertEqual(
                    [tuple(r) for r in store._db.execute("SELECT * FROM turns")], before
                )
                self.assertEqual(
                    [
                        tuple(r)
                        for r in store._db.execute(
                            "SELECT " + ",".join(old_columns) + " FROM speech_segments"
                        )
                    ],
                    segments,
                )
                self.assertEqual(
                    tuple(
                        store._db.execute(
                            "SELECT play_started_at,playback_position_ms,end_reason FROM speech_segments LIMIT 1"
                        ).fetchone()
                    ),
                    (None, None, None),
                )


class ParallelToolTests(NativeCase):
    async def test_parallel_children_reach_safe_boundary_before_native_stop(self):
        event, provider = self.environment(provider_class=CandidateProvider)
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        first, second, entered = asyncio.Event(), asyncio.Event(), asyncio.Event()
        starts, done, stops = [], [], []

        async def child(name, gate):
            starts.append(name)
            if len(starts) == 2:
                entered.set()
            await gate.wait()
            done.append(name)

        async def operation(event):
            await asyncio.gather(child("a", first), child("b", second))
            return "两个并发子操作均已完成"

        Tool, ToolSet = self.agent_env["FunctionTool"], self.agent_env["ToolSet"]
        tool = Tool(
            name="parallel_children",
            description="test",
            parameters={"type": "object", "properties": {}},
            handler=operation,
        )
        self.context.get_llm_tool_manager = lambda: NS(
            get_full_tool_set=lambda: ToolSet([tool]),
            get_func=lambda n: tool if n == tool.name else None,
            get_builtin_tool=lambda *a: None,
        )
        count = 0

        async def answer(payload):
            nonlocal count
            count += 1
            if count == 1:
                return response("", role="tool", names=[tool.name], args=[{}], ids=["parallel"])
            return response("新一轮")

        provider.responder = answer
        await backend.web_submit(web.token, web.reference, "old", "并发工具问题")
        await asyncio.wait_for(entered.wait(), 2)
        attempt, _ = backend._record(handle).reply.attempts["reply:old"]
        native_stop = attempt.runner.request_stop

        def observed_stop():
            stops.append(len(done))
            native_stop()

        attempt.runner.request_stop = observed_stop
        await backend.web_barge_in(
            web.token,
            web.reference,
            dict(
                utterance_id="new",
                generation_id="reply:old",
                seq=1,
                position_ms=200,
                started=1,
                action="begin",
            ),
        )
        await backend.web_audio(web.token, web.reference, "new", b"new", "audio/webm")
        for _ in range(100):
            state = await backend.web_state(web.token, web.reference)
            if state.barge_in and state.barge_in[0]["dispatch"] == "waiting":
                break
            await asyncio.sleep(0.01)
        self.assertEqual(stops, [])
        first.set()
        await asyncio.sleep(0.05)
        self.assertEqual(done, ["a"])
        self.assertEqual(stops, [])
        self.assertEqual(count, 1)
        second.set()
        await backend.drain()
        self.assertEqual(done, ["a", "b"])
        self.assertTrue(stops)
        self.assertTrue(all(n == 2 for n in stops))
        self.assertEqual(count, 2)


class BackchannelDecisionTests(unittest.TestCase):
    def test_phrases_variants_content_contexts_and_config(self):
        from bot_call.settings import schema, validate_config, SettingsInvalid
        from bot_call.barge_in import CandidateDecision

        mid = CandidateContext("middle")
        self.assertEqual(
            tuple(schema()["call_backchannel_phrases"]["default"]), DEFAULT_BACKCHANNEL_PHRASES
        )
        for word in (
            *DEFAULT_BACKCHANNEL_PHRASES,
            "嗯嗯",
            "恩恩",
            "对对",
            "对，对。",
            "嗯！嗯",
            "噢噢",
            "ＯＫ",
            "oK",
            "嗯 好的",
            "是的。",
            "啊啊",
        ):
            with self.subTest(word=word):
                self.assertEqual(
                    decide_candidate(word, mid), CandidateDecision("resume", "backchannel")
                )
        for text in (
            "嗯，但是我想问",
            "对了",
            "好，那你帮我",
            "不对",
            "别说了",
            "停",
            "等等",
            "嗯？",
            "啊好的",
            "知道了明白了",
            "!",
            "好+好",
            "嗯1",
        ):
            with self.subTest(content=text):
                self.assertEqual(
                    decide_candidate(text, mid), CandidateDecision("interrupt", "content")
                )
        for kind in ("question_context", "boundary", "context_unknown"):
            self.assertEqual(
                decide_candidate("对", CandidateContext(kind)), CandidateDecision("interrupt", kind)
            )
        self.assertEqual(
            decide_candidate("对", CandidateContext("middle", True)),
            CandidateDecision("interrupt", "long_speech"),
        )
        for text in ("", "  ", "\n"):
            self.assertEqual(
                decide_candidate(text, CandidateContext("boundary")),
                CandidateDecision("resume", "empty"),
            )
        self.assertEqual(
            decide_candidate("收到", CandidateContext("middle", phrases=("收到",))).reason,
            "backchannel",
        )
        self.assertEqual(
            decide_candidate("嗯", CandidateContext("middle", phrases=())).reason, "content"
        )
        from bot_call.native_host import settings_from

        self.assertEqual(
            settings_from(
                dict(
                    enabled=False,
                    access_mode="fixed_https",
                    public_origin="https://calls.example.test",
                    call_backchannel_phrases=["收到"],
                )
            )["call_backchannel_phrases"],
            ["收到"],
        )
        self.assertTrue(valid_phrases([]))
        self.assertFalse(valid_phrases(["太长的自定义词语"]))
        self.assertFalse(valid_phrases([True]))
        validate_config({"call_backchannel_phrases": []})
        with self.assertRaises(SettingsInvalid):
            validate_config({"call_backchannel_phrases": ["+", "\x00"]})

    def test_pause_context_snapshot_question_previous_final_and_unknown(self):
        store = BargeStore()
        parts = [
            dict(seq=0, state="complete", body="", playback_state="unknown"),
            dict(seq=1, body="继续介绍。", playback_state="started"),
        ]
        store._context_parts = lambda *a, **kw: parts
        data = dict(generation_id="reply:x", seq=1, remaining_ms=1500)
        self.assertEqual(store._candidate_context("x", data), "middle")
        for tail in ("你觉得好吗？", "可以吗。", "这样好不好", "你呢？", "你说呢？”", "对不对。"):
            parts[1]["body"] = tail
            self.assertEqual(store._candidate_context("x", data), "question_context")
        parts[1]["playback_state"] = "ended"
        parts.append(dict(seq=2, body="我在听。", playback_state="started"))
        self.assertEqual(store._candidate_context("x", dict(data, seq=2)), "question_context")
        parts[1]["body"] = "介绍。"
        self.assertEqual(
            store._candidate_context("x", dict(data, seq=2, remaining_ms=1000)), "boundary"
        )
        self.assertEqual(
            store._candidate_context("x", dict(data, seq=2, remaining_ms=None)), "context_unknown"
        )
        self.assertEqual(store._candidate_context("x", dict(data, remaining_ms=None)), "middle")

    def test_server_diagnostics_bounded_and_not_percentiles(self):
        from bot_call.observability import timing_row, timing_summary, CLIENT_STAGES

        for stage, maximum in (
            ("barge_context", 3),
            ("barge_reason", 9),
            ("barge_backchannels", 4096),
        ):
            self.assertNotIn(stage, CLIENT_STAGES)
            for invalid in (-1, maximum + 1, True, 1.2):
                self.assertIsNone(
                    timing_row(dict(stage=stage, sequence=0, start_ms=0, elapsed_ms=invalid))
                )
            row = dict(
                timing_row(dict(stage=stage, sequence=0, start_ms=0, elapsed_ms=maximum)),
                call_id="c",
                generation_id="capture:u",
                recorded_at=1,
            )
            found = next(s for s in timing_summary([row])["stages"] if s["stage"] == stage)
            self.assertEqual(found["latest_value"], maximum)
            self.assertNotIn("median_ms", found)
