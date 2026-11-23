"""Real native streaming Runner, synthetic audio and service-free Providers."""

import asyncio
from io import BytesIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock
import wave
from bot_call.access import AccessError, InvitationSecret
from bot_call.segment_runtime import SegmentRuntime
from bot_call.segments import Sentences
from bot_call.store import Store
from test_astrbot_context import binding
from test_backend import NativeCase, PublicProvider, response, WAV, MP3


class StreamingProvider(PublicProvider):
    gate = None
    fail = False

    async def text_chat_stream(self, **kwargs):
        self.calls.append({"stream": True})
        yield response("我先查一下。", chunk=True)
        if self.gate:
            await self.gate.wait()
        if self.fail:
            raise TimeoutError("SYNTHETIC_PRIVATE_ERROR")
        yield response("<think>隐藏分析</think>这是第二句话。", chunk=True)
        yield response("我先查一下。这是第二句话。")


class SentenceTests(unittest.TestCase):
    def test_boundaries_short_tail_split_thinking_and_protocol(self):
        b = Sentences()
        self.assertEqual(b.feed("嗯。"), [])
        self.assertEqual(
            b.feed("好的！接下来；下面一句\n"), ["嗯。好的！", "接下来；", "下面一句\n"]
        )
        self.assertEqual(b.feed("甲" * 65), ["甲" * 60])
        self.assertEqual(b.feed(final=True), ["甲" * 5])
        b = Sentences()
        self.assertEqual(b.feed("<thi"), [])
        self.assertEqual(b.feed("nk>隐私。内容！</think>公开文字。"), ["公开文字。"])
        self.assertEqual(b.feed("<tool_call>不朗读。", final=True), [])

    def test_only_first_chunk_uses_comma_or_enumeration_after_six_characters(self):
        for punctuation in "，、,；;":
            b = Sentences()
            self.assertEqual(
                b.feed("<think>隐藏内容很长，不能朗读。</think>" + "甲" * 5 + punctuation), []
            )
            self.assertEqual(
                b.feed("乙" + punctuation), ["甲" * 5 + punctuation + "乙" + punctuation]
            )
            later = b.feed("丙" * 6 + punctuation)
            if punctuation == "；":  # Subsequent full-width semicolons retain the original rule.
                self.assertEqual(later, ["丙" * 6 + punctuation])
                self.assertEqual(b.feed("后续完整句。"), ["后续完整句。"])
            else:
                self.assertEqual(later, [])
                self.assertEqual(b.feed("后续完整句。"), ["丙" * 6 + punctuation + "后续完整句。"])
            self.assertEqual(b.feed(final=True), [])
        b = Sentences()
        self.assertEqual(b.feed("甲" * 6), [])
        self.assertEqual(b.feed("，"), ["甲" * 6 + "，"])
        self.assertEqual(b.feed("乙" * 61), ["乙" * 60])
        self.assertEqual(b.feed(final=True), ["乙"])

    def test_first_pause_changes_only_boundaries_and_preserves_public_material(self):
        for mark in "，、,；;":
            text = "先说个结论吧" + mark + "后面的内容保持原样，仍然合成完整的句子。"
            splitter, parts = Sentences(), []
            for char in text:
                parts.extend(splitter.feed(char))
            parts.extend(splitter.feed(final=True))
            self.assertEqual(parts[0], "先说个结论吧" + mark)
            self.assertEqual("".join(parts), text)
        splitter = Sentences()
        self.assertEqual(splitter.feed("嗯，嗯，嗯，嗯，"), [])
        self.assertEqual(splitter.feed("这句话没有逗号。"), ["嗯，嗯，嗯，嗯，这句话没有逗号。"])

    def test_internal_notes_before_plaintext_across_every_stream_boundary(self):
        notes = (
            "朗读被打断，仅部分播放；第4句播放到约 5268 ms，"
            "具体已听到哪些词未知；未播出的旧叙述不作为用户已知前提。"
            "工具事实另行保留。仅表示正在听，不代表同意或工具授权。"
        )
        safe = "我刚才被你打断了。"
        for text in (notes, "[" + notes + "]", "【内部控制元信息】"):
            for width in (1, 2, 7, len(text)):
                splitter, parts = Sentences(), []
                for offset in range(0, len(text), width):
                    parts.extend(splitter.feed(text[offset : offset + width]))
                parts.extend(splitter.feed(safe, final=True))
                self.assertEqual("".join(parts), safe)
                self.assertGreater(splitter.notes.dropped, 0)
        for padding in range(45, 61):
            splitter = Sentences()
            parts = splitter.feed("甲" * padding + "朗读被打断。", final=True)
            self.assertNotIn("朗读", "".join(parts))
            self.assertGreater(splitter.notes.dropped, 0)
        for text in (
            safe,
            "[这句话是正常口语]",
            "播放到这里就可以了。",
            "我只听到了一部分。",
            "[网站](https://example.org)可以查看。",
        ):
            self.assertEqual(
                "".join(Sentences().feed(text, final=True)),
                (
                    "网站可以查看。"
                    if text.startswith("[网站]")
                    else "这句话是正常口语"
                    if text.startswith("[这句话")
                    else text
                ),
            )

    def test_schema18_upgrade_preserves_all_old_rows_and_definitions(self):
        with TemporaryDirectory() as d:
            path = Path(d) / "old.db"
            with Store(path) as s:
                s.create_call(binding("old"))
                s.append_user("old", "u", "旧材料", source="synthetic")
                s._db.execute("DROP TABLE speech_segments")
                from bot_call.retention import audio_progress_trigger
                from schema_fixture import remove_series_columns

                s._db.execute("DROP TRIGGER audio_progress")
                s._db.execute(audio_progress_trigger())

                remove_series_columns(s._db)
                from schema_fixture import remove_retry_schema

                remove_retry_schema(s._db)
                s._db.execute("PRAGMA user_version=18")
                sql = [
                    tuple(r)
                    for r in s._db.execute("SELECT name,sql FROM sqlite_master ORDER BY name")
                ]
                turns = [tuple(r) for r in s._db.execute("SELECT * FROM turns")]
            with Store(path) as s:
                self.assertEqual(s._db.execute("PRAGMA user_version").fetchone()[0], 24)
                self.assertEqual([tuple(r) for r in s._db.execute("SELECT * FROM turns")], turns)
                after = {
                    r["name"]: r["sql"] for r in s._db.execute("SELECT name,sql FROM sqlite_master")
                }
                for name, ddl in sql:
                    if name == "audio_progress":
                        self.assertEqual(after[name], audio_progress_trigger(retention=True))
                    elif name != "audio_jobs":
                        self.assertEqual(after[name], ddl, name)


class SegmentQueryTests(unittest.IsolatedAsyncioTestCase):
    call_id = "call"
    gid = "reply:long"

    def setUp(self):
        path = Path(self.enterContext(TemporaryDirectory())) / "calls.db"
        self.store = self.enterContext(Store(path))
        self.store.create_call(binding(self.call_id))
        self.store.append_user(self.call_id, "long", "请详细说明。", source="synthetic")
        self.store.claim_generation(self.call_id, self.gid, utterance_id="long", source="synthetic")
        secret = InvitationSecret.create()
        self.store.issue_web_invitation(self.call_id, secret, session_ttl=3600)
        session = self.store.exchange_web_invitation(secret.value)
        self.authority = (session.token, session.reference)
        self.bodies = []
        self.audio = {}
        for seq in range(1, 7):
            body = f"这是第 {seq} 句。"
            self.assertEqual(self.store.add_segment(self.call_id, self.gid, body), seq)
            self.bodies.append(body)
            buffer = BytesIO()
            with wave.open(buffer, "wb") as audio:
                audio.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
                audio.writeframes(seq.to_bytes(2, "little") * 65536)
            self.audio[seq] = buffer.getvalue()
        self.segment_selects = []

        def trace(sql):
            if sql.startswith("SELECT ") and " FROM speech_segments " in sql:
                self.segment_selects.append(sql)

        self.store._db.set_trace_callback(trace)
        self.addCleanup(self.store._db.set_trace_callback, None)

    def fill_audio(self):
        for seq, data in self.audio.items():
            self.store._db.execute(
                "UPDATE speech_segments SET state='running' "
                "WHERE call_id=? AND generation_id=? AND seq=?",
                (self.call_id, self.gid, seq),
            )
            self.store.finish_segment(self.call_id, self.gid, seq, "succeeded", data)

    def assert_segment_selects_without_audio(self):
        self.assertTrue(self.segment_selects)
        for sql in self.segment_selects:
            columns = sql.removeprefix("SELECT ").split(" FROM ", 1)[0].split(",")
            self.assertNotIn("*", columns, sql)
            self.assertNotIn("data", columns, sql)

    def test_audio_selects_only_the_group_state_and_requested_blob(self):
        self.fill_audio()
        self.segment_selects.clear()
        self.assertEqual(
            self.store.segment_audio(self.call_id, self.gid, 4, self.authority),
            self.audio[4],
        )
        self.assertEqual(
            self.segment_selects,
            [
                "SELECT state FROM speech_segments "
                "WHERE call_id='call' AND generation_id='reply:long' AND seq=0",
                "SELECT data FROM speech_segments "
                "WHERE call_id='call' AND generation_id='reply:long' "
                "AND seq=4 AND seq>0 AND state='succeeded'",
            ],
        )

        # Leave audio on the rejected rows so state and NULL handling are checked separately.
        for seq, state in enumerate(("pending", "cancelled", "failed"), 1):
            self.store._db.execute(
                "UPDATE speech_segments SET state=? WHERE call_id=? AND generation_id=? AND seq=?",
                (state, self.call_id, self.gid, seq),
            )
        self.store._db.execute(
            "UPDATE speech_segments SET data=NULL WHERE call_id=? AND generation_id=? AND seq=5",
            (self.call_id, self.gid),
        )
        for gid, seq in [(self.gid, n) for n in (0, -1, 99, 1, 2, 3, 5)] + [("missing", 4)]:
            with self.subTest(gid=gid, seq=seq), self.assertRaises(AccessError) as raised:
                self.store.segment_audio(self.call_id, gid, seq, self.authority)
            self.assertEqual(raised.exception.code, "conflict")

        self.segment_selects.clear()
        for call_id, authority in (
            (self.call_id, (self.authority[0], "0" * 64)),
            ("other-call", self.authority),
        ):
            with self.assertRaises(AccessError) as raised:
                self.store.segment_audio(call_id, self.gid, 4, authority)
            self.assertEqual(raised.exception.code, "denied")
        self.assertEqual(self.segment_selects, [])

        self.store.segment_playback(self.call_id, self.gid, 4, 1, "ended", self.authority)
        self.store.stop_segments(self.call_id, self.gid)
        self.segment_selects.clear()
        with self.assertRaises(AccessError) as raised:
            self.store.segment_audio(self.call_id, self.gid, 4, self.authority)
        self.assertEqual(raised.exception.code, "conflict")
        self.assertEqual(len(self.segment_selects), 1)
        self.assert_segment_selects_without_audio()

        self.store.end_call(self.call_id, prompt="回顾", _request_close=True)
        self.segment_selects.clear()
        with self.assertRaises(AccessError) as raised:
            self.store.segment_audio(self.call_id, self.gid, 4, self.authority)
        self.assertEqual(raised.exception.code, "denied")
        self.assertEqual(self.segment_selects, [])

    async def test_synthesis_advances_by_sequence_without_selecting_audio(self):
        self.assertEqual(
            self.store.next_pending_segment(self.call_id, self.gid),
            {"seq": 1, "body": self.bodies[0]},
        )
        runtime = SegmentRuntime()
        runtime._store = self.store
        runtime._worker_stopping = False
        runtime._segment_started = {}
        runtime._segment_tasks = {}
        runtime._notify_state = Mock()
        synthesized = []

        async def bridge(operation, binding, turn, guard):
            self.assertEqual(operation, "synthesize")
            self.assertEqual(binding.call_id, self.call_id)
            self.assertTrue(self.store.segment_open(self.call_id, self.gid, turn.segment_seq))
            synthesized.append((turn.segment_seq, turn.body))
            return self.audio[turn.segment_seq]

        runtime._bridge = bridge
        self.segment_selects.clear()
        await runtime._segment_synthesize(self.call_id, self.gid)
        self.assertEqual(synthesized, list(enumerate(self.bodies, 1)))
        self.assert_segment_selects_without_audio()
        self.assertEqual(
            [sql for sql in self.segment_selects if "state='pending'" in sql],
            [
                "SELECT seq,body FROM speech_segments "
                "WHERE call_id='call' AND generation_id='reply:long' "
                "AND seq>0 AND state='pending' ORDER BY seq LIMIT 1"
            ]
            * (len(self.bodies) + 1),
        )
        self.assertIsNone(self.store.next_pending_segment(self.call_id, self.gid))
        for seq, data in self.audio.items():
            self.assertEqual(
                self.store.segment_audio(self.call_id, self.gid, seq, self.authority), data
            )

    def test_material_review_and_playback_projection_keep_text_without_selecting_audio(self):
        self.fill_audio()
        self.store.stream_done(self.call_id, self.gid)
        turn = self.store.complete_generation(self.call_id, self.gid, "".join(self.bodies))
        self.store.segment_playback(self.call_id, self.gid, 1, 1, "ended", self.authority)
        self.store.segment_playback(self.call_id, self.gid, 2, 1, "started", self.authority)
        candidate = dict(
            generation_id=self.gid, utterance_id="interrupt", seq=2, position_ms=137, started=1
        )
        for action in ("begin", "long_speech"):
            self.store.barge_command(self.call_id, dict(candidate, action=action), self.authority)

        self.segment_selects.clear()
        parts = self.store.segments(self.call_id, self.gid, material=True)
        self.assert_segment_selects_without_audio()
        self.assertEqual([row["body"] for row in parts], ["", *self.bodies])
        self.assertEqual(
            [(row["playback_state"], row["playback_position_ms"]) for row in parts[1:3]],
            [("ended", None), ("stopped", 137)],
        )
        for row in parts:
            self.assertEqual(
                set(row),
                {
                    "call_id",
                    "generation_id",
                    "seq",
                    "body",
                    "state",
                    "playback_state",
                    "revision",
                    "created_at",
                    "ready_at",
                    "play_started_at",
                    "playback_position_ms",
                    "end_reason",
                },
            )

        self.segment_selects.clear()
        review = self.store.segment_review(self.call_id, "回顾")
        self.assert_segment_selects_without_audio()
        observation = json.loads(review.split("：", 1)[1])
        self.assertEqual(observation["replies"][0]["playback"], "播了一部分（浏览器观测）")
        notes = observation["segments"]
        self.assertEqual([note["text"] for note in notes], self.bodies)
        self.assertEqual(notes[0]["playback"], "已播放（浏览器报告结束）")
        self.assertEqual((notes[1]["position_ms"], notes[1]["end_reason"]), (137, "long_speech"))
        self.assertIn("具体已听到哪些词未知", notes[1]["playback"])
        self.assertEqual(notes[2]["playback"], "未播放／未确认")
        self.assertTrue(all("data" not in note for note in notes))

        self.segment_selects.clear()
        (projected,) = self.store.playback_projection(self.call_id, (turn,))
        self.assert_segment_selects_without_audio()
        self.assertEqual(projected.body, self.bodies[0])
        self.assertEqual(
            projected.sequence, self.store.get_generation(self.call_id, self.gid).user_sequence
        )


class StreamTests(NativeCase):
    async def test_stream_internal_notes_never_enter_segments_or_turn_material(self):
        class LeakingProvider(StreamingProvider):
            async def text_chat_stream(self, **kwargs):
                text = (
                    "我刚才被你打断了。"
                    "[朗读被打断，仅部分播放；第4句播放到约 5268 ms，"
                    "具体已听到哪些词未知；未播出的旧叙述不作为用户已知前提。"
                    "工具事实另行保留。]现在继续说。"
                )
                for char in text:
                    yield response(char, chunk=True)
                yield response(text)

        event, _ = self.environment(provider_class=LeakingProvider)
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "leak", "测试")
        await backend.drain()
        state = await backend.read_state(handle)
        turn = next(t for t in state.raw_turns if t.material_id == "reply:leak")
        self.assertEqual(turn.body, "我刚才被你打断了。现在继续说。")
        with Store(self.path / "calls.db") as store:
            parts = store.segments(state.binding.call_id, "reply:leak", material=True)
            self.assertEqual("".join(p["body"] for p in parts), turn.body)
            metric = store._db.execute(
                "SELECT elapsed_ms FROM turn_timings WHERE call_id=? AND stage='internal_note_dropped'",
                (state.binding.call_id,),
            ).fetchone()
            self.assertGreater(metric[0], 0)

    async def test_failed_first_receipt_preserves_native_following_sentences(self):
        event, _ = self.environment(provider_class=StreamingProvider)
        backend, _ = await self.backend()
        _, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "failure", "测试")
        await backend.drain()
        gid = "reply:failure"
        for revision, state in ((1, "started"), (2, "failed")):
            await backend.web_segment(
                web.token, web.reference, gid, 1, dict(revision=revision, state=state)
            )
        rows = [
            r
            for r in (await backend.web_state(web.token, web.reference)).segments
            if r["generation_id"] == gid
        ]
        self.assertNotEqual(rows[0]["state"], "stopped")
        self.assertEqual(rows[1]["playback_state"], "failed")
        self.assertEqual((rows[2]["state"], rows[2]["playback_state"]), ("succeeded", "unknown"))
        self.assertTrue(await backend.web_segment(web.token, web.reference, gid, 2))
        await backend.web_segment(web.token, web.reference, gid, 2, dict(revision=1, state="ended"))

    async def test_first_audio_before_model_end_order_material_playback_and_close(self):
        event, p = self.environment(provider_class=StreamingProvider)
        p.gate = asyncio.Event()
        self.gates.append(p.gate)
        backend, _ = await self.backend()
        h, w = await self.enter(backend, event)
        await backend.web_submit(w.token, w.reference, "u", "测试")
        for _ in range(100):
            s = await backend.web_state(w.token, w.reference)
            if any(r["seq"] == 1 and r["state"] == "succeeded" for r in s.segments):
                break
            await asyncio.sleep(0.005)
        self.assertTrue(any(r["seq"] == 1 and r["state"] == "succeeded" for r in s.segments))
        self.assertEqual(s.replies[0].state, "running")
        first = next(r for r in s.to_dict()["segments"] if r["seq"] == 1)
        self.assertEqual(first["text"], "我先查一下。")
        self.assertNotIn("data", first)
        self.assertNotIn("call_id", s.to_dict())
        self.assertEqual(await backend.web_segment(w.token, w.reference, "reply:u", 1), MP3)
        await backend.web_segment(
            w.token, w.reference, "reply:u", 1, dict(revision=1, state="started")
        )
        await backend.web_segment(
            w.token, w.reference, "reply:u", 1, dict(revision=2, state="ended")
        )
        p.gate.set()
        await backend.drain()
        state = await backend.read_state(h)
        self.assertEqual(state.turns[-1].body, "我先查一下。这是第二句话。")
        self.assertEqual(
            self.context.tts[event.unified_msg_origin].texts, ["我先查一下。", "这是第二句话。"]
        )
        await backend.web_end(w.token, w.reference)
        await backend.drain()
        state = await backend.read_state(h)
        self.assertIn("已播放", state.review.prompt)
        self.assertIn("未播放", state.review.prompt)
        self.assertIn("这是第二句话", state.review.prompt)
        with self.assertRaises(Exception):
            await backend.web_segment(w.token, w.reference, "reply:u", 2)

    async def test_stop_and_late_model_do_not_synthesize_more_or_change_material(self):
        event, p = self.environment(provider_class=StreamingProvider)
        p.gate = asyncio.Event()
        self.gates.append(p.gate)
        backend, _ = await self.backend()
        h, w = await self.enter(backend, event)
        await backend.web_submit(w.token, w.reference, "u", "测试")
        for _ in range(100):
            s = await backend.web_state(w.token, w.reference)
            if any(r["seq"] == 1 and r["state"] == "succeeded" for r in s.segments):
                break
            await asyncio.sleep(0.005)
        await backend.web_segment(
            w.token, w.reference, "reply:u", 1, dict(revision=1, state="stopped")
        )
        p.gate.set()
        await backend.drain()
        self.assertEqual(self.context.tts[event.unified_msg_origin].texts, ["我先查一下。"])
        self.assertEqual((await backend.read_state(h)).turns[-1].body, "我先查一下。")
        self.assertEqual((await backend.read_state(h)).turns[-1].playback_state, "stopped")
        await backend.web_end(w.token, w.reference)
        await backend.drain()
        self.assertIn("没播／未收到开始播放观测", (await backend.read_state(h)).review.prompt)
        with self.assertRaises(Exception):
            await backend.web_segment(w.token, w.reference, "reply:u", 1)

    async def test_tool_prelude_and_failed_stream_keep_only_public_submitted_material(self):
        from types import SimpleNamespace as NS

        class ToolStream(PublicProvider):
            async def text_chat_stream(self, **kwargs):
                self.calls.append(
                    {
                        "stream": True,
                        "contexts": [
                            m if isinstance(m, dict) else m.model_dump(mode="json")
                            for m in (kwargs.get("contexts") or [])
                        ],
                    }
                )
                if len(self.calls) == 1:
                    yield response("我先查一下。", chunk=True)
                    yield response(
                        "我先查一下。", role="tool", names=["lookup"], args=[{}], ids=["t"]
                    )
                else:
                    yield response("查到这些资料。", chunk=True)
                    yield response("查到这些资料。")

        event, p = self.environment(provider_class=ToolStream)

        async def lookup(event):
            return "synthetic result"

        env = self.agent_env
        tool = env["FunctionTool"](
            name="lookup",
            description="lookup",
            parameters={"type": "object", "properties": {}},
            handler=lookup,
        )
        self.context.get_llm_tool_manager = lambda: NS(
            get_full_tool_set=lambda: env["ToolSet"]([tool]),
            get_func=lambda n: tool if n == "lookup" else None,
            get_builtin_tool=lambda *a: None,
        )
        backend, _ = await self.backend()
        h, w = await self.enter(backend, event)
        await backend.web_submit(w.token, w.reference, "tool", "测试")
        await backend.drain()
        self.assertEqual((await backend.read_state(h)).turns[-1].body, "我先查一下。查到这些资料。")
        self.assertEqual(
            self.context.tts[event.unified_msg_origin].texts, ["我先查一下。", "查到这些资料。"]
        )
        self.assertTrue(any(m["role"] == "tool" for m in p.calls[1]["contexts"]))
        self.assertIn("synthetic result", json.dumps(p.calls[1]["contexts"]))
        event, p = self.environment("qq-B", "B", StreamingProvider)
        p.fail = True
        backend, _ = await self.backend(name="failed.db")
        h, w = await self.enter(backend, event)
        await backend.web_submit(w.token, w.reference, "err", "测试")
        await backend.drain()
        self.assertEqual(len(p.calls), 1)
        state = await backend.read_state(h)
        self.assertEqual(state.generations[0].state, "failed")
        self.assertEqual(self.context.tts[event.unified_msg_origin].texts, ["我先查一下。"])
        await backend.web_end(w.token, w.reference)
        await backend.drain()
        self.assertIn("我先查一下", (await backend.read_state(h)).review.prompt)
        self.assertNotIn("SYNTHETIC_PRIVATE_ERROR", (await backend.read_state(h)).review.prompt)
