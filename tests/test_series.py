"""Continuous speech over real ASGI/worker/store; synthetic audio only."""

import asyncio
import hashlib
import tempfile
from pathlib import Path
from functools import partial
from dataclasses import replace
import sqlite3
import unittest
from unittest.mock import patch

import test_audio as fixture
from test_astrbot_context import binding
from test_web_access import ORIGIN
from bot_call.audio import NoTextResult, AudioError
from bot_call.runtime import RuntimeAudio
from bot_call.store import Store
import bot_call.store as storage
from bot_call.web import CallWebApp


class SeriesTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixture.AudioTests.asyncSetUp
    asyncTearDown = fixture.AudioTests.asyncTearDown
    runtime = fixture.AudioTests.runtime
    request = fixture.AudioTests.request
    exchange = fixture.AudioTests.exchange
    issue = fixture.AudioTests.issue
    on_business = fixture.AudioTests.on_business

    async def setup_series(self, transcribe, limit=10):
        with patch.object(
            fixture, "RuntimeAudio", partial(RuntimeAudio, max_continuous_segments=limit)
        ):
            runtime = await self.runtime(transcribe)
        app = CallWebApp(runtime, origin=ORIGIN, max_audio_bytes=32)
        await runtime.create_call(binding("series"), retained_missing=True)
        secret, _ = await self.issue(runtime, binding("series"))
        cookie, ref, _ = await self.exchange(app, secret)
        app = CallWebApp(
            runtime, origin=ORIGIN, max_audio_bytes=32, call_max_continuous_segments=limit
        )
        return runtime, app, cookie, ref

    async def segment(
        self,
        app,
        cookie,
        ref,
        index,
        final=False,
        *,
        sid="s",
        key=None,
        route="audio-series",
        body=b"audio",
    ):
        headers = {
            "content-type": "audio/webm",
            "x-bot-utterance-id": key or (sid if index == 1 else f"{sid}-{index}"),
            "x-bot-series-id": sid,
            "x-bot-series-index": str(index),
            "x-bot-series-final": str(int(final)),
        }
        if route.endswith("commit"):
            headers.update(
                {
                    "x-bot-tail-offset": "0",
                    "x-bot-total-bytes": str(len(body)),
                    "x-bot-audio-sha256": hashlib.sha256(body).hexdigest(),
                }
            )
        return await self.request(
            app, route, cookie=cookie, reference=ref, raw=body, headers=headers
        )

    async def control(
        self, app, cookie, ref, action="seal", last=2, reason="no_audio_tail", sid="s"
    ):
        return await self.request(
            app,
            "audio-series-control",
            cookie=cookie,
            reference=ref,
            data=dict(series_id=sid, action=action, last_index=last, reason=reason),
        )

    async def test_complete_series_idempotent_commit_projection_and_close(self):
        runtime, app, cookie, ref = await self.setup_series(
            lambda b, j: asyncio.sleep(0, result=["我想", "请你", "解释一下"][j.series_index - 1])
        )
        self.assertEqual(
            (await self.segment(app, cookie, ref, 1, route="audio-series-commit"))[0], 200
        )
        await runtime.drain()
        self.assertEqual(self.reply_ids, [])
        self.assertEqual((await self.segment(app, cookie, ref, 1))[0], 200)
        self.assertEqual((await self.segment(app, cookie, ref, 1, True))[0], 409)
        self.assertEqual((await self.segment(app, cookie, ref, 3))[0], 409)
        self.assertEqual((await self.segment(app, cookie, ref, 2))[0], 200)
        await runtime.drain()
        self.assertEqual(self.reply_ids, [])
        self.assertEqual((await self.segment(app, cookie, ref, 3, True))[0], 200)
        await runtime.drain()
        self.assertEqual(self.reply_ids, ["s"])
        self.assertEqual(self.replies[0][1], "我想请你解释一下")
        state = await runtime.read_state("series")
        self.assertEqual(len(state.raw_turns), 4)
        self.assertEqual(len(state.turns), 2)
        self.assertEqual(
            (await self.segment(app, cookie, ref, 3, True, route="audio-series-commit"))[0], 200
        )
        self.assertEqual(len(self.audio_calls), 3)
        self.assertEqual(
            (await self.request(app, "end", cookie=cookie, reference=ref, data={}))[0], 200
        )
        await runtime.drain()
        self.assertEqual(
            [t.body for t in self.reviews[0][1] if t.role == "user"], ["我想请你解释一下"]
        )

    async def test_missing_abandon_seal_and_hangup_never_answer_prefix(self):
        async def transcribe(bound, job):
            return NoTextResult() if job.utterance_id == "s-2" else "已识别"

        runtime, app, cookie, ref = await self.setup_series(transcribe)
        await self.segment(app, cookie, ref, 1)
        await self.segment(app, cookie, ref, 2, True)
        await runtime.drain()
        self.assertEqual(self.reply_ids, [])
        self.assertEqual(
            (await self.control(app, cookie, ref, "abandon", 2, "stt_missing"))[0], 200
        )
        self.assertEqual(
            (await self.control(app, cookie, ref, "abandon", 2, "stt_missing"))[0], 200
        )
        self.assertEqual((await self.control(app, cookie, ref))[0], 409)
        await self.segment(app, cookie, ref, 1, sid="t")
        self.assertEqual((await self.control(app, cookie, ref, last=1, sid="t"))[0], 200)
        await runtime.drain()
        self.assertEqual(self.reply_ids, ["t"])
        await self.segment(app, cookie, ref, 1, sid="u")
        await runtime.drain()
        await self.request(app, "end", cookie=cookie, reference=ref, data={})
        await runtime.drain()
        self.assertEqual(self.reply_ids, ["t"])
        self.assertIn("连续表达限制", self.reviews[0][2])
        self.assertIn("缺失段序：2", self.reviews[0][2])
        self.assertEqual(len(self.audio_calls), 4)

    async def test_reload_open_series_seal_first_dispatch_limit_and_uncertain_no_retry(self):
        async def transcribe(bound, job):
            if job.series_id == "bad" and job.series_index == 2:
                raise RuntimeError("synthetic uncertainty")
            return "表达"

        runtime, app, cookie, ref = await self.setup_series(transcribe)
        await self.segment(app, cookie, ref, 1)
        await runtime.drain()
        await runtime.shutdown()
        runtime = await self.runtime(transcribe)
        app = CallWebApp(runtime, origin=ORIGIN, max_audio_bytes=32)
        await runtime.resume_pending()
        await runtime.drain()
        self.assertEqual(self.reply_ids, [])
        self.assertEqual(len(self.audio_calls), 1)
        self.assertEqual((await self.control(app, cookie, ref, last=1))[0], 200)
        await runtime.drain()
        self.assertEqual(self.reply_ids, ["s"])
        for n in range(1, 10):
            self.assertEqual((await self.segment(app, cookie, ref, n, sid="limit"))[0], 200)
        self.assertEqual((await self.segment(app, cookie, ref, 10, sid="limit"))[0], 409)
        self.assertEqual((await self.segment(app, cookie, ref, 10, True, sid="limit"))[0], 200)
        await runtime.drain()
        self.assertEqual(self.reply_ids, ["s", "limit"])
        await self.segment(app, cookie, ref, 1, sid="bad")
        await self.segment(app, cookie, ref, 2, True, sid="bad")
        await runtime.drain()
        calls = len(self.audio_calls)
        self.assertEqual((await self.segment(app, cookie, ref, 2, True, sid="bad"))[0], 200)
        self.assertEqual(
            (await self.control(app, cookie, ref, "abandon", 2, "stt_missing", sid="bad"))[0], 200
        )
        await runtime.drain()
        self.assertEqual(len(self.audio_calls), calls)
        self.assertEqual(self.reply_ids, ["s", "limit"])

    async def test_twenty_segments_header_json_and_frozen_limit(self):
        runtime, app, cookie, ref = await self.setup_series(
            lambda b, j: asyncio.sleep(0, result="片段"), 20
        )
        self.assertEqual((await self.segment(app, cookie, ref, 1))[0], 200)
        runtime._audio = replace(runtime._audio, max_continuous_segments=1)
        for n in range(2, 20):
            self.assertEqual((await self.segment(app, cookie, ref, n))[0], 200)
        self.assertEqual((await self.segment(app, cookie, ref, 20))[0], 409)
        body = b"audio"
        data = dict(
            utterance_id="s-20",
            total_bytes=len(body),
            sha256=hashlib.sha256(body).hexdigest(),
            mime="audio/webm",
            series_id="s",
            series_index=20,
            series_final=True,
        )
        # Both parsers accept two-digit indices, including JSON commit.
        self.assertEqual((await self.segment(app, cookie, ref, 20, True))[0], 200)
        self.assertEqual(
            (
                await self.request(
                    app, "audio-series-commit", cookie=cookie, reference=ref, data=data
                )
            )[0],
            200,
        )
        self.assertEqual((await self.segment(app, cookie, ref, 21, True))[0], 400)
        await runtime.drain()
        self.assertEqual(self.reply_ids, ["s"])
        state = await self.request(app, "state", cookie=cookie, reference=ref)
        self.assertEqual(state[1]["series"][0]["max_segments"], 20)
        self.assertEqual((await self.segment(app, cookie, ref, 1, sid="disabled"))[0], 409)
        self.assertEqual((await self.segment(app, cookie, ref, 1, True, sid="disabled"))[0], 200)
        await runtime.drain()
        self.assertEqual(self.reply_ids, ["s", "disabled"])


class SeriesMigrationTests(unittest.TestCase):
    def test_nonempty_schema19_upgrade_retains_all_old_rows_and_frozen_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "calls.db"
            with Store(path) as store:
                store.create_call(binding("old"))
                turn = store.append_user("old", "a", "旧文字", source="audio_transcription")
                store._db.execute(
                    "INSERT INTO audio_jobs (call_id,utterance_id,ordinal,mime,sha256,byte_count,received_at,state,error_code,user_sequence,result_sha256,data) VALUES (?,?,?,?,?,?,?,'succeeded',NULL,?,?,NULL)",
                    (
                        "old",
                        "a",
                        1,
                        "audio/webm",
                        hashlib.sha256(b"audio").hexdigest(),
                        5,
                        1000,
                        turn.sequence,
                        hashlib.sha256(turn.body.encode()).hexdigest(),
                    ),
                )
                job = store.end_call("old", prompt="原回顾")
                old_hash = job.input_sha256
                tables = [
                    r[0]
                    for r in store._db.execute("SELECT name FROM sqlite_master WHERE type='table'")
                ]
                counts = {
                    t: store._db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables
                }
                from schema_fixture import remove_series_columns

                remove_series_columns(store._db)
                from schema_fixture import remove_retry_schema

                remove_retry_schema(store._db)
                store._db.execute("PRAGMA user_version=19")
            with Store(path) as store:
                self.assertEqual(store._db.execute("PRAGMA user_version").fetchone()[0], 24)
                self.assertEqual(store.get_review("old").input_sha256, old_hash)
                self.assertEqual(
                    {
                        t: store._db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                        for t in tables
                    },
                    counts,
                )
                self.assertEqual(
                    store._db.execute(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
                    ).fetchone()[0],
                    len(tables),
                )
                self.assertEqual(store.get_audio_job("old", "a").series_id, None)
                self.assertEqual(store._db.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_schema20_upgrade_changes_only_constraint_and_preserves_frozen_six(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "calls.db"
            with Store(path) as store:
                store.create_call(binding("old20"))
                store.receive_audio(
                    "old20", "old", b"audio", "audio/webm", series=("old", 1, False), series_limit=6
                )
                rows = [tuple(r) for r in store._db.execute("SELECT * FROM audio_jobs")]
                store._db.execute("DROP TRIGGER checked_series_insert")
                store._db.execute(storage._SERIES_SCHEMA[1])
                from schema_fixture import remove_retry_schema

                remove_retry_schema(store._db)
                store._db.execute("PRAGMA user_version=20")
            with Store(path) as store:
                self.assertEqual(store._db.execute("PRAGMA user_version").fetchone()[0], 24)
                self.assertEqual(
                    [tuple(r) for r in store._db.execute("SELECT * FROM audio_jobs")], rows
                )
                self.assertEqual(store.get_audio_job("old20", "old").series_limit, 6)
                for n in range(2, 7):
                    store.receive_audio(
                        "old20",
                        f"old-{n}",
                        b"audio",
                        "audio/webm",
                        series=("old", n, n == 6),
                        series_limit=20,
                    )
                with self.assertRaises(AudioError):
                    store.receive_audio(
                        "old20",
                        "old-7",
                        b"audio",
                        "audio/webm",
                        series=("old", 7, True),
                        series_limit=20,
                    )
                # Exercise the updated SQL trigger independently of Python validation.
                store.create_call(binding("new21"))
                for n in range(1, 21):
                    store.receive_audio(
                        "new21",
                        "new" if n == 1 else f"new-{n}",
                        b"audio",
                        "audio/webm",
                        series=("new", n, n == 20),
                        series_limit=20,
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    store._db.execute(
                        "UPDATE audio_jobs SET series_limit=6 WHERE call_id='new21' AND utterance_id='new'"
                    )
                self.assertEqual(store._db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
