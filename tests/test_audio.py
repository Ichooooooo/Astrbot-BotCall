"""Three whole offline audio scenarios; synthetic bytes, no codec or listener."""

import asyncio
from contextlib import closing
from dataclasses import asdict
import hashlib
import logging
from pathlib import Path
import sqlite3
import traceback
import unittest
from unittest.mock import patch

from bot_call.audio import (
    AudioError,
    AudioNotApplied,
    ClosingReceipt,
    NoTextResult,
    audio_settled,
    audio_review_prompt,
    advance_audio,
)
from bot_call.access import InvitationSecret
from bot_call.publication import Outcome
from bot_call.review import DEFAULT_REVIEW_PROMPT
from bot_call.runtime import CallRuntime, RuntimeAudio, RuntimeCallbacks
from bot_call.store import Store, NotFound, StateConflict
import bot_call.store as storage
from bot_call.web import CallWebApp
import test_web_access as web_fixture
from test_astrbot_context import binding

PRIVATE = "SYNTHETIC_AUDIO_PRIVATE_BODY_CREDENTIAL"
TEXT = " 原样🙂 e\u0301\r\n\t正文，不strip。 "
FINAL = "唯一整通回顾 " + PRIVATE


class AudioStore(web_fixture.ObservedStore):
    after_receive = None
    faults = {}
    commit_denials = []

    def receive_audio(self, *args, **kwargs):
        result = super().receive_audio(*args, **kwargs)
        if type(self).after_receive:
            type(self).after_receive()
        return result

    def _deny_commit(self, key):
        denials = [1]

        def authorizer(action, op, unused, database, trigger):
            if action == sqlite3.SQLITE_TRANSACTION and op == "COMMIT" and denials[0]:
                denials[0] -= 1
                self.commit_denials.append(key)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self._db.set_authorizer(authorizer)

    def complete_audio(self, call_id, utterance_id, text):
        mode = self.faults.pop((call_id, "complete"), None)
        if mode in {"commit", "double"}:
            if mode == "double":
                self.faults[(call_id, "finish")] = True
            self._deny_commit((call_id, "complete"))
            try:
                return super().complete_audio(call_id, utterance_id, text)
            finally:
                self._db.set_authorizer(None)
        if mode == "readback":
            self.faults[(call_id, "readback")] = True
            raise sqlite3.OperationalError(PRIVATE)
        result = super().complete_audio(call_id, utterance_id, text)
        if mode == "ack":
            raise sqlite3.OperationalError(PRIVATE)
        return result

    def get_audio_job(self, call_id, utterance_id):
        if self.faults.pop((call_id, "readback"), False):
            raise sqlite3.OperationalError(PRIVATE)
        return super().get_audio_job(call_id, utterance_id)

    def finish_audio(self, call_id, utterance_id, **kwargs):
        if self.faults.get((call_id, "finish")):
            raise sqlite3.OperationalError(PRIVATE)
        mode = (
            self.faults.pop((call_id, "no-text"), None)
            if kwargs.get("error_code") == "empty_transcription"
            else None
        )
        if mode in {"commit", "double"}:
            if mode == "double":
                self.faults[(call_id, "finish")] = True
            self._deny_commit((call_id, "no-text"))
            try:
                return super().finish_audio(call_id, utterance_id, **kwargs)
            finally:
                self._db.set_authorizer(None)
        if mode == "readback":
            self.faults[(call_id, "readback")] = True
            raise sqlite3.OperationalError(PRIVATE)
        result = super().finish_audio(call_id, utterance_id, **kwargs)
        if mode == "ack":
            raise sqlite3.OperationalError(PRIVATE)
        return result

    def retain_audio_missing(self, call_id, utterance_id, **kwargs):
        mode = self.faults.pop((call_id, "resolution"), None)
        if mode == "readback":
            self.faults[(call_id, "readback")] = True
            raise sqlite3.OperationalError(PRIVATE)
        result = super().retain_audio_missing(call_id, utterance_id, **kwargs)
        if mode == "ack":
            raise sqlite3.OperationalError(PRIVATE)
        return result

    def end_call(self, call_id, **kwargs):
        if not kwargs.get("_request_close") and self.faults.pop((call_id, "final"), False):
            self._deny_commit((call_id, "final"))
            try:
                return super().end_call(call_id, **kwargs)
            finally:
                self._db.set_authorizer(None)
        return super().end_call(call_id, **kwargs)


class AudioTests(unittest.IsolatedAsyncioTestCase):
    request = web_fixture.WebAccessTests.request
    exchange = web_fixture.WebAccessTests.exchange
    issue = web_fixture.WebAccessTests.issue
    on_business = web_fixture.WebAccessTests.on_business
    private_error = web_fixture.WebAccessTests.private_error

    async def asyncSetUp(self):
        await web_fixture.WebAccessTests.asyncSetUp(self)
        AudioStore.after_receive, AudioStore.faults, AudioStore.commit_denials = None, {}, []
        extra = patch("bot_call.runtime.Store", AudioStore)
        extra.start()
        self.addCleanup(extra.stop)
        self.audio_calls = []
        self.late_reply = self.first_reply = None

    async def asyncTearDown(self):
        await web_fixture.WebAccessTests.asyncTearDown(self)

    async def runtime(self, transcribe=None, *, path=None):
        async def reply(bound, turn):
            self.on_business()
            self.reply_ids.append(turn.material_id)
            self.replies.append((bound, turn.body))
            if turn.material_id == "live" and self.late_reply:
                self.first_reply.set()
                await self.late_reply.wait()
            if turn.material_id in self.reply_failures:
                if self.first_reply:
                    self.first_reply.set()
                raise RuntimeError(PRIVATE)
            return "late Bot body"

        async def review(bound, turns, prompt):
            self.on_business()
            self.reviews.append((bound, turns, prompt))
            return FINAL

        async def page(bound, value):
            self.on_business()
            self.pages.append((bound, value.fragment))
            return Outcome.CONFIRMED

        async def context(bound, key, body):
            self.on_business()
            self.contexts.append((bound, key, body))
            return Outcome.CONFIRMED

        async def checked(bound, job):
            self.on_business()
            self.audio_calls.append((bound.call_id, job.utterance_id, job.ordinal, job.data))
            return await transcribe(bound, job)

        result = CallRuntime(
            path or self.path,
            RuntimeCallbacks(reply, review, page, context),
            audio=RuntimeAudio(checked, max_audio_bytes=32) if transcribe else None,
            char_budget=145,
        )
        self.runtimes.append(result)
        await result.start()
        return result

    async def upload(
        self, app, cookie, reference, key, data=b"synthetic", mime="audio/webm", **kwargs
    ):
        headers = {"content-type": mime, "x-bot-utterance-id": key}
        headers.update(kwargs.pop("headers", {}))
        return await self.request(
            app, "audio", raw=data, cookie=cookie, reference=reference, headers=headers, **kwargs
        )

    async def test_received_audio_order_last_segment_close_time_and_same_final_body(self):
        first, last = asyncio.Event(), asyncio.Event()
        first_entered, last_entered = asyncio.Event(), asyncio.Event()
        self.gates += [first, last]

        async def transcribe(bound, job):
            if job.utterance_id == "a1":
                first_entered.set()
                await first.wait()
            if job.utterance_id == "a2":
                last_entered.set()
                await last.wait()
            return TEXT + job.utterance_id

        runtime = await self.runtime(transcribe)
        app = CallWebApp(runtime, origin=web_fixture.ORIGIN, max_audio_bytes=32)
        target = binding("A")
        key, _ = await self.issue(runtime, target)
        cookie, reference, _ = await self.exchange(app, key)
        self.late_reply, self.first_reply = asyncio.Event(), asyncio.Event()
        self.gates.append(self.late_reply)
        await self.request(
            app,
            "utterances",
            data={"utterance_id": "live", "text": "已收到文字"},
            cookie=cookie,
            reference=reference,
        )
        await asyncio.wait_for(self.first_reply.wait(), 2)
        self.first_reply = asyncio.Event()
        self.reply_failures.add("a1")
        holder = {}
        AudioStore.after_receive = lambda: self.loop.call_soon_threadsafe(holder["task"].cancel)
        holder["task"] = asyncio.create_task(
            self.upload(app, cookie, reference, "a1", b"first-bytes")
        )
        with self.assertRaises(asyncio.CancelledError):
            await holder["task"]
        AudioStore.after_receive = None
        await asyncio.wait_for(first_entered.wait(), 2)
        saved = await runtime.read_state("A")
        self.assertEqual(
            (saved.audio_jobs[0].state, saved.audio_jobs[0].data), ("running", b"first-bytes")
        )
        self.assertNotIn("a1", [t.material_id for t in saved.turns])
        self.assertNotIn("first-bytes", repr(saved))
        self.assertEqual(
            (await self.upload(app, cookie, reference, "a1", b"first-bytes"))[1]["audio_received"],
            True,
        )
        self.assertEqual((await self.upload(app, cookie, reference, "a1", b"changed"))[0], 409)
        self.assertEqual(
            (await self.upload(app, cookie, reference, "a1", b"first-bytes", "audio/ogg"))[0], 409
        )
        self.assertEqual(
            (
                await self.request(
                    app,
                    "utterances",
                    data={"utterance_id": "a1", "text": TEXT},
                    cookie=cookie,
                    reference=reference,
                )
            )[0],
            409,
        )
        before = len(self.audio_calls)
        self.assertEqual(
            (await self.upload(app, cookie, reference, "live", b"cannot-replace-text"))[0], 409
        )
        self.assertEqual(
            (await self.upload(app, cookie, reference, "bad", b"x", "application/octet-stream"))[0],
            415,
        )
        self.assertEqual(
            (
                await self.upload(
                    app,
                    cookie,
                    reference,
                    "big",
                    chunks=[b"a" * 20, b"b" * 20],
                    headers={"content-length": "1"},
                )
            )[0],
            413,
        )
        self.assertEqual(len(self.audio_calls), before)
        first.set()
        await asyncio.wait_for(self.first_reply.wait(), 2)

        # Callback entry precedes its durable failure commit. Hangup must not
        # race that commit when this scenario specifically expects "failed".
        async def failed_reply_committed():
            while True:
                current = await runtime.read_state("A")
                if any(
                    g.generation_id == "reply:a1" and g.state == "failed"
                    for g in current.generations
                ):
                    return
                await asyncio.sleep(0.001)

        await asyncio.wait_for(failed_reply_committed(), 2)
        self.assertEqual((await self.upload(app, cookie, reference, "a2", b"last-bytes"))[0], 200)
        await asyncio.wait_for(last_entered.wait(), 2)
        self.assertEqual((await self.upload(app, cookie, reference, "a3", b"third-bytes"))[0], 200)
        self.assertEqual([key for call, key, ordinal, data in self.audio_calls], ["a1", "a2"])
        self.now = 1100
        end = await self.request(app, "end", data={}, cookie=cookie, reference=reference)
        self.assertEqual(
            (end[0], end[1]["ended"], end[1]["close_state"]), (200, False, "waiting_for_audio")
        )
        self.assertEqual(end[1]["close_requested_at"], 1100)
        waiting = await self.request(app, "state", cookie=cookie, reference=reference)
        self.assertTrue(waiting[1]["close_requested"])
        self.assertFalse(waiting[1]["ended"])
        self.assertNotIn("data", str(waiting[1]["audio"]))
        self.assertEqual((await self.upload(app, cookie, reference, "new", b"new"))[0], 409)
        self.assertEqual(
            (
                await self.request(
                    app,
                    "utterances",
                    data={"utterance_id": "new", "text": "new"},
                    cookie=cookie,
                    reference=reference,
                )
            )[0],
            409,
        )
        # Closing releases the realtime web slot while the old final audio is still running.
        key_b, _ = await self.issue(runtime, binding("B"))
        await self.exchange(app, key_b)
        self.now = 1200
        last.set()
        self.late_reply.set()
        await runtime.drain()
        state = await runtime.read_state("A")
        self.assertEqual(state.review.ended_at, 1100)
        self.assertTrue(state.publication.complete)
        self.assertEqual(state.review.body, FINAL)
        self.assertEqual(
            {t.material_id: t.body for t in state.turns if t.role == "user"},
            {"live": "已收到文字", "a1": TEXT + "a1", "a2": TEXT + "a2", "a3": TEXT + "a3"},
        )
        self.assertFalse(any(t.role == "assistant" for t in state.turns))
        self.assertEqual(
            [g.state for g in state.generations], ["cancelled", "failed", "cancelled", "cancelled"]
        )
        self.assertEqual([key for call, key, ordinal, data in self.audio_calls], ["a1", "a2", "a3"])
        self.assertEqual(
            [j.user_sequence for j in state.audio_jobs],
            sorted(j.user_sequence for j in state.audio_jobs),
        )
        self.assertTrue(all(j.state == "succeeded" and j.data is None for j in state.audio_jobs))
        self.assertEqual(self.reply_ids, ["live", "a1"])
        self.assertEqual(len(self.reviews), 1)
        self.assertEqual("".join(text for bound, text in self.pages), FINAL)
        self.assertEqual([body for bound, key, body in self.contexts], [FINAL])
        before = list(self.audio_calls)
        self.assertEqual((await self.upload(app, cookie, reference, "a2", b"last-bytes"))[0], 200)
        await runtime.resume_pending()
        await runtime.drain()
        self.assertEqual(self.audio_calls, before)
        # Exact trusted no-text releases order, never creates a user/generation.
        no_text_release, no_text_entered = asyncio.Event(), asyncio.Event()
        self.gates.append(no_text_release)

        async def no_text_tail(bound, job):
            if job.utterance_id == "tail":
                no_text_entered.set()
                await no_text_release.wait()
            return TEXT if job.utterance_id == "text" else NoTextResult()

        empty_runtime = await self.runtime(no_text_tail, path=self.path.with_name("no-text.db"))
        app2 = CallWebApp(empty_runtime, origin=web_fixture.ORIGIN, max_audio_bytes=32)
        for name in ["zero", "pure-idle", "mixed"]:
            key, _ = await self.issue(empty_runtime, binding(name))
            cookie2, ref2, _ = await self.exchange(app2, key)
            if name != "pure-idle":
                await self.upload(app2, cookie2, ref2, "blank", b"retained-empty")
                await empty_runtime.drain()
                snap = await empty_runtime.read_state(name)
                self.assertEqual(snap.turns, ())
                self.assertEqual(snap.generations, ())
                self.assertTrue(audio_settled(snap.audio_jobs[0]))
                self.assertEqual(snap.audio_jobs[0].data, b"retained-empty")
            if name == "mixed":
                await self.upload(app2, cookie2, ref2, "text", b"recognized")
                await empty_runtime.drain()
                await self.upload(app2, cookie2, ref2, "tail", b"tail-empty")
            response = await self.request(app2, "end", data={}, cookie=cookie2, reference=ref2)
            self.assertEqual(response[0], 200)
            if name == "mixed":
                await asyncio.wait_for(no_text_entered.wait(), 2)
                self.assertEqual(response[1]["close_state"], "waiting_for_audio")
                no_text_release.set()
            await empty_runtime.drain()
            snap = await empty_runtime.read_state(name)
            if name != "mixed":
                self.assertIsNone(snap.review)
                self.assertIsNone(snap.publication)
                self.assertEqual(snap.turns, ())
                self.assertEqual(snap.generations, ())
                public = (await self.request(app2, "state", cookie=cookie2, reference=ref2))[1]
                self.assertEqual(
                    (public["ended"], public["close_state"], public["complete"]),
                    (True, "closed_no_text", False),
                )
                self.assertFalse(any(b.call_id == name for b, _, _ in self.reviews))
            else:
                self.assertTrue(snap.publication.complete)
                self.assertEqual(snap.close_request.prompt, DEFAULT_REVIEW_PROMPT)
                self.assertEqual(
                    snap.review.prompt, audio_review_prompt(DEFAULT_REVIEW_PROMPT, snap.audio_jobs)
                )
                self.assertIn("有2段未取得可识别文字", snap.review.prompt)
                self.assertEqual([t.body for t in snap.turns if t.role == "user"], [TEXT])
                saved_prompt = snap.review.prompt
                self.assertEqual((await empty_runtime.end_call(name)).prompt, saved_prompt)
                self.assertEqual(
                    (await empty_runtime.end_call(name, prompt=DEFAULT_REVIEW_PROMPT)).prompt,
                    saved_prompt,
                )
                self.assertEqual(
                    (await empty_runtime.end_call(name, prompt=saved_prompt)).prompt, saved_prompt
                )
                with self.assertRaises(Exception):
                    await empty_runtime.end_call(name, prompt="different")
        await empty_runtime.shutdown(timeout=None)
        with Store(self.path.with_name("no-text.db")) as reopened:
            review = reopened.get_review("mixed")
            self.assertEqual(review.prompt, saved_prompt)
            self.assertEqual(reopened.end_call("mixed", prompt=DEFAULT_REVIEW_PROMPT), review)
            self.assertEqual(reopened.get_close_request("mixed").prompt, DEFAULT_REVIEW_PROMPT)
            with self.assertRaises(sqlite3.IntegrityError):
                reopened._db.execute(
                    "UPDATE audio_jobs SET error_code='not_applied' WHERE call_id='mixed' AND utterance_id='blank'"
                )

    async def test_v6_migration_pending_reopen_unknown_and_legacy_cannot_freeze(self):
        with Store(self.path) as store:
            target = binding("resume")
            store.create_call(target, background=PRIVATE)
            key = InvitationSecret.create()
            store.issue_web_invitation("resume", key, session_ttl=1000)
            grant = store.exchange_web_invitation(key.value)
            for uid in ["a1", "a2"]:
                store.receive_audio("resume", uid, uid.encode(), "audio/webm")
            node, won = store.claim_next_audio("resume")
            self.assertTrue(won)
            store.complete_audio("resume", "a1", TEXT + "a1")
            close = store.end_call("resume", prompt=DEFAULT_REVIEW_PROMPT, _request_close=True)
            self.assertIsInstance(close, ClosingReceipt)
            with self.assertRaises(StateConflict):
                store.end_call("resume", prompt=DEFAULT_REVIEW_PROMPT)
            for mode in ["running", "uncertain", "failed", "cancelled"]:
                store.create_call(binding(mode))
                store.receive_audio(mode, "first", b"raw", "audio/webm")
                store.receive_audio(mode, "later", b"later", "audio/webm")
                store.claim_next_audio(mode)
                if mode != "running":
                    store.finish_audio(
                        mode,
                        "first",
                        state=mode,
                        error_code={
                            "uncertain": "result_uncertain",
                            "failed": "not_applied",
                            "cancelled": "result_cancelled",
                        }[mode],
                    )
                store.end_call(mode, prompt=DEFAULT_REVIEW_PROMPT, _request_close=True)
            # Explicit, real v6 fixture: new audio tables must never be mislabeled v6.
            store.create_call(binding("old-v6"), background=PRIVATE)
            store.append_user("old-v6", "u", PRIVATE, source="text")
            store.end_call("old-v6", prompt=PRIVATE)
            store.claim_review("old-v6")
            store.complete_review("old-v6", FINAL)
            store.prepare_publication("old-v6")
            names = [
                "calls",
                "turns",
                "generations",
                "reviews",
                "publications",
                "publication_pages",
                "review_work",
                "review_work_nodes",
                "review_work_stages",
                "review_policy_manifests",
                "web_invitations",
                "web_sessions",
            ]
            rows = {n: [tuple(r) for r in store._db.execute("SELECT * FROM " + n)] for n in names}
        legacy = Path(self.directory.name) / "v6.db"
        with closing(sqlite3.connect(legacy)) as raw:
            for statement in (
                storage._SCHEMA
                + storage._REVIEW_SCHEMA
                + storage._PUBLICATION_SCHEMA
                + storage._WORK_SCHEMA
                + storage._MANIFEST_SCHEMA
                + storage._ACCESS_SCHEMA
            ):
                raw.execute(statement)
            raw.execute(f"PRAGMA application_id={storage._APPLICATION_ID}")
            raw.execute("PRAGMA user_version=6")
            for name in names:
                for row in rows[name]:
                    raw.execute(
                        "INSERT INTO " + name + " VALUES (" + ",".join("?" for _ in row) + ")", row
                    )
            raw.commit()
            schema = raw.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall()
        with patch.object(storage, "_AUDIO_SCHEMA", storage._AUDIO_SCHEMA + ("INVALID MIGRATION",)):
            with self.assertRaises(sqlite3.Error):
                Store(legacy)
        with closing(sqlite3.connect(legacy)) as raw:
            self.assertEqual(raw.execute("PRAGMA user_version").fetchone()[0], 6)
            self.assertEqual(
                raw.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall(), schema
            )
        with Store(legacy) as upgraded:
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], 24)
            for name in names:
                self.assertEqual(
                    [tuple(r) for r in upgraded._db.execute("SELECT * FROM " + name)], rows[name]
                )
        legacy_runtime = await self.runtime()
        await legacy_runtime.resume_pending()
        await legacy_runtime.drain()
        state = await legacy_runtime.read_state("resume")
        self.assertIsNone(state.review)
        self.assertEqual([j.state for j in state.audio_jobs], ["succeeded", "pending"])
        self.assertIsInstance(await legacy_runtime.end_call("resume"), ClosingReceipt)
        app = CallWebApp(legacy_runtime, origin=web_fixture.ORIGIN)
        self.assertEqual((await self.upload(app, "cookie", "reference", "a2"))[0], 503)
        await legacy_runtime.shutdown()

        async def transcribe(bound, job):
            return TEXT + job.utterance_id

        runtime = await self.runtime(transcribe)
        self.assertEqual(self.audio_calls, [])
        await runtime.resume_pending()
        await runtime.drain()
        state = await runtime.read_state("resume")
        self.assertEqual([key for call, key, ordinal, data in self.audio_calls], ["a2"])
        self.assertEqual(state.review.ended_at, close.requested_at)
        self.assertEqual([t.body for t in state.turns], [TEXT + "a1", TEXT + "a2"])
        self.assertTrue(state.publication.complete)
        for mode in ["running", "uncertain", "failed", "cancelled"]:
            state = await runtime.read_state(mode)
            self.assertIsNone(state.review)
            self.assertIsNone(state.publication)
            self.assertEqual([j.state for j in state.audio_jobs], [mode, "pending"])
            self.assertEqual(state.audio_jobs[0].data, b"raw")
        before = list(self.audio_calls)
        await runtime.resume_pending()
        await runtime.drain()
        self.assertEqual(self.audio_calls, before)

    async def test_commit_confirmation_failures_unknown_results_and_real_callback_shutdown(self):
        notes = []
        logger = logging.getLogger("astrbot")

        def collect(fmt, *values):
            if fmt.startswith("bot_call.audio_commit "):
                notes.append(fmt % values)

        collector = patch.object(logger, "info", side_effect=collect)
        collector.start()
        self.addCleanup(collector.stop)
        modes = [
            "not-applied",
            "error",
            "empty",
            "cancel",
            "commit",
            "double",
            "ack",
            "final",
            "readback",
            "no-text",
            "no-text-ack",
            "no-text-commit",
            "no-text-double",
            "no-text-readback",
            "no-text-subclass",
        ]
        with Store(self.path) as store:
            for mode in modes:
                store.create_call(binding(mode))
                store.receive_audio(mode, "u", b"raw-" + mode.encode(), "audio/webm")
                store.end_call(mode, prompt=DEFAULT_REVIEW_PROMPT, _request_close=True)
        AudioStore.faults = {
            ("commit", "complete"): "commit",
            ("double", "complete"): "double",
            ("ack", "complete"): "ack",
            ("final", "final"): True,
            ("readback", "complete"): "readback",
        }
        AudioStore.faults.update(
            {
                ("no-text-" + mode, "no-text"): mode
                for mode in ["ack", "commit", "double", "readback"]
            }
        )

        async def transcribe(bound, job):
            if bound.call_id == "no-text-subclass":

                class UntrustedNoText(NoTextResult):
                    pass

                return UntrustedNoText()
            if bound.call_id.startswith("no-text"):
                return NoTextResult()
            if bound.call_id == "not-applied":
                raise AudioNotApplied(PRIVATE)
            if bound.call_id == "error":
                raise RuntimeError(PRIVATE)
            if bound.call_id == "empty":
                return " \n "
            if bound.call_id == "cancel":
                raise asyncio.CancelledError(PRIVATE)
            return TEXT

        runtime = await self.runtime(transcribe)
        await runtime.resume_pending()
        await runtime.drain()
        expected = {
            "not-applied": "failed",
            "error": "uncertain",
            "empty": "failed",
            "cancel": "cancelled",
            "commit": "uncertain",
            "double": "running",
            "readback": "uncertain",
            "no-text-commit": "uncertain",
            "no-text-double": "running",
            "no-text-readback": "uncertain",
            "no-text-subclass": "failed",
        }
        for mode, state in expected.items():
            saved = await runtime.read_state(mode)
            self.assertEqual(saved.audio_jobs[0].state, state)
            self.assertEqual(saved.audio_jobs[0].data, b"raw-" + mode.encode())
            self.assertIsNone(saved.review)
            self.assertEqual(saved.turns, ())
        for key in ["no-text", "no-text-ack"]:
            saved = await runtime.read_state(key)
            self.assertEqual(
                (saved.audio_jobs[0].state, saved.audio_jobs[0].error_code),
                ("failed", "empty_transcription"),
            )
            self.assertEqual(saved.turns, ())
            self.assertEqual(saved.generations, ())
            self.assertIsNone(saved.review)
        ack = await runtime.read_state("ack")
        self.assertEqual(ack.review.body, FINAL)
        self.assertIsNone(ack.audio_jobs[0].data)
        pending = await runtime.read_state("final")
        self.assertIsNone(pending.review)
        self.assertEqual(pending.turns[0].body, TEXT)
        self.assertIsNone(pending.audio_jobs[0].data)
        before = list(self.audio_calls)
        self.now = 1200
        await runtime.resume_pending()
        await runtime.drain()
        self.assertEqual(self.audio_calls, before)
        final = await runtime.read_state("final")
        self.assertEqual(final.review.body, FINAL)
        self.assertEqual(final.review.ended_at, 1000)
        self.assertEqual(
            sorted(bound.call_id for bound, turns, prompt in self.reviews), ["ack", "final"]
        )
        self.assertEqual(
            sorted(AudioStore.commit_denials),
            [
                ("commit", "complete"),
                ("double", "complete"),
                ("final", "final"),
                ("no-text-commit", "no-text"),
                ("no-text-double", "no-text"),
            ],
        )
        self.assertEqual(len(self.audio_calls), len(modes))
        self.assertEqual(self.reply_ids, [])
        self.assertTrue(any("reason=saved outcome=succeeded" in note for note in notes))
        self.assertTrue(any("reason=ack_confirmed outcome=succeeded" in note for note in notes))
        self.assertTrue(any("reason=readback_failed outcome=uncertain" in note for note in notes))
        self.assertTrue(any("reason=readback_mismatch outcome=uncertain" in note for note in notes))
        for reason, outcome in [
            ("no_text_saved", "no_text"),
            ("no_text_ack_confirmed", "no_text"),
            ("no_text_readback_failed", "uncertain"),
            ("no_text_readback_mismatch", "uncertain"),
        ]:
            self.assertTrue(any(f"reason={reason} outcome={outcome}" in note for note in notes))
        from bot_call.audio import _commit_diagnostic

        class SecretString(str):
            def __str__(self):
                return PRIVATE

        _commit_diagnostic(SecretString("saved"), SecretString("succeeded"))
        _commit_diagnostic([], {})
        self.assertTrue(all("reason=unknown outcome=unknown" in note for note in notes[-2:]))
        self.assertNotIn(PRIVATE, "\n".join(notes))
        self.assertNotIn(str(self.path), "\n".join(notes))
        # A broken diagnostic sink must not make a saved transcription uncertain.
        observed = self.path.with_name("observer.db")
        with AudioStore(observed) as store:
            store.create_call(binding("observer"))
            store.receive_audio("observer", "u", b"raw", "audio/webm")

            async def local_text(bound, job):
                return TEXT

            with patch.object(logger, "info", side_effect=RuntimeError(PRIVATE)):
                await advance_audio(store, "observer", local_text, lambda turn: None, max_steps=1)
            self.assertEqual(store.get_audio_job("observer", "u").state, "succeeded")
        web_fixture.ObservedStore.instances.remove(store)
        # The fixed module error boundary, checked directly instead of an assertion swallowed by a callback.
        direct = self.path.with_name("direct.db")
        with AudioStore(direct) as store:
            store.create_call(binding("direct"))
            store.receive_audio("direct", "u", b"raw", "audio/webm")
            AudioStore.faults[("direct", "complete")] = "commit"

            async def generated(bound, job):
                return PRIVATE

            try:
                await advance_audio(store, "direct", generated, lambda turn: None, max_steps=1)
            except AudioError as error:
                self.private_error(error)
            else:
                self.fail("unconfirmed COMMIT claimed success")
        # This standalone Store above was deliberately owned by the synthetic test thread.
        web_fixture.ObservedStore.instances.remove(store)
        entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.gates.append(release)

        async def stubborn(bound, job):
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
                return TEXT

        path = self.path.with_name("stubborn.db")
        with Store(path) as store:
            store.create_call(binding("stubborn"))
            store.receive_audio("stubborn", "u", b"keep-raw", "audio/webm")
            store.end_call("stubborn", prompt=DEFAULT_REVIEW_PROMPT, _request_close=True)
        runtime = await self.runtime(stubborn, path=path)
        await runtime.resume_pending()
        await asyncio.wait_for(entered.wait(), 2)
        report = await runtime.shutdown(timeout=0.03)
        await asyncio.wait_for(cancelled.wait(), 2)
        self.assertEqual((report.stopped, report.error_code), (False, "still_stopping"))
        self.assertTrue(runtime._thread.is_alive())
        self.assertTrue(runtime._core_tasks)
        self.assertIsNone(web_fixture.ObservedStore.instances[-1].closed_thread)
        finished = asyncio.create_task(runtime.shutdown(timeout=None))
        heartbeat = asyncio.Event()
        self.loop.call_soon(heartbeat.set)
        await heartbeat.wait()
        self.assertFalse(finished.done())
        release.set()
        self.assertTrue((await finished).stopped)
        with Store(path) as store:
            job = store.get_audio_job("stubborn", "u")
            self.assertEqual((job.state, job.data), ("cancelled", b"keep-raw"))
            self.assertEqual(store.get_transcript("stubborn"), ())
            with self.assertRaises(NotFound):
                store.get_review("stubborn")

        # Future policy only: explicit missing decisions never modify raw jobs.
        path = self.path.with_name("retained.db")
        entered, release = asyncio.Event(), asyncio.Event()
        self.gates.append(release)

        async def retained_callback(bound, job):
            if job.utterance_id == "owned-cleanup":
                # Synthetic terminal persisted while the actual callback still
                # owns cleanup; state alone MUST NOT prove task exit.
                with Store(path) as external:
                    external.finish_audio(
                        bound.call_id,
                        job.utterance_id,
                        state="uncertain",
                        error_code="result_uncertain",
                    )
                entered.set()
                await release.wait()
                raise RuntimeError(PRIVATE)
            if job.utterance_id in {"unknown", "all-missing"}:
                raise RuntimeError(PRIVATE)
            if job.utterance_id == "blank":
                return NoTextResult()
            return TEXT

        future = await self.runtime(retained_callback, path=path)
        target = binding("future")
        await future.create_call(target, retained_missing=True)
        secret = InvitationSecret.create()
        await future.issue_web_invitation(target.call_id, secret, session_ttl=3600)
        app = CallWebApp(future, origin=web_fixture.ORIGIN, max_audio_bytes=32)
        cookie, ref, _ = await self.exchange(app, secret)
        await self.upload(app, cookie, ref, "unknown", b"keep-unknown")
        await future.drain()
        raw_before = (await future.read_state("future")).audio_jobs[0]
        count = len(self.audio_calls)
        for data in (
            {"utterance_id": "unknown", "state": "succeeded"},
            {"utterance_id": True},
            {"utterance_id": "other"},
        ):
            status = (
                await self.request(app, "audio-resolution", data=data, cookie=cookie, reference=ref)
            )[0]
            self.assertIn(status, (400, 404, 409, 503))
        for kwargs in (
            {"headers": {"origin": "https://foreign.invalid"}},
            {"headers": {"host": "foreign.invalid"}},
            {"reference": "0" * 64},
        ):
            values = {"cookie": cookie, "reference": ref}
            values.update(kwargs)
            self.assertIn(
                (
                    await self.request(
                        app, "audio-resolution", data={"utterance_id": "unknown"}, **values
                    )
                )[0],
                (401, 403),
            )
        self.assertIsNone((await future.read_state("future")).audio_jobs[0].resolution)
        AudioStore.faults[("future", "resolution")] = "readback"
        self.assertEqual(
            (
                await self.request(
                    app,
                    "audio-resolution",
                    data={"utterance_id": "unknown"},
                    cookie=cookie,
                    reference=ref,
                )
            )[0],
            503,
        )
        self.assertIsNone((await future.read_state("future")).audio_jobs[0].resolution)
        AudioStore.faults[("future", "resolution")] = "ack"
        ack = await self.request(
            app, "audio-resolution", data={"utterance_id": "unknown"}, cookie=cookie, reference=ref
        )
        self.assertEqual(ack[0], 200)
        self.assertEqual(ack[1]["audio"]["resolution"]["decision"], "retained_missing")
        self.assertEqual(
            (
                await self.request(
                    app,
                    "audio-resolution",
                    data={"utterance_id": "unknown"},
                    cookie=cookie,
                    reference=ref,
                )
            )[1],
            ack[1],
        )
        self.assertEqual(len(self.audio_calls), count)
        recorded = (await future.read_state("future")).audio_jobs[0]
        from dataclasses import replace

        self.assertEqual(replace(recorded, resolution=None), raw_before)
        await self.upload(app, cookie, ref, "text", b"recognized")
        await future.drain()
        self.assertEqual(
            [t.body for t in (await future.read_state("future")).turns if t.role == "user"], [TEXT]
        )
        await self.upload(app, cookie, ref, "owned-cleanup", b"owned-original")
        await asyncio.wait_for(entered.wait(), 2)
        during = await self.request(
            app,
            "audio-resolution",
            data={"utterance_id": "owned-cleanup"},
            cookie=cookie,
            reference=ref,
        )
        self.assertEqual(during[0], 503)
        await self.upload(app, cookie, ref, "blank", b"strict-empty")
        await self.upload(app, cookie, ref, "tail", b"untried-tail")
        await self.request(app, "end", data={}, cookie=cookie, reference=ref)
        state = await future.read_state("future")
        self.assertIsNone(state.review)
        self.assertIsNone(state.audio_jobs[2].resolution)
        self.assertEqual([j.state for j in state.audio_jobs][-2:], ["pending", "pending"])
        release.set()
        await future.drain()
        state = await future.read_state("future")
        self.assertTrue(state.publication.complete)
        self.assertEqual(
            [j.state for j in state.audio_jobs],
            ["uncertain", "succeeded", "uncertain", "failed", "succeeded"],
        )
        self.assertEqual(
            [j.data for j in state.audio_jobs],
            [b"keep-unknown", None, b"owned-original", b"strict-empty", None],
        )
        self.assertEqual(state.close_request.prompt, DEFAULT_REVIEW_PROMPT)
        self.assertIn("有3段未取得可识别文字", state.review.prompt)
        self.assertEqual(state.review.body, FINAL)
        self.assertEqual(
            state.publication.body, "".join(p.fragment for p in state.publication.pages)
        )
        self.assertEqual(
            [body for bound, key, body in self.contexts if bound.call_id == "future"],
            [state.publication.body],
        )
        self.assertTrue(state.publication.body.endswith(FINAL))
        self.assertTrue(state.publication.body.startswith("材料说明："))
        public = (await self.request(app, "state", cookie=cookie, reference=ref))[1]
        self.assertEqual(public["review_body"], state.publication.body)
        self.assertEqual(
            [uid for call, uid, _, _ in self.audio_calls if call == "future"],
            ["unknown", "text", "owned-cleanup", "blank", "tail"],
        )
        secret2, _ = await self.issue(future, binding("other-origin", "qq-other", "other-cid"))
        cookie2, ref2, _ = await self.exchange(app, secret2)
        self.assertEqual(
            (
                await self.request(
                    app,
                    "audio-resolution",
                    data={"utterance_id": "unknown"},
                    cookie=cookie2,
                    reference=ref,
                )
            )[0],
            401,
        )
        self.assertNotEqual(ref2, ref)
        frozen_prompt = state.review.prompt
        self.assertEqual(
            (await future.end_call("future", prompt=DEFAULT_REVIEW_PROMPT)).prompt, frozen_prompt
        )
        await future.shutdown(timeout=None)
        with Store(path) as saved:
            self.assertTrue(saved.audio_missing_enabled("future"))
            self.assertEqual(saved.get_publication("future"), state.publication)
            note = saved._audio_note("future")
            segments = __import__("json").loads(note["manifest_json"])["segments"]
            self.assertEqual([j["ordinal"] for j in segments], [1, 3, 4])
            self.assertEqual(
                [j["decision"] for j in segments],
                ["retained_missing", "retained_missing", "empty_transcription"],
            )
            for table in ("audio_missing_policies", "audio_resolutions", "review_audio_notes"):
                with self.assertRaises(sqlite3.IntegrityError):
                    saved._db.execute("DELETE FROM " + table + " WHERE call_id='future'")
        # All missing closes honestly with zero review/page/context callbacks.
        fresh = await self.runtime(retained_callback, path=path)
        await fresh.create_call(binding("all-missing"), retained_missing=True)
        with Store(path) as seed:
            seed.receive_audio("all-missing", "all-missing", b"all-retained", "audio/webm")
        before_counts = (len(self.reviews), len(self.pages), len(self.contexts))
        await fresh.end_call("all-missing")
        await fresh.drain()
        state = await fresh.read_state("all-missing")
        self.assertIsNone(state.review)
        self.assertIsNone(state.publication)
        self.assertEqual(state.turns, ())
        self.assertTrue(audio_settled(state.audio_jobs[0]))
        self.assertEqual(before_counts, (len(self.reviews), len(self.pages), len(self.contexts)))


if __name__ == "__main__":
    unittest.main()
