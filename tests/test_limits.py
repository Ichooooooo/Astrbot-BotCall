"""Two finite deadline lifecycles. Synthetic time, real worker/SQLite, no service."""

import asyncio
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import threading
import traceback
import unittest
from unittest.mock import patch

from bot_call.access import AccessError, InvitationSecret
from bot_call.audio import ClosingReceipt
from bot_call.limits import RuntimeLimits, LimitsError
from bot_call.publication import Outcome
from bot_call.review import DEFAULT_REVIEW_PROMPT, build_review_prompt, end_call
from bot_call.runtime import CallRuntime, RuntimeAudio, RuntimeCallbacks
from bot_call.store import Store
from bot_call.web import CallWebApp
import bot_call.store as storage
import test_web_access as web_fixture
from test_astrbot_context import binding

PRIVATE = "SYNTHETIC_LIMITS_PRIVATE_BODY"


class Wakeups:
    def __init__(self):
        self.handles = []
        self.threads = set()

    def __call__(self, delay, callback):
        self.threads.add(threading.get_ident())

        class Handle:
            def __init__(self):
                self.cancelled = False
                self.fired = False
                self.loop = asyncio.get_running_loop()

            def cancel(self):
                self.cancelled = True

            def fire(self):
                def invoke():
                    if not self.cancelled:
                        self.fired = True
                        callback()

                self.loop.call_soon_threadsafe(invoke)

        handle = Handle()
        handle.delay = delay
        self.handles.append(handle)
        return handle

    def active(self):
        return [h for h in self.handles if not h.cancelled and not h.fired]

    def fire(self):
        active = self.active()
        if len(active) != 1:
            raise AssertionError("expected one deadline owner")
        active[0].fire()


class LimitsTests(unittest.IsolatedAsyncioTestCase):
    request = web_fixture.WebAccessTests.request
    exchange = web_fixture.WebAccessTests.exchange

    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "limits.db"
        self.now = 1000.0
        self.loop = asyncio.get_running_loop()
        self.thread = threading.get_ident()
        self.runtimes = []
        self.gates = []
        self.reviews = []
        self.audio_calls = []
        self.pages = []
        self.imports = []
        self.violations = []
        self.unhandled = []
        self.old_handler = self.loop.get_exception_handler()
        self.loop.set_exception_handler(lambda loop, context: self.unhandled.append(context))
        web_fixture.ObservedStore.instances = []
        for patcher in [
            patch("bot_call.store.time", lambda: self.now),
            patch("bot_call.runtime.Store", web_fixture.ObservedStore),
        ]:
            patcher.start()
            self.addCleanup(patcher.stop)

    async def asyncTearDown(self):
        for gate in self.gates:
            gate.set()
        for runtime in self.runtimes:
            self.assertTrue((await runtime.shutdown(timeout=None)).stopped)
        for store in web_fixture.ObservedStore.instances:
            self.assertEqual(store.threads, {store.owner})
            self.assertNotEqual(store.owner, self.thread)
            self.assertEqual(store.closed_thread, store.owner)
        self.loop.set_exception_handler(self.old_handler)
        self.assertEqual(self.unhandled, [])
        self.assertEqual(self.violations, [])

    def business(self):
        if threading.get_ident() != self.thread or asyncio.get_running_loop() is not self.loop:
            self.violations.append("wrong loop")

    def safe(self, error):
        self.assertNotIn(PRIVATE, "".join(traceback.format_exception(error)))

    async def runtime(
        self, *, limits=RuntimeLimits(), path=None, audio=None, review_prompt=DEFAULT_REVIEW_PROMPT
    ):
        wakeups = Wakeups()

        async def reply(bound, turn):
            self.business()
            return "Bot suggestion"

        async def review(bound, turns, prompt):
            self.business()
            self.reviews.append((bound.call_id, turns, prompt))
            return "唯一回顾 " + bound.call_id

        async def page(bound, value):
            self.business()
            self.pages.append((bound.call_id, value.fragment))
            return Outcome.CONFIRMED

        async def readable(bound, key, body):
            self.business()
            self.imports.append((bound.call_id, body))
            return Outcome.CONFIRMED

        async def transcribe(bound, job):
            self.business()
            self.audio_calls.append((bound.call_id, job.utterance_id))
            return await audio(bound, job)

        result = CallRuntime(
            path or self.path,
            RuntimeCallbacks(reply, review, page, readable),
            limits=limits,
            audio=RuntimeAudio(transcribe) if audio else None,
            _limit_timer=wakeups,
            review_prompt=review_prompt,
        )
        self.runtimes.append(result)
        await result.start()
        return result, wakeups

    async def enter(self, runtime, name, *, ttl=10000, secret=None):
        if secret is None:
            await runtime.create_call(binding(name, "qq-" + name, "cid-" + name))
            secret = InvitationSecret.create()
            await runtime.issue_web_invitation(name, secret, session_ttl=ttl)
        grant = await runtime.exchange_web_invitation(secret.value)
        return grant, secret

    async def test_default_duration_warning_reconnect_and_deadline_close_are_persistent(self):
        legacy = (
            "根据本次完整通话文字生成详细客观回顾，覆盖开头、中段与结尾。"
            "区分用户表达、Bot建议、用户决定与未定事项，保留犹豫和不确定性。"
            "播放观测不等于用户确实听到或接受；不得把历史背景冒充本次发言。"
        )
        for blank in ("", " \t\n", "\u3000"):
            self.assertEqual(build_review_prompt(blank), legacy)
        self.assertEqual(DEFAULT_REVIEW_PROMPT, legacy)
        bounded = build_review_prompt(bounded_output=True)
        self.assertTrue(bounded.startswith(legacy))
        self.assertEqual(bounded.count("bot-call-review-bounded-output-v1"), 1)
        for blank in ("", " \t\n", "\u3000"):
            self.assertEqual(build_review_prompt(blank, bounded_output=True), bounded)
            self.assertEqual(build_review_prompt(blank, bounded_output=False), legacy)
        self.assertTrue(build_review_prompt("🙂" * 2000, bounded_output=True).endswith("🙂" * 2000))
        for invalid_flag in (None, 0, 1, "true", [], {}):
            with self.assertRaises(ValueError) as error:
                build_review_prompt(bounded_output=invalid_flag)
            self.assertEqual(str(error.exception), "review.invalid_instructions")
        self.assertTrue(build_review_prompt("🙂" * 2000).endswith("🙂" * 2000))
        for invalid in (None, True, 42, [], {}, " " * 2001, "🙂" * 2001, "\ud800"):
            with self.assertRaises(ValueError) as error:
                build_review_prompt(invalid)
            self.assertEqual(str(error.exception), "review.invalid_instructions")
            with self.assertRaises(ValueError):
                build_review_prompt(invalid, bounded_output=True)
        instructions = "SYNTHETIC_STYLE_PRIVATE 按主题组织，语气克制；保留 {literal}。"
        frozen = build_review_prompt(instructions)
        self.assertTrue(frozen.startswith(legacy))
        self.assertTrue(frozen.endswith(instructions))
        runtime, timers = await self.runtime(review_prompt=frozen)
        await runtime.create_call(binding("A"))
        secret = InvitationSecret.create()
        await runtime.issue_web_invitation("A", secret, session_ttl=10000)
        self.now = 1100.0
        grant = await runtime.exchange_web_invitation(secret.value)
        token, ref = grant.token, grant.reference
        state = await runtime.web_state(token, ref)
        self.assertEqual(
            (state.limits.entered_at, state.limits.remaining_seconds, state.limits.warning),
            (1100, 3600, False),
        )
        self.assertEqual(len(timers.active()), 1)
        self.now = float("nan")
        with self.assertRaises(AccessError) as error:
            await runtime.web_heartbeat(token, ref)
        self.safe(error.exception)
        self.now = 1100.0
        self.assertEqual((await runtime.web_state(token, ref)).limits.deadline_at, 4700)
        await asyncio.wait_for(runtime.drain(), 1)  # A future timer is not drain background work.
        self.assertEqual(len(timers.active()), 1)
        await runtime.web_submit(token, ref, "opening", PRIVATE)
        await runtime.drain()
        # Real authenticated heartbeat transactions maintain the default 2min
        # reconnect window while trusted time jumps through the 60min duration.
        for offset in range(100, 3301, 100):
            self.now = 1100 + offset
            status = await runtime.web_heartbeat(token, ref)
        self.assertEqual((status.remaining_seconds, status.warning), (300, True))
        self.assertEqual(status.deadline_at, 4700)
        self.now = 4500
        await runtime.web_heartbeat(token, ref)
        self.now = 4600
        await runtime.web_heartbeat(token, ref)
        self.now = 4690
        await runtime.web_heartbeat(token, ref)
        self.assertEqual(timers.active()[0].delay, 10)
        self.now = 4703
        timers.fire()
        await runtime.drain()
        state = await runtime.read_state("A")
        public = await runtime.web_state(token, ref)
        self.assertEqual(
            (state.review.ended_at, public.limits.closed_at, public.limits.close_reason),
            (4703, 4703, "duration"),
        )
        self.assertTrue(state.publication.complete)
        self.assertEqual(state.review.prompt, frozen)
        self.assertNotIn(instructions, repr(state.review))
        self.assertFalse(timers.active())
        with self.assertRaises(AccessError) as error:
            await runtime.web_heartbeat(token, ref)
        self.safe(error.exception)
        await runtime.end_call("A")
        await runtime.resume_pending()
        await runtime.drain()
        self.assertEqual([name for name, _, _ in self.reviews], ["A"])
        self.assertEqual(grant.expires_at, 11100)
        # A disconnect window may reconnect before expiry but never at/after it.
        b, _ = await self.enter(runtime, "B")
        await runtime.web_submit(b.token, b.reference, "opening", PRIVATE)
        await runtime.drain()
        self.now += 119
        alive = await runtime.web_heartbeat(b.token, b.reference)
        self.assertEqual(alive.reconnect_remaining_seconds, 120)
        self.now += 127
        timers.fire()
        await runtime.drain()
        bstate = await runtime.web_state(b.token, b.reference)
        self.assertEqual(bstate.limits.close_reason, "disconnected")
        self.assertEqual(bstate.limits.closed_at, self.now)
        self.assertEqual((await runtime.read_state("B")).review.ended_at, self.now)
        self.assertEqual((await runtime.read_state("B")).review.prompt, frozen)
        self.assertEqual([name for name, _, _ in self.reviews], ["A", "B"])
        self.assertEqual(timers.threads, {web_fixture.ObservedStore.instances[0].owner})
        app = CallWebApp(runtime, origin=web_fixture.ORIGIN)
        cookie = "__Secure-bot_call=" + b.token
        invalid = await self.request(
            app, "heartbeat", data={"clock": self.now}, cookie=cookie, reference=b.reference
        )
        self.assertEqual(invalid[0], 400)
        crossed = await self.request(app, "heartbeat", data={}, cookie=cookie, reference=ref)
        self.assertEqual(crossed[0], 401)
        for kwargs in [
            dict(max_seconds=True),
            dict(warning_seconds=3600),
            dict(disconnect_seconds=0),
            dict(max_seconds=2**100),
        ]:
            with self.assertRaises(LimitsError):
                RuntimeLimits(**kwargs)
        # Explicit web close and default trusted close use the same frozen
        # instance setting, while explicit trusted arguments remain authoritative.
        web, _ = await self.enter(runtime, "styled-web")
        await runtime.web_submit(web.token, web.reference, "opening", PRIVATE)
        await runtime.drain()
        await runtime.web_end(web.token, web.reference)
        await runtime.drain()
        self.assertEqual((await runtime.read_state("styled-web")).review.prompt, frozen)
        explicit, _ = await self.enter(runtime, "explicit-default")
        await runtime.web_submit(explicit.token, explicit.reference, "opening", PRIVATE)
        await runtime.drain()
        await runtime.end_call("explicit-default", prompt=DEFAULT_REVIEW_PROMPT)
        await runtime.drain()
        self.assertEqual((await runtime.end_call("explicit-default")).prompt, DEFAULT_REVIEW_PROMPT)
        from bot_call.runtime import RuntimeOperationError

        with self.assertRaises(RuntimeOperationError):
            await runtime.end_call("styled-web", prompt="explicit conflicting trusted prompt")
        await runtime.shutdown(timeout=None)
        # Nonempty prior review/work with an uncertain node, plus a saved close
        # waiting on audio, must not be replanned from a later configuration.
        from test_review_work import freeze, prepare

        tables = ("reviews", "review_work", "review_work_nodes", "review_work_stages")
        with Store(self.path) as seed:
            freeze(seed, "legacy-style-uncertain")
            work = prepare(seed, "legacy-style-uncertain")
            node, won = seed.claim_review_work_node(
                "legacy-style-uncertain", work.frontier[0].source_key
            )
            self.assertTrue(won)
            seed.finish_review_work_node(
                "legacy-style-uncertain", node.source_key, state="uncertain"
            )
            old = {
                name: seed._db.execute(
                    "SELECT * FROM " + name + " WHERE call_id=?", ("legacy-style-uncertain",)
                ).fetchall()
                for name in tables
            }
            seed.create_call(binding("closure-only"))
            seed.receive_audio("closure-only", "waiting", b"synthetic-pending", "audio/webm")
            seed.end_call("closure-only", prompt=frozen, _request_close=True)
            close = seed.get_close_request("closure-only")
            self.assertNotIn(instructions, repr(close))
            # The saved-prompt lookup neither loads transcript nor reviews/input.
            trace = []
            seed._db.set_trace_callback(trace.append)
            self.assertEqual(seed.get_saved_review_prompt("closure-only"), frozen)
            self.assertTrue(all(q.startswith("SELECT prompt FROM ") for q in trace))
            seed._db.set_trace_callback(None)
        later = build_review_prompt("SYNTHETIC_CHANGED_STYLE 另一组织要求", bounded_output=True)
        restored, _ = await self.runtime(review_prompt=later)
        before = len(self.reviews)
        self.assertEqual((await restored.end_call("styled-web")).prompt, frozen)
        self.assertEqual(
            (await restored.end_call("explicit-default")).prompt, DEFAULT_REVIEW_PROMPT
        )
        self.assertIsInstance(await restored.end_call("closure-only"), ClosingReceipt)
        with patch.object(
            Store,
            "get_transcript",
            side_effect=AssertionError("no full-input read to choose style"),
        ):
            self.assertIsInstance(await restored.end_call("closure-only"), ClosingReceipt)
        await restored.end_call("legacy-style-uncertain")
        await restored.resume_pending()
        await restored.drain()
        self.assertEqual(len(self.reviews), before, "old unknown work was regenerated")
        newer, _ = await self.enter(restored, "new-config")
        await restored.web_submit(newer.token, newer.reference, "opening", PRIVATE)
        await restored.drain()
        await restored.web_end(newer.token, newer.reference)
        await restored.drain()
        self.assertEqual((await restored.read_state("new-config")).review.prompt, later)
        await restored.shutdown(timeout=None)
        with Store(self.path) as seed:
            self.assertEqual(seed.get_close_request("closure-only"), close)
            self.assertEqual(
                {
                    name: seed._db.execute(
                        "SELECT * FROM " + name + " WHERE call_id=?", ("legacy-style-uncertain",)
                    ).fetchall()
                    for name in tables
                },
                old,
            )
            # Standalone omitted prompt also respects the original saved style;
            # explicit conflicting prompt still follows the original contract.
            self.assertEqual(end_call(seed, "styled-web").prompt, frozen)
        # Existing planner accounts for the whole prompt; oversized instructions
        # do not raise the budget or truncate material to force a request.
        from bot_call.review_work import prepare_review_work
        from bot_call.review_plan import ReviewPlanError

        with Store(self.path.with_name("style-budget.db")) as seed:
            seed.create_call(binding("budget"))
            seed.append_user("budget", "saved", PRIVATE, source="synthetic")
            seed.end_call("budget", prompt=build_review_prompt("x" * 2000, bounded_output=True))
            with self.assertRaises(ReviewPlanError):
                prepare_review_work(seed, "budget", input_token_budget=500, count_tokens=len)
            self.assertEqual(seed.get_transcript("budget")[0].body, PRIVATE)
            self.assertIsNone(seed.get_review_work("budget"))
            self.assertEqual(seed.get_review("budget").state, "pending")

    async def test_lock_expiry_audio_barrier_reopen_original_policy_and_v7_atomic_upgrade(self):
        started, release = asyncio.Event(), asyncio.Event()
        self.gates.append(release)

        async def transcribe(bound, job):
            started.set()
            await release.wait()
            return " 原样最后片段 " + PRIVATE

        runtime, timers = await self.runtime(limits=RuntimeLimits(600, 60, 120), audio=transcribe)
        a, secret = await self.enter(runtime, "audio")
        await runtime.web_audio(a.token, a.reference, "last", b"raw-final", "audio/webm")
        await asyncio.wait_for(started.wait(), 2)
        # Lock wait spans the disconnect deadline. No late heartbeat can renew it.
        entered = asyncio.Event()
        original = Store.heartbeat_web_session

        def waiting(store, *args, **kwargs):
            self.loop.call_soon_threadsafe(entered.set)
            return original(store, *args, **kwargs)

        with patch.object(web_fixture.ObservedStore, "heartbeat_web_session", waiting):
            with closing(sqlite3.connect(self.path)) as locker:
                locker.execute("BEGIN IMMEDIATE")
                pending = asyncio.create_task(runtime.web_heartbeat(a.token, a.reference))
                await asyncio.wait_for(entered.wait(), 2)
                heartbeat = asyncio.Event()
                self.loop.call_soon(heartbeat.set)
                await heartbeat.wait()
                self.assertFalse(pending.done())
                self.now = 1121
                locker.commit()
                with self.assertRaises(AccessError):
                    await pending
        timers.fire()  # No page refresh is needed to persist close intent.
        waiting_state = await runtime.web_state(a.token, a.reference)
        self.assertEqual(
            (waiting_state.ended, waiting_state.close_state), (False, "waiting_for_audio")
        )
        self.assertEqual(waiting_state.limits.close_reason, "disconnected")
        self.assertEqual(waiting_state.close_requested_at, 1121)
        self.assertEqual(self.reviews, [])
        b, _ = await self.enter(runtime, "B")  # Closing releases slot while A transcribes.
        with self.assertRaises(AccessError):
            await runtime.web_heartbeat(b.token, a.reference)
        with self.assertRaises(AccessError):
            await runtime.web_submit(a.token, a.reference, "late", "not accepted")
        release.set()
        await runtime.drain()
        self.assertEqual((await runtime.read_state("audio")).review.ended_at, 1121)
        self.assertTrue((await runtime.read_state("audio")).publication.complete)
        self.assertEqual(self.audio_calls, [("audio", "last")])
        await runtime.web_end(b.token, b.reference)
        await runtime.drain()
        # Keep one live persisted policy, then stop its owner with no timer task leak.
        r, _ = await self.enter(runtime, "recover")
        await runtime.web_heartbeat(r.token, r.reference)
        await runtime.shutdown(timeout=None)
        self.assertFalse(timers.active())
        with Store(self.path) as seed:
            seed.receive_audio("recover", "waiting", b"persisted-pending", "audio/webm")
            seed.create_call(binding("unknown"))
            seed.receive_audio("unknown", "u", b"unknown-result", "audio/webm")
            seed.claim_next_audio("unknown")
        blind, blind_timers = await self.runtime(limits=None, audio=transcribe)
        await blind.resume_pending()
        await blind.drain()
        self.assertEqual(self.audio_calls, [("audio", "last")])
        self.assertFalse(blind_timers.active())
        self.assertFalse((await blind.web_state(r.token, r.reference)).limits.enforced)
        await blind.shutdown(timeout=None)
        self.now = 1150
        restored, restored_timers = await self.runtime(limits=RuntimeLimits(9000, 900, 900))
        restored_view = await restored.web_state(r.token, r.reference)
        self.assertEqual(restored_view.limits.deadline_at, 1721)  # Original 600, not new 9000.
        self.assertEqual(restored_view.limits.reconnect_remaining_seconds, 91)
        await restored.shutdown(timeout=None)
        self.now = 1300
        recovered, rt = await self.runtime(limits=RuntimeLimits(9000, 900, 900), audio=transcribe)
        saved = await recovered.web_state(r.token, r.reference)
        self.assertFalse(saved.ended)
        self.assertEqual(saved.close_state, "waiting_for_audio")
        self.assertEqual(saved.limits.close_reason, "disconnected")
        self.assertEqual(saved.limits.closed_at, 1300)
        # Startup persists local close only: it never generates recovered pending review.
        self.assertEqual([name for name, _, _ in self.reviews], ["audio"])
        self.assertIsNone((await recovered.read_state("B")).review)
        self.assertIsNone((await recovered.read_state("recover")).review)
        self.assertEqual(self.audio_calls, [("audio", "last")])
        await recovered.resume_pending()
        await recovered.drain()
        self.assertEqual(self.audio_calls, [("audio", "last"), ("recover", "waiting")])
        self.assertEqual((await recovered.read_state("recover")).review.ended_at, 1300)
        self.assertEqual((await recovered.read_state("unknown")).audio_jobs[0].state, "running")
        self.assertEqual(
            (await recovered.read_state("unknown")).audio_jobs[0].data, b"unknown-result"
        )
        await recovered.shutdown(timeout=None)
        # Build a genuine v7 fixture from this project's old DDL, not an evolving
        # wildcard table list. Migration failure must leave all old bytes/rows.
        old = self.path.with_name("v7.db")
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
            "audio_jobs",
            "call_closures",
        ]
        with closing(sqlite3.connect(self.path)) as original:
            rows = {name: original.execute("SELECT * FROM " + name).fetchall() for name in names}
        self.assertTrue(all(all(value is None for value in row[12:]) for row in rows["audio_jobs"]))
        historical_rows = {
            name: ([row[:12] for row in values] if name == "audio_jobs" else values)
            for name, values in rows.items()
        }
        with closing(sqlite3.connect(old)) as raw:
            for statement in (
                storage._SCHEMA
                + storage._REVIEW_SCHEMA
                + storage._PUBLICATION_SCHEMA
                + storage._WORK_SCHEMA
                + storage._MANIFEST_SCHEMA
                + storage._ACCESS_SCHEMA
                + storage._AUDIO_SCHEMA
            ):
                raw.execute(statement)
            raw.execute(f"PRAGMA application_id={storage._APPLICATION_ID}")
            raw.execute("PRAGMA user_version=7")
            for name in names:
                for row in historical_rows[name]:
                    raw.execute(
                        "INSERT INTO " + name + " VALUES (" + ",".join("?" for _ in row) + ")", row
                    )
            raw.commit()
            schema = raw.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall()
        with patch.object(
            storage, "_LIMITS_SCHEMA", storage._LIMITS_SCHEMA + ("INVALID MIGRATION",)
        ):
            with self.assertRaises(sqlite3.Error):
                Store(old)
        with closing(sqlite3.connect(old)) as raw:
            self.assertEqual(raw.execute("PRAGMA user_version").fetchone()[0], 7)
            self.assertIsNone(
                raw.execute("SELECT name FROM sqlite_master WHERE name='call_limits'").fetchone()
            )
            self.assertEqual(
                raw.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall(), schema
            )
            for name in names:
                self.assertEqual(
                    raw.execute("SELECT * FROM " + name).fetchall(), historical_rows[name]
                )
        with Store(old) as upgraded_store:
            for name in names:
                self.assertEqual(
                    [tuple(row) for row in upgraded_store._db.execute("SELECT * FROM " + name)],
                    rows[name],
                )
            upgraded_store.create_call(binding("atomic"))
            atomic = InvitationSecret.create()
            upgraded_store.issue_web_invitation("atomic", atomic, session_ttl=1000)
            denials = [1]

            def authorizer(action, op, *unused):
                if action == sqlite3.SQLITE_TRANSACTION and op == "COMMIT" and denials[0]:
                    denials[0] -= 1
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            upgraded_store._db.set_authorizer(authorizer)
            try:
                with self.assertRaises(AccessError) as error:
                    upgraded_store.exchange_web_invitation(atomic.value, _limits=RuntimeLimits())
                self.safe(error.exception)
            finally:
                upgraded_store._db.set_authorizer(None)
            self.assertEqual(denials, [0])
            self.assertIsNone(upgraded_store.get_call_limits("atomic"))
            self.assertIsNone(
                upgraded_store._db.execute(
                    "SELECT * FROM web_sessions WHERE call_id='atomic'"
                ).fetchone()
            )
        # No mode, then enabled mode: existing web session is never adopted.
        legacy, lt = await self.runtime(limits=None, path=old)
        old_grant, old_secret = await self.enter(legacy, "old")
        self.assertIsNone((await legacy.web_state(old_grant.token, old_grant.reference)).limits)
        self.assertFalse(lt.active())
        await legacy.shutdown(timeout=None)
        upgraded, ut = await self.runtime(path=old)
        duplicate = await upgraded.exchange_web_invitation(old_secret.value)
        self.assertEqual(duplicate.token, old_grant.token)
        self.assertIsNone(await upgraded.web_heartbeat(duplicate.token, duplicate.reference))
        self.assertIsNone((await upgraded.web_state(duplicate.token, duplicate.reference)).limits)
        self.assertFalse(ut.active())
        await upgraded.shutdown(timeout=None)
        with Store(old) as store:
            self.assertEqual(store._db.execute("PRAGMA user_version").fetchone()[0], 24)


if __name__ == "__main__":
    unittest.main()
