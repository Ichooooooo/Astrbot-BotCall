"""Four synthetic runtime lifecycle scenarios; all threads/tasks are collected."""

import asyncio
from contextlib import closing
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import threading
import traceback
import unittest
from unittest.mock import patch

from bot_call.astrbot_context import AstrBotContextAdapter
from bot_call.publication import Outcome
from bot_call.review import end_call
from bot_call.runtime import CallRuntime, RuntimeCallbacks, RuntimeClosed, RuntimeOperationError
from bot_call.store import Store
from test_astrbot_context import BODY, PRIVATE, SharedLocks, Conversations, binding


class ObservedStore(Store):
    instances = []
    arm = None
    waiting = None
    after_end = None
    fail_close = False

    def __init__(self, path):
        self.owner = threading.get_ident()
        self.access_threads = set()
        self.closed_thread = None
        self.instances.append(self)
        super().__init__(path)
        self._db.set_trace_callback(self.trace)

    def trace(self, statement):
        self.access_threads.add(threading.get_ident())
        if statement == "BEGIN IMMEDIATE" and self.arm is not None and self.arm.is_set():
            self.waiting.set()

    def end_call(self, *args, **kwargs):
        result = super().end_call(*args, **kwargs)
        if type(self).after_end is not None:
            type(self).after_end()
        return result

    def close(self):
        super().close()
        self.closed_thread = threading.get_ident()
        if type(self).fail_close:
            raise RuntimeError(PRIVATE)


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "calls.db"
        self.business = asyncio.get_running_loop()
        self.business_thread = threading.get_ident()
        self.observed = []
        ObservedStore.instances = self.observed
        ObservedStore.arm = ObservedStore.waiting = ObservedStore.after_end = None
        ObservedStore.fail_close = False
        self.patch_store = patch("bot_call.runtime.Store", ObservedStore)
        self.patch_store.start()
        self.addCleanup(self.patch_store.stop)

    def assert_business(self):
        self.assertIs(asyncio.get_running_loop(), self.business)
        self.assertEqual(threading.get_ident(), self.business_thread)

    def runtime(self, callbacks, path=None):
        runtime = CallRuntime(path or self.path, callbacks, char_budget=180)
        self.addAsyncCleanup(runtime.shutdown, timeout=3)
        return runtime

    def callbacks(self, *, reply=None, review=None, send=None, context=None):
        async def reply_default(bound, turn):
            self.assert_business()
            return "generated reply"

        async def review_default(bound, turns, prompt):
            self.assert_business()
            return BODY

        async def confirmed(*args):
            self.assert_business()
            return Outcome.CONFIRMED

        return RuntimeCallbacks(
            reply or reply_default,
            review or review_default,
            send or confirmed,
            context or confirmed,
        )

    async def test_full_flow_owns_sqlite_thread_bridges_core_loop_and_survives_real_sqlite_lock(
        self,
    ):
        target = binding("full")
        locks = SharedLocks()
        manager = Conversations(locks, target)
        counts = {"reply": 0, "review": 0, "send": 0, "context": 0, "reader": 0}

        async def reader(bound):
            self.assert_business()
            counts["reader"] += 1
            return await manager.model_input(bound)

        adapter = AstrBotContextAdapter(
            type("Context", (), {"conversation_manager": manager})(),
            session_lock_manager=locks,
            model_input_reader=reader,
        )

        async def reply(bound, turn):
            self.assert_business()
            counts["reply"] += 1
            return "reply:" + turn.material_id

        async def review(bound, turns, prompt):
            self.assert_business()
            counts["review"] += 1
            self.assertEqual(len(turns), 4)
            return BODY

        async def send(bound, page):
            self.assert_business()
            counts["send"] += 1
            return Outcome.CONFIRMED

        async def context(*args):
            self.assert_business()
            counts["context"] += 1
            return await adapter.make_context_readable(*args)

        runtime = self.runtime(
            self.callbacks(reply=reply, review=review, send=send, context=context)
        )
        await runtime.start()
        await runtime.create_call(target, background="background stays separate")
        await runtime.submit_utterance(target.call_id, "first", " first user text ")
        await runtime.drain()
        # Reserve the SQLite write lock on an independent synthetic connection.
        arm, waiting = threading.Event(), threading.Event()
        ObservedStore.arm, ObservedStore.waiting = arm, waiting
        with closing(sqlite3.connect(self.path, isolation_level=None)) as blocker:
            blocker.execute("BEGIN IMMEDIATE")
            arm.set()
            submission = asyncio.create_task(
                runtime.submit_utterance(target.call_id, "second", "second user text")
            )
            try:
                self.assertTrue(await asyncio.to_thread(waiting.wait, 3))
                heartbeat = asyncio.Event()
                self.business.call_soon(heartbeat.set)
                await asyncio.wait_for(heartbeat.wait(), 2)
                other = binding("other", "qq-2", "C")
                other_manager = Conversations(locks, other)
                other_adapter = AstrBotContextAdapter(
                    type("Context", (), {"conversation_manager": other_manager})(),
                    session_lock_manager=locks,
                )
                receipt = await asyncio.wait_for(
                    other_adapter.import_review(other, "review:other:context", BODY), 2
                )
                self.assertFalse(receipt.already_present)
                self.assertFalse(
                    submission.done(),
                    "SQLite is blocked while business heartbeat/another UMO progress",
                )
            finally:
                blocker.rollback()
            await submission
        arm.clear()
        await asyncio.gather(runtime.drain(), runtime.drain())
        await runtime.end_call(target.call_id)
        await runtime.drain()
        final = await runtime.read_state(target.call_id)
        self.assertTrue(final.publication.complete)
        self.assertEqual(final.review.body, BODY)
        self.assertEqual(
            (counts["reply"], counts["review"], counts["context"], counts["reader"]), (2, 1, 1, 1)
        )
        self.assertEqual(counts["send"], final.publication.page_count)
        self.assertEqual(manager.current, "B")
        self.assertEqual(manager.token_usage, 0)
        before = dict(counts)
        await runtime.end_call(target.call_id)
        await runtime.resume_pending()
        await runtime.drain()
        self.assertEqual(counts, before)
        store = self.observed[0]
        self.assertNotEqual(store.owner, self.business_thread)
        self.assertEqual(store.access_threads, {store.owner})
        self.assertTrue((await runtime.shutdown()).stopped)
        self.assertEqual(store.closed_thread, store.owner)
        self.assertFalse(runtime._thread.is_alive())
        self.assertEqual(await runtime.shutdown(), await runtime.shutdown())
        with self.assertRaises(RuntimeClosed):
            await runtime.submit_utterance(target.call_id, "after", "closed")

    async def test_end_ack_lost_to_page_cancel_keeps_background_and_suppresses_late_reply(self):
        target = binding("cancel-page")
        reply_entered, reply_release = asyncio.Event(), asyncio.Event()
        review_entered, review_release = asyncio.Event(), asyncio.Event()

        async def reply(*args):
            self.assert_business()
            reply_entered.set()
            await reply_release.wait()
            return "LATE_REPLY_MUST_NOT_ENTER_FROZEN_INPUT"

        async def review(bound, turns, prompt):
            self.assert_business()
            self.assertEqual(
                [(turn.role, turn.body) for turn in turns], [("user", "confirmed before end")]
            )
            review_entered.set()
            await review_release.wait()
            return BODY

        runtime = self.runtime(self.callbacks(reply=reply, review=review))
        self.addCleanup(reply_release.set)
        self.addCleanup(review_release.set)
        await runtime.start()
        await runtime.create_call(target)
        accepted = await runtime.submit_utterance(target.call_id, "u", "confirmed before end")
        self.assertEqual(accepted.body, "confirmed before end")
        await asyncio.wait_for(reply_entered.wait(), 3)
        page = None

        def after_commit():
            ObservedStore.after_end = None
            # Scheduled after Store COMMIT, before the operation's ACK delivery.
            self.business.call_soon_threadsafe(page.cancel)

        ObservedStore.after_end = after_commit
        page = asyncio.create_task(runtime.end_call(target.call_id))
        with self.assertRaises(asyncio.CancelledError):
            await page
        await asyncio.wait_for(review_entered.wait(), 3)
        state = await runtime.read_state(target.call_id)
        self.assertIsNotNone(state.review)
        self.assertEqual(state.generations[0].state, "cancelled")
        await runtime.end_call(target.call_id)  # Stable replay retrieves the same request.
        review_release.set()
        reply_release.set()
        await runtime.drain()
        state = await runtime.read_state(target.call_id)
        self.assertTrue(state.publication.complete)
        self.assertEqual(
            [(turn.role, turn.body) for turn in state.turns], [("user", "confirmed before end")]
        )
        self.assertTrue((await runtime.shutdown()).stopped)
        self.assertFalse(runtime._core_tasks)
        self.assertFalse(runtime._thread.is_alive())

    async def test_shutdown_tracks_actual_callbacks_cancellation_and_lost_confirmation_without_leaks(
        self,
    ):
        captured = []
        previous = self.business.get_exception_handler()
        self.business.set_exception_handler(lambda loop, context: captured.append(context))
        self.addCleanup(self.business.set_exception_handler, previous)
        for mode in ["reply", "review", "send", "context", "stubborn-send", "lost-send"]:
            with self.subTest(mode=mode):
                path = Path(self.directory.name) / (mode + ".db")
                target = binding(mode)
                entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
                self.addCleanup(release.set)
                effects = []

                async def special(*args):
                    self.assert_business()
                    effects.append(mode)
                    entered.set()
                    if mode == "lost-send":
                        raise RuntimeError(PRIVATE)
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        cancelled.set()
                        if mode == "stubborn-send":
                            await release.wait()
                            return Outcome.CONFIRMED
                        raise

                callbacks = self.callbacks(
                    reply=special if mode == "reply" else None,
                    review=special if mode == "review" else None,
                    send=special if mode in {"send", "stubborn-send", "lost-send"} else None,
                    context=special if mode == "context" else None,
                )
                runtime = self.runtime(callbacks, path)
                await runtime.start()
                await runtime.create_call(target)
                await runtime.submit_utterance(target.call_id, "u", "retained complete user text")
                if mode != "reply":
                    await runtime.drain()
                    await runtime.end_call(target.call_id)
                await asyncio.wait_for(entered.wait(), 3)
                if mode == "lost-send":
                    await runtime.drain()
                    self.assertEqual(
                        (await runtime.read_state(target.call_id)).publication.pages[0].state,
                        "uncertain",
                    )
                try:
                    report = await runtime.shutdown(timeout=0.05 if mode == "stubborn-send" else 2)
                    if mode == "stubborn-send":
                        await asyncio.wait_for(cancelled.wait(), 2)
                        self.assertFalse(report.stopped)
                        self.assertEqual(report.error_code, "still_stopping")
                        self.assertTrue(runtime._thread.is_alive())
                        self.assertTrue(runtime._core_tasks)
                        self.assertIsNone(self.observed[-1].closed_thread)
                        with closing(sqlite3.connect(path)) as db:
                            self.assertEqual(
                                db.execute(
                                    "SELECT state FROM publication_pages WHERE number=1"
                                ).fetchone()[0],
                                "running",
                            )
                        indefinite = asyncio.create_task(runtime.shutdown(timeout=None))
                        heartbeat = asyncio.Event()
                        self.business.call_soon(heartbeat.set)
                        await heartbeat.wait()
                        self.assertFalse(indefinite.done())
                        release.set()
                        report = await indefinite
                    self.assertTrue(report.stopped)
                    self.assertFalse(runtime._thread.is_alive())
                    self.assertFalse(runtime._core_tasks)
                finally:
                    release.set()
                    await runtime.shutdown(timeout=3)
                with closing(sqlite3.connect(path)) as db:
                    self.assertEqual(
                        db.execute("SELECT body FROM turns WHERE role='user'").fetchone()[0],
                        "retained complete user text",
                    )
                    if mode == "reply":
                        self.assertEqual(
                            db.execute("SELECT state FROM generations").fetchone()[0], "cancelled"
                        )
                    elif mode == "review":
                        self.assertEqual(
                            db.execute("SELECT state FROM reviews").fetchone()[0], "cancelled"
                        )
                    elif mode == "context":
                        self.assertEqual(
                            db.execute("SELECT context_state FROM publications").fetchone()[0],
                            "uncertain",
                        )
                    else:
                        self.assertEqual(
                            db.execute(
                                "SELECT state FROM publication_pages WHERE number=1"
                            ).fetchone()[0],
                            "uncertain",
                        )
                if mode in {"send", "stubborn-send", "lost-send", "context"}:

                    async def forbidden(*args):
                        self.fail("An uncertain external step must never repeat")

                    reopened = self.runtime(
                        self.callbacks(
                            send=forbidden, context=forbidden if mode == "context" else None
                        ),
                        path,
                    )
                    await reopened.start()
                    await reopened.resume_pending()
                    await reopened.drain()
                    self.assertEqual(effects, [mode])
                    self.assertTrue((await reopened.shutdown()).stopped)
        self.assertEqual(captured, [], "All owned task exceptions must be collected")

    async def test_explicit_pending_recovery_and_startup_close_failures_leave_no_threads(self):
        # Fixture preparation only; the runtime never accesses this main-loop Store.
        with Store(self.path) as setup:
            for call_id in ["pending", "uncertain", "confirmed"]:
                setup.create_call(binding(call_id))
                setup.append_user(call_id, "u", "saved fixture material", source="synthetic")
                end_call(setup, call_id)
                if call_id != "pending":
                    setup.claim_review(call_id)
                    setup.complete_review(call_id, BODY)
                    setup.prepare_publication(call_id, char_budget=180)
                    while True:
                        page, claimed = setup.claim_publication_page(call_id)
                        if not claimed:
                            break
                        setup.finish_publication_page(
                            call_id,
                            page.number,
                            "uncertain" if call_id == "uncertain" else "succeeded",
                        )
                    if call_id == "confirmed":
                        setup.claim_publication_context(call_id)
                        setup.finish_publication_context(call_id, "succeeded")
        generated, sent = [], []

        async def generate(bound, turns, prompt):
            self.assert_business()
            generated.append(bound.call_id)
            return BODY

        async def send(bound, page):
            self.assert_business()
            sent.append(bound.call_id)
            return Outcome.CONFIRMED

        runtime = self.runtime(self.callbacks(review=generate, send=send))
        await runtime.start()
        self.assertEqual((await runtime.read_state("pending")).review.state, "pending")
        self.assertEqual(generated, [])
        await runtime.resume_pending()
        await runtime.drain()
        self.assertEqual(generated, ["pending"])
        self.assertTrue(sent and set(sent) == {"pending"})
        self.assertTrue((await runtime.read_state("pending")).publication.complete)
        self.assertEqual(
            (await runtime.read_state("uncertain")).publication.pages[0].state, "uncertain"
        )
        self.assertTrue((await runtime.read_state("confirmed")).publication.complete)
        ObservedStore.fail_close = True
        report = await runtime.shutdown()
        self.assertTrue(report.stopped)
        self.assertEqual(report.error_code, "worker_failed")
        self.assertFalse(runtime._thread.is_alive())
        ObservedStore.fail_close = False
        bad = Path(self.directory.name) / "foreign.db"
        with closing(sqlite3.connect(bad)) as db:
            db.execute("CREATE TABLE unrelated(value TEXT)")
        failed = self.runtime(self.callbacks(), bad)
        try:
            await failed.start()
        except RuntimeOperationError as error:
            self.assertNotIn(PRIVATE, "".join(traceback.format_exception(error)))
        else:
            self.fail("Startup must reject an external database")
        self.assertFalse(failed._thread.is_alive())
        self.assertEqual((await failed.shutdown()).error_code, "startup_failed")


if __name__ == "__main__":
    unittest.main()
