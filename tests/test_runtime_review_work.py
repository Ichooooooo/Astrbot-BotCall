"""Three offline integration scenarios; real temp SQLite, synthetic callbacks."""

import asyncio
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import threading
import traceback
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from bot_call.astrbot_context import AstrBotContextAdapter
from bot_call.publication import Outcome
from bot_call.review_work import prepare_review_work, NotApplied
from bot_call.runtime import CallRuntime, RuntimeCallbacks, RuntimeReviewWork
from bot_call.store import Store
from test_astrbot_context import SharedLocks, Conversations, binding


BUDGET = 2500
PRIVATE = "SYNTHETIC_PRIVATE_CALL_BODY_CREDENTIAL_十二阶段"
PROMPT = "PRIVATE_REVIEW_PROMPT 区分表达/建议/决定/未定与播放观测\n "
FINAL = (
    " 本次完整回顾：开头家庭，中段犹豫，结尾未定；Bot建议非决定，未知播放。🙂\n\n" * 3
) + PRIVATE


class TrackedStore(Store):
    instances = []
    before_begin = None
    after_end = None
    faults = {}

    def __init__(self, path):
        self.owner = threading.get_ident()
        self.access_threads = set()
        self.closed_thread = None
        self.instances.append(self)
        super().__init__(path)
        self._db.set_trace_callback(self.trace)

    def trace(self, sql):
        self.access_threads.add(threading.get_ident())
        hook = type(self).before_begin
        if sql == "BEGIN IMMEDIATE" and hook is not None:
            hook()

    def end_call(self, *args, **kwargs):
        result = super().end_call(*args, **kwargs)
        hook = type(self).after_end
        if hook is not None:
            hook(result.call_id)
        return result

    def finish_review_work_node(self, call_id, source_key, *, state, body=None):
        fault = self.faults.get(call_id)
        if fault == "double" or (fault == "commit" and state == "succeeded"):
            raise sqlite3.OperationalError(PRIVATE + PROMPT)
        result = super().finish_review_work_node(call_id, source_key, state=state, body=body)
        if fault == "ack" and state == "succeeded":
            raise sqlite3.OperationalError(PRIVATE + PROMPT)
        return result

    def complete_review(self, call_id, body):
        if self.faults.get(call_id) == "parent-fail":
            raise sqlite3.OperationalError(PRIVATE + PROMPT)
        return super().complete_review(call_id, body)

    def close(self):
        super().close()
        self.closed_thread = threading.get_ident()


class RuntimeReviewWorkTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "calls.db"
        self.business = asyncio.get_running_loop()
        self.business_thread = threading.get_ident()
        self.runtimes, self.gates = [], []
        self.violations, self.unexpected, self.captured = [], [], []
        self.counter_threads = set()
        self.callback_threads = set()
        self.old_handler = self.business.get_exception_handler()
        self.business.set_exception_handler(lambda loop, context: self.captured.append(context))
        TrackedStore.instances = []
        TrackedStore.before_begin = TrackedStore.after_end = None
        TrackedStore.faults = {}
        self.store_patch = patch("bot_call.runtime.Store", TrackedStore)
        self.store_patch.start()
        self.addCleanup(self.store_patch.stop)

    async def asyncTearDown(self):
        for gate in self.gates:
            gate.set()
        for runtime in reversed(self.runtimes):
            self.assertTrue((await runtime.shutdown(timeout=3)).stopped)
            if runtime._thread is not None:
                self.assertFalse(runtime._thread.is_alive())
            self.assertFalse(runtime._core_tasks)
        self.business.set_exception_handler(self.old_handler)
        self.assertEqual(self.violations, [])
        self.assertEqual(self.unexpected, [])
        self.assertEqual(self.captured, [], "owned task failures must be collected")
        for store in TrackedStore.instances:
            self.assertNotEqual(store.owner, self.business_thread)
            self.assertEqual(store.access_threads, {store.owner})
            self.assertEqual(store.closed_thread, store.owner)

    def on_business(self):
        actual = (asyncio.get_running_loop(), threading.get_ident())
        self.callback_threads.add(actual[1])
        if actual != (self.business, self.business_thread):
            self.violations.append("wrong callback loop/thread")

    def count(self, request):
        thread = threading.get_ident()
        self.counter_threads.add(thread)
        if thread == self.business_thread:
            self.violations.append("counter on business thread")
        return len(request)

    async def forbidden(self, *args):
        self.unexpected.append("unexpected callback")
        raise AssertionError("callback must not run")

    def runtime(
        self,
        *,
        path=None,
        generate=None,
        configured=True,
        counter=None,
        budget=BUDGET,
        reply=None,
        send=None,
        context=None,
    ):
        async def default_reply(bound, turn):
            self.on_business()
            return " Bot建议非用户决定；播放观测未知。 "

        async def default_generate(node):
            self.on_business()
            return "生成分块笔记，非用户原话" if node.kind != "final" else FINAL

        async def confirmed(*args):
            self.on_business()
            return Outcome.CONFIRMED

        callbacks = RuntimeCallbacks(
            reply or default_reply, self.forbidden, send or confirmed, context or confirmed
        )
        config = (
            RuntimeReviewWork(budget, counter or self.count, generate or default_generate)
            if configured
            else None
        )
        runtime = CallRuntime(path or self.path, callbacks, char_budget=145, review_work=config)
        self.runtimes.append(runtime)
        return runtime

    async def test_long_call_worker_planning_business_callbacks_and_same_final_publication(self):
        target = binding("long")
        locks = SharedLocks()
        manager = Conversations(locks, target)
        counts = {"map": [], "merge": [], "final": [], "send": [], "reader": []}

        async def reader(bound):
            self.on_business()
            counts["reader"].append(bound.call_id)
            return await manager.model_input(bound)

        adapter = AstrBotContextAdapter(
            SimpleNamespace(conversation_manager=manager),
            session_lock_manager=locks,
            model_input_reader=reader,
        )

        async def generate(node):
            self.on_business()
            counts[node.kind].append(node.source_key)
            self.assertLessEqual(len(node.request_json), BUDGET)
            self.assertEqual(node.token_count, len(node.request_json))
            data = json.loads(json.loads(node.request_json)["messages"][1]["content"])
            self.assertEqual(data["review_prompt"], PROMPT)
            if node.kind == "final":
                self.assertEqual(node.first_map, 0)
                self.assertEqual(node.stop_map, map_count[0])
                return FINAL
            return "生成分块笔记，非用户原话：开头/中段/结尾，Bot建议非决定/未知播放。" + " 料" * 20

        async def send(bound, page):
            self.on_business()
            self.assertEqual(bound, target)
            counts["send"].append(page.fragment)
            return Outcome.CONFIRMED

        async def context(*args):
            self.on_business()
            return await adapter.make_context_readable(*args)

        planning_threads = []
        import bot_call.review_work as work_module

        original_plan = work_module.build_review_plan

        def observed_plan(*args, **kwargs):
            planning_threads.append(threading.get_ident())
            return original_plan(*args, **kwargs)

        runtime = self.runtime(generate=generate, send=send, context=context)
        await runtime.start()
        await runtime.create_call(target, background="BACKGROUND_NOT_CALL")
        for material_id, topic in [
            ("opening", "开头家庭🙂 e\u0301\r\n "),
            ("middle", "中段犹豫\t "),
            ("ending", "结尾未定\n "),
        ]:
            await runtime.submit_utterance(target.call_id, material_id, topic * 500)
            await runtime.drain()
        expected_turns = (await runtime.read_state(target.call_id)).turns
        map_count = [0]
        # Hold a real SQLite writer lock while the original business loop runs.
        waiting = asyncio.Event()
        with closing(sqlite3.connect(self.path, isolation_level=None)) as blocker:
            blocker.execute("BEGIN IMMEDIATE")
            TrackedStore.before_begin = lambda: self.business.call_soon_threadsafe(waiting.set)
            with patch.object(work_module, "build_review_plan", side_effect=observed_plan):
                request = asyncio.create_task(runtime.end_call(target.call_id, prompt=PROMPT))
                try:
                    await asyncio.wait_for(waiting.wait(), 3)
                    heartbeat = asyncio.Event()
                    self.business.call_soon(heartbeat.set)
                    await asyncio.wait_for(heartbeat.wait(), 2)
                    self.assertFalse(request.done())
                finally:
                    TrackedStore.before_begin = None
                    blocker.rollback()
                job = await request
                # Read the installed snapshot through the worker request API;
                # each bridge round yields to other owned worker requests.
                state = await runtime.read_state(target.call_id)
                if state.review_work is not None:
                    map_count[0] = state.review_work.map_count
                await asyncio.gather(
                    runtime.resume_pending(),
                    runtime.resume_pending(),
                    runtime.end_call(target.call_id, prompt=PROMPT),
                )
                await runtime.drain()
        state = await runtime.read_state(target.call_id)
        self.assertEqual(state.review.state, "succeeded")
        self.assertEqual(state.review_work.state, "succeeded")
        self.assertEqual(state.review.body, FINAL)
        self.assertEqual(state.turns, expected_turns)
        self.assertEqual(json.loads(job.input_json), json.loads(state.review.input_json))
        self.assertEqual(
            state.review.input_sha256, hashlib.sha256(job.input_json.encode()).hexdigest()
        )
        self.assertGreater(state.review_work.map_count, 4)
        self.assertGreaterEqual(state.review_work.level, 2)
        all_keys = counts["map"] + counts["merge"] + counts["final"]
        self.assertEqual(len(all_keys), len(set(all_keys)))
        self.assertLessEqual(len(all_keys), 2 * state.review_work.map_count)
        self.assertEqual(len(counts["map"]), state.review_work.map_count)
        self.assertEqual(len(counts["final"]), 1)
        self.assertTrue(state.publication.complete)
        self.assertEqual("".join(counts["send"]), FINAL)
        self.assertEqual(counts["reader"], [target.call_id])
        self.assertEqual(manager.history[-1], {"role": "assistant", "content": FINAL})
        self.assertEqual(manager.current, "B")
        before = {key: list(value) for key, value in counts.items()}
        await runtime.resume_pending()
        await runtime.end_call(target.call_id, prompt=PROMPT)
        await runtime.drain()
        self.assertEqual(counts, before)
        owner = TrackedStore.instances[0].owner
        self.assertEqual(self.counter_threads, {owner})
        self.assertEqual(set(planning_threads), {owner})
        self.assertEqual(self.callback_threads, {self.business_thread})
        self.assertNotIn(PRIVATE, repr(state))
        self.assertNotIn(PROMPT, repr(state))

    async def test_reopen_modes_fixed_plan_and_end_ack_cancel_keep_background(self):
        # Synthetic fixtures only; the runtime never accesses this setup Store.
        with Store(self.path) as setup:
            for call in [
                "resume",
                "running",
                "uncertain",
                "failed",
                "cancelled",
                "empty",
                "legacy-running",
            ]:
                setup.create_call(binding(call))
                if call != "empty":
                    setup.append_user(
                        call,
                        "u",
                        "完整冻结材料🙂\n " * (600 if call == "resume" else 1),
                        source="text",
                    )
                setup.end_call(call, prompt=PROMPT)
                if call == "legacy-running":
                    setup.claim_review(call)
                    continue
                if call == "empty":
                    self.assertEqual(setup.get_transcript(call), ())
                    continue
                work = prepare_review_work(setup, call, input_token_budget=BUDGET, count_tokens=len)
                node, _ = setup.claim_review_work_node(call, work.frontier[0].source_key)
                if call == "resume":
                    setup.finish_review_work_node(
                        call, node.source_key, state="succeeded", body="已知成功笔记"
                    )
                    original_plan, known_key = work.plan_json, node.source_key
                elif call != "running":
                    setup.finish_review_work_node(call, node.source_key, state=call)
        unconfigured = self.runtime(configured=False)
        await unconfigured.start()
        await unconfigured.resume_pending()
        await unconfigured.drain()
        self.assertEqual(
            (await unconfigured.read_state("resume")).review_work.plan_json, original_plan
        )
        self.assertEqual((await unconfigured.read_state("legacy-running")).review.state, "running")
        await unconfigured.shutdown()
        mismatch = self.runtime(
            generate=self.forbidden, counter=lambda request: len(request) + 1, budget=9000
        )
        await mismatch.start()
        await mismatch.resume_pending()
        await mismatch.drain()
        state = await mismatch.read_state("resume")
        self.assertEqual(state.review_work.plan_json, original_plan)
        self.assertEqual(state.review_work.state, "pending")
        self.assertIsNone(state.publication)
        self.assert_safe_error(mismatch._progress_task.exception())
        await mismatch.shutdown()
        keys = []

        async def resumed(node):
            self.on_business()
            keys.append(node.source_key)
            self.assertEqual(node.call_id, "resume")
            self.assertNotEqual(node.source_key, known_key)
            self.assertLessEqual(len(node.request_json), BUDGET)
            return "已确认分块笔记" if node.kind != "final" else FINAL

        resumed_runtime = self.runtime(generate=resumed, budget=9000)
        await resumed_runtime.start()
        self.assertEqual(keys, [])
        await resumed_runtime.resume_pending()
        await resumed_runtime.drain()
        state = await resumed_runtime.read_state("resume")
        self.assertEqual(state.review_work.plan_json, original_plan)
        self.assertEqual(state.review_work.input_token_budget, BUDGET)
        self.assertTrue(state.publication.complete)
        self.assertEqual(len(keys), len(set(keys)))
        self.assertNotIn(known_key, keys)
        for call in ["running", "uncertain", "failed", "cancelled", "empty", "legacy-running"]:
            state = await resumed_runtime.read_state(call)
            self.assertIsNone(state.publication)
            self.assertTrue(state.review is None or state.review.body is None)
        empty = await resumed_runtime.read_state("empty")
        self.assertIsNone(empty.review)
        self.assertIsNone(empty.review_work)
        await resumed_runtime.shutdown()
        # A new page loses its end ACK after COMMIT; both reply and map are in flight.
        reply_entered, reply_release = asyncio.Event(), asyncio.Event()
        map_entered, map_release = asyncio.Event(), asyncio.Event()
        self.gates.extend([reply_release, map_release])
        page_keys = []

        async def reply(bound, turn):
            self.on_business()
            reply_entered.set()
            await reply_release.wait()
            return "LATE_REPLY_MUST_NOT_ENTER_FROZEN_INPUT"

        async def page_generate(node):
            self.on_business()
            page_keys.append(node.source_key)
            self.assertEqual(node.call_id, "page")
            if node.kind == "map":
                map_entered.set()
                await map_release.wait()
                return "本次分块笔记"
            return FINAL

        runtime = self.runtime(generate=page_generate, reply=reply)
        await runtime.start()
        self.assertEqual(page_keys, [])
        await runtime.create_call(binding("page"))
        await runtime.submit_utterance("page", "u", PRIVATE)
        await reply_entered.wait()
        holder = {}
        TrackedStore.after_end = lambda call: (
            self.business.call_soon_threadsafe(holder["task"].cancel) if call == "page" else None
        )
        holder["task"] = asyncio.create_task(runtime.end_call("page", prompt=PROMPT))
        with self.assertRaises(asyncio.CancelledError):
            await holder["task"]
        TrackedStore.after_end = None
        await map_entered.wait()
        state = await runtime.read_state("page")
        self.assertEqual(state.review_work.state, "running")
        self.assertEqual([(t.role, t.body) for t in state.turns], [("user", PRIVATE)])
        self.assertIsNone(state.publication)
        map_release.set()
        reply_release.set()
        await runtime.drain()
        state = await runtime.read_state("page")
        self.assertEqual(state.review.body, FINAL)
        self.assertTrue(state.publication.complete)
        self.assertEqual([(t.role, t.body) for t in state.turns], [("user", PRIVATE)])
        self.assertEqual(json.loads(state.review.input_json)[0]["body"], PRIVATE)
        self.assertNotIn("LATE_REPLY_MUST_NOT_ENTER_FROZEN_INPUT", state.review.input_json)
        self.assertEqual(len(page_keys), 2)
        self.assertEqual(len(page_keys), len(set(page_keys)))
        await runtime.shutdown()
        reopened = self.runtime(generate=self.forbidden)
        await reopened.start()
        await reopened.resume_pending()
        await reopened.drain()
        self.assertTrue((await reopened.read_state("page")).publication.complete)
        for budget in [True, 0, -1, 1.5]:
            with self.assertRaises(ValueError):
                RuntimeReviewWork(budget, len, resumed)

        async def async_counter(request):
            return len(request)

        with self.assertRaises(TypeError):
            RuntimeReviewWork(BUDGET, async_counter, resumed)

    def assert_safe_error(self, error):
        self.assertIsNotNone(error)
        output = "".join(traceback.format_exception(error))
        self.assertNotIn(PRIVATE, output)
        self.assertNotIn(PROMPT, output)
        self.assertTrue(error.__suppress_context__)

    async def test_chunk_outcomes_confirmation_faults_and_shutdown_wait_actual_callback(self):
        for mode in [
            "not-applied",
            "exception",
            "cancel",
            "commit",
            "double",
            "ack",
            "parent-fail",
            "stubborn",
        ]:
            with self.subTest(mode=mode):
                path = self.path.with_name(mode + ".db")
                target = binding(mode)
                entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
                self.gates.append(release)
                effects, sent, contexts = [], [], []

                async def fake(node):
                    self.on_business()
                    effects.append((node.source_key, node.kind))
                    entered.set()
                    if mode == "not-applied":
                        raise NotApplied(PRIVATE + PROMPT)
                    if mode == "exception":
                        raise RuntimeError(PRIVATE + PROMPT)
                    if mode == "cancel":
                        raise asyncio.CancelledError(PRIVATE + PROMPT)
                    if mode == "stubborn":
                        try:
                            await asyncio.Event().wait()
                        except asyncio.CancelledError:
                            cancelled.set()
                            await release.wait()
                            return PRIVATE
                    return "分块笔记" if node.kind != "final" else FINAL

                async def send(bound, page):
                    self.on_business()
                    sent.append(page.source_key)
                    return Outcome.CONFIRMED

                async def context(bound, key, body):
                    self.on_business()
                    contexts.append(body)
                    self.assertEqual(body, FINAL)
                    return Outcome.CONFIRMED

                if mode in {"commit", "double", "ack", "parent-fail"}:
                    TrackedStore.faults[mode] = mode
                runtime = self.runtime(path=path, generate=fake, send=send, context=context)
                await runtime.start()
                await runtime.create_call(target)
                await runtime.submit_utterance(mode, "u", PRIVATE)
                await runtime.drain()
                await runtime.end_call(mode, prompt=PROMPT)
                await asyncio.wait_for(entered.wait(), 3)
                if mode == "stubborn":
                    report = await runtime.shutdown(timeout=0.03)
                    await asyncio.wait_for(cancelled.wait(), 2)
                    self.assertEqual((report.stopped, report.error_code), (False, "still_stopping"))
                    self.assertTrue(runtime._thread.is_alive())
                    self.assertTrue(runtime._core_tasks)
                    self.assertIsNone(TrackedStore.instances[-1].closed_thread)
                    with closing(sqlite3.connect(path)) as raw:
                        self.assertEqual(
                            raw.execute("SELECT state FROM review_work_nodes").fetchone()[0],
                            "running",
                        )
                        self.assertEqual(
                            raw.execute("SELECT count(*) FROM publications").fetchone()[0], 0
                        )
                    indefinite = asyncio.create_task(runtime.shutdown(timeout=None))
                    heartbeat = asyncio.Event()
                    self.business.call_soon(heartbeat.set)
                    await heartbeat.wait()
                    self.assertFalse(indefinite.done())
                    release.set()
                    self.assertTrue((await indefinite).stopped)
                    with closing(sqlite3.connect(path)) as raw:
                        self.assertEqual(
                            raw.execute("SELECT state FROM review_work_nodes").fetchone()[0],
                            "cancelled",
                        )
                else:
                    await runtime.drain()
                    state = await runtime.read_state(mode)
                    if mode in {"ack", "parent-fail"}:
                        if mode == "parent-fail":
                            self.assertEqual(state.review_work.frontier[0].state, "succeeded")
                            self.assertEqual(state.review.state, "running")
                            self.assertIsNone(state.publication)
                            self.assert_safe_error(runtime._progress_task.exception())
                            before = list(effects)
                            del TrackedStore.faults[mode]
                            await runtime.resume_pending()
                            await runtime.drain()
                            self.assertEqual(effects, before)
                            state = await runtime.read_state(mode)
                        self.assertTrue(state.publication.complete)
                        self.assertEqual(state.review.body, FINAL)
                        self.assertEqual(contexts, [FINAL])
                        self.assertEqual([kind for key, kind in effects], ["map", "final"])
                    else:
                        expected = {
                            "not-applied": "failed",
                            "exception": "uncertain",
                            "cancel": "cancelled",
                            "commit": "uncertain",
                            "double": "running",
                        }[mode]
                        self.assertEqual(state.review_work.frontier[0].state, expected)
                        self.assertIsNone(state.review.body)
                        self.assertIsNone(state.publication)
                        self.assertEqual(sent, [])
                        self.assertEqual(contexts, [])
                        if mode in {"commit", "double"}:
                            self.assert_safe_error(runtime._progress_task.exception())
                    self.assertTrue((await runtime.shutdown()).stopped)
                before = list(effects)
                recovered = self.runtime(
                    path=path, generate=self.forbidden, send=send, context=context
                )
                await recovered.start()
                await recovered.resume_pending()
                await recovered.drain()
                self.assertEqual(effects, before)
                self.assertEqual(len({key for key, kind in effects}), len(effects))
                self.assertEqual(
                    (await recovered.read_state(mode)).review.input_sha256,
                    hashlib.sha256(
                        (await recovered.read_state(mode)).review.input_json.encode()
                    ).hexdigest(),
                )
                await recovered.shutdown()


if __name__ == "__main__":
    unittest.main()
