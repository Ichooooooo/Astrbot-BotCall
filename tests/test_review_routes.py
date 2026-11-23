"""New public policy resume and byte-preserving legacy decode, no old replay."""

import json
from dataclasses import replace
from types import SimpleNamespace as NS

from bot_call.astrbot_audio import AudioRequestPolicy
from bot_call.astrbot_reply import ReplyModel, ReplyPolicy, AstrBotReplyAdapter, ReplyNotApplied
from bot_call.astrbot_review import ReviewRequestPolicy, AstrBotReviewAdapter
from bot_call.call_policy import CallPolicy, CallPolicyError
from bot_call.native_provider import NATIVE_CONTRACT, COUNTER_RULE
from bot_call.review_policy import ReviewManifest
from bot_call.review_router import ReviewPolicyRouter, ReviewPolicySelection
from bot_call.review_work import prepare_review_work, NotApplied
from bot_call.publication import Outcome
from bot_call.runtime import CallRuntime, RuntimeCallbacks
from bot_call.store import Store
from test_astrbot_context import binding
from test_backend import NativeCase, TEXT, FINAL


def call_rows(store, call_id):
    result = {}
    for (name,) in store._db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        columns = [row[1] for row in store._db.execute('PRAGMA table_info("' + name + '")')]
        if "call_id" in columns:
            result[name] = store._db.execute(
                'SELECT * FROM "' + name + '" WHERE call_id=?', (call_id,)
            ).fetchall()
    return result


class ReviewRouteTests(NativeCase):
    async def test_reopen_original_manifest_rejects_same_count_changes_and_modes_never_adopt(self):
        event, provider = self.environment()
        host = self.host()
        legacy = binding("legacy", "qq-A", "A")
        counter = lambda text: len(text) // 4 + 1
        old_review = ReviewRequestPolicy(
            legacy,
            provider.meta().id,
            provider.get_model(),
            32000,
            2048,
            1024,
            "max_tokens",
            counter,
        )
        old_manifest = ReviewManifest.from_policy(old_review, "legacy-counter-v1")
        old_reply = ReplyPolicy(
            legacy, ReplyModel(provider.meta().id, provider.get_model(), 32000, 1024, "max_tokens")
        )
        old_audio = AudioRequestPolicy(legacy, "stt-A", "native-model", 2097152, 16777216)
        old_policy = CallPolicy.from_policies(old_reply, old_audio, old_manifest)
        self.assertFalse(old_policy.is_native)
        self.assertFalse(old_manifest.is_native)
        path = self.path / "old.db"
        with Store(path) as store:
            store.create_call(legacy, policy=old_policy)
            store.append_user("legacy", "u", TEXT, source="synthetic")
            store.end_call("legacy", prompt="legacy frozen prompt")
            work = prepare_review_work(
                store,
                "legacy",
                input_token_budget=old_review.input_token_budget,
                count_tokens=old_review.count_request,
                manifest=old_manifest,
            )
            node, won = store.claim_review_work_node(
                "legacy", work.frontier[0].source_key, manifest_sha256=old_manifest.fingerprint
            )
            self.assertTrue(won)
            store.finish_review_work_node(
                "legacy",
                node.source_key,
                state="uncertain",
                manifest_sha256=old_manifest.fingerprint,
            )
            before = call_rows(store, "legacy")
            schema = store._db.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
            self.assertEqual(store._db.execute("PRAGMA user_version").fetchone()[0], 24)
        with Store(path) as store:
            saved = store.get_call_policy("legacy")
            self.assertEqual(saved.canonical_json, old_policy.canonical_json)
            self.assertEqual(saved.fingerprint, old_policy.fingerprint)
            restored = saved.review_manifest.restore(legacy, {"legacy-counter-v1": counter})
            self.assertEqual(
                restored.payload_json(node.request_json), old_review.payload_json(node.request_json)
            )
            with self.assertRaises(NotApplied):
                await AstrBotReviewAdapter.bind(self.context, restored, privacy=host.privacy)
            self.assertEqual(call_rows(store, "legacy"), before)
            self.assertEqual(
                store._db.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' ORDER BY name"
                ).fetchall(),
                schema,
            )
        self.assertEqual(provider.calls, [])
        native = replace(legacy, call_id="native")
        native_review = replace(old_review, binding=native, cap_parameter=NATIVE_CONTRACT)
        native_manifest = ReviewManifest.from_policy(native_review, COUNTER_RULE)
        from bot_call.astrbot_agent import SourceAgentPolicy

        native_policy = CallPolicy.from_policies(
            SourceAgentPolicy(native),
            replace(old_audio, binding=native, selection=NATIVE_CONTRACT),
            native_manifest,
        )
        self.assertTrue(native_policy.is_native)
        self.assertTrue(native_manifest.is_native)
        self.assertNotEqual(
            native_manifest.fingerprint,
            ReviewManifest.from_policy(
                replace(native_review, cap_parameter="max_tokens"), COUNTER_RULE
            ).fingerprint,
        )
        data = json.loads(native_policy.canonical_json)
        data["version"] = "bot-call-policy-v1"
        from bot_call.call_policy import _json, _sha

        with self.assertRaises(CallPolicyError):
            CallPolicy(_json(data), _sha(_json(data)))
        native_path = self.path / "new.db"
        with Store(native_path) as store:
            store.create_call(native, policy=native_policy)
            store.append_user("native", "u", TEXT, source="synthetic")
            store.end_call("native", prompt="new frozen prompt")
            plan = prepare_review_work(
                store,
                "native",
                input_token_budget=native_review.input_token_budget,
                count_tokens=native_review.count_request,
                manifest=native_manifest,
            ).plan_json
        registry = {COUNTER_RULE: counter}

        async def resolve(bound, saved):
            return ReviewPolicySelection(saved.restore(bound, registry), saved.counter_rule_id)

        async def bind_adapter(policy):
            return await AstrBotReviewAdapter.bind(self.context, policy, privacy=host.privacy)

        router = ReviewPolicyRouter(resolve, bind_adapter)

        async def never(*args):
            raise AssertionError("legacy callback")

        async def sent(*args):
            return Outcome.SUBMITTED

        async def readable(*args):
            return Outcome.CONFIRMED

        runtime = CallRuntime(
            native_path, RuntimeCallbacks(never, never, sent, readable), review_routes=router
        )
        await runtime.start()
        try:
            self.assertEqual(provider.calls, [])
            await runtime.resume_pending()
            await runtime.drain()
            state = await runtime.read_state("native")
            self.assertTrue(state.publication.complete)
            self.assertEqual(state.review.body, FINAL)
            self.assertEqual(state.review_work.plan_json, plan)
            self.assertEqual(state.review_work.manifest, native_manifest)
            count = len(provider.calls)
            await runtime.resume_pending()
            await runtime.drain()
            self.assertEqual(len(provider.calls), count)
        finally:
            self.assertTrue((await runtime.shutdown(timeout=None)).stopped)


import asyncio
from contextlib import closing
from pathlib import Path
import sqlite3
import threading
import traceback
from unittest.mock import patch
from bot_call.runtime import RuntimeReviewWork
from bot_call.review_work import advance_review_work, ReviewWorkError
from bot_call.store import (
    StateConflict,
    _APPLICATION_ID,
    _SCHEMA,
    _REVIEW_SCHEMA,
    _PUBLICATION_SCHEMA,
    _WORK_SCHEMA,
)
from bot_call.review_policy import ReviewPolicyUnavailable
from bot_call import store as storage
from test_backend import PRIVATE, PublicProvider, response

RULE = COUNTER_RULE


class ObservedStore(Store):
    instances = []
    after_end = None
    ack_loss = set()

    def __init__(self, path):
        self.owner = threading.get_ident()
        self.access_threads = set()
        self.closed_thread = None
        self.instances.append(self)
        super().__init__(path)
        self._db.set_trace_callback(lambda sql: self.access_threads.add(threading.get_ident()))

    def end_call(self, *args, **kwargs):
        result = super().end_call(*args, **kwargs)
        if type(self).after_end:
            type(self).after_end(result.call_id)
        return result

    def install_review_work(self, plan, **kwargs):
        result = super().install_review_work(plan, **kwargs)
        if plan.call_id in self.ack_loss:
            self.ack_loss.remove(plan.call_id)
            raise sqlite3.OperationalError(PRIVATE)
        return result

    def close(self):
        super().close()
        self.closed_thread = threading.get_ident()


class RestoredReviewRoutesTests(NativeCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.path = self.path / "routes.db"
        self.loop = asyncio.get_running_loop()
        self.business_thread = threading.get_ident()
        self.runtimes = []
        self.unexpected = []
        self.violations = []
        self.public_calls = []
        self.captured = []

        class Counter:
            def __init__(inner):
                inner.threads = set()

            def __call__(inner, text):
                inner.threads.add(threading.get_ident())
                return len(text)

        self.counter = Counter()
        self.registry = {RULE: self.counter}
        self.old_handler = self.loop.get_exception_handler()
        self.loop.set_exception_handler(lambda loop, context: self.captured.append(context))
        ObservedStore.instances = []
        ObservedStore.ack_loss = set()
        ObservedStore.after_end = None
        p = patch("bot_call.runtime.Store", ObservedStore)
        p.start()
        self.addCleanup(p.stop)
        self.lease = __import__(
            "bot_call.sdk_privacy", fromlist=["ProviderLogPrivacy"]
        ).ProviderLogPrivacy()

    async def asyncTearDown(self):
        for gate in self.gates:
            gate.set()
        for runtime in reversed(self.runtimes):
            self.assertTrue((await runtime.shutdown(timeout=3)).stopped)
            self.assertFalse(runtime._thread.is_alive())
            self.assertFalse(runtime._core_tasks)
        for store in ObservedStore.instances:
            self.assertEqual(store.access_threads, {store.owner})
            self.assertNotEqual(store.owner, self.business_thread)
            self.assertEqual(store.closed_thread, store.owner)
        self.loop.set_exception_handler(self.old_handler)
        self.lease.close()
        self.assertEqual(self.unexpected, [])
        self.assertEqual(self.violations, [])
        self.assertEqual(self.captured, [])
        await super().asyncTearDown()

    def policy(self, bound, provider_id, model):
        return ReviewRequestPolicy(
            bound, provider_id, model, 2900, 300, 100, NATIVE_CONTRACT, self.counter
        )

    def environment(self, targets):
        providers = {}
        for target, identifier, model in targets:
            event, provider = NativeCase.environment(
                self, target.platform_id, target.conversation_id
            )
            self.context.by_id.pop(provider.meta().id)
            provider.provider_config.update(id=identifier, max_context_tokens=2900)
            provider.model = model
            self.context.by_id[identifier] = provider
            providers[identifier] = provider
            self.context.providers[target.unified_msg_origin] = provider

            async def respond(kwargs, provider=provider):
                self.on_business()
                data = json.loads(kwargs["contexts"][1]["content"])
                self.public_calls.append(
                    (data["call_id"], data["source_key"], provider.get_model(), NATIVE_CONTRACT)
                )
                self.assertNotIn("max_tokens", kwargs)
                self.assertNotIn("max_completion_tokens", kwargs)
                return response(
                    FINAL if data.get("kind") == "final" else "已知生成笔记，非用户原话"
                )

            provider.responder = respond
        return self.context, providers

    def router(self, context, resolve, bind_calls=None):
        async def bind_adapter(strategy):
            self.on_business()
            if bind_calls is not None:
                bind_calls.append(strategy.binding.call_id)
            return await AstrBotReviewAdapter.bind(context, strategy, privacy=self.lease)

        return ReviewPolicyRouter(resolve, bind_adapter)

    def on_business(self):
        if (
            asyncio.get_running_loop() is not self.loop
            or threading.get_ident() != self.business_thread
        ):
            self.violations.append("wrong business loop/thread")

    async def forbidden(self, *args):
        self.unexpected.append("unexpected callback")
        raise AssertionError("must not run")

    def runtime(self, *, router=None, old_mode=None, path=None):
        async def reply(bound, turn):
            self.on_business()
            return "原Bot建议，播放未知"

        async def send(bound, page):
            self.on_business()
            return Outcome.CONFIRMED

        async def readable(bound, key, body):
            self.on_business()
            self.assertEqual(body, FINAL)
            readback = body  # Independent synthetic model input boundary.
            return Outcome.CONFIRMED if readback == body else Outcome.UNCERTAIN

        result = CallRuntime(
            path or self.path,
            RuntimeCallbacks(reply, self.forbidden, send, readable),
            review_work=old_mode,
            review_routes=router,
            char_budget=145,
        )
        self.runtimes.append(result)
        return result

    async def ended(self, runtime, target, text=PRIVATE):
        await runtime.create_call(target, background="old background is not input")
        await runtime.submit_utterance(target.call_id, "u", text)
        await runtime.drain()
        await runtime.end_call(target.call_id, prompt=PRIVATE)
        await runtime.drain()

    def private_error(self, error):
        self.assertNotIn(PRIVATE, repr(error) + "".join(traceback.format_exception(error)))
        self.assertTrue(error.__suppress_context__)

    async def test_two_calls_isolated_unavailable_call_does_not_block_other_and_ack_cancel(self):
        a, b = binding("A", "qq-A", "cid-A"), binding("B", "qq-B", "cid-B")
        context, providers = self.environment([(a, "pa", "model-A"), (b, "pb", "model-B")])
        available = {"A": False, "B": True}
        resolver_calls = []

        async def resolve(bound, saved):
            self.on_business()
            resolver_calls.append(bound.call_id)
            if not available[bound.call_id]:
                raise RuntimeError(PRIVATE)
            strategy = (
                saved.restore(bound, self.registry)
                if saved
                else self.policy(
                    bound, "pa" if bound.call_id == "A" else "pb", "model-" + bound.call_id
                )
            )
            return ReviewPolicySelection(strategy, RULE)

        runtime = self.runtime(router=self.router(context, resolve))
        await runtime.start()
        await self.ended(runtime, a)
        state_a = await runtime.read_state("A")
        self.assertEqual(state_a.review_policy.status, "unavailable")
        self.assertEqual(state_a.review.state, "pending")
        self.assertIsNone(state_a.review_work)
        await self.ended(runtime, b)
        state_b = await runtime.read_state("B")
        self.assertEqual(state_b.review_policy.status, "complete")
        self.assertTrue(state_b.publication.complete)
        self.assertEqual({call for call, key, model, cap in self.public_calls}, {"B"})
        # Replaying end with page cancellation must not cancel resolver ownership.
        available["A"] = True
        holder = {}
        ObservedStore.after_end = lambda call: (
            self.loop.call_soon_threadsafe(holder["page"].cancel) if call == "A" else None
        )
        holder["page"] = asyncio.create_task(runtime.end_call("A", prompt=PRIVATE))
        with self.assertRaises(asyncio.CancelledError):
            await holder["page"]
        ObservedStore.after_end = None
        await runtime.drain()
        state_a = await runtime.read_state("A")
        self.assertTrue(state_a.publication.complete)
        self.assertEqual(state_a.review.body, state_b.review.body)
        self.assertNotEqual(state_a.review_work.manifest, state_b.review_work.manifest)
        self.assertEqual(state_a.review_work.manifest.binding, a)
        self.assertEqual(state_b.review_work.manifest.binding, b)
        self.assertNotIn(PRIVATE, repr(state_a))
        self.assertNotIn("model-A", repr(state_a.review_policy))
        import json

        manifest_data = json.loads(state_a.review_work.manifest.canonical_json)
        self.assertEqual(
            set(manifest_data),
            {
                "version",
                "binding",
                "provider_id",
                "model",
                "context_window",
                "input_token_budget",
                "output_token_limit",
                "safety_token_reserve",
                "cap_parameter",
                "counter_rule_id",
                "payload",
            },
        )
        self.assertNotIn(PRIVATE, state_a.review_work.manifest.canonical_json)
        for call, key, model, cap in self.public_calls:
            self.assertEqual(model, "model-" + call)
            self.assertEqual(cap, NATIVE_CONTRACT)
        before = list(self.public_calls)
        await asyncio.gather(
            runtime.resume_pending(),
            runtime.resume_pending(),
            runtime.end_call("B", prompt=PRIVATE),
        )
        await runtime.drain()
        self.assertEqual(self.public_calls, before)
        self.assertEqual(self.counter.threads, {ObservedStore.instances[0].owner})

    async def test_v4_migration_atomic_policy_plan_commit_ack_loss_and_resolver_shutdown(self):
        tables = (
            "calls",
            "turns",
            "generations",
            "reviews",
            "publications",
            "publication_pages",
            "review_work",
            "review_work_nodes",
            "review_work_stages",
        )
        with Store(self.path) as seed:
            target = binding("legacy")
            seed.create_call(target, background="legacy background")
            seed.append_user("legacy", "u", PRIVATE, source="text")
            seed.end_call("legacy", prompt=PRIVATE)
            prepare_review_work(seed, "legacy", input_token_budget=2500, count_tokens=len)

            async def fake(node):
                return "笔记" if node.kind == "map" else FINAL

            await advance_review_work(seed, "legacy", fake, count_tokens=len, max_steps=10)
            seed.prepare_publication("legacy")
            old = {
                name: [tuple(row) for row in seed._db.execute("SELECT * FROM " + name).fetchall()]
                for name in tables
            }
        legacy = self.path.with_name("v4.db")
        sentinel = self.path.with_name("keep-existing-file.bin")
        sentinel.write_bytes(b"existing directory bytes")
        with closing(sqlite3.connect(legacy)) as raw:
            for statement in (*_SCHEMA, *_REVIEW_SCHEMA, *_PUBLICATION_SCHEMA, *_WORK_SCHEMA):
                raw.execute(statement)
            raw.execute(f"PRAGMA application_id={_APPLICATION_ID}")
            raw.execute("PRAGMA user_version=4")
            for name, rows in old.items():
                for row in rows:
                    raw.execute(
                        "INSERT INTO " + name + " VALUES (" + ",".join("?" for _ in row) + ")", row
                    )
            raw.commit()
            catalog = raw.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name"
            ).fetchall()
        with patch.object(
            storage, "_MANIFEST_SCHEMA", (*storage._MANIFEST_SCHEMA, "INVALID LAST MIGRATION")
        ):
            with self.assertRaises(sqlite3.Error):
                Store(legacy)
        with closing(sqlite3.connect(legacy)) as raw:
            self.assertEqual(raw.execute("PRAGMA user_version").fetchone()[0], 4)
            self.assertEqual(
                raw.execute(
                    "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name"
                ).fetchall(),
                catalog,
            )
            for name, rows in old.items():
                self.assertEqual(raw.execute("SELECT * FROM " + name).fetchall(), rows)
        self.assertEqual(sentinel.read_bytes(), b"existing directory bytes")
        with Store(legacy) as upgraded:
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], 24)
            for name, rows in old.items():
                self.assertEqual(
                    [
                        tuple(row)
                        for row in upgraded._db.execute("SELECT * FROM " + name).fetchall()
                    ],
                    rows,
                )
            self.assertIsNone(upgraded.get_review_manifest("legacy"))
            target = binding("atomic")
            strategy = self.policy(target, "pa", "model-A")
            manifest = ReviewManifest.from_policy(strategy, RULE)
            upgraded.create_call(target)
            upgraded.append_user("atomic", "u", PRIVATE, source="text")
            job = upgraded.end_call("atomic", prompt=PRIVATE)
            denials = [1]

            def reject_commit(action, operation, unused, database, trigger):
                if action == sqlite3.SQLITE_TRANSACTION and operation == "COMMIT" and denials[0]:
                    denials[0] -= 1
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            upgraded._db.set_authorizer(reject_commit)
            try:
                try:
                    prepare_review_work(
                        upgraded,
                        "atomic",
                        input_token_budget=strategy.input_token_budget,
                        count_tokens=strategy.count_request,
                        manifest=manifest,
                    )
                except ReviewWorkError as error:
                    self.private_error(error)
                else:
                    self.fail("denied COMMIT pretended durable")
            finally:
                upgraded._db.set_authorizer(None)
            self.assertEqual(denials, [0])
            self.assertEqual(upgraded.get_review("atomic"), job)
            self.assertIsNone(upgraded.get_review_work("atomic"))
            self.assertIsNone(upgraded.get_review_manifest("atomic"))
            upgraded._db.execute(
                "CREATE TRIGGER fail_node BEFORE INSERT ON review_work_nodes WHEN NEW.call_id='atomic' BEGIN SELECT RAISE(ABORT,'synthetic'); END"
            )
            with self.assertRaises(ReviewWorkError):
                prepare_review_work(
                    upgraded,
                    "atomic",
                    input_token_budget=strategy.input_token_budget,
                    count_tokens=strategy.count_request,
                    manifest=manifest,
                )
            upgraded._db.execute("DROP TRIGGER fail_node")
            self.assertEqual(upgraded.get_review("atomic"), job)
            self.assertIsNone(upgraded.get_review_manifest("atomic"))
            self.assertIsNone(upgraded.get_review_work("atomic"))
        # A committed manifest/plan with local ACK loss is recovered from DB.
        target, stubborn_target = binding("ack"), binding("stubborn")
        context, providers = self.environment(
            [(target, "pa", "model-A"), (stubborn_target, "pa", "model-A")]
        )

        async def resolve(bound, saved):
            self.on_business()
            return ReviewPolicySelection(
                saved.restore(bound, self.registry)
                if saved
                else self.policy(bound, "pa", "model-A"),
                RULE,
            )

        runtime = self.runtime(
            router=self.router(context, resolve), path=self.path.with_name("ack.db")
        )
        ObservedStore.ack_loss.add("ack")
        await runtime.start()
        await self.ended(runtime, target)
        state = await runtime.read_state("ack")
        self.assertEqual(state.review_policy.status, "persistence_unconfirmed")
        self.assertEqual(state.review_work.state, "pending")
        self.assertIsNotNone(state.review_work.manifest)
        saved_manifest, saved_plan = state.review_work.manifest, state.review_work.plan_json
        self.assertEqual(self.public_calls, [])
        await runtime.shutdown()
        runtime = self.runtime(
            router=self.router(context, resolve), path=self.path.with_name("ack.db")
        )
        await runtime.start()
        await runtime.resume_pending()
        await runtime.drain()
        state = await runtime.read_state("ack")
        self.assertEqual(state.review_work.manifest, saved_manifest)
        self.assertEqual(state.review_work.plan_json, saved_plan)
        self.assertTrue(state.publication.complete)
        await runtime.shutdown()
        entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.gates.append(release)

        async def stubborn_resolve(bound, saved):
            self.on_business()
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
            return await resolve(bound, saved)

        path = self.path.with_name("resolver.db")
        runtime = self.runtime(router=self.router(context, stubborn_resolve), path=path)
        await runtime.start()
        await runtime.create_call(stubborn_target)
        await runtime.submit_utterance("stubborn", "u", PRIVATE)
        await runtime.drain()
        await runtime.end_call("stubborn", prompt=PRIVATE)
        await asyncio.wait_for(entered.wait(), 3)
        before = list(self.public_calls)
        report = await runtime.shutdown(timeout=0.03)
        await asyncio.wait_for(cancelled.wait(), 2)
        self.assertEqual((report.stopped, report.error_code), (False, "still_stopping"))
        self.assertTrue(runtime._thread.is_alive())
        self.assertTrue(runtime._core_tasks)
        self.assertIsNone(ObservedStore.instances[-1].closed_thread)
        with closing(sqlite3.connect(path)) as raw:
            self.assertEqual(raw.execute("SELECT state FROM reviews").fetchone()[0], "pending")
            self.assertEqual(raw.execute("SELECT count(*) FROM review_work").fetchone()[0], 0)
            self.assertEqual(
                raw.execute("SELECT count(*) FROM review_policy_manifests").fetchone()[0], 0
            )
        indefinite = asyncio.create_task(runtime.shutdown(timeout=None))
        heartbeat = asyncio.Event()
        self.loop.call_soon(heartbeat.set)
        await heartbeat.wait()
        self.assertFalse(indefinite.done())
        release.set()
        self.assertTrue((await indefinite).stopped)
        self.assertEqual(self.public_calls, before)
        with closing(sqlite3.connect(path)) as raw:
            self.assertEqual(
                raw.execute("SELECT count(*) FROM review_policy_manifests").fetchone()[0], 0
            )
