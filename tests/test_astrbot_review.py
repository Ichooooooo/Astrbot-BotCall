"""Public review responses and frozen requests, with synthetic native Providers."""

import asyncio
import hashlib
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace as NS

from bot_call.astrbot_review import AstrBotReviewAdapter, ReviewRequestPolicy
from bot_call.native_provider import NATIVE_CONTRACT, call_payload, observed_finish
from bot_call.publication import Outcome
from bot_call.review import build_review_prompt
from bot_call.review_work import NotApplied
from bot_call.runtime import CallRuntime, RuntimeCallbacks
from bot_call.store import Store
from test_astrbot_context import binding
from test_backend import NativeCase, response, TEXT, BOT, FINAL


from copy import deepcopy
import subprocess
import sys
import threading
from unittest.mock import patch
from bot_call.review_work import prepare_review_work
from bot_call.review_policy import ReviewManifest
from bot_call.native_provider import COUNTER_RULE
from test_runtime_review_work import TrackedStore
from test_backend import OtherPublicProvider


class AstrBotReviewTests(NativeCase):
    async def test_preflight_and_incomplete_responses_never_publish_or_retry_cancel_waits(self):
        event, provider = self.environment()
        target = binding("base", "qq-A", "A")
        host = self.host()
        for mode in (
            "raw-none",
            "foreign-raw",
            "length",
            "content_filter",
            "tool_calls",
            "function_call",
            "failed",
            "cancelled",
            "queued",
            "in_progress",
            "incomplete",
            "chunk",
            "error",
            "cancel",
        ):
            with self.subTest(mode=mode):
                bound = replace(target, call_id=mode)
                counted = []

                def counter(text):
                    counted.append(text)
                    return len(text) // 4 + 1

                policy = ReviewRequestPolicy(
                    bound,
                    provider.meta().id,
                    provider.get_model(),
                    32000,
                    2048,
                    1024,
                    NATIVE_CONTRACT,
                    counter,
                )
                adapter = await AstrBotReviewAdapter.bind(
                    self.context, policy, privacy=host.privacy
                )

                async def respond(kwargs):
                    if mode == "error":
                        raise RuntimeError("synthetic-private-error")
                    if mode == "cancel":
                        raise asyncio.CancelledError()
                    if mode == "raw-none":
                        return response(FINAL)
                    if mode == "foreign-raw":
                        return response(FINAL, raw={"native_result": "opaque"})
                    if mode == "chunk":
                        return response(FINAL, chunk=True)
                    raw = (
                        NS(choices=[NS(finish_reason=mode)])
                        if mode in {"length", "content_filter", "tool_calls", "function_call"}
                        else NS(status=mode)
                    )
                    return response(FINAL, raw=raw)

                provider.responder = respond
                sent = []

                async def reply(*args):
                    return BOT

                async def no_legacy(*args):
                    raise AssertionError("legacy generate")

                async def send(bound, page):
                    sent.append(page.fragment)
                    return Outcome.SUBMITTED

                async def readable(*args):
                    return Outcome.CONFIRMED

                path = self.path / (mode + ".db")
                runtime = CallRuntime(
                    path,
                    RuntimeCallbacks(reply, no_legacy, send, readable),
                    review_work=adapter.runtime_mode(),
                    review_prompt=build_review_prompt(bounded_output=True),
                )
                await runtime.start()
                try:
                    await runtime.create_call(bound)
                    for index in range(2):
                        await runtime.submit_utterance(mode, "u" + str(index), TEXT + str(index))
                        await runtime.drain()
                    frozen = await runtime.end_call(mode)
                    await runtime.drain()
                    state = await runtime.read_state(mode)
                    self.assertEqual(state.review.input_json, frozen.input_json)
                    self.assertEqual(len(json.loads(frozen.input_json)), 4)
                    if mode in {"raw-none", "foreign-raw"}:
                        self.assertEqual(state.review.state, "succeeded")
                        self.assertTrue(state.publication.complete)
                        self.assertEqual("".join(sent), FINAL)
                        self.assertEqual(len(state.review_work.frontier), 1)
                    else:
                        self.assertIsNone(state.publication)
                        self.assertIsNone(state.review.body)
                        self.assertEqual(
                            state.review_work.frontier[0].state,
                            "cancelled" if mode == "cancel" else "uncertain",
                        )
                    before = len(provider.calls)
                    await runtime.resume_pending()
                    await runtime.drain()
                    self.assertEqual(len(provider.calls), before)
                    with Store(path) as saved:
                        rows = saved._db.execute(
                            "SELECT request_json,token_count FROM review_work_nodes WHERE call_id=? ORDER BY level,ordinal",
                            (mode,),
                        ).fetchall()
                    for request_json, token_count in rows:
                        payload = policy.payload_json(request_json)
                        self.assertIn(payload, counted)
                        self.assertEqual(token_count, len(payload) // 4 + 1)
                        public = json.loads(payload)
                        self.assertEqual(
                            set(public), {"chat_provider_id", "contexts", "request_max_retries"}
                        )
                        data = json.loads(public["contexts"][1]["content"])
                        self.assertEqual(data["input_sha256"], frozen.input_sha256)
                        self.assertEqual(
                            data["prompt_sha256"],
                            hashlib.sha256(frozen.prompt.encode()).hexdigest(),
                        )
                        if "fragments" in data:
                            self.assertEqual(
                                [f["turn"] for f in data["fragments"]],
                                json.loads(frozen.input_json),
                            )
                finally:
                    self.assertTrue((await runtime.shutdown(timeout=None)).stopped)
        legacy = replace(policy, cap_parameter="max_tokens")
        before = len(provider.calls)
        with self.assertRaises(NotApplied):
            await AstrBotReviewAdapter.bind(self.context, legacy, privacy=host.privacy)
        self.assertEqual(len(provider.calls), before)

    async def test_bound_providers_exact_payload_caps_and_borrowed_parent_survives(self):
        # Legacy name; caps now describe planning reserves, not SDK generation.
        host = self.host()
        bound = []
        providers = []
        nodes = []
        adapters = []
        for cid, platform, cls in [
            ("A", "qq-A", OtherPublicProvider),
            ("B", "qq-B", OtherPublicProvider),
        ]:
            event, provider = self.environment(platform, cid, cls)
            providers.append(provider)
            target = binding(cid, platform, cid)
            bound.append(target)
            policy = ReviewRequestPolicy(
                target,
                provider.meta().id,
                provider.get_model(),
                32000,
                2048,
                1024,
                NATIVE_CONTRACT,
                lambda text: len(text),
            )
            adapter = await AstrBotReviewAdapter.bind(self.context, policy, privacy=host.privacy)
            adapters.append(adapter)
            manifest = ReviewManifest.from_policy(policy, COUNTER_RULE)
            with Store(self.path / (cid + ".db")) as store:
                store.create_call(target)
                store.append_user(cid, "u", TEXT, source="synthetic")
                store.end_call(cid, prompt="original frozen prompt")
                work = prepare_review_work(
                    store,
                    cid,
                    input_token_budget=policy.input_token_budget,
                    count_tokens=policy.count_request,
                    manifest=manifest,
                )
                node, won = store.claim_review_work_node(
                    cid, work.frontier[0].source_key, manifest_sha256=manifest.fingerprint
                )
                self.assertTrue(won)
                nodes.append(node)

            async def reply(kwargs):
                return response(FINAL)

            provider.responder = reply
            before = deepcopy(provider.provider_config)
            self.assertEqual(await adapter.generate(node), FINAL)
            payload = json.loads(policy.payload_json(node.request_json))
            self.assertEqual(payload["contexts"], provider.calls[-1]["contexts"])
            self.assertEqual(payload["chat_provider_id"], provider.meta().id)
            self.assertEqual(node.token_count, len(policy.payload_json(node.request_json)))
            self.assertEqual((policy.output_token_limit, policy.safety_token_reserve), (2048, 1024))
            self.assertNotIn("max_tokens", provider.calls[-1])
            self.assertNotIn("max_completion_tokens", provider.calls[-1])
            self.assertEqual(provider.provider_config, before)
            self.assertFalse(hasattr(provider, "client"))
        adapter = adapters[0]
        node = nodes[0]
        provider = providers[0]
        attempts = len(provider.calls)
        for invalid in [
            nodes[1],
            replace(node, source_key="wrong"),
            replace(node, token_count=999999),
            replace(
                node,
                request_json=node.request_json.replace(
                    "original frozen prompt", "other frozen prompt"
                ),
            ),
        ]:
            with self.assertRaises(NotApplied):
                await adapter.generate(invalid)
        self.assertEqual(len(provider.calls), attempts)
        original = provider.model
        provider.model = "changed"
        with self.assertRaises(NotApplied):
            await adapter.generate(node)
        provider.model = original
        umo = bound[0].unified_msg_origin
        self.context.providers[umo] = providers[1]
        with self.assertRaises(NotApplied):
            await adapter.generate(node)
        self.context.providers[umo] = provider

        def wrong_loop():
            async def run():
                with self.assertRaises(NotApplied):
                    await adapter.generate(node)

            asyncio.run(run())

        await asyncio.to_thread(wrong_loop)
        self.assertEqual(len(provider.calls), attempts)
        for changes in [
            dict(context_window=0),
            dict(safety_token_reserve=0),
            dict(output_token_limit=32000),
            dict(cap_parameter="unknown"),
            dict(output_token_limit=True),
        ]:
            with self.assertRaises(NotApplied):
                replace(adapter.policy, **changes)
        for counter in [
            lambda payload: True,
            lambda payload: -1,
            lambda payload: (_ for _ in ()).throw(RuntimeError("private")),
        ]:
            with self.assertRaises(NotApplied):
                replace(adapter.policy, count_payload_tokens=counter).count_request(
                    node.request_json
                )
        script = """
import asyncio,sqlite3,socket,threading,os,sys
def deny(*a,**k):raise AssertionError('import side effect')
sqlite3.connect=socket.socket=threading.Thread.start=os.mkdir=deny
import bot_call.astrbot_review
assert 'openai' not in sys.modules
assert not any(n=='astrbot' or n.startswith('astrbot.') for n in sys.modules)
"""
        imported = subprocess.run(
            [sys.executable, "-B", "-c", script], capture_output=True, text=True
        )
        self.assertEqual(imported.returncode, 0, imported.stderr)

    async def test_runtime_long_call_counts_payload_on_worker_sends_once_on_business(self):
        event, provider = self.environment()
        provider.provider_config["max_context_tokens"] = 2900
        target = binding("long", "qq-A", "A")
        host = self.host()
        business = threading.get_ident()
        counted = {}
        threads = set()
        public = []
        sent = []
        imported = []

        def counter(payload):
            threads.add(threading.get_ident())
            counted[hashlib.sha256(payload.encode()).hexdigest()] = len(payload)
            return len(payload)

        policy = ReviewRequestPolicy(
            target,
            provider.meta().id,
            provider.get_model(),
            2900,
            300,
            100,
            NATIVE_CONTRACT,
            counter,
        )
        prompt = "PRIVATE_REVIEW_PROMPT 区分表达与建议\n 不改空白 "

        async def respond(kwargs):
            self.assertEqual(threading.get_ident(), business)
            data = json.loads(kwargs["contexts"][1]["content"])
            self.assertEqual(data["review_prompt"], prompt)
            public.append(data["source_key"])
            return response(
                FINAL
                if data.get("kind") == "final"
                else "分块笔记，非用户原话；开头/中段/结尾，Bot建议非决定。"
            )

        provider.responder = respond
        before = deepcopy(provider.provider_config)
        adapter = await AstrBotReviewAdapter.bind(self.context, policy, privacy=host.privacy)

        async def reply(*args):
            return "Bot建议尚未决定；播放未知。"

        async def forbidden(*args):
            raise AssertionError("legacy review")

        async def send(bound, page):
            sent.append(page.fragment)
            return Outcome.CONFIRMED

        async def readable(bound, key, body):
            imported.append((bound, key, body))
            return Outcome.CONFIRMED

        TrackedStore.instances = []
        TrackedStore.faults = {}
        TrackedStore.before_begin = TrackedStore.after_end = None
        path = self.path / "long-review.db"
        with patch("bot_call.runtime.Store", TrackedStore):
            runtime = CallRuntime(
                path,
                RuntimeCallbacks(reply, forbidden, send, readable),
                review_work=adapter.runtime_mode(),
                char_budget=145,
            )
            await runtime.start()
            try:
                await runtime.create_call(target)
                for key, text in [
                    ("opening", " 开头家庭🙂 e\u0301\r\n " * 400),
                    ("middle", "中段犹豫\t " * 400),
                    ("ending", "结尾未定\n " * 400),
                ]:
                    await runtime.submit_utterance("long", key, text)
                    await runtime.drain()
                job = await runtime.end_call("long", prompt=prompt)
                await runtime.drain()
                state = await runtime.read_state("long")
                self.assertEqual(state.review_work.state, "succeeded")
                self.assertGreater(state.review_work.map_count, 4)
                self.assertGreaterEqual(state.review_work.level, 2)
                self.assertTrue(state.publication.complete)
                self.assertEqual(state.review.body, FINAL)
                self.assertEqual(state.review.input_json, job.input_json)
                self.assertEqual("".join(sent), FINAL)
                self.assertEqual(imported, [(target, state.publication.context_key, FINAL)])
                for kwargs in provider.calls:
                    # Reconstruct the documented public-call projection, not SDK bytes.
                    payload = json.dumps(
                        dict(
                            chat_provider_id=provider.meta().id,
                            contexts=kwargs["contexts"],
                            request_max_retries=1,
                        ),
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                    digest = hashlib.sha256(payload.encode()).hexdigest()
                    self.assertIn(digest, counted)
                    self.assertLessEqual(counted[digest], policy.input_token_budget)
                self.assertEqual(len(public), len(set(public)))
                self.assertLessEqual(len(public), 2 * state.review_work.map_count)
                self.assertEqual(threads, {TrackedStore.instances[0].owner})
                self.assertNotIn(business, threads)
                before_calls = len(public)
                await runtime.end_call("long", prompt=prompt)
                await runtime.resume_pending()
                await runtime.drain()
                self.assertEqual(len(public), before_calls)
            finally:
                await runtime.shutdown(timeout=None)
            observed = TrackedStore.instances[0]
            self.assertEqual(observed.access_threads, {observed.owner})
            self.assertEqual(observed.closed_thread, observed.owner)
        self.assertEqual(provider.provider_config, before)
        with Store(path) as store:
            frozen = json.loads(store.get_review("long").input_json)
            self.assertEqual(
                [t["material_id"] for t in frozen if t["role"] == "user"],
                ["opening", "middle", "ending"],
            )
