"""Long/series closure and explicit recovery: synthetic providers only."""

import asyncio
import json
from unittest.mock import patch

from test_backend import NativeCase, FINAL, response
from test_segments import StreamingProvider
from bot_call.review_plan import build_review_plan
from bot_call.store import Store


class ReviewPendingTests(NativeCase):
    async def test_long_series_close_inflight_segment(self):
        event, provider = self.environment(provider_class=StreamingProvider)
        backend, host = await self.backend()
        handle, web = await self.enter(backend, event)
        for n in range(10):
            await backend.web_submit(web.token, web.reference, f"u{n}", f"第{n}轮未决定")
            await backend.drain()
        await backend.web_audio(
            web.token, web.reference, "series", b"audio", "audio/webm", ("series", 1, False)
        )
        await backend.drain()
        provider.gate = asyncio.Event()
        self.gates.append(provider.gate)
        await backend.web_audio(
            web.token, web.reference, "series-2", b"audio", "audio/webm", ("series", 2, True)
        )
        for _ in range(200):
            state = await backend.read_state(handle)
            if any(
                r["generation_id"] == "reply:series" and r["seq"] == 1 and r["state"] == "succeeded"
                for r in (await backend.web_state(web.token, web.reference)).segments
            ):
                break
            await asyncio.sleep(0.005)
        else:
            self.fail("inflight segment did not start")
        await backend.web_end(web.token, web.reference)
        self.assertIsNone((await backend.read_state(handle)).review)
        provider.gate.set()
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual(state.review.state, "succeeded")
        self.assertTrue(state.publication.complete)
        self.assertEqual(state.review.body, FINAL)
        self.assertGreater(state.review.last_sequence, len(json.loads(state.review.input_json)))
        self.assertIn(
            FINAL,
            json.dumps(self.context.conversation_manager.items["A"].history, ensure_ascii=False),
        )

    async def test_annotation_budget_keeps_full_frozen_material(self):
        from test_astrbot_context import binding

        with Store(self.path / "annotation.db") as store:
            store.create_call(binding("annotations"))
            store.append_user("annotations", "u", "正文", source="synthetic")
            suffix = (
                "\n本通逐句公开文字及播放观测（与正文重复的只计一次；不代表听到或接受）："
                + "公开文字未知播放。" * 1800
            )
            job = store.end_call("annotations", prompt="冻结的写作要求" + suffix)
            from bot_call.astrbot_review import ReviewRequestPolicy
            from bot_call.native_provider import NATIVE_CONTRACT
            from bot_call.review_work import prepare_review_work, advance_review_work

            policy = ReviewRequestPolicy(
                binding("annotations"), "synthetic", "model", 6000, 500, 500, NATIVE_CONTRACT, len
            )
            plan = build_review_plan(
                job, input_token_budget=5000, count_tokens=policy.count_request
            )
            fragments = [f for c in plan.chunks for f in c.fragments]
            self.assertEqual(
                "".join(
                    f.turn.body for f in fragments if f.turn.source == "frozen_review_annotations"
                ),
                suffix,
            )
            self.assertTrue(all(c.token_count <= 5000 for c in plan.chunks))
            work = prepare_review_work(
                store, "annotations", input_token_budget=5000, count_tokens=policy.count_request
            )
            seen = []

            async def generate(node):
                policy.payload_json(node.request_json)  # exact adapter validation, all stages
                seen.append(node.kind)
                return "公开事实和未知播放状态" if node.kind != "final" else FINAL

            await advance_review_work(
                store,
                "annotations",
                generate,
                count_tokens=policy.count_request,
                max_steps=2 * work.map_count,
            )
            self.assertEqual(store.get_review("annotations").state, "succeeded")
            self.assertEqual(store.get_review("annotations").prompt, job.prompt)
            self.assertEqual(store.get_review("annotations").input_sha256, job.input_sha256)
            self.assertEqual(seen[-1], "final")

    async def test_explicit_reload_backfill_preview_exact_ids_context_only_and_no_replay(self):
        from bot_call.review_policy import ReviewPolicyUnavailable
        from bot_call.review_work import prepare_review_work

        event, provider = self.environment()
        backend, host = await self.backend()
        calls = []
        for name in ("chosen", "unselected", "uncertain"):
            handle, web = await self.enter(backend, event)
            await backend.web_submit(web.token, web.reference, name, "保留完整材料")
            await backend.drain()
            with patch.object(backend._router, "resolve", side_effect=ReviewPolicyUnavailable()):
                with self.assertLogs("bot_call.runtime", level="INFO") as logs:
                    await backend.web_end(web.token, web.reference)
                    await backend.drain()
            self.assertIn("stage=resolve reason=unavailable", "\n".join(logs.output))
            state = await backend.read_state(handle)
            self.assertEqual(state.review.state, "pending")
            calls.append(state.binding.call_id)
        await backend.shutdown()
        with Store(self.path / "calls.db") as store:
            saved = store.get_call_policy(calls[2]).review_manifest
            policy = saved.restore(store.get_binding(calls[2]), host.counters)
            work = prepare_review_work(
                store,
                calls[2],
                input_token_budget=policy.input_token_budget,
                count_tokens=policy.count_request,
                manifest=saved,
            )
            node, _ = store.claim_review_work_node(
                calls[2], work.frontier[0].source_key, manifest_sha256=saved.fingerprint
            )
            store.finish_review_work_node(
                calls[2], node.source_key, state="uncertain", manifest_sha256=saved.fingerprint
            )
        reopened, native = await self.backend()
        await reopened._runtime.resume_pending()
        await reopened.drain()
        diag = await reopened._runtime.diagnostic_reviews()
        self.assertEqual(diag["pending_count"], 3)
        self.assertEqual(diag["reasons"], {"scope_required": 2, "outcome_uncertain": 1})
        before = len(provider.calls), len(self.sent), len(self.context.conversation_manager.writes)
        preview = await reopened.backfill_reviews([calls[0]], counters=native.counters)
        self.assertEqual(preview["calls"][0]["llm_calls_min"], 2)
        self.assertEqual(
            (len(provider.calls), len(self.sent), len(self.context.conversation_manager.writes)),
            before,
        )
        for ids in ([], [calls[0], calls[0]], [calls[0], "unknown"], [calls[0][:8]]):
            with self.assertRaises(Exception):
                await reopened.backfill_reviews(ids, counters=native.counters, apply=True)
        self.assertEqual(reopened._calls, {})
        await reopened.backfill_reviews([calls[0], calls[2]], counters=native.counters, apply=True)
        await reopened.drain()
        state, _ = await reopened._runtime.inspect_frozen_call(calls[0])
        self.assertEqual(state.review.state, "succeeded")
        self.assertEqual(state.publication.context_state, "succeeded")
        self.assertTrue(all(p.state == "pending" for p in state.publication.pages))
        self.assertEqual(len(provider.calls) - before[0], 2)
        self.assertEqual(self.sent, [])
        unselected, _ = await reopened._runtime.inspect_frozen_call(calls[1])
        unknown, _ = await reopened._runtime.inspect_frozen_call(calls[2])
        self.assertEqual(unselected.review.state, "pending")
        self.assertEqual(unknown.review_work.frontier[0].state, "uncertain")
        after = len(provider.calls), len(self.context.conversation_manager.writes)
        await reopened.backfill_reviews([calls[0]], counters=native.counters, apply=True)
        await reopened.drain()
        self.assertEqual(
            (len(provider.calls), len(self.context.conversation_manager.writes)), after
        )
        self.assertEqual(self.sent, [])
        await reopened.backfill_reviews(
            [calls[0]], counters=native.counters, context_only=False, apply=True
        )
        await reopened.drain()
        self.assertTrue(self.sent)
        self.assertEqual(len(provider.calls), after[0])

    async def test_slow_call_does_not_block_later_review_and_fixed_prepare_diagnostic(self):
        from bot_call.review_router import DetachedReviewRoute

        event, provider = self.environment()
        backend, host = await self.backend()
        handle, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "slow", "不能出现在日志的正文")
        await backend.drain()
        slow = (await backend.read_state(handle)).binding.call_id
        entered, release = asyncio.Event(), asyncio.Event()
        self.gates.append(release)
        original = backend._router.resolve

        async def resolve(bound, saved):
            if bound.call_id == slow:
                entered.set()
                await release.wait()
                route = await original(bound, saved)

                def broken(_):
                    raise ValueError("不能出现在日志的正文")

                return DetachedReviewRoute(route.manifest, broken)
            return await original(bound, saved)

        with patch.object(backend._router, "resolve", new=resolve):
            with self.assertLogs("bot_call.runtime", level="INFO") as logs:
                await backend.web_end(web.token, web.reference)
                await asyncio.wait_for(entered.wait(), 2)
                fast, fw = await self.enter(backend, event)
                await backend.web_submit(fw.token, fw.reference, "fast", "新通话")
                # drain would intentionally wait for the slow call; observe just fast reply.
                for _ in range(200):
                    state = await backend.read_state(fast)
                    if state.generations and state.generations[0].state == "succeeded":
                        break
                    await asyncio.sleep(0.005)
                await backend.web_end(fw.token, fw.reference)
                for _ in range(200):
                    state = await backend.read_state(fast)
                    if state.publication and state.publication.complete:
                        break
                    await asyncio.sleep(0.005)
                self.assertTrue(state.publication and state.publication.complete)
                release.set()
                await backend.drain()
            output = "\n".join(logs.output)
            self.assertIn("stage=prepare reason=counter_failed", output)
            self.assertNotIn("不能出现在日志的正文", output)
        diag = await backend._runtime.diagnostic_reviews()
        self.assertEqual(diag["reasons"], {"counter_failed": 1})

    async def test_reload_and_new_call_never_adopt_historical_pending(self):
        from bot_call.review_policy import ReviewPolicyUnavailable

        event, provider = self.environment()
        original, _ = await self.backend()
        old_handle, old_web = await self.enter(original, event)
        await original.web_submit(old_web.token, old_web.reference, "old", "历史材料")
        await original.drain()
        with patch.object(original._router, "resolve", side_effect=ReviewPolicyUnavailable()):
            await original.web_end(old_web.token, old_web.reference)
            await original.drain()
        old = await original.read_state(old_handle)
        self.assertEqual(old.review.state, "pending")
        await original.shutdown()
        reopened, _ = await self.backend()
        fresh, web = await self.enter(reopened, event)
        await reopened.web_submit(web.token, web.reference, "new", "新版新通话")
        await reopened.drain()
        await reopened.web_end(web.token, web.reference)
        await reopened.drain()
        self.assertTrue((await reopened.read_state(fresh)).publication.complete)
        kept, _ = await reopened._runtime.inspect_frozen_call(old.binding.call_id)
        self.assertEqual(kept.review, old.review)
        self.assertIsNone(kept.review_work)
        self.assertIsNone(kept.publication)
        self.assertEqual(
            (await reopened._runtime.diagnostic_reviews())["reasons"], {"scope_required": 1}
        )
        self.assertTrue(all(b.call_id != old.binding.call_id for b, _ in self.sent))
