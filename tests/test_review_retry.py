"""Synthetic providers, additive migration, retries and public dashboard contract."""

import asyncio
import hashlib
from pathlib import Path
import sqlite3
from types import SimpleNamespace as NS
from unittest.mock import patch, AsyncMock
import unittest

from bot_call.access import session_reference
from bot_call.benchmark_run import BenchmarkController
from bot_call.review_retry import latest, AttemptStore
from bot_call.store import Store
from test_backend import NativeCase, PublicProvider


class RetryTests(NativeCase):
    async def failed_call(self, error):
        event, provider = self.environment()
        backend, native = await self.backend()
        handle, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "u", "用户尚未决定")
        await backend.drain()
        with patch.object(self.context, "llm_generate", side_effect=error):
            await backend.web_end(web.token, web.reference)
            await backend.drain()
        cid = (await backend.read_state(handle)).binding.call_id
        return event, provider, backend, native, cid

    def controller(self, backend, native):
        return BenchmarkController(
            NS(
                ready=True,
                _config={"enabled": True},
                _terminating=False,
                _backend=backend,
                _native=native,
            )
        )

    async def test_rejection_codes_cross_bridge_and_parent_failed(self):
        for error, code in (
            (RuntimeError("Error code: 403 - Free quota exhausted"), "balance"),
            (RuntimeError("HTTP 401 invalid key"), "auth"),
            (RuntimeError("insufficient balance"), "balance"),
            (RuntimeError("Error code: 404 model not found"), "model"),
            (RuntimeError("HTTP 400 request rejected"), "rejected"),
        ):
            with self.subTest(code=code):
                _, _, backend, _, cid = await self.failed_call(error)
                state = await backend._runtime.read_state(cid)
                self.assertEqual(state.review.state, "failed")
                self.assertEqual(state.review.error_code, code)
                node = state.review_work.frontier[0]
                self.assertEqual((node.state, node.error_code), ("failed", code))
                data = await backend._runtime.benchmark_reviews()
                item = next(
                    r
                    for r in data["calls"]
                    if r["ref"] == session_reference(hashlib.sha256(cid.encode()).hexdigest())
                )
                self.assertEqual(item["reason"], "model_failed")
                self.assertTrue(item["retryable"])
                self.assertIn("detail", item)
                self.assertIn("model", item)
                await backend.shutdown()

    async def test_timeout_interrupt_unknown_and_default_no_replay(self):
        for error in (TimeoutError(), ConnectionResetError(), asyncio.CancelledError()):
            _, provider, backend, native, cid = await self.failed_call(error)
            state = await backend._runtime.read_state(cid)
            self.assertIn(state.review_work.frontier[0].state, {"uncertain", "cancelled"})
            before = len(provider.calls)
            await backend.shutdown()
            reopened, native2 = await self.backend()
            await reopened._runtime.resume_pending()
            result = await reopened.backfill_reviews([cid], counters=native2.counters, apply=True)
            await reopened.drain()
            self.assertEqual(result["calls"][0]["reason"], "outcome_not_replayable")
            self.assertEqual(len(provider.calls), before)
            await reopened.shutdown()

    async def test_retry_preview_current_model_context_only_send_and_idempotency(self):
        _, old, backend, native, cid = await self.failed_call(TimeoutError())
        with Store(self.path / "calls.db") as store:
            old_node = tuple(
                store._db.execute(
                    "SELECT * FROM review_work_nodes WHERE call_id=?", (cid,)
                ).fetchone()
            )
            old_manifest = tuple(
                store._db.execute(
                    "SELECT * FROM review_policy_manifests WHERE call_id=?", (cid,)
                ).fetchone()
            )
            old_rows = {
                r[0]: r[1]
                for r in store._db.execute(
                    "SELECT name,sql FROM sqlite_master WHERE type='trigger'"
                )
            }
        current = PublicProvider("current-provider", "current-model")
        self.context.by_id["current-provider"] = current
        binding = (await backend._runtime.read_state(cid)).binding
        self.context.providers[binding.unified_msg_origin] = current
        controller = self.controller(backend, native)
        ref = session_reference(hashlib.sha256(cid.encode()).hexdigest())
        preview = await controller.retry_review(dict(action="retry_review", ref=ref, preview=True))
        self.assertEqual(preview["status"], "ok", preview)
        data = preview["data"]
        self.assertEqual(
            (data["model"], data["llm_calls_min"], data["llm_calls_max"]), ("current-model", 2, 2)
        )
        self.assertTrue(data["may_rebill"])
        self.assertTrue(data["context_only"])
        self.assertIn(old.model, data["notes"][0])
        self.assertEqual(current.calls, [])
        with Store(self.path / "calls.db") as store:
            self.assertIsNone(latest(store, cid))
        applied = await controller.retry_review(dict(action="retry_review", ref=ref, preview=False))
        self.assertEqual(applied["data"]["state"], "running", applied)
        await backend.drain()
        with Store(self.path / "calls.db") as store:
            row = latest(store, cid)
            self.assertEqual(row["state"], "succeeded")
            self.assertEqual(row["original_model"], old.model)
            view = AttemptStore(store, row["attempt_id"])
            pub = view.get_publication(cid)
            self.assertEqual(pub.context_state, "succeeded")
            self.assertTrue(all(p.state == "pending" for p in pub.pages))
            self.assertEqual(
                tuple(
                    store._db.execute(
                        "SELECT * FROM review_work_nodes WHERE call_id=?", (cid,)
                    ).fetchone()
                ),
                old_node,
            )
            self.assertEqual(
                tuple(
                    store._db.execute(
                        "SELECT * FROM review_policy_manifests WHERE call_id=?", (cid,)
                    ).fetchone()
                ),
                old_manifest,
            )
            for key, value in old_rows.items():
                self.assertEqual(
                    store._db.execute(
                        "SELECT sql FROM sqlite_master WHERE name=?", (key,)
                    ).fetchone()[0],
                    value,
                )
            with self.assertRaises(sqlite3.IntegrityError):
                store._db.execute(
                    "UPDATE review_work_nodes SET state='pending' WHERE call_id=?", (cid,)
                )
        self.assertEqual(len(current.calls), 2)
        self.assertFalse(self.sent)
        item = (await backend._runtime.benchmark_reviews())["calls"][0]
        self.assertEqual(item["detail"], "已补做，按选择只写入聊天记忆，未发 QQ")
        writes = len(self.context.conversation_manager.writes)
        repeated = await controller.retry_review(
            dict(action="retry_review", ref=ref, preview=False)
        )
        self.assertEqual(repeated["data"]["state"], "succeeded", repeated)
        result = await controller.retry_review(
            dict(action="retry_review", ref=ref, preview=False, send=True)
        )
        await backend.drain()
        self.assertEqual(result["status"], "ok", result)
        self.assertTrue(self.sent)
        sent = len(self.sent)
        await controller.retry_review(
            dict(action="retry_review", ref=ref, preview=False, send=True)
        )
        await backend.drain()
        self.assertEqual(
            (len(current.calls), len(self.sent), len(self.context.conversation_manager.writes)),
            (2, sent, writes),
        )
        item = (await backend._runtime.benchmark_reviews())["calls"][0]
        self.assertEqual((item["state"], item["retryable"]), ("succeeded", False))
        self.assertEqual(item["detail"], "已通过重试补做")

    async def test_concurrent_busy_not_found_not_retryable_and_explicit_backfill(self):
        event, provider, backend, native, cid = await self.failed_call(RuntimeError("HTTP 403"))
        controller = self.controller(backend, native)
        ref = session_reference(hashlib.sha256(cid.encode()).hexdigest())
        for wrong in ("nonexistent", cid, cid[:8]):
            result = await controller.retry_review(dict(action="retry_review", ref=wrong))
            self.assertEqual(result["data"]["code"], "not_found")
        with patch.object(backend._runtime, "active_call_count", AsyncMock(return_value=1)):
            result = await controller.retry_review(dict(action="retry_review", ref=ref))
            self.assertEqual(result["data"]["code"], "busy")
        entered, release = asyncio.Event(), asyncio.Event()
        self.gates.append(release)
        original = self.context.llm_generate

        async def blocked(**kwargs):
            entered.set()
            await release.wait()
            return await original(**kwargs)

        with patch.object(self.context, "llm_generate", side_effect=blocked):
            result = await controller.retry_review(
                dict(action="retry_review", ref=ref, preview=False)
            )
            await asyncio.wait_for(entered.wait(), 2)
            concurrent = await controller.retry_review(
                dict(action="retry_review", ref=ref, preview=False)
            )
            self.assertEqual(concurrent["data"]["code"], "running", concurrent)
            release.set()
            await backend.drain()
        # Unknown source / disabled are bounded errors with no raw IDs/text.
        controller.host.ready = False
        self.assertEqual((await controller.retry_review(dict(ref=ref)))["data"]["code"], "disabled")
        controller.host.ready = True
        with patch.object(
            backend, "retry_review", AsyncMock(return_value={"code": "not_retryable"})
        ):
            self.assertEqual(
                (await controller.retry_review(dict(ref=ref)))["data"]["code"], "not_retryable"
            )


class RetryMigrationTests(unittest.TestCase):
    def test_schema22_upgrade_retains_every_old_object_and_row(self):
        from tempfile import TemporaryDirectory
        from test_astrbot_context import binding

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "db"
            with Store(path) as store:
                store.create_call(binding("old"))
                store.append_user("old", "u", "旧材料", source="synthetic")
                store.end_call("old", prompt="原提示")
                db = store._db
                for ty, name in db.execute(
                    "SELECT type,name FROM sqlite_master WHERE name LIKE '%retry%' ORDER BY type DESC"
                ).fetchall():
                    if ty in {"trigger", "table"}:
                        db.execute(f'DROP {ty} "{name}"')
                db.execute("PRAGMA user_version=22")
                old_objects = db.execute(
                    "SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
                ).fetchall()
                old_rows = {
                    name: db.execute(f'SELECT * FROM "{name}"').fetchall()
                    for ty, name, _ in old_objects
                    if ty == "table"
                }
            with Store(path) as store:
                db = store._db
                self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 24)
                for ty, name, sql in old_objects:
                    self.assertEqual(
                        db.execute(
                            "SELECT sql FROM sqlite_master WHERE type=? AND name=?", (ty, name)
                        ).fetchone()[0],
                        sql,
                    )
                for name, rows in old_rows.items():
                    self.assertEqual(db.execute(f'SELECT * FROM "{name}"').fetchall(), rows)
                self.assertEqual(
                    db.execute("SELECT count(*) FROM review_retry_attempts").fetchone()[0], 0
                )
