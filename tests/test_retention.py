"""Retention exercises real SQLite triggers, publication state, and runtime timers."""

import asyncio
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from time import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from bot_call.access import InvitationSecret
from bot_call.backend import BackendConfig, BackendError
from bot_call.limits import RuntimeLimits
from bot_call.retention import audio_progress_trigger, reclaim_space
from bot_call.review_retry import AttemptStore
from bot_call.runtime import CallRuntime, RuntimeCallbacks
from bot_call.store import Store
from test_astrbot_context import binding


DAY = 86400
NOW = 1_730_000_000
OLD = NOW - 8 * DAY
RAW = b"synthetic original audio"
SYNTHESIZED = b"synthetic output audio"
GID = "reply:u"
RAW_FAILURES = {
    "failed": "not_applied",
    "cancelled": "result_cancelled",
    "uncertain": "result_uncertain",
}


def add_call(store, call_id, *, text=True, failure="failed", resolved=True, data=RAW):
    with patch("bot_call.store.time", return_value=OLD):
        store.create_call(binding(call_id), retained_missing=True)
        store.receive_audio(call_id, "raw", data, "audio/webm")
        store.claim_next_audio(call_id)
        error = RAW_FAILURES.get(failure, "empty_transcription")
        state = failure if failure in RAW_FAILURES else "failed"
        store.finish_audio(call_id, "raw", state=state, error_code=error)
        if resolved and failure in RAW_FAILURES:
            store.retain_audio_missing(call_id, "raw")
        if text:
            store.append_user(call_id, "u", "来电内容", source="synthetic")
            store.claim_generation(call_id, GID, utterance_id="u", source="synthetic")
            store.complete_generation(call_id, GID, "已播放的句子")
            store._db.execute(
                """INSERT INTO speech_segments
                (call_id,generation_id,seq,body,state,data,playback_state,revision,created_at,
                 ready_at,play_started_at,playback_position_ms,end_reason)
                VALUES (?,?,1,?,'succeeded',?,'ended',2,?,?,?,350,'interrupted')""",
                (call_id, GID, "已播放的句子", SYNTHESIZED, OLD, OLD, OLD),
            )
            store._db.execute(
                "INSERT INTO speech_jobs VALUES (?,?,'succeeded',?)",
                (call_id, GID, SYNTHESIZED),
            )


def close_call(store, call_id, *, ended=OLD, review="succeeded"):
    with patch("bot_call.store.time", return_value=ended):
        result = store.end_call(call_id, prompt="请整理", _request_close=True)
    if result.state != "pending" or review == "pending":
        return result
    store.claim_review(call_id)
    if review == "succeeded":
        store.complete_review(call_id, "最终回顾")
    elif review == "uncertain":
        store.mark_review_uncertain(call_id)
    elif review in {"failed", "cancelled"}:
        store.fail_review(
            call_id,
            state=review,
            error_code="fake_failed" if review == "failed" else "fake_cancelled",
        )
    return result


def publish(store, call_id, *, context=True, pages=True):
    store.prepare_publication(call_id, char_budget=500)
    if context:
        store.claim_publication_context(call_id)
        store.finish_publication_context(call_id, "succeeded")
    if pages:
        while True:
            page, claimed = store.claim_publication_page(call_id)
            if not claimed:
                break
            store.finish_publication_page(call_id, page.number, "succeeded")


def retry(store, call_id, *, context_only=False, sent=False, completed=OLD, sending=False):
    cursor = store._db.execute(
        """INSERT INTO review_retry_attempts
        (call_id,created_at,original_reason,manifest_json,manifest_sha256,context_only,
         state,body,completed_at,send_requested_at)
        VALUES (?,?,'model_failed','{}','synthetic',?,'succeeded','补做回顾',?,?)""",
        (call_id, OLD, int(context_only), completed, OLD if sending else None),
    )
    view = AttemptStore(store, cursor.lastrowid)
    publish(view, call_id, pages=sent)
    return view


def data_tuple(store, call_id):
    return tuple(
        row[0]
        for table in ("speech_segments", "speech_jobs", "audio_jobs")
        for row in store._db.execute(f"SELECT data FROM {table} WHERE call_id=?", (call_id,))
    )


def invite(store, call_id, *, limits=None):
    secret = InvitationSecret.create()
    store.issue_web_invitation(call_id, secret, session_ttl=3600)
    return store.exchange_web_invitation(secret.value, _limits=limits)


def downgrade(store):
    store._db.execute("DROP TRIGGER audio_progress")
    store._db.execute(audio_progress_trigger())
    store._db.execute("PRAGMA user_version=23")
    store._db.execute("PRAGMA auto_vacuum=NONE")
    store._db.execute("VACUUM")


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.path = Path(self.enterContext(TemporaryDirectory())) / "calls.db"
        self.store = self.enterContext(Store(self.path))

    def prune(self, **options):
        return self.store.prune_expired_audio(
            **(dict(retention_days=7, now=NOW, reclaim=False) | options)
        )

    def test_normal_review_preserves_all_text_and_frozen_evidence(self):
        add_call(self.store, "normal")
        close_call(self.store, "normal")
        publish(self.store, "normal")
        body = self.store.publication_body("normal")
        publication = self.store.get_publication("normal")
        db = self.store._db
        job = self.store.get_audio_job("normal", "raw")
        tables = ("turns", "reviews", "audio_resolutions", "review_audio_notes")
        old_rows = {table: db.execute(f"SELECT * FROM {table}").fetchall() for table in tables}
        segments = self.store.segments("normal", material=True)
        original = self.store._transaction
        changes = []

        @contextmanager
        def record_transaction():
            before = db.total_changes
            with original():
                yield
            changes.append(db.total_changes - before)

        with (
            patch.object(self.store, "_transaction", record_transaction),
            self.assertLogs("bot_call.retention", level="INFO") as logs,
        ):
            result = self.prune(batch_rows=1)
        self.assertEqual([count for count in changes if count], [1, 1, 1])
        self.assertEqual(
            (result["calls_pruned"], result["freed_bytes"]), (1, len(RAW) + 2 * len(SYNTHESIZED))
        )
        self.assertFalse(result["has_more"])
        self.assertEqual(result["cleared_segments"], 1)
        self.assertEqual(result["cleared_speech_jobs"], 1)
        self.assertEqual(result["cleared_audio_jobs"], 1)
        self.assertEqual(data_tuple(self.store, "normal"), (None, None, b""))
        self.assertEqual(self.store.segments("normal", material=True), segments)
        for table, rows in old_rows.items():
            self.assertEqual(db.execute(f"SELECT * FROM {table}").fetchall(), rows)
        cleared = self.store.get_audio_job("normal", "raw")
        self.assertEqual(cleared.resolution, job.resolution)
        self.assertEqual(
            (cleared.sha256, cleared.byte_count, cleared.mime, cleared.error_code),
            (job.sha256, job.byte_count, job.mime, job.error_code),
        )
        self.assertEqual(self.store.receive_audio("normal", "raw", RAW, "audio/webm"), cleared)
        self.assertEqual(self.store.publication_body("normal"), body)
        self.assertEqual(self.store.get_publication("normal"), publication)
        for private in ("normal", "来电内容", "最终回顾", RAW.decode()):
            self.assertNotIn(private, "\n".join(logs.output))

    def test_incomplete_and_recent_calls_are_protected(self):
        protected = (
            "active",
            "closing",
            "pending",
            "running",
            "failed",
            "uncertain",
            "cancelled",
            "no-plan",
            "no-context",
            "unfinished-page",
            "no-pages",
            "recent",
        )
        for call_id in protected:
            add_call(self.store, call_id, resolved=call_id != "closing")
            if call_id != "active":
                close_call(
                    self.store,
                    call_id,
                    ended=NOW - 7 * DAY + 1 if call_id == "recent" else OLD,
                    review=call_id
                    if call_id in {"pending", "running", *RAW_FAILURES}
                    else "succeeded",
                )
        invite(self.store, "active", limits=RuntimeLimits())
        publish(self.store, "no-context", context=False)
        publish(self.store, "unfinished-page", pages=False)
        publish(self.store, "no-pages")
        self.store._db.execute("DELETE FROM publication_pages WHERE call_id='no-pages'")
        publish(self.store, "recent")
        add_call(self.store, "due")
        close_call(self.store, "due", ended=NOW - 7 * DAY)
        publish(self.store, "due")
        before = {call_id: data_tuple(self.store, call_id) for call_id in protected}
        self.assertEqual(self.prune(retention_days=0)["calls_pruned"], 0)
        self.assertEqual(data_tuple(self.store, "due"), (SYNTHESIZED, SYNTHESIZED, RAW))
        self.assertEqual(self.prune()["calls_pruned"], 1)
        self.assertEqual(data_tuple(self.store, "due"), (None, None, b""))
        for call_id in protected:
            with self.subTest(call_id=call_id):
                self.assertEqual(data_tuple(self.store, call_id), before[call_id])

    def test_latest_successful_retry_and_page_rules(self):
        cases = {
            "sent": (dict(sent=True), True),
            "context-only": (dict(context_only=True), True),
            "sending": (dict(context_only=True, sending=True), False),
            "running-page": (dict(context_only=True), False),
            "latest-failed": (dict(context_only=True), False),
            "pending-attempt": (dict(context_only=True), False),
            "pending-node": (dict(context_only=True), False),
            "pending-original-node": (dict(context_only=True), False),
            "no-completion": (dict(context_only=True, completed=None), False),
            "recent-retry": (dict(sent=True, completed=NOW - DAY), False),
            "fallback-date": (dict(sent=True, completed=None), True),
        }
        db = self.store._db
        for call_id, (options, _) in cases.items():
            add_call(self.store, call_id)
            close_call(self.store, call_id, review="failed")
            view = retry(self.store, call_id, **options)
            if call_id == "running-page":
                view.claim_publication_page(call_id)
            elif call_id in {"latest-failed", "pending-attempt"}:
                state = "failed" if call_id == "latest-failed" else "pending"
                db.execute(
                    """INSERT INTO review_retry_attempts
                    (call_id,created_at,original_reason,manifest_json,manifest_sha256,context_only,state)
                    VALUES (?,?,'model_failed','{}','synthetic',0,?)""",
                    (call_id, OLD, state),
                )
            elif call_id == "pending-node":
                db.execute(
                    "INSERT INTO review_retry_nodes VALUES (?,'node','{}','pending',NULL,NULL,NULL)",
                    (view.attempt_id,),
                )
            elif call_id == "pending-original-node":
                db.execute(
                    "INSERT INTO review_work VALUES (?,'{}','synthetic',1,1,NULL)", (call_id,)
                )
                db.execute(
                    """INSERT INTO review_work_nodes VALUES
                    (?,'node',0,1,'map',0,1,'[]','{}',1,'pending',NULL,NULL,NULL)""",
                    (call_id,),
                )
        self.assertEqual(self.prune()["calls_pruned"], 3)
        for call_id, (_, cleared) in cases.items():
            with self.subTest(call_id=call_id):
                self.assertEqual(data_tuple(self.store, call_id)[-1], b"" if cleared else RAW)

    def test_no_text_closure_and_trigger_require_a_terminal_call(self):
        for failure in ("empty", *RAW_FAILURES):
            with self.subTest(failure=failure):
                add_call(self.store, failure, text=False, failure=failure)
                db = self.store._db
                with self.assertRaisesRegex(sqlite3.IntegrityError, "audio outcome is terminal"):
                    db.execute("UPDATE audio_jobs SET data=X'' WHERE call_id=?", (failure,))
                self.assertEqual(close_call(self.store, failure).state, "closed_no_text")
                for mutation in ("data=X'01'", "data=X'', error_code='other'", "data=NULL"):
                    with self.assertRaisesRegex(
                        sqlite3.IntegrityError, "audio outcome is terminal"
                    ):
                        db.execute(f"UPDATE audio_jobs SET {mutation} WHERE call_id=?", (failure,))
                db.execute("UPDATE audio_jobs SET data=X'' WHERE call_id=?", (failure,))
                self.assertEqual(self.store.get_audio_job(failure, "raw").data, b"")
        add_call(self.store, "eligible", text=False)
        close_call(self.store, "eligible")
        add_call(self.store, "unresolved", text=False, resolved=False)
        close_call(self.store, "unresolved")
        self.assertEqual(self.prune()["calls_pruned"], 1)
        self.assertEqual(data_tuple(self.store, "eligible"), (b"",))
        self.assertEqual(data_tuple(self.store, "unresolved"), (RAW,))

    def test_schema23_upgrade_changes_only_audio_progress_and_keeps_every_row(self):
        for call_id in ("active", "pending", "complete"):
            add_call(self.store, call_id)
        close_call(self.store, "pending", review="pending")
        close_call(self.store, "complete")
        publish(self.store, "complete")
        downgrade(self.store)

        def snapshot(store):
            sql = dict(store._db.execute("SELECT name,sql FROM sqlite_master"))
            tables = [
                row[0]
                for row in store._db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            ]
            rows = {
                table: store._db.execute(f'SELECT * FROM "{table}"').fetchall() for table in tables
            }
            return sql, rows

        before_sql, before_rows = snapshot(self.store)
        self.store.close()
        with Store(self.path) as upgraded:
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], 24)
            self.assertEqual(upgraded._db.execute("PRAGMA auto_vacuum").fetchone()[0], 0)
            after_sql, after_rows = snapshot(upgraded)
            self.assertEqual(after_rows, before_rows)
            self.assertEqual(before_sql.pop("audio_progress"), audio_progress_trigger())
            self.assertEqual(
                after_sql.pop("audio_progress"), audio_progress_trigger(retention=True)
            )
            self.assertEqual(after_sql, before_sql)
            with self.assertRaisesRegex(sqlite3.IntegrityError, "audio outcome is terminal"):
                upgraded._db.execute("UPDATE audio_jobs SET data=X'' WHERE call_id='pending'")
            self.assertEqual(
                upgraded.prune_expired_audio(retention_days=7, now=NOW)["calls_pruned"], 1
            )
            self.assertEqual(data_tuple(upgraded, "complete"), (None, None, b""))
            self.assertEqual(data_tuple(upgraded, "pending"), (SYNTHESIZED, SYNTHESIZED, RAW))
            self.assertEqual(data_tuple(upgraded, "active"), (SYNTHESIZED, SYNTHESIZED, RAW))

    def test_batches_prioritize_oldest_and_signal_remaining_calls(self):
        call_ids = ["e", "d", "c", "b", "a"]
        for index, call_id in enumerate(call_ids):
            add_call(self.store, call_id, text=False)
            close_call(self.store, call_id, ended=OLD + index)
        for count, more, pruned in ((2, True, 2), (2, True, 4), (1, False, 5), (0, False, 5)):
            result = self.prune(batch_calls=2)
            self.assertEqual((result["calls_pruned"], result["has_more"]), (count, more))
            for index, call_id in enumerate(call_ids):
                self.assertEqual(data_tuple(self.store, call_id)[0], b"" if index < pruned else RAW)

    def test_new_database_reclaims_bounded_pages(self):
        db = self.store._db
        self.assertEqual(db.execute("PRAGMA auto_vacuum").fetchone()[0], 2)
        add_call(self.store, "large", text=False, data=b"x" * (2 * 1024 * 1024))
        close_call(self.store, "large")
        size = self.path.stat().st_size
        self.assertEqual(self.prune(reclaim=True)["reclaim"], "incremental")
        self.assertGreater(size - self.path.stat().st_size, 0)
        self.assertLessEqual(
            size - self.path.stat().st_size, 256 * db.execute("PRAGMA page_size").fetchone()[0]
        )
        self.assertGreater(db.execute("PRAGMA freelist_count").fetchone()[0], 0)

    def test_old_database_checks_threshold_and_disk_before_rewrite(self):
        add_call(self.store, "large", text=False, data=b"x" * (2 * 1024 * 1024))
        close_call(self.store, "large")
        downgrade(self.store)
        self.store.close()
        with Store(self.path) as old:
            original_size = self.path.stat().st_size
            result = old.prune_expired_audio(retention_days=7, now=NOW, allow_vacuum=False)
            self.assertEqual(result["reclaim"], "skipped_threshold")
            self.assertEqual(data_tuple(old, "large"), (b"",))
            with patch(
                "bot_call.retention.shutil.disk_usage", return_value=SimpleNamespace(free=0)
            ):
                self.assertEqual(reclaim_space(old), "skipped_disk_space")
            self.assertEqual(self.path.stat().st_size, original_size)
            self.assertEqual(old._db.execute("PRAGMA auto_vacuum").fetchone()[0], 0)
            with patch(
                "bot_call.retention.shutil.disk_usage",
                return_value=SimpleNamespace(free=1024**3),
            ):
                self.assertEqual(reclaim_space(old), "converted_incremental")
            self.assertLess(self.path.stat().st_size, original_size)
            self.assertEqual(old._db.execute("PRAGMA auto_vacuum").fetchone()[0], 2)
            self.assertEqual(data_tuple(old, "large"), (b"",))

    def test_wal_reclaim_uses_passive_checkpoint(self):
        db = self.store._db
        self.assertEqual(db.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
        add_call(self.store, "old", text=False, data=b"x" * (1024 * 1024))
        close_call(self.store, "old")
        statements = []
        db.set_trace_callback(statements.append)
        self.assertEqual(self.prune(reclaim=True)["reclaim"], "incremental")
        self.assertIn("PRAGMA wal_checkpoint(PASSIVE)", statements)

    def test_writer_contention_skips_and_preserves_store_timeout(self):
        add_call(self.store, "old", text=False)
        close_call(self.store, "old")
        db = self.store._db
        with closing(sqlite3.connect(self.path, isolation_level=None)) as other:
            other.execute("BEGIN IMMEDIATE")
            with self.assertLogs("bot_call.retention", level="WARNING"):
                result = self.prune()
            self.assertEqual(result["reclaim"], "skipped_busy")
            self.assertTrue(result["has_more"])
            self.assertEqual(data_tuple(self.store, "old"), (RAW,))
            self.assertEqual(db.execute("PRAGMA busy_timeout").fetchone()[0], 10000)
        self.assertEqual(self.prune()["calls_pruned"], 1)

    def test_backend_config_retention_boundaries(self):
        args = (self.path, "https://call.example.com", 3600, RuntimeLimits())
        self.assertEqual(BackendConfig(*args).audio_retention_days, 7)
        for days in (0, 3650):
            self.assertEqual(
                BackendConfig(*args, audio_retention_days=days).audio_retention_days, days
            )
        for days in (-1, 3651, False, "7"):
            with self.subTest(days=days), self.assertRaises(BackendError):
                BackendConfig(*args, audio_retention_days=days)


class RetentionRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_timer_skips_active_call_then_cleans_after_end_and_cancels_at_shutdown(self):
        path = Path(self.enterContext(TemporaryDirectory())) / "calls.db"
        with Store(path) as store:
            add_call(store, "old", text=False)
            close_call(store, "old", ended=time() - 8 * DAY)
            add_call(store, "active", text=False)
            invite(store, "active")
        events = asyncio.Queue()
        loop = asyncio.get_running_loop()
        prune = Store.prune_expired_audio

        def observe(store, **kwargs):
            result = prune(store, **kwargs)
            loop.call_soon_threadsafe(events.put_nowait, result)
            return result

        async def unexpected(*args):
            self.fail("Maintenance must never call a provider or publish a review")

        runtime = CallRuntime(
            path,
            RuntimeCallbacks(unexpected, unexpected, unexpected, unexpected),
            audio_retention_days=7,
            retention_start_delay=0.01,
            retention_busy_delay=0.02,
        )
        self.addAsyncCleanup(runtime.shutdown)
        with patch.object(Store, "prune_expired_audio", observe):
            await runtime.start()
            await asyncio.wait_for(runtime.drain(), timeout=1)
            first = await asyncio.wait_for(events.get(), timeout=2)
            self.assertEqual(first["reclaim"], "skipped_active")
            self.assertEqual((await runtime.read_state("old")).audio_jobs[0].data, RAW)
            await runtime.end_call("active", prompt="请整理")
            async with asyncio.timeout(2):
                while not (await events.get())["calls_pruned"]:
                    pass
            self.assertEqual((await runtime.read_state("old")).audio_jobs[0].data, b"")
            self.assertEqual((await runtime.read_state("active")).audio_jobs[0].data, RAW)
            handle = runtime._retention_handle
            self.assertIsNotNone(handle)
            self.assertTrue((await asyncio.wait_for(runtime.shutdown(), timeout=1)).stopped)
            self.assertTrue(handle.cancelled())
            self.assertIsNone(runtime._retention_handle)
