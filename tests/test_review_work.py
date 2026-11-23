"""Offline whole scenarios: synthetic Stores, fake generation, no Provider."""

import asyncio
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import traceback
import unittest
from unittest.mock import patch

from bot_call.binding import CallBinding
from bot_call.publication import advance_publication, Outcome
from bot_call.review_work import (
    advance_review_work,
    prepare_review_work,
    ReviewWorkError,
    NotApplied,
)
from bot_call.store import (
    Store,
    StateConflict,
    _APPLICATION_ID,
    _SCHEMA,
    _REVIEW_SCHEMA,
    _PUBLICATION_SCHEMA,
)
import bot_call.store as storage


BUDGET = 2500
PRIVATE = "SYNTHETIC_PRIVATE_BODY_AND_CREDENTIAL_十一阶段"
PROMPT = "PRIVATE_REVIEW_REQUIREMENT 原样区分表达、建议、决定及未知播放\n "
FINAL = " 整通唯一回顾：开头家庭；中段犹豫；结尾未定；Bot建议非决定；未知播放。\n "


def binding(call_id):
    return CallBinding(
        call_id,
        "user",
        "bot",
        "qq-1",
        "aiocqhttp",
        "FriendMessage",
        "user",
        "qq-1:FriendMessage:user",
        "original-A",
        None,
    )


def freeze(store, call_id, *, long=False, empty=False):
    store.create_call(binding(call_id), background="BACKGROUND_NOT_THIS_CALL")
    if not empty:
        body = ("开头家庭🙂 e\u0301\r\n " * 1100) if long else PRIVATE
        store.append_user(call_id, "u", body, source="text")
        if long:
            store.claim_generation(call_id, "g", utterance_id="u", source="fake_reply")
            store.complete_generation(call_id, "g", " Bot建议非决定 " * 700)
            store.append_user(call_id, "middle", " 中段犹豫\t " * 1300, source="text")
            store.append_user(call_id, "end", "\n 结尾未定 " * 1300, source="text")
    return store.end_call(call_id, prompt=PROMPT)


def prepare(store, call_id):
    return prepare_review_work(store, call_id, input_token_budget=BUDGET, count_tokens=len)


_unexpected_callbacks = []


def no_fake(node):
    _unexpected_callbacks.append("unexpected callback")
    raise AssertionError("a saved/unresolved step must not be generated again")


class ReviewWorkTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        _unexpected_callbacks.clear()
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "calls.db"

    async def asyncTearDown(self):
        self.assertEqual(_unexpected_callbacks, [])

    def assert_private_error(self, error):
        rendered = repr(error) + "".join(traceback.format_exception(error))
        self.assertNotIn(PRIVATE, rendered)
        self.assertNotIn(PROMPT, rendered)
        self.assertTrue(error.__suppress_context__)

    async def test_full_multilevel_final_and_same_publication_body(self):
        with Store(self.path) as store:
            frozen = freeze(store, "whole", long=True)
            work = prepare(store, "whole")
            original_plan = work.plan_json
            map_count = work.map_count
            self.assertGreater(map_count, 8)
            with self.assertRaises(StateConflict):
                store.complete_review("whole", "premature body")
            self.assertFalse(store.claim_review("whole")[1])
            seen, kinds = set(), []
            facts = ["开头家庭", "Bot建议非决定", "中段犹豫", "结尾未定", "未知播放"]

            async def fake(node):
                self.assertFalse(store._db.in_transaction)
                self.assertNotIn(node.source_key, seen)
                seen.add(node.source_key)
                kinds.append((node.level, node.kind))
                self.assertLessEqual(len(node.request_json), BUDGET)
                self.assertEqual(len(node.request_json), node.token_count)
                messages = json.loads(node.request_json)["messages"]
                data = json.loads(messages[1]["content"])
                self.assertEqual(data["review_prompt"], PROMPT)
                self.assertNotIn("BACKGROUND_NOT_THIS_CALL", node.request_json)
                if node.kind == "map":
                    saved = json.loads(original_plan)["chunks"][node.ordinal - 1]
                    self.assertEqual(saved["request_json"], node.request_json)
                    tags = {facts[f["turn"]["sequence"] - 1] for f in data["fragments"]}
                    if any(f["turn"]["role"] == "assistant" for f in data["fragments"]):
                        self.assertTrue(
                            any(f["turn"]["playback_state"] == "unknown" for f in data["fragments"])
                        )
                        tags.add("未知播放")
                else:
                    position = node.first_map
                    for note in data["notes"]:
                        child = store.get_review_work_node("whole", note["source_key"])
                        self.assertEqual(child.state, "succeeded")
                        self.assertEqual(
                            note["result_sha256"], hashlib.sha256(child.body.encode()).hexdigest()
                        )
                        self.assertEqual(note["body"], child.body)
                        self.assertEqual(note["origin"], "generated_chunk_note_not_user_quote")
                        self.assertEqual(note["map_range"][0], position)
                        position = note["map_range"][1]
                    self.assertEqual(position, node.stop_map)
                    tags = {fact for fact in facts if any(fact in n["body"] for n in data["notes"])}
                    if node.kind == "final":
                        self.assertEqual(tags, set(facts))
                        self.assertEqual((node.first_map, node.stop_map), (0, map_count))
                        return FINAL
                    self.assertGreaterEqual(len(data["notes"]), 2)
                return "生成分块笔记，非用户原话：" + "|".join(sorted(tags)) + " 资料" * 30

            completed = await advance_review_work(
                store, "whole", fake, count_tokens=len, max_steps=500
            )
            self.assertEqual(
                completed.state,
                "succeeded",
                (completed.blocked_code, completed.level, completed.kind),
            )
            self.assertGreaterEqual(completed.level, 3)  # map + >=2 merge levels + final
            self.assertEqual(kinds.count((completed.level, "final")), 1)
            self.assertEqual(completed.review.body, FINAL)
            self.assertEqual(completed.review.input_json, frozen.input_json)
            self.assertEqual(completed.review.input_sha256, frozen.input_sha256)
            self.assertEqual(completed.plan_json, original_plan)
            self.assertEqual(
                await advance_review_work(store, "whole", no_fake, count_tokens=len), completed
            )
            publication = store.prepare_publication("whole", char_budget=120)
            sent, readable = [], []

            async def send(bound, page):
                self.assertEqual(bound, binding("whole"))
                sent.append(page.fragment)
                return Outcome.CONFIRMED

            async def context(bound, source_key, body):
                self.assertEqual(bound, binding("whole"))
                readable.append((source_key, body))  # Synthetic model-input reader.
                self.assertEqual(readable[-1][1], FINAL)
                return Outcome.CONFIRMED

            publication = await advance_publication(store, "whole", send, context)
            self.assertTrue(publication.complete)
            self.assertEqual("".join(sent), FINAL)
            self.assertEqual(readable, [(publication.context_key, FINAL)])
            self.assertEqual(publication.review_sha256, hashlib.sha256(FINAL.encode()).hexdigest())
            for value in [completed, *completed.frontier]:
                self.assertNotIn(PROMPT, repr(value))
                self.assertNotIn(FINAL, repr(value))

    async def test_reopen_fixed_plan_claim_competition_and_conservative_boundaries(self):
        with Store(self.path) as store:
            freeze(store, "resume", long=True)
            work = prepare(store, "resume")
            plan = work.plan_json
            seen = []

            async def fake(node):
                seen.append(node.source_key)
                return "笔记" if node.kind != "final" else FINAL

            saved = await advance_review_work(store, "resume", fake, count_tokens=len)
            self.assertEqual(saved.frontier[0].state, "succeeded")
        with Store(self.path) as first, Store(self.path) as second:
            self.assertIn("resume", first.review_work_ids())
            work = prepare_review_work(first, "resume", input_token_budget=0, count_tokens=no_fake)
            self.assertEqual(work.plan_json, plan)
            self.assertEqual(work.input_token_budget, BUDGET)
            entered, release = asyncio.Event(), asyncio.Event()

            async def held(node):
                self.assertNotIn(node.source_key, seen)
                seen.append(node.source_key)
                entered.set()
                await release.wait()
                return "笔记"

            task = asyncio.create_task(advance_review_work(first, "resume", held, count_tokens=len))
            await entered.wait()
            other = await advance_review_work(
                second, "resume", no_fake, count_tokens=len, max_steps=100
            )
            self.assertEqual(other.state, "running")
            release.set()
            await task
        with Store(self.path) as store:
            completed = await advance_review_work(
                store, "resume", fake, count_tokens=len, max_steps=500
            )
            self.assertEqual(completed.state, "succeeded")
            self.assertEqual(len(seen), len(set(seen)))
            freeze(store, "unknown")
            pending = prepare(store, "unknown")
            node, won = store.claim_review_work_node("unknown", pending.frontier[0].source_key)
            self.assertTrue(won)
        with Store(self.path) as store:
            self.assertEqual(
                (await advance_review_work(store, "unknown", no_fake, count_tokens=len)).state,
                "running",
            )
            store.finish_review_work_node("unknown", node.source_key, state="uncertain")
            self.assertEqual(
                (await advance_review_work(store, "unknown", no_fake, count_tokens=len)).state,
                "blocked",
            )
            freeze(store, "empty", empty=True)
            with self.assertRaises(storage.NotFound):
                store.get_review("empty")
            self.assertIsNone(store.get_review_work("empty"))
            freeze(store, "oversized")
            prepare(store, "oversized")
            huge = "不能截短的已知笔记 " * 1200

            async def too_long(node):
                return huge

            blocked = await advance_review_work(store, "oversized", too_long, count_tokens=len)
            self.assertEqual((blocked.state, blocked.blocked_code), ("blocked", "merge_budget"))
            self.assertEqual(blocked.frontier[0].body, huge)
            self.assertIsNone(blocked.review.body)
            await advance_review_work(store, "oversized", no_fake, count_tokens=len)
            store.create_call(binding("cannot-combine"))
            store.append_user("cannot-combine", "u", "多" * 3000, source="text")
            store.end_call("cannot-combine", prompt=PROMPT)
            combined = prepare(store, "cannot-combine")
            self.assertGreater(combined.map_count, 1)
            combined = await advance_review_work(
                store, "cannot-combine", too_long, count_tokens=len, max_steps=20
            )
            self.assertEqual(combined.blocked_code, "merge_budget")
            self.assertTrue(
                all(n.state == "succeeded" and n.body == huge for n in combined.frontier)
            )
            self.assertIsNone(combined.review.body)

    async def test_unknown_cancellation_commit_failures_and_lost_ack(self):
        with Store(self.path) as store:
            for label, exception, expected in [
                ("exception", RuntimeError(PRIVATE + PROMPT), "uncertain"),
                ("timeout", TimeoutError(PRIVATE + PROMPT), "uncertain"),
                ("not-applied", NotApplied(PRIVATE + PROMPT), "failed"),
            ]:
                freeze(store, label)
                prepare(store, label)

                async def broken(node):
                    raise exception

                work = await advance_review_work(store, label, broken, count_tokens=len)
                self.assertEqual(work.frontier[0].state, expected)
                self.assertEqual(work.state, "blocked")
                self.assertIsNone(work.review.body)
                await advance_review_work(store, label, no_fake, count_tokens=len)
            freeze(store, "cancel")
            prepare(store, "cancel")
            entered = asyncio.Event()

            async def cancelled(node):
                entered.set()
                await asyncio.Event().wait()

            task = asyncio.create_task(
                advance_review_work(store, "cancel", cancelled, count_tokens=len)
            )
            await entered.wait()
            task.cancel(PRIVATE + PROMPT)
            try:
                await task
            except asyncio.CancelledError as error:
                self.assert_private_error(error)
            else:
                self.fail("cancellation was lost")
            self.assertEqual(store.get_review_work("cancel").frontier[0].state, "cancelled")
            await advance_review_work(store, "cancel", no_fake, count_tokens=len)
            # External result exists, but success COMMIT fails; second status
            # write either succeeds as uncertain or fails leaving running.
            for double in [False, True]:
                call = "commit-" + str(double)
                freeze(store, call)
                prepare(store, call)
                calls, denials = [], [0]

                async def produced(node):
                    calls.append(node.source_key)
                    denials[0] = 2 if double else 1
                    return PRIVATE

                def reject_commit(action, operation, unused, database, trigger):
                    if (
                        action == sqlite3.SQLITE_TRANSACTION
                        and operation == "COMMIT"
                        and denials[0]
                    ):
                        denials[0] -= 1
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                store._db.set_authorizer(reject_commit)
                try:
                    try:
                        await advance_review_work(store, call, produced, count_tokens=len)
                    except ReviewWorkError as error:
                        self.assert_private_error(error)
                    else:
                        self.fail("failed persistence pretended success")
                finally:
                    store._db.set_authorizer(None)
                self.assertEqual(denials, [0])
                self.assertEqual(len(calls), 1)
                current = store.get_review_work(call)
                self.assertEqual(current.frontier[0].state, "running" if double else "uncertain")
                self.assertIsNone(current.review.body)
                self.assertEqual(
                    current.review.input_sha256,
                    hashlib.sha256(current.review.input_json.encode()).hexdigest(),
                )
                await advance_review_work(store, call, no_fake, count_tokens=len)
            freeze(store, "counter-failure")
            prepare(store, "counter-failure")

            def broken_counter(request):
                raise RuntimeError(PRIVATE + PROMPT)

            for counter in [broken_counter, lambda request: True]:
                try:
                    await advance_review_work(
                        store, "counter-failure", no_fake, count_tokens=counter
                    )
                except ReviewWorkError as error:
                    self.assert_private_error(error)
                else:
                    self.fail("invalid counter reached fake")
            self.assertEqual(store.get_review_work("counter-failure").frontier[0].state, "pending")
            # Local ACK loss after a successful node commit is reconciled by DB.
            freeze(store, "node-ack")
            prepare(store, "node-ack")
            original = store.finish_review_work_node

            def lost_ack(call_id, source_key, **kwargs):
                original(call_id, source_key, **kwargs)
                raise sqlite3.OperationalError(PRIVATE + PROMPT)

            async def note(node):
                return PRIVATE if node.kind == "map" else FINAL

            with patch.object(store, "finish_review_work_node", side_effect=lost_ack):
                saved = await advance_review_work(store, "node-ack", note, count_tokens=len)
            self.assertEqual(saved.kind, "final")
            self.assertEqual(saved.frontier[0].state, "pending")
            # Final result commits, then parent commit fails. A later local-only
            # retry can finish the parent; even a lost final ACK is reconciled.
            original_complete = store.complete_review
            with patch.object(
                store, "complete_review", side_effect=sqlite3.OperationalError(PRIVATE + PROMPT)
            ):
                try:
                    await advance_review_work(store, "node-ack", note, count_tokens=len)
                except ReviewWorkError as error:
                    self.assert_private_error(error)
                else:
                    self.fail("parent commit failure pretended complete")
            saved = store.get_review_work("node-ack")
            self.assertEqual(saved.frontier[0].state, "succeeded")
            self.assertEqual(saved.review.state, "running")
            self.assertIsNone(saved.review.body)
        with Store(self.path) as store:
            original_complete = store.complete_review

            def final_ack(call_id, body):
                original_complete(call_id, body)
                raise sqlite3.OperationalError(PRIVATE + PROMPT)

            with patch.object(store, "complete_review", side_effect=final_ack):
                done = await advance_review_work(store, "node-ack", no_fake, count_tokens=len)
            self.assertEqual(done.review.body, FINAL)
            self.assertEqual(done.state, "succeeded")
            await advance_review_work(store, "node-ack", no_fake, count_tokens=len)
            with self.assertRaises(ValueError):
                store.complete_review("node-ack", "different final")
            for call in ["commit-False", "commit-True", "cancel", "exception", "timeout"]:
                await advance_review_work(store, call, no_fake, count_tokens=len)

    async def test_v3_atomic_upgrade_preserves_old_records_and_failure_rolls_back(self):
        tables = ("calls", "turns", "generations", "reviews", "publications", "publication_pages")
        with Store(self.path) as seed:
            freeze(seed, "legacy-done")
            seed.claim_review("legacy-done")
            seed.complete_review("legacy-done", FINAL)
            seed.prepare_publication("legacy-done")
            freeze(seed, "legacy-pending")
            old = {
                name: [tuple(row) for row in seed._db.execute("SELECT * FROM " + name).fetchall()]
                for name in tables
            }
        legacy = self.path.with_name("v3.db")
        with closing(sqlite3.connect(legacy)) as raw:
            for statement in (*_SCHEMA, *_REVIEW_SCHEMA, *_PUBLICATION_SCHEMA):
                raw.execute(statement)
            raw.execute(f"PRAGMA application_id={_APPLICATION_ID}")
            raw.execute("PRAGMA user_version=3")
            for name, rows in old.items():
                for row in rows:
                    raw.execute(
                        "INSERT INTO " + name + " VALUES (" + ",".join("?" for _ in row) + ")", row
                    )
            raw.commit()
        with closing(sqlite3.connect(legacy)) as raw:
            catalog = raw.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name"
            ).fetchall()
        with patch.object(storage, "_WORK_SCHEMA", (*storage._WORK_SCHEMA, "INVALID MIGRATION")):
            with self.assertRaises(sqlite3.Error):
                Store(legacy)
        with closing(sqlite3.connect(legacy)) as raw:
            self.assertEqual(raw.execute("PRAGMA user_version").fetchone()[0], 3)
            self.assertEqual(
                raw.execute(
                    "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name"
                ).fetchall(),
                catalog,
            )
            self.assertEqual(
                raw.execute(
                    "SELECT name FROM sqlite_master WHERE name LIKE 'review_work%'"
                ).fetchall(),
                [],
            )
            for name, rows in old.items():
                self.assertEqual(raw.execute("SELECT * FROM " + name).fetchall(), rows)
        with Store(legacy) as upgraded:
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], 24)
            for name, rows in old.items():
                self.assertEqual(
                    [tuple(r) for r in upgraded._db.execute("SELECT * FROM " + name).fetchall()],
                    rows,
                )
            prepare(upgraded, "legacy-pending")

            async def fake(node):
                return "分块笔记" if node.kind == "map" else FINAL

            self.assertEqual(
                (
                    await advance_review_work(
                        upgraded, "legacy-pending", fake, count_tokens=len, max_steps=10
                    )
                ).state,
                "succeeded",
            )
            self.assertEqual(upgraded.get_publication("legacy-done").body, FINAL)


if __name__ == "__main__":
    unittest.main()
