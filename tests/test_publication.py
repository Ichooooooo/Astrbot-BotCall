"""Four full synthetic publication scenarios; no real transport or Context."""

import asyncio
from contextlib import closing
from dataclasses import astuple
import hashlib
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import traceback
import unittest
from unittest.mock import patch

from bot_call.binding import CallBinding
from bot_call.publication import Outcome, PublicationPersistenceError, advance_publication
from bot_call.review import end_call, run_pending
from bot_call.service import process_utterance
from bot_call.store import (
    MaterialConflict,
    NotFound,
    StateConflict,
    Store,
    Turn,
    Generation,
    _APPLICATION_ID,
    _SCHEMA,
    _REVIEW_SCHEMA,
    _PUBLICATION_SCHEMA,
)


PRIVATE = "PRIVATE_BODY_PROMPT_CREDENTIAL"


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
        "A",
        None,
    )


class FakeContext:
    """A fake shared-lock importer plus a separate fake model history reader."""

    def __init__(self):
        self.current = "B"
        self.histories = {
            "A": [{"role": "user", "content": "before call"}],
            "B": [{"role": "user", "content": "B untouched"}],
        }
        self.lock = asyncio.Lock()
        self.entered = asyncio.Event()
        self.readable = True
        self.imports = 0

    async def import_review(self, bound, key, body):
        self.entered.set()
        async with self.lock:
            latest = list(self.histories[bound.conversation_id])
            if not any(message.get("source_key") == key for message in latest):
                latest.extend(
                    [
                        {
                            "role": "system",
                            "source_key": key,
                            "content": "系统导入的本通电话回顾，非用户原话；来源：" + key,
                        },
                        {"role": "assistant", "content": body},
                    ]
                )
                self.histories[bound.conversation_id] = latest
                self.imports += 1
        # An append return alone is deliberately not a confirmation.
        return None

    async def model_read(self, bound, key):
        async with self.lock:
            if self.readable:
                messages = self.histories[bound.conversation_id]
                for index, message in enumerate(messages[:-1]):
                    if message.get("source_key") == key:
                        return messages[index + 1]["content"]
        return None

    async def make_readable(self, bound, key, body):
        await self.import_review(bound, key, body)
        actual = await self.model_read(bound, key)
        return Outcome.CONFIRMED if actual == body else Outcome.UNCERTAIN


class PublicationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "calls.db"

    async def prepare(self, store, call_id, body="stored review\n\nsecond paragraph", budget=112):
        store.create_call(binding(call_id), background="separate private background")

        async def reply(bound, turn):
            return "Bot suggestion, not a user decision"

        await process_utterance(store, call_id, "u", "complete user material", reply)
        end_call(store, call_id)

        async def generate(bound, turns, prompt):
            return body

        await run_pending(store, generate)
        return store.prepare_publication(call_id, char_budget=budget)

    async def test_full_review_publication_preserves_text_and_original_context_after_reopen(self):
        body = (
            "  🙂e\u0301\r\n开头\r\n \t\r\n" + "单段🧭e\u0301 " * 170 + "\n\n中段\n\n结尾未定\n \t"
        )
        with Store(self.path) as store:
            store.create_call(binding("full"), background="BACKGROUND_ONLY")

            async def reply(bound, turn):
                return "Bot建议"

            for index, text in enumerate(["开头", "中段", "结尾"]):
                await process_utterance(store, "full", str(index), text, reply)
            end_call(store, "full", prompt="persistent prompt")
            with self.assertRaises(StateConflict):
                store.prepare_publication("full")
            generated = []

            async def generate(bound, turns, prompt):
                generated.append(bound.call_id)
                self.assertEqual(prompt, "persistent prompt")
                self.assertEqual(len(turns), 6)
                return body

            await run_pending(store, generate)
            plan = store.prepare_publication("full", char_budget=116)
            self.assertGreater(len(plan.pages), 9)  # Exercise final total digit width.
            self.assertEqual("".join(page.fragment for page in plan.pages), body)
            self.assertEqual(plan.pages[0].fragment, "  🙂e\u0301\r\n开头\r\n \t\r\n")
            self.assertEqual(plan.review_sha256, hashlib.sha256(body.encode()).hexdigest())
            self.assertNotIn(body, repr(plan))
            self.assertEqual(store.prepare_publication("full", char_budget=116), plan)
            with self.assertRaises(MaterialConflict):
                store.prepare_publication("full", char_budget=117)
            fake_context = FakeContext()
            sent = []

            async def send(bound, page):
                self.assertEqual(bound, binding("full"))
                self.assertFalse(store._db.in_transaction)
                self.assertLessEqual(len(page.message), 116)
                self.assertTrue(
                    page.message.startswith(
                        f"本次回顾 {page.number}/{plan.page_count}\n通话时间（UTC）："
                    )
                )
                self.assertTrue(page.message.endswith(page.fragment))
                self.assertNotIn(page.fragment, repr(page))
                sent.append((page.source_key, page.message))
                await asyncio.sleep(0)
                # A legacy confirmed page and standard submitted pages both
                # advance, persist and skip reentry without a schema migration.
                return Outcome.CONFIRMED if page.number == 1 else Outcome.SUBMITTED

            async with fake_context.lock:
                worker = asyncio.create_task(
                    advance_publication(store, "full", send, fake_context.make_readable)
                )
                await asyncio.wait_for(fake_context.entered.wait(), 3)
                # The importer is waiting on the same fake lock. It must read this
                # latest message after lock acquisition, rather than overwrite it.
                fake_context.histories["A"].append(
                    {"role": "user", "content": "A QQ message during call"}
                )
            final = await worker
            self.assertTrue(final.complete)
            self.assertTrue(final.submission_complete)
            self.assertEqual(await fake_context.model_read(binding("full"), plan.context_key), body)
            self.assertEqual(fake_context.current, "B")
            self.assertEqual(
                fake_context.histories["B"], [{"role": "user", "content": "B untouched"}]
            )
            self.assertEqual(fake_context.histories["A"][1]["content"], "A QQ message during call")
            self.assertEqual(fake_context.histories["A"][-2]["role"], "system")
            self.assertIn("非用户原话", fake_context.histories["A"][-2]["content"])
            self.assertEqual(
                fake_context.histories["A"][-1], {"role": "assistant", "content": body}
            )
            await fake_context.make_readable(binding("full"), plan.context_key, body)
            self.assertEqual(fake_context.imports, 1)
            self.assertEqual(len(sent), plan.page_count)
            self.assertEqual(len({key for key, _ in sent}), plan.page_count)
            self.assertEqual(store.get_review("full").body, body)

        async def forbidden(*args):
            self.fail("A confirmed step must not execute again")

        with Store(self.path) as reopened:
            self.assertEqual(
                reopened.prepare_publication("full", char_budget=116).pages, final.pages
            )
            self.assertTrue(
                (await advance_publication(reopened, "full", forbidden, forbidden)).complete
            )
            self.assertEqual(await run_pending(reopened, forbidden), ())
            self.assertEqual(len(reopened.get_review_input("full")), 6)
        self.assertEqual(generated, ["full"])

    async def test_partial_results_independent_sides_pending_reopen_and_workers_claim_once(self):
        with Store(self.path) as store:
            for mode in [
                "not-applied",
                "lost-confirmation",
                "adapter-true",
                "reverse",
                "append-only",
                "context-submitted",
            ]:
                plan = await self.prepare(store, mode, body="first paragraph\n\n" + "long 🧭 " * 20)
                sent, contexts = [], []
                fake = FakeContext()
                fake.readable = mode != "append-only"

                async def send(bound, page):
                    sent.append(page.number)
                    if page.number == 2 and mode == "not-applied":
                        return Outcome.NOT_APPLIED
                    if page.number == 2 and mode == "lost-confirmation":
                        raise TimeoutError(PRIVATE)  # Fake side effect occurred, ACK lost.
                    if page.number == 2 and mode == "adapter-true":
                        return True  # An adapter bool is not our explicit confirmation.
                    return Outcome.CONFIRMED

                async def context(bound, key, body):
                    contexts.append(key)
                    if mode == "reverse":
                        return Outcome.NOT_APPLIED
                    if mode == "context-submitted":
                        return Outcome.SUBMITTED  # Submission cannot confirm context.
                    return await fake.make_readable(bound, key, body)

                result = await advance_publication(store, mode, send, context)
                self.assertFalse(result.complete)
                if mode in {"not-applied", "lost-confirmation", "adapter-true"}:
                    self.assertTrue(result.context_available)
                    self.assertFalse(result.delivery_confirmed)
                    self.assertEqual(sent, [1, 2])
                    self.assertEqual(result.pages[0].state, "succeeded")
                    self.assertEqual(
                        result.pages[1].state, "failed" if mode == "not-applied" else "uncertain"
                    )
                    self.assertTrue(all(page.state == "pending" for page in result.pages[2:]))
                else:
                    self.assertTrue(result.delivery_confirmed)
                    self.assertFalse(result.context_available)
                    self.assertEqual(
                        result.context_state, "failed" if mode == "reverse" else "uncertain"
                    )
                    if mode == "append-only":
                        self.assertEqual(
                            fake.imports, 1, "append happened but model read did not confirm"
                        )
                with Store(self.path) as reopened:
                    self.assertEqual(
                        await advance_publication(reopened, mode, send, context), result
                    )
                self.assertEqual(len(contexts), 1)
                self.assertEqual(
                    len(sent),
                    2
                    if mode in {"not-applied", "lost-confirmation", "adapter-true"}
                    else plan.page_count,
                )
            plan = await self.prepare(store, "workers", "many pages " * 25)
            # One confirmed page before closing the connection; remaining pending
            # pages and the independent context step must resume after reopen.
            first, won = store.claim_publication_page("workers")
            self.assertTrue(won)
            store.finish_publication_page("workers", first.number, "succeeded")
        entered, release = asyncio.Event(), asyncio.Event()
        page_calls, context_calls = [], []
        with Store(self.path) as one, Store(self.path) as two:

            async def send(bound, page):
                page_calls.append(page.number)
                if page.number == 2:
                    entered.set()
                    await release.wait()
                return Outcome.CONFIRMED

            async def context(bound, key, body):
                context_calls.append(key)
                return Outcome.CONFIRMED

            worker = asyncio.create_task(advance_publication(one, "workers", send, context))
            try:
                await asyncio.wait_for(entered.wait(), 3)
                interim = await advance_publication(two, "workers", send, context)
                self.assertEqual(interim.pages[1].state, "running")
                self.assertTrue(interim.context_available)
            finally:
                release.set()
                final = await worker
            self.assertTrue(final.complete)
            self.assertEqual(page_calls, list(range(2, plan.page_count + 1)))
            self.assertEqual(context_calls, [plan.context_key])

    async def test_cancel_side_effect_commit_failure_and_double_failure_are_conservative(self):
        with Store(self.path) as store:
            for mode in [
                "cancel-page",
                "cancel-context",
                "commit-page",
                "commit-context",
                "double-page",
                "double-context",
            ]:
                with self.subTest(mode=mode):
                    plan = await self.prepare(store, mode, "complete review " + PRIVATE, budget=180)
                    before = store.get_transcript(mode)
                    side_effects = []
                    entered = asyncio.Event()
                    is_context = mode.endswith("context")
                    table = "publications" if is_context else "publication_pages"
                    column = "context_state" if is_context else "state"
                    marker = []
                    if mode.startswith("commit"):
                        store._db.create_function(
                            "update_completed", 0, lambda: marker.append(True)
                        )
                        store._db.execute(f"""CREATE TEMP TRIGGER fail_confirmation AFTER UPDATE OF {column} ON {table}
                            WHEN NEW.{column}='succeeded'
                            BEGIN INSERT INTO generations VALUES (NEW.call_id,'bad',999999,'fake','running',NULL);
                                  SELECT update_completed(); END""")
                    elif mode.startswith("double"):
                        store._db.execute(f"""CREATE TEMP TRIGGER fail_confirmation BEFORE UPDATE OF {column} ON {table}
                            WHEN NEW.{column}='uncertain'
                            BEGIN SELECT RAISE(ABORT, 'PRIVATE_BODY_PROMPT_CREDENTIAL'); END""")

                    async def callback(*args):
                        side_effects.append(mode)
                        if mode.startswith("cancel"):
                            entered.set()
                            await asyncio.Event().wait()
                        if mode.startswith("double"):
                            raise RuntimeError(PRIVATE)
                        store._db.execute("PRAGMA defer_foreign_keys=ON")
                        return Outcome.CONFIRMED

                    async def confirmed(*args):
                        return Outcome.CONFIRMED

                    send = confirmed if is_context else callback
                    context = callback if is_context else confirmed
                    if mode.startswith("cancel"):
                        task = asyncio.create_task(advance_publication(store, mode, send, context))
                        await asyncio.wait_for(entered.wait(), 3)
                        task.cancel(PRIVATE)
                        try:
                            await task
                        except asyncio.CancelledError as error:
                            self.assertNotIn(PRIVATE, "".join(traceback.format_exception(error)))
                        else:
                            self.fail("Cancellation must propagate")
                    else:
                        try:
                            await advance_publication(store, mode, send, context)
                        except PublicationPersistenceError as error:
                            self.assertNotIn(PRIVATE, "".join(traceback.format_exception(error)))
                        else:
                            self.fail("Uncommitted outcome must report persistence uncertainty")
                        store._db.execute("DROP TRIGGER fail_confirmation")
                        if mode.startswith("commit"):
                            self.assertEqual(
                                marker,
                                [True],
                                "Confirmation UPDATE completed before actual COMMIT failed",
                            )
                            with self.assertRaises(NotFound):
                                store.get_generation(mode, "bad")
                    result = store.get_publication(mode)
                    actual = result.context_state if is_context else result.pages[0].state
                    self.assertEqual(
                        actual, "running" if mode.startswith("double") else "uncertain"
                    )
                    self.assertFalse(result.complete)
                    if is_context:
                        self.assertTrue(result.delivery_confirmed)
                    elif not mode.startswith("cancel"):
                        self.assertTrue(result.context_available)
                    self.assertEqual(store.get_review(mode).body, plan.body)
                    self.assertEqual(store.get_transcript(mode), before)
                    self.assertEqual(store.get_review_input(mode), before)
                    with Store(self.path) as reopened:
                        # A cancelled page worker may leave context pending; only
                        # that independent step is allowed to continue.
                        await advance_publication(reopened, mode, send, context)
                    self.assertEqual(side_effects, [mode])

    async def test_v2_upgrade_preserves_all_old_states_and_rolls_back_failed_migration(self):
        target = binding("legacy")
        user = Turn(
            "legacy", 1, "user", "u", "fake", " original user 🧭\n ", 1, "not_applicable", 0
        )
        bot = Turn("legacy", 2, "assistant", "g", "fake", "original bot", 2, "failed", 1)
        generation = Generation("legacy", "g", 1, "fake", "succeeded", 2)
        with closing(sqlite3.connect(self.path)) as db:
            for sql in _SCHEMA + _REVIEW_SCHEMA:
                db.execute(sql)
            db.execute(f"PRAGMA application_id={_APPLICATION_ID}")
            db.execute("PRAGMA user_version=2")
            db.execute(
                "INSERT INTO calls VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (*astuple(target), "original background", 0),
            )
            for turn in [user, bot]:
                db.execute("INSERT INTO turns VALUES (?,?,?,?,?,?,?,?,?)", astuple(turn))
            db.execute("INSERT INTO generations VALUES (?,?,?,?,?,?)", astuple(generation))
            # Review material is produced by the existing serializer, exactly as
            # a v2 Store would persist it. No current Store is opened yet.
            import json
            from dataclasses import asdict

            material = json.dumps(
                [asdict(user), asdict(bot)],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            digest = hashlib.sha256(material.encode()).hexdigest()
            for state in ["succeeded", "pending", "running", "failed", "cancelled", "uncertain"]:
                call_id = "legacy" if state == "succeeded" else state
                if call_id != "legacy":
                    db.execute(
                        "INSERT INTO calls VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (*astuple(binding(call_id)), None, 0),
                    )
                db.execute(
                    "INSERT INTO reviews VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        call_id,
                        state,
                        3,
                        2 if call_id == "legacy" else 0,
                        digest if call_id == "legacy" else hashlib.sha256(b"[]").hexdigest(),
                        material if call_id == "legacy" else "[]",
                        "stored prompt",
                        " unique final review " if state == "succeeded" else None,
                        None,
                    ),
                )
            db.commit()
            before = tuple(db.iterdump())
            before_rows = {
                table: db.execute(f"SELECT * FROM {table}").fetchall()
                for table in ["calls", "turns", "generations", "reviews"]
            }
        with patch(
            "bot_call.store._PUBLICATION_SCHEMA",
            _PUBLICATION_SCHEMA + ("SELECT * FROM forced_upgrade_failure",),
        ):
            with self.assertRaises(sqlite3.OperationalError):
                Store(self.path)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(tuple(db.iterdump()), before)
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 2)
        with Store(self.path) as store:
            self.assertEqual(store._db.execute("PRAGMA user_version").fetchone()[0], 24)
            for table, rows in before_rows.items():
                self.assertEqual(
                    [tuple(row) for row in store._db.execute(f"SELECT * FROM {table}")], rows
                )
            self.assertEqual(store.get_binding("legacy"), target)
            self.assertEqual(store.get_transcript("legacy"), (user, bot))
            self.assertEqual(store.get_generation("legacy", "g"), generation)
            for state in ["pending", "running", "failed", "cancelled", "uncertain"]:
                self.assertEqual(store.get_review(state).state, state)
                with self.assertRaises(StateConflict):
                    store.prepare_publication(state)
            plan = store.prepare_publication("legacy", char_budget=112)
            self.assertEqual(plan.body, " unique final review ")
            page, claimed = store.claim_publication_page("legacy")
            self.assertTrue(claimed)
            store.finish_publication_page("legacy", page.number, "succeeded")
            with self.assertRaises(StateConflict):
                store.finish_publication_page("legacy", page.number, "failed")
            with self.assertRaises(sqlite3.IntegrityError):
                store._db.execute("UPDATE publication_pages SET start=1 WHERE call_id='legacy'")
            with self.assertRaises(sqlite3.IntegrityError):
                store._db.execute(
                    "UPDATE publication_pages SET state='pending' WHERE call_id='legacy'"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                store._db.execute("UPDATE publications SET char_budget=113 WHERE call_id='legacy'")
            with self.assertRaises(sqlite3.IntegrityError):
                store._db.execute("UPDATE reviews SET body='changed' WHERE call_id='legacy'")


if __name__ == "__main__":
    unittest.main()
