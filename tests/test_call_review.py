"""Three complete call/review scenarios; all databases and callbacks are synthetic."""

import asyncio
from contextlib import closing
from dataclasses import astuple
from pathlib import Path
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Barrier
import traceback
import unittest

from bot_call.binding import CallBinding
from bot_call.review import (
    DEFAULT_REVIEW_PROMPT,
    ReviewPersistenceError,
    end_call,
    run_pending,
)
from bot_call.service import process_utterance
from bot_call.store import (
    CallClosed,
    Generation,
    MaterialConflict,
    NotFound,
    StateConflict,
    Store,
    Turn,
    _APPLICATION_ID,
    _SCHEMA,
)


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


class CallReviewTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "calls.db"

    async def test_full_flow_migrates_v1_preserves_100_rounds_and_runs_without_end_caller(self):
        target = binding("legacy-call")
        legacy_user = Turn(
            target.call_id, 1, "user", "legacy-u", "legacy", " 原有用户\n ", 2, "not_applicable", 0
        )
        legacy_bot = Turn(
            target.call_id, 2, "assistant", "legacy-g", "fake_reply", "原有Bot", 3, "failed", 1
        )
        legacy_step = Generation(target.call_id, "legacy-g", 1, "fake_reply", "succeeded", 2)
        # Exact phase-2 base schema; no upgrade/down-grade reconstruction.
        with closing(sqlite3.connect(self.path)) as raw:
            for statement in _SCHEMA:
                raw.execute(statement)
            raw.execute(f"PRAGMA application_id={_APPLICATION_ID}")
            raw.execute("PRAGMA user_version=1")
            raw.execute(
                "INSERT INTO calls VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (*astuple(target), "BACKGROUND_ONLY_NOT_THIS_CALL", 1),
            )
            for turn in [legacy_user, legacy_bot]:
                raw.execute("INSERT INTO turns VALUES (?,?,?,?,?,?,?,?,?)", astuple(turn))
            raw.execute("INSERT INTO generations VALUES (?,?,?,?,?,?)", astuple(legacy_step))
            raw.commit()
        expected = [("user", legacy_user.body), ("assistant", legacy_bot.body)]
        with Store(self.path) as store:
            self.assertEqual(store._db.execute("PRAGMA user_version").fetchone()[0], 24)
            self.assertEqual(store.get_binding(target.call_id), target)
            self.assertEqual(store.get_transcript(target.call_id), (legacy_user, legacy_bot))
            self.assertEqual(store.get_generation(target.call_id, "legacy-g"), legacy_step)
            self.assertEqual(store.get_background(target.call_id), "BACKGROUND_ONLY_NOT_THIS_CALL")
            self.assertEqual(
                store._db.execute(
                    "SELECT created_at FROM calls WHERE call_id=?", (target.call_id,)
                ).fetchone()[0],
                1,
            )

            async def reply(bound, user):
                self.assertEqual(bound, target)
                if user.material_id == "u-49":
                    raise RuntimeError("synthetic reply failure")
                return " Bot建议而非用户决定：" + user.material_id + "\n "

            for index in range(100):
                topic = "开头家庭" if index < 33 else ("中段工作" if index < 66 else "结尾未定决定")
                text = (
                    f" \n{topic} {index}🙂 e\u0301\r\n"
                    + ("长" * 4501 if index == 0 else "")
                    + "\t "
                )
                expected.append(("user", text))
                if index == 49:
                    with self.assertRaises(RuntimeError):
                        await process_utterance(store, target.call_id, f"u-{index}", text, reply)
                else:
                    await process_utterance(store, target.call_id, f"u-{index}", text, reply)
                    expected.append(("assistant", " Bot建议而非用户决定：" + f"u-{index}" + "\n "))
            store.observe_playback(target.call_id, "reply:u-50", revision=1, state="failed")
            self.assertEqual(
                [(t.role, t.body) for t in store.get_transcript(target.call_id)], expected
            )
            committed = asyncio.Event()
            prompt = "PRIVATE_REVIEW_PROMPT：区分表达和建议"

            async def request_end():
                end_call(store, target.call_id, prompt=prompt)
                committed.set()
                await asyncio.Event().wait()

            page_task = asyncio.create_task(request_end())
            await asyncio.sleep(0)
            if page_task.done():
                await page_task
            self.assertTrue(committed.is_set())
            page_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await page_task
            self.assertEqual(store.get_review(target.call_id).state, "pending")
            # A later receipt changes live observations, not the frozen review input.
            store.observe_playback(target.call_id, "reply:u-0", revision=1, state="started")
        calls = 0
        result_text = " \n完整回顾：开头家庭／中段工作／结尾未定决定🙂\n "

        async def review(bound, turns, received_prompt):
            nonlocal calls
            calls += 1
            self.assertEqual(bound, target)
            self.assertEqual(received_prompt, prompt)
            self.assertEqual([(t.role, t.body) for t in turns], expected)
            self.assertEqual(len(turns), 201)
            self.assertNotIn("BACKGROUND_ONLY_NOT_THIS_CALL", "".join(t.body for t in turns))
            self.assertEqual(
                next(t for t in turns if t.material_id == "reply:u-0").playback_state, "unknown"
            )
            self.assertTrue(any(t.playback_state == "failed" for t in turns))
            await asyncio.sleep(0)
            return result_text

        with Store(self.path) as background:
            worker = asyncio.create_task(run_pending(background, review))
            result = (await worker)[0]
            self.assertEqual((result.state, result.body), ("succeeded", result_text))
            self.assertNotIn(result_text, repr(result))
            self.assertNotIn(prompt, repr(result))
            self.assertEqual(end_call(background, target.call_id, prompt=prompt), result)
            self.assertEqual(await run_pending(background, review), ())
            with self.assertRaises(MaterialConflict):
                end_call(background, target.call_id, prompt="different prompt")
        with Store(self.path) as reopened:
            saved = reopened.get_review(target.call_id)
            self.assertEqual((saved.body, saved.last_sequence), (result_text, 201))
            self.assertEqual(saved.input_sha256, result.input_sha256)
            self.assertEqual(
                [(t.role, t.body) for t in reopened.get_transcript(target.call_id)], expected
            )
            self.assertEqual(
                reopened.get_background(target.call_id), "BACKGROUND_ONLY_NOT_THIS_CALL"
            )
        self.assertEqual(calls, 1)

    async def test_end_races_with_acceptance_and_suppresses_late_reply_after_frozen_review(self):
        target = binding("race")
        with Store(self.path) as store:
            store.create_call(target)
            entered, release = asyncio.Event(), asyncio.Event()

            async def delayed_reply(bound, user):
                entered.set()
                await release.wait()
                return "LATE_BOT_MUST_NOT_APPEND"

            segment = asyncio.create_task(
                process_utterance(
                    store, target.call_id, "in-flight", "already accepted", delayed_reply
                )
            )
            try:
                await entered.wait()
                barrier = Barrier(2)

                def submit():
                    with Store(self.path) as connection:
                        barrier.wait(timeout=5)
                        try:
                            turn = connection.append_user(
                                target.call_id,
                                "racing-u",
                                "racing accepted text",
                                source="synthetic",
                            )
                            return True, turn.sequence
                        except CallClosed:
                            return False, None

                def close():
                    with Store(self.path) as connection:
                        barrier.wait(timeout=5)
                        return end_call(connection, target.call_id)

                (accepted, sequence), job = await asyncio.gather(
                    asyncio.to_thread(submit), asyncio.to_thread(close)
                )
                frozen = store.get_review_input(target.call_id)
                self.assertEqual(frozen[0].body, "already accepted")
                self.assertEqual(any(t.material_id == "racing-u" for t in frozen), accepted)
                if accepted:
                    self.assertTrue(
                        any(
                            t.sequence == sequence and t.body == "racing accepted text"
                            for t in frozen
                        )
                    )

                async def forbidden(bound, user):
                    self.fail("Closed/replayed material must not trigger another fake")

                with self.assertRaises(CallClosed):
                    await process_utterance(store, target.call_id, "new", "not accepted", forbidden)
                with self.assertRaises(CallClosed):
                    store.append_user(
                        target.call_id, "new-direct", "not accepted", source="synthetic"
                    )
                replay = await process_utterance(
                    store, target.call_id, "in-flight", "already accepted", forbidden
                )
                self.assertEqual(replay.state, "cancelled")
                with self.assertRaises(MaterialConflict):
                    store.append_user(
                        target.call_id, "in-flight", "changed", source="simulated_stt"
                    )
                with self.assertRaises(StateConflict):
                    store.complete_generation(target.call_id, "reply:in-flight", "late")

                async def review(bound, turns, prompt):
                    self.assertEqual(turns, frozen)
                    return "review of the frozen, confirmed user text"

                self.assertEqual((await run_pending(store, review))[0].state, "succeeded")
                release.set()
                self.assertEqual((await segment).state, "cancelled")
                self.assertEqual(store.get_transcript(target.call_id), frozen)
                self.assertEqual(store.get_review_input(target.call_id), frozen)
                self.assertEqual(store.get_review(target.call_id).input_sha256, job.input_sha256)
            finally:
                release.set()
                await segment

    async def test_review_faults_preserve_material_and_uncertain_work_is_not_retried(self):
        with Store(self.path) as store:
            expected_states = {}
            for mode in ["fail", "cancel", "commit"]:
                target = binding(mode)
                store.create_call(target, background="background stays separate")
                store.append_user(target.call_id, "u", " 完整材料🙂\n ", source="synthetic")
                end_call(store, target.call_id)
                before = store.get_review_input(target.call_id)
                calls, update_finished = [], []
                entered = asyncio.Event()
                if mode == "commit":
                    store._db.create_function(
                        "mark_update_finished", 0, lambda: update_finished.append(True)
                    )
                    store._db.execute("""
                        CREATE TEMP TRIGGER fail_review_commit AFTER UPDATE OF state ON reviews
                        WHEN NEW.state='succeeded'
                        BEGIN
                            INSERT INTO generations VALUES (
                                NEW.call_id,'invalid-review-tx',999999,'synthetic','running',NULL
                            );
                            SELECT mark_update_finished();
                        END
                    """)

                async def generate(bound, turns, prompt):
                    calls.append(bound.call_id)
                    if mode == "fail":
                        raise RuntimeError("PRIVATE_BODY_PROMPT_CREDENTIAL")
                    if mode == "cancel":
                        entered.set()
                        await asyncio.Event().wait()
                    store._db.execute("PRAGMA defer_foreign_keys=ON")
                    return "must not report this uncommitted review as success"

                if mode == "fail":
                    result = (await run_pending(store, generate))[0]
                    self.assertEqual((result.state, result.error_code), ("failed", "fake_failed"))
                    expected_states[mode] = "failed"
                elif mode == "cancel":
                    task = asyncio.create_task(run_pending(store, generate))
                    await entered.wait()
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                    expected_states[mode] = "cancelled"
                else:
                    with self.assertRaises(ReviewPersistenceError) as error:
                        await run_pending(store, generate)
                    self.assertNotIn("PRIVATE", str(error.exception))
                    self.assertEqual(
                        update_finished, [True], "UPDATE completed before COMMIT failed"
                    )
                    self.assertEqual(store.get_review(mode).state, "uncertain")
                    with self.assertRaises(NotFound):
                        store.get_generation(mode, "invalid-review-tx")
                    store._db.execute("DROP TRIGGER fail_review_commit")
                    expected_states[mode] = "uncertain"
                self.assertEqual(calls, [mode])
                self.assertIsNone(store.get_review(mode).body)
                self.assertEqual(store.get_review_input(mode), before)
                self.assertEqual(store.get_transcript(mode), before)
                self.assertNotIn("PRIVATE_BODY_PROMPT_CREDENTIAL", repr(store.get_review(mode)))
            # Failure/cancellation reporting can itself fail. Never expose the
            # fake exception through an implicit traceback chain or auto-retry.
            for mode in ["failed", "invalid", "cancelled", "uncertain-write"]:
                with self.subTest(outcome_write=mode):
                    call_id = "outcome-" + mode
                    store.create_call(binding(call_id))
                    store.append_user(call_id, "u", "retained complete text", source="synthetic")
                    end_call(store, call_id)
                    before = store.get_review_input(call_id)
                    blocked = (
                        "'failed','cancelled','uncertain'"
                        if mode == "uncertain-write"
                        else "'failed','cancelled'"
                    )
                    store._db.execute(f"""
                        CREATE TEMP TRIGGER abort_outcome BEFORE UPDATE OF state ON reviews
                        WHEN NEW.state IN ({blocked})
                        BEGIN SELECT RAISE(ABORT, 'synthetic outcome write failure'); END
                    """)
                    entered = asyncio.Event()
                    calls = []

                    async def faulty_outcome(bound, turns, prompt):
                        calls.append(bound.call_id)
                        if mode == "cancelled":
                            entered.set()
                            await asyncio.Event().wait()
                        if mode == "invalid":
                            return "  "
                        raise RuntimeError("PRIVATE_BODY_PROMPT_CREDENTIAL")

                    if mode == "cancelled":
                        task = asyncio.create_task(run_pending(store, faulty_outcome))
                        await entered.wait()
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                    else:
                        try:
                            await run_pending(store, faulty_outcome)
                        except ReviewPersistenceError as error:
                            self.assertNotIn(
                                "PRIVATE_BODY_PROMPT_CREDENTIAL",
                                "".join(traceback.format_exception(error)),
                            )
                        else:
                            self.fail("An uncommitted outcome must not report success")
                    state = "running" if mode == "uncertain-write" else "uncertain"
                    self.assertEqual(store.get_review(call_id).state, state)
                    self.assertIsNone(store.get_review(call_id).body)
                    self.assertEqual(store.get_review_input(call_id), before)
                    self.assertEqual(store.get_transcript(call_id), before)
                    self.assertEqual(calls, [call_id])
                    store._db.execute("DROP TRIGGER abort_outcome")
                    self.assertEqual(await run_pending(store, faulty_outcome), ())
                    expected_states[call_id] = state
            orphan = binding("orphan")
            store.create_call(orphan)
            store.append_user("orphan", "u", "committed before worker crash", source="synthetic")
            end_call(store, "orphan")
            crashed = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-c",
                    "from bot_call.store import Store; import sys,os; "
                    "s=Store(sys.argv[1]); job,claimed=s.claim_review('orphan'); "
                    "assert claimed; os._exit(23)",
                    str(self.path),
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(crashed.returncode, 23, crashed.stderr)
            self.assertEqual(store.get_review("orphan").state, "running")

            async def never(bound, turns, prompt):
                self.fail("Failed/cancelled/running/uncertain jobs must not auto-retry")

            self.assertEqual(await run_pending(store, never), ())
            # The operator knows this synthetic worker has exited, so reconciliation
            # may explicitly mark uncertain. A normal connection open never does.
            self.assertEqual(store.mark_review_uncertain("orphan").state, "uncertain")
            expected_states["orphan"] = "uncertain"
        with Store(self.path) as reopened:
            self.assertEqual(await run_pending(reopened, never), ())
            for call_id, state in expected_states.items():
                self.assertEqual(reopened.get_review(call_id).state, state)
                self.assertIsNone(reopened.get_review(call_id).body)
                self.assertTrue(reopened.get_review_input(call_id))


if __name__ == "__main__":
    unittest.main()
