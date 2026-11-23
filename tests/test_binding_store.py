"""Behavior checks with synthetic identities, temporary databases and fake replies."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import FrozenInstanceError, replace
from enum import Enum
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from bot_call.binding import BindingError, bind_call
from bot_call.service import process_utterance
from bot_call.store import MaterialConflict, NotFound, StateConflict, Store


class MessageType(Enum):
    FRIEND = "FriendMessage"


class FakeEvent:
    def __init__(self, platform="qq-1", sender="user", session=None, platform_type="aiocqhttp"):
        self.platform = platform
        self.platform_type = platform_type
        self.sender = sender
        self.session = sender if session is None else session
        self.unified_msg_origin = f"{platform}:FriendMessage:{self.session}"

    def get_sender_id(self):
        return self.sender

    def get_self_id(self):
        return "synthetic-bot"

    def get_platform_id(self):
        return self.platform

    def get_platform_name(self):
        return self.platform_type

    def get_message_type(self):
        return MessageType.FRIEND

    def get_session_id(self):
        return self.session


class FakeConversations:
    """Read-only interface; no create/switch/write method is supplied."""

    def __init__(self, event, cid="A", persona=None):
        self.current = {event.unified_msg_origin: cid}
        self.conversations = {
            cid: SimpleNamespace(
                cid=cid,
                user_id=event.unified_msg_origin,
                platform_id=event.platform,
                persona_id=persona,
                history=["synthetic previous history"],
            )
        }
        self.lookups = []

    async def get_curr_conversation_id(self, umo):
        self.lookups.append(("current", umo))
        return self.current.get(umo)

    async def get_conversation(self, umo, cid):
        self.lookups.append(("conversation", umo, cid))
        return self.conversations.get(cid)


class BindingTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_sender_distinct_instances_and_origins_are_isolated(self):
        with TemporaryDirectory() as directory, Store(Path(directory) / "calls.db") as store:
            targets = []
            for platform, session, cid, platform_type in [
                ("qq-1", "user", "A", "aiocqhttp"),
                ("qq-2", "user", "B", "aiocqhttp"),
                ("qq-1", "separate-private-session", "C", "aiocqhttp"),
                ("official-1", "opaque:private:session", "D", "qq_official"),
            ]:
                event = FakeEvent(platform, session=session, platform_type=platform_type)
                binding = await bind_call(event, FakeConversations(event, cid))
                store.create_call(binding)
                store.append_user(binding.call_id, "same-utterance", cid, source="synthetic")
                targets.append(binding)
            self.assertEqual(len({b.unified_msg_origin for b in targets}), 4)
            for binding in targets:
                self.assertEqual(store.get_binding(binding.call_id), binding)
                self.assertEqual(
                    store.get_transcript(binding.call_id)[0].body, binding.conversation_id
                )

    async def test_pointer_switch_does_not_move_binding_or_touch_history(self):
        event = FakeEvent()
        reader = FakeConversations(event)
        binding = await bind_call(event, reader)
        reader.conversations["B"] = SimpleNamespace(history=[])
        reader.current[event.unified_msg_origin] = "B"
        reader.conversations["A"].history.append("QQ message A during call")
        reader.conversations["B"].history.append("QQ message B during call")
        with TemporaryDirectory() as directory:
            path = Path(directory) / "calls.db"
            with Store(path) as store:
                store.create_call(binding)
                store.append_user(binding.call_id, "u", "call-only text", source="synthetic")
            with Store(path) as reopened:
                self.assertEqual(reopened.get_binding(binding.call_id).conversation_id, "A")
        self.assertEqual(reader.current[event.unified_msg_origin], "B")
        self.assertEqual(
            reader.conversations["A"].history,
            [
                "synthetic previous history",
                "QQ message A during call",
            ],
        )
        self.assertEqual(reader.conversations["B"].history, ["QQ message B during call"])
        self.assertEqual(len(reader.lookups), 2)

    async def test_missing_conversation_and_wrong_ownership_are_rejected(self):
        for field, value in [
            ("user_id", "other:FriendMessage:user"),
            ("platform_id", "qq-2"),
            ("cid", "not-A"),
        ]:
            with self.subTest(field=field):
                event = FakeEvent()
                reader = FakeConversations(event)
                setattr(reader.conversations["A"], field, value)
                with self.assertRaises(BindingError):
                    await bind_call(event, reader)
        event = FakeEvent()
        reader = FakeConversations(event)
        reader.current.clear()
        with self.assertRaisesRegex(BindingError, "No current"):
            await bind_call(event, reader)
        reader.current[event.unified_msg_origin] = "gone"
        with self.assertRaisesRegex(BindingError, "no longer"):
            await bind_call(event, reader)

    async def test_event_consistency_and_default_persona(self):
        event = FakeEvent()
        for persona in [None, "", "default", "[%None]", "persona-A"]:
            binding = await bind_call(event, FakeConversations(event, persona=persona))
            self.assertEqual(binding.persona_id, persona)
        event.unified_msg_origin = "qq-2:FriendMessage:user"
        with self.assertRaises(BindingError):
            await bind_call(event, FakeConversations(event))
        event = FakeEvent()
        event.get_message_type = lambda: "GroupMessage"
        with self.assertRaises(BindingError):
            await bind_call(event, FakeConversations(event))
        event = FakeEvent(sender="")
        with self.assertRaises(BindingError):
            await bind_call(event, FakeConversations(event))


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "calls.db"
        event = FakeEvent()
        self.binding = asyncio.run(bind_call(event, FakeConversations(event)))
        self.call_id = self.binding.call_id
        self.store = Store(self.path)
        self.addCleanup(self.store.close)
        self.store.create_call(self.binding, background="  Background only\n旧事🧭  ")

    def add_reply(self, utterance="u", body="reply"):
        self.store.append_user(self.call_id, utterance, "user", source="synthetic")
        generation_id = "reply:" + utterance
        self.store.claim_generation(
            self.call_id, generation_id, utterance_id=utterance, source="fake_reply"
        )
        return self.store.complete_generation(self.call_id, generation_id, body)

    def test_full_100_rounds_preserve_exact_text_and_background_is_separate(self):
        expected = []
        for index in range(100):
            user = f" \t\n用户{index}：🙂 e\u0301\x00\r\n" + "长文" * 2300 + " \n "
            bot = f" \r\nBot {index} Ω\n" + "答" * 4101 + "\t "
            utterance = f"u-{index}"
            self.store.append_user(self.call_id, utterance, user, source="synthetic")
            self.store.claim_generation(
                self.call_id, f"g-{index}", utterance_id=utterance, source="fake_reply"
            )
            self.store.complete_generation(self.call_id, f"g-{index}", bot)
            expected.extend([("user", user), ("assistant", bot)])
        self.store.close()
        with Store(self.path) as reopened:
            turns = reopened.get_transcript(self.call_id)
            self.assertEqual(len(turns), 200)
            self.assertEqual([(t.role, t.body) for t in turns], expected)
            self.assertEqual([t.sequence for t in turns], list(range(1, 201)))
            self.assertEqual(reopened.get_background(self.call_id), "  Background only\n旧事🧭  ")
            self.assertTrue(all(t.body != reopened.get_background(self.call_id) for t in turns))

    def test_binding_cannot_be_changed_by_dict_copy_or_readback(self):
        readback = self.store.get_binding(self.call_id)
        with self.assertRaises(FrozenInstanceError):
            readback.conversation_id = "B"
        with self.assertRaises(TypeError):
            self.store.create_call({"call_id": self.call_id, "conversation_id": "B"})
        with self.assertRaises(MaterialConflict):
            self.store.create_call(replace(readback, conversation_id="B"))
        with self.assertRaises(MaterialConflict):
            self.store.create_call(readback, background="different")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store._db.execute(
                "UPDATE calls SET conversation_id='B' WHERE call_id=?", (self.call_id,)
            )
        self.assertEqual(self.store.get_binding(self.call_id), self.binding)
        self.assertEqual(
            self.store.create_call(readback, background="  Background only\n旧事🧭  "), readback
        )

    def test_stable_ids_replay_and_conflict_do_not_overwrite(self):
        first = self.store.append_user(self.call_id, "u", " exact \n", source="stt")
        self.assertEqual(
            self.store.append_user(self.call_id, "u", " exact \n", source="stt"), first
        )
        for body, source in [("exact", "stt"), (" exact \n", "different")]:
            with self.assertRaises(MaterialConflict):
                self.store.append_user(self.call_id, "u", body, source=source)
        step, winner = self.store.claim_generation(
            self.call_id, "g", utterance_id="u", source="fake_reply"
        )
        self.assertTrue(winner)
        self.assertEqual(
            self.store.claim_generation(self.call_id, "g", utterance_id="u", source="fake_reply"),
            (step, False),
        )
        for gen, source in [("g", "other"), ("different", "fake_reply")]:
            with self.assertRaises(MaterialConflict):
                self.store.claim_generation(self.call_id, gen, utterance_id="u", source=source)
        bot = self.store.complete_generation(self.call_id, "g", " bot \n")
        self.assertEqual(self.store.complete_generation(self.call_id, "g", " bot \n"), bot)
        with self.assertRaises(MaterialConflict):
            self.store.complete_generation(self.call_id, "g", "different")
        self.store.append_user(self.call_id, "u2", "other", source="stt")
        with self.assertRaises(MaterialConflict):
            self.store.claim_generation(self.call_id, "g", utterance_id="u2", source="fake_reply")
        self.assertEqual(len(self.store.get_transcript(self.call_id)), 3)

    def test_playback_unknown_failure_stale_and_duplicate_observations(self):
        bot = self.add_reply(body="  Generated, not necessarily heard\n ")
        self.assertEqual(bot.playback_state, "unknown")
        started = self.store.observe_playback(self.call_id, "reply:u", revision=2, state="started")
        failed = self.store.observe_playback(self.call_id, "reply:u", revision=4, state="failed")
        self.assertEqual(failed.body, bot.body)
        self.assertEqual(
            self.store.observe_playback(self.call_id, "reply:u", revision=1, state="ended"), failed
        )
        self.assertEqual(
            self.store.observe_playback(self.call_id, "reply:u", revision=4, state="failed"), failed
        )
        with self.assertRaises(MaterialConflict):
            self.store.observe_playback(self.call_id, "reply:u", revision=4, state="ended")
        with self.assertRaises(StateConflict):
            self.store.observe_playback(self.call_id, "reply:u", revision=5, state="started")
        with self.assertRaises(StateConflict):
            self.store.finish_without_reply(self.call_id, "reply:u", "failed")
        self.assertEqual(started.body, bot.body)
        with self.assertRaises(FrozenInstanceError):
            failed.body = "overwrite"
        with Store(self.path) as reopened:
            saved = reopened.get_transcript(self.call_id)[-1]
            self.assertEqual((saved.body, saved.playback_state), (bot.body, "failed"))

    def test_observation_validation_and_missing_start_receipt(self):
        self.add_reply()
        self.assertEqual(
            self.store.observe_playback(
                self.call_id, "reply:u", revision=3, state="ended"
            ).playback_state,
            "ended",
        )
        self.add_reply("other")
        self.store.observe_playback(self.call_id, "reply:other", revision=1, state="started")
        with self.assertRaises(StateConflict):
            self.store.observe_playback(self.call_id, "reply:other", revision=2, state="unknown")
        for revision in [0, -1, True, 1.5]:
            with self.assertRaises(ValueError):
                self.store.observe_playback(
                    self.call_id, "reply:u", revision=revision, state="failed"
                )
        with self.assertRaises(ValueError):
            self.store.observe_playback(self.call_id, "reply:u", revision=5, state="heard")
        with self.assertRaises(NotFound):
            self.store.observe_playback(self.call_id, "absent", revision=1, state="started")

    def test_multiple_connections_append_and_replay_without_sequence_conflicts(self):
        def worker(index):
            with Store(self.path) as connection:
                observed = []
                for offset in range(12):
                    utterance = f"{index}-{offset}"
                    user = connection.append_user(
                        self.call_id, utterance, utterance, source="synthetic"
                    )
                    connection.claim_generation(
                        self.call_id,
                        "reply:" + utterance,
                        utterance_id=utterance,
                        source="fake_reply",
                    )
                    bot = connection.complete_generation(
                        self.call_id, "reply:" + utterance, "bot:" + utterance
                    )
                    observed.extend([user.sequence, bot.sequence])
                    connection.append_user(
                        self.call_id, "shared", "shared material", source="synthetic"
                    )
                return observed

        with ThreadPoolExecutor(max_workers=6) as pool:
            sequences = [seq for result in pool.map(worker, range(6)) for seq in result]
        turns = self.store.get_transcript(self.call_id)
        self.assertEqual(len(turns), 145)
        self.assertEqual([t.sequence for t in turns], list(range(1, 146)))
        self.assertEqual(len(set(sequences)), 144)
        self.assertEqual(sum(t.material_id == "shared" for t in turns), 1)
        for index in range(6):
            for offset in range(12):
                utterance = f"{index}-{offset}"
                self.assertEqual(
                    sum(t.material_id == utterance and t.body == utterance for t in turns), 1
                )

    def test_actual_commit_failure_returns_no_ack_and_rolls_back_all_material(self):
        # Deferred foreign-key violation occurs at commit, after the user INSERT.
        after_insert = []
        self.store._db.create_function("mark_insert_complete", 0, lambda: after_insert.append(True))
        self.store._db.execute("PRAGMA defer_foreign_keys=ON")
        self.store._db.execute("""
            CREATE TEMP TRIGGER fail_at_commit AFTER INSERT ON turns
            BEGIN
                INSERT INTO generations VALUES (
                    NEW.call_id, 'invalid-reference', 9999999, 'synthetic', 'running', NULL
                );
                SELECT mark_insert_complete();
            END
        """)
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.append_user(self.call_id, "broken", "uncommitted", source="synthetic")
        self.assertEqual(after_insert, [True], "INSERT completed; the failure must occur at COMMIT")
        with Store(self.path) as reopened:
            self.assertEqual(reopened.get_transcript(self.call_id), ())
            with self.assertRaises(NotFound):
                reopened.get_generation(self.call_id, "invalid-reference")
        self.assertFalse(self.store._db.in_transaction)

    def test_bot_and_success_state_are_atomic_on_statement_failure(self):
        self.store.append_user(self.call_id, "u", "already committed", source="synthetic")
        self.store.claim_generation(self.call_id, "g", utterance_id="u", source="fake_reply")
        self.store._db.execute("""
            CREATE TEMP TRIGGER abort_success BEFORE UPDATE OF state ON generations
            WHEN NEW.state='succeeded'
            BEGIN SELECT RAISE(ABORT, 'synthetic transaction failure'); END
        """)
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.complete_generation(self.call_id, "g", "must roll back")
        with Store(self.path) as reopened:
            self.assertEqual(
                [t.body for t in reopened.get_transcript(self.call_id)], ["already committed"]
            )
            self.assertEqual(reopened.get_generation(self.call_id, "g").state, "running")

    def test_process_crash_rolls_back_and_new_process_can_read_committed_text(self):
        self.store.append_user(
            self.call_id, "committed", "完整\n already saved ", source="synthetic"
        )
        crash = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                "import sqlite3,sys,os; "
                "c=sqlite3.connect(sys.argv[1],isolation_level=None); "
                "c.execute('BEGIN IMMEDIATE'); "
                "c.execute(\"INSERT INTO turns VALUES (?,2,'user','crash','synthetic','not saved',0,'not_applicable',0)\",(sys.argv[2],)); "
                "os._exit(17)",
                str(self.path),
                self.call_id,
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(crash.returncode, 17, crash.stderr)
        read = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                "from bot_call.store import Store; import sys,json,hashlib; "
                "s=Store(sys.argv[1]); t=s.get_transcript(sys.argv[2]); "
                "print(json.dumps([len(t),hashlib.sha256(t[0].body.encode()).hexdigest()])); s.close()",
                str(self.path),
                self.call_id,
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(read.returncode, 0, read.stderr)
        self.assertEqual(
            json.loads(read.stdout),
            [
                1,
                hashlib.sha256("完整\n already saved ".encode()).hexdigest(),
            ],
        )

    def test_foreign_database_rejected_and_connections_close(self):
        foreign = Path(self.directory.name) / "foreign.db"
        with closing(sqlite3.connect(foreign)) as connection:
            connection.execute("CREATE TABLE unrelated(value TEXT)")
        with self.assertRaises(ValueError):
            Store(foreign)
        with closing(sqlite3.connect(foreign)) as connection:
            self.assertEqual(connection.execute("PRAGMA application_id").fetchone()[0], 0)
            self.assertEqual(
                connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(),
                [("unrelated",)],
            )
        with Store(self.path) as connection:
            raw = connection._db
        with self.assertRaises(sqlite3.ProgrammingError):
            raw.execute("SELECT 1")
        connection.close()


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "calls.db"
        event = FakeEvent()
        self.binding = await bind_call(event, FakeConversations(event))
        self.call_id = self.binding.call_id
        self.store = Store(self.path)
        self.addCleanup(self.store.close)
        self.store.create_call(self.binding)

    async def test_user_committed_before_failing_fake_and_replay_does_not_retry(self):
        calls = 0

        async def fail(binding, user):
            nonlocal calls
            calls += 1
            self.assertEqual(binding, self.binding)
            with Store(self.path) as observer:
                self.assertEqual(observer.get_transcript(binding.call_id)[0], user)
            raise RuntimeError("synthetic model failure")

        with self.assertRaises(RuntimeError):
            await process_utterance(self.store, self.call_id, "u", "  已识别\n ", fail)
        with Store(self.path) as reopened:
            self.assertEqual(
                [t.body for t in reopened.get_transcript(self.call_id)], ["  已识别\n "]
            )
            replay = await process_utterance(reopened, self.call_id, "u", "  已识别\n ", fail)
            self.assertEqual(replay.state, "failed")
        self.assertEqual(calls, 1)

    async def test_task_cancel_preserves_committed_user_and_no_bot(self):
        entered = asyncio.Event()

        async def wait(binding, user):
            with Store(self.path) as observer:
                self.assertEqual(observer.get_transcript(binding.call_id)[0].body, user.body)
            entered.set()
            await asyncio.Event().wait()
            return "unreachable"

        task = asyncio.create_task(
            process_utterance(self.store, self.call_id, "u", "cancel-safe text", wait)
        )
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        with Store(self.path) as reopened:
            self.assertEqual([t.role for t in reopened.get_transcript(self.call_id)], ["user"])
            self.assertEqual(reopened.get_generation(self.call_id, "reply:u").state, "cancelled")

    async def test_empty_and_invalid_fake_results_never_record_bot_success(self):
        for index, body in enumerate(["", " \t\n", None]):
            with self.subTest(body=body):

                async def fake(binding, user):
                    return body

                if body is None:
                    with self.assertRaises(TypeError):
                        await process_utterance(self.store, self.call_id, str(index), "user", fake)
                else:
                    step = await process_utterance(
                        self.store, self.call_id, str(index), "user", fake
                    )
                    self.assertEqual(step.state, "empty")
        self.assertEqual([t.role for t in self.store.get_transcript(self.call_id)], ["user"] * 3)

    async def test_generated_body_is_preserved_independently_of_playback(self):
        async def fake(binding, user):
            return "  回复🙂\r\n完整尾部\t "

        for utterance in ["unknown", "failed"]:
            step = await process_utterance(self.store, self.call_id, utterance, "user", fake)
            self.assertEqual(step.state, "succeeded")
        self.store.observe_playback(self.call_id, "reply:failed", revision=1, state="failed")
        with Store(self.path) as reopened:
            bots = [t for t in reopened.get_transcript(self.call_id) if t.role == "assistant"]
            self.assertEqual([t.body for t in bots], ["  回复🙂\r\n完整尾部\t "] * 2)
            self.assertEqual([t.playback_state for t in bots], ["unknown", "failed"])
            self.assertNotIn(bots[0].body, repr(bots[0]))

    async def test_concurrent_replays_claim_once_and_later_conflicts_do_not_call_fake(self):
        calls = 0
        entered, release = asyncio.Event(), asyncio.Event()

        async def fake(binding, user):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            return "single generated reply"

        task = asyncio.create_task(
            process_utterance(self.store, self.call_id, "u", "same exact text", fake)
        )
        await entered.wait()
        with Store(self.path) as second:
            pending = await process_utterance(second, self.call_id, "u", "same exact text", fake)
            self.assertEqual(pending.state, "running")
            release.set()
            self.assertEqual((await task).state, "succeeded")
            self.assertEqual(
                (await process_utterance(second, self.call_id, "u", "same exact text", fake)).state,
                "succeeded",
            )
            with self.assertRaises(MaterialConflict):
                await process_utterance(second, self.call_id, "u", "different text", fake)
            with self.assertRaises(MaterialConflict):
                await process_utterance(
                    second, self.call_id, "u", "same exact text", fake, source="other_source"
                )
        self.assertEqual(calls, 1)
        self.assertEqual(len(self.store.get_transcript(self.call_id)), 2)

    async def test_persisted_running_step_has_no_automatic_takeover(self):
        self.store.append_user(self.call_id, "u", "saved", source="simulated_stt")
        self.store.claim_generation(self.call_id, "reply:u", utterance_id="u", source="fake_reply")

        async def must_not_run(binding, user):
            self.fail("An uncertain running step must not invoke another model")

        with Store(self.path) as reopened:
            step = await process_utterance(reopened, self.call_id, "u", "saved", must_not_run)
            self.assertEqual(step.state, "running")
        self.assertEqual(len(self.store.get_transcript(self.call_id)), 1)

    async def test_no_output_of_bodies_and_no_caller_target_overrides(self):
        import contextlib
        import io
        import logging

        captured = []

        class Capture(logging.Handler):
            def emit(self, record):
                captured.append(record.getMessage())

        logger = logging.getLogger()
        previous_level = logger.level
        logger.setLevel(logging.DEBUG)
        self.addCleanup(logger.setLevel, previous_level)
        handler = Capture()
        logger.addHandler(handler)
        self.addCleanup(logger.removeHandler, handler)
        sentinel = "Synthetic body must not appear in logs\n🙂"

        async def fake(binding, user):
            self.assertEqual(binding, self.binding)
            return sentinel

        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            await process_utterance(self.store, self.call_id, "u", sentinel, fake)
            with self.assertRaises(TypeError):
                await process_utterance(
                    self.store,
                    self.call_id,
                    "other",
                    "text",
                    fake,
                    unified_msg_origin="forged",
                    conversation_id="B",
                )
        self.assertNotIn(sentinel, stdout.getvalue() + stderr.getvalue() + "".join(captured))
        self.assertEqual(self.store.get_binding(self.call_id), self.binding)
        self.assertEqual(len(self.store.get_transcript(self.call_id)), 2)


if __name__ == "__main__":
    unittest.main()
