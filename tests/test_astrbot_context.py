"""Four whole synthetic context-import scenarios; no real ConversationManager."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
import traceback
from types import SimpleNamespace
import unittest

from bot_call.astrbot_adapter import LoopMismatch
from bot_call.astrbot_context import AstrBotContextAdapter, ContextRejected, ContextUncertain
from bot_call.binding import CallBinding
from bot_call.publication import Outcome, advance_publication
from bot_call.review import end_call, run_pending
from bot_call.store import Store


PRIVATE = "PRIVATE_BODY_PROMPT_CREDENTIAL"
BODY = "  原样回顾🙂e\u0301\r\n\n用户犹豫；Bot建议不等于决定\n " + PRIVATE


def binding(call_id="call", platform="qq-1", cid="A"):
    return CallBinding(
        call_id,
        "user",
        "bot",
        platform,
        "aiocqhttp",
        "FriendMessage",
        "user",
        f"{platform}:FriendMessage:user",
        cid,
        None,
    )


class SharedLocks:
    def __init__(self):
        self.locks = {}
        self.held = set()
        self.owners = {}
        self.requests = []

    @asynccontextmanager
    async def acquire_lock(self, key):
        self.requests.append(key)
        lock = self.locks.setdefault(key, asyncio.Lock())
        async with lock:
            self.held.add(key)
            self.owners[key] = asyncio.current_task()
            try:
                yield
            finally:
                self.held.remove(key)
                self.owners.pop(key)


class Conversations:
    def __init__(self, locks, target):
        self.locks, self.target = locks, target
        self.current = "B"
        self.history = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "prior"},
                    {"type": "image_url", "image_url": {"url": "synthetic://image"}},
                ],
                "extra_vendor": {"preserve": [1, 2]},
            },
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "type": "function",
                        "function": {"name": "fake", "arguments": "{}"},
                    }
                ],
            },
            {"role": "tool", "tool_call_id": "tool-1", "content": "tool result"},
        ]
        self.b_history = [{"role": "user", "content": "B unchanged"}]
        self.token_usage = 999
        self.writes, self.reads = [], []
        self.mode = "normal"
        self.override = {}
        self.write_entered = asyncio.Event()
        self.model_entered = asyncio.Event()
        self.model_reads = 0

    async def get_conversation(
        self, unified_msg_origin, conversation_id, create_if_not_exists=False
    ):
        assert unified_msg_origin in self.locks.held
        assert create_if_not_exists is False
        self.reads.append((unified_msg_origin, conversation_id, create_if_not_exists))
        if self.mode == "missing":
            return None
        if self.writes and self.mode == "readback-fails":
            raise RuntimeError(PRIVATE)
        history = self.history
        if self.writes and self.mode == "readback-missing":
            history = self.history[:-2]
        values = dict(
            cid=self.target.conversation_id,
            user_id=self.target.unified_msg_origin,
            platform_id=self.target.platform_id,
            persona_id=None,
            history=json.dumps(history, ensure_ascii=False),
            token_usage=self.token_usage,
        )
        values.update(self.override)
        return SimpleNamespace(**values)

    async def update_conversation(
        self,
        unified_msg_origin,
        conversation_id=None,
        history=None,
        title=None,
        persona_id=None,
        token_usage=None,
    ):
        assert unified_msg_origin in self.locks.held
        assert (unified_msg_origin, conversation_id) == (
            self.target.unified_msg_origin,
            self.target.conversation_id,
        )
        assert title is None and persona_id is None and token_usage == 0
        self.writes.append((unified_msg_origin, conversation_id, deepcopy(history), token_usage))
        self.history = deepcopy(history)
        self.token_usage = token_usage
        if self.mode == "write-confirmation-lost":
            raise RuntimeError(PRIVATE)
        if self.mode == "cancel-write":
            self.write_entered.set()
            await asyncio.Event().wait()

    async def model_input(self, target):
        self.model_reads += 1
        if self.mode == "cancel-model":
            self.model_entered.set()
            await asyncio.Event().wait()
        async with self.locks.acquire_lock(target.unified_msg_origin):
            conversation = await self.get_conversation(
                target.unified_msg_origin, target.conversation_id, create_if_not_exists=False
            )
            # Mirrors the statically verified req.contexts=json.loads(history)
            # input boundary; this does not execute a real runner or Provider.
            messages = json.loads(conversation.history)
            return messages[:-2] if self.mode == "model-missing" else messages


class ContextTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "calls.db"

    def fake(self, target, reader=True):
        locks = SharedLocks()
        manager = Conversations(locks, target)
        adapter = AstrBotContextAdapter(
            SimpleNamespace(conversation_manager=manager),
            session_lock_manager=locks,
            model_input_reader=manager.model_input if reader else None,
        )
        return locks, manager, adapter

    async def prepare(self, store, target):
        store.create_call(target, background="separate background")
        store.append_user(target.call_id, "u", "complete original user text", source="synthetic")
        end_call(store, target.call_id, prompt="persistent prompt")

        async def generate(*args):
            return BODY

        await run_pending(store, generate)
        return store.prepare_publication(target.call_id, char_budget=180)

    async def test_publication_imports_latest_fixed_history_and_confirms_only_model_input(self):
        target = binding()
        locks, manager, adapter = self.fake(target)
        original = deepcopy(manager.history)
        with Store(self.path) as store:
            plan = await self.prepare(store, target)
            sql_under_lock = []

            def authorize(*args):
                if asyncio.current_task() in locks.owners.values():
                    sql_under_lock.append(True)
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            store._db.set_authorizer(authorize)
            sent = []

            async def send(bound, page):
                sent.append(page.number)
                return Outcome.CONFIRMED

            async with locks.acquire_lock(target.unified_msg_origin):
                worker = asyncio.create_task(
                    advance_publication(store, target.call_id, send, adapter.make_context_readable)
                )

                async def collect_worker():
                    if not worker.done():
                        worker.cancel()
                    await asyncio.gather(worker, return_exceptions=True)

                self.addAsyncCleanup(collect_worker)
                # The first import request must be waiting on the shared lock.
                for _ in range(100):
                    if worker.done():
                        await worker
                    if len(locks.requests) > 1:
                        break
                    await asyncio.sleep(0)
                self.assertGreater(len(locks.requests), 1)
                self.assertEqual(manager.writes, [])
                manager.history.append({"role": "user", "content": "A QQ message arrived later"})
            result = await worker
            self.assertTrue(result.complete)
            self.assertEqual(len(manager.writes), 1)
            self.assertEqual(manager.token_usage, 0)
            self.assertEqual(manager.history[:-3], original)
            self.assertEqual(
                manager.history[-3], {"role": "user", "content": "A QQ message arrived later"}
            )
            system, assistant = manager.history[-2:]
            self.assertEqual(set(system), {"role", "content"})
            self.assertEqual(system["role"], "system")
            self.assertIn("系统导入的本通电话回顾，非用户原话", system["content"])
            self.assertIn(plan.context_key, system["content"])
            self.assertEqual(
                assistant, {"role": "assistant", "content": store.get_review(target.call_id).body}
            )
            self.assertEqual(manager.current, "B")
            self.assertEqual(manager.b_history, [{"role": "user", "content": "B unchanged"}])
            self.assertEqual(sql_under_lock, [])
            receipt = await adapter.import_review(target, plan.context_key, BODY)
            self.assertTrue(receipt.already_present)
            self.assertNotIn(plan.context_key, repr(receipt))
            self.assertNotIn(PRIVATE, repr(receipt))
            self.assertEqual(len(manager.writes), 1)
            # Same source must not silently replace a differing body or a
            # damaged/duplicate provenance pair.
            with self.assertRaises(ContextRejected):
                await adapter.import_review(target, plan.context_key, BODY + "changed")
            saved = deepcopy(manager.history)
            for broken in [
                saved[:-1],
                saved + saved[-2:],
                saved[:-1] + [{"role": "user", "content": BODY}],
            ]:
                manager.history = broken
                with self.assertRaises(ContextRejected):
                    await adapter.import_review(target, plan.context_key, BODY)
            manager.history = saved[:-2]
            with self.assertRaises(ContextUncertain):
                await adapter.import_review(target, plan.context_key, BODY)
            self.assertEqual(
                len(manager.writes),
                1,
                "A missing trimmed/reset marker must not resurrect the import",
            )
            manager.history = saved
        with Store(self.path) as reopened:

            async def forbidden(*args):
                self.fail("Confirmed publication steps must not be called after reopen")

            self.assertTrue(
                (await advance_publication(reopened, target.call_id, forbidden, forbidden)).complete
            )
        # Merely quoting an exact source marker in a user message proves nothing.
        other = binding("quoted")
        _, quoted, writer = self.fake(other, reader=False)
        key = f"review:{other.call_id}:context"
        _, old_manager, old_adapter = self.fake(other)
        await old_adapter.import_review(other, key, BODY)
        quoted.history.append({"role": "user", "content": old_manager.history[-2]["content"]})
        receipt = await writer.import_review(other, key, BODY)
        self.assertFalse(receipt.already_present)
        self.assertEqual(len(quoted.writes), 1)
        self.assertIs(await writer.make_context_readable(other, key, BODY), Outcome.UNCERTAIN)
        self.assertEqual(len(quoted.writes), 1)

        # The same production context adapter receives the exact forced body
        # used by page slicing/hashes, even if the fake model omits all caveats.
        partial = binding("partial")
        _, manager2, adapter2 = self.fake(partial)
        with Store(self.path) as store:
            store.create_call(partial, retained_missing=True)
            store.receive_audio("partial", "gap", b"retained-private-audio", "audio/webm")
            store.claim_next_audio("partial")
            store.finish_audio("partial", "gap", state="uncertain", error_code="result_uncertain")
            store.retain_audio_missing("partial", "gap")
            store.receive_audio("partial", "blank", b"strict-empty", "audio/webm")
            store.claim_next_audio("partial")
            store.finish_audio("partial", "blank", state="failed", error_code="empty_transcription")
            store.receive_audio("partial", "text", b"recognized", "audio/webm")
            store.claim_next_audio("partial")
            store.complete_audio("partial", "text", "only recognized text")
            end_call(store, "partial", prompt="original writing instructions")

            async def no_caveat(*args):
                return BODY

            await run_pending(store, no_caveat)
            plan2 = store.prepare_publication("partial", char_budget=116)
            self.assertEqual(store.get_review("partial").body, BODY)
            self.assertTrue(
                plan2.body.startswith("材料说明：本次仅根据已识别文字整理，有2段未识别片段")
            )
            self.assertEqual("".join(p.fragment for p in plan2.pages), plan2.body)
            self.assertTrue(all(len(p.message) <= 116 for p in plan2.pages))
            import hashlib

            self.assertEqual(plan2.review_sha256, hashlib.sha256(plan2.body.encode()).hexdigest())
            sent2 = []

            async def send2(bound, page):
                sent2.append(page.fragment)
                return Outcome.SUBMITTED

            result2 = await advance_publication(
                store, "partial", send2, adapter2.make_context_readable
            )
            self.assertTrue(result2.complete)
            self.assertEqual("".join(sent2), plan2.body)
            self.assertEqual(manager2.history[-1], {"role": "assistant", "content": plan2.body})
            self.assertEqual(store.get_review("partial").body, BODY)
            frozen_note = tuple(store._audio_note("partial"))
            with self.assertRaises(sqlite3.IntegrityError):
                store._db.execute(
                    "UPDATE review_audio_notes SET notice='changed' WHERE call_id='partial'"
                )
        with Store(self.path) as reopened:
            self.assertEqual(tuple(reopened._audio_note("partial")), frozen_note)
            self.assertEqual(reopened.get_publication("partial"), result2)
            self.assertTrue(
                (await advance_publication(reopened, "partial", forbidden, forbidden)).complete
            )

    async def test_rejected_targets_bad_history_and_wrong_loop_never_write_or_leak(self):
        target = binding()
        for mode, override in [
            ("missing", {}),
            ("normal", {"cid": "B"}),
            ("normal", {"user_id": "wrong"}),
            ("normal", {"platform_id": "qq-2"}),
            ("normal", {"history": "{broken " + PRIVATE}),
            ("normal", {"history": json.dumps([{"role": "user", "content": 42}])}),
        ]:
            with self.subTest(mode=mode, fields=tuple(override)):
                locks, manager, adapter = self.fake(target)
                manager.mode, manager.override = mode, override
                try:
                    await adapter.import_review(target, "review:call:context", BODY)
                except ContextRejected as error:
                    self.assertNotIn(PRIVATE, "".join(traceback.format_exception(error)))
                else:
                    self.fail("Unsafe target/history must be rejected")
                self.assertEqual(manager.writes, [])
        locks, manager, adapter = self.fake(target)
        for bad_target, key in [
            (replace(target, conversation_id=""), "review:call:context"),
            (target, "review:other:context"),
        ]:
            with self.assertRaises(ContextRejected):
                await adapter.import_review(bad_target, key, BODY)
        self.assertEqual(manager.reads, [])
        self.assertEqual(locks.requests, [])

        # Invalid material must not be inspected before a wrong-loop rejection.
        class Poison:
            def __getattribute__(self, name):
                raise AssertionError(PRIVATE)

        async def wrong_loop():
            for method in [adapter.import_review, adapter.make_context_readable]:
                try:
                    await method(Poison(), Poison(), Poison())
                except LoopMismatch as error:
                    self.assertNotIn(PRIVATE, "".join(traceback.format_exception(error)))
                else:
                    raise AssertionError("Wrong loop accepted")

        await asyncio.to_thread(lambda: asyncio.run(wrong_loop()))
        self.assertEqual(manager.reads, [])
        self.assertEqual(locks.requests, [])

    async def test_postwrite_readback_model_loss_and_cancellation_keep_publication_uncertain(self):
        for mode in [
            "readback-fails",
            "readback-missing",
            "write-confirmation-lost",
            "model-missing",
            "cancel-write",
            "cancel-model",
        ]:
            with self.subTest(mode=mode), Store(self.path) as store:
                target = binding(mode)
                plan = await self.prepare(store, target)
                locks, manager, adapter = self.fake(target)
                manager.mode = mode

                async def send(*args):
                    return Outcome.CONFIRMED

                worker = asyncio.create_task(
                    advance_publication(store, target.call_id, send, adapter.make_context_readable)
                )
                if mode.startswith("cancel"):
                    entered = (
                        manager.write_entered if mode == "cancel-write" else manager.model_entered
                    )
                    await asyncio.wait_for(entered.wait(), 3)
                    worker.cancel(PRIVATE)
                    try:
                        await worker
                    except asyncio.CancelledError as error:
                        self.assertNotIn(PRIVATE, "".join(traceback.format_exception(error)))
                    else:
                        self.fail("Cancellation must propagate")
                else:
                    result = await worker
                    self.assertFalse(result.context_available)
                result = store.get_publication(target.call_id)
                self.assertEqual(result.context_state, "uncertain")
                self.assertTrue(result.delivery_confirmed)
                self.assertFalse(result.complete)
                self.assertEqual(len(manager.writes), 1)
                self.assertEqual(store.get_review(target.call_id).body, BODY)
                self.assertEqual(len(store.get_review_input(target.call_id)), 1)
                # Even if a new adapter cannot remember a previous local attempt,
                # the durable publication state must never call it again.
                manager.mode = "normal"
                replacement = AstrBotContextAdapter(
                    SimpleNamespace(conversation_manager=manager),
                    session_lock_manager=locks,
                    model_input_reader=manager.model_input,
                )
                with Store(self.path) as reopened:
                    again = await advance_publication(
                        reopened, target.call_id, send, replacement.make_context_readable
                    )
                    self.assertEqual(again.context_state, "uncertain")
                self.assertEqual(len(manager.writes), 1)
                # A direct call can verify a surviving pair without writing.
                receipt = await adapter.import_review(target, plan.context_key, BODY)
                self.assertTrue(receipt.already_present)
                self.assertEqual(len(manager.writes), 1)
        # Direct API errors also suppress the private manager exception chain.
        target = binding("direct")
        _, manager, adapter = self.fake(target)
        manager.mode = "readback-fails"
        try:
            await adapter.import_review(target, "review:direct:context", BODY)
        except ContextUncertain as error:
            self.assertNotIn(PRIVATE, "".join(traceback.format_exception(error)))
        else:
            self.fail("Readback loss must not produce a storage receipt")

    async def test_import_isolation_and_shared_umo_lock_allow_other_origin_to_proceed(self):
        isolated = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                r"""
import sys
opened = []
def audit(event, args):
    if event in {"sqlite3.connect", "socket.connect", "socket.bind"}:
        raise AssertionError("Unexpected database/network effect")
sys.addaudithook(audit)
import bot_call.astrbot_context
assert not any(name == "astrbot" or name.startswith("astrbot.") for name in sys.modules)
assert "bot_call.store" not in sys.modules
assert "bot_call.publication" not in sys.modules
print("pure import OK")
""",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(isolated.returncode, 0, isolated.stderr)
        self.assertEqual(isolated.stdout.strip(), "pure import OK")
        a, b = binding("wait"), binding("other", "qq-2", "C")
        locks = SharedLocks()
        am, bm = Conversations(locks, a), Conversations(locks, b)
        aa = AstrBotContextAdapter(
            SimpleNamespace(conversation_manager=am), session_lock_manager=locks
        )
        ba = AstrBotContextAdapter(
            SimpleNamespace(conversation_manager=bm), session_lock_manager=locks
        )
        async with locks.acquire_lock(a.unified_msg_origin):
            waiting = asyncio.create_task(aa.import_review(a, "review:wait:context", BODY))
            try:
                await asyncio.sleep(0)
                result = await asyncio.wait_for(
                    ba.import_review(b, "review:other:context", BODY), 3
                )
                self.assertFalse(result.already_present)
                self.assertFalse(waiting.done())
                self.assertEqual(am.reads, [])
            except BaseException:
                waiting.cancel()
                raise
        await asyncio.wait_for(waiting, 3)
        self.assertEqual(len(am.writes), 1)
        self.assertEqual(len(bm.writes), 1)


if __name__ == "__main__":
    unittest.main()
