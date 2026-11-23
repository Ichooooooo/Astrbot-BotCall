"""Three whole scenarios; pure fake counters, synthetic calls, no Provider."""

from dataclasses import asdict, FrozenInstanceError, replace
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import traceback
import unittest

from bot_call.binding import CallBinding
from bot_call.review_plan import build_review_plan, ReviewPlanError
from bot_call.store import Store


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


def payload(chunk):
    request = json.loads(chunk.request_json)
    return request, json.loads(request["messages"][1]["content"])


def canonical(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


class ReviewPlanTests(unittest.TestCase):
    def assert_complete(self, plan, frozen):
        fragments = [f for c in plan.chunks for f in c.fragments]
        expected_order = []
        rebuilt = []
        for original in frozen:
            sequence = original["sequence"]
            parts = [f for f in fragments if f.turn.sequence == sequence]
            self.assertTrue(parts)
            position = 0
            for part in parts:
                expected_order.append(sequence)
                self.assertEqual(part.start, position)
                self.assertEqual(part.stop - part.start, len(part.turn.body))
                self.assertEqual(part.body_length, len(original["body"]))
                self.assertEqual(part.turn.body, original["body"][part.start : part.stop])
                actual_meta = asdict(part.turn)
                del actual_meta["body"]
                expected_meta = dict(original)
                del expected_meta["body"]
                self.assertEqual(actual_meta, expected_meta)
                position = part.stop
            self.assertEqual(position, len(original["body"]))
            rebuilt.append({**original, "body": "".join(f.turn.body for f in parts)})
        self.assertEqual([f.turn.sequence for f in fragments], expected_order)
        self.assertEqual(rebuilt, frozen)
        for ordinal, chunk in enumerate(plan.chunks, 1):
            request, material = payload(chunk)
            self.assertEqual(chunk.ordinal, ordinal)
            self.assertEqual(material["ordinal"], ordinal)
            self.assertEqual(material["source_key"], chunk.source_key)
            self.assertEqual(material["input_sha256"], plan.input_sha256)
            self.assertEqual(material["purpose"], "partial_material_for_later_review_processing")
            self.assertIn("不是整通电话的最终回顾", request["messages"][0]["content"])
            self.assertEqual([m["role"] for m in request["messages"]], ["system", "user"])
            self.assertEqual(
                material["fragments"],
                [
                    {
                        "turn": asdict(f.turn),
                        "body_range": [f.start, f.stop],
                        "body_length": f.body_length,
                    }
                    for f in chunk.fragments
                ],
            )

    def test_frozen_whole_call_coverage_and_no_store_changes(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "calls.db"
            with Store(path) as store:
                store.create_call(binding("whole-call"), background="BACKGROUND_NOT_CALL_MATERIAL")
                store.append_user("whole-call", "opening", " 开头🙂 e\u0301\r\n\t ", source="text")
                store.claim_generation(
                    "whole-call", "suggestion", utterance_id="opening", source="fake_reply"
                )
                store.complete_generation(
                    "whole-call", "suggestion", " Bot建议：或许搬家，不代表用户决定。\n "
                )
                store.append_user(
                    "whole-call", "middle", "中段：尚未决定\n\n  保留空白 ", source="text"
                )
                store.append_user("whole-call", "empty", "", source="text")
                store.append_user("whole-call", "ending", " 结尾：下次再谈🙂\t", source="text")
                prompt = "PRIVATE_PROMPT 原样回顾要求\n  区分建议与决定 \t"
                job = store.end_call("whole-call", prompt=prompt)
                frozen = json.loads(job.input_json)
                self.assertEqual(frozen[1]["playback_state"], "unknown")
                store.observe_playback("whole-call", "suggestion", revision=1, state="ended")
                self.assertEqual(store.get_review_input("whole-call")[1].playback_state, "unknown")
                before = path.read_bytes()
                before_dump = tuple(store._db.iterdump())
                plan = build_review_plan(job, input_token_budget=1600, count_tokens=len)
                self.assertGreater(len(plan.chunks), 1)
                self.assert_complete(plan, frozen)
                for chunk in plan.chunks:
                    self.assertEqual(len(chunk.request_json), chunk.token_count)
                    self.assertLessEqual(chunk.token_count, 1600)
                    self.assertEqual(payload(chunk)[1]["review_prompt"], prompt)
                    self.assertNotIn("BACKGROUND_NOT_CALL_MATERIAL", chunk.request_json)
                self.assertEqual(
                    plan, build_review_plan(job, input_token_budget=1600, count_tokens=len)
                )
                changed_prompt = build_review_plan(
                    replace(job, prompt=prompt + " 新要求"),
                    input_token_budget=1600,
                    count_tokens=len,
                )
                self.assertNotEqual(plan.fingerprint, changed_prompt.fingerprint)
                self.assertNotEqual(plan.chunks[0].source_key, changed_prompt.chunks[0].source_key)
                changed_budget = build_review_plan(job, input_token_budget=1601, count_tokens=len)
                self.assertNotEqual(plan.fingerprint, changed_budget.fingerprint)
                self.assertEqual(store.get_review("whole-call"), job)
                self.assertEqual(path.read_bytes(), before)
                self.assertEqual(tuple(store._db.iterdump()), before_dump)
                with self.assertRaises(FrozenInstanceError):
                    plan.input_token_budget = 1
                for value in [plan, *plan.chunks, *(f for c in plan.chunks for f in c.fragments)]:
                    self.assertNotIn("PRIVATE_PROMPT", repr(value))
                    self.assertNotIn("Bot建议", repr(value))

    def test_long_turn_exact_envelope_overhead_and_nonmonotonic_counter(self):
        with TemporaryDirectory() as directory, Store(Path(directory) / "calls.db") as store:
            store.create_call(binding("long"))
            body = (' \n🙂 e\u0301\\"\t\r\n超长发言 ' * 150) + "尾声"
            store.append_user("long", "long-u", body, source="text")
            job = store.end_call("long", prompt='完整要求\\"\n 不许截短 \t' * 4)
            observed = []

            def full_surface(request_json):
                observed.append(request_json)
                messages = json.loads(request_json)["messages"]
                material = json.loads(messages[1]["content"])
                self.assertEqual(material["review_prompt"], job.prompt)
                # Fake count includes role/message/payload escaping, not body alone.
                self.assertGreater(len(request_json), len(material["fragments"][0]["turn"]["body"]))
                return len(request_json)

            plan = build_review_plan(job, input_token_budget=2350, count_tokens=full_surface)
            self.assertGreater(len(plan.chunks), 2)
            self.assert_complete(plan, json.loads(job.input_json))
            for chunk in plan.chunks:
                self.assertIn(chunk.request_json, observed)
                self.assertLessEqual(len(chunk.request_json), 2350)
                self.assertEqual(chunk.token_count, len(chunk.request_json))
            # A one-character prefix is *more* expensive than five characters.
            # Full ten chars fail; the conservative halving search still finds 5.
            store.create_call(binding("nonmonotonic"))
            store.append_user("nonmonotonic", "u", "abcdefghij", source="text")
            other = store.end_call("nonmonotonic", prompt="keep this whole instruction")

            def nonmonotonic(request_json):
                material = json.loads(json.loads(request_json)["messages"][1]["content"])
                total = sum(len(f["turn"]["body"]) for f in material["fragments"])
                return 4 if total == 5 else 9

            other_plan = build_review_plan(other, input_token_budget=4, count_tokens=nonmonotonic)
            self.assertEqual(
                [c.fragments[0].turn.body for c in other_plan.chunks], ["abcde", "fghij"]
            )
            self.assert_complete(other_plan, json.loads(other.input_json))
            self.assertTrue(all(c.token_count == 4 for c in other_plan.chunks))

    def test_fixed_private_failures_empty_input_and_import_isolation(self):
        private_body = "SYNTHETIC_PRIVATE_BODY_十阶段"
        private_prompt = "SYNTHETIC_PRIVATE_PROMPT_十阶段"
        with TemporaryDirectory() as directory, Store(Path(directory) / "calls.db") as store:
            store.create_call(binding("errors"))
            store.append_user("errors", "u", private_body, source="text")
            job = store.end_call("errors", prompt=private_prompt)

            def check(bad=job, budget=5000, counter=len, code=None):
                try:
                    build_review_plan(bad, input_token_budget=budget, count_tokens=counter)
                except ReviewPlanError as error:
                    self.assertEqual(str(error), code)
                    output = repr(error) + "".join(traceback.format_exception(error))
                    self.assertNotIn(private_body, output)
                    self.assertNotIn(private_prompt, output)
                    self.assertTrue(error.__suppress_context__)
                else:
                    self.fail("invalid input returned a plan")

            check(replace(job, input_sha256="0" * 64), code="review_plan.invalid_input")
            cases = [
                None,
                {},
                [{}],
                [{**json.loads(job.input_json)[0], "sequence": True}],
                [{**json.loads(job.input_json)[0], "created_at": float("nan")}],
                [{**json.loads(job.input_json)[0], "call_id": "another-call"}],
                [{**json.loads(job.input_json)[0], "playback_state": "unknown"}],
                [{**json.loads(job.input_json)[0], "body": 4}],
                [{**json.loads(job.input_json)[0], "extra": private_body}],
            ]
            for value in cases:
                text = json.dumps(value, ensure_ascii=False, sort_keys=True)
                check(
                    replace(
                        job, input_json=text, input_sha256=hashlib.sha256(text.encode()).hexdigest()
                    ),
                    code="review_plan.invalid_input",
                )
            duplicate = job.input_json.replace('"body":', '"body":"duplicate","body":', 1)
            check(
                replace(
                    job,
                    input_json=duplicate,
                    input_sha256=hashlib.sha256(duplicate.encode()).hexdigest(),
                ),
                code="review_plan.invalid_input",
            )
            check(replace(job, prompt="\ud800"), code="review_plan.invalid_input")
            for budget in [True, 0, -1, 1.5, None]:
                check(budget=budget, code="review_plan.invalid_budget")
            for result in [True, -1, 1.5, private_body, None]:
                check(
                    counter=lambda request, result=result: result, code="review_plan.invalid_count"
                )

            def broken(request):
                raise RuntimeError(private_body + private_prompt)

            check(counter=broken, code="review_plan.counter_failed")
            check(counter=None, code="review_plan.invalid_counter")
            check(budget=1, code="review_plan.budget_too_small")
            # Entire prompt is essential even if the body could fit by itself.
            check(
                replace(job, prompt=private_prompt * 1000),
                budget=5000,
                code="review_plan.budget_too_small",
            )
            calls = 0

            def changing(request):
                nonlocal calls
                calls += 1
                return calls

            check(counter=changing, code="review_plan.counter_changed")
            store.create_call(binding("empty"))
            empty = store.end_call("empty", prompt=private_prompt)

            def must_not_count(request):
                self.fail("empty input should not build a request")

            self.assertEqual(empty.state, "closed_no_text")
            from bot_call.store import NotFound

            with self.assertRaises(NotFound):
                store.get_review("empty")
            self.assertEqual(store.get_transcript("empty"), ())
        # A fresh interpreter imports only source; deny observable IO starts.
        script = """
import asyncio, os, sqlite3, socket, threading, sys
from pathlib import Path
def forbidden(*args, **kwargs):
    raise AssertionError('unexpected import side effect')
sqlite3.connect = forbidden
socket.socket = forbidden
threading.Thread.start = forbidden
os.mkdir = forbidden
import bot_call.review_plan
assert not any(n == 'astrbot' or n.startswith('astrbot.') for n in sys.modules)
"""
        completed = subprocess.run(
            [sys.executable, "-B", "-c", script], capture_output=True, text=True, check=False
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
