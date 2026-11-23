"""Fixed categories and real native adapter capture, with synthetic failures."""

import asyncio
import json
from types import SimpleNamespace as NS
import unittest
from bot_call.failures import (
    classify,
    record_failure,
    failure_summary,
    log_failure,
    CallConfigurationError,
)
from bot_call.observability import CURRENT_TIMING, RoundTiming
from test_backend import NativeCase, response
from test_call_history import sample


class StatusError(Exception):
    def __init__(self, status):
        super().__init__("SYNTHETIC_PRIVATE_ERROR")
        self.status_code = status


class FailureTests(unittest.TestCase):
    def test_redacted_failure_log_native_attempts_config_and_malicious_provider(self):
        timing = RoundTiming()
        timing.provider_id = "deepseek/phone"
        timing.add("llm", 0)
        timing.add("llm", 0)
        token = CURRENT_TIMING.set(timing)
        try:
            with self.assertLogs("bot_call.failures", level="WARNING") as logs:
                record_failure(
                    "chat",
                    "All chat models failed: ConnectError: PRIVATE https://private?key=sk-private",
                )
                record_failure("chat", ValueError("PRIVATE"))
            self.assertEqual(len(logs.output), 1)
            self.assertIn(
                "category=unavailable error_type=native provider=deepseek/phone attempts=2",
                logs.output[0],
            )
            self.assertNotIn("PRIVATE", logs.output[0])
            self.assertNotIn("https", logs.output[0])
        finally:
            CURRENT_TIMING.reset(token)
        with self.assertLogs("bot_call.failures", level="WARNING") as logs:
            log_failure(
                "summary",
                TimeoutError("PRIVATE"),
                provider="https://private/key",
                attempts=1,
                elapsed_ms=12,
            )
        self.assertIn("provider=unknown", logs.output[0])
        self.assertNotIn("PRIVATE", logs.output[0])
        self.assertEqual(classify(CallConfigurationError("PRIVATE")), "config")

    def test_every_category_nested_sdk_shapes_and_only_whitelisted_values(self):
        for error, code in [
            (StatusError(402), "balance"),
            (RuntimeError("insufficient_balance SYNTHETIC_PRIVATE_ERROR"), "balance"),
            (StatusError(401), "auth"),
            (StatusError(403), "auth"),
            (TimeoutError("private"), "timeout"),
            (StatusError(504), "timeout"),
            (StatusError(503), "unavailable"),
            (ConnectionError("private"), "unavailable"),
            (ValueError("private"), "other"),
            ("Error code: 401 - SYNTHETIC_PRIVATE_ERROR", "auth"),
            ("All chat models failed: ConnectError: private", "unavailable"),
        ]:
            self.assertEqual(classify(error), code)
            for service in ["stt", "chat", "tts", "summary"]:
                timing = RoundTiming()
                token = CURRENT_TIMING.set(timing)
                try:
                    record_failure(service, error)
                finally:
                    CURRENT_TIMING.reset(token)
                serialized = json.dumps(timing.rows)
                self.assertNotIn("PRIVATE", serialized)
                self.assertIn("error_" + service + "_" + code, serialized)
        wrapped = RuntimeError("private")
        wrapped.__cause__ = StatusError(402)
        self.assertEqual(classify(wrapped), "balance")
        sdk = RuntimeError("private")
        sdk.response = NS(status_code=403)
        self.assertEqual(classify(sdk), "auth")
        self.assertEqual(
            failure_summary([{"stage": "error_stt_balance", "generation_id": "u"}] * 2),
            [dict(service="stt", code="balance", count=2)],
        )


class FailureNativeTests(NativeCase):
    async def test_close_between_native_preflights_is_cancelled_without_failure_or_warning(self):
        from bot_call.store import Store

        event, provider = self.environment()
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        adapter = backend._record(handle).reply
        original = adapter.state
        entered, release = asyncio.Event(), asyncio.Event()
        self.gates.append(release)
        reads = []

        async def gated_state(bound, turn):
            reads.append(True)
            if len(reads) == 3:
                entered.set()
                await release.wait()
            return await original(bound, turn)

        adapter.state = gated_state
        with self.assertNoLogs("bot_call.failures", level="WARNING"):
            await backend.web_submit(web.token, web.reference, "closing", "retained tail")
            await asyncio.wait_for(entered.wait(), 3)
            self.assertEqual(provider.calls, [])
            await backend.web_end(web.token, web.reference)
            closing = await backend.read_state(handle)
            self.assertIsNotNone(closing.close_request)
            self.assertIsNone(closing.review)
            release.set()
            await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual(state.generations[0].state, "cancelled")
        self.assertEqual([(t.role, t.body) for t in state.turns], [("user", "retained tail")])
        self.assertIsNotNone(state.review)
        # Subsequent model calls are the review, never a live reply to the tail.
        self.assertFalse(any(p.get("prompt") == "retained tail" for p in provider.calls))
        public = await backend._runtime.diagnostic_timings()
        self.assertEqual(public["failures"], [])
        with Store(self.path / "calls.db") as store:
            rows = list(
                store._db.execute(
                    "SELECT stage FROM turn_timings WHERE generation_id='reply:closing'"
                )
            )
            stages = {r[0] for r in rows}
            self.assertIn("cancelled_closed", stages)
            self.assertNotIn("llm", stages)
            self.assertFalse(any(s.startswith("error_chat_") for s in stages))
            run = store.agent_record(state.binding.call_id, "reply:closing")
            self.assertEqual(run["state"], "unknown")
            self.assertEqual(json.loads(run["reports_json"]), [])

    async def test_control_audio_completed_after_hangup_retains_text_without_reply_callback(self):
        event, provider = self.environment()
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        entered, release = asyncio.Event(), asyncio.Event()
        self.gates.append(release)

        async def transcribe(path):
            entered.set()
            await release.wait()
            return "synthetic closing audio"

        self.context.stt[event.unified_msg_origin].transcriber = transcribe
        adapter = backend._record(handle).reply
        original = adapter.reply
        replies = []

        async def observed_reply(*args):
            replies.append(True)
            return await original(*args)

        adapter.reply = observed_reply
        with self.assertNoLogs("bot_call.failures", level="WARNING"):
            await backend.web_audio(web.token, web.reference, "tail", b"synthetic", "audio/webm")
            await asyncio.wait_for(entered.wait(), 3)
            await backend.web_end(web.token, web.reference)
            release.set()
            await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual(replies, [])
        self.assertEqual(
            [(t.role, t.body) for t in state.turns], [("user", "synthetic closing audio")]
        )
        self.assertEqual(state.generations[0].state, "cancelled")
        self.assertTrue(
            any(
                "synthetic closing audio" in json.dumps(p.get("contexts"), ensure_ascii=False)
                for p in provider.calls
            )
        )
        self.assertEqual((await backend._runtime.diagnostic_timings())["failures"], [])

    async def test_unclosed_material_value_error_still_records_a_real_failure(self):
        event, _ = self.environment()
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)

        async def unavailable(*args):
            raise ValueError("SYNTHETIC_PRIVATE_ERROR")

        backend._record(handle).reply.state = unavailable
        with self.assertLogs("bot_call.failures", level="WARNING") as logs:
            await backend.web_submit(web.token, web.reference, "real-error", "synthetic")
            await backend.drain()
        self.assertEqual(len(logs.output), 1)
        self.assertIn("category=other error_type=ValueError", logs.output[0])
        self.assertNotIn("PRIVATE", logs.output[0])
        public = await backend.web_state(web.token, web.reference)
        self.assertIn(
            dict(service="chat", code="other", count=1, generation_id="reply:real-error"),
            public.failures,
        )

    async def test_stt_chat_tts_and_background_summary_persist_categories_without_retry(self):
        event, p = self.environment()
        backend, _ = await self.backend()
        h, w = await self.enter(backend, event)

        async def no_balance(path):
            raise StatusError(402)

        self.context.stt[event.unified_msg_origin].transcriber = no_balance
        await backend.web_audio(w.token, w.reference, "stt", b"synthetic", "audio/webm")
        await backend.drain()
        s = await backend.web_state(w.token, w.reference)
        self.assertIn(
            dict(service="stt", code="balance", count=1, generation_id="reply:stt"), s.failures
        )
        self.assertEqual(len(self.context.stt[event.unified_msg_origin].paths), 1)

        async def auth(payload):
            return response("Error code: 401 - SYNTHETIC_PRIVATE_ERROR", role="err")

        p.responder = auth
        with self.assertLogs("bot_call.failures", level="WARNING") as logs:
            await backend.web_submit(w.token, w.reference, "chat", "synthetic")
            await backend.drain()
        self.assertEqual(len(logs.output), 1)
        self.assertIn("bot_call.chat stage=failed category=auth error_type=native", logs.output[0])
        self.assertIn("attempts=1", logs.output[0])
        self.assertNotIn("PRIVATE", logs.output[0])
        s = await backend.web_state(w.token, w.reference)
        self.assertIn(
            dict(service="chat", code="auth", count=1, generation_id="reply:chat"), s.failures
        )
        p.responder = None
        await backend.web_submit(w.token, w.reference, "tts", "synthetic")
        await backend.drain()

        async def unavailable(text):
            raise StatusError(503)

        self.context.tts[event.unified_msg_origin].synthesizer = unavailable
        with self.assertLogs("bot_call.failures", level="WARNING") as logs:
            await backend.web_speech(w.token, w.reference, "reply:tts")
            await backend.drain()
        self.assertIn("bot_call.tts stage=failed category=unavailable", logs.output[0])
        self.assertNotIn("PRIVATE", logs.output[0])
        s = await backend.web_state(w.token, w.reference)
        self.assertIn(
            dict(service="tts", code="unavailable", count=1, generation_id="reply:tts"), s.failures
        )
        self.assertEqual(len(self.context.tts[event.unified_msg_origin].texts), 1)
        # Background summary has no round ContextVar; its owner records only a code.
        event, p = self.environment("qq-B", "B")
        self.context.conversation_manager.items["B"].history = json.dumps(sample())
        original = self.settings
        self.settings = lambda: dict(original(), call_history_turns=1, call_history_summary=True)
        count = []

        async def timeout(**kwargs):
            count.append(1)
            raise TimeoutError("SYNTHETIC_PRIVATE_ERROR")

        self.context.llm_generate = timeout
        backend, _ = await self.backend(name="summary.db")
        h, w = await self.enter(backend, event)
        await backend.web_submit(w.token, w.reference, "summary", "synthetic")
        await backend.drain()
        await backend._record(h).reply.history.task
        s = await backend.web_state(w.token, w.reference)
        self.assertEqual(s.replies[0].state, "succeeded")
        self.assertIn(
            dict(service="summary", code="timeout", count=1, generation_id="reply:summary"),
            s.failures,
        )
        self.assertEqual(count, [1])
        self.assertNotIn("SYNTHETIC_PRIVATE_ERROR", json.dumps(s.to_dict(), ensure_ascii=False))
        diag = await backend._runtime.diagnostic_timings()
        self.assertIn(dict(service="summary", code="timeout", count=1), diag["failures"])
