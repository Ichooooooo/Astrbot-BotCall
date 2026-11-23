"""Idle and administrator closure with synthetic time and owned callbacks."""

import asyncio
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch

from bot_call.access import AccessError, InvitationSecret, session_reference
from bot_call.limits import DEFAULT_IDLE_SECONDS, LimitsError, RuntimeLimits
from bot_call.plugin import BENCHMARK_ROUTE, PLUGIN_NAME, PluginLifecycle
from bot_call.publication import Outcome
from bot_call.runtime import CallRuntime, RuntimeAudio, RuntimeCallbacks
from bot_call.store import Store
from test_astrbot_context import binding
from test_backend import Estimator, NativeCase, WAV, response
from test_limits import Wakeups
from test_plugin_lifecycle import Server


class IdleTests(unittest.IsolatedAsyncioTestCase):
    def test_limits_validation(self):
        self.assertIsNone(RuntimeLimits().idle_seconds)
        for value in (1, 3600):
            self.assertEqual(RuntimeLimits(idle_seconds=value).idle_seconds, value)
        for value in (True, 0, -1, 1.0, "600", 3601):
            with self.subTest(value=value), self.assertRaises(LimitsError):
                RuntimeLimits(idle_seconds=value)

    async def asyncSetUp(self):
        directory = self.enterContext(TemporaryDirectory())
        self.path = Path(directory) / "calls.db"
        self.now = 1000
        self.enterContext(patch("bot_call.store.time", lambda: self.now))
        self.enterContext(patch("bot_call.segments.time", lambda: self.now))
        self.timers = Wakeups()
        self.gates = []
        self.reviews = []

    async def asyncTearDown(self):
        for release in self.gates:
            release.set()
        if hasattr(self, "runtime"):
            await self.runtime.shutdown(timeout=None)

    def gate(self):
        entered, release = asyncio.Event(), asyncio.Event()
        self.gates.append(release)
        return entered, release

    async def start(self, *, reply=None, audio=None, synthesize=None):
        async def default_reply(*_):
            return "示例回复"

        async def review(bound, turns, prompt):
            self.reviews.append((bound.call_id, [turn.body for turn in turns]))
            return "示例回顾"

        async def publish(*_):
            return Outcome.CONFIRMED

        self.runtime = CallRuntime(
            self.path,
            RuntimeCallbacks(reply or default_reply, review, publish, publish, synthesize),
            audio=RuntimeAudio(audio) if audio else None,
            limits=RuntimeLimits(idle_seconds=10),
            _limit_timer=self.timers,
        )
        await self.runtime.start()

    async def enter(self, call_id):
        await self.runtime.create_call(binding(call_id))
        secret = InvitationSecret.create()
        await self.runtime.issue_web_invitation(call_id, secret, session_ttl=10000)
        grant = await self.runtime.exchange_web_invitation(secret.value)
        return grant.token, grant.reference

    async def heartbeat(self, auth, now):
        self.now = now
        return await self.runtime.web_heartbeat(*auth)

    def check_delay(self, seconds):
        self.assertEqual([handle.delay for handle in self.timers.active()], [seconds])

    async def test_heartbeat_does_not_extend_idle_and_close_preserves_review(self):
        await self.start()
        silent = await self.enter("silent")
        self.check_delay(10)
        for now in (1002, 1005, 1009):
            await self.heartbeat(silent, now)
            self.check_delay(1010 - now)
        with self.assertRaises(AccessError) as error:
            await self.heartbeat(silent, 1010)
        self.assertEqual(error.exception.code, "ended")
        state = await self.runtime.web_state(*silent)
        self.assertEqual((state.close_state, state.close_detail), ("closed_no_text", "idle"))
        self.assertEqual(state.limits.close_reason, "duration")
        self.assertEqual(self.reviews, [])
        speaking = await self.enter("speaking")
        self.now = 1015
        await self.runtime.web_submit(*speaking, "opening", "保留这句话")
        await self.runtime.drain()
        self.check_delay(10)
        await self.heartbeat(speaking, 1024)
        self.now = 1025
        self.timers.fire()
        await self.runtime.drain()
        state = await self.runtime.web_state(*speaking)
        self.assertTrue(state.ended)
        self.assertEqual(
            (state.limits.close_reason, state.limits.close_detail), ("duration", "idle")
        )
        self.assertEqual(self.reviews, [("speaking", ["保留这句话", "示例回复"])])

    async def test_chunks_transcription_reply_synthesis_and_playback_protect_line(self):
        stt_entered, stt_release = self.gate()
        reply_entered, reply_release = self.gate()
        tts_entered, tts_release = self.gate()
        bound = binding("speaking")
        gid = "reply:voice"

        async def audio(*_):
            stt_entered.set()
            await stt_release.wait()
            return "示例口述"

        async def reply(*_):
            await self.runtime.agent_journal("segment", bound.call_id, gid, "第一句")
            await self.runtime.agent_journal("stream_done", bound.call_id, gid)
            reply_entered.set()
            await reply_release.wait()
            return "第一句"

        async def synthesize(*_):
            tts_entered.set()
            await tts_release.wait()
            return WAV

        await self.start(reply=reply, audio=audio, synthesize=synthesize)
        auth = await self.enter(bound.call_id)
        await self.runtime.web_audio_chunk(*auth, "voice", 0, 0, b"audio", "audio/webm")
        await self.heartbeat(auth, 1012)
        self.assertIsNone(
            (await self.runtime.benchmark_snapshot())["active_calls"][0]["idle_seconds"]
        )
        await self.runtime.web_audio_commit(
            *auth, "voice", 5, hashlib.sha256(b"audio").hexdigest(), "audio/webm"
        )
        await asyncio.wait_for(stt_entered.wait(), 2)
        await self.heartbeat(auth, 1024)
        stt_release.set()
        await asyncio.wait_for(reply_entered.wait(), 2)
        await asyncio.wait_for(tts_entered.wait(), 2)
        await self.heartbeat(auth, 1036)
        reply_release.set()
        await self.heartbeat(auth, 1048)
        tts_release.set()
        await self.runtime.drain()
        self.check_delay(10)
        self.now = 1054
        await self.runtime.web_segment(*auth, gid, 1, {"state": "started", "revision": 1})
        await self.heartbeat(auth, 1066)
        await self.runtime.web_segment(*auth, gid, 1, {"state": "ended", "revision": 2})
        self.check_delay(10)
        await self.heartbeat(auth, 1075)
        await self.runtime.web_segment(*auth, gid, 1, {"state": "ended", "revision": 2})
        self.check_delay(1)
        with self.assertRaises(AccessError) as error:
            await self.heartbeat(auth, 1076)
        self.assertEqual(error.exception.code, "ended")

    async def test_reload_uses_saved_turn_time(self):
        await self.start()
        auth = await self.enter("recover")
        self.now = 1006
        await self.runtime.web_submit(*auth, "one", "已有文字")
        await self.runtime.drain()
        await self.runtime.shutdown(timeout=None)
        self.now = 1010
        await self.start()
        self.check_delay(6)
        await self.heartbeat(auth, 1015)
        self.check_delay(1)


class ManagementTests(NativeCase):
    async def initialize_lifecycle(self):
        lifecycle = PluginLifecycle(
            self.settings(),
            lambda: str(self.path),
            context=self.context,
            tools_factory=lambda _: (self.tools(), Estimator()),
            server_factory=Server,
            version="test",
        )
        self.addAsyncCleanup(lifecycle.terminate)
        self.enterContext(patch.object(PluginLifecycle, "_register_management"))
        self.enterContext(
            patch("bot_call.native_host.NativeCallbacks.send_text", return_value=Outcome.SUBMITTED)
        )
        await lifecycle.initialize()
        self.lifecycle = lifecycle
        return lifecycle._backend

    async def request(self, method="GET", body=None, *, authorized=True):
        from astrbot.api.web import PluginRequest, bind_request_context
        from starlette.requests import Request

        class Dashboard:
            def _extract_dashboard_jwt(self, request):
                return request.headers.get("authorization", "").removeprefix("Bearer ")

            def _validate_dashboard_token(self, value, path):
                return ({"username": "dashboard"} if value == "test-admin" else None), ""

        scope = dict(
            type="http",
            method=method,
            path="/api/v1/plugins/extensions" + BENCHMARK_ROUTE,
            query_string=b"",
            headers=[(b"authorization", b"Bearer test-admin")] if authorized else [],
            app=NS(state=NS(dashboard_app_adapter=NS(_dashboard_server=Dashboard()))),
        )

        async def receive():
            return dict(type="http.request", body=json.dumps(body).encode(), more_body=False)

        request = PluginRequest(
            Request(scope, receive), plugin_name=PLUGIN_NAME, username="dashboard"
        )
        with bind_request_context(request):
            reply = await self.lifecycle._benchmark_api()
        return reply.status_code, json.loads(reply.body)

    async def test_dashboard_tool_close_releases_line_and_freezes_review(self):
        self.now = 1000
        self.enterContext(patch("bot_call.store.time", lambda: self.now))
        self.enterContext(patch("bot_call.segments.time", lambda: self.now))
        event, provider = self.environment()
        other, _ = self.environment("platform-other", "other")
        backend = await self.initialize_lifecycle()
        self.assertEqual(backend._config.limits.idle_seconds, DEFAULT_IDLE_SECONDS)
        self.assertEqual(DEFAULT_IDLE_SECONDS, 600)
        self.assertEqual((await self.request())[1]["data"]["active_calls"], [])
        handle, web = await self.enter(backend, event)
        auth = web.token, web.reference
        await backend.web_submit(*auth, "first", "synthetic-private-transcript")
        await backend.drain()
        bound = (await backend.read_state(handle)).binding
        ref = session_reference(hashlib.sha256(bound.call_id.encode()).hexdigest())
        body = dict(action="force_hangup", ref=ref)
        self.assertEqual((await self.request("POST", body, authorized=False))[0], 401)
        for invalid in (dict(body, ref="bad"), dict(body, ref="f" * 64), dict(body, extra=True)):
            self.assertEqual((await self.request("POST", invalid))[1]["data"]["code"], "not_found")

        entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.gates.append(release)

        async def operation(event):
            entered.set()
            await release.wait()
            finished.set()
            return "已完成"

        tool = self.agent_env["FunctionTool"](
            name="example_tool",
            description="synthetic",
            parameters={"type": "object", "properties": {}},
            handler=operation,
        )
        self.context.get_llm_tool_manager = lambda: NS(
            get_full_tool_set=lambda: self.agent_env["ToolSet"]([tool]),
            get_func=lambda name: tool if name == tool.name else None,
            get_builtin_tool=lambda *_: None,
        )

        async def answer(_):
            return response("", role="tool", names=[tool.name], args=[{}], ids=["example"])

        provider.responder = answer
        await backend.web_submit(*auth, "tool", "synthetic-tool-input")
        await asyncio.wait_for(entered.wait(), 2)
        for self.now in (1100, 1200, 1300, 1400, 1500, 1601):
            await backend.web_heartbeat(*auth)
        code, payload = await self.request()
        self.assertEqual(code, 200)
        (active,) = payload["data"]["active_calls"]
        self.assertEqual(
            set(active),
            {
                "ref",
                "state",
                "created_at",
                "entered_at",
                "elapsed_seconds",
                "idle_seconds",
                "remaining_seconds",
                "idle_remaining_seconds",
                "turn_count",
                "tool_processing",
                "can_force_hangup",
            },
        )
        self.assertEqual((active["ref"], active["state"], active["turn_count"]), (ref, "open", 1))
        self.assertTrue(active["tool_processing"])
        self.assertEqual(active["elapsed_seconds"], 601)
        self.assertIsNone(active["idle_seconds"])
        self.assertIsNone(active["idle_remaining_seconds"])
        rendered = json.dumps(payload)
        for private in (
            bound.call_id,
            bound.unified_msg_origin,
            "synthetic-private-transcript",
            "synthetic-tool-input",
            web.token,
        ):
            self.assertNotIn(private, rendered)

        waiting = asyncio.create_task(backend.web_state(*auth, 5000, "tool"))
        async with backend._runtime.watch(*auth) as updates:
            await updates.get()
            code, result = await self.request("POST", body)
            changed = await asyncio.wait_for(updates.get(), 2)
            self.assertTrue(changed["closed"])
            self.assertEqual(changed["close_detail"], "admin_forced")
        self.assertEqual((code, result["status"]), (200, "ok"))
        self.assertTrue(result["data"]["review_expected"])
        closed = await asyncio.wait_for(waiting, 2)
        self.assertTrue(closed.ended)
        self.assertEqual(
            (closed.close_detail, closed.limits.close_reason), ("admin_forced", "manual")
        )
        (active,) = (await self.request())[1]["data"]["active_calls"]
        self.assertEqual((active["state"], active["can_force_hangup"]), ("closing", False))
        self.assertEqual((await self.request("POST", body))[1]["data"]["code"], "already_closing")
        self.assertFalse(finished.is_set())
        await self.enter(backend, other)
        provider.responder = None
        release.set()
        await backend.drain()
        self.assertTrue(finished.is_set())
        state = await backend.read_state(handle)
        self.assertEqual(state.review.state, "succeeded")
        self.assertIn("synthetic-private-transcript", state.review.input_json)

    async def test_management_rejects_duplicate_in_flight_submission(self):
        backend = await self.initialize_lifecycle()
        entered, release = asyncio.Event(), asyncio.Event()

        async def force_end(ref):
            entered.set()
            await release.wait()
            return dict(code="not_found")

        body = dict(action="force_hangup", ref="0" * 64)
        with patch.object(backend, "force_end_call", force_end):
            first = asyncio.create_task(self.request("POST", body))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                duplicate = (await self.request("POST", body))[1]
                self.assertEqual(duplicate["data"]["code"], "running")
            finally:
                release.set()
                await first

    async def test_admin_can_release_persisted_slot_after_reload(self):
        event, _ = self.environment()
        stale = binding("previous-call")
        with Store(self.path / "calls.sqlite3") as store:
            store.create_call(stale)
            secret = InvitationSecret.create()
            store.issue_web_invitation(stale.call_id, secret, session_ttl=3600)
            store.exchange_web_invitation(secret.value, _limits=RuntimeLimits())
        backend = await self.initialize_lifecycle()
        handle = backend.begin(event)
        grant = await backend.invitation(handle)
        with self.assertRaises(AccessError) as error:
            await backend.exchange_web_invitation(grant.secret.value)
        self.assertEqual(error.exception.code, "busy")
        (active,) = (await self.request())[1]["data"]["active_calls"]
        self.assertEqual(active["state"], "open")
        result = (await self.request("POST", dict(action="force_hangup", ref=active["ref"])))[1]
        self.assertEqual(result["status"], "ok")
        await backend.exchange_web_invitation(grant.secret.value)

    async def test_dashboard_force_end_invitation(self):
        event, _ = self.environment()
        backend = await self.initialize_lifecycle()
        handle = backend.begin(event)
        grant = await backend.invitation(handle)
        (active,) = (await self.request())[1]["data"]["active_calls"]
        self.assertEqual((active["state"], active["entered_at"]), ("invited", None))
        result = (await self.request("POST", dict(action="force_hangup", ref=active["ref"])))[1]
        self.assertEqual(result["status"], "ok")
        self.assertFalse(result["data"]["review_expected"])
        with self.assertRaises(AccessError) as error:
            await backend.exchange_web_invitation(grant.secret.value)
        self.assertEqual(error.exception.code, "ended")
        self.assertEqual((await self.request())[1]["data"]["active_calls"], [])
        await self.enter(backend, event)
