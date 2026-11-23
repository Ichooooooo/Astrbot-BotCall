"""Three offline HTTPS-ASGI scenarios, temporary SQLite, no listener/framework."""

import asyncio
from contextlib import closing
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import threading
import traceback
import unittest
from unittest.mock import AsyncMock, patch

from bot_call.access import AccessError, InvitationSecret, token_digest
from bot_call.publication import Outcome
from bot_call.review import DEFAULT_REVIEW_PROMPT
from bot_call.review_work import prepare_review_work
from bot_call.runtime import CallRuntime, RuntimeCallbacks, RuntimeOperationError
from bot_call.store import Store
import bot_call.store as storage
from bot_call.web import CallWebApp, COOKIE, PATH, REFERENCE_HEADER
from test_astrbot_context import binding

ORIGIN = "https://calls.example.test"
PRIVATE = "SYNTHETIC_PRIVATE_WEB_MATERIAL"
FINAL = " 原样整通回顾🙂 \r\n " + PRIVATE


class ObservedStore(Store):
    instances = []
    after_append = before_append = None
    lose_issue = lose_exchange = False

    def __init__(self, path):
        self.owner = threading.get_ident()
        self.threads = set()
        self.closed_thread = None
        super().__init__(path)
        self.instances.append(self)
        self._db.set_trace_callback(lambda sql: self.threads.add(threading.get_ident()))

    def append_user(self, *args, **kwargs):
        if kwargs.get("_web_authority") and type(self).before_append:
            type(self).before_append()
        result = super().append_user(*args, **kwargs)
        if kwargs.get("_web_authority") and type(self).after_append:
            type(self).after_append()
        return result

    def issue_web_invitation(self, *args, **kwargs):
        result = super().issue_web_invitation(*args, **kwargs)
        if type(self).lose_issue:
            type(self).lose_issue = False
            raise RuntimeError(PRIVATE)
        return result

    def exchange_web_invitation(self, *args, **kwargs):
        result = super().exchange_web_invitation(*args, **kwargs)
        if type(self).lose_exchange:
            type(self).lose_exchange = False
            raise RuntimeError(PRIVATE)
        return result

    def close(self):
        super().close()
        self.closed_thread = threading.get_ident()


class WebAccessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "calls.db"
        self.loop = asyncio.get_running_loop()
        self.business_thread = threading.get_ident()
        self.now = 1000.0
        self.runtimes, self.gates = [], []
        self.replies, self.reviews, self.pages, self.contexts = [], [], [], []
        self.reply_ids, self.reply_failures = [], set()
        self.violations, self.unhandled = [], []
        self.review_gate = None
        self.review_entered = asyncio.Event()
        self.old_handler = self.loop.get_exception_handler()
        self.loop.set_exception_handler(lambda loop, context: self.unhandled.append(context))
        ObservedStore.instances = []
        ObservedStore.before_append = ObservedStore.after_append = None
        ObservedStore.lose_issue = ObservedStore.lose_exchange = False
        for patcher in [
            patch("bot_call.runtime.Store", ObservedStore),
            patch("bot_call.store.time", lambda: self.now),
        ]:
            patcher.start()
            self.addCleanup(patcher.stop)

    async def asyncTearDown(self):
        for event in self.gates:
            event.set()
        for runtime in reversed(self.runtimes):
            self.assertTrue((await runtime.shutdown(timeout=3)).stopped)
            self.assertFalse(runtime._thread.is_alive())
            self.assertFalse(runtime._core_tasks)
        for store in ObservedStore.instances:
            self.assertEqual(store.threads, {store.owner})
            self.assertNotEqual(store.owner, self.business_thread)
            self.assertEqual(store.closed_thread, store.owner)
        self.loop.set_exception_handler(self.old_handler)
        self.assertEqual(self.violations, [])
        self.assertEqual(self.unhandled, [])

    def on_business(self):
        if (
            asyncio.get_running_loop() is not self.loop
            or threading.get_ident() != self.business_thread
        ):
            self.violations.append("callback ownership")

    async def runtime(self, path=None):
        async def reply(bound, turn):
            self.on_business()
            self.replies.append((bound, turn.body))
            self.reply_ids.append(turn.material_id)
            if turn.material_id in self.reply_failures:
                raise RuntimeError(PRIVATE)
            return "Bot建议，尚未确认播放"

        async def review(bound, turns, prompt):
            self.on_business()
            self.reviews.append((bound, turns, prompt))
            self.review_entered.set()
            if self.review_gate:
                await self.review_gate.wait()
            return FINAL

        async def page(bound, value):
            self.on_business()
            self.pages.append((bound, value.fragment))
            return Outcome.CONFIRMED

        async def context(bound, key, body):
            self.on_business()
            self.contexts.append((bound, key, body))
            return Outcome.CONFIRMED  # Explicit fake boundary only.

        runtime = CallRuntime(
            path or self.path, RuntimeCallbacks(reply, review, page, context), char_budget=145
        )
        self.runtimes.append(runtime)
        await runtime.start()
        return runtime

    async def issue(self, runtime, target, ttl=3600):
        await runtime.create_call(target)
        secret = InvitationSecret.create()
        grant = await runtime.issue_web_invitation(target.call_id, secret, session_ttl=ttl)
        self.assertNotIn(
            secret.value, repr(secret) + repr(grant) + repr(grant.link(ORIGIN + "/call"))
        )
        self.assertEqual(grant.expires_at, self.now + 900)
        self.assertEqual(
            grant.link(ORIGIN + "/call").value, ORIGIN + "/call#invitation=" + secret.value
        )
        return secret, grant

    async def request(
        self,
        app,
        route,
        *,
        data=None,
        method=None,
        cookie=None,
        reference=None,
        headers=None,
        scheme="https",
        query=b"",
        raw=None,
        chunks=None,
        disconnect=False,
        send_failure=False,
    ):
        method = method or ("GET" if route == "state" else "POST")
        body = (
            raw
            if raw is not None
            else (b"" if data is None else json.dumps(data, ensure_ascii=False).encode())
        )
        values = {"host": "calls.example.test"}
        if method == "POST":
            values.update(origin=ORIGIN, **{"content-type": "application/json"})
        if cookie is not None:
            values["cookie"] = cookie
        if reference is not None:
            values[REFERENCE_HEADER] = reference
        for key, value in (headers or {}).items():
            if value is None:
                values.pop(key, None)
            else:
                values[key] = value
        scope = {
            "type": "http",
            "scheme": scheme,
            "method": method,
            "path": PATH + route,
            "query_string": query,
            "headers": [(k.encode(), v.encode()) for k, v in values.items()],
        }
        events = (
            [{"type": "http.disconnect"}]
            if disconnect
            else [
                {"type": "http.request", "body": part, "more_body": i < len(chunks) - 1}
                for i, part in enumerate(chunks)
            ]
            if chunks is not None
            else [{"type": "http.request", "body": body, "more_body": False}]
        )
        responses = []

        async def receive():
            if events:
                return events.pop(0)
            return {"type": "http.disconnect"}

        async def send(event):
            if send_failure:
                raise ConnectionError(PRIVATE)
            responses.append(event)

        await app(scope, receive, send)
        if not responses:
            return None
        start = responses[0]
        self.assertEqual(dict(start["headers"])[b"cache-control"], b"no-store")
        self.assertNotIn(b"access-control-allow-origin", dict(start["headers"]))
        result = json.loads(responses[1]["body"])
        return start["status"], result, dict(start["headers"])

    async def test_call_page_ignores_query_without_reflecting_it(self):
        app = CallWebApp(None, origin=ORIGIN)
        receive = AsyncMock(return_value={"type": "http.request", "body": b""})
        send = AsyncMock()
        await app(
            dict(
                type="http",
                scheme="https",
                method="GET",
                path=PATH,
                query_string=b"from=qq&utm_source=test",
                headers=[(b"host", b"calls.example.test")],
            ),
            receive,
            send,
        )

        start, body = [call.args[0] for call in send.await_args_list]
        self.assertEqual(start["status"], 200)
        self.assertEqual(dict(start["headers"])[b"content-type"], b"text/html; charset=utf-8")
        self.assertEqual(body["body"], app._assets[PATH].body)
        for value in (b"from=qq", b"utm_source"):
            self.assertNotIn(value, repr((start, body)).encode())

    async def test_query_preserves_http_request_boundaries(self):
        app = CallWebApp(None, origin=ORIGIN)
        cases = [
            (403, "origin_rejected", dict(route="", method="GET", headers={"host": "wrong.test"})),
            (
                403,
                "origin_rejected",
                dict(route="", method="GET", headers={"origin": "https://wrong.test"}),
            ),
            (400, "request_rejected", dict(route="", method="GET", scheme="http")),
            (
                400,
                "request_rejected",
                dict(route="", method="GET", headers={"x-test": "bad\x00value"}),
            ),
            (405, "method_rejected", dict(route="", method="POST")),
            (400, "request_rejected", dict(route="state", method="GET", query=b"x=1")),
            (400, "request_rejected", dict(route="exchange", method="POST", query=b"x=1")),
            (400, "request_rejected", dict(route="audio", method="POST", query=b"x=1")),
            (400, "request_rejected", dict(route="call.js", method="GET", query=b"v=1")),
        ]
        for expected_status, error, request in cases:
            with self.subTest(request=request):
                status, body, _ = await self.request(app, **({"query": b"foo=bar"} | request))
                self.assertEqual((status, body), (expected_status, {"error": error}))

    async def test_websocket_rejects_query_before_handshake(self):
        app = CallWebApp(None, origin=ORIGIN)
        receive = AsyncMock(return_value={"type": "websocket.connect"})
        send = AsyncMock()
        await app(
            dict(
                type="websocket",
                scheme="wss",
                path=PATH + "ws",
                query_string=b"x=1",
                headers=[
                    (b"host", b"calls.example.test"),
                    (b"origin", ORIGIN.encode()),
                    (b"cookie", (COOKIE + "=" + "a" * 64).encode()),
                ],
                subprotocols=["bot-call." + "b" * 64],
            ),
            receive,
            send,
        )
        receive.assert_not_awaited()
        send.assert_awaited_once_with({"type": "websocket.close", "code": 1008})

    async def test_static_gzip_negotiation_cache_length_and_uncompressed_integrity(self):
        import gzip
        import hashlib

        app = CallWebApp(None, origin=ORIGIN)
        vendor = "vendor/vad-v5-ort-1.22.0-2623a295/"
        manifest = json.loads(app._assets[PATH + vendor + "sources.json"].body)

        async def get(name, encoding=None):
            sent = []

            async def receive():
                return {"type": "http.request", "body": b""}

            async def send(message):
                sent.append(message)

            headers = [(b"host", b"calls.example.test")]
            if encoding is not None:
                headers.append((b"accept-encoding", encoding.encode()))
            await app(
                dict(type="http", scheme="https", method="GET", path=PATH + name, headers=headers),
                receive,
                send,
            )
            self.assertEqual(sent[0]["status"], 200)
            return dict(sent[0]["headers"]), sent[1]["body"]

        for name in (
            vendor + "ort-wasm-simd-threaded.wasm",
            vendor + "silero_vad_v5.onnx",
            "call.js",
        ):
            headers, raw = await get(name)
            self.assertNotIn(b"content-encoding", headers)
            self.assertEqual(headers[b"vary"], b"accept-encoding")
            self.assertEqual(raw, app._assets[PATH + name].body)
            zipped, body = await get(name, "br, gzip;q=1")
            self.assertEqual(zipped[b"content-encoding"], b"gzip")
            self.assertEqual(int(zipped[b"content-length"]), len(body))
            self.assertEqual(gzip.decompress(body), raw)
            self.assertLess(len(body), len(raw))
            self.assertEqual(
                zipped[b"content-security-policy"], headers[b"content-security-policy"]
            )
            self.assertEqual(zipped[b"cache-control"], headers[b"cache-control"])
            task = app._assets[PATH + name].gzip_task
            self.assertEqual((await get(name, "gzip"))[1], body)
            self.assertIs(app._assets[PATH + name].gzip_task, task)
            for encoding in ("gzip;q=0, *;q=1", "br", "gzip;q=invalid"):
                plain, data = await get(name, encoding)
                self.assertNotIn(b"content-encoding", plain)
                self.assertEqual(data, raw)
        self.assertIn("sha256", json.dumps(manifest))

    async def exchange(self, app, secret):
        result = await self.request(app, "exchange", data={"invitation": secret.value})
        self.assertEqual(result[0], 200)
        header = result[2][b"set-cookie"].decode()
        for part in ["Secure", "HttpOnly", "SameSite=Strict", "Path=" + PATH]:
            self.assertIn(part, header)
        cookie = header.split(";", 1)[0]
        self.assertEqual(set(result[1]), {"reference", "expires_at", "experience", "capabilities"})
        self.assertEqual(
            result[1]["experience"],
            {"silence_ms": 1200, "sensitivity": 50, "capture_mode": "auto", "barge_in": "auto"},
        )
        self.assertEqual(
            result[1]["capabilities"]["seamless_series"], {"version": 1, "max_segments": 10}
        )
        self.assertNotIn(secret.value, json.dumps(result[1]))
        return cookie, result[1]["reference"], result[1]["expires_at"]

    def private_error(self, error):
        self.assertNotIn(PRIVATE, repr(error) + "".join(traceback.format_exception(error)))
        if error.__context__ is not None or error.__cause__ is not None:
            self.assertTrue(error.__suppress_context__)

    async def test_https_full_flow_saved_ack_cancel_replay_hangup_and_independent_expiry(self):
        runtime = await self.runtime()
        app = CallWebApp(runtime, origin=ORIGIN)
        target = binding("web-A")
        secret, grant = await self.issue(runtime, target)
        self.assertEqual((self.replies, self.reviews), ([], []))
        cookie, reference, expires = await self.exchange(app, secret)
        self.assertEqual(expires, self.now + 3600)
        self.now += 10
        duplicate = await self.exchange(app, secret)
        self.assertEqual(duplicate, (cookie, reference, expires))
        # The worker really waits behind another SQLite writer while this loop advances.
        entered, heartbeat = asyncio.Event(), asyncio.Event()
        ObservedStore.before_append = lambda: self.loop.call_soon_threadsafe(entered.set)
        with closing(sqlite3.connect(self.path)) as locker:
            locker.execute("BEGIN IMMEDIATE")
            pending = asyncio.create_task(
                self.request(
                    app,
                    "utterances",
                    data={"utterance_id": "lock", "text": "开头🙂 \n"},
                    cookie=cookie,
                    reference=reference,
                )
            )
            await asyncio.wait_for(entered.wait(), 2)
            self.loop.call_soon(heartbeat.set)
            await heartbeat.wait()
            self.assertFalse(pending.done())
            locker.commit()
            self.assertEqual((await pending)[0], 200)
        ObservedStore.before_append = None
        holder = {}
        ObservedStore.after_append = lambda: self.loop.call_soon_threadsafe(holder["task"].cancel)
        holder["task"] = asyncio.create_task(
            self.request(
                app,
                "utterances",
                data={"utterance_id": "stable", "text": PRIVATE},
                cookie=cookie,
                reference=reference,
            )
        )
        with self.assertRaises(asyncio.CancelledError):
            await holder["task"]
        ObservedStore.after_append = None
        await runtime.drain()
        replay = await self.request(
            app,
            "utterances",
            data={"utterance_id": "stable", "text": PRIVATE},
            cookie=cookie,
            reference=reference,
        )
        self.assertEqual((replay[0], replay[1]["accepted"]), (200, True))
        await runtime.drain()
        self.assertEqual([text for bound, text in self.replies], ["开头🙂 \n", PRIVATE])
        # Identical bodies are separate saved utterances, not implicit ACKs for each other.
        self.assertEqual(
            (
                await self.request(
                    app,
                    "utterances",
                    data={"utterance_id": "same-body", "text": PRIVATE},
                    cookie=cookie,
                    reference=reference,
                )
            )[0],
            200,
        )
        self.reply_failures.add("failed")
        self.assertEqual(
            (
                await self.request(
                    app,
                    "utterances",
                    data={"utterance_id": "failed", "text": "已保存但回复失败"},
                    cookie=cookie,
                    reference=reference,
                )
            )[0],
            200,
        )
        await runtime.drain()
        self.assertEqual(
            (
                await self.request(
                    app,
                    "utterances",
                    data={"utterance_id": "failed", "text": "已保存但回复失败"},
                    cookie=cookie,
                    reference=reference,
                )
            )[0],
            200,
        )
        await runtime.drain()
        self.assertEqual(self.reply_ids, ["lock", "stable", "same-body", "failed"])
        self.assertIsNone(
            await self.request(
                app, "utterances", disconnect=True, cookie=cookie, reference=reference
            )
        )
        observed = await self.request(
            app,
            "playback",
            data={"generation_id": "reply:stable", "revision": 1, "state": "started"},
            cookie=cookie,
            reference=reference,
        )
        self.assertEqual((observed[0], observed[1]["state"]), (200, "started"))
        self.assertEqual(
            (
                await self.request(
                    app,
                    "playback",
                    data={"generation_id": "reply:stable", "revision": 1, "state": "ended"},
                    cookie=cookie,
                    reference=reference,
                )
            )[0],
            409,
        )
        self.review_gate = asyncio.Event()
        self.gates.append(self.review_gate)
        self.assertIsNone(
            await self.request(
                app, "end", data={}, cookie=cookie, reference=reference, send_failure=True
            )
        )
        await asyncio.wait_for(self.review_entered.wait(), 2)
        state = await self.request(app, "state", cookie=cookie, reference=reference)
        self.assertEqual((state[0], state[1]["ended"], state[1]["complete"]), (200, True, False))
        frozen = (await runtime.read_state(target.call_id)).review.input_json
        # A transport ACK can be lost after a late playback COMMIT. Query the
        # original generation, repeat the same receipt, never rewrite frozen input.
        self.assertIsNone(
            await self.request(
                app,
                "playback",
                data={"generation_id": "reply:stable", "revision": 2, "state": "stopped"},
                cookie=cookie,
                reference=reference,
                send_failure=True,
            )
        )
        late = await self.request(
            app,
            "playback",
            data={"generation_id": "reply:stable", "revision": 2, "state": "stopped"},
            cookie=cookie,
            reference=reference,
        )
        self.assertEqual((late[0], late[1]["revision"], late[1]["state"]), (200, 2, "stopped"))
        self.assertEqual(
            (
                await self.request(
                    app,
                    "playback",
                    data={"generation_id": "reply:stable", "revision": 3, "state": "ended"},
                    cookie=cookie,
                    reference=reference,
                )
            )[0],
            409,
        )
        old = await self.request(
            app,
            "playback",
            data={"generation_id": "reply:stable", "revision": 1, "state": "started"},
            cookie=cookie,
            reference=reference,
        )
        self.assertEqual((old[1]["revision"], old[1]["state"]), (2, "stopped"))
        self.assertEqual((await runtime.read_state(target.call_id)).review.input_json, frozen)
        self.assertEqual(
            next(t for t in json.loads(frozen) if t["material_id"] == "reply:stable")[
                "playback_state"
            ],
            "started",
        )
        projection = (await self.request(app, "state", cookie=cookie, reference=reference))[1]
        self.assertEqual(
            next(t for t in projection["turns"] if t["material_id"] == "reply:stable")[
                "playback_revision"
            ],
            2,
        )

        self.now = grant.expires_at + 1
        self.assertEqual(
            (await self.request(app, "exchange", data={"invitation": secret.value}))[0], 401
        )
        self.review_gate.set()
        await runtime.drain()
        state = await self.request(app, "state", cookie=cookie, reference=reference)
        self.assertTrue(state[1]["complete"])
        self.assertEqual(state[1]["review_body"], FINAL)
        self.assertEqual(
            {t["body"] for t in state[1]["turns"] if t["role"] == "user"},
            {"开头🙂 \n", PRIVATE, "已保存但回复失败"},
        )
        saved = {t["material_id"]: t for t in state[1]["turns"] if t["role"] == "user"}
        self.assertEqual(set(saved), {"lock", "stable", "same-body", "failed"})
        self.assertEqual(saved["stable"]["body"], saved["same-body"]["body"])
        self.assertNotEqual(saved["stable"]["sequence"], saved["same-body"]["sequence"])
        replies = {r["utterance_id"]: r for r in state[1]["replies"]}
        self.assertEqual(replies["failed"]["state"], "failed")
        self.assertIsNone(replies["failed"]["bot_sequence"])
        self.assertEqual(saved["failed"]["body"], "已保存但回复失败")
        bots = {t["sequence"]: t for t in state[1]["turns"] if t["role"] == "assistant"}
        for utterance_id in saved:
            self.assertEqual(
                replies[utterance_id]["user_sequence"], saved[utterance_id]["sequence"]
            )
            self.assertEqual(replies[utterance_id]["generation_id"], "reply:" + utterance_id)
            if utterance_id != "failed":
                self.assertEqual(replies[utterance_id]["state"], "succeeded")
                self.assertEqual(
                    bots[replies[utterance_id]["bot_sequence"]]["material_id"],
                    replies[utterance_id]["generation_id"],
                )
        # Hangup forbids replaying submit, but the lost-ACK original ID is still queryable.
        self.assertEqual(
            (
                await self.request(
                    app,
                    "utterances",
                    data={"utterance_id": "stable", "text": PRIVATE},
                    cookie=cookie,
                    reference=reference,
                )
            )[0],
            409,
        )
        self.assertEqual(self.reply_ids, ["lock", "stable", "same-body", "failed"])
        self.assertEqual(len(self.reviews), 1)
        self.assertEqual(self.reviews[0][0], target)
        self.assertEqual(self.reviews[0][2], DEFAULT_REVIEW_PROMPT)
        self.assertEqual("".join(fragment for bound, fragment in self.pages), FINAL)
        self.assertEqual([body for bound, key, body in self.contexts], [FINAL])
        self.assertEqual(
            (
                await self.request(
                    app,
                    "utterances",
                    data={"utterance_id": "late", "text": PRIVATE},
                    cookie=cookie,
                    reference=reference,
                )
            )[0],
            409,
        )
        self.assertEqual(
            (await self.request(app, "end", data={}, cookie=cookie, reference=reference))[0], 200
        )
        await runtime.drain()
        self.assertEqual(len(self.reviews), 1)
        self.assertNotIn("binding", state[1])
        self.assertNotIn("review_policy", state[1])

    async def test_competing_entry_request_boundaries_and_old_tab_cannot_write_new_call(self):
        first, second = await self.runtime(), await self.runtime()
        apps = [
            CallWebApp(first, origin=ORIGIN, max_body_bytes=512),
            CallWebApp(second, origin=ORIGIN, max_body_bytes=512),
        ]
        a = replace(binding("A", "qq-A", "cid-A"), sender_id="user-A")
        b = replace(
            binding("B", "qq-B", "cid-B"),
            sender_id="user-B",
            platform_id="other-platform",
            unified_msg_origin="other-platform:FriendMessage:qq-B",
        )
        sa, _ = await self.issue(first, a, ttl=100)
        sb, _ = await self.issue(first, b, ttl=100)
        results = await asyncio.gather(
            *[
                self.request(app, "exchange", data={"invitation": s.value})
                for app, s in zip(apps, [sa, sb])
            ]
        )
        self.assertEqual(sorted(r[0] for r in results), [200, 409])
        winner = next(i for i, r in enumerate(results) if r[0] == 200)
        app, secret = apps[winner], [sa, sb][winner]
        cookie, ref, expiry = await self.exchange(app, secret)
        cross_user = replace(
            binding("same-user", "qq-cross", "cid-cross"),
            sender_id=[a, b][winner].sender_id,
            platform_id="third-platform",
            unified_msg_origin="third-platform:FriendMessage:qq-cross",
        )
        cross_secret, _ = await self.issue(first, cross_user, ttl=100)
        self.assertEqual(
            (await self.request(app, "exchange", data={"invitation": cross_secret.value}))[0], 409
        )
        before = (len(self.replies), len(self.reviews))
        invalid = [
            dict(route="state", cookie=cookie, reference="0" * 64),
            dict(route="state", cookie=cookie, reference=None),
            dict(
                route="state", cookie=COOKIE + "=" + InvitationSecret.create().value, reference=ref
            ),
            dict(route="state", cookie=cookie + "; " + cookie, reference=ref),
            dict(route="utterances", data={"utterance_id": "x", "text": PRIVATE, "call_id": "B"}),
            dict(
                route="playback",
                data={"generation_id": "reply:x", "revision": True, "state": "ended"},
            ),
            dict(
                route="playback",
                data={"generation_id": "reply:x", "revision": 1, "state": "ended", "call_id": "B"},
            ),
            dict(
                route="playback",
                data={"generation_id": "reply:x", "revision": 1, "state": "ended"},
                headers={"origin": "https://evil.test"},
            ),
            dict(
                route="utterances",
                data={"utterance_id": "x", "text": PRIVATE},
                headers={"origin": "https://evil.test"},
            ),
            dict(route="end", data={}, headers={"host": "evil.test"}),
            dict(route="end", data={}, headers={"origin": None}),
            dict(route="end", data={}, scheme="http", headers={"x-forwarded-proto": "https"}),
            dict(route="end", data={}, query=b"token=redacted-synthetic"),
            dict(route="state", method="POST", data={}),
            dict(route="unknown", data={}),
            dict(route="end", raw=b'{"a":1,"a":2}'),
            dict(route="end", raw=b"\xff"),
            dict(route="end", raw=b'{"x":NaN}'),
            dict(
                route="utterances",
                raw=json.dumps({"utterance_id": "x", "text": chr(0xD800)}).encode("ascii"),
            ),
            dict(route="end", chunks=[b"x" * 300, b"y" * 300]),
            dict(route="end", chunks=[b"x" * 300, b"y" * 300], headers={"content-length": "1"}),
        ]
        for case in invalid:
            case.setdefault("cookie", cookie)
            case.setdefault("reference", ref)
            result = await self.request(app, **case)
            self.assertGreaterEqual(result[0], 400)
            self.assertNotIn(PRIVATE, json.dumps(result[1]))
            self.assertNotIn(b"set-cookie", result[2])
        self.assertEqual((len(self.replies), len(self.reviews)), before)
        self.assertEqual(
            (await self.request(app, "end", data={}, cookie=cookie, reference=ref))[0], 200
        )
        await first.drain()
        await second.drain()
        loser = 1 - winner
        newcookie, newref, _ = await self.exchange(apps[loser], [sa, sb][loser])
        self.assertNotEqual(ref, newref)
        replies_before = len(self.replies)
        # Browser cookie now belongs to B, but old A tab keeps A's public reference.
        rejected = await self.request(
            apps[loser],
            "utterances",
            data={"utterance_id": "old-tab", "text": PRIVATE},
            cookie=newcookie,
            reference=ref,
        )
        self.assertEqual(rejected[0], 401)
        self.assertEqual(
            (
                await self.request(
                    apps[loser],
                    "playback",
                    data={"generation_id": "reply:stable", "revision": 1, "state": "ended"},
                    cookie=newcookie,
                    reference=ref,
                )
            )[0],
            401,
        )
        self.assertEqual(len(self.replies), replies_before)
        current = await self.request(apps[loser], "state", cookie=newcookie, reference=newref)
        self.assertEqual(current[1]["turns"], [])
        self.assertEqual((await self.request(app, "state", cookie=cookie, reference=ref))[0], 200)
        self.assertEqual((await first.read_state("A")).binding, a)
        self.assertEqual((await first.read_state("B")).binding, b)
        # Playback is authorized again after BEGIN IMMEDIATE, not only before
        # waiting for a writer. Seed one real success on the current web call.
        await self.request(
            apps[loser],
            "utterances",
            data={"utterance_id": "observe", "text": "保存用于播放"},
            cookie=newcookie,
            reference=newref,
        )
        await first.drain()
        await second.drain()
        entered_playback = asyncio.Event()
        original_observe = ObservedStore.observe_playback

        def signal_observe(store, *args, **kwargs):
            self.loop.call_soon_threadsafe(entered_playback.set)
            return original_observe(store, *args, **kwargs)

        prior_now = self.now
        with patch.object(ObservedStore, "observe_playback", signal_observe):
            with closing(sqlite3.connect(self.path)) as locker:
                locker.execute("BEGIN IMMEDIATE")
                waiting = asyncio.create_task(
                    self.request(
                        apps[loser],
                        "playback",
                        data={"generation_id": "reply:observe", "revision": 1, "state": "started"},
                        cookie=newcookie,
                        reference=newref,
                    )
                )
                await asyncio.wait_for(entered_playback.wait(), 2)
                self.now = expiry + 1
                locker.commit()
                self.assertEqual((await waiting)[0], 401)
        self.now = prior_now
        self.assertEqual(
            next(
                t
                for t in (await first.read_state([a, b][loser].call_id)).turns
                if t.material_id == "reply:observe"
            ).playback_revision,
            0,
        )
        replies_before = len(self.replies)
        # Expire while waiting for the actual write lock: recheck inside transaction rejects.
        entered = asyncio.Event()
        ObservedStore.before_append = lambda: self.loop.call_soon_threadsafe(entered.set)
        with closing(sqlite3.connect(self.path)) as locker:
            locker.execute("BEGIN IMMEDIATE")
            blocked = asyncio.create_task(
                self.request(
                    apps[loser],
                    "utterances",
                    data={"utterance_id": "expired", "text": PRIVATE},
                    cookie=newcookie,
                    reference=newref,
                )
            )
            await asyncio.wait_for(entered.wait(), 2)
            self.now = expiry + 1
            locker.commit()
            self.assertEqual((await blocked)[0], 401)
        ObservedStore.before_append = None
        self.assertEqual(len(self.replies), replies_before)
        c = binding("C")
        sc, _ = await self.issue(first, c, ttl=1500)
        cookie_c, ref_c, expiry_c = await self.exchange(apps[0], sc)
        unused, unused_grant = await self.issue(first, binding("never-entered"), ttl=1500)
        self.now = unused_grant.expires_at + 1
        denied = await self.request(apps[0], "exchange", data={"invitation": unused.value})
        self.assertEqual((denied[0], denied[1]), (401, {"error": "denied"}))
        # Expired open authority releases occupancy without generating/ending B.
        self.assertIsNone((await first.read_state([a, b][loser].call_id)).review)
        await first.shutdown()
        await second.shutdown()
        restarted = await self.runtime()
        reopened = CallWebApp(restarted, origin=ORIGIN)
        self.assertEqual(
            (await self.request(reopened, "state", cookie=cookie_c, reference=ref_c))[0], 200
        )
        self.assertEqual(len(self.replies), replies_before)
        self.assertEqual(len(self.reviews), 0)

    async def test_v5_upgrade_rollback_auth_commit_and_ack_loss_reopen_keep_secrets_out_of_db(self):
        seed = self.path.with_name("seed.db")
        with Store(seed) as store:
            target = binding("old")
            store.create_call(target, background=PRIVATE)
            store.append_user("old", "u", PRIVATE, source="text")
            store.end_call("old", prompt=PRIVATE)
            prepare_review_work(store, "old", input_token_budget=2500, count_tokens=len)
            names = [
                "calls",
                "turns",
                "generations",
                "reviews",
                "publications",
                "publication_pages",
                "review_work",
                "review_work_nodes",
                "review_work_stages",
                "review_policy_manifests",
            ]
            rows = {n: [tuple(r) for r in store._db.execute("SELECT * FROM " + n)] for n in names}
        with closing(sqlite3.connect(self.path)) as raw:
            for statement in (
                storage._SCHEMA
                + storage._REVIEW_SCHEMA
                + storage._PUBLICATION_SCHEMA
                + storage._WORK_SCHEMA
                + storage._MANIFEST_SCHEMA
            ):
                raw.execute(statement)
            raw.execute(f"PRAGMA application_id={storage._APPLICATION_ID}")
            raw.execute("PRAGMA user_version=5")
            for n in names:
                for row in rows[n]:
                    raw.execute(
                        "INSERT INTO " + n + " VALUES (" + ",".join("?" for _ in row) + ")", row
                    )
            raw.commit()
            schema = raw.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall()
        with patch.object(
            storage, "_ACCESS_SCHEMA", storage._ACCESS_SCHEMA + ("INVALID MIGRATION",)
        ):
            with self.assertRaises(sqlite3.Error):
                Store(self.path)
        with closing(sqlite3.connect(self.path)) as raw:
            self.assertEqual(raw.execute("PRAGMA user_version").fetchone()[0], 5)
            self.assertEqual(
                raw.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall(), schema
            )
        with Store(self.path) as upgraded:
            self.assertEqual(upgraded._db.execute("PRAGMA user_version").fetchone()[0], 24)
            for n in names:
                self.assertEqual(
                    [tuple(r) for r in upgraded._db.execute("SELECT * FROM " + n)], rows[n]
                )
            target = binding("commit")
            upgraded.create_call(target)
            secret = InvitationSecret.create()

            def deny_once():
                denials = [1]

                def authorizer(action, op, unused, database, trigger):
                    if action == sqlite3.SQLITE_TRANSACTION and op == "COMMIT" and denials[0]:
                        denials[0] -= 1
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                upgraded._db.set_authorizer(authorizer)
                return denials

            denied = deny_once()
            try:
                with self.assertRaises(AccessError) as error:
                    upgraded.issue_web_invitation("commit", secret, session_ttl=500)
                self.private_error(error.exception)
            finally:
                upgraded._db.set_authorizer(None)
            self.assertEqual(denied, [0])
            self.assertEqual(
                upgraded._db.execute("SELECT count(*) FROM web_invitations").fetchone()[0], 0
            )
            upgraded.issue_web_invitation("commit", secret, session_ttl=500)
            denied = deny_once()
            try:
                with self.assertRaises(AccessError) as error:
                    upgraded.exchange_web_invitation(secret.value)
                self.private_error(error.exception)
            finally:
                upgraded._db.set_authorizer(None)
            self.assertEqual(denied, [0])
            self.assertEqual(
                upgraded._db.execute("SELECT count(*) FROM web_sessions").fetchone()[0], 0
            )
        # COMMIT succeeded but its local ACK disappeared; same invitation/derived session recovers.
        runtime = await self.runtime(path=self.path.with_name("ack.db"))
        target = binding("ack")
        await runtime.create_call(target)
        key = InvitationSecret.create()
        ObservedStore.lose_issue = True
        try:
            await runtime.issue_web_invitation("ack", key, session_ttl=700)
        except RuntimeOperationError as error:
            self.private_error(error)
        else:
            self.fail("lost issuance ACK claimed success")
        self.now += 10
        grant = await runtime.issue_web_invitation("ack", key, session_ttl=700)
        self.assertEqual(grant.expires_at, 1900)
        for newkey, ttl in [(InvitationSecret.create(), 700), (key, 701)]:
            with self.assertRaises(AccessError):
                await runtime.issue_web_invitation("ack", newkey, session_ttl=ttl)
        app = CallWebApp(runtime, origin=ORIGIN)
        ObservedStore.lose_exchange = True
        failed = await self.request(app, "exchange", data={"invitation": key.value})
        self.assertEqual(failed[0], 503)
        self.assertNotIn(b"set-cookie", failed[2])
        self.now += 10
        cookie, ref, expires = await self.exchange(app, key)
        self.assertEqual(expires, 1710)
        self.assertEqual(cookie, (await self.exchange(app, key))[0])
        with Store(self.path.with_name("ack.db")) as inspect:
            dump = "\n".join(inspect._db.iterdump())
            for value in [key.value, cookie.split("=", 1)[1]]:
                self.assertNotIn(value, dump)
            row = inspect._db.execute("SELECT invitation_sha256 FROM web_invitations").fetchone()
            self.assertEqual(row[0], token_digest(key.value, "invitation"))
            for table in ["web_invitations", "web_sessions"]:
                with self.assertRaises(sqlite3.Error):
                    inspect._db.execute("DELETE FROM " + table)
        await runtime.shutdown()
        runtime = await self.runtime(path=self.path.with_name("ack.db"))
        app = CallWebApp(runtime, origin=ORIGIN)
        self.assertEqual((await self.request(app, "state", cookie=cookie, reference=ref))[0], 200)
        self.assertEqual(self.replies, [])
        self.assertEqual(self.reviews, [])


if __name__ == "__main__":
    unittest.main()
