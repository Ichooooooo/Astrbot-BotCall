"""Loopback TTS servers and real adapter/runtime integration."""

import asyncio
from contextlib import asynccontextmanager
import logging
import os
import struct
import wave
from pathlib import Path
from functools import partial
from types import SimpleNamespace as NS
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, patch

from aiohttp import ClientSession, encode_basic_auth, web

from bot_call.astrbot_compat import (
    dashscope_stream_settings,
    openai_stream_settings,
    tts_stream_settings,
)
from bot_call.astrbot_tts import AstrBotTTSAdapter, SpeechPolicy, convert_segment_audio
from bot_call.observability import CURRENT_TIMING, RoundTiming, TTS_STREAM_FALLBACK_REASONS
from bot_call.streaming_tts import stream_sentence, StreamFailure, CHUNK_BYTES
from test_astrbot_context import binding
from test_backend import NativeCase, MP3
from test_segments import StreamingProvider

SETTINGS = dict(model="cosyvoice-v3-flash", voice="synthetic", timeout=1, headers={})
PCM = bytes(CHUNK_BYTES)
SERVER_ERROR = "synthetic sensitive server error"


@asynccontextmanager
async def fake_dashscope(mode="ok", *, delay=0):
    seen, first, closed = [], asyncio.Event(), asyncio.Event()

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        try:
            run = await ws.receive_json()
            seen.append(run)
            tid = run["header"]["task_id"]

            async def event(name):
                header = dict(task_id=tid, event=name)
                if name == "task-failed":
                    header["error_message"] = SERVER_ERROR
                await ws.send_json(dict(header=header))

            if mode == "preclose":
                return ws
            await event("task-started")
            seen.append(await ws.receive_json())
            seen.append(await ws.receive_json())
            if delay:
                await asyncio.sleep(delay)
            if mode == "failed":
                await event("task-failed")
            elif mode == "timeout":
                await ws.receive()
            else:
                # Split within a sample and a 40ms frame to verify reassembly.
                await ws.send_bytes(PCM[:1])
                await ws.send_bytes(PCM[1:])
                first.set()
                if mode == "postclose":
                    return ws
                if mode == "postfailed":
                    await event("task-failed")
                elif mode == "hold":
                    await ws.receive()
                else:
                    for _ in range(132 if mode == "burst" else 24 if mode == "slow" else 1):
                        await ws.send_bytes(PCM)
                        if mode == "slow":
                            await asyncio.sleep(0.02)
                    await event("task-finished")
            return ws
        finally:
            await ws.close()
            closed.set()

    app = web.Application()
    app.router.add_get("/", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        yield NS(url=f"ws://127.0.0.1:{port}/", seen=seen, first=first, closed=closed)
    finally:
        await runner.cleanup()


class SettingsTests(TestCase):
    def test_dashscope_stream_model_scope(self):
        provider = NS(provider_config=dict(type="dashscope_tts", api_key="synthetic-key"))
        for model in (
            "cosyvoice-v3-flash",
            " CosyVoice-v2 ",
            "qwen-audio-3.1-tts-flash",
            "qwen-audio-3.0-tts-plus",
            "bailian-tts-v4",
        ):
            with self.subTest(model=model):
                provider.get_model = lambda: model
                self.assertEqual(dashscope_stream_settings(provider)["model"], model.strip())

        for model in (
            "sambert-zhichu-v1",
            "qwen-tts",
            "qwen-tts-latest",
            "qwen3-tts-flash",
            " QWEN3.5-TTS-flash ",
            "qwen3-tts-flash-realtime",
            "foo-realtime",
            "foo-realtime-preview",
            "",
            " ",
            None,
            123,
        ):
            with self.subTest(model=model):
                provider.get_model = lambda: model
                self.assertIsNone(dashscope_stream_settings(provider))

        provider.get_model = lambda: "cosyvoice-v3-flash"
        provider.provider_config["type"] = "other"
        self.assertIsNone(dashscope_stream_settings(provider))


class ProtocolTests(IsolatedAsyncioTestCase):
    async def test_protocol_normal_pre_post_failure_task_failed_timeout_cancel(self):
        for mode in ("ok", "preclose", "postclose", "failed", "postfailed", "timeout", "hold"):
            with self.subTest(mode=mode):
                chunks = []

                async def emit(data):
                    chunks.append(data)

                async with fake_dashscope(mode) as server:
                    settings = dict(SETTINGS, timeout=0.15 if mode == "timeout" else 2)
                    task = asyncio.create_task(
                        stream_sentence("合成测试。", settings, emit, url=server.url)
                    )
                    if mode == "ok":
                        self.assertEqual(await task, PCM * 2)
                        self.assertEqual(chunks, [PCM, PCM])
                    elif mode == "hold":
                        await asyncio.wait_for(server.first.wait(), 1)
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                    else:
                        with self.assertRaises(StreamFailure) as error:
                            await task
                        self.assertEqual(
                            error.exception.received, mode in {"postclose", "postfailed"}
                        )
                        self.assertEqual(
                            error.exception.reason,
                            "task_failed" if mode in {"failed", "postfailed"} else "other",
                        )
                        self.assertEqual(str(error.exception), "tts.stream_failed")
                        self.assertNotIn(SERVER_ERROR, repr(vars(error.exception)))
                    await asyncio.wait_for(server.closed.wait(), 1)
                    self.assertEqual(
                        [e["header"]["action"] for e in server.seen],
                        ["run-task"]
                        if mode == "preclose"
                        else ["run-task", "continue-task", "finish-task"],
                    )


class AdapterTests(NativeCase):
    async def test_stream_circuit_is_per_call_and_only_counts_pre_audio_failures(self):
        from bot_call.observability import (
            CURRENT_TIMING,
            RoundTiming,
            TTS_STREAM_FALLBACK_REASONS,
            timing_row,
        )

        event, _ = self.environment()
        provider = self.context.tts[event.unified_msg_origin]
        provider.provider_config.update(
            type="dashscope_tts", api_key="synthetic-key", dashscope_tts_voice="synthetic"
        )
        provider.model = "cosyvoice-v3-flash"
        host = self.host()
        guard = AsyncMock(return_value=True)
        pcm = AsyncMock(index=0, delivered=False)

        timeout = StreamFailure(reason="connect_timeout")
        proxy = StreamFailure(reason="proxy_unsupported")
        rejected = StreamFailure(reason="task_failed")
        partial_audio = StreamFailure(received=True, reason="task_failed")
        skipped = AssertionError("the open circuit must skip streaming")

        scenarios = (
            (
                "first",
                [
                    (timeout, "connect_timeout"),
                    (timeout, "connect_timeout"),
                    (skipped, "circuit_open"),
                ],
            ),
            ("new", [(PCM, None), (proxy, "proxy_unsupported"), (skipped, "circuit_open")]),
            ("rejected", [(rejected, "task_failed"), (skipped, "circuit_open")]),
            (
                "reset",
                [
                    (timeout, "connect_timeout"),
                    (PCM, None),
                    (timeout, "connect_timeout"),
                    (partial_audio, None),
                    (PCM, None),
                ],
            ),
        )
        for call_id, rounds in scenarios:
            target = binding(call_id, "qq-A", "A")
            adapter = AstrBotTTSAdapter(
                self.context,
                SpeechPolicy(target, provider.meta().id, provider.get_model()),
                converter=self.converter,
                segment_converter=self.converter,
                privacy=host.privacy,
            )
            turn = NS(call_id=call_id, role="assistant", body="合成测试。", segment_seq=1)
            for outcome, reason in rounds:
                with self.subTest(call_id=call_id, outcome=type(outcome).__name__, reason=reason):
                    timing = RoundTiming()
                    token = CURRENT_TIMING.set(timing)
                    provider_calls = len(provider.texts)
                    try:
                        with patch("bot_call.streaming_tts.stream_sentence") as stream:
                            if isinstance(outcome, Exception):
                                stream.side_effect = outcome
                            else:
                                stream.return_value = outcome
                            if outcome is partial_audio:
                                with self.assertRaisesRegex(
                                    RuntimeError, "^speech.result_uncertain$"
                                ):
                                    await adapter.synthesize(
                                        target, turn, before_invoke=guard, pcm=pcm
                                    )
                            elif reason:
                                with self.assertLogs(
                                    "bot_call.streaming_tts", level="WARNING"
                                ) as logs:
                                    result = await adapter.synthesize(
                                        target, turn, before_invoke=guard, pcm=pcm
                                    )
                                self.assertEqual(result, MP3)
                                self.assertEqual(
                                    logs.output,
                                    [
                                        f"WARNING:bot_call.streaming_tts:bot_call.tts_stream_fallback reason={reason}"
                                    ],
                                )
                            else:
                                self.assertEqual(
                                    await adapter.synthesize(
                                        target, turn, before_invoke=guard, pcm=pcm
                                    ),
                                    MP3,
                                )
                    finally:
                        CURRENT_TIMING.reset(token)
                    if reason == "circuit_open":
                        stream.assert_not_awaited()
                    else:
                        stream.assert_awaited_once()
                    self.assertEqual(len(provider.texts) - provider_calls, int(reason is not None))
                    codes = [
                        timing_row(r)["elapsed_ms"]
                        for r in timing.rows
                        if r["stage"] == "tts_stream_fallback"
                    ]
                    self.assertEqual(codes, [TTS_STREAM_FALLBACK_REASONS[reason]] if reason else [])

    async def test_fallback_once_only_before_audio_and_archive_after_success(self):
        event, _ = self.environment()
        provider = self.context.tts[event.unified_msg_origin]
        provider.provider_config.update(
            type="dashscope_tts", api_key="synthetic-key", dashscope_tts_voice="synthetic"
        )
        provider.model = "cosyvoice-v3-flash"
        host = self.host()
        target = binding("A", "qq-A", "A")
        adapter = AstrBotTTSAdapter(
            self.context,
            SpeechPolicy(target, provider.meta().id, provider.get_model()),
            converter=self.converter,
            segment_converter=self.converter,
            privacy=host.privacy,
        )
        turn = NS(call_id=target.call_id, role="assistant", body="合成测试。", segment_seq=1)
        before = 0

        async def guard():
            nonlocal before
            before += 1
            return True

        for mode in ("preclose", "postclose", "ok", "timeout", "failed"):
            with self.subTest(mode=mode):
                chunks = []

                async def emit(data):
                    chunks.append(data)
                    emit.delivered = True

                emit.delivered = False
                provider.provider_config["timeout"] = 0.1 if mode == "timeout" else 2
                calls = len(provider.texts)
                async with fake_dashscope(mode) as server:
                    with patch(
                        "bot_call.streaming_tts.stream_sentence",
                        partial(stream_sentence, url=server.url),
                    ):
                        if mode == "postclose":
                            with self.assertRaises(RuntimeError):
                                await adapter.synthesize(
                                    target, turn, before_invoke=guard, pcm=emit
                                )
                        else:
                            self.assertEqual(
                                await adapter.synthesize(
                                    target, turn, before_invoke=guard, pcm=emit
                                ),
                                MP3,
                            )
                self.assertEqual(
                    len(provider.texts) - calls, int(mode in {"preclose", "failed", "timeout"})
                )
                self.assertEqual(
                    chunks[-1:] if mode == "ok" else [], [None] if mode == "ok" else []
                )
        self.assertEqual(before, 8)
        self.assertFalse(adapter._tasks)

    async def test_real_push_claim_order_stop_disconnect_and_archive(self):
        event, provider = self.environment(provider_class=StreamingProvider)
        tts = self.context.tts[event.unified_msg_origin]
        tts.provider_config.update(
            type="dashscope_tts", api_key="synthetic-key", dashscope_tts_voice="synthetic"
        )
        tts.model = "cosyvoice-v3-flash"
        original_settings = self.settings
        self.settings = lambda: dict(original_settings(), call_tts_provider_id=tts.meta().id)
        backend, _ = await self.backend()
        handle, session = await self.enter(backend, event)
        bound = backend._record(handle).binding
        for mode in (
            "ok",
            "rejected",
            "accepted",
            "unaccepted",
            "congested",
            "hold",
            "disconnect",
            "hangup",
        ):
            async with fake_dashscope(
                "burst"
                if mode == "congested"
                else "slow"
                if mode == "unaccepted"
                else "ok"
                if mode in {"ok", "rejected", "accepted"}
                else "hold"
            ) as server:
                with patch(
                    "bot_call.streaming_tts.stream_sentence",
                    partial(stream_sentence, url=server.url),
                ):
                    async with backend.watch(session.token, session.reference, pcm=True) as q:
                        await q.get()
                        await backend.web_submit(session.token, session.reference, mode, "测试")
                        frame = await asyncio.wait_for(q.audio.get(), 2)
                        self.assertEqual(frame["pcm"]["index"], 0)
                        state = await backend.web_state(session.token, session.reference)
                        row = next(
                            r
                            for r in state.segments
                            if r["generation_id"] == "reply:" + mode and r["seq"] == 1
                        )
                        self.assertEqual((row["revision"], row["playback_state"]), (0, "unknown"))
                        if mode in {"ok", "accepted", "hold", "disconnect", "hangup"}:
                            await q.pcm_last.accept()
                            claimed = await backend.web_state(session.token, session.reference)
                            claimed_row = next(
                                r
                                for r in claimed.segments
                                if r["generation_id"] == "reply:" + mode and r["seq"] == 1
                            )
                            self.assertEqual(
                                (claimed_row["revision"], claimed_row["playback_state"]),
                                (-1, "unknown"),
                            )
                        if mode == "ok":
                            while not (await asyncio.wait_for(q.audio.get(), 2))["pcm"]["end"]:
                                pass
                            await backend.drain()
                            self.assertEqual(
                                await backend.web_segment(
                                    session.token, session.reference, "reply:ok", 1
                                ),
                                MP3,
                            )
                        elif mode in {"rejected", "accepted"}:
                            # Late positive/negative receipts retain socket ownership
                            # after conversion/archive has closed the producer.
                            await backend.drain()
                            self.assertIsNone(q.pcm_owner)
                            if mode == "accepted":
                                await backend.web_segment(
                                    session.token,
                                    session.reference,
                                    "reply:" + mode,
                                    1,
                                    dict(revision=1, state="started"),
                                )
                            await q.pcm_last.reject()
                            state = await backend.web_state(session.token, session.reference)
                            row = next(
                                r
                                for r in state.segments
                                if r["generation_id"] == "reply:" + mode and r["seq"] == 1
                            )
                            self.assertEqual(row["state"], "succeeded")
                            self.assertEqual(
                                (row["revision"], row["playback_state"]),
                                (0, "unknown") if mode == "rejected" else (1, "started"),
                            )
                        elif mode == "congested":
                            await backend.drain()
                            self.assertTrue(q.pcm_closed)
                            self.assertTrue(q.pcm_last.rejected)
                            self.assertEqual(
                                await backend.web_segment(
                                    session.token, session.reference, "reply:congested", 1
                                ),
                                MP3,
                            )
                        elif mode == "hold":
                            await backend.web_segment(
                                session.token,
                                session.reference,
                                "reply:hold",
                                1,
                                dict(revision=1, state="stopped"),
                            )
                        elif mode == "hangup":
                            await backend.web_end(session.token, session.reference)
                    await asyncio.wait_for(server.closed.wait(), 2)
                    await backend.drain()
                    if mode == "disconnect":
                        resumed = await backend.web_state(session.token, session.reference)
                        following = next(
                            r
                            for r in resumed.segments
                            if r["generation_id"] == "reply:disconnect" and r["seq"] == 2
                        )
                        self.assertEqual(following["state"], "succeeded")
                    if mode == "unaccepted":
                        archived = await backend.web_state(session.token, session.reference)
                        first = next(
                            r
                            for r in archived.segments
                            if r["generation_id"] == "reply:unaccepted" and r["seq"] == 1
                        )
                        self.assertEqual((first["state"], first["revision"]), ("succeeded", 0))
                        self.assertEqual(
                            await backend.web_segment(
                                session.token, session.reference, "reply:unaccepted", 1
                            ),
                            MP3,
                        )
        self.assertEqual(tts.texts.count("这是第二句话。"), 6)
        closed_state = await backend.read_state(handle)
        # Only the deliberately started fixture may be described as partial;
        # rejected/queued/stopped streams still have no playback observation.
        self.assertEqual(closed_state.review.prompt.count("播了一部分（浏览器观测）"), 1)
        self.assertIn(
            '"generation_id": "reply:accepted", "playback": "播了一部分（浏览器观测）"',
            closed_state.review.prompt,
        )
        timings = await backend._runtime.diagnostic_timings()
        self.assertTrue(any(r["stage"] == "tts_first_chunk" for r in timings["stages"]))

    async def test_empty_selection_keeps_native_path_even_for_dashscope(self):
        event, _ = self.environment(provider_class=StreamingProvider)
        tts = self.context.tts[event.unified_msg_origin]
        tts.model = "cosyvoice-v3-flash"
        tts.provider_config.update(
            type="dashscope_tts", api_key="synthetic-key", dashscope_tts_voice="synthetic"
        )
        backend, _ = await self.backend()
        _, session = await self.enter(backend, event)
        with patch(
            "bot_call.streaming_tts.stream_sentence",
            side_effect=AssertionError("unexpected stream"),
        ) as stream:
            async with backend.watch(session.token, session.reference, pcm=True) as queue:
                await backend.web_submit(session.token, session.reference, "default", "测试")
                await backend.drain()
                self.assertTrue(queue.audio.empty())
                stream.assert_not_called()
                self.assertEqual(len(tts.texts), 2)
        self.assertEqual(
            await backend.web_segment(session.token, session.reference, "reply:default", 1), MP3
        )


class _MemoryLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(self.format(record))


def _self_signed_tls(directory):
    from datetime import datetime, timedelta, timezone
    import ipaddress
    import ssl
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path = Path(directory) / "cert.pem"
    key_path = Path(directory) / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(str(cert_path), str(key_path))
    client_ctx = ssl.create_default_context(cafile=str(cert_path))
    return server_ctx, client_ctx


@asynccontextmanager
async def fake_dashscope_wss(mode="ok", *, expected_auth=None):
    from tempfile import TemporaryDirectory

    seen, headers_seen, closed = [], [], asyncio.Event()

    async def handler(request):
        headers_seen.append(dict(request.headers))
        if expected_auth and request.headers.get("Authorization") != expected_auth:
            return web.Response(status=401, text="unauthorized")
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        try:
            run = await ws.receive_json()
            seen.append(run)
            tid = run["header"]["task_id"]
            await ws.send_json(dict(header=dict(task_id=tid, event="task-started")))
            seen.append(await ws.receive_json())
            seen.append(await ws.receive_json())
            if mode == "ok":
                await ws.send_bytes(PCM[:1])
                await ws.send_bytes(PCM[1:])
                await ws.send_bytes(PCM)
                await ws.send_json(dict(header=dict(task_id=tid, event="task-finished")))
            return ws
        finally:
            await ws.close()
            closed.set()

    with TemporaryDirectory() as tmp:
        server_ctx, client_ctx = _self_signed_tls(tmp)
        app = web.Application()
        app.router.add_get("/", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=server_ctx)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            yield NS(
                url=f"wss://127.0.0.1:{port}/",
                ssl=client_ctx,
                seen=seen,
                headers=headers_seen,
                closed=closed,
            )
        finally:
            await runner.cleanup()


@asynccontextmanager
async def fake_http_proxy(mode="connect", *, expected_proxy_auth=None):
    connects, raw_transcripts, handlers = [], [], set()

    async def pipe(reader, writer, record=False):
        try:
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                if record:
                    raw_transcripts.append(chunk)
                writer.write(chunk)
                await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    async def handle(reader, writer):
        handlers.add((asyncio.current_task(), writer))
        upstream_writer = None
        try:
            if mode == "stall":
                await asyncio.sleep(30)
                return
            head = await reader.readuntil(b"\r\n\r\n")
            lines = head.decode("latin1").split("\r\n")
            method, target, _ = lines[0].split(" ", 2)
            req_headers = {}
            for line in lines[1:]:
                if ":" in line:
                    k, v = line.split(":", 1)
                    req_headers[k.strip().lower()] = v.strip()
            connects.append(dict(method=method, target=target, headers=req_headers))
            if mode == "reject_407":
                writer.write(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
                await writer.drain()
                return
            if (
                expected_proxy_auth
                and req_headers.get("proxy-authorization") != expected_proxy_auth
            ):
                writer.write(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
                await writer.drain()
                return
            host, port = target.rsplit(":", 1)
            upstream_reader, upstream_writer = await asyncio.open_connection(host, int(port))
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()
            await asyncio.gather(
                pipe(reader, upstream_writer, record=True),
                pipe(upstream_reader, writer, record=False),
            )
        except Exception:
            pass
        finally:
            handlers.discard((asyncio.current_task(), writer))
            writer.close()
            if upstream_writer is not None:
                upstream_writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield NS(
            url=f"http://127.0.0.1:{port}",
            port=port,
            connects=connects,
            raw_transcripts=raw_transcripts,
        )
    finally:
        server.close()
        for task, writer in tuple(handlers):
            writer.close()
            task.cancel()
        await server.wait_closed()


class ProxyAndTimeoutTests(NativeCase):
    def test_proxy_selection_and_no_proxy_rules(self):
        from bot_call.streaming_tts import resolve_proxy, should_bypass_proxy, URL

        self.assertEqual(resolve_proxy(URL, {}), (None, None))
        # Priority: wss_proxy > https_proxy > all_proxy; lowercase > uppercase; HTTP_PROXY ignored for wss.
        self.assertEqual(
            resolve_proxy(URL, {"HTTP_PROXY": "http://wrong:1"})[0],
            None,
        )
        self.assertEqual(
            resolve_proxy(URL, {"ALL_PROXY": "http://all:1"})[0],
            "http://all:1",
        )
        self.assertEqual(
            resolve_proxy(URL, {"HTTPS_PROXY": "http://https:1", "ALL_PROXY": "http://all:1"})[0],
            "http://https:1",
        )
        self.assertEqual(
            resolve_proxy(
                URL,
                {
                    "WSS_PROXY": "http://wss-upper:1",
                    "wss_proxy": "http://wss-lower:1",
                    "HTTPS_PROXY": "http://https:1",
                },
            )[0],
            "http://wss-lower:1",
        )
        # Empty lowercase clears uppercase and falls through to next scheme.
        self.assertEqual(
            resolve_proxy(
                URL,
                {
                    "wss_proxy": "",
                    "WSS_PROXY": "http://wss-upper:1",
                    "https_proxy": "http://https:1",
                },
            )[0],
            "http://https:1",
        )
        # https:// proxy scheme is supported; user/pass stripped into Proxy-Authorization header.
        proxy_url, proxy_headers = resolve_proxy(
            URL, {"HTTPS_PROXY": "https://u1:p%40ss@proxy.local:8443"}
        )
        self.assertEqual(proxy_url, "https://proxy.local:8443")
        self.assertIn("Proxy-Authorization", proxy_headers)
        self.assertNotIn("u1", proxy_url)
        self.assertNotIn("p%40ss", proxy_url)

        # NO_PROXY: hostname, suffix, leading dot, wildcard, IPv4/IPv6, CIDR.
        no_proxy = "localhost,127.0.0.1,::1,proxy.example,app.example,192.0.2.0/24,192.0.2.1"
        self.assertFalse(should_bypass_proxy(URL, no_proxy))
        for bypass_url in (
            "wss://localhost/x",
            "wss://127.0.0.1:443/x",
            "wss://[::1]:443/x",
            "wss://proxy.example/x",
            "wss://sub.proxy.example/x",
            "wss://192.0.2.42/x",
        ):
            self.assertTrue(should_bypass_proxy(bypass_url, no_proxy), bypass_url)
        self.assertFalse(should_bypass_proxy("wss://198.51.100.42/x", no_proxy))
        self.assertFalse(should_bypass_proxy("wss://notproxy.example/x", no_proxy))
        self.assertTrue(should_bypass_proxy(URL, ".aliyuncs.com"))
        self.assertTrue(should_bypass_proxy(URL, "aliyuncs.com"))
        self.assertTrue(should_bypass_proxy(URL, "*.aliyuncs.com"))
        self.assertFalse(should_bypass_proxy(URL, "evilaliyuncs.com"))
        self.assertTrue(should_bypass_proxy(URL, "*"))

    async def test_socks_proxy_immediately_falls_back_without_direct_connect(self):
        from bot_call.observability import (
            CURRENT_TIMING,
            RoundTiming,
            TTS_STREAM_FALLBACK_REASONS,
            timing_summary,
        )
        from bot_call.streaming_tts import FALLBACK_PROXY_UNSUPPORTED

        event, _ = self.environment()
        provider = self.context.tts[event.unified_msg_origin]
        provider.provider_config.update(
            type="dashscope_tts",
            api_key="synthetic-secret-key",
            dashscope_tts_voice="synthetic",
        )
        provider.model = "cosyvoice-v3-flash"
        host = self.host()
        target = binding("A", "qq-A", "A")
        adapter = AstrBotTTSAdapter(
            self.context,
            SpeechPolicy(target, provider.meta().id, provider.get_model()),
            converter=self.converter,
            segment_converter=self.converter,
            privacy=host.privacy,
        )
        turn = NS(call_id=target.call_id, role="assistant", body="代理不支持。", segment_seq=1)
        handler = _MemoryLogHandler()
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        self.addCleanup(root_logger.removeHandler, handler)

        async def emit(chunk):
            raise AssertionError("socks proxy must never emit PCM")

        async def guard():
            return True

        async with fake_dashscope("ok") as server:
            env = {
                "ALL_PROXY": "socks5h://proxy-user:proxy-pass-secret@127.0.0.1:1080",
            }
            with patch.dict(os.environ, env, clear=True):
                with self.assertRaises(StreamFailure) as err:
                    await stream_sentence(
                        "测试",
                        dict(SETTINGS, headers={"Authorization": "Bearer synthetic-secret-key"}),
                        emit,
                        url=server.url,
                    )
                self.assertFalse(err.exception.received)
                self.assertEqual(err.exception.reason, FALLBACK_PROXY_UNSUPPORTED)
                self.assertEqual(server.seen, [])

                timing = RoundTiming()
                token = CURRENT_TIMING.set(timing)
                try:
                    with patch(
                        "bot_call.streaming_tts.stream_sentence",
                        partial(stream_sentence, url=server.url),
                    ):
                        result = await adapter.synthesize(
                            target, turn, before_invoke=guard, pcm=emit
                        )
                finally:
                    CURRENT_TIMING.reset(token)
        self.assertEqual(result, MP3)
        self.assertEqual(len(provider.texts), 1)
        self.assertEqual(server.seen, [])
        row = next(r for r in timing.rows if r["stage"] == "tts_stream_fallback")
        self.assertEqual(row["elapsed_ms"], TTS_STREAM_FALLBACK_REASONS["proxy_unsupported"])
        summary = timing_summary([dict(row, call_id="c", generation_id="reply:1", recorded_at=1)])
        self.assertEqual(summary["tts_stream_fallbacks"], {"proxy_unsupported": 1})
        joined_logs = "\n".join(handler.messages)
        # If target matches NO_PROXY, socks env var is bypassed and direct WSS/WS still works.
        async with fake_dashscope("ok") as direct_server:
            chunks = []

            async def collect(chunk):
                chunks.append(chunk)

            with patch.dict(
                os.environ,
                {"ALL_PROXY": "socks5://127.0.0.1:1080", "NO_PROXY": "127.0.0.1"},
                clear=True,
            ):
                self.assertEqual(
                    await stream_sentence("直连测试。", SETTINGS, collect, url=direct_server.url),
                    PCM * 2,
                )
        self.assertIn("reason=proxy_unsupported", joined_logs)
        for secret in ("proxy-user", "proxy-pass-secret", "synthetic-secret-key", "Bearer"):
            self.assertNotIn(secret, joined_logs)
            self.assertNotIn(secret, str(err.exception))

    async def test_connect_3s_timeout_fast_fallback_and_post_connect_uses_provider_timeout(self):
        from bot_call.observability import CURRENT_TIMING, RoundTiming, TTS_STREAM_FALLBACK_REASONS
        from bot_call.streaming_tts import CONNECT_TIMEOUT, FALLBACK_CONNECT_TIMEOUT, FALLBACK_OTHER

        self.assertEqual(CONNECT_TIMEOUT, 5.0)
        event, _ = self.environment()
        provider = self.context.tts[event.unified_msg_origin]
        provider.provider_config.update(
            type="dashscope_tts",
            api_key="synthetic-secret-key",
            dashscope_tts_voice="synthetic",
            timeout=20,
        )
        provider.model = "cosyvoice-v3-flash"
        host = self.host()
        target = binding("A", "qq-A", "A")
        adapter = AstrBotTTSAdapter(
            self.context,
            SpeechPolicy(target, provider.meta().id, provider.get_model()),
            converter=self.converter,
            segment_converter=self.converter,
            privacy=host.privacy,
        )
        turn = NS(call_id=target.call_id, role="assistant", body="超时测试。", segment_seq=1)

        async def emit(chunk):
            pass

        async def guard():
            return True

        # Real 3s connect timeout while provider timeout is 20s -> falls back around 3s.
        async with fake_http_proxy("stall") as proxy:
            with patch.dict(os.environ, {"HTTPS_PROXY": proxy.url}, clear=True):
                timing = RoundTiming()
                token = CURRENT_TIMING.set(timing)
                started = asyncio.get_running_loop().time()
                try:
                    with patch(
                        "bot_call.streaming_tts.stream_sentence",
                        partial(stream_sentence, url="wss://127.0.0.1:65530/"),
                    ):
                        result = await adapter.synthesize(
                            target, turn, before_invoke=guard, pcm=emit
                        )
                finally:
                    elapsed = asyncio.get_running_loop().time() - started
                    CURRENT_TIMING.reset(token)
        self.assertEqual(result, MP3)
        self.assertGreaterEqual(elapsed, 2.7)
        self.assertLess(elapsed, 6.0)
        row = next(r for r in timing.rows if r["stage"] == "tts_stream_fallback")
        self.assertEqual(row["elapsed_ms"], TTS_STREAM_FALLBACK_REASONS[FALLBACK_CONNECT_TIMEOUT])

        # After connect succeeds within CONNECT_TIMEOUT, overall timeout follows provider timeout.
        with patch.dict(os.environ, {}, clear=True):
            with patch("bot_call.streaming_tts.CONNECT_TIMEOUT", 0.15):
                async with fake_dashscope("ok", delay=0.25) as server:
                    chunks = []

                    async def collect(chunk):
                        chunks.append(chunk)

                    pcm_data = await stream_sentence(
                        "首包等待。", dict(SETTINGS, timeout=1.5), collect, url=server.url
                    )
                    self.assertEqual(pcm_data, PCM * 2)
                async with fake_dashscope("timeout") as server:
                    with self.assertRaises(StreamFailure) as err:
                        await stream_sentence(
                            "整体超时。", dict(SETTINGS, timeout=0.35), emit, url=server.url
                        )
                    self.assertEqual(err.exception.reason, FALLBACK_OTHER)

    async def test_http_connect_proxy_wss_streaming_direct_and_log_sanitization(self):
        import aiohttp
        from bot_call.streaming_tts import FALLBACK_HANDSHAKE_FAILED

        handler = _MemoryLogHandler()
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        self.addCleanup(root_logger.removeHandler, handler)

        auth_header = "Bearer synthetic-secret-key"
        expected_proxy_auth = aiohttp.encode_basic_auth("proxy-user", "proxy-pass-secret")
        settings = dict(SETTINGS, timeout=5, headers={"Authorization": auth_header})

        # 1) WSS over HTTP CONNECT proxy succeeds, proxy never sees Authorization header.
        async with (
            fake_dashscope_wss("ok", expected_auth=auth_header) as server,
            fake_http_proxy("connect", expected_proxy_auth=expected_proxy_auth) as proxy,
        ):
            proxy_env = {
                "HTTPS_PROXY": f"http://proxy-user:proxy-pass-secret@127.0.0.1:{proxy.port}",
                "NO_PROXY": "localhost,192.0.2.0/24",
            }
            chunks = []

            async def emit(chunk):
                chunks.append(chunk)

            with patch.dict(os.environ, proxy_env, clear=True):
                result = await stream_sentence(
                    "代理流式。", settings, emit, url=server.url, ssl=server.ssl
                )
            self.assertEqual(result, PCM * 2)
            self.assertEqual(chunks, [PCM, PCM])
            self.assertEqual(len(proxy.connects), 1)
            self.assertEqual(proxy.connects[0]["method"], "CONNECT")
            self.assertEqual(
                proxy.connects[0]["headers"]["proxy-authorization"], expected_proxy_auth
            )
            self.assertNotIn("authorization", proxy.connects[0]["headers"])
            raw_bytes = b"".join(proxy.raw_transcripts)
            self.assertNotIn(b"synthetic-secret-key", raw_bytes)
            self.assertEqual(server.headers[0].get("Authorization"), auth_header)

        # 2) Proxy handshake rejection (407) maps to handshake_failed, never logs secrets.
        async with fake_http_proxy("reject_407") as bad_proxy:
            bad_env = {
                "HTTPS_PROXY": f"http://proxy-user:proxy-pass-secret@127.0.0.1:{bad_proxy.port}"
            }
            with patch.dict(os.environ, bad_env, clear=True):
                with self.assertRaises(StreamFailure) as err:
                    await stream_sentence(
                        "握手失败。",
                        settings,
                        lambda c: asyncio.sleep(0),
                        url="wss://127.0.0.1:443/",
                    )
                self.assertFalse(err.exception.received)
                self.assertEqual(err.exception.reason, FALLBACK_HANDSHAKE_FAILED)

        # 3) No proxy env keeps direct connection working unchanged.
        async with fake_dashscope_wss("ok", expected_auth=auth_header) as server:
            chunks = []

            async def collect(chunk):
                chunks.append(chunk)

            with patch.dict(os.environ, {}, clear=True):
                result = await stream_sentence(
                    "无代理直连。", settings, collect, url=server.url, ssl=server.ssl
                )
            self.assertEqual(result, PCM * 2)

        joined_logs = "\n".join(handler.messages)
        for secret in (
            "proxy-user",
            "proxy-pass-secret",
            "synthetic-secret-key",
            expected_proxy_auth,
            "Bearer",
        ):
            self.assertNotIn(secret, joined_logs)


@asynccontextmanager
async def http_speech_server(handler):
    app = web.Application()
    app.router.add_post("/v1/audio/speech", handler)
    runner = web.AppRunner(app, handler_cancellation=True, access_log=None)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        yield f"http://127.0.0.1:{port}/v1"
    finally:
        await runner.cleanup()


@asynccontextmanager
async def fake_openai(
    parts=(), *, mime="application/octet-stream", status=200, finish="eof", header_delay=0
):
    seen, closed = [], asyncio.Event()

    async def handler(request):
        seen.append((await request.json(), dict(request.headers)))
        if status != 200:
            return web.Response(status=status, text=SERVER_ERROR)
        if header_delay:
            await asyncio.sleep(header_delay)
        response = web.StreamResponse(headers={"Content-Type": mime})
        await response.prepare(request)
        try:
            for part in parts:
                await response.write(part)
                await asyncio.sleep(0)
            if finish == "abort":
                request.transport.abort()
            elif finish == "hold":
                await asyncio.Event().wait()
            return response
        finally:
            closed.set()

    async with http_speech_server(handler) as base:
        yield NS(base=base, url=base + "/audio/speech", seen=seen, closed=closed)


def openai_provider(**config):
    return NS(
        provider_config=dict(type="openai_tts_api", **config),
        get_model=lambda: " sample-model ",
        meta=lambda: NS(id="sample-tts"),
    )


def wav_chunks(pcm, *, rate=24000, channels=1, bits=16, codec=1, size=None, header=None):
    def block(tag, data):
        return tag + struct.pack("<I", len(data)) + data + bytes(len(data) % 2)

    align = channels * bits // 8
    fmt = struct.pack("<HHIIHH", codec, channels, rate, rate * align, align, bits)
    if codec == 0xFFFE:
        fmt += struct.pack("<HHI", 22, 16, 4) + bytes.fromhex(
            "01000000 0000 1000 8000 00aa00389b71"
        )
    metadata = block(b"JUNK", b"abc") + block(b"LIST", b"INFO")
    riff = metadata + block(b"fmt ", fmt) if header is None else header
    data = b"data" + struct.pack("<I", len(pcm) if size is None else size) + pcm
    body = b"WAVE" + riff + data
    return b"RIFF" + struct.pack("<I", len(body)) + body


class OpenAISettingsTests(TestCase):
    def test_endpoints_and_provider_headers(self):
        for base, endpoint, response_format, rate, key_required in (
            (None, "https://api.openai.com/v1", "pcm", None, True),
            (" \t ", "https://api.openai.com/v1", "pcm", None, True),
            ("https://api.openai.com/v1/", "https://api.openai.com/v1", "pcm", None, True),
            ("https://api.siliconflow.cn/v1", "https://api.siliconflow.cn/v1", "wav", 24000, True),
            (
                "https://tts.siliconflow.com/v1",
                "https://tts.siliconflow.com/v1",
                "wav",
                24000,
                True,
            ),
            (
                "https://api.siliconflow.com/v1",
                "https://api.siliconflow.com/v1",
                "wav",
                24000,
                True,
            ),
            ("https://voice.example.test/", "https://voice.example.test", "wav", None, False),
            ("http://127.0.0.1:8000/v1/", "http://127.0.0.1:8000/v1", "wav", None, False),
        ):
            with self.subTest(base=base):
                provider = openai_provider(api_base=base, api_key=" synthetic-key ")
                provider.request_headers = {
                    "X-Sample": "yes",
                    "AuThOrIzAtIoN": "synthetic-override",
                    "content-type": "audio/wav",
                }
                settings = openai_stream_settings(provider)
                self.assertEqual(settings, tts_stream_settings(provider))
                self.assertEqual(
                    settings,
                    dict(
                        protocol="openai",
                        url=endpoint + "/audio/speech",
                        model="sample-model",
                        voice="alloy",
                        response_format=response_format,
                        sample_rate=rate,
                        timeout=20.0,
                        headers={
                            "X-Sample": "yes",
                            "Content-Type": "application/json",
                            "Authorization": "Bearer synthetic-key",
                        },
                        proxy="",
                    ),
                )
                provider.provider_config["api_key"] = None
                if key_required:
                    self.assertIsNone(openai_stream_settings(provider))
                else:
                    self.assertNotIn("Authorization", openai_stream_settings(provider)["headers"])

    def test_validation_and_dashscope_precedence(self):
        provider = openai_provider(
            api_base="https://voice.example.test/v1",
            **{"openai-tts-voice": " sample-voice "},
        )
        config = provider.provider_config
        for timeout, expected in ((None, 20), ("", 20), (5, 5), (0.5, 0.5), ("300", 300)):
            with self.subTest(timeout=timeout):
                config["timeout"] = timeout
                settings = openai_stream_settings(provider)
                self.assertEqual(settings["timeout"], float(expected))
                self.assertEqual(settings["voice"], "sample-voice")
        for proxy in (None, " http://proxy.example.test:8080 ", "socks5://localhost:1080", "://"):
            with self.subTest(proxy=proxy):
                config["proxy"] = proxy
                self.assertEqual(openai_stream_settings(provider)["proxy"], (proxy or "").strip())
        for key, values in {
            "timeout": (False, True, "bad", "nan", "inf", -1, 0, 301, [], 10**400),
            "api_base": (
                "https://",
                "https://voice.example.test:65536",
                "https://voice.example.test:bad",
                "https://voice.example.test/?key=x",
                "https://voice.example.test/#",
                "http://ex ample.test",
                "http://exam\nple.test",
                "file:///example",
                {},
            ),
            "api_key": ([], 42),
            "openai-tts-voice": (" ", None),
            "proxy": (False, []),
        }.items():
            previous = config.get(key)
            for value in values:
                with self.subTest(key=key, value=value):
                    config[key] = value
                    self.assertIsNone(openai_stream_settings(provider))
            config[key] = previous
        provider.request_headers = None
        self.assertIsNone(openai_stream_settings(provider))
        provider.request_headers = {}
        provider.get_model = lambda: " "
        self.assertIsNone(openai_stream_settings(provider))
        self.assertIsNone(openai_stream_settings(object()))
        self.assertIsNone(openai_stream_settings(NS(provider_config=[])))
        with (
            patch("bot_call.astrbot_compat.dashscope_stream_settings", return_value={}),
            patch("bot_call.astrbot_compat.openai_stream_settings") as openai,
        ):
            self.assertEqual(tts_stream_settings(object()), {})
        openai.assert_not_called()


class OpenAIProtocolTests(IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(
            patch.dict(os.environ, {"NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"})
        )

    async def test_wav_drains_only_pcm_and_checks_request_fields(self):
        audio = bytes(range(256)) * 16
        for siliconflow, size, codec in (
            (True, 0xFFFFFFFF, 1),
            (True, None, 1),
            (False, None, 0xFFFE),
            (False, 0, 1),
        ):
            with self.subTest(siliconflow=siliconflow, size=size, codec=codec):
                body = wav_chunks(audio, size=size, codec=codec)
                if size is None:
                    body += b"LIST\x04\x00\x00\x00INFO"
                parts = [body[i : i + 3] for i in range(0, 84, 3)] + [body[84:1003], body[1003:]]
                emit = AsyncMock()
                async with fake_openai(parts, mime="audio/wav") as server:
                    provider = (
                        openai_provider(
                            api_base="https://api.siliconflow.cn/v1", api_key="synthetic-key"
                        )
                        if siliconflow
                        else openai_provider(api_base=server.base)
                    )
                    settings = openai_stream_settings(provider)
                    self.assertEqual(
                        await stream_sentence(
                            "测试。", settings, emit, url=server.url if siliconflow else None
                        ),
                        audio,
                    )
                payload, headers = server.seen[0]
                expected = dict(
                    model="sample-model", voice="alloy", input="测试。", response_format="wav"
                )
                if siliconflow:
                    expected["sample_rate"] = 24000
                    self.assertEqual(headers["Authorization"], "Bearer synthetic-key")
                else:
                    self.assertNotIn("Authorization", headers)
                self.assertEqual(payload, expected)
                self.assertEqual(
                    [call.args[0] for call in emit.await_args_list],
                    [audio[:1920], audio[1920:3840], audio[3840:]],
                )

    async def test_pre_audio_rejections(self):
        cases = (
            [("wav", "audio/wav", [wav_chunks(PCM, rate=rate)]) for rate in (16000, 32000, 44100)]
            + [
                ("wav", "audio/wav", [wav_chunks(PCM, **opts)])
                for opts in (
                    dict(channels=2),
                    dict(bits=8),
                    dict(codec=3, bits=32),
                    dict(header=b""),
                )
            ]
            + [
                ("wav", "audio/wav", [wav_chunks(PCM)[:25]]),
                ("wav", "audio/wav", [b"RIFF\x00\x00\x00\x00WAVEJUNK\xff\xff\xff\x7f"]),
                ("wav", "audio/mpeg", [b"ID3" + PCM]),
                ("wav", "application/json", [b'{"error":"synthetic"}']),
                ("pcm", "audio/x-wav", [PCM]),
                ("pcm", "application/problem+json", [PCM]),
                ("pcm", "text/plain", [PCM]),
            ]
            + [
                ("pcm", "application/octet-stream", [magic[:1], magic[1:] + b"fixture"])
                for magic in (
                    b"RIFF",
                    b"ID3",
                    b"OggS",
                    b"fLaC",
                    b"<!DO",
                    b"<htm",
                    b'{"er',
                    b'{"me',
                    b'{"de',
                )
            ]
        )
        for mode, mime, parts in cases:
            with self.subTest(mode=mode, mime=mime, prefix=parts[0][:16]):
                async with fake_openai(parts, mime=mime) as server:
                    settings = openai_stream_settings(openai_provider(api_base=server.base))
                    emit = AsyncMock()
                    with self.assertRaises(StreamFailure) as error:
                        await stream_sentence("测试。", dict(settings, response_format=mode), emit)
                self.assertFalse(error.exception.received)
                self.assertEqual(error.exception.reason, "format_unsupported")
                emit.assert_not_awaited()

    async def test_delayed_response_headers_and_cancel_close_connection(self):
        settings = openai_stream_settings(openai_provider(api_key="synthetic-key"))
        emit = AsyncMock()
        async with fake_openai([PCM], header_delay=0.15) as server:
            with patch("bot_call.streaming_tts.CONNECT_TIMEOUT", 0.05):
                self.assertEqual(
                    await stream_sentence("测试。", settings, emit, url=server.url), PCM
                )

        first = asyncio.Event()

        async def mark_first(chunk):
            first.set()

        async with fake_openai([PCM], finish="hold") as server:
            task = asyncio.create_task(
                stream_sentence("测试。", settings, mark_first, url=server.url)
            )
            try:
                await asyncio.wait_for(first.wait(), 2)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                await asyncio.wait_for(server.closed.wait(), 2)
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    async def test_connect_rejection_timeout_and_small_pcm_failures(self):
        settings = openai_stream_settings(openai_provider(api_key="synthetic-key"))
        emit = AsyncMock()
        for mode, reason in (("reject_407", "handshake_failed"), ("stall", "connect_timeout")):
            with self.subTest(proxy=mode):
                async with fake_http_proxy(mode) as proxy:
                    with self.assertRaises(StreamFailure) as error:
                        await stream_sentence(
                            "测试。",
                            dict(settings, timeout=0.25, proxy=proxy.url),
                            emit,
                            url="https://127.0.0.1/v1/audio/speech",
                        )
                self.assertEqual(error.exception.reason, reason)
                self.assertFalse(error.exception.received)
        for parts, finish, received in (
            ([], "eof", False),
            ([bytes(2)], "hold", True),
            ([bytes(1)], "eof", True),
        ):
            with self.subTest(parts=parts, finish=finish):
                async with fake_openai(parts, finish=finish) as server:
                    with self.assertRaises(StreamFailure) as error:
                        await stream_sentence(
                            "测试。", dict(settings, timeout=0.25), emit, url=server.url
                        )
                self.assertEqual(error.exception.reason, "other")
                self.assertEqual(error.exception.received, received)
        emit.assert_not_awaited()

    async def test_explicit_http_proxy_overrides_no_proxy_and_auth_is_stripped(self):
        handler = _MemoryLogHandler()
        self.addCleanup(logging.getLogger().removeHandler, handler)
        logging.getLogger().addHandler(handler)
        seen = []
        auth = encode_basic_auth("proxy-user", "proxy-pass")

        async def proxy(request):
            seen.append((str(request.url), request.headers.get("Proxy-Authorization")))
            async with ClientSession(trust_env=False) as session:
                async with session.post(request.url, json=await request.json()) as upstream:
                    return web.Response(
                        body=await upstream.read(), content_type=upstream.content_type
                    )

        async with (
            fake_openai([wav_chunks(PCM)], mime="audio/wav") as server,
            http_speech_server(proxy) as proxy_base,
        ):
            proxy_authority = proxy_base.removeprefix("http://").removesuffix("/v1")
            settings = openai_stream_settings(
                openai_provider(
                    api_base=server.base, proxy=f"http://proxy-user:proxy-pass@{proxy_authority}"
                )
            )
            emit = AsyncMock()
            self.assertEqual(await stream_sentence("测试。", settings, emit), PCM)
            emit.assert_awaited_once_with(PCM)
            self.assertEqual(seen, [(server.url, auth)])
            self.assertNotIn("Proxy-Authorization", server.seen[0][1])

            for invalid in (
                "socks5://proxy-user:proxy-pass@localhost:1080",
                "http://localhost:bad",
            ):
                with self.assertRaises(StreamFailure) as error:
                    await stream_sentence("测试。", dict(settings, proxy=invalid), emit)
                self.assertEqual(error.exception.reason, "proxy_unsupported")
                self.assertFalse(error.exception.received)
                self.assertNotIn("proxy-pass", str(error.exception))
        self.assertEqual(len(server.seen), 1)
        for secret in ("proxy-user", "proxy-pass", auth):
            self.assertNotIn(secret, "\n".join(handler.messages))


class OpenAIAdapterTests(NativeCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.enterContext(
            patch.dict(os.environ, {"NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"})
        )
        event, _ = self.environment()
        self.provider = self.context.tts[event.unified_msg_origin]
        self.provider.provider_config.update(type="openai_tts_api")
        self.target = binding("A", "qq-A", "A")
        self.turn = NS(call_id=self.target.call_id, role="assistant", body="测试。", segment_seq=1)
        self.privacy = self.host().privacy

    def adapter(self, segment_converter=None):
        return AstrBotTTSAdapter(
            self.context,
            SpeechPolicy(self.target, self.provider.meta().id, self.provider.get_model()),
            converter=self.converter,
            segment_converter=segment_converter or self.converter,
            privacy=self.privacy,
        )

    async def synth(self, adapter, emit):
        timing = RoundTiming()
        token = CURRENT_TIMING.set(timing)
        try:
            result = await adapter.synthesize(
                self.target, self.turn, before_invoke=AsyncMock(return_value=True), pcm=emit
            )
        finally:
            CURRENT_TIMING.reset(token)
        return result, timing.rows

    async def test_pcm_archives_with_real_converter_and_marks_first_chunk(self):
        audio = bytes(range(256)) * 16
        chunks = []

        async def emit(chunk):
            chunks.append(chunk)
            emit.delivered = True

        async def convert(source, **kwargs):
            with wave.open(source, "rb") as wav:
                self.assertEqual(
                    (wav.getframerate(), wav.getnchannels(), wav.getsampwidth()), (24000, 1, 2)
                )
                self.assertEqual(wav.readframes(wav.getnframes()), audio)
            return await convert_segment_audio(source, **kwargs)

        self.provider.provider_config["api_key"] = "synthetic-key"
        async with fake_openai(
            [audio[:1], audio[1:1931], audio[1931:]], mime="audio/pcm"
        ) as server:
            with patch(
                "bot_call.streaming_tts.stream_sentence", partial(stream_sentence, url=server.url)
            ):
                archive, rows = await self.synth(self.adapter(convert), emit)
        self.assertTrue(archive.startswith((b"ID3", b"\xff\xfb")))
        self.assertEqual(chunks, [audio[:1920], audio[1920:3840], audio[3840:], None])
        self.assertEqual(sum(row["stage"] == "tts_first_chunk" for row in rows), 1)
        self.assertEqual(self.provider.texts, [])
        self.assertEqual(
            server.seen[0][0],
            dict(
                model=self.provider.get_model(),
                voice="alloy",
                response_format="pcm",
                input="测试。",
            ),
        )

    async def test_pre_audio_fallback_and_circuit_thresholds(self):
        for status, parts, mime, proxy, reason, attempts in (
            (200, [wav_chunks(PCM, rate=32000)], "audio/wav", "", "format_unsupported", 1),
            (200, [wav_chunks(PCM, channels=2)], "audio/wav", "", "format_unsupported", 1),
            (200, [wav_chunks(PCM, bits=8)], "audio/wav", "", "format_unsupported", 1),
            (200, [wav_chunks(PCM, codec=3, bits=32)], "audio/wav", "", "format_unsupported", 1),
            (200, [PCM], "audio/mpeg", "", "format_unsupported", 1),
            (200, [b'{"error":"synthetic"}'], "application/json", "", "format_unsupported", 1),
            (400, [], "", "", "task_failed", 1),
            (401, [], "", "", "task_failed", 1),
            (404, [], "", "", "task_failed", 1),
            (500, [], "", "", "other", 2),
            (200, [], "", "socks5://localhost:1080", "proxy_unsupported", 1),
        ):
            with self.subTest(status=status, reason=reason):
                async with fake_openai(parts, mime=mime, status=status) as server:
                    self.provider.provider_config.update(api_base=server.base, proxy=proxy)
                    adapter = self.adapter()
                    emit = AsyncMock(index=0, delivered=False)
                    calls = len(self.provider.texts)
                    for expected in [reason] * attempts + ["circuit_open"]:
                        archive, rows = await self.synth(adapter, emit)
                        self.assertEqual(archive, MP3)
                        self.assertEqual(
                            [r["elapsed_ms"] for r in rows if r["stage"] == "tts_stream_fallback"],
                            [TTS_STREAM_FALLBACK_REASONS[expected]],
                        )
                    self.assertEqual(len(self.provider.texts) - calls, attempts + 1)
                    self.assertEqual(len(server.seen), 0 if proxy else attempts)
                    emit.assert_not_awaited()

    async def test_post_audio_failure_is_uncertain_and_never_calls_native(self):
        for finish, parts in (
            ("abort", [wav_chunks(PCM, size=0)]),
            ("hold", [wav_chunks(PCM, size=0)]),
            ("eof", [wav_chunks(PCM + b"\x01", size=0)]),
            ("eof", [wav_chunks(PCM, size=len(PCM) + 2)]),
        ):
            with self.subTest(finish=finish, length=len(parts[0])):
                async with fake_openai(parts, finish=finish) as server:
                    self.provider.provider_config.update(api_base=server.base, timeout=0.4)
                    adapter = self.adapter()
                    emit = AsyncMock(index=0, delivered=False)
                    with self.assertRaisesRegex(RuntimeError, "^speech.result_uncertain$"):
                        await self.synth(adapter, emit)
                    emit.assert_awaited_once_with(PCM)
                    self.assertEqual(len(server.seen), 1)
                self.assertEqual(self.provider.texts, [])
