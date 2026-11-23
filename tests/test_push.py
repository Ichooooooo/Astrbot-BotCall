"""Authenticated ASGI WebSocket, real storage thread, no listener or network."""

import asyncio
import json
import base64
import unittest
from bot_call.native_host import HostASGI
from bot_call.store import Store
from test_backend import NativeCase


class PushTests(NativeCase):
    async def test_socket_auth_progress_coalescing_resync_cleanup_and_numeric_metrics(self):
        event, _ = self.environment()
        self.context.persona_manager.results[event.unified_msg_origin][1]["name"] = "小声"
        backend, _ = await self.backend()
        h, w = await self.enter(backend, event)
        self.assertEqual(
            (await backend.web_state(w.token, w.reference)).to_dict()["bot_name"], "小声"
        )

        def scope(
            cookie=None,
            origin="https://calls.example.test",
            host="calls.example.test",
            reference=None,
        ):
            return dict(
                type="websocket",
                scheme="wss",
                path="/bot-call/ws",
                root_path="",
                query_string=b"",
                headers=[
                    (b"host", host.encode()),
                    (b"origin", origin.encode()),
                    (
                        b"cookie",
                        (cookie if cookie is not None else "__Secure-bot_call=" + w.token).encode(),
                    ),
                ],
                subprotocols=["bot-call." + (reference or w.reference)],
            )

        async def reject(sc):
            output = []

            async def recv():
                return {"type": "websocket.connect"}

            async def send(e):
                output.append(e)

            await backend.app(sc, recv, send)
            self.assertFalse(any(e["type"] == "websocket.accept" for e in output))

        await reject(scope(cookie=""))
        await reject(scope(cookie="__Secure-bot_call=" + "0" * 64))
        await reject(scope(origin="https://evil.test"))
        await reject(scope(host="evil.test"))
        await reject(scope(reference="0" * 64))
        # Only the trusted loopback HTTPS-proxy wrapper can open a WS path.
        output = []

        async def recv():
            return {"type": "websocket.connect"}

        async def send(e):
            output.append(e)

        await HostASGI(backend.app, lambda: True)(
            dict(scope(), client=("192.0.2.1", 10)), recv, send
        )
        self.assertEqual(output, [{"type": "websocket.close", "code": 1008}])
        incoming, outgoing = asyncio.Queue(), asyncio.Queue()
        await incoming.put({"type": "websocket.connect"})
        task = asyncio.create_task(backend.app(scope(), incoming.get, outgoing.put))
        try:
            self.assertEqual(
                (await asyncio.wait_for(outgoing.get(), 1))["type"], "websocket.accept"
            )
            sync = await asyncio.wait_for(outgoing.get(), 1)
            self.assertEqual(json.loads(sync["text"]), {"state": "sync"})
            await backend.web_metrics(
                w.token, w.reference, "page", {"client_model_cold": 254, "client_start_warm": 3}
            )
            await backend.web_audio(w.token, w.reference, "u", b"synthetic", "audio/webm")
            change = await asyncio.wait_for(outgoing.get(), 1)
            self.assertEqual(json.loads(change["text"])["state"], "changed")
            await backend.drain()
            state = await backend.web_state(w.token, w.reference)
            self.assertEqual(state.audio[0].state, "succeeded")
            await backend.web_metrics(
                w.token, w.reference, "reply:u", {"client_upload": 123, "client_download": 45}
            )
            with Store(self.path / "calls.db") as store:
                rows = list(
                    store._db.execute(
                        "SELECT stage,elapsed_ms FROM turn_timings WHERE stage LIKE 'client_%'"
                    )
                )
                self.assertEqual(
                    dict(rows),
                    {
                        "client_upload": 123,
                        "client_download": 45,
                        "client_model_cold": 254,
                        "client_start_warm": 3,
                    },
                )
                stored_ids = [
                    r[0]
                    for r in store._db.execute(
                        "SELECT generation_id FROM turn_timings WHERE stage='client_model_cold'"
                    )
                ]
                self.assertEqual(len(stored_ids), 1)
                self.assertTrue(stored_ids[0].startswith("page:"))
                self.assertNotIn(w.reference, stored_ids[0])

            async def metric_http(values, gid="reply:u"):
                payload = json.dumps({"generation_id": gid, "values": values}).encode()
                output = []

                async def read():
                    return {"type": "http.request", "body": payload}

                async def write(message):
                    output.append(message)

                sc = dict(
                    type="http",
                    method="POST",
                    scheme="https",
                    path="/bot-call/metrics",
                    headers=[
                        (b"host", b"calls.example.test"),
                        (b"origin", b"https://calls.example.test"),
                        (b"content-type", b"application/json"),
                        (b"cookie", ("__Secure-bot_call=" + w.token).encode()),
                        (b"x-bot-call-reference", w.reference.encode()),
                    ],
                )
                await backend.app(sc, read, write)
                return output[0]["status"]

            for values in [
                {"client_upload": True},
                {"client_upload": "private raw text"},
                {"unknown_stage": 12},
                {"client_upload": -1},
                {"client_upload": 3600001},
                {"client_transport": "websocket"},
                {"client_transport": True},
                {"client_transport": 3},
                {"client_ws_disconnects": 1.5},
                {"client_ws_reconnects": 4097},
                {"client_first_audio_push": True},
                {"client_first_audio_push": 0},
                {"client_first_audio_download": 2},
            ]:
                self.assertEqual(await metric_http(values), 400)
            # Playback snapshots use the existing metrics payload; sequence is
            # the sentence index, so later sentences do not overwrite seq1.
            for seq in (1, 2):
                self.assertEqual(
                    await metric_http(
                        {
                            "client_playback_sentence": seq,
                            "client_playback_path": seq,
                            "client_playback_base_us": seq * 1000,
                        }
                    ),
                    200,
                )
            for values in (
                {"client_playback_sentence": 0},
                {"client_playback_sentence": 513},
                {"client_playback_path": 1},
                {"client_playback_sentence": 1, "client_playback_path": True},
            ):
                self.assertIn(await metric_http(values), (400, 409))
            with Store(self.path / "calls.db") as stored:
                snapshots = stored._db.execute(
                    "SELECT sequence,elapsed_ms FROM turn_timings "
                    "WHERE stage='client_playback_base_us' ORDER BY sequence"
                ).fetchall()
                self.assertEqual([tuple(row) for row in snapshots], [(1, 1000), (2, 2000)])
            # No generation/audio job exists for a locally rejected clip.
            self.assertEqual(
                await metric_http(
                    {"client_clip_reject": 2, "client_clip_ms": 32000, "client_clip_bytes": 0},
                    "capture:rejected",
                ),
                200,
            )
            self.assertEqual(await metric_http({"client_clip_reject": 4}, "capture:missing"), 200)
            self.assertEqual(
                await metric_http(
                    {
                        "client_silent_drop": 1,
                        "client_silent_reason": 1,
                        "client_gate_vad_ms": 64,
                        "client_gate_vad_high_ms": 0,
                        "client_gate_vad_peak_permille": 564,
                        "client_gate_vad_prior_ms": 0,
                        "client_gate_energy_ms": 100,
                        "client_gate_energy_run_ms": 100,
                        "client_gate_last_ms": 20,
                        "client_gate_floor_ppm": 1500,
                        "client_gate_decision": 3,
                        "client_gate_unknown_reason": 32,
                        "client_gate_fallback_ms": 0,
                    },
                    "capture:silent",
                ),
                200,
            )
            for reason in (0, 8, 16, 32, 64, 128, 256, 512, 1023):
                self.assertEqual(await metric_http({"client_gate_unknown_reason": reason}), 200)
            for values in (
                {"client_silent_drop": 0},
                {"client_silent_drop": 2},
                {"client_silent_reason": 0},
                {"client_silent_reason": 7},
                {"client_silent_reason": "control"},
                {"client_gate_vad_ms": True},
                {"client_gate_floor_ppm": "private"},
                {"client_gate_last_ms": -1},
                {"client_gate_energy_ms": 3600001},
                {"client_gate_decision": 5},
                {"client_gate_unknown_reason": -1},
                {"client_gate_unknown_reason": 1024},
                {"client_gate_unknown_reason": True},
                {"client_gate_unknown_reason": "quality"},
                {"client_gate_unknown_reason": 1.5},
                {"client_gate_fallback_ms": 3600001},
                {"client_gate_vad_peak_permille": 1001},
            ):
                self.assertEqual(await metric_http(values, "capture:silent"), 400)

            for values in [
                {"client_clip_reject": True},
                {"client_clip_reject": 0},
                {"client_clip_reject": 5},
                {"client_clip_bytes": -1},
                {"client_clip_bytes": 1.5},
                {"client_clip_bytes": "private"},
                {"client_clip_ms": -1},
                {"client_clip_ms": 86400001},
                {"client_upload": 1},
                {"client_clip_reject": 2, "client_audio_state_end": 1},
            ]:
                self.assertIn(await metric_http(values, "capture:bad"), (400, 409))
            self.assertIn(await metric_http({"client_clip_reject": 2}, "capture:"), (400, 409))
            self.assertEqual(await metric_http({"client_clip_reject": 2}, "page"), 400)
            history = await backend._runtime.diagnostic_timings()
            rejected = {
                r["stage"]: r for r in history["stages"] if r["stage"].startswith("client_clip_")
            }
            self.assertEqual(rejected["client_clip_reject"]["latest_value"], 4)
            self.assertEqual(
                rejected["client_clip_ms"]["count"], 1, "missing must not be zero-filled"
            )
            self.assertEqual(rejected["client_clip_ms"]["median_ms"], 32000)
            self.assertEqual(rejected["client_clip_bytes"]["median_ms"], 0)
            self.assertEqual(await metric_http({"client_upload": 123}), 200)
            self.assertEqual(await metric_http({"client_upload": 123}, "page"), 400)
            self.assertEqual(await metric_http({"client_start_cold": 123}, "page"), 200)
            self.assertEqual(await metric_http({"client_segment_auto": 1}), 200)
            self.assertEqual(await metric_http({"client_segment_auto": 2}), 400)
            self.assertEqual(
                await metric_http(
                    {
                        "client_segment_auto_energy": 1,
                        "client_vad_gaps": 2,
                        "client_vad_lost_ms": 95,
                        "client_vad_start_gaps": 1,
                        "client_vad_start_skip_ms": 2291,
                    }
                ),
                200,
            )
            self.assertEqual(
                await metric_http(
                    {
                        "client_model_fetch_ms": 200,
                        "client_model_session_ms": 20,
                        "client_model_lag_ms": 0,
                    },
                    "page",
                ),
                200,
            )
            self.assertEqual(
                await metric_http(
                    {
                        "client_energy_frames": 0,
                        "client_audio_state_start": 1,
                        "client_audio_state_end": 4,
                    }
                ),
                200,
            )
            self.assertEqual(await metric_http({"client_energy_frames": 1}, "page"), 400)
            for bad in (
                {"client_energy_frames": True},
                {"client_energy_first_ms": 1.5},
                {"client_model_fetch_ms": -1},
                {"client_model_session_ms": "raw"},
                {"client_model_lag_ms": 600001},
                {"client_audio_state_end": 5},
                {"client_vad_gaps": True},
                {"client_vad_gaps": 66},
                {"client_vad_lost_ms": 60001},
                {"client_vad_start_gaps": True},
                {"client_vad_start_gaps": 66},
                {"client_vad_start_skip_ms": 60001},
                {"client_segment_auto_energy": 2},
            ):
                self.assertEqual(await metric_http(bad), 400)
            history = await backend._runtime.diagnostic_timings()
            self.assertEqual(history["vad"]["client_segment_auto_energy"], 1)
            self.assertEqual(history["vad_gaps"], dict(client_vad_gaps=2, client_vad_lost_ms=95))
            self.assertFalse(
                any(
                    r["stage"] in {"client_vad_gaps", "client_vad_lost_ms"}
                    for r in history["stages"]
                )
            )
            self.assertEqual(
                await metric_http(
                    {"client_transport": 0, "client_ws_disconnects": 0, "client_ws_reconnects": 0}
                ),
                200,
            )
            self.assertEqual(
                await metric_http(
                    {"client_transport": 1, "client_ws_disconnects": 1, "client_ws_reconnects": 1}
                ),
                200,
            )
            history = await backend._runtime.diagnostic_timings()
            self.assertEqual(
                history["transport"][0], dict(mode="mixed", disconnects=1, reconnects=1)
            )
            self.assertTrue(
                all(
                    r["stage"]
                    not in {"client_transport", "client_ws_disconnects", "client_ws_reconnects"}
                    for r in history["stages"]
                )
            )

            self.assertEqual(await metric_http({"client_first_audio_push": 1}), 200)
            self.assertEqual(
                (await backend._runtime.diagnostic_timings())["audio_delivery"],
                dict(push=1, download=0),
            )
            # This non-streaming route has no inline audio; no text/credentials.
            messages = [sync, change]
            while not outgoing.empty():
                messages.append(outgoing.get_nowait())
            serialized = json.dumps(messages, ensure_ascii=False)
            self.assertNotIn(w.token, serialized)
            self.assertNotIn(w.reference, serialized)
            self.assertNotIn("Bot建议", serialized)
            self.assertNotIn("body", serialized)
        finally:
            await incoming.put({"type": "websocket.disconnect", "code": 1000})
            await asyncio.wait_for(task, 1)
        self.assertEqual(backend._runtime._watchers, {})
        # A reconnect gets sync after registration and reads current full state.
        async with backend.watch(w.token, w.reference) as q:
            self.assertEqual((await q.get())["state"], "sync")
            self.assertEqual(
                (await backend.web_state(w.token, w.reference)).replies[0].state, "succeeded"
            )

    async def test_first_sentence_inline_bound_and_stopped_late_notifications(self):
        from test_segments import StreamingProvider
        from test_backend import MP3
        from bot_call.speech import MAX_PUSH_AUDIO_BYTES

        event, provider = self.environment(provider_class=StreamingProvider)
        provider.gate = asyncio.Event()
        self.gates.append(provider.gate)
        backend, _ = await self.backend()
        h, w = await self.enter(backend, event)
        scope = dict(
            type="websocket",
            scheme="wss",
            path="/bot-call/ws",
            headers=[
                (b"host", b"calls.example.test"),
                (b"origin", b"https://calls.example.test"),
                (b"cookie", ("__Secure-bot_call=" + w.token).encode()),
            ],
            subprotocols=["bot-call." + w.reference],
        )
        incoming, outgoing = asyncio.Queue(), asyncio.Queue()
        await incoming.put({"type": "websocket.connect"})
        task = asyncio.create_task(backend.app(scope, incoming.get, outgoing.put))

        async def until(predicate):
            for _ in range(30):
                message = await asyncio.wait_for(outgoing.get(), 2)
                if message["type"] == "websocket.send":
                    data = json.loads(message["text"])
                    if predicate(data):
                        return data
            self.fail("push milestone missing")

        try:
            await until(lambda e: e["state"] == "sync")
            await backend.web_submit(w.token, w.reference, "u", "测试")
            packet = await until(lambda e: "first_audio" in e)
            audio = packet["first_audio"]
            self.assertEqual(
                (audio["generation_id"], audio["seq"], audio["mime"]), ("reply:u", 1, "audio/mpeg")
            )
            self.assertEqual(base64.b64decode(audio["data"]), MP3)
            self.assertEqual(audio["byte_count"], len(MP3))
            self.assertNotIn(w.token, json.dumps(packet))
            self.assertNotIn(w.reference, json.dumps(packet))
            self.assertNotIn("我先查一下", json.dumps(packet, ensure_ascii=False))
            await backend.web_segment(
                w.token, w.reference, "reply:u", 1, dict(revision=1, state="stopped")
            )
            stopped = await until(
                lambda e: any(
                    r["seq"] == 0 and r["state"] == "stopped" for r in e.get("segments", [])
                )
            )
            self.assertNotIn("first_audio", stopped)
            # Force an old queued "ready" notification through AFTER stop.
            stale = dict(packet, inline_first="reply:u")
            stale.pop("first_audio")
            backend._runtime._deliver_push(backend._record(h).binding.call_id, stale)
            late = await until(
                lambda e: any(r["seq"] == 0 and r["state"] == "open" for r in e.get("segments", []))
            )
            self.assertNotIn("first_audio", late)
            provider.gate.set()
            await backend.drain()
            # A >48 KiB first sentence still works via the original HTTP reader.
            large = MP3 + bytes(MAX_PUSH_AUDIO_BYTES)

            async def synth(text):
                path = self.path / "large.mp3"
                path.write_bytes(large)
                return str(path)

            self.context.tts[event.unified_msg_origin].synthesizer = synth
            await backend.web_submit(w.token, w.reference, "large", "测试")
            await backend.drain()
            ready = await until(
                lambda e: any(
                    r["generation_id"] == "reply:large"
                    and r["seq"] == 1
                    and r["state"] == "succeeded"
                    for r in e.get("segments", [])
                )
            )
            self.assertNotIn("first_audio", ready)
            self.assertEqual(
                await backend.web_segment(w.token, w.reference, "reply:large", 1), large
            )
            await backend.web_end(w.token, w.reference)
            closed = await until(lambda e: e.get("closed"))
            self.assertNotIn("first_audio", closed)
        finally:
            await incoming.put({"type": "websocket.disconnect", "code": 1000})
            await asyncio.wait_for(task, 2)


class PCMOrderTests(unittest.IsolatedAsyncioTestCase):
    async def test_receipt_and_capability_preserve_dequeued_audio_and_latest_state(self):
        from contextlib import asynccontextmanager
        from types import SimpleNamespace
        from bot_call.push import websocket

        for kind in ("pcm-accepted", "pcm-capability"):
            with self.subTest(kind=kind):
                incoming, outgoing = asyncio.Queue(), asyncio.Queue()
                entered, release, taken = asyncio.Event(), asyncio.Event(), asyncio.Event()

                class AudioQueue(asyncio.Queue):
                    async def get(self):
                        value = await super().get()
                        if value["pcm"]["index"] == 1:
                            taken.set()
                        return value

                queue = asyncio.Queue(maxsize=1)
                queue.audio = AudioQueue(maxsize=128)
                queue.pcm_closed = False
                queue.pcm_owner = queue.pcm_last = None

                class Runtime:
                    @asynccontextmanager
                    async def watch(self, *args, **kwargs):
                        yield queue

                    async def web_state(self, *args):
                        if kind == "pcm-capability" and not release.is_set():
                            entered.set()
                            await release.wait()
                        return SimpleNamespace(
                            close_requested=False,
                            segments=[
                                dict(generation_id="g", seq=0, state="open"),
                                dict(
                                    generation_id="g",
                                    seq=1,
                                    state="running",
                                    playback_state="unknown",
                                ),
                            ],
                        )

                async def accept():
                    entered.set()
                    await release.wait()

                queue.pcm_owner = queue.pcm_last = SimpleNamespace(
                    gid="g", accept=accept, rejected=False
                )
                runtime = Runtime()
                app = SimpleNamespace(
                    _runtime=runtime,
                    _origin="https://test",
                    _headers=lambda *args: {"origin": "https://test"},
                    _session=lambda headers: ("synthetic", "0" * 64),
                )
                scope = dict(
                    scheme="wss",
                    path="/bot-call/ws",
                    subprotocols=["bot-call." + "0" * 64],
                )
                await incoming.put(dict(type="websocket.connect"))
                task = asyncio.create_task(websocket(app, scope, incoming.get, outgoing.put))
                try:
                    await asyncio.wait_for(outgoing.get(), 1)
                    value = dict(type=kind)
                    value.update(generation_id="g") if kind == "pcm-accepted" else value.update(
                        enabled=True
                    )
                    await incoming.put(dict(type="websocket.receive", text=json.dumps(value)))
                    await asyncio.wait_for(entered.wait(), 1)
                    # The consumer holds index 1 while authorization/claim awaits.
                    queue.audio.put_nowait(
                        dict(state="changed", pcm=dict(generation_id="g", index=1))
                    )
                    queue.put_nowait(dict(state="changed", revision=1))
                    await asyncio.wait_for(taken.wait(), 1)
                    queue.get_nowait() if not queue.empty() else None
                    queue.put_nowait(dict(state="changed", revision=2))
                    for index in range(2, 7):
                        queue.audio.put_nowait(
                            dict(state="changed", pcm=dict(generation_id="g", index=index))
                        )
                    release.set()
                    indexes, revisions = [], []
                    while len(indexes) < 6 or not revisions:
                        message = await asyncio.wait_for(outgoing.get(), 1)
                        self.assertEqual(message["type"], "websocket.send")
                        event = json.loads(message["text"])
                        if "pcm" in event:
                            indexes.append(event["pcm"]["index"])
                        elif "revision" in event:
                            revisions.append(event["revision"])
                    self.assertEqual(indexes, list(range(1, 7)))
                    self.assertEqual(revisions, [2], "newest coalesced state must win")
                finally:
                    release.set()
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
