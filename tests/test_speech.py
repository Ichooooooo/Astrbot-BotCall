"""Native TTS borrows output files and owns only local conversion copies."""

import asyncio
from pathlib import Path
from types import SimpleNamespace as NS

from bot_call.astrbot_tts import AstrBotTTSAdapter, SpeechPolicy
from bot_call.speech import SpeechUnavailable, MAX_SPEECH_BYTES, segment_audio_mime
from test_astrbot_context import binding
from test_backend import NativeCase, WAV, TEXT, MP3


from contextlib import closing
from dataclasses import replace
from tempfile import TemporaryDirectory
from unittest.mock import patch
import json
import tempfile
import threading
from bot_call.access import InvitationSecret, AccessError
from bot_call.runtime import CallRuntime, RuntimeAudio, RuntimeCallbacks, RuntimeOperationError
from bot_call.store import Store
from bot_call.web import CallWebApp, COOKIE
from bot_call.publication import Outcome
from test_review_work import freeze, prepare


class SpeechTests(NativeCase):
    async def test_sentence_mp3_conversion_native_passthrough_and_bounded_mime(self):
        event, _ = self.environment()
        target = binding("speech", "qq-A", "A")
        host = self.host()
        tts = self.context.tts[event.unified_msg_origin]
        conversions = []

        async def convert(source, *, output_format, output_path):
            conversions.append((output_format, Path(source).read_bytes()))
            Path(output_path).write_bytes(MP3)
            return output_path

        adapter = AstrBotTTSAdapter(
            self.context,
            SpeechPolicy(target, tts.meta().id, tts.get_model()),
            converter=self.converter,
            segment_converter=convert,
            privacy=host.privacy,
        )
        turn = NS(call_id=target.call_id, role="assistant", body=TEXT, segment_seq=1)

        async def yes():
            return True

        self.assertEqual(await adapter.synthesize(target, turn, before_invoke=yes), MP3)
        self.assertEqual(conversions, [("mp3", WAV)])
        borrowed = self.path / "misleading-native.wav"
        tagged = b"ID3\x04\x00\x00\x00\x00\x00\x00" + MP3
        borrowed.write_bytes(tagged)

        async def native(text):
            return str(borrowed)

        tts.synthesizer = native
        self.assertEqual(await adapter.synthesize(target, turn, before_invoke=yes), tagged)
        self.assertEqual(len(conversions), 1)
        self.assertEqual(borrowed.read_bytes(), tagged)
        self.assertEqual(segment_audio_mime(tagged), b"audio/mpeg")
        self.assertEqual(segment_audio_mime(WAV), b"audio/wav")
        for bad in (b"ID3" + bytes(21), b"not audio" * 4, b"RIFF" + bytes(40), MP3[:10]):
            with self.assertRaises(SpeechUnavailable):
                segment_audio_mime(bad)
        self.assertFalse(adapter._tasks)

    async def test_adapter_binding_cancel_and_cleanup(self):
        event, provider = self.environment()
        target = binding("speech", "qq-A", "A")
        host = self.host()
        tts = self.context.tts[event.unified_msg_origin]
        adapter = AstrBotTTSAdapter(
            self.context,
            SpeechPolicy(target, tts.meta().id, tts.get_model()),
            converter=self.converter,
            privacy=host.privacy,
        )
        turn = NS(call_id=target.call_id, role="assistant", body=TEXT)

        async def yes():
            return True

        self.assertEqual(await adapter.synthesize(target, turn, before_invoke=yes), WAV)
        borrowed = tts.paths[-1]
        self.assertTrue(borrowed.exists())
        self.assertTrue(all(not Path(source).parent.exists() for source, _, _ in self.conversions))

        async def no():
            return False

        before = len(tts.texts)
        with self.assertRaises(SpeechUnavailable):
            await adapter.synthesize(target, turn, before_invoke=no)
        self.assertEqual(len(tts.texts), before)
        entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.gates.append(release)

        async def stubborn(text):
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
            return str(borrowed)

        tts.synthesizer = stubborn
        task = asyncio.create_task(adapter.synthesize(target, turn, before_invoke=yes))
        await entered.wait()
        task.cancel()
        await cancelled.wait()
        self.assertFalse(task.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(borrowed.exists())
        self.assertFalse(adapter._tasks)
        oversized = self.path / "oversized-native.wav"
        with oversized.open("wb") as stream:
            stream.truncate(MAX_SPEECH_BYTES + 1)

        async def large(text):
            return str(oversized)

        tts.synthesizer = large
        with self.assertRaises(RuntimeError):
            await adapter.synthesize(target, turn, before_invoke=yes)
        self.assertTrue(oversized.exists())
        bad = self.path / "bad-native.wav"
        bad.write_bytes(b"not-a-wav")

        async def broken_conversion(source, *, output_format, output_path):
            Path(output_path).write_bytes(b"invalid")
            return output_path

        adapter.converter = broken_conversion

        async def bad_output(text):
            return str(bad)

        tts.synthesizer = bad_output
        with self.assertRaises(RuntimeError):
            await adapter.synthesize(target, turn, before_invoke=yes)
        self.assertTrue(bad.exists())
        self.assertTrue(borrowed.exists())

    async def test_durable_media_isolation_restart_and_close_guard(self):
        event, _ = self.environment()
        provider = self.context.tts[event.unified_msg_origin]
        a = binding("A", "qq-A", "A")
        b = binding("B", "qq-B", "B")
        host = self.host()
        parent = NS(calls=0, mode="ok")

        async def native_audio(text):
            parent.calls += 1
            if parent.mode == "fail":
                raise RuntimeError("SYNTHETIC_PRIVATE_NEVER_EXPORT")
            path = self.path / ("borrowed-" + str(parent.calls) + ".wav")
            path.write_bytes(WAV)
            return str(path)

        provider.synthesizer = native_audio
        adapter = AstrBotTTSAdapter(
            self.context,
            SpeechPolicy(a, provider.meta().id, provider.get_model()),
            converter=self.converter,
            privacy=host.privacy,
        )

        async def reply(bound, turn):
            return "saved text " + turn.body

        async def review(*args):
            return "review"

        async def send(*args):
            return Outcome.SUBMITTED

        async def readable(*args):
            return Outcome.CONFIRMED

        directory_entered = threading.Event()
        directory_release = threading.Event()
        original_mkdtemp = tempfile.mkdtemp

        def preparing_directory(**kwargs):
            directory_entered.set()
            if not directory_release.wait(5):
                raise RuntimeError("test directory gate timeout")
            return original_mkdtemp(**kwargs)

        async def synthesize(bound, turn, guard):
            return await adapter.synthesize(
                bound, turn, before_invoke=lambda: guard(bound, turn.material_id)
            )

        callbacks = RuntimeCallbacks(reply, review, send, readable, synthesize)

        async def wait_until(predicate):
            for _ in range(300):
                if predicate():
                    return
                await asyncio.sleep(0.001)
            self.fail("bounded synthetic wait gate did not settle")

        async def check_waits(directory):
            from bot_call.backend import CallBackend

            loop = asyncio.get_running_loop()
            previous = loop.get_exception_handler()
            unhandled = []
            loop.set_exception_handler(
                lambda loop, context: unhandled.append(context.get("message"))
            )
            self.addCleanup(loop.set_exception_handler, previous)
            entered, release, cancelled = [], asyncio.Event(), asyncio.Event()

            async def gated(bound, turn, guard):
                self.assertTrue(await guard(bound, turn.material_id))
                entered.append(turn.material_id)
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    await release.wait()  # Actual owner suppresses cancellation.
                return WAV

            current = CallRuntime(
                Path(directory) / "waits.db", replace(callbacks, synthesize=gated)
            )
            await current.start()
            http = []
            closing_task = None
            try:
                grants = []
                for bound in (b, a):
                    await current.create_call(bound)
                    secret = InvitationSecret.create()
                    await current.issue_web_invitation(bound.call_id, secret, session_ttl=3600)
                    grants.append(await current.exchange_web_invitation(secret.value))
                    if bound == b:
                        await current.web_end(grants[-1].token, grants[-1].reference)
                other, grant = grants
                app = CallWebApp(current, origin="https://calls.example.test")
                omitted = object()

                async def req(generation, wait=omitted, *, source=grant, fields=None):
                    data = {"generation_id": generation}
                    if wait is not omitted:
                        data["wait_ms"] = wait
                    if fields:
                        data.update(fields)
                    events = []
                    scope = dict(
                        type="http",
                        scheme="https",
                        method="POST",
                        path="/bot-call/speech",
                        query_string=b"",
                        headers=[
                            (b"host", b"calls.example.test"),
                            (b"origin", b"https://calls.example.test"),
                            (b"content-type", b"application/json"),
                            (b"cookie", (COOKIE + "=" + source.token).encode()),
                            (b"x-bot-call-reference", source.reference.encode()),
                        ],
                    )

                    async def receive():
                        return {
                            "type": "http.request",
                            "body": json.dumps(data).encode(),
                            "more_body": False,
                        }

                    async def send_event(event):
                        events.append(event)

                    await app(scope, receive, send_event)
                    return events[0]["status"], json.loads(events[1]["body"])

                for number in range(1, 6):
                    await current.submit_utterance("A", str(number), "synthetic wait text")
                await current.drain()
                for invalid in (True, False, -1, 8001, 1.0, None, "8000"):
                    self.assertEqual((await req("reply:1", invalid))[0], 400)
                self.assertEqual((await req("reply:1", fields={"extra": 1}))[0], 400)
                with self.assertRaises(AccessError):
                    await current.web_speech(grant.token, grant.reference, "reply:1", True)
                self.assertEqual(entered, [])
                self.assertFalse(current._speech_tasks)
                first = asyncio.create_task(req("reply:1", 8000))
                http.append(first)
                await wait_until(lambda: len(current._speech_waiters) == 1 and len(entered) == 1)
                duplicate = await asyncio.wait_for(req("reply:1", 8000), 1)
                self.assertEqual(duplicate, (200, {"generation_id": "reply:1", "state": "running"}))
                self.assertEqual(len(entered), 1)
                # Deadline returns running, not failure/cancellation of actual native callback.
                self.assertEqual((await req("reply:2", 1))[1]["state"], "running")
                await wait_until(lambda: len(entered) == 2)
                self.assertEqual(len(current._speech_waiters), 1)
                for number in (2, 3, 4):
                    task = asyncio.create_task(req("reply:" + str(number), 8000))
                    http.append(task)
                await wait_until(lambda: len(current._speech_waiters) == 4 and len(entered) == 4)
                overflow = await asyncio.wait_for(req("reply:5", 8000), 1)
                self.assertEqual(overflow[1]["state"], "running")
                await wait_until(lambda: len(entered) == 5)
                self.assertEqual(len(current._speech_tasks), 4)
                self.assertEqual(len(current._speech_waiters), 4)
                self.assertEqual((await req("reply:5"))[1]["state"], "running")
                self.assertEqual(
                    len(entered), 5, "index capacity changed claim/native callback behavior"
                )
                # HTTP cancellation drops only that result; shielded accepted
                # worker wait and all actual synthesis owners remain alive.
                first.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await first
                self.assertEqual(len(current._speech_waiters), 4)
                self.assertFalse(cancelled.is_set())
                await asyncio.wait_for(current.web_state(grant.token, grant.reference), 1)
                await asyncio.wait_for(current.web_heartbeat(grant.token, grant.reference), 1)
                await asyncio.wait_for(
                    current.web_playback(grant.token, grant.reference, "reply:1", 1, "unknown"), 1
                )
                self.assertEqual((await req("reply:1", 8000, source=other))[0], 409)
                # Reauthentication after completion sees expiry even though all
                # requests were valid at admission. No claimed job is replayed.
                with patch("bot_call.store.time", return_value=grant.expires_at + 1):
                    release.set()
                    replies = await asyncio.gather(*http[1:])
                    self.assertEqual([r[0] for r in replies], [401, 401, 401])
                    await current.drain()
                await wait_until(lambda: not current._speech_tasks and not current._speech_waiters)
                self.assertEqual((await req("reply:1", 8000))[1]["state"], "succeeded")
                self.assertEqual(len(entered), 5)
                self.assertFalse(cancelled.is_set())
                # Close while another wait is live: wrapper can be woken and
                # reauth rejects media, while the real owner is still retained.
                release.clear()
                await current.submit_utterance("A", "6", "synthetic closing text")
                await current.drain()
                tail = asyncio.create_task(req("reply:6", 8000))
                http.append(tail)
                await wait_until(lambda: bool(current._speech_waiters) and len(entered) == 6)
                await asyncio.wait_for(current.web_end(grant.token, grant.reference), 1)
                await current.stop_speech_waits()
                self.assertEqual((await asyncio.wait_for(tail, 1))[0], 409)
                self.assertFalse(cancelled.is_set())
                self.assertTrue(current._speech_tasks)
                self.assertEqual((await req("reply:6", 8000))[0], 409)
                release.set()
                await current.drain()
                with self.assertRaises(AccessError):
                    await current.web_speech_audio(grant.token, grant.reference, "reply:6")
            finally:
                release.set()
                await asyncio.gather(*http, return_exceptions=True)
                await current.shutdown(timeout=None)
            with closing(Store(Path(directory) / "waits.db")) as store:
                self.assertEqual(store.get_speech("A", "reply:6").state, "cancelled")
                # Pure status read emits SELECT/BEGIN only, with no write lock,
                # no WAV read and unchanged database change counter.
                trace = []
                changes = store._db.total_changes
                store._db.set_trace_callback(trace.append)
                with self.assertRaises(AccessError):
                    store.speech_status("A", "reply:1", (grant.token, grant.reference))
                self.assertEqual(store._db.total_changes, changes)
                self.assertEqual(trace[0], "BEGIN")
                self.assertFalse(any("BEGIN IMMEDIATE" in q for q in trace))
            # A fresh runtime's backend-close path must wake long HTTP waits
            # BEFORE gathering them, then retain the cancellation-suppressing native callback.
            release.clear()
            cancelled.clear()
            current = CallRuntime(
                Path(directory) / "close-waits.db", replace(callbacks, synthesize=gated)
            )
            await current.start()
            await current.create_call(a)
            secret = InvitationSecret.create()
            await current.issue_web_invitation("A", secret, session_ttl=3600)
            grant = await current.exchange_web_invitation(secret.value)
            await current.submit_utterance("A", "close", "synthetic")
            await current.drain()
            waiting = asyncio.create_task(
                current.web_speech(grant.token, grant.reference, "reply:close", 8000)
            )
            await wait_until(lambda: bool(current._speech_waiters))
            backend = CallBackend.__new__(CallBackend)
            backend._start_task = None
            backend._preparations = {}
            backend._recoveries = {}
            backend._commits = set()
            backend._web_tasks = {waiting}
            backend._warmup_tasks = set()
            backend._runtime = current
            closing_task = asyncio.create_task(backend._finish_close())
            try:
                result = await asyncio.wait_for(asyncio.shield(waiting), 1)
                self.assertEqual(result["state"], "running")
                await asyncio.wait_for(cancelled.wait(), 1)
                self.assertFalse(closing_task.done())
                self.assertTrue(current._thread.is_alive())
                self.assertFalse(current._speech_waiters)
                self.assertTrue(current._core_tasks)
                release.set()
                report = await closing_task
                self.assertTrue(report.stopped)
                self.assertFalse(current._thread.is_alive())
                self.assertFalse(current._core_tasks)
                self.assertFalse(current._speech_tasks)
            finally:
                release.set()
                await asyncio.gather(waiting, return_exceptions=True)
                await current.shutdown(timeout=None)
                if closing_task is not None:
                    await asyncio.gather(closing_task, return_exceptions=True)
            self.assertEqual(unhandled, [])

        async def check_state_waits(directory):
            from bot_call.backend import CallBackend

            audio_entered, audio_release = asyncio.Event(), asyncio.Event()
            reply_release, cancelled = asyncio.Event(), asyncio.Event()
            calls = []

            async def transcribe(bound, job):
                audio_entered.set()
                await audio_release.wait()
                return "synthetic transcription"

            async def pending_reply(bound, turn):
                calls.append(turn.material_id)
                try:
                    await reply_release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    await reply_release.wait()  # Retain actual cleanup ownership.
                return "synthetic saved reply"

            current = CallRuntime(
                Path(directory) / "state-waits.db",
                replace(callbacks, reply=pending_reply),
                audio=RuntimeAudio(transcribe),
            )
            await current.start()
            requests = []
            close_task = None
            try:
                grants = []
                for bound in (b, a):
                    await current.create_call(bound)
                    secret = InvitationSecret.create()
                    await current.issue_web_invitation(bound.call_id, secret, session_ttl=3600)
                    grants.append(await current.exchange_web_invitation(secret.value))
                    if bound == b:
                        await current.web_end(grants[-1].token, grants[-1].reference)
                other, grant = grants
                app = CallWebApp(current, origin="https://calls.example.test")

                async def req(headers=(), source=grant):
                    events = []
                    scope = dict(
                        type="http",
                        scheme="https",
                        method="GET",
                        path="/bot-call/state",
                        query_string=b"",
                        headers=[
                            (b"host", b"calls.example.test"),
                            (b"cookie", (COOKIE + "=" + source.token).encode()),
                            (b"x-bot-call-reference", source.reference.encode()),
                            *headers,
                        ],
                    )

                    async def receive():
                        return {"type": "http.request", "body": b"", "more_body": False}

                    async def send_event(event):
                        events.append(event)

                    await app(scope, receive, send_event)
                    return events[0]["status"], json.loads(events[1]["body"])

                def headers(target, wait="8000"):
                    return [
                        (b"x-bot-call-wait-ms", wait.encode()),
                        (b"x-bot-call-utterance", target.encode()),
                    ]

                await current.web_audio(
                    grant.token, grant.reference, "voice", b"synthetic audio", "audio/wav"
                )
                await audio_entered.wait()
                ordinary = await req()
                self.assertEqual(await req(headers("voice", "0")), ordinary)
                for invalid in ("-1", "8001", "8.0", " 8", "01", "true", ""):
                    self.assertEqual((await req(headers("voice", invalid)))[0], 400)
                for invalid in (
                    [headers("voice")[0]],
                    [headers("voice")[1]],
                    headers("voice") + [headers("voice")[0]],
                    headers("voice") + [headers("voice")[1]],
                    headers("bad target"),
                ):
                    self.assertEqual((await req(invalid))[0], 400)
                for invalid in (True, -1, 8001, 1.0):
                    with self.assertRaises(AccessError):
                        await current.web_state(grant.token, grant.reference, invalid, "voice")
                self.assertEqual((await req(headers("voice"), other))[0], 409)
                self.assertEqual((await req(headers("not-owned")))[0], 409)
                self.assertEqual(calls, [])
                waiting = asyncio.create_task(req(headers("voice")))
                requests.append(waiting)
                await wait_until(lambda: len(current._state_waiters) == 1)
                self.assertEqual(
                    (await asyncio.wait_for(req(headers("voice")), 1))[1]["audio"][0]["state"],
                    "running",
                )
                await asyncio.wait_for(current.web_heartbeat(grant.token, grant.reference), 1)
                audio_release.set()
                self.assertEqual(
                    (await asyncio.wait_for(waiting, 1))[1]["audio"][0]["state"], "succeeded"
                )
                await wait_until(lambda: calls == ["voice"])
                # STT completion is not reply completion: a fresh same-round
                # wrapper must wait for the separate durable reply commit.
                waiting = asyncio.create_task(req(headers("voice")))
                requests.append(waiting)
                await wait_until(lambda: len(current._state_waiters) == 1)
                self.assertFalse(waiting.done())
                short = await req(headers("voice", "1"))  # Duplicate capacity: immediate.
                self.assertEqual(short[1]["replies"][0]["state"], "running")
                for n in range(1, 5):
                    await current.submit_utterance("A", str(n), "synthetic text")
                await wait_until(lambda: len(calls) == 5)
                for n in range(1, 4):
                    task = asyncio.create_task(req(headers(str(n))))
                    requests.append(task)
                await wait_until(lambda: len(current._state_waiters) == 4)
                self.assertEqual((await asyncio.wait_for(req(headers("4")), 1))[0], 200)
                self.assertEqual(len(calls), 5)  # Wait quota did not limit reply claims.
                waiting.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await waiting
                self.assertEqual(len(current._state_waiters), 4)
                self.assertFalse(cancelled.is_set())
                # Reauthentication applies to the result, including abandoned HTTP.
                with patch("bot_call.store.time", return_value=grant.expires_at + 1):
                    reply_release.set()
                    for task in requests[2:]:
                        self.assertEqual((await asyncio.wait_for(task, 1))[0], 401)
                    await current.drain()
                await wait_until(lambda: not current._state_waiters)
                self.assertEqual(
                    (await req(headers("voice")))[1]["replies"][0]["state"], "succeeded"
                )
                await current.web_playback(
                    grant.token, grant.reference, "reply:voice", 1, "unknown"
                )
                # A commit between the initial predicate and registration must
                # be seen by the post-registration durable reread, without notify.
                reply_release.clear()
                await current.submit_utterance("A", "race", "synthetic text")
                await wait_until(lambda: "race" in calls)
                original = current._state_phase
                injected = []

                def commit_between(key):
                    phase = original(key)
                    if key == ("A", "race") and not injected:
                        injected.append(True)
                        current._store.complete_generation(
                            "A", "reply:race", "synthetic saved reply"
                        )
                    return phase

                with patch.object(current, "_state_phase", commit_between):
                    self.assertEqual((await asyncio.wait_for(req(headers("race")), 1))[0], 200)
                self.assertEqual(injected, [True])
                self.assertFalse(current._state_waiters)
                # An actually admitted short timeout retains its real callback.
                await current.submit_utterance("A", "short", "synthetic text")
                await wait_until(lambda: "short" in calls)
                self.assertEqual((await req(headers("short", "1")))[0], 200)
                self.assertFalse(cancelled.is_set())
                self.assertFalse(reply_release.is_set())
                # Hangup commits and wakes state immediately, without cancelling
                # the still-owned reply callback or replacing its result.
                waiting = asyncio.create_task(req(headers("short")))
                requests.append(waiting)
                await wait_until(lambda: bool(current._state_waiters))
                await asyncio.wait_for(current.web_end(grant.token, grant.reference), 1)
                self.assertTrue((await asyncio.wait_for(waiting, 1))[1]["close_requested"])
                self.assertFalse(cancelled.is_set())
                reply_release.set()
                await current.drain()
                # Backend stop wakes state before web gather, then truly retains
                # a cancellation-suppressing native callback until its cleanup gate exits.
                c = replace(a, call_id="state-close")
                await current.create_call(c)
                secret = InvitationSecret.create()
                await current.issue_web_invitation(c.call_id, secret, session_ttl=3600)
                grant = await current.exchange_web_invitation(secret.value)
                reply_release.clear()
                await current.submit_utterance(c.call_id, "close", "synthetic text")
                await wait_until(lambda: "close" in calls)
                waiting = asyncio.create_task(
                    current.web_state(grant.token, grant.reference, 8000, "close")
                )
                requests.append(waiting)
                await wait_until(lambda: bool(current._state_waiters))
                backend = CallBackend.__new__(CallBackend)
                backend._start_task = None
                backend._preparations = {}
                backend._recoveries = {}
                backend._commits = set()
                backend._web_tasks = {waiting}
                backend._warmup_tasks = set()
                backend._runtime = current
                close_task = asyncio.create_task(backend._finish_close())
                self.assertFalse((await asyncio.wait_for(asyncio.shield(waiting), 1)).ended)
                await asyncio.wait_for(cancelled.wait(), 1)
                self.assertFalse(close_task.done())
                self.assertTrue(current._thread.is_alive())
                self.assertTrue(current._core_tasks)
                self.assertFalse(current._state_waiters)
                reply_release.set()
                self.assertTrue((await close_task).stopped)
                self.assertFalse(current._thread.is_alive())
                self.assertFalse(current._core_tasks)
            finally:
                audio_release.set()
                reply_release.set()
                await current.stop_state_waits()
                await asyncio.gather(*requests, return_exceptions=True)
                await current.shutdown(timeout=None)
                if close_task is not None:
                    await asyncio.gather(close_task, return_exceptions=True)

        with TemporaryDirectory() as directory:
            await check_state_waits(directory)
            await check_waits(directory)
            # A recovery write failure must close the actual connection on its
            # storage thread, exit the worker, and leave existing bytes intact.
            startup_path = Path(directory) / "startup-fault.db"
            with closing(Store(startup_path)) as store:
                store.create_call(a)
                store.append_user("A", "saved", "preserved startup text", source="text")
            original_bytes = startup_path.read_bytes()
            closed_on = []
            owners = []

            class RecoveryFailureStore(Store):
                def __init__(inner, path):
                    owners.append(threading.get_ident())
                    super().__init__(path)

                def recover_speech(inner):
                    raise OSError("synthetic recovery write failure")

                def close(inner):
                    closed_on.append(threading.get_ident())
                    super().close()

            failed = CallRuntime(startup_path, callbacks)
            with patch("bot_call.runtime.Store", RecoveryFailureStore):
                with self.assertRaises(RuntimeOperationError):
                    await failed.start()
            self.assertTrue(failed._exited.done())
            self.assertFalse(failed._thread.is_alive())
            self.assertEqual(closed_on, owners)
            self.assertEqual(len(closed_on), 1)
            self.assertNotEqual(closed_on[0], threading.get_ident())
            self.assertEqual(parent.calls, 0)
            self.assertFalse(failed._core_tasks)
            self.assertEqual(startup_path.read_bytes(), original_bytes)
            path = Path(directory) / "speech.db"
            # Construct a nonempty schema9 fixture, including an uncertain work marker.
            with closing(Store(path)) as store:
                store.create_call(a)
                store.create_call(b)
                store.append_user("A", "old", "preserved original text", source="text")
                store.claim_generation("A", "reply:old", utterance_id="old", source="fake_reply")
                store.complete_generation("A", "reply:old", "preserved old bot")
                freeze(store, "legacy-uncertain")
                pending = prepare(store, "legacy-uncertain")
                node, won = store.claim_review_work_node(
                    "legacy-uncertain", pending.frontier[0].source_key
                )
                assert won
                store.finish_review_work_node(
                    "legacy-uncertain", node.source_key, state="uncertain"
                )
                original_work = store.get_review_work("legacy-uncertain")
                # Shape the existing synthetic schema9 fixture, including its
                # pre-query barrier; this is not a real historical Store binary.
                # Only this newly created temporary DB is shaped.
                saved_turns = store.get_transcript("A")
                store._db.execute("DROP TRIGGER review_query_barrier")
                for table in (
                    "turn_timings",
                    "review_agent_notes",
                    "agent_runs",
                    "review_query_notes",
                    "query_attempts",
                    "query_policies",
                    "review_audio_notes",
                    "audio_resolutions",
                    "audio_missing_policies",
                    "speech_jobs",
                ):
                    self.assertEqual(
                        store._db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0
                    )
                    store._db.execute(f"DROP TABLE {table}")
                store._db.execute("DROP TRIGGER review_audio_barrier")
                store._db.execute(
                    "CREATE TRIGGER review_audio_barrier BEFORE INSERT ON reviews "
                    "WHEN EXISTS(SELECT 1 FROM audio_jobs WHERE call_id=NEW.call_id AND state!='succeeded') "
                    "BEGIN SELECT RAISE(ABORT,'received audio is unresolved'); END"
                )
                from bot_call.retention import audio_progress_trigger
                from schema_fixture import remove_series_columns

                store._db.execute("DROP TRIGGER audio_progress")
                store._db.execute(audio_progress_trigger())

                remove_series_columns(store._db)
                from schema_fixture import remove_retry_schema

                remove_retry_schema(store._db)
                store._db.execute("PRAGMA user_version=9")
                self.assertEqual(store.get_transcript("A"), saved_turns)
                self.assertEqual(store.get_review_work("legacy-uncertain"), original_work)
            runtime = CallRuntime(path, callbacks)
            await runtime.start()
            try:
                # Accept this call only for NEW generation enrollment; old reply remains ineligible.
                await runtime.create_call(a)
                grants = []
                for item in (b, a):
                    secret = InvitationSecret.create()
                    await runtime.issue_web_invitation(item.call_id, secret, session_ttl=3600)
                    grants.append(await runtime.exchange_web_invitation(secret.value))
                    if item == b:
                        await runtime.web_end(grants[-1].token, grants[-1].reference)
                gb, ga = grants
                app = CallWebApp(runtime, origin="https://calls.example.test")

                async def request(
                    grant, route, generation, origin="https://calls.example.test", wait_ms=None
                ):
                    events = []
                    data = {"generation_id": generation}
                    if wait_ms is not None:
                        data["wait_ms"] = wait_ms
                    body = json.dumps(data).encode()
                    scope = dict(
                        type="http",
                        scheme="https",
                        method="POST",
                        path="/bot-call/" + route,
                        query_string=b"",
                        headers=[
                            (b"host", b"calls.example.test"),
                            (b"origin", origin.encode()),
                            (b"content-type", b"application/json"),
                            (b"cookie", (COOKIE + "=" + grant.token).encode()),
                            (b"x-bot-call-reference", grant.reference.encode()),
                        ],
                    )

                    async def receive():
                        return {"type": "http.request", "body": body, "more_body": False}

                    async def send_event(event):
                        events.append(event)

                    await app(scope, receive, send_event)
                    return events[0]["status"], events[1]["body"]

                self.assertEqual((await request(ga, "speech", "reply:old"))[0], 409)
                await runtime.submit_utterance("A", "one", "new text")
                await runtime.drain()
                first, second = await asyncio.gather(
                    request(ga, "speech", "reply:one", wait_ms=8000),
                    request(ga, "speech", "reply:one"),
                )
                self.assertEqual(first[0], 200)
                self.assertEqual(second[0], 200)
                self.assertEqual(json.loads(first[1])["state"], "succeeded")
                self.assertEqual(
                    (await request(ga, "speech-audio", "reply:one", wait_ms=1))[0], 400
                )
                await runtime.drain()
                self.assertEqual(parent.calls, 1)
                self.assertEqual(await request(ga, "speech-audio", "reply:one"), (200, WAV))
                self.assertEqual((await request(gb, "speech-audio", "reply:one"))[0], 409)
                self.assertEqual(
                    (
                        await request(
                            ga, "speech-audio", "reply:one", origin="https://other.invalid"
                        )
                    )[0],
                    403,
                )
                # Actual entered native callback error is uncertain, and duplicate requests do not retry it.
                parent.mode = "fail"
                await runtime.submit_utterance("A", "two", "failure")
                await runtime.drain()
                await request(ga, "speech", "reply:two")
                await runtime.drain()
                status, body = await request(ga, "speech", "reply:two")
                self.assertEqual(json.loads(body)["state"], "uncertain")
                self.assertEqual(parent.calls, 2)
                parent.mode = "ok"
                # Preparation is paused after claim. Persist hangup, then release the native path preparation.
                await runtime.submit_utterance("A", "three", "guard")
                await runtime.drain()
                with patch(
                    "bot_call.astrbot_tts.tempfile.mkdtemp", side_effect=preparing_directory
                ):
                    await request(ga, "speech", "reply:three")
                    self.assertTrue(await asyncio.to_thread(directory_entered.wait, 3))
                    await runtime.web_end(ga.token, ga.reference)
                    directory_release.set()
                    await runtime.drain()
                self.assertEqual(
                    parent.calls, 2, "hangup during preparation entered native callback"
                )
                self.assertEqual((await request(ga, "speech-audio", "reply:one"))[0], 409)
                self.assertEqual((await request(ga, "speech", "reply:one"))[0], 409)
                self.assertEqual(parent.calls, 2)
            finally:
                directory_release.set()
                await runtime.shutdown(timeout=None)
            with closing(Store(path)) as store:
                self.assertEqual(store._db.execute("PRAGMA user_version").fetchone()[0], 24)
                self.assertEqual(store.get_speech("A", "reply:three").state, "cancelled")
                self.assertEqual(store.get_review_work("legacy-uncertain"), original_work)
                self.assertEqual(store.get_transcript("A")[0].body, "preserved original text")
                # Simulate a crash after a durable running claim, without touching old review/work state.
                store._db.execute(
                    "UPDATE speech_jobs SET state='running' WHERE generation_id='reply:two'"
                )
            restart = CallRuntime(path, callbacks)
            await restart.start()
            try:
                await restart.resume_pending()
                await restart.drain()
            finally:
                await restart.shutdown(timeout=None)
            with closing(Store(path)) as store:
                self.assertEqual(store.get_speech("A", "reply:two").state, "uncertain")
                self.assertEqual(len(store.get_transcript("A")), 8)
            self.assertEqual(parent.calls, 2)
