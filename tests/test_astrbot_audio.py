"""Native STT output, file and cancellation ownership; no SDK fixture."""

import asyncio
import hashlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace as NS

from bot_call.audio import AudioJob, AudioNotApplied, NoTextResult
from bot_call.astrbot_audio import AudioRequestPolicy, AstrBotAudioAdapter, AudioResponseUncertain
from bot_call.native_provider import NATIVE_CONTRACT
from test_astrbot_context import binding
from test_backend import NativeCase, TEXT, WAV, STTProvider


from copy import deepcopy
from unittest.mock import patch
import json
import subprocess
import sys
import threading
from bot_call.runtime import CallRuntime, RuntimeCallbacks, RuntimeAudio
from bot_call.store import Store
from bot_call.publication import Outcome
import test_web_access as web_fixture
from test_backend import FINAL


class AudioTests(NativeCase):
    async def test_dedicated_stt_transcribes_and_disabled_provider_fails_without_fallback(self):
        event, _ = self.environment()
        other, _ = self.environment("qq-B", "B")
        source = self.context.stt[event.unified_msg_origin]
        dedicated = self.context.stt[other.unified_msg_origin]
        settings = self.settings()
        self.settings = lambda: dict(settings, call_stt_provider_id=dedicated.meta().id)
        backend, host = await self.backend()
        handle, web = await self.enter(backend, event)
        self.assertEqual(
            (await host.diagnostic_source(event.unified_msg_origin))["stt"], "dedicated"
        )
        await backend.web_audio(web.token, web.reference, "u1", b"synthetic", "audio/webm")
        await backend.drain()
        self.assertEqual((await backend.read_state(handle)).audio_jobs[0].state, "succeeded")
        self.assertEqual(len(dedicated.paths), 1)
        self.assertEqual(source.paths, [])
        dedicated.provider_config["enable"] = False
        await backend.web_audio(web.token, web.reference, "u2", b"synthetic2", "audio/webm")
        await backend.drain()
        self.assertNotEqual((await backend.read_state(handle)).audio_jobs[-1].state, "succeeded")
        self.assertEqual(len(dedicated.paths), 1)
        self.assertEqual(source.paths, [])

    async def test_stt_converter_is_separate_from_tts_and_reaches_public_provider(self):
        event, _ = self.environment()
        original_tools, original_converter = self.tools, self.converter
        conversions = []

        async def stt(source, **kwargs):
            conversions.append("stt")
            return await original_converter(source, **kwargs)

        async def generic(source, **kwargs):
            conversions.append("tts")
            return await original_converter(source, **kwargs)

        self.tools = lambda: replace(original_tools(), converter=generic, stt_converter=stt)
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        await backend.web_audio(web.token, web.reference, "u", b"synthetic", "audio/webm")
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual(state.audio_jobs[0].state, "succeeded")
        self.assertEqual(len(self.context.stt[event.unified_msg_origin].paths), 1)
        await backend.web_speech(web.token, web.reference, state.generations[0].generation_id)
        await backend.drain()
        self.assertEqual(conversions, ["stt", "tts"])
        self.assertTrue(
            all(not Path(p).exists() for p in self.context.stt[event.unified_msg_origin].paths)
        )

    async def test_preflight_unknown_and_actual_conversion_sdk_cancel_ownership(self):
        event, provider = self.environment()
        target = binding("audio", "qq-A", "A")
        host = self.host()
        stt = self.context.stt[event.unified_msg_origin]
        policy = AudioRequestPolicy(
            target, stt.meta().id, stt.get_model(), 128, 1024, NATIVE_CONTRACT
        )
        adapter = await AstrBotAudioAdapter.bind(
            self.context, policy, converter=self.converter, privacy=host.privacy
        )
        data = b"complete-synthetic-container"
        job = AudioJob(
            "audio",
            "u",
            1,
            "audio/webm",
            hashlib.sha256(data).hexdigest(),
            len(data),
            1.0,
            "running",
            data,
            None,
            None,
            None,
        )
        self.assertEqual(await adapter.transcribe(target, job), TEXT)
        self.assertFalse(Path(stt.paths[-1]).exists())

        async def empty(path):
            return "  "

        stt.transcriber = empty
        self.assertIsInstance(await adapter.transcribe(target, job), NoTextResult)
        for value in (None, 42, NS(text=TEXT), "\ud800"):

            async def invalid(path):
                return value

            stt.transcriber = invalid
            with self.assertRaises(AudioResponseUncertain):
                await adapter.transcribe(target, job)
        before = len(stt.paths)
        self.context.stt[event.unified_msg_origin] = STTProvider("changed")
        with self.assertRaises(AudioNotApplied):
            await adapter.transcribe(target, job)
        self.assertEqual(len(stt.paths), before)
        self.context.stt[event.unified_msg_origin] = stt
        entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.gates.append(release)

        async def stubborn(path):
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
            return TEXT

        stt.transcriber = stubborn
        task = asyncio.create_task(adapter.transcribe(target, job))
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.wait_for(cancelled.wait(), 2)
        self.assertFalse(task.done())
        self.assertTrue(Path(stt.paths[-1]).exists())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(Path(stt.paths[-1]).exists())
        self.assertFalse(adapter._tasks)
        # Cancellation during conversion does not cancel the converter's owner.
        preparing, finish = asyncio.Event(), asyncio.Event()
        self.gates.append(finish)

        async def convert(source, **kwargs):
            preparing.set()
            await finish.wait()
            return await self.converter(source, **kwargs)

        adapter._converter = convert
        before = len(stt.paths)
        task = asyncio.create_task(adapter.transcribe(target, job))
        await preparing.wait()
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        finish.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(stt.paths), before)
        with self.assertRaises(AudioNotApplied):
            await AstrBotAudioAdapter.bind(
                self.context,
                replace(policy, selection="frozen"),
                converter=self.converter,
                privacy=host.privacy,
            )

    async def test_original_umo_borrowed_parent_and_runtime_persistent_close(self):
        adapters = {}
        targets = []
        providers = []
        host = self.host()
        for cid, platform in [("A", "qq-A"), ("B", "qq-B")]:
            event, _ = self.environment(platform, cid)
            target = binding(cid, platform, cid)
            targets.append(target)
            stt = self.context.stt[event.unified_msg_origin]
            providers.append(stt)
            if cid == "B":

                async def blank(path):
                    return " \t\n"

                stt.transcriber = blank
            policy = AudioRequestPolicy(
                target, stt.meta().id, stt.get_model(), 128, 1024, NATIVE_CONTRACT
            )
            adapters[cid] = await AstrBotAudioAdapter.bind(
                self.context, policy, converter=self.converter, privacy=host.privacy
            )
        configs = deepcopy([p.provider_config for p in providers])
        replies = []
        reviews = []
        contexts = []

        async def reply(bound, turn):
            replies.append(bound.call_id)
            return "unused after close"

        async def review(bound, turns, prompt):
            reviews.append(bound)
            return FINAL

        async def page(*args):
            return Outcome.CONFIRMED

        async def readable(bound, key, body):
            contexts.append((bound.call_id, body))
            return Outcome.CONFIRMED

        async def transcribe(bound, job):
            return await adapters[bound.call_id].transcribe(bound, job)

        path = self.path / "persistent-audio.db"
        with Store(path) as seed:
            for target in targets:
                seed.create_call(target)
                seed.receive_audio(
                    target.call_id, "one", b"original-" + target.call_id.encode(), "audio/wav"
                )
        web_fixture.ObservedStore.instances = []
        with patch("bot_call.runtime.Store", web_fixture.ObservedStore):
            runtime = CallRuntime(
                path,
                RuntimeCallbacks(reply, review, page, readable),
                audio=RuntimeAudio(transcribe, max_audio_bytes=128),
            )
            await runtime.start()
            try:
                self.assertEqual([len(p.paths) for p in providers], [0, 0])
                for target in targets:
                    await runtime.end_call(target.call_id)
                await runtime.drain()
                a = await runtime.read_state("A")
                b = await runtime.read_state("B")
                self.assertEqual([t.body for t in a.turns], [TEXT])
                self.assertTrue(a.publication.complete)
                self.assertEqual(a.review.body, FINAL)
                self.assertEqual(a.audio_jobs[0].state, "succeeded")
                self.assertIsNone(a.audio_jobs[0].data)
                self.assertEqual([v["body"] for v in json.loads(a.review.input_json)], [TEXT])
                self.assertEqual(b.turns, ())
                self.assertIsNone(b.review)
                self.assertIsNone(b.publication)
                self.assertEqual(b.generations, ())
                self.assertIsNotNone(b.close_request)
                self.assertEqual(
                    (b.audio_jobs[0].state, b.audio_jobs[0].error_code),
                    ("failed", "empty_transcription"),
                )
                self.assertEqual(b.audio_jobs[0].data, b"original-B")
                self.assertEqual(replies, [])
                self.assertEqual(len(reviews), 1)
                self.assertEqual(contexts, [("A", FINAL)])
                await runtime.resume_pending()
                await runtime.drain()
                self.assertEqual([len(p.paths) for p in providers], [1, 1])
            finally:
                await runtime.shutdown(timeout=None)
            for store in web_fixture.ObservedStore.instances:
                self.assertEqual(store.threads, {store.owner})
                self.assertNotEqual(store.owner, threading.get_ident())
                self.assertEqual(store.closed_thread, store.owner)
        self.assertEqual([p.provider_config for p in providers], configs)
        self.assertTrue(all(not Path(path).exists() for p in providers for path in p.paths))
        self.assertEqual(len(self.conversions), 2)
        self.assertEqual({v[2] for v in self.conversions}, {b"original-A", b"original-B"})
        script = """
import asyncio,sys,sqlite3,socket,threading,os
def deny(*a,**k):raise AssertionError('import side effect')
sqlite3.connect=socket.socket=threading.Thread.start=os.mkdir=deny
import bot_call.astrbot_audio
assert 'openai' not in sys.modules
assert not any(n=='astrbot' or n.startswith('astrbot.') for n in sys.modules)
"""
        imported = subprocess.run(
            [sys.executable, "-B", "-c", script], capture_output=True, text=True
        )
        self.assertEqual(imported.returncode, 0, imported.stderr)
