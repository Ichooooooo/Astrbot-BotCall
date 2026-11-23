"""Real worker/SQLite/ASGI, synthetic fragments; no microphone or paid service."""

import asyncio
import hashlib
import unittest
from unittest.mock import patch
import test_audio as audio_fixture
from test_astrbot_context import binding
from test_web_access import ORIGIN
from bot_call.chunk_upload import ChunkUploads
from bot_call.web import CallWebApp


class ChunkTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = audio_fixture.AudioTests.asyncSetUp
    asyncTearDown = audio_fixture.AudioTests.asyncTearDown
    runtime = audio_fixture.AudioTests.runtime
    request = audio_fixture.AudioTests.request
    exchange = audio_fixture.AudioTests.exchange
    issue = audio_fixture.AudioTests.issue
    on_business = audio_fixture.AudioTests.on_business
    upload = audio_fixture.AudioTests.upload

    async def setup_upload(self):
        async def transcribe(bound, job):
            return "合成转写"

        runtime = await self.runtime(transcribe)
        app = CallWebApp(runtime, origin=ORIGIN, max_audio_bytes=32)
        secret, _ = await self.issue(runtime, binding("chunk"))
        cookie, ref, _ = await self.exchange(app, secret)
        return runtime, app, cookie, ref

    async def chunk(self, app, cookie, ref, seq=0, offset=0, body=b"abc", key="u", **kwargs):
        return await self.request(
            app,
            "audio-chunk",
            cookie=cookie,
            reference=ref,
            raw=body,
            headers={
                "content-type": "audio/webm",
                "x-bot-utterance-id": key,
                "x-bot-chunk-seq": str(seq),
                "x-bot-chunk-offset": str(offset),
                **kwargs,
            },
        )

    async def commit(self, app, cookie, ref, body=b"abcdef", key="u", **kwargs):
        return await self.request(
            app,
            "audio-commit",
            cookie=cookie,
            reference=ref,
            data={
                "utterance_id": key,
                "total_bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "mime": "audio/webm",
                **kwargs,
            },
        )

    async def final(
        self, app, cookie, ref, body=b"abcdef", key="u", offset=0, tail=None, **headers
    ):
        return await self.request(
            app,
            "audio-commit",
            cookie=cookie,
            reference=ref,
            raw=body[offset:] if tail is None else tail,
            headers={
                "content-type": "audio/webm",
                "x-bot-utterance-id": key,
                "x-bot-tail-offset": str(offset),
                "x-bot-total-bytes": str(len(body)),
                "x-bot-audio-sha256": hashlib.sha256(body).hexdigest(),
                **headers,
            },
        )

    async def test_final_tail_overlap_races_integrity_and_legacy(self):
        runtime, app, cookie, ref = await self.setup_upload()
        # Server has the in-flight bytes, but the caller never saw their ACK.
        await self.chunk(app, cookie, ref)
        self.assertEqual((await runtime.read_state("chunk")).audio_jobs, ())
        for kwargs in [
            dict(body=b"Xbcdef"),
            dict(tail=b"Xbcdef"),
            dict(offset=4),
            dict(body=b"abcdeg", tail=b"abcdef"),
        ]:
            self.assertEqual((await self.final(app, cookie, ref, **kwargs))[0], 409)
        # Rejections cannot repair or poison the acknowledged prefix.
        self.assertEqual((await self.commit(app, cookie, ref, body=b"abc"))[0], 200)
        await self.chunk(app, cookie, ref, key="tail")
        await self.chunk(app, cookie, ref, seq=1, offset=3, body=b"de", key="tail")
        self.assertEqual((await self.final(app, cookie, ref, key="tail", offset=3))[0], 200)
        self.assertEqual((await self.final(app, cookie, ref, key="tail", offset=3))[0], 200)
        self.assertEqual((await self.commit(app, cookie, ref, key="tail"))[0], 200)
        self.assertEqual(
            (await self.final(app, cookie, ref, key="tail", body=b"xxxxxx", offset=3))[0], 409
        )
        self.assertEqual((await self.chunk(app, cookie, ref, key="tail", seq=1, offset=3))[0], 409)
        # Commit may beat the first chunk; offset zero carries all bytes.
        self.assertEqual((await self.final(app, cookie, ref, key="first"))[0], 200)
        await self.chunk(app, cookie, ref, key="empty", body=b"abcdef")
        self.assertEqual((await self.final(app, cookie, ref, key="empty", offset=6))[0], 200)
        await self.chunk(app, cookie, ref, key="old")
        await self.chunk(app, cookie, ref, key="new", body=b"new")
        for kwargs in [dict(key="old"), dict(key="other"), dict(key="new", offset=4)]:
            self.assertEqual((await self.final(app, cookie, ref, **kwargs))[0], 409)
        self.assertEqual((await self.commit(app, cookie, ref, key="new", body=b"new"))[0], 200)
        self.assertEqual((await self.upload(app, cookie, ref, "old", b"abcdef"))[0], 200)
        self.assertEqual((await self.final(app, cookie, ref, key="old"))[0], 200)
        self.assertEqual(
            (await self.final(app, cookie, ref, key="oversize", body=b"x" * 33))[0], 413
        )
        self.assertEqual((await self.final(app, None, ref))[0], 401)
        self.assertEqual(
            (await self.final(app, cookie, ref, **{"x-bot-tail-offset": "01"}))[0], 400
        )
        await runtime.drain()
        self.assertEqual(len(self.audio_calls), 6)
        self.assertEqual(
            [c[-1] for c in self.audio_calls],
            [b"abc", b"abcdef", b"abcdef", b"abcdef", b"new", b"abcdef"],
        )

    async def test_append_commit_idempotency_integrity_auth_and_legacy_fallback(self):
        runtime, app, cookie, ref = await self.setup_upload()
        self.assertEqual((await self.chunk(app, None, ref))[0], 401)
        self.assertEqual((await self.chunk(app, cookie, ref, origin="https://evil.test"))[0], 403)
        self.assertEqual((await self.chunk(app, cookie, "wrong"))[0], 401)
        self.assertEqual((await self.chunk(app, cookie, ref, seq=1, offset=3))[0], 409)
        a = await self.chunk(app, cookie, ref)
        self.assertEqual(
            a[:2], (200, dict(chunk_received=True, utterance_id="u", next_seq=1, next_offset=3))
        )
        self.assertEqual((await self.chunk(app, cookie, ref))[:2], a[:2])
        for kwargs in [
            dict(seq=2, offset=3),
            dict(seq=1, offset=4),
            dict(body=b"xyz"),
            dict(key="other", seq=1, offset=3),
        ]:
            self.assertEqual((await self.chunk(app, cookie, ref, **kwargs))[0], 409)
        self.assertEqual((await self.chunk(app, cookie, ref, seq=1, offset=3, body=b"def"))[0], 200)
        self.assertEqual((await self.chunk(app, cookie, ref))[1]["next_offset"], 6)
        self.assertEqual((await runtime.read_state("chunk")).audio_jobs, ())
        self.assertEqual(self.audio_calls, [])  # No paid work before commit.
        for body in [b"abcde", b"abcdeg"]:
            self.assertEqual((await self.commit(app, cookie, ref, body=body))[0], 409)
        self.assertEqual((await self.commit(app, cookie, ref, sha256="invalid"))[0], 400)
        self.assertEqual((await self.commit(app, cookie, ref))[0], 200)
        self.assertEqual((await self.commit(app, cookie, ref))[1]["audio_received"], True)
        self.assertEqual((await self.commit(app, cookie, ref, body=b"xxxxxx"))[0], 409)
        await runtime.drain()
        self.assertEqual(len(self.audio_calls), 1)
        self.assertEqual(self.audio_calls[0][-1], b"abcdef")
        self.assertEqual((await self.chunk(app, cookie, ref, key="fallback"))[0], 200)
        self.assertEqual((await self.upload(app, cookie, ref, "fallback", b"abcDEF"))[0], 200)
        self.assertEqual(
            (await self.commit(app, cookie, ref, body=b"abcDEF", key="fallback"))[0], 200
        )
        await runtime.drain()
        self.assertEqual(len(self.audio_calls), 2)

    async def test_new_utterance_replaces_abandoned_upload_without_reviving_late_chunks(self):
        runtime, app, cookie, ref = await self.setup_upload()
        self.assertEqual((await self.chunk(app, cookie, ref, key="A"))[0], 200)
        self.assertEqual(
            (await self.chunk(app, cookie, ref, key="B", seq=1, offset=3))[1]["error"],
            "audio_chunk_missing",
        )
        self.assertEqual(
            (await self.chunk(app, cookie, ref, key="B", offset=1))[1]["error"],
            "audio_chunk_missing",
        )
        self.assertEqual((await self.chunk(app, cookie, ref, key="B", body=b"new"))[0], 200)
        # Even a delayed retry of A's FIRST block must not replace B.
        for seq, offset in [(0, 0), (1, 3)]:
            result = await self.chunk(app, cookie, ref, key="A", seq=seq, offset=offset)
            self.assertEqual(result[:2], (409, {"error": "audio_chunk_missing"}))
        self.assertEqual(
            (await self.commit(app, cookie, ref, key="A", body=b"abc"))[:2],
            (409, {"error": "audio_chunk_missing"}),
        )
        self.assertEqual((await self.commit(app, cookie, ref, key="B", body=b"new"))[0], 200)
        await runtime.drain()
        self.assertEqual([(r[1], r[-1]) for r in self.audio_calls], [("B", b"new")])
        self.assertEqual(
            (await self.chunk(app, cookie, ref, key="B", body=b"new"))[:2],
            (409, {"error": "audio_conflict"}),
        )
        for seq, offset in [(0, 0), (1, 3)]:
            self.assertEqual(
                (await self.chunk(app, cookie, ref, key="A", seq=seq, offset=offset))[1]["error"],
                "audio_chunk_missing",
            )
        self.assertEqual(
            (await self.commit(app, cookie, ref, key="A", body=b"abc"))[1]["error"],
            "audio_chunk_missing",
        )
        # Abandoned IDs remain eligible for the original, fully verified upload.
        self.assertEqual((await self.upload(app, cookie, ref, "A", b"abcdef"))[0], 200)
        await runtime.drain()
        self.assertEqual(
            [(r[1], r[-1]) for r in self.audio_calls], [("B", b"new"), ("A", b"abcdef")]
        )
        self.assertEqual(
            (await self.chunk(app, cookie, ref, key="A"))[1]["error"], "audio_conflict"
        )
        self.assertEqual((await self.commit(app, cookie, ref, key="A", body=b"abcdef"))[0], 200)
        # Global capacity counts calls, so replacement at full capacity still works.
        chunks = ChunkUploads(32, asyncio.get_running_loop())
        try:
            for i in range(16):
                chunks.put(str(i), "A", 0, 0, b"a", "audio/webm")
            self.assertTrue(chunks.put("0", "B", 0, 0, b"b", "audio/webm")["chunk_received"])
            self.assertEqual(len(chunks.pending), 16)
            with self.assertRaisesRegex(audio_fixture.AudioError, "audio.conflict"):
                chunks.put("17", "A", 0, 0, b"a", "audio/webm")
        finally:
            chunks.close()
        self.assertEqual(
            (await self.request(app, "end", data={}, cookie=cookie, reference=ref))[0], 200
        )
        self.assertEqual(runtime._chunks.replaced, {})

    async def test_limits_expiry_hangup_and_reload_drop_only_uncommitted_bytes(self):
        runtime, app, cookie, ref = await self.setup_upload()
        self.assertEqual((await self.chunk(app, cookie, ref, body=b"x" * 33))[0], 413)
        self.assertEqual((await self.chunk(app, cookie, ref, body=b"x" * 20))[0], 200)
        self.assertEqual(
            (await self.chunk(app, cookie, ref, seq=1, offset=20, body=b"x" * 13))[0], 413
        )
        self.assertEqual((await self.commit(app, cookie, ref, body=b"x" * 33))[0], 413)
        # Real worker timer, bounded wait only in offline test.
        with patch.object(ChunkUploads, "TTL", 0.01):
            self.assertEqual((await self.chunk(app, cookie, ref, body=b"x" * 20))[0], 200)
        await asyncio.sleep(0.03)
        self.assertEqual(
            (await self.commit(app, cookie, ref, body=b"x" * 20))[1]["error"], "audio_chunk_missing"
        )
        self.assertEqual((await self.chunk(app, cookie, ref))[0], 200)
        self.assertEqual(
            (await self.request(app, "end", data={}, cookie=cookie, reference=ref))[0], 200
        )
        self.assertEqual((await self.commit(app, cookie, ref, body=b"abc"))[0], 409)
        self.assertEqual((await runtime.read_state("chunk")).audio_jobs, ())
        self.assertEqual(runtime._chunks.pending, {})
        self.assertEqual(self.audio_calls, [])
        await runtime.shutdown()
        other = await self.runtime(lambda *a: None)
        self.assertEqual(other._chunks.pending, {})
        self.assertEqual((await other.read_state("chunk")).audio_jobs, ())
