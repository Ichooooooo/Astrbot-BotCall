import asyncio
from contextlib import nullcontext
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, patch

from bot_call.benchmark import baseline, hints, round_stages, snapshot
from bot_call.benchmark_run import BenchmarkController, infer_thinking, provider_config
from bot_call.observability import tts_stream_fallback_summary
from bot_call.streaming_tts import StreamFailure
from test_streaming_tts import PCM, fake_openai, openai_provider, wav_chunks


def row(stage, ms, gid="reply:a", seq=0):
    return dict(stage=stage, elapsed_ms=ms, generation_id=gid, sequence=seq, start_ms=0)


def database():
    db = sqlite3.connect(":memory:")
    for statement in (
        "CREATE TABLE calls(call_id TEXT, created_at REAL)",
        "CREATE TABLE turn_timings(call_id TEXT,generation_id TEXT,stage TEXT,sequence INT,start_ms REAL,elapsed_ms REAL)",
        "CREATE TABLE audio_jobs(call_id TEXT,utterance_id TEXT,series_id TEXT)",
        "CREATE TABLE reviews(call_id TEXT,ended_at REAL)",
    ):
        db.execute(statement)
    return db


class ProjectionTests(unittest.TestCase):
    def test_segments_missing_series_and_interrupted_rounds(self):
        rows = [
            row("stt_queue", 40, "capture:a"),
            row("stt_convert", 30, "capture:a"),
            row("stt_call", 60, "capture:a"),
            row("stt_queue", 20, "capture:b"),
            row("stt_call", 80, "capture:b"),
            row("client_upload", 999, "capture:a"),
            row("client_upload", 100),
            row("first_sentence", 900),
            row("tts_first_chunk", 300, seq=1),
            row("tts_first_chunk", 1, seq=2),
            row("client_pcm_play", 80),
            row("speech_end_first_sound", 3300),
            row("client_endpoint_quiet_ms", 1400),
            row("stt_call", 120, "reply:barge"),
            row("client_barge_candidates", 1, "reply:barge"),
            row("client_model_fetch_ms", 22000, "page:foo"),
        ]
        stages = round_stages(rows, [dict(utterance_id=k, series_id="a") for k in ("a", "b")])
        self.assertEqual(
            stages[0],
            dict(stt=200, upload=100, llm=900, tts=300, delivery=80, total=3300, endpoint=1400),
        )
        self.assertEqual(stages[1], {})
        self.assertEqual(round_stages([row("stt_call", 1, "capture:abandoned")]), [])
        self.assertEqual(round_stages([row("tts_call", 5, seq=2)]), [{}])
        legacy = round_stages(
            [
                row("tts_call", 500, seq=1),
                row("tts_convert", 10, seq=1),
                row("client_download", 70),
                row("client_play", 30),
            ]
        )
        self.assertEqual(legacy, [dict(tts=510, delivery=100)])

    def test_recent_limit_flags_and_reference_no_identifiers(self):
        db = database()
        self.addCleanup(db.close)
        for i in range(35):
            cid = f"private-call-{i}"
            db.execute("INSERT INTO calls VALUES (?,?)", (cid, i))
            for r in (
                row("first_sentence", i),
                row("client_capture_mode", i % 2),
                row("client_ws_disconnects", 3),
                row("client_ws_disconnects", 5, seq=1),
            ):
                db.execute(
                    "INSERT INTO turn_timings VALUES (?,?,?,?,?,?)",
                    (
                        cid,
                        *[
                            r[k]
                            for k in (
                                "generation_id",
                                "stage",
                                "sequence",
                                "start_ms",
                                "elapsed_ms",
                            )
                        ],
                    ),
                )
        data = snapshot(db)
        self.assertEqual(len(data["calls"]), 30)
        self.assertEqual(data["calls"][0]["started_at"], 34)
        self.assertEqual(data["calls"][0]["flags"]["ws_disconnects"], 5)
        self.assertNotIn("private-call", json.dumps(data))
        self.assertEqual(data["calls"][0]["ref"], snapshot(db)["calls"][0]["ref"])
        self.assertNotIn("access", data["calls"][0]["flags"])

    def test_all_hint_rules_and_threshold(self):
        b = baseline()
        stages = {"upload": dict(median_ms=b["stages"]["upload"]["median_ms"] * 3)}
        hs = hints(
            stages,
            dict(access="quick_tunnel", capture="mixed", ws_disconnects=3, model_fetch_ms=6000),
            b,
            1800,
        )
        self.assertEqual(len(hs), 6)
        self.assertTrue(all(r["level"] in {"warn", "info"} for r in hs))
        self.assertEqual(hints({}, {}, b, 1400), [])
        self.assertEqual(
            hints({"upload": dict(median_ms=b["stages"]["upload"]["median_ms"] * 2)}, {}, b), []
        )
        # Streaming TTS fallback hints: strictly over half triggers warn.
        msg = "流式合成频繁失败，已改用整句合成，首声会变慢；常见原因是服务器需经代理上网、无法直连语音合成服务，或当前服务/模型不支持 24 kHz 流式输出"
        self.assertEqual(hints({}, dict(tts_stream_attempts=2, tts_stream_fallbacks=1), b), [])
        over_half = hints({}, dict(tts_stream_attempts=3, tts_stream_fallbacks=2), b)
        self.assertEqual(over_half, [dict(stage="tts", level="warn", text=msg)])
        agg_hint = hints(
            {}, dict(tts_stream_attempts=10, tts_stream_fallbacks=2, tts_stream_fallback_calls=1), b
        )
        self.assertEqual(agg_hint, [dict(stage="tts", level="warn", text=msg)])

        db = database()
        self.addCleanup(db.close)
        db.execute("INSERT INTO calls VALUES ('c1', 100)")
        for gid, stage, val in (
            ("reply:1", "tts_stream_fallback", 1),
            ("reply:2", "tts_stream_fallback", 2),
            ("reply:3", "tts_first_chunk", 240),
        ):
            db.execute(
                "INSERT INTO turn_timings VALUES (?,?,?,?,?,?)",
                ("c1", gid, stage, 1, 0, val),
            )
        snap = snapshot(db)
        self.assertEqual(snap["calls"][0]["flags"]["tts_stream_attempts"], 3)
        self.assertEqual(snap["calls"][0]["flags"]["tts_stream_fallbacks"], 2)
        self.assertEqual(
            hints(snap["calls"][0]["stages"], snap["calls"][0]["flags"], b),
            [dict(stage="tts", level="warn", text=msg)],
        )

    def test_baseline_cli_same_projection_and_readonly(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "calls.sqlite3"
            db = database()
            dest = sqlite3.connect(path)
            db.backup(dest)
            db.close()
            dest.execute("INSERT INTO calls VALUES ('private',1)")
            for r in (
                row("client_endpoint_quiet_ms", 1400),
                row("client_upload", 120),
                row("stt_queue", 40),
                row("stt_call", 300),
                row("first_sentence", 900),
                row("tts_first_chunk", 400, seq=1),
                row("client_pcm_play", 80),
                row("speech_end_first_sound", 3000),
            ):
                dest.execute(
                    "INSERT INTO turn_timings VALUES (?,?,?,?,?,?)",
                    (
                        "private",
                        *[
                            r[k]
                            for k in (
                                "generation_id",
                                "stage",
                                "sequence",
                                "start_ms",
                                "elapsed_ms",
                            )
                        ],
                    ),
                )
            dest.commit()
            expected = snapshot(dest)["summary"]["stages"]
            dest.close()
            before = path.read_bytes()
            for args in (["--recent", "1"], ["--call-id", "private"]):
                p = subprocess.run(
                    [sys.executable, "tools/benchmark_baseline.py", str(path), *args],
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(p.returncode, 0, p.stderr)
                self.assertEqual(json.loads(p.stdout)["stages"], expected)
            self.assertEqual(path.read_bytes(), before)


class RunTests(unittest.IsolatedAsyncioTestCase):
    def host(self):
        self.host_obj = NS(
            ready=True,
            _terminating=False,
            _config=dict(
                enabled=True, access_mode="fixed_https", public_origin="https://example.test"
            ),
            _settings=NS(rejections=NS(latest=None)),
            _tunnel=None,
            _backend=NS(_runtime=NS(active_call_count=AsyncMock(return_value=0))),
            _native=NS(privacy=NS(scope=nullcontext)),
        )
        return self.host_obj

    async def test_guards_and_memory_only(self):
        h = self.host()
        c = BenchmarkController(h)
        c.providers = AsyncMock(return_value={k: NS() for k in ("stt", "llm", "tts")})
        c.selfcheck = AsyncMock(return_value=[])
        c.probe = AsyncMock(return_value=None)
        h.ready = False
        self.assertEqual((await c.run())["data"]["code"], "disabled")
        h.ready = True
        h._backend._runtime.active_call_count.return_value = 1
        self.assertEqual((await c.run())["data"]["code"], "busy")
        h._backend._runtime.active_call_count.return_value = 0
        block = asyncio.Event()

        async def probe(*args):
            await block.wait()

        c.probe.side_effect = probe
        task = asyncio.create_task(c.run())
        await asyncio.sleep(0)
        self.assertEqual((await c.run())["data"]["code"], "running")
        block.set()
        result = await task
        self.assertTrue(result["data"]["ok"])
        self.assertIs(c.last_run, result["data"])
        self.assertEqual((await c.run())["data"]["code"], "rate_limited")
        self.assertEqual(c.probe.await_count, 3)
        # Host deliberately has no database, Agent, persona or history attributes.

    async def test_probe_detached_arguments_and_no_history(self):
        c = BenchmarkController(self.host())
        llm = NS(
            text_chat=AsyncMock(return_value=NS(completion_text="测试成功", tools_call_name=[]))
        )
        stt = NS(get_text=AsyncMock(return_value="测试"))
        await c.probe("llm", llm)
        await c.probe("stt", stt)
        kwargs = llm.text_chat.call_args.kwargs
        self.assertEqual(kwargs["contexts"], [])
        self.assertEqual(kwargs["system_prompt"], "")
        self.assertIsNone(kwargs["func_tool"])
        self.assertFalse(Path(stt.get_text.call_args.args[0]).exists())

    async def test_selfchecks_and_failure_skip(self):
        h = self.host()
        c = BenchmarkController(h)
        with patch("bot_call.benchmark_run.shutil.which", return_value="/bin/fake"):
            good = await c.selfcheck({k: object() for k in ("stt", "llm", "tts")})
        self.assertTrue(all(r["state"] == "ok" for r in good), good)
        h.ready = False
        h._config.update(access_mode="quick_tunnel")
        with (
            patch("bot_call.benchmark_run.shutil.which", return_value=None),
            patch("bot_call.astrbot_compat.astrbot_version_status", side_effect=RuntimeError),
        ):
            bad = await c.selfcheck({})
        states = {r["key"]: r["state"] for r in bad}
        self.assertEqual(states["host"], "fail")
        self.assertEqual(states["cloudflared"], "fail")
        self.assertEqual(states["tunnel"], "warn")
        self.assertEqual(states["astrbot"], "fail")
        h.ready = True
        c.providers = AsyncMock(return_value={"stt": object()})
        c.probe = AsyncMock(side_effect=RuntimeError("SECRET"))
        result = await c.run()
        self.assertEqual([r["state"] for r in result["data"]["tests"]], ["fail", "skip", "skip"])
        self.assertNotIn("SECRET", json.dumps(result))

    async def test_astrbot_version_selfcheck(self):
        c = BenchmarkController(self.host())
        for version, state, detail in (
            ("4.27.2", "ok", "版本受支持"),
            (
                "4.31.2",
                "warn",
                "当前版本 4.31.2 尚未逐项验证，已按能力探测放行；"
                "如遇异常请查看日志或使用原生插件设置页",
            ),
            ("4.28.5", "fail", "请使用受支持的 AstrBot 版本"),
        ):
            with (
                self.subTest(version=version),
                patch("bot_call.astrbot_compat.framework_version", return_value=version),
            ):
                row = next(r for r in await c.selfcheck({}) if r["key"] == "astrbot")
                self.assertEqual(
                    row, dict(key="astrbot", label="AstrBot 版本", state=state, detail=detail)
                )

    async def test_selfcheck_resource_rejection_and_stream_first_chunk(self):
        h = self.host()
        c = BenchmarkController(h)
        h._settings.rejections.latest = {"reason": "synthetic"}
        with patch("bot_call.benchmark_run.Path.is_file", return_value=False):
            rows = {r["key"]: r for r in await c.selfcheck({})}
        self.assertEqual(rows["vad"]["state"], "fail")
        self.assertEqual(rows["access_rejection"]["state"], "warn")
        h._config["access_mode"] = "quick_tunnel"
        h._tunnel = NS(origin="https://example.test")
        with patch("bot_call.benchmark_run.shutil.which", return_value="/bin/fake"):
            rows = {r["key"]: r for r in await c.selfcheck({})}
        self.assertEqual(rows["tunnel"]["state"], "ok")

        async def stream(text, settings, emit):
            await emit(b"audio")
            return b"audio"

        with (
            patch("bot_call.astrbot_compat.dashscope_stream_settings", return_value={}),
            patch("bot_call.streaming_tts.stream_sentence", side_effect=stream),
        ):
            ms = await c.probe("tts", object())
        self.assertGreaterEqual(ms, 0)

    async def test_tts_stream_failure_falls_back_only_before_audio(self):
        for received, audio, expected in (
            (False, "synthetic-audio", "ok"),
            (False, "", "fail"),
            (True, "synthetic-audio", "fail"),
        ):
            with self.subTest(received=received, audio=bool(audio)):
                c = BenchmarkController(self.host())
                provider = NS(
                    provider_config=dict(type="dashscope_tts", api_key="synthetic-key"),
                    get_model=lambda: "bailian-tts-v4",
                    get_audio=AsyncMock(return_value=audio),
                )
                c.providers = AsyncMock(return_value={"tts": provider})
                c.selfcheck = AsyncMock(return_value=[])
                with patch(
                    "bot_call.streaming_tts.stream_sentence",
                    side_effect=StreamFailure(received, "task_failed"),
                ) as stream:
                    result = await c.run()
                stream.assert_awaited_once()
                row = result["data"]["tests"][-1]
                self.assertEqual(row["key"], "tts")
                self.assertEqual(row["state"], expected)
                if received:
                    provider.get_audio.assert_not_awaited()
                else:
                    provider.get_audio.assert_awaited_once_with("测试成功。")
                if expected == "ok":
                    self.assertEqual(
                        row["detail"], "流式合成不可用，已回退整句合成（固定样本调用成功）"
                    )
                    self.assertGreaterEqual(row["ms"], 0)

    async def test_openai_tts_config_and_manual_run(self):
        for parts, status, finish, state, fallback in (
            ([wav_chunks(PCM)], 200, "eof", "ok", False),
            ([wav_chunks(PCM, rate=32000)], 200, "eof", "ok", True),
            ([], 401, "eof", "ok", True),
            ([wav_chunks(PCM, size=0)], 200, "abort", "fail", False),
        ):
            with (
                self.subTest(status=status, state=state, fallback=fallback),
                patch.dict(os.environ, {"no_proxy": "127.0.0.1"}),
            ):
                c = BenchmarkController(self.host())
                c.selfcheck = AsyncMock(return_value=[])
                async with fake_openai(
                    parts, mime="audio/wav", status=status, finish=finish
                ) as server:
                    provider = openai_provider(api_base=server.base)
                    provider.get_audio = AsyncMock(return_value="synthetic-audio")
                    self.assertEqual(
                        provider_config(provider, "tts", "get_audio"),
                        dict(
                            label=provider.get_model(), model=provider.get_model(), streaming=True
                        ),
                    )
                    c.providers = AsyncMock(return_value={"tts": provider})
                    result = await c.run()
                self.assertEqual(len(server.seen), 1)
                row = result["data"]["tests"][-1]
                self.assertEqual((row["key"], row["state"]), ("tts", state))
                if fallback:
                    provider.get_audio.assert_awaited_once_with("测试成功。")
                    self.assertEqual(
                        row["detail"], "流式合成不可用，已回退整句合成（固定样本调用成功）"
                    )
                else:
                    provider.get_audio.assert_not_awaited()
                if state == "ok":
                    self.assertGreaterEqual(row["ms"], 0)
                else:
                    self.assertIsNone(row["ms"])

    async def test_cancel_releases_manual_run(self):
        c = BenchmarkController(self.host())
        c.providers = AsyncMock(return_value={"stt": object()})
        c.selfcheck = AsyncMock(return_value=[])
        entered = asyncio.Event()

        async def probe(*args):
            entered.set()
            await asyncio.Event().wait()

        c.probe = probe
        task = asyncio.create_task(c.run())
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(c.running)
        self.assertIsNone(c.task)
        self.assertIsNone(c.last_run)


if __name__ == "__main__":
    unittest.main()


class ProviderOriginTests(unittest.IsolatedAsyncioTestCase):
    async def test_unbound_reload_uses_latest_call_origin_for_stt(self):
        seen = []

        async def check_stt(umo):
            seen.append(umo)
            if umo is None:
                raise RuntimeError
            return ("stt-provider", "native")

        runtime = NS(benchmark_origin=AsyncMock(return_value="qq:FriendMessage:1"))
        host = NS(
            _backend=NS(diagnostic_binding=lambda: None, _runtime=runtime),
            _native=NS(
                settings={"call_chat_provider_id": ""},
                context=None,
                check_stt=check_stt,
                check_tts=AsyncMock(return_value="tts-provider"),
            ),
        )
        with patch("bot_call.astrbot_compat.using_provider", AsyncMock(return_value="llm")):
            result = await BenchmarkController(host).providers()
        self.assertEqual(seen, ["qq:FriendMessage:1"])
        self.assertEqual(result["stt"], "stt-provider")
        runtime.benchmark_origin.assert_awaited_once()

    def test_runtime_origin_query_reads_only_latest_origin(self):
        db = sqlite3.connect(":memory:")
        db.execute("CREATE TABLE calls(call_id TEXT, created_at REAL, unified_msg_origin TEXT)")
        db.executemany("INSERT INTO calls VALUES(?,?,?)", [("a", 1, "old"), ("b", 2, "new")])
        row = db.execute(
            "SELECT unified_msg_origin FROM calls ORDER BY created_at DESC,rowid DESC LIMIT 1"
        ).fetchone()
        self.assertEqual(row[0], "new")


class ConfigAndStatusTests(unittest.IsolatedAsyncioTestCase):
    def test_thinking_explicit_unknown_and_conflicts(self):
        for cfg, expected in [
            (None, "unknown"),
            ({}, "unknown"),
            ({"reasoning": False}, "off"),
            ({"reasoning": True}, "on"),
            ({"custom_extra_body": {"thinking": {"type": "disabled"}}, "reasoning": False}, "off"),
            ({"extra_body": {"thinking": {"type": "enabled"}}}, "on"),
            ({"enable_thinking": False}, "off"),
            ({"custom_extra_body": {"enable_thinking": True}}, "on"),
            ({"reasoning_effort": "none"}, "off"),
            ({"reasoning": {"effort": "high"}}, "on"),
            ({"reasoning_effort": "nonsense"}, "unknown"),
            ({"thinking": {"type": "auto"}}, "unknown"),
            ({"reasoning": False, "extra_body": {"enable_thinking": True}}, "unknown"),
            ({"reasoning": "false"}, "unknown"),
        ]:
            with self.subTest(cfg=cfg):
                self.assertEqual(infer_thinking(cfg), expected)
        for effort in ("minimal", "low", "medium", "high", "xhigh"):
            self.assertEqual(infer_thinking({"reasoning_effort": effort}), "on")

    def test_baseline_config_and_unchanged_samples(self):
        b = baseline()
        expected = {
            "endpoint": (1408, 1408),
            "upload": (210, 617),
            "stt": (401, 1400),
            "llm": (1189, 1574),
            "tts": (432, 488),
            "delivery": (191, 206),
            "total": (3953, 6306),
        }
        self.assertEqual(
            b["stages"], {k: dict(median_ms=v[0], p90_ms=v[1]) for k, v in expected.items()}
        )
        self.assertEqual(b["config"]["llm"]["model"], "deepseek-v4-flash")
        self.assertEqual(b["config"]["llm"]["thinking"], "off")
        self.assertTrue(b["config"]["tts"]["streaming"])

    async def test_reasoning_hints_use_valid_weighted_usage_and_distinct_turns(self):
        from bot_call.store import Store
        from test_astrbot_context import binding

        store = Store(":memory:")
        self.addCleanup(store.close)
        db = store._db
        call_ids = ["high", "slow", "fast", "clamped", "zero"]
        for cid in call_ids:
            store.create_call(binding(cid))

        def insert(cid, gid, stage, seq, ms, completion=None, reasoning=None):
            db.execute(
                "INSERT INTO turn_timings (call_id,generation_id,stage,sequence,start_ms,elapsed_ms,"
                "provider_id,model,selection,tool,recorded_at,completion_tokens,reasoning_tokens) "
                "VALUES (?,?,?,?,0,?,'unknown','unknown','unknown','unknown',0,?,?)",
                (cid, gid, stage, seq, ms, completion, reasoning),
            )

        for cid, ms, completion, reasoning in (
            ("high", 1100, 100, 70),
            ("slow", 2500, 100, 50),
            ("fast", 1100, 100, 50),
            ("clamped", 1100, 100, 150),
            ("zero", 1100, 1000, 0),
        ):
            insert(cid, "reply:a", "first_sentence", 0, ms)
            insert(cid, "reply:a", "llm", 0, ms, completion, reasoning)
        insert("high", "reply:a", "llm", 1, 1100, 100, 70)
        insert("high", "reply:b", "llm", 0, 1100, 100, 70)
        for seq, (gid, stage, completion, reasoning) in enumerate(
            [
                ("review:a", "llm", 1000, 0),
                ("reply:a", "first_sentence", 1000, 0),
                ("reply:a", "llm", 0, 500),
                ("reply:a", "llm", 1000, -1),
                ("reply:a", "llm", 1000, None),
                ("reply:a", "llm", 1000, 0.5),
                ("reply:a", "llm", 1000.5, 0),
                ("reply:a", "llm", "invalid", 500),
            ],
            start=3,
        ):
            insert("high", gid, stage, seq, 1100, completion, reasoning)

        runtime = NS(
            benchmark_snapshot=AsyncMock(),
            benchmark_reviews=AsyncMock(return_value={}),
        )
        controller = BenchmarkController(
            NS(
                _backend=NS(_runtime=runtime),
                _native=None,
                _config={},
                _version="0.0.0",
                ready=True,
            )
        )
        controller.providers = AsyncMock(return_value={})

        def thinking_hints(data):
            return [hint for hint in data["hints"] if "思考过程" in hint["text"]]

        for selection, percent in (
            (["high"], 70),
            (["slow"], 50),
            (["fast"], None),
            (["clamped"], 100),
            (["zero"], None),
            (["high", "clamped"], 90),  # 360 / 400; weighting uses raw counts before clamping.
            (call_ids, None),  # The known zero-thinking output contributes to the denominator.
        ):
            with self.subTest(selection=selection):
                runtime.benchmark_snapshot.return_value = snapshot(db, call_ids=selection)
                result = await controller.get()
                expected = (
                    []
                    if percent is None
                    else [
                        dict(
                            stage="llm",
                            level="warn",
                            text=f"对话模型约 {percent}% 的输出花在思考过程上，会明显拉慢开口速度。"
                            "建议在控制面板为电话单独指定关闭思考的快速模型。",
                        )
                    ]
                )
                self.assertEqual(thinking_hints(result), expected)
                payload = json.dumps(result)
                for key in (
                    "reasoning_tokens",
                    "completion_tokens",
                    "call_chat_provider_id",
                    "_reasoning_usage",
                ):
                    self.assertNotIn(key, payload)
                if len(selection) == 1:
                    call = result["calls"][0]
                    self.assertEqual(thinking_hints(call), expected)
                    if selection == ["zero"]:
                        self.assertNotIn("reasoning_ratio", call["flags"])
                        self.assertNotIn("reasoning_turns", call["flags"])
                    else:
                        cid = selection[0]
                        self.assertEqual(
                            call["flags"]["reasoning_ratio"],
                            {"high": 0.7, "clamped": 1.0, "slow": 0.5, "fast": 0.5}[cid],
                        )
                        self.assertEqual(
                            call["flags"]["reasoning_turns"], 2 if cid == "high" else 1
                        )

    async def test_circuit_open_fallback_warns_even_below_half_of_rounds(self):
        db = database()
        self.addCleanup(db.close)
        db.execute("INSERT INTO calls VALUES ('c1', 100)")
        metrics = [row("tts_first_chunk", 230, f"reply:{n}", seq=1) for n in range(4)] + [
            row("tts_stream_fallback", 2, "reply:4", seq=1),
            row("tts_stream_fallback", 5, "reply:5", seq=1),
            row("tts_stream_fallback", 6, "reply:6", seq=1),
        ]
        db.executemany(
            "INSERT INTO turn_timings VALUES (?,?,?,?,?,?)",
            [
                (
                    "c1",
                    r["generation_id"],
                    r["stage"],
                    r["sequence"],
                    r["start_ms"],
                    r["elapsed_ms"],
                )
                for r in metrics
            ],
        )
        self.assertEqual(
            tts_stream_fallback_summary(metrics),
            {"proxy_unsupported": 1, "circuit_open": 1, "task_failed": 1},
        )
        runtime = NS(
            benchmark_snapshot=AsyncMock(return_value=snapshot(db)),
            benchmark_reviews=AsyncMock(return_value={}),
        )
        controller = BenchmarkController(
            NS(
                _backend=NS(_runtime=runtime),
                _native=None,
                _config={},
                _version="0.0.0",
                ready=True,
            )
        )
        controller.providers = AsyncMock(return_value={})
        result = await controller.get()
        call = result["calls"][0]
        self.assertEqual(call["flags"]["tts_stream_attempts"], 7)
        self.assertEqual(call["flags"]["tts_stream_fallbacks"], 3)
        for report in (call, result):
            self.assertEqual([(h["stage"], h["level"]) for h in report["hints"]], [("tts", "warn")])

    async def test_get_objects_status_and_public_reviews(self):
        def provider(model, **cfg):
            return NS(
                meta=lambda: NS(id="synthetic"),
                get_model=lambda: model,
                provider_config=cfg,
                text_chat=lambda: None,
                get_text=lambda: None,
                get_audio=lambda: None,
            )

        runtime = NS(
            benchmark_snapshot=AsyncMock(
                return_value=dict(calls=[], summary=dict(calls=0, stages={}))
            ),
            benchmark_reviews=AsyncMock(
                return_value=dict(pending_count=0, reasons={}, calls=[], truncated=False)
            ),
        )
        h = NS(
            _backend=NS(_runtime=runtime),
            _native=NS(settings={"origin": "https://example.test"}),
            _config={"call_chat_provider_id": "dedicated", "access_mode": "fixed_https"},
            _version="0.14.0",
            ready=True,
        )
        c = BenchmarkController(h)
        tts = provider("cosyvoice-v3-flash", type="dashscope_tts", api_key="synthetic")
        c.providers = AsyncMock(
            return_value={"llm": provider("deepseek-v4-flash", reasoning=False), "tts": tts}
        )
        data = await c.get()
        from copy import deepcopy

        data = deepcopy(data)
        self.assertEqual(data["current_config"]["llm"]["compare_model"], "deepseek-v4-flash")
        for model, expected in (("DeepSeek-Flash", "deepseek-v4-flash"), ("other", "other")):
            c.providers.return_value["llm"] = provider(model, reasoning=False)
            alias_data = await c.get()
            self.assertEqual(alias_data["current_config"]["llm"]["model"], model)
            self.assertEqual(alias_data["current_config"]["llm"]["compare_model"], expected)
        c.providers.return_value["stt"] = provider("qwen")
        h._native.settings["call_stt_provider_id"] = "chosen"
        self.assertEqual((await c.get())["current_config"]["stt"]["selection"], "dedicated")
        h._native.settings["call_stt_provider_id"] = ""
        self.assertEqual((await c.get())["current_config"]["stt"]["selection"], "inherited")
        self.assertIsNone(data["current_config"]["stt"])
        self.assertEqual(data["current_config"]["llm"]["thinking"], "off")
        self.assertTrue(data["current_config"]["tts"]["streaming"])
        self.assertEqual(
            data["status"],
            dict(
                version="0.14.0",
                plugin_ready=True,
                https_ready=True,
                page_url="https://example.test/bot-call/",
                access="固定 HTTPS",
                call_model=dict(selection="dedicated", model="deepseek-v4-flash", available=True),
            ),
        )
        self.assertEqual(data["reviews"]["reason_labels"]["work_uncertain"], "生成结果未知，不重放")
        runtime.benchmark_snapshot = AsyncMock(
            return_value=dict(
                calls=[
                    dict(
                        ref="c1",
                        started_at=1,
                        duration_s=10,
                        turns=2,
                        stages={},
                        flags=dict(
                            barge_candidates=0,
                            ws_disconnects=0,
                            model_fetch_ms=0,
                            tts_stream_attempts=2,
                            tts_stream_fallbacks=2,
                        ),
                    ),
                    dict(
                        ref="c2",
                        started_at=2,
                        duration_s=10,
                        turns=4,
                        stages={},
                        flags=dict(
                            barge_candidates=0,
                            ws_disconnects=0,
                            model_fetch_ms=0,
                            tts_stream_attempts=4,
                            tts_stream_fallbacks=0,
                        ),
                    ),
                ],
                summary=dict(calls=2, stages={}),
            )
        )
        agg_data = await c.get()
        self.assertTrue(any("流式合成频繁失败" in item["text"] for item in agg_data["hints"]))
        self.assertTrue(
            any("流式合成频繁失败" in item["text"] for item in agg_data["calls"][0]["hints"])
        )
        self.assertFalse(
            any("流式合成频繁失败" in item["text"] for item in agg_data["calls"][1]["hints"])
        )
        self.assertNotIn("synthetic", json.dumps(data))
        self.assertFalse(provider_config(provider("other"), "tts", "get_audio")["streaming"])
        h._native = None
        h._backend = None
        h.ready = False
        h._config = {}
        c.providers = AsyncMock(return_value={})
        data = await c.get()
        self.assertFalse(data["status"]["https_ready"])
        self.assertEqual(data["status"]["call_model"]["selection"], "source")
        self.assertIsNone(data["status"]["page_url"])

    async def test_public_review_projection_caps_and_preserves_legacy_route(self):
        from bot_call.runtime import CallRuntime
        from bot_call.access import session_reference
        import hashlib

        from bot_call.store import Store
        from test_astrbot_context import binding

        store = Store(":memory:")
        self.addCleanup(store.close)
        for i in range(25):
            cid = f"private-{i}"
            store.create_call(binding(cid))
            store.append_user(cid, "u", "测试材料", source="synthetic")
            with patch("bot_call.store.time", return_value=i):
                store.end_call(cid, prompt="冻结")
        runtime = NS(
            _limits=None,
            _store=store,
            _call_allowed=lambda cid: False,
            _review_routes=None,
            _review_tasks={},
        )
        data = await CallRuntime._operation(runtime, "benchmark_reviews", ())
        self.assertEqual(data["pending_count"], 25)
        self.assertEqual(data["reasons"], {"scope_required": 25})
        self.assertTrue(data["truncated"])
        self.assertEqual(len(data["calls"]), 20)
        self.assertEqual([r["at"] for r in data["calls"]], list(range(24, 4, -1)))
        self.assertEqual(
            data["calls"][0]["ref"], session_reference(hashlib.sha256(b"private-24").hexdigest())
        )
        self.assertEqual(
            set(data["calls"][0]),
            {"ref", "state", "reason", "work_state", "at", "detail", "model", "retryable"},
        )
        self.assertNotIn("private-", json.dumps(data))
        legacy = await CallRuntime._operation(runtime, "review_diagnostics", ())
        self.assertEqual(len(legacy["calls"]), 25)
        self.assertIn("call_id", legacy["calls"][0])
        self.assertNotIn('"pages/diagnostics/', Path("tools/build_plugin.py").read_text())
