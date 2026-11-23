"""Synthetic timing data; real installed Runner, no live services."""

import asyncio
import hashlib
import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from bot_call import store as storage
from bot_call.store import Store
from bot_call.observability import timing_row, timing_summary
from test_astrbot_context import binding
from test_backend import NativeCase, response, TEXT, WAV


class TimingStoreTests(unittest.TestCase):
    def test_vad_settings_and_numeric_preparation_observations(self):
        from bot_call.native_host import settings_from, NativeHostError

        base = {"public_origin": "https://calls.example.test"}
        settings = settings_from(base)
        self.assertEqual(
            (settings["call_vad_silence_ms"], settings["call_vad_sensitivity"]), (1200, 50)
        )
        legacy = settings_from(dict(base, call_vad_sensitivity=70))
        self.assertEqual(legacy["call_vad_sensitivity"], 70)
        for key, values in {
            "call_vad_silence_ms": [299, 3001, True],
            "call_vad_sensitivity": [9, 91, False],
        }.items():
            for value in values:
                with self.assertRaises(NativeHostError):
                    settings_from(dict(base, **{key: value}))
        rows = []
        for stage, value in [
            ("client_start_warm", 3),
            ("client_model_cold", 254),
            ("client_segment_auto", 1),
        ]:
            row = timing_row(dict(stage=stage, sequence=0, start_ms=0, elapsed_ms=value))
            self.assertIsNotNone(row)
            rows.append(dict(row, call_id="c", generation_id="reply:u", recorded_at=1))
        summary = timing_summary(rows)
        self.assertEqual(summary["vad"]["client_segment_auto"], 1)
        self.assertNotIn("client_segment_auto", [r["stage"] for r in summary["stages"]])

    def test_capture_diagnostics_are_bounded_and_missing_values_remain_absent(self):
        from bot_call.observability import CAPTURE_METRICS, MODEL_OBSERVATIONS, client_metric_valid

        for key in CAPTURE_METRICS | dict.fromkeys(MODEL_OBSERVATIONS):
            for value in [True, -1, "4", 1.5, float("inf"), CAPTURE_METRICS.get(key, 600000) + 1]:
                self.assertFalse(client_metric_valid(key, value), (key, value))
            self.assertTrue(client_metric_valid(key, 1), key)
        self.assertTrue(client_metric_valid("client_clip_bytes", 2**31))
        self.assertIsNotNone(
            timing_row(dict(stage="client_clip_bytes", sequence=0, start_ms=0, elapsed_ms=2**31))
        )
        for key in ("client_clip_reject", "client_clip_ms", "client_clip_bytes"):
            for value in (True, -1, 1.5, "private", 2**53):
                self.assertFalse(client_metric_valid(key, value))
        self.assertFalse(client_metric_valid("client_audio_state_start", 0))
        self.assertFalse(client_metric_valid("client_audio_state_end", 5))
        rows = [
            dict(
                stage="client_audio_state_start",
                elapsed_ms=v,
                recorded_at=i,
                call_id="c",
                generation_id=f"reply:{i}",
            )
            for i, v in enumerate([1, 2])
        ]
        rows.append(
            dict(
                stage="client_energy_frames",
                elapsed_ms=0,
                recorded_at=3,
                call_id="c",
                generation_id="reply:3",
            )
        )
        stages = {r["stage"]: r for r in timing_summary(rows)["stages"]}
        self.assertEqual(stages["client_audio_state_start"]["latest_value"], 2)
        self.assertNotIn("median_ms", stages["client_audio_state_start"])
        self.assertEqual(stages["client_energy_frames"]["median_ms"], 0)
        self.assertNotIn("client_energy_first_ms", stages)

    def test_pcm_diagnostic_enums_are_bounded_and_report_latest(self):
        from bot_call.observability import client_metric_valid

        for stage, maximum in (
            ("client_pcm_failure", 9),
            ("tts_pcm_selection", 4),
            ("tts_stream_fallback", 7),
        ):
            for value in (True, 0, -1, maximum + 1, 1.5, "private"):
                self.assertIsNone(
                    timing_row(dict(stage=stage, sequence=0, start_ms=0, elapsed_ms=value))
                )
                if stage.startswith("client"):
                    self.assertFalse(client_metric_valid(stage, value))
            rows = [
                dict(
                    stage=stage,
                    elapsed_ms=v,
                    recorded_at=v,
                    call_id="c",
                    generation_id=f"reply:{v}",
                )
                for v in ((1, 6, 7) if stage == "tts_stream_fallback" else (1, maximum))
            ]
            summary = timing_summary(rows)
            self.assertEqual(summary["stages"][0]["latest_value"], maximum)
            if stage == "tts_stream_fallback":
                self.assertEqual(
                    summary["tts_stream_fallbacks"],
                    {"connect_timeout": 1, "task_failed": 1, "format_unsupported": 1},
                )

    def test_capture_start_recovery_and_foreground_metrics_are_bounded_latest(self):
        from bot_call.observability import CAPTURE_CODEC_METRICS, client_metric_valid

        names = (
            "client_capture_start_ms",
            "client_capture_start_probe",
            "client_capture_recovery_kind",
            "client_capture_recovery_ms",
            "client_capture_recovery_bytes",
            "client_capture_worker_ms",
            "client_capture_encode_wall_ms",
            "client_capture_ack_frames",
            "client_capture_pending_frames",
            "client_capture_timeout_stage",
            "client_capture_startup_retries",
            "client_capture_startup_state",
            "client_capture_startup_recovered_ms",
            "client_capture_model_overlap",
            "client_capture_model_overlap_ms",
            "client_gate_recheck",
            "client_gate_recheck_ms",
            "client_gate_initial_reason",
            "client_gate_aux_fault",
            "client_gate_aux_wall_ms",
            "client_gate_aux_clock_ms",
            "client_playback_sentence",
            "client_playback_path",
            "client_playback_context_created_after_input",
            "client_playback_base_us",
            "client_playback_output_us",
            "client_playback_latency_jump_ms",
            "client_playback_statechanges",
            "client_playback_interrupts",
            "client_hidden_ms",
            "client_foreground_result_ms",
            "client_ws_disconnect_ms",
            "client_ws_close_code",
            "client_ws_pull_attempts",
            "client_ws_pull_successes",
            "client_ws_pull_max_ms",
        )
        for name in names:
            limit = CAPTURE_CODEC_METRICS[name]
            for value in (True, -1, limit + 1, "private", 1.5):
                self.assertFalse(client_metric_valid(name, value))
            rows = [
                dict(
                    stage=name,
                    elapsed_ms=value,
                    sequence=i,
                    recorded_at=10 + i,
                    call_id="c",
                    generation_id="reply:u",
                )
                for i, value in enumerate((0, limit))
            ]
            stage = timing_summary(rows)["stages"][0]
            self.assertEqual(stage["latest_value"], limit)
            self.assertNotIn("median_ms", stage)

    def test_upload_breakdown_numbers_and_counts_are_separate(self):
        from bot_call.observability import UPLOAD_TIMES, client_metric_valid

        with TemporaryDirectory() as d, Store(Path(d) / "calls.db") as store:
            store.create_call(binding("upload"))
            values = {stage: 120 for stage in UPLOAD_TIMES}
            values["client_upload_chunks"] = 4
            self.assertTrue(all(client_metric_valid(k, v) for k, v in values.items()))
            for value in [-1, True, 4097, "4", 1.5]:
                self.assertFalse(client_metric_valid("client_upload_chunks", value))
            store.record_timings(
                "upload",
                "reply:u",
                [dict(stage=k, sequence=0, start_ms=0, elapsed_ms=v) for k, v in values.items()],
            )
            result = store.timing_snapshot()
            self.assertEqual(result["upload_chunks"], {"rounds": 1, "total": 4})
            self.assertEqual({r["stage"] for r in result["stages"]}, UPLOAD_TIMES)
            self.assertTrue(all(r["median_ms"] == 120 for r in result["stages"]))

    def test_schema16_atomic_upgrade_preserves_old_sql_rows_and_projects_numbers(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "old.db"
            with Store(path) as store:
                store.create_call(binding("old"))
                store.append_user("old", "u", TEXT, source="synthetic")
                store._db.execute("DROP TABLE turn_timings")
                from schema_fixture import remove_series_columns

                remove_series_columns(store._db)
                from schema_fixture import remove_retry_schema

                remove_retry_schema(store._db)
                store._db.execute("PRAGMA user_version=16")
                schema = list(store._db.execute("SELECT name,sql FROM sqlite_master ORDER BY name"))
                schema = [tuple(r) for r in schema]
                rows = list(store._db.execute("SELECT * FROM turns"))
                rows = [tuple(r) for r in rows]
            with patch.object(storage, "_TIMING_SCHEMA", storage._TIMING_SCHEMA + ("INVALID",)):
                with self.assertRaises(sqlite3.Error):
                    Store(path)
            with sqlite3.connect(path) as raw:
                self.assertEqual(raw.execute("PRAGMA user_version").fetchone()[0], 16)
                self.assertEqual(
                    raw.execute("SELECT name,sql FROM sqlite_master ORDER BY name").fetchall(),
                    schema,
                )
            with Store(path) as store:
                self.assertEqual(store._db.execute("PRAGMA user_version").fetchone()[0], 24)
                self.assertEqual([tuple(r) for r in store._db.execute("SELECT * FROM turns")], rows)
                for name, sql in schema:
                    if name in {"audio_jobs", "speech_segments"}:
                        continue  # schema20 intentionally adds nullable columns.
                    self.assertEqual(
                        store._db.execute(
                            "SELECT sql FROM sqlite_master WHERE name=?", (name,)
                        ).fetchone()[0],
                        sql,
                    )
                for seq, ms in enumerate([10, 20]):
                    store.record_timings(
                        "old",
                        "reply:u",
                        [
                            dict(
                                stage="llm",
                                sequence=seq,
                                start_ms=0,
                                elapsed_ms=ms,
                                provider_id="p",
                                model="m",
                                selection="inherited",
                                secret=TEXT,
                            )
                        ],
                    )
                store.record_timings(
                    "old",
                    "reply:v",
                    [dict(stage="llm", sequence=0, start_ms=0, elapsed_ms=50, provider_id=TEXT)],
                )
                result = store.timing_snapshot()
                self.assertEqual(result["round_count"], 2)
                self.assertEqual(result["stages"][0]["median_ms"], 40)
                self.assertNotIn(TEXT, json.dumps(result, ensure_ascii=False))
                self.assertEqual(
                    store._db.execute("SELECT count(*) FROM turn_timings").fetchone()[0], 3
                )
                for bad in [True, -1, float("nan"), float("inf"), "12", 86_400_001]:
                    self.assertIsNone(
                        timing_row(dict(stage="llm", sequence=0, start_ms=0, elapsed_ms=bad))
                    )
                # Keep the latest 50 round projection while preserving every row.
                for i in range(51):
                    store.record_timings(
                        "old",
                        "reply:r" + str(i),
                        [dict(stage="run", sequence=0, start_ms=0, elapsed_ms=i)],
                    )
                self.assertEqual(store.timing_snapshot()["round_count"], 50)
                self.assertEqual(
                    store._db.execute("SELECT count(*) FROM turn_timings").fetchone()[0], 54
                )


class TimingNativeTests(NativeCase):
    async def test_connection_metrics_persist_large_values_and_updates(self):
        from test_web_access import WebAccessTests

        event, _ = self.environment()
        backend, _ = await self.backend()
        _, web = await self.enter(backend, event)
        for value in (18000, 18000, 19000):
            status, _, _ = await WebAccessTests.request(
                self,
                backend.app,
                "metrics",
                cookie="__Secure-bot_call=" + web.token,
                reference=web.reference,
                data=dict(
                    generation_id="capture:foreground",
                    values={"client_hidden_ms": value, "client_ws_disconnect_ms": 50000},
                ),
            )
            self.assertEqual(status, 200)
        with Store(self.path / "calls.db") as store:
            rows = list(
                store._db.execute(
                    "SELECT stage,sequence,elapsed_ms FROM turn_timings ORDER BY stage,sequence"
                )
            )
            self.assertEqual(
                [tuple(r) for r in rows],
                [
                    ("client_hidden_ms", 0, 18000),
                    ("client_hidden_ms", 1, 19000),
                    ("client_ws_disconnect_ms", 0, 50000),
                ],
            )
            stages = {r["stage"]: r for r in store.timing_snapshot()["stages"]}
            self.assertEqual(stages["client_hidden_ms"]["latest_value"], 19000)

    async def test_capability_metrics_are_bounded_scoped_authenticated_and_immutable(self):
        from bot_call.observability import CAPABILITY_METRICS, client_metric_valid
        from test_web_access import WebAccessTests

        event, _ = self.environment()
        backend, _ = await self.backend()
        _, web = await self.enter(backend, event)
        gid = "page:cap:12345678-1234-1234-1234-123456789abc"
        values = dict(CAPABILITY_METRICS)
        for key, maximum in CAPABILITY_METRICS.items():
            for bad in (True, False, -1, maximum + 1, 0.5, "1", None):
                self.assertFalse(client_metric_valid(key, bad), (key, bad))
            self.assertTrue(client_metric_valid(key, maximum))

        async def send(generation=gid, fields=None, auth=True):
            return await WebAccessTests.request(
                self,
                backend.app,
                "metrics",
                cookie="__Secure-bot_call=" + web.token if auth else "",
                reference=web.reference if auth else "",
                data=dict(generation_id=generation, values=values if fields is None else fields),
            )

        self.assertNotEqual((await send(auth=False))[0], 200)
        for generation, fields in [
            ("reply:fake", values),
            ("page", values),
            ("capture:fake", values),
            ("page:cap:bad", values),
            (gid, {"client_cap_api": 1}),
            (gid, dict(values, client_cap_schema=2)),
            (gid, dict(values, client_cap_schema=True)),
            (gid, dict(values, raw_ua="private")),
            (gid, dict(values, client_cap_frames=65)),
            (gid, dict(values, client_cap_first_ms=float("inf"))),
            (gid, dict(values, client_upload=1)),
        ]:
            self.assertEqual((await send(generation, fields))[0], 400)
        self.assertEqual((await send())[:2], (200, {"recorded": True}))
        self.assertEqual((await send(fields={"client_cap_schema": 1, "client_cap_api": 0}))[0], 200)
        with Store(self.path / "calls.db") as store:
            rows = [dict(r) for r in store._db.execute("SELECT * FROM turn_timings")]
            self.assertEqual(len(rows), len(values))
            self.assertEqual({r["stage"]: r["elapsed_ms"] for r in rows}, values)
            self.assertEqual({r["generation_id"] for r in rows}, {gid})
            self.assertEqual(store.timing_snapshot()["round_count"], 0)
            self.assertEqual(store.timing_snapshot()["stages"], [])

    async def test_echo_metrics_are_bounded_scoped_authenticated_and_immutable(self):
        from bot_call.observability import ECHO_METRICS, client_metric_valid
        from test_web_access import WebAccessTests

        event, _ = self.environment()
        backend, _ = await self.backend()
        _, web = await self.enter(backend, event)
        gid = "page:echo:12345678-1234-1234-1234-123456789abc"
        values = dict(ECHO_METRICS)
        for key, maximum in ECHO_METRICS.items():
            for bad in (True, False, -1, maximum + 1, 0.5, "1", None):
                self.assertFalse(client_metric_valid(key, bad), (key, bad))
            self.assertTrue(client_metric_valid(key, maximum))

        async def send(generation=gid, fields=None, auth=True):
            return await WebAccessTests.request(
                self,
                backend.app,
                "metrics",
                cookie="__Secure-bot_call=" + web.token if auth else "",
                reference=web.reference if auth else "",
                data=dict(generation_id=generation, values=values if fields is None else fields),
            )

        self.assertNotEqual((await send(auth=False))[0], 200)
        for generation, fields in [
            ("reply:fake", values),
            ("page", values),
            ("capture:fake", values),
            ("page:echo:bad", values),
            (gid, {"client_echo_flags": 1}),
            (gid, dict(values, client_echo_schema=2)),
            (gid, dict(values, client_echo_schema=True)),
            (gid, dict(values, raw_ua="private")),
            (gid, dict(values, client_echo_pcm_frames=251)),
            (gid, dict(values, client_echo_lag_peak_ms=float("inf"))),
            (gid, dict(values, client_upload=1)),
        ]:
            self.assertEqual((await send(generation, fields))[0], 400)
        # Exercise the runtime entry directly, bypassing HTTP validation.
        from bot_call.runtime import RuntimeOperationError

        for generation, fields in [
            ("reply:fake", values),
            (gid, dict(values, client_echo_route=True)),
            (gid, dict(values, client_echo_pcm_ms=8001)),
            (gid, dict(values, client_upload=1)),
        ]:
            with self.assertRaises(RuntimeOperationError):
                await backend._runtime.web_metrics(web.token, web.reference, generation, fields)
        self.assertEqual((await send())[:2], (200, {"recorded": True}))
        self.assertEqual(
            (await send(fields={"client_echo_schema": 1, "client_echo_flags": 0}))[0], 200
        )
        with Store(self.path / "calls.db") as store:
            rows = [dict(r) for r in store._db.execute("SELECT * FROM turn_timings")]
            self.assertEqual(len(rows), len(values))
            self.assertEqual({r["stage"]: r["elapsed_ms"] for r in rows}, values)
            self.assertEqual({r["generation_id"] for r in rows}, {gid})
            self.assertEqual(store.timing_snapshot()["round_count"], 0)
            self.assertEqual(store.timing_snapshot()["stages"], [])

    async def test_real_runner_media_tokens_hooks_and_playback_projection(self):
        event, provider = self.environment()
        api = self.agent_api

        from native_astrbot import register_hook

        for kind in (api.event_types.OnLLMRequestEvent, api.event_types.OnLLMResponseEvent):

            async def hook(event, *args):
                await asyncio.sleep(0.01)

            register_hook(self, "synthetic_timing", kind, hook)
        from astrbot.core.provider.entities import TokenUsage

        async def answer(payload):
            result = response(
                "synthetic reply",
                raw={
                    "model": "actual-model",
                    "usage": {"completion_tokens_details": {"reasoning_tokens": 3}},
                },
            )
            result.usage = TokenUsage(input_other=11, input_cached=2, output=7)
            await asyncio.sleep(0.005)
            return result

        provider.responder = answer
        backend, host = await self.backend()
        handle, web = await self.enter(backend, event)
        await backend.web_audio_chunk(web.token, web.reference, "u", 0, 0, b"syn", "audio/webm")
        await backend.web_audio_chunk(web.token, web.reference, "u", 1, 3, b"thetic", "audio/webm")
        await backend.web_audio_commit(
            web.token, web.reference, "u", 9, hashlib.sha256(b"synthetic").hexdigest(), "audio/webm"
        )
        await backend.drain()
        await backend.web_speech(web.token, web.reference, "reply:u")
        await backend.drain()
        # Invalid optional observations are discarded; playback itself still succeeds.
        for revision, value in enumerate(
            [True, -1, float("nan"), "123", 3_600_001, 10**400, 1234], 1
        ):
            await backend.web_playback(
                web.token, web.reference, "reply:u", revision, "started", value
            )
        with Store(self.path / "calls.db") as store:
            rows = [dict(r) for r in store._db.execute("SELECT * FROM turn_timings")]
            stages = {r["stage"]: r for r in rows}
            self.assertTrue(
                {
                    "run",
                    "pre_llm",
                    "post_llm",
                    "llm",
                    "stt_queue",
                    "stt_call",
                    "stt_convert",
                    "tts_call",
                    "first_sound",
                }
                <= stages.keys()
            )
            llm = stages["llm"]
            self.assertEqual(
                (
                    llm["provider_id"],
                    llm["model"],
                    llm["prompt_tokens"],
                    llm["completion_tokens"],
                    llm["reasoning_tokens"],
                ),
                ("provider-A", "actual-model", 13, 7, 3),
            )
            self.assertGreaterEqual(stages["pre_llm"]["elapsed_ms"], 10)
            self.assertGreaterEqual(stages["post_llm"]["elapsed_ms"], 10)
            self.assertEqual(stages["first_sound"]["elapsed_ms"], 1234)
            self.assertNotIn(TEXT, json.dumps(rows, ensure_ascii=False))
            self.assertNotIn(
                "elapsed_ms",
                store.agent_record(backend._record(handle).binding.call_id, "reply:u")[
                    "reports_json"
                ],
            )
        from test_web_access import WebAccessTests

        status, body, _ = await WebAccessTests.request(
            self,
            backend.app,
            "playback",
            cookie="__Secure-bot_call=" + web.token,
            reference=web.reference,
            data=dict(
                generation_id="reply:u", revision=8, state="started", first_sound_ms="invalid"
            ),
        )
        self.assertEqual(status, 200)
        self.assertTrue(body["recorded"])
        status, body, _ = await WebAccessTests.request(
            self,
            backend.app,
            "playback",
            cookie="__Secure-bot_call=" + web.token,
            reference=web.reference,
            data=dict(generation_id="reply:u", revision=9, state="started", first_sound_ms=10**400),
        )
        self.assertEqual(status, 200)
        self.assertTrue(body["recorded"])
        from bot_call.observability import UPLOAD_TIMES

        values = {stage: 12 for stage in UPLOAD_TIMES}
        values["client_upload_chunks"] = 2
        status, body, _ = await WebAccessTests.request(
            self,
            backend.app,
            "metrics",
            cookie="__Secure-bot_call=" + web.token,
            reference=web.reference,
            data=dict(generation_id="reply:u", values=values),
        )
        self.assertEqual((status, body), (200, {"recorded": True}))
        projection = await backend._runtime.diagnostic_timings()
        self.assertEqual(projection["upload_chunks"], {"rounds": 1, "total": 2})
        self.assertEqual(projection["round_count"], 1)
        self.assertEqual(projection["models"][0]["model"], "actual-model")
