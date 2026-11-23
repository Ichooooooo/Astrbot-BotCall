"""Discardable business-loop stage totals; no identities, text or durable IO."""

from collections import deque
from time import monotonic_ns

STAGES = ("stt", "reply", "tts", "review")
OUTCOMES = ("success", "failed", "cancelled", "uncertain")
CAPACITY = 128


class StageTimings:
    """Window statistics, not all-time counts or SDK/phone latency.

    The runtime starts before scheduling the actual callback and finishes only
    after it returns/raises, including preparation and cancellation cleanup.
    Both writes and snapshots belong to the original business loop.
    """

    def __init__(self, *, clock=monotonic_ns):
        self._clock = clock
        self._samples = deque(maxlen=CAPACITY)
        self._closed = False

    def begin(self, stage):
        if self._closed or stage not in STAGES:
            return None
        return self._clock()

    def finish(self, stage, started, outcome):
        if self._closed or started is None or stage not in STAGES or outcome not in OUTCOMES:
            return
        elapsed = self._clock() - started
        # Invalid clocks and implausible intervals are omitted, never fabricated.
        if type(elapsed) is int and 0 <= elapsed <= 86_400_000_000_000:
            self._samples.append((stage, outcome, elapsed / 1_000_000))

    def snapshot(self):
        rows = []
        for stage in STAGES:
            values = [(outcome, ms) for key, outcome, ms in self._samples if key == stage]
            if values:
                rows.append(
                    {
                        "stage": stage,
                        "count": len(values),
                        "last_ms": round(values[-1][1], 2),
                        "mean_ms": round(sum(ms for _, ms in values) / len(values), 2),
                        "max_ms": round(max(ms for _, ms in values), 2),
                        "outcomes": {
                            key: sum(outcome == key for outcome, _ in values) for key in OUTCOMES
                        },
                    }
                )
        return {"capacity": CAPACITY, "sample_count": len(self._samples), "stages": rows}

    def close(self):
        self._closed = True
        self._samples.clear()


# Durable observations are separate from transcripts and Agent framework reports.
from contextvars import ContextVar
from contextlib import contextmanager
from math import ceil, isfinite
import re
from time import time

from .failures import ERROR_STAGES, failure_summary

CURRENT_TIMING = ContextVar("bot_call_round_timing", default=None)
REPLY_COUNT_STAGES = {"reply_chars", "reply_sentences"}
UPLOAD_COUNTS = {"client_upload_chunks"}
UPLOAD_TIMES = {
    "client_upload_flush",
    "client_upload_digest",
    "client_upload_precheck",
    "client_upload_post",
    "client_upload_tail",
}
CANCELLATION_STAGES = {"cancelled_closed"}
VAD_FLAGS = {
    "client_vad_first_ready",
    "client_vad_first_fallback",
    "client_segment_auto",
    "client_segment_vad_endpoint",
    "client_segment_auto_energy",
    "client_segment_auto_limit",
    "client_segment_manual",
    "client_segment_control",
    "client_vad_ready",
    "client_vad_fallback",
}
VAD_COUNTS = {"client_vad_gaps", "client_vad_lost_ms"}
MODEL_OBSERVATIONS = {"client_model_fetch_ms", "client_model_session_ms", "client_model_lag_ms"}
VAD_TIMES = {
    "client_model_cold",
    "client_start_cold",
    "client_start_warm",
    *MODEL_OBSERVATIONS,
}
# capture:<utterance id> observations exist even when no audio job was submitted.
CONNECTION_METRICS = {
    "client_ws_disconnect_ms": 86400000,
    "client_ws_pull_attempts": 4096,
    "client_ws_pull_successes": 4096,
    "client_ws_pull_max_ms": 60000,
    "client_ws_close_code": 8,
    "client_hidden_ms": 86400000,
    "client_foreground_result_ms": 60000,
}
PLAYBACK_METRICS = {
    "client_playback_sentence": 512,
    "client_playback_path": 2,
    "client_playback_context": 4,
    "client_playback_context_created_after_input": 1,
    "client_playback_end_context": 4,
    "client_playback_devicechanges": 4096,
    "client_playback_base_us": 2000000,
    "client_playback_output_us": 2000000,
    "client_playback_session_type": 6,
    "client_playback_session_state": 3,
    "client_playback_lead_ms": 1000,
    "client_playback_pcm_mp3_ms": 60000,
    "client_playback_statechanges": 4096,
    "client_playback_interrupts": 4096,
    "client_playback_latency_jump_ms": 2000,
}
BARGE_SERVER_METRICS = {"barge_context": 3, "barge_reason": 9, "barge_backchannels": 4096}
BARGE_METRICS = {
    "client_barge_stop_ms": 60000,
    "client_barge_command_ms": 60000,
    "client_barge_pause_ms": 60000,
    "client_barge_tail_ms": 1000,
    "client_barge_result": 5,
    "client_barge_resumed": 1,
    "client_barge_offset_error_us": 1000000,
    "client_barge_candidates": 4096,
    "client_barge_false": 4096,
    "client_barge_late_blocks": 65535,
    "client_barge_reference": 1,
    "client_barge_input_repairs": 3,
    "client_barge_vad_repairs": 3,
}
CAPTURE_CODEC_METRICS = {
    **BARGE_METRICS,
    **PLAYBACK_METRICS,
    **CONNECTION_METRICS,
    "client_capture_start_ms": 60000,
    "client_capture_start_probe": 1,
    "client_capture_recovery_kind": 3,
    "client_capture_recovery_bytes": 2097152,
    "client_capture_encode_wall_ms": 60000,
    "client_capture_worker_ms": 60000,
    "client_capture_ack_frames": 2000,
    "client_capture_pending_frames": 1550,
    "client_capture_model_overlap": 1,
    "client_capture_model_overlap_ms": 60000,
    "client_gate_recheck": 3,
    "client_gate_recheck_ms": 60000,
    "client_gate_initial_reason": 1023,
    "client_gate_aux_fault": 8,
    "client_gate_aux_wall_ms": 60000,
    "client_gate_aux_clock_ms": 60000,
    "client_capture_recovery_ms": 60000,
    "client_capture_input_reason": 17,
    "client_capture_input_rebuilds": 3,
    "client_capture_input_active": 1,
    "client_capture_input_speaking": 1,
    "client_capture_input_probe": 1,
    "client_capture_input_echo": 1,
    "client_capture_input_idle_ms": 60000,
    "client_capture_input_credits": 64,
    "client_capture_input_skip_ms": 60000,
    "client_capture_input_recovery": 4,
    "client_capture_timeout_stage": 3,
    "client_capture_startup_retries": 3,
    "client_capture_startup_state": 3,
    "client_capture_startup_recovered_ms": 600000,
    "client_capture_rebuilds": 3,
    "client_capture_rebuild_ms": 3000,
    "client_capture_clock_back_count": 1000,
    "client_capture_clock_back_max_ms": 60000,
    "client_capture_gap_count": 1000,
    "client_capture_gap_ms": 60000,
    "client_capture_gap_max_ms": 60000,
    "client_capture_mode": 1,
    "client_capture_fallback": 8,
    "client_capture_prefix_ms": 500,
    "client_capture_queue": 32,
    "client_capture_mux_reason": 8,
    "client_capture_packet": 10000,
    "client_capture_duration_us": 240001,
    "client_capture_duration_min_us": 240001,
    "client_capture_duration_max_us": 240001,
    "client_capture_ts_delta_us": 4000000,
    "client_capture_ts_offset_us": 4000000,
    "client_capture_skip": 5760,
    "client_capture_packets": 10000,
    "client_capture_samples": 2000000,
    "client_capture_coded_samples": 2000000,
    "client_capture_padding": 4000000,
    "client_capture_dropped": 10000,
    "client_capture_clock_flags": 15,
    "client_capture_packet_min": 5760,
    "client_capture_packet_max": 5760,
}
CLIP_REJECT_METRICS = {
    **CAPTURE_CODEC_METRICS,
    "client_clip_reject": 4,
    "client_silent_drop": 1,
    "client_silent_reason": 6,  # control/manual/limit/VAD/energy/other
    **dict.fromkeys(
        {
            "client_gate_vad_ms",
            "client_gate_vad_high_ms",
            "client_gate_vad_prior_ms",
            "client_gate_energy_ms",
            "client_gate_energy_run_ms",
            "client_gate_fallback_ms",
            "client_gate_last_ms",
            "client_gate_floor_ppm",
        },
        3600000,
    ),
    "client_gate_vad_peak_permille": 1000,
    "client_gate_decision": 4,  # unknown/VAD/energy/drop/high-confidence
    # Bitmask: POST/error/overflow/VAD invalid/gap/energy quality/frames/
    # missing speech duration/prior duration/gate (1 through 512).
    "client_gate_unknown_reason": 1023,
    "client_clip_ms": 86400000,
    "client_clip_bytes": 2**53 - 1,
}
CAPTURE_STATES = {"client_audio_state_start", "client_audio_state_end"}
CAPTURE_METRICS = {
    **CAPTURE_CODEC_METRICS,
    "client_vad_start_gaps": 65,
    "client_vad_start_skip_ms": 60000,
    "client_energy_frames": 1000000,
    "client_endpoint_quiet_ms": 3600000,
    "client_energy_first_ms": 3600000,
    **dict.fromkeys(CAPTURE_STATES, 4),
}
# Fixed enums, never exception messages or device identifiers.
PCM_METRICS = {"client_pcm_failure": 9}
TTS_STREAM_FALLBACK_REASONS = {
    "connect_timeout": 1,
    "proxy_unsupported": 2,
    "handshake_failed": 3,
    "other": 4,
    "circuit_open": 5,
    "task_failed": 6,
    "format_unsupported": 7,
}
TTS_ENUMS = {
    "tts_pcm_selection": 4,  # no receiver / multiple / occupied / selected
    "tts_stream_fallback": max(TTS_STREAM_FALLBACK_REASONS.values()),
}
DELIVERY_STAGES = {"client_first_audio_push", "client_first_audio_download"}
HOOK_STAGES = {"hook_ok", "hook_error", "hook_cancelled", "hook_skipped"}
TRANSPORT_STAGES = {
    "client_transport",
    "client_ws_disconnects",
    "client_ws_reconnects",
    *CONNECTION_METRICS,
}


# Schema 1: all capability fields are integers; no strings or arbitrary keys.
CAPABILITY_METRICS = {
    "client_cap_schema": 1,
    "client_cap_api": 1023,
    "client_cap_platform": 2,
    "client_cap_qq": 1,
    **dict.fromkeys(
        (
            "client_cap_opus",
            "client_cap_opus_checked",
            "client_cap_mse",
            "client_cap_mse_checked",
            "client_cap_mms",
            "client_cap_mms_checked",
        ),
        7,
    ),
    **dict.fromkeys(("client_cap_recorder", "client_cap_recorder_checked"), 63),
    "client_cap_encode": 3,
    "client_cap_encode_ms": 3000,
    "client_cap_chromium": 999,
    "client_cap_mux_reason": 8,
    "client_cap_roundtrip_samples": 96000,
    "client_cap_worker": 31,
    "client_cap_worker_state": 3,
    "client_cap_runtime": 1,
    "client_cap_context": 4,
    "client_cap_rate": 192000,
    "client_cap_worklet_ready": 1,
    "client_cap_frames": 64,
    "client_cap_first_ms": 60000,
    "client_cap_gap_peak_ms": 60000,
    "client_cap_gaps": 65,
    "client_cap_lost_ms": 60000,
    "client_cap_overflow": 16,
    "client_cap_error": 1,
}


# Echo schema 1: bounded integers only, independent page generation.
ECHO_METRICS = {
    "client_echo_schema": 1,
    "client_echo_turn": 4096,
    "client_echo_sentence": 4096,
    "client_echo_window": 4096,
    "client_echo_flags": 4095,
    **dict.fromkeys(
        (
            "client_echo_supported",
            "client_echo_checked",
            "client_echo_settings_known",
            "client_echo_settings_enabled",
        ),
        7,
    ),
    "client_echo_aec_mode": 4,
    "client_echo_route": 4,
    "client_echo_context": 4,
    "client_echo_rate": 192000,
    "client_echo_base_ms": 2000,
    "client_echo_base_mic_p90": 1200,
    "client_echo_lag_peak_ms": 1000,
    "client_echo_error": 7,
    **{
        f"client_echo_{path}_{key}": maximum
        for path in ("pcm", "mp3")
        for key, maximum in {
            "frames": 250,
            "ms": 8000,
            "rejected": 65535,
            "gaps": 65535,
            "mic_p50": 1200,
            "mic_p90": 1200,
            "ref_p90": 1200,
            "vad_ms": 8000,
            "vad_30_ms": 8000,
            "vad_65_ms": 8000,
            "run_65_ms": 8000,
            "candidates": 65535,
            "corr_q": 1000,
            "lag_ms": 512,
        }.items()
    },
}


def echo_values_valid(gid, values):
    if type(gid) is str and len(gid.split(":")) == 6:
        parts = gid.split(":")
        if (
            type(values) is not dict
            or not parts[-2].isdigit()
            or not parts[-1].isdigit()
            or not 0 <= int(parts[-2]) <= 4096
            or not 1 <= int(parts[-1]) <= 4096
            or values.get("client_echo_sentence") != int(parts[-2])
            or values.get("client_echo_window") != int(parts[-1])
        ):
            return False
    return (
        type(gid) is str
        and re.fullmatch(r"page:echo:[a-f0-9-]{36}(?::[a-f0-9-]{36}:[0-9]{1,4}:[0-9]{1,4})?", gid)
        is not None
        and type(values) is dict
        and values.get("client_echo_schema") == 1
        and values.keys() <= ECHO_METRICS.keys()
        and all(client_metric_valid(k, v) for k, v in values.items())
    )


def capability_values_valid(gid, values):
    return (
        type(gid) is str
        and re.fullmatch(r"page:cap:[a-f0-9-]{36}", gid) is not None
        and type(values) is dict
        and values.get("client_cap_schema") == 1
        and bool(values)
        and values.keys() <= CAPABILITY_METRICS.keys()
        and all(client_metric_valid(k, v) for k, v in values.items())
    )


def client_metric_valid(key, value):
    if key in ECHO_METRICS:
        return (
            type(value) is int
            and (1 if key == "client_echo_schema" else 0) <= value <= ECHO_METRICS[key]
        )
    if key in PCM_METRICS:
        return type(value) is int and 1 <= value <= PCM_METRICS[key]
    if key in CAPABILITY_METRICS:
        return (
            type(value) is int
            and (1 if key == "client_cap_schema" else 0) <= value <= CAPABILITY_METRICS[key]
        )
    if key in CLIP_REJECT_METRICS:
        return (
            type(value) is int
            and (
                1
                if key in {"client_clip_reject", "client_silent_drop", "client_silent_reason"}
                else 0
            )
            <= value
            <= CLIP_REJECT_METRICS[key]
        )
    if key in CAPTURE_METRICS or key in MODEL_OBSERVATIONS:
        maximum = CAPTURE_METRICS.get(key, 600000)
        return type(value) is int and (1 if key in CAPTURE_STATES else 0) <= value <= maximum
    if key in DELIVERY_STAGES | VAD_FLAGS:
        return type(value) is int and value == 1
    if key in UPLOAD_COUNTS:
        return type(value) is int and 0 <= value <= 4096
    if key in VAD_COUNTS:
        return type(value) is int and 0 <= value <= (65 if key == "client_vad_gaps" else 60000)
    if key in TRANSPORT_STAGES:
        return type(value) is int and 0 <= value <= (2 if key == "client_transport" else 4096)
    return key in CLIENT_STAGES and milliseconds(value, 3600000)


def transport_summary(rows):
    groups = {}
    for row in rows:
        key = (row["call_id"], row["generation_id"])
        item = groups.setdefault(
            key, {"recorded_at": 0, "modes": set(), "disconnects": None, "reconnects": None}
        )
        item["recorded_at"] = max(item["recorded_at"], row.get("recorded_at", 0))
        stage, value = row["stage"], row["elapsed_ms"]
        if stage == "client_transport":
            item["modes"].add(value)
        elif stage in {"client_ws_disconnects", "client_ws_reconnects"}:
            name = "disconnects" if stage == "client_ws_disconnects" else "reconnects"
            item[name] = int(max(item[name] or 0, value))
    output = []
    for item in sorted(groups.values(), key=lambda r: r["recorded_at"], reverse=True)[:8]:
        modes = item["modes"]
        mode = (
            "mixed"
            if 2 in modes or {0, 1} <= modes
            else "websocket"
            if 1 in modes
            else "poll"
            if 0 in modes
            else "unknown"
        )
        output.append(
            dict(mode=mode, disconnects=item["disconnects"], reconnects=item["reconnects"])
        )
    return output


CLIENT_STAGES = {
    *ECHO_METRICS,
    *PCM_METRICS,
    "client_pcm_first",
    "client_pcm_play",
    "speech_end_first_sound",
    *VAD_TIMES,
    *CAPABILITY_METRICS,
    *CAPTURE_METRICS,
    *CLIP_REJECT_METRICS,
    *UPLOAD_TIMES,
    *UPLOAD_COUNTS,
    "client_segment_handoff_ms",
    "client_upload",
    "client_transcript",
    "client_audio_ready",
    "client_download",
    "client_play",
    "first_sound",
}
DURABLE_STAGES = (
    {
        "run",
        "pre_llm",
        "post_llm",
        "llm",
        "tool",
        "stt_queue",
        "stt_call",
        "stt_convert",
        "tts_call",
        "tts_first_chunk",
        *TTS_ENUMS,
        "tts_convert",
        "first_sound",
        "first_sentence",
        "first_audio",
        "history",
        "history_summary",
        "history_chars",
        "series_segments",
        "series_sealed",
        "series_wait_ms",
    }
    | BARGE_SERVER_METRICS.keys()
    | CLIENT_STAGES
    | VAD_FLAGS
    | VAD_COUNTS
    | TRANSPORT_STAGES
    | ERROR_STAGES
    | HOOK_STAGES
    | DELIVERY_STAGES
    | REPLY_COUNT_STAGES
    | {"internal_note_dropped"}
    | CANCELLATION_STAGES
)
TIMING_FIELDS = (
    "stage",
    "sequence",
    "start_ms",
    "elapsed_ms",
    "provider_id",
    "model",
    "selection",
    "tool",
    "prompt_tokens",
    "completion_tokens",
    "reasoning_tokens",
    "history_before",
    "history_after",
    "history_summary",
)


def identifier(value):
    return value if type(value) is str and re.fullmatch(r"[\w./:@+\-]{1,160}", value) else "unknown"


def milliseconds(value, maximum=86_400_000):
    return type(value) in (int, float) and 0 <= value <= maximum and isfinite(value)


def timing_row(value):
    if type(value) is not dict or value.get("stage") not in DURABLE_STAGES:
        return None
    row = dict(value)
    if row["stage"] in BARGE_SERVER_METRICS and (
        type(row.get("elapsed_ms")) is not int
        or not 0 <= row["elapsed_ms"] <= BARGE_SERVER_METRICS[row["stage"]]
    ):
        return None
    if row["stage"] in TTS_ENUMS and (
        type(row.get("elapsed_ms")) is not int
        or not 1 <= row["elapsed_ms"] <= TTS_ENUMS[row["stage"]]
    ):
        return None
    if (
        row["stage"]
        in CAPABILITY_METRICS.keys()
        | PCM_METRICS.keys()
        | ECHO_METRICS.keys()
        | CAPTURE_CODEC_METRICS.keys()
        and not client_metric_valid(row["stage"], row.get("elapsed_ms"))
    ):
        return None
    if (
        type(row.get("sequence")) is not int
        or not 0 <= row["sequence"] <= 4096
        or not milliseconds(row.get("start_ms"))
        or not milliseconds(
            row.get("elapsed_ms"),
            CLIP_REJECT_METRICS["client_clip_bytes"]
            if row["stage"] == "client_clip_bytes"
            else 86400000,
        )
    ):
        return None
    for key in ("provider_id", "model", "tool"):
        row[key] = identifier(row.get(key))
    row["selection"] = (
        row.get("selection") if row.get("selection") in {"inherited", "dedicated"} else "unknown"
    )
    for key in ("prompt_tokens", "completion_tokens", "reasoning_tokens"):
        v = row.get(key)
        row[key] = v if type(v) is int and 0 <= v <= 1_000_000_000 else None
    for key in ("history_before", "history_after", "history_summary"):
        v = row.get(key)
        maximum = 1 if key == "history_summary" else 1_000_000_000
        row[key] = v if type(v) is int and 0 <= v <= maximum else None
    return {key: row[key] for key in TIMING_FIELDS}


class RoundTiming:
    def __init__(self, received_at=None):
        self.started = monotonic_ns()
        self.received_at = received_at
        self.rows = []
        self.first_llm = self.last_llm = None
        self.provider_id = self.model = "unknown"
        self.selection = "inherited"

    def now(self):
        return (monotonic_ns() - self.started) / 1_000_000

    def add(self, stage, start, *, end=None, **values):
        if len(self.rows) >= 4096:
            return
        row = dict(
            stage=stage,
            sequence=0,
            start_ms=start,
            elapsed_ms=(self.now() if end is None else end) - start,
            provider_id=self.provider_id,
            model=self.model,
            selection=self.selection,
            tool="unknown",
        )
        row.update(values)
        self.rows.append(row)
        return row

    def finish(self, stage):
        ended = self.now()
        self.add(stage, 0, end=ended)
        if stage == "run" and self.first_llm is not None:
            self.add("pre_llm", 0, end=self.first_llm)
            self.add("post_llm", self.last_llm, end=ended)


@contextmanager
def measure(stage):
    timing = CURRENT_TIMING.get()
    start = timing.now() if timing else None
    if timing and stage == "stt_call" and timing.received_at is not None:
        queued = (time() - timing.received_at) * 1000
        if milliseconds(queued):
            timing.add("stt_queue", 0, end=queued)
    try:
        yield
    finally:
        if timing:
            timing.add(stage, start)


def timing_summary(rows):
    stages, models = [], {}
    for stage in sorted(
        DURABLE_STAGES
        - CAPABILITY_METRICS.keys()
        - ECHO_METRICS.keys()
        - (TRANSPORT_STAGES - CONNECTION_METRICS.keys())
        - HOOK_STAGES
        - DELIVERY_STAGES
        - REPLY_COUNT_STAGES
        - {"internal_note_dropped"}
        - CANCELLATION_STAGES
        - VAD_FLAGS
        - VAD_COUNTS
        - UPLOAD_COUNTS
        - {"series_segments", "series_sealed"}
    ):
        totals = {}
        for row in rows:
            if row["stage"] == stage:
                key = (row["call_id"], row["generation_id"])
                totals[key] = totals.get(key, 0) + row["elapsed_ms"]
        values = sorted(totals.values())
        n = len(values)
        if n:
            if (
                stage
                in BARGE_SERVER_METRICS.keys()
                | CAPTURE_CODEC_METRICS.keys()
                | CAPTURE_STATES
                | PCM_METRICS.keys()
                | TTS_ENUMS.keys()
                | {"client_clip_reject"}
            ):
                latest = max(
                    (r for r in rows if r["stage"] == stage),
                    key=lambda r: (r["recorded_at"], r.get("sequence", 0)),
                )
                stages.append(dict(stage=stage, count=n, latest_value=latest["elapsed_ms"]))
                continue
            stages.append(
                dict(
                    stage=stage,
                    count=n,
                    median_ms=round((values[(n - 1) // 2] + values[n // 2]) / 2, 2),
                    p90_ms=round(values[ceil(n * 0.9) - 1], 2),
                )
            )
    for row in rows:
        if row["stage"] == "llm":
            key = (row["provider_id"], row["model"], row["selection"])
            models[key] = models.get(key, 0) + 1
    return dict(
        capacity=50,
        failures=failure_summary(rows),
        tts_stream_fallbacks=tts_stream_fallback_summary(rows),
        transport=transport_summary(rows),
        hooks=hook_summary(rows),
        reply_style=reply_style_summary(rows),
        vad={
            name: len({(r["call_id"], r["generation_id"]) for r in rows if r["stage"] == name})
            for name in VAD_FLAGS
        },
        vad_gaps={
            name: sum(r["elapsed_ms"] for r in rows if r["stage"] == name) for name in VAD_COUNTS
        },
        series_segments=[int(r["elapsed_ms"]) for r in rows if r["stage"] == "series_segments"],
        upload_chunks={
            "rounds": sum(r["stage"] == "client_upload_chunks" for r in rows),
            "total": sum(r["elapsed_ms"] for r in rows if r["stage"] == "client_upload_chunks"),
        },
        audio_delivery={
            name: len({(r["call_id"], r["generation_id"]) for r in rows if r["stage"] == stage})
            for name, stage in (
                ("push", "client_first_audio_push"),
                ("download", "client_first_audio_download"),
            )
        },
        round_count=len(
            {
                (r["call_id"], r["generation_id"])
                for r in rows
                if not r["generation_id"].startswith(("page:", "capture:"))
            }
        ),
        stages=stages,
        models=[
            dict(provider_id=k[0], model=k[1], selection=k[2], calls=v) for k, v in models.items()
        ],
    )


def hook_summary(rows):
    hooks = [r for r in rows if r["stage"] in HOOK_STAGES]
    latest = max(rows, key=lambda r: r.get("recorded_at", 0), default=None)
    latest_key = None if latest is None else (latest["call_id"], latest["generation_id"])
    groups = {}
    for row in hooks:
        key = (row["call_id"], row["generation_id"])
        group = groups.setdefault(
            row["provider_id"], {"totals": {}, "count": 0, "errors": 0, "skipped": 0}
        )
        group["count"] += 1
        group["errors"] += row["stage"] in {"hook_error", "hook_cancelled"}
        group["skipped"] += row["stage"] == "hook_skipped"
        group["totals"][key] = group["totals"].get(key, 0) + row["elapsed_ms"]
    output = []
    for plugin, group in sorted(groups.items()):
        values = sorted(group["totals"].values())
        n = len(values)
        output.append(
            dict(
                plugin=plugin,
                latest_ms=group["totals"].get(latest_key),
                median_ms=round((values[(n - 1) // 2] + values[n // 2]) / 2, 2),
                rounds=n,
                count=group["count"],
                errors=group["errors"],
                skipped=group["skipped"],
            )
        )
    return output


def reply_style_summary(rows):
    counts = {}
    for row in rows:
        if row["stage"] in REPLY_COUNT_STAGES:
            counts.setdefault((row["call_id"], row["generation_id"]), {})[row["stage"]] = row[
                "elapsed_ms"
            ]
    complete = [v for v in counts.values() if REPLY_COUNT_STAGES <= v.keys()]

    def median(stage):
        values = sorted(v[stage] for v in complete)
        n = len(values)
        return (values[(n - 1) // 2] + values[n // 2]) / 2 if n else None

    return dict(
        rounds=len(complete),
        chars_median=median("reply_chars"),
        sentences_median=median("reply_sentences"),
    )


def tts_stream_fallback_summary(rows):
    by_code = {code: name for name, code in TTS_STREAM_FALLBACK_REASONS.items()}
    counts = {}
    for row in rows:
        if row.get("stage") == "tts_stream_fallback":
            reason = by_code.get(row.get("elapsed_ms"), "other")
            counts[reason] = counts.get(reason, 0) + 1
    return counts
