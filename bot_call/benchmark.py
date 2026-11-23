"""Read-only latency projection shared by management and the baseline CLI."""

from collections import defaultdict
import hashlib
import json
from math import ceil, isfinite
from pathlib import Path
from statistics import median
import sqlite3

from .access import session_reference
from .observability import TTS_STREAM_FALLBACK_REASONS

STAGE_DEFS = [
    dict(key=k, label=l, desc=d)
    for k, l, d in (
        ("endpoint", "判停等待", "说完后等待静音确认的时间，主要由判停时长决定"),
        ("upload", "上传", "录音从手机传到服务器"),
        ("stt", "语音识别", "排队、转码与 STT 服务调用"),
        ("llm", "对话模型", "模型生成第一句回复"),
        ("tts", "语音合成", "第一句合成出首段音频"),
        ("delivery", "下发与起播", "音频送达手机并开始播放"),
        ("total", "说完到首声", "从你说完到听到第一声的总时长"),
    )
]


def baseline():
    return json.loads(Path(__file__).with_name("benchmark_baseline.json").read_text())


def stats(values):
    values = sorted(values)
    return dict(
        median_ms=round(median(values), 2), p90_ms=round(values[ceil(len(values) * 0.9) - 1], 2)
    )


def round_stages(rows, members=()):
    """One sample per reply; capture STT is summed, final capture uploads only."""
    groups = defaultdict(list)
    mapping = {r["utterance_id"]: r["series_id"] for r in members if r["series_id"]}
    for row in rows:
        gid = row["generation_id"]
        if gid.startswith("reply:"):
            groups[gid].append(row)
        elif gid.startswith("capture:") and gid[8:] in mapping:
            groups["reply:" + mapping[gid[8:]]].append(row)
    output = []
    for gid, group in groups.items():
        # A capture-only/abandoned series is not a reply round.
        if not any(r["generation_id"] == gid for r in group):
            continue
        by = defaultdict(list)
        for r in group:
            if r["stage"].startswith("client_") and r["generation_id"] != gid:
                continue
            if r["stage"] in {"tts_call", "tts_convert", "tts_first_chunk"} and r["sequence"] != 1:
                continue
            v = r["elapsed_ms"]
            if isinstance(v, (int, float)) and isfinite(v) and v >= 0:
                by[r["stage"]].append(r)

        def first(key):
            rs = by[key]
            return (
                min(rs, key=lambda r: (r["sequence"], r["start_ms"]))["elapsed_ms"] if rs else None
            )

        def complete(keys):
            return sum(first(k) for k in keys) if all(by[k] for k in keys) else None

        stages = dict(
            endpoint=first("client_endpoint_quiet_ms"),
            upload=first("client_upload"),
            llm=first("first_sentence"),
            total=first("speech_end_first_sound"),
        )
        # Only sum STT observations, never client transcript wall time.
        if all(by[k] for k in ("stt_queue", "stt_call")) and {
            r["generation_id"] for r in by["stt_queue"]
        } == {r["generation_id"] for r in by["stt_call"]}:
            stages["stt"] = sum(r["elapsed_ms"] for k in ("stt_queue", "stt_call") for r in by[k])
        stages["tts"] = (
            first("tts_first_chunk")
            if by["tts_first_chunk"]
            else complete(("tts_call", "tts_convert"))
        )
        stages["delivery"] = (
            first("client_pcm_play")
            if by["client_pcm_play"]
            else complete(("client_download", "client_play"))
        )
        output.append({k: v for k, v in stages.items() if v is not None})
    return output


def hints(stages, flags, standard, silence_ms=None):
    result = []

    def add(stage, level, text):
        result.append(dict(stage=stage, level=level, text=text))

    for key in (r["key"] for r in STAGE_DEFS):
        value = stages.get(key, {}).get("median_ms")
        target = standard["stages"].get(key, {}).get("median_ms", 0)
        if value is not None and target > 0 and value > 2 * target:
            add(
                key,
                "warn",
                f"{next(r['label'] for r in STAGE_DEFS if r['key'] == key)}比标准慢约 {value / target:.1f} 倍，请检查网络或对应服务配置。",
            )
    if (
        flags.get("access") == "quick_tunnel"
        and stages.get("upload", {}).get("median_ms", 0)
        > standard["stages"]["upload"]["median_ms"] * 2
    ):
        add(
            "upload",
            "info",
            "临时隧道可能增加上传耗时，可按使用手册中的“固定 HTTPS 配置”一节配置后对比。",
        )
    if flags.get("capture") in {"legacy", "mixed"}:
        add("upload", "info", "录音使用或退回旧方式，请检查浏览器录音能力与录音方式设置。")
    if flags.get("ws_disconnects", 0) >= 3:
        add("delivery", "warn", "推送连接断开较多，请检查网络稳定性并保持页面在前台。")
    if flags.get("model_fetch_ms", 0) >= 5000:
        add(
            "endpoint",
            "info",
            "人声模型首次下载较慢，首次访问属于正常准备，之后同源访问会使用缓存。",
        )
    ratio = flags.get("reasoning_ratio", 0)
    # 2026-11-18 reference: 93–97% reasoning took 6.2–9.8 s to the first sentence.
    # A comparable model with thinking disabled took about 1.1 s. Warn at 70%,
    # or at 50% when the first sentence takes at least 2.5 s (over twice that reference).
    if ratio >= 0.7 or (ratio >= 0.5 and stages.get("llm", {}).get("median_ms", 0) >= 2500):
        add(
            "llm",
            "warn",
            f"对话模型约 {round(ratio * 100)}% 的输出花在思考过程上，会明显拉慢开口速度。"
            "建议在控制面板为电话单独指定关闭思考的快速模型。",
        )
    stream_attempts = flags.get("tts_stream_attempts", 0)
    stream_fallbacks = flags.get("tts_stream_fallbacks", 0)
    if (
        flags.get("tts_stream_frequent_fallback")
        or flags.get("tts_stream_fallback_calls", 0) > 0
        or (stream_attempts > 0 and stream_fallbacks * 2 > stream_attempts)
    ):
        add(
            "tts",
            "warn",
            "流式合成频繁失败，已改用整句合成，首声会变慢；常见原因是服务器需经代理上网、无法直连语音合成服务，或当前服务/模型不支持 24 kHz 流式输出",
        )
    if silence_ms is not None and silence_ms != standard["config"]["silence_ms"]:
        add(
            "total",
            "info",
            f"当前判停时长 {silence_ms} ms，标准为 {standard['config']['silence_ms']} ms，会影响说完到首声；各段不能直接相加。",
        )
    return result


def snapshot(db, *, limit=30, call_ids=None):
    """Only metadata/timings are read; no transcript, audio or credentials."""
    db.row_factory = sqlite3.Row
    if call_ids:
        calls = db.execute(
            "SELECT call_id,created_at FROM calls WHERE call_id IN ("
            + ",".join("?" for _ in call_ids)
            + ") ORDER BY created_at DESC",
            call_ids,
        ).fetchall()
    else:
        calls = db.execute(
            "SELECT call_id,created_at FROM calls ORDER BY created_at DESC,rowid DESC LIMIT ?",
            (limit,),
        ).fetchall()
    projected, samples = [], defaultdict(list)
    total_completion = total_reasoning = 0
    for call in calls:
        cid = call["call_id"]
        # Read timing metadata including usage when present. The read-only baseline CLI
        # can also take snapshots of databases that only have latency measurements.
        rows = [dict(r) for r in db.execute("SELECT * FROM turn_timings WHERE call_id=?", (cid,))]
        members = [
            dict(r)
            for r in db.execute(
                "SELECT utterance_id,series_id FROM audio_jobs WHERE call_id=?", (cid,)
            )
        ]
        rounds = round_stages(rows, members)
        stages = {}
        for key in (r["key"] for r in STAGE_DEFS):
            values = [r[key] for r in rounds if key in r]
            if values:
                stages[key] = dict(
                    median_ms=round(median(values), 2), max_ms=round(max(values), 2), n=len(values)
                )
                samples[key].extend(values)
        modes = {r["elapsed_ms"] for r in rows if r["stage"] == "client_capture_mode"}

        def maximum(stage):
            return max((r["elapsed_ms"] for r in rows if r["stage"] == stage), default=0)

        flags = dict(
            barge_candidates=int(maximum("client_barge_candidates")),
            ws_disconnects=int(maximum("client_ws_disconnects")),
            model_fetch_ms=maximum("client_model_fetch_ms"),
        )
        if modes:
            flags["capture"] = "mixed" if len(modes) > 1 else ("auto" if 1 in modes else "legacy")
        completion = reasoning = 0
        reasoning_turns = set()
        for r in rows:
            if r["stage"] != "llm" or not r["generation_id"].startswith("reply:"):
                continue
            output_tokens, thinking_tokens = r.get("completion_tokens"), r.get("reasoning_tokens")
            if (
                type(output_tokens) is int
                and output_tokens > 0
                and type(thinking_tokens) is int
                and thinking_tokens >= 0
            ):
                completion += output_tokens
                reasoning += thinking_tokens
                if thinking_tokens:
                    reasoning_turns.add(r["generation_id"])
        total_completion += completion
        total_reasoning += reasoning
        if reasoning_turns:
            flags.update(
                reasoning_ratio=round(min(1.0, reasoning / completion), 3),
                reasoning_turns=len(reasoning_turns),
            )

        # Count rounds eligible for streaming, including those that bypassed an open circuit.
        stream_attempts = {
            r["generation_id"]
            for r in rows
            if r["generation_id"].startswith("reply:")
            and r["stage"] in {"tts_first_chunk", "tts_stream_fallback"}
        }
        stream_fallbacks = {
            r["generation_id"]
            for r in rows
            if r["generation_id"].startswith("reply:") and r["stage"] == "tts_stream_fallback"
        }
        if stream_attempts:
            flags["tts_stream_attempts"] = len(stream_attempts)
            flags["tts_stream_fallbacks"] = len(stream_fallbacks)
        if any(
            r["generation_id"].startswith("reply:")
            and r["stage"] == "tts_stream_fallback"
            and r["elapsed_ms"] == TTS_STREAM_FALLBACK_REASONS["circuit_open"]
            for r in rows
        ):
            flags["tts_stream_frequent_fallback"] = True
        end = db.execute("SELECT ended_at FROM reviews WHERE call_id=?", (cid,)).fetchone()
        projected.append(
            dict(
                ref=session_reference(hashlib.sha256(cid.encode()).hexdigest()),
                started_at=call["created_at"],
                duration_s=max(0, end[0] - call["created_at"]) if end else 0,
                turns=len(rounds),
                stages=stages,
                flags=flags,
            )
        )
    data = dict(
        calls=projected,
        summary=dict(calls=len(projected), stages={k: stats(v) for k, v in samples.items()}),
    )
    if total_reasoning:
        data["_reasoning_usage"] = (total_reasoning, total_completion)
    return data
