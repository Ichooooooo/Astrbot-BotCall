"""Manual, memory-only provider probes. Never enters an Agent or conversation."""

import asyncio
from pathlib import Path
import re
import shutil
from time import monotonic, time
from urllib.parse import urlsplit

from .benchmark import STAGE_DEFS, baseline, hints


def infer_thinking(config):
    """Only explicit configuration; conflicting evidence remains unknown."""
    from collections.abc import Mapping

    if not isinstance(config, Mapping):
        return "unknown"
    states = set()

    def read(body):
        if not isinstance(body, Mapping):
            return
        for key in ("reasoning", "enable_thinking"):
            value = body.get(key)
            if type(value) is bool:
                states.add("on" if value else "off")
        thinking = body.get("thinking")
        if isinstance(thinking, Mapping):
            value = thinking.get("type")
            if value in ("enabled", "disabled"):
                states.add("on" if value == "enabled" else "off")
        effort = body.get("reasoning_effort")
        if effort in ("none", "minimal", "low", "medium", "high", "xhigh"):
            states.add("off" if effort == "none" else "on")
        reasoning = body.get("reasoning")
        if isinstance(reasoning, Mapping):
            read({"reasoning_effort": reasoning.get("effort")})

    read(config)
    for key in ("extra_body", "custom_extra_body"):
        read(config.get(key))
    return next(iter(states)) if len(states) == 1 else "unknown"


def provider_config(provider, key, method):
    from .native_provider import provider_info
    from .astrbot_compat import tts_stream_settings

    try:
        _, model, _ = provider_info(provider, method)
        result = dict(label=model, model=model)
        if key == "llm":
            result["thinking"] = infer_thinking(provider.provider_config)
            result["label"] += {"off": "（关闭思考）", "on": "（开启思考）", "unknown": ""}[
                result["thinking"]
            ]
        elif key == "tts":
            result["streaming"] = tts_stream_settings(provider) is not None
        return result
    except Exception:
        return None


REASON_LABELS = {
    "model_failed": "回顾模型调用失败",
    "outcome_uncertain": "回顾结果未知（可显式重试）",
    "complete": "回顾已生成",
    "scope_required": "历史保留（未自动处理）",
    "unresolved": "等待处理",
    "resolving": "核对来源策略",
    "ready": "可处理",
    "unavailable": "来源不可用",
    "mismatch": "策略不一致",
    "counter_unavailable": "计数器不可用",
    "invalid_input": "冻结材料校验失败",
    "budget_too_small": "输入预算不足",
    "counter_failed": "计数失败",
    "counter_changed": "计数规则变化",
    "persistence_unconfirmed": "存储结果未确认",
    "processing_unconfirmed": "处理结果未确认",
    "work_uncertain": "生成结果未知，不重放",
    "work_running": "生成仍在途",
    "work_failed": "生成失败",
    "work_cancelled": "生成已取消",
    "work_blocked": "合并预算不足",
}


class BenchmarkController:
    def __init__(self, host):
        self.host = host
        self.last_run = None
        self.running = False
        self.last_started = None
        self.task = None
        self.retrying = set()
        self.hanging_up = set()

    async def providers(self):
        n = self.host._native
        binding = self.host._backend.diagnostic_binding() if self.host._backend else None
        umo = binding.unified_msg_origin if binding else None
        if umo is None and self.host._backend:
            # After a reload no call is bound in memory yet; STT selection is per source session.
            try:
                umo = await self.host._backend._runtime.benchmark_origin()
            except Exception:
                umo = None
        result = {}
        if n is None:
            return result
        from .astrbot_compat import dedicated_chat_provider, using_provider

        try:
            result["stt"] = (await n.check_stt(umo))[0]
        except Exception:
            pass
        try:
            pid = n.settings.get("call_chat_provider_id", "")
            result["llm"] = (
                dedicated_chat_provider(n.context, pid)
                if pid
                else (await using_provider(n.context, umo) if umo else None)
            )
        except Exception:
            pass
        try:
            result["tts"] = await n.check_tts(umo)
        except Exception:
            pass
        return {k: v for k, v in result.items() if v is not None}

    async def get(self):
        h = self.host
        data = (
            await h._backend._runtime.benchmark_snapshot()
            if h._backend
            else dict(calls=[], summary=dict(calls=0, stages={}))
        )
        data.setdefault("active_calls", [])
        config = h._config
        providers = await self.providers()
        current = {
            key: provider_config(providers.get(key), key, method)
            for key, method in (("stt", "get_text"), ("llm", "text_chat"), ("tts", "get_audio"))
        }
        current.update(
            access="固定 HTTPS" if config.get("access_mode") == "fixed_https" else "临时隧道",
            silence_ms=config.get("call_vad_silence_ms", 1400),
            capture=config.get("call_capture_mode", "auto"),
            barge_in=config.get("call_barge_in", "auto"),
        )
        if current["stt"] is not None:
            current["stt"]["selection"] = (
                "dedicated"
                if h._native and h._native.settings.get("call_stt_provider_id")
                else "inherited"
            )
        standard = baseline()
        if current["llm"] is not None:
            base_llm = standard["config"]["llm"]
            aliases = [base_llm["model"], *base_llm.get("model_aliases", [])]
            model = current["llm"]["model"]
            current["llm"]["compare_model"] = (
                base_llm["model"] if model.casefold() in {a.casefold() for a in aliases} else model
            )
        aggregate = dict(
            access=config.get("access_mode"),
            ws_disconnects=sum(c["flags"]["ws_disconnects"] for c in data["calls"]),
            model_fetch_ms=max((c["flags"]["model_fetch_ms"] for c in data["calls"]), default=0),
            tts_stream_attempts=sum(
                c["flags"].get("tts_stream_attempts", 0) for c in data["calls"]
            ),
            tts_stream_fallbacks=sum(
                c["flags"].get("tts_stream_fallbacks", 0) for c in data["calls"]
            ),
            tts_stream_fallback_calls=sum(
                1
                for c in data["calls"]
                if c["flags"].get("tts_stream_frequent_fallback")
                or (
                    c["flags"].get("tts_stream_fallbacks", 0) * 2
                    > c["flags"].get("tts_stream_attempts", 0)
                )
            ),
        )
        usage = data.pop("_reasoning_usage", None)
        if usage:
            reasoning, completion = usage
            aggregate["reasoning_ratio"] = min(1.0, reasoning / completion)
        modes = {c["flags"].get("capture") for c in data["calls"]} - {None}
        if modes:
            aggregate["capture"] = next(iter(modes)) if len(modes) == 1 else "mixed"
        for c in data["calls"]:
            c["hints"] = hints(c["stages"], c["flags"], standard)
        reviews = (
            await h._backend._runtime.benchmark_reviews()
            if h._backend
            else dict(pending_count=0, reasons={}, calls=[], truncated=False)
        )
        reviews["reason_labels"] = REASON_LABELS.copy()
        llm = current["llm"]
        origin = h._native.settings.get("origin", "") if h._native else ""
        status = dict(
            version=h._version,
            plugin_ready=h.ready,
            https_ready=bool(h.ready and origin.startswith("https://")),
            page_url=origin + "/bot-call/" if origin else None,
            access=current["access"],
            call_model=dict(
                selection="dedicated" if config.get("call_chat_provider_id") else "source",
                model=llm["model"] if llm else None,
                available=llm is not None,
            ),
        )
        if getattr(h, "_startup_error", None):
            status["error"] = h._startup_error
        data.update(
            status=status,
            reviews=reviews,
            schema=1,
            stage_defs=STAGE_DEFS,
            baseline=standard,
            current_config=current,
            hints=hints(data["summary"]["stages"], aggregate, standard, current["silence_ms"]),
            last_run=self.last_run,
        )
        return data

    async def force_hangup(self, body):
        messages = {
            "disabled": "插件尚未启用或服务未就绪",
            "not_found": "找不到这通通话",
            "already_closing": "这通电话正在收尾",
            "running": "这通电话正在挂断",
        }

        def error(code):
            return dict(status="error", message=f"{messages[code]}（{code}）", data=dict(code=code))

        h = self.host
        if not h.ready or not h._config.get("enabled", False) or h._terminating:
            return error("disabled")
        if (
            type(body) is not dict
            or set(body) != {"action", "ref"}
            or body["action"] != "force_hangup"
            or type(body["ref"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", body["ref"]) is None
        ):
            return error("not_found")
        ref = body["ref"]
        if ref in self.hanging_up:
            return error("running")
        self.hanging_up.add(ref)
        try:
            result = await h._backend.force_end_call(ref)
            if "code" in result:
                return error(result["code"])
            return dict(status="ok", data=dict(ref=ref, **result))
        finally:
            self.hanging_up.discard(ref)

    async def retry_review(self, body):
        def error(code, message):
            return dict(status="error", message=f"{message}（{code}）", data=dict(code=code))

        h = self.host
        if not h.ready or not h._config.get("enabled", False) or h._terminating:
            return error("disabled", "插件尚未启用或服务未就绪")
        if (
            type(body) is not dict
            or set(body) - {"action", "ref", "preview", "send"}
            or type(body.get("ref")) is not str
            or type(body.get("preview", True)) is not bool
            or type(body.get("send", False)) is not bool
        ):
            return error("not_found", "无法定位回顾")
        ref = body["ref"]
        if ref in self.retrying:
            return error("running", "该通回顾正在处理")
        self.retrying.add(ref)
        try:
            runtime = h._backend._runtime
            call_id = await runtime.resolve_review_ref(ref)
            if call_id is None:
                return error("not_found", "未找到该通回顾")
            if await runtime.active_call_count():
                return error("busy", "通话进行中，请挂断后重试")
            result = await h._backend.retry_review(
                call_id,
                counters=h._native.counters,
                preview=body.get("preview", True),
                context_only=not body.get("send", False),
            )
            if "code" in result:
                code = result["code"]
                return error(
                    code,
                    {
                        "running": "该通回顾正在处理",
                        "busy": "通话进行中",
                        "not_retryable": "该回顾无法重试",
                    }.get(code, "无法重试"),
                )
            return dict(status="ok", data=dict(ref=ref, **result))
        except Exception:
            return error("not_retryable", "来源模型或冻结材料不可用，请检查配置")
        finally:
            self.retrying.discard(ref)

    async def selfcheck(self, providers):
        h, cfg = self.host, self.host._config
        rows = []

        def add(key, label, state, detail):
            rows.append(dict(key=key, label=label, state=state, detail=detail))

        add(
            "host",
            "通话服务",
            "ok" if h.ready else "fail",
            "服务已就绪" if h.ready else "请启用插件并检查启动诊断",
        )
        origin = cfg.get("public_origin", "")
        fixed = cfg.get("access_mode") == "fixed_https"
        valid = (
            bool(urlsplit(origin).scheme == "https" and urlsplit(origin).hostname)
            if fixed
            else bool(h._tunnel and getattr(h._tunnel, "origin", None))
        )
        add(
            "access",
            "接入方式与公开地址",
            "ok" if valid else "warn",
            "公开地址已配置" if valid else "请检查接入方式与 HTTPS 公开地址",
        )
        if fixed:
            rejection = h._settings.rejections.latest
            add(
                "access_rejection",
                "最近接入拒绝",
                "warn" if rejection else "ok",
                "最近存在拒绝，请查看控制面板接入诊断" if rejection else "未记录接入拒绝",
            )
        else:
            executable = cfg.get("cloudflared_path", "cloudflared")
            add(
                "cloudflared",
                "临时隧道程序",
                "ok" if shutil.which(executable) else "fail",
                "程序可用" if shutil.which(executable) else "请配置可执行的 cloudflared",
            )
            add(
                "tunnel",
                "临时隧道状态",
                "ok" if valid else "warn",
                "隧道已就绪" if valid else "隧道尚未就绪",
            )
        add(
            "ffmpeg",
            "音频转换",
            "ok" if shutil.which("ffmpeg") else "fail",
            "ffmpeg 可用" if shutil.which("ffmpeg") else "请安装 ffmpeg 并加入 PATH",
        )
        for key, label in (("stt", "语音识别"), ("llm", "电话模型"), ("tts", "语音合成")):
            add(
                key,
                label,
                "ok" if key in providers else "fail",
                "已配置"
                if key in providers
                else "请在 AstrBot 配置对应服务，来源选择需先建立电话绑定",
            )
        vendor = Path(__file__).parent / "static/vendor/vad-v5-ort-1.22.0-2623a295"
        valid = all(
            (vendor / name).is_file()
            for name in (
                "silero_vad_v5.onnx",
                "ort.wasm.min.js",
                "ort-wasm-simd-threaded.mjs",
                "ort-wasm-simd-threaded.wasm",
            )
        )
        add(
            "vad",
            "人声检测资源",
            "ok" if valid else "fail",
            "资源完整" if valid else "请重新安装完整插件包",
        )
        try:
            from .astrbot_compat import astrbot_version_status

            status, version = astrbot_version_status()
        except Exception:
            status = "unsupported"
        if status == "verified":
            state, detail = "ok", "版本受支持"
        elif status == "unverified_newer":
            state = "warn"
            detail = (
                f"当前版本 {version} 尚未逐项验证，已按能力探测放行；"
                "如遇异常请查看日志或使用原生插件设置页"
            )
        else:
            state, detail = "fail", "请使用受支持的 AstrBot 版本"
        add("astrbot", "AstrBot 版本", state, detail)
        return rows

    async def probe(self, key, provider):
        """Return first-chunk milliseconds, a fallback detail, or None."""
        n = self.host._native
        with n.privacy.scope():
            if key == "stt":
                import tempfile
                from .astrbot_audio import convert_stt_audio

                with tempfile.TemporaryDirectory(prefix="bot-call-benchmark-") as tmp:
                    output = str(Path(tmp) / "sample.wav")
                    await convert_stt_audio(
                        str(Path(__file__).parent / "static/fixed-sample.wav"),
                        output_format="wav",
                        output_path=output,
                    )
                    value = await provider.get_text(output)
                    if not isinstance(value, str) or not value.strip():
                        raise ValueError
            elif key == "llm":
                value = await provider.text_chat(
                    prompt="请只回复：测试成功。", contexts=[], system_prompt="", func_tool=None
                )
                if not getattr(value, "completion_text", "") or getattr(
                    value, "tools_call_name", None
                ):
                    raise ValueError
            else:
                from .astrbot_compat import tts_stream_settings
                from .streaming_tts import StreamFailure, stream_sentence

                settings = tts_stream_settings(provider)
                if settings is not None:
                    first = None
                    started = monotonic()

                    async def emit(chunk):
                        nonlocal first
                        if first is None and chunk:
                            first = (monotonic() - started) * 1000

                    try:
                        await stream_sentence("测试成功。", settings, emit)
                    except StreamFailure as error:
                        if error.received:
                            raise
                    else:
                        if first is None:
                            raise ValueError
                        return first
                value = await provider.get_audio("测试成功。")
                if not value:
                    raise ValueError
                if settings is not None:
                    return "流式合成不可用，已回退整句合成（固定样本调用成功）"
        return None

    async def run(self):
        h = self.host

        def error(code, message):
            # The code also rides in the message: the page bridge may forward only the message.
            return dict(status="error", message=f"{message}（{code}）", data=dict(code=code))

        if self.running:
            return error("running", "测试正在运行")
        if not h.ready or not h._config.get("enabled", False) or h._terminating:
            return error("disabled", "插件尚未启用或服务未就绪")
        # Set before await so simultaneous requests cannot both pass.
        self.running = True
        self.task = asyncio.current_task()
        try:
            if await h._backend._runtime.active_call_count():
                return error("busy", "通话进行中，请挂断后测试")
            now = monotonic()
            if self.last_started is not None and now - self.last_started < 30:
                return error("rate_limited", "两次测试请间隔至少 30 秒")
            self.last_started = now
            result = dict(at=time(), ok=False, duration_ms=0, tests=[], selfcheck=[])
            try:
                async with asyncio.timeout(30):
                    providers = await self.providers()
                    result["selfcheck"] = await self.selfcheck(providers)
                    for key, label in (
                        ("stt", "语音识别"),
                        ("llm", "对话模型"),
                        ("tts", "语音合成"),
                    ):
                        row = dict(key=key, label=label, state="skip", ms=None, detail="未配置服务")
                        result["tests"].append(row)
                        if key not in providers:
                            continue
                        started = monotonic()
                        try:
                            async with asyncio.timeout(9):
                                outcome = await self.probe(key, providers[key])
                            detail = (
                                "已返回非空识别结果（未校对原文）"
                                if key == "stt"
                                else "固定样本调用成功"
                            )
                            if isinstance(outcome, str):
                                detail = outcome
                                outcome = None
                            elapsed = (
                                outcome if outcome is not None else (monotonic() - started) * 1000
                            )
                            row.update(state="ok", ms=round(elapsed, 2), detail=detail)
                        except Exception:
                            row.update(
                                state="fail", detail="服务调用失败或超时，请检查对应服务诊断"
                            )
            except TimeoutError:
                pass
            existing = {r["key"] for r in result["tests"]}
            for key, label in (("stt", "语音识别"), ("llm", "对话模型"), ("tts", "语音合成")):
                if key not in existing:
                    result["tests"].append(
                        dict(
                            key=key, label=label, state="skip", ms=None, detail="总时限已到，未调用"
                        )
                    )
            result["duration_ms"] = round((monotonic() - now) * 1000, 2)
            result["ok"] = all(r["state"] == "ok" for r in result["tests"]) and not any(
                r["state"] == "fail" for r in result["selfcheck"]
            )
            self.last_run = result
            return dict(status="ok", data=result)
        finally:
            self.running = False
            self.task = None
