"""One schema, startup/panel validation and plugin-owned configuration writes."""

from copy import deepcopy
from functools import lru_cache
import hashlib
import json
import logging
from pathlib import Path
from urllib.parse import urlsplit

# These presentation properties have no native AstrBot schema equivalents.
GROUPS = (
    (
        "access",
        "接入",
        "plug",
        (
            (
                "entry",
                "入口",
                (
                    "enabled",
                    "access_mode",
                    "public_origin",
                    "listen_port",
                    "listen_host",
                    "trusted_proxies",
                ),
            ),
            ("tunnel", "临时隧道", ("cloudflared_path", "tunnel_start_timeout")),
        ),
    ),
    (
        "conversation",
        "对话",
        "chat",
        (
            (
                "reply",
                "通话模型与回复",
                ("call_chat_provider_id", "call_reply_style", "call_skip_plugin_hooks"),
            ),
            (
                "context",
                "上下文",
                ("call_history_turns", "call_history_summary", "call_history_max_chars"),
            ),
        ),
    ),
    (
        "audio",
        "语音与断句",
        "mic",
        (
            (
                "vad",
                "合成与自动断句",
                (
                    "call_stt_provider_id",
                    "call_tts_provider_id",
                    "call_capture_mode",
                    "call_barge_in",
                    "call_backchannel_phrases",
                    "call_vad_silence_ms",
                    "call_vad_sensitivity",
                ),
            ),
            (
                "limits",
                "录音限制",
                ("max_audio_bytes", "max_wav_bytes", "call_max_continuous_segments"),
            ),
            ("retention", "音频留存", ("audio_retention_days",)),
        ),
    ),
    (
        "review",
        "回顾",
        "note",
        (
            (
                "style",
                "回顾写法",
                ("review_style", "review_voice", "review_system_notes", "review_instructions"),
            ),
            ("length", "篇幅", ("review_tokens", "safety_tokens")),
        ),
    ),
)
CHOICE_LABELS = {
    "fixed_https": "固定 HTTPS",
    "quick_tunnel": "临时隧道",
    "auto": "自动（保留起声）",
    "legacy": "兼容录音",
}
REVIEW_CHOICES = {
    "review_style": {
        "default": (
            "详细叙述（覆盖开头、中段、结尾）",
            "按通话顺序详细记录，区分表达、建议、决定和未定事项；短通话约500字、尽量不超过800字。",
            "先聊了周末安排，接着讨论天气，最后仍未决定去处。",
        ),
        "bullets": (
            "一句概括＋要点",
            "一句概括，再列三至五条要点；确有决定、待办或未定事项时才单列。",
            "聊了周末安排。\n- 周五再决定\n- 我建议先看天气",
        ),
        "brief": (
            "口语小结",
            "像挂完电话顺手发的一两段消息。",
            "刚才聊了周末去哪儿，你想等周五看天气再决定。",
        ),
        "sections": (
            "分节详细",
            "按概括、内容、决定、待办和未定分节。",
            "概括：讨论周末安排。\n未定：具体去哪儿。",
        ),
    },
    "review_voice": {
        "default": (
            "客观回顾（不限定人称）",
            "客观叙述用户表达、Bot建议与决定；不固定称呼或人称。",
            "本次围绕周末安排展开讨论，最后仍未确定去处。",
        ),
        "bot": (
            "Bot 对用户说",
            "延续来源人格，由 Bot 直接对你说。",
            "我们聊了周末去哪儿，你还没决定；我建议先看天气。",
        ),
        "neutral": (
            "中立纪要（用户／助手）",
            "明确用‘用户’‘助手’指代双方，不用‘我们’；角色称呼比客观回顾更固定。",
            "用户尚未决定去处，助手建议先看天气。",
        ),
    },
    "review_system_notes": {
        "default": (
            "完整说明（含播放与运行限制）",
            "保留材料缺失、播放是否听完及工具运行结果的限制说明，不据此认定用户接受或事务完成。",
            "播放记录不代表用户听完，运行结束也不表示事务已完成。",
        ),
        "on_issue": (
            "只在出问题时一句说明",
            "正常不写；确有材料问题时结尾一句说明。",
            "有1段录音没识别出来。",
        ),
        "none": ("不写系统说明", "不附加系统说明，事实中的未定状态仍保留。", "你还没决定去哪儿。"),
    },
}
RELOAD_HINT = "请在 AstrBot 插件管理里重载 Bot-Call"


@lru_cache(maxsize=1)
def schema():
    return json.loads((Path(__file__).parent.parent / "_conf_schema.json").read_text())


class SettingsInvalid(ValueError):
    def __init__(self, errors):
        super().__init__("bot_call.settings_invalid")
        self.errors = errors


def validate_config(config, *, require_ready=False):
    """Unknown legacy keys are retained by callers, never accepted as panel writes."""
    values, errors = {}, {}
    for key, field in schema().items():
        value = deepcopy(config.get(key, field["default"]))
        values[key] = value
        expected = {"int": int, "bool": bool, "string": str, "text": str, "list": list}[
            field["type"]
        ]
        if type(value) is not expected:
            errors[key] = "类型不正确"
            continue
        if "options" in field and value not in field["options"]:
            errors[key] = "请选择列出的选项"
        if expected is int:
            limits = field["slider"]
            if not limits["min"] <= value <= limits["max"]:
                errors[key] = f"请输入 {limits['min']} 至 {limits['max']} 的整数"
        if expected is str:
            try:
                if key in {"review_instructions", "call_reply_style"} and (
                    len(value) > 2000 or len(value.encode("utf-8")) > 8000
                ):
                    errors[key] = "最多 2000 字符或 8000 UTF-8 字节"
                else:
                    value.encode("utf-8")
            except UnicodeError:
                errors[key] = "文字编码不正确"
        if key in {"call_chat_provider_id", "call_tts_provider_id", "call_stt_provider_id"} and (
            len(value) > 256 or any(ord(c) < 32 for c in value)
        ):
            errors[key] = "模型 ID 最多 256 字符，不能含控制字符"
        if key == "call_backchannel_phrases":
            from .barge_in import valid_phrases

            if not valid_phrases(value):
                errors[key] = "最多64个短语；归一化后每个1～4个字母或汉字，原条目不超过32字符"
        if key == "call_skip_plugin_hooks" and (
            len(value) > 128
            or any(
                type(v) is not str or not v or len(v) > 160 or any(ord(c) < 32 for c in v)
                for v in value
            )
        ):
            errors[key] = "最多 128 个插件名；每个非空且最多 160 字符，不能含控制字符"
    from .proxy_access import address, proxy_networks

    try:
        host = address(values["listen_host"])
    except ValueError:
        errors["listen_host"] = "请输入 IPv4 或 IPv6 地址字面量（不带方括号、端口或区域标识）"
    try:
        networks = proxy_networks(values["trusted_proxies"])
    except ValueError:
        errors["trusted_proxies"] = (
            "请填写 1～64 个 IP/CIDR，禁止 0.0.0.0/0、::/0 及覆盖全部 IPv4 的映射网段"
        )
    if not errors and values["access_mode"] == "fixed_https" and not host.is_loopback:
        if all(n.network_address.is_loopback and n.broadcast_address.is_loopback for n in networks):
            errors["trusted_proxies"] = (
                "非回环监听需填写实际代理 IP 或专用子网，例如 Docker 网关 172.18.0.1；请先检查实际网关"
            )
    if errors:
        raise SettingsInvalid(errors)
    mode, origin, executable = (
        values[k] for k in ("access_mode", "public_origin", "cloudflared_path")
    )
    if mode == "quick_tunnel" and origin:
        errors["public_origin"] = "临时隧道模式下公开地址必须留空"
    if mode == "fixed_https" and (origin or require_ready or values["enabled"]):
        try:
            parts = urlsplit(origin)
            if (
                parts.scheme != "https"
                or not parts.hostname
                or parts.path
                or parts.query
                or parts.fragment
                or parts.username is not None
                or parts.password is not None
                or parts.netloc != parts.netloc.lower()
                or any(ord(c) <= 32 or ord(c) >= 127 for c in origin)
            ):
                raise ValueError
            parts.port
        except ValueError:
            errors["public_origin"] = (
                "固定模式需要有效的 https://域名[:端口]，不能带路径、凭据或查询参数"
            )
    if (
        not executable
        or executable.startswith("-")
        or any(ord(c) < 32 for c in executable)
        or ("/" in executable and not executable.startswith("/"))
    ):
        errors["cloudflared_path"] = "请填写 PATH 中的名称或可执行文件绝对路径"
    if errors:
        raise SettingsInvalid(errors)
    return values


def revision(values):
    raw = json.dumps(
        values, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class SettingsController:
    def __init__(self, config, context, version):
        self.config, self.context, self.version = config, context, version
        from .proxy_access import Rejections

        self.rejections = Rejections()
        self.applied = deepcopy(dict(config or {}))

    def state(self):
        from .astrbot_compat import plugin_config_state

        raw, reason = plugin_config_state(self.config)
        values = {k: deepcopy(raw.get(k, f["default"])) for k, f in schema().items()}
        try:
            validate_config(raw)
        except SettingsInvalid:
            reason = "配置尚未通过校验，请先在 AstrBot 插件配置页修正"
        return raw, values, reason

    def reload_required(self, values):
        return any(values[k] != self.applied.get(k, f["default"]) for k, f in schema().items())

    async def get(self, active_calls):
        from .astrbot_compat import (
            chat_provider_choices,
            tts_provider_choices,
            stt_provider_choices,
        )

        raw, values, reason = self.state()
        providers = {
            "call_chat_provider_id": chat_provider_choices(self.context),
            "call_tts_provider_id": tts_provider_choices(self.context),
            "call_stt_provider_id": stt_provider_choices(self.context),
        }
        groups = []
        for identity, title, icon, definitions in GROUPS:
            sections = []
            for section_id, section_title, keys in definitions:
                fields = [self._field(key, values, providers) for key in keys]
                sections.append(dict(id=section_id, title=section_title, fields=fields))
            fields = [field for section in sections for field in section["fields"]]
            groups.append(
                dict(id=identity, title=title, icon=icon, sections=sections, fields=fields)
            )
        return dict(
            version=self.version,
            revision=revision(raw),
            writable=reason is None,
            readonly_reason=reason or "",
            reload_required=self.reload_required(values),
            active_calls=active_calls,
            groups=groups,
            reload_hint=RELOAD_HINT,
            last_access_rejection=self.rejections.latest,
        )

    @staticmethod
    def _field(key, values, providers):
        definition = schema()[key]
        options = definition.get("options")
        item = dict(
            key=key,
            label=definition["description"],
            help=definition.get("hint", ""),
            type="choice"
            if options
            or key in {"call_chat_provider_id", "call_tts_provider_id", "call_stt_provider_id"}
            else definition["type"],
            value=values[key],
            default=deepcopy(definition["default"]),
            applies="reload",
        )
        if "slider" in definition:
            item.update(min=definition["slider"]["min"], max=definition["slider"]["max"])
        if options:
            item["choices"] = []
            for value in options:
                if key in REVIEW_CHOICES:
                    label, description, example = REVIEW_CHOICES[key][value]
                    item["choices"].append(
                        dict(value=value, label=label, description=description, example=example)
                    )
                else:
                    item["choices"].append(
                        dict(
                            value=value,
                            label=(
                                {"auto": "自动插话", "off": "关闭插话"}[value]
                                if key == "call_barge_in"
                                else CHOICE_LABELS[value]
                            ),
                        )
                    )
        if key in providers:
            item["choices"] = providers[key]
        return item

    async def post(self, body):
        from .astrbot_compat import (
            save_plugin_config,
            dedicated_chat_provider,
            dedicated_tts_provider,
            dedicated_stt_provider,
        )

        raw, _, reason = self.state()
        if reason:
            return "bot_call.settings_readonly", dict(readonly_reason=reason)
        if (
            type(body) is not dict
            or set(body) != {"revision", "values"}
            or type(body["revision"]) is not str
            or type(body["values"]) is not dict
        ):
            return "bot_call.settings_invalid", dict(
                errors={"_request": "请求格式不正确，请刷新后重试"}
            )
        if body["revision"] != revision(raw):
            return "bot_call.settings_conflict", {}
        edits = body["values"]
        errors = {k: "不支持的配置项" for k in edits if k not in schema()}
        if errors:
            return "bot_call.settings_invalid", dict(errors=errors)
        merged = deepcopy(raw)
        merged.update(edits)
        try:
            values = validate_config(merged)
            if values["access_mode"] == "quick_tunnel":
                from shutil import which

                if which(values["cloudflared_path"]) is None:
                    raise SettingsInvalid(
                        {
                            "cloudflared_path": "未找到 cloudflared，请先在 AstrBot 运行环境安装，或改用固定 HTTPS"
                        }
                    )
            if "call_stt_provider_id" in edits and values["call_stt_provider_id"]:
                try:
                    dedicated_stt_provider(self.context, values["call_stt_provider_id"])
                except Exception:
                    raise SettingsInvalid(
                        {"call_stt_provider_id": "请选择当前已启用的语音识别服务"}
                    ) from None
            if "call_tts_provider_id" in edits and values["call_tts_provider_id"]:
                try:
                    dedicated_tts_provider(self.context, values["call_tts_provider_id"])
                except Exception:
                    raise SettingsInvalid(
                        {"call_tts_provider_id": "请选择当前已启用的合成服务"}
                    ) from None
            if "call_chat_provider_id" in edits and values["call_chat_provider_id"]:
                try:
                    dedicated_chat_provider(self.context, values["call_chat_provider_id"])
                except Exception:
                    raise SettingsInvalid(
                        {"call_chat_provider_id": "请选择当前已启用的聊天模型"}
                    ) from None
        except SettingsInvalid as error:
            return "bot_call.settings_invalid", dict(errors=error.errors)
        # No await between fresh revision check, validation and the native atomic
        # save. Native config-page writes use this same configuration object.
        current, _, _ = self.state()
        if revision(current) != body["revision"]:
            return "bot_call.settings_conflict", {}
        try:
            save_plugin_config(self.config, merged)
        except Exception:
            return "bot_call.settings_save_failed", {}
        saved, values, _ = self.state()
        logging.getLogger("bot_call.settings").info(
            "bot_call.settings stage=saved keys=%s", ",".join(sorted(edits))
        )
        return None, dict(
            saved=list(edits),
            revision=revision(saved),
            reload_required=self.reload_required(values),
        )
