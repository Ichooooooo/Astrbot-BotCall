"""Pinned AstrBot host wiring; imports and construction perform no business IO."""

import asyncio
import hashlib
import json
import logging
import re
from urllib.parse import urlsplit
from dataclasses import replace

from .phone_style import DEFAULT_REPLY_STYLE
from .sdk_privacy import ProviderLogPrivacy
from .astrbot_compat import BASELINE_VERSION, load_host_tools, tool_entry_contract, text_chain
from .native_provider import NATIVE_CONTRACT, COUNTER_RULE, provider_info, selected_media


ESTIMATE_RULE = f"astrbot{BASELINE_VERSION}.canonical-json-estimate.v1"


class NativeHostError(RuntimeError):
    def __init__(self):
        super().__init__("bot_call.host_unavailable")


def estimate_counter(message_type, estimator):
    """Detached native estimator: only pure Message/counter dependencies cross worker.

    Native EstimateTokenCounter is synchronous and stateless. No loop, Context,
    Provider, tools bundle or bound host method is captured by this closure.
    """
    if not isinstance(message_type, type) or not callable(getattr(estimator, "count_tokens", None)):
        raise NativeHostError() from None

    def count_payload(payload):
        try:
            if type(payload) is not str:
                raise ValueError
            payload.encode("utf-8")
            count = estimator.count_tokens(
                [message_type(role="user", content=payload)], trusted_token_usage=0
            )
            if type(count) is not int or count < 0:
                raise ValueError
            return count
        except Exception:
            raise NativeHostError() from None

    return count_payload


def load_tools(context):
    """Resolve the installed native classes only after explicit enablement."""
    return load_host_tools(context)


def settings_from(config):
    """Read non-secret host settings. Window/model facts require explicit review."""
    try:
        from .review import build_review_prompt

        from .settings import validate_config

        checked = validate_config(config, require_ready=True)
        review_prompt = build_review_prompt(
            checked["review_instructions"],
            bounded_output=True,
            style=checked["review_style"],
            voice=checked["review_voice"],
            system_notes=checked["review_system_notes"],
        )
        values = {
            k: checked[k]
            for k in (
                "listen_port",
                "listen_host",
                "trusted_proxies",
                "call_vad_silence_ms",
                "call_vad_sensitivity",
                "call_max_continuous_segments",
                "audio_retention_days",
                "call_capture_mode",
                "call_barge_in",
                "call_backchannel_phrases",
                "review_tokens",
                "safety_tokens",
                "max_audio_bytes",
                "max_wav_bytes",
            )
        }
        reply_style, call_model = checked["call_reply_style"], checked["call_chat_provider_id"]
        skipped = checked["call_skip_plugin_hooks"]
        history_turns, history_summary, history_max_chars = (
            checked[k]
            for k in ("call_history_turns", "call_history_summary", "call_history_max_chars")
        )
        origin, mode, executable, timeout = (
            checked[k]
            for k in ("public_origin", "access_mode", "cloudflared_path", "tunnel_start_timeout")
        )
        return dict(
            values,
            call_reply_style=reply_style,
            call_skip_plugin_hooks=tuple(skipped),
            call_history_turns=history_turns,
            call_history_summary=history_summary,
            call_history_max_chars=history_max_chars,
            call_chat_provider_id=call_model,
            call_tts_provider_id=checked["call_tts_provider_id"],
            call_stt_provider_id=checked["call_stt_provider_id"],
            origin=origin,
            review_prompt=review_prompt,
            review_voice=checked["review_voice"],
            access_mode=mode,
            cloudflared_path=executable,
            tunnel_start_timeout=timeout,
        )
    except Exception:
        raise NativeHostError() from None


class NativeCallbacks:
    """One loop's real native operations, with a labelled estimate registry."""

    def __init__(self, context, tools, estimator, settings):
        from .astrbot_context import AstrBotContextAdapter

        self.loop = asyncio.get_running_loop()
        self.context, self.tools, self.settings = context, tools, settings
        self.counters = {
            rule: estimate_counter(tools.message_type, estimator)
            for rule in (ESTIMATE_RULE, COUNTER_RULE)
        }
        self.importer = AstrBotContextAdapter(
            context, business_loop=self.loop, session_lock_manager=tools.session_lock_manager
        )
        # Fail before creating a worker if the native Message/counter shape is unusable.
        self.counters[ESTIMATE_RULE]('{"messages":[]}')
        # Acquire last; if anything after acquisition fails, host has no object
        # reference yet and cannot release it on our behalf.
        privacy = ProviderLogPrivacy()
        try:
            self.tools = replace(
                tools,
                privacy=privacy,
                call_reply_style=settings.get("call_reply_style", DEFAULT_REPLY_STYLE),
                call_chat_provider_id=settings.get("call_chat_provider_id", ""),
                call_skip_plugin_hooks=tuple(settings.get("call_skip_plugin_hooks", ())),
                call_history_turns=settings.get("call_history_turns", 8),
                call_history_summary=settings.get("call_history_summary", True),
                call_history_max_chars=settings.get("call_history_max_chars", 8000),
            )
            self.privacy = privacy
            self.provider_integration = "astrbot_main_agent_v3"
            self.output_limits_managed_by = "astrbot"
        except BaseException:
            privacy.close()
            raise

    def check_loop(self):
        if asyncio.get_running_loop() is not self.loop:
            raise NativeHostError() from None

    def tool_entry(self, event):
        """Pinned framework bridge; no Provider calls, awaits or text retention."""
        from .binding import (
            ExpectedEntry,
            private_event_identity,
            reliable_message_id,
            validate_conversation,
        )

        try:
            self.check_loop()
            fallback, permissions, Plain, ProviderRequest = tool_entry_contract(self.context)
            if type(permissions) is not dict:
                raise ValueError
            if type(fallback) is not str or fallback not in ("member", "admin"):
                raise ValueError
            defaults = permissions.get("_default", {})
            if type(defaults) is not dict:
                raise ValueError
            permission = defaults.get("bot_call_start", fallback)
            if permission not in ("member", "admin") or type(permission) is not str:
                raise ValueError
            if permission == "admin" and event.is_admin() is not True:
                raise ValueError
            parts = event.message_obj.message
            if (
                type(parts) is not list
                or not 1 <= len(parts) <= 16
                or any(type(part) is not Plain or type(part.text) is not str for part in parts)
                or sum(len(part.text) for part in parts) > 80
            ):
                raise ValueError
            text = "".join(part.text for part in parts).strip()
            if (
                re.fullmatch(
                    r"请?(?:现在发起通话|现在开始通话|发起通话|开始通话|现在开始语音通话|发起语音通话|现在和我通话|给我通话链接)吧?[。！!]?",
                    text,
                )
                is None
            ):
                raise ValueError
            identity = private_event_identity(event)
            message_id = getattr(event.message_obj, "message_id", None)
            if not reliable_message_id(message_id):
                raise ValueError
            request = event.get_extra("provider_request")
            if type(request) is not ProviderRequest:
                raise ValueError
            conversation = request.conversation
            cid = getattr(conversation, "cid", None)
            if type(cid) is not str or not cid or cid.isspace():
                raise ValueError
            validate_conversation(conversation, umo=identity[6], cid=cid, platform_id=identity[2])
            if request.session_id and request.session_id != identity[6]:
                raise ValueError
            return ExpectedEntry(identity, message_id, cid)
        except Exception:
            raise NativeHostError() from None

    def model(self, provider_id, output):
        from .astrbot_reply import ReplyModel

        provider = self.context.get_provider_by_id(provider_id)
        identifier, model, window = provider_info(provider, "text_chat")
        if identifier != provider_id or window <= 0:
            raise NativeHostError() from None
        return ReplyModel(identifier, model, window, output, NATIVE_CONTRACT)

    def check_call_model(self):
        self.check_loop()
        provider_id = self.settings.get("call_chat_provider_id", "")
        if provider_id:
            from .astrbot_compat import dedicated_chat_provider

            dedicated_chat_provider(self.context, provider_id)

    async def diagnostic_model(self, umo=None):
        from .astrbot_compat import effective_chat_model

        return await effective_chat_model(
            self.context, self.settings.get("call_chat_provider_id", ""), umo
        )

    async def check_stt(self, umo):
        self.check_loop()
        from .astrbot_audio import STTUnavailable

        try:
            from .astrbot_compat import dedicated_stt_provider

            pid = self.settings.get("call_stt_provider_id", "")
            provider = (
                dedicated_stt_provider(self.context, pid)
                if pid
                else await selected_media(self.context, umo, "stt")
            )
            self.privacy.check()
            return provider, "dedicated" if pid else "native"
        except Exception:
            raise STTUnavailable() from None

    async def check_tts(self, umo):
        from .astrbot_tts import AstrBotTTSAdapter

        self.check_loop()
        return await AstrBotTTSAdapter.selected(
            self.context, umo, self.privacy, self.settings.get("call_tts_provider_id", "")
        )

    async def diagnostic_source(self, umo):
        """Current selection checks only; never invokes inference or reads text."""
        self.check_loop()
        stt, tts = "unavailable", "unavailable"
        try:
            _, selection = await self.check_stt(umo)
            if selection in {"native", "legacy", "dedicated"}:
                stt = selection
        except Exception:
            pass
        try:
            await self.check_tts(umo)
            tts = "native"
        except Exception:
            pass
        return {"stt": stt, "tts": tts}

    async def select(self, binding, snapshot):
        from .astrbot_reply import ReplyPolicy
        from .astrbot_audio import AudioRequestPolicy
        from .astrbot_review import ReviewRequestPolicy
        from .backend import BackendPolicies
        from .review_router import ReviewPolicySelection

        self.check_loop()
        self.privacy.check()
        from .astrbot_agent import SourceAgentPolicy

        main = self.model(snapshot.provider_id, self.settings["review_tokens"])
        if main.context_window != snapshot.provider_max_context_tokens:
            raise NativeHostError() from None
        stt, selection = await self.check_stt(binding.unified_msg_origin)
        audio = AudioRequestPolicy(
            binding,
            provider_info(stt, "get_text")[0],
            stt.get_model(),
            self.settings["max_audio_bytes"],
            self.settings["max_wav_bytes"],
            NATIVE_CONTRACT,
            dedicated=bool(self.settings.get("call_stt_provider_id", "")),
        )
        review = ReviewRequestPolicy(
            binding,
            main.provider_id,
            main.model,
            main.context_window,
            self.settings["review_tokens"],
            self.settings["safety_tokens"],
            main.cap_parameter,
            self.counters[COUNTER_RULE],
        )
        from .astrbot_tts import SpeechPolicy

        tts = await self.check_tts(binding.unified_msg_origin)
        speech = SpeechPolicy(
            binding,
            provider_info(tts, "get_audio")[0],
            tts.get_model(),
            dedicated=bool(self.settings.get("call_tts_provider_id", "")),
        )
        return BackendPolicies(
            SourceAgentPolicy(binding), audio, ReviewPolicySelection(review, COUNTER_RULE), speech
        )

    async def send_text(self, binding, text):
        from .binding import CallBinding
        from .publication import Outcome

        self.check_loop()
        try:
            if not isinstance(binding, CallBinding):
                raise ValueError
            identities = (
                binding.sender_id,
                binding.self_id,
                binding.platform_id,
                binding.session_id,
            )
            if (
                any(not isinstance(v, str) or not v or v.isspace() for v in identities)
                or binding.platform_type not in {"aiocqhttp", "qq_official"}
                or binding.message_type != "FriendMessage"
                or type(text) is not str
                or not text
                or text.isspace()
                or binding.unified_msg_origin
                != f"{binding.platform_id}:FriendMessage:{binding.session_id}"
            ):
                raise ValueError
            platforms = [
                p
                for p in self.context.platform_manager.platform_insts
                if p.meta().id == binding.platform_id
            ]
            if len(platforms) != 1 or platforms[0].meta().name != binding.platform_type:
                raise ValueError
            chain = text_chain(text)
        except Exception:
            return Outcome.NOT_APPLIED
        try:
            # Context routes the full UMO to the prechecked instance. The core
            # bool confirms route submission only, never platform/user receipt.
            result = await self.context.send_message(binding.unified_msg_origin, chain)
            if result is True:
                return Outcome.SUBMITTED
            return Outcome.NOT_APPLIED if result is False else Outcome.UNCERTAIN
        except asyncio.CancelledError:
            return Outcome.UNCERTAIN
        except Exception:
            return Outcome.UNCERTAIN

    async def send_page(self, binding, page):
        return await self.send_text(binding, page.message)

    async def import_context(self, binding, source_key, body):
        from .publication import Outcome

        self.check_loop()
        try:
            receipt = await self.importer.import_review(binding, source_key, body)
            if (
                receipt.call_id == binding.call_id
                and receipt.source_key == source_key
                and receipt.body_sha256 == hashlib.sha256(body.encode("utf-8")).hexdigest()
            ):
                return Outcome.CONFIRMED  # Core storage readback ONLY, not final model input.
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            pass
        return Outcome.UNCERTAIN


class HostASGI:
    """Exact-path HTTP plus lifespan, behind one trusted local TLS proxy."""

    def __init__(self, app, ready, trusted_proxies=("127.0.0.1",), rejections=None, origin=""):
        from .proxy_access import proxy_networks, Rejections

        self.networks = proxy_networks(list(trusted_proxies))
        self.rejections = rejections or Rejections()
        self.origin = origin
        self.app, self.ready = app, ready
        self.lifespans = set()

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            task = asyncio.current_task()
            self.lifespans.add(task)
            try:
                while True:
                    message = await receive()
                    if message["type"] == "lifespan.startup":
                        await send({"type": "lifespan.startup.complete"})
                    elif message["type"] == "lifespan.shutdown":
                        await send({"type": "lifespan.shutdown.complete"})
                        return
            finally:
                self.lifespans.discard(task)
        elif scope["type"] == "http":
            from .proxy_access import trusted_peer, forwarded_proto

            accepted = trusted_peer(scope, self.networks) and forwarded_proto(scope) == [b"https"]
            if not self.ready():
                status, code = 503, b'{"error":"host_unavailable"}'
            elif not accepted:
                self.rejections.record(scope, "proxy_rejected", self.origin)
                status, code = 403, b'{"error":"proxy_rejected"}'
            else:
                # Host, Origin, Cookie, root_path and path stay untouched.
                return await self.app(dict(scope, scheme="https"), receive, send)
            await send(
                {
                    "type": "http.response.start",
                    "status": status,
                    "headers": [
                        (b"cache-control", b"no-store"),
                        (b"content-type", b"application/json"),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": code})
        elif scope["type"] == "websocket":
            from .proxy_access import trusted_peer, forwarded_proto

            accepted = trusted_peer(scope, self.networks) and forwarded_proto(scope) == [b"https"]
            if self.ready() and accepted:
                return await self.app(dict(scope, scheme="wss"), receive, send)
            if self.ready():
                self.rejections.record(scope, "proxy_rejected", self.origin)
            await send({"type": "websocket.close", "code": 1008})
        else:
            raise NativeHostError() from None


class NativeServer:
    """Own the actual Hypercorn task, socket and lifespan until they really exit."""

    def __init__(self, app, port, host="127.0.0.1"):
        self.host = host
        self.app, self.port = app, port
        self.task = None
        self.stopped = False
        self._stop = asyncio.Event()
        self._ready = asyncio.get_running_loop().create_future()
        self._socket = None

    @property
    def ready(self):
        return (
            self._ready.done()
            and self.task is not None
            and not self.task.done()
            and not self._stop.is_set()
        )

    async def start(self):
        if self.task is None:
            self.task = asyncio.create_task(self._run(), name="BotCall:http")
            self.task.add_done_callback(lambda t: None if t.cancelled() else t.exception())
        await asyncio.wait({self.task, self._ready}, return_when=asyncio.FIRST_COMPLETED)
        if self.task.done():
            raise NativeHostError() from None
        if not self.ready:
            raise NativeHostError() from None

    async def _run(self):
        try:
            import socket
            from importlib.metadata import version
            from hypercorn.asyncio import serve
            from hypercorn.config import Config, Sockets
            from hypercorn.logging import Logger

            if version("hypercorn") != "0.18.0":
                raise NativeHostError()
            owner = self

            class BoundConfig(Config):
                def create_sockets(self):
                    return Sockets([], [owner._socket], [])

            class BoundLogger(Logger):
                async def info(self, message, *args, **kwargs):
                    # Hypercorn 0.18 emits this AFTER await asyncio.start_server.
                    # Verify the owned kernel listener too; lifespan is NOT ready.
                    if (
                        message.startswith("Running on http://")
                        and owner._socket.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN) == 1
                        and not owner._ready.done()
                    ):
                        owner._ready.set_result(None)

            config = BoundConfig()
            config.websocket_max_message_size = 1024
            config.bind = [
                f"[{self.host}]:{self.port}" if ":" in self.host else f"{self.host}:{self.port}"
            ]
            config.accesslog = config.errorlog = None
            config.logger_class = BoundLogger
            self._socket = socket.socket(
                socket.AF_INET6 if ":" in self.host else socket.AF_INET, socket.SOCK_STREAM
            )
            # Rebind after our closed connections enter TIME_WAIT. Never share
            # an active listener (SO_REUSEPORT would violate port ownership).
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._socket.setblocking(False)
            self._socket.bind((self.host, self.port))
            await serve(self.app, config, shutdown_trigger=self._stop.wait, mode="asgi")
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            raise NativeHostError() from None
        finally:
            if self._socket is not None:
                self._socket.close()
            # Hypercorn startup failure before its shutdown finally can leave a
            # lifespan task. Reap only tasks explicitly held by this wrapper.
            tasks = tuple(self.app.lifespans)
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.stopped = True

    async def close(self):
        self._stop.set()
        if self.task is None:
            self.stopped = True
        else:
            await asyncio.wait({self.task})
            if self.task.cancelled() or self.task.exception() is not None:
                raise NativeHostError() from None
        return self.stopped
