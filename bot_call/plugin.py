"""Native Star host ownership; disabled import/construction have no business IO."""

import asyncio
import json
import logging
import socket
import sys
from types import FunctionType
from pathlib import Path

from .prepare_diagnostics import attach_prepare
from .observability import StageTimings

PLUGIN_NAME = "astrbot_plugin_bot_call"
DATABASE_NAME = "calls.sqlite3"
DIAGNOSTIC_ROUTE = "/" + PLUGIN_NAME + "/diagnostics"
BENCHMARK_ROUTE = "/" + PLUGIN_NAME + "/benchmark"
SETTINGS_ROUTE = "/" + PLUGIN_NAME + "/settings"
REVIEWS_ROUTE = "/" + PLUGIN_NAME + "/reviews"
_CLAIM = "_bot_call_native_owner_v1"
_ORPHAN_STOP_TIMEOUT = 25.0


class PluginLifecycleError(RuntimeError):
    def __init__(self):
        super().__init__("bot_call.host_unavailable")


def _consume(task):
    if not task.cancelled():
        task.exception()


async def _wait(task):
    # Waiting cancellation never cancels the actual operation owned by this host.
    try:
        await asyncio.wait({task})
        return task.result()
    except asyncio.CancelledError:
        raise asyncio.CancelledError() from None


# Only fixed labels and this package's exact code filenames may enter a note.
_DIAGNOSTIC_FILES = {
    str(Path(__file__).parent / name): "bot_call/" + name
    for name in (
        "access.py",
        "astrbot_adapter.py",
        "astrbot_audio.py",
        "astrbot_context.py",
        "astrbot_reply.py",
        "astrbot_review.py",
        "audio.py",
        "backend.py",
        "binding.py",
        "call_policy.py",
        "limits.py",
        "native_provider.py",
        "native_host.py",
        "plugin.py",
        "publication.py",
        "quick_tunnel.py",
        "review.py",
        "review_plan.py",
        "review_policy.py",
        "review_router.py",
        "review_work.py",
        "runtime.py",
        "service.py",
        "store.py",
        "web.py",
    )
}
_DIAGNOSTIC_TYPES = {
    kind: name
    for kind, name in (
        (ValueError, "ValueError"),
        (TypeError, "TypeError"),
        (RuntimeError, "RuntimeError"),
        (OSError, "OSError"),
        (TimeoutError, "TimeoutError"),
        (PermissionError, "PermissionError"),
        (FileNotFoundError, "FileNotFoundError"),
        (ConnectionError, "ConnectionError"),
        (ConnectionRefusedError, "ConnectionRefusedError"),
        (AttributeError, "AttributeError"),
        (ImportError, "ImportError"),
        (ModuleNotFoundError, "ModuleNotFoundError"),
    )
}


def _startup_note(host, stage, error):
    """Read owned failures only; never stringify exceptions, notes or locals."""
    try:
        types = dict(_DIAGNOSTIC_TYPES)
        codes = {}
        for module, name, code in (
            ("plugin", "PluginLifecycleError", "bot_call.host_unavailable"),
            ("native_host", "NativeHostError", "bot_call.host_unavailable"),
            ("quick_tunnel", "TunnelError", "bot_call.tunnel_unavailable"),
            ("backend", "BackendError", None),
        ):
            kind = getattr(sys.modules.get(__package__ + "." + module), name, None)
            if isinstance(kind, type):
                types[kind] = name
                codes[kind] = code
        roots = [("initial", error)]
        server, tunnel, backend = host._server, host._tunnel, host._backend
        for label, task in (
            ("http", None if server is None else server.task),
            ("tunnel_launch", None if tunnel is None else tunnel._launch_task),
            ("tunnel_reader", None if tunnel is None else tunnel._reader),
            ("tunnel_monitor", None if tunnel is None else tunnel.task),
            ("backend", None if backend is None else backend._start_task),
        ):
            if task is not None and task.done() and not task.cancelled():
                failure = task.exception()
                if failure is not None:
                    roots.append((label, failure))
        rows, seen = [], set()
        for label, failure in roots:
            pending = [(failure, 0)]
            while pending and len(rows) < 8:
                exc, depth = pending.pop(0)
                if id(exc) in seen or depth > 3:
                    continue
                seen.add(id(exc))
                row = {"source": label, "class": types.get(type(exc), "unknown")}
                code = codes.get(type(exc))
                if code is not None:
                    row["code"] = code
                frames, trace, count = [], exc.__traceback__, 0
                while trace is not None and count < 40:
                    name = _DIAGNOSTIC_FILES.get(trace.tb_frame.f_code.co_filename)
                    if name is not None and type(trace.tb_lineno) is int:
                        frames.append({"file": name, "line": trace.tb_lineno})
                    trace, count = trace.tb_next, count + 1
                row["frames"] = frames[-2:]
                rows.append(row)
                for nested in (exc.__cause__, exc.__context__):
                    if nested is not None:
                        pending.append((nested, depth + 1))
        note = {"stage": stage, "errors": rows}
        if tunnel is not None:
            process = tunnel._process
            code = None if process is None else process.returncode
            note["tunnel"] = {
                "origin_present": tunnel.origin is not None,
                "registered": tunnel.registered is True,
                "child_returncode": code if type(code) is int else None,
            }
        return "bot_call.startup " + json.dumps(note, separators=(",", ":"))
    except Exception:
        # Observability cannot interrupt resource cleanup or copy unsafe text.
        return "bot_call.startup diagnostic=unknown"


def _attach_startup_note(error, diagnostic, *, cleanup_wait=False):
    # initialize waiters can observe the same owned stop exception. Keep notes
    # idempotent and bounded; a raised cleanup result does not imply a leak.
    if diagnostic is None:
        return
    additions = (
        (diagnostic, "bot_call.startup cleanup_wait=raised") if cleanup_wait else (diagnostic,)
    )
    for note in additions:
        existing = getattr(error, "__notes__", ())
        if len(existing) >= 2:
            return
        if not any(type(value) is str and value == note for value in existing):
            BaseException.add_note(error, note)


class PluginLifecycle:
    """Own one Backend/server on the original Context and business loop.

    Optional factories are trusted constructor injection, never ordinary config.
    References and the Context claim survive cancelled waiters and module reload.
    """

    def __init__(
        self,
        config,
        data_dir,
        *,
        context,
        tools_factory=None,
        server_factory=None,
        version="unknown",
    ):
        self._config, self._data_dir, self._context = config, data_dir, context
        self._tools_factory, self._server_factory = tools_factory, server_factory
        self._loop = None
        self._start_task = self._terminate_task = self._stop_task = None
        self._backend = self._server = self._native = self._app = self._tunnel = None
        self._ready, self._terminating, self._claimed = False, False, False
        self._server_failed = False
        self._startup_diagnostic = None
        self._startup_error = None
        self._events, self._invitations = {}, []
        self._version = version
        self._timings = StageTimings()
        from .settings import SettingsController

        self._settings = SettingsController(config, context, version)
        from .benchmark_run import BenchmarkController

        self._benchmark = BenchmarkController(self)
        self._benchmark_handler = self._benchmark_api
        self._settings_handler = self._settings_api
        self._reviews_handler = self._reviews_api
        self._management_handler = self._diagnostics  # Stable identity across cleanup.

    def _register_management(self):
        self._context.register_web_api(
            BENCHMARK_ROUTE, self._benchmark_handler, ["GET", "POST"], "Bot-Call benchmark"
        )
        self._context.register_web_api(
            SETTINGS_ROUTE, self._settings_handler, ["GET", "POST"], "Bot-Call plugin settings"
        )
        self._context.register_web_api(
            REVIEWS_ROUTE, self._reviews_handler, ["POST"], "Bot-Call explicit review recovery"
        )
        self._context.register_web_api(
            DIAGNOSTIC_ROUTE, self._management_handler, ["GET"], "Bot-Call read-only diagnostics"
        )

    def _stop_management(self):
        # Core does not unbind extension APIs on plugin disable. Do not delete a
        # successor's handler or any other plugin's registration from this list.
        registry = getattr(self._context, "registered_web_apis", None)
        if isinstance(registry, list):
            registry[:] = [
                row
                for row in registry
                if not (
                    isinstance(row, tuple)
                    and len(row) == 4
                    and (
                        (
                            row[0] == DIAGNOSTIC_ROUTE
                            and row[1] is self._management_handler
                            and row[2] == ["GET"]
                        )
                        or (
                            row[0] == REVIEWS_ROUTE
                            and row[1] is self._reviews_handler
                            and row[2] == ["POST"]
                        )
                        or (
                            row[0] == BENCHMARK_ROUTE
                            and row[1] is self._benchmark_handler
                            and row[2] == ["GET", "POST"]
                        )
                        or (
                            row[0] == SETTINGS_ROUTE
                            and row[1] is self._settings_handler
                            and row[2] == ["GET", "POST"]
                        )
                    )
                )
            ]
        try:
            self._timings.close()
        except BaseException:
            pass

    async def _benchmark_api(self):
        from .astrbot_compat import management_web, dashboard_authorized

        request, response = management_web()
        headers = {"Cache-Control": "no-store"}
        try:
            authorized = dashboard_authorized(
                request, PLUGIN_NAME, BENCHMARK_ROUTE, methods=("GET", "POST")
            )
        except Exception:
            authorized = False
        if not authorized:
            return response(
                dict(status="error", message="bot_call.management_required", data={}),
                status_code=401,
                headers=headers,
            )
        try:
            method = request.method
            body = await request.json() if method == "POST" else None

            async def action():
                if method == "GET":
                    return dict(status="ok", data=await self._benchmark.get())
                if type(body) is dict and body.get("action") == "retry_review":
                    return await self._benchmark.retry_review(body)
                if type(body) is dict and body.get("action") == "force_hangup":
                    return await self._benchmark.force_hangup(body)
                if body != {"action": "run"}:
                    return dict(status="error", message="无效测试请求", data={})
                return await self._benchmark.run()

            if self._loop is asyncio.get_running_loop():
                data = await action()
            elif self._loop is not None and self._loop.is_running():
                data = await asyncio.wrap_future(
                    asyncio.run_coroutine_threadsafe(action(), self._loop)
                )
            else:
                data = dict(
                    status="error", message="插件未启用（disabled）", data=dict(code="disabled")
                )
        except Exception:
            data = dict(status="error", message="评测暂不可用，请检查插件诊断", data={})
        return response(data, headers=headers)

    async def _settings_api(self):
        from .astrbot_compat import management_web, dashboard_authorized

        request, response = management_web()
        headers = {"Cache-Control": "no-store"}
        try:
            authorized = dashboard_authorized(
                request, PLUGIN_NAME, SETTINGS_ROUTE, methods=("GET", "POST")
            )
        except Exception:
            authorized = False
        if not authorized:
            return response(
                dict(status="error", message="bot_call.management_required", data={}),
                status_code=401,
                headers=headers,
            )
        method = request.method
        body = await request.json() if method == "POST" else None
        try:
            if self._loop is asyncio.get_running_loop():
                error, data = await self._settings_action(method, body)
            elif self._loop is not None and self._loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self._settings_action(method, body), self._loop
                )
                error, data = await asyncio.wrap_future(future)
            else:
                error, data = "bot_call.not_ready", {}
        except Exception:
            error, data = "bot_call.not_ready", {}
        message = error or ""
        if error == "bot_call.settings_invalid":
            message += ":" + json.dumps(data, ensure_ascii=False)
        return response(
            dict(status="error" if error else "ok", message=message, data=data), headers=headers
        )

    async def _settings_action(self, method, body):
        self._check_loop()
        if self._terminating:
            return "bot_call.not_ready", {}
        if method == "POST":
            return await self._settings.post(body)
        active = (
            await self._backend._runtime.active_call_count() if self._backend is not None else 0
        )
        return None, await self._settings.get(active)

    async def _reviews_api(self):
        from .astrbot_compat import management_web, dashboard_authorized

        request, response = management_web()
        headers = {"Cache-Control": "no-store"}
        try:
            authorized = dashboard_authorized(
                request, PLUGIN_NAME, REVIEWS_ROUTE, methods=("POST",)
            )
        except Exception:
            authorized = False
        if not authorized:
            return response(
                dict(status="error", message="bot_call.management_required", data={}),
                status_code=401,
                headers=headers,
            )
        try:
            body = await request.json()
            if self._loop is asyncio.get_running_loop():
                data = await self._reviews_action(body)
            elif self._loop is not None and self._loop.is_running():
                future = asyncio.run_coroutine_threadsafe(self._reviews_action(body), self._loop)
                data = await asyncio.wrap_future(future)
            else:
                raise PluginLifecycleError()
        except Exception:
            return response(
                dict(status="error", message="bot_call.review_recovery_unavailable", data={}),
                status_code=400,
                headers=headers,
            )
        return response(dict(status="ok", data=data), headers=headers)

    async def _reviews_action(self, body):
        self._check_loop()
        if not self.ready or getattr(self._context, _CLAIM, None) is not self:
            raise PluginLifecycleError()
        if type(body) is not dict or set(body) - {
            "call_ids",
            "apply",
            "context_only",
            "allow_uncertain",
        }:
            raise PluginLifecycleError()
        return await self._backend.backfill_reviews(
            body.get("call_ids"),
            counters=self._native.counters,
            context_only=body.get("context_only", True),
            apply=body.get("apply", False),
            **({"allow_uncertain": body["allow_uncertain"]} if "allow_uncertain" in body else {}),
        )

    async def _diagnostics(self):
        # Core binds PluginRequest after route auth, but v1 also allows API keys
        # and skips legacy asset-role middleware. Reuse the actual dashboard
        # server's role/path-aware validator, not a username-only dependency.
        # This private request/server bridge is pinned to the baseline; missing
        # contracts fail closed. Tokens stay in server locals, never the DTO/page.
        from .astrbot_compat import management_web, dashboard_authorized

        request, json_response = management_web()
        headers = {"Cache-Control": "no-store"}
        try:
            authorized = dashboard_authorized(request, PLUGIN_NAME, DIAGNOSTIC_ROUTE)
        except Exception:
            authorized = False
        if not authorized:
            return json_response(
                {"status": "error", "message": "bot_call.management_required", "data": {}},
                status_code=401,
                headers=headers,
            )
        try:
            if self._loop is asyncio.get_running_loop():
                data = await self._diagnostic_data()
            elif self._loop is not None and self._loop.is_running():
                # Dashboard request loops need not own the native business state.
                future = asyncio.run_coroutine_threadsafe(self._diagnostic_data(), self._loop)
                data = await asyncio.wrap_future(future)
            else:
                data = None
        except Exception:
            data = None
        if data is None:
            return json_response(
                {"status": "error", "message": "bot_call.not_ready", "data": {}},
                status_code=503,
                headers=headers,
            )
        return json_response({"status": "ok", "data": data}, headers=headers)

    async def _diagnostic_data(self):
        self._check_loop()
        if not self.ready or getattr(self._context, _CLAIM, None) is not self:
            return None
        # Read current sources synchronously; recheck readiness after the timing read.
        source = None
        binding = None
        try:
            binding = self._backend.diagnostic_binding()
            if binding is not None:
                source = await self._native.diagnostic_source(binding.unified_msg_origin)
        except Exception:
            pass
        try:
            timings = self._timings.snapshot()
        except BaseException:
            timings = {"capacity": 128, "sample_count": 0, "stages": []}
        history = await self._backend._runtime.diagnostic_timings()
        reviews = await self._backend._runtime.diagnostic_reviews()
        if not self.ready or getattr(self._context, _CLAIM, None) is not self:
            return None
        data = {
            "call_model": await self._native.diagnostic_model(
                binding.unified_msg_origin if binding is not None else None
            ),
            "round_timings": history,
            "reviews": reviews,
            "version": self._version,
            "plugin_ready": True,
            "https_ready": True,
            "bare_page_url": self._native.settings["origin"] + "/bot-call/",
            "source": source,
            "timings": timings,
            "provider_integration": (
                "astrbot_main_agent_v3"
                if getattr(self._native, "provider_integration", None) == "astrbot_main_agent_v3"
                else "unknown"
            ),
            "output_limits_managed_by": (
                "astrbot"
                if getattr(self._native, "output_limits_managed_by", None) == "astrbot"
                else "unknown"
            ),
        }
        return data

    def _check_loop(self):
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise PluginLifecycleError() from None

    @property
    def ready(self):
        return (
            self._ready
            and not self._terminating
            and self._server is not None
            and self._server.ready
            and (self._tunnel is None or self._tunnel.ready)
        )

    @property
    def app(self):
        self._check_loop()
        if not self.ready:
            raise PluginLifecycleError() from None
        return self._app

    async def initialize(self):
        self._check_loop()
        if self._terminating:
            raise PluginLifecycleError() from None
        if self._start_task is None:
            self._start_task = asyncio.create_task(self._initialize(), name="BotCall:host-start")
            self._start_task.add_done_callback(_consume)
        try:
            await _wait(self._start_task)
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            try:
                await _wait(self._schedule_stop())
            except Exception as error:
                _attach_startup_note(error, self._startup_diagnostic, cleanup_wait=True)
                raise error from None
            if self._startup_error:
                self._register_management()
            error = PluginLifecycleError()
            _attach_startup_note(error, self._startup_diagnostic)
            raise error from None

    def _is_orphan(self, owner):
        # A pre-0.7.4 owner has no module token. Its class functions retain the
        # original module globals even after AstrBot deletes/reimports modules.
        # Never use isinstance across imports, nor trust a foreign duck type.
        current = sys.modules.get(__name__)
        kind = type(owner)
        constructor = kind.__dict__.get("__init__")
        if (
            current is None
            or current.__dict__ is not globals()
            or kind.__module__ != __name__
            or kind.__name__ != "PluginLifecycle"
            or kind.__qualname__ != "PluginLifecycle"
            or not isinstance(constructor, FunctionType)
        ):
            return False
        original = constructor.__globals__
        if (
            original is current.__dict__
            or original.get("__name__") != __name__
            or original.get("PluginLifecycle") is not kind
            or original.get("_CLAIM") != _CLAIM
        ):
            return False
        for name in ("terminate", "_schedule_stop", "_stop_resources"):
            method = kind.__dict__.get(name)
            if not isinstance(method, FunctionType) or method.__globals__ is not original:
                return False
        state = vars(owner)
        if (
            state.get("_context") is not self._context
            or state.get("_loop") is not self._loop
            or state.get("_claimed") is not True
            or type(state.get("_terminating")) is not bool
            or type(state.get("_ready")) is not bool
            or not {"_backend", "_server", "_tunnel"} <= state.keys()
        ):
            return False
        for name in ("_start_task", "_stop_task", "_terminate_task"):
            if name not in state:
                return False
            task = state[name]
            if task is not None and (
                not isinstance(task, asyncio.Task) or task.get_loop() is not self._loop
            ):
                return False
        return True

    async def _stop_orphan(self, owner, port, host="127.0.0.1"):
        started = self._loop.time()
        outcome = "failed"
        try:
            # The old object's own code owns shutdown, including in-flight work.
            # A timeout must not cancel it or discard its remaining resources.
            task = asyncio.create_task(owner.terminate(), name="BotCall:orphan-stop")
            task.add_done_callback(_consume)
            done, _ = await asyncio.wait({task}, timeout=_ORPHAN_STOP_TIMEOUT)
            if not done:
                outcome = "timeout"
                raise TimeoutError() from None
            if task.cancelled():
                raise PluginLifecycleError() from None
            task.result()
            if getattr(self._context, _CLAIM, None) is not None or owner._claimed:
                raise PluginLifecycleError() from None
            # Confirm release before taking the claim. NativeServer still binds
            # its own socket afterwards and fails closed if another process wins.
            with socket.socket(
                socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM
            ) as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind((host, port))
            outcome = "stopped"
        finally:
            logging.getLogger(__name__).info(
                "bot_call.orphan_claim state=%s elapsed_ms=%d",
                outcome,
                (self._loop.time() - started) * 1000,
            )

    async def _initialize(self):
        stage = "settings"
        try:
            enabled = False if self._config is None else self._config.get("enabled", False)
            if type(enabled) is not bool:
                raise PluginLifecycleError()
            if not enabled and callable(getattr(self._context, "register_web_api", None)):
                self._register_management()
            if not enabled:
                return
            from .native_host import (
                settings_from,
                load_tools,
                NativeCallbacks,
                NativeServer,
                HostASGI,
            )

            settings = settings_from(self._config)
            if settings["access_mode"] == "quick_tunnel":
                from shutil import which

                if which(settings["cloudflared_path"]) is None:
                    stage = "tunnel"
                    self._startup_error = "临时隧道不可用：未找到 cloudflared"
                    logging.getLogger(__name__).error(self._startup_error)
                    raise PluginLifecycleError()
            from .proxy_access import listen_address

            host = listen_address(settings)
            review_prompt = settings.pop("review_prompt")  # Not source/diagnostic settings.
            stage = "claim"
            owner = getattr(self._context, _CLAIM, None)
            if owner is not None:
                if not self._is_orphan(owner):
                    raise PluginLifecycleError()
                stage = "orphan_claim"
                await self._stop_orphan(owner, settings["listen_port"], host)
            # Recheck after awaiting shutdown; never overwrite a racing successor.
            # Check/set are atomic on this loop and stored on the real Context.
            if self._terminating or getattr(self._context, _CLAIM, None) is not None:
                raise PluginLifecycleError()
            setattr(self._context, _CLAIM, self)
            self._claimed = True
            stage = "tools"
            tools, estimator = (self._tools_factory or load_tools)(self._context)
            self._native = NativeCallbacks(self._context, tools, estimator, settings)
            if self._terminating:
                raise PluginLifecycleError()
            stage = "http"
            # First own the configured listener with a fail-closed gate. Never
            # launch a public tunnel against an unowned/occupied local port.
            self._app = HostASGI(
                None,
                lambda: self.ready,
                ("127.0.0.1",)
                if settings["access_mode"] == "quick_tunnel"
                else settings["trusted_proxies"],
                self._settings.rejections,
                settings["origin"],
            )
            self._server = (self._server_factory or NativeServer)(
                self._app, settings["listen_port"]
            )
            self._server.host = host
            await self._server.start()
            self._server.task.add_done_callback(self._server_done)
            if self._terminating or not self._server.ready:
                raise PluginLifecycleError()
            if settings["access_mode"] == "quick_tunnel":
                stage = "tunnel"
                from .quick_tunnel import QuickTunnel

                self._tunnel = QuickTunnel(
                    settings["cloudflared_path"],
                    settings["listen_port"],
                    settings["tunnel_start_timeout"],
                )
                settings["origin"] = await self._tunnel.start()
                self._tunnel.task.add_done_callback(self._server_done)
            if self._terminating or not self._server.ready:
                raise PluginLifecycleError()
            stage = "data_dir"
            directory = Path(await asyncio.to_thread(self._data_dir))
            if not directory.is_absolute() or self._terminating:
                raise PluginLifecycleError()
            stage = "backend"
            from .backend import BackendConfig, CallBackend
            from .limits import DEFAULT_IDLE_SECONDS, RuntimeLimits

            config = BackendConfig(
                directory / DATABASE_NAME,
                settings["origin"],
                3600,
                RuntimeLimits(idle_seconds=DEFAULT_IDLE_SECONDS),
                max_audio_bytes=settings["max_audio_bytes"],
                call_vad_silence_ms=settings["call_vad_silence_ms"],
                call_vad_sensitivity=settings["call_vad_sensitivity"],
                call_max_continuous_segments=settings["call_max_continuous_segments"],
                audio_retention_days=settings["audio_retention_days"],
                call_capture_mode=settings["call_capture_mode"],
                call_barge_in=settings["call_barge_in"],
                call_backchannel_phrases=tuple(settings["call_backchannel_phrases"]),
                review_prompt=review_prompt,
                review_voice=settings["review_voice"],
            )
            self._backend = CallBackend(
                config,
                self._native.tools,
                select_policies=self._native.select,
                send_page=self._native.send_page,
                make_context_readable=self._native.import_context,
                timings=self._timings,
            )
            await self._backend.start()
            stage = "ready"
            self._app.app = self._backend.app
            self._backend.app.rejections = self._settings.rejections
            self._app.origin = settings["origin"]
            if (
                self._terminating
                or not self._server.ready
                or (self._tunnel is not None and not self._tunnel.ready)
            ):
                raise PluginLifecycleError()
            self._ready = True
            self._register_management()
        except asyncio.CancelledError:
            self._ready, self._terminating = False, True
            self._schedule_stop()
            raise asyncio.CancelledError() from None
        except Exception as error:
            self._startup_diagnostic = _startup_note(self, stage, error)
            self._ready, self._terminating = False, True
            self._schedule_stop()
            failure = PluginLifecycleError()
            _attach_startup_note(failure, self._startup_diagnostic)
            raise failure from None

    def _server_done(self, task):
        _consume(task)
        self._ready = False
        if not self._terminating:
            self._server_failed = True
            self._terminating = True
            self._schedule_stop()

    def _schedule_stop(self):
        if self._stop_task is None or (
            self._stop_task.done()
            and (self._stop_task.cancelled() or self._stop_task.exception() is not None)
        ):
            self._stop_task = asyncio.create_task(self._stop_resources(), name="BotCall:host-stop")
            self._stop_task.add_done_callback(_consume)
        return self._stop_task

    async def _stop_resources(self):
        task = self._benchmark.task
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._ready = False
        self._stop_management()
        if self._start_task is not None and not self._start_task.done():
            await asyncio.wait({self._start_task})
        # Gate already rejects new requests. Stop backend/tunnel concurrently,
        # retaining the HTTP listener until the tunnel has actually been reaped.
        # Never free a still-published port for an unrelated local service.
        tasks = []
        if self._tunnel is not None and self._tunnel.failed:
            self._server_failed = True  # Preserve an exit racing with terminate.
        if self._backend is not None:
            tasks.append(asyncio.create_task(self._backend.shutdown(timeout=None)))
        tunnel_stop = None
        if self._tunnel is not None:
            tunnel_stop = asyncio.create_task(self._tunnel.close())
            tasks.append(tunnel_stop)
        if self._server is not None:

            async def close_http():
                if tunnel_stop is not None:
                    await _wait(tunnel_stop)
                    if not self._tunnel.stopped:
                        raise PluginLifecycleError() from None
                return await self._server.close()

            tasks.append(asyncio.create_task(close_http()))
        invites = tuple(record[1] for record in self._invitations)
        for task in invites:
            if not task.done():
                task.cancel()  # Once; an actual client may suppress it.
        results = await asyncio.gather(*tasks, *invites, return_exceptions=True)
        if (
            (
                self._backend is not None
                and (not results or not getattr(results[0], "stopped", False))
            )
            or (self._server is not None and not self._server.stopped)
            or (self._tunnel is not None and not self._tunnel.stopped)
        ):
            raise PluginLifecycleError() from None
        # All backend/SDK owners and invitations are really stopped above.
        # A failure before that point must retain this protection and the claim.
        if self._native is not None:
            self._native.privacy.close()
        if self._claimed and getattr(self._context, _CLAIM, None) is self:
            delattr(self._context, _CLAIM)
            self._claimed = False
        # Actual resource release and a clean shutdown result are distinct. Keep
        # reporting fixed faults even if a later terminate reaps stopped resources.
        if (
            self._server_failed
            or any(isinstance(r, BaseException) for r in results[: len(tasks)])
            or (self._backend is not None and getattr(results[0], "error_code", None) is not None)
        ):
            raise PluginLifecycleError() from None

    def _register_entry(self, event, *, expected=None):
        """Common registration before first await; owned tasks retain all work."""
        self._check_loop()
        if not self.ready:
            return "not_ready"
        stage = "event"
        try:
            from .binding import BindingError, private_event_identity, reliable_message_id

            try:
                sender, self_id, platform, platform_type, kind, session, umo = (
                    private_event_identity(event)
                )
                if expected is not None:
                    expected.check_event(event)
            except BindingError:
                return "unsupported_event"
            if platform_type not in {"aiocqhttp", "qq_official"}:
                return "unsupported_event"
            message_id = getattr(event.message_obj, "message_id", None)
            reliable = reliable_message_id(message_id)
            key = (
                (platform, platform_type, self_id, sender, session, umo, kind, message_id)
                if reliable
                else None
            )
            record = self._events.get(key) if reliable else None
            if record is not None and expected is not None and record[2] != expected:
                # A command without original-request evidence cannot be upgraded
                # while already preparing. Never replace its original target.
                return "entry_conflict"
            if record is None:
                stage = "begin"
                handle = self._backend.begin(
                    expected if expected is not None else event, expected=expected
                )
                task = asyncio.create_task(
                    self._deliver_invitation(handle), name="BotCall:invitation"
                )
                task.add_done_callback(_consume)
                record = (handle, task, expected)
                self._invitations.append(record)
                if reliable:
                    self._events[key] = record
            return record
        except Exception as error:
            failure = PluginLifecycleError()
            attach_prepare(failure, stage, error, propagate=True)
            raise failure from None

    async def _entry_availability(self, event):
        from .binding import private_event_identity, BindingError

        try:
            identity = private_event_identity(event)
            if identity[3] not in {"aiocqhttp", "qq_official"}:
                return "unsupported_event"
        except BindingError:
            return "unsupported_event"
        from .astrbot_audio import STTUnavailable
        from .speech import SpeechUnavailable

        try:
            await self._native.check_stt(identity[6])
        except STTUnavailable:
            return "stt_unavailable"
        try:
            await self._native.check_tts(identity[6])
        except SpeechUnavailable:
            return "tts_unavailable"
        try:
            self._native.check_call_model()
        except Exception:
            return "chat_unavailable"
        return None

    async def initiate(self, event):
        if not self.ready:
            return "not_ready"
        reason = await self._entry_availability(event)
        if reason:
            return reason
        record = self._register_entry(event)
        if type(record) is str:
            return record
        try:
            return await _wait(record[1])
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception as error:
            failure = PluginLifecycleError()
            attach_prepare(failure, "wait", error, propagate=True)
            raise failure from None

    async def initiate_tool(self, event):
        # Old tool objects can survive in already-running native runners.
        self._check_loop()
        if not self.ready or not self._claimed or getattr(self._context, _CLAIM, None) is not self:
            return "通话插件尚未就绪，本次未登记通话请求。"
        try:
            expected = self._native.tool_entry(event)
            reason = await self._entry_availability(event)
            record = reason or self._register_entry(event, expected=expected)
            if type(record) is not str:
                return "发起请求已登记，邀请尚未确认发送。"
            if record == "stt_unavailable":
                return "本来源未选用可用STT，本次未登记通话请求。"
            if record == "tts_unavailable":
                return "本来源未选用可用TTS，本次未登记通话请求。"
            if record == "chat_unavailable":
                return "通话专用模型不存在、未启用或不是聊天模型；请检查插件的通话专用模型设置，本次未登记请求，不自动回退。"
        except Exception:
            pass
        return "本次未登记通话请求：权限、明确发起意图或原会话信息未通过检查。"

    async def _deliver_invitation(self, handle):
        from .publication import Outcome

        grant = await self._backend.invitation(handle)
        state = await self._backend.read_state(handle)
        if not self.ready:
            return Outcome.NOT_APPLIED
        link = grant.link(self._native.settings["origin"] + "/bot-call/")
        return await self._native.send_text(state.binding, link.value)

    def begin_recovery(self, call_id):
        """Trusted host only; never an event argument or HTTP route."""
        self._check_loop()
        if not self.ready:
            raise PluginLifecycleError() from None
        return self._backend.begin_recovery(call_id, counters=self._native.counters)

    async def recover(self, handle, *, retry=False):
        self._check_loop()
        if not self.ready:
            raise PluginLifecycleError() from None
        return await self._backend.recover(handle, retry=retry)

    async def terminate(self):
        self._stop_management()
        self._check_loop()
        self._ready, self._terminating = False, True
        if self._terminate_task is None or (
            self._terminate_task.done()
            and (self._terminate_task.cancelled() or self._terminate_task.exception() is not None)
        ):
            self._terminate_task = asyncio.create_task(
                self._terminate(), name="BotCall:host-terminate"
            )
            self._terminate_task.add_done_callback(_consume)
        await _wait(self._terminate_task)

    async def _terminate(self):
        if self._start_task is not None:
            try:
                await _wait(self._start_task)
            except (Exception, asyncio.CancelledError):
                pass
        await _wait(self._schedule_stop())
