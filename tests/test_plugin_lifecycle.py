"""Native lifecycle integration and scoped privacy. No live framework/server."""

import asyncio
import logging
from pathlib import Path
from unittest.mock import patch

from bot_call.plugin import PluginLifecycle
from bot_call.sdk_privacy import ProviderLogPrivacy, SDKPrivacyError
from bot_call.native_host import NativeCallbacks, settings_from
from bot_call.publication import Outcome
from test_backend import NativeCase, Estimator, TEXT


class Server:
    def __init__(self, app, port):
        self.ready = False
        self.stopped = False
        self.release = asyncio.Event()
        self.task = None

    async def start(self):
        self.ready = True
        self.task = asyncio.create_task(self.release.wait())

    async def close(self):
        self.ready = False
        self.release.set()
        if self.task is not None:
            await self.task
        self.stopped = True


class NativeLifecycleTests(NativeCase):
    async def test_native_command_to_audio_review_plain_pages_and_original_context(self):
        event, provider = self.environment()
        config = self.settings()
        # Obsolete capability/legacy STT fields no longer control native selection.
        config.update(model_capabilities="invalid obsolete table", stt_provider_id="obsolete-id")
        self.assertNotIn("capabilities", settings_from(config))
        self.assertNotIn("stt", settings_from(config))
        (self.path / "plugin").mkdir()
        lifecycle = PluginLifecycle(
            config,
            lambda: str(self.path / "plugin"),
            context=self.context,
            tools_factory=lambda context: (self.tools(), Estimator()),
            server_factory=Server,
            version="arbitrary-version",
        )
        self.addAsyncCleanup(lifecycle.terminate)

        async def send(native, bound, text):
            self.sent.append((bound, text))
            return Outcome.SUBMITTED

        with (
            patch.object(PluginLifecycle, "_register_management", lambda self: None),
            patch.object(NativeCallbacks, "send_text", send),
        ):
            await lifecycle.initialize()
            data = await lifecycle._diagnostic_data()
            self.assertEqual(data["provider_integration"], "astrbot_main_agent_v3")
            self.assertEqual(data["output_limits_managed_by"], "astrbot")
            self.assertEqual(data["version"], "arbitrary-version")
            self.assertEqual(provider.calls, [])
            backend = lifecycle._backend
            handle, web = await self.enter(backend, event)
            for index in range(2):
                await backend.web_submit(web.token, web.reference, "u" + str(index), TEXT)
                await backend.drain()
            await backend.web_end(web.token, web.reference)
            await backend.drain()
            state = await backend.read_state(handle)
            self.assertTrue(state.publication.complete)
            self.assertTrue(state.review_work.manifest.is_native)
            self.assertEqual(len(provider.calls), 4)
            await lifecycle.terminate()
            self.assertIsNone(await lifecycle._diagnostic_data())

    async def test_scoped_sdk_logging_concurrent_cancel_and_constructor_lease(self):
        # Legacy method name retained; no SDK installation/client inspection.
        logger = logging.getLogger("astrbot")
        messages = []

        class Capture(logging.Handler):
            def emit(self, record):
                messages.append(record.getMessage())

        handler = Capture()
        logger.addHandler(handler)
        old = logger.level
        logger.setLevel(logging.INFO)
        self.addCleanup(logger.removeHandler, handler)
        self.addCleanup(logger.setLevel, old)
        lease = ProviderLogPrivacy()
        other = ProviderLogPrivacy()
        try:
            with lease.scope():
                logger.info("synthetic scoped private")

                async def child():
                    logger.info("synthetic child private")

                await asyncio.create_task(child())
                with self.assertRaises(SDKPrivacyError):
                    lease.close()
            logger.info("outside scope")
            self.assertEqual(messages, ["outside scope"])
            lease.close()
            other.check()
            with other.scope():
                logger.info("other scoped private")
            self.assertEqual(messages, ["outside scope"])
        finally:
            lease.close()
            other.close()


from copy import deepcopy
from contextlib import contextmanager
from dataclasses import replace
import importlib.util
import json
import os
import subprocess
import sys
import threading
import traceback
from types import ModuleType, SimpleNamespace as NS
import bot_call
from bot_call.native_host import NativeServer
from bot_call.native_provider import COUNTER_RULE as ESTIMATE_RULE
from bot_call.plugin import PluginLifecycleError, _CLAIM
from bot_call.store import Store
from bot_call.call_policy import CallPolicy
from bot_call.review_policy import ReviewManifest, ReviewPolicyUnavailable
from bot_call.backend import BackendError
import test_backend as fixture


_TUNNEL_PROXY_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
)


class ObservedEstimator:
    """Pure native counter surface; observations attest complete payload/thread."""

    def __init__(self):
        self.records = []

    def count_tokens(self, messages, trusted_token_usage=0):
        payload = messages[0].model_dump()["content"]
        self.records.append((payload, threading.get_ident(), trusted_token_usage))
        return int(len(payload) * 0.3)


class ReadinessServer:
    """No sockets. Readiness is an owned bind receipt, independent of lifespan."""

    def __init__(self, app, port):
        self.app, self.port = app, port
        self.bound, self.release, self.entered = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.task = None
        self.stopped = False
        self.closing = False
        self.auto_bind = True

    @property
    def ready(self):
        return (
            self.bound.is_set()
            and self.task is not None
            and not self.task.done()
            and not self.closing
        )

    async def start(self):
        self.task = asyncio.create_task(self.run())
        self.entered.set()
        if self.auto_bind:
            self.bound.set()
        await self.bound.wait()
        if not self.ready:
            raise RuntimeError(fixture.PRIVATE)

    async def run(self):
        await self.release.wait()
        self.stopped = True

    async def close(self):
        self.closing = True
        self.release.set()
        await asyncio.wait({self.task})
        return self.stopped


class TunnelProcess:
    """In-memory pipe/process, including a separate actual wait receipt."""

    def __init__(self, gates):
        self.stdout = asyncio.StreamReader(limit=16384)
        self.returncode = None
        self.exited, self.reap = asyncio.Event(), asyncio.Event()
        self.termed, self.killed = asyncio.Event(), asyncio.Event()
        self.reap.set()
        gates.extend([self.exited, self.reap])
        self.ignore_term = False
        self.waited = False

    def log(self, message, **fields):
        self.stdout.feed_data(
            (json.dumps(dict(level="info", message=message, **fields)) + "\n").encode()
        )

    def address(self, origin="https://offline-call.trycloudflare.com"):
        self.log("|  " + origin + "    |")

    def registration(self):
        self.log(
            "Registered tunnel connection",
            connIndex=0,
            connection="32c6f832-218a-4ac2-9d4a-46c36ca01342",
            location="syd01",
            ip="192.0.2.1",
            protocol="http2",
        )

    def finish(self, code=1):
        if self.returncode is None:
            self.returncode = code
            self.stdout.feed_eof()
            self.exited.set()

    def terminate(self):
        self.termed.set()
        if not self.ignore_term:
            self.finish(-15)

    def kill(self):
        self.killed.set()
        self.finish(-9)

    async def wait(self):
        await self.exited.wait()
        await self.reap.wait()
        self.waited = True
        return self.returncode


class RestoredLifecycleTests(NativeCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.root = Path(__file__).resolve().parents[1]
        self.data = self.path / "native"
        self.loop = asyncio.get_running_loop()
        self.thread = threading.get_ident()
        self.directory_calls = []
        self.plugins = []
        self.servers = []
        self.qq_calls = []
        self.estimator = ObservedEstimator()
        self.qq_mode = "good"
        self.context_routes = []
        self.qq_entered = asyncio.Event()
        self.qq_release = asyncio.Event()
        self.gates.append(self.qq_release)
        self.stt_mode = "ok"
        self.stt_providers = {}
        self.stt_entered = asyncio.Event()
        self.stt_cancelled = asyncio.Event()
        self.stt_release = asyncio.Event()
        self.stt_release.set()
        self.gates.append(self.stt_release)
        self.entry = self.load_stub()
        self.context.platform_manager = NS(platform_insts=[])

        async def send_message(umo, chain):
            self.on_business()
            self.context_routes.append(umo)
            if self.qq_mode == "false":
                return False
            platform_id, kind, session_id = umo.split(":", 2)
            for platform in self.context.platform_manager.platform_insts:
                if platform.meta().id == platform_id:
                    await platform.send_by_session(
                        NS(
                            umo=umo,
                            platform_name=platform_id,
                            message_type=NS(value=kind),
                            session_id=session_id,
                        ),
                        chain,
                    )
                    return True
            return False

        self.context.send_message = send_message

    async def asyncTearDown(self):
        for gate in self.gates:
            gate.set()
        for server in self.servers:
            if hasattr(server, "bound"):
                server.bound.set()
                server.release.set()
        for plugin in reversed(self.plugins):
            try:
                await asyncio.wait_for(plugin.terminate(), 5)
            except PluginLifecycleError:
                pass
            backend = plugin._lifecycle._backend
            if backend is not None and backend._runtime is not None:
                self.assertFalse(backend._runtime._thread.is_alive())
                self.assertFalse(backend._runtime._core_tasks)
        await super().asyncTearDown()

    def on_business(self):
        self.assertEqual(threading.get_ident(), self.thread)

    async def request(self, app, route, **kwargs):
        return await fixture.web_fixture.WebAccessTests.request(self, app, route, **kwargs)

    def environment(self, platform="qq-A", cid="A"):
        event, provider = NativeCase.environment(self, platform, cid)
        stt = self.context.stt[event.unified_msg_origin]
        stt.calls = stt.paths
        self.stt_providers[event.unified_msg_origin] = stt

        async def transcribe(path):
            self.stt_entered.set()
            if self.stt_mode == "stubborn":
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    self.stt_cancelled.set()
                    await self.stt_release.wait()
            return fixture.TEXT

        stt.transcriber = transcribe
        return event, provider, stt

    def tools(self, context):
        return NativeCase.tools(self), self.estimator

    def load_stub(self):
        api, star, event = (
            ModuleType("astrbot.api"),
            ModuleType("astrbot.api.star"),
            ModuleType("astrbot.api.event"),
        )
        owner = self
        api.logger = logging.getLogger("astrbot")
        from contextvars import ContextVar

        self.management_request = ContextVar("management_request")
        web = ModuleType("astrbot.api.web")

        class RequestProxy:
            def __getattr__(_, name):
                return getattr(owner.management_request.get(), name)

        web.request = RequestProxy()
        web.json_response = lambda data, status_code=200, headers=None: NS(
            data=data, status_code=status_code, headers=headers
        )
        self.context.registered_web_apis = []

        def register_web_api(route, handler, methods, desc):
            for index, row in enumerate(self.context.registered_web_apis):
                if row[0] == route and row[2] == methods:
                    self.context.registered_web_apis[index] = (route, handler, methods, desc)
                    return
            self.context.registered_web_apis.append((route, handler, methods, desc))

        self.context.register_web_api = register_web_api

        class Star:
            def __init__(self, context, config=None):
                self.context = context

        class StarTools:
            @staticmethod
            def get_data_dir(name):
                owner.directory_calls.append((name, threading.get_ident()))
                owner.data.mkdir(exist_ok=True)
                return owner.data

        class CustomFilter:
            def __init__(self, raise_error=True):
                self.raise_error = raise_error

        class Filters:
            EventMessageType = NS(PRIVATE_MESSAGE="private")

            @staticmethod
            def record(kind, value):
                def decorate(method):
                    method.filters = getattr(method, "filters", []) + [(kind, value)]
                    return method

                return decorate

            custom_filter = staticmethod(lambda value: Filters.record("probe", value()))
            command = staticmethod(lambda value: Filters.record("command", value))
            event_message_type = staticmethod(lambda value: Filters.record("type", value))
            llm_tool = staticmethod(lambda *, name: Filters.record("llm_tool", name))

        class MessageChain:
            def __init__(self):
                self.chain = []

            def message(self, text):
                self.chain.append(NS(text=text))
                return self

        Filters.CustomFilter = CustomFilter
        star.Star, star.StarTools = Star, StarTools
        event.filter, event.MessageChain = Filters, MessageChain
        name = "native_host_stub_" + str(id(self))
        package = ModuleType(name)
        package.__path__ = [str(self.root)]
        modules = patch.dict(
            sys.modules,
            {
                "astrbot": ModuleType("astrbot"),
                "astrbot.api": api,
                "astrbot.api.star": star,
                "astrbot.api.event": event,
                "astrbot.api.web": web,
                name: package,
                name + ".bot_call": bot_call,
                **{
                    name + "." + key: value
                    for key, value in tuple(sys.modules.items())
                    if key.startswith("bot_call.")
                },
            },
        )
        modules.start()
        self.addCleanup(modules.stop)
        spec = importlib.util.spec_from_file_location(name + ".main", self.root / "main.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertNotIn("__del__", module.BotCallPlugin.__dict__)
        return module

    def settings(self):
        return dict(
            enabled=True,
            public_origin=fixture.ORIGIN,
            listen_port=6191,
            reply_tokens=180,
            review_tokens=300,
            safety_tokens=100,
            max_audio_bytes=512,
            max_wav_bytes=4096,
        )

    def plugin(self, config=None):
        plugin = self.entry.BotCallPlugin(self.context, config)
        plugin._lifecycle._tools_factory = self.tools

        def server(app, port):
            owned = ReadinessServer(app, port)
            self.servers.append(owned)
            return owned

        plugin._lifecycle._server_factory = server
        self.plugins.append(plugin)
        return plugin

    def quick_settings(self):
        return dict(
            self.settings(),
            access_mode="quick_tunnel",
            public_origin="",
            cloudflared_path="/usr/bin/true",
            tunnel_start_timeout=1,
        )

    @contextmanager
    def tunnel_process(self, plugin, mode="good", delayed=False, *, parent_env=None, expected=None):
        proc = TunnelProcess(self.gates)
        entered, release = asyncio.Event(), asyncio.Event()
        self.gates.append(release)
        if not delayed:
            release.set()

        async def spawn(*args, **kwargs):
            # The fake receives precisely the real exec call, never executes it.
            server = plugin._lifecycle._server
            self.assertTrue(server.ready)
            self.assertIsNone(plugin._lifecycle._backend)
            self.assertFalse(plugin._lifecycle.ready)
            denied = await self.request(server.app, "exchange", data={})
            self.assertEqual(denied[0], 503)
            self.assertEqual(args[0], str(Path("/usr/bin/true").resolve()))
            self.assertEqual(args[1], "tunnel")
            for flag, value in [
                ("--output", "json"),
                ("--protocol", "auto"),
                ("--url", "http://127.0.0.1:6191"),
                ("--metrics", "127.0.0.1:0"),
            ]:
                self.assertEqual(args[args.index(flag) + 1], value)
            self.assertIn("--no-autoupdate", args)
            self.assertNotIn("--http-host-header", args)
            config = Path(args[args.index("--config") + 1])
            self.assertEqual(config.read_text(), "{}\n")
            self.assertEqual(config.stat().st_mode & 0o777, 0o600)
            self.assertEqual(config.parent, Path(kwargs["cwd"]))
            self.assertEqual(config.parent.stat().st_mode & 0o777, 0o700)
            self.assertEqual(kwargs["env"], expected_env)
            self.assertEqual(kwargs["stdin"], asyncio.subprocess.DEVNULL)
            self.assertEqual(kwargs["stdout"], asyncio.subprocess.PIPE)
            self.assertEqual(kwargs["stderr"], asyncio.subprocess.STDOUT)
            entered.set()
            await release.wait()
            if mode == "spawn_failure":
                raise OSError(kwargs["env"].get("HTTPS_PROXY", fixture.PRIVATE))
            proc.address()
            if mode == "good":
                proc.registration()
            elif mode == "early_exit":
                proc.finish()
            elif mode == "ambiguous":
                proc.address("https://second.trycloudflare.com")
                proc.registration()
            elif mode == "invalid":
                proc.address("https://nested.bad.trycloudflare.com")
                proc.registration()
            elif mode == "long_line":
                proc.stdout.feed_data(b"x" * 8193)
            elif mode == "budget":
                proc.stdout.feed_data((b"{}\n") * 6000)
            return proc

        from bot_call import quick_tunnel

        with (
            patch("asyncio.create_subprocess_exec", new=spawn),
            patch.object(quick_tunnel, "START_OUTPUT_LIMIT", 16384),
            patch.dict(
                os.environ,
                {
                    "TUNNEL_URL": "http://unrelated.invalid",
                    "TUNNEL_TOKEN": fixture.PRIVATE,
                    "TUNNEL_NAME": "unrelated",
                    "TUNNEL_LOGFILE": "/unrelated.log",
                },
            ),
        ):
            for key in _TUNNEL_PROXY_KEYS:
                os.environ.pop(key, None)
            os.environ.update(parent_env or {})
            expected_env = {
                "PATH": os.environ.get("PATH") or os.defpath,
                "LANG": "C",
                "NO_COLOR": "1",
            }
            expected_env.update(expected or {})
            yield NS(proc=proc, entered=entered, release=release)

    async def test_quick_tunnel_inherits_path_or_uses_default(self):
        custom_path = str(self.path)
        for path, expected in ((custom_path, custom_path), (None, os.defpath), ("", os.defpath)):
            with self.subTest(path=path), patch.dict(os.environ):
                if path is None:
                    os.environ.pop("PATH", None)
                else:
                    os.environ["PATH"] = path
                plugin = self.plugin(self.quick_settings())
                with self.tunnel_process(plugin, expected={"PATH": expected}):
                    await plugin.initialize()
                    await plugin.terminate()

    async def test_quick_tunnel_inherits_only_nonempty_proxy_values(self):
        proxies = {
            "HTTP_PROXY": "http://proxy-a.example.com",
            "HTTPS_PROXY": "http://proxy-b.example.com",
            "ALL_PROXY": "socks5://proxy-c.example.com",
            "NO_PROXY": "internal-a.example.com,localhost",
            "http_proxy": "http://proxy-d.example.com",
            "https_proxy": "http://proxy-e.example.com",
            "all_proxy": "socks5://proxy-f.example.com",
            "no_proxy": "internal-b.example.com",
        }
        for empty_key in (None, "no_proxy"):
            with self.subTest(empty_key=empty_key):
                parent_env, expected = proxies.copy(), proxies.copy()
                if empty_key is not None:
                    parent_env[empty_key] = ""
                    del expected[empty_key]
                plugin = self.plugin(self.quick_settings())
                with self.tunnel_process(plugin, parent_env=parent_env, expected=expected):
                    await plugin.initialize()
                    await plugin.terminate()

    async def test_quick_tunnel_isolates_unrelated_environment(self):
        parent_env = dict.fromkeys(
            (
                "HOME",
                "XDG_CONFIG_HOME",
                "LD_PRELOAD",
                "LD_LIBRARY_PATH",
                "DYLD_INSERT_LIBRARIES",
                "SSH_AUTH_SOCK",
                "OPENAI_API_KEY",
            ),
            fixture.PRIVATE,
        )
        plugin = self.plugin(self.quick_settings())
        # The fixture also supplies TUNNEL_URL, TUNNEL_TOKEN, TUNNEL_NAME and TUNNEL_LOGFILE.
        with self.tunnel_process(plugin, parent_env=parent_env):
            await plugin.initialize()
            await plugin.terminate()

    async def test_quick_tunnel_spawn_failure_omits_proxy_credentials(self):
        proxy = {"HTTPS_PROXY": f"http://user:{fixture.PRIVATE}@proxy.example.com"}
        plugin = self.plugin(self.quick_settings())
        with (
            self.tunnel_process(plugin, "spawn_failure", parent_env=proxy, expected=proxy),
            self.assertRaises(PluginLifecycleError) as caught,
        ):
            await plugin.initialize()
        diagnostic = self.startup_diagnostic(caught.exception, "tunnel")
        self.assertTrue(any(item["class"] == "OSError" for item in diagnostic["errors"]))

    def startup_diagnostic(self, error, stage):
        self.assertEqual(str(error), "bot_call.host_unavailable")
        notes = error.__notes__
        self.assertLessEqual(len(notes), 2)
        self.assertEqual(len(notes), len(set(notes)))
        diagnostic = json.loads(
            next(note for note in notes if note.startswith("bot_call.startup {"))[
                len("bot_call.startup ") :
            ]
        )
        self.assertEqual(diagnostic["stage"], stage)
        self.assertLessEqual(len(diagnostic["errors"]), 8)
        for item in diagnostic["errors"]:
            self.assertLessEqual(len(item["frames"]), 2)
            for frame in item["frames"]:
                self.assertTrue(frame["file"].startswith("bot_call/"))
                self.assertNotIn("..", frame["file"])
                self.assertGreater(frame["line"], 0)
        rendered = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        for forbidden in (
            fixture.PRIVATE,
            "must-not-export.invalid",
            "offline-call.trycloudflare.com",
            str(self.root),
            "The above exception",
            "During handling",
        ):
            self.assertNotIn(forbidden, "\n".join(notes))
            # Standard formatting contains its own local traceback file paths;
            # only notes must omit paths, while formatted output must omit data.
            if forbidden != str(self.root):
                self.assertNotIn(forbidden, rendered)
        return diagnostic

    async def quick_failures(self):
        # Missing executable is a fixed failure, never a download/fallback.
        plugin = self.plugin(self.quick_settings())
        with patch("shutil.which", return_value=None):
            with self.assertRaises(PluginLifecycleError) as caught:
                await plugin.initialize()
        self.startup_diagnostic(caught.exception, "tunnel")
        self.assertIsNone(plugin._lifecycle._backend)
        self.assertFalse(hasattr(self.context, _CLAIM))
        for mode in (
            "spawn_failure",
            "address_only",
            "early_exit",
            "ambiguous",
            "invalid",
            "long_line",
            "budget",
        ):
            plugin = self.plugin(self.quick_settings())
            with self.tunnel_process(plugin, mode) as run:
                with self.assertRaises(PluginLifecycleError) as caught:
                    await plugin.initialize()
                diagnostic = self.startup_diagnostic(caught.exception, "tunnel")
                if mode == "address_only":
                    self.assertEqual(
                        diagnostic["tunnel"],
                        {"origin_present": True, "registered": False, "child_returncode": None},
                    )
                elif mode == "spawn_failure":
                    self.assertFalse(diagnostic["tunnel"]["origin_present"])
                    self.assertTrue(
                        any(item["class"] == "OSError" for item in diagnostic["errors"])
                    )
                elif mode == "early_exit":
                    self.assertEqual(diagnostic["tunnel"]["child_returncode"], 1)
                self.assertIsNone(plugin._lifecycle._backend)
                tunnel = plugin._lifecycle._tunnel
                self.assertTrue(tunnel.stopped)
                self.assertTrue(tunnel.task.done())
                self.assertFalse(Path(tunnel._directory.name).exists())
                if mode != "spawn_failure":
                    self.assertTrue(run.proc.waited)
                self.assertFalse(hasattr(self.context, _CLAIM))
        # Port bind failure occurs BEFORE attempting even a subprocess spawn.
        plugin = self.plugin(self.quick_settings())

        class Occupied(Server):
            async def start(inner):
                inner.task = asyncio.create_task(inner.run())
                raise OSError(fixture.PRIVATE)

        def occupied(app, port):
            server = Occupied(app, port)
            self.servers.append(server)
            return server

        plugin._lifecycle._server_factory = occupied
        with patch("asyncio.create_subprocess_exec", side_effect=AssertionError("unowned port")):
            with self.assertRaises(PluginLifecycleError):
                await plugin.initialize()
        self.assertIsNone(plugin._lifecycle._tunnel)
        self.assertFalse(hasattr(self.context, _CLAIM))

    async def quick_success_and_exit(self, event):
        plugin = self.plugin(self.quick_settings())
        origin = "https://offline-call.trycloudflare.com"
        with self.tunnel_process(plugin, "address_only") as run:
            starting = asyncio.create_task(plugin.initialize())
            await asyncio.wait_for(run.entered.wait(), 2)
            for _ in range(8):
                await asyncio.sleep(0)
            self.assertEqual(plugin._lifecycle._tunnel.origin, origin)
            self.assertFalse(plugin._lifecycle.ready)
            self.assertFalse(starting.done())
            run.proc.registration()
            await starting
            host = plugin._lifecycle
            backend = host._backend
            self.assertEqual(host._native.settings["origin"], origin)
            self.assertEqual(await self.dispatch(plugin, event), Outcome.SUBMITTED)
            message = self.qq_calls[-1][1]["message"][0]["data"]["text"]
            self.assertTrue(message.startswith(origin + "/bot-call/"))
            handle = host._invitations[0][0]
            grant = backend._record(handle).grant
            app = self.proxy_app(host)
            headers = {"host": "offline-call.trycloudflare.com", "origin": origin}
            for bad in (
                {"host": "other.trycloudflare.com", "origin": origin},
                {"host": headers["host"], "origin": "https://other.trycloudflare.com"},
            ):
                self.assertEqual(
                    (
                        await self.request(
                            app, "exchange", data={"invitation": grant.secret.value}, headers=bad
                        )
                    )[0],
                    403,
                )
            result = await self.request(
                app, "exchange", data={"invitation": grant.secret.value}, headers=headers
            )
            self.assertEqual(result[0], 200)
            cookie = result[2][b"set-cookie"].decode().split(";", 1)[0]
            ref = result[1]["reference"]
            self.assertEqual(
                (
                    await self.request(
                        app,
                        "audio",
                        cookie=cookie,
                        reference=ref,
                        raw=b"quick-material",
                        headers=dict(
                            headers, **{"content-type": "audio/webm", "x-bot-utterance-id": "quick"}
                        ),
                    )
                )[0],
                200,
            )
            await backend.drain()
            state = await backend.read_state(handle)
            self.assertTrue(state.turns)
            call_id = state.binding.call_id
            # More than the startup budget after ready is read/discarded safely.
            for _ in range(3):
                run.proc.stdout.feed_data(b"{}\n" * 2700)
                await asyncio.sleep(0)
            self.assertTrue(host.ready)
            run.proc.finish()
            self.assertFalse(host.ready)
            self.assertEqual(await self.dispatch(plugin, event), "not_ready")
            self.assertEqual((await self.request(host._app, "exchange", data={}))[0], 503)
            with self.assertRaises(PluginLifecycleError):
                await plugin.terminate()
            self.assertTrue(run.proc.waited)
            self.assertFalse(hasattr(self.context, _CLAIM))
            with Store(self.data / "calls.sqlite3") as store:
                self.assertEqual(
                    [t.body for t in store.get_transcript(call_id)], [t.body for t in state.turns]
                )
        # A fresh origin/backend cannot accept the previous cookie/reference.
        successor = self.plugin(self.quick_settings())
        with self.tunnel_process(successor) as run:
            await successor.initialize()
            self.assertEqual(
                (
                    await self.request(
                        self.proxy_app(successor._lifecycle),
                        "state",
                        cookie=cookie,
                        reference=ref,
                        headers=headers,
                    )
                )[0],
                401,
            )
            run.proc.address("https://changed.trycloudflare.com")
            for _ in range(8):
                await asyncio.sleep(0)
            self.assertFalse(successor._lifecycle.ready)
            self.assertEqual(await self.dispatch(successor, event), "not_ready")
            with self.assertRaises(PluginLifecycleError):
                await successor.terminate()
            self.assertTrue(run.proc.waited)

    async def quick_cancel_and_reload(self):
        plugin = self.plugin(self.quick_settings())
        with self.tunnel_process(plugin, delayed=True) as run:
            run.proc.ignore_term = True
            run.proc.reap.clear()
            first = asyncio.create_task(plugin.initialize())
            second = asyncio.create_task(plugin.initialize())
            await asyncio.wait_for(run.entered.wait(), 2)
            tunnel = plugin._lifecycle._tunnel
            tunnel.term_timeout = 0.01
            first.cancel(fixture.PRIVATE)
            with self.assertRaises(asyncio.CancelledError):
                await first
            self.assertFalse(tunnel._launch_task.done())
            ending = asyncio.create_task(plugin.terminate())
            await asyncio.sleep(0)
            ending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await ending
            # Module reload cannot acquire the claim while spawn/wait are pending.
            from bot_call.plugin import PluginLifecycle

            spec = importlib.util.spec_from_file_location(
                "bot_call.stage31_reload", self.root / "bot_call/plugin.py"
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            contender = module.PluginLifecycle(
                self.quick_settings(), lambda: self.data, context=self.context
            )
            with self.assertRaises(module.PluginLifecycleError):
                await contender.initialize()
            self.assertIs(getattr(self.context, _CLAIM), plugin._lifecycle)
            run.release.set()
            await asyncio.wait_for(run.proc.killed.wait(), 2)
            self.assertTrue(run.proc.termed.is_set())
            self.assertFalse(run.proc.waited)
            self.assertIs(getattr(self.context, _CLAIM), plugin._lifecycle)
            self.assertFalse(tunnel._close_task.done())
            self.assertTrue(plugin._lifecycle._server.ready)  # Keep the published port owned.
            self.assertEqual(
                (await self.request(plugin._lifecycle._app, "exchange", data={}))[0], 503
            )
            run.proc.reap.set()
            with self.assertRaises(PluginLifecycleError):
                await second
            await plugin.terminate()
            self.assertTrue(run.proc.waited)
            self.assertTrue(tunnel.stopped)
            self.assertTrue(
                all(
                    t.done()
                    for t in (
                        tunnel._launch_task,
                        tunnel.task,
                        tunnel._reader,
                        tunnel._exit,
                        tunnel._close_task,
                    )
                )
            )
            self.assertFalse(hasattr(self.context, _CLAIM))

    def host_event(
        self,
        platform="qq-A",
        cid="A",
        mid="1",
        *,
        platform_type="aiocqhttp",
        sender="12345",
        session=None,
    ):
        event, provider, stt = self.environment(platform, cid)
        old = event.unified_msg_origin
        session = sender if session is None else session
        event.unified_msg_origin = f"{platform}:FriendMessage:{session}"
        event.get_sender_id = lambda: sender
        event.get_session_id = lambda: session
        event.get_platform_name = lambda: platform_type
        event.get_self_id = lambda: "9988"
        event.message_obj = NS(message_id=mid)
        event.stopped = False
        event.stop_event = lambda: setattr(event, "stopped", True)
        event.is_stopped = lambda: event.stopped
        for mapping in (
            self.context.providers,
            self.context.configs,
            self.context.persona_manager.results,
            self.stt_providers,
            self.context.stt,
            self.context.tts,
            self.context.conversation_manager.current,
        ):
            mapping[event.unified_msg_origin] = mapping.pop(old)
        self.context.conversation_manager.items[cid].user_id = event.unified_msg_origin
        self.context.by_id["stt-" + cid] = self.stt_providers[event.unified_msg_origin]

        async def send_by_session(target, chain):
            self.on_business()
            self.qq_calls.append(
                (
                    platform,
                    {
                        "umo": target.umo,
                        "session_id": target.session_id,
                        "message": [
                            {"type": "text", "data": {"text": part.text}} for part in chain.chain
                        ],
                    },
                )
            )
            self.qq_entered.set()
            if self.qq_mode == "gate":
                await self.qq_release.wait()
            if self.qq_mode == "lost":
                raise RuntimeError(fixture.PRIVATE)
            if self.qq_mode == "cancel":
                raise asyncio.CancelledError()
            return None  # Standard platform API does not return a receipt.

        existing = next(
            p for p in self.context.platform_manager.platform_insts if p.meta().id == platform
        )
        source_meta = replace(existing.meta(), name=platform_type)
        existing.meta = lambda: source_meta
        existing.send_by_session = send_by_session
        return event, provider, stt

    async def dispatch(self, plugin, event, text="botcall"):
        # Real registration appends decorators bottom-up; WakingCheck runs
        # filters in order and breaks at the first rejection. Binding is partial.
        from functools import partial

        for kind, value in plugin.botcall.filters:
            if kind == "command" and text != value:
                return "ordinary_llm"
            if kind == "probe" and not value.filter(event, {}):
                return "ordinary_llm"
            if kind == "type" and event.get_message_type().value != "FriendMessage":
                return "ordinary_llm"
        return await partial(type(plugin).botcall, plugin)(event)

    def proxy_app(self, lifecycle, peer="127.0.0.1"):
        async def app(scope, receive, send):
            scope = dict(scope, scheme="http", client=(peer, 43210))
            scope["headers"] = scope["headers"] + [(b"x-forwarded-proto", b"https")]
            return await lifecycle.app(scope, receive, send)

        return app

    @contextmanager
    def reimported_host(self, config, *, server_factory=ReadinessServer):
        # Same module name, NEW module object, matching reload_failed_plugin.
        name = PluginLifecycle.__module__
        spec = importlib.util.spec_from_file_location(name, self.root / "bot_call/plugin.py")
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {name: module}):
            spec.loader.exec_module(module)
            host = module.PluginLifecycle(
                config,
                lambda: self.data,
                context=self.context,
                tools_factory=self.tools,
                server_factory=server_factory,
            )
            self.plugins.append(NS(_lifecycle=host, terminate=host.terminate))
            yield module, host

    def available_port(self):
        import socket

        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    async def test_orphan_reimport_stops_resources_then_rebinds_and_preserves_material(self):
        event, _, _ = self.host_event(mid="orphan")
        config = dict(self.settings(), listen_port=self.available_port())
        plugin = self.plugin(config)
        old = plugin._lifecycle
        old._server_factory = NativeServer
        await plugin.initialize()
        backend, server = old._backend, old._server
        handle, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "confirmed", TEXT)
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertTrue(state.turns)
        # Keep the call open: a reload must preserve already confirmed material.
        with self.reimported_host(config, server_factory=NativeServer) as (_, successor):
            self.assertIsNot(type(old), type(successor))
            with self.assertLogs("bot_call.plugin", level="INFO") as logs:
                await successor.initialize()
            self.assertTrue(successor.ready)
            self.assertIs(getattr(self.context, _CLAIM), successor)
            self.assertFalse(old._claimed)
            self.assertTrue(server.stopped)
            self.assertEqual(server._socket.fileno(), -1)
            self.assertFalse(backend._runtime._thread.is_alive())
            self.assertIsNot(server, successor._server)
            self.assertEqual(successor._server._socket.getsockname()[1], config["listen_port"])
            restored = await successor._backend._runtime.read_state(state.binding.call_id)
            self.assertEqual(restored.turns, state.turns)
            self.assertTrue(any("state=stopped elapsed_ms=" in row for row in logs.output))
            self.assertNotIn(fixture.PRIVATE, "".join(logs.output))
            await successor.terminate()
        # The normal owner path must also reap a published tunnel before rebinding.
        plugin = self.plugin(self.quick_settings())
        with self.tunnel_process(plugin) as run:
            await plugin.initialize()
            old = plugin._lifecycle
            with self.reimported_host(config) as (_, successor):
                await successor.initialize()
                self.assertTrue(successor.ready)
                self.assertTrue(old._tunnel.stopped)
                self.assertTrue(run.proc.waited)
                self.assertTrue(old._server.stopped)
                self.assertIs(getattr(self.context, _CLAIM), successor)
                await successor.terminate()

    async def test_claim_rejects_live_current_owner_and_unknown_or_incomplete_objects(self):
        plugin = self.plugin(self.settings())
        await plugin.initialize()
        old = plugin._lifecycle
        for owner in (old, object(), NS(_context=self.context, _claimed=True)):
            with self.subTest(owner=type(owner).__name__):
                setattr(self.context, _CLAIM, owner)
                contender = self.plugin(self.settings())
                with self.assertRaises(PluginLifecycleError) as caught:
                    await contender.initialize()
                self.startup_diagnostic(caught.exception, "claim")
                self.assertIs(getattr(self.context, _CLAIM), owner)
                self.assertIsNone(contender._lifecycle._server)
                self.assertIsNone(old._terminate_task)
                self.assertTrue(old.ready)
        setattr(self.context, _CLAIM, old)
        # A displaced module alone is insufficient when lifecycle state is missing.
        with self.reimported_host(self.settings()) as (module, contender):
            saved = vars(old).pop("_stop_task")
            try:
                with self.assertRaises(module.PluginLifecycleError) as caught:
                    await contender.initialize()
                self.startup_diagnostic(caught.exception, "claim")
                self.assertIsNone(old._terminate_task)
                self.assertTrue(old.ready)
                self.assertIs(getattr(self.context, _CLAIM), old)
            finally:
                old._stop_task = saved
        await plugin.terminate()

    async def test_orphan_stop_timeout_fault_and_occupied_port_remain_fail_closed(self):
        import socket

        for mode in ("timeout", "fault", "occupied"):
            with self.subTest(mode=mode):
                config = dict(self.settings(), listen_port=self.available_port())
                plugin = self.plugin(config)
                old = plugin._lifecycle
                old._server_factory = NativeServer
                await plugin.initialize()
                server = old._server
                release, entered = asyncio.Event(), asyncio.Event()
                self.gates.append(release)
                close = server.close

                async def blocked_close():
                    entered.set()
                    if mode == "fault":
                        raise RuntimeError(fixture.PRIVATE)
                    await release.wait()
                    return await close()

                with socket.socket() as occupant:
                    if mode != "occupied":
                        server.close = blocked_close
                    else:
                        # Simulate an unrelated process winning the port after stop.
                        occupant.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        occupant.bind(("127.0.0.1", self.available_port()))
                        occupant.listen()
                        config = dict(config, listen_port=occupant.getsockname()[1])
                    with self.reimported_host(config) as (module, contender):
                        with patch.object(module, "_ORPHAN_STOP_TIMEOUT", 0.03):
                            with self.assertRaises(module.PluginLifecycleError) as caught:
                                await contender.initialize()
                        diagnostic = self.startup_diagnostic(caught.exception, "orphan_claim")
                        if mode == "timeout":
                            self.assertTrue(entered.is_set())
                            self.assertFalse(old._terminate_task.done())
                            self.assertTrue(
                                any(r["class"] == "TimeoutError" for r in diagnostic["errors"])
                            )
                        self.assertFalse(contender.ready)
                        self.assertFalse(contender._claimed)
                        self.assertIsNone(contender._server)
                        self.assertIsNone(contender._tunnel)
                        self.assertIsNone(contender._backend)
                        self.assertIsNone(contender._native)
                        if mode != "occupied":
                            self.assertIs(getattr(self.context, _CLAIM), old)
                            self.assertFalse(server.stopped)
                            self.assertGreaterEqual(server._socket.fileno(), 0)
                        else:
                            self.assertFalse(hasattr(self.context, _CLAIM))
                            self.assertTrue(server.stopped)
                server.close = close
                release.set()
                await plugin.terminate()
                self.assertFalse(hasattr(self.context, _CLAIM))

    async def test_native_tool_permissions_snapshot_lock_dedup_and_shutdown(self):
        # Exercise the installed manager/proxy; only preferences are synthetic.
        import inspect
        from astrbot.core.provider import func_tool_manager as native_tools
        from astrbot.core.message.components import Plain
        from astrbot.core.provider.entities import ProviderRequest

        permission = {"value": {}, "fail": False}
        reads = []

        def get(key, default, **kwargs):
            reads.append((key, kwargs))
            self.assertEqual(
                (key, kwargs), ("tool_permissions", dict(scope="global", scope_id="global"))
            )
            if permission["fail"]:
                raise RuntimeError(fixture.PRIVATE)
            return permission["value"]

        async def global_get(key, default):
            return get(key, default, scope="global", scope_id="global")

        sp = NS(get=get, global_get=global_get)
        manager = native_tools.FunctionToolManager()
        self.context.get_llm_tool_manager = lambda: manager
        with patch.object(native_tools, "sp", sp), patch("astrbot.core.sp", sp):
            event, provider, stt = self.host_event(mid="tool-owned")
            extras = {}
            event.get_extra = lambda key, default=None: extras.get(key, default)
            event.set_extra = lambda key, value: extras.__setitem__(key, value)
            admin = [False]
            event.is_admin = lambda: admin[0]
            event.message_obj.message = [Plain("请现在发起通话吧！")]
            request = ProviderRequest(
                conversation=self.context.conversation_manager.items["A"],
                session_id=event.unified_msg_origin,
            )
            extras["provider_request"] = request
            plugin = self.plugin(self.settings())
            await plugin.initialize()
            host, backend = plugin._lifecycle, plugin._lifecycle._backend
            raw = manager.spec_to_func("bot_call_start", [], "start", plugin.bot_call_start)
            manager.func_list = [raw]
            guarded = manager.get_full_tool_set().tools[0]
            self.assertIsNone(guarded.handler)
            self.assertIs(manager.get_func("bot_call_start"), raw)  # explicit persona list
            self.assertEqual(raw.parameters, dict(type="object", properties={}))
            self.assertEqual(list(inspect.signature(plugin.bot_call_start).parameters), ["event"])
            self.assertEqual(plugin.bot_call_start.filters, [("llm_tool", "bot_call_start")])
            context = NS(context=NS(event=event))

            async def refused():
                result = await raw.handler(event)
                self.assertIn("未登记", result)
                self.assertNotIn(fixture.PRIVATE, result)
                self.assertNotIn(event.get_sender_id(), result)
                self.assertEqual(
                    (len(host._invitations), len(backend._preparations), len(self.qq_calls)),
                    (0, 0, 0),
                )

            permission["value"] = {"_default": {"bot_call_start": "admin"}}
            self.assertIn("Permission denied", await guarded.call(context))
            await refused()  # raw path also enforces admin
            admin[0] = True
            self.assertIsNotNone(host._native.tool_entry(event))
            admin[0] = False
            for value in (
                {"_default": {"bot_call_start": "unknown"}},
                {"_default": []},
                [],
                {"_default": {"bot_call_start": None}},
            ):
                permission["value"] = value
                await refused()
            permission["value"] = {}
            permission["fail"] = True
            # Native helper fails open, our own entry still refuses both routes.
            permission_result = manager._check_tool_permission("bot_call_start", context)
            self.assertIsNone(
                await permission_result
                if inspect.isawaitable(permission_result)
                else permission_result
            )
            await refused()
            self.assertIn("未登记", await guarded.call(context))
            permission["fail"] = False
            with patch.object(manager, "_default_permission", new=lambda name: "unknown"):
                await refused()
            for text in (
                "能不能通话",
                "不要开始通话",
                "明天发起通话",
                "如果现在开始通话",
                "“现在发起通话”",
                "转发：现在发起通话",
                "现在发起通话？",
                "现在发起通话" * 20,
            ):
                event.message_obj.message = [Plain(text)]
                await refused()
            event.message_obj.message = [Plain("现在发起通话"), NS(text="attachment")]
            await refused()
            event.message_obj.message = [Plain("现在发起通话")]
            for missing in (
                None,
                NS(conversation=request.conversation),
                ProviderRequest(None),
                NS(conversation=NS(cid="A", user_id="foreign", platform_id="qq-A")),
                ProviderRequest(conversation=request.conversation, session_id="foreign"),
            ):
                extras["provider_request"] = missing
                await refused()
            extras["provider_request"] = request
            for mid in (None, "", "  ", 0, True):
                event.message_obj.message_id = mid
                await refused()
            event.message_obj.message_id = "tool-owned"
            with self.assertRaises(TypeError):
                await raw.handler(event, cid="foreign")
            self.assertEqual(len(host._invitations), 0)
            registered = "发起请求已登记，邀请尚未确认发送。"
            umo = event.unified_msg_origin
            # Same non-reentrant manager as BackendTools/ReadAdapter, held by Agent.
            async with self.locks.acquire_lock(umo):
                self.locks.attempted.clear()
                self.assertEqual(await guarded.call(context), registered)
                self.assertFalse(event.stopped)
                self.assertEqual(await raw.handler(event), registered)
                self.assertEqual(len(host._invitations), 1)
                record = backend._record(host._invitations[0][0])
                self.assertIs(record.event, record.expected)
                self.assertFalse(hasattr(record.event, "get_extra"))
                self.assertNotIn(umo, repr(record.expected))
                original = request.conversation
                request.conversation = NS(cid="other", user_id=umo, platform_id="qq-A")
                self.assertIn("未登记", await raw.handler(event))
                request.conversation = original
                command = asyncio.create_task(self.dispatch(plugin, event))
                await asyncio.wait_for(self.locks.attempted.wait(), 2)
                self.assertFalse(command.done())
                self.assertTrue(event.stopped)
                self.assertEqual(
                    (record.binding, len(self.qq_calls), len(provider.calls), len(stt.calls)),
                    (None, 0, 0, 0),
                )
            self.assertEqual(await asyncio.wait_for(command, 2), Outcome.SUBMITTED)
            self.assertEqual((len(host._invitations), len(self.qq_calls)), (1, 1))
            self.assertNotIn("http", await raw.handler(event))
            # A -> B while queued: only current-pointer read, no conversation/history.
            event.stopped = False
            event.message_obj.message_id = "switch-before-bind"
            calls_before = list(self.context.conversation_manager.calls)
            async with self.locks.acquire_lock(umo):
                self.assertEqual(await raw.handler(event), registered)
                self.context.conversation_manager.current[umo] = "B"
                await asyncio.sleep(0)
            switched = host._invitations[-1]
            await asyncio.gather(switched[1], return_exceptions=True)
            self.assertIsNone(backend._record(switched[0]).binding)
            self.assertEqual(
                self.context.conversation_manager.calls[len(calls_before) :], [("current", umo)]
            )
            self.assertEqual((len(self.qq_calls), len(provider.calls), len(stt.calls)), (1, 0, 0))
            self.context.conversation_manager.current[umo] = "A"
            # ACK unknown is retained and never re-sent on repeated calls.
            self.qq_mode = "lost"
            event.message_obj.message_id = "tool-ack-unknown"
            self.assertEqual(await raw.handler(event), registered)
            lost = host._invitations[-1]
            self.assertEqual(await lost[1], Outcome.UNCERTAIN)
            sends = len(self.qq_calls)
            self.assertEqual(await raw.handler(event), registered)
            self.assertEqual(len(self.qq_calls), sends)
            # Stopping while queued cancels the owned preparation, not its lock owner.
            event.message_obj.message_id = "stop-queued"
            async with self.locks.acquire_lock(umo):
                self.assertEqual(await raw.handler(event), registered)
                await asyncio.sleep(0)
                await asyncio.wait_for(plugin.terminate(), 2)
                self.assertIn("未登记", await raw.handler(event))
                self.assertIn(umo, self.locks.held)
            self.assertEqual(len(self.qq_calls), sends)
            # Already accepted transport may suppress cancellation; retain ownership.
            next_plugin = self.plugin(self.settings())
            await next_plugin.initialize()
            next_host = next_plugin._lifecycle
            entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
            self.gates.append(release)

            async def in_flight(*args):
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    await release.wait()
                return True

            self.context.send_message = in_flight
            event.message_obj.message_id = "stop-in-flight"
            self.assertEqual(await next_plugin.bot_call_start(event), registered)
            await asyncio.wait_for(entered.wait(), 2)
            stopping = asyncio.create_task(next_plugin.terminate())
            await asyncio.wait_for(cancelled.wait(), 2)
            self.assertFalse(stopping.done())
            self.assertIs(getattr(self.context, _CLAIM), next_host)
            self.assertIn("未登记", await next_plugin.bot_call_start(event))
            self.assertIn("未登记", await raw.handler(event))  # old runner remains gated
            release.set()
            await asyncio.wait_for(stopping, 2)
            self.assertFalse(hasattr(self.context, _CLAIM))
            self.assertTrue(reads)
            self.assertEqual((len(provider.calls), len(stt.calls)), (0, 0))

    async def test_native_disabled_and_preflight_failures_never_issue_an_invitation(self):
        for config in (None, {}, {"enabled": False}):
            plugin = self.plugin(config)
            await asyncio.gather(plugin.initialize(), plugin.initialize())
            self.assertIsNone(plugin._lifecycle._backend)
            await plugin.terminate()
        self.assertEqual(self.directory_calls, [])
        self.assertFalse(self.data.exists())
        self.assertFalse(hasattr(self.context, _CLAIM))
        # Independent native-entry stub process blocks owned IO/thread creation.
        source = r"""
import asyncio,importlib.util,sys,threading
from pathlib import Path
from types import ModuleType,SimpleNamespace as NS
root=Path(sys.argv[1]);star=ModuleType('astrbot.api.star');event=ModuleType('astrbot.api.event')
class Star:
 def __init__(self,*args):pass
star.Star=Star
star.StarTools=NS(get_data_dir=lambda *a:(_ for _ in ()).throw(AssertionError('directory')))
event.filter=NS(command=lambda *a:lambda f:f,event_message_type=lambda *a:lambda f:f,
 custom_filter=lambda *a:lambda f:f,llm_tool=lambda **kw:lambda f:f,CustomFilter=object,EventMessageType=NS(PRIVATE_MESSAGE='private'))
pkg=ModuleType('native_disabled');pkg.__path__=[str(root)]
api=ModuleType('astrbot.api');api.logger=__import__('logging').getLogger('astrbot')
sys.modules.update({'astrbot':ModuleType('astrbot'),'astrbot.api':api,
 'astrbot.api.star':star,'astrbot.api.event':event,'native_disabled':pkg})
def audit(event,args):
 if event in {'sqlite3.connect','socket.connect','socket.bind','os.mkdir'}:raise AssertionError(event)
sys.addaudithook(audit)
threading.Thread.start=lambda *a:(_ for _ in ()).throw(AssertionError('thread'))
spec=importlib.util.spec_from_file_location('native_disabled.main',root/'main.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
async def run():
 p=m.BotCallPlugin(object());await p.initialize();await p.terminate()
asyncio.run(run())
assert 'native_disabled.bot_call.backend' not in sys.modules
print('disabled: no owned IO/thread')
"""
        result = subprocess.run(
            [sys.executable, "-B", "-W", "error::ResourceWarning", "-c", source, str(self.root)],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no owned IO/thread", result.stdout)
        for changed in (
            {"public_origin": ""},
            *(
                {"review_instructions": value}
                for value in (None, True, 42, [], {}, "SYNTHETIC_STYLE_INVALID" * 120, "\ud800")
            ),
        ):
            config = self.settings()
            config.update(changed)
            plugin = self.plugin(config)
            with self.assertRaises(PluginLifecycleError) as invalid:
                await plugin.initialize()
            self.assertNotIn(
                "SYNTHETIC_STYLE_INVALID", "".join(traceback.format_exception(invalid.exception))
            )
        self.assertEqual(self.directory_calls, [])
        bad_counter = self.plugin(self.settings())
        bad_counter._lifecycle._tools_factory = lambda context: (self.tools(context)[0], object())
        with self.assertRaises(PluginLifecycleError):
            await bad_counter.initialize()
        self.assertEqual(self.directory_calls, [])
        event, provider, _ = self.host_event()
        from bot_call.review import build_review_prompt

        writing = "SYNTHETIC_STYLE_PRIVATE\n按话题分段；保留 {literal}，不运行模板。"
        config = dict(self.settings(), review_instructions=writing)
        plugin = self.plugin(config)
        await plugin.initialize()
        frozen = build_review_prompt(writing, bounded_output=True)
        from bot_call.native_host import settings_from

        for blank in ("", " \t\n", "\u3000"):
            self.assertEqual(
                settings_from(dict(self.settings(), review_instructions=blank))["review_prompt"],
                build_review_prompt(bounded_output=True),
            )
        self.assertEqual(
            settings_from(self.settings())["review_prompt"],
            build_review_prompt(bounded_output=True),
        )
        self.assertTrue(frozen.endswith(writing))
        self.assertNotEqual(frozen, build_review_prompt(writing))
        self.assertEqual(plugin._lifecycle._backend._runtime._review_prompt, frozen)
        self.assertNotIn(writing, repr(plugin._lifecycle._backend._config))
        self.assertNotIn("review_prompt", plugin._lifecycle._native.settings)
        self.assertNotIn("review_instructions", plugin._lifecycle._native.settings)
        config["review_instructions"] = "later config edit requires reload"
        self.assertEqual(plugin._lifecycle._backend._runtime._review_prompt, frozen)
        self.assertEqual(await self.dispatch(plugin, event, "ordinary text"), "ordinary_llm")
        self.assertFalse(event.stopped)
        provider.provider_config["max_context_tokens"] = 0
        with self.assertRaises(PluginLifecycleError):
            await self.dispatch(plugin, event)
        self.assertTrue(event.stopped)
        self.assertEqual(self.qq_calls, [])
        provider.provider_config["max_context_tokens"] = 2900
        # New events cannot escape failed preflight via unknown counter/logging/STT.
        event.message_obj.message_id = "stt-unavailable"
        saved = self.context.stt.pop(event.unified_msg_origin)
        try:
            self.assertEqual(await self.dispatch(plugin, event), "stt_unavailable")
        finally:
            self.context.stt[event.unified_msg_origin] = saved
        self.assertEqual(self.qq_calls, [])
        event.get_platform_name = lambda: "other"
        self.assertEqual(await self.dispatch(plugin, event), "unsupported_event")
        self.assertEqual(self.qq_calls, [])
        await plugin.terminate()
        await self.quick_failures()

    async def test_bind_readiness_failure_and_actual_shutdown_keep_context_ownership(self):
        event, _, _ = self.host_event()
        fixed_data = self.data
        self.data = self.data.with_name("quick-native")
        await self.quick_success_and_exit(event)
        await self.quick_cancel_and_reload()
        self.data = fixed_data
        self.qq_calls.clear()
        self.qq_entered.clear()
        self.stt_entered.clear()
        self.stt_cancelled.clear()
        plugin = self.plugin(self.settings())

        def delayed(app, port):
            server = ReadinessServer(app, port)
            server.auto_bind = False
            self.servers.append(server)
            return server

        plugin._lifecycle._server_factory = delayed
        previous_servers = len(self.servers)
        starting = asyncio.create_task(plugin.initialize())
        for _ in range(100):
            if len(self.servers) > previous_servers:
                break
            await asyncio.sleep(0.001)
        server = self.servers[-1]
        await server.entered.wait()
        self.assertFalse(plugin._lifecycle.ready)
        self.assertFalse(starting.done())
        self.assertEqual(await self.dispatch(plugin, event), "not_ready")
        self.assertEqual(self.qq_calls, [])
        starting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await starting
        server.bound.set()
        await plugin.initialize()
        # Active server disappears: fail closed immediately and own backend cleanup.
        server.release.set()
        await server.task
        self.assertEqual(await self.dispatch(plugin, event), "not_ready")
        with self.assertRaises(PluginLifecycleError):
            await plugin.terminate()
        self.assertFalse(hasattr(self.context, _CLAIM))
        failed = self.plugin(self.settings())

        class FailServer(ReadinessServer):
            async def start(inner):
                inner.task = asyncio.create_task(inner.run())
                raise RuntimeError(fixture.PRIVATE)

        def failure(app, port):
            s = FailServer(app, port)
            self.servers.append(s)
            return s

        failed._lifecycle._server_factory = failure
        with self.assertRaises(PluginLifecycleError) as caught:
            await failed.initialize()
        self.startup_diagnostic(caught.exception, "http")
        self.assertIsNone(failed._lifecycle._backend)
        self.assertFalse(hasattr(self.context, _CLAIM))
        self.assertEqual(self.qq_calls, [])
        # Real NativeServer code with synthetic Hypercorn/socket surfaces; no bind.
        sockets = []
        entered = asyncio.Event()
        release = asyncio.Event()
        cancelled = asyncio.Event()
        self.gates.append(release)

        class Socket:
            def __init__(inner, *args):
                inner.closed = False
                sockets.append(inner)

            def setsockopt(inner, level, option, value):
                self.assertEqual(
                    (level, option, value), (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                )

            def setblocking(inner, value):
                pass

            def bind(inner, address):
                inner.address = address

            def getsockopt(inner, *args):
                return 1

            def close(inner):
                inner.closed = True

        class Config:
            pass

        class Logger:
            pass

        async def serve(app, config, *, shutdown_trigger, mode):
            self.assertEqual(config.create_sockets().insecure_sockets, [sockets[-1]])
            self.assertEqual(config.bind, ["127.0.0.1:6191"])
            self.assertIsNone(config.accesslog)
            self.assertIsNone(config.errorlog)
            entered.set()
            await config.logger_class().info("Running on http://127.0.0.1:6191")
            await shutdown_trigger()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()

        modules = {
            "hypercorn.asyncio": NS(serve=serve),
            "hypercorn.config": NS(
                Config=Config, Sockets=lambda secure, insecure, quic: NS(insecure_sockets=insecure)
            ),
            "hypercorn.logging": NS(Logger=Logger),
        }
        import socket

        real_socket = socket.socket

        def socket_factory(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0, fileno=None):
            # Keep asyncio's real local AF_UNIX self-pipe; replace only our INET
            # listener. No INET bind/connect is performed by the fixture.
            if family == socket.AF_INET and fileno is None:
                return Socket(family, type, proto)
            return real_socket(family, type, proto, fileno=fileno)

        with (
            patch.dict(sys.modules, modules),
            patch("importlib.metadata.version", return_value="0.18.0"),
            patch("socket.socket", socket_factory),
        ):

            async def fail_serve(*args, **kwargs):
                try:
                    raise ValueError(fixture.PRIVATE + " https://must-not-export.invalid/?secret")
                except ValueError:
                    raise type(fixture.PRIVATE, (RuntimeError,), {})(fixture.PRIVATE) from None

            failed = self.plugin(self.settings())
            failed._lifecycle._server_factory = NativeServer
            with patch.dict(sys.modules, {"hypercorn.asyncio": NS(serve=fail_serve)}):
                waiters = [asyncio.create_task(failed.initialize()) for _ in range(3)]
                errors = await asyncio.gather(*waiters, return_exceptions=True)
            for error in errors:
                self.assertIsInstance(error, PluginLifecycleError)
                diagnostic = self.startup_diagnostic(error, "http")
                self.assertIn("bot_call.startup cleanup_wait=raised", error.__notes__)
                self.assertTrue(
                    any(
                        item["source"] == "http" and item["class"] == "ValueError"
                        for item in diagnostic["errors"]
                    )
                )
                self.assertTrue(any(item["class"] == "unknown" for item in diagnostic["errors"]))
                self.assertTrue(
                    any(
                        frame["file"] == "bot_call/native_host.py"
                        for item in diagnostic["errors"]
                        for frame in item["frames"]
                    )
                )
            self.assertTrue(failed._lifecycle._server.stopped)
            self.assertFalse(hasattr(self.context, _CLAIM))
            self.assertTrue(sockets[-1].closed)
            owned = self.plugin(self.settings())
            owned._lifecycle._server_factory = NativeServer
            await owned.initialize()
            self.assertTrue(entered.is_set())
            self.assertTrue(owned._lifecycle.ready)
            self.assertEqual(await self.dispatch(owned, event), Outcome.SUBMITTED)
            handle = owned._lifecycle._invitations[0][0]
            backend = owned._lifecycle._backend
            grant = backend._record(handle).grant
            app = self.proxy_app(owned._lifecycle)
            response = await self.request(app, "exchange", data={"invitation": grant.secret.value})
            self.assertEqual(response[0], 200, response[1])
            cookie = response[2][b"set-cookie"].decode().split(";", 1)[0]
            reference = response[1]["reference"]
            self.stt_mode = "stubborn"
            self.assertEqual(
                (
                    await self.request(
                        app,
                        "audio",
                        cookie=cookie,
                        reference=reference,
                        raw=b"retained-tail",
                        headers={"content-type": "audio/webm", "x-bot-utterance-id": "tail"},
                    )
                )[0],
                200,
            )
            await asyncio.wait_for(self.stt_entered.wait(), 3)
            closing = asyncio.create_task(owned.terminate())
            await asyncio.wait_for(self.stt_cancelled.wait(), 3)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            closing.cancel(fixture.PRIVATE)
            with self.assertRaises(asyncio.CancelledError):
                await closing
            self.assertTrue(hasattr(self.context, _CLAIM))
            self.assertFalse(sockets[-1].closed)
            contender = self.plugin(self.settings())
            with self.assertRaises(PluginLifecycleError):
                await contender.initialize()
            self.assertIs(getattr(self.context, _CLAIM), owned._lifecycle)
            release.set()
            await asyncio.sleep(0)
            self.assertFalse(owned._lifecycle._terminate_task.done())
            self.assertTrue(backend._runtime._thread.is_alive())
            self.stt_release.set()
            await owned.terminate()
        with Store(self.data / "calls.sqlite3") as store:
            audio = store.get_audio_jobs(backend._record(handle).binding.call_id)[0]
            self.assertNotEqual(audio.state, "succeeded")
            self.assertEqual(audio.data, b"retained-tail")
        self.assertTrue(sockets[-1].closed)
        self.assertFalse(hasattr(self.context, _CLAIM))
        successor = self.plugin(self.settings())
        await successor.initialize()
        await successor.terminate()
        self.assertFalse(hasattr(self.context, _CLAIM))

    async def test_prepare_notes_cross_wrappers_fixed_notice_and_collector_failure(self):
        from bot_call.binding import BindingError
        from bot_call.native_host import NativeHostError
        from bot_call.prepare_diagnostics import PREFIX, attach_prepare, _validated

        entry_notes = []

        def record(fmt, *values):
            if fmt.startswith("bot_call.entry "):
                entry_notes.append(fmt % values)

        recording = patch.object(self.entry.logger, "info", side_effect=record)
        recording.start()
        self.addCleanup(recording.stop)
        notice_diagnostics = []
        warnings = patch.object(
            self.entry.logger,
            "warning",
            side_effect=lambda fmt, *values: notice_diagnostics.append(fmt % values),
        )
        warnings.start()
        self.addCleanup(warnings.stop)
        plugin = self.plugin(self.settings())
        await plugin.initialize()
        backend = plugin._lifecycle._backend
        self.assertTrue(any("stage=load ready=yes" in note for note in entry_notes))
        filters = plugin.botcall.filters
        self.assertEqual([kind for kind, _ in filters], ["command", "probe"])
        ordinary, _, _ = self.host_event()
        before = len(entry_notes)
        self.assertEqual(await self.dispatch(plugin, ordinary, "ordinary text"), "ordinary_llm")
        self.assertEqual(len(entry_notes), before)
        self.assertFalse(ordinary.stopped)
        sent = []
        for stage in ("bind", "select"):
            event, _, _ = self.host_event()
            event.message_obj.message_id = "diagnostic-" + stage
            extras = {}
            event.get_extra = lambda k, default=None: extras.get(k, default)
            event.set_extra = lambda k, v: extras.__setitem__(k, v)
            event.plain_result = lambda text: text

            async def send(text):
                sent.append(text)
                raise ValueError(fixture.PRIVATE)  # Unknown feedback delivery: no retry.

            event.send = send

            async def fail(*args):
                try:
                    raise ValueError(fixture.PRIVATE)
                except ValueError:
                    error = (BindingError if stage == "bind" else NativeHostError)()
                    error.add_note(
                        PREFIX + '{"stage":"send","errors":[],"openai_debug_enabled":false}'
                    )
                    error.add_note(PREFIX + '{"stage":"' + fixture.PRIVATE + '"}')
                    raise error from None

            owner, name = (backend._reader, "bind") if stage == "bind" else (backend, "_select")
            with patch.object(owner, name, fail):
                for _ in range(2):
                    with self.assertRaises(PluginLifecycleError) as caught:
                        await self.dispatch(plugin, event)
                    self.assertEqual(str(caught.exception), "bot_call.host_unavailable")
                    notes = caught.exception.__notes__
                    self.assertLessEqual(len(notes), 2)
                    values = [
                        json.loads(n[len(PREFIX) :]) for n in notes if n.startswith(PREFIX + "{")
                    ]
                    self.assertEqual(values[0]["stage"], stage)
                    self.assertTrue(all(_validated(n) == n for n in notes))
                    self.assertNotIn(fixture.PRIVATE, "\n".join(notes))
                    self.assertNotIn(
                        fixture.PRIVATE, "".join(traceback.format_exception(caught.exception))
                    )
            self.assertTrue(event.stopped)
        self.assertEqual(sent, ["本次通话未能确认就绪，请先不要重复发送，等待排查。"] * 2)
        for outcome in (
            "not_ready",
            Outcome.NOT_APPLIED,
            Outcome.UNCERTAIN,
            Outcome.CONFIRMED,
            Outcome.SUBMITTED,
        ):
            event, _, _ = self.host_event()
            extras = {}
            event.get_extra = lambda k, default=None: extras.get(k, default)
            event.set_extra = lambda k, v: extras.__setitem__(k, v)
            event.plain_result = lambda text: text
            event.send = send

            async def result(event):
                return outcome

            count = len(sent)
            with patch.object(plugin._lifecycle, "initiate", result):
                self.assertEqual(await self.dispatch(plugin, event), outcome)
            self.assertEqual(
                len(sent) - count, 0 if outcome in (Outcome.CONFIRMED, Outcome.SUBMITTED) else 1
            )

        # Only a matched command reaches the diagnostic probe. A private
        # event's QQ At component is irrelevant to its trusted message type.
        private, _, _ = self.host_event(platform_type="qq_official", sender="opaque-private")
        private.message_obj.message = [NS(type="At", qq="qq_official")]
        extras = {}
        private.get_extra = lambda k, default=None: extras.get(k, default)
        private.set_extra = lambda k, v: extras.__setitem__(k, v)
        private.plain_result = lambda text: text

        async def send_return(text):
            sent.append(text)

        private.send = send_return

        async def unavailable(event):
            return "not_ready"

        count = len(sent)
        with patch.object(plugin._lifecycle, "initiate", unavailable):
            self.assertEqual(await self.dispatch(plugin, private), "not_ready")
            self.assertEqual(await self.dispatch(plugin, private), "not_ready")
        self.assertEqual(len(sent), count + 1)  # Notice guard precedes even a successful send.
        self.assertTrue(
            any(
                "stage=matched" in note and "kind=private stopped=no" in note
                for note in entry_notes
            )
        )
        self.assertTrue(
            any("stage=entered ready=yes kind=private stopped=yes" in note for note in entry_notes)
        )
        self.assertTrue(
            any("stage=result" in note and "result=not_ready" in note for note in entry_notes)
        )
        self.assertTrue(
            any("stage=notice" in note and "result=returned" in note for note in entry_notes)
        )
        self.assertTrue(
            any("stage=notice" in note and "result=failed" in note for note in entry_notes)
        )
        self.assertTrue(
            any("stage=notice" in note and "result=skipped" in note for note in entry_notes)
        )
        for kind in ("GroupMessage", fixture.PRIVATE):
            rejected, _, _ = self.host_event(sender="private-identity-sentinel")
            rejected.get_message_type = lambda: NS(value=kind)
            extras = {}
            rejected.get_extra = lambda k, default=None: extras.get(k, default)
            rejected.set_extra = lambda k, v: extras.__setitem__(k, v)
            rejected.plain_result = lambda text: text
            rejected.send = send_return

            async def forbidden(event):
                self.fail("Non-private commands must not create resources")

            with patch.object(plugin._lifecycle, "initiate", forbidden):
                self.assertEqual(await self.dispatch(plugin, rejected), "unsupported_event")
            self.assertTrue(rejected.stopped)
        # Even a broken logger cannot block the probe, private backend or notice.
        extras.clear()
        private.send = send
        with (
            patch.object(self.entry.logger, "info", side_effect=SystemExit(fixture.PRIVATE)),
            patch.object(self.entry.logger, "warning", side_effect=SystemExit(fixture.PRIVATE)),
            patch.object(plugin._lifecycle, "initiate", unavailable),
        ):
            self.assertEqual(await self.dispatch(plugin, private), "not_ready")
        self.assertTrue(notice_diagnostics)
        for diagnostic in notice_diagnostics:
            self.assertEqual(_validated(diagnostic), diagnostic)
            decoded = json.loads(diagnostic[len(PREFIX) :])
            self.assertEqual(decoded["stage"], "send")
            self.assertEqual(decoded["errors"][0]["class"], "ValueError")
            self.assertIn(
                "main.py", {f["file"] for row in decoded["errors"] for f in row["frames"]}
            )
        self.assertNotIn(fixture.PRIVATE, "\n".join(notice_diagnostics))
        self.assertNotIn(str(self.root), "\n".join(notice_diagnostics))
        self.assertNotIn(fixture.PRIVATE, "\n".join(entry_notes))
        self.assertNotIn("opaque-private", "\n".join(entry_notes))
        self.assertNotIn("private-identity-sentinel", "\n".join(entry_notes))
        self.assertNotIn("qq_official", "\n".join(entry_notes))
        with Store(self.data / "calls.sqlite3") as store:
            self.assertEqual(store._db.execute("SELECT count(*) FROM calls").fetchone()[0], 0)
        failure = PluginLifecycleError()
        with patch("bot_call.prepare_diagnostics.prepare_note", side_effect=SystemExit()):
            attach_prepare(failure, "bind", ValueError(fixture.PRIVATE))
        self.assertEqual(str(failure), "bot_call.host_unavailable")
        await plugin.terminate()

    async def test_management_diagnostics_auth_snapshot_routing_and_handler_ownership(self):
        event, provider, stt = self.host_event(
            "qq-official-A",
            platform_type="qq_official",
            sender="openid-user",
            session="openid:private:session",
        )
        other, pb, sb = self.host_event("qq-B", "B", "2")
        provider.provider_config["max_context_tokens"] = 2900
        pb.provider_config["max_context_tokens"] = 2900
        plugin = self.plugin(self.settings())
        await asyncio.gather(plugin.initialize(), plugin.initialize())
        lifecycle = plugin._lifecycle
        backend = lifecycle._backend
        self.assertTrue(lifecycle.ready)  # Empty legacy ID no longer prevents startup.
        from bot_call.plugin import DIAGNOSTIC_ROUTE, PLUGIN_NAME

        handler = lifecycle._management_handler
        registry = self.context.registered_web_apis
        self.assertEqual(len(registry), 4)
        self.assertIs(registry[-1][1], handler)
        lifecycle._register_management()
        self.assertEqual(len(registry), 4)

        async def diagnostic(**overrides):
            api_handler = overrides.pop("api_handler", handler)
            fields = dict(
                method="GET",
                plugin_name=PLUGIN_NAME,
                path="/api/v1/plugins/extensions" + DIAGNOSTIC_ROUTE,
                username="synthetic-dashboard",
                query={},
            )
            auth_token = overrides.pop("auth_token", "synthetic-dashboard-jwt")
            missing_server = overrides.pop("missing_server", False)

            class Dashboard:
                @staticmethod
                def _extract_dashboard_jwt(raw):
                    return raw.auth_token

                @staticmethod
                def _validate_dashboard_token(token, path):
                    # Shapes of actual role-aware server methods, not JWT proof.
                    if token == "synthetic-dashboard-jwt":
                        return {"username": "synthetic-dashboard"}, ""
                    return None, "SYNTHETIC_SECRET_AUTH_EXCEPTION"

            server = None if missing_server else Dashboard()
            fields["_request"] = NS(
                auth_token=auth_token,
                app=NS(state=NS(dashboard_app_adapter=NS(_dashboard_server=server))),
            )
            fields.update(overrides)
            token = self.management_request.set(NS(**fields))
            try:
                return await api_handler()
            finally:
                self.management_request.reset(token)

        # A page asset token, call cookie, foreign namespace, or absent core-bound
        # request cannot grant admin access. Core HTTP authentication is external.
        self.assertEqual((await handler()).status_code, 401)
        for fields in [
            dict(username=None),
            dict(query={"asset_token": "synthetic"}),
            dict(plugin_name="other"),
            dict(path="/api/plug/other/diagnostics"),
            dict(method="POST"),
            dict(auth_token="synthetic-valid-asset-jwt"),
            dict(auth_token="synthetic-plugin-scope-api-key"),
            dict(auth_token=None),
            dict(username="other-dashboard"),
            dict(missing_server=True),
        ]:
            self.assertEqual((await diagnostic(**fields)).status_code, 401)
        self.assertEqual((await diagnostic(path="/api/plug" + DIAGNOSTIC_ROUTE)).status_code, 200)
        snapshot = (await diagnostic()).data["data"]
        self.assertEqual(
            set(snapshot),
            {
                "version",
                "plugin_ready",
                "https_ready",
                "bare_page_url",
                "source",
                "timings",
                "round_timings",
                "reviews",
                "call_model",
                "provider_integration",
                "output_limits_managed_by",
            },
        )
        self.assertEqual(snapshot["version"], self.entry.PLUGIN_VERSION)
        from bot_call.plugin import BENCHMARK_ROUTE

        self.assertEqual((await lifecycle._benchmark_handler()).status_code, 401)
        bench = await diagnostic(
            api_handler=lifecycle._benchmark_handler,
            path="/api/v1/plugins/extensions" + BENCHMARK_ROUTE,
        )
        self.assertEqual(bench.status_code, 200)
        self.assertEqual(bench.data["data"]["calls"], [])
        self.assertIsNone(bench.data["data"]["last_run"])
        foreign = await diagnostic(api_handler=lifecycle._benchmark_handler)
        self.assertEqual(foreign.status_code, 401)
        self.assertIsNone(snapshot["source"])
        self.assertEqual(snapshot["timings"], {"capacity": 128, "sample_count": 0, "stages": []})
        self.assertEqual(snapshot["bare_page_url"], fixture.ORIGIN + "/bot-call/")
        # A dashboard on a separate request loop still reads only the captured
        # business loop; the DTO crosses back, never Store or Provider objects.
        other_loop = await asyncio.to_thread(lambda: asyncio.run(diagnostic()))
        self.assertEqual(other_loop.data["data"], snapshot)
        self.assertEqual(self.qq_calls, [])
        self.assertFalse(provider.calls)
        self.assertFalse(stt.calls)
        self.assertFalse(self.context.conversation_manager.writes)
        self.assertEqual(lifecycle._invitations, [])
        umo = event.unified_msg_origin
        other_umo = other.unified_msg_origin
        native_stt = self.stt_providers[umo]
        self.assertEqual(await lifecycle._native.check_stt(umo), (native_stt, "native"))
        self.assertEqual(
            await lifecycle._native.check_stt(other_umo), (self.stt_providers[other_umo], "native")
        )
        self.context.stt.pop(umo)
        self.assertEqual(await self.dispatch(plugin, event), "stt_unavailable")
        self.context.stt[umo] = native_stt
        native_stt.provider_config["enable"] = False
        self.assertEqual(await self.dispatch(plugin, event), "stt_unavailable")
        native_stt.provider_config["enable"] = True
        self.assertEqual(lifecycle._invitations, [])
        self.assertEqual(self.qq_calls, [])
        self.assertEqual((len(stt.calls), len(sb.calls)), (0, 0))
        self.assertEqual(len(self.servers), 1)
        self.assertEqual(len(self.directory_calls), 1)
        self.assertNotEqual(self.directory_calls[0][1], self.thread)
        tts_selector = self.context.get_using_tts_provider
        self.context.get_using_tts_provider = lambda **kw: None
        self.assertEqual(await self.dispatch(plugin, event), "tts_unavailable")
        self.assertEqual(self.qq_calls, [])
        self.context.get_using_tts_provider = tts_selector
        self.qq_mode = "gate"
        first = asyncio.create_task(self.dispatch(plugin, event))
        await asyncio.wait_for(self.qq_entered.wait(), 3)
        first.cancel(fixture.PRIVATE)
        with self.assertRaises(asyncio.CancelledError):
            await first
        repeated = asyncio.create_task(self.dispatch(plugin, event))
        await asyncio.sleep(0)
        self.assertEqual(len(self.qq_calls), 1)
        self.qq_release.set()
        self.qq_mode = "good"
        self.assertEqual(await repeated, Outcome.SUBMITTED)
        self.assertEqual(await self.dispatch(plugin, event), Outcome.SUBMITTED)
        self.assertEqual(len(lifecycle._invitations), 1)
        handle = lifecycle._invitations[0][0]
        record = backend._record(handle)
        bound = record.binding
        grant = record.grant
        # Real settings_from -> native select -> existing immutable policy format.
        # No new invitation, SDK request, saved policy rewrite or budget reuse.
        from bot_call.native_host import settings_from, NativeHostError

        old_settings = lifecycle._native.settings
        old_policy = record.policy.canonical_json
        snapshot = await backend._reader.refresh(bound)
        try:
            for obsolete in (
                {},
                {
                    "reply_tokens": None,
                    "main_reply_tokens": "old",
                    "query_source": "legacy_tool_source",
                },
            ):
                lifecycle._native.settings = settings_from({**self.settings(), **obsolete})
                self.assertTrue(
                    all(
                        k not in lifecycle._native.settings
                        for k in ("query_source", "reply_tokens", "main_reply_tokens")
                    )
                )
                selected = await lifecycle._native.select(bound, snapshot)
                self.assertEqual(selected.reply.contract, "astrbot-main-agent-v1")
                frozen = CallPolicy.from_policies(
                    selected.reply,
                    selected.audio,
                    ReviewManifest.from_policy(selected.review.policy, ESTIMATE_RULE),
                )
                self.assertTrue(frozen.is_agent)
                self.assertEqual(CallPolicy(frozen.canonical_json, frozen.fingerprint), frozen)
        finally:
            lifecycle._native.settings = old_settings
        saved_policy = await backend._runtime.read_call_policy(bound.call_id)
        self.assertEqual(record.policy.canonical_json, old_policy)
        self.assertEqual(saved_policy.canonical_json, old_policy)
        self.assertEqual(saved_policy.reply.contract, "astrbot-main-agent-v1")
        from bot_call.native_provider import NATIVE_CONTRACT
        from bot_call.audio import AudioNotApplied

        self.assertEqual(record.policies.audio.selection, NATIVE_CONTRACT)
        self.assertEqual(record.policy.audio.selection, NATIVE_CONTRACT)
        self.context.stt[umo] = self.stt_providers[other_umo]
        with self.assertRaises(AudioNotApplied):
            await record.audio._resolve()
        self.context.stt.pop(umo)
        with self.assertRaises(AudioNotApplied):
            await record.audio._resolve()
        self.context.stt[umo] = native_stt
        await record.audio._resolve()
        self.assertEqual(len(stt.calls), 0)
        self.assertEqual(await self.dispatch(plugin, other), Outcome.SUBMITTED)
        other_handle = lifecycle._invitations[1][0]
        app = self.proxy_app(lifecycle)
        denied = await self.request(
            self.proxy_app(lifecycle, "192.0.2.1"),
            "exchange",
            data={"invitation": grant.secret.value},
        )
        self.assertEqual(denied[0], 403)
        result = await self.request(app, "exchange", data={"invitation": grant.secret.value})
        self.assertEqual(result[0], 200)
        cookie = result[2][b"set-cookie"].decode().split(";", 1)[0]
        reference = result[1]["reference"]
        cm = self.context.conversation_manager
        cm.current[bound.unified_msg_origin] = "B"
        async with self.locks.acquire_lock(bound.unified_msg_origin):
            cm.items["A"].history = json.dumps(
                [{"role": "user", "content": "原QQ新增，不能覆盖"}], ensure_ascii=False
            )
        history = cm.items["A"].history
        b_history = cm.items["B"].history
        long_text = fixture.RECOVERY_TEXT * 8

        async def long_transcription(file):
            self.on_business()
            return long_text

        stt.transcriber = long_transcription
        audio = dict(
            cookie=cookie,
            reference=reference,
            raw=b"synthetic-complete-clip",
            headers={"content-type": "audio/webm;codecs=opus", "x-bot-utterance-id": "audio-A"},
        )
        self.assertEqual((await self.request(app, "audio", **audio))[0], 200)
        self.assertEqual((await self.request(app, "audio", **audio))[0], 200)
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual([t.body for t in state.turns], [long_text, fixture.BOT])
        self.assertEqual(
            (len(stt.calls), len(provider.calls), len(pb.calls), len(sb.calls)), (1, 1, 0, 0)
        )
        # An authenticated read checks only known source metadata, not history,
        # new conversations, invite delivery or provider execution.
        before_diag = (
            len(stt.calls),
            len(sb.calls),
            len(provider.calls),
            len(pb.calls),
            len(self.qq_calls),
            len(lifecycle._invitations),
        )
        with patch.object(
            backend._runtime, "read_state", side_effect=AssertionError("no history reads")
        ):
            response = await diagnostic()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        payload = response.data["data"]
        self.assertEqual(payload["source"], {"stt": "native", "tts": "native"})
        self.assertTrue(
            {"stt", "reply"}.issubset({r["stage"] for r in payload["timings"]["stages"]})
        )
        self.assertEqual(payload["round_timings"]["models"][0]["provider_id"], provider.meta().id)
        self.assertNotIn(fixture.PRIVATE, json.dumps(payload, ensure_ascii=False))
        # Provider identifiers are now explicitly allowed; synthetic provider-A
        # contains the single-letter cid A without exposing the conversation.
        encoded = json.dumps(
            {k: v for k, v in payload.items() if k not in {"round_timings", "call_model"}},
            ensure_ascii=False,
        )
        for secret in (
            fixture.PRIVATE,
            long_text,
            fixture.BOT,
            umo,
            bound.call_id,
            bound.conversation_id,
        ):
            self.assertNotIn(secret, encoded)
        self.assertEqual(
            before_diag,
            (
                len(stt.calls),
                len(sb.calls),
                len(provider.calls),
                len(pb.calls),
                len(self.qq_calls),
                len(lifecycle._invitations),
            ),
        )
        generation = state.generations[0].generation_id
        self.assertEqual(
            (
                await self.request(
                    app,
                    "playback",
                    cookie=cookie,
                    reference=reference,
                    data={"generation_id": generation, "revision": 1, "state": "stopped"},
                )
            )[0],
            200,
        )
        self.assertEqual(
            (await self.request(app, "end", cookie=cookie, reference=reference, data={}))[0], 200
        )
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual(state.review.body, fixture.FINAL)
        self.assertTrue(state.publication.complete)
        self.assertGreater(state.review_work.map_count, 1)
        self.assertEqual(
            [t["body"] for t in json.loads(state.review.input_json)], [long_text, fixture.BOT]
        )
        self.assertEqual(
            json.loads(cm.items["A"].history)[: len(json.loads(history))], json.loads(history)
        )
        self.assertEqual(
            json.loads(cm.items["A"].history)[-1], {"role": "assistant", "content": fixture.FINAL}
        )
        self.assertEqual(cm.current[bound.unified_msg_origin], "B")
        self.assertEqual(cm.items["B"].history, b_history)
        self.assertEqual(len(cm.writes), 1)
        self.assertEqual(cm.writes[0][3], 0)
        self.assertEqual(len(cm.writes), 1)
        manifest = (await backend._runtime.read_call_policy(bound.call_id)).review_manifest
        self.assertEqual(manifest.counter_rule_id, ESTIMATE_RULE)
        with self.assertRaises(ReviewPolicyUnavailable):
            manifest.restore(bound, {})
        worker = backend._runtime._thread.ident
        counted = [
            (json.loads(s), thread) for s, thread, _ in self.estimator.records if thread == worker
        ]
        self.assertTrue(counted)
        self.assertTrue(
            all(
                set(payload) == {"chat_provider_id", "contexts", "request_max_retries"}
                for payload, _ in counted
            )
        )
        self.assertTrue(all(len(payload["contexts"]) == 2 for payload, _ in counted))
        counter = lifecycle._native.counters[ESTIMATE_RULE]
        self.assertIsNone(getattr(counter, "__self__", None))
        self.assertEqual(
            {type(c.cell_contents) for c in counter.__closure__}, {type, ObservedEstimator}
        )
        # Standard sends retain the complete opaque UMO and exact instance.
        for platform, kwargs in self.qq_calls:
            expected = other.unified_msg_origin if platform == "qq-B" else bound.unified_msg_origin
            self.assertEqual(kwargs["umo"], expected)
            self.assertEqual(kwargs["session_id"], expected.split(":", 2)[2])
            self.assertEqual(kwargs["message"][0]["type"], "text")
        public = (await self.request(app, "state", cookie=cookie, reference=reference))[1]
        self.assertEqual(public["submission_status"], "submitted")
        self.assertEqual(public["delivery_status"], "unverified")
        self.assertTrue(all(value == "submitted" for value in public["page_states"]))
        self.assertEqual(public["context_state"], "succeeded")
        pages = [kw["message"][0]["data"]["text"] for platform, kw in self.qq_calls[2:]]
        self.assertEqual(pages, [page.message for page in state.publication.pages])
        self.assertTrue(all(platform == bound.platform_id for platform, _ in self.qq_calls[2:]))
        counts = (len(self.qq_calls), len(provider.calls))
        await backend._runtime.resume_pending()
        await backend.drain()
        self.assertEqual((len(self.qq_calls), len(provider.calls)), counts)
        # Invitation ACK loss is conservative and collected, never resent on reentry.
        self.qq_mode = "lost"
        other.message_obj.message_id = "lost"
        self.assertEqual(await self.dispatch(plugin, other), Outcome.UNCERTAIN)
        count = len(self.qq_calls)
        self.assertEqual(await self.dispatch(plugin, other), Outcome.UNCERTAIN)
        self.assertEqual(len(self.qq_calls), count)
        # Preflight refuses missing, mismatched or ambiguous instances before
        # entering Context. No fallback to a different bot with the same user.
        native = lifecycle._native
        routes, sends = len(self.context_routes), len(self.qq_calls)
        for rejected in (
            replace(
                bound,
                platform_id="missing",
                unified_msg_origin=f"missing:FriendMessage:{bound.session_id}",
            ),
            replace(bound, platform_type="aiocqhttp"),
            replace(bound, unified_msg_origin=other.unified_msg_origin),
        ):
            self.assertEqual(await native.send_text(rejected, "synthetic"), Outcome.NOT_APPLIED)
        platforms = self.context.platform_manager.platform_insts
        platforms.append(platforms[0])
        self.assertEqual(await native.send_text(bound, "synthetic"), Outcome.NOT_APPLIED)
        platforms.pop()
        self.assertEqual((len(self.context_routes), len(self.qq_calls)), (routes, sends))
        self.qq_mode = "false"
        self.assertEqual(await native.send_text(bound, "synthetic"), Outcome.NOT_APPLIED)
        self.assertEqual(len(self.qq_calls), sends)
        self.qq_mode = "cancel"
        self.assertEqual(await native.send_text(bound, "synthetic"), Outcome.UNCERTAIN)
        self.qq_mode = "good"
        # A cookie cannot operate B, and B cannot adopt A's old reference.
        gb = backend._record(other_handle).grant
        rb = await self.request(app, "exchange", data={"invitation": gb.secret.value})
        self.assertEqual(rb[0], 200)
        cb = rb[2][b"set-cookie"].decode().split(";", 1)[0]
        self.assertEqual((await self.request(app, "state", cookie=cb, reference=reference))[0], 401)
        self.assertIn(
            "review", {r["stage"] for r in (await diagnostic()).data["data"]["timings"]["stages"]}
        )
        foreign = ("/other/diagnostics", object(), ["GET"], "foreign")
        registry.append(foreign)
        await plugin.terminate()
        self.assertFalse(backend._runtime._thread.is_alive())
        self.assertEqual(registry, [foreign])
        with patch.object(
            backend, "diagnostic_binding", side_effect=AssertionError("closed backend touched")
        ):
            self.assertEqual((await diagnostic()).status_code, 503)
        self.assertEqual(lifecycle._timings.snapshot()["sample_count"], 0)
        self.assertFalse(hasattr(self.context, _CLAIM))
        # Synthetic stopped-process fixture: saved closing audio uses its own STT
        # ID even though the global STT lookup is disabled. No old ticket adoption.
        closing = replace(bound, call_id="host-closing")
        saved = CallPolicy.from_policies(
            replace(record.policies.reply, binding=closing),
            replace(record.policies.audio, binding=closing),
            ReviewManifest.from_policy(
                replace(record.policies.review.policy, binding=closing), ESTIMATE_RULE
            ),
        )
        with Store(self.data / "calls.sqlite3") as store:
            store.create_call(closing, policy=saved)
            store.receive_audio(closing.call_id, "tail", b"saved-closing-tail", "audio/webm")
            store.end_call(closing.call_id, prompt=state.review.prompt, _request_close=True)
        self.qq_mode = "good"
        successor = self.plugin(self.settings())
        await successor.initialize()
        host = successor._lifecycle
        self.assertEqual(len(registry), 5)
        self.assertIs(registry[-1][1], host._management_handler)
        lifecycle._stop_management()  # Delayed old cleanup must preserve successor.
        self.assertEqual(len(registry), 5)
        self.assertIs(registry[-1][1], host._management_handler)
        self.assertEqual((await diagnostic()).status_code, 503)
        recovery = host.begin_recovery(closing.call_id)
        selected_stt = self.context.stt.pop(umo)
        with self.assertRaises(BackendError):
            await host.recover(recovery)
        self.assertEqual(len(stt.calls), 1)
        self.context.stt[umo] = selected_stt
        counter = host._native.counters.pop(ESTIMATE_RULE)
        with self.assertRaises(BackendError):
            await host.recover(recovery, retry=True)
        self.assertEqual(len(stt.calls), 1)
        host._native.counters[ESTIMATE_RULE] = counter
        original = selected_stt.model
        selected_stt.model = "changed"
        with self.assertRaises(BackendError):
            await host.recover(recovery, retry=True)
        selected_stt.model = original
        self.assertEqual(len(stt.calls), 1)
        before = len(provider.calls)
        await host.recover(recovery, retry=True)
        await host._backend.drain()
        restored = await host._backend.read_state(recovery)
        self.assertEqual(restored.turns[0].body, long_text)
        self.assertEqual(len(stt.calls), 2)
        self.assertTrue(restored.publication.complete)
        self.assertTrue(
            all(
                json.loads(payload["contexts"][1]["content"])["format"].startswith(
                    "bot-call-review-"
                )
                for payload in provider.calls[before:]
            )
        )
        self.assertEqual(
            (await self.request(self.proxy_app(host), "state", cookie=cookie, reference=reference))[
                0
            ],
            401,
        )
        await successor.terminate()

    async def test_call_model_preflight_refuses_invalid_without_invitation(self):
        from test_call_model import ChatProvider

        event, provider, stt = self.host_event(mid="call-model")
        disabled = ChatProvider("disabled")
        disabled.provider_config["enable"] = False
        self.context.by_id.update(disabled=disabled, wrong=stt)
        for identifier in ("missing", "disabled", "wrong"):
            plugin = self.plugin(dict(self.settings(), call_chat_provider_id=identifier))
            await plugin.initialize()
            notices = []

            async def notice(event, text):
                notices.append(text)

            plugin._notice = notice
            self.assertEqual(await self.dispatch(plugin, event), "chat_unavailable")
            self.assertEqual(plugin._lifecycle._invitations, [])
            self.assertFalse(plugin._lifecycle._backend._preparations)
            self.assertEqual(self.qq_calls, [])
            self.assertTrue(any("通话专用模型" in text for text in notices))
            data = await plugin._lifecycle._diagnostic_data()
            self.assertEqual(data["call_model"]["selection"], "dedicated")
            self.assertFalse(data["call_model"]["available"])
            await plugin.terminate()
        self.assertEqual(provider.calls, [])


class NativeRebindTests(NativeCase):
    async def test_time_wait_rebind_and_active_listener_stays_exclusive(self):
        import socket
        from bot_call.native_host import NativeServer, NativeHostError

        async def app(scope, receive, send):
            raise AssertionError("probe uses an owned socket, not ASGI traffic")

        app.lifespans = set()

        async def serve(app, config, *, shutdown_trigger, mode):
            sock = config.create_sockets().insecure_sockets[0]
            sock.listen()
            await config.logger_class(config).info("Running on http://127.0.0.1:probe")
            await shutdown_trigger()

        def time_wait(port):
            return any(
                row.split()[1].endswith(f":{port:04X}") and row.split()[3] == "06"
                for row in Path("/proc/net/tcp").read_text().splitlines()[1:]
            )

        with patch("hypercorn.asyncio.serve", serve):
            original = NativeServer(app, 0)
            await original.start()
            port = original._socket.getsockname()[1]
            self.assertEqual(original._socket.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR), 1)
            # Accepted socket inherits REUSEADDR. Server actively closes first,
            # then peer closes: the server port is genuinely in kernel TIME_WAIT.
            peer = socket.socket()
            peer.settimeout(1)
            peer.connect(("127.0.0.1", port))
            loop = asyncio.get_running_loop()
            accepted, _ = await loop.sock_accept(original._socket)
            accepted.shutdown(socket.SHUT_WR)
            self.assertEqual(peer.recv(1), b"")
            peer.shutdown(socket.SHUT_WR)
            self.assertEqual(await asyncio.wait_for(loop.sock_recv(accepted, 1), 1), b"")
            accepted.close()
            peer.close()
            await original.close()
            for _ in range(100):
                if time_wait(port):
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(time_wait(port), "fixture must create real TIME_WAIT")
            with socket.socket() as without_reuse:
                with self.assertRaises(OSError):
                    without_reuse.bind(("127.0.0.1", port))
            replacement = NativeServer(app, port)
            blocked = NativeServer(app, port)
            try:
                await replacement.start()
                self.assertTrue(replacement.ready)
                with self.assertRaises(NativeHostError):
                    await blocked.start()
                self.assertFalse(blocked.ready)
                self.assertTrue(blocked.stopped)
                self.assertEqual(
                    replacement._socket.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN), 1
                )
            finally:
                await replacement.close()
