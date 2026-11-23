"""Fixed HTTPS boundary: real sockets and a local streaming TCP reverse proxy."""

import asyncio
from contextlib import asynccontextmanager
import json
import socket
import fcntl
import struct
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import httpx
from websockets.asyncio.client import connect

from bot_call.native_host import HostASGI, NativeServer, settings_from
from bot_call.plugin import PluginLifecycle
from bot_call.proxy_access import Rejections, listen_address
from bot_call.settings import SettingsInvalid, validate_config
from bot_call.web import CallWebApp, COOKIE


ORIGIN = "https://calls.example.test"


class Runtime:
    @asynccontextmanager
    async def watch(self, token, reference, **kwargs):
        if token != "synthetic" or reference != "a" * 64:
            raise ValueError
        queue = asyncio.Queue()
        queue.audio = asyncio.Queue()
        queue.pcm_closed = False
        yield queue


class ProxyAccessTests(unittest.IsolatedAsyncioTestCase):
    def test_settings(self):
        defaults = validate_config({})
        self.assertEqual(listen_address(defaults), "127.0.0.1")
        self.assertEqual(defaults["trusted_proxies"], ["127.0.0.1"])
        good = dict(
            public_origin=ORIGIN, listen_host="0.0.0.0", trusted_proxies=["192.0.2.0/24", "::1"]
        )
        self.assertEqual(listen_address(validate_config(good)), "0.0.0.0")
        for values in (["0.0.0.0/0"], ["::/0"], ["::ffff:0:0/96"], [], ["hostname"]):
            with self.subTest(values=values), self.assertRaises(SettingsInvalid):
                validate_config(dict(good, trusted_proxies=values))
        for host in ("localhost", "::1%lo", "[::1]", "127.0.0.1:6191"):
            with self.subTest(host=host), self.assertRaises(SettingsInvalid):
                validate_config(dict(good, listen_host=host))
        with self.assertRaises(SettingsInvalid):
            validate_config(dict(good, trusted_proxies=["127.0.0.1"]))
        quick = settings_from(dict(access_mode="quick_tunnel", listen_host="0.0.0.0"))
        self.assertEqual(listen_address(quick), "127.0.0.1")

    async def test_http_ws_same_policy_and_private_diagnostics(self):
        reached = []

        async def app(scope, receive, send):
            reached.append(scope["scheme"])

        diag = Rejections()
        host = HostASGI(app, lambda: True, ["192.0.2.0/24"], diag, ORIGIN)
        for kind in ("http", "websocket"):
            for peer, proto, accepted in (
                ("192.0.2.2", [b"https"], True),
                ("198.51.100.2", [b"https"], False),
                ("::ffff:192.0.2.2", [b"https"], True),
                ("::ffff:198.51.100.2", [b"https"], False),
                ("::ffff:192.0.2.2", [b"https", b"https"], False),
                ("192.0.2.2", [b"https", b"https"], False),
                ("192.0.2.2", [], False),
                ("192.0.2.2", [b"http"], False),
            ):
                with self.subTest(kind=kind, peer=peer, proto=proto):
                    scope = dict(
                        type=kind,
                        client=(peer, 1000),
                        query_string=b"secret=PRIVATE",
                        headers=[(b"x-forwarded-proto", v) for v in proto]
                        + [
                            (b"cookie", b"PRIVATE"),
                            (b"host", b"user:PRIVATE@bad/?PRIVATE"),
                            (b"origin", b"https://PRIVATE"),
                        ],
                    )
                    before = len(reached)
                    sent = []

                    async def send(value):
                        sent.append(value)

                    await host(scope, None, send)
                    self.assertEqual(len(reached) > before, accepted)
                    if not accepted and peer.startswith("::ffff:"):
                        self.assertEqual(diag.latest["peer"], peer.removeprefix("::ffff:"))
        self.assertNotIn("PRIVATE", json.dumps(diag.latest))
        web = CallWebApp(Runtime(), origin=ORIGIN)
        web.rejections = diag
        for headers, write, reason in (
            ([(b"host", b"wrong")], False, "host_mismatch"),
            (
                [(b"host", b"calls.example.test"), (b"origin", b"https://PRIVATE")],
                True,
                "origin_mismatch",
            ),
            ([(b"host", b"calls.example.test")] * 2, False, "headers_invalid"),
        ):
            with self.assertRaises(Exception):
                web._headers(dict(scheme="https", headers=headers), write)
            self.assertEqual(diag.latest["reason"], reason)
            self.assertNotIn("PRIVATE", json.dumps(diag.latest))
        # The default proxy gate retains the exact existing peer behavior.
        default = HostASGI(app, lambda: True)
        for peer, accepted in (("127.0.0.1", True), ("127.0.0.2", False), ("::1", False)):
            before = len(reached)
            await default(
                dict(type="http", client=(peer, 0), headers=[(b"x-forwarded-proto", b"https")]),
                None,
                send,
            )
            self.assertEqual(len(reached) > before, accepted)

    def test_diagnostics_rate_limit_and_log_privacy(self):
        diag = Rejections()
        scope = dict(
            client=("192.0.2.1", 1),
            query_string=b"PRIVATE",
            headers=[
                (b"cookie", b"PRIVATE"),
                (b"authorization", b"PRIVATE"),
                (b"origin", b"https://PRIVATE"),
                (b"host", b"bad/?PRIVATE"),
            ],
        )
        with patch("bot_call.proxy_access.time.monotonic", side_effect=[0, 1, 10]):
            with self.assertLogs("bot_call.access", level="WARNING") as logs:
                for reason in ("proxy_rejected", "host_mismatch", "origin_mismatch"):
                    diag.record(scope, reason, ORIGIN)
        self.assertEqual(len(logs.output), 2)
        self.assertNotIn("PRIVATE", "".join(logs.output))
        self.assertEqual(diag.latest["reason"], "origin_mismatch")

    async def test_ipv6_listener(self):
        web = CallWebApp(Runtime(), origin=ORIGIN)
        server = NativeServer(HostASGI(web, lambda: True, ["::1"]), 0, "::1")
        await server.start()
        try:
            self.assertEqual(server._socket.family, socket.AF_INET6)
            self.assertEqual(server._socket.getsockname()[0], "::1")
            async with httpx.AsyncClient(trust_env=False) as client:
                response = await client.get(
                    f"http://[::1]:{server._socket.getsockname()[1]}/bot-call/",
                    headers={"Host": "calls.example.test", "X-Forwarded-Proto": "https"},
                )
                self.assertEqual(response.status_code, 200)
        finally:
            await server.close()

    async def test_real_nonloopback_proxy_page_ws_and_probe(self):
        # Discover an actual local IPv4 interface without making a network request.
        ip = None
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            for _, name in socket.if_nameindex():
                if name == "lo":
                    continue
                try:
                    result = fcntl.ioctl(probe, 0x8915, struct.pack("256s", name.encode()))
                    candidate = socket.inet_ntoa(result[20:24])
                    if not candidate.startswith("127."):
                        ip = candidate
                        break
                except OSError:
                    continue
        if not ip:
            self.skipTest("No non-loopback IPv4 interface")
        web = CallWebApp(Runtime(), origin=ORIGIN)
        diag = Rejections()
        web.rejections = diag
        gate = HostASGI(web, lambda: True, [ip + "/32"], diag, ORIGIN)
        server = NativeServer(gate, 0, "0.0.0.0")
        await server.start()
        port = server._socket.getsockname()[1]
        owner = SimpleNamespace(_claimed=False)

        async def terminate():
            pass

        owner.terminate = terminate
        lifecycle = SimpleNamespace(_loop=asyncio.get_running_loop(), _context=SimpleNamespace())
        # The orphan release probe uses the selected bind address, and detects
        # a wildcard listener that would prevent the real server's next bind.
        with self.assertRaises(OSError):
            await PluginLifecycle._stop_orphan(lifecycle, owner, port, "0.0.0.0")
        active = set()

        async def relay(reader, writer):
            active.add(asyncio.current_task())
            upstream = None
            try:
                r, upstream = await asyncio.open_connection(ip, port)
                head = await reader.readuntil(b"\r\n\r\n")
                # Minimal local reverse proxy overwrites the external proto header.
                lines = head.split(b"\r\n")
                lines = [v for v in lines if not v.lower().startswith(b"x-forwarded-proto:")]
                upstream.write(b"\r\n".join(lines[:-2]) + b"\r\nX-Forwarded-Proto: https\r\n\r\n")
                await upstream.drain()

                async def pipe(src, dst):
                    while data := await src.read(65536):
                        dst.write(data)
                        await dst.drain()

                jobs = [
                    asyncio.create_task(pipe(reader, upstream)),
                    asyncio.create_task(pipe(r, writer)),
                ]
                await asyncio.wait(jobs, return_when=asyncio.FIRST_COMPLETED)
                for job in jobs:
                    job.cancel()
                await asyncio.gather(*jobs, return_exceptions=True)
            finally:
                writer.close()
                if upstream:
                    upstream.close()
                active.discard(asyncio.current_task())

        proxy = await asyncio.start_server(relay, "127.0.0.1", 0)
        proxy_port = proxy.sockets[0].getsockname()[1]
        try:
            self.assertEqual(server._socket.getsockname()[0], "0.0.0.0")
            async with httpx.AsyncClient(trust_env=False) as client:
                response = await client.get(
                    f"http://127.0.0.1:{proxy_port}/bot-call/",
                    headers={"Host": "calls.example.test"},
                )
                self.assertEqual(response.status_code, 200)
                response = await client.get(
                    f"http://127.0.0.1:{port}/bot-call/",
                    headers={"Host": "calls.example.test", "X-Forwarded-Proto": "https"},
                )
                self.assertEqual(response.status_code, 403)
            # Nonloopback proxy reaches actual app credential checks and WS accept.
            async with connect(
                "ws://calls.example.test/bot-call/ws",
                host="127.0.0.1",
                port=proxy_port,
                proxy=None,
                origin=ORIGIN,
                subprotocols=["bot-call." + "a" * 64],
                additional_headers={"Cookie": COOKIE + "=synthetic"},
            ):
                pass
        finally:
            proxy.close()
            await proxy.wait_closed()
            for task in tuple(active):
                task.cancel()
            if active:
                await asyncio.gather(*active, return_exceptions=True)
            await server.close()
        await PluginLifecycle._stop_orphan(lifecycle, owner, port, "0.0.0.0")
