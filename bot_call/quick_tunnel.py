"""Owned, opt-in cloudflared Quick Tunnel. No shell, downloads or raw logging."""

import asyncio
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import uuid

# Fail closed on changed formats, ambiguity or excessive output. No raw history.
LINE_LIMIT = 8192
START_OUTPUT_LIMIT = 1048576
ORIGIN = re.compile(r"https://[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.trycloudflare\.com")

_PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
)


def _tunnel_env(environ=None):
    if environ is None:
        environ = os.environ
    env = {"PATH": environ.get("PATH") or os.defpath, "LANG": "C", "NO_COLOR": "1"}
    for key in _PROXY_ENV_KEYS:
        value = environ.get(key)
        if isinstance(value, str) and value != "":
            env[key] = value
    return env


class TunnelError(RuntimeError):
    def __init__(self):
        super().__init__("bot_call.tunnel_unavailable")


def _consume(task):
    if not task.cancelled():
        task.exception()


async def _wait(task):
    try:
        await asyncio.wait({task})
        return task.result()
    except asyncio.CancelledError:
        raise asyncio.CancelledError() from None


def _pairs(pairs):
    result = {}
    for k, v in pairs:
        if k in result:
            raise ValueError
        result[k] = v
    return result


class QuickTunnel:
    """Retain spawn, monitor, pipe and wait tasks until close really reaps them.

    start/close waiters may be cancelled; actual owned operations are never
    cancelled by those waiters. 'ready' means address + registration receipt,
    not an end-to-end HTTPS/mobile connectivity claim.
    """

    def __init__(self, executable, port, timeout):
        self.executable, self.port, self.timeout = executable, port, timeout
        self.origin = None
        self.registered = False
        self.task = self._launch_task = self._reader = self._exit = self._close_task = None
        self._receipt = asyncio.get_running_loop().create_future()
        self._directory = None
        self._process = None
        self._closing = self.stopped = False
        self.term_timeout = 5

    @property
    def ready(self):
        return (
            self.origin is not None
            and self.registered
            and not self._closing
            and self._process is not None
            and self._process.returncode is None
            and self.task is not None
            and not self.task.done()
            and self._reader is not None
            and not self._reader.done()
            and self._exit is not None
            and not self._exit.done()
        )

    @property
    def failed(self):
        return not self._closing and (
            (self.task is not None and self.task.done())
            or (self._reader is not None and self._reader.done())
            or (self._process is not None and self._process.returncode is not None)
        )

    async def start(self):
        if self.task is None and not self._closing:
            self._launch_task = asyncio.create_task(self._launch(), name="BotCall:tunnel-spawn")
            self._launch_task.add_done_callback(_consume)
            self.task = asyncio.create_task(self._monitor(), name="BotCall:tunnel-monitor")
            self.task.add_done_callback(_consume)
        if self.task is None:
            raise TunnelError() from None
        await asyncio.wait(
            {self.task, self._receipt}, timeout=self.timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if not self._receipt.done() or not self.ready:
            raise TunnelError() from None
        return self.origin

    async def _launch(self):
        try:
            executable = shutil.which(self.executable)
            if executable is None:
                raise ValueError
            executable = str(Path(executable).resolve(strict=True))
            self._directory = tempfile.TemporaryDirectory(prefix="bot-call-tunnel-")
            directory = Path(self._directory.name)
            config = directory / "isolated.yml"
            config.write_text("{}\n", encoding="ascii")
            config.chmod(0o600)
            # Explicit config overrides user and system config search. The child
            # inherits only PATH and allowlisted proxy variables, isolating tunnel
            # options, config lookup, unrelated credentials and loader settings.
            self._process = await asyncio.create_subprocess_exec(
                executable,
                "tunnel",
                "--config",
                str(config),
                "--no-autoupdate",
                "--output",
                "json",
                "--loglevel",
                "info",
                "--protocol",
                "auto",
                "--metrics",
                "127.0.0.1:0",
                "--management-diagnostics=false",
                "--url",
                f"http://127.0.0.1:{self.port}",
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=_tunnel_env(),
                cwd=str(directory),
                limit=LINE_LIMIT * 2,
            )
            self._exit = asyncio.create_task(self._process.wait(), name="BotCall:tunnel-wait")
            self._exit.add_done_callback(_consume)
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            raise TunnelError() from None

    def _line(self, raw):
        try:
            item = json.loads(raw, object_pairs_hook=_pairs)
            if type(item) is not dict or type(item.get("message")) is not str:
                return
            message = item["message"]
            # LogTable renders one row per JSON message. Accept only the entire
            # boxed origin, never a URL substring, suffix, path or extra label.
            if ".trycloudflare.com" in message:
                match = re.fullmatch(r"\|\s*(https://[^\s|]+)\s*\|", message)
                if match is None or ORIGIN.fullmatch(match[1]) is None:
                    raise ValueError
                origin = match[1]
                if self.origin is not None and self.origin != origin:
                    raise ValueError
                self.origin = origin
            if message == "Registered tunnel connection":
                if (
                    item.get("level") != "info"
                    or type(item.get("connIndex")) is not int
                    or not 0 <= item["connIndex"] <= 255
                    or item.get("protocol") not in {"quic", "http2"}
                    or type(item.get("location")) is not str
                    or re.fullmatch(r"[a-z0-9]{2,16}", item["location"]) is None
                    or type(item.get("connection")) is not str
                    or str(uuid.UUID(item["connection"])) != item["connection"]
                ):
                    raise ValueError
                ipaddress.ip_address(item["ip"])
                self.registered = True
            if self.origin is not None and self.registered and not self._receipt.done():
                self._receipt.set_result(None)
        except (json.JSONDecodeError, UnicodeDecodeError):
            # Non-JSON banners/diagnostics never constitute a receipt or address.
            return
        except Exception:
            raise TunnelError() from None

    async def _read(self):
        total, buffer = 0, b""
        try:
            while True:
                chunk = await self._process.stdout.read(2048)
                if not chunk:
                    # An unterminated/truncated record is never a receipt.
                    return
                if not self._receipt.done():
                    total += len(chunk)
                    if total > START_OUTPUT_LIMIT:
                        raise TunnelError()
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if len(line) > LINE_LIMIT:
                        raise TunnelError()
                    self._line(line)
                if len(buffer) > LINE_LIMIT:
                    raise TunnelError()
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            raise TunnelError() from None

    async def _monitor(self):
        try:
            await _wait(self._launch_task)
            self._reader = asyncio.create_task(self._read(), name="BotCall:tunnel-output")
            self._reader.add_done_callback(_consume)
            await asyncio.wait({self._reader, self._exit}, return_when=asyncio.FIRST_COMPLETED)
            # Pipe EOF, parser faults and process exit all invalidate this origin.
            if not self._closing:
                raise TunnelError()
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            raise TunnelError() from None

    async def close(self):
        self._closing = True  # Synchronous rejection before any await.
        if self._close_task is None or (
            self._close_task.done()
            and (self._close_task.cancelled() or self._close_task.exception() is not None)
        ):
            self._close_task = asyncio.create_task(self._close(), name="BotCall:tunnel-close")
            self._close_task.add_done_callback(_consume)
        return await _wait(self._close_task)

    async def _close(self):
        if self._launch_task is not None:
            try:
                await _wait(self._launch_task)  # Spawn may still be returning a child.
            except Exception:
                pass
        if self._process is not None:
            if self._process.returncode is None:
                try:
                    self._process.terminate()
                except ProcessLookupError:
                    pass
                await asyncio.wait({self._exit}, timeout=self.term_timeout)
                if not self._exit.done():
                    try:
                        self._process.kill()
                    except ProcessLookupError:
                        pass
            await _wait(self._exit)  # Always reap, including after KILL.
        if self._reader is not None and not self._reader.done():
            self._reader.cancel()
        if self._reader is not None:
            await asyncio.gather(self._reader, return_exceptions=True)
        if self.task is not None:
            await asyncio.gather(self.task, return_exceptions=True)
        if self._directory is not None:
            self._directory.cleanup()
        self.origin, self.registered, self.stopped = None, False, True
        return True
