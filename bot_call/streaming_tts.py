"""Cancellable streaming TTS with bounded mono PCM16 at 24 kHz."""

import asyncio
from contextlib import AsyncExitStack
from fnmatch import fnmatchcase
import ipaddress
import json
import os
import struct
from urllib.parse import urlsplit
import uuid

import aiohttp
from yarl import URL as YarlURL

from .observability import CURRENT_TIMING
from .speech import MAX_SPEECH_BYTES

URL = "wss://dashscope.aliyuncs.com/api-ws/v1/inference"
CHUNK_BYTES = 1920  # 40 ms, little-endian signed PCM16, mono at 24 kHz.
# Connect phase timeout covering TCP, proxy CONNECT, TLS, and WebSocket 101 handshake.
# Production checks reach DashScope over a slow HTTP proxy in ~2.4 s, while blocked direct
# sockets would otherwise stall for the full provider timeout (20 s by default).
# DashScope's own SDK caps WebSocket connect at 5 s; use the same bound so a slow but
# working proxy is not misjudged, while a dead path still falls back quickly.
CONNECT_TIMEOUT = 5.0

FALLBACK_CONNECT_TIMEOUT = "connect_timeout"
FALLBACK_PROXY_UNSUPPORTED = "proxy_unsupported"
FALLBACK_HANDSHAKE_FAILED = "handshake_failed"
FALLBACK_OTHER = "other"
FALLBACK_TASK_FAILED = "task_failed"
FALLBACK_FORMAT_UNSUPPORTED = "format_unsupported"

_PROXY_VARS_BY_SCHEME = {
    "wss": ("wss_proxy", "https_proxy", "all_proxy"),
    "https": ("https_proxy", "all_proxy"),
    "ws": ("ws_proxy", "http_proxy", "all_proxy"),
    "http": ("http_proxy", "all_proxy"),
}
_DEFAULT_PORTS = {"http": 80, "ws": 80, "https": 443, "wss": 443}


class _UnsupportedProxyError(ValueError):
    pass


class StreamFailure(RuntimeError):
    def __init__(self, received=False, reason=FALLBACK_OTHER):
        super().__init__("tts.stream_failed")
        self.received = received
        self.reason = reason


def _env_value(environ, lower_name):
    """Return proxy env var preferring exact lowercase (empty string clears uppercase)."""
    if lower_name in environ:
        return environ[lower_name].strip()
    upper_name = lower_name.upper()
    if upper_name in environ:
        if lower_name == "http_proxy" and "REQUEST_METHOD" in environ:
            return ""
        return environ[upper_name].strip()
    for key, value in environ.items():
        if key.lower() == lower_name:
            if lower_name == "http_proxy" and "REQUEST_METHOD" in environ:
                return ""
            return value.strip()
    return ""


def _split_host_port(entry):
    if entry.startswith("["):
        end = entry.find("]")
        if end != -1:
            host = entry[1:end]
            rest = entry[end + 1 :]
            if not rest:
                return host, None
            if rest.startswith(":") and rest[1:].isdigit():
                return host, int(rest[1:])
        return entry, None
    if entry.count(":") == 1:
        host, port = entry.rsplit(":", 1)
        if port.isdigit():
            return host, int(port)
    return entry, None


def should_bypass_proxy(url, no_proxy=None, *, environ=None):
    """Return True if url matches NO_PROXY (hostname, suffix, wildcard, IP, or CIDR)."""
    env = os.environ if environ is None else environ
    if no_proxy is None:
        no_proxy = _env_value(env, "no_proxy")
    target = urlsplit(str(url))
    hostname = target.hostname
    if not hostname:
        return True
    if not no_proxy:
        return False

    hostname_norm = hostname.lower().rstrip(".")
    target_port = target.port or _DEFAULT_PORTS.get(target.scheme.lower())
    host_ip = hostname.split("%", 1)[0]
    try:
        target_ip = ipaddress.ip_address(host_ip)
    except ValueError:
        target_ip = None

    for raw in no_proxy.split(","):
        entry = raw.strip()
        if not entry:
            continue
        if entry == "*":
            return True
        if target_ip is not None:
            if "/" in entry:
                try:
                    if target_ip in ipaddress.ip_network(entry.strip("[]"), strict=False):
                        return True
                except ValueError:
                    pass
                continue
            entry_host, entry_port = _split_host_port(entry)
            try:
                if ipaddress.ip_address(entry_host.split("%", 1)[0]) == target_ip:
                    if entry_port is None or entry_port == target_port:
                        return True
                continue
            except ValueError:
                pass
            if "*" in entry_host and fnmatchcase(host_ip.lower(), entry_host.lower()):
                if entry_port is None or entry_port == target_port:
                    return True
            continue

        entry_host, entry_port = _split_host_port(entry)
        if entry_port is not None and entry_port != target_port:
            continue
        if "*" in entry_host:
            if fnmatchcase(hostname_norm, entry_host.lower().rstrip(".")):
                return True
            continue
        domain = entry_host.lstrip(".").lower().rstrip(".")
        if domain and (hostname_norm == domain or hostname_norm.endswith("." + domain)):
            return True
    return False


def resolve_proxy(url, environ=None):
    """Select an http/https proxy URL and optional Proxy-Authorization header from env."""
    env = os.environ if environ is None else environ
    if should_bypass_proxy(url, environ=env):
        return None, None
    scheme = urlsplit(str(url)).scheme.lower()
    candidates = _PROXY_VARS_BY_SCHEME.get(scheme, ("https_proxy", "http_proxy", "all_proxy"))
    raw_proxy = ""
    for var in candidates:
        raw_proxy = _env_value(env, var)
        if raw_proxy:
            break
    if not raw_proxy:
        return None, None
    return _parse_proxy(raw_proxy)


def _parse_proxy(raw_proxy):
    if "://" not in raw_proxy:
        raw_proxy = "http://" + raw_proxy
    try:
        parsed = YarlURL(raw_proxy)
        host, port = parsed.host, parsed.port
        if (
            parsed.scheme not in {"http", "https"}
            or not host
            or port == 0
            or any(char.isspace() or ord(char) <= 32 or ord(char) == 127 for char in raw_proxy)
        ):
            raise ValueError
        proxy_headers = None
        if parsed.raw_user is not None or parsed.raw_password is not None:
            proxy_headers = {
                "Proxy-Authorization": aiohttp.encode_basic_auth(
                    parsed.user or "", parsed.password or ""
                )
            }
            parsed = parsed.with_user(None)
        return str(parsed), proxy_headers
    except ValueError:
        raise _UnsupportedProxyError from None


async def stream_sentence(text, settings, emit, *, url=None, ssl=True):
    openai = settings.get("protocol", "dashscope") == "openai"
    if url is None:
        url = settings["url"] if openai else URL
    try:
        explicit_proxy = settings.get("proxy")
        proxy_url, proxy_headers = (
            _parse_proxy(explicit_proxy) if explicit_proxy else resolve_proxy(url)
        )
    except _UnsupportedProxyError:
        raise StreamFailure(False, FALLBACK_PROXY_UNSUPPORTED) from None

    if openai:
        return await _stream_openai(text, settings, emit, url, ssl, proxy_url, proxy_headers)
    return await _stream_dashscope(text, settings, emit, url, ssl, proxy_url, proxy_headers)


async def _stream_dashscope(text, settings, emit, url, ssl, proxy_url, proxy_headers):
    received, data, pending = False, bytearray(), bytearray()
    task_id = uuid.uuid4().hex
    timing = CURRENT_TIMING.get()
    start = timing.now() if timing else 0
    connected = False

    def event(action, payload):
        return dict(
            header=dict(action=action, task_id=task_id, streaming="duplex"),
            payload=payload,
        )

    try:
        total_timeout = settings["timeout"]
        connect_timeout = min(CONNECT_TIMEOUT, total_timeout)
        async with asyncio.timeout(total_timeout):
            # Keep trust_env=False so we control proxy selection, NO_PROXY CIDR matching,
            # unsupported proxy scheme rejection, and credential stripping explicitly.
            async with aiohttp.ClientSession(trust_env=False) as session:
                async with AsyncExitStack() as stack:
                    async with asyncio.timeout(connect_timeout):
                        ws = await stack.enter_async_context(
                            session.ws_connect(
                                url,
                                headers=settings["headers"],
                                max_msg_size=1024 * 1024,
                                proxy=proxy_url,
                                proxy_headers=proxy_headers,
                                ssl=ssl,
                            )
                        )
                    connected = True
                    await ws.send_json(
                        event(
                            "run-task",
                            dict(
                                task_group="audio",
                                task="tts",
                                function="SpeechSynthesizer",
                                model=settings["model"],
                                parameters=dict(
                                    text_type="PlainText",
                                    voice=settings["voice"],
                                    format="pcm",
                                    sample_rate=24000,
                                    volume=50,
                                    rate=1,
                                    pitch=1,
                                ),
                                input={},
                            ),
                        )
                    )
                    started, finished = False, False
                    async for message in ws:
                        if message.type == aiohttp.WSMsgType.BINARY:
                            if not started or not message.data:
                                raise ValueError
                            if not received:
                                received = True
                                if timing:
                                    timing.add("tts_first_chunk", start)
                            data.extend(message.data)
                            pending.extend(message.data)
                            if len(data) > MAX_SPEECH_BYTES:
                                raise ValueError
                            while len(pending) >= CHUNK_BYTES:
                                await emit(bytes(pending[:CHUNK_BYTES]))
                                del pending[:CHUNK_BYTES]
                        elif message.type == aiohttp.WSMsgType.TEXT:
                            header = json.loads(message.data).get("header", {})
                            if header.get("task_id") != task_id:
                                raise ValueError
                            name = header.get("event")
                            if name == "task-started":
                                if started:
                                    raise ValueError
                                started = True
                                await ws.send_json(
                                    event("continue-task", dict(input=dict(text=text)))
                                )
                                await ws.send_json(event("finish-task", dict(input={})))
                            elif name == "task-failed":
                                raise StreamFailure(received, FALLBACK_TASK_FAILED)
                            elif name == "task-finished":
                                finished = True
                                break
                        else:
                            raise ValueError
                    if not finished or not data or len(data) % 2:
                        raise ValueError
                    if pending:
                        await emit(bytes(pending))
                    return bytes(data)
    except asyncio.CancelledError:
        # Context managers close the owned upstream connection before propagating.
        raise
    except StreamFailure:
        raise
    except TimeoutError:
        reason = FALLBACK_OTHER if connected else FALLBACK_CONNECT_TIMEOUT
        raise StreamFailure(received, reason) from None
    except Exception:
        reason = FALLBACK_OTHER if connected else FALLBACK_HANDSHAKE_FAILED
        raise StreamFailure(received, reason) from None


# Bound RIFF metadata reads before any bytes reach the player. Streaming writers
# commonly use zero or a sentinel near the signed/unsigned 32-bit size limit.
_WAV_HEADER_LIMIT = 64 * 1024
_WAV_STREAM_SIZE = 0x7FFFF000
_WAV_TYPES = {"audio/wav", "audio/x-wav", "audio/wave", "audio/vnd.wave"}
_COMPRESSED_TYPES = {
    "audio/mpeg",
    "audio/mp3",
    "audio/ogg",
    "audio/opus",
    "audio/flac",
    "audio/aac",
    "audio/mp4",
    "audio/webm",
    "application/ogg",
}
_NON_AUDIO_TYPES = {
    "application/json",
    "application/problem+json",
    "application/xml",
    "application/xhtml+xml",
}
_NON_PCM_PREFIXES = (b"riff", b"id3", b"oggs", b"flac", b"<!do", b"<htm", b'{"er', b'{"me', b'{"de')
_PCM_GUID = bytes.fromhex("01000000 0000 1000 8000 00aa00389b71")


def _check_wav_format(fmt):
    if len(fmt) < 16:
        return False
    codec, channels, rate, _, align, bits = struct.unpack_from("<HHIIHH", fmt)
    if (channels, rate, align, bits) != (1, 24000, 2, 16):
        return False
    if codec == 1:
        return True
    if codec != 0xFFFE or len(fmt) < 40:
        return False
    extension_size, valid_bits = struct.unpack_from("<HH", fmt, 16)
    return 22 <= extension_size <= len(fmt) - 18 and valid_bits == 16 and fmt[24:40] == _PCM_GUID


async def _wav_data_size(content):
    remaining = _WAV_HEADER_LIMIT

    async def read_header(size):
        nonlocal remaining
        remaining -= size
        if remaining < 0:
            raise StreamFailure(False, FALLBACK_FORMAT_UNSUPPORTED)
        return await content.readexactly(size)

    try:
        riff = await read_header(12)
        if riff[:4] != b"RIFF" or riff[8:] != b"WAVE":
            raise StreamFailure(False, FALLBACK_FORMAT_UNSUPPORTED)
        has_format = False
        while True:
            kind, size = struct.unpack("<4sI", await read_header(8))
            if kind == b"data":
                if not has_format:
                    raise StreamFailure(False, FALLBACK_FORMAT_UNSUPPORTED)
                return size if 0 < size < _WAV_STREAM_SIZE else None
            block = await read_header(size + size % 2)
            if kind == b"fmt ":
                if not _check_wav_format(block[:size]):
                    raise StreamFailure(False, FALLBACK_FORMAT_UNSUPPORTED)
                has_format = True
    except asyncio.IncompleteReadError:
        raise StreamFailure(False, FALLBACK_FORMAT_UNSUPPORTED) from None


async def _pcm_start(content):
    # A short first chunk can rule out the magic prefixes while the response is still open.
    prefix = b""
    while len(prefix) < 4:
        chunk = await content.read(4 - len(prefix))
        if not chunk:
            break
        prefix += chunk
        normalized = prefix.lower()
        if normalized.startswith(_NON_PCM_PREFIXES):
            raise StreamFailure(False, FALLBACK_FORMAT_UNSUPPORTED)
        if not any(magic.startswith(normalized) for magic in _NON_PCM_PREFIXES):
            break
    return prefix


async def _stream_openai(text, settings, emit, url, ssl, proxy_url, proxy_headers):
    response_format = settings["response_format"]
    payload = dict(
        model=settings["model"],
        voice=settings["voice"],
        input=text,
        response_format=response_format,
    )
    if settings.get("sample_rate") is not None:
        payload["sample_rate"] = settings["sample_rate"]

    headers = settings["headers"]
    if proxy_headers and urlsplit(url).scheme == "http":
        # HTTP proxies receive the request directly. HTTPS sends these headers on CONNECT.
        headers = {**headers, **proxy_headers}

    total_timeout = settings["timeout"]
    timeout = aiohttp.ClientTimeout(sock_connect=min(CONNECT_TIMEOUT, total_timeout))
    connected = response_started = received = False
    data, pending = bytearray(), bytearray()
    timing = CURRENT_TIMING.get()
    start = timing.now() if timing else 0

    trace = aiohttp.TraceConfig()

    async def connected_to_upstream(*args):
        nonlocal connected
        connected = True

    trace.on_connection_create_end.append(connected_to_upstream)

    async def consume(chunk):
        nonlocal received
        if not chunk:
            return
        if not received:
            received = True
            if timing:
                timing.add("tts_first_chunk", start)
        if len(data) + len(chunk) > MAX_SPEECH_BYTES:
            raise StreamFailure(True, FALLBACK_OTHER)
        data.extend(chunk)
        pending.extend(chunk)
        while len(pending) >= CHUNK_BYTES:
            await emit(bytes(pending[:CHUNK_BYTES]))
            del pending[:CHUNK_BYTES]

    try:
        async with asyncio.timeout(total_timeout):
            async with aiohttp.ClientSession(
                trust_env=False, timeout=timeout, trace_configs=[trace]
            ) as session:
                async with session.post(
                    url,
                    headers=headers,
                    json=payload,
                    proxy=proxy_url,
                    proxy_headers=proxy_headers,
                    ssl=ssl,
                    allow_redirects=False,
                ) as response:
                    response_started = True
                    if response.status != 200:
                        reason = (
                            FALLBACK_TASK_FAILED if 400 <= response.status < 500 else FALLBACK_OTHER
                        )
                        raise StreamFailure(False, reason)
                    mime = response.content_type
                    if (
                        mime.startswith("text/")
                        or mime in _NON_AUDIO_TYPES
                        or mime in _COMPRESSED_TYPES
                        or (response_format == "pcm" and mime in _WAV_TYPES)
                    ):
                        raise StreamFailure(False, FALLBACK_FORMAT_UNSUPPORTED)

                    content = response.content
                    remaining = None
                    if response_format == "wav":
                        remaining = await _wav_data_size(content)
                    else:
                        await consume(await _pcm_start(content))

                    async for chunk in content.iter_chunked(CHUNK_BYTES):
                        if remaining is not None:
                            chunk = chunk[:remaining]
                            remaining -= len(chunk)
                        await consume(chunk)
                        if remaining == 0:
                            break
                    if not data or len(data) % 2 or (remaining is not None and remaining != 0):
                        raise StreamFailure(received, FALLBACK_OTHER)
                    if pending:
                        await emit(bytes(pending))
                    return bytes(data)
    except StreamFailure:
        raise
    except TimeoutError:
        reason = FALLBACK_OTHER if connected else FALLBACK_CONNECT_TIMEOUT
        raise StreamFailure(received, reason) from None
    except Exception:
        reason = FALLBACK_OTHER if response_started else FALLBACK_HANDSHAKE_FAILED
        raise StreamFailure(received, reason) from None
