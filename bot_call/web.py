"""An owned HTTP-only ASGI callable; no listener, framework or management JWT."""

import asyncio
import json
import gzip
import re
from pathlib import Path
from urllib.parse import urlsplit

from .access import AccessError
from .audio import AUDIO_MIMES, AudioError, ClosingReceipt
from .runtime import RuntimeClosed, RuntimeOperationError

COOKIE = "__Secure-bot_call"
PATH = "/bot-call/"
REFERENCE_HEADER = "x-bot-call-reference"


class _HTTPError(Exception):
    def __init__(self, status, code):
        self.status, self.code = status, code


class _Static:
    def __init__(self, body, content_type, immutable=False):
        self.body, self.content_type, self.immutable = body, content_type, immutable
        self.gzip_task = None

    async def compressed(self):
        if self.gzip_task is None:
            self.gzip_task = asyncio.create_task(
                asyncio.to_thread(gzip.compress, self.body, compresslevel=9, mtime=0)
            )
        return await asyncio.shield(self.gzip_task)


def _accepts_gzip(value):
    qualities = {}
    for item in value.lower().split(","):
        name, *parameters = item.strip().split(";")
        quality = 1.0
        for parameter in parameters:
            key, _, raw = parameter.strip().partition("=")
            if key == "q":
                try:
                    quality = float(raw)
                except ValueError:
                    quality = 0
        qualities[name.strip()] = quality if 0 <= quality <= 1 else 0
    return qualities.get("gzip", qualities.get("*", 0)) > 0


class _Disconnected(Exception):
    pass


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _bad_constant(unused):
    raise ValueError


class CallWebApp:
    def __init__(
        self,
        runtime,
        *,
        origin: str,
        max_body_bytes: int = 16384,
        max_audio_bytes: int = 2097152,
        call_vad_silence_ms: int = 1200,
        call_vad_sensitivity: int = 50,
        call_max_continuous_segments: int = 10,
        call_capture_mode: str = "auto",
        call_barge_in: str = "auto",
    ):
        try:
            parts = urlsplit(origin)
            if (
                type(origin) is not str
                or parts.scheme != "https"
                or not parts.hostname
                or parts.username is not None
                or parts.password is not None
                or parts.path
                or parts.query
                or parts.fragment
                or not parts.netloc
                or parts.netloc != parts.netloc.lower()
                or any(ord(c) <= 32 or ord(c) >= 127 for c in origin)
            ):
                raise ValueError
            parts.port
            if any(type(v) is not int or v < 1 for v in (max_body_bytes, max_audio_bytes)):
                raise ValueError
        except Exception:
            raise ValueError("call_web.invalid_configuration") from None
        if (
            call_barge_in not in ("auto", "off")
            or call_capture_mode not in ("auto", "legacy")
            or type(call_max_continuous_segments) is not int
            or not 1 <= call_max_continuous_segments <= 20
            or type(call_vad_silence_ms) is not int
            or not 300 <= call_vad_silence_ms <= 3000
            or type(call_vad_sensitivity) is not int
            or not 10 <= call_vad_sensitivity <= 90
        ):
            raise ValueError("call_web.invalid_configuration")
        self.rejections = None
        self._capabilities = {
            "seamless_series": {"version": 1, "max_segments": call_max_continuous_segments}
        }
        self._experience = {
            "silence_ms": call_vad_silence_ms,
            "sensitivity": call_vad_sensitivity,
            "capture_mode": call_capture_mode,
            "barge_in": call_barge_in if call_capture_mode == "auto" else "off",
        }
        self._runtime, self._origin, self._host = runtime, origin, parts.netloc
        self._limit, self._audio_limit = max_body_bytes, max_audio_bytes
        # Fixed resource allowlist, loaded only on explicit app construction.
        root = Path(__file__).parent / "static"
        self._assets = {
            PATH + url: _Static((root / name).read_bytes(), mime)
            for url, name, mime in [
                ("", "index.html", b"text/html; charset=utf-8"),
                ("call.css", "call.css", b"text/css; charset=utf-8"),
                ("call.js", "call.js", b"text/javascript; charset=utf-8"),
                ("capture.js", "capture.js", b"text/javascript; charset=utf-8"),
                ("capture-worklet.js", "capture-worklet.js", b"text/javascript; charset=utf-8"),
                ("capture-worker.js", "capture-worker.js", b"text/javascript; charset=utf-8"),
                ("barge.js", "barge.js", b"text/javascript; charset=utf-8"),
                ("echo.js", "echo.js", b"text/javascript; charset=utf-8"),
                ("pcm.js", "pcm.js", b"text/javascript; charset=utf-8"),
                ("page.js", "page.js", b"text/javascript; charset=utf-8"),
                ("fixed-sample.wav", "fixed-sample.wav", b"audio/wav"),
                ("capability-worker.js", "capability-worker.js", b"text/javascript; charset=utf-8"),
                ("vad.js", "vad.js", b"text/javascript; charset=utf-8"),
                ("vad-worklet.js", "vad-worklet.js", b"text/javascript; charset=utf-8"),
            ]
        }
        vendor = "vendor/vad-v5-ort-1.22.0-2623a295/"
        for name, mime in [
            ("silero_vad_v5.onnx", b"application/octet-stream"),
            ("ort.wasm.min.js", b"text/javascript; charset=utf-8"),
            ("ort-wasm-simd-threaded.mjs", b"text/javascript; charset=utf-8"),
            ("ort-wasm-simd-threaded.wasm", b"application/wasm"),
            ("VAD-SILERO-LICENSE.txt", b"text/plain; charset=utf-8"),
            ("ORT-LICENSE.txt", b"text/plain; charset=utf-8"),
            ("ORT-ThirdPartyNotices.txt", b"text/plain; charset=utf-8"),
            ("sources.json", b"application/json; charset=utf-8"),
        ]:
            self._assets[PATH + vendor + name] = _Static(
                (root / vendor / name).read_bytes(), mime, True
            )

    def _headers(self, scope, write, audio=False, *, allow_query=False):
        if (
            scope.get("scheme") != "https"
            or scope.get("root_path", "")
            or (not allow_query and scope.get("query_string", b""))
        ):
            raise _HTTPError(400, "request_rejected")
        headers = {}
        try:
            for key, value in scope.get("headers", []):
                name, value = key.decode("ascii").lower(), value.decode("ascii")
                if name in headers and name in {
                    "host",
                    "origin",
                    "cookie",
                    "content-length",
                    "content-type",
                    REFERENCE_HEADER,
                    "x-bot-utterance-id",
                    "x-bot-chunk-seq",
                    "x-bot-chunk-offset",
                    "x-bot-tail-offset",
                    "x-bot-total-bytes",
                    "x-bot-audio-sha256",
                    "x-bot-series-id",
                    "x-bot-series-index",
                    "x-bot-series-final",
                    "x-bot-call-wait-ms",
                    "x-bot-call-utterance",
                }:
                    raise ValueError
                if any(ord(c) < 32 or ord(c) == 127 for c in value):
                    raise ValueError
                headers[name] = value
        except Exception:
            if self.rejections:
                self.rejections.record(scope, "headers_invalid", self._origin)
            raise _HTTPError(400, "request_rejected") from None
        if headers.get("host") != self._host:
            if self.rejections:
                self.rejections.record(scope, "host_mismatch", self._origin)
            raise _HTTPError(403, "origin_rejected")
        if (write and headers.get("origin") != self._origin) or (
            "origin" in headers and headers["origin"] != self._origin
        ):
            if self.rejections:
                self.rejections.record(scope, "origin_mismatch", self._origin)
            raise _HTTPError(403, "origin_rejected")
        if (
            write
            and not audio
            and headers.get("content-type", "").lower()
            not in {"application/json", "application/json; charset=utf-8"}
        ):
            raise _HTTPError(415, "json_required")
        return headers

    async def _body(self, receive, headers, limit=None):
        limit = self._limit if limit is None else limit
        expected = headers.get("content-length")
        if expected is not None and (re.fullmatch(r"[0-9]{1,10}", expected) is None):
            raise _HTTPError(400, "request_rejected")
        if expected is not None and int(expected) > limit:
            raise _HTTPError(413, "body_too_large")
        chunks, size = [], 0
        while True:
            event = await receive()
            if event.get("type") == "http.disconnect":
                raise _Disconnected
            if event.get("type") != "http.request" or type(event.get("body", b"")) is not bytes:
                raise _HTTPError(400, "request_rejected")
            chunk = event.get("body", b"")
            size += len(chunk)
            if size > limit:
                raise _HTTPError(413, "body_too_large")
            chunks.append(chunk)
            if not event.get("more_body", False):
                break
        if expected is not None and size != int(expected):
            raise _HTTPError(400, "request_rejected")
        return b"".join(chunks)

    def _session(self, headers):
        values = []
        for part in headers.get("cookie", "").split(";"):
            key, separator, value = part.strip().partition("=")
            if key == COOKIE and separator:
                values.append(value)
        if len(values) != 1 or REFERENCE_HEADER not in headers:
            raise _HTTPError(401, "denied")
        return values[0], headers[REFERENCE_HEADER]

    def _audio_ack(self, job):
        ack = dict(audio_received=True, utterance_id=job.utterance_id, audio_state=job.state)
        if job.series_id:
            snapshot = job.series_snapshot
            ack.update(
                series_state=snapshot["state"],
                series_end_index=snapshot["series_end_index"],
                max_segments=snapshot["max_segments"],
                series_id=job.series_id,
                series_index=job.series_index,
                series_final=bool(job.series_final),
                reply_utterance_id=job.series_id,
            )
        return ack

    async def _handle(self, scope, receive):
        path, method = scope.get("path"), scope.get("method")
        if path in self._assets:
            if method != "GET":
                raise _HTTPError(405, "method_rejected")
            headers = self._headers(scope, False, allow_query=path == PATH)
            if await self._body(receive, headers):
                raise _HTTPError(400, "request_rejected")
            asset = self._assets[path]
            if path.endswith((".wasm", ".onnx", ".js", ".mjs")) and len(asset.body) >= 4096:
                extra = [(b"vary", b"accept-encoding")]
                if _accepts_gzip(headers.get("accept-encoding", "")):
                    return _Static(
                        await asset.compressed(), asset.content_type, asset.immutable
                    ), extra + [(b"content-encoding", b"gzip")]
                return asset, extra
            return asset, []
        series_path = path in {PATH + "audio-series", PATH + "audio-series-commit"}
        if series_path:
            path = PATH + ("audio" if path.endswith("/audio-series") else "audio-commit")
        routes = {
            PATH + "audio-series-control": "POST",
            PATH + "audio-resolution": "POST",
            PATH + "heartbeat": "POST",
            PATH + "playback": "POST",
            PATH + "exchange": "POST",
            PATH + "state": "GET",
            PATH + "utterances": "POST",
            PATH + "end": "POST",
            PATH + "audio": "POST",
            PATH + "audio-chunk": "POST",
            PATH + "audio-commit": "POST",
            PATH + "speech": "POST",
            PATH + "speech-audio": "POST",
            PATH + "segment-audio": "POST",
            PATH + "segment-playback": "POST",
            PATH + "metrics": "POST",
            PATH + "barge-in": "POST",
        }
        if path not in routes:
            raise _HTTPError(404, "not_found")
        if method != routes[path]:
            raise _HTTPError(405, "method_rejected")
        binary_commit = path == PATH + "audio-commit" and any(
            k.lower() == b"content-type" and v.decode("latin1") in AUDIO_MIMES
            for k, v in scope.get("headers", [])
        )
        is_audio = path in {PATH + "audio", PATH + "audio-chunk"} or binary_commit
        headers = self._headers(scope, method == "POST", is_audio)
        if is_audio:
            if not self._runtime.audio_available:
                raise AudioError("unavailable")
            mime = headers.get("content-type")
            utterance_id = headers.get("x-bot-utterance-id", "")
            if mime not in AUDIO_MIMES:
                raise _HTTPError(415, "audio_type_rejected")
            if re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", utterance_id) is None:
                raise _HTTPError(400, "invalid_fields")
            token, reference = self._session(headers)
            series = None
            if series_path:
                sid = headers.get("x-bot-series-id", "")
                index = headers.get("x-bot-series-index", "")
                final = headers.get("x-bot-series-final", "")
                if (
                    re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", sid) is None
                    or re.fullmatch(r"(?:[1-9]|1[0-9]|20)", index) is None
                    or final not in {"0", "1"}
                ):
                    raise _HTTPError(400, "invalid_fields")
                series = (sid, int(index), final == "1")
            body = await self._body(receive, headers, self._audio_limit)
            if path == PATH + "audio-chunk":
                seq, offset = (
                    headers.get("x-bot-chunk-seq", ""),
                    headers.get("x-bot-chunk-offset", ""),
                )
                if (
                    re.fullmatch(r"0|[1-9][0-9]{0,3}", seq) is None
                    or int(seq) >= 4096
                    or re.fullmatch(r"0|[1-9][0-9]{0,9}", offset) is None
                ):
                    raise _HTTPError(400, "invalid_fields")
                return await self._runtime.web_audio_chunk(
                    token, reference, utterance_id, int(seq), int(offset), body, mime
                ), []
            if binary_commit:
                offset = headers.get("x-bot-tail-offset", "")
                total = headers.get("x-bot-total-bytes", "")
                sha = headers.get("x-bot-audio-sha256", "")
                if (
                    re.fullmatch(r"0|[1-9][0-9]{0,9}", offset) is None
                    or re.fullmatch(r"[1-9][0-9]{0,9}", total) is None
                    or re.fullmatch(r"[0-9a-f]{64}", sha) is None
                ):
                    raise _HTTPError(400, "invalid_fields")
                job = await self._runtime.web_audio_commit(
                    token, reference, utterance_id, int(total), sha, mime, int(offset), body, series
                )
            else:
                job = await self._runtime.web_audio(
                    token, reference, utterance_id, body, mime, series
                )
            return self._audio_ack(job), []
        body = await self._body(receive, headers)
        if method == "GET":
            if body:
                raise _HTTPError(400, "request_rejected")
            token, reference = self._session(headers)
            wait = headers.get("x-bot-call-wait-ms")
            target = headers.get("x-bot-call-utterance")
            if wait is None and target is None:
                return dict(
                    (await self._runtime.web_state(token, reference)).to_dict(),
                    experience=self._experience,
                    capabilities=self._capabilities,
                ), []
            if (
                wait is None
                or target is None
                or re.fullmatch(r"0|[1-9][0-9]{0,3}", wait) is None
                or int(wait) > 8000
                or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", target) is None
            ):
                raise _HTTPError(400, "invalid_fields")
            return dict(
                (await self._runtime.web_state(token, reference, int(wait), target)).to_dict(),
                experience=self._experience,
                capabilities=self._capabilities,
            ), []
        try:
            data = json.loads(
                body.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_bad_constant
            )
            if type(data) is not dict:
                raise ValueError
            # Reject invalid Unicode even when encoded using JSON escapes.
            for key, value in data.items():
                key.encode("utf-8")
                if isinstance(value, str):
                    value.encode("utf-8")
        except Exception:
            raise _HTTPError(400, "invalid_json") from None
        if path == PATH + "exchange":
            if set(data) != {"invitation"} or type(data["invitation"]) is not str:
                raise _HTTPError(400, "invalid_fields")
            grant = await self._runtime.exchange_web_invitation(data["invitation"])
            cookie = (
                COOKIE
                + "="
                + grant.token
                + "; Path="
                + PATH
                + "; Secure; HttpOnly; SameSite=Strict; Max-Age="
                + str(grant.max_age)
            )
            return {
                "reference": grant.reference,
                "expires_at": grant.expires_at,
                "experience": self._experience,
                "capabilities": self._capabilities,
            }, [(b"set-cookie", cookie.encode("ascii"))]
        token, reference = self._session(headers)
        if path == PATH + "barge-in":
            from .barge_in import valid_request

            if not valid_request(data) or self._experience["barge_in"] != "auto":
                raise _HTTPError(400, "invalid_fields")
            return await self._runtime.web_barge_in(token, reference, data), []
        if path == PATH + "audio-commit":
            fields = {"utterance_id", "total_bytes", "sha256", "mime"}
            if series_path:
                fields |= {"series_id", "series_index", "series_final"}
            if set(data) != fields:
                raise _HTTPError(400, "invalid_fields")
            job = await self._runtime.web_audio_commit(
                token,
                reference,
                data["utterance_id"],
                data["total_bytes"],
                data["sha256"],
                data["mime"],
                series=(data["series_id"], data["series_index"], data["series_final"])
                if series_path
                else None,
            )
            return self._audio_ack(job), []
        if path == PATH + "audio-series-control":
            if set(data) != {"series_id", "action", "last_index", "reason"} or any(
                type(data[k]) is not str for k in ("series_id", "action", "reason")
            ):
                raise _HTTPError(400, "invalid_fields")
            return await self._runtime.web_audio_series_control(
                token,
                reference,
                data["series_id"],
                data["action"],
                data["last_index"],
                data["reason"],
            ), []
        if path == PATH + "audio-resolution":
            if (
                set(data) != {"utterance_id"}
                or type(data["utterance_id"]) is not str
                or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", data["utterance_id"]) is None
            ):
                raise _HTTPError(400, "invalid_fields")
            result = await self._runtime.web_audio_resolution(
                token, reference, data["utterance_id"]
            )
            return {"recorded": True, "audio": result.to_dict()}, []
        if path == PATH + "metrics":
            from .observability import (
                CLIENT_STAGES,
                ECHO_METRICS,
                echo_values_valid,
                CAPABILITY_METRICS,
                capability_values_valid,
                TRANSPORT_STAGES,
                DELIVERY_STAGES,
                VAD_FLAGS,
                VAD_COUNTS,
                VAD_TIMES,
                CLIP_REJECT_METRICS,
                PLAYBACK_METRICS,
                client_metric_valid,
            )

            if (
                set(data) != {"generation_id", "values"}
                or type(data["generation_id"]) is not str
                or re.fullmatch(r"[A-Za-z0-9_.:-]{1,134}", data["generation_id"]) is None
                or type(data["values"]) is not dict
                or not data["values"]
                or not set(data["values"])
                <= CLIENT_STAGES | TRANSPORT_STAGES | DELIVERY_STAGES | VAD_FLAGS | VAD_COUNTS
                or (
                    (
                        data["generation_id"].startswith("page:cap:")
                        or set(data["values"]) & CAPABILITY_METRICS.keys()
                    )
                    and not capability_values_valid(data["generation_id"], data["values"])
                )
                or (
                    (
                        data["generation_id"].startswith("page:echo:")
                        or set(data["values"]) & ECHO_METRICS.keys()
                    )
                    and not echo_values_valid(data["generation_id"], data["values"])
                )
                or (
                    bool(set(data["values"]) & PLAYBACK_METRICS.keys())
                    and (
                        not set(data["values"]) <= PLAYBACK_METRICS.keys()
                        or type(data["values"].get("client_playback_sentence")) is not int
                        or not 1 <= data["values"]["client_playback_sentence"] <= 512
                        or not data["generation_id"].startswith("reply:")
                    )
                )
                or (data["generation_id"] == "page" and not set(data["values"]) <= VAD_TIMES)
                or (
                    data["generation_id"].startswith("capture:")
                    and (
                        re.fullmatch(r"capture:[A-Za-z0-9_-]{1,80}", data["generation_id"]) is None
                        or not set(data["values"]) <= CLIP_REJECT_METRICS.keys()
                    )
                )
                or any(not client_metric_valid(k, v) for k, v in data["values"].items())
            ):
                raise _HTTPError(400, "invalid_fields")
            return await self._runtime.web_metrics(
                token, reference, data["generation_id"], data["values"]
            ), []
        if path in {PATH + "segment-audio", PATH + "segment-playback"}:
            receipt = path.endswith("segment-playback")
            fields = (
                {"generation_id", "seq", "revision", "state"}
                if receipt
                else {"generation_id", "seq"}
            )
            if (
                set(data) != fields
                or type(data["generation_id"]) is not str
                or re.fullmatch(r"[A-Za-z0-9_.:-]{1,134}", data["generation_id"]) is None
                or type(data["seq"]) is not int
                or not 1 <= data["seq"] <= 512
            ):
                raise _HTTPError(400, "invalid_fields")
            result = await self._runtime.web_segment(
                token, reference, data["generation_id"], data["seq"], data if receipt else None
            )
            from .speech import segment_audio_mime

            return (result if receipt else _Static(result, segment_audio_mime(result))), []
        if path in {PATH + "speech", PATH + "speech-audio"}:
            fields = (
                ({"generation_id"}, {"generation_id", "wait_ms"})
                if path == PATH + "speech"
                else ({"generation_id"},)
            )
            if (
                set(data) not in fields
                or type(data["generation_id"]) is not str
                or re.fullmatch(r"[A-Za-z0-9_.:-]{1,134}", data["generation_id"]) is None
            ):
                raise _HTTPError(400, "invalid_fields")
            if path == PATH + "speech":
                wait_ms = data.get("wait_ms", 0)
                if type(wait_ms) is not int or not 0 <= wait_ms <= 8000:
                    raise _HTTPError(400, "invalid_fields")
                return await self._runtime.web_speech(
                    token, reference, data["generation_id"], wait_ms
                ), []
            audio = await self._runtime.web_speech_audio(token, reference, data["generation_id"])
            return _Static(audio, b"audio/wav"), []
        if path == PATH + "heartbeat":
            if data:
                raise _HTTPError(400, "invalid_fields")
            limits = await self._runtime.web_heartbeat(token, reference)
            return {"limits": limits.to_dict() if limits else None}, []
        if path == PATH + "playback":
            if (
                set(data)
                not in (
                    {"generation_id", "revision", "state"},
                    {"generation_id", "revision", "state", "first_sound_ms"},
                )
                or type(data["generation_id"]) is not str
                or not 1 <= len(data["generation_id"]) <= 134
                or data["generation_id"].isspace()
                or type(data["revision"]) is not int
                or data["revision"] < 1
                or type(data["state"]) is not str
                or data["state"] not in {"unknown", "started", "ended", "stopped", "failed"}
            ):
                raise _HTTPError(400, "invalid_fields")
            turn = await self._runtime.web_playback(
                token,
                reference,
                data["generation_id"],
                data["revision"],
                data["state"],
                data.get("first_sound_ms"),
            )
            return {
                "recorded": True,
                "generation_id": turn.material_id,
                "revision": turn.playback_revision,
                "state": turn.playback_state,
            }, []
        if path == PATH + "utterances":
            if (
                set(data) != {"utterance_id", "text"}
                or type(data["utterance_id"]) is not str
                or not 1 <= len(data["utterance_id"]) <= 128
                or data["utterance_id"].isspace()
                or type(data["text"]) is not str
                or not data["text"]
                or data["text"].isspace()
            ):
                raise _HTTPError(400, "invalid_fields")
            turn = await self._runtime.web_submit(
                token, reference, data["utterance_id"], data["text"]
            )
            return {
                "accepted": True,
                "utterance_id": data["utterance_id"],
                "sequence": turn.sequence,
            }, []
        if data:
            raise _HTTPError(400, "invalid_fields")
        job = await self._runtime.web_end(token, reference)
        if isinstance(job, ClosingReceipt):
            return {
                "ended": job.state == "closed_no_text",
                "close_requested": True,
                "close_state": job.state,
                "close_requested_at": job.requested_at,
                "review_state": "not_started",
            }, []
        return {"ended": True, "review_state": job.state}, []

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "websocket":
            from .push import websocket

            return await websocket(self, scope, receive, send)
        if scope.get("type") != "http":
            raise RuntimeError("call_web.http_only") from None
        status, extra = 200, []
        try:
            result, extra = await self._handle(scope, receive)
        except _Disconnected:
            return
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except _HTTPError as error:
            status, result = error.status, {"error": error.code}
        except AudioError as error:
            status = {
                "invalid": 400,
                "chunk_missing": 409,
                "too_large": 413,
                "conflict": 409,
                "closed": 409,
                "unavailable": 503,
                "unconfirmed": 503,
            }[error.code]
            result = {"error": "audio_" + error.code}
        except AccessError as error:
            status = {
                "denied": 401,
                "busy": 409,
                "ended": 409,
                "conflict": 409,
                "invalid_request": 400,
                "unconfirmed": 503,
            }[error.code]
            result = {"error": error.code}
        except (RuntimeClosed, RuntimeOperationError):
            status, result = 503, {"error": "unconfirmed"}
        except Exception:
            status, result = 503, {"error": "unconfirmed"}
        output = (
            result.body
            if isinstance(result, _Static)
            else json.dumps(
                result, ensure_ascii=False, separators=(",", ":"), allow_nan=False
            ).encode("utf-8")
        )
        content_type = (
            result.content_type
            if isinstance(result, _Static)
            else b"application/json; charset=utf-8"
        )
        immutable = isinstance(result, _Static) and result.immutable
        response_headers = [
            (b"content-type", content_type),
            (b"content-length", str(len(output)).encode()),
            (
                b"cache-control",
                b"public, max-age=31536000, immutable" if immutable else b"no-store",
            ),
            (b"pragma", b"" if immutable else b"no-cache"),
            (b"x-content-type-options", b"nosniff"),
            (b"referrer-policy", b"no-referrer"),
            (
                b"content-security-policy",
                b"default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; worker-src 'self'; style-src 'self'; connect-src 'self'; media-src 'self' blob:; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
            ),
            (b"permissions-policy", b"microphone=(self), camera=(), geolocation=()"),
        ] + extra
        try:
            await send(
                {"type": "http.response.start", "status": status, "headers": response_headers}
            )
            await send({"type": "http.response.body", "body": output})
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            # Transport ACK loss must not undo accepted runtime work or expose
            # a server exception containing cookies, headers or user material.
            return
