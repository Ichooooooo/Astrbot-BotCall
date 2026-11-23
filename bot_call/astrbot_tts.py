"""Native TTS calls with borrowed output paths and owned conversion copies."""

from .observability import CURRENT_TIMING, TTS_STREAM_FALLBACK_REASONS, identifier, measure

import asyncio
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
import shutil
import stat
import tempfile
from time import monotonic
from types import SimpleNamespace

from .binding import CallBinding
from .speech import (
    SpeechUnavailable,
    MAX_SPEECH_BYTES,
    MAX_SPEECH_TEXT,
    check_wav,
    segment_audio_mime,
)
from .native_provider import NATIVE_CONTRACT, provider_info, selected_media


# 2026-11-18 proxy-unreachable reference: each round waited the full connect timeout.
# Unsupported proxies, incompatible formats and task rejection open the circuit immediately. Other
# pre-audio failures open it after two consecutive failures.
STREAM_FAILURE_LIMIT = 2


async def convert_segment_audio(source, *, output_format, output_path, executable="ffmpeg"):
    """Owned sentence -> broadly supported 32 kbps mono MP3; never logs audio."""
    if output_format != "mp3":
        raise SpeechUnavailable()
    process = await asyncio.create_subprocess_exec(
        executable,
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-threads",
        "1",
        "-filter_threads",
        "1",
        "-i",
        source,
        "-map",
        "0:a:0",
        "-vn",
        "-sn",
        "-dn",
        "-ac",
        "1",
        "-ar",
        "24000",
        "-c:a",
        "libmp3lame",
        "-b:a",
        "32k",
        "-threads",
        "1",
        output_path,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        await process.wait()
    except asyncio.CancelledError:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        await process.wait()
        raise
    if process.returncode:
        raise SpeechUnavailable()
    return output_path


@dataclass(frozen=True, slots=True)
class SpeechPolicy:
    binding: CallBinding = field(repr=False)
    provider_id: str = field(repr=False)
    model: str = field(repr=False)
    voice: str = field(
        default="", repr=False
    )  # Legacy display only, not a generic TTS requirement.
    contract: str = NATIVE_CONTRACT
    dedicated: bool = False


class AstrBotTTSAdapter:
    def __init__(self, context, policy, *, converter, privacy, segment_converter=None):
        self._loop = asyncio.get_running_loop()
        self.context, self.policy, self.converter, self.privacy = (
            context,
            policy,
            converter,
            privacy,
        )
        self.segment_converter = segment_converter or converter
        self._provider = None
        self._stream_failures = 0
        self._stream_circuit_open = False
        self._tasks, self._cleanup = set(), set()
        if (
            not callable(converter)
            or not callable(self.segment_converter)
            or policy.contract != NATIVE_CONTRACT
        ):
            raise SpeechUnavailable()

    @staticmethod
    async def selected(context, umo, privacy, provider_id=""):
        try:
            from .astrbot_compat import dedicated_tts_provider

            provider = (
                dedicated_tts_provider(context, provider_id)
                if provider_id
                else await selected_media(context, umo, "tts")
            )
            privacy.check()
            return provider
        except Exception:
            raise SpeechUnavailable() from None

    async def check(self):
        try:
            if (
                asyncio.get_running_loop() is not self._loop
                or type(self.policy) is not SpeechPolicy
            ):
                raise ValueError
            provider = await self.selected(
                self.context,
                self.policy.binding.unified_msg_origin,
                self.privacy,
                self.policy.provider_id if self.policy.dedicated else "",
            )
            identifier, model, _ = provider_info(provider, "get_audio")
            if (identifier, model) != (self.policy.provider_id, self.policy.model) or (
                self._provider is not None and self._provider is not provider
            ):
                raise ValueError
            self._provider = provider
        except Exception:
            raise SpeechUnavailable() from None

    async def synthesize(self, binding, turn, *, before_invoke, pcm=None):
        await self.check()
        if (
            binding != self.policy.binding
            or turn.call_id != binding.call_id
            or turn.role != "assistant"
            or type(turn.body) is not str
            or not turn.body.strip()
            or len(turn.body) > MAX_SPEECH_TEXT
            or len(turn.body.encode()) > MAX_SPEECH_TEXT * 4
        ):
            raise SpeechUnavailable() from None
        state = SimpleNamespace(cancelled=False, native_task=None)
        task = asyncio.create_task(
            self._perform(
                turn.body,
                state,
                before_invoke,
                segment=bool(getattr(turn, "segment_seq", None)),
                pcm=pcm,
            )
        )
        self._tasks.add(task)
        cancelled = False
        try:
            while True:
                try:
                    result = await asyncio.shield(task)
                    break
                except asyncio.CancelledError:
                    if not cancelled:
                        cancelled = state.cancelled = True
                        if state.native_task is not None and not state.native_task.done():
                            state.native_task.cancel()
                    if task.done():
                        break
                except Exception:
                    if not cancelled:
                        raise
                    break
            if cancelled:
                if not task.cancelled():
                    task.exception()
                raise asyncio.CancelledError() from None
            return result
        finally:
            self._tasks.discard(task)

    async def _perform(self, text, state, before_invoke, *, segment=False, pcm=None):
        directory = None
        attempted = False
        started = monotonic()
        try:
            directory = Path(await asyncio.to_thread(tempfile.mkdtemp, prefix="bot-call-tts-"))
            if state.cancelled:
                raise asyncio.CancelledError()

            async def native():
                nonlocal attempted
                await self.check()
                if not await before_invoke():
                    raise SpeechUnavailable()
                await self.check()
                if state.cancelled:
                    raise asyncio.CancelledError()
                with self.privacy.scope():
                    timing = CURRENT_TIMING.get()
                    if timing:
                        timing.provider_id = identifier(self.policy.provider_id)
                        timing.model = identifier(self.policy.model)
                        timing.selection = "dedicated" if self.policy.dedicated else "inherited"
                    attempted = True
                    with measure("tts_call"):
                        if pcm is not None:
                            from .astrbot_compat import tts_stream_settings
                            from .streaming_tts import (
                                FALLBACK_FORMAT_UNSUPPORTED,
                                FALLBACK_PROXY_UNSUPPORTED,
                                FALLBACK_TASK_FAILED,
                                StreamFailure,
                                stream_sentence,
                            )

                            settings = tts_stream_settings(self._provider)
                            if settings is not None:
                                if self._stream_circuit_open:
                                    reason = "circuit_open"
                                else:
                                    try:
                                        data = await stream_sentence(text, settings, pcm)
                                    except StreamFailure as error:
                                        if error.received:
                                            raise RuntimeError("speech.result_uncertain") from None
                                        reason = (
                                            error.reason
                                            if error.reason in TTS_STREAM_FALLBACK_REASONS
                                            else "other"
                                        )
                                        self._stream_failures += 1
                                        if (
                                            reason
                                            in (
                                                FALLBACK_PROXY_UNSUPPORTED,
                                                FALLBACK_TASK_FAILED,
                                                FALLBACK_FORMAT_UNSUPPORTED,
                                            )
                                            or self._stream_failures >= STREAM_FAILURE_LIMIT
                                        ):
                                            self._stream_circuit_open = True
                                        await self.check()
                                        if state.cancelled or not await before_invoke():
                                            raise asyncio.CancelledError()
                                        # Exactly one fallback, only before upstream audio.
                                        # The failed request may still have been billed.
                                    else:
                                        self._stream_failures = 0
                                        import wave

                                        source = directory / "stream.wav"
                                        with wave.open(str(source), "wb") as wav:
                                            wav.setnchannels(1)
                                            wav.setsampwidth(2)
                                            wav.setframerate(24000)
                                            wav.writeframes(data)
                                        return str(source)

                                logging.getLogger("bot_call.streaming_tts").warning(
                                    "bot_call.tts_stream_fallback reason=%s", reason
                                )
                                if timing:
                                    timing.add(
                                        "tts_stream_fallback",
                                        0,
                                        end=TTS_STREAM_FALLBACK_REASONS[reason],
                                    )
                        return await self._provider.get_audio(text)

            state.native_task = asyncio.create_task(native())
            path = await state.native_task
            if state.cancelled:
                raise asyncio.CancelledError()
            target_format = "mp3" if segment else "wav"
            source, output = directory / "input.source", directory / ("output." + target_format)

            def copy_borrowed():
                # Providers retain ownership of their path; never delete or rename it.
                if type(path) is not str:
                    raise ValueError
                borrowed = Path(path)
                if not borrowed.is_absolute() or borrowed.is_symlink():
                    raise ValueError
                with borrowed.open("rb") as stream:
                    info = os.fstat(stream.fileno())
                    if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= MAX_SPEECH_BYTES:
                        raise ValueError
                    data = stream.read(MAX_SPEECH_BYTES + 1)
                if not 0 < len(data) <= MAX_SPEECH_BYTES:
                    raise ValueError
                source.write_bytes(data)
                return data

            original = await asyncio.to_thread(copy_borrowed)
            if state.cancelled:
                raise asyncio.CancelledError()
            if segment:
                try:
                    if segment_audio_mime(original) == b"audio/mpeg":
                        return original
                except SpeechUnavailable:
                    pass
            with measure("tts_convert"):
                converted = await (self.segment_converter if segment else self.converter)(
                    str(source), output_format=target_format, output_path=str(output)
                )
            if state.cancelled:
                raise asyncio.CancelledError()
            if converted != str(output):
                raise ValueError

            def read_output():
                if output.is_symlink():
                    raise ValueError
                info = output.stat()
                if not stat.S_ISREG(info.st_mode) or not 24 <= info.st_size <= MAX_SPEECH_BYTES:
                    raise ValueError
                with output.open("rb") as stream:
                    data = stream.read(MAX_SPEECH_BYTES + 1)
                if segment:
                    if segment_audio_mime(data) != b"audio/mpeg":
                        raise SpeechUnavailable()
                    return data
                return check_wav(data)

            result = await asyncio.to_thread(read_output)
            if pcm is not None and (getattr(pcm, "index", 0) or getattr(pcm, "delivered", False)):
                await pcm(None)
            return result
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception as error:
            from .failures import record_failure

            record_failure(
                "tts",
                error,
                provider=self.policy.provider_id,
                attempts=int(attempted),
                elapsed_ms=(monotonic() - started) * 1000,
            )
            if not attempted:
                raise SpeechUnavailable() from None
            raise RuntimeError("speech.result_uncertain") from None
        finally:
            if directory is not None:
                try:
                    await asyncio.to_thread(shutil.rmtree, directory)
                except Exception:
                    self._cleanup.add(directory)
                    raise RuntimeError("speech.cleanup_unconfirmed") from None

    async def cleanup_pending(self):
        if asyncio.get_running_loop() is not self._loop:
            raise SpeechUnavailable()
        for directory in tuple(self._cleanup):
            await asyncio.to_thread(shutil.rmtree, directory)
            self._cleanup.discard(directory)
