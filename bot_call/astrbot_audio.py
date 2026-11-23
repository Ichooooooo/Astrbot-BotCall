"""Controlled, business-loop STT borrowing. No native Provider/core imports or startup I/O.

The trusted owner supplies the verified concrete Provider/native Provider classes and
STT converter. This is not production wiring or a persistent STT policy registry.
"""

from .observability import measure

import asyncio
from dataclasses import asdict, dataclass, field
import hashlib
import logging
import math
from pathlib import Path
import shutil
import stat
import tempfile

from .audio import AUDIO_MIMES, AudioJob, AudioNotApplied, NoTextResult, stt_diagnostic
from .binding import CallBinding
from .native_provider import NATIVE_CONTRACT, provider_info, selected_media


class STTUnavailable(RuntimeError):
    def __init__(self):
        super().__init__("bot_call.stt_unavailable")


class AudioResponseUncertain(RuntimeError):
    def __init__(self):
        super().__init__("audio_adapter.result_unconfirmed")


def _reject():
    raise AudioNotApplied("audio_adapter.not_applied") from None


def _text(value):
    if type(value) is not str or not value or value.isspace():
        raise ValueError
    value.encode("utf-8")


@dataclass(frozen=True, slots=True)
class AudioRequestPolicy:
    binding: CallBinding = field(repr=False)
    provider_id: str = field(repr=False)
    model: str = field(repr=False)
    max_input_bytes: int
    max_wav_bytes: int
    selection: str = field(default="frozen", repr=False)
    dedicated: bool = False

    def __post_init__(self):
        try:
            if type(self.binding) is not CallBinding:
                raise ValueError
            for key, value in asdict(self.binding).items():
                if key == "persona_id" and value is None:
                    continue
                _text(value)
            b = self.binding
            if (
                b.message_type != "FriendMessage"
                or b.unified_msg_origin != f"{b.platform_id}:{b.message_type}:{b.session_id}"
            ):
                raise ValueError
            _text(self.provider_id)
            _text(self.model)
            if self.selection not in {"native", "legacy", "frozen", NATIVE_CONTRACT}:
                raise ValueError
            if any(
                type(v) is not int or v <= 0 for v in (self.max_input_bytes, self.max_wav_bytes)
            ):
                raise ValueError
        except Exception:
            _reject()


class _Attempt:
    def __init__(self):
        self.cancelled = False
        self.native_task = None
        self.stage, self.reason = "preflight", "preflight_failed"
        self.native_calls, self.cleanup, self.error, self.http = 0, "none", "none", "none"

    def check(self):
        if self.cancelled:
            raise asyncio.CancelledError() from None


def _read_wav(path, limit):
    # Exclusive owner directory; reject converter symlinks/foreign output.
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= limit:
        raise ValueError
    with path.open("rb") as source:
        data = source.read(limit + 1)
    if not 0 < len(data) <= limit:
        raise ValueError
    return data


async def convert_stt_audio(source, *, output_format, output_path, executable="ffmpeg"):
    """Owned phone input -> 16 kHz mono PCM; no MIME/extension shortcuts.

    The adapter shields conversion until the process exits before deleting its
    directory. Diagnostics deliberately omit ffmpeg stderr and private paths.
    TTS retains AstrBot's original converter and sample rate.
    """
    if output_format != "wav":
        _reject()
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
        "16000",
        "-c:a",
        "pcm_s16le",
        output_path,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        await process.wait()
    except asyncio.CancelledError:
        # Also safe when invoked independently of the adapter's shielded owner.
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        await process.wait()
        raise
    if process.returncode:
        _reject()
    return output_path


async def warmup_audio(*, executable="ffmpeg"):
    """Warm executable/codec pages with 20 ms generated silence, no files or STT."""
    process = await asyncio.create_subprocess_exec(
        executable,
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-threads",
        "1",
        "-filter_threads",
        "1",
        "-f",
        "lavfi",
        "-i",
        "anullsrc=r=16000:cl=mono",
        "-t",
        "0.02",
        "-c:a",
        "pcm_s16le",
        "-f",
        "wav",
        "pipe:1",
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
        raise RuntimeError("audio.warmup_failed")


class AstrBotAudioAdapter:
    """Live references remain on the captured business loop, never in Store.

    Every attempt owns a shielded resource task. Cancellation waits for actual
    file/converter work; conversion is not cancelled before its owner settles
    and reaps ffmpeg. An active native Provider task is cancelled once,
    then awaited even if it suppresses cancellation. Borrowed pools stay open.
    """

    def __init__(self, context, policy, *, converter, privacy=None):
        self._loop = asyncio.get_running_loop()
        if (
            type(policy) is not AudioRequestPolicy
            or policy.selection != NATIVE_CONTRACT
            or not callable(converter)
        ):
            _reject()
        self.policy, self._context, self._converter, self._privacy = (
            policy,
            context,
            converter,
            privacy,
        )
        self._provider = None
        self._tasks, self._cleanup_pending = set(), set()

    @classmethod
    async def bind(cls, context, policy, *, converter, privacy=None):
        adapter = cls(context, policy, converter=converter, privacy=privacy)
        await adapter._resolve()
        return adapter

    def _check_loop(self):
        try:
            if asyncio.get_running_loop() is not self._loop:
                _reject()
        except Exception:
            _reject()

    async def _resolve(self):
        self._check_loop()
        try:
            from .astrbot_compat import dedicated_stt_provider

            provider = (
                dedicated_stt_provider(self._context, self.policy.provider_id)
                if self.policy.dedicated
                else await selected_media(
                    self._context, self.policy.binding.unified_msg_origin, "stt"
                )
            )
            identifier, model, _ = provider_info(provider, "get_text")
            if (identifier, model) != (self.policy.provider_id, self.policy.model) or (
                self._provider is not None and self._provider is not provider
            ):
                raise ValueError
            self._provider = provider
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            _reject()

    def _check_job(self, binding, job):
        try:
            if (
                type(binding) is not CallBinding
                or binding != self.policy.binding
                or type(job) is not AudioJob
                or job.call_id != binding.call_id
                or job.state != "running"
                or type(job.data) is not bytes
                or type(job.byte_count) is not int
                or job.byte_count != len(job.data)
                or not 0 < job.byte_count <= self.policy.max_input_bytes
                or job.mime not in AUDIO_MIMES
                or type(job.mime) is not str
                or type(job.ordinal) is not int
                or job.ordinal <= 0
                or type(job.received_at) not in (int, float)
                or not math.isfinite(job.received_at)
                or job.sha256 != hashlib.sha256(job.data).hexdigest()
                or job.user_sequence is not None
                or job.result_sha256 is not None
                or job.error_code is not None
            ):
                raise ValueError
            _text(job.utterance_id)
        except Exception:
            _reject()

    def _log_preflight(self):
        if self._privacy is None:
            _reject()
        try:
            self._privacy.check()
        except Exception:
            _reject()

    async def _perform(self, job, attempt):
        directory = None
        attempted = False
        cancelled = False
        try:
            attempt.check()
            attempt.stage, attempt.reason = "prepare", "prepare_failed"
            directory = Path(await asyncio.to_thread(tempfile.mkdtemp, prefix="bot-call-stt-"))
            attempt.check()
            source, output = directory / "input.source", directory / "output.wav"
            # Never suffix the input .wav just because its declared MIME says so:
            # native same-extension shortcuts must not bypass actual conversion.
            await asyncio.to_thread(source.write_bytes, job.data)
            attempt.check()
            attempt.stage, attempt.reason = "convert", "convert_failed"
            with measure("stt_convert"):
                converted = await self._converter(
                    str(source), output_format="wav", output_path=str(output)
                )
            attempt.check()
            if type(converted) is not str or converted != str(output):
                _reject()
            attempt.reason = "wav_failed"
            data = await asyncio.to_thread(_read_wav, output, self.policy.max_wav_bytes)
            attempt.check()

            async def send():
                nonlocal attempted
                attempt.stage, attempt.reason = "preflight", "preflight_failed"
                attempt.check()
                await self._resolve()
                self._log_preflight()
                with self._privacy.scope():
                    attempt.check()
                    attempted = True
                    attempt.native_calls = 1
                    attempt.stage, attempt.reason = "native", "native_entered"
                    try:
                        with measure("stt_call"):
                            return await self._provider.get_text(str(output))
                    except Exception as error:
                        from .failures import record_failure

                        record_failure("stt", error)
                        attempt.reason = "native_error"
                        attempt.error = "timeout" if type(error) is TimeoutError else "other"
                        raise

            attempt.native_task = asyncio.create_task(send())
            text = await attempt.native_task
            attempt.check()
            attempt.stage, attempt.reason = "response", "text_type"
            if type(text) is not str:
                raise ValueError
            attempt.reason = "invalid_unicode"
            text.encode("utf-8")
            if not text or text.isspace():
                attempt.reason = "no_text"
                return ""  # Materialize the trusted result only AFTER real owned cleanup.
            _text(text)
            attempt.reason = "ok"
            return text
        except asyncio.CancelledError:
            cancelled = True
            attempt.reason = "cancelled"
            raise asyncio.CancelledError() from None
        except Exception:
            if attempted:
                raise AudioResponseUncertain() from None
            _reject()
        finally:
            if directory is not None:
                try:
                    await asyncio.to_thread(shutil.rmtree, directory)
                    attempt.cleanup = "ok"
                except Exception:
                    attempt.cleanup = "failed"
                    if attempt.reason in {"ok", "no_text"}:
                        attempt.stage, attempt.reason = "cleanup", "cleanup_failed"
                    # Retain ownership for explicit retry; never report success.
                    self._cleanup_pending.add(directory)
                    if attempt.cancelled or cancelled:
                        raise asyncio.CancelledError() from None
                    if attempted:
                        raise AudioResponseUncertain() from None
                    _reject()

    async def cleanup_pending(self):
        """Trusted owner may retry an OS cleanup failure; paths stay private."""
        self._check_loop()

        # Same shielded ownership as an attempt, including caller cancellation.
        async def cleanup():
            for directory in tuple(self._cleanup_pending):
                try:
                    await asyncio.to_thread(shutil.rmtree, directory)
                    self._cleanup_pending.discard(directory)
                except Exception:
                    raise AudioResponseUncertain() from None

        await self._own(cleanup(), _Attempt())

    async def _own(self, coroutine, attempt):
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        cancelled = False
        try:
            while True:
                try:
                    value = await asyncio.shield(task)
                    break
                except asyncio.CancelledError:
                    # task.cancel() is not proof that native Provider I/O has stopped.
                    if not cancelled:
                        cancelled = attempt.cancelled = True
                        if attempt.native_task is not None and not attempt.native_task.done():
                            attempt.native_task.cancel()
                    if task.done():
                        break
                except Exception:
                    if not cancelled:
                        raise
                    break
            if cancelled:
                # Retrieve any late exception before propagating fixed cancellation.
                if not task.cancelled():
                    task.exception()
                raise asyncio.CancelledError() from None
            return value
        finally:
            self._tasks.discard(task)

    async def transcribe(self, binding: CallBinding, job: AudioJob) -> str | NoTextResult:
        attempt, outcome = _Attempt(), "not_applied"
        try:
            self._check_loop()  # Before inspecting material, Context or native Provider resources.
            self._check_job(binding, job)
            await self._resolve()
            self._log_preflight()
            result = await self._own(self._perform(job, attempt), attempt)
            attempt.check()
            if attempt.reason == "no_text":
                result = NoTextResult()
            outcome = "no_text" if type(result) is NoTextResult else "succeeded"
            return result
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise asyncio.CancelledError() from None
        except AudioNotApplied:
            _reject()
        except Exception:
            outcome = "uncertain"
            raise AudioResponseUncertain() from None
        finally:
            # _own waits for the real native Provider/file/cleanup and privacy scope to exit.
            # Observation faults must never change that result or cancellation.
            try:
                logging.getLogger("astrbot").info(
                    "bot_call.stt stage=%s reason=%s outcome=%s native_calls=%s cleanup=%s",
                    attempt.stage,
                    attempt.reason,
                    outcome,
                    attempt.native_calls,
                    attempt.cleanup,
                )
            except BaseException:
                pass
