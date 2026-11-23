"""Finite, offline audio work. No codec, SDK, paths, tasks or startup effects."""

import asyncio
from dataclasses import dataclass, field
import hashlib
import logging

AUDIO_MIMES = frozenset(
    {
        "audio/webm",
        "audio/webm;codecs=opus",
        "audio/webm; codecs=opus",
        "audio/ogg",
        "audio/ogg;codecs=opus",
        "audio/ogg; codecs=opus",
        "audio/wav",
        "audio/mp4",
    }
)


class AudioError(RuntimeError):
    def __init__(self, code="unconfirmed"):
        self.code = (
            code
            if code
            in {
                "invalid",
                "conflict",
                "closed",
                "unavailable",
                "unconfirmed",
                "chunk_missing",
                "too_large",
            }
            else "unconfirmed"
        )
        super().__init__("audio." + self.code)


class AudioNotApplied(Exception):
    """Trusted callback explicitly guarantees no transcription was applied."""


@dataclass(frozen=True, slots=True)
class NoTextResult:
    """Trusted successful empty transcription, not proof of absent speech."""


def audio_no_text(job):
    return job.state == "failed" and job.error_code == "empty_transcription"


@dataclass(frozen=True, slots=True)
class AudioResolution:
    decision: str
    decided_at: float

    def to_dict(self):
        return {"decision": self.decision, "decided_at": self.decided_at}


def audio_retained(job):
    return (
        job.state in {"failed", "uncertain", "cancelled"}
        and not audio_no_text(job)
        and type(job.resolution) is AudioResolution
        and job.resolution.decision == "retained_missing"
    )


def audio_settled(job):
    return job.state == "succeeded" or audio_no_text(job) or audio_retained(job)


def audio_review_prompt(prompt, jobs):
    """Deterministic supplement only for a newly frozen review; no Turn changes."""
    count = sum(audio_no_text(job) or audio_retained(job) for job in jobs)
    if not count:
        return prompt
    return (
        prompt + f"\n\n识别缺失说明：有{count}段未取得可识别文字，不能确定无人讲话；"
        "仅基于已保存文字整理，明确说明识别缺失，不补写或声称覆盖全部口头发言。"
    )


@dataclass(frozen=True, slots=True)
class AudioJob:
    call_id: str
    utterance_id: str = field(repr=False)
    ordinal: int
    mime: str
    sha256: str
    byte_count: int
    received_at: float
    state: str
    data: bytes | None = field(repr=False)
    user_sequence: int | None
    result_sha256: str | None
    error_code: str | None
    resolution: AudioResolution | None = field(default=None, repr=False)

    series_id: str | None = None
    series_index: int | None = None
    series_final: int | None = None
    series_limit: int | None = None
    series_end_index: int | None = None
    series_disposition: str | None = None
    series_snapshot: dict | None = field(default=None, repr=False)

    def public(self):
        return AudioStatus(
            self.utterance_id,
            self.ordinal,
            self.mime,
            self.sha256,
            self.byte_count,
            self.received_at,
            self.state,
            self.user_sequence,
            self.error_code,
            self.resolution,
            self.series_id,
            self.series_index,
            self.series_final,
        )


@dataclass(frozen=True, slots=True)
class AudioStatus:
    utterance_id: str = field(repr=False)
    ordinal: int
    mime: str
    sha256: str
    byte_count: int
    received_at: float
    state: str
    user_sequence: int | None
    error_code: str | None
    resolution: AudioResolution | None = None

    series_id: str | None = None
    series_index: int | None = None
    series_final: int | None = None

    def to_dict(self):
        return {
            "utterance_id": self.utterance_id,
            "ordinal": self.ordinal,
            "mime": self.mime,
            "sha256": self.sha256,
            "byte_count": self.byte_count,
            "received_at": self.received_at,
            "state": self.state,
            "user_sequence": self.user_sequence,
            "error_code": self.error_code,
            "resolution": self.resolution.to_dict() if self.resolution else None,
            "series_id": self.series_id,
            "series_index": self.series_index,
            "series_final": self.series_final,
        }


@dataclass(frozen=True, slots=True)
class CloseRequest:
    call_id: str
    requested_at: float
    prompt: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class ClosingReceipt:
    requested_at: float
    state: str


def closing_state(jobs):
    if any(j.state in {"failed", "uncertain", "cancelled"} and not audio_settled(j) for j in jobs):
        return "audio_blocked"
    return "waiting_for_audio" if any(not audio_settled(j) for j in jobs) else "awaiting_freeze"


def stt_diagnostic(*, stage, reason, outcome, sdk_calls, cleanup, error, http):
    """Fixed metadata only, emitted after the owned SDK/privacy/cleanup exit."""
    try:
        stage = (
            stage
            if type(stage) is str
            and stage in {"preflight", "prepare", "convert", "sdk", "response", "cleanup"}
            else "unknown"
        )
        reason = (
            reason
            if type(reason) is str
            and reason
            in {
                "preflight_failed",
                "prepare_failed",
                "convert_failed",
                "wav_failed",
                "sdk_entered",
                "sdk_error",
                "privacy_failed",
                "response_type",
                "stream_response",
                "missing_text",
                "text_type",
                "empty_text",
                "blank_text",
                "invalid_unicode",
                "cleanup_failed",
                "cancelled",
                "ok",
                "no_text",
            }
            else "unknown"
        )
        outcome = (
            outcome
            if type(outcome) is str
            and outcome in {"succeeded", "no_text", "not_applied", "uncertain", "cancelled"}
            else "unknown"
        )
        sdk_calls = sdk_calls if type(sdk_calls) is int and sdk_calls in {0, 1} else 0
        cleanup = (
            cleanup if type(cleanup) is str and cleanup in {"none", "ok", "failed"} else "unknown"
        )
        error = (
            error
            if type(error) is str and error in {"none", "timeout", "connection", "http", "other"}
            else "other"
        )
        http = (
            http
            if type(http) is str and http in {"none", "1xx", "2xx", "3xx", "4xx", "5xx"}
            else "none"
        )
        logging.getLogger("astrbot").info(
            "bot_call.stt stage=%s reason=%s outcome=%s sdk_calls=%s cleanup=%s error=%s http=%s",
            stage,
            reason,
            outcome,
            sdk_calls,
            cleanup,
            error,
            http,
        )
    except BaseException:
        pass


def _commit_diagnostic(reason, outcome):
    try:
        reason = (
            reason
            if type(reason) is str
            and reason
            in {
                "saved",
                "ack_confirmed",
                "readback_failed",
                "readback_mismatch",
                "no_text_saved",
                "no_text_ack_confirmed",
                "no_text_readback_failed",
                "no_text_readback_mismatch",
            }
            else "unknown"
        )
        outcome = (
            outcome
            if type(outcome) is str and outcome in {"succeeded", "no_text", "uncertain"}
            else "unknown"
        )
        logging.getLogger("astrbot").info(
            "bot_call.audio_commit stage=complete reason=%s outcome=%s", reason, outcome
        )
    except BaseException:
        pass


def _finish(store, job, state, error_code):
    try:
        store.finish_audio(job.call_id, job.utterance_id, state=state, error_code=error_code)
    except Exception:
        try:
            saved = store.get_audio_job(job.call_id, job.utterance_id)
            if saved.state == state and saved.error_code == error_code:
                return
        except Exception:
            pass
        raise AudioError() from None


def _complete(store, job, text):
    try:
        turn = store.complete_audio(job.call_id, job.utterance_id, text)
    except Exception:
        try:
            saved = store.get_audio_job(job.call_id, job.utterance_id)
            turn = store._find_turn(job.call_id, "user", job.utterance_id)
            if (
                saved.state == "succeeded"
                and saved.data is None
                and turn is not None
                and saved.user_sequence == turn.sequence
                and turn.source == "audio_transcription"
                and turn.body == text
                and saved.result_sha256 == hashlib.sha256(text.encode("utf-8")).hexdigest()
            ):
                _commit_diagnostic("ack_confirmed", "succeeded")
                return turn
            reason = "readback_mismatch"
        except Exception:
            reason = "readback_failed"
        try:
            _finish(store, job, "uncertain", "result_uncertain")
        except Exception:
            pass  # A failed compensation leaves the committed running claim intact.
        _commit_diagnostic(reason, "uncertain")
        raise AudioError() from None
    _commit_diagnostic("saved", "succeeded")
    return turn


def _complete_no_text(store, job):
    try:
        store.finish_audio(
            job.call_id, job.utterance_id, state="failed", error_code="empty_transcription"
        )
    except Exception:
        try:
            saved = store.get_audio_job(job.call_id, job.utterance_id)
            if (
                audio_no_text(saved)
                and saved.data == job.data
                and saved.sha256 == job.sha256
                and saved.ordinal == job.ordinal
                and saved.byte_count == job.byte_count
                and saved.mime == job.mime
                and saved.received_at == job.received_at
                and saved.user_sequence is None
                and saved.result_sha256 is None
                and store._find_turn(job.call_id, "user", job.utterance_id) is None
            ):
                _commit_diagnostic("no_text_ack_confirmed", "no_text")
                return
            reason = "no_text_readback_mismatch"
        except Exception:
            reason = "no_text_readback_failed"
        try:
            _finish(store, job, "uncertain", "result_uncertain")
        except Exception:
            pass  # Never rewrite a committed terminal result or reissue SDK work.
        _commit_diagnostic(reason, "uncertain")
        raise AudioError() from None
    _commit_diagnostic("no_text_saved", "no_text")


async def advance_audio(store, call_id, transcribe, on_text, *, max_steps):
    """Explicit finite ordered steps; callbacks await outside SQLite transactions."""
    try:
        for _ in range(max_steps):
            job, claimed = store.claim_next_audio(call_id)
            if not claimed:
                return
            try:
                text = await transcribe(store.get_binding(call_id), job)
            except asyncio.CancelledError:
                try:
                    _finish(store, job, "cancelled", "result_cancelled")
                except Exception:
                    pass
                raise asyncio.CancelledError() from None
            except AudioNotApplied:
                _finish(store, job, "failed", "not_applied")
            except Exception:
                _finish(store, job, "uncertain", "result_uncertain")
            else:
                if type(text) is NoTextResult:
                    _complete_no_text(store, job)
                    continue
                try:
                    valid = type(text) is str and bool(text) and not text.isspace()
                    if valid:
                        text.encode("utf-8")
                except Exception:
                    valid = False
                if valid:
                    turn = _complete(store, job, text)
                    on_text(turn)
                else:
                    _finish(store, job, "failed", "invalid_result")
    except asyncio.CancelledError:
        raise asyncio.CancelledError() from None
    except Exception:
        raise AudioError() from None
