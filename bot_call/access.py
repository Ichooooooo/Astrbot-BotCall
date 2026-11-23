"""Call-only credentials and public projections; no startup or storage effects."""

import base64
from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import hmac
import re
import secrets
from urllib.parse import urlsplit

from .audio import AudioStatus
from .limits import LimitsStatus


class AccessError(RuntimeError):
    def __init__(self, code="denied"):
        self.code = (
            code
            if code in {"denied", "busy", "ended", "conflict", "invalid_request", "unconfirmed"}
            else "denied"
        )
        super().__init__("call_access." + self.code)


@contextmanager
def access_errors():
    try:
        yield
    except AccessError as error:
        raise AccessError(error.code) from None
    except Exception:
        raise AccessError("unconfirmed") from None


def _raw_token(token):
    try:
        if type(token) is not str or re.fullmatch(r"[A-Za-z0-9_-]{43}", token) is None:
            raise ValueError
        raw = base64.urlsafe_b64decode(token + "=")
        if len(raw) != 32 or base64.urlsafe_b64encode(raw).decode().rstrip("=") != token:
            raise ValueError
        return raw
    except Exception:
        raise AccessError() from None


def token_digest(token, kind):
    if kind not in {"invitation", "session"}:
        raise AccessError() from None
    return hashlib.sha256(b"bot-call/" + kind.encode() + b"/v1\0" + _raw_token(token)).hexdigest()


def session_token(invitation, call_id):
    data = b"bot-call/derive-session/v1\0" + call_id.encode("utf-8")
    return (
        base64.urlsafe_b64encode(hmac.digest(_raw_token(invitation), data, "sha256"))
        .decode()
        .rstrip("=")
    )


def session_reference(digest):
    return hashlib.sha256(b"bot-call/public-reference/v1\0" + digest.encode("ascii")).hexdigest()


@dataclass(frozen=True, slots=True)
class InvitationSecret:
    value: str = field(repr=False)

    def __post_init__(self):
        _raw_token(self.value)

    @classmethod
    def create(cls):
        return cls(secrets.token_urlsafe(32))


@dataclass(frozen=True, slots=True)
class SecretURL:
    value: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class InvitationGrant:
    secret: InvitationSecret = field(repr=False)
    expires_at: float

    def link(self, page_url):
        try:
            parts = urlsplit(page_url)
            if (
                parts.scheme != "https"
                or not parts.hostname
                or parts.username is not None
                or parts.password is not None
                or parts.query
                or parts.fragment
                or any(ord(c) <= 32 or ord(c) >= 127 for c in page_url)
            ):
                raise ValueError
            return SecretURL(page_url + "#invitation=" + self.secret.value)
        except Exception:
            raise AccessError("invalid_request") from None


@dataclass(frozen=True, slots=True)
class SessionGrant:
    token: str = field(repr=False)
    reference: str
    expires_at: float
    max_age: int


@dataclass(frozen=True, slots=True)
class WebTurn:
    sequence: int
    role: str
    body: str = field(repr=False)
    playback_state: str
    material_id: str = field(repr=False)
    playback_revision: int = 0


@dataclass(frozen=True, slots=True)
class WebReply:
    utterance_id: str = field(repr=False)
    user_sequence: int
    generation_id: str | None = field(repr=False)
    state: str
    bot_sequence: int | None


@dataclass(frozen=True, slots=True)
class WebCallState:
    ended: bool
    review_state: str
    work_state: str | None
    turns: tuple[WebTurn, ...] = field(repr=False)
    review_body: str | None = field(repr=False)
    page_states: tuple[str, ...]
    context_state: str
    replies: tuple[WebReply, ...] = field(default=(), repr=False)
    review_policy_status: str | None = None
    audio: tuple[AudioStatus, ...] = field(default=(), repr=False)
    close_requested: bool = False
    close_state: str = "open"
    close_requested_at: float | None = None
    audio_available: bool = False
    limits: LimitsStatus | None = field(default=None, repr=False)
    speech_available: bool = False
    audio_missing_policy: bool = False
    segments: tuple = field(default=(), repr=False)
    failures: tuple = field(default=(), repr=False)
    bot_name: str = "Bot"
    call_id: str = field(default="", repr=False)
    series: tuple = field(default=(), repr=False)
    barge_in: tuple = field(default=(), repr=False)
    background_processing: bool = False
    tool_processing: bool = False
    close_detail: str | None = None

    def to_dict(self):
        # Stored succeeded is a completed send step, including legacy results.
        # No schema migration or old-page resubmission is needed for this view.
        submitted = bool(self.page_states) and all(s == "succeeded" for s in self.page_states)
        submission = (
            "submitted"
            if submitted
            else "uncertain"
            if "uncertain" in self.page_states
            else "not_applied"
            if "failed" in self.page_states
            else "submitting"
            if "running" in self.page_states
            else "pending"
            if self.page_states
            else "not_prepared"
        )
        return {
            "bot_name": self.bot_name,
            "ended": self.ended,
            "review_state": self.review_state,
            "work_state": self.work_state,
            "turns": [
                {
                    "sequence": t.sequence,
                    "role": t.role,
                    "body": t.body,
                    "playback_state": t.playback_state,
                    "material_id": t.material_id,
                    "playback_revision": t.playback_revision,
                }
                for t in self.turns
            ],
            "replies": [
                {
                    "utterance_id": r.utterance_id,
                    "user_sequence": r.user_sequence,
                    "generation_id": r.generation_id,
                    "state": r.state,
                    "bot_sequence": r.bot_sequence,
                }
                for r in self.replies
            ],
            "review_policy_status": self.review_policy_status,
            "audio_missing_policy": self.audio_missing_policy,
            "audio": [a.to_dict() for a in self.audio],
            "series": list(self.series),
            "barge_in": list(self.barge_in),
            "background_processing": self.background_processing,
            "tool_processing": self.tool_processing,
            "segments": list(self.segments),
            "failures": list(self.failures),
            "audio_available": self.audio_available,
            "speech_available": self.speech_available,
            "limits": self.limits.to_dict() if self.limits else None,
            "close_requested": self.close_requested,
            "close_detail": self.close_detail,
            "close_state": self.close_state,
            "close_requested_at": self.close_requested_at,
            "review_body": self.review_body,
            "page_states": ["submitted" if s == "succeeded" else s for s in self.page_states],
            "submission_status": submission,
            "delivery_status": "unverified",
            "context_state": self.context_state,
            "complete": self.review_state == "succeeded"
            and submitted
            and self.context_state == "succeeded",
        }
