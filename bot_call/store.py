"""SQLite storage for this project's own databases and synthetic/local callers."""

from bisect import bisect_right
from contextlib import contextmanager
from datetime import datetime, timezone
from dataclasses import asdict, astuple, dataclass, field, replace
import hashlib
import hmac
import math
import json
from pathlib import Path
import re
import sqlite3
from time import time

from .binding import CallBinding
from .retention import _AUDIO_SETTLED_SQL, audio_progress_trigger, prune_expired_audio
from .tool_query import QueryResult, canonical as query_json, digest as query_digest, policy_details
from .call_policy import CallPolicy
from .limits import RuntimeLimits, CallLimits, LimitsError, checked_time
from .audio import (
    AudioError,
    AudioJob,
    CloseRequest,
    ClosingReceipt,
    closing_state,
    audio_settled,
    audio_no_text,
    audio_retained,
    AudioResolution,
    audio_review_prompt,
    AUDIO_MIMES,
)
from .access import (
    AccessError,
    InvitationSecret,
    InvitationGrant,
    SessionGrant,
    access_errors,
    token_digest,
    session_token,
    session_reference,
)
from .review_policy import ReviewManifest


class MaterialConflict(ValueError):
    """An existing stable identifier was reused with different material."""


class StateConflict(ValueError):
    """A generation or observation cannot make the requested transition."""


class CallClosed(StateConflict):
    """No new transcript material can be accepted after the end request."""


class NotFound(LookupError):
    """A call, utterance, or generation does not exist."""


@dataclass(frozen=True, slots=True)
class Turn:
    call_id: str
    sequence: int
    role: str
    material_id: str
    source: str
    body: str = field(repr=False)
    created_at: float
    playback_state: str
    playback_revision: int


@dataclass(frozen=True, slots=True)
class Generation:
    call_id: str
    generation_id: str
    user_sequence: int
    source: str
    state: str
    bot_sequence: int | None


_BINDING_COLUMNS = (
    "call_id",
    "sender_id",
    "self_id",
    "platform_id",
    "platform_type",
    "message_type",
    "session_id",
    "unified_msg_origin",
    "conversation_id",
    "persona_id",
)
_APPLICATION_ID = 0x424F5443
_CALL_POLICY_SCHEMA = (
    """CREATE TABLE call_policies (
        call_id TEXT PRIMARY KEY NOT NULL REFERENCES calls(call_id),
        policy_json TEXT NOT NULL,
        fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64)
    )""",
    """CREATE TRIGGER immutable_call_policy BEFORE UPDATE ON call_policies
        BEGIN SELECT RAISE(ABORT, 'call policy is immutable'); END""",
    """CREATE TRIGGER retained_call_policy BEFORE DELETE ON call_policies
        BEGIN SELECT RAISE(ABORT, 'call policy is retained'); END""",
)
_QUERY_SCHEMA = (
    """CREATE TABLE query_policies (
        call_id TEXT PRIMARY KEY NOT NULL REFERENCES calls(call_id),
        policy_json TEXT NOT NULL, fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64))""",
    """CREATE TABLE query_attempts (
        call_id TEXT NOT NULL, generation_id TEXT NOT NULL, native_call_id TEXT NOT NULL,
        policy_sha256 TEXT NOT NULL, args_sha256 TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('claimed','entered','succeeded','not_executed','unknown')),
        entered INTEGER NOT NULL CHECK(entered IN (0,1)), body TEXT, facts_json TEXT,
        PRIMARY KEY(call_id,generation_id),
        CHECK((state!='claimed' OR entered=0) AND (state NOT IN ('entered','succeeded') OR entered=1)),
        FOREIGN KEY(call_id,generation_id) REFERENCES generations(call_id,generation_id),
        CHECK((state IN ('claimed','entered') AND body IS NULL AND facts_json IS NULL)
          OR (state NOT IN ('claimed','entered') AND body IS NOT NULL AND facts_json IS NOT NULL)))""",
    """CREATE TABLE review_query_notes (
        call_id TEXT PRIMARY KEY NOT NULL REFERENCES calls(call_id),
        manifest_json TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
        base_prompt_sha256 TEXT NOT NULL, prompt TEXT NOT NULL)""",
    """CREATE TRIGGER query_progress BEFORE UPDATE ON query_attempts
        WHEN OLD.state NOT IN ('claimed','entered') OR NEW.call_id!=OLD.call_id
          OR NEW.generation_id!=OLD.generation_id OR NEW.native_call_id!=OLD.native_call_id
          OR NEW.policy_sha256!=OLD.policy_sha256 OR NEW.args_sha256!=OLD.args_sha256
          OR (OLD.entered=1 AND NEW.entered!=1)
          OR (NEW.state IN ('claimed','entered') AND NOT (OLD.state='claimed' AND NEW.state='entered' AND NEW.entered=1))
        BEGIN SELECT RAISE(ABORT,'query record is immutable'); END""",
    """CREATE TRIGGER retained_query BEFORE DELETE ON query_attempts
        BEGIN SELECT RAISE(ABORT,'query record is retained'); END""",
    """CREATE TRIGGER review_query_barrier BEFORE INSERT ON reviews
        WHEN EXISTS(SELECT 1 FROM query_attempts WHERE call_id=NEW.call_id AND state IN ('claimed','entered'))
        BEGIN SELECT RAISE(ABORT,'query is unresolved'); END""",
)

_SCHEMA = (
    """CREATE TABLE calls (
        call_id TEXT PRIMARY KEY,
        sender_id TEXT NOT NULL, self_id TEXT NOT NULL,
        platform_id TEXT NOT NULL, platform_type TEXT NOT NULL,
        message_type TEXT NOT NULL, session_id TEXT NOT NULL,
        unified_msg_origin TEXT NOT NULL, conversation_id TEXT NOT NULL,
        persona_id TEXT, background TEXT, created_at REAL NOT NULL
    )""",
    """CREATE TABLE turns (
        call_id TEXT NOT NULL REFERENCES calls(call_id),
        sequence INTEGER NOT NULL CHECK(sequence > 0),
        role TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
        material_id TEXT NOT NULL, source TEXT NOT NULL, body TEXT NOT NULL,
        created_at REAL NOT NULL,
        playback_state TEXT NOT NULL,
        playback_revision INTEGER NOT NULL DEFAULT 0 CHECK(playback_revision >= 0),
        PRIMARY KEY(call_id, sequence),
        UNIQUE(call_id, role, material_id),
        CHECK(
            (role='user' AND playback_state='not_applicable' AND playback_revision=0)
            OR (role='assistant' AND playback_state IN
                ('unknown','started','ended','stopped','failed'))
        )
    )""",
    """CREATE TABLE generations (
        call_id TEXT NOT NULL, generation_id TEXT NOT NULL,
        user_sequence INTEGER NOT NULL, source TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN
            ('running','succeeded','failed','cancelled','empty')),
        bot_sequence INTEGER,
        PRIMARY KEY(call_id, generation_id),
        UNIQUE(call_id, user_sequence),
        FOREIGN KEY(call_id, user_sequence) REFERENCES turns(call_id, sequence),
        FOREIGN KEY(call_id, bot_sequence) REFERENCES turns(call_id, sequence),
        CHECK((state='succeeded' AND bot_sequence IS NOT NULL)
            OR (state!='succeeded' AND bot_sequence IS NULL))
    )""",
    """CREATE TRIGGER immutable_call BEFORE UPDATE ON calls
        BEGIN SELECT RAISE(ABORT, 'call binding and background are immutable'); END""",
    """CREATE TRIGGER immutable_turn BEFORE UPDATE OF
        call_id,sequence,role,material_id,source,body,created_at ON turns
        BEGIN SELECT RAISE(ABORT, 'turn material is immutable'); END""",
    """CREATE TRIGGER immutable_generation_target BEFORE UPDATE OF
        call_id,generation_id,user_sequence,source ON generations
        BEGIN SELECT RAISE(ABORT, 'generation target is immutable'); END""",
)


@dataclass(frozen=True, slots=True)
class ReviewJob:
    call_id: str
    state: str
    ended_at: float
    last_sequence: int
    input_sha256: str
    input_json: str = field(repr=False)
    prompt: str = field(repr=False)
    body: str | None = field(repr=False)
    error_code: str | None


_REVIEW_SCHEMA = (
    """CREATE TABLE reviews (
        call_id TEXT PRIMARY KEY REFERENCES calls(call_id),
        state TEXT NOT NULL CHECK(state IN
            ('pending','running','succeeded','failed','cancelled','uncertain')),
        ended_at REAL NOT NULL,
        last_sequence INTEGER NOT NULL CHECK(last_sequence >= 0),
        input_sha256 TEXT NOT NULL, input_json TEXT NOT NULL, prompt TEXT NOT NULL,
        body TEXT, error_code TEXT,
        CHECK((state='succeeded' AND body IS NOT NULL)
            OR (state!='succeeded' AND body IS NULL))
    )""",
    """CREATE TRIGGER frozen_review_input BEFORE UPDATE OF
        call_id,ended_at,last_sequence,input_sha256,input_json,prompt ON reviews
        BEGIN SELECT RAISE(ABORT, 'review input is immutable'); END""",
    """CREATE TRIGGER closed_transcript BEFORE INSERT ON turns
        WHEN EXISTS(SELECT 1 FROM reviews WHERE call_id=NEW.call_id)
        BEGIN SELECT RAISE(ABORT, 'call transcript is closed'); END""",
    """CREATE TRIGGER final_review_result BEFORE UPDATE OF state,body ON reviews
        WHEN OLD.state='succeeded'
        BEGIN SELECT RAISE(ABORT, 'review result is final'); END""",
)


@dataclass(frozen=True, slots=True)
class PublicationPage:
    call_id: str
    number: int
    start: int
    stop: int
    source_key: str
    state: str
    error_code: str | None
    fragment: str = field(repr=False)
    message: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class Publication:
    call_id: str
    review_sha256: str
    char_budget: int
    started_at: float
    ended_at: float
    page_count: int
    context_key: str
    context_state: str
    context_error: str | None
    body: str = field(repr=False)
    pages: tuple[PublicationPage, ...]

    @property
    def submission_complete(self) -> bool:
        """All send steps finished; succeeded does not attest user receipt."""
        return all(page.state == "succeeded" for page in self.pages)

    @property
    def delivery_confirmed(self) -> bool:
        """Legacy name for send-step completion only; never a receipt flag.

        Old CONFIRMED and new SUBMITTED pages share the unchanged schema.
        External state exposes submission and unverified delivery separately.
        """
        return self.submission_complete

    @property
    def context_available(self) -> bool:
        return self.context_state == "succeeded"

    @property
    def complete(self) -> bool:
        return self.submission_complete and self.context_available


_PUBLICATION_SCHEMA = (
    """CREATE TABLE publications (
        call_id TEXT PRIMARY KEY REFERENCES reviews(call_id),
        review_sha256 TEXT NOT NULL, char_budget INTEGER NOT NULL CHECK(char_budget > 0),
        started_at REAL NOT NULL, ended_at REAL NOT NULL,
        page_count INTEGER NOT NULL CHECK(page_count > 0),
        context_key TEXT NOT NULL UNIQUE,
        context_state TEXT NOT NULL CHECK(context_state IN
            ('pending','running','succeeded','failed','uncertain')),
        context_error TEXT,
        CHECK((context_state IN ('pending','running','succeeded') AND context_error IS NULL)
            OR (context_state='failed' AND context_error='not_applied')
            OR (context_state='uncertain' AND context_error='result_uncertain'))
    )""",
    """CREATE TABLE publication_pages (
        call_id TEXT NOT NULL REFERENCES publications(call_id),
        number INTEGER NOT NULL CHECK(number > 0),
        start INTEGER NOT NULL CHECK(start >= 0),
        stop INTEGER NOT NULL CHECK(stop > start),
        source_key TEXT NOT NULL UNIQUE,
        state TEXT NOT NULL CHECK(state IN
            ('pending','running','succeeded','failed','uncertain')),
        error_code TEXT,
        PRIMARY KEY(call_id, number),
        CHECK((state IN ('pending','running','succeeded') AND error_code IS NULL)
            OR (state='failed' AND error_code='not_applied')
            OR (state='uncertain' AND error_code='result_uncertain'))
    )""",
    """CREATE TRIGGER immutable_publication BEFORE UPDATE OF
        call_id,review_sha256,char_budget,started_at,ended_at,page_count,context_key
        ON publications
        BEGIN SELECT RAISE(ABORT, 'publication plan is immutable'); END""",
    """CREATE TRIGGER immutable_publication_page BEFORE UPDATE OF
        call_id,number,start,stop,source_key ON publication_pages
        BEGIN SELECT RAISE(ABORT, 'publication page is immutable'); END""",
    """CREATE TRIGGER publication_context_progress BEFORE UPDATE OF
        context_state,context_error ON publications
        WHEN NOT ((OLD.context_state='pending' AND NEW.context_state='running')
            OR (OLD.context_state='running' AND NEW.context_state IN
                ('succeeded','failed','uncertain')))
        BEGIN SELECT RAISE(ABORT, 'context step cannot change terminal state'); END""",
    """CREATE TRIGGER publication_page_progress BEFORE UPDATE OF state,error_code
        ON publication_pages
        WHEN NOT ((OLD.state='pending' AND NEW.state='running')
            OR (OLD.state='running' AND NEW.state IN ('succeeded','failed','uncertain')))
        BEGIN SELECT RAISE(ABORT, 'page step cannot change terminal state'); END""",
)


@dataclass(frozen=True, slots=True)
class ReviewWorkNode:
    call_id: str
    source_key: str
    level: int
    ordinal: int
    kind: str
    first_map: int
    stop_map: int
    child_keys: tuple[str, ...] = field(repr=False)
    request_json: str = field(repr=False)
    token_count: int
    state: str
    body: str | None = field(repr=False)
    result_sha256: str | None
    error_code: str | None


@dataclass(frozen=True, slots=True)
class ReviewWork:
    call_id: str
    plan_json: str = field(repr=False)
    fingerprint: str
    input_token_budget: int
    map_count: int
    blocked_code: str | None
    level: int
    kind: str
    frontier: tuple[ReviewWorkNode, ...] = field(repr=False)
    review: ReviewJob = field(repr=False)
    manifest: ReviewManifest | None = field(default=None, repr=False)

    @property
    def state(self) -> str:
        if self.review.state == "succeeded":
            return "succeeded"
        if not self.map_count:
            return "empty"
        if (
            self.blocked_code
            or self.review.state != "running"
            or any(n.state in {"failed", "cancelled", "uncertain"} for n in self.frontier)
        ):
            return "blocked"
        if any(n.state == "running" for n in self.frontier):
            return "running"
        return "pending"


_WORK_SCHEMA = (
    """CREATE TABLE review_work (
        call_id TEXT PRIMARY KEY REFERENCES reviews(call_id),
        plan_json TEXT NOT NULL, fingerprint TEXT NOT NULL,
        input_token_budget INTEGER NOT NULL CHECK(input_token_budget>0),
        map_count INTEGER NOT NULL CHECK(map_count>=0), blocked_code TEXT
    )""",
    """CREATE TABLE review_work_nodes (
        call_id TEXT NOT NULL REFERENCES review_work(call_id),
        source_key TEXT NOT NULL UNIQUE,
        level INTEGER NOT NULL CHECK(level>=0), ordinal INTEGER NOT NULL CHECK(ordinal>0),
        kind TEXT NOT NULL CHECK(kind IN ('map','merge','final')),
        first_map INTEGER NOT NULL CHECK(first_map>=0),
        stop_map INTEGER NOT NULL CHECK(stop_map>first_map),
        children_json TEXT NOT NULL, request_json TEXT NOT NULL,
        token_count INTEGER NOT NULL CHECK(token_count>=0),
        state TEXT NOT NULL CHECK(state IN
            ('pending','running','succeeded','failed','cancelled','uncertain')),
        body TEXT, result_sha256 TEXT, error_code TEXT,
        PRIMARY KEY(call_id,level,ordinal),
        CHECK((state='succeeded' AND body IS NOT NULL AND result_sha256 IS NOT NULL
                AND error_code IS NULL)
            OR (state!='succeeded' AND body IS NULL AND result_sha256 IS NULL)),
        CHECK((state IN ('pending','running','succeeded') AND error_code IS NULL)
            OR (state IN ('failed','cancelled','uncertain') AND error_code IS NOT NULL))
    )""",
    """CREATE TABLE review_work_stages (
        call_id TEXT NOT NULL REFERENCES review_work(call_id),
        level INTEGER NOT NULL CHECK(level>=0),
        kind TEXT NOT NULL CHECK(kind IN ('map','merge','final')),
        frontier_json TEXT NOT NULL, PRIMARY KEY(call_id,level)
    )""",
    """CREATE TRIGGER immutable_review_work BEFORE UPDATE OF
        call_id,plan_json,fingerprint,input_token_budget,map_count ON review_work
        BEGIN SELECT RAISE(ABORT, 'review work plan is immutable'); END""",
    """CREATE TRIGGER immutable_review_work_stage BEFORE UPDATE ON review_work_stages
        BEGIN SELECT RAISE(ABORT, 'review work stage is immutable'); END""",
    """CREATE TRIGGER immutable_review_work_node BEFORE UPDATE OF
        call_id,source_key,level,ordinal,kind,first_map,stop_map,children_json,
        request_json,token_count ON review_work_nodes
        BEGIN SELECT RAISE(ABORT, 'review work request is immutable'); END""",
    """CREATE TRIGGER review_work_node_progress BEFORE UPDATE OF
        state,body,result_sha256,error_code ON review_work_nodes
        WHEN NOT ((OLD.state='pending' AND NEW.state='running')
            OR (OLD.state='running' AND NEW.state IN
                ('succeeded','failed','cancelled','uncertain')))
        BEGIN SELECT RAISE(ABORT, 'review work outcome is terminal'); END""",
    """CREATE TRIGGER review_work_block_is_final BEFORE UPDATE OF blocked_code ON review_work
        WHEN OLD.blocked_code IS NOT NULL OR NEW.blocked_code IS NULL
        BEGIN SELECT RAISE(ABORT, 'review work block is final'); END""",
)


_MANIFEST_SCHEMA = (
    """CREATE TABLE review_policy_manifests (
        call_id TEXT PRIMARY KEY REFERENCES review_work(call_id),
        plan_fingerprint TEXT NOT NULL, manifest_json TEXT NOT NULL,
        fingerprint TEXT NOT NULL
    )""",
    """CREATE TRIGGER immutable_review_manifest BEFORE UPDATE ON review_policy_manifests
        BEGIN SELECT RAISE(ABORT, 'review policy manifest is immutable'); END""",
    """CREATE TRIGGER retained_review_manifest BEFORE DELETE ON review_policy_manifests
        BEGIN SELECT RAISE(ABORT, 'review policy manifest cannot be detached'); END""",
    """CREATE TRIGGER review_manifest_source BEFORE INSERT ON review_policy_manifests
        WHEN NOT EXISTS(SELECT 1 FROM review_work w JOIN reviews r USING(call_id)
            WHERE w.call_id=NEW.call_id AND w.fingerprint=NEW.plan_fingerprint AND r.state='pending')
        BEGIN SELECT RAISE(ABORT, 'manifest must be installed with a new review plan'); END""",
)


_ACCESS_SCHEMA = (
    """CREATE TABLE web_invitations (
        call_id TEXT PRIMARY KEY REFERENCES calls(call_id),
        invitation_sha256 TEXT NOT NULL UNIQUE,
        issued_at REAL NOT NULL, expires_at REAL NOT NULL CHECK(expires_at>issued_at),
        session_ttl INTEGER NOT NULL CHECK(session_ttl>0)
    )""",
    """CREATE TABLE web_sessions (
        call_id TEXT PRIMARY KEY REFERENCES web_invitations(call_id),
        session_sha256 TEXT NOT NULL UNIQUE,
        entered_at REAL NOT NULL, expires_at REAL NOT NULL CHECK(expires_at>entered_at)
    )""",
    """CREATE TRIGGER immutable_web_invitation BEFORE UPDATE ON web_invitations
        BEGIN SELECT RAISE(ABORT, 'invitation is immutable'); END""",
    """CREATE TRIGGER retained_web_invitation BEFORE DELETE ON web_invitations
        BEGIN SELECT RAISE(ABORT, 'invitation cannot be detached'); END""",
    """CREATE TRIGGER immutable_web_session BEFORE UPDATE ON web_sessions
        BEGIN SELECT RAISE(ABORT, 'session is immutable'); END""",
    """CREATE TRIGGER retained_web_session BEFORE DELETE ON web_sessions
        BEGIN SELECT RAISE(ABORT, 'session cannot be detached'); END""",
)


_LIMITS_SCHEMA = (
    """CREATE TABLE call_limits (
        call_id TEXT PRIMARY KEY REFERENCES web_sessions(call_id),
        entered_at REAL NOT NULL, max_seconds INTEGER NOT NULL CHECK(max_seconds>0),
        warning_seconds INTEGER NOT NULL CHECK(warning_seconds>0 AND warning_seconds<max_seconds),
        disconnect_seconds INTEGER NOT NULL CHECK(disconnect_seconds>0),
        last_activity_at REAL NOT NULL CHECK(last_activity_at>=entered_at),
        close_reason TEXT CHECK(close_reason IN ('manual','duration','disconnected')),
        closed_at REAL,
        CHECK((close_reason IS NULL AND closed_at IS NULL) OR (close_reason IS NOT NULL AND closed_at IS NOT NULL))
    )""",
    """CREATE TRIGGER immutable_call_limit_source BEFORE UPDATE ON call_limits
        WHEN NEW.call_id IS NOT OLD.call_id OR NEW.entered_at IS NOT OLD.entered_at
          OR NEW.max_seconds IS NOT OLD.max_seconds OR NEW.warning_seconds IS NOT OLD.warning_seconds
          OR NEW.disconnect_seconds IS NOT OLD.disconnect_seconds OR NEW.last_activity_at<OLD.last_activity_at
          OR (OLD.close_reason IS NOT NULL AND (NEW.close_reason IS NOT OLD.close_reason
            OR NEW.closed_at IS NOT OLD.closed_at OR NEW.last_activity_at IS NOT OLD.last_activity_at))
        BEGIN SELECT RAISE(ABORT,'call limit source is immutable'); END""",
    """CREATE TRIGGER retained_call_limits BEFORE DELETE ON call_limits
        BEGIN SELECT RAISE(ABORT,'call limits cannot be detached'); END""",
)


_AUDIO_SCHEMA = (
    """CREATE TABLE audio_jobs (
        call_id TEXT NOT NULL REFERENCES calls(call_id), utterance_id TEXT NOT NULL,
        ordinal INTEGER NOT NULL CHECK(ordinal>0), mime TEXT NOT NULL, sha256 TEXT NOT NULL,
        byte_count INTEGER NOT NULL CHECK(byte_count>0), received_at REAL NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending','running','succeeded','failed','cancelled','uncertain')),
        data BLOB, user_sequence INTEGER, result_sha256 TEXT, error_code TEXT,
        PRIMARY KEY(call_id,utterance_id), UNIQUE(call_id,ordinal),
        FOREIGN KEY(call_id,user_sequence) REFERENCES turns(call_id,sequence),
        CHECK((state='succeeded' AND data IS NULL AND user_sequence IS NOT NULL AND result_sha256 IS NOT NULL AND error_code IS NULL)
           OR (state!='succeeded' AND data IS NOT NULL AND user_sequence IS NULL AND result_sha256 IS NULL)),
        CHECK((state IN ('pending','running','succeeded') AND error_code IS NULL)
           OR (state IN ('failed','cancelled','uncertain') AND error_code IS NOT NULL))
    )""",
    """CREATE TABLE call_closures (
        call_id TEXT PRIMARY KEY REFERENCES calls(call_id), requested_at REAL NOT NULL, prompt TEXT NOT NULL
    )""",
    """CREATE TRIGGER immutable_audio_source BEFORE UPDATE OF
        call_id,utterance_id,ordinal,mime,sha256,byte_count,received_at ON audio_jobs
        BEGIN SELECT RAISE(ABORT,'audio source is immutable'); END""",
    audio_progress_trigger(),
    """CREATE TRIGGER retained_audio BEFORE DELETE ON audio_jobs
        BEGIN SELECT RAISE(ABORT,'audio material cannot be detached'); END""",
    """CREATE TRIGGER immutable_closure BEFORE UPDATE ON call_closures
        BEGIN SELECT RAISE(ABORT,'close request is immutable'); END""",
    """CREATE TRIGGER retained_closure BEFORE DELETE ON call_closures
        BEGIN SELECT RAISE(ABORT,'close request cannot be detached'); END""",
    """CREATE TRIGGER review_audio_barrier BEFORE INSERT ON reviews
        WHEN EXISTS(SELECT 1 FROM audio_jobs WHERE call_id=NEW.call_id AND state!='succeeded')
        BEGIN SELECT RAISE(ABORT,'received audio is unresolved'); END""",
    """CREATE TRIGGER closing_turn_barrier BEFORE INSERT ON turns
        WHEN EXISTS(SELECT 1 FROM call_closures WHERE call_id=NEW.call_id)
         AND NOT (NEW.role='user' AND NEW.source='audio_transcription' AND EXISTS(
           SELECT 1 FROM audio_jobs WHERE call_id=NEW.call_id AND utterance_id=NEW.material_id AND state='running'))
        BEGIN SELECT RAISE(ABORT,'call is closing'); END""",
)


_MISSING_SCHEMA = (
    """CREATE TABLE audio_missing_policies (
        call_id TEXT PRIMARY KEY REFERENCES calls(call_id),
        policy TEXT NOT NULL CHECK(policy='retained_missing_v1'), created_at REAL NOT NULL)""",
    """CREATE TABLE audio_resolutions (
        call_id TEXT NOT NULL, utterance_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
        mime TEXT NOT NULL, sha256 TEXT NOT NULL, byte_count INTEGER NOT NULL, received_at REAL NOT NULL,
        original_state TEXT NOT NULL CHECK(original_state IN ('failed','uncertain','cancelled')),
        original_error TEXT NOT NULL, decision TEXT NOT NULL CHECK(decision='retained_missing'), decided_at REAL NOT NULL,
        PRIMARY KEY(call_id,utterance_id), FOREIGN KEY(call_id,utterance_id) REFERENCES audio_jobs(call_id,utterance_id),
        FOREIGN KEY(call_id) REFERENCES audio_missing_policies(call_id))""",
    """CREATE TABLE review_audio_notes (
        call_id TEXT PRIMARY KEY REFERENCES audio_missing_policies(call_id),
        manifest_json TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
        notice TEXT NOT NULL, original_prompt_sha256 TEXT NOT NULL, prompt TEXT NOT NULL)""",
    """CREATE TRIGGER new_audio_missing_policy BEFORE INSERT ON audio_missing_policies
        WHEN NOT EXISTS(SELECT 1 FROM calls WHERE call_id=NEW.call_id AND created_at=NEW.created_at)
         OR EXISTS(SELECT 1 FROM audio_jobs WHERE call_id=NEW.call_id)
         OR EXISTS(SELECT 1 FROM turns WHERE call_id=NEW.call_id)
         OR EXISTS(SELECT 1 FROM call_closures WHERE call_id=NEW.call_id)
         OR EXISTS(SELECT 1 FROM reviews WHERE call_id=NEW.call_id)
        BEGIN SELECT RAISE(ABORT,'missing policy requires a new call'); END""",
    """CREATE TRIGGER checked_audio_resolution BEFORE INSERT ON audio_resolutions
        WHEN NOT EXISTS(SELECT 1 FROM audio_missing_policies WHERE call_id=NEW.call_id AND policy='retained_missing_v1')
         OR NOT EXISTS(SELECT 1 FROM audio_jobs a WHERE a.call_id=NEW.call_id AND a.utterance_id=NEW.utterance_id
           AND a.ordinal=NEW.ordinal AND a.mime=NEW.mime AND a.sha256=NEW.sha256 AND a.byte_count=NEW.byte_count
           AND a.received_at=NEW.received_at AND a.state=NEW.original_state AND a.error_code=NEW.original_error
           AND a.state IN ('failed','uncertain','cancelled') AND a.error_code!='empty_transcription'
           AND a.data IS NOT NULL AND a.user_sequence IS NULL AND a.result_sha256 IS NULL)
         OR EXISTS(SELECT 1 FROM reviews WHERE call_id=NEW.call_id)
        BEGIN SELECT RAISE(ABORT,'missing resolution source rejected'); END""",
    """CREATE TRIGGER new_review_audio_notes BEFORE INSERT ON review_audio_notes
        WHEN EXISTS(SELECT 1 FROM reviews WHERE call_id=NEW.call_id)
         OR EXISTS(SELECT 1 FROM publications WHERE call_id=NEW.call_id)
        BEGIN SELECT RAISE(ABORT,'review notes must precede frozen review'); END""",
)


def _publication_header(started_at: float, ended_at: float, number: int, total: int) -> str:
    start = datetime.fromtimestamp(started_at, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    end = datetime.fromtimestamp(ended_at, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return f"本次回顾 {number}/{total}\n通话时间（UTC）：{start} — {end}\n\n"


def _publication_ranges(body: str, budget: int, started_at: float, ended_at: float):
    # Slice indices preserve every character, including separators and whitespace.
    boundaries = [match.end() for match in re.finditer(r"\r?\n[ \t]*\r?\n", body)]
    total = 1
    while True:
        ranges, start = [], 0
        while start < len(body):
            capacity = budget - len(
                _publication_header(started_at, ended_at, len(ranges) + 1, total)
            )
            if capacity < 1:
                raise ValueError("Character budget must fit the time/number header and body")
            stop = min(start + capacity, len(body))
            if stop < len(body):
                index = bisect_right(boundaries, stop) - 1
                if index >= 0 and boundaries[index] > start:
                    stop = boundaries[index]
            ranges.append((start, stop))
            start = stop
        if len(ranges) == total:
            return tuple(ranges)
        # Accounting for the final total's digit width can require another pass.
        total = len(ranges)


def _identifier(value: str) -> None:
    if not isinstance(value, str) or not value or value.isspace():
        raise ValueError("A nonempty stable identifier/source is required")


_AGENT_SCHEMA = (
    """CREATE TABLE agent_runs (
        call_id TEXT NOT NULL, generation_id TEXT NOT NULL, policy_sha256 TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('claimed','running','completed','unknown')),
        reports_json TEXT NOT NULL,
        PRIMARY KEY(call_id,generation_id),
        FOREIGN KEY(call_id,generation_id) REFERENCES generations(call_id,generation_id))""",
    """CREATE TABLE review_agent_notes (
        call_id TEXT PRIMARY KEY REFERENCES calls(call_id), manifest_json TEXT NOT NULL,
        manifest_sha256 TEXT NOT NULL, base_prompt_sha256 TEXT NOT NULL, prompt TEXT NOT NULL)""",
    """CREATE TRIGGER immutable_agent_run BEFORE UPDATE ON agent_runs
        WHEN OLD.state IN ('completed','unknown') OR NEW.call_id!=OLD.call_id OR NEW.generation_id!=OLD.generation_id
        OR NEW.policy_sha256!=OLD.policy_sha256
        BEGIN SELECT RAISE(ABORT,'agent report is immutable'); END""",
    """CREATE TRIGGER immutable_agent_note BEFORE UPDATE ON review_agent_notes
        BEGIN SELECT RAISE(ABORT,'agent note is immutable'); END""",
)

_TIMING_SCHEMA = (
    """CREATE TABLE turn_timings (
    call_id TEXT NOT NULL REFERENCES calls(call_id), generation_id TEXT NOT NULL,
    stage TEXT NOT NULL, sequence INTEGER NOT NULL, start_ms REAL NOT NULL,
    elapsed_ms REAL NOT NULL CHECK(elapsed_ms>=0), provider_id TEXT NOT NULL,
    model TEXT NOT NULL, selection TEXT NOT NULL, tool TEXT NOT NULL,
    prompt_tokens INTEGER, completion_tokens INTEGER, reasoning_tokens INTEGER,
    recorded_at REAL NOT NULL,
    PRIMARY KEY(call_id,generation_id,stage,sequence))""",
)


from .segments import SegmentStore, SCHEMA as _SEGMENT_SCHEMA


from .series import (
    SeriesStore,
    COLUMNS as _SERIES_COLUMNS,
    SCHEMA as _SERIES_SCHEMA,
    LIMIT_SCHEMA as _SERIES_LIMIT_SCHEMA,
)


from .barge_in import BargeStore, SCHEMA as _BARGE_SCHEMA


class Store(BargeStore, SeriesStore, SegmentStore):
    """One connection per thread, explicit transactions and commit-before-return.

    Open only this module's independent database. No model/network work belongs
    inside a transaction. No automatic retry, scheduler, or transcript pruning.
    """

    def __init__(self, path: str | Path):
        self._active_agents = set()
        self._active_queries = set()  # Real reply callback exit, not an ACK/timeout.
        self._speech_calls = set()  # Only newly accepted calls opt into synthesis.
        self._speech_eligible = set()
        self._db = sqlite3.connect(path, timeout=10, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._closed = False
        try:
            self._db.execute("PRAGMA foreign_keys=ON")
            if (
                self._db.execute("PRAGMA application_id").fetchone()[0] == 0
                and self._db.execute("PRAGMA user_version").fetchone()[0] == 0
                and not self._db.execute(
                    "SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 1"
                ).fetchone()
            ):
                self._db.execute("PRAGMA auto_vacuum=INCREMENTAL")
            with self._transaction():
                app_id = self._db.execute("PRAGMA application_id").fetchone()[0]
                version = self._db.execute("PRAGMA user_version").fetchone()[0]
                names = self._db.execute(
                    "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
                ).fetchall()
                if not names and app_id == 0 and version == 0:
                    for statement in _SCHEMA:
                        self._db.execute(statement)
                    self._db.execute(f"PRAGMA application_id={_APPLICATION_ID}")
                    version = 1
                elif app_id != _APPLICATION_ID or version not in {
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19,
                    20,
                    21,
                    22,
                    23,
                    24,
                }:
                    raise ValueError("Not a supported Bot-Call database")
                if version == 1:
                    # Only our marked v1 database is extended, in the same transaction.
                    for statement in _REVIEW_SCHEMA:
                        self._db.execute(statement)
                    version = 2
                if version == 2:
                    for statement in _PUBLICATION_SCHEMA:
                        self._db.execute(statement)
                    version = 3
                if version == 3:
                    for statement in _WORK_SCHEMA:
                        self._db.execute(statement)
                    version = 4
                if version == 4:
                    for statement in _MANIFEST_SCHEMA:
                        self._db.execute(statement)
                    version = 5
                if version == 5:
                    for statement in _ACCESS_SCHEMA:
                        self._db.execute(statement)
                    version = 6
                if version == 6:
                    for statement in _AUDIO_SCHEMA:
                        self._db.execute(statement)
                    version = 7
                if version == 7:
                    for statement in _LIMITS_SCHEMA:
                        self._db.execute(statement)
                    version = 8
                if version == 8:
                    for statement in _CALL_POLICY_SCHEMA:
                        self._db.execute(statement)
                    version = 9
                if version == 9:
                    self._db.execute("""CREATE TABLE speech_jobs (
                        call_id TEXT NOT NULL, generation_id TEXT NOT NULL,
                        state TEXT NOT NULL CHECK(state IN ('pending','running','succeeded','failed','uncertain','cancelled')),
                        data BLOB,
                        PRIMARY KEY(call_id,generation_id),
                        FOREIGN KEY(call_id,generation_id) REFERENCES generations(call_id,generation_id))""")
                    version = 10
                if version == 10:
                    self._db.execute("DROP TRIGGER review_audio_barrier")
                    self._db.execute("""CREATE TRIGGER review_audio_barrier BEFORE INSERT ON reviews
                        WHEN EXISTS(SELECT 1 FROM audio_jobs WHERE call_id=NEW.call_id
                          AND NOT (state='succeeded' OR (state='failed' AND error_code='empty_transcription')))
                        BEGIN SELECT RAISE(ABORT,'received audio is unresolved'); END""")
                    version = 11
                if version == 11:
                    for statement in _MISSING_SCHEMA:
                        self._db.execute(statement)
                    for table in (
                        "audio_missing_policies",
                        "audio_resolutions",
                        "review_audio_notes",
                    ):
                        for operation in ("UPDATE", "DELETE"):
                            self._db.execute(
                                f"CREATE TRIGGER immutable_{table}_{operation.lower()} BEFORE {operation} ON {table} "
                                "BEGIN SELECT RAISE(ABORT,'missing record is immutable'); END"
                            )
                    self._db.execute("DROP TRIGGER review_audio_barrier")
                    self._db.execute(
                        "CREATE TRIGGER review_audio_barrier BEFORE INSERT ON reviews "
                        "WHEN EXISTS(SELECT 1 FROM audio_jobs WHERE call_id=NEW.call_id AND NOT "
                        + _AUDIO_SETTLED_SQL
                        + ") "
                        "BEGIN SELECT RAISE(ABORT,'received audio is unresolved'); END"
                    )
                    version = 12
                if version == 12:
                    for statement in _QUERY_SCHEMA:
                        self._db.execute(statement)
                    for table in ("query_policies", "review_query_notes"):
                        for operation in ("UPDATE", "DELETE"):
                            self._db.execute(
                                f"CREATE TRIGGER immutable_{table}_{operation.lower()} BEFORE {operation} ON {table} "
                                "BEGIN SELECT RAISE(ABORT,'query source is immutable'); END"
                            )
                    version = 13
                if version in (13, 14):
                    # Semantic gate only: new frozen writable capability/dynamic args.
                    # Existing 23 tables, SQL definitions and all rows stay unchanged.
                    version = 15
                if version == 15:
                    for statement in _AGENT_SCHEMA:
                        self._db.execute(statement)
                    version = 16
                if version == 16:
                    for statement in _TIMING_SCHEMA:
                        self._db.execute(statement)
                    version = 17
                if version == 17:
                    for name in ("history_before", "history_after", "history_summary"):
                        self._db.execute(f"ALTER TABLE turn_timings ADD COLUMN {name} INTEGER")
                    version = 18
                if version == 18:
                    self._db.execute(_SEGMENT_SCHEMA)
                    version = 19
                if version == 19:
                    for name, kind in _SERIES_COLUMNS.items():
                        self._db.execute(f"ALTER TABLE audio_jobs ADD COLUMN {name} {kind}")
                    for statement in _SERIES_SCHEMA:
                        self._db.execute(statement)
                    version = 20
                if version == 20:
                    for statement in _SERIES_LIMIT_SCHEMA:
                        self._db.execute(statement)
                    version = 21
                if version == 21:
                    columns = {r[1] for r in self._db.execute("PRAGMA table_info(speech_segments)")}
                    for name, kind in (
                        ("play_started_at", "REAL"),
                        ("playback_position_ms", "INTEGER"),
                        ("end_reason", "TEXT"),
                    ):
                        if name not in columns:
                            self._db.execute(
                                f"ALTER TABLE speech_segments ADD COLUMN {name} {kind}"
                            )
                    self._db.execute(_BARGE_SCHEMA)
                    self._db.execute("PRAGMA user_version=22")
                    version = 22
                if version == 22:
                    from .review_retry import SCHEMA as retry_schema

                    for statement in retry_schema:
                        self._db.execute(statement)
                    version = 23
                if version == 23:
                    self._db.execute("DROP TRIGGER audio_progress")
                    self._db.execute(audio_progress_trigger(retention=True))
                    self._db.execute("PRAGMA user_version=24")
        except BaseException:
            self.close()
            raise

    def prune_expired_audio(self, **options):
        return prune_expired_audio(self, **options)

    def record_timings(self, call_id, generation_id, rows):
        from .observability import TIMING_FIELDS, timing_row, ECHO_METRICS, echo_values_valid

        if type(rows) is not list or len(rows) > 4096:
            return
        if (type(generation_id) is str and generation_id.startswith("page:echo:")) or any(
            type(v) is dict and v.get("stage") in ECHO_METRICS for v in rows
        ):
            fields = {v.get("stage"): v.get("elapsed_ms") for v in rows if type(v) is dict}
            if (
                len(fields) != len(rows)
                or not echo_values_valid(generation_id, fields)
                or any(
                    v.get("sequence") != 0 or v.get("start_ms") != 0 or timing_row(v) is None
                    for v in rows
                )
            ):
                return
            if self._db.execute(
                "SELECT 1 FROM turn_timings WHERE call_id=? AND generation_id=? LIMIT 1",
                (call_id, generation_id),
            ).fetchone():
                return
        values = [r for v in rows if (r := timing_row(v)) is not None]
        with self._transaction():
            for row in values:
                self._db.execute(
                    "INSERT OR IGNORE INTO turn_timings (call_id,generation_id,"
                    + ",".join(TIMING_FIELDS)
                    + ",recorded_at) VALUES ("
                    + ",".join("?" for _ in range(len(TIMING_FIELDS) + 3))
                    + ")",
                    (call_id, generation_id, *(row[k] for k in TIMING_FIELDS), time()),
                )

    def service_failures(self, call_id):
        from .failures import failure_summary

        rows = self._db.execute(
            "SELECT stage,generation_id FROM turn_timings WHERE call_id=? AND stage LIKE 'error_%' ORDER BY recorded_at DESC LIMIT 100",
            (call_id,),
        ).fetchall()
        return failure_summary(rows, per_generation=True)

    def timing_snapshot(self):
        from .observability import timing_summary

        rows = self._db.execute("""SELECT * FROM turn_timings WHERE (call_id,generation_id) IN
            (SELECT call_id,generation_id FROM turn_timings
             WHERE generation_id NOT LIKE 'page:echo:%'
             GROUP BY call_id,generation_id
             ORDER BY MAX(recorded_at) DESC, MAX(rowid) DESC LIMIT 50)""").fetchall()
        rows = [dict(row) for row in rows]
        for row in rows:
            if row["generation_id"].startswith("capture:") and (
                row["stage"].startswith("stt_") or row["stage"].startswith("error_stt_")
            ):
                job = self._db.execute(
                    "SELECT series_id FROM audio_jobs WHERE call_id=? AND utterance_id=?",
                    (row["call_id"], row["generation_id"][8:]),
                ).fetchone()
                if (
                    job
                    and job[0]
                    and self._db.execute(
                        "SELECT 1 FROM generations WHERE call_id=? AND generation_id=?",
                        (row["call_id"], "reply:" + job[0]),
                    ).fetchone()
                ):
                    row["generation_id"] = "reply:" + job[0]
        return timing_summary(rows)

    @contextmanager
    def _transaction(self, *, read_only=False):
        self._db.execute("BEGIN" if read_only else "BEGIN IMMEDIATE")
        try:
            yield
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def close(self) -> None:
        if not self._closed:
            try:
                if self._db.in_transaction:
                    self._db.rollback()
            finally:
                self._db.close()
                self._closed = True

    def get_binding(self, call_id: str) -> CallBinding:
        row = self._db.execute("SELECT * FROM calls WHERE call_id=?", (call_id,)).fetchone()
        if row is None:
            raise NotFound("Call does not exist")
        return CallBinding(*(row[column] for column in _BINDING_COLUMNS))

    def issue_web_invitation(
        self, call_id: str, secret: InvitationSecret, *, session_ttl: int
    ) -> InvitationGrant:
        """Trusted-only. Keep the same random secret for any lost-ACK replay.

        One invitation per call; no renewal or anonymous issuance. First-entry
        validity is 15 minutes; the explicitly supplied session TTL starts on entry.
        """
        with access_errors():
            if (
                type(secret) is not InvitationSecret
                or type(session_ttl) is not int
                or not 0 < session_ttl <= 2147483647
            ):
                raise AccessError("invalid_request")
            digest = token_digest(secret.value, "invitation")
            with self._transaction():
                self.get_binding(call_id)
                row = self._db.execute(
                    "SELECT * FROM web_invitations WHERE call_id=?", (call_id,)
                ).fetchone()
                if row is not None:
                    if (
                        not hmac.compare_digest(row["invitation_sha256"], digest)
                        or row["session_ttl"] != session_ttl
                    ):
                        raise AccessError("conflict")
                    expires = row["expires_at"]
                else:
                    if self.is_call_closing(call_id):
                        raise AccessError("ended")
                    now = time()
                    if not math.isfinite(now):
                        raise AccessError("unconfirmed")
                    expires = now + 900
                    self._db.execute(
                        "INSERT INTO web_invitations VALUES (?,?,?,?,?)",
                        (call_id, digest, now, expires, session_ttl),
                    )
            return InvitationGrant(secret, expires)

    def exchange_web_invitation(self, token: str, *, _limits=None) -> SessionGrant:
        """Atomically enter one globally active web call; duplicate entry never renews."""
        with access_errors():
            digest = token_digest(token, "invitation")
            if _limits is not None and type(_limits) is not RuntimeLimits:
                raise AccessError("invalid_request")
            with self._transaction():
                now = time()
                if _limits is not None:
                    checked_time(now)
                row = self._db.execute(
                    "SELECT * FROM web_invitations WHERE invitation_sha256=?", (digest,)
                ).fetchone()
                if (
                    row is None
                    or not math.isfinite(now)
                    or not hmac.compare_digest(row["invitation_sha256"], digest)
                    or now >= row["expires_at"]
                    or now < row["issued_at"]
                ):
                    raise AccessError()
                call_id = row["call_id"]
                derived = session_token(token, call_id)
                session_sha = token_digest(derived, "session")
                existing = self._db.execute(
                    "SELECT * FROM web_sessions WHERE call_id=?", (call_id,)
                ).fetchone()
                if existing is not None:
                    if (
                        not hmac.compare_digest(existing["session_sha256"], session_sha)
                        or now >= existing["expires_at"]
                        or now < existing["entered_at"]
                    ):
                        raise AccessError()
                    expires = existing["expires_at"]
                else:
                    if self.is_call_closing(call_id):
                        raise AccessError("ended")
                    # A single global slot implies the per-user limit, including
                    # users coming from different platforms. Ended calls only
                    # keep read authority; expired authority releases occupancy.
                    active = self._db.execute(
                        "SELECT 1 FROM web_sessions s LEFT JOIN reviews r USING(call_id) "
                        "WHERE s.expires_at>? AND r.call_id IS NULL "
                        "AND NOT EXISTS(SELECT 1 FROM call_closures c WHERE c.call_id=s.call_id) LIMIT 1",
                        (now,),
                    ).fetchone()
                    if active is not None:
                        raise AccessError("busy")
                    expires = now + row["session_ttl"]
                    self._db.execute(
                        "INSERT INTO web_sessions VALUES (?,?,?,?)",
                        (call_id, session_sha, now, expires),
                    )
                    if _limits is not None:
                        self._db.execute(
                            "INSERT INTO call_limits VALUES (?,?,?,?,?,?,NULL,NULL)",
                            (
                                call_id,
                                now,
                                _limits.max_seconds,
                                _limits.warning_seconds,
                                _limits.disconnect_seconds,
                                now,
                            ),
                        )
            return SessionGrant(
                derived, session_reference(session_sha), expires, max(0, int(expires - now))
            )

    def authenticate_web_session(self, token: str, reference: str, *, submit: bool = False) -> str:
        """Target comes from the cookie only. The public reference only guards it."""
        with access_errors():
            digest = token_digest(token, "session")
            if type(reference) is not str or re.fullmatch(r"[0-9a-f]{64}", reference) is None:
                raise AccessError()
            row = self._db.execute(
                "SELECT * FROM web_sessions WHERE session_sha256=?", (digest,)
            ).fetchone()
            now = time()
            if (
                row is None
                or not math.isfinite(now)
                or not hmac.compare_digest(row["session_sha256"], digest)
                or not hmac.compare_digest(session_reference(row["session_sha256"]), reference)
                or now >= row["expires_at"]
                or now < row["entered_at"]
            ):
                raise AccessError()
            if submit and (
                self.is_call_closing(row["call_id"]) or self.limit_due(row["call_id"], now)
            ):
                raise AccessError("ended")
            return row["call_id"]

    def get_call_limits(self, call_id):
        row = self._db.execute("SELECT * FROM call_limits WHERE call_id=?", (call_id,)).fetchone()
        return CallLimits(**dict(row)) if row else None

    def speech_activity(self, call_id):
        return self._db.execute(
            "SELECT MAX(stamp) FROM ("
            " SELECT MAX(created_at) AS stamp FROM turns WHERE call_id=?"
            " UNION ALL SELECT MAX(received_at) FROM audio_jobs WHERE call_id=?"
            " UNION ALL SELECT MAX(MAX(created_at,coalesce(ready_at,0),coalesce(play_started_at,0)))"
            " FROM speech_segments WHERE call_id=?)",
            (call_id,) * 3,
        ).fetchone()[0]

    def speech_in_flight(self, call_id):
        return bool(
            self._db.execute(
                "SELECT EXISTS(SELECT 1 FROM audio_jobs WHERE call_id=? AND state IN ('pending','running'))"
                " OR EXISTS(SELECT 1 FROM generations WHERE call_id=? AND state IN ('pending','running'))"
                " OR EXISTS(SELECT 1 FROM speech_segments WHERE call_id=?"
                " AND (state IN ('pending','running','open') OR playback_state='started'))"
                " OR EXISTS(SELECT 1 FROM speech_jobs WHERE call_id=? AND state='running')"
                " OR EXISTS(SELECT 1 FROM turns t WHERE call_id=? AND playback_state='started'"
                " AND NOT EXISTS(SELECT 1 FROM speech_segments s WHERE s.call_id=t.call_id"
                " AND s.generation_id=t.material_id))",
                (call_id,) * 5,
            ).fetchone()[0]
        )

    def active_web_calls(self, now):
        return self._db.execute(
            "SELECT c.call_id,c.created_at,s.entered_at,x.requested_at,"
            " (EXISTS(SELECT 1 FROM audio_jobs a LEFT JOIN audio_resolutions ar"
            " USING(call_id,utterance_id) WHERE a.call_id=c.call_id AND a.state!='succeeded'"
            " AND a.error_code IS NOT 'empty_transcription' AND ar.call_id IS NULL)"
            " OR EXISTS(SELECT 1 FROM agent_runs a WHERE a.call_id=c.call_id"
            " AND a.state IN ('claimed','running'))) AS closing_work,"
            " (SELECT count(*) FROM generations g WHERE g.call_id=c.call_id AND g.state='succeeded') AS turn_count"
            " FROM web_invitations i JOIN calls c USING(call_id)"
            " LEFT JOIN web_sessions s USING(call_id) LEFT JOIN call_closures x USING(call_id)"
            " LEFT JOIN reviews r USING(call_id) WHERE r.call_id IS NULL"
            " AND (x.call_id IS NOT NULL OR coalesce(s.expires_at,i.expires_at)>?)"
            " ORDER BY x.call_id IS NOT NULL,s.call_id IS NULL,c.created_at DESC,c.rowid DESC",
            (now,),
        ).fetchall()

    def limit_due(self, call_id, now=None):
        saved = self.get_call_limits(call_id)
        return saved.due_reason(checked_time(time()) if now is None else now) if saved else None

    def limit_call_ids(self, *, call_ids=None):
        # None is the historical whole-store discovery; empty means NO calls.
        allowed = None if call_ids is None else frozenset(call_ids)
        if allowed is not None and not allowed:
            return ()
        return tuple(
            r[0]
            for r in self._db.execute("SELECT call_id FROM call_limits ORDER BY entered_at,call_id")
            if allowed is None or r[0] in allowed
        )

    def next_limit_delay(self, *, call_ids=None, idle_remaining=None):
        now = checked_time(time())
        delays = []
        discovered = (
            self.limit_call_ids() if call_ids is None else self.limit_call_ids(call_ids=call_ids)
        )
        for call_id in discovered:
            saved = self.get_call_limits(call_id)
            if saved.close_reason is None and not self.is_call_closing(call_id):
                delays.append(min(saved.remaining(now)))
                if idle_remaining is not None:
                    idle = idle_remaining(call_id, saved, now)
                    if idle is not None:
                        delays.append(idle)
        return max(0, min(delays)) if delays else None

    def heartbeat_web_session(self, token, reference, *, enabled):
        with access_errors():
            with self._transaction():
                call_id = self.authenticate_web_session(token, reference)
                saved = self.get_call_limits(call_id)
                if saved is None:
                    return None  # Never silently adopt an old unconfigured call.
                if not enabled:
                    raise AccessError("unconfirmed")
                now = checked_time(time())
                if self.is_call_closing(call_id) or saved.due_reason(now):
                    raise AccessError("ended")
                self._db.execute(
                    "UPDATE call_limits SET last_activity_at=? WHERE call_id=?", (now, call_id)
                )
            return self.get_call_limits(call_id).public(now)

    def _record_limit_close(self, call_id, when, reason=None):
        saved = self.get_call_limits(call_id)
        if saved is not None and saved.close_reason is None:
            reason = reason or saved.due_reason(when) or "manual"
            self._db.execute(
                "UPDATE call_limits SET close_reason=?,closed_at=? WHERE call_id=?",
                (reason, when, call_id),
            )

    def get_close_request(self, call_id: str) -> CloseRequest | None:
        row = self._db.execute("SELECT * FROM call_closures WHERE call_id=?", (call_id,)).fetchone()
        return CloseRequest(**dict(row)) if row is not None else None

    def is_call_closing(self, call_id: str) -> bool:
        return self._review_exists(call_id) or self.get_close_request(call_id) is not None

    def get_audio_job(self, call_id: str, utterance_id: str) -> AudioJob:
        row = self._db.execute(
            "SELECT * FROM audio_jobs WHERE call_id=? AND utterance_id=?", (call_id, utterance_id)
        ).fetchone()
        if row is None:
            raise NotFound("Audio does not exist")
        return self._audio_with_resolution(row)

    def _audio_with_resolution(self, row):
        job = AudioJob(**dict(row))
        row = self._db.execute(
            "SELECT * FROM audio_resolutions WHERE call_id=? AND utterance_id=?",
            (job.call_id, job.utterance_id),
        ).fetchone()
        if row is None:
            return job
        identity = (
            job.call_id,
            job.utterance_id,
            job.ordinal,
            job.mime,
            job.sha256,
            job.byte_count,
            job.received_at,
            job.state,
            job.error_code,
        )
        if (
            tuple(row)[:9] != identity
            or row["decision"] != "retained_missing"
            or not self.audio_missing_enabled(job.call_id)
            or job.state not in {"failed", "uncertain", "cancelled"}
            or audio_no_text(job)
            # A zero-byte BLOB marks expired raw audio. The immutable source
            # metadata above still authenticates the saved missing-audio decision.
            or (
                job.data != b""
                and (
                    type(job.data) is not bytes
                    or len(job.data) != job.byte_count
                    or hashlib.sha256(job.data).hexdigest() != job.sha256
                )
            )
            or job.user_sequence is not None
            or job.result_sha256 is not None
            or not math.isfinite(row["decided_at"])
        ):
            raise AudioError("conflict")
        return replace(job, resolution=AudioResolution(row["decision"], row["decided_at"]))

    def audio_missing_enabled(self, call_id):
        row = self._db.execute(
            "SELECT policy FROM audio_missing_policies WHERE call_id=?", (call_id,)
        ).fetchone()
        return row is not None and row["policy"] == "retained_missing_v1"

    def retain_audio_missing(self, call_id, utterance_id, *, _web_authority=None):
        """Trusted worker only, AFTER actual callback/cleanup ownership exits."""
        with self._transaction():
            if _web_authority is not None:
                if self.authenticate_web_session(*_web_authority) != call_id:
                    raise AccessError()
                if self.is_call_closing(call_id) or self.limit_due(call_id):
                    raise AccessError("ended")
            if not self.audio_missing_enabled(call_id):
                raise AudioError("conflict")
            return self._retain_audio_missing_row(call_id, utterance_id)

    def _retain_audio_missing_row(self, call_id, utterance_id):
        job = self.get_audio_job(call_id, utterance_id)
        if job.resolution is not None:
            return job
        if (
            job.state not in {"failed", "uncertain", "cancelled"}
            or audio_no_text(job)
            or self._review_exists(call_id)
            or type(job.data) is not bytes
            or len(job.data) != job.byte_count
            or hashlib.sha256(job.data).hexdigest() != job.sha256
            or job.user_sequence is not None
            or job.result_sha256 is not None
        ):
            raise AudioError("conflict")
        self._db.execute(
            "INSERT INTO audio_resolutions VALUES (?,?,?,?,?,?,?,?,?,'retained_missing',?)",
            (
                call_id,
                utterance_id,
                job.ordinal,
                job.mime,
                job.sha256,
                job.byte_count,
                job.received_at,
                job.state,
                job.error_code,
                time(),
            ),
        )
        return self.get_audio_job(call_id, utterance_id)

    def get_audio_jobs(self, call_id: str) -> tuple[AudioJob, ...]:
        return tuple(
            self._audio_with_resolution(row)
            for row in self._db.execute(
                "SELECT * FROM audio_jobs WHERE call_id=? ORDER BY ordinal", (call_id,)
            ).fetchall()
        )

    def audio_call_ids(self) -> tuple[str, ...]:
        return tuple(
            row[0]
            for row in self._db.execute("SELECT DISTINCT call_id FROM audio_jobs ORDER BY call_id")
        )

    def receive_audio(
        self,
        call_id: str,
        utterance_id: str,
        data: bytes,
        mime: str,
        *,
        _web_authority=None,
        series=None,
        series_limit=10,
    ) -> AudioJob:
        if (
            type(utterance_id) is not str
            or not 1 <= len(utterance_id) <= 128
            or utterance_id.isspace()
            or type(data) is not bytes
            or not data
            or type(mime) is not str
            or mime not in AUDIO_MIMES
        ):
            raise AudioError("invalid") from None
        digest = hashlib.sha256(data).hexdigest()
        with self._transaction():
            if (
                _web_authority is not None
                and self.authenticate_web_session(*_web_authority) != call_id
            ):
                raise AccessError()
            self.get_binding(call_id)
            row = self._db.execute(
                "SELECT * FROM audio_jobs WHERE call_id=? AND utterance_id=?",
                (call_id, utterance_id),
            ).fetchone()
            if row is not None:
                from .series import matches

                matches(self.get_audio_job(call_id, utterance_id), series)
                if (
                    row["sha256"] != digest
                    or row["mime"] != mime
                    or row["byte_count"] != len(data)
                    or (row["data"] not in (None, b"") and row["data"] != data)
                ):
                    raise AudioError("conflict")
            else:
                if self._find_turn(call_id, "user", utterance_id) is not None:
                    raise AudioError("conflict")
                if self.is_call_closing(call_id) or self.limit_due(call_id):
                    raise AudioError("closed")
                metadata = self.check_series_receive(call_id, utterance_id, series, series_limit)
                ordinal = self._db.execute(
                    "SELECT COALESCE(MAX(ordinal),0)+1 FROM audio_jobs WHERE call_id=?", (call_id,)
                ).fetchone()[0]
                self._db.execute(
                    "INSERT INTO audio_jobs VALUES (?,?,?,?,?,?,?,'pending',?,NULL,NULL,NULL,?,?,?,?,?,?)",
                    (
                        call_id,
                        utterance_id,
                        ordinal,
                        mime,
                        digest,
                        len(data),
                        time(),
                        data,
                        *metadata,
                    ),
                )
                if series and series[1] > 1 and series[2]:
                    root = self.get_audio_job(call_id, series[0])
                    if root.series_disposition == "open":
                        self._db.execute(
                            "UPDATE audio_jobs SET series_end_index=?,series_disposition='sealed' WHERE call_id=? AND utterance_id=?",
                            (series[1], call_id, series[0]),
                        )
        return self.get_audio_job(call_id, utterance_id)

    def claim_next_audio(self, call_id: str) -> tuple[AudioJob | None, bool]:
        with self._transaction():
            row = self._db.execute(
                "SELECT * FROM audio_jobs WHERE call_id=? AND NOT "
                + _AUDIO_SETTLED_SQL
                + " ORDER BY ordinal LIMIT 1",
                (call_id,),
            ).fetchone()
            if row is None:
                return None, False
            claimed = row["state"] == "pending"
            if claimed:
                if (
                    row["data"] is None
                    or hashlib.sha256(row["data"]).hexdigest() != row["sha256"]
                    or len(row["data"]) != row["byte_count"]
                ):
                    raise AudioError()
                self._db.execute(
                    "UPDATE audio_jobs SET state='running' WHERE call_id=? AND utterance_id=?",
                    (call_id, row["utterance_id"]),
                )
            job = self.get_audio_job(call_id, row["utterance_id"])
        return job, claimed

    def complete_audio(self, call_id: str, utterance_id: str, text: str) -> Turn:
        if type(text) is not str or not text or text.isspace():
            raise AudioError("invalid") from None
        try:
            result_sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
        except Exception:
            raise AudioError("invalid") from None
        with self._transaction():
            job = self.get_audio_job(call_id, utterance_id)
            if job.state == "succeeded":
                turn = self._find_turn(call_id, "user", utterance_id)
                if turn is None or turn.body != text or turn.sequence != job.user_sequence:
                    raise AudioError("conflict")
            elif job.state == "running":
                if (
                    self._review_exists(call_id)
                    or any(
                        not audio_settled(j)
                        for j in self.get_audio_jobs(call_id)
                        if j.ordinal < job.ordinal
                    )
                    or self._find_turn(call_id, "user", utterance_id) is not None
                ):
                    raise AudioError()
                turn = self._insert_turn(call_id, "user", utterance_id, "audio_transcription", text)
                self._db.execute(
                    "UPDATE audio_jobs SET state='succeeded',data=NULL,user_sequence=?,result_sha256=?,error_code=NULL WHERE call_id=? AND utterance_id=?",
                    (turn.sequence, result_sha, call_id, utterance_id),
                )
            else:
                raise AudioError()
        return turn

    def finish_audio(
        self, call_id: str, utterance_id: str, *, state: str, error_code: str
    ) -> AudioJob:
        if (state, error_code) not in {
            ("failed", "not_applied"),
            ("failed", "invalid_result"),
            ("failed", "empty_transcription"),
            ("cancelled", "result_cancelled"),
            ("uncertain", "result_uncertain"),
        }:
            raise AudioError("invalid") from None
        with self._transaction():
            job = self.get_audio_job(call_id, utterance_id)
            if job.state == "running":
                self._db.execute(
                    "UPDATE audio_jobs SET state=?,error_code=? WHERE call_id=? AND utterance_id=?",
                    (state, error_code, call_id, utterance_id),
                )
            elif (job.state, job.error_code) != (state, error_code):
                raise AudioError()
            job = self.get_audio_job(call_id, utterance_id)
        return job

    def get_background(self, call_id: str) -> str | None:
        row = self._db.execute(
            "SELECT background FROM calls WHERE call_id=?", (call_id,)
        ).fetchone()
        if row is None:
            raise NotFound("Call does not exist")
        return row["background"]

    def get_call_policy(self, call_id: str) -> CallPolicy | None:
        """Trusted read only; legacy calls stay without a policy, never inferred."""
        binding = self.get_binding(call_id)
        row = self._db.execute("SELECT * FROM call_policies WHERE call_id=?", (call_id,)).fetchone()
        if row is None:
            return None
        policy = CallPolicy(row["policy_json"], row["fingerprint"])
        if policy.binding != binding:
            raise MaterialConflict("Saved call policy binding mismatch")
        return policy

    def create_call(
        self,
        binding: CallBinding,
        *,
        background: str | None = None,
        policy: CallPolicy | None = None,
        retained_missing: bool = False,
        query_policy: str | None = None,
    ) -> CallBinding:
        """Persist a server-created binding, never update an existing target.

        CallBinding is a data type, not a web authentication capability.
        Arbitrary internal Python code is outside this trust boundary.
        """
        if query_policy is not None:
            policy_details(query_policy)
        if type(retained_missing) is not bool:
            raise TypeError("Missing policy requires a trusted boolean")
        if not isinstance(binding, CallBinding):
            raise TypeError("Expected a server-created CallBinding")
        if background is not None and not isinstance(background, str):
            raise TypeError("Background must be text or None")
        if policy is not None and (type(policy) is not CallPolicy or policy.binding != binding):
            raise MaterialConflict("Call policy binding mismatch")
        with self._transaction():
            row = self._db.execute(
                "SELECT call_id FROM calls WHERE call_id=?", (binding.call_id,)
            ).fetchone()
            if row is not None:
                if (
                    self.get_binding(binding.call_id) != binding
                    or self.get_background(binding.call_id) != background
                ):
                    raise MaterialConflict(
                        "Call identifier already has a different binding/background"
                    )
                if self.get_query_policy(binding.call_id) != query_policy:
                    raise MaterialConflict("Query policy cannot be attached to an old call")
                if self.get_call_policy(binding.call_id) != policy:
                    raise MaterialConflict(
                        "Call identifier already has a different or missing policy"
                    )
            else:
                created_at = time()
                self._db.execute(
                    "INSERT INTO calls VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (*astuple(binding), background, created_at),
                )
                if query_policy is not None:
                    self._db.execute(
                        "INSERT INTO query_policies VALUES (?,?,?)",
                        (binding.call_id, query_policy, query_digest(query_policy)),
                    )
                if retained_missing:
                    self._db.execute(
                        "INSERT INTO audio_missing_policies VALUES (?,'retained_missing_v1',?)",
                        (binding.call_id, created_at),
                    )
                if policy is not None:
                    self._db.execute(
                        "INSERT INTO call_policies VALUES (?,?,?)",
                        (binding.call_id, policy.canonical_json, policy.fingerprint),
                    )
        return self.get_binding(binding.call_id)

    def get_query_policy(self, call_id):
        row = self._db.execute(
            "SELECT * FROM query_policies WHERE call_id=?", (call_id,)
        ).fetchone()
        if row is None:
            return None
        if query_digest(row["policy_json"]) != row["fingerprint"]:
            raise MaterialConflict("query.policy_mismatch")
        return row["policy_json"]

    def query_record(self, call_id, generation_id):
        row = self._db.execute(
            "SELECT * FROM query_attempts WHERE call_id=? AND generation_id=?",
            (call_id, generation_id),
        ).fetchone()
        return dict(row) if row is not None else None

    def claim_query(self, *args, **kwargs):
        raise StateConflict("legacy.read_only")

    def enter_query(self, *args, **kwargs):
        raise StateConflict("legacy.read_only")

    def finish_query(self, *args, **kwargs):
        raise StateConflict("legacy.read_only")

    def recover_queries(self):
        # Historical rows, including unresolved/unknown, are read-only.
        return None

    def query_scope_exit(self, call_id, generation_id):
        self._active_queries.discard((call_id, generation_id))

    def _query_note_prompt(self, call_id, base, *, freeze=False):
        note = self._db.execute(
            "SELECT * FROM review_query_notes WHERE call_id=?", (call_id,)
        ).fetchone()
        if note is not None:
            if (
                query_digest(note["manifest_json"]) != note["manifest_sha256"]
                or query_digest(base) != note["base_prompt_sha256"]
            ):
                raise MaterialConflict("query.review_mismatch")
            return note["prompt"]
        return base

    def agent_record(self, call_id, generation_id):
        row = self._db.execute(
            "SELECT * FROM agent_runs WHERE call_id=? AND generation_id=?", (call_id, generation_id)
        ).fetchone()
        return dict(row) if row is not None else None

    def claim_agent(self, call_id, generation_id):
        with self._transaction():
            policy = self.get_call_policy(call_id)
            generation = self.get_generation(call_id, generation_id)
            if (
                policy is None
                or not policy.is_agent
                or self.is_call_closing(call_id)
                or generation.state != "running"
            ):
                raise StateConflict("agent.unavailable")
            if self.agent_record(call_id, generation_id) is not None:
                return False
            self._db.execute(
                "INSERT INTO agent_runs VALUES (?,?,?,'claimed','[]')",
                (call_id, generation_id, policy.fingerprint),
            )
        return True

    def enter_agent(self, call_id, generation_id):
        with self._transaction():
            row = self.agent_record(call_id, generation_id)
            if row is None or row["state"] != "claimed" or self.is_call_closing(call_id):
                raise StateConflict("agent.unavailable")
            self._db.execute(
                "UPDATE agent_runs SET state='running' WHERE call_id=? AND generation_id=?",
                (call_id, generation_id),
            )

    def report_agent(self, call_id, generation_id, value):
        report = json.loads(value)
        allowed = {
            "agent_begin": {"kind"},
            "tool_start": {"kind", "sequence", "tool", "arguments_sha256", "background"},
            "framework_report": {
                "kind",
                "sequence",
                "tool",
                "text",
                "non_text",
                "is_error",
                "no_return",
                "background_submitted",
            },
        }
        if (
            type(report) is not dict
            or report.get("kind") not in allowed
            or set(report) != allowed[report["kind"]]
            or query_json(report) != value
        ):
            raise ValueError("agent.invalid_report")
        if "tool" in report and type(report["tool"]) is not str:
            raise ValueError("agent.invalid_report")
        if "text" in report and (
            type(report["text"]) is not list or any(type(t) is not str for t in report["text"])
        ):
            raise ValueError("agent.invalid_report")
        with self._transaction():
            row = self.agent_record(call_id, generation_id)
            if row is None or row["state"] != "running":
                raise StateConflict("agent.scope_ended")
            reports = json.loads(row["reports_json"])
            reports.append(report)
            self._db.execute(
                "UPDATE agent_runs SET reports_json=? WHERE call_id=? AND generation_id=?",
                (query_json(reports), call_id, generation_id),
            )

    def finish_agent(self, call_id, generation_id, state):
        if state not in ("completed", "unknown"):
            raise ValueError("agent.invalid_state")
        with self._transaction():
            row = self.agent_record(call_id, generation_id)
            if row is None:
                raise StateConflict("agent.unclaimed")
            if row["state"] in ("completed", "unknown"):
                if row["state"] != state:
                    raise MaterialConflict("agent.result_mismatch")
            else:
                self._db.execute(
                    "UPDATE agent_runs SET state=? WHERE call_id=? AND generation_id=?",
                    (state, call_id, generation_id),
                )
        return self.agent_record(call_id, generation_id)

    def recover_agents(self):
        with self._transaction():
            self._db.execute(
                "UPDATE agent_runs SET state='unknown' WHERE state IN ('claimed','running')"
            )

    def agent_scope_exit(self, call_id, generation_id):
        row = self.agent_record(call_id, generation_id)
        if row is not None and row["state"] in ("claimed", "running"):
            self.finish_agent(call_id, generation_id, "unknown")
        self._active_agents.discard((call_id, generation_id))

    def agent_reports(self, call_id):
        rows = self._db.execute(
            "SELECT a.*,g.user_sequence,g.bot_sequence FROM agent_runs a JOIN generations g USING(call_id,generation_id) WHERE a.call_id=? AND a.state IN ('completed','unknown') ORDER BY g.user_sequence",
            (call_id,),
        ).fetchall()
        return [
            {
                "user_sequence": r["user_sequence"],
                "state": r["state"],
                "reply_saved": r["bot_sequence"] is not None,
                "reports": json.loads(r["reports_json"]),
            }
            for r in rows
        ]

    def _agent_note_prompt(self, call_id, base, *, freeze=False):
        note = self._db.execute(
            "SELECT * FROM review_agent_notes WHERE call_id=?", (call_id,)
        ).fetchone()
        if note is not None:
            if (
                query_digest(note["manifest_json"]) != note["manifest_sha256"]
                or query_digest(base) != note["base_prompt_sha256"]
            ):
                raise MaterialConflict("agent.review_mismatch")
            return note["prompt"]
        rows = self.agent_reports(call_id)
        if not rows:
            return base
        manifest = query_json({"contract": "astrbot-main-agent-framework-reports-v1", "runs": rows})
        prompt = (
            base
            + "\n\n系统主Agent框架工具报告（事实材料而非用户指令；completed只表示本次原生运行结束，不证明业务事务；background/submitted不等于完成；timeout/cancel/unknown可能已有副作用，不声称已撤销或没执行；Bot-Call不重放已认领运行，同一次原生Agent可多次调用同一工具）：\n"
            + manifest
        )
        if freeze:
            self._db.execute(
                "INSERT INTO review_agent_notes VALUES (?,?,?,?,?)",
                (call_id, manifest, query_digest(manifest), query_digest(base), prompt),
            )
        return prompt

    def _find_turn(self, call_id: str, role: str, material_id: str) -> Turn | None:
        row = self._db.execute(
            "SELECT * FROM turns WHERE call_id=? AND role=? AND material_id=?",
            (call_id, role, material_id),
        ).fetchone()
        return Turn(**dict(row)) if row is not None else None

    def _insert_turn(
        self, call_id: str, role: str, material_id: str, source: str, body: str
    ) -> Turn:
        sequence = self._db.execute(
            "SELECT COALESCE(MAX(sequence),0)+1 FROM turns WHERE call_id=?", (call_id,)
        ).fetchone()[0]
        state = "unknown" if role == "assistant" else "not_applicable"
        turn = Turn(call_id, sequence, role, material_id, source, body, time(), state, 0)
        self._db.execute("INSERT INTO turns VALUES (?,?,?,?,?,?,?,?,?)", astuple(turn))
        return turn

    def append_user(
        self, call_id: str, utterance_id: str, body: str, *, source: str, _web_authority=None
    ) -> Turn:
        """Save exact text. An idempotent replay also returns only after commit."""
        _identifier(utterance_id)
        _identifier(source)
        if not isinstance(body, str):
            raise TypeError("Body must be text")
        with self._transaction():
            if (
                _web_authority is not None
                and self.authenticate_web_session(*_web_authority, submit=True) != call_id
            ):
                raise AccessError()
            self.get_binding(call_id)
            turn = self._find_turn(call_id, "user", utterance_id)
            audio = self._db.execute(
                "SELECT 1 FROM audio_jobs WHERE call_id=? AND utterance_id=?",
                (call_id, utterance_id),
            ).fetchone()
            if audio is not None and (turn is None or source != "audio_transcription"):
                raise AudioError("conflict")
            if turn is not None:
                if turn.body != body or turn.source != source:
                    raise MaterialConflict("Utterance identifier has different material")
            else:
                if source != "audio_transcription" and any(
                    self._series_unresolved(r) for r in self.series_roots(call_id)
                ):
                    raise AudioError("conflict")
                if self.is_call_closing(call_id):
                    raise CallClosed("Call is closed; new utterance was not accepted")
                turn = self._insert_turn(call_id, "user", utterance_id, source, body)
        return turn

    def get_transcript(self, call_id: str) -> tuple[Turn, ...]:
        self.get_binding(call_id)
        rows = self._db.execute(
            "SELECT * FROM turns WHERE call_id=? ORDER BY sequence", (call_id,)
        ).fetchall()
        return tuple(Turn(**dict(row)) for row in rows)

    def get_generation(self, call_id: str, generation_id: str) -> Generation:
        row = self._db.execute(
            "SELECT * FROM generations WHERE call_id=? AND generation_id=?",
            (call_id, generation_id),
        ).fetchone()
        if row is None:
            raise NotFound("Generation does not exist")
        return Generation(**dict(row))

    def claim_generation(
        self, call_id: str, generation_id: str, *, utterance_id: str, source: str
    ) -> tuple[Generation, bool]:
        """Claim one reply per utterance. Only the winner may call the fake."""
        _identifier(generation_id)
        _identifier(source)
        with self._transaction():
            user = self._find_turn(call_id, "user", utterance_id)
            if user is None:
                raise NotFound("User utterance does not exist")
            row = self._db.execute(
                "SELECT * FROM generations WHERE call_id=? AND "
                "(generation_id=? OR user_sequence=?)",
                (call_id, generation_id, user.sequence),
            ).fetchone()
            created = row is None
            if created:
                state = "cancelled" if self.is_call_closing(call_id) else "running"
                generation = Generation(call_id, generation_id, user.sequence, source, state, None)
                self._db.execute(
                    "INSERT INTO generations VALUES (?,?,?,?,?,?)", astuple(generation)
                )
            else:
                generation = Generation(**dict(row))
                if (
                    generation.generation_id != generation_id
                    or generation.user_sequence != user.sequence
                    or generation.source != source
                ):
                    raise MaterialConflict(
                        "Generation identifier or utterance has a different step"
                    )
        return generation, created and generation.state == "running"

    def complete_generation(self, call_id: str, generation_id: str, body: str) -> Turn:
        """Atomically commit the complete Bot text and generation success."""
        if not isinstance(body, str) or not body or body.isspace():
            raise ValueError("A successful reply requires nonblank text")
        with self._transaction():
            generation = self.get_generation(call_id, generation_id)
            if generation.state == "succeeded":
                turn = self._find_turn(call_id, "assistant", generation_id)
                if turn is None or turn.body != body or turn.source != generation.source:
                    raise MaterialConflict("Generation already has different material")
            elif generation.state == "running":
                if self.is_call_closing(call_id):
                    raise CallClosed("Call is closed; late reply was not saved")
                turn = self._insert_turn(
                    call_id, "assistant", generation_id, generation.source, body
                )
                self._db.execute(
                    "UPDATE generations SET state='succeeded',bot_sequence=? "
                    "WHERE call_id=? AND generation_id=?",
                    (turn.sequence, call_id, generation_id),
                )
                self.sync_segment_turn(call_id, generation_id)
                turn = self._find_turn(call_id, "assistant", generation_id)
                if call_id in self._speech_calls and not self.segments(call_id, generation_id):
                    self._db.execute(
                        "INSERT INTO speech_jobs VALUES (?,?,'pending',NULL)",
                        (call_id, generation_id),
                    )
                    self._speech_eligible.add((call_id, generation_id))
            else:
                raise StateConflict("Generation is already terminal")
        return turn

    def finish_without_reply(self, call_id: str, generation_id: str, state: str) -> Generation:
        if state not in {"failed", "cancelled", "empty"}:
            raise ValueError("Invalid unsuccessful generation state")
        with self._transaction():
            generation = self.get_generation(call_id, generation_id)
            if generation.state == "running":
                self._db.execute(
                    "UPDATE generations SET state=? WHERE call_id=? AND generation_id=?",
                    (state, call_id, generation_id),
                )
                generation = self.get_generation(call_id, generation_id)
            elif generation.state != state:
                if generation.state != "cancelled" or not (
                    self.is_call_closing(call_id) or self.speech_superseded(call_id, generation_id)
                ):
                    raise StateConflict("Generation is already terminal")
        return generation

    def observe_playback(
        self,
        call_id: str,
        generation_id: str,
        *,
        revision: int,
        state: str,
        _web_authority=None,
        _with_progress=False,
    ) -> Turn | tuple[Turn, bool]:
        """Single playback attempt; old observations are ignored, terminals stick.

        Revisions come from a future trusted receipt adapter, not wall-clock
        timestamps. Equal revision with different content is a conflict.
        'ended' is only a client observation, never proof the user heard it.
        """
        if type(revision) is not int or revision < 1:
            raise ValueError("Playback revision must be a positive integer")
        if state not in {"unknown", "started", "ended", "stopped", "failed"}:
            raise ValueError("Invalid playback state")
        with self._transaction():
            if (
                _web_authority is not None
                and self.authenticate_web_session(*_web_authority) != call_id
            ):
                raise AccessError()
            generation = self.get_generation(call_id, generation_id)
            if generation.state != "succeeded":
                raise StateConflict("No generated reply to observe")
            turn = self._find_turn(call_id, "assistant", generation_id)
            if turn is None:
                raise StateConflict("Generated reply is missing")
            if revision == turn.playback_revision and state != turn.playback_state:
                raise MaterialConflict("Playback revision has different content")
            advanced = revision > turn.playback_revision
            if advanced:
                previous = turn.playback_state
                if (previous in {"ended", "stopped", "failed"} and state != previous) or (
                    previous == "started" and state == "unknown"
                ):
                    raise StateConflict("Playback observation would reverse progress")
                self._db.execute(
                    "UPDATE turns SET playback_state=?,playback_revision=? "
                    "WHERE call_id=? AND role='assistant' AND material_id=?",
                    (state, revision, call_id, generation_id),
                )
                turn = self._find_turn(call_id, "assistant", generation_id)
        return (turn, advanced) if _with_progress else turn

    def _review_exists(self, call_id: str) -> bool:
        return (
            self._db.execute("SELECT 1 FROM reviews WHERE call_id=?", (call_id,)).fetchone()
            is not None
        )

    def get_saved_review_prompt(self, call_id: str) -> str | None:
        """Narrow immutable-prompt lookup; no input/body/audio/policy loading."""
        for table in ("reviews", "call_closures"):
            row = self._db.execute(
                "SELECT prompt FROM " + table + " WHERE call_id=?", (call_id,)
            ).fetchone()
            if row is not None:
                return row["prompt"]
        return None

    def get_review(self, call_id: str) -> ReviewJob:
        row = self._db.execute("SELECT * FROM reviews WHERE call_id=?", (call_id,)).fetchone()
        if row is None:
            raise NotFound("No end request exists for this call")
        return ReviewJob(**dict(row))

    def _audio_note(self, call_id):
        row = self._db.execute(
            "SELECT * FROM review_audio_notes WHERE call_id=?", (call_id,)
        ).fetchone()
        if row is None:
            return None
        if (
            hashlib.sha256(row["manifest_json"].encode("utf-8")).hexdigest()
            != row["manifest_sha256"]
        ):
            raise MaterialConflict("Frozen missing source fingerprint mismatch")
        return row

    def review_prompt_for_close(self, call_id, original, jobs):
        note = self._audio_note(call_id)
        if note is None:
            return self.series_review_prompt(
                call_id,
                self.segment_review(
                    call_id,
                    self._agent_note_prompt(
                        call_id,
                        self._query_note_prompt(call_id, audio_review_prompt(original, jobs)),
                    ),
                ),
            )
        if hashlib.sha256(original.encode("utf-8")).hexdigest() != note["original_prompt_sha256"]:
            raise MaterialConflict("Frozen missing prompt source mismatch")
        return self.series_review_prompt(
            call_id,
            self.segment_review(
                call_id,
                self._agent_note_prompt(call_id, self._query_note_prompt(call_id, note["prompt"])),
            ),
        )

    def _freeze_audio_note(self, call_id, original, jobs):
        prompt = audio_review_prompt(original, jobs)
        missing = [j for j in jobs if audio_no_text(j) or audio_retained(j)]
        if not missing or not self.audio_missing_enabled(call_id):
            return prompt
        entries = []
        for job in missing:
            entries.append(
                {
                    "utterance_id": job.utterance_id,
                    "ordinal": job.ordinal,
                    "mime": job.mime,
                    "sha256": job.sha256,
                    "byte_count": job.byte_count,
                    "received_at": job.received_at,
                    "state": job.state,
                    "error_code": job.error_code,
                    "decision": "empty_transcription"
                    if audio_no_text(job)
                    else job.resolution.decision,
                    "decided_at": None if audio_no_text(job) else job.resolution.decided_at,
                    "after_sequence": max(
                        (
                            j.user_sequence
                            for j in jobs
                            if j.ordinal < job.ordinal and j.user_sequence is not None
                        ),
                        default=0,
                    ),
                    "before_sequence": min(
                        (
                            j.user_sequence
                            for j in jobs
                            if j.ordinal > job.ordinal and j.user_sequence is not None
                        ),
                        default=None,
                    ),
                }
            )
        manifest = json.dumps(
            {"version": 1, "segments": entries},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        notice = f"材料说明：本次仅根据已识别文字整理，有{len(missing)}段未识别片段，原音频保留，不覆盖这些内容。"
        positions = "；".join(
            f"第{e['ordinal']}音频段（在文字序号{e['after_sequence']}之后"
            + (
                f"、{e['before_sequence']}之前）"
                if e["before_sequence"] is not None
                else "，后续无确认转写）"
            )
            for e in entries
        )
        prompt += "\n" + notice + "\n缺失的相对位置：" + positions + "。不得猜测缺失内容。"
        self._db.execute(
            "INSERT INTO review_audio_notes VALUES (?,?,?,?,?,?)",
            (
                call_id,
                manifest,
                hashlib.sha256(manifest.encode("utf-8")).hexdigest(),
                notice,
                hashlib.sha256(original.encode("utf-8")).hexdigest(),
                prompt,
            ),
        )
        return prompt

    def publication_body(self, call_id):
        review = self.get_review(call_id)
        if review.state != "succeeded":
            raise StateConflict("Only a successful review can be published")
        note = self._audio_note(call_id)
        if note is None:
            return review.body
        close = self.get_close_request(call_id)
        if (
            close is None
            or self.review_prompt_for_close(call_id, close.prompt, ()) != review.prompt
        ):
            raise MaterialConflict("Missing publication source mismatch")
        return note["notice"] + "\n\n" + review.body

    def end_call(
        self,
        call_id: str,
        *,
        prompt: str,
        _web_authority=None,
        _request_close=False,
        _deadline_only=False,
        _close_reason=None,
    ) -> ReviewJob | ClosingReceipt | None:
        """Commit end, logical cancellation and immutable full input together.

        A reply already awaiting its fake may finish, but its result cannot
        enter the transcript after this commit. Playback receipts may arrive
        later; the review keeps exactly the observations frozen at end time.
        """
        if not isinstance(prompt, str):
            raise TypeError("Review prompt must be text")
        if _close_reason not in (None, "manual", "duration", "disconnected"):
            raise ValueError("Invalid close reason")
        with self._transaction():
            if (
                _web_authority is not None
                and self.authenticate_web_session(*_web_authority) != call_id
            ):
                raise AccessError()
            if _deadline_only and (self.is_call_closing(call_id) or not self.limit_due(call_id)):
                return None
            self.get_binding(call_id)
            if self._review_exists(call_id):
                job = self.get_review(call_id)
                close = self.get_close_request(call_id)
                if job.prompt != prompt and not (
                    close is not None
                    and close.prompt == prompt
                    and job.prompt
                    == self.review_prompt_for_close(
                        call_id, close.prompt, self.get_audio_jobs(call_id)
                    )
                ):
                    raise MaterialConflict("End request already has a different prompt")
            else:
                jobs = self.get_audio_jobs(call_id)
                close = self.get_close_request(call_id)
                if close is not None and close.prompt != prompt:
                    raise MaterialConflict("Close request already has a different prompt")
                if any(not audio_settled(j) for j in jobs) and not _request_close:
                    raise StateConflict("Received audio is not fully transcribed")
                if close is None:
                    close = CloseRequest(call_id, time(), prompt)
                    self._db.execute("INSERT INTO call_closures VALUES (?,?,?)", astuple(close))
                self.stop_segments(call_id)
                if _close_reason == "manual":
                    self._db.execute(
                        "UPDATE speech_jobs SET state='cancelled' WHERE call_id=? AND state IN ('pending','running')",
                        (call_id,),
                    )
                    self._db.execute(
                        "UPDATE barge_candidates SET dispatch='cancelled' WHERE call_id=? AND dispatch IN ('pending','waiting')",
                        (call_id,),
                    )
                self._db.execute(
                    "UPDATE generations SET state='cancelled' WHERE call_id=? AND state='running'",
                    (call_id,),
                )
                if (
                    any(not audio_settled(j) for j in jobs)
                    or any(key[0] == call_id for key in self._active_agents)
                    or self._db.execute(
                        "SELECT 1 FROM agent_runs WHERE call_id=? AND state IN ('claimed','running')",
                        (call_id,),
                    ).fetchone()
                    is not None
                ):
                    self._record_limit_close(call_id, close.requested_at, _close_reason)
                    return ClosingReceipt(close.requested_at, closing_state(jobs))
                raw_turns = self.get_transcript(call_id)
                turns = self.effective_transcript(call_id)
                if not turns:
                    self._record_limit_close(call_id, close.requested_at, _close_reason)
                    return ClosingReceipt(close.requested_at, "closed_no_text")
                material = json.dumps(
                    [asdict(turn) for turn in turns],
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                final_prompt = self._agent_note_prompt(
                    call_id,
                    self._query_note_prompt(
                        call_id, self._freeze_audio_note(call_id, close.prompt, jobs)
                    ),
                    freeze=True,
                )
                final_prompt = self.series_review_prompt(
                    call_id, self.segment_review(call_id, final_prompt)
                )
                job = ReviewJob(
                    call_id,
                    "pending",
                    close.requested_at if close else time(),
                    raw_turns[-1].sequence if raw_turns else 0,
                    hashlib.sha256(material.encode("utf-8")).hexdigest(),
                    material,
                    final_prompt,
                    None,
                    None,
                )
                self._db.execute("INSERT INTO reviews VALUES (?,?,?,?,?,?,?,?,?)", astuple(job))
            self._record_limit_close(call_id, job.ended_at, _close_reason)
        return job

    def get_review_input(self, call_id: str) -> tuple[Turn, ...]:
        job = self.get_review(call_id)
        if hashlib.sha256(job.input_json.encode("utf-8")).hexdigest() != job.input_sha256:
            raise MaterialConflict("Frozen review input fingerprint mismatch")
        return tuple(Turn(**item) for item in json.loads(job.input_json))

    def review_call_ids(self) -> tuple[str, ...]:
        """Finite read-only discovery of ended calls, including unplanned results."""
        rows = self._db.execute("SELECT call_id FROM reviews ORDER BY ended_at,call_id").fetchall()
        return tuple(row["call_id"] for row in rows)

    def pending_review_ids(self) -> tuple[str, ...]:
        rows = self._db.execute(
            "SELECT call_id FROM reviews WHERE state='pending' ORDER BY ended_at,call_id"
        ).fetchall()
        return tuple(row["call_id"] for row in rows)

    def claim_review(self, call_id: str) -> tuple[ReviewJob, bool]:
        with self._transaction():
            job = self.get_review(call_id)
            claimed = job.state == "pending"
            if claimed:
                self._db.execute("UPDATE reviews SET state='running' WHERE call_id=?", (call_id,))
                job = self.get_review(call_id)
        return job, claimed

    def complete_review(
        self, call_id: str, body: str, *, manifest_sha256: str | None = None
    ) -> ReviewJob:
        if not isinstance(body, str) or not body or body.isspace():
            raise ValueError("A successful review requires nonblank text")
        with self._transaction():
            self._check_review_manifest(call_id, manifest_sha256)
            job = self.get_review(call_id)
            if job.state == "succeeded":
                if job.body != body:
                    raise MaterialConflict("Review already has a different result")
            elif job.state == "running":
                work = self.get_review_work(call_id)
                if work is not None and not (
                    not work.blocked_code
                    and work.kind == "final"
                    and len(work.frontier) == 1
                    and work.frontier[0].state == "succeeded"
                    and work.frontier[0].first_map == 0
                    and work.frontier[0].stop_map == work.map_count
                    and work.frontier[0].body == body
                ):
                    raise StateConflict("Chunk work has no complete final result")
                self._db.execute(
                    "UPDATE reviews SET state='succeeded',body=?,error_code=NULL WHERE call_id=?",
                    (body, call_id),
                )
                job = self.get_review(call_id)
            else:
                raise StateConflict("Review is not running")
        return job

    def fail_review(self, call_id: str, *, state: str, error_code: str) -> ReviewJob:
        if (state, error_code) not in {
            ("failed", "fake_failed"),
            ("failed", "invalid_result"),
            ("cancelled", "fake_cancelled"),
        }:
            raise ValueError("Invalid review failure state/code")
        with self._transaction():
            job = self.get_review(call_id)
            if job.state == "running":
                self._db.execute(
                    "UPDATE reviews SET state=?,error_code=? WHERE call_id=?",
                    (state, error_code, call_id),
                )
                job = self.get_review(call_id)
            elif job.state != state or job.error_code != error_code:
                raise StateConflict("Review is already terminal or uncertain")
        return job

    def mark_review_uncertain(self, call_id: str) -> ReviewJob:
        """Use after a known worker interruption; never steal an active worker.

        This records the need for manual reconciliation, not a retry lease.
        Opening another connection never marks or takes over running work.
        """
        with self._transaction():
            job = self.get_review(call_id)
            if job.state == "running":
                self._db.execute(
                    "UPDATE reviews SET state='uncertain',error_code='result_uncertain' "
                    "WHERE call_id=?",
                    (call_id,),
                )
                job = self.get_review(call_id)
        return job

    def get_review_work_node(self, call_id: str, source_key: str) -> ReviewWorkNode:
        row = self._db.execute(
            "SELECT * FROM review_work_nodes WHERE call_id=? AND source_key=?",
            (call_id, source_key),
        ).fetchone()
        if row is None:
            raise NotFound("Review work node does not exist")
        values = dict(row)
        values["child_keys"] = tuple(json.loads(values.pop("children_json")))
        return ReviewWorkNode(**values)

    def get_review_work(self, call_id: str) -> ReviewWork | None:
        row = self._db.execute("SELECT * FROM review_work WHERE call_id=?", (call_id,)).fetchone()
        if row is None:
            return None
        stage = self._db.execute(
            "SELECT * FROM review_work_stages WHERE call_id=? ORDER BY level DESC LIMIT 1",
            (call_id,),
        ).fetchone()
        frontier = tuple(
            self.get_review_work_node(call_id, key) for key in json.loads(stage["frontier_json"])
        )
        return ReviewWork(
            **dict(row),
            level=stage["level"],
            kind=stage["kind"],
            frontier=frontier,
            review=self.get_review(call_id),
            manifest=self.get_review_manifest(call_id),
        )

    def get_review_manifest(self, call_id: str) -> ReviewManifest | None:
        row = self._db.execute(
            "SELECT m.*,w.fingerprint AS original_plan FROM review_policy_manifests m "
            "JOIN review_work w USING(call_id) WHERE m.call_id=?",
            (call_id,),
        ).fetchone()
        if row is None:
            return None
        manifest = ReviewManifest(row["manifest_json"], row["fingerprint"])
        if row["plan_fingerprint"] != row["original_plan"] or manifest.binding != self.get_binding(
            call_id
        ):
            raise MaterialConflict("Review manifest source mismatch")
        return manifest

    def _check_review_manifest(self, call_id: str, fingerprint: str | None):
        saved = self.get_review_manifest(call_id)
        if (saved.fingerprint if saved is not None else None) != fingerprint:
            raise StateConflict("Review policy authorization is required")

    def review_work_ids(self) -> tuple[str, ...]:
        """Finite discovery, including blocked work; never resets a claim."""
        return tuple(
            row[0]
            for row in self._db.execute(
                "SELECT call_id FROM review_work ORDER BY call_id"
            ).fetchall()
        )

    def _insert_review_work_node(self, node: ReviewWorkNode) -> None:
        values = asdict(node)
        values["children_json"] = json.dumps(values.pop("child_keys"), separators=(",", ":"))
        columns = tuple(values)
        self._db.execute(
            "INSERT INTO review_work_nodes ("
            + ",".join(columns)
            + ") VALUES ("
            + ",".join("?" for _ in columns)
            + ")",
            tuple(values.values()),
        )

    def install_review_work(self, plan, *, manifest: ReviewManifest | None = None) -> ReviewWork:
        # Imports are local to avoid a Store/planner import cycle. This verifier
        # is pure; no user counter, callback or await runs in the transaction.
        from .review_work import _validate_plan

        with self._transaction():
            existing = self.get_review_work(plan.call_id)
            if existing is not None:
                if existing.manifest != manifest:
                    raise StateConflict("Existing work has a different policy mode")
                return existing
            job = self.get_review(plan.call_id)
            if job.state != "pending":
                raise StateConflict("Only an unclaimed review can install chunk work")
            _validate_plan(job, plan)
            if manifest is not None and (
                type(manifest) is not ReviewManifest
                or manifest.binding != self.get_binding(plan.call_id)
                or manifest.input_token_budget != plan.input_token_budget
            ):
                raise StateConflict("Manifest does not match the bound plan")
            encoded = json.dumps(
                asdict(plan),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            self._db.execute(
                "INSERT INTO review_work VALUES (?,?,?,?,?,NULL)",
                (job.call_id, encoded, plan.fingerprint, plan.input_token_budget, len(plan.chunks)),
            )
            if manifest is not None:
                self._db.execute(
                    "INSERT INTO review_policy_manifests VALUES (?,?,?,?)",
                    (job.call_id, plan.fingerprint, manifest.canonical_json, manifest.fingerprint),
                )
            keys = []
            for chunk in plan.chunks:
                node = ReviewWorkNode(
                    job.call_id,
                    chunk.source_key,
                    0,
                    chunk.ordinal,
                    "map",
                    chunk.ordinal - 1,
                    chunk.ordinal,
                    (),
                    chunk.request_json,
                    chunk.token_count,
                    "pending",
                    None,
                    None,
                    None,
                )
                self._insert_review_work_node(node)
                keys.append(node.source_key)
            self._db.execute(
                "INSERT INTO review_work_stages VALUES (?,0,'map',?)",
                (job.call_id, json.dumps(keys)),
            )
            self._db.execute("UPDATE reviews SET state='running' WHERE call_id=?", (job.call_id,))
        return self.get_review_work(job.call_id)

    def add_review_work_stage(
        self,
        call_id: str,
        previous_level: int,
        kind: str,
        nodes: tuple[ReviewWorkNode, ...],
        frontier_keys: tuple[str, ...],
        *,
        manifest_sha256: str | None = None,
    ) -> ReviewWork:
        with self._transaction():
            self._check_review_manifest(call_id, manifest_sha256)
            work = self.get_review_work(call_id)
            if work.level != previous_level:
                return work  # Another connection already committed the fixed next stage.
            if (
                work.state != "pending"
                or work.kind == "final"
                or kind not in {"merge", "final"}
                or not work.frontier
                or any(n.state != "succeeded" for n in work.frontier)
            ):
                raise StateConflict("Previous review work stage is not complete")
            old = {n.source_key: n for n in work.frontier}
            new = {n.source_key: n for n in nodes}
            if len(new) != len(nodes) or set(new) & set(old) or not nodes:
                raise StateConflict("Invalid review work stage")
            consumed, ordinal = [], 0
            for key in frontier_keys:
                if key in old:
                    if kind == "final":
                        raise StateConflict("Final stage must generate a final review")
                    consumed.append(key)
                    continue
                node = new.get(key)
                ordinal += 1
                if (
                    node is None
                    or node.call_id != call_id
                    or node.level != work.level + 1
                    or node.ordinal != ordinal
                    or node.kind != kind
                    or node.state != "pending"
                    or node.body is not None
                    or node.result_sha256 is not None
                    or node.error_code is not None
                    or type(node.token_count) is not int
                    or not 0 <= node.token_count <= work.input_token_budget
                    or not node.child_keys
                    or any(k not in old for k in node.child_keys)
                    or (kind == "merge" and len(node.child_keys) < 2)
                    or node.first_map != old[node.child_keys[0]].first_map
                    or node.stop_map != old[node.child_keys[-1]].stop_map
                ):
                    raise StateConflict("Invalid review work node")
                from .review_work import _reduction_request

                expected_key, expected_request = _reduction_request(
                    work,
                    kind,
                    tuple(old[k] for k in node.child_keys),
                    node.ordinal,
                )
                if (node.source_key, node.request_json) != (expected_key, expected_request):
                    raise StateConflict("Review work request does not match its sources")
                consumed.extend(node.child_keys)
            if (
                tuple(consumed) != tuple(old)
                or ordinal != len(nodes)
                or (kind == "merge" and len(frontier_keys) >= len(old))
                or (kind == "final" and (len(nodes) != 1 or len(frontier_keys) != 1))
            ):
                raise StateConflict("Review work stage must preserve ordered complete coverage")
            for node in nodes:
                self._insert_review_work_node(node)
            self._db.execute(
                "INSERT INTO review_work_stages VALUES (?,?,?,?)",
                (call_id, work.level + 1, kind, json.dumps(frontier_keys)),
            )
        return self.get_review_work(call_id)

    def claim_review_work_node(
        self, call_id: str, source_key: str, *, manifest_sha256: str | None = None
    ) -> tuple[ReviewWorkNode, bool]:
        with self._transaction():
            self._check_review_manifest(call_id, manifest_sha256)
            work = self.get_review_work(call_id)
            node = self.get_review_work_node(call_id, source_key)
            claimed = False
            if work.state == "pending":
                for candidate in work.frontier:
                    if candidate.state == "succeeded":
                        continue
                    if candidate.source_key == source_key and node.state == "pending":
                        self._db.execute(
                            "UPDATE review_work_nodes SET state='running' WHERE source_key=?",
                            (source_key,),
                        )
                        claimed = True
                    break
            node = self.get_review_work_node(call_id, source_key)
        return node, claimed

    def finish_review_work_node(
        self,
        call_id: str,
        source_key: str,
        *,
        state: str,
        body: str | None = None,
        manifest_sha256: str | None = None,
        error_code: str | None = None,
    ) -> ReviewWorkNode:
        from .failures import CATEGORIES

        if error_code is not None and (state != "failed" or error_code not in CATEGORIES):
            raise ValueError("Invalid review failure category")
        if state not in {"succeeded", "failed", "cancelled", "uncertain"}:
            raise ValueError("Invalid review work outcome")
        if state == "succeeded":
            if type(body) is not str or not body or body.isspace():
                raise ValueError("Review work result requires nonblank text")
            digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
            error = None
        else:
            if body is not None:
                raise ValueError("Unconfirmed result must not be saved as successful")
            digest, error = (
                None,
                {
                    "failed": "not_applied_or_invalid_result",
                    "cancelled": "result_cancelled",
                    "uncertain": "result_uncertain",
                }[state],
            )
        error = error_code or error
        with self._transaction():
            self._check_review_manifest(call_id, manifest_sha256)
            node = self.get_review_work_node(call_id, source_key)
            if node.state == "running":
                self._db.execute(
                    "UPDATE review_work_nodes SET state=?,body=?,result_sha256=?,error_code=? "
                    "WHERE call_id=? AND source_key=?",
                    (state, body, digest, error, call_id, source_key),
                )
                if state == "failed":
                    self._db.execute(
                        "UPDATE reviews SET state='failed',error_code=? WHERE call_id=? AND state='running'",
                        (error, call_id),
                    )
            elif (node.state, node.body, node.result_sha256) != (state, body, digest):
                raise StateConflict("Review work outcome is already terminal")
            node = self.get_review_work_node(call_id, source_key)
        return node

    def block_review_work(self, call_id: str, *, manifest_sha256: str | None = None) -> ReviewWork:
        with self._transaction():
            self._check_review_manifest(call_id, manifest_sha256)
            work = self.get_review_work(call_id)
            if work.blocked_code is None:
                if work.state != "pending" or any(n.state != "succeeded" for n in work.frontier):
                    raise StateConflict("Cannot block an active or unresolved stage")
                self._db.execute(
                    "UPDATE review_work SET blocked_code='merge_budget' WHERE call_id=?", (call_id,)
                )
        return self.get_review_work(call_id)

    def prepare_publication(self, call_id: str, *, char_budget: int = 1800) -> Publication:
        """Freeze one derived slice plan for the unique successful review body."""
        if type(char_budget) is not int or char_budget < 1:
            raise ValueError("Character budget must be a positive integer")
        with self._transaction():
            review = self.get_review(call_id)
            if review.state != "succeeded":
                raise StateConflict("Only a successful review can be published")
            row = self._db.execute(
                "SELECT char_budget FROM publications WHERE call_id=?", (call_id,)
            ).fetchone()
            if row is not None:
                if row["char_budget"] != char_budget:
                    raise MaterialConflict("Publication already has a different character budget")
            else:
                started_at = self._db.execute(
                    "SELECT created_at FROM calls WHERE call_id=?", (call_id,)
                ).fetchone()[0]
                body = self.publication_body(call_id)
                ranges = _publication_ranges(body, char_budget, started_at, review.ended_at)
                digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
                self._db.execute(
                    "INSERT INTO publications VALUES (?,?,?,?,?,?,?,'pending',NULL)",
                    (
                        call_id,
                        digest,
                        char_budget,
                        started_at,
                        review.ended_at,
                        len(ranges),
                        f"review:{call_id}:context",
                    ),
                )
                for number, (start, stop) in enumerate(ranges, 1):
                    self._db.execute(
                        "INSERT INTO publication_pages VALUES (?,?,?,?,?,'pending',NULL)",
                        (call_id, number, start, stop, f"review:{call_id}:page:{number}"),
                    )
        return self.get_publication(call_id)

    def get_publication(self, call_id: str) -> Publication:
        row = self._db.execute("SELECT * FROM publications WHERE call_id=?", (call_id,)).fetchone()
        if row is None:
            raise NotFound("Publication plan does not exist")
        review = self.get_review(call_id)
        body = self.publication_body(call_id)
        if (
            review.state != "succeeded"
            or hashlib.sha256(body.encode("utf-8")).hexdigest() != row["review_sha256"]
        ):
            raise MaterialConflict("Publication source fingerprint mismatch")
        pages = []
        for page in self._db.execute(
            "SELECT * FROM publication_pages WHERE call_id=? ORDER BY number", (call_id,)
        ).fetchall():
            fragment = body[page["start"] : page["stop"]]
            message = (
                _publication_header(
                    row["started_at"], row["ended_at"], page["number"], row["page_count"]
                )
                + fragment
            )
            pages.append(PublicationPage(**dict(page), fragment=fragment, message=message))
        return Publication(**dict(row), body=body, pages=tuple(pages))

    def claim_publication_page(self, call_id: str) -> tuple[PublicationPage | None, bool]:
        """Claim the earliest unfinished send step; terminal steps never resend."""
        with self._transaction():
            publication = self.get_publication(call_id)
            page = next((page for page in publication.pages if page.state != "succeeded"), None)
            claimed = page is not None and page.state == "pending"
            if claimed:
                self._db.execute(
                    "UPDATE publication_pages SET state='running' WHERE call_id=? AND number=?",
                    (call_id, page.number),
                )
                page = self.get_publication(call_id).pages[page.number - 1]
        return page, claimed

    def claim_publication_context(self, call_id: str) -> tuple[Publication, bool]:
        with self._transaction():
            publication = self.get_publication(call_id)
            claimed = publication.context_state == "pending"
            if claimed:
                self._db.execute(
                    "UPDATE publications SET context_state='running' WHERE call_id=?", (call_id,)
                )
                publication = self.get_publication(call_id)
        return publication, claimed

    @staticmethod
    def _publication_error(state: str) -> str | None:
        if state not in {"succeeded", "failed", "uncertain"}:
            raise ValueError("Invalid publication outcome")
        return {"succeeded": None, "failed": "not_applied", "uncertain": "result_uncertain"}[state]

    def finish_publication_page(self, call_id: str, number: int, state: str) -> PublicationPage:
        error = self._publication_error(state)
        with self._transaction():
            row = self._db.execute(
                "SELECT state FROM publication_pages WHERE call_id=? AND number=?",
                (call_id, number),
            ).fetchone()
            if row is None:
                raise NotFound("Publication page does not exist")
            if row["state"] == "running":
                self._db.execute(
                    "UPDATE publication_pages SET state=?,error_code=? WHERE call_id=? AND number=?",
                    (state, error, call_id, number),
                )
            elif row["state"] != state:
                raise StateConflict("Page is not running or already has a different outcome")
            page = self.get_publication(call_id).pages[number - 1]
        return page

    def finish_publication_context(self, call_id: str, state: str) -> Publication:
        error = self._publication_error(state)
        with self._transaction():
            publication = self.get_publication(call_id)
            if publication.context_state == "running":
                self._db.execute(
                    "UPDATE publications SET context_state=?,context_error=? WHERE call_id=?",
                    (state, error, call_id),
                )
                publication = self.get_publication(call_id)
            elif publication.context_state != state:
                raise StateConflict("Context is not running or already has a different outcome")
        return publication

    def enable_speech(self, call_id):
        self.get_binding(call_id)
        self._speech_calls.add(call_id)

    def recover_speech(self):
        with self._transaction():
            self._db.execute(
                "UPDATE speech_segments SET state='uncertain',data=NULL WHERE seq>0 AND state IN ('pending','running')"
            )
            self._db.execute(
                "UPDATE speech_segments SET state='stopped' WHERE seq=0 AND state='open'"
            )

        # Explicit runtime startup audit, never a retry/claim takeover.
        with self._transaction():
            self._db.execute(
                "UPDATE speech_jobs SET state='uncertain',data=NULL WHERE state='running'"
            )

    def get_speech(self, call_id, generation_id):
        from .speech import SpeechJob

        row = self._db.execute(
            "SELECT * FROM speech_jobs WHERE call_id=? AND generation_id=?",
            (call_id, generation_id),
        ).fetchone()
        if row is None:
            raise AccessError("conflict") from None
        return SpeechJob(**dict(row))

    def claim_speech(self, call_id, generation_id, authority):
        with self._transaction():
            if self.authenticate_web_session(*authority) != call_id:
                raise AccessError() from None
            job = self.get_speech(call_id, generation_id)
            if self.is_call_closing(call_id):
                raise AccessError("ended") from None
            if job.state == "pending":
                if (call_id, generation_id) not in self._speech_eligible:
                    raise AccessError("unconfirmed") from None
                generation = self.get_generation(call_id, generation_id)
                if generation.state != "succeeded":
                    raise AccessError("conflict") from None
                self._db.execute(
                    "UPDATE speech_jobs SET state='running' WHERE call_id=? AND generation_id=?",
                    (call_id, generation_id),
                )
                return self.get_speech(call_id, generation_id), True
            return job, False

    def finish_speech(self, call_id, generation_id, state, data=None):
        from .speech import check_wav

        if state not in {"succeeded", "failed", "uncertain", "cancelled"}:
            raise ValueError("Invalid synthesis outcome")
        if state == "succeeded":
            check_wav(data)
        with self._transaction():
            job = self.get_speech(call_id, generation_id)
            if job.state != "running":
                return job
            if self.is_call_closing(call_id):
                state, data = "cancelled", None
            if state != "succeeded":
                data = None
            self._db.execute(
                "UPDATE speech_jobs SET state=?,data=? WHERE call_id=? AND generation_id=?",
                (state, data, call_id, generation_id),
            )
            return self.get_speech(call_id, generation_id)

    def speech_status(self, call_id, generation_id, authority):
        """Pure authorized status read; no claim, audio bytes or retry authority."""
        with self._transaction(read_only=True):
            if self.authenticate_web_session(*authority) != call_id:
                raise AccessError() from None
            if self.is_call_closing(call_id):
                raise AccessError("ended") from None
            row = self._db.execute(
                "SELECT generation_id,state FROM speech_jobs WHERE call_id=? AND generation_id=?",
                (call_id, generation_id),
            ).fetchone()
            if row is None:
                raise AccessError("conflict") from None
            return dict(row)

    def speech_audio(self, call_id, generation_id, authority):
        with self._transaction():
            if self.authenticate_web_session(*authority) != call_id:
                raise AccessError() from None
            if self.is_call_closing(call_id):
                raise AccessError("ended") from None
            job = self.get_speech(call_id, generation_id)
            if job.state != "succeeded":
                raise AccessError("conflict") from None
            return job.data
