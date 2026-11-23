"""A single owned storage thread and explicit bridges to one business loop.

No plugin registration or production transport. Import has no startup effects. All authoritative call outcomes remain in Store.
"""

from __future__ import annotations

import asyncio
import json
import logging
import hashlib
import math
import re
import time
from concurrent.futures import Future
from dataclasses import dataclass, field, replace
from pathlib import Path
from inspect import iscoroutinefunction, isasyncgenfunction
from threading import Thread

from . import store as storage
from .observability import CURRENT_TIMING, RoundTiming, milliseconds
from .binding import CallBinding
from .call_policy import CallPolicy
from .speech import SpeechUnavailable, check_wav, segment_audio_mime
from .limits import RuntimeLimits
from .audio import (
    AudioJob,
    AudioError,
    AudioNotApplied,
    CloseRequest,
    ClosingReceipt,
    advance_audio,
    closing_state,
    audio_settled,
    audio_retained,
    NoTextResult,
    AUDIO_MIMES,
)
from .access import (
    AccessError,
    InvitationSecret,
    WebCallState,
    WebTurn,
    WebReply,
    token_digest,
    session_reference,
)
from .publication import Outcome, advance_publication
from .review import (
    DEFAULT_REVIEW_PROMPT,
    _USE_DEFAULT_PROMPT,
    saved_or_default_prompt,
    end_call,
    run_pending,
)
from .service import process_utterance
from .review_work import ModelFailed, NotApplied, advance_review_work, prepare_review_work
from .review_policy import ReviewManifest, ReviewPolicyState, ReviewPolicyUnavailable
from .review_router import DetachedReviewRoute, ReviewPolicyRouter
from .store import (
    MaterialConflict,
    StateConflict,
    Generation,
    NotFound,
    Publication,
    ReviewJob,
    ReviewWork,
    Store,
    Turn,
)


class RuntimeClosed(RuntimeError):
    """The runtime is not accepting requests."""


class RuntimeOperationError(RuntimeError):
    """A runtime operation failed; inspect the durable state before any retry."""


@dataclass(frozen=True, slots=True)
class RuntimeCallbacks:
    reply: object = field(repr=False)
    review: object = field(repr=False)
    send_page: object = field(repr=False)
    make_context_readable: object = field(repr=False)
    synthesize: object = field(default=None, repr=False)
    interrupt: object = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class RuntimeReviewWork:
    """Explicit offline fake mode, not a production per-UMO budget policy.

    The pure synchronous counter runs only on the storage thread. The async
    generator runs only on the captured business loop. No Provider objects or
    credentials belong in this configuration or the frozen requests.
    """

    input_token_budget: int
    count_tokens: object = field(repr=False)
    generate: object = field(repr=False)

    def __post_init__(self):
        if type(self.input_token_budget) is not int or self.input_token_budget < 1:
            raise ValueError("Input token budget must be a positive integer")
        counter = self.count_tokens
        counter_call = getattr(counter, "__call__", None)
        if (
            not callable(counter)
            or iscoroutinefunction(counter)
            or isasyncgenfunction(counter)
            or iscoroutinefunction(counter_call)
            or isasyncgenfunction(counter_call)
        ):
            raise TypeError("A trusted pure synchronous counter is required")
        if not callable(self.generate):
            raise TypeError("A trusted asynchronous chunk callback is required")


@dataclass(frozen=True, slots=True)
class RuntimeAudio:
    transcribe: object = field(repr=False)
    max_audio_bytes: int = 2097152
    max_continuous_segments: int = 10

    def __post_init__(self):
        if (
            not callable(self.transcribe)
            or type(self.max_audio_bytes) is not int
            or self.max_audio_bytes < 1
            or type(self.max_continuous_segments) is not int
            or not 1 <= self.max_continuous_segments <= 20
        ):
            raise ValueError("A trusted audio callback and positive byte limit are required")


@dataclass(frozen=True, slots=True)
class CallState:
    binding: CallBinding = field(repr=False)
    turns: tuple[Turn, ...] = field(repr=False)
    generations: tuple[Generation, ...]
    review: ReviewJob | None
    publication: Publication | None
    review_work: ReviewWork | None = field(default=None, repr=False)
    review_policy: ReviewPolicyState | None = field(default=None, repr=False)
    audio_jobs: tuple[AudioJob, ...] = field(default=(), repr=False)
    close_request: CloseRequest | None = field(default=None, repr=False)
    raw_turns: tuple[Turn, ...] | None = field(default=None, repr=False)
    material_notes: str = field(default="", repr=False)
    projected_turns: tuple[Turn, ...] | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class ClosingSource:
    """Immutable persisted close/source identity; processing states may advance."""

    close: CloseRequest = field(repr=False)
    audio_sources: tuple[tuple, ...] = field(repr=False)


@dataclass(frozen=True, slots=True)
class ShutdownReport:
    stopped: bool
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class _CoreResult:
    kind: str
    value: object = field(default=None, repr=False)


def _management_ref(call_id):
    return session_reference(hashlib.sha256(call_id.encode()).hexdigest())


def _consume(future):
    if not future.cancelled():
        future.exception()


from .chunk_upload import ChunkUploads
from .segment_runtime import SegmentRuntime
from .push import PushRuntime


from .barge_in import BargeRuntime


class CallRuntime(BargeRuntime, SegmentRuntime, PushRuntime):
    """Construct/start/use/shutdown on the captured business loop.

    A plugin must strongly own this object and await shutdown. A timeout leaves
    the owned shutdown task, thread, Store and actual callback tasks alive until
    their real completion. Never stop the business loop before cleanup finishes.
    """

    def __init__(
        self,
        path: str | Path,
        callbacks: RuntimeCallbacks,
        *,
        char_budget: int = 1800,
        review_work: RuntimeReviewWork | None = None,
        review_routes: ReviewPolicyRouter | None = None,
        audio: RuntimeAudio | None = None,
        limits: RuntimeLimits | None = None,
        restrict_calls: bool = False,
        _limit_timer=None,
        timings=None,
        review_prompt: str = DEFAULT_REVIEW_PROMPT,
        barge_in: bool = True,
        backchannel_phrases=None,
        audio_retention_days: int = 0,
        retention_start_delay: float = 60.0,  # Let startup/reload checks finish first.
        retention_interval: float = 3600.0,  # Day-scale expiry needs only hourly checks.
        retention_backlog_delay: float = 5.0,  # Yield to requests between batches.
        retention_busy_delay: float = 300.0,  # Give active calls time to finish.
    ):
        self._business = asyncio.get_running_loop()
        self._watchers = {}
        self._barge_tasks = {}
        self._barge_in = barge_in
        from .barge_in import DEFAULT_BACKCHANNEL_PHRASES

        self._backchannel_phrases = (
            DEFAULT_BACKCHANNEL_PHRASES
            if backchannel_phrases is None
            else tuple(backchannel_phrases)
        )
        try:
            if type(review_prompt) is not str:
                raise ValueError
            review_prompt.encode("utf-8")
        except Exception:
            raise ValueError("review.invalid_configuration") from None
        self._review_prompt = review_prompt
        self._call_review_prompts = {}
        if type(char_budget) is not int or char_budget < 1:
            raise ValueError("Character budget must be a positive integer")
        if not isinstance(callbacks, RuntimeCallbacks) or any(
            not callable(getattr(callbacks, name))
            for name in ("reply", "review", "send_page", "make_context_readable")
        ):
            raise TypeError("Four trusted asynchronous callbacks are required")
        if review_work is not None and not isinstance(review_work, RuntimeReviewWork):
            raise TypeError("Expected an explicit RuntimeReviewWork configuration")
        if review_routes is not None and (
            type(review_routes) is not ReviewPolicyRouter
            or review_work is not None
            or review_routes.business_loop is not self._business
        ):
            raise TypeError("Use one trusted review mode on the original business loop")
        if audio is not None and type(audio) is not RuntimeAudio:
            raise TypeError("Expected an explicit RuntimeAudio configuration")
        if limits is not None and type(limits) is not RuntimeLimits:
            raise ValueError("call_limits.invalid_configuration") from None
        if _limit_timer is not None and not callable(_limit_timer):
            raise ValueError("call_limits.invalid_configuration") from None
        if type(restrict_calls) is not bool or (restrict_calls and review_routes is None):
            raise ValueError("Restricted calls require the controlled review router") from None
        self._timings = timings
        self._restrict_calls = restrict_calls
        self._limits, self._limit_timer = limits, _limit_timer
        self._audio = audio
        self._audio_retention_days = audio_retention_days
        self._retention_start_delay = retention_start_delay
        self._retention_interval = retention_interval
        self._retention_backlog_delay = retention_backlog_delay
        self._retention_busy_delay = retention_busy_delay
        self._retention_handle = None
        self._review_routes = review_routes
        self._review_work = review_work
        self._path, self._budget = path, char_budget
        self._callbacks = callbacks  # Accessed only by _invoke on the business loop.
        self._core_tasks = {}
        self._cancel_tokens = set()
        self._started, self._exited = Future(), Future()
        self._started.add_done_callback(_consume)
        self._exited.add_done_callback(_consume)
        self._thread = None
        self._worker_loop = None
        self._start_task = self._close_task = None
        self._accepting = False
        self._closing = False
        self._worker_error = None

    def _check_loop(self):
        if asyncio.get_running_loop() is not self._business:
            raise RuntimeClosed("Runtime access requires the original business loop")

    async def start(self) -> None:
        self._check_loop()
        if self._closing:
            raise RuntimeClosed("Runtime is closing or closed")
        if self._start_task is None:
            self._start_task = asyncio.create_task(self._start(), name="BotCall:start")
            self._start_task.add_done_callback(_consume)
        await asyncio.shield(self._start_task)

    async def _start(self):
        self._thread = Thread(target=self._thread_main, name="BotCall:storage", daemon=False)
        try:
            self._thread.start()
        except Exception:
            self._closing = True
            raise RuntimeOperationError("Runtime startup failed") from None
        try:
            await asyncio.shield(asyncio.wrap_future(self._started))
        except Exception:
            await asyncio.shield(asyncio.wrap_future(self._exited))
            await asyncio.to_thread(self._thread.join)
            self._closing = True
            raise RuntimeOperationError("Runtime startup failed") from None
        self._accepting = not self._closing

    def _thread_main(self):
        loop = asyncio.new_event_loop()
        self._worker_loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._serve())
        except BaseException:
            self._worker_error = "worker_failed"
            if not self._started.done():
                self._started.set_exception(RuntimeOperationError("Runtime startup failed"))
        finally:
            loop.close()
            self._exited.set_result(None)

    async def _serve(self):
        self._owned = set()
        self._background = set()
        self._worker_stopping = False
        self._progress_requested = False
        self._progress_task = None
        self._stop = asyncio.Event()
        self._policy_status = {}
        self._review_observed = {}
        self._review_tasks, self._review_requested = {}, set()
        self._review_context_only = set()
        self._retry_adapters = {}
        self._audio_tasks, self._audio_requested = {}, set()
        self._chunks = ChunkUploads(
            self._audio.max_audio_bytes if self._audio else 0,
            asyncio.get_running_loop(),
            self._speech_progress if self._limits and self._limits.idle_seconds else None,
        )
        self._active_audio_jobs = set()  # Until actual business callback/cleanup exit.
        # Optional waiting index only. Overflow synthesis remains owned by _owned
        # and durable speech_jobs, with unchanged claim/SDK behavior.
        self._speech_tasks, self._speech_waiters = {}, set()
        self._segment_tasks, self._segment_started = {}, {}
        self._reply_tasks, self._speech_work = {}, {}
        self._last_speech_at, self._close_details = {}, {}
        self._active_tools: dict[tuple[str, str], int] = {}
        self._speech_wait_stop = asyncio.Event()
        self._state_waiters = {}
        self._state_wait_stop = asyncio.Event()
        self._limit_handle, self._limit_error = None, None
        # The scope is worker-owned and empty BEFORE opening/scanning Store.
        # None preserves the historical unfiltered Runtime behavior.
        self._recovery_calls = set()  # Recovery never grants interactive/web authority.
        self._closing_recovery_calls = {}  # Only validated closing sources can run background audio.
        self._accepted_calls = {} if self._restrict_calls else None
        self._issued_invites = {}
        self._store = Store(self._path)
        try:
            self._store.recover_speech()
            self._store.recover_agents()
            self._recovered_limit_calls = set(self._limit_call_ids())
            if self._limits is not None:
                self._sweep_limits(recovering=True)
                self._arm_limits()
            self._arm_retention(self._retention_start_delay)
            self._started.set_result(None)
            await self._stop.wait()
            if self._owned:
                await asyncio.gather(*tuple(self._owned), return_exceptions=True)
        finally:
            self._cancel_retention()
            if self._limit_handle is not None:
                self._limit_handle.cancel()
            self._chunks.close()
            self._store.close()

    def _call_allowed(self, call_id):
        return self._accepted_calls is None or call_id in self._accepted_calls

    def _require_call(self, call_id):
        if self._accepted_calls is not None and (
            call_id not in self._accepted_calls
            or self._store.get_binding(call_id) != self._accepted_calls[call_id]
        ):
            raise RuntimeOperationError("Runtime call is outside its accepted scope") from None

    def _require_interactive_call(self, call_id):
        self._require_call(call_id)
        if call_id in self._recovery_calls:
            raise RuntimeOperationError("Frozen recovery is not an interactive call") from None

    def _visible_calls(self, call_ids):
        return tuple(
            call_id
            for call_id in call_ids
            if self._call_allowed(call_id)
            and ((policy := self._store.get_call_policy(call_id)) is None or policy.is_agent)
        )

    def _limit_call_ids(self):
        values = (
            self._store.limit_call_ids()
            if self._accepted_calls is None
            else self._store.limit_call_ids(
                call_ids=set(self._accepted_calls) - self._recovery_calls
            )
        )
        return self._visible_calls(values)

    def _arm_retention(self, delay):
        if self._audio_retention_days > 0 and not self._worker_stopping:
            self._retention_handle = self._worker_loop.call_later(delay, self._retention_tick)

    def _cancel_retention(self):
        if self._retention_handle is not None:
            self._retention_handle.cancel()
            self._retention_handle = None

    def _retention_tick(self):
        self._retention_handle = None
        if self._worker_stopping:
            return
        result = self._prune_audio()
        if result is None or result["reclaim"] in {"skipped_active", "skipped_busy"}:
            delay = self._retention_busy_delay
        elif result["has_more"]:
            delay = self._retention_backlog_delay
        else:
            delay = self._retention_interval
        self._arm_retention(delay)

    def _prune_audio(self, now=None):
        # Wait for callbacks whose real cleanup can outlive their durable outcome.
        if (
            self._background
            or self._active_audio_jobs
            or self._store._active_agents
            or self._store._active_queries
        ):
            return None
        return self._store.prune_expired_audio(
            retention_days=self._audio_retention_days, now=now, idle_only=True
        )

    def _speech_activity(self, call_id):
        if self._limits and self._limits.idle_seconds and not self._store.is_call_closing(call_id):
            self._last_speech_at[call_id] = storage.time()

    def _speech_progress(self, call_id):
        self._notify_state(call_id, speech=True)

    def _idle_state(self, call_id, entered_at, now):
        in_flight = (
            call_id in self._chunks.pending
            or any(
                target == call_id
                for index in (
                    self._active_audio_jobs,
                    self._store._active_agents,
                    self._active_tools,
                    self._barge_tasks,
                )
                for target, _ in index
            )
            or self._store.speech_in_flight(call_id)
        )
        if in_flight:
            return None
        last = max(
            entered_at,
            self._last_speech_at.get(call_id, entered_at),
            self._store.speech_activity(call_id) or entered_at,
        )
        return max(0, now - last)

    def _idle_remaining(self, call_id, saved, now):
        if not (saved and self._limits and self._limits.idle_seconds):
            return None
        idle = self._idle_state(call_id, saved.entered_at, now)
        return None if idle is None else self._limits.idle_seconds - idle

    def _arm_limits(self):
        if self._limit_handle is not None:
            self._limit_handle.cancel()
            self._limit_handle = None
        if self._limits is None or self._worker_stopping:
            return
        try:
            delay = self._store.next_limit_delay(
                call_ids=set(self._limit_call_ids()),
                idle_remaining=self._idle_remaining
                if self._limits.idle_seconds is not None
                else None,
            )
            if delay is not None:
                timer = self._limit_timer or self._worker_loop.call_later
                self._limit_handle = timer(delay, self._deadline_fired)
            self._limit_error = None
        except Exception:
            self._limit_error = "unconfirmed"  # No busy-loop or hidden retry.

    def _deadline_fired(self):
        self._limit_handle = None
        if not self._worker_stopping:
            self._spawn(self._limit_tick(), "deadline")

    def _close_prompt(self, call_id):
        return saved_or_default_prompt(
            self._store, call_id, self._call_review_prompts.get(call_id, self._review_prompt)
        )

    def _sweep_limits(self, *, recovering=False, only=None):
        for call_id in self._limit_call_ids() if only is None else (only,):
            self._require_call(call_id)
            if self._store.is_call_closing(call_id):
                continue
            now = storage.time()
            saved = self._store.get_call_limits(call_id)
            idle = self._idle_remaining(call_id, saved, now)
            idle_due = idle is not None and idle <= 0 and idle < min(saved.remaining(now))
            job = self._store.end_call(
                call_id,
                prompt=self._close_prompt(call_id),
                _request_close=True,
                _deadline_only=not idle_due,
                _close_reason="duration" if idle_due else None,
            )
            if idle_due:
                self._close_details[call_id] = "idle"
            if job is not None:
                self._chunks.discard(call_id)
            self._notify_state(call_id)
            if job is not None and not recovering and call_id not in self._recovered_limit_calls:
                # Already-owned live audio continues through its old barrier.
                # Never claim old pending audio simply because a timer fired.
                self._schedule_progress()
                if self._store.audio_missing_enabled(call_id):
                    self._schedule_audio(call_id)

    async def _limit_tick(self):
        try:
            self._sweep_limits()
        except Exception:
            self._limit_error = "unconfirmed"
            raise RuntimeOperationError("call_limits.unconfirmed") from None
        else:
            self._limit_error = None
            self._arm_limits()

    def _limit_view(self, call_id):
        saved = self._store.get_call_limits(call_id)
        if saved is None:
            return None
        return replace(
            saved.public(
                storage.time(), enforced=self._limits is not None, error=self._limit_error
            ),
            close_detail=self._close_details.get(call_id),
        )

    def _active_line(self, ref=None):
        now = storage.time()
        for row in self._store.active_web_calls(now):
            call_id = row["call_id"]
            if ref is not None and _management_ref(call_id) != ref:
                continue
            entered_at = row["entered_at"]
            closing = row["requested_at"] is not None
            # A persisted session still owns the SQLite slot after reload, even
            # when its web credentials and provider adapters cannot be restored.
            if (entered_at is None or closing) and not (
                call_id in self._close_details or self._visible_calls((call_id,))
            ):
                continue
            if closing and not (
                row["closing_work"] or any(key[0] == call_id for key in self._store._active_agents)
            ):
                continue
            state = "closing" if closing else "invited" if entered_at is None else "open"
            return call_id, state, row, now
        return None

    def _active_call_view(self, line):
        call_id, state, row, now = line
        created_at, entered_at = row["created_at"], row["entered_at"]
        saved = self._store.get_call_limits(call_id)
        idle = self._idle_state(call_id, entered_at, now) if state == "open" else None
        timeout = self._limits.idle_seconds if self._limits else None
        return dict(
            ref=_management_ref(call_id),
            state=state,
            created_at=created_at,
            entered_at=entered_at,
            elapsed_seconds=max(0, now - (created_at if entered_at is None else entered_at)),
            idle_seconds=idle,
            remaining_seconds=max(0, saved.remaining(now)[0]) if saved else None,
            idle_remaining_seconds=max(0, timeout - idle) if timeout and idle is not None else None,
            turn_count=row["turn_count"],
            tool_processing=self._tool_processing(call_id),
            can_force_hangup=state != "closing",
        )

    def _force_end_call(self, ref):
        line = self._active_line(ref)
        if line is None:
            return dict(code="not_found")
        call_id, state, _, _ = line
        if state == "closing":
            return dict(code="already_closing")
        job = self._store.end_call(
            call_id, prompt=self._close_prompt(call_id), _request_close=True, _close_reason="manual"
        )
        self._close_details[call_id] = "admin_forced"
        self._chunks.discard(call_id)
        replies = set(self._reply_tasks.values())
        for index in (self._segment_tasks, self._speech_work, self._barge_tasks):
            for (target, _), task in tuple(index.items()):
                if target == call_id and task not in replies:
                    task.cancel()
        for (target, gid), task in tuple(self._reply_tasks.items()):
            if target == call_id:
                if callable(self._callbacks.interrupt):
                    self._spawn(
                        self._bridge("interrupt", self._store.get_binding(call_id), gid),
                        "interrupt",
                    )
                else:
                    task.cancel()
        self._notify_state(call_id)
        if self._call_allowed(call_id):
            self._schedule_audio(call_id)
            self._schedule_progress()
        return dict(
            state="ended",
            review_expected=not isinstance(job, ClosingReceipt) or job.state != "closed_no_text",
        )

    def _spawn(self, coroutine, name, result=None):
        task = asyncio.create_task(coroutine, name="BotCall:" + name)
        self._owned.add(task)
        if result is None:
            self._background.add(task)

        def finished(done):
            self._owned.discard(done)
            self._background.discard(done)
            if done.cancelled():
                if result is not None and not result.done():
                    result.set_exception(asyncio.CancelledError())
            else:
                error = done.exception()  # Always collect owned background failures.
                if result is not None and not result.done():
                    if error is not None:
                        result.set_exception(
                            AccessError(error.code)
                            if isinstance(error, AccessError)
                            else AudioError(error.code)
                            if isinstance(error, AudioError)
                            else RuntimeOperationError("Runtime operation failed")
                        )
                    else:
                        result.set_result(done.result())

        task.add_done_callback(finished)
        return task

    def _launch(self, operation, args, result):
        terminal_query = operation == "timing_write" or (
            operation == "agent_journal"
            and args[0] in {"finish", "read", "report", "stream_done", "failure", "tool_failed"}
        )
        if self._worker_stopping and not terminal_query:
            result.set_exception(RuntimeClosed("Runtime is closing or closed"))
            return
        self._spawn(self._operation(operation, args), "request", result)

    async def _request(self, operation, *args):
        self._check_loop()
        # Agent finalization needs its owned callback. Telemetry is flushed
        # before the still-owned storage bridge returns, even after callback exit.
        terminal_query = operation == "timing_write" or (
            operation == "agent_journal"
            and args[0] in {"finish", "read", "report", "stream_done", "failure", "tool_failed"}
            and bool(self._core_tasks)
        )
        if (
            (not self._accepting or self._closing)
            and not terminal_query
            or not self._thread.is_alive()
        ):
            raise RuntimeClosed("Runtime is not accepting requests")
        result = Future()
        result.add_done_callback(_consume)
        self._worker_loop.call_soon_threadsafe(self._launch, operation, args, result)
        wrapped = asyncio.wrap_future(result)
        wrapped.add_done_callback(_consume)
        # A page cancelling its wait must not cancel the accepted operation.
        # asyncio.wait also avoids Python 3.14 shield's extra exception-reporting
        # callback when a disconnected waiter later fails reauthentication.
        await asyncio.wait({wrapped})
        return wrapped.result()

    async def create_call(
        self,
        binding: CallBinding,
        *,
        background: str | None = None,
        policy: CallPolicy | None = None,
        retained_missing: bool = False,
        query_policy: str | None = None,
        review_prompt: str | None = None,
    ):
        return await self._request(
            "create", binding, background, policy, retained_missing, query_policy, review_prompt
        )

    async def active_call_count(self):
        return await self._request("management_active_calls")

    async def force_end_call(self, ref):
        # Resolve and close on the same worker turn, so the ref cannot race a new call.
        return await self._request("force_end_call", ref)

    async def preview_review(self, call_id, policy):
        return await self._request("review_preview", call_id, policy)

    async def benchmark_reviews(self):
        return await self._request("benchmark_reviews")

    async def diagnostic_reviews(self):
        return await self._request("review_diagnostics")

    async def set_review_delivery(self, call_id, *, context_only):
        return await self._request("review_delivery", call_id, context_only)

    async def benchmark_snapshot(self):
        return await self._request("benchmark_read")

    async def benchmark_origin(self):
        """Latest call's source origin for provider selection only; never returned to clients."""
        return await self._request("benchmark_origin")

    async def diagnostic_timings(self):
        return await self._request("timing_read")

    async def agent_journal(self, action, call_id, *args):
        return await self._request("agent_journal", action, call_id, *args)

    async def query_journal(self, action, call_id, *args):
        return await self._request("query_journal", action, call_id, *args)

    async def audio_missing_enabled(self, call_id):
        return await self._request("audio_missing_policy", call_id)

    async def read_call_policy(self, call_id: str) -> CallPolicy | None:
        """Trusted read only; never registers a call or schedules its tasks."""
        return await self._request("call_policy", call_id)

    async def inspect_frozen_call(self, call_id: str):
        """Trusted read-only inspection; never registers scope or changes claims."""
        return await self._request("inspect_frozen", call_id)

    async def recover_frozen_call(
        self, binding: CallBinding, policy: CallPolicy, review: ReviewJob
    ):
        """Trusted host only, AFTER its callback mappings/capabilities are ready."""
        return await self._request("recover_frozen", binding, policy, review)

    async def inspect_recovery_call(self, call_id: str):
        """Trusted read: frozen review OR an existing persisted audio close."""
        return await self._request("inspect_recovery", call_id)

    async def recover_closing_call(
        self, binding: CallBinding, policy: CallPolicy, source: ClosingSource
    ):
        """Trusted host registered saved STT/review capabilities before this call."""
        return await self._request("recover_closing", binding, policy, source)

    def _closing_source(self, state, policy):
        close = state.close_request
        if (
            close is None
            or close.call_id != state.binding.call_id
            or type(close.prompt) is not str
            or not math.isfinite(close.requested_at)
            or not state.audio_jobs
            or (
                state.review is not None
                and (
                    state.review.prompt
                    != self._store.review_prompt_for_close(
                        state.binding.call_id, close.prompt, state.audio_jobs
                    )
                    or state.review.ended_at != close.requested_at
                )
            )
        ):
            raise RuntimeOperationError("No matching persisted audio close")
        turns = {
            turn.sequence: turn
            for turn in (state.raw_turns if state.raw_turns is not None else state.turns)
        }
        sources = []
        limit = policy.audio.max_input_bytes
        for ordinal, job in enumerate(state.audio_jobs, 1):
            if (
                job.call_id != state.binding.call_id
                or job.ordinal != ordinal
                or type(job.utterance_id) is not str
                or not 1 <= len(job.utterance_id) <= 128
                or job.utterance_id.isspace()
                or job.mime not in AUDIO_MIMES
                or not 0 < job.byte_count <= limit
                or len(job.sha256) != 64
                or any(c not in "0123456789abcdef" for c in job.sha256)
                or not math.isfinite(job.received_at)
            ):
                raise RuntimeOperationError("Audio source identity was rejected")
            if job.state == "succeeded":
                turn = turns.get(job.user_sequence)
                if (
                    job.data is not None
                    or turn is None
                    or turn.call_id != job.call_id
                    or turn.role != "user"
                    or turn.material_id != job.utterance_id
                    or turn.source != "audio_transcription"
                    or hashlib.sha256(turn.body.encode("utf-8")).hexdigest() != job.result_sha256
                ):
                    raise RuntimeOperationError("Completed audio material was rejected")
            elif not (job.data == b"" and audio_settled(job)) and (
                type(job.data) is not bytes
                or len(job.data) != job.byte_count
                or hashlib.sha256(job.data).hexdigest() != job.sha256
            ):
                raise RuntimeOperationError("Received audio material was rejected")
            sources.append(
                (
                    job.call_id,
                    job.utterance_id,
                    job.ordinal,
                    job.mime,
                    job.sha256,
                    job.byte_count,
                    job.received_at,
                    job.series_id,
                    job.series_index,
                    job.series_final,
                    job.series_limit,
                    job.series_end_index,
                    job.series_disposition,
                )
            )
        return ClosingSource(close, tuple(sources))

    async def _inspect_recovery(self, call_id):
        state = await self._operation("state", (call_id,))
        policy = self._store.get_call_policy(call_id)
        if policy is None or policy.binding != state.binding:
            raise RuntimeOperationError("Recovery requires its original saved policy")
        if state.review is not None:
            state, policy = await self._inspect_frozen(call_id, state=state, policy=policy)
            return state, policy, None
        return state, policy, self._closing_source(state, policy)

    async def _inspect_frozen(self, call_id, *, state=None, policy=None):
        if state is None:
            state = await self._operation("state", (call_id,))
            policy = self._store.get_call_policy(call_id)
        if (
            policy is None
            or policy.binding != state.binding
            or state.review is None
            or any(not audio_settled(job) for job in state.audio_jobs)
            or (
                state.review_work is not None
                and state.review_work.manifest != policy.review_manifest
            )
        ):
            raise RuntimeOperationError("Call has no matching frozen recovery source") from None
        self._store.get_review_input(call_id)  # Existing frozen fingerprint/structure validation.
        return state, policy

    async def submit_utterance(
        self, call_id: str, utterance_id: str, text: str, *, source="simulated_stt"
    ) -> Turn:
        """ACK only the committed user turn; the owned reply continues separately."""
        return await self._request("submit", call_id, utterance_id, text, source)

    async def end_call(
        self, call_id: str, *, prompt: str = _USE_DEFAULT_PROMPT
    ) -> ReviewJob | ClosingReceipt:
        """ACK the committed end request; received audio can keep input unfrozen."""
        return await self._request("end", call_id, prompt)

    async def issue_web_invitation(
        self, call_id: str, secret: InvitationSecret, *, session_ttl: int
    ):
        """Trusted code only; retain secret for stable issuance/ACK-loss recovery."""
        return await self._request("web_invite", call_id, secret, session_ttl)

    async def exchange_web_invitation(self, token: str):
        return await self._request("web_exchange", token)

    async def web_state(
        self, token: str, reference: str, wait_ms=0, utterance_id=None
    ) -> WebCallState:
        if (
            type(wait_ms) is not int
            or not 0 <= wait_ms <= 8000
            or (utterance_id is None and wait_ms != 0)
            or (
                utterance_id is not None
                and (
                    type(utterance_id) is not str
                    or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", utterance_id) is None
                )
            )
        ):
            raise AccessError("invalid_request") from None
        return await self._request("web_state", token, reference, wait_ms, utterance_id)

    async def web_metrics(self, token, reference, gid, values):
        return await self._request("web_metrics", token, reference, gid, values)

    async def web_submit(self, token: str, reference: str, utterance_id: str, text: str):
        return await self._request("web_submit", token, reference, utterance_id, text)

    @property
    def audio_available(self):
        return self._audio is not None

    async def web_audio(
        self, token: str, reference: str, utterance_id: str, data: bytes, mime: str, series=None
    ):
        return await self._request("web_audio", token, reference, utterance_id, data, mime, series)

    async def web_audio_chunk(self, token, reference, utterance_id, seq, offset, data, mime):
        return await self._request(
            "web_audio_chunk", token, reference, utterance_id, seq, offset, data, mime
        )

    async def web_audio_commit(
        self,
        token,
        reference,
        utterance_id,
        total_bytes,
        sha256,
        mime,
        offset=None,
        tail=None,
        series=None,
    ):
        return await self._request(
            "web_audio_commit",
            token,
            reference,
            utterance_id,
            total_bytes,
            sha256,
            mime,
            offset,
            tail,
            series,
        )

    async def web_audio_series_control(self, token, reference, sid, action, last_index, reason):
        return await self._request(
            "web_audio_series_control", token, reference, sid, action, last_index, reason
        )

    async def web_audio_resolution(self, token, reference, utterance_id):
        if (
            type(utterance_id) is not str
            or not 1 <= len(utterance_id) <= 128
            or utterance_id.isspace()
        ):
            raise AudioError("invalid")
        return await self._request("web_audio_resolution", token, reference, utterance_id)

    async def web_heartbeat(self, token: str, reference: str):
        return await self._request("web_heartbeat", token, reference)

    async def web_playback(
        self,
        token: str,
        reference: str,
        generation_id: str,
        revision: int,
        state: str,
        first_sound_ms=None,
    ):
        return await self._request(
            "web_playback", token, reference, generation_id, revision, state, first_sound_ms
        )

    async def speech_open(self, binding, generation_id):
        return await self._request("speech_open", binding, generation_id)

    async def web_speech(self, token, reference, generation_id, wait_ms=0):
        if type(wait_ms) is not int or not 0 <= wait_ms <= 8000:
            raise AccessError("invalid_request") from None
        return await self._request("web_speech", token, reference, generation_id, wait_ms)

    async def stop_speech_waits(self):
        """Wake only bounded wait wrappers; actual SDK ownership is unchanged."""
        self._check_loop()
        if not self._accepting or self._closing:
            return
        await self._request("stop_speech_waits")

    async def stop_state_waits(self):
        """Wake read wrappers before backend web-task gather, without SDK cancellation."""
        self._check_loop()
        if not self._accepting or self._closing:
            return
        await self._request("stop_state_waits")

    async def web_speech_audio(self, token, reference, generation_id):
        return await self._request("web_speech_audio", token, reference, generation_id)

    async def web_end(self, token: str, reference: str):
        return await self._request("web_end", token, reference)

    async def resume_pending(self) -> None:
        """Explicit finite progress request; no startup replay or polling."""
        await self._request("resume")

    async def review_retry_info(self, call_id):
        return await self._request("review_retry_info", call_id)

    async def resolve_review_ref(self, reference):
        return await self._request("resolve_review_ref", reference)

    async def retry_review(self, call_id, policy, counter_rule, adapter, *, preview, context_only):
        manifest = ReviewManifest.from_policy(policy, counter_rule)
        if not preview:
            self._retry_adapters[(call_id, manifest.fingerprint)] = adapter
        return await self._request("retry_review", call_id, policy, manifest, preview, context_only)

    async def read_state(self, call_id: str) -> CallState:
        return await self._request("state", call_id)

    async def prune_audio(self, now=None):
        """Run one retention pass on the storage worker when it is idle."""
        return await self._request("prune_audio", now)

    async def drain(self) -> None:
        """Wait for the currently owned tasks; this is not a success assertion."""
        await self._request("drain")

    async def _operation(self, operation, args):
        try:
            if operation == "prune_audio":
                return self._prune_audio(*args)
            if operation == "management_active_calls":
                rows = self._store._db.execute(
                    "SELECT call_id FROM calls c WHERE NOT EXISTS(SELECT 1 FROM call_closures x WHERE x.call_id=c.call_id) AND NOT EXISTS(SELECT 1 FROM reviews r WHERE r.call_id=c.call_id)"
                ).fetchall()
                return len(self._visible_calls([row[0] for row in rows]))
            if operation == "force_end_call":
                return self._force_end_call(args[0])
            if operation == "resolve_review_ref":
                matches = [
                    c for c in self._store.review_call_ids() if _management_ref(c) == args[0]
                ]
                return matches[0] if len(matches) == 1 else None
            if operation == "review_retry_info":
                state, saved = await self._inspect_frozen(args[0])
                from .review_retry import status

                return (
                    state.binding,
                    saved,
                    status(
                        self._store,
                        args[0],
                        allowed=self._call_allowed(args[0]),
                        running=args[0] in self._review_tasks,
                    ),
                )
            if operation == "retry_review":
                from .review_retry import AttemptStore, latest, status, create_attempt
                from .review_plan import build_review_plan

                call_id, policy, manifest, preview, context_only = args
                if call_id in self._review_tasks:
                    return dict(code="running")
                if await self._operation("management_active_calls", ()):
                    return dict(code="busy")
                state, saved = await self._inspect_frozen(call_id)
                info = status(self._store, call_id, allowed=self._call_allowed(call_id))
                if not info["retryable"] and info["state"] != "succeeded":
                    return dict(code="not_retryable")
                # A successful generation only resumes stable publication keys.
                previous = latest(self._store, call_id)
                view = (
                    AttemptStore(self._store, previous["attempt_id"]) if previous else self._store
                )
                done = view.get_review(call_id).state == "succeeded"
                if done:
                    if not previous:
                        return dict(code="not_retryable")
                    try:
                        publication = view.get_publication(call_id)
                        if (
                            not preview
                            and publication.context_state == "succeeded"
                            and (
                                context_only
                                or all(p.state == "succeeded" for p in publication.pages)
                            )
                        ):
                            return dict(state="succeeded", detail="已通过重试补做，无需重复投递")
                    except NotFound:
                        pass
                plan = (
                    None
                    if done
                    else build_review_plan(
                        state.review,
                        input_token_budget=policy.input_token_budget,
                        count_tokens=policy.count_request,
                    )
                )
                count = len(plan.chunks) if plan else 0
                if plan is not None and not count:
                    return dict(code="not_retryable")
                model = json.loads(manifest.canonical_json)["model"] if not done else info["model"]
                result = dict(
                    model=model,
                    llm_calls_min=count + 1 if count else 0,
                    llm_calls_max=2 * count,
                    may_rebill=(
                        info["reason"] in {"outcome_uncertain", "work_running"}
                        or bool(
                            self._store._db.execute(
                                "SELECT 1 FROM review_work_nodes WHERE call_id=? AND state='succeeded' LIMIT 1",
                                (call_id,),
                            ).fetchone()
                        )
                        or bool(
                            self._store._db.execute(
                                "SELECT 1 FROM review_retry_nodes n JOIN review_retry_attempts a USING(attempt_id) WHERE a.call_id=? AND n.state='succeeded' LIMIT 1",
                                (call_id,),
                            ).fetchone()
                        )
                    )
                    if not done
                    else False,
                    context_only=context_only,
                    notes=[
                        "原模型 "
                        + (info["model"] or "未知")
                        + "（"
                        + info["detail"]
                        + "）→ 将使用 "
                        + model,
                        "重新生成全部阶段；旧尝试与模型记录保留"
                        if not done
                        else "仅继续未投递部分，不重新生成",
                        "只写入原会话上下文，不发 QQ"
                        if context_only
                        else "写入原会话上下文，并补发尚未投递的 QQ 页",
                        "调用数为插件调用次数，Provider 内部重试可能增加实际计费",
                    ],
                )
                if preview:
                    return result
                if not done:
                    view = create_attempt(self._store, call_id, manifest, context_only=context_only)
                if not context_only:
                    with self._store._transaction():
                        self._store._db.execute(
                            "UPDATE review_retry_attempts SET send_requested_at=coalesce(send_requested_at,?) WHERE attempt_id=?",
                            (time.time(), view.attempt_id),
                        )
                self._review_tasks[call_id] = self._spawn(
                    self._retry_call(call_id, view, policy, manifest, context_only, done),
                    "review-retry",
                )
                return dict(state="running", detail="已登记回顾重试，请刷新查看结果")
            if operation == "review_preview":
                call_id, policy = args
                state, saved = await self._inspect_frozen(call_id)
                from .review_policy import ReviewManifest

                if (
                    ReviewManifest.from_policy(policy, saved.review_manifest.counter_rule_id)
                    != saved.review_manifest
                ):
                    raise RuntimeOperationError("Review policy changed")
                work = state.review_work
                count = work.map_count if work else 0
                reason, lower, upper = "complete", 0, 0
                if state.review.state != "succeeded":
                    if (work is None and state.review.state != "pending") or (
                        work is not None and work.state != "pending"
                    ):
                        reason = "outcome_not_replayable"
                    else:
                        if work is None:
                            from .review_plan import build_review_plan

                            count = len(
                                build_review_plan(
                                    state.review,
                                    input_token_budget=policy.input_token_budget,
                                    count_tokens=policy.count_request,
                                ).chunks
                            )
                            lower = count + 1
                        else:
                            lower = sum(n.state == "pending" for n in work.frontier) + (
                                work.kind != "final"
                            )
                        upper, reason = 2 * count, "pending"
                return dict(
                    call_id=call_id,
                    reason=reason,
                    map_count=count,
                    llm_calls_min=lower,
                    llm_calls_max=upper,
                )
            if operation == "review_delivery":
                call_id, context_only = args
                if type(context_only) is not bool or (
                    call_id in self._review_tasks
                    and context_only != (call_id in self._review_context_only)
                ):
                    raise RuntimeOperationError("Review delivery is already active")
                if context_only:
                    self._review_context_only.add(call_id)
                else:
                    self._review_context_only.discard(call_id)
                return None
            if operation in {"review_diagnostics", "benchmark_reviews"}:
                counts, pending, rows = {}, 0, []
                for call_id in self._store.review_call_ids():
                    review = self._store.get_review(call_id)
                    work = self._store.get_review_work(call_id)
                    if review.state == "succeeded":
                        continue
                    pending += 1
                    from .review_retry import status

                    info = status(
                        self._store,
                        call_id,
                        allowed=self._call_allowed(call_id),
                        running=call_id in self._review_tasks,
                    )
                    if info["state"] == "succeeded":
                        pending -= 1
                    elif info["reason"] == "unresolved" and self._review_routes is not None:
                        reason = self._policy_state(call_id, review, work).status
                        from .benchmark_run import REASON_LABELS

                        info.update(reason=reason, detail=REASON_LABELS.get(reason, "等待处理"))
                    reason = info["reason"]
                    if info["state"] != "succeeded":
                        counts[reason] = counts.get(reason, 0) + 1
                    if operation == "benchmark_reviews":
                        rows.append(dict(ref=_management_ref(call_id), **info))
                    else:
                        rows.append(
                            dict(
                                call_id=call_id,
                                log_ref=hashlib.sha256(call_id.encode()).hexdigest()[:12],
                                state=info["state"],
                                reason=reason,
                                work_state=info["work_state"],
                            )
                        )
                public = operation == "benchmark_reviews"
                return dict(
                    pending_count=pending,
                    reasons=counts,
                    calls=list(reversed(rows[-20:])) if public else rows[-100:],
                    truncated=len(rows) > (20 if public else 100),
                )
            if operation == "benchmark_origin":
                row = self._store._db.execute(
                    "SELECT unified_msg_origin FROM calls ORDER BY created_at DESC,rowid DESC LIMIT 1"
                ).fetchone()
                return row[0] if row else None
            if operation == "benchmark_read":
                from .benchmark import snapshot

                data = snapshot(self._store._db)
                line = self._active_line()
                data["active_calls"] = [self._active_call_view(line)] if line else []
                return data
            if operation == "timing_read":
                return self._store.timing_snapshot()
            if operation == "timing_write":
                self._require_call(args[0])
                return self._store.record_timings(*args)
            if operation == "agent_journal":
                action, call_id, *values = args
                self._require_call(call_id)
                if self._worker_stopping and (
                    not values or (call_id, values[0]) not in self._store._active_agents
                ):
                    raise StateConflict("agent.scope_ended")
                if action == "failure":
                    from .failures import SERVICES, CATEGORIES

                    gid, service, code = values
                    if service in SERVICES and code in CATEGORIES:
                        self._store.record_timings(
                            call_id,
                            gid,
                            [
                                dict(
                                    stage=f"error_{service}_{code}",
                                    sequence=0,
                                    start_ms=0,
                                    elapsed_ms=0,
                                )
                            ],
                        )
                        self._notify_state(call_id)
                    return
                if action in {"segment", "stream_done"}:
                    return self._segment_action(action, call_id, values)
                if action == "read":
                    return self._store.agent_record(call_id, *values)
                if action == "cancelled_material":
                    return self._store.save_cancelled_material(call_id, *values)
                if action == "projection":
                    return self._store.playback_projection(
                        call_id, self._store.effective_transcript(call_id)
                    )
                if action == "reports":
                    return self._store.agent_reports(call_id)
                if action == "claim":
                    self._segment_started[(call_id, values[0])] = asyncio.get_running_loop().time()
                    return self._store.claim_agent(call_id, *values)
                if action == "enter":
                    return self._store.enter_agent(call_id, *values)
                if action == "report":
                    gid, report = values
                    self._store.report_agent(call_id, gid, report)
                    kind = json.loads(report)["kind"]
                    key = (call_id, gid)
                    if (
                        kind == "tool_start"
                        and key in self._store._active_agents
                        and not self._worker_stopping
                        and not self._store.is_call_closing(call_id)
                    ):
                        self._active_tools[key] = self._active_tools.get(key, 0) + 1
                        self._notify_state(call_id)
                    elif kind == "framework_report":
                        self._end_tool(call_id, gid)
                    return
                if action == "tool_failed":
                    self._end_tool(call_id, values[0])
                    return
                if action == "finish":
                    try:
                        return self._store.finish_agent(call_id, *values)
                    finally:
                        if self._active_tools.pop((call_id, values[0]), 0):
                            self._notify_state(call_id)
                raise StateConflict("agent.invalid_action")
            if operation == "query_journal":
                action, call_id, *values = args
                self._require_call(call_id)
                if action == "policy":
                    return self._store.get_query_policy(call_id)
                if action == "read":
                    return self._store.query_record(call_id, *values)
                raise StateConflict("legacy.read_only")
            if operation == "stop_state_waits":
                self._business.call_soon_threadsafe(self._close_push)
                self._state_wait_stop.set()
                return
            if operation == "stop_speech_waits":
                self._speech_wait_stop.set()
                return
            if operation == "pcm_claim":
                binding, gid, seq = args
                with self._store._transaction():
                    if (
                        self._store.get_binding(binding.call_id) != binding
                        or self._store.is_call_closing(binding.call_id)
                        or not any(
                            r["seq"] == 0 and r["state"] in {"open", "complete"}
                            for r in self._store.segments(binding.call_id, gid)
                        )
                    ):
                        return False
                    # Only a socket-owned positive receipt marks an attempt.
                    # An enqueue, rejection or disconnect cannot claim playback.
                    self._store._db.execute(
                        "UPDATE speech_segments SET revision=-1 WHERE call_id=? AND generation_id=? AND seq=? AND revision=0 AND playback_state='unknown' AND state IN ('running','succeeded')",
                        (binding.call_id, gid, seq),
                    )
                    return any(
                        r["seq"] == seq and (r["revision"] == -1 or r["revision"] > 0)
                        for r in self._store.segments(binding.call_id, gid)
                    )
            if operation == "segment_open":
                binding, gid, seq = args
                return self._store.get_binding(
                    binding.call_id
                ) == binding and self._store.segment_open(binding.call_id, gid, seq)
            if operation == "speech_open":
                binding, generation_id = args
                return (
                    self._store.get_binding(binding.call_id) == binding
                    and not self._store.is_call_closing(binding.call_id)
                    and self._store.get_speech(binding.call_id, generation_id).state == "running"
                )
            if operation == "web_invite":
                self._require_interactive_call(args[0])
                grant = self._store.issue_web_invitation(args[0], args[1], session_ttl=args[2])
                if self._accepted_calls is not None:
                    # Target of this worker's confirmed trusted issuance, never
                    # a plaintext token or a browser-specified call id.
                    self._issued_invites[token_digest(args[1].value, "invitation")] = args[0]
                return grant
            if operation == "web_exchange":
                if self._accepted_calls is not None:
                    call_id = self._issued_invites.get(token_digest(args[0], "invitation"))
                    if call_id is None:
                        raise AccessError() from None
                    self._require_interactive_call(call_id)
                return self._store.exchange_web_invitation(args[0], _limits=self._limits)
            if operation in {
                "web_barge_in",
                "web_state",
                "web_submit",
                "web_end",
                "web_audio",
                "web_audio_chunk",
                "web_audio_commit",
                "web_playback",
                "web_heartbeat",
                "web_speech",
                "web_speech_audio",
                "web_segment",
                "web_watch",
                "web_metrics",
                "web_audio_resolution",
                "web_audio_series_control",
            }:
                # Target comes from worker authentication. Write authority is
                # rechecked inside the existing commit transaction after any
                # SQLite lock wait, with no external await under the transaction.
                call_id = self._store.authenticate_web_session(
                    args[0], args[1], submit=operation == "web_submit"
                )
                self._require_interactive_call(call_id)
                if operation == "web_barge_in":
                    from .barge_in import valid_request

                    if not valid_request(args[2]):
                        raise AccessError("invalid_request")
                    if not self._barge_in:
                        raise AccessError("conflict")
                    row = self._store.barge_command(call_id, args[2], (args[0], args[1]))
                    self._speech_activity(call_id)
                    if row["closed_reason"] not in {None, "resumed"}:
                        self._cancel_barge_audio(call_id, row)
                    self._notify_state(call_id)
                    return row
                if operation == "web_watch":
                    return call_id
                if operation == "web_metrics":
                    gid = args[2]
                    from .observability import (
                        TRANSPORT_STAGES,
                        CONNECTION_METRICS,
                        PLAYBACK_METRICS,
                        VAD_TIMES,
                        ECHO_METRICS,
                        echo_values_valid,
                        CAPABILITY_METRICS,
                        capability_values_valid,
                        CLIP_REJECT_METRICS,
                        client_metric_valid,
                    )

                    echo = gid.startswith("page:echo:") or bool(set(args[3]) & ECHO_METRICS.keys())
                    if (
                        echo
                        or gid.startswith("page:cap:")
                        or set(args[3]) & CAPABILITY_METRICS.keys()
                    ):
                        if not (echo_values_valid if echo else capability_values_valid)(
                            gid, args[3]
                        ):
                            raise StateConflict("invalid_metrics")
                        # Immutable first packet, including a retried request.
                        if self._store._db.execute(
                            "SELECT 1 FROM turn_timings WHERE call_id=? AND generation_id=? LIMIT 1",
                            (call_id, gid),
                        ).fetchone():
                            return {"recorded": True}
                    elif gid == "page":
                        if not set(args[3]) <= VAD_TIMES:
                            raise StateConflict("invalid_metrics")

                        gid = "page:" + hashlib.sha256(args[1].encode()).hexdigest()[:16]
                    elif gid.startswith("capture:"):
                        if (
                            re.fullmatch(r"capture:[A-Za-z0-9_-]{1,80}", gid) is None
                            or not args[3]
                            or not set(args[3])
                            <= CLIP_REJECT_METRICS.keys()
                            | {
                                "client_segment_handoff_ms",
                                "client_upload",
                                "client_upload_flush",
                                "client_upload_digest",
                                "client_upload_precheck",
                                "client_upload_post",
                                "client_upload_tail",
                                "client_upload_chunks",
                            }
                            or any(not client_metric_valid(k, v) for k, v in args[3].items())
                        ):
                            raise StateConflict("invalid_metrics")
                        if not set(args[3]) <= CLIP_REJECT_METRICS.keys():
                            if self._store.get_audio_job(call_id, gid[8:]).series_id is None:
                                raise StateConflict("invalid_metrics")
                    else:
                        self._store.get_generation(call_id, gid)

                    playback = set(args[3]) & PLAYBACK_METRICS.keys()
                    if playback and (
                        not set(args[3]) <= PLAYBACK_METRICS.keys()
                        or not 1 <= args[3].get("client_playback_sentence", 0) <= 512
                        or not gid.startswith("reply:")
                    ):
                        raise StateConflict("invalid_metrics")
                    rows = []
                    for key, value in args[3].items():
                        sequence = value if key in TRANSPORT_STAGES else 0
                        if key in PLAYBACK_METRICS:
                            sequence = args[3]["client_playback_sentence"]
                        if key in CONNECTION_METRICS or key == "client_barge_late_blocks":
                            latest = self._store._db.execute(
                                "SELECT sequence,elapsed_ms FROM turn_timings "
                                "WHERE call_id=? AND generation_id=? AND stage=? "
                                "ORDER BY sequence DESC LIMIT 1",
                                (call_id, gid, key),
                            ).fetchone()
                            if latest and (latest[1] == value or latest[0] >= 4096):
                                continue
                            sequence = latest[0] + 1 if latest else 0
                        rows.append(
                            dict(stage=key, sequence=sequence, start_ms=0, elapsed_ms=value)
                        )
                    self._store.record_timings(call_id, gid, rows)
                    return {"recorded": True}
                if operation == "web_segment":
                    if args[4] is None:
                        return self._store.segment_audio(
                            call_id, args[2], args[3], (args[0], args[1])
                        )
                    row = self._store.segment_playback(
                        call_id,
                        args[2],
                        args[3],
                        args[4]["revision"],
                        args[4]["state"],
                        (args[0], args[1]),
                    )
                    if row["advanced"]:
                        self._speech_activity(call_id)
                    if args[4]["state"] == "stopped" or (
                        args[4]["state"] == "failed" and args[3] != 1
                    ):
                        task = self._segment_tasks.get((call_id, args[2]))
                        if task is not None:
                            task.cancel()
                    self._notify_state(call_id)
                    return dict(
                        recorded=True,
                        generation_id=args[2],
                        seq=row["seq"],
                        revision=row["revision"],
                        state=row["playback_state"],
                        superseded=row["superseded"],
                    )
                if operation == "web_speech_audio":
                    return self._store.speech_audio(call_id, args[2], (args[0], args[1]))
                if operation == "web_speech":
                    if not callable(self._callbacks.synthesize):
                        raise AccessError("unconfirmed") from None
                    deadline = asyncio.get_running_loop().time() + args[3] / 1000
                    job, claimed = self._store.claim_speech(call_id, args[2], (args[0], args[1]))
                    key = (call_id, args[2])
                    if claimed:
                        task = self._spawn(self._synthesize(*key), "speech")
                        self._speech_work[key] = task
                        task.add_done_callback(lambda _: self._speech_work.pop(key))
                        if len(self._speech_tasks) < 4:
                            # No await between claim, spawn and index registration.
                            self._speech_tasks[key] = task

                            def finished(done):
                                if self._speech_tasks.get(key) is done:
                                    del self._speech_tasks[key]

                            task.add_done_callback(finished)
                    await self._wait_speech(key, deadline, args[3])
                    # Always reauthenticate after possible waits, without claiming
                    # again or copying the WAV into this status-only response.
                    return self._store.speech_status(call_id, args[2], (args[0], args[1]))
                if operation == "web_heartbeat":
                    if (
                        self._limits
                        and self._limits.idle_seconds
                        and not self._store.is_call_closing(call_id)
                    ):
                        saved = self._store.get_call_limits(call_id)
                        idle = self._idle_remaining(call_id, saved, storage.time())
                        if idle is not None and idle <= 0:
                            self._sweep_limits(only=call_id)
                    return self._store.heartbeat_web_session(
                        args[0], args[1], enabled=self._limits is not None
                    )
                if (
                    operation in {"web_submit", "web_audio", "web_audio_chunk", "web_audio_commit"}
                    and self._limits is None
                    and self._store.get_call_limits(call_id) is not None
                ):
                    raise AccessError("unconfirmed")
                if operation == "web_playback":
                    try:
                        turn, advanced = self._store.observe_playback(
                            call_id,
                            args[2],
                            revision=args[3],
                            state=args[4],
                            _web_authority=(args[0], args[1]),
                            _with_progress=True,
                        )
                        if (
                            args[4] == "started"
                            and turn.playback_state == "started"
                            and milliseconds(args[5], 3_600_000)
                        ):
                            try:
                                self._store.record_timings(
                                    call_id,
                                    args[2],
                                    [
                                        dict(
                                            stage="first_sound",
                                            sequence=0,
                                            start_ms=0,
                                            elapsed_ms=args[5],
                                        )
                                    ],
                                )
                            except Exception:
                                pass

                        if advanced:
                            self._speech_activity(call_id)
                        return turn
                    except (MaterialConflict, StateConflict, NotFound):
                        raise AccessError("conflict") from None
                if operation == "web_audio_series_control":
                    if self._audio is None:
                        raise AudioError("unavailable")
                    self._store.series_control(
                        call_id, *args[2:], _web_authority=(args[0], args[1])
                    )
                    self._speech_activity(call_id)
                    self._observe_series_seal(call_id, args[2])
                    self._notify_state(call_id)
                    self._schedule_audio(call_id)
                    return {
                        "recorded": True,
                        "series": next(
                            r
                            for r in self._store.series_views(call_id)
                            if r["series_id"] == args[2]
                        ),
                    }
                if operation == "web_audio_resolution":
                    job = self._retain_missing(call_id, args[2], authority=(args[0], args[1]))
                    self._schedule_audio(call_id)
                    return job.public()
                if operation in {"web_audio_chunk", "web_audio_commit"}:
                    if self._audio is None:
                        raise AudioError("unavailable")
                    if operation == "web_audio_chunk":
                        if self._store.is_call_closing(call_id) or self._store.limit_due(call_id):
                            self._chunks.discard(call_id)
                            raise AudioError("closed")
                        try:
                            self._store.get_audio_job(call_id, args[2])
                        except NotFound:
                            pass
                        else:
                            raise AudioError("conflict")
                        result = self._chunks.put(call_id, *args[2:])
                        self._speech_activity(call_id)
                        return result
                    ChunkUploads.identity(args[2], args[5])
                    if (
                        type(args[3]) is not int
                        or not 0 < args[3] <= self._audio.max_audio_bytes
                        or type(args[4]) is not str
                        or re.fullmatch(r"[0-9a-f]{64}", args[4]) is None
                    ):
                        raise AudioError(
                            "too_large"
                            if type(args[3]) is int and args[3] > self._audio.max_audio_bytes
                            else "invalid"
                        )
                    if args[6] is not None:
                        ChunkUploads.validate_tail(args[3], args[6], args[7])
                    try:
                        job = self._store.get_audio_job(call_id, args[2])
                    except NotFound:
                        job = None
                    if job is not None:
                        from .series import matches

                        matches(job, args[8])
                        if (job.byte_count, job.sha256, job.mime) != args[3:6]:
                            raise AudioError("conflict")
                        return self._audio_ack_job(
                            job
                        )  # Lost commit ACK: never claim or replay a paid request.
                    if self._store.is_call_closing(call_id) or self._store.limit_due(call_id):
                        self._chunks.discard(call_id)
                        raise AudioError("closed")
                    data = self._chunks.data(call_id, *args[2:8])
                    job = self._store.receive_audio(
                        call_id,
                        args[2],
                        data,
                        args[5],
                        _web_authority=(args[0], args[1]),
                        series=args[8],
                        series_limit=self._audio.max_continuous_segments,
                    )
                    self._chunks.discard(call_id, args[2])
                    self._speech_activity(call_id)
                    self._schedule_audio(call_id)
                    return self._audio_ack_job(job)
                if operation == "web_audio":
                    if self._audio is None:
                        raise AudioError("unavailable")
                    if (
                        type(args[3]) is not bytes
                        or not 0 < len(args[3]) <= self._audio.max_audio_bytes
                    ):
                        raise AudioError("invalid")
                    job = self._store.receive_audio(
                        call_id,
                        args[2],
                        args[3],
                        args[4],
                        _web_authority=(args[0], args[1]),
                        series=args[5],
                        series_limit=self._audio.max_continuous_segments,
                    )
                    self._chunks.discard(call_id, args[2])
                    self._speech_activity(call_id)
                    self._schedule_audio(call_id)
                    return self._audio_ack_job(job)
                if operation == "web_submit":
                    turn = self._store.append_user(
                        call_id,
                        args[2],
                        args[3],
                        source="web_text",
                        _web_authority=(args[0], args[1]),
                    )
                    self._speech_activity(call_id)
                    self._spawn(
                        process_utterance(
                            self._store, call_id, args[2], args[3], self._reply, source="web_text"
                        ),
                        "reply",
                    ).add_done_callback(lambda _: self._speech_progress(call_id))
                    return turn
                if operation == "web_end":
                    self._chunks.discard(call_id)
                    job = self._store.end_call(
                        call_id,
                        prompt=self._close_prompt(call_id),
                        _web_authority=(args[0], args[1]),
                        _request_close=True,
                    )
                    for (target, _), task in tuple(self._segment_tasks.items()):
                        if target == call_id:
                            task.cancel()
                    self._notify_state(call_id)
                    self._schedule_audio(call_id)
                    self._schedule_progress()
                    return job
                # Register/recheck persistent progress; no claim, resume or SDK.
                if args[3] is not None:
                    await self._wait_state((call_id, args[3]), args[2])
                # Accepted reads may outlive HTTP cancellation or credential TTL.
                if self._store.authenticate_web_session(args[0], args[1]) != call_id:
                    raise AccessError() from None
                self._require_interactive_call(call_id)
                state = await self._operation("state", (call_id,))
                generations = {g.user_sequence: g for g in state.generations}
                replies = []
                roots = {r.series_id for r in self._store.series_roots(call_id)}
                for turn in state.turns:
                    if turn.role != "user" or (
                        turn.material_id in roots and turn.sequence not in generations
                    ):
                        continue
                    generation = generations.get(turn.sequence)
                    replies.append(
                        WebReply(
                            turn.material_id,
                            turn.sequence,
                            generation.generation_id if generation else None,
                            generation.state if generation else "not_started",
                            generation.bot_sequence if generation else None,
                        )
                    )
                no_text_closed = (
                    state.close_request is not None
                    and state.review is None
                    and not state.turns
                    and all(audio_settled(job) for job in state.audio_jobs)
                )
                detail = self._close_details.get(call_id)
                return WebCallState(
                    state.review is not None or no_text_closed or detail == "admin_forced",
                    state.review.state if state.review else "not_started",
                    state.review_work.state if state.review_work else None,
                    tuple(
                        WebTurn(
                            t.sequence,
                            t.role,
                            t.body,
                            t.playback_state,
                            t.material_id,
                            t.playback_revision,
                        )
                        for t in state.turns
                    ),
                    self._store.publication_body(call_id)
                    if state.review and state.review.state == "succeeded"
                    else None,
                    tuple(page.state for page in state.publication.pages)
                    if state.publication
                    else (),
                    state.publication.context_state if state.publication else "not_prepared",
                    tuple(replies),
                    state.review_policy.status if state.review_policy else None,
                    tuple(job.public() for job in state.audio_jobs),
                    state.close_request is not None or state.review is not None,
                    "frozen"
                    if state.review
                    else "closed_no_text"
                    if no_text_closed
                    else closing_state(state.audio_jobs)
                    if state.close_request
                    else "open",
                    state.close_request.requested_at
                    if state.close_request
                    else state.review.ended_at
                    if state.review
                    else None,
                    self.audio_available
                    and not (
                        self._limits is None and self._store.get_call_limits(call_id) is not None
                    ),
                    self._limit_view(call_id),
                    callable(self._callbacks.synthesize),
                    self._store.audio_missing_enabled(call_id),
                    tuple(self._store.segments(call_id, captions=True)),
                    tuple(self._store.service_failures(call_id)),
                    call_id=call_id,
                    series=self._store.series_views(call_id),
                    barge_in=tuple(self._store.barge_rows(call_id)),
                    background_processing=self._background_processing(call_id),
                    tool_processing=self._tool_processing(call_id),
                    close_detail=detail,
                )
            if operation == "inspect_recovery":
                return await self._inspect_recovery(args[0])
            if operation == "recover_closing":
                bound, policy, original = args
                if (
                    self._accepted_calls is None
                    or self._review_routes is None
                    or self._audio is None
                ):
                    raise RuntimeOperationError(
                        "Closing recovery requires restricted audio/routed mode"
                    )
                state, saved, _ = await self._inspect_recovery(bound.call_id)
                if not saved.is_agent:
                    raise RuntimeOperationError("Legacy policy is read only")
                source = self._closing_source(state, saved)
                if (
                    state.binding != bound
                    or saved != policy
                    or source != original
                    or (
                        bound.call_id in self._accepted_calls
                        and bound.call_id not in self._recovery_calls
                    )
                ):
                    raise RuntimeOperationError("Closing recovery source changed")
                self._accepted_calls[bound.call_id] = bound
                self._recovery_calls.add(bound.call_id)
                if state.review is None:
                    self._closing_recovery_calls[bound.call_id] = (saved, source)
                    self._schedule_audio(bound.call_id)
                else:
                    self._closing_recovery_calls.pop(bound.call_id, None)
                    self._schedule_progress()
                return bound
            if operation == "inspect_frozen":
                return await self._inspect_frozen(args[0])
            if operation == "recover_frozen":
                bound, policy, original = args
                if self._accepted_calls is None or self._review_routes is None:
                    raise RuntimeOperationError("Frozen recovery requires restricted routed mode")
                state, saved = await self._inspect_frozen(bound.call_id)
                if not saved.is_agent:
                    raise RuntimeOperationError("Legacy policy is read only")
                fields = (
                    "call_id",
                    "ended_at",
                    "last_sequence",
                    "input_sha256",
                    "input_json",
                    "prompt",
                )
                if (
                    state.binding != bound
                    or saved != policy
                    or any(getattr(state.review, f) != getattr(original, f) for f in fields)
                    or (
                        bound.call_id in self._accepted_calls
                        and bound.call_id not in self._recovery_calls
                    )
                ):
                    raise RuntimeOperationError("Frozen recovery source changed")
                self._accepted_calls[bound.call_id] = bound
                self._recovery_calls.add(bound.call_id)
                self._closing_recovery_calls.pop(bound.call_id, None)
                self._schedule_progress()  # Existing pending-only state machines; no audio/timer restart.
                return bound
            if operation == "audio_missing_policy":
                return self._store.audio_missing_enabled(args[0])
            if operation == "call_policy":
                return self._store.get_call_policy(args[0])
            if operation == "create":
                if args[5] is not None and type(args[5]) is not str:
                    raise RuntimeOperationError("Invalid review prompt")
                old_prompt = self._call_review_prompts.get(args[0].call_id)
                if args[5] is not None and old_prompt is not None and args[5] != old_prompt:
                    raise RuntimeOperationError("Review prompt changed")
                bound = self._store.create_call(
                    args[0],
                    background=args[1],
                    policy=args[2],
                    retained_missing=args[3],
                    query_policy=args[4],
                )
                if args[5] is not None:
                    self._call_review_prompts[bound.call_id] = args[5]
                if callable(self._callbacks.synthesize):
                    self._store.enable_speech(bound.call_id)
                if self._accepted_calls is not None:
                    self._accepted_calls[bound.call_id] = bound  # Only after successful commit.
                return bound
            if operation == "submit":
                call_id, utterance_id, text, source = args
                self._require_interactive_call(call_id)
                turn = self._store.append_user(call_id, utterance_id, text, source=source)
                self._speech_activity(call_id)
                self._spawn(
                    process_utterance(
                        self._store, call_id, utterance_id, text, self._reply, source=source
                    ),
                    "reply",
                ).add_done_callback(lambda _: self._speech_progress(call_id))
                return turn
            if operation == "end":
                self._require_interactive_call(args[0])
                prompt = self._close_prompt(args[0]) if args[1] is _USE_DEFAULT_PROMPT else args[1]
                job = self._store.end_call(args[0], prompt=prompt, _request_close=True)
                self._chunks.discard(args[0])
                self._notify_state(args[0])
                self._schedule_audio(args[0])
                self._schedule_progress()
                return job
            if operation == "resume":
                if self._limits is not None:
                    self._sweep_limits(recovering=True)
                for call_id in self._visible_calls(self._store.audio_call_ids()):
                    if self._limits is None and self._store.get_call_limits(call_id) is not None:
                        continue  # Explicit compatible deadline mode is needed for recovery.
                    self._schedule_audio(call_id)
                self._schedule_progress()
                return None
            if operation == "state":
                call_id = args[0]
                raw_turns = self._store.get_transcript(call_id)
                turns = self._store.effective_transcript(call_id, raw_turns)
                generations = []
                for turn in turns:
                    if turn.role == "user":
                        try:
                            generations.append(
                                self._store.get_generation(call_id, "reply:" + turn.material_id)
                            )
                        except NotFound:
                            pass
                try:
                    review = self._store.get_review(call_id)
                except NotFound:
                    review = None
                try:
                    from .review_retry import latest, AttemptStore

                    attempt = latest(self._store, call_id)
                    publication_store = (
                        AttemptStore(self._store, attempt["attempt_id"]) if attempt else self._store
                    )
                    publication = publication_store.get_publication(call_id)
                except NotFound:
                    publication = None
                work = self._store.get_review_work(call_id)
                return CallState(
                    self._store.get_binding(call_id),
                    turns,
                    tuple(generations),
                    review,
                    publication,
                    work,
                    self._policy_state(call_id, review, work),
                    self._store.get_audio_jobs(call_id),
                    self._store.get_close_request(call_id),
                    raw_turns,
                    self._store.series_review_prompt(call_id, "")
                    + self._store.playback_context_notes(call_id, turns),
                    self._store.playback_projection(call_id, turns),
                )
            if operation == "drain":
                while self._background:
                    await asyncio.shield(
                        asyncio.gather(*tuple(self._background), return_exceptions=True)
                    )
                    # gather/shield may finish synchronously for already-done tasks.
                    # Let queued _spawn cleanup callbacks remove them before rechecking.
                    await asyncio.sleep(0)
                return None
            raise RuntimeOperationError("Unknown runtime operation")
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except AudioError as error:
            raise AudioError(error.code) from None
        except AccessError as error:
            raise AccessError(error.code) from None
        except Exception:
            raise RuntimeOperationError("Runtime operation failed") from None
        finally:
            if self._limits is not None:
                self._arm_limits()

    def _retain_missing(self, call_id, utterance_id, *, authority=None):
        if (call_id, utterance_id) in self._active_audio_jobs:
            raise AudioError("unconfirmed")
        try:
            return self._store.retain_audio_missing(call_id, utterance_id, _web_authority=authority)
        except (AccessError, AudioError):
            raise
        except Exception:
            # Lost COMMIT ACK is only confirmed by a fresh checked source read.
            if authority is not None:
                if self._store.authenticate_web_session(*authority) != call_id:
                    raise AccessError()
            try:
                job = self._store.get_audio_job(call_id, utterance_id)
                if audio_retained(job):
                    return job
            except Exception:
                pass
            raise AudioError("unconfirmed") from None

    def _observe_series_seal(self, call_id, sid):
        try:
            if self._store.get_audio_job(call_id, sid).series_disposition != "sealed":
                return
            if (
                self._store._db.execute(
                    "SELECT 1 FROM turn_timings WHERE call_id=? AND generation_id=? AND stage='series_sealed'",
                    (call_id, "capture:" + sid),
                ).fetchone()
                is None
            ):
                self._store.record_timings(
                    call_id,
                    "capture:" + sid,
                    [dict(stage="series_sealed", sequence=0, start_ms=0, elapsed_ms=0)],
                )

        except Exception:
            pass  # Optional telemetry cannot change the audio ACK.

    def _audio_ack_job(self, job):
        if job.series_id:
            self._observe_series_seal(job.call_id, job.series_id)
            return replace(
                job,
                series_snapshot=next(
                    r
                    for r in self._store.series_views(job.call_id)
                    if r["series_id"] == job.series_id
                ),
            )
        return job

    async def _audio_reply_after_safe_boundary(self, call_id, uid, text, *, series=False):
        while self._background_processing(call_id):
            await asyncio.sleep(0.025)
        return await process_utterance(
            self._store,
            call_id,
            uid,
            text,
            self._reply,
            source="audio_transcription",
            series=series,
        )

    def _schedule_series_replies(self, call_id):
        if self._store.is_call_closing(call_id):
            return
        for root in self._store.series_roots(call_id):
            if self._store.series_ready(root) and not self._store.candidate_for(
                call_id, root.series_id
            ):
                self._spawn(
                    self._audio_reply_after_safe_boundary(call_id, root.series_id, "", series=True),
                    "reply",
                ).add_done_callback(lambda _: self._speech_progress(call_id))

    def _schedule_audio(self, call_id):
        if (
            self._audio is None
            or self._worker_stopping
            or not self._call_allowed(call_id)
            or (call_id in self._recovery_calls and call_id not in self._closing_recovery_calls)
        ):
            return
        self._audio_requested.add(call_id)
        task = self._audio_tasks.get(call_id)
        if task is None or task.done():
            task = self._spawn(self._audio_progress(call_id), "audio")
            self._audio_tasks[call_id] = task

            def finished(done):
                if self._audio_tasks.get(call_id) is done:
                    self._audio_tasks.pop(call_id, None)

            task.add_done_callback(finished)

    async def _audio_progress(self, call_id):
        if not self._call_allowed(call_id):
            return
        self._require_call(call_id)

        async def transcribe(bound, job):
            key = (bound.call_id, job.utterance_id)
            self._active_audio_jobs.add(key)
            try:
                return await self._bridge("transcribe", bound, job)
            finally:
                # _bridge cancellation also waits for the real callback, its
                # suppressed SDK cancellation, native threads, scope and cleanup.
                self._active_audio_jobs.discard(key)
                # advance_audio commits its exact success/NoText/failure directly
                # after this return, before yielding to the next bridge. Notify
                # on the next loop turn, then the waiter checks durable state.
                asyncio.get_running_loop().call_soon(self._speech_progress, call_id)

        def on_text(turn):
            self._notify_state(call_id)  # complete_audio committed before on_text.
            if self._store.candidate_for(call_id, turn.material_id):
                self._progress_candidates(call_id)
                return
            if self._store.get_audio_job(call_id, turn.material_id).series_id is not None:
                self._schedule_series_replies(call_id)
                return
            if self._store.is_call_closing(call_id):
                self._store.claim_generation(
                    call_id,
                    "reply:" + turn.material_id,
                    utterance_id=turn.material_id,
                    source="fake_reply",
                )  # durable cancelled, no reply callback
            else:
                self._spawn(
                    self._audio_reply_after_safe_boundary(call_id, turn.material_id, turn.body),
                    "reply",
                ).add_done_callback(lambda _: self._speech_progress(call_id))

        try:
            while call_id in self._audio_requested and not self._worker_stopping:
                self._audio_requested.discard(call_id)
                jobs = self._store.get_audio_jobs(call_id)
                if not jobs:
                    return
                self._require_call(call_id)
                proof = self._closing_recovery_calls.get(call_id)
                if proof is not None:
                    state, saved, _ = await self._inspect_recovery(call_id)
                    if proof != (saved, self._closing_source(state, saved)):
                        raise AudioError("conflict")
                await advance_audio(self._store, call_id, transcribe, on_text, max_steps=len(jobs))
                self._schedule_series_replies(call_id)
                self._progress_candidates(call_id)
                close = self._store.get_close_request(call_id)
                current = self._store.get_audio_jobs(call_id)
                if close and self._store.audio_missing_enabled(call_id):
                    first = next((j for j in current if not audio_settled(j)), None)
                    if first is not None and first.state in {"failed", "uncertain", "cancelled"}:
                        self._retain_missing(call_id, first.utterance_id)
                        self._audio_requested.add(
                            call_id
                        )  # Finite pending-only advance, never retry.
                        continue
                if close and all(audio_settled(j) for j in current):
                    self._store.end_call(call_id, prompt=close.prompt)
                    self._closing_recovery_calls.pop(call_id, None)
                    self._schedule_progress()
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            raise AudioError() from None

    def _policy_state(self, call_id, review, work):
        manifest = work.manifest if work is not None else None
        if manifest is None and self._review_routes is None:
            return None
        if manifest is not None and review is not None and review.state == "succeeded":
            status = "complete"
        elif self._review_routes is None:
            status = "route_required"
        elif review is None:
            status = "not_ended"
        elif work is not None and manifest is None:
            status = "legacy_plan"
        elif work is None and review.state != "pending":
            status = "legacy_running"
        else:
            status = self._policy_status.get(call_id, "unresolved")
        return ReviewPolicyState(status, manifest)

    def _review_note(self, call_id, stage, reason):
        # Bounded, transition-only metadata. Never log exceptions or source text.
        key = (call_id, stage)
        if self._review_observed.get(key) == reason:
            return
        self._review_observed[key] = reason
        try:
            logging.getLogger(__name__).info(
                "bot_call.review stage=%s reason=%s call=%s",
                stage,
                reason,
                hashlib.sha256(call_id.encode()).hexdigest()[:12],
            )
        except BaseException:
            pass  # An observer must not change a committed business outcome.

    def _visible_reviews(self):
        out = []
        for call_id in self._store.review_call_ids():
            if not self._call_allowed(call_id):
                self._review_note(call_id, "skip", "scope_required")
                continue
            try:
                policy = self._store.get_call_policy(call_id)
                if policy is not None and not policy.is_agent:
                    self._review_note(call_id, "skip", "legacy_policy")
                    continue
            except Exception:
                self._policy_status[call_id] = "processing_unconfirmed"
                self._review_note(call_id, "scope", "processing_unconfirmed")
                continue
            out.append(call_id)
        return out

    async def _progress_routed(self, call_id):
        stage = "scope"
        try:
            if not self._call_allowed(call_id):
                self._review_note(call_id, "skip", "scope_required")
                return
            self._require_call(call_id)
            work = self._store.get_review_work(call_id)
            if work is not None and (work.manifest is None or work.state != "pending"):
                reason = "legacy_plan" if work.manifest is None else "work_" + work.state
                self._policy_status[call_id] = reason
                self._review_note(call_id, "skip", reason)
                return
            if work is None and self._store.get_review(call_id).state != "pending":
                self._review_note(call_id, "skip", "review_not_pending")
                return
            policy = self._store.get_call_policy(call_id)
            saved = (
                work.manifest
                if work is not None
                else policy.review_manifest
                if policy is not None
                else None
            )
            if policy is not None and saved != policy.review_manifest:
                raise ReviewPolicyUnavailable("mismatch")
            stage = "resolve"
            self._policy_status[call_id] = "resolving"
            self._review_note(call_id, stage, "start")
            bound = self._store.get_binding(call_id)
            route = await self._bridge("review_route_resolve", bound, saved)
            if (
                type(route) is not DetachedReviewRoute
                or route.manifest.binding != bound
                or not callable(route.count_tokens)
                or (saved is not None and route.manifest != saved)
            ):
                raise ReviewPolicyUnavailable("mismatch")
            self._require_call(call_id)
            stage = "prepare"
            if work is None:
                work = prepare_review_work(
                    self._store,
                    call_id,
                    input_token_budget=route.manifest.input_token_budget,
                    count_tokens=route.count_tokens,
                    manifest=route.manifest,
                )
            if (
                work.manifest != route.manifest
                or work.input_token_budget != route.manifest.input_token_budget
            ):
                raise ReviewPolicyUnavailable("mismatch")
            self._policy_status[call_id] = "ready"
            stage = "generate"

            async def generate(node):
                return await self._bridge("review_route_generate", node, route.manifest)

            result = await advance_review_work(
                self._store,
                call_id,
                generate,
                count_tokens=route.count_tokens,
                max_steps=2 * work.map_count,
                manifest=route.manifest,
            )
            reason = "complete" if result.state == "succeeded" else "work_" + result.state
            self._policy_status[call_id] = reason
            self._review_note(call_id, "complete", reason)
        except asyncio.CancelledError:
            self._review_note(call_id, stage, "cancelled")
            raise asyncio.CancelledError() from None
        except ReviewPolicyUnavailable as error:
            self._policy_status[call_id] = error.code
            self._review_note(call_id, stage, error.code)
        except Exception as error:
            from .review_plan import ReviewPlanError
            from .review_work import ReviewWorkError

            reason = "processing_unconfirmed"
            if type(error) in {ReviewPlanError, ReviewWorkError}:
                # Only fixed codes authored by these modules may enter logs.
                code = str(error).split(".")[-1]
                if code in {
                    "invalid_input",
                    "invalid_budget",
                    "invalid_counter",
                    "counter_failed",
                    "invalid_count",
                    "budget_too_small",
                    "counter_changed",
                    "persistence_unconfirmed",
                    "policy_mismatch",
                }:
                    reason = code
            self._policy_status[call_id] = reason
            self._review_note(call_id, stage, reason)

    async def _retry_call(self, call_id, view, policy, manifest, context_only, done):
        try:
            if not done:
                work = prepare_review_work(
                    view,
                    call_id,
                    input_token_budget=policy.input_token_budget,
                    count_tokens=policy.count_request,
                    manifest=manifest,
                )

                async def generate(node):
                    return await self._bridge("review_retry_generate", node, manifest)

                await advance_review_work(
                    view,
                    call_id,
                    generate,
                    count_tokens=policy.count_request,
                    max_steps=2 * work.map_count,
                    manifest=manifest,
                )
            if view.get_review(call_id).state == "succeeded":
                try:
                    view.get_publication(call_id)
                except NotFound:
                    view.prepare_publication(call_id, char_budget=self._budget)
                publication = await advance_publication(
                    view, call_id, self._send, self._context, send_pages=not context_only
                )
                if publication.context_state == "succeeded":
                    with self._store._transaction():
                        self._store._db.execute(
                            "UPDATE review_retry_attempts SET completed_at=coalesce(completed_at,?) WHERE attempt_id=?",
                            (time.time(), view.attempt_id),
                        )
        except asyncio.CancelledError:
            raise
        except Exception:
            self._review_note(call_id, "retry", "processing_unconfirmed")
        finally:
            self._review_tasks.pop(call_id, None)
            self._business.call_soon_threadsafe(
                self._retry_adapters.pop, (call_id, manifest.fingerprint), None
            )

    async def _publish_review(self, call_id):
        self._require_call(call_id)
        if self._store.get_review(call_id).state != "succeeded":
            self._review_note(call_id, "publication", "review_not_succeeded")
            return
        try:
            self._store.get_publication(call_id)
        except NotFound:
            self._store.prepare_publication(call_id, char_budget=self._budget)
        publication = await advance_publication(
            self._store,
            call_id,
            self._send,
            self._context,
            send_pages=call_id not in self._review_context_only,
        )
        self._review_note(call_id, "context", publication.context_state)
        for state in {p.state for p in publication.pages}:
            self._review_note(call_id, "pages", state)

    async def _routed_call(self, call_id):
        try:
            while call_id in self._review_requested and not self._worker_stopping:
                self._review_requested.discard(call_id)
                await self._progress_routed(call_id)
                try:
                    await self._publish_review(call_id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._review_note(call_id, "publication", "processing_unconfirmed")
        finally:
            self._review_tasks.pop(call_id, None)

    def _schedule_progress(self):
        self._progress_requested = True
        if self._progress_task is None or self._progress_task.done():
            self._progress_task = self._spawn(self._progress(), "progress")

    async def _progress(self):
        # Re-run only when another explicit end/resume arrived during this pass.
        while self._progress_requested and not self._worker_stopping:
            self._progress_requested = False
            if self._review_routes is not None:
                for call_id in self._visible_reviews():
                    self._review_requested.add(call_id)
                    if call_id not in self._review_tasks:
                        self._review_tasks[call_id] = self._spawn(
                            self._routed_call(call_id), "review-call"
                        )
                continue
            elif self._review_work is None:
                # Installing a chunk plan atomically claims the parent as running;
                # legacy run_pending sees only pending parents and cannot adopt it.
                await run_pending(self._store, self._review)
            else:
                for call_id in self._visible_calls(self._store.review_call_ids()):
                    work = self._store.get_review_work(call_id)
                    if work is not None and work.manifest is not None:
                        continue  # A legacy global mode cannot authorize a controlled policy.
                    if work is None and self._store.get_review(call_id).state == "pending":
                        work = prepare_review_work(
                            self._store,
                            call_id,
                            input_token_budget=self._review_work.input_token_budget,
                            count_tokens=self._review_work.count_tokens,
                        )
                    if work is not None and work.state == "pending":
                        # M maps + at most M-1 merges + one independent final.
                        # Carries never generate. Empty/unknown work is not claimed.
                        await advance_review_work(
                            self._store,
                            call_id,
                            self._chunk,
                            count_tokens=self._review_work.count_tokens,
                            max_steps=2 * work.map_count,
                        )
            for call_id in self._visible_calls(self._store.review_call_ids()):
                self._require_call(call_id)
                if self._store.get_review(call_id).state != "succeeded":
                    continue
                try:
                    self._store.get_publication(call_id)
                except NotFound:
                    self._store.prepare_publication(call_id, char_budget=self._budget)
                await advance_publication(self._store, call_id, self._send, self._context)

    async def _invoke(self, token, name, args):
        stage = {
            "transcribe": "stt",
            "reply": "reply",
            "synthesize": "tts",
            "review": "review",
            "review_work": "review",
            "review_route_generate": "review",
            "review_retry_generate": "review",
        }.get(name)
        durable = (
            RoundTiming(getattr(args[1], "received_at", None))
            if stage in {"stt", "reply", "tts"}
            and len(args) >= 2
            and isinstance(args[0], CallBinding)
            else None
        )
        scope = CURRENT_TIMING.set(durable)
        started, outcome = None, "uncertain"
        try:
            if stage is not None and self._timings is not None:
                started = self._timings.begin(stage)
        except BaseException:
            pass  # Observation is optional, including a broken clock/collector.
        try:
            result = await self._invoke_owned(token, name, args)
            if result.kind == "cancelled":
                outcome = "cancelled"
            elif result.kind in {
                "speech_not_applied",
                "audio_not_applied",
                "not_applied",
                "policy_unavailable",
            }:
                outcome = "failed"
            elif result.kind == "value":
                outcome = "success" if result.value is not None and result.value != "" else "failed"
            return result
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        finally:
            CURRENT_TIMING.reset(scope)
            if durable is not None:
                if stage == "reply":
                    durable.finish("run")
                gid = (
                    ("capture:" if args[1].series_id else "reply:") + args[1].utterance_id
                    if stage == "stt"
                    else (args[1].material_id if stage == "tts" else "reply:" + args[1].material_id)
                )
                if stage == "tts" and getattr(args[1], "segment_seq", None):
                    for row in durable.rows:
                        row["sequence"] = args[1].segment_seq
                try:
                    await self._request("timing_write", args[0].call_id, gid, durable.rows)
                except BaseException:
                    pass  # Telemetry never retries business work or changes its result.
            try:
                if stage is not None and self._timings is not None:
                    self._timings.finish(stage, started, outcome)
            except BaseException:
                pass

    async def _invoke_owned(self, token, name, args):
        """Business-loop wrapper tracks the actual callback task until it ends."""

        async def call():
            if name == "transcribe":
                callback = self._audio.transcribe
            elif name == "review_route_resolve":
                callback = self._review_routes.resolve
            elif name == "review_retry_generate":
                callback = self._retry_adapters[(args[0].call_id, args[1].fingerprint)].generate
                return await callback(args[0])
            elif name == "review_route_generate":
                callback = self._review_routes.generate
            else:
                callback = (
                    self._review_work.generate
                    if name == "review_work"
                    else getattr(self._callbacks, name)
                )
            return await callback(*args)

        task = asyncio.create_task(call(), name="BotCall:core:" + name)
        self._core_tasks[token] = task
        if token in self._cancel_tokens:
            task.cancel()
        try:
            value = await task
            if name == "review_route_resolve":
                value = value if type(value) is DetachedReviewRoute else None
            elif name == "synthesize":
                if len(args) > 1 and getattr(args[1], "segment_seq", None):
                    segment_audio_mime(value)
                else:
                    value = check_wav(value)
            elif name == "transcribe":
                value = value if type(value) in {str, NoTextResult} else None
            elif name in {
                "reply",
                "review",
                "review_work",
                "review_route_generate",
                "review_retry_generate",
            }:
                value = value if isinstance(value, str) else None
            else:
                value = value if isinstance(value, Outcome) else None
            return _CoreResult("value", value)
        except asyncio.CancelledError:
            return _CoreResult("cancelled")
        except ReviewPolicyUnavailable as error:
            return (
                _CoreResult("policy_unavailable", error.code)
                if name == "review_route_resolve"
                else _CoreResult("error")
            )
        except SpeechUnavailable:
            return _CoreResult("speech_not_applied" if name == "synthesize" else "error")
        except AudioNotApplied:
            return _CoreResult("audio_not_applied" if name == "transcribe" else "error")
        except ModelFailed as error:
            return (
                _CoreResult("model_failed", error.code)
                if name in {"review_work", "review_route_generate", "review_retry_generate"}
                else _CoreResult("error")
            )
        except NotApplied:
            return _CoreResult(
                "not_applied"
                if name in {"review_work", "review_route_generate", "review_retry_generate"}
                else "error"
            )
        except Exception:
            return _CoreResult("error")
        finally:
            self._core_tasks.pop(token, None)
            self._cancel_tokens.discard(token)

    def _cancel_core(self, token):
        self._cancel_tokens.add(token)
        task = self._core_tasks.get(token)
        if task is not None:
            task.cancel()

    async def _bridge(self, name, *args):
        token = object()
        future = asyncio.run_coroutine_threadsafe(self._invoke(token, name, args), self._business)
        wrapped = asyncio.wrap_future(future)
        try:
            result = await asyncio.shield(wrapped)
        except asyncio.CancelledError:
            self._business.call_soon_threadsafe(self._cancel_core, token)
            # Never cancel the cross-thread future: it could say cancelled while
            # a callback that suppressed cancellation is still running.
            while True:
                try:
                    await asyncio.shield(wrapped)
                    break
                except asyncio.CancelledError:
                    continue
            self._business.call_soon_threadsafe(self._cancel_tokens.discard, token)
            raise asyncio.CancelledError() from None
        if result.kind == "cancelled":
            raise asyncio.CancelledError() from None
        if result.kind == "policy_unavailable" and name == "review_route_resolve":
            raise ReviewPolicyUnavailable(result.value) from None
        if result.kind == "speech_not_applied" and name == "synthesize":
            raise SpeechUnavailable() from None
        if result.kind == "audio_not_applied" and name == "transcribe":
            raise AudioNotApplied("Audio was not applied") from None
        if result.kind == "model_failed" and name in {
            "review_work",
            "review_route_generate",
            "review_retry_generate",
        }:
            raise ModelFailed(result.value) from None
        if result.kind == "not_applied" and name in {
            "review_work",
            "review_route_generate",
            "review_retry_generate",
        }:
            raise NotApplied("Chunk generation was not applied") from None
        if result.kind != "value":
            raise RuntimeOperationError("Business callback failed") from None
        return result.value

    def _end_tool(self, call_id, gid):
        key = (call_id, gid)
        count = self._active_tools.get(key, 0)
        if count > 1:
            self._active_tools[key] = count - 1
        elif count:
            del self._active_tools[key]
        if count:
            self._notify_state(call_id)

    def _background_processing(self, call_id):
        return any(
            c == call_id and self._store.speech_superseded(c, gid)
            for c, gid in self._store._active_agents
        )

    def _tool_processing(self, call_id):
        return not self._store.is_call_closing(call_id) and any(
            c == call_id and not self._store.speech_superseded(c, gid)
            for c, gid in self._active_tools
        )

    def _notify_state(self, call_id, *, speech=False):
        if speech and self._limits and self._limits.idle_seconds:
            self._speech_activity(call_id)
            self._arm_limits()
        try:
            closed = self._store.is_call_closing(call_id)
            if closed:
                self._last_speech_at.pop(call_id, None)
                for key in tuple(self._active_tools):
                    if key[0] == call_id:
                        del self._active_tools[key]
            event = {
                "state": "changed",
                "audio": [
                    dict(r)
                    for r in self._store._db.execute(
                        "SELECT utterance_id,state FROM audio_jobs WHERE call_id=?", (call_id,)
                    )
                ],
                "segments": self._store.segments(call_id),
                "closed": closed,
            }
            if call_id in self._close_details:
                event["close_detail"] = self._close_details[call_id]
            if not event["closed"]:
                from .speech import MAX_PUSH_AUDIO_BYTES

                candidate = self._store._db.execute(
                    "SELECT generation_id,EXISTS(SELECT 1 FROM turn_timings t WHERE "
                    "t.call_id=speech_segments.call_id AND t.generation_id=speech_segments.generation_id "
                    "AND t.stage='tts_pcm_selection') AS tts_pcm_selection "
                    "FROM speech_segments WHERE call_id=? "
                    "AND seq=1 AND state='succeeded' AND playback_state='unknown' AND revision=0 "
                    "AND length(data) BETWEEN 24 AND ? ORDER BY ready_at DESC LIMIT 1",
                    (call_id, MAX_PUSH_AUDIO_BYTES),
                ).fetchone()
                if candidate:
                    event["inline_first"] = candidate["generation_id"]
                    if candidate["tts_pcm_selection"]:
                        event["tts_pcm_selection"] = True
            self._business.call_soon_threadsafe(self._deliver_push, call_id, event)
        except Exception:
            pass  # Optional push must not change a committed business result.
        # Only committed progress/owned completion sites call this, never reads.
        for (target, _), event in tuple(self._state_waiters.items()):
            if target == call_id:
                event.set()

    def _state_phase(self, key):
        call_id, utterance_id = key
        try:
            audio = self._store.get_audio_job(call_id, utterance_id)
        except NotFound:
            audio = None
        turn = self._store._find_turn(call_id, "user", utterance_id)
        if audio is None and turn is None:
            raise AccessError("conflict") from None
        if self._store.is_call_closing(call_id):
            return None
        if audio is not None:
            if audio.state in {"pending", "running"}:
                return "audio"
            if audio.state != "succeeded":
                return None  # NoText/unknown/failure/resolution keep their meaning.
        try:
            generation = self._store.get_generation(call_id, "reply:" + utterance_id)
        except NotFound:
            return "reply"
        if generation.state in {"pending", "running"}:
            return (
                "reply",
                tuple(
                    (r["seq"], r["state"])
                    for r in self._store.segments(call_id, generation.generation_id)
                ),
            )
        if (
            generation.state == "succeeded"
            and self._store._find_turn(call_id, "assistant", generation.generation_id) is None
        ):
            return "reply"  # A partial/final tool decision is not saved speech text.
        return None

    async def _wait_state(self, key, wait_ms):
        deadline = asyncio.get_running_loop().time() + wait_ms / 1000
        phase = self._state_phase(key)  # Also validates call-local target for wait=0.
        if (
            not wait_ms
            or phase is None
            or self._state_wait_stop.is_set()
            or key in self._state_waiters
            or len(self._state_waiters) >= 4
        ):
            return
        call_id, _ = key
        tools = (self._tool_processing(call_id), self._background_processing(call_id))
        event = asyncio.Event()
        self._state_waiters[key] = event
        stopping = asyncio.create_task(self._state_wait_stop.wait(), name="BotCall:state-wait-stop")
        try:
            while not self._state_wait_stop.is_set():
                event.clear()
                # Registration precedes this recheck; a commit cannot be lost.
                # Audio terminal and the separately spawned reply are two phases.
                if self._state_phase(key) != phase or tools != (
                    self._tool_processing(call_id),
                    self._background_processing(call_id),
                ):
                    return
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return
                changed = asyncio.create_task(event.wait(), name="BotCall:state-wait-change")
                try:
                    done, _ = await asyncio.wait(
                        {changed, stopping}, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
                    )
                    if not done:
                        return
                finally:
                    changed.cancel()
                    await asyncio.gather(changed, return_exceptions=True)
        finally:
            if self._state_waiters.get(key) is event:
                del self._state_waiters[key]
            stopping.cancel()
            await asyncio.gather(stopping, return_exceptions=True)

    async def _wait_speech(self, key, deadline, wait_ms):
        task = self._speech_tasks.get(key)
        remaining = deadline - asyncio.get_running_loop().time()
        if (
            not wait_ms
            or remaining <= 0
            or task is None
            or task.done()
            or self._speech_wait_stop.is_set()
            or key in self._speech_waiters
            or len(self._speech_waiters) >= 4
        ):
            return
        self._speech_waiters.add(key)
        stopping = asyncio.create_task(
            self._speech_wait_stop.wait(), name="BotCall:speech-wait-stop"
        )
        try:
            # asyncio.wait timeout/cancellation never cancels either input task.
            # Store's claim transaction has already ended; the worker loop remains
            # available to SDK entry guards, media ACKs, heartbeat and hangup.
            await asyncio.wait(
                {task, stopping}, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            self._speech_waiters.discard(key)
            stopping.cancel()
            await asyncio.gather(stopping, return_exceptions=True)

    async def _synthesize(self, call_id, generation_id):
        # Claim was committed before this owned task can enter the business loop.
        try:
            if self._store.is_call_closing(call_id):
                self._store.finish_speech(call_id, generation_id, "cancelled")
                return
            turn = self._store._find_turn(call_id, "assistant", generation_id)
            data = await self._bridge(
                "synthesize", self._store.get_binding(call_id), turn, self.speech_open
            )
            self._store.finish_speech(call_id, generation_id, "succeeded", data)
        except asyncio.CancelledError:
            self._store.finish_speech(call_id, generation_id, "uncertain")
            raise
        except SpeechUnavailable:
            self._store.finish_speech(call_id, generation_id, "failed")
        except Exception:
            self._store.finish_speech(call_id, generation_id, "uncertain")
        finally:
            self._speech_progress(call_id)

    def _series_reply_metrics(self, call_id, sid):
        try:
            members = [j for j in self._store.get_audio_jobs(call_id) if j.series_id == sid]
            rows = [
                dict(stage="series_segments", sequence=0, start_ms=0, elapsed_ms=len(members) or 1)
            ]
            if members:
                sealed = self._store._db.execute(
                    "SELECT recorded_at FROM turn_timings WHERE call_id=? AND generation_id=? AND stage='series_sealed' ORDER BY rowid LIMIT 1",
                    (call_id, "capture:" + sid),
                ).fetchone()
                if sealed is not None:
                    rows.append(
                        dict(
                            stage="series_wait_ms",
                            sequence=0,
                            start_ms=0,
                            elapsed_ms=max(0, round((time.time() - sealed[0]) * 1000)),
                        )
                    )
            self._store.record_timings(call_id, "reply:" + sid, rows)
        except Exception:
            pass  # Measurements are not prerequisites for a reply.

    async def _reply(self, *args):
        bound, turn = args
        gid = "reply:" + turn.material_id
        # Closure may win after process_utterance claimed this generation but
        # before the callback starts. Preserve its transcript, without a paid reply.
        if self._store.is_call_closing(bound.call_id):
            self._store.record_timings(
                bound.call_id,
                gid,
                [dict(stage="cancelled_closed", sequence=0, start_ms=0, elapsed_ms=0)],
            )
            raise asyncio.CancelledError()
        self._series_reply_metrics(bound.call_id, turn.material_id)
        policy = self._store.get_call_policy(bound.call_id)
        scoped = policy is not None and policy.is_agent
        key = (bound.call_id, gid)
        if scoped:
            self._store._active_agents.add(key)
        self._reply_tasks[key] = asyncio.current_task()
        try:
            return await self._bridge("reply", *args)
        finally:
            del self._reply_tasks[key]
            if scoped:
                # _bridge did not return merely because its caller was cancelled.
                # Actual business callback/Executor/DB cleanup/privacy scope exited.
                call_id = bound.call_id
                had_tools = self._active_tools.pop((call_id, gid), 0)
                superseded = self._store.speech_superseded(call_id, gid)
                try:
                    self._store.agent_scope_exit(call_id, gid)
                finally:
                    if had_tools or superseded:
                        self._notify_state(call_id)
                if (bound.call_id, gid) not in self._segment_tasks:
                    self._segment_started.pop((bound.call_id, gid), None)
                close = self._store.get_close_request(bound.call_id)
                if close is not None:
                    self._store.end_call(bound.call_id, prompt=close.prompt, _request_close=True)
                    self._schedule_progress()

    async def _review(self, *args):
        return await self._bridge("review", *args)

    async def _chunk(self, *args):
        return await self._bridge("review_work", *args)

    async def _send(self, *args):
        return await self._bridge("send_page", *args)

    async def _context(self, *args):
        return await self._bridge("make_context_readable", *args)

    def _begin_stop(self):
        self._worker_stopping = True
        self._active_tools.clear()
        self._cancel_retention()
        self._chunks.close()
        self._speech_wait_stop.set()
        self._state_wait_stop.set()
        if self._limit_handle is not None:
            self._limit_handle.cancel()
            self._limit_handle = None
        for task in tuple(self._owned):
            task.cancel()
        self._stop.set()

    async def shutdown(self, *, timeout: float | None = 5.0) -> ShutdownReport:
        self._check_loop()
        if timeout is not None and timeout < 0:
            raise ValueError("Shutdown timeout must not be negative")
        self._closing, self._accepting = True, False
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close(), name="BotCall:shutdown")
            self._close_task.add_done_callback(_consume)
        try:
            return await asyncio.wait_for(asyncio.shield(self._close_task), timeout)
        except TimeoutError:
            return ShutdownReport(False, "still_stopping")

    async def _close(self):
        if self._start_task is None:
            return ShutdownReport(True)
        try:
            await asyncio.shield(self._start_task)
        except RuntimeOperationError:
            return ShutdownReport(True, "startup_failed")
        self._worker_loop.call_soon_threadsafe(self._begin_stop)
        await asyncio.shield(asyncio.wrap_future(self._exited))
        await asyncio.to_thread(self._thread.join)
        return ShutdownReport(True, self._worker_error)
