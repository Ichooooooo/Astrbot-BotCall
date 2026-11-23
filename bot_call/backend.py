"""Opt-in composition of the accepted adapters; no import/construction I/O.

The worker scope starts empty even in an existing project database. Only this
instance's explicitly prepared calls are registered. Unknown old work remains
untouched; reopening the database is not cross-restart policy recovery.
"""

import asyncio
from dataclasses import dataclass, field, replace
import hmac
import math
import logging
import os
import stat
from pathlib import Path
from urllib.parse import urlsplit

from .audio import audio_settled
from .access import AccessError, InvitationSecret, token_digest
from .astrbot_adapter import AstrBotReadAdapter
from .astrbot_audio import AstrBotAudioAdapter, AudioRequestPolicy
from .astrbot_tts import AstrBotTTSAdapter, SpeechPolicy
from .astrbot_agent import SourceAgentPolicy
from .phone_style import DEFAULT_REPLY_STYLE
from .astrbot_reply import AstrBotReplyAdapter, ReplyPolicy
from .astrbot_review import AstrBotReviewAdapter
from .binding import CallBinding, validate_conversation
from .prepare_diagnostics import attach_prepare
from .call_policy import CallPolicy
from .limits import RuntimeLimits
from .review_policy import ReviewManifest, ReviewPolicyUnavailable
from .review import DEFAULT_REVIEW_PROMPT
from .review_router import ReviewPolicyRouter, ReviewPolicySelection
from .runtime import CallRuntime, RuntimeAudio, RuntimeCallbacks, ShutdownReport
from .web import CallWebApp


class BackendError(RuntimeError):
    def __init__(self, code="unconfirmed"):
        self.code = (
            code
            if code
            in {
                "closed",
                "invalid_configuration",
                "unavailable",
                "unconfirmed",
                "recovery_unavailable",
                "counter_unavailable",
            }
            else "unconfirmed"
        )
        super().__init__("call_backend." + self.code)


def _fail(code="unconfirmed"):
    raise BackendError(code) from None


def _consume(task):
    if not task.cancelled():
        task.exception()


async def _wait_owned(task):
    # The owner retains/consumes task. asyncio.wait never cancels its input.
    # Python 3.14 shield installs an exception-reporting callback when its
    # waiter is cancelled, even if another owner consumes the eventual error.
    try:
        await asyncio.wait({task})
        return task.result()
    except asyncio.CancelledError:
        raise asyncio.CancelledError() from None


@dataclass(frozen=True, slots=True)
class BackendConfig:
    database: Path = field(repr=False)
    origin: str = field(repr=False)
    session_ttl: int
    limits: RuntimeLimits
    max_audio_bytes: int = 2097152
    max_body_bytes: int = 16384
    call_vad_silence_ms: int = 1200
    call_vad_sensitivity: int = 50
    call_max_continuous_segments: int = 10
    call_capture_mode: str = "auto"
    call_barge_in: str = "auto"
    call_backchannel_phrases: tuple | None = None
    char_budget: int = 1800
    review_prompt: str = field(default=DEFAULT_REVIEW_PROMPT, repr=False)
    review_voice: str = "default"
    audio_retention_days: int = 7

    def __post_init__(self):
        try:
            from .barge_in import valid_phrases

            if self.call_backchannel_phrases is not None and not valid_phrases(
                self.call_backchannel_phrases
            ):
                raise ValueError
            if type(self.review_voice) is not str or self.review_voice not in {
                "default",
                "bot",
                "neutral",
            }:
                raise ValueError
            if type(self.review_prompt) is not str:
                raise ValueError
            self.review_prompt.encode("utf-8")
            parts = urlsplit(self.origin)
            if (
                self.call_barge_in not in ("auto", "off")
                or self.call_capture_mode not in ("auto", "legacy")
                or type(self.call_max_continuous_segments) is not int
                or not 1 <= self.call_max_continuous_segments <= 20
                or type(self.audio_retention_days) is not int
                or not 0 <= self.audio_retention_days <= 3650
                or type(self.call_vad_silence_ms) is not int
                or not 300 <= self.call_vad_silence_ms <= 3000
                or type(self.call_vad_sensitivity) is not int
                or not 10 <= self.call_vad_sensitivity <= 90
                or not isinstance(self.database, Path)
                or not self.database.is_absolute()
                or type(self.origin) is not str
                or parts.scheme != "https"
                or not parts.hostname
                or parts.username is not None
                or parts.password is not None
                or parts.path
                or parts.query
                or parts.fragment
                or parts.netloc != parts.netloc.lower()
                or any(ord(c) <= 32 or ord(c) >= 127 for c in self.origin)
                or type(self.limits) is not RuntimeLimits
                or any(
                    type(v) is not int or v <= 0
                    for v in (
                        self.session_ttl,
                        self.max_audio_bytes,
                        self.max_body_bytes,
                        self.char_budget,
                    )
                )
                or self.session_ttl > 2147483647
            ):
                raise ValueError
            parts.port
        except Exception:
            _fail("invalid_configuration")


@dataclass(frozen=True, slots=True)
class BackendTools:
    """Trusted business-loop capabilities, not web configuration or a registry."""

    context: object = field(repr=False)
    session_lock_manager: object = field(repr=False)
    context_manager_type: type = field(repr=False)
    context_config_type: type = field(repr=False)
    message_type: type = field(repr=False)
    converter: object = field(repr=False)
    privacy: object = field(default=None, repr=False)
    speech_enabled: bool = True
    agent_api: object = field(default=None, repr=False)
    call_reply_style: str = field(default=DEFAULT_REPLY_STYLE, repr=False)
    call_chat_provider_id: str = field(default="", repr=False)
    call_history_turns: int = 8
    call_history_summary: bool = True
    call_skip_plugin_hooks: tuple = ()
    call_history_max_chars: int = 8000
    stt_converter: object = field(default=None, repr=False)
    segment_converter: object = field(default=None, repr=False)
    audio_warmup: object = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class BackendPolicies:
    reply: ReplyPolicy = field(repr=False)
    audio: AudioRequestPolicy = field(repr=False)
    review: ReviewPolicySelection = field(repr=False)
    speech: object = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class CallPreparation:
    """Retain this trusted-server handle BEFORE awaiting invitation delivery."""

    _owner: object = field(repr=False)
    _key: object = field(repr=False)


@dataclass(frozen=True, slots=True)
class CallRecovery:
    """Trusted-host handle; cannot be used as an invitation preparation."""

    _owner: object = field(repr=False)
    _key: object = field(repr=False)


@dataclass(slots=True)
class _Recovery:
    call_id: str = field(repr=False)
    counters: object = field(repr=False)
    binding: CallBinding | None = field(default=None, repr=False)
    policy: CallPolicy | None = field(default=None, repr=False)
    frozen_review: object = field(default=None, repr=False)
    closing_source: object = field(default=None, repr=False)
    audio: object = field(default=None, repr=False)
    selection: object = field(default=None, repr=False)
    review: object = field(default=None, repr=False)
    task: object = field(default=None, repr=False)


@dataclass(slots=True)
class _Preparation:
    event: object = field(repr=False)
    secret: InvitationSecret = field(repr=False)
    expected: object = field(default=None, repr=False)
    binding: CallBinding | None = field(default=None, repr=False)
    policies: BackendPolicies | None = field(default=None, repr=False)
    policy: CallPolicy | None = field(default=None, repr=False)
    query: object = field(default=None, repr=False)
    query_selected: bool = False
    bot_name: str = "Bot"
    review_prompt: str | None = field(default=None, repr=False)
    reply: object = field(default=None, repr=False)
    speech: object = field(default=None, repr=False)
    audio: object = field(default=None, repr=False)
    review: object = field(default=None, repr=False)
    grant: object = field(default=None, repr=False)
    task: object = field(default=None, repr=False)


def _reserve(path):
    # No SQLite probe outside its worker, no truncate/chmod/unlink of old files.
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if not stat.S_ISREG(path.lstat().st_mode):
            _fail("unavailable")  # Refuse directories and symlinks, even dangling.
    else:
        os.close(descriptor)


class CallBackend:
    def __init__(
        self, config, tools, *, select_policies, send_page, make_context_readable, timings=None
    ):
        if (
            type(config) is not BackendConfig
            or type(tools) is not BackendTools
            or any(
                not callable(v)
                for v in (select_policies, send_page, make_context_readable, tools.converter)
            )
            or any(
                v is not None and not callable(v)
                for v in (tools.stt_converter, tools.segment_converter, tools.audio_warmup)
            )
            or any(
                not isinstance(v, type)
                for v in (tools.context_manager_type, tools.context_config_type, tools.message_type)
            )
        ):
            _fail("invalid_configuration")
        self._config, self._tools, self._select = config, tools, select_policies
        self._send_page, self._make_context = send_page, make_context_readable
        self._timings = timings
        self._loop = None
        self._start_task = self._close_task = None
        self._runtime = self._reader = self._router = self._app = None
        self._closing, self._ready = False, False
        self._owner = object()
        self._preparations, self._calls = {}, {}
        self._recoveries, self._recovery_handles = {}, {}
        self._invitations, self._sessions = {}, {}
        self._web_tasks, self._commits = set(), set()
        self._warmup_tasks, self._audio_warmed = set(), set()

    def _check_loop(self):
        try:
            current = asyncio.get_running_loop()
            if self._loop is None:
                self._loop = current
            if current is not self._loop:
                _fail("closed")
        except Exception:
            _fail("closed")

    def diagnostic_binding(self):
        """Latest already-bound source only; no Store/history reads or creation."""
        self._open()
        for record in reversed(tuple(self._calls.values())):
            if type(record) is _Preparation and record.binding is not None:
                return record.binding
        return None

    def _open(self):
        self._check_loop()
        if self._closing or not self._ready:
            _fail("closed")

    async def start(self):
        self._check_loop()
        if self._closing:
            _fail("closed")
        if self._start_task is None:
            self._start_task = asyncio.create_task(self._start(), name="BotCall:backend-start")
            self._start_task.add_done_callback(_consume)
        await _wait_owned(self._start_task)

    async def _start(self):
        try:
            await asyncio.to_thread(_reserve, self._config.database)
            tools = self._tools
            self._reader = AstrBotReadAdapter(
                tools.context, session_lock_manager=tools.session_lock_manager
            )
            self._router = ReviewPolicyRouter(self._resolve_review, self._bind_review)
            self._runtime = CallRuntime(
                self._config.database,
                RuntimeCallbacks(
                    self._reply,
                    self._legacy,
                    self._send,
                    self._context,
                    self._synthesize if tools.speech_enabled else None,
                    self._interrupt,
                ),
                char_budget=self._config.char_budget,
                audio_retention_days=self._config.audio_retention_days,
                review_routes=self._router,
                audio=RuntimeAudio(
                    self._transcribe,
                    self._config.max_audio_bytes,
                    self._config.call_max_continuous_segments,
                ),
                limits=self._config.limits,
                restrict_calls=True,
                timings=self._timings,
                review_prompt=self._config.review_prompt,
                backchannel_phrases=self._config.call_backchannel_phrases,
                barge_in=self._config.call_barge_in == "auto"
                and self._config.call_capture_mode == "auto",
            )
            await self._runtime.start()
            # Fixed assets do synchronous file reads; retain the actual to_thread
            # operation in this shielded start task until construction completes.
            self._app = await asyncio.to_thread(
                CallWebApp,
                self,
                origin=self._config.origin,
                max_body_bytes=self._config.max_body_bytes,
                max_audio_bytes=self._config.max_audio_bytes,
                call_vad_silence_ms=self._config.call_vad_silence_ms,
                call_vad_sensitivity=self._config.call_vad_sensitivity,
                call_max_continuous_segments=self._config.call_max_continuous_segments,
                call_capture_mode=self._config.call_capture_mode,
                call_barge_in=self._config.call_barge_in,
            )
            self._ready = not self._closing
        except Exception:
            if self._runtime is not None:
                try:
                    await self._runtime.shutdown(timeout=None)
                except Exception:
                    pass  # Keep its reference; shutdown will report unconfirmed.
            _fail()

    @property
    def app(self):
        self._open()
        return self._app  # Host may mount this callable; no listener is created.

    def begin(self, event, *, expected=None):
        """Trusted server only: synchronously retain a handle before any await.

        Call begin once per initiation event. Reuse the handle for ACK loss;
        no cross-process/event deduplication is claimed.
        """
        self._open()
        handle = CallPreparation(self._owner, object())
        record = _Preparation(event, InvitationSecret.create(), expected=expected)
        self._preparations[handle._key] = record
        self._schedule(record)
        return handle

    def _record(self, handle):
        if type(handle) is not CallPreparation or handle._owner is not self._owner:
            _fail("unavailable")
        record = self._preparations.get(handle._key)
        if record is None:
            _fail("unavailable")
        return record

    def _schedule(self, record):
        record.task = asyncio.create_task(self._prepare(record), name="BotCall:prepare")
        record.task.add_done_callback(_consume)

    async def invitation(self, handle, *, retry=False):
        self._open()
        record = self._record(handle)
        if type(retry) is not bool:
            _fail("unavailable")
        if (
            retry
            and record.task.done()
            and (record.task.cancelled() or record.task.exception() is not None)
        ):
            self._schedule(record)  # Same binding/secret/policies; idempotent DB commits.
        grant = await _wait_owned(record.task)
        self._open()  # A concurrent shutdown must not hand out a usable invitation.
        return grant

    async def _prepare(self, record):
        stage = "bind"
        try:
            if record.binding is None:
                record.binding = await self._reader.bind(record.event, expected=record.expected)
                record.event = None
            bound = record.binding
            if record.policies is None:
                stage = "refresh"
                snapshot = await self._reader.refresh(bound)
                record.review_prompt = self._config.review_prompt
                if self._config.review_voice == "bot":
                    from .review import with_review_persona

                    record.review_prompt = with_review_persona(
                        record.review_prompt, snapshot.prompt
                    )
                persona = getattr(snapshot, "persona", None)
                name = persona.get("name") if type(persona) is dict else None
                name = name or getattr(snapshot, "selected_persona_id", None)
                if (
                    type(name) is str
                    and name.strip()
                    and len(name) <= 80
                    and all(ord(c) >= 32 for c in name)
                ):
                    record.bot_name = name
                stage = "select"
                selected = await self._select(bound, snapshot)
                stage = "policy"
                if (
                    type(selected) is not BackendPolicies
                    or type(selected.reply) is not SourceAgentPolicy
                    or type(selected.audio) is not AudioRequestPolicy
                    or type(selected.review) is not ReviewPolicySelection
                    or any(
                        p.binding != bound
                        for p in (selected.reply, selected.audio, selected.review.policy)
                    )
                    or selected.audio.max_input_bytes != self._config.max_audio_bytes
                ):
                    _fail("unavailable")
                manifest = ReviewManifest.from_policy(
                    selected.review.policy, selected.review.counter_rule_id
                )
                record.policy = CallPolicy.from_policies(selected.reply, selected.audio, manifest)
                record.policies = selected
            self._open()
            tools, policies = self._tools, record.policies
            if record.reply is None:
                stage = "reply_bind"
                record.reply = await AstrBotReplyAdapter.bind(
                    tools.context,
                    policies.reply,
                    read_adapter=self._reader,
                    read_call_state=self._runtime.read_state,
                    agent_api=tools.agent_api,
                    agent_journal=self._runtime.agent_journal,
                    call_reply_style=tools.call_reply_style,
                    call_chat_provider_id=tools.call_chat_provider_id,
                    call_skip_plugin_hooks=tools.call_skip_plugin_hooks,
                    call_history_turns=tools.call_history_turns,
                    call_history_summary=tools.call_history_summary,
                    call_history_max_chars=tools.call_history_max_chars,
                    session_lock_manager=tools.session_lock_manager,
                    privacy=tools.privacy,
                )
            if record.audio is None:
                stage = "audio_bind"
                record.audio = await AstrBotAudioAdapter.bind(
                    tools.context,
                    policies.audio,
                    converter=tools.stt_converter or tools.converter,
                    privacy=tools.privacy,
                )
            if policies.speech is not None and record.speech is None:
                stage = "audio_bind"
                if type(policies.speech) is not SpeechPolicy or policies.speech.binding != bound:
                    _fail("unavailable")
                record.speech = AstrBotTTSAdapter(
                    tools.context,
                    policies.speech,
                    converter=tools.converter,
                    privacy=tools.privacy,
                    segment_converter=tools.segment_converter,
                )
            if record.review is None:
                stage = "review_bind"
                record.review = await AstrBotReviewAdapter.bind(
                    tools.context, policies.review.policy, privacy=tools.privacy
                )
            self._open()
            previous = self._calls.get(bound.call_id)
            if previous is not None and previous is not record:
                _fail("unavailable")
            # Register ALL adapters before the worker can claim anything.
            self._calls[bound.call_id] = record
            stage = "router"
            await self._router.resolve(bound, record.policy.review_manifest)
            self._open()
            stage = "create_call"
            await self._commit(
                "create_call",
                bound,
                policy=record.policy,
                retained_missing=True,
                query_policy=None,
                review_prompt=record.review_prompt,
            )
            if record.grant is None:
                # A delayed DB ACK or a retained failed preparation must not
                # authorize an adapter whose original parent/model has changed.
                stage = "grant_recheck"
                await record.reply.check()
                await record.audio._resolve()
                if record.speech is not None:
                    await record.speech.check()
                await record.review._resolve()
                self._open()
                stage = "issue_invitation"
                record.grant = await self._commit(
                    "issue_web_invitation",
                    bound.call_id,
                    record.secret,
                    session_ttl=self._config.session_ttl,
                )
            self._invitations[token_digest(record.secret.value, "invitation")] = record
            return record.grant
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception as error:
            failure = BackendError()
            attach_prepare(failure, stage, error)
            raise failure from None

    async def backfill_reviews(
        self, call_ids, *, counters, context_only=True, apply=False, allow_uncertain=False
    ):
        """Explicit exact-ID frozen-only recovery. Preview never registers capabilities."""
        self._open()
        if (
            type(call_ids) is not list
            or not 1 <= len(call_ids) <= 32
            or any(type(c) is not str or not c or len(c) > 96 or c.isspace() for c in call_ids)
            or len(set(call_ids)) != len(call_ids)
            or type(context_only) is not bool
            or type(apply) is not bool
            or type(allow_uncertain) is not bool
        ):
            _fail("recovery_unavailable")
        if allow_uncertain:
            if len(call_ids) != 1:
                _fail("recovery_unavailable")
            item = await self.retry_review(
                call_ids[0], counters=counters, preview=not apply, context_only=context_only
            )
            return dict(
                apply=apply,
                context_only=context_only,
                calls=[dict(call_id=call_ids[0], **item)],
                acknowledgement="scheduled" if apply else "preview",
            )
        previews = []
        # Validate the complete selection before granting any execution scope.
        for call_id in call_ids:
            state, saved = await self._runtime.inspect_frozen_call(call_id)
            if not saved.is_agent:
                _fail("recovery_unavailable")
            policy = saved.review_manifest.restore(state.binding, counters)
            previews.append(await self._runtime.preview_review(call_id, policy))
            previous = self._calls.get(call_id)
            if previous is not None and type(previous) is not _Recovery:
                _fail("recovery_unavailable")
        if apply:
            for item in previews:
                call_id = item["call_id"]
                if item["reason"] == "outcome_not_replayable":
                    continue
                await self._runtime.set_review_delivery(call_id, context_only=context_only)
                handle = self.begin_recovery(call_id, counters=counters)
                await self.recover(handle, retry=True)
        return dict(
            apply=apply,
            context_only=context_only,
            calls=previews,
            acknowledgement="scheduled" if apply else "preview",
        )

    async def retry_review(self, call_id, *, counters, preview, context_only):
        """Shared explicit recovery entry; no Agent, audio or historical scan."""
        self._open()
        binding, saved, info = await self._runtime.review_retry_info(call_id)
        if info["reason"] == "work_running" and info["detail"] == "回顾正在处理":
            return dict(code="running")
        if not info["retryable"] and info["state"] != "succeeded":
            return dict(code="not_retryable")
        await self._check_recovery_target(binding)
        from .native_provider import provider_info, NATIVE_CONTRACT, COUNTER_RULE

        context = self._tools.context
        pid = await context.get_current_chat_provider_id(binding.unified_msg_origin)
        provider = context.get_provider_by_id(pid)
        actual_id, model, window = provider_info(provider, "text_chat")
        if actual_id != pid:
            _fail("recovery_unavailable")
        original = saved.review_manifest.restore(binding, counters)
        policy = replace(
            original,
            provider_id=pid,
            model=model,
            context_window=window,
            cap_parameter=NATIVE_CONTRACT,
            count_payload_tokens=counters[COUNTER_RULE],
        )
        adapter = await AstrBotReviewAdapter.bind(context, policy, privacy=self._tools.privacy)
        if not preview and call_id not in self._calls:
            self._calls[call_id] = _Recovery(call_id, counters, binding=binding, policy=saved)
        return await self._runtime.retry_review(
            call_id, policy, COUNTER_RULE, adapter, preview=preview, context_only=context_only
        )

    def begin_recovery(self, call_id, *, counters):
        """Trusted host only. No web route, tickets, current defaults or bulk scan.

        Retain the handle before awaiting recover(). The trusted registry stays on
        this loop; until selection succeeds its owner may register a missing rule.
        """
        self._open()
        if (
            type(call_id) is not str
            or not call_id
            or call_id.isspace()
            or type(counters) is not dict
        ):
            _fail("recovery_unavailable")
        handle = self._recovery_handles.get(call_id)
        if handle is not None:
            if self._recoveries[handle._key].counters is not counters:
                _fail("recovery_unavailable")
            return handle
        handle = CallRecovery(self._owner, object())
        record = _Recovery(call_id, counters)
        self._recovery_handles[call_id] = handle
        self._recoveries[handle._key] = record
        self._schedule_recovery(record)
        return handle

    def _recovery_record(self, handle):
        if type(handle) is not CallRecovery or handle._owner is not self._owner:
            _fail("recovery_unavailable")
        record = self._recoveries.get(handle._key)
        if record is None:
            _fail("recovery_unavailable")
        return record

    def _schedule_recovery(self, record):
        record.task = asyncio.create_task(self._prepare_recovery(record), name="BotCall:recover")
        record.task.add_done_callback(_consume)

    async def recover(self, handle, *, retry=False):
        """ACK registration/progress request, not review/delivery completion."""
        self._open()
        record = self._recovery_record(handle)
        if type(retry) is not bool:
            _fail("recovery_unavailable")
        if retry and record.task.done():
            # Explicit finite reinspection also works after a successful ACK.
            # Pending-only claims, not a cached ACK, decide what may run again.
            self._schedule_recovery(record)
        result = await _wait_owned(record.task)
        self._open()
        return result

    async def _check_recovery_target(self, binding):
        async with self._tools.session_lock_manager.acquire_lock(binding.unified_msg_origin):
            conversation = await self._tools.context.conversation_manager.get_conversation(
                binding.unified_msg_origin, binding.conversation_id, create_if_not_exists=False
            )
            validate_conversation(
                conversation,
                umo=binding.unified_msg_origin,
                cid=binding.conversation_id,
                platform_id=binding.platform_id,
            )

    async def _prepare_recovery(self, record):
        try:
            state, saved, source = await self._runtime.inspect_recovery_call(record.call_id)
            if record.binding is None:
                record.binding, record.policy, record.frozen_review = (
                    state.binding,
                    saved,
                    state.review,
                )
                record.closing_source = source
            if (
                record.binding != state.binding
                or record.policy != saved
                or (source is not None and record.closing_source != source)
            ):
                _fail("recovery_unavailable")
            await self._check_recovery_target(record.binding)
            if not saved.is_agent:
                _fail(
                    "recovery_unavailable"
                )  # Read legacy data without changing its execution contract.
            if state.review is None:
                first = next((job for job in state.audio_jobs if not audio_settled(job)), None)
                missing = await self._runtime.audio_missing_enabled(record.call_id)
                recoverable = lambda job: (
                    audio_settled(job)
                    or job.state == "pending"
                    or (missing and job.state in {"failed", "uncertain", "cancelled"})
                )
                needs_audio = (
                    first is not None
                    and first.state != "running"
                    and all(recoverable(job) for job in state.audio_jobs)
                    and any(job.state == "pending" for job in state.audio_jobs)
                )
                needs_review = all(recoverable(job) for job in state.audio_jobs) and (
                    bool(state.turns) or any(job.state == "pending" for job in state.audio_jobs)
                )
            else:
                needs_audio = False
                needs_review = (
                    state.review.state == "pending"
                    if state.review_work is None
                    else state.review_work.state == "pending"
                )
            if needs_audio:
                if record.audio is None:
                    record.audio = await AstrBotAudioAdapter.bind(
                        self._tools.context,
                        saved.audio,
                        converter=self._tools.stt_converter or self._tools.converter,
                        privacy=self._tools.privacy,
                    )
                else:
                    await record.audio._resolve()
            if needs_review:
                if record.selection is None:
                    manifest = saved.review_manifest
                    policy = manifest.restore(record.binding, record.counters)
                    record.selection = ReviewPolicySelection(policy, manifest.counter_rule_id)
                if record.review is None:
                    record.review = await AstrBotReviewAdapter.bind(
                        self._tools.context, record.selection.policy, privacy=self._tools.privacy
                    )
                else:
                    await record.review._resolve()
            self._open()
            previous = self._calls.get(record.call_id)
            if previous is not None and previous is not record:
                _fail("recovery_unavailable")
            # Register capabilities before the worker can adopt or claim.
            self._calls[record.call_id] = record
            if needs_review:
                await self._router.resolve(record.binding, saved.review_manifest)
            self._open()
            if record.closing_source is not None:
                return await self._commit(
                    "recover_closing_call", record.binding, saved, record.closing_source
                )
            return await self._commit(
                "recover_frozen_call", record.binding, saved, record.frozen_review
            )
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except ReviewPolicyUnavailable as error:
            _fail(
                "counter_unavailable"
                if error.code == "counter_unavailable"
                else "recovery_unavailable"
            )
        except Exception:
            _fail("recovery_unavailable")

    async def _commit(self, operation, *args, **kwargs):
        # A preparation cancelled during a committed request must not cancel
        # its actual Runtime waiter. Keep it until its real ACK/error is taken.
        task = asyncio.create_task(
            getattr(self._runtime, operation)(*args, **kwargs), name="BotCall:backend-commit"
        )
        self._commits.add(task)

        def done(completed):
            self._commits.discard(completed)
            _consume(completed)

        task.add_done_callback(done)
        return await _wait_owned(task)

    def _entry(self, binding):
        self._check_loop()
        record = self._calls.get(binding.call_id) if type(binding) is CallBinding else None
        if record is None or record.binding != binding:
            _fail("unavailable")
        return record

    async def _resolve_review(self, binding, saved):
        try:
            entry = self._entry(binding)
            selection = entry.selection if type(entry) is _Recovery else entry.policies.review
            if selection is None:
                raise ReviewPolicyUnavailable()
            if (
                saved is not None
                and ReviewManifest.from_policy(selection.policy, selection.counter_rule_id) != saved
            ):
                raise ReviewPolicyUnavailable("mismatch")
            return selection
        except ReviewPolicyUnavailable as error:
            raise ReviewPolicyUnavailable(error.code) from None
        except Exception:
            raise ReviewPolicyUnavailable() from None

    async def _bind_review(self, policy):
        entry = self._entry(policy.binding)
        selection = entry.selection if type(entry) is _Recovery else entry.policies.review
        if selection is None or selection.policy is not policy:
            raise ReviewPolicyUnavailable("mismatch") from None
        # Keep the original adapter/parent identity, but revalidate its accepted
        # Provider surface before the worker plans or claims a review node.
        await entry.review._resolve()
        return entry.review

    async def _reply(self, binding, turn):
        entry = self._entry(binding)
        if getattr(entry, "speech", None) is not None:
            await entry.speech.check()
        return await entry.reply.reply(binding, turn)

    async def _synthesize(self, binding, turn, guard):
        entry = self._entry(binding)
        if type(entry) is not _Preparation or entry.speech is None:
            from .speech import SpeechUnavailable

            raise SpeechUnavailable() from None
        pcm = None
        if getattr(turn, "segment_seq", None) == 1 and entry.speech.policy.dedicated:
            pcm = await self._runtime.prepare_pcm(binding, turn.material_id)
        try:
            return await entry.speech.synthesize(
                binding, turn, before_invoke=lambda: guard(binding, turn.material_id), pcm=pcm
            )
        finally:
            if pcm is not None:
                pcm.close()

    async def _transcribe(self, binding, job):
        entry = self._entry(binding)
        if getattr(entry, "speech", None) is not None:
            try:
                await entry.speech.check()
            except Exception:
                from .audio import AudioNotApplied

                raise AudioNotApplied() from None
        return await entry.audio.transcribe(binding, job)

    async def _legacy(self, *args):
        _fail("unavailable")

    async def _send(self, binding, page):
        self._entry(binding)
        try:
            return await self._send_page(binding, page)
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            _fail()

    async def _context(self, binding, key, body):
        self._entry(binding)
        try:
            return await self._make_context(binding, key, body)
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            _fail()

    @property
    def audio_available(self):
        self._check_loop()
        return self._ready and not self._closing

    def _session(self, token, reference):
        entry = self._sessions.get(token_digest(token, "session"))
        try:
            if (
                entry is None
                or type(reference) is not str
                or not hmac.compare_digest(entry[1].encode("ascii"), reference.encode("ascii"))
            ):
                raise ValueError
        except Exception:
            raise AccessError() from None
        self._entry(entry[0].binding)

    async def _web(self, operation, *args):
        self._open()
        # This additional in-memory fence never chooses a target. The existing
        # worker still authenticates cookie/reference and derives the real call.
        if operation == "exchange_web_invitation":
            record = self._invitations.get(token_digest(args[0], "invitation"))
            if record is None:
                raise AccessError() from None
        else:
            self._session(args[0], args[1])

        async def accepted():
            if operation in {"web_audio", "web_audio_commit"}:
                entry = self._sessions[token_digest(args[0], "session")][0]
                if entry.speech is not None:
                    await entry.speech.check()
            result = await getattr(self._runtime, operation)(*args)
            if operation == "web_state" and self._tools.speech_enabled:
                entry = self._sessions[token_digest(args[0], "session")][0]
                try:
                    await entry.speech.check()
                except Exception:
                    from dataclasses import replace

                    result = replace(result, speech_available=False)
            if operation == "exchange_web_invitation":
                self._sessions[token_digest(result.token, "session")] = (record, result.reference)
                self._start_audio_warmup(record)
            return result

        task = asyncio.create_task(accepted(), name="BotCall:backend-web")
        self._web_tasks.add(task)

        def done(completed):
            self._web_tasks.discard(completed)
            _consume(completed)

        task.add_done_callback(done)
        return await _wait_owned(task)

    def _start_audio_warmup(self, record):
        call_id = record.binding.call_id
        if self._closing or self._tools.audio_warmup is None or call_id in self._audio_warmed:
            return
        self._audio_warmed.add(call_id)

        async def run():
            started, outcome = self._loop.time(), "failed"
            try:
                await asyncio.wait_for(self._tools.audio_warmup(), timeout=3)
                outcome = "succeeded"
            except asyncio.CancelledError:
                outcome = "cancelled"
                raise
            except Exception:
                pass  # Local optimization only; never affects auth, capture or Providers.
            finally:
                logging.getLogger(__name__).info(
                    "bot_call.audio_warmup outcome=%s elapsed_ms=%.0f",
                    outcome,
                    (self._loop.time() - started) * 1000,
                )

        task = asyncio.create_task(run(), name="BotCall:audio-warmup")
        self._warmup_tasks.add(task)

        def done(completed):
            self._warmup_tasks.discard(completed)
            _consume(completed)

        task.add_done_callback(done)

    async def exchange_web_invitation(self, token):
        return await self._web("exchange_web_invitation", token)

    async def web_state(self, token, reference, wait_ms=0, utterance_id=None):
        state = await self._web("web_state", token, reference, wait_ms, utterance_id)
        record = self._calls.get(state.call_id)
        return replace(state, bot_name=getattr(record, "bot_name", "Bot"))

    async def web_submit(self, token, reference, utterance_id, text):
        return await self._web("web_submit", token, reference, utterance_id, text)

    async def _interrupt(self, binding, gid):
        entry = self._entry(binding)
        if entry.reply is not None:
            await entry.reply.interrupt(gid)

    async def web_barge_in(self, token, reference, data):
        return await self._web("web_barge_in", token, reference, data)

    async def web_audio(self, token, reference, utterance_id, data, mime, series=None):
        return await self._web("web_audio", token, reference, utterance_id, data, mime, series)

    async def web_audio_chunk(self, token, reference, utterance_id, seq, offset, data, mime):
        return await self._web(
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
        return await self._web(
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
        return await self._web(
            "web_audio_series_control", token, reference, sid, action, last_index, reason
        )

    async def web_audio_resolution(self, token, reference, utterance_id):
        return await self._web("web_audio_resolution", token, reference, utterance_id)

    async def web_speech(self, token, reference, generation_id, wait_ms=0):
        return await self._web("web_speech", token, reference, generation_id, wait_ms)

    def watch(self, token, reference, *, pcm=False):
        self._open()
        self._session(token, reference)
        return self._runtime.watch(token, reference, pcm=pcm)

    async def web_metrics(self, token, reference, gid, values):
        return await self._web("web_metrics", token, reference, gid, values)

    async def web_segment(self, token, reference, gid, seq, receipt=None):
        return await self._web("web_segment", token, reference, gid, seq, receipt)

    async def web_speech_audio(self, token, reference, generation_id):
        return await self._web("web_speech_audio", token, reference, generation_id)

    async def web_playback(
        self, token, reference, generation_id, revision, state, first_sound_ms=None
    ):
        return await self._web(
            "web_playback", token, reference, generation_id, revision, state, first_sound_ms
        )

    async def web_heartbeat(self, token, reference):
        return await self._web("web_heartbeat", token, reference)

    async def web_end(self, token, reference):
        return await self._web("web_end", token, reference)

    async def force_end_call(self, ref):
        self._open()
        return await self._runtime.force_end_call(ref)

    async def read_state(self, handle):
        self._open()
        record = (
            self._recovery_record(handle) if type(handle) is CallRecovery else self._record(handle)
        )
        self._entry(record.binding)
        try:
            return await self._runtime.read_state(record.binding.call_id)
        except Exception:
            _fail()

    async def drain(self):
        self._open()
        await self._runtime.drain()  # Completion here is NOT a success assertion.

    async def shutdown(self, *, timeout=5.0):
        self._check_loop()
        if timeout is not None and (
            type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout < 0
        ):
            _fail("invalid_configuration")
        self._closing, self._ready = True, False
        if self._close_task is None or (
            self._close_task.done() and not self._close_task.result().stopped
        ):
            self._close_task = asyncio.create_task(self._close(), name="BotCall:backend-close")
            self._close_task.add_done_callback(_consume)
        if timeout is None:
            return await _wait_owned(self._close_task)
        try:
            return await asyncio.wait_for(_wait_owned(self._close_task), timeout)
        except TimeoutError:
            return ShutdownReport(False, "still_stopping")

    async def _close(self):
        try:
            return await self._finish_close()
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            # Never turn an unexpected close fault into a fake stopped result,
            # or expose raw lifecycle exceptions to the future plugin host.
            return ShutdownReport(False, "shutdown_unconfirmed")

    async def _finish_close(self):
        start_failed = False
        if self._start_task is not None:
            try:
                await _wait_owned(self._start_task)
            except Exception:
                start_failed = True
        records = (*self._preparations.values(), *self._recoveries.values())
        tasks = [r.task for r in records if r.task is not None and not r.task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._commits:
            await asyncio.gather(*tuple(self._commits), return_exceptions=True)
        if self._runtime is not None:
            # Wake accepted long waits before gathering web tasks. This signal
            # neither cancels SDK tasks nor releases their actual ownership.
            await self._runtime.stop_speech_waits()
            await self._runtime.stop_state_waits()
        if self._web_tasks:
            await asyncio.gather(*tuple(self._web_tasks), return_exceptions=True)
        for task in tuple(self._warmup_tasks):
            task.cancel()
        if self._warmup_tasks:
            await asyncio.gather(*tuple(self._warmup_tasks), return_exceptions=True)
        report = (
            await self._runtime.shutdown(timeout=None)
            if self._runtime is not None
            else ShutdownReport(True)
        )
        try:
            for record in (*self._preparations.values(), *self._recoveries.values()):
                if getattr(record, "reply", None) is not None:
                    await record.reply.cleanup_pending()
                if record.audio is not None:
                    await record.audio.cleanup_pending()
                if getattr(record, "speech", None) is not None:
                    await record.speech.cleanup_pending()
        except Exception:
            return ShutdownReport(False, "cleanup_unconfirmed")
        # Retain records for inspection; never close borrowed clients or discard
        # live references while callbacks/resources may still exist.
        return ShutdownReport(
            report.stopped, "startup_failed" if start_failed else report.error_code
        )
