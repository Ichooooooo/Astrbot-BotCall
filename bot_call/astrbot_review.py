"""Public native review execution with byte-compatible legacy request decoding.

Native output allowances are input planning reserves, not enforced SDK caps.
No SDK or concrete Provider implementation is imported or inspected here.
"""

import asyncio
import logging
from dataclasses import asdict, dataclass, field
import inspect
import json
from types import SimpleNamespace

from .astrbot_adapter import CallSnapshot
from .binding import CallBinding
from .review_plan import _json, _sha, _SYSTEM, _request, TurnFragment
from .review_work import ModelFailed, NotApplied, _reduction_request
from .store import ReviewWorkNode, Turn
from .native_provider import (
    NATIVE_CONTRACT,
    provider_info,
    call_payload,
    response_text,
    observed_finish,
)


_REVIEW_FIELDS = {
    "stage": frozenset({"preflight", "native", "response", "complete"}),
    "reason": frozenset(
        {
            "preflight",
            "entered",
            "native_error",
            "timeout",
            "envelope",
            "finish",
            "index",
            "role",
            "refusal",
            "tool_call",
            "function_call",
            "audio",
            "content_type",
            "empty",
            "utf8",
            "confirmed",
            "cancelled",
            "privacy",
            "unknown",
        }
    ),
    "outcome": frozenset(
        {"not_applied", "uncertain", "succeeded", "failed", "cancelled", "unknown"}
    ),
    "finish": frozenset(
        {"stop", "length", "content_filter", "tool_calls", "function_call", "unknown"}
    ),
    "output": frozenset({"text", "nontext", "empty", "unknown"}),
}


def _review_observe(note, **values):
    # Internal bounded projection only; never retain response/errors or identifiers.
    for key, value in values.items():
        if key == "native_calls":
            note[key] = value if type(value) is int and value in (0, 1) else 0
        elif key in _REVIEW_FIELDS:
            note[key] = value if type(value) is str and value in _REVIEW_FIELDS[key] else "unknown"


def _observe(note, **values):
    try:
        _review_observe(note, **values)
    except BaseException:
        pass  # Instrumentation cannot change the business exception/cancellation.


def _review_diagnostic(note):
    try:
        if type(note) is not dict:
            note = {}
        safe = {
            key: value if type(value := note.get(key)) is str and value in allowed else "unknown"
            for key, allowed in _REVIEW_FIELDS.items()
        }
        calls = note.get("native_calls")
        safe["native_calls"] = calls if type(calls) is int and calls in (0, 1) else 0
        logging.getLogger("astrbot").info(
            "bot_call.review stage=%s reason=%s outcome=%s finish=%s output=%s native_calls=%s",
            safe["stage"],
            safe["reason"],
            safe["outcome"],
            safe["finish"],
            safe["output"],
            safe["native_calls"],
        )
    except BaseException:
        pass  # Logger/filter/observer failure is never a review result.


class ReviewResponseUncertain(RuntimeError):
    """A request was attempted; neither retry nor successful review is implied."""


def _reject():
    raise NotApplied("review_adapter.not_applied") from None


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _load(text):
    return json.loads(text, object_pairs_hook=_pairs, parse_constant=lambda unused: _reject())


def _text(value):
    if type(value) is not str or not value or value.isspace():
        raise ValueError
    value.encode("utf-8")


def _digest(value):
    if (
        type(value) is not str
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ValueError


@dataclass(frozen=True, slots=True)
class ReviewRequestPolicy:
    """Detached frozen policy, with legacy decode and native public-call modes.

    Native counting covers the complete public envelope, not downstream vendor
    serialization. output_token_limit is a planning reserve in native mode.
    Legacy cap names retain their exact historical payload transform for reads.
    """

    binding: CallBinding = field(repr=False)
    provider_id: str = field(repr=False)
    model: str = field(repr=False)
    context_window: int
    output_token_limit: int
    safety_token_reserve: int
    cap_parameter: str
    count_payload_tokens: object = field(repr=False)

    def __post_init__(self):
        try:
            if type(self.binding) is not CallBinding:
                raise ValueError
            for name, value in asdict(self.binding).items():
                if name == "persona_id" and value is None:
                    continue
                _text(value)
            bound = self.binding
            if (
                bound.message_type != "FriendMessage"
                or bound.unified_msg_origin
                != f"{bound.platform_id}:{bound.message_type}:{bound.session_id}"
            ):
                raise ValueError
            _text(self.provider_id)
            _text(self.model)
            if (
                any(
                    type(v) is not int or v <= 0
                    for v in (
                        self.context_window,
                        self.output_token_limit,
                        self.safety_token_reserve,
                    )
                )
                or self.input_token_budget <= 0
            ):
                raise ValueError
            if self.cap_parameter not in {"max_completion_tokens", "max_tokens", NATIVE_CONTRACT}:
                raise ValueError
            counter = self.count_payload_tokens
            method = getattr(counter, "__call__", None)
            if not callable(counter) or any(
                inspect.iscoroutinefunction(v)
                or inspect.isasyncgenfunction(v)
                or inspect.isgeneratorfunction(v)
                for v in (counter, method)
            ):
                raise ValueError
        except Exception:
            _reject()

    @classmethod
    def from_snapshot(
        cls,
        snapshot: CallSnapshot,
        *,
        model: str,
        output_token_limit: int,
        safety_token_reserve: int,
        cap_parameter: str,
        count_payload_tokens,
    ):
        try:
            if type(snapshot) is not CallSnapshot:
                raise ValueError
            # Copy only these immutable fields. Never capture history/persona,
            # provider_settings, provider_config, SDK clients or credentials.
            return cls(
                snapshot.binding,
                snapshot.provider_id,
                model,
                snapshot.provider_max_context_tokens,
                output_token_limit,
                safety_token_reserve,
                cap_parameter,
                count_payload_tokens,
            )
        except Exception:
            _reject()

    @property
    def input_token_budget(self) -> int:
        return self.context_window - self.output_token_limit - self.safety_token_reserve

    def _envelope(self, request_json):
        request = _load(request_json)
        if (
            type(request) is not dict
            or set(request) != {"messages"}
            or type(request["messages"]) is not list
            or len(request["messages"]) != 2
            or _json(request) != request_json
        ):
            raise ValueError
        for message, role in zip(request["messages"], ("system", "user")):
            if (
                type(message) is not dict
                or set(message) != {"role", "content"}
                or message["role"] != role
            ):
                raise ValueError
            _text(message["content"])
        data = _load(request["messages"][1]["content"])
        if type(data) is not dict or data["call_id"] != self.binding.call_id:
            raise ValueError
        _digest(data["input_sha256"])
        _digest(data["prompt_sha256"])
        if type(data["review_prompt"]) is not str or _sha(data["review_prompt"]) != data.get(
            "instructions_sha256", data["prompt_sha256"]
        ):
            raise ValueError
        if type(data["ordinal"]) is not int or data["ordinal"] <= 0:
            raise ValueError
        if data["format"] == "bot-call-review-input-v1":
            names = ("format", "call_id", "input_sha256", "prompt_sha256", "ordinal", "ranges")
            if (
                request["messages"][0]["content"] != _SYSTEM
                or data["purpose"] != "partial_material_for_later_review_processing"
                or type(data["fragments"]) is not list
                or not data["fragments"]
            ):
                raise ValueError
            ranges = [
                [f["turn"]["sequence"], *f["body_range"], f["body_length"]]
                for f in data["fragments"]
            ]
            if ranges != data["ranges"]:
                raise ValueError
            key = data["format"] + ":" + _sha(_json({k: data[k] for k in names}))
        elif data["format"] == "bot-call-review-reduction-v1":
            names = (
                "format",
                "kind",
                "call_id",
                "plan_fingerprint",
                "input_sha256",
                "prompt_sha256",
                "level",
                "ordinal",
                "map_range",
                "children",
            )
            _digest(data["plan_fingerprint"])
            if (
                data["kind"] not in {"merge", "final"}
                or type(data["level"]) is not int
                or data["level"] < 1
            ):
                raise ValueError
            if (
                type(data["notes"]) is not list
                or not data["notes"]
                or len(data["children"]) != len(data["notes"])
            ):
                raise ValueError
            position = data["map_range"][0]
            for child, note in zip(data["children"], data["notes"]):
                _digest(child["result_sha256"])
                if (
                    note["origin"] != "generated_chunk_note_not_user_quote"
                    or child != {k: note[k] for k in ("source_key", "result_sha256", "map_range")}
                    or _sha(note["body"]) != child["result_sha256"]
                    or child["map_range"][0] != position
                    or child["map_range"][1] <= position
                ):
                    raise ValueError
                position = child["map_range"][1]
            if position != data["map_range"][1]:
                raise ValueError
            key = "review-work:" + _sha(_json({k: data[k] for k in names}))
        else:
            raise ValueError
        if data["source_key"] != key:
            raise ValueError
        # Reconstruct the exact known request format, including system text and
        # all field names. These tiny views carry only frozen source fields;
        # they are not synthetic database jobs or live Context objects.
        source = SimpleNamespace(
            call_id=data["call_id"],
            input_sha256=data["input_sha256"],
            prompt=data["review_prompt"],
            request_prompt=data["review_prompt"],
            frozen_prompt_sha256=data["prompt_sha256"],
        )
        if data["format"] == "bot-call-review-input-v1":
            fragments = tuple(
                TurnFragment(Turn(**f["turn"]), *f["body_range"], f["body_length"])
                for f in data["fragments"]
            )
            expected = _request(source, data["prompt_sha256"], data["ordinal"], fragments)
        else:
            work = SimpleNamespace(
                call_id=data["call_id"],
                fingerprint=data["plan_fingerprint"],
                level=data["level"] - 1,
                review=source,
            )
            children = tuple(
                SimpleNamespace(
                    source_key=n["source_key"],
                    result_sha256=n["result_sha256"],
                    first_map=n["map_range"][0],
                    stop_map=n["map_range"][1],
                    body=n["body"],
                )
                for n in data["notes"]
            )
            expected = _reduction_request(work, data["kind"], children, data["ordinal"])
        if expected != (key, request_json):
            raise ValueError
        return request, data

    def payload_json(self, request_json: str) -> str:
        """Count/send the native envelope; preserve old SDK envelopes for reads."""
        try:
            request, _ = self._envelope(request_json)
            if self.cap_parameter == NATIVE_CONTRACT:
                return _json(call_payload(self.provider_id, request["messages"]))
            return _json(
                {
                    "messages": request["messages"],
                    "model": self.model,
                    self.cap_parameter: self.output_token_limit,
                    "stream": False,
                    "n": 1,
                    "store": False,
                }
            )
        except Exception:
            _reject()

    def count_request(self, request_json: str) -> int:
        """Pass this pure method to RuntimeReviewWork, on its worker thread."""
        try:
            value = self.count_payload_tokens(self.payload_json(request_json))
            if type(value) is not int or value < 0:
                raise ValueError
            return value
        except Exception:
            _reject()

    def checked_payload(self, node: ReviewWorkNode) -> dict:
        """Validate a trusted Store node, already recounted by the worker.

        Does not run a counter on the business loop. A hand-forged internal
        node/count is not a budget attestation; use the runtime_mode contract.
        """
        try:
            if (
                type(node) is not ReviewWorkNode
                or node.call_id != self.binding.call_id
                or node.state != "running"
                or type(node.token_count) is not int
                or any(
                    type(v) is not int
                    for v in (node.level, node.ordinal, node.first_map, node.stop_map)
                )
                or node.level < 0
                or node.ordinal < 1
                or node.first_map < 0
                or node.stop_map <= node.first_map
                or not 0 <= node.token_count <= self.input_token_budget
            ):
                raise ValueError
            _, data = self._envelope(node.request_json)
            if node.source_key != data["source_key"] or node.ordinal != data["ordinal"]:
                raise ValueError
            if node.kind == "map":
                if (
                    data["format"] != "bot-call-review-input-v1"
                    or node.level != 0
                    or node.child_keys
                    or (node.first_map, node.stop_map) != (node.ordinal - 1, node.ordinal)
                ):
                    raise ValueError
            elif (
                data["format"] != "bot-call-review-reduction-v1"
                or node.kind != data["kind"]
                or node.level != data["level"]
                or [node.first_map, node.stop_map] != data["map_range"]
                or node.child_keys != tuple(c["source_key"] for c in data["children"])
            ):
                raise ValueError
            return _load(self.payload_json(node.request_json))
        except Exception:
            _reject()


class AstrBotReviewAdapter:
    """Business-loop owner of borrowed objects; never closes the shared client.

    Trusted class arguments are capability attestations by the plugin owner:
    production must pass the verified ProviderOpenAIOfficial/AsyncOpenAI classes.
    Other concrete types/subclasses or unknown configuration are rejected. This
    is intentionally not an automatic generic Provider compatibility layer.
    """

    def __init__(self, context, policy, *, privacy=None):
        self._loop = asyncio.get_running_loop()
        if type(policy) is not ReviewRequestPolicy or policy.cap_parameter != NATIVE_CONTRACT:
            _reject()  # Old manifests are decoded, never transparently re-executed.
        self.policy, self._context, self._privacy = policy, context, privacy
        self._provider = None

    @classmethod
    async def bind(cls, context, policy, *, privacy=None):
        adapter = cls(context, policy, privacy=privacy)
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
            selected = await self._context.get_current_chat_provider_id(
                self.policy.binding.unified_msg_origin
            )
            if selected != self.policy.provider_id:
                raise ValueError
            provider = self._context.get_provider_by_id(selected)
            if provider_info(provider, "text_chat") != (
                selected,
                self.policy.model,
                self.policy.context_window,
            ) or (self._provider is not None and self._provider is not provider):
                raise ValueError
            self._provider = provider
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except Exception:
            _reject()

    def runtime_mode(self):
        self._check_loop()
        from .runtime import RuntimeReviewWork

        return RuntimeReviewWork(
            self.policy.input_token_budget, self.policy.count_request, self.generate
        )

    async def generate(self, node: ReviewWorkNode) -> str:
        note = {
            "stage": "preflight",
            "reason": "preflight",
            "outcome": "not_applied",
            "finish": "unknown",
            "output": "unknown",
            "native_calls": 0,
        }
        try:
            self._check_loop()  # Before inspecting node, Context or any Provider resource.
            try:
                payload = self.policy.checked_payload(node)
                await self._resolve()
                if self._privacy is None:
                    _reject()
                self._privacy.check()
            except asyncio.CancelledError:
                raise asyncio.CancelledError() from None
            except Exception:
                _reject()
            from .sdk_privacy import SDKPrivacyError

            try:
                with self._privacy.scope():
                    return await self._generate_once(payload, note)
            except SDKPrivacyError:
                _observe(note, stage="preflight", reason="privacy")
                _reject()
        except NotApplied:
            _observe(note, outcome="not_applied")
            raise
        except asyncio.CancelledError:
            _observe(note, reason="cancelled", outcome="cancelled")
            raise
        finally:
            # Emission is AFTER the real native coroutine and privacy scope exit.
            # No free exception text, traceback, body, token, path or identity.
            try:
                _review_diagnostic(note)
            except BaseException:
                pass

    async def _generate_once(self, payload, note):
        # Exactly one public invocation; Provider/SDK internal retries are native.
        try:
            await self._resolve()
            _observe(note, stage="native", reason="entered", outcome="uncertain", native_calls=1)
            try:
                response = await self._context.llm_generate(**payload)
            except Exception as error:
                from .failures import definite_review_failure

                code = definite_review_failure(error)
                if code is not None:
                    _observe(note, reason="native_error", outcome="failed")
                    raise ModelFailed(code) from None
                _observe(note, reason="timeout" if type(error) is TimeoutError else "native_error")
                raise
            _observe(note, stage="response", reason="finish", finish=observed_finish(response))
            body = response_text(response)
            _observe(note, stage="complete", reason="confirmed", outcome="succeeded", output="text")
            return body
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except ModelFailed:
            raise
        except Exception:
            raise ReviewResponseUncertain("review_adapter.result_unconfirmed") from None
