"""Pure, closed, non-sensitive manifests for one controlled review policy.

Counter names select an explicit trusted in-memory registry, never Python code
from SQLite. No live clients, credentials, URLs, history or callables serialize.
"""

from dataclasses import asdict, dataclass, field
import hashlib
import json
import re

from .binding import CallBinding
from .native_provider import NATIVE_CONTRACT


def _json(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _sha(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _profile(native=False):
    if native:
        return {
            "format": "bot-call-astrbot-review-payload-v2",
            "messages": "exact-frozen-system-user-v1",
            "invocation": "Context.llm_generate",
            "request_max_retries": 1,
            "output_allowance": "input-planning-reserve-only",
            "parameters_and_internal_retries": "astrbot-owned",
            "response": "native-llmresponse-explicit-incomplete-v1",
            "map_format": "bot-call-review-input-v1",
            "reduction_format": "bot-call-review-reduction-v1",
        }
    return {
        "format": "bot-call-openai-review-payload-v1",
        "stream": False,
        "n": 1,
        "store": False,
        "extra_body": "forbidden",
        "sdk_max_retries": 0,
        "messages": "exact-frozen-system-user-v1",
        "map_format": "bot-call-review-input-v1",
        "reduction_format": "bot-call-review-reduction-v1",
    }


class ReviewPolicyUnavailable(RuntimeError):
    def __init__(self, code="unavailable"):
        if code not in {"unavailable", "mismatch", "counter_unavailable"}:
            code = "unavailable"
        self.code = code
        super().__init__("review_policy." + code)


def _unavailable(code="mismatch"):
    raise ReviewPolicyUnavailable(code) from None


@dataclass(frozen=True, slots=True)
class ReviewManifest:
    canonical_json: str = field(repr=False)
    fingerprint: str

    def __post_init__(self):
        try:
            data = json.loads(self.canonical_json)
            keys = {
                "version",
                "binding",
                "provider_id",
                "model",
                "context_window",
                "output_token_limit",
                "safety_token_reserve",
                "input_token_budget",
                "cap_parameter",
                "counter_rule_id",
                "payload",
            }
            if (
                type(data) is not dict
                or set(data) != keys
                or _json(data) != self.canonical_json
                or _sha(self.canonical_json) != self.fingerprint
                or data["version"]
                != (
                    "bot-call-review-policy-v2"
                    if data["cap_parameter"] == NATIVE_CONTRACT
                    else "bot-call-review-policy-v1"
                )
                or _json(data["payload"])
                != _json(_profile(data["cap_parameter"] == NATIVE_CONTRACT))
            ):
                raise ValueError
            bound = CallBinding(**data["binding"])
            for key, value in asdict(bound).items():
                if key == "persona_id" and value is None:
                    continue
                if type(value) is not str or not value or value.isspace():
                    raise ValueError
                value.encode("utf-8")
            if (
                bound.message_type != "FriendMessage"
                or bound.unified_msg_origin
                != f"{bound.platform_id}:{bound.message_type}:{bound.session_id}"
            ):
                raise ValueError
            for key in ("provider_id", "model"):
                if type(data[key]) is not str or not data[key] or data[key].isspace():
                    raise ValueError
            for key in (
                "context_window",
                "output_token_limit",
                "safety_token_reserve",
                "input_token_budget",
            ):
                if type(data[key]) is not int or data[key] <= 0:
                    raise ValueError
            if (
                data["input_token_budget"]
                != data["context_window"]
                - data["output_token_limit"]
                - data["safety_token_reserve"]
                or data["cap_parameter"]
                not in {"max_tokens", "max_completion_tokens", NATIVE_CONTRACT}
                or type(data["counter_rule_id"]) is not str
                or re.fullmatch(r"[A-Za-z0-9_.:-]{1,96}", data["counter_rule_id"]) is None
            ):
                raise ValueError
        except Exception:
            _unavailable()

    @classmethod
    def from_policy(cls, policy, counter_rule_id):
        from .astrbot_review import ReviewRequestPolicy

        try:
            if type(policy) is not ReviewRequestPolicy:
                raise ValueError
            data = {
                "version": "bot-call-review-policy-v2"
                if policy.cap_parameter == NATIVE_CONTRACT
                else "bot-call-review-policy-v1",
                "binding": asdict(policy.binding),
                "counter_rule_id": counter_rule_id,
                "payload": _profile(policy.cap_parameter == NATIVE_CONTRACT),
            }
            for key in (
                "provider_id",
                "model",
                "context_window",
                "output_token_limit",
                "safety_token_reserve",
                "input_token_budget",
                "cap_parameter",
            ):
                data[key] = getattr(policy, key)
            encoded = _json(data)
            return cls(encoded, _sha(encoded))
        except Exception:
            _unavailable()

    @property
    def is_native(self):
        return json.loads(self.canonical_json)["version"] == "bot-call-review-policy-v2"

    @property
    def binding(self):
        return CallBinding(**json.loads(self.canonical_json)["binding"])

    @property
    def input_token_budget(self):
        return json.loads(self.canonical_json)["input_token_budget"]

    @property
    def counter_rule_id(self):
        return json.loads(self.canonical_json)["counter_rule_id"]

    def restore(self, binding, counters):
        """Rebuild from fixed fields plus a trusted, versioned local registry."""
        from .astrbot_review import ReviewRequestPolicy

        try:
            if binding != self.binding:
                _unavailable()
            data = json.loads(self.canonical_json)
            if data["counter_rule_id"] not in counters:
                _unavailable("counter_unavailable")
            result = ReviewRequestPolicy(
                binding,
                data["provider_id"],
                data["model"],
                data["context_window"],
                data["output_token_limit"],
                data["safety_token_reserve"],
                data["cap_parameter"],
                counters[data["counter_rule_id"]],
            )
            if self.from_policy(result, data["counter_rule_id"]) != self:
                _unavailable()
            return result
        except ReviewPolicyUnavailable as error:
            raise ReviewPolicyUnavailable(error.code) from None
        except Exception:
            _unavailable()


@dataclass(frozen=True, slots=True)
class ReviewPolicyState:
    status: str
    manifest: ReviewManifest | None = field(default=None, repr=False)
