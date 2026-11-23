"""Immutable policy identity, not authorization or a live adapter registry.

Only closed scalar policies serialize. SDK objects, counters and configuration
stay with the trusted host; the existing review registry restores its counter.
"""

from dataclasses import asdict, dataclass, field, fields
import hashlib
import json

from .binding import CallBinding
from .review_policy import ReviewManifest
from .native_provider import NATIVE_CONTRACT, DECISION_CONTRACT


class CallPolicyError(ValueError):
    def __init__(self):
        super().__init__("call_policy.invalid")


def _json(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _sha(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class CallPolicy:
    canonical_json: str = field(repr=False)
    fingerprint: str

    def __post_init__(self):
        try:
            if type(self.canonical_json) is not str or type(self.fingerprint) is not str:
                raise ValueError
            data = json.loads(self.canonical_json)
            if type(data) is dict and data.get("version") == "bot-call-policy-v3":
                if (
                    set(data)
                    != {
                        "version",
                        "binding",
                        "agent",
                        "audio",
                        "review",
                        "reply_contract",
                        "audio_contract",
                    }
                    or data["reply_contract"] != "bot-call-native-agent-v1"
                    or data["audio_contract"] != "bot-call-native-stt-v1"
                    or _json(data) != self.canonical_json
                    or _sha(self.canonical_json) != self.fingerprint
                ):
                    raise ValueError
                self._parts(data)
                return
            native = data.get("version") == "bot-call-policy-v2"
            keys = {
                "version",
                "binding",
                "reply",
                "audio",
                "review",
                "reply_contract",
                "audio_contract",
            }
            if native:
                keys.add("decision_contract")
            if (
                type(data) is not dict
                or set(data) != keys
                or data["version"] != ("bot-call-policy-v2" if native else "bot-call-policy-v1")
                or data["reply_contract"]
                != ("bot-call-native-reply-v1" if native else "bot-call-reply-request-v1")
                or data["audio_contract"]
                != ("bot-call-native-stt-v1" if native else "bot-call-stt-request-v1")
                or (native and data["decision_contract"] != DECISION_CONTRACT)
                or _json(data) != self.canonical_json
                or _sha(self.canonical_json) != self.fingerprint
            ):
                raise ValueError
            self._parts(data)  # Delegate parameter/binding checks to existing policies.
        except Exception:
            raise CallPolicyError() from None

    @staticmethod
    def _parts(data):
        # Lazy imports avoid the reply -> Store -> policy import cycle.
        from .astrbot_reply import ReplyModel, ReplyPolicy
        from .astrbot_audio import AudioRequestPolicy

        native = data["version"] == "bot-call-policy-v2"
        bound = CallBinding(**data["binding"])
        if data["version"] == "bot-call-policy-v3":
            from .astrbot_agent import SourceAgentPolicy

            agent = SourceAgentPolicy(bound, **data["agent"])
            sound = AudioRequestPolicy(bound, **data["audio"])
            manifest = ReviewManifest(**data["review"])
            if (
                manifest.binding != bound
                or not manifest.is_native
                or sound.selection != NATIVE_CONTRACT
            ):
                raise ValueError
            return bound, agent, sound, manifest
        reply, audio, review = data["reply"], data["audio"], data["review"]
        if (
            type(reply) is not dict
            or set(reply) != {"main", "compression"}
            or type(audio) is not dict
            or set(audio)
            != {
                "provider_id",
                "model",
                "max_input_bytes",
                "max_wav_bytes",
                *({"selection"} if native else set()),
            }
            or type(review) is not dict
            or set(review) != {"canonical_json", "fingerprint"}
        ):
            raise ValueError

        def model(value):
            if type(value) is not dict or set(value) != {f.name for f in fields(ReplyModel)}:
                raise ValueError
            return ReplyModel(**value)

        response = ReplyPolicy(
            bound,
            model(reply["main"]),
            None if reply["compression"] is None else model(reply["compression"]),
        )
        sound = AudioRequestPolicy(bound, **audio)
        manifest = ReviewManifest(**review)
        if (
            manifest.binding != bound
            or manifest.is_native != native
            or (response.main.cap_parameter == NATIVE_CONTRACT) != native
            or (
                response.compression is not None
                and (response.compression.cap_parameter == NATIVE_CONTRACT) != native
            )
            or (native and sound.selection != NATIVE_CONTRACT)
        ):
            raise ValueError
        return bound, response, sound, manifest

    @classmethod
    def from_policies(cls, reply, audio, review):
        from .astrbot_reply import ReplyPolicy
        from .astrbot_audio import AudioRequestPolicy
        from .astrbot_agent import SourceAgentPolicy

        try:
            if type(reply) is SourceAgentPolicy:
                if (
                    type(audio) is not AudioRequestPolicy
                    or type(review) is not ReviewManifest
                    or reply.binding != audio.binding
                    or reply.binding != review.binding
                ):
                    raise ValueError
                data = {
                    "version": "bot-call-policy-v3",
                    "binding": asdict(reply.binding),
                    "agent": {k: v for k, v in asdict(reply).items() if k != "binding"},
                    "reply_contract": "bot-call-native-agent-v1",
                    "audio_contract": "bot-call-native-stt-v1",
                    "audio": {
                        k: getattr(audio, k)
                        for k in (
                            "provider_id",
                            "model",
                            "max_input_bytes",
                            "max_wav_bytes",
                            "selection",
                        )
                    },
                    "review": asdict(review),
                }
                encoded = _json(data)
                return cls(encoded, _sha(encoded))
            if (
                type(reply) is not ReplyPolicy
                or type(audio) is not AudioRequestPolicy
                or type(review) is not ReviewManifest
                or reply.binding != audio.binding
                or reply.binding != review.binding
            ):
                raise ValueError
            native = reply.main.cap_parameter == NATIVE_CONTRACT
            data = {
                "version": "bot-call-policy-v2" if native else "bot-call-policy-v1",
                "binding": asdict(reply.binding),
                "reply_contract": "bot-call-native-reply-v1"
                if native
                else "bot-call-reply-request-v1",
                "audio_contract": "bot-call-native-stt-v1" if native else "bot-call-stt-request-v1",
                "reply": {
                    "main": asdict(reply.main),
                    "compression": asdict(reply.compression)
                    if reply.compression is not None
                    else None,
                },
                "audio": {
                    key: getattr(audio, key)
                    for key in ("provider_id", "model", "max_input_bytes", "max_wav_bytes")
                },
                "review": asdict(review),
            }
            if native:
                data["decision_contract"] = DECISION_CONTRACT
                data["audio"]["selection"] = audio.selection
            encoded = _json(data)
            return cls(encoded, _sha(encoded))
        except Exception:
            raise CallPolicyError() from None

    @property
    def is_native(self):
        return json.loads(self.canonical_json)["version"] in {
            "bot-call-policy-v2",
            "bot-call-policy-v3",
        }

    @property
    def is_agent(self):
        return json.loads(self.canonical_json)["version"] == "bot-call-policy-v3"

    @property
    def binding(self):
        return CallBinding(**json.loads(self.canonical_json)["binding"])

    @property
    def reply(self):
        return self._parts(json.loads(self.canonical_json))[1]

    @property
    def audio(self):
        return self._parts(json.loads(self.canonical_json))[2]

    @property
    def review_manifest(self):
        return ReviewManifest(**json.loads(self.canonical_json)["review"])
