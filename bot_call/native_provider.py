"""Provider-neutral public AstrBot calls; no SDK imports or client inspection.

The output allowances are planning reserves. Provider configuration owns actual
parameters, internal recovery and SDK retries. A public call is not an SDK count.
"""

from collections.abc import Mapping
from copy import deepcopy
import json
from .astrbot_compat import BASELINE_VERSION, using_provider

NATIVE_CONTRACT = "native-public-v1"
DECISION_CONTRACT = "native-llmresponse-single-tool-sha256-id-v1"
COUNTER_RULE = f"astrbot{BASELINE_VERSION}.native-public-call-estimate.v1"


class NativeProviderUnavailable(RuntimeError):
    def __init__(self):
        super().__init__("provider.public_unavailable")


def provider_info(provider, method):
    """Read only public metadata and the configured planning window."""
    try:
        if provider is None or not callable(getattr(provider, method, None)):
            raise ValueError
        meta = provider.meta()
        identifier, model = meta.id, provider.get_model()
        if any(type(v) is not str or not v.strip() for v in (identifier, model)):
            raise ValueError
        identifier.encode("utf-8")
        model.encode("utf-8")
        config = provider.provider_config
        if not isinstance(config, Mapping) or config.get("enable", True) is not True:
            raise ValueError
        window = config.get("max_context_tokens", 0)
        if type(window) is not int:
            raise ValueError
        return identifier, model, window
    except Exception:
        raise NativeProviderUnavailable() from None


async def selected_media(context, umo, kind):
    if kind == "stt":
        provider = await using_provider(context, umo, "stt")
        method = "get_text"
    elif kind == "tts":
        provider = await using_provider(context, umo, "tts")
        method = "get_audio"
    else:
        raise NativeProviderUnavailable()
    provider_info(provider, method)
    return provider


def call_payload(provider_id, contexts):
    # This is the public call envelope, not the final vendor/SDK serialization.
    return {
        "chat_provider_id": provider_id,
        "contexts": deepcopy(contexts),
        "request_max_retries": 1,
    }


def public_call_json(provider_id, contexts):
    return json.dumps(
        call_payload(provider_id, contexts),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _field(value, name, default=None):
    return value.get(name, default) if type(value) is dict else getattr(value, name, default)


def observed_finish(response):
    """Read explicitly exposed finish metadata; do not parse vendor responses.

    Missing/unfamiliar raw values rely on the native LLMResponse abstraction.
    Attribute errors are unconfirmed output, never a reason to retry.
    """
    raw = getattr(response, "raw_completion", None)
    values = [getattr(response, "finish_reason", None)]
    if raw is not None:
        values.append(_field(raw, "finish_reason"))
        choices = _field(raw, "choices")
        if type(choices) in (list, tuple) and choices:
            values.append(_field(choices[0], "finish_reason"))
        status = _field(raw, "status")
        if type(status) is str and status in {
            "incomplete",
            "failed",
            "cancelled",
            "queued",
            "in_progress",
        }:
            return "incomplete"
    for value in values:
        if type(value) is str and value in {"length", "content_filter", "incomplete"}:
            return value
    return next(
        (v for v in values if type(v) is str and v in {"stop", "tool_calls", "function_call"}),
        "unknown",
    )


def response_text(response):
    if (
        getattr(response, "is_chunk", None) is not False
        or getattr(response, "role", None) != "assistant"
        or observed_finish(response)
        in {"length", "content_filter", "incomplete", "tool_calls", "function_call"}
    ):
        raise ValueError("provider.unconfirmed_output")
    for name in ("tools_call_args", "tools_call_name", "tools_call_ids"):
        value = getattr(response, name, None)
        if type(value) is not list or value:
            raise ValueError("provider.unexpected_tools")
    body = response.completion_text
    if type(body) is not str or not body.strip():
        raise ValueError("provider.empty_output")
    body.encode("utf-8")
    return body
