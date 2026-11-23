"""Bounded preparation diagnostics; no free exception text or foreign notes."""

import json
import logging
from pathlib import Path
import sys

PREFIX = "bot_call.prepare "
STAGES = frozenset(
    (
        "event",
        "begin",
        "wait",
        "send",
        "bind",
        "refresh",
        "select",
        "policy",
        "reply_bind",
        "audio_bind",
        "review_bind",
        "router",
        "create_call",
        "grant_recheck",
        "issue_invitation",
    )
)
_FILES = {
    str(Path(__file__).parent / (n + ".py")): "bot_call/" + n + ".py"
    for n in (
        "plugin",
        "backend",
        "binding",
        "astrbot_adapter",
        "native_host",
        "astrbot_audio",
        "astrbot_reply",
        "astrbot_review",
        "review_policy",
        "review_router",
        "call_policy",
        "runtime",
        "store",
        "sdk_privacy",
        "prepare_diagnostics",
    )
}
_FILES[str(Path(__file__).parent.parent / "main.py")] = "main.py"
_BASE = {
    t: t.__name__
    for t in (
        ValueError,
        TypeError,
        RuntimeError,
        OSError,
        TimeoutError,
        PermissionError,
        FileNotFoundError,
        ConnectionError,
        ConnectionRefusedError,
        AttributeError,
        ImportError,
        ModuleNotFoundError,
    )
}
_KINDS = (
    ("plugin", "PluginLifecycleError"),
    ("backend", "BackendError"),
    ("binding", "BindingError"),
    ("astrbot_adapter", "LoopMismatch"),
    ("astrbot_adapter", "SnapshotError"),
    ("astrbot_adapter", "ProviderError"),
    ("native_host", "NativeHostError"),
    ("sdk_privacy", "SDKPrivacyError"),
    ("astrbot_reply", "ReplyNotApplied"),
    ("astrbot_reply", "ReplyResponseUncertain"),
    ("astrbot_audio", "AudioResponseUncertain"),
    ("audio", "AudioNotApplied"),
    ("astrbot_review", "ReviewResponseUncertain"),
    ("review_policy", "ReviewPolicyUnavailable"),
)
_CLASSES = frozenset((*_BASE.values(), *(n for _, n in _KINDS), "unknown"))
_CODES = {
    "PluginLifecycleError": "bot_call.host_unavailable",
    "NativeHostError": "bot_call.host_unavailable",
    "SDKPrivacyError": "bot_call.sdk_privacy_unavailable",
}


def _validated(note):
    if type(note) is not str or len(note) > 16384 or not note.startswith(PREFIX):
        return None
    if note == PREFIX + "diagnostic=unknown":
        return note
    obj = json.loads(note[len(PREFIX) :])
    if type(obj) is not dict or set(obj) != {"stage", "errors", "openai_debug_enabled"}:
        return None
    if (
        type(obj["stage"]) is not str
        or obj["stage"] not in STAGES
        or type(obj["openai_debug_enabled"]) is not bool
    ):
        return None
    if type(obj["errors"]) is not list or len(obj["errors"]) > 8:
        return None
    for row in obj["errors"]:
        if type(row) is not dict or set(row) not in (
            {"class", "frames"},
            {"class", "frames", "code"},
        ):
            return None
        if type(row["class"]) is not str or row["class"] not in _CLASSES:
            return None
        if row["class"] in _CODES:
            if type(row.get("code")) is not str or row["code"] != _CODES[row["class"]]:
                return None
        elif "code" in row:
            return None
        if type(row["frames"]) is not list or len(row["frames"]) > 2:
            return None
        for frame in row["frames"]:
            if (
                type(frame) is not dict
                or set(frame) != {"file", "line"}
                or type(frame["file"]) is not str
                or frame["file"] not in _FILES.values()
                or type(frame["line"]) is not int
                or not 0 < frame["line"] <= 1000000
            ):
                return None
    # Canonicalize; never export the original string, including ignored duplicates.
    return PREFIX + json.dumps(obj, separators=(",", ":"))


def prepare_note(stage, error):
    try:
        if type(stage) is not str or stage not in STAGES:
            raise ValueError
        types = dict(_BASE)
        for module, name in _KINDS:
            kind = getattr(sys.modules.get(__package__ + "." + module), name, None)
            if isinstance(kind, type):
                types[kind] = name
        pending = [(error, 0)]
        seen = set()
        rows = []
        while pending and len(rows) < 8:
            exc, depth = pending.pop(0)
            if id(exc) in seen or depth > 3:
                continue
            seen.add(id(exc))
            name = types.get(type(exc), "unknown")
            row = {"class": name, "frames": []}
            if name in _CODES:
                row["code"] = _CODES[name]
            trace = exc.__traceback__
            count = 0
            while trace is not None and count < 40:
                file = _FILES.get(trace.tb_frame.f_code.co_filename)
                if file is not None:
                    row["frames"].append({"file": file, "line": trace.tb_lineno})
                trace = trace.tb_next
                count += 1
            row["frames"] = row["frames"][-2:]
            rows.append(row)
            for nested in (exc.__cause__, exc.__context__):
                if isinstance(nested, BaseException):
                    pending.append((nested, depth + 1))
        value = PREFIX + json.dumps(
            {
                "stage": stage,
                "errors": rows,
                "openai_debug_enabled": logging.getLogger("openai._base_client").isEnabledFor(
                    logging.DEBUG
                ),
            },
            separators=(",", ":"),
        )
        return _validated(value) or PREFIX + "diagnostic=unknown"
    except BaseException:
        return PREFIX + "diagnostic=unknown"


def attach_prepare(error, stage, source, *, propagate=False):
    """Only attach to a new fixed public exception; failures leave it untouched."""
    try:
        values = []
        # Only our own backend wrapper may carry a prior trusted diagnostic.
        # An external exception cannot occupy the bounded slots with forged notes.
        backend = getattr(sys.modules.get(__package__ + ".backend"), "BackendError", None)
        notes = (
            getattr(source, "__notes__", ())
            if propagate is True and type(source) is backend
            else ()
        )
        if type(notes) in (list, tuple):
            for note in notes[:2]:
                try:
                    safe = _validated(note)
                except BaseException:
                    continue
                if safe is not None and safe not in values:
                    values.append(safe)
        note = prepare_note(stage, source)
        if note not in values:
            values.append(note)
        for value in values[:2]:
            BaseException.add_note(error, value)
    except BaseException:
        pass
