"""Reduce transient private exceptions to a fixed public category, never text."""

import re
import logging

SERVICES = {"stt", "chat", "tts", "summary"}
CATEGORIES = {"balance", "auth", "timeout", "unavailable", "other", "config", "model", "rejected"}
ERROR_STAGES = {f"error_{s}_{c}" for s in SERVICES for c in CATEGORIES}


class CallConfigurationError(ValueError):
    pass


def log_failure(service, error, *, provider="unknown", attempts=0, elapsed_ms=0):
    if service not in SERVICES:
        return
    safe_provider = (
        provider
        if type(provider) is str
        and re.fullmatch(r"[\w./+:-]{1,160}", provider)
        and "://" not in provider
        and not provider.startswith("sk-")
        else "unknown"
    )
    name = type(error).__name__ if isinstance(error, BaseException) else "native"
    name = name if re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]{0,79}", name) else "native"
    attempts = attempts if type(attempts) is int and 0 <= attempts <= 4096 else 0
    elapsed_ms = (
        round(elapsed_ms) if type(elapsed_ms) in (int, float) and 0 <= elapsed_ms <= 86400000 else 0
    )
    logging.getLogger("bot_call.failures").warning(
        "bot_call.%s stage=failed category=%s error_type=%s provider=%s attempts=%d elapsed_ms=%d",
        service,
        classify(error),
        name,
        safe_provider,
        attempts,
        elapsed_ms,
    )


def classify(error):
    try:
        return _classify(error)
    except Exception:
        return "other"


def _classify(error):
    if isinstance(error, CallConfigurationError):
        return "config"
    values, seen = [], set()
    for _ in range(4):
        if error is None or id(error) in seen:
            break
        seen.add(id(error))
        values.append(error)
        error = getattr(error, "__cause__", None) or getattr(error, "__context__", None)
    codes, texts, names = set(), [], []
    for error in values:
        for obj in (error, getattr(error, "response", None)):
            code = getattr(obj, "status_code", None)
            if type(code) is int:
                codes.add(code)
        try:
            texts.append(str(error)[:16000].casefold())
        except Exception:
            pass
        names.append(type(error).__name__.casefold())
    text = " ".join(texts)
    codes.update(
        int(v)
        for v in re.findall(r"(?:status(?:_code)?|http|error code)\s*[:=/ ]\s*(\d{3})\b", text)
    )
    if 402 in codes or re.search(
        r"insufficient[ _-]+balance|余额不足|insufficient_quota|free quota exhausted|quota[ _-]+exhausted|余额[已耗用]*尽|欠费",
        text,
    ):
        return "balance"
    if codes & {401, 403}:
        return "auth"
    if (
        codes & {408, 504}
        or any(isinstance(v, TimeoutError) for v in values)
        or any("timeout" in n for n in names)
        or re.search(r"timed? out|timeout|超时", text)
    ):
        return "timeout"
    if (
        any(500 <= c <= 599 for c in codes)
        or any(isinstance(v, ConnectionError) for v in values)
        or any("connect" in n for n in names)
        or re.search(
            r"connection (?:refused|reset|failed)|network is unreachable|apiconnectionerror|connecterror|connectionerror",
            text,
        )
    ):
        return "unavailable"
    if 404 in codes or re.search(
        r"model.{0,80}(?:not found|does not exist|not exist)|model_not_found", text
    ):
        return "model"
    if any(400 <= c < 500 and c != 408 for c in codes):
        return "rejected"
    return "other"


def definite_review_failure(error):
    """Only explicit rejection evidence, never timeout/transport/unknown responses."""
    # A mixed chain containing a transport/timeout failure cannot attest that
    # every provider attempt was rejected before producing a result.
    seen = set()
    current = error
    for _ in range(4):
        if current is None or id(current) in seen:
            break
        seen.add(id(current))
        if isinstance(current, (TimeoutError, ConnectionError)) or classify(current) in {
            "timeout",
            "unavailable",
        }:
            return None
        current = getattr(current, "__cause__", None) or getattr(current, "__context__", None)
    category = classify(error)
    return category if category in {"balance", "auth", "model", "rejected", "config"} else None


def record_failure(service, error, *, provider=None, attempts=None, elapsed_ms=None):
    from .observability import CURRENT_TIMING

    timing = CURRENT_TIMING.get()
    if service not in SERVICES:
        return
    if timing is None:
        log_failure(
            service,
            error,
            provider=provider or "unknown",
            attempts=attempts or 0,
            elapsed_ms=elapsed_ms or 0,
        )
        return
    stage = f"error_{service}_{classify(error)}"
    if not any(r["stage"].startswith(f"error_{service}_") for r in timing.rows):
        timing.add(stage, 0, end=0)
        llm = [r for r in timing.rows if r["stage"] == "llm"]
        log_failure(
            service,
            error,
            provider=provider or timing.provider_id,
            attempts=attempts if attempts is not None else len(llm),
            elapsed_ms=elapsed_ms if elapsed_ms is not None else timing.now(),
        )


def failure_summary(rows, *, per_generation=False):
    counts = {}
    for raw in rows:
        row = dict(raw)
        if row.get("stage") not in ERROR_STAGES:
            continue
        _, service, code = row["stage"].split("_")
        key = (service, code, row["generation_id"] if per_generation else None)
        counts[key] = counts.get(key, 0) + 1
    return [
        dict(service=s, code=c, count=n, **({"generation_id": gid} if per_generation else {}))
        for (s, c, gid), n in counts.items()
    ]
