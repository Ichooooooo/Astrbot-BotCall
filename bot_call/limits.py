"""Finite call deadlines; pure data, no clock/DB/task startup effects."""

from dataclasses import dataclass, field, asdict
import math


DEFAULT_IDLE_SECONDS = 600


class LimitsError(RuntimeError):
    def __init__(self):
        super().__init__("call_limits.unconfirmed")


def checked_time(value):
    try:
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError
        return value
    except Exception:
        raise LimitsError() from None


@dataclass(frozen=True, slots=True)
class RuntimeLimits:
    max_seconds: int = 3600
    warning_seconds: int = 300
    disconnect_seconds: int = 120
    idle_seconds: int | None = None

    def __post_init__(self):
        if (
            any(
                type(v) is not int or not 0 < v <= 2147483647
                for v in (self.max_seconds, self.warning_seconds, self.disconnect_seconds)
            )
            or self.warning_seconds >= self.max_seconds
            or (
                self.idle_seconds is not None
                and (
                    type(self.idle_seconds) is not int
                    or not 0 < self.idle_seconds <= self.max_seconds
                )
            )
        ):
            raise LimitsError() from None


@dataclass(frozen=True, slots=True)
class LimitsStatus:
    entered_at: float
    deadline_at: float
    server_now: float
    remaining_seconds: float
    reconnect_remaining_seconds: float
    warning: bool
    close_reason: str | None
    closed_at: float | None
    enforced: bool
    error: str | None
    close_detail: str | None = None

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CallLimits:
    call_id: str = field(repr=False)
    entered_at: float
    max_seconds: int
    warning_seconds: int
    disconnect_seconds: int
    last_activity_at: float
    close_reason: str | None
    closed_at: float | None

    def remaining(self, now):
        checked_time(now)
        checked_time(self.entered_at)
        checked_time(self.last_activity_at)
        RuntimeLimits(self.max_seconds, self.warning_seconds, self.disconnect_seconds)
        if now < self.last_activity_at or self.last_activity_at < self.entered_at:
            raise LimitsError() from None
        return (
            self.entered_at + self.max_seconds - now,
            self.last_activity_at + self.disconnect_seconds - now,
        )

    def due_reason(self, now):
        duration, disconnect = self.remaining(now)
        if min(duration, disconnect) > 0 or self.close_reason is not None:
            return None
        return "duration" if duration <= disconnect else "disconnected"

    def public(self, now, *, enforced=True, error=None):
        duration, disconnect = self.remaining(now)
        return LimitsStatus(
            self.entered_at,
            self.entered_at + self.max_seconds,
            now,
            max(0, duration),
            max(0, disconnect),
            self.close_reason is None and duration <= self.warning_seconds,
            self.close_reason,
            self.closed_at,
            enforced,
            error,
        )
