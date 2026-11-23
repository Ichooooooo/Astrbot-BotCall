"""Scoped filtering for known native/library loggers; no SDK/client inspection.

Unknown Provider logging channels are outside this filter. No global level or
shared client is changed. The lease lasts through real owned operation exit.
"""

from contextlib import contextmanager
from contextvars import ContextVar
import logging
import threading

_NAMES = (
    "sqlalchemy.engine.Engine",
    "aiosqlite",
    "astrbot",
    "openai._base_client",
    "openai._response",
    "openai._legacy_response",
    "openai.audio.transcriptions",
    "httpx",
    "httpcore.connection",
    "httpcore.http11",
    "httpcore.http2",
    "httpcore.proxy",
    "httpcore.socks",
)
_SCOPE = ContextVar("bot_call_sdk_log_scope", default=None)
_LOCK = threading.RLock()
_LEASES = set()


class SDKPrivacyError(RuntimeError):
    def __init__(self):
        super().__init__("bot_call.sdk_privacy_unavailable")


class _Filter(logging.Filter):
    def filter(self, record):
        # No formatting, exception inspection or mutation, even for malicious args.
        return _SCOPE.get() is None


_FILTER = _Filter()


class ProviderLogPrivacy:
    """Host lease stays installed until real backend/SDK owners have stopped."""

    def __init__(self):
        self._closed, self._scopes = True, 0
        with _LOCK:
            installed = []
            try:
                if _LEASES:
                    self._check_filters()
                else:
                    for name in _NAMES:
                        logger = logging.getLogger(name)
                        logger.filters.insert(0, _FILTER)
                        installed.append(logger)
                _LEASES.add(self)
                self._closed = False
            except BaseException:
                for logger in installed:
                    logger.filters[:] = [f for f in logger.filters if f is not _FILTER]
                raise SDKPrivacyError() from None

    @staticmethod
    def _check_filters():
        for name in _NAMES:
            filters = logging.getLogger(name).filters
            if not filters or filters[0] is not _FILTER:
                raise SDKPrivacyError()

    def check(self):
        with _LOCK:
            if self._closed or self not in _LEASES:
                raise SDKPrivacyError()
            self._check_filters()

    @contextmanager
    def scope(self):
        self.check()
        with _LOCK:
            self.check()  # Atomic reservation against a concurrent close.
            self._scopes += 1
        token = _SCOPE.set(self)
        try:
            yield
        finally:
            _SCOPE.reset(token)
            with _LOCK:
                self._scopes -= 1

    def close(self):
        with _LOCK:
            if self._closed:
                return
            if self._scopes:
                raise SDKPrivacyError()  # Retain protection and ownership.
            _LEASES.remove(self)
            self._closed = True
            if not _LEASES:
                for name in _NAMES:
                    logger = logging.getLogger(name)
                    logger.filters[:] = [f for f in logger.filters if f is not _FILTER]


# Compatibility import name only; no SDK transport/version contract remains.
SDKLogPrivacy = ProviderLogPrivacy
