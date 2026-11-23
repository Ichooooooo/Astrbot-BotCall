"""Persist separate review submission and original-context progress.

SUBMITTED means the standard route returned normally, not recipient delivery.
Legacy CONFIRMED page results remain terminal without resending old material.
"""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from enum import Enum
import sqlite3

from .binding import CallBinding
from .store import Publication, PublicationPage, Store


class Outcome(Enum):
    CONFIRMED = "confirmed"
    SUBMITTED = "submitted"
    NOT_APPLIED = "not_applied"
    UNCERTAIN = "uncertain"


class PublicationPersistenceError(RuntimeError):
    """A local publication outcome could not be confirmed."""


@contextmanager
def _database_boundary():
    try:
        yield
    except sqlite3.Error:
        raise PublicationPersistenceError("Publication persistence is unconfirmed") from None


def _record(record: Callable[[str], object], state: str) -> None:
    try:
        record(state)
    except sqlite3.Error:
        try:
            record("uncertain")
        except sqlite3.Error:
            pass
        raise PublicationPersistenceError("Publication persistence is unconfirmed") from None


async def _apply(
    action: Callable[[], Awaitable[Outcome]],
    record: Callable[[str], object],
    *,
    allow_submission: bool = False,
) -> str:
    try:
        result = await action()
    except asyncio.CancelledError:
        try:
            _record(record, "uncertain")
        except PublicationPersistenceError:
            pass
        # Keep task cancellation, stripping any callback-supplied private message.
        raise asyncio.CancelledError() from None
    except Exception:
        result = Outcome.UNCERTAIN
    if result is Outcome.CONFIRMED or (allow_submission and result is Outcome.SUBMITTED):
        # Existing schema: succeeded means this step finished. For pages that
        # includes submission only; context still requires explicit CONFIRMED.
        state = "succeeded"
    elif result is Outcome.NOT_APPLIED:
        state = "failed"
    else:
        # Unrecognised results (including bool True), timeouts and exceptions
        # cannot prove that an external side effect did not happen.
        state = "uncertain"
    _record(record, state)
    return state


async def advance_publication(
    store: Store,
    call_id: str,
    send_page: Callable[[CallBinding, PublicationPage], Awaitable[Outcome]],
    make_context_readable: Callable[[CallBinding, str, str], Awaitable[Outcome]],
    *,
    send_pages: bool = True,
) -> Publication:
    """Advance one prepared plan once, with separate ordered pages and context.

    Each worker owns its Store. Claims commit before callbacks; no transaction
    spans await. Terminal/running steps never retry. A context CONFIRMED result
    must attest the context adapter's own storage/readback contract, not a
    send submission. Page SUBMITTED/legacy CONFIRMED both finish the send step;
    no stored page status proves user receipt. Callbacks own source-key dedup.
    """
    persistence_failed = False
    with _database_boundary():
        binding = store.get_binding(call_id)
    while send_pages:
        with _database_boundary():
            page, claimed = store.claim_publication_page(call_id)
        if not claimed:
            break
        try:
            state = await _apply(
                lambda: send_page(binding, page),
                lambda state: store.finish_publication_page(call_id, page.number, state),
                allow_submission=True,
            )
        except PublicationPersistenceError:
            persistence_failed = True
            break
        if state != "succeeded":
            break
    with _database_boundary():
        publication, claimed = store.claim_publication_context(call_id)
    if claimed:
        try:
            await _apply(
                lambda: make_context_readable(binding, publication.context_key, publication.body),
                lambda state: store.finish_publication_context(call_id, state),
            )
        except PublicationPersistenceError:
            persistence_failed = True
    if persistence_failed:
        raise PublicationPersistenceError("Publication persistence is unconfirmed") from None
    with _database_boundary():
        return store.get_publication(call_id)
