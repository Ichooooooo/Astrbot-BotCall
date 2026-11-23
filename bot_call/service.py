"""A thin synthetic reply flow, without network or AstrBot imports."""

import asyncio
from collections.abc import Awaitable, Callable

from .binding import CallBinding
from .store import Generation, StateConflict, Store, Turn


async def process_utterance(
    store: Store,
    call_id: str,
    utterance_id: str,
    text: str,
    reply: Callable[[CallBinding, Turn], Awaitable[str]],
    *,
    source: str = "simulated_stt",
    series: bool = False,
) -> Generation:
    """Commit user text, claim the stable reply step, then invoke an injected fake.

    Replays never invoke the callback again, including running/failed/empty
    steps. A process death can leave running forever: no automatic recovery or
    claim takeover is implemented. This is not a production model client.
    """
    binding = store.get_binding(call_id)
    if series:
        root = store.get_audio_job(call_id, utterance_id)
        if not store.series_ready(root):
            raise StateConflict("Series is incomplete")
        user = next(
            t
            for t in store.effective_transcript(call_id)
            if t.material_id == utterance_id and t.role == "user"
        )
    else:
        user = store.append_user(call_id, utterance_id, text, source=source)
    generation_id = f"reply:{utterance_id}"
    generation, claimed = store.claim_generation(
        call_id, generation_id, utterance_id=utterance_id, source="fake_reply"
    )
    if not claimed:
        return generation
    try:
        body = await reply(binding, user)
        if not isinstance(body, str):
            raise TypeError("Fake reply must return text")
    except asyncio.CancelledError:
        store.finish_without_reply(call_id, generation_id, "cancelled")
        raise
    except Exception:
        outcome = store.finish_without_reply(call_id, generation_id, "failed")
        if outcome.state == "cancelled":
            return outcome
        raise
    if not body or body.isspace():
        return store.finish_without_reply(call_id, generation_id, "empty")
    try:
        store.complete_generation(call_id, generation_id, body)
    except StateConflict:
        outcome = store.get_generation(call_id, generation_id)
        if outcome.state != "cancelled" and not store.speech_superseded(call_id, generation_id):
            raise
        return outcome
    return store.get_generation(call_id, generation_id)
