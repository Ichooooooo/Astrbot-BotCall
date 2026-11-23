"""Storage-loop sentence queue; synthesis stays on the business loop."""

import asyncio
from types import SimpleNamespace
from .speech import SpeechUnavailable


class SegmentRuntime:
    async def web_segment(self, token, reference, gid, seq, receipt=None):
        return await self._request("web_segment", token, reference, gid, seq, receipt)

    async def _segment_guard(self, binding, gid, seq):
        return await self._request("segment_open", binding, gid, seq)

    def _segment_action(self, action, call_id, values):
        gid = values[0]
        if action == "segment":
            if not callable(self._callbacks.synthesize):
                return None
            seq = self._store.add_segment(call_id, gid, values[1])
            if seq is None and self._store.speech_superseded(call_id, gid):
                return -1  # Retain public text; the sealed group has no audio rights.
            key = (call_id, gid)
            if seq and (key not in self._segment_tasks or self._segment_tasks[key].done()):
                self._segment_tasks[key] = self._spawn(
                    self._segment_synthesize(call_id, gid), "speech"
                )
            self._notify_state(call_id)
            return seq
        self._store.stream_done(call_id, gid)
        self._notify_state(call_id)

    async def _segment_synthesize(self, call_id, gid):
        try:
            while not self._worker_stopping:
                row = self._store.next_pending_segment(call_id, gid)
                if row is None or self._store.is_call_closing(call_id):
                    return
                seq = row["seq"]
                self._store._db.execute(
                    "UPDATE speech_segments SET state='running' WHERE call_id=? AND generation_id=? AND seq=? AND state='pending'",
                    (call_id, gid, seq),
                )
                turn = SimpleNamespace(
                    call_id=call_id,
                    role="assistant",
                    material_id=gid,
                    body=row["body"],
                    segment_seq=seq,
                )

                async def guard(binding, ignored):
                    return await self._segment_guard(binding, gid, seq)

                try:
                    data = await self._bridge(
                        "synthesize", self._store.get_binding(call_id), turn, guard
                    )
                    self._store.finish_segment(call_id, gid, seq, "succeeded", data)
                    if seq == 1:
                        started = self._segment_started.get(
                            (call_id, gid), asyncio.get_running_loop().time()
                        )
                        self._store.record_timings(
                            call_id,
                            gid,
                            [
                                dict(
                                    stage="first_audio",
                                    sequence=0,
                                    start_ms=0,
                                    elapsed_ms=(asyncio.get_running_loop().time() - started) * 1000,
                                )
                            ],
                        )
                except asyncio.CancelledError:
                    self._store.finish_segment(call_id, gid, seq, "uncertain")
                    # A socket-owned first-player cancellation is reported by
                    # the bridge without cancelling this storage worker. Keep
                    # the remaining sentences; real worker/stop cancellation
                    # still propagates.
                    if asyncio.current_task().cancelling() or not self._continue_after_first(
                        call_id, gid, seq
                    ):
                        raise
                except SpeechUnavailable:
                    self._store.finish_segment(call_id, gid, seq, "failed")
                    return
                except Exception:
                    self._store.finish_segment(call_id, gid, seq, "uncertain")
                    if not self._continue_after_first(call_id, gid, seq):
                        return
                finally:
                    self._notify_state(call_id, speech=True)
        finally:
            self._segment_tasks.pop((call_id, gid), None)
            rows = self._store.segments(call_id, gid)
            if not rows or rows[0]["state"] != "open":
                self._segment_started.pop((call_id, gid), None)

    def _continue_after_first(self, call_id, gid, seq):
        if seq != 1 or self._worker_stopping or self._store.is_call_closing(call_id):
            return False
        rows = self._store.segments(call_id, gid)
        return any(r["seq"] == 0 and r["state"] in {"open", "complete"} for r in rows) and any(
            r["seq"] == 1 and r["revision"] != 0 for r in rows
        )
