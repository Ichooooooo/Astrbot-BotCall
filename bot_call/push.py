"""Coalesced state notifications; bounded first audio on authenticated sockets."""

import asyncio
from contextlib import asynccontextmanager


class PushRuntime:
    @asynccontextmanager
    async def watch(self, token, reference, *, pcm=False):
        self._check_loop()
        call_id = await self._request("web_watch", token, reference)
        queue = asyncio.Queue(maxsize=1)
        queue.audio = asyncio.Queue(maxsize=128)
        queue.pcm = pcm
        queue.pcm_owner = None
        queue.pcm_last = None
        queue.pcm_closed = False
        watchers = self._watchers.setdefault(call_id, set())
        if len(watchers) >= 4:
            from .access import AccessError

            raise AccessError("busy")
        watchers.add(queue)
        queue.put_nowait({"state": "sync"})
        try:
            yield queue
        finally:
            if queue.pcm_owner is not None:
                queue.pcm_owner.disconnect()
            watchers.discard(queue)
            if not watchers:
                self._watchers.pop(call_id, None)

    async def prepare_pcm(self, binding, gid):
        self._check_loop()
        queues = [q for q in self._watchers.get(binding.call_id, ()) if q.pcm]
        from .observability import CURRENT_TIMING

        mode = 1 if not queues else 2 if len(queues) != 1 else 3 if queues[0].pcm_owner else 4
        timing = CURRENT_TIMING.get()
        if timing:
            timing.add("tts_pcm_selection", 0, end=mode, sequence=1)
        if mode != 4:
            return None
        return PCMDelivery(self, queues[0], binding, gid)

    def _deliver_push(self, call_id, event):
        for queue in tuple(self._watchers.get(call_id, ())):
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(event)

    def _close_push(self):
        for call_id in tuple(self._watchers):
            self._deliver_push(call_id, {"state": "closed"})


async def websocket(app, scope, receive, send):
    tasks = []
    try:
        # Browser APIs cannot set the reference header. Bind the existing
        # reference as a subprotocol, with no credentials in URLs or messages.
        headers = app._headers(dict(scope, scheme="https"), False)
        if headers.get("origin") != app._origin and getattr(app, "rejections", None):
            app.rejections.record(scope, "origin_mismatch", app._origin)
        if (
            scope.get("scheme") != "wss"
            or headers.get("origin") != app._origin
            or scope.get("path") != "/bot-call/ws"
        ):
            raise ValueError
        protocols = scope.get("subprotocols", [])
        if (
            len(protocols) not in (1, 2)
            or not protocols[0].startswith("bot-call.")
            or (len(protocols) == 2 and protocols[1] != "bot-call.pcm-v1")
        ):
            raise ValueError
        reference = protocols[0][9:]
        import re

        if not re.fullmatch("[0-9a-f]{64}", reference):
            raise ValueError
        headers["x-bot-call-reference"] = reference
        token, reference = app._session(headers)
        if (await receive()).get("type") != "websocket.connect":
            raise ValueError
        async with app._runtime.watch(token, reference, pcm=len(protocols) == 2) as queue:
            await send({"type": "websocket.accept", "subprotocol": protocols[0]})
            incoming = asyncio.create_task(receive())
            tasks.append(incoming)
            # Keep one consumer per queue across receipt/authentication awaits.
            # A completed get owns the next event; never append it to the tail.
            changed = asyncio.create_task(queue.get())
            audio = asyncio.create_task(queue.audio.get())
            tasks.extend((changed, audio))
            while True:
                if queue.pcm_closed:
                    return
                done, _ = await asyncio.wait(
                    {incoming, changed, audio}, timeout=10, return_when=asyncio.FIRST_COMPLETED
                )
                if incoming in done:
                    message = incoming.result()
                    if message.get("type") != "websocket.receive":
                        return
                    import json

                    # Capability updates and positive ownership receipts share
                    # this authenticated socket; neither can invoke synthesis.
                    raw = message.get("text", "")
                    if len(raw) > 256:
                        return
                    value = json.loads(raw)
                    if not isinstance(value, dict):
                        return
                    await app._runtime.web_state(token, reference)
                    if value.get("type") == "pcm-capability":
                        if set(value) != {"type", "enabled"} or type(value["enabled"]) is not bool:
                            return
                        queue.pcm = value["enabled"]
                    else:
                        if (
                            set(value) != {"type", "generation_id"}
                            or value.get("type") not in {"pcm-accepted", "pcm-unavailable"}
                            or not isinstance(value["generation_id"], str)
                        ):
                            return
                        delivery = queue.pcm_owner or queue.pcm_last
                        if delivery is not None and delivery.gid == value["generation_id"]:
                            if value["type"] == "pcm-accepted":
                                await delivery.accept()
                            else:
                                await delivery.reject()
                    tasks.remove(incoming)
                    incoming = asyncio.create_task(receive())
                    tasks.append(incoming)
                    continue
                if audio in done:
                    event = audio.result()
                    tasks.remove(audio)
                    audio = asyncio.create_task(queue.audio.get())
                    tasks.append(audio)
                elif changed in done:
                    event = changed.result()
                    # While audio/receipts were handled, a newer state may
                    # have replaced this snapshot. Preserve latest-state wins.
                    if not queue.empty():
                        event = queue.get_nowait()
                    tasks.remove(changed)
                    changed = asyncio.create_task(queue.get())
                    tasks.append(changed)
                    if event["state"] == "closed":
                        return
                else:
                    event = {"state": "alive"}
                # Recheck cookie expiry/ownership even when no state changes.
                current = await app._runtime.web_state(token, reference)
                event = dict(event)
                frame = event.get("pcm")
                if frame:
                    delivery = queue.pcm_last
                    if (
                        delivery is not None
                        and delivery.gid == frame["generation_id"]
                        and delivery.rejected
                    ):
                        continue
                    rows = [
                        r for r in current.segments if r["generation_id"] == frame["generation_id"]
                    ]
                    if (
                        current.close_requested
                        or not any(
                            r["seq"] == 0 and r["state"] in {"open", "complete"} for r in rows
                        )
                        or not any(
                            r["seq"] == 1
                            and r["state"] in {"running", "succeeded"}
                            and r["playback_state"] in {"unknown", "started"}
                            for r in rows
                        )
                    ):
                        continue
                gid = event.pop("inline_first", None)
                if gid and not event.get("closed"):
                    # A coalesced/queued notification may predate stop or close.
                    # Recheck current playback and use the SAME authenticated
                    # segment reader as HTTP. Never carry bytes in watch queues.
                    rows = [r for r in current.segments if r["generation_id"] == gid]
                    if any(
                        r["seq"] == 0 and r["state"] in {"open", "complete"} for r in rows
                    ) and any(
                        r["seq"] == 1
                        and r["state"] == "succeeded"
                        and r["playback_state"] == "unknown"
                        and r["revision"] == 0
                        for r in rows
                    ):
                        try:
                            from .speech import MAX_PUSH_AUDIO_BYTES, segment_audio_mime
                            import base64

                            data = await app._runtime.web_segment(token, reference, gid, 1)
                            if len(data) <= MAX_PUSH_AUDIO_BYTES:
                                event["first_audio"] = dict(
                                    generation_id=gid,
                                    seq=1,
                                    byte_count=len(data),
                                    mime=segment_audio_mime(data).decode("ascii"),
                                    data=base64.b64encode(data).decode("ascii"),
                                )
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            pass  # Optional payload; HTTP fallback stays available.

                import json

                await asyncio.wait_for(
                    send(
                        {"type": "websocket.send", "text": json.dumps(event, separators=(",", ":"))}
                    ),
                    5,
                )
    except asyncio.CancelledError:
        raise
    except Exception:
        pass  # Fixed close code; never publish exception text or auth material.
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await send({"type": "websocket.close", "code": 1008})


class PCMDelivery:
    """One socket owns a first sentence; bytes never use coalesced state queues."""

    def __init__(self, runtime, queue, binding, gid):
        self.runtime, self.queue, self.binding, self.gid = runtime, queue, binding, gid
        self.task = asyncio.current_task()
        self.delivered = False
        self.index = 0
        self.closed = False
        self.rejected = False
        queue.pcm_owner = queue.pcm_last = self

    async def __call__(self, data):
        import base64

        if self.rejected:
            return  # Finish this same upstream request into the MP3 archive.
        if self.closed:
            raise asyncio.CancelledError()
        if self.queue not in self.runtime._watchers.get(self.binding.call_id, ()):
            if self.delivered:
                raise asyncio.CancelledError()
            return  # No first block delivered: archive the ongoing request only.
        if not await self.runtime._request(
            "segment_open",
            self.binding,
            self.gid,
            1,
        ):
            raise asyncio.CancelledError()
        if self.rejected:
            return
        packet = dict(
            generation_id=self.gid,
            seq=1,
            index=self.index,
            rate=24000,
            format="s16le",
            end=data is None,
        )
        if data is not None:
            packet["data"] = base64.b64encode(data).decode("ascii")
        try:
            self.queue.audio.put_nowait(dict(state="changed", pcm=packet))
        except asyncio.QueueFull:
            self.queue.pcm_closed = True
            if not self.delivered:
                self.rejected = True
                while not self.queue.audio.empty():
                    self.queue.audio.get_nowait()
                if self.queue.empty():
                    self.queue.put_nowait(dict(state="closed"))
                return  # Close optional transport, finish the same archive.
            self.queue.pcm = False
            self.queue.put_nowait(dict(state="closed")) if self.queue.empty() else None
            raise RuntimeError("tts.slow_consumer") from None
        self.index += 1

    async def accept(self):
        if not self.rejected and not self.delivered and self.index:
            self.delivered = await self.runtime._request("pcm_claim", self.binding, self.gid, 1)

    async def reject(self):
        if not self.delivered:
            self.rejected = True

    def disconnect(self):
        if self.delivered and not self.closed and not self.rejected:
            self.task.cancel()

    def close(self):
        self.closed = True
        if self.queue.pcm_owner is self:
            self.queue.pcm_owner = None
