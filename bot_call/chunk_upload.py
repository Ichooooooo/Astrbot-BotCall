"""Worker-owned, bounded volatile uploads. No acknowledgement of durable receipt."""

import hashlib
import re
from .audio import AUDIO_MIMES, AudioError


class ChunkUploads:
    TTL = 120

    def __init__(self, limit, loop, on_expire=None):
        self.limit, self.loop, self.pending = limit, loop, {}
        self.on_expire = on_expire
        self.replaced = {}  # Short-lived tombstones, never retained audio.

    def discard(self, call_id, utterance_id=None):
        entry = self.pending.get(call_id)
        if entry is not None and (utterance_id is None or entry["id"] == utterance_id):
            self.pending.pop(call_id)
            entry["timer"].cancel()
        if utterance_id is None:
            for key in tuple(self.replaced):
                if key[0] == call_id:
                    self.replaced.pop(key).cancel()

    def _expire(self, call_id):
        self.discard(call_id)
        if self.on_expire is not None:
            self.on_expire(call_id)

    def close(self):
        for call_id in set(self.pending) | {key[0] for key in self.replaced}:
            self.discard(call_id)

    @staticmethod
    def identity(utterance_id, mime):
        if (
            type(utterance_id) is not str
            or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", utterance_id) is None
            or type(mime) is not str
            or mime not in AUDIO_MIMES
        ):
            raise AudioError("invalid")

    def put(self, call_id, utterance_id, seq, offset, data, mime):
        self.identity(utterance_id, mime)
        if (
            type(seq) is not int
            or not 0 <= seq < 4096
            or type(offset) is not int
            or offset < 0
            or type(data) is not bytes
            or not data
        ):
            raise AudioError("invalid")
        if offset + len(data) > self.limit:
            raise AudioError("too_large")
        if (call_id, utterance_id) in self.replaced:
            raise AudioError("chunk_missing")
        entry = self.pending.get(call_id)
        if entry is not None and entry["id"] != utterance_id:
            if seq != 0 or offset != 0:
                raise AudioError("chunk_missing")
            if len(self.replaced) >= 65536:
                raise AudioError("conflict")  # Bound metadata as well as audio buffers.
            old_key = (call_id, entry["id"])
            self.replaced[old_key] = self.loop.call_later(
                self.TTL, self.replaced.pop, old_key, None
            )
            self.discard(call_id, entry["id"])
            entry = None
        if entry is None:
            if seq != 0 or offset != 0:
                raise AudioError("chunk_missing")
            if len(self.pending) >= 16:
                raise AudioError("conflict")
            entry = dict(id=utterance_id, mime=mime, data=bytearray(), parts=[], timer=None)
        if entry["id"] != utterance_id or entry["mime"] != mime:
            raise AudioError("conflict")
        parts, body = entry["parts"], entry["data"]
        if seq < len(parts):
            start, length = parts[seq]
            if start != offset or length != len(data) or body[start : start + length] != data:
                raise AudioError("conflict")
        elif seq != len(parts) or offset != len(body):
            raise AudioError("conflict")
        else:
            parts.append((offset, len(data)))
            body.extend(data)
        if entry["timer"] is not None:
            entry["timer"].cancel()
        entry["timer"] = self.loop.call_later(self.TTL, self._expire, call_id)
        self.pending[call_id] = entry
        return dict(
            chunk_received=True,
            utterance_id=utterance_id,
            next_seq=len(parts),
            next_offset=len(body),
        )

    def data(self, call_id, utterance_id, total, sha256, mime, offset=None, tail=None):
        self.identity(utterance_id, mime)
        if (
            type(total) is not int
            or total < 1
            or type(sha256) is not str
            or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
        ):
            raise AudioError("invalid")
        if total > self.limit:
            raise AudioError("too_large")
        entry = self.pending.get(call_id)
        if offset is None:
            if entry is None or entry["id"] != utterance_id:
                raise AudioError("chunk_missing")
            data = bytes(entry["data"])
        else:
            self.validate_tail(total, offset, tail)
            if (call_id, utterance_id) in self.replaced or (
                entry is not None and entry["id"] != utterance_id
            ):
                raise AudioError("chunk_missing")
            if entry is None:
                if offset != 0:
                    raise AudioError("chunk_missing")
                data = tail
            else:
                previous = entry["data"]
                if offset > len(previous) or len(previous) > total:
                    raise AudioError("conflict")
                overlap = len(previous) - offset
                if previous[offset:] != tail[:overlap]:
                    raise AudioError("conflict")
                data = bytes(previous[:offset]) + tail
        if (
            (entry is not None and entry["mime"] != mime)
            or len(data) != total
            or hashlib.sha256(data).hexdigest() != sha256
        ):
            raise AudioError("conflict")
        return data  # Validation never mutates staged/acknowledged bytes.

    @staticmethod
    def validate_tail(total, offset, tail):
        if type(offset) is not int or offset < 0 or type(tail) is not bytes:
            raise AudioError("invalid")
        if offset + len(tail) != total:
            raise AudioError("conflict")
