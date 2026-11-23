"""Call-owned sentence material and audio; no framework or network access."""

from contextlib import nullcontext
import json
import re
from time import time
from .phone_style import PhonePlainText

SCHEMA = """CREATE TABLE IF NOT EXISTS speech_segments (
 call_id TEXT NOT NULL, generation_id TEXT NOT NULL, seq INTEGER NOT NULL CHECK(seq>=0),
 body TEXT NOT NULL, state TEXT NOT NULL, data BLOB,
 playback_state TEXT NOT NULL DEFAULT 'unknown', revision INTEGER NOT NULL DEFAULT 0,
 created_at REAL NOT NULL, ready_at REAL,
 PRIMARY KEY(call_id,generation_id,seq),
 FOREIGN KEY(call_id,generation_id) REFERENCES generations(call_id,generation_id))"""


class InternalNotes:
    """Inspect raw clauses before Markdown removes their brackets.

    Commas are existing TTS boundaries. Bracket blocks stay private until their
    closing delimiter, including punctuation inside them. Markdown links remain
    ordinary prose; only bracket blocks with explicit control labels are metadata.
    """

    template = re.compile(
        r"朗读被打断|仅部分播放|播放到约\s*\d+(?:\.\d+)?\s*ms"
        r"|具体已听到哪些词未知|未播出的旧叙述|工具事实另行保留"
        r"|仅表示正在听|不代表同意或工具授权"
    )

    prefixes = (
        "朗读被打断",
        "仅部分播放",
        "播放到约",
        "具体已听到哪些词未知",
        "未播出的旧叙述",
        "工具事实另行保留",
        "仅表示正在听",
        "不代表同意或工具授权",
    )

    def _unfinished_template(self):
        return any(
            self.pending.endswith(phrase[:n])
            for phrase in self.prefixes
            for n in range(1, len(phrase))
        ) or bool(re.search(r"播放到约\s*[\d.]*\s*m?$", self.pending))

    def __init__(self):
        self.pending = ""
        self.bracket = False
        self.closed = False
        self.dropped = 0

    def _flush(self):
        text, self.pending = self.pending, ""
        blocked = bool(self.template.search(text)) or (
            self.closed
            and text.lstrip().startswith(("[", "【"))
            and re.search(
                r"内部(?:备注|说明|控制)|元信息|系统(?:备注|标注)|playback[_ =:]|purpose=backchannel",
                text,
                re.I,
            )
        )
        self.bracket = self.closed = False
        if blocked:
            self.dropped += 1
            return ""
        return text

    def feed(self, char="", *, final=False):
        out = ""
        if self.closed:
            if char == "(" and self.pending.lstrip().startswith("["):
                # Link label, not standalone metadata.
                out, self.pending = self.pending, ""
                self.bracket = self.closed = False
            else:
                out = self._flush()
        self.pending += char
        if char in ("[", "【") and not self.pending[:-1].strip():
            self.bracket = True
        if self.bracket:
            if char in ("]", "】"):
                self.closed = True
        elif char in "，、,；;。！？!?…\n" or (
            len(self.pending) >= 60 and not self._unfinished_template()
        ):
            out += self._flush()
        if final:
            out += self._flush()
        return out


class Sentences:
    """Keep split tags private across deltas; never speak reasoning/protocol."""

    def __init__(self):
        self.pending = ""
        self.text = ""
        self.hidden = False
        self.protocol = False
        self.first = True
        self.plain = PhonePlainText()
        self.notes = InternalNotes()

    def feed(self, delta="", *, final=False):
        self.pending += delta
        out = []
        while self.pending:
            if self.pending[0] == "<":
                end = self.pending.find(">")
                if end < 0:
                    break
                tag, self.pending = self.pending[: end + 1], self.pending[end + 1 :]
                if re.search(r"think|reasoning", tag, re.I):
                    self.hidden = not bool(re.match(r"<\s*/", tag))
                elif re.search(r"DSML|tool_call|function_call", tag, re.I):
                    self.protocol = True
                elif not self.hidden and not self.protocol:
                    self._append(self.plain.feed(self.notes.feed(tag)), out)
                continue
            char, self.pending = self.pending[0], self.pending[1:]
            if self.hidden or self.protocol:
                continue
            self._append(self.plain.feed(self.notes.feed(char)), out)
        if final:
            self._append(self.plain.feed(self.notes.feed(final=True), final=True), out)
        if final and self.text.strip():
            out.append(self.text)
            self.text = ""
            self.first = False
        return out

    def _append(self, text, out):
        for char in text:
            self.text += char
            pause = self.first and char in "，、,；;"
            first_clause = pause and sum(c.isalnum() for c in self.text[:-1]) >= 6
            full_stop = char in "。！？!?；…\n" and not pause
            if (first_clause or full_stop or len(self.text) >= 60) and len(self.text.strip()) >= 4:
                out.append(self.text)
                self.text = ""
                self.first = False


class SegmentStore:
    def segments(self, call_id, gid=None, *, material=False, captions=False):
        keys = ("generation_id", "seq", "state", "playback_state", "revision")
        if captions:
            keys += ("text",)
        if material:
            columns = (
                "call_id,generation_id,seq,body,state,playback_state,revision,created_at,ready_at,"
                "play_started_at,playback_position_ms,end_reason"
            )
        else:
            columns = ",".join("body AS text" if k == "text" else k for k in keys)
        sql = "SELECT " + columns + " FROM speech_segments WHERE call_id=?"
        args = [call_id]
        if gid is not None:
            sql += " AND generation_id=?"
            args.append(gid)
        rows = self._db.execute(sql + " ORDER BY generation_id,seq", args)
        return [dict(r) if material else {k: r[k] for k in keys} for r in rows]

    def next_pending_segment(self, call_id, gid):
        row = self._db.execute(
            "SELECT seq,body FROM speech_segments "
            "WHERE call_id=? AND generation_id=? AND seq>0 AND state='pending' ORDER BY seq LIMIT 1",
            (call_id, gid),
        ).fetchone()
        return dict(row) if row is not None else None

    def add_segment(self, call_id, gid, body):
        from .store import StateConflict

        if type(body) is not str or not body.strip() or len(body) > 6000:
            raise StateConflict("segment.invalid")
        with self._transaction():
            if (
                self.is_call_closing(call_id)
                or self.get_generation(call_id, gid).state != "running"
            ):
                return None
            rows = self.segments(call_id, gid)
            if rows and rows[0]["state"] != "open":
                return None
            if len(rows) > 512:
                raise StateConflict("segment.limit")
            if not rows:
                self._db.execute(
                    "INSERT INTO speech_segments(call_id,generation_id,seq,body,state,created_at) VALUES (?, ?,0,'','open',?)",
                    (call_id, gid, time()),
                )
            seq = len(rows) if rows else 1
            self._db.execute(
                "INSERT INTO speech_segments(call_id,generation_id,seq,body,state,created_at) VALUES (?,?,?,?,'pending',?)",
                (call_id, gid, seq, body, time()),
            )
            return seq

    def sync_segment_turn(self, call_id, gid):
        rows = self.segments(call_id, gid)
        if len(rows) < 2:
            return
        group, parts = rows[0], rows[1:]
        played = any(
            r["playback_state"] in {"started", "ended"}
            or r["revision"] >= 2
            or r.get("playback_position_ms") is not None
            for r in parts
        )
        complete = all(r["playback_state"] == "ended" for r in parts)
        state = (
            "ended"
            if complete and group["state"] in {"complete", "stopped"}
            else "stopped"
            if group["state"] == "stopped"
            else "started"
            if played
            else "unknown"
        )
        self._db.execute(
            "UPDATE turns SET playback_state=?,playback_revision=playback_revision+1 WHERE call_id=? AND material_id=? AND role='assistant' AND playback_state<>?",
            (state, call_id, gid, state),
        )

    def stream_done(self, call_id, gid):
        with self._transaction():
            self._db.execute(
                "UPDATE speech_segments SET state='complete' WHERE call_id=? AND generation_id=? AND seq=0 AND state='open'",
                (call_id, gid),
            )
            self.sync_segment_turn(call_id, gid)

    def segment_open(self, call_id, gid, seq):
        rows = self.segments(call_id, gid)
        return (
            not self.is_call_closing(call_id)
            and bool(rows)
            and rows[0]["state"] in {"open", "complete"}
            and any(r["seq"] == seq and r["state"] == "running" for r in rows)
        )

    def finish_segment(self, call_id, gid, seq, state, data=None):
        from .speech import segment_audio_mime

        if state == "succeeded":
            segment_audio_mime(data)
        with self._transaction():
            if not self.segment_open(call_id, gid, seq):
                state, data = "cancelled", None
            self._db.execute(
                "UPDATE speech_segments SET state=?,data=?,ready_at=? WHERE call_id=? AND generation_id=? AND seq=? AND state='running'",
                (state, data, time(), call_id, gid, seq),
            )

    def stop_segments(self, call_id, gid=None):
        with nullcontext() if self._db.in_transaction else self._transaction():
            query, args = "call_id=?", [call_id]
            if gid is not None:
                query += " AND generation_id=?"
                args.append(gid)
            self._db.execute(
                "UPDATE speech_segments SET state='stopped' WHERE " + query + " AND seq=0", args
            )
            self._db.execute(
                "UPDATE speech_segments SET state='cancelled',data=NULL WHERE "
                + query
                + " AND seq>0 AND playback_state NOT IN ('started','ended')",
                args,
            )

            self._db.execute(
                "UPDATE speech_segments SET revision=revision+CASE WHEN playback_state='started' THEN 1 ELSE 0 END, playback_state='stopped' WHERE "
                + query
                + " AND seq>0 AND playback_state IN ('unknown','started')",
                args,
            )
            for item in self._db.execute(
                "SELECT generation_id FROM speech_segments WHERE " + query + " AND seq=0", args
            ).fetchall():
                self.sync_segment_turn(call_id, item[0])

    def segment_audio(self, call_id, gid, seq, authority):
        from .access import AccessError

        with self._transaction():
            if self.authenticate_web_session(*authority) != call_id or self.is_call_closing(
                call_id
            ):
                raise AccessError()
            group = self._db.execute(
                "SELECT state FROM speech_segments WHERE call_id=? AND generation_id=? AND seq=0",
                (call_id, gid),
            ).fetchone()
            if group is None or group["state"] == "stopped":
                raise AccessError("conflict")
            row = self._db.execute(
                "SELECT data FROM speech_segments "
                "WHERE call_id=? AND generation_id=? AND seq=? AND seq>0 AND state='succeeded'",
                (call_id, gid, seq),
            ).fetchone()
            if row is None or row["data"] is None:
                raise AccessError("conflict")
            return row["data"]

    def segment_playback(self, call_id, gid, seq, revision, state, authority):
        from .access import AccessError

        if (
            type(seq) is not int
            or seq < 1
            or type(revision) is not int
            or revision < 1
            or state not in {"started", "ended", "stopped", "failed"}
        ):
            raise AccessError("invalid_request")
        with self._transaction():
            if self.authenticate_web_session(*authority) != call_id:
                raise AccessError()
            row = next((r for r in self.segments(call_id, gid) if r["seq"] == seq), None)
            if row is None:
                raise AccessError("conflict")
            advanced = (
                not self.is_call_closing(call_id)
                and self.segments(call_id, gid)[0]["state"] != "stopped"
                and row["playback_state"] not in {"ended", "stopped", "failed"}
                and revision > row["revision"]
            )
            if advanced:
                self._db.execute(
                    "UPDATE speech_segments SET playback_state=?,revision=?,play_started_at=CASE WHEN ?='started' THEN COALESCE(play_started_at,?) ELSE play_started_at END WHERE call_id=? AND generation_id=? AND seq=?",
                    (state, revision, state, time(), call_id, gid, seq),
                )
                if state == "stopped" or (state == "failed" and seq != 1):
                    self.stop_segments(call_id, gid)
            self.sync_segment_turn(call_id, gid)
            result = next(r for r in self.segments(call_id, gid) if r["seq"] == seq)
            result["advanced"] = advanced
            result["superseded"] = result["playback_state"] in {"ended", "stopped", "failed"} and (
                result["revision"] < revision or result["playback_state"] != state
            )
            return result

    def segment_review(self, call_id, prompt):
        rows = self.segments(call_id, material=True)
        notes = [
            {
                "generation_id": r["generation_id"],
                "seq": r["seq"],
                "text": r["body"],
                "position_ms": r["playback_position_ms"],
                "end_reason": r["end_reason"],
                "playback": f"朗读被打断，仅部分播放；播放到约 {r['playback_position_ms']} ms，具体已听到哪些词未知"
                if r["playback_position_ms"] is not None
                else "已播放（浏览器报告结束）"
                if r["playback_state"] == "ended"
                else "已开始，未确认播完"
                if r["playback_state"] == "started"
                else "未播放／未确认",
            }
            for r in rows
            if r["seq"] > 0
        ]
        summaries = []
        for gid in dict.fromkeys(r["generation_id"] for r in rows):
            parts = [r for r in rows if r["generation_id"] == gid and r["seq"] > 0]
            played = any(
                r["playback_state"] in {"started", "ended"}
                or r["revision"] >= 2
                or r.get("playback_position_ms") is not None
                for r in parts
            )
            label = (
                "播完（浏览器报告）"
                if parts and all(r["playback_state"] == "ended" for r in parts)
                else "播了一部分（浏览器观测）"
                if played
                else "没播／未收到开始播放观测"
            )
            summaries.append({"generation_id": gid, "playback": label})
        return (
            prompt
            if not notes
            else prompt
            + "\n本通逐句公开文字及播放观测（与正文重复的只计一次；不代表听到或接受）："
            + json.dumps({"replies": summaries, "segments": notes}, ensure_ascii=False)
        )
