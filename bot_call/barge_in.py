"""One audio identity, durable decision, and playback observations (schema 22)."""

import asyncio
import re
import unicodedata
from dataclasses import dataclass, replace
from time import time

from .access import AccessError

FIELDS = {"utterance_id", "generation_id", "seq", "position_ms", "started", "action"}
ACTIONS = {
    "begin",
    "silent",
    "resumed",
    "timeout",
    "capacity",
    "unavailable",
    "cancel",
    "refresh",
    "long_speech",
}
SCHEMA = """CREATE TABLE IF NOT EXISTS barge_candidates (
 call_id TEXT NOT NULL, utterance_id TEXT NOT NULL, generation_id TEXT NOT NULL,
 seq INTEGER NOT NULL CHECK(seq BETWEEN 1 AND 512),
 position_ms INTEGER NOT NULL CHECK(position_ms BETWEEN 0 AND 3600000),
 started INTEGER NOT NULL CHECK(started IN (0,1)), created_at REAL NOT NULL,
 decision TEXT NOT NULL DEFAULT 'pending'
 CHECK(decision IN ('pending','interrupt','resume','error','cancelled')),
 closed_reason TEXT, dispatch TEXT NOT NULL DEFAULT 'pending'
 CHECK(dispatch IN ('pending','waiting','started','speech_only','cancelled')),
 PRIMARY KEY(call_id,utterance_id),
 FOREIGN KEY(call_id,generation_id) REFERENCES generations(call_id,generation_id))"""


def valid_request(data):
    return (
        type(data) is dict
        and FIELDS <= data.keys() <= FIELDS | {"remaining_ms"}
        and (
            "remaining_ms" not in data
            or data["remaining_ms"] is None
            or type(data["remaining_ms"]) is int
            and 0 <= data["remaining_ms"] <= 3600000
        )
        and all(
            type(data[k]) is str and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", data[k])
            for k in ("utterance_id", "generation_id")
        )
        and type(data["seq"]) is int
        and 1 <= data["seq"] <= 512
        and type(data["position_ms"]) is int
        and 0 <= data["position_ms"] <= 3600000
        and type(data["started"]) is int
        and data["started"] in (0, 1)
        and type(data["action"]) is str
        and data["action"] in ACTIONS
    )


DEFAULT_BACKCHANNEL_PHRASES = (
    "嗯",
    "恩",
    "唔",
    "哦",
    "噢",
    "喔",
    "啊",
    "对",
    "对的",
    "是",
    "是的",
    "好",
    "好的",
    "行",
    "明白",
    "知道了",
    "OK",
)
REASONS = {
    name: i
    for i, name in enumerate(
        (
            "content",
            "backchannel",
            "question_context",
            "boundary",
            "long_speech",
            "empty",
            "context_unknown",
            "error",
            "cancelled",
        ),
        1,
    )
}
# Snapshot at begin, before further generation/TTS can change the context.
CONTEXTS = {"middle": 0, "question_context": 1, "boundary": 2, "context_unknown": 3}


def normalize_backchannel(text):
    text = unicodedata.normalize("NFKC", text).casefold()
    text = "".join(
        c for c in text if not c.isspace() and not unicodedata.category(c).startswith("P")
    )
    return re.sub(r"(.)\1+", r"\1", text)


def valid_phrases(phrases):
    return (
        type(phrases) in (list, tuple)
        and len(phrases) <= 64
        and all(
            type(p) is str
            and 0 < len(p) <= 32
            and not any(unicodedata.category(c).startswith("C") for c in p)
            and 0 < len(normalize_backchannel(p)) <= 4
            and all(c.isalpha() for c in normalize_backchannel(p))
            for p in phrases
        )
    )


@dataclass(frozen=True)
class CandidateContext:
    kind: str = "context_unknown"
    long_speech: bool = False
    phrases: tuple = DEFAULT_BACKCHANNEL_PHRASES


@dataclass(frozen=True)
class CandidateDecision:
    action: str
    reason: str


def decide_candidate(text, context):
    """Whole-utterance lexical check; uncertain evidence always yields the floor."""
    if not text.strip():
        return CandidateDecision("resume", "empty")
    if context.long_speech:
        return CandidateDecision("interrupt", "long_speech")
    normalized = normalize_backchannel(text)
    words = {normalize_backchannel(p) for p in context.phrases}
    reachable = {0}
    if 0 < len(normalized) <= 4 and "?" not in unicodedata.normalize("NFKC", text):
        for i in range(len(normalized)):
            for word in words:
                # 啊 alone can be a backchannel; mixed 啊 is ambiguous.
                if (
                    i in reachable
                    and word
                    and (word != "啊" or normalized == "啊")
                    and normalized.startswith(word, i)
                ):
                    reachable.add(i + len(word))
    if not normalized or len(normalized) not in reachable:
        return CandidateDecision("interrupt", "content")
    if context.kind != "middle":
        return CandidateDecision(
            "interrupt", context.kind if context.kind in CONTEXTS else "context_unknown"
        )
    return CandidateDecision("resume", "backchannel")


def question_sentence(text):
    text = text.rstrip().rstrip('”’"』」）) ').rstrip()
    return (
        "?" in text
        or "？" in text
        or bool(
            re.search(
                r"(?:吗|嘛|么|呢|好不好|对不对|是不是|行不行|可不可以|要不要|能不能|有没有|如何|怎样|怎么样)[。！!，,；;…\s]*$",
                text,
            )
        )
    )


class BargeStore:
    def _candidate_consumed(self, call_id, uid):
        row = self._db.execute(
            "SELECT decision,dispatch FROM barge_candidates WHERE call_id=? AND utterance_id=?",
            (call_id, uid),
        ).fetchone()
        return row is not None and (
            row["decision"] in {"resume", "error", "cancelled"}
            or row["dispatch"] in {"speech_only", "cancelled"}
        )

    def _series_unresolved(self, root):
        # A classified candidate may intentionally have no Agent generation.
        # It must not retain M5's single-expression slot forever.
        return not self._candidate_consumed(
            root.call_id, root.series_id
        ) and super()._series_unresolved(root)

    def series_views(self, call_id):
        return tuple(
            dict(row, state="replied")
            if self._barge_metric(call_id, row["series_id"], "barge_reason")
            == REASONS["backchannel"]
            else dict(row, state="interrupted")
            if self._candidate_consumed(call_id, row["series_id"]) and row["state"] != "abandoned"
            else row
            for row in super().series_views(call_id)
        )

    def _barge_metric(self, call_id, uid, stage, value=None):
        if value is not None:
            from .observability import BARGE_SERVER_METRICS, TIMING_FIELDS, timing_row

            assert stage in BARGE_SERVER_METRICS
            metric = timing_row(dict(stage=stage, sequence=0, start_ms=0, elapsed_ms=value))
            assert metric is not None
            self._db.execute(
                "INSERT OR IGNORE INTO turn_timings (call_id,generation_id,"
                + ",".join(TIMING_FIELDS)
                + ",recorded_at) VALUES ("
                + ",".join("?" for _ in range(len(TIMING_FIELDS) + 3))
                + ")",
                (call_id, "capture:" + uid, *(metric[k] for k in TIMING_FIELDS), time()),
            )
        row = self._db.execute(
            "SELECT elapsed_ms FROM turn_timings WHERE call_id=? AND generation_id=? AND stage=? AND sequence=0",
            (call_id, "capture:" + uid, stage),
        ).fetchone()
        return int(row[0]) if row else None

    def _context_parts(self, call_id, gid):
        # Context never fetches encoded audio BLOBs into the pause budget.
        return [
            dict(r)
            for r in self._db.execute(
                "SELECT seq,body,playback_state FROM speech_segments WHERE call_id=? AND generation_id=? ORDER BY seq",
                (call_id, gid),
            )
        ]

    def _candidate_context(self, call_id, data):
        parts = self._context_parts(call_id, data["generation_id"])
        current = next((p for p in parts if p["seq"] == data["seq"]), None)
        previous = next(
            (
                p
                for p in parts
                if p["seq"] == data["seq"] - 1 and p["seq"] > 0 and p["playback_state"] == "ended"
            ),
            None,
        )
        if not current:
            return "context_unknown"
        if any(question_sentence(p["body"]) for p in (current, previous) if p):
            return "question_context"
        # Only a known following sentence proves this is not the final one.
        last = not any(p["seq"] > data["seq"] for p in parts)
        remaining = data.get("remaining_ms")
        if last and remaining is None:
            return "context_unknown"
        if last and remaining <= 1000:
            return "boundary"
        return "middle"

    def barge_rows(self, call_id):
        names = {code: name for name, code in REASONS.items()}
        out = []
        for row in self._db.execute(
            "SELECT b.*,t.elapsed_ms AS reason_code FROM barge_candidates b LEFT JOIN turn_timings t ON t.call_id=b.call_id AND t.generation_id='capture:'||b.utterance_id AND t.stage='barge_reason' AND t.sequence=0 WHERE b.call_id=? ORDER BY b.created_at",
            (call_id,),
        ):
            item = dict(row)
            item["reason"] = names.get(item.pop("reason_code"))
            item["purpose"] = "backchannel" if item["reason"] == "backchannel" else None
            out.append(item)
        return out

    def candidate_for(self, call_id, utterance_id):
        from .store import NotFound

        try:
            job = self.get_audio_job(call_id, utterance_id)
            utterance_id = job.series_id or utterance_id
        except NotFound:
            pass
        return next(
            (r for r in self.barge_rows(call_id) if r["utterance_id"] == utterance_id), None
        )

    def barge_command(self, call_id, data, authority):
        from .store import NotFound

        if not valid_request(data):
            raise AccessError("invalid_request")
        uid, gid, seq = (data[k] for k in ("utterance_id", "generation_id", "seq"))
        with self._transaction():
            if self.authenticate_web_session(*authority) != call_id:
                raise AccessError()
            row = self.candidate_for(call_id, uid)
            if row is None:
                if data["action"] != "begin" or self.is_call_closing(call_id):
                    raise AccessError("conflict")
                candidates = self.barge_rows(call_id)
                if len(candidates) >= 4096 or any(r["decision"] == "pending" for r in candidates):
                    raise AccessError("conflict")
                parts = self.segments(call_id, gid)
                if not any(r["seq"] == 0 and r["state"] in {"open", "complete"} for r in parts):
                    raise AccessError("conflict")
                if not any(
                    r["seq"] == seq and r["playback_state"] not in {"ended", "stopped", "failed"}
                    for r in parts
                ):
                    raise AccessError("conflict")
                if self._find_turn(call_id, "user", uid) is not None:
                    raise AccessError("conflict")
                try:
                    self.get_audio_job(call_id, uid)
                except NotFound:
                    pass
                else:
                    raise AccessError("conflict")
                self._db.execute(
                    "INSERT INTO barge_candidates(call_id,utterance_id,generation_id,seq,position_ms,started,created_at) VALUES (?,?,?,?,?,?,?)",
                    (call_id, uid, gid, seq, data["position_ms"], data["started"], time()),
                )
                self._barge_metric(
                    call_id, uid, "barge_context", CONTEXTS[self._candidate_context(call_id, data)]
                )
                row = self.candidate_for(call_id, uid)
            if (
                row["utterance_id"],
                row["generation_id"],
                row["seq"],
                row["position_ms"],
                row["started"],
            ) != (uid, gid, seq, data["position_ms"], data["started"]):
                raise AccessError("conflict")
            action = data["action"]
            if action == "silent" and row["decision"] == "pending":
                try:
                    self.get_audio_job(call_id, uid)
                except NotFound:
                    self._barge_metric(call_id, uid, "barge_reason", REASONS["empty"])
                    self._db.execute(
                        "UPDATE barge_candidates SET decision='resume' WHERE call_id=? AND utterance_id=?",
                        (call_id, uid),
                    )
                else:
                    raise AccessError("conflict")
            elif action == "resumed":
                if row["decision"] != "resume" or row["closed_reason"] not in {None, "resumed"}:
                    raise AccessError("conflict")
                parts = self.segments(call_id, gid)
                if not any(r["seq"] == 0 and r["state"] in {"open", "complete"} for r in parts):
                    raise AccessError("conflict")
                self._db.execute(
                    "UPDATE barge_candidates SET closed_reason='resumed' WHERE call_id=? AND utterance_id=?",
                    (call_id, uid),
                )
            elif action == "long_speech":
                # Acoustic confirmation ends output now, but classification and
                # Agent dispatch still wait for the complete, once-transcribed
                # expression. Preserve stop-reading/tool semantics until then.
                if row["decision"] != "pending" or row["closed_reason"] not in {
                    None,
                    "long_speech",
                }:
                    raise AccessError("conflict")
                self._db.execute(
                    "UPDATE barge_candidates SET closed_reason='long_speech' WHERE call_id=? AND utterance_id=?",
                    (call_id, uid),
                )
                self.seal_barge_group(call_id, row, "long_speech", stop_agent=False)
            elif action in {"cancel", "refresh", "timeout", "capacity", "unavailable"}:
                self._db.execute(
                    "UPDATE barge_candidates SET closed_reason=COALESCE(closed_reason,?),decision=CASE WHEN ? IN ('timeout','capacity','unavailable') THEN decision WHEN decision='pending' THEN 'cancelled' ELSE decision END WHERE call_id=? AND utterance_id=?",
                    (action, action, call_id, uid),
                )
                if action in {"cancel", "refresh"}:
                    if row["decision"] == "pending":
                        self._barge_metric(call_id, uid, "barge_reason", REASONS["cancelled"])
                    self._db.execute(
                        "UPDATE barge_candidates SET dispatch='cancelled' WHERE call_id=? AND utterance_id=? AND dispatch IN ('pending','waiting')",
                        (call_id, uid),
                    )
                self.seal_barge_group(call_id, row, action, stop_agent=False)
        return self.candidate_for(call_id, uid)

    def seal_barge_group(self, call_id, row, reason="superseded", *, stop_agent=True):
        # Caller owns a transaction; preserve all generated text and completed facts.
        gid = row["generation_id"]
        self._db.execute(
            "UPDATE speech_segments SET playback_position_ms=?,end_reason=? WHERE call_id=? AND generation_id=? AND seq=? AND playback_state<>'ended'",
            (row["position_ms"], reason, call_id, gid, row["seq"]),
        )
        self.stop_segments(call_id, gid)
        if stop_agent:
            self._db.execute(
                "UPDATE generations SET state='cancelled' WHERE call_id=? AND generation_id=? AND state='running'",
                (call_id, gid),
            )
        self._db.execute(
            "UPDATE speech_segments SET end_reason=COALESCE(end_reason,?) WHERE call_id=? AND generation_id=?",
            (reason, call_id, gid),
        )

    def resolve_candidate(self, call_id, row, phrases=DEFAULT_BACKCHANNEL_PHRASES):
        from .store import NotFound

        if row["decision"] != "pending":
            return row
        uid = row["utterance_id"]
        try:
            job = self.get_audio_job(call_id, uid)
        except NotFound:
            return row
        text, decision = "", None
        if job.series_id and job.series_disposition == "sealed" and self.series_ready(job):
            text = next(
                t.body
                for t in self.effective_transcript(call_id)
                if t.role == "user" and t.material_id == uid
            )
        elif job.series_id:
            members = [j for j in self.get_audio_jobs(call_id) if j.series_id == uid]
            if any(j.state == "failed" and j.error_code == "empty_transcription" for j in members):
                decision = (
                    "resume"
                    if len(members) == 1
                    and job.series_disposition == "sealed"
                    and job.series_end_index == 1
                    else "error"
                )
            elif any(j.state in {"failed", "uncertain", "cancelled"} for j in members):
                decision = "error"
            else:
                return row
        elif job.state == "succeeded":
            text = self._find_turn(call_id, "user", uid).body
        elif job.state == "failed" and job.error_code == "empty_transcription":
            decision = "resume"
        elif job.state in {"failed", "uncertain", "cancelled"}:
            decision = "error"
        else:
            return row
        context = CandidateContext(
            next(
                (
                    name
                    for name, value in CONTEXTS.items()
                    if value == self._barge_metric(call_id, uid, "barge_context")
                ),
                "context_unknown",
            ),
            row["closed_reason"] == "long_speech",
            tuple(phrases),
        )
        choice = (
            decide_candidate(text, context)
            if decision != "error"
            else CandidateDecision("error", "error")
        )
        decision = choice.action
        with self._transaction():
            self._db.execute(
                "UPDATE barge_candidates SET decision=? WHERE call_id=? AND utterance_id=? AND decision='pending'",
                (decision, call_id, uid),
            )
            self._barge_metric(call_id, uid, "barge_reason", REASONS[choice.reason])
            if choice.reason == "backchannel":
                count = self._db.execute(
                    "SELECT COUNT(*) FROM barge_candidates b JOIN turn_timings t ON t.call_id=b.call_id AND t.generation_id='capture:'||b.utterance_id AND t.stage='barge_reason' AND t.elapsed_ms=? WHERE b.call_id=?",
                    (REASONS["backchannel"], call_id),
                ).fetchone()[0]
                self._barge_metric(call_id, uid, "barge_backchannels", min(4096, count))
            if decision in {"interrupt", "error"}:
                self.seal_barge_group(call_id, row)
        return self.candidate_for(call_id, uid)

    def effective_transcript(self, call_id, raw=None):
        # Immutable turns retain STT provenance. Persisted classification supplies
        # the purpose tag in the effective material and frozen review snapshot.
        return tuple(
            replace(t, source="backchannel")
            if t.role == "user"
            and self._barge_metric(call_id, t.material_id, "barge_reason") == REASONS["backchannel"]
            else t
            for t in super().effective_transcript(call_id, raw)
        )

    def series_review_prompt(self, call_id, prompt):
        if any(t.source == "backchannel" for t in self.effective_transcript(call_id)):
            prompt += "\nsource=backchannel 的用户原文是附和，仅表示正在听；不代表接受建议、作出决定或授权工具操作。"
        return super().series_review_prompt(call_id, prompt)

    def speech_superseded(self, call_id, gid):
        return any(
            r["generation_id"] == gid
            and (
                r["decision"] in {"interrupt", "error", "cancelled"}
                or r["closed_reason"] not in {None, "resumed"}
            )
            for r in self.barge_rows(call_id)
        )

    def playback_projection(self, call_id, turns):
        out = []
        for t in turns:
            if t.role != "assistant" or not self.speech_superseded(call_id, t.material_id):
                out.append(t)
                continue
            parts = self.segments(call_id, t.material_id, material=True)
            expressed = "".join(
                r["body"] for r in parts if r["seq"] > 0 and r["playback_state"] == "ended"
            )
            sequence = self.get_generation(call_id, t.material_id).user_sequence
            out.append(replace(t, sequence=sequence, body=expressed))
        return tuple(sorted(out, key=lambda t: t.sequence))

    def playback_context_notes(self, call_id, turns):
        """System-only observations, never dialogue or a spoken reply template."""
        notes = []
        for t in turns:
            if t.role == "assistant" and self.speech_superseded(call_id, t.material_id):
                notes.append(
                    f"assistant sequence={self.get_generation(call_id, t.material_id).user_sequence}: interrupted=true; "
                    "only_completed_sentences_in_context=true; partial_words=unknown; "
                    "unplayed_text_is_not_shared_knowledge=true; tool_reports_remain_valid=true"
                )
            elif t.role == "user" and t.source == "backchannel":
                notes.append(f"user sequence={t.sequence}: purpose=backchannel")
        if not notes:
            return ""
        return (
            "电话系统播放观测（控制信息，不是对话内容，禁止在回复中复述）："
            "被打断的回复在上下文中仅含已完整播放的句子，部分句子实际听到哪些词未知；"
            "未播内容不能视为用户已知，工具事实按独立报告保留。"
            "purpose=backchannel 的用户附和仅表示正在听，不代表同意、决定或工具授权。\n"
            + "\n".join(notes)
        )

    def save_cancelled_material(self, call_id, gid, body):
        if not body.strip() or not self.speech_superseded(call_id, gid):
            return
        with self._transaction():
            if self._find_turn(call_id, "assistant", gid) is None and not self._review_exists(
                call_id
            ):
                generation = self.get_generation(call_id, gid)
                self._insert_turn(call_id, "assistant", gid, generation.source, body)
                self.sync_segment_turn(call_id, gid)


class BargeRuntime:
    async def web_barge_in(self, token, reference, data):
        return await self._request("web_barge_in", token, reference, data)

    def _cancel_barge_audio(self, call_id, row):
        task = self._segment_tasks.get((call_id, row["generation_id"]))
        if task is not None:
            task.cancel()

    def _progress_candidates(self, call_id):
        for before in self._store.barge_rows(call_id):
            row = self._store.resolve_candidate(call_id, before, self._backchannel_phrases)
            if row["decision"] in {"interrupt", "error", "cancelled"} or row[
                "closed_reason"
            ] not in {None, "resumed"}:
                self._cancel_barge_audio(call_id, row)
            key = (call_id, row["utterance_id"])
            if (
                row["decision"] == "interrupt"
                and row["dispatch"] == "pending"
                and key not in self._barge_tasks
            ):
                task = self._spawn(self._dispatch_candidate(call_id, row), "reply")
                self._barge_tasks[key] = task

                def finished(_, key=key):
                    self._barge_tasks.pop(key, None)
                    self._speech_progress(key[0])

                task.add_done_callback(finished)
        self._notify_state(call_id)

    async def _dispatch_candidate(self, call_id, row):
        uid, gid = row["utterance_id"], row["generation_id"]
        self._store._db.execute(
            "UPDATE barge_candidates SET dispatch='waiting' WHERE call_id=? AND utterance_id=? AND dispatch='pending'",
            (call_id, uid),
        )
        self._notify_state(call_id)
        # Business callback closes owned LLM wait; tools are allowed to finish.
        if callable(self._callbacks.interrupt):
            await self._bridge("interrupt", self._store.get_binding(call_id), gid)
        while (call_id, gid) in self._store._active_agents:
            await asyncio.sleep(0.01)
        task = self._segment_tasks.get((call_id, gid))
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        current = self._store.candidate_for(call_id, uid)
        if (
            self._store.is_call_closing(call_id)
            or current["dispatch"] != "waiting"
            or current["decision"] != "interrupt"
            or current["closed_reason"] in {"cancel", "refresh"}
        ):
            self._store._db.execute(
                "UPDATE barge_candidates SET dispatch='cancelled' WHERE call_id=? AND utterance_id=?",
                (call_id, uid),
            )
            self._notify_state(call_id)
            return
        from .service import process_utterance

        job = self._store.get_audio_job(call_id, uid)
        turn = self._store._find_turn(call_id, "user", uid)
        self._store._db.execute(
            "UPDATE barge_candidates SET dispatch='started' WHERE call_id=? AND utterance_id=?",
            (call_id, uid),
        )
        self._notify_state(call_id)
        await process_utterance(
            self._store,
            call_id,
            uid,
            turn.body,
            self._reply,
            source="audio_transcription",
            series=job.series_id is not None,
        )
        self._notify_state(call_id)
