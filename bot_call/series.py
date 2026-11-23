"""Persisted speech-series identity and read-only material projection. No IO/SDK."""

from dataclasses import replace
import re

from .audio import AudioError, audio_settled

COLUMNS = {
    "series_id": "TEXT",
    "series_index": "INTEGER",
    "series_final": "INTEGER",
    "series_limit": "INTEGER",
    "series_end_index": "INTEGER",
    "series_disposition": "TEXT",
}
DISPOSITIONS = (
    "open",
    "sealed",
    "abandoned_local_discard",
    "abandoned_upload_missing",
    "abandoned_stt_missing",
    "abandoned_page_reopened",
)
SCHEMA = (
    "CREATE UNIQUE INDEX series_member ON audio_jobs(call_id,series_id,series_index) WHERE series_id IS NOT NULL",
    """CREATE TRIGGER checked_series_insert BEFORE INSERT ON audio_jobs
    WHEN COALESCE(NOT (
      (NEW.series_id IS NULL AND NEW.series_index IS NULL AND NEW.series_final IS NULL
       AND NEW.series_limit IS NULL AND NEW.series_end_index IS NULL AND NEW.series_disposition IS NULL)
      OR (NEW.series_id IS NOT NULL AND NEW.series_index BETWEEN 1 AND 6 AND NEW.series_final IN (0,1)
       AND ((NEW.series_index=1 AND NEW.series_id=NEW.utterance_id AND NEW.series_limit BETWEEN 1 AND 6
         AND NEW.series_disposition IN ('open','sealed')
         AND ((NEW.series_final=0 AND NEW.series_disposition='open' AND NEW.series_end_index IS NULL)
           OR (NEW.series_final=1 AND NEW.series_disposition='sealed' AND NEW.series_end_index=1)))
        OR (NEW.series_index>1 AND NEW.series_limit IS NULL AND NEW.series_end_index IS NULL
         AND NEW.series_disposition IS NULL AND EXISTS(SELECT 1 FROM audio_jobs r
           WHERE r.call_id=NEW.call_id AND r.utterance_id=NEW.series_id AND r.series_index=1
           AND NEW.series_index<=r.series_limit)))
       AND NEW.series_index=COALESCE((SELECT MAX(series_index)+1 FROM audio_jobs a
          WHERE a.call_id=NEW.call_id AND a.series_id=NEW.series_id),1)))
    ,1) BEGIN SELECT RAISE(ABORT,'invalid series identity'); END""",
    """CREATE TRIGGER immutable_series_identity BEFORE UPDATE OF series_id,series_index,series_final,series_limit ON audio_jobs
      BEGIN SELECT RAISE(ABORT,'series source is immutable'); END""",
    """CREATE TRIGGER checked_series_progress BEFORE UPDATE OF series_end_index,series_disposition ON audio_jobs
    WHEN COALESCE(NOT (OLD.series_index=1 AND NEW.series_end_index BETWEEN 1 AND OLD.series_limit
      AND (OLD.series_end_index IS NULL OR NEW.series_end_index=OLD.series_end_index)
      AND (OLD.series_disposition='open' OR OLD.series_disposition='sealed' AND NEW.series_disposition LIKE 'abandoned_%')
      AND NEW.series_disposition IN ('sealed','abandoned_local_discard','abandoned_upload_missing','abandoned_stt_missing','abandoned_page_reopened'))
    ,1) BEGIN SELECT RAISE(ABORT,'series decision is terminal'); END""",
)


# schema20's trigger remains the historical migration; 21 only broadens the
# insert constraint. Persisted rows and the first segment's frozen cap stay put.
LIMIT_SCHEMA = (
    "DROP TRIGGER checked_series_insert",
    SCHEMA[1].replace("BETWEEN 1 AND 6", "BETWEEN 1 AND 20"),
)


def identity(series):
    if series is None:
        return
    if (
        type(series) is not tuple
        or len(series) != 3
        or type(series[0]) is not str
        or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", series[0]) is None
        or type(series[1]) is not int
        or not 1 <= series[1] <= 20
        or type(series[2]) is not bool
    ):
        raise AudioError("invalid")


def matches(job, series):
    identity(series)
    if (job.series_id, job.series_index, job.series_final) != (
        (series[0], series[1], int(series[2])) if series else (None, None, None)
    ):
        raise AudioError("conflict")


def join_text(parts):
    out = ""
    for text in parts:
        if (
            out
            and text
            and out[-1].isascii()
            and out[-1].isalnum()
            and text[0].isascii()
            and text[0].isalnum()
        ):
            out += " "
        out += text
    return out


class SeriesStore:
    def series_roots(self, call_id):
        return tuple(
            self._audio_with_resolution(row)
            for row in self._db.execute(
                "SELECT * FROM audio_jobs WHERE call_id=? AND series_id IS NOT NULL AND series_index=1 ORDER BY ordinal",
                (call_id,),
            )
        )

    def _series_members(self, call_id, sid):
        return tuple(
            self._audio_with_resolution(row)
            for row in self._db.execute(
                "SELECT * FROM audio_jobs WHERE call_id=? AND series_id=? ORDER BY series_index",
                (call_id, sid),
            )
        )

    def _series_unresolved(self, root):
        return (
            root.series_disposition in {"open", "sealed"}
            and self._db.execute(
                "SELECT 1 FROM generations WHERE call_id=? AND generation_id=?",
                (root.call_id, "reply:" + root.series_id),
            ).fetchone()
            is None
        )

    def check_series_receive(self, call_id, utterance_id, series, limit):
        identity(series)
        active = [r for r in self.series_roots(call_id) if self._series_unresolved(r)]
        if series is None:
            if active:
                raise AudioError("conflict")
            return (None,) * 6
        sid, index, final = series
        roots = {r.series_id: r for r in self.series_roots(call_id)}
        root = roots.get(sid)
        if root is None:
            if index != 1 or utterance_id != sid or active:
                raise AudioError("conflict")
            maximum = limit
        else:
            maximum = root.series_limit
            members = self._series_members(call_id, sid)
            if index != len(members) + 1 or root.series_disposition == "sealed":
                raise AudioError("conflict")
            if root.series_disposition != "open" and index > root.series_end_index:
                raise AudioError("conflict")
        if index > maximum or (
            index == maximum and not final and (root is None or root.series_disposition == "open")
        ):
            raise AudioError("conflict")
        return (
            sid,
            index,
            int(final),
            maximum if index == 1 else None,
            1 if index == 1 and final else None,
            ("sealed" if final else "open") if index == 1 else None,
        )

    def series_control(self, call_id, sid, action, last_index, reason, *, _web_authority=None):
        identity((sid, last_index, False))
        disposition = (
            "sealed"
            if action == "seal" and reason == "no_audio_tail"
            else (
                "abandoned_" + reason
                if action == "abandon" and "abandoned_" + reason in DISPOSITIONS
                else None
            )
        )
        if disposition is None:
            raise AudioError("invalid")
        with self._transaction():
            if (
                _web_authority is not None
                and self.authenticate_web_session(*_web_authority) != call_id
            ):
                raise AudioError("conflict")
            root = self.get_audio_job(call_id, sid)
            members = self._series_members(call_id, sid)
            if root.series_index != 1 or not len(members) <= last_index <= root.series_limit:
                raise AudioError("conflict")
            if root.series_disposition == disposition and root.series_end_index == last_index:
                return root
            if self.is_call_closing(call_id) or self.limit_due(call_id):
                raise AudioError("closed")
            if not self._series_unresolved(root):
                raise AudioError("conflict")
            if disposition == "sealed" and len(members) != last_index:
                raise AudioError("conflict")
            if root.series_disposition not in {"open", "sealed"} or (
                root.series_end_index is not None and root.series_end_index != last_index
            ):
                raise AudioError("conflict")
            if disposition.startswith("abandoned_"):
                # Retain uncertain bytes before releasing the ordered STT barrier.
                for job in members:
                    if job.state in {"failed", "uncertain", "cancelled"} and not audio_settled(job):
                        self._retain_audio_missing_row(call_id, job.utterance_id)
            self._db.execute(
                "UPDATE audio_jobs SET series_end_index=?,series_disposition=? WHERE call_id=? AND utterance_id=?",
                (last_index, disposition, call_id, sid),
            )
        return self.get_audio_job(call_id, sid)

    def series_ready(self, root):
        members = self._series_members(root.call_id, root.series_id)
        return (
            root.series_disposition == "sealed"
            and len(members) == root.series_end_index
            and all(j.state == "succeeded" for j in members)
        )

    def series_views(self, call_id):
        out = []
        for root in self.series_roots(call_id):
            members = self._series_members(call_id, root.series_id)
            generation = self._db.execute(
                "SELECT state FROM generations WHERE call_id=? AND generation_id=?",
                (call_id, "reply:" + root.series_id),
            ).fetchone()
            blocked = next(
                (j for j in members if j.state in {"failed", "uncertain", "cancelled"}), None
            )
            state = (
                "abandoned"
                if root.series_disposition.startswith("abandoned_")
                else "replied"
                if generation and generation["state"] == "succeeded"
                else "interrupted"
                if self.is_call_closing(call_id)
                else "blocked"
                if blocked
                else "ready"
                if self.series_ready(root)
                else root.series_disposition
            )
            out.append(
                dict(
                    series_id=root.series_id,
                    max_segments=root.series_limit,
                    received_segments=len(members),
                    transcribed_segments=sum(j.state == "succeeded" for j in members),
                    series_end_index=root.series_end_index,
                    state=state,
                    disposition=root.series_disposition,
                    blocked_utterance_id=blocked.utterance_id if blocked else None,
                    blocked_index=blocked.series_index if blocked else None,
                    reply_utterance_id=root.series_id,
                )
            )
        return tuple(out)

    def effective_transcript(self, call_id, raw=None):
        raw = self.get_transcript(call_id) if raw is None else raw
        jobs = {
            row["utterance_id"]: row
            for row in self._db.execute(
                "SELECT utterance_id,series_id,series_index,series_disposition FROM audio_jobs WHERE call_id=? AND series_id IS NOT NULL",
                (call_id,),
            )
        }
        if not jobs:
            return raw
        roots = {r["series_id"]: r for r in jobs.values() if r["series_index"] == 1}
        grouped = {}
        for turn in raw:
            job = jobs.get(turn.material_id) if turn.role == "user" else None
            if job:
                grouped.setdefault(job["series_id"], []).append(turn.body)
        emitted, out = set(), []
        for turn in raw:
            job = jobs.get(turn.material_id) if turn.role == "user" else None
            if job is None:
                out.append(turn)
                continue
            if job["series_id"] in emitted:
                continue
            emitted.add(job["series_id"])
            root = roots[job["series_id"]]
            parts = grouped[job["series_id"]]
            # Missing/abandoned parts are separate fragments, never a completed sentence.
            body = (
                "\n".join(parts)
                if root["series_disposition"].startswith("abandoned_")
                else join_text(parts)
            )
            out.append(replace(turn, material_id=root["series_id"], body=body))
        return tuple(out)

    def series_review_prompt(self, call_id, prompt):
        notes = []
        for row in self.series_views(call_id):
            root = self.get_audio_job(call_id, row["series_id"])
            if row["state"] in {"abandoned", "interrupted", "open", "blocked"}:
                members = self._series_members(call_id, root.series_id)
                end = root.series_end_index or len(members)
                good = {j.series_index for j in members if j.state == "succeeded"}
                missing = [str(n) for n in range(1, end + 1) if n not in good]
                notes.append(
                    f"第{root.ordinal}段起的连续表达已收到部分、未完成或未回复；"
                    + ("缺失段序：" + ",".join(missing) + "。" if missing else "后续未确认。")
                )
        return prompt + (
            "\n\n连续表达限制：" + "\n".join(notes) + "不能跨缺失拼成完整发言或推定决定。"
            if notes
            else ""
        )
