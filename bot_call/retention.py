"""Expire audio bytes while preserving the text and frozen review material."""

from contextlib import contextmanager
import logging
from pathlib import Path
import shutil
import sqlite3
from time import monotonic, time

_log = logging.getLogger(__name__)
_MIB = 1024 * 1024
_FAILED_STATES = "('failed','cancelled','uncertain')"
_AUDIO_TABLES = (
    ("speech_segments", "data IS NOT NULL", "NULL", "cleared_segments"),
    ("speech_jobs", "data IS NOT NULL", "NULL", "cleared_speech_jobs"),
    ("audio_jobs", f"state IN {_FAILED_STATES} AND length(data)>0", "X''", "cleared_audio_jobs"),
)
_JOB_STATES = {
    "generations": "pending",
    "audio_jobs": "pending",
    "speech_jobs": "pending",
    "speech_segments": "pending",
    "review_retry_attempts": "pending",
    "agent_runs": "claimed",
}
_UNCLOSED_LIMIT = "SELECT 1 FROM call_limits AS limits WHERE limits.close_reason IS NULL"

# Shared with Store's audio claim and insert-review guard. The frozen resolution
# identifies the original source even after its bytes expire.
_AUDIO_SETTLED_SQL = """(state='succeeded' OR (state='failed' AND error_code='empty_transcription')
    OR EXISTS(SELECT 1 FROM audio_resolutions r JOIN audio_missing_policies p USING(call_id)
      WHERE p.policy='retained_missing_v1' AND r.call_id=audio_jobs.call_id
       AND r.utterance_id=audio_jobs.utterance_id AND r.ordinal=audio_jobs.ordinal
       AND r.mime=audio_jobs.mime AND r.sha256=audio_jobs.sha256 AND r.byte_count=audio_jobs.byte_count
       AND r.received_at=audio_jobs.received_at AND r.original_state=audio_jobs.state
       AND r.original_error=audio_jobs.error_code AND r.decision='retained_missing'))"""

_ALL_PAGES_SENT = """(EXISTS(SELECT 1 FROM publication_pages WHERE call_id=c.call_id)
    AND NOT EXISTS(SELECT 1 FROM publication_pages WHERE call_id=c.call_id AND state!='succeeded'))"""
_IDLE_CALL = " AND ".join(
    [f"NOT EXISTS({_UNCLOSED_LIMIT} AND limits.call_id=c.call_id)"]
    + [
        f"NOT EXISTS(SELECT 1 FROM {table} AS job WHERE job.call_id=c.call_id AND job.state IN ('running','{queued}'))"
        for table, queued in _JOB_STATES.items()
    ]
)


def _terminal_time(call_id):
    # Used for candidate selection, the in-transaction recheck, and audio_progress.
    # A NULL result means that the review is not terminal.
    return f"""(SELECT CASE
        WHEN p.context_state='succeeded' AND a.state='succeeded'
         AND NOT EXISTS(SELECT 1 FROM review_retry_nodes AS n
             JOIN review_retry_attempts AS attempt USING(attempt_id)
             WHERE attempt.call_id=c.call_id AND n.state IN ('pending','running'))
         AND NOT EXISTS(SELECT 1 FROM review_work_nodes
             WHERE call_id=c.call_id AND state IN ('pending','running'))
         AND ({_ALL_PAGES_SENT} OR (a.context_only=1 AND a.completed_at IS NOT NULL
             AND a.send_requested_at IS NULL
             AND NOT EXISTS(SELECT 1 FROM publication_pages
                 WHERE call_id=c.call_id AND state='running')))
        THEN COALESCE(a.completed_at,r.ended_at)
        WHEN r.state='succeeded' AND p.context_state='succeeded' AND {_ALL_PAGES_SENT}
        THEN r.ended_at
        WHEN x.call_id IS NOT NULL AND r.call_id IS NULL
         AND NOT EXISTS(SELECT 1 FROM turns WHERE call_id=c.call_id)
         AND NOT EXISTS(SELECT 1 FROM audio_jobs WHERE call_id=c.call_id AND NOT {_AUDIO_SETTLED_SQL})
        THEN x.requested_at
        END
        FROM calls AS c LEFT JOIN reviews AS r USING(call_id)
        LEFT JOIN publications AS p USING(call_id)
        LEFT JOIN call_closures AS x USING(call_id)
        LEFT JOIN review_retry_attempts AS a ON a.attempt_id=(
            SELECT MAX(attempt_id) FROM review_retry_attempts WHERE call_id=c.call_id)
        WHERE c.call_id={call_id} AND {_IDLE_CALL})"""


def audio_progress_trigger(*, retention=False):
    cleanup = (
        f"""
          OR (OLD.state IN {_FAILED_STATES} AND NEW.state=OLD.state AND NEW.user_sequence IS OLD.user_sequence
            AND NEW.result_sha256 IS OLD.result_sha256 AND NEW.error_code=OLD.error_code
            AND NEW.data IS X'' AND {_terminal_time("OLD.call_id")} IS NOT NULL)"""
        if retention
        else ""
    )
    return f"""CREATE TRIGGER audio_progress BEFORE UPDATE OF state,data,user_sequence,result_sha256,error_code ON audio_jobs
        WHEN NOT ((OLD.state='pending' AND NEW.state='running' AND NEW.data=OLD.data)
          OR (OLD.state='running' AND NEW.state='succeeded' AND NEW.data IS NULL)
          OR (OLD.state='running' AND NEW.state IN ('failed','cancelled','uncertain') AND NEW.data=OLD.data){cleanup})
        BEGIN SELECT RAISE(ABORT,'audio outcome is terminal'); END"""


def has_active_calls(store, now=None):
    # Closed sessions can still read final state. Queued historical work is
    # protected per call and does not make every other call permanently busy.
    running = " OR ".join(
        f"EXISTS(SELECT 1 FROM {table} WHERE state='running')" for table in _JOB_STATES
    )
    return bool(
        store._db.execute(
            f"""SELECT EXISTS({_UNCLOSED_LIMIT}) OR EXISTS(
                SELECT 1 FROM web_sessions AS s WHERE s.entered_at<=:now AND s.expires_at>:now
                 AND NOT EXISTS(SELECT 1 FROM reviews WHERE call_id=s.call_id)
                 AND NOT EXISTS(SELECT 1 FROM call_closures WHERE call_id=s.call_id)
                 AND NOT EXISTS(SELECT 1 FROM call_limits WHERE call_id=s.call_id))
                OR EXISTS(SELECT 1 FROM agent_runs WHERE state='claimed')
                OR {running}""",
            {"now": time() if now is None else now},
        ).fetchone()[0]
    )


@contextmanager
def _without_lock_wait(db):
    # Maintenance yields to other connections; ordinary Store work keeps its timeout.
    timeout = db.execute("PRAGMA busy_timeout").fetchone()[0]
    db.execute("PRAGMA busy_timeout=0")
    try:
        yield
    finally:
        db.execute(f"PRAGMA busy_timeout={timeout}")


def _reclaim_free_pages(db, free_pages, allow_vacuum):
    mode = db.execute("PRAGMA auto_vacuum").fetchone()[0]
    if mode == 2:
        # Reclaim at most 256 pages per pass (about 1 MiB at 4 KiB/page).
        db.execute("PRAGMA incremental_vacuum(256)").fetchall()
        return "incremental"
    database = db.execute("PRAGMA database_list").fetchone()[2]
    if mode != 0 or not allow_vacuum or not database:
        return "skipped_threshold"
    page_size = db.execute("PRAGMA page_size").fetchone()[0]
    page_count = db.execute("PRAGMA page_count").fetchone()[0]
    # A full rewrite needs substantial savings: at least 1 MiB and 25% of pages.
    if free_pages * page_size < _MIB or free_pages * 4 < page_count:
        return "skipped_threshold"
    path = Path(database)
    try:
        # WAL may hold newer pages that have not reached the main file.
        db_size = max(path.stat().st_size, page_count * page_size)
        # Bound rewrite I/O; provision 2.5 copies plus 32 MiB for journal and overhead.
        if db_size > 256 * _MIB:
            return "skipped_db_too_large"
        if shutil.disk_usage(path.parent).free < db_size * 2.5 + 32 * _MIB:
            return "skipped_disk_space"
    except OSError:
        return "skipped_disk_space"
    db.execute("PRAGMA auto_vacuum=INCREMENTAL")
    db.execute("VACUUM")
    return "converted_incremental"


def reclaim_space(store, *, allow_vacuum=True):
    db = store._db
    try:
        with _without_lock_wait(db):
            if has_active_calls(store):
                return "skipped_active"
            free_pages = db.execute("PRAGMA freelist_count").fetchone()[0]
            state = _reclaim_free_pages(db, free_pages, allow_vacuum) if free_pages else "none"
            if db.execute("PRAGMA journal_mode").fetchone()[0] == "wal":
                db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            return state
    except sqlite3.OperationalError:
        _log.warning("stage=reclaim status=skipped_busy")
        return "skipped_busy"


def _clear_batch(store, call_id, cutoff, batch_rows):
    cleared, freed = {}, 0
    with store._transaction():
        if not store._db.execute(f"SELECT {_terminal_time('?')}<=?", (call_id, cutoff)).fetchone()[
            0
        ]:
            return cleared, freed
        for table, predicate, replacement, key in _AUDIO_TABLES:
            rows = store._db.execute(
                f"SELECT rowid,length(data) FROM {table} WHERE call_id=? AND {predicate} LIMIT ?",
                (call_id, batch_rows),
            ).fetchall()
            if rows:
                store._db.executemany(
                    f"UPDATE {table} SET data={replacement} WHERE rowid=?",
                    ((rowid,) for rowid, _ in rows),
                )
                cleared[key] = len(rows)
                freed += sum(size for _, size in rows)
                batch_rows -= len(rows)
                if not batch_rows:
                    break
    return cleared, freed


def prune_expired_audio(
    store,
    *,
    retention_days,
    now=None,
    batch_calls=10,
    batch_rows=200,
    reclaim=True,
    allow_vacuum=True,
    idle_only=False,
):
    started = monotonic()
    result = dict(
        calls_pruned=0,
        cleared_segments=0,
        cleared_speech_jobs=0,
        cleared_audio_jobs=0,
        freed_bytes=0,
        has_more=False,
        reclaim="none",
        elapsed_ms=0.0,
    )
    if retention_days <= 0:
        return result
    if batch_calls < 1 or batch_rows < 1:
        raise ValueError("Batch sizes must be positive")
    cutoff = (time() if now is None else now) - retention_days * 86400
    has_audio = " OR ".join(
        f"EXISTS(SELECT 1 FROM {table} WHERE call_id=source.call_id AND {predicate})"
        for table, predicate, _, _ in _AUDIO_TABLES
    )
    try:
        with _without_lock_wait(store._db):
            if idle_only and has_active_calls(store):
                result["reclaim"] = "skipped_active"
                return result
            candidates = store._db.execute(
                f"""SELECT call_id, {_terminal_time("source.call_id")} AS ended_at
                    FROM calls AS source WHERE ({has_audio}) AND ended_at<=?
                    ORDER BY ended_at,call_id LIMIT ?""",
                (cutoff, batch_calls + 1),
            ).fetchall()
            result["has_more"] = len(candidates) > batch_calls
            for call_id, _ in candidates[:batch_calls]:
                cleared, freed = _clear_batch(store, call_id, cutoff, batch_rows)
                if cleared:
                    result["calls_pruned"] += 1
                while cleared:
                    for key, count in cleared.items():
                        result[key] += count
                    result["freed_bytes"] += freed
                    if sum(cleared.values()) < batch_rows:
                        break
                    cleared, freed = _clear_batch(store, call_id, cutoff, batch_rows)
    except sqlite3.OperationalError:
        _log.warning("stage=prune status=skipped_busy")
        result.update(has_more=True, reclaim="skipped_busy")
    else:
        if reclaim:
            result["reclaim"] = reclaim_space(store, allow_vacuum=allow_vacuum)
    result["elapsed_ms"] = round((monotonic() - started) * 1000, 2)
    if result["calls_pruned"] or result["reclaim"] != "none":
        _log.info("stage=retention %s", result)
    return result
