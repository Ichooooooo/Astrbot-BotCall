"""Shape owned synthetic historical databases before testing real migrations."""


def remove_series_columns(db):
    db.execute("DROP TABLE IF EXISTS barge_candidates")
    present = {r[1] for r in db.execute("PRAGMA table_info(speech_segments)")}
    for name in ("play_started_at", "playback_position_ms", "end_reason"):
        if name in present:
            db.execute("ALTER TABLE speech_segments DROP COLUMN " + name)
    columns = (
        "series_id",
        "series_index",
        "series_final",
        "series_limit",
        "series_end_index",
        "series_disposition",
    )
    if "series_id" not in {r[1] for r in db.execute("PRAGMA table_info(audio_jobs)")}:
        return
    # This helper must never reshape actual series material.
    assert not any(
        any(value is not None for value in row)
        for row in db.execute("SELECT " + ",".join(columns) + " FROM audio_jobs")
    )
    for name in ("checked_series_insert", "immutable_series_identity", "checked_series_progress"):
        db.execute("DROP TRIGGER " + name)
    db.execute("DROP INDEX series_member")
    for column in reversed(columns):
        db.execute("ALTER TABLE audio_jobs DROP COLUMN " + column)


def remove_retry_schema(db):
    """A simulated pre-23 database must not contain additive retry objects."""
    for kind, name in db.execute(
        "SELECT type,name FROM sqlite_master WHERE name LIKE '%retry%' ORDER BY type DESC"
    ).fetchall():
        if kind in {"trigger", "table"}:
            db.execute(f'DROP {kind} "{name}"')
