"""Generate baseline using exactly the API projection, opening a copy read-only."""

import argparse
from datetime import date
import json
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bot_call.benchmark import STAGE_DEFS, baseline, snapshot


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("database", type=Path)
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--recent", type=int)
    group.add_argument("--call-id", action="append")
    args = p.parse_args()
    if args.recent is not None and args.recent < 1:
        p.error("--recent must be positive")
    with sqlite3.connect(args.database.resolve().as_uri() + "?mode=ro", uri=True) as db:
        data = snapshot(db, limit=args.recent or 30, call_ids=args.call_id)
    # Keep the shipped model identifiers/thinking/streaming metadata; only samples change.
    b = baseline()
    b.update(
        label="标准配置",
        placeholder=False,
        measured_at=date.today().isoformat(),
        stages=data["summary"]["stages"],
    )
    missing = {r["key"] for r in STAGE_DEFS} - b["stages"].keys()
    if missing:
        p.error("样本缺少阶段，不能作为完整基线: " + ",".join(sorted(missing)))
    print(json.dumps(b, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
