"""Local SQLite query/allocation cost; no Provider or end-to-end latency claim."""

import json
from pathlib import Path
from statistics import median
import sys
from time import perf_counter

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parent)]
from bot_call.store import Store
from test_astrbot_context import binding

with Store(":memory:") as store:
    store.create_call(binding("cost"))
    for n in range(120):
        store.receive_audio("cost", f"u{n}", b"synthetic", "audio/webm")
        # Public append path with real ordinary audio identities.
        store.claim_next_audio("cost")
        store.complete_audio("cost", f"u{n}", "synthetic voice")
    rows = []
    for label, fn in (
        ("projection", lambda: store.effective_transcript("cost")),
        ("roots", lambda: store.series_roots("cost")),
    ):
        counts = []
        times = []
        for _ in range(30):
            sql = []
            store._db.set_trace_callback(sql.append)
            start = perf_counter()
            fn()
            times.append((perf_counter() - start) * 1000)
            counts.append(len(sql))
            store._db.set_trace_callback(None)
        rows.append(
            dict(
                operation=label,
                ordinary_jobs=120,
                median_ms=round(median(times), 3),
                queries=counts[-1],
            )
        )
    print(json.dumps(rows))
