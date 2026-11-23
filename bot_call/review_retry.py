"""Explicit, additive attempts over the original frozen input.

Attempt identity is (attempt_id, source_key); old work/manifests are never reset.
The existing planner, work engine and publication state machine are shared.
"""

from dataclasses import asdict, replace
import hashlib
import json
from time import time

from .review_policy import ReviewManifest
from .store import Store, ReviewWork, ReviewWorkNode, StateConflict


SCHEMA = (
    """CREATE TABLE review_retry_attempts (
        attempt_id INTEGER PRIMARY KEY, call_id TEXT NOT NULL REFERENCES reviews(call_id),
        created_at REAL NOT NULL, original_model TEXT, original_reason TEXT NOT NULL,
        manifest_json TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
        context_only INTEGER NOT NULL CHECK(context_only IN (0,1)),
        state TEXT NOT NULL CHECK(state IN ('pending','running','succeeded','failed','uncertain','cancelled')),
        error_code TEXT, body TEXT, plan_json TEXT, fingerprint TEXT,
        completed_at REAL, send_requested_at REAL,
        input_token_budget INTEGER, map_count INTEGER, blocked_code TEXT,
        CHECK((state='succeeded' AND body IS NOT NULL) OR (state!='succeeded' AND body IS NULL))
    )""",
    """CREATE TABLE review_retry_nodes (
        attempt_id INTEGER NOT NULL REFERENCES review_retry_attempts(attempt_id),
        source_key TEXT NOT NULL, node_json TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending','running','succeeded','failed','uncertain','cancelled')),
        body TEXT, result_sha256 TEXT, error_code TEXT,
        PRIMARY KEY(attempt_id,source_key),
        CHECK((state='succeeded' AND body IS NOT NULL AND result_sha256 IS NOT NULL AND error_code IS NULL)
           OR (state!='succeeded' AND body IS NULL AND result_sha256 IS NULL)),
        CHECK((state IN ('pending','running','succeeded') AND error_code IS NULL)
           OR (state IN ('failed','uncertain','cancelled') AND error_code IS NOT NULL))
    )""",
    """CREATE TABLE review_retry_stages (
        attempt_id INTEGER NOT NULL REFERENCES review_retry_attempts(attempt_id),
        level INTEGER NOT NULL, kind TEXT NOT NULL, frontier_json TEXT NOT NULL,
        PRIMARY KEY(attempt_id,level)
    )""",
    """CREATE TRIGGER immutable_retry_identity BEFORE UPDATE OF
        attempt_id,call_id,created_at,original_model,original_reason,manifest_json,manifest_sha256,context_only
        ON review_retry_attempts BEGIN SELECT RAISE(ABORT,'retry identity is immutable'); END""",
    """CREATE TRIGGER immutable_retry_plan BEFORE UPDATE OF plan_json,fingerprint,input_token_budget,map_count
        ON review_retry_attempts WHEN OLD.plan_json IS NOT NULL
        BEGIN SELECT RAISE(ABORT,'retry plan is immutable'); END""",
    """CREATE TRIGGER retry_progress BEFORE UPDATE OF state,body,error_code ON review_retry_attempts
        WHEN NOT (OLD.state='pending' AND NEW.state='running'
          OR OLD.state='running' AND NEW.state IN ('succeeded','failed','uncertain','cancelled'))
        BEGIN SELECT RAISE(ABORT,'retry outcome is terminal'); END""",
    """CREATE TRIGGER immutable_retry_node BEFORE UPDATE OF attempt_id,source_key,node_json ON review_retry_nodes
        BEGIN SELECT RAISE(ABORT,'retry request is immutable'); END""",
    """CREATE TRIGGER retry_node_progress BEFORE UPDATE OF state,body,result_sha256,error_code ON review_retry_nodes
        WHEN NOT (OLD.state='pending' AND NEW.state='running'
          OR OLD.state='running' AND NEW.state IN ('succeeded','failed','uncertain','cancelled'))
        BEGIN SELECT RAISE(ABORT,'retry node outcome is terminal'); END""",
    """CREATE TRIGGER immutable_retry_stage BEFORE UPDATE ON review_retry_stages
        BEGIN SELECT RAISE(ABORT,'retry stage is immutable'); END""",
) + tuple(
    f"""CREATE TRIGGER retained_{table} BEFORE DELETE ON {table}
        BEGIN SELECT RAISE(ABORT,'retry evidence is retained'); END"""
    for table in ("review_retry_attempts", "review_retry_nodes", "review_retry_stages")
)


def latest(store, call_id):
    return store._db.execute(
        "SELECT * FROM review_retry_attempts WHERE call_id=? ORDER BY attempt_id DESC LIMIT 1",
        (call_id,),
    ).fetchone()


class AttemptStore(Store):
    """Borrow the worker connection; only review work is scoped to this attempt.

    Publication uses the original call's stable keys, so successful/unknown
    deliveries remain terminal even across retries and plugin reloads.
    """

    def __init__(self, store, attempt_id):
        self._db = store._db
        self.attempt_id = attempt_id
        self._active_agents = store._active_agents

    def row(self):
        return self._db.execute(
            "SELECT * FROM review_retry_attempts WHERE attempt_id=?", (self.attempt_id,)
        ).fetchone()

    def get_review(self, call_id):
        row = self.row()
        if row["call_id"] != call_id:
            raise StateConflict("Retry target mismatch")
        return replace(
            Store.get_review(self, call_id),
            state=row["state"],
            body=row["body"],
            error_code=row["error_code"],
        )

    def get_review_manifest(self, call_id):
        row = self.row()
        if row["call_id"] != call_id:
            raise StateConflict("Retry target mismatch")
        return ReviewManifest(row["manifest_json"], row["manifest_sha256"])

    def get_review_work(self, call_id):
        row = self.row()
        if row["plan_json"] is None:
            return None
        stage = self._db.execute(
            "SELECT * FROM review_retry_stages WHERE attempt_id=? ORDER BY level DESC LIMIT 1",
            (self.attempt_id,),
        ).fetchone()
        return ReviewWork(
            call_id,
            row["plan_json"],
            row["fingerprint"],
            row["input_token_budget"],
            row["map_count"],
            row["blocked_code"],
            stage["level"],
            stage["kind"],
            tuple(
                self.get_review_work_node(call_id, k) for k in json.loads(stage["frontier_json"])
            ),
            self.get_review(call_id),
            self.get_review_manifest(call_id),
        )

    def get_review_work_node(self, call_id, source_key):
        row = self._db.execute(
            "SELECT * FROM review_retry_nodes WHERE attempt_id=? AND source_key=?",
            (self.attempt_id, source_key),
        ).fetchone()
        data = json.loads(row["node_json"])
        data["child_keys"] = tuple(data["child_keys"])
        data.update({k: row[k] for k in ("state", "body", "result_sha256", "error_code")})
        if data["call_id"] != call_id:
            raise StateConflict("Retry node mismatch")
        return ReviewWorkNode(**data)

    def _insert_review_work_node(self, node):
        self._db.execute(
            "INSERT INTO review_retry_nodes VALUES (?,?,?,'pending',NULL,NULL,NULL)",
            (self.attempt_id, node.source_key, json.dumps(asdict(node), ensure_ascii=False)),
        )

    def install_review_work(self, plan, *, manifest=None):
        from .review_work import _validate_plan

        with self._transaction():
            existing = self.get_review_work(plan.call_id)
            if existing:
                return existing
            _validate_plan(self.get_review(plan.call_id), plan)
            if manifest != self.get_review_manifest(plan.call_id):
                raise StateConflict("Retry policy mismatch")
            self._db.execute(
                """UPDATE review_retry_attempts SET state='running',plan_json=?,fingerprint=?,
                input_token_budget=?,map_count=? WHERE attempt_id=?""",
                (
                    json.dumps(asdict(plan), ensure_ascii=False),
                    plan.fingerprint,
                    plan.input_token_budget,
                    len(plan.chunks),
                    self.attempt_id,
                ),
            )
            keys = []
            for c in plan.chunks:
                n = ReviewWorkNode(
                    plan.call_id,
                    c.source_key,
                    0,
                    c.ordinal,
                    "map",
                    c.ordinal - 1,
                    c.ordinal,
                    (),
                    c.request_json,
                    c.token_count,
                    "pending",
                    None,
                    None,
                    None,
                )
                self._insert_review_work_node(n)
                keys.append(n.source_key)
            self._db.execute(
                "INSERT INTO review_retry_stages VALUES (?,0,'map',?)",
                (self.attempt_id, json.dumps(keys)),
            )
        return self.get_review_work(plan.call_id)

    def claim_review_work_node(self, call_id, source_key, *, manifest_sha256=None):
        with self._transaction():
            self._check_review_manifest(call_id, manifest_sha256)
            work = self.get_review_work(call_id)
            candidate = next((n for n in work.frontier if n.state != "succeeded"), None)
            claimed = (
                work.state == "pending"
                and candidate is not None
                and candidate.source_key == source_key
                and candidate.state == "pending"
            )
            if claimed:
                self._db.execute(
                    "UPDATE review_retry_nodes SET state='running' WHERE attempt_id=? AND source_key=?",
                    (self.attempt_id, source_key),
                )
        return self.get_review_work_node(call_id, source_key), claimed

    def finish_review_work_node(
        self, call_id, source_key, *, state, body=None, manifest_sha256=None, error_code=None
    ):
        from .failures import CATEGORIES

        if state not in {"succeeded", "failed", "uncertain", "cancelled"}:
            raise StateConflict("Invalid retry outcome")
        if state == "succeeded":
            if type(body) is not str or not body.strip():
                raise StateConflict("Invalid retry result")
            digest, code = hashlib.sha256(body.encode()).hexdigest(), None
        else:
            if body is not None or (error_code is not None and error_code not in CATEGORIES):
                raise StateConflict("Invalid retry failure")
            digest = None
            code = (
                error_code
                or {
                    "failed": "not_applied_or_invalid_result",
                    "uncertain": "result_uncertain",
                    "cancelled": "result_cancelled",
                }[state]
            )
        with self._transaction():
            self._check_review_manifest(call_id, manifest_sha256)
            node = self.get_review_work_node(call_id, source_key)
            if node.state == "running":
                self._db.execute(
                    """UPDATE review_retry_nodes SET state=?,body=?,result_sha256=?,error_code=?
                    WHERE attempt_id=? AND source_key=?""",
                    (state, body, digest, code, self.attempt_id, source_key),
                )
                if state != "succeeded":
                    self._db.execute(
                        "UPDATE review_retry_attempts SET state=?,error_code=? WHERE attempt_id=?",
                        (state, code, self.attempt_id),
                    )
            elif (node.state, node.body, node.error_code) != (state, body, code):
                raise StateConflict("Retry outcome is terminal")
        return self.get_review_work_node(call_id, source_key)

    def complete_review(self, call_id, body, *, manifest_sha256=None):
        with self._transaction():
            self._check_review_manifest(call_id, manifest_sha256)
            work = self.get_review_work(call_id)
            if self.get_review(call_id).state == "succeeded":
                if self.get_review(call_id).body != body:
                    raise StateConflict("Retry result mismatch")
                return self.get_review(call_id)
            if (
                work.kind != "final"
                or len(work.frontier) != 1
                or work.frontier[0].state != "succeeded"
                or work.frontier[0].body != body
            ):
                raise StateConflict("Retry final is incomplete")
            self._db.execute(
                "UPDATE review_retry_attempts SET state='succeeded',body=? WHERE attempt_id=?",
                (body, self.attempt_id),
            )
        return self.get_review(call_id)

    def block_review_work(self, call_id, *, manifest_sha256=None):
        self._check_review_manifest(call_id, manifest_sha256)
        with self._transaction():
            self._db.execute(
                "UPDATE review_retry_attempts SET blocked_code='merge_budget' WHERE attempt_id=?",
                (self.attempt_id,),
            )
        return self.get_review_work(call_id)

    def add_review_work_stage(
        self,
        call_id: str,
        previous_level: int,
        kind: str,
        nodes: tuple[ReviewWorkNode, ...],
        frontier_keys: tuple[str, ...],
        *,
        manifest_sha256: str | None = None,
    ) -> ReviewWork:
        with self._transaction():
            self._check_review_manifest(call_id, manifest_sha256)
            work = self.get_review_work(call_id)
            if work.level != previous_level:
                return work  # Another connection already committed the fixed next stage.
            if (
                work.state != "pending"
                or work.kind == "final"
                or kind not in {"merge", "final"}
                or not work.frontier
                or any(n.state != "succeeded" for n in work.frontier)
            ):
                raise StateConflict("Previous review work stage is not complete")
            old = {n.source_key: n for n in work.frontier}
            new = {n.source_key: n for n in nodes}
            if len(new) != len(nodes) or set(new) & set(old) or not nodes:
                raise StateConflict("Invalid review work stage")
            consumed, ordinal = [], 0
            for key in frontier_keys:
                if key in old:
                    if kind == "final":
                        raise StateConflict("Final stage must generate a final review")
                    consumed.append(key)
                    continue
                node = new.get(key)
                ordinal += 1
                if (
                    node is None
                    or node.call_id != call_id
                    or node.level != work.level + 1
                    or node.ordinal != ordinal
                    or node.kind != kind
                    or node.state != "pending"
                    or node.body is not None
                    or node.result_sha256 is not None
                    or node.error_code is not None
                    or type(node.token_count) is not int
                    or not 0 <= node.token_count <= work.input_token_budget
                    or not node.child_keys
                    or any(k not in old for k in node.child_keys)
                    or (kind == "merge" and len(node.child_keys) < 2)
                    or node.first_map != old[node.child_keys[0]].first_map
                    or node.stop_map != old[node.child_keys[-1]].stop_map
                ):
                    raise StateConflict("Invalid review work node")
                from .review_work import _reduction_request

                expected_key, expected_request = _reduction_request(
                    work,
                    kind,
                    tuple(old[k] for k in node.child_keys),
                    node.ordinal,
                )
                if (node.source_key, node.request_json) != (expected_key, expected_request):
                    raise StateConflict("Review work request does not match its sources")
                consumed.extend(node.child_keys)
            if (
                tuple(consumed) != tuple(old)
                or ordinal != len(nodes)
                or (kind == "merge" and len(frontier_keys) >= len(old))
                or (kind == "final" and (len(nodes) != 1 or len(frontier_keys) != 1))
            ):
                raise StateConflict("Review work stage must preserve ordered complete coverage")
            for node in nodes:
                self._insert_review_work_node(node)
            self._db.execute(
                "INSERT INTO review_retry_stages VALUES (?,?,?,?)",
                (self.attempt_id, work.level + 1, kind, json.dumps(frontier_keys)),
            )
        return self.get_review_work(call_id)


DETAILS = {
    "balance": "额度或余额不足",
    "auth": "鉴权或访问权限失败",
    "model": "模型不存在或不可用",
    "rejected": "请求被服务拒绝",
    "config": "模型配置无效",
    "not_applied_or_invalid_result": "未执行或返回结果无效",
}


def status(store, call_id, *, allowed=False, running=False):
    """Metadata only; failure evidence takes precedence over instance scope."""
    row = latest(store, call_id)
    view = AttemptStore(store, row["attempt_id"]) if row else store
    review, work = view.get_review(call_id), view.get_review_work(call_id)
    manifest = view.get_review_manifest(call_id) or (
        store.get_call_policy(call_id).review_manifest if store.get_call_policy(call_id) else None
    )
    model = json.loads(manifest.canonical_json)["model"] if manifest else None
    states = {n.state for n in work.frontier} if work else set()
    if review.state == "failed" or "failed" in states:
        code = review.error_code or next(n.error_code for n in work.frontier if n.state == "failed")
        reason, detail = "model_failed", "回顾模型调用失败：" + DETAILS.get(code, "生成失败")
    elif (
        review.state in {"uncertain", "cancelled"}
        or states & {"uncertain", "cancelled"}
        or (row and not running and review.state in {"pending", "running"})
    ):
        reason, detail = "outcome_uncertain", "回顾生成结果未知；默认保留，显式重试可能重复计费"
    elif running or "running" in states:
        reason, detail = (
            "work_running",
            "回顾正在处理" if running else "上次回顾处理已中断，结果未知",
        )
    elif review.state == "succeeded":
        reason, detail = "complete", "回顾已生成"
    elif not allowed:
        reason, detail = "scope_required", "历史保留（未自动处理）"
    else:
        reason, detail = "unresolved", "等待处理"
    retryable = bool(
        manifest
        and manifest.is_native
        and reason in {"model_failed", "outcome_uncertain", "work_running"}
        and not running
        and not (work and work.blocked_code)
    )
    if row and review.state == "succeeded":
        reason, detail, retryable = "complete", "已通过重试补做", False
        if (
            row["context_only"]
            and row["completed_at"] is not None
            and row["send_requested_at"] is None
        ):
            detail = "已补做，按选择只写入聊天记忆，未发 QQ"
    return dict(
        state=review.state,
        reason=reason,
        detail=detail,
        model=model,
        retryable=retryable,
        work_state=work.state if work else None,
        at=review.ended_at,
    )


def create_attempt(store, call_id, manifest, *, context_only):
    info = status(store, call_id)
    if not info["retryable"] or info["state"] == "succeeded":
        raise StateConflict("Review is not retryable")
    with store._transaction():
        # Publication identity must never be rebound to a different review body.
        if store._db.execute("SELECT 1 FROM publications WHERE call_id=?", (call_id,)).fetchone():
            raise StateConflict("Existing publication must be resumed")
        cursor = store._db.execute(
            """INSERT INTO review_retry_attempts
            (call_id,created_at,original_model,original_reason,manifest_json,manifest_sha256,
             context_only,state) VALUES (?,?,?,?,?,?,?,'pending')""",
            (
                call_id,
                time(),
                info["model"],
                info["reason"],
                manifest.canonical_json,
                manifest.fingerprint,
                int(context_only),
            ),
        )
    return AttemptStore(store, cursor.lastrowid)
