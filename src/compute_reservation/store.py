"""SQLite 持久化层。

设计要点：
- 单连接 + 可重入锁串行化写入，WAL 模式允许并发读取；
- 所有多步写入都在 ``tx()`` 的 IMMEDIATE 事务中完成，要么全部提交要么全部回滚；
- 容量台账由触发器兜底，代码路径之外也不允许超卖；
- 每个预留在同一时刻至多一条开放计费段（部分唯一索引），从存储层杜绝双重计费；
- audit_log 为只增审计流，支撑履约轨迹核对与重启恢复。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .clock import Clock, SystemClock
from .models import canonical_json

SCHEMA_VERSION = "1"

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    inventory_ref TEXT UNIQUE,
    total_cards INTEGER NOT NULL CHECK (total_cards > 0),
    valid_from REAL NOT NULL,
    valid_until REAL NOT NULL CHECK (valid_until > valid_from),
    fault_domain TEXT NOT NULL,
    energy_tier TEXT NOT NULL,
    capabilities TEXT NOT NULL,
    price_per_card_hour REAL NOT NULL CHECK (price_per_card_hour > 0),
    state TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS capacity_ledger (
    batch_id TEXT PRIMARY KEY REFERENCES batches(batch_id),
    allocated_cards INTEGER NOT NULL CHECK (allocated_cards >= 0)
);

-- 超卖硬约束：台账分配量永不超过批次总容量。
CREATE TRIGGER IF NOT EXISTS no_oversell BEFORE UPDATE OF allocated_cards ON capacity_ledger
BEGIN
    SELECT RAISE(ABORT, 'capacity_exceeded')
    WHERE NEW.allocated_cards > (SELECT total_cards FROM batches WHERE batch_id = NEW.batch_id);
END;

CREATE TABLE IF NOT EXISTS idempotency_keys (
    idem_key TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (idem_key, tenant_id)
);

CREATE TABLE IF NOT EXISTS quotes (
    quote_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    cards INTEGER NOT NULL CHECK (cards > 0),
    start_at REAL NOT NULL,
    duration_seconds REAL NOT NULL CHECK (duration_seconds > 0),
    required_caps TEXT NOT NULL,
    breakdown TEXT NOT NULL,
    inputs_hash TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    expires_at REAL NOT NULL,
    state TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    quote_id TEXT REFERENCES quotes(quote_id),
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    node_id TEXT NOT NULL,
    cards INTEGER NOT NULL CHECK (cards > 0),
    start_at REAL NOT NULL,
    end_at REAL NOT NULL CHECK (end_at > start_at),
    base_duration REAL NOT NULL,
    extension_seconds REAL NOT NULL,
    locked_unit_price REAL NOT NULL,
    required_caps TEXT NOT NULL,
    state TEXT NOT NULL,
    auto_renew INTEGER NOT NULL,
    max_renewals INTEGER NOT NULL,
    renewals_used INTEGER NOT NULL DEFAULT 0,
    migratable INTEGER NOT NULL,
    interruptible INTEGER NOT NULL,
    shrink_on_partial INTEGER NOT NULL,
    tenant_priority INTEGER NOT NULL,
    idem_key TEXT,
    restored_from TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS reservations_idem
    ON reservations(tenant_id, idem_key) WHERE idem_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS task_groups (
    group_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    reservation_id TEXT NOT NULL REFERENCES reservations(reservation_id),
    state TEXT NOT NULL,
    detail TEXT,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL REFERENCES task_groups(group_id),
    tenant_id TEXT NOT NULL,
    name TEXT NOT NULL,
    cards INTEGER NOT NULL CHECK (cards > 0),
    state TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    node_id TEXT,
    started_at REAL,
    completed_at REAL,
    UNIQUE (group_id, name)
);

-- 计费段：每个预留同一时刻至多一条 end_at 为 NULL 的开放段。
CREATE TABLE IF NOT EXISTS billing_segments (
    segment_id TEXT PRIMARY KEY,
    reservation_id TEXT NOT NULL REFERENCES reservations(reservation_id),
    node_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    cards INTEGER NOT NULL CHECK (cards > 0),
    unit_price REAL NOT NULL,
    start_at REAL NOT NULL,
    end_at REAL,
    close_reason TEXT,
    open_reason TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_segment
    ON billing_segments(reservation_id) WHERE end_at IS NULL;

CREATE TABLE IF NOT EXISTS migrations (
    migration_id TEXT PRIMARY KEY,
    reservation_id TEXT NOT NULL REFERENCES reservations(reservation_id),
    from_node TEXT NOT NULL,
    to_node TEXT NOT NULL,
    from_batch TEXT NOT NULL,
    to_batch TEXT NOT NULL,
    reason TEXT NOT NULL,
    mode TEXT NOT NULL,
    state TEXT NOT NULL,
    planned_at REAL NOT NULL,
    cutover_at REAL,
    detail TEXT
);

CREATE TABLE IF NOT EXISTS consumption_events (
    event_id TEXT PRIMARY KEY,
    reservation_id TEXT NOT NULL,
    task_id TEXT,
    node_id TEXT NOT NULL,
    cards INTEGER NOT NULL CHECK (cards > 0),
    usage_start REAL NOT NULL,
    usage_end REAL NOT NULL CHECK (usage_end > usage_start),
    energy_kwh REAL NOT NULL DEFAULT 0,
    payload_hash TEXT NOT NULL,
    received_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS bills (
    bill_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    reservation_id TEXT NOT NULL,
    period_start REAL NOT NULL,
    period_end REAL NOT NULL,
    kind TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    lines TEXT NOT NULL,
    usage TEXT NOT NULL,
    warnings TEXT NOT NULL,
    total REAL NOT NULL,
    currency TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    supersedes TEXT,
    created_at REAL NOT NULL,
    UNIQUE (reservation_id, period_start, period_end, kind)
);

CREATE TABLE IF NOT EXISTS preemptions (
    preemption_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at REAL NOT NULL,
    state TEXT NOT NULL,
    items TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    actor TEXT NOT NULL,
    type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    tenant_id TEXT,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS audit_tenant ON audit_log(tenant_id, seq);
"""


class Store:
    """SQLite 存储门面，提供事务、查询与审计写入。"""

    def __init__(self, path: str | Path = ":memory:", clock: Clock | None = None) -> None:
        self.clock = clock or SystemClock()
        self.path = str(path)
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
                (SCHEMA_VERSION,),
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """IMMEDIATE 事务：进入即取得写锁，保证读写一致性。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def one(self, sql: str, params: tuple = ()) -> dict | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def audit(
        self,
        conn: sqlite3.Connection,
        *,
        actor: str,
        type_: str,
        entity_id: str,
        tenant_id: str | None = None,
        payload: object = None,
    ) -> None:
        """在调用方事务内追加一条审计记录。"""
        conn.execute(
            "INSERT INTO audit_log(ts, actor, type, entity_id, tenant_id, payload) VALUES(?,?,?,?,?,?)",
            (self.clock.now(), actor, type_, entity_id, tenant_id, canonical_json(payload or {})),
        )


def decode_json_columns(row: dict, columns: tuple[str, ...]) -> dict:
    """把行中的 JSON 文本列解码为对象，便于服务层与 API 使用。"""
    for column in columns:
        if column in row and isinstance(row[column], str):
            row[column] = json.loads(row[column])
    return row
