"""SQLite 持久化层：模式、事务与单调节点编码。

设计要点：
- 单库文件即全部状态，进程重启后凭库恢复过期回收与未决迁移；
- 所有写操作走 ``tx()``（BEGIN IMMEDIATE），多线程/多进程下准入检查原子生效；
- 业务编码（如 RSV-000001）由 sequences 表在事务内单调分配，绝不复用。
"""

from __future__ import annotations

from contextlib import contextmanager
import sqlite3
import threading
from typing import Any, Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS sequences (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    node_code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    fault_domain TEXT NOT NULL,
    energy_level TEXT NOT NULL,
    state TEXT NOT NULL,
    capabilities TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS capacity_batches (
    batch_code TEXT PRIMARY KEY,
    node_code TEXT NOT NULL REFERENCES nodes(node_code),
    resource_type TEXT NOT NULL,
    total_units INTEGER NOT NULL CHECK (total_units >= 0),
    allocated_units INTEGER NOT NULL DEFAULT 0 CHECK (allocated_units >= 0),
    available_from TEXT NOT NULL,
    available_until TEXT NOT NULL,
    fault_domain TEXT NOT NULL,
    energy_level TEXT NOT NULL,
    capabilities TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL,
    CHECK (allocated_units <= total_units)
);
CREATE INDEX IF NOT EXISTS idx_batches_window ON capacity_batches(state, resource_type, available_until);

CREATE TABLE IF NOT EXISTS quotes (
    quote_code TEXT PRIMARY KEY,
    tenant_code TEXT NOT NULL,
    batch_code TEXT NOT NULL REFERENCES capacity_batches(batch_code),
    units INTEGER NOT NULL CHECK (units > 0),
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    billing_mode TEXT NOT NULL,
    unit_price REAL NOT NULL,
    amount_cents INTEGER NOT NULL,
    snapshot TEXT NOT NULL,
    snapshot_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS reservations (
    reservation_code TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_fingerprint TEXT NOT NULL,
    quote_code TEXT REFERENCES quotes(quote_code),
    tenant_code TEXT NOT NULL,
    batch_code TEXT NOT NULL REFERENCES capacity_batches(batch_code),
    node_code TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    units INTEGER NOT NULL CHECK (units > 0),
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    billing_mode TEXT NOT NULL,
    unit_price REAL NOT NULL,
    state TEXT NOT NULL,
    contract TEXT NOT NULL,
    settled_upto TEXT,
    activated_at TEXT NOT NULL,
    closed_at TEXT,
    close_reason TEXT,
    preempted_at TEXT,
    recovery_rank INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reservations_batch ON reservations(batch_code, state);
CREATE INDEX IF NOT EXISTS idx_reservations_expiry ON reservations(state, end_at);
CREATE INDEX IF NOT EXISTS idx_reservations_tenant ON reservations(tenant_code);

-- 预留的占用区间：每次迁移切出一段，区间连续且不重叠，是"不重复计费"的根基
CREATE TABLE IF NOT EXISTS reservation_segments (
    segment_code TEXT PRIMARY KEY,
    reservation_code TEXT NOT NULL REFERENCES reservations(reservation_code),
    batch_code TEXT NOT NULL,
    node_code TEXT NOT NULL,
    seq INTEGER NOT NULL,
    units INTEGER NOT NULL CHECK (units > 0),
    start_at TEXT NOT NULL,
    end_at TEXT,
    UNIQUE (reservation_code, seq)
);

CREATE TABLE IF NOT EXISTS task_groups (
    group_code TEXT PRIMARY KEY,
    tenant_code TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    task_code TEXT PRIMARY KEY,
    group_code TEXT NOT NULL REFERENCES task_groups(group_code),
    tenant_code TEXT NOT NULL,
    reservation_code TEXT NOT NULL REFERENCES reservations(reservation_code),
    name TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    required_units INTEGER NOT NULL CHECK (required_units > 0),
    state TEXT NOT NULL,
    submitted_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    progress REAL NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_group ON tasks(group_code);
CREATE INDEX IF NOT EXISTS idx_tasks_reservation ON tasks(reservation_code);

CREATE TABLE IF NOT EXISTS migrations (
    migration_code TEXT PRIMARY KEY,
    reservation_code TEXT NOT NULL REFERENCES reservations(reservation_code),
    from_batch TEXT NOT NULL,
    to_batch TEXT NOT NULL,
    units INTEGER NOT NULL,
    reason TEXT NOT NULL,
    interruptible INTEGER NOT NULL,
    state TEXT NOT NULL,
    planned_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_migrations_state ON migrations(state);

CREATE TABLE IF NOT EXISTS preemptions (
    preemption_code TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    trigger_batch TEXT,
    state TEXT NOT NULL,
    plan TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS consumption_events (
    event_code TEXT PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    reservation_code TEXT NOT NULL,
    task_code TEXT,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    unit_hours REAL,
    payload TEXT NOT NULL,
    received_at TEXT NOT NULL,
    seq INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_reservation ON consumption_events(reservation_code, occurred_at);

CREATE TABLE IF NOT EXISTS ledger_entries (
    entry_code TEXT PRIMARY KEY,
    bill_code TEXT,
    reservation_code TEXT NOT NULL,
    task_code TEXT,
    entry_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ledger_reservation ON ledger_entries(reservation_code);
CREATE INDEX IF NOT EXISTS idx_ledger_unbilled ON ledger_entries(bill_code);

CREATE TABLE IF NOT EXISTS bills (
    bill_code TEXT PRIMARY KEY,
    tenant_code TEXT NOT NULL,
    reservation_code TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    entry_count INTEGER NOT NULL,
    state TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    UNIQUE (reservation_code, period_start, period_end)
);
CREATE INDEX IF NOT EXISTS idx_bills_tenant ON bills(tenant_code);

CREATE TABLE IF NOT EXISTS rate_cards (
    rate_code TEXT PRIMARY KEY,
    resource_type TEXT NOT NULL,
    energy_level TEXT NOT NULL,
    billing_mode TEXT NOT NULL,
    unit_price REAL NOT NULL,
    effective_from TEXT NOT NULL,
    UNIQUE (resource_type, energy_level, billing_mode, effective_from)
);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_code TEXT PRIMARY KEY,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_code TEXT NOT NULL,
    tenant_code TEXT,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_resource ON audit_log(resource_type, resource_code);
CREATE INDEX IF NOT EXISTS idx_audit_tenant ON audit_log(tenant_code);
"""

KEY_COLUMNS = {
    "nodes": "node_code",
    "capacity_batches": "batch_code",
    "quotes": "quote_code",
    "reservations": "reservation_code",
    "reservation_segments": "segment_code",
    "task_groups": "group_code",
    "tasks": "task_code",
    "migrations": "migration_code",
    "preemptions": "preemption_code",
    "consumption_events": "event_code",
    "ledger_entries": "entry_code",
    "bills": "bill_code",
    "rate_cards": "rate_code",
    "audit_log": "audit_code",
}

CODE_PREFIXES = {
    "nodes": "NB",
    "capacity_batches": "CB",
    "quotes": "QT",
    "reservations": "RSV",
    "reservation_segments": "SEG",
    "task_groups": "TG",
    "tasks": "TSK",
    "migrations": "MIG",
    "preemptions": "PRE",
    "consumption_events": "EVT",
    "ledger_entries": "LE",
    "bills": "BILL",
    "rate_cards": "RC",
    "audit_log": "AUD",
}


class Store:
    """SQLite 存储门面：事务、编码分配与行读取。"""

    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """独占写事务：同一时刻只有一个事务，准入检查现在此串行化。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            yield self._conn

    def next_code(self, conn: sqlite3.Connection, table: str) -> str:
        """在事务内分配单调业务编码，如 RSV-000007。"""
        prefix = CODE_PREFIXES[table]
        conn.execute(
            "INSERT INTO sequences(name, value) VALUES(?, 1) "
            "ON CONFLICT(name) DO UPDATE SET value = value + 1",
            (table,),
        )
        value = conn.execute("SELECT value FROM sequences WHERE name = ?", (table,)).fetchone()["value"]
        return f"{prefix}-{value:06d}"

    @staticmethod
    def get(conn: sqlite3.Connection, table: str, code: str) -> dict[str, Any] | None:
        key = KEY_COLUMNS[table]
        row = conn.execute(f"SELECT * FROM {table} WHERE {key} = ?", (code,)).fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    def query(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]

    @staticmethod
    def one(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> dict[str, Any] | None:
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row is not None else None
