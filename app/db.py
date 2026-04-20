import sqlite3
from pathlib import Path

from flask import current_app, g


SCHEMA = """
CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_code TEXT UNIQUE,
    full_name TEXT NOT NULL,
    sex TEXT,
    position TEXT,
    province TEXT,
    city_municipality TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS employee_outputs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id INTEGER NOT NULL,
    work_date TEXT NOT NULL,
    category TEXT NOT NULL,
    activity_type TEXT,
    sex TEXT,
    source_key TEXT,
    quantity INTEGER NOT NULL DEFAULT 0,
    remarks TEXT,
    province TEXT,
    city_municipality TEXT,
    source_ref TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_id) REFERENCES employees (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS signatories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_type TEXT NOT NULL DEFAULT 'general',
    role TEXT NOT NULL,
    name TEXT NOT NULL,
    position TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_date TEXT NOT NULL,
    city_municipality TEXT NOT NULL,
    assigned_rko_employee_id INTEGER,
    assigned_ra_employee_id INTEGER,
    event_place_activity TEXT,
    status TEXT NOT NULL DEFAULT 'Pending',
    vehicle TEXT NOT NULL DEFAULT 'PSA',
    needed_rko_count INTEGER NOT NULL DEFAULT 1,
    needed_ra_count INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (assigned_rko_employee_id) REFERENCES employees (id) ON DELETE SET NULL,
    FOREIGN KEY (assigned_ra_employee_id) REFERENCES employees (id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS schedule_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_id INTEGER NOT NULL,
    employee_id INTEGER NOT NULL,
    role TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (schedule_id) REFERENCES schedules (id) ON DELETE CASCADE,
    FOREIGN KEY (employee_id) REFERENCES employees (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_outputs_employee_date
ON employee_outputs (employee_id, work_date);

CREATE INDEX IF NOT EXISTS idx_outputs_city_date
ON employee_outputs (city_municipality, work_date);

CREATE INDEX IF NOT EXISTS idx_schedules_date
ON schedules (schedule_date);

CREATE INDEX IF NOT EXISTS idx_schedule_assignments_date_role
ON schedule_assignments (schedule_id, role);
"""


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        db_path = Path(current_app.config["DATABASE"])
        g.db = sqlite3.connect(db_path)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


def close_db(_error=None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    db = get_db()
    db.executescript(SCHEMA)
    _ensure_column(db, "employee_outputs", "sex", "TEXT")
    _ensure_column(db, "employee_outputs", "source_key", "TEXT")
    db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_outputs_source_key ON employee_outputs (source_key)"
    )
    db.execute(
        "INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, ?)",
        ("organization_name", "PHILIPPINE STATISTICS AUTHORITY"),
    )
    db.execute(
        "INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, ?)",
        ("report_title", "Daily Accomplishment Report"),
    )
    _ensure_column(db, "schedules", "assigned_rko_employee_id", "INTEGER")
    _ensure_column(db, "schedules", "assigned_ra_employee_id", "INTEGER")
    _ensure_column(db, "schedules", "event_place_activity", "TEXT")
    _ensure_column(db, "schedules", "status", "TEXT NOT NULL DEFAULT 'Pending'")
    _ensure_column(db, "schedules", "vehicle", "TEXT NOT NULL DEFAULT 'PSA'")
    _ensure_column(db, "schedules", "needed_rko_count", "INTEGER NOT NULL DEFAULT 1")
    _ensure_column(db, "schedules", "needed_ra_count", "INTEGER NOT NULL DEFAULT 1")
    db.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_schedule_assignments_unique
        ON schedule_assignments (schedule_id, employee_id, role)
        """
    )
    db.commit()


def _ensure_column(db: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    columns = {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in columns:
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_app(app) -> None:
    app.teardown_appcontext(close_db)
    with app.app_context():
        init_db()
