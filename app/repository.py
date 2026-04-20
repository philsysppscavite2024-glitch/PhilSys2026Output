from __future__ import annotations

from datetime import datetime
from typing import Iterable

from .db import get_db


SERVICE_TYPES = [
    "National ID Registration",
    "Issuance of National ID in Paper Form Only",
    "National ID Registration and Assistance in Generating the Digital National ID",
    "Assistance in Generating the Digital National ID Only",
    "TRN Retrieval",
    "TRN Retrieval and Issuance of National ID in Paper Form Only",
    "Authentication",
    "Recapture",
    "Rejected Packet",
    "Authentication and Issuance of National ID in Paper Form",
]


def _upper_text(value: str | None) -> str:
    return (value or "").strip().upper()


def fetch_settings() -> dict[str, str]:
    rows = get_db().execute("SELECT key, value FROM app_settings").fetchall()
    return {row["key"]: row["value"] for row in rows}


def set_setting(key: str, value: str) -> None:
    db = get_db()
    db.execute(
        """
        INSERT INTO app_settings (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, value),
    )
    db.commit()


def list_employees() -> list:
    return get_db().execute(
        """
        SELECT id, employee_code, full_name, position, province, city_municipality, active
        FROM employees
        ORDER BY active DESC, full_name ASC
        """
    ).fetchall()


def get_employee(employee_id: int):
    return get_db().execute(
        """
        SELECT id, employee_code, full_name, position, province, city_municipality, active
        FROM employees WHERE id = ?
        """,
        (employee_id,),
    ).fetchone()


def save_employee(employee_id: int | None, payload: dict) -> None:
    db = get_db()
    fields = (
        (_upper_text(payload.get("employee_code")) or None),
        _upper_text(payload["full_name"]),
        _upper_text(payload.get("position")),
        _upper_text(payload.get("province")),
        _upper_text(payload.get("city_municipality")),
        1 if payload.get("active") else 0,
    )
    if employee_id is None:
        db.execute(
            """
            INSERT INTO employees
            (employee_code, full_name, position, province, city_municipality, active)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            fields,
        )
    else:
        db.execute(
            """
            UPDATE employees
            SET employee_code=?, full_name=?, position=?, province=?, city_municipality=?, active=?
            WHERE id=?
            """,
            (*fields, employee_id),
        )
    db.commit()


def delete_employee(employee_id: int) -> None:
    db = get_db()
    db.execute("DELETE FROM employees WHERE id = ?", (employee_id,))
    db.commit()


def list_schedule_employees(position: str) -> list:
    """Employees filtered by position for schedule assignment dropdowns."""
    return get_db().execute(
        """
        SELECT id, full_name, position
        FROM employees
        WHERE active = 1 AND lower(position) = lower(?)
        ORDER BY full_name ASC
        """,
        (position,),
    ).fetchall()


def list_schedules(filters: dict | None = None) -> list:
    filters = filters or {}
    sql = """
        SELECT
            s.id,
            s.schedule_date,
            s.city_municipality,
            s.assigned_rko_employee_id,
            s.assigned_ra_employee_id,
            s.event_place_activity,
            s.status,
            s.vehicle,
            s.needed_rko_count,
            s.needed_ra_count,
            rko.full_name AS assigned_rko_name,
            ra.full_name AS assigned_ra_name
        FROM schedules s
        LEFT JOIN employees rko ON rko.id = s.assigned_rko_employee_id
        LEFT JOIN employees ra ON ra.id = s.assigned_ra_employee_id
        WHERE 1 = 1
    """
    params: list = []
    if filters.get("start_date"):
        sql += " AND s.schedule_date >= ?"
        params.append(filters["start_date"])
    if filters.get("end_date"):
        sql += " AND s.schedule_date <= ?"
        params.append(filters["end_date"])
    if filters.get("status"):
        sql += " AND lower(s.status) = lower(?)"
        params.append(filters["status"])
    if filters.get("employee_id"):
        sql += """
            AND (
                EXISTS (
                    SELECT 1
                    FROM schedule_assignments sa_filter
                    WHERE sa_filter.schedule_id = s.id AND sa_filter.employee_id = ?
                )
                OR s.assigned_rko_employee_id = ?
                OR s.assigned_ra_employee_id = ?
            )
        """
        params.extend([filters["employee_id"], filters["employee_id"], filters["employee_id"]])
    sql += " ORDER BY s.schedule_date ASC, s.city_municipality ASC, s.id ASC"
    rows = get_db().execute(sql, params).fetchall()
    db = get_db()
    result: list[dict] = []
    for row in rows:
        row_dict = dict(row)
        assignments = db.execute(
            """
            SELECT sa.role, sa.employee_id, e.full_name
            FROM schedule_assignments sa
            JOIN employees e ON e.id = sa.employee_id
            WHERE sa.schedule_id = ?
            ORDER BY e.full_name ASC
            """,
            (row["id"],),
        ).fetchall()
        rko_names = [a["full_name"] for a in assignments if a["role"] == "rko"]
        ra_names = [a["full_name"] for a in assignments if a["role"] == "ra"]
        rko_ids = [a["employee_id"] for a in assignments if a["role"] == "rko"]
        ra_ids = [a["employee_id"] for a in assignments if a["role"] == "ra"]

        if not rko_names and row["assigned_rko_name"]:
            rko_names = [row["assigned_rko_name"]]
        if not ra_names and row["assigned_ra_name"]:
            ra_names = [row["assigned_ra_name"]]
        if not rko_ids and row["assigned_rko_employee_id"]:
            rko_ids = [int(row["assigned_rko_employee_id"])]
        if not ra_ids and row["assigned_ra_employee_id"]:
            ra_ids = [int(row["assigned_ra_employee_id"])]

        row_dict["assigned_rko_names"] = rko_names
        row_dict["assigned_ra_names"] = ra_names
        row_dict["assigned_rko_ids"] = rko_ids
        row_dict["assigned_ra_ids"] = ra_ids
        result.append(row_dict)
    return result


def get_schedule(schedule_id: int):
    row = get_db().execute(
        """
        SELECT id, schedule_date, city_municipality, assigned_rko_employee_id,
               assigned_ra_employee_id, event_place_activity, status, vehicle,
               needed_rko_count, needed_ra_count
        FROM schedules
        WHERE id = ?
        """,
        (schedule_id,),
    ).fetchone()
    if not row:
        return None
    db = get_db()
    row_dict = dict(row)
    assignments = db.execute(
        """
        SELECT role, employee_id
        FROM schedule_assignments
        WHERE schedule_id = ?
        """,
        (schedule_id,),
    ).fetchall()
    rko_ids = [a["employee_id"] for a in assignments if a["role"] == "rko"]
    ra_ids = [a["employee_id"] for a in assignments if a["role"] == "ra"]
    if not rko_ids and row["assigned_rko_employee_id"]:
        rko_ids = [int(row["assigned_rko_employee_id"])]
    if not ra_ids and row["assigned_ra_employee_id"]:
        ra_ids = [int(row["assigned_ra_employee_id"])]
    row_dict["assigned_rko_employee_ids"] = rko_ids
    row_dict["assigned_ra_employee_ids"] = ra_ids
    return row_dict


def save_schedule(schedule_id: int | None, payload) -> None:
    db = get_db()
    rko_ids = [
        int(v)
        for v in payload.getlist("assigned_rko_employee_ids")
        if str(v).strip()
    ]
    ra_ids = [
        int(v)
        for v in payload.getlist("assigned_ra_employee_ids")
        if str(v).strip()
    ]
    needed_rko_count = max(0, int(payload.get("needed_rko_count", 1) or 0))
    needed_ra_count = max(0, int(payload.get("needed_ra_count", 1) or 0))
    fields = (
        payload["schedule_date"],
        _upper_text(payload.get("city_municipality")),
        (rko_ids[0] if rko_ids else None),
        (ra_ids[0] if ra_ids else None),
        _upper_text(payload.get("event_place_activity")),
        (_upper_text(payload.get("status", "Pending")) or "PENDING"),
        (_upper_text(payload.get("vehicle", "PSA")) or "PSA"),
        needed_rko_count,
        needed_ra_count,
    )
    if schedule_id is None:
        cursor = db.execute(
            """
            INSERT INTO schedules
            (schedule_date, city_municipality, assigned_rko_employee_id, assigned_ra_employee_id,
             event_place_activity, status, vehicle, needed_rko_count, needed_ra_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            fields,
        )
        target_schedule_id = cursor.lastrowid
    else:
        db.execute(
            """
            UPDATE schedules
            SET schedule_date=?, city_municipality=?, assigned_rko_employee_id=?, assigned_ra_employee_id=?,
                event_place_activity=?, status=?, vehicle=?, needed_rko_count=?, needed_ra_count=?
            WHERE id=?
            """,
            (*fields, schedule_id),
        )
        target_schedule_id = schedule_id

    db.execute("DELETE FROM schedule_assignments WHERE schedule_id = ?", (target_schedule_id,))
    for employee_id in sorted(set(rko_ids)):
        db.execute(
            """
            INSERT OR IGNORE INTO schedule_assignments (schedule_id, employee_id, role)
            VALUES (?, ?, 'rko')
            """,
            (target_schedule_id, employee_id),
        )
    for employee_id in sorted(set(ra_ids)):
        db.execute(
            """
            INSERT OR IGNORE INTO schedule_assignments (schedule_id, employee_id, role)
            VALUES (?, ?, 'ra')
            """,
            (target_schedule_id, employee_id),
        )
    db.commit()


def delete_schedule(schedule_id: int) -> None:
    db = get_db()
    db.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))
    db.commit()


def list_outputs(filters: dict | None = None) -> list:
    filters = filters or {}
    sql = """
        SELECT o.id, o.work_date, o.category, o.activity_type, o.quantity, o.remarks,
               o.province, o.city_municipality, o.source_ref,
               e.id AS employee_id, e.full_name, e.employee_code
        FROM employee_outputs o
        JOIN employees e ON e.id = o.employee_id
        WHERE 1 = 1
    """
    params: list = []
    if filters.get("employee_id"):
        sql += " AND e.id = ?"
        params.append(filters["employee_id"])
    if filters.get("month"):
        sql += " AND substr(o.work_date, 1, 7) = ?"
        params.append(filters["month"])
    if filters.get("start_date"):
        sql += " AND o.work_date >= ?"
        params.append(filters["start_date"])
    if filters.get("end_date"):
        sql += " AND o.work_date <= ?"
        params.append(filters["end_date"])
    if filters.get("category"):
        sql += " AND lower(o.category) = lower(?)"
        params.append(filters["category"])
    sql += " ORDER BY o.work_date DESC, e.full_name ASC, o.id DESC"
    return get_db().execute(sql, params).fetchall()


def output_per_person_rows(filters: dict | None = None) -> list[dict]:
    filters = filters or {}
    sql = """
        SELECT
            o.work_date,
            e.full_name
    """
    for idx, service in enumerate(SERVICE_TYPES):
        sql += f""",
            SUM(CASE WHEN o.activity_type = ? THEN o.quantity ELSE 0 END) AS s{idx}
        """
    sql += """
            ,SUM(CASE WHEN o.activity_type = ? THEN o.quantity ELSE 0 END) AS total
        FROM employee_outputs o
        JOIN employees e ON e.id = o.employee_id
        WHERE 1 = 1
    """

    params: list = list(SERVICE_TYPES)
    params.append(SERVICE_TYPES[0])
    if filters.get("employee_id"):
        sql += " AND e.id = ?"
        params.append(filters["employee_id"])
    if filters.get("start_date"):
        sql += " AND o.work_date >= ?"
        params.append(filters["start_date"])
    if filters.get("end_date"):
        sql += " AND o.work_date <= ?"
        params.append(filters["end_date"])

    sql += """
        GROUP BY o.work_date, e.full_name
        ORDER BY e.full_name ASC, o.work_date ASC
    """

    rows = get_db().execute(sql, params).fetchall()
    result = []
    running_totals: dict[str, int] = {}
    for row in rows:
        name = row["full_name"]
        total = row["total"]
        national_id_registration = row["s0"]
        running_totals[name] = running_totals.get(name, 0) + national_id_registration
        item = {
            "date": datetime.strptime(row["work_date"], "%Y-%m-%d").strftime("%m/%d/%Y"),
            "name": name,
            "total": total,
            "cumulative": running_totals[name],
        }
        for idx, service in enumerate(SERVICE_TYPES):
            item[service] = row[f"s{idx}"]
        result.append(item)
    result.sort(key=lambda item: (datetime.strptime(item["date"], "%m/%d/%Y"), item["name"]))
    return result


def output_summary_grand_totals(rows: list[dict], service_types: list[str]) -> dict[str, int | dict[str, int]]:
    """Sum each numeric column across filtered rows (date range / employee scope of ``rows``)."""
    by_service: dict[str, int] = {s: 0 for s in service_types}
    total_sum = 0
    cumulative_sum = 0
    for row in rows:
        for s in service_types:
            by_service[s] += int(row.get(s, 0) or 0)
        total_sum += int(row.get("total", 0) or 0)
        cumulative_sum += int(row.get("cumulative", 0) or 0)
    return {"services": by_service, "total": total_sum, "cumulative": cumulative_sum}


def get_output(output_id: int):
    return get_db().execute(
        """
        SELECT id, employee_id, work_date, category, activity_type, quantity, remarks,
               sex, province, city_municipality, source_ref
        FROM employee_outputs WHERE id = ?
        """,
        (output_id,),
    ).fetchone()


def save_output(output_id: int | None, payload: dict) -> None:
    db = get_db()
    fields = (
        int(payload["employee_id"]),
        payload["work_date"],
        _upper_text(payload["category"]),
        payload.get("activity_type", "").strip(),
        _upper_text(payload.get("sex")),
        int(payload.get("quantity", 0) or 0),
        _upper_text(payload.get("remarks")),
        _upper_text(payload.get("province")),
        _upper_text(payload.get("city_municipality")),
        payload.get("source_ref", "").strip(),
        (payload.get("source_key") or "").strip() or None,
    )
    if output_id is None:
        db.execute(
            """
            INSERT OR IGNORE INTO employee_outputs
            (employee_id, work_date, category, activity_type, sex, quantity, remarks,
             province, city_municipality, source_ref, source_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            fields,
        )
    else:
        db.execute(
            """
            UPDATE employee_outputs
            SET employee_id=?, work_date=?, category=?, activity_type=?, sex=?, quantity=?, remarks=?,
                province=?, city_municipality=?, source_ref=?, source_key=?
            WHERE id=?
            """,
            (*fields, output_id),
        )
    db.commit()


def delete_output(output_id: int) -> None:
    db = get_db()
    db.execute("DELETE FROM employee_outputs WHERE id = ?", (output_id,))
    db.commit()


def list_signatories() -> list:
    return get_db().execute(
        """
        SELECT id, report_type, role, name, position
        FROM signatories
        ORDER BY report_type ASC, role ASC, name ASC
        """
    ).fetchall()


def get_signatory(signatory_id: int):
    return get_db().execute(
        """
        SELECT id, report_type, role, name, position
        FROM signatories WHERE id = ?
        """,
        (signatory_id,),
    ).fetchone()


def save_signatory(signatory_id: int | None, payload: dict) -> None:
    db = get_db()
    fields = (
        payload.get("report_type", "general").strip() or "general",
        payload["role"].strip(),
        _upper_text(payload["name"]),
        _upper_text(payload["position"]),
    )
    if signatory_id is None:
        db.execute(
            """
            INSERT INTO signatories (report_type, role, name, position)
            VALUES (?, ?, ?, ?)
            """,
            fields,
        )
    else:
        db.execute(
            """
            UPDATE signatories
            SET report_type=?, role=?, name=?, position=?
            WHERE id=?
            """,
            (*fields, signatory_id),
        )
    db.commit()


def delete_signatory(signatory_id: int) -> None:
    db = get_db()
    db.execute("DELETE FROM signatories WHERE id = ?", (signatory_id,))
    db.commit()


def monthly_city_records(month: str) -> list:
    return get_db().execute(
        """
        SELECT
            COALESCE(NULLIF(o.city_municipality, ''), NULLIF(e.city_municipality, ''), 'Unassigned') AS city_municipality,
            lower(o.category) AS category_key,
            COUNT(DISTINCT o.employee_id) AS employees,
            SUM(o.quantity) AS total_quantity
        FROM employee_outputs o
        JOIN employees e ON e.id = o.employee_id
        WHERE substr(o.work_date, 1, 7) = ?
        GROUP BY COALESCE(NULLIF(o.city_municipality, ''), NULLIF(e.city_municipality, ''), 'Unassigned'), lower(o.category)
        ORDER BY city_municipality ASC, category_key ASC
        """,
        (month,),
    ).fetchall()


def city_service_rows(
    filters: dict | None = None,
    city_municipalities: list[str] | None = None,
) -> list[dict]:
    """Per-city service summary with Male/Female/Total per service.

    Ensures all ``city_municipalities`` are present even with zero records.
    """
    filters = filters or {}
    sql = """
        SELECT
            COALESCE(NULLIF(o.city_municipality, ''), NULLIF(e.city_municipality, ''), 'Unassigned') AS city_municipality,
            COALESCE(NULLIF(o.activity_type, ''), '') AS activity_type,
            lower(COALESCE(o.sex, '')) AS sex_key,
            SUM(o.quantity) AS qty
        FROM employee_outputs o
        JOIN employees e ON e.id = o.employee_id
        WHERE 1 = 1
    """
    params: list = []
    if filters.get("start_date"):
        sql += " AND o.work_date >= ?"
        params.append(filters["start_date"])
    if filters.get("end_date"):
        sql += " AND o.work_date <= ?"
        params.append(filters["end_date"])
    sql += """
        GROUP BY
            COALESCE(NULLIF(o.city_municipality, ''), NULLIF(e.city_municipality, ''), 'Unassigned'),
            COALESCE(NULLIF(o.activity_type, ''), ''),
            lower(COALESCE(o.sex, ''))
    """

    raw_rows = get_db().execute(sql, params).fetchall()

    def _norm_city(city: str) -> str:
        return (city or "").strip().upper()

    base_cities = city_municipalities or []
    ordered_keys = [_norm_city(city) for city in base_cities if (city or "").strip()]
    key_to_display = {_norm_city(city): city for city in base_cities if (city or "").strip()}

    by_city: dict[str, dict] = {}
    for row in raw_rows:
        city_display = (row["city_municipality"] or "Unassigned").strip() or "Unassigned"
        city_key = _norm_city(city_display) or "UNASSIGNED"
        if city_key not in by_city:
            by_city[city_key] = {
                "city_municipality": key_to_display.get(city_key, city_display),
                "services": {
                    service: {"male": 0, "female": 0, "total": 0}
                    for service in SERVICE_TYPES
                },
                "male": 0,
                "female": 0,
                "total": 0,
            }
        activity = (row["activity_type"] or "").strip()
        if activity not in SERVICE_TYPES:
            continue
        qty = int(row["qty"] or 0)
        sex_key = (row["sex_key"] or "").strip().lower()
        slot = by_city[city_key]["services"][activity]
        slot["total"] += qty
        by_city[city_key]["total"] += qty
        if sex_key == "male":
            slot["male"] += qty
            by_city[city_key]["male"] += qty
        elif sex_key == "female":
            slot["female"] += qty
            by_city[city_key]["female"] += qty

    # Ensure official city list appears even without rows.
    for city in base_cities:
        city_key = _norm_city(city)
        if city_key not in by_city:
            by_city[city_key] = {
                "city_municipality": city,
                "services": {
                    service: {"male": 0, "female": 0, "total": 0}
                    for service in SERVICE_TYPES
                },
                "male": 0,
                "female": 0,
                "total": 0,
            }

    extras = sorted([k for k in by_city.keys() if k not in set(ordered_keys)])
    ordered = ordered_keys + extras
    return [by_city[k] for k in ordered]


def employee_monthly_summary(employee_id: int, month: str, category: str | None = None) -> list:
    sql = """
        SELECT work_date, category, activity_type, quantity, remarks,
               COALESCE(NULLIF(city_municipality, ''), '') AS city_municipality,
               COALESCE(NULLIF(province, ''), '') AS province
        FROM employee_outputs
        WHERE employee_id = ? AND substr(work_date, 1, 7) = ?
    """
    params: list = [employee_id, month]
    if category:
        sql += " AND lower(category) = lower(?)"
        params.append(category)
    sql += " ORDER BY work_date ASC, id ASC"
    return get_db().execute(sql, params).fetchall()


def employee_date_summary(
    employee_id: int,
    start_date: str,
    end_date: str,
    category: str | None = None,
) -> list:
    sql = """
        SELECT work_date, category, activity_type, quantity, remarks,
               COALESCE(NULLIF(city_municipality, ''), '') AS city_municipality,
               COALESCE(NULLIF(province, ''), '') AS province
        FROM employee_outputs
        WHERE employee_id = ? AND work_date >= ? AND work_date <= ?
    """
    params: list = [employee_id, start_date, end_date]
    if category:
        sql += " AND lower(category) = lower(?)"
        params.append(category)
    sql += " ORDER BY work_date ASC, id ASC"
    return get_db().execute(sql, params).fetchall()


def _normalize_signatory_role(role_raw: str | None) -> str | None:
    """Map DB role strings to prepared_by / verified_by (handles minor typos)."""
    r = (role_raw or "").strip().lower().replace(" ", "_").replace("-", "_")
    if r in ("prepared_by", "preparedby"):
        return "prepared_by"
    if r in ("verified_by", "verifiedby"):
        return "verified_by"
    if "verify" in r:
        return "verified_by"
    if "prepared" in r:
        return "prepared_by"
    return None


def signatories_for_report(report_type: str) -> dict[str, dict]:
    """Resolve signatories by role for a report. Prefer the most specific `report_type`, then `general`.

    Employee Output Summary uses ``report_type='output'``. Signatories may be stored as ``output``,
    ``delivery`` (legacy), ``registration``, or ``general`` — merged with priority order.

    If the report-specific row exists but has an empty name or position, merge from the next
    matching row so the same rules apply to PDF and HTML preview.
    """
    db = get_db()
    rt = (report_type or "general").strip().lower()

    if rt == "output":
        rows = db.execute(
            """
            SELECT role, name, position, report_type
            FROM signatories
            WHERE report_type IN ('output', 'delivery', 'registration', 'general')
            ORDER BY
                CASE report_type
                    WHEN 'output' THEN 0
                    WHEN 'delivery' THEN 1
                    WHEN 'registration' THEN 2
                    WHEN 'general' THEN 3
                    ELSE 4
                END,
                role ASC
            """
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT role, name, position, report_type
            FROM signatories
            WHERE report_type IN (?, 'general')
            ORDER BY CASE WHEN report_type = ? THEN 0 ELSE 1 END, role ASC
            """,
            (rt, rt),
        ).fetchall()

    result: dict[str, dict] = {}
    for row in rows:
        role_key = _normalize_signatory_role(row["role"])
        if not role_key:
            continue
        name = (row["name"] or "").strip()
        position = (row["position"] or "").strip()
        if role_key not in result:
            result[role_key] = {"name": row["name"] or "", "position": row["position"] or ""}
            continue
        cur = result[role_key]
        if not (cur.get("name") or "").strip() and name:
            cur["name"] = row["name"] or ""
        if not (cur.get("position") or "").strip() and position:
            cur["position"] = row["position"] or ""

    # Fallback for installations where "Verified as correct by" was saved as a
    # Focal Person row but not tagged with role=verified_by.
    verified = result.get("verified_by") or {}
    if not (verified.get("name") or "").strip():
        for row in rows:
            role_raw = (row["role"] or "").strip().lower()
            name = (row["name"] or "").strip()
            position = (row["position"] or "").strip()
            if not name:
                continue
            if role_raw == "verified_by" or "focal person" in position.lower():
                result["verified_by"] = {"name": row["name"] or "", "position": position or "Focal Person"}
                break

    # Last resort: newest signatory row whose role maps to verified_by (any report_type)
    if not ((result.get("verified_by") or {}).get("name") or "").strip():
        for row in db.execute(
            "SELECT role, name, position FROM signatories ORDER BY id DESC"
        ).fetchall():
            if _normalize_signatory_role(row["role"]) == "verified_by" and (row["name"] or "").strip():
                result["verified_by"] = {
                    "name": row["name"] or "",
                    "position": (row["position"] or "") or "Focal Person",
                }
                break
    return result


def find_employee_by_code_or_name(employee_code: str | None, full_name: str | None):
    db = get_db()
    if employee_code:
        row = db.execute(
            "SELECT * FROM employees WHERE employee_code = ?",
            (employee_code.strip(),),
        ).fetchone()
        if row:
            return row
    if full_name:
        return db.execute(
            "SELECT * FROM employees WHERE lower(full_name) = lower(?)",
            (full_name.strip(),),
        ).fetchone()
    return None


def create_employee_if_missing(payload: dict):
    employee = find_employee_by_code_or_name(
        payload.get("employee_code"), payload.get("full_name")
    )
    if employee:
        return employee["id"]

    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO employees (employee_code, full_name, position, province, city_municipality, active)
        VALUES (?, ?, ?, ?, ?, 1)
        """,
        (
            payload.get("employee_code") or None,
            _upper_text(payload["full_name"]),
            _upper_text(payload.get("position")),
            _upper_text(payload.get("province")),
            _upper_text(payload.get("city_municipality")),
        ),
    )
    db.commit()
    return cursor.lastrowid


def bulk_insert_outputs(rows: Iterable[dict]) -> int:
    db = get_db()
    inserted = 0
    for row in rows:
        db.execute(
            """
            INSERT INTO employee_outputs
            (employee_id, work_date, category, activity_type, sex, quantity, remarks,
             province, city_municipality, source_ref, source_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["employee_id"],
                row["work_date"],
                _upper_text(row["category"]),
                row.get("activity_type", ""),
                _upper_text(row.get("sex", "")),
                int(row.get("quantity", 0) or 0),
                _upper_text(row.get("remarks", "")),
                _upper_text(row.get("province", "")),
                _upper_text(row.get("city_municipality", "")),
                row.get("source_ref", ""),
                row.get("source_key") or None,
            ),
        )
        if db.execute("SELECT changes()").fetchone()[0]:
            inserted += 1
    db.commit()
    return inserted


def delete_imported_outputs(source_type: str = "all") -> int:
    db = get_db()
    if source_type == "google_sheet":
        cursor = db.execute(
            """
            DELETE FROM employee_outputs
            WHERE source_ref LIKE 'https://docs.google.com/spreadsheets/%'
               OR source_ref LIKE 'https://docs.google.com/%'
            """
        )
    elif source_type == "apps_script":
        cursor = db.execute(
            """
            DELETE FROM employee_outputs
            WHERE source_ref LIKE 'https://script.google.com/%'
            """
        )
    else:
        cursor = db.execute(
            """
            DELETE FROM employee_outputs
            WHERE source_ref IS NOT NULL AND trim(source_ref) <> ''
            """
        )
    db.commit()
    return cursor.rowcount
