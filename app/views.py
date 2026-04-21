from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from flask import flash, redirect, render_template, request, send_file, send_from_directory, url_for
from werkzeug.utils import secure_filename

from .repository import (
    SERVICE_TYPES,
    delete_schedule,
    output_summary_grand_totals,
    delete_employee,
    delete_output,
    delete_signatory,
    fetch_settings,
    get_employee,
    get_output,
    get_signatory,
    city_service_rows,
    delete_imported_outputs,
    get_schedule,
    list_employees,
    list_outputs,
    list_schedule_employees,
    list_schedules,
    list_signatories,
    monthly_city_records,
    output_per_person_rows,
    save_employee,
    save_output,
    save_schedule,
    save_signatory,
    set_setting,
    signatories_for_report,
)
from .services.importers import import_from_apps_script, import_from_csv_url
from .services.reports import (
    generate_bulk_reports_by_date,
    generate_city_service_summary_report,
    generate_employee_report_by_date,
    generate_locator_chart_all_report,
    generate_locator_chart_report,
    generate_output_summary_report,
    generate_all_employees_output_reports_by_date,
    generate_schedule_report,
)

CITY_MUNICIPALITIES = [
    "ALFONSO",
    "AMADEO",
    "BACOOR CITY",
    "CARMONA",
    "CAVITE CITY",
    "CITY OF DASMARIÑAS",
    "GENERAL EMILIO AGUINALDO",
    "CITY OF GENERAL TRIAS",
    "IMUS CITY",
    "INDANG",
    "KAWIT",
    "MAGALLANES",
    "MARAGONDON",
    "MENDEZ (MENDEZ-NUÑEZ)",
    "NAIC",
    "NOVELETA",
    "ROSARIO",
    "SILANG",
    "TAGAYTAY CITY",
    "TANZA",
    "TERNATE",
    "TRECE MARTIRES CITY (Capital)",
    "GEN. MARIANO ALVAREZ",
]


def register_routes(app):
    @app.get("/health")
    def health_check():
        return {"status": "ok"}, 200

    @app.context_processor
    def inject_settings():
        return {
            "app_settings": fetch_settings(),
            "city_municipalities": CITY_MUNICIPALITIES,
        }

    @app.get("/logo/<path:filename>")
    def logo_file(filename):
        # Try uploaded logo first (uploads stored in UPLOAD_DIR), then fall back to the repo /logo directory
        upload_dir = Path(app.config.get("UPLOAD_DIR", Path(app.root_path).parent))
        uploaded_candidate = upload_dir / filename
        if uploaded_candidate.exists():
            return send_from_directory(str(upload_dir), filename)

        fixed_dir = Path(app.root_path).parent / "logo"
        fixed_candidate = fixed_dir / filename
        if fixed_candidate.exists():
            return send_from_directory(str(fixed_dir), filename)

        # If an uploaded logo exists under a generic name, serve it as a fallback
        for candidate in upload_dir.glob("logo.*"):
            return send_from_directory(str(upload_dir), candidate.name)

        from flask import abort

        abort(404)

    @app.route("/")
    def dashboard():
        try:
            start_date, end_date = _resolve_date_filter()
            outputs = list_outputs({"start_date": start_date, "end_date": end_date})
            employees = list_employees()
            total_quantity = sum(row["quantity"] for row in outputs)
            active_employees = sum(1 for row in employees if row["active"])
            city_rows = monthly_city_records(start_date[:7])
            return render_template(
                "dashboard.html",
                month=start_date[:7],
                start_date=start_date,
                end_date=end_date,
                outputs=outputs,
                active_employees=active_employees,
                total_quantity=total_quantity,
                city_rows=city_rows,
            )
        except Exception as e:
            from flask import jsonify
            return jsonify({"error": str(e)}), 500

    @app.route("/employees")
    def employees():
        return render_template("employees.html", employees=list_employees())

    @app.route("/employees/new", methods=["GET", "POST"])
    @app.route("/employees/<int:employee_id>/edit", methods=["GET", "POST"])
    def employee_form(employee_id=None):
        employee = get_employee(employee_id) if employee_id else None
        if request.method == "POST":
            try:
                save_employee(employee_id, request.form)
                flash("Employee saved successfully.", "success")
                return redirect(url_for("employees"))
            except sqlite3.IntegrityError:
                flash("Employee code already exists. Please use a unique code.", "error")
                employee = request.form
        return render_template("employee_form.html", employee=employee)

    @app.post("/employees/<int:employee_id>/delete")
    def employee_delete(employee_id):
        delete_employee(employee_id)
        flash("Employee deleted.", "success")
        return redirect(url_for("employees"))

    @app.route("/outputs")
    def outputs():
        filters = {
            "employee_id": request.args.get("employee_id") or None,
            "start_date": request.args.get("start_date") or "",
            "end_date": request.args.get("end_date") or "",
        }
        filters = {key: value for key, value in filters.items() if value}
        preview_query = {}
        if filters.get("employee_id"):
            preview_query["employee_id"] = filters["employee_id"]
        if filters.get("start_date"):
            preview_query["start_date"] = filters["start_date"]
        if filters.get("end_date"):
            preview_query["end_date"] = filters["end_date"]
        export_query = {k: filters[k] for k in ("start_date", "end_date") if filters.get(k)}
        return render_template(
            "outputs.html",
            outputs=output_per_person_rows(filters),
            employees=list_employees(),
            filters=filters,
            preview_query=preview_query,
            export_query=export_query,
            service_types=SERVICE_TYPES,
        )

    @app.get("/reports/export-all-outputs")
    def export_all_outputs():
        start_date, end_date = _resolve_output_report_date_range()
        zip_path = generate_all_employees_output_reports_by_date(start_date, end_date)
        return send_file(
            zip_path,
            as_attachment=True,
            download_name=f"{start_date}_to_{end_date}_all_employees_output_reports.zip",
        )

    @app.route("/outputs/new", methods=["GET", "POST"])
    @app.route("/outputs/<int:output_id>/edit", methods=["GET", "POST"])
    def output_form(output_id=None):
        output = get_output(output_id) if output_id else None
        if request.method == "POST":
            save_output(output_id, request.form)
            flash("Employee output saved successfully.", "success")
            return redirect(url_for("outputs"))
        return render_template(
            "output_form.html",
            output=output,
            employees=list_employees(),
        )

    @app.post("/outputs/<int:output_id>/delete")
    def output_delete(output_id):
        delete_output(output_id)
        flash("Employee output deleted.", "success")
        return redirect(url_for("outputs"))

    @app.route("/city-records")
    def city_records():
        start_date, end_date = _resolve_date_filter()
        rows = city_service_rows(
            {"start_date": start_date, "end_date": end_date},
            city_municipalities=CITY_MUNICIPALITIES,
        )
        return render_template(
            "city_records.html",
            month=start_date[:7],
            start_date=start_date,
            end_date=end_date,
            rows=rows,
            service_types=SERVICE_TYPES,
        )

    @app.get("/reports/city-records")
    def city_records_report():
        start_date, end_date = _resolve_date_filter()
        rows = city_service_rows(
            {"start_date": start_date, "end_date": end_date},
            city_municipalities=CITY_MUNICIPALITIES,
        )
        pdf_path = generate_city_service_summary_report(rows, SERVICE_TYPES, start_date, end_date)
        return send_file(pdf_path, mimetype="application/pdf")

    @app.route("/schedules")
    def schedules():
        filters = {
            "start_date": (request.args.get("start_date") or "").strip(),
            "end_date": (request.args.get("end_date") or "").strip(),
            "status": (request.args.get("status") or "").strip(),
            "employee_id": (request.args.get("employee_id") or "").strip(),
        }
        filters = {key: value for key, value in filters.items() if value}
        rows = list_schedules(filters)
        calendar_events = [
            {
                "title": f"{row['city_municipality']} - {(row['status'] or 'Pending')}",
                "start": row["schedule_date"],
                "color": "#16a34a" if (row["status"] or "").lower() == "approved" else "#f59e0b",
                "url": url_for("schedule_form", schedule_id=row["id"]),
            }
            for row in rows
        ]
        return render_template(
            "schedules.html",
            schedules=rows,
            filters=filters,
            calendar_events=calendar_events,
            employees=list_employees(),
        )

    @app.get("/reports/schedules")
    def schedules_report():
        filters = {
            "start_date": (request.args.get("start_date") or "").strip(),
            "end_date": (request.args.get("end_date") or "").strip(),
            "status": (request.args.get("status") or "").strip(),
            "employee_id": (request.args.get("employee_id") or "").strip(),
        }
        scoped = {k: v for k, v in filters.items() if v}
        rows = list_schedules(scoped)
        pdf_path = generate_schedule_report(
            rows,
            start_date=filters["start_date"],
            end_date=filters["end_date"],
            status=filters["status"],
        )
        return send_file(pdf_path, mimetype="application/pdf")

    @app.get("/reports/locators-chart")
    def locator_chart_report():
        employee_id_raw = (request.args.get("employee_id") or "").strip()
        if not employee_id_raw:
            flash("Please select an employee before printing Locator's Chart.", "error")
            return redirect(url_for("schedules"))
        try:
            employee_id = int(employee_id_raw)
        except ValueError:
            flash("Invalid employee selected.", "error")
            return redirect(url_for("schedules"))

        employee = get_employee(employee_id)
        if not employee:
            flash("Employee not found.", "error")
            return redirect(url_for("schedules"))

        filters = {
            "start_date": (request.args.get("start_date") or "").strip(),
            "end_date": (request.args.get("end_date") or "").strip(),
            "status": (request.args.get("status") or "").strip(),
            "employee_id": employee_id_raw,
        }
        scoped = {k: v for k, v in filters.items() if v}
        rows = list_schedules(scoped)
        pdf_path = generate_locator_chart_report(
            dict(employee),
            rows,
            start_date=filters["start_date"],
            end_date=filters["end_date"],
        )
        return send_file(pdf_path, mimetype="application/pdf")

    @app.get("/reports/locators-chart-all")
    def locator_chart_all_report():
        filters = {
            "start_date": (request.args.get("start_date") or "").strip(),
            "end_date": (request.args.get("end_date") or "").strip(),
            "status": (request.args.get("status") or "").strip(),
        }
        scoped = {k: v for k, v in filters.items() if v}
        rows = list_schedules(scoped)
        by_employee: dict[int, list[dict]] = {}
        for row in rows:
            for emp_id in row.get("assigned_rko_ids", []):
                by_employee.setdefault(int(emp_id), []).append(row)
            for emp_id in row.get("assigned_ra_ids", []):
                by_employee.setdefault(int(emp_id), []).append(row)
        employees = [dict(emp) for emp in list_employees() if int(emp["id"]) in by_employee]
        pdf_path = generate_locator_chart_all_report(
            employees,
            by_employee,
            start_date=filters["start_date"],
            end_date=filters["end_date"],
        )
        return send_file(pdf_path, mimetype="application/pdf")

    @app.route("/schedules/new", methods=["GET", "POST"])
    @app.route("/schedules/<int:schedule_id>/edit", methods=["GET", "POST"])
    def schedule_form(schedule_id=None):
        schedule = get_schedule(schedule_id) if schedule_id else None
        if request.method == "POST":
            save_schedule(schedule_id, request.form)
            flash("Schedule saved successfully.", "success")
            return redirect(url_for("schedules"))
        existing_rows = list_schedules({})
        assigned_by_date: dict[str, dict[str, list[int]]] = {}
        for row in existing_rows:
            if schedule_id and row["id"] == schedule_id:
                continue
            date_key = (row["schedule_date"] or "").strip()
            if not date_key:
                continue
            bucket = assigned_by_date.setdefault(date_key, {"rko_ids": [], "ra_ids": []})
            for employee_id in row.get("assigned_rko_ids", []):
                bucket["rko_ids"].append(int(employee_id))
            for employee_id in row.get("assigned_ra_ids", []):
                bucket["ra_ids"].append(int(employee_id))
        rko_employees = list_schedule_employees("Registration Kit Operator")
        ra_employees = list_schedule_employees("Registration Assistant")
        return render_template(
            "schedule_form.html",
            schedule=schedule,
            rko_employees=rko_employees,
            ra_employees=ra_employees,
            assigned_by_date=assigned_by_date,
            rko_total=len(rko_employees),
            ra_total=len(ra_employees),
        )

    @app.post("/schedules/<int:schedule_id>/delete")
    def schedule_delete(schedule_id):
        delete_schedule(schedule_id)
        flash("Schedule deleted.", "success")
        return redirect(url_for("schedules"))

    @app.route("/signatories")
    def signatories():
        return render_template("signatories.html", signatories=list_signatories())

    @app.route("/signatories/new", methods=["GET", "POST"])
    @app.route("/signatories/<int:signatory_id>/edit", methods=["GET", "POST"])
    def signatory_form(signatory_id=None):
        signatory = get_signatory(signatory_id) if signatory_id else None
        if request.method == "POST":
            save_signatory(signatory_id, request.form)
            flash("Signatory saved successfully.", "success")
            return redirect(url_for("signatories"))
        return render_template("signatory_form.html", signatory=signatory)

    @app.post("/signatories/<int:signatory_id>/delete")
    def signatory_delete(signatory_id):
        delete_signatory(signatory_id)
        flash("Signatory deleted.", "success")
        return redirect(url_for("signatories"))

    @app.post("/imports/google-sheet")
    def import_google_sheet():
        csv_url = _normalize_google_sheet_url(request.form.get("csv_url", "").strip())
        if not csv_url:
            flash("Google Sheet CSV export URL is required.", "error")
            return redirect(url_for("dashboard"))
        try:
            inserted = import_from_csv_url(csv_url)
            flash(f"Imported {inserted} new output rows from Google Sheet. Existing rows were kept without duplication.", "success")
        except RuntimeError as e:
            flash(f"Import failed: {str(e)}", "error")
        except Exception as e:
            flash(f"An unexpected error occurred during import: {str(e)}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/imports/apps-script")
    def import_apps_script():
        script_url = request.form.get("script_url", "").strip()
        method = request.form.get("method", "GET").strip().upper()
        payload_text = request.form.get("payload", "").strip()
        payload = json.loads(payload_text) if payload_text else {}
        try:
            inserted = import_from_apps_script(script_url, method=method, payload=payload)
            flash(f"Imported {inserted} output rows from Apps Script.", "success")
        except RuntimeError as e:
            flash(f"Import failed: {str(e)}", "error")
        except Exception as e:
            flash(f"An unexpected error occurred during import: {str(e)}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/imports/clear")
    def clear_imported_data():
        source_type = request.form.get("source_type", "google_sheet").strip()
        deleted = delete_imported_outputs(source_type)
        if source_type == "google_sheet":
            label = "Google Sheet"
        elif source_type == "apps_script":
            label = "Apps Script"
        else:
            label = "imported"
        flash(f"Deleted {deleted} {label} imported rows from the database.", "success")
        return redirect(url_for("dashboard"))

    @app.post("/branding/logo")
    def upload_logo():
        uploaded = request.files.get("logo")
        if not uploaded or not uploaded.filename:
            flash("Please choose a logo file.", "error")
            return redirect(url_for("dashboard"))

        filename = secure_filename(uploaded.filename)
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if ext not in {"png", "jpg", "jpeg"}:
            flash("Logo must be a PNG or JPG image.", "error")
            return redirect(url_for("dashboard"))

        upload_dir = Path(app.config["UPLOAD_DIR"])
        for existing in upload_dir.glob("logo.*"):
            existing.unlink(missing_ok=True)
        saved_path = upload_dir / f"logo.{ext}"
        uploaded.save(saved_path)
        flash("Logo updated.", "success")
        return redirect(url_for("dashboard"))

    @app.post("/settings")
    def save_settings():
        for key in ("organization_name", "report_title"):
            set_setting(key, request.form.get(key, "").strip())
        flash("Report settings updated.", "success")
        return redirect(url_for("dashboard"))

    @app.get("/reports/employee/<int:employee_id>")
    def employee_report(employee_id):
        start_date, end_date = _resolve_date_filter()
        report_type = request.args.get("category") or "registration"
        pdf_path = generate_employee_report_by_date(employee_id, start_date, end_date, report_type)
        employee = get_employee(employee_id)
        return send_file(
            pdf_path,
            as_attachment=True,
            download_name=f"{start_date}_to_{end_date}_{employee['full_name']}_{report_type}.pdf",
        )

    @app.get("/reports/export-all")
    def export_all_reports():
        start_date, end_date = _resolve_date_filter()
        report_type = request.args.get("category") or "registration"
        zip_path = generate_bulk_reports_by_date(start_date, end_date, report_type)
        return send_file(zip_path, as_attachment=True, download_name=zip_path.name)

    @app.get("/reports/output-summary")
    def output_summary_report():
        filters, start_date, end_date, employee_id, employee_name = _output_report_filters_from_request()
        rows = output_per_person_rows(filters)
        prepared_position = None
        if employee_id:
            try:
                emp = get_employee(int(employee_id))
                prepared_position = emp["position"] if emp else None
            except (ValueError, TypeError):
                prepared_position = None
        pdf_path = generate_output_summary_report(
            rows,
            SERVICE_TYPES,
            start_date,
            end_date,
            employee_name,
            prepared_position,
        )
        # Allow previewing inline in the browser (default) or forcing download with ?download=1
        download = str(request.args.get("download", "")).lower() in ("1", "true", "yes")
        if download:
            resp = send_file(pdf_path, as_attachment=True, download_name=pdf_path.name)
        else:
            resp = send_file(pdf_path, mimetype="application/pdf")
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        return resp

    @app.get("/reports/output-summary/preview")
    def output_summary_preview():
        filters, start_date, end_date, employee_id, employee_name = _output_report_filters_from_request()
        rows = output_per_person_rows(filters)
        grand_totals = output_summary_grand_totals(rows, SERVICE_TYPES) if rows else None
        sigs = signatories_for_report("output")
        prepared = sigs.get("prepared_by")
        verified = sigs.get("verified_by")
        report_query = {}
        if employee_id:
            report_query["employee_id"] = employee_id
        if start_date:
            report_query["start_date"] = start_date
        if end_date:
            report_query["end_date"] = end_date
        return render_template(
            "output_summary_preview.html",
            rows=rows,
            grand_totals=grand_totals,
            service_types=SERVICE_TYPES,
            start_date=start_date,
            end_date=end_date,
            employee_id=employee_id,
            report_query=report_query,
            employee_name=employee_name,
            prepared_by_name=employee_name or ((prepared or {}).get("name") or ""),
            prepared_by_position=(prepared or {}).get("position") or "National ID Registration",
            verified_by=verified,
        )


def _current_month():
    from datetime import datetime

    return datetime.now().strftime("%Y-%m")


def _resolve_output_report_date_range() -> tuple[str, str]:
    """Start/end from query string only. Empty means no date bound (same as Employee Output filters)."""
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()
    return start_date, end_date


def _output_report_filters_from_request():
    """Build filters for ``output_per_person_rows`` from the report URL (preview / PDF)."""
    start_date, end_date = _resolve_output_report_date_range()
    employee_raw = (request.args.get("employee_id") or "").strip()
    filters: dict = {}
    if start_date:
        filters["start_date"] = start_date
    if end_date:
        filters["end_date"] = end_date
    employee_id = None
    employee_name = None
    if employee_raw:
        filters["employee_id"] = employee_raw
        try:
            emp = get_employee(int(employee_raw))
            employee_name = emp["full_name"] if emp else None
        except (ValueError, TypeError):
            employee_name = None
        employee_id = employee_raw
    return filters, start_date, end_date, employee_id, employee_name


def _resolve_date_filter():
    from datetime import datetime

    start_date = request.args.get("start_date") or request.form.get("start_date") or ""
    end_date = request.args.get("end_date") or request.form.get("end_date") or ""
    if start_date and end_date:
        return start_date, end_date

    today = datetime.now().strftime("%Y-%m-%d")
    return start_date or today, end_date or start_date or today


def _normalize_google_sheet_url(url: str) -> str:
    if not url:
        return ""
    if "/gviz/tq" in url or "export?format=csv" in url:
        return url
    if "/edit" in url and "/spreadsheets/d/" in url:
        sheet_id = url.split("/spreadsheets/d/")[1].split("/")[0]
        return f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet=Form%20Responses%201"
    return url
