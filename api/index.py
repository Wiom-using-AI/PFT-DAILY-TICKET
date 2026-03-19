"""
Vercel Serverless Entry Point
Wraps the dashboard server as a Flask app for Vercel deployment.
"""

import sys
import os
import json
import csv
import io
import urllib.request
import threading
import time
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request, Response, make_response

# Add parent directory to path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from history_db import (
    get_available_dates,
    get_daily_summary,
    get_all_summaries,
    get_tickets_for_date,
    get_ticket_trail,
    get_all_tickets_for_date,
    get_category_breakdown,
    init_db,
)

IST = timezone(timedelta(hours=5, minutes=30))
MASTER_SHEET_URL = "https://docs.google.com/spreadsheets/d/1E3Ij57bFHznf3S6cRJSzONaVJ7Tgloud51Z__vXLet0/edit?gid=1626982265#gid=1626982265"
MASTER_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/1E3Ij57bFHznf3S6cRJSzONaVJ7Tgloud51Z__vXLet0/export?format=csv&gid=1626982265"

app = Flask(__name__)
init_db()

# ---- Master Sheet Cache ----
_master_ticket_ids = set()
_master_last_refreshed = None
_master_lock = threading.Lock()


def refresh_master_ids():
    global _master_ticket_ids, _master_last_refreshed
    try:
        req = urllib.request.Request(MASTER_SHEET_CSV_URL)
        req.add_header("User-Agent", "Mozilla/5.0")
        response = urllib.request.urlopen(req, timeout=30)
        data = response.read().decode("utf-8-sig")
        reader = csv.reader(io.StringIO(data))
        next(reader)
        ids = set()
        for row in reader:
            if row and row[0].strip():
                ids.add(row[0].strip())
        with _master_lock:
            _master_ticket_ids = ids
            _master_last_refreshed = datetime.now(IST)
    except Exception as e:
        print(f"[Master] Error: {e}")


def get_master_ids():
    with _master_lock:
        if _master_last_refreshed is None or \
                (datetime.now(IST) - _master_last_refreshed).seconds > 1800:
            threading.Thread(target=refresh_master_ids, daemon=True).start()
            if not _master_ticket_ids:
                _master_lock.release()
                time.sleep(3)
                _master_lock.acquire()
        return _master_ticket_ids.copy(), _master_last_refreshed


def make_csv_response(rows, filename):
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    resp = make_response(output.getvalue())
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp


# ---- Import the dashboard HTML generator ----
# We need to import the generate_dashboard_html function
# But it's embedded in dashboard_server.py, so we replicate the import
sys.path.insert(0, os.path.dirname(__file__))
try:
    # Try to import from parent
    from dashboard_server import generate_dashboard_html
except Exception:
    def generate_dashboard_html():
        return "<h1>Dashboard loading error. Check server logs.</h1>"


# ========== ROUTES ==========

@app.route("/")
@app.route("/dashboard")
def dashboard():
    html = generate_dashboard_html()
    return Response(html, mimetype="text/html")


@app.route("/api/dates")
def api_dates():
    return jsonify(get_available_dates())


@app.route("/api/summary")
def api_summary():
    date = request.args.get("date")
    if not date:
        dates = get_available_dates()
        date = dates[0] if dates else None
    if date:
        summary = get_daily_summary(date)
        return jsonify(summary if summary else {"error": "No data"})
    return jsonify({"error": "No data available"}), 404


@app.route("/api/trends")
def api_trends():
    return jsonify(get_all_summaries())


@app.route("/api/tickets")
def api_tickets():
    date = request.args.get("date")
    if date:
        return jsonify(get_tickets_for_date(date))
    return jsonify({"error": "date required"}), 400


@app.route("/api/all-tickets")
def api_all_tickets():
    date = request.args.get("date")
    if date:
        return jsonify(get_all_tickets_for_date(date))
    return jsonify({"error": "date required"}), 400


@app.route("/api/ticket-trail")
def api_ticket_trail():
    ticket_no = request.args.get("ticket_no")
    if ticket_no:
        return jsonify(get_ticket_trail(ticket_no))
    return jsonify({"error": "ticket_no required"}), 400


@app.route("/api/categories")
def api_categories():
    date = request.args.get("date")
    if date:
        return jsonify(get_category_breakdown(date))
    return jsonify({"error": "date required"}), 400


@app.route("/api/download")
def api_download():
    date = request.args.get("date")
    section = request.args.get("section", "all")
    if date:
        tickets = get_all_tickets_for_date(date)
        return make_csv_response(tickets, f"internet_issues_{date}_{section}.csv")
    return jsonify({"error": "date required"}), 400


@app.route("/api/download-filtered")
def api_download_filtered():
    date = request.args.get("date")
    bucket = request.args.get("bucket")
    queue = request.args.get("queue")
    if date:
        tickets = get_all_tickets_for_date(date)
        if bucket:
            tickets = [t for t in tickets if t.get("aging_bucket") == bucket]
        if queue:
            tickets = [t for t in tickets if t.get("current_queue") == queue]
        fname = f"filtered_{date}_{bucket or 'all'}_{queue or 'all'}.csv".replace(" ", "_")
        return make_csv_response(tickets, fname)
    return jsonify({"error": "date required"}), 400


@app.route("/api/master-compare")
def api_master_compare():
    date = request.args.get("date")
    if date:
        summary = get_daily_summary(date)
        if summary and summary.get("master_snapshot_time"):
            new_ids = (summary.get("master_new_ids") or "").split(",")
            new_ids = [x for x in new_ids if x.strip()]
            return jsonify({
                "total_internet": summary["total_internet"],
                "already_in_master": summary.get("master_already", 0),
                "new_to_upload": summary.get("master_new", 0),
                "master_total": summary.get("master_total", 0),
                "master_refreshed": summary.get("master_snapshot_time", ""),
                "snapshot_fixed": True,
                "new_ticket_ids": new_ids,
            })
        else:
            tickets = get_tickets_for_date(date)
            master_ids, refreshed = get_master_ids()
            ticket_ids = [t["ticket_no"] for t in tickets]
            already = [tid for tid in ticket_ids if tid in master_ids]
            new = [tid for tid in ticket_ids if tid not in master_ids]
            return jsonify({
                "total_internet": len(ticket_ids),
                "already_in_master": len(already),
                "new_to_upload": len(new),
                "master_total": len(master_ids),
                "master_refreshed": refreshed.strftime("%Y-%m-%d %H:%M IST") if refreshed else None,
                "snapshot_fixed": False,
                "new_ticket_ids": new,
            })
    return jsonify({"error": "date required"}), 400


@app.route("/api/master-live")
def api_master_live():
    date = request.args.get("date")
    if date:
        tickets = get_tickets_for_date(date)
        master_ids, refreshed = get_master_ids()
        ticket_ids = [t["ticket_no"] for t in tickets]
        already = [tid for tid in ticket_ids if tid in master_ids]
        new = [tid for tid in ticket_ids if tid not in master_ids]
        return jsonify({
            "total_internet": len(ticket_ids),
            "already_in_master": len(already),
            "new_to_upload": len(new),
            "master_total": len(master_ids),
            "master_refreshed": refreshed.strftime("%Y-%m-%d %H:%M IST") if refreshed else None,
            "new_ticket_ids": new,
        })
    return jsonify({"error": "date required"}), 400


@app.route("/api/download-new-tickets")
def api_download_new():
    date = request.args.get("date")
    if date:
        summary = get_daily_summary(date)
        new_ids = set()
        if summary and summary.get("master_new_ids"):
            new_ids = set(x.strip() for x in summary["master_new_ids"].split(",") if x.strip())
        tickets = get_all_tickets_for_date(date)
        if new_ids:
            new_tickets = [t for t in tickets if t.get("ticket_no") in new_ids]
        else:
            master_ids, _ = get_master_ids()
            new_tickets = [t for t in tickets if t.get("ticket_no") not in master_ids]
        return make_csv_response(new_tickets, f"NEW_tickets_to_upload_{date}.csv")
    return jsonify({"error": "date required"}), 400


@app.route("/api/download-existing-tickets")
def api_download_existing():
    date = request.args.get("date")
    if date:
        summary = get_daily_summary(date)
        new_ids = set()
        if summary and summary.get("master_new_ids"):
            new_ids = set(x.strip() for x in summary["master_new_ids"].split(",") if x.strip())
        tickets = get_all_tickets_for_date(date)
        if summary and summary.get("master_snapshot_time"):
            existing = [t for t in tickets if t.get("ticket_no") not in new_ids]
        else:
            master_ids, _ = get_master_ids()
            existing = [t for t in tickets if t.get("ticket_no") in master_ids]
        return make_csv_response(existing, f"existing_tickets_{date}.csv")
    return jsonify({"error": "date required"}), 400


@app.route("/api/download-still-pending")
def api_download_still_pending():
    date = request.args.get("date")
    if date:
        tickets = get_all_tickets_for_date(date)
        master_ids, _ = get_master_ids()
        still_pending = [t for t in tickets if t.get("ticket_no") not in master_ids]
        return make_csv_response(still_pending, f"still_pending_upload_{date}.csv")
    return jsonify({"error": "date required"}), 400


@app.route("/api/refresh-master")
def api_refresh_master():
    threading.Thread(target=refresh_master_ids, daemon=True).start()
    return jsonify({"status": "refreshing"})
