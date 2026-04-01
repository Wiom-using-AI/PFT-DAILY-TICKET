"""
PFT Advanced Dashboard Server
===============================
A local web server with:
- Date picker (d-1, d-2, d-n)
- Customizable filter builder (AND, OR, IF logic)
- Charts: Bar, Pie, Heatmap, Trend lines
- Downloadable CSV/XLSX for every section
- Ticket trail across dates
- All column headers visible
"""

import http.server
import json
import os
import csv
import io
import urllib.parse
import urllib.request
import threading
import time
from datetime import datetime, timezone, timedelta

from history_db import (
    get_available_dates,
    get_daily_summary,
    get_all_summaries,
    get_tickets_for_date,
    get_ticket_trail,
    get_all_tickets_for_date,
    get_category_breakdown,
    get_category_aging_pivot,
    get_full_tickets_by_category_bucket,
    get_summary_range,
    get_category_aging_pivot_range,
    get_category_breakdown_range,
    get_category_daily_trend,
    get_category_l4_daily_trend,
    get_tickets_for_download,
    get_aging_daily_trend,
    get_category_trend_chart,
    init_db,
    AGENT_LIST,
    get_agent_dates,
    save_attendance,
    get_attendance,
    assign_tickets_round_robin,
    get_agent_assignments,
    get_agent_active_tickets,
    get_agent_summary,
    update_agent_ticket,
    reassign_tickets,
)

IST = timezone(timedelta(hours=5, minutes=30))
PORT = int(os.environ.get("PORT", 8091))  # Cloud services set PORT env var
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_SHEET_URL = "https://docs.google.com/spreadsheets/d/1E3Ij57bFHznf3S6cRJSzONaVJ7Tgloud51Z__vXLet0/edit?gid=1626982265#gid=1626982265"
MASTER_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/1E3Ij57bFHznf3S6cRJSzONaVJ7Tgloud51Z__vXLet0/export?format=csv&gid=1626982265"

# ---- Master Sheet Ticket ID Cache ----
_master_ticket_ids = set()
_master_last_refreshed = None
_master_lock = threading.Lock()


def refresh_master_ids():
    """Fetch ticket IDs from master sheet column A."""
    global _master_ticket_ids, _master_last_refreshed
    try:
        req = urllib.request.Request(MASTER_SHEET_CSV_URL)
        req.add_header("User-Agent", "Mozilla/5.0")
        response = urllib.request.urlopen(req, timeout=30)
        data = response.read().decode("utf-8-sig")
        reader = csv.reader(io.StringIO(data))
        next(reader)  # skip header
        ids = set()
        for row in reader:
            if row and row[0].strip():
                ids.add(row[0].strip())
        with _master_lock:
            _master_ticket_ids = ids
            _master_last_refreshed = datetime.now(IST)
        print(f"[Master] Refreshed: {len(ids)} ticket IDs from master sheet")
    except Exception as e:
        print(f"[Master] Error refreshing: {e}")


def get_master_ids():
    """Return cached master IDs, refresh if stale (>30 min)."""
    with _master_lock:
        if _master_last_refreshed is None or \
                (datetime.now(IST) - _master_last_refreshed).seconds > 1800:
            # Refresh in background to not block request
            threading.Thread(target=refresh_master_ids, daemon=True).start()
            # If first load, wait briefly
            if not _master_ticket_ids:
                _master_lock.release()
                time.sleep(3)
                _master_lock.acquire()
        return _master_ticket_ids.copy(), _master_last_refreshed


class DashboardHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send_json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def send_csv(self, rows, filename="export.csv"):
        self.send_response(200)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        output = io.StringIO()
        if rows:
            writer = csv.DictWriter(output, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        self.wfile.write(output.getvalue().encode("utf-8-sig"))

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        if path == "/" or path == "/dashboard":
            self.serve_dashboard()
        elif path == "/api/dates":
            self.send_json(get_available_dates())
        elif path == "/api/summary":
            date = params.get("date", [None])[0]
            if not date:
                dates = get_available_dates()
                date = dates[0] if dates else None
            if date:
                summary = get_daily_summary(date)
                if summary:
                    # Enrich: if total_pending is missing, use full_report count
                    if not summary.get("total_pending"):
                        try:
                            import sqlite3 as _sq
                            _conn = _sq.connect(os.path.join(SCRIPT_DIR, "ticket_history.db"))
                            _c = _conn.cursor()
                            _c.execute("SELECT COUNT(*) FROM full_report_history WHERE report_date = ?", (date,))
                            _fr_count = _c.fetchone()[0]
                            _conn.close()
                            if _fr_count > 0:
                                summary["total_pending"] = _fr_count
                        except Exception:
                            pass
                    # Exclude Router Pickup from total_pending
                    try:
                        import json as _json
                        _cb = _json.loads(summary.get("category_breakdown") or "{}")
                        _router = _cb.get("Router Pickup", 0)
                        if _router and summary.get("total_pending"):
                            summary["total_pending"] = summary["total_pending"] - _router
                    except Exception:
                        pass
                    self.send_json(summary)
                else:
                    self.send_json({"error": "No data"})
            else:
                self.send_json({"error": "No data available"}, 404)
        elif path == "/api/trends":
            self.send_json(get_all_summaries())
        elif path == "/api/tickets":
            date = params.get("date", [None])[0]
            if date:
                tickets = get_tickets_for_date(date)
                self.send_json(tickets)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/all-tickets":
            date = params.get("date", [None])[0]
            if date:
                tickets = get_all_tickets_for_date(date)
                self.send_json(tickets)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/ticket-trail":
            ticket_no = params.get("ticket_no", [None])[0]
            if ticket_no:
                self.send_json(get_ticket_trail(ticket_no))
            else:
                self.send_json({"error": "ticket_no required"}, 400)
        elif path == "/api/download":
            date = params.get("date", [None])[0]
            section = params.get("section", ["all"])[0]
            if date:
                tickets = get_all_tickets_for_date(date)
                fname = f"internet_issues_{date}_{section}.csv"
                self.send_csv(tickets, fname)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/download-filtered":
            # POST body will have filter criteria
            date = params.get("date", [None])[0]
            bucket = params.get("bucket", [None])[0]
            queue = params.get("queue", [None])[0]
            if date:
                tickets = get_all_tickets_for_date(date)
                if bucket:
                    tickets = [t for t in tickets if t.get("aging_bucket") == bucket]
                if queue:
                    tickets = [t for t in tickets if t.get("current_queue") == queue]
                fname = f"filtered_{date}_{bucket or 'all'}_{queue or 'all'}.csv"
                self.send_csv(tickets, fname.replace(" ", "_"))
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/categories":
            date = params.get("date", [None])[0]
            if date:
                cats = get_category_breakdown(date)
                self.send_json(cats)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/category-aging":
            date = params.get("date", [None])[0]
            if date:
                pivot = get_category_aging_pivot(date)
                self.send_json(pivot)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/download-category-bucket":
            date = params.get("date", [None])[0]
            category = params.get("category", [None])[0]
            bucket = params.get("bucket", [None])[0]
            if date:
                tickets = get_full_tickets_by_category_bucket(date, category, bucket)
                cat_safe = (category or "all").replace(" ", "_")
                buck_safe = (bucket or "all").replace(" ", "_")
                fname = f"tickets_{date}_{cat_safe}_{buck_safe}.csv"
                self.send_csv(tickets, fname)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/master-compare":
            date = params.get("date", [None])[0]
            if date:
                # Use STORED snapshot from daily run (fixed, doesn't change)
                summary = get_daily_summary(date)
                if summary and summary.get("master_snapshot_time"):
                    new_ids = (summary.get("master_new_ids") or "").split(",")
                    new_ids = [x for x in new_ids if x.strip()]
                    self.send_json({
                        "total_internet": summary["total_internet"],
                        "already_in_master": summary.get("master_already", 0),
                        "new_to_upload": summary.get("master_new", 0),
                        "master_total": summary.get("master_total", 0),
                        "master_refreshed": summary.get("master_snapshot_time", ""),
                        "snapshot_fixed": True,
                        "new_ticket_ids": new_ids,
                    })
                else:
                    # No snapshot yet — do live comparison (for today before daily run completes)
                    tickets = get_tickets_for_date(date)
                    master_ids, refreshed = get_master_ids()
                    ticket_ids = [t["ticket_no"] for t in tickets]
                    already = [tid for tid in ticket_ids if tid in master_ids]
                    new = [tid for tid in ticket_ids if tid not in master_ids]
                    self.send_json({
                        "total_internet": len(ticket_ids),
                        "already_in_master": len(already),
                        "new_to_upload": len(new),
                        "master_total": len(master_ids),
                        "master_refreshed": refreshed.strftime("%Y-%m-%d %H:%M IST") if refreshed else None,
                        "snapshot_fixed": False,
                        "new_ticket_ids": new,
                    })
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/download-new-tickets":
            date = params.get("date", [None])[0]
            if date:
                # First try cached CSV (frozen at processing time)
                from history_db import get_new_tickets_cache
                cached_csv, cached_count = get_new_tickets_cache(date)
                if cached_csv:
                    fname = f"NEW_tickets_to_upload_{date}.csv"
                    self.send_response(200)
                    self.send_header("Content-Type", "text/csv")
                    self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
                    self.end_headers()
                    self.wfile.write(cached_csv.encode("utf-8"))
                else:
                    # Fallback: generate from snapshot IDs
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
                    fname = f"NEW_tickets_to_upload_{date}.csv"
                    self.send_csv(new_tickets, fname)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/download-existing-tickets":
            date = params.get("date", [None])[0]
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
                fname = f"existing_tickets_{date}.csv"
                self.send_csv(existing, fname)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/master-live":
            date = params.get("date", [None])[0]
            if date:
                tickets = get_tickets_for_date(date)
                master_ids, refreshed = get_master_ids()
                ticket_ids = [t["ticket_no"] for t in tickets]
                already = [tid for tid in ticket_ids if tid in master_ids]
                new = [tid for tid in ticket_ids if tid not in master_ids]
                self.send_json({
                    "total_internet": len(ticket_ids),
                    "already_in_master": len(already),
                    "new_to_upload": len(new),
                    "master_total": len(master_ids),
                    "master_refreshed": refreshed.strftime("%Y-%m-%d %H:%M IST") if refreshed else None,
                    "new_ticket_ids": new,
                })
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/download-still-pending":
            date = params.get("date", [None])[0]
            if date:
                tickets = get_all_tickets_for_date(date)
                master_ids, _ = get_master_ids()
                still_pending = [t for t in tickets if t.get("ticket_no") not in master_ids]
                fname = f"still_pending_upload_{date}.csv"
                self.send_csv(still_pending, fname)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/refresh-master":
            threading.Thread(target=refresh_master_ids, daemon=True).start()
            self.send_json({"status": "refreshing"})
        # ---- Range Aggregation APIs ----
        elif path == "/api/summary/range":
            date_from = params.get("from", [None])[0]
            date_to = params.get("to", [None])[0]
            if date_from and date_to:
                summary = get_summary_range(date_from, date_to)
                if summary:
                    self.send_json(summary)
                else:
                    self.send_json({"error": "No data in range"}, 404)
            else:
                self.send_json({"error": "from and to required"}, 400)
        elif path == "/api/categories/range":
            date_from = params.get("from", [None])[0]
            date_to = params.get("to", [None])[0]
            if date_from and date_to:
                cats = get_category_breakdown_range(date_from, date_to)
                self.send_json(cats)
            else:
                self.send_json({"error": "from and to required"}, 400)
        elif path == "/api/category-aging/range":
            date_from = params.get("from", [None])[0]
            date_to = params.get("to", [None])[0]
            if date_from and date_to:
                pivot = get_category_aging_pivot_range(date_from, date_to)
                self.send_json(pivot)
            else:
                self.send_json({"error": "from and to required"}, 400)
        elif path == "/api/category-daily-trend":
            date_from = params.get("from", [None])[0]
            date_to = params.get("to", [None])[0]
            if date_from and date_to:
                trend = get_category_daily_trend(date_from, date_to)
                self.send_json(trend)
            else:
                self.send_json({"error": "from and to required"}, 400)
        elif path == "/api/pivot-l4-breakdown":
            date = params.get("date", [None])[0]
            l3 = params.get("l3", [None])[0]
            date_from = params.get("from", [None])[0]
            date_to = params.get("to", [None])[0]
            if l3:
                from history_db import get_pivot_l4_breakdown
                self.send_json(get_pivot_l4_breakdown(date, l3, date_from=date_from, date_to=date_to))
            else:
                self.send_json({"error": "l3 required"}, 400)
        elif path == "/api/unique-tickets":
            date_from = params.get("from", [None])[0]
            date_to = params.get("to", [None])[0]
            if date_from and date_to:
                from history_db import get_unique_ticket_counts
                self.send_json(get_unique_ticket_counts(date_from, date_to))
            else:
                self.send_json({"error": "from and to required"}, 400)
        elif path == "/api/aging-daily-trend":
            date_from = params.get("from", [None])[0]
            date_to = params.get("to", [None])[0]
            l3 = params.get("l3", [None])[0]
            l4 = params.get("l4", [None])[0]
            if date_from and date_to:
                self.send_json(get_aging_daily_trend(date_from, date_to, l3_list=l3, l4_list=l4))
            else:
                self.send_json({"error": "from and to required"}, 400)
        elif path == "/api/category-trend-chart":
            date_from = params.get("from", [None])[0]
            date_to = params.get("to", [None])[0]
            buckets = params.get("buckets", [None])[0]
            l3 = params.get("l3", [None])[0]
            l4 = params.get("l4", [None])[0]
            expand_l4 = params.get("expand_l4", ["0"])[0] == "1"
            queue = params.get("queue", [None])[0]
            if date_from and date_to:
                self.send_json(get_category_trend_chart(date_from, date_to, bucket_filter=buckets, l3_filter=l3, l4_filter=l4, expand_l4=expand_l4, queue_filter=queue))
            else:
                self.send_json({"error": "from and to required"}, 400)
        elif path == "/api/category-l4-trend":
            date_from = params.get("from", [None])[0]
            date_to = params.get("to", [None])[0]
            l3 = params.get("l3", [None])[0]
            if date_from and date_to and l3:
                self.send_json(get_category_l4_daily_trend(date_from, date_to, l3))
            else:
                self.send_json({"error": "from, to and l3 required"}, 400)
        elif path == "/api/download-category-tickets":
            date = params.get("date", [None])[0]
            l3 = params.get("l3", [None])[0]
            l4 = params.get("l4", [None])[0]
            if date:
                tickets = get_tickets_for_download(date, l3, l4)
                import csv as csv_mod
                import io as io_mod
                output = io_mod.StringIO()
                if tickets:
                    writer = csv_mod.DictWriter(output, fieldnames=tickets[0].keys())
                    writer.writeheader()
                    writer.writerows(tickets)
                parts = [date, (l3 or "all").replace(" ", "_")]
                if l4:
                    parts.append(l4.replace(" ", "_"))
                fname = f"tickets_{'_'.join(parts)}.csv"
                self.send_response(200)
                self.send_header("Content-Type", "text/csv")
                self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
                self.end_headers()
                self.wfile.write(output.getvalue().encode())
            else:
                self.send_json({"error": "date required"}, 400)
        # ---- Agent Dashboard APIs ----
        elif path == "/agent":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(generate_agent_html().encode())
        elif path == "/api/agent/dates":
            self.send_json(get_agent_dates())
        elif path == "/api/agent/list":
            self.send_json({"agents": AGENT_LIST})
        elif path == "/api/agent/attendance":
            date = params.get("date", [None])[0]
            if date:
                self.send_json(get_attendance(date))
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/agent/assignments":
            date = params.get("date", [None])[0]
            agent = params.get("agent", [None])[0]
            if date:
                rows = get_agent_assignments(date, agent if agent else None)
                self.send_json(rows)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/agent/summary":
            date = params.get("date", [None])[0]
            if date:
                self.send_json(get_agent_summary(date))
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/agent/active-tickets":
            # Get ALL active tickets held by agents: today's own + temp-redistributed from past dates
            date = params.get("date", [None])[0]
            agent = params.get("agent", [None])[0]
            if date:
                rows = get_agent_active_tickets(date, agent if agent else None)
                self.send_json(rows)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/agent/download":
            date = params.get("date", [None])[0]
            agent = params.get("agent", [None])[0]
            if date:
                rows = get_agent_active_tickets(date, agent if agent else None)
                aname = (agent or "all").replace(" ", "_")
                self.send_csv(rows, f"agent_tickets_{date}_{aname}.csv")
            else:
                self.send_json({"error": "date required"}, 400)
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len) if content_len else b""
        try:
            data = json.loads(body) if body else {}
        except json.JSONDecodeError:
            data = {}

        if path == "/api/agent/save-attendance":
            date = data.get("date")
            present = data.get("present", [])
            if date:
                result = save_attendance(date, present)
                self.send_json(result)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/agent/assign":
            date = data.get("date")
            present = data.get("present")
            if date:
                result = assign_tickets_round_robin(date, present)
                self.send_json(result)
            else:
                self.send_json({"error": "date required"}, 400)
        elif path == "/api/agent/reassign":
            date = data.get("date")
            present = data.get("present", [])
            if date and present:
                result = reassign_tickets(date, present)
                self.send_json(result)
            else:
                self.send_json({"error": "date and present agents required"}, 400)
        elif path == "/api/agent/update-ticket":
            date = data.get("date")
            ticket_no = data.get("ticket_no")
            updates = data.get("updates", {})
            if date and ticket_no:
                update_agent_ticket(date, ticket_no, updates)
                self.send_json({"status": "ok"})
            else:
                self.send_json({"error": "date and ticket_no required"}, 400)
        else:
            self.send_response(404)
            self.end_headers()

    def serve_dashboard(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        html = generate_dashboard_html()
        self.wfile.write(html.encode())


def generate_dashboard_html():
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PFT Pending Ticket Tracker</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
  :root {{
    --bg:#f5f7fa; --card:#ffffff; --card2:#f0f2f5; --text:#1a1a2e;
    --text2:#6b7280; --accent:#1a73e8; --green:#0d9f6e; --red:#dc2626;
    --orange:#ea580c; --yellow:#ca8a04; --border:#e5e7eb;
    --shadow:0 1px 3px rgba(0,0,0,.08),0 1px 2px rgba(0,0,0,.04);
    --shadow-md:0 4px 6px rgba(0,0,0,.07),0 2px 4px rgba(0,0,0,.04);
  }}
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{font-family:'Inter',system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--text);padding:20px 24px;font-size:13px}}

  .header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;flex-wrap:wrap;gap:10px;
    background:var(--card);padding:16px 20px;border-radius:12px;box-shadow:var(--shadow);border:1px solid var(--border)}}
  .header h1{{font-size:18px;font-weight:700;color:var(--text)}}
  .header h1 span{{color:var(--accent)}}
  .header-right{{display:flex;gap:8px;align-items:center;flex-wrap:wrap}}
  .btn{{padding:7px 14px;border-radius:8px;text-decoration:none;font-size:11px;font-weight:600;
    border:1px solid var(--border);color:var(--text2);background:var(--card);cursor:pointer;transition:all .2s;
    box-shadow:0 1px 2px rgba(0,0,0,.04)}}
  .btn:hover{{background:var(--accent);border-color:var(--accent);color:#fff}}
  .btn-primary{{background:var(--accent);border-color:var(--accent);color:#fff}}
  .btn-primary:hover{{background:#1557b0}}
  .btn-sm{{padding:4px 10px;font-size:10px}}
  .btn-download{{background:#ecfdf5;border-color:#a7f3d0;color:var(--green)}}
  .btn-download:hover{{background:var(--green);color:#fff;border-color:var(--green)}}

  .chart-dd-item{{padding:8px 14px;font-size:12px;cursor:pointer;transition:background .1s;color:var(--text);white-space:nowrap}}
  .chart-dd-item:hover{{background:#f0f4ff;color:#4338ca}}
  .chart-dd-item.active{{background:#eef2ff;color:#4338ca;font-weight:600}}

  .date-nav{{display:flex;align-items:center;gap:6px;background:var(--card);padding:10px 16px;border-radius:10px;
    border:1px solid var(--border);margin-bottom:18px;flex-wrap:wrap;box-shadow:var(--shadow)}}
  .date-nav label{{font-size:11px;color:var(--text2);text-transform:uppercase;letter-spacing:1px;font-weight:600}}
  .date-btn{{padding:6px 14px;border-radius:6px;border:1px solid var(--border);background:var(--card2);
    color:var(--text);font-size:11px;cursor:pointer;font-weight:600;transition:all .15s}}
  .date-btn:hover,.date-btn.active{{background:var(--accent);border-color:var(--accent);color:#fff}}
  .date-select{{padding:6px 10px;border-radius:6px;border:1px solid var(--border);background:var(--card);
    color:var(--text);font-size:11px;cursor:pointer}}
  .date-info{{font-size:12px;color:var(--text2);margin-left:auto;font-weight:500}}

  .cards{{display:grid;grid-template-columns:repeat(7,1fr);gap:10px;margin-bottom:18px}}
  .card{{background:var(--card);border-radius:8px;padding:10px 10px 8px;border:1px solid var(--border);box-shadow:var(--shadow);
    transition:box-shadow .2s;min-width:0}}
  .card:hover{{box-shadow:var(--shadow-md)}}
  .card-label{{font-size:9px;color:var(--text2);text-transform:uppercase;letter-spacing:.6px;margin-bottom:4px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
  .card-value{{font-size:22px;font-weight:700;letter-spacing:-0.5px}}
  .card-value.green{{color:var(--green)}} .card-value.red{{color:var(--red)}}
  .card-value.orange{{color:var(--orange)}} .card-value.blue{{color:var(--accent)}}
  .card-sub{{font-size:9px;color:var(--text2);margin-top:2px}}
  .card-delta{{font-size:9px;margin-top:2px;font-weight:600}}
  .card-delta.up{{color:var(--red)}} .card-delta.down{{color:var(--green)}} .card-delta.neutral{{color:var(--text2)}}

  .section{{background:var(--card);border-radius:10px;padding:18px;border:1px solid var(--border);margin-bottom:18px;
    box-shadow:var(--shadow)}}
  .section-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px}}
  .section h3{{font-size:13px;color:var(--text);font-weight:700;letter-spacing:0}}
  .charts{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:18px}}
  .chart-card{{background:var(--card);border-radius:10px;padding:18px;border:1px solid var(--border);box-shadow:var(--shadow)}}
  .chart-card .section-header{{margin-bottom:12px}}
  .chart-card h3{{font-size:13px;color:var(--text);font-weight:700}}
  .chart-container{{position:relative;height:260px}}
  .full-width{{grid-column:1/-1}}

  table{{width:100%;border-collapse:separate;border-spacing:0;font-size:12px}}
  th{{text-align:left;padding:10px 12px;background:var(--card2);color:var(--text2);font-weight:600;
    font-size:10px;text-transform:uppercase;letter-spacing:.5px;border-bottom:2px solid var(--border);
    cursor:pointer;user-select:none;white-space:nowrap}}
  th:first-child{{border-radius:8px 0 0 0}} th:last-child{{border-radius:0 8px 0 0}}
  th:hover{{color:var(--accent)}}
  td{{padding:10px 12px;border-bottom:1px solid var(--border)}}
  tr:hover{{background:#f0f7ff}}
  .num{{text-align:center;font-variant-numeric:tabular-nums;font-weight:600}}
  .total{{font-weight:700;color:var(--accent)}}
  .dot{{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px}}
  .bar-bg{{background:var(--card2);border-radius:4px;height:18px;width:100%;overflow:hidden}}
  .bar-fill{{height:100%;border-radius:4px;transition:width .5s}}
  .badge{{padding:3px 8px;border-radius:6px;font-size:10px;font-weight:700}}
  .badge-red{{background:#fef2f2;color:#dc2626;border:1px solid #fecaca}}
  .badge-orange{{background:#fff7ed;color:#ea580c;border:1px solid #fed7aa}}
  .badge-yellow{{background:#fefce8;color:#ca8a04;border:1px solid #fef08a}}
  .badge-green{{background:#ecfdf5;color:#0d9f6e;border:1px solid #a7f3d0}}
  .clickable{{cursor:pointer;color:var(--accent);text-decoration:none;font-weight:600}}
  .clickable:hover{{text-decoration:underline}}

  /* Filter Builder */
  .filter-panel{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:18px;margin-bottom:18px;
    box-shadow:var(--shadow)}}
  .filter-panel h3{{font-size:13px;color:var(--text);font-weight:700;margin-bottom:12px}}
  .filter-row{{display:flex;gap:6px;align-items:center;margin-bottom:8px;flex-wrap:wrap}}
  .filter-row select,.filter-row input{{padding:6px 10px;border-radius:6px;border:1px solid var(--border);
    background:var(--card);color:var(--text);font-size:11px;font-family:'Inter',sans-serif}}
  .filter-row select{{min-width:130px}}
  .filter-row input{{min-width:110px}}
  .filter-logic{{padding:4px 10px;border-radius:6px;font-size:10px;font-weight:700;cursor:pointer;border:1px solid var(--border);
    background:var(--card2);color:var(--accent)}}
  .filter-logic.and{{color:var(--green);border-color:#a7f3d0;background:#ecfdf5}}
  .filter-logic.or{{color:var(--orange);border-color:#fed7aa;background:#fff7ed}}
  .filter-results{{font-size:12px;color:var(--text2);margin-top:10px;padding:8px 12px;background:var(--card2);border-radius:6px}}
  .filter-results strong{{color:var(--accent)}}

  /* Heatmap */
  .heatmap-grid{{display:grid;gap:2px;font-size:11px}}
  .heatmap-cell{{padding:6px;text-align:center;border-radius:4px;font-weight:600;cursor:pointer;transition:transform .1s}}
  .heatmap-cell:hover{{transform:scale(1.05);outline:2px solid var(--accent)}}
  .heatmap-header{{font-weight:700;color:var(--text2);background:transparent !important;font-size:10px}}

  /* Tabs */
  .tabs{{display:flex;gap:4px;margin-bottom:14px}}
  .tab{{padding:6px 14px;border-radius:6px;border:1px solid var(--border);background:var(--card);
    color:var(--text2);font-size:11px;cursor:pointer;font-weight:600}}
  .tab.active{{background:var(--accent);border-color:var(--accent);color:#fff}}

  /* Modal */
  .modal-overlay{{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.4);
    backdrop-filter:blur(4px);z-index:100;align-items:center;justify-content:center}}
  .modal-overlay.show{{display:flex}}
  .show{{display:block!important}}
  .modal{{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:24px;
    max-width:90vw;width:950px;max-height:85vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.15)}}
  .modal h2{{font-size:16px;margin-bottom:16px;font-weight:700;color:var(--text)}}
  .modal-close{{float:right;cursor:pointer;font-size:20px;color:var(--text2);background:var(--card2);border:none;
    width:32px;height:32px;border-radius:8px;display:flex;align-items:center;justify-content:center}}
  .modal-close:hover{{background:var(--border);color:var(--text)}}

  .table-scroll{{overflow-x:auto}}
  @media(max-width:900px){{.charts{{grid-template-columns:1fr}}}}
  @media(max-width:600px){{.cards{{grid-template-columns:1fr 1fr}}}}
  .loading{{text-align:center;padding:30px;color:var(--text2);font-weight:500}}
  .pill{{display:inline-block;padding:3px 10px;border-radius:12px;font-size:10px;font-weight:600;margin:1px}}

  /* Dashboard Section Customization */
  .dashboard-section{{position:relative;transition:opacity .3s,transform .3s}}
  .dashboard-section.dragging{{opacity:.5;transform:scale(.98)}}
  .dashboard-section.drag-over{{border-top:3px solid var(--accent);margin-top:-3px}}
  .section-toolbar{{display:flex;gap:4px;justify-content:flex-end;align-items:center;
    height:32px;padding:0 10px;background:#f8fafc;border-bottom:1px solid var(--border);
    border-radius:10px 10px 0 0;margin:-18px -18px 12px -18px}}
  .section-toolbar button{{width:28px;height:28px;border-radius:6px;border:1px solid transparent;background:transparent;
    color:#b0b8c4;cursor:pointer;font-size:13px;display:flex;align-items:center;justify-content:center;transition:all .15s;padding:0;line-height:1}}
  .section-toolbar button:hover{{background:#e2e8f0;border-color:var(--border);color:var(--text)}}
  .section-toolbar button.remove-btn:hover{{background:#fef2f2;border-color:#fecaca;color:var(--red)}}
  .dashboard-section[draggable="true"]{{cursor:default}}
  .dashboard-section[draggable="true"] .section-header{{cursor:grab}}
  .dashboard-section[draggable="true"] .section-header:active{{cursor:grabbing}}

  /* Removed Templates Drawer */
  .hidden-drawer{{position:fixed;bottom:0;left:20px;z-index:90;font-family:'Inter',system-ui,sans-serif}}
  .hidden-drawer-toggle{{padding:8px 16px;background:var(--card);border:1px solid var(--border);border-bottom:none;
    border-radius:10px 10px 0 0;cursor:pointer;font-size:12px;font-weight:600;color:var(--text2);
    box-shadow:var(--shadow);display:flex;align-items:center;gap:6px;transition:all .2s;user-select:none}}
  .hidden-drawer-toggle:hover{{background:#f1f5f9;color:var(--text)}}
  .hidden-drawer-panel{{background:var(--card);border:1px solid var(--border);border-bottom:none;border-radius:10px 10px 0 0;
    box-shadow:var(--shadow-md);max-height:0;overflow:hidden;transition:max-height .3s ease,padding .3s ease;padding:0 16px}}
  .hidden-drawer-panel.open{{max-height:300px;padding:12px 16px;overflow-y:auto}}
  .hidden-drawer-item{{display:flex;align-items:center;justify-content:space-between;padding:8px 0;
    border-bottom:1px solid var(--border);font-size:12px}}
  .hidden-drawer-item:last-child{{border-bottom:none}}
  .hidden-drawer-item span{{color:var(--text);font-weight:500}}
  .hidden-drawer-item button{{padding:4px 12px;border-radius:6px;border:1px solid #a7f3d0;background:#ecfdf5;
    color:var(--green);font-size:11px;font-weight:600;cursor:pointer;transition:all .15s}}
  .hidden-drawer-item button:hover{{background:var(--green);color:#fff;border-color:var(--green)}}
</style>
</head>
<body>

<div class="header">
  <h1><span>PFT</span> Pending Ticket Tracker</h1>
  <div class="header-right">
    <a href="/agent" class="btn btn-primary" style="background:#7c3aed;border-color:#7c3aed">&#128101; Agent Dashboard</a>
    <a href="{MASTER_SHEET_URL}" target="_blank" class="btn btn-primary">&#128196; Master Sheet</a>
    <button class="btn btn-download" onclick="downloadAll()">&#11015; Download All Data</button>
    <button class="btn" onclick="window.print()">&#128424; Print</button>
    <button class="btn" onclick="openRulesModal()" style="font-size:11px;padding:4px 10px">&#128220; Rules</button>
  </div>
</div>

<!-- Rules Modal -->
<div id="rulesModal" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.5);z-index:10000;justify-content:center;align-items:center">
  <div style="background:#fff;width:90%;max-width:800px;max-height:85vh;border-radius:12px;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,0.3);display:flex;flex-direction:column">
    <div style="padding:16px 24px;border-bottom:1px solid #e5e7eb;display:flex;justify-content:space-between;align-items:center;background:#f8fafc">
      <h2 style="margin:0;font-size:16px;color:#1a1a2e">&#128220; PFT Agent Dashboard &mdash; Rules &amp; Configuration</h2>
      <button onclick="closeRulesModal()" style="background:none;border:none;font-size:22px;cursor:pointer;color:#666;padding:0 4px">&times;</button>
    </div>
    <div style="padding:24px;overflow-y:auto;font-size:13px;line-height:1.7;color:#333" id="rulesContent">

      <h3 style="color:#1a1a2e;border-bottom:2px solid #3b82f6;padding-bottom:4px;margin-top:0">&#128260; Daily Agent Execution</h3>
      <ol>
        <li><b>Trigger:</b> Runs daily, checks Gmail for the morning pending report email</li>
        <li><b>Email source:</b> <code>no-reply-report@kapturecrm.com</code></li>
        <li><b>Subject match:</b> "Queue wise pending report last 60 days"</li>
        <li><b>Only first email:</b> If multiple emails arrive with same subject, only the FIRST one is used &mdash; later ones are IGNORED</li>
        <li><b>Retry logic:</b> If email hasn't arrived, retry every <b>5 minutes</b> until <b>12:00 PM IST</b></li>
        <li><b>Deadline:</b> If no email by 12:00 PM, stop retrying and log a failure message</li>
      </ol>

      <h3 style="color:#1a1a2e;border-bottom:2px solid #10b981;padding-bottom:4px">&#128451; Data Storage Rules</h3>
      <ol start="7">
        <li><b>Ticket-level data (raw rows):</b> Keep for <b>7 days</b> only, then auto-delete</li>
        <li><b>Daily summary numbers:</b> Keep <b>forever</b> (infinite retention) &mdash; only ~1 KB per day</li>
        <li><b>Router Pickup:</b> Do NOT store individual ticket rows &mdash; only keep the <b>daily count</b> in category breakdown</li>
        <li><b>All other categories</b> (Internet Issues, Refund, Payment Issues, etc.): Store full ticket-level data</li>
        <li><b>New Tickets CSV cache:</b> Save at processing time, available for download until <b>11:59 PM</b> that day, then auto-delete</li>
        <li><b>Database cleanup:</b> Runs after each daily agent execution</li>
      </ol>

      <h3 style="color:#1a1a2e;border-bottom:2px solid #f59e0b;padding-bottom:4px">&#128202; Dashboard &mdash; KPI Summary Cards</h3>
      <ol start="13">
        <li><b>7 cards:</b> Total Pending, Internet Issues, Created on Report Day, Critical (&gt;48h), Partner Queue, CX High Pain, PX-Send to Wiom</li>
        <li><b>Single day view:</b> Show raw numbers</li>
        <li><b>Multi-day view:</b> Aggregation options &mdash; <b>Average</b> (default), Sum, Median, Min, Max, Unique</li>
        <li><b>Unique mode:</b> Shows deduplicated ticket count + % of total sum</li>
        <li><b>Delta comparison:</b> Show change vs previous period's average</li>
      </ol>

      <h3 style="color:#1a1a2e;border-bottom:2px solid #8b5cf6;padding-bottom:4px">&#128200; Ticket Bifurcation (Category Summary)</h3>
      <ol start="18">
        <li><b>% values:</b> Center-aligned in table cells</li>
        <li><b>Expandable L4 sub-rows:</b> Click on any L3 category to expand Level 4 breakdown</li>
        <li><b>L4 contribution %:</b> Calculated on the <b>category total</b> (not grand total)</li>
        <li><b>Click-to-download:</b> Clicking any % value downloads a raw CSV of those tickets</li>
        <li><b>Filter:</b> Multi-category filter with checkbox dropdown</li>
      </ol>

      <h3 style="color:#1a1a2e;border-bottom:2px solid #ef4444;padding-bottom:4px">&#9203; Ticket Aging Breakdown</h3>
      <ol start="23">
        <li><b>Separate independent section</b> (not inside another section)</li>
        <li><b>Display format:</b> Number on top (bold), % below it (small gray text)</li>
        <li><b>Date range:</b> Show last 7 days of data including today</li>
        <li><b>Filters:</b> Same date range + filter options as Category Summary</li>
        <li><b>L3/L4 multi-select:</b> Checkbox dropdown filters for Disposition Folder Level 3 and Level 4</li>
        <li><b>Combined filtering:</b> Can select multiple L3 + multiple L4 categories together</li>
        <li><b>TOTAL row updates:</b> Recalculates based on selected filters</li>
        <li><b>Draggable/movable:</b> Section can be reordered like other dashboard sections</li>
        <li><b>No distribution column:</b> Bar chart column removed</li>
      </ol>

      <h3 style="color:#1a1a2e;border-bottom:2px solid #6b7280;padding-bottom:4px">&#128465; Removed Sections</h3>
      <ol start="32">
        <li>Aging Distribution chart &mdash; Removed</li>
        <li>Queue Split doughnut chart &mdash; Removed</li>
        <li>Queue x Aging Heatmap &mdash; Removed</li>
        <li>Daily Trend line chart &mdash; Removed</li>
      </ol>

      <h3 style="color:#1a1a2e;border-bottom:2px solid #0ea5e9;padding-bottom:4px">&#128203; Master Sheet Comparison</h3>
      <ol start="36">
        <li><b>Snapshot is FIXED</b> at daily run time &mdash; does not change throughout the day</li>
        <li><b>New Tickets CSV:</b> Cached in database at processing time, always downloadable even if master sheet was manually updated later</li>
        <li><b>Live Upload Status:</b> "Check Now" button fetches current master sheet state for live comparison</li>
        <li><b>Master sheet URL:</b> Google Sheets export as CSV</li>
        <li><b>Comparison by:</b> ticket_no (column A of master sheet)</li>
      </ol>

      <h3 style="color:#1a1a2e;border-bottom:2px solid #14b8a6;padding-bottom:4px">&#9881; Processing Pipeline (Order)</h3>
      <ol start="41">
        <li>Search Gmail for today's email</li>
        <li>Download the full pending report (.xlsx)</li>
        <li>Filter Internet Issues tickets &mdash; save filtered file</li>
        <li>Save daily snapshot to database (ticket_history + daily_summary)</li>
        <li>Extract category breakdown + save full report (all categories except Router Pickup)</li>
        <li>Fetch master sheet &rarr; compare &rarr; save snapshot &rarr; cache new tickets CSV</li>
        <li>Cleanup old data (7-day ticket purge + expired cache removal)</li>
      </ol>

      <h3 style="color:#1a1a2e;border-bottom:2px solid #e11d48;padding-bottom:4px">&#128231; Gmail Configuration</h3>
      <ol start="48">
        <li><b>Gmail account:</b> avakash.gupta@wiom.in</li>
        <li><b>Authentication:</b> Gmail App Password (stored as GMAIL_APP_PASSWORD env variable)</li>
        <li><b>Protocol:</b> IMAP (imap.gmail.com:993)</li>
      </ol>

      <h3 style="color:#1a1a2e;border-bottom:2px solid #d946ef;padding-bottom:4px">&#128337; Aging Calculation</h3>
      <ol start="51">
        <li><b>Aging is calculated from:</b> The time the email/report came (report_time_ist), NOT from current time</li>
        <li><b>Formula:</b> pending_hours = (report_time_ist - ticket_created_datetime) / 3600</li>
        <li><b>Buckets:</b> 0-12h, 12-24h, 24-36h, 36-48h, 48-72h, 72-120h, &gt;120h</li>
      </ol>

      <h3 style="color:#1a1a2e;border-bottom:2px solid #f97316;padding-bottom:4px">&#127760; Deployment</h3>
      <ol start="54">
        <li><b>Platform:</b> Vercel (auto-deploys from GitHub push)</li>
        <li><b>URL:</b> pft-daily-ticket.vercel.app</li>
        <li><b>Database:</b> SQLite file stored in repo (via Git LFS for files &gt;100 MB)</li>
        <li><b>Local server:</b> Available via dashboard_server.py for local testing</li>
      </ol>

    </div>
  </div>
</div>

<!-- Date Navigation -->
<div class="date-nav" id="dateNav">
  <label>View:</label>
  <button type="button" class="date-btn" onclick="navigateDate('latest')">Today</button>
  <button type="button" class="date-btn" onclick="navigateDate(-1)">D-1</button>
  <button type="button" class="date-btn" onclick="navigateDate(-2)">D-2</button>
  <button type="button" class="date-btn" onclick="navigateDate(-3)">D-3</button>
  <span style="width:1px;height:20px;background:var(--border);margin:0 2px"></span>
  <button type="button" class="date-btn" onclick="navigatePeriod('wk',0)">Wk-0</button>
  <button type="button" class="date-btn" onclick="navigatePeriod('wk',1)">Wk-1</button>
  <button type="button" class="date-btn" onclick="navigatePeriod('wk',2)">Wk-2</button>
  <button type="button" class="date-btn" onclick="navigatePeriod('wk',3)">Wk-3</button>
  <span style="width:1px;height:20px;background:var(--border);margin:0 2px"></span>
  <button type="button" class="date-btn" onclick="navigatePeriod('m',0)">M-0</button>
  <button type="button" class="date-btn" onclick="navigatePeriod('m',1)">M-1</button>
  <button type="button" class="date-btn" onclick="navigatePeriod('m',2)">M-2</button>
  <span style="width:1px;height:20px;background:var(--border);margin:0 4px"></span>
  <label style="font-size:10px;color:var(--text2);font-weight:600">From:</label>
  <input type="date" id="dateFrom" class="date-select" style="padding:5px 8px;font-size:11px">
  <label style="font-size:10px;color:var(--text2);font-weight:600">To:</label>
  <input type="date" id="dateTo" class="date-select" style="padding:5px 8px;font-size:11px">
  <button type="button" class="date-btn" onclick="applyDateRange()" style="background:var(--accent);color:#fff;border-color:var(--accent);padding:5px 12px">Apply</button>
  <span class="date-info" id="dateInfo">Loading...</span>
</div>

<!-- Summary Cards -->
<div class="dashboard-section" data-section-id="summaryCards" data-section-label="Summary Cards" draggable="true">
<div class="section">
  <div class="section-toolbar">
    <button onclick="moveSectionUp(this.closest('.dashboard-section'))" title="Move up">&#11014;</button>
    <button onclick="moveSectionDown(this.closest('.dashboard-section'))" title="Move down">&#11015;</button>
    <button class="remove-btn" onclick="hideSection(this.closest('.dashboard-section'))" title="Remove section">&#10005;</button>
  </div>
  <div id="aggModeBar" style="display:none;padding:6px 12px 2px;margin-bottom:2px">
    <span style="font-size:10px;font-weight:600;color:var(--text2);margin-right:8px">SHOW AS:</span>
    <button class="date-btn agg-btn active" data-agg="avg" onclick="setAggMode('avg',this)">Average</button>
    <button class="date-btn agg-btn" data-agg="sum" onclick="setAggMode('sum',this)">Sum</button>
    <button class="date-btn agg-btn" data-agg="median" onclick="setAggMode('median',this)">Median</button>
    <button class="date-btn agg-btn" data-agg="min" onclick="setAggMode('min',this)">Min</button>
    <button class="date-btn agg-btn" data-agg="max" onclick="setAggMode('max',this)">Max</button>
    <button class="date-btn agg-btn" data-agg="unique" onclick="setAggMode('unique',this)">Unique</button>
  </div>
  <div class="cards" id="summaryCards"><div class="loading">Loading...</div></div>
</div>
</div>

<!-- Ticket Bifurcation (Pivot Table) -->
<div class="dashboard-section" data-section-id="categorySection" data-section-label="Ticket Bifurcation" draggable="true">
<div class="section" id="categorySection">
  <div class="section-toolbar">
    <button onclick="moveSectionUp(this.closest('.dashboard-section'))" title="Move up">&#11014;</button>
    <button onclick="moveSectionDown(this.closest('.dashboard-section'))" title="Move down">&#11015;</button>
    <button class="remove-btn" onclick="hideSection(this.closest('.dashboard-section'))" title="Remove section">&#10005;</button>
  </div>
  <div class="section-header" style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">
    <h3>&#128202; Ticket Bifurcation — All Categories from Email Report</h3>
    <div id="pivotFilterContainer"></div>
  </div>
  <div id="pivotContent">
    <div class="loading">Loading...</div>
  </div>
</div>
</div>

<!-- Category Summary — Daily Trend -->
<div class="dashboard-section" data-section-id="categorySummary" data-section-label="Category Summary" draggable="true">
<div class="section" id="categorySummarySection">
  <div class="section-toolbar">
    <button onclick="moveSectionUp(this.closest('.dashboard-section'))" title="Move up">&#11014;</button>
    <button onclick="moveSectionDown(this.closest('.dashboard-section'))" title="Move down">&#11015;</button>
    <button class="remove-btn" onclick="hideSection(this.closest('.dashboard-section'))" title="Remove section">&#10005;</button>
  </div>
  <div class="section-header" style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">
    <h3>&#128202; Category Summary &mdash; Daily Trend</h3>
    <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
      <div id="catTrendFilterContainer" style="margin-right:8px"></div>
      <label style="font-size:11px;color:var(--text2);font-weight:600">FROM</label>
      <input type="date" id="catTrendFrom" style="padding:4px 8px;border:1px solid var(--border);border-radius:6px;font-size:11px;font-family:inherit">
      <label style="font-size:11px;color:var(--text2);font-weight:600">TO</label>
      <input type="date" id="catTrendTo" style="padding:4px 8px;border:1px solid var(--border);border-radius:6px;font-size:11px;font-family:inherit">
      <button class="btn btn-sm btn-primary" onclick="applyCatTrendFilter()">Apply</button>
    </div>
  </div>
  <div id="categorySummaryContent">
    <div class="loading">Loading...</div>
  </div>
</div>
</div>

<!-- Master Sheet Comparison -->
<div class="dashboard-section" data-section-id="masterComparison" data-section-label="Master Sheet Comparison" draggable="true">
<div class="section" id="masterSection" style="background:linear-gradient(135deg,#f0f7ff,#e8f0fe);border-left:3px solid var(--accent)">
  <div class="section-toolbar">
    <button onclick="moveSectionUp(this.closest('.dashboard-section'))" title="Move up">&#11014;</button>
    <button onclick="moveSectionDown(this.closest('.dashboard-section'))" title="Move down">&#11015;</button>
    <button class="remove-btn" onclick="hideSection(this.closest('.dashboard-section'))" title="Remove section">&#10005;</button>
  </div>
  <div class="section-header">
    <h3>&#128203; Master Sheet Comparison</h3>
    <div style="display:flex;gap:6px;align-items:center">
      <span id="masterRefreshInfo" style="font-size:10px;color:var(--text2)"></span>
      <button class="btn btn-sm" onclick="refreshMaster()">&#8635; Refresh</button>
      <a href="{MASTER_SHEET_URL}" target="_blank" class="btn btn-sm btn-primary">Open Master Sheet</a>
    </div>
  </div>
  <div id="masterContent"><div class="loading">Comparing with master sheet...</div></div>
</div>
</div>

<!-- Custom Filter Builder -->
<div class="filter-panel" id="filterPanel">
  <div class="section-header">
    <h3>&#9881; Custom Filter Builder (AND / OR / IF)</h3>
    <div>
      <button class="btn btn-sm" onclick="addFilterRow()">+ Add Rule</button>
      <button class="btn btn-sm btn-primary" onclick="applyFilters()">Apply Filters</button>
      <button class="btn btn-sm" onclick="resetFilters()">Reset</button>
      <button class="btn btn-sm btn-download" onclick="downloadFiltered()">&#11015; Download Filtered</button>
    </div>
  </div>
  <div id="filterRows"></div>
  <div class="filter-results" id="filterResults"></div>
</div>

<!-- Aging Daily Trend -->
<div class="dashboard-section" data-section-id="agingTrend" data-section-label="Aging Breakdown" draggable="true">
<div class="section" id="agingTrendSection">
  <div class="section-toolbar">
    <button onclick="moveSectionUp(this.closest('.dashboard-section'))" title="Move up">&#11014;</button>
    <button onclick="moveSectionDown(this.closest('.dashboard-section'))" title="Move down">&#11015;</button>
    <button class="remove-btn" onclick="hideSection(this.closest('.dashboard-section'))" title="Remove section">&#10005;</button>
  </div>
  <div class="section-header" style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">
    <h3>&#9200; Ticket Aging &mdash; Daily Trend</h3>
    <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
      <div id="agingTrendFilterContainer" style="margin-right:8px"></div>
      <label style="font-size:11px;color:var(--text2);font-weight:600">FROM</label>
      <input type="date" id="agingTrendFrom" style="padding:4px 8px;border:1px solid var(--border);border-radius:6px;font-size:11px;font-family:inherit">
      <label style="font-size:11px;color:var(--text2);font-weight:600">TO</label>
      <input type="date" id="agingTrendTo" style="padding:4px 8px;border:1px solid var(--border);border-radius:6px;font-size:11px;font-family:inherit">
      <button class="btn btn-sm btn-primary" onclick="applyAgingTrendFilter()">Apply</button>
    </div>
  </div>
  <div id="agingTrendContent">
    <div class="loading">Loading...</div>
  </div>

</div>
</div>

<!-- Aging Trend Chart (Independent Section) -->
<div class="dashboard-section" data-section-id="agingChart" data-section-label="Aging Trend Chart" draggable="true">
<div class="section" id="agingChartSection">
  <div class="section-toolbar">
    <button onclick="moveSectionUp(this.closest('.dashboard-section'))" title="Move up">&#11014;</button>
    <button onclick="moveSectionDown(this.closest('.dashboard-section'))" title="Move down">&#11015;</button>
    <button class="remove-btn" onclick="hideSection(this.closest('.dashboard-section'))" title="Remove section">&#10005;</button>
  </div>
  <div class="section-header" style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">
    <h3>&#128202; Ticket Trend Chart</h3>
    <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
      <div id="chartBucketFilterContainer" style="margin-right:4px"></div>
      <div id="chartL3Container" style="margin-right:4px"></div>
      <div id="chartL4Container" style="margin-right:4px"></div>
      <div id="chartQueueContainer" style="margin-right:4px"></div>
      <label style="font-size:11px;color:var(--text2);font-weight:600">FROM</label>
      <input type="date" id="chartFrom" style="padding:4px 8px;border:1px solid var(--border);border-radius:6px;font-size:11px;font-family:inherit">
      <label style="font-size:11px;color:var(--text2);font-weight:600">TO</label>
      <input type="date" id="chartTo" style="padding:4px 8px;border:1px solid var(--border);border-radius:6px;font-size:11px;font-family:inherit">
      <button class="btn btn-sm btn-primary" onclick="applyChartFilter()">Apply</button>
      <div id="agingChartTypeContainer" style="position:relative;display:inline-block">
        <button onclick="document.getElementById('agingChartDropdown').classList.toggle('show')"
          style="padding:5px 12px;border:1px solid var(--border);border-radius:6px;background:#fff;cursor:pointer;font-size:11px;font-family:inherit;display:flex;align-items:center;gap:6px">
          &#128202; <span id="agingChartLabel">Line</span> &#9660;</button>
        <div id="agingChartDropdown" style="display:none;position:absolute;right:0;top:100%;margin-top:4px;background:#fff;border:1px solid var(--border);border-radius:8px;box-shadow:0 4px 16px rgba(0,0,0,0.12);z-index:100;min-width:180px;overflow:hidden">
          <div class="chart-dd-item" data-ctype="bar" onclick="pickAgingChart('bar','Column',this)">&#9642; Column</div>
          <div class="chart-dd-item" data-ctype="stackedBar" onclick="pickAgingChart('stackedBar','Stacked Column',this)">&#9642; Stacked Column</div>
          <div class="chart-dd-item" data-ctype="percent" onclick="pickAgingChart('percent','100% Stacked',this)">&#9642; 100% Stacked</div>
          <div class="chart-dd-item active" data-ctype="line" onclick="pickAgingChart('line','Line',this)">&#9642; Line</div>
          <div class="chart-dd-item" data-ctype="area" onclick="pickAgingChart('area','Area',this)">&#9642; Area</div>
          <div class="chart-dd-item" data-ctype="stackedArea" onclick="pickAgingChart('stackedArea','Stacked Area',this)">&#9642; Stacked Area</div>
          <div class="chart-dd-item" data-ctype="combo" onclick="pickAgingChart('combo','Line + Column',this)">&#9642; Line + Column</div>
          <div class="chart-dd-item" data-ctype="pie" onclick="pickAgingChart('pie','Pie',this)">&#9642; Pie</div>
          <div class="chart-dd-item" data-ctype="doughnut" onclick="pickAgingChart('doughnut','Doughnut',this)">&#9642; Doughnut</div>
          <div class="chart-dd-item" data-ctype="radar" onclick="pickAgingChart('radar','Radar',this)">&#9642; Radar</div>
        </div>
      </div>
    </div>
  </div>
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;flex-wrap:wrap">
    <div id="chartSubtitle"></div>
    <div id="chartBadges"></div>
  </div>
  <div style="position:relative;height:400px;width:100%">
    <canvas id="agingTrendChart"></canvas>
  </div>
</div>
</div>

<!-- Legacy aging section (hidden, used by drill-down) -->
<div class="section" id="agingSection" style="display:none"></div>

<!-- Charts Row 1: Aging + Queue (hidden) -->
<div class="charts" id="chartsRow1" style="display:none"></div>

<!-- Heatmap: Queue x Aging (hidden) -->
<div class="section" id="heatmapSection" style="display:none"></div>

<!-- Daily Trend Chart (removed) -->
<div class="dashboard-section" data-section-id="trendChart" data-section-label="Daily Trend Chart" draggable="true" style="display:none">
<div class="section" id="trendSection"></div>
</div>

<!-- Charts Row 3: Zone + Partner -->
<div class="charts" id="chartsRow2"></div>

<!-- Filtered Tickets Table -->
<div class="section" id="filteredTableSection" style="display:none">
  <div class="section-header">
    <h3 id="filteredTableTitle">Filtered Tickets</h3>
    <button class="btn btn-sm btn-download" onclick="downloadFiltered()">&#11015; Download CSV</button>
  </div>
  <div class="table-scroll" id="filteredTableContent"></div>
</div>

<!-- Critical Tickets -->
<div class="section" id="criticalSection"></div>

<!-- Removed Templates Drawer -->
<div class="hidden-drawer" id="hiddenDrawer">
  <div class="hidden-drawer-panel" id="hiddenDrawerPanel"></div>
  <div class="hidden-drawer-toggle" id="hiddenDrawerToggle" onclick="toggleHiddenDrawer()">
    &#128230; Removed Templates (<span id="hiddenCount">0</span>)
  </div>
</div>

<!-- Ticket Trail Modal -->
<div class="modal-overlay" id="trailModal">
  <div class="modal">
    <button class="modal-close" onclick="closeTrail()">&times;</button>
    <h2 id="trailTitle">Ticket Trail</h2>
    <div id="trailContent"></div>
  </div>
</div>

<!-- Drill-Down Modal (click on heatmap/chart) -->
<div class="modal-overlay" id="drillModal">
  <div class="modal">
    <button class="modal-close" onclick="closeDrill()">&times;</button>
    <h2 id="drillTitle">Drill Down</h2>
    <div style="margin-bottom:10px">
      <button class="btn btn-sm btn-download" onclick="downloadDrill()">&#11015; Download CSV</button>
    </div>
    <div class="table-scroll" id="drillContent"></div>
  </div>
</div>

<script>
const BUCKET_LABELS = ['< 4h','4h - 12h','12h - 24h','24h - 36h','36h - 48h','48h - 72h','72h - 120h','> 120h'];
const BUCKET_COLORS = ['#2563eb','#16a34a','#65a30d','#ca8a04','#ea580c','#dc2626','#9333ea','#be123c'];
const BUCKET_DB_KEYS = ['bucket_lt4h','bucket_4_12h','bucket_12_24h','bucket_24_36h','bucket_36_48h','bucket_48_72h','bucket_72_120h','bucket_gt120h'];
const FILTERABLE_COLS = ['aging_bucket','current_queue','sub_status','status','zone','mapped_partner','city','channel_partner','disposition_l1','disposition_l2','disposition_l3','pending_days'];
const OPERATORS = ['equals','not equals','contains','not contains','greater than','less than','is empty','is not empty'];

let availableDates = [];
let currentDate = null;
let currentRangeMode = false;  // true when showing aggregated range data
let currentRangeFrom = null;
let currentRangeTo = null;
let currentPeriodType = null;  // 'week', 'month', or null for single-date/custom
let prevSummary = null;
let allTickets = [];
let filteredTickets = [];
let drillData = [];
let charts = {{}};

async function api(path) {{
  try {{
    const r = await fetch(path);
    if (!r.ok) return {{ error: 'HTTP ' + r.status }};
    return await r.json();
  }} catch(e) {{
    console.error('API error:', path, e);
    return {{ error: e.message }};
  }}
}}

// Helper: format a Date as YYYY-MM-DD in LOCAL timezone (avoids UTC shift from toISOString)
function localDateStr(d) {{
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${{y}}-${{m}}-${{day}}`;
}}

// Helper: format a date string as "Mon 17 Mar" for info bar display
function shortDate(dateStr) {{
  const dt = new Date(dateStr + 'T00:00:00');
  const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return `${{days[dt.getDay()]}} ${{dt.getDate()}} ${{months[dt.getMonth()]}}`;
}}

// ========== INIT ==========
async function init() {{
  availableDates = await api('/api/dates');
  if (availableDates.length > 0) {{
    loadDate(availableDates[0]);
  }} else {{
    document.getElementById('summaryCards').innerHTML = '<div class="loading">No data yet. Run backfill_history.py first.</div>';
  }}
  loadTrends();
  addFilterRow(); // Start with one filter row
}}

function highlightBtn(el) {{
  document.querySelectorAll('.date-btn').forEach(b => b.classList.remove('active'));
  if (el) el.classList.add('active');
}}
function navigateDate(offset) {{
  // Highlight clicked button
  highlightBtn(event && event.target);
  if (offset === 'latest') {{ loadDate(availableDates[0]); return; }}
  // For D-N buttons: calculate the target date as N calendar days ago, then find closest available
  const today = new Date();
  const target = new Date(today);
  target.setDate(target.getDate() + offset); // offset is negative
  const targetStr = localDateStr(target);
  // Find the closest available date on or before target
  const match = availableDates.find(d => d <= targetStr);
  if (match) loadDate(match);
  else if (availableDates.length > 0) loadDate(availableDates[availableDates.length - 1]);
}}

function navigatePeriod(type, n) {{
  // Highlight clicked button
  highlightBtn(event && event.target);
  // type='wk' or 'm', n=0 is current, n=1 is previous, etc.
  const today = new Date();
  let startStr, endStr, periodLabel;

  if (type === 'wk') {{
    currentPeriodType = 'week';
    periodLabel = `Wk-${{n}}`;
    // Week: Monday-based. Wk-0 = current week Mon..today, Wk-1 = last week Mon..Sun, etc.
    const dayOfWeek = today.getDay(); // 0=Sun, 1=Mon, ...
    const mondayOffset = dayOfWeek === 0 ? 6 : dayOfWeek - 1; // days since Monday
    const thisMonday = new Date(today);
    thisMonday.setDate(today.getDate() - mondayOffset);

    if (n === 0) {{
      startStr = localDateStr(thisMonday);
      endStr = localDateStr(today);
    }} else {{
      const weekStart = new Date(thisMonday);
      weekStart.setDate(thisMonday.getDate() - 7 * n);
      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekStart.getDate() + 6);
      startStr = localDateStr(weekStart);
      endStr = localDateStr(weekEnd);
    }}
  }} else if (type === 'm') {{
    currentPeriodType = 'month';
    periodLabel = `M-${{n}}`;
    // Month: M-0 = current month, M-1 = previous month, etc.
    const targetMonth = new Date(today.getFullYear(), today.getMonth() - n, 1);
    startStr = localDateStr(targetMonth);
    if (n === 0) {{
      endStr = localDateStr(today);
    }} else {{
      const monthEnd = new Date(targetMonth.getFullYear(), targetMonth.getMonth() + 1, 0);
      endStr = localDateStr(monthEnd);
    }}
  }}

  // Check if any available dates exist in this range
  const datesInRange = availableDates.filter(d => d >= startStr && d <= endStr);
  if (datesInRange.length === 0) {{
    alert('No data available for this period (' + startStr + ' to ' + endStr + ')');
    return;
  }}

  // Update date range pickers to show the period
  document.getElementById('dateFrom').value = startStr;
  document.getElementById('dateTo').value = endStr;

  // Load aggregated data for the range with period label
  loadDateRange(startStr, endStr, periodLabel);
}}

function applyDateRange() {{
  highlightBtn(null); // clear all highlights for custom range
  const from = document.getElementById('dateFrom').value;
  const to = document.getElementById('dateTo').value;
  if (!from || !to) {{
    alert('Please select both From and To dates');
    return;
  }}
  if (from > to) {{
    alert('From date must be before To date');
    return;
  }}
  // Check if any data exists in the range
  const datesInRange = availableDates.filter(d => d >= from && d <= to);
  if (datesInRange.length === 0) {{
    alert('No data available in the selected date range (' + from + ' to ' + to + ')');
    return;
  }}

  // If it's a single date, load normally; otherwise aggregate
  if (from === to) {{
    currentPeriodType = null;
    loadDate(from);
  }} else {{
    currentPeriodType = 'period';
    loadDateRange(from, to, 'Custom Range');
  }}
}}

async function loadDateRange(fromDate, toDate, periodLabel) {{
  currentRangeMode = true;
  currentRangeFrom = fromDate;
  currentRangeTo = toDate;
  cachedUniqueData = null; // reset unique cache for new range

  // Count how many data days are in the range
  const datesInRange = availableDates.filter(d => d >= fromDate && d <= toDate);
  const numDays = datesInRange.length;

  // Show loading state
  const label = periodLabel || 'Range';
  document.getElementById('dateInfo').textContent = `${{label}} | Loading...`;

  // Fetch aggregated summary
  const summary = await api(`/api/summary/range?from=${{fromDate}}&to=${{toDate}}`);
  if (summary.error) {{
    document.getElementById('summaryCards').innerHTML = '<div class="loading">No data for this range</div>';
    document.getElementById('dateInfo').textContent = `${{label}} | No data`;
    return;
  }}

  // Use latest date in range for ticket-level data (heatmap, critical, filters, etc.)
  const latestDateInRange = datesInRange[0]; // already sorted descending
  currentDate = latestDateInRange;
  allTickets = await api(`/api/tickets?date=${{latestDateInRange}}`);
  filteredTickets = [...allTickets];

  // Compute previous period for comparison
  const fromDt = new Date(fromDate + 'T00:00:00');
  const toDt = new Date(toDate + 'T00:00:00');
  const rangeDays = Math.round((toDt - fromDt) / (1000 * 60 * 60 * 24)) + 1;
  const prevTo = new Date(fromDt);
  prevTo.setDate(prevTo.getDate() - 1);
  const prevFrom = new Date(prevTo);
  prevFrom.setDate(prevFrom.getDate() - rangeDays + 1);
  const prevFromStr = localDateStr(prevFrom);
  const prevToStr = localDateStr(prevTo);

  prevSummary = await api(`/api/summary/range?from=${{prevFromStr}}&to=${{prevToStr}}`);
  if (prevSummary && prevSummary.error) prevSummary = null;

  // Build the year from the toDate for display
  const toYear = new Date(toDate + 'T00:00:00').getFullYear();
  document.getElementById('dateInfo').textContent =
    `${{label}} | ${{shortDate(fromDate)}} - ${{shortDate(toDate)}} ${{toYear}} | ${{numDays}} day(s) data | Per Day Avg`;

  // Add report_time field for renderSummary compatibility
  summary.report_time = `${{fromDate}} to ${{toDate}}`;

  renderSummary(summary);
  renderAging(summary);
  renderCharts(summary, allTickets);
  renderHeatmap(allTickets);
  renderZonePartnerCharts(allTickets);
  renderCritical(allTickets);
  resetFilters();
  loadPivotTable(null, fromDate, toDate);
  loadCategoryDailyTrend(fromDate, toDate);
  loadAgingChart(fromDate, toDate);
  // Hide master comparison for range view (not meaningful for aggregated data)
  document.getElementById('masterContent').innerHTML =
    '<div class="loading" style="color:var(--text2)">Master sheet comparison is only available for single-date views.</div>';
}}

async function loadDate(date) {{
  if (!date) return;
  currentDate = date;
  currentRangeMode = false;
  currentRangeFrom = null;
  currentRangeTo = null;
  currentPeriodType = null;

  const idx = availableDates.indexOf(date);
  prevSummary = null;
  if (idx + 1 < availableDates.length) {{
    prevSummary = await api(`/api/summary?date=${{availableDates[idx + 1]}}`);
  }}

  const summary = await api(`/api/summary?date=${{date}}`);
  allTickets = await api(`/api/tickets?date=${{date}}`);
  filteredTickets = [...allTickets];

  const dLabel = idx === 0 ? 'Latest' : `D-${{idx}}`;
  document.getElementById('dateInfo').textContent =
    `${{dLabel}} | ${{formatDate(date)}} | Report: ${{summary.report_time || ''}} | ${{allTickets.length}} tickets`;

  renderSummary(summary);
  renderAging(summary);
  renderCharts(summary, allTickets);
  renderHeatmap(allTickets);
  renderZonePartnerCharts(allTickets);
  renderCritical(allTickets);
  resetFilters();
  loadPivotTable(date);
  loadCategoryDailyTrend();
  loadAgingDailyTrend();
  loadAgingChart();
  loadMasterComparison(date);
}}

// ========== CATEGORY BIFURCATION ==========
const CAT_COLORS = {{
  'Internet Issues': '#1a73e8',
  'Router Pickup': '#f97316',
  'Others': '#6b7280',
  'Payment Issues': '#eab308',
  'Shifting Request': '#8b5cf6',
  'Partner Misbehavior': '#ef4444',
  'Refund': '#ec4899',
  'Change Request': '#14b8a6',
  'Remove Connection - Talk to Customer': '#64748b',
  'Unknown': '#94a3b8',
}};

// ========== PIVOT TABLE (Category x Aging) ==========
async function loadPivotTable(date, fromDate, toDate) {{
  const pivotEl = document.getElementById('pivotContent');
  pivotEl.innerHTML = '<div class="loading">Loading bifurcation...</div>';
  try {{
    let pivot;
    if (fromDate && toDate) {{
      pivot = await api(`/api/category-aging/range?from=${{fromDate}}&to=${{toDate}}`);
    }} else {{
      pivot = await api(`/api/category-aging?date=${{date}}`);
    }}

    if (!pivot || pivot.error || !pivot.categories || pivot.categories.length === 0) {{
      pivotEl.innerHTML = '<div class="loading">No pivot data available</div>';
      return;
    }}

    window._pivotData = pivot;
    const buckets = pivot.buckets;

    let dropdownItems = pivot.categories.map(cat => {{
      const color = CAT_COLORS[cat] || '#94a3b8';
      return `<label style="display:flex;align-items:center;gap:8px;padding:6px 12px;cursor:pointer;font-size:12px;white-space:nowrap;transition:background .1s"
        onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='transparent'">
        <input type="checkbox" checked data-cat="${{cat}}" onchange="filterPivotTable()"
          style="accent-color:${{color}};width:14px;height:14px;cursor:pointer">
        <span style="width:8px;height:8px;border-radius:50%;background:${{color}};display:inline-block;flex-shrink:0"></span>
        ${{cat}}
      </label>`;
    }}).join('');

    const noteText = (fromDate && toDate) ? `Aggregated data from ${{fromDate}} to ${{toDate}}` : '&#128279; Click any number to download the raw ticket data for that cell';

    pivotEl.innerHTML = `
      <div style="margin-bottom:12px">
        <div style="overflow-x:auto;border:1px solid var(--border);border-radius:8px">
          <table id="pivotTable" style="min-width:100%;border-collapse:collapse">
            <thead><tr id="pivotHead" style="background:#f8fafc;border-bottom:2px solid var(--border)"></tr></thead>
            <tbody id="pivotBody"></tbody>
          </table>
        </div>
        <div style="font-size:11px;color:var(--text2);margin-top:6px">${{noteText}}</div>
      </div>`;

    window._pivotFilterDropdown = `
      <div style="position:relative;display:inline-block" id="pivotFilterWrap">
        <button onclick="var d=document.getElementById('pivotDropdown');d.style.display=d.style.display==='none'?'block':'none'"
          style="padding:5px 14px;border:1px solid #e2e8f0;border-radius:6px;background:#fff;cursor:pointer;font-size:12px;font-family:inherit;font-weight:500;display:flex;align-items:center;gap:6px">
          &#9776; Filter Categories <span style="font-size:10px;color:#64748b" id="pivotFilterCount">(${{pivot.categories.length}}/${{pivot.categories.length}})</span>
          <span style="font-size:9px">&#9660;</span>
        </button>
        <div id="pivotDropdown" style="display:none;position:absolute;right:0;top:100%;margin-top:4px;background:#fff;border:1px solid #e2e8f0;border-radius:8px;box-shadow:0 4px 16px rgba(0,0,0,.12);z-index:50;min-width:280px;max-height:320px;overflow-y:auto">
          <div style="display:flex;gap:6px;padding:8px 12px;border-bottom:1px solid #e2e8f0;position:sticky;top:0;background:#fff;z-index:1">
            <button onclick="document.querySelectorAll('#pivotDropdown input[data-cat]').forEach(c=>c.checked=true);filterPivotTable()"
              style="flex:1;padding:4px 8px;border:1px solid #e2e8f0;border-radius:4px;background:#f8fafc;cursor:pointer;font-size:11px;font-weight:600">Select All</button>
            <button onclick="document.querySelectorAll('#pivotDropdown input[data-cat]').forEach(c=>c.checked=false);filterPivotTable()"
              style="flex:1;padding:4px 8px;border:1px solid #e2e8f0;border-radius:4px;background:#f8fafc;cursor:pointer;font-size:11px;font-weight:600">Deselect All</button>
          </div>
          ${{dropdownItems}}
        </div>
      </div>`;

    document.addEventListener('click', function(e) {{
      const wrap = document.getElementById('pivotFilterWrap');
      const dd = document.getElementById('pivotDropdown');
      if (wrap && dd && !wrap.contains(e.target)) dd.style.display = 'none';
    }});

    const fc = document.getElementById('pivotFilterContainer');
    if (fc && window._pivotFilterDropdown) fc.innerHTML = window._pivotFilterDropdown;
    filterPivotTable();
  }} catch(e) {{
    pivotEl.innerHTML = '<div class="loading">Could not load pivot data</div>';
  }}
}}

// Pivot filter + render function
window.filterPivotTable = function() {{
  const pivot = window._pivotData;
  if (!pivot) return;

  const buckets = pivot.buckets;
  const catData = pivot.data;
  const totalsByCat = pivot.totals_by_cat;

  const checked = Array.from(document.querySelectorAll('#pivotDropdown input[data-cat]:checked')).map(c => c.dataset.cat);
  const catsToShow = checked.length > 0 ? pivot.categories.filter(c => checked.includes(c)) : [];

  const countEl = document.getElementById('pivotFilterCount');
  if (countEl) countEl.textContent = `(${{checked.length}}/${{pivot.categories.length}})`;

  if (catsToShow.length === 0) {{
    document.getElementById('pivotHead').innerHTML = '';
    document.getElementById('pivotBody').innerHTML = '<tr><td style="text-align:center;padding:30px;color:var(--text2)">Select at least one category to view data</td></tr>';
    return;
  }}

  let headerCells = `<th style="text-align:left;min-width:180px;position:sticky;left:0;background:#f8fafc;z-index:2">Disposition Folder Level 3</th>`;
  buckets.forEach(b => {{
    headerCells += `<th style="text-align:center;min-width:80px;white-space:nowrap">${{b}}</th>`;
  }});
  headerCells += `<th style="text-align:center;min-width:90px;font-weight:700">Grand Total</th>`;
  document.getElementById('pivotHead').innerHTML = headerCells;

  let bodyRows = '';
  let filteredTotalsByBucket = {{}};
  let filteredGrandTotal = 0;

  catsToShow.forEach(cat => {{
    const isInternet = cat === 'Internet Issues';
    const color = CAT_COLORS[cat] || '#94a3b8';
    const rowStyle = isInternet ? 'background:#eff6ff;font-weight:700' : '';

    let cells = `<td style="position:sticky;left:0;background:${{isInternet ? '#eff6ff' : '#fff'}};z-index:1;cursor:pointer" onclick="togglePivotL4('${{cat.replace(/'/g, "\\\\'")}}', this.parentElement)">
      <span style="display:inline-block;transition:transform 0.2s;font-size:9px;margin-right:4px" class="pivotArrow">&#9654;</span>
      <span class="dot" style="background:${{color}}"></span>${{cat}}${{isInternet ? ' &#9733;' : ''}}
    </td>`;

    buckets.forEach(b => {{
      const val = (catData[cat] && catData[cat][b]) || 0;
      filteredTotalsByBucket[b] = (filteredTotalsByBucket[b] || 0) + val;
      if (val > 0) {{
        const encCat = encodeURIComponent(cat);
        const encBuck = encodeURIComponent(b);
        cells += `<td class="num">
          <a href="/api/download-category-bucket?date=${{currentDate}}&category=${{encCat}}&bucket=${{encBuck}}"
             style="color:${{isInternet ? '#1a73e8' : '#374151'}};text-decoration:none;cursor:pointer;border-bottom:1px dashed ${{isInternet ? '#1a73e8' : '#9ca3af'}}"
             title="Download ${{val}} tickets: ${{cat}} / ${{b}}"
             target="_blank">${{val.toLocaleString()}}</a>
        </td>`;
      }} else {{
        cells += `<td class="num" style="color:#d1d5db">—</td>`;
      }}
    }});

    const catTotal = totalsByCat[cat] || 0;
    filteredGrandTotal += catTotal;
    const encCat = encodeURIComponent(cat);
    cells += `<td class="num" style="font-weight:700">
      <a href="/api/download-category-bucket?date=${{currentDate}}&category=${{encCat}}"
         style="color:#1a73e8;text-decoration:none;border-bottom:1px dashed #1a73e8"
         title="Download all ${{catTotal}} tickets: ${{cat}}"
         target="_blank">${{catTotal.toLocaleString()}}</a>
    </td>`;

    bodyRows += `<tr style="${{rowStyle}}">${{cells}}</tr>`;
  }});

  let totalCells = `<td style="position:sticky;left:0;background:#f1f5f9;z-index:1;font-weight:700">Grand Total</td>`;
  buckets.forEach(b => {{
    const val = filteredTotalsByBucket[b] || 0;
    const encBuck = encodeURIComponent(b);
    totalCells += `<td class="num" style="font-weight:700">
      <a href="/api/download-category-bucket?date=${{currentDate}}&bucket=${{encBuck}}"
         style="color:#1a73e8;text-decoration:none;border-bottom:1px dashed #1a73e8"
         title="Download ${{val}} tickets in ${{b}}"
         target="_blank">${{val.toLocaleString()}}</a>
    </td>`;
  }});
  totalCells += `<td class="num" style="font-weight:700;color:#1a73e8">${{filteredGrandTotal.toLocaleString()}}</td>`;

  document.getElementById('pivotBody').innerHTML = bodyRows +
    `<tr style="border-top:2px solid var(--border);background:#f1f5f9">${{totalCells}}</tr>`;
}};

// Toggle L4 sub-rows in pivot table
window.togglePivotL4 = async function(l3Cat, rowEl) {{
  const arrow = rowEl.querySelector('.pivotArrow');

  // Check if L4 rows already exist — toggle visibility
  let nextRow = rowEl.nextElementSibling;
  const existing = [];
  while (nextRow && nextRow.classList.contains('pivotL4Row') && nextRow.dataset.parentL3 === l3Cat) {{
    existing.push(nextRow);
    nextRow = nextRow.nextElementSibling;
  }}

  if (existing.length > 0) {{
    const visible = existing[0].style.display !== 'none';
    existing.forEach(r => r.style.display = visible ? 'none' : '');
    arrow.style.transform = visible ? '' : 'rotate(90deg)';
    return;
  }}

  // Fetch L4 data
  arrow.style.transform = 'rotate(90deg)';
  const pivot = window._pivotData;
  const buckets = pivot.buckets;
  const encL3 = encodeURIComponent(l3Cat);

  let url = `/api/pivot-l4-breakdown?l3=${{encL3}}&date=${{currentDate}}`;
  if (currentRangeFrom && currentRangeTo) {{
    url = `/api/pivot-l4-breakdown?l3=${{encL3}}&from=${{currentRangeFrom}}&to=${{currentRangeTo}}`;
  }}

  const data = await api(url);
  if (!data || !data.l4_categories || data.l4_categories.length === 0) {{
    arrow.style.transform = '';
    return;
  }}

  // Insert L4 rows after the L3 row
  const l3Total = pivot.totals_by_cat[l3Cat] || 1;
  let insertHtml = '';

  data.l4_categories.forEach(l4 => {{
    const l4Total = data.totals[l4] || 0;
    const pct = ((l4Total / l3Total) * 100).toFixed(1);
    let cells = `<td style="position:sticky;left:0;background:#fafbfc;z-index:1;padding-left:36px;font-size:12px;color:#64748b">
      &#8627; ${{l4}} <span style="font-size:10px;color:#94a3b8">(${{pct}}%)</span>
    </td>`;

    buckets.forEach(b => {{
      const val = (data.data[l4] && data.data[l4][b]) || 0;
      if (val > 0) {{
        cells += `<td class="num" style="font-size:12px;color:#64748b">${{val.toLocaleString()}}</td>`;
      }} else {{
        cells += `<td class="num" style="color:#e2e8f0;font-size:12px">—</td>`;
      }}
    }});
    cells += `<td class="num" style="font-size:12px;font-weight:600;color:#64748b">${{l4Total.toLocaleString()}}</td>`;

    insertHtml += `<tr class="pivotL4Row" data-parent-l3="${{l3Cat}}" style="background:#fafbfc;border-left:3px solid #e2e8f0">${{cells}}</tr>`;
  }});

  rowEl.insertAdjacentHTML('afterend', insertHtml);
}};

// ========== CATEGORY DAILY TREND ==========
// Load category daily trend (independent section with its own date filter)
async function loadCategoryDailyTrend(overrideFrom, overrideTo) {{
  const container = document.getElementById('categorySummaryContent');
  container.innerHTML = '<div class="loading">Loading category trend...</div>';

  try {{
    let fromDate, toDate;
    if (overrideFrom && overrideTo) {{
      fromDate = overrideFrom;
      toDate = overrideTo;
    }} else {{
      // Default: last 7 days ending at currentDate or latest available
      const refDate = currentDate || (availableDates.length > 0 ? availableDates[0] : null);
      if (!refDate) {{
        container.innerHTML = '<div class="loading">No dates available</div>';
        return;
      }}
      toDate = refDate;
      const to = new Date(refDate + 'T00:00:00');
      to.setDate(to.getDate() - 6);
      fromDate = localDateStr(to);
    }}

    // Update the section's date inputs
    document.getElementById('catTrendFrom').value = fromDate;
    document.getElementById('catTrendTo').value = toDate;

    const data = await api(`/api/category-daily-trend?from=${{fromDate}}&to=${{toDate}}`);
    if (!data || data.error || !data.dates || data.dates.length === 0) {{
      container.innerHTML = '<div class="loading">No category trend data available for this range</div>';
      return;
    }}

    const dates = data.dates;
    const categories = data.categories;

    // Store for filter recalculation
    window._catTrendDates = dates;
    window._catTrendCategories = categories;

    // Collect all category names and sort by total descending
    const catNames = Object.keys(categories);
    const catTotals = {{}};
    catNames.forEach(cat => {{
      catTotals[cat] = dates.reduce((s, d) => s + (categories[cat][d] || 0), 0);
    }});
    catNames.sort((a, b) => catTotals[b] - catTotals[a]);

    // Compute daily totals
    const dailyTotals = {{}};
    dates.forEach(d => {{
      dailyTotals[d] = catNames.reduce((s, cat) => s + (categories[cat][d] || 0), 0);
    }});

    // Format date for column header: "Mar 18"
    function shortCol(dateStr) {{
      const dt = new Date(dateStr + 'T00:00:00');
      return dt.toLocaleDateString('en-IN', {{ day: 'numeric', month: 'short' }});
    }}

    // Build header
    let headerCells = `<th style="text-align:left;min-width:200px;position:sticky;left:0;background:#f8fafc;z-index:2">Category</th>`;
    dates.forEach(d => {{
      headerCells += `<th style="text-align:center;min-width:75px;white-space:nowrap;font-size:11px">${{shortCol(d)}}</th>`;
    }});

    // Build body rows
    let bodyRows = '';
    catNames.forEach(cat => {{
      const color = CAT_COLORS[cat] || '#94a3b8';
      const isInternet = cat === 'Internet Issues';
      const rowStyle = isInternet ? 'background:#eff6ff;font-weight:700' : '';
      const catId = cat.replace(/[^a-zA-Z0-9]/g, '_');

      let cells = `<td style="position:sticky;left:0;background:${{isInternet ? '#eff6ff' : '#fff'}};z-index:1;white-space:nowrap;cursor:pointer" onclick="toggleL4('${{catId}}','${{cat.replace(/'/g, "\\\\'")}}')">
        <span id="arrow_${{catId}}" style="display:inline-block;width:14px;font-size:10px;transition:transform 0.2s">&#9654;</span>
        <span class="dot" style="background:${{color}}"></span>${{cat}}${{isInternet ? ' &#9733;' : ''}}
      </td>`;

      dates.forEach(d => {{
        const count = categories[cat][d] || 0;
        const total = dailyTotals[d] || 1;
        const pct = (count / total * 100).toFixed(1);
        const dlUrl = `/api/download-category-tickets?date=${{d}}&l3=${{encodeURIComponent(cat)}}`;
        cells += `<td class="num" style="font-size:11px;cursor:pointer" title="Click to download ${{cat}} tickets for ${{d}}" onclick="window.location.href='${{dlUrl}}'">${{count > 0 ? pct + '%' : '—'}}</td>`;
      }});

      bodyRows += `<tr data-catrow="${{catId}}" style="${{rowStyle}}">${{cells}}</tr>`;
      // Placeholder for L4 sub-rows (inserted dynamically)
      bodyRows += `<!-- L4_PLACEHOLDER_${{catId}} -->`;
    }});

    // TOTAL row with actual numbers
    let totalCells = `<td style="position:sticky;left:0;background:#f1f5f9;z-index:1;font-weight:700">TOTAL</td>`;
    dates.forEach(d => {{
      totalCells += `<td class="num" style="font-weight:700;font-size:11px">${{(dailyTotals[d] || 0).toLocaleString()}}</td>`;
    }});

    const tableHtml = `
      <div style="overflow-x:auto;border:1px solid var(--border);border-radius:8px">
        <table style="min-width:100%;border-collapse:collapse">
          <thead><tr style="background:#f8fafc;border-bottom:2px solid var(--border)">${{headerCells}}</tr></thead>
          <tbody>
            ${{bodyRows}}
            <tr style="border-top:2px solid var(--border);background:#f1f5f9">${{totalCells}}</tr>
          </tbody>
        </table>
      </div>
      <div style="font-size:11px;color:var(--text2);margin-top:6px">
        Showing ${{dates.length}} day(s) from ${{shortCol(dates[0])}} to ${{shortCol(dates[dates.length - 1])}} &mdash; each cell shows % of daily total
      </div>`;

    container.innerHTML = tableHtml;

    // Build filter dropdown (same style as pivot table filter)
    const checkedCount = catNames.length;
    const totalCount = catNames.length;
    let filterItems = '';
    catNames.forEach(cat => {{
      const color = CAT_COLORS[cat] || '#94a3b8';
      filterItems += `<label style="display:flex;align-items:center;gap:6px;padding:4px 10px;cursor:pointer;font-size:12px;white-space:nowrap">
        <input type="checkbox" checked data-cattrend="${{cat}}" onchange="filterCatTrend()"
               style="accent-color:${{color}};cursor:pointer"> ${{cat}}
      </label>`;
    }});

    const filterHtml = `
      <div style="position:relative;display:inline-block">
        <button onclick="document.getElementById('catTrendDropdown').classList.toggle('show')"
                style="padding:4px 10px;border:1px solid var(--border);border-radius:6px;background:#fff;cursor:pointer;font-size:11px;font-family:inherit;display:flex;align-items:center;gap:4px">
          &#9776; Filter Categories <span id="catTrendFilterCount">(${{checkedCount}}/${{totalCount}})</span> &#9660;
        </button>
        <div id="catTrendDropdown" style="display:none;position:absolute;right:0;top:100%;margin-top:4px;background:#fff;border:1px solid var(--border);border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.1);z-index:100;min-width:220px;max-height:300px;overflow-y:auto">
          <div style="display:flex;gap:6px;padding:8px 10px;border-bottom:1px solid var(--border)">
            <button onclick="document.querySelectorAll('#catTrendDropdown input[data-cattrend]').forEach(c=>c.checked=true);filterCatTrend()"
                    style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#f0fdf4;cursor:pointer;font-size:10px">Select All</button>
            <button onclick="document.querySelectorAll('#catTrendDropdown input[data-cattrend]').forEach(c=>c.checked=false);filterCatTrend()"
                    style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#fef2f2;cursor:pointer;font-size:10px">Deselect All</button>
          </div>
          ${{filterItems}}
        </div>
      </div>`;

    const fc = document.getElementById('catTrendFilterContainer');
    if (fc) fc.innerHTML = filterHtml;

    // Close dropdown when clicking outside
    document.addEventListener('click', function(e) {{
      const dd = document.getElementById('catTrendDropdown');
      const container = document.getElementById('catTrendFilterContainer');
      if (dd && container && !container.contains(e.target)) dd.classList.remove('show');
    }});

  }} catch(e) {{
    container.innerHTML = '<div class="loading">Could not load category trend</div>';
  }}
}}

// Toggle L4 sub-rows for a category
window._l4Expanded = {{}};
async function toggleL4(catId, catName) {{
  const table = document.querySelector('#categorySummaryContent table tbody');
  if (!table) return;
  const arrow = document.getElementById('arrow_' + catId);

  // If already expanded, collapse
  if (window._l4Expanded[catId]) {{
    table.querySelectorAll(`tr[data-l4parent="${{catId}}"]`).forEach(r => r.remove());
    window._l4Expanded[catId] = false;
    if (arrow) arrow.style.transform = 'rotate(0deg)';
    return;
  }}

  // Fetch L4 data
  const fromDate = document.getElementById('catTrendFrom').value;
  const toDate = document.getElementById('catTrendTo').value;
  if (!fromDate || !toDate) return;

  try {{
    const data = await api(`/api/category-l4-trend?from=${{fromDate}}&to=${{toDate}}&l3=${{encodeURIComponent(catName)}}`);
    if (!data || !data.l4_categories) return;

    const dates = window._catTrendDates || data.dates;
    const l4Cats = data.l4_categories;
    const l3Totals = data.l3_totals || {{}};

    // Sort L4 categories by total count descending
    const l4Names = Object.keys(l4Cats);
    l4Names.sort((a, b) => {{
      const totalA = dates.reduce((s, d) => s + (l4Cats[a][d] || 0), 0);
      const totalB = dates.reduce((s, d) => s + (l4Cats[b][d] || 0), 0);
      return totalB - totalA;
    }});

    // Find the parent row to insert after
    const parentRow = table.querySelector(`tr[data-catrow="${{catId}}"]`);
    if (!parentRow) return;

    // Build L4 sub-rows
    l4Names.forEach(l4Name => {{
      const tr = document.createElement('tr');
      tr.setAttribute('data-l4parent', catId);
      tr.style.cssText = 'background:#fafbfc;font-size:11px;';

      let tdName = document.createElement('td');
      tdName.style.cssText = 'position:sticky;left:0;background:#fafbfc;z-index:1;white-space:nowrap;padding-left:40px;color:#64748b;font-size:11px';
      tdName.textContent = '└ ' + l4Name;
      tr.appendChild(tdName);

      dates.forEach(d => {{
        const count = l4Cats[l4Name][d] || 0;
        const l3Total = l3Totals[d] || 1;
        const pct = l3Total > 0 ? (count / l3Total * 100).toFixed(1) : '0.0';
        const td = document.createElement('td');
        td.className = 'num';
        td.style.cssText = 'font-size:10px;color:#64748b;cursor:pointer';
        td.textContent = count > 0 ? pct + '%' : '—';
        td.title = `${{count}} of ${{l3Total}} ${{catName}} tickets (${{l4Name}}) on ${{d}} — Click to download`;
        td.onclick = function() {{
          window.location.href = `/api/download-category-tickets?date=${{d}}&l3=${{encodeURIComponent(catName)}}&l4=${{encodeURIComponent(l4Name)}}`;
        }};
        tr.appendChild(td);
      }});

      parentRow.after(tr);
      // Insert in order: we need to insert after the last L4 row
      const existingL4 = table.querySelectorAll(`tr[data-l4parent="${{catId}}"]`);
      const lastL4 = existingL4[existingL4.length - 1];
      if (lastL4 && lastL4 !== tr) {{
        lastL4.after(tr);
      }}
    }});

    window._l4Expanded[catId] = true;
    if (arrow) arrow.style.transform = 'rotate(90deg)';
  }} catch(e) {{
    console.error('Failed to load L4 data:', e);
  }}
}}

// Show/hide category rows and recalculate totals
window.filterCatTrend = function() {{
  const checked = Array.from(document.querySelectorAll('#catTrendDropdown input[data-cattrend]:checked')).map(c => c.getAttribute('data-cattrend'));
  const totalCount = document.querySelectorAll('#catTrendDropdown input[data-cattrend]').length;
  document.getElementById('catTrendFilterCount').textContent = `(${{checked.length}}/${{totalCount}})`;

  const table = document.querySelector('#categorySummaryContent table');
  if (!table) return;
  const rows = table.querySelectorAll('tbody tr');
  let totalRow = null;

  rows.forEach(row => {{
    const firstTd = row.querySelector('td');
    if (!firstTd) return;
    const text = firstTd.textContent.trim().replace(' ★', '').replace(/^[►▶]\s*/, '');
    if (text === 'TOTAL') {{
      totalRow = row;
      return;
    }}
    // Handle L4 sub-rows
    const l4Parent = row.getAttribute('data-l4parent');
    if (l4Parent) {{
      // Show/hide L4 rows based on parent category visibility
      const parentCatRow = table.querySelector(`tr[data-catrow="${{l4Parent}}"]`);
      if (parentCatRow) {{
        const parentText = parentCatRow.querySelector('td').textContent.trim().replace(' ★', '').replace(/^[►▶]\s*/, '');
        row.style.display = checked.includes(parentText) ? '' : 'none';
      }}
      return;
    }}
    const catRow = row.getAttribute('data-catrow');
    if (catRow) {{
      // Get clean category name from the text content (skip arrow + dot)
      const cleanText = firstTd.textContent.trim().replace(' ★', '').replace(/^[►▶]\s*/, '');
      row.style.display = checked.includes(cleanText) ? '' : 'none';
      return;
    }}
    row.style.display = checked.includes(text) ? '' : 'none';
  }});

  // Recalculate TOTAL row based on selected categories
  if (totalRow && window._catTrendDates && window._catTrendCategories) {{
    const dates = window._catTrendDates;
    const categories = window._catTrendCategories;
    const tds = totalRow.querySelectorAll('td');

    dates.forEach((d, i) => {{
      let filteredTotal = 0;
      checked.forEach(cat => {{
        filteredTotal += (categories[cat] && categories[cat][d]) || 0;
      }});
      if (tds[i + 1]) tds[i + 1].textContent = filteredTotal.toLocaleString();
    }});

    // Also recalculate % for visible rows based on filtered totals
    rows.forEach(row => {{
      if (row === totalRow || row.style.display === 'none') return;
      if (row.getAttribute('data-l4parent')) return; // Skip L4 sub-rows
      const firstTd = row.querySelector('td');
      if (!firstTd) return;
      const cat = firstTd.textContent.trim().replace(' ★', '').replace(/^[►▶]\s*/, '');
      const cells = row.querySelectorAll('td');
      dates.forEach((d, i) => {{
        const count = (categories[cat] && categories[cat][d]) || 0;
        let filteredTotal = 0;
        checked.forEach(c => {{
          filteredTotal += (categories[c] && categories[c][d]) || 0;
        }});
        const pct = filteredTotal > 0 ? (count / filteredTotal * 100).toFixed(1) : '0.0';
        if (cells[i + 1]) cells[i + 1].textContent = count > 0 ? pct + '%' : '—';
      }});
    }});
  }}
}}

// Apply the section's own date range filter
function applyCatTrendFilter() {{
  const from = document.getElementById('catTrendFrom').value;
  const to = document.getElementById('catTrendTo').value;
  if (!from || !to) {{
    alert('Please select both FROM and TO dates');
    return;
  }}
  if (from > to) {{
    alert('FROM date must be before TO date');
    return;
  }}
  // Limit to 120 days
  const diff = (new Date(to + 'T00:00:00') - new Date(from + 'T00:00:00')) / (1000*60*60*24);
  if (diff > 120) {{
    alert('Maximum range is 120 days. Please narrow your selection.');
    return;
  }}
  loadCategoryDailyTrend(from, to);
}}

// ========== MASTER SHEET COMPARISON ==========
async function loadMasterComparison(date) {{
  document.getElementById('masterContent').innerHTML = '<div class="loading">Loading morning snapshot...</div>';
  try {{
    // Get LOCKED morning snapshot only (no live fetch on page load)
    const snapshot = await api(`/api/master-compare?date=${{date}}`);

    if (snapshot.error) {{
      document.getElementById('masterContent').innerHTML = `<p style="color:var(--red)">No comparison data available</p>`;
      return;
    }}

    const s = snapshot;
    const pctNew = s.total_internet ? (s.new_to_upload / s.total_internet * 100).toFixed(1) : 0;
    const pctOld = s.total_internet ? (s.already_in_master / s.total_internet * 100).toFixed(1) : 0;

    document.getElementById('masterRefreshInfo').textContent =
      s.snapshot_fixed ? `Snapshot: ${{s.master_refreshed}} (locked)` : '';

    document.getElementById('masterContent').innerHTML = `
      <!-- Morning Snapshot (Primary — always visible) -->
      <div style="margin-bottom:6px;display:flex;align-items:center;gap:8px">
        <span style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.5px">
          Morning Snapshot (10:15 AM — Locked)</span>
        <span style="font-size:10px;color:var(--text2)">${{s.master_refreshed || ''}}</span>
      </div>
      <div class="cards" style="margin-bottom:16px">
        <div class="card" style="border-left:3px solid var(--accent)">
          <div class="card-label">Total Internet Issues</div>
          <div class="card-value blue">${{s.total_internet.toLocaleString()}}</div>
          <div class="card-sub">Filtered from report</div>
        </div>
        <div class="card" style="border-left:3px solid var(--text2)">
          <div class="card-label">Already in Master</div>
          <div class="card-value" style="color:var(--text2)">${{s.already_in_master.toLocaleString()}}</div>
          <div class="card-sub">${{pctOld}}% — Old/existing</div>
        </div>
        <div class="card" style="border-left:3px solid var(--green);background:#ecfdf5">
          <div class="card-label">&#9733; New Tickets to Upload</div>
          <div class="card-value green">${{s.new_to_upload.toLocaleString()}}</div>
          <div class="card-sub">${{pctNew}}% — Not yet in master</div>
        </div>
        <div class="card" style="border-left:3px solid var(--border)">
          <div class="card-label">Master Sheet Total</div>
          <div class="card-value" style="color:var(--text2);font-size:22px">${{s.master_total.toLocaleString()}}</div>
          <div class="card-sub">At time of snapshot</div>
        </div>
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:18px">
        <button class="btn btn-download" onclick="window.open('/api/download-new-tickets?date=${{currentDate}}')">
          &#11015; Download NEW Tickets (${{s.new_to_upload}}) — Full Details CSV
        </button>
        <button class="btn btn-sm" onclick="window.open('/api/download-existing-tickets?date=${{currentDate}}')">
          &#11015; Download Existing (${{s.already_in_master}})
        </button>
        <button class="btn btn-sm" onclick="showNewTicketsList()">
          &#128065; View New Ticket IDs
        </button>
      </div>

      <!-- Live Upload Status — only loads on click -->
      <div style="border-top:2px solid var(--border);padding-top:14px">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">
          <span style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.5px">
            Live Upload Status</span>
          <button class="btn btn-sm" onclick="checkLiveUploadStatus()" style="margin-left:auto">&#8635; Check Now</button>
        </div>
        <div id="liveStatusContent">
          <p style="color:var(--text2);font-size:13px">Click <b>Check Now</b> to fetch live upload status from the master sheet.</p>
        </div>
      </div>
    `;
    window._newTicketIds = s.new_ticket_ids || [];
    window._snapshotData = s;
  }} catch(e) {{
    document.getElementById('masterContent').innerHTML =
      '<p style="color:var(--orange)">Could not load comparison. Check connection.</p>';
  }}
}}

async function checkLiveUploadStatus() {{
  const liveEl = document.getElementById('liveStatusContent');
  if (!liveEl) return;
  liveEl.innerHTML = '<div class="loading">Fetching live data from master sheet...</div>';

  try {{
    await api('/api/refresh-master');
    // Wait for master sheet to refresh
    await new Promise(r => setTimeout(r, 4000));
    const live = await api(`/api/master-live?date=${{currentDate}}`);
    const s = window._snapshotData;

    if (!s || !live || live.master_total === 0) {{
      liveEl.innerHTML = '<p style="color:var(--orange)">Could not fetch master sheet. Try again in a moment.</p>';
      return;
    }}

    const uploadedCount = s.new_to_upload - live.new_to_upload;
    if (uploadedCount < 0) {{
      liveEl.innerHTML = '<p style="color:var(--orange)">Master sheet data inconsistent. Try refreshing again.</p>';
      return;
    }}

    const stillPending = live.new_to_upload;
    const uploadPct = s.new_to_upload > 0 ? Math.round(uploadedCount / s.new_to_upload * 100) : 100;
    const allUploaded = stillPending === 0;

    liveEl.innerHTML = `
      <div style="font-size:10px;color:var(--text2);margin-bottom:10px">
        Last checked: ${{live.master_refreshed || 'now'}}
      </div>
      <div class="cards">
        <div class="card" style="border-left:3px solid ${{allUploaded ? 'var(--green)' : 'var(--orange)'}};
          background:${{allUploaded ? '#ecfdf5' : '#fff7ed'}}">
          <div class="card-label">Upload Progress</div>
          <div class="card-value" style="color:${{allUploaded ? 'var(--green)' : 'var(--orange)'}}">${{uploadPct}}%</div>
          <div class="card-sub">${{allUploaded
            ? '<span style="color:var(--green);font-weight:700">ALL UPLOADED</span>'
            : '<span style="color:var(--orange);font-weight:700">' + stillPending + ' still pending</span>'}}</div>
          <div style="margin-top:6px">
            <div class="bar-bg" style="height:8px">
              <div class="bar-fill" style="width:${{uploadPct}}%;background:${{allUploaded ? 'var(--green)' : 'var(--orange)'}}"></div>
            </div>
          </div>
        </div>
        <div class="card" style="border-left:3px solid var(--green)">
          <div class="card-label">Uploaded to Master</div>
          <div class="card-value green">${{uploadedCount.toLocaleString()}}</div>
          <div class="card-sub">of ${{s.new_to_upload.toLocaleString()}} new tickets</div>
        </div>
        <div class="card" style="border-left:3px solid ${{stillPending > 0 ? 'var(--red)' : 'var(--green)'}}">
          <div class="card-label">Still Pending Upload</div>
          <div class="card-value ${{stillPending > 0 ? 'red' : 'green'}}">${{stillPending.toLocaleString()}}</div>
          <div class="card-sub">${{stillPending > 0 ? 'Not yet in master sheet' : 'All done!'}}</div>
        </div>
        <div class="card" style="border-left:3px solid var(--accent)">
          <div class="card-label">Master Sheet Now</div>
          <div class="card-value blue" style="font-size:22px">${{live.master_total.toLocaleString()}}</div>
          <div class="card-sub">Current total in master</div>
        </div>
      </div>
      ${{stillPending > 0 ? `
      <div style="margin-top:10px">
        <button class="btn btn-download" onclick="window.open('/api/download-still-pending?date=${{currentDate}}')">
          &#11015; Download Still-Pending Tickets (${{stillPending}}) — CSV
        </button>
      </div>` : ''}}
    `;
  }} catch(e) {{
    liveEl.innerHTML = '<p style="color:var(--orange)">Error fetching live status. Try again.</p>';
  }}
}}

function showNewTicketsList() {{
  const ids = window._newTicketIds || [];
  if (!ids.length) {{ alert('No new tickets found'); return; }}
  const newTickets = allTickets.filter(t => ids.includes(t.ticket_no));
  drillData = newTickets;
  showDrillModal(`New Tickets to Upload (${{newTickets.length}})`, newTickets);
}}

async function refreshMaster() {{
  document.getElementById('masterRefreshInfo').textContent = 'Refreshing master sheet...';
  await api('/api/refresh-master');
  setTimeout(() => loadMasterComparison(currentDate), 4000);
}}

async function refreshLiveStatus() {{
  checkLiveUploadStatus();
}}

// ========== DELTA ==========
function delta(curr, prev, key, invert=false) {{
  if (!prev || prev[key] == null || curr[key] == null) return '';
  // For multi-day views, compare per-day averages
  const currDays = curr.num_days || 1;
  const prevDays = prev.num_days || 1;
  const currAvg = curr[key] / currDays;
  const prevAvg = prev[key] / prevDays;
  const diff = Math.round(currAvg - prevAvg);
  let vsLabel = 'vs prev day';
  if (currDays > 1 || prevDays > 1) {{
    if (currentPeriodType === 'week') vsLabel = 'vs prev wk';
    else if (currentPeriodType === 'month') vsLabel = 'vs prev month';
    else vsLabel = 'vs prev period';
  }}
  if (diff === 0) return `<div class="card-delta neutral">&mdash; No change ${{vsLabel}}</div>`;
  const arrow = diff > 0 ? '&#9650;' : '&#9660;';
  const cls = invert ? (diff > 0 ? 'down' : 'up') : (diff > 0 ? 'up' : 'down');
  const pctChange = prevAvg > 0 ? Math.abs(Math.round((currAvg - prevAvg) / prevAvg * 100)) : 0;
  const pctStr = prevAvg > 0 ? ` (${{pctChange}}%)` : '';
  return `<div class="card-delta ${{cls}}">${{arrow}} ${{Math.abs(diff).toLocaleString()}}${{pctStr}} ${{vsLabel}}</div>`;
}}

// ========== SUMMARY CARDS ==========
let currentAggMode = 'avg';
let lastSummaryData = null;

let cachedUniqueData = null;

async function setAggMode(mode, btn) {{
  currentAggMode = mode;
  document.querySelectorAll('.agg-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  if (mode === 'unique' && !cachedUniqueData && currentRangeFrom && currentRangeTo) {{
    document.getElementById('summaryCards').innerHTML = '<div class="loading">Loading unique tickets...</div>';
    cachedUniqueData = await api(`/api/unique-tickets?from=${{currentRangeFrom}}&to=${{currentRangeTo}}`);
  }}
  if (lastSummaryData) renderSummary(lastSummaryData);
}}

function calcAgg(s, key) {{
  const days = s.num_days || 1;
  if (days <= 1) return s[key] || 0;
  const dv = s.daily_values;
  if (!dv || !dv[key]) return Math.round((s[key] || 0) / days);
  const arr = dv[key].map(x => x || 0).sort((a, b) => a - b);
  switch (currentAggMode) {{
    case 'sum': return s[key] || 0;
    case 'avg': return Math.round(arr.reduce((a, b) => a + b, 0) / arr.length);
    case 'median':
      const mid = Math.floor(arr.length / 2);
      return arr.length % 2 ? arr[mid] : Math.round((arr[mid - 1] + arr[mid]) / 2);
    case 'min': return arr[0];
    case 'max': return arr[arr.length - 1];
    default: return Math.round((s[key] || 0) / days);
  }}
}}

function uniqueCard(uniqueVal, sumVal, label, color, borderColor) {{
  const pct = sumVal > 0 ? ((uniqueVal / sumVal) * 100).toFixed(1) : '0.0';
  return `<div class="card" style="border-left:3px solid ${{borderColor}}">
    <div class="card-label">${{label}}</div>
    <div class="card-value" style="color:${{color}}">${{uniqueVal.toLocaleString()}}<span style="font-size:9px;color:#888;font-weight:400"> unique</span></div>
    <div class="card-sub">${{pct}}% of ${{sumVal.toLocaleString()}} total</div>
  </div>`;
}}

function renderSummary(s) {{
  lastSummaryData = s;
  const days = s.num_days || 1;
  const isMulti = days > 1;

  // Show/hide aggregation mode bar
  document.getElementById('aggModeBar').style.display = isMulti ? '' : 'none';

  // Unique mode — show unique counts with % of total
  if (isMulti && currentAggMode === 'unique' && cachedUniqueData && !cachedUniqueData.error) {{
    const u = cachedUniqueData;
    document.getElementById('summaryCards').innerHTML =
      uniqueCard(u.unique_total, s.total_pending, 'Total Pending Tickets', '#1a1a2e', 'var(--text2)') +
      uniqueCard(u.unique_internet, s.total_internet, 'Internet Issue Tickets', 'var(--accent)', 'var(--accent)') +
      `<div class="card" style="border-left:3px solid var(--green)">
        <div class="card-label">Created on Report Day</div>
        <div class="card-value green">${{(s.created_today || 0).toLocaleString()}}<span style="font-size:9px;color:#888;font-weight:400"> total</span></div>
        <div class="card-sub">Sum across ${{days}} days</div>
      </div>` +
      uniqueCard(u.unique_critical, s.critical_gt48h, 'Critical (> 48h)', 'var(--red)', 'var(--red)') +
      uniqueCard(u.unique_partner, s.queue_partner, 'Partner Queue', 'var(--orange)', 'var(--orange)') +
      uniqueCard(u.unique_cx_high_pain, s.queue_cx_high_pain, 'CX High Pain', '#a855f7', '#a855f7') +
      uniqueCard(u.unique_px_send_wiom, s.queue_px_send_wiom, 'PX-Send to Wiom', '#06b6d4', '#06b6d4');
    return;
  }}

  const v = (key) => isMulti ? calcAgg(s, key) : (s[key] || 0);
  const pct48 = v('total_internet') ? Math.round(v('critical_gt48h')/v('total_internet')*100) : 0;
  const pctInternet = v('total_pending') ? (v('total_internet')/v('total_pending')*100).toFixed(1) : 0;

  const modeLabels = {{ avg: 'avg/day', sum: 'total', median: 'median', min: 'min', max: 'max' }};
  const modeLabel = isMulti ? `<span style="font-size:9px;color:#888;font-weight:400"> ${{modeLabels[currentAggMode]}}</span>` : '';
  const subNote = isMulti ? `${{modeLabels[currentAggMode].charAt(0).toUpperCase() + modeLabels[currentAggMode].slice(1)}} (${{days}} days)` : '';

  document.getElementById('summaryCards').innerHTML = `
    <div class="card" style="border-left:3px solid var(--text2)">
      <div class="card-label">Total Pending Tickets</div>
      <div class="card-value" style="color:#1a1a2e">${{v('total_pending').toLocaleString()}}${{modeLabel}}</div>
      <div class="card-sub">${{isMulti ? subNote : 'All pending tickets received'}}</div>
      ${{delta(s, prevSummary, 'total_pending')}}</div>
    <div class="card" style="border-left:3px solid var(--accent)">
      <div class="card-label">Internet Issue Tickets</div>
      <div class="card-value blue">${{v('total_internet').toLocaleString()}}${{modeLabel}}</div>
      <div class="card-sub">${{pctInternet}}% of total pending</div>
      ${{delta(s, prevSummary, 'total_internet')}}</div>
    <div class="card" style="border-left:3px solid var(--green)">
      <div class="card-label">Created on Report Day</div>
      <div class="card-value green">${{v('created_today').toLocaleString()}}${{modeLabel}}</div>
      <div class="card-sub">${{isMulti ? subNote : 'New tickets that day'}}</div>
      ${{delta(s, prevSummary, 'created_today')}}</div>
    <div class="card" style="border-left:3px solid var(--red)">
      <div class="card-label">Critical (&gt; 48h)</div>
      <div class="card-value red">${{v('critical_gt48h').toLocaleString()}}${{modeLabel}}</div>
      <div class="card-sub">${{pct48}}% of internet tickets</div>
      ${{delta(s, prevSummary, 'critical_gt48h')}}</div>
    <div class="card" style="border-left:3px solid var(--orange)">
      <div class="card-label">Partner Queue</div>
      <div class="card-value orange">${{v('queue_partner').toLocaleString()}}${{modeLabel}}</div>
      <div class="card-sub">${{isMulti ? subNote : 'Waiting on partner'}}</div>
      ${{delta(s, prevSummary, 'queue_partner')}}</div>
    <div class="card" style="border-left:3px solid #a855f7">
      <div class="card-label">CX High Pain</div>
      <div class="card-value" style="color:#a855f7">${{v('queue_cx_high_pain').toLocaleString()}}${{modeLabel}}</div>
      <div class="card-sub">${{isMulti ? subNote : 'Escalated'}}</div>
      ${{delta(s, prevSummary, 'queue_cx_high_pain')}}</div>
    <div class="card" style="border-left:3px solid #06b6d4">
      <div class="card-label">PX-Send to Wiom</div>
      <div class="card-value" style="color:#06b6d4">${{v('queue_px_send_wiom').toLocaleString()}}${{modeLabel}}</div>
      <div class="card-sub">${{isMulti ? subNote : 'Wiom queue'}}</div>
      ${{delta(s, prevSummary, 'queue_px_send_wiom')}}</div>
  `;
}}

// ========== AGING TABLE ==========
function renderAging(s) {{
  const total = s.total_internet || 1;
  let rows = '';
  BUCKET_LABELS.forEach((label, i) => {{
    const count = s[BUCKET_DB_KEYS[i]] || 0;
    const pct = (count / total * 100).toFixed(1);
    rows += `<tr style="cursor:pointer" onclick="drillBucket('${{label}}')">
      <td><span class="dot" style="background:${{BUCKET_COLORS[i]}}"></span>${{label}}</td>
      <td class="num">${{count.toLocaleString()}}</td>
      <td class="num">${{pct}}%</td>
      <td><div class="bar-bg"><div class="bar-fill" style="width:${{pct}}%;background:${{BUCKET_COLORS[i]}}"></div></div></td>
      <td><button class="btn btn-sm btn-download" onclick="event.stopPropagation();downloadBucket('${{label}}')">&#11015;</button></td>
    </tr>`;
  }});
  document.getElementById('agingSection').innerHTML = `
    <div class="section-header">
      <h3>Ticket Aging Breakdown (click row to drill down)</h3>
      <button class="btn btn-sm btn-download" onclick="downloadSection('aging')">&#11015; Full CSV</button>
    </div>
    <table><thead><tr><th>Aging Bucket</th><th style="text-align:right">Tickets</th>
    <th style="text-align:right">%</th><th>Distribution</th><th style="width:40px"></th></tr></thead><tbody>${{rows}}</tbody></table>`;
}}

// ========== AGING DAILY TREND ==========
window._agingSelectedL3 = [];
window._agingSelectedL4 = [];

async function loadAgingDailyTrend(overrideFrom, overrideTo) {{
  const container = document.getElementById('agingTrendContent');
  const l3Vals = window._agingSelectedL3;
  const l4Vals = window._agingSelectedL4;

  container.innerHTML = '<div class="loading">Loading aging trend...</div>';

  try {{
    let fromDate, toDate;
    if (overrideFrom && overrideTo) {{
      fromDate = overrideFrom;
      toDate = overrideTo;
    }} else {{
      const refDate = currentDate || (availableDates.length > 0 ? availableDates[0] : null);
      if (!refDate) {{ container.innerHTML = '<div class="loading">No dates available</div>'; return; }}
      toDate = refDate;
      const to = new Date(refDate + 'T00:00:00');
      to.setDate(to.getDate() - 6);
      fromDate = localDateStr(to);
    }}
    document.getElementById('agingTrendFrom').value = fromDate;
    document.getElementById('agingTrendTo').value = toDate;

    let url = `/api/aging-daily-trend?from=${{fromDate}}&to=${{toDate}}`;
    if (l3Vals.length) url += `&l3=${{encodeURIComponent(l3Vals.join(','))}}`;
    if (l4Vals.length) url += `&l4=${{encodeURIComponent(l4Vals.join(','))}}`;

    const data = await api(url);
    if (!data || data.error || !data.dates || !data.dates.length) {{
      container.innerHTML = '<div class="loading">No aging trend data for this range</div>';
      return;
    }}
    const dates = data.dates, buckets = data.buckets;
    window._agingTrendDates = dates;
    window._agingTrendBuckets = buckets;
    const l3Options = data.available_l3 || [], l4Options = data.available_l4 || [];

    // --- Multi-select L3 dropdown ---
    let l3Items = '';
    l3Options.forEach(v => {{
      l3Items += `<label style="display:flex;align-items:center;gap:6px;padding:4px 10px;cursor:pointer;font-size:11px;white-space:nowrap">
        <input type="checkbox" ${{l3Vals.includes(v)?'checked':''}} data-aging-l3="${{v}}" style="cursor:pointer"> ${{v}}</label>`;
    }});
    const l3Cnt = l3Vals.length;
    const l3Html = `<div style="position:relative;display:inline-block" id="agingL3Container">
      <button onclick="document.getElementById('agingL3Dropdown').classList.toggle('show')"
        style="padding:4px 10px;border:1px solid ${{l3Cnt?'#6366f1':'var(--border)'}};border-radius:6px;background:${{l3Cnt?'#eef2ff':'#fff'}};cursor:pointer;font-size:11px;font-family:inherit;display:flex;align-items:center;gap:4px;color:${{l3Cnt?'#4338ca':'inherit'}}">
        ${{l3Cnt ? 'Category L3 ('+l3Cnt+')' : 'All Categories (L3)'}} &#9660;</button>
      <div id="agingL3Dropdown" style="display:none;position:absolute;left:0;top:100%;margin-top:4px;background:#fff;border:1px solid var(--border);border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.1);z-index:100;min-width:250px;max-height:300px;overflow-y:auto">
        <div style="display:flex;gap:6px;padding:8px 10px;border-bottom:1px solid var(--border)">
          <button onclick="document.querySelectorAll('#agingL3Dropdown input[data-aging-l3]').forEach(c=>c.checked=true);agingApplyL3()"
            style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#f0fdf4;cursor:pointer;font-size:10px">All</button>
          <button onclick="document.querySelectorAll('#agingL3Dropdown input[data-aging-l3]').forEach(c=>c.checked=false);agingApplyL3()"
            style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#fef2f2;cursor:pointer;font-size:10px">None</button>
          <button onclick="agingApplyL3();document.getElementById('agingL3Dropdown').classList.remove('show')"
            style="flex:1;padding:3px;border:1px solid #6366f1;border-radius:4px;background:#eef2ff;cursor:pointer;font-size:10px;color:#4338ca;font-weight:600">Apply</button>
        </div>
        ${{l3Items}}
      </div></div>`;

    // --- Multi-select L4 dropdown ---
    let l4Items = '';
    l4Options.forEach(v => {{
      l4Items += `<label style="display:flex;align-items:center;gap:6px;padding:4px 10px;cursor:pointer;font-size:11px;white-space:nowrap">
        <input type="checkbox" ${{l4Vals.includes(v)?'checked':''}} data-aging-l4="${{v}}" style="cursor:pointer"> ${{v}}</label>`;
    }});
    const l4Cnt = l4Vals.length;
    const l4Disabled = l3Cnt === 0;
    const l4Html = l4Disabled
      ? `<button disabled style="padding:4px 10px;border:1px solid var(--border);border-radius:6px;background:#f5f5f5;font-size:11px;color:#aaa;cursor:not-allowed">All Sub-categories (L4) &#9660;</button>`
      : `<div style="position:relative;display:inline-block" id="agingL4Container">
      <button onclick="document.getElementById('agingL4Dropdown').classList.toggle('show')"
        style="padding:4px 10px;border:1px solid ${{l4Cnt?'#d97706':'var(--border)'}};border-radius:6px;background:${{l4Cnt?'#fffbeb':'#fff'}};cursor:pointer;font-size:11px;font-family:inherit;display:flex;align-items:center;gap:4px;color:${{l4Cnt?'#92400e':'inherit'}}">
        ${{l4Cnt ? 'Sub-cat L4 ('+l4Cnt+')' : 'All Sub-categories (L4)'}} &#9660;</button>
      <div id="agingL4Dropdown" style="display:none;position:absolute;left:0;top:100%;margin-top:4px;background:#fff;border:1px solid var(--border);border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.1);z-index:100;min-width:280px;max-height:300px;overflow-y:auto">
        <div style="display:flex;gap:6px;padding:8px 10px;border-bottom:1px solid var(--border)">
          <button onclick="document.querySelectorAll('#agingL4Dropdown input[data-aging-l4]').forEach(c=>c.checked=true);agingApplyL4()"
            style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#f0fdf4;cursor:pointer;font-size:10px">All</button>
          <button onclick="document.querySelectorAll('#agingL4Dropdown input[data-aging-l4]').forEach(c=>c.checked=false);agingApplyL4()"
            style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#fef2f2;cursor:pointer;font-size:10px">None</button>
          <button onclick="agingApplyL4();document.getElementById('agingL4Dropdown').classList.remove('show')"
            style="flex:1;padding:3px;border:1px solid #d97706;border-radius:4px;background:#fffbeb;cursor:pointer;font-size:10px;color:#92400e;font-weight:600">Apply</button>
        </div>
        ${{l4Items}}
      </div></div>`;

    // Filter badges
    let badges = '';
    l3Vals.forEach(v => {{ badges += `<span style="background:#e0e7ff;color:#3730a3;padding:2px 8px;border-radius:12px;font-size:10px;font-weight:600">${{v}}</span> `; }});
    l4Vals.forEach(v => {{ badges += `<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:12px;font-size:10px;font-weight:600">${{v}}</span> `; }});
    if (l3Vals.length || l4Vals.length) badges += `<button onclick="clearAgingCatFilters()" style="background:none;border:none;color:#ef4444;cursor:pointer;font-size:11px;font-weight:600">&#10005; Clear</button>`;

    const bucketNames = BUCKET_LABELS;
    const dailyTotals = {{}};
    dates.forEach(d => {{ dailyTotals[d] = bucketNames.reduce((s,b) => s + ((buckets[b]&&buckets[b][d])||0), 0); }});

    function shortCol(ds) {{ const dt = new Date(ds+'T00:00:00'); return dt.toLocaleDateString('en-IN',{{day:'numeric',month:'short'}}); }}

    let headerCells = `<th style="text-align:left;min-width:160px;position:sticky;left:0;background:#f8fafc;z-index:2">Aging Bucket</th>`;
    dates.forEach(d => {{ headerCells += `<th style="text-align:center;min-width:75px;white-space:nowrap;font-size:11px">${{shortCol(d)}}</th>`; }});

    let bodyRows = '';
    bucketNames.forEach((label, i) => {{
      const color = BUCKET_COLORS[i];
      let cells = `<td style="position:sticky;left:0;background:#fff;z-index:1;white-space:nowrap"><span class="dot" style="background:${{color}}"></span>${{label}}</td>`;
      dates.forEach(d => {{
        const count = (buckets[label]&&buckets[label][d])||0;
        const total = dailyTotals[d]||1;
        const pct = (count/total*100).toFixed(1);
        const dlUrl = `/api/download-filtered?date=${{d}}&bucket=${{encodeURIComponent(label)}}`;
        cells += `<td class="num" style="font-size:11px;cursor:pointer" title="${{count.toLocaleString()}} tickets" onclick="window.location.href='${{dlUrl}}'">
          ${{count > 0 ? count.toLocaleString() : '—'}}
          ${{count > 0 ? '<div style=\\"font-size:9px;color:#94a3b8;font-weight:400\\">' + pct + '%</div>' : ''}}</td>`;
      }});
      bodyRows += `<tr data-agingrow="${{label}}">${{cells}}</tr>`;
    }});

    let totalCells = `<td style="position:sticky;left:0;background:#f1f5f9;z-index:1;font-weight:700">TOTAL</td>`;
    dates.forEach(d => {{ totalCells += `<td class="num" style="font-weight:700;font-size:11px">${{(dailyTotals[d]||0).toLocaleString()}}</td>`; }});

    container.innerHTML = `
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;flex-wrap:wrap">${{l3Html}} ${{l4Html}}</div>
      ${{badges ? '<div style="margin-bottom:8px;display:flex;align-items:center;gap:4px;flex-wrap:wrap">'+badges+'</div>' : ''}}
      <div style="overflow-x:auto;border:1px solid var(--border);border-radius:8px">
        <table style="min-width:100%;border-collapse:collapse">
          <thead><tr style="background:#f8fafc;border-bottom:2px solid var(--border)">${{headerCells}}</tr></thead>
          <tbody>${{bodyRows}}<tr style="border-top:2px solid var(--border);background:#f1f5f9">${{totalCells}}</tr></tbody>
        </table>
      </div>
      <div style="font-size:11px;color:var(--text2);margin-top:6px">
        Showing ${{dates.length}} day(s) from ${{shortCol(dates[0])}} to ${{shortCol(dates[dates.length-1])}} &mdash; each cell shows count + % of daily total
      </div>`;

    // Build bucket filter dropdown
    let filterItems = '';
    bucketNames.forEach((label, i) => {{
      filterItems += `<label style="display:flex;align-items:center;gap:6px;padding:4px 10px;cursor:pointer;font-size:12px;white-space:nowrap">
        <input type="checkbox" checked data-agingtrend="${{label}}" onchange="filterAgingTrend()" style="accent-color:${{BUCKET_COLORS[i]}};cursor:pointer"> ${{label}}</label>`;
    }});
    const fc = document.getElementById('agingTrendFilterContainer');
    if (fc) fc.innerHTML = `<div style="position:relative;display:inline-block">
      <button onclick="document.getElementById('agingTrendDropdown').classList.toggle('show')"
        style="padding:4px 10px;border:1px solid var(--border);border-radius:6px;background:#fff;cursor:pointer;font-size:11px;font-family:inherit;display:flex;align-items:center;gap:4px">
        &#9776; Filter Buckets <span id="agingTrendFilterCount">(${{bucketNames.length}}/${{bucketNames.length}})</span> &#9660;</button>
      <div id="agingTrendDropdown" style="display:none;position:absolute;right:0;top:100%;margin-top:4px;background:#fff;border:1px solid var(--border);border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.1);z-index:100;min-width:200px;max-height:300px;overflow-y:auto">
        <div style="display:flex;gap:6px;padding:8px 10px;border-bottom:1px solid var(--border)">
          <button onclick="document.querySelectorAll('#agingTrendDropdown input[data-agingtrend]').forEach(c=>c.checked=true);filterAgingTrend()"
            style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#f0fdf4;cursor:pointer;font-size:10px">All</button>
          <button onclick="document.querySelectorAll('#agingTrendDropdown input[data-agingtrend]').forEach(c=>c.checked=false);filterAgingTrend()"
            style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#fef2f2;cursor:pointer;font-size:10px">None</button>
        </div>${{filterItems}}</div></div>`;

    // Close dropdowns on outside click
    document.addEventListener('click', function(e) {{
      ['agingTrendDropdown','agingL3Dropdown','agingL4Dropdown'].forEach(id => {{
        const dd = document.getElementById(id);
        if (!dd) return;
        const inside = ['agingTrendFilterContainer','agingL3Container','agingL4Container'].some(cid => {{
          const c = document.getElementById(cid); return c && c.contains(e.target);
        }});
        if (!inside) dd.classList.remove('show');
      }});
    }});

  }} catch(e) {{
    container.innerHTML = '<div class="loading">Could not load aging trend</div>';
  }}
}}

// L3/L4 multi-select handlers
window.agingApplyL3 = function() {{
  window._agingSelectedL3 = Array.from(document.querySelectorAll('#agingL3Dropdown input[data-aging-l3]:checked')).map(c => c.getAttribute('data-aging-l3'));
  window._agingSelectedL4 = []; // reset L4
  const from = document.getElementById('agingTrendFrom').value, to = document.getElementById('agingTrendTo').value;
  if (from && to) loadAgingDailyTrend(from, to);
}}
window.agingApplyL4 = function() {{
  window._agingSelectedL4 = Array.from(document.querySelectorAll('#agingL4Dropdown input[data-aging-l4]:checked')).map(c => c.getAttribute('data-aging-l4'));
  const from = document.getElementById('agingTrendFrom').value, to = document.getElementById('agingTrendTo').value;
  if (from && to) loadAgingDailyTrend(from, to);
}}
window.clearAgingCatFilters = function() {{
  window._agingSelectedL3 = [];
  window._agingSelectedL4 = [];
  const from = document.getElementById('agingTrendFrom').value, to = document.getElementById('agingTrendTo').value;
  if (from && to) loadAgingDailyTrend(from, to);
}}

// Show/hide aging rows and recalculate totals
window.filterAgingTrend = function() {{
  const checked = Array.from(document.querySelectorAll('#agingTrendDropdown input[data-agingtrend]:checked')).map(c => c.getAttribute('data-agingtrend'));
  const totalCount = document.querySelectorAll('#agingTrendDropdown input[data-agingtrend]').length;
  document.getElementById('agingTrendFilterCount').textContent = `(${{checked.length}}/${{totalCount}})`;

  const table = document.querySelector('#agingTrendContent table');
  if (!table) return;
  const rows = table.querySelectorAll('tbody tr');
  let totalRow = null;

  rows.forEach(row => {{
    const firstTd = row.querySelector('td');
    if (!firstTd) return;
    const text = firstTd.textContent.trim();
    if (text === 'TOTAL') {{
      totalRow = row;
      return;
    }}
    const agingLabel = row.getAttribute('data-agingrow');
    row.style.display = agingLabel && checked.includes(agingLabel) ? '' : 'none';
  }});

  // Recalculate TOTAL and %
  if (totalRow && window._agingTrendDates && window._agingTrendBuckets) {{
    const dates = window._agingTrendDates;
    const buckets = window._agingTrendBuckets;
    const tds = totalRow.querySelectorAll('td');

    dates.forEach((d, i) => {{
      let filteredTotal = 0;
      checked.forEach(b => {{
        filteredTotal += (buckets[b] && buckets[b][d]) || 0;
      }});
      if (tds[i + 1]) tds[i + 1].textContent = filteredTotal.toLocaleString();
    }});

    rows.forEach(row => {{
      if (row === totalRow || row.style.display === 'none') return;
      const agingLabel = row.getAttribute('data-agingrow');
      if (!agingLabel) return;
      const cells = row.querySelectorAll('td');
      dates.forEach((d, i) => {{
        const count = (buckets[agingLabel] && buckets[agingLabel][d]) || 0;
        let filteredTotal = 0;
        checked.forEach(b => {{
          filteredTotal += (buckets[b] && buckets[b][d]) || 0;
        }});
        const pct = filteredTotal > 0 ? (count / filteredTotal * 100).toFixed(1) : '0.0';
        if (cells[i + 1]) {{
          cells[i + 1].innerHTML = count > 0
            ? count.toLocaleString() + '<div style="font-size:9px;color:#94a3b8;font-weight:400">' + pct + '%</div>'
            : '—';
        }}
      }});
    }});
  }}
}}

function applyAgingTrendFilter() {{
  const from = document.getElementById('agingTrendFrom').value;
  const to = document.getElementById('agingTrendTo').value;
  if (!from || !to) {{
    alert('Please select both FROM and TO dates');
    return;
  }}
  if (from > to) {{
    alert('FROM date must be before TO date');
    return;
  }}
  const d1 = new Date(from), d2 = new Date(to);
  const diff = (d2 - d1) / (1000 * 60 * 60 * 24);
  if (diff > 120) {{
    alert('Max range is 120 days');
    return;
  }}
  loadAgingDailyTrend(from, to);
}}

// ========== AGING TREND CHART (Independent Section) ==========
let _agingChart = null;
let _agingChartType = 'line';
window._chartDates = [];
window._chartCategories = {{}};
window._chartGroupBy = 'l3';
window._chartSelectedL3 = ['Internet Issues'];
window._chartSelectedL4 = [];
window._chartSelectedBuckets = BUCKET_LABELS.slice();
window._chartExpandL4 = false; // false = show L3, true = drill into L4
window._chartSelectedQueues = [];

// 20 distinct colors for category lines
const CHART_COLORS = [
  '#2563eb','#dc2626','#16a34a','#ea580c','#9333ea','#ca8a04','#0891b2','#be123c',
  '#65a30d','#7c3aed','#0d9488','#db2777','#4f46e5','#059669','#d97706','#6366f1',
  '#e11d48','#14b8a6','#f59e0b','#8b5cf6'
];

function pickAgingChart(type, label, item) {{
  _agingChartType = type;
  document.querySelectorAll('.chart-dd-item').forEach(i => i.classList.remove('active'));
  if (item) item.classList.add('active');
  document.getElementById('agingChartLabel').textContent = label;
  document.getElementById('agingChartDropdown').classList.remove('show');
  renderAgingChart();
}}

// Close dropdowns on outside click
document.addEventListener('click', function(e) {{
  ['agingChartDropdown','chartBucketDD','chartL3DD','chartQueueDD'].forEach(id => {{
    const dd = document.getElementById(id);
    if (!dd) return;
    const containers = ['agingChartTypeContainer','chartBucketFilterContainer','chartL3Container','chartL4Container','chartQueueContainer'];
    const inside = containers.some(cid => {{ const c = document.getElementById(cid); return c && c.contains(e.target); }});
    if (!inside) dd.classList.remove('show');
  }});
}});

async function loadAgingChart(overrideFrom, overrideTo) {{
  let fromDate, toDate;
  if (overrideFrom && overrideTo) {{
    fromDate = overrideFrom;
    toDate = overrideTo;
  }} else {{
    const refDate = currentDate || (availableDates.length > 0 ? availableDates[0] : null);
    if (!refDate) return;
    toDate = refDate;
    const to = new Date(refDate + 'T00:00:00');
    to.setDate(to.getDate() - 6);
    fromDate = localDateStr(to);
  }}
  document.getElementById('chartFrom').value = fromDate;
  document.getElementById('chartTo').value = toDate;

  const l3 = window._chartSelectedL3;
  const l4 = window._chartSelectedL4;
  const buckets = window._chartSelectedBuckets;
  const expandL4 = window._chartExpandL4 && l3.length === 1;

  const queues = window._chartSelectedQueues;

  let url = `/api/category-trend-chart?from=${{fromDate}}&to=${{toDate}}`;
  if (buckets.length && buckets.length < BUCKET_LABELS.length) url += `&buckets=${{encodeURIComponent(buckets.join(','))}}`;
  if (l3.length) url += `&l3=${{encodeURIComponent(l3.join(','))}}`;
  if (l4.length) url += `&l4=${{encodeURIComponent(l4.join(','))}}`;
  if (queues.length) url += `&queue=${{encodeURIComponent(queues.join(','))}}`;
  if (expandL4) url += `&expand_l4=1`;
  // If expand L4 but multiple L3s, force L3 view
  if (l3.length !== 1) window._chartExpandL4 = false;

  try {{
    const data = await api(url);
    if (!data || !data.dates || !data.dates.length) return;
    window._chartDates = data.dates;
    window._chartCategories = data.categories || {{}};
    window._chartGroupBy = data.group_by || 'l3';
    const l3Options = data.available_l3 || [];
    const l4Options = data.available_l4 || [];

    // Build Bucket filter (aging filter, not grouping) — Apply button only, no auto-apply
    const bfc = document.getElementById('chartBucketFilterContainer');
    if (bfc) {{
      const selBuckets = window._chartSelectedBuckets;
      let bItems = '';
      BUCKET_LABELS.forEach((b, i) => {{
        bItems += `<label style="display:flex;align-items:center;gap:6px;padding:4px 10px;cursor:pointer;font-size:11px;white-space:nowrap">
          <input type="checkbox" ${{selBuckets.includes(b)?'checked':''}} data-chartbucket="${{b}}" style="accent-color:${{BUCKET_COLORS[i]}};cursor:pointer"> ${{b}}</label>`;
      }});
      bfc.innerHTML = `<div style="position:relative;display:inline-block">
        <button onclick="document.getElementById('chartBucketDD').classList.toggle('show')"
          style="padding:4px 10px;border:1px solid var(--border);border-radius:6px;background:#fff;cursor:pointer;font-size:11px;font-family:inherit;display:flex;align-items:center;gap:4px">
          &#9776; Aging Filter <span id="chartBucketCount">(${{selBuckets.length}}/${{BUCKET_LABELS.length}})</span> &#9660;</button>
        <div id="chartBucketDD" style="display:none;position:absolute;right:0;top:100%;margin-top:4px;background:#fff;border:1px solid var(--border);border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.1);z-index:100;min-width:200px;max-height:300px;overflow-y:auto">
          <div style="display:flex;gap:6px;padding:8px 10px;border-bottom:1px solid var(--border)">
            <button onclick="document.querySelectorAll('#chartBucketDD input[data-chartbucket]').forEach(c=>c.checked=true)"
              style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#f0fdf4;cursor:pointer;font-size:10px">All</button>
            <button onclick="document.querySelectorAll('#chartBucketDD input[data-chartbucket]').forEach(c=>c.checked=false)"
              style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#fef2f2;cursor:pointer;font-size:10px">None</button>
            <button onclick="chartApplyBuckets();document.getElementById('chartBucketDD').classList.remove('show')"
              style="flex:1;padding:3px;border:1px solid #2563eb;border-radius:4px;background:#eff6ff;cursor:pointer;font-size:10px;color:#1d4ed8;font-weight:600">Apply</button>
          </div>${{bItems}}</div></div>`;
    }}

    // Build L3 dropdown
    const l3c = document.getElementById('chartL3Container');
    if (l3c) {{
      let l3Items = '';
      l3Options.forEach(v => {{
        l3Items += `<label style="display:flex;align-items:center;gap:6px;padding:4px 10px;cursor:pointer;font-size:11px;white-space:nowrap">
          <input type="checkbox" ${{l3.includes(v)?'checked':''}} data-chartl3="${{v}}" style="cursor:pointer"> ${{v}}</label>`;
      }});
      const l3Cnt = l3.length;
      l3c.innerHTML = `<div style="position:relative;display:inline-block">
        <button onclick="document.getElementById('chartL3DD').classList.toggle('show')"
          style="padding:4px 10px;border:1px solid ${{l3Cnt?'#6366f1':'var(--border)'}};border-radius:6px;background:${{l3Cnt?'#eef2ff':'#fff'}};cursor:pointer;font-size:11px;font-family:inherit;display:flex;align-items:center;gap:4px;color:${{l3Cnt?'#4338ca':'inherit'}}">
          ${{l3Cnt ? 'L3 ('+l3Cnt+')' : 'All Categories (L3)'}} &#9660;</button>
        <div id="chartL3DD" style="display:none;position:absolute;left:0;top:100%;margin-top:4px;background:#fff;border:1px solid var(--border);border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.1);z-index:100;min-width:250px;max-height:300px;overflow-y:auto">
          <div style="display:flex;gap:6px;padding:8px 10px;border-bottom:1px solid var(--border)">
            <button onclick="document.querySelectorAll('#chartL3DD input[data-chartl3]').forEach(c=>c.checked=true)"
              style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#f0fdf4;cursor:pointer;font-size:10px">All</button>
            <button onclick="document.querySelectorAll('#chartL3DD input[data-chartl3]').forEach(c=>c.checked=false)"
              style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#fef2f2;cursor:pointer;font-size:10px">None</button>
            <button onclick="chartApplyL3();document.getElementById('chartL3DD').classList.remove('show')"
              style="flex:1;padding:3px;border:1px solid #6366f1;border-radius:4px;background:#eef2ff;cursor:pointer;font-size:10px;color:#4338ca;font-weight:600">Apply</button>
          </div>${{l3Items}}</div></div>`;
    }}

    // Build L4 expand button (only when exactly 1 L3 selected)
    const l4c = document.getElementById('chartL4Container');
    if (l4c) {{
      if (l3.length === 1) {{
        const expanded = window._chartExpandL4;
        l4c.innerHTML = `<button onclick="chartToggleL4()"
          style="padding:4px 12px;border:1px solid ${{expanded?'#d97706':'var(--border)'}};border-radius:6px;background:${{expanded?'#fffbeb':'#fff'}};cursor:pointer;font-size:11px;font-family:inherit;display:flex;align-items:center;gap:4px;color:${{expanded?'#92400e':'inherit'}};font-weight:${{expanded?'600':'normal'}}">
          ${{expanded ? '&#9660; Collapse to L3' : '&#9654; Expand L4'}}
        </button>`;
      }} else {{
        l4c.innerHTML = '';
        window._chartExpandL4 = false;
      }}
    }}

    // Build Queue filter dropdown
    const queueOptions = data.available_queues || [];
    const qc = document.getElementById('chartQueueContainer');
    if (qc) {{
      if (!queueOptions.length) {{
        qc.innerHTML = `<button disabled style="padding:4px 10px;border:1px solid var(--border);border-radius:6px;background:#f5f5f5;font-size:11px;color:#aaa;cursor:not-allowed" title="Queue filter available for last 7 days only">Queue &#9660;</button>`;
      }} else {{
        let qItems = '';
        queueOptions.forEach(v => {{
          qItems += `<label style="display:flex;align-items:center;gap:6px;padding:4px 10px;cursor:pointer;font-size:11px;white-space:nowrap">
            <input type="checkbox" ${{queues.includes(v)?'checked':''}} data-chartqueue="${{v}}" style="cursor:pointer"> ${{v}}</label>`;
        }});
        const qCnt = queues.length;
        qc.innerHTML = `<div style="position:relative;display:inline-block">
          <button onclick="document.getElementById('chartQueueDD').classList.toggle('show')"
            style="padding:4px 10px;border:1px solid ${{qCnt?'#0891b2':'var(--border)'}};border-radius:6px;background:${{qCnt?'#ecfeff':'#fff'}};cursor:pointer;font-size:11px;font-family:inherit;display:flex;align-items:center;gap:4px;color:${{qCnt?'#0e7490':'inherit'}}">
            ${{qCnt ? 'Queue ('+qCnt+')' : 'All Queues'}} &#9660;</button>
          <div id="chartQueueDD" style="display:none;position:absolute;left:0;top:100%;margin-top:4px;background:#fff;border:1px solid var(--border);border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.1);z-index:100;min-width:250px;max-height:300px;overflow-y:auto">
            <div style="display:flex;gap:6px;padding:8px 10px;border-bottom:1px solid var(--border)">
              <button onclick="document.querySelectorAll('#chartQueueDD input[data-chartqueue]').forEach(c=>c.checked=true);chartApplyQueue()"
                style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#f0fdf4;cursor:pointer;font-size:10px">All</button>
              <button onclick="document.querySelectorAll('#chartQueueDD input[data-chartqueue]').forEach(c=>c.checked=false);chartApplyQueue()"
                style="flex:1;padding:3px;border:1px solid var(--border);border-radius:4px;background:#fef2f2;cursor:pointer;font-size:10px">None</button>
              <button onclick="chartApplyQueue();document.getElementById('chartQueueDD').classList.remove('show')"
                style="flex:1;padding:3px;border:1px solid #0891b2;border-radius:4px;background:#ecfeff;cursor:pointer;font-size:10px;color:#0e7490;font-weight:600">Apply</button>
            </div>${{qItems}}</div></div>`;
      }}
    }}

    // Subtitle showing what each line represents
    const subtitle = document.getElementById('chartSubtitle');
    if (subtitle) {{
      const isL4 = window._chartGroupBy === 'l4';
      subtitle.innerHTML = `<span style="font-size:11px;color:#6b7280">Each line/bar = <strong>${{isL4 ? 'L4 Sub-categories' : 'L3 Categories'}}</strong></span>`;
    }}

    // Badges
    let badges = '';
    l3.forEach(v => {{ badges += `<span style="background:#e0e7ff;color:#3730a3;padding:2px 8px;border-radius:12px;font-size:10px;font-weight:600">${{v}}</span> `; }});
    l4.forEach(v => {{ badges += `<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:12px;font-size:10px;font-weight:600">${{v}}</span> `; }});
    if (buckets.length < BUCKET_LABELS.length) badges += `<span style="background:#fef2f2;color:#dc2626;padding:2px 8px;border-radius:12px;font-size:10px;font-weight:600">Aging: ${{buckets.length}}/${{BUCKET_LABELS.length}}</span> `;
    queues.forEach(v => {{ badges += `<span style="background:#f0fdfa;color:#0f766e;padding:2px 8px;border-radius:12px;font-size:10px;font-weight:600">${{v}}</span> `; }});
    if (l3.length || l4.length || buckets.length < BUCKET_LABELS.length || queues.length) badges += `<button onclick="chartClearFilters()" style="background:none;border:none;color:#ef4444;cursor:pointer;font-size:11px;font-weight:600">&#10005; Clear All</button>`;
    document.getElementById('chartBadges').innerHTML = badges;

    renderAgingChart();
  }} catch(e) {{
    console.error('Chart load error:', e);
  }}
}}

window.chartApplyBuckets = function() {{
  window._chartSelectedBuckets = Array.from(document.querySelectorAll('#chartBucketDD input[data-chartbucket]:checked')).map(c => c.getAttribute('data-chartbucket'));
  const total = document.querySelectorAll('#chartBucketDD input[data-chartbucket]').length;
  const el = document.getElementById('chartBucketCount');
  if (el) el.textContent = `(${{window._chartSelectedBuckets.length}}/${{total}})`;
  const from = document.getElementById('chartFrom').value, to = document.getElementById('chartTo').value;
  if (from && to) loadAgingChart(from, to);
}};

window.chartApplyL3 = function() {{
  window._chartSelectedL3 = Array.from(document.querySelectorAll('#chartL3DD input[data-chartl3]:checked')).map(c => c.getAttribute('data-chartl3'));
  window._chartSelectedL4 = [];
  window._chartExpandL4 = false;
  const from = document.getElementById('chartFrom').value, to = document.getElementById('chartTo').value;
  if (from && to) loadAgingChart(from, to);
}};

window.chartToggleL4 = function() {{
  window._chartExpandL4 = !window._chartExpandL4;
  const from = document.getElementById('chartFrom').value, to = document.getElementById('chartTo').value;
  if (from && to) loadAgingChart(from, to);
}};

window.chartApplyQueue = function() {{
  window._chartSelectedQueues = Array.from(document.querySelectorAll('#chartQueueDD input[data-chartqueue]:checked')).map(c => c.getAttribute('data-chartqueue'));
  const from = document.getElementById('chartFrom').value, to = document.getElementById('chartTo').value;
  if (from && to) loadAgingChart(from, to);
}};

window.chartClearFilters = function() {{
  window._chartSelectedL3 = [];
  window._chartSelectedL4 = [];
  window._chartSelectedBuckets = BUCKET_LABELS.slice();
  window._chartSelectedQueues = [];
  window._chartExpandL4 = false;
  const from = document.getElementById('chartFrom').value, to = document.getElementById('chartTo').value;
  if (from && to) loadAgingChart(from, to);
}};

function applyChartFilter() {{
  const from = document.getElementById('chartFrom').value;
  const to = document.getElementById('chartTo').value;
  if (!from || !to) {{ alert('Please select both FROM and TO dates'); return; }}
  if (from > to) {{ alert('FROM date must be before TO date'); return; }}
  const d1 = new Date(from), d2 = new Date(to);
  if ((d2 - d1) / 86400000 > 90) {{ alert('Max range is 90 days'); return; }}
  loadAgingChart(from, to);
}}

function renderAgingChart() {{
  const dates = window._chartDates;
  const categories = window._chartCategories;
  if (!dates || !dates.length || !categories) return;

  const canvas = document.getElementById('agingTrendChart');
  if (!canvas) return;

  if (_agingChart) {{ _agingChart.destroy(); _agingChart = null; }}

  const catNames = Object.keys(categories);
  if (!catNames.length) return;

  const shortLabel = (ds) => {{ const dt = new Date(ds+'T00:00:00'); return dt.toLocaleDateString('en-IN',{{day:'numeric',month:'short'}}); }};
  const labels = dates.map(shortLabel);
  const type = _agingChartType;

  // Build datasets — each line/bar = one L3 or L4 category
  const datasets = [];
  catNames.forEach((catName, i) => {{
    const color = CHART_COLORS[i % CHART_COLORS.length];
    const data = dates.map(d => (categories[catName] && categories[catName][d]) || 0);

    if (type === 'pie' || type === 'doughnut') return;

    const ds = {{
      label: catName,
      data: data,
      backgroundColor: color + (type === 'area' || type === 'stackedArea' ? '55' : 'cc'),
      borderColor: color,
      borderWidth: type === 'line' || type === 'area' || type === 'stackedArea' || type === 'combo' ? 3 : 1,
      pointRadius: type === 'line' || type === 'area' || type === 'stackedArea' ? 5 : 0,
      pointHoverRadius: 7,
      pointBackgroundColor: color,
      pointBorderColor: '#fff',
      pointBorderWidth: 2,
      tension: 0.3,
    }};

    if (type === 'area' || type === 'stackedArea') ds.fill = type === 'stackedArea' ? 'origin' : true;
    if (type === 'combo') {{
      if (i < Math.ceil(catNames.length / 2)) {{ ds.type = 'bar'; }}
      else {{ ds.type = 'line'; ds.borderWidth = 3; ds.pointRadius = 5; ds.pointBorderWidth = 2; ds.pointBorderColor = '#fff'; ds.fill = false; }}
    }}
    datasets.push(ds);
  }});

  // Pie / Doughnut
  if (type === 'pie' || type === 'doughnut') {{
    const totals = catNames.map(cat => dates.reduce((sum, d) => sum + ((categories[cat] && categories[cat][d]) || 0), 0));
    const colors = catNames.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]);
    _agingChart = new Chart(canvas, {{
      type: type,
      data: {{ labels: catNames, datasets: [{{ data: totals, backgroundColor: colors.map(c => c + 'cc'), borderColor: colors, borderWidth: 2 }}] }},
      plugins: [ChartDataLabels],
      options: {{
        responsive: true, maintainAspectRatio: false,
        plugins: {{
          legend: {{ position: 'right', labels: {{ font: {{ size: 12, weight: 'bold' }}, padding: 12 }} }},
          title: {{ display: true, text: `Category Distribution (Total across ${{dates.length}} days)`, font: {{ size: 13 }} }},
          tooltip: {{ callbacks: {{ label: function(ctx) {{
            const total = ctx.dataset.data.reduce((a,b) => a + b, 0);
            return `${{ctx.label}}: ${{ctx.raw.toLocaleString()}} (${{total > 0 ? (ctx.raw/total*100).toFixed(1) : 0}}%)`;
          }} }} }},
          datalabels: {{
            color: '#fff',
            font: {{ size: 11, weight: 'bold' }},
            formatter: function(val, ctx) {{
              const total = ctx.dataset.data.reduce((a,b) => a + b, 0);
              const pct = total > 0 ? (val/total*100).toFixed(1) : 0;
              return pct > 4 ? val.toLocaleString() : '';
            }}
          }}
        }}
      }}
    }});
    return;
  }}

  // Radar
  if (type === 'radar') {{
    _agingChart = new Chart(canvas, {{
      type: 'radar',
      data: {{ labels: dates.map(shortLabel), datasets: catNames.map((cat, ci) => {{
        const color = CHART_COLORS[ci % CHART_COLORS.length];
        return {{ label: cat, data: dates.map(d => (categories[cat] && categories[cat][d]) || 0),
          borderColor: color, backgroundColor: color + '30', borderWidth: 2, pointRadius: 3 }};
      }}) }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        plugins: {{ legend: {{ position: 'right', labels: {{ font: {{ size: 10 }}, padding: 6 }} }} }},
        scales: {{ r: {{ beginAtZero: true, ticks: {{ font: {{ size: 9 }} }}, pointLabels: {{ font: {{ size: 10 }} }} }} }}
      }}
    }});
    return;
  }}

  // 100% stacked
  if (type === 'percent') {{
    dates.forEach((d, di) => {{
      const total = catNames.reduce((s, cat) => s + ((categories[cat] && categories[cat][d]) || 0), 0) || 1;
      datasets.forEach(ds => {{ ds.data[di] = parseFloat((ds.data[di] / total * 100).toFixed(1)); }});
    }});
  }}

  let chartType = 'bar';
  if (type === 'line' || type === 'area' || type === 'stackedArea') chartType = 'line';
  if (type === 'combo') chartType = 'bar';
  const stacked = type === 'stackedBar' || type === 'stackedArea' || type === 'percent';

  const showDatalabels = (type === 'line' || type === 'area' || type === 'combo');
  const isStacked = (type === 'stackedBar' || type === 'stackedArea' || type === 'percent');
  const fewCategories = catNames.length <= 5;

  _agingChart = new Chart(canvas, {{
    type: chartType,
    data: {{ labels, datasets }},
    plugins: [ChartDataLabels],
    options: {{
      responsive: true, maintainAspectRatio: false,
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{
        legend: {{
          position: 'bottom',
          labels: {{
            font: {{ size: 12, weight: 'bold' }},
            padding: 14,
            usePointStyle: true,
            pointStyle: 'circle',
            pointStyleWidth: 12,
            generateLabels: function(chart) {{
              return chart.data.datasets.map((ds, i) => ({{
                text: ds.label,
                fillStyle: ds.borderColor || ds.backgroundColor,
                strokeStyle: ds.borderColor || ds.backgroundColor,
                lineWidth: 3,
                pointStyle: 'circle',
                hidden: !chart.isDatasetVisible(i),
                datasetIndex: i
              }}));
            }}
          }}
        }},
        tooltip: {{ callbacks: {{
          label: function(ctx) {{ return `${{ctx.dataset.label}}: ${{ctx.raw.toLocaleString()}}${{type === 'percent' ? '%' : ''}}`; }},
          footer: function(items) {{ if (type === 'percent') return 'Total: 100%'; return `Total: ${{items.reduce((s,i) => s+i.raw, 0).toLocaleString()}}`; }}
        }} }},
        datalabels: {{
          display: function(ctx) {{
            if (isStacked) return false;
            if (!fewCategories) return false;
            if (type === 'bar') return ctx.dataset.data[ctx.dataIndex] > 0;
            return showDatalabels || type === 'line';
          }},
          color: function(ctx) {{ return ctx.dataset.borderColor || '#333'; }},
          font: {{ size: 10, weight: 'bold' }},
          anchor: 'end',
          align: 'top',
          offset: 2,
          formatter: function(val) {{ return type === 'percent' ? val + '%' : val.toLocaleString(); }},
          clamp: true,
          clip: false
        }}
      }},
      scales: {{
        x: {{ stacked, grid: {{ display: false }}, ticks: {{ font: {{ size: 10 }} }} }},
        y: {{ stacked, beginAtZero: true, max: type === 'percent' ? 100 : undefined,
          ticks: {{ font: {{ size: 10 }}, callback: function(v) {{ return type === 'percent' ? v + '%' : v.toLocaleString(); }} }},
          grid: {{ color: '#f0f0f0' }} }}
      }}
    }}
  }});
}}

// ========== HEATMAP (Queue x Aging) ==========
function renderHeatmap(tickets) {{
  const queueBuckets = {{}};
  tickets.forEach(t => {{
    const q = t.current_queue || 'Unknown';
    if (!queueBuckets[q]) queueBuckets[q] = {{}};
    queueBuckets[q][t.aging_bucket] = (queueBuckets[q][t.aging_bucket] || 0) + 1;
  }});

  const queues = Object.keys(queueBuckets).sort();
  const maxVal = Math.max(...queues.flatMap(q => BUCKET_LABELS.map(b => queueBuckets[q][b] || 0)), 1);

  function heatColor(val) {{
    if (val === 0) return '#f5f7fa';
    const intensity = Math.min(val / maxVal, 1);
    if (intensity < 0.15) return `rgba(13,159,110,${{0.15 + intensity}})`;
    if (intensity < 0.35) return `rgba(202,138,4,${{0.2 + intensity}})`;
    if (intensity < 0.6) return `rgba(234,88,12,${{0.25 + intensity}})`;
    return `rgba(220,38,38,${{0.3 + intensity * 0.6}})`;
  }}

  const cols = BUCKET_LABELS.length + 2;
  let html = `<div class="heatmap-grid" style="grid-template-columns: 140px repeat(${{BUCKET_LABELS.length}}, 1fr) 60px">`;
  // Header
  html += `<div class="heatmap-cell heatmap-header">Queue \\ Age</div>`;
  BUCKET_LABELS.forEach(b => {{ html += `<div class="heatmap-cell heatmap-header">${{b}}</div>`; }});
  html += `<div class="heatmap-cell heatmap-header">Total</div>`;

  queues.forEach(q => {{
    html += `<div class="heatmap-cell" style="text-align:left;font-weight:600;font-size:10px;color:var(--text)">${{q}}</div>`;
    let rowTotal = 0;
    BUCKET_LABELS.forEach(b => {{
      const val = queueBuckets[q][b] || 0;
      rowTotal += val;
      html += `<div class="heatmap-cell" style="background:${{heatColor(val)}};color:#fff"
                onclick="drillQueueBucket('${{q}}','${{b}}')">${{val || ''}}</div>`;
    }});
    html += `<div class="heatmap-cell" style="font-weight:700;color:var(--accent)">${{rowTotal}}</div>`;
  }});

  html += '</div>';

  document.getElementById('heatmapSection').innerHTML = `
    <div class="section-header">
      <h3>&#128293; Queue x Aging Heatmap (click any cell to drill down)</h3>
      <button class="btn btn-sm btn-download" onclick="downloadSection('heatmap')">&#11015; CSV</button>
    </div>${{html}}`;
}}

// ========== CHARTS ==========
function renderCharts(summary, tickets) {{
  Object.values(charts).forEach(c => {{ if (c && c.destroy) c.destroy() }});
  charts = {{}};

  const bucketData = BUCKET_DB_KEYS.map(k => summary[k] || 0);
  const queueCounts = {{}};
  const subStatusCounts = {{}};
  tickets.forEach(t => {{
    queueCounts[t.current_queue] = (queueCounts[t.current_queue] || 0) + 1;
    subStatusCounts[t.sub_status] = (subStatusCounts[t.sub_status] || 0) + 1;
  }});

  document.getElementById('chartsRow1').innerHTML = `
    <div class="chart-card">
      <div class="section-header"><h3>Aging Distribution</h3>
        <button class="btn btn-sm btn-download" onclick="downloadSection('aging')">&#11015;</button></div>
      <div class="chart-container"><canvas id="agingChart"></canvas></div></div>
    <div class="chart-card">
      <div class="section-header"><h3>Queue Split</h3>
        <button class="btn btn-sm btn-download" onclick="downloadSection('queue')">&#11015;</button></div>
      <div class="chart-container"><canvas id="queueChart"></canvas></div></div>
  `;

  const opts = (legend=false) => ({{responsive:true,maintainAspectRatio:false,
    onClick:(e,els)=>{{if(els.length)chartClick(e,els)}},
    plugins:{{legend:{{display:legend,labels:{{color:'#6b7280',font:{{size:10}}}}}}}}}});

  charts.aging = new Chart(document.getElementById('agingChart'), {{
    type:'bar', data:{{labels:BUCKET_LABELS, datasets:[{{data:bucketData, backgroundColor:BUCKET_COLORS, borderRadius:5, borderSkipped:false}}]}},
    options:{{...opts(), scales:{{x:{{ticks:{{color:'#6b7280',font:{{size:10}}}},grid:{{display:false}}}},y:{{ticks:{{color:'#6b7280'}},grid:{{color:'rgba(0,0,0,.08)'}}}}}}}}
  }});

  const qLabels = Object.keys(queueCounts);
  const qValues = Object.values(queueCounts);
  charts.queue = new Chart(document.getElementById('queueChart'), {{
    type:'doughnut', data:{{labels:qLabels, datasets:[{{data:qValues, backgroundColor:['#3b82f6','#f97316','#22c55e','#eab308','#a855f7','#ec4899'], borderWidth:0}}]}},
    options:{{...opts(true), cutout:'55%', plugins:{{legend:{{position:'right',labels:{{color:'#6b7280',padding:10,font:{{size:10}}}}}}}}}}
  }});
}}

function renderZonePartnerCharts(tickets) {{
  const zoneCounts = {{}};
  const partnerCounts = {{}};
  tickets.forEach(t => {{
    const zone = (t.zone || '').split(',')[0].trim();
    if (zone) zoneCounts[zone] = (zoneCounts[zone] || 0) + 1;
    const partner = (t.mapped_partner || '').trim();
    if (partner) partnerCounts[partner] = (partnerCounts[partner] || 0) + 1;
  }});

  const zSorted = Object.entries(zoneCounts).sort((a,b) => b[1]-a[1]).slice(0, 15);
  const pSorted = Object.entries(partnerCounts).sort((a,b) => b[1]-a[1]).slice(0, 15);

  document.getElementById('chartsRow2').innerHTML = `
    <div class="chart-card">
      <div class="section-header"><h3>Top 15 Zones</h3>
        <button class="btn btn-sm btn-download" onclick="downloadSection('zones')">&#11015;</button></div>
      <div class="chart-container" style="height:320px"><canvas id="zoneChart"></canvas></div></div>
    <div class="chart-card">
      <div class="section-header"><h3>Top 15 Partners</h3>
        <button class="btn btn-sm btn-download" onclick="downloadSection('partners')">&#11015;</button></div>
      <div class="chart-container" style="height:320px"><canvas id="partnerChart"></canvas></div></div>
  `;

  const hOpts = {{responsive:true,maintainAspectRatio:false,indexAxis:'y',
    plugins:{{legend:{{display:false}}}},
    scales:{{x:{{ticks:{{color:'#6b7280'}},grid:{{color:'rgba(0,0,0,.08)'}}}},
             y:{{ticks:{{color:'#6b7280',font:{{size:9}}}},grid:{{display:false}}}}}}}};

  if (charts.zone) charts.zone.destroy();
  if (charts.partner) charts.partner.destroy();

  charts.zone = new Chart(document.getElementById('zoneChart'), {{
    type:'bar', data:{{labels:zSorted.map(z=>z[0]), datasets:[{{data:zSorted.map(z=>z[1]), backgroundColor:'#3b82f6', borderRadius:3}}]}},
    options:hOpts
  }});

  charts.partner = new Chart(document.getElementById('partnerChart'), {{
    type:'bar', data:{{labels:pSorted.map(p=>p[0]), datasets:[{{data:pSorted.map(p=>p[1]), backgroundColor:'#f97316', borderRadius:3}}]}},
    options:hOpts
  }});
}}

// ========== TRENDS ==========
async function loadTrends() {{
  const summaries = await api('/api/trends');
  if (summaries.length < 2) {{ document.getElementById('trendSection').style.display = 'none'; return; }}
  if (charts.trend) charts.trend.destroy();

  const dates = summaries.map(s => s.report_date);
  charts.trend = new Chart(document.getElementById('trendChart'), {{
    type:'line',
    data:{{
      labels:dates,
      datasets:[
        {{label:'Total Internet', data:summaries.map(s=>s.total_internet), borderColor:'#3b82f6', backgroundColor:'rgba(59,130,246,.1)', fill:true, tension:.3, pointRadius:3}},
        {{label:'Critical (>48h)', data:summaries.map(s=>s.critical_gt48h), borderColor:'#ef4444', backgroundColor:'rgba(239,68,68,.1)', fill:true, tension:.3, pointRadius:3}},
        {{label:'Created That Day', data:summaries.map(s=>s.created_today), borderColor:'#22c55e', borderDash:[5,3], tension:.3, pointRadius:3}},
        {{label:'Partner Queue', data:summaries.map(s=>s.queue_partner), borderColor:'#f97316', borderDash:[3,3], tension:.3, pointRadius:2}},
      ]
    }},
    options:{{responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{labels:{{color:'#6b7280',font:{{size:10}}}}}}}},
      scales:{{x:{{ticks:{{color:'#6b7280',maxRotation:45}},grid:{{color:'rgba(0,0,0,.06)'}}}},
               y:{{ticks:{{color:'#6b7280'}},grid:{{color:'rgba(0,0,0,.06)'}}}}}}
    }}
  }});
}}

// ========== CRITICAL TABLE ==========
function renderCritical(tickets) {{
  const critical = tickets.filter(t => t.pending_hours > 48).sort((a,b) => b.pending_hours - a.pending_hours).slice(0, 50);
  let rows = critical.map(t => {{
    const h = t.pending_hours;
    const cls = h > 120 ? 'badge-red' : h > 72 ? 'badge-orange' : 'badge-yellow';
    return `<tr>
      <td class="clickable" onclick="showTrail('${{t.ticket_no}}')">${{t.ticket_no}}</td>
      <td>${{t.created_date}} ${{t.created_time || ''}}</td>
      <td><span class="badge ${{cls}}">${{Math.round(h)}}h</span></td>
      <td>${{t.aging_bucket}}</td>
      <td>${{t.current_queue}}</td>
      <td>${{t.mapped_partner || '-'}}</td>
      <td>${{(t.zone || '-').split(',')[0]}}</td>
      <td>${{t.sub_status}}</td>
    </tr>`;
  }}).join('');

  document.getElementById('criticalSection').innerHTML = `
    <div class="section-header">
      <h3>Top 50 Critical Tickets (&gt; 48h) — Click ticket to see trail</h3>
      <button class="btn btn-sm btn-download" onclick="downloadSection('critical')">&#11015; CSV</button>
    </div>
    <div class="table-scroll"><table><thead><tr><th>Ticket No</th><th>Created</th><th>Pending</th>
    <th>Bucket</th><th>Queue</th><th>Partner</th><th>Zone</th><th>Sub Status</th></tr></thead>
    <tbody>${{rows}}</tbody></table></div>`;
}}

// ========== CUSTOM FILTER BUILDER ==========
let filterRowCount = 0;

function addFilterRow() {{
  filterRowCount++;
  const id = filterRowCount;
  const colOpts = FILTERABLE_COLS.map(c => `<option value="${{c}}">${{c.replace(/_/g,' ')}}</option>`).join('');
  const opOpts = OPERATORS.map(o => `<option value="${{o}}">${{o}}</option>`).join('');
  const logic = id > 1 ? `<button class="filter-logic and" onclick="toggleLogic(this)" data-logic="AND">AND</button>` : '';

  const row = document.createElement('div');
  row.className = 'filter-row';
  row.id = `filterRow${{id}}`;
  row.innerHTML = `
    ${{logic}}
    <select id="fCol${{id}}">${{colOpts}}</select>
    <select id="fOp${{id}}">${{opOpts}}</select>
    <input id="fVal${{id}}" placeholder="value..." />
    <button class="btn btn-sm" onclick="removeFilterRow(${{id}})" style="color:var(--red)">&times;</button>
  `;
  document.getElementById('filterRows').appendChild(row);
}}

function removeFilterRow(id) {{
  const row = document.getElementById(`filterRow${{id}}`);
  if (row) row.remove();
}}

function toggleLogic(btn) {{
  const current = btn.getAttribute('data-logic');
  if (current === 'AND') {{
    btn.setAttribute('data-logic', 'OR');
    btn.textContent = 'OR';
    btn.className = 'filter-logic or';
  }} else {{
    btn.setAttribute('data-logic', 'AND');
    btn.textContent = 'AND';
    btn.className = 'filter-logic and';
  }}
}}

function applyFilters() {{
  const rows = document.querySelectorAll('.filter-row');
  const rules = [];

  rows.forEach((row, i) => {{
    const id = row.id.replace('filterRow', '');
    const col = document.getElementById(`fCol${{id}}`)?.value;
    const op = document.getElementById(`fOp${{id}}`)?.value;
    const val = document.getElementById(`fVal${{id}}`)?.value?.trim();
    const logicBtn = row.querySelector('.filter-logic');
    const logic = logicBtn ? logicBtn.getAttribute('data-logic') : 'AND';
    if (col) rules.push({{ col, op, val, logic }});
  }});

  if (rules.length === 0) {{ filteredTickets = [...allTickets]; }}
  else {{
    filteredTickets = allTickets.filter(ticket => {{
      let result = evaluateRule(ticket, rules[0]);
      for (let i = 1; i < rules.length; i++) {{
        const ruleResult = evaluateRule(ticket, rules[i]);
        if (rules[i].logic === 'AND') result = result && ruleResult;
        else result = result || ruleResult;
      }}
      return result;
    }});
  }}

  document.getElementById('filterResults').innerHTML =
    `Showing <strong>${{filteredTickets.length}}</strong> of ${{allTickets.length}} tickets`;

  // Show filtered table
  renderFilteredTable(filteredTickets);
  renderHeatmap(filteredTickets);
  renderZonePartnerCharts(filteredTickets);
  renderCritical(filteredTickets);
}}

function evaluateRule(ticket, rule) {{
  const val = String(ticket[rule.col] || '').toLowerCase();
  const target = (rule.val || '').toLowerCase();
  switch(rule.op) {{
    case 'equals': return val === target;
    case 'not equals': return val !== target;
    case 'contains': return val.includes(target);
    case 'not contains': return !val.includes(target);
    case 'greater than': return parseFloat(val) > parseFloat(target);
    case 'less than': return parseFloat(val) < parseFloat(target);
    case 'is empty': return val === '' || val === 'null' || val === 'undefined';
    case 'is not empty': return val !== '' && val !== 'null' && val !== 'undefined';
    default: return true;
  }}
}}

function resetFilters() {{
  filteredTickets = [...allTickets];
  document.getElementById('filterRows').innerHTML = '';
  filterRowCount = 0;
  addFilterRow();
  document.getElementById('filterResults').innerHTML = '';
  document.getElementById('filteredTableSection').style.display = 'none';
}}

function renderFilteredTable(tickets) {{
  if (tickets.length === 0) {{
    document.getElementById('filteredTableSection').style.display = 'none';
    return;
  }}
  document.getElementById('filteredTableSection').style.display = 'block';
  document.getElementById('filteredTableTitle').textContent = `Filtered Tickets (${{tickets.length}})`;

  const cols = ['ticket_no','created_date','created_time','pending_hours','aging_bucket','current_queue',
    'sub_status','status','zone','mapped_partner','city','customer_name','device_id','channel_partner',
    'disposition_l1','disposition_l2','disposition_l3','pending_days'];

  let thead = cols.map(c => `<th onclick="sortFilteredTable('${{c}}')">${{c.replace(/_/g,' ')}}</th>`).join('');
  let tbody = tickets.slice(0, 200).map(t => {{
    const cells = cols.map(c => {{
      let v = t[c] || '';
      if (c === 'ticket_no') return `<td class="clickable" onclick="showTrail('${{v}}')">${{v}}</td>`;
      if (c === 'pending_hours') return `<td class="num">${{typeof v === 'number' ? Math.round(v) : v}}</td>`;
      return `<td>${{v}}</td>`;
    }}).join('');
    return `<tr>${{cells}}</tr>`;
  }}).join('');

  const note = tickets.length > 200 ? `<p style="color:var(--text2);font-size:11px;margin-top:8px">Showing 200 of ${{tickets.length}} — download CSV for full data</p>` : '';

  document.getElementById('filteredTableContent').innerHTML =
    `<table><thead><tr>${{thead}}</tr></thead><tbody>${{tbody}}</tbody></table>${{note}}`;
}}

let sortCol = null, sortAsc = true;
function sortFilteredTable(col) {{
  if (sortCol === col) sortAsc = !sortAsc; else {{ sortCol = col; sortAsc = true; }}
  filteredTickets.sort((a, b) => {{
    let va = a[col], vb = b[col];
    if (typeof va === 'number' && typeof vb === 'number') return sortAsc ? va - vb : vb - va;
    va = String(va || ''); vb = String(vb || '');
    return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
  }});
  renderFilteredTable(filteredTickets);
}}

// ========== DRILL DOWNS ==========
function drillBucket(bucket) {{
  drillData = allTickets.filter(t => t.aging_bucket === bucket);
  showDrillModal(`Tickets in "${{bucket}}" bucket (${{drillData.length}})`, drillData);
}}

function drillQueueBucket(queue, bucket) {{
  drillData = allTickets.filter(t => t.current_queue === queue && t.aging_bucket === bucket);
  showDrillModal(`${{queue}} / ${{bucket}} (${{drillData.length}} tickets)`, drillData);
}}

function showDrillModal(title, data) {{
  document.getElementById('drillModal').classList.add('show');
  document.getElementById('drillTitle').textContent = title;

  if (data.length === 0) {{
    document.getElementById('drillContent').innerHTML = '<p>No tickets.</p>';
    return;
  }}

  const cols = ['ticket_no','created_date','pending_hours','aging_bucket','current_queue','sub_status','mapped_partner','zone','customer_name','device_id'];
  let thead = cols.map(c => `<th>${{c.replace(/_/g,' ')}}</th>`).join('');
  let tbody = data.slice(0, 100).map(t => {{
    const cells = cols.map(c => {{
      let v = t[c] || '';
      if (c === 'ticket_no') return `<td class="clickable" onclick="showTrail('${{v}}')">${{v}}</td>`;
      if (c === 'pending_hours') return `<td class="num">${{typeof v === 'number' ? Math.round(v) + 'h' : v}}</td>`;
      return `<td>${{v}}</td>`;
    }}).join('');
    return `<tr>${{cells}}</tr>`;
  }}).join('');

  const note = data.length > 100 ? `<p style="color:var(--text2);font-size:11px;margin-top:8px">Showing 100 of ${{data.length}} — download for full data</p>` : '';
  document.getElementById('drillContent').innerHTML =
    `<table><thead><tr>${{thead}}</tr></thead><tbody>${{tbody}}</tbody></table>${{note}}`;
}}

function closeDrill() {{ document.getElementById('drillModal').classList.remove('show'); }}

// ========== TICKET TRAIL ==========
async function showTrail(ticketNo) {{
  document.getElementById('trailModal').classList.add('show');
  document.getElementById('trailTitle').textContent = `Ticket Trail: ${{ticketNo}}`;
  document.getElementById('trailContent').innerHTML = '<div class="loading">Loading...</div>';

  const trail = await api(`/api/ticket-trail?ticket_no=${{ticketNo}}`);
  if (trail.length === 0) {{
    document.getElementById('trailContent').innerHTML = '<p>No history found.</p>';
    return;
  }}

  // Also show ticket details from current data
  const current = allTickets.find(t => t.ticket_no === ticketNo);
  let details = '';
  if (current) {{
    details = `<div style="margin-bottom:12px;padding:10px;background:var(--card2);border-radius:8px;font-size:11px">
      <strong>Customer:</strong> ${{current.customer_name || '-'}} |
      <strong>Partner:</strong> ${{current.mapped_partner || '-'}} |
      <strong>Zone:</strong> ${{current.zone || '-'}} |
      <strong>City:</strong> ${{current.city || '-'}} |
      <strong>Device:</strong> ${{current.device_id || '-'}} |
      <strong>Channel:</strong> ${{current.channel_partner || '-'}}
    </div>`;
  }}

  let rows = trail.map(t => `<tr>
    <td>${{t.report_date}}</td>
    <td><span class="badge ${{t.pending_hours > 120 ? 'badge-red' : t.pending_hours > 48 ? 'badge-orange' : 'badge-green'}}">${{Math.round(t.pending_hours)}}h</span></td>
    <td>${{t.aging_bucket}}</td>
    <td>${{t.current_queue}}</td>
    <td>${{t.sub_status}}</td>
    <td>${{t.status}}</td>
  </tr>`).join('');

  document.getElementById('trailContent').innerHTML = `${{details}}
    <p style="color:var(--text2);margin-bottom:8px;font-size:11px">Across ${{trail.length}} report(s):</p>
    <table><thead><tr><th>Date</th><th>Pending</th><th>Bucket</th><th>Queue</th><th>Sub Status</th><th>Status</th></tr></thead>
    <tbody>${{rows}}</tbody></table>`;
}}

function closeTrail() {{ document.getElementById('trailModal').classList.remove('show'); }}

// ========== DOWNLOADS ==========
function openRulesModal() {{
  document.getElementById('rulesModal').style.display = 'flex';
}}
function closeRulesModal() {{
  document.getElementById('rulesModal').style.display = 'none';
}}
// Close on clicking outside the modal content
document.getElementById('rulesModal').addEventListener('click', function(e) {{
  if (e.target === this) closeRulesModal();
}});

function downloadAll() {{
  window.open(`/api/download?date=${{currentDate}}&section=all`);
}}

function downloadSection(section) {{
  window.open(`/api/download?date=${{currentDate}}&section=${{section}}`);
}}

function downloadBucket(bucket) {{
  window.open(`/api/download-filtered?date=${{currentDate}}&bucket=${{encodeURIComponent(bucket)}}`);
}}

function downloadFiltered() {{
  // Create CSV from filteredTickets in browser
  if (!filteredTickets.length) {{ alert('No filtered data to download'); return; }}
  const cols = Object.keys(filteredTickets[0]);
  let csv = cols.join(',') + '\\n';
  filteredTickets.forEach(t => {{
    csv += cols.map(c => {{
      let v = String(t[c] || '').replace(/"/g, '""');
      return `"${{v}}"`;
    }}).join(',') + '\\n';
  }});
  const blob = new Blob([csv], {{type:'text/csv'}});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `filtered_tickets_${{currentDate}}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}}

function downloadDrill() {{
  if (!drillData.length) return;
  const cols = Object.keys(drillData[0]);
  let csv = cols.join(',') + '\\n';
  drillData.forEach(t => {{
    csv += cols.map(c => {{ let v = String(t[c] || '').replace(/"/g, '""'); return `"${{v}}"`; }}).join(',') + '\\n';
  }});
  const blob = new Blob([csv], {{type:'text/csv'}});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = `drill_down_${{currentDate}}.csv`;
  a.click(); URL.revokeObjectURL(url);
}}

function chartClick(event, elements) {{
  // Placeholder for chart click drill-down
  if (elements.length > 0) {{
    const idx = elements[0].index;
    drillBucket(BUCKET_LABELS[idx]);
  }}
}}

function formatDate(d) {{
  const dt = new Date(d + 'T00:00:00');
  return dt.toLocaleDateString('en-IN', {{day:'numeric',month:'short',year:'numeric',weekday:'short'}});
}}

// ========== DASHBOARD CUSTOMIZATION ==========
const LAYOUT_KEY = 'pft_dashboard_layout';
const DEFAULT_ORDER = ['summaryCards','categorySection','categorySummary','masterComparison','trendChart'];

function getLayout() {{
  try {{
    const stored = localStorage.getItem(LAYOUT_KEY);
    if (stored) {{
      const layout = JSON.parse(stored);
      if (layout.order && Array.isArray(layout.order) && layout.hidden && Array.isArray(layout.hidden)) {{
        // Ensure all section IDs are present
        const allIds = DEFAULT_ORDER.slice();
        layout.order.forEach(id => {{ if (!allIds.includes(id)) allIds.push(id); }});
        allIds.forEach(id => {{ if (!layout.order.includes(id) && !layout.hidden.includes(id)) layout.order.push(id); }});
        return layout;
      }}
    }}
  }} catch(e) {{}}
  return {{ order: DEFAULT_ORDER.slice(), hidden: [] }};
}}

function saveLayout() {{
  const container = document.getElementById('sectionContainer');
  if (!container) return;
  const sections = container.querySelectorAll('.dashboard-section');
  const order = Array.from(sections).map(s => s.dataset.sectionId);
  const hidden = [];
  document.querySelectorAll('.dashboard-section[data-hidden="true"]').forEach(s => {{
    if (!order.includes(s.dataset.sectionId)) order.push(s.dataset.sectionId);
    hidden.push(s.dataset.sectionId);
  }});
  localStorage.setItem(LAYOUT_KEY, JSON.stringify({{ order, hidden }}));
  updateHiddenDrawer();
}}

function applySavedLayout() {{
  const layout = getLayout();
  const container = document.getElementById('sectionContainer');
  if (!container) return;

  const sectionMap = {{}};
  container.querySelectorAll('.dashboard-section').forEach(s => {{
    sectionMap[s.dataset.sectionId] = s;
  }});

  // Reorder
  layout.order.forEach(id => {{
    const el = sectionMap[id];
    if (el) container.appendChild(el);
  }});

  // Hide
  layout.hidden.forEach(id => {{
    const el = sectionMap[id];
    if (el) {{
      el.style.display = 'none';
      el.dataset.hidden = 'true';
    }}
  }});

  updateHiddenDrawer();
}}

function moveSectionUp(section) {{
  const prev = section.previousElementSibling;
  if (prev && prev.classList.contains('dashboard-section')) {{
    section.parentNode.insertBefore(section, prev);
    saveLayout();
  }}
}}

function moveSectionDown(section) {{
  const next = section.nextElementSibling;
  if (next && next.classList.contains('dashboard-section')) {{
    section.parentNode.insertBefore(next, section);
    saveLayout();
  }}
}}

function hideSection(section) {{
  section.style.display = 'none';
  section.dataset.hidden = 'true';
  saveLayout();
}}

function restoreSection(sectionId) {{
  const section = document.querySelector(`.dashboard-section[data-section-id="${{sectionId}}"]`);
  if (section) {{
    section.style.display = '';
    section.dataset.hidden = 'false';
    saveLayout();
  }}
}}

function updateHiddenDrawer() {{
  const hiddenSections = document.querySelectorAll('.dashboard-section[data-hidden="true"]');
  const count = hiddenSections.length;
  document.getElementById('hiddenCount').textContent = count;

  const panel = document.getElementById('hiddenDrawerPanel');
  if (count === 0) {{
    panel.innerHTML = '<div style="padding:4px 0;color:var(--text2);font-size:12px">No removed templates</div>';
    if (panel.classList.contains('open')) panel.classList.remove('open');
  }} else {{
    panel.innerHTML = Array.from(hiddenSections).map(s => {{
      return `<div class="hidden-drawer-item">
        <span>${{s.dataset.sectionLabel || s.dataset.sectionId}}</span>
        <button onclick="restoreSection('${{s.dataset.sectionId}}')">+ Restore</button>
      </div>`;
    }}).join('');
  }}
}}

function toggleHiddenDrawer() {{
  const panel = document.getElementById('hiddenDrawerPanel');
  panel.classList.toggle('open');
}}

// ---- Drag & Drop ----
let draggedSection = null;

function initDragDrop() {{
  document.querySelectorAll('.dashboard-section').forEach(section => {{
    section.addEventListener('dragstart', function(e) {{
      // Only allow drag from section-header area
      const header = section.querySelector('.section-header') || section.querySelector('.cards');
      if (!header) {{ e.preventDefault(); return; }}
      draggedSection = section;
      section.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', section.dataset.sectionId);
    }});

    section.addEventListener('dragend', function() {{
      section.classList.remove('dragging');
      document.querySelectorAll('.dashboard-section.drag-over').forEach(s => s.classList.remove('drag-over'));
      draggedSection = null;
      saveLayout();
    }});

    section.addEventListener('dragover', function(e) {{
      e.preventDefault();
      e.dataTransfer.dropEffect = 'move';
      if (draggedSection && draggedSection !== section) {{
        section.classList.add('drag-over');
      }}
    }});

    section.addEventListener('dragleave', function() {{
      section.classList.remove('drag-over');
    }});

    section.addEventListener('drop', function(e) {{
      e.preventDefault();
      section.classList.remove('drag-over');
      if (draggedSection && draggedSection !== section) {{
        const container = section.parentNode;
        const allSections = Array.from(container.querySelectorAll('.dashboard-section'));
        const dragIdx = allSections.indexOf(draggedSection);
        const dropIdx = allSections.indexOf(section);
        if (dragIdx < dropIdx) {{
          container.insertBefore(draggedSection, section.nextSibling);
        }} else {{
          container.insertBefore(draggedSection, section);
        }}
      }}
    }});
  }});
}}

// Wrap sections in a container on DOMContentLoaded
(function() {{
  const sections = document.querySelectorAll('.dashboard-section');
  if (sections.length === 0) return;
  const parent = sections[0].parentNode;
  const container = document.createElement('div');
  container.id = 'sectionContainer';
  // Insert container before first dashboard-section
  parent.insertBefore(container, sections[0]);
  sections.forEach(s => container.appendChild(s));
  applySavedLayout();
  initDragDrop();
}})();

init();
</script>

</body>
</html>"""


def generate_agent_html():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PFT Agent Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {
  --accent: #7c3aed; --accent2: #6d28d9; --bg: #f8fafc; --card: #ffffff;
  --border: #e2e8f0; --text: #1e293b; --text2: #64748b; --text3: #94a3b8;
  --green: #16a34a; --green-bg: #dcfce7; --red: #dc2626; --red-bg: #fee2e2;
  --blue: #2563eb; --blue-bg: #dbeafe; --orange: #ea580c; --orange-bg: #ffedd5;
  --yellow: #ca8a04; --yellow-bg: #fef9c3;
}
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:'Inter',sans-serif; background:var(--bg); color:var(--text); font-size:13px; }

/* Top bar */
.topbar { background:var(--card); border-bottom:2px solid var(--accent); padding:10px 24px; display:flex; align-items:center; gap:16px; position:sticky; top:0; z-index:200; box-shadow:0 1px 3px rgba(0,0,0,0.06); }
.topbar h1 { font-size:16px; font-weight:700; color:var(--accent); }
.topbar a { color:var(--accent); text-decoration:none; font-size:12px; font-weight:500; }
.topbar a:hover { text-decoration:underline; }
.topbar-right { margin-left:auto; display:flex; align-items:center; gap:12px; }

/* Controls bar */
.controls { background:var(--card); border-bottom:1px solid var(--border); padding:12px 24px; display:flex; align-items:center; gap:16px; flex-wrap:wrap; position:sticky; top:42px; z-index:190; }
.controls label { font-size:11px; font-weight:600; text-transform:uppercase; color:var(--text2); letter-spacing:0.5px; }
.controls select, .controls input[type="date"] {
  padding:6px 10px; border:1px solid var(--border); border-radius:6px; font-size:13px;
  font-family:inherit; background:var(--bg); color:var(--text); cursor:pointer;
}
.controls select:focus, .controls input[type="date"]:focus { outline:none; border-color:var(--accent); box-shadow:0 0 0 2px rgba(124,58,237,0.15); }

/* Buttons */
.btn { padding:7px 14px; border-radius:6px; font-size:12px; font-weight:600; font-family:inherit; border:1px solid var(--border); background:var(--card); color:var(--text); cursor:pointer; transition:all 0.15s; display:inline-flex; align-items:center; gap:5px; }
.btn:hover { background:var(--bg); }
.btn-accent { background:var(--accent); color:#fff; border-color:var(--accent); }
.btn-accent:hover { background:var(--accent2); }
.btn-green { background:var(--green); color:#fff; border-color:var(--green); }
.btn-green:hover { background:#15803d; }
.btn-red { background:var(--red); color:#fff; border-color:var(--red); }
.btn-red:hover { background:#b91c1c; }
.btn-sm { padding:4px 8px; font-size:11px; }
.btn:disabled { opacity:0.5; cursor:not-allowed; }

/* Container */
.container { max-width:1800px; margin:0 auto; padding:16px 24px; }

/* Attendance panel */
.attendance-panel { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:16px 20px; margin-bottom:16px; }
.attendance-panel h3 { font-size:13px; font-weight:600; color:var(--text2); text-transform:uppercase; letter-spacing:0.5px; margin-bottom:12px; }
.agent-chips { display:flex; flex-wrap:wrap; gap:8px; }
.agent-chip { padding:6px 14px; border-radius:20px; font-size:12px; font-weight:600; cursor:pointer; border:2px solid var(--border); background:var(--card); color:var(--text2); transition:all 0.15s; user-select:none; }
.agent-chip.present { background:var(--green-bg); border-color:var(--green); color:var(--green); }
.agent-chip:hover { transform:scale(1.03); }

/* Summary cards */
.summary-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(150px, 1fr)); gap:12px; margin-bottom:16px; }
.agent-card { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:14px 16px; cursor:pointer; transition:all 0.15s; position:relative; }
.agent-card:hover { border-color:var(--accent); box-shadow:0 2px 8px rgba(124,58,237,0.1); }
.agent-card.active { border-color:var(--accent); background:linear-gradient(135deg, rgba(124,58,237,0.04), rgba(124,58,237,0.08)); box-shadow:0 0 0 2px rgba(124,58,237,0.2); }
.agent-card .name { font-size:13px; font-weight:700; margin-bottom:4px; }
.agent-card .count { font-size:28px; font-weight:700; color:var(--accent); }
.agent-card .label { font-size:10px; color:var(--text3); text-transform:uppercase; letter-spacing:0.5px; }
.agent-card.total-card { background:linear-gradient(135deg, var(--accent), var(--accent2)); color:#fff; }
.agent-card.total-card .count { color:#fff; }
.agent-card.total-card .label { color:rgba(255,255,255,0.7); }
.agent-card.total-card .name { color:rgba(255,255,255,0.9); }

/* Ticket table */
.table-wrap { background:var(--card); border:1px solid var(--border); border-radius:10px; overflow:hidden; }
.table-header { padding:12px 16px; display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid var(--border); flex-wrap:wrap; gap:8px; }
.table-header h3 { font-size:14px; font-weight:700; }
.table-header .badge { background:var(--accent); color:#fff; padding:2px 10px; border-radius:12px; font-size:11px; font-weight:700; }
.table-scroll { overflow-x:auto; max-height:calc(100vh - 340px); overflow-y:auto; }
table { width:100%; border-collapse:collapse; font-size:12px; }
thead { position:sticky; top:0; z-index:10; }
th { background:#f1f5f9; padding:8px 10px; text-align:left; font-size:10px; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; color:var(--text2); border-bottom:2px solid var(--border); white-space:nowrap; }
td { padding:7px 10px; border-bottom:1px solid #f1f5f9; vertical-align:top; }
tr:hover td { background:rgba(124,58,237,0.02); }
tr.agent-separator td { background:var(--accent); color:#fff; font-weight:700; font-size:12px; padding:6px 10px; }

/* Editable cells */
td.editable { position:relative; cursor:pointer; min-width:100px; }
td.editable:hover { background:rgba(124,58,237,0.06); }
td.editable .cell-val { min-height:18px; }
td.editable .cell-val:empty::after { content:'—'; color:var(--text3); }
td.editable textarea { width:100%; min-height:50px; padding:4px 6px; border:1.5px solid var(--accent); border-radius:4px; font-family:inherit; font-size:12px; resize:vertical; background:#fff; }
td.editable select.cell-select { width:100%; padding:3px 6px; border:1.5px solid var(--accent); border-radius:4px; font-family:inherit; font-size:12px; }

/* Status badges */
.aging-badge { padding:2px 8px; border-radius:4px; font-size:10px; font-weight:700; white-space:nowrap; }
.aging-critical { background:var(--red-bg); color:var(--red); }
.aging-warn { background:var(--orange-bg); color:var(--orange); }
.aging-ok { background:var(--green-bg); color:var(--green); }
.aging-mid { background:var(--yellow-bg); color:var(--yellow); }

/* Temp badge */
.temp-badge { background:#ede9fe; color:#7c3aed; padding:1px 6px; border-radius:3px; font-size:9px; font-weight:700; margin-left:4px; }
.own-badge { background:var(--blue-bg); color:var(--blue); padding:1px 6px; border-radius:3px; font-size:9px; font-weight:700; margin-left:4px; }

/* Work status */
.ws-pending { color:var(--orange); font-weight:600; }
.ws-completed { color:var(--green); font-weight:600; }
.ws-select { padding:2px 6px; border:1px solid var(--border); border-radius:4px; font-size:11px; font-family:inherit; cursor:pointer; }

/* Card sub-stats */
.card-sub-stats { display:flex; gap:8px; margin-top:6px; font-size:10px; }
.card-sub-stats .stat { padding:1px 6px; border-radius:3px; }
.stat-own { background:var(--blue-bg); color:var(--blue); }
.stat-temp { background:#ede9fe; color:#7c3aed; }
.stat-done { background:var(--green-bg); color:var(--green); }

/* Row highlighting for temp tickets */
tr.temp-row td { background:rgba(124,58,237,0.03); }
tr.temp-row td:first-child { border-left:3px solid var(--accent); }

/* Toast notification */
.toast { position:fixed; bottom:20px; right:20px; background:#1e293b; color:#fff; padding:10px 20px; border-radius:8px; font-size:13px; font-weight:500; z-index:9999; opacity:0; transform:translateY(10px); transition:all 0.3s; pointer-events:none; }
.toast.show { opacity:1; transform:translateY(0); }
.toast.success { background:var(--green); }
.toast.error { background:var(--red); }

/* Loading spinner */
.spinner { display:inline-block; width:16px; height:16px; border:2px solid rgba(124,58,237,0.2); border-top-color:var(--accent); border-radius:50%; animation:spin 0.6s linear infinite; }
@keyframes spin { to { transform:rotate(360deg); } }

/* Empty state */
.empty-state { text-align:center; padding:60px 20px; color:var(--text2); }
.empty-state h2 { font-size:20px; margin-bottom:8px; color:var(--text); }
.empty-state p { font-size:13px; line-height:1.6; }

/* Responsive */
@media(max-width:768px) {
  .controls { flex-direction:column; align-items:stretch; }
  .summary-grid { grid-template-columns:repeat(auto-fill, minmax(120px, 1fr)); }
  .table-scroll { max-height:60vh; }
}

/* Print */
@media print {
  .topbar, .controls, .attendance-panel, .btn { display:none !important; }
  .table-scroll { max-height:none; overflow:visible; }
}
</style>
</head>
<body>

<!-- Top bar -->
<div class="topbar">
  <h1>PFT Agent Dashboard</h1>
  <a href="/">&#8592; Main Dashboard</a>
  <div class="topbar-right">
    <span id="statusText" style="font-size:11px;color:var(--text3)"></span>
  </div>
</div>

<!-- Controls bar -->
<div class="controls">
  <div>
    <label>Report Date</label><br>
    <select id="dateSelect" onchange="loadDate()"></select>
  </div>
  <div>
    <label>Filter Agent</label><br>
    <select id="agentFilter" onchange="filterTable()">
      <option value="">All Agents</option>
    </select>
  </div>
  <div style="margin-left:auto;display:flex;gap:8px;align-items:end">
    <button class="btn btn-accent" onclick="doAssign()" id="assignBtn">Assign Tickets</button>
    <button class="btn btn-red btn-sm" onclick="doReassign()" id="reassignBtn" title="Re-distribute tickets among present agents">Reassign</button>
    <button class="btn" onclick="downloadCSV()">Download CSV</button>
    <button class="btn btn-sm" onclick="window.print()" title="Print">Print</button>
  </div>
</div>

<div class="container">

  <!-- Attendance Panel -->
  <div class="attendance-panel" id="attendancePanel">
    <h3>Agent Attendance &mdash; <span id="presentCount">0</span>/<span id="totalCount">0</span> Present</h3>
    <div class="agent-chips" id="agentChips"></div>
    <div style="margin-top:10px;display:flex;gap:8px">
      <button class="btn btn-green btn-sm" onclick="saveAttendance()">Save Attendance</button>
      <button class="btn btn-sm" onclick="selectAllAgents()">Select All</button>
      <button class="btn btn-sm" onclick="deselectAllAgents()">Deselect All</button>
    </div>
  </div>

  <!-- Summary Cards -->
  <div class="summary-grid" id="summaryGrid"></div>

  <!-- Ticket Table -->
  <div class="table-wrap" id="tableWrap" style="display:none">
    <div class="table-header">
      <h3>Assigned Tickets <span class="badge" id="ticketCount">0</span></h3>
      <div id="tableActions" style="display:flex;gap:6px"></div>
    </div>
    <div class="table-scroll">
      <table>
        <thead>
          <tr>
            <th style="width:30px">#</th>
            <th>Ticket No</th>
            <th>Type</th>
            <th>Work Status</th>
            <th>Creation Date</th>
            <th>Phone</th>
            <th>Disposition L4</th>
            <th>Customer Name</th>
            <th>Partner</th>
            <th>Queue</th>
            <th>Reopen</th>
            <th>Kapture Status</th>
            <th>Aging</th>
            <th>Ground Team Update</th>
            <th>Assigned Date</th>
            <th>Worked By</th>
            <th>Ping</th>
            <th>Cx Action</th>
            <th>Px Call Status</th>
            <th>Update Date</th>
            <th>Agent Remark</th>
            <th>Partner Concern</th>
          </tr>
        </thead>
        <tbody id="ticketBody"></tbody>
      </table>
    </div>
  </div>

  <!-- Empty state -->
  <div class="empty-state" id="emptyState">
    <h2>No Assignments Yet</h2>
    <p>Select a date, mark agents as present, then click <b>Assign Tickets</b> to distribute tickets via round-robin.</p>
  </div>
</div>

<!-- Toast -->
<div class="toast" id="toast"></div>

<script>
// ---- State ----
let currentDate = '';
let agents = [];
let attendance = {};
let assignments = [];
let agentSummary = {};

// ---- Helpers ----
function $(id) { return document.getElementById(id); }
function toast(msg, type='') {
  const t = $('toast');
  t.textContent = msg;
  t.className = 'toast show' + (type ? ' ' + type : '');
  setTimeout(() => t.className = 'toast', 2500);
}

async function api(path, opts) {
  try {
    const r = await fetch(path, opts);
    return await r.json();
  } catch(e) { toast('API error: ' + e.message, 'error'); return null; }
}

function agingBadge(hours, bucket) {
  if (!bucket) bucket = '';
  let cls = 'aging-ok';
  if (hours > 120) cls = 'aging-critical';
  else if (hours > 48) cls = 'aging-critical';
  else if (hours > 24) cls = 'aging-warn';
  else if (hours > 12) cls = 'aging-mid';
  const display = hours != null ? Math.round(hours) + 'h' : bucket;
  return '<span class="aging-badge ' + cls + '">' + display + '</span>';
}

function escHtml(s) {
  if (s == null) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ---- Init ----
async function init() {
  // Load agents
  const agentData = await api('/api/agent/list');
  if (agentData) agents = agentData.agents || [];

  // Populate agent filter
  const af = $('agentFilter');
  agents.forEach(a => {
    const o = document.createElement('option');
    o.value = a; o.textContent = a;
    af.appendChild(o);
  });

  // Load dates
  const dates = await api('/api/dates');
  const agentDates = await api('/api/agent/dates');
  const allDates = [...new Set([...(dates||[]), ...(agentDates||[])])].sort().reverse();

  const ds = $('dateSelect');
  if (allDates.length === 0) {
    const o = document.createElement('option');
    o.value = ''; o.textContent = 'No dates available';
    ds.appendChild(o);
    return;
  }
  allDates.forEach(d => {
    const o = document.createElement('option');
    o.value = d; o.textContent = d;
    ds.appendChild(o);
  });

  currentDate = allDates[0];
  loadDate();
}

async function loadDate() {
  currentDate = $('dateSelect').value;
  if (!currentDate) return;
  $('statusText').innerHTML = '<span class="spinner"></span> Loading...';

  // Load attendance
  const att = await api('/api/agent/attendance?date=' + currentDate);
  attendance = att || {};
  renderAttendance();

  // Load active tickets (own + temp-redistributed)
  const rows = await api('/api/agent/active-tickets?date=' + currentDate);
  assignments = rows || [];

  // Load summary
  agentSummary = await api('/api/agent/summary?date=' + currentDate) || {};

  renderSummary();
  renderTable();
  $('statusText').textContent = 'Loaded ' + currentDate;
}

// ---- Attendance ----
function renderAttendance() {
  const container = $('agentChips');
  container.innerHTML = '';
  let presentCount = 0;

  agents.forEach(a => {
    const chip = document.createElement('div');
    chip.className = 'agent-chip' + (attendance[a] !== false ? ' present' : '');
    chip.textContent = a;
    chip.onclick = () => {
      attendance[a] = !attendance[a];
      chip.classList.toggle('present');
      updatePresentCount();
    };
    container.appendChild(chip);
    if (attendance[a] !== false) presentCount++;
  });

  $('totalCount').textContent = agents.length;
  $('presentCount').textContent = presentCount;
}

function updatePresentCount() {
  const present = agents.filter(a => attendance[a] !== false).length;
  $('presentCount').textContent = present;
}

function selectAllAgents() {
  agents.forEach(a => attendance[a] = true);
  renderAttendance();
}
function deselectAllAgents() {
  agents.forEach(a => attendance[a] = false);
  renderAttendance();
}

async function saveAttendance() {
  const present = agents.filter(a => attendance[a] !== false);
  const result = await api('/api/agent/save-attendance', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ date: currentDate, present })
  });
  if (result) toast('Attendance saved (' + present.length + ' present)', 'success');
}

// ---- Assignment ----
async function doAssign() {
  const present = agents.filter(a => attendance[a] !== false);
  if (present.length === 0) { toast('Mark at least one agent as present', 'error'); return; }

  $('assignBtn').disabled = true;
  $('assignBtn').innerHTML = '<span class="spinner"></span> Assigning...';

  // Save attendance first
  await api('/api/agent/save-attendance', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ date: currentDate, present })
  });

  const result = await api('/api/agent/assign', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ date: currentDate, present })
  });

  $('assignBtn').disabled = false;
  $('assignBtn').textContent = 'Assign Tickets';

  if (result && result.status === 'assigned') {
    let msg = result.total + ' new tickets assigned to ' + result.agents + ' agents';
    if (result.redistributed > 0) msg += ' + ' + result.redistributed + ' pending tickets redistributed from absent agents';
    toast(msg, 'success');
    loadDate();
  } else if (result && result.status === 'already_assigned') {
    toast('Already assigned (' + result.count + ' tickets). Use Reassign to redistribute.', 'error');
  } else {
    toast(result?.message || 'Assignment failed', 'error');
  }
}

async function doReassign() {
  const present = agents.filter(a => attendance[a] !== false);
  if (present.length === 0) { toast('Mark at least one agent as present', 'error'); return; }
  if (!confirm('This will:\\n1. Reclaim pending tickets for returning agents\\n2. Re-distribute todays tickets among present agents\\n3. Temp-redistribute absent agents pending tickets\\n\\nContinue?')) return;

  $('reassignBtn').disabled = true;
  const result = await api('/api/agent/reassign', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ date: currentDate, present })
  });
  $('reassignBtn').disabled = false;

  if (result && result.status === 'assigned') {
    let msg = result.total + ' tickets reassigned to ' + result.agents + ' agents';
    if (result.reclaimed > 0) msg += ' | ' + result.reclaimed + ' tickets reclaimed by returning agents';
    if (result.redistributed > 0) msg += ' | ' + result.redistributed + ' redistributed from absent';
    toast(msg, 'success');
    loadDate();
  } else {
    toast(result?.message || 'Reassignment failed', 'error');
  }
}

// ---- Summary ----
function renderSummary() {
  const grid = $('summaryGrid');
  grid.innerHTML = '';

  const total = assignments.length;
  const totalOwn = assignments.filter(t => t.ticket_type !== 'temp').length;
  const totalTemp = assignments.filter(t => t.ticket_type === 'temp').length;

  // Total card
  const totalCard = document.createElement('div');
  totalCard.className = 'agent-card total-card';
  totalCard.innerHTML = '<div class="name">All Agents</div><div class="count">' + total + '</div><div class="label">Total Active Tickets</div>' +
    '<div class="card-sub-stats"><span class="stat stat-own">' + totalOwn + ' own</span><span class="stat stat-temp">' + totalTemp + ' temp</span></div>';
  totalCard.onclick = () => { $('agentFilter').value = ''; filterTable(); highlightCard(''); };
  grid.appendChild(totalCard);

  // Per-agent cards
  agents.forEach(a => {
    const s = agentSummary[a] || {};
    const own = s.own_today || 0;
    const temp = s.temp_holding || 0;
    const totalAgent = own + temp;
    const completed = s.completed_all || 0;
    if (totalAgent === 0 && completed === 0 && !Object.keys(agentSummary).length) return;
    const card = document.createElement('div');
    card.className = 'agent-card';
    card.id = 'card-' + a;
    const isAbsent = attendance[a] === false;
    card.innerHTML = '<div class="name">' + escHtml(a) + (isAbsent ? ' <span style="color:var(--red);font-size:10px">(absent)</span>' : '') + '</div>' +
      '<div class="count">' + totalAgent + '</div><div class="label">active tickets</div>' +
      '<div class="card-sub-stats">' +
        '<span class="stat stat-own">' + own + ' own</span>' +
        (temp > 0 ? '<span class="stat stat-temp">' + temp + ' temp</span>' : '') +
        (completed > 0 ? '<span class="stat stat-done">' + completed + ' done</span>' : '') +
      '</div>';
    card.onclick = () => { $('agentFilter').value = a; filterTable(); highlightCard(a); };
    grid.appendChild(card);
  });
}

function highlightCard(agent) {
  document.querySelectorAll('.agent-card').forEach(c => c.classList.remove('active'));
  if (agent) {
    const card = document.getElementById('card-' + agent);
    if (card) card.classList.add('active');
  } else {
    document.querySelector('.agent-card.total-card')?.classList.add('active');
  }
}

// ---- Table ----
function renderTable() {
  const tbody = $('ticketBody');
  tbody.innerHTML = '';

  if (assignments.length === 0) {
    $('tableWrap').style.display = 'none';
    $('emptyState').style.display = 'block';
    return;
  }

  $('tableWrap').style.display = 'block';
  $('emptyState').style.display = 'none';
  $('ticketCount').textContent = assignments.length;

  // Group by agent
  const grouped = {};
  assignments.forEach(t => {
    if (!grouped[t.agent_name]) grouped[t.agent_name] = [];
    grouped[t.agent_name].push(t);
  });

  let idx = 0;
  const sortedAgents = Object.keys(grouped).sort();
  sortedAgents.forEach(agent => {
    // Agent separator row
    const sepTr = document.createElement('tr');
    sepTr.className = 'agent-separator';
    sepTr.dataset.agent = agent;
    sepTr.innerHTML = '<td colspan="22">' + escHtml(agent) + ' (' + grouped[agent].length + ' tickets)</td>';
    tbody.appendChild(sepTr);

    grouped[agent].forEach(t => {
      idx++;
      const tr = document.createElement('tr');
      tr.dataset.agent = t.agent_name;
      tr.dataset.ticket = t.ticket_no;
      if (t.ticket_type === 'temp' || t.is_temp) tr.className = 'temp-row';

      const createdDisplay = t.created_date ? (t.created_date + (t.created_time ? ' ' + t.created_time : '')) : '';
      const isTemp = t.ticket_type === 'temp' || t.is_temp;
      const typeBadge = isTemp
        ? '<span class="temp-badge">TEMP</span><br><span style="font-size:9px;color:var(--text3)">from ' + escHtml(t.original_agent) + '</span>'
        : '<span class="own-badge">OWN</span>';
      const ws = t.work_status || 'pending';

      tr.innerHTML =
        '<td style="color:var(--text3)">' + idx + '</td>' +
        '<td style="font-weight:600;white-space:nowrap">' + escHtml(t.ticket_no) + (isTemp ? '<br><span style="font-size:9px;color:var(--text3)">' + escHtml(t.report_date) + '</span>' : '') + '</td>' +
        '<td style="text-align:center">' + typeBadge + '</td>' +
        '<td>' + workStatusSelect(t) + '</td>' +
        '<td style="white-space:nowrap;font-size:11px">' + escHtml(createdDisplay) + '</td>' +
        '<td>' + escHtml(t.phone) + '</td>' +
        '<td style="max-width:180px;font-size:11px">' + escHtml(t.disposition_l4 || t.disposition_l3) + '</td>' +
        '<td style="max-width:150px">' + escHtml(t.customer_name) + '</td>' +
        '<td style="max-width:150px;font-size:11px">' + escHtml(t.mapped_partner) + '</td>' +
        '<td style="font-size:11px">' + escHtml(t.current_queue) + '</td>' +
        '<td style="text-align:center">' + (t.reopen_count || 0) + '</td>' +
        '<td style="font-size:11px">' + escHtml(t.status) + '</td>' +
        '<td>' + agingBadge(t.pending_hours, t.aging_bucket) + '</td>' +
        editableCell(t, 'ground_team_update', 'text') +
        '<td style="font-size:11px;white-space:nowrap">' + escHtml(t.assigned_at ? t.assigned_at.split(' ')[0] : '') + '</td>' +
        '<td style="font-weight:600;color:var(--accent)">' + escHtml(t.agent_name) + '</td>' +
        editableCell(t, 'ping_status', 'select', ['', 'Pinged', 'No Response', 'Responded']) +
        editableCell(t, 'cx_action', 'text') +
        editableCell(t, 'px_call_status', 'select', ['', 'Called', 'DNP', 'Busy', 'Switched Off', 'Not Reachable', 'Call Back']) +
        editableCell(t, 'update_date', 'text') +
        editableCell(t, 'agent_remark', 'text') +
        editableCell(t, 'partner_concern', 'text');

      tbody.appendChild(tr);
    });
  });
}

function workStatusSelect(ticket) {
  const ws = ticket.work_status || 'pending';
  const rd = ticket.report_date;
  const tid = ticket.ticket_no;
  const opts = ['pending', 'in_progress', 'completed'];
  const labels = {'pending':'Pending', 'in_progress':'In Progress', 'completed':'Completed'};
  const colors = {'pending':'var(--orange)', 'in_progress':'var(--blue)', 'completed':'var(--green)'};
  const optHtml = opts.map(o =>
    '<option value="' + o + '"' + (o === ws ? ' selected' : '') + ' style="color:' + colors[o] + '">' + labels[o] + '</option>'
  ).join('');
  return '<select class="ws-select" style="color:' + colors[ws] + '" data-rd="' + escHtml(rd) + '" data-tid="' + escHtml(tid) + '" onchange="saveWorkStatus(this)">' + optHtml + '</select>';
}

async function saveWorkStatus(el) {
  const reportDate = el.dataset.rd;
  const ticketNo = el.dataset.tid;
  const status = el.value;
  const colors = {'pending':'var(--orange)', 'in_progress':'var(--blue)', 'completed':'var(--green)'};
  el.style.color = colors[status] || '';
  await api('/api/agent/update-ticket', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ date: reportDate, ticket_no: ticketNo, updates: { work_status: status } })
  });
  const t = assignments.find(a => a.ticket_no === ticketNo && a.report_date === reportDate);
  if (t) t.work_status = status;
  toast('Status: ' + status, 'success');
}

function editableCell(ticket, field, type, options) {
  const val = ticket[field] || '';
  const tid = ticket.ticket_no;
  const rd = ticket.report_date;
  const cellId = 'cell-' + field + '-' + tid;

  if (type === 'select') {
    const optHtml = (options || []).map(o =>
      '<option value="' + escHtml(o) + '"' + (o === val ? ' selected' : '') + '>' + (o || '&#8212;') + '</option>'
    ).join('');
    return '<td class="editable" id="' + cellId + '">' +
      '<select class="cell-select" data-tid="' + escHtml(tid) + '" data-rd="' + escHtml(rd) + '" data-field="' + field + '" onchange="saveCellFromSelect(this)">' +
      optHtml + '</select></td>';
  }

  return '<td class="editable" id="' + cellId + '" data-tid="' + escHtml(tid) + '" data-rd="' + escHtml(rd) + '" data-field="' + field + '" onclick="startEditFromTd(this)">' +
    '<div class="cell-val">' + escHtml(val) + '</div></td>';
}

function saveCellFromSelect(el) {
  saveCell(el.dataset.rd, el.dataset.tid, el.dataset.field, el.value);
}

function startEditFromTd(td) {
  startEdit(td, td.dataset.rd, td.dataset.tid, td.dataset.field);
}

function startEdit(td, reportDate, ticketNo, field) {
  if (td.querySelector('textarea')) return;
  const current = td.querySelector('.cell-val')?.textContent || '';
  td.innerHTML = '<textarea>' + escHtml(current) + '</textarea>';
  const ta = td.querySelector('textarea');
  ta.focus();
  ta.selectionStart = ta.value.length;

  ta.onblur = () => {
    const newVal = ta.value.trim();
    saveCell(reportDate, ticketNo, field, newVal);
    td.innerHTML = '<div class="cell-val">' + escHtml(newVal) + '</div>';
    td.onclick = () => startEdit(td, reportDate, ticketNo, field);
  };

  ta.onkeydown = (e) => {
    if (e.key === 'Escape') { ta.blur(); }
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); ta.blur(); }
  };
}

async function saveCell(reportDate, ticketNo, field, value) {
  const updates = {};
  updates[field] = value;
  await api('/api/agent/update-ticket', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ date: reportDate, ticket_no: ticketNo, updates })
  });
  // Update local state
  const t = assignments.find(a => a.ticket_no === ticketNo && a.report_date === reportDate);
  if (t) t[field] = value;
  toast('Saved', 'success');
}

function filterTable() {
  const agent = $('agentFilter').value;
  const rows = $('ticketBody').querySelectorAll('tr');
  let visibleCount = 0;
  rows.forEach(tr => {
    if (!agent) {
      tr.style.display = '';
      if (!tr.classList.contains('agent-separator')) visibleCount++;
    } else {
      if (tr.dataset.agent === agent) {
        tr.style.display = '';
        if (!tr.classList.contains('agent-separator')) visibleCount++;
      } else {
        tr.style.display = 'none';
      }
    }
  });
  $('ticketCount').textContent = agent ? visibleCount : assignments.length;
  highlightCard(agent);
}

function downloadCSV() {
  const agent = $('agentFilter').value;
  let url = '/api/agent/download?date=' + currentDate;
  if (agent) url += '&agent=' + encodeURIComponent(agent);
  window.open(url);
}

// ---- Boot ----
init();
</script>
</body>
</html>"""


def main():
    init_db()
    print(f"Starting PFT Advanced Dashboard on http://localhost:{PORT}")
    print(f"Open http://localhost:{PORT} in your browser")
    print(f"Agent Dashboard: http://localhost:{PORT}/agent")
    print("Press Ctrl+C to stop.\n")
    server = http.server.ThreadingHTTPServer(("", PORT), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
