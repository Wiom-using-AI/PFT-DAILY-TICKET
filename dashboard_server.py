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
    init_db,
    AGENT_LIST,
    get_agent_dates,
    save_attendance,
    get_attendance,
    assign_tickets_round_robin,
    get_agent_assignments,
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
                # Use stored new IDs from snapshot
                summary = get_daily_summary(date)
                new_ids = set()
                if summary and summary.get("master_new_ids"):
                    new_ids = set(x.strip() for x in summary["master_new_ids"].split(",") if x.strip())
                tickets = get_all_tickets_for_date(date)
                if new_ids:
                    new_tickets = [t for t in tickets if t.get("ticket_no") in new_ids]
                else:
                    # Fallback to live if no snapshot
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
        elif path == "/api/agent/download":
            date = params.get("date", [None])[0]
            agent = params.get("agent", [None])[0]
            if date:
                rows = get_agent_assignments(date, agent if agent else None)
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
<title>PFT Internet Issues Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
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
  .card-sub{{font-size:9px;color:var(--text2);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
  .card-delta{{font-size:9px;margin-top:2px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
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
  .num{{text-align:right;font-variant-numeric:tabular-nums;font-weight:600}}
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
  <h1><span>PFT</span> Internet Issues Dashboard</h1>
  <div class="header-right">
    <a href="/agent" class="btn btn-primary" style="background:#7c3aed;border-color:#7c3aed">&#128101; Agent Dashboard</a>
    <a href="{MASTER_SHEET_URL}" target="_blank" class="btn btn-primary">&#128196; Master Sheet</a>
    <button class="btn btn-download" onclick="downloadAll()">&#11015; Download All Data</button>
    <button class="btn" onclick="window.print()">&#128424; Print</button>
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

<!-- Aging Table -->
<div class="section" id="agingSection"><div class="loading">Loading...</div></div>

<!-- Charts Row 1: Aging + Queue -->
<div class="charts" id="chartsRow1"></div>

<!-- Heatmap: Queue x Aging -->
<div class="section" id="heatmapSection"></div>

<!-- Charts Row 2: Trends -->
<div class="dashboard-section" data-section-id="trendChart" data-section-label="Daily Trend Chart" draggable="true">
<div class="section" id="trendSection">
  <div class="section-toolbar">
    <button onclick="moveSectionUp(this.closest('.dashboard-section'))" title="Move up">&#11014;</button>
    <button onclick="moveSectionDown(this.closest('.dashboard-section'))" title="Move down">&#11015;</button>
    <button class="remove-btn" onclick="hideSection(this.closest('.dashboard-section'))" title="Remove section">&#10005;</button>
  </div>
  <div class="section-header">
    <h3>Daily Trend (All Available Dates)</h3>
    <button class="btn btn-sm btn-download" onclick="downloadSection('trends')">&#11015; CSV</button>
  </div>
  <div class="chart-container" style="height:280px"><canvas id="trendChart"></canvas></div>
</div>
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
const BUCKET_COLORS = ['#3b82f6','#22c55e','#84cc16','#eab308','#f97316','#ef4444','#dc2626','#991b1b'];
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

function navigateDate(offset) {{
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
  document.querySelectorAll('.date-btn').forEach(b => b.classList.remove('active'));

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
    `${{label}} | ${{shortDate(fromDate)}} - ${{shortDate(toDate)}} ${{toYear}} | ${{numDays}} day(s) data | Aggregated`;

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
  document.querySelectorAll('.date-btn').forEach(b => b.classList.remove('active'));

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

    let cells = `<td style="position:sticky;left:0;background:${{isInternet ? '#eff6ff' : '#fff'}};z-index:1">
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

      let cells = `<td style="position:sticky;left:0;background:${{isInternet ? '#eff6ff' : '#fff'}};z-index:1;white-space:nowrap">
        <span class="dot" style="background:${{color}}"></span>${{cat}}${{isInternet ? ' &#9733;' : ''}}
      </td>`;

      dates.forEach(d => {{
        const count = categories[cat][d] || 0;
        const total = dailyTotals[d] || 1;
        const pct = (count / total * 100).toFixed(1);
        cells += `<td class="num" style="font-size:11px">${{count > 0 ? pct + '%' : '—'}}</td>`;
      }});

      bodyRows += `<tr style="${{rowStyle}}">${{cells}}</tr>`;
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
      if (dd && !dd.closest('div').contains(e.target)) dd.classList.remove('show');
    }});

  }} catch(e) {{
    container.innerHTML = '<div class="loading">Could not load category trend</div>';
  }}
}}

// Show/hide category rows in the daily trend table
window.filterCatTrend = function() {{
  const checked = Array.from(document.querySelectorAll('#catTrendDropdown input[data-cattrend]:checked')).map(c => c.getAttribute('data-cattrend'));
  const total = document.querySelectorAll('#catTrendDropdown input[data-cattrend]').length;
  document.getElementById('catTrendFilterCount').textContent = `(${{checked.length}}/${{total}})`;

  // Get the table in categorySummaryContent
  const table = document.querySelector('#categorySummaryContent table');
  if (!table) return;
  const rows = table.querySelectorAll('tbody tr');
  rows.forEach(row => {{
    const firstTd = row.querySelector('td');
    if (!firstTd) return;
    const text = firstTd.textContent.trim().replace(' ★', '');
    if (text === 'TOTAL') return; // always show total
    row.style.display = checked.includes(text) ? '' : 'none';
  }});
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
  const diff = curr[key] - prev[key];
  let vsLabel = 'vs prev day';
  if (currentPeriodType === 'week') vsLabel = 'vs prev week';
  else if (currentPeriodType === 'month') vsLabel = 'vs prev month';
  else if (currentPeriodType === 'period' || currentRangeMode) vsLabel = 'vs same prev days';
  if (diff === 0) return `<div class="card-delta neutral">&mdash; No change ${{vsLabel}}</div>`;
  const arrow = diff > 0 ? '&#9650;' : '&#9660;';
  const cls = invert ? (diff > 0 ? 'down' : 'up') : (diff > 0 ? 'up' : 'down');
  return `<div class="card-delta ${{cls}}">${{arrow}} ${{Math.abs(diff)}} ${{vsLabel}}</div>`;
}}

// ========== SUMMARY CARDS ==========
function renderSummary(s) {{
  const pct48 = s.total_internet ? Math.round(s.critical_gt48h/s.total_internet*100) : 0;
  const pctInternet = s.total_pending ? (s.total_internet/s.total_pending*100).toFixed(1) : 0;
  document.getElementById('summaryCards').innerHTML = `
    <div class="card" style="border-left:3px solid var(--text2)">
      <div class="card-label">Total Tickets in Email</div>
      <div class="card-value" style="color:#1a1a2e">${{s.total_pending?.toLocaleString() || 0}}</div>
      <div class="card-sub">All pending tickets received</div>
      ${{delta(s, prevSummary, 'total_pending')}}</div>
    <div class="card" style="border-left:3px solid var(--accent)">
      <div class="card-label">Internet Issue Tickets</div>
      <div class="card-value blue">${{s.total_internet?.toLocaleString() || 0}}</div>
      <div class="card-sub">${{pctInternet}}% of total pending</div>
      ${{delta(s, prevSummary, 'total_internet')}}</div>
    <div class="card" style="border-left:3px solid var(--green)">
      <div class="card-label">Created on Report Day</div>
      <div class="card-value green">${{s.created_today?.toLocaleString() || 0}}</div>
      <div class="card-sub">New tickets that day</div>
      ${{delta(s, prevSummary, 'created_today')}}</div>
    <div class="card" style="border-left:3px solid var(--red)">
      <div class="card-label">Critical (&gt; 48h)</div>
      <div class="card-value red">${{s.critical_gt48h?.toLocaleString() || 0}}</div>
      <div class="card-sub">${{pct48}}% of internet tickets</div>
      ${{delta(s, prevSummary, 'critical_gt48h')}}</div>
    <div class="card" style="border-left:3px solid var(--orange)">
      <div class="card-label">Partner Queue</div>
      <div class="card-value orange">${{s.queue_partner?.toLocaleString() || 0}}</div>
      <div class="card-sub">Waiting on partner</div>
      ${{delta(s, prevSummary, 'queue_partner')}}</div>
    <div class="card" style="border-left:3px solid #a855f7">
      <div class="card-label">CX High Pain</div>
      <div class="card-value" style="color:#a855f7">${{s.queue_cx_high_pain?.toLocaleString() || 0}}</div>
      <div class="card-sub">Escalated</div>
      ${{delta(s, prevSummary, 'queue_cx_high_pain')}}</div>
    <div class="card" style="border-left:3px solid #06b6d4">
      <div class="card-label">PX-Send to Wiom</div>
      <div class="card-value" style="color:#06b6d4">${{s.queue_px_send_wiom?.toLocaleString() || 0}}</div>
      <div class="card-sub">Wiom queue</div>
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
    agents_json = json.dumps(AGENT_LIST)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PFT Agent Dashboard — Ticket Assignments</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {{
  --accent: #1a73e8; --green: #10b981; --red: #ef4444; --orange: #f59e0b;
  --bg: #f8fafc; --card: #ffffff; --border: #e2e8f0; --text: #1e293b; --text2: #64748b;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:'Inter',sans-serif; background:var(--bg); color:var(--text); font-size:13px; }}
.topbar {{ background:var(--card); border-bottom:1px solid var(--border); padding:12px 24px; display:flex; align-items:center; gap:16px; position:sticky; top:0; z-index:100; }}
.topbar h1 {{ font-size:16px; font-weight:700; }}
.topbar a {{ color:var(--accent); text-decoration:none; font-size:12px; }}
.container {{ max-width:1600px; margin:0 auto; padding:16px 24px; }}
.panel {{ background:var(--card); border:1px solid var(--border); border-radius:10px; padding:16px 20px; margin-bottom:16px; }}
.panel-title {{ font-size:13px; font-weight:700; color:var(--text2); text-transform:uppercase; letter-spacing:.5px; margin-bottom:12px; }}
.row {{ display:flex; gap:16px; flex-wrap:wrap; align-items:flex-start; }}
.row > * {{ flex:1; min-width:200px; }}

/* Attendance */
.agent-grid {{ display:flex; flex-wrap:wrap; gap:8px; }}
.agent-chip {{ display:flex; align-items:center; gap:6px; padding:6px 14px; border-radius:20px; border:2px solid var(--border);
  cursor:pointer; user-select:none; transition:all .2s; font-size:13px; font-weight:500; }}
.agent-chip:hover {{ border-color:var(--accent); }}
.agent-chip.present {{ background:#eff6ff; border-color:var(--accent); color:var(--accent); }}
.agent-chip.present::before {{ content:'\\2713'; font-weight:700; color:var(--green); }}
.agent-chip.absent {{ background:#fef2f2; border-color:#fca5a5; color:var(--red); opacity:.6; }}
.agent-chip.absent::before {{ content:'\\2717'; color:var(--red); }}

/* Buttons */
.btn {{ padding:8px 18px; border-radius:6px; border:1px solid var(--border); background:var(--card); cursor:pointer; font-size:12px; font-weight:600; transition:all .2s; }}
.btn:hover {{ background:#f1f5f9; }}
.btn-primary {{ background:var(--accent); color:white; border-color:var(--accent); }}
.btn-primary:hover {{ background:#1557b0; }}
.btn-green {{ background:var(--green); color:white; border-color:var(--green); }}
.btn-green:hover {{ background:#059669; }}
.btn-sm {{ padding:4px 10px; font-size:11px; }}

/* Summary cards */
.summary-cards {{ display:flex; gap:10px; flex-wrap:wrap; margin-bottom:12px; }}
.scard {{ padding:10px 16px; background:var(--bg); border-radius:8px; border:1px solid var(--border); min-width:120px; text-align:center; }}
.scard .name {{ font-size:11px; color:var(--text2); font-weight:600; }}
.scard .count {{ font-size:22px; font-weight:700; color:var(--accent); }}

/* Filter bar */
.filter-bar {{ display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin-bottom:12px; }}
.filter-bar select, .filter-bar input {{ padding:6px 12px; border:1px solid var(--border); border-radius:6px; font-size:13px; font-family:inherit; }}
.filter-bar select {{ min-width:160px; }}

/* Table */
.table-wrap {{ overflow-x:auto; border:1px solid var(--border); border-radius:8px; max-height:70vh; }}
table {{ width:100%; border-collapse:collapse; font-size:12px; white-space:nowrap; min-width:1800px; }}
thead th {{ background:#f1f5f9; padding:8px 10px; text-align:left; font-weight:700; font-size:11px;
  text-transform:uppercase; letter-spacing:.3px; color:var(--text2); position:sticky; top:0; z-index:2;
  border-bottom:2px solid var(--border); }}
tbody td {{ padding:6px 10px; border-bottom:1px solid #f1f5f9; }}
tbody tr:hover {{ background:#f8fafc; }}
tbody tr:nth-child(even) {{ background:#fafbfc; }}
.cell-ticket {{ color:var(--accent); font-weight:600; }}
.cell-agent {{ font-weight:600; }}
.aging-high {{ color:var(--red); font-weight:600; }}
.aging-med {{ color:var(--orange); font-weight:600; }}
.aging-low {{ color:var(--green); }}
.status-closed {{ color:var(--green); }}
.status-pending {{ color:var(--red); font-weight:600; }}

/* Date picker */
select#datePicker {{ padding:6px 12px; border:1px solid var(--border); border-radius:6px; font-size:13px; font-weight:600; }}

.badge {{ display:inline-block; padding:2px 8px; border-radius:10px; font-size:10px; font-weight:600; }}
.badge-green {{ background:#d1fae5; color:#065f46; }}
.badge-blue {{ background:#dbeafe; color:#1e40af; }}
.badge-red {{ background:#fee2e2; color:#991b1b; }}
.loading {{ text-align:center; color:var(--text2); padding:40px; }}
</style>
</head>
<body>

<div class="topbar">
  <h1>&#128101; Agent Dashboard — Ticket Assignments</h1>
  <select id="datePicker" onchange="loadDate(this.value)"></select>
  <span id="ticketCount" style="font-size:12px;color:var(--text2)"></span>
  <div style="margin-left:auto;display:flex;gap:8px">
    <a href="/">&#8592; Main Dashboard</a>
    <button class="btn btn-sm" onclick="downloadCSV()">&#11015; Download CSV</button>
  </div>
</div>

<div class="container">
  <!-- Attendance Panel -->
  <div class="panel">
    <div class="panel-title">&#128203; Agent Attendance — Mark Present</div>
    <div class="agent-grid" id="attendanceGrid"></div>
    <div style="margin-top:12px;display:flex;gap:8px;align-items:center">
      <button class="btn btn-primary" onclick="saveAttendanceAndAssign()">&#9989; Save Attendance & Assign Tickets</button>
      <button class="btn" onclick="reassignTickets()">&#8635; Re-assign (Round Robin)</button>
      <span id="assignStatus" style="font-size:12px;color:var(--text2)"></span>
    </div>
  </div>

  <!-- Summary -->
  <div class="panel" id="summaryPanel" style="display:none">
    <div class="panel-title">&#128202; Assignment Summary</div>
    <div class="summary-cards" id="summaryCards"></div>
  </div>

  <!-- Filter & Table -->
  <div class="panel">
    <div class="filter-bar">
      <label style="font-weight:600">Filter by Agent:</label>
      <select id="agentFilter" onchange="filterTable()">
        <option value="">All Agents</option>
      </select>
      <label style="font-weight:600">Search:</label>
      <input type="text" id="searchBox" placeholder="Ticket No, Customer, Partner..." oninput="filterTable()" style="width:220px">
      <label style="font-weight:600">Status:</label>
      <select id="statusFilter" onchange="filterTable()">
        <option value="">All</option>
        <option value="Ticket pending">Pending</option>
        <option value="Ticket Closed">Closed</option>
      </select>
      <span id="filterCount" style="font-size:12px;color:var(--text2)"></span>
    </div>
    <div class="table-wrap" style="max-height:70vh;overflow-y:auto">
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>Ticket No</th>
            <th>Created</th>
            <th>Phone</th>
            <th>Disposition L4</th>
            <th>Customer Name</th>
            <th>Mapped Partner</th>
            <th>Current Queue</th>
            <th>Reopen</th>
            <th>Kapture Status</th>
            <th>Aging</th>
            <th>Ground Team Update</th>
            <th>Assigned Date</th>
            <th>Worked By</th>
            <th>Ping</th>
            <th>After Call Cx Action</th>
            <th>Px Call Status</th>
            <th>Update Date</th>
            <th>PFT Agent Remark</th>
          </tr>
        </thead>
        <tbody id="ticketBody">
          <tr><td colspan="19" class="loading">Select a date to load assignments</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</div>

<script>
const AGENTS = {agents_json};
let currentDate = '';
let allAssignments = [];

async function api(url, opts) {{
  const r = await fetch(url, opts);
  return r.json();
}}

async function init() {{
  const dates = await api('/api/agent/dates');
  const sel = document.getElementById('datePicker');
  dates.forEach(d => {{
    const opt = document.createElement('option');
    opt.value = d; opt.textContent = d;
    sel.appendChild(opt);
  }});
  if (dates.length) loadDate(dates[0]);
}}

async function loadDate(date) {{
  currentDate = date;
  document.getElementById('datePicker').value = date;

  // Load attendance
  const att = await api(`/api/agent/attendance?date=${{date}}`);
  renderAttendance(att);

  // Load assignments
  await loadAssignments();
}}

function renderAttendance(att) {{
  const grid = document.getElementById('attendanceGrid');
  grid.innerHTML = AGENTS.map(a => {{
    const present = att[a] !== false;
    return `<div class="agent-chip ${{present ? 'present' : 'absent'}}"
                 onclick="toggleAgent(this, '${{a}}')"
                 data-agent="${{a}}" data-present="${{present ? '1' : '0'}}">
              ${{a}}</div>`;
  }}).join('');
}}

function toggleAgent(el, name) {{
  const isPresent = el.dataset.present === '1';
  el.dataset.present = isPresent ? '0' : '1';
  el.className = `agent-chip ${{isPresent ? 'absent' : 'present'}}`;
}}

function getPresentAgents() {{
  return Array.from(document.querySelectorAll('.agent-chip[data-present="1"]'))
    .map(el => el.dataset.agent);
}}

async function saveAttendanceAndAssign() {{
  const present = getPresentAgents();
  if (!present.length) {{ alert('Mark at least one agent as present'); return; }}
  const status = document.getElementById('assignStatus');
  status.textContent = 'Saving attendance & assigning...';

  await api('/api/agent/save-attendance', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{ date: currentDate, present }})
  }});

  const result = await api('/api/agent/assign', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{ date: currentDate, present }})
  }});

  if (result.status === 'assigned') {{
    status.innerHTML = `<span style="color:var(--green)">&#9989; Assigned ${{result.total}} tickets to ${{result.agents}} agents</span>`;
  }} else if (result.status === 'already_assigned') {{
    status.innerHTML = `<span style="color:var(--accent)">Tickets already assigned (${{result.count}}). Use Re-assign to change.</span>`;
  }} else {{
    status.innerHTML = `<span style="color:var(--red)">${{result.message || 'Error'}}</span>`;
  }}
  await loadAssignments();
}}

async function reassignTickets() {{
  const present = getPresentAgents();
  if (!present.length) {{ alert('Mark at least one agent as present'); return; }}
  if (!confirm(`Re-assign ALL tickets for ${{currentDate}} among ${{present.length}} agents?\\n\\nThis will clear existing assignments.`)) return;

  const status = document.getElementById('assignStatus');
  status.textContent = 'Re-assigning...';

  await api('/api/agent/save-attendance', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{ date: currentDate, present }})
  }});

  const result = await api('/api/agent/reassign', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{ date: currentDate, present }})
  }});

  if (result.status === 'assigned') {{
    status.innerHTML = `<span style="color:var(--green)">&#9989; Re-assigned ${{result.total}} tickets to ${{result.agents}} agents</span>`;
  }}
  await loadAssignments();
}}

async function loadAssignments() {{
  allAssignments = await api(`/api/agent/assignments?date=${{currentDate}}`);
  document.getElementById('ticketCount').textContent = `${{allAssignments.length}} tickets assigned`;

  // Summary
  const summary = {{}};
  allAssignments.forEach(t => {{ summary[t.agent_name] = (summary[t.agent_name] || 0) + 1; }});
  const panel = document.getElementById('summaryPanel');
  if (Object.keys(summary).length) {{
    panel.style.display = '';
    document.getElementById('summaryCards').innerHTML = Object.entries(summary)
      .sort((a,b) => b[1] - a[1])
      .map(([a, c]) => `<div class="scard"><div class="name">${{a}}</div><div class="count">${{c}}</div></div>`)
      .join('') + `<div class="scard"><div class="name">TOTAL</div><div class="count" style="color:var(--text)">${{allAssignments.length}}</div></div>`;
  }} else {{
    panel.style.display = 'none';
  }}

  // Populate agent filter
  const sel = document.getElementById('agentFilter');
  const current = sel.value;
  sel.innerHTML = '<option value="">All Agents</option>' +
    AGENTS.filter(a => summary[a]).map(a => `<option value="${{a}}">${{a}} (${{summary[a] || 0}})</option>`).join('');
  sel.value = current;

  filterTable();
}}

function filterTable() {{
  const agent = document.getElementById('agentFilter').value;
  const search = document.getElementById('searchBox').value.toLowerCase();
  const status = document.getElementById('statusFilter').value;

  let filtered = allAssignments;
  if (agent) filtered = filtered.filter(t => t.agent_name === agent);
  if (status) filtered = filtered.filter(t => (t.status || '').toLowerCase().includes(status.toLowerCase()));
  if (search) filtered = filtered.filter(t =>
    (t.ticket_no || '').toLowerCase().includes(search) ||
    (t.customer_name || '').toLowerCase().includes(search) ||
    (t.mapped_partner || '').toLowerCase().includes(search) ||
    (t.phone || '').toLowerCase().includes(search) ||
    (t.disposition_l4 || '').toLowerCase().includes(search) ||
    (t.agent_remark || '').toLowerCase().includes(search) ||
    (t.ground_team_update || '').toLowerCase().includes(search)
  );

  document.getElementById('filterCount').textContent = `Showing ${{filtered.length}} of ${{allAssignments.length}}`;

  const tbody = document.getElementById('ticketBody');
  if (!filtered.length) {{
    tbody.innerHTML = '<tr><td colspan="19" class="loading">No tickets found</td></tr>';
    return;
  }}

  tbody.innerHTML = filtered.map((t, i) => {{
    const agingClass = (t.aging_bucket || '').includes('120') || (t.aging_bucket || '').includes('72')
      ? 'aging-high' : (t.aging_bucket || '').includes('48') || (t.aging_bucket || '').includes('36')
      ? 'aging-med' : 'aging-low';
    const statusClass = (t.status || '').toLowerCase().includes('closed') ? 'status-closed' : 'status-pending';
    return `<tr>
      <td>${{i + 1}}</td>
      <td class="cell-ticket">${{t.ticket_no || ''}}</td>
      <td>${{t.created_date || ''}} ${{t.created_time || ''}}</td>
      <td>${{t.phone || ''}}</td>
      <td>${{t.disposition_l4 || ''}}</td>
      <td>${{t.customer_name || ''}}</td>
      <td>${{t.mapped_partner || ''}}</td>
      <td>${{t.current_queue || ''}}</td>
      <td>${{t.reopen_count || 0}}</td>
      <td class="${{statusClass}}">${{t.status || ''}}</td>
      <td class="${{agingClass}}">${{t.aging_bucket || ''}}</td>
      <td>${{t.ground_team_update || ''}}</td>
      <td>${{t.assigned_at ? t.assigned_at.split(' ')[0] : ''}}</td>
      <td class="cell-agent">${{t.agent_name || ''}}</td>
      <td>${{t.ping_status || ''}}</td>
      <td>${{t.cx_action || ''}}</td>
      <td>${{t.px_call_status || ''}}</td>
      <td>${{t.update_date || ''}}</td>
      <td>${{t.agent_remark || ''}}</td>
    </tr>`;
  }}).join('');
}}

function downloadCSV() {{
  const agent = document.getElementById('agentFilter').value;
  const url = `/api/agent/download?date=${{currentDate}}${{agent ? '&agent=' + encodeURIComponent(agent) : ''}}`;
  window.open(url);
}}

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
