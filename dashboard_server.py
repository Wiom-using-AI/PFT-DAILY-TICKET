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
    init_db,
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
                self.send_json(summary if summary else {"error": "No data"})
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

  .cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px;margin-bottom:18px}}
  .card{{background:var(--card);border-radius:10px;padding:16px;border:1px solid var(--border);box-shadow:var(--shadow);
    transition:box-shadow .2s}}
  .card:hover{{box-shadow:var(--shadow-md)}}
  .card-label{{font-size:10px;color:var(--text2);text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px;font-weight:600}}
  .card-value{{font-size:28px;font-weight:700;letter-spacing:-0.5px}}
  .card-value.green{{color:var(--green)}} .card-value.red{{color:var(--red)}}
  .card-value.orange{{color:var(--orange)}} .card-value.blue{{color:var(--accent)}}
  .card-sub{{font-size:10px;color:var(--text2);margin-top:3px}}
  .card-delta{{font-size:10px;margin-top:3px;font-weight:600}}
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
</style>
</head>
<body>

<div class="header">
  <h1><span>PFT</span> Internet Issues Dashboard</h1>
  <div class="header-right">
    <a href="{MASTER_SHEET_URL}" target="_blank" class="btn btn-primary">&#128196; Master Sheet</a>
    <button class="btn btn-download" onclick="downloadAll()">&#11015; Download All Data</button>
    <button class="btn" onclick="window.print()">&#128424; Print</button>
  </div>
</div>

<!-- Date Navigation -->
<div class="date-nav" id="dateNav">
  <label>View:</label>
  <button class="date-btn" onclick="navigateDate('latest')">Today</button>
  <button class="date-btn" onclick="navigateDate(-1)">D-1</button>
  <button class="date-btn" onclick="navigateDate(-2)">D-2</button>
  <button class="date-btn" onclick="navigateDate(-3)">D-3</button>
  <button class="date-btn" onclick="navigateDate(-5)">D-5</button>
  <button class="date-btn" onclick="navigateDate(-7)">D-7</button>
  <button class="date-btn" onclick="navigateDate(-14)">D-14</button>
  <button class="date-btn" onclick="navigateDate(-30)">D-30</button>
  <select class="date-select" id="dateSelect" onchange="loadDate(this.value)">
    <option value="">All dates...</option>
  </select>
  <span class="date-info" id="dateInfo">Loading...</span>
</div>

<!-- Summary Cards -->
<div class="cards" id="summaryCards"><div class="loading">Loading...</div></div>

<!-- Category Bifurcation (All Ticket Types from Email) -->
<div class="section" id="categorySection">
  <div class="section-header">
    <h3>&#128202; Ticket Bifurcation — All Categories from Email Report</h3>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px" id="categoryContent">
    <div class="loading">Loading...</div>
  </div>
</div>

<!-- Master Sheet Comparison -->
<div class="section" id="masterSection" style="background:linear-gradient(135deg,#f0f7ff,#e8f0fe);border-left:3px solid var(--accent)">
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
<div class="section" id="trendSection">
  <div class="section-header">
    <h3>Daily Trend (All Available Dates)</h3>
    <button class="btn btn-sm btn-download" onclick="downloadSection('trends')">&#11015; CSV</button>
  </div>
  <div class="chart-container" style="height:280px"><canvas id="trendChart"></canvas></div>
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
let prevSummary = null;
let allTickets = [];
let filteredTickets = [];
let drillData = [];
let charts = {{}};

async function api(path) {{ const r = await fetch(path); return r.json(); }}

// ========== INIT ==========
async function init() {{
  availableDates = await api('/api/dates');
  const select = document.getElementById('dateSelect');
  select.innerHTML = '<option value="">All dates...</option>';
  availableDates.forEach((d, i) => {{
    const opt = document.createElement('option');
    opt.value = d;
    opt.textContent = i === 0 ? `${{d}} (Latest)` : `${{d}} (D-${{i}})`;
    select.appendChild(opt);
  }});
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
  const idx = Math.min(Math.abs(offset), availableDates.length - 1);
  if (availableDates[idx]) loadDate(availableDates[idx]);
}}

async function loadDate(date) {{
  if (!date) return;
  currentDate = date;
  document.getElementById('dateSelect').value = date;
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
  loadCategories(date);
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

async function loadCategories(date) {{
  try {{
    // Fetch both: simple category breakdown + pivot data
    const [cats, pivot] = await Promise.all([
      api(`/api/categories?date=${{date}}`),
      api(`/api/category-aging?date=${{date}}`).catch(() => null),
    ]);

    if ((!cats || Object.keys(cats).length === 0) && !pivot) {{
      document.getElementById('categoryContent').innerHTML = '<div class="loading">No category data available</div>';
      return;
    }}

    // ---- PIVOT TABLE (Category × Aging Bracket) ----
    let pivotHtml = '';
    if (pivot && pivot.categories && pivot.categories.length > 0) {{
      const buckets = pivot.buckets;
      const catData = pivot.data;
      const totalsByCat = pivot.totals_by_cat;
      const totalsByBucket = pivot.totals_by_bucket;
      const grandTotal = pivot.grand_total;

      // Build header row
      let headerCells = `<th style="text-align:left;min-width:180px;position:sticky;left:0;background:#f8fafc;z-index:2">Disposition Folder Level 3</th>`;
      buckets.forEach(b => {{
        headerCells += `<th style="text-align:center;min-width:80px;white-space:nowrap">${{b}}</th>`;
      }});
      headerCells += `<th style="text-align:center;min-width:90px;font-weight:700">Grand Total</th>`;

      // Build data rows
      let bodyRows = '';
      pivot.categories.forEach(cat => {{
        const isInternet = cat === 'Internet Issues';
        const color = CAT_COLORS[cat] || '#94a3b8';
        const rowStyle = isInternet
          ? 'background:#eff6ff;font-weight:700'
          : '';

        let cells = `<td style="position:sticky;left:0;background:${{isInternet ? '#eff6ff' : '#fff'}};z-index:1">
          <span class="dot" style="background:${{color}}"></span>${{cat}}${{isInternet ? ' &#9733;' : ''}}
        </td>`;

        buckets.forEach(b => {{
          const val = (catData[cat] && catData[cat][b]) || 0;
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

        // Grand Total for this category
        const catTotal = totalsByCat[cat] || 0;
        const encCat = encodeURIComponent(cat);
        cells += `<td class="num" style="font-weight:700">
          <a href="/api/download-category-bucket?date=${{currentDate}}&category=${{encCat}}"
             style="color:#1a73e8;text-decoration:none;border-bottom:1px dashed #1a73e8"
             title="Download all ${{catTotal}} tickets: ${{cat}}"
             target="_blank">${{catTotal.toLocaleString()}}</a>
        </td>`;

        bodyRows += `<tr style="${{rowStyle}}">${{cells}}</tr>`;
      }});

      // Grand Total row
      let totalCells = `<td style="position:sticky;left:0;background:#f1f5f9;z-index:1;font-weight:700">Grand Total</td>`;
      buckets.forEach(b => {{
        const val = totalsByBucket[b] || 0;
        const encBuck = encodeURIComponent(b);
        totalCells += `<td class="num" style="font-weight:700">
          <a href="/api/download-category-bucket?date=${{currentDate}}&bucket=${{encBuck}}"
             style="color:#1a73e8;text-decoration:none;border-bottom:1px dashed #1a73e8"
             title="Download all ${{val}} tickets in ${{b}}"
             target="_blank">${{val.toLocaleString()}}</a>
        </td>`;
      }});
      totalCells += `<td class="num" style="font-weight:700">
        <a href="/api/download-category-bucket?date=${{currentDate}}"
           style="color:#1a73e8;text-decoration:none;border-bottom:1px dashed #1a73e8"
           title="Download all ${{grandTotal}} tickets"
           target="_blank">${{grandTotal.toLocaleString()}}</a>
      </td>`;

      pivotHtml = `
        <div style="grid-column:1/-1;margin-bottom:12px">
          <div style="font-size:12px;color:var(--text2);margin-bottom:8px">
            &#128279; Click any number to download the raw ticket data for that cell
          </div>
          <div style="overflow-x:auto;border:1px solid var(--border);border-radius:8px">
            <table style="min-width:100%;border-collapse:collapse">
              <thead><tr style="background:#f8fafc;border-bottom:2px solid var(--border)">${{headerCells}}</tr></thead>
              <tbody>${{bodyRows}}
                <tr style="border-top:2px solid var(--border);background:#f1f5f9">${{totalCells}}</tr>
              </tbody>
            </table>
          </div>
        </div>`;
    }}

    // ---- SUMMARY TABLE + CHART (existing) ----
    let summaryHtml = '';
    let chartHtml = '';
    if (cats && Object.keys(cats).length > 0) {{
      const sorted = Object.entries(cats).sort((a,b) => b[1] - a[1]);
      const total = sorted.reduce((s, [k,v]) => s + v, 0);

      let tableRows = sorted.map(([cat, count]) => {{
        const pct = (count / total * 100).toFixed(1);
        const color = CAT_COLORS[cat] || '#94a3b8';
        const isInternet = cat === 'Internet Issues';
        return `<tr style="${{isInternet ? 'background:#eff6ff;font-weight:700' : ''}}">
          <td><span class="dot" style="background:${{color}}"></span>${{cat}}${{isInternet ? ' &#9733;' : ''}}</td>
          <td class="num">${{count.toLocaleString()}}</td>
          <td class="num">${{pct}}%</td>
          <td><div class="bar-bg"><div class="bar-fill" style="width:${{pct}}%;background:${{color}}"></div></div></td>
        </tr>`;
      }}).join('');

      tableRows += `<tr style="border-top:2px solid var(--border);font-weight:700">
        <td>TOTAL</td><td class="num">${{total.toLocaleString()}}</td><td class="num">100%</td><td></td></tr>`;

      summaryHtml = `<div>
        <div style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px">
          Category Summary</div>
        <table><thead><tr><th>Category (Disposition L3)</th><th style="text-align:right">Tickets</th>
        <th style="text-align:right">%</th><th>Distribution</th></tr></thead>
        <tbody>${{tableRows}}</tbody></table></div>`;

      chartHtml = `<div><div style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px">
        Distribution Chart</div>
        <div class="chart-container" style="height:280px"><canvas id="categoryChart"></canvas></div></div>`;
    }}

    document.getElementById('categoryContent').innerHTML = pivotHtml + summaryHtml + chartHtml;

    // Render doughnut chart
    if (cats && Object.keys(cats).length > 0) {{
      const sorted = Object.entries(cats).sort((a,b) => b[1] - a[1]);
      if (charts.category) charts.category.destroy();
      charts.category = new Chart(document.getElementById('categoryChart'), {{
        type: 'doughnut',
        data: {{
          labels: sorted.map(([k]) => k),
          datasets: [{{
            data: sorted.map(([,v]) => v),
            backgroundColor: sorted.map(([k]) => CAT_COLORS[k] || '#94a3b8'),
            borderWidth: 2,
            borderColor: '#ffffff',
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          cutout: '50%',
          plugins: {{
            legend: {{
              position: 'right',
              labels: {{ color: '#6b7280', padding: 8, font: {{ size: 10 }}, usePointStyle: true, pointStyle: 'circle' }}
            }}
          }}
        }}
      }});
    }}
  }} catch(e) {{
    document.getElementById('categoryContent').innerHTML = '<div class="loading">Could not load categories</div>';
  }}
}}

// ========== MASTER SHEET COMPARISON ==========
async function loadMasterComparison(date) {{
  document.getElementById('masterContent').innerHTML = '<div class="loading">Comparing with master sheet...</div>';
  try {{
    // 1. Get LOCKED morning snapshot
    const snapshot = await api(`/api/master-compare?date=${{date}}`);

    // 2. Get LIVE current status
    let live = null;
    try {{ live = await api(`/api/master-live?date=${{date}}`); }} catch(e) {{}}

    if (snapshot.error && !live) {{
      document.getElementById('masterContent').innerHTML = `<p style="color:var(--red)">No comparison data available</p>`;
      return;
    }}

    const s = snapshot.error ? null : snapshot;
    const pctNew = s && s.total_internet ? (s.new_to_upload / s.total_internet * 100).toFixed(1) : 0;
    const pctOld = s && s.total_internet ? (s.already_in_master / s.total_internet * 100).toFixed(1) : 0;

    // Compute upload progress — only if live data is valid
    let uploadedCount = 0;
    let stillPending = 0;
    let uploadPct = 0;
    let liveValid = false;
    if (s && live && s.snapshot_fixed && live.master_total > 0) {{
      uploadedCount = s.new_to_upload - live.new_to_upload;
      // Only treat as valid if the math makes sense (no negative values)
      if (uploadedCount >= 0) {{
        liveValid = true;
        stillPending = live.new_to_upload;
        uploadPct = s.new_to_upload > 0 ? Math.round(uploadedCount / s.new_to_upload * 100) : 100;
      }}
    }}
    const allUploaded = liveValid && stillPending === 0;

    document.getElementById('masterRefreshInfo').textContent =
      s && s.snapshot_fixed
        ? `Snapshot: ${{s.master_refreshed}} (locked)`
        : s && s.master_refreshed ? `Live: ${{s.master_refreshed}}` : '';

    document.getElementById('masterContent').innerHTML = `
      <!-- Morning Snapshot (Primary — always visible) -->
      <div style="margin-bottom:6px;display:flex;align-items:center;gap:8px">
        <span style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.5px">
          Morning Snapshot (10:15 AM — Locked)</span>
        <span style="font-size:10px;color:var(--text2)">${{s ? s.master_refreshed : ''}}</span>
      </div>
      <div class="cards" style="margin-bottom:16px">
        <div class="card" style="border-left:3px solid var(--accent)">
          <div class="card-label">Total Internet Issues</div>
          <div class="card-value blue">${{s ? s.total_internet.toLocaleString() : '—'}}</div>
          <div class="card-sub">Filtered from report</div>
        </div>
        <div class="card" style="border-left:3px solid var(--text2)">
          <div class="card-label">Already in Master</div>
          <div class="card-value" style="color:var(--text2)">${{s ? s.already_in_master.toLocaleString() : '—'}}</div>
          <div class="card-sub">${{pctOld}}% — Old/existing</div>
        </div>
        <div class="card" style="border-left:3px solid var(--green);background:#ecfdf5">
          <div class="card-label">&#9733; New Tickets to Upload</div>
          <div class="card-value green">${{s ? s.new_to_upload.toLocaleString() : '—'}}</div>
          <div class="card-sub">${{pctNew}}% — Not yet in master</div>
        </div>
        <div class="card" style="border-left:3px solid var(--border)">
          <div class="card-label">Master Sheet Total</div>
          <div class="card-value" style="color:var(--text2);font-size:22px">${{s ? s.master_total.toLocaleString() : '—'}}</div>
          <div class="card-sub">At time of snapshot</div>
        </div>
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:18px">
        <button class="btn btn-download" onclick="window.open('/api/download-new-tickets?date=${{currentDate}}')">
          &#11015; Download NEW Tickets (${{s ? s.new_to_upload : 0}}) — Full Details CSV
        </button>
        <button class="btn btn-sm" onclick="window.open('/api/download-existing-tickets?date=${{currentDate}}')">
          &#11015; Download Existing (${{s ? s.already_in_master : 0}})
        </button>
        <button class="btn btn-sm" onclick="showNewTicketsList()">
          &#128065; View New Ticket IDs
        </button>
      </div>

      <!-- Live Upload Status (only shown when valid data available) -->
      ${{liveValid ? `
      <div style="border-top:2px solid var(--border);padding-top:14px">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">
          <span style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.5px">
            Live Upload Status</span>
          <span style="font-size:10px;color:var(--text2)">Last checked: ${{live.master_refreshed || 'now'}}</span>
          <button class="btn btn-sm" onclick="refreshLiveStatus()" style="margin-left:auto">&#8635; Check Now</button>
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
            <div class="card-sub">of ${{s ? s.new_to_upload.toLocaleString() : '?'}} new tickets</div>
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
      </div>` : ''}}
    `;
    window._newTicketIds = s ? (s.new_ticket_ids || []) : [];
  }} catch(e) {{
    document.getElementById('masterContent').innerHTML =
      '<p style="color:var(--orange)">Could not load comparison. Check connection.</p>';
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
  await api('/api/refresh-master');
  setTimeout(() => loadMasterComparison(currentDate), 4000);
}}

// ========== DELTA ==========
function delta(curr, prev, key, invert=false) {{
  if (!prev || prev[key] == null || curr[key] == null) return '';
  const diff = curr[key] - prev[key];
  if (diff === 0) return '<div class="card-delta neutral">— No change</div>';
  const arrow = diff > 0 ? '&#9650;' : '&#9660;';
  const cls = invert ? (diff > 0 ? 'down' : 'up') : (diff > 0 ? 'up' : 'down');
  return `<div class="card-delta ${{cls}}">${{arrow}} ${{Math.abs(diff)}} vs prev day</div>`;
}}

// ========== SUMMARY CARDS ==========
function renderSummary(s) {{
  const pct48 = s.total_internet ? Math.round(s.critical_gt48h/s.total_internet*100) : 0;
  const pctInternet = s.total_pending ? (s.total_internet/s.total_pending*100).toFixed(1) : 0;
  document.getElementById('summaryCards').innerHTML = `
    <div class="card" style="border-left:3px solid var(--text2)">
      <div class="card-label">Total Tickets in Email</div>
      <div class="card-value" style="color:#e2e8f0">${{s.total_pending?.toLocaleString() || 0}}</div>
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

init();
</script>
</body>
</html>"""


def main():
    init_db()
    print(f"Starting PFT Advanced Dashboard on http://localhost:{PORT}")
    print(f"Open http://localhost:{PORT} in your browser")
    print("Press Ctrl+C to stop.\n")
    server = http.server.HTTPServer(("", PORT), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
