"""
History Database - Stores daily ticket snapshots in SQLite
===========================================================
Each day's run saves all Internet Issues tickets into the database.
This enables looking back at any past date (d-1, d-2, d-n).
"""

import sqlite3
import os
from datetime import datetime, timezone, timedelta

import openpyxl

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "ticket_history.db")
IST = timezone(timedelta(hours=5, minutes=30))


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create the database tables if they don't exist."""
    conn = get_connection()
    c = conn.cursor()

    # Daily summary table - one row per report date
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_summary (
            report_date TEXT PRIMARY KEY,
            report_time TEXT,
            total_pending INTEGER,
            total_internet INTEGER,
            created_today INTEGER,
            critical_gt48h INTEGER,
            bucket_lt4h INTEGER DEFAULT 0,
            bucket_4_12h INTEGER DEFAULT 0,
            bucket_12_24h INTEGER DEFAULT 0,
            bucket_24_36h INTEGER DEFAULT 0,
            bucket_36_48h INTEGER DEFAULT 0,
            bucket_48_72h INTEGER DEFAULT 0,
            bucket_72_120h INTEGER DEFAULT 0,
            bucket_gt120h INTEGER DEFAULT 0,
            queue_partner INTEGER DEFAULT 0,
            queue_cx_high_pain INTEGER DEFAULT 0,
            queue_px_send_wiom INTEGER DEFAULT 0,
            master_total INTEGER DEFAULT 0,
            master_already INTEGER DEFAULT 0,
            master_new INTEGER DEFAULT 0,
            master_new_ids TEXT DEFAULT '',
            master_snapshot_time TEXT,
            inserted_at TEXT
        )
    """)

    # Add master columns to existing tables (migration-safe)
    for col, default in [
        ("master_total", 0), ("master_already", 0), ("master_new", 0),
        ("master_new_ids", "''"), ("master_snapshot_time", "NULL"),
    ]:
        try:
            c.execute(f"ALTER TABLE daily_summary ADD COLUMN {col} INTEGER DEFAULT {default}"
                      if col != "master_new_ids" and col != "master_snapshot_time"
                      else f"ALTER TABLE daily_summary ADD COLUMN {col} TEXT DEFAULT {default}")
        except Exception:
            pass  # Column already exists

    # Ticket-level detail table - all tickets per day
    c.execute("""
        CREATE TABLE IF NOT EXISTS ticket_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date TEXT NOT NULL,
            ticket_no TEXT,
            created_date TEXT,
            created_time TEXT,
            pending_hours REAL,
            aging_bucket TEXT,
            pending_days INTEGER,
            current_queue TEXT,
            sub_status TEXT,
            status TEXT,
            zone TEXT,
            mapped_partner TEXT,
            city TEXT,
            customer_name TEXT,
            device_id TEXT,
            channel_partner TEXT,
            disposition_l1 TEXT,
            disposition_l2 TEXT,
            disposition_l3 TEXT,
            UNIQUE(report_date, ticket_no)
        )
    """)

    # Index for fast queries
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticket_date ON ticket_history(report_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticket_no ON ticket_history(ticket_no)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticket_bucket ON ticket_history(report_date, aging_bucket)")

    conn.commit()
    conn.close()


def parse_datetime_ist(date_str, time_str):
    """Parse date + time into IST datetime."""
    if not date_str or not time_str:
        return None
    try:
        dt = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M:%S")
        return dt.replace(tzinfo=IST)
    except (ValueError, TypeError):
        return None


AGING_BUCKETS = [
    ("< 4h", 0, 4),
    ("4h - 12h", 4, 12),
    ("12h - 24h", 12, 24),
    ("24h - 36h", 24, 36),
    ("36h - 48h", 36, 48),
    ("48h - 72h", 48, 72),
    ("72h - 120h", 72, 120),
    ("> 120h", 120, float("inf")),
]


def get_bucket(hours):
    if hours is None:
        return "Unknown"
    for label, low, high in AGING_BUCKETS:
        if low <= hours < high:
            return label
    return "> 120h"


def save_daily_snapshot(filtered_xlsx_path, report_date_str, report_time_ist, total_pending=None):
    """
    Save all tickets from a filtered Internet Issues xlsx into the database.

    Args:
        filtered_xlsx_path: Path to the internet_issues_tickets_*.xlsx file
        report_date_str: Date string like "2026-03-18"
        report_time_ist: datetime object of when report was generated (10 AM IST)
        total_pending: Total tickets in the original pending report (before filtering)
    """
    init_db()
    conn = get_connection()
    c = conn.cursor()

    # Check if already saved for this date
    c.execute("SELECT COUNT(*) FROM daily_summary WHERE report_date = ?", (report_date_str,))
    if c.fetchone()[0] > 0:
        print(f"[History] Data for {report_date_str} already exists. Updating...")
        c.execute("DELETE FROM ticket_history WHERE report_date = ?", (report_date_str,))
        c.execute("DELETE FROM daily_summary WHERE report_date = ?", (report_date_str,))

    # Load the filtered xlsx
    wb = openpyxl.load_workbook(filtered_xlsx_path, read_only=True)
    ws = wb.active
    headers = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
    col = {h: i for i, h in enumerate(headers) if h}

    tickets = []
    bucket_counts = {}
    queue_counts = {"Partner": 0, "CX - High Pain": 0, "PX-Send to Wiom": 0}
    critical_count = 0
    created_today_count = 0
    report_date_ddmm = report_time_ist.strftime("%d/%m/%Y")

    for row in ws.iter_rows(min_row=2, values_only=True):
        created_date = row[col.get("Created Date", 1)]
        created_time = row[col.get("Created Time", 2)]
        created_dt = parse_datetime_ist(str(created_date) if created_date else None,
                                         str(created_time) if created_time else None)

        if created_dt and report_time_ist:
            hours = max(0, (report_time_ist - created_dt).total_seconds() / 3600)
        else:
            hours = None

        bucket = get_bucket(hours)
        queue = str(row[col.get("Current Queue Name", 47)] or "Unknown").strip()

        ticket = (
            report_date_str,
            str(row[col.get("Ticket No", 0)] or ""),
            str(created_date or ""),
            str(created_time or ""),
            round(hours, 1) if hours is not None else None,
            bucket,
            row[col.get("Pending No of Days", 63)],
            queue,
            str(row[col.get("Sub Status", 83)] or "Unknown").strip(),
            str(row[col.get("Status", 82)] or "Unknown").strip(),
            str(row[col.get("Zone", 70)] or "").strip(),
            str(row[col.get("Mapped Partner name", 69)] or "").strip(),
            str(row[col.get("City", 72)] or "").strip(),
            str(row[col.get("Customer Name", 65)] or "").strip(),
            str(row[col.get("Device ID", 68)] or "").strip(),
            str(row[col.get("Channel Partner", 67)] or "").strip(),
            str(row[col.get("Disposition Folder Level 1", 39)] or "").strip(),
            str(row[col.get("Disposition Folder Level 2", 40)] or "").strip(),
            str(row[col.get("Disposition Folder Level 3", 41)] or "").strip(),
        )
        tickets.append(ticket)

        # Aggregations
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        if queue in queue_counts:
            queue_counts[queue] += 1
        if hours is not None and hours > 48:
            critical_count += 1
        if str(created_date) == report_date_ddmm:
            created_today_count += 1

    wb.close()

    # Insert tickets
    c.executemany("""
        INSERT OR REPLACE INTO ticket_history
        (report_date, ticket_no, created_date, created_time, pending_hours, aging_bucket,
         pending_days, current_queue, sub_status, status, zone, mapped_partner, city,
         customer_name, device_id, channel_partner, disposition_l1, disposition_l2, disposition_l3)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, tickets)

    # Insert daily summary
    c.execute("""
        INSERT INTO daily_summary
        (report_date, report_time, total_pending, total_internet, created_today,
         critical_gt48h, bucket_lt4h, bucket_4_12h, bucket_12_24h, bucket_24_36h,
         bucket_36_48h, bucket_48_72h, bucket_72_120h, bucket_gt120h,
         queue_partner, queue_cx_high_pain, queue_px_send_wiom, inserted_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        report_date_str,
        report_time_ist.strftime("%Y-%m-%d %H:%M:%S"),
        total_pending,
        len(tickets),
        created_today_count,
        critical_count,
        bucket_counts.get("< 4h", 0),
        bucket_counts.get("4h - 12h", 0),
        bucket_counts.get("12h - 24h", 0),
        bucket_counts.get("24h - 36h", 0),
        bucket_counts.get("36h - 48h", 0),
        bucket_counts.get("48h - 72h", 0),
        bucket_counts.get("72h - 120h", 0),
        bucket_counts.get("> 120h", 0),
        queue_counts.get("Partner", 0),
        queue_counts.get("CX - High Pain", 0),
        queue_counts.get("PX-Send to Wiom", 0),
        datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
    ))

    conn.commit()
    conn.close()

    print(f"[History] Saved {len(tickets)} tickets for {report_date_str}")
    return len(tickets)


def save_master_snapshot(report_date_str, master_ids_set, ticket_ids_list):
    """
    Save master sheet comparison at the time of daily run (fixed snapshot).
    Called once when daily agent runs. Does NOT change later.
    """
    init_db()
    conn = get_connection()
    c = conn.cursor()

    already = [tid for tid in ticket_ids_list if tid in master_ids_set]
    new = [tid for tid in ticket_ids_list if tid not in master_ids_set]

    c.execute("""
        UPDATE daily_summary SET
            master_total = ?,
            master_already = ?,
            master_new = ?,
            master_new_ids = ?,
            master_snapshot_time = ?
        WHERE report_date = ?
    """, (
        len(master_ids_set),
        len(already),
        len(new),
        ",".join(new),
        datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        report_date_str,
    ))

    conn.commit()
    conn.close()
    print(f"[Master] Snapshot saved: {len(already)} existing, {len(new)} new, {len(master_ids_set)} in master")
    return {"already": len(already), "new": len(new), "master_total": len(master_ids_set), "new_ids": new}


def get_available_dates():
    """Return all dates that have data, sorted descending (newest first)."""
    init_db()
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT report_date FROM daily_summary ORDER BY report_date DESC")
    dates = [row["report_date"] for row in c.fetchall()]
    conn.close()
    return dates


def get_daily_summary(report_date):
    """Get summary for a specific date."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM daily_summary WHERE report_date = ?", (report_date,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_summaries():
    """Get all daily summaries for trend charts."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM daily_summary ORDER BY report_date ASC")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_tickets_for_date(report_date):
    """Get all ticket details for a specific date."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM ticket_history WHERE report_date = ? ORDER BY pending_hours DESC", (report_date,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_all_tickets_for_date(report_date):
    """Get ALL ticket columns for a specific date (for CSV download)."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM ticket_history WHERE report_date = ? ORDER BY pending_hours DESC", (report_date,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_ticket_trail(ticket_no):
    """Get the history of a specific ticket across all dates it appeared."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT report_date, pending_hours, aging_bucket, current_queue, sub_status, status
        FROM ticket_history
        WHERE ticket_no = ?
        ORDER BY report_date ASC
    """, (ticket_no,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at: {DB_PATH}")
    dates = get_available_dates()
    if dates:
        print(f"Available dates: {', '.join(dates)}")
    else:
        print("No data yet. Run the daily agent to start collecting.")
