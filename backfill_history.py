"""
Backfill History - Download and process all historical 10 AM reports
====================================================================
Connects to Gmail via IMAP, finds all morning (~10 AM) pending reports
from last 60 days, downloads each, filters Internet Issues, saves to DB.
"""

import imaplib
import email
import re
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from pft_internet_ticket_agent import download_report, DOWNLOAD_DIR
from history_db import save_daily_snapshot, init_db, get_available_dates

import openpyxl

IST = timezone(timedelta(hours=5, minutes=30))
GMAIL_USER = "avakash.gupta@wiom.in"
GMAIL_APP_PASSWORD_ENV = "GMAIL_APP_PASSWORD"
IMAP_SERVER = "imap.gmail.com"
IMAP_PORT = 993
SENDER_FILTER = "no-reply-report@kapturecrm.com"
FILTER_COLUMN = "Disposition Folder Level 3"
FILTER_VALUE = "Internet Issues"


def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def filter_internet_tickets_for_backfill(input_path, report_date_str):
    """Filter tickets - returns (output_path, total_rows) or (None, total_rows)."""
    wb = openpyxl.load_workbook(input_path, read_only=True)
    ws = wb.active

    headers = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]

    try:
        filter_col_idx = headers.index(FILTER_COLUMN)
    except ValueError:
        log(f"  ERROR: Column '{FILTER_COLUMN}' not found!")
        wb.close()
        return None, 0

    filtered_rows = []
    total_rows = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        total_rows += 1
        cell_value = row[filter_col_idx]
        if cell_value and str(cell_value).strip() == FILTER_VALUE:
            filtered_rows.append(row)
    wb.close()

    if not filtered_rows:
        return None, total_rows

    # Create output file
    output_filename = f"internet_issues_tickets_{report_date_str.replace('-', '')}_1010.xlsx"
    output_path = os.path.join(DOWNLOAD_DIR, output_filename)

    out_wb = openpyxl.Workbook()
    out_ws = out_wb.active
    out_ws.title = "Internet Issues"

    # Write headers
    for col_idx, header in enumerate(headers, 1):
        out_ws.cell(row=1, column=col_idx, value=header)

    # Write filtered data
    for row_idx, row_data in enumerate(filtered_rows, 2):
        for col_idx, value in enumerate(row_data, 1):
            out_ws.cell(row=row_idx, column=col_idx, value=value)

    out_wb.save(output_path)
    return output_path, total_rows


def get_all_morning_emails(days_back=60):
    """Fetch all ~10 AM morning report emails from last N days via IMAP."""
    password = os.environ.get(GMAIL_APP_PASSWORD_ENV)
    if not password:
        log(f"ERROR: Set {GMAIL_APP_PASSWORD_ENV} environment variable!")
        sys.exit(1)

    log("Connecting to Gmail via IMAP...")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    mail.login(GMAIL_USER, password)
    mail.select("inbox")

    since_date = (datetime.now(IST) - timedelta(days=days_back)).strftime("%d-%b-%Y")
    search_criteria = f'(FROM "{SENDER_FILTER}" SINCE "{since_date}" SUBJECT "pending")'
    log(f"IMAP search: {search_criteria}")

    status, message_ids = mail.search(None, search_criteria)
    if status != "OK" or not message_ids[0]:
        log("No matching emails found.")
        mail.logout()
        return []

    ids = message_ids[0].split()
    log(f"Found {len(ids)} total emails. Filtering for ~10 AM ones...")

    morning_emails = []

    for msg_id in ids:
        status, msg_data = mail.fetch(msg_id, "(RFC822)")
        if status != "OK":
            continue

        msg = email.message_from_bytes(msg_data[0][1])
        subject = str(msg.get("Subject", "")).lower()

        # Parse date
        date_str_raw = msg.get("Date", "")
        try:
            msg_date = email.utils.parsedate_to_datetime(date_str_raw)
            if msg_date.tzinfo is None:
                msg_date = msg_date.replace(tzinfo=timezone.utc)
            msg_date_ist = msg_date.astimezone(IST)
        except Exception:
            continue

        # Only keep 9:30 AM - 11:00 AM IST emails
        if not (9 <= msg_date_ist.hour <= 10 and
                (msg_date_ist.hour == 10 and msg_date_ist.minute <= 30 or msg_date_ist.hour == 9)):
            continue

        # Extract download link
        body_text = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() in ("text/plain", "text/html"):
                    payload = part.get_payload(decode=True)
                    if payload:
                        body_text += payload.decode("utf-8", errors="ignore")
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                body_text = payload.decode("utf-8", errors="ignore")

        pattern = r'https://storage\.googleapis\.com/kapture_report/EXCEL_Report/[^">\s]+'
        match = re.search(pattern, body_text)
        if not match:
            continue

        report_date = msg_date_ist.strftime("%Y-%m-%d")
        morning_emails.append({
            "date": report_date,
            "time_ist": msg_date_ist,
            "download_url": match.group(0),
            "subject": msg.get("Subject", ""),
        })

    mail.logout()

    # Deduplicate by date (keep earliest per date)
    seen_dates = {}
    for em in morning_emails:
        d = em["date"]
        if d not in seen_dates:
            seen_dates[d] = em

    result = sorted(seen_dates.values(), key=lambda x: x["date"])
    log(f"Found {len(result)} unique morning reports")
    return result


def backfill(days_back=60, skip_existing=True):
    """Main backfill function."""
    init_db()

    existing_dates = set(get_available_dates()) if skip_existing else set()
    log(f"Already in DB: {len(existing_dates)} dates")

    morning_emails = get_all_morning_emails(days_back)

    if not morning_emails:
        log("No morning emails found. Nothing to backfill.")
        return

    # Filter out existing
    to_process = [e for e in morning_emails if e["date"] not in existing_dates]
    log(f"Need to process: {len(to_process)} dates (skipping {len(morning_emails) - len(to_process)} existing)")

    success = 0
    failed = 0

    for i, em in enumerate(to_process):
        date = em["date"]
        url = em["download_url"]
        report_time = em["time_ist"].replace(minute=10, second=0, microsecond=0)

        log(f"\n[{i+1}/{len(to_process)}] Processing {date}...")
        log(f"  Email time: {em['time_ist'].strftime('%I:%M %p IST')}")

        try:
            # Download
            filename = f"pending_report_{date.replace('-', '')}_morning.xlsx"
            filepath = os.path.join(DOWNLOAD_DIR, filename)

            if os.path.exists(filepath):
                log(f"  Report already downloaded: {filename}")
            else:
                log(f"  Downloading...")
                urllib.request.urlretrieve(url, filepath)
                size_mb = os.path.getsize(filepath) / (1024 * 1024)
                log(f"  Downloaded ({size_mb:.1f} MB)")

            # Filter
            log(f"  Filtering Internet Issues...")
            output_path, total_rows = filter_internet_tickets_for_backfill(filepath, date)

            if output_path:
                # Save to DB
                log(f"  Saving to database...")
                count = save_daily_snapshot(output_path, date, report_time, total_rows)
                log(f"  Done! {count} internet tickets from {total_rows} total")
                success += 1
            else:
                log(f"  No Internet Issues tickets found for {date}")
                failed += 1

            # Small delay to be polite to Google's servers
            time.sleep(1)

        except Exception as e:
            log(f"  ERROR processing {date}: {e}")
            failed += 1
            continue

    log(f"\n{'='*60}")
    log(f"BACKFILL COMPLETE")
    log(f"  Processed: {success + failed}")
    log(f"  Success: {success}")
    log(f"  Failed: {failed}")
    log(f"  Total dates in DB: {len(get_available_dates())}")
    log(f"{'='*60}")


if __name__ == "__main__":
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    backfill(days_back=days)
