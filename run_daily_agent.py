"""
Daily Runner - Automated Email Check + Download + Filter
=========================================================
This script is the daily entry point that:
1. Checks Gmail for today's ~10 AM "Queue Wise Pending Report" email
2. Extracts the download link
3. Downloads the report
4. Filters Internet Issues tickets
5. Saves the output

Can be scheduled via Windows Task Scheduler or run manually.
"""

import subprocess
import json
import re
import os
import sys
import urllib.request
import smtplib
from datetime import datetime, timezone, timedelta

# Add parent dir to path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from pft_internet_ticket_agent import (
    download_report,
    filter_internet_tickets,
    log,
    IST,
    DOWNLOAD_DIR,
)

# Gmail API via Google Apps Script or direct IMAP
# For this automation, we use IMAP to fetch the email

import imaplib
import email
from email.header import decode_header


# --- Configuration ---
GMAIL_USER = "avakash.gupta@wiom.in"
# App password (generate at https://myaccount.google.com/apppasswords)
# Store in environment variable for security
GMAIL_APP_PASSWORD_ENV = "GMAIL_APP_PASSWORD"

IMAP_SERVER = "imap.gmail.com"
IMAP_PORT = 993

SENDER_FILTER = "no-reply-report@kapturecrm.com"
SUBJECT_KEYWORDS = ["queue", "pending", "report"]


def get_app_password():
    """Get Gmail app password from environment variable."""
    password = os.environ.get(GMAIL_APP_PASSWORD_ENV)
    if not password:
        print(f"\nERROR: Gmail App Password not set!")
        print(f"Please set the environment variable: {GMAIL_APP_PASSWORD_ENV}")
        print(f"Steps to generate an App Password:")
        print(f"  1. Go to https://myaccount.google.com/apppasswords")
        print(f"  2. Select 'Mail' and your device")
        print(f"  3. Copy the 16-character password")
        print(f"  4. Set it: set {GMAIL_APP_PASSWORD_ENV}=your_app_password")
        sys.exit(1)
    return password


def search_todays_morning_report():
    """Connect to Gmail via IMAP and find today's morning pending report."""
    password = get_app_password()

    log("Connecting to Gmail via IMAP...")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    mail.login(GMAIL_USER, password)
    mail.select("inbox")

    # Search for today's emails from Kapture
    today = datetime.now(IST)
    date_str = today.strftime("%d-%b-%Y")  # e.g., "18-Mar-2026"

    search_criteria = f'(FROM "{SENDER_FILTER}" SINCE "{date_str}" SUBJECT "pending")'
    log(f"IMAP search: {search_criteria}")

    status, message_ids = mail.search(None, search_criteria)
    if status != "OK" or not message_ids[0]:
        log("No matching emails found for today.")
        mail.logout()
        return None

    ids = message_ids[0].split()
    log(f"Found {len(ids)} matching email(s) today")

    # Find the ~10 AM report (between 9:30 AM and 11:00 AM IST)
    target_start = today.replace(hour=9, minute=30, second=0, microsecond=0)
    target_end = today.replace(hour=11, minute=0, second=0, microsecond=0)

    best_match = None
    best_link = None

    for msg_id in ids:
        status, msg_data = mail.fetch(msg_id, "(RFC822)")
        if status != "OK":
            continue

        msg = email.message_from_bytes(msg_data[0][1])

        # Check subject contains queue/pending
        subject = str(msg.get("Subject", "")).lower()
        if not any(kw in subject for kw in SUBJECT_KEYWORDS):
            continue

        # Parse date
        date_str_raw = msg.get("Date", "")
        try:
            msg_date = email.utils.parsedate_to_datetime(date_str_raw)
            if msg_date.tzinfo is None:
                msg_date = msg_date.replace(tzinfo=timezone.utc)
            msg_date_ist = msg_date.astimezone(IST)
        except Exception:
            continue

        log(f"  Email: '{msg.get('Subject')}' at {msg_date_ist.strftime('%I:%M %p IST')}")

        # Extract download link from body
        body_text = ""
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type in ("text/plain", "text/html"):
                    payload = part.get_payload(decode=True)
                    if payload:
                        body_text += payload.decode("utf-8", errors="ignore")
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                body_text = payload.decode("utf-8", errors="ignore")

        # Extract Kapture download link
        pattern = r'https://storage\.googleapis\.com/kapture_report/EXCEL_Report/[^">\s]+'
        match = re.search(pattern, body_text)
        if not match:
            continue

        download_link = match.group(0)

        # Prefer the 10 AM report, but take any if no exact match
        if target_start <= msg_date_ist <= target_end:
            best_match = msg_date_ist
            best_link = download_link
            log(f"  -> Matched as 10 AM morning report!")
            break
        elif best_match is None:
            best_match = msg_date_ist
            best_link = download_link

    mail.logout()

    if best_link:
        log(f"Selected report from {best_match.strftime('%I:%M %p IST')}")
        log(f"Download link: {best_link}")
    else:
        log("No valid download link found in today's emails.")

    return best_link


def main():
    log("=" * 60)
    log("PFT INTERNET TICKET AGENT - DAILY RUN")
    log("=" * 60)

    # Step 1: Find today's email and get download link
    if len(sys.argv) > 1:
        # Allow passing a URL directly for testing
        download_url = sys.argv[1]
        log(f"Using provided URL: {download_url}")
    else:
        download_url = search_todays_morning_report()

    if not download_url:
        log("FAILED: Could not find today's pending report email.")
        log("Make sure the report has been sent (~10 AM IST).")
        sys.exit(1)

    # Step 2: Download the report
    timestamp = datetime.now(IST).strftime("%Y%m%d")
    filename = f"pending_report_{timestamp}_morning.xlsx"
    report_path = download_report(download_url, filename)

    # Step 3: Filter for Internet Issues
    output_path = filter_internet_tickets(report_path)

    if output_path:
        log("SUCCESS - Internet issues tickets extracted!")
        log(f"Output: {output_path}")

        # Step 4: Save to history database for dashboard
        try:
            from history_db import save_daily_snapshot, save_master_snapshot, get_tickets_for_date
            report_date = datetime.now(IST).strftime("%Y-%m-%d")
            report_time = datetime.now(IST).replace(hour=10, minute=10, second=0, microsecond=0)
            save_daily_snapshot(output_path, report_date, report_time)
            log("History database updated.")
        except Exception as e:
            log(f"Warning: Could not save to history DB: {e}")

        # Step 5: Snapshot master sheet comparison (FIXED at run time)
        try:
            import csv
            import io
            log("Fetching master sheet for comparison snapshot...")
            MASTER_CSV_URL = "https://docs.google.com/spreadsheets/d/1E3Ij57bFHznf3S6cRJSzONaVJ7Tgloud51Z__vXLet0/export?format=csv&gid=1626982265"
            req = urllib.request.Request(MASTER_CSV_URL)
            req.add_header("User-Agent", "Mozilla/5.0")
            response = urllib.request.urlopen(req, timeout=30)
            data = response.read().decode("utf-8-sig")
            reader = csv.reader(io.StringIO(data))
            next(reader)  # skip header
            master_ids = set()
            for row in reader:
                if row and row[0].strip():
                    master_ids.add(row[0].strip())
            log(f"Master sheet has {len(master_ids)} ticket IDs")

            # Get today's ticket IDs from DB
            tickets = get_tickets_for_date(report_date)
            ticket_ids = [t["ticket_no"] for t in tickets]
            result = save_master_snapshot(report_date, master_ids, ticket_ids)
            log(f"Master snapshot: {result['already']} existing, {result['new']} new to upload")
        except Exception as e:
            log(f"Warning: Could not snapshot master sheet: {e}")
    else:
        log("No Internet Issues tickets found in today's report.")

    return output_path


if __name__ == "__main__":
    main()
