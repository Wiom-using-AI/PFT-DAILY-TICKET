"""
Take a screenshot of the Ticket Bifurcation section from the Vercel dashboard
and post it to a Slack channel.
"""
import os
import sys
import json
import time
import requests
from datetime import datetime

# --- Config ---
DASHBOARD_URL = "https://pft-daily-ticket.vercel.app"
SECTION_ID = "categorySection"
SCREENSHOT_PATH = "ticket_bifurcation.png"

# Only show these 4 categories in the screenshot (others will be unchecked)
SELECTED_CATEGORIES = [
    "Internet Issues",
    "Others",
    "Shifting Request",
    "Partner Misbehavior",
]

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "")  # e.g., #pft-reports


def take_screenshot():
    """Use Playwright to screenshot the Ticket Bifurcation section."""
    from playwright.sync_api import sync_playwright

    print("[Slack] Opening dashboard for screenshot...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1400, "height": 900})

        # Navigate and wait for full load
        page.goto(DASHBOARD_URL, wait_until="networkidle", timeout=60000)

        # Wait a bit more for JS rendering (pivot table, charts)
        page.wait_for_timeout(5000)

        # Wait for the section to appear
        section = page.locator(f"#{SECTION_ID}")
        section.wait_for(state="visible", timeout=30000)

        # Apply filter: uncheck all categories, then check only the desired ones
        print(f"[Slack] Applying filter: {SELECTED_CATEGORIES}")
        selected_json = json.dumps(SELECTED_CATEGORIES)
        page.evaluate(f"""() => {{
            const selectedCats = {selected_json};
            // Uncheck all checkboxes first
            document.querySelectorAll('#pivotDropdown input[data-cat]').forEach(cb => {{
                cb.checked = false;
            }});
            // Check only the desired categories
            document.querySelectorAll('#pivotDropdown input[data-cat]').forEach(cb => {{
                const catName = cb.getAttribute('data-cat');
                if (selectedCats.includes(catName)) {{
                    cb.checked = true;
                }}
            }});
            // Update the filter count display
            const total = document.querySelectorAll('#pivotDropdown input[data-cat]').length;
            const checked = document.querySelectorAll('#pivotDropdown input[data-cat]:checked').length;
            const btn = document.querySelector('#pivotFilterBtn');
            if (btn) btn.innerHTML = '&#9776; Filter Categories <sup>(' + checked + '/' + total + ')</sup> &#9660;';
            // Apply the filter
            if (typeof filterPivotTable === 'function') filterPivotTable();
        }}""")
        page.wait_for_timeout(1000)

        # Scroll to section
        section.scroll_into_view_if_needed()
        page.wait_for_timeout(500)

        # Take screenshot of just the section
        section.screenshot(path=SCREENSHOT_PATH)

        browser.close()

    print(f"[Slack] Screenshot saved: {SCREENSHOT_PATH}")
    return SCREENSHOT_PATH


def upload_to_slack(image_path):
    """Upload the screenshot to Slack using Bot Token (files.upload v2)."""
    if not SLACK_BOT_TOKEN:
        print("[Slack] ERROR: SLACK_BOT_TOKEN not set")
        return False

    today = datetime.now().strftime("%Y-%m-%d")

    # Step 1: Get upload URL
    print("[Slack] Requesting upload URL...")
    file_size = os.path.getsize(image_path)
    resp = requests.get(
        "https://slack.com/api/files.getUploadURLExternal",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        params={
            "filename": f"ticket_bifurcation_{today}.png",
            "length": file_size,
        }
    )
    data = resp.json()
    if not data.get("ok"):
        print(f"[Slack] Failed to get upload URL: {data}")
        # Fallback: send text notification via webhook
        send_text_fallback(today)
        return False

    upload_url = data["upload_url"]
    file_id = data["file_id"]

    # Step 2: Upload the file
    print("[Slack] Uploading screenshot...")
    with open(image_path, "rb") as f:
        resp = requests.post(upload_url, files={"file": f})

    if resp.status_code != 200:
        print(f"[Slack] Upload failed: {resp.status_code}")
        send_text_fallback(today)
        return False

    # Step 3: Complete upload and share to channel
    print("[Slack] Sharing to channel...")
    channel_id = SLACK_CHANNEL
    resp = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json",
        },
        json={
            "files": [{"id": file_id, "title": f"Ticket Bifurcation - {today}"}],
            "channel_id": channel_id,
            "initial_comment": f":bar_chart: *PFT Daily Ticket Bifurcation Report — {today}*\n\nView full dashboard: {DASHBOARD_URL}",
        }
    )
    data = resp.json()
    if data.get("ok"):
        print("[Slack] Screenshot posted to Slack successfully!")
        return True
    else:
        print(f"[Slack] Share failed: {data}")
        send_text_fallback(today)
        return False


def send_text_fallback(today):
    """If image upload fails, send a text notification via webhook."""
    if not SLACK_WEBHOOK_URL:
        print("[Slack] No webhook URL for fallback")
        return

    payload = {
        "text": f":bar_chart: *PFT Daily Ticket Bifurcation Report — {today}*\n\nScreenshot upload failed. Please view the dashboard directly:\n:link: {DASHBOARD_URL}\n\n_Auto-generated by PFT Agent_"
    }
    requests.post(SLACK_WEBHOOK_URL, json=payload)
    print("[Slack] Fallback text notification sent")


def send_webhook_notification(status="success"):
    """Send a simple status notification via webhook."""
    today = datetime.now().strftime("%Y-%m-%d")

    if status == "success":
        text = f":white_check_mark: *PFT Daily Update Completed — {today}*\n\n:link: View Dashboard: {DASHBOARD_URL}"
    else:
        text = f":x: *PFT Daily Update Failed — {today}*\n\nPlease run manually via Claude Code."

    if SLACK_WEBHOOK_URL:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text})
        print(f"[Slack] Webhook notification sent: {status}")


if __name__ == "__main__":
    try:
        img_path = take_screenshot()
        if SLACK_BOT_TOKEN and SLACK_CHANNEL:
            upload_to_slack(img_path)
        elif SLACK_WEBHOOK_URL:
            # Can't upload image with just webhook, send link instead
            today = datetime.now().strftime("%Y-%m-%d")
            send_text_fallback(today)
        else:
            print("[Slack] No Slack credentials configured")
    except Exception as e:
        print(f"[Slack] Error: {e}")
        send_webhook_notification("failure")
        sys.exit(1)
