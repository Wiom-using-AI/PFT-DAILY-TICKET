"""
Take a screenshot of the Ticket Bifurcation section and post to Slack.
Uses the new files.getUploadURLExternal + completeUploadExternal API.
"""
import os
import sys
import json
import requests
from datetime import datetime

# --- Config ---
DASHBOARD_URL = "https://pft-daily-ticket.vercel.app"
SECTION_ID = "categorySection"
SCREENSHOT_PATH = "ticket_bifurcation.png"

SELECTED_CATEGORIES = [
    "Internet Issues",
    "Others",
    "Shifting Request",
    "Partner Misbehavior",
]

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "")


def take_screenshot():
    """Use Playwright to screenshot the Ticket Bifurcation section."""
    from playwright.sync_api import sync_playwright

    print("[Slack] Opening dashboard for screenshot...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1400, "height": 900})

        page.goto(DASHBOARD_URL, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(5000)

        section = page.locator(f"#{SECTION_ID}")
        section.wait_for(state="visible", timeout=30000)

        # Apply filter
        print(f"[Slack] Applying filter: {SELECTED_CATEGORIES}")
        selected_json = json.dumps(SELECTED_CATEGORIES)
        page.evaluate(f"""() => {{
            const selectedCats = {selected_json};
            document.querySelectorAll('#pivotDropdown input[data-cat]').forEach(cb => {{
                cb.checked = false;
            }});
            document.querySelectorAll('#pivotDropdown input[data-cat]').forEach(cb => {{
                const catName = cb.getAttribute('data-cat');
                if (selectedCats.includes(catName)) {{
                    cb.checked = true;
                }}
            }});
            const total = document.querySelectorAll('#pivotDropdown input[data-cat]').length;
            const checked = document.querySelectorAll('#pivotDropdown input[data-cat]:checked').length;
            const btn = document.querySelector('#pivotFilterBtn');
            if (btn) btn.innerHTML = '&#9776; Filter Categories <sup>(' + checked + '/' + total + ')</sup> &#9660;';
            if (typeof filterPivotTable === 'function') filterPivotTable();
        }}""")
        page.wait_for_timeout(1000)

        section.scroll_into_view_if_needed()
        page.wait_for_timeout(500)

        section.screenshot(path=SCREENSHOT_PATH)
        browser.close()

    print(f"[Slack] Screenshot saved: {SCREENSHOT_PATH}")
    return SCREENSHOT_PATH


def post_message_to_slack(text):
    """Post a text message to Slack channel using chat.postMessage."""
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json",
    }
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers=headers,
        json={"channel": SLACK_CHANNEL, "text": text}
    )
    data = resp.json()
    if data.get("ok"):
        print("[Slack] Message posted successfully!")
        return True
    else:
        print(f"[Slack] chat.postMessage failed: {data.get('error')}")
        return False


def upload_to_slack(image_path):
    """Upload screenshot using the new Slack files API."""
    today = datetime.now().strftime("%Y-%m-%d")
    message = f"<!here> Please review the pending tickets as of today that are currently under action by the PFT team.\n\n:bar_chart: *PFT Daily Ticket Bifurcation Report — {today}*\n\nView full dashboard: {DASHBOARD_URL}"

    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    file_size = os.path.getsize(image_path)
    filename = f"ticket_bifurcation_{today}.png"

    # Step 1: Get upload URL
    print("[Slack] Step 1: Getting upload URL...")
    resp = requests.get(
        "https://slack.com/api/files.getUploadURLExternal",
        headers=headers,
        params={"filename": filename, "length": file_size}
    )
    data = resp.json()
    if not data.get("ok"):
        print(f"[Slack] getUploadURL failed: {data}")
        return post_message_to_slack(message + "\n\n_(Screenshot upload failed. Please check the dashboard link above.)_")

    upload_url = data["upload_url"]
    file_id = data["file_id"]
    print(f"[Slack] Got upload URL, file_id: {file_id}")

    # Step 2: Upload the file content
    print("[Slack] Step 2: Uploading file...")
    with open(image_path, "rb") as f:
        resp = requests.post(upload_url, files={"file": f})

    if resp.status_code != 200:
        print(f"[Slack] File upload failed: {resp.status_code}")
        return post_message_to_slack(message + "\n\n_(Screenshot upload failed.)_")

    print("[Slack] File uploaded successfully")

    # Step 3: Complete upload - share to channel with message
    print(f"[Slack] Step 3: Sharing to channel {SLACK_CHANNEL}...")
    resp = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "files": [{"id": file_id, "title": f"Ticket Bifurcation - {today}"}],
            "channel_id": SLACK_CHANNEL,
            "initial_comment": message,
        }
    )
    data = resp.json()
    print(f"[Slack] completeUpload response: ok={data.get('ok')}, error={data.get('error', 'none')}")

    if data.get("ok"):
        print("[Slack] Screenshot posted to Slack successfully!")
        return True
    else:
        print(f"[Slack] completeUpload failed: {data}")
        # Fallback: post text message with dashboard link
        return post_message_to_slack(message + "\n\n_(Screenshot upload failed. Please check the dashboard link above.)_")


if __name__ == "__main__":
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL:
        print("[Slack] ERROR: SLACK_BOT_TOKEN or SLACK_CHANNEL not set")
        sys.exit(1)

    try:
        img_path = take_screenshot()
        success = upload_to_slack(img_path)
        if not success:
            print("[Slack] All upload methods failed")
    except Exception as e:
        print(f"[Slack] Error: {e}")
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            post_message_to_slack(f":x: *PFT Daily Update Failed — {today}*\nPlease run manually via Claude Code.")
        except:
            pass
        sys.exit(1)
