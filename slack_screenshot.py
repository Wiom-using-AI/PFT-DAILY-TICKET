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
        page = browser.new_page(viewport={"width": 1600, "height": 1000}, device_scale_factor=2)

        page.goto(DASHBOARD_URL, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(5000)

        # Wait for the pivot table to load
        page.locator("#pivotTable").wait_for(state="visible", timeout=30000)

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

        # Expand L4 sub-rows for selected categories
        print("[Slack] Expanding L4 sub-rows...")
        for cat in SELECTED_CATEGORIES:
            try:
                page.evaluate(f"""() => {{
                    if (typeof togglePivotL4 === 'function') {{
                        const rows = document.querySelectorAll('#pivotBody tr');
                        for (const row of rows) {{
                            const td = row.querySelector('td');
                            if (td && td.textContent.includes('{cat}')) {{
                                togglePivotL4('{cat}', row);
                                break;
                            }}
                        }}
                    }}
                }}""")
                page.wait_for_timeout(1500)  # Wait for API response
            except Exception as ex:
                print(f"[Slack] Could not expand L4 for {cat}: {ex}")

        page.wait_for_timeout(1000)

        # Screenshot only the header + pivot table (not the summary/chart below)
        # We select the section header and the pivot table wrapper together
        pivot_area = page.locator("#categorySection .section-header, #pivotTable")

        # Use JavaScript to create a wrapper around just the parts we want
        page.evaluate("""() => {
            const section = document.getElementById('categorySection');
            const header = section.querySelector('.section-header');
            const tableWrapper = document.getElementById('pivotTable').closest('div[style*="overflow"]') || document.getElementById('pivotTable');
            const hint = section.querySelector('p');

            // Create a temporary div with just the header + table
            const tempDiv = document.createElement('div');
            tempDiv.id = 'screenshotArea';
            tempDiv.style.background = 'white';
            tempDiv.style.padding = '32px';
            tempDiv.style.borderRadius = '12px';
            tempDiv.style.border = '1px solid #e2e8f0';
            tempDiv.style.width = '1500px';
            tempDiv.style.fontSize = '16px';

            // Clone and style the header
            const headerClone = header.cloneNode(true);
            headerClone.style.marginBottom = '16px';
            const h3 = headerClone.querySelector('h3');
            if (h3) h3.style.fontSize = '22px';

            // Clone table and make text bigger
            const tableClone = tableWrapper.cloneNode(true);
            const table = tableClone.querySelector('table') || tableClone;
            table.style.fontSize = '15px';
            table.querySelectorAll('th, td').forEach(cell => {
                cell.style.padding = '12px 16px';
            });

            tempDiv.appendChild(headerClone);
            tempDiv.appendChild(tableClone);
            if (hint) {
                const hintClone = hint.cloneNode(true);
                hintClone.style.marginTop = '12px';
                tempDiv.appendChild(hintClone);
            }

            document.body.appendChild(tempDiv);
        }""")
        page.wait_for_timeout(500)

        screenshot_area = page.locator("#screenshotArea")
        screenshot_area.scroll_into_view_if_needed()
        page.wait_for_timeout(300)

        screenshot_area.screenshot(path=SCREENSHOT_PATH)
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
