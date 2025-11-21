import os
from collections import defaultdict
import gspread
from google.oauth2 import service_account

# -----------------------------
# Google Sheet Config
# -----------------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

SPREADSHEET_ID = os.environ.get("GOOGLE_SHEET_ID")

if not SPREADSHEET_ID:
    raise ValueError("GOOGLE_SHEET_ID environment variable is not set.")

# -----------------------------
# Google Sheet connection helpers
# -----------------------------
def _get_client():
    creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client

def _get_sheet():
    client = _get_client()
    sh = client.open_by_key(SPREADSHEET_ID)

    try:
        ws = sh.worksheet("deals")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="deals", rows="2000", cols="10")
        ws.append_row(["timestamp", "user_name", "market", "channel_name", "deals"])
    
    return ws

# -----------------------------
# Auto-detect market
# Example: "blitz-arkansas-deals" → "arkansas"
# -----------------------------
def extract_market(channel_name: str) -> str:
    if not channel_name:
        return "unknown"

    parts = channel_name.lower().split("-")
    if len(parts) >= 3 and parts[0] == "blitz" and parts[-1] == "deals":
        return parts[1]
    return "unknown"

# -----------------------------
# Log deals
# -----------------------------
def append_deal(user_name: str, channel_name: str, deals: int, timestamp: str):
    ws = _get_sheet()
    market = extract_market(channel_name)

    ws.append_row([
        timestamp,
        user_name,
        market,
        channel_name,
        deals
    ])

# -----------------------------
# Load all deal rows
# -----------------------------
def _load_all_deals():
    ws = _get_sheet()
    rows = ws.get_all_records()
    return rows

# -----------------------------
# Master leaderboard across all markets
# -----------------------------
def get_master_leaderboard() -> str:
    rows = _load_all_deals()
    totals = defaultdict(int)

    for row in rows:
        user = row.get("user_name") or "Unknown"
        deals = int(row.get("deals") or 0)
        totals[user] += deals

    if not totals:
        return ""

    sorted_rows = sorted(totals.items(), key=lambda x: x[1], reverse=True)

    lines = ["*Master Leaderboard – All Markets*"]
    rank = 1
    for user, deals in sorted_rows:
        lines.append(f"{rank}. {user} — {deals}")
        rank += 1

    return "\n".join(lines)
