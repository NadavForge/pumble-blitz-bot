import os
from collections import defaultdict
from datetime import datetime, timedelta
import gspread
from google.oauth2 import service_account
import pytz

# -----------------------------
# Timezone Config
# -----------------------------
PST = pytz.timezone("America/Los_Angeles")

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

def _get_spreadsheet():
    client = _get_client()
    return client.open_by_key(SPREADSHEET_ID)

def _get_sheet():
    sh = _get_spreadsheet()
    try:
        ws = sh.worksheet("deals")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="deals", rows="2000", cols="10")
        ws.append_row(["timestamp", "user_name", "market", "channel_name", "deals"])
    return ws

# -----------------------------
# Auto-detect market
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
    ws.append_row([timestamp, user_name, market, channel_name, deals])

# -----------------------------
# Load all deal rows
# -----------------------------
def _load_all_deals():
    ws = _get_sheet()
    rows = ws.get_all_records()
    return rows

# -----------------------------
# Time filtering helpers
# -----------------------------
def get_period_start(period: str) -> datetime:
    """
    Get the start datetime for a period in PST.
    - today: midnight PST today
    - week: midnight PST on most recent Monday
    - month: midnight PST on 1st of current month
    """
    now = datetime.now(PST)
    
    if period == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    elif period == "week":
        # Monday = 0, Sunday = 6
        days_since_monday = now.weekday()
        monday = now - timedelta(days=days_since_monday)
        return monday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    elif period == "month":
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Default to all time (return very old date)
    return datetime(2000, 1, 1, tzinfo=PST)

def parse_timestamp(ts_str: str) -> datetime:
    """Parse ISO timestamp string to datetime in PST"""
    try:
        # Handle ISO format with timezone
        if "+" in ts_str or ts_str.endswith("Z"):
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        else:
            # Assume PST if no timezone
            dt = datetime.fromisoformat(ts_str)
            if dt.tzinfo is None:
                dt = PST.localize(dt)
        return dt.astimezone(PST)
    except:
        # If parsing fails, return old date (will be excluded from "today")
        return datetime(2000, 1, 1, tzinfo=PST)

def filter_deals_by_period(rows: list, period: str) -> list:
    """Filter deal rows to only include those within the specified period"""
    period_start = get_period_start(period)
    filtered = []
    
    for row in rows:
        ts_str = row.get("timestamp", "")
        row_time = parse_timestamp(ts_str)
        if row_time >= period_start:
            filtered.append(row)
    
    return filtered

# -----------------------------
# Channel leaderboard (with period filter)
# -----------------------------
def get_channel_leaderboard(channel_name: str, period: str = "today") -> str:
    rows = _load_all_deals()
    rows = filter_deals_by_period(rows, period)
    
    totals = defaultdict(int)
    for row in rows:
        if row.get("channel_name") == channel_name:
            user = row.get("user_name") or "Unknown"
            deals = int(row.get("deals") or 0)
            totals[user] += deals

    if not totals:
        return ""

    sorted_rows = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    
    lines = []
    rank = 1
    total_deals = 0
    for user, deals in sorted_rows:
        lines.append(f"{rank}. {user} — {deals}")
        total_deals += deals
        rank += 1
    
    # Add total count at bottom
    lines.append("─────────────")
    lines.append(f"Total: {total_deals} deals")

    return "\n".join(lines)
# -----------------------------
# Master leaderboard (with period filter)
# -----------------------------
def get_master_leaderboard(period: str = "today") -> str:
    rows = _load_all_deals()
    rows = filter_deals_by_period(rows, period)
    
    # Track totals and market breakdown per user
    totals = defaultdict(int)
    user_markets = defaultdict(lambda: defaultdict(int))
    
    for row in rows:
        user = row.get("user_name") or "Unknown"
        market = row.get("market") or "unknown"
        deals = int(row.get("deals") or 0)
        totals[user] += deals
        user_markets[user][market] += deals

    if not totals:
        return ""

    sorted_rows = sorted(totals.items(), key=lambda x: x[1], reverse=True)

    lines = []
    rank = 1
    total_deals = 0
    for user, deals in sorted_rows:
        # Show market for daily/weekly, not for monthly
        if period in ("today", "week"):
            markets = user_markets[user]
            primary_market = max(markets, key=markets.get).title()
            lines.append(f"{rank}. {user} ({primary_market}) — {deals}")
        else:
            lines.append(f"{rank}. {user} — {deals}")
        total_deals += deals
        rank += 1
    
    # Add total count at bottom
    lines.append("─────────────")
    lines.append(f"Total: {total_deals} deals")

    return "\n".join(lines)

# -----------------------------
# Current Week helpers (for weekly auto-post on Sunday)
# -----------------------------
def get_current_week_date_range():
    """
    Returns (start, end) datetime for current week (Monday 00:00 to now)
    """
    now = datetime.now(PST)
    
    # Find this week's Monday
    days_since_monday = now.weekday()
    this_monday = now - timedelta(days=days_since_monday)
    this_monday = this_monday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    return this_monday, now

def get_master_leaderboard_current_week() -> str:
    """
    Get master leaderboard for the current week (Monday through now)
    Shows each rep's primary market (where they logged most deals)
    """
    rows = _load_all_deals()
    
    this_monday, now = get_current_week_date_range()
    
    # Filter to current week
    filtered = []
    for row in rows:
        ts_str = row.get("timestamp", "")
        row_time = parse_timestamp(ts_str)
        if this_monday <= row_time <= now:
            filtered.append(row)
    
    # Track totals and market breakdown per user
    totals = defaultdict(int)
    user_markets = defaultdict(lambda: defaultdict(int))
    
    for row in filtered:
        user = row.get("user_name") or "Unknown"
        market = row.get("market") or "unknown"
        deals = int(row.get("deals") or 0)
        totals[user] += deals
        user_markets[user][market] += deals

    if not totals:
        return ""

    sorted_rows = sorted(totals.items(), key=lambda x: x[1], reverse=True)

    lines = []
    rank = 1
    for user, deals in sorted_rows:
        # Find primary market (most deals)
        markets = user_markets[user]
        primary_market = max(markets, key=markets.get).title()
        lines.append(f"{rank}. {user} ({primary_market}) — {deals}")
        rank += 1

    return "\n".join(lines)

# -----------------------------
# Monthly Archive & Reset
# -----------------------------
def archive_and_reset_monthly() -> str:
    """
    Archive current deals to a new tab (e.g., 'deals-2025-11')
    and clear the main deals sheet for the new month.
    Returns the name of the archive sheet.
    """
    sh = _get_spreadsheet()
    ws = _get_sheet()
    
    # Create archive sheet name based on previous month
    now = datetime.now(PST)
    # Get last month (handle January edge case)
    if now.month == 1:
        archive_year = now.year - 1
        archive_month = 12
    else:
        archive_year = now.year
        archive_month = now.month - 1
    
    archive_name = f"deals-{archive_year}-{archive_month:02d}"
    
    # Duplicate current sheet as archive
    archive_ws = sh.duplicate_sheet(
        source_sheet_id=ws.id,
        new_sheet_name=archive_name
    )
    
    # Clear main deals sheet (keep header row)
    ws.delete_rows(2, ws.row_count)
    
    return archive_name
