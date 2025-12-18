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
        ws.append_row(["timestamp", "user_name", "market", "channel_name", "deals", "package_size_gb"])
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
def append_deal(user_name: str, channel_name: str, deals: int, package_size_gb: float, timestamp: str):
    ws = _get_sheet()
    market = extract_market(channel_name)
    ws.append_row([timestamp, user_name, market, channel_name, deals, package_size_gb])

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

# -----------------------------
# Date Range Parsing Helpers
# -----------------------------
def parse_date_input(date_str: str) -> datetime:
    """
    Parse various date formats into datetime (PST).
    Supports:
    - MM/DD (assumes current year)
    - MM/DD/YYYY
    - "november 15" or "nov 15"
    - "december 1 2024"
    """
    date_str = date_str.strip().lower()
    now = datetime.now(PST)
    
    # Try MM/DD or MM/DD/YYYY format
    if "/" in date_str:
        parts = date_str.split("/")
        try:
            month = int(parts[0])
            day = int(parts[1])
            year = int(parts[2]) if len(parts) == 3 else now.year
            return PST.localize(datetime(year, month, day, 0, 0, 0))
        except (ValueError, IndexError):
            raise ValueError(f"Invalid date format: {date_str}")
    
    # Try month name format (e.g., "november 15" or "nov 15 2024")
    month_names = {
        'january': 1, 'jan': 1,
        'february': 2, 'feb': 2,
        'march': 3, 'mar': 3,
        'april': 4, 'apr': 4,
        'may': 5,
        'june': 6, 'jun': 6,
        'july': 7, 'jul': 7,
        'august': 8, 'aug': 8,
        'september': 9, 'sep': 9, 'sept': 9,
        'october': 10, 'oct': 10,
        'november': 11, 'nov': 11,
        'december': 12, 'dec': 12
    }
    
    parts = date_str.split()
    if len(parts) >= 2:
        month_str = parts[0]
        if month_str in month_names:
            try:
                month = month_names[month_str]
                day = int(parts[1])
                year = int(parts[2]) if len(parts) >= 3 else now.year
                return PST.localize(datetime(year, month, day, 0, 0, 0))
            except (ValueError, IndexError):
                pass
    
    raise ValueError(f"Could not parse date: {date_str}")

def parse_date_range(range_str: str) -> tuple:
    """
    Parse date range string like "12/1 to 12/15" or "november 1 to november 15"
    Returns (start_datetime, end_datetime) in PST
    """
    range_str = range_str.lower().strip()
    
    # Split on " to "
    if " to " not in range_str:
        raise ValueError("Date range must use format: [start date] to [end date]")
    
    parts = range_str.split(" to ")
    if len(parts) != 2:
        raise ValueError("Date range must use format: [start date] to [end date]")
    
    start_date = parse_date_input(parts[0])
    end_date = parse_date_input(parts[1])
    
    # Set end_date to end of day (23:59:59)
    end_date = end_date.replace(hour=23, minute=59, second=59)
    
    if start_date > end_date:
        raise ValueError("Start date must be before end date")
    
    return start_date, end_date

def format_date_range_label(start_date: datetime, end_date: datetime) -> str:
    """
    Format a date range for display in leaderboard headers.
    Examples:
    - Same day: "December 17, 2024"
    - Same month: "Dec 1-15, 2024"
    - Different months: "Nov 25 - Dec 5, 2024"
    - Different years: "Dec 20, 2024 - Jan 5, 2025"
    """
    if start_date.date() == end_date.date():
        # Same day
        return start_date.strftime("%B %d, %Y")
    
    if start_date.year == end_date.year:
        if start_date.month == end_date.month:
            # Same month
            return f"{start_date.strftime('%b')} {start_date.day}-{end_date.day}, {start_date.year}"
        else:
            # Different months, same year
            return f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
    else:
        # Different years
        return f"{start_date.strftime('%b %d, %Y')} - {end_date.strftime('%b %d, %Y')}"

# -----------------------------
# Archive Sheet Access
# -----------------------------
def _get_archived_sheet(year: int, month: int):
    """
    Get an archived deals sheet by year and month.
    Returns worksheet object or None if not found.
    """
    sh = _get_spreadsheet()
    archive_name = f"deals-{year}-{month:02d}"
    try:
        return sh.worksheet(archive_name)
    except gspread.WorksheetNotFound:
        return None

def _load_deals_from_date_range(start_date: datetime, end_date: datetime) -> list:
    """
    Load all deals within a date range, querying archived sheets as needed.
    Returns list of deal records.
    """
    all_deals = []
    
    # Determine which months we need to query
    current_month_start = datetime.now(PST).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # If range is entirely in current month, just query main sheet
    if start_date >= current_month_start:
        ws = _get_sheet()
        rows = ws.get_all_records()
        all_deals.extend(rows)
    else:
        # Need to query archived sheets
        # Generate list of (year, month) tuples to query
        months_to_query = []
        current = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        while current <= end_date:
            months_to_query.append((current.year, current.month))
            # Move to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        
        # Query each month's sheet
        for year, month in months_to_query:
            if year == current_month_start.year and month == current_month_start.month:
                # Current month - use main sheet
                ws = _get_sheet()
                rows = ws.get_all_records()
                all_deals.extend(rows)
            else:
                # Archived month
                ws = _get_archived_sheet(year, month)
                if ws:
                    rows = ws.get_all_records()
                    all_deals.extend(rows)
    
    # Filter to exact date range
    filtered = []
    for row in all_deals:
        ts_str = row.get("timestamp", "")
        row_time = parse_timestamp(ts_str)
        if start_date <= row_time <= end_date:
            filtered.append(row)
    
    return filtered

def get_period_start_end(period: str) -> tuple:
    """
    Get the start and end datetime for a period in PST.
    Returns (start_datetime, end_datetime)
    
    Periods:
    - today: midnight today to now
    - yesterday: midnight yesterday to 23:59:59 yesterday
    - week: midnight Monday to now
    - last week: midnight previous Monday to Sunday 23:59:59
    - month: midnight 1st of month to now
    - last month: midnight 1st of previous month to last day 23:59:59
    """
    now = datetime.now(PST)
    
    if period == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, now
    
    elif period == "yesterday":
        yesterday = now - timedelta(days=1)
        start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start, end
    
    elif period == "week":
        # Current week: Monday to now
        days_since_monday = now.weekday()
        monday = now - timedelta(days=days_since_monday)
        start = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, now
    
    elif period == "last week":
        # Previous complete week: Monday to Sunday
        days_since_monday = now.weekday()
        this_monday = now - timedelta(days=days_since_monday)
        last_monday = this_monday - timedelta(days=7)
        last_sunday = this_monday - timedelta(days=1)
        start = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = last_sunday.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start, end
    
    elif period == "month":
        # Current month: 1st to now
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return start, now
    
    elif period == "last month":
        # Previous complete month
        first_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day_of_last_month = first_of_this_month - timedelta(days=1)
        first_of_last_month = last_day_of_last_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = last_day_of_last_month.replace(hour=23, minute=59, second=59, microsecond=999999)
        return first_of_last_month, end
    
    # Default to all time
    return datetime(2000, 1, 1, tzinfo=PST), now

def get_period_label(period: str, start_date: datetime = None, end_date: datetime = None) -> str:
    """
    Return human-readable label for period.
    If start_date and end_date provided, format them nicely.
    """
    if start_date and end_date:
        return format_date_range_label(start_date, end_date)
    
    if period == "today":
        return "Today"
    elif period == "yesterday":
        return "Yesterday"
    elif period == "week":
        return "This Week"
    elif period == "last week":
        start, end = get_period_start_end("last week")
        return format_date_range_label(start, end)
    elif period == "month":
        return "This Month"
    elif period == "last month":
        start, end = get_period_start_end("last month")
        return start.strftime("%B %Y")
    
    return ""

# -----------------------------
# Channel leaderboard (with period filter)
# -----------------------------
# -----------------------------
# Channel leaderboard (with period or date range)
# -----------------------------
def get_channel_leaderboard(channel_name: str, period: str = "today", date_range: tuple = None) -> tuple:
    """
    Get channel leaderboard for a period or custom date range.
    Returns (leaderboard_text, period_label)
    """
    if date_range:
        start_date, end_date = date_range
        rows = _load_deals_from_date_range(start_date, end_date)
        period_label = format_date_range_label(start_date, end_date)
    else:
        start_date, end_date = get_period_start_end(period)
        rows = _load_deals_from_date_range(start_date, end_date)
        period_label = get_period_label(period, start_date, end_date)
    
    totals = defaultdict(int)
    for row in rows:
        if row.get("channel_name") == channel_name:
            user = row.get("user_name") or "Unknown"
            deals = int(row.get("deals") or 0)
            totals[user] += deals

    if not totals:
        return "", period_label

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
    lines.append(f"Total: {total_deals}")

    return "\n".join(lines), period_label
# -----------------------------
# Master leaderboard (with period filter)
# -----------------------------
# -----------------------------
# Master leaderboard (with period or date range)
# -----------------------------
def get_master_leaderboard(period: str = "today", date_range: tuple = None) -> tuple:
    """
    Get master leaderboard for a period or custom date range.
    Returns (leaderboard_text, period_label)
    """
    if date_range:
        start_date, end_date = date_range
        rows = _load_deals_from_date_range(start_date, end_date)
        period_label = format_date_range_label(start_date, end_date)
    else:
        start_date, end_date = get_period_start_end(period)
        rows = _load_deals_from_date_range(start_date, end_date)
        period_label = get_period_label(period, start_date, end_date)
    
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
        return "", period_label

    sorted_rows = sorted(totals.items(), key=lambda x: x[1], reverse=True)

    lines = []
    rank = 1
    total_deals = 0
    for user, deals in sorted_rows:
        # Show market for daily/weekly/custom ranges, not for monthly
        if period in ("today", "yesterday", "week", "last week") or date_range:
            markets = user_markets[user]
            primary_market = max(markets, key=markets.get).title()
            lines.append(f"{rank}. {user} ({primary_market}) — {deals}")
        else:
            lines.append(f"{rank}. {user} — {deals}")
        total_deals += deals
        rank += 1
    
    # Add total count at bottom
    lines.append("─────────────")
    lines.append(f"Total: {total_deals}")

    return "\n".join(lines), period_label
    
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
# Deal Removal Functions
# -----------------------------
def _get_deletions_sheet():
    """Get or create the deletions audit log sheet"""
    sh = _get_spreadsheet()
    try:
        ws = sh.worksheet("deletions")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="deletions", rows="1000", cols="10")
        ws.append_row(["deletion_timestamp", "user_name", "original_timestamp", "market", "channel_name", "deals", "package_size_gb"])
    return ws

def remove_last_deal(user_name: str, channel_name: str) -> tuple:
    """
    Remove the most recent deal for a user from today only.
    
    Args:
        user_name: Name of the user
        channel_name: Channel where deal was logged
    
    Returns:
        (success: bool, message: str, deals_removed: int, gb_removed: float)
    """
    ws = _get_sheet()
    deletions_ws = _get_deletions_sheet()
    
    # Get all records
    all_records = ws.get_all_records()
    
    # Get today's date range in PST
    now = datetime.now(PST)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Filter to user's deals from today in this channel
    user_deals_today = []
    for idx, row in enumerate(all_records, start=2):  # start=2 because row 1 is header
        if row.get("user_name") != user_name:
            continue
        if row.get("channel_name") != channel_name:
            continue
        
        ts_str = row.get("timestamp", "")
        row_time = parse_timestamp(ts_str)
        
        if row_time < today_start:
            continue
        
        user_deals_today.append((idx, row))
    
    if not user_deals_today:
        return (False, "❌ No deals found to remove from today", 0, 0)
    
    # Get the most recent deal (last in the list)
    row_idx, deal_to_remove = user_deals_today[-1]
    
    # Log to deletions sheet
    deletion_timestamp = datetime.now(PST).isoformat()
    deletions_ws.append_row([
        deletion_timestamp,
        deal_to_remove.get("user_name"),
        deal_to_remove.get("timestamp"),
        deal_to_remove.get("market"),
        deal_to_remove.get("channel_name"),
        deal_to_remove.get("deals"),
        deal_to_remove.get("package_size_gb")
    ])
    
    # Delete the row from main sheet
    ws.delete_rows(row_idx)
    
    deals_count = int(deal_to_remove.get("deals", 1))

# Handle empty or missing package_size_gb
gb_value = deal_to_remove.get("package_size_gb", 0)
if gb_value == "" or gb_value is None:
    gb_size = 0.0
else:
    try:
        gb_size = float(gb_value)
    except (ValueError, TypeError):
        gb_size = 0.0
    
    return (True, "", deals_count, gb_size)
    
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
