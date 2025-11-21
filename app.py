from flask import Flask, request, jsonify
import requests
import os
import re
from datetime import datetime, timezone, timedelta
import pytz

from google_sheet import (
    append_deal,
    get_master_leaderboard,
    get_channel_leaderboard,
    archive_and_reset_monthly
)

# -----------------------------
# Environment Variables
# -----------------------------
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET")

if not SLACK_BOT_TOKEN:
    raise ValueError("SLACK_BOT_TOKEN is missing in environment variables")

# -----------------------------
# Timezone Config
# -----------------------------
PST = pytz.timezone("America/Los_Angeles")

# -----------------------------
# Flask app
# -----------------------------
app = Flask(__name__)

# -----------------------------
# Bot User ID (from environment variable)
# -----------------------------
SLACK_BOT_USER_ID = os.environ.get("SLACK_BOT_USER_ID")

if not SLACK_BOT_USER_ID:
    print("WARNING: SLACK_BOT_USER_ID not set. Bot loop prevention may not work!")
else:
    print(f"Bot User ID loaded: {SLACK_BOT_USER_ID}")

# -----------------------------
# Cache for user & channel lookups
# -----------------------------
USER_CACHE = {}
CHANNEL_CACHE = {}

# -----------------------------
# Helper to call Slack API
# -----------------------------
def slack_api_get(method, params=None):
    """Use GET for info methods (conversations.info, users.info, auth.test)"""
    url = f"https://slack.com/api/{method}"
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    resp = requests.get(url, headers=headers, params=params or {})
    return resp.json()

def slack_api_post(method, payload=None):
    """Use POST for action methods (chat.postMessage)"""
    url = f"https://slack.com/api/{method}"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json; charset=utf-8",
    }
    resp = requests.post(url, headers=headers, json=payload or {})
    return resp.json()

def get_user_name(user_id):
    if user_id in USER_CACHE:
        return USER_CACHE[user_id]

    data = slack_api_get("users.info", {"user": user_id})
    if data.get("ok"):
        profile = data["user"].get("profile", {})
        name = profile.get("display_name") or profile.get("real_name") or user_id
    else:
        print(f"Warning: users.info failed for {user_id}: {data.get('error')}")
        name = user_id

    USER_CACHE[user_id] = name
    return name

def get_channel_name(channel_id):
    if channel_id in CHANNEL_CACHE:
        return CHANNEL_CACHE[channel_id]

    data = slack_api_get("conversations.info", {"channel": channel_id})
    if data.get("ok"):
        name = data["channel"].get("name") or channel_id
    else:
        print(f"Warning: conversations.info failed for {channel_id}: {data.get('error')}")
        name = channel_id

    CHANNEL_CACHE[channel_id] = name
    return name

# -----------------------------
# Helper: send message back into Slack
# -----------------------------
def send_message(channel, text):
    slack_api_post("chat.postMessage", {"channel": channel, "text": text})

# -----------------------------
# Deal detection pattern
# Matches: 1g, 2G, 1gb, 1GB, 1gig, 2gigs, "1g too easy", etc.
# -----------------------------
DEAL_PATTERN = re.compile(r"\b[1-9]\s*(g|gb|gig)s?\b", re.IGNORECASE)

def is_deal_message(text):
    """
    Returns True if text contains a deal-style message.
    Every valid deal message = exactly 1 deal (per spec).
    """
    if not text:
        return False
    return bool(DEAL_PATTERN.search(text))

# -----------------------------
# Parse leaderboard commands
# -----------------------------
def parse_leaderboard_command(text):
    """
    Parse leaderboard commands and return (command_type, period)
    
    Commands:
      leaderboard           -> ("channel", "today")
      leaderboard today     -> ("channel", "today")
      leaderboard week      -> ("channel", "week")
      leaderboard month     -> ("channel", "month")
      master leaderboard    -> ("master", "today")
      master leaderboard today  -> ("master", "today")
      master leaderboard week   -> ("master", "week")
      master leaderboard month  -> ("master", "month")
    """
    lower = text.lower().strip()
    
    # Master leaderboard variants
    if lower.startswith("master leaderboard"):
        remainder = lower.replace("master leaderboard", "").strip()
        if remainder == "week":
            return ("master", "week")
        elif remainder == "month":
            return ("master", "month")
        else:
            return ("master", "today")
    
    # Channel leaderboard variants
    if lower.startswith("leaderboard"):
        remainder = lower.replace("leaderboard", "").strip()
        if remainder == "week":
            return ("channel", "week")
        elif remainder == "month":
            return ("channel", "month")
        else:
            return ("channel", "today")
    
    return (None, None)

def get_period_label(period):
    """Return human-readable label for period"""
    if period == "today":
        return "Today"
    elif period == "week":
        return "This Week"
    elif period == "month":
        return "This Month"
    return ""

# -----------------------------
# ROUTING
# -----------------------------
@app.route("/")
def home():
    return "Slack Bot Running"

@app.route("/slack/events", methods=["GET"])
def slack_events_get():
    return "OK", 200

@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.get_json()
    print("Incoming Slack Event:", data)

    # Slack challenge verification (required for initial setup)
    if "challenge" in data:
        return jsonify({"challenge": data["challenge"]})

    # Process events
    if "event" in data:
        event = data["event"]
        
        # --- BOT LOOP PREVENTION ---
        if event.get("subtype") == "bot_message":
            return "ok", 200
        
        user_id = event.get("user")
        if SLACK_BOT_USER_ID and user_id == SLACK_BOT_USER_ID:
            return "ok", 200
        
        if event.get("bot_id"):
            return "ok", 200

        if event.get("type") != "message":
            return "ok", 200

        text = event.get("text") or ""
        channel_id = event.get("channel")

        channel_name = get_channel_name(channel_id)
        is_deal_channel = channel_name.lower().endswith("-deals")

        # -----------------------------
        # 1) DEAL DETECTION LOGIC
        # -----------------------------
        if is_deal_message(text) and is_deal_channel and user_id:
            user_name = get_user_name(user_id)
            timestamp = datetime.now(PST).isoformat()

            append_deal(
                user_name=user_name,
                channel_name=channel_name,
                deals=1,
                timestamp=timestamp
            )
            print(f"Logged 1 deal for {user_name} in {channel_name}")

        # -----------------------------
        # 2) LEADERBOARD COMMANDS
        # -----------------------------
        command_type, period = parse_leaderboard_command(text)
        
        if command_type == "channel":
            market = channel_name.lower().replace("blitz-", "").replace("-deals", "").title()
            period_label = get_period_label(period)
            
            leaderboard_text = get_channel_leaderboard(channel_name, period)
            if leaderboard_text:
                header = f"*Leaderboard – {market} ({period_label})*\n{leaderboard_text}"
            else:
                header = f"No deals logged yet for {market} ({period_label.lower()})."
            send_message(channel_id, header)
        
        elif command_type == "master":
            period_label = get_period_label(period)
            
            leaderboard_text = get_master_leaderboard(period)
            if leaderboard_text:
                header = f"*Master Leaderboard – All Markets ({period_label})*\n{leaderboard_text}"
            else:
                header = f"No deals logged yet ({period_label.lower()})."
            send_message(channel_id, header)

    return "ok", 200

# -----------------------------
# Debug/Test Routes
# -----------------------------
import traceback

@app.route("/sheet-test")
def sheet_test():
    try:
        from google_sheet import _get_sheet
        ws = _get_sheet()
        return "SUCCESS: Connected to Google Sheets!"
    except Exception as e:
        tb = traceback.format_exc()
        print("SHEET TEST ERROR:", tb)
        return "<pre>ERROR:\n" + tb + "</pre>"

@app.route("/test-creds")
def test_creds():
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not path:
        return "No env var found"
    try:
        with open(path, "r") as f:
            data = f.read()
        return "Loaded secret file successfully! Length: " + str(len(data))
    except Exception as e:
        return "ERROR: " + str(e)

@app.route("/bot-id")
def bot_id_check():
    """Debug route to verify bot user ID was fetched"""
    return f"Bot User ID: {SLACK_BOT_USER_ID or 'NOT SET'}"

# -----------------------------
# Daily Leaderboard Auto-Post
# -----------------------------
DAILY_POST_SECRET = os.environ.get("DAILY_POST_SECRET", "")
LEADERBOARD_CHANNEL_ID = os.environ.get("LEADERBOARD_CHANNEL_ID", "")

@app.route("/daily-leaderboard", methods=["GET", "POST"])
def daily_leaderboard():
    """
    Endpoint for external cron job to trigger daily master leaderboard post.
    Call with ?secret=YOUR_SECRET for security.
    """
    provided_secret = request.args.get("secret", "")
    if not DAILY_POST_SECRET or provided_secret != DAILY_POST_SECRET:
        print("Warning: daily-leaderboard called with invalid or missing secret")
        return "Unauthorized", 401
    
    if not LEADERBOARD_CHANNEL_ID:
        print("Error: LEADERBOARD_CHANNEL_ID not set")
        return "LEADERBOARD_CHANNEL_ID not configured", 500
    
    leaderboard_text = get_master_leaderboard("today")
    if not leaderboard_text:
        leaderboard_text = "No deals logged today."
    
    today = datetime.now(PST).strftime("%B %d, %Y")
    message = f"📊 *Daily Summary — {today}*\n\n*Master Leaderboard – All Markets (Today)*\n{leaderboard_text}"
    
    slack_api_post("chat.postMessage", {
        "channel": LEADERBOARD_CHANNEL_ID,
        "text": message
    })
    
    print(f"Daily leaderboard posted to {LEADERBOARD_CHANNEL_ID}")
    return "OK", 200

# -----------------------------
# Monthly Archive Endpoint
# -----------------------------
ARCHIVE_SECRET = os.environ.get("ARCHIVE_SECRET", "")

# -----------------------------
# Weekly Leaderboard Auto-Post
# -----------------------------
WEEKLY_POST_SECRET = os.environ.get("WEEKLY_POST_SECRET", "")

@app.route("/weekly-leaderboard", methods=["GET", "POST"])
def weekly_leaderboard():
    """
    Endpoint for external cron job to trigger weekly master leaderboard post.
    Posts the CURRENT week's results (Mon through now).
    Call with ?secret=YOUR_SECRET for security.
    Run on Sunday evening.
    """
    provided_secret = request.args.get("secret", "")
    if not WEEKLY_POST_SECRET or provided_secret != WEEKLY_POST_SECRET:
        print("Warning: weekly-leaderboard called with invalid or missing secret")
        return "Unauthorized", 401
    
    if not LEADERBOARD_CHANNEL_ID:
        print("Error: LEADERBOARD_CHANNEL_ID not set")
        return "LEADERBOARD_CHANNEL_ID not configured", 500
    
    from google_sheet import get_master_leaderboard_current_week, get_current_week_date_range
    
    leaderboard_text = get_master_leaderboard_current_week()
    if not leaderboard_text:
        leaderboard_text = "No deals logged this week."
    
    start_date, end_date = get_current_week_date_range()
    date_range = f"{start_date.strftime('%B %d')} – {end_date.strftime('%B %d, %Y')}"
    
    message = f"📊 *Weekly Summary — {date_range}*\n\n*Master Leaderboard – All Markets (This Week)*\n{leaderboard_text}"
    
    slack_api_post("chat.postMessage", {
        "channel": LEADERBOARD_CHANNEL_ID,
        "text": message
    })
    
    print(f"Weekly leaderboard posted to {LEADERBOARD_CHANNEL_ID}")
    return "OK", 200

# -----------------------------
# Monthly Archive Endpoint
# -----------------------------
@app.route("/monthly-archive", methods=["GET", "POST"])
def monthly_archive():
    """
    Endpoint for external cron job to archive and reset monthly data.
    Call with ?secret=YOUR_SECRET for security.
    Run at midnight PST on the 1st of each month.
    """
    provided_secret = request.args.get("secret", "")
    if not ARCHIVE_SECRET or provided_secret != ARCHIVE_SECRET:
        print("Warning: monthly-archive called with invalid or missing secret")
        return "Unauthorized", 401
    
    try:
        archive_name = archive_and_reset_monthly()
        print(f"Monthly archive complete: {archive_name}")
        
        # Optional: post notification to leaderboard channel
        if LEADERBOARD_CHANNEL_ID:
            slack_api_post("chat.postMessage", {
                "channel": LEADERBOARD_CHANNEL_ID,
                "text": f"📁 Monthly data archived to `{archive_name}`. Leaderboards have been reset for the new month!"
            })
        
        return f"Archived to {archive_name}", 200
    except Exception as e:
        print(f"Archive error: {e}")
        return f"Error: {e}", 500

if __name__ == "__main__":
    app.run(debug=True)
