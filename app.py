from flask import Flask, request, jsonify
import requests
import os
import re
from collections import deque
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
# Deduplication cache for preventing duplicate deal logs
# -----------------------------
RECENT_MESSAGES = deque(maxlen=200)  # Keep last 200 messages in memory

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
    Parse leaderboard commands and return (command_type, period, date_range)
    
    Commands:
      leaderboard                    -> ("channel", "today", None)
      leaderboard today              -> ("channel", "today", None)
      leaderboard yesterday          -> ("channel", "yesterday", None)
      leaderboard week               -> ("channel", "week", None)
      leaderboard last week          -> ("channel", "last week", None)
      leaderboard month              -> ("channel", "month", None)
      leaderboard last month         -> ("channel", "last month", None)
      leaderboard 12/1 to 12/15      -> ("channel", None, "12/1 to 12/15")
      
      master leaderboard             -> ("master", "today", None)
      master leaderboard yesterday   -> ("master", "yesterday", None)
      master leaderboard last week   -> ("master", "last week", None)
      master leaderboard last month  -> ("master", "last month", None)
      master leaderboard 12/1 to 12/15 -> ("master", None, "12/1 to 12/15")
    """
    lower = text.lower().strip()
    
    # Master leaderboard variants
    if lower.startswith("master leaderboard"):
        remainder = lower.replace("master leaderboard", "").strip()
        
        # Check for date range (contains "to")
        if " to " in remainder:
            return ("master", None, remainder)
        
        # Check for named periods
        if remainder == "yesterday":
            return ("master", "yesterday", None)
        elif remainder == "week":
            return ("master", "week", None)
        elif remainder == "last week":
            return ("master", "last week", None)
        elif remainder == "month":
            return ("master", "month", None)
        elif remainder == "last month":
            return ("master", "last month", None)
        elif remainder == "today" or remainder == "":
            return ("master", "today", None)
        else:
            # Unknown period - return None to trigger error message
            return (None, None, None)
    
    # Channel leaderboard variants
    if lower.startswith("leaderboard"):
        remainder = lower.replace("leaderboard", "").strip()
        
        # Check for date range (contains "to")
        if " to " in remainder:
            return ("channel", None, remainder)
        
        # Check for named periods
        if remainder == "yesterday":
            return ("channel", "yesterday", None)
        elif remainder == "week":
            return ("channel", "week", None)
        elif remainder == "last week":
            return ("channel", "last week", None)
        elif remainder == "month":
            return ("channel", "month", None)
        elif remainder == "last month":
            return ("channel", "last month", None)
        elif remainder == "today" or remainder == "":
            return ("channel", "today", None)
        else:
            # Unknown period - return None to trigger error message
            return (None, None, None)
    
    return (None, None, None)

# -----------------------------
# ROUTING
# -----------------------------
@app.route("/")
def home():
    return "Slack Bot Running"

@app.route("/keep-alive", methods=["GET"])
def keep_alive():
    """
    Simple endpoint for external monitoring services (cron-job.org).
    Returns 200 OK to confirm the service is running.
    """
    return "✅ ForgeBot is alive and running!", 200

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
        # DEDUPLICATION CHECK
        # -----------------------------
        message_ts = event.get("ts")  # Slack's unique timestamp for this message
        message_id = (user_id, message_ts, channel_id)
        
        if message_id in RECENT_MESSAGES:
            user_name = get_user_name(user_id)
            print(f"⚠️ DUPLICATE DETECTED - User: {user_name}, Channel: {channel_name}, Timestamp: {message_ts}")
            return "ok", 200
        
        # Add to cache to prevent future duplicates
        RECENT_MESSAGES.append(message_id)
        
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
            send_message(channel_id, f"✅ Deal logged for {user_name}!")

        # -----------------------------
        # 2) LEADERBOARD COMMANDS
        # -----------------------------
        command_type, period, date_range_str = parse_leaderboard_command(text)
        
        if command_type == "channel":
            from google_sheet import get_channel_leaderboard, parse_date_range
            
            market = channel_name.lower().replace("blitz-", "").replace("-deals", "").title()
            
            try:
                # Handle date range or period
                if date_range_str:
                    date_range = parse_date_range(date_range_str)
                    leaderboard_text, period_label = get_channel_leaderboard(channel_name, date_range=date_range)
                else:
                    leaderboard_text, period_label = get_channel_leaderboard(channel_name, period)
                
                if leaderboard_text:
                    header = f"*Leaderboard – {market} ({period_label})*\n{leaderboard_text}"
                else:
                    header = f"No deals logged yet for {market} ({period_label.lower()})."
                send_message(channel_id, header)
                
            except ValueError as e:
                # Date parsing error
                error_msg = f"❌ Invalid date format: {str(e)}\n\nSupported formats:\n• `leaderboard yesterday`\n• `leaderboard last week`\n• `leaderboard last month`\n• `leaderboard 12/1 to 12/15`\n• `leaderboard november 1 to november 15`"
                send_message(channel_id, error_msg)
        
        elif command_type == "master":
            from google_sheet import get_master_leaderboard, parse_date_range
            
            try:
                # Handle date range or period
                if date_range_str:
                    date_range = parse_date_range(date_range_str)
                    leaderboard_text, period_label = get_master_leaderboard(date_range=date_range)
                else:
                    leaderboard_text, period_label = get_master_leaderboard(period)
                
                if leaderboard_text:
                    header = f"*Master Leaderboard – All Markets ({period_label})*\n{leaderboard_text}"
                else:
                    header = f"No deals logged yet ({period_label.lower()})."
                send_message(channel_id, header)
                
            except ValueError as e:
                # Date parsing error
                error_msg = f"❌ Invalid date format: {str(e)}\n\nSupported formats:\n• `master leaderboard yesterday`\n• `master leaderboard last week`\n• `master leaderboard last month`\n• `master leaderboard 12/1 to 12/15`\n• `master leaderboard november 1 to november 15`"
                send_message(channel_id, error_msg)
        
        elif command_type is None and (text.lower().strip().startswith("leaderboard") or text.lower().strip().startswith("master leaderboard")):
            # User typed a leaderboard command but with invalid syntax
            error_msg = f"❌ I didn't understand that leaderboard command.\n\nSupported commands:\n• `leaderboard` or `leaderboard today`\n• `leaderboard yesterday`\n• `leaderboard week` or `leaderboard last week`\n• `leaderboard month` or `leaderboard last month`\n• `leaderboard 12/1 to 12/15`\n\nSame formats work with `master leaderboard`"
            send_message(channel_id, error_msg)

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

@app.route("/debug-channels")
def debug_channels():
    """Debug endpoint to test channel listing"""
    try:
        result = slack_api_get("conversations.list", {
            "types": "public_channel",
            "limit": 100
        })
        
        if not result.get("ok"):
            return f"Error: {result.get('error')}", 500
        
        all_channels = result.get("channels", [])
        channel_names = [ch.get("name") for ch in all_channels]
        
        blitz_channels = [name for name in channel_names if name.startswith("blitz-") and name.endswith("-deals")]
        
        return f"Found {len(all_channels)} total channels<br>Blitz channels: {', '.join(blitz_channels)}", 200
    except Exception as e:
        return f"Exception: {str(e)}", 500

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
    
    leaderboard_text, period_label = get_master_leaderboard("today")
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
# Nightly Reminder to All Blitz Channels
# -----------------------------
REMINDER_SECRET = os.environ.get("REMINDER_SECRET", "")
# Comma-separated list of channel IDs to send reminders to
REMINDER_CHANNEL_IDS = os.environ.get("REMINDER_CHANNEL_IDS", "")

@app.route("/nightly-reminder", methods=["GET", "POST"])
def nightly_reminder():
    """
    Endpoint for external cron job to send nightly reminders to all blitz-*-deals channels.
    Automatically discovers channels matching the pattern.
    Call with ?secret=YOUR_SECRET for security.
    Run at 8 PM EST daily.
    """
    provided_secret = request.args.get("secret", "")
    if not REMINDER_SECRET or provided_secret != REMINDER_SECRET:
        print("Warning: nightly-reminder called with invalid or missing secret")
        return "Unauthorized", 401
    
    try:
        # Get all channels the bot has access to
        result = slack_api_get("conversations.list", {
            "types": "public_channel",
            "limit": 1000
        })
        
        if not result.get("ok"):
            print(f"Error fetching channels: {result.get('error')}")
            return f"Error: {result.get('error')}", 500
        
        all_channels = result.get("channels", [])
        
        # Filter for blitz-*-deals channels
        blitz_channels = [
            ch for ch in all_channels 
            if ch.get("name", "").startswith("blitz-") and ch.get("name", "").endswith("-deals")
        ]
        
        if not blitz_channels:
            print("No blitz-*-deals channels found")
            return "No matching channels found", 404
        
        print(f"Found {len(blitz_channels)} blitz-*-deals channels")
        
        # Post reminder to each channel
        reminder_message = "⏰ Reminder to fill out the daily form and spreadsheet with today's deals!"
        
        posted_count = 0
        failed_channels = []
        
        for channel in blitz_channels:
            channel_id = channel["id"]
            channel_name = channel["name"]
            
            try:
                response = slack_api_post("chat.postMessage", {
                    "channel": channel_id,
                    "text": reminder_message
                })
                
                if response.get("ok"):
                    print(f"✅ Posted reminder to #{channel_name} ({channel_id})")
                    posted_count += 1
                else:
                    error = response.get('error')
                    print(f"❌ Failed to post to #{channel_name}: {error}")
                    failed_channels.append(f"{channel_name} ({error})")
                    
            except Exception as e:
                print(f"❌ Exception posting to #{channel_name}: {e}")
                failed_channels.append(f"{channel_name} (exception)")
        
        result_msg = f"Posted to {posted_count}/{len(blitz_channels)} channels"
        if failed_channels:
            result_msg += f". Failed: {', '.join(failed_channels)}"
        
        return result_msg, 200
        
    except Exception as e:
        print(f"Error in nightly_reminder: {e}")
        return f"Error: {e}", 500

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
