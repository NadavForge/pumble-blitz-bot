from flask import Flask, request, jsonify
import requests
import os
import re
from datetime import datetime, timezone

from google_sheet import append_deal, get_leaderboard_for_channel, get_master_leaderboard

# -----------------------------
# Environment Variables
# -----------------------------
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET")

if not SLACK_BOT_TOKEN:
    raise ValueError("SLACK_BOT_TOKEN is missing in environment variables")

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
def slack_api(method, params=None):
    url = f"https://slack.com/api/{method}"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json; charset=utf-8",
    }
    resp = requests.post(url, headers=headers, json=params or {})
    return resp.json()

def get_user_name(user_id):
    if user_id in USER_CACHE:
        return USER_CACHE[user_id]

    data = slack_api("users.info", {"user": user_id})
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

    data = slack_api("conversations.info", {"channel": channel_id})
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
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"channel": channel, "text": text}
    requests.post(url, headers=headers, json=payload)

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
        # 1. Ignore bot_message subtype
        if event.get("subtype") == "bot_message":
            return "ok", 200
        
        # 2. Ignore messages from this bot's own user ID
        user_id = event.get("user")
        if SLACK_BOT_USER_ID and user_id == SLACK_BOT_USER_ID:
            return "ok", 200
        
        # 3. Ignore if bot_id is present (another safety check)
        if event.get("bot_id"):
            return "ok", 200

        # Only process regular messages
        if event.get("type") != "message":
            return "ok", 200

        text = event.get("text") or ""
        channel_id = event.get("channel")

        # Convert IDs to human-readable names
        channel_name = get_channel_name(channel_id)

        # -----------------------------
        # Only process deal channels (must end with "-deals")
        # -----------------------------
        is_deal_channel = channel_name.lower().endswith("-deals")

        # -----------------------------
        # 1) DEAL DETECTION LOGIC
        # -----------------------------
        if is_deal_message(text) and is_deal_channel and user_id:
            user_name = get_user_name(user_id)
            timestamp = datetime.now(timezone.utc).isoformat()

            # Log exactly 1 deal per valid message
            append_deal(
                user_name=user_name,
                channel_name=channel_name,
                deals=1,  # Always 1 deal per message (per spec)
                timestamp=timestamp
            )
            print(f"Logged 1 deal for {user_name} in {channel_name}")

        # -----------------------------
        # 2) LEADERBOARD COMMANDS
        # -----------------------------
        lower = text.lower().strip()

        if lower == "leaderboard":
            leaderboard_text = get_leaderboard_for_channel(channel_name)
            send_message(channel_id, leaderboard_text or f"No deals logged yet for {channel_name}.")

        elif lower == "master leaderboard":
            leaderboard_text = get_master_leaderboard()
            send_message(channel_id, leaderboard_text or "No deals logged yet.")

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

if __name__ == "__main__":
    app.run(debug=True)
