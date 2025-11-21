from flask import Flask, request, jsonify
import requests
import json
import os
import re
from datetime import datetime, timezone

from google_sheet import append_deal, get_leaderboard_for_channel, get_master_leaderboard

# Tokens from environment variables (Render)
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET")

if not SLACK_BOT_TOKEN:
    raise ValueError("SLACK_BOT_TOKEN is missing in environment variables")

# -----------------------------
# Flask app
# -----------------------------
app = Flask(__name__)

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
    payload = {
        "channel": channel,
        "text": text,
    }
    requests.post(url, headers=headers, json=payload)

# -----------------------------
# Deal detection pattern
# -----------------------------
DEAL_PATTERN = re.compile(r"\b([1-9])\s*(g|gb|gig)s?\b", re.IGNORECASE)

def extract_deal_count(text):
    """
    Returns int number of deals if text contains deal-style message:
    1g, 2g, 1gb, 1gig, 2G im ramping, etc.
    Otherwise returns 0.
    """
    if not text:
        return 0

    match = DEAL_PATTERN.search(text)
    if not match:
        return 0

    try:
        return int(match.group(1))
    except:
        return 0

# -----------------------------
# ROUTING
# -----------------------------
@app.route("/")
def home():
    return "Slack Bot Running"

# Slack sometimes GET checks the route
@app.route("/slack/events", methods=["GET"])
def slack_events_get():
    return "OK", 200

@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.get_json()

    # Slack challenge verification
    if "challenge" in data:
        return jsonify({"challenge": data["challenge"]})

    # Process events
    if "event" in data:
        event = data["event"]

        # Ignore bot messages
        if event.get("subtype") == "bot_message":
            return "ok", 200

        # Only respond to real messages
        if event.get("type") != "message":
            return "ok", 200

        user_id = event.get("user")
        text = event.get("text") or ""
        channel_id = event.get("channel")

        # Convert IDs into readable names
        channel_name = get_channel_name(channel_id)

        # -----------------------------
        # AUTO-DETECT DEAL CHANNELS (must end with "-deals")
        # -----------------------------
        is_deal_channel = channel_name.lower().endswith("-deals")

        # -----------------------------
        # 1) DEAL DETECTION LOGIC
        # -----------------------------
        deal_count = extract_deal_count(text)

        if deal_count > 0 and is_deal_channel and user_id:
            user_name = get_user_name(user_id)
            timestamp = datetime.now(timezone.utc).isoformat()

            # Log deal to Google Sheets
            append_deal(
                user_name=user_name,
                channel_name=channel_name,
                deals=deal_count,
                timestamp=timestamp
            )

            # Optional confirmation message:
            # send_message(channel_id, f"Logged {deal_count}g for *{user_name}*")

        # -----------------------------
        # 2) LEADERBOARD COMMANDS
        # -----------------------------
        lower = text.lower().strip()

        # Channel-only leaderboard
        if lower == "leaderboard":
            leaderboard_text = get_leaderboard_for_channel(channel_name)
            send_message(channel_id, leaderboard_text or f"No deals logged yet for {channel_name}.")
        
        # Master leaderboard (all blitzes)
        elif lower == "master leaderboard":
            leaderboard_text = get_master_leaderboard()
            send_message(channel_id, leaderboard_text or "No deals logged yet.")

    return "ok", 200


if __name__ == "__main__":
    app.run(debug=True)
