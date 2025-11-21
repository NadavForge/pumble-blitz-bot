import os
import re
import json
from flask import Flask, request, jsonify
import requests
from google_sheet import append_deal, get_leaderboard_for_channel, get_master_leaderboard

app = Flask(__name__)

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_SIGNING_SECRET = os.environ["SLACK_SIGNING_SECRET"]

# ------------------------------------------------------------------------------
# Helper: Send message to Slack
# ------------------------------------------------------------------------------
def slack_send(channel, text):
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"channel": channel, "text": text}
    requests.post(url, headers=headers, json=payload)

# ------------------------------------------------------------------------------
# Helper: Get display name
# ------------------------------------------------------------------------------
def get_display_name(user_id: str) -> str:
    url = "https://slack.com/api/users.info"
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    r = requests.get(url, headers=headers, params={"user": user_id})
    data = r.json()

    if not data.get("ok"):
        return user_id  # fallback

    profile = data["user"]["profile"]
    return profile.get("display_name") or profile.get("real_name") or user_id

# ------------------------------------------------------------------------------
# Deal detection: match ANY of these:
# "1g" "2g" "1G" "2G" "1gb" "2gb" "1gig" "2gig" "1GB" "2GB"
# ------------------------------------------------------------------------------
DEAL_REGEX = re.compile(r"\b([1-9])\s*(g|gb|gig)\b", re.IGNORECASE)

def detect_deals(text: str) -> int:
    match = DEAL_REGEX.search(text.lower())
    if not match:
        return 0
    return int(match.group(1))

# ------------------------------------------------------------------------------
# Only track deals in channels matching: blitz-<market>-deals
# ------------------------------------------------------------------------------
def is_deal_channel(name: str) -> bool:
    name = name.lower()
    return name.startswith("blitz-") and name.endswith("-deals")

# ------------------------------------------------------------------------------
# Slack Event Handler
# ------------------------------------------------------------------------------
@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json

    # URL Verification
    if "challenge" in data:
        return jsonify({"challenge": data["challenge"]})

    if "event" not in data:
        return "OK"

    event = data["event"]

    # Ignore bot messages
    if event.get("subtype") == "bot_message":
        return "OK"

    user = event.get("user")
    text = event.get("text", "")
    channel = event.get("channel")

    # Get channel name
    channel_info = requests.get(
        "https://slack.com/api/conversations.info",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        params={"channel": channel}
    ).json()

    channel_name = channel_info["channel"]["name"]

    # --------------------------
    # COMMAND: leaderboard
    # --------------------------
    if "leaderboard" in text.lower():
        board = get_leaderboard_for_channel(channel_name)
        if not board:
            slack_send(channel, f"No deals logged yet for {channel_name}.")
        else:
            slack_send(channel, board)
        return "OK"

    # --------------------------
    # COMMAND: master leaderboard
    # --------------------------
    if "master leaderboard" in text.lower():
        board = get_master_leaderboard()
        if not board:
            slack_send(channel, "No deals logged yet across all markets.")
        else:
            slack_send(channel, board)
        return "OK"

    # --------------------------
    # DEAL DETECTION
    # --------------------------
    if is_deal_channel(channel_name):
        deals = detect_deals(text)
        if deals > 0:
            display_name = get_display_name(user)
            timestamp = event.get("ts")

            append_deal(display_name, channel_name, deals, timestamp)

    return "OK"

# ------------------------------------------------------------------------------
# Test endpoint – DO NOT DELETE
# ------------------------------------------------------------------------------
@app.route("/")
def home():
    return "ForgeBot Running"

