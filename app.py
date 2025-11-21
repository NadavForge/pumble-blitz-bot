import os
from flask import Flask, request, jsonify
import requests
import re
from datetime import datetime
from google_sheet import append_deal, get_leaderboard_for_channel, get_master_leaderboard

app = Flask(__name__)

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_BOT_USER_ID = os.environ.get("SLACK_BOT_USER_ID")  # <-- Add your bot user ID in Render
VERIFY_TOKEN = os.environ.get("SLACK_VERIFY_TOKEN")


# ---------------------------------------------------------
# Send message back to Slack
# ---------------------------------------------------------
def slack_send(channel, text):
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    requests.post(url, headers=headers, json={"channel": channel, "text": text})


# ---------------------------------------------------------
# Parse if message contains a deal (1g, 2g, 1GB, 2gig, etc.)
# ---------------------------------------------------------
DEAL_REGEX = re.compile(r"\b[12]\s*(g|gb|gig)\b", re.IGNORECASE)

def message_has_deal(text: str) -> bool:
    return bool(DEAL_REGEX.search(text))


# ---------------------------------------------------------
# Route – Slack sends events here
# ---------------------------------------------------------
@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json

    # --- URL Verification ---
    if "challenge" in data:
        return jsonify({"challenge": data["challenge"]})

    event = data.get("event", {})
    if not event:
        return "no event", 200

    user = event.get("user")
    text = event.get("text", "")
    channel = event.get("channel")
    subtype = event.get("subtype")

    # ---------------------------------------------------------
    # 1. IGNORE BOT MESSAGES (fixes endless loop)
    # ---------------------------------------------------------
    if subtype == "bot_message":
        return "ignored bot message", 200

    if user == SLACK_BOT_USER_ID:
        return "ignored my own message", 200

    # ---------------------------------------------------------
    # 2. Handle deal logs (ANY valid G pattern = +1 deal)
    # ---------------------------------------------------------
    if message_has_deal(text):
        ts = datetime.utcnow().isoformat()
        append_deal(user_name=user, channel_name=channel, deals=1, timestamp=ts)

    # ---------------------------------------------------------
    # 3. Leaderboard command
    # ---------------------------------------------------------
    if text.lower().strip() == "leaderboard":
        board = get_leaderboard_for_channel(channel)
        if not board:
            board = f"No deals logged yet for <#{channel}>."
        slack_send(channel, board)

    # ---------------------------------------------------------
    # 4. Master leaderboard command
    # ---------------------------------------------------------
    if text.lower().strip() == "master leaderboard":
        board = get_master_leaderboard()
        if not board:
            board = "No deals logged yet across all markets."
        slack_send(channel, board)

    return "ok", 200


@app.route("/")
def home():
    return "ForgeBot is running!"


if __name__ == "__main__":
    app.run()
