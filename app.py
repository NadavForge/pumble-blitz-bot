import os
import json
from flask import Flask, request, make_response
from datetime import datetime
from google_sheet import append_deal, get_leaderboard_for_channel, get_master_leaderboard

app = Flask(__name__)

# -------------------------------------------------
# Helper: extract number of Gs from message text
# -------------------------------------------------
def parse_deal_count(text: str) -> int:
    """
    Detect messages like:
    - '1g'
    - '2G'
    - '1 gig'
    - '2gig too easy'
    - '1 g'
    """
    text = text.lower().replace(" ", "")
    if "gig" in text:
        for n in ["1", "2", "3", "4", "5"]:
            if f"{n}gig" in text:
                return int(n)

    if "g" in text:
        for n in ["1", "2", "3", "4", "5"]:
            if f"{n}g" in text:
                return int(n)

    return 0


# -------------------------------------------------
# Helper: get Slack username
# -------------------------------------------------
def get_user_name(user_id):
    """Return fallback to ID for now (optional: expand using Slack API)."""
    return user_id


# -------------------------------------------------
# Slack Event Endpoint
# -------------------------------------------------
@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json

    # Challenge handshake (Slack verification)
    if "challenge" in data:
        return make_response(data["challenge"], 200)

    if data.get("type") != "event_callback":
        return make_response("", 200)

    event = data.get("event", {})
    event_type = event.get("type")

    # Only handle messages
    if event_type != "message" or "text" not in event:
        return make_response("", 200)

    text = event.get("text", "")
    channel = event.get("channel", "")
    user = event.get("user", "")
    timestamp = datetime.utcnow().isoformat()

    # -------------------------------------------------
    # COMMAND: leaderboards
    # -------------------------------------------------
    if text.lower() == "leaderboard":
        leaderboard = get_leaderboard_for_channel(channel)
        if leaderboard:
            send_message(channel, leaderboard)
        else:
            send_message(channel, f"No deals logged yet for {channel}.")
        return make_response("", 200)

    if text.lower() == "master leaderboard":
        leaderboard = get_master_leaderboard()
        if leaderboard:
            send_message(channel, leaderboard)
        else:
            send_message(channel, "No deals logged yet.")
        return make_response("", 200)

    # -------------------------------------------------
    # DEAL DETECTION
    # -------------------------------------------------
    deals = parse_deal_count(text)
    if deals > 0:
        user_name = get_user_name(user)
        append_deal(user_name, channel, deals, timestamp)
        return make_response("", 200)

    return make_response("", 200)


# -------------------------------------------------
# Send message back to Slack
# -------------------------------------------------
import requests

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")

def send_message(channel, text):
    """Post a message to Slack."""
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "channel": channel,
        "text": text
    }
    requests.post(url, headers=headers, data=json.dumps(payload))


# -------------------------------------------------
# Test routes
# -------------------------------------------------
@app.route("/")
def home():
    return "ForgeBot is running!"

@app.route("/test-creds")
def test_creds():
    try:
        path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        with open(path, "r") as f:
            data = f.read()
        return f"Loaded secret file successfully! Length: {len(data)}"
    except Exception as e:
        return f"ERROR:\n{e}"

@app.route("/sheet-test")
def sheet_test():
    try:
        # Quick sheet ping
        append_deal("TEST_USER", "test-channel", 1, datetime.utcnow().isoformat())
        return "Sheet write OK!"
    except Exception as e:
        return f"ERROR:\n{e}"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
