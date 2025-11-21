from flask import Flask, request, jsonify
import requests
import json

SLACK_BOT_TOKEN = "xoxb-9953760733959-9965935131925-6ZzMkqxsNxfGx37DLYpdInCQ"
SLACK_SIGNING_SECRET = "53f81950ca5dcb4a60b70815abd3df81"

app = Flask(__name__)

# ----------------------------------------
# Helper: send message back to Slack
# ----------------------------------------
def send_message(channel, text):
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "channel": channel,
        "text": text
    }
    requests.post(url, headers=headers, json=payload)

# ----------------------------------------
# Event handler (Slack → your bot)
# ----------------------------------------
@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json

    # Respond to Slack URL verification
    if "challenge" in data:
        return jsonify({"challenge": data["challenge"]})

    # Handle message events
    if "event" in data:
        event = data["event"]

        # Ignore bot messages
        if event.get("subtype") == "bot_message":
            return "ok"

        user = event.get("user")
        text = event.get("text")
        channel = event.get("channel")

        # Example: respond when someone types "leaderboard"
        if "leaderboard" in text.lower():
            send_message(channel, "Leaderboard goes here!")

        # Example: track deals like "Nadav 3"
        # You will add your logic here later

    return "ok"

# ----------------------------------------
# Home route
# ----------------------------------------
@app.route("/")
def home():
    return "Slack Bot Running"


if __name__ == "__main__":
    app.run(debug=True)
