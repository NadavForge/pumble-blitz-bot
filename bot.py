from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

PUMBLE_WEBHOOK = "https://api.pumble.com/workspaces/6914e116ca19779dd71ade87/incomingWebhooks/postMessage/AcAmLgwknJcG6Cp7aUgkKZgn"

@app.route("/", methods=["GET"])
def home():
    return "Bot is live!", 200

@app.route("/pumble", methods=["POST"])
def pumble():
    data = request.json
    text = data.get("text", "No message received.")

    # Send message to Pumble
    requests.post(PUMBLE_WEBHOOK, json={"text": text})

    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run()
