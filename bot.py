from flask import Flask, request, jsonify
from google_sheet import add_deal
import re

app = Flask(__name__)

@app.route("/deal", methods=["POST"])
def deal():
    data = request.json

    # Extract info from Pumble slash command payload
    user = data.get("user_name", "Unknown User")
    text = data.get("text", "").strip()
    channel = data.get("channel_name", "Unknown blitz")

    # Accept only "1g" or "2g"
    if text not in ["1g", "2g"]:
        return jsonify({"text": "❌ Invalid deal format. Use: /deal 1g or /deal 2g"})

    # Log to Google Sheets
    add_deal(channel, user, text)

    return jsonify({"text": f"✅ Logged deal: {text} for {user} in {channel}."})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
