from flask import Flask, request, jsonify
import requests
import json

app = Flask(__name__)

with open("instances_ips.json", "r") as f:
    instance_ips = json.load(f)
    proxy_ip = instance_ips["proxy_ip"]

PROXY_URL = f"http://{proxy_ip}:5000"

@app.route("/", methods=["GET"])
def health_check():
    return "Trusted Host OK", 200

@app.route("/mode", methods=["GET", "POST"])
def process_mode():
    data = request.json if request.method == "POST" else {}
    try:
        if request.method == "GET":
            response = requests.get(f"{PROXY_URL}/mode")
        else:
            response = requests.post(f"{PROXY_URL}/mode", json=data)
        return jsonify(response.json()), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

@app.route("/query", methods=["POST"])
def forward_query():
    data = request.json
    try:
        response = requests.post(PROXY_URL, json=data)
        return jsonify(response.json()), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)