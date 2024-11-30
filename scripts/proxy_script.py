from flask import Flask, request, jsonify
import requests
import random
import json
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open("instances_ips.json", "r") as f:
    instances_ips = json.load(f)
    manager_ip = instances_ips["manager_ip"]
    worker_ips = instances_ips.get("worker_ips", [])
    
MANAGER_URL = f"http://{manager_ip}:5000"
WORKER_URLS = [f"http://{ip}:5000" for ip in worker_ips]

def get_fastest_server():
    response_times = {}
    servers = WORKER_URLS
    for server in servers:
        try:
            response = requests.get(server, timeout=2)
            response_times[server] = response.elapsed.total_seconds()
        except requests.exceptions.RequestException:
            response_times[server] = float("inf")
    return min(response_times, key=response_times.get)

@app.route("/", methods=["GET"])
def health_check():
    return "Proxy OK", 200

@app.route("/", methods=["POST"])
def process_request():
    data = request.json
    logger.info(f"Received request: {data}")
    request_type = data.get("type", "").lower()
    if request_type == "write":
        try:
            response = requests.post(MANAGER_URL, json=data)
            return jsonify(response.json()), response.status_code
        except requests.exceptions.RequestException as e:
            return jsonify({"error": str(e)}), 500
    elif request_type == "read":
        mode = data.get("mode", "direct_hit")
        target_url = None
        if mode == "direct_hit":
            target_url = WORKER_URLS[0]  # Direct to the first worker
        elif mode == "random":
            target_url = random.choice(WORKER_URLS)
        elif mode == "customized":
            target_url = get_fastest_server()
        if target_url:
            try:
                response = requests.post(target_url, json=data)
                return jsonify(response.json()), response.status_code
            except requests.exceptions.RequestException as e:
                return jsonify({"error": str(e)}), 500
    return jsonify({"error": "Invalid request"}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)