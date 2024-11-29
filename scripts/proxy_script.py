from flask import Flask, request, jsonify
import requests
import random

app = Flask(__name__)

#TODO: IP du Manager et des Workers
MANAGER_URL = "http://{manager_ip}:3306"
WORKER_URLS = {workers_list}

def get_fastest_server():
    response_times = {{}}
    servers = WORKER_URLS
    for server in servers:
        try:
            response = requests.get(server, timeout=2)
            response_times[server] = response.elapsed.total_seconds()
        except requests.exceptions.RequestException:
            response_times[server] = float("inf")
    return min(response_times, key=response_times.get)

@app.route("/", methods=["POST"])
def process_request():
    data = request.json
    request_type = data.get("type", "").lower()
    if request_type == "write":
        return requests.post(MANAGER_URL, json=data).json()
    elif request_type == "read":
        mode = data.get("mode", "direct_hit")
        if mode == "random":
            return requests.post(random.choice(WORKER_URLS), json=data).json()
        elif mode == "customized":
            return requests.post(get_fastest_server(), json=data).json()
    return {{"error": "Invalid request"}}, 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)