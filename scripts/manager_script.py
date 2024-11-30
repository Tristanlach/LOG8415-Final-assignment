from flask import Flask, request, jsonify
import mysql.connector
import requests
import random
import json

app = Flask(__name__)

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "password",
    "database": "sakila"
}

with open("instances_ips.json", "r") as f:
    worker_ips = json.load(f).get("worker_ips", [])

WORKER_URLS = [f"http://{ip}:5000" for ip in worker_ips]

def get_fastest_worker():
    response_times = {}
    for worker in WORKER_URLS:
        try:
            response = requests.get(worker + "/health", timeout=2)
            response_times[worker] = response.elapsed.total_seconds()
        except requests.exceptions.RequestException:
            response_times[worker] = float('inf')
    return min(response_times, key=response_times.get)

@app.route("/", methods=["GET"])
def health_check():
    return "Manager OK", 200

@app.route("/", methods=["POST"])
def handle_request():
    data = request.json
    if not data or "query" not in data or "type" not in data:
        return jsonify({"error": "Invalid request format"}), 400

    query = data["query"]
    request_type = data["type"].lower()
    mode = data.get("mode", "direct_hit").lower()

    if request_type == "write":
        # Handle write operations locally
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return jsonify({"status": "success"}), 200
        except mysql.connector.Error as err:
            return jsonify({"error": str(err)}), 500
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    elif request_type == "read":
        # Handle read operations by communicating with workers
        if mode == "direct_hit":
            target_worker = WORKER_URLS[0]
        elif mode == "random":
            target_worker = random.choice(WORKER_URLS)
        elif mode == "customized":
            target_worker = get_fastest_worker()
        else:
            return jsonify({"error": "Invalid mode"}), 400

        try:
            response = requests.post(target_worker, json={"query": query})
            return jsonify(response.json()), response.status_code
        except requests.exceptions.RequestException as e:
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"error": "Invalid request type"}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)