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

@app.route("/", methods=["GET"])
def health_check():
    return "Manager OK", 200

@app.route("/", methods=["POST"])
def handle_request():
    try:
        # Read and validate the request data
        data = request.json
        if not data or "query" not in data or "type" not in data:
            return jsonify({"error": "Invalid request format"}), 400

        query = data["query"]
        request_type = data["type"].lower()
        is_write_query = request_type == "write"

        # Connection to the database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        if is_write_query:
            # Local write query
            cursor.execute(query)
            conn.commit()

            # Replication to workers
            replication_errors = []
            for worker_url in WORKER_URLS:
                try:
                    replication_response = requests.post(worker_url, json={"query": query})
                    if replication_response.status_code != 200:
                        replication_errors.append(f"Worker {worker_url} error: {replication_response.text}")
                except requests.exceptions.RequestException as e:
                    replication_errors.append(f"Worker {worker_url} exception: {str(e)}")

            if replication_errors:
                return jsonify({
                    "status": "partial_success",
                    "message": "Local write succeeded, but some workers failed",
                    "errors": replication_errors
                }), 207
            return jsonify({"message": "Write query executed successfully"}), 200
        else:
            # Local read query
            cursor.execute(query)
            result = cursor.fetchall()
            return jsonify(result), 200

    except mysql.connector.Error as err:
        # Error handling for MySQL
        return jsonify({"error": f"MySQL error: {err}"}), 500

    except Exception as e:
        # Error handling for other exceptions
        return jsonify({"error": str(e)}), 500

    finally:
        # Close the database connection
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
