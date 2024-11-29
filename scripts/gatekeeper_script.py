from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

#TODO: IP du Trusted Host
TRUSTED_HOST_URL = "http://<TRUSTED_HOST_IP>:80"

@app.route("/", methods=["POST"])
def validate_and_forward():
    # Validation basique des données
    data = request.json
    if not data or "operation" not in data or "payload" not in data:
        return jsonify({"error": "Invalid request format"}), 400

    # Transmettre les requêtes validées au Trusted Host
    try:
        response = requests.post(TRUSTED_HOST_URL, json=data)
        return jsonify(response.json()), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)

