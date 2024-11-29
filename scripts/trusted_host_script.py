from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

#TODO: IP du Proxy
PROXY_URL = "http://<PROXY_IP>:80"

@app.route("/", methods=["POST"])
def forward_to_proxy():
    # Recevoir et transmettre les requêtes au Proxy
    data = request.json
    try:
        response = requests.post(PROXY_URL, json=data)
        return jsonify(response.json()), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
