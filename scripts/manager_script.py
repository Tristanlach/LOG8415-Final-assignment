from flask import Flask, request, jsonify
import mysql.connector

app = Flask(__name__)

#TODO: Configuration de la base de données
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "yourpassword",
    "database": "sakila"
}

@app.route("/", methods=["POST"])
def handle_write():
    data = request.json
    if not data or "query" not in data:
        return jsonify({"error": "Invalid request format"}), 400

    query = data["query"]
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3306)
