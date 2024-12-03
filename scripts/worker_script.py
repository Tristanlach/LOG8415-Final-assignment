from flask import Flask, request, jsonify
import mysql.connector
import logging

app = Flask(__name__)

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "password",
    "database": "sakila",
}

# Logs configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WorkerApp")

@app.route("/", methods=["GET"])
def health_check():
    logger.info("Health check requested")
    return "Worker OK", 200

@app.route("/", methods=["POST"])
def handle_query():
    try:
        data = request.json
        logger.info(f"Request received: {data}")

        if not data or "query" not in data:
            logger.warning("Invalid request format")
            return jsonify({"error": "Invalid request format. Expecting JSON with 'query' key"}), 400

        query = data["query"]
        logger.info(f"Query received : {query}")

        is_write_query = query.strip().lower().startswith(("insert", "update", "delete"))

        # Connection to the database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        if is_write_query:
            # Write query
            cursor.execute(query)
            conn.commit()
            logger.info("Write query executed successfully")
            return jsonify({"message": "Write query executed successfully"}), 200
        else:
            # Reading query
            cursor.execute(query)
            result = cursor.fetchall()
            logger.info("Read query executed successfully")
            return jsonify(result), 200

    except mysql.connector.Error as err:
        logger.error(f"Erreur MySQL: {err}")
        return jsonify({"error": f"MySQL error: {err}"}), 500

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({"error": str(e)}), 500

    finally:
        # Closing the database connection
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            logger.info("Connection to the database closed")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)