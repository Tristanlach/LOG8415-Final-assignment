from flask import Flask, request, jsonify
import mysql.connector
import logging

app = Flask(__name__)

# Configuration de la base de données
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "password",  # Remplacez par votre mot de passe MySQL
    "database": "sakila",
}

# Configuration des logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WorkerApp")

@app.route("/", methods=["GET"])
def health_check():
    logger.info("Health check requested")
    return "Worker OK", 200

@app.route("/", methods=["POST"])
def handle_query():
    try:
        # Lecture des données de la requête
        data = request.json
        logger.info(f"Requête reçue: {data}")

        # Validation des données
        if not data or "query" not in data:
            logger.warning("Format de requête invalide")
            return jsonify({"error": "Invalid request format. Expecting JSON with 'query' key"}), 400

        query = data["query"]
        logger.info(f"Query reçue : {query}")

        # Vérification du type de requête (écriture ou lecture)
        is_write_query = query.strip().lower().startswith(("insert", "update", "delete"))

        # Connexion à la base de données
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        if is_write_query:
            # Requête d'écriture
            cursor.execute(query)
            conn.commit()
            logger.info("Requête d'écriture exécutée avec succès")
            return jsonify({"message": "Write query executed successfully"}), 200
        else:
            # Requête de lecture
            cursor.execute(query)
            result = cursor.fetchall()
            logger.info("Requête de lecture exécutée avec succès")
            return jsonify(result), 200

    except mysql.connector.Error as err:
        # Gestion des erreurs MySQL
        logger.error(f"Erreur MySQL: {err}")
        return jsonify({"error": f"MySQL error: {err}"}), 500

    except Exception as e:
        # Gestion des erreurs générales
        logger.error(f"Erreur inattendue: {e}")
        return jsonify({"error": str(e)}), 500

    finally:
        # Fermeture de la connexion à la base de données
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            logger.info("Connexion à la base de données fermée")


if __name__ == "__main__":
    # Lancement de l'application Flask
    app.run(host="0.0.0.0", port=5000)