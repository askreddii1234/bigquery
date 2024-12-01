from flask import Flask, request, jsonify
from migration_service import migrate_data

app = Flask(__name__)

@app.route('/migrate/<environment>', methods=['POST'])
def migrate(environment):
    response = migrate_data(environment)
    return jsonify(response), 200 if 'message' in response else 400

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
