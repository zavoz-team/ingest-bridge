import os

from flask import Flask, jsonify

app = Flask(__name__)


@app.get('/health/live')
def health_live():
    return jsonify({'status': 'ok'}), 200


if __name__ == '__main__':
    host = os.environ.get('INGEST_BRIDGE_HOST', '0.0.0.0')
    port = int(os.environ.get('INGEST_BRIDGE_PORT', '8080'))
    app.run(host=host, port=port)
