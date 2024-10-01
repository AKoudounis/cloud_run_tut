import os, json, time

from flask import Flask, request

app = Flask(__name__)

from google.cloud import bigquery

@app.route("/", methods=['GET', 'POST'])
def hello_world():
    """Example Hello World route."""

    return f"Hello World!!!!!!"


@app.route("/event_looks", methods=['POST'])
def event_looks():
    print(request.method)
    payload = json.loads(request.data)
    print(payload)

    return "Event Received"
    
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
