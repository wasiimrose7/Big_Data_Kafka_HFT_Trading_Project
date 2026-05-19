from flask import Flask, jsonify, render_template, request
import subprocess
import sys
import requests

app = Flask(__name__)


VM_METRICS_URL = "http://<YOUR_VM_PUBLIC_IP>:6000/data"  

#flask routes 

@app.route("/")
def home():
    return render_template("index.html")


@app.route("/start", methods=["POST"])
def start():
    data = request.json
    rate = data.get("rate", 5000)
    duration = data.get("duration", 10)

    subprocess.Popen([
        sys.executable,
        "cloud_producer.py",
        "--rate", str(rate),
        "--duration", str(duration)
    ])

    return jsonify({"status": "producer started"})


@app.route("/data")
def data():
    try:
        resp = requests.get(VM_METRICS_URL, timeout=1)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": "VM unreachable", "details": str(e)})


@app.route("/reset", methods=["POST"])
def reset():
    return jsonify({"status": "reset handled on VM (not local)"})



if __name__ == "__main__":
    app.run(debug=True, port=5000)