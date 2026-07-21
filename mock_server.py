"""
Mock backend server for testing the Aegis Edge Proxy.

Run multiple instances on different ports to simulate a cluster of
backend nodes. Each instance can independently:
  - inject artificial latency (to test timeouts / p99 metrics)
  - inject random failures (to test circuit breaker trip logic)
  - be manually forced healthy/unhealthy via an admin endpoint
    (to test failover deterministically, without relying on randomness)

Usage:
    python mock_server.py --port 9001 --name node-1
    python mock_server.py --port 9002 --name node-2 --failure-rate 0.3
    python mock_server.py --port 9003 --name node-3 --min-latency-ms 50 --max-latency-ms 400

Endpoints:
    GET  /              -> simulated work, subject to latency/failure injection
    GET  /health         -> plain health check (always 200 unless forced down)
    POST /admin/health   -> {"healthy": true|false}  force node up/down
    POST /admin/failure-rate -> {"rate": 0.0-1.0}     change failure rate live
    GET  /admin/stats    -> request counters, useful for verifying proxy routing
"""

import argparse
import random
import threading
import time

from flask import Flask, jsonify, request


def create_app(name: str, failure_rate: float, min_latency_ms: int, max_latency_ms: int):
    app = Flask(__name__)

    state = {
        "healthy": True,
        "failure_rate": failure_rate,
        "min_latency_ms": min_latency_ms,
        "max_latency_ms": max_latency_ms,
        "request_count": 0,
        "failure_count": 0,
        "lock": threading.Lock(),
    }

    @app.route("/", methods=["GET", "POST"])
    def handle_request():
        with state["lock"]:
            state["request_count"] += 1

        # Forced-down node: fail immediately, no latency, so the proxy's
        # timeout logic isn't what's being tested here — the circuit
        # breaker / retry-on-failure path is.
        if not state["healthy"]:
            with state["lock"]:
                state["failure_count"] += 1
            return jsonify({"error": "node forced unhealthy", "node": name}), 503

        # Simulate variable processing time.
        latency_ms = random.randint(state["min_latency_ms"], state["max_latency_ms"])
        time.sleep(latency_ms / 1000.0)

        # Random failure injection, independent of forced health state.
        if random.random() < state["failure_rate"]:
            with state["lock"]:
                state["failure_count"] += 1
            return jsonify({"error": "simulated failure", "node": name}), 500

        return jsonify({
            "node": name,
            "latency_ms": latency_ms,
            "message": "ok",
        }), 200

    @app.route("/health", methods=["GET"])
    def health():
        if state["healthy"]:
            return jsonify({"status": "healthy", "node": name}), 200
        return jsonify({"status": "unhealthy", "node": name}), 503

    @app.route("/admin/health", methods=["POST"])
    def set_health():
        payload = request.get_json(force=True, silent=True) or {}
        healthy = payload.get("healthy")
        if healthy is None:
            return jsonify({"error": "expected {\"healthy\": true|false}"}), 400
        with state["lock"]:
            state["healthy"] = bool(healthy)
        return jsonify({"node": name, "healthy": state["healthy"]}), 200

    @app.route("/admin/failure-rate", methods=["POST"])
    def set_failure_rate():
        payload = request.get_json(force=True, silent=True) or {}
        rate = payload.get("rate")
        if rate is None or not (0.0 <= float(rate) <= 1.0):
            return jsonify({"error": "expected {\"rate\": 0.0-1.0}"}), 400
        with state["lock"]:
            state["failure_rate"] = float(rate)
        return jsonify({"node": name, "failure_rate": state["failure_rate"]}), 200

    @app.route("/admin/stats", methods=["GET"])
    def stats():
        with state["lock"]:
            return jsonify({
                "node": name,
                "healthy": state["healthy"],
                "request_count": state["request_count"],
                "failure_count": state["failure_count"],
                "failure_rate": state["failure_rate"],
            }), 200

    return app


def main():
    parser = argparse.ArgumentParser(description="Mock backend node for Aegis proxy testing")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--name", type=str, default=None, help="Node identifier, defaults to node-<port>")
    parser.add_argument("--failure-rate", type=float, default=0.0, help="Probability (0-1) a live request fails")
    parser.add_argument("--min-latency-ms", type=int, default=5)
    parser.add_argument("--max-latency-ms", type=int, default=30)
    args = parser.parse_args()

    node_name = args.name or f"node-{args.port}"
    app = create_app(node_name, args.failure_rate, args.min_latency_ms, args.max_latency_ms)

    print(f"[{node_name}] listening on http://127.0.0.1:{args.port}  "
          f"(failure_rate={args.failure_rate}, latency={args.min_latency_ms}-{args.max_latency_ms}ms)")
    # threaded=True so one slow request doesn't block others - closer to
    # how a real backend under concurrent load behaves.
    app.run(host="127.0.0.1", port=args.port, threaded=True)


if __name__ == "__main__":
    main()
