#!/usr/bin/env bash
# One-command fair benchmark: Aegis vs a baseline nginx reverse proxy,
# both single-worker, both fronting the identical fast backend.
#
# Why not just proxy_pass to mock_server.py? Flask's dev server is
# single-process and GIL-bound - it'd cap out around a few hundred req/s
# regardless of proxy speed, making ANY comparison meaningless (you'd be
# benchmarking Flask, not the proxies). This script spins up a trivial
# static-response nginx "backend" instead, so the proxy layer is what's
# actually being measured.
#
# What this does:
#   1. Starts 3 instant-response nginx backends (ports 9001-9003, matching
#      main.cpp's hardcoded backend list).
#   2. Starts Aegis (port 8080) with its rate limiter raised via env vars -
#      the production default (10 req/s/client, burst 20) is real and
#      correct, but would make wrk (a single client IP) measure the rate
#      limiter instead of proxy throughput. Raised ONLY for this benchmark.
#   3. Figures out which single backend Aegis's hash ring picks for this
#      machine's loopback client IP, then points a baseline nginx reverse
#      proxy (1 worker, port 8090) at that SAME backend - so both proxies
#      are doing identical work.
#   4. Runs identical `wrk` load against both, prints results side by side.
#   5. Cleans up every process it started, even on Ctrl+C.
#
# Requires: nginx, wrk  (macOS: brew install nginx wrk)
#
# Usage:
#   chmod +x bench.sh
#   ./bench.sh                      # defaults: 2 threads, 50 conns, 15s
#   ./bench.sh 4 100 30s            # threads conns duration

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

THREADS="${1:-2}"
CONNS="${2:-50}"
DURATION="${3:-15s}"

WORKDIR=$(mktemp -d)
AEGIS_PID=""
BACKEND_PIDS=()
BASELINE_CONF=""

cleanup() {
    echo -e "\n[cleanup] stopping everything..."
    [[ -n "$AEGIS_PID" ]] && kill "$AEGIS_PID" 2>/dev/null || true
    for conf in "$WORKDIR"/backend_*.conf; do
        [[ -f "$conf" ]] && nginx -c "$conf" -s stop 2>/dev/null || true
    done
    [[ -n "$BASELINE_CONF" ]] && nginx -c "$BASELINE_CONF" -s stop 2>/dev/null || true
    rm -rf "$WORKDIR"
}
trap cleanup EXIT INT TERM

command -v nginx >/dev/null || { echo "nginx not found. macOS: brew install nginx"; exit 1; }
command -v wrk   >/dev/null || { echo "wrk not found. macOS: brew install wrk"; exit 1; }

echo "==> Starting 3 fast static backends (9001-9003)"
for port in 9001 9002 9003; do
cat > "$WORKDIR/backend_$port.conf" << EOF
worker_processes 1;
error_log $WORKDIR/backend_${port}_error.log;
pid $WORKDIR/backend_${port}.pid;
events { worker_connections 1024; }
http {
    access_log off;
    server {
        listen 127.0.0.1:$port;
        location / {
            default_type application/json;
            return 200 '{"node":"backend-$port","message":"ok"}';
        }
    }
}
EOF
    nginx -c "$WORKDIR/backend_$port.conf"
done
sleep 1

echo "==> Building Aegis (Release)"
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release > /tmp/aegis_bench_cmake.log 2>&1
NPROC=$( (command -v nproc >/dev/null && nproc) || sysctl -n hw.ncpu )
cmake --build build -j"$NPROC" --target aegis_proxy > /tmp/aegis_bench_build.log 2>&1

echo "==> Starting Aegis on :8080 (rate limiter raised for this benchmark only)"
AEGIS_RATE_LIMIT_RPS=1000000 AEGIS_RATE_LIMIT_BURST=1000000 \
    ./build/aegis_proxy 8080 > "$WORKDIR/aegis.log" 2>&1 &
AEGIS_PID=$!
sleep 1

echo "==> Determining which backend Aegis routes this client to"
curl -s http://127.0.0.1:8080/ > /dev/null
sleep 0.3
PICKED_BACKEND_IDX=$(grep -o "backend_selected backend=[0-9]" "$WORKDIR/aegis.log" | tail -1 | grep -o "[0-9]$")
PICKED_PORT=$((9001 + PICKED_BACKEND_IDX))
echo "    -> backend index $PICKED_BACKEND_IDX (port $PICKED_PORT)"

echo "==> Starting baseline nginx on :8090, pointed at the SAME backend (port $PICKED_PORT)"
BASELINE_CONF="$WORKDIR/baseline.conf"
cat > "$BASELINE_CONF" << EOF
worker_processes 1;
error_log $WORKDIR/baseline_error.log;
pid $WORKDIR/baseline.pid;
events { worker_connections 1024; }
http {
    access_log off;
    upstream backend { server 127.0.0.1:$PICKED_PORT; keepalive 32; }
    server {
        listen 127.0.0.1:8090;
        location / {
            proxy_pass http://backend;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
        }
    }
}
EOF
nginx -c "$BASELINE_CONF"
sleep 1

echo ""
echo "############ AEGIS PROXY  (:8080)  -t$THREADS -c$CONNS -d$DURATION ############"
wrk -t"$THREADS" -c"$CONNS" -d"$DURATION" --latency http://127.0.0.1:8080/

echo ""
echo "############ BASELINE NGINX (:8090)  -t$THREADS -c$CONNS -d$DURATION ############"
wrk -t"$THREADS" -c"$CONNS" -d"$DURATION" --latency http://127.0.0.1:8090/

echo ""
echo "############ MEMORY (RSS) ############"
ps -o pid,rss,comm -p "$AEGIS_PID" 2>/dev/null || true
pgrep -f "baseline.conf" | xargs -I{} ps -o pid,rss,comm -p {} 2>/dev/null || true

echo ""
echo "==> Sanity check: confirm this run had zero rate-limiting / zero failures on Aegis"
curl -s http://127.0.0.1:8081/ | grep -E "rate_limited_total|fallback_503_total"
echo ""
echo "Done. (Ctrl+C-safe - cleanup runs automatically.)"
