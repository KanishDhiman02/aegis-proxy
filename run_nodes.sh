#!/usr/bin/env bash
# Launches a 5-node mock backend cluster for testing the proxy.
# PIDs are written to .nodes.pid as "<port> <pid>" so kill_node.sh
# can take down one specific node (real process kill, not just a
# forced-503 flag) without touching the others.
#
# Ctrl+C kills all nodes and cleans up (trap below).

set -e

PID_FILE=".nodes.pid"
> "$PID_FILE"

start_node() {
    local port=$1
    local name=$2
    shift 2
    python mock_server.py --port "$port" --name "$name" "$@" &
    echo "$port $!" >> "$PID_FILE"
}

cleanup() {
    echo "Stopping all nodes..."
    while read -r port pid; do
        kill "$pid" 2>/dev/null || true
    done < "$PID_FILE"
    rm -f "$PID_FILE"
}
trap cleanup EXIT

start_node 9001 node-1
start_node 9002 node-2 --failure-rate 0.05 --min-latency-ms 20 --max-latency-ms 100
start_node 9003 node-3
start_node 9004 node-4
start_node 9005 node-5 --min-latency-ms 10 --max-latency-ms 50

echo "5 mock nodes running on ports 9001-9005."
echo "PIDs recorded in $PID_FILE. Use ./kill_node.sh <port> to kill one node."
echo "Press Ctrl+C to stop all nodes."
wait
