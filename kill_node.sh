#!/usr/bin/env bash
# Kills one mock node by port, without touching the rest of the cluster.
# Use this to test real connection-refused failover, as opposed to
# /admin/health, which only simulates a 503 from a live process.
#
# Usage: ./kill_node.sh 9002

PID_FILE=".nodes.pid"

if [ -z "$1" ]; then
    echo "Usage: ./kill_node.sh <port>"
    exit 1
fi

if [ ! -f "$PID_FILE" ]; then
    echo "No $PID_FILE found - is run_nodes.sh still running?"
    exit 1
fi

pid=$(grep "^$1 " "$PID_FILE" | awk '{print $2}')

if [ -z "$pid" ]; then
    echo "No node found on port $1"
    exit 1
fi

kill "$pid"
grep -v "^$1 " "$PID_FILE" > "$PID_FILE.tmp" && mv "$PID_FILE.tmp" "$PID_FILE"
echo "Killed node on port $1 (pid $pid)"
