#!/usr/bin/env bash
# One-command build + run for the whole Aegis Proxy stack.
#
# What this does, in order:
#   1. Configures + builds the C++ project with CMake (generates
#      build/compile_commands.json as a side effect - point your editor's
#      clangd/C++ extension at that file, or run the symlink step below,
#      for full autocomplete and inline error checking).
#   2. Starts 3 mock backend nodes (Python/Flask) on ports 9001-9003.
#   3. Starts the Aegis proxy on port 8080 (metrics on 8081).
#   4. On Ctrl+C, stops everything cleanly - no orphaned background
#      processes left listening on your ports.
#
# Usage:
#   chmod +x run.sh
#   ./run.sh
#
# Then in another terminal:
#   curl http://127.0.0.1:8080/
#   curl http://127.0.0.1:8081/          # Prometheus metrics

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

echo "==> [1/4] Configuring + building (CMake + g++, C++20)..."
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release > /tmp/aegis_cmake_configure.log 2>&1 \
    || { echo "CMake configure failed - see /tmp/aegis_cmake_configure.log"; exit 1; }
cmake --build build -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)" \
    > /tmp/aegis_cmake_build.log 2>&1 \
    || { echo "Build failed - see /tmp/aegis_cmake_build.log"; exit 1; }

# Symlink compile_commands.json to the project root - most editors
# (VS Code's clangd extension, CLion, vim/neovim with clangd) look for
# it there by default, not inside build/.
ln -sf build/compile_commands.json compile_commands.json
echo "    build OK. compile_commands.json linked at project root."

echo "==> [2/4] Setting up the mock backend virtualenv (first run only)..."
if [ ! -d venv ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install -q --disable-pip-version-check -r requirements.txt

PIDS=()
cleanup() {
    echo ""
    echo "==> Shutting down..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    echo "    stopped."
}
trap cleanup EXIT INT TERM

echo "==> [3/4] Starting 3 mock backend nodes (ports 9001-9003)..."
python3 mock_server.py --port 9001 --name node-1 > /tmp/aegis_node-1.log 2>&1 &
PIDS+=($!)
python3 mock_server.py --port 9002 --name node-2 > /tmp/aegis_node-2.log 2>&1 &
PIDS+=($!)
python3 mock_server.py --port 9003 --name node-3 > /tmp/aegis_node-3.log 2>&1 &
PIDS+=($!)
sleep 1

echo "==> [4/4] Starting the Aegis proxy (port 8080, metrics on 8081)..."
echo ""
echo "    Try in another terminal:"
echo "      curl http://127.0.0.1:8080/"
echo "      curl http://127.0.0.1:8081/"
echo ""
echo "    Ctrl+C to stop everything."
echo ""

./build/aegis_proxy 8080
