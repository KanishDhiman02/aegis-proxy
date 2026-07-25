#!/usr/bin/env bash
# One-command build + run for the ENTIRE Aegis Proxy test suite.
#
# What this does, in order:
#   1. (macOS only) Aliases the extra loopback addresses the integration
#      test needs onto lo0. Linux doesn't need this - the whole 127.0.0.0/8
#      block is loopback there automatically, so this step is skipped.
#   2. Configures + builds the project with CMake (Release, cached - only
#      rebuilds what changed).
#   3. Runs all four test binaries in order: hash_ring, rate_limiter,
#      circuit_breaker, then the full proxy_integration suite.
#   4. Prints one clear PASS/FAIL summary at the end.
#
# Usage:
#   chmod +x test.sh
#   ./test.sh
#
# Exits non-zero if anything failed, so it's CI/script-friendly too.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

BOLD='\033[1m'; GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[0;33m'; NC='\033[0m'
step() { echo -e "\n${BOLD}==> $1${NC}"; }

# --- 1. macOS loopback aliasing (no-op on Linux) ---------------------------
if [[ "$(uname)" == "Darwin" ]]; then
    step "Setting up macOS loopback aliases (needed for the integration test's simulated clients)"
    LOOPBACK_IPS=(127.0.20.1 127.0.21.1 127.0.22.7 127.0.25.1)
    for ip in "${LOOPBACK_IPS[@]}"; do
        if ! ifconfig lo0 | grep -q "$ip"; then
            echo "  aliasing $ip onto lo0 (sudo required)..."
            sudo ifconfig lo0 alias "$ip" up
        else
            echo "  $ip already aliased, skipping"
        fi
    done
fi

# --- 2. Configure + build ---------------------------------------------------
step "Configuring + building (Release)"
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release > /tmp/aegis_cmake.log 2>&1 || {
    echo -e "${RED}CMake configure failed. Log:${NC}"; cat /tmp/aegis_cmake.log; exit 1;
}
NPROC=$( (command -v nproc >/dev/null && nproc) || sysctl -n hw.ncpu )
cmake --build build -j"$NPROC" > /tmp/aegis_build.log 2>&1 || {
    echo -e "${RED}Build failed. Log:${NC}"; cat /tmp/aegis_build.log; exit 1;
}
echo "  build OK"

# --- 3. Run every test binary ------------------------------------------------
FAILED=0
run_suite() {
    local name="$1" bin="$2"
    step "Running $name"
    if ./build/"$bin"; then
        echo -e "${GREEN}  $name: PASS${NC}"
    else
        echo -e "${RED}  $name: FAIL${NC}"
        FAILED=1
    fi
}

run_suite "hash_ring"          test_hash_ring
run_suite "rate_limiter"       test_rate_limiter
run_suite "circuit_breaker"    test_circuit_breaker
run_suite "proxy_integration"  test_proxy_integration

# --- 4. Summary ---------------------------------------------------------------
echo ""
if [[ $FAILED -eq 0 ]]; then
    echo -e "${GREEN}${BOLD}ALL SUITES PASSED${NC}"
else
    echo -e "${RED}${BOLD}SOME SUITES FAILED${NC} - scroll up for details"
fi
exit $FAILED
