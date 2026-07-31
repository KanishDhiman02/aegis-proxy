# Aegis Proxy

A Layer-4 reverse proxy built from scratch in **C++20**, using **Asio** and coroutines for async I/O. Aegis sits in front of multiple backend servers and, for every incoming connection, decides which backend should handle it — with consistent-hash routing, per-client rate limiting, circuit breaking, connection pooling, and structured observability, all built as first-class pieces rather than bolted on.

This is not a wrapper around an existing proxy or framework — every piece below (the hash ring, the token bucket, the breaker state machine, the connection pool) is original code, written to actually understand what a proxy does at the socket level rather than have it hidden behind a library.

## What it does

| Component | Problem it solves |
|---|---|
| **Consistent hashing** (500 virtual nodes/backend) | Routes each client to a backend, and survives backends being added/removed without reshuffling every other client — only ~1/N of clients are reassigned when one of N backends changes, not ~100%. |
| **Token-bucket rate limiter** (64-way sharded) | Per-client burst + sustained rate limiting, without one hot global lock serializing unrelated clients. |
| **Circuit breaker** (CLOSED / OPEN / HALF_OPEN) | Detects a failing backend, fails fast instead of waiting on doomed connections, and recovers cautiously via a single gated trial request instead of a stampede. |
| **Connection pooling** | Reuses warm backend connections instead of paying a fresh TCP handshake on every request. |
| **Safe retry** | Retries a different backend if one fails *before* any response bytes reach the client (replaying a bounded request buffer) — and never retries once bytes have already gone out, since a second response would corrupt what the client already has. |
| **Structured tracing + Prometheus metrics** | Every request gets a correlation ID; every log line for its lifetime is tagged with it. A second HTTP listener serves live metrics in Prometheus text format. |

## Benchmark results

Load-tested against a baseline nginx reverse proxy under identical, fair conditions — both pinned to a single worker thread (Aegis is currently single-threaded; see **Limitations**), both fronting the same trivial static backend so neither proxy's own overhead was hidden behind backend latency. Generated with `wrk`, 4 threads / 100 connections / 20s, on an 8-core MacBook Air (M-series).

| Metric | Aegis | nginx (baseline) |
|---|---|---|
| Requests/sec | **~71,700** | ~12,400 |
| p50 latency | **1.26 ms** | 6.51 ms |
| p90 latency | **1.71 ms** | 18.68 ms |
| p99 latency | **3.62 ms** | 39.97 ms |

**Read this honestly:** this is not a claim that Aegis "beats nginx." nginx has 20 years of production hardening, TLS termination, HTTP/2, and a full module ecosystem, running here with only basic proxying enabled. The fair claim: for this one specific job, Aegis's C++20 code path currently has less per-request overhead than nginx's general-purpose config, in this exact test. Reproduce it yourself with `./bench.sh` (see below).

## Getting started

Requires CMake, a C++20 compiler, and Python 3 (for the mock backends).

```bash
git clone https://github.com/KanishDhiman02/aegis-proxy.git
cd aegis-proxy
./run.sh
```

This builds everything, starts 3 mock backend nodes (ports 9001–9003), and starts the proxy on port 8080 (metrics on 8081). In another terminal:

```bash
curl http://127.0.0.1:8080/
curl http://127.0.0.1:8081/   # Prometheus metrics
```

`Ctrl+C` tears everything down cleanly.

### Running the test suite

```bash
chmod +x test.sh
./test.sh
```

Builds and runs all four test suites — hash ring, rate limiter, circuit breaker, and full end-to-end integration/chaos tests — ending in a single `ALL SUITES PASSED` or a specific failure. On macOS, this also handles the loopback address aliasing the integration tests need (see **Known issues** below).

### Reproducing the benchmark

```bash
brew install nginx wrk   # macOS; apt install nginx (and build wrk from source) on Linux
chmod +x bench.sh
./bench.sh 4 100 20s     # threads, connections, duration
```

Spins up fast dummy backends, builds and starts Aegis, starts a baseline nginx pointed at the identical backend, runs identical `wrk` load against both, and prints results side by side — cleaning up every process it started, even on Ctrl+C.

## Project structure

```
main.cpp                    entry point, event loop, listener
session.{hpp,cpp}            per-connection relay logic, retry buffer, safe-retry rule
connection_pool.{hpp,cpp}    per-backend warm connection pools
hash_ring.{hpp,cpp}          consistent hashing with virtual nodes
rate_limiter.{hpp,cpp}       sharded token-bucket rate limiter
circuit_breaker.{hpp,cpp}    three-state breaker with half-open trial gating
metrics.hpp                  Prometheus-format counters/gauges
tracer.hpp                   correlation-ID structured logging

test_hash_ring.cpp           distribution + reshuffle-impact tests
test_rate_limiter.cpp        correctness + contention benchmark
test_circuit_breaker.cpp     state machine tests
test_proxy_integration.cpp   full chaos test suite (real sockets, controllable fake backends)

mock_server.py               Flask backend with runtime-controllable health/latency/failure rate
run.sh / test.sh / bench.sh  one-command run / test / benchmark scripts
```

## Architecture

A request flows through the proxy in this order:

```
Client
  │
  ▼
[Rate Limiter] ──(no tokens)──▶ HTTP 429, done
  │ (allowed)
  ▼
[Hash Ring: pick backend] ◀────────────┐
  │                                    │ (retry, different backend)
  ▼                                    │
[Circuit Breaker check] ──(OPEN)───────┤
  │ (CLOSED / HALF_OPEN trial)         │
  ▼                                    │
[Connection Pool: acquire socket]      │
  │                                    │
  ▼                                    │
[Relay bytes both directions]          │
  │                                    │
  ├─(failed, 0 bytes sent)─────────────┘
  │
  ├─(failed, bytes already sent)──▶ connection just ends, no retry
  │
  └─(success)──▶ response streamed to client, done

If every attempt is exhausted before any bytes reached the client:
  → HTTP 503, done
```

Aegis operates at **Layer 4** — it relays raw TCP bytes and never parses HTTP. This is a deliberate scope decision (see Limitations), not an oversight.

## Known issues & how they were found

Every bug below was caught by actually running the code under real conditions, not by code inspection:

- **Retry deadlock on partial backend failure** — automated chaos testing (not manual testing) found that retrying a failed backend without buffering the client's already-consumed request bytes left the new backend waiting forever for data that would never arrive. Fixed with a capped request-replay buffer.
- **RST instead of clean FIN on rejection paths** — closing a socket with unread client bytes still in its receive buffer triggers an OS-level RST, not a clean close, regardless of application intent. Fixed by explicitly draining unread bytes before closing.
- **Weak hash diffusion under virtual nodes** — plain FNV-1a showed poor load distribution on short, similar keys; a murmur3-style avalanche finalizer fixed it, confirmed by measuring 100,000 simulated keys before and after.
- **A real SIGBUS crash under concurrent load (ARM64)** — found via `lldb` attached to a live process under real `wrk` load, after ruling out memory-unsafety with AddressSanitizer/UndefinedBehaviorSanitizer (both came back clean). Root cause: an initializer-list-of-temporaries pattern building a debug-log field vector didn't reliably survive a coroutine suspend/resume boundary on AppleClang/ARM64. Fixed by building the vector explicitly instead.
- **macOS loopback binding gap** — integration tests using non-`127.0.0.1` loopback addresses to simulate distinct clients silently failed to bind on macOS (only Linux auto-aliases the whole `127.0.0.0/8` block). `test.sh` now handles this automatically.

Full write-up of all nine bugs found, with symptom/diagnosis/root-cause/fix for each, is in the project's companion guide document (not in this repo).

## Limitations — what this is not (yet)

Stated plainly, not hidden:

- **Single-threaded.** One `io_context`, one core. The natural next step is `SO_REUSEPORT` with multiple worker threads — one `io_context` per core — which is how nginx and Envoy actually get multi-core throughput.
- **No TLS/HTTPS termination.** Plain HTTP/TCP only.
- **Not HTTP-aware at the response level.** The circuit breaker can tell "did any bytes come back," not "did the backend return a 500" — a backend that connects fine but replies with an error page currently looks healthy from Aegis's point of view.
- **Rate limiting and routing decisions happen once per TCP connection**, not once per HTTP request when a connection is reused via keep-alive.

None of these block this from being a solid, honest systems project — they're the natural "what would you build next" answer.

## License

MIT
