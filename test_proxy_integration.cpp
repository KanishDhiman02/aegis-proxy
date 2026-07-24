// Integration / chaos test suite for the Aegis proxy.
//
// Unlike the per-module tests (test_hash_ring, test_rate_limiter,
// test_circuit_breaker), this drives the REAL handle_client pipeline
// over REAL sockets - a test client connects exactly the way a real
// client would, and fake backends are separate asio TCP servers whose
// behavior can be flipped at runtime (healthy / instantly-closing /
// slow / partial-then-dead), so failures are genuine socket-level
// events, not simulated return values.
#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "circuit_breaker.hpp"
#include "connection_pool.hpp"
#include "hash_ring.hpp"
#include "metrics.hpp"
#include "rate_limiter.hpp"
#include "session.hpp"

using asio::ip::tcp;
using namespace asio::experimental::awaitable_operators;

namespace {

int g_failures = 0;
void check(bool cond, const std::string& what) {
    std::printf("  [%s] %s\n", cond ? "PASS" : "FAIL", what.c_str());
    if (!cond) ++g_failures;
}

// ---- Fake backend: a controllable stand-in for a real service ----

enum class Mode { NORMAL, FAIL_CLOSE, SLOW, PARTIAL_THEN_CLOSE };

struct FakeBackend {
    std::atomic<Mode> mode{Mode::NORMAL};
    std::atomic<int> hit_count{0};
    std::string canned_response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";
    std::chrono::milliseconds slow_delay{80};
};

asio::awaitable<void> fake_backend_session(tcp::socket socket, std::shared_ptr<FakeBackend> cfg) {
    cfg->hit_count.fetch_add(1, std::memory_order_relaxed);
    asio::error_code ec;
    std::array<char, 4096> buf;
    co_await socket.async_read_some(asio::buffer(buf), asio::redirect_error(asio::use_awaitable, ec));

    switch (cfg->mode.load()) {
        case Mode::NORMAL:
            co_await asio::async_write(socket, asio::buffer(cfg->canned_response),
                                        asio::redirect_error(asio::use_awaitable, ec));
            break;
        case Mode::SLOW: {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(cfg->slow_delay);
            co_await timer.async_wait(asio::use_awaitable);
            co_await asio::async_write(socket, asio::buffer(cfg->canned_response),
                                        asio::redirect_error(asio::use_awaitable, ec));
            break;
        }
        case Mode::FAIL_CLOSE:
            // Accept, then close without responding - the "got_any_response=false" path.
            break;
        case Mode::PARTIAL_THEN_CLOSE: {
            static const std::string partial = "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nHALF";
            co_await asio::async_write(socket, asio::buffer(partial),
                                        asio::redirect_error(asio::use_awaitable, ec));
            // Coroutine ends here -> socket destructs -> connection dies
            // mid-body, after the client has already received "HALF".
            break;
        }
    }
}

asio::awaitable<void> fake_backend_listener(std::shared_ptr<tcp::acceptor> acceptor,
                                             std::shared_ptr<FakeBackend> cfg) {
    for (;;) {
        asio::error_code ec;
        tcp::socket socket = co_await acceptor->async_accept(asio::redirect_error(asio::use_awaitable, ec));
        if (ec) co_return; // acceptor was closed (simulating the node being taken fully offline)
        asio::co_spawn(acceptor->get_executor(), fake_backend_session(std::move(socket), cfg),
                        asio::detached);
    }
}

// ---- Test harness: a full proxy pipeline pointed at 3 fake backends ----

struct Pipeline {
    asio::io_context& io_context;
    aegis::ConnectionPool pool;
    aegis::HashRing ring;
    aegis::RateLimiter limiter;
    std::deque<aegis::CircuitBreaker> breakers;
    aegis::MetricsRegistry metrics;
    std::shared_ptr<tcp::acceptor> proxy_acceptor;
    std::vector<std::shared_ptr<tcp::acceptor>> backend_acceptors;
    std::vector<std::shared_ptr<FakeBackend>> backends_cfg;

    Pipeline(asio::io_context& ioc, unsigned short proxy_port, std::vector<unsigned short> backend_ports,
             double rl_refill, double rl_capacity, int breaker_failure_threshold,
             int breaker_success_threshold, std::chrono::milliseconds breaker_cooldown,
             std::size_t min_pool_size = 2)
        : io_context(ioc),
          pool(ioc, [&] {
              std::vector<aegis::BackendConfig> cfgs;
              for (auto port : backend_ports) {
                  cfgs.push_back({"127.0.0.1", std::to_string(port), min_pool_size});
              }
              return cfgs;
          }()),
          limiter(rl_refill, rl_capacity),
          metrics(backend_ports.size()) {
        for (std::size_t i = 0; i < backend_ports.size(); ++i) {
            ring.add_backend(i);
            breakers.emplace_back(breaker_failure_threshold, breaker_success_threshold, breaker_cooldown);
        }
        proxy_acceptor = std::make_shared<tcp::acceptor>(
            ioc, tcp::endpoint(tcp::v4(), proxy_port));
        for (auto port : backend_ports) {
            auto cfg = std::make_shared<FakeBackend>();
            auto acceptor = std::make_shared<tcp::acceptor>(ioc, tcp::endpoint(tcp::v4(), port));
            backends_cfg.push_back(cfg);
            backend_acceptors.push_back(acceptor);
            asio::co_spawn(ioc, fake_backend_listener(acceptor, cfg), asio::detached);
        }
    }

    asio::awaitable<void> listen() {
        for (;;) {
            asio::error_code ec;
            tcp::socket socket = co_await proxy_acceptor->async_accept(asio::redirect_error(asio::use_awaitable, ec));
            if (ec) co_return;
            socket.set_option(tcp::no_delay(true));

            auto remote = socket.remote_endpoint(ec);
            std::string client_key = ec ? "unknown" : remote.address().to_string();

            if (!limiter.allow(client_key)) {
                metrics.record_rate_limited();
                asio::co_spawn(proxy_acceptor->get_executor(),
                                [](tcp::socket s) -> asio::awaitable<void> {
                                    static const std::string body = "Rate limited\n";
                                    std::string resp = "HTTP/1.1 429 Too Many Requests\r\nContent-Length: " +
                                                        std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;
                                    asio::error_code wec;
                                    co_await asio::async_write(s, asio::buffer(resp),
                                                                asio::redirect_error(asio::use_awaitable, wec));
                                }(std::move(socket)),
                                asio::detached);
                continue;
            }

            asio::co_spawn(proxy_acceptor->get_executor(),
                            aegis::handle_client(std::move(socket), pool, ring, breakers, client_key,
                                                  "test-" + client_key, metrics),
                            asio::detached);
        }
    }
};

// Blocking helper run on a background thread pool - opens a real socket
// to the proxy, sends a minimal request, returns the raw response.
// bind_ip lets us simulate distinct clients (which the ring/limiter key
// on), same technique used in manual testing throughout this project.
std::string send_request(unsigned short proxy_port, const std::string& bind_ip, int timeout_sec = 3) {
    (void)timeout_sec; // reserved for a future select()/poll()-based timeout if a scenario needs one
    try {
        asio::io_context ioc;
        tcp::socket socket(ioc);
        if (!bind_ip.empty()) {
            socket.open(tcp::v4());
            socket.bind(tcp::endpoint(asio::ip::make_address(bind_ip), 0));
        }
        socket.connect(tcp::endpoint(asio::ip::make_address("127.0.0.1"), proxy_port));
        std::string req = "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
        asio::write(socket, asio::buffer(req));

        socket.non_blocking(false);
        asio::error_code ec;
        std::array<char, 4096> buf;
        std::string data;
        // Every fake-backend mode and every proxy response path
        // eventually closes its end of the connection, so a plain
        // blocking read loop terminates via EOF - no artificial timeout
        // needed for these scenarios.
        while (true) {
            std::size_t n = socket.read_some(asio::buffer(buf), ec);
            if (ec || n == 0) break;
            data.append(buf.data(), n);
        }
        return data;
    } catch (const std::exception& e) {
        return std::string("ERROR: ") + e.what();
    }
}

} // namespace

int main() {
    std::deque<Pipeline> pipelines; // must outlive every scenario - see note above main()
    // A single io_context drives every Pipeline's asio objects; a
    // background thread runs io_context.run() so the blocking
    // send_request() helper (run from the main thread / test threads)
    // can talk to it over real loopback sockets, exactly as a real
    // client would - this is a genuine client/server boundary, not a
    // function call.
    asio::io_context ioc;
    auto guard = asio::make_work_guard(ioc);
    std::thread io_thread([&ioc]() { ioc.run(); });

    std::printf("=== Scenario A: normal operation ===\n");
    {
        pipelines.emplace_back(ioc, 21000, std::vector<unsigned short>{21001, 21002, 21003}, 1000.0, 1000.0, 5, 2,
                                std::chrono::milliseconds(5000));
        auto& p = pipelines.back();
        asio::co_spawn(ioc, p.listen(), asio::detached);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        auto resp = send_request(21000, "");
        check(resp.find("200 OK") != std::string::npos, "normal request gets 200 OK");
    }

    std::printf("\n=== Scenario B: rate limiting ===\n");
    {
        pipelines.emplace_back(ioc, 21010, std::vector<unsigned short>{21011, 21012, 21013}, 5.0, 10.0, 5, 2,
                                std::chrono::milliseconds(5000));
        auto& p = pipelines.back();
        asio::co_spawn(ioc, p.listen(), asio::detached);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        std::vector<std::thread> threads;
        std::vector<std::string> results(20);
        for (int i = 0; i < 20; ++i) {
            threads.emplace_back([&, i]() { results[i] = send_request(21010, "127.0.20.1"); });
        }
        for (auto& t : threads) t.join();

        int ok = 0, limited = 0;
        for (auto& r : results) {
            if (r.find("200 OK") != std::string::npos) ++ok;
            if (r.find("429") != std::string::npos) ++limited;
        }
        std::printf("  200s=%d 429s=%d (capacity=10)\n", ok, limited);
        check(ok == 10 && limited == 10, "burst of 20 against capacity 10 -> exactly 10 allowed, 10 limited");
    }

    std::printf("\n=== Scenario C: circuit breaker trips on real failures, retry keeps client happy ===\n");
    {
        pipelines.emplace_back(ioc, 21020, std::vector<unsigned short>{21021, 21022, 21023}, 1000.0, 1000.0, /*failure_threshold=*/3, 2,
                                std::chrono::milliseconds(60000)); // long cooldown - we test the trip, not recovery, here
        auto& p = pipelines.back();
        asio::co_spawn(ioc, p.listen(), asio::detached);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Find a client key that maps to backend 0.
        std::string client_key;
        for (int i = 1; i < 250; ++i) {
            std::string candidate = "127.0.21." + std::to_string(i);
            if (p.ring.get_backend(candidate) == 0) { client_key = candidate; break; }
        }
        check(!client_key.empty(), "found a client key mapping to backend 0");

        p.backends_cfg[0]->mode.store(Mode::FAIL_CLOSE);

        bool all_ok = true;
        for (int i = 0; i < 3; ++i) {
            auto resp = send_request(21020, client_key);
            if (resp.find("200 OK") == std::string::npos) all_ok = false;
        }
        check(all_ok, "3 requests against a failing backend 0 still all succeed via retry");
        check(p.breakers[0].state() == aegis::BreakerState::OPEN,
              "backend 0's breaker is OPEN after failure_threshold=3 real failures");

        int hits_before = p.backends_cfg[0]->hit_count.load();
        send_request(21020, client_key);
        int hits_after = p.backends_cfg[0]->hit_count.load();
        check(hits_after == hits_before, "OPEN breaker prevents a 4th connection attempt to backend 0");
    }

    std::printf("\n=== Scenario D: half-open stampede gating under real concurrency ===\n");
    {
        pipelines.emplace_back(ioc, 21030, std::vector<unsigned short>{21031, 21032, 21033}, 1000.0, 1000.0, /*failure_threshold=*/1, /*success_threshold=*/2,
                                std::chrono::milliseconds(150));
        auto& p = pipelines.back();
        asio::co_spawn(ioc, p.listen(), asio::detached);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        std::string client_key;
        for (int i = 1; i < 250; ++i) {
            std::string candidate = "127.0.22." + std::to_string(i);
            if (p.ring.get_backend(candidate) == 0) { client_key = candidate; break; }
        }

        p.backends_cfg[0]->mode.store(Mode::FAIL_CLOSE);
        send_request(21030, client_key); // trips the breaker (threshold 1)
        check(p.breakers[0].state() == aegis::BreakerState::OPEN, "backend 0 tripped OPEN after 1 failure");

        std::this_thread::sleep_for(std::chrono::milliseconds(200)); // cooldown elapses

        // Backend "recovers", but answers slowly - wide enough a window
        // for many concurrent requests to all reach allow_request() while
        // the first trial is still unresolved.
        p.backends_cfg[0]->mode.store(Mode::SLOW);
        p.backends_cfg[0]->slow_delay = std::chrono::milliseconds(200);
        int hits_before_burst = p.backends_cfg[0]->hit_count.load();

        std::vector<std::thread> threads;
        for (int i = 0; i < 10; ++i) {
            threads.emplace_back([&]() { send_request(21030, client_key); });
        }
        for (auto& t : threads) t.join();

        int hits_during_burst = p.backends_cfg[0]->hit_count.load() - hits_before_burst;
        std::printf("  backend 0 hit_count during 10-way concurrent burst: %d (gate should keep this small)\n",
                    hits_during_burst);
        check(hits_during_burst <= 2,
              "half-open gate limits concurrent trials against backend 0 (got " +
                  std::to_string(hits_during_burst) + ", not 10)");
    }

    std::printf("\n=== Scenario E: total outage -> clean 503 ===\n");
    {
        pipelines.emplace_back(ioc, 21040, std::vector<unsigned short>{21041, 21042, 21043}, 1000.0, 1000.0, 5, 2,
                                std::chrono::milliseconds(5000));
        auto& p = pipelines.back();
        asio::co_spawn(ioc, p.listen(), asio::detached);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        for (auto& cfg : p.backends_cfg) cfg->mode.store(Mode::FAIL_CLOSE);
        auto resp = send_request(21040, "");
        check(resp.find("503") != std::string::npos, "all backends failing -> clean 503");
        check(resp.find("200") == std::string::npos, "503 response contains no stray 200");
    }

    std::printf("\n=== Scenario F: retry never happens after the stream-commit point ===\n");
    {
        pipelines.emplace_back(ioc, 21050, std::vector<unsigned short>{21051, 21052, 21053}, 1000.0, 1000.0, 5, 2,
                                std::chrono::milliseconds(5000));
        auto& p = pipelines.back();
        asio::co_spawn(ioc, p.listen(), asio::detached);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        std::string client_key;
        for (int i = 1; i < 250; ++i) {
            std::string candidate = "127.0.25." + std::to_string(i);
            if (p.ring.get_backend(candidate) == 0) { client_key = candidate; break; }
        }
        p.backends_cfg[0]->mode.store(Mode::PARTIAL_THEN_CLOSE);

        auto resp = send_request(21050, client_key);
        check(resp.find("HALF") != std::string::npos, "client receives the partial bytes that were sent");
        check(resp.find("503") == std::string::npos,
              "no 503 gets appended after partial data - no retry once bytes are committed");

        int hits = p.backends_cfg[0]->hit_count.load();
        check(hits == 1, "backend 0 was hit exactly once - no retry attempted after commit");
    }

    guard.reset();
    ioc.stop();
    io_thread.join();

    std::printf("\n%s (%d failure%s)\n", g_failures == 0 ? "ALL SCENARIOS PASS" : "SOME SCENARIOS FAILED",
                g_failures, g_failures == 1 ? "" : "s");
    return g_failures == 0 ? 0 : 1;
}
