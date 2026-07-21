"""
Pulls /admin/stats from every mock node and prints a distribution table.
Use this after sending traffic through the proxy to verify the
consistent-hash ring is actually spreading load, and how evenly.

Usage:
    python collect_stats.py
    python collect_stats.py --ports 9001 9002 9003 9004 9005
"""

import argparse
import requests

DEFAULT_PORTS = [9001, 9002, 9003, 9004, 9005]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ports", type=int, nargs="+", default=DEFAULT_PORTS)
    args = parser.parse_args()

    rows = []
    total_requests = 0

    for port in args.ports:
        try:
            resp = requests.get(f"http://127.0.0.1:{port}/admin/stats", timeout=1)
            data = resp.json()
            rows.append(data)
            total_requests += data["request_count"]
        except requests.exceptions.ConnectionError:
            rows.append({"node": f"port-{port}", "healthy": False,
                         "request_count": 0, "failure_count": 0, "unreachable": True})

    print(f"{'node':<10} {'healthy':<8} {'requests':<10} {'failures':<10} {'share':<8}")
    for r in rows:
        if r.get("unreachable"):
            print(f"{r['node']:<10} {'DOWN':<8} {'-':<10} {'-':<10} {'-':<8}")
            continue
        share = (r["request_count"] / total_requests * 100) if total_requests else 0
        print(f"{r['node']:<10} {str(r['healthy']):<8} {r['request_count']:<10} "
              f"{r['failure_count']:<10} {share:.1f}%")

    print(f"\nTotal requests observed across cluster: {total_requests}")


if __name__ == "__main__":
    main()
