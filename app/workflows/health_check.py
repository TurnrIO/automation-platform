"""
Health Check — polls a list of URLs and prints a status report.

Edit TARGETS below to add your own endpoints.
Run via the Scripts page or schedule it on a cron trigger.
"""

import urllib.request
import urllib.error
import time

# ── Configuration ──────────────────────────────────────────────────────────────
TARGETS = [
    {"name": "Google",       "url": "https://www.google.com"},
    {"name": "Cloudflare",   "url": "https://1.1.1.1"},
    {"name": "Example API",  "url": "https://httpbin.org/get"},
]
TIMEOUT = 10   # seconds per request
# ───────────────────────────────────────────────────────────────────────────────

results = []

for target in TARGETS:
    name = target["name"]
    url  = target["url"]
    t0   = time.time()
    try:
        req  = urllib.request.Request(url, headers={"User-Agent": "automations-health-check/1.0"})
        resp = urllib.request.urlopen(req, timeout=TIMEOUT)
        ms   = int((time.time() - t0) * 1000)
        results.append({"name": name, "url": url, "status": resp.status, "ms": ms, "ok": True})
        print(f"  ✓  {name:20s}  HTTP {resp.status}  ({ms} ms)")
    except urllib.error.HTTPError as e:
        ms = int((time.time() - t0) * 1000)
        results.append({"name": name, "url": url, "status": e.code, "ms": ms, "ok": False})
        print(f"  ✗  {name:20s}  HTTP {e.code}  ({ms} ms)")
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        results.append({"name": name, "url": url, "status": None, "ms": ms, "ok": False, "error": str(e)})
        print(f"  ✗  {name:20s}  ERROR: {e}")

total   = len(results)
healthy = sum(1 for r in results if r["ok"])
print(f"\n{'─'*50}")
print(f"  {healthy}/{total} targets healthy")
if healthy < total:
    print("  DEGRADED — some targets are unreachable")
else:
    print("  ALL CLEAR")
