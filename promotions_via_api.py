"""
Export ALL BigCommerce v3 promotions created via the API (created_from == "api") to CSV.

- Credentials are set inline below.
- Follows pagination using meta.pagination.links.next until exhausted.
- Writes ALL fields for each promotion to CSV:
    * Primitive values go in their own columns.
    * Nested dicts/lists are JSON-encoded into a single cell so no data is lost.

Usage:
  python export_bc_promotions_api_created.py --csv promotions_api.csv --limit 250

You can omit args; defaults are fine for most cases.
"""

import argparse
import csv
import json
import time
from typing import Any, Dict, Iterable, List, Optional
import requests

# -----------------------------
# Place your credentials here
# -----------------------------
STORE_HASH = ""
ACCESS_TOKEN = ""

API_BASE = "https://api.bigcommerce.com"


def bc_headers(token: str) -> Dict[str, str]:
    return {
        "X-Auth-Token": token,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def is_primitive(val: Any) -> bool:
    return isinstance(val, (str, int, float, bool)) or val is None


def normalize_row(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a promotion object into a flat dict suitable for CSV:
      - Keep top-level primitives as-is.
      - JSON-encode any nested dict/list under the same top-level key.
    This guarantees we include ALL data without losing structure.
    """
    row: Dict[str, Any] = {}
    for k, v in obj.items():
        if is_primitive(v):
            row[k] = v
        else:
            # Preserve the entire nested structure
            row[k] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    return row


def fetch_promotions(
    store_hash: str,
    token: str,
    limit: int = 250,
    timeout: int = 30,
    max_retries: int = 5,
    backoff_base: float = 0.8,
) -> Iterable[Dict[str, Any]]:
    """
    Yields promotion objects across ALL pages using meta.pagination.links.next.
    Handles 429/5xx with exponential backoff and honors Retry-After.
    Supports both relative and absolute 'next' links.
    """
    session = requests.Session()
    headers = bc_headers(token)

    base_url = f"{API_BASE}/stores/{store_hash}/v3/promotions"
    params = {"limit": limit}
    next_url: Optional[str] = None

    while True:
        url = next_url or base_url
        kwargs = {"headers": headers, "timeout": timeout}
        if next_url is None:
            kwargs["params"] = params

        attempt = 0
        while True:
            try:
                resp = session.get(url, **kwargs)
            except requests.RequestException as e:
                attempt += 1
                if attempt > max_retries:
                    raise
                time.sleep(backoff_base * (2 ** (attempt - 1)))
                continue

            # Retry on rate limit/server hiccups
            if resp.status_code in (429, 500, 502, 503, 504):
                attempt += 1
                if attempt > max_retries:
                    resp.raise_for_status()
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_for = float(retry_after)
                    except ValueError:
                        sleep_for = backoff_base * (2 ** (attempt - 1))
                else:
                    sleep_for = backoff_base * (2 ** (attempt - 1))
                time.sleep(sleep_for)
                continue

            resp.raise_for_status()
            break

        payload = resp.json()
        data = payload.get("data", []) or []
        for promo in data:
            yield promo

        meta = payload.get("meta") or {}
        links = (meta.get("pagination") or {}).get("links") or {}
        nxt = links.get("next")

        if not nxt:
            break

        if nxt.startswith("http"):
            # Absolute URL provided by server
            next_url = nxt
        else:
            # Relative link like "?cursor=...&limit=250"
            # Compose absolute for the next iteration
            next_url = f"{base_url}{nxt}"


def main():
    parser = argparse.ArgumentParser(description="Export BigCommerce promotions created via API to CSV.")
    parser.add_argument("--csv", default="promotions_api.csv", help="Output CSV path (default: promotions_api.csv)")
    parser.add_argument("--limit", type=int, default=250, help="Page size (default: 250)")
    parser.add_argument("--include-non-api", action="store_true",
                        help="Include promotions NOT created via API (by default we filter to created_from == 'api').")
    args = parser.parse_args()

    total_scanned = 0
    total_matched = 0
    rows: List[Dict[str, Any]] = []
    all_headers: set = set()

    for promo in fetch_promotions(STORE_HASH, ACCESS_TOKEN, limit=args.limit):
        total_scanned += 1
        created_from = str(promo.get("created_from", "")).lower()

        if args.include_non_api or created_from == "api":
            total_matched += 1
            row = normalize_row(promo)
            rows.append(row)
            all_headers.update(row.keys())

    # Write CSV with a stable column order:
    # Put a few helpful columns first if present, then the rest sorted.
    priority_order = [k for k in ["id", "name", "status", "created_at", "updated_at", "created_from"] if k in all_headers]
    remaining = sorted([h for h in all_headers if h not in priority_order])
    headers = priority_order + remaining

    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"Scanned promotions: {total_scanned}")
    if args.include_non_api:
        print(f"Exported (all promotions): {total_matched}")
    else:
        print(f"Exported (created_from == 'api'): {total_matched}")
    print(f"Wrote CSV: {args.csv}")


if __name__ == "__main__":
    # Basic guardrails for missing creds:
    if not STORE_HASH or STORE_HASH == "your_store_hash" or not ACCESS_TOKEN or ACCESS_TOKEN == "your_access_token":
        raise SystemExit("Please set STORE_HASH and ACCESS_TOKEN at the top of the script.")
    main()