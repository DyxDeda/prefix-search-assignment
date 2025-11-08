#!/usr/bin/env python3
"""Replay a fixed subset of prefix queries against a running search API.

The script helps reviewers capture comparable snapshots for manual relevance
checks. It reads the shared `data/prefix_queries.csv`, selects the first N rows
by default (matching the open-query block), calls `/search?q=...&top_k=...`,
and writes a CSV log with the top documents plus any HTTP/JSON errors.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

DEFAULT_ENDPOINT = "/search"


def iter_queries(path: Path, limit: int | None) -> Iterable[Dict[str, str]]:
    with path.open(encoding="utf-8") as src:
        reader = csv.DictReader(src)
        for idx, row in enumerate(reader):
            if limit is not None and idx >= limit:
                break
            yield row


def call_search(url: str, query: str, top_k: int, timeout: float) -> Tuple[int, float, Dict | None, str]:
    params = {"q": query, "top_k": top_k}
    encoded = f"{url}?{urlencode(params)}"
    req = Request(encoded, headers={"Accept": "application/json"})
    start = time.perf_counter()
    body = None
    status = 0
    error = ""
    payload = None
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            status = resp.status
    except HTTPError as exc:
        status = exc.code
        error = f"HTTPError: {exc}"
        body = exc.read()
    except URLError as exc:
        error = f"URLError: {exc.reason}"
    except Exception as exc:  # pragma: no cover - defensive
        error = f"Unexpected error: {exc}"
    latency_ms = (time.perf_counter() - start) * 1000

    if body:
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            error = error or "Malformed JSON payload"

    return status, latency_ms, payload, error


def normalize_base(base: str) -> str:
    if not base:
        raise ValueError("Base URL must be provided")
    if not base.startswith(("http://", "https://")):
        raise ValueError(f"Base URL must include scheme, got: {base}")
    return base if base.endswith("/") else f"{base}/"


def build_output_path(raw: str | None) -> Path:
    if raw:
        return Path(raw)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("reports") / f"manual_sample_{timestamp}.csv"


def extract_summary(results: List[Dict], top_k: int) -> Tuple[str, str, str]:
    top = results[:top_k]
    names = " | ".join(r.get("name", "") for r in top)
    categories = " | ".join(r.get("category", "") for r in top)
    ids = " | ".join(r.get("id", "") for r in top)
    return names, categories, ids


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay queries for manual relevance checks.")
    parser.add_argument("--base-url", default="http://localhost:5000", help="Search API base URL (scheme + host + optional port).")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Path to the search endpoint (default: /search).")
    parser.add_argument("--queries", default="data/prefix_queries.csv", help="CSV with queries to replay.")
    parser.add_argument("--limit", type=int, default=20, help="Number of rows to replay (default: first 20 open queries).")
    parser.add_argument("--top-k", type=int, default=5, help="Max results requested from the API.")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout per request in seconds.")
    parser.add_argument("--output", help="Where to store the CSV log (default: reports/manual_sample_<timestamp>.csv).")
    args = parser.parse_args()

    queries_path = Path(args.queries)
    if not queries_path.exists():
        raise SystemExit(f"Queries file not found: {queries_path}")

    base = normalize_base(args.base_url)
    endpoint = args.endpoint.lstrip("/")
    full_url = urljoin(base, endpoint)

    output_path = build_output_path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, object]] = []
    successes = failures = 0

    for idx, row in enumerate(iter_queries(queries_path, args.limit), start=1):
        query = row.get("query", "")
        site = row.get("site") or row.get("store") or ""
        notes = row.get("notes", "")
        q_type = row.get("type") or row.get("expected_category") or ""

        status, latency_ms, payload, error = call_search(full_url, query, args.top_k, args.timeout)
        results = payload.get("results", []) if (payload and isinstance(payload, dict)) else []
        names, categories, ids = extract_summary(results, args.top_k)
        judgement = ""  # reviewer fills 1/0 manually

        if status == 200 and not error:
            successes += 1
        else:
            failures += 1

        rows.append({
            "idx": idx,
            "query": query,
            "site_or_store": site,
            "type": q_type,
            "notes": notes,
            "status_code": status,
            "latency_ms": f"{latency_ms:.1f}",
            "top_names": names,
            "top_categories": categories,
            "top_ids": ids,
            "raw_results_count": len(results),
            "error": error,
            "judgement": judgement,
        })

    with output_path.open("w", newline="", encoding="utf-8") as dst:
        fieldnames = list(rows[0].keys()) if rows else [
            "idx", "query", "site_or_store", "type", "notes", "status_code",
            "latency_ms", "top_names", "top_categories", "top_ids",
            "raw_results_count", "error", "judgement",
        ]
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"Saved {len(rows)} queries to {output_path}")
    print(f"HTTP 200: {successes}, errors: {failures}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
