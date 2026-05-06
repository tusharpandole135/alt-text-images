#!/usr/bin/env python3
"""
Read alt-text BigQuery CSV and enrich each row with S3 ETags for base + context URLs.
Uses boto3 head_object (not subprocess) to keep connections pooled and avoid CLI fork-per-call overhead.

Usage:
  python3 enrich_etags.py <input_csv> <output_csv> [--max-workers N]
"""

import argparse
import csv
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, unquote

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

S3_HOST_RE = re.compile(r"^([^.]+)\.s3(?:[.-][^.]+)*\.amazonaws\.com$")

OUT_FIELDS = [
    "base_url",
    "context_url",
    "base_image_etag",
    "context_image_etag",
    "existing_alt_text",
    "language",
    "cache_hit",
    "generated_alt_text",
    "existing_alt_text_bucket",
    "image_bucket",
]


def parse_s3_url(url):
    if not url:
        return None, None
    try:
        u = urlparse(url)
    except Exception:
        return None, None
    if u.scheme not in ("http", "https"):
        return None, None
    m = S3_HOST_RE.match(u.netloc)
    if not m:
        return None, None
    bucket = m.group(1)
    key = unquote(u.path.lstrip("/"))
    if not key:
        return None, None
    return bucket, key


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input_csv")
    ap.add_argument("output_csv")
    ap.add_argument("--max-workers", type=int, default=64)
    ap.add_argument("--region", default="us-east-1")
    args = ap.parse_args()

    cfg = Config(
        max_pool_connections=args.max_workers + 4,
        retries={"max_attempts": 3, "mode": "standard"},
        connect_timeout=10,
        read_timeout=15,
    )
    s3 = boto3.client("s3", region_name=args.region, config=cfg)

    with open(args.input_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Loaded {len(rows)} rows from {args.input_csv}", flush=True)

    unique_urls = set()
    for r in rows:
        if r.get("base_url"):
            unique_urls.add(r["base_url"])
        if r.get("context_url"):
            unique_urls.add(r["context_url"])
    print(f"Unique URLs to head: {len(unique_urls)}", flush=True)

    cache = {}
    lock = threading.Lock()

    def fetch(url):
        bucket, key = parse_s3_url(url)
        if not bucket:
            return url, ""
        try:
            resp = s3.head_object(Bucket=bucket, Key=key)
            etag = (resp.get("ETag") or "").strip('"')
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "ClientError")
            etag = f"ERR:{code}"
        except Exception as e:
            etag = f"ERR:{type(e).__name__}"
        with lock:
            cache[url] = etag
        return url, etag

    done = 0
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futures = [ex.submit(fetch, u) for u in unique_urls]
        for fut in as_completed(futures):
            done += 1
            if done % 2000 == 0 or done == len(unique_urls):
                print(f"  fetched {done}/{len(unique_urls)} ETags", flush=True)

    with open(args.output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            r["base_image_etag"] = cache.get(r.get("base_url", ""), "")
            r["context_image_etag"] = cache.get(r.get("context_url", ""), "")
            writer.writerow(r)

    err = sum(1 for v in cache.values() if v.startswith("ERR:"))
    empty = sum(1 for v in cache.values() if v == "")
    ok = len(cache) - err - empty
    print(f"\nETag fetch summary: ok={ok} errors={err} empty={empty}", flush=True)
    print(f"Wrote {len(rows)} rows to {args.output_csv}", flush=True)


if __name__ == "__main__":
    main()
