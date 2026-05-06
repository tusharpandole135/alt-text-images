#!/usr/bin/env python3
"""
QA dataset generator for WebA11y alt-text caching validation.

Single command pipeline:
  1. Pull last N days of cache events from BigQuery (joined with a11y_engine for website URL).
  2. Enrich with S3 ETags (boto3).
  3. Compute is_llm_served + paired_source_image_id from cache-key timeline.
  4. Mirror images S3 -> GitHub repo (streaming, no local disk).
  5. Write QA-ready CSV.

All intermediate state cached so re-runs are cheap (don't re-pull, re-fetch ETags, or re-upload).

Usage:
  ./make_qa_dataset.py --days 3
  ./make_qa_dataset.py --days 1 --skip-upload   # CSV without GitHub URLs (fast)
"""

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, unquote

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# -------- config --------
BQ_PROJECT = "browserstack-production"
GITHUB_REPO = "tusharpandole135/alt-text-images"  # owner/repo
GITHUB_BRANCH = "main"
S3_REGION = "us-east-1"
S3_HOST_RE = re.compile(r"^([^.]+)\.s3(?:[.-][^.]+)*\.amazonaws\.com$")

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".qa_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# WebA11y prompt version composite — must match constants.js
# (used in cache key calculation for parity with production)
WEB_A11Y_PROMPT_VERSION_COMPOSITE = (
    "classification:v6|decorative:v2|alttext:v3|scoring:v6"
)
CACHE_KEY_PREFIX = "a11y:llmcache:"


def build_cache_key(base_etag, existing_alt, language):
    """Mirror of llmCache.js buildCacheKey — sha256 of joined fields."""
    if not base_etag or base_etag.startswith("ERR:"):
        return ""
    raw = f"{base_etag}|{existing_alt}|{language}|{WEB_A11Y_PROMPT_VERSION_COMPOSITE}"
    return CACHE_KEY_PREFIX + hashlib.sha256(raw.encode("utf-8")).hexdigest()

ETAG_CACHE = os.path.join(CACHE_DIR, "etag_cache.json")
GITHUB_CACHE = os.path.join(CACHE_DIR, "github_cache.json")

FINAL_FIELDS = [
    "request_id",
    "image_id",
    "job_created_at",
    "group_id",
    "website_url",
    "language",
    "base_url",
    "context_url",
    "base_image_etag",
    "context_image_etag",
    "base_image_github_url",
    "context_image_github_url",
    "existing_alt_text",
    "cache_key",
    "cache_role",
    "cache_hit",
    "is_llm_served",
    "paired_source_image_id",
    "generated_alt_text",
    "existing_alt_text_bucket",
    "image_bucket",
    # populator side-by-side columns (filled only for cache_role=served)
    "populator_website_url",
    "populator_base_url",
    "populator_context_url",
    "populator_base_image_etag",
    "populator_context_image_etag",
    "populator_base_image_github_url",
    "populator_context_image_github_url",
    "populator_existing_alt_text",
    "populator_generated_alt_text",
    "populator_group_id",
]

POPULATOR_JOIN_FIELDS = [
    "website_url",
    "base_url",
    "context_url",
    "base_image_etag",
    "context_image_etag",
    "base_image_github_url",
    "context_image_github_url",
    "existing_alt_text",
    "generated_alt_text",
    "group_id",
]


# -------- helpers --------
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_cache(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_cache(path, d):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(d, f)
    os.replace(tmp, path)


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
    return (bucket, key) if key else (None, None)


# -------- phase 1: BigQuery --------
def phase_bq_pull(days, raw_csv):
    if os.path.exists(raw_csv) and os.environ.get("FORCE_BQ") != "1":
        log(f"phase 1: cached BQ output exists at {raw_csv} (FORCE_BQ=1 to redo)")
        return
    job_window = days
    req_window = days + 1
    log(f"phase 1: pulling {job_window}d of jobs from BigQuery...")
    sql = f"""
WITH jobs AS (
  SELECT
    request_id,
    JSON_VALUE(meta_data, '$.feature_identifier.image_id') AS image_id,
    CAST(JSON_VALUE(response, '$.cacheHit') AS BOOL) AS cache_hit,
    JSON_VALUE(response, '$.altText') AS generated_alt_text,
    JSON_VALUE(response, '$.existingAltTextBucket') AS existing_alt_text_bucket,
    JSON_VALUE(response, '$.imageBucket') AS image_bucket,
    user_id,
    group_id,
    created_at AS job_created_at
  FROM `{BQ_PROJECT}.tcg_service.tcg_llm_service_req_data_partitioned`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL {job_window} DAY)
    AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {job_window * 24} HOUR)
    AND service = 'alttextGenerationWorker'
    AND JSON_VALUE(meta_data, '$.feature') = 'webAllyAlttextGeneration'
),
requests AS (
  SELECT
    request_id,
    JSON_VALUE(image, '$.id') AS image_id,
    JSON_VALUE(image, '$.baseImage') AS base_url,
    JSON_VALUE(image, '$.contextImage') AS context_url,
    JSON_VALUE(image, '$.altText') AS existing_alt_text,
    JSON_VALUE(image, '$.language') AS language
  FROM `{BQ_PROJECT}.tcg_service.tcg_llm_service_req_data_partitioned`,
       UNNEST(JSON_QUERY_ARRAY(meta_data, '$.request_body.images')) AS image
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL {req_window} DAY)
    AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {req_window * 24} HOUR)
    AND service = 'tcg'
    AND JSON_VALUE(meta_data, '$.feature') = 'webAllyAlttextGeneration'
),
engine AS (
  SELECT
    JSON_VALUE(item, '$.uuid') AS uuid,
    ANY_VALUE(JSON_VALUE(item, '$.url')) AS website_url
  FROM `{BQ_PROJECT}.a11y_engine.a11y_engine_stats_partitioned`,
       UNNEST(JSON_QUERY_ARRAY(url, '$.arr')) AS item
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL {req_window} DAY)
    AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {req_window * 24} HOUR)
  GROUP BY uuid
)
SELECT
  j.request_id, j.image_id, j.user_id, j.group_id,
  CAST(j.job_created_at AS STRING) AS job_created_at,
  r.base_url, r.context_url, r.existing_alt_text, r.language,
  j.cache_hit, j.generated_alt_text, j.existing_alt_text_bucket, j.image_bucket,
  e.website_url
FROM jobs j
LEFT JOIN requests r ON j.request_id = r.request_id AND j.image_id = r.image_id
LEFT JOIN engine e ON e.uuid = REGEXP_EXTRACT(j.request_id, r'^(.*)_\\d+$')
ORDER BY j.job_created_at DESC
"""
    proc = subprocess.run(
        [
            "bq", f"--project_id={BQ_PROJECT}", "query",
            "--use_legacy_sql=false", "--format=csv",
            "--max_rows=2000000", "--quiet",
        ],
        input=sql, text=True, capture_output=True,
    )
    if proc.returncode != 0:
        log(f"BQ FAILED: {proc.stderr[-500:]}")
        sys.exit(1)
    with open(raw_csv, "w") as f:
        f.write(proc.stdout)
    n = sum(1 for _ in open(raw_csv)) - 1
    log(f"phase 1: wrote {n} rows to {raw_csv}")


# -------- phase 2: ETag enrichment --------
def phase_etag_enrich(rows, max_workers=100):
    cache = load_cache(ETAG_CACHE)
    log(f"phase 2: ETag cache has {len(cache)} entries")

    urls_needed = set()
    for r in rows:
        for k in ("base_url", "context_url"):
            u = r.get(k) or ""
            if u and u not in cache:
                urls_needed.add(u)
    log(f"phase 2: {len(urls_needed)} new URLs to head")

    if urls_needed:
        cfg = Config(max_pool_connections=max_workers + 4,
                     retries={"max_attempts": 3, "mode": "standard"},
                     connect_timeout=10, read_timeout=15)
        s3 = boto3.client("s3", region_name=S3_REGION, config=cfg)
        lock = threading.Lock()

        def head(url):
            bucket, key = parse_s3_url(url)
            if not bucket:
                return url, ""
            try:
                resp = s3.head_object(Bucket=bucket, Key=key)
                return url, (resp.get("ETag") or "").strip('"')
            except ClientError as e:
                return url, f"ERR:{e.response.get('Error', {}).get('Code', 'X')}"
            except Exception as e:
                return url, f"ERR:{type(e).__name__}"

        done = 0
        save_every = 2000
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(head, u) for u in urls_needed]
            for fut in as_completed(futures):
                u, etag = fut.result()
                with lock:
                    cache[u] = etag
                done += 1
                if done % save_every == 0:
                    save_cache(ETAG_CACHE, cache)
                    log(f"  fetched {done}/{len(urls_needed)} ETags")
        save_cache(ETAG_CACHE, cache)

    # fill rows
    for r in rows:
        r["base_image_etag"] = cache.get(r.get("base_url") or "", "")
        r["context_image_etag"] = cache.get(r.get("context_url") or "", "")
    log(f"phase 2: done")


# -------- phase 3: pair computation --------
def phase_compute_pairs(rows):
    log("phase 3: computing cache_key, is_llm_served, paired_source_image_id...")
    # group by cache key
    groups = {}
    for r in rows:
        be = r.get("base_image_etag") or ""
        ck = build_cache_key(be, r.get("existing_alt_text") or "", r.get("language") or "")
        r["cache_key"] = ck
        if not ck:
            r["is_llm_served"] = ""
            r["paired_source_image_id"] = ""
            r["cache_role"] = ""
            continue
        groups.setdefault(ck, []).append(r)

    pair_rows = 0
    for ck, members in groups.items():
        members.sort(key=lambda r: r.get("job_created_at") or "")
        populator = next((m for m in members if (m.get("cache_hit") or "").lower() == "false"), None)
        has_hit = any((m.get("cache_hit") or "").lower() == "true" for m in members)

        for m in members:
            ch = (m.get("cache_hit") or "").lower()
            if ch == "false":
                m["is_llm_served"] = "true"
                m["paired_source_image_id"] = ""
                m["cache_role"] = "populator"  # every LLM call populates cache
            elif ch == "true":
                m["is_llm_served"] = "false"
                m["paired_source_image_id"] = populator["image_id"] if populator else ""
                m["cache_role"] = "served"
            else:
                m["is_llm_served"] = ""
                m["paired_source_image_id"] = ""
                m["cache_role"] = ""

        if has_hit:
            for m in members:
                m["_pair_relevant"] = True
                pair_rows += 1
    log(f"phase 3: identified {pair_rows} pair-relevant rows")


# -------- phase 4: GitHub mirror via git clone + bulk push --------
EXT_BY_MIME = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}


def sniff_ext(content_bytes, content_type=""):
    ct = (content_type or "").lower()
    if ct in EXT_BY_MIME:
        return EXT_BY_MIME[ct]
    b = content_bytes[:16] if content_bytes else b""
    if b[:2] == b"\xff\xd8":
        return ".jpg"
    if b[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if b[:4] == b"GIF8":
        return ".gif"
    if b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        return ".webp"
    return ".bin"


def raw_url(repo, path):
    return f"https://raw.githubusercontent.com/{repo}/{GITHUB_BRANCH}/{path}"


def run_git(args, cwd=None, check=True):
    proc = subprocess.run(["git"] + args, cwd=cwd, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {proc.stderr[-300:]}")
    return proc


def ensure_repo_clone(workdir):
    if os.path.exists(os.path.join(workdir, ".git")):
        log(f"  refreshing existing clone at {workdir}")
        run_git(["fetch", "origin", GITHUB_BRANCH], cwd=workdir)
        run_git(["reset", "--hard", f"origin/{GITHUB_BRANCH}"], cwd=workdir)
        run_git(["clean", "-fd"], cwd=workdir)
        return
    log(f"  cloning {GITHUB_REPO} to {workdir}")
    os.makedirs(os.path.dirname(workdir), exist_ok=True)
    # use gh-injected credentials via https
    token = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, check=True).stdout.strip()
    clone_url = f"https://x-access-token:{token}@github.com/{GITHUB_REPO}.git"
    run_git(["clone", "--depth=1", "--branch", GITHUB_BRANCH, clone_url, workdir])
    # ensure user identity for commits
    run_git(["config", "user.email", "tushar.p@browserstack.com"], cwd=workdir)
    run_git(["config", "user.name", "QA Dataset Bot"], cwd=workdir)


def phase_github_mirror(rows, max_workers=20, cleanup_local=False):
    cache = load_cache(GITHUB_CACHE)
    log(f"phase 4: GitHub cache has {len(cache)} entries")

    todo = {}  # etag -> s3_url
    for r in rows:
        if not r.get("_pair_relevant"):
            continue
        for url_k, etag_k in (("base_url", "base_image_etag"), ("context_url", "context_image_etag")):
            etag = r.get(etag_k) or ""
            url = r.get(url_k) or ""
            if not etag or etag.startswith("ERR:") or not url:
                continue
            if etag in cache and not cache[etag].startswith("ERR:"):
                continue
            todo.setdefault(etag, url)
    log(f"phase 4: {len(todo)} unique images to mirror")

    if not todo:
        _fill_github_urls_in_rows(rows, cache)
        return

    workdir = os.path.join(CACHE_DIR, "repo_work")
    ensure_repo_clone(workdir)

    s3 = boto3.client("s3", region_name=S3_REGION,
                      config=Config(max_pool_connections=max_workers + 4))
    lock = threading.Lock()
    written = {}  # etag -> rel_path or ERR
    fail = 0

    def fetch(etag, s3_url):
        bucket, key = parse_s3_url(s3_url)
        if not bucket:
            return etag, "ERR:bad_url"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            content = obj["Body"].read()
            ext = sniff_ext(content, obj.get("ContentType") or "")
            shard = etag[:2]
            rel = f"images/{shard}/{etag}{ext}"
            full = os.path.join(workdir, rel)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "wb") as f:
                f.write(content)
            return etag, rel
        except Exception as e:
            return etag, f"ERR:s3:{type(e).__name__}"

    done = 0
    save_every = 200
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(fetch, e, u) for e, u in todo.items()]
        for fut in as_completed(futures):
            etag, result = fut.result()
            with lock:
                written[etag] = result
                if result.startswith("ERR:"):
                    fail += 1
            done += 1
            if done % save_every == 0:
                log(f"  downloaded {done}/{len(todo)} (errors={fail})")
    log(f"  download done: {done - fail} ok, {fail} errors")

    # commit + push (one big commit)
    log("  committing + pushing...")
    run_git(["add", "images/"], cwd=workdir, check=False)
    diff = run_git(["diff", "--cached", "--name-only"], cwd=workdir, check=False)
    staged = [l for l in diff.stdout.splitlines() if l.strip()]
    if staged:
        msg = f"Add {len(staged)} images"
        run_git(["commit", "-m", msg], cwd=workdir)
        run_git(["push", "origin", GITHUB_BRANCH], cwd=workdir)
        log(f"  pushed {len(staged)} files")
    else:
        log("  nothing to push (all files already in repo)")

    # update cache
    for etag, result in written.items():
        if result.startswith("ERR:"):
            cache[etag] = result
        else:
            cache[etag] = raw_url(GITHUB_REPO, result)
    save_cache(GITHUB_CACHE, cache)

    if cleanup_local:
        log(f"  cleaning up local clone at {workdir}")
        shutil.rmtree(workdir, ignore_errors=True)
    else:
        log(f"  keeping clone at {workdir} for next run (set --cleanup-local to remove)")

    _fill_github_urls_in_rows(rows, cache)
    err = sum(1 for v in cache.values() if v.startswith("ERR:"))
    log(f"phase 4: done; cache={len(cache)} (errors={err})")


def _fill_github_urls_in_rows(rows, cache):
    for r in rows:
        for url_k, etag_k, gh_k in (
            ("base_url", "base_image_etag", "base_image_github_url"),
            ("context_url", "context_image_etag", "context_image_github_url"),
        ):
            etag = r.get(etag_k) or ""
            gh = cache.get(etag, "") if etag and not etag.startswith("ERR:") else ""
            r[gh_k] = gh if gh and not gh.startswith("ERR:") else ""


# -------- phase 5: write final CSV --------
def _join_populator_fields(rows):
    pop_lookup = {}
    for r in rows:
        if r.get("cache_role") == "populator" and r.get("_pair_relevant"):
            pop_lookup[r.get("image_id")] = r
    for r in rows:
        if r.get("cache_role") != "served":
            continue
        pop = pop_lookup.get(r.get("paired_source_image_id"))
        if not pop:
            continue
        for f in POPULATOR_JOIN_FIELDS:
            r[f"populator_{f}"] = pop.get(f, "")


def _filter_to_primary_window(rows, days):
    """Keep only rows whose job_created_at is within last `days` days."""
    import datetime
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
    out = []
    for r in rows:
        ts = r.get("job_created_at") or ""
        if not ts:
            continue
        # parse "2026-05-05 19:56:18+00" or "...UTC"
        ts_clean = ts.replace(" UTC", "+00:00").replace("+00", "+00:00") if "+00:00" not in ts else ts
        try:
            dt = datetime.datetime.fromisoformat(ts_clean)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            continue
        if dt >= cutoff:
            out.append(r)
    return out


def phase_write_final(rows, out_csv, days, pairs_only=False, hits_only=False):
    _join_populator_fields(rows)
    rows = _filter_to_primary_window(rows, days)
    if hits_only:
        rows = [r for r in rows if r.get("cache_role") == "served"]
    elif pairs_only:
        rows = [r for r in rows if r.get("_pair_relevant")]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FINAL_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    log(f"phase 5: wrote {len(rows)} rows to {out_csv}")


# -------- main --------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3,
                    help="Output window: only rows from last N days appear in CSV (default 3)")
    ap.add_argument("--populator-lookback-days", type=int, default=14,
                    help="Pull this many days of jobs from BQ for populator coverage (default 14). "
                         "Cache TTL refreshes on hit, so populators can be much older than --days.")
    ap.add_argument("--output", default="qa_dataset.csv")
    ap.add_argument("--skip-upload", action="store_true",
                    help="Skip GitHub mirroring (CSV without github_url columns filled)")
    ap.add_argument("--pairs-only", action="store_true",
                    help="Only output rows that are part of cache pairs")
    ap.add_argument("--hits-only", action="store_true",
                    help="Only output cache_role=served rows, with populator joined side-by-side")
    ap.add_argument("--cleanup-local", action="store_true",
                    help="Delete the local repo clone after pushing (slower re-runs)")
    args = ap.parse_args()

    lookback = max(args.days, args.populator_lookback_days)
    raw_csv = os.path.join(CACHE_DIR, f"raw_{lookback}d.csv")
    phase_bq_pull(args.days, lookback, raw_csv)

    log(f"loading rows from {raw_csv}...")
    with open(raw_csv) as f:
        rows = list(csv.DictReader(f))
    log(f"loaded {len(rows)} rows")

    phase_etag_enrich(rows)
    phase_compute_pairs(rows)

    if not args.skip_upload:
        phase_github_mirror(rows, cleanup_local=args.cleanup_local)
    else:
        for r in rows:
            r["base_image_github_url"] = ""
            r["context_image_github_url"] = ""

    phase_write_final(rows, args.output, pairs_only=args.pairs_only, hits_only=args.hits_only)

    # summary
    total = len(rows)
    hits = sum(1 for r in rows if (r.get("cache_hit") or "").lower() == "true")
    pairs = sum(1 for r in rows if r.get("_pair_relevant"))
    log(f"\nSUMMARY")
    log(f"  total rows: {total}")
    log(f"  cache hits: {hits} ({100 * hits / total:.2f}%)")
    log(f"  pair-relevant rows: {pairs}")
    log(f"  output: {args.output}")


if __name__ == "__main__":
    main()
