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
    "auto_bucket",
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


# Lean column set sent to the QA tool (manual eval — buckets 1 & 2)
QA_OUTPUT_FIELDS = [
    "image_id",
    "request_id",
    "group_id",
    "cache_key",
    "existing_alt_text",
    "generated_alt_text",
    "language",
    # served (cache hit) row
    "base_url",
    "context_url",
    "base_image_etag",
    "context_image_etag",
    "base_image_github_url",
    "context_image_github_url",
    # populator row (joined)
    "populator_base_url",
    "populator_context_url",
    "populator_base_image_etag",
    "populator_context_image_etag",
    "populator_base_image_github_url",
    "populator_context_image_github_url",
]

# Auto-classified rows (B3 / B4) — already have their bucket pre-filled
AUTO_OUTPUT_FIELDS = QA_OUTPUT_FIELDS + [
    "bucket",
    "auto_bucket",
    "website_url",
    "populator_website_url",
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


def webally_fallback_bucket(bucket):
    """Same image is also written to the WebA11y bucket with longer retention.
    See https://browserstack.atlassian.net/wiki/spaces/AIA/pages/6194495649
    Returns the WebA11y bucket name, or None if no rewrite applies.
    """
    if not bucket or "a11y-engine-prod" not in bucket:
        return None
    return bucket.replace("a11y-engine-prod", "accessibility-prod")


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
def _fetch_etags_via_boto(urls_needed, cache, max_workers, label=""):
    """Run head_object for each URL with WebA11y bucket fallback. Mutates `cache`."""
    if not urls_needed:
        return
    cfg = Config(max_pool_connections=max_workers + 4,
                 retries={"max_attempts": 2, "mode": "standard"},
                 connect_timeout=8, read_timeout=12)
    s3 = boto3.client("s3", region_name=S3_REGION, config=cfg)
    lock = threading.Lock()

    def head(url):
        # Try WebA11y bucket FIRST — has both fresh + old objects (long retention).
        # Engine bucket is 2-day TTL only, so skipping it as primary avoids 86% of
        # 404 RTTs for a 14-day window.
        eng_bucket, key = parse_s3_url(url)
        if not eng_bucket:
            return url, ""
        wa = webally_fallback_bucket(eng_bucket) or eng_bucket
        try:
            resp = s3.head_object(Bucket=wa, Key=key)
            return url, (resp.get("ETag") or "").strip('"')
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "X")
            if code in ("404", "NoSuchKey", "NotFound") and wa != eng_bucket:
                # WebA11y miss — try engine bucket as fallback
                try:
                    resp = s3.head_object(Bucket=eng_bucket, Key=key)
                    return url, (resp.get("ETag") or "").strip('"')
                except ClientError as e2:
                    code = e2.response.get("Error", {}).get("Code", code)
            return url, f"ERR:{code}"
        except Exception as e:
            return url, f"ERR:{type(e).__name__}"

    done = 0
    save_every = 5000
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(head, u) for u in urls_needed]
        for fut in as_completed(futures):
            u, etag = fut.result()
            with lock:
                cache[u] = etag
            done += 1
            if done % save_every == 0 or done == len(urls_needed):
                save_cache(ETAG_CACHE, cache)
                log(f"  {label} fetched {done}/{len(urls_needed)} ETags")
    save_cache(ETAG_CACHE, cache)


def phase_etag_enrich(rows, max_workers=200):
    """Two-pass ETag enrichment for speed:
       Pass 1 — all base URLs (needed for cache_key grouping).
       Pass 2 — context URLs only for cache hits + their populators (after pair compute)."""
    cache = load_cache(ETAG_CACHE)
    log(f"phase 2a: ETag cache has {len(cache)} entries")

    base_needed = set()
    for r in rows:
        u = r.get("base_url") or ""
        if u and u not in cache:
            base_needed.add(u)
    log(f"phase 2a: {len(base_needed)} new BASE URLs to head")
    _fetch_etags_via_boto(base_needed, cache, max_workers, label="base")

    # Fill base etags now; context etags filled later in phase 2b
    for r in rows:
        r["base_image_etag"] = cache.get(r.get("base_url") or "", "")
        r["context_image_etag"] = cache.get(r.get("context_url") or "", "")
    log(f"phase 2a: done")


def phase_etag_enrich_context(rows, max_workers=200):
    """Pass 2: context URLs for served rows + populators of those served rows.
       Cache misses without any hit in their group don't get context ETag (saves
       ~95% of work)."""
    cache = load_cache(ETAG_CACHE)
    needed = set()
    for r in rows:
        # served (cache hit) AND populator-relevant rows
        if r.get("cache_role") in ("served",) or r.get("_pair_relevant"):
            u = r.get("context_url") or ""
            if u and u not in cache:
                needed.add(u)
    log(f"phase 2b: {len(needed)} new CONTEXT URLs to head (served + pair-relevant only)")
    _fetch_etags_via_boto(needed, cache, max_workers, label="context")
    # refresh context etags on all rows
    for r in rows:
        r["context_image_etag"] = cache.get(r.get("context_url") or "", "")


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
    "image/svg+xml": ".svg",
}


def sniff_ext(content_bytes, content_type=""):
    ct = (content_type or "").lower()
    if ct in EXT_BY_MIME:
        return EXT_BY_MIME[ct]
    head = content_bytes[:512] if content_bytes else b""
    if head[:2] == b"\xff\xd8":
        return ".jpg"
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if head[:4] == b"GIF8":
        return ".gif"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return ".webp"
    try:
        text = head.decode("utf-8", errors="ignore").lstrip().lower()
        if text.startswith("<svg") or "<svg" in text[:200]:
            return ".svg"
    except Exception:
        pass
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

    # Build set of QA-target rows: served (cache hit) rows that are manual_eval
    # bucket and not BrowserStack internal (group_id 2). Plus the populator they
    # paired with. We mirror images only for these — saves huge time on big pulls.
    qa_target_image_ids = set()
    populator_ids_needed = set()
    for r in rows:
        if r.get("cache_role") != "served":
            continue
        if r.get("auto_bucket") != "manual_eval":
            continue
        if str(r.get("group_id") or "").strip() == "2":
            continue
        qa_target_image_ids.add(r.get("image_id") or "")
        psi = r.get("paired_source_image_id") or ""
        if psi:
            populator_ids_needed.add(psi)

    qa_relevant_ids = qa_target_image_ids | populator_ids_needed
    log(f"phase 4: QA-target rows: {len(qa_target_image_ids)} served + {len(populator_ids_needed)} populator image_ids")

    todo = {}  # etag -> s3_url
    for r in rows:
        if r.get("image_id") not in qa_relevant_ids:
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
        # Nothing new to upload — skip clone/push entirely (saves minutes).
        log("phase 4: nothing to upload, skipping git clone/push")
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
        eng_bucket, key = parse_s3_url(s3_url)
        if not eng_bucket:
            return etag, "ERR:bad_url"
        wa = webally_fallback_bucket(eng_bucket) or eng_bucket

        def get(b, k):
            obj = s3.get_object(Bucket=b, Key=k)
            return obj["Body"].read(), (obj.get("ContentType") or "")

        try:
            try:
                content, ct = get(wa, key)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in ("404", "NoSuchKey", "NotFound") and wa != eng_bucket:
                    content, ct = get(eng_bucket, key)
                else:
                    raise
            ext = sniff_ext(content, ct)
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


def _normalize_url_parts(url):
    """Return (hostname, path) lowercase, no trailing slash. None on parse error."""
    if not url:
        return None, None
    try:
        u = urlparse(url)
    except Exception:
        return None, None
    host = (u.hostname or "").lower()
    path = (u.path or "").rstrip("/")
    return host, path


def _compute_auto_bucket(served_url, pop_url):
    """Classify the served-vs-populator pair from website URLs alone.
    Returns:
      B3_diff_website   = different domain
      B4_diff_webpage   = same domain, different path
      manual_eval       = same domain + same path (QA decides B1 same-context vs B2 different-context)
      unknown           = website URL missing on one side
    """
    s_host, s_path = _normalize_url_parts(served_url)
    p_host, p_path = _normalize_url_parts(pop_url)
    if not s_host or not p_host:
        return "unknown"
    if s_host != p_host:
        return "B3_diff_website"
    if s_path != p_path:
        return "B4_diff_webpage"
    return "manual_eval"


def phase_compute_auto_bucket(rows):
    log("phase 3.5: auto-classifying buckets from URLs...")
    counts = {"B3_diff_website": 0, "B4_diff_webpage": 0, "manual_eval": 0, "unknown": 0, "n/a": 0}
    for r in rows:
        if r.get("cache_role") != "served":
            r["auto_bucket"] = ""
            counts["n/a"] += 1
            continue
        b = _compute_auto_bucket(r.get("website_url"), r.get("populator_website_url"))
        r["auto_bucket"] = b
        counts[b] = counts.get(b, 0) + 1
    log(f"phase 3.5: auto-bucket distribution: {counts}")


def _filter_to_days(rows, days):
    """Keep only rows whose job_created_at is within last `days` days."""
    import datetime
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
    out = []
    for r in rows:
        ts = (r.get("job_created_at") or "").strip()
        if not ts:
            continue
        ts_clean = ts.replace(" UTC", "+00:00")
        if "+00:00" not in ts_clean and ts_clean.endswith("+00"):
            ts_clean = ts_clean[:-3] + "+00:00"
        try:
            dt = datetime.datetime.fromisoformat(ts_clean)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            continue
        if dt >= cutoff:
            out.append(r)
    return out


def _has_all_4_images(r):
    return all((r.get(k) or "").startswith("http") for k in (
        "base_image_github_url", "context_image_github_url",
        "populator_base_image_github_url", "populator_context_image_github_url"))


def _write_csv(rows, out_csv, fields):
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def phase_write_final(rows, out_csv, auto_csv, days, pairs_only=False, hits_only=False, qa_only=False):
    _join_populator_fields(rows)
    rows = _filter_to_days(rows, days)

    if qa_only:
        # Two outputs:
        #   1. qa_for_eval.csv  — manual-eval candidates (B1/B2): same site + same page
        #      Excludes group_id=2 (BS internal). Strict: all 4 images visible.
        #   2. auto_classified.csv  — pre-classified B3 (cross-site) + B4 (cross-page)
        is_real_customer = lambda r: str(r.get("group_id") or "").strip() != "2"
        manual_rows = [r for r in rows
                       if r.get("auto_bucket") == "manual_eval"
                       and is_real_customer(r)
                       and _has_all_4_images(r)]
        auto_rows = [r for r in rows
                     if r.get("auto_bucket") in ("B3_diff_website", "B4_diff_webpage")
                     and is_real_customer(r)]
        # pre-fill bucket for auto rows (QA gets a complete picture per row)
        bucket_map = {"B3_diff_website": "B3", "B4_diff_webpage": "B4"}
        for r in auto_rows:
            r["bucket"] = bucket_map.get(r.get("auto_bucket"), "")
        _write_csv(manual_rows, out_csv, QA_OUTPUT_FIELDS)
        _write_csv(auto_rows, auto_csv, AUTO_OUTPUT_FIELDS)
        log(f"phase 5: wrote {len(manual_rows)} rows to {out_csv} (QA manual eval)")
        log(f"phase 5: wrote {len(auto_rows)} rows to {auto_csv} (auto-classified B3/B4)")
        return

    if hits_only:
        rows = [r for r in rows if r.get("cache_role") == "served"]
    elif pairs_only:
        rows = [r for r in rows if r.get("_pair_relevant")]
    _write_csv(rows, out_csv, FINAL_FIELDS)
    log(f"phase 5: wrote {len(rows)} rows to {out_csv}")


# -------- main --------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3,
                    help="Window: pull last N days of cache events (default 3)")
    ap.add_argument("--output", default="qa_for_eval.csv",
                    help="Output CSV path for QA manual eval (B1/B2 candidates)")
    ap.add_argument("--auto-output", default="auto_classified.csv",
                    help="Output CSV path for rows auto-classified as B3 (cross-site) "
                         "or B4 (cross-page). Only written in --qa-only mode.")
    ap.add_argument("--skip-upload", action="store_true",
                    help="Skip GitHub mirroring (CSV without github_url columns filled)")
    ap.add_argument("--pairs-only", action="store_true",
                    help="Only output rows that are part of cache pairs")
    ap.add_argument("--hits-only", action="store_true",
                    help="Only output cache_role=served rows, with populator joined side-by-side")
    ap.add_argument("--qa-only", action="store_true",
                    help="Only output rows that need QA's manual eval (same website + same webpage). "
                         "Auto-classifies cross-site / cross-page pairs and excludes group_id=2.")
    ap.add_argument("--cleanup-local", action="store_true", default=False,
                    help="Delete the local repo clone after pushing (saves ~4 GB disk). "
                         "Default OFF — keeps clone for fast re-runs. Use only if disk is tight.")
    args = ap.parse_args()

    raw_csv = os.path.join(CACHE_DIR, f"raw_{args.days}d.csv")
    phase_bq_pull(args.days, raw_csv)

    log(f"loading rows from {raw_csv}...")
    with open(raw_csv) as f:
        rows = list(csv.DictReader(f))
    log(f"loaded {len(rows)} rows")

    phase_etag_enrich(rows)            # pass 1: base URLs only
    phase_compute_pairs(rows)
    _join_populator_fields(rows)        # populator data side-by-side (uses image_id index)
    phase_compute_auto_bucket(rows)     # B2/B3 classification needs populator_website_url
    phase_etag_enrich_context(rows)    # pass 2: context URLs of served + populators only

    if not args.skip_upload:
        phase_github_mirror(rows, cleanup_local=args.cleanup_local)
    else:
        # Still populate github_url columns from existing cache; just don't upload
        # anything new (no clone, no push). Images already mirrored will resolve.
        gh_cache = load_cache(GITHUB_CACHE)
        log(f"--skip-upload: using existing GitHub cache ({len(gh_cache)} entries)")
        _fill_github_urls_in_rows(rows, gh_cache)

    phase_write_final(rows, args.output, args.auto_output, args.days,
                      pairs_only=args.pairs_only,
                      hits_only=args.hits_only,
                      qa_only=args.qa_only)

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
