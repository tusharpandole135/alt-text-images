#!/usr/bin/env python3
"""
One-shot fix: sniff magic bytes of every .bin file in the alt-text-images repo,
rename to correct extension via `git mv`, and update github_cache.json so future
runs use the corrected URLs.
"""
import json
import os
import subprocess
import sys

REPO_DIR = "/Users/tusharpandole/misc-services/.qa_cache/repo_work"
GITHUB_REPO = "tusharpandole135/alt-text-images"
BRANCH = "main"
CACHE_PATH = "/Users/tusharpandole/misc-services/.qa_cache/github_cache.json"


def sniff_ext(path):
    with open(path, "rb") as f:
        head = f.read(512)
    if head[:2] == b"\xff\xd8":
        return ".jpg"
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if head[:4] == b"GIF8":
        return ".gif"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return ".webp"
    # SVG: text-based, look for <svg in first 512 bytes (after possible BOM/XML declaration)
    try:
        text = head.decode("utf-8", errors="ignore").lstrip().lower()
        if text.startswith("<svg") or "<svg" in text[:200]:
            return ".svg"
    except Exception:
        pass
    return None


def main():
    img_dir = os.path.join(REPO_DIR, "images")
    if not os.path.isdir(img_dir):
        print(f"Repo not found at {img_dir}")
        sys.exit(1)

    rename_map = {}  # old_relpath -> new_relpath
    skipped = 0

    for root, _, files in os.walk(img_dir):
        for fn in files:
            if not fn.endswith(".bin"):
                continue
            full = os.path.join(root, fn)
            new_ext = sniff_ext(full)
            if not new_ext:
                skipped += 1
                continue
            new_full = full[:-4] + new_ext
            rel_old = os.path.relpath(full, REPO_DIR)
            rel_new = os.path.relpath(new_full, REPO_DIR)
            rename_map[rel_old] = rel_new

    print(f"Found {len(rename_map)} .bin files to rename ({skipped} skipped — unknown magic).")
    if not rename_map:
        return

    for old, new in rename_map.items():
        subprocess.run(["git", "mv", old, new], cwd=REPO_DIR, check=True)
    print(f"git mv done.")

    # commit + push
    subprocess.run(["git", "commit", "-m", f"fix: rename {len(rename_map)} .bin files to proper image extensions"],
                   cwd=REPO_DIR, check=True)
    subprocess.run(["git", "push", "origin", BRANCH], cwd=REPO_DIR, check=True)
    print(f"Pushed.")

    # update github_cache.json
    with open(CACHE_PATH) as f:
        cache = json.load(f)
    BASE = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{BRANCH}/"
    fixed = 0
    for etag, url in list(cache.items()):
        if not url.startswith(BASE):
            continue
        rel = url[len(BASE):]
        if rel in rename_map:
            cache[etag] = BASE + rename_map[rel]
            fixed += 1
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f)
    print(f"Updated {fixed} entries in github_cache.json")


if __name__ == "__main__":
    main()
