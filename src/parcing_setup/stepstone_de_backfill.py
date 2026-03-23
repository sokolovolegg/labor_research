import asyncio
import csv
import logging
import os
import re
import sys
import json
import tempfile
import shutil
import aiohttp
from bs4 import BeautifulSoup

BASE_DIR   = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
INPUT_CSV  = os.path.join(BASE_DIR, "data", "processed", "stepstone_de_jobs_dedup.csv")
LOG_FILE   = os.path.join(BASE_DIR, "logs", "stepstone_de_backfill.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

CONCURRENCY     = 10
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS  = 2
RETRY_DELAY     = 3.0
BATCH_SIZE      = 200
DRY_RUN         = "--dry-run" in sys.argv

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}

JUNK_MARKERS = [
    "verify you are human", "please enable cookies", "captcha",
    "access denied", "this page requires javascript",
    "robot or human", "checking your browser", "just a moment",
    "wayback machine doesn't have", "got an http 429 error",
    "this snapshot was not found",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", str(text)).strip() if text else ""


def _strip_html(html_text: str) -> str:
    return _clean(BeautifulSoup(html_text, "lxml").get_text(separator=" "))


def is_junk(html: str) -> bool:
    if not html or len(html.strip()) < 500:
        return True
    low = html.lower()
    return any(m in low for m in JUNK_MARKERS)


def extract_description(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # 1. JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except Exception:
            continue
        if isinstance(data, list):
            data = next((d for d in data if d.get("@type") == "JobPosting"), None)
            if not data:
                continue
        if isinstance(data, dict) and data.get("@type") == "JobPosting":
            desc = data.get("description", "")
            if desc:
                return _strip_html(desc)

    # 2. listing-content
    for cls_pattern in [
        "listing-content js-listing-content",
        "listing-content",
    ]:
        el = soup.find("div", class_=lambda c: c and "listing-content" in c)
        if el:
            for unwanted in el.find_all(class_=re.compile(r"similar|footer|apply|modal|banner", re.I)):
                unwanted.decompose()
            text = _clean(el.get_text(separator=" ", strip=True))
            if len(text) > 200:
                return text

    blocks = soup.find_all("div", class_=re.compile(r"^richtext", re.I))
    if blocks:
        text = _clean(" ".join(b.get_text(separator=" ", strip=True) for b in blocks))
        if len(text) > 200:
            return text

    for tag, attrs in [
        ("div",     {"data-at": "jobad-duties-section"}),
        ("div",     {"class": re.compile(r"job-ad-display|jobAdPage|job-description", re.I)}),
        ("section", {"itemprop": "description"}),
        ("div",     {"itemprop": "description"}),
    ]:
        el = soup.find(tag, attrs)
        if el:
            text = _clean(el.get_text(separator=" ", strip=True))
            if len(text) > 200:
                return text

    return ""


async def fetch_description(session, semaphore, row: dict, stats: dict) -> dict:
    wayback_url = row.get("wayback_url", "")
    if not wayback_url:
        return row

    async with semaphore:
        html = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                async with session.get(
                    wayback_url, headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                    allow_redirects=True,
                ) as resp:
                    if resp.status == 429:
                        log.warning(f"429 — waiting {60 * attempt}s")
                        await asyncio.sleep(60 * attempt)
                        continue
                    if resp.status != 200:
                        stats["skipped"] += 1
                        return row
                    html = await resp.text(encoding="utf-8", errors="replace")
                    break
            except (asyncio.TimeoutError, aiohttp.ClientError):
                if attempt < RETRY_ATTEMPTS:
                    await asyncio.sleep(RETRY_DELAY)

        if html is None:
            stats["errors"] += 1
            return row

        if is_junk(html):
            stats["junk"] += 1
            return row

        desc = extract_description(html)
        if desc:
            row = dict(row)
            row["description"] = desc
            stats["filled"] += 1
            if stats["filled"] % 100 == 0:
                log.info(f"  Filled: {stats['filled']:,} | "
                         f"Still empty: {stats['still_empty']:,} | "
                         f"Errors: {stats['errors']:,}")
        else:
            stats["still_empty"] += 1

        return row


async def main():
    log.info("=" * 60)
    log.info("StepStone DE — backfill descriptions")
    log.info(f"Input: {INPUT_CSV}")
    if DRY_RUN:
        log.info("Mode --dry-run, file will not be modified")
    log.info("=" * 60)

    all_rows: list[dict] = []
    fieldnames = None
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            all_rows.append(row)

    total = len(all_rows)
    no_desc  = [r for r in all_rows if not r.get("description", "").strip()]
    has_desc = [r for r in all_rows if r.get("description", "").strip()]

    log.info(f"Total records:          {total:,}")
    log.info(f"Already have description: {len(has_desc):,}")
    log.info(f"Missing description (todo): {len(no_desc):,}")

    if not no_desc or DRY_RUN:
        if DRY_RUN:
            log.info("--dry-run: exiting without changes.")
        else:
            log.info("Nothing to fill.")
        return

    # Stats
    stats = {"filled": 0, "still_empty": 0, "skipped": 0, "errors": 0, "junk": 0}

    # Process in batches
    semaphore = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY + 5, ssl=False)
    updated_rows: dict[str, dict] = {r.get("wayback_url", ""): r for r in has_desc}

    async with aiohttp.ClientSession(connector=connector) as session:
        for batch_start in range(0, len(no_desc), BATCH_SIZE):
            batch = no_desc[batch_start: batch_start + BATCH_SIZE]
            tasks = [fetch_description(session, semaphore, row, stats) for row in batch]
            results = await asyncio.gather(*tasks)
            for row in results:
                updated_rows[row.get("wayback_url", "")] = row
            done = batch_start + len(batch)
            pct = done / len(no_desc) * 100
            log.info(f"Progress: {done:,}/{len(no_desc):,} ({pct:.1f}%) | "
                     f"Filled: {stats['filled']:,}")

    # Restore original order and write file
    key_map = {r.get("wayback_url", ""): r for r in all_rows}
    key_map.update(updated_rows)
    final_rows = [key_map.get(r.get("wayback_url", ""), r) for r in all_rows]

    # Atomic write via temp file
    tmp_path = INPUT_CSV + ".tmp"
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in final_rows:
            w.writerow(row)
    shutil.move(tmp_path, INPUT_CSV)

    log.info("=" * 60)
    log.info(f"Done!")
    log.info(f"  New descriptions filled: {stats['filled']:,}")
    log.info(f"  Still missing description: {stats['still_empty']:,}")
    log.info(f"  Wayback stubs: {stats['junk']:,}")
    log.info(f"  Network errors: {stats['errors']:,}")
    log.info(f"  File updated: {INPUT_CSV}")
    log.info("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped. Partial progress NOT saved (batch not completed).")
        log.info("Run again — it will process the same records that are still missing descriptions.")