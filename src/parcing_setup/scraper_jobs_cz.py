"""
Jobs.cz — Wayback Machine Historical Job Scraper
=================================================
Czech Republic, period 2016–2024.
Largest Czech job board.

URL Pattern:
  /rpd/<job_id>/   — main format

Usage:
  python scraper_jobs_cz.py
  python scraper_jobs_cz.py --dedup
"""

import asyncio
import csv
import json
import logging
import os
import re
import time
import warnings

import aiohttp
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from tqdm.asyncio import tqdm

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# ─────────────────────────────────────────────
#  SETTINGS
# ─────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", ".."))

OUTPUT_CSV = os.path.join(ROOT_DIR, "data", "raw", "jobs_cz_jobs.csv")
LOG_FILE   = os.path.join(ROOT_DIR, "logs", "jobs_cz.log")

os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

CDX_URL          = "http://web.archive.org/cdx/search/cdx"
CDX_URL_PATTERNS = [
    "jobs.cz/rpd/*",
]
CDX_FROM        = "20160101"
CDX_TO          = "20241231"

CONCURRENCY     = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS  = 2
RETRY_DELAY     = 3.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
}

CSV_FIELDS = [
    "job_id", "title", "company", "city", "region", "country",
    "employment_type", "date_posted", "description",
    "wayback_url", "original_url", "timestamp",
]

JUNK_MARKERS = [
    "verify you are human", "please enable cookies", "captcha",
    "access denied", "this page requires javascript", "enable javascript",
    "robot or human", "checking your browser", "cf-browser-verification",
    "ddos-guard", "please wait... | cloudflare", "attention required! | cloudflare",
    "just a moment", "wayback machine doesn't have", "got an http 429 error",
    "this snapshot was not found", "stránka nebyla nalezena", "404",
    "inzerát byl smazán", "tato nabídka již není aktivní",
]

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  CDX — FULL PAGINATION
# ─────────────────────────────────────────────
def fetch_cdx_urls() -> list[dict]:
    import urllib.request
    seen: dict[str, dict] = {}

    for pattern in CDX_URL_PATTERNS:
        pages_url = (
            f"{CDX_URL}?url={pattern}&output=json&collapse=urlkey"
            f"&from={CDX_FROM}&to={CDX_TO}"
            f"&filter=statuscode:200&showNumPages=true"
        )
        try:
            with urllib.request.urlopen(pages_url, timeout=60) as resp:
                raw = json.loads(resp.read().decode("utf-8"))
            num_pages = int(raw[-1][0]) if len(raw) >= 2 else 0
        except Exception as e:
            log.error(f"CDX showNumPages error for {pattern}: {e}")
            continue

        if num_pages == 0:
            log.warning(f"CDX empty: {pattern}")
            continue

        log.info(f"CDX pattern {pattern}: {num_pages} pages (~{num_pages * 5000:,} records)")

        for page in range(num_pages):
            page_url = (
                f"{CDX_URL}?url={pattern}&output=json&collapse=urlkey"
                f"&from={CDX_FROM}&to={CDX_TO}"
                f"&fl=timestamp,original&filter=statuscode:200"
                f"&page={page}"
            )
            for attempt in range(3):
                try:
                    with urllib.request.urlopen(page_url, timeout=90) as resp:
                        raw = json.loads(resp.read().decode("utf-8"))
                    break
                except Exception as e:
                    log.warning(f"  CDX page {page}/{num_pages} error (attempt {attempt+1}): {e}")
                    time.sleep(5 * (attempt + 1))
                    raw = None

            if not raw or len(raw) < 2:
                log.warning(f"  CDX page {page} empty, skipping")
                continue

            rows = raw[1:]
            new_count = 0
            for ts, orig in rows:
                if not _is_job_url(orig):
                    continue
                if orig not in seen or ts < seen[orig]["timestamp"]:
                    seen[orig] = {
                        "timestamp":    ts,
                        "original_url": orig,
                        "wayback_url":  f"https://web.archive.org/web/{ts}/{orig}",
                    }
                    new_count += 1

            log.info(f"  Page {page+1}/{num_pages}: +{new_count} new (total: {len(seen):,})")
            time.sleep(0.5)

    result = list(seen.values())
    log.info(f"Total unique vacancies after CDX: {len(result):,}")
    return result


def _is_job_url(url: str) -> bool:
    url_lower = url.lower()
    exclude = [
        "/rpd/$", "jobs.cz/$", "jobs.cz/index",
        "/search/", "/hledani/", "/brigady/",
        "?page=", "?rps=",
    ]
    for ex in exclude:
        if ex in url_lower:
            return False
    # Valid job: /rpd/<numeric id>/
    return bool(re.search(r"/rpd/\d{7,12}", url_lower))


# ─────────────────────────────────────────────
#  JOB ID EXTRACTION
# ─────────────────────────────────────────────
def extract_job_id(url: str) -> str:
    m = re.search(r"/rpd/(\d{7,12})", url)
    if m:
        return m.group(1)
    return ""


# ─────────────────────────────────────────────
#  JUNK DETECTION
# ─────────────────────────────────────────────
def is_junk_page(html: str) -> bool:
    if not html or len(html.strip()) < 500:
        return True
    text_lower = html.lower()
    return any(marker in text_lower for marker in JUNK_MARKERS)


# ─────────────────────────────────────────────
#  PARSING
# ─────────────────────────────────────────────
def parse_jobs_cz_html(html: str, original_url: str, timestamp: str) -> dict | None:
    soup = BeautifulSoup(html, "lxml")

    job = {
        "job_id":          extract_job_id(original_url),
        "title":           "",
        "company":         "",
        "city":            "",
        "region":          "",
        "country":         "CZ",
        "employment_type": "",
        "date_posted":     "",
        "description":     "",
        "wayback_url":     f"https://web.archive.org/web/{timestamp}/{original_url}",
        "original_url":    original_url,
        "timestamp":       timestamp,
    }

    # Attempt 1: JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(data, list):
            data = next((d for d in data if d.get("@type") == "JobPosting"), None)
            if not data:
                continue
        if data.get("@type") != "JobPosting":
            continue

        job["title"]           = _clean(data.get("title", ""))
        job["date_posted"]     = _clean(data.get("datePosted", ""))
        job["employment_type"] = _clean(data.get("employmentType", ""))

        org = data.get("hiringOrganization", {})
        if isinstance(org, dict):
            job["company"] = _clean(org.get("name", ""))

        loc = data.get("jobLocation", {})
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        addr = loc.get("address", {}) if isinstance(loc, dict) else {}
        if isinstance(addr, dict):
            job["city"]   = _clean(addr.get("addressLocality", ""))
            job["region"] = _clean(addr.get("addressRegion", ""))

        raw_desc = data.get("description", "")
        if raw_desc:
            job["description"] = _strip_html(raw_desc)

        if job["title"]:
            return job

    # Attempt 2: HTML Selectors
    for tag, attrs in [
        ("h1", {"class": re.compile(r"title|nadpis|position|job", re.I)}),
        ("h1", {"itemprop": "title"}),
        ("h1", {}),
    ]:
        el = soup.find(tag, attrs)
        if el and el.get_text(strip=True):
            job["title"] = _clean(el.get_text(strip=True))
            break

    for tag, attrs in [
        ("span", {"itemprop": "name"}),
        ("div",  {"class": re.compile(r"company|employer|spolecnost|firma", re.I)}),
        ("a",    {"class": re.compile(r"company|employer", re.I)}),
        ("strong", {"class": re.compile(r"company|employer", re.I)}),
    ]:
        el = soup.find(tag, attrs)
        if el and el.get_text(strip=True):
            text = el.get_text(strip=True)
            if len(text) < 100:
                job["company"] = _clean(text)
                break

    for tag, attrs in [
        ("span", {"itemprop": "addressLocality"}),
        ("li",   {"class": re.compile(r"location|address|adresa|mesto", re.I)}),
        ("span", {"class": re.compile(r"location|address|adresa", re.I)}),
        ("meta", {"itemprop": "addressLocality"}),
    ]:
        el = soup.find(tag, attrs)
        if el:
            text = el.get("content") or el.get_text(strip=True)
            if text:
                job["city"] = _clean(text.split(",")[0])
                break

    for tag, attrs in [
        ("span", {"itemprop": "employmentType"}),
        ("meta", {"itemprop": "employmentType"}),
        ("li",   {"class": re.compile(r"employment|uvazek|plny|castecny", re.I)}),
        ("div",  {"class": re.compile(r"employment.type|job.type", re.I)}),
    ]:
        el = soup.find(tag, attrs)
        if el:
            text = el.get("content") or el.get_text(strip=True)
            if text and len(text) < 60:
                job["employment_type"] = _clean(text)
                break

    for tag, attrs in [
        ("time", {"itemprop": "datePosted"}),
        ("time", {}),
        ("meta", {"itemprop": "datePosted"}),
        ("span", {"class": re.compile(r"date|datum|posted", re.I)}),
    ]:
        el = soup.find(tag, attrs)
        if el:
            text = el.get("datetime") or el.get("content") or el.get_text(strip=True)
            if text:
                job["date_posted"] = _clean(text)
                break

    for tag, attrs in [
        ("div",  {"itemprop": "description"}),
        ("div",  {"class": re.compile(r"description|popis|job-detail|offer-detail", re.I)}),
        ("div",  {"id": re.compile(r"description|popis|job-detail", re.I)}),
        ("section", {"class": re.compile(r"description|detail", re.I)}),
    ]:
        el = soup.find(tag, attrs)
        if el:
            job["description"] = _clean(el.get_text(separator=" ", strip=True))
            break

    # Fallback to main content
    if not job["description"]:
        main = soup.find("main") or soup.find("article") or soup.find("div", {"id": "content"})
        if main:
            job["description"] = _clean(main.get_text(separator=" ", strip=True))[:3000]

    if not job["title"] and not job["description"]:
        return None

    return job


# ─────────────────────────────────────────────
#  UTILS
# ─────────────────────────────────────────────
def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _strip_html(html_text: str) -> str:
    return _clean(BeautifulSoup(html_text, "lxml").get_text(separator=" "))


# ─────────────────────────────────────────────
#  RESUME
# ─────────────────────────────────────────────
def load_done_urls(csv_path: str) -> set[str]:
    done = set()
    if not os.path.exists(csv_path):
        return done
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                url = row.get("wayback_url", "").strip()
                if url:
                    done.add(url)
    except Exception as e:
        log.warning(f"Failed to read CSV: {e}")
    return done


def init_csv(csv_path: str):
    if not os.path.exists(csv_path):
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()
        log.info(f"Created file: {csv_path}")
    else:
        log.info(f"Resuming file: {csv_path}")


# ─────────────────────────────────────────────
#  ASYNC WORKER
# ─────────────────────────────────────────────
async def fetch_and_parse(session, semaphore, entry, csv_lock, csv_path, stats):
    wayback_url  = entry["wayback_url"]
    original_url = entry["original_url"]
    timestamp    = entry["timestamp"]

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
                        await asyncio.sleep(60 * attempt)
                        continue
                    if resp.status != 200:
                        stats["skipped"] += 1
                        return
                    html = await resp.text(errors="replace")
                    break
            except asyncio.TimeoutError:
                if attempt < RETRY_ATTEMPTS:
                    await asyncio.sleep(RETRY_DELAY)
            except aiohttp.ClientError:
                if attempt < RETRY_ATTEMPTS:
                    await asyncio.sleep(RETRY_DELAY)

        if html is None:
            stats["errors"] += 1
            return
        if is_junk_page(html):
            stats["junk"] += 1
            return

        job = parse_jobs_cz_html(html, original_url, timestamp)
        if job is None:
            stats["empty"] += 1
            return

        async with csv_lock:
            with open(csv_path, "a", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow(job)
            stats["saved"] += 1

        log.info(
            f"[{stats['saved']}] {job['title'][:50]!r} | "
            f"{job['company'][:30]!r} | {job['city']} | {job['date_posted']}"
        )


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
async def main():
    log.info("=" * 60)
    log.info("Jobs.cz — Wayback Machine Scraper")
    log.info(f"Period: {CDX_FROM} – {CDX_TO}")
    log.info("=" * 60)

    all_entries = fetch_cdx_urls()
    if not all_entries:
        log.error("CDX returned no URLs. Exiting.")
        return

    init_csv(OUTPUT_CSV)
    done_urls = load_done_urls(OUTPUT_CSV)
    log.info(f"Already processed: {len(done_urls)}")

    todo = [e for e in all_entries if e["wayback_url"] not in done_urls]
    log.info(f"Remaining: {len(todo)} of {len(all_entries)}")

    if not todo:
        log.info("Finished.")
        return

    stats = {"saved": 0, "skipped": 0, "errors": 0, "junk": 0, "empty": 0}
    semaphore = asyncio.Semaphore(CONCURRENCY)
    csv_lock  = asyncio.Lock()

    connector = aiohttp.TCPConnector(limit=CONCURRENCY + 5, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_and_parse(session, semaphore, e, csv_lock, OUTPUT_CSV, stats)
            for e in todo
        ]
        try:
            await tqdm.gather(*tasks, desc="Scraping Jobs.cz", unit="page")
        except KeyboardInterrupt:
            log.info("Paused by Ctrl+C.")

    log.info(f"Done! Saved: {stats['saved']} | Junk: {stats['junk']} | "
             f"Empty: {stats['empty']} | Errors: {stats['errors']}")


# ─────────────────────────────────────────────
#  DEDUPLICATION
# ─────────────────────────────────────────────
def deduplicate():
    if not os.path.exists(OUTPUT_CSV):
        print(f"File not found: {OUTPUT_CSV}")
        return
    rows: dict[str, dict] = {}
    with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            jid = row.get("job_id", "").strip() or row.get("original_url", "")
            ts  = row.get("timestamp", "")
            if jid not in rows or ts < rows[jid]["timestamp"]:
                rows[jid] = row
    out = OUTPUT_CSV.replace(".csv", "_dedup.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for row in sorted(rows.values(), key=lambda r: r.get("timestamp", "")):
            w.writerow(row)
    print(f"Deduplication complete: {len(rows)} records → {out}")


if __name__ == "__main__":
    import sys
    if "--dedup" in sys.argv:
        deduplicate()
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Stopped. Progress saved.")