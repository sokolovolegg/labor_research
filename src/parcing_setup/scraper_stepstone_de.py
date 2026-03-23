

import asyncio
import csv
import json
import logging
import os
import re
import time
from urllib.parse import urlparse, unquote
import aiohttp
from bs4 import BeautifulSoup
from tqdm import tqdm

# ---------------------------------------------
#  SETTINGS
# ---------------------------------------------
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR   = os.path.normpath(os.path.join(BASE_DIR, "..", ".."))
OUTPUT_CSV = os.path.join(ROOT_DIR, "data", "raw", "stepstone_de_jobs.csv")
LOG_FILE   = os.path.join(ROOT_DIR, "logs", "stepstone_de.log")

os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

CDX_URL          = "http://web.archive.org/cdx/search/cdx"
CDX_URL_PATTERNS = [
    "stepstone.de/stellenangebote--*",
    "stepstone.de/5/*",
    "stepstone.de/job/*",
]
CDX_FROM        = "20160101"
CDX_TO          = "20241231"
CONCURRENCY     = 12
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS  = 2
RETRY_DELAY     = 3.0
BATCH_SIZE      = 500  # Process URLs in batches to prevent memory overflow

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
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
    "this snapshot was not found",
]

# ---------------------------------------------
#  LOGGING
# ---------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------
#  CDX - Full Pagination
# ---------------------------------------------
def fetch_cdx_urls() -> list[dict]:
    import urllib.request
    seen: dict[str, dict] = {}
    for pattern in CDX_URL_PATTERNS:
        # Step 1: Determine total number of pages
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
            log.warning(f"CDX returned no results for: {pattern}")
            continue

        log.info(f"CDX Pattern {pattern}: {num_pages} pages (~{num_pages * 5000:,} records)")
        
        # Step 2: Download all pages
        for page in range(num_pages):
            page_url = (
                f"{CDX_URL}?url={pattern}&output=json&collapse=urlkey"
                f"&from={CDX_FROM}&to={CDX_TO}"
                f"&fl=timestamp,original&filter=statuscode:200"
                f"&page={page}"
            )
            raw = None
            for attempt in range(3):
                try:
                    with urllib.request.urlopen(page_url, timeout=90) as resp:
                        raw = json.loads(resp.read().decode("utf-8"))
                    break
                except Exception as e:
                    log.warning(f"  CDX Page {page}/{num_pages} error (Attempt {attempt+1}): {e}")
                    time.sleep(5 * (attempt + 1))
            
            if not raw or len(raw) < 2:
                log.warning(f"  CDX Page {page} is empty, skipping")
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
            log.info(f"  Page {page+1}/{num_pages}: +{new_count} new entries (Total: {len(seen):,})")
            time.sleep(0.5)

    result = list(seen.values())
    log.info(f"Total unique vacancies found after CDX: {len(result):,}")
    return result

def _is_job_url(url: str) -> bool:
    url_lower = url.lower()
    exclude = [
        "/stellenangebote?", "/stellenangebote/?",
        "/kandidaten/", "/bewerber/",
        "/unternehmen/", "/ratgeber/",
        "/gehalt/", "/karriere/",
        "stepstone.de/$", "stepstone.de/index",
        "/suche/", "/jobs?", "/jobs/?",
    ]
    for ex in exclude:
        if ex in url_lower:
            return False
    include = ["/stellenangebote--", "/5/", "/job/"]
    return any(inc in url_lower for inc in include)

# ---------------------------------------------
#  EXTRACT JOB ID
# ---------------------------------------------
def extract_job_id(url: str) -> str:
    m = re.search(r"--(\d{6,12})\.html", url)
    if m:
        return m.group(1)
    m = re.search(r"/5/(\d{6,12})", url)
    if m:
        return m.group(1)
    m = re.search(r"/job/[^/]+-(\d{6,12})/?$", url)
    if m:
        return m.group(1)
    m = re.search(r"(\d{6,12})", url)
    if m:
        return m.group(1)
    return ""

# ---------------------------------------------
#  JUNK DETECTION
# ---------------------------------------------
def is_junk_page(html: str) -> bool:
    if not html or len(html.strip()) < 500:
        return True
    text_lower = html.lower()
    return any(marker in text_lower for marker in JUNK_MARKERS)

# ---------------------------------------------
#  PARSE HTML
# ---------------------------------------------
def parse_stepstone_html(html: str, original_url: str, timestamp: str) -> dict | None:
    soup = BeautifulSoup(html, "lxml")
    job = {
        "job_id":          extract_job_id(original_url),
        "title":           "",
        "company":         "",
        "city":            "",
        "region":          "",
        "country":         "DE",
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
        
        hiringOrg = data.get("hiringOrganization", {})
        if isinstance(hiringOrg, dict):
            job["company"] = _clean(hiringOrg.get("name", ""))
        
        loc = data.get("jobLocation", {})
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        addr = loc.get("address", {}) if isinstance(loc, dict) else {}
        if isinstance(addr, dict):
            job["city"]    = _clean(addr.get("addressLocality", ""))
            job["region"]  = _clean(addr.get("addressRegion", ""))
            country        = _clean(addr.get("addressCountry", "DE"))
            job["country"] = country if country else "DE"
            
        raw_desc = data.get("description", "")
        if raw_desc:
            job["description"] = _strip_html(raw_desc)
        if job["title"]:
            return job

    # Attempt 2: HTML Fallback
    for tag, attrs in [
        ("h1", {"data-at": "job-ad-header-title"}),
        ("h1", {"class": re.compile(r"jobAd-header|job-title|listing-title", re.I)}),
        ("h1", {"itemprop": "title"}),
        ("h1", {}),
    ]:
        el = soup.find(tag, attrs)
        if el and el.get_text(strip=True):
            job["title"] = _clean(el.get_text(strip=True))
            break
            
    for tag, attrs in [
        ("span", {"data-at": "job-ad-company-name"}),
        ("span", {"class": re.compile(r"company|employer|hiring", re.I)}),
        ("a",    {"data-at": "job-ad-company-link"}),
        ("div",  {"itemprop": "hiringOrganization"}),
    ]:
        el = soup.find(tag, attrs)
        if el:
            text = el.get("content") or el.get_text(strip=True)
            if text:
                job["company"] = _clean(text)
                break
                
    for tag, attrs in [
        ("span", {"data-at": "job-ad-header-location"}),
        ("li",   {"data-at": "job-ad-header-location"}),
        ("span", {"class": re.compile(r"location|city|ort", re.I)}),
        ("meta", {"itemprop": "addressLocality"}),
    ]:
        el = soup.find(tag, attrs)
        if el:
            text = el.get("content") or el.get_text(strip=True)
            if text:
                job["city"] = _clean(text.split(",")[0])
                break
                
    for tag, attrs in [
        ("time", {}),
        ("span", {"data-at": "job-posting-date"}),
        ("meta", {"itemprop": "datePosted"}),
    ]:
        el = soup.find(tag, attrs)
        if el:
            text = el.get("datetime") or el.get("content") or el.get_text(strip=True)
            if text:
                job["date_posted"] = _clean(text)
                break
                
    for tag, attrs in [
        ("span", {"data-at": "job-ad-employment-type"}),
        ("meta", {"itemprop": "employmentType"}),
        ("li",   {"class": re.compile(r"employment|vollzeit|teilzeit", re.I)}),
    ]:
        el = soup.find(tag, attrs)
        if el:
            text = el.get("content") or el.get_text(strip=True)
            if text:
                job["employment_type"] = _clean(text)
                break
                
    for tag, attrs in [
        ("div",      {"data-at": "jobad-duties-section"}),
        ("div",      {"class": re.compile(r"job-ad-display|jobAdPage|job-description|description", re.I)}),
        ("section", {"itemprop": "description"}),
        ("div",      {"itemprop": "description"}),
    ]:
        el = soup.find(tag, attrs)
        if el:
            job["description"] = _clean(el.get_text(separator=" ", strip=True))
            break

    if not job["title"] and not job["description"]:
        return None
    return job

# ---------------------------------------------
#  UTILITIES
# ---------------------------------------------
def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()

def _strip_html(html_text: str) -> str:
    return _clean(BeautifulSoup(html_text, "lxml").get_text(separator=" "))

# ---------------------------------------------
#  RESUME SUPPORT
# ---------------------------------------------
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
        log.warning(f"Could not read CSV file: {e}")
    return done

def init_csv(csv_path: str):
    if not os.path.exists(csv_path):
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()
        log.info(f"Created new file: {csv_path}")
    else:
        log.info(f"Resuming writing to existing file: {csv_path}")

# ---------------------------------------------
#  ASYNC WORKER
# ---------------------------------------------
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
                    html = await resp.text(encoding="utf-8", errors="replace")
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
        
        job = parse_stepstone_html(html, original_url, timestamp)
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

# ---------------------------------------------
#  MAIN EXECUTION
# ---------------------------------------------
async def main():
    log.info("-" * 60)
    log.info("StepStone.de — Wayback Machine Scraper")
    log.info(f"Period: {CDX_FROM} to {CDX_TO}")
    log.info("-" * 60)

    all_entries = fetch_cdx_urls()
    if not all_entries:
        log.error("CDX returned no URLs. Exiting.")
        return

    init_csv(OUTPUT_CSV)
    done_urls = load_done_urls(OUTPUT_CSV)
    log.info(f"Already processed: {len(done_urls)}")

    todo = [e for e in all_entries if e["wayback_url"] not in done_urls]
    log.info(f"Remaining: {len(todo)} out of {len(all_entries)}")

    if not todo:
        log.info("All entries have already been processed.")
        return

    stats     = {"saved": 0, "skipped": 0, "errors": 0, "junk": 0, "empty": 0}
    semaphore = asyncio.Semaphore(CONCURRENCY)
    csv_lock  = asyncio.Lock()
    total     = len(todo)

    connector = aiohttp.TCPConnector(limit=CONCURRENCY + 5, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        with tqdm(total=total, desc="Scraping StepStone.de", unit="page") as pbar:
            for batch_start in range(0, total, BATCH_SIZE):
                batch = todo[batch_start : batch_start + BATCH_SIZE]
                tasks = [
                    fetch_and_parse(session, semaphore, entry, csv_lock, OUTPUT_CSV, stats)
                    for entry in batch
                ]
                await asyncio.gather(*tasks)
                pbar.update(len(batch))
                
                done_n = batch_start + len(batch)
                log.info(
                    f"Progress: {done_n:,}/{total:,} ({done_n / total * 100:.1f}%) | "
                    f"Saved: {stats['saved']:,} | Junk: {stats['junk']:,} | "
                    f"Errors: {stats['errors']:,}"
                )

    log.info(
        f"Completed! Saved: {stats['saved']:,} | Junk: {stats['junk']:,} | "
        f"Empty: {stats['empty']:,} | Errors: {stats['errors']:,}"
    )

# ---------------------------------------------
#  DEDUPLICATION
# ---------------------------------------------
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
    print(f"Deduplication complete: {len(rows)} entries -> {out}")

if __name__ == "__main__":
    import sys
    if "--dedup" in sys.argv:
        deduplicate()
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Interrupted by user. Progress saved.")