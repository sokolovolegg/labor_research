import aiohttp
import asyncio
import requests
import json
import csv
import os
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm

# SETUP
LIMIT = None  
CONCURRENCY = 15  
OUTPUT_FOLDER = "data"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "ch_data.csv")

def get_target_urls():
    print("1. Fetching all available pages from the Archive...")
    api_url = "http://web.archive.org/cdx/search/cdx"
    
    patterns = [
        "www.jobs.ch/en/vacancies/detail/*",        
        "www.jobs.ch/de/stellenangebote/detail/*",  
        "www.jobs.ch/fr/offres-emplois/detail/*"              
    ]
    
    urls = []
    for pattern in patterns:
        params = {
            "url": pattern, 
            "output": "json",
            "collapse": "urlkey",
            "from": "20160101",
            "to": "20241231"
        }
        
        try:
            res = requests.get(api_url, params=params)
            if res.status_code == 200:
                data = res.json()[1:]
                for row in data:
                    timestamp, original_url = row[1], row[2]
                    urls.append(f"https://web.archive.org/web/{timestamp}/{original_url}")
        except Exception as e:
            print(f"Error fetching {pattern}: {e}")
            
    print(f"\nTOTAL URLs found in Archive: {len(urls)}")
    return urls[:LIMIT] if LIMIT else urls

async def fetch_and_parse(session, url, semaphore, file_lock, csv_headers):
    async with semaphore: 
        for attempt in range(2): # 2 attempts in case of failure
            try:
                timeout = aiohttp.ClientTimeout(total=45) # 45 sec timeout
                async with session.get(url, timeout=timeout) as response:
                    if response.status != 200:
                        continue 
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    scripts = soup.find_all('script', type='application/ld+json')
                    for script in scripts:
                        try:
                            data = json.loads(script.string)
                            if isinstance(data, dict):
                                data = [data]
                                
                            for item in data:
                                if item.get('@type') == 'JobPosting':
                                    raw_desc = item.get('description', '')
                                    clean_desc = BeautifulSoup(raw_desc, "html.parser").get_text(separator=" ", strip=True)
                                    
                                    job_data = {
                                        'archive_url': url,
                                        'title': item.get('title', ''),
                                        'company': item.get('hiringOrganization', {}).get('name', ''),
                                        'date_posted': item.get('datePosted'),
                                        'valid_through': item.get('validThrough'),
                                        'employment_type': item.get('employmentType'),
                                        'city': item.get('jobLocation', {}).get('address', {}).get('addressLocality', ''),
                                        'postal_code': item.get('jobLocation', {}).get('address', {}).get('postalCode', ''),
                                        'description_preview': clean_desc
                                    }
                                    
                                    async with file_lock:
                                        with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                                            writer = csv.DictWriter(f, fieldnames=csv_headers)
                                            writer.writerow(job_data)
                                    return 
                        except Exception:
                            continue
                    return 
            except Exception:
                await asyncio.sleep(3)
        return

async def main():
    urls = get_target_urls()
    if not urls:
        return

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    csv_headers = ['archive_url', 'title', 'company', 'date_posted', 'valid_through', 'employment_type', 'city', 'postal_code', 'description_preview']
    
    # --- RESUME BLOCK (CHECKPOINTING) ---
    processed_urls = set()
    file_exists = os.path.exists(OUTPUT_FILE)
    
    if file_exists:
        print("\n2. Checking already downloaded data...")
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    processed_urls.add(row['archive_url'])
            print(f" -> Found {len(processed_urls)} already processed URLs in the file.")
        except Exception as e:
            print(f"Error reading file: {e}")
            
    # Keep only URLs that are not yet in the file
    urls_to_download = [url for url in urls if url not in processed_urls]
    print(f"\n3. Remaining to download: {len(urls_to_download)} URLs.")
    
    if not urls_to_download:
        print("\nAll data has been collected! Parsing is complete.")
        return

    print(f"\n4. Starting the download process...")
    
    # Open in append mode
    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers)
        if not file_exists or os.path.getsize(OUTPUT_FILE) == 0:
            writer.writeheader()
    
    semaphore = asyncio.Semaphore(CONCURRENCY)
    file_lock = asyncio.Lock() 
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_parse(session, url, semaphore, file_lock, csv_headers) for url in urls_to_download]
        await tqdm.gather(*tasks, desc="Downloading", unit="page")
        
    print(f"\n5. Scraping complete! All data saved in '{OUTPUT_FILE}'.")

if __name__ == '__main__':
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # --- GRACEFUL PAUSE HANDLING ---
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n[PAUSED] Scraper stopped by user. Don't worry, all progress is saved.")
        print("Run the script again later to resume exactly where you left off!")