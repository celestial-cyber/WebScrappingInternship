#!/usr/bin/env python3
"""
CollegeDunia Web Scraper – Stage 1 (Profile URL Collection)

Objective:
Collect and deduplicate all college profile URLs across streams
to enable complete scraping of ~20,558 colleges.

Author: Internship Scraping Assistant
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin
import random

# ============ CONFIGURATION ============

BASE_URL = "https://collegedunia.com"

STREAM_URLS = {
    "Architecture": "https://collegedunia.com/architecture/architectural-planning-colleges",
    "Engineering": "https://collegedunia.com/engineering/engineering-colleges",
    "Management": "https://collegedunia.com/management/management-colleges",
    "Science": "https://collegedunia.com/science/science-colleges",
    "Commerce": "https://collegedunia.com/commerce/commerce-colleges",
    "Arts": "https://collegedunia.com/arts/arts-colleges",
}

MAX_PAGES_PER_STREAM = 1500
REQUEST_TIMEOUT = 15
SLEEP_MIN = 1
SLEEP_MAX = 3

HEADERS_LIST = [
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    },
]

# ============ LOGGING ============

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("scrape_log.txt", mode="w", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ============ HELPERS ============

def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(random.choice(HEADERS_LIST))
    return session


def polite_sleep():
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


def fetch_page(
    session: requests.Session,
    url: str,
    params: Optional[dict] = None
) -> Optional[BeautifulSoup]:
    try:
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.warning(f"Non-200 status {resp.status_code} for URL: {resp.url}")
            return None
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as e:
        logger.error(f"Request failed for {url}: {e}")
        return None


def extract_profile_urls(soup: BeautifulSoup) -> List[str]:
    urls = set()
    for a in soup.select('a[href^="/college/"]'):
        href = a.get("href")
        if href:
            clean = href.split("?")[0]
            urls.add(urljoin(BASE_URL, clean))
    return list(urls)


# ============ SCRAPER CLASS ============

class CollegeDuniaScraper:
    def __init__(self):
        self.session = get_session()
        self.start_time = datetime.now()

    def collect_college_urls(self) -> pd.DataFrame:
        collected: List[Dict[str, str]] = []

        for stream, base_url in STREAM_URLS.items():
            logger.info(f"Starting stream: {stream}")
            empty_pages = 0

            for page in range(1, MAX_PAGES_PER_STREAM + 1):
                polite_sleep()
                params = {"page": page} if page > 1 else None
                logger.info(f"[{stream}] Scraping page {page}")

                soup = fetch_page(self.session, base_url, params)
                if not soup:
                    empty_pages += 1
                    if empty_pages >= 3:
                        break
                    continue

                urls = extract_profile_urls(soup)
                if not urls:
                    empty_pages += 1
                    if empty_pages >= 3:
                        break
                else:
                    empty_pages = 0
                    for u in urls:
                        collected.append({
                            "college_profile_url": u,
                            "stream": stream,
                            "source_listing_url": base_url
                        })

            logger.info(f"Completed stream: {stream}")

        df = pd.DataFrame(collected)
        df = df.drop_duplicates(subset="college_profile_url")
        df.to_csv("college_urls.csv", index=False)

        logger.info(f"Total unique colleges collected: {len(df)}")
        return df


# ============ MAIN ============

def main():
    scraper = CollegeDuniaScraper()
    df = scraper.collect_college_urls()

    if df.empty:
        logger.error("No college URLs collected")
    else:
        logger.info("Stage 1 completed successfully")


if __name__ == "__main__":
    main()
