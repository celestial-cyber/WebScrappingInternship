#!/usr/bin/env python3
"""
CollegeDunia Web Scraper - Science Colleges
Scrapes detailed Science college information from https://collegedunia.com/science-colleges

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
LISTING_URL = "https://collegedunia.com/science-colleges"

MAX_PAGES = 3000        # Maximum pages to scrape (adjustable)
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
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/119.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    },
]

# ============ LOGGING SETUP ============

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

def fetch_page(session: requests.Session, url: str, params: Optional[dict] = None) -> Optional[BeautifulSoup]:
    try:
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.warning(f"Non-200 status {resp.status_code} for URL: {resp.url}")
            return None
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as e:
        logger.error(f"Request failed for {url}: {e}")
        return None

# ============ PARSING LOGIC ============

def find_listing_table(soup: BeautifulSoup):
    table = soup.select_one("div.listing-block-container table")
    return table or soup.find("table")

def parse_college_row(tr) -> Dict[str, str]:
    data = {
        "CD Rank": "",
        "College Name": "",
        "City": "",
        "State": "",
        "Course Fees": "",
        "Placement": "",
        "User Reviews": "",
        "Ranking": "",
    }

    tds = tr.find_all("td", recursive=False)
    if len(tds) < 2:
        return data

    data["CD Rank"] = tds[0].get_text(strip=True)

    info_td = tds[1]
    name_tag = info_td.select_one("a.college_name h3")
    if name_tag:
        data["College Name"] = " ".join(name_tag.get_text(" ", strip=True).split())

    loc_span = info_td.select_one("span.location")
    if loc_span:
        parts = [p.strip() for p in loc_span.get_text(strip=True).split(",")]
        if len(parts) > 0:
            data["City"] = parts[0]
        if len(parts) > 1:
            data["State"] = parts[1]

    fees_td = tr.select_one("td.col-fees")
    if fees_td:
        amount = fees_td.select_one("span.text-lg.text-green")
        label = fees_td.select_one("span[title]")
        parts = []
        if amount:
            parts.append(amount.get_text(strip=True))
        if label:
            parts.append(f"({label['title']})")
        data["Course Fees"] = " ".join(parts)

    placement_td = tr.select_one("td.col-placement")
    if placement_td:
        data["Placement"] = placement_td.get_text(" ", strip=True)

    reviews_td = tr.select_one("td.col-reviews")
    if reviews_td:
        data["User Reviews"] = reviews_td.get_text(" ", strip=True)

    ranking_td = tr.select_one("td.col-ranking")
    if ranking_td:
        data["Ranking"] = ranking_td.get_text(" ", strip=True)

    return data

def find_college_rows(soup: BeautifulSoup):
    table = find_listing_table(soup)
    return table.select("tr.table-row") if table else []

# ============ SCRAPER CLASS ============

class CollegeDuniaScraper:
    def __init__(self):
        self.session = get_session()
        self.colleges_data: List[Dict[str, str]] = []
        self.start_time = datetime.now()

    def scrape_listing_page(self, page_num: int) -> int:
        params = {"page": page_num} if page_num > 1 else None
        logger.info(f"Scraping listing page {page_num}")

        soup = fetch_page(self.session, LISTING_URL, params=params)
        if not soup:
            return 0

        rows = find_college_rows(soup)
        count = 0

        for tr in rows:
            college = parse_college_row(tr)
            if college["College Name"]:
                self.colleges_data.append(college)
                count += 1

        return count

    def run(
        self,
        start_page: int = 1,
        max_pages: int = 1000,       # Adjust for testing
        expected_total: int = 3000  # Adjust for testing
    ) -> List[Dict[str, str]]:
        total_scraped = 0
        empty_pages = 0

        for page in range(start_page, start_page + max_pages):
            polite_sleep()
            count = self.scrape_listing_page(page)

            if count == 0:
                empty_pages += 1
                if empty_pages >= 3:
                    logger.info("3 consecutive empty pages reached, stopping scraper.")
                    break
            else:
                empty_pages = 0
                total_scraped += count

            if total_scraped >= expected_total:
                break

        logger.info(f"Total colleges scraped: {total_scraped}")
        return self.colleges_data

    def save_to_csv(self, filename: str):
        pd.DataFrame(self.colleges_data).to_csv(filename, index=False)

    def save_to_excel(self, filename: str):
        pd.DataFrame(self.colleges_data).to_excel(filename, index=False)

# ============ MAIN ============

def main():
    scraper = CollegeDuniaScraper()
    data = scraper.run()

    if data:
        scraper.save_to_csv("management_colleges.csv")
        scraper.save_to_excel("management_colleges.xlsx")
        logger.info("Scraping completed successfully")
    else:
        logger.error("No data scraped")

if __name__ == "__main__":
    main()
