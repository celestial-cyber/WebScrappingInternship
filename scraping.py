#!/usr/bin/env python3
"""
CollegeDunia Web Scraper - India Colleges
Scrapes detailed college information from https://collegedunia.com/india-colleges

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
from pathlib import Path

# ============ CONFIGURATION ============

BASE_URL = "https://collegedunia.com"
LISTING_URL = "https://collegedunia.com/india-colleges"

MAX_PAGES = 3000
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


def get_session() -> requests.Session:
    session = requests.Session()
    headers = random.choice(HEADERS_LIST)
    session.headers.update(headers)
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


# ============ NEW PARSING LOGIC (TABLE ROWS) ============

def find_listing_table(soup: BeautifulSoup):
    """
    Locate the main colleges listing table.
    From your CSS path, it sits under a div.listing-block-container.
    """
    container = soup.select_one("div.listing-block-container table")
    if container:
        return container
    # fallback: first table
    return soup.find("table")


def parse_college_row(tr) -> Dict[str, str]:
    """
    Parse one <tr class="table-row ..."> into a dict.
    Structure for IIMA row (example): [file:65][attachment:1]

    <tr class="... table-row ...">
      <td> #1 </td>   <-- CD Rank
      <td> ... college-info-wrapper ... </td>
      <td class="col-fees"> ... </td>
      <td class="col-placement"> ... </td>
      <td class="col-reviews"> ... </td>
      <td class="col-ranking"> ... </td>
    </tr>
    """

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

    # ----- CD Rank (td[0]) -----
    data["CD Rank"] = tds[0].get_text(strip=True)

    # ----- College + City + State (td[1]) -----
    info_td = tds[1]
    name_tag = info_td.select_one("a.college_name h3")
    if name_tag:
        # h3 text is "IIMA - Indian Institute of Management, Ahmedabad"
        data["College Name"] = " ".join(name_tag.get_text(" ", strip=True).split())

    loc_span = info_td.select_one("span.location")
    if loc_span:
        loc_text = loc_span.get_text(" ", strip=True)
        parts = [p.strip() for p in loc_text.split(",")]
        if len(parts) >= 1:
            data["City"] = parts[0]
        if len(parts) >= 2:
            data["State"] = parts[1]

    # ----- Course Fees (td with class col-fees) -----
    fees_td = tr.select_one("td.col-fees")
    if fees_td:
        amount_span = fees_td.select_one("span.text-lg.text-green")
        amount_text = amount_span.get_text(" ", strip=True) if amount_span else ""
        label_span = fees_td.select_one("span[title]")
        label_text = label_span.get("title", "").strip() if label_span else ""
        # Build like sample: "₹ 27,50,000 (MBA/PGDM - Total Fees)" [attachment:1]
        parts = []
        if amount_text:
            parts.append(amount_text)
        if label_text:
            parts.append(f"({label_text})")
        data["Course Fees"] = " ".join(parts).strip()

    # ----- Placement (td.col-placement) -----
    placement_td = tr.select_one("td.col-placement")
    placement_parts = []
    if placement_td:
        blocks = placement_td.select("a.jsx-914129990")  # avg & highest package links
        for block in blocks:
            value_span = block.select_one("span.text-green")
            value_text = value_span.get_text(" ", strip=True) if value_span else ""
            label_span = block.select_one("span.text-sm.text-dark-gray")
            label_text = label_span.get_text(" ", strip=True) if label_span else ""
            if value_text and label_text:
                placement_parts.append(f"{value_text} {label_text}")
        # placement percentage
        perc_span = placement_td.select_one("span.placement-percentage")
        if perc_span:
            placement_parts.append(perc_span.get_text(" ", strip=True))
        # placement score
        score_container = placement_td.select_one("div.placement-score")
        if score_container:
            score_text = score_container.get_text(" ", strip=True)
            placement_parts.append(score_text)

    data["Placement"] = " ".join(placement_parts).strip()

    # ----- User Reviews (td.col-reviews) -----
    reviews_td = tr.select_one("td.col-reviews")
    if reviews_td:
        rating_span = reviews_td.select_one("span.lr-key")
        rating_text = rating_span.get_text(" ", strip=True) if rating_span else ""
        count_span = reviews_td.select_one("span.lr-value")
        count_text = count_span.get_text(" ", strip=True) if count_span else ""
        tagline_span = reviews_td.select_one("span.tagline span.jsx-3698117056")
        tagline_text = tagline_span.get_text(" ", strip=True) if tagline_span else ""
        pieces = []
        if rating_text:
            pieces.append(rating_text)
        if count_text:
            pieces.append(count_text)
        if tagline_text:
            pieces.append(tagline_text)
        data["User Reviews"] = " ".join(pieces).strip()

    # ----- Ranking (td.col-ranking) -----
    ranking_td = tr.select_one("td.col-ranking")
    if ranking_td:
        rank_span = ranking_td.select_one("span.rank-span")
        rank_text = rank_span.get_text(" ", strip=True) if rank_span else ""
        # often something like "#428th/2000 in India 2025"
        rank_container = ranking_td.select_one("span.rank-container")
        container_text = rank_container.get_text(" ", strip=True) if rank_container else ""
        # Prefer container text if available
        if container_text:
            data["Ranking"] = " ".join(container_text.split())
        elif rank_text:
            data["Ranking"] = " ".join(rank_text.split())

    return data


def find_college_rows(soup: BeautifulSoup):
    """
    Find all <tr class="... table-row ..."> inside the main listing table. [file:65]
    """
    table = find_listing_table(soup)
    if not table:
        return []
    rows = table.select("tr.table-row")
    return rows


# ============ SCRAPER CLASS (UPDATED TO USE ROWS) ============

class CollegeDuniaScraper:
    def __init__(self):
        self.session = get_session()
        self.colleges_data: List[Dict[str, str]] = []
        self.start_time = datetime.now()

    def scrape_listing_page(self, page_num: int) -> int:
        params = {"page": page_num} if page_num > 1 else None
        logger.info(f"Scraping listing page {page_num} ...")

        soup = fetch_page(self.session, LISTING_URL, params=params)
        if not soup:
            logger.warning(f"Failed to fetch page {page_num}")
            return 0

        rows = find_college_rows(soup)
        if not rows:
            logger.warning(f"No college rows found on page {page_num}")
            return 0

        count = 0
        for tr in rows:
            college_data = parse_college_row(tr)
            if college_data.get("College Name"):
                self.colleges_data.append(college_data)
                count += 1

        logger.info(f"Page {page_num}: Parsed {count} colleges")
        return count

    def run(self, max_pages: int = MAX_PAGES, expected_total: int = 20558) -> List[Dict[str, str]]:
        logger.info("=" * 80)
        logger.info("🎓 Starting Collegedunia India Colleges Scraper")
        logger.info(f"Listing URL: {LISTING_URL}")
        logger.info(f"Expected total colleges: {expected_total}")
        logger.info("=" * 80)

        total_scraped = 0
        consecutive_empty_pages = 0

        for page_num in range(1, max_pages + 1):
            polite_sleep()
            count = self.scrape_listing_page(page_num)

            if count == 0:
                consecutive_empty_pages += 1
                logger.warning(
                    f"Empty page detected (page {page_num}), "
                    f"consecutive empty pages = {consecutive_empty_pages}"
                )
                if consecutive_empty_pages >= 3:
                    logger.warning("Multiple empty pages in a row. Stopping.")
                    break
            else:
                consecutive_empty_pages = 0
                total_scraped += count

            if total_scraped >= expected_total:
                logger.info(
                    f"Reached expected total {expected_total} colleges. "
                    f"Stopping at page {page_num}."
                )
                break

        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds() / 60

        logger.info("=" * 80)
        logger.info("🎓 SCRAPING COMPLETE")
        logger.info(f"Total Colleges Scraped: {total_scraped}")
        logger.info(f"Expected Colleges: {expected_total}")
        logger.info(f"Duration: {duration:.2f} minutes")
        logger.info(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End Time:   {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)

        return self.colleges_data

    def save_to_csv(self, filename: str = "colleges_data.csv") -> Optional[str]:
        if not self.colleges_data:
            logger.error("No data to save to CSV.")
            return None

        try:
            df = pd.DataFrame(self.colleges_data)
            df.to_csv(filename, index=False, encoding="utf-8")
            file_size_kb = Path(filename).stat().st_size / 1024
            logger.info(f"CSV saved: {filename} ({len(df)} records, {file_size_kb:.2f} KB)")
            return filename
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
            return None

    def save_to_excel(self, filename: str = "colleges_data.xlsx") -> Optional[str]:
        if not self.colleges_data:
            logger.error("No data to save to Excel.")
            return None

        try:
            df = pd.DataFrame(self.colleges_data)
            df.to_excel(filename, index=False, engine="openpyxl")
            file_size_kb = Path(filename).stat().st_size / 1024
            logger.info(f"Excel saved: {filename} ({len(df)} records, {file_size_kb:.2f} KB)")
            return filename
        except Exception as e:
            logger.error(f"Error saving Excel: {e}")
            return None


def main():
    scraper = CollegeDuniaScraper()
    data = scraper.run(expected_total=20558)

    if data:
        scraper.save_to_csv("colleges_data.csv")
        scraper.save_to_excel("colleges_data.xlsx")
        logger.info("Data export complete.")
    else:
        logger.error("No data scraped; skipping file export.")


if __name__ == "__main__":
    main()
