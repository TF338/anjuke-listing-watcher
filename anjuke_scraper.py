#!/usr/bin/env python3
"""
Anjuke Listing Watcher
======================
Crawls Anjuke real estate website and notifies users of new listings matching criteria.

Usage:
    python anjuke_scraper.py

Configuration:
    All settings must be defined in config.yaml

Exit Codes:
    0 - Success (even if no new listings found)
    1 - Critical error (config, database, network failure)
"""

import os
import sys
import json
import time
import random
import sqlite3
import logging
import smtplib
import argparse
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Dict, Any

import requests
import yaml
from bs4 import BeautifulSoup


# =============================================================================
# Configuration
# =============================================================================

DEFAULT_CONFIG_FILE = "config.yaml"
DEFAULT_CACHE_FILE = "cache.db"
DEFAULT_LOG_FILE = "anjuke_scraper.log"

LISTING_TYPE_URLS = {
    "rent_apartment": "/fangyuan/",
    "rent_house": "/fangyuan/",
    "sale_apartment": "/sale/",
    "sale_house": "/sale/",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


# =============================================================================
# Logging Setup
# =============================================================================

def setup_logging(log_file: str = DEFAULT_LOG_FILE) -> logging.Logger:
    """
    Configure logging to both file and console.
    
    Args:
        log_file: Path to log file
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("anjuke_scraper")
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # File handler with rotation (5MB, 3 backups)
    try:
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8"
        )
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Warning: Could not create log file: {e}")
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    return logger


# =============================================================================
# Configuration Loading
# =============================================================================

def validate_city_url(config: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    Validate that the city code returns a valid response from Anjuke.
    
    Args:
        config: Configuration dictionary
        logger: Logger instance
        
    Returns:
        True if URL is accessible, False otherwise
    """
    city = config["city"]
    listing_type = config["listing_type"]
    path = LISTING_TYPE_URLS[listing_type]
    
    if listing_type.startswith("rent_"):
        url = f"https://{city}.zu.anjuke.com{path}"
    else:
        url = f"https://{city}.anjuke.com{path}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    
    for attempt in range(2):
        try:
            response = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
            if response.status_code == 200:
                logger.info(f"Validated city code '{city}': {url}")
                return True
            elif response.status_code == 404:
                logger.warning(f"City code '{city}' not found (404)")
                return False
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout validating city '{city}' (attempt {attempt + 1}/2)")
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error validating city '{city}': {e} (attempt {attempt + 1}/2)")
        except Exception as e:
            logger.warning(f"Error validating city '{city}': {e} (attempt {attempt + 1}/2)")
        
        if attempt < 1:
            time.sleep(2)
    
    return False


def load_config(config_file: str = DEFAULT_CONFIG_FILE) -> Dict[str, Any]:
    """
    Load and validate configuration from YAML file.
    
    Args:
        config_file: Path to configuration file
        
    Returns:
        Configuration dictionary
        
    Raises:
        SystemExit: If configuration is invalid or missing required fields
    """
    logger = logging.getLogger("anjuke_scraper")
    
    if not os.path.exists(config_file):
        logger.error(f"Configuration file not found: {config_file}")
        print(f"Error: Configuration file '{config_file}' not found.")
        print(f"Create it from the example or specify with --config")
        sys.exit(1)
    
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in configuration: {e}")
        print(f"Error: Invalid YAML syntax in config file: {e}")
        sys.exit(1)
    
    # Validate required fields
    required_fields = ["city", "listing_type", "price_min", "price_max", "keywords"]
    missing = [f for f in required_fields if not config.get(f)]
    if missing:
        logger.error(f"Missing required configuration fields: {missing}")
        print(f"Error: Missing required fields: {missing}")
        sys.exit(1)
    
    # Validate listing_type
    if config["listing_type"] not in LISTING_TYPE_URLS:
        logger.error(f"Invalid listing_type: {config['listing_type']}")
        print(f"Error: Invalid listing_type. Must be one of: {list(LISTING_TYPE_URLS.keys())}")
        sys.exit(1)
    
    # Validate price range
    if config["price_min"] >= config["price_max"]:
        logger.error("price_min must be less than price_max")
        print("Error: price_min must be less than price_max")
        sys.exit(1)
    
    # Validate city and URL by making a test request
    if not validate_city_url(config, logger):
        logger.error(f"Invalid city code or URL not accessible: {config['city']}")
        print(f"Error: Could not access Anjuke with city code '{config['city']}'")
        print(f"       Please check the city code in your config.")
        print(f"       Find your city at https://www.anjuke.com (use 2-letter code from URL)")
        sys.exit(1)
    
    # Set defaults
    config.setdefault("neighborhoods", [])
    config.setdefault("pages_to_scan", 3)
    config.setdefault("rate_limit_random_min", 5)
    config.setdefault("rate_limit_random_max", 10)
    config.setdefault("fetch_detail_pages", True)
    config.setdefault("output_mode", "file")
    config.setdefault("output_file", "listings.txt")
    config.setdefault("email", {})
    
    # Validate output_mode
    if config["output_mode"] not in ["file", "email"]:
        logger.error(f"Invalid output_mode: {config['output_mode']}")
        print("Error: output_mode must be 'file' or 'email'")
        sys.exit(1)
    
    # Validate email settings if needed
    if config["output_mode"] == "email":
        email_required = ["smtp_server", "smtp_port", "username", "password", "sender", "recipients"]
        email_missing = [f for f in email_required if not config["email"].get(f)]
        if email_missing:
            logger.error(f"Missing email configuration: {email_missing}")
            print(f"Error: Missing email fields: {email_missing}")
            sys.exit(1)
        if not isinstance(config["email"]["recipients"], list) or not config["email"]["recipients"]:
            logger.error("email.recipients must be a non-empty list")
            print("Error: email.recipients must be a non-empty list")
            sys.exit(1)
    
    logger.info(f"Configuration loaded: city={config['city']}, type={config['listing_type']}")
    return config


# =============================================================================
# Exceptions
# =============================================================================

class CAPTCHAException(Exception):
    """Raised when CAPTCHA is detected on the page."""
    pass


# =============================================================================
# Cache Manager (SQLite)
# =============================================================================

class CacheManager:
    """
    Manages persistent cache of visited listing URLs using SQLite.
    
    Automatically cleans up records older than 180 days on startup.
    """
    
    def __init__(self, cache_file: str = DEFAULT_CACHE_FILE, logger: Optional[logging.Logger] = None):
        """
        Initialize cache manager.
        
        Args:
            cache_file: Path to SQLite database
            logger: Logger instance
        """
        self.cache_file = cache_file
        self.logger = logger or logging.getLogger("anjuke_scraper")
        self._init_database()
    
    def _init_database(self) -> None:
        """Initialize database and create tables."""
        try:
            with sqlite3.connect(self.cache_file) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS visited_listings (
                        url TEXT PRIMARY KEY,
                        visited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.commit()
                self.logger.info(f"Cache database initialized: {self.cache_file}")
        except sqlite3.Error as e:
            self.logger.error(f"Failed to initialize cache database: {e}")
            raise
    
    def cleanup_old_records(self, days: int = 180) -> int:
        """
        Remove records older than specified days.
        
        Args:
            days: Number of days to keep
            
        Returns:
            Number of records deleted
        """
        try:
            with sqlite3.connect(self.cache_file) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    DELETE FROM visited_listings 
                    WHERE visited_at < datetime('now', '-' || ? || ' days')
                """, (days,))
                deleted = cursor.rowcount
                conn.commit()
                if deleted > 0:
                    self.logger.info(f"Cleaned up {deleted} old cache records (>{days} days)")
                return deleted
        except sqlite3.Error as e:
            self.logger.error(f"Failed to cleanup old cache records: {e}")
            return 0
    
    def is_visited(self, url: str) -> bool:
        """
        Check if a URL has been visited before.
        
        Args:
            url: Listing URL to check
            
        Returns:
            True if URL exists in cache
        """
        try:
            with sqlite3.connect(self.cache_file) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM visited_listings WHERE url = ?", (url,))
                return cursor.fetchone() is not None
        except sqlite3.Error as e:
            self.logger.error(f"Failed to check cache: {e}")
            return False
    
    def add(self, url: str) -> None:
        """
        Add a URL to the cache.
        
        Args:
            url: Listing URL to add
        """
        try:
            with sqlite3.connect(self.cache_file) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO visited_listings (url, visited_at)
                    VALUES (?, CURRENT_TIMESTAMP)
                """, (url,))
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Failed to add URL to cache: {e}")


# =============================================================================
# Anjuke Scraper
# =============================================================================

class AnjukeScraper:
    """
    Handles HTTP requests to Anjuke website with rate limiting and retry logic.
    """
    
    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        """
        Initialize scraper with configuration.
        
        Args:
            config: Configuration dictionary
            logger: Logger instance
        """
        self.config = config
        self.logger = logger or logging.getLogger("anjuke_scraper")
        self.session = requests.Session()
        self.session.headers.update({**HEADERS, "User-Agent": USER_AGENTS[0]})
        self.rate_limit_random_min = config.get("rate_limit_random_min", 5)
        self.rate_limit_random_max = config.get("rate_limit_random_max", 10)
        self.last_request_time = 0
    
    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        random_delay = random.uniform(self.rate_limit_random_min, self.rate_limit_random_max)
        if elapsed < random_delay:
            time.sleep(random_delay - elapsed)
        self.last_request_time = time.time()
    
    def _get_base_url(self) -> str:
        """Build the base URL for the configured city and listing type."""
        city = self.config["city"]
        listing_type = self.config["listing_type"]
        path = LISTING_TYPE_URLS[listing_type]
        
        if listing_type.startswith("rent_"):
            return f"https://{city}.zu.anjuke.com{path}"
        else:
            return f"https://{city}.anjuke.com{path}"
    
    def _get_page_url(self, page: int, neighborhood: Optional[str] = None) -> str:
        """
        Generate URL for a specific page and optional neighborhood.
        
        Args:
            page: Page number (1-based)
            neighborhood: Optional neighborhood filter
            
        Returns:
            Full URL for the page
        """
        base = self._get_base_url()
        
        if neighborhood:
            base = f"{base}{neighborhood}/"
        
        if page > 1:
            # Anjuke uses /p{page}/ for pagination
            if not base.endswith("/"):
                base += "/"
            base = f"{base}p{page}/"
        
        return base
    
    def fetch_page(self, url: str, max_retries: int = 3) -> Optional[str]:
        """
        Fetch a page with retry logic and rate limiting.
        
        Args:
            url: URL to fetch
            max_retries: Maximum number of retry attempts
            
        Returns:
            Page HTML content or None on failure
        """
        self._rate_limit()
        
        for attempt in range(max_retries):
            try:
                # Rotate User-Agent occasionally
                if attempt > 0:
                    self.session.headers["User-Agent"] = USER_AGENTS[attempt % len(USER_AGENTS)]
                
                response = self.session.get(url, timeout=30)
                
                # Handle rate limiting (HTTP 429)
                if response.status_code == 429:
                    wait_time = (2 ** attempt) * 10
                    self.logger.warning(f"Rate limited. Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                
                # Handle other HTTP errors
                if response.status_code >= 400:
                    self.logger.warning(f"HTTP {response.status_code} for {url}")
                    if response.status_code < 500:
                        return None
                    continue
                
                response.raise_for_status()
                
                # Check for CAPTCHA page (more strict detection)
                if "访问过于频繁" in response.text or "geetest" in response.text:
                    self.logger.error("CAPTCHA detected! Stopping to avoid further blocking.")
                    raise CAPTCHAException("CAPTCHA detected on page")
                
                self.logger.debug(f"Fetched: {url}")
                return response.text
                
            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout fetching {url} (attempt {attempt + 1}/{max_retries})")
            except requests.exceptions.ConnectionError as e:
                self.logger.warning(f"Connection error for {url}: {e} (attempt {attempt + 1}/{max_retries})")
            except requests.exceptions.HTTPError as e:
                self.logger.warning(f"HTTP error for {url}: {e} (attempt {attempt + 1}/{max_retries})")
            except CAPTCHAException:
                raise
            except Exception as e:
                self.logger.error(f"Unexpected error fetching {url}: {e}")
                return None
            
            # Wait before retry
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
        
        self.logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return None
    
    def parse_listings(self, html: str) -> List[Dict[str, Any]]:
        """
        Parse listing data from HTML using BeautifulSoup.
        
        Args:
            html: Page HTML content
            
        Returns:
            List of parsed listing dictionaries
        """
        listings = []
        try:
            soup = BeautifulSoup(html, "lxml")
            
            # Try multiple selectors for different listing layouts
            # Primary: .zu-itemmod (Anjuke rental listings)
            items = soup.select(".zu-itemmod")
            
            if not items:
                # Alternative: .property-item (common class for listings)
                items = soup.select(".property-item")
            
            if not items:
                # Alternative: .listing-item
                items = soup.select(".listing-item")
            
            if not items:
                # Alternative: .house-item
                items = soup.select(".house-item")
            
            if not items:
                # Try to find any item in a listings container
                items = soup.select("[class*='item']")
            
            for item in items:
                try:
                    listing = self._parse_listing_item(item)
                    if listing:
                        listings.append(listing)
                except Exception as e:
                    self.logger.debug(f"Failed to parse listing item: {e}")
                    continue
            
            self.logger.info(f"Parsed {len(listings)} listings from page")
            
        except Exception as e:
            self.logger.error(f"Failed to parse listings: {e}")
        
        return listings
    
    def parse_listing_detail(self, html: str) -> Dict[str, Any]:
        """Parse listing detail page for additional information."""
        result = {
            "title": "",
            "house_info": "",
            "house_facilities": "",
            "house_overview": "",
            "community": "",
            "community_qa": "",
        }
        
        try:
            soup = BeautifulSoup(html, "lxml")
            
            # Title
            title_elem = soup.select_one("h1.house-title")
            if title_elem:
                result["title"] = title_elem.get_text(strip=True)
            
            # House info (房屋信息)
            house_info_elem = soup.select_one(".house-info-zufang")
            if house_info_elem:
                result["house_info"] = house_info_elem.get_text(strip=True)
            
            # House facilities (房屋配套)
            facility_elems = soup.select(".house-info-peitao .peitao-info")
            facilities = [f.get_text(strip=True) for f in facility_elems]
            result["house_facilities"] = " ".join(facilities)
            
            # House overview (房源概况)
            overview_elem = soup.select_one(".auto-general")
            if overview_elem:
                result["house_overview"] = overview_elem.get_text(strip=True)
            
            # Community (小区)
            comm_elem = soup.select_one("h2#commArround")
            if comm_elem:
                result["community"] = comm_elem.get_text(strip=True)
            
            # Community Q&A (小区问答)
            qa_elems = soup.select(".comm-qa-unanswer li a p")
            qa_texts = [q.get_text(strip=True) for q in qa_elems]
            result["community_qa"] = " ".join(qa_texts)
            
        except Exception as e:
            self.logger.debug(f"Failed to parse detail page: {e}")
        
        return result
    
    def _parse_listing_item(self, item: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """
        Parse a single listing item.
        
        Args:
            item: BeautifulSoup element for a listing
            
        Returns:
            Listing dictionary or None if parsing fails
        """
        try:
            # Try multiple selectors for title
            title_elem = (
                item.select_one(".house-title") or
                item.select_one(".item-title") or
                item.select_one(".property-title") or
                item.select_one(".title") or
                item.select_one("a.title") or
                item.select_one("h3 a") or
                item.select_one("a[href*='fangyuan']") or
                item.select_one("a[href*='sale']") or
                item.select_one(".zu-title") or
                item.select_one("a.house-link")
            )
            
            if not title_elem:
                return None
            
            title = title_elem.get_text(strip=True) or title_elem.get("title", "")
            
            # Get URL
            url = title_elem.get("href") if title_elem.name == "a" else None
            if not url:
                link = item.select_one("a")
                if link:
                    url = link.get("href")
            
            if not url:
                return None
            
            # Ensure URL is complete
            if url.startswith("//"):
                url = "https:" + url
            elif url.startswith("/"):
                base = self._get_base_url()
                url = base + url
            
            # Try multiple selectors for price
            price_elem = (
                item.select_one(".zu-itemprice") or
                item.select_one(".property-price") or
                item.select_one(".price") or
                item.select_one("[class*='price']") or
                item.select_one(".house-price")
            )
            
            price = price_elem.get_text(strip=True) if price_elem else ""
            
            # Try multiple selectors for location/neighborhood
            location_elem = (
                item.select_one(".property-location") or
                item.select_one(".location") or
                item.select_one("[class*='location']") or
                item.select_one(".address") or
                item.select_one(".region") or
                item.select_one(".zu-info")
            )
            
            location = location_elem.get_text(strip=True) if location_elem else ""
            
            # Try multiple selectors for area/square meters
            # Look for elements containing 平米 or ㎡
            area = ""
            area_elems = item.select(".details-item") or item.select("[class*='details']")
            for ae in area_elems:
                text = ae.get_text(strip=True)
                if "平米" in text or "㎡" in text:
                    area = text
                    break
            
            if not area:
                area_elem = (
                    item.select_one(".property-area") or
                    item.select_one(".area") or
                    item.select_one("[class*='area']") or
                    item.select_one(".size")
                )
                area = area_elem.get_text(strip=True) if area_elem else ""
            
            return {
                "title": title,
                "price": price,
                "location": location,
                "area": area,
                "url": url,
            }
            
        except Exception as e:
            self.logger.debug(f"Failed to parse listing item: {e}")
            return None


# =============================================================================
# Filtering Logic
# =============================================================================

def extract_price(price_str: str) -> Optional[float]:
    """
    Extract numeric price from price string.
    
    Args:
        price_str: Price string like "2000元/月" or "200万"
        
    Returns:
        Numeric price or None if extraction fails
    """
    if not price_str:
        return None
    
    import re
    
    # Remove common characters and extract numbers
    cleaned = price_str.replace(",", "").replace(" ", "")
    
    # Handle 万元 (10,000 yuan)
    if "万" in cleaned:
        match = re.search(r"([\d.]+)\s*万", cleaned)
        if match:
            return float(match.group(1)) * 10000
    
    # Handle 元/月 or just 元
    match = re.search(r"([\d.]+)\s*(?:元|块)", cleaned)
    if match:
        return float(match.group(1))
    
    # Try to find any number
    match = re.search(r"([\d.]+)", cleaned)
    if match:
        return float(match.group(1))
    
    return None


def extract_area(area_str: str) -> Optional[float]:
    """
    Extract numeric area from area string.
    
    Args:
        area_str: Area string like "80㎡", "80平米", "80 m²"
        
    Returns:
        Numeric area in square meters or None if extraction fails
    """
    if not area_str:
        return None
    
    import re
    
    cleaned = area_str.replace(",", "").replace(" ", "")
    
    match = re.search(r"([\d.]+)\s*(?:㎡|平米|m²|平方米|平)", cleaned)
    if match:
        return float(match.group(1))
    
    match = re.search(r"([\d.]+)", cleaned)
    if match:
        return float(match.group(1))
    
    return None


def filter_listing(
    listing: Dict[str, Any],
    config: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> Optional[List[str]]:
    """
    Check if a listing matches all filter criteria.
    
    Args:
        listing: Listing dictionary with title, price, location, url
        config: Configuration dictionary
        logger: Logger instance
        
    Returns:
        List of matched keywords if listing matches, None otherwise
    """
    logger = logger or logging.getLogger("anjuke_scraper")
    
    # Filter 1: Price range
    price = extract_price(listing.get("price", ""))
    if price is not None:
        if price < config["price_min"] or price > config["price_max"]:
            return None
    
    # Filter 2: Neighborhood (if configured)
    neighborhoods = config.get("neighborhoods", [])
    if neighborhoods:
        location = listing.get("location", "")
        title = listing.get("title", "")
        combined = location + title
        matched_neighborhood = False
        for nb in neighborhoods:
            if nb in combined:
                matched_neighborhood = True
                break
        if not matched_neighborhood:
            return None
    
    # Filter 3: Area range (if configured)
    area_min = config.get("area_min")
    area_max = config.get("area_max")
    if area_min is not None or area_max is not None:
        area = extract_area(listing.get("area", ""))
        if area is not None:
            if area_min is not None and area < area_min:
                return None
            if area_max is not None and area > area_max:
                return None
    
    # Filter 4: Keywords
    keywords = config.get("keywords", [])
    if keywords:
        title = listing.get("title", "")
        location = listing.get("location", "")
        
        # Combine all detail fields for keyword matching
        combined = " ".join([
            title,
            location,
            listing.get("house_info", ""),
            listing.get("house_facilities", ""),
            listing.get("house_overview", ""),
            listing.get("community", ""),
            listing.get("community_qa", ""),
        ])
        
        matched_keywords = []
        for kw in keywords:
            if kw in combined:
                matched_keywords.append(kw)
        
        if not matched_keywords:
            return None
        
        return matched_keywords
    
    # If no keywords configured, listing still matches (if passed other filters)
    return []


# =============================================================================
# Notification
# =============================================================================

class Notifier:
    """
    Handles notification of new listings via file or email.
    """
    
    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        """
        Initialize notifier.
        
        Args:
            config: Configuration dictionary
            logger: Logger instance
        """
        self.config = config
        self.logger = logger or logging.getLogger("anjuke_scraper")
    
    def notify(self, listings: List[Dict[str, Any]]) -> None:
        """
        Send notifications for new listings.
        
        Args:
            listings: List of matching listing dictionaries
        """
        if not listings:
            self.logger.info("No new listings to notify")
            return
        
        output_mode = self.config.get("output_mode", "file")
        
        if output_mode == "file":
            self._notify_file(listings)
        elif output_mode == "email":
            self._notify_email(listings)
    
    def _notify_file(self, listings: List[Dict[str, Any]]) -> None:
        """Append listings to output file."""
        output_file = self.config.get("output_file", "listings.txt")
        
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            with open(output_file, "a", encoding="utf-8") as f:
                f.write(f"\n=== New Listings Found: {timestamp} ===\n")
                f.write(f"Total: {len(listings)} listings\n\n")
                
                for listing in listings:
                    keywords_str = ", ".join(listing.get("matched_keywords", [])) or "N/A"
                    area_str = listing.get("area", "") or "N/A"
                    f.write(f"Title: {listing['title']}\n")
                    f.write(f"Price: {listing['price']}\n")
                    f.write(f"Area: {area_str}\n")
                    f.write(f"Keywords: [{keywords_str}]\n")
                    f.write(f"URL: {listing['url']}\n")
                    f.write("---\n")
                
                f.write(f"\n")
            
            self.logger.info(f"Notified {len(listings)} listings to {output_file}")
            
        except IOError as e:
            self.logger.error(f"Failed to write to output file: {e}")
            raise
    
    def _notify_email(self, listings: List[Dict[str, Any]]) -> None:
        """Send listing notification via email."""
        email_config = self.config.get("email", {})
        
        smtp_server = email_config["smtp_server"]
        smtp_port = email_config["smtp_port"]
        username = email_config["username"]
        password = email_config["password"]
        sender = email_config["sender"]
        recipients = email_config["recipients"]
        
        # Build email content
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        html_content = f"""
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .listing {{ margin-bottom: 20px; padding: 10px; border: 1px solid #ddd; }}
                .title {{ font-weight: bold; font-size: 16px; }}
                .price {{ color: #e74c3c; font-size: 18px; }}
                .keywords {{ color: #27ae60; }}
                .url {{ color: #3498db; }}
            </style>
        </head>
        <body>
            <h2>New Anjuke Listings - {timestamp}</h2>
            <p>Found {len(listings)} new matching listing(s):</p>
        """
        
        for listing in listings:
            keywords_str = ", ".join(listing.get("matched_keywords", [])) or "N/A"
            area_str = listing.get("area", "") or "N/A"
            html_content += f"""
            <div class="listing">
                <div class="title">{listing['title']}</div>
                <div class="price">{listing['price']}</div>
                <div class="area">Area: {area_str}</div>
                <div class="keywords">Keywords: {keywords_str}</div>
                <div class="url"><a href="{listing['url']}">{listing['url']}</a></div>
            </div>
            """
        
        html_content += """
        </body>
        </html>
        """
        
        # Plain text version
        text_content = f"New Anjuke Listings - {timestamp}\n\n"
        text_content += f"Found {len(listings)} new matching listing(s):\n\n"
        
        for listing in listings:
            keywords_str = ", ".join(listing.get("matched_keywords", [])) or "N/A"
            area_str = listing.get("area", "") or "N/A"
            text_content += f"Title: {listing['title']}\n"
            text_content += f"Price: {listing['price']}\n"
            text_content += f"Area: {area_str}\n"
            text_content += f"Keywords: {keywords_str}\n"
            text_content += f"URL: {listing['url']}\n"
            text_content += "---\n\n"
        
        # Create message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[Anjuke] New Listings Found - {timestamp}"
        msg["From"] = sender
        msg["To"] = ", ".join(recipients)
        
        msg.attach(MIMEText(text_content, "plain", "utf-8"))
        msg.attach(MIMEText(html_content, "html", "utf-8"))
        
        # Send email
        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(username, password)
                server.send_message(msg)
            
            self.logger.info(f"Sent email notification with {len(listings)} listings")
            
        except smtplib.SMTPException as e:
            self.logger.error(f"Failed to send email: {e}")
            raise
    
    def notify_captcha(self, url: str, listings: Optional[List[Dict[str, Any]]] = None, error_message: str = "") -> None:
        """Send notification when CAPTCHA is encountered."""
        output_mode = self.config.get("output_mode", "file")
        
        if output_mode == "file":
            self._notify_captcha_file(url, listings, error_message)
        elif output_mode == "email":
            self._notify_captcha_email(url, listings, error_message)
    
    def _notify_captcha_file(self, url: str, listings: Optional[List[Dict[str, Any]]] = None, error_message: str = "") -> None:
        """Append CAPTCHA alert to output file."""
        output_file = self.config.get("output_file", "listings.txt")
        
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            with open(output_file, "a", encoding="utf-8") as f:
                f.write(f"\n{'='*50}\n")
                f.write(f"CAPTCHA DETECTED - Scraping Stopped\n")
                f.write(f"{'='*50}\n")
                f.write(f"Time: {timestamp}\n")
                f.write(f"URL: {url}\n")
                f.write(f"City: {self.config.get('city', 'N/A')}\n")
                f.write(f"Listing Type: {self.config.get('listing_type', 'N/A')}\n")
                
                # Add matches found before CAPTCHA
                if listings:
                    f.write(f"\n=== Matches Found Before CAPTCHA ===\n")
                    f.write(f"Found {len(listings)} matching listing(s):\n\n")
                    for listing in listings:
                        keywords_str = ", ".join(listing.get("matched_keywords", [])) or "N/A"
                        area_str = listing.get("area", "") or "N/A"
                        f.write(f"Title: {listing.get('title', 'N/A')}\n")
                        f.write(f"Price: {listing.get('price', 'N/A')}\n")
                        f.write(f"Area: {area_str}\n")
                        f.write(f"Keywords: [{keywords_str}]\n")
                        f.write(f"URL: {listing.get('url', 'N/A')}\n")
                        f.write("---\n")
                
                # Add error message at the end
                if error_message:
                    f.write(f"\n{'='*50}\n")
                    f.write(f"ERROR: {error_message}\n")
                    f.write(f"{'='*50}\n")
                
                f.write(f"\n")
            
            self.logger.info(f"CAPTCHA notification written to {output_file}")
            
        except IOError as e:
            self.logger.error(f"Failed to write CAPTCHA notification: {e}")
    
    def _notify_captcha_email(self, url: str, listings: Optional[List[Dict[str, Any]]] = None, error_message: str = "") -> None:
        """Send CAPTCHA alert via email."""
        email_config = self.config.get("email", {})
        
        smtp_server = email_config["smtp_server"]
        smtp_port = email_config["smtp_port"]
        username = email_config["username"]
        password = email_config["password"]
        sender = email_config["sender"]
        recipients = email_config["recipients"]
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        subject = f"[Anjuke] CAPTCHA Detected - Scraping Stopped"
        
        # Build matches content
        matches_html = ""
        matches_text = ""
        if listings:
            matches_text = f"\n=== Matches Found Before CAPTCHA ===\nFound {len(listings)} matching listing(s):\n\n"
            matches_html = f"<h3>Matches Found Before CAPTCHA</h3><p>Found {len(listings)} matching listing(s):</p>"
            
            for listing in listings:
                keywords_str = ", ".join(listing.get("matched_keywords", [])) or "N/A"
                area_str = listing.get("area", "") or "N/A"
                matches_text += f"Title: {listing.get('title', 'N/A')}\n"
                matches_text += f"Price: {listing.get('price', 'N/A')}\n"
                matches_text += f"Area: {area_str}\n"
                matches_text += f"Keywords: [{keywords_str}]\n"
                matches_text += f"URL: {listing.get('url', 'N/A')}\n"
                matches_text += "---\n\n"
                
                matches_html += f"""
                <div class="listing">
                    <div class="title">{listing.get('title', 'N/A')}</div>
                    <div class="price">{listing.get('price', 'N/A')}</div>
                    <div class="area">Area: {area_str}</div>
                    <div class="keywords">Keywords: [{keywords_str}]</div>
                    <div class="url"><a href="{listing.get('url', '#')}">{listing.get('url', 'N/A')}</a></div>
                </div>
                """
        
        # Build error message section
        error_text = ""
        error_html = ""
        if error_message:
            error_text = f"\n{'='*50}\nERROR: {error_message}\n{'='*50}\n"
            error_html = f"<h3 style='color: red;'>{'='*50}<br/>ERROR: {error_message}<br/>{'='*50}</h3>"
        
        text_content = f"""CAPTCHA DetECTED - Scraping Stopped

Time: {timestamp}
URL: {url}
City: {self.config.get('city', 'N/A')}
Listing Type: {self.config.get('listing_type', 'N/A')}
{matches_text}{error_text}
Please manually verify and restart the scraper.
"""
        
        html_content = f"""<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; }}
        .listing {{ margin-bottom: 20px; padding: 10px; border: 1px solid #ddd; }}
        .title {{ font-weight: bold; font-size: 16px; }}
        .price {{ color: #e74c3c; font-size: 18px; }}
        .keywords {{ color: #27ae60; }}
        .url {{ color: #3498db; }}
    </style>
</head>
<body>
    <h2 style="color: red;">CAPTCHA Detected - Scraping Stopped</h2>
    <p><strong>Time:</strong> {timestamp}</p>
    <p><strong>URL:</strong> {url}</p>
    <p><strong>City:</strong> {self.config.get('city', 'N/A')}</p>
    <p><strong>Listing Type:</strong> {self.config.get('listing_type', 'N/A')}</p>
    {matches_html}
    {error_html}
    <p>Please manually verify and restart the scraper.</p>
</body>
</html>
"""
        
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = ", ".join(recipients)
        
        msg.attach(MIMEText(text_content, "plain", "utf-8"))
        msg.attach(MIMEText(html_content, "html", "utf-8"))
        
        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(username, password)
                server.send_message(msg)
            
            self.logger.info("Sent CAPTCHA alert email")
            
        except smtplib.SMTPException as e:
            self.logger.error(f"Failed to send CAPTCHA email: {e}")


# =============================================================================
# Main Function
# =============================================================================

def main():
    """Main entry point for the scraper."""
    # Parse arguments
    parser = argparse.ArgumentParser(description="Anjuke Listing Watcher")
    parser.add_argument(
        "--config", "-c",
        default=DEFAULT_CONFIG_FILE,
        help=f"Path to configuration file (default: {DEFAULT_CONFIG_FILE})"
    )
    parser.add_argument(
        "--cache",
        default=DEFAULT_CACHE_FILE,
        help=f"Path to cache database (default: {DEFAULT_CACHE_FILE})"
    )
    parser.add_argument(
        "--log",
        default=DEFAULT_LOG_FILE,
        help=f"Path to log file (default: {DEFAULT_LOG_FILE})"
    )
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log)
    
    logger.info("=" * 60)
    logger.info("Anjuke Listing Watcher starting")
    logger.info("=" * 60)
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Initialize cache
        cache = CacheManager(args.cache, logger)
        cache.cleanup_old_records(180)
        
        # Initialize scraper
        scraper = AnjukeScraper(config, logger)
        
        # Initialize notifier
        notifier = Notifier(config, logger)
        
        # Track new matches
        new_matches = []
        
        # Determine neighborhoods to scan
        neighborhoods = config.get("neighborhoods", [])
        if not neighborhoods:
            neighborhoods = [None]  # Scan all
        
        # Scan each neighborhood
        for neighborhood in neighborhoods:
            neighborhood_name = neighborhood or "all"
            logger.info(f"Scanning neighborhood: {neighborhood_name}")
            
            # Scan pages
            pages_to_scan = config.get("pages_to_scan", 3)
            for page in range(1, pages_to_scan + 1):
                url = scraper._get_page_url(page, neighborhood)
                logger.info(f"Fetching page {page}{' (' + neighborhood_name + ')' if neighborhood else ''}: {url}")
                
                html = scraper.fetch_page(url)
                
                if not html:
                    logger.warning(f"Failed to fetch page {page}")
                    continue
                
                # Parse listings
                listings = scraper.parse_listings(html)
                
                if not listings:
                    logger.info(f"No listings found on page {page}, stopping pagination")
                    break
                
                # Counter for detail pages
                detail_page_count = 0
                
                # Process each listing
                for listing in listings:
                    url = listing.get("url")
                    if not url:
                        continue
                    
                    # Check cache first - skip if already visited
                    if cache.is_visited(url):
                        logger.debug(f"Skipping visited: {url}")
                        continue
                    
                    # Fetch listing detail page if enabled (applies rate limiting)
                    fetch_detail = config.get("fetch_detail_pages", True)
                    if fetch_detail:
                        detail_page_count += 1
                        logger.info(f"Crawling details page {detail_page_count}: {url}")
                        
                        try:
                            detail_html = scraper.fetch_page(url)
                            
                            if detail_html:
                                # Parse detail page for additional fields
                                detail_data = scraper.parse_listing_detail(detail_html)
                                listing.update(detail_data)
                            
                        except CAPTCHAException as e:
                            logger.error(f"CAPTCHA encountered, stopping: {e}")
                            notifier.notify_captcha(url, new_matches, str(e))
                            return 1
                    
                    # Apply filters (now includes keyword matching on detail page content)
                    matched_keywords = filter_listing(listing, config, logger)
                    
                    if matched_keywords is not None:
                        # Add to matches
                        listing["matched_keywords"] = matched_keywords
                        new_matches.append(listing)
                        logger.info(f"New match: {listing['title']} - {listing['price']}")
                    
                    # Mark as visited
                    cache.add(url)
        
        # Notify if there are new matches
        if new_matches:
            logger.info(f"Found {len(new_matches)} new matching listings")
            notifier.notify(new_matches)
        else:
            logger.info("No new matching listings found")
        
        logger.info("Scraper completed successfully")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 1
        
    except SystemExit as e:
        return e.code
        
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
