#!/usr/bin/env python3
"""
Anjuke Crawler Module
====================
Provides a simple interface for crawling Anjuke listings.

Usage:
    from crawler import crawl_city
    
    listings = crawl_city(
        city="km",
        pages=1,
        price_min=1000,
        price_max=3000,
        sqm_min=40,
        sqm_max=100,
        keywords=["地铁", "精装"],
        cache_path="cache.db",
        rate_limit=2
    )
"""

import os
import sys
import time
import sqlite3
import logging
from typing import Optional, List, Dict, Any

# Add current directory to path to import from anjuke_scraper
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from anjuke_scraper import (
    AnjukeScraper,
    CacheManager,
    extract_price,
    extract_area,
    LISTING_TYPE_URLS,
    USER_AGENTS,
    HEADERS,
    CAPTCHAException,
)


# Configure module logger
logger = logging.getLogger("anjuke_crawler")
logger.setLevel(logging.INFO)

# Add console handler if not already present
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(handler)


class CacheManager:
    """Simple cache manager for visited URLs."""
    
    def __init__(self, cache_path: str):
        self.cache_path = cache_path
        self._init_database()
    
    def _init_database(self):
        """Initialize database and create tables."""
        with sqlite3.connect(self.cache_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS visited_listings (
                    url TEXT PRIMARY KEY,
                    visited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
    
    def is_visited(self, url: str) -> bool:
        """Check if URL has been visited."""
        try:
            with sqlite3.connect(self.cache_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM visited_listings WHERE url = ?", (url,))
                return cursor.fetchone() is not None
        except sqlite3.Error:
            return False
    
    def add(self, url: str):
        """Add URL to cache."""
        try:
            with sqlite3.connect(self.cache_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO visited_listings (url, visited_at)
                    VALUES (?, CURRENT_TIMESTAMP)
                """, (url,))
                conn.commit()
        except sqlite3.Error:
            pass


def crawl_city(
    city: str,
    pages: int = 1,
    price_min: Optional[int] = None,
    price_max: Optional[int] = None,
    sqm_min: Optional[int] = None,
    sqm_max: Optional[int] = None,
    keywords: Optional[List[str]] = None,
    cache_path: Optional[str] = None,
    rate_limit_random_min: int = 5,
    rate_limit_random_max: int = 10,
    fetch_detail_pages: bool = True,
    listing_type: str = "rent_apartment"
) -> List[Dict[str, Any]]:
    """
    Crawl Anjuke listings for a city with filters.
    
    Args:
        city: City code (e.g., 'km', 'sz', 'sh', 'bj')
        pages: Number of pages to crawl
        price_min: Minimum price (monthly rent for rentals)
        price_max: Maximum price (monthly rent for rentals)
        sqm_min: Minimum square meters
        sqm_max: Maximum square meters
        keywords: Keywords to match in title/description
        cache_path: Path to SQLite cache file (optional)
        rate_limit_random_min: Minimum random delay between requests (seconds)
        rate_limit_random_max: Maximum random delay between requests (seconds)
        fetch_detail_pages: Whether to fetch individual listing detail pages
        listing_type: Type of listing (rent_apartment, rent_house, sale_apartment, sale_house)
        
    Returns:
        List of listing dictionaries with:
            - title: str
            - price: float
            - square_meters: float or None
            - url: str
            - description: str (optional)
            - matched_keywords: List[str]
    """
    if keywords is None:
        keywords = []
    
    # Build config for the scraper
    config = {
        "city": city,
        "listing_type": listing_type,
        "price_min": price_min if price_min is not None else 0,
        "price_max": price_max if price_max is not None else 999999,
        "keywords": keywords,
        "neighborhoods": [],
        "pages_to_scan": pages,
        "rate_limit_random_min": rate_limit_random_min,
        "rate_limit_random_max": rate_limit_random_max,
        "fetch_detail_pages": fetch_detail_pages,
    }
    
    # Initialize cache if path provided
    cache = None
    if cache_path:
        cache = CacheManager(cache_path)
    
    # Create scraper instance
    scraper = AnjukeScraper(config, logger)
    
    # Determine base URL
    path = LISTING_TYPE_URLS[listing_type]
    if listing_type.startswith("rent_"):
        base_url = f"https://{city}.zu.anjuke.com{path}"
    else:
        base_url = f"https://{city}.anjuke.com{path}"
    
    results = []
    
    for page in range(1, pages + 1):
        # Build page URL
        if page > 1:
            url = f"{base_url}p{page}/"
        else:
            url = base_url
        
        logger.info(f"Crawling page {page}: {url}")
        
        # Fetch page
        html = scraper.fetch_page(url)
        
        if not html:
            logger.warning(f"Failed to fetch page {page}")
            continue
        
        # Parse listings
        listings = scraper.parse_listings(html)
        
        if not listings:
            logger.info(f"No listings found on page {page}")
            break
        
        # Process each listing
        for listing in listings:
            url = listing.get("url")
            if not url:
                continue
            
            # Skip if already cached
            if cache and cache.is_visited(url):
                continue
            
            # Fetch listing detail page if enabled (applies rate limiting)
            if fetch_detail_pages:
                try:
                    detail_html = scraper.fetch_page(url)
                    
                    if detail_html:
                        # Parse detail page for additional fields
                        detail_data = scraper.parse_listing_detail(detail_html)
                        listing.update(detail_data)
                        
                except CAPTCHAException as e:
                    logger.error(f"CAPTCHA encountered, stopping: {e}")
                    return []
            
            # Extract price
            price = extract_price(listing.get("price", ""))
            
            # Extract square meters (area)
            square_meters = extract_area(listing.get("area", ""))
            
            # Apply price filter
            if price_min is not None and price is not None and price < price_min:
                continue
            if price_max is not None and price is not None and price > price_max:
                continue
            
            # Apply sqm filter
            if sqm_min is not None and square_meters is not None and square_meters < sqm_min:
                continue
            if sqm_max is not None and square_meters is not None and square_meters > sqm_max:
                continue
            
            # Apply keyword filter
            matched_keywords = []
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
                
                for kw in keywords:
                    if kw in combined:
                        matched_keywords.append(kw)
                
                if not matched_keywords:
                    continue
            
            # Build result dict with normalized field names
            result = {
                "title": listing.get("title", ""),
                "price": price if price is not None else 0.0,
                "square_meters": square_meters,
                "url": url,
                "description": listing.get("location", ""),
                "matched_keywords": matched_keywords,
            }
            
            results.append(result)
            
            # Add to cache
            if cache:
                cache.add(url)
    
    logger.info(f"Found {len(results)} matching listings")
    return results


def get_listing_url(city: str, listing_type: str = "rent_apartment") -> str:
    """
    Get the base listing URL for a city.
    
    Args:
        city: City code (e.g., 'km', 'sz', 'sh')
        listing_type: Type of listing
        
    Returns:
        Full URL string
    """
    path = LISTING_TYPE_URLS[listing_type]
    if listing_type.startswith("rent_"):
        return f"https://{city}.zu.anjuke.com{path}"
    else:
        return f"https://{city}.anjuke.com{path}"


if __name__ == "__main__":
    # Simple CLI test
    import argparse
    
    parser = argparse.ArgumentParser(description="Test crawler")
    parser.add_argument("--city", default="km", help="City code")
    parser.add_argument("--pages", type=int, default=1, help="Pages to crawl")
    args = parser.parse_args()
    
    listings = crawl_city(city=args.city, pages=args.pages)
    print(f"Found {len(listings)} listings")
    for listing in listings[:3]:
        print(f"  - {listing['title']}: {listing['price']}")
