# Anjuke Listing Watcher

[中文说明](./README.md)

## Features

- Support multiple listing types: rentals and sales
- Price range filtering
- Area/square meters filtering
- Keyword filtering
- Persistent cache (avoids duplicate scraping)
- File or email notifications
- Comprehensive logging

## Installation

```bash
pip install requests beautifulsoup4 pyyaml
```

## Quick Start

1. Copy the example config file
```bash
cp config.example.yaml config.yaml
```

2. Edit `config.yaml` with your settings

3. Run the program
```bash
python3 anjuke_scraper.py
```

## Configuration

| Parameter | Description | Example |
|-----------|-------------|---------|
| city | City code (2-letter pinyin) | km, sz, sh |
| listing_type | Listing type | rent_apartment, sale_apartment |
| price_min | Minimum price | 1000 |
| price_max | Maximum price | 3000 |
| area_min | Minimum area (sqm) | 40 |
| area_max | Maximum area (sqm) | 100 |
| keywords | Keyword list | ["地铁", "精装"] |
| output_mode | Output mode | file / email |

## Running

```bash
# Default run
python3 anjuke_scraper.py

# With custom config
python3 anjuke_scraper.py --config /path/to/config.yaml
```

## Running Tests

```bash
# Run all tests
pytest

# Run only integration tests
pytest -m integration

# Skip slow tests
pytest -m "not slow"
```

## Project Structure

```
.
├── anjuke_scraper.py    # Main program
├── crawler.py           # Crawler module
├── config.yaml         # Configuration file
├── config.example.yaml # Configuration template
├── cache.db           # Cache database (auto-created)
├── listings.txt       # Output results
├── tests/            # Test directory
└── pytest.ini        # pytest configuration
```

## City Codes

City codes come from the Anjuke URL, for example:
- Kunming: km
- Shenzhen: sz
- Shanghai: sh
- Beijing: bj
- Guangzhou: gz

For the complete list, visit https://www.anjuke.com
