"""Command line entry point for the elcats scraper."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .scraper import ElcatsScraper
from .storage import Storage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape https://www.elcats.ru/ into a SQLite database.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/elcats.db"),
        help="Path to the output SQLite database.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Delay in seconds between HTTP requests.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker threads to use for vehicle scraping.",
    )
    parser.add_argument(
        "--brand",
        dest="brands",
        action="append",
        help="Limit scraping to the given brand slug. Can be specified multiple times.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    storage = Storage(args.output)
    scraper = ElcatsScraper(storage, delay=args.delay, max_workers=args.workers)
    try:
        scraper.scrape(args.brands)
    finally:
        storage.close()


if __name__ == "__main__":
    main()
