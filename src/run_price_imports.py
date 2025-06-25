import logging
import sys
import argparse
import time
import threading

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
from sqlalchemy import text
from pathlib import Path
from src.import_prices import fetch_all_cards_from_api, process_and_import_prices
from src.db_utils import connect_to_db
class RateLimiter:
    """A thread-safe class to limit the rate of function calls."""
    def __init__(self, calls_per_second: float):
        self.period = 1.0 / calls_per_second
        self.lock = threading.Lock()
        self.last_call_time = 0.0

    def wait(self):
        """Blocks until it's safe to make the next call."""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call_time
            
            if elapsed < self.period:
                time.to_sleep = self.period - elapsed
                time.sleep(time.to_sleep)
            
            self.last_call_time = time.time()

# Configure logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / f'price_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def get_set_ids() -> List[str]:
    """Query the database to get all set IDs."""
    engine = connect_to_db()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT id FROM sets"))
            return [row[0] for row in result]
    except Exception as e:
        logging.error(f"Failed to get set IDs from database: {e}")
        raise

def fetch_data_from_endpoints_parallel(endpoints: List[str], max_workers: int, calls_per_second: float) -> List[Dict]:
    """
    Fetches card data from multiple API endpoints in parallel.
    
    Args:
        endpoints: List of API endpoint URLs.
        max_workers: Maximum number of parallel threads.
        
    Returns:
        A list containing all card data dictionaries from all endpoints.
    """
    total = len(endpoints)
    completed = 0
    all_fetched_cards = []
    start_time = time.time()

    # Create a single rate limiter instance to be shared by all threads
    limiter = RateLimiter(calls_per_second)

    logging.info(f"Starting parallel API fetch for {total} endpoints with {max_workers} workers.")
    logging.info(f"Rate limit set to {calls_per_second} calls/second.")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_endpoint = {
            executor.submit(fetch_all_cards_from_api, endpoint): endpoint
            for endpoint in endpoints
        }

        for future in as_completed(future_to_endpoint):
            completed += 1
            endpoint = future_to_endpoint[future]
            try:
                # fetch_all_cards_from_api returns a list of cards for that endpoint
                cards_from_endpoint = future.result()
                all_fetched_cards.extend(cards_from_endpoint)
                logging.info(f"Progress: {completed}/{total} endpoints fetched. Fetched {len(cards_from_endpoint)} cards from {endpoint}.")
            except Exception as e:
                logging.error(f"Failed to fetch data from endpoint {endpoint}: {e}")

    elapsed_time = time.time() - start_time
    logging.info(
        f"API fetching complete in {elapsed_time:.1f} seconds. "
        f"Fetched a total of {len(all_fetched_cards)} cards."
    )
    return all_fetched_cards

def main():
    parser = argparse.ArgumentParser(description='Run a unified card price import process.')
    parser.add_argument(
        '--base-url',
        default='https://api.pokemontcg.io/v2/cards?select=id,name,tcgplayer',
        help='Base API URL'
    )
    parser.add_argument(
        '--workers',
        '-w',
        type=int,
        default=5,
        help='Number of parallel workers for API fetching'
    )
    parser.add_argument(
        '--rate-limit',
        type=float,
        default=1.0,
        help='Maximum number of API calls per second'
    )
    
    args = parser.parse_args()
    
    # 1. Get set IDs and generate endpoints
    try:
        set_ids = get_set_ids()
        endpoints = [f"{args.base_url}&q=set.id:{set_id}" for set_id in set_ids]
        logging.info(f"Generated {len(endpoints)} endpoints from {len(set_ids)} set IDs in the database.")
    except Exception:
        sys.exit(1)
        
    if not endpoints:
        logging.warning("No sets found in the database to process.")
        sys.exit(0)
    
    # 2. Fetch all data from all endpoints in parallel
    all_cards = fetch_data_from_endpoints_parallel(endpoints, args.workers, args.rate_limit)
    
    if not all_cards:
        logging.warning("No card data was fetched from the API. Nothing to import.")
        sys.exit(0)
        
    # 3. Process and import all collected data in a single bulk operation
    logging.info("Starting unified database import process for all fetched cards...")
    start_time = time.time()
    try:
        process_and_import_prices(all_cards)
        elapsed = time.time() - start_time
        logging.info(f"Database import process completed successfully in {elapsed:.1f} seconds.")
    except Exception as e:
        logging.error(f"The main database import process failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()