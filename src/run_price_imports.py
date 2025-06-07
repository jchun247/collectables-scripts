import subprocess
import logging
from datetime import datetime
import sys
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List
import time
from pathlib import Path
from src.db_utils import connect_to_db
from sqlalchemy import text

# Configure logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f'price_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

def setup_logging():
    """Configure logging with proper handling of multiprocessing"""
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Create file handler
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Remove any existing handlers to avoid duplicates
    root_logger.handlers = []
    
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger

# Initialize logging
setup_logging()

def process_endpoint(endpoint: str, max_retries: int = 3, retry_delay: int = 10) -> tuple[str, bool]:
    """
    Process a single endpoint with retry logic
    
    Args:
        endpoint: The API endpoint URL
        max_retries: Maximum number of retry attempts
        retry_delay: Delay in seconds between retries
    
    Returns:
        tuple of (endpoint, success_status)
    """
    # Set up logging for this process
    setup_logging()
    for attempt in range(max_retries):
        try:
            result = subprocess.run(
                [sys.executable, "import_prices.py", endpoint],
                capture_output=True,
                text=True,
                check=True
            )
            logging.info(f"Successfully processed endpoint {endpoint}")
            logging.debug(f"Output: {result.stdout}")
            return endpoint, True
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Attempt {attempt + 1}/{max_retries} failed for endpoint {endpoint}")
            logging.error(f"Error output: {e.stderr}")
            
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error(f"All attempts failed for endpoint {endpoint}")
                return endpoint, False
                
        except Exception as e:
            logging.error(f"Unexpected error processing endpoint {endpoint}: {str(e)}")
            return endpoint, False
    
    return endpoint, False

def run_imports(endpoints: List[str], max_workers: int = 3):
    """
    Run imports in parallel with progress tracking
    
    Args:
        endpoints: List of API endpoints to process
        max_workers: Maximum number of parallel processes
    """
    total = len(endpoints)
    completed = 0
    successful = 0
    start_time = time.time()
    
    logging.info(f"Starting parallel import of {total} endpoints with {max_workers} workers")
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_endpoint = {
            executor.submit(process_endpoint, endpoint): endpoint 
            for endpoint in endpoints
        }
        
        # Process completed tasks as they finish
        for future in as_completed(future_to_endpoint):
            completed += 1
            endpoint = future_to_endpoint[future]
            
            try:
                _, success = future.result()
                if success:
                    successful += 1
                
                # Calculate progress and estimated time remaining
                elapsed_time = time.time() - start_time
                avg_time_per_endpoint = elapsed_time / completed
                remaining_endpoints = total - completed
                estimated_remaining = avg_time_per_endpoint * remaining_endpoints
                
                logging.info(
                    f"Progress: {completed}/{total} endpoints processed "
                    f"({successful} successful) - "
                    f"Est. remaining time: {estimated_remaining:.1f} seconds"
                )
                
            except Exception as e:
                logging.error(f"Error processing {endpoint}: {str(e)}")
    
    # Final summary
    elapsed_time = time.time() - start_time
    logging.info(
        f"Import completed in {elapsed_time:.1f} seconds - "
        f"Processed {total} endpoints with {successful} successful imports"
    )

def get_set_ids():
    """Query the database to get all set IDs"""
    engine = connect_to_db()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT id FROM sets"))
            return [row[0] for row in result]
    except Exception as e:
        logging.error(f"Failed to get set IDs from database: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Run card price imports in parallel')
    parser.add_argument(
        '--base-url',
        default='https://api.pokemontcg.io/v2/cards?select=id,name,tcgplayer',
        help='Base API URL (default: https://api.pokemontcg.io/v2/cards?select=id,name,tcgplayer)'
    )
    parser.add_argument(
        '--workers',
        '-w',
        type=int,
        default=3,
        help='Number of parallel workers (default: 3)'
    )
    
    args = parser.parse_args()
    
    # Get endpoints from the database
    try:
        set_ids = get_set_ids()
        endpoints = [f"{args.base_url}&q=set.id:{set_id}" for set_id in set_ids]
        logging.info(f"Generated {len(endpoints)} endpoints from set IDs in database")
    except Exception as e:
        logging.error(f"Failed to generate endpoints from database: {e}")
        sys.exit(1)
        
    if not endpoints:
        logging.error("No sets found in database")
        sys.exit(1)
    
    logging.info(f"Starting price import batch process with {len(endpoints)} endpoints")
    run_imports(endpoints, args.workers)

if __name__ == '__main__':
    main()
