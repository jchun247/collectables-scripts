import logging
import os
import requests
import sys
from sqlalchemy import text, create_engine
from datetime import datetime
from dotenv import load_dotenv
from src.db_utils import get_engine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Load environment variables
load_dotenv()
API_TOKEN = os.getenv('API_TOKEN')
if not API_TOKEN:
    raise ValueError("API_TOKEN environment variable is not set")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@retry(
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((requests.exceptions.HTTPError, requests.exceptions.ConnectionError)),
    before_sleep=lambda retry_state: logging.warning(f"Retrying API call due to: {retry_state.outcome.exception()}. Attempt #{retry_state.attempt_number}")
)
def get_with_retry(url: str, headers: dict):
    """Makes a GET request and raises an exception for bad status codes."""
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response

def get_price(price_data):
    """Select the price from the available TCGPlayer price data fields."""
    return (
        price_data.get('market') or
        price_data.get('mid') or
        price_data.get('low') or
        price_data.get('high') or
        price_data.get('directLow')
    )

def fetch_all_cards_from_api(api_url, rate_limiter=None):
    """Fetch all card data from the paginated API."""
    all_cards = []
    page = 1
    headers = {'Authorization': f'Bearer {API_TOKEN}'}
    
    while True:
        paginated_url = f"{api_url}{'&' if '?' in api_url else '?'}page={page}"
        
        try:
            if rate_limiter:
                rate_limiter.wait() # This will pause if we're going too fast

            logging.info(f"Fetching page {page} from API...")
            response = get_with_retry(paginated_url, headers)

            data = response.json()
            page_cards = data.get('data', [])
            if not page_cards:
                logging.info("No more cards found on this page. Concluding API fetch.")
                break
                
            all_cards.extend(page_cards)
            
            total_count = data.get('totalCount', 0)
            logging.info(f"Retrieved {len(page_cards)} cards. Total progress: {len(all_cards)}/{total_count}")

            if len(all_cards) >= total_count:
                logging.info("All cards have been fetched from the API.")
                break
                
            page += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {e}")
            raise
            
    return all_cards

def process_and_import_prices(all_cards):
    """
    Process card prices in bulk and import them into the database.
    This function is designed to minimize database round-trips.
    """
    if not all_cards:
        logging.info("No cards to process.")
        return

    try:
        engine = get_engine()

        with engine.begin() as conn:
            # === Step 1: Map external IDs to internal database IDs in one query ===
            external_ids = [card['id'] for card in all_cards]
            id_map_query = text("SELECT external_id, id FROM cards WHERE external_id = ANY(:external_ids)")
            result = conn.execute(id_map_query, {'external_ids': external_ids})
            card_id_map = {row[0]: row[1] for row in result}
            
            logging.info(f"Mapped {len(card_id_map)} external IDs to internal database IDs.")

            # === Step 2: Fetch all existing prices for these cards in one query ===
            internal_card_ids = list(card_id_map.values())
            existing_prices_query = text("""
                SELECT card_id, finish, condition, price, updated_at 
                FROM card_price 
                WHERE card_id = ANY(:card_ids)
            """)
            result = conn.execute(existing_prices_query, {'card_ids': internal_card_ids})
            
            existing_prices = {}
            for row in result:
                key = (row.card_id, row.finish, row.condition)
                existing_prices[key] = {'price': row.price, 'timestamp': row.updated_at}

            logging.info(f"Fetched {len(existing_prices)} existing price records from the database.")

            # === Step 3: Prepare price data in memory ===
            updated_at = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            price_updates = []
            price_history = []

            price_variants = {
                'normal': 'NORMAL',
                'holofoil': 'HOLOFOIL',
                'reverseHolofoil': 'REVERSE_HOLO',
                '1stEditionHolofoil': 'FIRST_EDITION_HOLOFOIL',
                '1stEditionNormal': 'FIRST_EDITION_NORMAL',
                '1stEdition': 'FIRST_EDITION',
                'unlimitedHolofoil': 'UNLIMITED_HOLOFOIL',
                'unlimited': 'UNLIMITED',
            }

            for card_data in all_cards:
                external_id = card_data['id']
                internal_id = card_id_map.get(external_id)

                if not internal_id:
                    logging.warning(f"Card with external ID {external_id} not found in the database. Skipping.")
                    continue

                tcgplayer_prices = card_data.get('tcgplayer', {}).get('prices', {})
                if not tcgplayer_prices:
                    continue

                for price_key, finish_enum in price_variants.items():
                    if price_key in tcgplayer_prices:
                        price_info = tcgplayer_prices[price_key]
                        new_price = get_price(price_info)

                        if new_price is not None:
                            # Prepare for main price table update/insert
                            price_updates.append({
                                'card_id': internal_id,
                                'finish': finish_enum,
                                'condition': 'NEAR_MINT',
                                'price': new_price,
                                'updated_at': updated_at
                            })

                            # Check if we need to create a history record
                            existing = existing_prices.get((internal_id, finish_enum, 'NEAR_MINT'))
                            if existing and existing['timestamp'] != updated_at and existing['price'] is not None:
                                price_history.append({
                                    'card_id': internal_id,
                                    'finish': finish_enum,
                                    'condition': 'NEAR_MINT',
                                    'price': existing['price'],
                                    'timestamp': existing['timestamp']
                                })
            
            logging.info(f"Prepared {len(price_updates)} price updates and {len(price_history)} history records.")

            # === Step 4: Execute bulk insert for price history ===
            if price_history:
                history_insert_query = text("""
                    INSERT INTO card_price_history (card_id, finish, condition, price, timestamp)
                    VALUES (:card_id, :finish, :condition, :price, :timestamp)
                """)
                conn.execute(history_insert_query, price_history)
                logging.info(f"Inserted {len(price_history)} records into card_price_history.")

            # === Step 5: Execute bulk insert/update for current prices ===
            if price_updates:
                price_update_query = text("""
                    INSERT INTO card_price (card_id, finish, condition, price, updated_at)
                    VALUES (:card_id, :finish, :condition, :price, :updated_at)
                    ON CONFLICT ON CONSTRAINT card_price_card_id_finish_condition_key
                    DO UPDATE SET
                        price = EXCLUDED.price,
                        updated_at = EXCLUDED.updated_at
                """)
                conn.execute(price_update_query, price_updates)
                logging.info(f"Upserted {len(price_updates)} records into card_price.")

            logging.info("Bulk price import completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during the bulk price import process: {e}")
        raise

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python import_prices.py <api_url>")
        sys.exit(1)
        
    api_url = sys.argv[1]
    try:
        all_cards_data = fetch_all_cards_from_api(api_url)
        process_and_import_prices(all_cards_data)
        logging.info("Script execution finished.")
    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        sys.exit(1)