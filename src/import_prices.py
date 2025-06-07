import logging
from sqlalchemy import text
from datetime import datetime
import os
from dotenv import load_dotenv
import requests
from src.db_utils import connect_to_db

# Load API token
load_dotenv()
API_TOKEN = os.getenv('API_TOKEN')
if not API_TOKEN:
    raise ValueError("API_TOKEN environment variable is not set")

def insert_price_data(conn, card_id, price_data, finish, condition, updated_at):
    """Insert or update price data for a card variant, tracking price history.
    Only maintains 1 year of historical price data."""
    try:
        # First check if there's an existing price record with different timestamp
        check_query = """
            SELECT id, updated_at, price 
            FROM card_price 
            WHERE card_id = :card_id 
            AND finish = :finish 
            AND condition = :condition
        """
        result = conn.execute(text(check_query), {
            'card_id': card_id,
            'finish': finish,
            'condition': condition
        })
        existing_price_data = result.fetchone()

        if existing_price_data:
            # If timestamp is different, add to price history
            existing_timestamp = existing_price_data[1]
            old_price = existing_price_data[2]
            if existing_timestamp != updated_at and old_price is not None:
                history_query = """
                    INSERT INTO card_price_history (
                        card_id,
                        finish,
                        condition,
                        price,
                        timestamp
                    ) VALUES (
                        :card_id,
                        :finish,
                        :condition,
                        :price,
                        :timestamp
                    )
                """
                # Insert the historical price
                conn.execute(text(history_query), {
                    'card_id': card_id,
                    'finish': finish,
                    'condition': condition,
                    'price': old_price,
                    'timestamp': existing_timestamp
                })

        # Now proceed with normal price update
        update_query = """
            INSERT INTO card_price (
                card_id,
                finish,
                updated_at,
                condition,
                price
            )
            VALUES (
                :card_id,
                :finish,
                :updated_at,
                :condition,
                :price
            )
            ON CONFLICT ON CONSTRAINT card_price_card_id_finish_condition_key
            DO UPDATE SET
                price = EXCLUDED.price,
                updated_at = EXCLUDED.updated_at
        """
        new_price = price_data.get('market')
        if new_price is None:
            new_price = price_data.get('mid')
        if new_price is None:
            new_price = price_data.get('low')
        if new_price is None:
            new_price = price_data.get('high')
        if new_price is None:
            new_price = price_data.get('directLow')
            
        conn.execute(text(update_query), {
            'card_id': card_id,
            'finish': finish,
            'updated_at': updated_at,
            'condition': condition,
            'price': new_price
        })
    except Exception as e:
        logging.error(f"Failed to insert price data for card {card_id} {e}")
        raise

def process_card_prices(card_data):
    """Process price data for a card"""
    try:
        engine = connect_to_db()
        
        with engine.begin() as conn:
            # Get card ID from external_id
            result = conn.execute(
                text("SELECT id FROM cards WHERE external_id = :external_id"),
                {"external_id": card_data['id']}
            )
            card_row = result.fetchone()
            if not card_row:
                logging.warning(f"Card not found with external ID: {card_data['id']}")
                return

            card_id = card_row[0]

            # Get the current date for this run, with time set to midnight for consistency
            # This ensures that if the script is run multiple times on the same day,
            # the timestamp remains the same, aligning with previous date-only precision.
            updated_at = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            tcgplayer_data = card_data.get('tcgplayer', {})
            if not tcgplayer_data:
                # If tcgplayer_data itself is missing, we can't get prices from it.
                logging.info(f"No TCGPlayer data (price source) available for card {card_data['id']}, skipping")
                return
                
            # Note: The 'updatedAt' field from tcgplayer_data is no longer the source for the main timestamp.
            # The timestamp for price records is now based on the execution time of this script.
            
            prices = tcgplayer_data.get('prices', {})
            condition = 'NEAR_MINT'

            # Process normal variant prices
            if 'normal' in prices:
                insert_price_data(conn, card_id, prices['normal'], 'NORMAL', condition, updated_at)

            if 'holofoil' in prices:
                insert_price_data(conn, card_id, prices['holofoil'], 'HOLOFOIL', condition, updated_at)

            # Process reverse holofoil variant prices
            if 'reverseHolofoil' in prices:
                insert_price_data(conn, card_id, prices['reverseHolofoil'], 'REVERSE_HOLO', condition, updated_at)

            logging.info(f"Successfully processed prices for card ID: {card_id}")

    except Exception as e:
        logging.error(f"Error processing prices: {e}")
        raise

def import_prices_from_api(api_url):
    """Import card prices from the API"""
    try:
        all_cards = []
        page = 1
        
        while True:
            # Construct paginated URL
            paginated_url = f"{api_url}{'&' if '?' in api_url else '?'}page={page}"
            logging.info(f"Fetching page {page} from API: {paginated_url}")
            
            headers = {'Authorization': f'Bearer {API_TOKEN}'}
            response = requests.get(paginated_url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            page_cards = data.get('data', [])  # Assuming cards are in 'data' field
            all_cards.extend(page_cards)
            
            # Check if we've received all pages
            total_count = data.get('totalCount', 0)
            current_count = len(all_cards)
            
            logging.info(f"Retrieved {len(page_cards)} cards from page {page}. "
                        f"Total progress: {current_count}/{total_count}")
            
            # Break if we've fetched all cards or if there's no more data
            if current_count >= total_count or not page_cards:
                break
                
            page += 1
            
        logging.info(f"Retrieved total of {len(all_cards)} cards with price data")
        
        # Process all cards after fetching all pages
        for card in all_cards:
            process_card_prices(card)
            
        logging.info("Price import completed successfully")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Price import failed: {e}")
        raise

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python import_prices.py <api_url>")
        sys.exit(1)
        
    api_url = sys.argv[1]
    try:
        import_prices_from_api(api_url)
    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        sys.exit(1)
