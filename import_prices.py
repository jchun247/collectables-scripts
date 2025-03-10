import json
import logging
from sqlalchemy import create_engine, text
from datetime import datetime
import os
from dotenv import load_dotenv
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()
DB_URI = 'postgresql://postgres:week7day@localhost:5432/collectables?options=-c%20search_path=collectables'
API_TOKEN = os.getenv('API_TOKEN')

def connect_to_db():
    try:
        engine = create_engine(DB_URI)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("Database connection successful")
        return engine
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

def insert_price_data(conn, card_id, price_data, finish, updated_at):
    """Insert or update price data for a card variant, tracking price history"""
    try:
        # First check if there's an existing price record with different timestamp
        check_query = """
            SELECT id, updated_at, price 
            FROM card_price 
            WHERE card_id = :card_id 
            AND finish = :finish 
            AND condition = 'NEAR_MINT'
        """
        result = conn.execute(text(check_query), {
            'card_id': card_id,
            'finish': finish
        })
        existing_price = result.fetchone()
        new_price = price_data.get('market')

        if existing_price:
            # If timestamp is different, add to price history
            if existing_price[1] != updated_at and new_price is not None:
                history_query = """
                    INSERT INTO card_price_history (
                        card_price_id,
                        price,
                        timestamp
                    ) VALUES (
                        :card_price_id,
                        :price,
                        :timestamp
                    )
                """
                conn.execute(text(history_query), {
                    'card_price_id': existing_price[0],
                    'price': new_price,
                    'timestamp': updated_at
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
        conn.execute(text(update_query), {
            'card_id': card_id,
            'finish': finish,
            'updated_at': updated_at,
            'condition': 'NEAR_MINT',
            'price': new_price
        })
    except Exception as e:
        logging.error(f"Failed to insert price data for card {card_id} {e}")
        raise

def process_card_prices(card_data):
    """Process TCGPlayer price data for a card"""
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
            tcgplayer_data = card_data.get('tcgplayer', {})
            updated_at = datetime.strptime(tcgplayer_data['updatedAt'], '%Y/%m/%d').date()
            prices = tcgplayer_data.get('prices', {})

            # Process normal variant prices
            if 'normal' in prices:
                insert_price_data(conn, card_id, prices['normal'], 'NORMAL', updated_at)

            if 'holofoil' in prices:
                insert_price_data(conn, card_id, prices['holofoil'], 'HOLOFOIL', updated_at)

            # Process reverse holofoil variant prices
            if 'reverseHolofoil' in prices:
                insert_price_data(conn, card_id, prices['reverseHolofoil'], 'REVERSE_HOLO', updated_at)

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
