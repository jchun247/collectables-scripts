import json
import pandas as pd
import logging
from sqlalchemy import create_engine, text, inspect
from datetime import datetime
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()
DB_URI = 'postgresql://postgres:week7day@localhost:5432/collectables?options=-c%20search_path=collectables'

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

def get_set_id_from_filename(file_path, conn):
    """Get set id from filename"""
    # Extract id from filename (remove .json extension and path)
    set_id = os.path.splitext(os.path.basename(file_path))[0]
    
    # Verify the set exists in database
    result = conn.execute(
        text("SELECT id FROM sets WHERE id = :id"),
        {"id": set_id}
    )
    if not result.fetchone():
        logging.error(f"No set found with id: {set_id}")
        raise ValueError(f"Set id '{set_id}' not found in database")
    return set_id  # Return the set_id string directly

def get_set_printedtotal(set_id, conn):
    """Get printed_total for a set"""
    result = conn.execute(
        text("SELECT printed_total FROM sets WHERE id = :id"),
        {"id": set_id}
    )
    row = result.fetchone()
    if row:
        return row[0]
    return None

def check_if_set_after_swsh(set_id, conn):
    """Check if set is after Sword & Shield base set"""
    result = conn.execute(
        text("""
            SELECT series = 'SCARLET_AND_VIOLET' OR 
                  (series = 'SWORD_AND_SHIELD' AND release_date >= 
                   (SELECT release_date FROM sets WHERE id = 'swsh1'))
            FROM sets WHERE id = :id
        """),
        {"id": set_id}
    )
    row = result.fetchone()
    return row[0] if row else False

def create_card_set_number(set_number, total_cards, is_modern_set):
    """Create a standardized card set number in format XXX/YYY"""
    if not set_number or not set_number.strip():
        return None
    
    # Remove any non-numeric characters
    numeric_part = ''.join(filter(str.isdigit, set_number))
    if not numeric_part:
        return None
    
    # Convert to integer
    number = int(numeric_part)
    
    if is_modern_set:
        # Modern sets use fixed leading zeros
        if number < 10:
            formatted_number = f"00{number}"
        elif number < 100:
            formatted_number = f"0{number}"
        else:
            formatted_number = str(number)
    else:
        # Legacy sets use padding based on total cards
        max_digits = len(str(total_cards))
        formatted_number = f"{number:0{max_digits}d}"
    
    # Append the total cards to create the full set number
    return f"{formatted_number}/{total_cards}"

def insert_cards(conn, cards_data):
    """Insert cards one at a time and return id mapping"""
    if not cards_data:
        return {}
    
    id_mapping = {}
    query = """
        INSERT INTO cards (
            name, game, set_id, set_number, rarity, illustrator_name,
            hit_points, flavour_text, type, retreat_cost,
            weakness_type, weakness_modifier, weakness_amount
        )
        VALUES (
            :name, :game, :set_id, :set_number, :rarity, :illustrator_name,
            :hit_points, :flavour_text, :type, :retreat_cost,
            :weakness_type, :weakness_modifier, :weakness_amount
        )
        RETURNING id, set_number
    """
    
    for item in cards_data:
        result = conn.execute(text(query), item)
        row = result.fetchone()
        if row:
            id_mapping[row[1]] = row[0]  # map set_number to id
    
    return id_mapping

def get_json_files(directory='./data/cards'):
    """Get all JSON files in the specified directory"""
    json_files = []
    try:
        for file in os.listdir(directory):
            if file.endswith('.json'):
                json_files.append(os.path.join(directory, file))
        return json_files
    except Exception as e:
        logging.error(f"Error reading directory {directory}: {e}")
        raise

def import_cards(file_path):
    """Import cards from a JSON file into the database"""
    try:
        # Connect to database
        engine = connect_to_db()
        
        # Validate file path
        if not os.path.isfile(file_path):
            raise ValueError(f"File not found: {file_path}")
            
        # Read JSON file
        logging.info(f"Reading data from {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logging.info(f"Processing {len(data)} cards")
        
        with engine.begin() as conn:
            # Process cards
            cards_data = []
            # Get set ID from filename
            set_id = get_set_id_from_filename(file_path, conn)
            logging.info(f"Importing cards for set ID: {set_id}")
            
            # Get total cards in set for set number formatting
            total_cards = get_set_printedtotal(set_id, conn)
            is_modern_set = check_if_set_after_swsh(set_id, conn)
            
            for item in data:
                # Extract weakness data
                weakness = item.get('weaknesses', [{}])[0] if item.get('weaknesses') else {}
                weakness_value = weakness.get('value', '')
                weakness_modifier = weakness_value[0] if weakness_value else None
                weakness_multiplier = int(weakness_value[1:]) if weakness_value else None
                
                # Get the first type if available
                type_ = item.get('types', [''])[0]
                
                # Convert HP to integer
                hp = int(item.get('hp', 0)) if item.get('hp', '').isdigit() else None
                
                # Create standardized set number
                set_number = create_card_set_number(item['number'], total_cards, is_modern_set)
                
                card_data = {
                    'name': item['name'],
                    'game': 'POKEMON',
                    'set_id': set_id,
                    'set_number': set_number,
                    'rarity': item.get('rarity'),
                    'illustrator_name': item.get('artist'),
                    'hit_points': hp,
                    'flavour_text': item.get('flavorText'),
                    'type': type_,
                    'retreat_cost': item.get('convertedRetreatCost', 0),
                    'weakness_type': weakness.get('type'),
                    'weakness_modifier': weakness_modifier,
                    'weakness_amount': weakness_multiplier
                }
                cards_data.append(card_data)
            
            # Insert cards
            logging.info(f"Inserting {len(cards_data)} cards")
            id_mapping = insert_cards(conn, cards_data)
            logging.info(f"Created {len(id_mapping)} card-to-id mappings")

        
        logging.info("Import completed successfully")
        
    except Exception as e:
        logging.error(f"Import failed: {e}")
        raise

if __name__ == '__main__':
    try:
        import sys
        file_path = sys.argv[1] if len(sys.argv) > 1 else './data/cards'
        
        if os.path.isdir(file_path):
            # Process all JSON files in directory
            json_files = get_json_files(file_path)
            if not json_files:
                logging.error(f"No JSON files found in {file_path}")
                sys.exit(1)
            
            for json_file in json_files:
                logging.info(f"Processing file: {json_file}")
                import_cards(json_file)
        else:
            # Process single file
            import_cards(file_path)
            
    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        sys.exit(1)
