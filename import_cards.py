import json
import logging
from sqlalchemy import text, inspect
import os
from db_utils import connect_to_db

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
    """Create a standardized card set number in format XXX/YYY or PREFIX00/PREFIX00"""
    if not set_number or not set_number.strip():
        return None

    # Check for gallery patterns (TG01/TG30 or GG04/GG70)
    import re
    gallery_match = re.match(r'^(TG|GG)(\d+)(?:/(?:TG|GG)\d+)?$', set_number)
    if gallery_match:
        prefix = gallery_match.group(1)  # TG or GG
        number = int(gallery_match.group(2))
        # Always use 2-digit padding for gallery cards
        formatted_number = f"{number:02d}"
        return f"{prefix}{formatted_number}/{prefix}{total_cards}"

    # Handle regular card numbers
    parts = set_number.split('/')
    if not parts:
        return None
        
    original_number = parts[0]
    
    # Extract first group of digits
    match = re.search(r'(\d+)', original_number)
    if not match:
        return None
        
    # Get position where the number starts and ends
    start = match.start()
    end = match.end()
    
    # Get the parts before, during, and after the number
    prefix = original_number[:start]
    number = int(match.group(1))
    suffix = original_number[end:]
    
    if is_modern_set:
        # Modern sets use fixed leading zeros
        if number < 10:
            formatted_number = f"00{number}"
        elif number < 100:
            formatted_number = f"0{number}"
        else:
            formatted_number = str(number)
    else:
        # Legacy sets use original number without padding
        formatted_number = str(number)
    
    # Reconstruct the number with any original prefix/suffix preserved exactly
    return f"{prefix}{formatted_number}{suffix}/{total_cards}"

def insert_card(conn, card_data):
    """Insert a single card and return its id and set number"""
    query = """
        INSERT INTO cards (
            name, game, external_id, set_id, set_number, rarity, illustrator_name,
            hit_points, flavour_text, retreat_cost,
            weakness_type, weakness_modifier, weakness_amount,
            resistance_type, resistance_modifier, resistance_amount
        )
        VALUES (
            :name, :game, :external_id, :set_id, :set_number, :rarity, :illustrator_name,
            :hit_points, :flavour_text, :retreat_cost,
            :weakness_type, :weakness_modifier, :weakness_amount,
            :resistance_type, :resistance_modifier, :resistance_amount
        )
        RETURNING id, set_number
    """
    result = conn.execute(text(query), card_data)
    return result.fetchone()

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
            # Get set ID from filename
            set_id = get_set_id_from_filename(file_path, conn)
            logging.info(f"Importing cards for set ID: {set_id}")
            
            # Get total cards in set for set number formatting
            total_cards = get_set_printedtotal(set_id, conn)
            is_modern_set = check_if_set_after_swsh(set_id, conn)
            
            # Process and insert all card data in a single pass
            cards_processed = 0
            for item in data:
                # Extract weakness data
                weakness = item.get('weaknesses', [{}])[0] if item.get('weaknesses') else {}
                weakness_value = weakness.get('value', '')
                weakness_modifier = weakness_value[0] if weakness_value else None
                weakness_multiplier = int(weakness_value[1:]) if weakness_value else None
                
                # Extract resistance data
                resistance = item.get('resistances', [{}])[0] if item.get('resistances') else {}
                resistance_value = resistance.get('value', '')
                resistance_modifier = resistance_value[0] if resistance_value else None
                resistance_multiplier = int(resistance_value[1:]) if resistance_value else None
                
                # Convert HP to integer
                hp = int(item.get('hp', 0)) if item.get('hp', '').isdigit() else None
                
                # Create standardized set number
                set_number = create_card_set_number(item['number'], total_cards, is_modern_set)
                
                card_data = {
                    'name': item['name'],
                    'external_id': item['id'],
                    'game': 'POKEMON',
                    'set_id': set_id,
                    'set_number': set_number,
                    'rarity': item.get('rarity'),
                    'illustrator_name': item.get('artist'),
                    'hit_points': hp,
                    'flavour_text': item.get('flavorText'),
                    'retreat_cost': item.get('convertedRetreatCost', 0),
                    'weakness_type': weakness.get('type'),
                    'weakness_modifier': weakness_modifier,
                    'weakness_amount': weakness_multiplier,
                    'resistance_type': resistance.get('type'),
                    'resistance_modifier': resistance_modifier,
                    'resistance_amount': resistance_multiplier
                }
                
                # Insert card and get ID
                row = insert_card(conn, card_data)
                if row:
                    cards_processed += 1
                    card_id = row[0]
                    
                    # Process attacks and attack costs for this card immediately
                    for attack in item.get('attacks', []):
                        # Insert attack
                        attack_result = conn.execute(
                            text("""
                                INSERT INTO card_attacks (card_id, name, damage, text)
                                VALUES (:card_id, :name, :damage, :text)
                                RETURNING id
                            """),
                            {
                                'card_id': card_id,
                                'name': attack.get('name'),
                                'damage': attack.get('damage'),
                                'text': attack.get('text')
                            }
                        )
                        attack_id = attack_result.fetchone()[0]
                        
                        # Handle attack costs
                        costs = attack.get('cost', [])
                        if not costs:
                            # If attack has no energy cost requirements, store as 'FREE'
                            conn.execute(
                                text("""
                                    INSERT INTO card_attack_costs (attack_id, cost)
                                    VALUES (:attack_id, :cost)
                                """),
                                {
                                    'attack_id': attack_id,
                                    'cost': 'FREE'
                                }
                            )
                        else:
                            # Insert each energy cost
                            for cost in costs:
                                conn.execute(
                                    text("""
                                        INSERT INTO card_attack_costs (attack_id, cost)
                                        VALUES (:attack_id, :cost)
                                    """),
                                    {
                                        'attack_id': attack_id,
                                        'cost': cost
                                    }
                                )

                    # Process card abilities
                    for ability in item.get('abilities', []):
                        conn.execute(
                            text("""
                                INSERT INTO card_abilities (card_id, name, text, type)
                                VALUES (:card_id, :name, :text, :type)
                            """),
                            {
                                'card_id': card_id,
                                'name': ability.get('name'),
                                'text': ability.get('text'),
                                'type': ability.get('type')
                            }
                        )

                    # Process card types
                    for card_type in item.get('types', []):
                        conn.execute(
                            text("""
                                INSERT INTO card_types (card_id, type)
                                VALUES (:card_id, :type)
                            """),
                            {
                                'card_id': card_id,
                                'type': card_type
                            }
                        )

                    # Process card subtypes
                    for subtype in item.get('subtypes', []):
                        conn.execute(
                            text("""
                                INSERT INTO card_subtypes (card_id, subtype)
                                VALUES (:card_id, :subtype)
                            """),
                            {
                                'card_id': card_id,
                                'subtype': subtype
                            }
                        )
                    
                    # Process card images
                    for resolution, url in item.get('images', {}).items():
                        conn.execute(
                            text("""
                                INSERT INTO card_images (card_id, resolution, url)
                                VALUES (:card_id, :resolution, :url)
                            """),
                            {
                                'card_id': card_id,
                                'resolution': resolution,
                                'url': url
                            }
                        )

                
            logging.info(f"Processed {cards_processed} cards")

        
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
