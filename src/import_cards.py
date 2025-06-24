import json
import logging
from sqlalchemy import text, inspect
import os
from src.db_utils import connect_to_db
from data.promo_set_formatting_rules import PROMO_SET_RULES
from data.gallery_set_formatting_rules import format_gallery_card_number

def get_series_from_set_id(set_id):
    """Extracts the series from the set ID, removing 'p' if present."""
    if set_id.endswith('p'):
        return set_id[:-1]
    return set_id

def get_set_details(set_id, conn):
    """
    Gets all required set details in a single query.
    """
    query = text("""
        SELECT 
            id, 
            printed_total,
            series = 'SCARLET_AND_VIOLET' OR 
            (series = 'SWORD_AND_SHIELD' AND release_date >= 
             (SELECT release_date FROM sets WHERE id = 'swsh1')) as is_modern
        FROM sets 
        WHERE id = :id
    """)
    result = conn.execute(query, {"id": set_id}).fetchone()
    if not result:
        # get_set_id_from_filename already handles the error, but this is good practice
        raise ValueError(f"Set id '{set_id}' not found in database")
    return result

def create_card_set_number(set_id, set_number, total_cards, is_modern_set):
    """Create a standardized card set number in format XXX/YYY or PREFIX00/PREFIX00"""
    if not set_number or not set_number.strip():
        return None
    
    # Handle promo sets (sets ending in 'p')
    if set_id.endswith('p'):
        series = get_series_from_set_id(set_id)
        rule = PROMO_SET_RULES.get(set_id, PROMO_SET_RULES["default"])
        return rule(series, set_number)

    # Handle gallery sets (ending in 'tg' or 'gg')
    if set_id.endswith('tg') or set_id.endswith('gg'):
        return format_gallery_card_number(set_id, set_number, total_cards)

    # Handle regular card numbers
    import re

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
        # :03d means "format as a decimal, padded with leading zeros to 3 digits".
        formatted_number = f"{number:03d}"
    else:
        # Legacy sets use original number without padding
        formatted_number = str(number)
    
    # Reconstruct the number with any original prefix/suffix preserved exactly
    return f"{prefix}{formatted_number}{suffix}/{total_cards}"

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

def _sync_cards_table_bulk(conn, card_data_list):
    """
    Handles the bulk "upsert" for the main cards table.
    Returns a dictionary mapping {external_id: database_id}.
    """
    if not card_data_list:
        return {}

    # This statement inserts new cards. If a card with the same external_id and set_id
    # already exists (violating a unique constraint), it updates the specified fields instead.
    # It then returns the definitive database ID and external_id for all processed rows.
    stmt = text("""
        INSERT INTO cards (
            external_id, set_id, name, set_number, rarity, illustrator_name, supertype, game
        )
        VALUES (
            :external_id, :set_id, :name, :set_number, :rarity, :illustrator_name, :supertype, 'POKEMON'
        )
        ON CONFLICT (external_id, set_id) DO UPDATE SET
            name = EXCLUDED.name,
            set_number = EXCLUDED.set_number,
            rarity = EXCLUDED.rarity,
            illustrator_name = EXCLUDED.illustrator_name,
            supertype = EXCLUDED.supertype
        RETURNING id, external_id;
    """)
    
    result = conn.execute(stmt, card_data_list)
    return {row.external_id: row.id for row in result}

def _sync_pokemon_details_bulk(conn, pokemon_details_list, external_id_to_db_id):
    """
    Handles the bulk "upsert" for the card_pokemon_details table.
    Returns a dictionary mapping {database_card_id: pokemon_details_id}.
    """
    if not pokemon_details_list:
        return {}
        
    # Before inserting, replace the temporary external_id with the real card_id from the database.
    for detail in pokemon_details_list:
        detail['card_id'] = external_id_to_db_id.get(detail['external_id'])
        if not detail['card_id']:
            logging.warning(f"Skipping Pokémon detail for missing card external_id: {detail['external_id']}")

    # Filter out any details that couldn't be mapped to a card_id
    valid_details = [d for d in pokemon_details_list if 'card_id' in d and d['card_id'] is not None]
    if not valid_details:
        return {}

    stmt = text("""
        INSERT INTO card_pokemon_details (
            card_id, hit_points, retreat_cost, flavour_text,
            weakness_type, weakness_modifier, weakness_value,
            resistance_type, resistance_modifier, resistance_value
        ) VALUES (
            :card_id, :hit_points, :retreat_cost, :flavour_text,
            :weakness_type, :weakness_modifier, :weakness_value,
            :resistance_type, :resistance_modifier, :resistance_value
        )
        ON CONFLICT (card_id) DO UPDATE SET
            hit_points = EXCLUDED.hit_points,
            retreat_cost = EXCLUDED.retreat_cost,
            flavour_text = EXCLUDED.flavour_text,
            weakness_type = EXCLUDED.weakness_type,
            weakness_modifier = EXCLUDED.weakness_modifier,
            weakness_value = EXCLUDED.weakness_value,
            resistance_type = EXCLUDED.resistance_type,
            resistance_modifier = EXCLUDED.resistance_modifier,
            resistance_value = EXCLUDED.resistance_value
        RETURNING id, card_id;
    """)
    
    result = conn.execute(stmt, valid_details)
    return {row.card_id: row.id for row in result}

def _sync_attacks_and_costs_bulk(conn, attacks_list, details_id_map, external_id_to_db_id):
    """
    Handles the complex bulk insert for attacks and their associated costs.
    """
    if not attacks_list:
        return

    # Map the pokemon_details_id to each attack
    for attack in attacks_list:
        card_id = external_id_to_db_id.get(attack['external_id'])
        attack['card_pokemon_details_id'] = details_id_map.get(card_id)

    valid_attacks = [a for a in attacks_list if a.get('card_pokemon_details_id') is not None]
    if not valid_attacks:
        return

    # 1. Bulk insert all attacks and get their new database IDs
    attack_stmt = text("""
        INSERT INTO card_attacks (card_pokemon_details_id, name, damage, text)
        VALUES (:card_pokemon_details_id, :name, :damage, :text)
        RETURNING id, card_pokemon_details_id, name;
    """)
    
    inserted_attacks = conn.execute(attack_stmt, valid_attacks).fetchall()

    # 2. Create a map to link an attack (by details_id and name) to its new attack_id
    attack_id_map = {
        (attack.card_pokemon_details_id, attack.name): attack.id 
        for attack in inserted_attacks
    }

    # 3. Prepare the list of costs for bulk insertion
    costs_to_insert = []
    for attack_data in valid_attacks:
        attack_id = attack_id_map.get((attack_data['card_pokemon_details_id'], attack_data['name']))
        if attack_id:
            for cost in attack_data.get('cost', ['FREE']):
                costs_to_insert.append({'attack_id': attack_id, 'cost': cost})

    # 4. Bulk insert all costs
    if costs_to_insert:
        cost_stmt = text("INSERT INTO card_attack_costs (attack_id, cost) VALUES (:attack_id, :cost)")
        conn.execute(cost_stmt, costs_to_insert)

def sync_all_data_bulk(conn, set_id, card_data_list, pokemon_details_list, subtypes_list, images_list, rules_list, attacks_list, abilities_list, types_list):
    """
    Orchestrates the entire bulk sync process for a set using the "Delete-Then-Insert" pattern.
    """
    # Step 1: Sync the main 'cards' table and get a map of {external_id -> db_id}
    logging.info("Syncing main card data...")
    external_id_to_db_id = _sync_cards_table_bulk(conn, card_data_list)
    
    if not external_id_to_db_id:
        logging.warning(f"No cards were synced for set {set_id}. Aborting.")
        return

    all_card_ids = list(external_id_to_db_id.values())

    # Step 2: Sync 'card_pokemon_details' and get a map of {card_id -> details_id}
    logging.info("Syncing Pokémon details...")
    details_id_map = _sync_pokemon_details_bulk(conn, pokemon_details_list, external_id_to_db_id)
    all_details_ids = list(details_id_map.values())

    # Step 3: Clean slate - Bulk delete all existing child data for the set.
    # Assumes ON DELETE CASCADE is set up for card_attack_costs. If not, delete costs first.
    logging.info("Deleting old child data...")
    if all_details_ids:
        conn.execute(text("DELETE FROM card_attacks WHERE card_pokemon_details_id = ANY(:details_ids)"), {"details_ids": all_details_ids})
        conn.execute(text("DELETE FROM card_abilities WHERE card_pokemon_details_id = ANY(:details_ids)"), {"details_ids": all_details_ids})
        conn.execute(text("DELETE FROM card_types WHERE card_pokemon_details_id = ANY(:details_ids)"), {"details_ids": all_details_ids})

    conn.execute(text("DELETE FROM card_subtypes WHERE card_id = ANY(:card_ids)"), {"card_ids": all_card_ids})
    conn.execute(text("DELETE FROM card_images WHERE card_id = ANY(:card_ids)"), {"card_ids": all_card_ids})
    conn.execute(text("DELETE FROM card_rules WHERE card_id = ANY(:card_ids)"), {"card_ids": all_card_ids})
    
    # Step 4: Bulk insert all new child data.
    logging.info("Inserting new child data...")

    # Map external_id to card_id for insertion
    for s in subtypes_list: s['card_id'] = external_id_to_db_id.get(s.pop('external_id'))
    if subtypes_list: conn.execute(text("INSERT INTO card_subtypes (card_id, subtype) VALUES (:card_id, :subtype)"), [s for s in subtypes_list if s['card_id']])
    
    for i in images_list: i['card_id'] = external_id_to_db_id.get(i.pop('external_id'))
    if images_list: conn.execute(text("INSERT INTO card_images (card_id, resolution, url) VALUES (:card_id, :resolution, :url)"), [i for i in images_list if i['card_id']])

    for r in rules_list: r['card_id'] = external_id_to_db_id.get(r.pop('external_id'))
    if rules_list: conn.execute(text("INSERT INTO card_rules (card_id, text) VALUES (:card_id, :text)"), [r for r in rules_list if r['card_id']])
    
    # Map external_id to pokemon_details_id for insertion
    for a in abilities_list: 
        card_id = external_id_to_db_id.get(a.pop('external_id'))
        a['card_pokemon_details_id'] = details_id_map.get(card_id)
    if abilities_list: conn.execute(text("INSERT INTO card_abilities (card_pokemon_details_id, name, text, type) VALUES (:card_pokemon_details_id, :name, :text, :type)"), [a for a in abilities_list if a['card_pokemon_details_id']])

    for t in types_list:
        card_id = external_id_to_db_id.get(t.pop('external_id'))
        t['card_pokemon_details_id'] = details_id_map.get(card_id)
    if types_list: conn.execute(text("INSERT INTO card_types (card_pokemon_details_id, type) VALUES (:card_pokemon_details_id, :type)"), [t for t in types_list if t['card_pokemon_details_id']])

    # Handle attacks and costs
    _sync_attacks_and_costs_bulk(conn, attacks_list, details_id_map, external_id_to_db_id)

    logging.info("Bulk sync complete.")

def import_cards(file_path):
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
        
        # Prepares all data in memory before syncing with the database"""
        card_data_list = []
        pokemon_details_list = []
        subtypes_list = []
        images_list = []
        rules_list = []
        attacks_list = []
        abilities_list = []
        types_list = []
        
        logging.info(f"Processing {len(data)} cards")
        
        with engine.begin() as conn:
            # Retrieve set data
            set_id = os.path.splitext(os.path.basename(file_path))[0]
            set_details = get_set_details(set_id, conn)
            total_cards = set_details.printed_total
            is_modern_set = set_details.is_modern

            logging.info(f"Importing cards for set ID: {set_id}")
            
            # Process and insert all card data in a single pass
            cards_processed = 0
            for item in data:
                # Create standardized set number
                set_number = create_card_set_number(set_id, item['number'], total_cards, is_modern_set)
                
                # Base card data that applies to all types
                card_data = {
                    'name': item['name'],
                    'external_id': item['id'],
                    'set_id': set_id,
                    'set_number': set_number,
                    'rarity': item.get('rarity'),
                    'illustrator_name': item.get('artist'),
                    'supertype': item.get('supertype')
                }

                card_data_list.append(card_data)
                # Add related data to their respective lists, keyed by external_id
                subtypes_list.extend([{'external_id': item['id'], 'subtype': s} for s in item.get('subtypes', [])])
                images_list.extend([{'external_id': item['id'], 'resolution': r, 'url': u} for r, u in item.get('images', {}).items()])
                rules_list.extend([{'external_id': item['id'], 'text': r} for r in item.get('rules', [])])

                # Pokemon specific data
                if item.get('supertype') == 'Pokémon':
                    # Extract weakness data
                    weakness = item.get('weaknesses', [{}])[0] if item.get('weaknesses') else {}
                    raw_weakness_value = weakness.get('value', '')
                    weakness_modifier = raw_weakness_value[0] if raw_weakness_value else None
                    weakness_value = None
                    # SAFER PARSING for weakness value:
                    if len(raw_weakness_value) > 1:
                        try:
                            weakness_value = int(raw_weakness_value[1:])
                        except ValueError:
                            logging.warning(f"Could not parse weakness value from '{raw_weakness_value}' for card {item['id']}")
                    
                    # Extract resistance data
                    resistance = item.get('resistances', [{}])[0] if item.get('resistances') else {}
                    raw_resistance_value = resistance.get('value', '')
                    resistance_modifier = raw_resistance_value[0] if raw_resistance_value else None
                    resistance_value = None
                    # SAFER PARSING for resistance value:
                    if len(raw_resistance_value) > 1:
                        try:
                            resistance_value = int(raw_resistance_value[1:])
                        except ValueError:
                            logging.warning(f"Could not parse resistance value from '{raw_resistance_value}' for card {item['id']}")
                    
                    # Convert HP to integer
                    hp = int(item.get('hp', 0)) if item.get('hp', '').isdigit() else None

                    pokemon_data = {
                        'external_id': item['id'],
                        'flavour_text': item.get('flavorText'),
                        'hit_points': hp,
                        'retreat_cost': item.get('convertedRetreatCost', 0),
                        'weakness_type': weakness.get('type'),
                        'weakness_modifier': weakness_modifier,
                        'weakness_value': weakness_value,
                        'resistance_type': resistance.get('type'),
                        'resistance_modifier': resistance_modifier,
                        'resistance_value': resistance_value
                    }
                    pokemon_details_list.append(pokemon_data)
                    # Add related pokemon details data
                    attacks_list.extend([{'external_id': item['id'], **a} for a in item.get('attacks', [])])
                    abilities_list.extend([{'external_id': item['id'], **a} for a in item.get('abilities', [])])
                    types_list.extend([{'external_id': item['id'], 'type': t} for t in item.get('types', [])])

                cards_processed += 1

            # --- Sync all data for set ---
            logging.info(f"Syncing {len(card_data_list)} cards for set {set_id}...")
            sync_all_data_bulk(conn, set_id, card_data_list, pokemon_details_list, subtypes_list, images_list, rules_list, attacks_list, abilities_list, types_list)

        logging.info(f"Processed {cards_processed} cards")
        logging.info(f"Successfully imported cards from file: {file_path}")
        
    except Exception as e:
        logging.error(f"Import failed: {str(e)}")
        logging.error(f"Error occurred while processing file: {file_path}")
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
        logging.exception(f"A critical error occurred during script execution: {e}")
        sys.exit(1)
