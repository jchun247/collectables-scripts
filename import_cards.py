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

def check_card_exists(conn, external_id, set_id):
    """Check if a card exists by external_id and set_id"""
    result = conn.execute(
        text("SELECT id FROM cards WHERE external_id = :external_id AND set_id = :set_id"),
        {"external_id": external_id, "set_id": set_id}
    )
    row = result.fetchone()
    return row[0] if row else None

def sync_card_attacks(conn, pokemon_details_id, attacks):
    """Synchronize card attacks and their costs"""
    current_attacks = conn.execute(
        text("SELECT id, name, damage, text FROM card_attacks WHERE card_pokemon_details_id = :pokemon_details_id"),
        {"pokemon_details_id": pokemon_details_id}
    ).fetchall()
    
    processed = set()
    attack_map = {a.name: a for a in current_attacks} if current_attacks else {}
    
    for attack in attacks:
        name = attack.get('name')
        existing = attack_map.get(name)
        
        if existing:
            conn.execute(text("UPDATE card_attacks SET damage = :damage, text = :text WHERE id = :id"), 
                       {'id': existing.id, 'damage': attack.get('damage'), 'text': attack.get('text')})
            attack_id = existing.id
            processed.add(attack_id)
        else:
            result = conn.execute(text("""
                INSERT INTO card_attacks (card_pokemon_details_id, name, damage, text) 
                VALUES (:pokemon_details_id, :name, :damage, :text) 
                RETURNING id"""),
                {'pokemon_details_id': pokemon_details_id, 'name': name, 'damage': attack.get('damage'), 'text': attack.get('text')})
            attack_id = result.fetchone()[0]
        
        # Handle attack costs
        costs = attack.get('cost', ['FREE'])
        current_costs = conn.execute(text("SELECT id, cost FROM card_attack_costs WHERE attack_id = :attack_id"), {"attack_id": attack_id}).fetchall()
        
        processed_costs = set()
        cost_map = {c.cost: c for c in current_costs} if current_costs else {}
        
        for cost in costs:
            if cost in cost_map:
                processed_costs.add(cost_map[cost].id)
            else:
                result = conn.execute(text("INSERT INTO card_attack_costs (attack_id, cost) VALUES (:attack_id, :cost) RETURNING id"),
                                    {'attack_id': attack_id, 'cost': cost})
                processed_costs.add(result.fetchone()[0])
        
        # Remove old costs
        for cost in current_costs or []:
            if cost.id not in processed_costs:
                conn.execute(text("DELETE FROM card_attack_costs WHERE id = :id"), {"id": cost.id})
    
    # Remove old attacks
    for attack in current_attacks or []:
        if attack.id not in processed:
            conn.execute(text("DELETE FROM card_attacks WHERE id = :id"), {"id": attack.id})

def sync_card_abilities(conn, pokemon_details_id, abilities):
    """Synchronize card abilities"""
    current = conn.execute(
        text("SELECT id, name, text, type FROM card_abilities WHERE card_pokemon_details_id = :pokemon_details_id"),
        {"pokemon_details_id": pokemon_details_id}
    ).fetchall()
    
    processed = set()
    ability_map = {a.name: a for a in current} if current else {}
    
    for ability in abilities:
        name = ability.get('name')
        existing = ability_map.get(name)
        
        if existing:
            conn.execute(text("UPDATE card_abilities SET text = :text, type = :type WHERE id = :id"), 
                       {'id': existing.id, 'text': ability.get('text'), 'type': ability.get('type')})
            processed.add(existing.id)
        else:
            result = conn.execute(text("""
                INSERT INTO card_abilities (card_pokemon_details_id, name, text, type) 
                VALUES (:pokemon_details_id, :name, :text, :type) 
                RETURNING id"""),
                {'pokemon_details_id': pokemon_details_id, 'name': name, 'text': ability.get('text'), 'type': ability.get('type')})
            processed.add(result.fetchone()[0])
    
    # Remove old abilities
    for ability in current or []:
        if ability.id not in processed:
            conn.execute(text("DELETE FROM card_abilities WHERE id = :id"), {"id": ability.id})

def sync_card_types(conn, pokemon_details_id, types):
    """Synchronize card types"""
    current = conn.execute(
        text("SELECT type FROM card_types WHERE card_pokemon_details_id = :pokemon_details_id"),
        {"pokemon_details_id": pokemon_details_id}
    ).fetchall()
    
    processed = set()
    type_map = {t.type: t for t in current} if current else {}
    
    for card_type in types:
        if card_type in type_map:
            # No need to update since type value is the key and won't change
            processed.add(card_type)
        else:
            conn.execute(text("""
                INSERT INTO card_types (card_pokemon_details_id, type) 
                VALUES (:pokemon_details_id, :type)"""),
                {'pokemon_details_id': pokemon_details_id, 'type': card_type})
            processed.add(card_type)
    
    # Remove old types
    for type_row in current or []:
        if type_row.type not in processed:
            conn.execute(text("DELETE FROM card_types WHERE card_pokemon_details_id = :pokemon_details_id AND type = :type"),
                       {"pokemon_details_id": pokemon_details_id, "type": type_row.type})

def sync_card_subtypes(conn, card_id, subtypes):
    """Synchronize card subtypes"""
    current = conn.execute(
        text("SELECT card_id, subtype FROM card_subtypes WHERE card_id = :card_id"),
        {"card_id": card_id}
    ).fetchall()
    
    processed = set()
    subtype_map = {s.subtype: s for s in current} if current else {}
    
    for subtype in subtypes:
        if subtype in subtype_map:
            # No need to update since subtype value is the key and won't change
            processed.add(subtype)
        else:
            conn.execute(text("INSERT INTO card_subtypes (card_id, subtype) VALUES (:card_id, :subtype)"),
                       {'card_id': card_id, 'subtype': subtype})
            processed.add(subtype)
    
    # Remove old subtypes
    for subtype_row in current or []:
        if subtype_row.subtype not in processed:
            conn.execute(text("DELETE FROM card_subtypes WHERE card_id = :card_id AND subtype = :subtype"),
                       {"card_id": card_id, "subtype": subtype_row.subtype})

def sync_card_images(conn, card_id, images):
    """Synchronize card images"""
    current = conn.execute(
        text("SELECT resolution, url FROM card_images WHERE card_id = :card_id"),
        {"card_id": card_id}
    ).fetchall()
    
    image_map = {i.resolution: i.url for i in current}
    
    for resolution, url in images.items():
        if resolution not in image_map:
            conn.execute(text("INSERT INTO card_images (card_id, resolution, url) VALUES (:card_id, :resolution, :url)"),
                        {'card_id': card_id, 'resolution': resolution, 'url': url})
        elif image_map[resolution] != url:
            conn.execute(text("UPDATE card_images SET url = :url WHERE card_id = :card_id AND resolution = :resolution"),
                        {'card_id': card_id, 'resolution': resolution, 'url': url})

def sync_card_rules(conn, card_id, rules):
    """Synchronize card rules"""
    current = conn.execute(
        text("SELECT id, text FROM card_rules WHERE card_id = :card_id"),
        {"card_id": card_id}
    ).fetchall()
    
    processed = set()
    rule_map = {r.text: r for r in current} if current else {}
    
    for rule in rules:
        if rule in rule_map:
            processed.add(rule_map[rule].id)
        else:
            result = conn.execute(text("INSERT INTO card_rules (card_id, text) VALUES (:card_id, :text) RETURNING id"),
                                {'card_id': card_id, 'text': rule})
            processed.add(result.fetchone()[0])
    
    # Remove old rules
    for rule_row in current or []:
        if rule_row.id not in processed:
            conn.execute(text("DELETE FROM card_rules WHERE id = :id"), {"id": rule_row.id})

def update_pokemon_details(conn, card_id, pokemon_data):
    """Update Pokemon-specific details for a card"""
    # Check if Pokemon details exist
    result = conn.execute(
        text("SELECT id FROM card_pokemon_details WHERE card_id = :card_id"),
        {"card_id": card_id}
    )
    pokemon_details_id = result.fetchone()
    
    if pokemon_details_id:
        # Update existing Pokemon details
        conn.execute(
            text("""
                UPDATE card_pokemon_details
                SET hit_points = :hit_points,
                    retreat_cost = :retreat_cost,
                    flavour_text = :flavour_text,
                    weakness_type = :weakness_type,
                    weakness_modifier = :weakness_modifier,
                    weakness_value = :weakness_value,
                    resistance_type = :resistance_type,
                    resistance_modifier = :resistance_modifier,
                    resistance_value = :resistance_value
                WHERE card_id = :card_id
                RETURNING id
            """),
            {"card_id": card_id, **pokemon_data}
        )
    else:
        # Insert new Pokemon details
        pokemon_details_id = conn.execute(
            text("""
                INSERT INTO card_pokemon_details (
                    card_id, hit_points, retreat_cost, flavour_text,
                    weakness_type, weakness_modifier, weakness_value,
                    resistance_type, resistance_modifier, resistance_value
                ) VALUES (
                    :card_id, :hit_points, :retreat_cost, :flavour_text,
                    :weakness_type, :weakness_modifier, :weakness_value,
                    :resistance_type, :resistance_modifier, :resistance_value
                )
                RETURNING id
            """),
            {"card_id": card_id, **pokemon_data}
        ).fetchone()

    return pokemon_details_id[0] if pokemon_details_id else None

def update_card(conn, card_id, card_data, pokemon_data=None):
    """Update an existing card"""
    # Update base card data
    query = """
        UPDATE cards
        SET name = :name,
            game = 'POKEMON',
            set_number = :set_number,
            rarity = :rarity,
            illustrator_name = :illustrator_name,
            supertype = :supertype
        WHERE id = :id
        RETURNING id, set_number
    """
    
    card_data['id'] = card_id
    result = conn.execute(text(query), card_data)
    row = result.fetchone()
    
    # Update Pokemon details if applicable
    if pokemon_data:
        update_pokemon_details(conn, card_id, pokemon_data)
    
    return row

def insert_card(conn, card_data, pokemon_data=None):
    """Insert a single card or update if it already exists"""
    # Check if card exists
    card_id = check_card_exists(conn, card_data['external_id'], card_data['set_id'])
    
    if card_id:
        # Update the card
        row = update_card(conn, card_id, card_data, pokemon_data)
    else:
        # Insert base card data
        query = """
            INSERT INTO cards (
                name, game, external_id, set_id, set_number, rarity, 
                illustrator_name, supertype
            )
            VALUES (
                :name, 'POKEMON', :external_id, :set_id, :set_number, :rarity, 
                :illustrator_name, :supertype
            )
            RETURNING id, set_number
        """
        
        result = conn.execute(text(query), card_data)
        row = result.fetchone()
    
    if row:
        # Try different ways to access the ID
        try:
            card_id = row._mapping['id']  # SQLAlchemy result row
        except (KeyError, AttributeError):
            try:
                card_id = row['id']  # Dict-like access
            except (KeyError, TypeError):
                card_id = row[0]  # Tuple-like access
        # Create Pokemon card
        if pokemon_data:
            update_pokemon_details(conn, card_id, pokemon_data)
    
    return row

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
            
        # Add debug logging for first item
        if data and len(data) > 0:
            logging.info(f"First card data: id={data[0].get('id')}, name={data[0].get('name')}")
        
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
                # Create standardized set number
                set_number = create_card_set_number(item['number'], total_cards, is_modern_set)
                
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

                # Handle Pokemon-specific data if applicable
                pokemon_data = None
                supertype = item.get('supertype', '')
                if supertype == 'Pokémon':
                    # Extract weakness data
                    weakness = item.get('weaknesses', [{}])[0] if item.get('weaknesses') else {}
                    raw_weakness_value = weakness.get('value', '')
                    weakness_modifier = raw_weakness_value[0] if raw_weakness_value else None
                    weakness_value = int(raw_weakness_value[1:]) if raw_weakness_value else None
                    
                    # Extract resistance data
                    resistance = item.get('resistances', [{}])[0] if item.get('resistances') else {}
                    raw_resistance_value = resistance.get('value', '')
                    resistance_modifier = raw_resistance_value[0] if raw_resistance_value else None
                    resistance_value = int(raw_resistance_value[1:]) if raw_resistance_value else None
                    
                    # Convert HP to integer
                    hp = int(item.get('hp', 0)) if item.get('hp', '').isdigit() else None

                    pokemon_data = {
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

                # Insert/update card
                try:
                    row = insert_card(conn, card_data, pokemon_data)
                    logging.info(f"Successfully processed card: {card_data['name']} ({card_data['external_id']})")
                    if row:
                        card_id = row[0]
                        
                        # Sync data common to all cards
                        sync_card_subtypes(conn, card_id, item.get('subtypes', []))
                        sync_card_images(conn, card_id, item.get('images', {}))
                        
                        # Sync Pokemon-specific data only for Pokemon cards
                        if supertype == 'Pokémon':
                            pokemon_details_id = update_pokemon_details(conn, card_id, pokemon_data)
                            if pokemon_details_id:
                                sync_card_attacks(conn, pokemon_details_id, item.get('attacks', []))
                                sync_card_abilities(conn, pokemon_details_id, item.get('abilities', []))
                                sync_card_types(conn, pokemon_details_id, item.get('types', []))
                        # Otherwise, sync trainer and energy rules
                        else:
                            sync_card_rules(conn, card_id, item.get('rules', []))
                        
                        cards_processed += 1
                except Exception as e:
                    logging.error(f"Failed to process card: {card_data['name']} ({card_data['external_id']})")
                    logging.error(f"Error: {str(e)}")
                    raise
                
            logging.info(f"Processed {cards_processed} cards")

        logging.info("Import completed successfully")
        
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
        logging.error(f"Script execution failed: {e}")
        sys.exit(1)
