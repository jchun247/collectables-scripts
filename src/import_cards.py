import json
import logging
from sqlalchemy import text, inspect, Table, MetaData, Column, String, BigInteger, Integer
from sqlalchemy.dialects.postgresql import insert as pg_insert
import os
import re
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
        raise ValueError(f"Set id '{set_id}' not found in database")
    return result

def create_combined_set_number(set_id, set_number, total_cards, is_modern_set):
    """Create a standardized card set number in format XXX/YYY or PREFIX00/PREFIX00"""
    if not set_number or not set_number.strip():
        return None
    
    if set_id.endswith('p'):
        series = get_series_from_set_id(set_id)
        rule = PROMO_SET_RULES.get(set_id, PROMO_SET_RULES["default"])
        match = re.search(r'(\d+)', set_number)
        if not match:
            # If no digits are found, return the original number.
            return set_number
        promo_number_digits = match.group(1)
        return rule(series, promo_number_digits)

    if set_id.endswith('tg') or set_id.endswith('gg'):
        return format_gallery_card_number(set_id, set_number, total_cards)

    parts = set_number.split('/')
    if not parts:
        return None
    original_number = parts[0]
    match = re.search(r'(\d+)', original_number)
    if not match:
        return None
    start, end = match.start(), match.end()
    prefix, number_str, suffix = original_number[:start], match.group(1), original_number[end:]
    number = int(number_str)
    
    formatted_number = f"{number:03d}" if is_modern_set else str(number)
    
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
    Handles the bulk "upsert" for the main cards table using SQLAlchemy Core.
    Returns a dictionary mapping {external_id: database_id}.
    """
    if not card_data_list:
        return {}

    metadata = MetaData()
    cards_table = Table('cards', metadata,
        Column('id', BigInteger, primary_key=True),
        Column('external_id', String),
        Column('set_id', String),
        Column('name', String),
        Column('set_number', String),
        Column('combined_set_number', String),
        Column('rarity', String),
        Column('illustrator_name', String),
        Column('supertype', String),
        Column('game', String)
    )

    # Prepare the insert statement
    insert_stmt = pg_insert(cards_table).values(card_data_list)

    # Define the update part of the ON CONFLICT clause
    update_dict = {
        'name': insert_stmt.excluded.name,
        'set_number': insert_stmt.excluded.set_number,
        'combined_set_number': insert_stmt.excluded.combined_set_number,
        'rarity': insert_stmt.excluded.rarity,
        'illustrator_name': insert_stmt.excluded.illustrator_name,
        'supertype': insert_stmt.excluded.supertype,
    }
    
    # Create the full ON CONFLICT ... DO UPDATE statement
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['external_id', 'set_id'],
        set_=update_dict
    ).returning(cards_table.c.id, cards_table.c.external_id)

    # Execute and fetch results
    result = conn.execute(upsert_stmt).fetchall()
    return {row.external_id: row.id for row in result}


def _sync_pokemon_details_bulk(conn, pokemon_details_list, external_id_to_db_id):
    """
    Handles the bulk "upsert" for the card_pokemon_details table using SQLAlchemy Core.
    Returns a dictionary mapping {database_card_id: pokemon_details_id}.
    """
    if not pokemon_details_list:
        return {}
        
    for detail in pokemon_details_list:
        detail['card_id'] = external_id_to_db_id.get(detail.pop('external_id'))
        if not detail['card_id']:
            logging.warning(f"Skipping Pokémon detail for missing card external_id.")

    valid_details = [d for d in pokemon_details_list if 'card_id' in d and d['card_id'] is not None]
    if not valid_details:
        return {}

    metadata = MetaData()
    details_table = Table('card_pokemon_details', metadata, autoload_with=conn.engine)

    insert_stmt = pg_insert(details_table).values(valid_details)

    # Dynamically create the update dictionary for all columns except the primary key
    update_dict = {
        c.name: c
        for c in insert_stmt.excluded
        if not c.primary_key
    }

    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['card_id'],
        set_=update_dict
    ).returning(details_table.c.id, details_table.c.card_id)
    
    result = conn.execute(upsert_stmt).fetchall()
    return {row.card_id: row.id for row in result}

def _sync_attacks_and_costs_bulk(conn, attacks_list, details_id_map, external_id_to_db_id):
    """
    Handles the complex bulk insert for attacks and their associated costs.
    """
    if not attacks_list:
        return

    # Map the pokemon_details_id to each attack
    for attack in attacks_list:
        card_id = external_id_to_db_id.get(attack.pop('external_id'))
        attack['card_pokemon_details_id'] = details_id_map.get(card_id)

    valid_attacks = [a for a in attacks_list if a.get('card_pokemon_details_id') is not None]
    if not valid_attacks:
        return
    
    metadata = MetaData()
    attacks_table = Table('card_attacks', metadata, autoload_with=conn.engine)
    costs_table = Table('card_attack_costs', metadata, autoload_with=conn.engine)

    # We need the 'cost' data from the original list, so we can't just insert valid_attacks.
    # We must prepare the attack data separately.
    attacks_to_insert = [
        {
            'card_pokemon_details_id': a['card_pokemon_details_id'],
            'name': a['name'],
            'damage': a.get('damage'),
            'text': a.get('text')
        } for a in valid_attacks
    ]

    # 1. Bulk insert all attacks and get their new database IDs
    attack_insert_stmt = attacks_table.insert().values(attacks_to_insert).returning(
        attacks_table.c.id,
        attacks_table.c.card_pokemon_details_id,
        attacks_table.c.name
    )
    
    inserted_attacks = conn.execute(attack_insert_stmt).fetchall()

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
        conn.execute(costs_table.insert(), costs_to_insert)

def sync_all_data_bulk(conn, set_id, card_data_list, pokemon_details_list, subtypes_list, images_list, rules_list, attacks_list, abilities_list, types_list):
    """
    Orchestrates the entire bulk sync process for a set using the "Delete-Then-Insert" pattern.
    """
    logging.info("Syncing main card data...")
    external_id_to_db_id = _sync_cards_table_bulk(conn, card_data_list)
    
    if not external_id_to_db_id:
        logging.warning(f"No cards were synced for set {set_id}. Aborting.")
        return

    all_card_ids = list(external_id_to_db_id.values())

    logging.info("Syncing Pokémon details...")
    details_id_map = _sync_pokemon_details_bulk(conn, pokemon_details_list, external_id_to_db_id)
    all_details_ids = list(details_id_map.values())

    logging.info("Deleting old child data...")
    if all_details_ids:
        # Get all attack IDs associated with the pokemon details being updated
        attack_ids_result = conn.execute(
            text("SELECT id FROM card_attacks WHERE card_pokemon_details_id = ANY(:details_ids)"),
            {"details_ids": all_details_ids}
        ).fetchall()

        attack_ids = [row.id for row in attack_ids_result]

        if attack_ids:
            # Delete from the child table (card_attack_costs) FIRST
            conn.execute(text("DELETE FROM card_attack_costs WHERE attack_id = ANY(:attack_ids)"), {"attack_ids": attack_ids})
        
        # Then proceed with deleting other table data
        conn.execute(text("DELETE FROM card_attacks WHERE card_pokemon_details_id = ANY(:details_ids)"), {"details_ids": all_details_ids})
        conn.execute(text("DELETE FROM card_abilities WHERE card_pokemon_details_id = ANY(:details_ids)"), {"details_ids": all_details_ids})
        conn.execute(text("DELETE FROM card_types WHERE card_pokemon_details_id = ANY(:details_ids)"), {"details_ids": all_details_ids})

    conn.execute(text("DELETE FROM card_subtypes WHERE card_id = ANY(:card_ids)"), {"card_ids": all_card_ids})
    conn.execute(text("DELETE FROM card_images WHERE card_id = ANY(:card_ids)"), {"card_ids": all_card_ids})
    conn.execute(text("DELETE FROM card_rules WHERE card_id = ANY(:card_ids)"), {"card_ids": all_card_ids})
    
    logging.info("Inserting new child data...")

    for s in subtypes_list: s['card_id'] = external_id_to_db_id.get(s.pop('external_id'))
    if subtypes_list: conn.execute(text("INSERT INTO card_subtypes (card_id, subtype) VALUES (:card_id, :subtype)"), [s for s in subtypes_list if s['card_id']])
    
    for i in images_list: i['card_id'] = external_id_to_db_id.get(i.pop('external_id'))
    if images_list: conn.execute(text("INSERT INTO card_images (card_id, resolution, url) VALUES (:card_id, :resolution, :url)"), [i for i in images_list if i['card_id']])

    for r in rules_list: r['card_id'] = external_id_to_db_id.get(r.pop('external_id'))
    if rules_list: conn.execute(text("INSERT INTO card_rules (card_id, text) VALUES (:card_id, :text)"), [r for r in rules_list if r['card_id']])
    
    for a in abilities_list: 
        card_id = external_id_to_db_id.get(a.pop('external_id'))
        a['card_pokemon_details_id'] = details_id_map.get(card_id)
    if abilities_list: conn.execute(text("INSERT INTO card_abilities (card_pokemon_details_id, name, text, type) VALUES (:card_pokemon_details_id, :name, :text, :type)"), [a for a in abilities_list if a.get('card_pokemon_details_id')])

    for t in types_list:
        card_id = external_id_to_db_id.get(t.pop('external_id'))
        t['card_pokemon_details_id'] = details_id_map.get(card_id)
    if types_list: conn.execute(text("INSERT INTO card_types (card_pokemon_details_id, type) VALUES (:card_pokemon_details_id, :type)"), [t for t in types_list if t.get('card_pokemon_details_id')])

    _sync_attacks_and_costs_bulk(conn, attacks_list, details_id_map, external_id_to_db_id)

    logging.info("Bulk sync complete.")

def import_cards(file_path):
    try:
        engine = connect_to_db()
        
        if not os.path.isfile(file_path):
            raise ValueError(f"File not found: {file_path}")
            
        logging.info(f"Reading data from {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        card_data_list, pokemon_details_list, subtypes_list, images_list, rules_list, attacks_list, abilities_list, types_list = [], [], [], [], [], [], [], []
        
        logging.info(f"Processing {len(data)} cards")
        
        with engine.begin() as conn:
            set_id = os.path.splitext(os.path.basename(file_path))[0]
            set_details = get_set_details(set_id, conn)

            logging.info(f"Importing cards for set ID: {set_id}")
            
            for item in data:
                combined_set_number = create_combined_set_number(set_id, item.get('number'), set_details.printed_total, set_details.is_modern)
                
                card_data_list.append({
                    'name': item['name'], 'external_id': item['id'], 'set_id': set_id,
                    'set_number': item.get('number'), 'combined_set_number': combined_set_number,
                    'rarity': item.get('rarity'), 'illustrator_name': item.get('artist'),
                    'supertype': item.get('supertype'), 'game': 'POKEMON'
                })
                subtypes_list.extend([{'external_id': item['id'], 'subtype': s} for s in item.get('subtypes', [])])
                images_list.extend([{'external_id': item['id'], 'resolution': r, 'url': u} for r, u in item.get('images', {}).items()])
                rules_list.extend([{'external_id': item['id'], 'text': r} for r in item.get('rules', [])])

                if item.get('supertype') == 'Pokémon':
                    weakness = item.get('weaknesses', [{}])[0]
                    resistance = item.get('resistances', [{}])[0]
                    
                    pokemon_details_list.append({
                        'external_id': item['id'], 'flavour_text': item.get('flavorText'),
                        'hit_points': int(item.get('hp', 0)) if item.get('hp', '').isdigit() else None,
                        'retreat_cost': item.get('convertedRetreatCost', 0),
                        'weakness_type': weakness.get('type'),
                        'weakness_modifier': weakness.get('value', '')[:1] or None,
                        'weakness_value': int(weakness.get('value', '')[1:]) if len(weakness.get('value', '')) > 1 else None,
                        'resistance_type': resistance.get('type'),
                        'resistance_modifier': resistance.get('value', '')[:1] or None,
                        'resistance_value': int(resistance.get('value', '')[1:]) if len(resistance.get('value', '')) > 1 else None,
                    })
                    attacks_list.extend([{'external_id': item['id'], **a} for a in item.get('attacks', [])])
                    abilities_list.extend([{'external_id': item['id'], **a} for a in item.get('abilities', [])])
                    types_list.extend([{'external_id': item['id'], 'type': t} for t in item.get('types', [])])

            logging.info(f"Syncing {len(card_data_list)} cards for set {set_id}...")
            sync_all_data_bulk(conn, set_id, card_data_list, pokemon_details_list, subtypes_list, images_list, rules_list, attacks_list, abilities_list, types_list)

        logging.info(f"Successfully processed {len(data)} cards from file: {file_path}")
        
    except Exception as e:
        logging.error(f"Import failed: {str(e)}")
        logging.error(f"Error occurred while processing file: {file_path}")
        raise

if __name__ == '__main__':
    try:
        import sys
        path_arg = sys.argv[1] if len(sys.argv) > 1 else './data/cards'
        
        files_to_process = get_json_files(path_arg) if os.path.isdir(path_arg) else [path_arg]
        
        if not files_to_process:
            logging.error(f"No JSON files found in {path_arg}")
            sys.exit(1)
        
        for json_file in files_to_process:
            logging.info(f"Processing file: {json_file}")
            import_cards(json_file)
            
    except Exception as e:
        logging.exception(f"A critical error occurred during script execution: {e}")
        sys.exit(1)