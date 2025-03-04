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
#DB_URI = os.getenv('DATABASE_URL', 'postgresql://postgres:week7day@localhost:5432/collectables')
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

def map_to_card_series_enum(series_string):
    # Create a mapping directory
    mapping = {
        'base': 'BASE',
        'gym': 'GYM',
        'neo': 'NEO',
        'e-card': 'E_CARD',
        'ex': 'EX',
        'pop': 'POP',
        'diamond & pearl': 'DIAMOND_AND_PEARL',
        'platinum': 'PLATINUM',
        'heartgold & soulsilver': 'HEARTGOLD_AND_SOULSILVER',
        'black & white': 'BLACK_AND_WHITE',
        'xy': 'XY',
        'sun & moon': 'SUN_AND_MOON',
        'sword & shield': 'SWORD_AND_SHIELD',
        'scarlet & violet': 'SCARLET_AND_VIOLET',
        'np': 'NP',
        'other': 'OTHER'
    }

    normalized = series_string.lower()

    return mapping.get(normalized, 'OTHER')

def insert_sets(conn, sets_data):
    """Insert sets one at a time"""
    if not sets_data:
        return
    
    query = """
        INSERT INTO sets (id, code, name, game, series, release_date, last_updated, printed_total, total)
        VALUES (:id, :code, :name, :game, :series, :release_date, :last_updated, :printed_total, :total)
        ON CONFLICT (id) DO UPDATE 
        SET code = EXCLUDED.code,
            name = EXCLUDED.name,
            game = EXCLUDED.game,
            series = EXCLUDED.series,
            release_date = EXCLUDED.release_date,
            last_updated = EXCLUDED.last_updated,
            printed_total = EXCLUDED.printed_total,
            total = EXCLUDED.total
    """
    
    for item in sets_data:
        conn.execute(text(query), item)

def insert_legalities(conn, legalities_data):
    """Insert set legalities"""
    if not legalities_data:
        return
    
    # First delete all existing legalities as we'll insert fresh data
    conn.execute(text("DELETE FROM set_legalities"))
    
    query = """
        INSERT INTO set_legalities (set_id, format, legality)
        VALUES (:set_id, :format, :legality)
    """
    
    for item in legalities_data:
        conn.execute(text(query), item)

def insert_images(conn, images_data):
    """Insert set images"""
    if not images_data:
        return
    
    # First delete all existing images as we'll insert fresh data
    conn.execute(text("DELETE FROM set_images"))
    
    query = """
        INSERT INTO set_images (set_id, image_type, url)
        VALUES (:set_id, :image_type, :url)
    """
    
    for item in images_data:
        conn.execute(text(query), item)

def import_card_sets(file_path='./data/ptcg_sets.json'):
    try:
        # Connect to database
        engine = connect_to_db()
        
        # Read JSON file
        logging.info(f"Reading data from {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)['data']
        
        logging.info(f"Processing {len(data)} card sets")
        
        # Process sets data
        sets_data = []
        
        for item in data:
            # Use ptcgoCode if available, otherwise fallback to id
            set_code = item.get('ptcgoCode', item['id'])
            if not item.get('ptcgoCode'):
                logging.warning(f"No ptcgoCode found for set {item['name']}, using id '{set_code}' instead")
                
            set_data = {
                'id': item['id'],
                'code': set_code,
                'name': item['name'],
                'game': 'POKEMON',
                'series': map_to_card_series_enum(item['series']),
                'release_date': datetime.strptime(item['releaseDate'], '%Y/%m/%d').date(),
                'last_updated': datetime.strptime(item['updatedAt'], '%Y/%m/%d %H:%M:%S'),
                'printed_total': item['printedTotal'],
                'total': item['total']
            }
            sets_data.append(set_data)
        
        with engine.begin() as conn:
            # Insert sets
            logging.info(f"Inserting {len(sets_data)} sets")
            insert_sets(conn, sets_data)
            
            # Process legalities data
            legalities_data = []
            for item in data:
                for format_name, legality in item.get('legalities', {}).items():
                    legalities_data.append({
                        'set_id': item['id'],
                        'format': format_name.upper(),
                        'legality': legality
                    })
            
            if legalities_data:
                logging.info(f"Inserting legality records")
                insert_legalities(conn, legalities_data)
            
            # Process images data
            images_data = []
            for item in data:
                for image_type, url in item.get('images', {}).items():
                    images_data.append({
                        'set_id': item['id'],
                        'image_type': image_type,
                        'url': url
                    })
            
            if images_data:
                logging.info(f"Inserting image records")
                insert_images(conn, images_data)
                
        logging.info("Import completed successfully")
        
    except Exception as e:
        logging.error(f"Import failed: {e}")
        raise

if __name__ == '__main__':
    try:
        import_card_sets()
    except Exception as e:
        logging.error(f"Script execution failed: {e}")
