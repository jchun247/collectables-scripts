import logging
import os
from sqlalchemy import create_engine, text, Engine
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Get database connection parameters from system environment variables
DB_USER = os.getenv('APP_LIQUIBASE_USER')
DB_PASSWORD = os.getenv('APP_LIQUIBASE_PASSWORD')
DB_URL = os.getenv('DB_URL')

# Validate all required environment variables are set
if not all([DB_USER, DB_PASSWORD, DB_URL]):
    raise ValueError("DB_USER, DB_PASSWORD, and DB_URL environment variables must be set")

# URL-encode the password to handle special characters like '@'
ENCODED_DB_PASSWORD = quote_plus(DB_PASSWORD)

# Construct database URI
DB_URI = f'postgresql://{DB_USER}:{ENCODED_DB_PASSWORD}@{DB_URL}/collectables?options=-c%20search_path=collectables'

_engine: Engine | None = None

def get_engine() -> Engine:
    global _engine
    
    # Check if the engine has already been created
    if _engine is None:
        try:
            # Create the engine instance
            _engine = create_engine(DB_URI)
            
            # Test the connection the first time the engine is created
            with _engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info("Database engine created and connection successful.")
            
        except Exception as e:
            logging.error(f"Database connection failed during initial setup: {e}")
            # Set engine back to None on failure to allow for a retry if desired
            _engine = None 
            raise
    
    return _engine

# For backwards compatibility, you can alias your old function name.
connect_to_db = get_engine
