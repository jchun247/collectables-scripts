import logging
import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

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
