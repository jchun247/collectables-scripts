import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()
DB_URI = os.getenv('DATABASE_URL')
if not DB_URI:
    raise ValueError("DATABASE_URL environment variable is not set")

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
