import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import Base

# Get database connection details from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    # Fallback to individual components if DATABASE_URL is not available
    PGUSER = os.getenv('PGUSER', 'postgres')
    PGPASSWORD = os.getenv('PGPASSWORD', '')
    PGHOST = os.getenv('PGHOST', 'localhost')
    PGPORT = os.getenv('PGPORT', '5432')
    PGDATABASE = os.getenv('PGDATABASE', 'postgres')
    
    DATABASE_URL = f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"

# Create engine and session factory
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db_session():
    """Create and return a database session"""
    return SessionLocal()

def test_connection():
    """Test database connection"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            return True
    except Exception as e:
        return False, str(e)
