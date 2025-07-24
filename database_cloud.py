import os
import logging
from typing import Optional, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from models import Base

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables for connection management
_engine: Optional[Engine] = None
_SessionLocal: Optional[sessionmaker] = None

def get_cloud_sql_engine() -> Engine:
    """Create Cloud SQL engine with proper connector for Streamlit Cloud"""
    global _engine
    
    if _engine is not None:
        return _engine
    
    try:
        # Try Cloud SQL Connector first (preferred for Google Cloud)
        from google.cloud.sql.connector import Connector
        import pg8000
        
        # Cloud SQL configuration
        instance_connection_name = os.getenv('INSTANCE_CONNECTION_NAME')  # project:region:instance
        db_user = os.getenv('DB_USER', 'postgres')
        db_pass = os.getenv('DB_PASS', '')
        db_name = os.getenv('DB_NAME', 'postgres')
        
        if instance_connection_name:
            logger.info(f"Using Cloud SQL Connector for: {instance_connection_name}")
            
            connector = Connector()
            
            def getconn():
                conn = connector.connect(
                    instance_connection_name,
                    "pg8000",
                    user=db_user,
                    password=db_pass,
                    db=db_name,
                )
                return conn
            
            # Create engine with Cloud SQL connector
            _engine = create_engine(
                "postgresql+pg8000://",
                creator=getconn,
                pool_size=5,
                max_overflow=2,
                pool_timeout=30,
                pool_recycle=1800,
                echo=False
            )
            
            logger.info("Cloud SQL Connector engine created successfully")
            return _engine
    
    except ImportError as e:
        logger.warning(f"Cloud SQL Connector not available: {e}")
    except Exception as e:
        logger.error(f"Failed to create Cloud SQL Connector engine: {e}")
    
    # Fallback to standard PostgreSQL connection
    return get_standard_engine()

def get_standard_engine() -> Engine:
    """Create standard PostgreSQL engine"""
    global _engine
    
    if _engine is not None:
        return _engine
    
    # Get database URL from environment
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        # Build from individual components
        pguser = os.getenv('PGUSER', 'postgres')
        pgpassword = os.getenv('PGPASSWORD', '')
        pghost = os.getenv('PGHOST', 'localhost')
        pgport = os.getenv('PGPORT', '5432')
        pgdatabase = os.getenv('PGDATABASE', 'skating')
        
        database_url = f"postgresql://{pguser}:{pgpassword}@{pghost}:{pgport}/{pgdatabase}"
    
    # Add SSL mode for cloud connections
    if 'localhost' not in database_url and '127.0.0.1' not in database_url:
        if '?sslmode=' not in database_url:
            if '?' in database_url:
                database_url += '&sslmode=require'
            else:
                database_url += '?sslmode=require'
    
    logger.info(f"Creating standard PostgreSQL engine")
    
    _engine = create_engine(
        database_url,
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800,
        echo=False
    )
    
    return _engine

def get_engine() -> Engine:
    """Get database engine with cloud-first approach"""
    return get_cloud_sql_engine()

def get_session_factory() -> sessionmaker:
    """Get session factory"""
    global _SessionLocal
    
    if _SessionLocal is None:
        engine = get_engine()
        _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    return _SessionLocal

def get_db_session():
    """Create and return a database session"""
    SessionLocal = get_session_factory()
    return SessionLocal()

def test_connection() -> Tuple[bool, Optional[str]]:
    """Test database connection with detailed error reporting"""
    try:
        engine = get_engine()
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            row = result.fetchone()
            
            if row and row[0] == 1:
                logger.info("Database connection test successful")
                return True, None
            else:
                error_msg = "Database connection test failed: unexpected result"
                logger.error(error_msg)
                return False, error_msg
                
    except Exception as e:
        error_msg = f"Database connection failed: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def close_connections():
    """Clean up database connections"""
    global _engine, _SessionLocal
    
    if _engine:
        _engine.dispose()
        _engine = None
    
    _SessionLocal = None
    logger.info("Database connections closed")

# Create tables on import (for first-time setup)
def initialize_database():
    """Initialize database tables"""
    try:
        engine = get_engine()
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return False

# Backwards compatibility
def create_engine_legacy():
    """Legacy function for backwards compatibility"""
    return get_engine()