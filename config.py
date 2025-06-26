import os
from pathlib import Path
from typing import Optional

class Config:
    """
    Centralized configuration for the functional profile database project.
    Environment variables take priority over default values.
    """
    
    DATABASE_PATH: str = os.getenv('DATABASE_PATH', 'database.db')
    DATABASE_MEMORY_LIMIT: str = os.getenv('DATABASE_MEMORY_LIMIT', '4GB')
    DATABASE_THREADS: int = int(os.getenv('DATABASE_THREADS', '4'))
    DATABASE_TEMP_DIR: str = os.getenv('DATABASE_TEMP_DIR', '/tmp')
    
    DATA_DIR: str = os.getenv('DATA_DIR', './data')
    BATCH_SIZE: int = int(os.getenv('BATCH_SIZE', '10000'))
    MAX_WORKERS: int = int(os.getenv('MAX_WORKERS', '8'))
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    LOG_DIR: str = os.getenv('LOG_DIR', './logs')
    LOG_FILE_MAX_SIZE: int = int(os.getenv('LOG_FILE_MAX_SIZE', '10485760'))
    LOG_BACKUP_COUNT: int = int(os.getenv('LOG_BACKUP_COUNT', '5'))
    CONSOLE_LOG_LEVEL: str = os.getenv('CONSOLE_LOG_LEVEL', 'ERROR')
    
    # Processing Configuration
    SIGNATURE_PROCESSING_ENABLED: bool = os.getenv('SIGNATURE_PROCESSING_ENABLED', 'true').lower() == 'true'
    TAXA_PROCESSING_ENABLED: bool = os.getenv('TAXA_PROCESSING_ENABLED', 'true').lower() == 'true'
    GATHER_PROCESSING_ENABLED: bool = os.getenv('GATHER_PROCESSING_ENABLED', 'true').lower() == 'true'
    
    # Performance Configuration
    MEMORY_LIMIT_WARNING_THRESHOLD: float = float(os.getenv('MEMORY_LIMIT_WARNING_THRESHOLD', '0.8'))
    PROGRESS_BAR_ENABLED: bool = os.getenv('PROGRESS_BAR_ENABLED', 'true').lower() == 'true'
    
    # Query Configuration
    MAX_QUERY_RESULTS: int = int(os.getenv('MAX_QUERY_RESULTS', '1000'))
    QUERY_TIMEOUT: int = int(os.getenv('QUERY_TIMEOUT', '60'))
    DISPLAY_ROWS_LIMIT: int = int(os.getenv('DISPLAY_ROWS_LIMIT', '20'))
    
    # Schema Configuration
    DEFAULT_SAMPLE_ID: str = os.getenv('DEFAULT_SAMPLE_ID', 'DRR012227')
    SAMPLE_ID_PATTERN: str = os.getenv('SAMPLE_ID_PATTERN', r'DRR\d{6}')
    
    # File Processing Configuration
    ARCHIVE_EXTENSIONS: list = os.getenv('ARCHIVE_EXTENSIONS', '.tar.gz,.zip').split(',')
    SKIP_EMPTY_FILES: bool = os.getenv('SKIP_EMPTY_FILES', 'true').lower() == 'true'
    CLEANUP_TEMP_FILES: bool = os.getenv('CLEANUP_TEMP_FILES', 'true').lower() == 'true'
    
    # Error Handling Configuration
    CONTINUE_ON_ERROR: bool = os.getenv('CONTINUE_ON_ERROR', 'true').lower() == 'true'
    MAX_RETRY_ATTEMPTS: int = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))
    RETRY_DELAY: float = float(os.getenv('RETRY_DELAY', '1.0'))
    
    @classmethod
    def get_database_path(cls) -> str:
        """Get absolute path to database file"""
        return os.path.abspath(cls.DATABASE_PATH)
    
    @classmethod
    def get_data_dir(cls) -> str:
        """Get absolute path to data directory"""
        path = Path(cls.DATA_DIR)
        path.mkdir(exist_ok=True)
        return str(path.absolute())
    
    @classmethod
    def get_log_dir(cls) -> str:
        """Get absolute path to log directory"""
        path = Path(cls.LOG_DIR)
        path.mkdir(exist_ok=True)
        return str(path.absolute())
    
    
   
    
    @classmethod
    def load_from_file(cls, config_file: str) -> bool:
        """Load configuration from file"""
        try:
            import configparser
            config = configparser.ConfigParser()
            config.read(config_file)
            return True
        except Exception:
            return False

# Example .env file content
ENV_FILE_TEMPLATE = """
# Database Configuration
DATABASE_PATH=database.db
DATABASE_MEMORY_LIMIT=4GB
DATABASE_THREADS=4
DATABASE_TEMP_DIR=/tmp

# Data Processing Configuration
DATA_DIR=./data
BATCH_SIZE=10000
MAX_WORKERS=4

# Logging Configuration
LOG_LEVEL=INFO
LOG_DIR=./logs
LOG_FILE_MAX_SIZE=10485760
LOG_BACKUP_COUNT=5
CONSOLE_LOG_LEVEL=ERROR

# Processing Configuration
SIGNATURE_PROCESSING_ENABLED=true
TAXA_PROCESSING_ENABLED=true
GATHER_PROCESSING_ENABLED=true

# Performance Configuration
MEMORY_LIMIT_WARNING_THRESHOLD=0.8
PROGRESS_BAR_ENABLED=true

# Query Configuration
MAX_QUERY_RESULTS=1000
QUERY_TIMEOUT=60
DISPLAY_ROWS_LIMIT=20

# Schema Configuration
DEFAULT_SAMPLE_ID=DRR012227
SAMPLE_ID_PATTERN=DRR\\d{6}

# File Processing Configuration
ARCHIVE_EXTENSIONS=.tar.gz,.zip
SKIP_EMPTY_FILES=true
CLEANUP_TEMP_FILES=true

# Error Handling Configuration
CONTINUE_ON_ERROR=true
MAX_RETRY_ATTEMPTS=3
RETRY_DELAY=1.0
"""

def create_env_file(path: str = '.env') -> bool:
    """
    Create a sample .env file with all configuration options
    
    Args:
        path: Path where to create the .env file
        
    Returns:
        bool: True if file was created successfully
    """
    try:
        with open(path, 'w') as f:
            f.write(ENV_FILE_TEMPLATE)
        return True
    except Exception:
        return False

if os.path.exists('.env'):
    Config.load_from_file('.env')

validation_result = Config.validate_config()
if not validation_result['valid']:
    print("⚠️ Configuration validation failed:")
    for issue in validation_result['issues']:
        print(f"  ❌ {issue}")

if validation_result['warnings']:
    print("⚠️ Configuration warnings:")
    for warning in validation_result['warnings']:
        print(f"  ⚠️ {warning}")
