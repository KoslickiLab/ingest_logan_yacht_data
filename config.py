import os
from typing import Optional, Union
from pathlib import Path

class Config:
    """
    Centralized configuration for the functional profile database project.
    Environment variables take priority over default values.
    """

    def __init__(self):
        pass

    # AI Provider Configuration
    LLM_PROVIDER: str = os.getenv('LLM_PROVIDER', 'openai')
    API_KEY: str = os.getenv('API_KEY', '')
    MODEL: str = os.getenv('MODEL', 'gpt-4o-mini')
    BASE_URL: Optional[str] = os.getenv('BASE_URL', None)
    MAX_TOKENS: int = int(os.getenv('MAX_TOKENS', '4096'))
    TEMPERATURE: float = float(os.getenv('TEMPERATURE', '0.7'))

    # Ollama specific settings
    OLLAMA_MODEL: str = os.getenv('OLLAMA_MODEL', 'llama3.1')
    OLLAMA_HOST: str = os.getenv('OLLAMA_HOST', 'http://localhost:11434')
    OLLAMA_TIMEOUT: int = int(os.getenv('OLLAMA_TIMEOUT', '300'))
    
    # Database Configuration
    DATABASE_PATH: str = os.getenv('DATABASE_PATH', 'database.db')
    DATABASE_MEMORY_LIMIT: str = os.getenv('DATABASE_MEMORY_LIMIT', '4GB')
    DATABASE_THREADS: int = int(os.getenv('DATABASE_THREADS', '4'))
    DATABASE_TEMP_DIR: str = os.getenv('DATABASE_TEMP_DIR', '/tmp')
    CHROMADB_PATH: str = os.getenv('CHROMADB_PATH', 'chromadb.db')
    
    # Training Configuration
    RETRAIN_THRESHOLD: int = int(os.getenv('RETRAIN_THRESHOLD', '1000'))
    
    # Data Processing Configuration
    DATA_DIR: str = os.getenv('DATA_DIR', './data')
    BATCH_SIZE: int = int(os.getenv('BATCH_SIZE', '100000000'))
    MAX_WORKERS: int = int(os.getenv('MAX_WORKERS', '4'))  # zip workers in the current setup
    
    # UI Configuration
    PROGRESS_BAR_ENABLED: bool = os.getenv('PROGRESS_BAR_ENABLED', 'true').lower() == 'true'
    MAX_QUERY_RESULTS: int = int(os.getenv('MAX_QUERY_RESULTS', '1000'))
    
    # System Configuration
    CLEANUP_TEMP_FILES: bool = os.getenv('CLEANUP_TEMP_FILES', 'true').lower() == 'true'
    CONTINUE_ON_ERROR: bool = os.getenv('CONTINUE_ON_ERROR', 'true').lower() == 'true'
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')

    @classmethod
    def validate(cls) -> bool:
        """Validate configuration settings"""
        errors = []
        
        # Validate AI provider
        if cls.LLM_PROVIDER not in ['openai', 'ollama']:
            errors.append(f"Invalid LLM_PROVIDER: {cls.LLM_PROVIDER}")
        
        # Validate API key for OpenAI
        if cls.LLM_PROVIDER == 'openai' and not cls.API_KEY:
            errors.append("API_KEY is required for OpenAI provider")
        
        # Validate database path
        if not cls.DATABASE_PATH:
            errors.append("DATABASE_PATH cannot be empty")
        
        # Validate numeric values
        if cls.MAX_TOKENS <= 0:
            errors.append("MAX_TOKENS must be positive")
        
        if not 0.0 <= cls.TEMPERATURE <= 2.0:
            errors.append("TEMPERATURE must be between 0.0 and 2.0")
        
        if cls.DATABASE_THREADS <= 0:
            errors.append("DATABASE_THREADS must be positive")
        
        if cls.BATCH_SIZE <= 0:
            errors.append("BATCH_SIZE must be positive")
        
        if cls.MAX_WORKERS <= 0:
            errors.append("MAX_WORKERS must be positive")
        
        if cls.MAX_QUERY_RESULTS <= 0:
            errors.append("MAX_QUERY_RESULTS must be positive")
        
        if errors:
            raise ValueError(f"Configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors))
        
        return True

    @classmethod
    def reload_from_env(cls):
        """Reload configuration from environment variables"""
        cls.LLM_PROVIDER = os.getenv('LLM_PROVIDER', 'openai')
        cls.API_KEY = os.getenv('API_KEY', '')
        cls.MODEL = os.getenv('MODEL', 'gpt-4o-mini')
        cls.BASE_URL = os.getenv('BASE_URL', None)
        cls.MAX_TOKENS = int(os.getenv('MAX_TOKENS', '4096'))
        cls.TEMPERATURE = float(os.getenv('TEMPERATURE', '0.7'))
        cls.OLLAMA_MODEL = os.getenv('OLLAMA_MODEL', 'llama3.1')
        cls.OLLAMA_HOST = os.getenv('OLLAMA_HOST', 'http://localhost:11434')
        cls.OLLAMA_TIMEOUT = int(os.getenv('OLLAMA_TIMEOUT', '300'))
        cls.DATABASE_PATH = os.getenv('DATABASE_PATH', 'database.db')
        cls.DATABASE_MEMORY_LIMIT = os.getenv('DATABASE_MEMORY_LIMIT', '4GB')
        cls.DATABASE_THREADS = int(os.getenv('DATABASE_THREADS', '4'))
        cls.DATABASE_TEMP_DIR = os.getenv('DATABASE_TEMP_DIR', '/tmp')
        cls.CHROMADB_PATH = os.getenv('CHROMADB_PATH', 'chromadb.db')
        cls.RETRAIN_THRESHOLD = int(os.getenv('RETRAIN_THRESHOLD', '1000'))
        cls.DATA_DIR = os.getenv('DATA_DIR', './data')
        cls.BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100000000'))
        cls.MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))
        cls.PROGRESS_BAR_ENABLED = os.getenv('PROGRESS_BAR_ENABLED', 'true').lower() == 'true'
        cls.MAX_QUERY_RESULTS = int(os.getenv('MAX_QUERY_RESULTS', '1000'))
        cls.CLEANUP_TEMP_FILES = os.getenv('CLEANUP_TEMP_FILES', 'true').lower() == 'true'
        cls.CONTINUE_ON_ERROR = os.getenv('CONTINUE_ON_ERROR', 'true').lower() == 'true'
        cls.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    @classmethod
    def get_database_path(cls) -> Path:
        """Get database path as Path object"""
        return Path(cls.DATABASE_PATH)

    @classmethod
    def get_data_dir(cls) -> Path:
        """Get data directory as Path object"""
        return Path(cls.DATA_DIR)

    @classmethod
    def get_chromadb_path(cls) -> Path:
        """Get ChromaDB path as Path object"""
        return Path(cls.CHROMADB_PATH)