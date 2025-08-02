import os

class Config:
    """
    Centralized configuration for the functional profile database project.
    Environment variables take priority over default values.
    """

    def __init__(self):
        pass

    LLM_PROVIDER: str = os.getenv('LLM_PROVIDER', 'openai')
    API_KEY: str = os.getenv('API_KEY', '')
    MODEL: str = os.getenv('MODEL', 'gpt-4o-mini')
    BASE_URL: str = os.getenv('BASE_URL', None)
    MAX_TOKENS: int = int(os.getenv('MAX_TOKENS', '4096'))
    TEMPERATURE: float = float(os.getenv('TEMPERATURE', '0.7'))

    # Ollama specific settings
    OLLAMA_MODEL: str = os.getenv('OLLAMA_MODEL', 'llama3.1')
    OLLAMA_HOST: str = os.getenv('OLLAMA_HOST', 'http://localhost:11434')
    OLLAMA_TIMEOUT: int = int(os.getenv('OLLAMA_TIMEOUT', '300'))
    
    DATABASE_PATH: str = os.getenv('DATABASE_PATH', 'database.db')
    DATABASE_MEMORY_LIMIT: str = os.getenv('DATABASE_MEMORY_LIMIT', '4GB')
    DATABASE_THREADS: int = int(os.getenv('DATABASE_THREADS', '4'))
    DATABASE_TEMP_DIR: str = os.getenv('DATABASE_TEMP_DIR', '/tmp')
    CHROMADB_PATH: str = os.getenv('CHROMADB_PATH', 'chromadb.db')
    
    RETRAIN_THRESHOLD: int = int(os.getenv('RETRAIN_THRESHOLD', '1000'))
    
    DATA_DIR: str = os.getenv('DATA_DIR', './data')
    BATCH_SIZE: int = int(os.getenv('BATCH_SIZE', '100000000'))
    MAX_WORKERS: int = int(os.getenv('MAX_WORKERS', '4'))  # zip workers in the current setup
    
    PROGRESS_BAR_ENABLED: bool = os.getenv('PROGRESS_BAR_ENABLED', 'true').lower() == 'true'
    
    MAX_QUERY_RESULTS: int = int(os.getenv('MAX_QUERY_RESULTS', '1000'))
    CLEANUP_TEMP_FILES: bool = os.getenv('CLEANUP_TEMP_FILES', 'true').lower() == 'true'
    CONTINUE_ON_ERROR: bool = os.getenv('CONTINUE_ON_ERROR', 'true').lower() == 'true'

    LOG_LEVEL = "INFO"