"""
AI Provider implementations for different LLM services
All providers use ChromaDB for vector storage and have self.vn attribute for Flask compatibility
"""

import logging
from typing import List
from config import Config

from vanna.types import TrainingPlan, TrainingPlanItem

logger = logging.getLogger(__name__)


class BaseProvider:
    """Base class for all AI providers"""

    def __init__(self, config):
        self.config = config or {}
        self.vn = None

    def connect_to_duckdb(self, url):
        """Connect to DuckDB database"""
        return self.vn.connect_to_duckdb(url)

    def train(self, **kwargs):
        """Train the model with various data types"""
        return self.vn.train(**kwargs)

    def get_training_data(self):
        """Get existing training data"""
        return self.vn.get_training_data()

    def get_training_plan_generic(self, df):
        """Generate training plan from dataframe"""
        return self.vn.get_training_plan_generic(df)

    def generate_sql(self, question):
        """Generate SQL from natural language question"""
        return self.vn.generate_sql(question)

    def ask(self, question):
        """Ask a question and get formatted response"""
        return self.vn.ask(question)

    def run_sql(self, sql):
        """Execute SQL query"""
        return self.vn.run_sql(sql)

    def remove_training_data(self, id):
        """Remove training data by ID"""
        return self.vn.remove_training_data(id=id)

    def add_ddl(self, ddl):
        """Add DDL statement for training"""
        return self.vn.train(ddl=ddl)

    def add_documentation(self, doc):
        """Add documentation for training"""
        return self.vn.train(documentation=doc)

    def add_question_sql(self, question, sql):
        """Add question-SQL pair for training"""
        return self.vn.train(question=question, sql=sql)

    def generate_explanation(self, question, sql):
        """Generate explanation for SQL query"""
        if hasattr(self.vn, "generate_explanation"):
            return self.vn.generate_explanation(question, sql)
        else:
            return f"This query was generated to answer: {question}"


def get_available_providers():
    """Get list of available AI providers based on installed packages"""
    providers = []

    try:
        from vanna.chromadb import ChromaDB_VectorStore
        import openai

        providers.append("openai")
    except ImportError:
        pass

    try:
        from vanna.chromadb import ChromaDB_VectorStore
        import requests

        providers.append("ollama")
    except ImportError:
        pass

    return providers


class OpenAIProvider(BaseProvider):
    """
    Universal OpenAI-compatible provider with ChromaDB vector storage
    Supports OpenAI API and other OpenAI-compatible endpoints
    """

    def __init__(self, config):
        super().__init__(config)

        try:
            from vanna.chromadb import ChromaDB_VectorStore
            from vanna.base import VannaBase
            import openai

            class VannaOpenAIImpl(ChromaDB_VectorStore, VannaBase):
                def __init__(self, config=None):
                    chroma_config = {"path": config.get("path", "./chroma_openai_db")}
                    ChromaDB_VectorStore.__init__(self, config=chroma_config)
                    VannaBase.__init__(self, config=config)

                    # Configure OpenAI client
                    api_key = config.get("api_key")
                    base_url = config.get("base_url")
                    
                    # Set base_url to None if empty (OpenAI default) or use provided value
                    if base_url == "" or base_url is None:
                        base_url = None
                    
                    # Create OpenAI client with optional base_url
                    client_config = {"api_key": api_key}
                    if base_url is not None:
                        client_config["base_url"] = base_url
                    
                    self.client = openai.OpenAI(**client_config)
                    
                    # Set default model
                    default_model = "gpt-4o-mini"
                    
                    self.model = config.get("model", default_model)
                    self.max_tokens = config.get("max_tokens", 4096)
                    self.temperature = config.get("temperature", 0.1)

                    self.db = None
                    self.db_type = None

                def system_message(self, message: str) -> any:
                    """Format system message"""
                    return {"role": "system", "content": message}

                def user_message(self, message: str) -> any:
                    """Format user message"""
                    return {"role": "user", "content": message}

                def assistant_message(self, message: str) -> any:
                    """Format assistant message"""
                    return {"role": "assistant", "content": message}

                def run_sql(self, sql: str):
                    """Execute SQL query and return results as DataFrame"""
                    if self.db is None:
                        raise Exception(
                            "Please connect to a database using connect_to_duckdb() first"
                        )

                    try:
                        result = self.db.execute(sql).df()
                        return result
                    except Exception as e:
                        logger.error(f"SQL execution error: {str(e)}")
                        raise

                def submit_prompt(self, prompt, **kwargs):
                    """Submit prompt to OpenAI-compatible API"""
                    try:
                        if isinstance(prompt, str):
                            messages = [{"role": "user", "content": prompt}]
                        elif isinstance(prompt, list):
                            messages = prompt
                        else:
                            messages = [{"role": "user", "content": str(prompt)}]

                        response = self.client.chat.completions.create(
                            model=self.model,
                            messages=messages,
                            max_tokens=self.max_tokens,
                            temperature=self.temperature,
                        )
                        return response.choices[0].message.content
                    except Exception as e:
                        logger.error(f"OpenAI API error: {str(e)}")
                        raise

                def generate_explanation(self, question, sql):
                    """Generate explanation using OpenAI-compatible API"""
                    prompt = f"""
                    Explain this SQL query in simple terms:
                    
                    Question: {question}
                    SQL: {sql}
                    
                    Provide a brief, clear explanation of what this query does.
                    """

                    try:
                        return self.submit_prompt(prompt)
                    except:
                        return f"This query was generated to answer: {question}"
                    

            self.vn = VannaOpenAIImpl(config=config)

            model_name = config.get("model", "gpt-4o-mini")
            logger.info(f"Initialized OpenAI-compatible provider with model: {model_name}")

        except ImportError as e:
            logger.error(f"Failed to initialize OpenAI provider: {str(e)}")
            raise ImportError(
                "OpenAI-compatible provider requires: pip install 'vanna[chromadb]' openai"
            )
    
    def get_training_plan_generic(self, df):
        """Generate training plan from dataframe"""
        return get_training_plan(df)


class OllamaProvider(BaseProvider):
    """
    Ollama provider with ChromaDB vector storage
    """

    def __init__(self, config):
        super().__init__(config)

        try:
            from vanna.chromadb import ChromaDB_VectorStore
            from vanna.base import VannaBase
            import requests

            class VannaOllamaImpl(ChromaDB_VectorStore, VannaBase):
                def __init__(self, config=None):
                    chroma_config = {"path": config.get("path", "./chroma_ollama_db")}
                    ChromaDB_VectorStore.__init__(self, config=chroma_config)
                    VannaBase.__init__(self, config=config)

                    self.base_url = config.get("base_url", "http://localhost:11434")
                    self.model = config.get("model", "llama3.2")
                    self.timeout = config.get("timeout", 300)

                    self.db = None
                    self.db_type = None

                def system_message(self, message: str) -> any:
                    """Format system message"""
                    return {"role": "system", "content": message}

                def user_message(self, message: str) -> any:
                    """Format user message"""
                    return {"role": "user", "content": message}

                def assistant_message(self, message: str) -> any:
                    """Format assistant message"""
                    return {"role": "assistant", "content": message}

                def connect_to_duckdb(self, url: str, init_sql: str = None):
                    """Connect to DuckDB database"""
                    try:
                        import duckdb

                        if url.startswith("motherduck:"):
                            self.db = duckdb.connect(url, read_only=True)
                        else:
                            self.db = duckdb.connect(url, read_only=True)

                        self.db_type = "duckdb"

                        if init_sql:
                            self.db.execute(init_sql)

                        logger.info(f"Connected to DuckDB: {url}")
                        return True

                    except Exception as e:
                        logger.error(f"Failed to connect to DuckDB: {str(e)}")
                        raise

                def run_sql(self, sql: str):
                    """Execute SQL query and return results as DataFrame"""
                    if self.db is None:
                        raise Exception(
                            "Please connect to a database using connect_to_duckdb() first"
                        )

                    try:
                        result = self.db.execute(sql).df()
                        return result
                    except Exception as e:
                        logger.error(f"SQL execution error: {str(e)}")
                        raise

                def submit_prompt(self, prompt, **kwargs):
                    """Submit prompt to Ollama API"""
                    try:
                        if isinstance(prompt, str):
                            messages = [{"role": "user", "content": prompt}]
                        elif isinstance(prompt, list):
                            messages = prompt
                        else:
                            messages = [{"role": "user", "content": str(prompt)}]

                        full_prompt = "\n".join([msg["content"] for msg in messages])

                        response = requests.post(
                            f"{self.base_url}/api/generate",
                            json={
                                "model": self.model,
                                "prompt": full_prompt,
                                "stream": False,
                            },
                            timeout=self.timeout,
                        )
                        response.raise_for_status()
                        return response.json()["response"]
                    except Exception as e:
                        logger.error(f"Ollama API error: {str(e)}")
                        raise

                def generate_explanation(self, question, sql):
                    """Generate explanation using Ollama"""
                    prompt = f"""
                    Explain this SQL query in simple terms:
                    
                    Question: {question}
                    SQL: {sql}
                    
                    Provide a brief, clear explanation of what this query does.
                    """

                    try:
                        return self.submit_prompt(prompt)
                    except:
                        return f"This query was generated to answer: {question}"

            self.vn = VannaOllamaImpl(config=config)

            logger.info(
                f"Initialized Ollama provider with model: {config.get('model', 'llama3.2')}"
            )

        except ImportError:
            raise ImportError(
                "Ollama support requires: pip install 'vanna[chromadb]' requests"
            )


def create_ai_provider(config: Config = None):
    """Create AI provider based on configuration"""
    provider = config.LLM_PROVIDER.lower()

    if provider == "openai":
        provider_config = {
            "api_key": config.API_KEY,
            "model": config.MODEL,
            "base_url": config.BASE_URL,
            "max_tokens": config.MAX_TOKENS,
            "temperature": config.TEMPERATURE,
            "path": config.CHROMADB_PATH,
        }
        logger.info("Creating OpenAI provider with ChromaDB")
        return OpenAIProvider(provider_config)
        
    elif provider == "ollama":
        provider_config = {
            "model": config.OLLAMA_MODEL,
            "base_url": config.OLLAMA_HOST,
            "timeout": config.OLLAMA_TIMEOUT,
            "path": config.CHROMADB_PATH,
        }
        logger.info("Creating Ollama provider with ChromaDB")
        return OllamaProvider(provider_config)
    else:
        raise ValueError(
            f"Unknown AI provider: {provider}. Available providers: openai, ollama"
        )

def get_training_plan(df) -> TrainingPlan:
    """
    This method is used to generate a training plan from an information schema dataframe.

    Basically what it does is breaks up INFORMATION_SCHEMA.COLUMNS into groups of table/column descriptions that can be used to pass to the LLM.

    Args:
        df (pd.DataFrame): The dataframe to generate the training plan from.

    Returns:
        TrainingPlan: The training plan.
    """
    # Validate input
    if df is None or df.empty:
        logger.warning("DataFrame is None or empty")
        return TrainingPlan([])
    
    logger.info(f"DataFrame columns: {df.columns.tolist()}")
    
    # For each of the following, we look at the df columns to see if there's a match:
    database_candidates = df.columns[
        df.columns.str.lower().str.contains("database")
        | df.columns.str.lower().str.contains("table_catalog")
    ].to_list()
    
    schema_candidates = df.columns[
        df.columns.str.lower().str.contains("table_schema")
    ].to_list()
    
    if not schema_candidates:
        logger.error("No table_schema column found in DataFrame")
        return TrainingPlan([])
    
    table_candidates = df.columns[
        df.columns.str.lower().str.contains("table_name")
    ].to_list()
    
    if not table_candidates:
        logger.error("No table_name column found in DataFrame")
        return TrainingPlan([])
    
    # Use database column if available, otherwise use a default database name
    if database_candidates:
        database_column = database_candidates[0]
        use_database_column = True
    else:
        database_column = None
        use_database_column = False
        logger.info("No database/table_catalog column found, using default database name")
    
    schema_column = schema_candidates[0]
    table_column = table_candidates[0]
    
    columns = [schema_column, table_column]
    if use_database_column:
        columns.insert(0, database_column)
    
    candidates = ["column_name", "data_type", "comment"]
    matches = df.columns.str.lower().str.contains("|".join(candidates), regex=True)
    columns += df.columns[matches].to_list()

    plan = TrainingPlan([])

    try:
        if use_database_column:
            # Original logic with database column
            for database in df[database_column].unique().tolist():
                for schema in (
                    df.query(f'{database_column} == "{database}"')[schema_column]
                    .unique()
                    .tolist()
                ):
                    for table in (
                        df.query(
                            f'{database_column} == "{database}" and {schema_column} == "{schema}"'
                        )[table_column]
                        .unique()
                        .tolist()
                    ):
                        df_columns_filtered_to_table = df.query(
                            f'{database_column} == "{database}" and {schema_column} == "{schema}" and {table_column} == "{table}"'
                        )
                        doc = f"The following columns are in the {table} table in the {database} database:\n\n"
                        doc += df_columns_filtered_to_table[columns].to_markdown()

                        plan._plan.append(
                            TrainingPlanItem(
                                item_type=TrainingPlanItem.ITEM_TYPE_IS,
                                item_group=f"{database}.{schema}",
                                item_name=table,
                                item_value=doc,
                            )
                        )
        else:
            # Modified logic without database column
            default_database = "main"
            for schema in df[schema_column].unique().tolist():
                for table in (
                    df.query(f'{schema_column} == "{schema}"')[table_column]
                    .unique()
                    .tolist()
                ):
                    df_columns_filtered_to_table = df.query(
                        f'{schema_column} == "{schema}" and {table_column} == "{table}"'
                    )
                    doc = f"The following columns are in the {table} table in the {schema} schema:\n\n"
                    doc += df_columns_filtered_to_table[columns].to_markdown()

                    plan._plan.append(
                        TrainingPlanItem(
                            item_type=TrainingPlanItem.ITEM_TYPE_IS,
                            item_group=f"{default_database}.{schema}",
                            item_name=table,
                            item_value=doc,
                        )
                    )
    except Exception as e:
        logger.error(f"Error processing training plan: {e}")
        return TrainingPlan([])

    return plan
