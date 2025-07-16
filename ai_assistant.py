import os
import duckdb
import pandas as pd
import logging
from tqdm import tqdm
from db_schema import get_database_schema, get_sample_queries, get_database_documentation
from config import Config
from ai_providers import create_ai_provider

logger = logging.getLogger(__name__)

class FunctionalProfileVanna:
    def __init__(self, db_path=None, config:Config=None):
        """
        Initialize AI assistant with the functional profile database
        
        Args:
            db_path (str): Path to the DuckDB database (uses Config.DATABASE_PATH if None)
            ai_config (dict): AI configuration (uses Config.get_ai_config() if None)
        """
        self.db_path = db_path or config.DATABASE_PATH
        self.retrain_threshold = config.RETRAIN_THRESHOLD
        self.progress_bar = config.PROGRESS_BAR_ENABLED
        self.max_results = config.MAX_QUERY_RESULTS
        
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database {self.db_path} not found! Please run process_functional_profiles.py first.")
        
        self.ai_provider = create_ai_provider(config)
        
        self.conn = duckdb.connect(self.db_path)
        
        logger.info(f"Initialized AI assistant with provider: {config.LLM_PROVIDER}")
    
    def setup_training_data(self, force_retrain=False):
        """Set up training data for AI to understand the database schema"""
        logger.info("Setting up training data for AI...")
        
        if not force_retrain:
            try:
                print("🔍 Checking for existing training data...")
                existing_data = self.ai_provider.get_training_data()
                if len(existing_data) > self.retrain_threshold:
                    logger.info(f"Found {len(existing_data)} existing training examples. Skipping retrain.")
                    print(f"✅ Found {len(existing_data)} existing training examples. Skipping retrain.")
                    return
            except:
                pass
        
        print("📚 Training AI model with database schema and examples...")
        
        self._connect_ai_to_database()
        
        self._auto_train_from_information_schema()
        
        self._train_with_domain_knowledge()
        
        print("💡 Adding sample SQL queries...")
        self._train_with_sample_queries()
        
        logger.info("Training data setup complete!")
        print("✅ Training data setup complete!")
    
    def _connect_ai_to_database(self):
        """Connect AI provider to DuckDB if supported"""
        if hasattr(self.ai_provider, 'connect_to_duckdb'):
            try:
                print("🔌 Connecting AI provider to database...")
                self.ai_provider.connect_to_duckdb(url=self.db_path)
                print("✅ AI provider connected to database")
            except Exception as e:
                logger.warning(f"Could not connect AI provider to database: {e}")
    
    def _auto_train_from_information_schema(self):
        """Auto-train from information schema like in the notebook"""
        try:
            print("🔍 Analyzing database schema...")
            
            if hasattr(self.ai_provider, 'run_sql'):
                df_information_schema = self.ai_provider.run_sql("""
                    SELECT 
                        table_schema,
                        table_name, 
                        column_name,
                        data_type,
                        is_nullable
                    FROM information_schema.columns 
                    WHERE table_schema NOT IN ('information_schema', 'main', 'pg_catalog', 'temp')
                    ORDER BY table_schema, table_name, ordinal_position
                """)
            else:
                df_information_schema = self.conn.execute("""
                    SELECT 
                        table_schema,
                        table_name, 
                        column_name,
                        data_type,
                        is_nullable
                    FROM information_schema.columns 
                    WHERE table_schema NOT IN ('information_schema', 'main', 'pg_catalog', 'temp')
                    ORDER BY table_schema, table_name, ordinal_position
                """).df()
            
            print(f"📊 Found {len(df_information_schema)} columns across all schemas")
            
            if hasattr(self.ai_provider, 'get_training_plan_generic'):
                print("📋 Generating training plan...")
                plan = self.ai_provider.get_training_plan_generic(df_information_schema)
                print(f"📝 Training plan contains {len(plan._plan)} items")
                
                for item in plan._plan[:3]:
                    print(f"- {item.item_type}: {str(item.item_value)[:100]}...")
                
                print("🏋️ Training AI model with schema...")
                self.ai_provider.train(plan=plan)
                print("✅ Schema training complete!")
            else:
                self._train_with_manual_schema(df_information_schema)
                
        except Exception as e:
            logger.warning(f"Could not auto-train from information schema: {e}")
            self._train_with_manual_schema_fallback()
    
    def _train_with_manual_schema(self, df_information_schema):
        """Manual schema training as fallback"""
        print("📝 Manual schema training...")
        
        schemas = df_information_schema['table_schema'].unique()
        
        for schema in tqdm(schemas, desc="Training schemas"):
            schema_tables = df_information_schema[df_information_schema['table_schema'] == schema]
            tables = schema_tables['table_name'].unique()
            
            for table in tables[:5]:
                table_columns = schema_tables[schema_tables['table_name'] == table]
                
                ddl = f"-- Schema: {schema}\nCREATE TABLE {schema}.{table} (\n"
                column_defs = []
                
                for _, col in table_columns.iterrows():
                    nullable = "" if col['is_nullable'] == 'YES' else " NOT NULL"
                    column_defs.append(f"    {col['column_name']} {col['data_type']}{nullable}")
                
                ddl += ",\n".join(column_defs) + "\n);"
                
                self.ai_provider.add_ddl(ddl)
    
    def _train_with_manual_schema_fallback(self):
        """Fallback manual schema training"""
        print("📚 Fallback schema training...")
        
        schema_info = get_database_schema(self.conn)
        documentation = get_database_documentation()
        
        self.ai_provider.add_documentation(documentation['overview'])
        
        for schema_name, tables in schema_info.items():
            schema_desc = documentation['schemas'].get(schema_name, {}).get('description', '')
            self.ai_provider.add_documentation(f"""
            Schema: {schema_name}
            Description: {schema_desc}
            Tables: {', '.join(tables.keys())}
            """)
    
    def _train_with_domain_knowledge(self):
        """Add domain-specific metagenomic analysis knowledge"""
        print("🧬 Adding metagenomic domain knowledge...")
        
        metagenomic_docs = """
        METAGENOMIC DATABASE KNOWLEDGE:
        
        This database contains metagenomic analysis results with key concepts:
        
        SCHEMAS AND DATA TYPES:
        - functional_profile: KEGG Orthology (KO) functional annotations with abundance data (unified table)
        - taxa_profiles: Taxonomic classifications with confidence scores and organism identification
        - functional_profile_data: Sourmash gather results with sequence similarity metrics
        - sigs_aa/sigs_dna: Protein and DNA signature data with min-hash values
        - signature_mins_sigs_aa/signature_mins_sigs_dna: Individual hash values for each sample's signatures
        
        SAMPLE IDENTIFICATION:
        - Sample IDs follow pattern DRR****** (DRR + 6 digits, e.g., DRR012227, DRR000001)  
        - functional_profile.profiles has sample_id column linking all data
        - Other schemas have sample-specific tables named by sample_id
        
        KEY METRICS:
        - KO abundance: Quantitative functional gene family abundance
        - max_containment: Sequence similarity containment score (0-1)
        - f_query_match: Fraction of query sequence matched (0-1)
        - average_containment_ani: Average nucleotide identity based on containment  
        - actual_confidence_with_coverage: Taxonomic confidence with coverage correction
        - in_sample_est: Boolean estimate if organism is present in sample
        
        ORGANISM IDENTIFICATION:
        - organism_name: Full organism name from reference database
        - organism_id: Extracted NCBI accession (GCA_/GCF_) identifier
        - KO IDs format: 'ko:K00001', etc.
        """
        
        self.ai_provider.train(documentation=metagenomic_docs)
        
        sample_ddls = [
            """
            -- Functional profile unified table (all samples)
            CREATE TABLE functional_profile.profiles (
                sample_id VARCHAR,          -- Sample identifier (DRR******)
                ko_id VARCHAR,              -- KEGG Orthology ID (e.g., 'ko:K00001')
                abundance DOUBLE            -- Quantitative abundance measurement
            );
            """,

            """
            -- Gather data table (all samples combined)
            CREATE TABLE functional_profile_data.gather_data (
                sample_id VARCHAR,                   -- Sample identifier (DRR******)
                intersect_bp INTEGER,                -- Base pairs of intersection
                jaccard DOUBLE,                      -- Jaccard similarity index
                max_containment DOUBLE,              -- Maximum containment score (0-1)
                f_query_match DOUBLE,                -- Fraction of query matched (0-1)
                f_match_query DOUBLE,                -- Fraction of match covered by query
                match_filename VARCHAR,              -- Filename of matching reference
                match_name VARCHAR,                  -- Name of matching reference sequence
                match_md5 VARCHAR,                   -- MD5 hash of match
                match_bp BIGINT,                    -- Base pairs in match
                query_filename VARCHAR,              -- Query filename
                query_name VARCHAR,                  -- Query sequence name
                query_md5 VARCHAR,                   -- MD5 hash of query
                query_bp INTEGER,                    -- Base pairs in query
                ksize INTEGER,                       -- K-mer size used
                moltype VARCHAR,                     -- Molecule type (DNA/protein)
                scaled INTEGER,                      -- Scaling factor
                query_n_hashes INTEGER,              -- Number of hashes in query
                query_abundance BOOLEAN,             -- Whether query has abundance data
                query_containment_ani DOUBLE,        -- Query containment ANI
                match_containment_ani DOUBLE,        -- Match containment ANI
                average_containment_ani DOUBLE,      -- Average nucleotide identity
                max_containment_ani DOUBLE,          -- Maximum containment ANI
                potential_false_negative BOOLEAN     -- Flag for potential false negatives
            );
            """,

            """
            -- Taxa profiles unified table (all samples)
            CREATE TABLE taxa_profiles.profiles (
                sample_id VARCHAR,                          -- Sample identifier (DRR******)
                organism_name VARCHAR,                      -- Full organism name
                organism_id VARCHAR,                        -- Extracted NCBI accession (GCA_/GCF_)
                tax_id INTEGER,                             -- NCBI taxonomy ID (-1 if not mapped)
                num_unique_kmers_in_genome_sketch INTEGER,  -- Unique k-mers in genome sketch
                num_total_kmers_in_genome_sketch INTEGER,   -- Total k-mers in genome sketch
                scale_factor DOUBLE,                        -- Scale factor
                num_exclusive_kmers_in_sample_sketch INTEGER, -- Exclusive k-mers in sample
                num_total_kmers_in_sample_sketch INTEGER,   -- Total k-mers in sample sketch
                min_coverage DOUBLE,                        -- Minimum coverage
                p_vals DOUBLE,                              -- P-values
                num_exclusive_kmers_to_genome INTEGER,      -- Exclusive k-mers to genome
                num_exclusive_kmers_to_genome_coverage DOUBLE, -- Coverage of exclusive k-mers
                num_matches INTEGER,                        -- Number of matches
                acceptance_threshold_with_coverage DOUBLE,  -- Acceptance threshold with coverage
                actual_confidence_with_coverage DOUBLE,     -- Actual confidence with coverage
                alt_confidence_mut_rate_with_coverage DOUBLE, -- Alternative confidence
                in_sample_est BOOLEAN                       -- Boolean estimate if in sample
            );
            """,

            """
            -- Protein signatures manifest table
            CREATE TABLE sigs_aa.manifests (
                internal_location VARCHAR,      -- Internal file location in archive
                md5 VARCHAR,                    -- MD5 hash of signature
                md5short VARCHAR,               -- Short MD5 hash
                ksize INTEGER,                  -- K-mer size
                moltype VARCHAR,                -- Molecule type (protein)
                num INTEGER,                    -- Number parameter
                scaled INTEGER,                 -- Scaling factor
                n_hashes INTEGER,               -- Number of hashes
                with_abundance BOOLEAN,         -- Has abundance data
                name VARCHAR,                   -- Signature name
                filename VARCHAR,               -- Original filename
                sample_id VARCHAR,              -- Sample identifier (DRR******)
                archive_file VARCHAR            -- Source archive filename
            );
            """,
            
            """
            -- Protein signatures metadata table
            CREATE TABLE sigs_aa.signatures (
                md5 VARCHAR,                    -- MD5 hash linking to manifest
                sample_id VARCHAR,              -- Sample identifier (DRR******)
                hash_function VARCHAR,          -- Hash function used
                molecule VARCHAR,               -- Molecule type (protein)
                filename VARCHAR,               -- Signature filename
                class VARCHAR,                  -- Signature class
                email VARCHAR,                  -- Contact email
                license VARCHAR,                -- License information
                ksize INTEGER,                  -- K-mer size
                seed INTEGER,                   -- Random seed
                max_hash BIGINT,                -- Maximum hash value
                num_mins INTEGER,               -- Number of min-hash values
                signature_size INTEGER,         -- Estimated signature size
                has_abundances BOOLEAN,         -- Whether signature has abundances
                archive_file VARCHAR            -- Source archive filename
            );
            """,
            
            """
            -- Protein signature min-hash unified table (all samples)
            CREATE TABLE sigs_aa.signature_mins (
                sample_id VARCHAR,              -- Sample identifier (DRR******)
                md5 VARCHAR,                    -- MD5 hash linking to signature metadata
                min_hash BIGINT,                -- Individual min-hash value
                abundance INTEGER,              -- Abundance for this hash (1 if no abundance data)
                position INTEGER                -- Position index in signature
            );
            """,
            
            """
            -- DNA signatures manifest table
            CREATE TABLE sigs_dna.manifests (
                internal_location VARCHAR,      -- Internal file location in archive
                md5 VARCHAR,                    -- MD5 hash of signature
                md5short VARCHAR,               -- Short MD5 hash
                ksize INTEGER,                  -- K-mer size
                moltype VARCHAR,                -- Molecule type (DNA)
                num INTEGER,                    -- Number parameter
                scaled INTEGER,                 -- Scaling factor
                n_hashes INTEGER,               -- Number of hashes
                with_abundance BOOLEAN,         -- Has abundance data
                name VARCHAR,                   -- Signature name
                filename VARCHAR,               -- Original filename
                sample_id VARCHAR,              -- Sample identifier (DRR******)
                archive_file VARCHAR            -- Source archive filename
            );
            """,
            
            """
            -- DNA signatures metadata table
            CREATE TABLE sigs_dna.signatures (
                md5 VARCHAR,                    -- MD5 hash linking to manifest
                sample_id VARCHAR,              -- Sample identifier (DRR******)
                hash_function VARCHAR,          -- Hash function used
                molecule VARCHAR,               -- Molecule type (DNA)
                filename VARCHAR,               -- Signature filename
                class VARCHAR,                  -- Signature class
                email VARCHAR,                  -- Contact email
                license VARCHAR,                -- License information
                ksize INTEGER,                  -- K-mer size
                seed INTEGER,                   -- Random seed
                max_hash BIGINT,                -- Maximum hash value
                num_mins INTEGER,               -- Number of min-hash values
                signature_size INTEGER,         -- Estimated signature size
                has_abundances BOOLEAN,         -- Whether signature has abundances
                archive_file VARCHAR            -- Source archive filename
            );
            """,
            
            """
            -- DNA signature min-hash unified table (all samples)
            CREATE TABLE sigs_dna.signature_mins (
                sample_id VARCHAR,              -- Sample identifier (DRR******)
                md5 VARCHAR,                    -- MD5 hash linking to signature metadata
                min_hash BIGINT,                -- Individual min-hash value
                abundance INTEGER,              -- Abundance for this hash (1 if no abundance data)
                position INTEGER                -- Position index in signature
            );
            """
        ]
        
        for ddl in sample_ddls:
            self.ai_provider.train(ddl=ddl)
        
        additional_docs = """
        TABLE NAMING AND ACCESS PATTERNS:
        
        FUNCTIONAL PROFILES:
        - Schema: functional_profile
        - Table: profiles (unified table for all samples)
        - Access pattern: SELECT ko_id, abundance FROM functional_profile.profiles WHERE sample_id = 'DRR012227'
        - Filter by sample: WHERE sample_id = 'sample_name'
        - Aggregate across samples: GROUP BY sample_id or GROUP BY ko_id
        
        TAXA PROFILES:
        - Schema: taxa_profiles  
        - Table: profiles (unified table for all samples)
        - Access pattern: SELECT organism_name, tax_id, actual_confidence_with_coverage FROM taxa_profiles.profiles WHERE sample_id = 'DRR012227'
        - Filter by sample: WHERE sample_id = 'sample_name'
        - Filter by taxonomy: WHERE tax_id = specific_id OR WHERE tax_id != -1 (for mapped organisms)
        - Cross-sample analysis: GROUP BY sample_id
        - Organism analysis: GROUP BY organism_name, organism_id, tax_id
        - Taxonomy analysis: GROUP BY tax_id for phylogenetic studies
        
        TAXONOMY MAPPING:
        - tax_id column contains NCBI taxonomy IDs
        - Value -1 indicates no taxonomy mapping found
        - Use WHERE tax_id != -1 to filter only mapped organisms
        - Use WHERE tax_id = -1 to find unmapped organisms
        - Taxonomy IDs enable phylogenetic and taxonomic hierarchy analysis
        
        GATHER DATA:
        - Schema: functional_profile_data
        - Table: gather_data (contains all samples)
        - Access pattern: SELECT * FROM functional_profile_data.gather_data WHERE sample_id = 'DRR012227'
        
        SIGNATURE MANIFESTS:
        - Schemas: sigs_aa, sigs_dna
        - Tables: manifests (contains all samples)
        - Access pattern: SELECT * FROM sigs_aa.manifests WHERE sample_id = 'DRR012227'
        
        SIGNATURE METADATA:
        - Schemas: sigs_aa, sigs_dna
        - Tables: signatures (contains all samples)
        - Access pattern: SELECT * FROM sigs_aa.signatures WHERE sample_id = 'DRR012227'
        
        SIGNATURE MIN-HASH VALUES:
        - Schemas: sigs_aa, sigs_dna
        - Tables: signature_mins (unified tables for all samples)
        - Access pattern: SELECT min_hash, abundance FROM sigs_aa.signature_mins WHERE sample_id = 'DRR012227'
        - Filter by sample: WHERE sample_id = 'sample_name'
        - Cross-sample analysis: GROUP BY sample_id
        - Hash overlap analysis: JOIN on min_hash between samples
        
        UNIFIED TABLE QUERY PATTERNS:
        - All major tables now have sample_id column for easy filtering and joining
        - Single sample queries: WHERE sample_id = 'DRR012227'
        - Multiple sample queries: WHERE sample_id IN ('DRR012227', 'DRR000001')
        - Cross-sample analysis: GROUP BY sample_id
        - Data comparison: JOIN tables on sample_id
        - Sample overlap analysis: JOIN on shared values (ko_id, organism_id, min_hash, tax_id)
        - Organism analysis: GROUP BY organism_name, organism_id across all samples
        - Functional analysis: GROUP BY ko_id across all samples
        - Taxonomic analysis: GROUP BY tax_id for phylogenetic studies
        """
        
        self.ai_provider.train(documentation=additional_docs)
    
    def _train_with_sample_queries(self):
        """Add sample queries for training"""
        sample_queries = get_sample_queries()
        
        query_progress = tqdm(sample_queries.items(), desc="Adding sample queries", unit="query")
        
        for description, sql in query_progress:
            query_progress.set_postfix(query=description[:50] + "...")
            self.ai_provider.add_question_sql(question=description, sql=sql)
    
    def ask_question(self, question):
        """
        Ask a natural language question about the database
        
        Args:
            question (str): Natural language question
            
        Returns:
            dict: Contains SQL query, results, and explanation
        """
        try:
            logger.info(f"Processing question: {question}")
            
            print("🤖 Generating SQL query...")
            with tqdm(total=2, desc="Processing", unit="step", disable=not self.progress_bar) as pbar:
                pbar.set_description("Generating SQL")
                sql = self.ai_provider.generate_sql(question)
                logger.info(f"Generated SQL: {sql}")
                pbar.update(1)
                
                pbar.set_description("Executing query")
                df = self.conn.execute(sql).df()
                
                if len(df) > self.max_results:
                    logger.warning(f"Query returned {len(df)} rows, limiting to {self.max_results}")
                    df = df.head(self.max_results)
                
                pbar.update(1)
            
            explanation = self._generate_simple_explanation(sql, df)
            
            return {
                'question': question,
                'sql': sql,
                'results': df,
                'explanation': explanation,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Error processing question: {str(e)}")
            return {
                'question': question,
                'error': str(e),
                'success': False
            }
    
    def _generate_simple_explanation(self, sql, df):
        """Generate a simple explanation based on the SQL query and results"""
        explanation_parts = []
        
        sql_lower = sql.lower()
        
        if 'select count' in sql_lower:
            explanation_parts.append("This query counts records in the database.")
        elif 'group by' in sql_lower:
            explanation_parts.append("This query groups data and provides aggregated results.")
        elif 'join' in sql_lower:
            explanation_parts.append("This query combines data from multiple tables.")
        elif 'distinct' in sql_lower:
            explanation_parts.append("This query finds unique values.")
        else:
            explanation_parts.append("This query retrieves data from the database.")
        
        if len(df) == 0:
            explanation_parts.append("No matching records were found.")
        elif len(df) == 1:
            explanation_parts.append("Found 1 matching record.")
        else:
            explanation_parts.append(f"Found {len(df)} matching records.")
        
        if 'functional_profile' in sql_lower:
            explanation_parts.append("The query examines functional annotation data (KO gene families).")
        if 'taxa_profiles' in sql_lower:
            explanation_parts.append("The query analyzes taxonomic classification data.")
        if 'gather_data' in sql_lower:
            explanation_parts.append("The query uses sequence similarity analysis results.")
        if 'signatures' in sql_lower:
            explanation_parts.append("The query examines sourmash signature data.")
        
        return " ".join(explanation_parts)

    def interactive_mode(self):
        """Start interactive question-answering mode"""
        from interactive_ui import InteractiveUI
        
        ui = InteractiveUI(self)
        ui.run()
    
    def launch_flask_app(self, host='127.0.0.1', port=5000, debug=False):
        """
        Launch Flask web interface for the AI assistant
        
        Args:
            host (str): Host to bind to
            port (int): Port to bind to  
            debug (bool): Enable debug mode
        """
        try:
            try:
                from vanna.flask import VannaFlaskApp
            except ImportError:
                print("❌ Flask dependencies not found!")
                print("Install with: pip install flask 'vanna[flask]'")
                return
            
            print(f"🌐 Starting Flask web interface on http://{host}:{port}")
            print("💡 Use Ctrl+C to stop the server")
            
            vn = self.ai_provider.vn
            vn.connect_to_duckdb(url=self.db_path)

            app = VannaFlaskApp(vn, allow_llm_to_see_data=True)
            app.run(host=host, port=port, debug=debug)
            
        except Exception as e:
            logger.error(f"Error launching Flask app: {str(e)}")
            print(f"❌ Error launching Flask app: {str(e)}")
            print("Make sure Flask dependencies are installed: pip install flask 'vanna[flask]'")
