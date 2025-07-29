#!/usr/bin/env python
"""
Stage 2: Ingest Parquet files into DuckDB.

This script efficiently loads the Parquet files created by extract_to_parquet.py
into DuckDB using bulk operations and optimized settings.

Key features:
1. Bulk COPY operations from Parquet files
2. Deferred index creation for maximum performance
3. Parallel processing where beneficial
4. Progress tracking and error recovery
5. Memory-efficient processing of large tables
"""
from __future__ import annotations

import os, sys, glob, argparse, json
from datetime import datetime
from pathlib import Path
import shutil

import duckdb
import pandas as pd
from tqdm import tqdm
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

# Track which indexes need to be created
INDEXES_TO_CREATE = []

def setup_logger():
    """Setup logging configuration."""
    logs_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"ingestion_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logging.info(f"Log file: {log_file}")
    return logging.getLogger()

def optimize_duckdb_settings(conn: duckdb.DuckDBPyConnection):
    """Apply optimal DuckDB settings for bulk loading."""
    # Get DuckDB version for compatibility
    version_info = conn.execute("SELECT version()").fetchone()[0]
    logging.info(f"DuckDB version: {version_info}")
    
    # Increase memory limit for better performance
    conn.execute(f"SET memory_limit='{Config.DATABASE_MEMORY_LIMIT}'")
    
    # Use all available threads
    conn.execute(f"SET threads={Config.DATABASE_THREADS}")
    
    # Set temp directory
    conn.execute(f"SET temp_directory='{Config.DATABASE_TEMP_DIR}'")
    
    # Disable checkpoint on WAL for faster loading
    conn.execute("SET checkpoint_threshold='10GB'")
    
    # For DuckDB v1.3.2, we don't have buffer_pool_size
    # Instead, we can set other performance-related settings
    
    # Disable progress bar for bulk operations
    conn.execute("SET enable_progress_bar=false")
    
    # Disable preserving insertion order for better performance
    conn.execute("SET preserve_insertion_order=false")
    
    logging.info("DuckDB settings optimized for bulk loading")

def create_schemas(conn: duckdb.DuckDBPyConnection):
    """Create all necessary schemas."""
    schemas = [
        'functional_profile',
        'functional_profile_data',
        'taxa_profiles',
        'sigs_aa',
        'sigs_dna',
        'geographical_location_data'
    ]
    
    for schema in schemas:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        logging.info(f"Created schema: {schema}")

def create_tables(conn: duckdb.DuckDBPyConnection):
    """Create all tables without indexes."""
    
    # Functional profiles
    conn.execute("""
        CREATE TABLE IF NOT EXISTS functional_profile.profiles (
            sample_id VARCHAR,
            ko_id VARCHAR,
            abundance DOUBLE,
            archive_name VARCHAR
        )
    """)
    INDEXES_TO_CREATE.append(('functional_profile.profiles', 'sample_id'))
    INDEXES_TO_CREATE.append(('functional_profile.profiles', 'ko_id'))
    
    # Taxa profiles
    conn.execute("""
        CREATE TABLE IF NOT EXISTS taxa_profiles.profiles (
            sample_id VARCHAR,
            organism_name VARCHAR,
            organism_id VARCHAR,
            tax_id INTEGER,
            num_unique_kmers_in_genome_sketch INTEGER,
            num_total_kmers_in_genome_sketch INTEGER,
            scale_factor DOUBLE,
            num_exclusive_kmers_in_sample_sketch INTEGER,
            num_total_kmers_in_sample_sketch INTEGER,
            min_coverage DOUBLE,
            p_vals DOUBLE,
            num_exclusive_kmers_to_genome INTEGER,
            num_exclusive_kmers_to_genome_coverage DOUBLE,
            num_matches INTEGER,
            acceptance_threshold_with_coverage DOUBLE,
            actual_confidence_with_coverage DOUBLE,
            alt_confidence_mut_rate_with_coverage DOUBLE,
            in_sample_est BOOLEAN,
            archive_name VARCHAR
        )
    """)
    INDEXES_TO_CREATE.extend([
        ('taxa_profiles.profiles', 'sample_id'),
        ('taxa_profiles.profiles', 'organism_id'),
        ('taxa_profiles.profiles', 'organism_name'),
        ('taxa_profiles.profiles', 'tax_id')
    ])
    
    # Gather data
    conn.execute("""
        CREATE TABLE IF NOT EXISTS functional_profile_data.gather_data (
            sample_id VARCHAR,
            intersect_bp INTEGER,
            jaccard DOUBLE,
            max_containment DOUBLE,
            f_query_match DOUBLE,
            f_match_query DOUBLE,
            match_filename VARCHAR,
            match_name VARCHAR,
            match_md5 VARCHAR,
            match_bp BIGINT,
            query_filename VARCHAR,
            query_name VARCHAR,
            query_md5 VARCHAR,
            query_bp BIGINT,
            ksize INTEGER,
            moltype VARCHAR,
            scaled INTEGER,
            query_n_hashes INTEGER,
            query_abundance BOOLEAN,
            query_containment_ani DOUBLE,
            match_containment_ani DOUBLE,
            average_containment_ani DOUBLE,
            max_containment_ani DOUBLE,
            potential_false_negative BOOLEAN,
            archive_name VARCHAR
        )
    """)
    
    # Signature tables for both aa and dna
    for sig_type in ['sigs_aa', 'sigs_dna']:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {sig_type}.manifests (
                internal_location VARCHAR,
                md5 VARCHAR,
                md5short VARCHAR,
                ksize INTEGER,
                moltype VARCHAR,
                num INTEGER,
                scaled INTEGER,
                n_hashes INTEGER,
                with_abundance BOOLEAN,
                name VARCHAR,
                filename VARCHAR,
                sample_id VARCHAR,
                archive_file VARCHAR,
                archive_name VARCHAR
            )
        """)
        INDEXES_TO_CREATE.extend([
            (f'{sig_type}.manifests', 'sample_id'),
            (f'{sig_type}.manifests', 'md5')
        ])
        
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {sig_type}.signatures (
                md5 VARCHAR,
                sample_id VARCHAR,
                hash_function VARCHAR,
                molecule VARCHAR,
                filename VARCHAR,
                class VARCHAR,
                email VARCHAR,
                license VARCHAR,
                ksize INTEGER,
                seed INTEGER,
                max_hash BIGINT,
                num_mins INTEGER,
                signature_size INTEGER,
                has_abundances BOOLEAN,
                archive_file VARCHAR,
                archive_name VARCHAR
            )
        """)
        INDEXES_TO_CREATE.extend([
            (f'{sig_type}.signatures', 'sample_id'),
            (f'{sig_type}.signatures', 'md5')
        ])
        
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {sig_type}.signature_mins (
                sample_id VARCHAR,
                md5 VARCHAR,
                min_hash BIGINT,
                abundance INTEGER,
                position INTEGER,
                ksize INTEGER
            )
        """)
        INDEXES_TO_CREATE.extend([
            (f'{sig_type}.signature_mins', 'sample_id'),
            (f'{sig_type}.signature_mins', 'md5')
        ])
    
    # Geographical location data
    conn.execute("""
        CREATE TABLE IF NOT EXISTS geographical_location_data.locations (
            accession VARCHAR,
            attribute_name VARCHAR,
            attribute_value VARCHAR,
            lat_lon VARCHAR,
            palm_virome VARCHAR,
            elevation VARCHAR,
            center_name VARCHAR,
            country VARCHAR,
            biome VARCHAR,
            confidence VARCHAR,
            sample_id VARCHAR
        )
    """)
    INDEXES_TO_CREATE.extend([
        ('geographical_location_data.locations', 'accession'),
        ('geographical_location_data.locations', 'sample_id'),
        ('geographical_location_data.locations', 'country'),
        ('geographical_location_data.locations', 'biome')
    ])
    
    logging.info("Created all tables")

def ingest_parquet_files(conn: duckdb.DuckDBPyConnection, parquet_dir: str, 
                        table_name: str, target_table: str):
    """
    Ingest all Parquet files for a table using DuckDB's native Parquet reader.
    
    Args:
        conn: DuckDB connection
        parquet_dir: Directory containing parquet files
        table_name: Name of source table directory
        target_table: Full target table name (schema.table)
    """
    table_path = Path(parquet_dir) / table_name
    
    if not table_path.exists():
        logging.warning(f"No parquet files found for {table_name}")
        return
    
    # Count total files
    parquet_files = list(table_path.rglob("*.parquet"))
    if not parquet_files:
        logging.warning(f"No parquet files found in {table_path}")
        return
    
    logging.info(f"Found {len(parquet_files)} parquet files for {table_name}")
    
    # Check if we have partitioned data
    partitions = [d for d in table_path.iterdir() if d.is_dir() and d.name.startswith("partition_key=")]
    has_partition_key = len(partitions) > 0
    
    # Build the appropriate query based on whether we have partition_key
    if has_partition_key:
        # Use glob pattern to read all files at once
        pattern = str(table_path / "**/*.parquet")
        
        # Get list of columns from first parquet file to check what to exclude
        sample_file = str(parquet_files[0])
        sample_cols = conn.execute(f"SELECT * FROM read_parquet('{sample_file}') LIMIT 0").description
        col_names = [col[0] for col in sample_cols]
        
        # Build EXCLUDE clause if partition_key exists
        exclude_clause = " EXCLUDE (partition_key)" if "partition_key" in col_names else ""
    else:
        pattern = str(table_path / "*.parquet")
        exclude_clause = ""
    
    try:
        # First, check how many rows we're dealing with
        count_query = f"SELECT COUNT(*) FROM read_parquet('{pattern}')"
        row_count = conn.execute(count_query).fetchone()[0]
        logging.info(f"Total rows to ingest for {table_name}: {row_count:,}")
        
        if row_count == 0:
            logging.warning(f"No data found in parquet files for {table_name}")
            return
        
        # For very large tables, use INSERT in chunks
        if row_count > 10_000_000:  # 10 million rows
            logging.info(f"Large table detected, using chunked insertion...")
            
            if partitions:
                # Process each partition separately
                for partition_dir in tqdm(partitions, desc=f"Ingesting {table_name} partitions"):
                    partition_pattern = str(partition_dir / "*.parquet")
                    insert_query = f"""
                        INSERT INTO {target_table}
                        SELECT *{exclude_clause}
                        FROM read_parquet('{partition_pattern}')
                    """
                    conn.execute(insert_query)
            else:
                # Process in chunks using LIMIT/OFFSET
                chunk_size = 5_000_000
                num_chunks = (row_count + chunk_size - 1) // chunk_size
                
                for i in tqdm(range(num_chunks), desc=f"Ingesting {table_name}"):
                    offset = i * chunk_size
                    insert_query = f"""
                        INSERT INTO {target_table}
                        SELECT *{exclude_clause}
                        FROM read_parquet('{pattern}')
                        LIMIT {chunk_size} OFFSET {offset}
                    """
                    conn.execute(insert_query)
        else:
            # For smaller tables, insert all at once
            insert_query = f"""
                INSERT INTO {target_table}
                SELECT *{exclude_clause}
                FROM read_parquet('{pattern}')
            """
            
            logging.info(f"Inserting all data for {table_name}...")
            conn.execute(insert_query)
        
        # Verify insertion
        verify_query = f"SELECT COUNT(*) FROM {target_table}"
        inserted_count = conn.execute(verify_query).fetchone()[0]
        logging.info(f"Successfully inserted {inserted_count:,} rows into {target_table}")
        
    except Exception as e:
        logging.error(f"Error ingesting {table_name}: {e}")
        # Log more details about the error
        if "partition_key" in str(e):
            logging.error(f"Note: Check if partition_key column exists in parquet files")
        raise

def ingest_standalone_files(conn: duckdb.DuckDBPyConnection, data_dir: str):
    """Ingest taxonomy and geographical location files."""
    
    # Process taxonomy files
    logging.info("Processing taxonomy mapping files...")
    tsv_files = glob.glob(os.path.join(data_dir, "*.tsv"))
    txt_files = glob.glob(os.path.join(data_dir, "*taxonomy*.txt"))
    tab_files = glob.glob(os.path.join(data_dir, "*taxonomy*.tab"))
    
    taxonomy_files = tsv_files + txt_files + tab_files
    
    if taxonomy_files:
        # Create temporary table for taxonomy mapping
        conn.execute("CREATE TEMPORARY TABLE temp_taxonomy_mapping (genome_id VARCHAR, taxid INTEGER)")
        
        for file_path in tqdm(taxonomy_files, desc="Loading taxonomy files"):
            try:
                # Try different separators
                for sep in ['\t', ',']:
                    try:
                        df = pd.read_csv(file_path, sep=sep, nrows=5)
                        
                        # Find relevant columns
                        genome_col = None
                        taxid_col = None
                        
                        for col in df.columns:
                            col_lower = col.lower()
                            if any(term in col_lower for term in ['genome_id', 'genome', 'accession']):
                                genome_col = col
                            if any(term in col_lower for term in ['taxid', 'tax_id', 'taxonomy_id']):
                                taxid_col = col
                        
                        if genome_col and taxid_col:
                            # Load full file using DuckDB's CSV reader
                            # First check if file exists and is readable
                            if not os.path.exists(file_path):
                                logging.warning(f"File not found: {file_path}")
                                continue
                                
                            # Use proper column names in query
                            query = f"""
                                INSERT INTO temp_taxonomy_mapping
                                SELECT "{genome_col}" as genome_id, 
                                       CAST("{taxid_col}" AS INTEGER) as taxid
                                FROM read_csv('{file_path}', sep='{sep}', header=true)
                                WHERE "{genome_col}" IS NOT NULL 
                                  AND "{taxid_col}" IS NOT NULL
                                  AND TRY_CAST("{taxid_col}" AS INTEGER) IS NOT NULL
                            """
                            conn.execute(query)
                            logging.info(f"Loaded taxonomy from {os.path.basename(file_path)}")
                            break
                    except Exception as e:
                        logging.debug(f"Failed to read {file_path} with separator '{sep}': {e}")
                        continue
                        
            except Exception as e:
                logging.warning(f"Could not load taxonomy file {file_path}: {e}")
        
        # Update taxa_profiles with taxonomy IDs
        try:
            count = conn.execute("SELECT COUNT(*) FROM temp_taxonomy_mapping").fetchone()[0]
            if count > 0:
                logging.info(f"Updating taxa_profiles with {count} taxonomy mappings...")
                conn.execute("""
                    UPDATE taxa_profiles.profiles
                    SET tax_id = tm.taxid
                    FROM temp_taxonomy_mapping tm
                    WHERE taxa_profiles.profiles.organism_id = tm.genome_id
                      AND taxa_profiles.profiles.tax_id = -1
                """)
                
                updated = conn.execute("""
                    SELECT COUNT(*) FROM taxa_profiles.profiles 
                    WHERE tax_id != -1
                """).fetchone()[0]
                logging.info(f"Updated {updated} taxa profiles with taxonomy IDs")
        except Exception as e:
            logging.warning(f"Could not update taxonomy IDs: {e}")
    
    # Process geographical location files
    logging.info("Processing geographical location files...")
    geo_files = []
    geo_files.extend(glob.glob(os.path.join(data_dir, "*geographical_location*.csv")))
    geo_files.extend(glob.glob(os.path.join(data_dir, "*geo_location*.csv")))
    geo_files.extend(glob.glob(os.path.join(data_dir, "*biosample_geographical*.csv")))
    geo_files.extend(glob.glob(os.path.join(data_dir, "output.csv")))
    
    # Remove duplicates
    geo_files = list(set(geo_files))
    
    for file_path in tqdm(geo_files, desc="Loading geographical files"):
        if not os.path.exists(file_path):
            continue
            
        try:
            # Check if the CSV has the expected columns
            df_sample = pd.read_csv(file_path, nrows=5)
            expected_cols = ['accession', 'attribute_name', 'attribute_value', 'lat_lon',
                           'palm_virome', 'elevation', 'center_name', 'country',
                           'biome', 'confidence', 'sample_id']
            
            # Check if we have at least some of the expected columns
            matching_cols = [col for col in expected_cols if col in df_sample.columns]
            
            if len(matching_cols) >= 5:  # At least 5 matching columns
                # Use DuckDB's CSV reader with all_varchar to handle mixed types
                query = f"""
                    INSERT INTO geographical_location_data.locations
                    SELECT * FROM read_csv('{file_path}', all_varchar=true, header=true)
                """
                conn.execute(query)
                
                count = conn.execute("""
                    SELECT COUNT(*) FROM geographical_location_data.locations
                """).fetchone()[0]
                logging.info(f"Loaded {count} total geographical records")
            else:
                logging.warning(f"Skipping {file_path} - doesn't match expected schema")
                
        except Exception as e:
            logging.warning(f"Could not load geographical file {file_path}: {e}")

def create_indexes(conn: duckdb.DuckDBPyConnection):
    """Create all indexes after data is loaded."""
    logging.info("Creating indexes...")
    
    # Remove duplicates from index list
    unique_indexes = list(set(INDEXES_TO_CREATE))
    
    for table, column in tqdm(unique_indexes, desc="Creating indexes"):
        index_name = f"idx_{table.replace('.', '_')}_{column}"
        try:
            # Check if table exists and has data
            schema, table_name = table.split('.')
            check_query = f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = '{schema}' 
                  AND table_name = '{table_name}'
            """
            if conn.execute(check_query).fetchone()[0] > 0:
                # Check if table has rows
                row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if row_count > 0:
                    conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({column})")
                    logging.info(f"Created index {index_name}")
                else:
                    logging.info(f"Skipping index {index_name} - table is empty")
            else:
                logging.warning(f"Table {table} does not exist, skipping index")
                
        except Exception as e:
            logging.warning(f"Could not create index {index_name}: {e}")

def vacuum_database(conn: duckdb.DuckDBPyConnection):
    """Vacuum and analyze the database for optimal performance."""
    logging.info("Running VACUUM ANALYZE...")
    try:
        conn.execute("VACUUM ANALYZE")
        logging.info("Database optimization complete")
    except Exception as e:
        logging.warning(f"VACUUM ANALYZE failed: {e}")
        # Try just VACUUM if ANALYZE fails
        try:
            conn.execute("VACUUM")
            logging.info("VACUUM complete (ANALYZE skipped)")
        except Exception as e2:
            logging.warning(f"VACUUM also failed: {e2}")

def get_statistics(conn: duckdb.DuckDBPyConnection):
    """Get and display database statistics."""
    stats = {}
    
    tables = [
        ('functional_profile.profiles', 'Functional Profiles'),
        ('taxa_profiles.profiles', 'Taxa Profiles'),
        ('functional_profile_data.gather_data', 'Gather Data'),
        ('sigs_aa.signatures', 'AA Signatures'),
        ('sigs_aa.signature_mins', 'AA Signature Mins'),
        ('sigs_dna.signatures', 'DNA Signatures'),
        ('sigs_dna.signature_mins', 'DNA Signature Mins'),
        ('geographical_location_data.locations', 'Geographical Locations')
    ]
    
    print("\n📊 Database Statistics:")
    print("=" * 60)
    
    for table, name in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats[table] = count
            print(f"{name:<30} {count:>20,} rows")
        except:
            print(f"{name:<30} {'No data':>20}")
    
    # Get database size
    try:
        # For DuckDB 1.3.2, use pragma database_size
        db_size_info = conn.execute("CALL pragma_database_size()").fetchall()
        total_size = 0
        for row in db_size_info:
            if len(row) >= 6:  # database_size returns multiple columns
                total_size += row[2] + row[3] + row[4]  # file_size + wal_size + memory_size
        
        if total_size > 0:
            print(f"\n💾 Total database size: {total_size / (1024**3):.2f} GB")
    except Exception as e:
        logging.debug(f"Could not get database size: {e}")