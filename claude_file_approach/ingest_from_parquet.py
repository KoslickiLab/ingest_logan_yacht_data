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
    # Increase memory limit for better performance
    conn.execute(f"SET memory_limit='{Config.DATABASE_MEMORY_LIMIT}'")
    
    # Use all available threads
    conn.execute(f"SET threads={Config.DATABASE_THREADS}")
    
    # Set temp directory
    conn.execute(f"SET temp_directory='{Config.DATABASE_TEMP_DIR}'")
    
    # Disable checkpoint on WAL for faster loading
    conn.execute("SET checkpoint_threshold='10GB'")
    
    # Increase buffer pool size
    conn.execute("SET buffer_pool_size='4GB'")

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
    
    # Use DuckDB's glob pattern to read all files at once
    # This is much more efficient than reading file by file
    pattern = str(table_path / "**/*.parquet")
    
    try:
        # First, check how many rows we're dealing with
        count_query = f"SELECT COUNT(*) FROM read_parquet('{pattern}')"
        row_count = conn.execute(count_query).fetchone()[0]
        logging.info(f"Total rows to ingest for {table_name}: {row_count:,}")
        
        # For very large tables, use INSERT in chunks
        if row_count > 10_000_000:  # 10 million rows
            logging.info(f"Large table detected, using chunked insertion...")
            
            # Process by partition if the data is partitioned
            partitions = [d for d in table_path.iterdir() if d.is_dir() and d.name.startswith("partition_key=")]
            
            if partitions:
                # Process each partition separately
                for partition_dir in tqdm(partitions, desc=f"Ingesting {table_name} partitions"):
                    partition_pattern = str(partition_dir / "*.parquet")
                    insert_query = f"""
                        INSERT INTO {target_table}
                        SELECT * EXCLUDE (partition_key)
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
                        SELECT * EXCLUDE (partition_key)
                        FROM read_parquet('{pattern}')
                        LIMIT {chunk_size} OFFSET {offset}
                    """
                    conn.execute(insert_query)
        else:
            # For smaller tables, insert all at once
            insert_query = f"""
                INSERT INTO {target_table}
                SELECT * EXCLUDE (partition_key)
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
                            # Load full file
                            query = f"""
                                INSERT INTO temp_taxonomy_mapping
                                SELECT "{genome_col}" as genome_id, "{taxid_col}" as taxid
                                FROM read_csv_auto('{file_path}', sep='{sep}')
                                WHERE "{genome_col}" IS NOT NULL AND "{taxid_col}" IS NOT NULL
                            """
                            conn.execute(query)
                            logging.info(f"Loaded taxonomy from {os.path.basename(file_path)}")
                            break
                    except:
                        continue
                        
            except Exception as e:
                logging.warning(f"Could not load taxonomy file {file_path}: {e}")
        
        # Update taxa_profiles with taxonomy IDs
        if conn.execute("SELECT COUNT(*) FROM temp_taxonomy_mapping").fetchone()[0] > 0:
            logging.info("Updating taxa_profiles with taxonomy IDs...")
            conn.execute("""
                UPDATE taxa_profiles.profiles
                SET tax_id = tm.taxid
                FROM temp_taxonomy_mapping tm
                WHERE taxa_profiles.profiles.organism_id = tm.genome_id
                  AND taxa_profiles.profiles.tax_id = -1
            """)
    
    # Process geographical location files
    logging.info("Processing geographical location files...")
    geo_files = []
    geo_files.extend(glob.glob(os.path.join(data_dir, "*geographical_location*.csv")))
    geo_files.extend(glob.glob(os.path.join(data_dir, "*geo_location*.csv")))
    geo_files.extend(glob.glob(os.path.join(data_dir, "*biosample_geographical*.csv")))
    geo_files.extend(glob.glob(os.path.join(data_dir, "output.csv")))
    
    for file_path in tqdm(geo_files, desc="Loading geographical files"):
        try:
            # Use DuckDB's CSV reader
            query = f"""
                INSERT INTO geographical_location_data.locations
                SELECT * FROM read_csv_auto('{file_path}', all_varchar=true)
            """
            conn.execute(query)
            logging.info(f"Loaded geographical data from {os.path.basename(file_path)}")
        except Exception as e:
            logging.warning(f"Could not load geographical file {file_path}: {e}")

def create_indexes(conn: duckdb.DuckDBPyConnection):
    """Create all indexes after data is loaded."""
    logging.info("Creating indexes...")
    
    for table, column in tqdm(INDEXES_TO_CREATE, desc="Creating indexes"):
        index_name = f"idx_{table.replace('.', '_')}_{column}"
        try:
            conn.execute(f"CREATE INDEX {index_name} ON {table} ({column})")
            logging.info(f"Created index {index_name}")
        except Exception as e:
            logging.warning(f"Could not create index {index_name}: {e}")

def vacuum_database(conn: duckdb.DuckDBPyConnection):
    """Vacuum and analyze the database for optimal performance."""
    logging.info("Running VACUUM ANALYZE...")
    conn.execute("VACUUM ANALYZE")
    logging.info("Database optimization complete")

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
    db_size = conn.execute("SELECT SUM(bytes) FROM duckdb_blocks()").fetchone()[0]
    if db_size:
        print(f"\n💾 Total database size: {db_size / (1024**3):.2f} GB")
    
    return stats

def parse_args():
    parser = argparse.ArgumentParser(
        description='Ingest Parquet files into DuckDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
    python ingest_from_parquet.py --parquet-dir ./parquet_output
    python ingest_from_parquet.py --database custom.db --no-indexes
    python ingest_from_parquet.py --tables functional_profile taxa_profiles""")
    
    parser.add_argument('--parquet-dir', default='./parquet_output',
                       help='Directory containing Parquet files (default: ./parquet_output)')
    parser.add_argument('--data-dir', default=Config.DATA_DIR,
                       help='Directory containing standalone CSV/TSV files')
    parser.add_argument('--database', default=Config.DATABASE_PATH,
                       help=f'Output database path (default: {Config.DATABASE_PATH})')
    parser.add_argument('--tables', nargs='+',
                       help='Specific tables to ingest (default: all)')
    parser.add_argument('--no-indexes', action='store_true',
                       help='Skip index creation')
    parser.add_argument('--no-vacuum', action='store_true',
                       help='Skip VACUUM ANALYZE')
    parser.add_argument('--append', action='store_true',
                       help='Append to existing database instead of recreating')
    
    return parser.parse_args()

def main():
    args = parse_args()
    setup_logger()
    
    print(f"📁 Parquet directory: {args.parquet_dir}")
    print(f"📁 Data directory: {args.data_dir}")
    print(f"🗄️  Database path: {args.database}")
    print(f"📊 Mode: {'Append' if args.append else 'Create new'}")
    
    # Check if parquet directory exists
    if not Path(args.parquet_dir).exists():
        print(f"❌ Parquet directory not found: {args.parquet_dir}")
        print("Please run extract_to_parquet.py first.")
        return
    
    # Create/connect to database
    if not args.append and Path(args.database).exists():
        backup_name = f"{args.database}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        shutil.copy2(args.database, backup_name)
        print(f"📦 Created backup: {backup_name}")
        os.remove(args.database)
    
    conn = duckdb.connect(args.database)
    optimize_duckdb_settings(conn)
    
    # Create schemas and tables
    create_schemas(conn)
    create_tables(conn)
    
    # Define table mappings
    table_mappings = [
        ('functional_profiles', 'functional_profile.profiles'),
        ('taxa_profiles', 'taxa_profiles.profiles'),
        ('gather_data', 'functional_profile_data.gather_data'),
        ('sigs_aa_manifests', 'sigs_aa.manifests'),
        ('sigs_aa_signatures', 'sigs_aa.signatures'),
        ('sigs_aa_signature_mins', 'sigs_aa.signature_mins'),
        ('sigs_dna_manifests', 'sigs_dna.manifests'),
        ('sigs_dna_signatures', 'sigs_dna.signatures'),
        ('sigs_dna_signature_mins', 'sigs_dna.signature_mins')
    ]
    
    # Filter tables if specified
    if args.tables:
        table_mappings = [(src, dst) for src, dst in table_mappings 
                         if any(t in src for t in args.tables)]
    
    # Ingest each table
    print("\n📥 Ingesting data...")
    for source_table, target_table in table_mappings:
        print(f"\n Processing {source_table} -> {target_table}")
        try:
            ingest_parquet_files(conn, args.parquet_dir, source_table, target_table)
        except Exception as e:
            logging.error(f"Failed to ingest {source_table}: {e}")
            if not args.append:
                raise
    
    # Ingest standalone files
    print("\n📥 Processing standalone files...")
    ingest_standalone_files(conn, args.data_dir)
    
    # Create indexes
    if not args.no_indexes:
        print("\n🔧 Creating indexes...")
        create_indexes(conn)
    
    # Vacuum database
    if not args.no_vacuum:
        print("\n🧹 Optimizing database...")
        vacuum_database(conn)
    
    # Display statistics
    get_statistics(conn)
    
    conn.close()
    
    print("\n✅ Ingestion complete!")
    print(f"📊 Database ready at: {args.database}")

if __name__ == "__main__":
    main()
    