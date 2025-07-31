#!/usr/bin/env python
"""
import_parquet_to_duckdb.py  –  with ENUM→VARCHAR normalisation

1.  Bulk‑loads all Parquet shards into DuckDB
2.  Casts dictionary‑encoded ENUM columns back to VARCHAR
3.  Applies GTDB tax_id mapping
4.  Builds ART indexes
"""

import argparse, os, duckdb, logging
from pathlib import Path

DEFAULT_DB, DEFAULT_STAGE = "logan.db", "data_staging"


# ────────────────────────────────────────────────────────────────────
#  Helpers
# ────────────────────────────────────────────────────────────────────
def ingest_table(conn, schema: str, table: str, path_glob: str):
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    g = os.path.abspath(path_glob)
    logging.info(f"› {schema}.{table}  ←  {g}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    conn.execute(f"CREATE TABLE {schema}.{table} AS SELECT * FROM read_parquet('{g}')")
    normalize_enum_columns(conn, schema, table)
    handle_null_columns(conn, schema, table)


def normalize_enum_columns(conn, schema: str, table: str):
    """
    DuckDB maps dictionary‑encoded strings to ENUM.  Cast back to VARCHAR
    so ART index creation is always safe (work‑around for up‑to‑1.3 bug).
    """
    enum_cols = conn.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name   = '{table}'
          AND data_type    LIKE 'ENUM%'
    """).fetchall()
    for (col,) in enum_cols:
        logging.debug(f"  • Casting {schema}.{table}.{col} ENUM→VARCHAR")
        conn.execute(f"""
            ALTER TABLE {schema}.{table}
            ALTER COLUMN "{col}" SET DATA TYPE VARCHAR
        """)


def handle_null_columns(conn, schema: str, table: str):
    """
    Replace NULL values in columns that contain only NULLs with a placeholder.
    This maintains data transparency while preventing index creation issues.
    """
    # Get all columns and their data types
    columns = conn.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """).fetchall()

    null_only_columns = []

    for col_name, data_type in columns:
        # Skip tax_id column as it needs special handling
        if col_name == 'tax_id':
            continue

        # Check if column has only NULL values
        non_null_count = conn.execute(f"""
            SELECT COUNT(*) 
            FROM {schema}.{table} 
            WHERE "{col_name}" IS NOT NULL
        """).fetchone()[0]

        total_count = conn.execute(f"""
            SELECT COUNT(*) 
            FROM {schema}.{table}
        """).fetchone()[0]

        if non_null_count == 0 and total_count > 0:
            null_only_columns.append((col_name, data_type))

    # Handle NULL-only columns
    for col_name, data_type in null_only_columns:
        logging.warning(f"  ⚠️  Column {schema}.{table}.{col_name} contains only NULL values")

        try:
            # First convert to VARCHAR if needed to ensure we can store string values
            if not any(x in data_type.upper() for x in ['VARCHAR', 'TEXT', 'STRING']):
                logging.info(f"  • Converting {col_name} from {data_type} to VARCHAR")
                conn.execute(f"""
                    ALTER TABLE {schema}.{table} 
                    ALTER COLUMN "{col_name}" SET DATA TYPE VARCHAR
                """)

            # Replace NULLs with placeholder
            logging.info(f"  • Replacing NULL values in {col_name} with 'NULL_PLACEHOLDER'")
            conn.execute(f"""
                UPDATE {schema}.{table} 
                SET "{col_name}" = 'NULL_PLACEHOLDER' 
                WHERE "{col_name}" IS NULL
            """)

        except Exception as e:
            logging.error(f"  ❌ Failed to handle NULL column {col_name}: {e}")


def apply_taxonomy_mapping(conn: duckdb.DuckDBPyConnection):
    if not conn.execute("""
                        SELECT COUNT(*)
                        FROM information_schema.tables
                        WHERE table_schema = 'taxonomy_mapping'
                          AND table_name = 'mappings'
                        """).fetchone()[0]:
        logging.warning("No taxonomy_mapping.mappings table – skipping tax_id update")
        return

    # logging.info("🧬  Deduplicating taxonomy mapping …")
    # conn.execute("""
    #    CREATE OR REPLACE TABLE taxonomy_mapping.mappings AS
    #    SELECT genome_id, first(taxid) AS taxid
    #    FROM taxonomy_mapping.mappings
    #    GROUP BY genome_id
    # """)
    # ── 1.  Normalise TYPES  ────────────────────────────────────────────
    # taxid → BIGINT; any non‑numeric token becomes NULL
    conn.execute("""
                 ALTER TABLE taxonomy_mapping.mappings
                     ALTER COLUMN taxid SET DATA TYPE BIGINT USING try_cast(taxid AS BIGINT)
                 """)

    logging.info("🧬  Deduplicating taxonomy mapping …")
    conn.execute("""
        CREATE OR REPLACE TABLE taxonomy_mapping.mappings AS
        SELECT genome_id,
               first(taxid) FILTER (WHERE taxid IS NOT NULL) AS taxid
        FROM   taxonomy_mapping.mappings
        GROUP  BY genome_id
        HAVING taxid IS NOT NULL
    """)

    # Ensure the target column is BIGINT too
    conn.execute("""
                 ALTER TABLE taxa_profiles.profiles
                     ALTER COLUMN tax_id SET DATA TYPE BIGINT
                 """)

    logging.info("🔄  Updating taxa_profiles.profiles.tax_id …")
    conn.execute("""
                 UPDATE taxa_profiles.profiles AS tp
                 SET tax_id = tm.taxid FROM   taxonomy_mapping.mappings AS tm
                 WHERE tp.organism_id = tm.genome_id
                 """)

    mapped = conn.execute("""
                          SELECT COUNT(*)
                          FROM taxa_profiles.profiles
                          WHERE tax_id != -1
                          """).fetchone()[0]
    logging.info(f"✅  tax_id now populated for {mapped:,d} records")
    # drop mapping table to avoid ENUM columns later ──
    conn.execute("DROP TABLE taxonomy_mapping.mappings")


def verify_column_stats(conn, schema: str, table: str, column: str) -> dict:
    """
    Get detailed statistics about a column to help diagnose issues.
    """
    try:
        stats = {}

        # Basic counts
        total_count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
        non_null_count = conn.execute(f"""
            SELECT COUNT(*) FROM {schema}.{table} WHERE "{column}" IS NOT NULL
        """).fetchone()[0]
        distinct_count = conn.execute(f"""
            SELECT COUNT(DISTINCT "{column}") FROM {schema}.{table}
        """).fetchone()[0]

        stats['total_rows'] = total_count
        stats['non_null_count'] = non_null_count
        stats['null_count'] = total_count - non_null_count
        stats['distinct_values'] = distinct_count
        stats['null_percentage'] = (stats['null_count'] / total_count * 100) if total_count > 0 else 0

        # Check for placeholder values (only for string columns)
        data_type = conn.execute(f"""
            SELECT data_type FROM information_schema.columns 
            WHERE table_schema = '{schema}' AND table_name = '{table}' AND column_name = '{column}'
        """).fetchone()[0]

        if any(x in data_type.upper() for x in ['VARCHAR', 'TEXT', 'STRING']):
            placeholder_count = conn.execute(f"""
                SELECT COUNT(*) FROM {schema}.{table} 
                WHERE "{column}" = 'NULL_PLACEHOLDER'
            """).fetchone()[0]
            stats['placeholder_count'] = placeholder_count
        else:
            stats['placeholder_count'] = 0

        return stats

    except Exception as e:
        logging.error(f"  Error getting stats for {schema}.{table}.{column}: {e}")
        return {}


def build_indexes(conn: duckdb.DuckDBPyConnection):
    logging.info("⏳ Building indexes …")

    # Force checkpoint and analyze before index creation
    try:
        conn.execute("CHECKPOINT")
        conn.execute("ANALYZE")
    except Exception as e:
        logging.warning(f"Checkpoint/analyze warning: {e}")

    # Index definitions
    indexes = [
        ("idx_taxa_profiles_sample_id", "taxa_profiles.profiles", "sample_id"),
        ("idx_taxa_profiles_organism_id", "taxa_profiles.profiles", "organism_id"),
        ("idx_taxa_profiles_organism_name", "taxa_profiles.profiles", "organism_name"),
        ("idx_taxa_profiles_tax_id", "taxa_profiles.profiles", "tax_id"),

        ("idx_functional_profile_sample_id", "functional_profile.profiles", "sample_id"),
        ("idx_functional_profile_ko_id", "functional_profile.profiles", "ko_id"),

        ("idx_sigs_aa_manifests_sample_id", "sigs_aa.manifests", "sample_id"),
        ("idx_sigs_aa_manifests_md5", "sigs_aa.manifests", "md5"),
        ("idx_sigs_aa_signatures_sample_id", "sigs_aa.signatures", "sample_id"),
        ("idx_sigs_aa_signatures_md5", "sigs_aa.signatures", "md5"),
        ("idx_sigs_aa_signature_mins_sample_id", "sigs_aa.signature_mins", "sample_id"),
        ("idx_sigs_aa_signature_mins_md5", "sigs_aa.signature_mins", "md5"),

        ("idx_sigs_dna_manifests_sample_id", "sigs_dna.manifests", "sample_id"),
        ("idx_sigs_dna_manifests_md5", "sigs_dna.manifests", "md5"),
        ("idx_sigs_dna_signatures_sample_id", "sigs_dna.signatures", "sample_id"),
        ("idx_sigs_dna_signatures_md5", "sigs_dna.signatures", "md5"),
        ("idx_sigs_dna_signature_mins_sample_id", "sigs_dna.signature_mins", "sample_id"),
        ("idx_sigs_dna_signature_mins_md5", "sigs_dna.signature_mins", "md5"),

        ("idx_gather_sample_id", "functional_profile_data.gather_data", "sample_id"),
        ("idx_geo_accession", "geographical_location_data.locations", "accession"),
    ]

    successful_indexes = []
    failed_indexes = []

    for idx_name, table_full, column in indexes:
        schema, table = table_full.split('.')

        # Get column statistics for logging
        stats = verify_column_stats(conn, schema, table, column)
        if stats:
            logging.info(f"  Column {column} stats: {stats['non_null_count']:,d} non-null, "
                         f"{stats['null_count']:,d} null ({stats['null_percentage']:.1f}%), "
                         f"{stats['placeholder_count']:,d} placeholders")

        try:
            # Drop existing index if any
            conn.execute(f"DROP INDEX IF EXISTS {idx_name}")

            # Create index (let DuckDB choose the type)
            logging.info(f"  Creating index {idx_name} on {table_full}({column})")
            conn.execute(f"CREATE INDEX {idx_name} ON {table_full}(\"{column}\")")
            successful_indexes.append(idx_name)

        except duckdb.InternalException as e:
            if "node without metadata" in str(e):
                logging.error(f"  ❌ ART index error for {idx_name}: {e}")
                logging.info(f"  🔄 Attempting to recreate without specific index type")
                try:
                    conn.execute(f"DROP INDEX IF EXISTS {idx_name}")
                    conn.execute(f"CREATE INDEX {idx_name} ON {table_full}(\"{column}\")")
                    logging.info(f"  ✅ Index created successfully for {idx_name}")
                    successful_indexes.append(f"{idx_name} (recreated)")
                except Exception as fallback_error:
                    logging.error(f"  ❌ Index recreation also failed for {idx_name}: {fallback_error}")
                    failed_indexes.append(idx_name)
            else:
                logging.error(f"  ❌ Index creation failed for {idx_name}: {e}")
                failed_indexes.append(idx_name)
        except Exception as e:
            logging.error(f"  ❌ Unexpected error creating index {idx_name}: {e}")
            failed_indexes.append(idx_name)

    # Final analyze
    try:
        conn.execute("ANALYZE")
    except Exception as e:
        logging.warning(f"Final analyze warning: {e}")

    # Report NULL placeholder usage
    logging.info("\n📊 NULL placeholder usage report:")
    tables_to_check = [
        ("taxa_profiles", "profiles"),
        ("functional_profile", "profiles"),
        ("functional_profile_data", "gather_data"),
        ("sigs_aa", "manifests"),
        ("sigs_aa", "signatures"),
        ("sigs_aa", "signature_mins"),
        ("sigs_dna", "manifests"),
        ("sigs_dna", "signatures"),
        ("sigs_dna", "signature_mins"),
        ("geographical_location_data", "locations")
    ]

    for schema, table in tables_to_check:
        try:
            # Get all columns for this table
            columns = conn.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' AND table_name = '{table}'
            """).fetchall()

            placeholder_cols = []
            for (col_name,) in columns:
                # Check if this column has any NULL_PLACEHOLDER values
                count = conn.execute(f"""
                    SELECT COUNT(*) 
                    FROM {schema}.{table} 
                    WHERE "{col_name}" = 'NULL_PLACEHOLDER'
                """).fetchone()[0]

                if count > 0:
                    placeholder_cols.append(col_name)

            if placeholder_cols:
                logging.warning(f"  {schema}.{table} has NULL placeholders in: {placeholder_cols}")
        except Exception as e:
            logging.debug(f"  Could not check {schema}.{table} for placeholders: {e}")

    logging.info(f"\n📊 Index creation summary:")
    logging.info(f"  ✅ Successful: {len(successful_indexes)} indexes")
    if failed_indexes:
        logging.warning(f"  ❌ Failed: {len(failed_indexes)} indexes: {', '.join(failed_indexes)}")

    if len(successful_indexes) > 0:
        logging.info("The database is ready for use!")


# ────────────────────────────────────────────────────────────────────
#  Main
# ────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--staging-dir", default=DEFAULT_STAGE,
                    help="Root produced by export_to_parquet.py")
    ap.add_argument("--database", default=DEFAULT_DB,
                    help="Output *.db file (overwritten if exists)")
    ap.add_argument("--threads", type=int, default=os.cpu_count(),
                    help="DuckDB parallelism")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)s  %(message)s")

    stage = Path(args.staging_dir).resolve()
    if not stage.exists():
        raise SystemExit(f"Staging directory {stage} missing.")

    conn = duckdb.connect(args.database)
    conn.execute(f"PRAGMA threads={args.threads}")
    conn.execute("PRAGMA memory_limit='3 TB';")

    # ── bulk ingest ────────────────────────────────────────────────
    ingest_table(conn, "functional_profile", "profiles",
                 f"{stage}/functional_profile/profiles/*.parquet")
    ingest_table(conn, "functional_profile_data", "gather_data",
                 f"{stage}/functional_profile_data/gather_data/*.parquet")
    ingest_table(conn, "taxa_profiles", "profiles",
                 f"{stage}/taxa_profiles/profiles/*.parquet")

    for sigs in ("sigs_aa", "sigs_dna"):
        ingest_table(conn, sigs, "manifests",
                     f"{stage}/{sigs}/manifests/*.parquet")
        ingest_table(conn, sigs, "signatures",
                     f"{stage}/{sigs}/signatures/*.parquet")
        ingest_table(conn, sigs, "signature_mins",
                     f"{stage}/{sigs}/signature_mins/*.parquet")

    ingest_table(conn, "geographical_location_data", "locations",
                 f"{stage}/geographical_location_data/locations/*.parquet")

    mapping_dir = stage / "taxonomy_mapping" / "mappings"
    if mapping_dir.exists():
        ingest_table(conn, "taxonomy_mapping", "mappings",
                     f"{mapping_dir}/*.parquet")
        apply_taxonomy_mapping(conn)

    # ── indexes ────────────────────────────────────────────────────
    build_indexes(conn)
    conn.close()
    logging.info("🎉  DuckDB import finished.")


if __name__ == "__main__":
    main()