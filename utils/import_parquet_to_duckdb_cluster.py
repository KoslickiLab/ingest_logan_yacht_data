untested
#!/usr/bin/env python
"""
import_parquet_to_duckdb.py
───────────────────────────
Bulk‑ingest Logan Parquet shards into a DuckDB database **with
scale‑aware physical layout & indexing**.

Additions over the previous version:

  •  Clusters sigs_{aa,dna}.signature_mins by sample_id
  •  Converts min_hash → UBIGINT
  •  Builds ART indexes on min_hash  and (sample_id,min_hash)
  •  Adds composite index (organism_id,cover_level) to taxa profiles
  •  Creates sigs_{aa,dna}.coverage  (hash‑space coverage summary)

All domain schemas stay the same; downstream queries remain valid.
"""

import argparse, os, duckdb, logging, sys
from pathlib import Path

DEFAULT_DB, DEFAULT_STAGE = "logan.db", "data_staging"


# ────────────────────────────────────────────────────────────────────
#  Generic helpers
# ────────────────────────────────────────────────────────────────────
def ingest_table(conn, schema: str, table: str, path_glob: str):
    """Create <schema>.<table> from a Parquet glob, casting ENUM → VARCHAR."""
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    g = os.path.abspath(path_glob)
    logging.info(f"› {schema}.{table}  ←  {g}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    conn.execute(f"CREATE TABLE {schema}.{table} AS SELECT * FROM read_parquet('{g}')")
    normalize_enum_columns(conn, schema, table)


def normalize_enum_columns(conn, schema: str, table: str):
    """DuckDB often imports dictionary‑encoded strings as ENUM – cast back."""
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


# ────────────────────────────────────────────────────────────────────
#  Taxonomy mapping
# ────────────────────────────────────────────────────────────────────
def apply_taxonomy_mapping(conn: duckdb.DuckDBPyConnection):
    present = conn.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema='taxonomy_mapping' AND table_name='mappings'
    """).fetchone()[0]
    if not present:
        logging.warning("No taxonomy_mapping.mappings table – skipping tax_id update")
        return

    # taxid → BIGINT; drop weird tokens
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
    # taxa_profiles.profiles.tax_id → BIGINT
    conn.execute("""
        ALTER TABLE taxa_profiles.profiles
        ALTER COLUMN tax_id SET DATA TYPE BIGINT
    """)
    logging.info("🔄  Updating taxa_profiles.profiles.tax_id …")
    conn.execute("""
        UPDATE taxa_profiles.profiles AS tp
        SET    tax_id = tm.taxid
        FROM   taxonomy_mapping.mappings AS tm
        WHERE  tp.organism_id = tm.genome_id
    """)
    mapped = conn.execute("""
        SELECT COUNT(*) FROM taxa_profiles.profiles WHERE tax_id != -1
    """).fetchone()[0]
    logging.info(f"✅  tax_id now populated for {mapped:,d} rows")
    conn.execute("DROP TABLE taxonomy_mapping.mappings")


# ────────────────────────────────────────────────────────────────────
#  Signature‑mins clustering & coverage summary
# ────────────────────────────────────────────────────────────────────
def cluster_and_summarise(conn: duckdb.DuckDBPyConnection, sigs: str):
    """
    Physically cluster <sigs>.signature_mins by sample_id, ensure min_hash is
    UBIGINT, and compute hash‑space coverage per sample.
    """
    logging.info(f"🚜  Clustering {sigs}.signature_mins by sample_id …")
    conn.execute(f"""
        ALTER TABLE {sigs}.signature_mins
        ALTER COLUMN min_hash SET DATA TYPE UBIGINT
    """)
    conn.execute(f"""
        CREATE TABLE {sigs}.signature_mins_clustered AS
        SELECT * FROM {sigs}.signature_mins
        ORDER BY sample_id
    """)
    conn.execute(f"DROP TABLE {sigs}.signature_mins")
    conn.execute(f"ALTER TABLE {sigs}.signature_mins_clustered RENAME TO signature_mins")

    logging.info(f"📊  Building {sigs}.coverage summary …")
    conn.execute(f"DROP TABLE IF EXISTS {sigs}.coverage")
    conn.execute(f"""
        CREATE TABLE {sigs}.coverage AS
        SELECT sample_id,
               COUNT(DISTINCT min_hash) AS covered_min_hashes,
               COUNT(DISTINCT min_hash) / 18446744073709551616.0 AS frac_64bit_space
        FROM   {sigs}.signature_mins
        GROUP  BY sample_id
    """)


# ────────────────────────────────────────────────────────────────────
#  Index builder
# ────────────────────────────────────────────────────────────────────
def build_indexes(conn: duckdb.DuckDBPyConnection):
    logging.info("⏳  Building indexes …")

    statements = [
        # taxa profiles
        """CREATE INDEX idx_taxa_profiles_sample_id  ON taxa_profiles.profiles(sample_id)""",
        """CREATE INDEX idx_taxa_profiles_org_id     ON taxa_profiles.profiles(organism_id)""",
        """CREATE INDEX idx_taxa_profiles_org_name   ON taxa_profiles.profiles(organism_name)""",
        """CREATE INDEX idx_taxa_profiles_tax_id     ON taxa_profiles.profiles(tax_id)""",
        """CREATE INDEX idx_taxa_profiles_org_covlvl ON taxa_profiles.profiles(organism_id, cover_level)""",

        # functional profile
        """CREATE INDEX idx_func_profile_sample_id   ON functional_profile.profiles(sample_id)""",
        """CREATE INDEX idx_func_profile_ko_id       ON functional_profile.profiles(ko_id)""",

        # AA signatures
        """CREATE INDEX idx_sigs_aa_manifest_sample  ON sigs_aa.manifests(sample_id)""",
        """CREATE INDEX idx_sigs_aa_mf_md5           ON sigs_aa.manifests(md5)""",
        """CREATE INDEX idx_sigs_aa_sign_sample      ON sigs_aa.signatures(sample_id)""",
        """CREATE INDEX idx_sigs_aa_sign_md5         ON sigs_aa.signatures(md5)""",
        """CREATE INDEX idx_sigs_aa_mins_sample      ON sigs_aa.signature_mins(sample_id)""",
        """CREATE INDEX idx_sigs_aa_mins_md5         ON sigs_aa.signature_mins(md5)""",
        """CREATE INDEX idx_sigs_aa_mins_minhash     ON sigs_aa.signature_mins(min_hash)""",
        """CREATE INDEX idx_sigs_aa_mins_sample_hash ON sigs_aa.signature_mins(sample_id, min_hash)""",

        # DNA signatures
        """CREATE INDEX idx_sigs_dna_manifest_sample ON sigs_dna.manifests(sample_id)""",
        """CREATE INDEX idx_sigs_dna_mf_md5          ON sigs_dna.manifests(md5)""",
        """CREATE INDEX idx_sigs_dna_sign_sample     ON sigs_dna.signatures(sample_id)""",
        """CREATE INDEX idx_sigs_dna_sign_md5        ON sigs_dna.signatures(md5)""",
        """CREATE INDEX idx_sigs_dna_mins_sample     ON sigs_dna.signature_mins(sample_id)""",
        """CREATE INDEX idx_sigs_dna_mins_md5        ON sigs_dna.signature_mins(md5)""",
        """CREATE INDEX idx_sigs_dna_mins_minhash    ON sigs_dna.signature_mins(min_hash)""",
        """CREATE INDEX idx_sigs_dna_mins_sample_hash ON sigs_dna.signature_mins(sample_id, min_hash)""",

        # gather & geo
        """CREATE INDEX idx_gather_sample            ON functional_profile_data.gather_data(sample_id)""",
        """CREATE INDEX idx_geo_accession            ON geographical_location_data.locations(accession)"""
    ]

    for stmt in statements:
        try:
            conn.execute(stmt)
        except Exception as e:
            logging.debug(f"  (index skipped) {e}")

    conn.execute("ANALYZE")   # collect stats


# ────────────────────────────────────────────────────────────────────
#  Main
# ────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--staging-dir", default=DEFAULT_STAGE,
                    help="Directory produced by export_to_parquet.py")
    ap.add_argument("--database", default=DEFAULT_DB,
                    help="Output *.db file (overwritten if exists)")
    ap.add_argument("--threads", type=int, default=os.cpu_count(),
                    help="DuckDB parallelism")
    ap.add_argument("--mem", default="3TB",
                    help="DuckDB memory_limit (e.g. 500GB, 90%)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)s  %(message)s")

    stage = Path(args.staging_dir).resolve()
    if not stage.exists():
        sys.exit(f"Staging directory {stage} not found.")

    conn = duckdb.connect(args.database, read_only=False)
    conn.execute(f"PRAGMA threads={args.threads}")
    conn.execute(f"PRAGMA memory_limit='{args.mem}';")

    # ── bulk ingest ────────────────────────────────────────────────
    ingest_table(conn, "functional_profile",      "profiles",
                 f"{stage}/functional_profile/profiles/*.parquet")
    ingest_table(conn, "functional_profile_data", "gather_data",
                 f"{stage}/functional_profile_data/gather_data/*.parquet")
    ingest_table(conn, "taxa_profiles",           "profiles",
                 f"{stage}/taxa_profiles/profiles/*.parquet")

    for sigs in ("sigs_aa", "sigs_dna"):
        ingest_table(conn, sigs, "manifests",
                     f"{stage}/{sigs}/manifests/*.parquet")
        ingest_table(conn, sigs, "signatures",
                     f"{stage}/{sigs}/signatures/*.parquet")
        ingest_table(conn, sigs, "signature_mins",
                     f"{stage}/{sigs}/signature_mins/*.parquet")
        cluster_and_summarise(conn, sigs)   # <── NEW

    ingest_table(conn, "geographical_location_data", "locations",
                 f"{stage}/geographical_location_data/locations/*.parquet")

    mapping_dir = stage / "taxonomy_mapping" / "mappings"
    if mapping_dir.exists():
        ingest_table(conn, "taxonomy_mapping", "mappings",
                     f"{mapping_dir}/*.parquet")
        apply_taxonomy_mapping(conn)

    # ── indexes & stats ────────────────────────────────────────────
    build_indexes(conn)
    conn.close()
    logging.info("🎉  DuckDB import finished.")


if __name__ == "__main__":
    main()
