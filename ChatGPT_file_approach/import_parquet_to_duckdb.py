#!/usr/bin/env python
"""
import_parquet_to_duckdb.py  –  final version with taxonomy mapping

1.  Creates/refreshes a DuckDB database from Parquet shards
2.  Applies GTDB taxonomy → tax_id mapping
3.  Builds ART indexes (now including tax_id)

Requires DuckDB ≥1.0, PyArrow ≥15.
"""

import argparse, os, duckdb, logging
from pathlib import Path

DEFAULT_DB    = "logan.db"
DEFAULT_STAGE = "data_staging"


# ────────────────────────────────────────────────────────────────────
#  Helpers
# ────────────────────────────────────────────────────────────────────
def ingest_table(conn, schema: str, table: str, path_glob: str):
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    g = os.path.abspath(path_glob)
    logging.info(f"› {schema}.{table}  ←  {g}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    conn.execute(f"CREATE TABLE {schema}.{table} AS SELECT * FROM read_parquet('{g}')")


def apply_taxonomy_mapping(conn: duckdb.DuckDBPyConnection):
    """
    Replace the placeholder -1 in taxa_profiles.profiles.tax_id
    with real NCBI/GTDB taxids from taxonomy_mapping.mappings.
    """
    # Does the mapping table exist?
    if not conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema='taxonomy_mapping' AND table_name='mappings'
        """).fetchone()[0]:
        logging.warning("No taxonomy_mapping.mappings table – skipping tax_id update")
        return

    # Deduplicate mapping so each genome_id appears once
    logging.info("🧬  Deduplicating taxonomy mapping …")
    conn.execute("""
        CREATE OR REPLACE TABLE taxonomy_mapping.mappings AS
        SELECT genome_id, first(taxid) AS taxid
        FROM taxonomy_mapping.mappings
        GROUP BY genome_id
    """)

    # How many will be updated?
    total_before = conn.execute("""
        SELECT COUNT(*) FROM taxa_profiles.profiles
        WHERE tax_id != -1
    """).fetchone()[0]

    logging.info("🔄  Updating taxa_profiles.profiles.tax_id …")
    conn.execute("""
        UPDATE taxa_profiles.profiles AS tp
        SET    tax_id = tm.taxid
        FROM   taxonomy_mapping.mappings AS tm
        WHERE  tp.organism_id = tm.genome_id
    """)

    total_after = conn.execute("""
        SELECT COUNT(*) FROM taxa_profiles.profiles
        WHERE tax_id != -1
    """).fetchone()[0]

    logging.info(f"✅  tax_id mapped for {total_after - total_before:,d} additional records "
                 f"({total_after:,d} total mapped)")


def build_indexes(conn: duckdb.DuckDBPyConnection):
    logging.info("⏳ Building indexes …")
    statements = """
        CREATE INDEX idx_taxa_profiles_sample_id      ON taxa_profiles.profiles(sample_id);
        CREATE INDEX idx_taxa_profiles_organism_id    ON taxa_profiles.profiles(organism_id);
        CREATE INDEX idx_taxa_profiles_organism_name  ON taxa_profiles.profiles(organism_name);
        CREATE INDEX idx_taxa_profiles_tax_id         ON taxa_profiles.profiles(tax_id);

        CREATE INDEX idx_functional_profile_sample_id ON functional_profile.profiles(sample_id);
        CREATE INDEX idx_functional_profile_ko_id     ON functional_profile.profiles(ko_id);

        CREATE INDEX idx_sigs_aa_manifests_sample_id  ON sigs_aa.manifests(sample_id);
        CREATE INDEX idx_sigs_aa_manifests_md5        ON sigs_aa.manifests(md5);
        CREATE INDEX idx_sigs_aa_signatures_sample_id ON sigs_aa.signatures(sample_id);
        CREATE INDEX idx_sigs_aa_signatures_md5       ON sigs_aa.signatures(md5);
        CREATE INDEX idx_sigs_aa_signature_mins_sample_id ON sigs_aa.signature_mins(sample_id);
        CREATE INDEX idx_sigs_aa_signature_mins_md5   ON sigs_aa.signature_mins(md5);

        CREATE INDEX idx_sigs_dna_manifests_sample_id ON sigs_dna.manifests(sample_id);
        CREATE INDEX idx_sigs_dna_manifests_md5       ON sigs_dna.manifests(md5);
        CREATE INDEX idx_sigs_dna_signatures_sample_id ON sigs_dna.signatures(sample_id);
        CREATE INDEX idx_sigs_dna_signatures_md5      ON sigs_dna.signatures(md5);
        CREATE INDEX idx_sigs_dna_signature_mins_sample_id ON sigs_dna.signature_mins(sample_id);
        CREATE INDEX idx_sigs_dna_signature_mins_md5  ON sigs_dna.signature_mins(md5);

        CREATE INDEX idx_gather_sample_id             ON functional_profile_data.gather_data(sample_id);
        CREATE INDEX idx_geo_accession                ON geographical_location_data.locations(accession);
        ANALYZE;
    """
    for stmt in (s.strip() for s in statements.split(";")):
        if stmt:
            conn.execute(stmt)


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
