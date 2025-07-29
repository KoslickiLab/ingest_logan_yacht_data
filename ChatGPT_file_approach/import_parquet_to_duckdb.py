#!/usr/bin/env python
"""
Create or refresh a DuckDB database from the Parquet staging area produced by
export_to_parquet.py, then build all deferred ART indexes in one shot.
"""

import argparse, os, duckdb, logging, glob
from pathlib import Path

DEFAULT_DB = "logan.db"
DEFAULT_STAGE = "data_staging"


def ingest_table(conn, schema: str, table: str, path_glob: str):
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    full_glob = os.path.abspath(path_glob)
    logging.info(f"› {schema}.{table}  ←  {full_glob}")

    conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    conn.execute(f"""
        CREATE TABLE {schema}.{table} AS
        SELECT * FROM read_parquet('{full_glob}')
    """)


def build_indexes(conn):
    logging.info("⏳ Building indexes …")

    conn.executescript("""
        CREATE INDEX idx_taxa_profiles_sample_id
               ON taxa_profiles.profiles(sample_id);
        CREATE INDEX idx_taxa_profiles_organism_id
               ON taxa_profiles.profiles(organism_id);
        CREATE INDEX idx_taxa_profiles_organism_name
               ON taxa_profiles.profiles(organism_name);

        CREATE INDEX idx_functional_profile_sample_id
               ON functional_profile.profiles(sample_id);
        CREATE INDEX idx_functional_profile_ko_id
               ON functional_profile.profiles(ko_id);

        CREATE INDEX idx_sigs_aa_manifests_sample_id
               ON sigs_aa.manifests(sample_id);
        CREATE INDEX idx_sigs_aa_manifests_md5
               ON sigs_aa.manifests(md5);
        CREATE INDEX idx_sigs_aa_signatures_sample_id
               ON sigs_aa.signatures(sample_id);
        CREATE INDEX idx_sigs_aa_signatures_md5
               ON sigs_aa.signatures(md5);
        CREATE INDEX idx_sigs_aa_signature_mins_sample_id
               ON sigs_aa.signature_mins(sample_id);
        CREATE INDEX idx_sigs_aa_signature_mins_md5
               ON sigs_aa.signature_mins(md5);

        CREATE INDEX idx_sigs_dna_manifests_sample_id
               ON sigs_dna.manifests(sample_id);
        CREATE INDEX idx_sigs_dna_manifests_md5
               ON sigs_dna.manifests(md5);
        CREATE INDEX idx_sigs_dna_signatures_sample_id
               ON sigs_dna.signatures(sample_id);
        CREATE INDEX idx_sigs_dna_signatures_md5
               ON sigs_dna.signatures(md5);
        CREATE INDEX idx_sigs_dna_signature_mins_sample_id
               ON sigs_dna.signature_mins(sample_id);
        CREATE INDEX idx_sigs_dna_signature_mins_md5
               ON sigs_dna.signature_mins(md5);

        CREATE INDEX idx_gather_sample_id
               ON functional_profile_data.gather_data(sample_id);
        CREATE INDEX idx_geo_accession
               ON geographical_location_data.locations(accession);
        ANALYZE;
    """)


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
    conn.execute("PRAGMA memory_limit='2TB';")     # leave head‑room

    # ------------------------------------------------------------------
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

    build_indexes(conn)
    conn.close()
    logging.info("🎉  DuckDB import finished.")


if __name__ == "__main__":
    main()
