#!/usr/bin/env python3
"""
Attach 'date received' metadata to an existing DuckDB database
— with indexes created *after* the bulk insert (recommended).

Usage
-----
python add_sra_received_dates.py \
       --db  /path/to/my_database.duckdb \
       --tsv /path/to/Accessions_to_date_received.tsv
"""
from __future__ import annotations
import argparse, pathlib, sys, duckdb

# --------------------------------------------------------------------------- #
def ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the metadata table *without* indexes or constraints."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sample_received (
            sample_id     VARCHAR,          
            date_received TIMESTAMPTZ
        );
    """)

def load_tsv_view(conn: duckdb.DuckDBPyConnection, tsv: pathlib.Path) -> None:
    """Expose the TSV as a temp view."""
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW tsv_raw AS
        SELECT  Accession AS accession,
                CAST(Received AS TIMESTAMPTZ) AS received
        FROM    read_csv_auto('{tsv}', delim='\t', header=true);
    """)

def bulk_insert(conn: duckdb.DuckDBPyConnection) -> None:
    """Insert only those accessions referenced by existing tables."""
    conn.execute("""
        INSERT INTO sample_received
        SELECT  accession      AS sample_id,
                received       AS date_received
        FROM    tsv_raw t
        WHERE   EXISTS (
            SELECT 1
            FROM   sigs_dna.signature_mins sm
            WHERE  sm.sample_id = t.accession
        );
    """)

def build_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the ART indexes in bulk, after the data is present."""
    conn.execute("""
        -- unique index gives you fast equality joins and enforces uniqueness
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sample_received_id
            ON sample_received(sample_id);

        -- date index accelerates range predicates
        CREATE INDEX IF NOT EXISTS idx_sample_received_date
            ON sample_received(date_received);
    """)

# --------------------------------------------------------------------------- #
def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Add SRA 'date_received' metadata")
    ap.add_argument("--db",  required=True, type=pathlib.Path)
    ap.add_argument("--tsv", required=True, type=pathlib.Path)
    args = ap.parse_args(argv)

    if not args.tsv.is_file(): sys.exit(f"TSV file not found: {args.tsv}")
    if not args.db.is_file():  sys.exit(f"DuckDB file not found: {args.db}")

    with duckdb.connect(str(args.db)) as conn:
        conn.execute("BEGIN;")           # single fast transaction
        ensure_table(conn)
        load_tsv_view(conn, args.tsv)
        bulk_insert(conn)
        build_indexes(conn)
        conn.execute("ANALYZE sample_received;")  # fresh stats
        conn.execute("COMMIT;")

    print("✅  sample_received populated and indexed.")

if __name__ == "__main__":
    main()
