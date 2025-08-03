#!/usr/bin/env python3
"""
export_diversity_metrics.py  –  Extract multiple diversity metrics from a DuckDB
                                SRA‑metagenome database.

Creates a tidy CSV with the following columns per calendar year:

year
n_samples                       -- # distinct metagenome accessions received
new_hashes                      -- hashes first observed in that year
cumulative_samples
cumulative_hashes
hashes_per_sample               -- new_hashes / n_samples
cumulative_hashes_per_sample    -- cumulative_hashes / cumulative_samples

Example
-------
python export_diversity_metrics.py \
       --db  /data/sra_metagenomes.duckdb \
       --out diversity_metrics.csv \
       --threads 64 --mem 512GB
"""
from __future__ import annotations
import argparse, pathlib, sys, duckdb, textwrap, math

HASH_BITS = 64
SCALE     = 1_000
HASH_SPACE = math.ceil(2 ** HASH_BITS / SCALE)    # used by the plotting script

# ──────────────────────────────────────────────────────────────────────────────
RAW_SQL = textwrap.dedent(r"""
    --------------------------------------------------------------------------------
    -- 1. For every min_hash find the first year it appears
    --------------------------------------------------------------------------------
    WITH first_seen AS (
        SELECT
            sm.min_hash,
            MIN(EXTRACT(year FROM sr.date_received)) AS first_year
        FROM sigs_dna.signature_mins AS sm
        JOIN sample_received          AS sr
          ON sr.sample_id = sm.sample_id
        GROUP BY sm.min_hash
    ),

    --------------------------------------------------------------------------------
    -- 2. Hash novelty per year
    --------------------------------------------------------------------------------
    new_hashes AS (
        SELECT first_year    AS year,
               COUNT(*)      AS new_hashes
        FROM   first_seen
        GROUP  BY first_year
    ),

    --------------------------------------------------------------------------------
    -- 3. Sample counts per year
    --------------------------------------------------------------------------------
    samples AS (
        SELECT EXTRACT(year FROM date_received) AS year,
               COUNT(DISTINCT sample_id)        AS n_samples
        FROM   sample_received
        GROUP  BY year
    ),

    --------------------------------------------------------------------------------
    -- 4. Dense sequence of calendar years covering the entire range
    --------------------------------------------------------------------------------
    yr AS (
        SELECT *
        FROM range(
            (SELECT MIN(year) FROM samples),          -- inclusive start
            (SELECT MAX(year) FROM samples) + 1       -- exclusive stop
        ) AS y(year)
    ),

    --------------------------------------------------------------------------------
    -- 5. Join & replace NULLs with 0, then accumulate with window functions
    --------------------------------------------------------------------------------
    joined AS (
        SELECT
            y.year,
            COALESCE(s.n_samples,  0)::BIGINT AS n_samples,
            COALESCE(n.new_hashes, 0)::BIGINT AS new_hashes
        FROM yr y
        LEFT JOIN samples    s ON s.year = y.year
        LEFT JOIN new_hashes n ON n.year = y.year
    )

    SELECT
        year,
        n_samples,
        new_hashes,
        SUM(n_samples)  OVER (ORDER BY year) AS cumulative_samples,
        SUM(new_hashes) OVER (ORDER BY year) AS cumulative_hashes,
        CASE WHEN n_samples            > 0 THEN new_hashes            ::DOUBLE / n_samples            ELSE 0 END AS hashes_per_sample,
        CASE WHEN SUM(n_samples)  OVER (ORDER BY year) > 0
             THEN SUM(new_hashes) OVER (ORDER BY year)::DOUBLE
                  / SUM(n_samples) OVER (ORDER BY year)
             ELSE 0 END AS cumulative_hashes_per_sample
    FROM joined
    ORDER BY year;
""").strip()   # ← NO trailing semicolon

# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    argp = argparse.ArgumentParser(description="Export SRA diversity metrics.")
    argp.add_argument("--db",      required=True, type=pathlib.Path)
    argp.add_argument("--out",     required=True, type=pathlib.Path)
    argp.add_argument("--threads", type=int, help="PRAGMA threads=N")
    argp.add_argument("--mem",     type=str, help="PRAGMA memory_limit='...'")
    args = argp.parse_args()

    if not args.db.is_file():
        sys.exit(f"Database not found → {args.db}")

    out_path = args.out.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(args.db), read_only=True) as conn:
        if args.threads:
            conn.execute(f"PRAGMA threads={args.threads};")
        if args.mem:
            conn.execute(f"PRAGMA memory_limit='{args.mem}';")

        conn.execute("PRAGMA enable_progress_bar;")

        # Escape path for SQL literal
        out_literal = str(out_path).replace("'", "''")

        conn.execute(f"""
            COPY ({RAW_SQL})
            TO '{out_literal}'
            (HEADER, DELIMITER ',');
        """)

    print(f"✅  Diversity metrics exported → {out_path}")

if __name__ == "__main__":
    main()
