#!/usr/bin/env python3
"""
export_rarefaction_fast.py  –  memory‑efficient rarefaction export with progress bar.

Example
-------
python export_rarefaction_fast.py \
       --db  /path/to/my_database.duckdb \
       --out /scratch/rarefaction_curve.csv \
       --threads 32 --mem 128GB
"""
from __future__ import annotations
import argparse, pathlib, sys, duckdb

QUERY = r"""
WITH first_seen AS (
    SELECT
        sm.min_hash,
        MIN(EXTRACT(year FROM sr.date_received)) AS first_year
    FROM sigs_dna.signature_mins AS sm
    JOIN sample_received          AS sr
      ON sr.sample_id = sm.sample_id
    GROUP BY sm.min_hash
),
new_per_year AS (
    SELECT first_year AS year,
           COUNT(*)   AS new_hashes
    FROM   first_seen
    GROUP  BY first_year
),
cumulative AS (
    SELECT
        year,
        SUM(new_hashes)
            OVER (ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS cumulative_hashes
    FROM new_per_year
)
SELECT year, cumulative_hashes
FROM   cumulative
ORDER  BY year;
"""

def main() -> None:
    ap = argparse.ArgumentParser(description="Fast rarefaction export.")
    ap.add_argument("--db",  required=True, type=pathlib.Path)
    ap.add_argument("--out", required=True, type=pathlib.Path)
    ap.add_argument("--threads", type=int, help="PRAGMA threads=N")
    ap.add_argument("--mem", type=str, help="PRAGMA memory_limit='...'")
    args = ap.parse_args()

    if not args.db.is_file():
        sys.exit(f"DB not found → {args.db}")

    out_path = args.out.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(args.db), read_only=True) as conn:
        #Resource knobs
        # set temp directory to /scratch/tmp
        conn.execute("SET temp_directory='/scratch/tmp'")

        if args.threads:
            conn.execute(f"PRAGMA threads={args.threads};")
        if args.mem:
            conn.execute(f"PRAGMA memory_limit='{args.mem}';")

        # Progress bar (prints to stderr)
        conn.execute("PRAGMA enable_progress_bar;")

        # Stream result directly to CSV
        copy_sql = f"""
            COPY ({QUERY})
            TO '{out_path}'
            (HEADER, DELIMITER ',');
        """
        conn.execute(copy_sql)

    print(f"✅  Rarefaction exported → {out_path}")

if __name__ == "__main__":
    main()
