#!/usr/bin/env python3
"""
export_rarefaction.py  –  create a rarefaction‐curve dataset from DuckDB.

Example
-------
python export_rarefaction.py \
       --db   /path/to/my_database.duckdb \
       --out  rarefaction_curve.csv
"""
from __future__ import annotations
import argparse, pathlib, sys, duckdb, pandas as pd

# --------------------------------------------------------------------------- #
QUERY = r"""
WITH first_seen AS (
    -- Find the first calendar year in which each distinct min_hash appears
    SELECT
        sm.min_hash,
        MIN(EXTRACT(year FROM sr.date_received)) AS first_year
    FROM sigs_dna.signature_mins AS sm
    JOIN sample_received          AS sr
      ON sr.sample_id = sm.sample_id
    GROUP BY sm.min_hash
),
year_range AS (
    -- Generate one row per year from the earliest to the latest observation
    SELECT *
    FROM range(
        (SELECT MIN(first_year) FROM first_seen),          -- inclusive start
        (SELECT MAX(first_year) FROM first_seen) + 1       -- exclusive stop
    ) AS yrs(year)                                         -- yrs.year is INT
)
SELECT
    yr.year                                            AS year,
    (
        SELECT COUNT(*)                                -- cumulative total
        FROM first_seen f
        WHERE f.first_year <= yr.year
    ) AS cumulative_hashes
FROM year_range AS yr
ORDER BY yr.year;
"""

# --------------------------------------------------------------------------- #
def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Export rarefaction data to CSV.")
    p.add_argument("--db",  required=True, type=pathlib.Path,
                   help="Path to DuckDB database file.")
    p.add_argument("--out", required=True, type=pathlib.Path,
                   help="Output CSV path.")
    args = p.parse_args(argv)

    if not args.db.is_file():
        sys.exit(f"Error: database not found → {args.db}")

    # Run the query and fetch into a Pandas DataFrame
    with duckdb.connect(str(args.db), read_only=True) as conn:
        df: pd.DataFrame = conn.execute(QUERY).fetchdf()

    # Ensure the output directory exists, then write CSV
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)

    print(f"✅  Rarefaction data written to {args.out} "
          f"({len(df)} rows, {df['cumulative_hashes'].iloc[-1]:,} hashes).")


if __name__ == "__main__":   # pragma: no cover
    main()
