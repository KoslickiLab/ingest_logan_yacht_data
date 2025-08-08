#!/usr/bin/env python3
"""
Fast, memory‑efficient rarefaction export with progress bar.
"""

from __future__ import annotations
import argparse
import pathlib
import sys
import duckdb
import textwrap

# --------------------------------------------------------------------------- #
RAW_QUERY = textwrap.dedent(r"""
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
                OVER (ORDER BY year
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                AS cumulative_hashes
        FROM new_per_year
    )
    SELECT year, cumulative_hashes
    FROM   cumulative
    ORDER  BY year
""").strip()                     # ← NO trailing semicolon

# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description="Export rarefaction curve to CSV.")
    ap.add_argument("--db",      required=True, type=pathlib.Path)
    ap.add_argument("--out",     required=True, type=pathlib.Path)
    ap.add_argument("--threads", type=int, help="PRAGMA threads=N")
    ap.add_argument("--mem",     type=str, help="PRAGMA memory_limit='…'")
    ap.add_argument("--tmp",     type=pathlib.Path,
                    help="Temp directory for spills (PRAGMA temp_directory)")
    args = ap.parse_args()

    if not args.db.is_file():
        sys.exit(f"DB not found → {args.db}")

    out_path = args.out.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(args.db), read_only=True) as conn:
        # Resource knobs ----------------------------------------------------- #
        if args.threads:
            conn.execute(f"PRAGMA threads={args.threads};")
        if args.mem:
            conn.execute(f"PRAGMA memory_limit='{args.mem}';")
        if args.tmp:
            conn.execute(f"PRAGMA temp_directory='{args.tmp}';")

        # Progress bar ------------------------------------------------------- #
        conn.execute("PRAGMA enable_progress_bar;")
        conn.execute("SET enable_progress_bar = true;")
        conn.execute("SET enable_progress_bar_print = true;")
        conn.execute("SET progress_bar_time = 0;")

        # Escape the output path for SQL literal
        out_sql = str(out_path).replace("'", "''")

        copy_sql = f"""
            COPY (
                {RAW_QUERY}
            )
            TO '{out_sql}'
            (HEADER, DELIMITER ',');
        """
        conn.execute(copy_sql)

    print(f"✅  Rarefaction exported → {out_path}")

# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    main()
