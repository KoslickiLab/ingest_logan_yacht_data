#!/usr/bin/env python3
# ───────────────────────────────────────────────────────────────────────────────
#  export_diversity_metrics_bucket.py
#  ---------------------------------
#  "Approach 3" – build minhash_first_seen in small, RAM‑bounded partitions,
#  then run the original diversity‑metrics query against the materialised table.
# ───────────────────────────────────────────────────────────────────────────────
"""
Usage
-----
python export_diversity_metrics_bucket.py \
       --db   database.db \
       --out  diversity_metrics.csv \
       --buckets 256              # default
       --threads 64 --mem 512GB   # optional DuckDB PRAGMAs

Memory safety
-------------
Total resident‑set ≤  (DB working‑set for *one* bucket)
                      + output buffers (< 1 GiB typical)

Choose a power‑of‑two bucket count so that         (largest bucket size)
×  2 × (8–16 B per hash in DuckDB's hash table)
stays below available RAM.

If you re‑run after an interruption the script skips completed buckets, so it is
safe to kill and resume.
"""
from __future__ import annotations

import argparse, itertools, logging, math, pathlib, sys, textwrap, time
import duckdb

# ───────────────────────────────────────────────────────────────────── helpers ──
LOG_FMT = "[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO, datefmt="%H:%M:%S")
log = logging.getLogger("bucket")

HASH_BITS = 64


def power_of_two(n: int) -> bool:
    return n >= 1 and (n & (n - 1)) == 0


def create_first_seen_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Idempotent DDL for the materialised table."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS minhash_first_seen (
            min_hash   UBIGINT PRIMARY KEY,
            first_year INTEGER
        );
        """
    )


def existing_bucket_ids(conn: duckdb.DuckDBPyConnection, shift: int) -> set[int]:
    """Return the set of bucket IDs already materialised (resume support)."""
    query = f"SELECT DISTINCT (min_hash >> {shift}) AS b FROM minhash_first_seen;"
    return {row[0] for row in conn.execute(query).fetchall()}


def materialise_bucket(
    conn: duckdb.DuckDBPyConnection, bucket: int, shift: int
) -> None:
    """Run the GROUP BY for one bucket and append to minhash_first_seen."""
    log.info("  ↳ bucket %4d …", bucket)
    sql = f"""
        INSERT OR IGNORE INTO minhash_first_seen
        SELECT  sm.min_hash,
                MIN(EXTRACT(year FROM sr.date_received)) AS first_year
        FROM    sigs_dna.signature_mins sm
        JOIN    sample_received sr USING (sample_id)
        WHERE   (sm.min_hash >> {shift}) = {bucket}
        GROUP   BY sm.min_hash;
    """
    conn.execute(sql)


def export_metrics(conn: duckdb.DuckDBPyConnection, out: pathlib.Path) -> None:
    """Run the original report (minus the expensive first CTE)."""
    out_literal = str(out).replace("'", "''")
    report_sql = textwrap.dedent(
        """
        WITH
          new_hashes AS (
              SELECT first_year AS year,
                     COUNT(*)   AS new_hashes
              FROM   minhash_first_seen
              GROUP  BY first_year
          ),
          samples AS (
              SELECT EXTRACT(year FROM date_received) AS year,
                     COUNT(DISTINCT sample_id)        AS n_samples
              FROM   sample_received
              GROUP  BY year
          ),
          yr AS (
              SELECT *
              FROM range(
                  (SELECT MIN(year) FROM samples),
                  (SELECT MAX(year) FROM samples) + 1
              ) AS y(year)
          ),
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
            CASE WHEN n_samples            > 0
                 THEN new_hashes::DOUBLE / n_samples
                 ELSE 0 END                               AS hashes_per_sample,
            CASE WHEN SUM(n_samples) OVER (ORDER BY year) > 0
                 THEN SUM(new_hashes) OVER (ORDER BY year)::DOUBLE
                      / SUM(n_samples) OVER (ORDER BY year)
                 ELSE 0 END                               AS cumulative_hashes_per_sample
        FROM joined
        ORDER BY year
        """
    )

    conn.execute(
        f"COPY ({report_sql}) TO '{out_literal}' (HEADER, DELIMITER ',');"
    )


# ────────────────────────────────────────────────────────────────────────── main
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Compute diversity metrics using hash bucketing."
    )
    ap.add_argument("--db", required=True, type=pathlib.Path)
    ap.add_argument("--out", required=True, type=pathlib.Path)
    ap.add_argument("--buckets", type=int, default=256, help="Power-of-two bucket count")
    ap.add_argument("--threads", type=int, help="DuckDB PRAGMA threads")
    ap.add_argument("--mem", type=str, help="DuckDB PRAGMA memory_limit")
    args = ap.parse_args()

    if not args.db.is_file():
        sys.exit(f"Database not found → {args.db}")

    if not power_of_two(args.buckets):
        sys.exit("--buckets must be a power of two")

    shift = HASH_BITS - int(math.log2(args.buckets))
    log.info("Using %d buckets → shift=%d", args.buckets, shift)

    out_path = args.out.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(args.db)) as conn:
        if args.threads:
            conn.execute(f"PRAGMA threads={args.threads};")
        if args.mem:
            conn.execute(f"PRAGMA memory_limit='{args.mem}';")

        create_first_seen_table(conn)
        done = existing_bucket_ids(conn, shift)
        todo = [b for b in range(args.buckets) if b not in done]

        log.info("%d / %d buckets remaining", len(todo), args.buckets)
        start = time.perf_counter()
        for b in todo:
            materialise_bucket(conn, b, shift)
        log.info("Materialisation finished in %.1f min",
                 (time.perf_counter() - start) / 60)

        export_metrics(conn, out_path)

    log.info("✅  Diversity metrics written → %s", out_path)


if __name__ == "__main__":
    main()
