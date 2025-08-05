#!/usr/bin/env python3
"""
compute_diversity_from_buckets.py  –  robust, resumable aggregation of
diversity metrics from a Hive‑partitioned Parquet dataset.

Changes vs. the earlier version
-------------------------------
* Each bucket writes its own counts file
      <bucket_dir>/year_counts.parquet
  after the aggregation finishes.
* On restart we **reuse** those files instead of reading the big Parquet
  data again.
* The old bucket_progress.json mechanism is gone—no risk of “all zeros”.
"""
from __future__ import annotations
import argparse, collections, csv, logging, pathlib, sys, time, duckdb

DEFAULT_BUCKET_DIR = pathlib.Path("/scratch/minhash_buckets")
LOG_FMT = "[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO, datefmt="%H:%M:%S")
log = logging.getLogger("aggregator")


# ───────────────────────────────────────────────────────── helpers ──────────
def available_buckets(root: pathlib.Path) -> list[pathlib.Path]:
    """Return Path objects for every bucket=NNN directory present."""
    return sorted(p for p in root.glob("bucket=*") if p.is_dir())


def counts_file(bucket_dir: pathlib.Path) -> pathlib.Path:
    return bucket_dir / "year_counts.parquet"


def ensure_counts(conn: duckdb.DuckDBPyConnection, bucket_dir: pathlib.Path) -> None:
    """
    If <bucket_dir>/year_counts.parquet exists, do nothing.
    Else compute first‑seen counts for that bucket and write the file.
    """
    cfile = counts_file(bucket_dir)
    if cfile.exists():
        return

    parquet_glob = bucket_dir / "*.parquet"
    query = f"""
        WITH first_seen AS (
            SELECT min_hash,
                   MIN(year) AS first_year
            FROM read_parquet('{parquet_glob}')
            GROUP BY min_hash
        )
        SELECT first_year AS year,
               COUNT(*)   AS cnt
        FROM first_seen
        GROUP BY first_year
        ORDER BY year
    """
    conn.execute(
        f"""
        COPY ({query})
        TO '{str(cfile).replace("'", "''")}'
        (FORMAT parquet, COMPRESSION ZSTD);
        """
    )


def load_counts(conn: duckdb.DuckDBPyConnection, bucket_dir: pathlib.Path) -> list[tuple[int, int]]:
    """Return [(year, count), …] from the per‑bucket counts file."""
    cfile = counts_file(bucket_dir)
    return conn.execute(
        f"SELECT year, cnt FROM read_parquet('{cfile}')"
    ).fetchall()


# ─────────────────────────────────────────────────────────── main ──────────
def main() -> None:
    ap = argparse.ArgumentParser(description="Compute diversity metrics from bucketed Parquet.")
    ap.add_argument("--db",      required=True, type=pathlib.Path)
    ap.add_argument("--buckets", type=pathlib.Path, default=DEFAULT_BUCKET_DIR)
    ap.add_argument("--out",     type=pathlib.Path, default=pathlib.Path("diversity_metrics.csv"))
    ap.add_argument("--threads", type=int, help="DuckDB PRAGMA threads")
    ap.add_argument("--mem",     type=str, help="DuckDB PRAGMA memory_limit")
    args = ap.parse_args()

    if not args.buckets.is_dir():
        sys.exit(f"Bucket directory not found → {args.buckets}")
    if not args.db.is_file():
        sys.exit(f"Database not found → {args.db}")

    bucket_dirs = available_buckets(args.buckets)
    if not bucket_dirs:
        sys.exit("No bucket=NNN folders found – nothing to aggregate.")
    log.info("Found %d bucket folders.", len(bucket_dirs))

    new_hashes = collections.Counter()

    with duckdb.connect(":memory:") as conn:
        if args.threads is not None:
            conn.execute(f"PRAGMA threads={args.threads};")
        if args.mem:
            conn.execute(f"PRAGMA memory_limit='{args.mem}';")

        t0 = time.perf_counter()
        for i, bdir in enumerate(bucket_dirs, 1):
            ensure_counts(conn, bdir)
            for y, c in load_counts(conn, bdir):
                new_hashes[int(y)] += int(c)

            if i % 10 == 0 or i == len(bucket_dirs):
                log.info("Processed %d / %d buckets – %.1f min elapsed",
                         i, len(bucket_dirs), (time.perf_counter() - t0) / 60)

    # ------------------------------- sample counts (unchanged) ---------------
    with duckdb.connect(str(args.db), read_only=True) as conn_db:
        if args.threads is not None:
            conn_db.execute(f"PRAGMA threads={args.threads};")
        if args.mem:
            conn_db.execute(f"PRAGMA memory_limit='{args.mem}';")

        sample_counts = {
            int(y): int(n)
            for y, n in conn_db.execute(
                """
                SELECT EXTRACT(year FROM date_received) AS yr,
                       COUNT(DISTINCT sample_id)        AS n
                FROM sample_received
                GROUP  BY yr
                """
            ).fetchall()
        }

    # ------------------------------------ assemble final CSV -----------------
    years = sorted(set(sample_counts) | set(new_hashes))
    cum_samples = cum_hashes = 0

    out_path = args.out.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([
            "year", "n_samples", "new_hashes",
            "cumulative_samples", "cumulative_hashes",
            "hashes_per_sample", "cumulative_hashes_per_sample",
        ])
        for y in years:
            ns, nh = sample_counts.get(y, 0), new_hashes.get(y, 0)
            cum_samples += ns
            cum_hashes += nh
            hps  = nh / ns               if ns          else 0.0
            chps = cum_hashes / cum_samples if cum_samples else 0.0
            w.writerow([y, ns, nh, cum_samples, cum_hashes,
                        f"{hps:.6g}", f"{chps:.6g}"])

    log.info("✅  Diversity metrics written → %s", out_path)


if __name__ == "__main__":
    main()
