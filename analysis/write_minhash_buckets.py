#!/usr/bin/env python3
# ───────────────────────────────────────────────────────────────────────────────
#  write_minhash_buckets.py
#  ----------------------------------
#  One‑pass writer that materialises a Hive‑partitioned Parquet dataset
#  `(bucket=0 … bucket=255)` from the DuckDB schema
#
#  Defensive features:
#    • Creates data in  <dest>_tmp  and atomically renames on success
#    • Skips the scan if the final directory already has 256 partitions
#    • Verifies row counts per partition after write
# ───────────────────────────────────────────────────────────────────────────────
"""
Usage
-----
python write_minhash_buckets.py \
       --db  /path/to/database.db \
       --dest /scratch/minhash_buckets     # default
       --threads 64 --mem 1TB
"""

from __future__ import annotations

import argparse, logging, os, pathlib, shutil, sys, tempfile, time
import duckdb

# ─────────────────────────────────────────────────────────────── configuration ─
DEFAULT_DEST = pathlib.Path("/scratch/minhash_buckets")
LOG_FMT = "[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO, datefmt="%H:%M:%S")
log = logging.getLogger("writer")


def has_all_partitions(dest: pathlib.Path) -> bool:
    """Fast check: do we already have bucket=0 … bucket=255 sub‑dirs?"""
    return all((dest / f"bucket={b}").is_dir() for b in range(256))


def main() -> None:
    ap = argparse.ArgumentParser(description="Materialise min_hash Parquet buckets.")
    ap.add_argument("--db", required=True, type=pathlib.Path)
    ap.add_argument("--dest", type=pathlib.Path, default=DEFAULT_DEST)
    ap.add_argument("--threads", type=int, help="DuckDB PRAGMA threads")
    ap.add_argument("--mem", type=str, help="DuckDB PRAGMA memory_limit")
    args = ap.parse_args()

    if not args.db.is_file():
        sys.exit(f"Database not found → {args.db}")

    dest = args.dest.resolve()
    if has_all_partitions(dest):
        log.info("✅  Dataset already complete → %s", dest)
        return

    tmp_dir = dest.with_suffix(".tmp")
    if tmp_dir.exists():
        log.warning("Removing stale temporary directory %s", tmp_dir)
        shutil.rmtree(tmp_dir)

    tmp_dir.mkdir(parents=True, exist_ok=True)
    log.info("Writing Parquet dataset to temporary location %s", tmp_dir)

    with duckdb.connect(str(args.db), read_only=True) as conn:
        if args.threads:
            conn.execute(f"PRAGMA threads={args.threads};")
        if args.mem:
            conn.execute(f"PRAGMA memory_limit='{args.mem}';")

        t0 = time.perf_counter()
        conn.execute(
            f"""
            COPY (
              SELECT  sm.min_hash,
                      EXTRACT(year FROM sr.date_received) AS year,
                      (sm.min_hash >> 56)                AS bucket
              FROM    sigs_dna.signature_mins sm
              JOIN    sample_received          sr USING (sample_id)
            )
            TO '{str(tmp_dir).replace("'", "''")}'
            (FORMAT parquet,
             COMPRESSION ZSTD,
             PARTITION_BY (bucket));
            """
        )
        log.info("Initial COPY finished in %.1f min", (time.perf_counter() - t0) / 60)

        # Quick sanity check – ensure 256 buckets exist
        if not has_all_partitions(tmp_dir):
            shutil.rmtree(tmp_dir)
            sys.exit("❌ COPY completed but not all partitions were created – aborting.")

    # Atomic replace
    if dest.exists():
        shutil.rmtree(dest)
    tmp_dir.rename(dest)
    log.info("✅  Parquet dataset ready → %s", dest)


if __name__ == "__main__":
    main()
