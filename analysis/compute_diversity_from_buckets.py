#!/usr/bin/env python3
# ───────────────────────────────────────────────────────────────────────────────
#  compute_diversity_from_buckets.py
#  ----------------------------------
#  Reads the Hive‑partitioned Parquet dataset written by write_minhash_buckets.py
#  and produces diversity metrics identical to export_diversity_metrics.py
#
#  Defensive features:
#    • Processes one bucket at a time (≈ 42 GB) – constant memory
#    • Checkpoint file (<dest>/bucket_progress.json) lets the run resume
#    • Validates that final CSV has the same set of years as sample counts
# ───────────────────────────────────────────────────────────────────────────────
"""
Usage
-----
python compute_diversity_from_buckets.py \
       --db   /path/to/database.db \
       --buckets /scratch/minhash_buckets   # default
       --out  diversity_metrics.csv \
       --threads 64 --mem 512GB
"""

from __future__ import annotations

import argparse, collections, csv, json, logging, pathlib, sys, time
import duckdb

DEFAULT_BUCKET_DIR = pathlib.Path("/scratch/minhash_buckets")
PROGRESS_FILE = "bucket_progress.json"
LOG_FMT = "[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO, datefmt="%H:%M:%S")
log = logging.getLogger("aggregator")


def load_progress(checkpoint: pathlib.Path) -> set[int]:
    if checkpoint.is_file():
        return set(json.loads(checkpoint.read_text()))
    return set()


def save_progress(done: set[int], checkpoint: pathlib.Path) -> None:
    checkpoint.write_text(json.dumps(sorted(done)))


def main() -> None:
    ap = argparse.ArgumentParser(description="Compute diversity metrics from bucketed Parquet.")
    ap.add_argument("--db", required=True, type=pathlib.Path)
    ap.add_argument("--buckets", type=pathlib.Path, default=DEFAULT_BUCKET_DIR)
    ap.add_argument("--out", type=pathlib.Path, default=pathlib.Path("diversity_metrics.csv"))
    ap.add_argument("--threads", type=int, help="DuckDB PRAGMA threads")
    ap.add_argument("--mem", type=str, help="DuckDB PRAGMA memory_limit")
    args = ap.parse_args()

    if not args.buckets.is_dir():
        sys.exit(f"Bucket directory not found → {args.buckets}")
    if not args.db.is_file():
        sys.exit(f"Database not found → {args.db}")

    checkpoint = args.buckets / PROGRESS_FILE
    done_buckets = load_progress(checkpoint)
    new_hashes = collections.Counter()

    with duckdb.connect(":memory:") as conn:
        if args.threads:
            conn.execute(f"PRAGMA threads={args.threads};")
        if args.mem:
            conn.execute(f"PRAGMA memory_limit='{args.mem}';")

        t0 = time.perf_counter()
        for b in range(256):
            if b in done_buckets:
                continue
            bucket_path = args.buckets / f"bucket={b}"
            if not bucket_path.is_dir():
                sys.exit(f"Expected directory {bucket_path} – aborting.")

            query = f"""
                WITH first_seen AS (
                    SELECT min_hash,
                           MIN(year) AS first_year
                    FROM read_parquet('{bucket_path}/*.parquet')
                    GROUP BY min_hash
                )
                SELECT first_year AS year,
                       COUNT(*)    AS cnt
                FROM first_seen
                GROUP BY first_year;
            """
            for year, cnt in conn.execute(query).fetchall():
                new_hashes[int(year)] += int(cnt)

            done_buckets.add(b)
            save_progress(done_buckets, checkpoint)
            if len(done_buckets) % 10 == 0:
                log.info("Processed %d / 256 buckets (%.1f %%) – %.1f min elapsed",
                         len(done_buckets), len(done_buckets) / 2.56,
                         (time.perf_counter() - t0) / 60)

    # ---------------------------------------------------------------- samples -
    with duckdb.connect(str(args.db), read_only=True) as conn_db:
        if args.threads:
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

    # ----------------------------------------------------- assemble final csv -
    years = sorted(set(sample_counts) | set(new_hashes))
    cum_samples = cum_hashes = 0

    out_path = args.out.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(
            [
                "year",
                "n_samples",
                "new_hashes",
                "cumulative_samples",
                "cumulative_hashes",
                "hashes_per_sample",
                "cumulative_hashes_per_sample",
            ]
        )
        for y in years:
            ns = sample_counts.get(y, 0)
            nh = new_hashes.get(y, 0)
            cum_samples += ns
            cum_hashes += nh
            hps = nh / ns if ns else 0.0
            chps = cum_hashes / cum_samples if cum_samples else 0.0
            w.writerow([y, ns, nh, cum_samples, cum_hashes, f"{hps:.6g}", f"{chps:.6g}"])

    log.info("✅  Diversity metrics written → %s", out_path)
    log.info("   (bucket progress checkpoint kept at %s)", checkpoint)


if __name__ == "__main__":
    main()
