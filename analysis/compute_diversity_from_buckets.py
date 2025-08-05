#!/usr/bin/env python3
"""
compute_diversity_from_buckets.py  –  aggregate diversity metrics from a
Hive‑partitioned Parquet dataset written by write_minhash_buckets.py.

Key improvements
----------------
* **Processes only the bucket folders that actually exist.**
  A tiny test database that produces e.g.  ➜  bucket=0, bucket=7, bucket=42
  works fine.
* **Never aborts when a bucket number is missing.**

"""
from __future__ import annotations
import argparse, collections, csv, json, logging, pathlib, sys, time
import duckdb

DEFAULT_BUCKET_DIR = pathlib.Path("/scratch/minhash_buckets")
PROGRESS_FILE      = "bucket_progress.json"

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s: %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
)
log = logging.getLogger("aggregator")


# ───────────────────────────────────────────────────────── helpers ──
def available_buckets(root: pathlib.Path) -> list[int]:
    """Return all bucket numbers that are present as bucket=NNN directories."""
    return sorted(
        int(p.name.split("=", 1)[1])
        for p in root.glob("bucket=*")
        if p.is_dir() and p.name.count("=") == 1 and p.name.split("=", 1)[1].isdigit()
    )


def load_progress(file: pathlib.Path) -> set[int]:
    return set(json.loads(file.read_text())) if file.is_file() else set()


def save_progress(done: set[int], file: pathlib.Path) -> None:
    file.write_text(json.dumps(sorted(done)))


# ─────────────────────────────────────────────────────────── main ──
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Compute yearly diversity metrics from bucketed Parquet."
    )
    ap.add_argument("--db", required=True, type=pathlib.Path)
    ap.add_argument("--buckets", type=pathlib.Path, default=DEFAULT_BUCKET_DIR)
    ap.add_argument("--out", type=pathlib.Path, default=pathlib.Path("diversity_metrics.csv"))
    ap.add_argument("--threads", "--thread", type=int, dest="threads", help="DuckDB PRAGMA threads")
    ap.add_argument("--mem", type=str, help="DuckDB PRAGMA memory_limit")
    args = ap.parse_args()

    if not args.db.is_file():
        sys.exit(f"Database not found → {args.db}")
    if not args.buckets.is_dir():
        sys.exit(f"Bucket directory not found → {args.buckets}")

    bucket_numbers = available_buckets(args.buckets)
    if not bucket_numbers:
        sys.exit("No bucket=NNN folders found – nothing to aggregate.")
    log.info("Discovered %d bucket folders: %s …", len(bucket_numbers), bucket_numbers[:5])

    checkpoint = args.buckets / PROGRESS_FILE
    done = load_progress(checkpoint)
    new_hashes = collections.Counter()

    # --------------------------------------------- aggregate bucket by bucket
    with duckdb.connect(":memory:") as conn:
        if args.threads is not None:
            conn.execute(f"PRAGMA threads={args.threads};")
        if args.mem:
            conn.execute(f"PRAGMA memory_limit='{args.mem}';")

        t0 = time.perf_counter()
        for idx, b in enumerate(bucket_numbers, 1):
            if b in done:
                continue

            path = args.buckets / f"bucket={b}"
            query = f"""
                WITH first_seen AS (
                    SELECT min_hash,
                           MIN(year) AS first_year
                    FROM read_parquet('{path}/*.parquet')
                    GROUP BY min_hash
                )
                SELECT first_year AS y,
                       COUNT(*)    AS c
                FROM first_seen
                GROUP BY first_year;
            """
            for y, c in conn.execute(query).fetchall():
                new_hashes[int(y)] += int(c)

            done.add(b)
            save_progress(done, checkpoint)
            log.info("Bucket %3d done  (%d / %d)  –  %.1f min elapsed",
                     b, len(done), len(bucket_numbers),
                     (time.perf_counter() - t0) / 60)

    # --------------------------------------------- sample counts (unchanged)
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

    # ------------------------------------------------------- write CSV
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
    log.info("   Progress checkpoint kept at %s", checkpoint)


if __name__ == "__main__":
    main()
