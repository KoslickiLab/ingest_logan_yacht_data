#!/usr/bin/env python3
# ───────────────────────────────────────────────────────────────────────────────
#  export_diversity_metrics_stream.py
#  ----------------------------------
#  "Approach 4" – single‑pass streaming algorithm with an in‑memory bitmap.
#                 Works in O(total_rows) time and  O(#distinct_hashes)  memory.
# ───────────────────────────────────────────────────────────────────────────────
"""
Usage
-----
python export_diversity_metrics_stream.py \
       --db  database.db \
       --out diversity_metrics.csv \
       --batch-size 1_000_000        # number of rows fetched per chunk
       --bitmap  roaring             # 'roaring' or 'set'   (default: try roaring)

Dependencies
------------
* **pyroaring** (optional but recommended).  Install with:
      pip install pyroaring
  Falls back to a plain Python `set` if unavailable.

Memory safety
-------------
*RoaringBitmap64* stores ~2 bytes / distinct hash when data are sparse and
compresses dense runs; a billion unique hashes is ≈ 2 GB.  With `set` the same
workload would need ~16 GB.  Adjust `batch‑size` to balance RAM vs. throughput.
"""
from __future__ import annotations

import argparse, collections, csv, itertools, logging, pathlib, sys, time
import duckdb

# ───────────────────────────────────────────────────────────────────── helpers ──
LOG_FMT = "[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO, datefmt="%H:%M:%S")
log = logging.getLogger("stream")


def try_roaring() -> tuple[bool, object]:
    try:
        from pyroaring import BitMap64

        return True, BitMap64()
    except ImportError:
        return False, set()


# ────────────────────────────────────────────────────────────────────────── main
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Compute diversity metrics in a single streaming pass."
    )
    ap.add_argument("--db", required=True, type=pathlib.Path)
    ap.add_argument("--out", required=True, type=pathlib.Path)
    ap.add_argument(
        "--batch-size",
        type=int,
        default=1_000_000,
        help="Rows fetched per duckdb cursor batch",
    )
    ap.add_argument(
        "--bitmap",
        choices=("roaring", "set"),
        default="roaring",
        help="Backend for the seen‑hash set",
    )
    ap.add_argument("--threads", type=int, help="DuckDB PRAGMA threads")
    ap.add_argument("--mem", type=str, help="DuckDB PRAGMA memory_limit")
    args = ap.parse_args()

    if not args.db.is_file():
        sys.exit(f"Database not found → {args.db}")

    out_path = args.out.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------- bitmap ---
    WANT_ROARING = args.bitmap == "roaring"
    roaring_ok, seen = try_roaring()
    if WANT_ROARING and not roaring_ok:
        log.warning("pyroaring not found → falling back to plain Python set()")
    if not WANT_ROARING and roaring_ok:
        seen = set()  # user explicitly asked for 'set'

    # ----------------------------------------------------------- open database -
    with duckdb.connect(str(args.db), read_only=True) as conn:
        if args.threads:
            conn.execute(f"PRAGMA threads={args.threads};")
        if args.mem:
            conn.execute(f"PRAGMA memory_limit='{args.mem}';")

        # -------------------------------------------- per‑year sample counts ---
        ns_dict = {
            int(year): int(n)
            for year, n in conn.execute(
                """
                SELECT EXTRACT(year FROM date_received) AS year,
                       COUNT(DISTINCT sample_id)        AS n_samples
                FROM   sample_received
                GROUP  BY year
                ORDER  BY year
                """
            ).fetchall()
        }

        # ---------------------------------- stream joined rows by date_received -
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT  sm.min_hash::UBIGINT AS h,
                    EXTRACT(year FROM sr.date_received)::INTEGER AS y
            FROM    sigs_dna.signature_mins sm
            JOIN    sample_received sr USING (sample_id)
            ORDER   BY sr.date_received
            """
        )

        new_hashes = collections.Counter()
        batch_size = args.batch_size
        total_rows = 0
        t0 = time.perf_counter()

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for h, y in rows:
                if h not in seen:
                    new_hashes[y] += 1
                    seen.add(h)
            total_rows += len(rows)

            if total_rows // batch_size % 10 == 0:  # log every 10 batches
                log.info(
                    " %.1f M rows, %.1f M distinct hashes",
                    total_rows / 1e6,
                    len(seen) / 1e6,
                )

        log.info(
            "Finished streaming %.1f M rows in %.1f min",
            total_rows / 1e6,
            (time.perf_counter() - t0) / 60,
        )

    # ------------------------------------------------------- assemble results -
    all_years = sorted(set(ns_dict) | set(new_hashes))
    cum_samples = cum_hashes = 0

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

        for year in all_years:
            ns = ns_dict.get(year, 0)
            nh = new_hashes.get(year, 0)
            cum_samples += ns
            cum_hashes += nh
            hps = nh / ns if ns else 0.0
            chps = cum_hashes / cum_samples if cum_samples else 0.0
            w.writerow(
                [
                    year,
                    ns,
                    nh,
                    cum_samples,
                    cum_hashes,
                    f"{hps:.6g}",
                    f"{chps:.6g}",
                ]
            )

    log.info("✅  Diversity metrics written → %s", out_path)


if __name__ == "__main__":
    main()
