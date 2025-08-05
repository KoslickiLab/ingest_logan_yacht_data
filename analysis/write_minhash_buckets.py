#!/usr/bin/env python3
"""
write_minhash_buckets.py
------------------------
Single‑pass scan of the DuckDB that writes a Hive‑partitioned Parquet
dataset (bucket=0 … bucket=255) under <dest>.  The script is **idempotent**:

* Buckets that already exist in <dest> are left untouched.
* If the previous run was interrupted the temporary directory is inspected
  and any completed buckets inside it are *merged* into <dest>.
* The scan is skipped entirely when *all* 256 buckets already exist.
"""
from __future__ import annotations
import argparse, logging, os, pathlib, shutil, sys, tempfile, time
import duckdb

DEFAULT_DEST = pathlib.Path("/scratch/minhash_buckets")   # change if desired
LOG_FMT = "[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO, datefmt="%H:%M:%S")
log = logging.getLogger("writer")


def existing_buckets(root: pathlib.Path) -> set[int]:
    """Return the set of bucket IDs (0‑255) that already exist as dirs."""
    return {
        int(p.name.split("=", 1)[1])
        for p in root.glob("bucket=*")
        if p.is_dir() and p.name.count("=") == 1 and p.name.split("=", 1)[1].isdigit()
    }


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
    dest.mkdir(parents=True, exist_ok=True)

    done = existing_buckets(dest)
    if len(done) == 256:
        log.info("✅  All 256 buckets already present – nothing to do.")
        return
    log.info("Found %d / 256 buckets in %s; %d remain to be written.",
             len(done), dest, 256 - len(done))

    #
    # Write missing buckets to a throw‑away temporary dir first
    #
    with tempfile.TemporaryDirectory(dir=dest.parent, prefix="minhash_tmp_") as tmp:
        tmp_path = pathlib.Path(tmp)
        log.info("Copying missing buckets into   %s", tmp_path)

        where_clause = ""
        if done:
            done_list = ",".join(map(str, sorted(done)))
            where_clause = f"WHERE (sm.min_hash >> 56) NOT IN ({done_list})"

        with duckdb.connect(str(args.db), read_only=True) as conn:
            if args.threads:
                conn.execute(f"PRAGMA threads={args.threads};")
            if args.mem:
                conn.execute(f"PRAGMA memory_limit='{args.mem}';")

            t0 = time.perf_counter()
            conn.execute(f"""
                COPY (
                  SELECT  sm.min_hash,
                          EXTRACT(year FROM sr.date_received) AS year,
                          (sm.min_hash >> 56)                AS bucket
                  FROM    sigs_dna.signature_mins sm
                  JOIN    sample_received          sr USING (sample_id)
                  {where_clause}
                )
                TO '{str(tmp_path).replace("'", "''")}'
                (FORMAT parquet,
                 COMPRESSION ZSTD,
                 PARTITION_BY (bucket));
            """)
            log.info("COPY finished in %.1f min",
                     (time.perf_counter() - t0) / 60)

        #
        # Move any new bucket=NNN folders over *individually* (atomic per bucket)
        #
        new_buckets = 0
        for p in tmp_path.glob("bucket=*"):
            b = int(p.name.split("=", 1)[1])
            target = dest / p.name
            if target.exists():
                # Should only happen if two concurrent writers race;
                # keep the first version and drop the duplicate.
                shutil.rmtree(p)
                continue
            shutil.move(str(p), target)
            new_buckets += 1

        log.info("Moved %d new bucket(s) into %s", new_buckets, dest)

    # tmp directory auto‑removed by context manager
    final = existing_buckets(dest)
    log.info("Dataset now has %d / 256 buckets.", len(final))


if __name__ == "__main__":
    main()
