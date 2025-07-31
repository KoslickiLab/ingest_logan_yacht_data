#!/usr/bin/env python
"""
export_to_parquet.py
────────────────────
Two‑stage producer/consumer pipeline **with live progress dashboard**.

* Domain logic, column names, Parquet layout — unchanged.
* New `Progress` class (shared atomic counters) + a background thread that
  refreshes a single status line 2×/s.
* Zero overhead for small test runs: pass `--dash-off` to disable.
"""

from __future__ import annotations

# ── stdlib ───────────────────────────────────────────────────────────
import argparse, gzip, json, logging, os, re, shutil, sys, tarfile, tempfile, \
       uuid, zipfile, multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from threading import Thread
from time import sleep
from typing import List
from typing import List, Optional

# ── 3rd‑party ────────────────────────────────────────────────────────
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# ── project config (unchanged) ───────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config                              # noqa: E402

# ═════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═════════════════════════════════════════════════════════════════════
ROW_GROUP_SIZE    = 128 * 1024 * 1024        # 128 MiB
DEFAULT_PRODUCERS = 16
DEFAULT_CONSUMERS = max(1, os.cpu_count() // 2)
DEFAULT_BUFFER    = 10

STAGING_ROOT = Path(os.getenv("STAGING_DIR", "data_staging")).resolve()
STAGING_ROOT.mkdir(parents=True, exist_ok=True)

# ═════════════════════════════════════════════════════════════════════
# Progress dashboard
# ═════════════════════════════════════════════════════════════════════
class Progress:
    """Shared, lock‑free progress counters implemented with mp.Value."""
    def __init__(self, total_archives: int):
        self.total       = total_archives
        self.extracted   = mp.Value("Q", 0)   # unsigned long long
        self.processing  = mp.Value("Q", 0)
        self.completed   = mp.Value("Q", 0)
        self.failed      = mp.Value("Q", 0)

    # helpers below hide the with‑statement noise
    def inc(self, field: str, n: int = 1):
        val = getattr(self, field)
        with val.get_lock():
            val.value += n

    def value(self, field: str) -> int:
        return getattr(self, field).value


def _dashboard(tracker: Progress,
               q: mp.JoinableQueue,
               stop_evt: mp.Event,
               refresh_hz: float = 2.0):
    """Runs in a daemon thread inside the main process."""
    start = datetime.now()
    clear = "\033[2K\r"
    while not stop_evt.is_set():
        done   = tracker.value("completed")
        failed = tracker.value("failed")
        proc   = tracker.value("processing")
        extr   = tracker.value("extracted")
        queued = q.qsize() if hasattr(q, "qsize") else max(extr - done - proc, 0)
        rate   = done / max((datetime.now() - start).total_seconds(), 1)
        eta_sec = (tracker.total - done) / rate if rate else 0
        eta   = str(timedelta(seconds=int(eta_sec)))
        line = (f"Extracted {extr}/{tracker.total} · "
                f"Queue {queued} · "
                f"Processing {proc} · "
                f"Done {done} · "
                f"Fail {failed} · "
                f"{rate:5.2f} / s · ETA {eta}")
        print(f"{clear}{line}", end="", flush=True)
        sleep(1 / refresh_hz)
    print()              # final newline when we exit



# ═════════════════════════════════════════════════════════════════════
# Helper – Parquet writer  (UNCHANGED)
# ═════════════════════════════════════════════════════════════════════
def write_partitioned_parquet(df: pd.DataFrame, logical_path: str) -> None:
    """Append *df* to STAGING_ROOT/logical_path/part‑<uuid>.parquet."""
    if df is None or df.empty:
        return

    table_dir = STAGING_ROOT / logical_path
    table_dir.mkdir(parents=True, exist_ok=True)

    # force deterministic dtype inference
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype("string")

    out_file = table_dir / f"part-{uuid.uuid4()}.parquet"
    table    = pa.Table.from_pandas(df, preserve_index=False)

    pq.write_table(
        table,
        where=out_file,
        compression="zstd",
        use_dictionary=True,
        row_group_size=ROW_GROUP_SIZE,
        data_page_size=1_048_576,
        write_statistics=True,
    )

# ═════════════════════════════════════════════════════════════════════
# Stage‑1  –  Extract one archive
# ═════════════════════════════════════════════════════════════════════
def extract_nested_archives(archive_path: str):
    """
    Untar *archive_path* → tmp dir, find sub‑directories and return their paths.
    Returns tuple(func, taxa, sigs_aa, sigs_dna, tmpdir).
    """
    temp_dir = tempfile.mkdtemp(prefix="logan_")
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(temp_dir)

    func = taxa = sigs_aa = sigs_dna = None
    for root, dirs, _ in os.walk(temp_dir):
        if "func_profiles" in dirs:        # anchor directory
            parent = Path(root)
            func   = parent / "func_profiles"
            if "taxa_profiles" in dirs:
                taxa = parent / "taxa_profiles"
            if "sigs_aa" in dirs:
                sigs_aa = parent / "sigs_aa"
            if "sigs_dna" in dirs:
                sigs_dna = parent / "sigs_dna"
            break

    if func is None:
        raise FileNotFoundError(f"{archive_path}: func_profiles missing")

    # Return simple strings – picklable & lightweight
    return str(func), str(taxa) if taxa else "", \
           str(sigs_aa) if sigs_aa else "", \
           str(sigs_dna) if sigs_dna else "", \
           temp_dir

# ═════════════════════════════════════════════════════════════════════
# Stage‑2 helpers  (ALL UNCHANGED BUSINESS LOGIC)
# ═════════════════════════════════════════════════════════════════════
# ▸ Functional profile + gather
def process_functional_profiles(func_profiles_dir: Path):
    files = list(func_profiles_dir.glob("*_functional_profile"))
    dfs: List[pd.DataFrame] = []
    for fp in files:
        sid = fp.name.split("_functional_profile")[0]
        try:
            df = pd.read_csv(fp)
        except pd.errors.EmptyDataError:
            continue
        if df.empty:
            continue
        df["sample_id"] = sid
        dfs.append(df[["sample_id", "ko_id", "abundance"]])
    if dfs:
        write_partitioned_parquet(pd.concat(dfs, ignore_index=True),
                                  "functional_profile/profiles")

def process_gather_files(func_profiles_dir: Path):
    files = list(func_profiles_dir.glob("*.unitigs.fa_gather_*.tmp"))
    dfs: List[pd.DataFrame] = []
    for fp in files:
        sid = fp.name.split(".unitigs.fa_gather_")[0]
        try:
            df = pd.read_csv(fp)
        except pd.errors.EmptyDataError:
            continue
        if df.empty:
            continue
        df["sample_id"] = sid
        cols = ["sample_id"] + [c for c in df.columns if c != "sample_id"]
        dfs.append(df[cols])
    if dfs:
        write_partitioned_parquet(pd.concat(dfs, ignore_index=True),
                                  "functional_profile_data/gather_data")

# ▸ Taxa profiles
EXPECTED_TAXA_COLS = [
    'sample_id', 'organism_name', 'organism_id', 'tax_id',
    'num_unique_kmers_in_genome_sketch', 'num_total_kmers_in_genome_sketch',
    'scale_factor', 'num_exclusive_kmers_in_sample_sketch',
    'num_total_kmers_in_sample_sketch', 'min_coverage', 'p_vals',
    'num_exclusive_kmers_to_genome', 'num_exclusive_kmers_to_genome_coverage',
    'num_matches', 'acceptance_threshold_with_coverage',
    'actual_confidence_with_coverage', 'alt_confidence_mut_rate_with_coverage',
    'in_sample_est'
]
def process_taxa_profiles(taxa_dir: Optional[Path]):
    if not taxa_dir or not taxa_dir.exists():
        logging.warning("No taxa_profiles directory; skipping")
        return
    excel_files = list(taxa_dir.glob("result_*.xlsx"))
    dfs: List[pd.DataFrame] = []
    for fp in excel_files:
        sid = fp.name.split("_ANI_")[1].split(".xlsx")[0]
        try:
            sheets = pd.read_excel(fp, sheet_name=None)
        except Exception as e:
            logging.error(f"Failed reading {fp}: {e}")
            continue
        for df in sheets.values():
            if df.empty:
                continue
            df["sample_id"] = sid
            if "organism_name" in df.columns:
                df["organism_id"] = df["organism_name"].str.extract(
                    r"^(GC[AF]_\d+\.\d+)", expand=False)
            df["tax_id"] = -1
            for col in EXPECTED_TAXA_COLS:
                if col not in df.columns:
                    df[col] = False if col == "in_sample_est" else 0
            dfs.append(df[EXPECTED_TAXA_COLS])
    if dfs:
        write_partitioned_parquet(pd.concat(dfs, ignore_index=True),
                                  "taxa_profiles/profiles")

# ▸ Signature archives (AA & DNA)
SIG_MANIFEST_COLS  = ["internal_location", "md5", "md5short", "ksize",
                      "moltype", "num", "scaled", "n_hashes",
                      "with_abundance", "name", "filename",
                      "sample_id", "archive_file"]
SIG_SIGNATURE_COLS = ["md5", "sample_id", "hash_function", "molecule",
                      "filename", "class", "email", "license", "ksize",
                      "seed", "max_hash", "num_mins", "signature_size",
                      "has_abundances", "archive_file"]
SIG_MINS_COLS      = ["sample_id", "md5", "min_hash", "abundance",
                      "position", "ksize"]

def _parse_zip_task(args):
    """Standalone top‑level fn so it can be pickled by ProcessPool."""
    zip_path, sig_type = args
    import pandas as pd, json, gzip, tempfile, zipfile
    from pathlib import Path

    manifests, signatures, mins = [], [], []
    fname = Path(zip_path).name
    if fname.endswith(".unitigs.fa.sig.zip"):
        sid = fname.replace(".unitigs.fa.sig.zip", "")
    elif ".unitigs.fa_sketch_" in fname:
        sid = fname.split(".unitigs.fa_sketch_")[0]
    else:
        sid = fname.replace(".zip", "")

    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp)
        mani_fp = Path(tmp) / "SOURMASH-MANIFEST.csv"
        if not mani_fp.exists():
            return manifests, signatures, mins
        df_mani = pd.read_csv(mani_fp, skiprows=1)
        df_mani["sample_id"] = sid
        df_mani["archive_file"] = fname
        manifests.extend(df_mani.to_dict("records"))

        for row in df_mani.to_dict("records"):
            sig_fp = Path(tmp) / row["internal_location"]
            if not sig_fp.exists():
                continue
            raw = gzip.open(sig_fp, "rb").read().decode() if sig_fp.suffix == ".gz" \
                  else sig_fp.read_text()
            try:
                js = json.loads(raw)
                if isinstance(js, list):
                    js = js[0]
            except json.JSONDecodeError:
                continue

            sig_info = {
                "md5": row["md5"], "sample_id": sid,
                "hash_function": js.get("hash_function", ""),
                "molecule": "", "filename": js.get("filename", ""),
                "class": js.get("class", ""), "email": js.get("email", ""),
                "license": js.get("license", ""), "ksize": 0,
                "seed": 0, "max_hash": 0, "num_mins": 0,
                "signature_size": 0, "has_abundances": False,
                "archive_file": fname
            }
            mins_list, abund = [], []
            if js.get("signatures"):
                s0 = js["signatures"][0]
                sig_info.update({
                    "molecule": s0.get("molecule", ""),
                    "ksize": s0.get("ksize", 0),
                    "seed": s0.get("seed", 0),
                    "max_hash": s0.get("max_hash", 0)
                })
                mins_list = s0.get("mins", [])
                abund     = s0.get("abundances", [])
                sig_info["num_mins"]       = len(mins_list)
                sig_info["signature_size"] = len(str(mins_list))
                sig_info["has_abundances"] = bool(abund)
            signatures.append(sig_info)

            if mins_list:
                if abund and len(abund) == len(mins_list):
                    for pos, (mh, ab) in enumerate(zip(mins_list, abund)):
                        mins.append({
                            "sample_id": sid, "md5": row["md5"],
                            "min_hash": int(mh), "abundance": int(ab),
                            "position": pos, "ksize": sig_info["ksize"]
                        })
                else:
                    for pos, mh in enumerate(mins_list):
                        mins.append({
                            "sample_id": sid, "md5": row["md5"],
                            "min_hash": int(mh), "abundance": 1,
                            "position": pos, "ksize": sig_info["ksize"]
                        })
    return manifests, signatures, mins


def process_signature_files(sig_dir: Path,
                            sig_type: str,
                            batch_size: int,
                            procs: int):
    """
    Parse *every* .zip in sig_dir in parallel **across processes**
    and stream results to Parquet.

    Parameters
    ----------
    sig_dir     : Path      directory with *.zip
    sig_type    : str       'sigs_aa' or 'sigs_dna'
    batch_size  : int       flush Parquet when mins rows reach this
    procs       : int       worker processes in the ProcessPool
    """
    if not sig_dir or not sig_dir.exists():
        logging.warning(f"No {sig_type} directory; skipping")
        return

    zip_files = list(sig_dir.glob("*.zip"))
    if not zip_files:
        return

    manifests, signatures, mins = [], [], []

    with ProcessPoolExecutor(max_workers=procs) as pool:
        tasks = pool.map(_parse_zip_task,
                         ((str(z), sig_type) for z in zip_files))
        for m, s, mn in tqdm(tasks, total=len(zip_files),
                             desc=f"{sig_type} zips", position=5, leave=False):
            manifests.extend(m); signatures.extend(s); mins.extend(mn)
            if len(mins) >= batch_size:
                _flush_signature_buffers(manifests, signatures, mins,
                                         sig_type)
    _flush_signature_buffers(manifests, signatures, mins, sig_type)


def _flush_signature_buffers(manifests, signatures, mins, sig_type):
    """Write the three signature tables and empty the buffers."""
    if manifests:
        write_partitioned_parquet(
            pd.DataFrame(manifests)[SIG_MANIFEST_COLS],
            f"{sig_type}/manifests")
        manifests.clear()
    if signatures:
        write_partitioned_parquet(
            pd.DataFrame(signatures)[SIG_SIGNATURE_COLS],
            f"{sig_type}/signatures")
        signatures.clear()
    if mins:
        write_partitioned_parquet(
            pd.DataFrame(mins)[SIG_MINS_COLS],
            f"{sig_type}/signature_mins")
        mins.clear()

# ▸ One‑off files (taxonomy map & geo) – unchanged
def process_taxonomy_mapping(data_dir: Path):
    tsv  = list(data_dir.glob("*.tsv"))
    txt  = list(data_dir.glob("*taxonomy*.txt"))
    tab  = list(data_dir.glob("*taxonomy*.tab"))
    files = tsv + txt + tab
    if not files:
        logging.warning("No taxonomy mapping files found")
        return
    dfs = []
    for fp in files:
        try:
            df = pd.read_csv(fp, sep=None, engine="python")
        except Exception as e:
            logging.error(f"Failed {fp}: {e}")
            continue
        genome_col = next((c for c in df.columns
                           if re.search(r"(genome|accession)", c, re.I)), None)
        taxid_col  = next((c for c in df.columns
                           if re.search(r"tax(id)?", c, re.I)), None)
        if not genome_col or not taxid_col:
            logging.warning(f"Skip {fp}: no genome/taxid cols")
            continue
        df = df.rename(columns={genome_col: "genome_id",
                                taxid_col: "taxid"})
        df = df[["genome_id", "taxid"]].dropna()
        df["source_file"] = fp.name
        dfs.append(df)
    if dfs:
        write_partitioned_parquet(pd.concat(dfs, ignore_index=True),
                                  "taxonomy_mapping/mappings")

GEO_EXPECTED = [
    'accession', 'attribute_name', 'attribute_value', 'lat_lon',
    'palm_virome', 'elevation', 'center_name', 'country',
    'biome', 'confidence', 'sample_id'
]
def process_geographical_location_data(data_dir: Path):
    patterns = ["*geographical_location*.csv", "*geo_location*.csv",
                "*biosample_geographical*.csv", "output.csv"]
    files: List[Path] = []
    for pat in patterns:
        files.extend(data_dir.glob(pat))
    files = list(dict.fromkeys(files))
    if not files:
        logging.warning("No geographical CSVs found")
        return

    dfs = []
    for fp in files:
        try:
            df_iter = pd.read_csv(fp, dtype=str, chunksize=50_000,
                                  na_filter=False, keep_default_na=False)
        except pd.errors.EmptyDataError:
            continue
        except Exception:
            try:    # tiny file → read whole
                df_iter = [pd.read_csv(fp, dtype=str,
                                       na_filter=False, keep_default_na=False)]
            except Exception as e:
                logging.error(f"Fail geo {fp}: {e}")
                continue
        for chunk in df_iter:
            if chunk.empty:
                continue
            for col in GEO_EXPECTED:
                if col not in chunk.columns:
                    chunk[col] = None
            dfs.append(chunk[GEO_EXPECTED])
    if dfs:
        write_partitioned_parquet(pd.concat(dfs, ignore_index=True),
                                  "geographical_location_data/locations")


# ═════════════════════════════════════════════════════════════════════
# Consumer
# ═════════════════════════════════════════════════════════════════════
def consumer_loop(q: mp.JoinableQueue,
                  batch_size: int,
                  zip_workers: int,
                  tracker: Progress):
    while True:
        item = q.get()
        if item is None:          # sentinel
            q.task_done()
            break

        tracker.inc("processing", +1)
        func, taxa, aa, dna, tmp = item
        try:
            process_functional_profiles(Path(func))
            process_gather_files(Path(func))
            process_taxa_profiles(Path(taxa) if taxa else None)
            process_signature_files(Path(aa)  if aa  else None,
                                    "sigs_aa", batch_size, zip_workers)
            process_signature_files(Path(dna) if dna else None,
                                    "sigs_dna", batch_size, zip_workers)
            tracker.inc("completed", +1)
        except Exception as e:
            logging.exception(f"Consumer failed on {tmp}: {e}")
            tracker.inc("failed", +1)
        finally:
            tracker.inc("processing", -1)
            if Config.CLEANUP_TEMP_FILES:
                shutil.rmtree(tmp, ignore_errors=True)
            q.task_done()


# ═════════════════════════════════════════════════════════════════════
# CLI / main
# ═════════════════════════════════════════════════════════════════════
def parse_args():
    ap = argparse.ArgumentParser(
        description="Logan archive → partitioned Parquet exporter")
    ap.add_argument("--data-dir", default=Config.DATA_DIR,
                    help="Directory with *.tar.gz archives")
    ap.add_argument("--staging-dir", default=str(STAGING_ROOT),
                    help="Output directory for Parquet shards")
    ap.add_argument("--zip-workers", type=int, default=Config.MAX_WORKERS,
                    help="ThreadPool size for signature zip extraction")
    ap.add_argument("--batch-size", type=int, default=Config.BATCH_SIZE,
                    help="Flush signature mins every N rows")
    ap.add_argument("--producers", type=int, default=DEFAULT_PRODUCERS)
    ap.add_argument("--consumers", type=int, default=DEFAULT_CONSUMERS)
    ap.add_argument("--buffer-size", type=int, default=DEFAULT_BUFFER)
    ap.add_argument("--dash-off", action="store_true",
                    help="Disable live dashboard")
    return ap.parse_args()


def setup_logger():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    Path("logs").mkdir(exist_ok=True)
    log_file = Path("logs") / f"export_to_parquet_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(processName)s  %(message)s",
        handlers=[logging.FileHandler(log_file),
                  logging.StreamHandler(sys.stderr)]
    )
    logging.info(f"Log file: {log_file}")


def main():
    args = parse_args()

    global STAGING_ROOT
    STAGING_ROOT = Path(args.staging_dir).resolve()
    STAGING_ROOT.mkdir(parents=True, exist_ok=True)
    os.environ["STAGING_DIR"] = str(STAGING_ROOT)     # visible to children
    setup_logger()

    # ── locate archives, smallest → largest ─────────────────────────
    archives = sorted(Path(args.data_dir).glob("*.tar.gz"),
                      key=lambda p: p.stat().st_size)
    if not archives:
        raise SystemExit(f"No *.tar.gz files found in {args.data_dir}")
    logging.info(f"{len(archives)} archives discovered in {args.data_dir}")

    # ── shared progress tracker & dashboard ─────────────────────────
    tracker = Progress(len(archives))
    stop_evt = mp.Event()
    dir_q    = mp.JoinableQueue(maxsize=args.buffer_size)
    if not args.dash_off:
        dash_thread = Thread(target=_dashboard,
                             args=(tracker, dir_q, stop_evt),
                             daemon=True)
        dash_thread.start()

    # ── spawn consumer pool ─────────────────────────────────────────
    consumers = []
    for _ in range(args.consumers):
        p = mp.Process(target=consumer_loop,
                       args=(dir_q, args.batch_size, args.zip_workers, tracker))
        p.start()
        consumers.append(p)

    # ── producer pool: untar archives ───────────────────────────────
    with ProcessPoolExecutor(max_workers=args.producers) as ex:
        futures = {ex.submit(extract_nested_archives, str(a)): a
                   for a in archives}
        for fut in tqdm(as_completed(futures), total=len(futures),
                        desc="Extracting"):
            try:
                res = fut.result()
                tracker.inc("extracted", +1)
                dir_q.put(res)              # blocks if buffer full
            except Exception as e:
                logging.exception(f"Extraction failed: {e}")
                tracker.inc("failed", +1)

    # ── stop consumers ──────────────────────────────────────────────
    for _ in consumers:
        dir_q.put(None)
    dir_q.join()
    for p in consumers:
        p.join()

    # ── stop dashboard ─────────────────────────────────────────────
    stop_evt.set()
    if not args.dash_off:
        dash_thread.join()

    # one‑off files (not inside each archive) ────────────────────────
    process_taxonomy_mapping(Path(args.data_dir))
    process_geographical_location_data(Path(args.data_dir))

    print(f"\n🎉  Export finished. Parquet files are in {STAGING_ROOT}")


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
