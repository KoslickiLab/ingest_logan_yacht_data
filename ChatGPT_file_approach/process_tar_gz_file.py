#!/usr/bin/env python
"""
process_tar_gz_file.py
──────────────────────
Single‑threaded conversion of ONE *.tar.gz archive to partitioned Parquet.

Business‑logic functions are *inline* and taken verbatim from the original
export_to_parquet_progress.py (© your existing project).  Only concurrency
has been stripped so that you may launch many copies with GNU parallel,
SLURM array jobs, etc.

Usage
-----
    python process_tar_gz_file.py ARCHIVE.tar.gz
                                  --staging-dir /scratch/staging
                                  --batch-size   50000
                                  --success-flag-dir /path/to/flags
                                  --log-level INFO

Return‑code 0  → already done *or* completed successfully.  Non‑zero → error.
"""

from __future__ import annotations

# ── stdlib ───────────────────────────────────────────────────────────
import argparse, gzip, json, logging, os, re, shutil, sys, tarfile, tempfile, \
       uuid, zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# ── 3rd‑party ────────────────────────────────────────────────────────
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# ═════════════════════════════════════════════════════════════════════
# CONSTANTS & ENVIRONMENT
# ═════════════════════════════════════════════════════════════════════
ROW_GROUP_SIZE = 128 * 1024 * 1024               # 128MiB

SCRATCH_TMP_ROOT = Path(os.getenv("SCRATCH_TMPDIR", "/scratch/tmp")).resolve()
SCRATCH_TMP_ROOT.mkdir(parents=True, exist_ok=True)
for _v in ("TMPDIR", "TEMP", "TMP"):             # propagate to libs/children
    os.environ[_v] = str(SCRATCH_TMP_ROOT)
tempfile.tempdir = str(SCRATCH_TMP_ROOT)         # force stdlib

# ── helper: Parquet writer (unchanged) ───────────────────────────────
def write_partitioned_parquet(df: pd.DataFrame, logical_path: str) -> None:
    """Append *df* to STAGING_ROOT/logical_path/part‑<uuid>.parquet."""
    if df is None or df.empty:
        return
    table_dir = STAGING_ROOT / logical_path
    table_dir.mkdir(parents=True, exist_ok=True)
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype("string")
    out_file = table_dir / f"part-{uuid.uuid4()}.parquet"
    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        where=out_file,
        compression="zstd",
        use_dictionary=True,
        row_group_size=ROW_GROUP_SIZE,
        data_page_size=1_048_576,
        write_statistics=True,
    )

# ═════════════════════════════════════════════════════════════════════
# STAGE‑1  –  ARCHIVE EXTRACTION (unchanged)
# ═════════════════════════════════════════════════════════════════════
def extract_nested_archives(archive_path: str):
    """
    Untar *archive_path* → tmp dir, return tuple
    (func_profiles_dir, taxa_profiles_dir or "", sigs_aa_dir or "",
     sigs_dna_dir or "", temp_dir)
    """
    temp_dir = tempfile.mkdtemp(prefix="logan_")      # now on /scratch
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(temp_dir)

    func = taxa = sigs_aa = sigs_dna = None
    for root, dirs, _ in os.walk(temp_dir):
        if "func_profiles" in dirs:
            parent = Path(root)
            func   = parent / "func_profiles"
            taxa   = parent / "taxa_profiles" if "taxa_profiles" in dirs else None
            sigs_aa = parent / "sigs_aa" if "sigs_aa" in dirs else None
            sigs_dna = parent / "sigs_dna" if "sigs_dna" in dirs else None
            break
    if func is None:
        raise FileNotFoundError(f"{archive_path}: func_profiles missing")
    return str(func), str(taxa or ""), str(sigs_aa or ""), str(sigs_dna or ""), temp_dir

# ═════════════════════════════════════════════════════════════════════
# STAGE‑2  –  BUSINESS LOGIC   (all functions verbatim, no parallelism)
# ═════════════════════════════════════════════════════════════════════
def process_functional_profiles(func_profiles_dir: Path):
    files = list(func_profiles_dir.glob("*_functional_profile"))
    dfs: List[pd.DataFrame] = []
    for fp in tqdm(files, desc="functional profiles", leave=False):
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
    for fp in tqdm(files, desc="gather files", leave=False):
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
    for fp in tqdm(excel_files, desc="taxa profiles", leave=False):
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

# ▸ Signatures  (single‑threaded variant)
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

def _parse_zip_file(zip_path: Path):
    """Parse ONE .sig.zip → (manifest_rows, signature_rows, mins_rows)."""
    manifests, signatures, mins = [], [], []
    fname = zip_path.name
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
            raw = gzip.open(sig_fp, "rb").read().decode() \
                  if sig_fp.suffix == ".gz" else sig_fp.read_text()
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

def _flush_signature_buffers(manifests, signatures, mins, sig_type):
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

def process_signature_files(sig_dir: Path,
                            sig_type: str,
                            batch_size: int):
    """
    Sequentially parse every *.zip in *sig_dir* and stream results to Parquet.
    """
    if not sig_dir or not sig_dir.exists():
        logging.warning(f"No {sig_type} directory; skipping")
        return

    zip_files = list(sig_dir.glob("*.zip"))
    if not zip_files:
        return
    manifests, signatures, mins = [], [], []
    for zf in tqdm(zip_files, desc=f"{sig_type} zips", leave=False):
        m, s, mn = _parse_zip_file(zf)
        manifests.extend(m); signatures.extend(s); mins.extend(mn)
        if len(mins) >= batch_size:
            _flush_signature_buffers(manifests, signatures, mins, sig_type)
    _flush_signature_buffers(manifests, signatures, mins, sig_type)

# ═════════════════════════════════════════════════════════════════════
# CLI / MAIN
# ═════════════════════════════════════════════════════════════════════
def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Process a single Logan .tar.gz")
    ap.add_argument("archive", help="Path to the .tar.gz archive")
    ap.add_argument("--staging-dir", default="data_staging",
                    help="Destination root for Parquet shards")
    ap.add_argument("--batch-size", type=int, default=50_000,
                    help="Flush signature mins every N rows")
    ap.add_argument("--success-flag-dir",
                    help="Directory where <archive>.done is placed "
                         "(defaults to same folder as the archive)")
    ap.add_argument("--log-level", default="INFO",
                    choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return ap.parse_args()

def _setup_logger(level: str, archive: Path) -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    Path("logs").mkdir(exist_ok=True)
    log_file = Path("logs") / f"process_tar_gz_{archive.stem}_{ts}.log"
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s  %(levelname)-8s %(message)s",
        handlers=[logging.FileHandler(log_file),
                  logging.StreamHandler(sys.stderr)]
    )
    logging.info(f"Log file: {log_file}")

def main() -> None:
    args     = _parse_args()
    archive  = Path(args.archive).resolve()

    if not archive.exists():
        sys.exit(f"❌ archive not found: {archive}")

    # success flag ---------------------------------------------------
    flag_dir  = Path(args.success_flag_dir or archive.parent)
    flag_dir.mkdir(parents=True, exist_ok=True)
    flag_file = flag_dir / f"{archive.name}.done"
    if flag_file.exists():
        print(f"✔︎ {archive.name} already processed — skipping.")
        sys.exit(0)

    # logging & environment -----------------------------------------
    _setup_logger(args.log_level, archive)
    global STAGING_ROOT
    STAGING_ROOT = Path(args.staging_dir).resolve()
    STAGING_ROOT.mkdir(parents=True, exist_ok=True)
    os.environ["STAGING_DIR"] = str(STAGING_ROOT)

    logging.info(f"▶ Starting {archive.name}")
    tmp_dir = None
    try:
        # 1. extract -------------------------------------------------
        logging.info("Step 1/5  extract archive …")
        func_d, taxa_d, aa_d, dna_d, tmp_dir = extract_nested_archives(str(archive))

        # 2. functional profiles + gather ---------------------------
        logging.info("Step 2/5  functional profiles …")
        process_functional_profiles(Path(func_d))
        process_gather_files(Path(func_d))

        # 3. taxa profiles ------------------------------------------
        logging.info("Step 3/5  taxa profiles …")
        process_taxa_profiles(Path(taxa_d) if taxa_d else None)

        # 4. signature archives (AA & DNA) --------------------------
        logging.info("Step 4/5  signature archives (AA) …")
        process_signature_files(Path(aa_d) if aa_d else None,
                                "sigs_aa", args.batch_size)
        logging.info("Step 4/5  signature archives (DNA) …")
        process_signature_files(Path(dna_d) if dna_d else None,
                                "sigs_dna", args.batch_size)

        # 5. success flag -------------------------------------------
        flag_file.touch()
        logging.info(f"✅ success — flag written to {flag_file}")

    except Exception as exc:
        logging.exception(f"✖︎ failed while processing {archive}: {exc}")
        sys.exit(1)

    finally:
        if tmp_dir and Path(tmp_dir).exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
            logging.debug(f"scratch cleaned: {tmp_dir}")

    # footer: how many archives remain in the folder ----------------
    remaining = [p for p in archive.parent.glob("*.tar.gz")
                 if not (archive.parent / f"{p.name}.done").exists()]
    logging.info(f"📦 {len(remaining)} archives still pending in {archive.parent}")

if __name__ == "__main__":
    main()
