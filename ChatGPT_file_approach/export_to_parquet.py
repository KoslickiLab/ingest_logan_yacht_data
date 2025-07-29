#!/usr/bin/env python
"""
export_to_parquet.py
────────────────────
Extract Logan functional‑profile archives and write the normalised
tables to **partitioned Parquet files** instead of a DuckDB database.

Column sets and data‑cleaning follow import_to_db.py exactly; the
only difference is the storage backend.

"""
from __future__ import annotations

# ────────── stdlib
import argparse, gzip, json, logging, os, re, shutil, sys, tarfile, tempfile, uuid, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List

# ────────── 3rd‑party
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# ────────── project config (unchanged)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config                      # noqa: E402  (before black)

# ══════════════════════════════════════════════════════════════════════
# GLOBALS
# ══════════════════════════════════════════════════════════════════════
ROW_GROUP_SIZE = 128 * 1024 * 1024          # 128MiB row groups
STAGING_ROOT   = Path(os.getenv("STAGING_DIR", "data_staging")).resolve()
STAGING_ROOT.mkdir(parents=True, exist_ok=True)

# ══════════════════════════════════════════════════════════════════════
# Helper – Parquet writer
# ══════════════════════════════════════════════════════════════════════
def write_partitioned_parquet(df: pd.DataFrame, logical_path: str) -> None:
    """
    Append *df* to STAGING_ROOT/logical_path/part‑<uuid>.parquet.

    Files are compressed with ZSTD‑3 and dictionary encoding so the
    resulting staging area is typically ⅓ of CSV size.
    """
    if df is None or df.empty:
        return
    table_dir = STAGING_ROOT / logical_path
    table_dir.mkdir(parents=True, exist_ok=True)

    # ── force all “object” columns to pandas’ nullable StringDtype ──────────
    #    This turns any stray integers or bytes into strings *before*
    #    PyArrow inspects the data, so type inference is deterministic.
    for col in df.select_dtypes(include=["object"]).columns:
        # astype("string") keeps missing values as <NA>, not the literal "nan"
        df[col] = df[col].astype("string")

    out_file = table_dir / f"part-{uuid.uuid4()}.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)

    pq.write_table(
        table,
        where=out_file,
        compression="zstd",
        use_dictionary=True,
        row_group_size=ROW_GROUP_SIZE,
        data_page_size=1_048_576,          # 1 MiB
        write_statistics=True,
    )

# ══════════════════════════════════════════════════════════════════════
# Archive extraction (unchanged except for return)
# ══════════════════════════════════════════════════════════════════════
def extract_nested_archives(archive_path: str):
    """Extract *.tar.gz → temporary directory, return paths to sub‑dirs."""
    temp_dir = tempfile.mkdtemp(prefix="logan_")
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(temp_dir)

    func, taxa, sigs_aa, sigs_dna = None, None, None, None
    for root, dirs, _ in os.walk(temp_dir):
        if "func_profiles" in dirs:
            func = Path(root) / "func_profiles"
            parent = Path(root)
            if "taxa_profiles" in dirs:
                taxa = parent / "taxa_profiles"
            if "sigs_aa" in dirs:
                sigs_aa = parent / "sigs_aa"
            if "sigs_dna" in dirs:
                sigs_dna = parent / "sigs_dna"
            break
    if not func:
        raise FileNotFoundError("func_profiles directory missing in archive")
    return func, taxa, sigs_aa, sigs_dna, temp_dir

# ══════════════════════════════════════════════════════════════════════
# Functional‑profile & gather handlers  (same cleaning, Parquet writer)
# ══════════════════════════════════════════════════════════════════════
def process_functional_profiles(func_profiles_dir: Path):
    files = list(func_profiles_dir.glob("*_functional_profile"))
    dfs: List[pd.DataFrame] = []
    for fp in tqdm(files, desc="Functional profiles"):
        sid = fp.name.split("_functional_profile")[0]
        try:
            df = pd.read_csv(fp)
        except pd.errors.EmptyDataError:
            continue
        if df.empty:
            continue
        df["sample_id"] = sid
        df = df[["sample_id", "ko_id", "abundance"]]
        dfs.append(df)
    if dfs:
        write_partitioned_parquet(
            pd.concat(dfs, ignore_index=True),
            "functional_profile/profiles"
        )

def process_gather_files(func_profiles_dir: Path):
    files = list(func_profiles_dir.glob("*.unitigs.fa_gather_*.tmp"))
    dfs: List[pd.DataFrame] = []
    for fp in tqdm(files, desc="Gather files"):
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
        write_partitioned_parquet(
            pd.concat(dfs, ignore_index=True),
            "functional_profile_data/gather_data"
        )

# ══════════════════════════════════════════════════════════════════════
# Taxa profiles
# ══════════════════════════════════════════════════════════════════════
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

def process_taxa_profiles(taxa_dir: Path):
    if not taxa_dir:
        logging.warning("No taxa_profiles directory; skipping")
        return
    excel_files = list(taxa_dir.glob("result_*.xlsx"))
    dfs: List[pd.DataFrame] = []
    for fp in tqdm(excel_files, desc="Taxa profiles"):
        sid = fp.name.split("_ANI_")[1].split(".xlsx")[0]
        try:
            sheets = pd.read_excel(fp, sheet_name=None)
        except Exception as e:
            logging.error(f"Failed reading {fp}: {e}")
            continue
        for _, df in sheets.items():
            if df.empty:
                continue
            df["sample_id"] = sid
            if "organism_name" in df.columns:
                df["organism_id"] = df["organism_name"].str.extract(
                    r"^(GC[AF]_\d+\.\d+)", expand=False)
            df["tax_id"] = -1
            for col in EXPECTED_TAXA_COLS:
                if col not in df.columns:
                    default = False if col == "in_sample_est" else 0
                    df[col] = default
            dfs.append(df[EXPECTED_TAXA_COLS])
    if dfs:
        write_partitioned_parquet(
            pd.concat(dfs, ignore_index=True),
            "taxa_profiles/profiles"
        )

# ══════════════════════════════════════════════════════════════════════
# Signature archives (AA & DNA)  – manifest, signatures, mins tables
# ══════════════════════════════════════════════════════════════════════
SIG_MANIFEST_COLS = [
    "internal_location", "md5", "md5short", "ksize", "moltype", "num",
    "scaled", "n_hashes", "with_abundance", "name", "filename",
    "sample_id", "archive_file"
]
SIG_SIGNATURE_COLS = [
    "md5", "sample_id", "hash_function", "molecule", "filename", "class",
    "email", "license", "ksize", "seed", "max_hash", "num_mins",
    "signature_size", "has_abundances", "archive_file"
]
SIG_MINS_COLS = [
    "sample_id", "md5", "min_hash", "abundance", "position", "ksize"
]

def process_signature_files(sig_dir: Path, sig_type: str,
                            batch_size: int, workers: int):
    if not sig_dir:
        logging.warning(f"No {sig_type} directory; skipping")
        return
    zip_files = list(sig_dir.glob("*.zip"))

    manifests, signatures, mins = [], [], []

    def extract_zip(zip_path: Path):
        local_manifests, local_signatures, local_mins = [], [], []
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
                return local_manifests, local_signatures, local_mins
            df_mani = pd.read_csv(mani_fp, skiprows=1)
            df_mani["sample_id"] = sid
            df_mani["archive_file"] = fname
            local_manifests.extend(df_mani.to_dict("records"))

            for row in df_mani.to_dict("records"):
                sig_fp = Path(tmp) / row["internal_location"]
                if not sig_fp.exists():
                    continue
                if sig_fp.suffix == ".gz":
                    with gzip.open(sig_fp, "rb") as fh:
                        raw = fh.read().decode()
                else:
                    raw = sig_fp.read_text()
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
                local_signatures.append(sig_info)

                if mins_list:
                    if abund and len(abund) == len(mins_list):
                        for pos, (mh, ab) in enumerate(zip(mins_list, abund)):
                            local_mins.append({
                                "sample_id": sid, "md5": row["md5"],
                                "min_hash": int(mh), "abundance": int(ab),
                                "position": pos, "ksize": sig_info["ksize"]
                            })
                    else:
                        for pos, mh in enumerate(mins_list):
                            local_mins.append({
                                "sample_id": sid, "md5": row["md5"],
                                "min_hash": int(mh), "abundance": 1,
                                "position": pos, "ksize": sig_info["ksize"]
                            })
        return local_manifests, local_signatures, local_mins

    def flush():
        if manifests:
            write_partitioned_parquet(
                pd.DataFrame(manifests)[SIG_MANIFEST_COLS],
                f"{sig_type}/manifests"
            )
            manifests.clear()
        if signatures:
            write_partitioned_parquet(
                pd.DataFrame(signatures)[SIG_SIGNATURE_COLS],
                f"{sig_type}/signatures"
            )
            signatures.clear()
        if mins:
            write_partitioned_parquet(
                pd.DataFrame(mins)[SIG_MINS_COLS],
                f"{sig_type}/signature_mins"
            )
            mins.clear()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(extract_zip, z): z for z in zip_files}
        for fut in tqdm(as_completed(futs), total=len(futs),
                        desc=f"{sig_type} zips"):
            m, s, mn = fut.result()
            manifests.extend(m); signatures.extend(s); mins.extend(mn)
            if len(mins) >= batch_size:
                flush()
    flush()

# ══════════════════════════════════════════════════════════════════════
# Taxonomy mapping  →  taxonomy_mapping/mappings/*.parquet
# ══════════════════════════════════════════════════════════════════════
def process_taxonomy_mapping(data_dir: Path):
    tsv  = list(data_dir.glob("*.tsv"))
    txt  = list(data_dir.glob("*taxonomy*.txt"))
    tab  = list(data_dir.glob("*taxonomy*.tab"))
    files = tsv + txt + tab
    if not files:
        logging.warning("No taxonomy mapping files found")
        return
    dfs = []
    for fp in tqdm(files, desc="Taxonomy mapping"):
        try:
            df = pd.read_csv(fp, sep=None, engine="python")
        except Exception as e:
            logging.error(f"Failed {fp}: {e}"); continue
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
        write_partitioned_parquet(
            pd.concat(dfs, ignore_index=True),
            "taxonomy_mapping/mappings"
        )

# ══════════════════════════════════════════════════════════════════════
# Geographical location CSVs
# ══════════════════════════════════════════════════════════════════════
GEO_EXPECTED = [
    'accession', 'attribute_name', 'attribute_value', 'lat_lon',
    'palm_virome', 'elevation', 'center_name', 'country',
    'biome', 'confidence', 'sample_id'
]

def process_geographical_location_data(data_dir: Path):
    patterns = [
        "*geographical_location*.csv", "*geo_location*.csv",
        "*biosample_geographical*.csv", "output.csv"
    ]
    files = []
    for pat in patterns:
        files.extend(data_dir.glob(pat))
    files = list(dict.fromkeys(files))
    if not files:
        logging.warning("No geographical CSVs found")
        return
    dfs = []
    for fp in tqdm(files, desc="Geo CSV"):
        try:
            df_iter = pd.read_csv(fp, dtype=str, chunksize=50000,
                                  na_filter=False, keep_default_na=False)
        except pd.errors.EmptyDataError:
            continue
        except Exception:
            # try small file whole
            try:
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
        write_partitioned_parquet(
            pd.concat(dfs, ignore_index=True),
            "geographical_location_data/locations"
        )

# ══════════════════════════════════════════════════════════════════════
# CLI / main
# ══════════════════════════════════════════════════════════════════════
def parse_args():
    ap = argparse.ArgumentParser(
        description="Logan archive → partitioned Parquet exporter")
    ap.add_argument("--data-dir", required=False,
                    default=Config.DATA_DIR,
                    help="Directory with *.tar.gz archives")
    ap.add_argument("--staging-dir", default=str(STAGING_ROOT),
                    help="Output directory for Parquet shards")
    ap.add_argument("--workers", type=int, default=Config.MAX_WORKERS,
                    help="ThreadPool size for signature zip extraction")
    ap.add_argument("--batch-size", type=int, default=Config.BATCH_SIZE,
                    help="Flush signature mins every N rows")
    ap.add_argument("--no-progress", action="store_true",
                    help="Disable tqdm bars")
    return ap.parse_args()

def setup_logger():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = Path("logs"); log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"export_to_parquet_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stderr)
        ]
    )
    logging.info(f"Log file: {log_file}")

def main():
    args = parse_args()
    global STAGING_ROOT
    STAGING_ROOT = Path(args.staging_dir).resolve()
    STAGING_ROOT.mkdir(parents=True, exist_ok=True)
    setup_logger()

    archs = list(Path(args.data_dir).glob("*.tar.gz"))
    logging.info(f"{len(archs)} archives found in {args.data_dir}")

    for a in tqdm(archs, desc="Archives", disable=args.no_progress):
        try:
            func, taxa, aa, dna, tmp = extract_nested_archives(a)
            process_functional_profiles(func)
            process_gather_files(func)
            process_taxa_profiles(taxa)
            process_signature_files(aa, "sigs_aa",
                                    args.batch_size, args.workers)
            process_signature_files(dna, "sigs_dna",
                                    args.batch_size, args.workers)
            if Config.CLEANUP_TEMP_FILES:
                shutil.rmtree(tmp, ignore_errors=True)
        except Exception as e:
            logging.exception(f"Failed archive {a}: {e}")

    # one‑off files (not inside each archive)
    #process_taxonomy_mapping(Path(args.data_dir))
    #process_geographical_location_data(Path(args.data_dir))

    print(f"\n🎉  Export finished. Parquet files are in {STAGING_ROOT}")

if __name__ == "__main__":
    main()
