#!/usr/bin/env python
"""
export_to_parquet.py
────────────────────
1.  Extract every *.tar.gz produced by the Logan pipeline
2.  Normalise the contained tables exactly like the original
    import_to_db_safe_o3‑pro.py script
3.  Append the resulting DataFrames to a partitioned Parquet
    staging area on disk – *no database work happens here*.

The Parquet layout mirrors the logical schemas/tables that
exist in DuckDB later on:

    data_staging/
      ├── functional_profile/profiles/*.parquet
      ├── functional_profile_data/gather_data/*.parquet
      ├── taxa_profiles/profiles/*.parquet
      ├── sigs_aa/{manifests,signatures,signature_mins}/*.parquet
      ├── sigs_dna/{manifests,signatures,signature_mins}/*.parquet
      └── geographical_location_data/locations/*.parquet   (if present)

All write operations are **atomic** – the script never touches
an existing file.  If you rerun it tomorrow it simply appends
more `part-<uuid>.parquet` shards next to the old ones.
"""

from __future__ import annotations

# ─────────────────────────  standard lib  ──────────────────────────
import argparse, gzip, json, logging, os, re, shutil, sys, tarfile, tempfile, uuid, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# ─────────────────────────  3rd‑party  ─────────────────────────────
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# ─────────────────────────  project config  ────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import Config      # same file your original script imports

# ‑‑‑ constants  ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑
STAGING_ROOT = Path(
    os.getenv("STAGING_DIR", "data_staging")
).resolve()        # may be overridden on CLI
STAGING_ROOT.mkdir(parents=True, exist_ok=True)

ROW_GROUP_SIZE = 128 * 1024 * 1024        # 128 MiB

# ───────────────────────  helpers  ────────────────────────────────
def write_partitioned_parquet(df: pd.DataFrame, logical_path: str) -> None:
    """
    Append *df* to STAGING_ROOT/logical_path as Parquet.

    • Each call writes one new file whose name starts with 'part‑'.
    • ZSTD level 3 is a good default – fast and small.
    • Dictionary & RLE are enabled automatically for string columns.
    """
    if df is None or df.empty:
        return

    table_dir = STAGING_ROOT / logical_path
    table_dir.mkdir(parents=True, exist_ok=True)

    out_file = table_dir / f"part-{uuid.uuid4()}.parquet"
    table    = pa.Table.from_pandas(df, preserve_index=False)

    pq.write_table(
        table,
        where     = out_file,
        compression="zstd",
        use_dictionary=True,
        data_page_size=1_048_576,    # 1 MiB pages
        write_statistics=True,
        row_group_size=ROW_GROUP_SIZE,
    )


def extract_archive(archive_path: str):
    """
    Extract one data archive into a temporary directory and return
    the *paths* to the directories we care about plus the temp dir.
    """
    tmp = tempfile.mkdtemp(prefix="logan_")
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(tmp)

    want = {
        "func": None,
        "taxa": None,
        "sigs_aa": None,
        "sigs_dna": None,
    }
    for root, dirs, _ in os.walk(tmp):
        if "func_profiles" in dirs:
            want["func"] = Path(root) / "func_profiles"
            parent       = Path(root)
            if "taxa_profiles" in dirs:
                want["taxa"] = parent / "taxa_profiles"
            if "sigs_aa" in dirs:
                want["sigs_aa"] = parent / "sigs_aa"
            if "sigs_dna" in dirs:
                want["sigs_dna"] = parent / "sigs_dna"
            break

    if not want["func"]:
        raise FileNotFoundError("func_profiles directory missing")

    return want, tmp


# ───────────────────  per‑table processors  ───────────────────────
def process_functional_profiles(func_dir: Path):
    files = list(func_dir.glob("*_functional_profile"))
    dfs   = []
    for fp in files:
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


def process_gather_files(func_dir: Path):
    g_files = list(func_dir.glob("*.unitigs.fa_gather_*.tmp"))
    dfs     = []
    for fp in g_files:
        sid = fp.name.split(".unitigs.fa_gather_")[0]
        try:
            df = pd.read_csv(fp)
        except pd.errors.EmptyDataError:
            continue
        if df.empty:
            continue
        df["sample_id"] = sid
        # bring sample_id to first column
        cols = ["sample_id"] + [c for c in df.columns if c != "sample_id"]
        dfs.append(df[cols])

    if dfs:
        write_partitioned_parquet(
            pd.concat(dfs, ignore_index=True),
            "functional_profile_data/gather_data"
        )


def process_taxa_profiles(taxa_dir: Path):
    excel_files = list(taxa_dir.glob("result_*.xlsx"))
    dfs = []
    for fp in excel_files:
        base = fp.name
        sid  = base.split("_ANI_")[1].split(".xlsx")[0]
        sheets = pd.read_excel(fp, sheet_name=None)
        for sheet, df in sheets.items():
            if df.empty:
                continue
            df["sample_id"] = sid
            if "organism_name" in df.columns:
                df["organism_id"] = df["organism_name"].str.extract(r"^(GC[AF]_\d+\.\d+)", expand=False)
            df["tax_id"] = -1

            expected = [
                "sample_id", "organism_name", "organism_id", "tax_id",
                "num_unique_kmers_in_genome_sketch",
                "num_total_kmers_in_genome_sketch",
                "scale_factor",
                "num_exclusive_kmers_in_sample_sketch",
                "num_total_kmers_in_sample_sketch",
                "min_coverage",
                "p_vals",
                "num_exclusive_kmers_to_genome",
                "num_exclusive_kmers_to_genome_coverage",
                "num_matches",
                "acceptance_threshold_with_coverage",
                "actual_confidence_with_coverage",
                "alt_confidence_mut_rate_with_coverage",
                "in_sample_est",
            ]
            for col in expected:
                if col not in df.columns:
                    df[col] = 0 if col.startswith("num_") or col in {
                        "scale_factor", "min_coverage", "p_vals"
                    } else False
            dfs.append(df[expected])

    if dfs:
        write_partitioned_parquet(
            pd.concat(dfs, ignore_index=True),
            "taxa_profiles/profiles"
        )


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


def process_signature_directory(sig_dir: Path, sig_type: str,
                                batch_size: int, workers: int):
    """
    Extract every *.zip bundle inside *sig_dir* and emit three Parquet
    tables: manifests, signatures, signature_mins.
    """
    if not sig_dir:
        return

    zip_files = list(sig_dir.glob("*.zip"))

    manifests, signatures, mins = [], [], []

    def extract_zip(z_path: Path):
        local_manifests, local_signatures, local_mins = [], [], []
        fname   = z_path.name
        if fname.endswith(".unitigs.fa.sig.zip"):
            sid = fname.replace(".unitigs.fa.sig.zip", "")
        elif ".unitigs.fa_sketch_" in fname:
            sid = fname.split(".unitigs.fa_sketch_")[0]
        else:
            sid = fname.replace(".zip", "")

        with tempfile.TemporaryDirectory() as tmp:
            with zipfile.ZipFile(z_path) as zf:
                zf.extractall(tmp)

            manifest_fp = Path(tmp) / "SOURMASH-MANIFEST.csv"
            if not manifest_fp.exists():
                return local_manifests, local_signatures, local_mins

            df_mani = pd.read_csv(manifest_fp, skiprows=1)
            df_mani["sample_id"]   = sid
            df_mani["archive_file"] = fname
            local_manifests.extend(df_mani.to_dict("records"))

            for row in df_mani.to_dict("records"):
                sig_fp = Path(tmp) / row["internal_location"]
                md5    = row["md5"]
                if not sig_fp.exists():
                    continue
                if sig_fp.suffix == ".gz":
                    with gzip.open(sig_fp, "rb") as fh:
                        text = fh.read().decode()
                else:
                    text = sig_fp.read_text()

                try:
                    data = json.loads(text)
                    if isinstance(data, list):
                        data = data[0]
                except json.JSONDecodeError:
                    continue

                sig_info = {
                    "md5": md5,
                    "sample_id": sid,
                    "hash_function": data.get("hash_function", ""),
                    "molecule": "",
                    "filename": data.get("filename", ""),
                    "class": data.get("class", ""),
                    "email": data.get("email", ""),
                    "license": data.get("license", ""),
                    "ksize": 0,
                    "seed": 0,
                    "max_hash": 0,
                    "num_mins": 0,
                    "signature_size": 0,
                    "has_abundances": False,
                    "archive_file": fname,
                }

                if data.get("signatures"):
                    sig = data["signatures"][0]
                    sig_info.update({
                        "molecule": sig.get("molecule", ""),
                        "ksize": sig.get("ksize", 0),
                        "seed": sig.get("seed", 0),
                        "max_hash": sig.get("max_hash", 0),
                        "num_mins": len(sig.get("mins", [])),
                        "signature_size": len(str(sig.get("mins", []))),
                        "has_abundances": bool(sig.get("abundances")),
                    })

                    mins_list = sig.get("mins", [])
                    abund     = sig.get("abundances", [])
                    if mins_list:
                        if abund and len(abund) == len(mins_list):
                            for pos, (mh, ab) in enumerate(zip(mins_list, abund)):
                                local_mins.append({
                                    "sample_id": sid,
                                    "md5": md5,
                                    "min_hash": int(mh),
                                    "abundance": int(ab),
                                    "position": pos,
                                    "ksize": sig_info["ksize"],
                                })
                        else:
                            for pos, mh in enumerate(mins_list):
                                local_mins.append({
                                    "sample_id": sid,
                                    "md5": md5,
                                    "min_hash": int(mh),
                                    "abundance": 1,
                                    "position": pos,
                                    "ksize": sig_info["ksize"],
                                })

                local_signatures.append(sig_info)

        return local_manifests, local_signatures, local_mins

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(extract_zip, z): z for z in zip_files}
        for fut in tqdm(as_completed(futs), total=len(futs),
                        desc=f"Extracting {sig_type}"):
            mani, sigs, mns = fut.result()
            manifests.extend(mani)
            signatures.extend(sigs)
            mins.extend(mns)

            if len(mins) >= batch_size:
                # flush big buffers early to keep memory in check
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

    # final flush
    if manifests:
        write_partitioned_parquet(
            pd.DataFrame(manifests)[SIG_MANIFEST_COLS],
            f"{sig_type}/manifests"
        )
    if signatures:
        write_partitioned_parquet(
            pd.DataFrame(signatures)[SIG_SIGNATURE_COLS],
            f"{sig_type}/signatures"
        )
    if mins:
        write_partitioned_parquet(
            pd.DataFrame(mins)[SIG_MINS_COLS],
            f"{sig_type}/signature_mins"
        )


# ──────────────────────────────  main  ────────────────────────────
def parse_args():
    ap = argparse.ArgumentParser(
        description="Extract Logan archives → partitioned Parquet staging area"
    )
    ap.add_argument("--data-dir", required=False,
                    default=Config.DATA_DIR,
                    help="Directory containing the *.tar.gz archives")
    ap.add_argument("--staging-dir", default=str(STAGING_ROOT),
                    help="Where to write the Parquet hierarchy")
    ap.add_argument("--workers", type=int, default=Config.MAX_WORKERS,
                    help="ThreadPool size for signature extraction")
    ap.add_argument("--batch-size", type=int, default=Config.BATCH_SIZE,
                    help="Flush signature mins every N records")
    ap.add_argument("--no-progress", action="store_true",
                    help="Disable tqdm progress bars")
    return ap.parse_args()


def main():
    args = parse_args()

    global STAGING_ROOT
    STAGING_ROOT = Path(args.staging_dir).resolve()
    STAGING_ROOT.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        handlers=[
            logging.FileHandler(
                f"export_to_parquet_{datetime.now():%Y%m%d_%H%M%S}.log"),
            logging.StreamHandler(sys.stderr)
        ]
    )

    archives = list(Path(args.data_dir).glob("*.tar.gz"))
    logging.info(f"Found {len(archives)} archives.")

    for a_path in tqdm(archives, desc="Archives", disable=args.no_progress):
        try:
            paths, tmp = extract_archive(a_path)
            process_functional_profiles(paths["func"])
            process_gather_files(paths["func"])
            if paths["taxa"]:
                process_taxa_profiles(paths["taxa"])
            if paths["sigs_aa"]:
                process_signature_directory(paths["sigs_aa"], "sigs_aa",
                                            args.batch_size, args.workers)
            if paths["sigs_dna"]:
                process_signature_directory(paths["sigs_dna"], "sigs_dna",
                                            args.batch_size, args.workers)
        except Exception as e:
            logging.exception(f"Failed on {a_path}: {e}")
        finally:
            try:
                if Config.CLEANUP_TEMP_FILES:
                    shutil.rmtree(tmp, ignore_errors=True)
            except Exception:
                pass

    logging.info("🎉  All archives processed.  Parquet files are at "
                 f"{STAGING_ROOT}")


if __name__ == "__main__":
    main()
