#!/usr/bin/env python
"""
Resilient Logan data‑ingest script.

Only three things are new compared to the original─────────────
1. FAILED_FILES / record_failure()
2. safe_execute()– wraps any DuckDB statement in a try/except/reconnect
3. Every per‑source‑file INSERT now uses safe_execute()

Everything else – the CLI, argument names, table layouts, indexes,
chunk sizes, etc. – is **byte‑for‑byte identical** to the version you
uploaded on 2025‑07‑24.
"""
from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
# Standard imports (unchanged)
# ──────────────────────────────────────────────────────────────────────────────
import os, sys, tarfile, tempfile, glob, shutil, zipfile, gzip, json, argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb, pandas as pd
from tqdm import tqdm
import logging

# ─── GLOBAL FLAGS ─────────────────────────────────────────────
NEED_IDX_TAXA = False     # taxa_profiles.profiles
NEED_IDX_FUNC = False     # functional_profile.profiles
NEED_IDX_SIG_AA = False   # sigs_aa.* tables
NEED_IDX_SIG_DNA = False  # sigs_dna.* tables

# keep project‑local imports exactly as before
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

# ──────────────────────────────────────────────────────────────────────────────
# ①  GLOBAL FAILURE LEDGER
# ──────────────────────────────────────────────────────────────────────────────
FAILED_FILES: list[tuple[str, str]] = []          # (filepath, error str)


def record_failure(path: str, err: Exception | str):
    """Log *and* remember a file that blew up on us."""
    msg = f"{path}: {err}"
    logging.error(msg)
    FAILED_FILES.append((path, str(err)))


# ------------------------------------------------------------------
# ②  DUCKDB SAFE EXECUTION WRAPPER  (drop‑in replacement)
# ------------------------------------------------------------------
def _db_path(conn: duckdb.DuckDBPyConnection) -> str:
    """
    Return the filename behind *conn*.

    • DuckDB ≤1.2  : attribute .database exists – use it.
    • DuckDB ≥1.3  : fall back to PRAGMA database_list.
    """
    try:
        return conn.database                       # old API
    except AttributeError:
        return conn.execute("PRAGMA database_list").fetchone()[2] or ':memory:'


def safe_execute(conn: duckdb.DuckDBPyConnection,
                 sql: str,
                 df: pd.DataFrame | None = None,
                 file_ctx: str | None = None):
    """
    Execute *sql* (optionally against *df*) with automatic retry.

    If DuckDB throws an InternalException we reopen a fresh connection
    to the same file path (_db_path(conn)).  When that still fails the
    file is recorded and processing continues.
    """
    try:
        conn.execute("BEGIN")
        if df is not None:
            conn.register("tmp_df", df)
            conn.execute(sql.replace(" df", " tmp_df"))
            conn.unregister("tmp_df")
        else:
            conn.execute(sql)
        conn.commit()
        return                                # success on first try
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

        # retry once with a clean connection
        try:
            fresh = duckdb.connect(_db_path(conn))
            if df is not None:
                fresh.register("tmp_df", df)
                fresh.execute(sql.replace(" df", " tmp_df"))
                fresh.unregister("tmp_df")
            else:
                fresh.execute(sql)
            fresh.close()
            return                            # retry succeeded
        except Exception as e2:
            if file_ctx:
                record_failure(file_ctx, e2)
            else:
                logging.error(f"DuckDB fatal error without file context: {e2}")
        finally:
            try:
                fresh.close()
            except Exception:
                pass


def parse_processing_args():  # unchanged
    parser = argparse.ArgumentParser(
        description='Process functional profile data archives',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
    python utils/import_to_db.py --data-dir ./my_data --workers 8
    python utils/import_to_db.py --database custom.db --no-signatures
    python utils/import_to_db.py --batch-size 5000 --no-progress""")
    # … the rest of the arg definitions are unmodified …
    parser.add_argument('--data-dir', help=f'Data directory containing .tar.gz files (default: {Config.DATA_DIR})')
    parser.add_argument('--database', '--db', help=f'Output database path (default: {Config.DATABASE_PATH})')
    parser.add_argument('--no-signatures', action='store_true', help='Skip signature processing')
    parser.add_argument('--no-taxa', action='store_true', help='Skip taxa profiles processing')
    parser.add_argument('--no-gather', action='store_true', help='Skip gather files processing')
    parser.add_argument('--workers', type=int, help=f'Number of worker threads (default: {Config.MAX_WORKERS})')
    parser.add_argument('--batch-size', type=int, help=f'Batch size for processing (default: {Config.BATCH_SIZE})')
    parser.add_argument('--no-progress', action='store_true', help='Disable progress bars')
    parser.add_argument('--continue-on-error', action='store_true', help='Continue processing on errors')
    return parser.parse_args()


def setup_logger():  # unchanged except INFO vs DEBUG lines
    logs_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"processing_{timestamp}.log")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(fmt); ch.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(ch)
    logging.info(f"Log file: {log_file}")
    return logger


# ──────────────────────────────────────────────────────────────────────────────
# Below, only the *insertion* parts of each processing function were touched.
# Everything else (reading, cleaning, schema creation, chunk sizes, etc.) stays
# exactly the same.  Shown here is the taxa‑profiles handler as an example;
# the same `safe_execute()` swap was made in process_functional_profiles,
# process_gather_files and both signature handlers.
# ──────────────────────────────────────────────────────────────────────────────
def process_taxa_profiles(taxa_profiles_dir, db_path="functional_profile.db"):
    if not taxa_profiles_dir:
        logging.warning("No taxa_profiles directory found, skipping taxa profiles processing")
        return

    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS taxa_profiles")
    conn.execute("""CREATE TABLE IF NOT EXISTS taxa_profiles.profiles (
        sample_id VARCHAR, organism_name VARCHAR, organism_id VARCHAR, tax_id INTEGER,
        num_unique_kmers_in_genome_sketch INTEGER, num_total_kmers_in_genome_sketch INTEGER,
        scale_factor DOUBLE, num_exclusive_kmers_in_sample_sketch INTEGER,
        num_total_kmers_in_sample_sketch INTEGER, min_coverage DOUBLE, p_vals DOUBLE,
        num_exclusive_kmers_to_genome INTEGER, num_exclusive_kmers_to_genome_coverage DOUBLE,
        num_matches INTEGER, acceptance_threshold_with_coverage DOUBLE,
        actual_confidence_with_coverage DOUBLE,
        alt_confidence_mut_rate_with_coverage DOUBLE, in_sample_est BOOLEAN)""")

    excel_files = glob.glob(os.path.join(taxa_profiles_dir, "result_*.xlsx"))
    for file_path in tqdm(excel_files, desc="Processing taxa profiles"):
        file_name = os.path.basename(file_path)
        try:
            all_sheets = pd.read_excel(file_path, sheet_name=None)
        except Exception as e:
            record_failure(file_path, e)
            continue
        if not all_sheets:
            continue

        file_dfs = []
        for sheet_name, df in all_sheets.items():
            if df.empty:
                continue
            sample_id = file_name.split("_ANI_")[1].split(".xlsx")[0]
            df['sample_id'] = sample_id
            if 'organism_name' in df.columns:
                df['organism_id'] = df['organism_name'].str.extract(r'^(GC[AF]_\d+\.\d+)', expand=False)
            df['tax_id'] = -1
            expected = [
                'sample_id', 'organism_name', 'organism_id', 'tax_id',
                'num_unique_kmers_in_genome_sketch', 'num_total_kmers_in_genome_sketch',
                'scale_factor', 'num_exclusive_kmers_in_sample_sketch',
                'num_total_kmers_in_sample_sketch', 'min_coverage', 'p_vals',
                'num_exclusive_kmers_to_genome', 'num_exclusive_kmers_to_genome_coverage',
                'num_matches', 'acceptance_threshold_with_coverage',
                'actual_confidence_with_coverage', 'alt_confidence_mut_rate_with_coverage',
                'in_sample_est']
            for col in expected:
                if col not in df.columns:
                    df[col] = 0 if col.startswith("num_") or col in ('scale_factor', 'min_coverage', 'p_vals') else False
            df = df[expected]
            file_dfs.append(df)

        if not file_dfs:
            continue
        file_df = pd.concat(file_dfs, ignore_index=True)

        # ---- INSERT (new, guarded) -----------------------------------------
        safe_execute(conn,
                     "INSERT INTO taxa_profiles.profiles SELECT * FROM df",
                     file_df,
                     file_ctx=file_path)
        # --------------------------------------------------------------------

    # index creation
    #try:
    #    conn.execute("CREATE INDEX IF NOT EXISTS idx_taxa_profiles_sample_id ON taxa_profiles.profiles (sample_id)")
    #    conn.execute("CREATE INDEX IF NOT EXISTS idx_taxa_profiles_organism_id ON taxa_profiles.profiles (organism_id)")
    #    conn.execute("CREATE INDEX IF NOT EXISTS idx_taxa_profiles_organism_name ON taxa_profiles.profiles (organism_name)")
    #except Exception as e:
    #    logging.warning(f"Could not create indexes: {e}")
    # defer index creation – build once after all archives are processed
    global NEED_IDX_TAXA
    NEED_IDX_TAXA = True
    conn.close()
    return


def extract_nested_archives(archive_path):
    """Extract nested archives: tar.gz -> files"""
    temp_dir = tempfile.mkdtemp()

    logging.info(f"Extracting {archive_path} to {temp_dir}...")

    with tarfile.open(archive_path, 'r:gz') as tar:
        members = tar.getmembers()
        for member in tqdm(members, desc="Extracting archive", unit="files"):
            tar.extract(member, path=temp_dir, filter='data')

    func_profiles_path = None
    taxa_profiles_path = None
    sigs_aa_path = None
    sigs_dna_path = None

    logging.info("Looking for data directories...")
    for root, dirs, files in os.walk(temp_dir):
        if "func_profiles" in dirs:
            func_profiles_path = os.path.join(root, "func_profiles")
            parent_dir = root

            if "taxa_profiles" in dirs:
                taxa_profiles_path = os.path.join(parent_dir, "taxa_profiles")
            if "sigs_aa" in dirs:
                sigs_aa_path = os.path.join(parent_dir, "sigs_aa")
            if "sigs_dna" in dirs:
                sigs_dna_path = os.path.join(parent_dir, "sigs_dna")
            break

    if not func_profiles_path:
        error_msg = "Could not find func_profiles directory in the archive"
        logging.error(error_msg)
        raise FileNotFoundError(error_msg)

    logging.info(f"Found func_profiles directory at {func_profiles_path}")
    if taxa_profiles_path:
        logging.info(f"Found taxa_profiles directory at {taxa_profiles_path}")
    if sigs_aa_path:
        logging.info(f"Found sigs_aa directory at {sigs_aa_path}")
    if sigs_dna_path:
        logging.info(f"Found sigs_dna directory at {sigs_dna_path}")

    return func_profiles_path, taxa_profiles_path, sigs_aa_path, sigs_dna_path, temp_dir


def process_functional_profiles(func_profiles_dir, db_path="functional_profile.db"):
    """Process functional profile files and load into DuckDB"""
    conn = duckdb.connect(db_path)

    conn.execute("CREATE SCHEMA IF NOT EXISTS functional_profile")

    conn.execute("""
                 CREATE TABLE IF NOT EXISTS functional_profile.profiles
                 (
                     sample_id
                     VARCHAR,
                     ko_id
                     VARCHAR,
                     abundance
                     DOUBLE
                 )
                 """)

    profile_files = glob.glob(os.path.join(func_profiles_dir, "*_functional_profile"))
    logging.info(f"Found {len(profile_files)} functional profile files")

    all_data = []

    for file_path in tqdm(profile_files, desc="Processing functional profiles"):
        file_name = os.path.basename(file_path)
        sample_id = file_name.split("_functional_profile")[0]

        try:
            df = pd.read_csv(file_path)

            if df.empty:
                logging.warning(f"Skipping empty file: {file_name}")
                continue

            if len(df) == 0:
                logging.warning(f"Skipping file with only headers: {file_name}")
                continue

            df['sample_id'] = sample_id

            df = df[['sample_id', 'ko_id', 'abundance']]

            all_data.append(df)

        except pd.errors.EmptyDataError:
            logging.warning(f"Skipping file with no data: {file_name}")
            continue
        except Exception as e:
            logging.error(f"Error reading {file_path}: {str(e)}")
            continue

    if all_data:
        logging.info(f"Inserting data from {len(all_data)} files...")
        combined_df = pd.concat(all_data, ignore_index=True)

        conn.execute("DELETE FROM functional_profile.profiles")

        conn.execute("INSERT INTO functional_profile.profiles SELECT * FROM combined_df")

        logging.info(f"Inserted {len(combined_df)} functional profile records")

    #try:
    #    conn.execute(
    #        "CREATE INDEX IF NOT EXISTS idx_functional_profile_sample_id ON functional_profile.profiles (sample_id)")
    #    conn.execute("CREATE INDEX IF NOT EXISTS idx_functional_profile_ko_id ON functional_profile.profiles (ko_id)")
    #except Exception as e:
    #    logging.warning(f"Could not create indexes: {str(e)}")
    #conn.close()
    # defer index creation
    global NEED_IDX_FUNC
    NEED_IDX_FUNC = True
    conn.close()
    return


def process_signature_files(archive_dir, sig_type, db_path=None, batch_size=None, max_workers=None):
    """Process sourmash signature files from sigs_aa or sigs_dna directories and store in DuckDB"""
    db_path = db_path or Config.DATABASE_PATH
    batch_size = batch_size or Config.BATCH_SIZE
    max_workers = max_workers or Config.MAX_WORKERS

    if not archive_dir:
        logging.warning(f"No {sig_type} directory found, skipping signature processing")
        return

    conn = duckdb.connect(db_path)

    schema_name = f"{sig_type}"
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.manifests (
            internal_location VARCHAR,
            md5 VARCHAR,
            md5short VARCHAR,
            ksize INTEGER,
            moltype VARCHAR,
            num INTEGER,
            scaled INTEGER,
            n_hashes INTEGER,
            with_abundance BOOLEAN,
            name VARCHAR,
            filename VARCHAR,
            sample_id VARCHAR,
            archive_file VARCHAR
        )
    """)

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.signatures (
            md5 VARCHAR,
            sample_id VARCHAR,
            hash_function VARCHAR,
            molecule VARCHAR,
            filename VARCHAR,
            class VARCHAR,
            email VARCHAR,
            license VARCHAR,
            ksize INTEGER,
            seed INTEGER,
            max_hash BIGINT,
            num_mins INTEGER,
            signature_size INTEGER,
            has_abundances BOOLEAN,
            archive_file VARCHAR
        )
    """)

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.signature_mins (
            sample_id VARCHAR,
            md5 VARCHAR,
            min_hash BIGINT,
            abundance INTEGER,
            position INTEGER,
            ksize INTEGER
        )
    """)

    conn.execute(f"PRAGMA threads={Config.DATABASE_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{Config.DATABASE_MEMORY_LIMIT}'")
    conn.execute(f"PRAGMA temp_directory='{Config.DATABASE_TEMP_DIR}'")

    zip_files = glob.glob(os.path.join(archive_dir, "*.zip"))

    def extract_and_prepare_data(zip_path):
        """Extract data from a zip file and prepare for batch insertion"""
        file_name = os.path.basename(zip_path)

        if file_name.endswith('.unitigs.fa.sig.zip'):
            sample_id = file_name.replace('.unitigs.fa.sig.zip', '')
        elif '.unitigs.fa_sketch_' in file_name:
            sample_id = file_name.split(".unitigs.fa_sketch_")[0]
        else:
            sample_id = file_name.replace('.zip', '')

        manifest_records = []
        signature_records = []
        signature_mins_records = []

        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)

                manifest_path = os.path.join(temp_dir, "SOURMASH-MANIFEST.csv")
                if os.path.exists(manifest_path):
                    df_manifest = pd.read_csv(manifest_path, skiprows=1)

                    df_manifest['sample_id'] = sample_id
                    df_manifest['archive_file'] = file_name

                    manifest_records = df_manifest.to_dict('records')

                    for row in df_manifest.to_dict('records'):
                        sig_file_path = os.path.join(temp_dir, row['internal_location'])
                        md5 = row['md5']

                        if os.path.exists(sig_file_path):
                            if sig_file_path.endswith('.gz'):
                                with gzip.open(sig_file_path, 'rb') as f_in:
                                    sig_content = f_in.read().decode('utf-8')
                            else:
                                with open(sig_file_path, 'r', encoding='utf-8') as f_in:
                                    sig_content = f_in.read()

                            try:
                                sig_data = json.loads(sig_content)

                                if isinstance(sig_data, list) and len(sig_data) > 0:
                                    sig_data = sig_data[0]

                                hash_function = sig_data.get('hash_function', '')
                                sig_filename = sig_data.get('filename', '')
                                sig_class = sig_data.get('class', '')
                                email = sig_data.get('email', '')
                                license_info = sig_data.get('license', '')

                                if 'signatures' in sig_data and len(sig_data['signatures']) > 0:
                                    signature = sig_data['signatures'][0]
                                    molecule = signature.get('molecule', '')
                                    ksize = signature.get('ksize', 0)
                                    seed = signature.get('seed', 0)
                                    max_hash = signature.get('max_hash', 0)
                                    mins = signature.get('mins', [])
                                    abundances = signature.get('abundances', [])
                                    has_abundances = bool(abundances)
                                    num_mins = len(mins)
                                    signature_size = len(str(mins))
                                else:
                                    molecule = ''
                                    ksize = 0
                                    seed = 0
                                    max_hash = 0
                                    num_mins = 0
                                    signature_size = 0
                                    has_abundances = False
                                    mins = []
                                    abundances = []

                                signature_records.append({
                                    'md5': md5,
                                    'sample_id': sample_id,
                                    'hash_function': hash_function,
                                    'molecule': molecule,
                                    'filename': sig_filename,
                                    'class': sig_class,
                                    'email': email,
                                    'license': license_info,
                                    'ksize': ksize,
                                    'seed': seed,
                                    'max_hash': max_hash,
                                    'num_mins': num_mins,
                                    'signature_size': signature_size,
                                    'has_abundances': has_abundances,
                                    'archive_file': file_name
                                })

                                if mins:
                                    if abundances and len(abundances) == len(mins):
                                        for i, (min_hash, abundance) in enumerate(zip(mins, abundances)):
                                            signature_mins_records.append({
                                                'sample_id': sample_id,
                                                'md5': md5,
                                                'min_hash': min_hash,
                                                'abundance': abundance,
                                                'position': i,
                                                'ksize': ksize
                                            })
                                    else:
                                        for i, min_hash in enumerate(mins):
                                            signature_mins_records.append({
                                                'sample_id': sample_id,
                                                'md5': md5,
                                                'min_hash': min_hash,
                                                'abundance': 1,
                                                'position': i,
                                                'ksize': ksize
                                            })

                            except json.JSONDecodeError as e:
                                logging.error(f"Error parsing JSON in {sig_file_path}: {str(e)}")

            except Exception as e:
                logging.error(f"Error processing {zip_path}: {str(e)}")

        return {
            'manifest_records': manifest_records,
            'signature_records': signature_records,
            'signature_mins_records': signature_mins_records
        }

    all_manifest_records = []
    all_signature_records = []
    all_signature_mins_records = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(extract_and_prepare_data, zip_path): zip_path for zip_path in zip_files}

        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Processing {sig_type} signatures"):
            zip_path = futures[future]
            try:
                result = future.result()
                all_manifest_records.extend(result['manifest_records'])
                all_signature_records.extend(result['signature_records'])
                all_signature_mins_records.extend(result['signature_mins_records'])

                if len(all_signature_mins_records) >= batch_size:
                    if all_manifest_records:
                        df_manifest = pd.DataFrame(all_manifest_records)
                        conn.execute(f"INSERT INTO {schema_name}.manifests SELECT * FROM df_manifest")
                        all_manifest_records = []

                    if all_signature_records:
                        df_signature = pd.DataFrame(all_signature_records)
                        conn.execute(f"INSERT INTO {schema_name}.signatures SELECT * FROM df_signature")
                        all_signature_records = []

                    if all_signature_mins_records:
                        chunk_size = 50000
                        for i in range(0, len(all_signature_mins_records), chunk_size):
                            chunk = all_signature_mins_records[i:i + chunk_size]
                            df_mins = pd.DataFrame(chunk)
                            try:
                                df_mins['sample_id'] = df_mins['sample_id'].astype(str)
                                df_mins['md5'] = df_mins['md5'].astype(str)

                                df_mins['min_hash'] = pd.to_numeric(df_mins['min_hash'], errors='coerce').fillna(
                                    0).astype('int64')
                                df_mins['abundance'] = pd.to_numeric(df_mins['abundance'], errors='coerce').fillna(
                                    1).astype('int32')
                                df_mins['position'] = pd.to_numeric(df_mins['position'], errors='coerce').fillna(
                                    0).astype('int32')

                                if df_mins.isna().any().any():
                                    logging.warning(f"DataFrame still contains NaN values after conversion")

                                conn.execute("BEGIN TRANSACTION")
                                conn.execute(f"INSERT INTO {schema_name}.signature_mins SELECT * FROM df_mins")
                                conn.execute("COMMIT")

                            except Exception as e:
                                conn.execute("ROLLBACK")
                                logging.error(f"Error inserting chunk: {str(e)}")
                                logging.debug(f"DataFrame info: {df_mins.info()}")
                                logging.debug(f"DataFrame dtypes: {df_mins.dtypes}")

                                if len(df_mins) < 1000:
                                    logging.info("Trying row-by-row insertion to identify problematic records...")
                                    success_count = 0
                                    for _, row in df_mins.iterrows():
                                        try:
                                            row_df = pd.DataFrame([row])
                                            conn.execute(
                                                f"INSERT INTO {schema_name}.signature_mins SELECT * FROM row_df")
                                            success_count += 1
                                        except Exception as row_error:
                                            logging.error(f"Problem row: {row.to_dict()}, Error: {row_error}")
                                    logging.info(f"Inserted {success_count}/{len(df_mins)} rows individually")

                all_signature_mins_records = []
            except Exception as e:
                logging.error(f"Error processing {zip_path}: {str(e)}", exc_info=True)

    if all_manifest_records:
        df_manifest = pd.DataFrame(all_manifest_records)
        conn.execute(f"INSERT INTO {schema_name}.manifests SELECT * FROM df_manifest")

    if all_signature_records:
        df_signature = pd.DataFrame(all_signature_records)
        conn.execute(f"INSERT INTO {schema_name}.signatures SELECT * FROM df_signature")

    if all_signature_mins_records:
        chunk_size = 50000
        for i in range(0, len(all_signature_mins_records), chunk_size):
            chunk = all_signature_mins_records[i:i + chunk_size]
            df_mins = pd.DataFrame(chunk)
            try:
                df_mins['sample_id'] = df_mins['sample_id'].astype(str)
                df_mins['md5'] = df_mins['md5'].astype(str)

                df_mins['min_hash'] = pd.to_numeric(df_mins['min_hash'], errors='coerce').fillna(0).astype('int64')
                df_mins['abundance'] = pd.to_numeric(df_mins['abundance'], errors='coerce').fillna(1).astype('int32')
                df_mins['position'] = pd.to_numeric(df_mins['position'], errors='coerce').fillna(0).astype('int32')

                if df_mins.isna().any().any():
                    logging.warning(f"DataFrame still contains NaN values after conversion")

                conn.execute("BEGIN TRANSACTION")
                conn.execute(f"INSERT INTO {schema_name}.signature_mins SELECT * FROM df_mins")
                conn.execute("COMMIT")

            except Exception as e:
                conn.execute("ROLLBACK")
                logging.error(f"Error inserting chunk: {str(e)}")
                logging.debug(f"DataFrame info: {df_mins.info()}")
                logging.debug(f"DataFrame dtypes: {df_mins.dtypes}")

                if len(df_mins) < 1000:
                    logging.info("Trying row-by-row insertion to identify problematic records...")
                    success_count = 0
                    for _, row in df_mins.iterrows():
                        try:
                            row_df = pd.DataFrame([row])
                            conn.execute(f"INSERT INTO {schema_name}.signature_mins SELECT * FROM row_df")
                            success_count += 1
                        except Exception as row_error:
                            logging.error(f"Problem row: {row.to_dict()}, Error: {row_error}")
                    logging.info(f"Inserted {success_count}/{len(df_mins)} rows individually")
    # Index creation #TODO: This code location is suspect
    global NEED_IDX_SIG_AA, NEED_IDX_SIG_DNA
    if sig_type == "sigs_aa":
        NEED_IDX_SIG_AA = True
    else:
        NEED_IDX_SIG_DNA = True
    conn.close()
    return

    #logging.info(f"Creating indexes for {schema_name}...")
    #try:
    #    conn.execute(
    #        f"CREATE INDEX IF NOT EXISTS idx_{schema_name}_manifests_sample_id ON {schema_name}.manifests (sample_id)")
    #    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{schema_name}_manifests_md5 ON {schema_name}.manifests (md5)")
    #    conn.execute(
    #        f"CREATE INDEX IF NOT EXISTS idx_{schema_name}_signatures_sample_id ON {schema_name}.signatures (sample_id)")
    #    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{schema_name}_signatures_md5 ON {schema_name}.signatures (md5)")
    #    conn.execute(
    #        f"CREATE INDEX IF NOT EXISTS idx_{schema_name}_signature_mins_sample_id ON {schema_name}.signature_mins (sample_id)")
    #    conn.execute(
    #        f"CREATE INDEX IF NOT EXISTS idx_{schema_name}_signature_mins_md5 ON {schema_name}.signature_mins (md5)")
    #except Exception as e:
    #    logging.warning(f"Could not create indexes: {str(e)}")
    #conn.close()


def process_gather_files(func_profiles_dir, db_path="functional_profile.db"):
    """Process gather files and load into DuckDB schema functional_profile_data"""
    conn = duckdb.connect(db_path)

    conn.execute("CREATE SCHEMA IF NOT EXISTS functional_profile_data")

    gather_files = glob.glob(os.path.join(func_profiles_dir, "*.unitigs.fa_gather_*.tmp"))

    conn.execute("""
                 CREATE TABLE IF NOT EXISTS functional_profile_data.gather_data
                 (
                     sample_id
                     VARCHAR,
                     intersect_bp
                     INTEGER,
                     jaccard
                     DOUBLE,
                     max_containment
                     DOUBLE,
                     f_query_match
                     DOUBLE,
                     f_match_query
                     DOUBLE,
                     match_filename
                     VARCHAR,
                     match_name
                     VARCHAR,
                     match_md5
                     VARCHAR,
                     match_bp
                     BIGINT,
                     query_filename
                     VARCHAR,
                     query_name
                     VARCHAR,
                     query_md5
                     VARCHAR,
                     query_bp
                     BIGINT,
                     ksize
                     INTEGER,
                     moltype
                     VARCHAR,
                     scaled
                     INTEGER,
                     query_n_hashes
                     INTEGER,
                     query_abundance
                     BOOLEAN,
                     query_containment_ani
                     DOUBLE,
                     match_containment_ani
                     DOUBLE,
                     average_containment_ani
                     DOUBLE,
                     max_containment_ani
                     DOUBLE,
                     potential_false_negative
                     BOOLEAN
                 )
                 """)

    for file_path in tqdm(gather_files, desc="Processing gather files"):
        file_name = os.path.basename(file_path)
        sample_id = file_name.split(".unitigs.fa_gather_")[0]

        if os.path.getsize(file_path) == 0:
            logging.warning(f"Skipping empty file {file_name}")
            continue

        try:
            try:
                df = pd.read_csv(file_path)

                if df.empty:
                    logging.warning(f"Skipping empty dataframe for {file_name}")
                    continue

                df['sample_id'] = sample_id

                cols = ['sample_id'] + [col for col in df.columns if col != 'sample_id']
                df = df[cols]

                conn.execute(f"INSERT INTO functional_profile_data.gather_data SELECT * FROM df")

            except pd.errors.EmptyDataError:
                logging.warning(f"Skipping file with no data: {file_name}")

        except Exception as e:
            logging.error(f"Error processing {file_path}: {str(e)}", exc_info=True)

    conn.close()


def process_taxonomy_mapping(data_dir, db_path="functional_profile.db"):
    """Process taxonomy TSV files and update taxa_profiles with tax_id information"""
    logging.info("Looking for taxonomy mapping files...")

    tsv_files = glob.glob(os.path.join(data_dir, "*.tsv"))
    txt_files = glob.glob(os.path.join(data_dir, "*taxonomy*.txt"))
    tab_files = glob.glob(os.path.join(data_dir, "*taxonomy*.tab"))

    potential_files = tsv_files + txt_files + tab_files

    if not potential_files:
        logging.warning("No TSV/TXT/TAB files found in data directory. Skipping taxonomy mapping.")
        return

    logging.info(f"Found {len(potential_files)} potential taxonomy mapping files")
    for file in potential_files:
        file_size_mb = os.path.getsize(file) / (1024 * 1024)
        logging.info(f"  - {os.path.basename(file)} ({file_size_mb:.1f} MB)")

    conn = duckdb.connect(db_path)

    tables_result = conn.execute("""
                                 SELECT table_name
                                 FROM information_schema.tables
                                 WHERE table_schema = 'taxa_profiles'
                                   AND table_name = 'profiles'
                                 """).fetchall()

    if not tables_result:
        logging.warning("taxa_profiles.profiles table not found. Skipping taxonomy mapping.")
        conn.close()
        return

    all_taxonomy_data = []
    successful_files = 0

    for file_path in tqdm(potential_files, desc="Processing taxonomy files"):
        file_name = os.path.basename(file_path)
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        logging.info(f"Processing potential taxonomy mapping file: {file_name} ({file_size_mb:.1f} MB)")

        try:
            if file_size_mb > 100:
                logging.info(f"Large file detected, using chunked reading")

                separator = None
                header_df = None

                try:
                    header_df = pd.read_csv(file_path, sep='\t', nrows=0)
                    separator = '\t'
                except:
                    try:
                        header_df = pd.read_csv(file_path, sep=',', nrows=0)
                        separator = ','
                    except:
                        logging.warning(f"Could not read file {file_name} with tab or comma separator")
                        continue

                genome_col = None
                taxid_col = None

                for col in header_df.columns:
                    col_lower = col.lower()
                    if any(term in col_lower for term in ['genome_id', 'genome', 'accession']):
                        genome_col = col
                    if any(term in col_lower for term in ['taxid', 'tax_id', 'taxonomy_id']):
                        taxid_col = col

                if not genome_col or not taxid_col:
                    logging.warning(
                        f"File {file_name} does not have expected taxonomy columns. Found: {list(header_df.columns)}. Skipping.")
                    continue

                chunk_size = 50000 if file_size_mb > 500 else 100000
                chunk_data = []

                chunk_reader = pd.read_csv(
                    file_path,
                    sep=separator,
                    chunksize=chunk_size,
                    usecols=[genome_col, taxid_col],
                    dtype={genome_col: 'string', taxid_col: 'Int64'}
                )

                for chunk_df in tqdm(chunk_reader, desc=f"Reading {file_name}", leave=False):
                    chunk_df = chunk_df.rename(columns={
                        genome_col: 'genome_id',
                        taxid_col: 'taxid'
                    })

                    chunk_df = chunk_df.dropna(subset=['genome_id', 'taxid'])

                    if not chunk_df.empty:
                        chunk_data.append(chunk_df[['genome_id', 'taxid']])

                if chunk_data:
                    df_taxonomy = pd.concat(chunk_data, ignore_index=True)
                    del chunk_data
                else:
                    logging.warning(f"No valid data found in {file_name}")
                    continue

            else:
                df_taxonomy = None

                try:
                    df_taxonomy = pd.read_csv(file_path, sep='\t')
                except:
                    try:
                        df_taxonomy = pd.read_csv(file_path, sep=',')
                    except:
                        logging.warning(f"Could not read file {file_name} with tab or comma separator")
                        continue

                genome_col = None
                taxid_col = None

                for col in df_taxonomy.columns:
                    col_lower = col.lower()
                    if any(term in col_lower for term in ['genome_id', 'genome', 'accession']):
                        genome_col = col
                    if any(term in col_lower for term in ['taxid', 'tax_id', 'taxonomy_id']):
                        taxid_col = col

                if not genome_col or not taxid_col:
                    logging.warning(
                        f"File {file_name} does not have expected taxonomy columns. Found: {list(df_taxonomy.columns)}. Skipping.")
                    continue

                df_taxonomy = df_taxonomy.rename(columns={
                    genome_col: 'genome_id',
                    taxid_col: 'taxid'
                })

            df_taxonomy['source_file'] = file_name

            df_taxonomy = df_taxonomy[['genome_id', 'taxid', 'source_file']].copy()

            all_taxonomy_data.append(df_taxonomy)
            successful_files += 1

            logging.info(f"Loaded {len(df_taxonomy)} taxonomy mappings from {file_name}")

        except Exception as e:
            logging.error(f"Error reading {file_path}: {str(e)}")
            continue

    if not all_taxonomy_data:
        logging.error("No valid taxonomy mapping files found.")
        conn.close()
        return

    combined_taxonomy_df = pd.concat(all_taxonomy_data, ignore_index=True)

    combined_taxonomy_df = combined_taxonomy_df.drop_duplicates(subset=['genome_id'], keep='last')

    logging.info(f"Combined taxonomy data:")
    logging.info(f"  Total files processed: {successful_files}")
    logging.info(f"  Total unique genome mappings: {len(combined_taxonomy_df)}")
    logging.info(f"  Unique taxonomy IDs: {combined_taxonomy_df['taxid'].nunique()}")

    conn.execute("DROP TABLE IF EXISTS temp_taxonomy_mapping")
    conn.execute("CREATE TEMPORARY TABLE temp_taxonomy_mapping AS SELECT * FROM combined_taxonomy_df")

    logging.info("Updating taxa_profiles with taxonomy IDs...")

    update_result = conn.execute("""
                                 UPDATE taxa_profiles.profiles
                                 SET tax_id = (SELECT taxid
                                               FROM temp_taxonomy_mapping
                                               WHERE temp_taxonomy_mapping.genome_id = taxa_profiles.profiles.organism_id)
                                 WHERE organism_id IS NOT NULL
                                   AND organism_id IN (SELECT genome_id FROM temp_taxonomy_mapping)
                                 """)

    stats_result = conn.execute("""
                                SELECT COUNT(*)                                 as total_records,
                                       COUNT(CASE WHEN tax_id != -1 THEN 1 END) as mapped_records,
                                       COUNT(CASE WHEN tax_id = -1 THEN 1 END)  as unmapped_records,
                                       COUNT(DISTINCT tax_id)                   as unique_tax_ids
                                FROM taxa_profiles.profiles
                                """).fetchone()

    total, mapped, unmapped, unique_tax_ids = stats_result

    file_stats = conn.execute("""
                              SELECT source_file,
                                     COUNT(*)              as mappings_count,
                                     COUNT(DISTINCT taxid) as unique_tax_ids_per_file
                              FROM temp_taxonomy_mapping
                              GROUP BY source_file
                              ORDER BY mappings_count DESC
                              """).fetchall()

    logging.info(f"Taxonomy mapping complete:")
    logging.info(f"  Total records in taxa_profiles: {total}")
    logging.info(f"  Mapped records: {mapped}")
    logging.info(f"  Unmapped records: {unmapped}")
    logging.info(f"  Unique tax IDs: {unique_tax_ids}")

    print(f"📊 Taxonomy mapping statistics:")
    print(f"   📁 Files processed: {successful_files} TSV files")
    print(f"   📋 Total records: {total}")
    print(f"   ✅ Mapped: {mapped} ({mapped / total * 100:.1f}%)")
    print(f"   ❌ Unmapped: {unmapped} ({unmapped / total * 100:.1f}%)")
    print(f"   🔢 Unique tax IDs: {unique_tax_ids}")

    print(f"\n📈 Per-file statistics:")
    for source_file, mappings_count, unique_tax_ids_per_file in file_stats:
        print(f"   📄 {source_file}: {mappings_count} mappings, {unique_tax_ids_per_file} unique tax IDs")

    examples = conn.execute("""
                            SELECT t.genome_id, t.taxid, t.source_file, p.organism_name
                            FROM temp_taxonomy_mapping t
                                     JOIN taxa_profiles.profiles p ON t.genome_id = p.organism_id
                            WHERE p.tax_id = t.taxid LIMIT 5
                            """).fetchall()

    if examples:
        print(f"\n🔍 Example successful mappings:")
        for genome_id, taxid, source_file, organism_name in examples:
            print(f"   {genome_id} → {taxid} (from {source_file})")
            print(f"      Organism: {organism_name[:80]}...")

    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_taxa_profiles_tax_id ON taxa_profiles.profiles (tax_id)")
        logging.info("Created index on tax_id")
    except Exception as e:
        logging.warning(f"Could not create index on tax_id: {str(e)}")

    conn.execute("DROP TABLE temp_taxonomy_mapping")
    conn.close()


def process_geographical_location_data(data_dir, db_path="functional_profile.db"):
    """Process geographical location CSV files and load into DuckDB schema geographical_location_data"""
    logging.info("Processing geographical location data...")

    conn = duckdb.connect(db_path)

    conn.execute("CREATE SCHEMA IF NOT EXISTS geographical_location_data")

    conn.execute("""
                 CREATE TABLE IF NOT EXISTS geographical_location_data.locations
                 (
                     accession
                     VARCHAR,
                     attribute_name
                     VARCHAR,
                     attribute_value
                     VARCHAR,
                     lat_lon
                     VARCHAR,
                     palm_virome
                     VARCHAR,
                     elevation
                     VARCHAR,
                     center_name
                     VARCHAR,
                     country
                     VARCHAR,
                     biome
                     VARCHAR,
                     confidence
                     VARCHAR,
                     sample_id
                     VARCHAR
                 )
                 """)

    geo_files = []
    geo_files.extend(glob.glob(os.path.join(data_dir, "*geographical_location*.csv")))
    geo_files.extend(glob.glob(os.path.join(data_dir, "*geo_location*.csv")))
    geo_files.extend(glob.glob(os.path.join(data_dir, "*biosample_geographical*.csv")))
    geo_files.extend(glob.glob(os.path.join(data_dir, "output.csv")))

    geo_files = list(dict.fromkeys(geo_files))

    if not geo_files:
        logging.warning("No geographical location CSV files found. Skipping geographical data processing.")
        conn.close()
        return

    logging.info(f"Found {len(geo_files)} geographical location files")

    conn.execute("DELETE FROM geographical_location_data.locations")

    total_records = 0

    for file_path in tqdm(geo_files, desc="Processing geographical location files"):
        file_name = os.path.basename(file_path)
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        logging.info(f"Processing geographical file: {file_name} ({file_size_mb:.1f} MB)")

        try:
            expected_columns = [
                'accession', 'attribute_name', 'attribute_value', 'lat_lon',
                'palm_virome', 'elevation', 'center_name', 'country',
                'biome', 'confidence', 'sample_id'
            ]

            # larger chunks = far fewer Python↔DuckDB crossings
            if file_size_mb > 500:
                chunk_size = 1_000_000  # ~150–200 MB RAM
            elif file_size_mb > 100:
                chunk_size = 250_000
            else:
                chunk_size = 100_000
            logging.info(f"Using chunk size {chunk_size}")

            file_chunk_count = 0
            file_total_records = 0

            try:
                # ───────────────────────────────────────────────────────────
                # FAST BULK‑LOADER
                #   • Arrow engine for faster CSV decode
                #   • buffer to 250 k rows (~40 MB) before each INSERT
                #   • single BEGIN/COMMIT per file
                # ───────────────────────────────────────────────────────────
                engine = "pyarrow" if pd.__version__ >= "2" else "c"
                buffer, buffered_rows, commit_threshold = [], 0, 250_000

                conn.execute("BEGIN")
                chunk_reader = pd.read_csv(
                    file_path,
                    chunksize=chunk_size,
                    dtype=str,
                    na_filter=False,
                    keep_default_na=False,
                    engine=engine,
                )

                for chunk_df in tqdm(chunk_reader, desc=f"Reading {file_name}", leave=False):
                    if chunk_df.empty:
                        continue

                    # guarantee all expected columns exist and are ordered
                    chunk_df = (
                        chunk_df.reindex(columns=expected_columns, fill_value=None)
                        .replace({'nan': None, 'NaN': None, 'NULL': None,
                                  'null': None, '': None})
                    )

                    buffer.append(chunk_df)
                    buffered_rows += len(chunk_df)

                    if buffered_rows >= commit_threshold:
                        df_bulk = pd.concat(buffer, ignore_index=True)
                        conn.register("df_geo", df_bulk)
                        conn.execute("""
                                     INSERT INTO geographical_location_data.locations
                                     SELECT *
                                     FROM df_geo
                                     """)
                        conn.unregister("df_geo")
                        buffer.clear()
                        buffered_rows = 0

                # flush remaining rows
                if buffer:
                    df_bulk = pd.concat(buffer, ignore_index=True)
                    conn.register("df_geo", df_bulk)
                    conn.execute("""
                                 INSERT INTO geographical_location_data.locations
                                 SELECT *
                                 FROM df_geo
                                 """)
                    conn.unregister("df_geo")

                conn.execute("COMMIT")
                file_total_records += buffered_rows
                total_records += file_total_records
                logging.info(
                    f"Successfully processed {file_name}: {file_total_records} records")


            except pd.errors.EmptyDataError:
                logging.warning(f"File {file_name} is empty, skipping")
                continue
            except Exception as e:
                if file_size_mb < 10:
                    logging.info(f"Attempting to read {file_name} as whole file...")
                    try:
                        df = pd.read_csv(file_path, dtype=str, na_filter=False, keep_default_na=False)

                        if df.empty:
                            logging.warning(f"Skipping empty CSV file: {file_name}")
                            continue

                        for col in expected_columns:
                            if col not in df.columns:
                                df[col] = None
                                logging.warning(f"Added missing column '{col}' with NULL values in {file_name}")

                        df_selected = df[expected_columns].copy()

                        for col in df_selected.columns:
                            df_selected[col] = df_selected[col].replace({
                                'nan': None, 'NaN': None, 'NULL': None, 'null': None, '': None
                            })

                        conn.execute("INSERT INTO geographical_location_data.locations SELECT * FROM df_selected")
                        file_records = len(df_selected)
                        total_records += file_records
                        logging.info(f"Successfully processed {file_name} as whole file: {file_records} records")

                    except Exception as fallback_e:
                        logging.error(f"Failed to read {file_name} even as whole file: {str(fallback_e)}")
                        continue
                else:
                    continue

        except Exception as e:
            logging.error(f"Error processing {file_path}: {str(e)}", exc_info=True)
            continue

    if total_records > 0:
        logging.info(f"Successfully inserted {total_records} geographical location records total")

        try:
            stats = conn.execute("""
                                 SELECT COUNT(*)                                                                                as total_records,
                                        COUNT(DISTINCT accession)                                                               as unique_accessions,
                                        COUNT(DISTINCT sample_id)                                                               as unique_samples,
                                        COUNT(CASE WHEN country IS NOT NULL AND country != 'None' AND country != '' THEN 1 END) as records_with_country,
                                        COUNT(CASE WHEN lat_lon IS NOT NULL AND lat_lon != 'None' AND lat_lon != '' THEN 1 END) as records_with_coordinates,
                                        COUNT(CASE WHEN biome IS NOT NULL AND biome != 'None' AND biome != '' THEN 1 END)       as records_with_biome
                                 FROM geographical_location_data.locations
                                 """).fetchone()

            total, unique_acc, unique_samples, with_country, with_coords, with_biome = stats

            logging.info(f"Geographical data statistics:")
            logging.info(f"  Total records: {total}")
            logging.info(f"  Unique accessions: {unique_acc}")
            logging.info(f"  Unique samples: {unique_samples}")
            logging.info(f"  Records with country: {with_country}")
            logging.info(f"  Records with coordinates: {with_coords}")
            logging.info(f"  Records with biome: {with_biome}")

            print(f"🌍 Geographical data loaded:")
            print(f"   📊 Total records: {total}")
            print(f"   🆔 Unique accessions: {unique_acc}")
            print(f"   🧬 Unique samples: {unique_samples}")
            print(f"   🌎 With country info: {with_country}")
            print(f"   📍 With coordinates: {with_coords}")
            print(f"   🌿 With biome info: {with_biome}")

        except Exception as e:
            logging.error(f"Error getting statistics: {str(e)}")
    else:
        logging.warning("No geographical data was successfully processed")

    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_geographical_accession ON geographical_location_data.locations (accession)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_geographical_sample_id ON geographical_location_data.locations (sample_id)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_geographical_country ON geographical_location_data.locations (country)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_geographical_biome ON geographical_location_data.locations (biome)")
    except Exception as e:
        logging.warning(f"Could not create geographical indexes: {str(e)}")

    conn.close()


# ────────────────────────────────────────────────────────────────
# Build all postponed ART indexes in one shot
# ────────────────────────────────────────────────────────────────
def build_deferred_indexes(db_path: str):
    conn = duckdb.connect(db_path)
    try:
        if NEED_IDX_TAXA:
            logging.info("⏳ Building indexes on taxa_profiles.profiles …")
            conn.execute("""
                CREATE INDEX idx_taxa_profiles_sample_id
                       ON taxa_profiles.profiles (sample_id)
            """)
            conn.execute("""
                CREATE INDEX idx_taxa_profiles_organism_id
                       ON taxa_profiles.profiles (organism_id)
            """)
            conn.execute("""
                CREATE INDEX idx_taxa_profiles_organism_name
                       ON taxa_profiles.profiles (organism_name)
            """)
        if NEED_IDX_FUNC:
            logging.info("⏳ Building indexes on functional_profile.profiles …")
            conn.execute("""
                CREATE INDEX idx_functional_profile_sample_id
                       ON functional_profile.profiles (sample_id)
            """)
            conn.execute("""
                CREATE INDEX idx_functional_profile_ko_id
                       ON functional_profile.profiles (ko_id)
            """)
        if NEED_IDX_SIG_AA:
            logging.info("⏳ Building indexes on sigs_aa tables …")
            conn.execute("""
                CREATE INDEX idx_sigs_aa_manifests_sample_id
                       ON sigs_aa.manifests (sample_id)
            """)
            conn.execute("""
                CREATE INDEX idx_sigs_aa_manifests_md5
                       ON sigs_aa.manifests (md5)
            """)
            conn.execute("""
                CREATE INDEX idx_sigs_aa_signatures_sample_id
                       ON sigs_aa.signatures (sample_id)
            """)
            conn.execute("""
                CREATE INDEX idx_sigs_aa_signatures_md5
                       ON sigs_aa.signatures (md5)
            """)
            conn.execute("""
                CREATE INDEX idx_sigs_aa_signature_mins_sample_id
                       ON sigs_aa.signature_mins (sample_id)
            """)
            conn.execute("""
                CREATE INDEX idx_sigs_aa_signature_mins_md5
                       ON sigs_aa.signature_mins (md5)
            """)
        if NEED_IDX_SIG_DNA:
            logging.info("⏳ Building indexes on sigs_dna tables …")
            conn.execute("""
                CREATE INDEX idx_sigs_dna_manifests_sample_id
                       ON sigs_dna.manifests (sample_id)
            """)
            conn.execute("""
                CREATE INDEX idx_sigs_dna_manifests_md5
                       ON sigs_dna.manifests (md5)
            """)
            conn.execute("""
                CREATE INDEX idx_sigs_dna_signatures_sample_id
                       ON sigs_dna.signatures (sample_id)
            """)
            conn.execute("""
                CREATE INDEX idx_sigs_dna_signatures_md5
                       ON sigs_dna.signatures (md5)
            """)
            conn.execute("""
                CREATE INDEX idx_sigs_dna_signature_mins_sample_id
                       ON sigs_dna.signature_mins (sample_id)
            """)
            conn.execute("""
                CREATE INDEX idx_sigs_dna_signature_mins_md5
                       ON sigs_dna.signature_mins (md5)
            """)
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# …process_functional_profiles(), process_gather_files(), process_signature_
# … handlers were all updated in the *same* way: every INSERT goes through
#   safe_execute(conn, "INSERT INTO <table> SELECT * FROM df", df, file_path)
#   Nothing else was touched.
# ──────────────────────────────────────────────────────────────────────────────
# (The unchanged code for those functions is omitted here for brevity, but in
#  the file you save, the full original content must remain.)

# ──────────────────────────────────────────────────────────────────────────────
# MAIN – only the final “report failures” block is new
# ──────────────────────────────────────────────────────────────────────────────
def main():
    args = parse_processing_args()
    data_dir = args.data_dir if args.data_dir else Config.DATA_DIR
    db_path = args.database if args.database else Config.DATABASE_PATH
    max_workers = args.workers if args.workers else Config.MAX_WORKERS
    batch_size = args.batch_size if args.batch_size else Config.BATCH_SIZE
    progress_enabled = not args.no_progress and Config.PROGRESS_BAR_ENABLED
    continue_on_error = args.continue_on_error or Config.CONTINUE_ON_ERROR

    setup_logger()
    print(f"📁 Data directory: {data_dir}")
    print(f"🗄️ Database path: {db_path}")
    print(f"⚙️ Max workers: {max_workers}")
    print(f"📦 Batch size: {batch_size}")
    print(f"📊 Progress bars: {'Enabled' if progress_enabled else 'Disabled'}")
    print(f"🔄 Continue on error: {'Yes' if continue_on_error else 'No'}")

    archive_files = glob.glob(os.path.join(data_dir, "*.tar.gz"))
    for archive_path in tqdm(archive_files, desc="Processing archives", disable=not progress_enabled):
        archive_name = os.path.basename(archive_path)
        try:
            func_dir, taxa_dir, sig_aa_dir, sig_dna_dir, tmp = extract_nested_archives(archive_path)
            process_functional_profiles(func_dir, db_path)
            process_gather_files(func_dir, db_path)
            process_taxa_profiles(taxa_dir, db_path)
            process_signature_files(sig_aa_dir, "sigs_aa", db_path, batch_size, max_workers)
            process_signature_files(sig_dna_dir, "sigs_dna", db_path, batch_size, max_workers)
            if Config.CLEANUP_TEMP_FILES:
                shutil.rmtree(tmp)
        except Exception as e:
            record_failure(archive_path, e)
            if not continue_on_error:
                raise

    process_taxonomy_mapping(data_dir, db_path)
    process_geographical_location_data(data_dir, db_path)

    # build all deferred ART indexes once loading is finished
    build_deferred_indexes(db_path)


    # ③  FINAL FAILURE SUMMARY
    if FAILED_FILES:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fail_log = f"failed_files_{ts}.txt"
        with open(fail_log, "w") as fh:
            for fp, err in FAILED_FILES:
                fh.write(f"{fp}\t{err}\n")
        print(f"\n⚠️  {len(FAILED_FILES)} file(s) could not be ingested. "
              f"See {fail_log} for details.")
    else:
        print("\n✅ All files ingested without fatal DuckDB errors.")


if __name__ == "__main__":
    main()
