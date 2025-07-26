import os
import sys
import tarfile
import tempfile
import duckdb
import pandas as pd
import glob
import shutil
import zipfile
import gzip
import json
import argparse
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import datetime
import traceback
from typing import List, Tuple, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config


# Global error tracking
class ErrorTracker:
    def __init__(self):
        self.failed_files = []
        self.error_details = {}

    def add_error(self, file_path: str, operation: str, error: Exception):
        """Track an error for a specific file"""
        error_info = {
            'file': file_path,
            'operation': operation,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'timestamp': datetime.now().isoformat(),
            'traceback': traceback.format_exc()
        }
        self.failed_files.append(file_path)
        if file_path not in self.error_details:
            self.error_details[file_path] = []
        self.error_details[file_path].append(error_info)

    def save_report(self, output_dir: str = None):
        """Save error report to file"""
        if not output_dir:
            output_dir = os.path.join(os.getcwd(), 'logs')
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = os.path.join(output_dir, f"error_report_{timestamp}.json")

        with open(report_file, 'w') as f:
            json.dump({
                'summary': {
                    'total_failures': len(set(self.failed_files)),
                    'failed_files': list(set(self.failed_files))
                },
                'details': self.error_details
            }, f, indent=2)

        return report_file


# Global error tracker instance
error_tracker = ErrorTracker()


def parse_processing_args():
    """Parse command line arguments for import_to_db.py"""
    parser = argparse.ArgumentParser(
        description='Process functional profile data archives',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python utils/import_to_db.py --data-dir ./my_data --workers 8
    python utils/import_to_db.py --database custom.db --no-signatures
    python utils/import_to_db.py --batch-size 5000 --no-progress
        """)

    parser.add_argument('--data-dir',
                        help=f'Data directory containing .tar.gz files (default: {Config.DATA_DIR})')
    parser.add_argument('--database', '--db',
                        help=f'Output database path (default: {Config.DATABASE_PATH})')

    parser.add_argument('--no-signatures', action='store_true',
                        help='Skip signature processing')
    parser.add_argument('--no-taxa', action='store_true',
                        help='Skip taxa profiles processing')
    parser.add_argument('--no-gather', action='store_true',
                        help='Skip gather files processing')

    parser.add_argument('--workers', type=int,
                        help=f'Number of worker threads (default: {Config.MAX_WORKERS})')
    parser.add_argument('--batch-size', type=int,
                        help=f'Batch size for processing (default: {Config.BATCH_SIZE})')

    parser.add_argument('--no-progress', action='store_true',
                        help='Disable progress bars')
    parser.add_argument('--continue-on-error', action='store_true',
                        help='Continue processing on errors')

    return parser.parse_args()


def setup_logger():
    """Set up logger to write to both file and console"""
    logs_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(logs_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"processing_{timestamp}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info(f"Log file created at: {log_file}")
    return logger


def safe_db_operation(func):
    """Decorator to handle database operations safely"""

    def wrapper(*args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Database operation failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                    # Force close any existing connections
                    if 'conn' in kwargs and kwargs['conn'] is not None:
                        try:
                            kwargs['conn'].close()
                        except:
                            pass
                else:
                    raise
        return None

    return wrapper


def create_new_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """Create a new DuckDB connection with proper settings"""
    conn = duckdb.connect(db_path)
    conn.execute(f"PRAGMA threads={Config.DATABASE_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{Config.DATABASE_MEMORY_LIMIT}'")
    conn.execute(f"PRAGMA temp_directory='{Config.DATABASE_TEMP_DIR}'")
    return conn


def process_signature_files(archive_dir, sig_type, db_path=None, batch_size=None, max_workers=None):
    """Process sourmash signature files from sigs_aa or sigs_dna directories and store in DuckDB"""
    db_path = db_path or Config.DATABASE_PATH
    batch_size = batch_size or Config.BATCH_SIZE
    max_workers = max_workers or Config.MAX_WORKERS

    if not archive_dir:
        logging.warning(f"No {sig_type} directory found, skipping signature processing")
        return

    conn = None
    try:
        conn = create_new_connection(db_path)

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
                                    error_tracker.add_error(sig_file_path, "json_parse", e)

                except Exception as e:
                    logging.error(f"Error processing {zip_path}: {str(e)}")
                    error_tracker.add_error(zip_path, "signature_extraction", e)

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
                        # Insert batch with error handling
                        try:
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

                                        df_mins['min_hash'] = pd.to_numeric(df_mins['min_hash'],
                                                                            errors='coerce').fillna(0).astype('int64')
                                        df_mins['abundance'] = pd.to_numeric(df_mins['abundance'],
                                                                             errors='coerce').fillna(1).astype('int32')
                                        df_mins['position'] = pd.to_numeric(df_mins['position'],
                                                                            errors='coerce').fillna(0).astype('int32')

                                        if df_mins.isna().any().any():
                                            logging.warning(f"DataFrame still contains NaN values after conversion")

                                        conn.execute("BEGIN TRANSACTION")
                                        conn.execute(f"INSERT INTO {schema_name}.signature_mins SELECT * FROM df_mins")
                                        conn.execute("COMMIT")

                                    except Exception as e:
                                        conn.execute("ROLLBACK")
                                        logging.error(f"Error inserting chunk: {str(e)}")
                                        error_tracker.add_error(f"batch_insert_{sig_type}", "signature_mins_insertion",
                                                                e)

                                        # Try to recover connection
                                        try:
                                            conn.close()
                                        except:
                                            pass
                                        conn = create_new_connection(db_path)

                            all_signature_mins_records = []

                        except Exception as batch_error:
                            logging.error(f"Batch insertion error: {str(batch_error)}")
                            error_tracker.add_error(f"batch_{sig_type}", "batch_insertion", batch_error)
                            # Reset connection
                            try:
                                conn.close()
                            except:
                                pass
                            conn = create_new_connection(db_path)

                except Exception as e:
                    logging.error(f"Error processing {zip_path}: {str(e)}", exc_info=True)
                    error_tracker.add_error(zip_path, "signature_processing", e)

        # Insert remaining records
        if all_manifest_records or all_signature_records or all_signature_mins_records:
            try:
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

                            df_mins['min_hash'] = pd.to_numeric(df_mins['min_hash'], errors='coerce').fillna(0).astype(
                                'int64')
                            df_mins['abundance'] = pd.to_numeric(df_mins['abundance'], errors='coerce').fillna(
                                1).astype('int32')
                            df_mins['position'] = pd.to_numeric(df_mins['position'], errors='coerce').fillna(0).astype(
                                'int32')

                            conn.execute("BEGIN TRANSACTION")
                            conn.execute(f"INSERT INTO {schema_name}.signature_mins SELECT * FROM df_mins")
                            conn.execute("COMMIT")

                        except Exception as e:
                            conn.execute("ROLLBACK")
                            logging.error(f"Error inserting final chunk: {str(e)}")
                            error_tracker.add_error(f"final_batch_{sig_type}", "signature_mins_insertion", e)

            except Exception as final_error:
                logging.error(f"Final batch insertion error: {str(final_error)}")
                error_tracker.add_error(f"final_batch_{sig_type}", "final_insertion", final_error)

        # Create indexes with error handling
        logging.info(f"Creating indexes for {schema_name}...")
        try:
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{schema_name}_manifests_sample_id ON {schema_name}.manifests (sample_id)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{schema_name}_manifests_md5 ON {schema_name}.manifests (md5)")
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{schema_name}_signatures_sample_id ON {schema_name}.signatures (sample_id)")
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{schema_name}_signatures_md5 ON {schema_name}.signatures (md5)")
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{schema_name}_signature_mins_sample_id ON {schema_name}.signature_mins (sample_id)")
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{schema_name}_signature_mins_md5 ON {schema_name}.signature_mins (md5)")
        except Exception as e:
            logging.warning(f"Could not create indexes: {str(e)}")
            error_tracker.add_error(f"{schema_name}_indexes", "index_creation", e)

    except Exception as e:
        logging.error(f"Critical error in process_signature_files: {str(e)}")
        error_tracker.add_error(archive_dir, "signature_processing_critical", e)
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass


def extract_nested_archives(archive_path):
    """Extract nested archives: tar.gz -> files"""
    temp_dir = tempfile.mkdtemp()

    try:
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

    except Exception as e:
        logging.error(f"Error extracting archive {archive_path}: {str(e)}")
        error_tracker.add_error(archive_path, "archive_extraction", e)
        # Clean up temp directory on error
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise


def process_functional_profiles(func_profiles_dir, db_path="functional_profile.db"):
    """Process functional profile files and load into DuckDB"""
    conn = None
    try:
        conn = create_new_connection(db_path)

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
                error_tracker.add_error(file_path, "functional_profile_read", e)
                continue

        if all_data:
            logging.info(f"Inserting data from {len(all_data)} files...")
            combined_df = pd.concat(all_data, ignore_index=True)

            try:
                conn.execute("DELETE FROM functional_profile.profiles")
                conn.execute("INSERT INTO functional_profile.profiles SELECT * FROM combined_df")
                logging.info(f"Inserted {len(combined_df)} functional profile records")
            except Exception as e:
                logging.error(f"Error inserting functional profiles: {str(e)}")
                error_tracker.add_error(func_profiles_dir, "functional_profile_insertion", e)
                raise

        try:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_functional_profile_sample_id ON functional_profile.profiles (sample_id)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_functional_profile_ko_id ON functional_profile.profiles (ko_id)")
        except Exception as e:
            logging.warning(f"Could not create indexes: {str(e)}")

    except Exception as e:
        logging.error(f"Critical error in process_functional_profiles: {str(e)}")
        error_tracker.add_error(func_profiles_dir, "functional_profile_critical", e)
        raise
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass


def process_gather_files(func_profiles_dir, db_path="functional_profile.db"):
    """Process gather files and load into DuckDB schema functional_profile_data"""
    conn = None
    try:
        conn = create_new_connection(db_path)

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
                error_tracker.add_error(file_path, "gather_file_processing", e)
                # Try to recover connection
                try:
                    conn.close()
                except:
                    pass
                conn = create_new_connection(db_path)

    except Exception as e:
        logging.error(f"Critical error in process_gather_files: {str(e)}")
        error_tracker.add_error(func_profiles_dir, "gather_files_critical", e)
        raise
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

def process_taxa_profiles(taxa_profiles_dir, db_path="functional_profile.db"):
    """Process taxa profile Excel files and load into DuckDB schema taxa_profiles"""
    if not taxa_profiles_dir:
        logging.warning("No taxa_profiles directory found, skipping taxa profiles processing")
        return

    conn = None
    try:
        conn = create_new_connection(db_path)

        conn.execute("CREATE SCHEMA IF NOT EXISTS taxa_profiles")

        conn.execute("""
                     CREATE TABLE IF NOT EXISTS taxa_profiles.profiles
                     (
                         sample_id
                         VARCHAR,
                         organism_name
                         VARCHAR,
                         organism_id
                         VARCHAR,
                         tax_id
                         INTEGER,
                         num_unique_kmers_in_genome_sketch
                         INTEGER,
                         num_total_kmers_in_genome_sketch
                         INTEGER,
                         scale_factor
                         DOUBLE,
                         num_exclusive_kmers_in_sample_sketch
                         INTEGER,
                         num_total_kmers_in_sample_sketch
                         INTEGER,
                         min_coverage
                         DOUBLE,
                         p_vals
                         DOUBLE,
                         num_exclusive_kmers_to_genome
                         INTEGER,
                         num_exclusive_kmers_to_genome_coverage
                         DOUBLE,
                         num_matches
                         INTEGER,
                         acceptance_threshold_with_coverage
                         DOUBLE,
                         actual_confidence_with_coverage
                         DOUBLE,
                         alt_confidence_mut_rate_with_coverage
                         DOUBLE,
                         in_sample_est
                         BOOLEAN
                     )
                     """)

        excel_files = glob.glob(os.path.join(taxa_profiles_dir, "result_*.xlsx"))

        all_data = []

        for file_path in tqdm(excel_files, desc="Processing taxa profiles"):
            file_name = os.path.basename(file_path)

            try:
                match = file_name.split("_ANI_")[1].split(".xlsx")[0]
                sample_id = match

                # Read all sheets from Excel file
                all_sheets = pd.read_excel(file_path, sheet_name=None)

                if not all_sheets:
                    logging.warning(f"No sheets found in Excel file: {file_name}")
                    continue

                # Process each sheet
                file_dataframes = []
                for sheet_name, df in all_sheets.items():
                    logging.info(f"Processing sheet '{sheet_name}' from {file_name}")

                    if df.empty:
                        logging.warning(f"Skipping empty sheet '{sheet_name}' in {file_name}")
                        continue

                    df['sample_id'] = sample_id

                    if 'organism_name' in df.columns:
                        df['organism_id'] = df['organism_name'].str.extract(r'^(GC[AF]_\d+\.\d+)', expand=False)

                        valid_ids = df['organism_id'].notna().sum()
                        total_rows = len(df)
                        logging.info(
                            f"Extracted {valid_ids}/{total_rows} organism IDs from sheet '{sheet_name}' in {file_name}")

                    df['tax_id'] = -1

                    numeric_columns = [
                        'num_unique_kmers_in_genome_sketch', 'num_total_kmers_in_genome_sketch',
                        'scale_factor', 'num_exclusive_kmers_in_sample_sketch', 'num_total_kmers_in_sample_sketch',
                        'min_coverage', 'p_vals', 'num_exclusive_kmers_to_genome',
                        'num_exclusive_kmers_to_genome_coverage', 'num_matches',
                        'acceptance_threshold_with_coverage',
                        'actual_confidence_with_coverage', 'alt_confidence_mut_rate_with_coverage'
                    ]

                    if 'num_exclusive_kmers_to_genоme_coverage' in df.columns:
                        df = df.rename(columns={
                            'num_exclusive_kmers_to_genоme_coverage': 'num_exclusive_kmers_to_genome_coverage'})
                        logging.warning(
                            f"Fixed column name typo in sheet '{sheet_name}' of {file_name}: genоme -> genome")

                    for col in [c for c in numeric_columns if c in df.columns]:
                        if df[col].dtype == 'object':
                            try:
                                df[col] = df[col].astype(str).str.replace(',', '.').replace('', 'NaN').fillna(pd.NA)

                                df[col] = pd.to_numeric(df[col], errors='coerce')

                                # Additional cleaning for problematic values
                                df[col] = df[col].replace([float('inf'), float('-inf')], None)
                                df[col] = df[col].fillna(0.0)

                            except Exception as e:
                                logging.warning(f"Could not convert column {col} in sheet '{sheet_name}': {str(e)}")
                                df[col] = 0.0  # Set default value if conversion fails

                    expected_columns = [
                        'sample_id', 'organism_name', 'organism_id', 'tax_id', 'num_unique_kmers_in_genome_sketch',
                        'num_total_kmers_in_genome_sketch', 'scale_factor', 'num_exclusive_kmers_in_sample_sketch',
                        'num_total_kmers_in_sample_sketch', 'min_coverage', 'p_vals',
                        'num_exclusive_kmers_to_genome',
                        'num_exclusive_kmers_to_genome_coverage', 'num_matches',
                        'acceptance_threshold_with_coverage',
                        'actual_confidence_with_coverage', 'alt_confidence_mut_rate_with_coverage', 'in_sample_est'
                    ]

                    for col in expected_columns:
                        if col not in df.columns:
                            if col == 'sample_id':
                                df[col] = sample_id
                            elif col == 'organism_id':
                                df[col] = None
                            elif col == 'tax_id':
                                df[col] = -1
                            elif col == 'in_sample_est':
                                df[col] = False
                            elif col.startswith('num_') or col in ['scale_factor', 'min_coverage', 'p_vals']:
                                df[col] = 0.0
                            else:
                                df[col] = None
                            logging.warning(
                                f"Added missing column '{col}' with default value in sheet '{sheet_name}' of {file_name}")

                    # Ensure in_sample_est is properly converted to boolean
                    if 'in_sample_est' in df.columns:
                        df['in_sample_est'] = df['in_sample_est'].fillna(False)
                        if df['in_sample_est'].dtype == 'object':
                            df['in_sample_est'] = df['in_sample_est'].astype(str).str.lower().isin(
                                ['true', '1', 'yes', 'y'])
                        df['in_sample_est'] = df['in_sample_est'].astype(bool)

                    df_selected = df[expected_columns].copy()

                    if len(df_selected.columns) != 18:
                        logging.error(
                            f"Column count mismatch in sheet '{sheet_name}' of {file_name}: expected 18, got {len(df_selected.columns)}")
                        logging.error(f"Columns: {list(df_selected.columns)}")
                        continue

                    file_dataframes.append(df_selected)
                    logging.info(
                        f"Successfully processed sheet '{sheet_name}' from {file_name}: {len(df_selected)} records, {len(df_selected.columns)} columns")

                # Combine all sheets from this file
                if file_dataframes:
                    file_combined_df = pd.concat(file_dataframes, ignore_index=True)
                    all_data.append(file_combined_df)
                    logging.info(
                        f"Combined {len(file_dataframes)} sheets from {file_name}: total {len(file_combined_df)} records")
                else:
                    logging.warning(f"No valid sheets found in {file_name}")

            except Exception as e:
                logging.error(f"Error processing {file_path}: {str(e)}", exc_info=True)
                error_tracker.add_error(file_path, "taxa_profile_processing", e)

        if all_data:
            logging.info(f"Inserting data from {len(all_data)} files...")
            combined_df = pd.concat(all_data, ignore_index=True)

            # Clean up data before inserting to prevent DuckDB internal errors
            logging.info("Cleaning data before database insertion...")

            # Handle NaN values in numeric columns
            numeric_columns = [
                'num_unique_kmers_in_genome_sketch', 'num_total_kmers_in_genome_sketch',
                'scale_factor', 'num_exclusive_kmers_in_sample_sketch', 'num_total_kmers_in_sample_sketch',
                'min_coverage', 'p_vals', 'num_exclusive_kmers_to_genome',
                'num_exclusive_kmers_to_genome_coverage', 'num_matches', 'acceptance_threshold_with_coverage',
                'actual_confidence_with_coverage', 'alt_confidence_mut_rate_with_coverage'
            ]

            for col in numeric_columns:
                if col in combined_df.columns:
                    try:
                        # Replace inf and -inf with NaN, then fill with 0
                        combined_df[col] = combined_df[col].replace([float('inf'), float('-inf')], None)
                        combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0.0)

                        # Additional check for string representations
                        if combined_df[col].dtype == 'object':
                            combined_df[col] = combined_df[col].astype(str)
                            combined_df[col] = combined_df[col].replace(
                                ['inf', '-inf', 'Infinity', '-Infinity', 'NaN', 'nan'], '0.0')
                            combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0.0)

                    except Exception as e:
                        logging.warning(f"Error cleaning column {col}: {str(e)}, setting to 0.0")
                        combined_df[col] = 0.0

            # Ensure tax_id is properly set
            combined_df['tax_id'] = combined_df['tax_id'].fillna(-1).astype(int)

            # Clean string columns
            string_columns = ['sample_id', 'organism_name', 'organism_id']
            for col in string_columns:
                if col in combined_df.columns:
                    combined_df[col] = combined_df[col].astype(str).replace('nan', None)

            # Ensure boolean column is properly typed
            combined_df['in_sample_est'] = combined_df['in_sample_est'].fillna(False).astype(bool)

            try:
                conn.execute("DELETE FROM taxa_profiles.profiles")
                conn.execute("INSERT INTO taxa_profiles.profiles SELECT * FROM combined_df")
                logging.info(f"Successfully inserted {len(combined_df)} taxa profile records")
            except Exception as e:
                logging.error(f"Error inserting data: {str(e)}")
                error_tracker.add_error(taxa_profiles_dir, "taxa_profile_insertion", e)
                try:
                    table_schema = conn.execute("DESCRIBE taxa_profiles.profiles").fetchall()
                    logging.error(f"Table schema: {table_schema}")
                except:
                    pass
                raise

        try:
            logging.info("Creating indexes for taxa_profiles...")

            # Check if data exists before creating indexes
            count_result = conn.execute("SELECT COUNT(*) FROM taxa_profiles.profiles").fetchone()
            if count_result and count_result[0] > 0:
                logging.info(f"Creating indexes for {count_result[0]} records...")

                # Create indexes one by one with individual error handling
                try:
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_taxa_profiles_sample_id ON taxa_profiles.profiles (sample_id)")
                    logging.info("Created index on sample_id")
                except Exception as e:
                    logging.warning(f"Could not create index on sample_id: {str(e)}")

                try:
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_taxa_profiles_organism_id ON taxa_profiles.profiles (organism_id)")
                    logging.info("Created index on organism_id")
                except Exception as e:
                    logging.warning(f"Could not create index on organism_id: {str(e)}")

                try:
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_taxa_profiles_organism_name ON taxa_profiles.profiles (organism_name)")
                    logging.info("Created index on organism_name")
                except Exception as e:
                    logging.warning(f"Could not create index on organism_name: {str(e)}")
            else:
                logging.warning("No data found in taxa_profiles.profiles, skipping index creation")

        except Exception as e:
            logging.warning(f"Error during index creation: {str(e)}")

    except Exception as e:
        logging.error(f"Critical error in process_taxa_profiles: {str(e)}")
        error_tracker.add_error(taxa_profiles_dir, "taxa_profiles_critical", e)
        raise
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

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

    conn = None
    try:
        conn = create_new_connection(db_path)

        tables_result = conn.execute("""
                                     SELECT table_name
                                     FROM information_schema.tables
                                     WHERE table_schema = 'taxa_profiles'
                                       AND table_name = 'profiles'
                                     """).fetchall()

        if not tables_result:
            logging.warning("taxa_profiles.profiles table not found. Skipping taxonomy mapping.")
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
                error_tracker.add_error(file_path, "taxonomy_file_read", e)
                continue

        if not all_taxonomy_data:
            logging.error("No valid taxonomy mapping files found.")
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

    except Exception as e:
        logging.error(f"Critical error in process_taxonomy_mapping: {str(e)}")
        error_tracker.add_error(data_dir, "taxonomy_mapping_critical", e)
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

def process_geographical_location_data(data_dir, db_path="functional_profile.db"):
    """Process geographical location CSV files and load into DuckDB schema geographical_location_data"""
    logging.info("Processing geographical location data...")

    conn = None
    try:
        conn = create_new_connection(db_path)

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

                if file_size_mb > 100:
                    chunk_size = 10000
                    logging.info(f"Large file detected, using chunked reading with chunk size {chunk_size}")
                elif file_size_mb > 50:
                    chunk_size = 25000
                else:
                    chunk_size = 50000

                file_chunk_count = 0
                file_total_records = 0

                try:
                    chunk_reader = pd.read_csv(
                        file_path,
                        chunksize=chunk_size,
                        dtype=str,
                        na_filter=False,
                        keep_default_na=False
                    )

                    for chunk_df in tqdm(chunk_reader, desc=f"Reading {file_name}", leave=False):
                        file_chunk_count += 1

                        if chunk_df.empty:
                            logging.warning(f"Skipping empty chunk {file_chunk_count} in {file_name}")
                            continue

                        for col in expected_columns:
                            if col not in chunk_df.columns:
                                chunk_df[col] = None
                                if file_chunk_count == 1:
                                    logging.warning(f"Added missing column '{col}' with NULL values in {file_name}")

                        chunk_df_selected = chunk_df[expected_columns].copy()

                        for col in chunk_df_selected.columns:
                            chunk_df_selected[col] = chunk_df_selected[col].replace({
                                'nan': None, 'NaN': None, 'NULL': None, 'null': None, '': None
                            })

                        try:
                            conn.execute(
                                "INSERT INTO geographical_location_data.locations SELECT * FROM chunk_df_selected")
                            chunk_records = len(chunk_df_selected)
                            file_total_records += chunk_records

                            if file_chunk_count % 10 == 0:
                                logging.info(
                                    f"Processed {file_chunk_count} chunks from {file_name}, {file_total_records} records so far")

                        except Exception as e:
                            logging.error(f"Error inserting chunk {file_chunk_count} from {file_name}: {str(e)}")
                            error_tracker.add_error(f"{file_path}_chunk_{file_chunk_count}", "geo_chunk_insertion",
                                                    e)
                            # Try to recover connection
                            try:
                                conn.close()
                            except:
                                pass
                            conn = create_new_connection(db_path)
                            continue

                    total_records += file_total_records
                    logging.info(
                        f"Successfully processed {file_name}: {file_total_records} records in {file_chunk_count} chunks")

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
                            error_tracker.add_error(file_path, "geo_file_read_fallback", fallback_e)
                            continue
                        else:
                            error_tracker.add_error(file_path, "geo_file_read", e)
                            continue

            except Exception as e:
                logging.error(f"Error processing {file_path}: {str(e)}", exc_info=True)
                error_tracker.add_error(file_path, "geo_file_processing", e)
                continue

        if total_records > 0:
            logging.info(f"Successfully inserted {total_records} geographical location records total")
            
            try:
                stats = conn.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT accession) as unique_accessions,
                        COUNT(DISTINCT sample_id) as unique_samples,
                        COUNT(CASE WHEN country IS NOT NULL AND country != 'None' AND country != '' THEN 1 END) as records_with_country,
                        COUNT(CASE WHEN lat_lon IS NOT NULL AND lat_lon != 'None' AND lat_lon != '' THEN 1 END) as records_with_coordinates,
                        COUNT(CASE WHEN biome IS NOT NULL AND biome != 'None' AND biome != '' THEN 1 END) as records_with_biome
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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_geographical_accession ON geographical_location_data.locations (accession)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_geographical_sample_id ON geographical_location_data.locations (sample_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_geographical_country ON geographical_location_data.locations (country)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_geographical_biome ON geographical_location_data.locations (biome)")
        except Exception as e:
            logging.warning(f"Could not create geographical indexes: {str(e)}")
            
    except Exception as e:
        logging.error(f"Critical error in process_geographical_location_data: {str(e)}")
        error_tracker.add_error(data_dir, "geographical_data_critical", e)
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

def process_archive_safely(archive_path, db_path, max_workers, batch_size, progress_enabled, skip_signatures, skip_taxa, skip_gather):
    """Process a single archive with comprehensive error handling"""
    archive_name = os.path.basename(archive_path)
    temp_dir = None
    
    try:
        logging.info(f"Processing archive: {archive_name}")
        
        # Extract archive
        try:
            func_profiles_dir, taxa_profiles_dir, sigs_aa_dir, sigs_dna_dir, temp_dir = extract_nested_archives(archive_path)
        except Exception as e:
            logging.error(f"Failed to extract archive {archive_name}: {str(e)}")
            error_tracker.add_error(archive_path, "archive_extraction", e)
            return False
        
        # Process functional profiles
        try:
            logging.info("Processing functional profiles...")
            process_functional_profiles(func_profiles_dir, db_path)
        except Exception as e:
            logging.error(f"Failed to process functional profiles for {archive_name}: {str(e)}")
            error_tracker.add_error(archive_path, "functional_profiles", e)
            # Continue with other processing
        
        # Process gather files
        if not skip_gather:
            try:
                logging.info("Processing gather files...")
                process_gather_files(func_profiles_dir, db_path)
            except Exception as e:
                logging.error(f"Failed to process gather files for {archive_name}: {str(e)}")
                error_tracker.add_error(archive_path, "gather_files", e)
                # Continue with other processing
        
        # Process taxa profiles
        if not skip_taxa and taxa_profiles_dir:
            try:
                logging.info("Processing taxa profiles...")
                process_taxa_profiles(taxa_profiles_dir, db_path)
            except Exception as e:
                logging.error(f"Failed to process taxa profiles for {archive_name}: {str(e)}")
                error_tracker.add_error(archive_path, "taxa_profiles", e)
                # Continue with other processing
        
        # Process signatures
        if not skip_signatures:
            if sigs_aa_dir:
                try:
                    logging.info("Processing protein signatures...")
                    process_signature_files(sigs_aa_dir, "sigs_aa", db_path=db_path, 
                                          batch_size=batch_size, max_workers=max_workers)
                except Exception as e:
                    logging.error(f"Failed to process protein signatures for {archive_name}: {str(e)}")
                    error_tracker.add_error(archive_path, "sigs_aa", e)
                    # Continue with other processing
            
            if sigs_dna_dir:
                try:
                    logging.info("Processing DNA signatures...")
                    process_signature_files(sigs_dna_dir, "sigs_dna", db_path=db_path,
                                          batch_size=batch_size, max_workers=max_workers)
                except Exception as e:
                    logging.error(f"Failed to process DNA signatures for {archive_name}: {str(e)}")
                    error_tracker.add_error(archive_path, "sigs_dna", e)
                    # Continue with other processing
        
        return True
        
    except Exception as e:
        logging.error(f"Unexpected error processing archive {archive_name}: {str(e)}")
        error_tracker.add_error(archive_path, "archive_processing", e)
        return False
        
    finally:
        # Always clean up temp directory
        if temp_dir and os.path.exists(temp_dir):
            try:
                if Config.CLEANUP_TEMP_FILES:
                    logging.info("Cleaning up temporary files...")
                    shutil.rmtree(temp_dir)
            except Exception as e:
                logging.warning(f"Failed to clean up temp directory: {str(e)}")

def main():
    args = parse_processing_args()
    
    data_dir = args.data_dir if args.data_dir else Config.DATA_DIR
    db_path = args.database if args.database else Config.DATABASE_PATH
    max_workers = args.workers if args.workers else Config.MAX_WORKERS
    batch_size = args.batch_size if args.batch_size else Config.BATCH_SIZE
    
    progress_enabled = not args.no_progress and Config.PROGRESS_BAR_ENABLED
    continue_on_error = args.continue_on_error or Config.CONTINUE_ON_ERROR
    
    setup_logger()
    logging.info("Starting functional profile processing")
    
    print(f"📁 Data directory: {data_dir}")
    print(f"🗄️ Database path: {db_path}")
    print(f"⚙️ Max workers: {max_workers}")
    print(f"📦 Batch size: {batch_size}")
    print(f"📊 Progress bars: {'Enabled' if progress_enabled else 'Disabled'}")
    print(f"🔄 Continue on error: {'Yes' if continue_on_error else 'No'}")
    
    os.makedirs(data_dir, exist_ok=True)
    
    archive_files = glob.glob(os.path.join(data_dir, "*.tar.gz"))
    
    if not archive_files:
        logging.error(f"No archive files found in {data_dir}. Please place .tar.gz files there.")
        return
    
    logging.info(f"Found {len(archive_files)} archive files to process.")
    
    successful_archives = 0
    failed_archives = []
    
    for archive_path in tqdm(archive_files, desc="Processing archives", disable=not progress_enabled):
        success = process_archive_safely(
            archive_path, db_path, max_workers, batch_size, 
            progress_enabled, args.no_signatures, args.no_taxa, args.no_gather
        )
        
        if success:
            successful_archives += 1
        else:
            failed_archives.append(archive_path)
            if not continue_on_error:
                logging.error(f"Stopping due to error in {archive_path}")
                break
    
    # Process additional data files
    print("\n🔬 Processing taxonomy mapping...")
    try:
        process_taxonomy_mapping(data_dir, db_path)
    except Exception as e:
        logging.error(f"Failed to process taxonomy mapping: {str(e)}")
        error_tracker.add_error(data_dir, "taxonomy_mapping", e)
    
    print("\n🌍 Processing geographical location data...")
    try:
        process_geographical_location_data(data_dir, db_path)
    except Exception as e:
        logging.error(f"Failed to process geographical location data: {str(e)}")
        error_tracker.add_error(data_dir, "geographical_data", e)
    
    # Save error report
    if error_tracker.failed_files:
        error_report_path = error_tracker.save_report()
        print(f"\n⚠️ Error report saved to: {error_report_path}")
        print(f"   Total errors: {len(set(error_tracker.failed_files))}")
        print(f"   Failed files:")
        for file in sorted(set(error_tracker.failed_files)):
            print(f"     - {file}")
    
    # Final summary
    print(f"\n📊 Processing Summary:")
    print(f"   ✅ Successful archives: {successful_archives}/{len(archive_files)}")
    print(f"   ❌ Failed archives: {len(failed_archives)}")
    
    if failed_archives:
        print(f"\n   Failed archive files:")
        for archive in failed_archives:
            print(f"     - {os.path.basename(archive)}")
    
    logging.info(f"Processing complete! Database created at {db_path} with:")
    logging.info("- Schema functional_profile (contains functional profile tables)")
    logging.info("- Schema functional_profile_data (contains gather data)")
    logging.info("- Schema taxa_profiles (contains taxa profile tables with taxonomy IDs)")
    logging.info("- Schema geographical_location_data (contains geographical location data)")
    logging.info("- Schema sigs_aa (contains protein signatures)")
    logging.info("- Schema sigs_dna (contains DNA signatures)")
    
    if error_tracker.failed_files:
        logging.info(f"⚠️ Processing completed with {len(set(error_tracker.failed_files))} errors. Check error report for details.")

if __name__ == "__main__":
    main()