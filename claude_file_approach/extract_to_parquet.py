#!/usr/bin/env python
"""
Stage 1: Extract Logan data to Parquet files.

This script extracts data from tar.gz archives and writes to partitioned
Parquet files instead of directly to DuckDB. This approach is much faster
and more scalable for processing billions of rows.

Key improvements:
1. No database connections during extraction
2. Efficient columnar storage with Parquet
3. Partitioning by sample_id prefix for better organization
4. Batch writing to reduce I/O operations
5. Progress tracking with checkpoint support
"""
from __future__ import annotations

import os, sys, tarfile, tempfile, glob, shutil, zipfile, gzip, json, argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import hashlib

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import logging
import numpy as np

# Keep project-local imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

# Global failure tracking
FAILED_FILES: list[tuple[str, str]] = []
CHECKPOINT_FILE = "extraction_checkpoint.json"

def record_failure(path: str, err: Exception | str):
    """Log and remember failed files."""
    msg = f"{path}: {err}"
    logging.error(msg)
    FAILED_FILES.append((path, str(err)))

def save_checkpoint(processed_archives: set[str]):
    """Save progress checkpoint."""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({
            'processed': list(processed_archives),
            'timestamp': datetime.now().isoformat()
        }, f)

def load_checkpoint() -> set[str]:
    """Load progress checkpoint."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
                return set(data.get('processed', []))
        except:
            pass
    return set()

def get_partition_key(sample_id: str) -> str:
    """
    Generate partition key from sample_id.
    Uses first 2 characters for reasonable partition count.
    """
    if not sample_id:
        return "unknown"
    # Use first 2 chars of sample_id for partitioning
    return sample_id[:2].lower()

def ensure_parquet_dir(base_dir: str, table_name: str, partition: str = None) -> Path:
    """Ensure parquet directory exists."""
    if partition:
        path = Path(base_dir) / table_name / f"partition={partition}"
    else:
        path = Path(base_dir) / table_name
    path.mkdir(parents=True, exist_ok=True)
    return path

def write_to_parquet(df: pd.DataFrame, base_dir: str, table_name: str, 
                    partition_col: str = None, mode: str = 'append'):
    """
    Write DataFrame to partitioned Parquet files.
    
    Args:
        df: DataFrame to write
        base_dir: Base directory for parquet files
        table_name: Name of the table (becomes subdirectory)
        partition_col: Column to partition by
        mode: 'append' or 'overwrite'
    """
    if df.empty:
        return
    
    output_dir = Path(base_dir) / table_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if partition_col and partition_col in df.columns:
        # Write partitioned
        for partition_value, group_df in df.groupby(partition_col):
            partition_dir = output_dir / f"{partition_col}={partition_value}"
            partition_dir.mkdir(exist_ok=True)
            
            # Generate unique filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = partition_dir / f"data_{timestamp}.parquet"
            
            # Drop partition column from data (it's in the directory structure)
            group_df = group_df.drop(columns=[partition_col])
            
            # Write with compression
            group_df.to_parquet(
                filename,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
    else:
        # Write non-partitioned
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = output_dir / f"data_{timestamp}.parquet"
        
        df.to_parquet(
            filename,
            engine='pyarrow',
            compression='snappy',
            index=False
        )

def process_taxa_profiles(taxa_profiles_dir: str, output_dir: str, archive_name: str):
    """Process taxa profiles and write to Parquet."""
    if not taxa_profiles_dir:
        logging.warning("No taxa_profiles directory found")
        return

    excel_files = glob.glob(os.path.join(taxa_profiles_dir, "result_*.xlsx"))
    all_data = []
    
    for file_path in tqdm(excel_files, desc="Processing taxa profiles", leave=False):
        file_name = os.path.basename(file_path)
        try:
            all_sheets = pd.read_excel(file_path, sheet_name=None)
        except Exception as e:
            record_failure(file_path, e)
            continue
            
        if not all_sheets:
            continue

        for sheet_name, df in all_sheets.items():
            if df.empty:
                continue
            
            # Create a copy to avoid SettingWithCopyWarning
            df = df.copy()
            
            sample_id = file_name.split("_ANI_")[1].split(".xlsx")[0]
            df['sample_id'] = sample_id
            df['archive_name'] = archive_name
            
            if 'organism_name' in df.columns:
                df['organism_id'] = df['organism_name'].str.extract(r'^(GC[AF]_\d+\.\d+)', expand=False)
            
            df['tax_id'] = -1
            
            # Ensure all expected columns exist
            expected = [
                'sample_id', 'organism_name', 'organism_id', 'tax_id',
                'num_unique_kmers_in_genome_sketch', 'num_total_kmers_in_genome_sketch',
                'scale_factor', 'num_exclusive_kmers_in_sample_sketch',
                'num_total_kmers_in_sample_sketch', 'min_coverage', 'p_vals',
                'num_exclusive_kmers_to_genome', 'num_exclusive_kmers_to_genome_coverage',
                'num_matches', 'acceptance_threshold_with_coverage',
                'actual_confidence_with_coverage', 'alt_confidence_mut_rate_with_coverage',
                'in_sample_est', 'archive_name'
            ]
            
            for col in expected:
                if col not in df.columns:
                    if col.startswith("num_") or col in ('scale_factor', 'min_coverage', 'p_vals'):
                        df[col] = 0
                    elif col == 'in_sample_est':
                        df[col] = False
                    else:
                        df[col] = None
            
            df = df[expected]
            
            # Add partition column
            df['partition_key'] = df['sample_id'].apply(get_partition_key)
            
            all_data.append(df)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        write_to_parquet(combined_df, output_dir, 'taxa_profiles', 'partition_key')
        logging.info(f"Wrote {len(combined_df)} taxa profile records to Parquet")

def process_functional_profiles(func_profiles_dir: str, output_dir: str, archive_name: str, 
                               batch_size: int = 1000):
    """Process functional profiles and write to Parquet."""
    profile_files = glob.glob(os.path.join(func_profiles_dir, "*_functional_profile"))
    logging.info(f"Found {len(profile_files)} functional profile files")
    
    batch_data = []
    
    for i, file_path in enumerate(tqdm(profile_files, desc="Processing functional profiles", leave=False)):
        file_name = os.path.basename(file_path)
        sample_id = file_name.split("_functional_profile")[0]
        
        try:
            df = pd.read_csv(file_path)
            
            if df.empty or len(df) == 0:
                continue
            
            # Create a copy to avoid warnings
            df = df.copy()
            
            df['sample_id'] = sample_id
            df['archive_name'] = archive_name
            df['partition_key'] = get_partition_key(sample_id)
            
            df = df[['sample_id', 'ko_id', 'abundance', 'archive_name', 'partition_key']]
            batch_data.append(df)
            
            # Write batch to parquet
            if len(batch_data) >= batch_size or i == len(profile_files) - 1:
                if batch_data:
                    combined_df = pd.concat(batch_data, ignore_index=True)
                    write_to_parquet(combined_df, output_dir, 'functional_profiles', 'partition_key')
                    logging.info(f"Wrote batch of {len(combined_df)} functional profile records")
                    batch_data = []
                    
        except Exception as e:
            record_failure(file_path, e)
            continue

def process_gather_files(func_profiles_dir: str, output_dir: str, archive_name: str,
                        batch_size: int = 500):
    """Process gather files and write to Parquet."""
    gather_files = glob.glob(os.path.join(func_profiles_dir, "*.unitigs.fa_gather_*.tmp"))
    
    batch_data = []
    
    for i, file_path in enumerate(tqdm(gather_files, desc="Processing gather files", leave=False)):
        file_name = os.path.basename(file_path)
        sample_id = file_name.split(".unitigs.fa_gather_")[0]
        
        if os.path.getsize(file_path) == 0:
            continue
            
        try:
            df = pd.read_csv(file_path)
            
            if df.empty:
                continue
            
            # Create a copy to avoid warnings
            df = df.copy()
            
            # Ensure all columns are the correct type
            # Convert object columns that should be strings
            string_columns = ['match_filename', 'match_name', 'match_md5', 
                            'query_filename', 'query_name', 'query_md5', 'moltype']
            for col in string_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            
            # Convert numeric columns
            numeric_columns = {
                'intersect_bp': 'int64',
                'jaccard': 'float64',
                'max_containment': 'float64',
                'f_query_match': 'float64',
                'f_match_query': 'float64',
                'match_bp': 'int64',
                'query_bp': 'int64',
                'ksize': 'int64',
                'scaled': 'int64',
                'query_n_hashes': 'int64'
            }
            
            for col, dtype in numeric_columns.items():
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    if dtype.startswith('int'):
                        df[col] = df[col].fillna(0).astype(dtype)
                    else:
                        df[col] = df[col].astype(dtype)
            
            # Handle boolean columns
            boolean_columns = ['query_abundance', 'potential_false_negative']
            for col in boolean_columns:
                if col in df.columns:
                    df[col] = df[col].astype(bool)
            
            # Handle float columns that might have NaN
            float_columns = ['query_containment_ani', 'match_containment_ani', 
                           'average_containment_ani', 'max_containment_ani']
            for col in float_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
            df['sample_id'] = sample_id
            df['archive_name'] = archive_name
            df['partition_key'] = get_partition_key(sample_id)
            
            batch_data.append(df)
            
            if len(batch_data) >= batch_size or i == len(gather_files) - 1:
                if batch_data:
                    combined_df = pd.concat(batch_data, ignore_index=True)
                    write_to_parquet(combined_df, output_dir, 'gather_data', 'partition_key')
                    batch_data = []
                    
        except Exception as e:
            record_failure(file_path, e)

def process_signature_files(archive_dir: str, sig_type: str, output_dir: str, 
                          archive_name: str, batch_size: int = 50000,
                          manifest_batch_size: int = 500):
    """Process signature files and write to Parquet."""
    if not archive_dir:
        logging.warning(f"No {sig_type} directory found")
        return
        
    zip_files = glob.glob(os.path.join(archive_dir, "*.zip"))
    
    # Batch collectors
    manifest_batch = []
    signature_batch = []
    mins_batch = []
    mins_count = 0
    
    for zip_path in tqdm(zip_files, desc=f"Processing {sig_type} signatures", leave=False):
        file_name = os.path.basename(zip_path)
        
        # Extract sample_id
        if file_name.endswith('.unitigs.fa.sig.zip'):
            sample_id = file_name.replace('.unitigs.fa.sig.zip', '')
        elif '.unitigs.fa_sketch_' in file_name:
            sample_id = file_name.split(".unitigs.fa_sketch_")[0]
        else:
            sample_id = file_name.replace('.zip', '')
            
        partition_key = get_partition_key(sample_id)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                    
                manifest_path = os.path.join(temp_dir, "SOURMASH-MANIFEST.csv")
                if os.path.exists(manifest_path):
                    df_manifest = pd.read_csv(manifest_path, skiprows=1)
                    df_manifest = df_manifest.copy()  # Avoid SettingWithCopyWarning
                    
                    df_manifest['sample_id'] = sample_id
                    df_manifest['archive_file'] = file_name
                    df_manifest['archive_name'] = archive_name
                    df_manifest['partition_key'] = partition_key
                    
                    # Ensure correct types for manifest
                    if 'ksize' in df_manifest.columns:
                        df_manifest['ksize'] = pd.to_numeric(df_manifest['ksize'], errors='coerce').fillna(0).astype('int64')
                    if 'num' in df_manifest.columns:
                        df_manifest['num'] = pd.to_numeric(df_manifest['num'], errors='coerce').fillna(0).astype('int64')
                    if 'scaled' in df_manifest.columns:
                        df_manifest['scaled'] = pd.to_numeric(df_manifest['scaled'], errors='coerce').fillna(0).astype('int64')
                    if 'n_hashes' in df_manifest.columns:
                        df_manifest['n_hashes'] = pd.to_numeric(df_manifest['n_hashes'], errors='coerce').fillna(0).astype('int64')
                    if 'with_abundance' in df_manifest.columns:
                        df_manifest['with_abundance'] = df_manifest['with_abundance'].astype(bool)
                    
                    manifest_batch.append(df_manifest)
                    
                    # Process signature files
                    for _, row in df_manifest.iterrows():
                        sig_file_path = os.path.join(temp_dir, row['internal_location'])
                        md5 = str(row['md5'])  # Ensure it's a string
                        
                        if os.path.exists(sig_file_path):
                            # Read signature file
                            if sig_file_path.endswith('.gz'):
                                with gzip.open(sig_file_path, 'rb') as f:
                                    sig_content = f.read().decode('utf-8')
                            else:
                                with open(sig_file_path, 'r') as f:
                                    sig_content = f.read()
                                    
                            try:
                                sig_data = json.loads(sig_content)
                                if isinstance(sig_data, list) and len(sig_data) > 0:
                                    sig_data = sig_data[0]
                                    
                                # Extract signature metadata
                                sig_record = {
                                    'md5': md5,
                                    'sample_id': sample_id,
                                    'hash_function': str(sig_data.get('hash_function', '')),
                                    'molecule': '',
                                    'filename': str(sig_data.get('filename', '')),
                                    'class': str(sig_data.get('class', '')),
                                    'email': str(sig_data.get('email', '')),
                                    'license': str(sig_data.get('license', '')),
                                    'ksize': 0,
                                    'seed': 0,
                                    'max_hash': 0,
                                    'num_mins': 0,
                                    'signature_size': 0,
                                    'has_abundances': False,
                                    'archive_file': file_name,
                                    'archive_name': archive_name,
                                    'partition_key': partition_key
                                }
                                
                                if 'signatures' in sig_data and len(sig_data['signatures']) > 0:
                                    signature = sig_data['signatures'][0]
                                    sig_record.update({
                                        'molecule': str(signature.get('molecule', '')),
                                        'ksize': int(signature.get('ksize', 0)),
                                        'seed': int(signature.get('seed', 0)),
                                        'max_hash': int(signature.get('max_hash', 0)),
                                        'num_mins': len(signature.get('mins', [])),
                                        'signature_size': len(str(signature.get('mins', []))),
                                        'has_abundances': bool(signature.get('abundances', []))
                                    })
                                    
                                    # Process mins
                                    mins = signature.get('mins', [])
                                    abundances = signature.get('abundances', [])
                                    
                                    if mins:
                                        mins_records = []
                                        for i, min_hash in enumerate(mins):
                                            abundance = abundances[i] if i < len(abundances) else 1
                                            mins_records.append({
                                                'sample_id': sample_id,
                                                'md5': md5,
                                                'min_hash': int(min_hash),
                                                'abundance': int(abundance),
                                                'position': i,
                                                'ksize': sig_record['ksize'],
                                                'partition_key': partition_key
                                            })
                                        
                                        if mins_records:
                                            mins_df = pd.DataFrame(mins_records)
                                            mins_batch.append(mins_df)
                                            mins_count += len(mins_records)
                                
                                signature_batch.append(sig_record)
                                
                            except json.JSONDecodeError as e:
                                logging.error(f"Error parsing JSON: {e}")
                                
            except Exception as e:
                record_failure(zip_path, e)
                
        # Write batches when they get large
        if len(manifest_batch) >= manifest_batch_size:
            combined_manifest = pd.concat(manifest_batch, ignore_index=True)
            write_to_parquet(combined_manifest, output_dir, f'{sig_type}_manifests', 'partition_key')
            manifest_batch = []
            
        if len(signature_batch) >= 5000:
            sig_df = pd.DataFrame(signature_batch)
            write_to_parquet(sig_df, output_dir, f'{sig_type}_signatures', 'partition_key')
            signature_batch = []
            
        if mins_count >= batch_size:
            combined_mins = pd.concat(mins_batch, ignore_index=True)
            write_to_parquet(combined_mins, output_dir, f'{sig_type}_signature_mins', 'partition_key')
            mins_batch = []
            mins_count = 0
    
    # Write remaining batches
    if manifest_batch:
        combined_manifest = pd.concat(manifest_batch, ignore_index=True)
        write_to_parquet(combined_manifest, output_dir, f'{sig_type}_manifests', 'partition_key')
        
    if signature_batch:
        sig_df = pd.DataFrame(signature_batch)
        write_to_parquet(sig_df, output_dir, f'{sig_type}_signatures', 'partition_key')
        
    if mins_batch:
        combined_mins = pd.concat(mins_batch, ignore_index=True)
        write_to_parquet(combined_mins, output_dir, f'{sig_type}_signature_mins', 'partition_key')

def extract_nested_archives(archive_path: str) -> tuple:
    """Extract nested archives and return paths."""
    temp_dir = tempfile.mkdtemp()
    
    logging.info(f"Extracting {archive_path} to {temp_dir}...")
    
    with tarfile.open(archive_path, 'r:gz') as tar:
        members = tar.getmembers()
        for member in tqdm(members, desc="Extracting archive", unit="files", leave=False):
            tar.extract(member, path=temp_dir, filter='data')
    
    # Find directories
    func_profiles_path = None
    taxa_profiles_path = None
    sigs_aa_path = None
    sigs_dna_path = None
    
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
        raise FileNotFoundError("Could not find func_profiles directory")
        
    return func_profiles_path, taxa_profiles_path, sigs_aa_path, sigs_dna_path, temp_dir

def parse_args():
    parser = argparse.ArgumentParser(
        description='Extract Logan data to Parquet files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
    python extract_to_parquet.py --data-dir ./my_data --output-dir ./parquet_output
    python extract_to_parquet.py --workers 8 --resume
    python extract_to_parquet.py --no-signatures --no-taxa""")
    
    parser.add_argument('--data-dir', default=Config.DATA_DIR,
                       help=f'Data directory containing .tar.gz files (default: {Config.DATA_DIR})')
    parser.add_argument('--output-dir', default='./parquet_output',
                       help='Output directory for Parquet files (default: ./parquet_output)')
    parser.add_argument('--no-signatures', action='store_true',
                       help='Skip signature processing')
    parser.add_argument('--no-taxa', action='store_true',
                       help='Skip taxa profiles processing')
    parser.add_argument('--no-gather', action='store_true',
                       help='Skip gather files processing')
    parser.add_argument('--workers', type=int, default=Config.MAX_WORKERS,
                       help=f'Number of worker threads (default: {Config.MAX_WORKERS})')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from checkpoint')
    parser.add_argument('--no-cleanup', action='store_true',
                       help='Do not cleanup temporary files')
    parser.add_argument('--func-batch-size', type=int, default=1000,
                       help='Batch size for functional profiles (default: 1000)')
    parser.add_argument('--gather-batch-size', type=int, default=500,
                       help='Batch size for gather files (default: 500)')
    parser.add_argument('--signature-batch-size', type=int, default=50000,
                       help='Batch size for signature mins (default: 50000)')
    
    return parser.parse_args()

def setup_logger():
    """Setup logging configuration."""
    logs_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"extraction_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logging.info(f"Log file: {log_file}")
    return logging.getLogger()

def main():
    args = parse_args()
    setup_logger()
    
    # Create output directory
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    
    # Load checkpoint if resuming
    processed_archives = load_checkpoint() if args.resume else set()
    
    print(f"📁 Data directory: {args.data_dir}")
    print(f"📁 Output directory: {args.output_dir}")
    print(f"⚙️  Max workers: {args.workers}")
    print(f"🔄 Resume mode: {'Yes' if args.resume else 'No'}")
    print(f"🧹 Cleanup temp files: {'Yes' if not args.no_cleanup else 'No'}")
    
    # Find archives
    archive_files = glob.glob(os.path.join(args.data_dir, "*.tar.gz"))
    
    # Filter out already processed if resuming
    if args.resume:
        archive_files = [f for f in archive_files if f not in processed_archives]
        print(f"📊 Resuming: {len(processed_archives)} already processed, {len(archive_files)} remaining")
    
    # Process archives
    for archive_path in tqdm(archive_files, desc="Processing archives"):
        archive_name = os.path.basename(archive_path)
        
        try:
            # Extract archive
            func_dir, taxa_dir, sig_aa_dir, sig_dna_dir, temp_dir = extract_nested_archives(archive_path)
            
            # Process each data type
            process_functional_profiles(func_dir, args.output_dir, archive_name, 
                                       args.func_batch_size)
            
            if not args.no_gather:
                process_gather_files(func_dir, args.output_dir, archive_name,
                                   args.gather_batch_size)
                
            if not args.no_taxa:
                process_taxa_profiles(taxa_dir, args.output_dir, archive_name)
                
            if not args.no_signatures:
                process_signature_files(sig_aa_dir, "sigs_aa", args.output_dir, 
                                      archive_name, args.signature_batch_size)
                process_signature_files(sig_dna_dir, "sigs_dna", args.output_dir, 
                                      archive_name, args.signature_batch_size)
            
            # Cleanup
            if not args.no_cleanup and Config.CLEANUP_TEMP_FILES:
                shutil.rmtree(temp_dir)
                
            # Update checkpoint
            processed_archives.add(archive_path)
            save_checkpoint(processed_archives)
            
        except Exception as e:
            record_failure(archive_path, e)
            logging.error(f"Failed to process {archive_path}: {e}")
            continue
    
    # Process standalone files (taxonomy and geographical data)
    print("\n📊 Processing standalone data files...")
    
    # Note: These will be handled in the ingestion script since they're one-time loads
    
    # Final summary
    if FAILED_FILES:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fail_log = f"failed_extraction_{timestamp}.txt"
        with open(fail_log, 'w') as f:
            for fp, err in FAILED_FILES:
                f.write(f"{fp}\t{err}\n")
        print(f"\n⚠️  {len(FAILED_FILES)} file(s) failed. See {fail_log}")
    else:
        print("\n✅ All files extracted successfully!")
        
    # Print statistics
    print("\n📊 Extraction complete! Next steps:")
    print("1. Run ingest_from_parquet.py to load data into DuckDB")
    print("2. The Parquet files are organized by table and partitioned by sample_id prefix")

if __name__ == "__main__":
    main()
