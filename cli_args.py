"""
Command line argument parsing and configuration management
"""
import argparse
from config import Config


def parse_processing_args():
    """Parse command line arguments for process_functional_profiles.py"""
    parser = argparse.ArgumentParser(
        description='Process functional profile data archives',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python process_functional_profiles.py --data-dir ./my_data --workers 8
  python process_functional_profiles.py --database custom.db --no-signatures
  python process_functional_profiles.py --batch-size 5000 --no-progress
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

    
    retrain_source = "command line" if args.retrain else "config"
    progress_source = "command line" if args.no_progress else "config"
    print(f"Progress Bars: {not args.no_progress and Config.PROGRESS_BAR_ENABLED} (from {progress_source})")
    print("=" * 30)
