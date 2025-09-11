import argparse
import logging
import os

from tqdm import tqdm

from ai_assistant import FunctionalProfileVanna
from config import Config
import datetime
from pathlib import Path

# Create logs directory if it doesn't exist
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Configure logging with file output
log_filename = f"logan_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = logs_dir / log_filename

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()  # Also output to console
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main function to run the AI interface"""
    try:
        config = Config()
        
        parser = argparse.ArgumentParser(
            description="Functional Profile AI Assistant",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        parser.add_argument('--ai-provider', '--provider', 
                           choices=['ollama', 'openai'],
                           help='AI provider to use',
                           default=config.LLM_PROVIDER)
        
        parser.add_argument('--database', '--db',
                           help=f'Database path',
                           default=config.DATABASE_PATH)
        
        parser.add_argument('--retrain', '--force-retrain',
                           action='store_true',
                           help='Force retrain AI model')
        
        parser.add_argument('--no-progress',
                           action='store_true',
                           help='Disable progress bars')
        
        parser.add_argument('--flask',
                           action='store_true',
                           help='Launch Flask web interface instead of CLI')
        
        parser.add_argument('--flask-host',
                           default='127.0.0.1',
                           help='Flask host (default: 127.0.0.1)')
        
        parser.add_argument('--flask-port',
                           type=int,
                           default=5000,
                           help='Flask port (default: 5000)')
        
        parser.add_argument('--validate-config',
                           action='store_true',
                           help='Validate configuration and exit')
        
        args = parser.parse_args()
        
        # Update config with command line arguments
        config.LLM_PROVIDER = args.ai_provider
        config.DATABASE_PATH = args.database
        
        # Validate configuration if requested
        if args.validate_config:
            try:
                config.validate()
                print("✅ Configuration is valid!")
                return 0
            except ValueError as e:
                print(f"❌ Configuration validation failed: {e}")
                return 1
        
        # Validate database exists
        db_path = args.database if args.database else config.DATABASE_PATH
        if not os.path.exists(db_path):
            print(f"❌ Error: Database {db_path} not found!")
            print("Please run the data processing pipeline first to create the database.")
            print("See README.md for instructions.")
            return 1
        
        force_retrain = args.retrain
        progress_enabled = not args.no_progress and config.PROGRESS_BAR_ENABLED
        
        print("🚀 Initializing AI assistant with functional profile database...")
        
        with tqdm(total=3, desc="Initializing", unit="step", disable=not progress_enabled) as pbar:
            pbar.set_description("Creating AI instance")
            ai_assistant = FunctionalProfileVanna(
                db_path=db_path,
                config=config
            )
            pbar.update(1)
            
            pbar.set_description("Setting up training data")
            ai_assistant.setup_training_data(force_retrain=force_retrain)
            pbar.update(1)
            
            pbar.set_description("Ready!")
            pbar.update(1)
        
        print("✅ Ready! You can now ask questions about your data.\n")
        
        if args.flask:
            print("🌐 Starting Flask web interface...")
            ai_assistant.launch_flask_app(
                host=args.flask_host,
                port=args.flask_port,
                debug=False
            )
        else:
            ai_assistant.interactive_mode()
        
        return 0
        
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        return 0
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        print(f"❌ File not found: {e}")
        return 1
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        print(f"❌ Configuration error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"❌ Unexpected error: {str(e)}")
        print("Please check the logs for more details.")
        return 1

if __name__ == "__main__":
    main()
