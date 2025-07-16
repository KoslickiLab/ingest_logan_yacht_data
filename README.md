# Metagenomic Database AI Assistant

This project provides intelligent analysis of metagenomic data using artificial intelligence. Instead of writing complex SQL queries, you can interact with your data through a simple conversational interface.

## 🧬 Key Features

- **Metagenomic data processing**: Automated loading and indexing of functional profiles, taxonomic classifications, sequence signatures, and geographical metadata
- **AI assistant**: Natural language interface for database queries
- **Multiple AI provider support**: OpenAI, Ollama
- **High-performance database**: DuckDB for fast analytical queries
- **Web interface**: Flask application for convenient browser-based access

## 💡 How it works

1. **You**: Ask questions in Ukrainian or English, for example: "What are the most abundant KO functions in sample DRR012227?"
2. **AI**: Analyzes your question, consults the database schema, and generates the appropriate SQL query
3. **DuckDB**: Executes the SQL query and returns results
4. **AI**: Presents results in an understandable format (table, summary, explanation)

## 🚀 Quick Start

### Step 1: Install Dependencies

It's recommended to use a virtual environment:

```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Linux/Mac)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Prepare Data

Place your data files in the `./data` folder. The following file types are supported:

#### `.tar.gz` Archives
Should contain subdirectories with different data types:
- `func_profiles/` - Functional profiles (KO abundance in CSV format)
- `taxa_profiles/` - Taxonomic classifications (Excel files)
- `sigs_aa/` - Protein signatures (ZIP files with sourmash signatures)
- `sigs_dna/` - DNA signatures (ZIP files with sourmash signatures)
- Gather results (files `*.unitigs.fa_gather_*.tmp`)

#### Other Files
- **`.tsv` taxonomy files**: For mapping genome identifiers to taxonomy IDs (e.g., `GTDB_rs14_genome_to_taxid.tsv`)
- **`.csv` geographical data**: Metadata about sample collection sites (e.g., `biosample_geographical_location.*.csv`)

### Step 3: Load Data into Database

Run the processing script to create the database:

```bash
python utils/import_to_db.py
```

Command line parameters:
- `--data-dir ./data` - Folder with input data
- `--database database.db` - Path to database file
- `--workers 8` - Number of processing threads
- `--batch-size 10000` - Batch size for processing
- `--no-progress` - Disable progress bars

Example with custom settings:
```bash
python utils/import_to_db.py --workers 8 --batch-size 5000 --data-dir ./my_data
```

### Step 4: Configure AI Provider

Create a `.env` file or set environment variables:

#### For OpenAI:
```bash
export LLM_PROVIDER=openai
export API_KEY=your_openai_api_key
export MODEL=gpt-4o-mini
```

#### For OpenAI-compatible services (e.g., custom endpoints):
```bash
export LLM_PROVIDER=openai
export API_KEY=your_api_key
export BASE_URL=https://your-custom-endpoint.com/v1
export MODEL=gpt-4o-mini
```

#### For Ollama (local):
```bash
export LLM_PROVIDER=ollama
export OLLAMA_MODEL=llama3.1
export OLLAMA_HOST=http://localhost:11434
```

### Step 5: Run the Application

#### Console interface:
```bash
python main.py
```

#### Web interface:
```bash
python main.py --flask --flask-host 0.0.0.0 --flask-port 5000
```

## 📋 main.py Launch Parameters

- `--ai-provider` - AI provider (ollama, openai)
- `--database` - Database path (default: database.db)
- `--retrain` - Force retrain AI model
- `--no-progress` - Disable progress bars
- `--flask` - Launch web interface instead of console
- `--flask-host` - Flask host (default: 127.0.0.1)
- `--flask-port` - Flask port (default: 5000)

Examples:
```bash
# Run with forced retraining
python main.py --retrain

# Run web interface on all interfaces
python main.py --flask --flask-host 0.0.0.0

# Use specific AI provider
python main.py --ai-provider ollama
```

---

## 📝 Example Queries

After launching the application, you can ask questions in natural language:

### Functional Analysis
```
What are the most abundant KO functions in sample DRR012227?
Compare functional profiles between samples DRR012227 and DRR000001
Show top 10 KO functions by abundance across all samples
```

### Taxonomic Analysis
```
Which organisms are present in sample DRR012227?
Show taxonomic composition of the sample with highest confidence
Find samples containing Escherichia coli
```

### Geographical Analysis
```
Which countries do the samples come from?
Show sample distribution by biomes
Find samples from marine environments
```

### Complex Queries
```
Which KO functions are most common in marine samples?
Compare taxonomic composition between terrestrial and marine samples
```

## 🛠 Interactive Commands

Special commands available in console mode:
- `help` - Show example questions
- `schema` - Display database structure
- `retrain` - Update AI model with latest data
- `quit` - Exit the program

## 🧪 Testing

The project includes comprehensive tests for all major components. Run tests using pytest:

```bash
# Install test dependencies (if not already installed)
pip install pytest

# Run all tests
pytest

# Run tests with verbose output
pytest -v

# Run specific test file
pytest tests/test_database_operations.py

# Run tests with coverage report
pip install pytest-cov
pytest --cov=. --cov-report=html

# Run tests in parallel (faster execution)
pip install pytest-xdist
pytest -n auto
```

### Test Structure

Tests are organized in the `tests/` directory:
- `test_database_operations.py` - Database connection and query tests
- `test_file_processing.py` - Data file processing tests
- `test_data_insertion.py` - Data insertion and validation tests
- `test_archive_extraction.py` - Archive extraction tests
- `test_utility_functions.py` - Helper function tests

### Running Tests Before Deployment

It's recommended to run tests before deploying or making significant changes:

```bash
# Full test suite
pytest tests/ -v

# Quick smoke tests (excluding slow integration tests)
pytest tests/ -m "not slow" -v
```

---

## 🔧 Configuration

All settings can be changed through environment variables or `.env` file:

```bash
# AI provider
LLM_PROVIDER=openai
API_KEY=your_api_key
MODEL=gpt-4o-mini

# Database
DATABASE_PATH=database.db
DATABASE_MEMORY_LIMIT=4GB
DATABASE_THREADS=4

# Data processing
BATCH_SIZE=10000
MAX_WORKERS=8
PROGRESS_BAR_ENABLED=true

# Logging
LOG_LEVEL=INFO
```

## 🚨 Troubleshooting

### Error: Database not found
```bash
❌ Error: Database database.db not found!
```
**Solution**: First run `python utils/import_to_db.py` to create the database.

### Error: IndexError in get_training_plan_generic
**Cause**: Database doesn't contain expected INFORMATION_SCHEMA structure.
**Solution**: Ensure data is loaded correctly and database contains tables with data.

### AI provider connection error
**Solution**: Check API key settings and provider availability.

## 📄 License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

