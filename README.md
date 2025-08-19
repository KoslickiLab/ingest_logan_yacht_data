# Logan Metagenomic Database System

A high-performance pipeline for processing and analyzing large-scale metagenomic data from the NCBI SRA, with an AI-powered natural language query interface.

## Overview

This system processes terabytes of metagenomic data (functional profiles, taxonomic classifications, and sequence signatures) into a DuckDB database, then provides both SQL and natural language query capabilities through an AI assistant.

## Architecture

The pipeline has three main components:

1. **Data Ingestion**: Parallel extraction and conversion of `.tar.gz` archives to Parquet format
2. **Database Creation**: Import Parquet files into an indexed DuckDB database  
3. **Query Interface**: AI assistant that translates natural language to SQL

## Installation

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

## Data Processing Pipeline

### Step 1: Convert Archives to Parquet

Prep the geo data, and process Logan archives containing metagenomic analysis results:

```bash
# Add geographical data
python utils/merge_geo.py  # The Logan-provided geographic location data doesn't use sample_ids, but we can connect them via Bio Sample using the SRA's own metadata

python utils/export_to_parquet.py \
    --data-dir /path/to/archives \
    --staging-dir /scratch/parquet_staging \
    --producers 10 \
    --consumers 50 \
    --zip-workers 4
```

This extracts:
- Functional profiles (KEGG Orthology abundances)
- Taxonomic classifications  
- Sourmash signatures (DNA/protein min-hashes)
- Gather results (sequence similarity)

**Internal Note**: The archive resides on the Koslicki Lab GPU server at: `/scratch/shared_data_new/Logan_yacht_data/raw_downloads`. 

### Step 2: Create DuckDB Database

Import Parquet files and build indexes:

```bash
python utils/import_parquet_to_duckdb.py \
    --staging-dir /scratch/parquet_staging \
    --database logan.db \
    --threads 64
```

Add the `--fast` flag to skip slow indexing steps during testing.

### Step 3: Add Metadata (Optional)

Add the temporal metadata:

```bash
# Add sample dates
python utils/add_date_info.py \
    --db logan.db \
    --tsv Accessions_to_date_received.tsv
```

## Using the AI Query Interface

### Configure AI Provider

Set environment variables in `.env`:

```bash
# For OpenAI
LLM_PROVIDER=openai
API_KEY=your_api_key
MODEL=gpt-4o-mini

# For local Ollama
LLM_PROVIDER=ollama
OLLAMA_MODEL=llama3.1
OLLAMA_HOST=http://localhost:11434
```

### Launch the Interface

**Console mode:**
```bash
python main.py --database logan.db
```

**Web interface:**
```bash
python main.py --database logan.db --flask --flask-host 0.0.0.0
```

### Example Queries

Ask questions in natural language:

- "What are the most abundant KO functions in sample DRR012227?"
- "Which organisms are present in marine samples?"
- "Compare functional diversity between samples DRR012227 and DRR000001"
- "Show samples with high sequence similarity (containment > 0.8)"

## Diversity Analysis Pipeline

Analyze sequence diversity trends across the entire dataset:

### Step 1: Create Hash Buckets

```bash
python analysis/write_minhash_buckets.py \
    --db logan.db \
    --dest /scratch/minhash_buckets \
    --ksize 31
```

### Step 2: Compute Metrics

```bash
python analysis/compute_diversity_from_buckets.py \
    --db logan.db \
    --buckets /scratch/minhash_buckets \
    --out diversity_metrics.csv
```

### Step 3: Generate Plots

```bash
python analysis/plot_diversity_metrics_color.py \
    --csv diversity_metrics.csv \
    --outdir figures/
```

## Database Schema

Key tables and their purposes:

- `functional_profile.profiles`: KO gene abundances per sample
- `taxa_profiles.profiles`: Organism classifications with confidence scores
- `sigs_{aa,dna}.signature_mins`: Min-hash values for sequence comparison
- `functional_profile_data.gather_data`: Sequence similarity results
- `geographical_location_data.locations`: Sample metadata (location, biome)
- `sample_received`: Temporal metadata

## Performance Considerations

- **Memory**: Set DuckDB memory limit based on available RAM (`--mem 512GB`)
- **Disk Space**: Parquet staging requires ~2x the compressed archive size
- **Parallelism**: Adjust `--producers`, `--consumers`, and `--threads` based on system resources
- **Scratch Storage**: Use fast local storage for temporary files by setting `SCRATCH_TMPDIR=/path/to/scratch`

## Troubleshooting

**Out of Memory**: Reduce `--consumers` or process archives individually with `process_tar_gz_file.py`

**Index Creation Fails**: Use `--fast` flag to skip problematic indexes, or check for NULL-only columns

**AI Query Errors**: Run with `--retrain` to rebuild the AI model's understanding of the schema

## Citation

If you use this system in your research, please cite [appropriate paper/DOI].




