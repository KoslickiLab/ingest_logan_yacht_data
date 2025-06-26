# Metagenomics Database

This repository provides tools for processing and analyzing metagenomic data, with a focus on functional profiles, taxonomic classifications, sequence signatures, and geographical metadata. The data is stored in a specialized DuckDB database, designed to be queried using an AI assistant.

---

## 💡 The Concept

Instead of writing complex SQL, you can interact with your data through a simple conversational interface. The workflow is as follows:

1. **You**: Ask a question in plain English, like "What are the top 10 most abundant KO functions in sample DRR012227?"
2. **AI**: Analyzes your question, consults the database schema it has learned, and generates the appropriate SQL query.
3. **DuckDB**: Executes the SQL query and returns the results.
4. **AI**: Presents the results to you in a human-readable format (e.g., as a table or summary).

---

## 🚀 Getting Started

### Step 1: Prepare Your Data

Before running any scripts, place all your input files into the `./data` directory. The processing script will automatically find and handle:

- **`.tar.gz` Archives**: These should contain subdirectories for different data types:
  - `func_profiles/` - Functional profiles (KO abundance).
  - `taxa_profiles/` - Taxonomic classifications.
  - `sigs_aa/` & `sigs_dna/` - sourmash signature files.
  - Gather results files.
- **`.tsv` Taxonomy Files**: Used to map genome identifiers to taxonomy IDs.
- **`.csv` Geodata Files**: Geographic metadata for samples (e.g., `biosample_geographical_location.*.csv`).
- **SRA_Accessions File**: A list of SRA accession IDs.

### Step 2: Process Data and Build the Database

The main script, `process_functional_profiles.py`, orchestrates the entire data pipeline:

#### `process_geo.py`

- Merges geographical metadata with SRA accession data.
- Links BioSample IDs from geographical CSVs to sample IDs from the SRA file.
- Outputs a merged CSV (`output.csv`) for further integration.
- Run this script first, as soon as you have prepared your data.

#### `process_functional_profiles.py`

- **Main orchestrator script** for the pipeline.
- Extracts `.tar.gz` archives and processes:
  - **Functional profiles** (`func_profiles/`): Loads KO abundance tables into DuckDB.
  - **Taxonomic profiles** (`taxa_profiles/`): Loads Excel files with organism/taxonomy info.
  - **Signatures** (`sigs_aa/`, `sigs_dna/`): Loads sourmash signature ZIPs, parses manifests, metadata, and min-hash values, and inserts them in chunks for efficiency.
  - **Gather results**: Loads sequence similarity results.
  - **Geographical metadata**: Loads and merges geographical CSVs, handling large files in chunks.
- **Taxonomy mapping**: Integrates taxonomy IDs from `.tsv` files into the taxonomic profiles.
- **Database**: All data is loaded into a DuckDB database with multiple schemas and indexed tables for fast querying.
- **Logging**: All steps are logged to file and console for reproducibility and debugging.

#### `db_schema.py`

- Provides programmatic access to the database schema, documentation, and a large set of example SQL queries for analysis.
- Useful for AI-powered assistants or for users to understand and explore the database.

---

To run the processing, execute the following command in your terminal:

```bash
python process_functional_profiles.py --data_dir ./data --database database.db
```

You can customize the run with optional flags:
- `--workers 8`: Increase the number of parallel processing threads.
- `--batch-size 5000`: Adjust the batch size for processing.
- `--no-signatures`: Skip the processing of signature files.
- `--no-taxa`: Skip the processing of taxonomic profiles.
- `--continue-on-error`: Continue processing if errors occur.

For geographical data processing:
```bash
python process_geo.py
```
---

## Database Structure

The database uses DuckDB and is organized into multiple schemas containing different types of metagenomic analysis data:

### 🧬 `functional_profile` Schema

Contains KEGG Orthology (KO) functional annotations with abundance data.

**Table: `profiles`** (Unified table for all samples)
```sql
CREATE TABLE functional_profile.profiles (
    sample_id VARCHAR,          -- Sample identifier (DRR######)
    ko_id VARCHAR,              -- KEGG Orthology ID (e.g., 'K00001', 'ko:K00001')
    abundance DOUBLE            -- Quantitative abundance measurement
);
```

### 🦠 `taxa_profiles` Schema

Contains taxonomic classification results with organism identification and confidence metrics.

**Table: `profiles`** (Unified table for all samples)
```sql
CREATE TABLE taxa_profiles.profiles (
    sample_id VARCHAR,                          -- Sample identifier (DRR######)
    organism_name VARCHAR,                      -- Full organism name from reference
    organism_id VARCHAR,                        -- Extracted NCBI accession (GCA_/GCF_)
    tax_id INTEGER,                             -- NCBI taxonomy ID (-1 if not mapped)
    num_unique_kmers_in_genome_sketch INTEGER,  -- Unique k-mers in genome sketch
    num_total_kmers_in_genome_sketch INTEGER,   -- Total k-mers in genome sketch
    scale_factor DOUBLE,                        -- Scale factor for sketching
    num_exclusive_kmers_in_sample_sketch INTEGER, -- Exclusive k-mers in sample
    num_total_kmers_in_sample_sketch INTEGER,   -- Total k-mers in sample sketch
    min_coverage DOUBLE,                        -- Minimum coverage threshold
    p_vals DOUBLE,                              -- Statistical p-values
    num_exclusive_kmers_to_genome INTEGER,      -- Exclusive k-mers to this genome
    num_exclusive_kmers_to_genome_coverage DOUBLE, -- Coverage of exclusive k-mers
    num_matches INTEGER,                        -- Number of k-mer matches
    acceptance_threshold_with_coverage DOUBLE,  -- Acceptance threshold with coverage
    actual_confidence_with_coverage DOUBLE,     -- Confidence score with coverage
    alt_confidence_mut_rate_with_coverage DOUBLE, -- Alternative confidence metric
    in_sample_est BOOLEAN                       -- Boolean estimate if organism is in sample
);
```

### 🔍 `functional_profile_data` Schema

Contains sourmash gather analysis results showing sequence similarity.

**Table: `gather_data`** (All samples combined)
```sql
CREATE TABLE functional_profile_data.gather_data (
    sample_id VARCHAR,                   -- Sample identifier (DRR######)
    intersect_bp INTEGER,                -- Base pairs of intersection
    jaccard DOUBLE,                      -- Jaccard similarity index (0-1)
    max_containment DOUBLE,              -- Maximum containment score (0-1)
    f_query_match DOUBLE,                -- Fraction of query matched (0-1)
    f_match_query DOUBLE,                -- Fraction of match covered by query
    match_filename VARCHAR,              -- Filename of matching reference
    match_name VARCHAR,                  -- Name of matching reference sequence
    match_md5 VARCHAR,                   -- MD5 hash of match
    match_bp BIGINT,                    -- Base pairs in match
    query_filename VARCHAR,              -- Query filename
    query_name VARCHAR,                  -- Query sequence name
    query_md5 VARCHAR,                   -- MD5 hash of query
    query_bp INTEGER,                    -- Base pairs in query
    ksize INTEGER,                       -- K-mer size used
    moltype VARCHAR,                     -- Molecule type (DNA/protein)
    scaled INTEGER,                      -- Scaling factor for sketching
    query_n_hashes INTEGER,              -- Number of hashes in query
    query_abundance BOOLEAN,             -- Whether query has abundance data
    query_containment_ani DOUBLE,        -- Query containment ANI
    match_containment_ani DOUBLE,        -- Match containment ANI
    average_containment_ani DOUBLE,      -- Average nucleotide identity (%)
    max_containment_ani DOUBLE,          -- Maximum containment ANI
    potential_false_negative BOOLEAN     -- Flag for potential false negatives
);
```

### 🧪 `sigs_aa` Schema (Protein Signatures)

Contains sourmash protein signature data including manifests, metadata, and min-hash values.

**Table: `manifests`** (Signature file metadata)
```sql
CREATE TABLE sigs_aa.manifests (
    internal_location VARCHAR,      -- Internal file location in archive
    md5 VARCHAR,                    -- MD5 hash of signature
    md5short VARCHAR,               -- Shortened MD5 hash
    ksize INTEGER,                  -- K-mer size used
    moltype VARCHAR,                -- Molecule type (protein)
    num INTEGER,                    -- Number parameter
    scaled INTEGER,                 -- Scaling factor for sketching
    n_hashes INTEGER,               -- Number of hashes in signature
    with_abundance BOOLEAN,         -- Whether signature has abundance data
    name VARCHAR,                   -- Signature name
    filename VARCHAR,               -- Original filename
    sample_id VARCHAR,              -- Sample identifier (DRR######)
    archive_file VARCHAR            -- Source archive filename
);
```

**Table: `signatures`** (Detailed signature metadata)
```sql
CREATE TABLE sigs_aa.signatures (
    md5 VARCHAR,                    -- MD5 hash linking to manifest
    sample_id VARCHAR,              -- Sample identifier (DRR######)
    hash_function VARCHAR,          -- Hash function used (FNV, etc.)
    molecule VARCHAR,               -- Molecule type (protein)
    filename VARCHAR,               -- Signature filename
    class VARCHAR,                  -- Signature class
    email VARCHAR,                  -- Contact email from signature
    license VARCHAR,                -- License information
    ksize INTEGER,                  -- K-mer size
    seed INTEGER,                   -- Random seed used
    max_hash BIGINT,                -- Maximum hash value
    num_mins INTEGER,               -- Number of min-hash values
    signature_size INTEGER,         -- Estimated signature size in bytes
    has_abundances BOOLEAN,         -- Whether signature contains abundances
    archive_file VARCHAR            -- Source archive filename
);
```

**Table: `signature_mins`** (Unified min-hash values for all samples)
```sql
CREATE TABLE sigs_aa.signature_mins (
    sample_id VARCHAR,              -- Sample identifier (DRR######)
    md5 VARCHAR,                    -- MD5 hash linking to signature metadata
    min_hash BIGINT,                -- Individual min-hash value
    abundance INTEGER,              -- Abundance for this hash (1 if no abundance)
    position INTEGER                -- Position index in signature
);
```

### 🧬 `sigs_dna` Schema (DNA Signatures)

Contains sourmash DNA signature data with identical structure to protein signatures.

**Tables**: `manifests`, `signatures`, `signature_mins` (same structure as `sigs_aa` but for DNA data)

### 📊 Key Relationships and Indexes

**Primary Keys and Relationships:**
- `sample_id` links all data across schemas (format: DRR######)
- `md5` links signatures across manifests, signatures, and signature_mins tables
- `organism_id` contains extracted NCBI accessions (GCA_/GCF_)
- `tax_id` provides NCBI taxonomy integration (-1 = unmapped)

**Performance Indexes:**
```sql
-- Functional profiles
CREATE INDEX idx_functional_profile_sample_id ON functional_profile.profiles (sample_id);
CREATE INDEX idx_functional_profile_ko_id ON functional_profile.profiles (ko_id);

-- Taxa profiles
CREATE INDEX idx_taxa_profiles_sample_id ON taxa_profiles.profiles (sample_id);
CREATE INDEX idx_taxa_profiles_organism_id ON taxa_profiles.profiles (organism_id);
CREATE INDEX idx_taxa_profiles_tax_id ON taxa_profiles.profiles (tax_id);

-- Signature indexes for both sigs_aa and sigs_dna
CREATE INDEX idx_sigs_aa_signature_mins_sample_id ON sigs_aa.signature_mins (sample_id);
CREATE INDEX idx_sigs_dna_signature_mins_sample_id ON sigs_dna.signature_mins (sample_id);
```

## 📊 Data Processing

### Prepare Your Data

Place your `.tar.gz` archive files and `.tsv` taxonomy mapping files in the `./data` directory. Archives should contain:
- `func_profiles/` - Functional profile CSV files
- `taxa_profiles/` - Taxonomic classification Excel files
- `sigs_aa/` - Protein signature ZIP files
- `sigs_dna/` - DNA signature ZIP files

### Process the Data

```bash
# Basic processing
python process_functional_profiles.py

# With custom settings
python process_functional_profiles.py --data-dir ./my_data --workers 8 --batch-size 5000

# Skip certain data types
python process_functional_profiles.py --no-signatures --no-taxa
```

### Processing Options

- `--data-dir` - Data directory containing .tar.gz files (default: ./data)
- `--database` - Output database path (default: database.db)
- `--workers` - Number of worker threads (default: 4)
- `--batch-size` - Batch size for processing (default: 10000)
- `--no-signatures` - Skip signature processing
- `--no-taxa` - Skip taxa profiles processing
- `--no-gather` - Skip gather files processing
- `--no-progress` - Disable progress bars
- `--continue-on-error` - Continue processing on errors


```

### Interactive Commands

- `help` - Show example questions
- `schema` - Display database structure
- `retrain` - Update AI model with latest data
- `quit` - Exit the program

## 📝 Example Queries

### Basic Database Exploration
```
How many samples are in the database?
List all functional profile samples
Show me all available sample IDs
```

### Functional Profile Analysis
```
What are the top 10 most abundant KO functions in sample DRR012227?
Find functional diversity for sample DRR012227
Show me samples with specific KO function K00001
Compare functional profiles between samples DRR012227 and DRR000001
```

### Taxonomic Analysis
```
Get taxonomic composition for sample DRR012227 with high confidence
Find samples containing Escherichia coli in taxa profiles
Show me organisms with highest confidence in sample DRR012227
What is the average number of organisms detected per sample?
```

### Sequence Similarity Analysis
```
Find samples with high quality matches (containment > 0.8)
Show sequence similarity statistics
Get samples with the most diverse microbial communities
Find samples with low sequence coverage
```

### Signature Analysis
```
Compare signature statistics between protein and DNA
Show k-mer size distribution in signatures
Find samples with highest hash counts in protein signatures
Analyze hash abundance distribution across samples
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file or set environment variables:

```env


# Database Configuration
DATABASE_PATH=database.db
DATA_DIR=./data

# Performance Settings
MAX_WORKERS=4
BATCH_SIZE=10000
DATABASE_MEMORY_LIMIT=4GB


# Processing Options
SIGNATURE_PROCESSING_ENABLED=true
TAXA_PROCESSING_ENABLED=true
GATHER_PROCESSING_ENABLED=true
```

### Command Line Configuration

All environment variables can be overridden with command line arguments:

```bash
python main.py --ai-provider github \
               --api-key github_pat_... \
               --model openai/gpt-4o-mini \
               --database custom.db \
               --chromadb-path ./custom_chroma
```

## 🚨 Troubleshooting

### Common Issues

**Database not found:**
```bash
# Make sure to process data first
python process_functional_profiles.py
```


**Memory issues with large datasets:**
```bash
# Reduce batch size and workers
python process_functional_profiles.py --batch-size 1000 --workers 2

# Increase database memory limit
export DATABASE_MEMORY_LIMIT=8GB
```
 
### Performance Optimization

**For large datasets:**
- Increase `MAX_WORKERS` based on CPU cores
- Adjust `BATCH_SIZE` (larger = more memory, faster processing)
- Set `DATABASE_MEMORY_LIMIT` to 50-70% of available RAM
- Use SSD storage for better I/O performance


## 📊 Data Sources

### Supported Input Formats

- **Functional Profiles**: CSV files with ko_id and abundance columns
- **Taxa Profiles**: Excel files (.xlsx) with organism classification data
- **Gather Results**: CSV files from sourmash gather analysis
- **Signatures**: ZIP archives containing sourmash signature files
- **Taxonomy Mapping**: TSV files with genome_id and taxid columns

### Sample ID Format

All samples should follow the pattern `DRR######` (DRR followed by 6 digits), e.g., `DRR012227`, `DRR000001`.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.


## 🔮 Roadmap

- [ ] Support for additional metagenomic data formats
- [ ] Advanced phylogenetic analysis features
- [ ] Real-time data streaming capabilities
- [ ] Enhanced visualization components
- [ ] Integration with cloud databases
- [ ] Multi-language support for queries
python process_functional_profiles.py --batch-size 1000 --workers 2

# Increase database memory limit
export DATABASE_MEMORY_LIMIT=8GB
```

### Performance Optimization

**For large datasets:**
- Increase `MAX_WORKERS` based on CPU cores
- Adjust `BATCH_SIZE` (larger = more memory, faster processing)
- Set `DATABASE_MEMORY_LIMIT` to 50-70% of available RAM
- Use SSD storage for better I/O performance


## 📊 Data Sources

### Supported Input Formats

- **Functional Profiles**: CSV files with ko_id and abundance columns
- **Taxa Profiles**: Excel files (.xlsx) with organism classification data
- **Gather Results**: CSV files from sourmash gather analysis
- **Signatures**: ZIP archives containing sourmash signature files
- **Taxonomy Mapping**: TSV files with genome_id and taxid columns

### Sample ID Format

All samples should follow the pattern `DRR######` (DRR followed by 6 digits), e.g., `DRR012227`, `DRR000001`.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.


## 🔮 Roadmap

- [ ] Support for additional metagenomic data formats
- [ ] Advanced phylogenetic analysis features
- [ ] Real-time data streaming capabilities
- [ ] Enhanced visualization components
- [ ] Integration with cloud databases
- [ ] Multi-language support for queries
