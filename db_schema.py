import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)

def get_database_schema(conn):
    """
    Get comprehensive schema information from the database
    
    Args:
        conn: DuckDB connection object
        
    Returns:
        dict: Schema information organized by schema -> table -> columns
    """
    schema_info = {}
    
    try:
        print("🔍 Discovering database schemas...")
        schemas_result = conn.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'main', 'pg_catalog', 'temp')
        """).fetchall()
        
        schema_progress = tqdm(schemas_result, desc="Loading schemas", unit="schema")
        
        for (schema_name,) in schema_progress:
            schema_progress.set_postfix(schema=schema_name)
            schema_info[schema_name] = {}
            
            tables_result = conn.execute(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{schema_name}'
                ORDER BY table_name
            """).fetchall()
            
            table_progress = tqdm(tables_result, 
                                desc=f"Tables in {schema_name}", 
                                unit="table", 
                                leave=False)
            
            for (table_name,) in table_progress:
                table_progress.set_postfix(table=table_name)
                
                columns_result = conn.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
                    ORDER BY ordinal_position
                """).fetchall()
                
                schema_info[schema_name][table_name] = [
                    {
                        'name': col_name,
                        'type': data_type,
                        'nullable': is_nullable == 'YES'
                    }
                    for col_name, data_type, is_nullable in columns_result
                ]
    
    except Exception as e:
        logger.error(f"Error getting database schema: {str(e)}")
    
    return schema_info

def get_sample_queries():
    """
    Get sample SQL queries with descriptions for training Vanna AI
    
    Returns:
        dict: Description -> SQL query pairs
    """
    
    print("📝 Loading sample queries for AI training...")
    
    sample_queries = {
        "How many functional profiles are in the database": """
            SELECT COUNT(DISTINCT sample_id) as total_samples
            FROM functional_profile.profiles
        """,
        
        "List all functional profile samples": """
            SELECT DISTINCT sample_id
            FROM functional_profile.profiles
            ORDER BY sample_id
        """,
        
        "List all taxonomic profile sample IDs": """
            SELECT DISTINCT sample_id
            FROM taxa_profiles.profiles
            ORDER BY sample_id
        """,
        
        "Get top 10 most abundant KO functions in sample DRR012573": """
            SELECT ko_id, abundance 
            FROM functional_profile.profiles
            WHERE sample_id = 'DRR012573'
            ORDER BY abundance DESC
            LIMIT 10
        """,
        
        "Find functional diversity (number of KO functions) for sample DRR012573": """
            SELECT 
                sample_id,
                COUNT(DISTINCT ko_id) as number_kos,
                SUM(abundance) as total_abundance,
                AVG(abundance) as avg_abundance
            FROM functional_profile.profiles
            WHERE sample_id = 'DRR012573'
            GROUP BY sample_id
        """,
        
        "Check if sample has specific KO function K01601": """
            SELECT sample_id, ko_id, abundance 
            FROM functional_profile.profiles 
            WHERE ko_id IN ('ko:K01601')
        """,
        
        "Find top 10 most abundant KO functions across all samples": """
            SELECT 
                ko_id,
                SUM(abundance) as total_abundance,
                COUNT(DISTINCT sample_id) as sample_count,
                AVG(abundance) as avg_abundance
            FROM functional_profile.profiles
            GROUP BY ko_id
            ORDER BY total_abundance DESC
            LIMIT 10
        """,
        
        "Compare the count, total abundance, and average abundance of KOs between samples DRR012573 and DRR012574": """
            SELECT 
                sample_id,
                COUNT(DISTINCT ko_id) as ko_diversity,
                SUM(abundance) as total_abundance,
                AVG(abundance) as avg_abundance
            FROM functional_profile.profiles
            WHERE sample_id IN ('DRR012573', 'DRR012574')
            GROUP BY sample_id
            ORDER BY ko_diversity DESC
        """,
        
        "Return all functional profiles ordered by abundance": """
            SELECT 
                sample_id,
                ko_id,
                abundance
            FROM functional_profile.profiles
            ORDER BY abundance DESC
        """,
        
        "Get functional profile statistics per sample for the top 20 samples with the most KOs": """
            SELECT 
                sample_id,
                COUNT(DISTINCT ko_id) as unique_ko_functions,
                SUM(abundance) as total_abundance,
                AVG(abundance) as avg_abundance,
                MAX(abundance) as max_abundance,
                MIN(abundance) as min_abundance
            FROM functional_profile.profiles
            GROUP BY sample_id
            ORDER BY unique_ko_functions DESC
            LIMIT 20
        """,
        
        "Find the top 20 (in terms of total abunance) KOs that appear in more than 5 samples": """
            SELECT 
                ko_id,
                COUNT(DISTINCT sample_id) as present_in_samples,
                SUM(abundance) as total_abundance,
                AVG(abundance) as avg_abundance,
                MAX(abundance) as max_abundance
            FROM functional_profile.profiles
            GROUP BY ko_id
            HAVING COUNT(DISTINCT sample_id) > 5
            ORDER BY total_abundance DESC
            LIMIT 20
        """,
        
        "Compare KO function abundance between two samples DRR014155 and DRR014163": """
            SELECT 
                fp1.ko_id,
                fp1.abundance as sample1_abundance,
                fp2.abundance as sample2_abundance,
                ABS(fp1.abundance - fp2.abundance) as abundance_diff
            FROM functional_profile.profiles fp1
            JOIN functional_profile.profiles fp2 ON fp1.ko_id = fp2.ko_id
            WHERE fp1.sample_id = 'DRR014155' 
            AND fp2.sample_id = 'DRR014163'
            ORDER BY abundance_diff DESC
            LIMIT 20
        """,
        
        "Show the average fraction of the query that matched, the average max containment, and the average containment ANI in all the functiona profile data": """
            SELECT 
                COUNT(DISTINCT sample_id) as num_samples,
                COUNT(*) as total_matches,
                ROUND(AVG(f_query_match), 3) as avg_query_coverage,
                ROUND(AVG(max_containment), 3) as avg_containment,
                ROUND(AVG(average_containment_ani), 2) as avg_ani
            FROM functional_profile_data.gather_data
        """,
        
        "Find samples with high quality matches (containment > 0.8)": """
            SELECT 
                sample_id,
                COUNT(*) as num_high_quality_matches,
                ROUND(AVG(max_containment), 3) as avg_containment,
                ROUND(AVG(average_containment_ani), 2) as avg_ani
            FROM functional_profile_data.gather_data
            WHERE max_containment > 0.8
            GROUP BY sample_id
            ORDER BY avg_containment DESC
        """,
        
        "Get organism diversity per sample from gather data": """
            SELECT 
                sample_id,
                COUNT(DISTINCT match_name) as organism_diversity,
                ROUND(AVG(f_query_match), 3) as avg_coverage,
                ROUND(AVG(average_containment_ani), 2) as avg_ani
            FROM functional_profile_data.gather_data
            GROUP BY sample_id
            ORDER BY organism_diversity DESC
            LIMIT 20
        """,
        
        "Get taxonomic composition for sample DRR014147 with high confidence": """
            SELECT 
                organism_name, 
                organism_id,
                tax_id,
                ROUND(actual_confidence_with_coverage, 3) as confidence,
                in_sample_est,
                num_exclusive_kmers_to_genome
            FROM taxa_profiles.profiles
            WHERE sample_id = 'DRR014147'
            AND in_sample_est = true 
            AND actual_confidence_with_coverage > 0.5
            ORDER BY actual_confidence_with_coverage DESC
            LIMIT 15
        """,
        
        "Find organisms with highest confidence in sample DRR014147": """
            SELECT 
                organism_name,
                organism_id,
                tax_id,
                ROUND(actual_confidence_with_coverage, 3) as confidence,
                num_exclusive_kmers_to_genome
            FROM taxa_profiles.profiles
            WHERE sample_id = 'DRR014147'
            AND actual_confidence_with_coverage > 0.8
            ORDER BY actual_confidence_with_coverage DESC
        """,
        
        "Count organisms detected in sample DRR014147": """
            SELECT 
                sample_id,
                COUNT(*) as total_organisms,
                COUNT(CASE WHEN in_sample_est = true THEN 1 END) as confirmed_organisms,
                ROUND(AVG(actual_confidence_with_coverage), 3) as avg_confidence
            FROM taxa_profiles.profiles
            WHERE sample_id = 'DRR014147'
            GROUP BY sample_id
        """,
        
        "Find samples containing Escherichia coli in taxa profiles": """
            SELECT 
                sample_id,
                organism_name,
                organism_id,
                tax_id,
                ROUND(actual_confidence_with_coverage, 3) as confidence
            FROM taxa_profiles.profiles
            WHERE LOWER(organism_name) LIKE '%escherichia%coli%'
            AND in_sample_est = true
            ORDER BY actual_confidence_with_coverage DESC
        """,
        
        "Get most common organisms across samples with taxonomy info": """
            SELECT 
                organism_name,
                organism_id,
                tax_id,
                COUNT(DISTINCT sample_id) as present_in_samples,
                ROUND(AVG(actual_confidence_with_coverage), 3) as avg_confidence,
                COUNT(CASE WHEN in_sample_est = true THEN 1 END) as confirmed_detections
            FROM taxa_profiles.profiles
            WHERE in_sample_est = true
            GROUP BY organism_name, organism_id, tax_id
            HAVING COUNT(DISTINCT sample_id) >= 2
            ORDER BY present_in_samples DESC, avg_confidence DESC
            LIMIT 20
        """,
        
        "Get taxonomy mapping statistics": """
            SELECT 
                'Total records' as category,
                COUNT(*) as count
            FROM taxa_profiles.profiles
            UNION ALL
            SELECT 
                'Records with taxonomy ID' as category,
                COUNT(*) as count
            FROM taxa_profiles.profiles
            WHERE tax_id != -1
            UNION ALL
            SELECT 
                'Records without taxonomy ID' as category,
                COUNT(*) as count
            FROM taxa_profiles.profiles
            WHERE tax_id = -1
            UNION ALL
            SELECT 
                'Unique taxonomy IDs' as category,
                COUNT(DISTINCT tax_id) as count
            FROM taxa_profiles.profiles
            WHERE tax_id != -1
        """,
        
        "Find organisms by specific taxonomy ID: 515619": """
            SELECT 
                sample_id,
                organism_name,
                organism_id,
                ROUND(actual_confidence_with_coverage, 3) as confidence,
                in_sample_est
            FROM taxa_profiles.profiles
            WHERE tax_id = 515619
            ORDER BY actual_confidence_with_coverage DESC
        """,

        "Compare signature statistics between protein and DNA": """
            SELECT 
                'protein' as signature_type,
                COUNT(DISTINCT sample_id) as unique_samples,
                COUNT(*) as num_signatures,
                ROUND(AVG(num_mins), 0) as avg_hash_count,
                ROUND(AVG(signature_size), 0) as avg_signature_size
            FROM sigs_aa.signatures
            UNION ALL
            SELECT 
                'dna' as signature_type,
                COUNT(DISTINCT sample_id) as unique_samples,
                COUNT(*) as num_signatures,
                ROUND(AVG(num_mins), 0) as avg_hash_count,
                ROUND(AVG(signature_size), 0) as avg_signature_size
            FROM sigs_dna.signatures
        """,
        
        "Show k-mer size distribution in signatures": """
            SELECT 
                'protein' as sig_type,
                ksize,
                COUNT(*) as signature_count,
                COUNT(DISTINCT sample_id) as unique_samples
            FROM sigs_aa.signatures
            GROUP BY ksize
            UNION ALL
            SELECT 
                'dna' as sig_type,
                ksize,
                COUNT(*) as signature_count,
                COUNT(DISTINCT sample_id) as unique_samples
            FROM sigs_dna.signatures
            GROUP BY ksize
            ORDER BY sig_type, ksize
        """,
        
        "Get signature manifest info for sample DRR011161": """
            SELECT 
                sample_id,
                md5,
                ksize,
                moltype,
                scaled,
                with_abundance,
                n_hashes
            FROM sigs_aa.manifests
            WHERE sample_id = 'DRR011161'
        """,
        
        "Get signature hash count distribution for sample DRR011161": """
            SELECT 
                'protein' as sig_type,
                COUNT(*) as total_hashes,
                ROUND(AVG(abundance), 2) as avg_abundance
            FROM sigs_aa.signature_mins
            WHERE sample_id = 'DRR011161'
            UNION ALL
            SELECT 
                'dna' as sig_type,
                COUNT(*) as total_hashes,
                ROUND(AVG(abundance), 2) as avg_abundance
            FROM sigs_dna.signature_mins
            WHERE sample_id = 'DRR011161'
        """,
        
        "Find samples with highest hash counts in protein signatures": """
            SELECT 
                sample_id,
                COUNT(*) as total_hashes,
                AVG(abundance) as avg_abundance,
                MAX(abundance) as max_abundance
            FROM sigs_aa.signature_mins
            GROUP BY sample_id
            ORDER BY total_hashes DESC
            LIMIT 20
        """,
        
        "Compare signature hash overlap between samples DRR012067 and DRR012493": """
            SELECT 
                s1.sample_id as sample1,
                s2.sample_id as sample2,
                COUNT(*) as shared_hashes
            FROM sigs_aa.signature_mins s1
            JOIN sigs_aa.signature_mins s2 ON s1.min_hash = s2.min_hash
            WHERE s1.sample_id != s2.sample_id
            AND s1.sample_id IN ('DRR012067', 'DRR012493')
            AND s2.sample_id IN ('DRR012067', 'DRR012493')
            GROUP BY s1.sample_id, s2.sample_id
        """,
        
        "Analyze hash abundance distribution across samples": """
            SELECT 
                abundance,
                COUNT(*) as hash_count,
                COUNT(DISTINCT sample_id) as sample_count
            FROM sigs_aa.signature_mins
            GROUP BY abundance
            ORDER BY abundance
        """,
        
        "Get sequence similarity matrix between samples": """
            SELECT 
                g1.sample_id as sample1,
                g2.sample_id as sample2,
                AVG(g1.max_containment) as avg_similarity
            FROM functional_profile_data.gather_data g1
            JOIN functional_profile_data.gather_data g2 ON g1.match_md5 = g2.match_md5
            WHERE g1.sample_id != g2.sample_id
            GROUP BY g1.sample_id, g2.sample_id
            LIMIT 20
        """,
        
        "Analyze signature hash overlap between sample": """
            SELECT 
                sample_id,
                COUNT(*) as total_hashes,
                AVG(abundance) as avg_abundance,
                MIN(abundance) as min_abundance,
                MAX(abundance) as max_abundance
            FROM sigs_aa.signature_mins
            WHERE sample_id = 'DRR011161'
            GROUP BY sample_id
        """,
        
        "Find potential contamination in samples": """
            SELECT 
                sample_id,
                match_name,
                f_query_match,
                max_containment,
                potential_false_negative
            FROM functional_profile_data.gather_data
            WHERE potential_false_negative = true
            ORDER BY f_query_match DESC
        """,
        
        "Get geographical distribution of samples": """
            SELECT 
                country,
                COUNT(DISTINCT sample_id) as sample_count,
                COUNT(DISTINCT accession) as accession_count
            FROM geographical_location_data.locations
            WHERE country IS NOT NULL AND country != 'None'
            GROUP BY country
            ORDER BY sample_count DESC
        """,
        
        "Find samples with geographical coordinates": """
            SELECT 
                sample_id,
                accession,
                country,
                biome,
                lat_lon,
                elevation
            FROM geographical_location_data.locations
            WHERE lat_lon IS NOT NULL AND lat_lon != 'None'
            ORDER BY country, sample_id
        """,
        
        "Get biome distribution of samples": """
            SELECT 
                biome,
                COUNT(DISTINCT sample_id) as sample_count,
                COUNT(DISTINCT country) as country_count
            FROM geographical_location_data.locations
            WHERE biome IS NOT NULL AND biome != 'None'
            GROUP BY biome
            ORDER BY sample_count DESC
        """,
        
        "Find samples from specific country (e.g., Japan)": """
            SELECT 
                sample_id,
                accession,
                biome,
                lat_lon,
                elevation,
                center_name
            FROM geographical_location_data.locations
            WHERE LOWER(country) LIKE '%japan%'
            ORDER BY sample_id
        """,
        
        "Get geographical metadata for sample DRR014147": """
            SELECT 
                sample_id,
                accession,
                country,
                biome,
                lat_lon,
                elevation,
                center_name,
                confidence
            FROM geographical_location_data.locations
            WHERE sample_id = 'DRR014147'
        """,
        
        "Find samples by biome (e.g., marine)": """
            SELECT 
                sample_id,
                accession,
                country,
                lat_lon,
                elevation,
                biome
            FROM geographical_location_data.locations
            WHERE LOWER(biome) LIKE '%marine%'
            ORDER BY country, sample_id
        """,
        
        "Get samples with elevation data": """
            SELECT 
                sample_id,
                accession,
                country,
                biome,
                elevation,
                lat_lon
            FROM geographical_location_data.locations
            WHERE elevation IS NOT NULL AND elevation != 'None' AND elevation != ''
            ORDER BY CAST(elevation AS DOUBLE) DESC
        """,
        
        "Join geographical data with functional profiles": """
            SELECT 
                g.sample_id,
                g.country,
                g.biome,
                COUNT(DISTINCT f.ko_id) as ko_diversity,
                SUM(f.abundance) as total_abundance
            FROM geographical_location_data.locations g
            JOIN functional_profile.profiles f ON g.sample_id = f.sample_id
            WHERE g.country IS NOT NULL AND g.country != 'None'
            GROUP BY g.sample_id, g.country, g.biome
            ORDER BY ko_diversity DESC
            LIMIT 20
        """,
        
        "Join geographical data with taxonomic profiles": """
            SELECT 
                g.sample_id,
                g.country,
                g.biome,
                COUNT(CASE WHEN t.in_sample_est = true THEN 1 END) as confirmed_organisms,
                AVG(t.actual_confidence_with_coverage) as avg_confidence
            FROM geographical_location_data.locations g
            JOIN taxa_profiles.profiles t ON g.sample_id = t.sample_id
            WHERE g.country IS NOT NULL AND g.country != 'None'
            GROUP BY g.sample_id, g.country, g.biome
            ORDER BY confirmed_organisms DESC
            LIMIT 20
        """,
        
        "Compare functional diversity by country": """
            SELECT 
                g.country,
                COUNT(DISTINCT g.sample_id) as sample_count,
                AVG(ko_stats.ko_diversity) as avg_ko_diversity,
                AVG(ko_stats.total_abundance) as avg_total_abundance
            FROM geographical_location_data.locations g
            JOIN (
                SELECT 
                    sample_id,
                    COUNT(DISTINCT ko_id) as ko_diversity,
                    SUM(abundance) as total_abundance
                FROM functional_profile.profiles
                GROUP BY sample_id
            ) ko_stats ON g.sample_id = ko_stats.sample_id
            WHERE g.country IS NOT NULL AND g.country != 'None'
            GROUP BY g.country
            HAVING COUNT(DISTINCT g.sample_id) >= 2
            ORDER BY avg_ko_diversity DESC
        """,
        
        "Compare taxonomic diversity by biome": """
            SELECT 
                g.biome,
                COUNT(DISTINCT g.sample_id) as sample_count,
                AVG(taxa_stats.confirmed_organisms) as avg_confirmed_organisms,
                AVG(taxa_stats.avg_confidence) as avg_confidence
            FROM geographical_location_data.locations g
            JOIN (
                SELECT 
                    sample_id,
                    COUNT(CASE WHEN in_sample_est = true THEN 1 END) as confirmed_organisms,
                    AVG(actual_confidence_with_coverage) as avg_confidence
                FROM taxa_profiles.profiles
                GROUP BY sample_id
            ) taxa_stats ON g.sample_id = taxa_stats.sample_id
            WHERE g.biome IS NOT NULL AND g.biome != 'None'
            GROUP BY g.biome
            HAVING COUNT(DISTINCT g.sample_id) >= 2
            ORDER BY avg_confirmed_organisms DESC
        """,
        
        "What are the most prevalent species per geographic location (using human taxonomy and the SRA metadata)?": """
            SELECT 
                g.country,
                t.organism_name,
                COUNT(*) AS occurrence_count
            FROM 
                taxa_profiles.profiles t
            JOIN 
                geographical_location_data.locations g
                ON t.sample_id = g.sample_id
            WHERE 
                t.in_sample_est = TRUE
            GROUP BY 
                g.country, t.organism_name
            ORDER BY 
                g.country, occurrence_count DESC
        """,
        
        "What are the most prevalent functions (KEGG KOs) per geographic location?": """
            WITH ranked_kos AS (
              SELECT 
                g.country,
                f.ko_id,
                SUM(f.abundance) AS total_abundance,
                ROW_NUMBER() OVER (
                  PARTITION BY g.country 
                  ORDER BY SUM(f.abundance) DESC
                ) AS rank
              FROM 
                functional_profile.profiles f
              JOIN 
                geographical_location_data.locations g
                ON f.sample_id = g.sample_id
              GROUP BY 
                g.country, f.ko_id
            )
            SELECT 
              country,
              ko_id,
              total_abundance
            FROM 
              ranked_kos
            WHERE 
              rank = 1
            ORDER BY 
              total_abundance DESC
        """,
        
        "What are the most abundant KEGG KOs operating on the database": """
            SELECT 
                ko_id,
                SUM(abundance) AS total_abundance
            FROM 
                functional_profile.profiles
            GROUP BY 
                ko_id
            ORDER BY 
                total_abundance DESC
            LIMIT 10
        """,
        
        "What are the least abundant KEGG KOs operating on the database": """
            SELECT 
                ko_id,
                SUM(abundance) AS total_abundance
            FROM 
                functional_profile.profiles
            GROUP BY 
                ko_id
            ORDER BY 
                total_abundance ASC
            LIMIT 10
        """
    }
    
    query_progress = tqdm(sample_queries.items(), desc="Preparing sample queries", unit="query")
    processed_queries = {}
    
    for description, sql in query_progress:
        query_progress.set_postfix(query=description[:40] + "...")
        processed_queries[description] = sql
    
    return processed_queries

def get_database_documentation():
    """
    Get detailed documentation about the database structure
    
    Returns:
        dict: Documentation for each schema and common use cases
    """
    
    print("📚 Loading database documentation...")
    
    doc_sections = [
        "Database overview",
        "Functional profile schema",
        "Functional profile data schema", 
        "Taxa profiles schema",
        "Signature schemas",
        "Common patterns"
    ]
    
    with tqdm(doc_sections, desc="Loading documentation", unit="section") as pbar:
        for section in pbar:
            pbar.set_postfix(section=section)
            import time
            time.sleep(0.1)
    
    documentation = {
        "overview": """
        This database contains metagenomic analysis results from sourmash and other bioinformatics tools:
        - Functional profiles using KEGG Orthology (KO) gene annotations with abundance data
        - Taxonomic classifications and organism identification with confidence metrics  
        - Sourmash signature data for both protein and DNA sequences with min-hash values
        - Sequence similarity analysis results (gather data) with containment and ANI metrics
        - Geographical location and metadata information for samples
        - Sample metadata and processing information
        
        The data is organized by sample_id (format: DRR******) which links related information across different schemas.
        All samples follow the pattern DRR followed by 6 digits (e.g., DRR011161).
        """,
        
        "schemas": {
            "functional_profile": {
                "description": "Contains functional annotation data using KEGG Orthology (KO) terms. Each sample has its own table named by sample_id (DRR******) with KO IDs and their abundance values.",
                "tables": {
                    "*": "Individual sample tables (named DRR******) containing ko_id (VARCHAR) and abundance (DOUBLE) columns. Each row represents a functional gene family and its abundance in that sample."
                },
                "key_columns": {
                    "ko_id": "KEGG Orthology identifier (e.g., K00001)",
                    "abundance": "Quantitative abundance measurement for the KO function"
                }
            },
            
            "functional_profile_data": {
                "description": "Contains sourmash gather analysis results showing sequence similarity between samples and reference databases",
                "tables": {
                    "gather_data": "Main table with sequence similarity matches, containment metrics, ANI values, and match information for all samples"
                },
                "key_columns": {
                    "sample_id": "Sample identifier (DRR******)",
                    "intersect_bp": "Base pairs of intersection between query and match",
                    "max_containment": "Maximum containment score (0-1)",
                    "f_query_match": "Fraction of query matched",
                    "match_name": "Name of the matching reference sequence",
                    "average_containment_ani": "Average nucleotide identity based on containment",
                    "potential_false_negative": "Boolean flag for potential false negatives"
                }
            },
            
            "taxa_profiles": {
                "description": "Contains taxonomic classification results with organism identification, confidence metrics, and taxonomy IDs (unified table for all samples)",
                "tables": {
                    "profiles": "Unified table containing all samples' taxonomic data with organism names, confidence scores, coverage metrics, extracted organism_id (GCA_/GCF_ accessions), and NCBI taxonomy IDs"
                },
                "key_columns": {
                    "sample_id": "Sample identifier (DRR******)",
                    "organism_name": "Full organism name from reference database",
                    "organism_id": "Extracted NCBI accession (GCA_/GCF_) identifier",
                    "tax_id": "NCBI taxonomy ID (-1 if not found in mapping)",
                    "in_sample_est": "Boolean estimate if organism is present in sample",
                    "actual_confidence_with_coverage": "Confidence score with coverage correction",
                    "num_exclusive_kmers_to_genome": "Number of k-mers exclusive to this genome",
                    "acceptance_threshold_with_coverage": "Acceptance threshold with coverage"
                }
            },
            
            "sigs_aa": {
                "description": "Protein signature data from sourmash including manifests, signatures, and unified min-hash values",
                "tables": {
                    "manifests": "Signature file metadata with locations, MD5 hashes, k-mer parameters",
                    "signatures": "Detailed signature statistics with hash counts, parameters, and file information",
                    "signature_mins": "Unified table with all protein signature min-hash values (all samples)"
                },
                "key_columns_manifests": {
                    "sample_id": "Sample identifier (DRR******)",
                    "md5": "MD5 hash of the signature",
                    "ksize": "K-mer size used",
                    "scaled": "Scaling factor for sketching",
                    "with_abundance": "Boolean if abundance information is included"
                },
                "key_columns_signatures": {
                    "sample_id": "Sample identifier (DRR******)",
                    "md5": "MD5 hash of the signature", 
                    "num_mins": "Number of min-hash values in signature",
                    "has_abundances": "Boolean if signature contains abundance data"
                },
                "key_columns_signature_mins": {
                    "sample_id": "Sample identifier (DRR******)",
                    "md5": "MD5 hash linking to signature metadata",
                    "min_hash": "Individual min-hash value (BIGINT)",
                    "abundance": "Abundance for this hash (INTEGER)",
                    "position": "Position index in signature"
                }
            },
            
            "sigs_dna": {
                "description": "DNA signature data from sourmash including manifests, signatures, and unified min-hash values",
                "tables": {
                    "manifests": "Signature file metadata with locations, MD5 hashes, k-mer parameters",
                    "signatures": "Detailed signature statistics with hash counts, parameters, and file information",
                    "signature_mins": "Unified table with all DNA signature min-hash values (all samples)"
                },
                "key_columns_manifests": {
                    "sample_id": "Sample identifier (DRR******)",
                    "md5": "MD5 hash of the signature",
                    "ksize": "K-mer size used", 
                    "scaled": "Scaling factor for sketching",
                    "with_abundance": "Boolean if abundance information is included"
                },
                "key_columns_signatures": {
                    "sample_id": "Sample identifier (DRR******)",
                    "md5": "MD5 hash of the signature",
                    "num_mins": "Number of min-hash values in signature",
                    "has_abundances": "Boolean if signature contains abundance data"
                },
                "key_columns_signature_mins": {
                    "sample_id": "Sample identifier (DRR******)",
                    "md5": "MD5 hash linking to signature metadata",
                    "min_hash": "Individual min-hash value (BIGINT)",
                    "abundance": "Abundance for this hash (INTEGER)",
                    "position": "Position index in signature"
                }
            },
            
            "signature_mins_sigs_aa": {
                "description": "Individual tables for each sample's protein signature min-hash values (one table per sample_id)",
                "tables": {
                    "*": "Tables named by sample_id (DRR******) containing md5, min_hash values, abundances, and positions"
                },
                "key_columns": {
                    "md5": "MD5 hash linking to signature metadata",
                    "min_hash": "Individual min-hash value (BIGINT)",
                    "abundance": "Abundance for this hash (INTEGER)",
                    "position": "Position index in the signature"
                }
            },
            
            "signature_mins_sigs_dna": {
                "description": "Individual tables for each sample's DNA signature min-hash values (one table per sample_id)",
                "tables": {
                    "*": "Tables named by sample_id (DRR******) containing md5, min_hash values, abundances, and positions"
                },
                "key_columns": {
                    "md5": "MD5 hash linking to signature metadata",
                    "min_hash": "Individual min-hash value (BIGINT)", 
                    "abundance": "Abundance for this hash (INTEGER)",
                    "position": "Position index in the signature"
                }
            },
            
            "geographical_location_data": {
                "description": "Contains geographical location and metadata information for samples including coordinates, country, biome, and elevation data",
                "tables": {
                    "locations": "Main table with geographical metadata for all samples including coordinates, country, biome, elevation, and center information"
                },
                "key_columns": {
                    "accession": "BioSample accession identifier",
                    "sample_id": "Sample identifier (DRR******) linking to other schemas",
                    "attribute_name": "Description of the geographical attribute",
                    "attribute_value": "Value of the geographical attribute",
                    "lat_lon": "Latitude and longitude coordinates",
                    "country": "Country where sample was collected",
                    "biome": "Biome or environment type (e.g., marine, terrestrial)",
                    "elevation": "Elevation/depth information",
                    "center_name": "Sequencing center or institution",
                    "confidence": "Confidence level of geographical annotation"
                }
            }
        },
        
        "common_patterns": {
            "sample_identification": "sample_id follows DRR****** format (DRR + 6 digits) and appears across all schemas to link related data",
            "abundance_data": "abundance columns contain quantitative measurements (counts, relative abundances, etc.)",
            "confidence_scores": "Various confidence and quality metrics help filter reliable results from uncertain ones",
            "organism_identification": "organism_id contains standardized NCBI accession numbers (GCA_/GCF_) extracted from organism names",
            "md5_hashes": "MD5 values link signatures across manifest and detailed hash tables",
            "containment_metrics": "f_query_match, max_containment show how much of query/reference sequences overlap",
            "ani_metrics": "ANI (Average Nucleotide Identity) values indicate sequence similarity percentages"
        },
        
        "important_notes": {
            "functional_profiles": "All samples in unified table functional_profile.profiles with sample_id column",
            "taxa_profiles": "All samples in unified table taxa_profiles.profiles with sample_id column", 
            "signature_mins": "All samples in unified tables sigs_aa.signature_mins and sigs_dna.signature_mins with sample_id column",
            "gather_data": "All samples' gather results are in one table with sample_id column",
            "signature_metadata": "Manifests and signatures tables contain metadata for all samples with sample_id column",
            "geographical_data": "All samples' geographical metadata in one table geographical_location_data.locations with sample_id column"
        }
    }
    
    return documentation
