import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def process_geo_data():
    print("Reading SRA_Accessions file...")
    sra_df = pd.read_csv('./data/SRA_Accessions', sep='\t', 
                         usecols=['Accession', 'Type', 'BioSample'])
    
    sra_df = sra_df.dropna(subset=['BioSample'])
    sra_df = sra_df[sra_df['BioSample'] != '-']
    
    sra_df = sra_df[sra_df['Type'] == 'RUN']
    
    print(f"Loaded {len(sra_df)} SRA records with BioSample data")
    
    print("Reading biosample_geographical_location.202503.csv...")
    geo_df = pd.read_csv('./data/biosample_geographical_location.202503.csv')
    
    print(f"Loaded {len(geo_df)} geographical records")
    
    print("Merging data...")
    merged_df = geo_df.merge(
        sra_df[['Accession', 'BioSample']], 
        left_on='accession', 
        right_on='BioSample', 
        how='left'
    )
    
    merged_df = merged_df.rename(columns={'Accession': 'sample_id'})
    
    merged_df = merged_df.drop(columns=['BioSample'])
    
    print(f"Merged data contains {len(merged_df)} records")
    print(f"Records with sample_id: {merged_df['sample_id'].notna().sum()}")
    
    output_path = './data/output.csv'
    merged_df.to_csv(output_path, index=False)
    print(f"Output saved to {output_path}")
    
    return merged_df

if __name__ == "__main__":
    result = process_geo_data()
    print("\nFirst few rows of merged data:")
    print(result.head())
