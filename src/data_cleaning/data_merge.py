import pandas as pd
import glob
import os

CURRENT_SCRIPT_PATH = os.path.abspath(__file__) 

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_SCRIPT_PATH)))

PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "MASTER_DATASET_CLEAN.csv")

def merge_all():
    search_pattern = os.path.join(PROCESSED_DIR, "*.csv")
    all_files = [f for f in glob.glob(search_pattern) if "MASTER" not in f]
    
    print(f"Searching in: {PROCESSED_DIR}") 
    print(f"Files found: {len(all_files)}")
    
    if not all_files:
        print("Error: No files found in data/processed for merging!")
        return

    final_df_list = []
    
    for file in all_files:
        # Extract source name from filename (e.g., 'stackoverflow' from 'stackoverflow_jobs.csv')
        source = os.path.basename(file).split('_')[0]
        print(f"Processing source: {source}")
        
        try:
            df = pd.read_csv(file, low_memory=False)
            
            # Column Standardization: Find a column containing 'description' and rename it
            desc_col = next((c for c in df.columns if 'description' in c.lower()), None)
            if desc_col:
                df = df.rename(columns={desc_col: 'description'})
            
            # Data Cleaning: Drop rows without a description and add source label
            if 'description' in df.columns:
                df = df.dropna(subset=['description'])
                df['source'] = source
                final_df_list.append(df)
            else:
                print(f"  [Skip] No description column found in {file}")
                
        except Exception as e:
            print(f"  [Error] Could not process {file}: {e}")

    # Combine all dataframes into one master dataset
    if final_df_list:
        master_df = pd.concat(final_df_list, ignore_index=True)
        master_df.to_csv(OUTPUT_PATH, index=False)
        print("-" * 30)
        print(f"Success! Created MASTER_DATASET_CLEAN.csv")
        print(f"Total records: {len(master_df):,}")
    else:
        print("No valid data was merged.")

if __name__ == "__main__":
    merge_all()