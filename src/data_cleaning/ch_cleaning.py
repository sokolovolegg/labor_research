import pandas as pd
import re

file_path = 'data/ch_data_raw.csv'
df = pd.read_csv(file_path)

# OVERVIEW 
print("### 1. DATASET OVERVIEW ###")
print(f"Shape: {df.shape[0]} rows and {df.shape[1]} columns")
print("\nData types and non-null counts:")
print(df.info())

print("\nPercentage of missing values per column:")
null_pct = (df.isnull().sum() / len(df)) * 100
print(null_pct)

# FULL ROW DUPLICATES CHECK (if all columns are identical)
full_duplicates = df.duplicated().sum()
print(f"\n### 2. FULL ROW DUPLICATES ###")
print(f"Total full-row duplicates found: {full_duplicates}")

# JOB ID EXTRACTION
# The pattern 'detail/([^/?]+)' captures the unique identifier in the URL
def extract_job_id(url):
    match = re.search(r'detail/([^/?]+)', str(url))
    return match.group(1) if match else None

print(f"\n### 3. JOB ID EXTRACTION ###")
df['job_id'] = df['archive_url'].apply(extract_job_id)

missing_ids = df['job_id'].isnull().sum()
print(f"Records where ID extraction failed: {missing_ids}")

# JOB ID UNIQUENESS ANALYSIS
unique_ids_count = df['job_id'].nunique()
total_rows = len(df)

print(f"\n### 4. UNIQUE JOB ID ANALYSIS ###")
print(f"Total records in database: {total_rows}")
print(f"Total unique Job IDs: {unique_ids_count}")

if unique_ids_count < total_rows:
    duplicates_count = total_rows - unique_ids_count
    print(f"Found {duplicates_count} records with non-unique IDs.")
else:
    print("All Job IDs are unique.")

print("\nFirst 5 records with extracted Job IDs:")
print(df[['job_id', 'title', 'company', 'date_posted']].head())

# DEDUPLICATION BY JOB ID
print("\n### 5. DEDUPLICATION (KEEPING EARLIEST DATE) ###")

df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce', utc=True) # mixed time-zones error handling

df = df.sort_values(by=['job_id', 'date_posted'], ascending=[True, True])

# Drop duplicates keeping the first occurrence
df_clean = df.drop_duplicates(subset=['job_id'], keep='first').copy()

print(f"Rows before deduplication: {len(df)}")
print(f"Rows after deduplication: {len(df_clean)}")

clean_file_path = 'data/ch_data_cleaned.csv'
df_clean.to_csv(clean_file_path, index=False, encoding='utf-8')
print(f"\nCleaned dataset saved to: {clean_file_path}")