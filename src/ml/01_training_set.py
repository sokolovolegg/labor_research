import pandas as pd
import numpy as np
import os
import re


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PROC_DIR = os.path.join(BASE_DIR, "data", "processed")
INPUT_FILE = os.path.join(PROC_DIR, "MASTER_DATASET_CLEAN.csv")
OUT_TRAIN = os.path.join(PROC_DIR, "training_set.csv")
OUT_HELD = os.path.join(PROC_DIR, "held_out_set.csv")

# --- Regex Patterns for Classification ---
FINANCE_MARKERS = r'(?i)analyst|trader|risk|compliance|fund|portfolio|credit|quant|actuar'
HR_MARKERS = r'(?i)recruiter|hr|human resources|talent|people operations|l&d'
OTHER_MARKERS = r'(?i)arzt|pfleger|fahrer|bauingenieur|koch|kellner|lekarz|kierowca|kelner|mechanic'

def build_training_set():
    print(f"Loading master dataset from {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE, low_memory=False).dropna(subset=['title'])
    
    # 1. IT
    print("Sampling IT vacancies...")
    it_pool = df[df['source'] == 'stackoverflow']
    it_sample = it_pool.sample(n=min(20000, len(it_pool)), random_state=42)
    it_sample['label'] = 'IT'
    
    # 2. Finance
    print("Sampling Finance vacancies...")
    fin_pool = df[(df['source'] == 'efinancialcareers') & (df['title'].str.contains(FINANCE_MARKERS, na=False))]
    fin_sample = fin_pool.sample(n=min(20000, len(fin_pool)), random_state=42)
    fin_sample['label'] = 'Finance'

    # 3. HR
    print("Sampling HR vacancies...")
    hr_pool = df[df['title'].str.contains(HR_MARKERS, na=False)]
    hr_sample = hr_pool.sample(n=min(5000, len(hr_pool)), random_state=42)
    hr_sample['label'] = 'HR'

    # 4. Other
    print("Sampling Other vacancies...")
    other_pool = df[df['title'].str.contains(OTHER_MARKERS, na=False)]
    other_sample = other_pool.sample(n=min(5000, len(other_pool)), random_state=42)
    other_sample['label'] = 'Other'
    
    # merge and save
    train_df = pd.concat([it_sample, fin_sample, hr_sample, other_sample], ignore_index=True)
    train_df.to_csv(OUT_TRAIN, index=False)
    
    print(f"Success! Training rows: {len(train_df)}")

if __name__ == "__main__":
    build_training_set()