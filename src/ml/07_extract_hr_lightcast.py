import json
import pandas as pd
import os
import ast
from tqdm import tqdm
from flashtext import KeywordProcessor

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LIGHTCAST_FILE = os.path.join(BASE_DIR, "data", "raw", "lightcast", "skills.json")
INPUT_FILE = os.path.join(BASE_DIR, "data", "processed", "TARGET_VACANCIES.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "processed", "HR_LIGHTCAST_SKILLS.csv")

def build_processor(skills_list):
    kp = KeywordProcessor()
    skill_dict = {}
    for skill in skills_list:
        name = skill['name'].strip()
        skill_dict[name] = [name.lower()]
    kp.add_keywords_from_dict(skill_dict)
    return kp

def run_extraction():
    with open(LIGHTCAST_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    skills_data = data['data']
    
    kp_hard = build_processor([s for s in skills_data if s['type']['name'] in ['Specialized Skill', 'Certification']])
    kp_soft = build_processor([s for s in skills_data if s['type']['name'] == 'Common Skill'])
    
    df = pd.read_csv(INPUT_FILE, usecols=['description', 'domain_label', 'year_month', 'country', 'source'], low_memory=False)
    df_hr = df[df['domain_label'] == 'HR'].copy()
    del df
    
    df_hr['description'] = df_hr['description'].fillna("")
    
    df_hr['hard_skills'] = [list(set(kp_hard.extract_keywords(text))) for text in tqdm(df_hr['description'])]
    df_hr['soft_skills'] = [list(set(kp_soft.extract_keywords(text))) for text in tqdm(df_hr['description'])]
    
    df_hr.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    run_extraction()