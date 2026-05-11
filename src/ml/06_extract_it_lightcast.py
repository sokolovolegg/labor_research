import json
import pandas as pd
import os
import ast
from tqdm import tqdm
from flashtext import KeywordProcessor

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LIGHTCAST_FILE = os.path.join(BASE_DIR, "data", "raw", "lightcast", "skills.json")
INPUT_FILE = os.path.join(BASE_DIR, "data", "processed", "TARGET_VACANCIES.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "processed", "IT_LIGHTCAST_SKILLS.csv")

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
    
    hard_skills_raw = [s for s in skills_data if s['type']['name'] in ['Specialized Skill', 'Certification']]
    soft_skills_raw = [s for s in skills_data if s['type']['name'] == 'Common Skill']
    
    kp_hard = build_processor(hard_skills_raw)
    kp_soft = build_processor(soft_skills_raw)
    
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    df_it = df[df['domain_label'] == 'IT'].copy()
    
    df_it['description'] = df_it['description'].fillna("")
    descriptions = df_it['description'].tolist()
    
    hard_skills_list = []
    soft_skills_list = []
    
    for text in tqdm(descriptions):
        hard_skills_list.append(list(set(kp_hard.extract_keywords(text))))
        soft_skills_list.append(list(set(kp_soft.extract_keywords(text))))
        
    df_it['hard_skills'] = hard_skills_list
    df_it['soft_skills'] = soft_skills_list
    
    df_it.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    run_extraction()