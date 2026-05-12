import pandas as pd
import os
import ast
from tqdm import tqdm
from flashtext import KeywordProcessor

# --- Path Configuration ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
INPUT_FILE = os.path.join(BASE_DIR, "data", "processed", "TARGET_VACANCIES.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "processed", "VACANCIES_WITH_SKILLS.csv")

SKILLS_DICTIONARY = {
    # IT Skills
    "Python": ["python", "python3", "pandas", "numpy"],
    "SQL": ["sql", "mysql", "postgresql", "t-sql", "pl/sql"],
    "Java": ["java", "java 8", "java 11", "spring boot"],
    "Cloud Computing": ["aws", "amazon web services", "azure", "gcp", "google cloud"],
    "Machine Learning": ["machine learning", "ml", "tensorflow", "pytorch", "scikit-learn"],
    "Git": ["git", "github", "gitlab", "version control"],
    "Docker": ["docker", "containerization", "kubernetes", "k8s"],
    
    # Finance Skills
    "Excel": ["excel", "ms excel", "microsoft excel", "vlookup", "pivot tables", "macros", "vba"],
    "Financial Modeling": ["financial modeling", "dcf", "valuation", "financial analysis"],
    "Accounting Standards": ["ifrs", "gaap", "us gaap"],
    "ERP Systems": ["sap", "oracle e-business", "1c"],
    "Bloomberg": ["bloomberg", "bloomberg terminal"],
    
    # HR & Management Skills
    "Agile/Scrum": ["agile", "scrum", "kanban", "sprint planning"],
    "Recruiting": ["recruiting", "talent acquisition", "sourcing", "headhunting"],
    "ATS": ["ats", "applicant tracking system", "workday", "greenhouse", "lever"]
}

def extract_skills():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Could not find {INPUT_FILE}")
        return

    print(f"1. Loading dataset: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    
    # Fill empty descriptions to avoid crashes
    df['description'] = df['description'].fillna("")
    descriptions = df['description'].tolist()
    
    print("2. Initializing FlashText Keyword Processor...")
    keyword_processor = KeywordProcessor()
    keyword_processor.add_keywords_from_dict(SKILLS_DICTIONARY)
    
    print("3. Extracting skills from 500k+ descriptions...")
    extracted_skills_list = []
    
    # Process texts and show progress bar
    for text in tqdm(descriptions):
        # keyword_processor.extract_keywords returns a list of found standardized names
        found_skills = keyword_processor.extract_keywords(text)
        # Remove duplicates from the result for this specific job posting
        unique_skills = list(set(found_skills))
        extracted_skills_list.append(unique_skills)
        
    # Add extracted skills as a new column
    df['extracted_skills'] = extracted_skills_list
    
    print(f"4. Saving results to {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False)
    print("Skill extraction completed successfully")

if __name__ == "__main__":
    extract_skills()