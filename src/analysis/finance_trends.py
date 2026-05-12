import pandas as pd
import ast
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FILE_PATH = os.path.join(BASE_DIR, "data", "processed", "FINANCE_LIGHTCAST_SKILLS.csv")
MIN_JOBS_PER_YEAR = 30
TARGET_COUNTRIES = ['CH', 'PL', 'DE', 'US']

def process_trends(df, skill_column, prefix):
    df_exploded = df.explode(skill_column).dropna(subset=[skill_column])
    
    global_totals = df.groupby('year').size().reset_index(name='total_jobs')
    global_skills = df_exploded.groupby(['year', skill_column]).size().reset_index(name='skill_count')
    
    global_trends = pd.merge(global_skills, global_totals, on='year')
    global_trends['percentage'] = (global_trends['skill_count'] / global_trends['total_jobs']) * 100
    global_trends = global_trends[global_trends['total_jobs'] >= MIN_JOBS_PER_YEAR]
    
    top_30_global = (global_trends.sort_values(['year', 'percentage'], ascending=[True, False])
                     .groupby('year').head(30))
    
    country_totals = df.groupby(['year', 'country']).size().reset_index(name='total_jobs')
    country_skills = df_exploded.groupby(['year', 'country', skill_column]).size().reset_index(name='skill_count')
    
    country_trends = pd.merge(country_skills, country_totals, on=['year', 'country'])
    country_trends['percentage'] = (country_trends['skill_count'] / country_trends['total_jobs']) * 100
    country_trends = country_trends[country_trends['total_jobs'] >= MIN_JOBS_PER_YEAR]
    country_trends = country_trends[country_trends['country'].isin(TARGET_COUNTRIES)]
    
    top_30_by_country = (country_trends.sort_values(['country', 'year', 'percentage'], ascending=[True, True, False])
                         .groupby(['country', 'year']).head(30))
    
    global_path = os.path.join(BASE_DIR, "data", "processed", f"FINANCE_TOP_30_GLOBAL_{prefix}.csv")
    country_path = os.path.join(BASE_DIR, "data", "processed", f"FINANCE_TOP_30_BY_COUNTRY_{prefix}.csv")
    
    top_30_global.to_csv(global_path, index=False)
    top_30_by_country.to_csv(country_path, index=False)

if __name__ == "__main__":
    df = pd.read_csv(FILE_PATH, low_memory=False)
    
    df['hard_skills'] = df['hard_skills'].apply(ast.literal_eval)
    df['soft_skills'] = df['soft_skills'].apply(ast.literal_eval)
    df['year'] = df['year_month'].astype(str).str[:4].astype(int)
    
    process_trends(df, 'hard_skills', 'HARD')
    process_trends(df, 'soft_skills', 'SOFT')