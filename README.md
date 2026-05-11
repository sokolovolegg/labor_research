# Labor Market Transformations: A Multilingual NLP Analysis (2010-2024)

This project investigates structural transformations within the global and European labor markets over the past decade. By applying Natural Language Processing (transformer-based NLP architectures) to a massive corpus of historical job postings, we track the evolution of employer demands, the convergence of sectors (e.g., the FinTech shift), and the impact of Generative AI across three core domains: **IT, Finance, and HR**.

## Project Objectives

* **Evolution of Skills:** Track how technical (hard) and transversal (soft) skill requirements have evolved over the last decade across different geographies.
* **Algorithmic Reliability:** Evaluate the performance of Zero-Shot Cross-Lingual Transfer using multilingual transformer models on non-English job postings, explicitly investigating biases such as "Domain Leakage".
* **Structural Sectoral Convergence:** Analyze the blurring lines between traditional industries, specifically the profound technological shift within the financial sector (FinTech).

## Dataset

We compiled and standardized a custom multilingual dataset aggregating unstructured job advertisements from multiple platforms.

* **Volume:** ~464,700 unique, cleaned records.
* **Time Horizon:** 2010 – 2024.
* **Languages:** English, German, Polish.
* **Sources:**
  * Global Tech & Finance: *Stack Overflow, Dice.com, eFinancialCareers, Monster*
  * European Local Markets: *Jobs.ch (Switzerland), StepStone.de (Germany), Pracuj.pl (Poland)*
* **Validation Benchmark:** 300 manually annotated multilingual records utilized as a "Gold Standard" for model evaluation.

## Repository Structure

```text
labor_research-1/
├── data/                  
│   ├── raw/               # Raw taxonomies (e.g., Lightcast skills.json)
│   └── processed/         # Deduplicated source files, labeled datasets, and final skill matrices
├── models/
│   └── domain_classifier/ # Fine-tuned DistilBERT model weights, tokenizer, and configs
├── notebooks/             # Jupyter Notebooks (1. EDA, 2. NLP Classification, 3. Trend Analysis)
├── scripts/               # Python scripts for data collection and preprocessing (if applicable)
├── .gitignore          
├── requirements.txt    
└── README.md

(Note: Directories like `data/` and `models/` are ignored by git due to size constraints).
```

## Analytical Pipeline & Methodology

* **Phase 1: Data Aggregation & Cleaning** – Processed raw HTML/JSON data, removed temporal duplicates, detected languages (`langdetect`), and standardized text formatting across 7 major job boards.
* **Phase 2: NLP Domain Classification** – Deployed and fine-tuned a `DistilBERT-multilingual-cased` transformer model to perform Extreme Multi-Label Classification. The model categorized unstructured job descriptions into distinct domains (IT, Finance, HR) without prior machine translation.
* **Phase 3: Model Validation & Error Analysis** – Assessed model metrics (Accuracy, F1-score) against the manual validation set. Conducted deep error analysis via Confusion Matrices to identify cross-lingual classification boundaries and the "Domain Leakage" phenomenon (e.g., tech-heavy HR recruiter jobs misclassified as IT).
* **Phase 4: Skill Extraction & Statistical Analysis** – Extracted explicit and implicit hard/soft skills using established taxonomies (Lightcast). Evaluated longitudinal trends (the rise of GenAI, the persistence of human-centric soft skills) using statistical testing for significance.

## How to Run Locally

### Clone the repository:

```bash
git clone https://github.com/sokolovolegg/labor_research
cd labor_research
```

### Set up the virtual environment:

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate
```

### Install dependencies:

```bash
pip install -r requirements.txt
```

## Data Access Note

The complete compiled datasets and the fine-tuned `.safetensors` model weights exceed GitHub's file size limits. If you need access to the data to reproduce the analysis, please download it from our shared Google Drive Folder or contact Oleg Sokolov at oleg.sokolov@uzh.ch.