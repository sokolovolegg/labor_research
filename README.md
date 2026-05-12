# Labor Market Transformations: A Multilingual NLP and Network Analysis (2010-2024)

This project investigates structural transformations within the global and local (CH, US, DE, PL) markets over the past decade. By applying Natural Language Processing (transformer-based NLP architectures), Statistical Regression, and Graph Theory to a massive corpus of historical job postings, we track the evolution of employer demands, the convergence of sectors, and the phenomenon of "Skill Bloat" across two core domains: **IT and Finance**.

## Project Objectives

* **Structural Sectoral Convergence:** Analyze the blurring lines between traditional industries, explicitly mapping the deep technological integration within the financial sector (FinTech) using Co-occurrence Convergence Indexes.
* **Skill Bloat & Complexity (Network Topology):** Quantify the inflation of employer requirements using Welch's t-test and Cohen's d effect size. Utilize Network Density mapping to prove the transition of professions from segmented tasks to "hyper-hybridized" cliques.
* **Algorithmic Reliability (Zero-Shot Transfer):** Evaluate the performance of multilingual transformer models (DistilBERT) on domain classification across non-English job postings, investigating linguistic limitations and "Domain Leakage" in specialized corporate jargon.
* **Geographic Benchmarking:** Compare the hyper-dynamic US market (the global tech vanguard) against the more conservative, specialized European markets (focusing heavily on Switzerland).

## Dataset

We compiled and standardized a custom multilingual dataset aggregating unstructured job advertisements scraped via the Wayback Machine and direct APIs.

* **Volume:** ~464,700 unique, cleaned records.
* **Time Horizon:** 2010 – 2024.
* **Languages:** English, German, Polish.
* **Geographies:** United States (US), Switzerland (CH), Germany (DE), Poland (PL).
* **Sources:**
  * Global Platforms: *Stack Overflow, Dice.com, eFinancialCareers, Monster*
  * European Local Boards: *Jobs.ch, StepStone.de, Pracuj.pl*

## Repository Structure

```text
labor_research-1/
├── data/                  
│   ├── raw/               # Scraped CSV files from job boards and raw taxonomies (e.g., Lightcast skills.json)
│   └── processed/         # Master deduplicated datasets, labeled data, and extracted Top-30 skill matrices
├── analysis/              # Core analysis environment
│   ├── 01_dataset_overview.ipynb               # EDA and data aggregation
│   ├── 02_multilingual_domain_classification.ipynb # ML evaluation and classification reports
│   ├── 03_market_structure_and_skills.ipynb    # Skill bloat, convergence, and network density
│   └── *.py / *.png                            # Auxiliary scripts for trend plotting and generated network graphs
├── ml/                    # Machine Learning and Feature Extraction Pipeline
│   ├── 01_training_set.py                      # Weak supervision / Heuristic labeling
│   ├── 02_train_domain_classifier.py           # Fine-tuning DistilBERT script
│   ├── 03_apply_domain_classifier.py           # Batch inference script
│   └── 04_extract_*.py                         # Skill extraction using Lightcast taxonomy
├── parcing_setup/         # Data Collection 
│   └── scraper_*.py       # Web scrapers for Wayback Machine and live job boards
├── domain_classifier/     # Saved fine-tuned DistilBERT weights, tokenizers, and configs (.safetensors)
├── data_cleaning/         # Data sanitization and deduplication scripts
├── requirements.txt       # Python dependencies
└── README.md
```

> **Note:** Directories like `data/` and `domain_classifier/` are ignored by git due to size constraints.

## Analytical Pipeline & Methodology

* **Phase 1: Data Aggregation & Collection** (`parcing_setup`) – Scraped historical job descriptions using custom Wayback Machine parsers to bypass the lack of historical data availability on modern job boards.
* **Phase 2: NLP Domain Classification** (`ml`) – Fine-tuned a `DistilBERT-multilingual-cased` transformer model to perform Extreme Multi-Label Classification. The model categorized unstructured job descriptions into distinct domains (IT, Finance, HR) without prior machine translation.
* **Phase 3: Taxonomy & Skill Extraction** (`ml`) – Parsed unstructured text against the Lightcast Skills Taxonomy (ESCO-aligned) to extract explicit and implicit Hard (Specialized) and Soft (Common) skills.
* **Phase 4: Statistical & Topological Analysis** (`analysis`):
  * **OLS Regression:** Applied to track linear growth rates (slopes) and statistical significance (p-values) of emerging skills over time.
  * **Skill Bloat Testing:** Utilized Welch's t-test and Cohen's d to prove the statistically significant inflation in the volume of skills demanded per vacancy.
  * **Network Topology:** Constructed complex Skill Co-occurrence Networks, applying relative support thresholds and Density metrics to visualize the hybridization of modern professions.

## How to Run Locally

### Clone the repository

```bash
git clone https://github.com/sokolovolegg/labor_research
cd labor_research
```

### Set up the virtual environment

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate
```

### Install dependencies

```bash
pip install -r requirements.txt
```

## Data Access Note

The complete compiled datasets (`data/`) and the fine-tuned `.safetensors` model weights (`domain_classifier/`) exceed GitHub's file size limits. To reproduce the analysis, download the data from our shared [Google Drive Folder](#) or contact Oleg Sokolov at [oleg.sokolov@uzh.ch](mailto:oleg.sokolov@uzh.ch).