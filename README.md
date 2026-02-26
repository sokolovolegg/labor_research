# Labor Market Transformations: A 2016-2024 Retrospective

This project investigates structural transformations within the labor markets of the EU, CH, and the US over the past decade. By analyzing historical job postings, we track the evolution of employer demands, skill requirements, and market structure across three core sectors: **IT, Finance, and HR**.

## Project Objectives
* **Evolution of Skills:** Track how core requirements for candidates have changed over the last 10 years.
* **Impact of Macro-Events:** Analyze the impact of global disruptions (COVID-19, economic recessions, AI emergence) on the labor market.
* **Market Structure Dynamics:** Understand how the composition of job offerings (Junior vs. Middle vs. Senior) has transformed.

## Dataset
Since historical job data is rarely publicly available, we built a custom dataset using the **Internet Archive's Wayback Machine (CDX API)**. 
* **Current Scope:** Switzerland (`jobs.ch`).
* **Volume:** 23,474 unique, fully cleaned historical job postings (2016–2024).
* **Features:** Full job descriptions, posting dates, locations, and employment types.

## Repository Structure

\`\`\`text
labor_research/
├── data/                   # Directory for raw and cleaned CSV datasets (ignored in git)
├── src/
│   ├── parcing_setup/      # Asynchronous web scrapers for data collection (CDX API)
│   └── data_cleaning/      # Scripts and notebooks for EDA and deduplication
├── .gitignore              # Ignored files (data/, venv/, etc.)
├── requirements.txt        # Project dependencies
└── README.md               # Project documentation
\`\`\`

## Pipeline & Current Status

1. **Phase 1: Data Collection (CH Completed, EU, US - pending) **
   * Built an asynchronous Python scraper (`aiohttp`, `asyncio`) to extract historical snapshots from web archives.
   * Implemented checkpointing for reliable long-running data extraction.
2. **Phase 2: Data Cleaning (CH Completed)**
   * Processed ~28,000 raw records using `pandas`.
   * Extracted unique identifiers (`job_id`) and removed temporal duplicates, resulting in a gold-standard dataset of ~23.5k records.
3. **Phase 3: TBD** 


## How to Run Locally

1. **Clone the repository:**
   \`\`\`bash
   git clone <your_github_repo_link_here>
   cd labor_research
   \`\`\`

2. **Set up the virtual environment:**
   \`\`\`bash
   python -m venv venv
   # Windows:
   venv\Scripts\activate
   # Mac/Linux:
   source venv/bin/activate
   \`\`\`

3. **Install dependencies:**
   \`\`\`bash
   pip install -r requirements.txt
   \`\`\`

> **Note:** The `data/` directory is not tracked via Git due to file size constraints. If you need access to the pre-compiled and cleaned dataset, you can download it from our shared **[Google Drive Folder](https://drive.google.com/drive/folders/1KWPHsys0V140w393qvS7hhLLNBRuMxBR?usp=sharing)** or contact Oleg Sokolov at **oleg.sokolov@uzh.ch**.