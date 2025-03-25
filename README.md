# TableVQA Data Generation and Annotation

This repository contains tools for generating and annotating visual question-answering (VQA) data for multi-table database scenarios.

## Overview

The project provides a complete pipeline for:
1. Extracting structured data from databases
2. Generating paired table subsets
3. Creating VQA (Visual Question Answer) pairs using LLMs
4. Annotating and validating generated QA pairs

## Prerequisites

- Python 3.8+
- Gemini API key

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file with your Gemini API key:
   ```
   GEMINI_API_KEY=your_api_key_here
   ```

## Data Generation Pipeline

### Step 1: Extract Tables from Database

Extract tables from a database and prepare them for processing:

```bash
./gen.sh <db_id>
```

This script:
1. Extracts tables from a source database (`db_extract_tables.py`)
2. Generates paired table subsets with related data (`gen_db.py`)

### Step 2: Generate VQA Pairs

Generate question-answer pairs based on the extracted tables:

```bash
./vqa.sh <db_id>
```

This script runs `tableVQA.py` to:
1. Generate complex multi-table reasoning questions
2. Create corresponding answers
3. Verify generated QA pairs with LLM verification

## Human Annotation Tool

After generating QA pairs, you can use the Streamlit-based annotation tool to review and annotate them:

```bash
streamlit run app.py
```

The annotation tool allows you to:
- Browse databases and table pairs
- Review AI-generated QA pairs
- Validate, modify, or reject questions and answers
- Save human-annotated datasets

### Annotation Options

For each QA pair, you can:
- ✓ Mark as correct (no changes needed)
- 🖊️ Submit a modified answer
- 📝 Modify both question and answer
- ❌ Reject the pair as invalid

## Project Structure

- `VQA/` - Visual Question Answering generation scripts
  - `tableVQA.py` - Main VQA generation script
  - `prompts/` - Prompt templates and categories
- `db_scripts/` - Database extraction and processing scripts
  - `db_extract_tables.py` - Extract tables from databases
  - `gen_db.py` - Generate related table subsets
  - `spider_vqa.py` - VQA generation for Spider datasets
- `app.py` - Streamlit annotation application

## Example Workflow

```bash
# Step 1: Extract and generate database tables
./gen.sh example_db

# Step 2: Generate VQA pairs
./vqa.sh example_db

# Step 3: Run annotation tool
streamlit run app.py
```

## Output Format

The generated datasets follow this structure:

```
BIRD_INSTRUCT/
└── <db_id>/
    └── <table1>-<table2>/
        ├── <table1>_subset.json
        ├── <table2>_subset.json
        ├── all_qa_pairs.json
        └── human_annotated.json (after annotation)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
