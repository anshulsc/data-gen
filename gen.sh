#!/bin/bash

# Check if db_id parameter is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <db_id>"
    exit 1
fi

db_id=$1

# Base directories
dataset_folder="/Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/raw_data/BIRD/train/train_databases"
output_folder="/Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/data/MTabVQA-BIRD"
gen_db_folder="${output_folder}/${db_id}/gen_db"
analysis_file="/Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/bird_analysis/bird_multitable_analysis.json"

# Create output directory if it doesn't exist
mkdir -p "${output_folder}"

# Step 1: Extract tables from database
echo "Extracting tables for database ${db_id}..."
python db_scripts/db_extract_tables.py \
    --dataset_folder "${dataset_folder}" \
    --output_folder "${output_folder}" \
    --db_id "${db_id}"\
    --max_rows 500

# Create gen_db directory
echo "Creating gen_db directory..."
mkdir -p "${gen_db_folder}"

# Step 2: Generate database
echo "Generating database..."
python db_scripts/gen_db.py \
    --analysis_file "${analysis_file}" \
    --json_dir "${output_folder}/${db_id}" \
    --output_dir "${gen_db_folder}" \
    --db_id "${db_id}" \
    --api_key "API_KEY

echo "Database extraction and generation completed for ${db_id}!"