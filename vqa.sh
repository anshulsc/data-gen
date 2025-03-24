#!/bin/bash

# Check if db_id parameter is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <db_id>"
    exit 1
fi

db_id=$1

# Base directories
output_folder="/Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/BIRD_INSTRUCT/${db_id}"
dataset_folder="/Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/data/MtabVQA-BIRD/${db_id}/gen_db/${db_id}"

# Create output directory if it doesn't exist
echo "Creating output directory..."
mkdir -p "${output_folder}"

# Run tableVQA.py script
echo "Running tableVQA.py for database ${db_id}..."
python vqa/tableVQA.py \
    --dataset_folder "${dataset_folder}" \
    --output_folder "${output_folder}" \
    --api_key "AIzaSyCt_99EHzoe3K6uxQ86JBQbo6LuUSl3pnk"

echo "TableVQA generation completed for ${db_id}!"