import json
import os
import argparse
import time
from termcolor import cprint
import google.generativeai as genai
from dotenv import load_dotenv

import re

load_dotenv()

class Config:
    def __init__(self, model_path):
        self.model = type("ModelConfig", (), {"model_path": model_path})

class GeminiModel:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        load_dotenv()
        genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
        self.generation_config = {
            "temperature": 0.2,
            "top_p": 0.95,
            "top_k": 64,
            "max_output_tokens": 1000000,
            # "response_mime_type": "application/json",
        }
        self.rate_limit = 5  # Requests per minute
        self.time_period = 60  # Seconds in a minute
        self.requests_made = 0
        self.start_time = time.time()
        self.model = self.load_model()

    def load_model(self):
        return genai.GenerativeModel(
            model_name=self.cfg.model.model_path,
            generation_config=self.generation_config,
        )

    def generate_response(self, inputs):
        try:
            response = self.model.generate_content(inputs["prompt"])
            self.requests_made += 1
            return response.text
        except Exception as e:
            cprint(f"Failed to generate response: {e}", "red")
            return None

def load_analysis_data(analysis_file):
    with open(analysis_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_table_pairs(db_id, analysis_data):
    if db_id not in analysis_data['database_details']:
        raise ValueError(f"Database ID '{db_id}' not found in analysis data")
    
    db_details = analysis_data['database_details'][db_id]
    table_pairs = []
    
    for pair_str, count in db_details['table_pairs'].items():
        pair_str = pair_str.strip("()'")
        tables = [t.strip("' ") for t in pair_str.split(",")]
        if len(tables) == 2:
            table_pairs.append((tables[0], tables[1], count))
    return sorted(table_pairs, key=lambda x: x[2], reverse=True)

def load_json_table(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def process_table_pair(db_id, table1, table2, json_dir, output_dir, api_key):
    pair_dir = os.path.join(output_dir, db_id, f"{table1}-{table2}")
    os.makedirs(pair_dir, exist_ok=True)
    
    table1_path = os.path.join(json_dir,  f"{table1}.json")
    table2_path = os.path.join(json_dir,  f"{table2}.json")
    
    if not os.path.exists(table1_path) or not os.path.exists(table2_path):
        print(f"Error: Could not find JSON files for tables {table1} and {table2}")
        return False

    table1_data = load_json_table(table1_path)
    table2_data = load_json_table(table2_path)
    generate_dataset(pair_dir, table1, table2, table1_data, table2_data)
    
    return True

def extract_json_from_response(response_text, table1, table2):
    """
    Extract JSON data structures from LLM response text with improved parsing.
    
    Args:
        response_text (str): The full text response from the model
        table1 (str): Name of the first table
        table2 (str): Name of the second table
        
    Returns:
        tuple: (tables_dict, examples_list, examples_text)
    """
    result = {}
    examples = []
    
    # First try to find JSON in code blocks
    json_blocks = re.findall(r'```(?:json)?\s*\n(.*?)\n```', response_text, re.DOTALL)
    
    # If that doesn't work, try to find JSON-like structures directly
    if not json_blocks or len(json_blocks) < 2:
        # Look for table structures with the expected format
        table_patterns = [
            # Pattern for table_name format
            r'{\s*"table_name"\s*:\s*"([^"]+)"\s*,\s*"columns"\s*:\s*\[.*?\],\s*"data"\s*:\s*\[.*?\]\s*}',
            # Fallback pattern without table_name
            r'{\s*"columns"\s*:\s*\[.*?\],\s*"data"\s*:\s*\[.*?\]\s*}'
        ]
        
        for pattern in table_patterns:
            potential_blocks = re.findall(pattern, response_text, re.DOTALL)
            if potential_blocks and (len(potential_blocks) >= 2 or pattern == table_patterns[0]):
                # For the first pattern, we might have found table names
                if pattern == table_patterns[0]:
                    # Try to extract each table by name
                    for table_name in potential_blocks:
                        table_pattern = r'{\s*"table_name"\s*:\s*"' + re.escape(table_name) + r'"\s*,\s*"columns"\s*:\s*\[.*?\],\s*"data"\s*:\s*\[.*?\]\s*}'
                        table_match = re.search(table_pattern, response_text, re.DOTALL)
                        if table_match:
                            try:
                                parsed = json.loads(table_match.group(0))
                                result[table_name] = parsed
                            except json.JSONDecodeError:
                                continue
                else:
                    # For the second pattern, just take the first two matches
                    for i, block in enumerate(potential_blocks[:2]):
                        try:
                            parsed = json.loads(block)
                            # Assign to table1 or table2 based on order
                            table_name = table1 if i == 0 else table2
                            result[table_name] = parsed
                        except json.JSONDecodeError:
                            continue
                
                # If we found tables, we can break
                if len(result) > 0:
                    break
    else:
        # Process JSON blocks found with the regex
        for block in json_blocks:
            try:
                # Try to parse the JSON
                parsed = json.loads(block)
                
                # Check if it's a table structure with table_name
                if isinstance(parsed, dict) and "table_name" in parsed:
                    result[parsed["table_name"]] = parsed
                # Check if it's a table structure without table_name
                elif isinstance(parsed, dict) and "columns" in parsed and "data" in parsed:
                    if table1 not in result:
                        result[table1] = parsed
                    elif table2 not in result:
                        result[table2] = parsed
                else:
                    # This might be example questions/queries
                    examples.append(parsed)
            except json.JSONDecodeError:
                # Not valid JSON, try cleaning it up
                try:
                    # Sometimes there are extra characters or formatting issues
                    cleaned_block = re.sub(r'(?m)^\s*//.*$', '', block)  # Remove comments
                    cleaned_block = re.sub(r',\s*}', '}', cleaned_block)  # Fix trailing commas
                    cleaned_block = re.sub(r',\s*]', ']', cleaned_block)  # Fix trailing commas in arrays
                    parsed = json.loads(cleaned_block)
                    
                    if isinstance(parsed, dict) and "table_name" in parsed:
                        result[parsed["table_name"]] = parsed
                    elif isinstance(parsed, dict) and "columns" in parsed and "data" in parsed:
                        if table1 not in result:
                            result[table1] = parsed
                        elif table2 not in result:
                            result[table2] = parsed
                    else:
                        examples.append(parsed)
                except json.JSONDecodeError:
                    # Still not valid JSON, skip
                    continue
    
    # Look for examples section
    examples_text = ""
    examples_match = re.search(r'#\s*Examples(.*?)(?:#|\Z)', response_text, re.DOTALL)
    if examples_match:
        examples_text = examples_match.group(1).strip()
    
    # Try to handle case where tables might be in examples_text
    if not result and examples_text:
        for table_name in [table1, table2]:
            table_pattern = r'{\s*"table_name"\s*:\s*"' + re.escape(table_name) + r'"\s*,\s*"columns"\s*:\s*\[.*?\],\s*"data"\s*:\s*\[.*?\]\s*}'
            table_match = re.search(table_pattern, examples_text, re.DOTALL)
            if table_match:
                try:
                    parsed = json.loads(table_match.group(0))
                    result[table_name] = parsed
                except json.JSONDecodeError:
                    continue
    
    # Make sure tables have the proper structure even if table_name was provided separately
    for table_name, table_data in result.items():
        if "table_name" not in table_data:
            table_data["table_name"] = table_name
    
    return result, examples, examples_text

def generate_dataset(pair_dir, table1, table2, table1_data, table2_data):
    cfg = Config(model_path="gemini-2.0-flash-thinking-exp-01-21")
    gemini_model = GeminiModel(cfg)
    
    with open(os.path.join(pair_dir, f"{table1}_original.json"), 'w', encoding='utf-8') as f:
        json.dump(table1_data, f, indent=2)
    
    with open(os.path.join(pair_dir, f"{table2}_original.json"), 'w', encoding='utf-8') as f:
        json.dump(table2_data, f, indent=2)
    
    table1_str = json.dumps(table1_data, indent=2)
    table2_str = json.dumps(table2_data, indent=2)
    
    prompt = f"""
    # Dataset Creation Guidelines for Multi-Table Question Answering

    ## Input Processing
    You'll receive multiple JSON files, each representing a table as an array of JSON objects (with each object being a row).

    ## Data Selection Guidelines
    - For tables with more than 100 rows, create a subset of maximum 50 rows and on avg 20 related rows in multiple tables.
    - Aim for approximately 20 rows per table on average
    - Select rows that preserve the original data distribution and maintain inter-table relationships

    ## Table Relationship Requirements
    Design tables to maintain logical connections through:
    - Foreign key relationships
    - Shared entities
    - Common data points
    - Other logical connections that support multi-hop reasoning

    ## Question Categories Support
    Ensure your tables allow for generating questions across these categories:
    - Basic: Match-Based Fact Checking, Multi-hop Fact Checking
    - Numerical: Arithmetic Calculation, Comparison, Aggregation, Ranking, Counting
    - Temporal: Time-based Calculation
    - Advanced: Multi-hop Numerical Reasoning, Descriptive Analysis
    - Analytical: Anomaly Detection, Statistical Analysis, Correlation Analysis, Causal Analysis
    - Predictive: Trend Forecasting, Impact Analysis

    ## Key Requirements
    - Keep tables concise but information-rich
    - Ensure tables work together to enable multi-table reasoning
    - Design data to support questions requiring reasoning across multiple tables
    - Include reasoning steps in question-answer pairs

    I'll provide you with two tables from a database. Your task is to:

    1. Analyze these tables and identify relationships between them
    2. Create optimized versions of each table (with maximum 50 rows per table)
    3. Drop Columns if to big, or text columns very big
    4. Format these tables in the exact JSON structure shown below:
    

    ```json
    {{
        "table_name": "name",
        "columns": ["column1", "column2", "column3"],
        "data": [
            [value1, value2, value3],
            [value1, value2, value3]
        ]
    }}
    ```

    ## Table 1: {table1}
    {table1_str}

    ## Table 2: {table2}
    {table2_str}

    ## Requirements
    - Return exactly TWO JSON tables with the structure shown above
    - Each JSON table must include the table_name field exactly as specified
    - Ensure tables maintain relationships with each otherfor multi-hop reasoning
    - Include foreign key relationships and shared entities in both table for mult-table question answering
    - Support questions requiring both tables to answer

    ## Output Format
    Your response must contain:
    1. First JSON table in the specified format with code block
    2. Second JSON table in the specified format with code block
    3. Example questions, SQL queries and answers

    """

    inputs = {"prompt": prompt}
    response_text = gemini_model.generate_response(inputs)
    cprint("Response generated", "green")

    
    if response_text is None:
        cprint("No response received.", "red")
        return


    with open(os.path.join(pair_dir, "full_response.txt"), 'w', encoding='utf-8') as f:
        f.write(response_text)
    

    tables, examples, examples_text = extract_json_from_response(response_text, table1, table2)

    # Additional logging for debugging
    cprint(f"Extracted tables: {list(tables.keys())}", "cyan")
    
    if table1 in tables:
        with open(os.path.join(pair_dir, f"{table1}_subset.json"), 'w', encoding='utf-8') as f:
            json.dump(tables[table1], f, indent=2)
        cprint(f"Successfully saved optimized table for {table1}", "green")
    else:
        cprint(f"Warning: Could not extract optimized table for {table1}", "yellow")
        # Try to save what we found if anything
        if len(tables) >= 1:
            first_table = list(tables.keys())[0]
            with open(os.path.join(pair_dir, f"{table1}_subset.json"), 'w', encoding='utf-8') as f:
                json.dump(tables[first_table], f, indent=2)
            cprint(f"Saved fallback table as {table1}_fallback.json", "yellow")
    
    if table2 in tables:
        with open(os.path.join(pair_dir, f"{table2}_subset.json"), 'w', encoding='utf-8') as f:
            json.dump(tables[table2], f, indent=2)
        cprint(f"Successfully saved optimized table for {table2}", "green")
    else:
        cprint(f"Warning: Could not extract optimized table for {table2}", "yellow")
        # Try to save what we found if anything
        if len(tables) >= 2:
            second_table = list(tables.keys())[1]
            with open(os.path.join(pair_dir, f"{table2}_subset.json"), 'w', encoding='utf-8') as f:
                json.dump(tables[second_table], f, indent=2)
            cprint(f"Saved fallback table as {table2}_fallback.json", "yellow")
    
    # Save examples
    if examples:
        with open(os.path.join(pair_dir, "examples.json"), 'w', encoding='utf-8') as f:
            json.dump(examples, f, indent=2)
    
    if examples_text:
        with open(os.path.join(pair_dir, "examples.txt"), 'w', encoding='utf-8') as f:
            f.write(examples_text)
    
    cprint(f"Generated dataset saved to {pair_dir}", "green")

def main():
    parser = argparse.ArgumentParser(
        description="Process database table pairs using Gemini to create multi-table QA datasets"
    )
    parser.add_argument(
        "--analysis_file",
        required=True,
        help="Path to the BIRD analysis JSON file"
    )
    parser.add_argument(
        "--json_dir",
        required=True,
        help="Directory containing JSON tables extracted from databases"
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Output directory where processed datasets will be stored"
    )
    parser.add_argument(
        "--db_id",
        required=True,
        help="Database ID to process"
    )
    parser.add_argument(
        "--num_pairs",
        type=int,
        default=5,
        help="Number of top table pairs to process"
    )
    parser.add_argument(
        "--api_key",
        required=True,
        help="Gemini API key"
    )
    args = parser.parse_args()
    

    print(f"Loading analysis data from {args.analysis_file}...")
    analysis_data = load_analysis_data(args.analysis_file)

    print(f"Getting table pairs for database {args.db_id}...")
    table_pairs = get_table_pairs(args.db_id, analysis_data)
    

    pairs_to_process = max(args.num_pairs, len(table_pairs))
    print(f"Processing top {pairs_to_process} table pairs...")
    
    for i, (table1, table2, count) in enumerate(table_pairs[:pairs_to_process]):
        print(f"Processing pair {i+1}/{pairs_to_process}: {table1} + {table2} (frequency: {count})")
        process_table_pair(args.db_id, table1, table2, args.json_dir, args.output_dir, args.api_key)
    
    print(f"Completed processing {pairs_to_process} table pairs")

if __name__ == "__main__":
    main()
    
    """
    python gen_db.py \
  --analysis_file /Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/bird_analysis/bird_multitable_analysis.json \
  --json_dir /Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/data/MtabVQA-BIRD/chicago_crime\
  --output_dir /Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/data/MtabVQA-BIRD/chicago_crime/gen_db \
  --db_id chicago_crime \
  --api_key API_KEY
    """