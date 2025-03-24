import os
import json
import time
import argparse
import random
from typing import List, Dict, Any
import google.generativeai as genai
from termcolor import cprint
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from prompts.categories import load_question_types_and_examples
from prompts.prompts_vqa import VQA_PROMPT
load_dotenv()

class Config:
    def __init__(self, model_path="gemini-2.0-flash"):
        self.model = type("ModelConfig", (), {"model_path": model_path})

class GeminiModel:
    def __init__(self, cfg: Config, temperature=1.0, rate_limit=5):
        self.cfg = cfg
        genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
        self.generation_config = {
            "temperature": temperature,
            "top_p": 0.95,
            "top_k": 64,
            "max_output_tokens": 300000,
        }
        # Rate limiting
        self.rate_limit = rate_limit
        self.time_period = 120
        self.requests_made = 0
        self.start_time = time.time()
        self.model = self.load_model()

    def load_model(self):
        return genai.GenerativeModel(
            model_name=self.cfg.model.model_path,
            generation_config=self.generation_config,
        )

    def generate_response(self, prompt):
        # Check rate limit
        current_time = time.time()
        if current_time - self.start_time > self.time_period:
            self.requests_made = 0
            self.start_time = current_time
        
        if self.requests_made >= self.rate_limit:
            wait_time = 30
            if wait_time > 0:
                cprint(f"Rate limit reached. Waiting {wait_time:.2f} seconds...", "yellow")
                time.sleep(wait_time)
                self.requests_made = 0
                self.start_time = time.time()
        
        chat_session = self.model.start_chat(history=[])
        try:
            response = chat_session.send_message(prompt)
            self.requests_made += 1
            return response.text
        except Exception as e:
            cprint(f"Failed to generate response: {e}", "red")
            return None


def generate_qa_pair(tables_data: Dict[str, Any], question_type: str, examples: List[str], gemini_model: GeminiModel):
    
    unique_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
    table_count = len(tables_data)
    
    prompt_template = VQA_PROMPT
    
    formatted_prompt = prompt_template.format(
        table_count=table_count,
        tables_json=json.dumps(tables_data, indent=2),
        question_type=question_type,
        examples="\n".join(examples),
        unique_id=unique_id
    )
    
    response = gemini_model.generate_response(formatted_prompt)
    if response is None:
        return None
    
    # Clean the response to handle markdown code blocks
    cleaned_response = clean_model_response(response)
    time.sleep(10)
    try:
        qa_pair = json.loads(cleaned_response)
        
        # Validate that the QA pair uses multiple tables
        if len(qa_pair.get('tables_used', [])) < 2:
            cprint(f"QA pair for {question_type} doesn't use multiple tables, regenerating...", "yellow")
            return generate_qa_pair(tables_data, question_type, examples, gemini_model)
        
        # Ensure the ID is present
        if 'id' not in qa_pair:
            qa_pair['id'] = unique_id
            
        return qa_pair
    except json.JSONDecodeError as e:
        cprint(f"Failed to parse JSON from response for {question_type}: {e}", "red")
        cprint(f"Response: {cleaned_response[:100]}...", "red")
        return None

def clean_model_response(response):
  
    if "```json" in response or "```" in response:
        import re
        json_pattern = r"```(?:json)?\s*([\s\S]*?)```"
        matches = re.findall(json_pattern, response)
        if matches:
            return matches[0].strip()
    return response.strip()



def majority_voting(verification_results):
    
    valid_votes = sum(1 for result in verification_results if result.get("is_valid", False))
    multiple_tables_votes = sum(1 for result in verification_results if result.get("uses_multiple_tables", False))
    avg_score = sum(result.get("score", 0) for result in verification_results) / len(verification_results)
    is_selected = (valid_votes >= (len(verification_results) / 2) and 
                  multiple_tables_votes >= (len(verification_results) / 2) and 
                  avg_score >= 7.0)
    
    return {
        "is_selected": is_selected,
        "valid_votes": valid_votes,
        "multiple_tables_votes": multiple_tables_votes,
        "total_votes": len(verification_results),
        "average_score": avg_score,
        "verification_comments": [result.get("verification_comments", "") for result in verification_results]
    }
    
def verify_qa_pair(qa_pair: Dict[str, Any], tables_data: Dict[str, Any], gemini_model: GeminiModel):
    
    verification_prompt = f"""
    You are a verification agent for table-based question answering. You need to verify if the answer and reasoning for the given question are correct.
    
    [Tables Used]
    {json.dumps(tables_data, indent=2)}
    
    [Question-Answer Pair]
    Question: {qa_pair['question']}
    Answer: {json.dumps(qa_pair.get('answer', {}), indent=2)}
    Reasoning Steps: {qa_pair.get("reasoning_steps", {})}
    Question Type: {qa_pair.get('question_type', 'Unknown')}
    
    Your task:
    1. Check if the question is well-formed and requires multi-hop reasoning across MULTIPLE tables
    2. Verify if the answer is accurate based on the given tables, if answer is not right then is valid is False.
    3. Check if the tables_used field accurately lists all relevant tables (must be at least 2 tables)
    4. Chack and Validate reasnong steps as well.
    Respond with ONLY a JSON object (no markdown formatting or code blocks) containing:
    {{
        "is_valid": true/false,
        "verification_comments": "Your detailed verification comments",
        "score": <a score from 0-10 where 10 is perfect>,
        "uses_multiple_tables": true/false
    }}
    """
    
    
    response = gemini_model.generate_response(verification_prompt)
    if response is None:
        return {"is_valid": False, "verification_comments": "Verification failed due to model error", "score": 0, "uses_multiple_tables": False}
    
    cleaned_response = clean_model_response(response)
    
    try:
        verification_result = json.loads(cleaned_response)
        return verification_result
    except json.JSONDecodeError as e:
        cprint(f"Failed to parse verification response: {e}", "red")
        cprint(f"Response: {cleaned_response[:100]}...", "red")
        return {"is_valid": False, "verification_comments": "Failed to parse verification result", "score": 0, "uses_multiple_tables": False}


def safe_json_dump(data, file_path):
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        cprint(f"Error writing to {file_path}: {e}", "red")
        return False
    
def load_tables_from_folder(db_folder):
    tables_data = {}
    table_files = [f for f in os.listdir(db_folder) if f.endswith("_subset.json")]
    
    if not table_files:
        cprint(f"No table files found in {db_folder}", "yellow")
        return None
    
    for tf in table_files:
        table_path = os.path.join(db_folder, tf)
        try:
            with open(table_path, "r", encoding="utf-8") as f:
                table_json = json.load(f)
            table_name = tf.replace("_subset.json", "")
            tables_data[table_name] = table_json
        except Exception as e:
            cprint(f"Error loading {table_path}: {e}", "red")
    
    return tables_data

def process_database_folder(db_folder, output_folder, types, examples, generator_model, verifier_models):
    
    db_name = os.path.basename(db_folder)
    output_db_folder = os.path.join(output_folder, db_name)
    os.makedirs(output_db_folder, exist_ok=True)
    
    tables_data = load_tables_from_folder(db_folder)
    if not tables_data:
        return

    if len(tables_data) < 2:
        cprint(f"Skipping {db_name}: Only {len(tables_data)} tables found (need at least 2)", "yellow")
        return

    for table_name, table_data in tables_data.items():
        output_table_path = os.path.join(output_db_folder, f"{table_name}_subset.json")
        safe_json_dump(table_data, output_table_path)

    valid_qa_pairs = []
    invalid_qa_pairs = []
    failed_qa_types = []
    
    for question_type in types:
        type_examples = examples.get(question_type, ["No examples available"])
        cprint(f"Generating QA pair for type: {question_type}", "cyan")
        
        for attempt in range(3):
            qa_pair = generate_qa_pair(tables_data, question_type, type_examples, generator_model)
            if qa_pair:
                break
            elif attempt < 2:  
                cprint(f"Attempt {attempt+1} failed, retrying...", "yellow")
                time.sleep(2) 
        
        if not qa_pair:
            cprint(f"Failed to generate QA pair for {question_type} after 3 attempts", "red")
            failed_qa_types.append(question_type)
            continue
        
        verification_results = []
        for i, verifier_model in enumerate(verifier_models):
            cprint(f"Verifying with model {i+1}...", "cyan")
            time.sleep(5)
            verification_result = verify_qa_pair(qa_pair, tables_data, verifier_model)
            verification_results.append(verification_result)
        voting_result = majority_voting(verification_results)

        qa_pair["verification"] = voting_result

        if voting_result["is_selected"]:
            valid_qa_pairs.append(qa_pair)
            cprint(f"✓ QA pair for {question_type} passed verification", "green")
        else:
            # Store non-valid QA pairs as well
            invalid_qa_pairs.append(qa_pair)
            cprint(f"✗ QA pair for {question_type} failed verification, but stored for analysis", "yellow")
        time.sleep(10)
 
    # Save valid QA pairs
    output_valid_qa_path = os.path.join(output_db_folder, "valid_qa_pairs.json")
    if valid_qa_pairs:
        if safe_json_dump(valid_qa_pairs, output_valid_qa_path):
            cprint(f"Generated {len(valid_qa_pairs)} verified QA pairs for {db_name}.", "green")
        else:
            cprint(f"Failed to write valid QA pairs to file for {db_name}.", "red")
    else:
        cprint(f"No valid QA pairs generated for {db_name}. Failed types: {failed_qa_types}", "red")
        safe_json_dump([], output_valid_qa_path)
    
    # Save invalid QA pairs
    output_invalid_qa_path = os.path.join(output_db_folder, "invalid_qa_pairs.json")
    if invalid_qa_pairs:
        if safe_json_dump(invalid_qa_pairs, output_invalid_qa_path):
            cprint(f"Stored {len(invalid_qa_pairs)} invalid QA pairs for {db_name} for analysis.", "yellow")
        else:
            cprint(f"Failed to write invalid QA pairs to file for {db_name}.", "red")
    else:
        cprint(f"No invalid QA pairs generated for {db_name}.", "cyan")
        safe_json_dump([], output_invalid_qa_path)
    
    # Save all QA pairs (both valid and invalid) for completeness
    output_all_qa_path = os.path.join(output_db_folder, "all_qa_pairs.json")
    all_qa_pairs = valid_qa_pairs + invalid_qa_pairs
    if all_qa_pairs:
        if safe_json_dump(all_qa_pairs, output_all_qa_path):
            cprint(f"Processing complete for {db_name}. Total QA pairs: {len(all_qa_pairs)}", "green")
        else:
            cprint(f"Failed to write all QA pairs to file for {db_name}.", "red")
    
    # For backward compatibility, also save in the original format
    output_qa_path = os.path.join(output_db_folder, "qa_pairs.json")
    if safe_json_dump(valid_qa_pairs, output_qa_path):
        cprint(f"Saved valid QA pairs to legacy path for {db_name}.", "green")
        
        
def process_dataset(dataset_folder, output_folder, generator_model, verifier_models):

    if not os.path.exists(dataset_folder):
        cprint(f"Dataset folder {dataset_folder} does not exist", "red")
        return
    
    os.makedirs(output_folder, exist_ok=True)

    types, examples = load_question_types_and_examples()

    db_folders = [os.path.join(dataset_folder, d) for d in os.listdir(dataset_folder) 
                  if os.path.isdir(os.path.join(dataset_folder, d))]
    
    for db_folder in db_folders:
        cprint(f"Processing database: {os.path.basename(db_folder)}", "magenta")
        process_database_folder(db_folder, output_folder, types, examples, generator_model, verifier_models)

def main():
    parser = argparse.ArgumentParser(
        description="Generate and verify multi-hop QA pairs for database tables."
    )
    parser.add_argument(
        "--dataset_folder", "-d", required=True,
        help="Path to the dataset folder containing database subdirectories"
    )
    parser.add_argument(
        "--output_folder", "-o", required=True,
        help="Path to the output folder where processed data will be stored"
    )
    parser.add_argument(
        "--api_key", "-k", 
        help="API key for Gemini model (overrides env variable)"
    )
    parser.add_argument(
        "--model", "-m", default="gemini-2.0-flash",
        help="Model to use for generation (default: gemini-2.0-flash)"
    )
    parser.add_argument(
        "--temp", "-t", type=float, default=1.0,
        help="Temperature for generation (default: 1.0)"
    )
    
    args = parser.parse_args()
    
 
    if args.api_key:
        os.environ["GEMINI_API_KEY"] = args.api_key
    

    generator_cfg = Config(model_path="gemini-2.0-flash")
    generator_model = GeminiModel(generator_cfg, temperature=args.temp)
    

    verifier_models = [
        GeminiModel(Config(model_path=args.model), temperature=0.5),
        GeminiModel(Config(model_path=args.model), temperature=0.7),
        GeminiModel(Config(model_path=args.model), temperature=0.9)
    ]
    
    process_dataset(args.dataset_folder, args.output_folder, generator_model, verifier_models)
    cprint("All database folders processed successfully!", "green")

if __name__ == "__main__":
    main()
    
"""

mkdir /Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/BIRD_INSTRUCT/{db_id}
python tableVQA.py \
    --dataset_folder "/Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/data/MtabVQA-BIRD/{db_id}gen_db/{db_id}" \
    --output_folder "/Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/BIRD_INSTRUCT/{db_id}" \
    --api_key "AIzaSyCt_99EHzoe3K6uxQ86JBQbo6LuUSl3pnk"

"""