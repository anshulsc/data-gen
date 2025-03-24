import os
import json
import time
import argparse
from termcolor import cprint
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

# ---------------------------
# Dummy Config and GeminiModel
# ---------------------------
class Config:
    def __init__(self, model_path):
        # A simple configuration object.
        self.model = type("ModelConfig", (), {"model_path": "gemini-2.0-flash" })


class GeminiModel:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        load_dotenv()
        genai.configure(api_key="AIzaSyBMLGJr-55FedA7vul19WYPbKpfIpg1I5w")
        self.generation_config = {
            "temperature": 1,
            "top_p": 0.95,
            "top_k": 64,
            "max_output_tokens": 8192,
            "response_mime_type": "application/json",
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
        # For our purpose, we are not using files, so history is empty.
        chat_session = self.model.start_chat(history=[])
        try:
            response = chat_session.send_message(inputs["prompt"])
        except Exception as e:
            cprint(f"Failed to generate response: {e}", "red")
            return None
        self.requests_made += 1
        return response.text

# ---------------------------
# VQA Generation Functions
# ---------------------------
def generate_vqa_for_database(db_folder, gemini_model):
    """
    For the given database folder, load all table JSON files (those starting with 'table_' and ending with '.json'),
    combine them into a dictionary, generate at least 10 VQA pairs using the GeminiModel,
    and write the pairs to a 'vqa.jsonl' file in the same folder.
    """
    # Find table JSON files in the current database folder.
    table_files = [f for f in os.listdir(db_folder) if f.endswith(".json") or f.endswith(".jsonl")]
    if "vqa.jsonl" in table_files:
        cprint(f"Skipping database folder {db_folder}: VQA file already exists.", "yellow")
        return
    if not table_files:
        cprint(f"No table JSON files found in {db_folder}", "yellow")
        return

    tables_data = {}
    for tf in table_files:
        table_path = os.path.join(db_folder, tf)
        try:
            with open(table_path, "r", encoding="utf-8") as f:
                table_json = json.load(f)
            # Extract table name from filename: e.g., "table_employees.json" -> "employees"
            table_name = tf[:-len(".json")]
            tables_data[table_name] = table_json
        except Exception as e:
            cprint(f"Error loading {table_path}: {e}", "red")

    # Define the system prompt for VQA generation.
    SYSTEM_PROMPT = (
        "You are a system for generating complex question-answer pairs from structured JSON data representing relational tables. "
        "Your task is to create a diverse set of questions that require multi-step reasoning and utilize different operations on the data.\n\n"
        "Specifically:\n\n"
        "Input: You will be given a set of JSON data representing tables. These tables can be related to each other.\n"
        "Task: Generate at least 10 question-answer pairs in JSON format. Each pair should include the following keys:\n"
        "\"question\": A complex question that requires reasoning across multiple tables and the use of operations such as filtering, joining, aggregation, counting, etc.\n"
        "\"reasoning steps\": A list of numbered strings describing the logical steps needed to arrive at the answer. Be specific about table names and operations performed.\n"
        "\"answer\": A JSON object containing the final answer. The answer format is a nested list structure \"data\": [ [value1], [value2], ... ].\n"
        "\"tables\": A list of strings indicating which tables from the input data were used in answering the question.\n\n"
        "Reasoning Requirements: The generated questions should require a variety of reasoning skills, including:\n"
        "Selection/Filtering, List, Counts, Joins across multiple tables, Aggregations (e.g., sum, average, max, min) and other table operations (e.g., grouping, sorting).\n\n"
        "Diversity: Aim for a diverse set of questions that explore different aspects of the data and require different combinations of reasoning steps.\n\n"
        "JSON format example:\n"
        "{\n"
        "  \"question\": \"Question goes here\",\n"
        "  \"reasoning steps\": [\n"
        "    \"Step 1: ...\",\n"
        "    \"Step 2: ...\"\n"
        "  ],\n"
        "  \"answer\": {\"data\": [ [value1], [value2], ... ] },\n"
        "  \"tables\": [\n"
        "    \"Table1\",\n"
        "    \"Table2\"\n"
        "  ]\n"
        "}\n\n"
        "Begin generating the question-answer pairs based on the specifications above. Give at least 10 Visual Question-Answers."
    )

    # Build the full prompt by appending the JSON representation of the tables data.
    prompt = SYSTEM_PROMPT + "\n\nInput Tables Data:\n" + json.dumps(tables_data, indent=2)

    cprint("Generating VQA pairs...", "cyan")
    inputs = {"files": [], "prompt": prompt}
    response_text = gemini_model.generate_response(inputs)

    if response_text is None:
        cprint("No response received.", "red")
        return

    try:
        # Expecting the response to be a JSON array of Q-A pairs.
        vqa_pairs = json.loads(response_text)
    except Exception as e:
        cprint(f"Error parsing response JSON: {e}", "red")
        cprint("Response text:", "red")
        cprint(response_text, "red")
        return

    # Write each Q-A pair as one JSON object per line (jsonl format)
    output_path = os.path.join(db_folder, "vqa.jsonl")
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            for pair in vqa_pairs:
                f.write(json.dumps(pair) + "\n")
        cprint(f"VQA pairs written to {output_path}", "green")
    except Exception as e:
        cprint(f"Error writing VQA pairs to file: {e}", "red")


def process_dataset_folder(dataset_folder, gemini_model):
    """
    Iterates over each subdirectory inside the dataset_folder.
    For each subdirectory (representing a database), generate VQA pairs using the available table JSON files.
    """
    if not os.path.isdir(dataset_folder):
        cprint(f"Error: {dataset_folder} is not a valid directory.", "red")
        return

    for subdir in os.listdir(dataset_folder):
        subdir_path = os.path.join(dataset_folder, subdir)
        if os.path.isdir(subdir_path):
            cprint(f"Processing database folder: {subdir_path}", "magenta")
            generate_vqa_for_database(subdir_path, gemini_model)
        else:
            cprint(f"Skipping non-directory: {subdir_path}", "yellow")


def main():
    parser = argparse.ArgumentParser(
        description="Generate VQA pairs from database table JSON files and store them as a jsonl file in each database folder."
    )
    parser.add_argument(
        "--dataset_folder",
        "-d",
        required=True,
        help="Path to the dataset folder containing database subdirectories."
    )

    args = parser.parse_args()

    cfg = Config(model_path="gemini-2.0-flash")
    gemini_model = GeminiModel(cfg)

    process_dataset_folder(args.dataset_folder, gemini_model)


if __name__ == '__main__':
    main()


"""

python spider_vqa.py -d "/Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/data/MTabVQA-Spider1.0"

"""