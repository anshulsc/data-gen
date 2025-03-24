"""
python multitab_gen.py \
    --multitabqa_path /path/to/multitabqa.parquet \
    --output_dir /path/to/output_dir \
    --multiprocessing


python multitab_gen.py \
    --multitabqa_path /path/to/multitabqa.jsonl \
    --output_dir /path/to/output_dir \
    --multiprocessing
    
python multitab_gen.py \
    --atis_path raw_data/atis-tableQA/data/train-00000-of-00001-958582fd079297ac.parquet \
    --output_dir /Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/data/ATIS \
    --multiprocessing

"""

import os
import json
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from table_gen.table2img2 import generate_table_image_multitab
import pandas as pd
import argparse
import json
from datasets import load_dataset
from termcolor import cprint


def process_item(args):
    """
    Processes a single item from the MMQA dataset.
    """
    item, output_dir, table_images_dir, index = args
    question = item["query"]
    answer = item["answer"]
    table_names = item["table_names"]
    tables = item["tables"]

    qa_pairs = []
    table_image_ids = []

    for i, table in enumerate(tables):
        table_name = table_names[i] if i < len(table_names) else f"table_{i}"
        image_id, image_path = generate_table_image_multitab(
            table, table_name, table_images_dir
        )
        if image_id is not None:
            table_image_ids.append(image_id)

    new_qa_pair = {
        "question": str(question),
        "answer": json.loads(str(answer)),
        "table_names": str(table_names),
        "table_image_ids": table_image_ids,
        "original_data_index": index,
    }
    qa_pairs.append(new_qa_pair)

    return qa_pairs


def process_item_query(args):
    """
    Processes a single item from the MMQA dataset.
    """
    item, output_dir, table_images_dir, index = args
    question = item["question"]
    answer = item["answer"]
    table_names = item["table_names"]
    tables = item["table_texts"]

    qa_pairs = []
    table_image_ids = []

    for i, table in enumerate(tables):
        table_name = table_names[i] if i < len(table_names) else f"table_{i}"
        image_id, image_path = generate_table_image_multitab(
            table, table_name, table_images_dir
        )
        if image_id is not None:
            table_image_ids.append(image_id)

    new_qa_pair = {
        "question": str(question),
        "answer": json.loads(str(answer)),
        "table_names": str(table_names),
        "table_image_ids": table_image_ids,
        "original_data_index": index,
    }
    qa_pairs.append(new_qa_pair)

    return qa_pairs


def process_atis_item(args):
    """
    Processes a single item from the ATIS dataset.
    """
    item, output_dir, table_images_dir, index = args
    query = item["query"]
    question = item["question"]
    answer = item["answer"]
    table_names = item["table_names"] if "table_names" in item else []
    tables = item["tables"] if "tables" in item else []

    if isinstance(table_names, str):
        try:
            table_names = json.loads(table_names)
        except json.JSONDecodeError:
            table_names = [table_names]

    qa_pairs = []
    table_image_ids = []

    for i, table in enumerate(tables):
        if isinstance(table, dict) and "table_content" in table:
            if len(table["table_content"]) > 50:
                table["table_content"] = table["table_content"][:50]
        elif isinstance(table, pd.DataFrame):
            if len(table) > 50:
                table = table.head(50)

        table_name = table_names[i] if i < len(table_names) else f"table_{i}"
        image_id, image_path = generate_table_image_multitab(
            table, table_name, table_images_dir
        )
        if image_id is not None:
            table_image_ids.append(image_id)

    new_qa_pair = {
        "question": str(question),
        "answer": answer,  # Assuming answer is already in the correct format
        "table_names": str(table_names),
        "table_image_ids": table_image_ids,
        "query": query,
        "original_data_index": index,
    }
    qa_pairs.append(new_qa_pair)

    return qa_pairs


def create_extended_multitab_dataset(
    multitabqa_path, output_dir, use_multiprocessing, num_processes
):
    """
    Creates an extended MMQA dataset with table images, writing results incrementally.
    """
    table_images_dir = os.path.join(output_dir, "table_images")
    os.makedirs(table_images_dir, exist_ok=True)
    extended_json_path = os.path.join(output_dir, "multitabqa_extended_02.jsonl")

    # Load MMQA data
    print("Loading data...")
    mmqa_data = pd.read_parquet(multitabqa_path)

    total_items = len(mmqa_data)
    print(f"Processing {total_items} items...")

    # Open the output file in append mode
    with open(extended_json_path, "a") as outfile:
        if use_multiprocessing:
            with Pool(processes=num_processes) as pool:
                args_iter = [
                    (item.to_dict(), output_dir, table_images_dir, i)
                    for i, item in mmqa_data.iterrows()
                ]

                for result in tqdm(
                    pool.imap(process_item, args_iter),
                    total=total_items,
                    desc="Processing items (multiprocessing)",
                    unit="item",
                ):
                    for qa_pair in result:
                        # Write each qa_pair as a separate JSON object, followed by a newline
                        outfile.write(json.dumps(qa_pair) + "\n")
        else:
            for i, item in tqdm(
                mmqa_data.iterrows(), desc="Processing items", unit="item"
            ):
                result = process_item((item.to_dict(), output_dir, table_images_dir, i))
                for qa_pair in result:
                    # Write each qa_pair as a separate JSON object, followed by a newline
                    outfile.write(json.dumps(qa_pair) + "\n")

    print(
        f"Successfully processed {total_items} items and saved results incrementally to {extended_json_path}."
    )


def create_extended_multitab_query_dataset(
    multitabqa_path, output_dir, use_multiprocessing, num_processes
):
    """
    Creates an extended MMQA dataset with table images, writing results incrementally.
    """
    table_images_dir = os.path.join(output_dir, "table_images")
    os.makedirs(table_images_dir, exist_ok=True)
    extended_json_path = os.path.join(output_dir, "multitabqa_extended_02.jsonl")

    # Load MMQA data
    print("Loading data...")

    mmqa_data = load_dataset(multitabqa_path, split="train")

    total_items = len(mmqa_data)
    print(f"Processing {total_items} items...")

    # Open the output file in append mode
    with open(extended_json_path, "a") as outfile:
        if use_multiprocessing:
            with Pool(processes=num_processes) as pool:
                args_iter = [
                    (item, output_dir, table_images_dir, i)
                    for i, item in enumerate(mmqa_data)
                ]

                for result in tqdm(
                    pool.imap(process_item_query, args_iter),
                    total=total_items,
                    desc="Processing items (multiprocessing)",
                    unit="item",
                ):
                    for qa_pair in result:
                        # Write each qa_pair as a separate JSON object, followed by a newline
                        outfile.write(json.dumps(qa_pair) + "\n")
        else:
            for i, item in tqdm(
                enumerate(mmqa_data), desc="Processing items", unit="item"
            ):
                result = process_item_query((item, output_dir, table_images_dir, i))
                for qa_pair in result:
                    # Write each qa_pair as a separate JSON object, followed by a newline
                    outfile.write(json.dumps(qa_pair) + "\n")

    print(
        f"Successfully processed {total_items} items and saved results incrementally to {extended_json_path}."
    )


def create_extended_atis_dataset(
    atis_path, output_dir, use_multiprocessing, num_processes
):
    """
    Creates an extended dataset from ATIS data with table images, ensuring tables don't exceed 50 rows.
    """
    table_images_dir = os.path.join(output_dir, "table_images")
    os.makedirs(table_images_dir, exist_ok=True)
    extended_json_path = os.path.join(output_dir, "MTabVQA-atis.jsonl")

    try:
        atis_data = pd.read_parquet(atis_path)
        atis_data = atis_data.to_dict("records")
        cprint("Loaded ATIS data using pandas read_parquet.", "green")
        # cprint("Atis data: ", "green", atis_data)
    except Exception as e:
        try:
            atis_data = load_dataset(atis_path, split="train")
            cprint("Loaded ATIS data using Hugging Face datasets library.", "green")
            cprint("Atis data: ", "green", atis_data[0])
        except Exception as e:
            print(f"Error loading ATIS data: {e}")
            return

    total_items = len(atis_data)
    cprint(f"Processing {total_items} ATIS items...")

    processed_items = []
    for i, item in enumerate(atis_data):
        query = item.get("query", "")
        question = item.get("question", "")
        answer = item.get("answer", {})
        table_names = []
        if "table_names" in item:
            table_names = item["table_names"]

        tables = []
        if "tables" in item:
            tables = item["tables"]

        if "columns" in item and "data" in item:
            table = {"table_columns": item["columns"], "table_content": item["data"]}
            tables.append(table)

        processed_item = {
            "question": question,
            "answer": answer,
            "table_names": table_names,
            "tables": tables,
            "query": query,
        }
        processed_items.append(processed_item)

    with open(extended_json_path, "a") as outfile:
        if use_multiprocessing:
            with Pool(processes=num_processes) as pool:
                args_iter = [
                    (item, output_dir, table_images_dir, i)
                    for i, item in enumerate(processed_items)
                ]

                for result in tqdm(
                    pool.imap(process_atis_item, args_iter),
                    total=total_items,
                    desc="Processing ATIS items (multiprocessing)",
                    unit="item",
                ):
                    for qa_pair in result:
                        outfile.write(json.dumps(qa_pair) + "\n")
        else:
            for i, item in tqdm(
                enumerate(processed_items), desc="Processing ATIS items", unit="item"
            ):
                result = process_atis_item((item, output_dir, table_images_dir, i))
                for qa_pair in result:
                    outfile.write(json.dumps(qa_pair) + "\n")

    cprint(
        f"Successfully processed {total_items} ATIS items and saved results incrementally to {extended_json_path}.",
        "yellow",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create an extended dataset with table images from various sources."
    )
    parser.add_argument(
        "--multitabqa_path",
        help="Path to the original MMQA JSON dataset.",
    )
    parser.add_argument(
        "--atis_path",
        help="Path to the ATIS dataset.",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Directory to save the extended dataset and images.",
    )
    parser.add_argument(
        "--multiprocessing",
        action="store_true",
        help="Enable multiprocessing for faster processing.",
    )
    parser.add_argument(
        "--num_processes",
        type=int,
        default=cpu_count(),
        help="Number of processes to use if multiprocessing is enabled (default: number of CPU cores).",
    )
    parser.add_argument(
        "--query",
        action="store_true",
        help="Enable if the multitabqa_path is a query dataset.",
    )
    args = parser.parse_args()

    if args.multitabqa_path:
        if args.query:
            create_extended_multitab_query_dataset(
                args.multitabqa_path,
                args.output_dir,
                args.multiprocessing,
                args.num_processes,
            )
        else:
            create_extended_multitab_dataset(
                args.multitabqa_path,
                args.output_dir,
                args.multiprocessing,
                args.num_processes,
            )

    if args.atis_path:
        cprint("Creating extended ATIS dataset...", "green")
        create_extended_atis_dataset(
            args.atis_path,
            args.output_dir,
            args.multiprocessing,
            args.num_processes,
        )
