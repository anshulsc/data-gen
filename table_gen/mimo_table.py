""" python data_gen/mimo_table.py \
  --mimotable_json_path "MiMoTable/data/english/problems.json"\
  --mimotable_spreadsheet_dir "MiMoTable/data/english/spreadsheet" \
  --output_dir "MTabVQA-Mimo/" \
  --multiprocessing --num_processes 4 """

import os
import json
import argparse
from multiprocessing import Pool, cpu_count
from table_gen.table2img2 import (
    generate_sheet_image,
)  # We will create generate_sheet_image in table2img2.py
from tqdm import tqdm
import pandas as pd
from termcolor import cprint  # Add this import


def process_item(args):
    """
    Processes a single item from the MiMoTable dataset.
    Converts each sheet of the referenced XLSX spreadsheets to images.
    """
    item, mimotable_spreadsheet_dir, output_dir, index = args
    question = item["question"]
    answer = item["answer"]
    spreadsheet_list = item["spreadsheet_list"]
    table_type = item["table_type"]
    table_difficulty = item["table_difficulty"]
    meta_operation_list = item["meta_operation_list"]
    question_difficulty = item["question_difficulty"]

    qa_pairs = []
    sheet_image_ids = []
    sheet_image_paths = []
    spreadsheet_filenames = []

    for spreadsheet_rel_path in spreadsheet_list:
        spreadsheet_filename = os.path.basename(spreadsheet_rel_path.split("/")[-1])
        spreadsheet_filenames.append(spreadsheet_filename)
        spreadsheet_path = os.path.join(mimotable_spreadsheet_dir, spreadsheet_filename)
        cprint(f"Processing: {spreadsheet_path}", "cyan")  # Changed to colored print

        try:
            xls = pd.ExcelFile(spreadsheet_path)
            sheet_names = xls.sheet_names
            for sheet_name in sheet_names:
                sheet_df = pd.read_excel(xls, sheet_name=sheet_name)
                image_id, image_path = generate_sheet_image(
                    sheet_df, sheet_name, output_dir, spreadsheet_path
                )
                if image_id is not None:
                    sheet_image_ids.append(image_id)
                    sheet_image_paths.append(image_path)
        except Exception as e:
            cprint(
                f"Error processing spreadsheet: {spreadsheet_path}, error: {e}", "red"
            )  # Changed to colored print
            continue

    new_qa_pair = {
        "question": question,
        "answer": answer,
        "spreadsheet_filenames": spreadsheet_filenames,  # Store spreadsheet filenames
        "table_image_ids": sheet_image_ids,
        "sheet_image_paths": sheet_image_paths,  # Store sheet image paths
        "original_data_index": index,
        "table_type": table_type,
    }
    qa_pairs.append(new_qa_pair)

    return qa_pairs


def create_extended_mimotable_dataset(
    mimotable_json_path,
    mimotable_spreadsheet_dir,
    output_dir,
    use_multiprocessing,
    num_processes,
):
    """
    Creates an extended MiMoTable dataset with sheet images, writing results incrementally.
    """
    sheet_images_dir = os.path.join(output_dir, "table_images")
    os.makedirs(sheet_images_dir, exist_ok=True)
    extended_json_path = os.path.join(output_dir, "mimotable_extended_sheets.jsonl")

    cprint("Loading MiMoTable data...", "green")  # Changed to colored print
    mimotable_data = []
    with open(mimotable_json_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    item = json.loads(line)
                    mimotable_data.append(item)
                except json.JSONDecodeError as e:
                    cprint(
                        f"Error decoding line: {e}", "red"
                    )  # Changed to colored print
                    continue

    total_items = len(mimotable_data)
    cprint(f"Processing {total_items} items...", "yellow")  # Changed to colored print

    # Open the output file in append mode
    with open(extended_json_path, "a", encoding="utf-8") as outfile:
        if use_multiprocessing:
            with Pool(processes=num_processes) as pool:
                args_iter = [
                    (item, mimotable_spreadsheet_dir, sheet_images_dir, i)
                    for i, item in enumerate(mimotable_data)
                ]

                for result in tqdm(
                    pool.imap(process_item, args_iter),
                    total=total_items,
                    desc="Processing items (multiprocessing)",
                    unit="item",
                ):
                    for qa_pair in result:
                        outfile.write(json.dumps(qa_pair) + "\n")
        else:
            for i, item in tqdm(
                enumerate(mimotable_data), desc="Processing items", unit="item"
            ):
                result = process_item(
                    (item, mimotable_spreadsheet_dir, sheet_images_dir, i)
                )
                for qa_pair in result:
                    outfile.write(json.dumps(qa_pair) + "\n")

    cprint(
        f"Successfully processed {total_items} items and saved results incrementally to {extended_json_path}.",
        "green",
    )  # Changed to colored print


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create an extended MiMoTable dataset with sheet images."
    )
    parser.add_argument(
        "--mimotable_json_path",
        required=True,
        help="Path to the MiMoTable JSON dataset file.",
    )
    parser.add_argument(
        "--mimotable_spreadsheet_dir",
        required=True,
        help="Path to the MiMoTable spreadsheet directory.",
    )  # Added spreadsheet dir path
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

    args = parser.parse_args()

    create_extended_mimotable_dataset(
        args.mimotable_json_path,
        args.mimotable_spreadsheet_dir,
        args.output_dir,
        args.multiprocessing,
        args.num_processes,
    )
