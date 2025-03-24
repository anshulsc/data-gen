""" python mmqa_gen.py \
  --mmqa_json_path "../MMQA/three_table_with_keys.json"\
  --output_dir "../mmqa_extended/three_tables" \
  --multiprocessing --num_processes 4 """

# or

""" python mmqa_gen.py \
    --mmqa_json_path "../MMQA/three_table_with_keys.json"\
    --output_dir "../mmqa_extended/three_tables" \
    --multiprocessing --num_processes 4 --query """

import os
import json
import argparse
from multiprocessing import Pool, cpu_count
from table_gen.table2img2 import generate_table_image
from tqdm import tqdm
import ijson


def process_item(args):
    """
    Processes a single item from the MMQA dataset.
    """
    item, output_dir, table_images_dir, index = args  # Add index to args
    question = item["question"]
    answer = item["answer"]
    table_names = item["table_names"]
    tables = item["tables"]

    qa_pairs = []
    table_image_ids = []

    for i, table in enumerate(tables):
        table_name = table_names[i] if i < len(table_names) else f"table_{i}"
        image_id, image_path = generate_table_image(table, table_name, table_images_dir)
        if image_id is not None:
            table_image_ids.append(image_id)

    new_qa_pair = {
        "question": question,
        "answer": answer,
        "table_names": table_names,
        "table_image_ids": table_image_ids,
        "original_data_index": index,  # Store only index of original data
    }
    qa_pairs.append(new_qa_pair)

    return qa_pairs


# #  def create_extended_mmqa_dataset(mmqa_json_path, output_dir, use_multiprocessing, num_processes):
#     """
#     Creates an extended MMQA dataset with table images.

#     Args:
#         mmqa_json_path: Path to the original MMQA JSON dataset.
#         output_dir: Directory to save the extended dataset and images.
#         use_multiprocessing: Boolean flag to enable or disable multiprocessing.
#         num_processes: Number of processes to use if multiprocessing is enabled.
#     """
#     table_images_dir = os.path.join(output_dir, "table_images")
#     os.makedirs(table_images_dir, exist_ok=True)

#     extended_data = []

#     # Load MMQA data with a progress bar
#     print("Loading MMQA data...")
#     with open(mmqa_json_path, "r") as f:
#         mmqa_data = json.load(f)

#     total_items = len(mmqa_data)
#     print(f"Processing {total_items} items...")

#     if use_multiprocessing:
#         # Process items in parallel with progress tracking
#         with Pool(processes=num_processes) as pool:
#             # Create an iterator for the arguments
#             args_iter = [(item, output_dir, table_images_dir) for item in mmqa_data]

#             # Use imap instead of starmap for better progress bar support
#             for result in tqdm(
#                 pool.imap(process_item, args_iter),
#                 total=total_items,
#                 desc="Processing items (multiprocessing)",
#                 unit="item"
#             ):
#                 extended_data.extend(result)
#     else:
#         # Process items sequentially with progress tracking
#         for item in tqdm(mmqa_data, desc="Processing items", unit="item"):
#             result = process_item((item, output_dir, table_images_dir))
#             extended_data.extend(result)

#     # Save extended metadata with progress indication
#     extended_json_path = os.path.join(output_dir, "mmqa_extended.json")
#     print(f"\nSaving results to {extended_json_path}...")

#     if os.path.exists(extended_json_path):
#         with open(extended_json_path, "r") as f:
#             old_data = json.load(f)
#     else:
#         old_data = []

#     old_data.extend(extended_data)

#     with open(extended_json_path, "w") as f:
#         json.dump(old_data, f, indent=4)

#     print(f"Successfully processed {total_items} items and saved results.")


def create_extended_mmqa_dataset(
    mmqa_json_path, output_dir, use_multiprocessing, num_processes
):
    """
    Creates an extended MMQA dataset with table images, writing results incrementally.
    """
    table_images_dir = os.path.join(output_dir, "table_images")
    os.makedirs(table_images_dir, exist_ok=True)
    extended_json_path = os.path.join(output_dir, "mmqa_extended.jsonl")

    # Load MMQA data
    print("Loading MMQA data...")
    with open(mmqa_json_path, "r") as f:
        mmqa_data = json.load(f)

    total_items = len(mmqa_data)
    print(f"Processing {total_items} items...")

    # Open the output file in append mode
    with open(extended_json_path, "a") as outfile:
        if use_multiprocessing:
            with Pool(processes=num_processes) as pool:
                args_iter = [
                    (item, output_dir, table_images_dir, i)
                    for i, item in enumerate(mmqa_data)
                ]  # Add index

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
                enumerate(mmqa_data), desc="Processing items", unit="item"
            ):  # Add index
                result = process_item(
                    (item, output_dir, table_images_dir, i)
                )  # Add index
                for qa_pair in result:
                    # Write each qa_pair as a separate JSON object, followed by a newline
                    outfile.write(json.dumps(qa_pair) + "\n")

    print(
        f"Successfully processed {total_items} items and saved results incrementally to {extended_json_path}."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create an extended MMQA dataset with table images."
    )
    parser.add_argument(
        "--mmqa_json_path",
        required=True,
        help="Path to the original MMQA JSON dataset.",
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

    args = parser.parse_args()

    create_extended_mmqa_dataset(
        args.mmqa_json_path, args.output_dir, args.multiprocessing, args.num_processes
    )
