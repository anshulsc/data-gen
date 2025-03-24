from termcolor import cprint
from datasets import Dataset, DatasetDict, load_dataset
from huggingface_hub import create_repo, upload_folder
from huggingface_hub import HfApi

repo_name = "anshulsc/MTabVQA-atis"
local_dataset_path = "data/MTabVQA-atis/"

create_repo(repo_name, repo_type="dataset", exist_ok=True)

api = HfApi()

api.upload_large_folder(
    folder_path=local_dataset_path,
    repo_id=repo_name,
    repo_type="dataset",
)

cprint("Dataset uploaded successfully!")
