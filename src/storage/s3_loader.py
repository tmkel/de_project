"""
Pipeline Step 4 Scenario 2, S3

Upload localing staging parquer to s3 raw, as cloud input
"""

import os
import logging
from pathlib import Path
import boto3

BUCKET = os.environ.get("CARBON_S3_BUCKET", "carbon-intensity-lake-tl")
RAW_PREFIX = "raw"

DATASETS = [
    "national_intensity",
    "generation",
    "regional_intensity",
]

logger = logging.getLogger(__name__)

def upload_staging(dataset:str, local_dir:str|None = None) -> None:
    """
    Upload all staged parquet files under local_dir to s3://{BUCKET}/raw/{dataset}
    
    local_dir: defined in src.staging.staging, default, data/staging/xxx
    dataset: national_intensity, generation or regional_intensity
    """
    if local_dir is None:
        local_dir = f"./data/staging/{dataset}"
    
    s3 = boto3.client("s3")
    files = list(Path(local_dir).glob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"No parquet files in {local_dir}")
    
    for f in files:
        key = f"{RAW_PREFIX}/{dataset}/{f.name}"
        s3.upload_file(str(f), BUCKET, key)
        logger.info("Uploaded to %s", f"s3://{BUCKET}/{key}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=DATASETS + ["all"], default="all")
    args = parser.parse_args()

    targets = DATASETS if args.dataset == "all" else [args.dataset]

    for dataset in targets:
        upload_staging(dataset)