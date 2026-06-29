"""
Pipeline Step: S3 raw (s3_loader.py)
- Upload local staged parquet files to s3 raw, as cloud input for Glue PySpark jobs

Ref: Overall data flow

                     UK Carbon Intensity API
                            |
                     Ingestion (Python + requests)
                            |
                     Validation (Pydantic)
                            |
                     Staging (JSON → Parquet)
                      /                    \
        Postgres raw load            S3 raw  (s3_loader)
              |                              |
    dbt  (stg → core → marts)        Glue PySpark jobs
              |                              |
      Postgres marts                 S3 curated  →  (Iceberg → Athena · planned)
"""

import os
import logging
from pathlib import Path
import boto3

from src import config

BUCKET = os.environ.get("CARBON_S3_BUCKET", "carbon-intensity-lake")
RAW_PREFIX = "raw"

DATASETS = config.DATASETS

logger = logging.getLogger(__name__)

def upload_staging(dataset:str, local_dir:str|None = None) -> None:
    """
    Upload all staged parquet files under local_dir to s3://{BUCKET}/raw/{dataset}
    
    local_dir: defined in src/staging/staging.py, default to "./data/staging/{dataset}"
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

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=DATASETS + ["all"], default="all")
    args = parser.parse_args()

    targets = DATASETS if args.dataset == "all" else [args.dataset]

    for dataset in targets:
        upload_staging(dataset)