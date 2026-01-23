import pytest
from datetime import datetime
from load.load_mongo import (
    clean_dataframe,
    connect_mongo,
    get_latest_s3_key,
    load_s3_jsonl,
    BUCKET_NAME,
    df1_infoclimat_prefix
)