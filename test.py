import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from dotenv import load_dotenv
import time

# Load .env
load_dotenv()

# AWS configuration
BUCKET_NAME = os.getenv("BUCKET_NAME")
REGION = os.getenv("AWS_DEFAULT_REGION")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=REGION
)

# Local Parquet file
local_file = "sample.parquet"
s3_key = f"test/{int(time.time())}_sample.parquet"

# Create sample data
data = {
    "stock": ["AAPL", "TSLA", "MSFT", "AMZN", "GOOG"],
    "price": [150.2, 700.5, 299.1, 3300.0, 2800.3],
    "volume": [1000, 500, 1200, 200, 800],
    "timestamp": [time.time() for _ in range(5)]
}

df = pd.DataFrame(data)

# Save as Parquet
table = pa.Table.from_pandas(df)
pq.write_table(table, local_file)
print(f"Parquet file saved locally: {local_file}")

# Upload to S3
try:
    s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
    print(f"Uploaded successfully to s3://{BUCKET_NAME}/{s3_key}")
except Exception as e:
    print("Error uploading to S3:", e)