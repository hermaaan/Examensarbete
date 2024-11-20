import requests
from google.cloud import bigquery
import os
import json
from datetime import datetime

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Herman\AppData\Roaming\gcloud\application_default_credentials.json"

# Initialize BigQuery client
client = bigquery.Client()

# Define your dataset and table details
project_id = "examensarbete-438311"
dataset_id = "examensarbete-438311.Staging"
table_id = "examensarbete-438311.Staging.crypto-api"

# Full table reference
table_ref = f"{table_id}"

# Fetch data from CoinCap API
api_url = "https://api.coincap.io/v2/assets"
response = requests.get(api_url)

if response.status_code == 200:
    raw_data = response.json()["data"][:100]  # Extract and limit to the first 100 rows
else:
    print(f"Error: {response.status_code}, {response.text}")
    exit()

# Add a timestamp field to each record
current_timestamp = datetime.utcnow().isoformat()
for record in raw_data:
    record["timestamp"] = current_timestamp

# Define job configuration for loading raw JSON data
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    write_disposition="WRITE_TRUNCATE",  # Overwrite existing data
    autodetect=True,  # Infer schema automatically
)

# Prepare the data as newline-delimited JSON in-memory
ndjson_data = "\n".join(json.dumps(record) for record in raw_data)

# Load the data directly into BigQuery
try:
    load_job = client.load_table_from_file(
        file_obj=ndjson_data.encode("utf-8"),
        destination=table_ref,
        job_config=job_config,
    )
    load_job.result()  # Wait for the job to complete
    print(f"Successfully loaded the first 100 rows with timestamps to {table_ref}")
except Exception as e:
    print(f"Error: {e}")
