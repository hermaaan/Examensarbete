import requests
from google.cloud import bigquery
import os
import json

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\herma\AppData\Roaming\gcloud\application_default_credentials.json"

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

# Define job configuration for loading raw JSON data
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    write_disposition="WRITE_APPEND",  # Append data without overwriting
    autodetect=True,  # Infer schema automatically
)

# Save the raw data to a temporary JSON file
temp_file = "first_100_coincap_data.json"
with open(temp_file, "w") as f:
    for record in raw_data:
        f.write(json.dumps(record) + "\n")  # Each record on a new line (NDJSON format)

# Load the JSON file into BigQuery
try:
    with open(temp_file, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    print(f"Successfully loaded the first 100 rows to {table_ref}")
except Exception as e:
    print(f"Error: {e}")
