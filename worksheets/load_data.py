import requests
from google.cloud import bigquery
from datetime import datetime, timezone, timedelta

#Initialize BigQuery client with project ID
project_id = "examensarbete-438311"
client = bigquery.Client(project=project_id)

table_id = "examensarbete-438311.staging.crypto-api"

api_url = "https://api.coincap.io/v2/assets"
response = requests.get(api_url)

if response.status_code != 200:
    print(f"Error: {response.status_code}, {response.text}")
    exit()

#Extract the first 100 rows
raw_data = response.json()["data"][:100]

#Add timestamp in CET/CEST
swedish_offset = timedelta(hours=1)  #CET is UTC+1
if datetime.utcnow().month in [4, 5, 6, 7, 8, 9, 10]:  #Adjusting for CEST
    swedish_offset = timedelta(hours=2)
timestamp = (datetime.utcnow() + swedish_offset).isoformat()

for record in raw_data:
    record['timestamp'] = timestamp

#Defining job config
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    write_disposition="WRITE_APPEND",  #Append table data
    autodetect=True,
)

#Load data
try:
    load_job = client.load_table_from_json(raw_data, table_id, job_config=job_config)
    load_job.result()
    print(f"Successfully loaded the first 100 rows to {table_id}")
    
except Exception as e:
    print(f"Error: {e}")
