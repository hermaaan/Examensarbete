from google.cloud import bigquery
import requests
import os
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Herman\AppData\Roaming\gcloud\application_default_credentials.json'


# Fetch CoinCap data
def fetch_coincap_data():
    url = "https://api.coincap.io/v2/assets"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['data']
    else:
        raise Exception(f"API request failed with status {response.status_code}")

data = fetch_coincap_data()

# Load data into BigQuery
def load_data_to_bigquery(data):
    client = bigquery.Client()
    table_id = 'examensarbete-438311.Staging.crypto-api'
    
    # Convert to rows for BigQuery insertion
    rows_to_insert = [
        {key: row[key] for key in row.keys()} for row in data
    ]
    
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Errors: {errors}")
    else:
        print("Data loaded successfully")

load_data_to_bigquery(data)
