import os
import uuid
import logging
import requests
import pandas as pd
import json
import time
from datetime import datetime, timezone
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPIError

# Set up logging
log_file = 'script_output.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(message)s')

def print_and_log(message):
    print(message)
    logging.debug(message)

# Function to fetch transactions from SumUp API
def fetch_transactions(api_key, start_date, end_date):
    BASE_URL = 'https://api.sumup.com/v0.1'
    headers = {'Authorization': f'Bearer {api_key}'}
    endpoint = f'{BASE_URL}/me/transactions/history'
    params = {'from': start_date.strftime('%Y-%m-%d'), 'to': end_date.strftime('%Y-%m-%d')}
    all_transactions = []

    while True:
        response = requests.get(endpoint, headers=headers, params=params)
        if response.status_code == 200:
            transactions_response = response.json()
            if 'items' in transactions_response:
                transactions = transactions_response['items']
                all_transactions.extend(transactions)
            else:
                print_and_log("The 'items' key was not found in the response.")
                break

            next_link = next((link for link in transactions_response.get('links', []) if link['rel'] == 'next'), None)
            if next_link:
                endpoint = f"{BASE_URL}/me/transactions/history?{next_link['href']}"
                params = {}
            else:
                break
        else:
            print_and_log(f"Failed to retrieve transactions. Status code: {response.status_code}")
            print_and_log("Response: " + response.text)
            break

    return all_transactions

# Function to save transactions to a CSV file
def save_transactions_to_csv(transactions, save_directory):
    if transactions:
        start_date = datetime(2023, 12, 3, tzinfo=timezone.utc)  # Ensure dates are correct
        end_date = datetime.now(timezone.utc)

        df = pd.DataFrame(transactions)
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert('UTC')
        df = df[df['status'] == 'SUCCESSFUL']
        df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
        df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
        df['time'] = df['timestamp'].dt.strftime('%H:%M:%S')
        df['day_of_week'] = df['timestamp'].dt.strftime('%A')
        df = df[['date', 'time', 'day_of_week', 'amount']]

        os.makedirs(save_directory, exist_ok=True)
        csv_filename = f"TotalSales_{datetime.now().strftime('%Y%m%d')}.csv"
        full_path = os.path.join(save_directory, csv_filename)
        df.to_csv(full_path, index=False)

        if os.path.exists(full_path):
            print_and_log(f"Transactions exported to {full_path}")
            print_and_log(f"File size: {os.path.getsize(full_path)} bytes")
        else:
            print_and_log("CSV file was not created successfully.")
        
        return full_path
    else:
        print_and_log("No transactions found for the specified date range.")
        return None

# Function to upload CSV to BigQuery with retries
def upload_csv_to_bigquery(csv_path, credentials_json):
    credentials_info = json.loads(credentials_json)
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = bigquery.Client(credentials=credentials, project='sumup-integration')  # Replace with your project ID

    dataset_id = 'sumup-integration.TotalSales'  # Replace with your dataset ID
    table_id = 'sumup-integration.TotalSales.TotalSalesTable'      # Replace with your table ID
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("time", "TIME"),
            bigquery.SchemaField("day_of_week", "STRING"),
            bigquery.SchemaField("amount", "FLOAT64"),
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    retries = 3
    for attempt in range(retries):
        try:
            with open(csv_path, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
            job.result()  # Wait for the load job to complete
            print_and_log(f"Data loaded into BigQuery table '{table_id}'.")
            break
        except GoogleAPIError as e:
            print_and_log(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                print_and_log("Retrying...")
                time.sleep(5)  # Wait before retrying
            else:
                print_and_log("All retry attempts failed. Exiting script.")
                raise

# Function to upload file to Google Cloud Storage
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name, credentials_json):
    credentials_info = json.loads(credentials_json)
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    
    client = storage.Client(credentials=credentials, project='sumup-integration')  # Replace with your project ID
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    print_and_log(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")

# Main script execution
def main():
    api_key = os.getenv('SUMUP_API_KEY')
    if not api_key:
        print_and_log("API key is missing.")
        exit(1)

    start_date = datetime(2023, 12, 3, tzinfo=timezone.utc)
    end_date = datetime.now(timezone.utc)

    transactions = fetch_transactions(api_key, start_date, end_date)
    csv_path = save_transactions_to_csv(transactions, 'data')

    if csv_path:
        credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if not credentials_json:
            print_and_log("Credentials environment variable not found. Exiting script.")
            exit(1)
        upload_csv_to_bigquery(csv_path, credentials_json)
        upload_to_gcs('your_bucket_name', csv_path, f"data/{os.path.basename(csv_path)}", credentials_json)

if __name__ == "__main__":
    main()
