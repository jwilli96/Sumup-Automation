import os
import logging
import requests
import pandas as pd
import json
import time
from datetime import datetime, timezone
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from google.oauth2.service_account import Credentials

# Set up logging
log_file = 'script_output.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(message)s')

def print_and_log(message):
    print(message)
    logging.debug(message)

def log_recent_transactions(transactions, stage):
    """ Logs the most recent 20 transactions to help diagnose issues. """
    print_and_log(f"--- {stage} ---")
    if transactions:
        last_20_transactions = transactions[-20:]
        for txn in last_20_transactions:
            print_and_log(json.dumps(txn, indent=2))
    else:
        print_and_log("No transactions available.")

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

    log_recent_transactions(all_transactions, "After Fetching Transactions")
    return all_transactions

# Function to save transactions to a CSV file
def save_transactions_to_csv(transactions, save_directory):
    if transactions:
        start_date = datetime(2023, 12, 3, tzinfo=timezone.utc)
        end_date = datetime.now(timezone.utc)

        df = pd.DataFrame(transactions)
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert('UTC')
        df = df[df['status'] == 'SUCCESSFUL']
        df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]

        log_recent_transactions(df.to_dict(orient='records'), "After Filtering by Status and Date")

        # Check for duplicates by a combination of columns
        df_before_dedup = df.copy()
        df = df.drop_duplicates(subset=['id', 'timestamp', 'amount'], keep='first')

        # Log information about duplicates removed
        num_duplicates = len(df_before_dedup) - len(df)
        print_and_log(f"Number of duplicates removed: {num_duplicates}")

        df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
        df['time'] = df['timestamp'].dt.strftime('%H:%M:%S')
        df['day_of_week'] = df['timestamp'].dt.strftime('%A')
        df = df[['date', 'time', 'day_of_week', 'amount']]

        log_recent_transactions(df.to_dict(orient='records'), "After Deduplication and Formatting")

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
def upload_csv_to_bigquery(csv_path):
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not os.path.exists(credentials_path):
        print_and_log(f"Credentials file {credentials_path} not found.")
        exit(1)

    credentials = Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project='sumup-integration')

    dataset_id = 'TotalSales'
    table_id = 'TotalSalesTable'
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
                time.sleep(5)
            else:
                print_and_log("All retry attempts failed. Exiting script.")
                raise

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
        upload_csv_to_bigquery(csv_path)

if __name__ == "__main__":
    main()
