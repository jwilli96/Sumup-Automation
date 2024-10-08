import os
import logging
import requests
import pandas as pd
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
                print_and_log(f"Fetched {len(transactions)} transactions.")

            else:
                print_and_log("The 'items' key was not found in the response.")
                break

            next_link = next((link for link in transactions_response.get('links', []) if link['rel'] == 'next'), None)
            if next_link:
                # Properly construct the full URL for the next request
                endpoint = f"{BASE_URL}/me/transactions/history?{next_link['href']}"
                params = {}  # Clear params to avoid duplication in the URL
            else:
                break
        else:
            print_and_log(f"Failed to retrieve transactions. Status code: {response.status_code}")
            print_and_log("Response: " + response.text)
            break

    print_and_log(f"Total transactions fetched: {len(all_transactions)}")
    return all_transactions

# Function to save transactions to a CSV file
def save_transactions_to_csv(transactions, save_directory):
    if transactions:
        start_date = datetime(2023, 12, 3, tzinfo=timezone.utc)  # Ensure dates are correct
        end_date = datetime.now(timezone.utc)

        df = pd.DataFrame(transactions)
        
        # Convert timestamp to datetime and then to BST if it's timezone-aware
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)  # Remove any existing timezone info
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC').dt.tz_convert('Europe/London')
        
        # Filter transactions based on amount and time
        df = df[df['status'] == 'SUCCESSFUL']
        df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
        
        # Drop duplicates
        df.drop_duplicates(inplace=True)
        
        # Extract date and time details
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

def print_last_10_csv_rows(csv_path):
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        # Ensure only the relevant columns are present
        if all(col in df.columns for col in ['date', 'time', 'day_of_week', 'amount']):
            print_and_log(f"\nMost recent 10 rows in the CSV file {csv_path}:")
            print_and_log(df[['date', 'time', 'day_of_week', 'amount']].tail(10).to_string(index=False))
        else:
            print_and_log("The CSV file does not contain the required columns.")
    else:
        print_and_log(f"CSV file {csv_path} does not exist.")

# Function to upload CSV to BigQuery and log job details
def upload_csv_to_bigquery(csv_path):
    # Use credentials file specified in GOOGLE_APPLICATION_CREDENTIALS environment variable
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not os.path.exists(credentials_path):
        print_and_log(f"Credentials file {credentials_path} not found.")
        exit(1)

    # Create BigQuery client using the credentials file
    credentials = Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project='sumup-integration')

    dataset_id = 'TotalSales'  # Your dataset ID
    table_id = 'TotalSalesTable'  # Only the table name
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
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite the table data with new data
    )

    try:
        with open(csv_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        
        # Wait for the load job to complete
        job.result()

        # Log job details
        log_bigquery_job_details(job)
        
    except GoogleAPIError as e:
        print_and_log(f"Failed to upload data to BigQuery: {e}")
        raise

def log_bigquery_job_details(job):
    # Convert job to API representation
    job_details = job.to_api_repr()

    # Log raw job details for debugging
    print_and_log(f"Raw job details: {job_details}")

    # Log job details
    print_and_log(f"Data loaded into BigQuery table '{job_details.get('destinationTable', {}).get('tableId', 'Unknown Table')}'.")
    print_and_log(f"Load job status: {job_details.get('status', {}).get('state', 'UNKNOWN')}")

    if 'errorResult' in job_details:
        print_and_log(f"Job failed with errors: {job_details['errorResult']}")
    
    if 'statistics' in job_details:
        statistics = job_details['statistics']
        print_and_log(f"Total rows: {statistics.get('totalRows', 'UNKNOWN')}")
        print_and_log(f"Total bytes processed: {statistics.get('totalBytesProcessed', 'UNKNOWN')}")
        print_and_log(f"Total bytes billed: {statistics.get('totalBytesBilled', 'UNKNOWN')}")
    
    if 'configuration' in job_details and 'load' in job_details['configuration']:
        load_config = job_details['configuration']['load']
        print_and_log(f"Source URIs: {load_config.get('sourceUris', 'UNKNOWN')}")

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
        print_last_10_csv_rows(csv_path)

if __name__ == "__main__":
    main()
