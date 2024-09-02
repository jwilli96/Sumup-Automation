import requests
import pandas as pd
import os
import json
from datetime import datetime, timezone
from google.cloud import bigquery
from google.oauth2 import service_account

# Access API key from environment variable
api_key = os.getenv('SUMUP_API_KEY')

if api_key:
    print("API key is loaded successfully.")
else:
    print("API key is missing.")
    exit(1)  # Exit if API key is missing

# Base URL for SumUp API
BASE_URL = 'https://api.sumup.com/v0.1'

# Set up headers with the API key for authorization
headers = {
    'Authorization': f'Bearer {api_key}'
}

# Define the date range from 2023-12-03 to today
start_date = datetime(2023, 12, 3, tzinfo=timezone.utc)
end_date = datetime.now(timezone.utc)

# API endpoint to get transactions within a date range
endpoint = f'{BASE_URL}/me/transactions/history'

# Parameters to filter transactions by date range
params = {
    'from': start_date.strftime('%Y-%m-%d'),
    'to': end_date.strftime('%Y-%m-%d')
}

all_transactions = []

# Fetch transactions using pagination
while True:
    response = requests.get(endpoint, headers=headers, params=params)

    if response.status_code == 200:
        transactions_response = response.json()
        
        # Access transactions under the 'items' key
        if 'items' in transactions_response:
            transactions = transactions_response['items']
            all_transactions.extend(transactions)
        else:
            print("The 'items' key was not found in the response.")
            break

        # Handle pagination
        next_link = next((link for link in transactions_response.get('links', []) if link['rel'] == 'next'), None)
        if next_link:
            endpoint = f"{BASE_URL}/me/transactions/history?{next_link['href']}"
            params = {}  # Clear params to prevent conflict with pagination
        else:
            break
    else:
        print(f"Failed to retrieve transactions. Status code: {response.status_code}")
        print("Response:", response.text)
        break

# Process transactions if found
if all_transactions:
    df = pd.DataFrame(all_transactions)

    # Convert 'timestamp' to datetime and set timezone to UTC
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert('UTC')
    
    # Filter for successful transactions
    df = df[df['status'] == 'SUCCESSFUL']

    # Filter by date range
    df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]

    # Extract date, time, and day of the week from the timestamp
    df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')  # Date in YYYY-MM-DD format
    df['time'] = df['timestamp'].dt.strftime('%H:%M:%S')  # Time in HH:MM:SS format
    df['day_of_week'] = df['timestamp'].dt.strftime('%A')  # Day of the week (e.g., Monday)

    # Select the required columns
    df = df[['date', 'time', 'day_of_week', 'amount']]
    
    # Save directory for CSV
    save_directory = 'data'
    os.makedirs(save_directory, exist_ok=True)

    # Define CSV filename
    csv_filename = f"TotalSales_{datetime.now().strftime('%Y%m%d')}.csv"
    full_path = os.path.join(save_directory, csv_filename)

    # Write DataFrame to CSV
    df.to_csv(full_path, index=False)
    
    # Check if file was successfully created
    if os.path.exists(full_path):
        print(f"Transactions exported to {full_path}")
        print(f"File size: {os.path.getsize(full_path)} bytes")
    else:
        print(f"Failed to create CSV file at {full_path}")
        exit(1)
else:
    print("No transactions found for the specified date range.")
    exit(1)  # Exit if no transactions are found

# Read Google Cloud credentials from environment variable
credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

if not credentials_json:
    print("Credentials environment variable not found. Exiting script.")
    exit(1)

# Parse credentials from JSON string
credentials_info = json.loads(credentials_json)

# Initialize BigQuery client using parsed credentials
credentials = service_account.Credentials.from_service_account_info(credentials_info)
client = bigquery.Client(credentials=credentials, project='sumup-integration')

# Load CSV data into DataFrame
df = pd.read_csv(full_path)

# Convert 'date' and 'time' columns to appropriate formats
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce').dt.date
df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S', errors='coerce').dt.time

print("Data types and column names after loading CSV:")
print(df.columns)
print(df.dtypes)
print(df.head())

# Define BigQuery table ID and schema
table_id = "sumup-integration.TotalSales.TotalSalesTable"
schema = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("time", "TIME"),
    bigquery.SchemaField("day_of_week", "STRING"),
    bigquery.SchemaField("amount", "FLOAT64"),
]

# Configure BigQuery job
job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition="WRITE_TRUNCATE"  # Use WRITE_APPEND if you want to append data
)

# Load data to BigQuery
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()  # Wait for the job to complete

# Check for errors
if job.error_result:
    print(f"Error: {job.error_result}")
else:
    print(f"Data successfully loaded to BigQuery table: {table_id}")

# Print job details
print(f"Job ID: {job.job_id}")
print(f"Job State: {job.state}")
print(f"Creation Time: {job.created}")
print(f"End Time: {job.ended}")
