import requests
import pandas as pd
import os
import json  # Missing import for parsing credentials
from datetime import datetime, timezone
from google.cloud import bigquery
from google.oauth2 import service_account
import sys  # For proper script termination

# Access API key from environment variable
api_key = os.getenv('SUMUP_API_KEY')

if api_key:
    print("API key is loaded successfully.")
else:
    print("API key is missing.")
    sys.exit(1)  # Exit if API key is missing

# Base URL for SumUp API
BASE_URL = 'https://api.sumup.com/v0.1'

# Set up headers with your API key for authorization
headers = {
    'Authorization': f'Bearer {api_key}'
}

# Define the date range from 2023-12-03 to today
start_date = datetime(2023, 12, 3, tzinfo=timezone.utc)
end_date = datetime.now(timezone.utc)

# Example API endpoint to get transactions within a date range
endpoint = f'{BASE_URL}/me/transactions/history'

# Parameters to filter transactions by date range
params = {
    'from': start_date.strftime('%Y-%m-%d'),
    'to': end_date.strftime('%Y-%m-%d')
}

all_transactions = []

while True:
    # Make a GET request to the SumUp API
    response = requests.get(endpoint, headers=headers, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        transactions_response = response.json()

        # Access the transactions data under the 'items' key
        if 'items' in transactions_response:
            transactions = transactions_response['items']
            all_transactions.extend(transactions)
        else:
            print("The 'items' key was not found in the response.")
            break

        # Check if there is a 'next' link for pagination
        next_link = next((link for link in transactions_response.get('links', []) if link['rel'] == 'next'), None)
        if next_link:
            # Update endpoint with the next link's href
            endpoint = f"{BASE_URL}/me/transactions/history?{next_link['href']}"
            # Clear params to prevent conflict with the 'next' link query
            params = {}
        else:
            # No more pages to fetch
            break
    else:
        print(f"Failed to retrieve transactions. Status code: {response.status_code}")
        print("Response:", response.text)
        break

# Proceed only if transactions are found
if all_transactions:
    df = pd.DataFrame(all_transactions)

    # Convert 'timestamp' column to datetime and set timezone to UTC
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert('UTC')
    
    # Filter transactions by successful status
    df = df[df['status'] == 'SUCCESSFUL']

    # Filter transactions to ensure they fall within the correct date range
    df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]

    # Extract date, time, and day of the week from the timestamp
    df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')  # Date in YYYY-MM-DD format
    df['time'] = df['timestamp'].dt.strftime('%H:%M:%S')  # Time in HH:MM:SS format
    df['day_of_week'] = df['timestamp'].dt.strftime('%A')  # Day of the week (e.g., Monday)
    
    # Select the required columns and rename them
    df = df[['date', 'time', 'day_of_week', 'amount']]
    
    # Directory where you want to save the CSV file
    save_directory = 'data'

    # Create the directory if it does not exist
    os.makedirs(save_directory, exist_ok=True)
    
    csv_filename = f"TotalSales_{datetime.now().strftime('%Y%m%d')}.csv"

    # Generate the full file path
    full_path = os.path.join(save_directory, csv_filename)

    # Write the DataFrame to a CSV file with proper formatting
    df.to_csv(full_path, index=False)
   
    # Debugging information
    print(f"Transactions exported to {full_path}")
    print(f"File size: {os.path.getsize(full_path)} bytes")

    # Read credentials from environment variable
    credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')

    if not credentials_json:
        print("Credentials environment variable not found. Exiting script.")
        sys.exit(1)

    # Parse credentials from JSON string
    credentials_info = json.loads(credentials_json)

    try:
        # Initialize BigQuery client using parsed credentials
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        client = bigquery.Client(credentials=credentials, project='sumup-integration')

        # Load your CSV file into a DataFrame (if needed)
        df = pd.read_csv(full_path)

        # Convert 'date' to datetime format and extract date only
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce').dt.date

        # Convert 'time' to datetime format and extract time only
        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S', errors='coerce').dt.time

        # Print to verify data types and column names
        print(df.columns)
        print(df.dtypes)
        print(df.head())

        # Define BigQuery table ID
        table_id = "sumup-integration.TotalSales.TotalSalesTable"

        # Define schema explicitly
        schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("time", "TIME"),
            bigquery.SchemaField("day_of_week", "STRING"),
            bigquery.SchemaField("amount", "FLOAT64"),
        ]

        # Configure the job to truncate the table (or append if necessary)
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",  # Use WRITE_APPEND if you want to append data
        )

        # Load DataFrame to BigQuery with schema definition
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete

        # Check for errors
        if job.error_result:
            print(f"Error: {job.error_result}")

        # Print job details
        print(f"Job ID: {job.job_id}")
        print(f"Job State: {job.state}")
        print(f"Creation Time: {job.created}")
        print(f"End Time: {job.ended}")

    except Exception as e:
        print(f"Error occurred: {e}")
else:
    print("No transactions found for the specified date range.")
