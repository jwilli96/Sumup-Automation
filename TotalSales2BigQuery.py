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

    # Extract date,
