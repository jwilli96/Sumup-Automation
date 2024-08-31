import requests
import pandas as pd
import os
from datetime import datetime, timezone

# Access API key from environment variable
api_key = os.getenv('SUMUP_API_KEY')

if api_key:
    print("API key is loaded successfully.")
else:
    print("API key is missing.")
    exit(1)

BASE_URL = 'https://api.sumup.com/v0.1'
headers = {
    'Authorization': f'Bearer {api_key}'
}

start_date = datetime(2023, 12, 3, tzinfo=timezone.utc)
end_date = datetime.now(timezone.utc)
endpoint = f'{BASE_URL}/me/transactions/history'
params = {
    'from': start_date.strftime('%Y-%m-%d'),
    'to': end_date.strftime('%Y-%m-%d')
}

all_transactions = []

while True:
    response = requests.get(endpoint, headers=headers, params=params)

    if response.status_code == 200:
        transactions_response = response.json()

        if 'items' in transactions_response:
            transactions = transactions_response['items']
            all_transactions.extend(transactions)
        else:
            print("The 'items' key was not found in the response.")
            break

        next_link = next((link for link in transactions_response.get('links', []) if link['rel'] == 'next'), None)
        if next_link:
            endpoint = f"{BASE_URL}/me/transactions/history?{next_link['href']}"
            params = {}
        else:
            break
    else:
        print(f"Failed to retrieve transactions. Status code: {response.status_code}")
        print("Response:", response.text)
        break

if all_transactions:
    df = pd.DataFrame(all_transactions)
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert('UTC')
    df = df[df['status'] == 'SUCCESSFUL']
    df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
    df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
    df['time'] = df['timestamp'].dt.strftime('%H:%M:%S')
    df['day_of_week'] = df['timestamp'].dt.strftime('%A')
    df = df[['date', 'time', 'day_of_week', 'amount']]

    save_directory = 'data'
    os.makedirs(save_directory, exist_ok=True)

    csv_filename = f"TotalSales_{datetime.now().strftime('%Y%m%d')}.csv"
    full_path = os.path.join(save_directory, csv_filename)

    print(f"Saving CSV to {full_path}")

    try:
        df.to_csv(full_path, index=False)
        print(f"Transactions exported to {full_path}")
    except Exception as e:
        print(f"Failed to write CSV file: {e}")
else:
    print("No transactions found for the specified date range.")
