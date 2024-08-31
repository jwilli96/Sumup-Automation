import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import os
import json

print("Script started.")

credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')

if not credentials_json:
    print("Credentials environment variable not found. Exiting script.")
    exit(1)

credentials_info = json.loads(credentials_json)
credentials = service_account.Credentials.from_service_account_info(credentials_info)
client = bigquery.Client(credentials=credentials, project='sumup-integration')

csv_filename = f"TotalSales_{datetime.now().strftime('%Y%m%d')}.csv"
save_directory = 'data'
full_path = os.path.join(save_directory, csv_filename)

print("Checking if directory 'data' exists:")
print(f"Directory exists: {'Yes' if os.path.isdir(save_directory) else 'No'}")

print(f"Full path to CSV file: {full_path}")

# Print the directory contents
print("Directory contents:")
for root, dirs, files in os.walk(save_directory):
    for name in files:
        print(os.path.join(root, name))

if not os.path.isfile(full_path):
    print(f"CSV file '{full_path}' not found. Exiting script.")
    exit(1)

try:
    df = pd.read_csv(full_path)
    print("CSV file loaded successfully.")
    # Continue with your BigQuery upload logic
except Exception as e:
    print(f"Failed to load CSV file: {e}")
    exit(1)
