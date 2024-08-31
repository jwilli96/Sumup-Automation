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

print(f"Checking if directory '{save_directory}' exists:")
print(f"Full path to CSV file: {full_path}")

if not os.path.exists(save_directory):
    print(f"Directory '{save_directory}' does not exist. Exiting script.")
    exit(1)

print("Directory contents:")
for root, dirs, files in os.walk(save_directory):
    for name in files:
        print(os.path.join(root, name))

if not os.path.isfile(full_path):
    print(f"CSV file '{full_path}' not found. Exiting script.")
    exit(1)

df = pd.read_csv(full_path)

# Proceed with your BigQuery upload logic
# ...
