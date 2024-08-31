import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import os
import json

print("Script started.")

# Read credentials from environment variable
credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')

if not credentials_json:
    print("Credentials environment variable not found. Exiting script.")
    exit(1)

# Parse credentials from JSON string
credentials_info = json.loads(credentials_json)

# Initialize BigQuery client using parsed credentials
credentials = service_account.Credentials.from_service_account_info(credentials_info)
client = bigquery.Client(credentials=credentials, project='sumup-integration')

# Store the filename (adjusted for today's date format)
csv_filename = f"TotalSales_{datetime.now().strftime('%Y%m%d')}.csv"

# Directory where the CSV file is stored
save_directory = 'data'

# Full path to the CSV file
full_path = os.path.join(save_directory, csv_filename)

# Check if directory exists
if not os.path.exists(save_directory):
    print(f"Directory '{save_directory}' does not exist. Exiting script.")
    exit(1)

# Check if CSV file exists
if not os.path.isfile(full_path):
    print(f"CSV file '{full_path}' not found. Exiting script.")
    exit(1)

# Load your CSV file into a DataFrame
df = pd.read_csv(full_path)

# Proceed with the rest of your code
# ...
