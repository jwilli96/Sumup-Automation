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

print("Checking if directory 'data' exists:")
print(f"Directory exists: {'Yes' if os.path.isdir(save_directory) else 'No'}")

print(f"Full path to CSV file: {full_path}")

# Print detailed directory contents
print("Directory contents:")
for root, dirs, files in os.walk(save_directory):
    for name in files:
        file_path = os.path.join(root, name)
        print(f"File found: {file_path}")

# Check if file exists and check file permissions
if not os.path.isfile(full_path):
    print(f"CSV file '{full_path}' not found. Exiting script.")
    exit(1)

print(f"Checking file permissions for '{full_path}':")
print(f"Readable: {'Yes' if os.access(full_path, os.R_OK) else 'No'}")
print(f"Writable: {'Yes' if os.access(full_path, os.W_OK) else 'No'}")
print(f"Executable: {'Yes' if os.access(full_path, os.X_OK) else 'No'}")

# Attempt to load the CSV file
try:
    df = pd.read_csv(full_path)
    print("CSV file loaded successfully.")
    # Proceed with the rest of your code
    # ...
except Exception as e:
    print(f"Failed to load CSV file: {e}")
    exit(1)
