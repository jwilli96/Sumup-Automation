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

# Ensure the directory exists
if not os.path.exists(save_directory):
    os.makedirs(save_directory)
    print(f"Created directory '{save_directory}'.")

# Full path to the CSV file
full_path = os.path.join(save_directory, csv_filename)

# Print debug information
print(f"Checking if directory '{save_directory}' exists:")
print(f"Directory exists: {'Yes' if os.path.exists(save_directory) else 'No'}")

# Assuming the script generates the CSV file here, add debug statement to confirm
print(f"Saving CSV file to: {full_path}")

# Uncomment the following line to generate a dummy CSV for testing
# df.to_csv(full_path)

print("Directory contents:")
for item in os.listdir(save_directory):
    print(f"  {item}")

if not os.path.isfile(full_path):
    print(f"CSV file '{full_path}' not found. Exiting script.")
    exit(1)

# Load your CSV file into a DataFrame
df = pd.read_csv(full_path)
print(f"CSV file '{full_path}' loaded successfully.")
print(f"DataFrame head:")
print(df.head())

# Proceed with the rest of your code
# ...
