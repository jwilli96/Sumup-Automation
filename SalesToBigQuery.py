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

# Load your CSV file into a DataFrame
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
