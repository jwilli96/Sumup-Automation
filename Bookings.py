import os
import logging
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# Set up logging
log_file = 'script_output.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(message)s')

def print_and_log(message):
    print(message)
    logging.debug(message)

# Function to process and clean the bookings data
def process_bookings():
    # Set up the credentials
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))

    # Authenticate and open the Google Sheet
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    client = gspread.authorize(creds)
    spreadsheet = client.open("Logs 2024")
    sheet = spreadsheet.worksheet("Bookings")

    # Define expected headers to resolve header duplication issues
    expected_headers = ['Date', 'Time', 'Adult', 'Child', 'Under 4', 'Name', 'Contact']

    # Get all the data from the 'Bookings' tab using expected headers
    data = sheet.get_all_records(expected_headers=expected_headers)

    # Convert the data to a DataFrame for easier analysis
    df = pd.DataFrame(data)

    # Convert columns
    df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%y', dayfirst=True, errors='coerce')
    df['Time'] = pd.to_datetime(df['Time'], format='%H:%M', errors='coerce').dt.time
    df['Adult'] = pd.to_numeric(df['Adult'], errors='coerce').fillna(0).astype(int)
    df['Child'] = pd.to_numeric(df['Child'], errors='coerce').fillna(0).astype(int)
    df['Under 4'] = pd.to_numeric(df['Under 4'], errors='coerce').fillna(0).astype(int)

    # Rename the 'Under 4' column to 'Under_4'
    df.rename(columns={'Under 4': 'Under_4'}, inplace=True)

    # Remove rows with invalid data
    df = df[df['Date'].notna() & df['Time'].notna() & (df['Adult'] >= 1)]

    # Clean the 'Contact' column
    df['Contact'] = df['Contact'].str.strip().str.lower().replace({
        'email': 'Email', 
        'insta': 'Insta', 
        'call': 'Call', 
        'walk in': 'Walk In', 
        'whatsapp': 'WhatsApp', 
        'in person': 'In Person',
        'phone': 'Phone'
    })

    # Remove empty or invalid contacts
    df = df[df['Contact'] != ""]

    # Remove duplicate rows based on all columns
    df.drop_duplicates(inplace=True)

    # Ensure the DataFrame only contains the specified columns
    required_columns = ['Date', 'Time', 'Adult', 'Child', 'Under_4', 'Name', 'Contact']
    df = df[required_columns]

    # Function to remove all whitespace from string values
    def remove_whitespace_from_series(series):
        if series.dtype == 'object':
            return series.apply(lambda x: ''.join(x.split()) if isinstance(x, str) else x)
        return series

    # Apply the function to each column
    df = df.apply(remove_whitespace_from_series)

    print_and_log(df)

    # Directory where you want to save the CSV file
    save_directory = os.getenv('CSV_SAVE_DIRECTORY', 'data')

    # Define the CSV filename
    csv_filename = f"Bookings_{datetime.now().strftime('%Y%m%d')}.csv"

    # Generate the full file path
    full_path = os.path.join(save_directory, csv_filename)

    # Write the DataFrame to a CSV file with proper formatting
    os.makedirs(save_directory, exist_ok=True)
    df.to_csv(full_path, index=False)

    print_and_log(f"Bookings exported to {full_path}")

    return full_path

def upload_csv_to_bigquery(csv_path):
    # Use credentials file specified in GOOGLE_APPLICATION_CREDENTIALS environment variable
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not os.path.exists(credentials_path):
        print_and_log(f"Credentials file {credentials_path} not found.")
        exit(1)

    # Create BigQuery client using the credentials file
    credentials = Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project='sumup-integration')

    dataset_id = 'Bookings'  # Your dataset ID
    table_id = 'BookingsTable'  # Only the table name
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Time", "TIME"),
            bigquery.SchemaField("Adult", "INTEGER"),
            bigquery.SchemaField("Child", "INTEGER"),
            bigquery.SchemaField("Under_4", "INTEGER"),
            bigquery.SchemaField("Name", "STRING"),
            bigquery.SchemaField("Contact", "STRING"),
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite the table data with new data
    )

    try:
        with open(csv_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        
        # Wait for the load job to complete
        job.result()

        # Log job details
        log_bigquery_job_details(job)
        
    except Exception as e:
        print_and_log(f"Failed to upload data to BigQuery: {e}")
        raise

def log_bigquery_job_details(job):
    # Convert job to API representation
    job_details = job.to_api_repr()

    # Log raw job details for debugging
    print_and_log(f"Raw job details: {job_details}")

    # Log job details
    print_and_log(f"Data loaded into BigQuery table '{job_details.get('destinationTable', {}).get('tableId', 'Unknown Table')}'.")
    print_and_log(f"Load job status: {job_details.get('status', {}).get('state', 'UNKNOWN')}")

    if 'errorResult' in job_details:
        print_and_log(f"Job failed with errors: {job_details['errorResult']}")
    
    if 'statistics' in job_details:
        statistics = job_details['statistics']
        print_and_log(f"Total rows: {statistics.get('totalRows', 'UNKNOWN')}")
        print_and_log(f"Total bytes processed: {statistics.get('totalBytesProcessed', 'UNKNOWN')}")
        print_and_log(f"Total bytes billed: {statistics.get('totalBytesBilled', 'UNKNOWN')}")
    
    if 'configuration' in job_details and 'load' in job_details['configuration']:
        load_config = job_details['configuration']['load']
        print_and_log(f"Source URIs: {load_config.get('sourceUris', 'UNKNOWN')}")

# Main script execution
def main():
    csv_path = process_bookings()

    if csv_path:
        upload_csv_to_bigquery(csv_path)
        print_last_10_csv_rows(csv_path)

def print_last_10_csv_rows(csv_path):
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        # Ensure only the relevant columns are present
        if all(col in df.columns for col in ['Date', 'Time', 'Adult', 'Child', 'Under_4', 'Name', 'Contact']):
            print_and_log(f"\nMost recent 10 rows in the CSV file {csv_path}:")
            print_and_log(df[['Date', 'Time', 'Adult', 'Child', 'Under_4', 'Name', 'Contact']].tail(10).to_string(index=False))
        else:
            print_and_log("The CSV file does not contain the required columns.")
    else:
        print_and_log(f"CSV file {csv_path} does not exist.")

if __name__ == "__main__":
    main()
