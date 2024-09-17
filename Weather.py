import os
import logging
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from meteostat import Point, Hourly

# Coordinates for Brighton, UK
LAT = 50.8225
LON = -0.1372

# Set up logging
log_file = 'script_output.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(message)s')

def print_and_log(message):
    print(message)
    logging.debug(message)

def fetch_weather_data(lat, lon, start_date, end_date):
    """Fetch historical weather data for a specific date and location."""
    location = Point(lat, lon)
    weather_data = Hourly(location, start=start_date, end=end_date)
    weather_data = weather_data.fetch()
    return weather_data

def filter_weather_data(data, start_hour=9, end_hour=16, weekdays=None):
    """Filter the weather data between specific hours and weekdays."""
    filtered_data = []
    if weekdays is None:
        weekdays = {0, 1, 2, 3, 4, 5, 6}  # Default to all days

    for index, row in data.iterrows():
        local_time = index
        if start_hour <= local_time.hour < end_hour and local_time.weekday() in weekdays:
            filtered_data.append({
                "time": local_time.strftime("%Y-%m-%d %H:%M:%S"),
                "temperature": row.get("temp", "N/A"),
                "rain": row.get("prcp", 0),
                "wind_speed": row.get("wspd", "N/A")
            })
    
    return filtered_data

def get_weather_data(start_date, end_date):
    """Get weather data between specific hours and days."""
    weather_data = fetch_weather_data(LAT, LON, start_date, end_date)
    weekdays = {0, 2, 3, 4, 5, 6}
    filtered_data = filter_weather_data(weather_data, weekdays=weekdays)
    return filtered_data

def save_to_csv(data, file_path):
    """Save the data to a CSV file."""
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    print_and_log(f"Data saved to: {file_path}")

def upload_csv_to_bigquery(csv_path):
    """Upload CSV file to Google BigQuery."""
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not os.path.exists(credentials_path):
        print_and_log(f"Credentials file {credentials_path} not found.")
        exit(1)

    credentials = Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project='sumup-integration')

    dataset_id = 'Weather'  # Your dataset ID
    table_id = 'WeatherTable'  # Only the table name
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("time", "TIMESTAMP"),
            bigquery.SchemaField("temperature", "FLOAT64"),
            bigquery.SchemaField("rain", "FLOAT64"),
            bigquery.SchemaField("wind_speed", "FLOAT64"),
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    try:
        with open(csv_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        
        job.result()
        log_bigquery_job_details(job)
        
    except Exception as e:
        print_and_log(f"Failed to upload data to BigQuery: {e}")
        raise

def log_bigquery_job_details(job):
    """Log details of the BigQuery job."""
    job_details = job.to_api_repr()
    print_and_log(f"Raw job details: {job_details}")
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

if __name__ == "__main__":
    start_date = datetime(2023, 12, 3, 0, 0, 0)
    end_date = datetime.now()
    
    print(f"Fetching data for: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    weather_data = get_weather_data(start_date, end_date)
    
    csv_file_path = r'C:\Users\James.Williams\Sumup\Sales\weather_data.csv'
    save_to_csv(weather_data, csv_file_path)

    upload_csv_to_bigquery(csv_file_path)
