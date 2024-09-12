import gspread
from google.auth import default
import pandas as pd
from collections import Counter
import datetime
import os  # Make sure to import os for path operations

# Define the scopes needed for Google Sheets and Drive
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Use the default credentials with explicit scopes
credentials, _ = default(scopes=scopes)

# Authenticate with gspread
client = gspread.authorize(credentials)

# Open the specific Google Sheets document by name
spreadsheet = client.open("Logs 2024")
sheet = spreadsheet.worksheet("Bookings")

# Define expected headers to resolve header duplication issues
expected_headers = ['Date', 'Time', 'Adult', 'Child', 'Under 4', 'Name', 'Contact']

# Get all the data from the 'Bookings' tab using expected headers
data = sheet.get_all_records(expected_headers=expected_headers)

# Convert the data to a DataFrame for easier analysis
df = pd.DataFrame(data)

# Convert the 'Date' column to datetime, specifying the correct format
df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%y', dayfirst=True, errors='coerce')

# Convert the 'Time' column to datetime (time format only)
df['Time'] = pd.to_datetime(df['Time'], format='%H:%M', errors='coerce').dt.time

# Convert 'Adult', 'Child', and 'Under 4' columns to numeric, setting errors='coerce'
df['Adult'] = pd.to_numeric(df['Adult'], errors='coerce').fillna(0).astype(int)
df['Child'] = pd.to_numeric(df['Child'], errors='coerce').fillna(0).astype(int)
df['Under 4'] = pd.to_numeric(df['Under 4'], errors='coerce').fillna(0).astype(int)

# Rename the 'Under 4' column to 'Under_4'
df.rename(columns={'Under 4': 'Under_4'}, inplace=True)

# Remove rows with invalid data
df = df[df['Date'].notna() & df['Time'].notna() & (df['Adult'] >= 1)]

# Clean the 'Contact' column: Remove whitespace, normalize case, and fix common typos
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

print(df)

# Directory where you want to save the CSV file
save_directory = r"C:\Users\James.Williams\Sumup\Sales"

# Define the CSV filename
csv_filename = f"Bookings_{datetime.datetime.now().strftime('%Y%m%d')}.csv"

# Generate the full file path
full_path = os.path.join(save_directory, csv_filename)

# Write the DataFrame to a CSV file with proper formatting
df.to_csv(full_path, index=False)

print(f"Bookings exported to {full_path}")
