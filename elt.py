import pandas as pd
import mysql.connector
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import io

# Google API setup
SCOPES = ['https://www.googleapis.com/auth/drive.readonly', 'https://www.googleapis.com/auth/spreadsheets.readonly']
SERVICE_ACCOUNT_FILE = 'C:/Users/HP/autochek-dw-ad6090375a89.json'

# Authenticate using service account credentials
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service_drive = build('drive', 'v3', credentials=credentials)
service_sheets = build('sheets', 'v4', credentials=credentials)

# Function to fetch data from Google Sheets
def fetch_google_sheet(sheet_id, sheet_name="Sheet1"):
    try:
        sheet = service_sheets.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id, range=f"{sheet_name}!A1:Z1000").execute()
        values = result.get('values', [])
        if not values:
            print(f"No data found in Google Sheet: {sheet_id}")
            return None
        df = pd.DataFrame(values[1:], columns=values[0])  # Use first row as headers
        print(f"Google Sheet data fetched successfully from {sheet_id}")
        return df
    except HttpError as e:
        print(f"Error fetching Google Sheets data: {e}")
        return None

# Function to fetch CSV or XLSX files from Google Drive
def fetch_file_from_drive(file_id):
    try:
        # Get file metadata
        file_metadata = service_drive.files().get(fileId=file_id).execute()
        mime_type = file_metadata.get('mimeType', '')

        # Download the file based on its type
        if mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':  # XLSX
            request = service_drive.files().get_media(fileId=file_id)
            file_stream = io.BytesIO(request.execute())
            df = pd.read_excel(file_stream)
        elif mime_type == 'text/csv':  # CSV
            request = service_drive.files().get_media(fileId=file_id)
            file_stream = io.BytesIO(request.execute())
            df = pd.read_csv(file_stream)
        else:
            print(f"Unsupported file type: {mime_type}")
            return None

        print(f"File data fetched successfully from {file_id}")
        return df

    except HttpError as e:
        print(f"Error fetching file from Google Drive: {e}")
        return None
    except Exception as e:
        print(f"Error processing file content: {e}")
        return None

# Function to create MySQL connection
def get_mysql_connection():
    try:
        connection = mysql.connector.connect(
        host="localhost",  # or your MySQL host
        user="root",  # your MySQL username
        password="Phronesis@1",  # your MySQL password
        database="autochek_dw"  # your database name
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

# Function to convert date columns to the correct format if needed
def convert_date_columns(data, date_columns):
    for column in date_columns:
        data[column] = pd.to_datetime(data[column], errors='coerce')
    return data

# Function to insert data into MySQL with INSERT IGNORE to handle duplicates
def insert_into_mysql_with_ignore(table_name, data):
    conn = get_mysql_connection()
    if conn is None:
        return
    
    cursor = conn.cursor()

    # Check if data is empty or has missing values
    if data.empty:
        print(f"Data is empty for table {table_name}. Skipping insertion.")
        return

    # Print column names and the first row of data for debugging
    print(f"Columns in the DataFrame for {table_name}: {data.columns.tolist()}")
    print(f"First row of data: {data.iloc[0].to_dict()}")

    # Convert date columns if necessary
    if table_name == "loan_table":
        date_columns = ['Date_of_release', 'Maturity_date']
        data = convert_date_columns(data, date_columns)
    
    # Define query for each table with INSERT IGNORE to handle duplicates
    if table_name == "loan_payment":
        query = """
        INSERT IGNORE INTO loan_payment (loan_id, payment_id, amount_paid, date_paid, created_at) 
        VALUES (%s, %s, %s, %s, %s)
        """
        # Swap Amount_paid and Date_paid in loan_payment data
        for index, row in data.iterrows():
            amount_paid = row['Date_paid']  # Swap: Date_paid goes into amount_paid
            date_paid = row['Amount_paid']  # Swap: Amount_paid goes into date_paid

            # Only insert valid date
            try:
                date_paid = pd.to_datetime(date_paid, errors='coerce')
                if pd.isnull(date_paid):
                    print(f"Skipping row with invalid date: {row['Amount_paid']}")
                    continue
            except Exception as e:
                print(f"Error processing date for row {row}: {e}")
                continue

            # Prepare the row for insertion with swapped values and include created_at
            created_at = pd.Timestamp.now()  # Get the current timestamp for created_at
            values = (row['loan_id(fk)'], row['payment_id(pk)'], amount_paid, date_paid, created_at)
            print(f"Row to insert: {values}")
            try:
                cursor.execute(query, values)
            except mysql.connector.Error as err:
                print(f"Error inserting data into {table_name}: {err}")
                continue

    elif table_name == "borrower_table":
        query = """
        INSERT IGNORE INTO borrower_table (borrower_id, state, city, zip_code, created_at) 
        VALUES (%s, %s, %s, %s, %s)
        """
    elif table_name == "loan_table":
        query = """
        INSERT IGNORE INTO loan_table (borrower_id, loan_id, date_of_release, term, interest_rate, loan_amount, 
        downpayment, payment_frequency, maturity_date, created_at) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
    elif table_name == "payment_schedule":
        query = """
        INSERT IGNORE INTO payment_schedule (loan_id, schedule_id, expected_payment_date, expected_payment_amount, created_at)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
    else:
        print(f"Table {table_name} is not supported.")
        return

    # Insert the remaining rows into MySQL for non-loan_payment tables
    for _, row in data.iterrows():
        values = tuple(row)  # Convert the row to a tuple for insertion
        print(f"Row to insert: {values}")
        try:
            cursor.execute(query, values)
        except mysql.connector.Error as err:
            print(f"Error inserting data into {table_name}: {err}")
            continue

    # Commit and close connection
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data inserted into {table_name} successfully.")

# Example usage for processing files
file_ids = [
    "1mfm4NUfv4wOJfMdjOIA5k99N79JFJDJeTBhzUfIYtzs",  # CSV file ID for borrower_table
    "1vyDYpJM-URFMxUj6BkgKchLO2SjdZC5F1tJ-IxzWfgQ",  # CSV file ID for loan_table
    "1sGlPiHt5lnlESDfecsipllIf9X99b3x9NEsvnWwsjlM",  # CSV file ID for payment_schedule
    "1phiQkfR90dsNlHo9XDcbJ7G8Dli7Aekumh-B2MOvSVo"  # XLSX file ID for loan_payment
]

for file_id in file_ids:
    print(f"\nProcessing file ID: {file_id}")

    # Check if the file is a Google Sheet or a Drive file (CSV/XLSX)
    try:
        file_metadata = service_drive.files().get(fileId=file_id).execute()
        mime_type = file_metadata.get('mimeType', '')

        if mime_type == 'application/vnd.google-apps.spreadsheet':  # Google Sheet
            data = fetch_google_sheet(file_id)
        elif mime_type in ['text/csv', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']:  # CSV or XLSX
            data = fetch_file_from_drive(file_id)
        else:
            print(f"Unsupported file type: {mime_type}")
            data = None

        if data is not None:
            print("Data fetched successfully:")
            print(data.head())  # Print the first few rows for verification

            # Determine the table name and insert data into MySQL
            if file_id == "1mfm4NUfv4wOJfMdjOIA5k99N79JFJDJeTBhzUfIYtzs":
                insert_into_mysql_with_ignore("borrower_table", data)
            elif file_id == "1vyDYpJM-URFMxUj6BkgKchLO2SjdZC5F1tJ-IxzWfgQ":
                insert_into_mysql_with_ignore("loan_table", data)
            elif file_id == "1sGlPiHt5lnlESDfecsipllIf9X99b3x9NEsvnWwsjlM":
                insert_into_mysql_with_ignore("payment_schedule", data)
            elif file_id == "1phiQkfR90dsNlHo9XDcbJ7G8Dli7Aekumh-B2MOvSVo":
                insert_into_mysql_with_ignore("loan_payment", data)

    except HttpError as e:
        print(f"Error fetching file from Google Drive: {e}")
