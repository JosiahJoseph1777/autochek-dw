# **Dimensional Data Warehouse for Loan and Borrower Data**

## **Project Overview**
This project implements a dimensional data model for loan and borrower data to enable efficient business intelligence (BI) reporting and financial analysis. The solution integrates data from Google Sheets and Google Drive, processes it using Python, and stores it in a MySQL database.

### **Key Features**
- Dimensional modeling using a star schema.
- Automated data extraction from Google Sheets and Google Drive files.
- Transformations to ensure data consistency and integrity.
- Loading data into a MySQL database for structured reporting.
- Efficient querying for analytical insights.

---

## **Methodology**

### **1. Data Modeling**
The project uses a **star schema** for the data warehouse, consisting of:
- **Fact Table:** 
  - `loan_payment`: Contains transactional payment data.
- **Dimension Tables:**
  - `borrower_table`: Stores borrower demographic details.
  - `loan_table`: Holds loan-specific details.
  - `payment_schedule`: Tracks expected payment schedules.

### **2. ELT Process**
The Extract-Load-Transform (ELT) process follows these steps:
1. **Extract:**
   - Fetch data from Google Sheets or Google Drive (CSV/XLSX).
   - Authenticate using Google Service Account credentials.
2. **Load:**
   - Load raw data into in-memory dataframes using Pandas.
3. **Transform:**
   - Validate data types and convert date columns.
   - Handle duplicate entries with `INSERT IGNORE` to prevent redundancy.
   - Ensure relationships between fact and dimension tables are maintained.
4. **Load to MySQL:**
   - Insert data into appropriate tables with robust error handling.

### **3. Technology Stack**
- **Programming Language:** Python
- **Database:** MySQL
- **Libraries:**
  - `pandas` for data manipulation.
  - `mysql-connector-python` for database connectivity.
  - Google APIs (`google.oauth2`, `googleapiclient`) for data extraction.

---

## **Setup Instructions**

### **1. Prerequisites**
- Python 3.8+
- MySQL database installed locally or on a server.
- Google Cloud Service Account JSON credentials with access to Google Drive and Google Sheets.

### **2. Clone Repository**
```bash
git clone 
cd 


3. Install Dependencies
pip install -r requirements.txt


4. Configure Google API Credentials
Place the Google Service Account JSON file in the project directory.
Update the SERVICE_ACCOUNT_FILE path in the script.


CREATE DATABASE autochek_dw;

USE autochek_dw;

-- Borrower Table
CREATE TABLE borrower_table (
    borrower_id VARCHAR(100) PRIMARY KEY,
    state VARCHAR(100),
    city VARCHAR(100),
    zip_code VARCHAR(20),
    created_at DATETIME
);

-- Loan Table
CREATE TABLE loan_table (
    loan_id VARCHAR(100) PRIMARY KEY,
    borrower_id VARCHAR(100),
    date_of_release DATE,
    term INT,
    interest_rate DECIMAL(5, 2),
    loan_amount DECIMAL(25, 2),
    downpayment DECIMAL(25, 2),
    payment_frequency VARCHAR(50),
    maturity_date DATE,
    created_at DATETIME,
    FOREIGN KEY (borrower_id) REFERENCES borrower_table (borrower_id)
);

-- Loan Payment Table
CREATE TABLE loan_payment (
    loan_id VARCHAR(255) NOT NULL,
    payment_id VARCHAR(255) PRIMARY KEY,
    amount_paid DECIMAL(15, 4),
    date_paid DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (loan_id) REFERENCES loan_table(loan_id)
);

-- Payment Schedule Table
CREATE TABLE payment_schedule (
    schedule_id VARCHAR(100) PRIMARY KEY,
    loan_id VARCHAR(100),
    expected_payment_date DATE,
    expected_payment_amount DECIMAL(15, 2),
    created_at DATETIME,
    FOREIGN KEY (loan_id) REFERENCES loan_table (loan_id)
);


6. Run ELT Script
python elt.py  


7. Script Details
The ELT logic script performs the following functions:

--Google API Authentication:
--Connects to Google Sheets and Drive to fetch data.

Data Fetching:
Retrieves data from Google Sheets (using fetch_google_sheet).
Downloads CSV/XLSX files from Google Drive (using fetch_file_from_drive).

Data Validation:
Ensures correct data types and handles missing or invalid values.

Data Loading:
Inserts data into MySQL tables using INSERT IGNORE




