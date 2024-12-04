
--Expected steps:
--1. Go through the above schemas, try to understand the relationship between the schemas

-- Borrower Table (Dimension)
CREATE TABLE autochek_dw.borrower_table (
    borrower_id VARCHAR(100) PRIMARY KEY,  
    state VARCHAR(100),
    city VARCHAR(100),
    zip_code VARCHAR(20),
    created_at DATETIME
);


-- Loan Table (Dimension)
CREATE TABLE autochek_dw.loan_table (
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

-- Loan Payment Table (Fact)
CREATE TABLE autochek_dw.loan_payment (
    loan_id VARCHAR(255) NOT NULL,        
    payment_id VARCHAR(255) PRIMARY KEY,  
    amount_paid DECIMAL(15, 4),          
    date_paid DATE,                       
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
    FOREIGN KEY (loan_id) REFERENCES loan_table(loan_id)  
);

---------Payment Schedule (Dimension)
CREATE TABLE autochek_dw.payment_schedule (
    schedule_id VARCHAR(100) PRIMARY KEY,
    loan_id VARCHAR(100),
    expected_payment_date DATE,
    expected_payment_amount DECIMAL(15, 2),
    created_at DATETIME,
    FOREIGN KEY (loan_id) REFERENCES loan_table (loan_id)
);



--Step2 Implement an ELT Pipeline that will be used for loading the data into the source area and transforming it into required Dimensional models.
-- Refer to elt.py file for ELT solution

--Step 3 Refer to image on project path ER_Daigram for image representation of the data model

--Star Schema Structure
--The design follows a Star Schema, a common dimensional modeling approach, which simplifies querying and reporting. It consists of:

--Fact Table: Contains transactional data used for quantitative analysis.
--Dimension Tables: Store descriptive attributes related to the facts, providing context.


--Tables and Their Roles
--A. Fact Table: loan_payment
--Purpose: Tracks transactional data related to loan payments.
    ----Key Attributes:
    --1. payment_id (PK): Uniquely identifies each payment.
    --2. loan_id (FK): Links to the specific loan being paid.
    --3. amount_paid: Captures the payment amount.
    --4. date_paid: Helps in tracking payment trends over time.
    --5. created_at: Records the timestamp for ETL purposes.
    --Relationships: Connected to the loan_table via loan_id.
    
--B. Dimension Tables: borrower_table
--Purpose: Provides contextual information about borrowers.
    ----Key Attributes:
    --1. borrower_id (PK): Unique identifier for each borrower.
    --2. state, city, zip_code: Geographic attributes for regional analysis.
    --3. created_at: Tracks when the borrower record was created.
--Usage: Helps analyze loans or payments by borrower demographics or location.
    
--C. Dimension Tables: loan_table
--Purpose: Stores details of each loan.
    ----Key Attributes:
    --1. loan_id (PK): Unique identifier for loans.
    --2. borrower_id (FK): Links to the borrower who took the loan.
    --3. Loan-specific attributes like date_of_release, term, interest_rate, loan_amount, etc.
--Usage: Acts as a bridge between borrower_table and loan_payment for understanding payment behavior, overdue loans, etc.
    
--D. Dimension Tables: payment_schedule
--Purpose: Maintains the expected payment schedule for each loan.
    ----Key Attributes:
    --1. schedule_id (PK): Unique identifier for the schedule.
    --2. loan_id (FK): Links to the associated loan.
    --3. expected_payment_date: Helps compare planned vs. actual payments.
    --4. expected_payment_amount: Enables gap analysis between expected and actual payments.
    --5. created_at: Tracks ETL operations.
--Usage: Supports PAR (Portfolio At Risk) analysis and payment behavior tracking.

--3. Relationships Between Tables
--The design incorporates foreign key (FK) relationships to enforce data integrity and establish a clear hierarchy:

--borrower_table (Dimension) → loan_table (Dimension):

--Relationship: A borrower can have multiple loans.
--borrower_id is a foreign key in loan_table.
--loan_table (Dimension) → loan_payment (Fact):

--Relationship: Each loan can have multiple payments.
--loan_id is a foreign key in loan_payment.
--loan_table (Dimension) → payment_schedule (Dimension):

--Relationship: Each loan can have multiple payment schedules.
--loan_id is a foreign key in payment_schedule.

--4. Why This Design?
    --Ease of Reporting
    --Enables analysis of borrower demographics (e.g., loan distributions by city or state).
    --Facilitates loan performance tracking, including overdue loans and actual payments vs. schedules.

--5. Support for BI Queries
    --Time-based trends like monthly loan payments or overdue amounts.
    --Customer-centric analysis like identifying top-paying borrowers.
    --Loan-centric insights such as loans nearing maturity.
    --Efficient Data Retrieval
    --The star schema ensures efficient querying by minimizing joins.
    --Dimensions is designed to support drill-downs and aggregations.


-- Step 4
/*  Problem Statetment 2 Question 4
Using the dimensional tables above, write queries to Calculate PAR Days - Par Days means the number of days the loan was not paid 
in full. Eg If the loan repayment was due on the 10th Feb 2022 and payment was made on the 15th Feb 2022 the par days would be 5 days
(NOTE: For each day a customer missed a payment the amount_at_risk is the total amount of money we are expecting from the customer as at that time, for instance, if
the customer owes for 5000 from month 1 and 10000 for current month the amount_at_risk will be the total amount 5000 + 10000 = 15000)
*/

---------Implementing the solution with a CTE
WITH LoanDetails AS (
    SELECT 
        lt.loan_id,
        lt.loan_amount,
        ps.schedule_id,
        ps.expected_payment_date,
        ps.expected_payment_amount,
        lp.payment_id,
        lp.date_paid,
        lp.amount_paid,
        DATEDIFF(lp.date_paid, ps.expected_payment_date) AS par_days  
    FROM 
        autochek_dw.loan_table lt
    JOIN 
        autochek_dw.payment_schedule ps ON lt.loan_id = ps.loan_id  
    LEFT JOIN 
        autochek_dw.loan_payment lp ON lt.loan_id = lp.loan_id  
    WHERE

        (lp.date_paid IS NOT NULL AND lp.date_paid > ps.expected_payment_date) OR
        (lp.date_paid IS NULL) 
),
AmountAtRisk AS (

    SELECT 
        ld.loan_id,
        SUM(ld.expected_payment_amount) AS total_amount_at_risk  
    FROM 
        LoanDetails ld
    WHERE 

        ld.par_days > 0  
    GROUP BY 
        ld.loan_id
)

SELECT 
    ld.loan_id,
    ld.par_days,
    aar.total_amount_at_risk
FROM 
    LoanDetails ld
JOIN 
    AmountAtRisk aar ON ld.loan_id = aar.loan_id  
ORDER BY 
    ld.loan_id, ld.par_days;