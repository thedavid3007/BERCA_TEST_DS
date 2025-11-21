--------------------------------------------------------------------------------
-- 1. PRIVILEGE GRANTS (MUST BE RUN BY ACCOUNTADMIN)
--------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE DATABASE BERCA_TEST_DS;

-- Grant USAGE and CREATE privileges to SECURITYADMIN for creating policies
GRANT USAGE ON SCHEMA BERCA_TEST_DS.BRONZE TO ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA BERCA_TEST_DS.SILVER TO ROLE SECURITYADMIN;
GRANT CREATE MASKING POLICY ON SCHEMA BERCA_TEST_DS.BRONZE TO ROLE SECURITYADMIN;
GRANT CREATE MASKING POLICY ON SCHEMA BERCA_TEST_DS.SILVER TO ROLE SECURITYADMIN;

--------------------------------------------------------------------------------
-- 2. DMF IMPLEMENTATION (Data Quality Check)
--------------------------------------------------------------------------------
USE ROLE SYSADMIN;
USE SCHEMA SILVER;

-- Create the Data Metric Function (DMF) - Syntax fixed
CREATE OR REPLACE DATA METRIC FUNCTION check_customer_name_completeness(t TABLE(CUSTOMER_NAME VARCHAR))
RETURNS FLOAT
-- COMMENT 'Calculates the completeness (non-null percentage) of the CUSTOMER_NAME column.'
AS
$$
    SELECT (COUNT(CUSTOMER_NAME)::FLOAT / COUNT(*)::FLOAT) * 100 FROM t
$$;

-- We schedule this to run automatically.
ALTER TABLE DIM_CUSTOMER
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- Apply the DMF to the DIM_CUSTOMER table
ALTER TABLE DIM_CUSTOMER
ADD DATA METRIC FUNCTION BERCA_TEST_DS.SILVER.check_customer_name_completeness
ON (CUSTOMER_NAME);

-- Show the data quality check result (must be run by an ACCOUNTADMIN or similar)
SELECT check_customer_name_completeness(SELECT CUSTOMER_NAME FROM DIM_CUSTOMER);

--------------------------------------------------------------------------------
-- 3. DYNAMIC DATA MASKING IMPLEMENTATION (Security)
--------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;
USE DATABASE BERCA_TEST_DS;
USE SCHEMA BRONZE; -- Set context for policy creation

-- Role setup (safe to re-run)
CREATE OR REPLACE ROLE DATA_ANALYST; -- For Testing
CREATE OR REPLACE ROLE DATA_GOVERNANCE; -- For Testing
GRANT ROLE DATA_ANALYST TO ROLE SYSADMIN;
GRANT ROLE DATA_GOVERNANCE TO ROLE SYSADMIN;

-- Create a Masking Policy for Phone Numbers (in BERCA_TEST_DS.BRONZE)
CREATE OR REPLACE MASKING POLICY phone_mask
AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_GOVERNANCE', 'SYSADMIN') THEN val
    ELSE '***-***-****' -- Mask for Data Analysts
  END;

-- Create a Masking Policy for Customer Names (in BERCA_TEST_DS.BRONZE)
CREATE OR REPLACE MASKING POLICY name_partial_mask
AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_GOVERNANCE', 'SYSADMIN') THEN val
    ELSE SUBSTR(val, 1, 2) || '***' -- Partial Mask
  END;

-- Apply the masking policies to the BRONZE.CUSTOMER table
-- These statements now reference policies that successfully exist in the BRONZE schema.
ALTER TABLE BRONZE.CUSTOMER MODIFY COLUMN C_PHONE
SET MASKING POLICY phone_mask;

ALTER TABLE BRONZE.CUSTOMER MODIFY COLUMN C_NAME
SET MASKING POLICY name_partial_mask;


--------------------------------------------------------------------------------
-- 4. FINAL VERIFICATION
--------------------------------------------------------------------------------

USE ROLE DATA_ANALYST;
SELECT C_NAME, C_PHONE, C_ACCTBAL FROM BRONZE.CUSTOMER LIMIT 3;
-- Expected Result: C_NAME and C_PHONE should be masked.

USE ROLE SYSADMIN;
SELECT C_NAME, C_PHONE, C_ACCTBAL FROM BRONZE.CUSTOMER LIMIT 3;
-- Expected Result: All data should be unmasked.

USE ROLE ACCOUNTADMIN;
SELECT C_NAME, C_PHONE, C_ACCTBAL FROM BRONZE.CUSTOMER LIMIT 3;
-- Expected Result: All data should be unmasked.