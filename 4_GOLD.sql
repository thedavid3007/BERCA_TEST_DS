USE ROLE SYSADMIN;
USE WAREHOUSE BERCA_WH;
USE DATABASE BERCA_TEST_DS;
USE SCHEMA GOLD;

--------------------------------------------------------------------------------
-- 1. Dynamic Table: Continuous Business Aggregation
--------------------------------------------------------------------------------
-- Task: Build a Dynamic Table in the Gold layer that continuously keeps
-- your business data aggregated (e.g., Sales by Region/Date).

CREATE OR REPLACE DYNAMIC TABLE AGG_DAILY_SALES_REGION
 TARGET_LAG = '1 MINUTE' -- Continuously updates with a maximum 1 minute lag
 WAREHOUSE = BERCA_WH
 AS
SELECT
    FS.ORDER_DATE_KEY AS SALES_DATE,
    DC.REGION_NAME,
    COUNT(DISTINCT FS.ORDER_KEY) AS TOTAL_ORDERS,
    SUM(FS.SALES_AMOUNT) AS TOTAL_SALES_AMOUNT,
    AVG(FS.SALES_AMOUNT) AS AVG_ORDER_VALUE
FROM
    SILVER.FACT_SALES FS
INNER JOIN
    SILVER.DIM_CUSTOMER DC ON FS.CUSTOMER_KEY = DC.CUSTOMER_KEY
GROUP BY 1, 2
ORDER BY 1, 2;

-- Dynamic Tables automatically handle the scheduling and incremental updates,
-- fulfilling the requirement for a continuously aggregated table.

--------------------------------------------------------------------------------
-- 2. Standard Gold Table (for comparison)
--------------------------------------------------------------------------------
-- A standard table with slightly different aggregation (e.g., Sales by Product Brand)

CREATE OR REPLACE TABLE AGG_BRAND_SALES (
    BRAND VARCHAR,
    TOTAL_SALES_AMOUNT NUMBER(38,2),
    TOTAL_QUANTITY NUMBER(38,0),
    LOAD_TIMESTAMP TIMESTAMP_NTZ(9)
);

-- Initial load for standard gold table
INSERT INTO AGG_BRAND_SALES
SELECT
    DP.BRAND,
    SUM(FS.SALES_AMOUNT) AS TOTAL_SALES_AMOUNT,
    SUM(FS.QUANTITY) AS TOTAL_QUANTITY,
    CURRENT_TIMESTAMP()
FROM
    SILVER.FACT_SALES FS
INNER JOIN
    SILVER.DIM_PART DP ON FS.PART_KEY = DP.PART_KEY
GROUP BY 1;


--------------------------------------------------------------------------------
-- 3. SNOWFLAKE TASK: Silver to Gold Automation (for standard table)
--------------------------------------------------------------------------------

-- Create a stream on the SILVER.FACT_SALES table to track changes
CREATE OR REPLACE STREAM SILVER.FACT_SALES_STREAM ON TABLE SILVER.FACT_SALES;

-- Create a task to automate the recalculation of the standard AGG_BRAND_SALES table
-- Note: For simplicity, this task does a full refresh of the standard table,
-- though a merge/incremental approach is better for large datasets.

CREATE OR REPLACE TASK SILVER_TO_GOLD_BRAND_SALES
  WAREHOUSE = BERCA_WH
  SCHEDULE = '1 HOUR' -- Runs hourly
  WHEN SYSTEM$STREAM_HAS_DATA('SILVER.FACT_SALES_STREAM')
AS
BEGIN
    TRUNCATE TABLE AGG_BRAND_SALES;
    INSERT INTO AGG_BRAND_SALES
    SELECT
        DP.BRAND,
        SUM(FS.SALES_AMOUNT) AS TOTAL_SALES_AMOUNT,
        SUM(FS.QUANTITY) AS TOTAL_QUANTITY,
        CURRENT_TIMESTAMP()
    FROM
        SILVER.FACT_SALES FS
    INNER JOIN
        SILVER.DIM_PART DP ON FS.PART_KEY = DP.PART_KEY
    GROUP BY 1;
END;

-- Activate the task
ALTER TASK SILVER_TO_GOLD_BRAND_SALES RESUME;

SELECT * FROM AGG_DAILY_SALES_REGION LIMIT 10;
SELECT * FROM AGG_BRAND_SALES LIMIT 10;