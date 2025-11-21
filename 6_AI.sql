USE ROLE ACCOUNTADMIN; -- Switching to High Privilege to ensure access to AI functions
USE WAREHOUSE BERCA_WH;
USE DATABASE BERCA_TEST_DS;
USE SCHEMA GOLD;

--------------------------------------------------------------------------------
-- SECTION 1: AI SENTIMENT ANALYSIS (Using Snowflake Cortex)
-- Business Goal: Identify "High Value" customers who are leaving negative feedback.
--------------------------------------------------------------------------------

-- 1. Create an AI-Enriched Table
-- We pull data from Silver/Bronze and apply the Cortex LLM function 'SENTIMENT'
-- Returns a score from -1 (Very Negative) to 1 (Very Positive).

CREATE OR REPLACE TABLE AI_CUSTOMER_SENTIMENT AS
SELECT
    C.CUSTOMER_KEY,
    C.CUSTOMER_NAME,
    C.MARKET_SEGMENT,
    C.CUSTOMER_ACCOUNT_BALANCE,
    -- We use the raw comment from Bronze to analyze sentiment
    B.C_COMMENT AS RAW_COMMENT,
    -- CALLING THE AI FUNCTION:
    SNOWFLAKE.CORTEX.SENTIMENT(B.C_COMMENT) AS SENTIMENT_SCORE
FROM
    SILVER.DIM_CUSTOMER C
JOIN
    BRONZE.CUSTOMER B ON C.CUSTOMER_KEY = B.C_CUSTKEY
WHERE
    B.C_COMMENT IS NOT NULL;

-- 2. Create a Business View for "Churn Risk"
-- This filters the AI results to find the specific customers a manager should call.

CREATE OR REPLACE VIEW V_CHURN_RISK_ALERT AS
SELECT
    CUSTOMER_NAME,
    MARKET_SEGMENT,
    CUSTOMER_ACCOUNT_BALANCE,
    SENTIMENT_SCORE,
    RAW_COMMENT,
    CASE
        WHEN SENTIMENT_SCORE < -0.1 THEN 'Urgent: Contact Customer'
        WHEN SENTIMENT_SCORE < 0.2 THEN 'Monitor'
        ELSE 'Satisfied'
    END AS ACTION_RECOMMENDATION
FROM
    AI_CUSTOMER_SENTIMENT
WHERE
    SENTIMENT_SCORE < 0 -- Filter only for negative sentiment
ORDER BY
    CUSTOMER_ACCOUNT_BALANCE DESC; -- Prioritize high-value customers

-- 3. View the Results
SELECT * FROM V_CHURN_RISK_ALERT LIMIT 10;


--------------------------------------------------------------------------------
-- SECTION 2: TIME-SERIES FORECASTING (Using Snowflake ML)
-- Business Goal: Predict the next 7 days of sales based on historical Gold data.
--------------------------------------------------------------------------------

-- 1. Prepare the training data from our Dynamic Table or Fact Table
-- We need a clean view with a TIMESTAMP and a TARGET value (Sales).
CREATE OR REPLACE VIEW V_SALES_TRAINING_DATA AS
SELECT
    TO_TIMESTAMP_NTZ(ORDER_DATE_KEY) AS SALE_DATE,
    SUM(SALES_AMOUNT) AS TOTAL_SALES
FROM
    SILVER.FACT_SALES
GROUP BY
    1
ORDER BY
    1;

-- 2. Train the Forecast Model
-- This creates a machine learning model object inside Snowflake.
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST SALES_FORECAST_MODEL(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_SALES_TRAINING_DATA'),
    TIMESTAMP_COLNAME => 'SALE_DATE',
    TARGET_COLNAME => 'TOTAL_SALES'
);

-- 3. Generate Predictions
-- Ask the model to predict the next 7 days.
CALL SALES_FORECAST_MODEL!FORECAST(FORECASTING_PERIODS => 7);

-- 4. Persist Predictions to a Table for Visualization
CREATE OR REPLACE TABLE AI_SALES_PREDICTION AS
SELECT
    TS AS FORECAST_DATE,
    FORECAST AS PREDICTED_SALES,
    LOWER_BOUND,
    UPPER_BOUND
FROM
    TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- View the Forecast
SELECT * FROM AI_SALES_PREDICTION;