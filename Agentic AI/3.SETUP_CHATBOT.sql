USE ROLE SYSADMIN;
USE WAREHOUSE BERCA_WH;
USE DATABASE BERCA_TEST_DS;
USE SCHEMA GOLD;

CREATE TABLE IF NOT EXISTS CHAT_HISTORY (
    ID INTEGER AUTOINCREMENT,
    ROLE STRING,
    MESSAGE STRING,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

SELECT $1
FROM @BERCA_TEST_DS.GOLD.AGENT_STAGE/BERCA_TEST_DS_SEMANTIC_MODEL.yaml;

CREATE OR REPLACE PROCEDURE GENERATE_AND_EXECUTE_SQL(USER_QUESTION VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas') 
HANDLER = 'generate_sql_and_answer'
AS
$$
import json
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit

def get_semantic_model_context():
    return """
    name: BERCA_TEST_DS_SEMANTIC_MODEL
description: This semantic model connects all data in BERCA_TEST_DS DB
tables:
  - name: CUSTOMER
    description: This table stores information about customers, including their unique identifier, name, address, phone number, account balance, market segment, and any additional comments. The data is partially masked for security and privacy purposes, specifically the name and phone number fields.
    base_table:
      database: BERCA_TEST_DS
      schema: BRONZE
      table: CUSTOMER
    dimensions:
      - name: C_ADDRESS
        description: The physical location of the customer.
        expr: C_ADDRESS
        data_type: VARCHAR
        sample_values:
          - Address 0
          - Address 119
          - Address 199
      - name: C_COMMENT
        description: Customer comments or feedback regarding their experience with the company.
        expr: C_COMMENT
        data_type: VARCHAR
        sample_values:
          - Customer services was terrible. The resolution took 3 weeks and I am very unhappy.
          - Just an average transaction. Nothing special to report.
          - Product arrived damaged. This is unacceptable and I need a full refund immediately.
      - name: C_CUSTKEY
        description: Unique identifier for a customer in the database.
        expr: C_CUSTKEY
        data_type: NUMBER
        sample_values:
          - '98'
          - '29'
          - '1'
      - name: C_MKTSEGMENT
        description: Market segment to which the customer belongs, indicating the type of business or industry they operate in.
        expr: C_MKTSEGMENT
        data_type: VARCHAR
        sample_values:
          - FURNITURE
          - AUTOMOBILE
          - BUILDING
      - name: C_NAME
        description: The name of the customer, used to identify and distinguish between individual customers.
        expr: C_NAME
        data_type: VARCHAR
        sample_values:
          - Customer 1
          - Customer 6
          - Customer 10
      - name: C_NATIONKEY
        description: The nation key of the customer's nation.
        expr: C_NATIONKEY
        data_type: NUMBER
        sample_values:
          - '11'
          - '7'
          - '18'
      - name: C_PHONE
        description: The customer's phone number.
        expr: C_PHONE
        data_type: VARCHAR
        sample_values:
          - 111-222-0000
          - 111-222-0002
          - 111-222-0250
    facts:
      - name: C_ACCTBAL
        description: The current account balance of the customer.
        expr: C_ACCTBAL
        data_type: NUMBER
        sample_values:
          - '3505.00'
          - '3041.00'
          - '6294.00'
    primary_key:
      columns:
        - C_CUSTKEY
  - name: DIM_CUSTOMER
    description: This table stores customer information, including demographic data and account details, to support business intelligence and analytics. It captures key attributes such as customer name, address, phone number, account balance, market segment, and geographic location, enabling analysis and reporting on customer behavior and trends.
    base_table:
      database: BERCA_TEST_DS
      schema: SILVER
      table: DIM_CUSTOMER
    dimensions:
      - name: CUSTOMER_ADDRESS
        description: The physical location where the customer resides or receives mail and other communications.
        expr: CUSTOMER_ADDRESS
        data_type: VARCHAR
        sample_values:
          - Address 0
          - Address 119
          - Address 199
      - name: CUSTOMER_KEY
        description: Unique identifier for a customer in the database, used to link customer data across different tables and systems.
        expr: CUSTOMER_KEY
        data_type: NUMBER
        sample_values:
          - '98'
          - '29'
          - '1'
      - name: CUSTOMER_NAME
        description: The name of the customer, used to identify and distinguish between individual customers.
        expr: CUSTOMER_NAME
        data_type: VARCHAR
        sample_values:
          - Customer 67
          - Customer 78
          - Customer 232
      - name: CUSTOMER_PHONE
        description: The phone number associated with a customer.
        expr: CUSTOMER_PHONE
        data_type: VARCHAR
        sample_values:
          - 111-222-0028
          - 111-222-0096
          - 111-222-0000
      - name: MARKET_SEGMENT
        description: The market segment to which the customer belongs, categorizing them based on the type of products or services they are interested in, such as furniture, automobiles, or building materials.
        expr: MARKET_SEGMENT
        data_type: VARCHAR
        sample_values:
          - FURNITURE
          - AUTOMOBILE
          - BUILDING
      - name: NATION_NAME
        description: The country of origin or nationality of the customer.
        expr: NATION_NAME
        data_type: VARCHAR
        sample_values:
          - ROMANIA
          - MOZAMBIQUE
          - CHINA
      - name: REGION_NAME
        description: Geographic region where the customer is located.
        expr: REGION_NAME
        data_type: VARCHAR
        sample_values:
          - MIDDLE EAST
          - EUROPE
          - AMERICA
    facts:
      - name: CUSTOMER_ACCOUNT_BALANCE
        description: The current balance of the customer's account, representing the total amount of money the customer owes or is owed by the company.
        expr: CUSTOMER_ACCOUNT_BALANCE
        data_type: NUMBER
        sample_values:
          - '3505.00'
          - '3041.00'
          - '6294.00'
    primary_key:
      columns:
        - CUSTOMER_KEY
  - name: DIM_PART
    description: This table stores information about various parts, including their unique identifier, name, manufacturer, brand, type, size, and retail price, providing a centralized repository for part details.
    base_table:
      database: BERCA_TEST_DS
      schema: SILVER
      table: DIM_PART
    dimensions:
      - name: BRAND
        description: The brand or manufacturer of the part.
        expr: BRAND
        data_type: VARCHAR
        sample_values:
          - Brand 2
          - Brand 0
          - Brand 1
      - name: MANUFACTURER
        description: The name of the manufacturer that produced the part.
        expr: MANUFACTURER
        data_type: VARCHAR
        sample_values:
          - Manufacturer 2
          - Manufacturer 1
          - Manufacturer 0
      - name: PART_KEY
        description: Unique identifier for a part in the inventory.
        expr: PART_KEY
        data_type: NUMBER
        sample_values:
          - '98'
          - '29'
          - '1'
      - name: PART_NAME
        description: The name of a specific part in the inventory.
        expr: PART_NAME
        data_type: VARCHAR
        sample_values:
          - Part 19
          - Part 18
          - Part 72
      - name: PART_SIZE
        description: The size of the part, measured in inches, which is a characteristic used to categorize and analyze parts of varying dimensions.
        expr: PART_SIZE
        data_type: NUMBER
        sample_values:
          - '24'
          - '23'
          - '30'
      - name: PART_TYPE
        description: 'Categorization of parts based on their functional or structural characteristics, with three distinct types: Type 0, Type 1, and Type 2.'
        expr: PART_TYPE
        data_type: VARCHAR
        sample_values:
          - Type 0
          - Type 1
          - Type 2
    facts:
      - name: RETAIL_PRICE
        description: The retail price of a part, representing the manufacturer's suggested selling price to the end customer.
        expr: RETAIL_PRICE
        data_type: NUMBER
        sample_values:
          - '621.00'
          - '451.00'
          - '1462.00'
    primary_key:
      columns:
        - PART_KEY
  - name: FACT_SALES
    description: This table stores sales transaction data, capturing key information about each order, including the customer, part, and supplier involved, as well as the order and ship dates, quantities, prices, discounts, taxes, and net sales amounts, with a timestamp for when the data was loaded.
    base_table:
      database: BERCA_TEST_DS
      schema: SILVER
      table: FACT_SALES
    dimensions:
      - name: CUSTOMER_KEY
        description: Unique identifier for a customer in the sales data.
        expr: CUSTOMER_KEY
        data_type: NUMBER
        sample_values:
          - '8971'
          - '8125'
          - '11994'
      - name: ORDER_KEY
        description: Unique identifier for each sales order.
        expr: ORDER_KEY
        data_type: NUMBER
        sample_values:
          - '98'
          - '29'
          - '1'
      - name: PART_KEY
        description: Unique identifier for a part or product sold.
        expr: PART_KEY
        data_type: NUMBER
        sample_values:
          - '14163'
          - '18378'
          - '2184'
      - name: QUANTITY
        description: The quantity of items sold in a transaction.
        expr: QUANTITY
        data_type: NUMBER
        sample_values:
          - '23'
          - '45'
          - '8'
      - name: RETURN_FLAG
        description: 'Indicates whether a sale was returned, with values representing: A (All, i.e. the sale was partially returned), N (No, i.e. the sale was not returned), and R (Yes, i.e. the sale was fully returned).'
        expr: RETURN_FLAG
        data_type: VARCHAR
        sample_values:
          - A
          - 'N'
          - R
      - name: SUPPLIER_KEY
        description: Unique identifier for the supplier of the product sold.
        expr: SUPPLIER_KEY
        data_type: NUMBER
        sample_values:
          - '127'
          - '653'
          - '105'
    time_dimensions:
      - name: LOAD_TIMESTAMP
        description: The date and time when the sales data was loaded into the system.
        expr: LOAD_TIMESTAMP
        data_type: TIMESTAMP_NTZ
        sample_values:
          - 2025-11-20T23:42:32.192+0000
      - name: ORDER_DATE_KEY
        description: The date on which the sales order was placed.
        expr: ORDER_DATE_KEY
        data_type: DATE
        sample_values:
          - '2025-02-20'
          - '2023-07-07'
          - '2025-03-16'
      - name: SHIP_DATE_KEY
        description: The date on which the order was shipped to the customer.
        expr: SHIP_DATE_KEY
        data_type: DATE
        sample_values:
          - '2025-04-04'
          - '2023-07-30'
          - '2023-09-20'
    facts:
      - name: DISCOUNT
        description: The percentage discount applied to a sale, with 0.00 indicating no discount and higher values indicating a percentage reduction in the sale price.
        expr: DISCOUNT
        data_type: NUMBER
        sample_values:
          - '0.10'
          - '0.00'
      - name: EXTENDED_PRICE
        description: The total amount of money earned from the sale of a product or service, calculated by multiplying the unit price by the quantity sold.
        expr: EXTENDED_PRICE
        data_type: NUMBER
        sample_values:
          - '4046.00'
          - '786.00'
          - '605.00'
      - name: NET_PRICE
        description: The net price of a sale, representing the amount of money received from a customer after all discounts and deductions have been applied.
        expr: NET_PRICE
        data_type: NUMBER
        sample_values:
          - '3818.70'
          - '1778.40'
          - '1731.00'
      - name: SALES_AMOUNT
        description: The total amount of sales generated from a specific transaction or event.
        expr: SALES_AMOUNT
        data_type: NUMBER
        sample_values:
          - '2226.65'
          - '1806.24'
          - '3175.01'
      - name: TAX
        description: The percentage of sales tax applied to a transaction.
        expr: TAX
        data_type: NUMBER
        sample_values:
          - '0.03'
          - '0.05'
          - '0.08'
  - name: AI_SALES_PREDICTION
    description: This table stores sales predictions for future dates, including the predicted sales amount and a confidence interval defined by lower and upper bounds, allowing for the estimation of potential sales variability.
    base_table:
      database: BERCA_TEST_DS
      schema: GOLD
      table: AI_SALES_PREDICTION
    time_dimensions:
      - name: FORECAST_DATE
        description: Date for which the sales forecast is being made.
        expr: FORECAST_DATE
        data_type: TIMESTAMP_NTZ
        sample_values:
          - 2025-11-20T00:00:00.000+0000
          - 2025-11-23T00:00:00.000+0000
          - 2025-11-21T00:00:00.000+0000
    facts:
      - name: LOWER_BOUND
        description: The minimum predicted sales value for a specific time period or product, representing the lower end of the forecasted sales range.
        expr: LOWER_BOUND
        data_type: FLOAT
        sample_values:
          - '1122523.6808037'
          - '1130621.77141692'
          - '1129709.49787372'
      - name: PREDICTED_SALES
        description: The predicted sales amount for a specific period, representing the forecasted revenue based on historical data and trends.
        expr: PREDICTED_SALES
        data_type: FLOAT
        sample_values:
          - '1383354.22280133'
          - '1381978.50444035'
          - '1375717.04443605'
      - name: UPPER_BOUND
        description: The maximum predicted sales amount for a given period or product.
        expr: UPPER_BOUND
        data_type: FLOAT
        sample_values:
          - '1636086.69918574'
          - '1628910.3955684'
          - '1634247.51100699'
  - name: AI_CUSTOMER_SENTIMENT
    description: This table stores customer sentiment data, capturing the emotional tone and attitude of customers towards a company or product. It contains key customer information, including customer name and account balance, as well as the raw text of their comments or feedback. The sentiment score, a numerical value, represents the degree of positivity or negativity in the customer's comment, allowing for analysis and tracking of customer satisfaction and sentiment over time.
    base_table:
      database: BERCA_TEST_DS
      schema: GOLD
      table: AI_CUSTOMER_SENTIMENT
    dimensions:
      - name: CUSTOMER_KEY
        description: Unique identifier for a customer in the customer database.
        expr: CUSTOMER_KEY
        data_type: NUMBER
        sample_values:
          - '98'
          - '29'
          - '1'
      - name: CUSTOMER_NAME
        description: The name of the customer who provided sentiment feedback.
        expr: CUSTOMER_NAME
        data_type: VARCHAR
        sample_values:
          - Customer 67
          - Customer 78
          - Customer 232
      - name: MARKET_SEGMENT
        description: The market segment to which the customer belongs, categorizing the industry or sector of the customer's business, such as furniture, automobile, or building.
        expr: MARKET_SEGMENT
        data_type: VARCHAR
        sample_values:
          - FURNITURE
          - AUTOMOBILE
          - BUILDING
      - name: RAW_COMMENT
        description: The customer's original, unedited feedback or comment about their experience with the company.
        expr: RAW_COMMENT
        data_type: VARCHAR
        sample_values:
          - Customer services was terrible. The resolution took 3 weeks and I am very unhappy.
          - Just an average transaction. Nothing special to report.
          - Product arrived damaged. This is unacceptable and I need a full refund immediately.
    facts:
      - name: CUSTOMER_ACCOUNT_BALANCE
        description: The current balance in the customer's account, representing the total amount of money the customer owes or is owed by the company.
        expr: CUSTOMER_ACCOUNT_BALANCE
        data_type: NUMBER
        sample_values:
          - '3505.00'
          - '3041.00'
          - '6294.00'
      - name: SENTIMENT_SCORE
        description: A numerical score representing the sentiment of a customer's feedback or review, ranging from -1 (very negative) to 1 (very positive), with 0 indicating a neutral sentiment.
        expr: SENTIMENT_SCORE
        data_type: FLOAT
        sample_values:
          - '0.8828125'
          - '-0.8671875'
          - '0.890625'
  - name: AGG_BRAND_SALES
    description: This table stores aggregated sales data by brand, including the total sales amount and quantity, along with a timestamp indicating when the data was loaded.
    base_table:
      database: BERCA_TEST_DS
      schema: GOLD
      table: AGG_BRAND_SALES
    dimensions:
      - name: BRAND
        description: The brand name of the product sold.
        expr: BRAND
        data_type: VARCHAR
        sample_values:
          - Brand 3
          - Brand 9
          - Brand 4
      - name: TOTAL_QUANTITY
        description: The total quantity of products sold for each brand.
        expr: TOTAL_QUANTITY
        data_type: NUMBER
        sample_values:
          - '1530852'
          - '1532730'
          - '1517870'
    time_dimensions:
      - name: LOAD_TIMESTAMP
        description: The timestamp when the brand sales data was loaded into the system.
        expr: LOAD_TIMESTAMP
        data_type: TIMESTAMP_NTZ
        sample_values:
          - 2025-11-20T23:42:46.893+0000
    facts:
      - name: TOTAL_SALES_AMOUNT
        description: The total amount of sales generated by a brand across all its products and regions.
        expr: TOTAL_SALES_AMOUNT
        data_type: NUMBER
        sample_values:
          - '150594979.65'
          - '149979103.56'
          - '150497025.62'
  - name: AGG_DAILY_SALES_REGION
    description: This table provides a daily summary of sales performance by region, including the total number of orders, total sales amount, and average order value, updated in near real-time with a 1-minute lag.
    base_table:
      database: BERCA_TEST_DS
      schema: GOLD
      table: AGG_DAILY_SALES_REGION
    dimensions:
      - name: REGION_NAME
        description: The geographic region where the sales were generated.
        expr: REGION_NAME
        data_type: VARCHAR
        sample_values:
          - ASIA
          - AFRICA
          - AMERICA
      - name: TOTAL_ORDERS
        description: The total number of orders received in a region on a given day.
        expr: TOTAL_ORDERS
        data_type: NUMBER
        sample_values:
          - '23'
          - '25'
          - '33'
    time_dimensions:
      - name: SALES_DATE
        description: Date on which the sales were made.
        expr: SALES_DATE
        data_type: DATE
        sample_values:
          - '2023-01-07'
          - '2022-12-19'
          - '2022-11-22'
    facts:
      - name: AVG_ORDER_VALUE
        description: The average value of all orders placed in a region on a given day.
        expr: AVG_ORDER_VALUE
        data_type: NUMBER
        sample_values:
          - '2418.13411765'
          - '2609.06410000'
          - '2399.72009259'
      - name: TOTAL_SALES_AMOUNT
        description: The total amount of sales generated by all stores within a region on a given day.
        expr: TOTAL_SALES_AMOUNT
        data_type: NUMBER
        sample_values:
          - '231666.13'
          - '258661.09'
          - '351714.20'
  - name: V_CHURN_RISK_ALERT
    description: This view provides a list of customers with negative sentiment scores, prioritized by their account balance, along with a recommended action to mitigate potential churn risk.
    base_table:
      database: BERCA_TEST_DS
      schema: GOLD
      table: V_CHURN_RISK_ALERT
    dimensions:
      - name: ACTION_RECOMMENDATION
        description: The recommended course of action to mitigate the risk of customer churn, with possible values including immediate contact with the customer or ongoing monitoring of their activity.
        expr: ACTION_RECOMMENDATION
        data_type: VARCHAR
        sample_values:
          - 'Urgent: Contact Customer'
          - Monitor
      - name: CUSTOMER_NAME
        description: The name of the customer who is at risk of churning.
        expr: CUSTOMER_NAME
        data_type: VARCHAR
        sample_values:
          - Customer 14452
          - Customer 10409
          - Customer 984
      - name: MARKET_SEGMENT
        description: The market segment to which the customer belongs, categorizing their primary business or industry focus.
        expr: MARKET_SEGMENT
        data_type: VARCHAR
        sample_values:
          - FURNITURE
          - HOUSEHOLD
          - BUILDING
      - name: RAW_COMMENT
        description: Customer feedback or comments about their transaction experience, including both positive and negative aspects.
        expr: RAW_COMMENT
        data_type: VARCHAR
        sample_values:
          - I have mixed feelings. The price was good, but the delivery driver was rude.
          - I found a small defect, but it was not worth the hassle of returning the item.
          - Just an average transaction. Nothing special to report.
    facts:
      - name: CUSTOMER_ACCOUNT_BALANCE
        description: The current balance of the customer's account, representing the total amount of funds available for use.
        expr: CUSTOMER_ACCOUNT_BALANCE
        data_type: NUMBER
        sample_values:
          - '9833.00'
          - '9798.00'
          - '9458.00'
      - name: SENTIMENT_SCORE
        description: A score indicating the sentiment of customer feedback or interactions, ranging from -1 (very negative) to 1 (very positive), with 0 being neutral, used to assess the risk of customer churn.
        expr: SENTIMENT_SCORE
        data_type: FLOAT
        sample_values:
          - '-0.3203125'
          - '-0.01953125'
          - '-0.28125'
relationships:
  - name: DIM_CUSTOMER_TO_CUSTOMER
    left_table: DIM_CUSTOMER
    right_table: CUSTOMER
    join_type: inner
    relationship_type: one_to_one
    relationship_columns:
      - left_column: CUSTOMER_KEY
        right_column: C_CUSTKEY
  - name: FACT_SALES_TO_DIM_PART
    left_table: FACT_SALES
    right_table: DIM_PART
    join_type: inner
    relationship_type: many_to_one
    relationship_columns:
      - left_column: PART_KEY
        right_column: PART_KEY
verified_queries:
  - name: On which date is the predicted sales amount highest, and what is its upper bound estimate?
    question: On which date is the predicted sales amount highest, and what is its upper bound estimate?
    sql: SELECT FORECAST_DATE, PREDICTED_SALES, UPPER_BOUND FROM ai_sales_prediction ORDER BY PREDICTED_SALES DESC LIMIT 1
    verified_at: 1763715018
    verified_by: Semantic Model Generator
  - name: How many customers with account balances over $5,000 are currently flagged for monitoring?
    question: How many customers with account balances over $5,000 are currently flagged for monitoring?
    sql: SELECT COUNT(CUSTOMER_NAME) FROM v_churn_risk_alert WHERE ACTION_RECOMMENDATION = 'Monitor' AND CUSTOMER_ACCOUNT_BALANCE > 5000
    verified_at: 1763715018
    verified_by: Semantic Model Generator
  - name: Which region had the highest total sales amount yesterday?
    question: Which region had the highest total sales amount yesterday?
    sql: SELECT REGION_NAME FROM agg_daily_sales_region WHERE SALES_DATE = DATEADD(DAY, -1, CURRENT_DATE) ORDER BY TOTAL_SALES_AMOUNT DESC LIMIT 1
    verified_at: 1763715018
    verified_by: Semantic Model Generator
  - name: What is the average sentiment score for customers in the machinery sector?
    question: What is the average sentiment score for customers in the machinery sector?
    sql: SELECT AVG(SENTIMENT_SCORE) FROM ai_customer_sentiment WHERE MARKET_SEGMENT = 'MACHINERY'
    verified_at: 1763715018
    verified_by: Semantic Model Generator
  - name: What was the total quantity sold for Brand 5 products in the last quarter?
    question: What was the total quantity sold for Brand 5 products in the last quarter?
    sql: SELECT SUM(T1.QUANTITY) FROM fact_sales AS T1 JOIN dim_part AS T2 ON T1.PART_KEY = T2.PART_KEY WHERE T2.BRAND = 'Brand 5' AND T1.ORDER_DATE_KEY >= DATEADD(QUARTER, -1, CURRENT_DATE)
    verified_at: 1763715018
    verified_by: Semantic Model Generator
  - name: What is the address and phone number of the customer who complained that customer services was terrible?
    question: What is the address and phone number of the customer who complained that customer services was terrible?
    sql: SELECT T1.CUSTOMER_ADDRESS, T1.CUSTOMER_PHONE FROM dim_customer AS T1 JOIN customer AS T2 ON T1.CUSTOMER_KEY = T2.C_CUSTKEY WHERE T2.C_COMMENT = 'Customer services was terrible.'
    verified_at: 1763715018
    verified_by: Semantic Model Generator
  - name: What has been our daily total sales trend over the last 30 days?
    question: What has been our daily total sales trend over the last 30 days?
    sql: |-
      WITH __agg_daily_sales_region AS (
        SELECT
          agg_daily_sales_region.sales_date AS sales_date,
          agg_daily_sales_region.total_sales_amount AS total_sales_amount
        FROM
          __agg_daily_sales_region AS agg_daily_sales_region
      )
      SELECT
        __agg_daily_sales_region.sales_date AS sales_date,
        SUM(__agg_daily_sales_region.total_sales_amount) AS daily_total_sales
      FROM
        __agg_daily_sales_region AS __agg_daily_sales_region
      WHERE
        __agg_daily_sales_region.sales_date >= DATEADD(DAY, -30, CURRENT_DATE)
      GROUP BY
        __agg_daily_sales_region.sales_date
      ORDER BY
        sales_date DESC NULLS LAST
    use_as_onboarding_question: false
    verified_by: David The
    verified_at: 1763715090
  - name: What was the average order value by region on November 1st, 2025?
    question: What was the average order value by region on November 1st, 2025?
    sql: |-
      WITH __agg_daily_sales_region AS (
        SELECT
          agg_daily_sales_region.region_name AS region_name,
          agg_daily_sales_region.sales_date AS sales_date,
          agg_daily_sales_region.avg_order_value AS avg_order_value
        FROM
          __agg_daily_sales_region AS agg_daily_sales_region
      )
      SELECT
        __agg_daily_sales_region.region_name AS region_name,
        -- Calculate the AVERAGE of the AVG_ORDER_VALUE for the specified date
        -- (Grouping by region is unnecessary since the sales date is fixed, but is included for robustness)
        AVG(__agg_daily_sales_region.avg_order_value) AS average_order_value
      FROM
        __agg_daily_sales_region AS __agg_daily_sales_region
      WHERE
        -- Correctly filters for November 1st, 2025 (Absolute Date)
        __agg_daily_sales_region.sales_date = '2025-11-01'::DATE
      GROUP BY
          __agg_daily_sales_region.region_name
      ORDER BY
        region_name DESC NULLS LAST
    use_as_onboarding_question: false
    verified_by: David The
    verified_at: 1763715184
    """

def generate_sql_and_answer(session: Session, user_question: str) -> str:
    import re

    # 1. Get semantic context
    semantic_context = get_semantic_model_context()
    
    sql_generation_prompt = f"""
You are an expert Snowflake SQL generator.
Convert the following user question into a valid Snowflake SQL query.
Use only the tables/columns provided in the SCHEMA CONTEXT.
Return SQL ONLY — do not include explanations or commentary.

SCHEMA CONTEXT:
---
{semantic_context}
---

USER QUESTION: '{user_question}'

Generated SQL ONLY:
"""

    try:
        # 2. Generate SQL using Claude
        sql_query_result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'claude-3-5-sonnet',
                '{sql_generation_prompt.replace("'", "''")}'
            )
        """).collect()
        
        generated_sql = sql_query_result[0][0].strip()

        # 3. Table mapping goes HERE
        TABLE_MAPPING = {
            "CUSTOMER": '"BERCA_TEST_DS"."BRONZE"."CUSTOMER"',
            "DIM_CUSTOMER": '"BERCA_TEST_DS"."SILVER"."DIM_CUSTOMER"',
            "DIM_PART": '"BERCA_TEST_DS"."SILVER"."DIM_PART"',
            "FACT_SALES": '"BERCA_TEST_DS"."SILVER"."FACT_SALES"',
            "AI_SALES_PREDICTION": '"BERCA_TEST_DS"."GOLD"."AI_SALES_PREDICTION"',
            "AI_CUSTOMER_SENTIMENT": '"BERCA_TEST_DS"."GOLD"."AI_CUSTOMER_SENTIMENT"',
            "AGG_BRAND_SALES": '"BERCA_TEST_DS"."GOLD"."AGG_BRAND_SALES"',
            "AGG_DAILY_SALES_REGION": '"BERCA_TEST_DS"."GOLD"."AGG_DAILY_SALES_REGION"',
            "V_CHURN_RISK_ALERT": '"BERCA_TEST_DS"."GOLD"."V_CHURN_RISK_ALERT"'
        }

        # 4. Replace table names with fully qualified names
        for table, full_name in TABLE_MAPPING.items():
            generated_sql = re.sub(rf'\b{table}\b', full_name, generated_sql)

    except Exception as e:
        return f"Error during SQL generation by Cortex: {e}"

    try:
        # 5. Execute SQL
        df_result = session.sql(generated_sql).to_pandas()

        # 6. Format result with LLM
        result_formatting_prompt = f"""
The user asked: '{user_question}'.
The raw SQL result in JSON format is: {df_result.to_json(orient='records')}

Please summarize this data into a conversational, non-technical sentence or paragraph.
If the result set is empty, respond with 'I found no data matching your request.'
"""

        answer_query_result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'claude-3-5-sonnet',
                '{result_formatting_prompt.replace("'", "''")}'
            )
        """).collect()
        
        return answer_query_result[0][0].strip()
        
    except Exception as e:
        return f"I ran into an error trying to execute the generated query. The SQL generated was: `{generated_sql}`. Error message: {str(e)}"

$$;

USE SCHEMA PUBLIC;

CREATE OR REPLACE STAGE STREAML_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

CREATE OR REPLACE STREAMLIT BERCA_TEST_DS.PUBLIC.CHATBOT_AGENT
  ROOT_LOCATION = '@BERCA_TEST_DS.PUBLIC.STREAML_STAGE' -- Use a stage to hold the Python file
  MAIN_FILE = 'chatbot_app.py';

USE SCHEMA GOLD;