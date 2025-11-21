-- Set the role to SYSADMIN
USE ROLE SYSADMIN;

-- 1. Create a dedicated Warehouse
-- For a technical test, a modest size is sufficient.
CREATE OR REPLACE WAREHOUSE BERCA_WH WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    COMMENT = 'Warehouse for Berca Data Scientist Technical Test.';

-- 2. Create the Database
CREATE OR REPLACE DATABASE BERCA_TEST_DS;

-- 3. Set Context
USE WAREHOUSE BERCA_WH;
USE DATABASE BERCA_TEST_DS;

-- 4. Create Schemas for Medallion Architecture
CREATE OR REPLACE SCHEMA BRONZE COMMENT = 'Raw ingested data layer.';
CREATE OR REPLACE SCHEMA SILVER COMMENT = 'Cleaned, conformed, and dimensional layer.';
CREATE OR REPLACE SCHEMA GOLD COMMENT = 'Aggregated, summarized, and business-ready layer.';
