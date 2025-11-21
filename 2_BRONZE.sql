USE ROLE SYSADMIN;
USE WAREHOUSE BERCA_WH;
USE DATABASE BERCA_TEST_DS;
USE SCHEMA BRONZE;

-- TPC-H Table Cardinalities (Approximation for Demo):
-- REGION (R): 5
-- NATION (N): 25
-- SUPPLIER (S): 1,000
-- CUSTOMER (C): 15,000
-- PART (P): 20,000
-- PARTSUPP (PS): 80,000
-- ORDERS (O): 150,000
-- LINEITEM (L): 600,000

-- Helper function to generate a random number within a range
CREATE OR REPLACE FUNCTION RAND_INT(min_val INT, max_val INT)
RETURNS INT
AS 'UNIFORM(min_val, max_val, RANDOM())';

--------------------------------------------------------------------------------
-- 1. REGION (R)
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE REGION (
    R_REGIONKEY NUMBER,
    R_NAME VARCHAR,
    R_COMMENT VARCHAR
)
AS
SELECT
    SEQ4() AS R_REGIONKEY,
    CASE MOD(SEQ4(), 5)
        WHEN 0 THEN 'AFRICA'
        WHEN 1 THEN 'ASIA'
        WHEN 2 THEN 'AMERICA'
        WHEN 3 THEN 'EUROPE'
        ELSE 'MIDDLE EAST'
    END AS R_NAME,
    'Region comment ' || SEQ4() AS R_COMMENT
FROM
    TABLE(GENERATOR(ROWCOUNT => 5));


--------------------------------------------------------------------------------
-- 2. NATION (N)
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE NATION (
    N_NATIONKEY NUMBER,
    N_NAME VARCHAR,
    N_REGIONKEY NUMBER,
    N_COMMENT VARCHAR
)
AS
SELECT
    SEQ4() AS N_NATIONKEY,
    CASE SEQ4()
        WHEN 0 THEN 'ALGERIA'
        WHEN 1 THEN 'ARGENTINA'
        WHEN 2 THEN 'BRAZIL'
        WHEN 3 THEN 'CANADA'
        WHEN 4 THEN 'EGYPT'
        WHEN 5 THEN 'ETHIOPIA'
        WHEN 6 THEN 'FRANCE'
        WHEN 7 THEN 'GERMANY'
        WHEN 8 THEN 'INDIA'
        WHEN 9 THEN 'INDONESIA'
        WHEN 10 THEN 'IRAN'
        WHEN 11 THEN 'IRAQ'
        WHEN 12 THEN 'JAPAN'
        WHEN 13 THEN 'JORDAN'
        WHEN 14 THEN 'KENYA'
        WHEN 15 THEN 'MOROCCO'
        WHEN 16 THEN 'MOZAMBIQUE'
        WHEN 17 THEN 'PERU'
        WHEN 18 THEN 'CHINA'
        WHEN 19 THEN 'ROMANIA'
        WHEN 20 THEN 'SAUDI ARABIA'
        WHEN 21 THEN 'VIETNAM'
        WHEN 22 THEN 'RUSSIA'
        WHEN 23 THEN 'UNITED KINGDOM'
        WHEN 24 THEN 'UNITED STATES'
    END AS N_NAME,
    CASE SEQ4()
        WHEN 0 THEN 0
        WHEN 1 THEN 1
        WHEN 2 THEN 1
        WHEN 3 THEN 1
        WHEN 4 THEN 4
        WHEN 5 THEN 0
        WHEN 6 THEN 3
        WHEN 7 THEN 3
        WHEN 8 THEN 2
        WHEN 9 THEN 2
        WHEN 10 THEN 4
        WHEN 11 THEN 4
        WHEN 12 THEN 2
        WHEN 13 THEN 4
        WHEN 14 THEN 0
        WHEN 15 THEN 0
        WHEN 16 THEN 0
        WHEN 17 THEN 1
        WHEN 18 THEN 2
        WHEN 19 THEN 3
        WHEN 20 THEN 4
        WHEN 21 THEN 2
        WHEN 22 THEN 3
        WHEN 23 THEN 3
        WHEN 24 THEN 1
    END AS N_REGIONKEY, -- FK to REGION
    'Nation comment ' || SEQ4() AS N_COMMENT
FROM
    TABLE(GENERATOR(ROWCOUNT => 25));


--------------------------------------------------------------------------------
-- 3. CUSTOMER (C)
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE CUSTOMER (
    C_CUSTKEY NUMBER,
    C_NAME VARCHAR,
    C_ADDRESS VARCHAR,
    C_NATIONKEY NUMBER,
    C_PHONE VARCHAR,
    C_ACCTBAL NUMBER(12,2),
    C_MKTSEGMENT VARCHAR,
    C_COMMENT VARCHAR
)
AS
SELECT
    SEQ4() AS C_CUSTKEY,
    'Customer ' || SEQ4() AS C_NAME,
    'Address ' || SEQ4() AS C_ADDRESS,
    RAND_INT(0, 24) AS C_NATIONKEY, -- FK to NATION
    '111-222-' || LPAD(SEQ4(), 4, '0') AS C_PHONE,
    UNIFORM(100.00, 10000.00, RANDOM(1))::NUMBER(12,2) AS C_ACCTBAL,
    CASE MOD(SEQ4(), 5)
        WHEN 0 THEN 'AUTOMOBILE'
        WHEN 1 THEN 'BUILDING'
        WHEN 2 THEN 'FURNITURE'
        WHEN 3 THEN 'MACHINERY'
        ELSE 'HOUSEHOLD'
    END AS C_MKTSEGMENT,
    -- Use CASE statement to generate realistic comments with mixed sentiment
    CASE MOD(SEQ4(), 10)
        WHEN 0 THEN 'Customer services was terrible. The resolution took 3 weeks and I am very unhappy.' -- Strongly Negative
        WHEN 1 THEN 'The shipping was fast and the product quality exceeded expectations. A great purchase!' -- Strongly Positive
        WHEN 2 THEN 'Product arrived damaged. This is unacceptable and I need a full refund immediately.' -- Negative
        WHEN 3 THEN 'Excellent value for the price. I would highly recommend this supplier to anyone.' -- Positive
        WHEN 4 THEN 'I have mixed feelings. The price was good, but the delivery driver was rude.' -- Neutral/Mixed
        WHEN 5 THEN 'Everything was handled smoothly and professionally. No complaints here.' -- Positive
        WHEN 6 THEN 'I found a small defect, but it was not worth the hassle of returning the item.' -- Mildly Negative
        WHEN 7 THEN 'Just an average transaction. Nothing special to report.' -- Neutral
        WHEN 8 THEN 'Best purchase of the year! Fantastic quality and customer support.' -- Highly Positive
        ELSE 'Comment concerning the delivery time. It was delayed by 3 days.' -- Neutral/Slightly Negative
    END AS C_COMMENT
FROM
    TABLE(GENERATOR(ROWCOUNT => 15000));


--------------------------------------------------------------------------------
-- 4. SUPPLIER (S)
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SUPPLIER (
    S_SUPPKEY NUMBER,
    S_NAME VARCHAR,
    S_ADDRESS VARCHAR,
    S_NATIONKEY NUMBER,
    S_PHONE VARCHAR,
    S_ACCTBAL NUMBER(12,2),
    S_COMMENT VARCHAR
)
AS
SELECT
    SEQ4() AS S_SUPPKEY,
    'Supplier ' || SEQ4() AS S_NAME,
    'Address ' || SEQ4() AS S_ADDRESS,
    RAND_INT(0, 24) AS S_NATIONKEY, -- FK to NATION
    '333-444-' || LPAD(SEQ4(), 4, '0') AS S_PHONE,
    UNIFORM(100.00, 10000.00, RANDOM(2))::NUMBER(12,2) AS S_ACCTBAL,
    'Supplier comment ' || SEQ4() AS S_COMMENT
FROM
    TABLE(GENERATOR(ROWCOUNT => 1000));


--------------------------------------------------------------------------------
-- 5. PART (P)
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE PART (
    P_PARTKEY NUMBER,
    P_NAME VARCHAR,
    P_MFGR VARCHAR,
    P_BRAND VARCHAR,
    P_TYPE VARCHAR,
    P_SIZE NUMBER,
    P_CONTAINER VARCHAR,
    P_RETAILPRICE NUMBER(12,2),
    P_COMMENT VARCHAR
)
AS
SELECT
    SEQ4() AS P_PARTKEY,
    'Part ' || SEQ4() AS P_NAME,
    'Manufacturer ' || MOD(SEQ4(), 5) AS P_MFGR,
    'Brand ' || MOD(SEQ4(), 10) AS P_BRAND,
    'Type ' || MOD(SEQ4(), 15) AS P_TYPE,
    RAND_INT(1, 50) AS P_SIZE,
    CASE MOD(SEQ4(), 4)
        WHEN 0 THEN 'SM BOX'
        WHEN 1 THEN 'LG BOX'
        WHEN 2 THEN 'WRAP'
        ELSE 'BULK'
    END AS P_CONTAINER,
    UNIFORM(100.00, 2000.00, RANDOM(3))::NUMBER(12,2) AS P_RETAILPRICE,
    'Part comment ' || SEQ4() AS P_COMMENT
FROM
    TABLE(GENERATOR(ROWCOUNT => 20000));


--------------------------------------------------------------------------------
-- 6. PARTSUPP (PS) - Linking Part and Supplier
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE PARTSUPP (
    PS_PARTKEY NUMBER,
    PS_SUPPKEY NUMBER,
    PS_AVAILQTY NUMBER,
    PS_SUPPLYCOST NUMBER(12,2),
    PS_COMMENT VARCHAR
)
AS
SELECT
    -- Generate Part Key: MOD(SEQ4(), 20000) maps the sequence to 0-19999 (the 20k parts)
    MOD(SEQ4(), 20000) AS PS_PARTKEY,
    -- Generate Supplier Key: MOD(SEQ4(), 1000) maps the sequence to 0-999 (the 1k suppliers)
    MOD(SEQ4(), 1000) AS PS_SUPPKEY,
    RAND_INT(100, 1000) AS PS_AVAILQTY,
    UNIFORM(100.00, 500.00, RANDOM(4))::NUMBER(12,2) AS PS_SUPPLYCOST,
    'Partsupp comment ' || SEQ4() AS PS_COMMENT
FROM
    TABLE(GENERATOR(ROWCOUNT => 80000));

--SELECT COUNT(*) FROM PARTSUPP; -- Should be 80,000


--------------------------------------------------------------------------------
-- 7. ORDERS (O)
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE ORDERS (
    O_ORDERKEY NUMBER,
    O_CUSTKEY NUMBER,
    O_ORDERSTATUS VARCHAR(1),
    O_TOTALPRICE NUMBER(12,2),
    O_ORDERDATE DATE,
    O_ORDERPRIORITY VARCHAR,
    O_CLERK VARCHAR,
    O_SHIPPRIORITY NUMBER,
    O_COMMENT VARCHAR
)
AS
SELECT
    SEQ4() AS O_ORDERKEY,
    RAND_INT(0, 14999) AS O_CUSTKEY, -- FK to CUSTOMER
    CASE MOD(SEQ4(), 3)
        WHEN 0 THEN 'O' -- Open
        WHEN 1 THEN 'F' -- Finished
        ELSE 'P' -- Pending
    END AS O_ORDERSTATUS,
    UNIFORM(500.00, 100000.00, RANDOM(5))::NUMBER(12,2) AS O_TOTALPRICE,
    DATEADD(day, -RAND_INT(1, 1095), CURRENT_DATE()) AS O_ORDERDATE, -- Last 3 years
    'Priority ' || MOD(SEQ4(), 5) AS O_ORDERPRIORITY,
    'Clerk ' || LPAD(MOD(SEQ4(), 100), 3, '0') AS O_CLERK,
    RAND_INT(0, 7) AS O_SHIPPRIORITY,
    'Order comment ' || SEQ4() AS O_COMMENT
FROM
    TABLE(GENERATOR(ROWCOUNT => 150000));


--------------------------------------------------------------------------------
-- 8. LINEITEM (L)
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE LINEITEM (
    L_ORDERKEY NUMBER,
    L_PARTKEY NUMBER,
    L_SUPPKEY NUMBER,
    L_LINENUMBER NUMBER,
    L_QUANTITY NUMBER,
    L_EXTENDEDPRICE NUMBER(12,2),
    L_DISCOUNT NUMBER(12,2),
    L_TAX NUMBER(12,2),
    L_RETURNFLAG VARCHAR(1),
    L_LINESTATUS VARCHAR(1),
    L_SHIPDATE DATE,
    L_COMMITDATE DATE,
    L_RECEIPTDATE DATE,
    L_SHIPINSTRUCT VARCHAR,
    L_SHIPMODE VARCHAR,
    L_COMMENT VARCHAR
)
AS
SELECT
    -- Assign ORDERKEY: MOD(SEQ4(), 150000) maps sequence to 0-149999 (the 150k orders)
    MOD(SEQ4(), 150000) AS L_ORDERKEY,
    RAND_INT(0, 19999) AS L_PARTKEY, -- FK to PART
    RAND_INT(0, 999) AS L_SUPPKEY, -- FK to SUPPLIER
    (MOD(SEQ4(), 7) + 1) AS L_LINENUMBER, -- Max 7 line items per order
    RAND_INT(1, 50) AS L_QUANTITY,
    UNIFORM(100.00, 5000.00, RANDOM(6))::NUMBER(12,2) AS L_EXTENDEDPRICE,
    UNIFORM(0.00, 0.10, RANDOM(7))::NUMBER(12,2) AS L_DISCOUNT,
    UNIFORM(0.00, 0.08, RANDOM(8))::NUMBER(12,2) AS L_TAX,
    CASE MOD(SEQ4(), 3) WHEN 0 THEN 'R' WHEN 1 THEN 'A' ELSE 'N' END AS L_RETURNFLAG,
    CASE MOD(SEQ4(), 2) WHEN 0 THEN 'O' ELSE 'F' END AS L_LINESTATUS,
    -- Approximate random dates for simplicity and speed:
    DATEADD(day, -RAND_INT(1, 1000), CURRENT_DATE()) AS L_SHIPDATE,
    DATEADD(day, RAND_INT(1, 30), L_SHIPDATE) AS L_COMMITDATE, -- Commit date after Ship date
    DATEADD(day, RAND_INT(31, 60), L_SHIPDATE) AS L_RECEIPTDATE, -- Receipt date after Ship date
    'Deliver ' || MOD(SEQ4(), 5) AS L_SHIPINSTRUCT,
    CASE MOD(SEQ4(), 7) WHEN 0 THEN 'REG AIR' WHEN 1 THEN 'FOB' WHEN 2 THEN 'TRUCK' ELSE 'MAIL' END AS L_SHIPMODE,
    'Lineitem comment ' || SEQ4() AS L_COMMENT
FROM
    TABLE(GENERATOR(ROWCOUNT => 600000));

-- SELECT COUNT(*) FROM REGION; -- Should be 5
-- SELECT COUNT(*) FROM LINEITEM; -- Should be approx 600,000