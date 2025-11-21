# BERCA_TEST_DS

[![SQL](https://img.shields.io/badge/Language-SQL-blue)](https://www.sql.org/) 
[![Python](https://img.shields.io/badge/Language-Python-yellowgreen)](https://www.python.org/) 
[![Streamlit](https://img.shields.io/badge/Framework-Streamlit-orange)](https://streamlit.io/)
[![Snowflake](https://img.shields.io/badge/Platform-Snowflake-5390F0)](https://www.snowflake.com/)
[![LLM](https://img.shields.io/badge/AI-LLM-lightgrey)](https://www.llamaindex.ai/)

---

## Overview

This repository contains a **technical test project for BERCA** as a Data Scientist.  
It includes a **data pipeline**, **data masking scripts**, **AI/ML SQL queries**, and an **agentic chatbot prototype** using Python and Streamlit.

---

## Folder Structure

- Agentic AI/                # Python scripts for building a chatbot / agent
- 1_INITIAL.sql               # Initial database setup
- 2_BRONZE.sql                # Bronze layer transformation scripts
- 3_SILVER.sql                # Silver layer transformation scripts
- 4_GOLD.sql                  # Gold layer transformation scripts
- 5_DNF_AND_DATA MASKING.sql  # Scripts for data masking & anonymization
- 6_AI.sql                    # AI-related scripts / queries
- README.md                   # Project documentation


---

## SQL Data Pipeline

The SQL scripts implement a **multi-layer data pipeline**:

- **Initial Layer (`1_INITIAL.sql`)** – Set up raw tables  
- **Bronze Layer (`2_BRONZE.sql`)** – Basic cleaning & ingestion  
- **Silver Layer (`3_SILVER.sql`)** – Transformation & integration  
- **Gold Layer (`4_GOLD.sql`)** – Aggregated / business-ready tables  
- **Data Masking (`5_DNF_AND_DATA MASKING.sql`)** – Data anonymization  
- **AI Queries (`6_AI.sql`)** – AI/ML related queries  

**Execution order:**  
```sql
1_INITIAL.sql → 2_BRONZE.sql → 3_SILVER.sql → 4_GOLD.sql → 5_DNF_AND_DATA MASKING.sql → 6_AI.sql
```
---

## Agentic Chatbot (Python)

The Agentic AI folder contains:

- Python scripts to build a chatbot using Streamlit

- Integration with Snowflake for querying structured data

- YAML-based semantic model to map tables/columns for natural language questions

- Supports SQL generation via LLM (OpenAI API or small local model)

---

## How to Run

1️⃣ SQL Pipeline

- Connect to your Snowflake account

- Execute scripts in order (see above)

2️⃣ Agentic Chatbot

- Make sure Snowflake Streamlit is enabled

- Ensure the semantic YAML model is uploaded to a Snowflake stage

- Install required Python packages:

```sql
- pip install streamlit snowflake-snowpark-python llama-index openai
```

Deploy or run chatbot_app.py in Streamlit:

streamlit run Agentic\ AI/chatbot_app.py


## Ask natural language questions like 
```sql
What are the total sales this month?
```
```sql
How many new customers did we get in the last 90 days?
```
```sql
Who are the top 10 customers by account balance?
```

— the chatbot will generate SQL queries and return results from Snowflake

## Notes

Works with Snowflake Free Account

Supports OpenAI GPT API or local LLM for SQL generation

Data masking scripts ensure sensitive data is anonymized

---

## Architecture Diagram

flowchart TD
- A[Raw Tables (Initial Layer)] --> B[Bronze Layer]
- B --> C[Silver Layer]
- C --> D[Gold Layer]
- D --> E[Semantic YAML Model]
- E --> F[Streamlit Chatbot]
- F --> G[Snowflake SQL Execution]
- G --> H[Query Results Returned]

---

- Raw data flows through Bronze → Silver → Gold layers

- Semantic YAML model maps table/column relationships

- Chatbot generates SQL using LLM

- Snowflake executes SQL and returns results in Streamlit
