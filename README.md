# Databricks PySpark Data Pipeline

This project demonstrates a modular and testable PySpark pipeline in Databricks using Delta Lake and Autoloader.

## Features
- Incremental ingestion using Autoloader (`cloudFiles`)
- Delta Lake writes
- Data cleaning, aggregation, and window logic
- Unit testing using PyTest
- Modular structure for Databricks Repos

## Run Steps
1. Upload `sample_transactions.csv` to `/FileStore/raw/transactions/`
2. Run `main.py` in Databricks
3. Check Delta outputs:
   - Silver: `/FileStore/silver_delta/`
   - Gold: `/FileStore/gold_delta/`

## Run Tests
```bash
pytest tests/
```
