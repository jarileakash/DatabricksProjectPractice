import pytest
from pyspark.sql import SparkSession
from src.transformations import clean_transactions, aggregate_customer_spend, get_latest_transaction

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("TestSession").getOrCreate()

def test_clean_transactions(spark):
    data = [("1", "tx1", "2024-01-01", "100.5"),
            ("1", "tx1", "2024-01-01", "100.5")]
    df = spark.createDataFrame(data, ["customer_id", "transaction_id", "transaction_date", "amount"])
    result = clean_transactions(df)
    assert result.count() == 1

def test_aggregate_customer_spend(spark):
    data = [("1", "tx1", "2024-01-01", 100.0),
            ("1", "tx2", "2024-01-02", 200.0)]
    df = spark.createDataFrame(data, ["customer_id", "transaction_id", "transaction_date", "amount"])
    result = aggregate_customer_spend(df)
    row = result.collect()[0]
    assert row["total_spend"] == 300.0
    assert row["num_transactions"] == 2

def test_get_latest_transaction(spark):
    data = [("1", "tx1", "2024-01-01", 100.0),
            ("1", "tx2", "2024-01-03", 200.0)]
    df = spark.createDataFrame(data, ["customer_id", "transaction_id", "transaction_date", "amount"])
    result = get_latest_transaction(df)
    assert result.collect()[0]["transaction_id"] == "tx2"
