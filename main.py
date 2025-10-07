from pyspark.sql import SparkSession
from src.ingestion import read_autoloader
from src.transformations import clean_transactions, aggregate_customer_spend, get_latest_transaction
from src.writer import write_to_delta

spark = SparkSession.builder.appName("CustomerTransactionsPipeline").getOrCreate()

source_path = "dbfs:/FileStore/raw/transactions/"
silver_path = "dbfs:/FileStore/silver_delta/"
gold_path = "dbfs:/FileStore/gold_delta/"

df_raw = read_autoloader(spark, source_path)
df_clean = clean_transactions(df_raw)
df_latest = get_latest_transaction(df_clean)
df_agg = aggregate_customer_spend(df_clean)

write_to_delta(df_latest, silver_path)
write_to_delta(df_agg, gold_path)

spark.streams.awaitAnyTermination()



