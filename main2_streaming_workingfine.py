from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("CustomerTransactionsPipeline").getOrCreate()

# Paths
source_path = "dbfs:/FileStore/raw/transactions/"
schema_path = "dbfs:/FileStore/schema/transactions/"
bronze_path = "dbfs:/FileStore/bronze_delta/"
silver_path = "dbfs:/FileStore/silver_delta/"
gold_path = "dbfs:/FileStore/gold_delta/"

# 1️⃣ Read Stream (Autoloader → Bronze)
df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(source_path)
)

# Write raw stream to Bronze (for traceability)
(
    df_raw.writeStream
    .format("delta")
    .option("checkpointLocation", f"{bronze_path}/_checkpoints")
    .outputMode("append")
    .start(bronze_path)
)

# 2️⃣ Clean Transformations (still streaming) → Silver
df_clean = (
    df_raw.dropDuplicates(["customer_id", "transaction_id"])
          .withColumn("amount", F.col("amount").cast("double"))
          .withColumn("transaction_date", F.to_date("transaction_date"))
)

# Write clean stream to Silver
(
    df_clean.writeStream
    .format("delta")
    .option("checkpointLocation", f"{silver_path}/_checkpoints")
    .outputMode("append")
    .start(silver_path)
)

# 3️⃣ Batch Step (Silver → Gold)
#    ⏳ Wait for a few seconds to let the Silver stream process first
import time
time.sleep(20)  # (Optional, allows streaming to ingest initial batch)

# Read from Silver (static batch mode)
df_silver = spark.read.format("delta").load(silver_path)

# Apply window functions safely in batch mode
window_spec = Window.partitionBy("customer_id").orderBy(F.col("transaction_date").desc())
df_latest = df_silver.withColumn("rn", F.row_number().over(window_spec)).filter("rn = 1").drop("rn")

df_agg = df_silver.groupBy("customer_id").agg(
    F.sum("amount").alias("total_spend"),
    F.countDistinct("transaction_id").alias("num_transactions")
)

# Join both results → Gold
df_gold = df_latest.join(df_agg, "customer_id", "inner")

# Write to Gold Delta (batch write)
df_gold.write.format("delta").mode("overwrite").save(gold_path)

print("✅ Gold table successfully updated.")

# 4️⃣ Keep the streaming ingestion alive (Bronze & Silver)
spark.streams.awaitAnyTermination()
