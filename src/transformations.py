from pyspark.sql import functions as F
from pyspark.sql.window import Window

def clean_transactions(df):
    df_clean = df.dropDuplicates(["customer_id", "transaction_id"])
    df_clean = (
        df_clean.withColumn("amount", F.col("amount").cast("double"))
                .withColumn("transaction_date", F.to_date("transaction_date"))
    )
    return df_clean

def aggregate_customer_spend(df):
    return df.groupBy("customer_id").agg(
        F.sum("amount").alias("total_spend"),
        F.countDistinct("transaction_id").alias("num_transactions")
    )

def get_latest_transaction(df):
    w = Window.partitionBy("customer_id").orderBy(F.col("transaction_date").desc())
    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )
