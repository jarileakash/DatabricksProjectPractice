from pyspark.sql import SparkSession

def read_autoloader(spark, source_path: str):
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schema/transactions/")  # fix here
        .load(source_path)
    )
    return df
 


#read_autoloader(spark, "/FileStore/raw/transactions/")
