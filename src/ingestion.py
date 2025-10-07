from pyspark.sql import SparkSession

def read_autoloader(spark, source_path: str):
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(source_path)
    )
    return df
