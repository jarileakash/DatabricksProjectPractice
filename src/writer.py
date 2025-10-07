def write_to_delta(df, target_path: str, mode: str = "overwrite"):
    (df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(target_path))
