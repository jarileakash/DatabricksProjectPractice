def write_to_delta(df, target_path: str, mode: str = "append"):
    (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{target_path}/_checkpoints")  # required for streaming
        .outputMode(mode)  # append or complete
        .start(target_path)
    )
