def initialize_metadata(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS pipeline_metadata (
            layer STRING,
            city STRING,
            date DATE,
            processed_at TIMESTAMP
        )
        USING PARQUET
    """)