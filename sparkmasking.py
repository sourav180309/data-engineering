from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
import json

# Ingestion Module
def ingest_data(spark: SparkSession, source: str, format: str, options: dict, is_streaming: bool):
    if is_streaming:
        df = spark.readStream.format(format).options(**options).load(source)
    else:
        df = spark.read.format(format).options(**options).load(source)
    return df

# Processing Module
def process_data(df):
    # Example transformation
    df = df.withColumnRenamed("old_name", "new_name")
    return df

# Masking Module
def mask_data(df, columns_to_mask):
    for column in columns_to_mask:
        df = df.withColumn(column, when(col(column).isNotNull(), lit('****')).otherwise(col(column)))
    return df

# Metadata Module
class MetadataManager:
    def __init__(self, metadata_store):
        self.metadata_store = metadata_store
    
    def save_metadata(self, metadata):
        with open(self.metadata_store, 'w') as f:
            json.dump(metadata, f)
    
    def load_metadata(self):
        with open(self.metadata_store, 'r') as f:
            return json.load(f)

# Storage Module
def save_to_data_lake(df, path):
    df.write.parquet(path)

# Access Module
def query_data_lake(spark: SparkSession, path: str):
    df = spark.read.parquet(path)
    df.createOrReplaceTempView("data_lake_table")
    result = spark.sql("SELECT * FROM data_lake_table LIMIT 10")
    result.show()

# Example Usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName("SparkDataEngineeringFramework").getOrCreate()

    # Ingest Data
    source = "s3://rawdata_eds/revops_stage/adobe/"
    format = "csv"
    options = {"header": "true", "inferSchema": "true"}
    is_streaming = False  # Set to True for streaming ingestion
    df = ingest_data(spark, source, format, options, is_streaming)
    
    if is_streaming:
        query = df.writeStream.format("console").start()
        query.awaitTermination()
    else:
        df.show()

    # Mask Sensitive Data
    columns_to_mask = ["sensitive_column1", "sensitive_column2"]
    masked_df = mask_data(df, columns_to_mask)

    # Process Data
    processed_df = process_data(masked_df)

    # Save Metadata
    metadata_manager = MetadataManager("path/to/metadata_store.json")
    metadata = {"source": source, "transformation": "rename column", "masking": columns_to_mask}
    metadata_manager.save_metadata(metadata)
    loaded_metadata = metadata_manager.load_metadata()
    print(loaded_metadata)

    # Save Processed Data to Data Lake
    save_to_data_lake(processed_df, "s3://path/to/data_lake")

    # Query Data Lake
    query_data_lake(spark, "s3://path/to/data_lake")
