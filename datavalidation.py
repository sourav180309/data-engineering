from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
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

# Validation Module
def validate_data(df, schema):
    for field in schema.fields:
        if field.dataType == StringType():
            df = df.withColumn(field.name, when(col(field.name).isNull(), lit("UNKNOWN")).otherwise(col(field.name)))
        elif field.dataType == IntegerType():
            df = df.withColumn(field.name, when(col(field.name).isNull(), lit(0)).otherwise(col(field.name)))
    return df

# Quality Check Module
def check_data_quality(df, checks):
    quality_issues = {}
    for column, condition in checks.items():
        failed_records = df.filter(~condition).count()
        if failed_records > 0:
            quality_issues[column] = failed_records
    return quality_issues

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

    # Define Schema for Validation
    schema = StructType([
        StructField("column1", StringType(), True),
        StructField("column2", IntegerType(), True),
        # Add more fields as required
    ])

    # Validate Data
    validated_df = validate_data(masked_df, schema)

    # Define Data Quality Checks
    quality_checks = {
        "column1": col("column1").isNotNull(),
        "column2": col("column2") > 0
        # Add more checks as required
    }

    # Check Data Quality
    quality_issues = check_data_quality(validated_df, quality_checks)
    if quality_issues:
        print(f"Data quality issues found: {quality_issues}")
    else:
        print("No data quality issues found.")

    # Process Data
    processed_df = process_data(validated_df)

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
