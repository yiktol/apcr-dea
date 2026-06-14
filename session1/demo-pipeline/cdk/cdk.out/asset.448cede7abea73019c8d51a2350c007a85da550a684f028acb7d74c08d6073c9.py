"""
Glue ETL Job: Raw JSON → Curated Parquet

Reads newline-delimited JSON from the raw zone, flattens/cleans the data,
and writes partitioned Parquet to the curated zone.

Data lake pattern:
- Raw zone: landing area (append-only, immutable)
- Curated zone: optimized for analytics (Parquet, partitioned, deduplicated)
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp

# Get job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATA_LAKE_BUCKET"])
bucket = args["DATA_LAKE_BUCKET"]

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Define explicit schema matching the Lambda transform output
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("payload", StringType(), True),
    StructField("processed_at", StringType(), True),
    StructField("kinesis_shard", StringType(), True),
    StructField("arrival_timestamp", DoubleType(), True),
])

# Read raw JSON from S3 with explicit schema
# recursiveFileLookup=true is critical because files are nested: raw/2026/06/13/file.json
raw_path = f"s3://{bucket}/raw/"
print(f"Reading from: {raw_path}")

raw_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .schema(schema)
    .json(raw_path)
)

record_count = raw_df.count()
print(f"Raw records: {record_count}")

if record_count > 0:
    raw_df.printSchema()

    # Transform: add partition columns from timestamp
    transformed_df = (
        raw_df
        .filter(col("timestamp").isNotNull())
        .withColumn("ts", to_timestamp(col("timestamp")))
        .withColumn("year", year(col("ts")).cast("string"))
        .withColumn("month", month(col("ts")).cast("string"))
        .withColumn("day", dayofmonth(col("ts")).cast("string"))
        .drop("ts")
        .dropDuplicates(["event_id"])  # Deduplicate by event_id
    )

    curated_count = transformed_df.count()
    print(f"Curated records (after dedup): {curated_count}")

    # Write as partitioned Parquet to curated zone
    curated_path = f"s3://{bucket}/curated/"
    print(f"Writing to: {curated_path}")

    (
        transformed_df
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(curated_path)
    )

    print("ETL complete — curated zone updated.")

    # Repair table so Athena discovers new partitions
    print("Repairing Athena table partitions...")
    spark.sql("MSCK REPAIR TABLE dea_data_lake.events_curated")
    print("Partitions registered.")
else:
    print("No data found in raw zone. Nothing to process.")

job.commit()
