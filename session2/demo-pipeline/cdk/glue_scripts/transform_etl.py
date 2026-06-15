"""
Glue ETL Job: Raw JSON → Curated Parquet

Session 2 demonstration of:
- JSON to columnar (Parquet) conversion
- Data deduplication by event_id
- Partition by year/month/day for query performance
- Snappy compression for cost optimization

Exam concepts:
- AWS Glue ETL with PySpark
- File format optimization (JSON vs Parquet)
- Partitioning strategy for Athena cost savings
- Data quality: deduplication
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp

# Initialize
args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATA_LAKE_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args["DATA_LAKE_BUCKET"]
raw_path = f"s3://{bucket}/raw/"
curated_path = f"s3://{bucket}/curated/"

# Read raw JSON
print(f"Reading from: {raw_path}")
raw_df = spark.read.json(raw_path)

if raw_df.count() == 0:
    print("No data found in raw zone. Exiting.")
    job.commit()
    sys.exit(0)

print(f"Raw records: {raw_df.count()}")
print(f"Raw schema:")
raw_df.printSchema()

# Deduplicate by event_id
deduped_df = raw_df.dropDuplicates(["event_id"])
print(f"After deduplication: {deduped_df.count()}")

# Add partition columns from timestamp
partitioned_df = (
    deduped_df
    .withColumn("ts_parsed", to_timestamp(col("timestamp")))
    .withColumn("year", year(col("ts_parsed")).cast("string"))
    .withColumn("month", month(col("ts_parsed")).cast("string"))
    .withColumn("day", dayofmonth(col("ts_parsed")).cast("string"))
    .drop("ts_parsed")
)

# Select final columns (excluding internal metadata fields)
output_columns = [
    "event_id", "event_type", "source", "timestamp",
    "device_id", "user_id", "payload",
    "year", "month", "day",
]
final_df = partitioned_df.select(
    *[col(c) for c in output_columns if c in partitioned_df.columns]
)

# Write as Parquet with Snappy compression, partitioned
print(f"Writing curated Parquet to: {curated_path}")
(
    final_df
    .repartition("year", "month", "day")
    .write
    .mode("append")
    .partitionBy("year", "month", "day")
    .option("compression", "snappy")
    .parquet(curated_path)
)

print("ETL job completed successfully!")
job.commit()
