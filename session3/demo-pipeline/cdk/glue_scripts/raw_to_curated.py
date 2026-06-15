"""
Glue ETL Job: Raw JSON → Curated Parquet

Session 3 demonstration of:
- JSON to Parquet conversion (columnar optimization)
- Partitioning by year/month/day
- Deduplication by order_id
- Snappy compression

Exam concepts:
- Storage format optimization (row vs columnar)
- Partition strategy aligned to query patterns
- Compression for cost reduction
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
    print("No data in raw zone. Exiting.")
    job.commit()
    sys.exit(0)

print(f"Raw records: {raw_df.count()}")

# Deduplicate by order_id
deduped_df = raw_df.dropDuplicates(["order_id"])
print(f"After deduplication: {deduped_df.count()}")

# Add partition columns
partitioned_df = (
    deduped_df
    .withColumn("ts_parsed", to_timestamp(col("timestamp")))
    .withColumn("year", year(col("ts_parsed")).cast("string"))
    .withColumn("month", month(col("ts_parsed")).cast("string"))
    .withColumn("day", dayofmonth(col("ts_parsed")).cast("string"))
    .drop("ts_parsed")
)

# Write as Parquet
print(f"Writing to: {curated_path}")
(
    partitioned_df
    .repartition("year", "month", "day")
    .write
    .mode("append")
    .partitionBy("year", "month", "day")
    .option("compression", "snappy")
    .parquet(curated_path)
)

print("ETL complete!")
job.commit()
