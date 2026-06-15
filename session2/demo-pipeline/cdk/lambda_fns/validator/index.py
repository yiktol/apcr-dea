"""
Validator Lambda — Step Functions first step

Checks that raw zone has data to process and validates
basic schema requirements before kicking off the Glue ETL job.

Exam concepts:
- Step Functions task states
- Input/output processing
- Validation before transformation
"""
import os
import json
import boto3
from datetime import datetime, timezone

s3 = boto3.client("s3")

BUCKET = os.environ["DATA_LAKE_BUCKET"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/")


def handler(event, context):
    """Validate that raw zone has processable data."""

    # List objects in raw zone
    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=RAW_PREFIX,
        MaxKeys=1000,
    )

    objects = response.get("Contents", [])
    total_size = sum(obj["Size"] for obj in objects)
    file_count = len(objects)

    if file_count == 0:
        return {
            "status": "SKIP",
            "message": "No files in raw zone to process",
            "fileCount": 0,
            "totalSizeBytes": 0,
            "validatedAt": datetime.now(timezone.utc).isoformat(),
        }

    # Sample first file to validate schema
    sample_key = objects[0]["Key"]
    sample_obj = s3.get_object(Bucket=BUCKET, Key=sample_key)
    first_line = sample_obj["Body"].read(4096).decode("utf-8").split("\n")[0]

    try:
        record = json.loads(first_line)
        required_fields = ["event_type", "source", "timestamp"]
        missing = [f for f in required_fields if f not in record]

        if missing:
            return {
                "status": "INVALID",
                "message": f"Missing required fields: {missing}",
                "fileCount": file_count,
                "totalSizeBytes": total_size,
                "sampleKey": sample_key,
                "validatedAt": datetime.now(timezone.utc).isoformat(),
            }
    except json.JSONDecodeError as e:
        return {
            "status": "INVALID",
            "message": f"Invalid JSON in {sample_key}: {str(e)}",
            "fileCount": file_count,
            "totalSizeBytes": total_size,
            "validatedAt": datetime.now(timezone.utc).isoformat(),
        }

    return {
        "status": "VALID",
        "message": f"Validated {file_count} files ({total_size / 1024:.1f} KB)",
        "fileCount": file_count,
        "totalSizeBytes": total_size,
        "bucket": BUCKET,
        "rawPrefix": RAW_PREFIX,
        "validatedAt": datetime.now(timezone.utc).isoformat(),
    }
