"""
Kinesis → S3 Consumer Lambda

Reads records from Kinesis Data Streams, adds metadata,
and writes newline-delimited JSON to S3 raw zone.

Exam concepts:
- Lambda event source mapping with Kinesis
- Batch processing and error handling
- Partition key awareness
"""
import os
import json
import base64
import uuid
import boto3
from datetime import datetime, timezone

s3 = boto3.client("s3")

BUCKET = os.environ["DATA_LAKE_BUCKET"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/")


def handler(event, context):
    records = event.get("Records", [])
    if not records:
        return {"statusCode": 200, "body": "No records"}

    # Group records into a single file per invocation
    processed = []
    failed = 0

    for record in records:
        try:
            payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            data = json.loads(payload)

            # Add ingestion metadata
            data["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            data["_partition_key"] = record["kinesis"]["partitionKey"]
            data["_shard_id"] = record["eventID"].split(":")[0] if ":" in record.get("eventID", "") else "unknown"
            data["_sequence_number"] = record["kinesis"]["sequenceNumber"]

            # Ensure event_id exists
            if "event_id" not in data:
                data["event_id"] = str(uuid.uuid4())

            processed.append(json.dumps(data))
        except Exception as e:
            failed += 1
            print(f"Failed to process record: {e}")

    if processed:
        # Write as newline-delimited JSON
        now = datetime.now(timezone.utc)
        key = (
            f"{RAW_PREFIX}"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"batch-{now.strftime('%H%M%S')}-{uuid.uuid4().hex[:8]}.json"
        )

        body = "\n".join(processed)
        s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": len(processed),
            "failed": failed,
            "batchSize": len(records),
        }),
    }
