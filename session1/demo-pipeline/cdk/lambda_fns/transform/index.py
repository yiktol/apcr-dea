"""
Kinesis → S3 Transform Lambda

Reads batches of records from Kinesis Data Streams, validates and enriches them,
then writes to S3 Data Lake in partitioned JSON format (raw zone).

Key concepts:
- Event-driven processing (Velocity)
- Data validation (Veracity)
- Partitioned storage (Volume)
"""
import json
import os
import base64
import hashlib
import logging
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET = os.environ["DATA_LAKE_BUCKET"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/")
ERRORS_PREFIX = os.environ.get("ERRORS_PREFIX", "errors/")


def handler(event, context):
    """Process a batch of Kinesis records."""
    records = event.get("Records", [])
    logger.info(f"Processing batch of {len(records)} records")

    success_count = 0
    error_count = 0
    now = datetime.now(timezone.utc)

    # Group records by partition for efficient writes
    partitioned = {}
    errors = []

    for record in records:
        try:
            # Decode Kinesis record
            payload_bytes = base64.b64decode(record["kinesis"]["data"])
            payload = json.loads(payload_bytes)

            # Validate required fields
            validate(payload)

            # Enrich with processing metadata
            enriched = enrich(payload, record, now)

            # Determine partition
            partition_key = f"{now.year}/{now.month:02d}/{now.day:02d}"
            if partition_key not in partitioned:
                partitioned[partition_key] = []
            partitioned[partition_key].append(enriched)
            success_count += 1

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            error_count += 1
            errors.append({
                "error": str(e),
                "record_sequence": record["kinesis"].get("sequenceNumber", "unknown"),
                "timestamp": now.isoformat(),
            })
            logger.warning(f"Record validation failed: {e}")

    # Write partitioned records to raw zone
    for partition, batch in partitioned.items():
        write_to_s3(batch, partition, now)

    # Write errors if any
    if errors:
        write_errors(errors, now)

    logger.info(
        f"Batch complete: {success_count} successful, {error_count} errors"
    )

    return {
        "batchItemFailures": [],  # All processed (errors go to error bucket)
    }


def validate(payload: dict) -> None:
    """Validate that a record has required fields."""
    required = ["event_type", "source"]
    missing = [f for f in required if f not in payload]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")


def enrich(payload: dict, kinesis_record: dict, now: datetime) -> dict:
    """Enrich record with processing metadata."""
    # Generate event_id if not present
    if "event_id" not in payload:
        raw = f"{kinesis_record['kinesis']['sequenceNumber']}-{now.isoformat()}"
        payload["event_id"] = hashlib.md5(raw.encode()).hexdigest()

    # Add processing timestamp
    payload["processed_at"] = now.isoformat()
    payload["kinesis_shard"] = kinesis_record["eventID"].split(":")[0] if ":" in kinesis_record.get("eventID", "") else "unknown"
    payload["arrival_timestamp"] = kinesis_record["kinesis"].get("approximateArrivalTimestamp", 0)

    # Ensure timestamp exists
    if "timestamp" not in payload:
        payload["timestamp"] = now.isoformat()

    return payload


def write_to_s3(records: list, partition: str, now: datetime) -> None:
    """Write records to S3 as newline-delimited JSON."""
    # Create a unique key per batch
    batch_id = hashlib.md5(f"{now.isoformat()}-{len(records)}".encode()).hexdigest()[:8]
    key = f"{RAW_PREFIX}{partition}/{now.hour:02d}{now.minute:02d}_{batch_id}.json"

    body = "\n".join(json.dumps(r) for r in records)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    logger.info(f"Wrote {len(records)} records to s3://{BUCKET}/{key}")


def write_errors(errors: list, now: datetime) -> None:
    """Write error records to the errors prefix."""
    batch_id = hashlib.md5(now.isoformat().encode()).hexdigest()[:8]
    key = f"{ERRORS_PREFIX}{now.year}/{now.month:02d}/{now.day:02d}/{batch_id}.json"

    body = "\n".join(json.dumps(e) for e in errors)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    logger.info(f"Wrote {len(errors)} error records to s3://{BUCKET}/{key}")
