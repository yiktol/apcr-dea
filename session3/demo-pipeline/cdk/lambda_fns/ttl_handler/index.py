"""
DynamoDB Streams → Lambda: Capture TTL deletions

Logs expired items to S3 for audit and demonstrates
the CDC (Change Data Capture) pattern with DynamoDB Streams.

Exam concepts:
- DynamoDB TTL auto-deletion (no write capacity consumed)
- DynamoDB Streams captures deletion events
- Event-driven architecture with Lambda
"""
import os
import json
import boto3
from datetime import datetime, timezone

s3 = boto3.client("s3")

BUCKET = os.environ["DATA_LAKE_BUCKET"]


def handler(event, context):
    """Process DynamoDB Stream events for TTL-expired items."""
    expired_items = []

    for record in event.get("Records", []):
        # TTL deletions have eventName=REMOVE and userIdentity with principalId containing "dynamodb.amazonaws.com"
        if record["eventName"] == "REMOVE":
            old_image = record.get("dynamodb", {}).get("OldImage", {})

            # Check if this was a TTL deletion
            user_identity = record.get("userIdentity", {})
            is_ttl = user_identity.get("type") == "Service" and \
                     user_identity.get("principalId") == "dynamodb.amazonaws.com"

            item = {
                "order_id": old_image.get("order_id", {}).get("S", ""),
                "timestamp": old_image.get("timestamp", {}).get("S", ""),
                "expired_at": datetime.now(timezone.utc).isoformat(),
                "was_ttl_deletion": is_ttl,
                "event_id": record.get("eventID", ""),
            }
            expired_items.append(item)

    if expired_items:
        # Write expired items to S3 for audit trail
        now = datetime.now(timezone.utc)
        key = (
            f"ttl-audit/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"expired-{now.strftime('%H%M%S')}-{context.aws_request_id[:8]}.json"
        )
        body = "\n".join(json.dumps(item) for item in expired_items)
        s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))

    return {
        "processed": len(expired_items),
        "total_records": len(event.get("Records", [])),
    }
