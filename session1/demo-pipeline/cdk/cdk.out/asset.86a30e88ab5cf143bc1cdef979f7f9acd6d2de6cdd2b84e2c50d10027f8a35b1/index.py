"""
PII Masking Lambda

Triggered by EventBridge when Amazon Macie detects PII in S3 objects.
Downloads the flagged object, masks PII fields, and writes to the clean bucket.

Key concepts:
- Event-driven governance (Veracity)
- Automated compliance response
- Macie → EventBridge → Lambda pattern (Exam Q3)
"""
import json
import os
import re
import hashlib
import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

SOURCE_BUCKET = os.environ["SOURCE_BUCKET"]
MASKED_BUCKET = os.environ["MASKED_BUCKET"]

# PII patterns
PATTERNS = {
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    "phone": re.compile(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b"),
    "credit_card": re.compile(r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b"),
}


def handler(event, context):
    """Handle Macie finding event from EventBridge."""
    logger.info(f"Received event: {json.dumps(event)}")

    # Extract S3 object info from Macie finding
    detail = event.get("detail", {})
    resources = detail.get("resourcesAffected", {})
    s3_object = resources.get("s3Object", {})
    s3_bucket_info = resources.get("s3Bucket", {})

    bucket = s3_bucket_info.get("name", SOURCE_BUCKET)
    key = s3_object.get("key", "")

    if not key:
        logger.error("No S3 key found in Macie finding")
        return {"statusCode": 400, "body": "No S3 key in finding"}

    logger.info(f"Processing PII finding for s3://{bucket}/{key}")

    try:
        # Download the flagged object
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")

        # Mask PII
        masked_content, mask_count = mask_pii(content)

        # Write masked version to clean bucket
        masked_key = f"masked/{key}"
        s3.put_object(
            Bucket=MASKED_BUCKET,
            Key=masked_key,
            Body=masked_content.encode("utf-8"),
            ContentType="application/x-ndjson",
            Metadata={
                "original-bucket": bucket,
                "original-key": key,
                "pii-masks-applied": str(mask_count),
            },
        )

        logger.info(
            f"Masked {mask_count} PII instances. Output: s3://{MASKED_BUCKET}/{masked_key}"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "source": f"s3://{bucket}/{key}",
                "output": f"s3://{MASKED_BUCKET}/{masked_key}",
                "masks_applied": mask_count,
            }),
        }

    except Exception as e:
        logger.error(f"Error processing {key}: {e}")
        raise


def mask_pii(content: str) -> tuple[str, int]:
    """Mask PII patterns in content. Returns (masked_content, count)."""
    total_masks = 0

    for pii_type, pattern in PATTERNS.items():
        matches = pattern.findall(content)
        for match in matches:
            # Hash the value for consistent tokenization
            token = hashlib.sha256(match.encode()).hexdigest()[:8]
            replacement = f"[{pii_type.upper()}_MASKED_{token}]"
            content = content.replace(match, replacement)
            total_masks += 1

    return content, total_masks
