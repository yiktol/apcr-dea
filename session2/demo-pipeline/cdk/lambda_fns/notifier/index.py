"""
Notifier Lambda — Step Functions final step

Sends pipeline completion notifications via SNS.
Demonstrates alert integration in data pipelines.

Exam concepts:
- SNS for pipeline notifications
- Step Functions success/failure paths
- Error handling patterns
"""
import os
import json
import boto3
from datetime import datetime, timezone

sns = boto3.client("sns")

TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def handler(event, context):
    """Send pipeline notification."""
    status = event.get("status", "UNKNOWN")
    message = event.get("message", "No message provided")
    job_name = event.get("jobName", "unknown")

    timestamp = datetime.now(timezone.utc).isoformat()

    subject = f"[DEA Pipeline] {status}: {job_name}"

    body = json.dumps({
        "pipeline": "dea-s2-etl-orchestrator",
        "status": status,
        "message": message,
        "jobName": job_name,
        "timestamp": timestamp,
        "executionContext": {
            "functionName": context.function_name,
            "requestId": context.aws_request_id,
        },
    }, indent=2)

    try:
        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject=subject[:100],  # SNS subject limit
            Message=body,
        )
    except Exception as e:
        print(f"Failed to publish SNS notification: {e}")

    return {
        "notified": True,
        "status": status,
        "timestamp": timestamp,
        "topicArn": TOPIC_ARN,
    }
