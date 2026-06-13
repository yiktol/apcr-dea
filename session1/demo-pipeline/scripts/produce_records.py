#!/usr/bin/env python3
"""
Standalone producer script for the data pipeline.
Pushes sample records into the Kinesis stream.

Usage:
    python produce_records.py --count 100 --source iot --rate 10
    python produce_records.py --count 50 --source pii
    python produce_records.py --flood  # Trigger throttling
"""
import argparse
import boto3
import json
import uuid
import time
import random
import sys
from datetime import datetime, timezone


def get_stream_name():
    """Get stream name from CDK outputs."""
    try:
        cfn = boto3.client("cloudformation")
        response = cfn.describe_stacks(StackName="DataEngineerPipeline")
        raw = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
        config = {k.replace("Output", "", 1) if k.startswith("Output") else k: v for k, v in raw.items()}
        return config["StreamName"]
    except Exception:
        return "dea-ingestion-stream"


def generate_record(source_type):
    """Generate a record based on source type."""
    now = datetime.now(timezone.utc).isoformat()

    if source_type == "iot":
        return {
            "event_type": "sensor_reading",
            "source": "iot_sensor",
            "device_id": f"sensor-{random.randint(1, 50):03d}",
            "timestamp": now,
            "payload": json.dumps({
                "temperature": round(random.uniform(18.0, 35.0), 2),
                "humidity": round(random.uniform(30.0, 80.0), 2),
                "pressure": round(random.uniform(995.0, 1025.0), 2),
            }),
        }
    elif source_type == "clickstream":
        return {
            "event_type": random.choice(["page_view", "button_click", "add_to_cart"]),
            "source": "web_app",
            "device_id": f"browser-{uuid.uuid4().hex[:8]}",
            "user_id": f"user-{random.randint(1000, 9999)}",
            "timestamp": now,
            "payload": json.dumps({
                "page": random.choice(["/home", "/products", "/cart", "/checkout"]),
                "duration_ms": random.randint(100, 5000),
            }),
        }
    elif source_type == "pii":
        first_names = ["John", "Jane", "Alice", "Bob", "Charlie", "Diana"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia"]
        return {
            "event_type": "user_registration",
            "source": "web_app",
            "user_id": f"user-{random.randint(10000, 99999)}",
            "device_id": f"browser-{uuid.uuid4().hex[:8]}",
            "timestamp": now,
            "payload": json.dumps({
                "name": f"{random.choice(first_names)} {random.choice(last_names)}",
                "email": f"{uuid.uuid4().hex[:6]}@example.com",
                "ssn": f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}",
                "phone": f"{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            }),
        }
    else:  # logs
        return {
            "event_type": "application_log",
            "source": "server_logs",
            "device_id": f"server-{random.randint(1, 10):02d}",
            "timestamp": now,
            "payload": json.dumps({
                "level": random.choice(["INFO", "WARN", "ERROR"]),
                "service": random.choice(["auth", "payment", "order"]),
                "message": f"Processed in {random.randint(5, 500)}ms",
                "status_code": random.choice([200, 200, 200, 400, 500]),
            }),
        }


def main():
    parser = argparse.ArgumentParser(description="Produce records to Kinesis")
    parser.add_argument("--count", type=int, default=10, help="Number of records")
    parser.add_argument("--source", choices=["iot", "clickstream", "logs", "pii", "mixed"], default="mixed")
    parser.add_argument("--rate", type=int, default=10, help="Records per second")
    parser.add_argument("--flood", action="store_true", help="Send 500 records instantly (triggers throttling)")
    args = parser.parse_args()

    stream_name = get_stream_name()
    kinesis = boto3.client("kinesis")

    print(f"Stream: {stream_name}")
    print(f"Source: {args.source}")

    if args.flood:
        print("🔥 FLOOD MODE: Sending 500 records to single partition key...")
        total_failed = 0
        for batch in range(5):
            records = []
            for _ in range(100):
                record = generate_record("clickstream")
                records.append({
                    "Data": json.dumps(record).encode(),
                    "PartitionKey": "flood-test",
                })
            response = kinesis.put_records(StreamName=stream_name, Records=records)
            failed = response.get("FailedRecordCount", 0)
            total_failed += failed
            print(f"  Batch {batch+1}/5: {100-failed} sent, {failed} throttled")

        if total_failed > 0:
            print(f"\n🚨 {total_failed}/500 records THROTTLED!")
            print("This is ProvisionedThroughputExceededException in action.")
        else:
            print("\n✅ All records accepted. Stream has enough capacity.")
        return

    # Normal mode
    sources = ["iot", "clickstream", "logs", "pii"] if args.source == "mixed" else [args.source]
    sent = 0
    failed = 0
    batch_size = min(args.rate, 100)
    delay = batch_size / args.rate if args.rate > 0 else 1

    print(f"Sending {args.count} records at ~{args.rate}/sec...")

    while sent + failed < args.count:
        batch = []
        for _ in range(min(batch_size, args.count - sent - failed)):
            source = random.choice(sources)
            record = generate_record(source)
            partition_key = record.get("user_id") or record.get("device_id", uuid.uuid4().hex[:8])
            batch.append({
                "Data": json.dumps(record).encode(),
                "PartitionKey": partition_key,
            })

        response = kinesis.put_records(StreamName=stream_name, Records=batch)
        batch_failed = response.get("FailedRecordCount", 0)
        sent += len(batch) - batch_failed
        failed += batch_failed

        print(f"\r  Sent: {sent} | Failed: {failed}", end="", flush=True)
        time.sleep(delay)

    print(f"\n\n✅ Done! Sent {sent} records, {failed} failed.")


if __name__ == "__main__":
    main()
