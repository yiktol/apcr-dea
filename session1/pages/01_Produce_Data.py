"""
Page 1: Produce Data into Kinesis

Covers:
- Velocity: Real-time ingestion into Kinesis Data Streams
- Variety: Multiple data source types (IoT, clickstream, logs)
- Sharding and partition keys
- ProvisionedThroughputExceededException (intentional overload)
"""
import streamlit as st
import boto3
import json
import uuid
import time
import random

boto3.setup_default_session(region_name="ap-southeast-1")
from datetime import datetime, timezone

st.set_page_config(page_title="Produce Data", page_icon="⚡", layout="wide")

st.markdown("# ⚡ Produce Data (Velocity + Variety)")
st.markdown("""
Push records into Kinesis Data Streams for real-time ingestion. 
Choose from different data source types to show **Variety**, and observe 
throughput limits to understand **Velocity** constraints.
""")

# Architecture diagram
import os
_diagram = os.path.join(os.path.dirname(__file__), "..", "diagrams", "02_ingestion_layer.png")
if os.path.exists(_diagram):
    with st.expander("📐 Architecture Diagram — Ingestion Layer", expanded=False):
        st.image(_diagram, caption="Ingestion Layer: Producers → Kinesis Shards → Consumers", width="stretch")


def get_kinesis_client():
    return boto3.client("kinesis")


def get_stream_name():
    try:
        cfn = boto3.client("cloudformation")
        response = cfn.describe_stacks(StackName="DataEngineerPipeline")
        raw = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
        config = {k.replace("Output", "", 1) if k.startswith("Output") else k: v for k, v in raw.items()}
        return config.get("StreamName", "dea-ingestion-stream")
    except Exception:
        return "dea-ingestion-stream"


def generate_iot_record():
    """Generate a simulated IoT sensor record."""
    return {
        "event_type": "sensor_reading",
        "source": "iot_sensor",
        "device_id": f"sensor-{random.randint(1, 50):03d}",
        "user_id": None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": json.dumps({
            "temperature": round(random.uniform(18.0, 35.0), 2),
            "humidity": round(random.uniform(30.0, 80.0), 2),
            "pressure": round(random.uniform(995.0, 1025.0), 2),
            "battery_pct": random.randint(10, 100),
        }),
    }


def generate_clickstream_record():
    """Generate a simulated clickstream record."""
    pages = ["/home", "/products", "/cart", "/checkout", "/profile", "/search"]
    actions = ["page_view", "button_click", "form_submit", "scroll", "add_to_cart"]
    return {
        "event_type": random.choice(actions),
        "source": "web_app",
        "device_id": f"browser-{uuid.uuid4().hex[:8]}",
        "user_id": f"user-{random.randint(1000, 9999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": json.dumps({
            "page": random.choice(pages),
            "session_id": uuid.uuid4().hex[:12],
            "duration_ms": random.randint(100, 5000),
            "referrer": random.choice(["google", "direct", "email", "social"]),
        }),
    }


def generate_log_record():
    """Generate a simulated application log record."""
    levels = ["INFO", "WARN", "ERROR", "DEBUG"]
    services = ["auth-service", "payment-service", "order-service", "inventory-service"]
    return {
        "event_type": "application_log",
        "source": "server_logs",
        "device_id": f"server-{random.randint(1, 10):02d}",
        "user_id": None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": json.dumps({
            "level": random.choice(levels),
            "service": random.choice(services),
            "message": f"Request processed in {random.randint(5, 500)}ms",
            "request_id": uuid.uuid4().hex[:16],
            "status_code": random.choice([200, 200, 200, 201, 400, 404, 500]),
        }),
    }


def generate_pii_record():
    """Generate a record containing PII (for Macie)."""
    first_names = ["John", "Jane", "Alice", "Bob", "Charlie"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones"]
    return {
        "event_type": "user_registration",
        "source": "web_app",
        "device_id": f"browser-{uuid.uuid4().hex[:8]}",
        "user_id": f"user-{random.randint(10000, 99999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": json.dumps({
            "name": f"{random.choice(first_names)} {random.choice(last_names)}",
            "email": f"{uuid.uuid4().hex[:6]}@example.com",
            "ssn": f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}",
            "phone": f"{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        }),
    }


GENERATORS = {
    "🌡️ IoT Sensors": generate_iot_record,
    "🖱️ Clickstream": generate_clickstream_record,
    "📋 Application Logs": generate_log_record,
    "⚠️ PII Data (for Macie)": generate_pii_record,
}

# Sidebar controls
st.sidebar.markdown("### Producer Settings")
source_type = st.sidebar.selectbox("Data Source Type", list(GENERATORS.keys()))
batch_size = st.sidebar.slider("Records per batch", 1, 100, 10)
num_batches = st.sidebar.slider("Number of batches", 1, 20, 1)
delay_between = st.sidebar.slider("Delay between batches (sec)", 0.0, 2.0, 0.5)

# Stream info
stream_name = get_stream_name()
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Stream:** `{stream_name}`")

# Main area
col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("### Record Preview")
    generator = GENERATORS[source_type]
    sample = generator()
    st.json(sample)

with col2:
    st.markdown("### Partition Key Strategy")
    if "IoT" in source_type:
        st.info("**Key:** `device_id`\nDistributes sensor readings across shards by device.")
    elif "Clickstream" in source_type:
        st.info("**Key:** `user_id`\nKeeps all events for a user on the same shard (ordering).")
    elif "Logs" in source_type:
        st.info("**Key:** `device_id` (server)\nBalances log load across shards by server.")
    else:
        st.info("**Key:** `user_id`\nGroups user registration events for sequential processing.")

st.markdown("---")

# Send button
if st.button(f"🚀 Send {batch_size * num_batches} Records to Kinesis", type="primary", width="stretch"):
    kinesis = get_kinesis_client()
    progress = st.progress(0)
    status = st.empty()
    metrics_area = st.empty()

    total_sent = 0
    total_failed = 0
    start_time = time.time()

    for batch_num in range(num_batches):
        records = []
        for _ in range(batch_size):
            record = generator()
            # Use appropriate partition key
            if record.get("user_id"):
                partition_key = record["user_id"]
            else:
                partition_key = record.get("device_id", uuid.uuid4().hex[:8])

            records.append({
                "Data": json.dumps(record).encode("utf-8"),
                "PartitionKey": partition_key,
            })

        try:
            response = kinesis.put_records(
                StreamName=stream_name,
                Records=records,
            )
            failed = response.get("FailedRecordCount", 0)
            total_sent += batch_size - failed
            total_failed += failed

            if failed > 0:
                st.warning(f"Batch {batch_num + 1}: {failed} records failed (throttled)")

        except Exception as e:
            st.error(f"Batch {batch_num + 1} error: {e}")
            total_failed += batch_size

        # Update progress
        progress.progress((batch_num + 1) / num_batches)
        elapsed = time.time() - start_time
        rate = total_sent / elapsed if elapsed > 0 else 0
        status.markdown(
            f"**Batch {batch_num + 1}/{num_batches}** | "
            f"Sent: {total_sent} | Failed: {total_failed} | "
            f"Rate: {rate:.0f} records/sec"
        )

        if batch_num < num_batches - 1:
            time.sleep(delay_between)

    # Final summary
    elapsed = time.time() - start_time
    st.success(
        f"✅ Complete! Sent **{total_sent}** records in **{elapsed:.1f}s** "
        f"({total_sent/elapsed:.0f} records/sec)"
    )
    if total_failed > 0:
        st.warning(
            f"⚠️ {total_failed} records failed — this shows "
            "ProvisionedThroughputExceededException! "
            "Try adding shards or reducing batch rate."
        )

# Throughput overload
st.markdown("---")
st.markdown("### 💥 Throughput Overload")
st.markdown("""
Kinesis shards have a **1 MB/sec write** and **2 MB/sec read** limit. 
Push this button to intentionally overwhelm a shard and see the throttling behavior 
discussed in the exam (ProvisionedThroughputExceededException).
""")

if st.button("💥 Flood Stream (500 records, no delay)", type="secondary"):
    kinesis = get_kinesis_client()
    progress = st.progress(0)

    # Send 500 records as fast as possible to a SINGLE partition key
    failed_total = 0
    for i in range(5):
        records = []
        for _ in range(100):
            record = generate_clickstream_record()
            # Force all to same partition key to overwhelm one shard
            records.append({
                "Data": json.dumps(record).encode("utf-8"),
                "PartitionKey": "overload-test",
            })
        try:
            response = kinesis.put_records(StreamName=stream_name, Records=records)
            failed_total += response.get("FailedRecordCount", 0)
        except Exception as e:
            st.error(f"Error: {e}")
            failed_total += 100
        progress.progress((i + 1) / 5)

    if failed_total > 0:
        st.error(
            f"🚨 **{failed_total}/500 records throttled!** "
            "This is the ProvisionedThroughputExceededException in action. "
            "Solutions: add shards, use exponential backoff, or enable enhanced fan-out for reads."
        )
    else:
        st.success("All records accepted — try increasing the stream load or reducing shard count.")
