"""
Page 1: Stream Ingestion — KDS vs Firehose

Covers Task 1.1: Perform data ingestion
- Kinesis Data Streams: real-time, sub-second latency
- Data Firehose: near real-time, buffered delivery
- Resharding: split/merge shards for throughput
- Enhanced Fan-Out for parallel consumers
- Partition key strategies
"""
import streamlit as st
import boto3
import json
import uuid
import time
import random
from datetime import datetime, timezone

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Stream Ingestion", page_icon="⚡", layout="wide")

st.markdown("# ⚡ Stream Ingestion (Task 1.1)")
st.markdown("""
Compare **Kinesis Data Streams** (millisecond latency) vs **Data Firehose** (buffered delivery).
Send the same data to both and observe the difference in behavior.
""")


# ============================================================
# Config
# ============================================================
def get_config():
    if "s2_config" not in st.session_state:
        try:
            cfn = boto3.client("cloudformation")
            response = cfn.describe_stacks(StackName="DeaSession2Pipeline")
            raw = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
            st.session_state.s2_config = raw
        except Exception:
            st.session_state.s2_config = {
                "StreamName": "dea-s2-ingestion-stream",
                "FirehoseName": "dea-s2-firehose",
            }
    return st.session_state.s2_config


# ============================================================
# Data Generators
# ============================================================
def generate_iot_record():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "sensor_reading",
        "source": "iot_sensor",
        "device_id": f"sensor-{random.randint(1, 50):03d}",
        "user_id": None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": json.dumps({
            "temperature": round(random.uniform(18.0, 42.0), 2),
            "humidity": round(random.uniform(20.0, 90.0), 2),
            "pressure": round(random.uniform(990.0, 1030.0), 2),
            "battery_pct": random.randint(5, 100),
        }),
    }


def generate_clickstream_record():
    pages = ["/home", "/products", "/cart", "/checkout", "/profile", "/search"]
    actions = ["page_view", "click", "add_to_cart", "purchase", "scroll"]
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(actions),
        "source": "web_app",
        "device_id": f"browser-{uuid.uuid4().hex[:8]}",
        "user_id": f"user-{random.randint(1000, 9999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": json.dumps({
            "page": random.choice(pages),
            "session_id": uuid.uuid4().hex[:12],
            "duration_ms": random.randint(50, 8000),
        }),
    }


def generate_app_log_record():
    levels = ["INFO", "WARN", "ERROR"]
    services = ["auth-svc", "payment-svc", "order-svc", "inventory-svc"]
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "application_log",
        "source": "server_logs",
        "device_id": f"server-{random.randint(1, 20):02d}",
        "user_id": None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": json.dumps({
            "level": random.choice(levels),
            "service": random.choice(services),
            "latency_ms": random.randint(1, 2000),
            "status_code": random.choice([200, 200, 200, 201, 400, 404, 500]),
        }),
    }


GENERATORS = {
    "🌡️ IoT Sensors": generate_iot_record,
    "🖱️ Clickstream": generate_clickstream_record,
    "📋 Application Logs": generate_app_log_record,
}


# ============================================================
# UI Layout
# ============================================================
config = get_config()
stream_name = config.get("StreamName", "dea-s2-ingestion-stream")
firehose_name = config.get("FirehoseName", "dea-s2-firehose")

# Sidebar
st.sidebar.markdown("### Producer Settings")
source_type = st.sidebar.selectbox("Data Source", list(GENERATORS.keys()))
batch_size = st.sidebar.slider("Records per batch", 1, 100, 25)
st.sidebar.markdown("---")
st.sidebar.markdown(f"**KDS Stream:** `{stream_name}`")
st.sidebar.markdown(f"**Firehose:** `{firehose_name}`")

# Comparison section
st.markdown("## 📊 Side-by-Side Comparison")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    ### Kinesis Data Streams
    | Attribute | Value |
    |-----------|-------|
    | **Latency** | ~70ms (Enhanced Fan-Out) |
    | **Storage** | 24h – 365 days |
    | **Consumers** | Multiple (KCL, Lambda) |
    | **Scaling** | Manual (resharding) |
    | **Ordering** | Per shard (partition key) |
    """)

with col2:
    st.markdown("""
    ### Amazon Data Firehose
    | Attribute | Value |
    |-----------|-------|
    | **Latency** | 60–900 seconds (buffered) |
    | **Storage** | Buffer only (max 128 MB) |
    | **Destinations** | S3, Redshift, OpenSearch |
    | **Scaling** | Automatic |
    | **Transform** | Built-in (Lambda, Glue) |
    """)

st.markdown("---")

# Record preview
st.markdown("### 📝 Record Preview")
generator = GENERATORS[source_type]
sample = generator()
st.json(sample)

st.markdown("---")

# Send to both
st.markdown("## 🚀 Send Data")

col1, col2, col3 = st.columns(3)

with col1:
    send_kds = st.button("⚡ Send to Kinesis Data Streams", type="primary", use_container_width=True)

with col2:
    send_firehose = st.button("🔥 Send to Data Firehose", use_container_width=True)

with col3:
    send_both = st.button("📊 Send to Both (Compare)", use_container_width=True)

# Execution
targets_to_send = []
if send_kds:
    targets_to_send = ["kds"]
elif send_firehose:
    targets_to_send = ["firehose"]
elif send_both:
    targets_to_send = ["kds", "firehose"]

if targets_to_send:
    records = [generator() for _ in range(batch_size)]
    results = {}

    if "kds" in targets_to_send:
        kinesis_client = boto3.client("kinesis")
        start = time.time()
        try:
            response = kinesis_client.put_records(
                StreamName=stream_name,
                Records=[
                    {
                        "Data": json.dumps(r).encode("utf-8"),
                        "PartitionKey": r.get("user_id") or r.get("device_id") or uuid.uuid4().hex[:8],
                    }
                    for r in records
                ],
            )
            elapsed = (time.time() - start) * 1000
            failed = response.get("FailedRecordCount", 0)
            results["kds"] = {
                "sent": batch_size - failed,
                "failed": failed,
                "latency_ms": elapsed,
            }
        except Exception as e:
            results["kds"] = {"error": str(e)}

    if "firehose" in targets_to_send:
        firehose_client = boto3.client("firehose")
        start = time.time()
        try:
            response = firehose_client.put_record_batch(
                DeliveryStreamName=firehose_name,
                Records=[
                    {"Data": json.dumps(r).encode("utf-8")}
                    for r in records
                ],
            )
            elapsed = (time.time() - start) * 1000
            failed = response.get("FailedPutCount", 0)
            results["firehose"] = {
                "sent": batch_size - failed,
                "failed": failed,
                "latency_ms": elapsed,
                "note": "Data will appear in S3 after buffer interval (60s)",
            }
        except Exception as e:
            results["firehose"] = {"error": str(e)}

    # Display results
    st.markdown("### Results")
    col1, col2 = st.columns(2)

    if "kds" in results:
        with col1:
            r = results["kds"]
            if "error" in r:
                st.error(f"KDS Error: {r['error']}")
            else:
                st.success(f"**Kinesis Data Streams**")
                st.metric("Records Sent", r["sent"])
                st.metric("API Latency", f"{r['latency_ms']:.0f} ms")
                if r["failed"] > 0:
                    st.warning(f"{r['failed']} records throttled")

    if "firehose" in results:
        with col2:
            r = results["firehose"]
            if "error" in r:
                st.error(f"Firehose Error: {r['error']}")
            else:
                st.success(f"**Data Firehose**")
                st.metric("Records Sent", r["sent"])
                st.metric("API Latency", f"{r['latency_ms']:.0f} ms")
                st.info("⏱️ Data buffered — will land in S3 in ~60s")

# ============================================================
# Resharding Demo
# ============================================================
st.markdown("---")
st.markdown("## 🔀 Resharding Demo")
st.markdown("""
Kinesis shards have throughput limits: **1 MB/sec write**, **2 MB/sec read** per shard.
Resharding lets you split (scale up) or merge (scale down) shards.
""")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📈 Describe Stream", use_container_width=True):
        try:
            kinesis_client = boto3.client("kinesis")
            response = kinesis_client.describe_stream_summary(StreamName=stream_name)
            summary = response["StreamDescriptionSummary"]
            st.json({
                "StreamName": summary["StreamName"],
                "StreamStatus": summary["StreamStatus"],
                "OpenShardCount": summary["OpenShardCount"],
                "RetentionPeriodHours": summary["RetentionPeriodHours"],
                "StreamModeDetails": summary.get("StreamModeDetails", {}),
            })
        except Exception as e:
            st.error(f"Error: {e}")

with col2:
    st.markdown("""
    **Split shard** = divide 1 shard into 2
    - Doubles capacity of that shard
    - Increases cost proportionally
    """)

with col3:
    st.markdown("""
    **Merge shards** = combine 2 into 1
    - Reduces cost
    - Reduces capacity
    """)

# Throughput overload
st.markdown("---")
st.markdown("## 💥 Throughput Overload Demo")
st.markdown("""
Force all records to a **single partition key** to overwhelm one shard.
This triggers `ProvisionedThroughputExceededException` — a key exam topic.
""")

if st.button("💥 Flood Single Shard (200 records, same partition key)", type="secondary"):
    kinesis_client = boto3.client("kinesis")
    failed_total = 0
    total_sent = 0
    progress = st.progress(0)

    for i in range(4):
        records = [generate_clickstream_record() for _ in range(50)]
        try:
            response = kinesis_client.put_records(
                StreamName=stream_name,
                Records=[
                    {"Data": json.dumps(r).encode("utf-8"), "PartitionKey": "hot-key-overload"}
                    for r in records
                ],
            )
            failed_total += response.get("FailedRecordCount", 0)
            total_sent += 50 - response.get("FailedRecordCount", 0)
        except Exception as e:
            st.error(f"Error: {e}")
            failed_total += 50
        progress.progress((i + 1) / 4)

    if failed_total > 0:
        st.error(
            f"🚨 **{failed_total}/200 records throttled!** "
            "This is `ProvisionedThroughputExceededException`. "
            "Solutions: more shards, better partition keys, or exponential backoff."
        )
    else:
        st.success(f"All {total_sent} records accepted. Try increasing volume to trigger throttling.")

# Key concepts
st.markdown("---")
st.markdown("## 💡 Exam Tips")
st.markdown("""
| Concept | Key Point |
|---------|-----------|
| **KDS vs Firehose** | KDS = custom consumers, ordering, replay. Firehose = managed delivery, auto-scaling. |
| **Partition Key** | Determines which shard gets the record. Hot keys → throttling. |
| **Resharding** | Split increases capacity, merge reduces cost. Can't do both at once. |
| **Enhanced Fan-Out** | Dedicated 2 MB/sec per consumer per shard (vs shared 2 MB/sec). |
| **Replay** | KDS retains data 24h–365 days. Firehose does NOT support replay. |
""")
