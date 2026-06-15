"""
Page 1: Ingest Data — IoT Device Simulator → IoT Core → Kinesis

Pipeline step: Self-driving cars → IoT Core → IoT Rule → Kinesis Data Streams → Lambda → S3 raw/
Also covers: KDS vs Firehose comparison, resharding, throttling demo
"""
import streamlit as st
import boto3
import json
import uuid
import time
import random
from datetime import datetime, timezone

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Ingest Data", page_icon="📡", layout="wide")

st.markdown("# 📡 Step 1: Ingest Data")
st.markdown("""
Self-driving cars publish telemetry via **IoT Core**. An IoT Rule routes messages 
to **Kinesis Data Streams**, which feeds **Data Firehose** to deliver data to S3 as raw JSON.
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


config = get_config()
stream_name = config.get("StreamName", "dea-s2-ingestion-stream")
firehose_name = config.get("FirehoseName", "dea-s2-firehose")

# ============================================================
# Architecture
# ============================================================
st.markdown("## Architecture")

import os
_diagram = os.path.join(os.path.dirname(__file__), "..", "diagrams", "01_ingest.png")
if os.path.exists(_diagram):
    st.image(_diagram, caption="Ingestion: IoT Devices → IoT Core → IoT Rule → Kinesis → Firehose → S3", use_container_width=True)
else:
    st.info("Run `create_diagrams.py` in `diagrams/` to generate architecture diagrams.")

st.markdown("---")

# ============================================================
# IoT Device Simulator
# ============================================================
st.markdown("## 🚗 Vehicle Telemetry Ingestion")

st.markdown("""
The **IoT Device Simulator** generates realistic connected vehicle telemetry and publishes 
to IoT Core. The IoT Rule (`dea_s2_to_kinesis`) routes messages into the Kinesis stream automatically.
""")

# Check for simulator stack
def get_simulator_url():
    try:
        cfn = boto3.client("cloudformation")
        paginator = cfn.get_paginator("list_stacks")
        for page in paginator.paginate(StackStatusFilter=["CREATE_COMPLETE", "UPDATE_COMPLETE"]):
            for stack in page["StackSummaries"]:
                if "iot" in stack["StackName"].lower() and "simulator" in stack["StackName"].lower():
                    response = cfn.describe_stacks(StackName=stack["StackName"])
                    outputs = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0].get("Outputs", [])}
                    return outputs.get("ConsoleURL", outputs.get("Console URL", ""))
        return ""
    except Exception:
        return ""


col1, col2 = st.columns([1, 1])

with col1:
    simulator_url = get_simulator_url()
    if simulator_url:
        st.success("✅ IoT Device Simulator is deployed")
        st.markdown(f"### [🔗 Open Simulator Console]({simulator_url})")
    else:
        st.warning("IoT Device Simulator not detected")

    st.markdown("""
    **Steps to generate data:**
    1. Log in to the simulator console
    2. Go to **Device Types** → Create "Connected Vehicle"
    3. Set topic: `iot/simulator/vehicle`
    4. Add attributes: vehiclespeed, enginespeed, fuellevel, oiltemp
    5. Go to **Simulations** → Start with 10-50 devices
    6. Data flows automatically: IoT Core → Kinesis → Firehose → S3
    """)

with col2:
    st.markdown("**Sample vehicle telemetry payload:**")
    st.json({
        "timestamp": "2024-06-15T10:30:00.000Z",
        "trip_id": "trip-427",
        "vin": "VIN38291",
        "brake": 0,
        "steeringwheelangle": 12,
        "torqueattransmission": 250.5,
        "enginespeed": 2800.0,
        "vehiclespeed": 65.3,
        "acceleration": 1.2,
        "parkingbrakestatus": False,
        "brakepedalstatus": False,
        "transmissiongearposition": "drive",
        "gearleverposition": "D",
        "odometer": 45023.7,
        "ignitionstatus": "run",
        "fuellevel": 72.1,
        "fuelconsumedsincerestart": 3.45,
        "oiltemp": 95.2,
        "location": {"latitude": 1.3521, "longitude": 103.8198},
    })

# IoT Rule status
st.markdown("---")
st.markdown("### 🔌 IoT Rule Status")

try:
    iot = boto3.client("iot")
    rule_response = iot.get_topic_rule(ruleName="dea_s2_to_kinesis")
    rule = rule_response.get("rule", {})
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Rule", "dea_s2_to_kinesis")
    with col2:
        st.metric("Status", "Active" if not rule.get("ruleDisabled", False) else "Disabled")
    with col3:
        st.metric("Target", stream_name)
    st.code(rule.get("sql", ""), language="sql")
except Exception:
    st.info("IoT Rule `dea_s2_to_kinesis` not found.")

st.markdown("---")

# ============================================================
# KDS vs Firehose Comparison
# ============================================================
st.markdown("## 📊 Kinesis Data Streams vs Data Firehose")

st.markdown("""
| Attribute | Kinesis Data Streams | Data Firehose |
|-----------|---------------------|---------------|
| **Latency** | ~70ms (Enhanced Fan-Out) | 60–900 seconds (buffered) |
| **Consumers** | Multiple (Lambda, KCL, etc.) | Single destination |
| **Replay** | Yes (24h–365d retention) | No replay |
| **Scaling** | Manual (resharding) | Automatic |
| **Transform** | Custom (consumer code) | Built-in (Lambda, format conversion) |
| **Ordering** | Per shard (partition key) | No ordering guarantee |
| **Use case** | Real-time processing, multiple consumers | Delivery to S3/Redshift/OpenSearch |
""")

st.markdown("---")

# ============================================================
# Exam Tips
# ============================================================
st.markdown("## 💡 Exam Tips")
st.markdown("""
| Concept | Key Point |
|---------|-----------|
| **IoT Rule → Kinesis** | Common pattern for device ingestion into analytics pipelines |
| **Partition key** | Determines shard routing. Hot keys → throttling |
| **KDS replay** | Data retained 24h–365d. Consumers can re-read. Firehose cannot. |
| **Enhanced Fan-Out** | Dedicated 2 MB/sec per consumer per shard |
| **Lambda ESM** | Event Source Mapping tracks checkpoint per shard. Not Lambda itself. |
| **Resharding** | Split = scale up (more cost). Merge = scale down (less cost). |
""")
