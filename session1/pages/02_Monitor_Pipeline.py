"""
Page 2: Monitor Pipeline

Covers:
- Real-time monitoring of Kinesis throughput
- Lambda invocation metrics
- S3 object counts (data landing)
- CloudWatch integration
"""
import streamlit as st
import boto3
import time
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta, timezone

st.set_page_config(page_title="Monitor Pipeline", page_icon="📈", layout="wide")

st.markdown("# 📈 Monitor Pipeline (Velocity)")
st.markdown("""
Real-time view of data flowing through the pipeline. Watch records move from 
Kinesis → Lambda → S3, and observe metrics like throughput, latency, and error rates.
""")


def get_config():
    try:
        cfn = boto3.client("cloudformation")
        response = cfn.describe_stacks(StackName="DataEngineerPipeline")
        raw = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
        return {k.replace("Output", "", 1) if k.startswith("Output") else k: v for k, v in raw.items()}
    except Exception:
        return {}


def get_stream_metrics(stream_name, minutes=30):
    """Get Kinesis stream metrics from CloudWatch."""
    cw = boto3.client("cloudwatch")
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=minutes)

    metrics = {}
    metric_configs = [
        ("IncomingRecords", "Sum"),
        ("IncomingBytes", "Sum"),
        ("GetRecords.IteratorAgeMilliseconds", "Average"),
        ("WriteProvisionedThroughputExceeded", "Sum"),
    ]

    for metric_name, stat in metric_configs:
        try:
            response = cw.get_metric_statistics(
                Namespace="AWS/Kinesis",
                MetricName=metric_name,
                Dimensions=[{"Name": "StreamName", "Value": stream_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=60,
                Statistics=[stat],
            )
            datapoints = sorted(response["Datapoints"], key=lambda x: x["Timestamp"])
            metrics[metric_name] = datapoints
        except Exception:
            metrics[metric_name] = []

    return metrics


def get_lambda_metrics(function_name, minutes=30):
    """Get Lambda function metrics."""
    cw = boto3.client("cloudwatch")
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=minutes)

    metrics = {}
    metric_configs = [
        ("Invocations", "Sum"),
        ("Duration", "Average"),
        ("Errors", "Sum"),
        ("ConcurrentExecutions", "Maximum"),
    ]

    for metric_name, stat in metric_configs:
        try:
            response = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName=metric_name,
                Dimensions=[{"Name": "FunctionName", "Value": function_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=60,
                Statistics=[stat],
            )
            datapoints = sorted(response["Datapoints"], key=lambda x: x["Timestamp"])
            metrics[metric_name] = datapoints
        except Exception:
            metrics[metric_name] = []

    return metrics


def get_s3_object_count(bucket, prefix):
    """Count objects in S3 prefix."""
    try:
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        count = 0
        total_size = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                count += 1
                total_size += obj["Size"]
        return count, total_size
    except Exception:
        return 0, 0


# Load config
config = get_config()
stream_name = config.get("StreamName", "dea-ingestion-stream")
transform_fn = config.get("TransformFunctionName", "dea-stream-transform")
bucket = config.get("DataLakeBucket", "")

# Time range selector
minutes = st.sidebar.slider("Time window (minutes)", 5, 120, 30)

# Auto-refresh
auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
if auto_refresh:
    st.sidebar.markdown("🔄 Refreshing every 30 seconds...")
    time.sleep(30)
    st.rerun()

st.markdown("---")

# S3 Data Lake stats
st.markdown("## 📊 Data Lake Status")
if bucket:
    col1, col2, col3, col4 = st.columns(4)
    raw_count, raw_size = get_s3_object_count(bucket, "raw/")
    curated_count, curated_size = get_s3_object_count(bucket, "curated/")
    error_count, error_size = get_s3_object_count(bucket, "errors/")

    with col1:
        st.metric("Raw Zone Objects", raw_count, help="Newline-delimited JSON files")
    with col2:
        st.metric("Raw Zone Size", f"{raw_size / 1024:.1f} KB")
    with col3:
        st.metric("Curated Zone Objects", curated_count, help="Parquet files")
    with col4:
        st.metric("Error Objects", error_count, delta=f"-{error_count}" if error_count > 0 else None)
else:
    st.warning("Pipeline not deployed. Run `cdk deploy` first.")

st.markdown("---")

# Kinesis metrics
st.markdown("## ⚡ Kinesis Data Stream Metrics")
kinesis_metrics = get_stream_metrics(stream_name, minutes)

col1, col2 = st.columns(2)

with col1:
    # Incoming records chart
    incoming = kinesis_metrics.get("IncomingRecords", [])
    if incoming:
        df = pd.DataFrame(incoming)
        fig = px.area(
            df, x="Timestamp", y="Sum",
            title="Incoming Records (per minute)",
            labels={"Sum": "Records", "Timestamp": "Time"},
        )
        fig.update_traces(fill="tozeroy", line_color="#FF9900")
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No incoming records data yet. Produce some data first!")

with col2:
    # Throttling
    throttled = kinesis_metrics.get("WriteProvisionedThroughputExceeded", [])
    if throttled:
        df = pd.DataFrame(throttled)
        fig = px.bar(
            df, x="Timestamp", y="Sum",
            title="Write Throttle Events",
            labels={"Sum": "Throttled Records", "Timestamp": "Time"},
            color_discrete_sequence=["#c62828"],
        )
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.success("✅ No throttling detected")

# Iterator age (consumer lag)
iterator_age = kinesis_metrics.get("GetRecords.IteratorAgeMilliseconds", [])
if iterator_age:
    df = pd.DataFrame(iterator_age)
    latest_age = df["Average"].iloc[-1] if len(df) > 0 else 0
    st.metric(
        "Consumer Lag (Iterator Age)",
        f"{latest_age:.0f} ms",
        help="How far behind the consumer is from the latest record. Lower is better.",
    )

st.markdown("---")

# Lambda metrics
st.markdown("## 🔧 Lambda Transform Metrics")
lambda_metrics = get_lambda_metrics(transform_fn, minutes)

col1, col2, col3 = st.columns(3)

with col1:
    invocations = lambda_metrics.get("Invocations", [])
    total_invocations = sum(d.get("Sum", 0) for d in invocations)
    st.metric("Total Invocations", int(total_invocations))

with col2:
    duration = lambda_metrics.get("Duration", [])
    avg_duration = (
        sum(d.get("Average", 0) for d in duration) / len(duration) if duration else 0
    )
    st.metric("Avg Duration", f"{avg_duration:.0f} ms")

with col3:
    errors = lambda_metrics.get("Errors", [])
    total_errors = sum(d.get("Sum", 0) for d in errors)
    st.metric("Errors", int(total_errors), delta=f"+{int(total_errors)}" if total_errors > 0 else None, delta_color="inverse")

# Invocations chart
if invocations:
    df = pd.DataFrame(invocations)
    fig = px.bar(
        df, x="Timestamp", y="Sum",
        title="Lambda Invocations (per minute)",
        labels={"Sum": "Invocations", "Timestamp": "Time"},
        color_discrete_sequence=["#1565c0"],
    )
    fig.update_layout(height=250)
    st.plotly_chart(fig, use_container_width=True)

# Pipeline flow visualization
st.markdown("---")
st.markdown("## 🔄 Pipeline Flow")
import os

col1, col2 = st.columns(2)
with col1:
    _diagram = os.path.join(os.path.dirname(__file__), "..", "diagrams", "03_processing_layer.png")
    if os.path.exists(_diagram):
        st.image(_diagram, caption="Processing: Kinesis → Lambda → S3", use_column_width=True)

with col2:
    _diagram2 = os.path.join(os.path.dirname(__file__), "..", "diagrams", "07_networking_layer.png")
    if os.path.exists(_diagram2):
        st.image(_diagram2, caption="Networking: VPC + Private Subnets", use_column_width=True)
