"""
Page 2: ETL Transform — Glue Job Execution

Covers Task 1.2: Transform and process data
- Glue ETL: JSON → Parquet with Snappy compression
- Deduplication by event_id
- Partitioning by year/month/day
- Compression ratio comparison
- Data lake zones: raw → curated
"""
import streamlit as st
import boto3
import json
import time
import pandas as pd
from datetime import datetime, timezone

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="ETL Transform", page_icon="🔄", layout="wide")

st.markdown("# 🔄 ETL Transform (Task 1.2)")
st.markdown("""
Run the Glue ETL job to transform raw JSON into optimized **Parquet** in the curated zone.
Watch deduplication, partitioning, and compression in action.
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
            st.session_state.s2_config = {}
    return st.session_state.s2_config


def get_s3_zone_stats(bucket, prefix):
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


config = get_config()
bucket = config.get("DataLakeBucket", "")
glue_job_name = config.get("GlueJobName", "dea-s2-transform-etl")


# ============================================================
# Data Lake Zones Status
# ============================================================
st.markdown("## 📊 Data Lake Zones")

if bucket:
    col1, col2, col3 = st.columns(3)

    raw_count, raw_size = get_s3_zone_stats(bucket, "raw/")
    curated_count, curated_size = get_s3_zone_stats(bucket, "curated/")

    with col1:
        st.metric("🟠 Raw Zone (JSON)", f"{raw_count} files")
        st.caption(f"{raw_size / 1024:.1f} KB — Newline-delimited JSON")

    with col2:
        st.metric("🟢 Curated Zone (Parquet)", f"{curated_count} files")
        st.caption(f"{curated_size / 1024:.1f} KB — Snappy compressed, partitioned")

    with col3:
        if raw_size > 0 and curated_size > 0:
            ratio = raw_size / curated_size
            savings = ((raw_size - curated_size) / raw_size) * 100
            st.metric("📉 Compression Ratio", f"{ratio:.1f}x")
            st.caption(f"{savings:.0f}% storage savings with Parquet")
        else:
            st.metric("📉 Compression Ratio", "N/A")
            st.caption("Run the ETL job to see compression results")
else:
    st.warning("Pipeline not deployed. Run `cdk deploy` first.")

st.markdown("---")

# ============================================================
# ETL Process Explanation
# ============================================================
st.markdown("## 🔧 What the ETL Job Does")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown("""
    ### 1️⃣ Extract
    Read all JSON files from `raw/` zone in S3
    """)
with col2:
    st.markdown("""
    ### 2️⃣ Deduplicate
    Remove duplicates by `event_id` using Spark `dropDuplicates()`
    """)
with col3:
    st.markdown("""
    ### 3️⃣ Partition
    Add `year/month/day` columns from timestamp for query optimization
    """)
with col4:
    st.markdown("""
    ### 4️⃣ Load
    Write as Snappy-compressed Parquet to `curated/` zone
    """)

st.markdown("---")

# ============================================================
# Run Glue Job
# ============================================================
st.markdown("## 🚀 Run ETL Job")

col1, col2 = st.columns([1, 2])

with col1:
    if st.button("▶️ Start Glue ETL Job", type="primary", use_container_width=True):
        try:
            glue_client = boto3.client("glue")
            response = glue_client.start_job_run(
                JobName=glue_job_name,
                Arguments={"--DATA_LAKE_BUCKET": bucket},
            )
            st.session_state["s2_glue_run_id"] = response["JobRunId"]
            st.success(f"✅ Job started! Run ID: `{response['JobRunId']}`")
        except Exception as e:
            st.error(f"Failed to start job: {e}")

with col2:
    st.markdown("""
    **Glue Job Configuration:**
    - **Engine:** PySpark (Glue 4.0)
    - **Workers:** 2 × G.1X (4 vCPU, 16 GB each)
    - **Timeout:** 10 minutes
    - **Script:** `transform_etl.py`
    """)

# Job Status
st.markdown("---")
st.markdown("## 📈 Job Status")

run_id = st.session_state.get("s2_glue_run_id", "")

if run_id:
    try:
        glue_client = boto3.client("glue")
        response = glue_client.get_job_run(JobName=glue_job_name, RunId=run_id)
        run = response["JobRun"]

        state = run["JobRunState"]
        duration = run.get("ExecutionTime", 0)

        state_icons = {
            "STARTING": "🟡", "RUNNING": "🔵", "SUCCEEDED": "🟢",
            "FAILED": "🔴", "STOPPED": "⚪", "TIMEOUT": "🟠",
        }
        icon = state_icons.get(state, "⚪")

        st.markdown(f"### {icon} {state}")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Run ID", run_id[:16] + "...")
        with col2:
            st.metric("Duration", f"{duration}s" if duration else "In progress...")
        with col3:
            st.metric("Workers", run.get("NumberOfWorkers", "N/A"))

        if state in ("STARTING", "RUNNING"):
            st.info("Job is running... Click Refresh to check status.")
            if st.button("🔄 Refresh Status"):
                st.rerun()
        elif state == "SUCCEEDED":
            st.success(f"✅ Job completed in {duration} seconds!")
            # Show updated stats
            curated_count, curated_size = get_s3_zone_stats(bucket, "curated/")
            st.metric("Curated Zone (updated)", f"{curated_count} files • {curated_size / 1024:.1f} KB")
        elif state == "FAILED":
            st.error(f"Job failed: {run.get('ErrorMessage', 'Unknown error')}")
    except Exception as e:
        st.warning(f"Could not fetch status: {e}")
else:
    st.info("No job running. Start the Glue ETL job above.")

# Job History
st.markdown("---")
st.markdown("## 📋 Job Run History")
try:
    glue_client = boto3.client("glue")
    response = glue_client.get_job_runs(JobName=glue_job_name, MaxResults=5)
    runs = response.get("JobRuns", [])
    if runs:
        history = []
        for run in runs:
            history.append({
                "Run ID": run["Id"][:16] + "...",
                "State": run["JobRunState"],
                "Started": run.get("StartedOn", "").strftime("%Y-%m-%d %H:%M") if run.get("StartedOn") else "-",
                "Duration (s)": run.get("ExecutionTime", "-"),
                "Workers": run.get("NumberOfWorkers", "-"),
            })
        st.dataframe(pd.DataFrame(history), use_container_width=True)
    else:
        st.info("No previous runs.")
except Exception:
    st.info("Job history unavailable.")

# ============================================================
# Format Comparison
# ============================================================
st.markdown("---")
st.markdown("## 💡 JSON vs Parquet — Why It Matters")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    ### 📄 JSON (Raw Zone)
    - **Row-oriented** — must scan entire file
    - Human readable, easy debugging
    - No built-in compression
    - No schema enforcement
    - **Athena cost:** Scans ALL data
    """)

with col2:
    st.markdown("""
    ### 📊 Parquet (Curated Zone)
    - **Columnar** — reads only needed columns
    - Binary format, self-describing schema
    - Built-in Snappy/GZIP compression
    - Predicate pushdown support
    - **Athena cost:** 80-90% less scan
    """)

st.markdown("""
> **Exam tip:** Athena charges **$5 per TB scanned**. Converting to Parquet + partitioning
> typically reduces costs by 90%+ because queries only read the columns and partitions they need.
""")
