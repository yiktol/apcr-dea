"""
Page 5: Glue ETL Job - Raw → Curated

Covers:
- Data lake zones pattern (raw → curated → published)
- Glue ETL with PySpark (serverless Spark)
- JSON → Parquet conversion (columnar, compressed)
- Partitioning for query performance
- Deduplication
"""
import streamlit as st
import boto3
import time
import pandas as pd
from datetime import datetime, timezone

st.set_page_config(page_title="ETL Job", page_icon="🔧", layout="wide")

st.markdown("# 🔧 Glue ETL: Raw → Curated")
st.markdown("""
Run the Glue ETL job to transform raw JSON data into optimized Parquet in the curated zone.
This is the **Volume** optimization step — Parquet is columnar, compressed, and partitioned 
for fast analytical queries.
""")

# Architecture diagram
import os
_diagram = os.path.join(os.path.dirname(__file__), "..", "diagrams", "04_storage_layer.png")
if os.path.exists(_diagram):
    with st.expander("📐 Architecture Diagram — Storage & ETL Layer", expanded=False):
        st.image(_diagram, caption="Storage Layer: Raw → Glue ETL → Curated (Parquet) + Lake Formation", width="stretch")


def get_config():
    try:
        cfn = boto3.client("cloudformation")
        response = cfn.describe_stacks(StackName="DataEngineerPipeline")
        raw = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
        return {k.replace("Output", "", 1) if k.startswith("Output") else k: v for k, v in raw.items()}
    except Exception:
        return {}


def get_s3_zone_stats(bucket, prefix):
    """Get object count and size for an S3 prefix."""
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
glue_job_name = config.get("GlueJobName", "dea-raw-to-curated")

# Data Lake zones status
st.markdown("## 📊 Data Lake Zones")

if bucket:
    col1, col2, col3 = st.columns(3)

    raw_count, raw_size = get_s3_zone_stats(bucket, "raw/")
    curated_count, curated_size = get_s3_zone_stats(bucket, "curated/")
    errors_count, errors_size = get_s3_zone_stats(bucket, "errors/")

    with col1:
        st.metric("🟠 Raw Zone", f"{raw_count} files")
        st.caption(f"{raw_size / 1024:.1f} KB • JSON (newline-delimited)")

    with col2:
        st.metric("🟢 Curated Zone", f"{curated_count} files")
        st.caption(f"{curated_size / 1024:.1f} KB • Parquet (partitioned)")

    with col3:
        st.metric("🔴 Errors Zone", f"{errors_count} files")
        st.caption(f"{errors_size / 1024:.1f} KB • Failed records")

    # Compression ratio
    if raw_size > 0 and curated_size > 0:
        ratio = raw_size / curated_size
        st.success(f"📉 Compression ratio: **{ratio:.1f}x** (Parquet vs JSON)")
else:
    st.warning("Pipeline not deployed.")

st.markdown("---")

# Glue job controls
st.markdown("## 🚀 Run ETL Job")
st.markdown(f"**Job name:** `{glue_job_name}`")

col1, col2 = st.columns([1, 2])

with col1:
    if st.button("▶️ Start Glue Job", type="primary", width="stretch"):
        try:
            glue_client = boto3.client("glue")
            response = glue_client.start_job_run(
                JobName=glue_job_name,
                Arguments={
                    "--DATA_LAKE_BUCKET": bucket,
                },
            )
            run_id = response["JobRunId"]
            st.session_state["glue_run_id"] = run_id
            st.success(f"✅ Job started! Run ID: `{run_id}`")
        except Exception as e:
            st.error(f"Failed to start job: {e}")

with col2:
    st.markdown("""
    **What this job does:**
    1. Reads all JSON files from `raw/`
    2. Deduplicates by `event_id`
    3. Adds partition columns (year/month/day)
    4. Writes as Parquet to `curated/`
    """)

# Job status monitoring
st.markdown("---")
st.markdown("## 📈 Job Status")

run_id = st.session_state.get("glue_run_id", "")

if run_id:
    try:
        glue_client = boto3.client("glue")
        response = glue_client.get_job_run(JobName=glue_job_name, RunId=run_id)
        run = response["JobRun"]

        state = run["JobRunState"]
        started = run.get("StartedOn", "")
        completed = run.get("CompletedOn", "")
        duration = run.get("ExecutionTime", 0)

        state_colors = {
            "STARTING": "🟡",
            "RUNNING": "🔵",
            "SUCCEEDED": "🟢",
            "FAILED": "🔴",
            "STOPPED": "⚪",
            "TIMEOUT": "🟠",
        }

        icon = state_colors.get(state, "⚪")
        st.markdown(f"### {icon} {state}")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Run ID", run_id[:12] + "...")
        with col2:
            st.metric("Duration", f"{duration}s" if duration else "In progress...")
        with col3:
            st.metric("Workers", run.get("NumberOfWorkers", "N/A"))

        if state in ("STARTING", "RUNNING"):
            st.info("Job is running... Click below to refresh status.")
            if st.button("🔄 Refresh"):
                st.rerun()

        elif state == "SUCCEEDED":
            st.success(f"Job completed in {duration} seconds!")
            st.markdown("**Next steps:** Query the curated zone in the Query Data page using the `events_curated` table.")

            # Show updated stats
            curated_count, curated_size = get_s3_zone_stats(bucket, "curated/")
            st.metric("Curated Zone (updated)", f"{curated_count} files • {curated_size / 1024:.1f} KB")

        elif state == "FAILED":
            error_msg = run.get("ErrorMessage", "Unknown error")
            st.error(f"Job failed: {error_msg}")

    except Exception as e:
        st.warning(f"Could not fetch job status: {e}")
else:
    st.info("No job run yet. Start the Glue job above to see status here.")

# Recent job runs
st.markdown("---")
st.markdown("## 📋 Job History")

try:
    glue_client = boto3.client("glue")
    response = glue_client.get_job_runs(JobName=glue_job_name, MaxResults=5)
    runs = response.get("JobRuns", [])

    if runs:
        history = []
        for run in runs:
            history.append({
                "Run ID": run["Id"][:12] + "...",
                "State": run["JobRunState"],
                "Started": run.get("StartedOn", "").strftime("%Y-%m-%d %H:%M") if run.get("StartedOn") else "-",
                "Duration (s)": run.get("ExecutionTime", "-"),
                "Workers": run.get("NumberOfWorkers", "-"),
            })
        st.dataframe(pd.DataFrame(history), width="stretch")
    else:
        st.info("No previous runs.")
except Exception as e:
    st.info("Job history unavailable — run the job at least once.")

# Explanation
st.markdown("---")
st.markdown("## 💡 Why Parquet?")

col1, col2 = st.columns(2)
with col1:
    st.markdown("""
    ### JSON (Raw Zone)
    - Row-oriented
    - Human readable
    - No compression
    - Scans entire file for queries
    - Good for: landing, debugging
    """)

with col2:
    st.markdown("""
    ### Parquet (Curated Zone)
    - Columnar
    - Compressed (Snappy)
    - Schema embedded
    - Only reads needed columns
    - Good for: analytics, Athena, Redshift Spectrum
    """)

st.markdown("""
**Impact on Athena cost:** Athena charges $5/TB scanned. Parquet typically reduces scan 
volume by 80-90% vs JSON because it only reads the columns your query needs.
""")
