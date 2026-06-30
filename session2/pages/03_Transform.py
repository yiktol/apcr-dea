"""
Page 3: Transform — Glue ETL Job (JSON → Parquet)

Pipeline step: S3 raw/ (JSON) → Glue ETL → S3 curated/ (Parquet, partitioned, compressed)
"""
import streamlit as st
import boto3
import json
import time
import pandas as pd

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Transform", page_icon="🔄", layout="wide")

st.markdown("# 🔄 Step 3: Transform (ETL)")
st.markdown("""
The Glue ETL job reads raw JSON from S3, deduplicates, partitions by date, 
compresses with Snappy, and writes optimized **Parquet** to the curated zone.
""")



def get_config():
    if "s2_config" not in st.session_state or not st.session_state.s2_config:
        try:
            cfn = boto3.client("cloudformation")
            response = cfn.describe_stacks(StackName="DeaSession2Pipeline")
            raw = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
            st.session_state.s2_config = raw
        except Exception:
            st.session_state.s2_config = {}
    return st.session_state.s2_config


config = get_config()
bucket = config.get("OutputDataLakeBucket", "")
glue_job_name = config.get("OutputGlueJobName", "dea-s2-transform-etl")

# ============================================================
# Architecture
# ============================================================
st.markdown("## Architecture")

import os
_diagram = os.path.join(os.path.dirname(__file__), "..", "diagrams", "03_transform.png")
if os.path.exists(_diagram):
    st.image(_diagram, caption="Transform: S3 Raw (JSON) → Glue ETL (PySpark) → S3 Curated (Parquet)", use_container_width=True)
else:
    st.info("Run `create_diagrams.py` in `diagrams/` to generate architecture diagrams.")

st.markdown("---")

# ============================================================
# Data Lake Zones Status
# ============================================================
st.markdown("## 📊 Data Lake Zones")

if bucket:
    def get_zone_stats(prefix):
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        count, size = 0, 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                count += 1
                size += obj["Size"]
        return count, size

    col1, col2, col3 = st.columns(3)
    raw_count, raw_size = get_zone_stats("raw/")
    curated_count, curated_size = get_zone_stats("curated/")

    with col1:
        st.metric("🟠 Raw (JSON)", f"{raw_count} files")
        st.caption(f"{raw_size / 1024:.1f} KB")

    with col2:
        st.metric("🟢 Curated (Parquet)", f"{curated_count} files")
        st.caption(f"{curated_size / 1024:.1f} KB")

    with col3:
        if raw_size > 0 and curated_size > 0:
            ratio = raw_size / curated_size
            st.metric("📉 Compression", f"{ratio:.1f}x smaller")
        else:
            st.metric("📉 Compression", "Run ETL first")
else:
    st.warning("Stack not deployed.")

st.markdown("---")

# ============================================================
# ETL Job Execution
# ============================================================
st.markdown("## 🚀 Run ETL Job")

col1, col2 = st.columns([1, 2])

with col1:
    if st.button("▶️ Start Glue ETL", type="primary", use_container_width=True):
        try:
            glue = boto3.client("glue")
            # Check if a run is already active before starting a new one
            runs = glue.get_job_runs(JobName=glue_job_name, MaxResults=1).get("JobRuns", [])
            active_states = {"STARTING", "RUNNING", "STOPPING"}

            if runs and runs[0]["JobRunState"] in active_states:
                existing_id = runs[0]["Id"]
                st.session_state["s2_glue_run_id"] = existing_id
                st.info(f"⏳ Job is already running (Run ID: `{existing_id[:16]}...`). Showing status below.")
            else:
                response = glue.start_job_run(JobName=glue_job_name,
                                             Arguments={"--DATA_LAKE_BUCKET": bucket})
                st.session_state["s2_glue_run_id"] = response["JobRunId"]
                st.success(f"✅ Started: `{response['JobRunId'][:16]}...`")
        except Exception as e:
            st.error(f"Error: {e}")

    run_id = st.session_state.get("s2_glue_run_id", "")
    if run_id:
        if st.button("🔄 Check Status", use_container_width=True):
            try:
                glue = boto3.client("glue")
                run = glue.get_job_run(JobName=glue_job_name, RunId=run_id)["JobRun"]
                state = run["JobRunState"]
                icons = {"STARTING": "🟡", "RUNNING": "🔵", "SUCCEEDED": "🟢", "FAILED": "🔴"}
                st.markdown(f"### {icons.get(state, '⚪')} {state}")
                if run.get("ExecutionTime"):
                    st.write(f"Duration: {run['ExecutionTime']}s")
                if state == "FAILED":
                    st.error(run.get("ErrorMessage", "Unknown error"))
            except Exception as e:
                st.error(f"Error: {e}")

with col2:
    st.markdown("""
    **ETL Operations:**
    
    | Step | What | Why |
    |------|------|-----|
    | **Read** | Load all JSON from `raw/` | Source data |
    | **Deduplicate** | `dropDuplicates(["event_id"])` | Data quality |
    | **Partition** | Add `year/month/day` columns | Query pruning |
    | **Compress** | Snappy codec | 75% smaller |
    | **Write** | Parquet to `curated/` | Columnar analytics |
    """)

st.markdown("---")

# ============================================================
# Format Comparison
# ============================================================
st.markdown("## 📄 JSON vs 📊 Parquet")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    ### JSON (Raw Zone)
    - Row-oriented — scans entire file
    - Human readable
    - No compression built-in
    - No schema enforcement
    - **Athena:** Scans ALL data → expensive
    """)

with col2:
    st.markdown("""
    ### Parquet (Curated Zone)
    - Columnar — reads only needed columns
    - Binary, self-describing schema
    - Built-in Snappy/GZIP compression
    - Predicate pushdown (skip blocks)
    - **Athena:** 80-90% less scan → cheap
    """)

st.info("💡 **Exam tip:** Athena charges $5/TB scanned. Parquet + partitioning reduces costs by 90%+ because only relevant columns and partitions are read.")

st.markdown("---")

# ============================================================
# Exam Tips
# ============================================================
st.markdown("## 💡 Exam Tips")
st.markdown("""
| Concept | Key Point |
|---------|-----------|
| **Glue ETL** | Serverless PySpark. Pay per DPU-hour. G.1X = 4 vCPU, 16 GB. |
| **Parquet** | Columnar + compressed. Default choice for analytics on AWS. |
| **Partitioning** | Align to query patterns (WHERE year='2024'). Athena skips unneeded partitions. |
| **Deduplication** | `dropDuplicates()` in Spark. Ensures data quality. |
| **Bookmarks** | Glue job bookmarks track processed files — avoids reprocessing. |
| **ORC vs Parquet** | ORC = smaller files, Hive-native. Parquet = broader ecosystem, Spark/Athena default. |
""")
