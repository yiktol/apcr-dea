"""
Page 2: Data Catalog — Glue Crawlers, Schema Discovery, Partitions

Covers Task 2.2: Understand data cataloging systems
- Run Glue Crawler on-demand
- Browse discovered tables/schemas
- Partition management
- Glue Data Catalog as Hive Metastore replacement
"""
import streamlit as st
import boto3
import json
import time
import pandas as pd
from datetime import datetime, timezone

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Data Catalog", page_icon="📚", layout="wide")

st.markdown("# 📚 Data Catalog (Task 2.2)")
st.markdown("""
Run the Glue Crawler to auto-discover schemas from S3, then browse the Data Catalog.
See how partitions are detected and synchronized.
""")


def get_config():
    if "s3_config" not in st.session_state:
        try:
            cfn = boto3.client("cloudformation")
            response = cfn.describe_stacks(StackName="DeaSession3DataStore")
            raw = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
            st.session_state.s3_config = raw
        except Exception:
            st.session_state.s3_config = {}
    return st.session_state.s3_config


config = get_config()
database = config.get("GlueDatabase", "dea_s3_datastore")
crawler_name = config.get("CrawlerName", "dea-s3-datalake-crawler")

# ============================================================
# Crawler Execution
# ============================================================
st.markdown("## 🕷️ Glue Crawler")

col1, col2 = st.columns([1, 2])

with col1:
    if st.button("▶️ Run Crawler", type="primary", use_container_width=True):
        try:
            glue_client = boto3.client("glue")
            glue_client.start_crawler(Name=crawler_name)
            st.success(f"✅ Crawler `{crawler_name}` started!")
            st.session_state["crawler_running"] = True
        except glue_client.exceptions.CrawlerRunningException:
            st.warning("Crawler is already running.")
        except Exception as e:
            st.error(f"Error: {e}")

    if st.button("🔄 Check Status", use_container_width=True):
        try:
            glue_client = boto3.client("glue")
            response = glue_client.get_crawler(Name=crawler_name)
            state = response["Crawler"]["State"]
            last_crawl = response["Crawler"].get("LastCrawl", {})
            st.metric("Crawler State", state)
            if last_crawl:
                st.write(f"Last run: {last_crawl.get('Status', 'N/A')}")
                st.write(f"Tables created: {last_crawl.get('TablesCreated', 0)}")
                st.write(f"Tables updated: {last_crawl.get('TablesUpdated', 0)}")
        except Exception as e:
            st.error(f"Error: {e}")

with col2:
    st.markdown("""
    **What the Crawler does:**
    1. Connects to S3 targets (`raw/` and `curated/`)
    2. Runs classifiers to detect file format (JSON, Parquet, CSV)
    3. Infers schema (column names, types)
    4. Detects partitions (year=/month=/day=)
    5. Writes metadata to the Glue Data Catalog
    
    **Triggers:** Scheduled (cron), Conditional, or On-demand
    """)

st.markdown("---")

# ============================================================
# Browse Data Catalog
# ============================================================
st.markdown("## 📖 Browse Data Catalog")

try:
    glue_client = boto3.client("glue")
    response = glue_client.get_tables(DatabaseName=database)
    tables = response.get("TableList", [])

    if tables:
        table_names = [t["Name"] for t in tables]
        selected_table = st.selectbox("Select table:", table_names)

        # Get table detail
        table_info = next(t for t in tables if t["Name"] == selected_table)
        storage = table_info.get("StorageDescriptor", {})
        columns = storage.get("Columns", [])
        partitions = table_info.get("PartitionKeys", [])
        location = storage.get("Location", "")
        classification = table_info.get("Parameters", {}).get("classification", "unknown")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Format", classification.upper())
        with col2:
            st.metric("Columns", len(columns))
        with col3:
            st.metric("Partition Keys", len(partitions))

        st.markdown(f"**Location:** `{location}`")

        # Schema
        st.markdown("### Schema")
        schema_data = [{"Column": c["Name"], "Type": c["Type"]} for c in columns]
        if partitions:
            for p in partitions:
                schema_data.append({"Column": f"{p['Name']} (partition)", "Type": p["Type"]})
        st.dataframe(pd.DataFrame(schema_data), use_container_width=True)

        # Partitions
        if partitions:
            st.markdown("### Discovered Partitions")
            try:
                parts_response = glue_client.get_partitions(
                    DatabaseName=database, TableName=selected_table, MaxResults=20
                )
                discovered_parts = parts_response.get("Partitions", [])
                if discovered_parts:
                    part_data = [{"Values": "/".join(p["Values"]), "Location": p["StorageDescriptor"]["Location"]}
                                 for p in discovered_parts[:10]]
                    st.dataframe(pd.DataFrame(part_data), use_container_width=True)
                else:
                    st.info("No partitions discovered yet. Run the ETL job to create partitioned data, then re-crawl.")
            except Exception:
                st.info("Run the crawler after producing data to discover partitions.")
    else:
        st.info("No tables found. Deploy the stack and produce some data first.")
except Exception as e:
    st.warning(f"Could not connect to Glue: {e}")

st.markdown("---")

# ============================================================
# Data Catalog Integrations
# ============================================================
st.markdown("## 🔗 Catalog Integrations")

st.markdown("""
The Glue Data Catalog is a **drop-in replacement for Apache Hive Metastore** and integrates natively with:

| Service | How it uses the Catalog |
|---------|------------------------|
| **Amazon Athena** | Reads table definitions to query S3 directly via SQL |
| **Amazon Redshift Spectrum** | Creates external schemas referencing Catalog tables |
| **Amazon EMR** | Replaces Hive Metastore for Spark/Hive/Presto |
| **AWS Lake Formation** | Adds fine-grained permissions on Catalog resources |
| **Amazon QuickSight** | Discovers datasets from Catalog for visualization |
""")

st.markdown("---")

# ============================================================
# Exam Tips
# ============================================================
st.markdown("## 💡 Exam Tips")
st.markdown("""
| Concept | Key Point |
|---------|-----------|
| **Crawler** | Auto-discovers schema + partitions. Scheduled, conditional, or on-demand. |
| **Partition sync** | Crawler detects new `year=/month=/day=` prefixes automatically |
| **Schema Registry** | Validates streaming data schemas, prevents breaking changes |
| **Catalog sharing** | Share across accounts via AWS RAM or Lake Formation |
| **Custom classifiers** | Define your own if built-in classifiers can't detect your format |
| **MSCK REPAIR TABLE** | Athena command to manually add partitions (alternative to crawler) |
""")
