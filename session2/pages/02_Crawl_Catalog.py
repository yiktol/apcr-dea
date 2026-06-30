"""
Page 2: Crawl & Catalog — Glue Crawler discovers schema, populates Data Catalog

Pipeline step: S3 raw/ → Glue Crawler → Glue Data Catalog (tables, partitions, schema)
"""
import streamlit as st
import boto3
import json
import pandas as pd
from datetime import datetime, timezone

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Crawl & Catalog", page_icon="📚", layout="wide")

st.markdown("# 📚 Step 2: Crawl & Catalog")
st.markdown("""
The Glue Crawler scans S3, detects file format and schema, discovers partitions,
and writes metadata to the **Glue Data Catalog** — making the data queryable by Athena, 
Redshift Spectrum, and EMR.
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
database = config.get("OutputGlueDatabase", "dea_s2_pipeline")

# ============================================================
# Architecture
# ============================================================
st.markdown("## Architecture")

import os
_diagram = os.path.join(os.path.dirname(__file__), "..", "diagrams", "02_crawl_catalog.png")
if os.path.exists(_diagram):
    st.image(_diagram, caption="Crawl: S3 → Glue Crawler → Data Catalog → Athena / EMR", use_container_width=True)
else:
    st.info("Run `create_diagrams.py` in `diagrams/` to generate architecture diagrams.")

st.markdown("---")

# ============================================================
# Crawler Controls
# ============================================================
st.markdown("## 🕷️ Run Crawlers")

RAW_CRAWLER = "dea-s2-raw-crawler"
CURATED_CRAWLER = "dea-s2-curated-crawler"


def get_crawler_status(crawler_name):
    """Get crawler state and last run info."""
    try:
        glue = boto3.client("glue")
        response = glue.get_crawler(Name=crawler_name)
        crawler = response["Crawler"]
        return {
            "state": crawler["State"],
            "last_status": crawler.get("LastCrawl", {}).get("Status", "N/A"),
            "tables_created": crawler.get("LastCrawl", {}).get("TablesCreated", 0),
            "tables_updated": crawler.get("LastCrawl", {}).get("TablesUpdated", 0),
        }
    except Exception:
        return None


def start_crawler_safe(crawler_name):
    """Start a crawler only if it's not already running."""
    glue = boto3.client("glue")
    status = get_crawler_status(crawler_name)

    if status and status["state"] in ("RUNNING", "STOPPING"):
        st.info(f"Crawler `{crawler_name}` is already **{status['state']}**. Please wait.")
        return

    try:
        glue.start_crawler(Name=crawler_name)
        st.success(f"✅ Crawler `{crawler_name}` started! Takes ~1-2 minutes.")
    except Exception as e:
        if "CrawlerRunning" in str(e):
            st.info(f"Crawler `{crawler_name}` is already running.")
        else:
            st.error(f"Error: {e}")


col1, col2 = st.columns(2)

with col1:
    st.markdown("### Raw Zone Crawler")
    st.caption(f"`{RAW_CRAWLER}` → scans `raw/` (JSON)")

    if st.button("▶️ Start Raw Crawler", type="primary", use_container_width=True):
        start_crawler_safe(RAW_CRAWLER)

    raw_status = get_crawler_status(RAW_CRAWLER)
    if raw_status:
        st.metric("State", raw_status["state"])
        st.write(f"Last run: **{raw_status['last_status']}** | Tables created: {raw_status['tables_created']} | Updated: {raw_status['tables_updated']}")
    else:
        st.caption("Crawler not found.")

with col2:
    st.markdown("### Curated Zone Crawler")
    st.caption(f"`{CURATED_CRAWLER}` → scans `curated/` (Parquet)")

    if st.button("▶️ Start Curated Crawler", type="primary", use_container_width=True):
        start_crawler_safe(CURATED_CRAWLER)

    curated_status = get_crawler_status(CURATED_CRAWLER)
    if curated_status:
        st.metric("State", curated_status["state"])
        st.write(f"Last run: **{curated_status['last_status']}** | Tables created: {curated_status['tables_created']} | Updated: {curated_status['tables_updated']}")
    else:
        st.caption("Crawler not found.")

st.markdown("---")
st.markdown("""
**Crawler process:**
1. Connects to S3 target path
2. Runs classifiers (JSON? Parquet? CSV?)
3. Infers schema (column names + types)
4. Detects `year=/month=/day=` partition patterns
5. Writes table definitions to Data Catalog

**Scheduling options:** Scheduled (cron) | On-demand | Conditional (after job completes)
""")

st.markdown("---")

# ============================================================
# Browse Data Catalog
# ============================================================
st.markdown("## 📖 Data Catalog Tables")

try:
    glue = boto3.client("glue")
    tables_response = glue.get_tables(DatabaseName=database)
    tables = tables_response.get("TableList", [])

    if tables:
        for table in tables:
            name = table["Name"]
            storage = table.get("StorageDescriptor", {})
            columns = storage.get("Columns", [])
            partitions = table.get("PartitionKeys", [])
            location = storage.get("Location", "")
            classification = table.get("Parameters", {}).get("classification", "unknown")

            with st.expander(f"📋 **{name}** — {classification.upper()} | {len(columns)} cols | {len(partitions)} partitions", expanded=False):
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**Schema:**")
                    schema_df = pd.DataFrame([{"Column": c["Name"], "Type": c["Type"]} for c in columns])
                    st.dataframe(schema_df, use_container_width=True, hide_index=True)

                with col2:
                    st.markdown(f"**Location:** `{location}`")
                    st.markdown(f"**Format:** {classification}")
                    if partitions:
                        st.markdown(f"**Partition keys:** {', '.join(p['Name'] for p in partitions)}")

                # Show discovered partitions
                if partitions:
                    try:
                        parts = glue.get_partitions(DatabaseName=database, TableName=name, MaxResults=10)
                        discovered = parts.get("Partitions", [])
                        if discovered:
                            st.markdown(f"**Discovered partitions ({len(discovered)}):**")
                            part_df = pd.DataFrame([{
                                "Values": "/".join(p["Values"]),
                                "Location": p["StorageDescriptor"]["Location"].split("/")[-2] + "/"
                            } for p in discovered[:10]])
                            st.dataframe(part_df, use_container_width=True, hide_index=True)
                    except Exception:
                        st.caption("Run crawler to discover partitions.")
    else:
        st.info("No tables found. Run the crawler after ingesting data.")
except Exception as e:
    st.warning(f"Could not connect to Glue: {e}")

st.markdown("---")

# ============================================================
# Catalog Integrations
# ============================================================
st.markdown("## 🔗 Who Uses the Data Catalog?")

st.markdown("""
| Service | Integration |
|---------|-------------|
| **Amazon Athena** | Reads table definitions → queries S3 via SQL |
| **Redshift Spectrum** | External schemas reference Catalog tables |
| **Amazon EMR** | Drop-in replacement for Apache Hive Metastore |
| **AWS Lake Formation** | Fine-grained column/row-level access control |
| **Amazon QuickSight** | Discovers datasets for visualization |
| **AWS Glue ETL** | Reads/writes using Catalog table references |
""")

st.markdown("---")

# ============================================================
# Exam Tips
# ============================================================
st.markdown("## 💡 Exam Tips")
st.markdown("""
| Concept | Key Point |
|---------|-----------|
| **Crawler** | Auto-discovers schema + partitions from S3 |
| **MSCK REPAIR TABLE** | Athena command to manually register new partitions (alternative to crawler) |
| **Schema Registry** | For streaming — validates schema evolution, prevents breaking changes |
| **Hive Metastore compatible** | Glue Catalog is a drop-in replacement for Hive Metastore |
| **Partition sync** | Crawler detects new `key=value/` prefixes automatically |
| **Custom classifiers** | Define your own if built-in classifiers can't detect your format |
""")
