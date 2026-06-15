"""
Page 5: Query & Analyze — Athena SQL on raw vs curated

Pipeline step: Athena queries on JSON (raw) vs Parquet (curated) — cost comparison
"""
import streamlit as st
import boto3
import json
import time
import pandas as pd
import plotly.express as px

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Query & Analyze", page_icon="💎", layout="wide")

st.markdown("# 💎 Step 5: Query & Analyze")
st.markdown("""
Query the vehicle telemetry using **Amazon Athena**. Compare the cost of scanning 
raw JSON vs curated Parquet — the format optimization from Step 3 pays off here.
""")



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


config = get_config()
database = config.get("GlueDatabase", "dea_s2_pipeline")
workgroup = config.get("AthenaWorkgroup", "dea-s2-pipeline")

# ============================================================
# Architecture
# ============================================================
st.markdown("## Architecture")

import os
_diagram = os.path.join(os.path.dirname(__file__), "..", "diagrams", "05_query.png")
if os.path.exists(_diagram):
    st.image(_diagram, caption="Query: Glue Catalog + S3 (Raw/Curated) → Athena → Data Engineer", use_container_width=True)
else:
    st.info("Run `create_diagrams.py` in `diagrams/` to generate architecture diagrams.")

st.markdown("---")


# ============================================================
# Query Helper
# ============================================================
def run_query(query):
    athena = boto3.client("athena")
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    qid = response["QueryExecutionId"]
    while True:
        result = athena.get_query_execution(QueryExecutionId=qid)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            raise Exception(result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown"))
        time.sleep(1)

    results = athena.get_query_results(QueryExecutionId=qid)
    columns = [c["Label"] for c in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = [[f.get("VarCharValue", "") for f in r["Data"]] for r in results["ResultSet"]["Rows"][1:]]
    stats = result["QueryExecution"]["Statistics"]
    return pd.DataFrame(rows, columns=columns), stats.get("DataScannedInBytes", 0), stats.get("EngineExecutionTimeInMillis", 0)


# ============================================================
# Side-by-Side Comparison
# ============================================================
st.markdown("## 📊 Raw JSON vs Curated Parquet")

COMPARISONS = {
    "Count all records": {
        "raw": f"SELECT COUNT(*) as total FROM {database}.events_raw",
        "curated": f"SELECT COUNT(*) as total FROM {database}.events_curated",
    },
    "Events by source": {
        "raw": f"SELECT source, COUNT(*) as cnt FROM {database}.events_raw GROUP BY source ORDER BY cnt DESC",
        "curated": f"SELECT source, COUNT(*) as cnt FROM {database}.events_curated GROUP BY source ORDER BY cnt DESC",
    },
    "Specific columns only": {
        "raw": f"SELECT event_type, device_id FROM {database}.events_raw LIMIT 20",
        "curated": f"SELECT event_type, device_id FROM {database}.events_curated LIMIT 20",
    },
}

selected = st.selectbox("Query:", list(COMPARISONS.keys()))

if st.button("▶️ Run on Both Tables", type="primary"):
    queries = COMPARISONS[selected]
    col1, col2 = st.columns(2)

    scanned_raw, scanned_cur = 0, 0

    with col1:
        st.markdown("### 📄 Raw (JSON)")
        try:
            df, scanned_raw, exec_time = run_query(queries["raw"])
            st.metric("Data Scanned", f"{scanned_raw / 1024:.1f} KB")
            st.metric("Time", f"{exec_time} ms")
            st.metric("Cost", f"${(scanned_raw / (1024**4)) * 5:.8f}")
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")

    with col2:
        st.markdown("### 📊 Curated (Parquet)")
        try:
            df, scanned_cur, exec_time = run_query(queries["curated"])
            st.metric("Data Scanned", f"{scanned_cur / 1024:.1f} KB")
            st.metric("Time", f"{exec_time} ms")
            st.metric("Cost", f"${(scanned_cur / (1024**4)) * 5:.8f}")
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")

    if scanned_raw > 0 and scanned_cur > 0:
        savings = ((scanned_raw - scanned_cur) / scanned_raw) * 100
        st.success(f"📉 Parquet scanned **{savings:.0f}% less** data ({scanned_raw/max(scanned_cur,1):.1f}x reduction)")

st.markdown("---")

# ============================================================
# Custom SQL
# ============================================================
st.markdown("## 📝 Custom SQL")

query = st.text_area("Query:",
    value=f"SELECT source, event_type, COUNT(*) as cnt\nFROM {database}.events_curated\nGROUP BY source, event_type\nORDER BY cnt DESC\nLIMIT 20",
    height=100)

if st.button("▶️ Run"):
    try:
        df, scanned, exec_time = run_query(query)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Scanned", f"{scanned / 1024:.1f} KB")
        with col2:
            st.metric("Time", f"{exec_time} ms")
        with col3:
            st.metric("Cost", f"${(scanned / (1024**4)) * 5:.6f}")
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")

st.markdown("---")

# ============================================================
# Redshift Patterns
# ============================================================
st.markdown("## 🏢 Redshift Patterns (Reference)")

st.markdown("""
| Command | Direction | Use Case |
|---------|-----------|----------|
| **COPY** | S3 → Redshift | Bulk load data INTO the warehouse |
| **UNLOAD** | Redshift → S3 | Export data OUT as Parquet |
| **Spectrum** | Query S3 from Redshift | JOIN local + external tables |
| **Federated Query** | Query RDS/Aurora from Redshift | Live operational data joins |

> **Exam trap:** COPY = inbound. UNLOAD = outbound. Spectrum queries S3. Federated queries RDS (not S3!).
""")

st.markdown("---")

# ============================================================
# Exam Tips
# ============================================================
st.markdown("## 💡 Exam Tips")
st.markdown("""
| Concept | Key Point |
|---------|-----------|
| **Athena pricing** | $5 per TB scanned. Parquet + partitions = 90% savings. |
| **Partition pruning** | `WHERE year='2024'` skips all other year folders entirely |
| **SELECT *** | Reads ALL columns in Parquet. Specify only what you need. |
| **CTAS** | CREATE TABLE AS SELECT — materialize results as a new table |
| **Workgroups** | Set per-query data scan limits to prevent runaway costs |
| **Spectrum** | External tables in Glue Catalog, queried from Redshift cluster |
""")
