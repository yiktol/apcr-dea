"""
Page 4: Query & Optimize — Athena SQL on Raw vs Curated

Covers Task 1.4: Apply programming concepts (SQL optimization)
- Athena queries on JSON (raw) vs Parquet (curated)
- Data scanned comparison → cost difference
- Partition pruning demonstration
- UNLOAD command for Redshift → S3 pattern
"""
import streamlit as st
import boto3
import json
import time
import pandas as pd
import plotly.express as px

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Query & Optimize", page_icon="💎", layout="wide")

st.markdown("# 💎 Query & Optimize (Task 1.4)")
st.markdown("""
Run SQL queries against **raw JSON** and **curated Parquet** tables to see the cost/performance
difference. This directly maps to exam questions about format optimization and UNLOAD patterns.
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


config = get_config()
database = config.get("GlueDatabase", "dea_s2_pipeline")
workgroup = config.get("AthenaWorkgroup", "dea-s2-pipeline")

st.sidebar.markdown("### Athena Settings")
st.sidebar.markdown(f"**Database:** `{database}`")
st.sidebar.markdown(f"**Workgroup:** `{workgroup}`")


# ============================================================
# Athena Query Helper
# ============================================================
def run_athena_query(query, database, workgroup):
    """Execute Athena query and return DataFrame + stats."""
    athena = boto3.client("athena")

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    query_id = response["QueryExecutionId"]

    # Poll for completion
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise Exception(f"Query {state}: {reason}")
        time.sleep(1)

    # Get results
    results = athena.get_query_results(QueryExecutionId=query_id)
    columns = [col["Label"] for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = []
    for row in results["ResultSet"]["Rows"][1:]:
        rows.append([field.get("VarCharValue", "") for field in row["Data"]])

    # Stats
    stats = result["QueryExecution"]["Statistics"]
    data_scanned = stats.get("DataScannedInBytes", 0)
    exec_time = stats.get("EngineExecutionTimeInMillis", 0)

    return pd.DataFrame(rows, columns=columns), data_scanned, exec_time


# ============================================================
# Format Comparison Queries
# ============================================================
st.markdown("## 📊 Raw vs Curated — Cost Comparison")
st.markdown("""
Run the same query on both tables and compare **data scanned** (which drives Athena cost).
""")

COMPARISON_QUERIES = {
    "Count all events": {
        "raw": f"SELECT COUNT(*) as total FROM {database}.events_raw",
        "curated": f"SELECT COUNT(*) as total FROM {database}.events_curated",
    },
    "Events by source": {
        "raw": f"SELECT source, COUNT(*) as cnt FROM {database}.events_raw GROUP BY source ORDER BY cnt DESC",
        "curated": f"SELECT source, COUNT(*) as cnt FROM {database}.events_curated GROUP BY source ORDER BY cnt DESC",
    },
    "Events by type (select specific columns)": {
        "raw": f"SELECT event_type, COUNT(*) as cnt FROM {database}.events_raw GROUP BY event_type",
        "curated": f"SELECT event_type, COUNT(*) as cnt FROM {database}.events_curated GROUP BY event_type",
    },
}

selected = st.selectbox("Choose comparison query:", list(COMPARISON_QUERIES.keys()))

if st.button("▶️ Run on Both Tables", type="primary"):
    queries = COMPARISON_QUERIES[selected]

    col1, col2 = st.columns(2)

    # Raw query
    with col1:
        st.markdown("### 📄 Raw (JSON)")
        try:
            df_raw, scanned_raw, time_raw = run_athena_query(queries["raw"], database, workgroup)
            st.metric("Data Scanned", f"{scanned_raw / 1024:.1f} KB")
            st.metric("Execution Time", f"{time_raw} ms")
            cost_raw = (scanned_raw / (1024**4)) * 5.0
            st.metric("Est. Cost", f"${cost_raw:.8f}")
            st.dataframe(df_raw, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")
            scanned_raw, time_raw = 0, 0

    # Curated query
    with col2:
        st.markdown("### 📊 Curated (Parquet)")
        try:
            df_curated, scanned_curated, time_curated = run_athena_query(queries["curated"], database, workgroup)
            st.metric("Data Scanned", f"{scanned_curated / 1024:.1f} KB")
            st.metric("Execution Time", f"{time_curated} ms")
            cost_curated = (scanned_curated / (1024**4)) * 5.0
            st.metric("Est. Cost", f"${cost_curated:.8f}")
            st.dataframe(df_curated, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")
            scanned_curated, time_curated = 0, 0

    # Savings summary
    if scanned_raw > 0 and scanned_curated > 0:
        savings_pct = ((scanned_raw - scanned_curated) / scanned_raw) * 100
        st.markdown("---")
        st.markdown("### 📉 Savings Summary")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Scan Reduction", f"{savings_pct:.0f}%")
        with col2:
            st.metric("Speed Improvement", f"{time_raw - time_curated} ms faster")
        with col3:
            st.metric("Parquet Advantage", f"{scanned_raw / max(scanned_curated, 1):.1f}x less data")

# ============================================================
# Custom SQL Editor
# ============================================================
st.markdown("---")
st.markdown("## 📝 SQL Editor")

custom_query = st.text_area(
    "Write your own query:",
    value=f"SELECT event_type, source, COUNT(*) as cnt\nFROM {database}.events_raw\nGROUP BY event_type, source\nORDER BY cnt DESC\nLIMIT 20",
    height=120,
)

if st.button("▶️ Run Custom Query"):
    with st.spinner("Executing..."):
        try:
            df, scanned, exec_time = run_athena_query(custom_query, database, workgroup)

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Data Scanned", f"{scanned / 1024:.1f} KB")
            with col2:
                st.metric("Execution Time", f"{exec_time} ms")
            with col3:
                cost = (scanned / (1024**4)) * 5.0
                st.metric("Est. Cost", f"${cost:.6f}")

            st.dataframe(df, use_container_width=True)

            # Auto-visualize
            if len(df.columns) >= 2 and len(df) > 1:
                numeric_cols = []
                for col_name in df.columns:
                    try:
                        df[col_name] = pd.to_numeric(df[col_name])
                        numeric_cols.append(col_name)
                    except (ValueError, TypeError):
                        pass

                if numeric_cols:
                    cat_col = [c for c in df.columns if c not in numeric_cols][0] if len(df.columns) > len(numeric_cols) else df.columns[0]
                    fig = px.bar(df, x=cat_col, y=numeric_cols[0], title=f"{numeric_cols[0]} by {cat_col}")
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Query failed: {e}")

# ============================================================
# SQL Optimization Tips
# ============================================================
st.markdown("---")
st.markdown("## 💡 SQL Optimization for the Exam")

st.markdown("""
| Technique | Impact | Example |
|-----------|--------|---------|
| **Use Parquet/ORC** | 80-90% less scan | Convert with Glue ETL or Firehose |
| **Partition data** | Skip irrelevant files | `WHERE year='2024' AND month='06'` |
| **Avoid SELECT *** | Read only needed columns | `SELECT event_type, count(*)` |
| **Use LIMIT** | Cap result set | `LIMIT 100` for exploration |
| **CTAS for caching** | Materialize frequent queries | `CREATE TABLE AS SELECT...` |

### Redshift Patterns (Exam Questions)
| Command | Direction | Use Case |
|---------|-----------|----------|
| **COPY** | S3 → Redshift | Load data INTO the cluster |
| **UNLOAD** | Redshift → S3 | Export data OUT (as Parquet!) |
| **Spectrum** | Query S3 in-place | JOIN external + local tables |
| **Federated Query** | Query RDS/Aurora | Live joins with operational DBs |
""")

st.markdown("""
> **Exam tip:** UNLOAD + Spectrum is the go-to pattern for moving cold data out of Redshift
> to reduce cluster costs while maintaining query access. UNLOAD as **Parquet** for best results.
""")
