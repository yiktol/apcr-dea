"""
Page 5: Query Compare — Athena on JSON vs Parquet, Partition Pruning

Covers Task 2.1 + 2.4: Storage formats impact on query cost
- Athena query on raw JSON vs curated Parquet
- Data scanned comparison → cost impact
- Partition pruning demonstration
- Redshift Spectrum patterns
"""
import streamlit as st
import boto3
import json
import time
import pandas as pd
import plotly.express as px

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Query Compare", page_icon="⚡", layout="wide")

st.markdown("# ⚡ Query Compare (Task 2.1 + 2.4)")
st.markdown("""
Run the same SQL on **raw JSON** vs **curated Parquet** to see the real cost difference.
Athena charges **$5 per TB scanned** — format choice directly impacts your bill.
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
workgroup = config.get("AthenaWorkgroup", "dea-s3-datastore")

st.sidebar.markdown(f"**Database:** `{database}`")
st.sidebar.markdown(f"**Workgroup:** `{workgroup}`")


def run_athena_query(query, database, workgroup):
    """Execute Athena query, return DataFrame + stats."""
    athena = boto3.client("athena")
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    query_id = response["QueryExecutionId"]

    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise Exception(f"Query {state}: {reason}")
        time.sleep(1)

    results = athena.get_query_results(QueryExecutionId=query_id)
    columns = [col["Label"] for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = [[field.get("VarCharValue", "") for field in row["Data"]]
            for row in results["ResultSet"]["Rows"][1:]]

    stats = result["QueryExecution"]["Statistics"]
    data_scanned = stats.get("DataScannedInBytes", 0)
    exec_time = stats.get("EngineExecutionTimeInMillis", 0)

    return pd.DataFrame(rows, columns=columns), data_scanned, exec_time


# ============================================================
# Side-by-Side Comparison
# ============================================================
st.markdown("## 📊 JSON vs Parquet — Same Query, Different Cost")

QUERIES = {
    "Count all records": {
        "raw": f"SELECT COUNT(*) as total FROM {database}.sales_raw",
        "curated": f"SELECT COUNT(*) as total FROM {database}.sales_curated",
    },
    "Sales by category": {
        "raw": f"SELECT category, SUM(CAST(quantity AS INTEGER) * CAST(price AS DOUBLE)) as revenue FROM {database}.sales_raw GROUP BY category ORDER BY revenue DESC",
        "curated": f"SELECT category, SUM(quantity * price) as revenue FROM {database}.sales_curated GROUP BY category ORDER BY revenue DESC",
    },
    "Top products (specific columns)": {
        "raw": f"SELECT product, COUNT(*) as orders FROM {database}.sales_raw GROUP BY product ORDER BY orders DESC LIMIT 10",
        "curated": f"SELECT product, COUNT(*) as orders FROM {database}.sales_curated GROUP BY product ORDER BY orders DESC LIMIT 10",
    },
}

selected = st.selectbox("Choose comparison:", list(QUERIES.keys()))

if st.button("▶️ Run on Both Tables", type="primary"):
    queries = QUERIES[selected]
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📄 Raw (JSON)")
        try:
            df_raw, scanned_raw, time_raw = run_athena_query(queries["raw"], database, workgroup)
            st.metric("Data Scanned", f"{scanned_raw / 1024:.1f} KB")
            st.metric("Execution Time", f"{time_raw} ms")
            st.metric("Est. Cost", f"${(scanned_raw / (1024**4)) * 5:.8f}")
            st.dataframe(df_raw, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")
            scanned_raw, time_raw = 0, 0

    with col2:
        st.markdown("### 📊 Curated (Parquet)")
        try:
            df_cur, scanned_cur, time_cur = run_athena_query(queries["curated"], database, workgroup)
            st.metric("Data Scanned", f"{scanned_cur / 1024:.1f} KB")
            st.metric("Execution Time", f"{time_cur} ms")
            st.metric("Est. Cost", f"${(scanned_cur / (1024**4)) * 5:.8f}")
            st.dataframe(df_cur, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")
            scanned_cur, time_cur = 0, 0

    # Summary
    if scanned_raw > 0 and scanned_cur > 0:
        savings = ((scanned_raw - scanned_cur) / scanned_raw) * 100
        st.markdown("---")
        st.markdown("### 📉 Impact Summary")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Scan Reduction", f"{savings:.0f}%")
        with col2:
            st.metric("Parquet Advantage", f"{scanned_raw / max(scanned_cur, 1):.1f}x less")
        with col3:
            speed = time_raw - time_cur
            st.metric("Speed Gain", f"{speed} ms faster" if speed > 0 else "Similar")

st.markdown("---")

# ============================================================
# Custom Query
# ============================================================
st.markdown("## 📝 Custom SQL")

query = st.text_area(
    "Write a query:",
    value=f"SELECT category, region, COUNT(*) as cnt\nFROM {database}.sales_raw\nGROUP BY category, region\nORDER BY cnt DESC\nLIMIT 20",
    height=100,
)

if st.button("▶️ Run"):
    with st.spinner("Executing..."):
        try:
            df, scanned, exec_time = run_athena_query(query, database, workgroup)
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Scanned", f"{scanned / 1024:.1f} KB")
            with col2:
                st.metric("Time", f"{exec_time} ms")
            with col3:
                st.metric("Cost", f"${(scanned / (1024**4)) * 5:.6f}")
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Query failed: {e}")

st.markdown("---")

# ============================================================
# Redshift Spectrum Pattern
# ============================================================
st.markdown("## 🔭 Redshift Spectrum (Reference)")

st.markdown("""
Spectrum lets you query S3 data **from within Redshift** using external tables defined in the Glue Catalog.
""")

st.code("""
-- Create external schema pointing to Glue Catalog
CREATE EXTERNAL SCHEMA datalake
FROM DATA CATALOG
DATABASE 'dea_s3_datastore'
IAM_ROLE 'arn:aws:iam::123:role/RedshiftSpectrumRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Query S3 data via Spectrum
SELECT category, SUM(price * quantity) as revenue
FROM datalake.sales_curated
WHERE year = '2024' AND month = '06'
GROUP BY category;

-- JOIN local Redshift table with external S3 table
SELECT r.customer_name, s.product, s.price
FROM local_schema.customers r
JOIN datalake.sales_curated s ON r.customer_id = s.customer_id
WHERE s.year = '2024';
""", language="sql")

st.markdown("""
**When to use Spectrum:**
- Query cold/historical data in S3 without loading it into Redshift
- JOIN local cluster tables with S3 external tables
- Offload infrequently-queried data to S3 while keeping it queryable
""")

st.markdown("---")

# ============================================================
# Exam Tips
# ============================================================
st.markdown("## 💡 Exam Tips")
st.markdown("""
| Optimization | Impact |
|-------------|--------|
| **JSON → Parquet** | 80-90% less scan (columnar reads only needed columns) |
| **Partition by date** | Athena skips entire folders when filtering by partition key |
| **Snappy compression** | ~75% smaller files, Athena scans less |
| **Avoid SELECT *** | Each column = more data scanned in columnar format |
| **CTAS** | `CREATE TABLE AS SELECT` to materialize frequent query results |
| **Spectrum** | Query S3 without COPY into Redshift — massively parallel on Spectrum nodes |
| **Federated Query** | Query RDS/Aurora FROM Redshift (not S3!) — common exam confusion |
""")
