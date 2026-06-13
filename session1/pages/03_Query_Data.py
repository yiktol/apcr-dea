"""
Page 3: Query Data with Athena

Covers:
- Volume: Querying unlimited data in S3 without moving it
- Value: SQL analytics on the data lake
- Serverless query execution (no infrastructure to manage)
- Cost model: pay per TB scanned
"""
import streamlit as st
import boto3
import pandas as pd
import time
import plotly.express as px

st.set_page_config(page_title="Query Data", page_icon="💎", layout="wide")

st.markdown("# 💎 Query Data (Volume + Value)")
st.markdown("""
Run SQL queries against the data lake using Amazon Athena. Data stays in S3 — 
Athena scans it on-demand — the **Value** dimension: 
extracting meaningful insights from stored data.
""")

# Architecture diagram
import os
_diagram = os.path.join(os.path.dirname(__file__), "..", "diagrams", "05_analytics_layer.png")
if os.path.exists(_diagram):
    with st.expander("📐 Architecture Diagram — Analytics Layer", expanded=False):
        st.image(_diagram, caption="Analytics Layer: S3 → Athena / Redshift / OpenSearch → BI", use_column_width=True)


def get_config():
    try:
        cfn = boto3.client("cloudformation")
        response = cfn.describe_stacks(StackName="DataEngineerPipeline")
        raw = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
        return {k.replace("Output", "", 1) if k.startswith("Output") else k: v for k, v in raw.items()}
    except Exception:
        return {
            "GlueDatabase": "dea_data_lake",
            "AthenaWorkgroup": "dea-pipeline",
            "AthenaResultsBucket": "",
        }


def run_athena_query(query, database, workgroup):
    """Execute an Athena query and return results as a DataFrame."""
    athena_client = boto3.client("athena")

    # Start query
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    query_id = response["QueryExecutionId"]

    # Poll for completion
    while True:
        result = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise Exception(f"Query {state}: {reason}")

        time.sleep(1)

    # Get results
    results = athena_client.get_query_results(QueryExecutionId=query_id)

    # Parse into DataFrame
    columns = [col["Label"] for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = []
    for row in results["ResultSet"]["Rows"][1:]:  # Skip header row
        rows.append([field.get("VarCharValue", "") for field in row["Data"]])

    # Get query stats
    stats = result["QueryExecution"]["Statistics"]
    data_scanned = stats.get("DataScannedInBytes", 0)
    execution_time = stats.get("EngineExecutionTimeInMillis", 0)

    return pd.DataFrame(rows, columns=columns), data_scanned, execution_time


# Config
config = get_config()
database = config.get("GlueDatabase", "dea_data_lake")
workgroup = config.get("AthenaWorkgroup", "dea-pipeline")

st.sidebar.markdown("### Query Settings")
st.sidebar.markdown(f"**Database:** `{database}`")
st.sidebar.markdown(f"**Workgroup:** `{workgroup}`")

# Pre-built queries
st.markdown("## 📝 Quick Queries")

SAMPLE_QUERIES = {
    "Count all events": f"SELECT COUNT(*) as total_events FROM {database}.events_raw",
    "Events by source": f"""
        SELECT source, COUNT(*) as count 
        FROM {database}.events_raw 
        GROUP BY source 
        ORDER BY count DESC
    """,
    "Events by type": f"""
        SELECT event_type, COUNT(*) as count 
        FROM {database}.events_raw 
        GROUP BY event_type 
        ORDER BY count DESC
    """,
    "Recent events (last 10)": f"""
        SELECT event_id, event_type, source, timestamp 
        FROM {database}.events_raw 
        ORDER BY timestamp DESC 
        LIMIT 10
    """,
    "Events per hour": f"""
        SELECT 
            substr(timestamp, 1, 13) as hour,
            COUNT(*) as event_count
        FROM {database}.events_raw 
        GROUP BY substr(timestamp, 1, 13)
        ORDER BY hour DESC
        LIMIT 24
    """,
    "Data volume by source": f"""
        SELECT 
            source,
            COUNT(*) as records,
            COUNT(DISTINCT device_id) as unique_devices,
            COUNT(DISTINCT user_id) as unique_users
        FROM {database}.events_raw
        GROUP BY source
    """,
}

selected_query = st.selectbox("Choose a query:", list(SAMPLE_QUERIES.keys()))

# Custom query editor
st.markdown("### SQL Editor")
query = st.text_area(
    "Query (editable):",
    value=SAMPLE_QUERIES[selected_query].strip(),
    height=120,
)

# Execute
col1, col2 = st.columns([1, 4])
with col1:
    run_button = st.button("▶️ Run Query", type="primary", use_container_width=True)

if run_button:
    with st.spinner("Executing query..."):
        try:
            df, data_scanned, exec_time = run_athena_query(query, database, workgroup)

            # Show metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Data Scanned", f"{data_scanned / 1024:.1f} KB")
            with col2:
                st.metric("Execution Time", f"{exec_time} ms")
            with col3:
                # Athena costs $5 per TB scanned
                cost = (data_scanned / (1024**4)) * 5.0
                st.metric("Est. Cost", f"${cost:.6f}")

            # Show results
            st.markdown("### Results")
            st.dataframe(df, use_container_width=True)

            # Auto-visualize if appropriate
            if len(df.columns) >= 2 and len(df) > 1:
                st.markdown("### Visualization")
                # Check if we have a count/numeric column
                numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
                if not numeric_cols:
                    # Try converting
                    for col in df.columns:
                        try:
                            df[col] = pd.to_numeric(df[col])
                            numeric_cols.append(col)
                        except (ValueError, TypeError):
                            pass

                if numeric_cols:
                    category_col = [c for c in df.columns if c not in numeric_cols][0] if len(df.columns) > len(numeric_cols) else df.columns[0]
                    value_col = numeric_cols[0]

                    fig = px.bar(
                        df, x=category_col, y=value_col,
                        title=f"{value_col} by {category_col}",
                        color=category_col,
                        color_discrete_sequence=px.colors.qualitative.Set2,
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Query failed: {e}")
            st.info("Make sure the pipeline is deployed and data has been produced.")

# Explanation
st.markdown("---")
st.markdown("## 💡 Key Concepts")
col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    ### Athena (Serverless SQL)
    - No infrastructure to manage
    - Queries S3 directly (no data movement)
    - Pay per TB scanned ($5/TB)
    - Supports JSON, Parquet, ORC, CSV
    - Uses Presto/Trino engine
    """)

with col2:
    st.markdown("""
    ### Cost Optimization Tips
    - **Use Parquet** — columnar format scans less data
    - **Partition data** — skip irrelevant date ranges
    - **Compress files** — less data = less cost
    - **Use workgroups** — set per-query data limits
    """)
