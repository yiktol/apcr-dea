"""
Page 6: Visualize — QuickSight dashboards from the pipeline data

Pipeline step: Athena (curated data) → QuickSight → Business Dashboards
"""
import streamlit as st
import boto3
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Visualize", page_icon="📊", layout="wide")

st.markdown("# 📊 Step 6: Visualize")
st.markdown("""
The final step: turn curated data into **business insights** using Amazon QuickSight
or any BI tool connected to Athena. Here we simulate what a vehicle fleet dashboard looks like.
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
_diagram = os.path.join(os.path.dirname(__file__), "..", "diagrams", "06_visualize.png")
if os.path.exists(_diagram):
    st.image(_diagram, caption="Visualize: S3 Curated → Athena → QuickSight → Stakeholders", use_container_width=True)
else:
    st.info("Run `create_diagrams.py` in `diagrams/` to generate architecture diagrams.")

st.markdown("---")

# ============================================================
# Live Data Visualization (from Athena)
# ============================================================
st.markdown("## 📈 Fleet Telemetry Dashboard")
st.markdown("*Querying curated Parquet data via Athena and visualizing in real-time:*")


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
            return None
        time.sleep(1)
    results = athena.get_query_results(QueryExecutionId=qid)
    columns = [c["Label"] for c in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = [[f.get("VarCharValue", "") for f in r["Data"]] for r in results["ResultSet"]["Rows"][1:]]
    return pd.DataFrame(rows, columns=columns)


if st.button("📊 Load Dashboard Data", type="primary"):
    with st.spinner("Querying Athena..."):
        # Events by source
        df_source = run_query(f"SELECT source, COUNT(*) as cnt FROM {database}.events_curated GROUP BY source ORDER BY cnt DESC LIMIT 10")
        # Events by type
        df_type = run_query(f"SELECT event_type, COUNT(*) as cnt FROM {database}.events_curated GROUP BY event_type ORDER BY cnt DESC LIMIT 10")
        # Events over time
        df_time = run_query(f"SELECT year, month, day, COUNT(*) as cnt FROM {database}.events_curated GROUP BY year, month, day ORDER BY year, month, day LIMIT 30")

    if df_source is not None and len(df_source) > 0:
        col1, col2 = st.columns(2)

        with col1:
            # Convert cnt to numeric
            df_source["cnt"] = pd.to_numeric(df_source["cnt"], errors="coerce")
            fig = px.pie(df_source, names="source", values="cnt", title="Events by Source")
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            if df_type is not None and len(df_type) > 0:
                df_type["cnt"] = pd.to_numeric(df_type["cnt"], errors="coerce")
                fig = px.bar(df_type, x="event_type", y="cnt", title="Events by Type",
                            color="event_type", color_discrete_sequence=px.colors.qualitative.Set2)
                fig.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        if df_time is not None and len(df_time) > 0:
            df_time["cnt"] = pd.to_numeric(df_time["cnt"], errors="coerce")
            df_time["date"] = df_time["year"] + "-" + df_time["month"].str.zfill(2) + "-" + df_time["day"].str.zfill(2)
            fig = px.area(df_time, x="date", y="cnt", title="Events Over Time")
            fig.update_traces(fill="tozeroy", line_color="#FF9900")
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No curated data available yet. Run the ETL job in Step 3 first.")

st.markdown("---")

# ============================================================
# QuickSight Setup Guide
# ============================================================
st.markdown("## 🛠️ QuickSight Setup")

st.markdown("""
### Connecting QuickSight to Your Pipeline

| Step | Action |
|------|--------|
| 1 | Open QuickSight → **Datasets** → **New Dataset** |
| 2 | Select **Athena** as data source |
| 3 | Choose workgroup: `dea-s2-pipeline` |
| 4 | Select database: `dea_s2_pipeline` |
| 5 | Choose table: `events_curated` |
| 6 | Select **SPICE** (in-memory) or **Direct Query** |
| 7 | Create analysis → auto-suggested visualizations |
| 8 | Publish as dashboard → share with stakeholders |

### SPICE vs Direct Query

| Mode | Behavior | Best For |
|------|----------|----------|
| **SPICE** | Imports data into in-memory cache | Fast dashboards, scheduled refresh |
| **Direct Query** | Queries Athena live on each interaction | Always-current data, large datasets |
""")

st.markdown("---")

# ============================================================
# Vehicle Fleet Dashboard Mockup
# ============================================================
st.markdown("## 🚗 Fleet Dashboard (What It Could Look Like)")

col1, col2, col3, col4 = st.columns(4)

import random

with col1:
    st.metric("Active Vehicles", random.randint(30, 50), delta=f"+{random.randint(1,5)}")
with col2:
    st.metric("Avg Speed", f"{random.uniform(45, 75):.1f} km/h")
with col3:
    st.metric("Avg Fuel Level", f"{random.uniform(50, 80):.0f}%", delta=f"-{random.randint(1,3)}%")
with col4:
    st.metric("Trips Today", random.randint(100, 300))

# Simulated speed distribution
speeds = [random.gauss(60, 15) for _ in range(200)]
fig = go.Figure(data=[go.Histogram(x=speeds, nbinsx=20, marker_color="#FF9900")])
fig.update_layout(title="Vehicle Speed Distribution", xaxis_title="Speed (km/h)",
                  yaxis_title="Count", height=300)
st.plotly_chart(fig, use_container_width=True)

st.caption("*Simulated data — in production, this would query live curated Parquet via Athena/SPICE*")

st.markdown("---")

# ============================================================
# Exam Tips
# ============================================================
st.markdown("## 💡 Exam Tips")
st.markdown("""
| Concept | Key Point |
|---------|-----------|
| **QuickSight** | Serverless BI. Connects to Athena, Redshift, RDS, S3, and more. |
| **SPICE** | In-memory cache. Fast dashboards. Scheduled or manual refresh. |
| **Direct Query** | Live queries to source. Always current. Slower for large datasets. |
| **Row-Level Security** | Control which rows users can see based on their identity. |
| **Embedded dashboards** | Embed QuickSight visuals in your own applications. |
| **ML Insights** | Built-in anomaly detection, forecasting, and narratives. |
""")

# ============================================================
# Full Pipeline Summary
# ============================================================
st.markdown("---")
st.markdown("## 🔄 Complete Pipeline")

_full = os.path.join(os.path.dirname(__file__), "..", "diagrams", "00_full_pipeline.png")
if os.path.exists(_full):
    st.image(_full, caption="End-to-End: IoT Devices → IoT Core → Kinesis → Lambda → S3 → Crawler → Catalog → Step Functions → Glue ETL → Athena → QuickSight", use_container_width=True)

st.success("✅ **Pipeline complete!** Data flows from IoT devices through ingestion, cataloging, transformation, orchestration, querying, and visualization — all serverless.")
