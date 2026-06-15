"""
Session 2 - ETL Pipeline Orchestration

End-to-end data pipeline: IoT Ingestion → Catalog → Transform → Orchestrate → Query → Visualize
"""
import streamlit as st
import boto3
import os

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(
    page_title="ETL Pipeline - Session 2",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown("""
<style>
:root {
    --aws-orange: #FF9900;
    --aws-blue: #232F3E;
}
.main-header {
    background: linear-gradient(135deg, #232F3E 0%, #37474f 100%);
    padding: 1.5rem;
    border-radius: 10px;
    color: white;
    text-align: center;
    margin-bottom: 1.5rem;
}
.step-card {
    padding: 1rem;
    border-radius: 8px;
    text-align: center;
    color: white;
    font-weight: bold;
    margin: 0.25rem;
}
.metric-card {
    background: white;
    padding: 1rem;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    border-top: 3px solid #FF9900;
    text-align: center;
}
</style>
""", unsafe_allow_html=True)


def get_config():
    """Load pipeline configuration from CDK outputs."""
    if "config" not in st.session_state:
        try:
            cfn = boto3.client("cloudformation")
            response = cfn.describe_stacks(StackName="DeaSession2Pipeline")
            raw = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
            st.session_state.config = raw
        except Exception:
            st.session_state.config = {
                "StreamName": "dea-s2-ingestion-stream",
                "GlueDatabase": "dea_s2_pipeline",
                "AthenaWorkgroup": "dea-s2-pipeline",
            }
    return st.session_state.config


def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>🔄 ETL Pipeline Orchestration</h1>
        <h3>Domain 1: Data Ingestion and Transformation</h3>
        <p>Data Engineer Associate — Session 2</p>
    </div>
    """, unsafe_allow_html=True)

    # Pipeline steps visual
    cols = st.columns(6)
    steps = [
        ("📡 Ingest", "#e65100", "IoT → Kinesis"),
        ("📚 Catalog", "#1565c0", "Glue Crawler"),
        ("🔄 Transform", "#2e7d32", "JSON → Parquet"),
        ("🎭 Orchestrate", "#6a1b9a", "Step Functions"),
        ("💎 Query", "#c62828", "Athena SQL"),
        ("📊 Visualize", "#00695c", "QuickSight"),
    ]
    for col, (label, color, desc) in zip(cols, steps):
        with col:
            st.markdown(
                f'<div class="step-card" style="background:{color};">'
                f"{label}<br><small>{desc}</small></div>",
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # Pipeline overview
    config = get_config()

    st.markdown("## Pipeline Overview")
    st.markdown("""
    This demo walks through a complete serverless data pipeline using 
    **self-driving car telemetry** from the IoT Device Simulator:
    
    1. **Ingest** — IoT devices publish to IoT Core → Kinesis → Firehose → S3
    2. **Crawl & Catalog** — Glue Crawler discovers schema and partitions
    3. **Transform** — Glue ETL converts raw JSON to optimized Parquet
    4. **Orchestrate** — Step Functions coordinates Validate → ETL → Notify
    5. **Query** — Athena SQL compares raw vs curated cost
    6. **Visualize** — QuickSight dashboards from curated data
    """)

    # Architecture diagram
    st.markdown("## Architecture")
    diagram_path = os.path.join(os.path.dirname(__file__), "diagrams", "00_full_pipeline.png")
    if os.path.exists(diagram_path):
        st.image(diagram_path, caption="End-to-End Pipeline Architecture", use_container_width=True)
    else:
        st.info("Run `create_diagrams.py` in `diagrams/` to generate the architecture diagram.")

    # Deployment status
    st.markdown("## Deployment Status")
    col1, col2, col3 = st.columns(3)

    with col1:
        stream = config.get("StreamName", "Not deployed")
        st.markdown(f"""
        <div class="metric-card">
            <h4>📡 Kinesis Stream</h4>
            <p><code>{stream}</code></p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        bucket = config.get("DataLakeBucket", "Not deployed")
        display_bucket = bucket[:30] + "..." if len(bucket) > 30 else bucket
        st.markdown(f"""
        <div class="metric-card">
            <h4>🗄️ Data Lake Bucket</h4>
            <p><code>{display_bucket}</code></p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        workgroup = config.get("AthenaWorkgroup", "Not deployed")
        st.markdown(f"""
        <div class="metric-card">
            <h4>💎 Athena Workgroup</h4>
            <p><code>{workgroup}</code></p>
        </div>
        """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
