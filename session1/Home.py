"""
Session 1 - 5 Vs of Big Data Pipeline

End-to-end data pipeline covering the 5 V's of Big Data.
"""
import streamlit as st
import boto3
import json

st.set_page_config(
    page_title="Data Pipeline - 5 V's of Big Data",
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
.v-card {
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
    """Load pipeline configuration from CDK outputs or environment."""
    if "config" not in st.session_state:
        # Try to load from CloudFormation outputs
        try:
            cfn = boto3.client("cloudformation")
            response = cfn.describe_stacks(StackName="DataEngineerPipeline")
            raw = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
            # Normalize keys — strip "Output" prefix
            config = {}
            for k, v in raw.items():
                normalized = k.replace("Output", "", 1) if k.startswith("Output") else k
                config[normalized] = v
            st.session_state.config = config
        except Exception:
            # Fallback - manual config
            st.session_state.config = {
                "StreamName": "dea-ingestion-stream",
                "GlueDatabase": "dea_data_lake",
                "AthenaWorkgroup": "dea-pipeline",
            }
    return st.session_state.config


def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>🔄 Data Pipeline</h1>
        <h3>5 V's of Big Data on AWS</h3>
        <p>Data Engineer Associate — Session 1</p>
    </div>
    """, unsafe_allow_html=True)

    # 5 V's visual
    cols = st.columns(5)
    v_data = [
        ("📊 Volume", "#2e7d32", "S3 Data Lake"),
        ("🔀 Variety", "#1565c0", "Multiple Sources"),
        ("⚡ Velocity", "#e65100", "Kinesis + Lambda"),
        ("✅ Veracity", "#c62828", "Macie + Masking"),
        ("💎 Value", "#6a1b9a", "Athena + Redshift"),
    ]
    for col, (label, color, desc) in zip(cols, v_data):
        with col:
            st.markdown(
                f'<div class="v-card" style="background:{color};">'
                f"{label}<br><small>{desc}</small></div>",
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # Pipeline status
    config = get_config()

    st.markdown("## Pipeline Overview")
    st.markdown("""
    This deploys a complete data pipeline covering every core concept 
    from Session 1. Use the sidebar pages to interact with each layer:
    
    1. **Produce Data** — Push records into Kinesis (Velocity + Variety)
    2. **Monitor Pipeline** — Watch Lambda processing in real-time (Velocity)
    3. **Query Data** — Run Athena SQL on the data lake (Volume + Value)
    4. **Governance** — See PII detection and masking in action (Veracity)
    """)

    # Architecture diagram
    st.markdown("## Architecture")
    import os
    diagram_path = os.path.join(os.path.dirname(__file__), "diagrams", "01_overall_pipeline.png")
    if os.path.exists(diagram_path):
        st.image(diagram_path, caption="End-to-End Pipeline Architecture", width="stretch")
    else:
        st.info("Run `create_diagrams.py` in `diagrams/` to generate the architecture diagram.")

    # Quick status check
    st.markdown("## Deployment Status")
    col1, col2, col3 = st.columns(3)

    with col1:
        stream_name = config.get("StreamName", "Not deployed")
        st.markdown(f"""
        <div class="metric-card">
            <h4>⚡ Kinesis Stream</h4>
            <p><code>{stream_name}</code></p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        bucket = config.get("DataLakeBucket", "Not deployed")
        st.markdown(f"""
        <div class="metric-card">
            <h4>📊 Data Lake Bucket</h4>
            <p><code>{bucket[:30]}...</code></p>
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
