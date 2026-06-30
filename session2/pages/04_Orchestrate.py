"""
Page 4: Orchestrate — Step Functions coordinates the pipeline

Pipeline step: Step Functions state machine: Validate → Glue ETL → Notify
"""
import streamlit as st
import boto3
import json
import time
import pandas as pd
from datetime import datetime, timezone

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Orchestrate", page_icon="🎭", layout="wide")

st.markdown("# 🎭 Step 4: Orchestrate")
st.markdown("""
**AWS Step Functions** coordinates the full ETL workflow with error handling and retry logic.
The state machine validates data, runs the Glue ETL job, and sends notifications.
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
state_machine_arn = config.get("OutputStateMachineArn", "")

# ============================================================
# Architecture
# ============================================================
st.markdown("## Architecture")

import os
_diagram = os.path.join(os.path.dirname(__file__), "..", "diagrams", "04_orchestrate.png")
if os.path.exists(_diagram):
    st.image(_diagram, caption="Orchestrate: EventBridge → Step Functions (Validate → Glue ETL → Notify)", use_container_width=True)
else:
    st.info("Run `create_diagrams.py` in `diagrams/` to generate architecture diagrams.")

st.markdown("---")

# ============================================================
# State Machine Workflow
# ============================================================
st.markdown("## 🗺️ State Machine Detail")

import os
_sm_diagram = os.path.join(os.path.dirname(__file__), "..", "diagrams", "04_state_machine.png")
if os.path.exists(_sm_diagram):
    st.image(_sm_diagram, caption="Step Functions Workflow: Validate → Glue ETL (with retry) → Notify", width=500)
else:
    st.info("Run `create_state_machine.py` in `diagrams/` to generate the state machine diagram.")

st.markdown("---")

# ============================================================
# Execute State Machine
# ============================================================
st.markdown("## 🚀 Execute Pipeline")

col1, col2 = st.columns([1, 1])

with col1:
    if st.button("▶️ Start State Machine", type="primary", use_container_width=True):
        if not state_machine_arn:
            st.error("State machine ARN not found. Deploy the stack first.")
        else:
            try:
                sfn = boto3.client("stepfunctions")
                # Check if an execution is already running
                running = sfn.list_executions(
                    stateMachineArn=state_machine_arn,
                    statusFilter="RUNNING",
                    maxResults=1,
                )
                active = running.get("executions", [])

                if active:
                    existing_arn = active[0]["executionArn"]
                    existing_name = active[0]["name"]
                    st.session_state["s2_sfn_arn"] = existing_arn
                    st.info(f"⏳ Execution already running: `{existing_name}`. Showing status below.")
                else:
                    exec_name = f"demo-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
                    response = sfn.start_execution(
                        stateMachineArn=state_machine_arn,
                        name=exec_name,
                        input=json.dumps({"source": "streamlit", "timestamp": datetime.now(timezone.utc).isoformat()}),
                    )
                    st.session_state["s2_sfn_arn"] = response["executionArn"]
                    st.success(f"✅ Started: `{exec_name}`")
            except Exception as e:
                st.error(f"Error: {e}")

with col2:
    exec_arn = st.session_state.get("s2_sfn_arn", "")
    if exec_arn:
        if st.button("🔄 Check Execution", use_container_width=True):
            try:
                sfn = boto3.client("stepfunctions")
                response = sfn.describe_execution(executionArn=exec_arn)
                status = response["status"]
                icons = {"RUNNING": "🔵", "SUCCEEDED": "🟢", "FAILED": "🔴", "TIMED_OUT": "🟠"}
                st.markdown(f"### {icons.get(status, '⚪')} {status}")

                if response.get("stopDate") and response.get("startDate"):
                    duration = (response["stopDate"] - response["startDate"]).total_seconds()
                    st.write(f"Duration: {duration:.1f}s")

                if status == "SUCCEEDED" and response.get("output"):
                    with st.expander("Output", expanded=True):
                        st.json(json.loads(response["output"]))
                elif status == "FAILED":
                    st.error(f"Error: {response.get('error', 'Unknown')}")
            except Exception as e:
                st.error(f"Error: {e}")

st.markdown("---")

# ============================================================
# Orchestration Comparison
# ============================================================
st.markdown("## 📊 Orchestration Options")

st.markdown("""
| Factor | Step Functions | Glue Workflow | MWAA (Airflow) |
|--------|---------------|--------------|----------------|
| **Best for** | Multi-service integration | Glue-only pipelines | Reuse existing DAGs |
| **Infrastructure** | Serverless | Serverless | Managed (EC2) |
| **Error handling** | Rich (Catch, Retry, Fallback) | Basic retry | DAG-level retry |
| **Visualization** | Real-time execution graph | Console graph | Airflow UI |
| **Pricing** | Per state transition | Per job run | Per environment hour |
| **Max duration** | Standard: 1 year / Express: 5 min | Job timeout | Unlimited |
""")

st.info("""
💡 **Exam tip:** Step Functions **Standard** = exactly-once, 1 year max, execution history. 
**Express** = at-least-once, 5 min max, for high-event-rate streaming workloads.
""")

st.markdown("---")

# ============================================================
# Exam Tips
# ============================================================
st.markdown("## 💡 Exam Tips")
st.markdown("""
| Concept | Key Point |
|---------|-----------|
| **Step Functions** | Serverless orchestration. Integrates with 200+ AWS services directly. |
| **Standard vs Express** | Standard for long ETL (audit trail). Express for streaming (high throughput). |
| **Error handling** | Catch + Retry with backoff. ResultPath for error details. |
| **EventBridge trigger** | Schedule or event-driven start (S3 PutObject → run pipeline). |
| **Glue Workflow** | Built-in option when all steps are Glue jobs/crawlers. |
| **MWAA** | Use when migrating existing Airflow DAGs to AWS. |
""")
