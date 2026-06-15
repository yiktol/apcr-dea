"""
Page 3: Orchestrate — Step Functions Workflow

Covers Task 1.3: Orchestrate data pipelines
- Step Functions state machine execution
- Standard vs Express workflows
- Error handling and retry logic
- Event-driven triggers (EventBridge)
- Pipeline comparison: Glue Workflow vs Step Functions vs MWAA
"""
import streamlit as st
import boto3
import json
import time
import pandas as pd
from datetime import datetime, timezone

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Orchestrate Pipeline", page_icon="🎭", layout="wide")

st.markdown("# 🎭 Orchestrate Pipeline (Task 1.3)")
st.markdown("""
Execute the **Step Functions** state machine that coordinates the full ETL workflow:
**Validate → Transform (Glue) → Notify**.
Watch error handling and retry logic in action.
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
state_machine_arn = config.get("StateMachineArn", "")
state_machine_name = config.get("StateMachineName", "dea-s2-etl-orchestrator")


# ============================================================
# Workflow Visualization
# ============================================================
st.markdown("## 🗺️ Workflow Architecture")

import utils.common as common

workflow_mermaid = """
graph TD
    Start([▶ Start]) --> Validate[🔍 Validate Data]
    Validate --> Decision{Valid?}
    Decision -->|Yes| Transform[🔄 Glue ETL Job]
    Decision -->|No/Skip| End([⏹ End])
    Transform --> NotifySuccess[✅ Notify Success]
    Transform -->|Error| HandleError[❌ Handle Error]
    HandleError --> NotifyFailure[🚨 Notify Failure]
    NotifySuccess --> End
    NotifyFailure --> End

    classDef active fill:#4CAF50,stroke:#2E7D32,color:#fff
    classDef error fill:#F44336,stroke:#C62828,color:#fff
    classDef process fill:#2196F3,stroke:#1565C0,color:#fff
    
    class Validate,Transform process
    class NotifySuccess active
    class HandleError,NotifyFailure error
"""

common.mermaid(workflow_mermaid, height=450, show_controls=False)

st.markdown("---")

# ============================================================
# Execute Workflow
# ============================================================
st.markdown("## 🚀 Execute Workflow")

col1, col2 = st.columns([1, 1])

with col1:
    if st.button("▶️ Start State Machine", type="primary", use_container_width=True):
        if not state_machine_arn:
            st.error("State machine ARN not found. Deploy the stack first.")
        else:
            try:
                sfn_client = boto3.client("stepfunctions")
                exec_name = f"demo-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
                response = sfn_client.start_execution(
                    stateMachineArn=state_machine_arn,
                    name=exec_name,
                    input=json.dumps({"source": "streamlit-demo", "timestamp": datetime.now(timezone.utc).isoformat()}),
                )
                st.session_state["s2_sfn_exec_arn"] = response["executionArn"]
                st.success(f"✅ Execution started: `{exec_name}`")
            except Exception as e:
                st.error(f"Failed to start: {e}")

with col2:
    st.markdown("""
    **Workflow steps:**
    1. **Validate** — Check raw zone has data, validate schema
    2. **Transform** — Run Glue ETL (JSON → Parquet)
    3. **Notify** — Send SNS on success or failure
    """)

# Execution Status
st.markdown("---")
st.markdown("## 📈 Execution Status")

exec_arn = st.session_state.get("s2_sfn_exec_arn", "")

if exec_arn:
    try:
        sfn_client = boto3.client("stepfunctions")
        response = sfn_client.describe_execution(executionArn=exec_arn)

        status = response["status"]
        start_time = response.get("startDate", "")
        stop_time = response.get("stopDate", "")

        status_icons = {
            "RUNNING": "🔵", "SUCCEEDED": "🟢",
            "FAILED": "🔴", "TIMED_OUT": "🟠", "ABORTED": "⚪",
        }
        icon = status_icons.get(status, "⚪")

        st.markdown(f"### {icon} {status}")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Execution Name", response["name"])
        with col2:
            if start_time and stop_time:
                duration = (stop_time - start_time).total_seconds()
                st.metric("Duration", f"{duration:.1f}s")
            else:
                st.metric("Duration", "Running...")
        with col3:
            st.metric("Status", status)

        # Show execution output
        if status == "SUCCEEDED":
            output = json.loads(response.get("output", "{}"))
            with st.expander("📄 Execution Output", expanded=True):
                st.json(output)

        elif status == "FAILED":
            st.error(f"Error: {response.get('error', 'Unknown')}")
            cause = response.get("cause", "No details")
            st.code(cause, language="json")

        elif status == "RUNNING":
            if st.button("🔄 Refresh"):
                st.rerun()

        # Execution history
        st.markdown("#### Step History")
        history = sfn_client.get_execution_history(
            executionArn=exec_arn,
            maxResults=20,
            reverseOrder=False,
        )
        events = history.get("events", [])
        if events:
            event_data = []
            for evt in events:
                event_data.append({
                    "Time": evt["timestamp"].strftime("%H:%M:%S"),
                    "Type": evt["type"],
                    "ID": evt["id"],
                })
            st.dataframe(pd.DataFrame(event_data), use_container_width=True)

    except Exception as e:
        st.warning(f"Could not fetch execution status: {e}")
else:
    st.info("No execution started yet. Click 'Start State Machine' above.")

# ============================================================
# Recent Executions
# ============================================================
st.markdown("---")
st.markdown("## 📋 Recent Executions")

if state_machine_arn:
    try:
        sfn_client = boto3.client("stepfunctions")
        response = sfn_client.list_executions(
            stateMachineArn=state_machine_arn,
            maxResults=10,
        )
        executions = response.get("executions", [])
        if executions:
            exec_data = []
            for ex in executions:
                exec_data.append({
                    "Name": ex["name"],
                    "Status": ex["status"],
                    "Started": ex["startDate"].strftime("%Y-%m-%d %H:%M:%S"),
                    "Stopped": ex.get("stopDate", "").strftime("%H:%M:%S") if ex.get("stopDate") else "Running",
                })
            st.dataframe(pd.DataFrame(exec_data), use_container_width=True)
        else:
            st.info("No executions yet.")
    except Exception as e:
        st.info(f"Could not list executions: {e}")

# ============================================================
# Orchestration Comparison
# ============================================================
st.markdown("---")
st.markdown("## 💡 Orchestration Options Comparison")

st.markdown("""
| Factor | AWS Glue Workflow | AWS Step Functions | Amazon MWAA (Airflow) |
|--------|------------------|-------------------|----------------------|
| **Best for** | Glue jobs + crawlers only | Multi-service integration | Reuse existing Airflow DAGs |
| **Infrastructure** | Serverless | Serverless | Managed (EC2 under the hood) |
| **Pricing** | Per job/crawler run | Per state transition | Per environment hour |
| **Error handling** | Basic retry | Rich (Catch, Retry, Fallback) | DAG-level retry |
| **Visualization** | Console graph | Real-time visual debugging | Airflow UI |
| **Workflow types** | N/A | Standard (1yr) / Express (5min) | DAGs |
""")

st.markdown("""
> **Exam tip:** Step Functions **Standard** = exactly-once, up to 1 year, with execution history.
> **Express** = at-least-once, up to 5 min, for high-event-rate workloads like streaming.
""")
