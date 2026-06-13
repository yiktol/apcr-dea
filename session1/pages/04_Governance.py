"""
Page 4: Governance - PII Detection & Masking

Covers:
- Veracity: Data quality and trustworthiness
- Amazon Macie for PII discovery
- EventBridge for event-driven automation
- Lambda for real-time masking
- This is the exact pattern from Exam Q3 (Personally Identified)
"""
import streamlit as st
import boto3
import json
import re
import hashlib
import pandas as pd
from datetime import datetime, timezone

st.set_page_config(page_title="Governance", page_icon="🔒", layout="wide")

st.markdown("# 🔒 Governance (Veracity)")
st.markdown("""
Automated PII detection and masking — the exact architecture from 
**Exam Question 3** (Personally Identified). 

**Pattern:** Macie → EventBridge → Lambda Masker → Clean S3 Bucket
""")

# Architecture diagram
import os
_diagram = os.path.join(os.path.dirname(__file__), "..", "diagrams", "06_governance_layer.png")
if os.path.exists(_diagram):
    with st.expander("📐 Architecture Diagram — Governance Layer", expanded=False):
        st.image(_diagram, caption="Governance: S3 → Macie → EventBridge → Lambda Masker → Clean Bucket", use_column_width=True)


def get_config():
    try:
        cfn = boto3.client("cloudformation")
        response = cfn.describe_stacks(StackName="DataEngineerPipeline")
        raw = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
        return {k.replace("Output", "", 1) if k.startswith("Output") else k: v for k, v in raw.items()}
    except Exception:
        return {}


# PII patterns (same as masker Lambda)
PATTERNS = {
    "SSN": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "Email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    "Phone": re.compile(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b"),
    "Credit Card": re.compile(r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b"),
}


def mask_value(value, pii_type):
    """Mask a PII value with a hash token."""
    token = hashlib.sha256(value.encode()).hexdigest()[:8]
    return f"[{pii_type}_MASKED_{token}]"


def scan_for_pii(text):
    """Scan text for PII and return findings."""
    findings = []
    for pii_type, pattern in PATTERNS.items():
        matches = pattern.findall(text)
        for match in matches:
            findings.append({"type": pii_type, "value": match})
    return findings


def mask_text(text):
    """Apply masking to all PII in text."""
    masked = text
    for pii_type, pattern in PATTERNS.items():
        matches = pattern.findall(masked)
        for match in matches:
            masked = masked.replace(match, mask_value(match, pii_type))
    return masked


config = get_config()
data_lake_bucket = config.get("DataLakeBucket", "")
masked_bucket = config.get("MaskedBucket", "")

# Tabs
tab1, tab2, tab3 = st.tabs(["🔍 PII Scanner", "📤 Upload & Detect", "📊 Findings Dashboard"])

with tab1:
    st.markdown("### Interactive PII Detection & Masking")
    st.markdown("""
    Paste sample data below to see how the masking Lambda processes it. 
    This simulates what happens when Macie detects PII and triggers the masking function.
    """)

    # Sample data with PII
    sample_data = """{"name": "John Smith", "email": "john.smith@company.com", "ssn": "123-45-6789", "phone": "555-123-4567"}
{"name": "Jane Doe", "email": "jane.doe@example.org", "ssn": "987-65-4321", "phone": "555-987-6543"}
{"name": "Bob Wilson", "email": "bob@test.com", "ssn": "456-78-9012", "card": "4532-1234-5678-9012"}"""

    input_data = st.text_area(
        "Input Data (JSON lines with PII):",
        value=sample_data,
        height=150,
    )

    if st.button("🔍 Scan & Mask PII", type="primary"):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 🚨 PII Findings")
            findings = scan_for_pii(input_data)

            if findings:
                df = pd.DataFrame(findings)
                st.dataframe(df, use_container_width=True)

                # Summary
                summary = df.groupby("type").size().reset_index(name="count")
                st.bar_chart(summary.set_index("type"))
            else:
                st.success("No PII detected!")

        with col2:
            st.markdown("#### ✅ Masked Output")
            masked = mask_text(input_data)
            st.code(masked, language="json")

            st.metric("PII Instances Masked", len(findings))

with tab2:
    st.markdown("### Upload Data to S3 (Trigger Macie)")
    st.markdown("""
    Upload a file to the data lake's raw zone. If Macie is enabled, it will automatically 
    scan the file and trigger the masking pipeline when PII is detected.
    
    **Flow:** S3 Upload → Macie Scan → EventBridge Rule → Masking Lambda → Clean Bucket
    """)

    uploaded_file = st.file_uploader("Upload CSV or JSON with sample PII data", type=["csv", "json", "txt"])

    if uploaded_file and data_lake_bucket:
        content = uploaded_file.read().decode("utf-8")
        st.markdown("#### Preview:")
        st.code(content[:500], language="json")

        # Show PII scan preview
        findings = scan_for_pii(content)
        if findings:
            st.warning(f"⚠️ Detected **{len(findings)} PII instances** in this file!")
            df = pd.DataFrame(findings)
            st.dataframe(df.head(20), use_container_width=True)

        if st.button("📤 Upload to Data Lake", type="primary"):
            try:
                s3 = boto3.client("s3")
                now = datetime.now(timezone.utc)
                key = f"raw/{now.year}/{now.month:02d}/{now.day:02d}/uploaded_{uploaded_file.name}"

                s3.put_object(
                    Bucket=data_lake_bucket,
                    Key=key,
                    Body=content.encode("utf-8"),
                    ContentType="application/json",
                )
                st.success(f"✅ Uploaded to `s3://{data_lake_bucket}/{key}`")
                st.info(
                    "Macie will scan this file automatically (if enabled). "
                    "When PII is found, EventBridge triggers the masking Lambda."
                )
            except Exception as e:
                st.error(f"Upload failed: {e}")

    elif not data_lake_bucket:
        st.warning("Pipeline not deployed. Run `cdk deploy` first.")

with tab3:
    st.markdown("### Macie Findings & Masked Objects")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Recent Macie Findings")
        if data_lake_bucket:
            try:
                # Check if Macie is enabled and has findings
                macie = boto3.client("macie2")
                response = macie.list_findings(
                    findingCriteria={
                        "criterion": {
                            "resourcesAffected.s3Bucket.name": {
                                "eq": [data_lake_bucket]
                            }
                        }
                    },
                    maxResults=10,
                )
                finding_ids = response.get("findingIds", [])

                if finding_ids:
                    findings_detail = macie.get_findings(findingIds=finding_ids)
                    for finding in findings_detail.get("findings", []):
                        severity = finding.get("severity", {}).get("description", "Unknown")
                        finding_type = finding.get("type", "Unknown")
                        st.markdown(
                            f"- **{severity}**: {finding_type} "
                            f"(`{finding.get('resourcesAffected', {}).get('s3Object', {}).get('key', 'unknown')}`)"
                        )
                else:
                    st.info("No Macie findings yet. Upload some PII data or wait for scheduled scan.")

            except Exception as e:
                st.info(
                    "Macie findings unavailable. Macie may not be enabled yet.\n\n"
                    "To enable: AWS Console → Macie → Enable → Create discovery job for the data lake bucket."
                )

    with col2:
        st.markdown("#### Masked Objects")
        if masked_bucket:
            try:
                s3 = boto3.client("s3")
                response = s3.list_objects_v2(Bucket=masked_bucket, Prefix="masked/", MaxKeys=20)
                objects = response.get("Contents", [])

                if objects:
                    for obj in objects:
                        st.markdown(f"- `{obj['Key']}` ({obj['Size']} bytes, {obj['LastModified'].strftime('%Y-%m-%d %H:%M')})")
                else:
                    st.info("No masked objects yet. Upload PII data and let the pipeline process it.")
            except Exception as e:
                st.info("Masked bucket not accessible.")
        else:
            st.warning("Pipeline not deployed.")

# Exam reference
st.markdown("---")
st.markdown("## 📝 Exam Connection")
st.markdown("""
**Question:** A company wants automated PII detection with least operational overhead. 
The masking application already exists. Which solution?

**Answer:** Enable Amazon Macie → Create EventBridge rule for Macie findings → 
Set the masking application as the target.

**Why this is correct:**
- Macie provides ML-powered PII discovery (no custom detection logic)
- EventBridge enables real-time, event-driven invocation (no polling)
- Minimal operational overhead (fully managed services)

**Why others are wrong:**
- Lambda + S3 notifications: You'd have to build PII detection logic yourself
- EventBridge for S3 uploads (no Macie): Triggers on all uploads, not just PII
- Macie + polling Lambda: Higher overhead than EventBridge (scheduled, not real-time)
""")
