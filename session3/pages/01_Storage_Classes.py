"""
Page 1: Storage Classes — S3 Tiers, Versioning, Access Patterns

Covers Task 2.1: Choose a data store
- S3 storage classes comparison
- Upload objects to different prefixes/tiers
- Versioning and delete markers demo
- Object/Block/File storage comparison
"""
import streamlit as st
import boto3
import json
import uuid
import time
from datetime import datetime, timezone

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Storage Classes", page_icon="📦", layout="wide")

st.markdown("# 📦 Storage Classes & Access Patterns (Task 2.1)")
st.markdown("""
Explore S3 storage classes, lifecycle transitions, and versioning behavior.
Understand when to use each tier based on access frequency and cost.
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
data_lake_bucket = config.get("DataLakeBucket", "")
versioned_bucket = config.get("VersionedBucket", "")

# ============================================================
# Storage Classes Comparison
# ============================================================
st.markdown("## 📊 S3 Storage Classes Comparison")

st.markdown("""
| Storage Class | Access | Retrieval | Min Duration | Use Case |
|--------------|--------|-----------|--------------|----------|
| **S3 Standard** | Frequent | ms, no fee | None | Hot data, active workloads |
| **S3 Intelligent-Tiering** | Unknown | ms, no fee | 30 days | Unpredictable access |
| **S3 Standard-IA** | Monthly | ms, per-GB fee | 30 days | Backups, disaster recovery |
| **S3 One Zone-IA** | Monthly | ms, per-GB fee | 30 days | Re-creatable data |
| **S3 Glacier Instant Retrieval** | Quarterly | ms, per-GB fee | 90 days | Archive with instant access |
| **S3 Glacier Flexible** | 1-2x/year | min-hours | 90 days | Long-term archive |
| **S3 Glacier Deep Archive** | <1x/year | 12-48 hours | 180 days | Compliance, 7-10yr retention |
""")

st.markdown("---")

# ============================================================
# Lifecycle Rules Visualization
# ============================================================
st.markdown("## 🔄 Lifecycle Rules (This Stack)")

import utils.common as common

lifecycle_mermaid = """
graph LR
    A[S3 Standard<br/>raw/] -->|60 days| B[Expire/Delete]
    C[S3 Standard<br/>curated/] -->|30 days| D[S3 Standard-IA]
    E[S3 Standard<br/>archive/] -->|1 day| F[Glacier Instant<br/>Retrieval]
    F -->|365 days| G[Expire/Delete]

    classDef hot fill:#FF9900,color:#fff,stroke:#E65100
    classDef warm fill:#42A5F5,color:#fff,stroke:#1565C0
    classDef cold fill:#7E57C2,color:#fff,stroke:#4527A0
    classDef expire fill:#EF5350,color:#fff,stroke:#C62828

    class A,C,E hot
    class D warm
    class F cold
    class B,G expire
"""

common.mermaid(lifecycle_mermaid, height=200, show_controls=False)

st.markdown("---")

# ============================================================
# Upload & Inspect Objects
# ============================================================
st.markdown("## 🚀 Upload Sample Data")

if data_lake_bucket:
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("📄 Upload to raw/ (Standard)", use_container_width=True):
            s3_client = boto3.client("s3")
            data = {"order_id": str(uuid.uuid4()), "product": "Coffee", "price": 4.99,
                    "timestamp": datetime.now(timezone.utc).isoformat()}
            key = f"raw/sample-{uuid.uuid4().hex[:8]}.json"
            s3_client.put_object(Bucket=data_lake_bucket, Key=key,
                                Body=json.dumps(data).encode(), StorageClass="STANDARD")
            st.success(f"✅ Uploaded to `{key}` (Standard)")

    with col2:
        if st.button("📦 Upload to archive/ (Standard→Glacier)", use_container_width=True):
            s3_client = boto3.client("s3")
            data = {"order_id": str(uuid.uuid4()), "archived": True,
                    "timestamp": datetime.now(timezone.utc).isoformat()}
            key = f"archive/old-{uuid.uuid4().hex[:8]}.json"
            s3_client.put_object(Bucket=data_lake_bucket, Key=key,
                                Body=json.dumps(data).encode(), StorageClass="STANDARD")
            st.success(f"✅ Uploaded to `{key}` (will transition to Glacier IR after 1 day)")

    with col3:
        if st.button("📊 List Bucket Contents", use_container_width=True):
            s3_client = boto3.client("s3")
            response = s3_client.list_objects_v2(Bucket=data_lake_bucket, MaxKeys=20)
            objects = response.get("Contents", [])
            if objects:
                for obj in objects[:15]:
                    st.text(f"{obj['StorageClass']:20s} {obj['Size']:>8d} B  {obj['Key']}")
            else:
                st.info("Bucket is empty. Upload some data first.")
else:
    st.warning("Stack not deployed. Run `cdk deploy` first.")

st.markdown("---")

# ============================================================
# Versioning Demo
# ============================================================
st.markdown("## 🔀 S3 Versioning Demo")
st.markdown("""
This bucket has **versioning enabled**. Overwriting or deleting an object
creates a new version / delete marker instead of permanently removing it.
""")

if versioned_bucket:
    col1, col2 = st.columns(2)

    with col1:
        if st.button("📝 Write v1 → Overwrite v2 → Delete", type="primary", use_container_width=True):
            s3_client = boto3.client("s3")
            key = "demo/versioned-file.json"

            # v1
            s3_client.put_object(Bucket=versioned_bucket, Key=key,
                                Body=json.dumps({"version": 1, "data": "original"}).encode())
            st.write("✅ Wrote version 1")

            # v2 (overwrite)
            s3_client.put_object(Bucket=versioned_bucket, Key=key,
                                Body=json.dumps({"version": 2, "data": "updated"}).encode())
            st.write("✅ Wrote version 2 (overwrite)")

            # Delete (creates delete marker)
            s3_client.delete_object(Bucket=versioned_bucket, Key=key)
            st.write("🗑️ Deleted (creates delete marker — object still exists as versions)")

    with col2:
        if st.button("📋 List All Versions", use_container_width=True):
            s3_client = boto3.client("s3")
            response = s3_client.list_object_versions(Bucket=versioned_bucket, Prefix="demo/")
            versions = response.get("Versions", [])
            markers = response.get("DeleteMarkers", [])

            if versions:
                st.markdown("**Object Versions:**")
                for v in versions:
                    st.text(f"  {v['VersionId'][:12]}...  {v['Size']:>6d} B  Latest={v['IsLatest']}")

            if markers:
                st.markdown("**Delete Markers:**")
                for m in markers:
                    st.text(f"  {m['VersionId'][:12]}...  (delete marker)  Latest={m['IsLatest']}")

            if not versions and not markers:
                st.info("No versions yet. Click 'Write v1 → Overwrite v2 → Delete' first.")

st.markdown("---")

# ============================================================
# Exam Tips
# ============================================================
st.markdown("## 💡 Exam Tips")
st.markdown("""
| Concept | Key Point |
|---------|-----------|
| **Glacier Instant Retrieval** | Millisecond access, 68% cheaper than Standard-IA for quarterly access |
| **S3 Intelligent-Tiering** | Use when access patterns are unknown — no retrieval fees |
| **Minimum duration** | Standard-IA: 30d, Glacier Flexible: 90d, Deep Archive: 180d |
| **Versioning + delete** | Creates delete marker, doesn't remove data. MFA Delete for protection |
| **Object lock** | WORM model — Governance mode (override with permissions) vs Compliance mode (no override) |
| **One Zone-IA** | 20% cheaper than Standard-IA but no cross-AZ resilience. Use for re-creatable data |
""")
