"""
Page 3: Data Lifecycle — S3 Lifecycle, DynamoDB TTL, COPY/UNLOAD

Covers Task 2.3: Manage the lifecycle of data
- S3 Lifecycle rule visualization
- DynamoDB TTL: write items, watch them expire
- COPY/UNLOAD SQL patterns (Redshift)
- Hot/warm/cold data tiering
"""
import streamlit as st
import boto3
import json
import uuid
import time
import pandas as pd
from datetime import datetime, timezone, timedelta

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Data Lifecycle", page_icon="🔄", layout="wide")

st.markdown("# 🔄 Data Lifecycle (Task 2.3)")
st.markdown("""
Manage data across its lifecycle: hot → warm → cold → archive → expire.
See DynamoDB TTL auto-deletion and Redshift COPY/UNLOAD patterns.
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
dynamo_table = config.get("DynamoTableName", "dea-s3-orders")

# ============================================================
# Data Tiering Strategy
# ============================================================
st.markdown("## 🌡️ Hot / Warm / Cold Data Tiers")

import utils.common as common

tier_mermaid = """
graph LR
    HOT[Hot Data<br/>S3 Standard<br/>DynamoDB] -->|30 days| WARM[Warm Data<br/>S3 Standard-IA<br/>Redshift RA3]
    WARM -->|90 days| COLD[Cold Data<br/>Glacier Instant<br/>Retrieval]
    COLD -->|365 days| ARCHIVE[Archive<br/>Glacier Deep<br/>Archive]
    ARCHIVE -->|Retention met| DELETE[Delete /<br/>Expire]

    classDef hot fill:#FF5722,color:#fff
    classDef warm fill:#FF9800,color:#fff
    classDef cold fill:#2196F3,color:#fff
    classDef archive fill:#673AB7,color:#fff
    classDef delete fill:#9E9E9E,color:#fff

    class HOT hot
    class WARM warm
    class COLD cold
    class ARCHIVE archive
    class DELETE delete
"""

common.mermaid(tier_mermaid, height=180, show_controls=False)

st.markdown("---")

# ============================================================
# DynamoDB TTL Demo
# ============================================================
st.markdown("## ⏱️ DynamoDB TTL Demo")
st.markdown("""
Write items with a `ttl` attribute set to a future Unix timestamp.
DynamoDB automatically deletes expired items **without consuming write capacity**.
""")

if dynamo_table:
    col1, col2 = st.columns([1, 1])

    with col1:
        ttl_minutes = st.slider("TTL (minutes from now)", 1, 10, 2)

        if st.button("📝 Write 5 Items with TTL", type="primary", use_container_width=True):
            ddb = boto3.client("dynamodb")
            ttl_epoch = int((datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)).timestamp())

            for i in range(5):
                order_id = f"order-{uuid.uuid4().hex[:8]}"
                ddb.put_item(
                    TableName=dynamo_table,
                    Item={
                        "order_id": {"S": order_id},
                        "timestamp": {"S": datetime.now(timezone.utc).isoformat()},
                        "product": {"S": f"Coffee #{i+1}"},
                        "price": {"N": str(round(3.99 + i * 0.5, 2))},
                        "ttl": {"N": str(ttl_epoch)},
                    },
                )
            expires_at = datetime.fromtimestamp(ttl_epoch, tz=timezone.utc).strftime("%H:%M:%S UTC")
            st.success(f"✅ Wrote 5 items. TTL expires at **{expires_at}** (~{ttl_minutes} min from now)")

    with col2:
        if st.button("📋 Scan Table (show current items)", use_container_width=True):
            ddb = boto3.client("dynamodb")
            response = ddb.scan(TableName=dynamo_table, Limit=20)
            items = response.get("Items", [])

            if items:
                now_epoch = int(datetime.now(timezone.utc).timestamp())
                table_data = []
                for item in items:
                    ttl_val = int(item.get("ttl", {}).get("N", "0"))
                    remaining = max(0, ttl_val - now_epoch)
                    table_data.append({
                        "order_id": item.get("order_id", {}).get("S", ""),
                        "product": item.get("product", {}).get("S", ""),
                        "price": item.get("price", {}).get("N", ""),
                        "ttl_remaining": f"{remaining}s" if remaining > 0 else "EXPIRED (pending deletion)",
                    })
                st.dataframe(pd.DataFrame(table_data), use_container_width=True)
                st.caption(f"Total items: {len(items)}")
            else:
                st.info("Table is empty. Write some items with TTL first.")

    st.markdown("""
    > **Note:** DynamoDB deletes expired items within **48 hours** of the TTL timestamp.
    > Items may remain visible for a short period after expiration. The deletion does NOT
    > consume write capacity — this is a key exam point.
    """)

st.markdown("---")

# ============================================================
# COPY / UNLOAD Patterns
# ============================================================
st.markdown("## 📥📤 Redshift COPY & UNLOAD")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 📥 COPY (S3 → Redshift)")
    st.code("""
-- Load Parquet from S3 into Redshift
COPY sales_table
FROM 's3://my-bucket/curated/'
IAM_ROLE 'arn:aws:iam::123:role/RedshiftRole'
FORMAT AS PARQUET;

-- Load compressed CSV
COPY orders
FROM 's3://my-bucket/raw/orders.csv.gz'
IAM_ROLE 'arn:aws:iam::123:role/RedshiftRole'
CSV GZIP
IGNOREHEADER 1;
    """, language="sql")
    st.markdown("""
    **Key points:**
    - Parallel load across cluster nodes
    - Supports Parquet, CSV, JSON, Avro
    - More efficient than INSERT
    - Use MANIFEST for specific file lists
    """)

with col2:
    st.markdown("### 📤 UNLOAD (Redshift → S3)")
    st.code("""
-- Export as Parquet (best for analytics)
UNLOAD ('SELECT * FROM sales WHERE year=2024')
TO 's3://my-bucket/export/sales_2024/'
IAM_ROLE 'arn:aws:iam::123:role/RedshiftRole'
FORMAT AS PARQUET
PARTITION BY (region);

-- Export as CSV
UNLOAD ('SELECT * FROM orders')
TO 's3://my-bucket/export/orders/'
IAM_ROLE 'arn:aws:iam::123:role/RedshiftRole'
CSV HEADER GZIP;
    """, language="sql")
    st.markdown("""
    **Key points:**
    - Export data OUT of Redshift to S3
    - Parquet + PARTITION BY for Spectrum
    - Use for cold data offloading
    - Supports compression (GZIP, ZSTD)
    """)

st.markdown("""
> **Exam trap:** `COPY` = **inbound** (S3→Redshift). `UNLOAD` = **outbound** (Redshift→S3).
> UNLOAD as Parquet with PARTITION BY is the pattern for extending your warehouse to a data lake.
""")

st.markdown("---")

# ============================================================
# Exam Tips
# ============================================================
st.markdown("## 💡 Exam Tips")
st.markdown("""
| Concept | Key Point |
|---------|-----------|
| **S3 Lifecycle** | Rule-based for known patterns; Intelligent-Tiering for unknown |
| **Minimum duration** | Transition fees apply; filter objects <128KB for Glacier |
| **DynamoDB TTL** | Per-item, epoch seconds, no write capacity consumed |
| **S3 Versioning** | Delete creates a marker; previous versions recoverable |
| **COPY command** | Parallel load, most efficient way to bulk-load Redshift |
| **UNLOAD** | Export as Parquet + PARTITION BY → query via Spectrum |
| **Glacier restore** | Temporarily copies to S3 Standard; must copy to keep permanently |
""")
