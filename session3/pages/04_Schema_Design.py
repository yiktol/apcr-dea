"""
Page 4: Schema Design — Redshift patterns, Columnar vs Row, Sort/Dist

Covers Task 2.4: Design data models and schema evolution
- Sort keys and distribution styles (Redshift)
- Columnar vs row storage comparison
- Compression encoding
- Schema conversion patterns (DMS/SCT)
"""
import streamlit as st
import boto3
import json
import pandas as pd

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="Schema Design", page_icon="🏗️", layout="wide")

st.markdown("# 🏗️ Schema Design (Task 2.4)")
st.markdown("""
Understand Redshift schema design best practices: sort keys, distribution styles,
compression, and how schema conversion tools help with migration.
""")

import utils.common as common

# ============================================================
# Redshift Distribution Styles
# ============================================================
st.markdown("## 📐 Redshift Distribution Styles")

st.markdown("""
Distribution style determines **how rows are distributed across compute nodes** for parallel processing.
""")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("""
    ### AUTO
    Redshift chooses the best style based on table size and query patterns.
    
    **Use when:** Unsure which is best
    """)

with col2:
    st.markdown("""
    ### KEY
    Rows with same key value → same node. Collocates join partners.
    
    **Use when:** Frequent joins on a specific column
    """)

with col3:
    st.markdown("""
    ### ALL
    Full copy of table on every node. Eliminates shuffling for small dimension tables.
    
    **Use when:** Small lookup tables (<2M rows)
    """)

with col4:
    st.markdown("""
    ### EVEN
    Round-robin distribution. Equal spread regardless of values.
    
    **Use when:** No clear join key, want uniform parallelism
    """)

st.markdown("---")

# ============================================================
# Sort Keys
# ============================================================
st.markdown("## 🔑 Redshift Sort Keys")

st.markdown("""
Sort keys determine **physical order of data on disk**. The query optimizer uses zone maps
(min/max per block) to skip blocks that don't match the predicate.
""")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    ### Compound Sort Key
    ```sql
    CREATE TABLE sales (
        sale_id INT,
        sale_date DATE,
        region VARCHAR(50),
        amount DECIMAL(10,2)
    )
    COMPOUND SORTKEY (sale_date, region);
    ```
    - Sorts by first key, then second within ties
    - Best for queries that filter on the **prefix** columns
    - Order matters: `WHERE sale_date = ...` benefits; `WHERE region = ...` alone does not
    """)

with col2:
    st.markdown("""
    ### Interleaved Sort Key
    ```sql
    CREATE TABLE events (
        event_id INT,
        event_date DATE,
        user_id INT,
        event_type VARCHAR(50)
    )
    INTERLEAVED SORTKEY (event_date, user_id);
    ```
    - Equal weight to each key column
    - Best when queries filter on **any combination** of sort columns
    - Higher maintenance cost (VACUUM REINDEX)
    """)

st.markdown("---")

# ============================================================
# Columnar vs Row Storage
# ============================================================
st.markdown("## 📊 Row-Based vs Columnar Storage")

row_col_mermaid = """
graph TD
    subgraph Row-Based [Row Storage - CSV, Avro]
        R1[Row 1: id=1, name=Alice, age=30, city=NYC]
        R2[Row 2: id=2, name=Bob, age=25, city=LA]
        R3[Row 3: id=3, name=Carol, age=35, city=CHI]
    end

    subgraph Columnar [Column Storage - Parquet, ORC]
        C1[id: 1, 2, 3]
        C2[name: Alice, Bob, Carol]
        C3[age: 30, 25, 35]
        C4[city: NYC, LA, CHI]
    end

    classDef row fill:#FFECB3,stroke:#FF9800
    classDef col fill:#E3F2FD,stroke:#2196F3

    class R1,R2,R3 row
    class C1,C2,C3,C4 col
"""

common.mermaid(row_col_mermaid, height=350, show_controls=False)

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    ### Row-Based (JSON, CSV, Avro)
    - Read entire row at once
    - Good for: OLTP, INSERT/UPDATE heavy
    - Bad for analytics: scans all columns
    - No column pruning
    """)

with col2:
    st.markdown("""
    ### Columnar (Parquet, ORC)
    - Read only needed columns
    - Excellent compression (similar values together)
    - Predicate pushdown (skip blocks via min/max stats)
    - Good for: OLAP, analytics, Athena
    """)

st.markdown("---")

# ============================================================
# Parquet vs ORC
# ============================================================
st.markdown("## ⚔️ Parquet vs ORC")

st.markdown("""
| Feature | Parquet | ORC |
|---------|---------|-----|
| **Origin** | Apache (Twitter/Cloudera) | Apache (Hortonworks/Hive) |
| **Best with** | Spark, Athena, Redshift Spectrum | Hive, Presto |
| **Nested data** | Excellent (Dremel encoding) | Good |
| **Compression** | Snappy, GZIP, LZO, ZSTD | ZLIB, Snappy, LZO |
| **File size** | Slightly larger | Slightly smaller |
| **Predicate pushdown** | Yes (row group stats) | Yes (stripe stats) |
| **Exam default** | Most common answer for analytics on AWS | Less common on exam |
""")

st.markdown("---")

# ============================================================
# Schema Conversion
# ============================================================
st.markdown("## 🔄 Schema Conversion (DMS / SCT)")

conversion_mermaid = """
graph LR
    SRC[Source Database<br/>Oracle / SQL Server] --> SCT[AWS SCT<br/>Schema Conversion]
    SCT --> TGT[Target Database<br/>Aurora / Redshift]
    SRC --> DMS[AWS DMS<br/>Data Migration]
    DMS --> TGT

    classDef tool fill:#E8F5E9,stroke:#4CAF50
    classDef db fill:#E3F2FD,stroke:#2196F3

    class SCT,DMS tool
    class SRC,TGT db
"""

common.mermaid(conversion_mermaid, height=200, show_controls=False)

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    ### AWS SCT (Schema Conversion Tool)
    - Converts DDL: tables, views, stored procedures
    - **Scans application SQL** for embedded statements
    - Generates assessment report (% auto-converted)
    - Supports: Oracle→Aurora, SQL Server→PostgreSQL, Cassandra→DynamoDB
    - Downloadable desktop tool
    """)

with col2:
    st.markdown("""
    ### AWS DMS (Database Migration Service)
    - Migrates the **data** (not schema)
    - Full load + Change Data Capture (CDC)
    - Minimal downtime migration
    - DMS Schema Conversion (cloud-based, simpler)
    - Supports heterogeneous migrations
    """)

st.markdown("---")

# ============================================================
# Exam Tips
# ============================================================
st.markdown("## 💡 Exam Tips")
st.markdown("""
| Concept | Key Point |
|---------|-----------|
| **DIST KEY** | Choose the column you JOIN on most. Collocates matching rows. |
| **DIST ALL** | Small dimension tables (<2M rows). Full copy on every node. |
| **SORT KEY** | Choose columns in WHERE/ORDER BY. Compound for prefix; Interleaved for any combo. |
| **ENCODE AUTO** | Let Redshift pick compression. COPY auto-applies on first load. |
| **Column sizing** | Use smallest type that fits. VARCHAR(50) not VARCHAR(MAX). |
| **DATE type** | Use DATE/TIMESTAMP not CHAR for date values. |
| **SCT then DMS** | SCT converts schema first, DMS migrates data second. |
| **Parquet default** | When exam says "optimize for analytics" → Parquet + partitioning. |
""")
