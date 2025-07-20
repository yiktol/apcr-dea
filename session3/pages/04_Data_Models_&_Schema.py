
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import utils.common as common
import utils.authenticate as authenticate
import json

# Page configuration
st.set_page_config(
    page_title="AWS Data Modeling & Schema Management",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# AWS Color Scheme
AWS_COLORS = {
    'primary': '#FF9900',
    'secondary': '#232F3E',
    'light_blue': '#4B9EDB',
    'dark_blue': '#1B2631',
    'light_gray': '#F2F3F3',
    'success': '#3FB34F'
}

common.initialize_mermaid()

def apply_custom_styles():
    """Apply custom CSS styling with AWS color scheme"""
    st.markdown(f"""
    <style>
        .main {{
            background-color: {AWS_COLORS['light_gray']};
        }}
        
        .stTabs [data-baseweb="tab-list"] {{
            gap: 24px;
            background-color: white;
            border-radius: 10px;
            padding: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .stTabs [data-baseweb="tab"] {{
            height: 60px;
            padding: 0px 24px;
            background-color: {AWS_COLORS['light_gray']};
            border-radius: 8px;
            color: {AWS_COLORS['secondary']};
            font-weight: 600;
            border: 2px solid transparent;
        }}
        
        .stTabs [aria-selected="true"] {{
            background-color: {AWS_COLORS['primary']};
            color: white;
            border: 2px solid {AWS_COLORS['secondary']};
        }}
        
        .metric-card {{
            background: linear-gradient(135deg, {AWS_COLORS['primary']} 0%, {AWS_COLORS['light_blue']} 100%);
            padding: 20px;
            border-radius: 15px;
            color: white;
            text-align: center;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            margin: 10px 0;
        }}
        
        .concept-card {{
            background: white;
            padding: 20px;
            border-radius: 15px;
            border-left: 5px solid {AWS_COLORS['primary']};
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            margin: 15px 0;
        }}
        
        .highlight-box {{
            background: linear-gradient(135deg, {AWS_COLORS['light_blue']} 0%, {AWS_COLORS['primary']} 100%);
            padding: 20px;
            border-radius: 12px;
            color: white;
            margin: 15px 0;
        }}
        
        .footer {{
            text-align: center;
            padding: 1rem;
            background-color: {AWS_COLORS['secondary']};
            color: white;
            margin-top: 1rem;
            border-radius: 8px;
        }}

        .code-container {{
            background-color: {AWS_COLORS['dark_blue']};
            color: white;
            padding: 20px;
            border-radius: 10px;
            border-left: 4px solid {AWS_COLORS['primary']};
            margin: 15px 0;
        }}
        
        .design-pattern {{
            background: white;
            padding: 15px;
            border-radius: 10px;
            border: 2px solid {AWS_COLORS['light_blue']};
            margin: 10px 0;
            text-align: center;
        }}
        
        .info-box {{
            background-color: #E6F2FF;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 15px;
            border-left: 5px solid #00A1C9;
        }}
        
        .warning-box {{
            background-color: #FFF3CD;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 15px;
            border-left: 5px solid {AWS_COLORS['primary']};
        }}
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    common.initialize_session_state()
    if 'session_started' not in st.session_state:
        st.session_state.session_started = True
        st.session_state.schemas_designed = []
        st.session_state.migration_progress = 0

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🏗️ Amazon Redshift Schema Design - Distribution keys, sort keys, compression
            - 🔄 AWS Database Migration Service - Managed migration with minimal downtime
            - 📊 AWS DMS Schema Conversion - Automated schema transformation
            - 🛠️ AWS Schema Conversion Tool (SCT) - Desktop tool for database conversion
            
            **Learning Objectives:**
            - Master Redshift schema design best practices
            - Understand database migration strategies
            - Learn automated schema conversion techniques
            - Explore migration tools and methodologies
            """)

def create_redshift_architecture_mermaid():
    """Create mermaid diagram for Redshift architecture"""
    return """
    graph TB
        subgraph "Redshift Cluster"
            LEADER[Leader Node<br/>📊 Query Planning<br/>🔄 Distribution]
            
            subgraph "Compute Nodes"
                CN1[Node 1<br/>Slice 1<br/>Slice 2]
                CN2[Node 2<br/>Slice 3<br/>Slice 4]
                CN3[Node 3<br/>Slice 5<br/>Slice 6]
            end
        end
        
        APP[SQL Applications] --> LEADER
        BI[BI Tools] --> LEADER
        
        LEADER --> CN1
        LEADER --> CN2
        LEADER --> CN3
        
        CN1 --> S3_1[S3 Storage<br/>Slice 1 & 2 Data]
        CN2 --> S3_2[S3 Storage<br/>Slice 3 & 4 Data]
        CN3 --> S3_3[S3 Storage<br/>Slice 5 & 6 Data]
        
        style LEADER fill:#FF9900,stroke:#232F3E,color:#fff
        style CN1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CN2 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CN3 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style S3_1 fill:#3FB34F,stroke:#232F3E,color:#fff
        style S3_2 fill:#3FB34F,stroke:#232F3E,color:#fff
        style S3_3 fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_dms_migration_flow():
    """Create mermaid diagram for DMS migration flow"""
    return """
    graph LR
        subgraph "Source Environment"
            SRC_DB[(Source Database<br/>Oracle/SQL Server<br/>MySQL/PostgreSQL)]
        end
        
        subgraph "AWS DMS"
            DMS[DMS Replication<br/>Instance]
            SCT[Schema Conversion]
        end
        
        subgraph "Target Environment"
            TGT_DB[(Target Database<br/>Amazon Redshift<br/>Amazon RDS<br/>Amazon Aurora)]
        end
        
        subgraph "Monitoring"
            CW[CloudWatch<br/>Monitoring]
            LOGS[Migration Logs]
        end
        
        SRC_DB --> SCT
        SCT --> DMS
        DMS --> TGT_DB
        
        DMS --> CW
        DMS --> LOGS
        
        style SRC_DB fill:#FF9900,stroke:#232F3E,color:#fff
        style DMS fill:#4B9EDB,stroke:#232F3E,color:#fff
        style SCT fill:#3FB34F,stroke:#232F3E,color:#fff
        style TGT_DB fill:#FF9900,stroke:#232F3E,color:#fff
        style CW fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_schema_conversion_flow():
    """Create mermaid diagram for schema conversion process"""
    return """
    graph TB
        START[Start Migration] --> ASSESS[Assessment Report]
        ASSESS --> CONVERT[Convert Schema]
        CONVERT --> REVIEW[Review & Edit]
        REVIEW --> APPLY[Apply to Target]
        APPLY --> VALIDATE[Validate Schema]
        VALIDATE --> MIGRATE[Migrate Data]
        MIGRATE --> TEST[Test & Verify]
        TEST --> CUTOVER[Production Cutover]
        
        ASSESS --> MANUAL{Manual<br/>Conversion<br/>Required?}
        MANUAL -->|Yes| CUSTOM[Custom Code<br/>Development]
        MANUAL -->|No| CONVERT
        CUSTOM --> REVIEW
        
        style START fill:#FF9900,stroke:#232F3E,color:#fff
        style ASSESS fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CONVERT fill:#3FB34F,stroke:#232F3E,color:#fff
        style CUTOVER fill:#232F3E,stroke:#FF9900,color:#fff
    """

def redshift_schema_design_tab():
    """Content for Redshift Schema Design tab"""
    st.markdown("## 🏗️ Amazon Redshift Schema Design")
    st.markdown("*Optimize performance through proper distribution keys, sort keys, and compression*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Design Principles
    Amazon Redshift schema design focuses on three core elements:
    - **Distribution Keys**: Control how data is distributed across nodes
    - **Sort Keys**: Optimize query performance through data ordering
    - **Compression**: Reduce storage space and improve I/O performance
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ Redshift Cluster Architecture")
    common.mermaid(create_redshift_architecture_mermaid(), height=650)
    
    # Interactive schema designer
    st.markdown("#### 🎨 Interactive Schema Designer")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Table Configuration")
        table_name = st.text_input("Table Name", value="sales_fact")
        table_type = st.selectbox("Table Type", ["Fact Table", "Dimension Table", "Staging Table"])
        estimated_rows = st.number_input("Estimated Rows (millions)", min_value=1, max_value=10000, value=100)
    
    with col2:
        st.markdown("##### Distribution Strategy")
        dist_style = st.selectbox("Distribution Style", ["AUTO", "KEY", "ALL", "EVEN"])
        if dist_style == "KEY":
            dist_key = st.selectbox("Distribution Key", ["customer_id", "product_id", "order_id", "date_key"])
        else:
            dist_key = None
    
    # Sort key configuration
    st.markdown("##### Sort Key Configuration")
    sort_type = st.selectbox("Sort Key Type", ["AUTO", "COMPOUND", "INTERLEAVED"])
    if sort_type != "AUTO":
        sort_columns = st.multiselect("Sort Key Columns", 
                                     ["order_date", "customer_id", "product_id", "sales_amount"],
                                     default=["order_date"])
    else:
        sort_columns = []
    
    # Generate schema recommendation
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Schema Recommendation for {table_name}
    **Table Type**: {table_type}  
    **Distribution Style**: {dist_style} {f"({dist_key})" if dist_key else ""}  
    **Sort Strategy**: {sort_type} {f"({', '.join(sort_columns)})" if sort_columns else ""}  
    **Expected Performance**: {'Optimized for analytics queries' if table_type == 'Fact Table' else 'Optimized for lookups'}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Distribution styles comparison
    st.markdown("#### 📊 Distribution Styles Comparison")
    
    dist_col1, dist_col2, dist_col3, dist_col4 = st.columns(4)
    
    with dist_col1:
        st.markdown('<div class="design-pattern">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 KEY Distribution
        **Best For:**
        - Large fact tables
        - Frequent joins
        
        **Example:**
        ```sql
        DISTKEY(customer_id)
        ```
        
        **Benefits:**
        - Collocated joins
        - Reduced network traffic
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with dist_col2:
        st.markdown('<div class="design-pattern">', unsafe_allow_html=True)
        st.markdown("""
        ### 📋 ALL Distribution
        **Best For:**
        - Small lookup tables
        - Reference data
        
        **Example:**
        ```sql
        DISTSTYLE ALL
        ```
        
        **Benefits:**
        - No network joins
        - Fast lookups
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with dist_col3:
        st.markdown('<div class="design-pattern">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚖️ EVEN Distribution
        **Best For:**
        - No clear join patterns
        - Balanced workloads
        
        **Example:**
        ```sql
        DISTSTYLE EVEN
        ```
        
        **Benefits:**
        - Balanced storage
        - Parallel processing
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with dist_col4:
        st.markdown('<div class="design-pattern">', unsafe_allow_html=True)
        st.markdown("""
        ### 🤖 AUTO Distribution
        **Best For:**
        - Let Redshift decide
        - Unknown patterns
        
        **Example:**
        ```sql
        DISTSTYLE AUTO
        ```
        
        **Benefits:**
        - Adaptive optimization
        - Maintenance-free
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Schema Design Examples")
    
    tab1, tab2, tab3 = st.tabs(["Fact Table Design", "Dimension Table Design", "Performance Optimization"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Optimized Fact Table Design
CREATE TABLE sales_fact (
    sale_id BIGINT IDENTITY(1,1),
    customer_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    store_key INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    tax_amount DECIMAL(10,2) NOT NULL,
    created_timestamp TIMESTAMP DEFAULT GETDATE()
)
-- Distribution on frequently joined dimension
DISTKEY(customer_key)
-- Sort by date for time-series queries
SORTKEY(date_key, customer_key)
-- Enable automatic compression
ENCODE AUTO;

-- Add constraints for query optimization
ALTER TABLE sales_fact 
ADD CONSTRAINT fk_customer 
FOREIGN KEY (customer_key) 
REFERENCES customer_dim(customer_key);

ALTER TABLE sales_fact 
ADD CONSTRAINT fk_product 
FOREIGN KEY (product_key) 
REFERENCES product_dim(product_key);

ALTER TABLE sales_fact 
ADD CONSTRAINT fk_date 
FOREIGN KEY (date_key) 
REFERENCES date_dim(date_key);

-- Analyze table for statistics
ANALYZE sales_fact;

-- Create materialized view for common aggregations
CREATE MATERIALIZED VIEW sales_monthly_summary AS
SELECT 
    date_key,
    customer_key,
    SUM(total_amount) as total_sales,
    SUM(quantity) as total_quantity,
    COUNT(*) as transaction_count
FROM sales_fact
WHERE date_key >= 20240101
GROUP BY date_key, customer_key;
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Customer Dimension Table
CREATE TABLE customer_dim (
    customer_key INTEGER IDENTITY(1,1),
    customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    customer_segment VARCHAR(50),
    registration_date DATE,
    last_updated TIMESTAMP DEFAULT GETDATE(),
    is_active BOOLEAN DEFAULT TRUE
)
-- Small dimension - distribute to all nodes
DISTSTYLE ALL
-- Sort by most commonly filtered column
SORTKEY(customer_id)
-- Manual encoding for specific columns
ENCODE AUTO;

-- Product Dimension Table with SCD Type 2
CREATE TABLE product_dim (
    product_key INTEGER IDENTITY(1,1),
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_cost DECIMAL(10,2),
    unit_price DECIMAL(10,2),
    supplier_id VARCHAR(50),
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    version INTEGER DEFAULT 1
)
DISTSTYLE ALL
SORTKEY(product_id, effective_date)
ENCODE AUTO;

-- Date Dimension Table
CREATE TABLE date_dim (
    date_key INTEGER NOT NULL,
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR(20),
    quarter_number INTEGER,
    year_number INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
)
DISTSTYLE ALL
SORTKEY(date_key)
ENCODE AUTO;

-- Primary keys for referential integrity (informational)
ALTER TABLE customer_dim ADD PRIMARY KEY (customer_key);
ALTER TABLE product_dim ADD PRIMARY KEY (product_key);
ALTER TABLE date_dim ADD PRIMARY KEY (date_key);
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Performance Optimization Techniques

-- 1. Analyze table statistics regularly
ANALYZE TABLE sales_fact;
ANALYZE TABLE customer_dim;
ANALYZE TABLE product_dim;

-- 2. Monitor table design effectiveness
SELECT 
    schema_name,
    table_name,
    diststyle,
    sortkey1,
    max_varchar,
    sortkey_num,
    size,
    pct_used,
    unsorted
FROM svv_table_info
WHERE schema_name = 'public'
ORDER BY size DESC;

-- 3. Check for data skew
SELECT 
    slice,
    COUNT(*) as row_count
FROM sales_fact
GROUP BY slice
ORDER BY slice;

-- 4. Optimize column encoding
SELECT 
    schemaname,
    tablename,
    columnname,
    encoding,
    distkey,
    sortkey
FROM pg_table_def
WHERE schemaname = 'public'
AND tablename = 'sales_fact';

-- 5. Query performance analysis
SELECT 
    query,
    substring(querytxt, 1, 100) as query_text,
    starttime,
    endtime,
    datediff(seconds, starttime, endtime) as duration_seconds,
    aborted
FROM stl_query
WHERE userid != 1
AND starttime >= dateadd(hour, -24, getdate())
ORDER BY duration_seconds DESC
LIMIT 10;

-- 6. Create indexes for frequently used queries
-- Note: Redshift doesn't support traditional indexes
-- Instead, optimize using sort keys and distribution keys

-- 7. Vacuum and analyze maintenance
VACUUM FULL sales_fact;
ANALYZE sales_fact;

-- 8. Column compression analysis
SELECT 
    schema_name,
    table_name,
    column_name,
    type,
    encoding,
    distkey,
    sortkey,
    blocks
FROM svv_diskusage
WHERE schema_name = 'public'
AND table_name = 'sales_fact'
ORDER BY blocks DESC;

-- 9. Workload Management (WLM) optimization
-- Create queue for ETL processes
-- Create queue for analytical queries
-- Monitor queue wait times

-- 10. Result set caching
-- Enable result caching for repeated queries
SET enable_result_cache_for_session TO true;

SELECT 
    customer_segment,
    SUM(total_amount) as segment_sales
FROM sales_fact sf
JOIN customer_dim cd ON sf.customer_key = cd.customer_key
WHERE sf.date_key >= 20240101
GROUP BY customer_segment
ORDER BY segment_sales DESC;
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)

def dms_tab():
    """Content for AWS Database Migration Service tab"""
    st.markdown("## 🔄 AWS Database Migration Service")
    st.markdown("*Trusted by customers to securely migrate 1M+ databases with minimal downtime*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Capabilities
    AWS DMS is a managed migration and replication service that helps move database and analytics workloads to AWS:
    - **Minimal Downtime**: Source database remains operational during migration
    - **Zero Data Loss**: Continuous data replication ensures consistency
    - **Multiple Sources**: Support for 15+ database engines
    - **Change Data Capture**: Real-time replication of ongoing changes
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # DMS Migration Flow
    st.markdown("#### 🔄 DMS Migration Architecture")
    common.mermaid(create_dms_migration_flow(), height=550)
    
    # Migration types
    st.markdown("#### 📊 Migration Types")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="design-pattern">', unsafe_allow_html=True)
        st.markdown("""
        ### 📥 Full Load
        **Description:**
        One-time migration of all existing data
        
        **Use Cases:**
        - Initial data migration
        - Historical data transfer
        - Development/testing
        
        **Characteristics:**
        - Complete data copy
        - Downtime acceptable
        - Point-in-time snapshot
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="design-pattern">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 CDC Only
        **Description:**
        Replicate ongoing changes only
        
        **Use Cases:**
        - Real-time replication
        - Data lake streaming
        - Analytics pipelines
        
        **Characteristics:**
        - Continuous sync
        - No initial load
        - Low latency
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="design-pattern">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Full Load + CDC
        **Description:**
        Initial load followed by continuous replication
        
        **Use Cases:**
        - Production migrations
        - Hybrid architectures
        - Disaster recovery
        
        **Characteristics:**
        - Zero downtime
        - Complete solution
        - Automatic failover
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive migration planner
    st.markdown("#### 🎯 Migration Planning Assistant")
    
    col1, col2 = st.columns(2)
    
    with col1:
        source_engine = st.selectbox("Source Database", [
            "Oracle", "SQL Server", "MySQL", "PostgreSQL", 
            "MongoDB", "Amazon Aurora", "MariaDB", "IBM Db2"
        ])
        source_size = st.number_input("Database Size (GB)", min_value=1, max_value=100000, value=500)
        
    with col2:
        target_engine = st.selectbox("Target Database", [
            "Amazon Redshift", "Amazon RDS PostgreSQL", "Amazon RDS MySQL",
            "Amazon Aurora", "Amazon DynamoDB", "Amazon S3"
        ])
        downtime_tolerance = st.selectbox("Downtime Tolerance", [
            "Zero downtime required", "< 1 hour", "< 4 hours", "Flexible"
        ])
    
    # Calculate migration recommendations
    migration_time = calculate_migration_time(source_size, source_engine, target_engine)
    recommended_strategy = get_migration_strategy(downtime_tolerance, source_size)
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📋 Migration Plan Recommendation
    **Source**: {source_engine} ({source_size} GB)  
    **Target**: {target_engine}  
    **Recommended Strategy**: {recommended_strategy}  
    **Estimated Migration Time**: {migration_time}  
    **Replication Instance**: {get_instance_recommendation(source_size)}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Supported endpoints
    st.markdown("#### 🔗 Supported Database Engines")
    
    endpoints_data = {
        'Source Engines': [
            'Oracle', 'SQL Server', 'MySQL', 'PostgreSQL', 'MongoDB',
            'Amazon Aurora', 'MariaDB', 'SAP ASE', 'IBM Db2', 'Amazon S3'
        ],
        'Target Engines': [
            'Amazon Redshift', 'Amazon RDS', 'Amazon Aurora', 'Amazon DynamoDB',
            'Amazon S3', 'Amazon ElastiCache', 'Amazon Kinesis Data Streams',
            'Amazon DocumentDB', 'Amazon Timestream', 'Apache Kafka'
        ],
        'Migration Complexity': [
            'Medium', 'Medium', 'Low', 'Low', 'High',
            'Low', 'Low', 'High', 'High', 'Low'
        ]
    }
    
    # Create comparison table
    max_len = max(len(endpoints_data['Source Engines']), len(endpoints_data['Target Engines']))
    
    # Pad shorter lists
    for key in endpoints_data:
        while len(endpoints_data[key]) < max_len:
            endpoints_data[key].append('')
    
    df_endpoints = pd.DataFrame(endpoints_data)
    st.dataframe(df_endpoints, use_container_width=True)
    
    # Code examples
    st.markdown("#### 💻 DMS Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Setup Migration", "Monitor Progress", "Troubleshooting"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create DMS replication instance using AWS CLI
aws dms create-replication-instance \
    --replication-instance-identifier my-replication-instance \
    --replication-instance-class dms.r5.xlarge \
    --allocated-storage 100 \
    --vpc-security-group-ids sg-12345678 \
    --replication-subnet-group-identifier my-subnet-group \
    --publicly-accessible false \
    --multi-az true

# Python Boto3 - Complete DMS setup
import boto3
import json

dms_client = boto3.client('dms')

# 1. Create replication instance
def create_replication_instance():
    response = dms_client.create_replication_instance(
        ReplicationInstanceIdentifier='prod-migration-instance',
        ReplicationInstanceClass='dms.r5.2xlarge',
        AllocatedStorage=200,
        VpcSecurityGroupIds=['sg-12345678'],
        ReplicationSubnetGroupIdentifier='dms-subnet-group',
        PubliclyAccessible=False,
        MultiAZ=True,
        Tags=[
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Project', 'Value': 'DataMigration'}
        ]
    )
    return response['ReplicationInstance']['ReplicationInstanceArn']

# 2. Create source endpoint (Oracle)
def create_source_endpoint():
    response = dms_client.create_endpoint(
        EndpointIdentifier='oracle-source',
        EndpointType='source',
        EngineName='oracle',
        Username='source_user',
        Password='source_password',
        ServerName='oracle-server.company.com',
        Port=1521,
        DatabaseName='PROD',
        SslMode='require',
        Tags=[
            {'Key': 'Type', 'Value': 'Source'},
            {'Key': 'Database', 'Value': 'Oracle'}
        ]
    )
    return response['Endpoint']['EndpointArn']

# 3. Create target endpoint (Redshift)
def create_target_endpoint():
    response = dms_client.create_endpoint(
        EndpointIdentifier='redshift-target',
        EndpointType='target',
        EngineName='redshift',
        Username='target_user',
        Password='target_password',
        ServerName='redshift-cluster.abc123.us-west-2.redshift.amazonaws.com',
        Port=5439,
        DatabaseName='analytics',
        ExtraConnectionAttributes='acceptAnyDate=true;dateFormat=yyyy-mm-dd',
        Tags=[
            {'Key': 'Type', 'Value': 'Target'},
            {'Key': 'Database', 'Value': 'Redshift'}
        ]
    )
    return response['Endpoint']['EndpointArn']

# 4. Create migration task
def create_migration_task(replication_instance_arn, source_arn, target_arn):
    table_mappings = {
        "rules": [
            {
                "rule-type": "selection",
                "rule-id": "1",
                "rule-name": "1",
                "object-locator": {
                    "schema-name": "SALES",
                    "table-name": "%"
                },
                "rule-action": "include"
            }
        ]
    }
    
    migration_settings = {
        "TargetMetadata": {
            "SupportLobs": true,
            "FullLobMode": true,
            "LobChunkSize": 64,
            "LimitedSizeLobMode": false
        },
        "FullLoadSettings": {
            "TargetTablePrepMode": "DROP_AND_CREATE",
            "CreatePkAfterFullLoad": true,
            "StopTaskCachedChangesApplied": true,
            "StopTaskCachedChangesNotApplied": false
        }
    }
    
    response = dms_client.create_replication_task(
        ReplicationTaskIdentifier='oracle-to-redshift-migration',
        SourceEndpointArn=source_arn,
        TargetEndpointArn=target_arn,
        ReplicationInstanceArn=replication_instance_arn,
        MigrationType='full-load-and-cdc',
        TableMappings=json.dumps(table_mappings),
        ReplicationTaskSettings=json.dumps(migration_settings),
        Tags=[
            {'Key': 'Migration', 'Value': 'OracleToRedshift'},
            {'Key': 'Priority', 'Value': 'High'}
        ]
    )
    return response['ReplicationTask']['ReplicationTaskArn']

# Execute migration setup
try:
    print("Creating replication instance...")
    instance_arn = create_replication_instance()
    
    print("Creating endpoints...")
    source_arn = create_source_endpoint()
    target_arn = create_target_endpoint()
    
    print("Creating migration task...")
    task_arn = create_migration_task(instance_arn, source_arn, target_arn)
    
    print(f"Migration setup complete. Task ARN: {task_arn}")
    
except Exception as e:
    print(f"Migration setup failed: {e}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Monitor DMS migration progress
import boto3
import time
from datetime import datetime

dms_client = boto3.client('dms')
cloudwatch = boto3.client('cloudwatch')

def monitor_migration_task(task_arn):
    """Monitor DMS task progress and statistics"""
    
    while True:
        # Get task status
        response = dms_client.describe_replication_tasks(
            Filters=[
                {'Name': 'replication-task-arn', 'Values': [task_arn]}
            ]
        )
        
        task = response['ReplicationTasks'][0]
        status = task['Status']
        
        print(f"Task Status: {status}")
        print(f"Last Update: {task.get('ReplicationTaskCreationDate')}")
        
        if status in ['stopped', 'failed', 'ready']:
            break
            
        # Get statistics
        stats = dms_client.describe_table_statistics(
            ReplicationTaskArn=task_arn
        )
        
        print("\nTable Statistics:")
        for table_stat in stats['TableStatistics']:
            print(f"Table: {table_stat['SchemaName']}.{table_stat['TableName']}")
            print(f"  Full Load Rows: {table_stat.get('FullLoadRows', 0)}")
            print(f"  Inserts: {table_stat.get('Inserts', 0)}")
            print(f"  Updates: {table_stat.get('Updates', 0)}")
            print(f"  Deletes: {table_stat.get('Deletes', 0)}")
            print(f"  DDLs: {table_stat.get('Ddls', 0)}")
        
        # Check for errors
        if status == 'failed':
            print("\nTask failed. Checking events...")
            events = dms_client.describe_events(
                SourceIdentifier=task_arn.split('/')[-1],
                SourceType='replication-task'
            )
            
            for event in events['Events']:
                print(f"Event: {event['Message']}")
        
        time.sleep(30)  # Check every 30 seconds

def get_cloudwatch_metrics(task_arn):
    """Retrieve CloudWatch metrics for DMS task"""
    
    task_id = task_arn.split('/')[-1]
    
    # Get CPU utilization
    cpu_response = cloudwatch.get_metric_statistics(
        Namespace='AWS/DMS',
        MetricName='CPUUtilization',
        Dimensions=[
            {'Name': 'ReplicationTaskArn', 'Value': task_arn}
        ],
        StartTime=datetime.utcnow() - timedelta(hours=1),
        EndTime=datetime.utcnow(),
        Period=300,
        Statistics=['Average', 'Maximum']
    )
    
    # Get memory utilization
    memory_response = cloudwatch.get_metric_statistics(
        Namespace='AWS/DMS',
        MetricName='FreeableMemory',
        Dimensions=[
            {'Name': 'ReplicationTaskArn', 'Value': task_arn}
        ],
        StartTime=datetime.utcnow() - timedelta(hours=1),
        EndTime=datetime.utcnow(),
        Period=300,
        Statistics=['Average', 'Minimum']
    )
    
    return {
        'cpu_metrics': cpu_response['Datapoints'],
        'memory_metrics': memory_response['Datapoints']
    }

# Create CloudWatch dashboard for monitoring
def create_monitoring_dashboard(task_arn):
    """Create CloudWatch dashboard for DMS monitoring"""
    
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "properties": {
                    "metrics": [
                        ["AWS/DMS", "FreeableMemory", "ReplicationTaskArn", task_arn],
                        ["AWS/DMS", "CPUUtilization", "ReplicationTaskArn", task_arn]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-west-2",
                    "title": "DMS Instance Performance"
                }
            },
            {
                "type": "metric", 
                "properties": {
                    "metrics": [
                        ["AWS/DMS", "FullLoadThroughputBandwidthSource", "ReplicationTaskArn", task_arn],
                        ["AWS/DMS", "FullLoadThroughputBandwidthTarget", "ReplicationTaskArn", task_arn]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-west-2",
                    "title": "Migration Throughput"
                }
            }
        ]
    }
    
    cloudwatch.put_dashboard(
        DashboardName='DMS-Migration-Monitoring',
        DashboardBody=json.dumps(dashboard_body)
    )

# Usage example
task_arn = 'arn:aws:dms:us-west-2:123456789012:task:ABCDEFGHIJKLMNOPQRSTUVWXYZ'
monitor_migration_task(task_arn)
create_monitoring_dashboard(task_arn)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# DMS Troubleshooting and Error Resolution
import boto3
import json

dms_client = boto3.client('dms')

def diagnose_migration_issues(task_arn):
    """Comprehensive troubleshooting for DMS tasks"""
    
    print("🔍 DMS Task Diagnostics")
    print("=" * 50)
    
    # 1. Check task status and basic info
    task_info = dms_client.describe_replication_tasks(
        Filters=[{'Name': 'replication-task-arn', 'Values': [task_arn]}]
    )
    
    task = task_info['ReplicationTasks'][0]
    print(f"Task Status: {task['Status']}")
    print(f"Task Creation Date: {task['ReplicationTaskCreationDate']}")
    
    # 2. Check for recent events
    print("\n📋 Recent Events:")
    events = dms_client.describe_events(
        SourceIdentifier=task_arn.split('/')[-1],
        SourceType='replication-task',
        MaxRecords=20
    )
    
    for event in events['Events'][:5]:  # Show last 5 events
        print(f"  {event['Date']}: {event['Message']}")
    
    # 3. Check table statistics for stuck tables
    print("\n📊 Table Statistics:")
    stats = dms_client.describe_table_statistics(
        ReplicationTaskArn=task_arn
    )
    
    for table_stat in stats['TableStatistics']:
        table_state = table_stat.get('TableState', 'Unknown')
        if table_state in ['Table error', 'Table does not exist']:
            print(f"  ❌ {table_stat['SchemaName']}.{table_stat['TableName']}: {table_state}")
        elif table_state == 'Full load':
            print(f"  🔄 {table_stat['SchemaName']}.{table_stat['TableName']}: {table_state}")
        else:
            print(f"  ✅ {table_stat['SchemaName']}.{table_stat['TableName']}: {table_state}")
    
    # 4. Test endpoint connections
    print("\n🔗 Testing Endpoint Connections:")
    
    task_endpoints = dms_client.describe_endpoints(
        Filters=[
            {'Name': 'engine-name', 'Values': ['oracle', 'redshift', 'postgresql']}
        ]
    )
    
    for endpoint in task_endpoints['Endpoints']:
        try:
            test_result = dms_client.test_connection(
                ReplicationInstanceArn=task['ReplicationInstanceArn'],
                EndpointArn=endpoint['EndpointArn']
            )
            print(f"  Testing {endpoint['EndpointIdentifier']}: Initiated")
        except Exception as e:
            print(f"  ❌ {endpoint['EndpointIdentifier']}: {str(e)}")

def common_error_solutions():
    """Common DMS errors and their solutions"""
    
    errors_and_solutions = {
        "ERROR: relation does not exist": {
            "cause": "Target table not created or wrong schema",
            "solution": "Check table mapping rules and target preparation mode"
        },
        "ERROR: Timeout waiting for WAL": {
            "cause": "WAL (Write-Ahead Log) issues in PostgreSQL source",
            "solution": "Increase wal_sender_timeout and check replication slots"
        },
        "ORA-01555: snapshot too old": {
            "cause": "Oracle undo retention too small",
            "solution": "Increase undo_retention parameter in Oracle"
        },
        "Connection to the database failed": {
            "cause": "Network or authentication issues",
            "solution": "Check security groups, NACLs, and credentials"
        },
        "Insufficient privileges": {
            "cause": "Database user lacks required permissions",
            "solution": "Grant necessary privileges for migration"
        }
    }
    
    print("\n🛠️ Common Error Solutions:")
    print("=" * 50)
    
    for error, details in errors_and_solutions.items():
        print(f"\nError: {error}")
        print(f"  Cause: {details['cause']}")
        print(f"  Solution: {details['solution']}")

def optimize_migration_performance(task_arn):
    """Recommendations for improving migration performance"""
    
    print("\n⚡ Performance Optimization Tips:")
    print("=" * 50)
    
    recommendations = [
        "1. Use parallel load settings for large tables",
        "2. Increase MaxFullLoadSubTasks for faster full load",
        "3. Enable BatchApplyEnabled for better CDC performance",
        "4. Use larger replication instance for CPU-intensive tasks",
        "5. Optimize source database (disable triggers, indexes during load)",
        "6. Use COPY command optimization for Redshift targets",
        "7. Monitor and adjust MemoryLimitTotal setting",
        "8. Use table-level parallelism for independent tables"
    ]
    
    for rec in recommendations:
        print(f"  {rec}")
    
    # Sample optimized task settings
    optimized_settings = {
        "TargetMetadata": {
            "ParallelLoadThreads": 8,
            "ParallelLoadBufferSize": 50,
            "BatchApplyEnabled": True,
            "BatchApplyPreserveTransaction": False
        },
        "FullLoadSettings": {
            "MaxFullLoadSubTasks": 8,
            "TransactionConsistencyTimeout": 600,
            "CommitRate": 10000
        },
        "ChangeProcessingSettings": {
            "MemoryLimitTotal": 1024,
            "MemoryKeepTime": 60,
            "StatementCacheSize": 50
        },
        "ErrorBehavior": {
            "DataErrorPolicy": "LOG_ERROR",
            "DataTruncationErrorPolicy": "LOG_ERROR",
            "DataErrorEscalationPolicy": "SUSPEND_TABLE",
            "DataErrorEscalationCount": 50
        }
    }
    
    print(f"\n📝 Optimized Task Settings:")
    print(json.dumps(optimized_settings, indent=2))

# Run diagnostics
try:
    task_arn = 'arn:aws:dms:us-west-2:123456789012:task:ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    diagnose_migration_issues(task_arn)
    common_error_solutions()
    optimize_migration_performance(task_arn)
except Exception as e:
    print(f"Diagnostics failed: {e}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def dms_schema_conversion_tab():
    """Content for AWS DMS Schema Conversion tab"""
    st.markdown("## 📊 AWS DMS Schema Conversion")
    st.markdown("*Use DMS Schema Conversion to assess complexity and convert database schemas*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Schema Conversion Capabilities
    DMS Schema Conversion automates the process of converting database schemas and code objects:
    - **Automated Assessment**: Analyze migration complexity and effort required
    - **Schema Transformation**: Convert tables, indexes, and constraints automatically  
    - **Code Conversion**: Transform stored procedures, functions, and views
    - **Action Items**: Clear guidance for manual conversion tasks
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Schema conversion process
    st.markdown("#### 🔄 Schema Conversion Process")
    common.mermaid(create_schema_conversion_flow(), height=1200)
    
    # Interactive conversion analyzer
    st.markdown("#### 🔍 Migration Complexity Analyzer")
    
    col1, col2 = st.columns(2)
    
    with col1:
        source_db = st.selectbox("Source Database", [
            "Oracle", "SQL Server", "MySQL", "PostgreSQL", "IBM Db2", "SAP ASE"
        ])
        
        schema_objects = st.multiselect("Schema Objects", [
            "Tables", "Views", "Stored Procedures", "Functions", 
            "Triggers", "Indexes", "Constraints", "Sequences"
        ], default=["Tables", "Views", "Stored Procedures"])
    
    with col2:
        target_db = st.selectbox("Target Database", [
            "Amazon Redshift", "Amazon RDS PostgreSQL", "Amazon RDS MySQL", "Amazon Aurora"
        ])
        
        complexity_factors = st.multiselect("Complexity Factors", [
            "Custom Data Types", "Complex Stored Procedures", "Triggers with Business Logic",
            "Nested Queries", "Vendor-Specific Functions", "Partitioned Tables"
        ])
    
    # Calculate complexity score
    complexity_score = calculate_complexity_score(source_db, target_db, schema_objects, complexity_factors)
    complexity_level = get_complexity_level(complexity_score)
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Migration Complexity Assessment
    **Source**: {source_db} → **Target**: {target_db}  
    **Complexity Score**: {complexity_score}/100  
    **Complexity Level**: {complexity_level}  
    **Estimated Effort**: {get_effort_estimate(complexity_score)}  
    **Automation Level**: {get_automation_level(source_db, target_db)}%
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Conversion compatibility matrix
    st.markdown("#### 🔄 Database Conversion Compatibility")
    
    compatibility_data = create_compatibility_matrix()
    st.dataframe(compatibility_data, use_container_width=True)
    
    # Common conversion challenges
    st.markdown("#### ⚠️ Common Conversion Challenges")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="warning-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚨 High-Risk Conversions
        **Oracle → PostgreSQL:**
        - PL/SQL procedures → PL/pgSQL
        - Oracle-specific functions
        - Hierarchical queries (CONNECT BY)
        - Packages and package bodies
        
        **SQL Server → Redshift:**
        - T-SQL stored procedures
        - Common Table Expressions (CTE)
        - Window functions variations
        - Identity columns handling
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="info-box">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Well-Supported Conversions
        **MySQL → Aurora MySQL:**
        - Tables and indexes
        - Basic stored procedures
        - Standard data types
        - Views and triggers
        
        **PostgreSQL → Aurora PostgreSQL:**
        - Full compatibility
        - Extensions support
        - Data types preserved
        - Minimal manual work
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Schema Conversion Examples")
    
    tab1, tab2, tab3 = st.tabs(["Assessment Report", "Schema Transformation", "Manual Conversions"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Generate DMS Schema Conversion Assessment Report
import boto3
import json

# Note: DMS Schema Conversion is accessed through the DMS console
# This example shows the Python equivalent for assessment

def analyze_database_schema(connection_params):
    """Analyze source database schema for conversion complexity"""
    
    analysis_results = {
        "database_info": {
            "engine": connection_params["engine"],
            "version": connection_params["version"],
            "total_schemas": 0,
            "total_tables": 0,
            "total_procedures": 0,
            "total_functions": 0
        },
        "conversion_summary": {
            "automatic_conversion": 0,
            "manual_conversion": 0,
            "action_items": []
        },
        "complexity_factors": []
    }
    
    # Simulate schema analysis (in real scenario, this connects to database)
    if connection_params["engine"] == "oracle":
        # Oracle-specific analysis
        oracle_challenges = [
            "PL/SQL packages need manual conversion",
            "ROWNUM usage requires rewriting",
            "Oracle-specific date functions",
            "Hierarchical queries with CONNECT BY",
            "Custom object types not supported"
        ]
        analysis_results["complexity_factors"].extend(oracle_challenges)
        analysis_results["conversion_summary"]["manual_conversion"] = 35
        analysis_results["conversion_summary"]["automatic_conversion"] = 65
        
    elif connection_params["engine"] == "sqlserver":
        # SQL Server-specific analysis  
        sqlserver_challenges = [
            "T-SQL stored procedures need conversion",
            "IDENTITY columns handling",
            "Common Table Expressions (CTE) syntax",
            "SQL Server-specific functions",
            "Computed columns conversion"
        ]
        analysis_results["complexity_factors"].extend(sqlserver_challenges)
        analysis_results["conversion_summary"]["manual_conversion"] = 25
        analysis_results["conversion_summary"]["automatic_conversion"] = 75
    
    return analysis_results

def generate_assessment_report(source_config, target_config):
    """Generate comprehensive assessment report"""
    
    print("📊 DATABASE MIGRATION ASSESSMENT REPORT")
    print("=" * 60)
    
    # Source database analysis
    source_analysis = analyze_database_schema(source_config)
    
    print(f"\n🔍 SOURCE DATABASE ANALYSIS")
    print(f"Engine: {source_config['engine'].upper()}")
    print(f"Version: {source_config['version']}")
    print(f"Schemas Analyzed: {source_analysis['database_info']['total_schemas']}")
    print(f"Tables Found: {source_analysis['database_info']['total_tables']}")
    
    # Conversion feasibility
    auto_conversion = source_analysis['conversion_summary']['automatic_conversion']
    manual_conversion = source_analysis['conversion_summary']['manual_conversion']
    
    print(f"\n📋 CONVERSION FEASIBILITY")
    print(f"Automatic Conversion: {auto_conversion}%")
    print(f"Manual Conversion Required: {manual_conversion}%")
    
    # Action items
    print(f"\n⚠️ COMPLEXITY FACTORS")
    for i, factor in enumerate(source_analysis['complexity_factors'], 1):
        print(f"{i}. {factor}")
    
    # Recommendations
    recommendations = generate_recommendations(source_config, target_config, auto_conversion)
    print(f"\n✅ RECOMMENDATIONS")
    for i, rec in enumerate(recommendations, 1):
        print(f"{i}. {rec}")
    
    return {
        "source_analysis": source_analysis,
        "recommendations": recommendations,
        "estimated_effort_weeks": estimate_effort(manual_conversion),
        "success_probability": calculate_success_probability(auto_conversion)
    }

def generate_recommendations(source_config, target_config, auto_percentage):
    """Generate migration recommendations based on analysis"""
    
    recommendations = []
    
    if auto_percentage >= 80:
        recommendations.append("High automation potential - proceed with DMS Schema Conversion")
        recommendations.append("Minimal manual intervention required")
    elif auto_percentage >= 60:
        recommendations.append("Good automation potential with some manual work")
        recommendations.append("Plan for 2-4 weeks of manual conversion effort")
    else:
        recommendations.append("Significant manual conversion required")
        recommendations.append("Consider phased migration approach")
        recommendations.append("Engage database migration specialists")
    
    # Engine-specific recommendations
    if source_config["engine"] == "oracle" and target_config["engine"] == "postgresql":
        recommendations.append("Consider using orafce extension for Oracle compatibility")
        recommendations.append("Plan for PL/SQL to PL/pgSQL conversion")
    
    return recommendations

# Example usage
source_database = {
    "engine": "oracle",
    "version": "19c",
    "host": "oracle-prod.company.com",
    "schemas": ["SALES", "INVENTORY", "HR"]
}

target_database = {
    "engine": "postgresql", 
    "version": "13",
    "instance_type": "Amazon RDS"
}

# Generate assessment
assessment = generate_assessment_report(source_database, target_database)
print(f"\nEstimated project duration: {assessment['estimated_effort_weeks']} weeks")
print(f"Success probability: {assessment['success_probability']}%")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Schema Transformation Examples
-- Original Oracle Table
CREATE TABLE employees (
    emp_id NUMBER(10) PRIMARY KEY,
    emp_name VARCHAR2(100) NOT NULL,
    hire_date DATE DEFAULT SYSDATE,
    salary NUMBER(10,2),
    department_id NUMBER(10),
    created_by VARCHAR2(50) DEFAULT USER,
    CONSTRAINT fk_dept FOREIGN KEY (department_id) REFERENCES departments(dept_id)
);

-- Converted to PostgreSQL (Automatic)
CREATE TABLE employees (
    emp_id INTEGER PRIMARY KEY,
    emp_name VARCHAR(100) NOT NULL,
    hire_date DATE DEFAULT CURRENT_DATE,
    salary DECIMAL(10,2),
    department_id INTEGER,
    created_by VARCHAR(50) DEFAULT CURRENT_USER,
    CONSTRAINT fk_dept FOREIGN KEY (department_id) REFERENCES departments(dept_id)
);

-- Original Oracle PL/SQL Procedure
CREATE OR REPLACE PROCEDURE update_employee_salary(
    p_emp_id IN NUMBER,
    p_new_salary IN NUMBER
) AS
BEGIN
    UPDATE employees 
    SET salary = p_new_salary,
        modified_date = SYSDATE
    WHERE emp_id = p_emp_id;
    
    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Employee not found');
    END IF;
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;

-- Converted to PostgreSQL PL/pgSQL (Manual adjustment needed)
CREATE OR REPLACE FUNCTION update_employee_salary(
    p_emp_id INTEGER,
    p_new_salary DECIMAL
) RETURNS VOID AS $$
DECLARE
    row_count INTEGER;
BEGIN
    UPDATE employees 
    SET salary = p_new_salary,
        modified_date = CURRENT_DATE
    WHERE emp_id = p_emp_id;
    
    GET DIAGNOSTICS row_count = ROW_COUNT;
    
    IF row_count = 0 THEN
        RAISE EXCEPTION 'Employee not found' USING ERRCODE = 'P0001';
    END IF;
    
    -- PostgreSQL handles transactions automatically in functions
EXCEPTION
    WHEN OTHERS THEN
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- SQL Server to Redshift Conversion
-- Original SQL Server Table with Identity
CREATE TABLE sales_orders (
    order_id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATETIME2 DEFAULT GETDATE(),
    total_amount MONEY,
    status VARCHAR(20) DEFAULT 'PENDING'
);

-- Converted to Redshift
CREATE TABLE sales_orders (
    order_id INTEGER IDENTITY(1,1),
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP DEFAULT GETDATE(),
    total_amount DECIMAL(19,4),
    status VARCHAR(20) DEFAULT 'PENDING'
)
DISTKEY(customer_id)
SORTKEY(order_date)
ENCODE AUTO;

-- Complex Oracle Query with Hierarchical Data
SELECT employee_id, employee_name, manager_id, level, 
       SYS_CONNECT_BY_PATH(employee_name, '/') as hierarchy_path
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id
ORDER SIBLINGS BY employee_name;

-- PostgreSQL Equivalent (Manual Conversion)
WITH RECURSIVE employee_hierarchy AS (
    -- Base case: top-level managers
    SELECT 
        employee_id, 
        employee_name, 
        manager_id,
        1 as level,
        employee_name::TEXT as hierarchy_path
    FROM employees 
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive case: employees with managers
    SELECT 
        e.employee_id,
        e.employee_name,
        e.manager_id,
        eh.level + 1,
        eh.hierarchy_path || '/' || e.employee_name
    FROM employees e
    JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT employee_id, employee_name, manager_id, level, hierarchy_path
FROM employee_hierarchy
ORDER BY hierarchy_path;

-- MySQL to Aurora MySQL (Minimal Changes)
-- Original MySQL
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_name (name),
    INDEX idx_price (price)
) ENGINE=InnoDB;

-- Aurora MySQL (No changes needed - full compatibility)
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_name (name),
    INDEX idx_price (price)
) ENGINE=InnoDB;
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Manual Conversion Guidelines and Templates

# 1. Oracle ROWNUM to PostgreSQL LIMIT/OFFSET
-- Original Oracle
SELECT * FROM (
    SELECT emp_id, emp_name, ROWNUM as rn
    FROM employees
    ORDER BY hire_date
) WHERE rn <= 10;

-- PostgreSQL Conversion
SELECT emp_id, emp_name
FROM employees
ORDER BY hire_date
LIMIT 10;

# 2. SQL Server TOP to PostgreSQL LIMIT
-- Original SQL Server
SELECT TOP 100 customer_id, order_date, total_amount
FROM orders
ORDER BY order_date DESC;

-- PostgreSQL Conversion
SELECT customer_id, order_date, total_amount
FROM orders
ORDER BY order_date DESC
LIMIT 100;

# 3. Oracle (+) Outer Join to ANSI JOIN
-- Original Oracle
SELECT e.emp_name, d.dept_name
FROM employees e, departments d
WHERE e.dept_id = d.dept_id(+);

-- Standard ANSI (works in all databases)
SELECT e.emp_name, d.dept_name
FROM employees e
LEFT OUTER JOIN departments d ON e.dept_id = d.dept_id;

# 4. Database-Specific Function Conversions
def convert_database_functions(sql_text, source_engine, target_engine):
    """Convert database-specific functions"""
    
    conversions = {
        ("oracle", "postgresql"): {
            "SYSDATE": "CURRENT_DATE",
            "SYSTIMESTAMP": "CURRENT_TIMESTAMP", 
            "USER": "CURRENT_USER",
            "LENGTH": "CHAR_LENGTH",
            "SUBSTR": "SUBSTRING",
            "INSTR": "POSITION",
            "NVL": "COALESCE",
            "NVL2": "COALESCE",
            "DECODE": "CASE WHEN"
        },
        ("sqlserver", "postgresql"): {
            "GETDATE()": "CURRENT_TIMESTAMP",
            "GETUTCDATE()": "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'",
            "ISNULL": "COALESCE",
            "LEN": "LENGTH",
            "DATEDIFF": "DATE_PART",
            "PATINDEX": "POSITION"
        },
        ("oracle", "redshift"): {
            "SYSDATE": "GETDATE()",
            "SYSTIMESTAMP": "GETDATE()",
            "ROWNUM": "ROW_NUMBER() OVER()",
            "CONNECT BY": "-- Requires recursive CTE conversion",
            "DUAL": "-- Remove DUAL table references"
        }
    }
    
    conversion_map = conversions.get((source_engine, target_engine), {})
    
    converted_sql = sql_text
    for old_func, new_func in conversion_map.items():
        converted_sql = converted_sql.replace(old_func, new_func)
    
    return converted_sql

# 5. Error Handling Conversion
-- Oracle Exception Handling
CREATE OR REPLACE PROCEDURE process_order(p_order_id NUMBER) AS
BEGIN
    -- Process order logic
    UPDATE orders SET status = 'PROCESSED' WHERE order_id = p_order_id;
    
    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Order not found');
    END IF;
EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
        RAISE_APPLICATION_ERROR(-20002, 'Duplicate order processing');
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;

-- PostgreSQL Equivalent
CREATE OR REPLACE FUNCTION process_order(p_order_id INTEGER) 
RETURNS VOID AS $$
DECLARE
    row_count INTEGER;
BEGIN
    -- Process order logic
    UPDATE orders SET status = 'PROCESSED' WHERE order_id = p_order_id;
    
    GET DIAGNOSTICS row_count = ROW_COUNT;
    
    IF row_count = 0 THEN
        RAISE EXCEPTION 'Order not found' USING ERRCODE = 'P0001';
    END IF;
EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'Duplicate order processing' USING ERRCODE = 'P0002';
    WHEN OTHERS THEN
        RAISE;
END;
$$ LANGUAGE plpgsql;

# 6. Data Type Conversion Matrix
data_type_conversions = {
    "oracle_to_postgresql": {
        "NUMBER": "INTEGER/DECIMAL",
        "NUMBER(p,s)": "DECIMAL(p,s)",
        "VARCHAR2": "VARCHAR",
        "CLOB": "TEXT",
        "BLOB": "BYTEA",
        "DATE": "DATE/TIMESTAMP",
        "TIMESTAMP": "TIMESTAMP",
        "RAW": "BYTEA"
    },
    "sqlserver_to_redshift": {
        "INT": "INTEGER",
        "BIGINT": "BIGINT", 
        "MONEY": "DECIMAL(19,4)",
        "DATETIME": "TIMESTAMP",
        "DATETIME2": "TIMESTAMP",
        "NVARCHAR": "VARCHAR",
        "TEXT": "VARCHAR(MAX)",
        "IMAGE": "Not supported",
        "XML": "VARCHAR(MAX)"
    },
    "mysql_to_aurora_mysql": {
        # Most types are directly compatible
        "AUTO_INCREMENT": "AUTO_INCREMENT",  # No change needed
        "ENUM": "ENUM",  # Supported
        "SET": "SET",    # Supported
        "JSON": "JSON"   # Supported in Aurora MySQL 5.7+
    }
}

# 7. Manual Conversion Checklist
conversion_checklist = {
    "Schema Objects": [
        "✓ Tables and columns converted",
        "✓ Primary and foreign keys migrated", 
        "✓ Indexes recreated with appropriate syntax",
        "✓ Views updated with target syntax",
        "✓ Sequences converted to appropriate mechanism"
    ],
    "Code Objects": [
        "⚠ Stored procedures require manual review",
        "⚠ Functions need syntax adaptation",
        "⚠ Triggers require rewriting",
        "⚠ Packages need restructuring"
    ],
    "Data Types": [
        "✓ Standard types automatically converted",
        "⚠ Custom types need manual mapping",
        "⚠ LOB handling verification needed"
    ],
    "Application Code": [
        "⚠ Connection strings updated",
        "⚠ SQL syntax in application code",
        "⚠ Error handling adaptation",
        "⚠ Transaction management review"
    ]
}

print("📋 MANUAL CONVERSION CHECKLIST")
print("=" * 40)
for category, items in conversion_checklist.items():
    print(f"\n{category}:")
    for item in items:
        print(f"  {item}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def sct_tab():
    """Content for AWS Schema Conversion Tool tab"""
    st.markdown("## 🛠️ AWS Schema Conversion Tool (SCT)")
    st.markdown("*Downloadable software to automatically assess and convert database schemas*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Schema Conversion Tool Features
    AWS SCT is a desktop application that provides comprehensive database conversion capabilities:
    - **OLTP Conversion**: Convert relational database schemas between different engines
    - **Data Warehouse**: Transform data warehouse schemas (Teradata, Oracle, SQL Server → Redshift)
    - **NoSQL Migration**: Convert from relational to NoSQL (DynamoDB)
    - **ETL Migration**: Convert ETL processes and data pipelines
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # SCT Architecture
    sct_architecture = """
    graph TB
        subgraph "Desktop Environment"
            SCT[AWS Schema<br/>Conversion Tool<br/>Desktop App]
        end
        
        subgraph "Source Databases"
            ORACLE[(Oracle Database)]
            SQLSERVER[(SQL Server)]
            MYSQL[(MySQL)]
            POSTGRES[(PostgreSQL)]
            TERADATA[(Teradata)]
            DB2[(IBM Db2)]
        end
        
        subgraph "AWS Target Services"
            RDS[(Amazon RDS)]
            AURORA[(Amazon Aurora)]
            REDSHIFT[(Amazon Redshift)]
            DYNAMODB[(Amazon DynamoDB)]
        end
        
        subgraph "SCT Features"
            ASSESS[Assessment<br/>Report]
            CONVERT[Schema<br/>Conversion]
            OPTIMIZE[Query<br/>Optimization]
            VALIDATE[Validation<br/>Reports]
        end
        
        ORACLE --> SCT
        SQLSERVER --> SCT
        MYSQL --> SCT
        POSTGRES --> SCT
        TERADATA --> SCT
        DB2 --> SCT
        
        SCT --> ASSESS
        SCT --> CONVERT
        SCT --> OPTIMIZE
        SCT --> VALIDATE
        
        SCT --> RDS
        SCT --> AURORA
        SCT --> REDSHIFT
        SCT --> DYNAMODB
        
        style SCT fill:#FF9900,stroke:#232F3E,color:#fff
        style ASSESS fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CONVERT fill:#3FB34F,stroke:#232F3E,color:#fff
        style REDSHIFT fill:#232F3E,stroke:#FF9900,color:#fff
    """
    
    st.markdown("#### 🏗️ SCT Architecture & Workflow")
    common.mermaid(sct_architecture, height=550)
    
    # Supported conversions
    st.markdown("#### 🔄 Supported Database Conversions")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗃️ OLTP Database Conversions
        **Sources Supported:**
        - Oracle Database
        - Microsoft SQL Server  
        - MySQL
        - PostgreSQL
        - IBM Db2 LUW
        - SAP ASE (Sybase)
        - Amazon Aurora
        
        **Targets Supported:**
        - Amazon RDS (MySQL, PostgreSQL, Oracle, SQL Server)
        - Amazon Aurora (MySQL, PostgreSQL)
        - Amazon EC2 databases
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏭 Data Warehouse Conversions
        **Sources Supported:**
        - Teradata
        - Oracle Data Warehouse
        - Microsoft SQL Server 
        - IBM Db2 LUW
        - Apache Cassandra
        - IBM Netezza
        
        **Primary Target:**
        - Amazon Redshift
        - Amazon S3 (data lake)
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive conversion simulator
    st.markdown("#### 🎮 SCT Conversion Simulator")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        conversion_type = st.selectbox("Conversion Type", ["OLTP", "Data Warehouse", "NoSQL"])
        source_system = st.selectbox("Source System", get_source_options(conversion_type))
    
    with col2:
        target_system = st.selectbox("Target System", get_target_options(conversion_type))
        schema_size = st.selectbox("Schema Size", ["Small (< 50 objects)", "Medium (50-500 objects)", "Large (> 500 objects)"])
    
    with col3:
        complexity_level = st.selectbox("Code Complexity", ["Low", "Medium", "High"])
        
    # Generate conversion estimate
    conversion_estimate = calculate_sct_estimate(conversion_type, source_system, target_system, schema_size, complexity_level)
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 SCT Conversion Estimate
    **Conversion Path**: {source_system} → {target_system}  
    **Automation Level**: {conversion_estimate['automation']}%  
    **Manual Effort**: {conversion_estimate['manual_days']} days  
    **Key Challenges**: {', '.join(conversion_estimate['challenges'])}  
    **Success Probability**: {conversion_estimate['success_rate']}%
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # SCT Features breakdown
    st.markdown("#### ⭐ SCT Key Features")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="design-pattern">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Assessment Reports
        - **Complexity Analysis**
        - **Effort Estimation**  
        - **Risk Assessment**
        - **Action Item Lists**
        - **Cost Projections**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="design-pattern">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Schema Conversion
        - **Automated DDL Generation**
        - **Data Type Mapping**
        - **Constraint Conversion**
        - **Index Optimization**
        - **View Transformation**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="design-pattern">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 Code Conversion  
        - **Stored Procedure Migration**
        - **Function Conversion**
        - **Trigger Transformation**
        - **Query Optimization**
        - **ETL Process Migration**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 SCT Usage Examples")
    
    tab1, tab2, tab3 = st.tabs(["Installation & Setup", "Project Configuration", "Conversion Process"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# AWS Schema Conversion Tool Installation and Setup

# 1. Download and Install SCT
# Download from: https://docs.aws.amazon.com/SchemaConversionTool/latest/userguide/CHAP_Installing.html
# Available for Windows, macOS, and Linux

# System Requirements:
# - Java 8 or later
# - 4 GB RAM minimum (8 GB recommended)
# - 2 GB disk space
# - Network access to source and target databases

# 2. Database Driver Installation
# SCT requires JDBC drivers for source and target databases

# Oracle Driver Setup
# Download ojdbc8.jar from Oracle website
# Place in SCT installation folder: /lib directory

# SQL Server Driver (already included)
# Microsoft JDBC Driver for SQL Server is pre-installed

# PostgreSQL Driver (already included) 
# PostgreSQL JDBC driver is pre-installed

# MySQL Driver (already included)
# MySQL Connector/J is pre-installed

# 3. AWS Credentials Configuration
# Method 1: AWS CLI
aws configure
# Enter Access Key ID, Secret Access Key, Region

# Method 2: IAM Role (for EC2)
# Attach IAM role with necessary permissions to EC2 instance

# Method 3: Environment Variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-west-2

# Required IAM Permissions for SCT
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "rds:DescribeDBInstances",
                "rds:DescribeDBClusters",
                "redshift:DescribeClusters",
                "redshift:DescribeClusterParameters",
                "s3:ListBucket",
                "s3:GetObject",
                "s3:PutObject",
                "iam:ListRoles",
                "iam:GetRole"
            ],
            "Resource": "*"
        }
    ]
}

# 4. First Time Setup
# Launch SCT application
# Go to Settings > Global Settings
# Configure:
# - AWS Profile
# - Default AWS Region  
# - Logging Level
# - Memory Settings (increase for large schemas)

# 5. License Information
# SCT is provided at no additional charge
# Some database drivers may require separate licensing
# Check vendor licensing requirements for:
# - Oracle Database connectivity
# - IBM Db2 connectivity
# - Teradata connectivity

# 6. Network Configuration
# Ensure firewall/security group rules allow:
# - Source database connectivity (port varies by database)
# - Target database connectivity (port varies by service)
# - Internet access for AWS API calls

# Common Ports:
# Oracle: 1521
# SQL Server: 1433
# MySQL: 3306  
# PostgreSQL: 5432
# Teradata: 1025
# Amazon Redshift: 5439

# 7. Troubleshooting Installation
# Check Java version
java -version

# Increase memory allocation (for large schemas)
# Edit sct.sh (Linux/Mac) or sct.bat (Windows)
# Add: -Xmx8g (for 8GB RAM allocation)

# Verify driver installation
# Check /lib directory for required JDBC drivers
# ls -la /opt/aws-schema-conversion-tool/lib/

# Test connectivity
# Use SCT's built-in connection test feature
# File > Test Connection before starting assessment
        ''', language='bash')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# SCT Project Configuration Examples

# 1. Create New Project
# File > New Project
# Project Type: Database Migration
# Choose source and target database types

# 2. Source Database Configuration
# Connection Parameters for Oracle Source:
{
    "connection_name": "Oracle Production",
    "server_name": "oracle-prod.company.com",
    "port": 1521,
    "service_name": "PROD",  # or SID
    "username": "sct_user",
    "password": "secure_password",
    "connection_options": {
        "use_ssl": true,
        "trust_store_path": "/path/to/truststore.jks"
    }
}

# Connection Parameters for SQL Server Source:
{
    "connection_name": "SQL Server Production", 
    "server_name": "sqlserver-prod.company.com",
    "port": 1433,
    "database_name": "ProductionDB",
    "username": "sct_user",
    "password": "secure_password",
    "authentication": "SQL Server Authentication",  # or Windows Authentication
    "connection_options": {
        "encrypt": true,
        "trust_server_certificate": false
    }
}

# 3. Target Database Configuration  
# Amazon Redshift Target:
{
    "connection_name": "Redshift Analytics",
    "endpoint": "redshift-cluster.abc123.us-west-2.redshift.amazonaws.com",
    "port": 5439,
    "database_name": "analytics",
    "username": "admin", 
    "password": "secure_password",
    "connection_options": {
        "ssl_mode": "require"
    }
}

# Amazon RDS PostgreSQL Target:
{
    "connection_name": "RDS PostgreSQL",
    "endpoint": "postgres-instance.abc123.us-west-2.rds.amazonaws.com",
    "port": 5432,
    "database_name": "migrated_db",
    "username": "postgres",
    "password": "secure_password",
    "connection_options": {
        "ssl_mode": "require"
    }
}

# 4. Project Settings Configuration
project_settings = {
    "general": {
        "project_name": "Oracle_to_Redshift_Migration",
        "description": "Migrate Oracle DW to Amazon Redshift",
        "source_database": "Oracle 19c",
        "target_database": "Amazon Redshift"
    },
    "assessment": {
        "include_schemas": ["SALES", "INVENTORY", "FINANCE"],
        "exclude_schemas": ["SYS", "SYSTEM", "TEMP"],
        "include_objects": [
            "tables", "views", "procedures", "functions", "packages"
        ],
        "complexity_factors": [
            "custom_data_types",
            "nested_procedures", 
            "dynamic_sql",
            "vendor_specific_functions"
        ]
    },
    "conversion": {
        "optimization_level": "balanced",  # conservative, balanced, aggressive
        "preserve_case": false,
        "add_column_comments": true,
        "generate_drop_statements": false
    },
    "output": {
        "sql_file_format": "UTF-8",
        "script_organization": "by_object_type",  # single_file, by_schema, by_object_type
        "include_statistics": true
    }
}

# 5. Schema Mapping Rules
schema_mapping_rules = [
    {
        "rule_type": "rename_schema",
        "source_schema": "SALES_PROD", 
        "target_schema": "sales"
    },
    {
        "rule_type": "rename_table",
        "source_table": "CUSTOMER_INFO",
        "target_table": "customers"
    },
    {
        "rule_type": "exclude_object",
        "object_type": "table",
        "pattern": "TEMP_*"
    },
    {
        "rule_type": "data_type_mapping",
        "source_type": "NUMBER(10)",
        "target_type": "INTEGER"
    }
]

# 6. Conversion Settings
conversion_settings = {
    "data_types": {
        "oracle_number_to_redshift": {
            "NUMBER": "DECIMAL",
            "NUMBER(p)": "DECIMAL(p,0)", 
            "NUMBER(p,s)": "DECIMAL(p,s)"
        },
        "string_conversions": {
            "VARCHAR2": "VARCHAR",
            "CLOB": "VARCHAR(MAX)",
            "CHAR": "CHAR"
        }
    },
    "code_conversion": {
        "procedure_style": "stored_procedure",  # stored_procedure, user_defined_function
        "error_handling": "postgresql_style",
        "transaction_management": "automatic"
    },
    "optimization": {
        "distribution_keys": "auto_detect",
        "sort_keys": "based_on_queries",
        "compression": "auto"
    }
}

# 7. Save Project
# File > Save Project
# Saves all configuration to .sct project file
# Can be shared with team members
# Include version control for project files

print("✅ SCT Project Configuration Complete")
print("Ready to begin database assessment and conversion")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# SCT Conversion Process Step-by-Step

# 1. Database Assessment Phase
def run_database_assessment():
    """
    SCT Assessment generates comprehensive analysis of:
    - Schema complexity
    - Conversion feasibility  
    - Effort estimation
    - Risk factors
    """
    
    assessment_results = {
        "summary": {
            "total_objects": 1250,
            "auto_convertible": 987,  # 79%
            "manual_required": 263,   # 21%
            "estimated_effort_days": 45
        },
        "object_breakdown": {
            "tables": {"total": 450, "auto": 445, "manual": 5},
            "views": {"total": 125, "auto": 98, "manual": 27},
            "procedures": {"total": 89, "auto": 23, "manual": 66},
            "functions": {"total": 156, "auto": 134, "manual": 22},
            "triggers": {"total": 34, "auto": 0, "manual": 34}
        },
        "conversion_issues": [
            {
                "severity": "High",
                "category": "PL/SQL Packages",
                "count": 15,
                "description": "Package bodies require manual conversion"
            },
            {
                "severity": "Medium", 
                "category": "Oracle Functions",
                "count": 45,
                "description": "Oracle-specific functions need replacement"
            },
            {
                "severity": "Low",
                "category": "Data Types",
                "count": 12,
                "description": "Minor data type adjustments needed"
            }
        ]
    }
    
    return assessment_results

# 2. Schema Conversion Phase
def execute_schema_conversion():
    """
    Convert database objects from source to target format
    """
    
    conversion_steps = [
        "1. Connect to source database",
        "2. Extract metadata and DDL",
        "3. Apply conversion rules", 
        "4. Generate target DDL",
        "5. Create conversion report",
        "6. Highlight manual tasks"
    ]
    
    # Example conversion output for Oracle to Redshift
    oracle_table = """
    CREATE TABLE sales_fact (
        sale_id NUMBER(19) PRIMARY KEY,
        customer_id NUMBER(10) NOT NULL,
        product_id NUMBER(10) NOT NULL,
        sale_date DATE NOT NULL,
        amount NUMBER(12,2) NOT NULL,
        quantity NUMBER(8) NOT NULL
    ) TABLESPACE sales_data;
    """
    
    redshift_table = """
    -- Converted by AWS SCT
    CREATE TABLE sales_fact (
        sale_id BIGINT IDENTITY(1,1),
        customer_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        sale_date DATE NOT NULL,
        amount DECIMAL(12,2) NOT NULL,
        quantity INTEGER NOT NULL
    )
    DISTKEY(customer_id)
    SORTKEY(sale_date)
    ENCODE AUTO;
    """
    
    return {
        "source_ddl": oracle_table,
        "target_ddl": redshift_table,
        "conversion_notes": [
            "NUMBER(19) converted to BIGINT with IDENTITY",
            "Added DISTKEY on customer_id for join optimization",
            "Added SORTKEY on sale_date for time queries",
            "Removed TABLESPACE (not applicable to Redshift)"
        ]
    }

# 3. Code Object Conversion
def convert_stored_procedures():
    """
    Convert stored procedures and functions
    """
    
    # Oracle PL/SQL Procedure
    oracle_procedure = """
    CREATE OR REPLACE PROCEDURE get_customer_orders(
        p_customer_id IN NUMBER,
        p_start_date IN DATE,
        p_orders OUT SYS_REFCURSOR
    ) AS
    BEGIN
        OPEN p_orders FOR
        SELECT order_id, order_date, total_amount
        FROM orders
        WHERE customer_id = p_customer_id
          AND order_date >= p_start_date
        ORDER BY order_date DESC;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20001, 'No orders found');
    END get_customer_orders;
    """
    
    # Redshift User-Defined Function (manual conversion required)
    redshift_function = """
    -- Manual conversion required for Redshift
    -- Redshift doesn't support stored procedures with OUT parameters
    -- Alternative: Create view or use application logic
    
    CREATE VIEW customer_orders_view AS
    SELECT 
        customer_id,
        order_id, 
        order_date,
        total_amount
    FROM orders
    WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
    ORDER BY customer_id, order_date DESC;
    
    -- Or create SQL function for specific logic
    CREATE OR REPLACE FUNCTION get_recent_orders(customer_id INTEGER)
    RETURNS TABLE(order_id INTEGER, order_date DATE, total_amount DECIMAL(12,2))
    AS $$
        SELECT o.order_id, o.order_date, o.total_amount
        FROM orders o
        WHERE o.customer_id = $1
          AND o.order_date >= CURRENT_DATE - INTERVAL '30 days'  
        ORDER BY o.order_date DESC;
    $$ LANGUAGE SQL;
    """
    
    return {
        "conversion_status": "Manual intervention required",
        "original_code": oracle_procedure,
        "converted_code": redshift_function,
        "action_items": [
            "Refactor procedure to use views or application logic",
            "Replace OUT parameters with return tables",
            "Update application code to handle different calling pattern"
        ]
    }

# 4. Validation and Testing
def validate_conversion():
    """
    Validate converted schema and identify issues
    """
    
    validation_checks = {
        "schema_validation": {
            "tables_created": True,
            "constraints_applied": True,
            "indexes_created": True,
            "views_functional": False  # Requires manual review
        },
        "data_validation": {
            "row_counts_match": True,
            "data_types_compatible": True,
            "referential_integrity": True
        },
        "performance_validation": {
            "query_execution": "Pending manual optimization",
            "index_effectiveness": "Requires analysis",
            "distribution_keys": "Auto-selected, review recommended"
        }
    }
    
    return validation_checks

# 5. Generate Final Reports
def generate_conversion_reports():
    """
    Generate comprehensive conversion documentation
    """
    
    reports = {
        "assessment_report": "Database_Assessment_Report.pdf",
        "conversion_summary": "Schema_Conversion_Summary.html", 
        "action_items": "Manual_Conversion_Tasks.xlsx",
        "sql_scripts": {
            "ddl_scripts": "target_schema_ddl.sql",
            "dml_scripts": "data_migration_scripts.sql",
            "cleanup_scripts": "cleanup_and_maintenance.sql"
        },
        "validation_report": "Conversion_Validation_Results.html"
    }
    
    return reports

# Execute Complete Conversion Process
print("🚀 Starting SCT Conversion Process")
print("=" * 50)

# Step 1: Assessment
print("Phase 1: Database Assessment")
assessment = run_database_assessment()
print(f"  Objects analyzed: {assessment['summary']['total_objects']}")
print(f"  Auto-convertible: {assessment['summary']['auto_convertible']}")
print(f"  Manual effort: {assessment['summary']['estimated_effort_days']} days")

# Step 2: Schema Conversion  
print("\nPhase 2: Schema Conversion")
schema_conv = execute_schema_conversion()
print("  DDL conversion completed")
print("  Distribution and sort keys applied")

# Step 3: Code Conversion
print("\nPhase 3: Code Object Conversion") 
code_conv = convert_stored_procedures()
print(f"  Status: {code_conv['conversion_status']}")

# Step 4: Validation
print("\nPhase 4: Validation")
validation = validate_conversion()
print("  Schema validation: Completed")
print("  Performance tuning: Required")

# Step 5: Reporting
print("\nPhase 5: Report Generation")
reports = generate_conversion_reports()
print("  All reports generated successfully")

print("\n✅ SCT Conversion Process Complete!")
print("Next Steps: Review action items and begin manual conversions")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

# Helper functions
def calculate_migration_time(size_gb, source, target):
    """Calculate estimated migration time"""
    base_hours = size_gb / 100  # 100 GB per hour baseline
    
    # Adjust for complexity
    complexity_multiplier = {
        ("Oracle", "Amazon Redshift"): 1.5,
        ("SQL Server", "Amazon Redshift"): 1.3,
        ("MySQL", "Amazon Aurora"): 1.1,
        ("PostgreSQL", "Amazon Aurora"): 1.0
    }
    
    multiplier = complexity_multiplier.get((source, target), 1.2)
    estimated_hours = base_hours * multiplier
    
    if estimated_hours < 1:
        return "< 1 hour"
    elif estimated_hours < 24:
        return f"{estimated_hours:.1f} hours"
    else:
        return f"{estimated_hours/24:.1f} days"

def get_migration_strategy(downtime_tolerance, size_gb):
    """Get recommended migration strategy"""
    if downtime_tolerance == "Zero downtime required":
        return "Full Load + CDC"
    elif size_gb > 1000 and downtime_tolerance in ["< 1 hour", "< 4 hours"]:
        return "Full Load + CDC"
    else:
        return "Full Load Only"

def get_instance_recommendation(size_gb):
    """Get DMS instance recommendation based on size"""
    if size_gb < 100:
        return "dms.t3.medium"
    elif size_gb < 1000:
        return "dms.r5.large"
    elif size_gb < 10000:
        return "dms.r5.2xlarge"
    else:
        return "dms.r5.8xlarge"

def calculate_complexity_score(source_db, target_db, objects, factors):
    """Calculate migration complexity score"""
    base_score = 30
    
    # Database engine compatibility
    engine_scores = {
        ("Oracle", "Amazon Redshift"): 40,
        ("SQL Server", "Amazon Redshift"): 30,
        ("MySQL", "Amazon Aurora"): 10,
        ("PostgreSQL", "Amazon Aurora"): 5
    }
    
    engine_score = engine_scores.get((source_db, target_db), 25)
    
    # Object complexity
    object_scores = {
        "Tables": 5,
        "Views": 10,
        "Stored Procedures": 25,
        "Functions": 20,
        "Triggers": 30,
        "Indexes": 5,
        "Constraints": 5,
        "Sequences": 10
    }
    
    object_score = sum(object_scores.get(obj, 0) for obj in objects) / len(objects) if objects else 0
    
    # Complexity factors
    factor_score = len(factors) * 5
    
    total_score = min(base_score + engine_score + object_score + factor_score, 100)
    return int(total_score)

def get_complexity_level(score):
    """Get complexity level description"""
    if score <= 30:
        return "Low Complexity"
    elif score <= 60:
        return "Medium Complexity"  
    else:
        return "High Complexity"

def get_effort_estimate(score):
    """Get effort estimate based on complexity"""
    if score <= 30:
        return "1-2 weeks"
    elif score <= 60:
        return "4-8 weeks"
    else:
        return "12+ weeks"

def get_automation_level(source, target):
    """Get automation percentage"""
    automation_levels = {
        ("Oracle", "Amazon Redshift"): 65,
        ("SQL Server", "Amazon Redshift"): 75,
        ("MySQL", "Amazon Aurora"): 95,
        ("PostgreSQL", "Amazon Aurora"): 98
    }
    return automation_levels.get((source, target), 70)

def create_compatibility_matrix():
    """Create database compatibility matrix"""
    data = {
        'Source → Target': [
            'Oracle → Redshift',
            'SQL Server → Redshift', 
            'MySQL → Aurora MySQL',
            'PostgreSQL → Aurora PostgreSQL',
            'Oracle → RDS PostgreSQL',
            'SQL Server → RDS PostgreSQL',
            'Teradata → Redshift',
            'MongoDB → DynamoDB'
        ],
        'Complexity': ['High', 'Medium', 'Low', 'Low', 'High', 'Medium', 'High', 'Medium'],
        'Automation %': [65, 75, 95, 98, 60, 70, 55, 80],
        'Manual Effort': ['High', 'Medium', 'Low', 'Minimal', 'High', 'Medium', 'Very High', 'Medium'],
        'Success Rate': ['85%', '90%', '98%', '99%', '80%', '85%', '75%', '88%']
    }
    return pd.DataFrame(data)

def get_source_options(conversion_type):
    """Get source database options based on conversion type"""
    options = {
        "OLTP": ["Oracle", "SQL Server", "MySQL", "PostgreSQL", "IBM Db2"],
        "Data Warehouse": ["Teradata", "Oracle DW", "SQL Server DW", "IBM Netezza"],
        "NoSQL": ["MongoDB", "Cassandra", "MySQL", "PostgreSQL"]
    }
    return options.get(conversion_type, ["Oracle", "SQL Server"])

def get_target_options(conversion_type):
    """Get target database options based on conversion type"""
    options = {
        "OLTP": ["Amazon RDS", "Amazon Aurora", "Amazon EC2"],
        "Data Warehouse": ["Amazon Redshift", "Amazon S3"],
        "NoSQL": ["Amazon DynamoDB", "Amazon DocumentDB"]
    }
    return options.get(conversion_type, ["Amazon RDS", "Amazon Aurora"])

def calculate_sct_estimate(conv_type, source, target, size, complexity):
    """Calculate SCT conversion estimate"""
    base_automation = 70
    
    # Adjust based on conversion path
    if source == "Oracle" and "Redshift" in target:
        base_automation = 65
    elif source == "MySQL" and "Aurora" in target:
        base_automation = 95
    elif source == "PostgreSQL" and "Aurora" in target:
        base_automation = 98
    
    # Adjust for complexity
    complexity_adjustments = {"Low": 10, "Medium": 0, "High": -15}
    automation = max(base_automation + complexity_adjustments.get(complexity, 0), 40)
    
    # Calculate manual effort
    size_multipliers = {
        "Small (< 50 objects)": 0.5,
        "Medium (50-500 objects)": 1.0,
        "Large (> 500 objects)": 2.0
    }
    
    base_days = 10
    manual_days = int(base_days * size_multipliers.get(size, 1.0) * (100 - automation) / 50)
    
    # Common challenges
    challenges = []
    if source == "Oracle":
        challenges.extend(["PL/SQL conversion", "Oracle functions"])
    if "Redshift" in target:
        challenges.extend(["Distribution keys", "Sort keys"])
    if complexity == "High":
        challenges.extend(["Custom code", "Complex queries"])
    
    return {
        'automation': automation,
        'manual_days': manual_days,
        'challenges': challenges[:3],  # Top 3 challenges
        'success_rate': min(95, 60 + automation // 3)
    }

def main():
    """Main application function"""
    # Apply styling
    apply_custom_styles()
    
    # Initialize session
    initialize_session_state()
    
    # Create sidebar
    create_sidebar()
    
    # Main header
    st.markdown("""
    # 🏗️ AWS Data Modeling & Schema Management
    <div class='info-box'>
    Master database schema design, migration strategies, and automated conversion techniques for AWS data platforms including Amazon Redshift, RDS, and Aurora.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🏗️ Amazon Redshift Schema Design",
        "🔄 AWS Database Migration Service", 
        "📊 AWS DMS Schema Conversion",
        "🛠️ AWS Schema Conversion Tool (SCT)"
    ])
    
    with tab1:
        redshift_schema_design_tab()
    
    with tab2:
        dms_tab()
    
    with tab3:
        dms_schema_conversion_tab()
    
    with tab4:
        sct_tab()
    
    # Footer
    st.markdown("""
    <div class="footer">
        <p>© 2025, Amazon Web Services, Inc. or its affiliates. All rights reserved.</p>
    </div>
    """, unsafe_allow_html=True)

# Main execution flow
if __name__ == "__main__":
    if 'localhost' in st.context.headers["host"]:
        main()
    else:
        # First check authentication
        is_authenticated = authenticate.login()
        
        # If authenticated, show the main app content
        if is_authenticated:
            main()
