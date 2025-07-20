
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from datetime import datetime, timedelta
import utils.common as common

# Page configuration
st.set_page_config(
    page_title="AWS Data Store Management - Lifecycle & Cost Optimization",
    page_icon="🗄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

common.initialize_mermaid()

# AWS Color Scheme
AWS_COLORS = {
    'primary': '#FF9900',
    'secondary': '#232F3E',
    'light_blue': '#4B9EDB',
    'dark_blue': '#1B2631',
    'light_gray': '#F2F3F3',
    'success': '#3FB34F',
    'danger': '#D13212'
}

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

        .info-box {{
            background-color: #E6F2FF;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 15px;
            border-left: 5px solid #00A1C9;
        }}
        
        .highlight-box {{
            background: linear-gradient(135deg, {AWS_COLORS['light_blue']} 0%, {AWS_COLORS['primary']} 100%);
            padding: 20px;
            border-radius: 12px;
            color: white;
            margin: 15px 0;
        }}
        
        .code-container {{
            background-color: {AWS_COLORS['dark_blue']};
            color: white;
            padding: 20px;
            border-radius: 10px;
            border-left: 4px solid {AWS_COLORS['primary']};
            margin: 15px 0;
        }}
        
        .cost-card {{
            background: white;
            padding: 15px;
            border-radius: 10px;
            border: 2px solid {AWS_COLORS['light_blue']};
            margin: 10px 0;
            text-align: center;
        }}
        
        .lifecycle-stage {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            border: 2px solid {AWS_COLORS['success']};
            margin: 10px 0;
            text-align: center;
        }}
        
        .footer {{
            text-align: center;
            padding: 1rem;
            background-color: {AWS_COLORS['secondary']};
            color: white;
            margin-top: 2rem;
            border-radius: 8px;
        }}
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    
    common.initialize_session_state()
    if 'session_started' not in st.session_state:
        st.session_state.session_started = True

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🔄 Redshift – Loading data from Amazon S3
            - 📦 Amazon S3 Lifecycle management  
            - ⏰ S3 Lifecycle – Expiring Objects
            - 🧊 S3 Glacier Classes – Cost considerations
            - 📋 Amazon S3 Versioning system
            - ⏲️ Amazon DynamoDB - Time to Live (TTL)
            
            **Learning Objectives:**
            - Master data loading strategies for Redshift
            - Optimize storage costs with lifecycle policies
            - Understand S3 versioning and object expiration
            - Calculate Glacier storage costs effectively
            - Implement DynamoDB TTL for data management
            """)
        

def create_redshift_loading_mermaid():
    """Create mermaid diagram for Redshift data loading"""
    return """
    graph TB
        subgraph "Data Sources"
            S3[Amazon S3<br/>CSV, JSON, Parquet]
            RDS[Amazon RDS<br/>Relational Data]
            DDB[Amazon DynamoDB<br/>NoSQL Data]
        end
        
        subgraph "AWS Redshift Cluster"
            LEADER[Leader Node<br/>Query Planning]
            
            subgraph "Compute Nodes"
                CN1[Compute Node 1]
                CN2[Compute Node 2]
                CN3[Compute Node 3]
            end
        end
        
        subgraph "Loading Methods"
            COPY[COPY Command<br/>Parallel Loading]
            BULK[Bulk Insert<br/>INSERT statements]
            STREAM[Streaming<br/>Real-time data]
        end
        
        S3 --> COPY
        RDS --> COPY
        DDB --> COPY
        
        COPY --> LEADER
        BULK --> LEADER
        STREAM --> LEADER
        
        LEADER --> CN1
        LEADER --> CN2
        LEADER --> CN3
        
        style S3 fill:#FF9900,stroke:#232F3E,color:#fff
        style LEADER fill:#4B9EDB,stroke:#232F3E,color:#fff
        style COPY fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_s3_lifecycle_mermaid():
    """Create mermaid diagram for S3 lifecycle transitions"""
    return """
    graph LR
        UPLOAD[Object Upload] --> STANDARD[S3 Standard<br/>$0.023/GB/month]
        
        STANDARD --> |30 days| IA[S3 Standard-IA<br/>$0.0125/GB/month]
        IA --> |90 days| GLACIER[S3 Glacier<br/>$0.004/GB/month]
        GLACIER --> |180 days| DEEP[S3 Glacier Deep Archive<br/>$0.00099/GB/month]
        
        DEEP --> |2555 days| DELETE[Object Expiration<br/>Automatic Deletion]
        
        subgraph "Intelligent Tiering"
            INTEL[S3 Intelligent-Tiering<br/>Automatic optimization]
        end
        
        STANDARD -.-> |Optional| INTEL
        
        style STANDARD fill:#FF9900,stroke:#232F3E,color:#fff
        style IA fill:#4B9EDB,stroke:#232F3E,color:#fff
        style GLACIER fill:#3FB34F,stroke:#232F3E,color:#fff
        style DEEP fill:#232F3E,stroke:#FF9900,color:#fff
        style DELETE fill:#D13212,stroke:#232F3E,color:#fff
    """

def create_s3_versioning_mermaid():
    """Create mermaid diagram for S3 versioning"""
    return """
    graph TB
        subgraph "S3 Bucket with Versioning"
            OBJ[Object: document.txt]
            
            subgraph "Version History"
                V1[Version 1<br/>Current]
                V2[Version 2<br/>Previous]
                V3[Version 3<br/>Oldest]
            end
            
            DEL[Delete Marker<br/>Logical deletion]
        end
        
        OBJ --> V1
        V1 --> V2
        V2 --> V3
        
        DELETE[DELETE Request] --> DEL
        DEL -.-> V1
        
        RESTORE[Restore Version] --> V2
        V2 --> CURRENT[Becomes Current]
        
        PERM_DELETE[Permanent Delete] --> V3
        V3 --> GONE[Permanently Removed]
        
        style V1 fill:#FF9900,stroke:#232F3E,color:#fff
        style V2 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style V3 fill:#3FB34F,stroke:#232F3E,color:#fff
        style DEL fill:#D13212,stroke:#232F3E,color:#fff
    """

def create_dynamodb_ttl_mermaid():
    """Create mermaid diagram for DynamoDB TTL"""
    return """
    graph TB
        subgraph "DynamoDB Table"
            ITEM[Table Item]
            TTL_ATTR[TTL Attribute<br/>Timestamp]
            DATA[Other Attributes]
        end
        
        subgraph "TTL Process"
            CHECK[TTL Background Process<br/>Checks every 48 hours]
            COMPARE[Compare TTL timestamp<br/>with current time]
            EXPIRE[Mark for deletion<br/>if expired]
            DELETE[Delete expired items<br/>within 48 hours]
        end
        
        ITEM --> TTL_ATTR
        ITEM --> DATA
        
        TTL_ATTR --> CHECK
        CHECK --> COMPARE
        COMPARE --> |Past time| EXPIRE
        COMPARE --> |Future time| KEEP[Keep item]
        EXPIRE --> DELETE
        
        DELETE --> STREAM[DynamoDB Streams<br/>Optional notification]
        
        style ITEM fill:#FF9900,stroke:#232F3E,color:#fff
        style TTL_ATTR fill:#4B9EDB,stroke:#232F3E,color:#fff
        style DELETE fill:#D13212,stroke:#232F3E,color:#fff
        style STREAM fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def calculate_s3_lifecycle_cost(object_size_gb, days, current_tier="Standard"):
    """Calculate S3 storage cost based on lifecycle tier"""
    costs_per_gb = {
        "Standard": 0.023,
        "Standard-IA": 0.0125,
        "Glacier": 0.004,
        "Deep Archive": 0.00099
    }
    
    # Determine tier based on age
    if days < 30:
        tier = "Standard"
    elif days < 90:
        tier = "Standard-IA"
    elif days < 180:
        tier = "Glacier"
    else:
        tier = "Deep Archive"
    
    monthly_cost = object_size_gb * costs_per_gb[tier]
    return tier, monthly_cost

def redshift_loading_tab():
    """Content for Redshift data loading tab"""
    st.markdown("## 🔄 Redshift – Loading data from Amazon S3")
    st.markdown("*Efficiently load large datasets into Amazon Redshift using parallel processing*")
    
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    The COPY command in Amazon Redshift loads data in parallel from Amazon S3, providing much better 
    performance than INSERT statements. It leverages Redshift's MPP (Massively Parallel Processing) 
    architecture to distribute the workload across compute nodes.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("###  🏗️ Redshift Data Loading Architecture")
    common.mermaid(create_redshift_loading_mermaid(), height=700)
    
    # Interactive loading simulator
    st.markdown("###  🧮 Data Loading Performance Calculator")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        file_count = st.slider("Number of Files", 1, 100, 10)
        file_size_mb = st.slider("File Size (MB)", 1, 1000, 100)
    
    with col2:
        node_count = st.slider("Redshift Nodes", 1, 32, 4)
        file_format = st.selectbox("File Format", ["CSV", "JSON", "Parquet", "Avro"])
    
    with col3:
        compression = st.selectbox("Compression", ["None", "GZIP", "BZIP2", "LZO"])
        parallel_load = st.checkbox("Parallel Loading", value=True)
    
    # Calculate estimated performance
    total_data_gb = (file_count * file_size_mb) / 1024
    
    # Performance factors
    format_multiplier = {"CSV": 1.0, "JSON": 0.8, "Parquet": 1.5, "Avro": 1.2}
    compression_multiplier = {"None": 1.0, "GZIP": 0.7, "BZIP2": 0.6, "LZO": 0.8}
    
    base_speed = 100  # MB/s per node
    if parallel_load:
        effective_speed = base_speed * node_count * format_multiplier[file_format] * compression_multiplier[compression]
    else:
        effective_speed = base_speed * format_multiplier[file_format] * compression_multiplier[compression]
    
    load_time_seconds = (total_data_gb * 1024) / effective_speed
    load_time_minutes = load_time_seconds / 60
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### ⚡ Performance Estimate
    **Total Data Size**: {total_data_gb:.2f} GB  
    **Effective Load Speed**: {effective_speed:.0f} MB/s  
    **Estimated Load Time**: {load_time_minutes:.1f} minutes  
    **Parallel Processing**: {'✅ Enabled' if parallel_load else '❌ Disabled'}  
    **Optimization**: {file_format} format with {compression} compression
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Best practices
    st.markdown("###  📋 Loading Best Practices")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Optimization Tips
        - **Split large files** into smaller chunks (1-125 MB)
        - **Use compression** to reduce network transfer
        - **Choose columnar formats** like Parquet for analytics
        - **Leverage parallel loading** with multiple files
        - **Use manifest files** for complex loading scenarios
        - **Monitor STL_LOAD_ERRORS** for troubleshooting
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚠️ Common Pitfalls
        - Loading single large files (serialized load)
        - Not using compression for large datasets
        - Incorrect IAM permissions for S3 access
        - Mixed data types causing conversion errors
        - Not handling special characters properly
        - Ignoring load error logs and monitoring
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("###  💻 Code Examples")
    
    tab1, tab2, tab3 = st.tabs(["Basic COPY Command", "Advanced Loading", "Python Automation"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Basic COPY command for CSV files
COPY sales_data 
FROM 's3://my-data-bucket/sales/2024/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
REGION 'us-west-2';

-- Load JSON data with automatic schema detection
COPY user_events 
FROM 's3://my-events-bucket/events/2024/01/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3Role'
FORMAT AS JSON 'auto'
TIMEFORMAT 'YYYY-MM-DD HH:MI:SS'
REGION 'us-west-2';

-- Load Parquet files (columnar format)
COPY product_catalog 
FROM 's3://my-catalog-bucket/products/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3Role'
FORMAT AS PARQUET
REGION 'us-west-2';

-- Check load status
SELECT 
    query, 
    filename, 
    line_number, 
    colname, 
    type, 
    err_reason
FROM stl_load_errors
WHERE query = pg_last_copy_id()
ORDER BY query DESC;
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Advanced COPY with error handling and compression
COPY transaction_data 
FROM 's3://financial-data/transactions/2024/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER '|'
QUOTE '"'
ESCAPE
IGNOREHEADER 1
GZIP
MAXERROR 100
ACCEPTINVCHARS
BLANKSASNULL
EMPTYASNULL
DATEFORMAT 'YYYY-MM-DD'
TIMEFORMAT 'YYYY-MM-DD HH:MI:SS'
REGION 'us-west-2';

-- Using manifest file for complex loading
COPY customer_data 
FROM 's3://my-bucket/manifest/customer_load_manifest.json'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3Role'
FORMAT AS CSV
MANIFEST
REGION 'us-west-2';

-- Manifest file example (customer_load_manifest.json)
/*
{
  "entries": [
    {"url":"s3://my-bucket/customers/part-001.csv", "mandatory":true},
    {"url":"s3://my-bucket/customers/part-002.csv", "mandatory":true},
    {"url":"s3://my-bucket/customers/part-003.csv", "mandatory":false}
  ]
}
*/

-- Load with data conversion and validation
COPY order_details (
    order_id,
    customer_id,
    order_date,
    total_amount,
    status
)
FROM 's3://orders-bucket/order-details/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER ','
NULL AS 'NULL'
ESCAPE
ACCEPTANYDATE
TRUNCATECOLUMNS
REGION 'us-west-2';
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
import psycopg2
import pandas as pd
from datetime import datetime
import logging

class RedshiftDataLoader:
    def __init__(self, cluster_endpoint, database, username, password):
        self.cluster_endpoint = cluster_endpoint
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def connect(self):
        """Establish connection to Redshift"""
        try:
            self.connection = psycopg2.connect(
                host=self.cluster_endpoint,
                port=5439,
                dbname=self.database,
                user=self.username,
                password=self.password
            )
            self.logger.info("Connected to Redshift successfully")
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            raise
    
    def load_from_s3(self, table_name, s3_path, iam_role, **kwargs):
        """Load data from S3 using COPY command"""
        
        # Build COPY command
        copy_options = {
            'format': kwargs.get('format', 'CSV'),
            'delimiter': kwargs.get('delimiter', ','),
            'ignore_header': kwargs.get('ignore_header', 1),
            'region': kwargs.get('region', 'us-west-2'),
            'compression': kwargs.get('compression', None),
            'max_error': kwargs.get('max_error', 0)
        }
        
        copy_sql = f"""
        COPY {table_name} 
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        """
        
        if copy_options['format'] == 'CSV':
            copy_sql += f"FORMAT AS CSV DELIMITER '{copy_options['delimiter']}'"
            if copy_options['ignore_header']:
                copy_sql += f" IGNOREHEADER {copy_options['ignore_header']}"
        elif copy_options['format'] == 'JSON':
            copy_sql += "FORMAT AS JSON 'auto'"
        elif copy_options['format'] == 'PARQUET':
            copy_sql += "FORMAT AS PARQUET"
        
        if copy_options['compression']:
            copy_sql += f" {copy_options['compression']}"
        
        if copy_options['max_error']:
            copy_sql += f" MAXERROR {copy_options['max_error']}"
        
        copy_sql += f" REGION '{copy_options['region']}'"
        
        try:
            cursor = self.connection.cursor()
            
            # Execute COPY command
            start_time = datetime.now()
            cursor.execute(copy_sql)
            self.connection.commit()
            end_time = datetime.now()
            
            # Get load statistics
            cursor.execute("SELECT pg_last_copy_id();")
            copy_id = cursor.fetchone()[0]
            
            cursor.execute(f"""
                SELECT COUNT(*) as rows_loaded 
                FROM stl_load_commits 
                WHERE query = {copy_id}
            """)
            rows_loaded = cursor.fetchone()[0]
            
            # Check for errors
            cursor.execute(f"""
                SELECT COUNT(*) as error_count 
                FROM stl_load_errors 
                WHERE query = {copy_id}
            """)
            error_count = cursor.fetchone()[0]
            
            load_time = (end_time - start_time).total_seconds()
            
            self.logger.info(f"""
            Load completed successfully:
            - Table: {table_name}
            - Rows loaded: {rows_loaded}
            - Load time: {load_time:.2f} seconds
            - Errors: {error_count}
            """)
            
            cursor.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Load failed: {e}")
            self.connection.rollback()
            return False
    
    def get_load_errors(self, copy_id=None):
        """Retrieve load errors for troubleshooting"""
        cursor = self.connection.cursor()
        
        if copy_id:
            error_sql = f"""
                SELECT query, filename, line_number, colname, type, err_reason
                FROM stl_load_errors 
                WHERE query = {copy_id}
                ORDER BY query DESC
            """
        else:
            error_sql = """
                SELECT query, filename, line_number, colname, type, err_reason
                FROM stl_load_errors 
                WHERE query = pg_last_copy_id()
                ORDER BY query DESC
            """
        
        cursor.execute(error_sql)
        errors = cursor.fetchall()
        cursor.close()
        
        return errors
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Connection closed")

# Usage example
def main():
    # Initialize loader
    loader = RedshiftDataLoader(
        cluster_endpoint='my-cluster.abc123.us-west-2.redshift.amazonaws.com',
        database='analytics',
        username='admin',
        password='secure_password'
    )
    
    try:
        # Connect to Redshift
        loader.connect()
        
        # Load sales data
        success = loader.load_from_s3(
            table_name='sales_fact',
            s3_path='s3://my-sales-bucket/sales/2024/',
            iam_role='arn:aws:iam::123456789012:role/RedshiftS3Role',
            format='CSV',
            delimiter=',',
            ignore_header=1,
            compression='GZIP',
            max_error=100
        )
        
        if not success:
            # Check errors
            errors = loader.get_load_errors()
            for error in errors:
                print(f"Error in {error[1]}: {error[5]}")
        
    finally:
        loader.close()

if __name__ == "__main__":
    main()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def s3_lifecycle_tab():
    """Content for S3 Lifecycle tab"""
    st.markdown("## 📦 Amazon S3 Lifecycle")
    st.markdown("*Define rules to transition objects from one storage class to another to save on storage costs*")
    
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    S3 Lifecycle management enables you to define rules that automatically transition objects between 
    storage classes or delete them when they're no longer needed, helping optimize costs based on 
    data access patterns and business requirements.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Lifecycle flow diagram
    st.markdown("###  🔄 S3 Lifecycle Transitions")
    common.mermaid(create_s3_lifecycle_mermaid(), height=400)
    
    # Interactive lifecycle simulator
    st.markdown("###  🧮 Lifecycle Cost Simulator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        object_size_gb = st.slider("Object Size (GB)", 0.1, 1000.0, 10.0, step=0.1)
        object_age_days = st.slider("Object Age (Days)", 0, 3000, 365)
    
    with col2:
        enable_lifecycle = st.checkbox("Enable Lifecycle Policy", value=True)
        intelligent_tiering = st.checkbox("Use Intelligent Tiering", value=False)
    
    # Calculate costs
    if enable_lifecycle and not intelligent_tiering:
        current_tier, monthly_cost = calculate_s3_lifecycle_cost(object_size_gb, object_age_days)
    elif intelligent_tiering:
        current_tier = "Intelligent-Tiering"
        monthly_cost = object_size_gb * 0.0125  # Simplified calculation
    else:
        current_tier = "Standard"
        monthly_cost = object_size_gb * 0.023
    
    # Calculate savings compared to Standard
    standard_cost = object_size_gb * 0.023
    savings = standard_cost - monthly_cost
    savings_percentage = (savings / standard_cost) * 100 if standard_cost > 0 else 0
    
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 💰 Cost Analysis Results
    **Current Storage Tier**: {current_tier}  
    **Monthly Cost**: ${monthly_cost:.4f}  
    **Cost vs Standard**: ${savings:.4f} savings ({savings_percentage:.1f}%)  
    **Annual Savings**: ${savings * 12:.2f}  
    **Object Age**: {object_age_days} days
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Lifecycle stages visualization
    st.markdown("###  📊 Storage Class Comparison")
    
    storage_data = {
        'Storage Class': ['S3 Standard', 'S3 Standard-IA', 'S3 Glacier Flexible', 'S3 Deep Archive'],
        'Cost ($/GB/month)': [0.023, 0.0125, 0.004, 0.00099],
        'Minimum Duration': ['None', '30 days', '90 days', '180 days'],
        'Retrieval Time': ['Milliseconds', 'Milliseconds', '1-5 minutes', '12 hours'],
        'Use Case': ['Active data', 'Infrequent access', 'Archive', 'Long-term archive']
    }
    
    df = pd.DataFrame(storage_data)
    
    # Create cost comparison chart
    fig = px.bar(df, x='Storage Class', y='Cost ($/GB/month)',
                 title='Storage Class Cost Comparison',
                 color='Cost ($/GB/month)',
                 color_continuous_scale=[[0, AWS_COLORS['success']], [1, AWS_COLORS['danger']]])
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_color=AWS_COLORS['secondary']
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Lifecycle policy examples
    st.markdown("###  📋 Lifecycle Policy Examples")
    
    tab1, tab2, tab3 = st.tabs(["Basic Policy", "Advanced Policy", "Policy Management"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
{
    "Rules": [
        {
            "ID": "BasicLifecycleRule",
            "Status": "Enabled",
            "Filter": {
                "Prefix": "documents/"
            },
            "Transitions": [
                {
                    "Days": 30,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "Days": 90,
                    "StorageClass": "GLACIER"
                },
                {
                    "Days": 365,
                    "StorageClass": "DEEP_ARCHIVE"
                }
            ],
            "Expiration": {
                "Days": 2555
            }
        }
    ]
}

# Apply lifecycle policy using AWS CLI
aws s3api put-bucket-lifecycle-configuration \
    --bucket my-storage-bucket \
    --lifecycle-configuration file://lifecycle-policy.json

# Python Boto3 - Apply lifecycle policy
import boto3

s3_client = boto3.client('s3')

lifecycle_config = {
    'Rules': [
        {
            'ID': 'DocumentArchiving',
            'Status': 'Enabled',
            'Filter': {'Prefix': 'documents/'},
            'Transitions': [
                {'Days': 30, 'StorageClass': 'STANDARD_IA'},
                {'Days': 90, 'StorageClass': 'GLACIER'},
                {'Days': 365, 'StorageClass': 'DEEP_ARCHIVE'}
            ],
            'Expiration': {'Days': 2555}
        }
    ]
}

response = s3_client.put_bucket_lifecycle_configuration(
    Bucket='my-storage-bucket',
    LifecycleConfiguration=lifecycle_config
)

print("Lifecycle policy applied successfully!")
        ''', language='json')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
{
    "Rules": [
        {
            "ID": "LogsArchival",
            "Status": "Enabled",
            "Filter": {
                "And": {
                    "Prefix": "logs/",
                    "Tags": [
                        {
                            "Key": "Archive",
                            "Value": "true"
                        }
                    ]
                }
            },
            "Transitions": [
                {
                    "Days": 1,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "Days": 7,
                    "StorageClass": "GLACIER"
                }
            ],
            "Expiration": {
                "Days": 90
            }
        },
        {
            "ID": "IntelligentTiering",
            "Status": "Enabled", 
            "Filter": {
                "Prefix": "analytics/"
            },
            "Transitions": [
                {
                    "Days": 0,
                    "StorageClass": "INTELLIGENT_TIERING"
                }
            ]
        },
        {
            "ID": "IncompleteMultipartUploads",
            "Status": "Enabled",
            "Filter": {},
            "AbortIncompleteMultipartUpload": {
                "DaysAfterInitiation": 7
            }
        },
        {
            "ID": "OldVersionCleanup",
            "Status": "Enabled",
            "Filter": {},
            "NoncurrentVersionTransitions": [
                {
                    "NoncurrentDays": 30,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "NoncurrentDays": 90,
                    "StorageClass": "GLACIER"
                }
            ],
            "NoncurrentVersionExpiration": {
                "NoncurrentDays": 365
            }
        }
    ]
}

# Python script for complex lifecycle management
import boto3
import json
from datetime import datetime, timedelta

class S3LifecycleManager:
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def create_tiered_policy(self, bucket_name, prefix, 
                           ia_days=30, glacier_days=90, 
                           deep_archive_days=365, expiry_days=2555):
        """Create a comprehensive tiered storage policy"""
        
        policy = {
            'Rules': [
                {
                    'ID': f'TieredStorage-{prefix}',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': prefix},
                    'Transitions': [
                        {'Days': ia_days, 'StorageClass': 'STANDARD_IA'},
                        {'Days': glacier_days, 'StorageClass': 'GLACIER'},
                        {'Days': deep_archive_days, 'StorageClass': 'DEEP_ARCHIVE'}
                    ],
                    'Expiration': {'Days': expiry_days}
                }
            ]
        }
        
        try:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=policy
            )
            return True
        except Exception as e:
            print(f"Error applying policy: {e}")
            return False
    
    def get_cost_savings_estimate(self, bucket_name, prefix=""):
        """Estimate cost savings from lifecycle policies"""
        
        # Get objects with specified prefix
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        total_size = 0
        object_ages = []
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_size += obj['Size']
                    age_days = (datetime.now(obj['LastModified'].tzinfo) - obj['LastModified']).days
                    object_ages.append(age_days)
        
        # Calculate potential savings
        total_gb = total_size / (1024**3)
        
        # Current cost (assuming all in Standard)
        current_monthly_cost = total_gb * 0.023
        
        # Optimized cost with lifecycle
        optimized_cost = 0
        for age in object_ages:
            obj_gb = total_gb / len(object_ages)  # Average per object
            
            if age < 30:
                optimized_cost += obj_gb * 0.023
            elif age < 90:
                optimized_cost += obj_gb * 0.0125
            elif age < 365:
                optimized_cost += obj_gb * 0.004
            else:
                optimized_cost += obj_gb * 0.00099
        
        savings = current_monthly_cost - optimized_cost
        
        return {
            'total_size_gb': total_gb,
            'current_monthly_cost': current_monthly_cost,
            'optimized_monthly_cost': optimized_cost,
            'monthly_savings': savings,
            'annual_savings': savings * 12
        }

# Usage example
manager = S3LifecycleManager()
bucket = 'my-data-bucket'

# Apply tiered storage policy
manager.create_tiered_policy(
    bucket_name=bucket,
    prefix='analytics/',
    ia_days=7,      # Quick transition for analytics data
    glacier_days=30,
    deep_archive_days=365
)

# Get cost estimate
savings = manager.get_cost_savings_estimate(bucket, 'analytics/')
print(f"Potential annual savings: ${savings['annual_savings']:.2f}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Lifecycle policy management and monitoring

# View current lifecycle configuration
aws s3api get-bucket-lifecycle-configuration --bucket my-bucket

# Delete lifecycle configuration
aws s3api delete-bucket-lifecycle --bucket my-bucket

# Python - Monitor lifecycle transitions
import boto3
import pandas as pd
from datetime import datetime, timedelta

def monitor_lifecycle_transitions(bucket_name):
    """Monitor objects transitioning between storage classes"""
    
    s3_client = boto3.client('s3')
    cloudtrail_client = boto3.client('cloudtrail')
    
    # Get lifecycle events from CloudTrail
    end_time = datetime.now()
    start_time = end_time - timedelta(days=30)
    
    events = cloudtrail_client.lookup_events(
        LookupAttributes=[
            {
                'AttributeKey': 'EventName',
                'AttributeValue': 'LifecycleTransition'
            }
        ],
        StartTime=start_time,
        EndTime=end_time
    )
    
    transitions = []
    for event in events['Events']:
        if bucket_name in event.get('ResourceName', ''):
            transitions.append({
                'timestamp': event['EventTime'],
                'object_key': event.get('ResourceName', ''),
                'event_name': event['EventName'],
                'user': event.get('Username', 'System')
            })
    
    return pd.DataFrame(transitions)

def analyze_storage_distribution(bucket_name):
    """Analyze current storage class distribution"""
    
    s3_client = boto3.client('s3')
    
    storage_classes = {}
    total_size = 0
    
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket_name):
        if 'Contents' in page:
            for obj in page['Contents']:
                storage_class = obj.get('StorageClass', 'STANDARD')
                size = obj['Size']
                
                if storage_class not in storage_classes:
                    storage_classes[storage_class] = {'count': 0, 'size': 0}
                
                storage_classes[storage_class]['count'] += 1
                storage_classes[storage_class]['size'] += size
                total_size += size
    
    # Calculate costs and percentages
    cost_per_gb = {
        'STANDARD': 0.023,
        'STANDARD_IA': 0.0125,
        'GLACIER': 0.004,
        'DEEP_ARCHIVE': 0.00099,
        'INTELLIGENT_TIERING': 0.0125
    }
    
    analysis = []
    for storage_class, data in storage_classes.items():
        size_gb = data['size'] / (1024**3)
        percentage = (data['size'] / total_size) * 100
        monthly_cost = size_gb * cost_per_gb.get(storage_class, 0.023)
        
        analysis.append({
            'storage_class': storage_class,
            'object_count': data['count'],
            'size_gb': size_gb,
            'percentage': percentage,
            'monthly_cost': monthly_cost
        })
    
    return pd.DataFrame(analysis)

def optimize_lifecycle_policy(bucket_name, cost_target_reduction=50):
    """Suggest optimizations for existing lifecycle policy"""
    
    analysis = analyze_storage_distribution(bucket_name)
    
    recommendations = []
    
    # Check if too much data is in expensive tiers
    standard_percentage = analysis[analysis['storage_class'] == 'STANDARD']['percentage'].sum()
    
    if standard_percentage > 30:
        recommendations.append({
            'type': 'transition_optimization',
            'message': f'{standard_percentage:.1f}% of data is in Standard storage. Consider shorter transition periods.',
            'action': 'Reduce Standard-IA transition from 30 to 7-14 days'
        })
    
    # Check for missing Intelligent Tiering
    if 'INTELLIGENT_TIERING' not in analysis['storage_class'].values:
        recommendations.append({
            'type': 'intelligent_tiering',
            'message': 'Consider Intelligent Tiering for unpredictable access patterns',
            'action': 'Add Intelligent Tiering rule for specific prefixes'
        })
    
    # Check Deep Archive usage
    deep_archive_percentage = analysis[analysis['storage_class'] == 'DEEP_ARCHIVE']['percentage'].sum()
    
    if deep_archive_percentage < 20:
        recommendations.append({
            'type': 'deep_archive_optimization',
            'message': 'Low Deep Archive usage suggests opportunity for more aggressive archiving',
            'action': 'Reduce Deep Archive transition period for old data'
        })
    
    return recommendations

# Usage examples
bucket = 'my-production-bucket'

# Monitor recent transitions
transitions_df = monitor_lifecycle_transitions(bucket)
print("Recent lifecycle transitions:")
print(transitions_df.head())

# Analyze current distribution  
distribution_df = analyze_storage_distribution(bucket)
print("\nStorage class distribution:")
print(distribution_df)

# Get optimization recommendations
recommendations = optimize_lifecycle_policy(bucket)
for rec in recommendations:
    print(f"\n{rec['type'].upper()}: {rec['message']}")
    print(f"Action: {rec['action']}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def s3_expiring_objects_tab():
    """Content for S3 Lifecycle Expiring Objects tab"""
    st.markdown("## ⏰ S3 Lifecycle – Expiring Objects")
    st.markdown("*S3 Lifecycle configurations allow objects to be given a set expiry (deletion)*")
    
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    S3 Lifecycle expiration rules automatically delete objects after a specified time period, 
    helping manage storage costs and comply with data retention policies. Understanding minimum 
    storage duration charges is crucial for cost optimization.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Minimum storage duration charges
    st.markdown("###  ⚠️ Minimum Storage Duration Charges")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="cost-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Standard-IA & One Zone-IA
        **Minimum Duration**: 30 days  
        **Early Deletion**: Charged for full 30 days  
        **Use Case**: Data kept at least 30 days  
        **Cost Impact**: Significant for short-lived objects
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="cost-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🧊 Glacier & Deep Archive
        **Glacier Flexible**: 90 days minimum  
        **Deep Archive**: 180 days minimum  
        **Early Deletion**: Full minimum period charge  
        **Planning**: Ensure data won't be deleted early
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive expiration calculator
    st.markdown("###  🧮 Expiration Cost Calculator")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        object_size_gb = st.slider("Object Size (GB)", 0.1, 100.0, 1.0, step=0.1)
        storage_class = st.selectbox("Storage Class", 
                                   ["STANDARD", "STANDARD_IA", "ONEZONE_IA", 
                                    "GLACIER", "DEEP_ARCHIVE"])
    
    with col2:
        days_stored = st.slider("Days Stored", 1, 365, 15)
        expiration_enabled = st.checkbox("Expiration Rule Enabled", value=True)
    
    with col3:
        early_deletion = st.checkbox("Early Deletion Scenario", value=False)
    
    # Calculate costs
    storage_costs = {
        "STANDARD": 0.023,
        "STANDARD_IA": 0.0125,
        "ONEZONE_IA": 0.01,
        "GLACIER": 0.004,
        "DEEP_ARCHIVE": 0.00099
    }
    
    minimum_days = {
        "STANDARD": 0,
        "STANDARD_IA": 30,
        "ONEZONE_IA": 30,
        "GLACIER": 90,
        "DEEP_ARCHIVE": 180
    }
    
    # Calculate actual cost
    daily_cost = (storage_costs[storage_class] * object_size_gb) / 30
    
    if early_deletion and days_stored < minimum_days[storage_class]:
        # Charge for minimum duration
        charged_days = minimum_days[storage_class]
        total_cost = daily_cost * charged_days
        early_deletion_fee = daily_cost * (minimum_days[storage_class] - days_stored)
    else:
        charged_days = days_stored
        total_cost = daily_cost * days_stored
        early_deletion_fee = 0
    
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 💰 Cost Calculation Results
    **Storage Class**: {storage_class}  
    **Object Size**: {object_size_gb} GB  
    **Days Stored**: {days_stored}  
    **Days Charged**: {charged_days}  
    **Total Cost**: ${total_cost:.4f}  
    **Early Deletion Fee**: ${early_deletion_fee:.4f}  
    **Minimum Duration**: {minimum_days[storage_class]} days
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Expiration strategies
    st.markdown("###  📋 Expiration Strategies")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Best Practices
        - **Plan retention periods** carefully before storage class selection
        - **Use Standard** for data with uncertain retention periods
        - **Set appropriate expiration** based on business requirements
        - **Consider legal requirements** for data retention
        - **Monitor lifecycle events** for compliance tracking
        - **Test policies** in non-production environments first
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚠️ Common Mistakes
        - Moving short-lived data to IA storage classes
        - Not accounting for minimum storage duration charges
        - Setting expiration shorter than minimum duration
        - Forgetting about compliance and legal requirements
        - Not testing expiration rules before production
        - Ignoring versioning implications with expiration
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("###  💻 Expiration Policy Examples")
    
    tab1, tab2, tab3 = st.tabs(["Basic Expiration", "Conditional Expiration", "Monitoring & Alerts"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Basic expiration lifecycle policy
{
    "Rules": [
        {
            "ID": "LogExpiration",
            "Status": "Enabled",
            "Filter": {
                "Prefix": "logs/"
            },
            "Expiration": {
                "Days": 90
            }
        },
        {
            "ID": "TempDataExpiration", 
            "Status": "Enabled",
            "Filter": {
                "Prefix": "temp/"
            },
            "Expiration": {
                "Days": 7
            }
        },
        {
            "ID": "BackupExpiration",
            "Status": "Enabled",
            "Filter": {
                "And": {
                    "Prefix": "backups/",
                    "Tags": [
                        {
                            "Key": "Type",
                            "Value": "Weekly"
                        }
                    ]
                }
            },
            "Transitions": [
                {
                    "Days": 30,
                    "StorageClass": "GLACIER"
                }
            ],
            "Expiration": {
                "Days": 365
            }
        }
    ]
}

# Python - Apply expiration policy
import boto3

def apply_expiration_policy(bucket_name):
    s3_client = boto3.client('s3')
    
    lifecycle_config = {
        'Rules': [
            {
                'ID': 'AutoExpireLogs',
                'Status': 'Enabled',
                'Filter': {'Prefix': 'logs/'},
                'Expiration': {'Days': 90}
            },
            {
                'ID': 'AutoExpireTemp',
                'Status': 'Enabled', 
                'Filter': {'Prefix': 'temp/'},
                'Expiration': {'Days': 1}
            }
        ]
    }
    
    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
        print(f"Expiration policy applied to {bucket_name}")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

# Usage
apply_expiration_policy('my-application-bucket')
        ''', language='json')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Advanced expiration with conditions and tags
{
    "Rules": [
        {
            "ID": "ConditionalExpiration",
            "Status": "Enabled",
            "Filter": {
                "And": {
                    "Prefix": "documents/",
                    "Tags": [
                        {
                            "Key": "RetentionPeriod",
                            "Value": "Short"
                        },
                        {
                            "Key": "Department", 
                            "Value": "Marketing"
                        }
                    ]
                }
            },
            "Expiration": {
                "Days": 30
            }
        },
        {
            "ID": "VersionedObjectExpiration",
            "Status": "Enabled",
            "Filter": {},
            "NoncurrentVersionExpiration": {
                "NoncurrentDays": 30,
                "NewerNoncurrentVersions": 3
            },
            "AbortIncompleteMultipartUpload": {
                "DaysAfterInitiation": 7
            }
        },
        {
            "ID": "ExpiredDeleteMarkerCleanup",
            "Status": "Enabled",
            "Filter": {},
            "Expiration": {
                "ExpiredObjectDeleteMarker": true
            }
        }
    ]
}

# Python - Dynamic expiration based on object metadata
import boto3
from datetime import datetime, timedelta

class DynamicExpirationManager:
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def set_expiration_by_content_type(self, bucket_name):
        """Set different expiration periods based on content type"""
        
        # Define expiration rules by content type
        content_type_rules = {
            'image/': 180,      # Images: 6 months
            'video/': 90,       # Videos: 3 months  
            'text/': 30,        # Text files: 1 month
            'application/log': 7 # Log files: 1 week
        }
        
        rules = []
        
        for content_type, days in content_type_rules.items():
            rule = {
                'ID': f'Expire-{content_type.replace("/", "-")}',
                'Status': 'Enabled',
                'Filter': {
                    'Tag': {
                        'Key': 'ContentType',
                        'Value': content_type
                    }
                },
                'Expiration': {'Days': days}
            }
            rules.append(rule)
        
        lifecycle_config = {'Rules': rules}
        
        try:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=lifecycle_config
            )
            return True
        except Exception as e:
            print(f"Error applying dynamic expiration: {e}")
            return False
    
    def schedule_object_expiration(self, bucket_name, object_key, days_from_now):
        """Schedule specific object for expiration"""
        
        expiration_date = datetime.now() + timedelta(days=days_from_now)
        
        # Use object tagging to mark for expiration
        self.s3_client.put_object_tagging(
            Bucket=bucket_name,
            Key=object_key,
            Tagging={
                'TagSet': [
                    {
                        'Key': 'ScheduledExpiration',
                        'Value': expiration_date.strftime('%Y-%m-%d')
                    },
                    {
                        'Key': 'ExpirationDays',
                        'Value': str(days_from_now)
                    }
                ]
            }
        )
        
        print(f"Object {object_key} scheduled for expiration in {days_from_now} days")
    
    def create_compliance_expiration_policy(self, bucket_name, retention_years=7):
        """Create compliance-focused expiration policy"""
        
        retention_days = retention_years * 365
        
        compliance_policy = {
            'Rules': [
                {
                    'ID': 'ComplianceRetention',
                    'Status': 'Enabled',
                    'Filter': {
                        'Tag': {
                            'Key': 'ComplianceData',
                            'Value': 'true'
                        }
                    },
                    'Transitions': [
                        {'Days': 90, 'StorageClass': 'GLACIER'},
                        {'Days': 365, 'StorageClass': 'DEEP_ARCHIVE'}
                    ],
                    'Expiration': {'Days': retention_days}
                },
                {
                    'ID': 'NonComplianceQuickExpiry',
                    'Status': 'Enabled',
                    'Filter': {
                        'Tag': {
                            'Key': 'ComplianceData',
                            'Value': 'false'
                        }
                    },
                    'Expiration': {'Days': 30}
                }
            ]
        }
        
        try:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=compliance_policy
            )
            print(f"Compliance expiration policy applied with {retention_years} year retention")
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False

# Usage examples
manager = DynamicExpirationManager()

# Apply content-type based expiration
manager.set_expiration_by_content_type('my-media-bucket')

# Schedule specific object expiration
manager.schedule_object_expiration('my-bucket', 'reports/quarterly-report.pdf', 90)

# Apply compliance policy
manager.create_compliance_expiration_policy('compliance-bucket', retention_years=10)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Monitor and alert on object expiration events
import boto3
import json
from datetime import datetime, timedelta

class ExpirationMonitor:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.cloudwatch_client = boto3.client('cloudwatch')
        self.sns_client = boto3.client('sns')
    
    def setup_expiration_notifications(self, bucket_name, sns_topic_arn):
        """Set up S3 event notifications for object expiration"""
        
        notification_config = {
            'TopicConfigurations': [
                {
                    'Id': 'ExpirationNotification',
                    'TopicArn': sns_topic_arn,
                    'Events': [
                        's3:ObjectRemoved:Delete',
                        's3:ObjectRemoved:DeleteMarkerCreated'
                    ]
                }
            ]
        }
        
        try:
            self.s3_client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=notification_config
            )
            print(f"Expiration notifications configured for {bucket_name}")
            return True
        except Exception as e:
            print(f"Error setting up notifications: {e}")
            return False
    
    def get_objects_expiring_soon(self, bucket_name, days_ahead=7):
        """Find objects that will expire within specified days"""
        
        # Get lifecycle configuration
        try:
            lifecycle = self.s3_client.get_bucket_lifecycle_configuration(
                Bucket=bucket_name
            )
        except:
            print("No lifecycle configuration found")
            return []
        
        expiring_objects = []
        
        # List all objects
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                obj_key = obj['Key']
                obj_date = obj['LastModified']
                obj_age = (datetime.now(obj_date.tzinfo) - obj_date).days
                
                # Check against lifecycle rules
                for rule in lifecycle['Rules']:
                    if self._object_matches_rule(obj_key, rule):
                        if 'Expiration' in rule:
                            exp_days = rule['Expiration'].get('Days', 0)
                            days_until_expiry = exp_days - obj_age
                            
                            if 0 < days_until_expiry <= days_ahead:
                                expiring_objects.append({
                                    'key': obj_key,
                                    'age_days': obj_age,
                                    'expires_in_days': days_until_expiry,
                                    'rule_id': rule['ID']
                                })
        
        return expiring_objects
    
    def _object_matches_rule(self, object_key, rule):
        """Check if object matches lifecycle rule filter"""
        rule_filter = rule.get('Filter', {})
        
        # Simple prefix matching
        if 'Prefix' in rule_filter:
            return object_key.startswith(rule_filter['Prefix'])
        
        # If no filter, applies to all objects
        if not rule_filter:
            return True
        
        return False
    
    def create_expiration_report(self, bucket_name):
        """Generate comprehensive expiration report"""
        
        expiring_soon = self.get_objects_expiring_soon(bucket_name, 30)
        
        report = {
            'bucket': bucket_name,
            'report_date': datetime.now().isoformat(),
            'total_objects_expiring': len(expiring_soon),
            'breakdown': {}
        }
        
        # Breakdown by time periods
        time_periods = {'1-7 days': 0, '8-14 days': 0, '15-30 days': 0}
        
        for obj in expiring_soon:
            days = obj['expires_in_days']
            if days <= 7:
                time_periods['1-7 days'] += 1
            elif days <= 14:
                time_periods['8-14 days'] += 1
            else:
                time_periods['15-30 days'] += 1
        
        report['breakdown'] = time_periods
        report['objects'] = expiring_soon[:100]  # Limit for readability
        
        return report
    
    def send_expiration_alert(self, bucket_name, sns_topic_arn):
        """Send expiration alert via SNS"""
        
        expiring_objects = self.get_objects_expiring_soon(bucket_name, 7)
        
        if not expiring_objects:
            return
        
        message = f"""
        S3 Expiration Alert - {bucket_name}
        
        {len(expiring_objects)} objects will expire within 7 days:
        
        """
        
        for obj in expiring_objects[:10]:  # Show first 10
            message += f"- {obj['key']} (expires in {obj['expires_in_days']} days)\n"
        
        if len(expiring_objects) > 10:
            message += f"\n... and {len(expiring_objects) - 10} more objects"
        
        try:
            self.sns_client.publish(
                TopicArn=sns_topic_arn,
                Subject=f'S3 Expiration Alert - {bucket_name}',
                Message=message
            )
            print("Expiration alert sent successfully")
        except Exception as e:
            print(f"Error sending alert: {e}")

# Usage examples
monitor = ExpirationMonitor()

# Set up notifications
sns_topic = 'arn:aws:sns:us-west-2:123456789012:s3-expiration-alerts'
monitor.setup_expiration_notifications('my-bucket', sns_topic)

# Get expiration report
report = monitor.create_expiration_report('my-bucket')
print(json.dumps(report, indent=2))

# Send alert for objects expiring soon
monitor.send_expiration_alert('my-bucket', sns_topic)

# Create CloudWatch custom metric for expiring objects
def publish_expiration_metrics(bucket_name):
    monitor = ExpirationMonitor()
    cloudwatch = boto3.client('cloudwatch')
    
    expiring_objects = monitor.get_objects_expiring_soon(bucket_name, 7)
    
    cloudwatch.put_metric_data(
        Namespace='S3/Lifecycle',
        MetricData=[
            {
                'MetricName': 'ObjectsExpiringIn7Days',
                'Dimensions': [
                    {
                        'Name': 'BucketName',
                        'Value': bucket_name
                    }
                ],
                'Value': len(expiring_objects),
                'Unit': 'Count'
            }
        ]
    )

# Schedule this to run daily
publish_expiration_metrics('my-production-bucket')
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def glacier_cost_tab():
    """Content for S3 Glacier cost considerations tab"""
    st.markdown("## 🧊 S3 Glacier Classes – Cost considerations")
    st.markdown("*Understanding storage overhead, retention periods, and retrieval costs for cost optimization*")
    
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    S3 Glacier storage classes have several cost considerations beyond basic storage fees: 
    storage overhead charges, minimum retention periods, transition fees, and retrieval costs. 
    Understanding these is crucial for accurate cost planning.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Cost factors overview
    st.markdown("###  💰 Four Key Cost Factors")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="cost-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Storage Overhead
        **Per Object Charges:**
        - 8KB for name and metadata
        - 32KB for index metadata  
        - **Total**: 40KB overhead per object
        
        **Impact**: Small objects become expensive
        **Solution**: Aggregate smaller objects
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="cost-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Transition Requests  
        **Per Transition Fees:**
        - Each object moved incurs cost
        - Applies to ALL objects
        
        **Optimization**: Filter objects <128KB
        **Best Practice**: Batch transitions
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="cost-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⏰ Minimum Duration
        **Archive Minimums:**
        - Glacier Flexible: 90 days  
        - Deep Archive: 180 days
        
        **Early Deletion**: Full period charged
        **Planning**: Ensure true archival intent
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="cost-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔓 Data Restoration
        **Retrieval Costs:**
        - Per GB retrieval fees
        - Speed affects pricing
        
        **Duration**: Temporarily restored
        **Action**: Copy to Standard for permanent access
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive cost calculator
    st.markdown("###  🧮 Glacier Cost Calculator")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Object Characteristics**")
        num_objects = st.number_input("Number of Objects", min_value=1, max_value=1000000, value=1000)
        avg_object_size_mb = st.slider("Average Object Size (MB)", 0.1, 1000.0, 10.0, step=0.1)
    
    with col2:
        st.markdown("**Storage Settings**")
        glacier_type = st.selectbox("Glacier Type", ["Glacier Flexible", "Glacier Deep Archive"])
        storage_months = st.slider("Storage Duration (Months)", 1, 60, 12)
    
    with col3:
        st.markdown("**Retrieval Settings**")
        retrieval_percentage = st.slider("Data Retrieved (%)", 0, 100, 10)
        retrieval_speed = st.selectbox("Retrieval Speed", ["Standard", "Expedited", "Bulk"])
    
    # Cost calculations
    total_data_gb = (num_objects * avg_object_size_mb) / 1024
    overhead_gb = (num_objects * 40) / (1024**3)  # 40KB per object in GB
    
    # Storage costs
    if glacier_type == "Glacier Flexible":
        storage_rate = 0.004
        minimum_months = 3
    else:  # Deep Archive
        storage_rate = 0.00099
        minimum_months = 6
    
    # Calculate effective storage months (minimum duration consideration)
    effective_months = max(storage_months, minimum_months)
    
    storage_cost = (total_data_gb + overhead_gb) * storage_rate * effective_months
    
    # Transition costs (approximate)
    transition_cost = num_objects * 0.0004  # $0.0004 per 1,000 requests
    
    # Retrieval costs
    retrieval_gb = total_data_gb * (retrieval_percentage / 100)
    retrieval_rates = {
        "Glacier Flexible": {"Standard": 0.01, "Expedited": 0.03, "Bulk": 0.0025},
        "Glacier Deep Archive": {"Standard": 0.02, "Expedited": 0.10, "Bulk": 0.005}
    }
    
    retrieval_cost = retrieval_gb * retrieval_rates[glacier_type][retrieval_speed]
    
    total_cost = storage_cost + transition_cost + retrieval_cost
    
    # Comparison with Standard storage
    standard_cost = total_data_gb * 0.023 * effective_months
    savings = standard_cost - total_cost
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 💰 Cost Breakdown Analysis
    **Total Data**: {total_data_gb:.2f} GB + {overhead_gb:.4f} GB overhead  
    **Storage Cost**: ${storage_cost:.2f} ({effective_months} months)  
    **Transition Cost**: ${transition_cost:.2f}  
    **Retrieval Cost**: ${retrieval_cost:.2f}  
    **Total Glacier Cost**: ${total_cost:.2f}  
    **vs Standard Storage**: ${savings:.2f} savings ({((savings/standard_cost)*100):.1f}%)
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Cost optimization strategies
    st.markdown("###  📈 Cost Optimization Strategies")
    
    # Create visualization of cost factors
    cost_data = {
        'Cost Component': ['Storage', 'Overhead', 'Transitions', 'Retrievals'],
        'Amount': [storage_cost, overhead_gb * storage_rate * effective_months, 
                  transition_cost, retrieval_cost],
        'Percentage': [
            (storage_cost/total_cost)*100,
            ((overhead_gb * storage_rate * effective_months)/total_cost)*100,
            (transition_cost/total_cost)*100,
            (retrieval_cost/total_cost)*100
        ]
    }
    
    df_costs = pd.DataFrame(cost_data)
    
    fig = px.pie(df_costs, values='Amount', names='Cost Component',
                 title='Glacier Cost Breakdown',
                 color_discrete_sequence=[AWS_COLORS['primary'], AWS_COLORS['light_blue'], 
                                        AWS_COLORS['success'], AWS_COLORS['secondary']])
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Optimization recommendations
    st.markdown("###  🎯 Optimization Recommendations")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Best Practices
        - **Aggregate small files** before archiving (use TAR, ZIP)
        - **Filter lifecycle rules** to exclude objects <128KB
        - **Plan retention carefully** to avoid early deletion fees
        - **Use Bulk retrievals** for non-urgent data access
        - **Monitor retrieval patterns** to optimize storage class selection
        - **Consider Intelligent Tiering** for unpredictable access patterns
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚠️ Cost Traps to Avoid
        - Archiving many small objects without aggregation
        - Setting lifecycle transitions too aggressively
        - Not accounting for minimum storage duration
        - Frequent retrievals from Deep Archive
        - Using Expedited retrievals unnecessarily
        - Ignoring overhead costs in calculations
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("###  💻 Cost Optimization Code")
    
    tab1, tab2 = st.tabs(["Object Aggregation", "Cost Analysis Tools"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
import tarfile
import io
from datetime import datetime

class GlacierOptimizer:
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def analyze_object_sizes(self, bucket_name, prefix=""):
        """Analyze object sizes to identify aggregation opportunities"""
        
        size_buckets = {
            'tiny': [],      # < 1KB
            'small': [],     # 1KB - 128KB  
            'medium': [],    # 128KB - 1MB
            'large': []      # > 1MB
        }
        
        total_objects = 0
        total_size = 0
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                size = obj['Size']
                key = obj['Key']
                total_objects += 1
                total_size += size
                
                if size < 1024:
                    size_buckets['tiny'].append({'key': key, 'size': size})
                elif size < 128 * 1024:
                    size_buckets['small'].append({'key': key, 'size': size})
                elif size < 1024 * 1024:
                    size_buckets['medium'].append({'key': key, 'size': size})
                else:
                    size_buckets['large'].append({'key': key, 'size': size})
        
        # Calculate potential savings
        small_objects = len(size_buckets['tiny']) + len(size_buckets['small'])
        overhead_cost_current = small_objects * 40 * 1024 * 0.004 / (1024**3) * 12  # Annual
        
        # If aggregated into 1MB chunks
        aggregated_objects = (small_objects * 64 * 1024) // (1024 * 1024)  # Assume avg 64KB
        overhead_cost_optimized = aggregated_objects * 40 * 1024 * 0.004 / (1024**3) * 12
        
        potential_savings = overhead_cost_current - overhead_cost_optimized
        
        return {
            'analysis': size_buckets,
            'total_objects': total_objects,
            'total_size_gb': total_size / (1024**3),
            'small_objects_count': small_objects,
            'potential_annual_savings': potential_savings,
            'recommendation': 'Consider aggregating small objects' if small_objects > 100 else 'Current sizing is optimal'
        }
    
    def create_aggregated_archive(self, bucket_name, object_keys, archive_name):
        """Aggregate multiple small objects into a single archive"""
        
        # Create tar archive in memory
        tar_buffer = io.BytesIO()
        
        with tarfile.open(fileobj=tar_buffer, mode='w:gz') as tar:
            for key in object_keys:
                try:
                    # Download object
                    response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
                    object_data = response['Body'].read()
                    
                    # Add to archive
                    tarinfo = tarfile.TarInfo(name=key)
                    tarinfo.size = len(object_data)
                    tarinfo.mtime = datetime.now().timestamp()
                    
                    tar.addfile(tarinfo, io.BytesIO(object_data))
                    
                except Exception as e:
                    print(f"Error processing {key}: {e}")
                    continue
        
        # Upload aggregated archive
        tar_buffer.seek(0)
        
        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=archive_name,
            Body=tar_buffer.getvalue(),
            Metadata={
                'aggregated-objects': str(len(object_keys)),
                'created-date': datetime.now().isoformat()
            }
        )
        
        print(f"Created aggregated archive: {archive_name}")
        print(f"Contains {len(object_keys)} objects")
        
        return archive_name
    
    def optimize_lifecycle_for_cost(self, bucket_name):
        """Create cost-optimized lifecycle policy"""
        
        # Analyze current objects
        analysis = self.analyze_object_sizes(bucket_name)
        
        # Create policy that excludes small objects from expensive transitions
        lifecycle_rules = []
        
        # Rule for larger objects (cost-effective for Glacier)
        large_object_rule = {
            'ID': 'LargeObjectArchiving',
            'Status': 'Enabled',
            'Filter': {
                'And': {
                    'ObjectSizeGreaterThan': 128 * 1024,  # 128KB minimum
                    'Prefix': ''
                }
            },
            'Transitions': [
                {'Days': 30, 'StorageClass': 'STANDARD_IA'},
                {'Days': 90, 'StorageClass': 'GLACIER'},
                {'Days': 365, 'StorageClass': 'DEEP_ARCHIVE'}
            ]
        }
        lifecycle_rules.append(large_object_rule)
        
        # Rule for small objects (keep in Standard/IA only)
        small_object_rule = {
            'ID': 'SmallObjectOptimization',
            'Status': 'Enabled',
            'Filter': {
                'ObjectSizeLessThan': 128 * 1024  # Less than 128KB
            },
            'Transitions': [
                {'Days': 30, 'StorageClass': 'STANDARD_IA'}
                # No Glacier transition for small objects
            ],
            'Expiration': {
                'Days': 365  # Delete after 1 year to avoid long-term small object costs
            }
        }
        lifecycle_rules.append(small_object_rule)
        
        # Apply optimized policy
        try:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration={'Rules': lifecycle_rules}
            )
            print("Cost-optimized lifecycle policy applied")
            return True
        except Exception as e:
            print(f"Error applying lifecycle policy: {e}")
            return False

# Usage examples
optimizer = GlacierOptimizer()

# Analyze bucket for optimization opportunities
analysis = optimizer.analyze_object_sizes('my-archive-bucket')
print(f"Analysis results: {analysis['recommendation']}")
print(f"Potential annual savings: ${analysis['potential_annual_savings']:.2f}")

# Aggregate small objects
small_objects = [obj['key'] for obj in analysis['analysis']['small'][:100]]  # First 100
if small_objects:
    archive_name = f"aggregated/small-objects-{datetime.now().strftime('%Y%m%d')}.tar.gz"
    optimizer.create_aggregated_archive('my-archive-bucket', small_objects, archive_name)

# Apply cost-optimized lifecycle policy
optimizer.optimize_lifecycle_for_cost('my-archive-bucket')
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
import pandas as pd
from datetime import datetime, timedelta

class GlacierCostAnalyzer:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.cloudwatch_client = boto3.client('cloudwatch')
    
    def calculate_total_glacier_cost(self, bucket_name, months=12):
        """Calculate comprehensive Glacier storage costs"""
        
        costs = {
            'storage': 0,
            'overhead': 0,
            'transitions': 0,
            'retrievals': 0
        }
        
        object_counts = {'GLACIER': 0, 'DEEP_ARCHIVE': 0, 'STANDARD': 0}
        
        # Analyze current objects
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                storage_class = obj.get('StorageClass', 'STANDARD')
                size_gb = obj['Size'] / (1024**3)
                
                object_counts[storage_class] = object_counts.get(storage_class, 0) + 1
                
                # Storage costs
                if storage_class == 'GLACIER':
                    costs['storage'] += size_gb * 0.004 * months
                    costs['overhead'] += (40 * 1024 / (1024**3)) * 0.004 * months
                elif storage_class == 'DEEP_ARCHIVE':
                    costs['storage'] += size_gb * 0.00099 * months
                    costs['overhead'] += (40 * 1024 / (1024**3)) * 0.00099 * months
        
        # Estimate transition costs from CloudWatch metrics
        try:
            # Get transition metrics (if available)
            end_time = datetime.now()
            start_time = end_time - timedelta(days=30)
            
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace='AWS/S3',
                MetricName='BucketTransitionRequests',
                Dimensions=[
                    {
                        'Name': 'BucketName',
                        'Value': bucket_name
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=86400,  # Daily
                Statistics=['Sum']
            )
            
            if response['Datapoints']:
                monthly_transitions = sum([dp['Sum'] for dp in response['Datapoints']])
                costs['transitions'] = monthly_transitions * 0.0004 * months  # $0.0004 per 1,000 requests
        
        except Exception as e:
            print(f"Could not get transition metrics: {e}")
            # Estimate based on object count
            archive_objects = object_counts.get('GLACIER', 0) + object_counts.get('DEEP_ARCHIVE', 0)
            costs['transitions'] = archive_objects * 0.0004
        
        return {
            'cost_breakdown': costs,
            'total_cost': sum(costs.values()),
            'object_distribution': object_counts,
            'analysis_period_months': months
        }
    
    def project_cost_scenarios(self, bucket_name):
        """Project costs under different scenarios"""
        
        scenarios = {
            'current': 'Current configuration',
            'aggressive_archiving': 'Aggressive lifecycle (7 days to IA, 30 to Glacier)',
            'conservative_archiving': 'Conservative lifecycle (90 days to IA, 365 to Glacier)',
            'intelligent_tiering': 'Intelligent Tiering for all objects'
        }
        
        results = {}
        
        # Get current object inventory
        total_size_gb = 0
        object_count = 0
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                total_size_gb += obj['Size'] / (1024**3)
                object_count += 1
        
        # Calculate costs for each scenario
        for scenario, description in scenarios.items():
            if scenario == 'current':
                cost_analysis = self.calculate_total_glacier_cost(bucket_name)
                annual_cost = cost_analysis['total_cost']
            
            elif scenario == 'aggressive_archiving':
                # Assume 80% in Glacier, 15% in IA, 5% in Standard
                annual_cost = (
                    total_size_gb * 0.8 * 0.004 * 12 +  # Glacier
                    total_size_gb * 0.15 * 0.0125 * 12 +  # IA
                    total_size_gb * 0.05 * 0.023 * 12 +   # Standard
                    object_count * 0.8 * (40*1024/(1024**3)) * 0.004 * 12  # Overhead
                )
            
            elif scenario == 'conservative_archiving':
                # Assume 30% in Glacier, 40% in IA, 30% in Standard
                annual_cost = (
                    total_size_gb * 0.3 * 0.004 * 12 +   # Glacier
                    total_size_gb * 0.4 * 0.0125 * 12 +  # IA
                    total_size_gb * 0.3 * 0.023 * 12 +   # Standard
                    object_count * 0.3 * (40*1024/(1024**3)) * 0.004 * 12  # Overhead
                )
            
            elif scenario == 'intelligent_tiering':
                # Assume average cost between Standard and IA
                annual_cost = total_size_gb * 0.0175 * 12  # Avg of Standard and IA
            
            results[scenario] = {
                'description': description,
                'annual_cost': annual_cost,
                'monthly_cost': annual_cost / 12
            }
        
        return results
    
    def generate_cost_optimization_report(self, bucket_name):
        """Generate comprehensive cost optimization report"""
        
        # Get current costs
        current_analysis = self.calculate_total_glacier_cost(bucket_name)
        
        # Get projections
        scenarios = self.project_cost_scenarios(bucket_name)
        
        # Find best scenario
        best_scenario = min(scenarios.items(), key=lambda x: x[1]['annual_cost'])
        
        # Object size analysis
        size_analysis = self.analyze_object_sizes(bucket_name)
        
        report = {
            'bucket_name': bucket_name,
            'report_date': datetime.now().isoformat(),
            'current_costs': current_analysis,
            'cost_scenarios': scenarios,
            'recommended_scenario': best_scenario,
            'object_size_analysis': size_analysis,
            'recommendations': []
        }
        
        # Generate recommendations
        if size_analysis.get('small_objects_count', 0) > 1000:
            report['recommendations'].append({
                'priority': 'High',
                'action': 'Aggregate small objects to reduce overhead costs',
                'potential_savings': f"${size_analysis.get('potential_annual_savings', 0):.2f}/year"
            })
        
        current_cost = current_analysis['total_cost']
        best_cost = best_scenario[1]['annual_cost']
        
        if best_cost < current_cost * 0.8:  # More than 20% savings possible
            report['recommendations'].append({
                'priority': 'Medium',
                'action': f"Consider {best_scenario[1]['description']}",
                'potential_savings': f"${current_cost - best_cost:.2f}/year"
            })
        
        return report

# Usage examples
analyzer = GlacierCostAnalyzer()

# Get comprehensive cost analysis
cost_analysis = analyzer.calculate_total_glacier_cost('my-archive-bucket')
print(f"Total annual Glacier cost: ${cost_analysis['total_cost']:.2f}")

# Compare scenarios
scenarios = analyzer.project_cost_scenarios('my-archive-bucket')
for scenario, data in scenarios.items():
    print(f"{scenario}: ${data['annual_cost']:.2f}/year - {data['description']}")

# Generate full optimization report
report = analyzer.generate_cost_optimization_report('my-archive-bucket')
print(f"\\nOptimization Report for {report['bucket_name']}:")
for rec in report['recommendations']:
    print(f"{rec['priority']} Priority: {rec['action']} - {rec['potential_savings']}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def s3_versioning_tab():
    """Content for S3 Versioning tab"""
    st.markdown("## 📋 Amazon S3 Versioning")
    st.markdown("*Keep multiple versions of an object in one bucket to restore accidentally deleted or overwritten objects*")
    
   
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    S3 Versioning allows you to preserve, retrieve, and restore every version of every object in your bucket. 
    When versioning is enabled, S3 automatically maintains multiple versions of objects, protecting against 
    accidental deletion and data corruption.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Versioning architecture
    st.markdown("###  🏗️ S3 Versioning Architecture")
    common.mermaid(create_s3_versioning_mermaid(), height=800)
    
    # Versioning states
    st.markdown("###  🔄 Versioning States & Behavior")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="lifecycle-stage">', unsafe_allow_html=True)
        st.markdown("""
        ### ❌ Unversioned Bucket
        **Default State**
        - No version tracking
        - Objects overwrite completely
        - DELETE removes permanently
        
        **Behavior:**
        - Single object per key
        - No version IDs
        - No recovery possible
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="lifecycle-stage">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Versioning-Enabled
        **Active Versioning**
        - New versions created
        - Unique version IDs assigned
        - DELETE adds delete marker
        
        **Behavior:**
        - Multiple versions stored
        - Current/noncurrent versions
        - Full version history
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="lifecycle-stage">', unsafe_allow_html=True)
        st.markdown("""
        ### ⏸️ Versioning-Suspended
        **Paused State**
        - No new versions created
        - NULL version ID for new objects
        - Existing versions preserved
        
        **Behavior:**
        - Acts like unversioned for new objects
        - Historical versions remain
        - Can re-enable versioning
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive versioning simulator
    st.markdown("###  🧮 Versioning Cost Impact Calculator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        avg_object_size_mb = st.slider("Average Object Size (MB)", 0.1, 100.0, 5.0, step=0.1)
        objects_count = st.number_input("Number of Objects", min_value=1, max_value=100000, value=1000)
        versions_per_object = st.slider("Average Versions per Object", 1, 20, 5)
    
    with col2:
        versioning_enabled = st.checkbox("Versioning Enabled", value=True)
        intelligent_tiering = st.checkbox("Use Intelligent Tiering for Old Versions", value=False)
        lifecycle_cleanup = st.checkbox("Lifecycle Cleanup Old Versions", value=True)
    
    # Calculate storage costs
    total_current_gb = (objects_count * avg_object_size_mb) / 1024
    
    if versioning_enabled:
        total_versions = objects_count * versions_per_object
        total_versioned_gb = (total_versions * avg_object_size_mb) / 1024
        
        if lifecycle_cleanup:
            # Assume lifecycle reduces old version storage by 80%
            effective_versioned_gb = total_current_gb + (total_versioned_gb - total_current_gb) * 0.2
        else:
            effective_versioned_gb = total_versioned_gb
        
        monthly_cost = effective_versioned_gb * 0.023
        additional_cost = (effective_versioned_gb - total_current_gb) * 0.023
    else:
        effective_versioned_gb = total_current_gb
        monthly_cost = total_current_gb * 0.023
        additional_cost = 0
    
    # Calculate potential data protection value
    data_protection_value = total_current_gb * 100  # Assume $100/GB for critical data recovery
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 💰 Versioning Cost Analysis
    **Current Data**: {total_current_gb:.2f} GB  
    **With Versioning**: {effective_versioned_gb:.2f} GB  
    **Monthly Storage Cost**: ${monthly_cost:.2f}  
    **Additional Cost for Versioning**: ${additional_cost:.2f}  
    **Estimated Data Protection Value**: ${data_protection_value:.0f}  
    **ROI for Data Protection**: {(data_protection_value / (additional_cost * 12)):.1f}x annual
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Version lifecycle management
    st.markdown("###  📊 Version Lifecycle Management")
    
    # Create version age distribution chart
    version_ages = list(range(0, 365, 30))  # Monthly intervals
    
    # Simulate version distribution (more recent versions more common)
    version_distribution = [max(100 - age//10, 10) for age in version_ages]
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=version_ages,
        y=version_distribution,
        mode='lines+markers',
        name='Version Count',
        line=dict(color=AWS_COLORS['primary'], width=3),
        marker=dict(size=8)
    ))
    
    fig.update_layout(
        title='Typical Object Version Age Distribution',
        xaxis_title='Version Age (Days)',
        yaxis_title='Number of Versions',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_color=AWS_COLORS['secondary']
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Code examples
    st.markdown("###  💻 Versioning Management Code")
    
    tab1, tab2, tab3 = st.tabs(["Enable & Configure", "Version Operations", "Lifecycle Integration"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
from botocore.exceptions import ClientError

class S3VersioningManager:
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def enable_versioning(self, bucket_name):
        """Enable versioning on an S3 bucket"""
        try:
            response = self.s3_client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={
                    'Status': 'Enabled'
                }
            )
            print(f"Versioning enabled for bucket: {bucket_name}")
            return True
        except ClientError as e:
            print(f"Error enabling versioning: {e}")
            return False
    
    def suspend_versioning(self, bucket_name):
        """Suspend versioning on an S3 bucket"""
        try:
            response = self.s3_client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={
                    'Status': 'Suspended'
                }
            )
            print(f"Versioning suspended for bucket: {bucket_name}")
            return True
        except ClientError as e:
            print(f"Error suspending versioning: {e}")
            return False
    
    def get_versioning_status(self, bucket_name):
        """Get current versioning status of a bucket"""
        try:
            response = self.s3_client.get_bucket_versioning(Bucket=bucket_name)
            status = response.get('Status', 'Unversioned')
            print(f"Bucket {bucket_name} versioning status: {status}")
            return status
        except ClientError as e:
            print(f"Error getting versioning status: {e}")
            return None
    
    def configure_versioning_with_mfa_delete(self, bucket_name, mfa_serial, mfa_token):
        """Configure versioning with MFA Delete protection"""
        try:
            response = self.s3_client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={
                    'Status': 'Enabled',
                    'MfaDelete': 'Enabled'
                },
                MFA=f"{mfa_serial} {mfa_token}"
            )
            print(f"Versioning with MFA Delete enabled for: {bucket_name}")
            return True
        except ClientError as e:
            print(f"Error configuring MFA Delete: {e}")
            return False

# AWS CLI commands for versioning
"""
# Enable versioning
aws s3api put-bucket-versioning \
    --bucket my-bucket \
    --versioning-configuration Status=Enabled

# Get versioning status
aws s3api get-bucket-versioning --bucket my-bucket

# Suspend versioning  
aws s3api put-bucket-versioning \
    --bucket my-bucket \
    --versioning-configuration Status=Suspended

# Enable versioning with MFA delete
aws s3api put-bucket-versioning \
    --bucket my-bucket \
    --versioning-configuration Status=Enabled,MfaDelete=Enabled \
    --mfa "arn:aws:iam::123456789012:mfa/user 123456"
"""

# Usage examples
manager = S3VersioningManager()

# Enable versioning on a bucket
manager.enable_versioning('my-important-data-bucket')

# Check versioning status
status = manager.get_versioning_status('my-important-data-bucket')

# Configure with MFA delete (requires MFA device)
# manager.configure_versioning_with_mfa_delete(
#     'my-critical-bucket',
#     'arn:aws:iam::123456789012:mfa/admin',
#     '123456'
# )
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
from datetime import datetime

class S3VersionOperations:
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def list_object_versions(self, bucket_name, prefix="", max_keys=1000):
        """List all versions of objects in a bucket"""
        try:
            response = self.s3_client.list_object_versions(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            versions = []
            
            # Process object versions
            for version in response.get('Versions', []):
                versions.append({
                    'key': version['Key'],
                    'version_id': version['VersionId'],
                    'is_latest': version['IsLatest'],
                    'last_modified': version['LastModified'],
                    'size': version['Size'],
                    'storage_class': version.get('StorageClass', 'STANDARD')
                })
            
            # Process delete markers
            for delete_marker in response.get('DeleteMarkers', []):
                versions.append({
                    'key': delete_marker['Key'],
                    'version_id': delete_marker['VersionId'],
                    'is_latest': delete_marker['IsLatest'],
                    'last_modified': delete_marker['LastModified'],
                    'is_delete_marker': True
                })
            
            return sorted(versions, key=lambda x: (x['key'], x['last_modified']), reverse=True)
            
        except Exception as e:
            print(f"Error listing object versions: {e}")
            return []
    
    def restore_object_version(self, bucket_name, object_key, version_id):
        """Restore a specific version of an object by copying it as the current version"""
        try:
            # Copy the specific version as the new current version
            copy_source = {
                'Bucket': bucket_name,
                'Key': object_key,
                'VersionId': version_id
            }
            
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=object_key
            )
            
            print(f"Restored version {version_id} of {object_key}")
            return True
            
        except Exception as e:
            print(f"Error restoring object version: {e}")
            return False
    
    def delete_specific_version(self, bucket_name, object_key, version_id):
        """Permanently delete a specific version of an object"""
        try:
            response = self.s3_client.delete_object(
                Bucket=bucket_name,
                Key=object_key,
                VersionId=version_id
            )
            
            print(f"Permanently deleted version {version_id} of {object_key}")
            return True
            
        except Exception as e:
            print(f"Error deleting object version: {e}")
            return False
    
    def remove_delete_marker(self, bucket_name, object_key, version_id):
        """Remove a delete marker to undelete an object"""
        try:
            response = self.s3_client.delete_object(
                Bucket=bucket_name,
                Key=object_key,
                VersionId=version_id
            )
            
            print(f"Removed delete marker for {object_key}")
            return True
            
        except Exception as e:
            print(f"Error removing delete marker: {e}")
            return False
    
    def analyze_version_storage(self, bucket_name):
        """Analyze storage usage across all object versions"""
        versions = self.list_object_versions(bucket_name)
        
        analysis = {
            'total_versions': 0,
            'current_versions': 0,  
            'old_versions': 0,
            'delete_markers': 0,
            'total_size_bytes': 0,
            'current_size_bytes': 0,
            'old_versions_size_bytes': 0,
            'objects_with_multiple_versions': 0
        }
        
        objects = {}
        
        for version in versions:
            key = version['key']
            
            if key not in objects:
                objects[key] = {'versions': [], 'delete_markers': []}
            
            if version.get('is_delete_marker'):
                objects[key]['delete_markers'].append(version)
                analysis['delete_markers'] += 1
            else:
                objects[key]['versions'].append(version)
                analysis['total_versions'] += 1
                analysis['total_size_bytes'] += version['size']
                
                if version['is_latest']:
                    analysis['current_versions'] += 1
                    analysis['current_size_bytes'] += version['size']
                else:
                    analysis['old_versions'] += 1
                    analysis['old_versions_size_bytes'] += version['size']
        
        # Count objects with multiple versions
        for key, data in objects.items():
            if len(data['versions']) > 1:
                analysis['objects_with_multiple_versions'] += 1
        
        return analysis
    
    def cleanup_old_versions(self, bucket_name, keep_versions=5, older_than_days=30):
        """Clean up old versions based on count and age criteria"""
        from datetime import datetime, timedelta
        
        cutoff_date = datetime.now() - timedelta(days=older_than_days)
        versions = self.list_object_versions(bucket_name)
        
        # Group versions by object key
        objects = {}
        for version in versions:
            if not version.get('is_delete_marker'):
                key = version['key']
                if key not in objects:
                    objects[key] = []
                objects[key].append(version)
        
        deleted_count = 0
        
        for key, object_versions in objects.items():
            # Sort by last modified (newest first)
            object_versions.sort(key=lambda x: x['last_modified'], reverse=True)
            
            # Keep the most recent versions
            versions_to_delete = object_versions[keep_versions:]
            
            for version in versions_to_delete:
                # Only delete if older than cutoff date
                if version['last_modified'].replace(tzinfo=None) < cutoff_date:
                    success = self.delete_specific_version(
                        bucket_name, 
                        key, 
                        version['version_id']
                    )
                    if success:
                        deleted_count += 1
        
        print(f"Cleaned up {deleted_count} old versions")
        return deleted_count

# Usage examples
version_ops = S3VersionOperations()

# List all versions of objects
versions = version_ops.list_object_versions('my-versioned-bucket')
print(f"Found {len(versions)} total versions")

# Analyze version storage usage
analysis = version_ops.analyze_version_storage('my-versioned-bucket')
print(f"Storage analysis:")
print(f"  Current versions: {analysis['current_versions']}")
print(f"  Old versions: {analysis['old_versions']}")
print(f"  Total size: {analysis['total_size_bytes'] / (1024**3):.2f} GB")
print(f"  Old versions size: {analysis['old_versions_size_bytes'] / (1024**3):.2f} GB")

# Restore a specific version (example)
# version_ops.restore_object_version('my-bucket', 'important-doc.pdf', 'version-id-here')

# Clean up old versions (keep 3 versions, older than 90 days)
cleaned = version_ops.cleanup_old_versions('my-versioned-bucket', keep_versions=3, older_than_days=90)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Lifecycle policy for versioned objects
{
    "Rules": [
        {
            "ID": "VersionedObjectManagement",
            "Status": "Enabled",
            "Filter": {},
            "Transitions": [
                {
                    "Days": 30,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "Days": 90, 
                    "StorageClass": "GLACIER"
                }
            ],
            "NoncurrentVersionTransitions": [
                {
                    "NoncurrentDays": 30,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "NoncurrentDays": 90,
                    "StorageClass": "GLACIER"
                },
                {
                    "NoncurrentDays": 365,
                    "StorageClass": "DEEP_ARCHIVE"
                }
            ],
            "NoncurrentVersionExpiration": {
                "NoncurrentDays": 2555,
                "NewerNoncurrentVersions": 5
            },
            "Expiration": {
                "ExpiredObjectDeleteMarker": true
            },
            "AbortIncompleteMultipartUpload": {
                "DaysAfterInitiation": 7
            }
        }
    ]
}

# Python - Advanced lifecycle management for versioned buckets
import boto3
import json
from datetime import datetime, timedelta

class VersionedLifecycleManager:
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def create_comprehensive_versioned_policy(self, bucket_name):
        """Create a comprehensive lifecycle policy for versioned objects"""
        
        policy = {
            "Rules": [
                {
                    "ID": "CurrentVersionOptimization",
                    "Status": "Enabled",
                    "Filter": {},
                    "Transitions": [
                        {
                            "Days": 30,
                            "StorageClass": "STANDARD_IA"
                        },
                        {
                            "Days": 90,
                            "StorageClass": "GLACIER"
                        },
                        {
                            "Days": 365,
                            "StorageClass": "DEEP_ARCHIVE"
                        }
                    ]
                },
                {
                    "ID": "NoncurrentVersionManagement", 
                    "Status": "Enabled",
                    "Filter": {},
                    "NoncurrentVersionTransitions": [
                        {
                            "NoncurrentDays": 7,      # Quick transition for old versions
                            "StorageClass": "STANDARD_IA"
                        },
                        {
                            "NoncurrentDays": 30,
                            "StorageClass": "GLACIER"
                        },
                        {
                            "NoncurrentDays": 90,
                            "StorageClass": "DEEP_ARCHIVE"
                        }
                    ],
                    "NoncurrentVersionExpiration": {
                        "NoncurrentDays": 365,
                        "NewerNoncurrentVersions": 3  # Keep 3 old versions
                    }
                },
                {
                    "ID": "DeleteMarkerCleanup",
                    "Status": "Enabled", 
                    "Filter": {},
                    "Expiration": {
                        "ExpiredObjectDeleteMarker": true
                    }
                },
                {
                    "ID": "MultipartUploadCleanup",
                    "Status": "Enabled",
                    "Filter": {},
                    "AbortIncompleteMultipartUpload": {
                        "DaysAfterInitiation": 7
                    }
                }
            ]
        }
        
        try:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=policy
            )
            print(f"Comprehensive versioned lifecycle policy applied to {bucket_name}")
            return True
        except Exception as e:
            print(f"Error applying lifecycle policy: {e}")
            return False
    
    def create_selective_version_policy(self, bucket_name, prefixes_config):
        """Create selective policies based on object prefixes"""
        
        rules = []
        
        for prefix, config in prefixes_config.items():
            rule = {
                "ID": f"Policy-{prefix.replace('/', '-')}",
                "Status": "Enabled",
                "Filter": {"Prefix": prefix},
                "Transitions": config.get("current_transitions", []),
                "NoncurrentVersionTransitions": config.get("noncurrent_transitions", []),
                "NoncurrentVersionExpiration": {
                    "NoncurrentDays": config.get("noncurrent_expiration_days", 365),
                    "NewerNoncurrentVersions": config.get("keep_versions", 2)
                }
            }
            
            if config.get("current_expiration_days"):
                rule["Expiration"] = {"Days": config["current_expiration_days"]}
                
            rules.append(rule)
        
        # Add global delete marker cleanup
        rules.append({
            "ID": "GlobalDeleteMarkerCleanup",
            "Status": "Enabled",
            "Filter": {},
            "Expiration": {"ExpiredObjectDeleteMarker": true}
        })
        
        policy = {"Rules": rules}
        
        try:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=policy
            )
            print(f"Selective versioned lifecycle policy applied to {bucket_name}")
            return True
        except Exception as e:
            print(f"Error applying selective policy: {e}")
            return False
    
    def monitor_version_lifecycle_impact(self, bucket_name):
        """Monitor the impact of lifecycle policies on versioned objects"""
        
        # Get current lifecycle configuration
        try:
            lifecycle = self.s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        except:
            print("No lifecycle configuration found")
            return None
        
        # Analyze version distribution
        versions = []
        paginator = self.s3_client.get_paginator('list_object_versions')
        
        for page in paginator.paginate(Bucket=bucket_name):
            versions.extend(page.get('Versions', []))
        
        # Group by storage class and version status
        analysis = {
            'current_versions': {'STANDARD': 0, 'STANDARD_IA': 0, 'GLACIER': 0, 'DEEP_ARCHIVE': 0},
            'noncurrent_versions': {'STANDARD': 0, 'STANDARD_IA': 0, 'GLACIER': 0, 'DEEP_ARCHIVE': 0},
            'total_objects': len(set([v['Key'] for v in versions])),
            'total_versions': len(versions)
        }
        
        for version in versions:
            storage_class = version.get('StorageClass', 'STANDARD')
            
            if version['IsLatest']:
                analysis['current_versions'][storage_class] = analysis['current_versions'].get(storage_class, 0) + 1
            else:
                analysis['noncurrent_versions'][storage_class] = analysis['noncurrent_versions'].get(storage_class, 0) + 1
        
        return analysis

# Usage examples
lifecycle_manager = VersionedLifecycleManager()

# Apply comprehensive versioned policy
lifecycle_manager.create_comprehensive_versioned_policy('my-versioned-bucket')

# Create selective policies for different data types
prefixes_config = {
    'documents/': {
        'current_transitions': [
            {'Days': 90, 'StorageClass': 'STANDARD_IA'},
            {'Days': 365, 'StorageClass': 'GLACIER'}
        ],
        'noncurrent_transitions': [
            {'NoncurrentDays': 30, 'StorageClass': 'STANDARD_IA'},
            {'NoncurrentDays': 90, 'StorageClass': 'GLACIER'}
        ],
        'noncurrent_expiration_days': 2555,  # 7 years
        'keep_versions': 5
    },
    'logs/': {
        'current_transitions': [
            {'Days': 7, 'StorageClass': 'STANDARD_IA'},
            {'Days': 30, 'StorageClass': 'GLACIER'}
        ],
        'noncurrent_transitions': [
            {'NoncurrentDays': 1, 'StorageClass': 'STANDARD_IA'},
            {'NoncurrentDays': 7, 'StorageClass': 'GLACIER'}
        ],
        'current_expiration_days': 90,
        'noncurrent_expiration_days': 30,
        'keep_versions': 1
    },
    'media/': {
        'current_transitions': [
            {'Days': 180, 'StorageClass': 'STANDARD_IA'},
            {'Days': 365, 'StorageClass': 'GLACIER'}
        ],
        'noncurrent_transitions': [
            {'NoncurrentDays': 90, 'StorageClass': 'STANDARD_IA'},
            {'NoncurrentDays': 180, 'StorageClass': 'DEEP_ARCHIVE'}
        ],
        'noncurrent_expiration_days': 1095,  # 3 years  
        'keep_versions': 3
    }
}

lifecycle_manager.create_selective_version_policy('my-versioned-bucket', prefixes_config)

# Monitor policy impact
impact = lifecycle_manager.monitor_version_lifecycle_impact('my-versioned-bucket')
if impact:
    print("Lifecycle Policy Impact Analysis:")
    print(f"Total objects: {impact['total_objects']}")
    print(f"Total versions: {impact['total_versions']}")
    print(f"Current versions by storage class: {impact['current_versions']}")
    print(f"Noncurrent versions by storage class: {impact['noncurrent_versions']}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def dynamodb_ttl_tab():
    """Content for DynamoDB TTL tab"""
    st.markdown("## ⏲️ Amazon DynamoDB - Time to Live (TTL)")
    st.markdown("*Cost effective method for deleting items that are no longer relevant*")
    
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    DynamoDB Time to Live (TTL) automatically deletes items from your table when they expire, 
    helping reduce storage costs and maintain data freshness. TTL uses a designated attribute 
    containing a timestamp to determine when items should be deleted.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # TTL Process diagram
    st.markdown("###  🔄 DynamoDB TTL Process")
    common.mermaid(create_dynamodb_ttl_mermaid(), height=1000)
    
    # TTL Configuration steps
    st.markdown("###  ⚙️ TTL Configuration Steps")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 1️⃣ Setup Requirements
        - **Enable TTL** on your DynamoDB table
        - **Define TTL attribute** to store expiration timestamp
        - **Use Unix timestamp** in seconds (not milliseconds)
        - **Calculate expiration** when creating/updating items
        
        ### 2️⃣ TTL Behavior
        - **Background process** checks items every ~48 hours
        - **Grace period** of up to 48 hours after expiration
        - **No additional cost** for TTL processing
        - **DynamoDB Streams** can capture deletion events
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 3️⃣ Best Practices
        - **Don't rely on exact timing** - use for approximate expiration
        - **Account for 48-hour delay** in your application logic
        - **Test TTL behavior** in development environment
        - **Monitor via CloudWatch** for TTL deletion metrics
        
        ### 4️⃣ Use Cases
        - **Session data** - User sessions, shopping carts
        - **Temporary data** - Cache entries, temporary tokens
        - **Event data** - Log entries, time-series data
        - **Compliance** - Data retention requirements
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive TTL calculator
    st.markdown("###  🧮 TTL Expiration Calculator")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        data_type = st.selectbox("Data Type", [
            "User Sessions", "Cache Entries", "Log Entries", 
            "Temporary Tokens", "Event Data", "Custom"
        ])
        retention_hours = st.slider("Retention Period (Hours)", 1, 8760, 24)  # Up to 1 year
    
    with col2:
        current_time = datetime.now()
        st.write(f"**Current Time**: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        expiration_time = current_time + timedelta(hours=retention_hours)
        st.write(f"**Expiration Time**: {expiration_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    with col3:
        ttl_timestamp = int(expiration_time.timestamp())
        st.write(f"**TTL Timestamp**: {ttl_timestamp}")
        st.write(f"**Retention Days**: {retention_hours / 24:.1f}")
    
    # Show example item structure
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📝 Example DynamoDB Item Structure
    ```json
    {{
        "user_id": "user123",
        "session_data": "{{...}}",
        "created_at": "{current_time.isoformat()}",
        "ttl_timestamp": {ttl_timestamp},
        "data_type": "{data_type}"
    }}
    ```
    
    **TTL Configuration**: Set `ttl_timestamp` as the TTL attribute name in DynamoDB table settings.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # TTL cost savings analysis
    st.markdown("###  💰 Cost Savings Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        items_per_day = st.number_input("Items Created Per Day", min_value=1, max_value=1000000, value=10000)
        avg_item_size_kb = st.slider("Average Item Size (KB)", 0.1, 100.0, 2.0, step=0.1)
    
    with col2:
        manual_cleanup = st.checkbox("Manual Cleanup Alternative", value=False)
        if manual_cleanup:
            cleanup_frequency_days = st.slider("Manual Cleanup Frequency (Days)", 1, 30, 7)
        else:
            cleanup_frequency_days = retention_hours / 24
    
    # Calculate costs
    daily_storage_gb = (items_per_day * avg_item_size_kb) / (1024 * 1024)
    monthly_storage_gb = daily_storage_gb * 30
    
    # Without TTL (items accumulate)
    without_ttl_storage = monthly_storage_gb * (30 / max(cleanup_frequency_days, 1))
    without_ttl_cost = without_ttl_storage * 0.25  # $0.25 per GB per month
    
    # With TTL (automatic cleanup)
    with_ttl_storage = daily_storage_gb * (retention_hours / 24)
    with_ttl_cost = with_ttl_storage * 0.25
    
    monthly_savings = without_ttl_cost - with_ttl_cost
    annual_savings = monthly_savings * 12
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 💰 TTL Cost Impact Analysis
    **Daily Data Volume**: {daily_storage_gb:.3f} GB  
    **Without TTL Storage**: {without_ttl_storage:.2f} GB  
    **With TTL Storage**: {with_ttl_storage:.2f} GB  
    **Monthly Storage Cost Savings**: ${monthly_savings:.2f}  
    **Annual Cost Savings**: ${annual_savings:.2f}  
    **Storage Reduction**: {((without_ttl_storage - with_ttl_storage) / without_ttl_storage * 100):.1f}%
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Common TTL patterns
    st.markdown("###  📋 Common TTL Patterns")
    
    patterns_data = {
        'Use Case': ['User Sessions', 'API Rate Limiting', 'Cache Entries', 'Event Logs', 'Temporary Files'],
        'Typical Retention': ['2-8 hours', '1 hour', '15 minutes - 1 hour', '7-30 days', '1-24 hours'],
        'TTL Strategy': ['Session timeout', 'Window reset', 'Cache invalidation', 'Compliance retention', 'Temp cleanup'],
        'Business Value': ['Security', 'Rate control', 'Performance', 'Cost optimization', 'Storage management']
    }
    
    df_patterns = pd.DataFrame(patterns_data)
    st.dataframe(df_patterns, use_container_width=True)
    
    # Code examples
    st.markdown("###  💻 DynamoDB TTL Implementation")
    
    tab1, tab2, tab3 = st.tabs(["Enable TTL", "Item Management", "Advanced Patterns"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
import time
from datetime import datetime, timedelta

class DynamoDBTTLManager:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.dynamodb_client = boto3.client('dynamodb')
    
    def enable_ttl(self, table_name, ttl_attribute_name):
        """Enable TTL on a DynamoDB table"""
        try:
            response = self.dynamodb_client.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={
                    'Enabled': True,
                    'AttributeName': ttl_attribute_name
                }
            )
            print(f"TTL enabled on table {table_name} with attribute {ttl_attribute_name}")
            return True
        except Exception as e:
            print(f"Error enabling TTL: {e}")
            return False
    
    def disable_ttl(self, table_name):
        """Disable TTL on a DynamoDB table"""
        try:
            response = self.dynamodb_client.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={
                    'Enabled': False
                }
            )
            print(f"TTL disabled on table {table_name}")
            return True
        except Exception as e:
            print(f"Error disabling TTL: {e}")
            return False
    
    def get_ttl_status(self, table_name):
        """Get current TTL status for a table"""
        try:
            response = self.dynamodb_client.describe_time_to_live(
                TableName=table_name
            )
            
            ttl_spec = response.get('TimeToLiveDescription', {})
            status = ttl_spec.get('TimeToLiveStatus', 'DISABLED')
            attribute = ttl_spec.get('AttributeName', 'N/A')
            
            print(f"Table: {table_name}")
            print(f"TTL Status: {status}")
            print(f"TTL Attribute: {attribute}")
            
            return {
                'status': status,
                'attribute_name': attribute,
                'enabled': status == 'ENABLED'
            }
        except Exception as e:
            print(f"Error getting TTL status: {e}")
            return None
    
    def calculate_ttl_timestamp(self, hours_from_now):
        """Calculate TTL timestamp for items"""
        expiration_time = datetime.now() + timedelta(hours=hours_from_now)
        return int(expiration_time.timestamp())
    
    def put_item_with_ttl(self, table_name, item_data, ttl_hours):
        """Put an item with TTL timestamp"""
        table = self.dynamodb.Table(table_name)
        
        # Add TTL timestamp to item
        item_data['ttl_timestamp'] = self.calculate_ttl_timestamp(ttl_hours)
        item_data['created_at'] = datetime.now().isoformat()
        
        try:
            response = table.put_item(Item=item_data)
            print(f"Item added with TTL expiration in {ttl_hours} hours")
            return True
        except Exception as e:
            print(f"Error putting item: {e}")
            return False

# AWS CLI commands for TTL management
"""
# Enable TTL
aws dynamodb update-time-to-live \
    --table-name MyTable \
    --time-to-live-specification "Enabled=true,AttributeName=ttl_timestamp"

# Check TTL status
aws dynamodb describe-time-to-live --table-name MyTable

# Disable TTL
aws dynamodb update-time-to-live \
    --table-name MyTable \
    --time-to-live-specification "Enabled=false"
"""

# Usage examples
ttl_manager = DynamoDBTTLManager()

# Enable TTL on a table
ttl_manager.enable_ttl('user-sessions', 'ttl_timestamp')

# Check TTL status
status = ttl_manager.get_ttl_status('user-sessions')

# Add item with TTL (expires in 24 hours)
session_data = {
    'user_id': 'user123',
    'session_token': 'abc123xyz',
    'login_time': datetime.now().isoformat()
}

ttl_manager.put_item_with_ttl('user-sessions', session_data, ttl_hours=24)

# Calculate TTL timestamp manually
ttl_timestamp = ttl_manager.calculate_ttl_timestamp(2)  # 2 hours from now
print(f"Item will expire at timestamp: {ttl_timestamp}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
from datetime import datetime, timedelta
from decimal import Decimal

class TTLItemManager:
    def __init__(self, table_name):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)
        self.table_name = table_name
    
    def create_session_item(self, user_id, session_data, session_duration_hours=8):
        """Create a user session item with TTL"""
        ttl_timestamp = int((datetime.now() + timedelta(hours=session_duration_hours)).timestamp())
        
        item = {
            'user_id': user_id,
            'session_id': f"sess_{int(datetime.now().timestamp())}",
            'session_data': session_data,
            'created_at': datetime.now().isoformat(),
            'ttl_timestamp': ttl_timestamp,
            'session_duration_hours': session_duration_hours
        }
        
        try:
            self.table.put_item(Item=item)
            print(f"Session created for user {user_id}, expires in {session_duration_hours} hours")
            return item['session_id']
        except Exception as e:
            print(f"Error creating session: {e}")
            return None
    
    def create_cache_item(self, cache_key, cache_value, ttl_minutes=30):
        """Create a cache item with TTL"""
        ttl_timestamp = int((datetime.now() + timedelta(minutes=ttl_minutes)).timestamp())
        
        item = {
            'cache_key': cache_key,
            'cache_value': cache_value,
            'cached_at': datetime.now().isoformat(),
            'ttl_timestamp': ttl_timestamp,
            'ttl_minutes': ttl_minutes
        }
        
        try:
            self.table.put_item(Item=item)
            print(f"Cache item created: {cache_key}, expires in {ttl_minutes} minutes")
            return True
        except Exception as e:
            print(f"Error creating cache item: {e}")
            return False
    
    def create_rate_limit_item(self, user_id, api_endpoint, request_count=1, window_minutes=60):
        """Create rate limiting item with TTL"""
        ttl_timestamp = int((datetime.now() + timedelta(minutes=window_minutes)).timestamp())
        
        # Use composite key for rate limiting
        rate_limit_key = f"{user_id}#{api_endpoint}#{datetime.now().strftime('%Y%m%d%H')}"
        
        item = {
            'rate_limit_key': rate_limit_key,
            'user_id': user_id,
            'api_endpoint': api_endpoint,
            'request_count': request_count,
            'window_start': datetime.now().isoformat(),
            'ttl_timestamp': ttl_timestamp
        }
        
        try:
            # Try to update existing item or create new one
            response = self.table.update_item(
                Key={'rate_limit_key': rate_limit_key},
                UpdateExpression='ADD request_count :inc SET updated_at = :now',
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':now': datetime.now().isoformat()
                },
                ReturnValues='ALL_NEW'
            )
            
            current_count = response['Attributes']['request_count']
            print(f"Rate limit updated for {user_id}: {current_count} requests")
            return current_count
            
        except self.table.meta.client.exceptions.ClientError:
            # Item doesn't exist, create it
            try:
                self.table.put_item(Item=item)
                print(f"Rate limit item created for {user_id}")
                return 1
            except Exception as e:
                print(f"Error creating rate limit item: {e}")
                return None
    
    def extend_ttl(self, item_key, additional_hours):
        """Extend TTL for an existing item"""
        new_ttl = int((datetime.now() + timedelta(hours=additional_hours)).timestamp())
        
        try:
            response = self.table.update_item(
                Key=item_key,
                UpdateExpression='SET ttl_timestamp = :new_ttl, extended_at = :now',
                ExpressionAttributeValues={
                    ':new_ttl': new_ttl,
                    ':now': datetime.now().isoformat()
                },
                ReturnValues='ALL_NEW'
            )
            print(f"TTL extended by {additional_hours} hours")
            return response['Attributes']
        except Exception as e:
            print(f"Error extending TTL: {e}")
            return None
    
    def remove_ttl(self, item_key):
        """Remove TTL from an item (make it persistent)"""
        try:
            response = self.table.update_item(
                Key=item_key,
                UpdateExpression='REMOVE ttl_timestamp SET made_persistent_at = :now',
                ExpressionAttributeValues={
                    ':now': datetime.now().isoformat()
                },
                ReturnValues='ALL_NEW'
            )
            print("TTL removed - item is now persistent")
            return response['Attributes']
        except Exception as e:
            print(f"Error removing TTL: {e}")
            return None
    
    def query_items_by_ttl(self, ttl_range_hours=24):
        """Query items that will expire within specified hours"""
        future_timestamp = int((datetime.now() + timedelta(hours=ttl_range_hours)).timestamp())
        current_timestamp = int(datetime.now().timestamp())
        
        try:
            # Note: This requires a GSI on ttl_timestamp for efficient querying
            response = self.table.scan(
                FilterExpression='ttl_timestamp BETWEEN :start AND :end',
                ExpressionAttributeValues={
                    ':start': current_timestamp,
                    ':end': future_timestamp
                }
            )
            
            items = response['Items']
            print(f"Found {len(items)} items expiring within {ttl_range_hours} hours")
            return items
        except Exception as e:
            print(f"Error querying items by TTL: {e}")
            return []

# Application-specific implementations
class SessionManager(TTLItemManager):
    """Specialized class for session management"""
    
    def create_user_session(self, user_id, login_data):
        """Create user session with standard 8-hour expiry"""
        session_data = {
            'login_time': datetime.now().isoformat(),
            'user_agent': login_data.get('user_agent', ''),
            'ip_address': login_data.get('ip_address', ''),
            'permissions': login_data.get('permissions', [])
        }
        
        return self.create_session_item(user_id, session_data, session_duration_hours=8)
    
    def extend_session(self, user_id, session_id):
        """Extend user session by 4 hours"""
        item_key = {'user_id': user_id, 'session_id': session_id}
        return self.extend_ttl(item_key, additional_hours=4)

class CacheManager(TTLItemManager):
    """Specialized class for cache management"""
    
    def cache_api_response(self, api_endpoint, response_data, ttl_minutes=30):
        """Cache API response with TTL"""
        cache_key = f"api_cache_{api_endpoint.replace('/', '_')}"
        
        cache_value = {
            'response_data': response_data,
            'cached_from': api_endpoint,
            'response_size': len(str(response_data))
        }
        
        return self.create_cache_item(cache_key, cache_value, ttl_minutes)
    
    def get_cached_response(self, api_endpoint):
        """Retrieve cached API response"""
        cache_key = f"api_cache_{api_endpoint.replace('/', '_')}"
        
        try:
            response = self.table.get_item(Key={'cache_key': cache_key})
            
            if 'Item' in response:
                item = response['Item']
                # Check if item hasn't expired yet (client-side check)
                if item['ttl_timestamp'] > int(datetime.now().timestamp()):
                    return item['cache_value']
                else:
                    print("Cache item found but already expired")
                    return None
            else:
                print("Cache miss")
                return None
        except Exception as e:
            print(f"Error retrieving cached response: {e}")
            return None

# Usage examples
session_manager = SessionManager('user-sessions')
cache_manager = CacheManager('api-cache')

# Create user session
login_data = {
    'user_agent': 'Mozilla/5.0...',
    'ip_address': '192.168.1.100',
    'permissions': ['read', 'write']
}
session_id = session_manager.create_user_session('user123', login_data)

# Cache API response
api_data = {'result': 'success', 'data': [1, 2, 3, 4, 5]}
cache_manager.cache_api_response('/api/v1/data', api_data, ttl_minutes=15)

# Retrieve cached response
cached_data = cache_manager.get_cached_response('/api/v1/data')
if cached_data:
    print("Using cached response")
else:
    print("Cache miss - fetch from API")

# Extend session
session_manager.extend_session('user123', session_id)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
from datetime import datetime, timedelta
import json

class AdvancedTTLPatterns:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.cloudwatch = boto3.client('cloudwatch')
        self.sns = boto3.client('sns')
    
    def setup_ttl_with_streams(self, table_name, stream_arn, lambda_function_arn):
        """Set up DynamoDB Streams to capture TTL deletions"""
        
        # This would typically be done through CloudFormation or Terraform
        # Here's the conceptual setup
        
        stream_config = {
            'StreamEnabled': True,
            'StreamViewType': 'NEW_AND_OLD_IMAGES'
        }
        
        # Lambda trigger would process TTL deletion events
        lambda_trigger_code = """
        def lambda_handler(event, context):
            for record in event['Records']:
                if record['eventName'] == 'REMOVE':
                    # Check if removal was due to TTL
                    if 'userIdentity' in record and record['userIdentity']['type'] == 'Service':
                        # This was a TTL deletion
                        old_image = record['dynamodb'].get('OldImage', {})
                        
                        # Process TTL deletion event
                        process_ttl_deletion(old_image)
            
            return {'statusCode': 200}
        
        def process_ttl_deletion(item):
            # Custom logic for handling expired items
            # Send notifications, cleanup related data, etc.
            pass
        """
        
        print(f"TTL Streams processing configured for {table_name}")
        return lambda_trigger_code
    
    def implement_cascading_ttl(self, base_table_name, related_tables):
        """Implement cascading TTL across related tables"""
        
        class CascadingTTLManager:
            def __init__(self, base_table, related_tables):
                self.base_table = self.dynamodb.Table(base_table)
                self.related_tables = {name: self.dynamodb.Table(name) for name in related_tables}
            
            def create_item_with_cascade(self, base_item, related_items, ttl_hours):
                """Create items across multiple tables with synchronized TTL"""
                ttl_timestamp = int((datetime.now() + timedelta(hours=ttl_hours)).timestamp())
                
                # Add TTL to base item
                base_item['ttl_timestamp'] = ttl_timestamp
                base_item['cascade_id'] = f"cascade_{int(datetime.now().timestamp())}"
                
                try:
                    # Create base item
                    self.base_table.put_item(Item=base_item)
                    
                    # Create related items with same TTL
                    for table_name, items in related_items.items():
                        if table_name in self.related_tables:
                            table = self.related_tables[table_name]
                            
                            for item in items:
                                item['ttl_timestamp'] = ttl_timestamp
                                item['cascade_id'] = base_item['cascade_id']
                                table.put_item(Item=item)
                    
                    print(f"Cascading TTL items created with {ttl_hours} hour expiration")
                    return base_item['cascade_id']
                    
                except Exception as e:
                    print(f"Error creating cascading TTL items: {e}")
                    return None
        
        return CascadingTTLManager(base_table_name, related_tables)
    
    def implement_conditional_ttl(self, table_name):
        """Implement conditional TTL based on item attributes"""
        
        class ConditionalTTLManager:
            def __init__(self, table_name):
                self.table = self.dynamodb.Table(table_name)
            
            def put_item_with_conditional_ttl(self, item_data):
                """Set TTL based on item attributes"""
                
                # Example: Different TTL based on priority
                priority = item_data.get('priority', 'normal')
                
                ttl_hours = {
                    'critical': 168,   # 1 week
                    'high': 72,       # 3 days  
                    'normal': 24,     # 1 day
                    'low': 4          # 4 hours
                }.get(priority, 24)
                
                # Different TTL for different data types
                data_type = item_data.get('data_type', 'general')
                
                if data_type == 'session':
                    ttl_hours = 8
                elif data_type == 'cache':
                    ttl_hours = 1
                elif data_type == 'log':
                    ttl_hours = 168  # 1 week
                elif data_type == 'temp':
                    ttl_hours = 0.5  # 30 minutes
                
                # Calculate TTL timestamp
                ttl_timestamp = int((datetime.now() + timedelta(hours=ttl_hours)).timestamp())
                
                item_data['ttl_timestamp'] = ttl_timestamp
                item_data['ttl_category'] = f"{priority}_{data_type}"
                
                try:
                    self.table.put_item(Item=item_data)
                    print(f"Item created with {ttl_hours} hour TTL ({priority} {data_type})")
                    return True
                except Exception as e:
                    print(f"Error creating conditional TTL item: {e}")
                    return False
        
        return ConditionalTTLManager(table_name)
    
    def monitor_ttl_deletions(self, table_name):
        """Monitor TTL deletion patterns and costs"""
        
        # Create CloudWatch custom metrics for TTL monitoring
        def publish_ttl_metrics(deleted_count, table_size_mb):
            try:
                self.cloudwatch.put_metric_data(
                    Namespace='DynamoDB/TTL',
                    MetricData=[
                        {
                            'MetricName': 'TTLDeletedItems',
                            'Dimensions': [
                                {
                                    'Name': 'TableName',
                                    'Value': table_name
                                }
                            ],
                            'Value': deleted_count,
                            'Unit': 'Count'
                        },
                        {
                            'MetricName': 'StorageSavedMB',
                            'Dimensions': [
                                {
                                    'Name': 'TableName', 
                                    'Value': table_name
                                }
                            ],
                            'Value': table_size_mb,
                            'Unit': 'Megabytes'
                        }
                    ]
                )
                print(f"TTL metrics published for {table_name}")
            except Exception as e:
                print(f"Error publishing metrics: {e}")
        
        return publish_ttl_metrics
    
    def implement_ttl_with_soft_delete(self, table_name):
        """Implement soft delete pattern with TTL"""
        
        class SoftDeleteTTLManager:
            def __init__(self, table_name):
                self.table = self.dynamodb.Table(table_name)
            
            def soft_delete_item(self, item_key, soft_delete_ttl_days=30):
                """Mark item as deleted with TTL for permanent removal"""
                
                ttl_timestamp = int((datetime.now() + timedelta(days=soft_delete_ttl_days)).timestamp())
                
                try:
                    response = self.table.update_item(
                        Key=item_key,
                        UpdateExpression='SET deleted = :true, deleted_at = :now, ttl_timestamp = :ttl',
                        ExpressionAttributeValues={
                            ':true': True,
                            ':now': datetime.now().isoformat(),
                            ':ttl': ttl_timestamp
                        },
                        ReturnValues='ALL_NEW'
                    )
                    
                    print(f"Item soft deleted, will be permanently removed in {soft_delete_ttl_days} days")
                    return response['Attributes']
                    
                except Exception as e:
                    print(f"Error soft deleting item: {e}")
                    return None
            
            def restore_soft_deleted_item(self, item_key):
                """Restore a soft deleted item"""
                try:
                    response = self.table.update_item(
                        Key=item_key,
                        UpdateExpression='REMOVE deleted, deleted_at, ttl_timestamp SET restored_at = :now',
                        ExpressionAttributeValues={
                            ':now': datetime.now().isoformat()
                        },
                        ConditionExpression='deleted = :true',
                        ExpressionAttributeValues={
                            ':true': True,
                            ':now': datetime.now().isoformat()
                        },
                        ReturnValues='ALL_NEW'
                    )
                    
                    print("Item restored from soft delete")
                    return response['Attributes']
                    
                except Exception as e:
                    print(f"Error restoring item: {e}")
                    return None
            
            def query_soft_deleted_items(self):
                """Query items marked for soft delete"""
                try:
                    response = self.table.scan(
                        FilterExpression='deleted = :true',
                        ExpressionAttributeValues={':true': True}
                    )
                    
                    items = response['Items']
                    print(f"Found {len(items)} soft deleted items")
                    return items
                    
                except Exception as e:
                    print(f"Error querying soft deleted items: {e}")
                    return []
        
        return SoftDeleteTTLManager(table_name)

# Usage examples
advanced_ttl = AdvancedTTLPatterns()

# Set up cascading TTL
cascade_manager = advanced_ttl.implement_cascading_ttl(
    'orders', 
    ['order_items', 'order_tracking', 'order_notifications']
)

base_order = {
    'order_id': 'ord_123',
    'customer_id': 'cust_456',
    'total_amount': 99.99
}

related_data = {
    'order_items': [
        {'order_id': 'ord_123', 'item_id': 'item_1', 'quantity': 2},
        {'order_id': 'ord_123', 'item_id': 'item_2', 'quantity': 1}
    ],
    'order_tracking': [
        {'order_id': 'ord_123', 'status': 'pending', 'timestamp': datetime.now().isoformat()}
    ]
}

cascade_id = cascade_manager.create_item_with_cascade(base_order, related_data, ttl_hours=720)  # 30 days

# Set up conditional TTL
conditional_manager = advanced_ttl.implement_conditional_ttl('application_data')

# Create items with different TTL based on attributes
conditional_manager.put_item_with_conditional_ttl({
    'item_id': 'item_1',
    'priority': 'critical',
    'data_type': 'log',
    'content': 'Important system event'
})

conditional_manager.put_item_with_conditional_ttl({
    'item_id': 'item_2', 
    'priority': 'low',
    'data_type': 'cache',
    'content': 'Temporary cache data'
})

# Implement soft delete with TTL
soft_delete_manager = advanced_ttl.implement_ttl_with_soft_delete('user_data')

# Soft delete an item (30 day grace period)
soft_delete_manager.soft_delete_item({'user_id': 'user_123'}, soft_delete_ttl_days=30)

# Query soft deleted items
soft_deleted = soft_delete_manager.query_soft_deleted_items()

# Restore if needed
# soft_delete_manager.restore_soft_deleted_item({'user_id': 'user_123'})
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

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
    # 🗄️ Data Lifecycle & Cost Optimization
    <div class='info-box'>
    Master lifecycle policies, cost optimization, and data retention strategies
    </div>
    """, unsafe_allow_html=True)
    
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🔄 Redshift Data Loading",
        "📦 S3 Lifecycle", 
        "⏰ S3 Expiring Objects",
        "🧊 Glacier Cost Analysis",
        "📋 S3 Versioning",
        "⏲️ DynamoDB TTL"
    ])
    
    with tab1:
        redshift_loading_tab()
    
    with tab2:
        s3_lifecycle_tab()
    
    with tab3:
        s3_expiring_objects_tab()
    
    with tab4:
        glacier_cost_tab()
    
    with tab5:
        s3_versioning_tab()
    
    with tab6:
        dynamodb_ttl_tab()
    
    # Footer
    st.markdown("""
    <div class="footer">
        <p>© 2025, Amazon Web Services, Inc. or its affiliates. All rights reserved.</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
