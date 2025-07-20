
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import utils.common as common
import utils.authenticate as authenticate
import json
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="AWS Glue & Data Cataloging",
    page_icon="🔍",
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
            padding: 0px 12px;
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
        
        .glue-service {{
            background: white;
            padding: 15px;
            border-radius: 10px;
            border: 2px solid {AWS_COLORS['light_blue']};
            margin: 10px 0;
            text-align: center;
        }}
        
        .crawler-status {{
            padding: 10px;
            border-radius: 8px;
            margin: 5px 0;
            text-align: center;
            font-weight: bold;
        }}
        .info-box {{
            background-color: #E6F2FF;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 15px;
            border-left: 5px solid #00A1C9;
        }}
        
        .status-running {{
            background-color: {AWS_COLORS['primary']};
            color: white;
        }}
        
        .status-success {{
            background-color: {AWS_COLORS['success']};
            color: white;
        }}
        
        .status-failed {{
            background-color: #d32f2f;
            color: white;
        }}
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    common.initialize_session_state()
    if 'session_started' not in st.session_state:
        st.session_state.session_started = True
        st.session_state.glue_explored = []
        st.session_state.concepts_learned = []
        st.session_state.crawler_runs = []

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🔍 AWS Glue - Serverless data integration service
            - 📊 AWS Glue Data Catalog - Central metadata repository
            - 🔄 AWS Glue Schema Registry - Schema management and evolution
            - 🕷️ AWS Glue Crawlers - Automated data discovery
            - 📂 AWS Glue Table Partitions - Data organization strategies
            - ⏰ AWS Glue Crawler Scheduling - Automated catalog synchronization
            
            **Learning Objectives:**
            - Understand serverless data integration concepts
            - Master data catalog management and metadata
            - Learn schema evolution and registry patterns
            - Explore automated data discovery with crawlers
            - Implement partition strategies for performance
            - Configure scheduling for data pipeline automation
            """)

def create_glue_architecture_mermaid():
    """Create mermaid diagram for AWS Glue architecture"""
    return """
    graph TB
        subgraph "Data Sources"
            S3[Amazon S3]
            RDS[Amazon RDS]
            REDSHIFT[Amazon Redshift]
            DYNAMO[Amazon DynamoDB]
        end
        
        subgraph "AWS Glue"
            CATALOG[Glue Data Catalog<br/>📊 Metadata Store]
            CRAWLER[Glue Crawlers<br/>🕷️ Data Discovery]
            ETL[Glue ETL Jobs<br/>⚙️ Data Processing]
            STUDIO[Glue Studio<br/>🎨 Visual ETL]
            REGISTRY[Schema Registry<br/>🔄 Schema Management]
        end
        
        subgraph "Analytics Services"
            ATHENA[Amazon Athena]
            EMR[Amazon EMR]
            SPECTRUM[Redshift Spectrum]
            QUICKSIGHT[Amazon QuickSight]
        end
        
        S3 --> CRAWLER
        RDS --> CRAWLER
        REDSHIFT --> CRAWLER
        DYNAMO --> CRAWLER
        
        CRAWLER --> CATALOG
        ETL --> CATALOG
        REGISTRY --> CATALOG
        
        CATALOG --> ATHENA
        CATALOG --> EMR
        CATALOG --> SPECTRUM
        CATALOG --> QUICKSIGHT
        
        STUDIO --> ETL
        
        style CATALOG fill:#FF9900,stroke:#232F3E,color:#fff
        style CRAWLER fill:#4B9EDB,stroke:#232F3E,color:#fff
        style ETL fill:#3FB34F,stroke:#232F3E,color:#fff
        style REGISTRY fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_crawler_workflow_mermaid():
    """Create mermaid diagram for crawler workflow"""
    return """
    graph TD
        START[Crawler Started]
        CONNECT[Connect to Data Store]
        CLASSIFY[Run Classifiers]
        CUSTOM[Custom Classifiers]
        BUILTIN[Built-in Classifiers]
        SCHEMA[Infer Schema]
        METADATA[Write Metadata]
        CATALOG[Update Data Catalog]
        END[Crawler Complete]
        
        START --> CONNECT
        CONNECT --> CLASSIFY
        CLASSIFY --> CUSTOM
        CUSTOM --> |Match Found| SCHEMA
        CUSTOM --> |No Match| BUILTIN
        BUILTIN --> SCHEMA
        SCHEMA --> METADATA
        METADATA --> CATALOG
        CATALOG --> END
        
        style START fill:#FF9900,stroke:#232F3E,color:#fff
        style CLASSIFY fill:#4B9EDB,stroke:#232F3E,color:#fff
        style SCHEMA fill:#3FB34F,stroke:#232F3E,color:#fff
        style CATALOG fill:#232F3E,stroke:#FF9900,color:#fff
        style END fill:#FF9900,stroke:#232F3E,color:#fff
    """

def create_partition_diagram_mermaid():
    """Create mermaid diagram for table partitions"""
    return """
    graph TB
        subgraph "S3 Bucket: my-app-bucket"
            subgraph "Year=2024"
                subgraph "Month=01"
                    D1[Day=01<br/>sales_2024_01_01.csv]
                    D2[Day=02<br/>sales_2024_01_02.csv]
                end
                subgraph "Month=02"
                    D3[Day=01<br/>sales_2024_02_01.csv]
                    D4[Day=02<br/>sales_2024_02_02.csv]
                end
            end
            subgraph "Year=2025"
                subgraph "Month=01"
                    D5[Day=01<br/>sales_2025_01_01.csv]
                    D6[Day=02<br/>sales_2025_01_02.csv]
                end
            end
        end
        
        CRAWLER[AWS Glue Crawler]
        TABLE[Partitioned Table<br/>Partition Keys:<br/>year, month, day]
        
        D1 --> CRAWLER
        D2 --> CRAWLER
        D3 --> CRAWLER
        D4 --> CRAWLER
        D5 --> CRAWLER
        D6 --> CRAWLER
        
        CRAWLER --> TABLE
        
        style CRAWLER fill:#FF9900,stroke:#232F3E,color:#fff
        style TABLE fill:#4B9EDB,stroke:#232F3E,color:#fff
    """

def aws_glue_tab():
    """Content for AWS Glue tab"""
    st.markdown("## 🔍 AWS Glue")
    st.markdown("*Discover, prepare, and integrate all your data at any scale*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    AWS Glue is a serverless data integration service that makes it easier to discover, prepare, move, 
    and integrate data from multiple sources for analytics, machine learning (ML), and application development.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # AWS Glue Architecture
    st.markdown("#### 🏗️ AWS Glue Architecture")
    common.mermaid(create_glue_architecture_mermaid(), height=600)
    
    # Glue Components
    st.markdown("#### 🧩 Core Components")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="glue-service">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Data Catalog
        - Central metadata repository
        - Table definitions and schemas
        - Partition information
        - Apache Hive Metastore compatible
        
        ### 🕷️ Crawlers
        - Automated data discovery
        - Schema inference
        - Partition detection
        - Scheduled updates
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="glue-service">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚙️ ETL Jobs
        - Scala and Python support
        - Auto-generated code
        - Visual job editor
        - Spark-based processing
        
        ### 🔄 Schema Registry
        - Schema evolution control
        - Compatibility validation
        - Avro and JSON support
        - Stream integration
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive data source selector
    st.markdown("#### 🗂️ Supported Data Sources")
    
    data_sources = {
        'Cloud Databases': ['Amazon RDS', 'Amazon Redshift', 'Amazon DynamoDB', 'Amazon Aurora'],
        'Storage': ['Amazon S3', 'Amazon EFS'],
        'Streaming': ['Amazon Kinesis Data Streams', 'Amazon MSK', 'Apache Kafka'],  
        'On-Premises': ['MySQL', 'PostgreSQL', 'Oracle', 'SQL Server'],
        'Third-Party': ['Snowflake', 'Google BigQuery', 'Teradata', 'MongoDB']
    }
    
    selected_category = st.selectbox("Select Data Source Category:", list(data_sources.keys()))
    
    if selected_category:
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### {selected_category} Sources
        {', '.join(data_sources[selected_category])}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Use cases
    st.markdown("#### 🎯 Common Use Cases")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Data Lake Analytics
        - Catalog S3 data
        - Query with Athena
        - ETL processing
        - Schema evolution
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Data Warehouse ETL
        - Extract from sources
        - Transform data
        - Load to Redshift
        - Automated scheduling
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🤖 ML Data Preparation
        - Feature engineering
        - Data cleaning
        - Format conversion
        - SageMaker integration
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Code Examples")
    
    tab1, tab2 = st.tabs(["Basic Glue Operations", "ETL Job Creation"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# AWS CLI - Basic Glue operations
# List databases
aws glue get-databases

# List tables in a database
aws glue get-tables --database-name my_database

# Get table details
aws glue get-table --database-name my_database --name my_table

# Python Boto3 - AWS Glue operations
import boto3

glue_client = boto3.client('glue')

# Create database
response = glue_client.create_database(
    DatabaseInput={
        'Name': 'analytics_db',
        'Description': 'Database for analytics data'
    }
)

# List all databases
databases = glue_client.get_databases()
for db in databases['DatabaseList']:
    print(f"Database: {db['Name']}")

# Create table manually
table_input = {
    'Name': 'sales_data',
    'StorageDescriptor': {
        'Columns': [
            {'Name': 'sale_id', 'Type': 'bigint'},
            {'Name': 'customer_id', 'Type': 'int'},
            {'Name': 'product_id', 'Type': 'int'},
            {'Name': 'sale_date', 'Type': 'date'},
            {'Name': 'amount', 'Type': 'decimal(10,2)'}
        ],
        'Location': 's3://my-data-bucket/sales/',
        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
            'Parameters': {'field.delim': ','}
        }
    },
    'PartitionKeys': [
        {'Name': 'year', 'Type': 'string'},
        {'Name': 'month', 'Type': 'string'}
    ]
}

glue_client.create_table(
    DatabaseName='analytics_db',
    TableInput=table_input
)

print("Table created successfully!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create ETL Job with Python script
etl_script = """
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from Data Catalog
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="analytics_db",
    table_name="raw_sales_data",
    transformation_ctx="datasource"
)

# Apply transformations
# Remove null values
clean_data = DropNullFields.apply(
    frame=datasource,
    transformation_ctx="clean_data"
)

# Rename columns
renamed_data = RenameField.apply(
    frame=clean_data,
    old_name="sale_amt",
    new_name="sale_amount",
    transformation_ctx="renamed_data"
)

# Write to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=renamed_data,
    connection_type="s3",
    connection_options={
        "path": "s3://processed-data-bucket/sales/",
        "partitionKeys": ["year", "month"]
    },
    format="parquet",
    transformation_ctx="output"
)

job.commit()
"""

# Create Glue job
import boto3

glue_client = boto3.client('glue')

job_response = glue_client.create_job(
    Name='sales-data-etl',
    Role='arn:aws:iam::123456789012:role/GlueServiceRole',
    Command={
        'Name': 'glueetl',
        'ScriptLocation': 's3://my-scripts-bucket/sales_etl.py',
        'PythonVersion': '3'
    },
    DefaultArguments={
        '--TempDir': 's3://my-temp-bucket/temp/',
        '--job-bookmark-option': 'job-bookmark-enable',
        '--enable-metrics': '',
        '--enable-continuous-cloudwatch-log': 'true'
    },
    MaxRetries=1,
    Timeout=60,
    GlueVersion='4.0',
    NumberOfWorkers=2,
    WorkerType='G.1X'
)

print(f"Created job: {job_response['Name']}")

# Start job run
run_response = glue_client.start_job_run(
    JobName='sales-data-etl',
    Arguments={
        '--source_database': 'analytics_db',
        '--source_table': 'raw_sales_data',
        '--target_path': 's3://processed-data-bucket/sales/'
    }
)

print(f"Started job run: {run_response['JobRunId']}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def data_catalog_tab():
    """Content for AWS Glue Data Catalog tab"""
    st.markdown("## 📊 AWS Glue Data Catalog")
    st.markdown("*Central repository to store structural and operational metadata for all your data assets*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    The AWS Glue Data Catalog is a central repository for storing metadata about your data assets. 
    It acts as a unified view of your data across different AWS services like Athena, EMR, and Redshift Spectrum.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Catalog components
    st.markdown("#### 🏗️ Data Catalog Components")
    
    catalog_diagram = """
    graph TB
        subgraph "Data Catalog"
            DB[Databases<br/>📁 Logical Grouping]
            TABLE[Tables<br/>📋 Schema & Location]
            PARTITION[Partitions<br/>🗂️ Data Organization]
            CONN[Connections<br/>🔗 Data Source Info]
        end
        
        subgraph "Metadata Information"
            SCHEMA[Schema Definition<br/>Column Names & Types]
            LOCATION[Data Location<br/>S3 Paths, URIs]
            FORMAT[Data Format<br/>CSV, JSON, Parquet]
            STATS[Statistics<br/>Row Counts, Size]
        end
        
        subgraph "Integration Services"
            ATHENA[Amazon Athena]
            EMR[Amazon EMR]
            SPECTRUM[Redshift Spectrum]
            SAGEMAKER[Amazon SageMaker]
        end
        
        DB --> TABLE
        TABLE --> PARTITION
        TABLE --> SCHEMA
        TABLE --> LOCATION
        TABLE --> FORMAT
        TABLE --> STATS
        
        DB --> ATHENA
        DB --> EMR
        DB --> SPECTRUM
        DB --> SAGEMAKER
        
        style DB fill:#FF9900,stroke:#232F3E,color:#fff
        style TABLE fill:#4B9EDB,stroke:#232F3E,color:#fff
        style ATHENA fill:#3FB34F,stroke:#232F3E,color:#fff
    """
    
    common.mermaid(catalog_diagram, height=550)
    
    # Interactive catalog explorer
    st.markdown("#### 🔍 Interactive Catalog Explorer")
    
    # Sample catalog data
    sample_databases = {
        'ecommerce_analytics': {
            'description': 'E-commerce analytics data',
            'tables': {
                'customer_orders': {
                    'location': 's3://ecommerce-data/orders/',
                    'format': 'parquet',
                    'columns': ['order_id', 'customer_id', 'order_date', 'total_amount'],
                    'partitions': ['year', 'month'],
                    'record_count': 1500000
                },
                'product_catalog': {
                    'location': 's3://ecommerce-data/products/', 
                    'format': 'json',
                    'columns': ['product_id', 'name', 'category', 'price'],
                    'partitions': ['category'],
                    'record_count': 50000
                }
            }
        },
        'web_analytics': {
            'description': 'Website analytics and user behavior',
            'tables': {
                'page_views': {
                    'location': 's3://web-logs/page-views/',
                    'format': 'csv',
                    'columns': ['timestamp', 'user_id', 'page_url', 'session_id'],
                    'partitions': ['year', 'month', 'day'],
                    'record_count': 10000000
                },
                'user_sessions': {
                    'location': 's3://web-logs/sessions/',
                    'format': 'parquet',
                    'columns': ['session_id', 'user_id', 'start_time', 'duration'],
                    'partitions': ['date'],
                    'record_count': 2000000
                }
            }
        }
    }
    
    col1, col2 = st.columns(2)
    
    with col1:
        selected_db = st.selectbox("Select Database:", list(sample_databases.keys()))
    
    with col2:
        if selected_db:
            selected_table = st.selectbox("Select Table:", list(sample_databases[selected_db]['tables'].keys()))
    
    # Display table details
    if selected_db and selected_table:
        table_info = sample_databases[selected_db]['tables'][selected_table]
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 📋 Table Details: {selected_table}
        **Database**: {selected_db}  
        **Location**: {table_info['location']}  
        **Format**: {table_info['format'].upper()}  
        **Record Count**: {table_info['record_count']:,}  
        **Partitions**: {', '.join(table_info['partitions'])}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Display schema
        st.markdown("###### 📄 Schema")
        schema_df = pd.DataFrame({
            'Column Name': table_info['columns'],
            'Data Type': ['string', 'bigint', 'timestamp', 'decimal(10,2)'][:len(table_info['columns'])],
            'Nullable': ['Yes'] * len(table_info['columns'])
        })
        st.dataframe(schema_df, use_container_width=True)
    
    # Catalog benefits
    st.markdown("#### ✅ Key Benefits")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Unified View
        - Single source of truth
        - Cross-service integration
        - Consistent metadata
        - Automatic synchronization
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚡ Query Optimization
        - Statistics-based planning
        - Partition pruning
        - Column pruning
        - Cost-based optimization
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛡️ Data Governance
        - Schema evolution tracking
        - Data lineage
        - Access control integration
        - Compliance support
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Code Examples")
    
    tab1, tab2 = st.tabs(["Catalog Management", "Query Integration"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Python Boto3 - Data Catalog management
import boto3

glue_client = boto3.client('glue')

# Create database
def create_database(name, description):
    response = glue_client.create_database(
        DatabaseInput={
            'Name': name,
            'Description': description,
            'Parameters': {
                'created_by': 'data_engineering_team',
                'environment': 'production'
            }
        }
    )
    return response

# Create table with detailed schema
def create_catalog_table(database_name, table_name, s3_location):
    table_input = {
        'Name': table_name,
        'Description': f'Table containing {table_name} data',
        'StorageDescriptor': {
            'Columns': [
                {
                    'Name': 'id',
                    'Type': 'bigint',
                    'Comment': 'Unique identifier'
                },
                {
                    'Name': 'timestamp',
                    'Type': 'timestamp',
                    'Comment': 'Event timestamp'
                },
                {
                    'Name': 'user_id',
                    'Type': 'string',
                    'Comment': 'User identifier'
                },
                {
                    'Name': 'event_data',
                    'Type': 'struct<action:string,value:double>',
                    'Comment': 'Nested event data'
                }
            ],
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            },
            'Compressed': True,
            'StoredAsSubDirectories': False
        },
        'PartitionKeys': [
            {
                'Name': 'year',
                'Type': 'string',
                'Comment': 'Partition by year'
            },
            {
                'Name': 'month', 
                'Type': 'string',
                'Comment': 'Partition by month'
            }
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'parquet',
            'compressionType': 'snappy',
            'typeOfData': 'file'
        }
    }
    
    response = glue_client.create_table(
        DatabaseName=database_name,
        TableInput=table_input
    )
    return response

# Add partitions to existing table
def add_partition(database_name, table_name, partition_values):
    partition_input = {
        'Values': partition_values,  # e.g., ['2024', '01']
        'StorageDescriptor': {
            'Location': f's3://my-bucket/data/year={partition_values[0]}/month={partition_values[1]}/',
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            }
        }
    }
    
    response = glue_client.create_partition(
        DatabaseName=database_name,
        TableName=table_name,
        PartitionInput=partition_input
    )
    return response

# Example usage
create_database('analytics_catalog', 'Analytics data catalog')
create_catalog_table('analytics_catalog', 'user_events', 's3://analytics-data/events/')
add_partition('analytics_catalog', 'user_events', ['2024', '01'])
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Athena queries using Glue Data Catalog
import boto3
import time

athena_client = boto3.client('athena')

def execute_athena_query(query, database, s3_output_location):
    """Execute Athena query using Glue Data Catalog"""
    
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': s3_output_location}
    )
    
    query_execution_id = response['QueryExecutionId']
    
    # Wait for query completion
    while True:
        result = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        
        status = result['QueryExecution']['Status']['State']
        
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
            
        time.sleep(1)
    
    if status == 'SUCCEEDED':
        # Get query results
        results = athena_client.get_query_results(
            QueryExecutionId=query_execution_id
        )
        return results
    else:
        error = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"Query failed: {error}")

# Example queries leveraging Data Catalog
queries = {
    'table_info': """
        DESCRIBE analytics_catalog.user_events;
    """,
    
    'partition_info': """
        SHOW PARTITIONS analytics_catalog.user_events;
    """,
    
    'aggregated_analytics': """
        SELECT 
            year,
            month,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as unique_users,
            AVG(event_data.value) as avg_value
        FROM analytics_catalog.user_events
        WHERE year = '2024'
        GROUP BY year, month
        ORDER BY month;
    """,
    
    'cross_table_join': """
        SELECT 
            e.user_id,
            COUNT(e.id) as event_count,
            p.name as product_name,
            p.category
        FROM analytics_catalog.user_events e
        JOIN analytics_catalog.product_catalog p
            ON e.event_data.product_id = p.product_id
        WHERE e.year = '2024' 
            AND e.month = '01'
        GROUP BY e.user_id, p.name, p.category
        HAVING COUNT(e.id) > 10
        ORDER BY event_count DESC;
    """
}

# Execute queries
for query_name, query_sql in queries.items():
    print(f"Executing {query_name}...")
    
    try:
        results = execute_athena_query(
            query=query_sql,
            database='analytics_catalog',
            s3_output_location='s3://athena-results-bucket/queries/'
        )
        print(f"✅ {query_name} completed successfully")
        
    except Exception as e:
        print(f"❌ {query_name} failed: {str(e)}")

# EMR integration with Data Catalog
spark_sql_example = """
// Spark SQL on EMR using Glue Data Catalog
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
    .appName("GlueCatalogExample")
    .enableHiveSupport()
    .getOrCreate()

// Use Glue Data Catalog as Hive Metastore
spark.sql("USE analytics_catalog")

// Query table from catalog
val events = spark.sql("""
    SELECT user_id, event_data.action, COUNT(*) as action_count
    FROM user_events 
    WHERE year = '2024' AND month = '01'
    GROUP BY user_id, event_data.action
""")

events.show()

// Write results back to catalog
events.write
    .mode("overwrite")
    .option("path", "s3://analytics-results/user_actions/")
    .saveAsTable("user_action_summary")
"""

print("Spark SQL example for EMR integration:")
print(spark_sql_example)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def schema_registry_tab():
    """Content for AWS Glue Schema Registry tab"""
    st.markdown("## 🔄 AWS Glue Schema Registry")
    st.markdown("*Centrally discover, control, and evolve data stream schemas*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    AWS Glue Schema Registry provides a centralized repository for managing and validating schemas for streaming data. 
    It helps ensure data quality and enables safe schema evolution across your data streaming applications.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Schema Registry architecture
    registry_diagram = """
    graph TB
        subgraph "Data Producers"
            KAFKA_PROD[Kafka Producer]
            KINESIS_PROD[Kinesis Producer]
            MSK_PROD[MSK Producer]
        end
        
        subgraph "Schema Registry"
            REGISTRY[Glue Schema Registry<br/>📝 Schema Storage]
            AVRO[Avro Schemas]
            JSON[JSON Schemas]
            COMPAT[Compatibility Rules]
        end
        
        subgraph "Data Consumers"
            KAFKA_CONS[Kafka Consumer]
            KINESIS_CONS[Kinesis Consumer]
            FLINK[Flink Applications]
            LAMBDA[Lambda Functions]
        end
        
        subgraph "Benefits"
            VALIDATE[Schema Validation<br/>✅ Data Quality]
            EVOLVE[Schema Evolution<br/>🔄 Safe Changes]
            COMPRESS[Data Compression<br/>📦 Cost Savings]
        end
        
        KAFKA_PROD --> REGISTRY
        KINESIS_PROD --> REGISTRY
        MSK_PROD --> REGISTRY
        
        REGISTRY --> AVRO
        REGISTRY --> JSON
        REGISTRY --> COMPAT
        
        REGISTRY --> KAFKA_CONS
        REGISTRY --> KINESIS_CONS
        REGISTRY --> FLINK
        REGISTRY --> LAMBDA
        
        REGISTRY --> VALIDATE
        REGISTRY --> EVOLVE
        REGISTRY --> COMPRESS
        
        style REGISTRY fill:#FF9900,stroke:#232F3E,color:#fff
        style AVRO fill:#4B9EDB,stroke:#232F3E,color:#fff
        style JSON fill:#3FB34F,stroke:#232F3E,color:#fff
        style VALIDATE fill:#232F3E,stroke:#FF9900,color:#fff
    """
    
    st.markdown("#### 🏗️ Schema Registry Architecture")
    common.mermaid(registry_diagram, height=500)
    
    # Schema compatibility modes
    st.markdown("#### 🔧 Schema Compatibility Modes")
    
    compatibility_modes = {
        'BACKWARD': {
            'description': 'New schema can read data written with previous schema',
            'use_case': 'Consumer applications need to handle old data',
            'example': 'Adding optional fields, removing fields'
        },
        'FORWARD': {
            'description': 'Previous schema can read data written with new schema', 
            'use_case': 'Producer applications upgrade before consumers',
            'example': 'Removing optional fields, adding fields with defaults'
        },
        'FULL': {
            'description': 'Both backward and forward compatibility',
            'use_case': 'Maximum flexibility for schema changes',
            'example': 'Adding/removing optional fields only'
        },
        'BACKWARD_TRANSITIVE': {
            'description': 'New schema compatible with all previous versions',
            'use_case': 'Long-term data retention with evolving schema',
            'example': 'Continuous addition of optional fields'
        },
        'FORWARD_TRANSITIVE': {
            'description': 'All previous schemas work with new schema',
            'use_case': 'Gradual consumer updates',
            'example': 'Systematic removal of deprecated fields'
        },
        'FULL_TRANSITIVE': {
            'description': 'Full compatibility across all versions',
            'use_case': 'Strict schema governance',
            'example': 'Very conservative schema changes'
        }
    }
    
    selected_mode = st.selectbox("Select Compatibility Mode:", list(compatibility_modes.keys()))
    
    if selected_mode:
        mode_info = compatibility_modes[selected_mode]
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### {selected_mode} Compatibility
        **Description**: {mode_info['description']}  
        **Use Case**: {mode_info['use_case']}  
        **Example**: {mode_info['example']}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive schema evolution simulator
    st.markdown("#### 🔄 Schema Evolution Simulator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("###### Original Schema (v1)")
        original_schema = {
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "timestamp", "type": "long"}
            ]
        }
        st.json(original_schema)
    
    with col2:
        st.markdown("###### Evolution Options")
        evolution_type = st.selectbox("Select Evolution:", [
            "Add optional field",
            "Add required field", 
            "Remove field",
            "Change field type",
            "Rename field"
        ])
    
    # Show evolved schema based on selection
    evolved_schemas = {
        "Add optional field": {
            "type": "record",
            "name": "UserEvent", 
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "session_id", "type": ["null", "string"], "default": None}
            ]
        },
        "Add required field": {
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "device_type", "type": "string"}
            ]
        },
        "Remove field": {
            "type": "record", 
            "name": "UserEvent",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "timestamp", "type": "long"}
            ]
        },
        "Change field type": {
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "event_type", "type": "string"}, 
                {"name": "timestamp", "type": "string"}
            ]
        },
        "Rename field": {
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "userId", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "timestamp", "type": "long"}
            ]
        }
    }
    
    if evolution_type in evolved_schemas:
        st.markdown("###### Evolved Schema (v2)")
        st.json(evolved_schemas[evolution_type])
        
        # Compatibility check
        compatibility_results = {
            "Add optional field": ("✅ BACKWARD Compatible", "Old consumers can read new data"),
            "Add required field": ("❌ NOT BACKWARD Compatible", "Old consumers missing required field"),
            "Remove field": ("✅ FORWARD Compatible", "New consumers ignore missing field"),
            "Change field type": ("❌ NOT Compatible", "Type mismatch causes errors"),
            "Rename field": ("❌ NOT Compatible", "Field name mismatch")
        }
        
        result, explanation = compatibility_results[evolution_type]
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown(f"""
        #### Compatibility Analysis
        **Result**: {result}  
        **Explanation**: {explanation}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Benefits section
    st.markdown("#### ✅ Key Benefits")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛡️ Data Quality
        - Schema validation at source
        - Prevent malformed data
        - Centralized governance
        - Error detection early
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 💰 Cost Optimization
        - Data compression
        - Reduced storage costs
        - Efficient serialization
        - Network bandwidth savings
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Safe Evolution
        - Compatibility checking
        - Version management
        - Backwards compatibility
        - Gradual rollouts
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Code Examples")
    
    tab1, tab2 = st.tabs(["Schema Registry Setup", "Producer/Consumer Code"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Python Boto3 - Schema Registry management
import boto3
import json

glue_client = boto3.client('glue')

# Create schema registry
def create_schema_registry(registry_name, description):
    response = glue_client.create_registry(
        RegistryName=registry_name,
        Description=description,
        Tags={
            'Environment': 'Production',
            'Team': 'DataEngineering'
        }
    )
    return response

# Create schema
def create_schema(registry_name, schema_name, schema_definition, data_format='AVRO'):
    response = glue_client.create_schema(
        RegistryId={'RegistryName': registry_name},
        SchemaName=schema_name,
        DataFormat=data_format,
        Compatibility='BACKWARD',
        SchemaDefinition=json.dumps(schema_definition),
        Description=f'Schema for {schema_name} events',
        Tags={
            'Version': '1.0',
            'Owner': 'DataTeam'
        }
    )
    return response

# User event schema definition
user_event_schema = {
    "type": "record",
    "name": "UserEvent",
    "namespace": "com.company.events",
    "fields": [
        {
            "name": "user_id",
            "type": "string",
            "doc": "Unique user identifier"
        },
        {
            "name": "event_type", 
            "type": {
                "type": "enum",
                "name": "EventType",
                "symbols": ["CLICK", "VIEW", "PURCHASE", "SEARCH"]
            },
            "doc": "Type of user event"
        },
        {
            "name": "timestamp",
            "type": "long",
            "logicalType": "timestamp-millis",
            "doc": "Event timestamp in milliseconds"
        },
        {
            "name": "properties",
            "type": {
                "type": "map",
                "values": "string"
            },
            "doc": "Additional event properties"
        },
        {
            "name": "session_id",
            "type": ["null", "string"],
            "default": None,
            "doc": "Optional session identifier"
        }
    ]
}

# Create registry and schema
registry_response = create_schema_registry(
    'user-events-registry',
    'Registry for user event schemas'
)

schema_response = create_schema(
    'user-events-registry',
    'UserEvent',
    user_event_schema
)

print(f"Created schema: {schema_response['SchemaArn']}")

# Update schema with new version
def update_schema(registry_name, schema_name, new_schema_definition):
    response = glue_client.register_schema_version(
        SchemaId={
            'RegistryName': registry_name,
            'SchemaName': schema_name
        },
        SchemaDefinition=json.dumps(new_schema_definition)
    )
    return response

# Evolve schema - add optional field
evolved_schema = user_event_schema.copy()
evolved_schema['fields'].append({
    "name": "device_info",
    "type": ["null", {
        "type": "record",
        "name": "DeviceInfo",
        "fields": [
            {"name": "device_type", "type": "string"},
            {"name": "os_version", "type": "string"}
        ]
    }],
    "default": None,
    "doc": "Optional device information"
})

# Register new version
version_response = update_schema(
    'user-events-registry',
    'UserEvent', 
    evolved_schema
)

print(f"Created schema version: {version_response['VersionNumber']}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Kafka Producer with Schema Registry integration
from kafka import KafkaProducer
from aws_glue_schema_registry import GlueSchemaRegistryClient, DataAndSchema
import json
import avro.schema
import avro.io
import io

# Initialize Glue Schema Registry client
glue_client = GlueSchemaRegistryClient(
    region_name='us-west-2',
    registry_name='user-events-registry'
)

# Create Kafka producer with schema registry serializer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: glue_client.serialize(v)
)

# Sample user event data
user_event_data = {
    "user_id": "user123",
    "event_type": "PURCHASE",
    "timestamp": 1640995200000,  # 2022-01-01 00:00:00
    "properties": {
        "product_id": "prod456",
        "amount": "99.99",
        "currency": "USD"
    },
    "session_id": "session789"
}

# Create DataAndSchema object
data_and_schema = DataAndSchema(
    data=user_event_data,
    schema_name='UserEvent'
)

# Send message with schema validation
try:
    future = producer.send('user-events', value=data_and_schema)
    record_metadata = future.get(timeout=10)
    print(f"Message sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
except Exception as e:
    print(f"Failed to send message: {e}")

producer.close()

# Kafka Consumer with Schema Registry integration
from kafka import KafkaConsumer

# Create consumer with schema registry deserializer
consumer = KafkaConsumer(
    'user-events',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda v: glue_client.deserialize(v),
    auto_offset_reset='earliest',
    group_id='user-events-consumer-group'
)

# Consume messages
for message in consumer:
    try:
        # Deserialize using schema registry
        deserialized_data = message.value
        
        print(f"Received event:")
        print(f"  User ID: {deserialized_data['user_id']}")
        print(f"  Event Type: {deserialized_data['event_type']}")
        print(f"  Timestamp: {deserialized_data['timestamp']}")
        print(f"  Properties: {deserialized_data['properties']}")
        
        # Handle optional fields gracefully
        if 'session_id' in deserialized_data and deserialized_data['session_id']:
            print(f"  Session ID: {deserialized_data['session_id']}")
            
        if 'device_info' in deserialized_data and deserialized_data['device_info']:
            print(f"  Device: {deserialized_data['device_info']}")
            
    except Exception as e:
        print(f"Error processing message: {e}")

# Kinesis Producer with Schema Registry
import boto3
from aws_glue_schema_registry.serializers import GlueKinesisSerializer

kinesis_client = boto3.client('kinesis', region_name='us-west-2')

# Create Kinesis serializer
kinesis_serializer = GlueKinesisSerializer(
    glue_client,
    registry_name='user-events-registry'
)

# Send data to Kinesis with schema validation
kinesis_data = {
    "user_id": "user456", 
    "event_type": "CLICK",
    "timestamp": 1640995260000,
    "properties": {
        "page_url": "/products",
        "referrer": "search"
    }
}

serialized_data = kinesis_serializer.serialize(
    data=kinesis_data,
    schema_name='UserEvent'
)

response = kinesis_client.put_record(
    StreamName='user-events-stream',
    Data=serialized_data,
    PartitionKey=kinesis_data['user_id']
)

print(f"Sent to Kinesis: {response['SequenceNumber']}")

# Lambda function for processing with schema registry
import json
import boto3
from aws_glue_schema_registry.deserializers import GlueKinesisDeserializer

def lambda_handler(event, context):
    """Process Kinesis events with schema registry deserialization"""
    
    # Initialize deserializer
    glue_deserializer = GlueKinesisDeserializer(
        glue_client,
        registry_name='user-events-registry'
    )
    
    processed_records = 0
    
    for record in event['Records']:
        try:
            # Decode and deserialize Kinesis data
            kinesis_data = record['kinesis']['data']
            deserialized_event = glue_deserializer.deserialize(kinesis_data)
            
            # Process the event
            user_id = deserialized_event['user_id']
            event_type = deserialized_event['event_type']
            
            print(f"Processing {event_type} event for user {user_id}")
            
            # Your business logic here
            # e.g., update user analytics, trigger recommendations, etc.
            
            processed_records += 1
            
        except Exception as e:
            print(f"Error processing record: {e}")
            # Handle error - could send to DLQ, log, etc.
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Processed {processed_records} records',
            'total_records': len(event['Records'])
        })
    }
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def crawlers_tab():
    """Content for AWS Glue Crawlers tab"""
    st.markdown("## 🕷️ AWS Glue Crawlers")
    st.markdown("*Connects to a data store, progresses through a prioritized list of classifiers to extract the schema of your data*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    AWS Glue crawlers automatically discover and catalog your data by connecting to data stores, 
    inferring schemas, and populating the AWS Glue Data Catalog with table definitions and metadata.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Crawler workflow
    st.markdown("#### 🔄 Crawler Workflow")
    common.mermaid(create_crawler_workflow_mermaid(), height=1000)
    
    # Interactive crawler simulator
    st.markdown("#### 🎮 Interactive Crawler Simulator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("###### Configure Crawler")
        crawler_name = st.text_input("Crawler Name", value="sales-data-crawler")
        data_source = st.selectbox("Data Source", [
            "s3://sales-data-bucket/",
            "RDS MySQL Database",
            "Redshift Cluster", 
            "DynamoDB Table"
        ])
        
        if data_source.startswith("s3://"):
            file_format = st.selectbox("File Format", ["CSV", "JSON", "Parquet", "Avro", "ORC"])
            include_path = st.text_input("Include Path", value="sales/year=2024/")
            exclude_patterns = st.text_input("Exclude Patterns", value="*.tmp,*_backup*")
    
    with col2:
        st.markdown("###### Crawler Settings")
        schedule_type = st.selectbox("Schedule", ["On Demand", "Hourly", "Daily", "Weekly", "Custom Cron"])
        
        if schedule_type == "Custom Cron":
            cron_expression = st.text_input("Cron Expression", value="0 2 * * *")  # Daily at 2 AM
        
        table_prefix = st.text_input("Table Prefix", value="sales_")
        configuration = st.selectbox("Schema Change Policy", [
            "Update the table definition in the data catalog",
            "Add new columns only", 
            "Ignore the change and don't update the table"
        ])
    
    # Simulate crawler run
    if st.button("🚀 Run Crawler Simulation"):
        with st.spinner("Running crawler..."):
            # Simulate crawler progress
            progress_bar = st.progress(0)
            
            steps = [
                ("Connecting to data source", 20),
                ("Scanning files and inferring schema", 40), 
                ("Running classifiers", 60),
                ("Creating table definitions", 80),
                ("Updating Data Catalog", 100)
            ]
            
            for step, progress in steps:
                st.write(f"⏳ {step}...")
                progress_bar.progress(progress)
                
        # Show results
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Crawler Run Complete
        **Crawler**: {crawler_name}  
        **Data Source**: {data_source}  
        **Tables Created**: 3  
        **Partitions Added**: 12  
        **Runtime**: 2 minutes 34 seconds
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Sample discovered tables
        st.markdown("###### 📋 Discovered Tables")
        discovered_tables = pd.DataFrame({
            'Table Name': [f'{table_prefix}orders', f'{table_prefix}customers', f'{table_prefix}products'],
            'Record Count': ['1,250,000', '50,000', '12,500'],
            'Columns': ['8', '12', '15'],
            'Partitions': ['4', '0', '2'],
            'Classification': [file_format.lower() if data_source.startswith("s3://") else 'relational', 
                             file_format.lower() if data_source.startswith("s3://") else 'relational',
                             file_format.lower() if data_source.startswith("s3://") else 'relational']
        })
        st.dataframe(discovered_tables, use_container_width=True)
    
    # Crawler types and classifiers
    st.markdown("#### 🔍 Built-in Classifiers")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📄 File Format Classifiers
        - **CSV**: Comma-separated values with headers
        - **JSON**: JavaScript Object Notation
        - **Parquet**: Columnar storage format
        - **Avro**: Row-based serialization format
        - **ORC**: Optimized Row Columnar format
        - **XML**: Extensible Markup Language
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗄️ Database Classifiers
        - **Relational**: Standard SQL tables
        - **DynamoDB**: NoSQL document store
        - **MongoDB**: Document database
        - **Custom Classifiers**: User-defined rules
        - **Grok Patterns**: Log file parsing
        - **Regex**: Pattern matching
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Best practices
    st.markdown("#### 🏆 Crawler Best Practices")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚡ Performance
        - Use include/exclude patterns
        - Limit crawler scope
        - Run during low-traffic periods
        - Monitor crawler metrics
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 💰 Cost Optimization
        - Schedule appropriately
        - Sample large datasets
        - Use incremental crawling
        - Monitor DPU consumption
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛡️ Data Quality
        - Validate schema consistency
        - Handle schema evolution
        - Set up error handling
        - Monitor data quality metrics
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Code Examples")
    
    tab1, tab2 = st.tabs(["Crawler Creation", "Monitoring & Management"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Python Boto3 - Create and configure AWS Glue crawler
import boto3

glue_client = boto3.client('glue')

def create_s3_crawler(crawler_name, s3_path, database_name, table_prefix=""):
    """Create a crawler for S3 data source"""
    
    crawler_config = {
        'Name': crawler_name,
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'DatabaseName': database_name,
        'Description': f'Crawler for {s3_path}',
        'Targets': {
            'S3Targets': [
                {
                    'Path': s3_path,
                    'Exclusions': [
                        '**/_temporary/**',
                        '**/*.tmp',
                        '**/*_backup*',
                        '**/.spark/**'
                    ]
                }
            ]
        },
        'TablePrefix': table_prefix,
        'SchemaChangePolicy': {
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'LOG'
        },
        'RecrawlPolicy': {
            'RecrawlBehavior': 'CRAWL_EVERYTHING'
        },
        'LineageConfiguration': {
            'CrawlerLineageSettings': 'ENABLE'
        },
        'Configuration': """{
            "Version": 1.0,
            "CrawlerOutput": {
                "Partitions": {
                    "AddOrUpdateBehavior": "InheritFromTable"
                },
                "Tables": {
                    "AddOrUpdateBehavior": "MergeNewColumns"
                }
            }
        }"""
    }
    
    response = glue_client.create_crawler(**crawler_config)
    return response

def create_database_crawler(crawler_name, connection_name, database_name):
    """Create a crawler for database source"""
    
    crawler_config = {
        'Name': crawler_name,
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'DatabaseName': database_name,
        'Description': f'Crawler for database connection {connection_name}',
        'Targets': {
            'JdbcTargets': [
                {
                    'ConnectionName': connection_name,
                    'Path': 'sales_db/%',  # Crawl all tables in sales_db
                    'Exclusions': [
                        'temp_%',
                        'backup_%',
                        'staging_%'
                    ]
                }
            ]
        },
        'SchemaChangePolicy': {
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        },
        'RecrawlPolicy': {
            'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'
        }
    }
    
    response = glue_client.create_crawler(**crawler_config)
    return response

def create_custom_classifier():
    """Create a custom classifier for specific data formats"""
    
    # Create a Grok classifier for log files
    grok_classifier = {
        'Name': 'apache-access-logs',
        'Classification': 'apache-access-logs',
        'GrokPattern': '%{COMMONAPACHELOG}',
        'CustomPatterns': """
            CUSTOM_TIMESTAMP %{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{ISO8601_TIMEZONE}
        """
    }
    
    response = glue_client.create_classifier(
        GrokClassifier=grok_classifier
    )
    
    # Create a JSON classifier
    json_classifier = {
        'Name': 'custom-json-classifier',
        'JsonPath': '$.records[*]'  # Extract records array from JSON
    }
    
    response2 = glue_client.create_classifier(
        JsonClassifier=json_classifier
    )
    
    return response, response2

# Create crawlers for different use cases
# S3 data lake crawler
s3_response = create_s3_crawler(
    crawler_name='ecommerce-sales-crawler',
    s3_path='s3://ecommerce-data-lake/sales/',
    database_name='ecommerce_analytics',
    table_prefix='sales_'
)

print(f"Created S3 crawler: {s3_response}")

# Database crawler
db_response = create_database_crawler(
    crawler_name='rds-inventory-crawler',
    connection_name='rds-mysql-connection',
    database_name='operational_data'
)

print(f"Created database crawler: {db_response}")

# Custom classifiers
classifier_responses = create_custom_classifier()
print(f"Created custom classifiers: {classifier_responses}")

# Add crawler to existing database
def add_crawler_schedule(crawler_name, schedule_expression):
    """Add schedule to existing crawler"""
    
    response = glue_client.update_crawler(
        Name=crawler_name,
        Schedule=schedule_expression  # e.g., "cron(0 2 * * ? *)" for daily at 2 AM
    )
    return response

# Schedule crawler to run daily at 2 AM UTC
schedule_response = add_crawler_schedule(
    'ecommerce-sales-crawler',
    'cron(0 2 * * ? *)'
)

print(f"Added schedule to crawler: {schedule_response}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Crawler monitoring and management
import boto3
import time
from datetime import datetime, timedelta

glue_client = boto3.client('glue')
cloudwatch = boto3.client('cloudwatch')

def start_crawler_and_monitor(crawler_name):
    """Start crawler and monitor its progress"""
    
    try:
        # Start the crawler
        response = glue_client.start_crawler(Name=crawler_name)
        print(f"Started crawler: {crawler_name}")
        
        # Monitor crawler status
        while True:
            crawler_info = glue_client.get_crawler(Name=crawler_name)
            state = crawler_info['Crawler']['State']
            
            print(f"Crawler state: {state}")
            
            if state == 'READY':
                # Get last run info
                last_crawl = crawler_info['Crawler'].get('LastCrawl', {})
                if last_crawl:
                    print(f"Last crawl status: {last_crawl.get('Status')}")
                    print(f"Tables updated: {last_crawl.get('TablesUpdated', 0)}")
                    print(f"Tables created: {last_crawl.get('TablesCreated', 0)}")
                    print(f"Tables deleted: {last_crawl.get('TablesDeleted', 0)}")
                break
            elif state == 'STOPPING':
                print("Crawler is stopping...")
                break
            
            time.sleep(30)  # Check every 30 seconds
            
    except Exception as e:
        print(f"Error starting crawler: {e}")

def get_crawler_metrics(crawler_name, hours_back=24):
    """Get CloudWatch metrics for crawler"""
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours_back)
    
    # Get DPU hours consumed
    dpu_response = cloudwatch.get_metric_statistics(
        Namespace='AWS/Glue',
        MetricName='glue.driver.aggregate.numCompletedTasks',
        Dimensions=[
            {
                'Name': 'JobName',
                'Value': crawler_name
            },
            {
                'Name': 'JobRunId',
                'Value': 'ALL'
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Sum']
    )
    
    # Get success/failure rates
    success_response = cloudwatch.get_metric_statistics(
        Namespace='AWS/Glue',
        MetricName='glue.driver.aggregate.numCompletedStages',
        Dimensions=[
            {
                'Name': 'JobName', 
                'Value': crawler_name
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Sum']
    )
    
    return {
        'dpu_metrics': dpu_response['Datapoints'],
        'success_metrics': success_response['Datapoints']
    }

def manage_crawler_errors(crawler_name):
    """Handle crawler errors and troubleshooting"""
    
    try:
        # Get crawler runs history
        runs_response = glue_client.get_crawler_metrics(
            CrawlerNameList=[crawler_name]
        )
        
        for metric in runs_response['CrawlerMetricsList']:
            print(f"\n=== Crawler Metrics for {metric['CrawlerName']} ===")
            print(f"Tables created: {metric.get('TablesCreated', 0)}")
            print(f"Tables updated: {metric.get('TablesUpdated', 0)}")
            print(f"Tables deleted: {metric.get('TablesDeleted', 0)}")
            print(f"Still estimating: {metric.get('StillEstimating', False)}")
            print(f"Last runtime: {metric.get('LastRuntimeSeconds', 0)} seconds")
            print(f"Median runtime: {metric.get('MedianRuntimeSeconds', 0)} seconds")
            
            if metric.get('TablesCreated', 0) == 0 and metric.get('TablesUpdated', 0) == 0:
                print("⚠️  Warning: No tables created or updated")
                print("   - Check data source permissions")
                print("   - Verify S3 path or database connection")
                print("   - Review crawler logs in CloudWatch")
    
    except Exception as e:
        print(f"Error getting crawler metrics: {e}")

def optimize_crawler_performance(crawler_name):
    """Optimize crawler performance"""
    
    # Get current crawler configuration
    crawler = glue_client.get_crawler(Name=crawler_name)['Crawler']
    
    optimization_tips = []
    
    # Check for include/exclude patterns
    s3_targets = crawler.get('Targets', {}).get('S3Targets', [])
    for target in s3_targets:
        if not target.get('Exclusions'):
            optimization_tips.append("Add exclusion patterns to skip temporary files")
    
    # Check schedule frequency
    schedule = crawler.get('Schedule')
    if schedule and 'cron(0 * * * ? *)' in schedule:  # Hourly
        optimization_tips.append("Consider reducing crawler frequency if data doesn't change hourly")
    
    # Check recrawl policy
    recrawl_policy = crawler.get('RecrawlPolicy', {}).get('RecrawlBehavior')
    if recrawl_policy == 'CRAWL_EVERYTHING':
        optimization_tips.append("Use CRAWL_NEW_FOLDERS_ONLY for better performance on large datasets")
    
    return optimization_tips

# Example usage
crawler_name = 'ecommerce-sales-crawler'

# Start and monitor crawler
start_crawler_and_monitor(crawler_name)

# Get performance metrics
metrics = get_crawler_metrics(crawler_name, hours_back=48)
print(f"Crawler metrics: {metrics}")

# Check for errors and issues
manage_crawler_errors(crawler_name)

# Get optimization recommendations  
tips = optimize_crawler_performance(crawler_name)
if tips:
    print("\n📈 Optimization Recommendations:")
    for tip in tips:
        print(f"  • {tip}")

# Bulk crawler management
def manage_multiple_crawlers(crawler_names, action='status'):
    """Manage multiple crawlers at once"""
    
    results = {}
    
    for crawler_name in crawler_names:
        try:
            if action == 'start':
                response = glue_client.start_crawler(Name=crawler_name)
                results[crawler_name] = 'started'
            elif action == 'stop':
                response = glue_client.stop_crawler(Name=crawler_name)
                results[crawler_name] = 'stopped'
            elif action == 'status':
                crawler = glue_client.get_crawler(Name=crawler_name)
                results[crawler_name] = crawler['Crawler']['State']
                
        except Exception as e:
            results[crawler_name] = f'error: {str(e)}'
    
    return results

# Manage multiple crawlers
crawler_list = ['sales-crawler', 'inventory-crawler', 'customer-crawler']
statuses = manage_multiple_crawlers(crawler_list, 'status')

print("\n📊 Crawler Status Summary:")
for crawler, status in statuses.items():
    print(f"  {crawler}: {status}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def table_partitions_tab():
    """Content for AWS Glue Table Partitions tab"""
    st.markdown("## 📂 AWS Glue - Table Partitions")
    st.markdown("*AWS Glue table definition of an S3 folder can describe partitioned tables*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    Table partitions in AWS Glue allow you to organize your data in S3 based on specific keys (like date, region, or category). 
    This enables query optimization by allowing services like Athena to scan only relevant partitions.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Partition diagram
    st.markdown("#### 🗂️ Partition Structure")
    common.mermaid(create_partition_diagram_mermaid(), height=500)
    
    # Interactive partition designer
    st.markdown("#### 🎨 Interactive Partition Designer")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("###### Design Your Partition Strategy")
        data_type = st.selectbox("Data Type", [
            "Sales Data", "Log Files", "User Events", "IoT Sensor Data", "Financial Transactions"
        ])
        
        partition_keys = st.multiselect("Partition Keys (in order)", [
            "year", "month", "day", "hour", "region", "country", "category", 
            "department", "status", "device_type", "customer_segment"
        ], default=["year", "month"])
        
        data_volume = st.selectbox("Daily Data Volume", [
            "< 1 GB", "1-10 GB", "10-100 GB", "100 GB - 1 TB", "> 1 TB"
        ])
    
    with col2:
        st.markdown("###### S3 Path Preview")
        if partition_keys:
            sample_paths = []
            base_path = f"s3://my-data-bucket/{data_type.lower().replace(' ', '-')}/"
            
            # Generate sample partition paths
            partition_combinations = [
                {"year": "2024", "month": "01", "day": "15", "hour": "14", 
                 "region": "us-east-1", "category": "electronics"},
                {"year": "2024", "month": "02", "day": "01", "hour": "09", 
                 "region": "us-west-2", "category": "books"},
                {"year": "2024", "month": "02", "day": "15", "hour": "16", 
                 "region": "eu-west-1", "category": "clothing"}
            ]
            
            for combo in partition_combinations[:3]:
                path_parts = []
                for key in partition_keys:
                    if key in combo:
                        path_parts.append(f"{key}={combo[key]}")
                
                if path_parts:
                    full_path = base_path + "/".join(path_parts) + "/"
                    sample_paths.append(full_path)
            
            for path in sample_paths:
                st.code(path, language="text")
    
    # Partition benefits analysis
    if partition_keys:
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        
        # Calculate estimated benefits
        partition_count = len(partition_keys)
        if data_volume == "< 1 GB":
            scan_reduction = min(90, partition_count * 25)
            cost_savings = min(85, partition_count * 20)
        elif data_volume == "> 1 TB":
            scan_reduction = min(95, partition_count * 30)
            cost_savings = min(90, partition_count * 25)
        else:
            scan_reduction = min(90, partition_count * 22)
            cost_savings = min(80, partition_count * 18)
        
        st.markdown(f"""
        ### 📊 Estimated Performance Benefits
        **Partition Strategy**: {' → '.join(partition_keys)}  
        **Data Scan Reduction**: ~{scan_reduction}%  
        **Query Cost Savings**: ~{cost_savings}%  
        **Partition Count**: ~{2**(min(len(partition_keys), 4))} partitions per month
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Partition best practices
    st.markdown("#### 🏆 Partitioning Best Practices")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📏 Partition Size
        - **Target**: 64 MB - 1 GB per partition
        - **Too Small**: Excessive metadata overhead
        - **Too Large**: Poor query performance
        - **Monitor**: Partition sizes regularly
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔑 Key Selection
        - **Most Selective First**: High cardinality
        - **Query Patterns**: Match filtering patterns
        - **Date-based**: Usually most effective
        - **Avoid Over-partitioning**: < 10,000 partitions
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚀 Performance Tips
        - **Partition Projection**: Athena optimization
        - **Partition Pruning**: Leverage in queries
        - **MSCK REPAIR**: Sync partitions
        - **Regular Cleanup**: Remove old partitions
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Partition troubleshooting
    st.markdown("#### 🛠️ Common Partition Issues & Solutions")
    
    issues_solutions = {
        "HIVE_PARTITION_SCHEMA_MISMATCH": {
            "description": "Schema differs between table and partition",
            "causes": ["Schema evolution", "Data type changes", "Column additions/removals"],
            "solutions": [
                "Drop and recreate partition",
                "Use MSCK REPAIR TABLE",
                "Update table schema to match partition",
                "Enable schema evolution in crawlers"
            ]
        },
        "Too Many Small Partitions": {
            "description": "Excessive number of small partitions causing overhead",
            "causes": ["Fine-grained partitioning", "Low data volume", "Poor key selection"],
            "solutions": [
                "Reduce partition granularity",
                "Combine data into larger files",
                "Use fewer partition keys",
                "Implement partition compaction"
            ]
        },
        "Partition Not Found": {
            "description": "Query references non-existent partition",
            "causes": ["Missing MSCK REPAIR", "Partition not registered", "Incorrect path"],
            "solutions": [
                "Run MSCK REPAIR TABLE",
                "Add partitions manually",
                "Update crawler to scan partitions",
                "Verify S3 path structure"
            ]
        }
    }
    
    selected_issue = st.selectbox("Select Issue to Troubleshoot:", list(issues_solutions.keys()))
    
    if selected_issue:
        issue_info = issues_solutions[selected_issue]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"#### 🚨 {selected_issue}")
            st.markdown(f"**Description**: {issue_info['description']}")
            st.markdown("**Common Causes**:")
            for cause in issue_info['causes']:
                st.markdown(f"• {cause}")
        
        with col2:
            st.markdown("###### ✅ Solutions")
            for solution in issue_info['solutions']:
                st.markdown(f"• {solution}")
    
    # Code examples
    st.markdown("#### 💻 Code Examples")
    
    tab1, tab2, tab3 = st.tabs(["Creating Partitioned Tables", "Managing Partitions", "Query Optimization"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create partitioned table in AWS Glue Data Catalog
import boto3

glue_client = boto3.client('glue')

def create_partitioned_table(database_name, table_name, s3_location, partition_keys):
    """Create a partitioned table in Glue Data Catalog"""
    
    # Define table schema
    columns = [
        {'Name': 'transaction_id', 'Type': 'string'},
        {'Name': 'user_id', 'Type': 'string'},
        {'Name': 'amount', 'Type': 'decimal(10,2)'},
        {'Name': 'currency', 'Type': 'string'},
        {'Name': 'timestamp', 'Type': 'timestamp'},
        {'Name': 'merchant_id', 'Type': 'string'},
        {'Name': 'category', 'Type': 'string'}
    ]
    
    # Define partition columns
    partition_columns = []
    for key in partition_keys:
        partition_columns.append({
            'Name': key,
            'Type': 'string',
            'Comment': f'Partition by {key}'
        })
    
    table_input = {
        'Name': table_name,
        'Description': f'Partitioned table for {table_name}',
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            },
            'Compressed': True,
            'Parameters': {
                'classification': 'parquet'
            }
        },
        'PartitionKeys': partition_columns,
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'has_encrypted_data': 'false',
            'projection.enabled': 'true',  # Enable partition projection for Athena
            'projection.year.type': 'integer',
            'projection.year.range': '2020,2030',
            'projection.month.type': 'integer', 
            'projection.month.range': '1,12',
            'projection.month.digits': '2',
            'projection.day.type': 'integer',
            'projection.day.range': '1,31',
            'projection.day.digits': '2',
            'storage.location.template': f'{s3_location}year=${{year}}/month=${{month}}/day=${{day}}/'
        }
    }
    
    response = glue_client.create_table(
        DatabaseName=database_name,
        TableInput=table_input
    )
    
    print(f"Created partitioned table: {table_name}")
    return response

# Create sample partitioned tables
create_partitioned_table(
    database_name='financial_data',
    table_name='transactions',
    s3_location='s3://financial-data-bucket/transactions/',
    partition_keys=['year', 'month', 'day']
)

# Create table with complex partition structure
def create_complex_partitioned_table():
    """Create table with multiple partition dimensions"""
    
    table_input = {
        'Name': 'user_events_complex',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'event_id', 'Type': 'string'},
                {'Name': 'user_id', 'Type': 'string'},
                {'Name': 'event_type', 'Type': 'string'},
                {'Name': 'properties', 'Type': 'struct<page:string,duration:int>'},
                {'Name': 'timestamp', 'Type': 'timestamp'}
            ],
            'Location': 's3://analytics-data/events/',
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            }
        },
        'PartitionKeys': [
            {'Name': 'year', 'Type': 'string'},
            {'Name': 'month', 'Type': 'string'},
            {'Name': 'region', 'Type': 'string'},
            {'Name': 'event_category', 'Type': 'string'}
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            # Advanced partition projection
            'projection.enabled': 'true',
            'projection.year.type': 'integer',
            'projection.year.range': '2020,2030',
            'projection.month.type': 'integer',
            'projection.month.range': '1,12',
            'projection.month.digits': '2',
            'projection.region.type': 'enum',
            'projection.region.values': 'us-east-1,us-west-2,eu-west-1,ap-south-1',
            'projection.event_category.type': 'enum',
            'projection.event_category.values': 'pageview,click,purchase,search',
            'storage.location.template': 's3://analytics-data/events/year=${year}/month=${month}/region=${region}/category=${event_category}/'
        }
    }
    
    return glue_client.create_table(
        DatabaseName='analytics',
        TableInput=table_input
    )

create_complex_partitioned_table()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Partition management operations
import boto3
from datetime import datetime, timedelta

glue_client = boto3.client('glue')
athena_client = boto3.client('athena')

def add_single_partition(database_name, table_name, partition_values, s3_location):
    """Add a single partition to an existing table"""
    
    partition_input = {
        'Values': partition_values,  # e.g., ['2024', '01', '15']
        'StorageDescriptor': {
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            }
        }
    }
    
    try:
        response = glue_client.create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInput=partition_input
        )
        print(f"Added partition: {partition_values}")
        return response
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Partition already exists: {partition_values}")

def batch_add_partitions(database_name, table_name, partition_list):
    """Add multiple partitions in batch"""
    
    partition_inputs = []
    for partition_info in partition_list:
        partition_inputs.append({
            'Values': partition_info['values'],
            'StorageDescriptor': {
                'Location': partition_info['location'],
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                }
            }
        })
    
    # Batch create (max 100 partitions per request)
    for i in range(0, len(partition_inputs), 100):
        batch = partition_inputs[i:i+100]
        
        try:
            response = glue_client.batch_create_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionInputList=batch
            )
            
            if response.get('Errors'):
                for error in response['Errors']:
                    print(f"Error creating partition: {error}")
            
            print(f"Created {len(batch)} partitions successfully")
            
        except Exception as e:
            print(f"Batch partition creation failed: {e}")

def generate_date_partitions(start_date, end_date, base_s3_path):
    """Generate date-based partitions for a date range"""
    
    partitions = []
    current_date = start_date
    
    while current_date <= end_date:
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')
        day = current_date.strftime('%d')
        
        partition_values = [year, month, day]
        s3_location = f"{base_s3_path}year={year}/month={month}/day={day}/"
        
        partitions.append({
            'values': partition_values,
            'location': s3_location
        })
        
        current_date += timedelta(days=1)
    
    return partitions

# Example: Add partitions for last 30 days
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

date_partitions = generate_date_partitions(
    start_date=start_date,
    end_date=end_date,
    base_s3_path='s3://analytics-data/transactions/'
)

batch_add_partitions(
    database_name='financial_data',
    table_name='transactions',
    partition_list=date_partitions
)

def repair_table_partitions(database_name, table_name):
    """Use MSCK REPAIR to sync partitions with S3 structure"""
    
    query = f"MSCK REPAIR TABLE {database_name}.{table_name};"
    
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={
            'OutputLocation': 's3://athena-results-bucket/repairs/'
        }
    )
    
    query_execution_id = response['QueryExecutionId']
    print(f"Started MSCK REPAIR: {query_execution_id}")
    
    return query_execution_id

def drop_old_partitions(database_name, table_name, days_old=90):
    """Drop partitions older than specified days"""
    
    cutoff_date = datetime.now() - timedelta(days=days_old)
    
    # Get all partitions
    response = glue_client.get_partitions(
        DatabaseName=database_name,
        TableName=table_name
    )
    
    partitions_to_drop = []
    
    for partition in response['Partitions']:
        values = partition['Values']
        
        # Assume date partitioning with year, month, day
        if len(values) >= 3:
            try:
                partition_date = datetime(
                    int(values[0]),    # year
                    int(values[1]),    # month
                    int(values[2])     # day
                )
                
                if partition_date < cutoff_date:
                    partitions_to_drop.append(values)
                    
            except (ValueError, IndexError):
                print(f"Could not parse date from partition values: {values}")
    
    # Drop old partitions in batches
    for i in range(0, len(partitions_to_drop), 25):  # Max 25 per batch
        batch = partitions_to_drop[i:i+25]
        
        partition_inputs = [{'Values': values} for values in batch]
        
        try:
            glue_client.batch_delete_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionsToDelete=partition_inputs
            )
            print(f"Dropped {len(batch)} old partitions")
            
        except Exception as e:
            print(f"Error dropping partitions: {e}")

# Example usage
repair_table_partitions('financial_data', 'transactions')
drop_old_partitions('financial_data', 'transactions', days_old=365)

def list_table_partitions(database_name, table_name, max_partitions=100):
    """List all partitions for a table with details"""
    
    paginator = glue_client.get_paginator('get_partitions')
    
    partition_info = []
    
    for page in paginator.paginate(
        DatabaseName=database_name,
        TableName=table_name,
        MaxItems=max_partitions
    ):
        for partition in page['Partitions']:
            info = {
                'values': partition['Values'],
                'location': partition['StorageDescriptor']['Location'],
                'creation_time': partition.get('CreationTime'),
                'last_analyzed': partition.get('LastAnalyzedTime'),
                'parameters': partition.get('Parameters', {})
            }
            partition_info.append(info)
    
    return partition_info

# Get partition information
partitions = list_table_partitions('financial_data', 'transactions')
for partition in partitions[:5]:  # Show first 5
    print(f"Partition: {partition['values']}")
    print(f"Location: {partition['location']}")
    print(f"Created: {partition['creation_time']}")
    print("---")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Query optimization using partitions

-- 1. Efficient partition filtering (partition pruning)
SELECT 
    user_id,
    SUM(amount) as total_spent,
    COUNT(*) as transaction_count
FROM financial_data.transactions
WHERE year = '2024' 
    AND month = '01'
    AND day BETWEEN '01' AND '07'  -- First week of January
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 100;

-- 2. Cross-partition aggregation with date functions
SELECT 
    year,
    month,
    AVG(amount) as avg_transaction,
    COUNT(*) as daily_transactions,
    SUM(amount) as daily_total
FROM financial_data.transactions
WHERE year = '2024'
    AND month IN ('01', '02', '03')  -- Q1 analysis
GROUP BY year, month
ORDER BY year, month;

-- 3. Complex filtering with multiple partition keys
SELECT 
    region,
    event_category,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM analytics.user_events_complex
WHERE year = '2024'
    AND month = '01'
    AND region IN ('us-east-1', 'us-west-2')
    AND event_category = 'purchase'
GROUP BY region, event_category;

-- 4. Partition projection query (no MSCK REPAIR needed)
-- This query automatically discovers partitions using projection config
SELECT 
    COUNT(*) as total_events,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(properties.duration) as avg_duration
FROM analytics.user_events_complex
WHERE year = '2024'
    AND month = '01'
    AND region = 'us-east-1';

-- 5. Time range queries across multiple partitions
SELECT 
    DATE(from_unixtime(timestamp/1000)) as event_date,
    event_type,
    COUNT(*) as event_count
FROM analytics.user_events_complex  
WHERE year = '2024'
    AND month = '01'
    AND day BETWEEN '15' AND '31'
    AND region = 'us-east-1'
GROUP BY DATE(from_unixtime(timestamp/1000)), event_type
ORDER BY event_date, event_count DESC;

-- Performance optimization queries

-- Check partition sizes
SELECT 
    year,
    month, 
    day,
    COUNT(*) as row_count,
    approx_distinct(user_id) as unique_users,
    SUM(amount) as total_amount
FROM financial_data.transactions
WHERE year = '2024' AND month = '01'
GROUP BY year, month, day
ORDER BY row_count DESC;

-- Find poorly distributed partitions
WITH partition_stats AS (
    SELECT 
        year,
        month,
        COUNT(*) as partition_size,
        AVG(COUNT(*)) OVER() as avg_partition_size
    FROM financial_data.transactions
    WHERE year = '2024'
    GROUP BY year, month
)
SELECT 
    year,
    month,
    partition_size,
    avg_partition_size,
    CASE 
        WHEN partition_size < avg_partition_size * 0.1 THEN 'TOO_SMALL'
        WHEN partition_size > avg_partition_size * 10 THEN 'TOO_LARGE'
        ELSE 'OPTIMAL'
    END as partition_health
FROM partition_stats
ORDER BY partition_size;

-- Partition elimination verification
-- Use EXPLAIN to verify partition pruning
EXPLAIN (FORMAT JSON)
SELECT COUNT(*)
FROM financial_data.transactions
WHERE year = '2024' 
    AND month = '01'
    AND amount > 100;

-- Create optimized views for common partition patterns
CREATE VIEW monthly_transaction_summary AS
SELECT 
    year,
    month,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    COUNT(DISTINCT user_id) as unique_users
FROM financial_data.transactions
GROUP BY year, month;

-- Query the optimized view
SELECT * 
FROM monthly_transaction_summary
WHERE year = '2024'
ORDER BY month;

-- Advanced partition pruning with computed columns
SELECT 
    year,
    month,
    category,
    COUNT(*) as events,
    -- Use partition columns in computations for better pruning
    CONCAT(year, '-', month) as year_month,
    CASE 
        WHEN CAST(month AS INTEGER) <= 3 THEN 'Q1'
        WHEN CAST(month AS INTEGER) <= 6 THEN 'Q2'
        WHEN CAST(month AS INTEGER) <= 9 THEN 'Q3'
        ELSE 'Q4'
    END as quarter
FROM analytics.user_events_complex
WHERE year = '2024'
    AND CAST(month AS INTEGER) BETWEEN 1 AND 3  -- Q1 filter
GROUP BY year, month, event_category
ORDER BY year, month, event_category;

-- Partition troubleshooting queries

-- Check for missing partitions
WITH expected_dates AS (
    SELECT date_add('day', seq, DATE '2024-01-01') as expected_date
    FROM unnest(sequence(0, 30)) AS t(seq)  -- 31 days in January
),
actual_partitions AS (
    SELECT DISTINCT 
        DATE(CONCAT(year, '-', month, '-', day)) as partition_date
    FROM financial_data.transactions
    WHERE year = '2024' AND month = '01'
)
SELECT e.expected_date
FROM expected_dates e
LEFT JOIN actual_partitions a ON e.expected_date = a.partition_date
WHERE a.partition_date IS NULL;

-- Identify schema mismatches between partitions
SHOW PARTITIONS financial_data.transactions;

-- Check partition metadata consistency
SELECT 
    table_name,
    partition_keys,
    count(*) as partition_count
FROM information_schema.partitions 
WHERE table_schema = 'financial_data'
    AND table_name = 'transactions'
GROUP BY table_name, partition_keys;
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)

def crawler_scheduling_tab():
    """Content for AWS Glue Crawler Scheduling tab"""
    st.markdown("## ⏰ AWS Glue - Crawler Scheduling")
    st.markdown("*Using AWS Glue crawlers to keep your partitions in data catalog synchronized*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    Crawler scheduling ensures your AWS Glue Data Catalog stays synchronized with your data sources by automatically 
    running crawlers at specified intervals or based on triggers, keeping metadata fresh and partitions up-to-date.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Scheduling architecture
    scheduling_diagram = """
    graph TB
        subgraph "Data Sources"
            S3[S3 Data Lake<br/>📁 New Data Arrives]
            RDS[RDS Database<br/>🗄️ Schema Changes]
        end
        
        subgraph "Scheduling Triggers"
            CRON[Scheduled Trigger<br/>🕒 Time-based]
            EVENT[Event Trigger<br/>⚡ Data-driven]
            MANUAL[On-Demand Trigger<br/>👤 Manual Run]
        end
        
        subgraph "Crawler Execution"
            CRAWLER[Glue Crawler<br/>🕷️ Schema Discovery]
            CLASSIFY[Data Classification]
            UPDATE[Metadata Update]
        end
        
        subgraph "Data Catalog"
            CATALOG[Updated Catalog<br/>📊 Fresh Metadata]
            PARTITIONS[New Partitions<br/>🗂️ Synchronized]
        end
        
        subgraph "Downstream Services"
            ATHENA[Amazon Athena<br/>📊 Ready to Query]
            EMR[Amazon EMR<br/>⚙️ Big Data Processing]
            QUICKSIGHT[Amazon QuickSight<br/>📈 BI Dashboards]
        end
        
        S3 --> EVENT
        RDS --> EVENT
        
        CRON --> CRAWLER
        EVENT --> CRAWLER
        MANUAL --> CRAWLER
        
        CRAWLER --> CLASSIFY
        CLASSIFY --> UPDATE
        UPDATE --> CATALOG
        UPDATE --> PARTITIONS
        
        CATALOG --> ATHENA
        CATALOG --> EMR
        CATALOG --> QUICKSIGHT
        
        style CRAWLER fill:#FF9900,stroke:#232F3E,color:#fff
        style CATALOG fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CRON fill:#3FB34F,stroke:#232F3E,color:#fff
        style EVENT fill:#232F3E,stroke:#FF9900,color:#fff
    """
    
    st.markdown("#### 🏗️ Crawler Scheduling Architecture")
    common.mermaid(scheduling_diagram, height=1100)
    
    # Interactive schedule builder
    st.markdown("#### ⚙️ Interactive Schedule Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("###### Schedule Configuration")
        schedule_type = st.selectbox("Schedule Type", [
            "Time-based (Cron)",
            "Event-driven (Conditional)",
            "On-demand (Manual)"
        ])
        
        if schedule_type == "Time-based (Cron)":
            frequency = st.selectbox("Frequency", [
                "Hourly", "Daily", "Weekly", "Monthly", "Custom"
            ])
            
            if frequency == "Daily":
                hour = st.slider("Hour (UTC)", 0, 23, 2)
                minute = st.slider("Minute", 0, 59, 0)
                cron_expr = f"cron({minute} {hour} * * ? *)"
            elif frequency == "Weekly":
                day_of_week = st.selectbox("Day of Week", [
                    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
                ])
                hour = st.slider("Hour (UTC)", 0, 23, 2)
                days = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]
                day_code = days[["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"].index(day_of_week)]
                cron_expr = f"cron(0 {hour} ? * {day_code} *)"
            elif frequency == "Hourly":
                minute = st.slider("Minute past hour", 0, 59, 0)
                cron_expr = f"cron({minute} * * * ? *)"
            elif frequency == "Custom":
                cron_expr = st.text_input("Cron Expression", "cron(0 2 * * ? *)")
            else:
                cron_expr = "cron(0 2 1 * ? *)"  # Monthly
        
        elif schedule_type == "Event-driven (Conditional)":
            trigger_jobs = st.multiselect("Trigger on Job Completion", [
                "daily-etl-job", "data-ingestion-job", "preprocessing-job"
            ])
            trigger_condition = st.selectbox("Condition", [
                "All jobs succeed", "Any job succeeds", "Any job fails"
            ])
    
    with col2:
        st.markdown("###### Additional Settings")
        
        max_concurrent = st.slider("Max Concurrent Runs", 1, 10, 1)
        timeout_minutes = st.slider("Timeout (minutes)", 30, 480, 120)
        
        schema_change_policy = st.selectbox("Schema Change Policy", [
            "Update table definition",
            "Add new columns only",
            "Ignore changes"
        ])
        
        delete_behavior = st.selectbox("Delete Behavior", [
            "Log deletion",
            "Delete from catalog",
            "Deprecate in catalog"
        ])
    
    # Display schedule summary
    if schedule_type == "Time-based (Cron)":
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 📅 Schedule Summary
        **Type**: {schedule_type}  
        **Cron Expression**: `{cron_expr}`  
        **Next Run**: Based on current UTC time  
        **Max Concurrent**: {max_concurrent} runs  
        **Timeout**: {timeout_minutes} minutes
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Trigger types explanation
    st.markdown("#### 🔧 Trigger Types")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🕒 Scheduled Triggers
        **Time-based execution**
        - Cron expressions
        - Regular intervals
        - Predictable runs
        - Good for batch processing
        
        **Example**: Daily at 2 AM UTC
        `cron(0 2 * * ? *)`
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚡ Conditional Triggers  
        **Event-driven execution**
        - Job completion based
        - Success/failure conditions
        - Workflow orchestration
        - Real-time processing
        
        **Example**: After ETL job completes
        Trigger when job status = SUCCEEDED
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 👤 On-Demand Triggers
        **Manual execution**
        - User-initiated
        - API/Console driven
        - Testing scenarios
        - Ad-hoc requirements
        
        **Example**: Manual testing
        `aws glue start-crawler --name my-crawler`
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Crawler status simulator
    st.markdown("#### 📊 Crawler Status Monitor")
    
    # Sample crawler runs
    if 'crawler_runs' not in st.session_state:
        st.session_state.crawler_runs = []
    
    if st.button("🏃 Simulate Crawler Run"):
        import random
        from datetime import datetime, timedelta
        
        # Simulate a new crawler run
        run_time = datetime.now() - timedelta(minutes=random.randint(1, 60))
        status = random.choice(['SUCCEEDED', 'RUNNING', 'FAILED'])
        tables_created = random.randint(0, 5) if status == 'SUCCEEDED' else 0
        tables_updated = random.randint(0, 10) if status == 'SUCCEEDED' else 0
        runtime = random.randint(30, 300) if status != 'RUNNING' else random.randint(10, 120)
        
        new_run = {
            'run_id': f"cr-{random.randint(1000, 9999)}",
            'start_time': run_time,
            'status': status,
            'tables_created': tables_created,
            'tables_updated': tables_updated,
            'runtime_seconds': runtime,
            'trigger_type': random.choice(['Scheduled', 'Manual', 'Conditional'])
        }
        
        st.session_state.crawler_runs.insert(0, new_run)
        if len(st.session_state.crawler_runs) > 10:
            st.session_state.crawler_runs = st.session_state.crawler_runs[:10]
    
    # Display crawler runs
    if st.session_state.crawler_runs:
        st.markdown("###### Recent Crawler Runs")
        
        for run in st.session_state.crawler_runs:
            status_class = {
                'SUCCEEDED': 'status-success',
                'RUNNING': 'status-running', 
                'FAILED': 'status-failed'
            }.get(run['status'], 'status-running')
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(f'<div class="crawler-status {status_class}">{run["status"]}</div>', 
                           unsafe_allow_html=True)
            
            with col2:
                st.write(f"**Run ID**: {run['run_id']}")
                st.write(f"**Trigger**: {run['trigger_type']}")
            
            with col3:
                st.write(f"**Tables Created**: {run['tables_created']}")
                st.write(f"**Tables Updated**: {run['tables_updated']}")
            
            with col4:
                st.write(f"**Runtime**: {run['runtime_seconds']}s")
                st.write(f"**Started**: {run['start_time'].strftime('%H:%M:%S')}")
            
            st.markdown("---")
    
    # Best practices
    st.markdown("#### 🏆 Scheduling Best Practices")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⏰ Timing Considerations
        - **Off-peak Hours**: Schedule during low-usage periods
        - **Data Arrival**: Align with data ingestion patterns  
        - **Downstream Dependencies**: Consider query workloads
        - **Time Zones**: Use UTC for consistency
        - **Buffer Time**: Allow processing time before queries
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚀 Performance Optimization
        - **Incremental Crawling**: Use CRAWL_NEW_FOLDERS_ONLY
        - **Exclude Patterns**: Skip unnecessary files
        - **Concurrent Limits**: Prevent resource contention
        - **Partition Strategy**: Optimize for query patterns
        - **Monitoring**: Set up CloudWatch alarms
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Code Examples")
    
    tab1, tab2, tab3 = st.tabs(["Schedule Configuration", "Trigger Management", "Monitoring & Alerts"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Configure crawler scheduling
import boto3
from datetime import datetime, timedelta

glue_client = boto3.client('glue')

def create_scheduled_crawler(crawler_name, s3_path, database_name, schedule_expression):
    """Create a crawler with time-based scheduling"""
    
    crawler_config = {
        'Name': crawler_name,
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole', 
        'DatabaseName': database_name,
        'Description': f'Scheduled crawler for {s3_path}',
        'Targets': {
            'S3Targets': [
                {
                    'Path': s3_path,
                    'Exclusions': [
                        '**/_temporary/**',
                        '**/*.tmp',
                        '**/*_backup*'
                    ]
                }
            ]
        },
        'Schedule': schedule_expression,  # Cron expression
        'SchemaChangePolicy': {
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'LOG'
        },
        'RecrawlPolicy': {
            'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'  # Incremental crawling
        },
        'Configuration': """{
            "Version": 1.0,
            "CrawlerOutput": {
                "Partitions": {
                    "AddOrUpdateBehavior": "InheritFromTable"
                }
            },
            "Grouping": {
                "TableGroupingPolicy": "CombineCompatibleSchemas"
            }
        }"""
    }
    
    response = glue_client.create_crawler(**crawler_config)
    print(f"Created scheduled crawler: {crawler_name}")
    return response

# Common schedule patterns
schedules = {
    'hourly': 'cron(0 * * * ? *)',           # Every hour
    'daily_2am': 'cron(0 2 * * ? *)',        # Daily at 2 AM UTC
    'weekly_sunday': 'cron(0 2 ? * SUN *)',  # Sunday at 2 AM UTC
    'monthly': 'cron(0 2 1 * ? *)',          # First day of month
    'weekdays_only': 'cron(0 2 ? * MON-FRI *)', # Weekdays at 2 AM
    'business_hours': 'cron(0 9-17 ? * MON-FRI *)', # Every hour 9-5 weekdays
}

# Create different types of scheduled crawlers
create_scheduled_crawler(
    crawler_name='daily-sales-crawler',
    s3_path='s3://sales-data-bucket/transactions/',
    database_name='sales_analytics',
    schedule_expression=schedules['daily_2am']
)

create_scheduled_crawler(
    crawler_name='hourly-logs-crawler', 
    s3_path='s3://application-logs/raw/',
    database_name='log_analytics',
    schedule_expression=schedules['hourly']
)

def update_crawler_schedule(crawler_name, new_schedule):
    """Update existing crawler schedule"""
    
    try:
        response = glue_client.update_crawler(
            Name=crawler_name,
            Schedule=new_schedule
        )
        print(f"Updated schedule for {crawler_name}: {new_schedule}")
        return response
    except Exception as e:
        print(f"Error updating schedule: {e}")

def pause_crawler_schedule(crawler_name):
    """Temporarily disable crawler schedule"""
    
    try:
        # Remove schedule (empty string disables scheduling)
        response = glue_client.update_crawler(
            Name=crawler_name,
            Schedule=''
        )
        print(f"Paused schedule for {crawler_name}")
        return response
    except Exception as e:
        print(f"Error pausing schedule: {e}")

def resume_crawler_schedule(crawler_name, schedule_expression):
    """Resume crawler with previous schedule"""
    
    return update_crawler_schedule(crawler_name, schedule_expression)

# Advanced scheduling with multiple configurations
def create_tiered_crawling_schedule():
    """Create multiple crawlers for different data tiers"""
    
    # Hot data - crawl frequently
    create_scheduled_crawler(
        crawler_name='hot-data-crawler',
        s3_path='s3://data-lake/hot/',
        database_name='real_time_analytics',
        schedule_expression='cron(0 */2 * * ? *)'  # Every 2 hours
    )
    
    # Warm data - crawl daily
    create_scheduled_crawler(
        crawler_name='warm-data-crawler',
        s3_path='s3://data-lake/warm/',
        database_name='batch_analytics', 
        schedule_expression='cron(0 3 * * ? *)'   # Daily at 3 AM
    )
    
    # Cold data - crawl weekly
    create_scheduled_crawler(
        crawler_name='cold-data-crawler',
        s3_path='s3://data-lake/cold/',
        database_name='archive_analytics',
        schedule_expression='cron(0 4 ? * SUN *)'  # Weekly Sunday 4 AM
    )

create_tiered_crawling_schedule()

# Dynamic schedule adjustment based on data volume
def adjust_schedule_based_on_data_volume(crawler_name, s3_path):
    """Adjust crawler schedule based on data arrival patterns"""
    
    import boto3
    from datetime import datetime, timedelta
    
    s3_client = boto3.client('s3')
    
    # Check recent data additions
    bucket = s3_path.replace('s3://', '').split('/')[0]
    prefix = '/'.join(s3_path.replace('s3://', '').split('/')[1:])
    
    # Get objects modified in last 24 hours
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1000
    )
    
    recent_objects = 0
    if 'Contents' in response:
        yesterday = datetime.now() - timedelta(days=1)
        for obj in response['Contents']:
            if obj['LastModified'].replace(tzinfo=None) > yesterday:
                recent_objects += 1
    
    # Adjust schedule based on data velocity
    if recent_objects > 100:
        new_schedule = 'cron(0 */2 * * ? *)'  # Every 2 hours
        print(f"High data volume detected: {recent_objects} files")
    elif recent_objects > 10:
        new_schedule = 'cron(0 6 * * ? *)'    # Daily at 6 AM
        print(f"Medium data volume detected: {recent_objects} files")
    else:
        new_schedule = 'cron(0 6 ? * SUN *)'  # Weekly
        print(f"Low data volume detected: {recent_objects} files")
    
    update_crawler_schedule(crawler_name, new_schedule)
    return new_schedule

# Example dynamic adjustment
adjust_schedule_based_on_data_volume(
    'dynamic-sales-crawler', 
    's3://dynamic-data-bucket/sales/'
)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Trigger management and workflow orchestration
import boto3
import json

glue_client = boto3.client('glue')

def create_conditional_trigger(trigger_name, crawler_name, job_dependencies):
    """Create conditional trigger based on job completion"""
    
    # Define job conditions
    conditions = []
    for job_name in job_dependencies:
        conditions.append({
            'LogicalOperator': 'EQUALS',
            'JobName': job_name,
            'State': 'SUCCEEDED'  # Trigger when job succeeds
        })
    
    trigger_config = {
        'Name': trigger_name,
        'Type': 'CONDITIONAL',
        'Actions': [
            {
                'CrawlerName': crawler_name
            }
        ],
        'Predicate': {
            'Conditions': conditions,
            'Logical': 'AND'  # All jobs must succeed
        },
        'Description': f'Trigger {crawler_name} after job dependencies complete'
    }
    
    response = glue_client.create_trigger(**trigger_config)
    print(f"Created conditional trigger: {trigger_name}")
    return response

def create_workflow_with_triggers():
    """Create a complete data processing workflow"""
    
    workflow_name = 'daily-data-pipeline'
    
    # Create workflow
    workflow_response = glue_client.create_workflow(
        Name=workflow_name,
        Description='Daily data processing pipeline',
        DefaultRunProperties={
            'environment': 'production',
            'team': 'data-engineering'
        }
    )
    
    # Trigger 1: Start with raw data ingestion job
    glue_client.create_trigger(
        Name='start-ingestion-trigger',
        Type='SCHEDULED',
        Schedule='cron(0 1 * * ? *)',  # 1 AM daily
        Actions=[
            {
                'JobName': 'raw-data-ingestion-job'
            }
        ],
        StartOnCreation=True,
        WorkflowName=workflow_name
    )
    
    # Trigger 2: Crawl raw data after ingestion
    glue_client.create_trigger(
        Name='crawl-raw-data-trigger',
        Type='CONDITIONAL',
        Actions=[
            {
                'CrawlerName': 'raw-data-crawler'
            }
        ],
        Predicate={
            'Conditions': [
                {
                    'LogicalOperator': 'EQUALS',
                    'JobName': 'raw-data-ingestion-job',
                    'State': 'SUCCEEDED'
                }
            ]
        },
        StartOnCreation=True,
        WorkflowName=workflow_name
    )
    
    # Trigger 3: Data transformation after crawling
    glue_client.create_trigger(
        Name='transform-data-trigger',
        Type='CONDITIONAL',
        Actions=[
            {
                'JobName': 'data-transformation-job'
            }
        ],
        Predicate={
            'Conditions': [
                {
                    'LogicalOperator': 'EQUALS',
                    'CrawlerName': 'raw-data-crawler',
                    'CrawlState': 'SUCCEEDED'
                }
            ]
        },
        StartOnCreation=True,
        WorkflowName=workflow_name
    )
    
    # Trigger 4: Final crawl of processed data
    glue_client.create_trigger(
        Name='crawl-processed-data-trigger',
        Type='CONDITIONAL',
        Actions=[
            {
                'CrawlerName': 'processed-data-crawler'
            }
        ],
        Predicate={
            'Conditions': [
                {
                    'LogicalOperator': 'EQUALS',
                    'JobName': 'data-transformation-job',
                    'State': 'SUCCEEDED'
                }
            ]
        },
        StartOnCreation=True,
        WorkflowName=workflow_name
    )
    
    print(f"Created workflow: {workflow_name}")
    return workflow_response

def manage_trigger_states():
    """Manage trigger activation and lifecycle"""
    
    # List all triggers
    response = glue_client.get_triggers()
    
    for trigger in response['Triggers']:
        trigger_name = trigger['Name']
        trigger_state = trigger['State']
        trigger_type = trigger['Type']
        
        print(f"Trigger: {trigger_name}")
        print(f"  Type: {trigger_type}")
        print(f"  State: {trigger_state}")
        
        # Start inactive triggers
        if trigger_state == 'CREATED':
            try:
                glue_client.start_trigger(Name=trigger_name)
                print(f"  ✅ Started trigger: {trigger_name}")
            except Exception as e:
                print(f"  ❌ Failed to start: {e}")
        
        print("---")

def create_multi_condition_trigger():
    """Create trigger with complex conditions"""
    
    complex_trigger = {
        'Name': 'complex-data-pipeline-trigger',
        'Type': 'CONDITIONAL',
        'Actions': [
            {
                'CrawlerName': 'final-data-crawler'
            },
            {
                'JobName': 'data-quality-check-job'
            }
        ],
        'Predicate': {
            'Logical': 'AND',
            'Conditions': [
                {
                    'LogicalOperator': 'EQUALS',
                    'JobName': 'etl-job-1',
                    'State': 'SUCCEEDED'
                },
                {
                    'LogicalOperator': 'EQUALS', 
                    'JobName': 'etl-job-2',
                    'State': 'SUCCEEDED'
                },
                {
                    'LogicalOperator': 'EQUALS',
                    'CrawlerName': 'source-data-crawler',
                    'CrawlState': 'SUCCEEDED'
                }
            ]
        },
        'Description': 'Trigger final steps after all ETL jobs and crawlers complete'
    }
    
    response = glue_client.create_trigger(**complex_trigger)
    return response

# Event-driven triggers with Lambda integration
def create_s3_event_driven_crawler():
    """Create S3 event-driven crawler using Lambda and Glue"""
    
    lambda_code = """
import boto3
import json

def lambda_handler(event, context):
    """Trigger crawler when new S3 objects arrive"""
    
    glue_client = boto3.client('glue')
    
    # Parse S3 event
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        print(f"New object detected: s3://{bucket}/{key}")
        
        # Determine which crawler to run based on S3 path
        crawler_mapping = {
            'raw-data/': 'raw-data-crawler',
            'processed-data/': 'processed-data-crawler',
            'logs/': 'log-data-crawler'
        }
        
        crawler_name = None
        for path_prefix, crawler in crawler_mapping.items():
            if key.startswith(path_prefix):
                crawler_name = crawler
                break
        
        if crawler_name:
            try:
                # Check if crawler is already running
                crawler_info = glue_client.get_crawler(Name=crawler_name)
                crawler_state = crawler_info['Crawler']['State']
                
                if crawler_state == 'READY':
                    response = glue_client.start_crawler(Name=crawler_name)
                    print(f"Started crawler: {crawler_name}")
                else:
                    print(f"Crawler {crawler_name} is not ready (state: {crawler_state})")
                    
            except Exception as e:
                print(f"Error starting crawler {crawler_name}: {e}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Crawler trigger processed successfully')
    }
    """
    
    print("Lambda function code for S3 event-driven crawlers:")
    print(lambda_code)

# Usage examples
create_conditional_trigger(
    trigger_name='etl-completion-trigger',
    crawler_name='processed-data-crawler',
    job_dependencies=['data-ingestion-job', 'data-cleaning-job']
)

create_workflow_with_triggers()
manage_trigger_states()
create_multi_condition_trigger()
create_s3_event_driven_crawler()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Monitoring and alerting for scheduled crawlers
import boto3
import json
from datetime import datetime, timedelta

cloudwatch = boto3.client('cloudwatch')
sns = boto3.client('sns')
glue_client = boto3.client('glue')

def setup_crawler_monitoring(crawler_name, sns_topic_arn):
    """Set up CloudWatch alarms for crawler monitoring"""
    
    # Alarm for crawler failures
    cloudwatch.put_metric_alarm(
        AlarmName=f'{crawler_name}-failure-alarm',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=1,
        MetricName='glue.crawler.numCompletedTasks',
        Namespace='AWS/Glue',
        Period=300,
        Statistic='Sum',
        Threshold=0.0,
        ActionsEnabled=True,
        AlarmActions=[sns_topic_arn],
        AlarmDescription=f'Alert when {crawler_name} fails',
        Dimensions=[
            {
                'Name': 'CrawlerName',
                'Value': crawler_name
            }
        ],
        Unit='Count'
    )
    
    # Alarm for long-running crawlers
    cloudwatch.put_metric_alarm(
        AlarmName=f'{crawler_name}-duration-alarm',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=1,
        MetricName='glue.crawler.runtime.seconds',
        Namespace='AWS/Glue',
        Period=300,
        Statistic='Maximum',
        Threshold=3600.0,  # 1 hour
        ActionsEnabled=True,
        AlarmActions=[sns_topic_arn],
        AlarmDescription=f'Alert when {crawler_name} runs longer than 1 hour',
        Dimensions=[
            {
                'Name': 'CrawlerName',
                'Value': crawler_name
            }
        ],
        Unit='Seconds'
    )
    
    # Alarm for no tables created/updated
    cloudwatch.put_metric_alarm(
        AlarmName=f'{crawler_name}-no-updates-alarm',
        ComparisonOperator='LessThanThreshold',
        EvaluationPeriods=2,  # 2 consecutive periods
        MetricName='glue.crawler.tablesUpdated',
        Namespace='AWS/Glue',
        Period=86400,  # Daily check
        Statistic='Sum',
        Threshold=1.0,
        ActionsEnabled=True,
        AlarmActions=[sns_topic_arn],
        AlarmDescription=f'Alert when {crawler_name} finds no updates for 2 days',
        Dimensions=[
            {
                'Name': 'CrawlerName',
                'Value': crawler_name
            }
        ],
        Unit='Count'
    )
    
    print(f"Set up monitoring for crawler: {crawler_name}")

def get_crawler_performance_metrics(crawler_name, days_back=7):
    """Retrieve and analyze crawler performance metrics"""
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days_back)
    
    metrics = {}
    
    # Get runtime metrics
    runtime_response = cloudwatch.get_metric_statistics(
        Namespace='AWS/Glue',
        MetricName='glue.driver.aggregate.elapsedTime',
        Dimensions=[
            {
                'Name': 'JobName',
                'Value': crawler_name
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Average', 'Maximum', 'Minimum']
    )
    
    metrics['runtime'] = runtime_response['Datapoints']
    
    # Get success/failure rates
    for metric_name in ['tablesCreated', 'tablesUpdated', 'tablesDeleted']:
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Glue',
            MetricName=f'glue.crawler.{metric_name}',
            Dimensions=[
                {
                    'Name': 'CrawlerName',
                    'Value': crawler_name
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,
            Statistics=['Sum']
        )
        metrics[metric_name] = response['Datapoints']
    
    return metrics

def create_crawler_dashboard():
    """Create CloudWatch dashboard for crawler monitoring"""
    
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "x": 0,
                "y": 0,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/Glue", "glue.crawler.runtime.seconds", "CrawlerName", "sales-data-crawler"],
                        [".", "glue.crawler.tablesCreated", ".", "."],
                        [".", "glue.crawler.tablesUpdated", ".", "."]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": "us-west-2",
                    "title": "Crawler Performance Metrics",
                    "period": 300
                }
            },
            {
                "type": "metric",
                "x": 0,
                "y": 6,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/Glue", "glue.crawler.numCompletedTasks", "CrawlerName", "sales-data-crawler"],
                        [".", "glue.crawler.numFailedTasks", ".", "."]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": "us-west-2",
                    "title": "Crawler Task Success/Failure",
                    "period": 300
                }
            }
        ]
    }
    
    response = cloudwatch.put_dashboard(
        DashboardName='GlueCrawlerMonitoring',
        DashboardBody=json.dumps(dashboard_body)
    )
    
    print("Created CloudWatch dashboard: GlueCrawlerMonitoring")
    return response

def automated_health_check():
    """Automated health check for all crawlers"""
    
    # Get all crawlers
    crawlers_response = glue_client.get_crawlers()
    
    health_report = {
        'healthy': [],
        'warnings': [],
        'critical': []
    }
    
    for crawler in crawlers_response['CrawlerList']:
        crawler_name = crawler['Name']
        crawler_state = crawler['State']
        
        # Check last crawl info
        last_crawl = crawler.get('LastCrawl', {})
        
        if not last_crawl:
            health_report['warnings'].append({
                'crawler': crawler_name,
                'issue': 'Never run',
                'recommendation': 'Run initial crawl'
            })
            continue
        
        last_crawl_time = last_crawl.get('StartTime')
        crawl_status = last_crawl.get('Status')
        
        # Check if crawler is stale (hasn't run in 48 hours)
        if last_crawl_time:
            time_since_last_run = datetime.now(last_crawl_time.tzinfo) - last_crawl_time
            if time_since_last_run > timedelta(hours=48):
                health_report['warnings'].append({
                    'crawler': crawler_name,
                    'issue': f'Last run {time_since_last_run.days} days ago',
                    'recommendation': 'Check schedule and data source'
                })
        
        # Check crawler status
        if crawl_status == 'FAILED':
            health_report['critical'].append({
                'crawler': crawler_name,
                'issue': 'Last crawl failed',
                'recommendation': 'Check crawler logs and configuration'
            })
        elif crawl_status == 'SUCCEEDED':
            # Check if no tables were created/updated
            tables_created = last_crawl.get('TablesCreated', 0)
            tables_updated = last_crawl.get('TablesUpdated', 0)
            
            if tables_created == 0 and tables_updated == 0:
                health_report['warnings'].append({
                    'crawler': crawler_name,
                    'issue': 'No schema changes detected',
                    'recommendation': 'Verify data source has new data'
                })
            else:
                health_report['healthy'].append({
                    'crawler': crawler_name,
                    'tables_created': tables_created,
                    'tables_updated': tables_updated
                })
    
    return health_report

def send_health_report_notification(health_report, sns_topic_arn):
    """Send health report via SNS"""
    
    message = "🕷️ **Glue Crawler Health Report**\n\n"
    
    # Healthy crawlers
    if health_report['healthy']:
        message += "✅ **Healthy Crawlers:**\n"
        for item in health_report['healthy']:
            message += f"  • {item['crawler']}: {item['tables_created']} created, {item['tables_updated']} updated\n"
        message += "\n"
    
    # Warnings
    if health_report['warnings']:
        message += "⚠️ **Warnings:**\n"
        for item in health_report['warnings']:
            message += f"  • {item['crawler']}: {item['issue']}\n"
            message += f"    → {item['recommendation']}\n"
        message += "\n"
    
    # Critical issues
    if health_report['critical']:
        message += "🚨 **Critical Issues:**\n"
        for item in health_report['critical']:
            message += f"  • {item['crawler']}: {item['issue']}\n"
            message += f"    → {item['recommendation']}\n"
    
    # Send notification
    response = sns.publish(
        TopicArn=sns_topic_arn,
        Message=message,
        Subject="Glue Crawler Health Report"
    )
    
    return response

# Usage examples
sns_topic_arn = 'arn:aws:sns:us-west-2:123456789012:glue-crawler-alerts'

# Set up monitoring for specific crawler
setup_crawler_monitoring('sales-data-crawler', sns_topic_arn)

# Get performance metrics
metrics = get_crawler_performance_metrics('sales-data-crawler', days_back=7)
print(f"Crawler metrics: {len(metrics)} metric types collected")

# Create monitoring dashboard
create_crawler_dashboard()

# Run automated health check
health_report = automated_health_check()
print(f"\nHealth Check Results:")
print(f"Healthy: {len(health_report['healthy'])}")
print(f"Warnings: {len(health_report['warnings'])}")
print(f"Critical: {len(health_report['critical'])}")

# Send health report
if health_report['warnings'] or health_report['critical']:
    send_health_report_notification(health_report, sns_topic_arn)
    print("Health report notification sent!")

def setup_cost_monitoring():
    """Monitor crawler costs and usage"""
    
    # Create budget alert for Glue DPU usage
    budgets_client = boto3.client('budgets')
    
    budget = {
        'BudgetName': 'GlueCrawlerBudget',
        'BudgetLimit': {
            'Amount': '100.0',
            'Unit': 'USD'
        },
        'TimeUnit': 'MONTHLY',
        'CostFilters': {
            'Service': ['Amazon Glue']
        },
        'BudgetType': 'COST'
    }
    
    subscribers = [
        {
            'SubscriptionType': 'EMAIL',
            'Address': 'data-team@company.com'
        }
    ]
    
    notifications = [
        {
            'Notification': {
                'NotificationType': 'ACTUAL',
                'ComparisonOperator': 'GREATER_THAN',
                'Threshold': 80.0,
                'ThresholdType': 'PERCENTAGE'
            },
            'Subscribers': subscribers
        }
    ]
    
    try:
        budgets_client.create_budget(
            AccountId='123456789012',
            Budget=budget,
            NotificationsWithSubscribers=notifications
        )
        print("Created Glue crawler budget monitoring")
    except Exception as e:
        print(f"Budget creation failed: {e}")

setup_cost_monitoring()
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
    # 🔍 AWS Glue & Data Cataloging Systems
    <div class='info-box'>
    Master serverless data integration and metadata management
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🔍 AWS Glue",
        "📊 AWS Glue Data Catalog",
        "🔄 AWS Glue Schema Registry",
        "🕷️ AWS Glue Crawlers",
        "📂 AWS Glue Table Partitions",
        "⏰ AWS Glue Crawler Scheduling"
    ])
    
    with tab1:
        aws_glue_tab()
    
    with tab2:
        data_catalog_tab()
    
    with tab3:
        schema_registry_tab()
    
    with tab4:
        crawlers_tab()
    
    with tab5:
        table_partitions_tab()
    
    with tab6:
        crawler_scheduling_tab()
    
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
