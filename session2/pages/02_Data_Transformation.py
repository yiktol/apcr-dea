import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import utils.common as common
import utils.authenticate as authenticate

# Page configuration
st.set_page_config(
    page_title="AWS Data Transformation & Processing Hub",
    page_icon="🔄",
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
    'success': '#3FB34F',
    'warning': '#FF6B35',
    'purple': '#8A2BE2'
}

def apply_custom_styles():
    """Apply custom CSS styling with AWS color scheme"""
    st.markdown(f"""
    <style>
        .main {{
            background-color: {AWS_COLORS['light_gray']};
        }}
        
        .stTabs [data-baseweb="tab-list"] {{
            gap: 20px;
            background-color: white;
            border-radius: 12px;
            padding: 12px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        
        .stTabs [data-baseweb="tab"] {{
            height: 65px;
            padding: 8px 20px;
            background-color: {AWS_COLORS['light_gray']};
            border-radius: 10px;
            color: {AWS_COLORS['secondary']};
            font-weight: 600;
            border: 2px solid transparent;
            font-size: 14px;
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
            padding: 25px;
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
        
        .warning-box {{
            background: linear-gradient(135deg, {AWS_COLORS['warning']} 0%, {AWS_COLORS['primary']} 100%);
            padding: 20px;
            border-radius: 12px;
            color: white;
            margin: 15px 0;
        }}
        
        .footer {{
            margin-top: 50px;
            padding: 20px;
            text-align: center;
            background-color: {AWS_COLORS['secondary']};
            color: white;
            border-radius: 10px;
        }}
        
        .code-container {{
            background-color: {AWS_COLORS['dark_blue']};
            color: white;
            padding: 20px;
            border-radius: 10px;
            border-left: 4px solid {AWS_COLORS['primary']};
            margin: 15px 0;
        }}
        
        .service-comparison {{
            background: white;
            padding: 20px;
            border-radius: 12px;
            border: 2px solid {AWS_COLORS['light_blue']};
            margin: 15px 0;
        }}
        
        .etl-step {{
            background: linear-gradient(135deg, {AWS_COLORS['success']} 0%, {AWS_COLORS['light_blue']} 100%);
            padding: 15px;
            border-radius: 10px;
            color: white;
            margin: 10px 0;
            text-align: center;
        }}
    </style>
    """, unsafe_allow_html=True)

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🔄 Data Transformation & Processing
            - 📊 ETL (Extract, Transform, Load) Concepts
            - ⚙️ AWS Glue for ETL Processing
            - 🔗 Data Sources & Connections
            - 🏗️ Building ETL Pipelines
            - 💾 Data Storage & Formats
            
            **Learning Objectives:**
            - Understand ETL pipeline concepts
            - Learn AWS Glue capabilities
            - Explore data transformation techniques
            - Practice with interactive examples
            - Master data format optimization
            """)

def create_etl_overview_mermaid():
    """Create mermaid diagram for ETL overview"""
    return """
    graph TD
        A[📊 Raw Data Sources] --> B[🔄 ETL Process]
        
        subgraph "Extract"
            C[🏢 Databases]
            D[📁 Files]
            E[🌐 APIs]
            F[📡 Streaming Data]
        end
        
        subgraph "Transform"
            G[🧹 Data Cleaning]
            H[🔧 Data Validation]
            I[📐 Data Formatting]
            J[🔗 Data Enrichment]
        end
        
        subgraph "Load"
            K[🏢 Data Warehouse]
            L[📊 Data Lake]
            M[💾 Databases]
            N[📈 Analytics Tools]
        end
        
        C --> B
        D --> B
        E --> B
        F --> B
        
        B --> G
        B --> H
        B --> I
        B --> J
        
        G --> K
        H --> L
        I --> M
        J --> N
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#3FB34F,stroke:#232F3E,color:#fff
        style E fill:#3FB34F,stroke:#232F3E,color:#fff
        style F fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_data_pipeline_mermaid():
    """Create mermaid diagram for data pipeline"""
    return """
    graph LR
        A[📱 Mobile Apps] --> E[🔄 AWS Glue]
        B[🌐 Web Applications] --> E
        C[🏢 On-Premises DB] --> E
        D[☁️ Cloud Services] --> E
        
        E --> F[📊 Data Catalog]
        E --> G[🔧 Transform Jobs]
        E --> H[📋 Crawlers]
        
        G --> I[🗂️ Amazon S3]
        G --> J[🏢 Amazon Redshift]
        G --> K[📈 Amazon Athena]
        
        I --> L[📊 QuickSight]
        J --> L
        K --> L
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#FF9900,stroke:#232F3E,color:#fff
        style C fill:#FF9900,stroke:#232F3E,color:#fff
        style D fill:#FF9900,stroke:#232F3E,color:#fff
        style E fill:#4B9EDB,stroke:#232F3E,color:#fff
        style L fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_glue_architecture_mermaid():
    """Create mermaid diagram for AWS Glue architecture"""
    return """
    graph TD
        A[🗂️ AWS Glue Data Catalog] --> B[📋 Crawlers]
        A --> C[🔧 ETL Jobs]
        A --> D[📊 Data Discovery]
        
        B --> E[📁 Data Sources]
        E --> F[🏢 RDS]
        E --> G[🗂️ S3]
        E --> H[🏢 Redshift]
        E --> I[🌐 External DBs]
        
        C --> J[🐍 Python/Scala Scripts]
        C --> K[✨ Visual ETL Editor]
        C --> L[🔄 Apache Spark Engine]
        
        L --> M[🎯 Target Systems]
        M --> N[📊 Data Lake]
        M --> O[🏢 Data Warehouse]
        M --> P[📈 Analytics Services]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#4B9EDB,stroke:#232F3E,color:#fff
        style D fill:#4B9EDB,stroke:#232F3E,color:#fff
        style L fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_data_formats_comparison():
    """Create comparison chart for data formats"""
    formats_data = {
        'Format': ['JSON', 'CSV', 'Parquet', 'ORC', 'Avro'],
        'Compression Ratio': [1.0, 1.2, 3.5, 3.8, 2.1],
        'Query Performance': [2, 3, 9, 9, 6],
        'Schema Evolution': [8, 2, 7, 6, 9],
        'Human Readable': [9, 9, 1, 1, 2],
        'Use Case Score': [6, 5, 9, 8, 7]
    }
    
    df = pd.DataFrame(formats_data)
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Compression Efficiency', 'Query Performance', 'Schema Evolution Support', 'Overall Comparison'),
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "polar"}]]
    )
    
    # Compression ratio
    fig.add_trace(go.Bar(x=df['Format'], y=df['Compression Ratio'], 
                        marker_color=AWS_COLORS['primary'], name='Compression'),
                  row=1, col=1)
    
    # Query performance
    fig.add_trace(go.Bar(x=df['Format'], y=df['Query Performance'], 
                        marker_color=AWS_COLORS['light_blue'], name='Performance'),
                  row=1, col=2)
    
    # Schema evolution
    fig.add_trace(go.Bar(x=df['Format'], y=df['Schema Evolution'], 
                        marker_color=AWS_COLORS['success'], name='Schema'),
                  row=2, col=1)
    
    # Radar chart for overall comparison
    fig.add_trace(go.Scatterpolar(
        r=df.loc[df['Format'] == 'Parquet', ['Compression Ratio', 'Query Performance', 'Schema Evolution', 'Human Readable', 'Use Case Score']].values[0],
        theta=['Compression', 'Performance', 'Schema Evolution', 'Readability', 'Use Case'],
        fill='toself',
        name='Parquet',
        marker_color=AWS_COLORS['primary']
    ), row=2, col=2)
    
    fig.add_trace(go.Scatterpolar(
        r=df.loc[df['Format'] == 'JSON', ['Compression Ratio', 'Query Performance', 'Schema Evolution', 'Human Readable', 'Use Case Score']].values[0],
        theta=['Compression', 'Performance', 'Schema Evolution', 'Readability', 'Use Case'],
        fill='toself',
        name='JSON',
        marker_color=AWS_COLORS['warning']
    ), row=2, col=2)
    
    fig.update_layout(height=700, showlegend=True, title_text="Data Format Comparison")
    
    return fig

def transform_process_data_tab():
    """Content for Transform and Process Data tab"""
    st.markdown("# 🔄 Transform and Process Data")
    st.markdown("*Understanding data transformation and processing fundamentals*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Data transformation** is the process of converting data from one format or structure to another, 
    making it suitable for analysis, storage, or integration with other systems. It's like translating 
    languages so different systems can understand each other.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Data Transformation Simulator
    st.markdown("## 🛠️ Interactive Data Transformation Simulator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Source Data Configuration")
        source_format = st.selectbox("Source Format:", ["JSON", "CSV", "XML", "Database Table"])
        data_quality = st.slider("Data Quality (% clean):", 50, 100, 75)
        data_volume = st.selectbox("Data Volume:", ["Small (< 1GB)", "Medium (1-10GB)", "Large (10-100GB)", "Very Large (> 100GB)"], key="data_volume")
        
    with col2:
        st.markdown("### Transformation Requirements")
        transformations = st.multiselect("Select Transformations:", [
            "🧹 Data Cleaning", "🔧 Format Conversion", "📐 Schema Mapping",
            "🔗 Data Enrichment", "🎭 Data Masking", "📊 Aggregation",
            "🔍 Data Validation", "📈 Derived Columns"
        ])
        
        target_format = st.selectbox("Target Format:", ["Parquet", "ORC", "JSON", "CSV", "Database"])
    
    if st.button("🚀 Execute Transformation (Simulation)", use_container_width=True):
        
        # Simulate processing time based on volume and transformations
        base_time = {"Small (< 1GB)": 5, "Medium (1-10GB)": 25, "Large (10-100GB)": 120, "Very Large (> 100GB)": 600}
        processing_time = base_time[data_volume] + len(transformations) * 10
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Transformation Completed Successfully!
        
        **Processing Summary:**
        - **Source**: {source_format} → **Target**: {target_format}
        - **Data Quality Improvement**: {data_quality}% → {min(data_quality + 15, 100)}%
        - **Transformations Applied**: {len(transformations)} operations
        - **Processing Time**: ~{processing_time} minutes
        - **Data Volume**: {data_volume}
        
        **Applied Transformations:**
        {chr(10).join([f"• {t}" for t in transformations]) if transformations else "• No transformations selected"}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Transformation Types
    st.markdown("## 🔧 Types of Data Transformations")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🧹 Data Cleansing
        - **Remove duplicates and errors**
        - Handle missing values
        - Standardize formats
        - **Example**: Email validation, phone number formatting
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Format Conversion
        - **Convert between data formats**
        - Optimize for query performance
        - Reduce storage costs
        - **Example**: JSON to Parquet conversion
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔗 Data Enrichment
        - **Add context and value**
        - Join with reference data
        - Calculate derived fields
        - **Example**: Adding geolocation data
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Performance Metrics
    st.markdown("## 📊 Transformation Performance Metrics")
    
    # Sample data for performance visualization
    performance_data = {
        'Metric': ['Throughput (MB/s)', 'Error Rate (%)', 'Processing Time (min)', 'Cost ($)'],
        'Before Optimization': [50, 5, 120, 25],
        'After Optimization': [200, 1, 30, 15]
    }
    
    df_perf = pd.DataFrame(performance_data)
    
    fig = go.Figure()
    fig.add_trace(go.Bar(name='Before Optimization', x=df_perf['Metric'], y=df_perf['Before Optimization'], 
                        marker_color=AWS_COLORS['warning']))
    fig.add_trace(go.Bar(name='After Optimization', x=df_perf['Metric'], y=df_perf['After Optimization'], 
                        marker_color=AWS_COLORS['success']))
    
    fig.update_layout(title='Data Transformation Performance Comparison', barmode='group')
    st.plotly_chart(fig, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Data Transformation")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Data transformation using pandas and AWS services
import pandas as pd
import boto3
from datetime import datetime

def transform_customer_data(input_df):
    \"\"\"
    Transform raw customer data for analytics
    \"\"\"
    
    # Data Cleaning
    # Remove duplicates
    df_clean = input_df.drop_duplicates(subset=['customer_id'])
    
    # Handle missing values
    df_clean['email'] = df_clean['email'].fillna('unknown@example.com')
    df_clean['phone'] = df_clean['phone'].fillna('000-000-0000')
    
    # Data Standardization
    # Standardize phone numbers
    df_clean['phone_clean'] = df_clean['phone'].str.replace(r'[^0-9]', '', regex=True)
    
    # Standardize email to lowercase
    df_clean['email_clean'] = df_clean['email'].str.lower().str.strip()
    
    # Data Enrichment
    # Calculate customer age from birth_date
    df_clean['birth_date'] = pd.to_datetime(df_clean['birth_date'])
    df_clean['age'] = (datetime.now() - df_clean['birth_date']).dt.days // 365
    
    # Create customer segments based on age
    def categorize_age(age):
        if age < 25: return 'Young Adult'
        elif age < 45: return 'Adult'
        elif age < 65: return 'Middle Age'
        else: return 'Senior'
    
    df_clean['age_segment'] = df_clean['age'].apply(categorize_age)
    
    # Data Validation
    # Validate email format
    email_pattern = r'^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$'
    df_clean['valid_email'] = df_clean['email_clean'].str.match(email_pattern)
    
    # Create data quality score
    df_clean['data_quality_score'] = (
        df_clean['valid_email'].astype(int) +
        (df_clean['phone_clean'].str.len() == 10).astype(int) +
        (df_clean['age'].between(18, 120)).astype(int)
    ) / 3
    
    return df_clean

# Example usage with AWS Glue context
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.context import SparkContext

# Initialize Glue context
glueContext = GlueContext(SparkContext.getOrCreate())

# Read data from S3
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="customer_database", 
    table_name="raw_customers"
)

# Convert to DataFrame for transformation
df = datasource.toDF()

# Apply transformations
transformed_data = transform_customer_data(df.toPandas())

# Convert back to DynamicFrame
transformed_dyf = DynamicFrame.fromDF(
    glueContext.spark_session.createDataFrame(transformed_data), 
    glueContext, 
    "transformed_customers"
)

# Write transformed data to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dyf,
    connection_type="s3",
    connection_options={"path": "s3://my-bucket/transformed-customers/"},
    format="parquet"
)
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def etl_concepts_tab():
    """Content for ETL Concepts tab"""
    st.markdown("# 📊 ETL (Extract, Transform, Load) Concepts")
    st.markdown("*Complete guide to ETL processes and workflows*")
    
    # ETL Overview
    st.markdown("## 🔄 ETL Process Overview")
    common.mermaid(create_etl_overview_mermaid(), height=400)
    
    # Interactive ETL Process Builder
    st.markdown("## 🛠️ Interactive ETL Process Builder")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="etl-step">🔍 EXTRACT</div>', unsafe_allow_html=True)
        extract_sources = st.multiselect("Data Sources:", [
            "📊 MySQL Database", "🗄️ PostgreSQL", "🌐 REST APIs", 
            "📁 CSV Files", "📋 Excel Files", "☁️ Cloud Storage",
            "📡 Streaming Data", "🏢 ERP Systems"
        ])
        extract_frequency = st.selectbox("Extract Frequency:", [
            "Real-time", "Every 15 minutes", "Hourly", "Daily", "Weekly"
        ])
    
    with col2:
        st.markdown('<div class="etl-step">🔧 TRANSFORM</div>', unsafe_allow_html=True)
        transform_operations = st.multiselect("Transformations:", [
            "🧹 Data Cleaning", "🔄 Format Conversion", "📊 Aggregation",
            "🔗 Data Joining", "🎭 Data Masking", "📐 Schema Mapping",
            "🔍 Data Validation", "💰 Currency Conversion"
        ])
        transform_complexity = st.select_slider("Complexity:", ["Simple", "Moderate", "Complex", "Very Complex"])
    
    with col3:
        st.markdown('<div class="etl-step">📤 LOAD</div>', unsafe_allow_html=True)
        load_targets = st.multiselect("Target Systems:", [
            "🏢 Data Warehouse", "📊 Data Lake", "📈 Analytics DB",
            "☁️ Cloud Storage", "📊 BI Tools", "🔄 Operational DB"
        ])
        load_strategy = st.selectbox("Load Strategy:", [
            "Full Load", "Incremental Load", "Upsert", "Delta Load"
        ])
    
    if st.button("🚀 Generate ETL Pipeline (Simulation)", use_container_width=True, key="sim1"):
        
        # Calculate estimated metrics
        complexity_factor = {"Simple": 1, "Moderate": 2, "Complex": 3, "Very Complex": 4}[transform_complexity]
        estimated_time = len(extract_sources) * 15 + len(transform_operations) * 10 + len(load_targets) * 5
        estimated_cost = complexity_factor * 0.50 + len(extract_sources) * 0.10
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ ETL Pipeline Generated Successfully!
        
        **Pipeline Configuration:**
        - **Extract Sources**: {len(extract_sources)} sources ({extract_frequency})
        - **Transform Operations**: {len(transform_operations)} operations ({transform_complexity})
        - **Load Targets**: {len(load_targets)} destinations ({load_strategy})
        
        **Estimated Metrics:**
        - **Processing Time**: ~{estimated_time} minutes per run
        - **Estimated Cost**: ${estimated_cost:.2f} per execution
        - **Data Pipeline ID**: etl-{np.random.randint(100000, 999999)}
        
        **Components Created:**
        • AWS Glue Jobs for transformations
        • CloudWatch monitoring and logging
        • S3 staging areas for intermediate data
        • Data catalog entries for all datasets
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # ETL vs ELT Comparison
    st.markdown("## ⚔️ ETL vs ELT Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="service-comparison">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 ETL (Extract, Transform, Load)
        
        **Process Flow:**
        1. Extract data from sources
        2. Transform data in staging area
        3. Load processed data to target
        
        **Best For:**
        - Structured data processing
        - Complex business rules
        - Data privacy/compliance requirements
        - Limited target system resources
        
        **Tools:** AWS Glue, Data Pipeline, EMR
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="service-comparison">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔃 ELT (Extract, Load, Transform)
        
        **Process Flow:**
        1. Extract data from sources
        2. Load raw data to target system
        3. Transform data within target system
        
        **Best For:**
        - Big data and cloud-native architectures
        - Flexible, schema-on-read approaches
        - Powerful target systems (data lakes)
        - Real-time and streaming data
        
        **Tools:** Amazon Athena, Redshift, Spark
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # ETL Best Practices
    st.markdown("## 💡 ETL Best Practices")
    
    practices_data = {
        'Practice': ['Data Quality Checks', 'Error Handling', 'Monitoring & Logging', 
                    'Incremental Processing', 'Schema Evolution', 'Performance Optimization'],
        'Importance': [95, 90, 85, 88, 75, 92],
        'Implementation Difficulty': [60, 70, 50, 75, 85, 80],
        'ROI Impact': [90, 85, 70, 88, 65, 95]
    }
    
    df_practices = pd.DataFrame(practices_data)
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df_practices['Implementation Difficulty'],
        y=df_practices['ROI Impact'],
        mode='markers+text',
        text=df_practices['Practice'],
        textposition="top center",
        marker=dict(
            size=df_practices['Importance'],
            color=df_practices['Importance'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Importance Score")
        ),
        name='ETL Best Practices'
    ))
    
    fig.update_layout(
        title='ETL Best Practices: Implementation vs ROI',
        xaxis_title='Implementation Difficulty',
        yaxis_title='ROI Impact',
        height=500
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Complete ETL Pipeline")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Complete ETL pipeline using AWS Glue
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def extract_data():
    \"\"\"Extract data from multiple sources\"\"\"
    
    # Extract from RDS MySQL
    rds_data = glueContext.create_dynamic_frame.from_catalog(
        database="sales_db",
        table_name="orders",
        transformation_ctx="rds_source"
    )
    
    # Extract from S3 CSV files
    s3_data = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": ["s3://my-bucket/customer-data/"]},
        format="csv",
        format_options={"withHeader": True},
        transformation_ctx="s3_source"
    )
    
    return rds_data, s3_data

def transform_data(orders_df, customers_df):
    \"\"\"Apply comprehensive data transformations\"\"\"
    
    # Convert to Spark DataFrames for easier manipulation
    orders = orders_df.toDF()
    customers = customers_df.toDF()
    
    # Data Cleaning
    # Remove null order_ids and invalid dates
    orders_clean = orders.filter(
        (F.col("order_id").isNotNull()) &
        (F.col("order_date") >= "2020-01-01")
    )
    
    # Data Enrichment
    # Add calculated fields
    orders_enriched = orders_clean.withColumn(
        "order_year", F.year(F.col("order_date"))
    ).withColumn(
        "order_month", F.month(F.col("order_date"))
    ).withColumn(
        "order_value_category",
        F.when(F.col("total_amount") < 100, "Low")
         .when(F.col("total_amount") < 500, "Medium")
         .otherwise("High")
    )
    
    # Data Joining
    # Join orders with customer data
    joined_data = orders_enriched.join(
        customers,
        orders_enriched.customer_id == customers.customer_id,
        "left"
    )
    
    # Data Aggregation
    # Create summary metrics
    monthly_summary = joined_data.groupBy(
        "order_year", "order_month", "customer_segment"
    ).agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    
    # Data Quality Validation
    # Add data quality flags
    quality_checked = joined_data.withColumn(
        "data_quality_score",
        (F.when(F.col("email").rlike("^[\\\\w\\\\.-]+@[\\\\w\\\\.-]+\\\\.[a-zA-Z]{2,}$"), 1).otherwise(0) +
         F.when(F.col("phone").isNotNull(), 1).otherwise(0) +
         F.when(F.col("total_amount") > 0, 1).otherwise(0)) / 3
    )
    
    return quality_checked, monthly_summary

def load_data(transformed_data, summary_data):
    \"\"\"Load data to multiple targets\"\"\"
    
    # Convert back to DynamicFrames
    main_data_dyf = DynamicFrame.fromDF(transformed_data, glueContext, "main_data")
    summary_data_dyf = DynamicFrame.fromDF(summary_data, glueContext, "summary_data")
    
    # Load to S3 Data Lake (Parquet format with partitioning)
    glueContext.write_dynamic_frame.from_options(
        frame=main_data_dyf,
        connection_type="s3",
        connection_options={
            "path": "s3://my-data-lake/processed-orders/",
            "partitionKeys": ["order_year", "order_month"]
        },
        format="glueparquet",
        transformation_ctx="s3_load"
    )
    
    # Load summary to Redshift
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=summary_data_dyf,
        catalog_connection="redshift-connection",
        connection_options={
            "dbtable": "monthly_order_summary",
            "database": "analytics_db"
        },
        redshift_tmp_dir="s3://my-temp-bucket/redshift-temp/",
        transformation_ctx="redshift_load"
    )

def main():
    \"\"\"Main ETL execution function\"\"\"
    
    try:
        # Extract
        print("Starting data extraction...")
        orders_data, customer_data = extract_data()
        
        # Transform
        print("Applying data transformations...")
        transformed_orders, summary_data = transform_data(orders_data, customer_data)
        
        # Load
        print("Loading data to targets...")
        load_data(transformed_orders, summary_data)
        
        print("ETL pipeline completed successfully!")
        
    except Exception as e:
        print(f"ETL pipeline failed: {str(e)}")
        raise e
    
    finally:
        job.commit()

# Execute the ETL pipeline
if __name__ == "__main__":
    main()
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def data_extraction_tab():
    """Content for Data Extraction tab"""
    st.markdown("# 🔍 Data Extraction")
    st.markdown("*Extracting data from various sources efficiently*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Data extraction** is the process of retrieving data from various source systems to begin the ETL process. 
    It's like collecting ingredients from different pantries before cooking a meal.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Extraction Methods Comparison
    st.markdown("## 📊 Data Extraction Methods")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔔 Update Notification
        - **Source system notifies** when data changes
        - Real-time data extraction
        - Low latency, efficient
        - **Example**: Database triggers, webhooks
        - **Best for**: Critical real-time updates
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📅 Incremental Extraction
        - **Periodic checks** for changes
        - Extract only changed data
        - Scheduled intervals
        - **Example**: Daily batch jobs
        - **Best for**: Regular business reporting
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗂️ Full Extraction
        - **Complete data copy** every time
        - High data transfer volume
        - Resource intensive
        - **Example**: Initial data loads
        - **Best for**: Small tables, historical loads
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Extraction Planner
    st.markdown("## 🛠️ Interactive Extraction Strategy Planner")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Source System Configuration")
        source_type = st.selectbox("Source System Type:", [
            "🗄️ Relational Database (MySQL, PostgreSQL)",
            "📊 Data Warehouse (Redshift, Snowflake)",
            "📁 File System (CSV, JSON, XML)",
            "🌐 REST API",
            "📡 Streaming (Kafka, Kinesis)",
            "☁️ Cloud Storage (S3, GCS)"
        ])
        
        data_size = st.selectbox("Data Volume:", [
            "Small (< 1GB)", "Medium (1-10GB)", "Large (10-100GB)", "Very Large (> 100GB)"
        ])
        
        change_frequency = st.selectbox("Data Change Frequency:", [
            "Real-time", "Every few minutes", "Hourly", "Daily", "Weekly", "Monthly"
        ])
    
    with col2:
        st.markdown("### Business Requirements")
        latency_requirement = st.selectbox("Latency Requirement:", [
            "Real-time (< 1 minute)", "Near real-time (< 15 minutes)", 
            "Batch (hours)", "Historical (days)"
        ])
        
        availability_window = st.selectbox("Source System Availability:", [
            "24/7 Available", "Business hours only", "Maintenance windows"
        ])
        
        network_bandwidth = st.selectbox("Network Bandwidth:", [
            "High (> 1 Gbps)", "Medium (100 Mbps - 1 Gbps)", "Low (< 100 Mbps)"
        ])
    
    if st.button("🎯 Recommend Extraction Strategy", use_container_width=True):
        
        # Logic to recommend strategy
        if change_frequency in ["Real-time", "Every few minutes"] and latency_requirement == "Real-time (< 1 minute)":
            strategy = "Update Notification"
            aws_service = "Amazon Kinesis Data Streams"
            explanation = "Real-time requirements demand immediate data capture"
        elif data_size in ["Large (10-100GB)", "Very Large (> 100GB)"] and change_frequency in ["Daily", "Weekly"]:
            strategy = "Incremental Extraction"
            aws_service = "AWS Glue with bookmarks"
            explanation = "Large datasets benefit from incremental processing"
        else:
            strategy = "Incremental Extraction"
            aws_service = "AWS Data Pipeline"
            explanation = "Balanced approach for most use cases"
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 🎯 Recommended Extraction Strategy
        
        **Strategy**: {strategy}
        **Recommended AWS Service**: {aws_service}
        
        **Reasoning**: {explanation}
        
        **Implementation Details**:
        - **Source**: {source_type}
        - **Volume**: {data_size}
        - **Frequency**: {change_frequency}
        - **Latency**: {latency_requirement}
        - **Estimated Cost**: ${"50-200" if data_size.startswith("Large") else "10-50"}/month
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Extraction Performance Comparison
    st.markdown("## 📊 Extraction Method Performance")
    
    performance_data = {
        'Method': ['Update Notification', 'Incremental Extraction', 'Full Extraction'],
        'Latency (minutes)': [1, 15, 60],
        'Network Usage (GB/hour)': [0.1, 2, 20],
        'CPU Usage (%)': [10, 30, 80],
        'Complexity Score': [8, 5, 2]
    }
    
    df_performance = pd.DataFrame(performance_data)
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Latency Comparison', 'Network Usage', 'CPU Usage', 'Implementation Complexity'),
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "bar"}]]
    )
    
    colors = [AWS_COLORS['success'], AWS_COLORS['primary'], AWS_COLORS['warning']]
    
    # Latency
    fig.add_trace(go.Bar(x=df_performance['Method'], y=df_performance['Latency (minutes)'], 
                        marker_color=colors, name='Latency'), row=1, col=1)
    
    # Network Usage
    fig.add_trace(go.Bar(x=df_performance['Method'], y=df_performance['Network Usage (GB/hour)'], 
                        marker_color=colors, name='Network'), row=1, col=2)
    
    # CPU Usage
    fig.add_trace(go.Bar(x=df_performance['Method'], y=df_performance['CPU Usage (%)'], 
                        marker_color=colors, name='CPU'), row=2, col=1)
    
    # Complexity
    fig.add_trace(go.Bar(x=df_performance['Method'], y=df_performance['Complexity Score'], 
                        marker_color=colors, name='Complexity'), row=2, col=2)
    
    fig.update_layout(height=600, showlegend=False, title_text="Data Extraction Method Comparison")
    st.plotly_chart(fig, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Data Extraction Strategies")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Various data extraction strategies using AWS services
import boto3
import pandas as pd
from datetime import datetime, timedelta
import json

class DataExtractor:
    def __init__(self):
        self.glue = boto3.client('glue')
        self.s3 = boto3.client('s3')
        self.rds = boto3.client('rds-data')
    
    def full_extraction(self, source_config):
        \"\"\"
        Full extraction - extract all data from source
        Best for: Initial loads, small datasets
        \"\"\"
        
        print("Starting full extraction...")
        
        # Example: Extract all data from RDS table
        sql_query = f\"\"\"
        SELECT * FROM {source_config['table_name']}
        WHERE created_date >= '{source_config['start_date']}'
        \"\"\"
        
        response = self.rds.execute_statement(
            resourceArn=source_config['cluster_arn'],
            secretArn=source_config['secret_arn'],
            database=source_config['database'],
            sql=sql_query
        )
        
        print(f"Extracted {len(response['records'])} records")
        return response['records']
    
    def incremental_extraction(self, source_config, last_extraction_time):
        \"\"\"
        Incremental extraction - extract only changed data
        Best for: Regular batch processing
        \"\"\"
        
        print(f"Starting incremental extraction from {last_extraction_time}")
        
        # Extract only records modified since last extraction
        sql_query = f\"\"\"
        SELECT * FROM {source_config['table_name']}
        WHERE modified_date > '{last_extraction_time}'
        ORDER BY modified_date ASC
        \"\"\"
        
        response = self.rds.execute_statement(
            resourceArn=source_config['cluster_arn'],
            secretArn=source_config['secret_arn'],
            database=source_config['database'],
            sql=sql_query
        )
        
        # Update bookmark for next extraction
        if response['records']:
            latest_timestamp = max([record['modified_date']['stringValue'] 
                                  for record in response['records']])
            self.update_extraction_bookmark(source_config['bookmark_key'], 
                                          latest_timestamp)
        
        print(f"Extracted {len(response['records'])} incremental records")
        return response['records']
    
    def streaming_extraction(self, kinesis_stream_name):
        \"\"\"
        Streaming extraction - real-time data capture
        Best for: Real-time analytics, event processing
        \"\"\"
        
        kinesis = boto3.client('kinesis')
        
        # Get stream description
        stream_description = kinesis.describe_stream(StreamName=kinesis_stream_name)
        
        # Get shard iterator
        shard_id = stream_description['StreamDescription']['Shards'][0]['ShardId']
        
        shard_iterator_response = kinesis.get_shard_iterator(
            StreamName=kinesis_stream_name,
            ShardId=shard_id,
            ShardIteratorType='LATEST'
        )
        
        shard_iterator = shard_iterator_response['ShardIterator']
        
        # Continuously read from stream
        while True:
            records_response = kinesis.get_records(ShardIterator=shard_iterator)
            
            records = records_response['Records']
            if records:
                print(f"Processing {len(records)} streaming records")
                for record in records:
                    data = json.loads(record['Data'])
                    self.process_streaming_record(data)
            
            # Get next shard iterator
            shard_iterator = records_response.get('NextShardIterator')
            if not shard_iterator:
                break
    
    def api_extraction(self, api_config):
        \"\"\"
        API-based extraction - extract from REST APIs
        Best for: Third-party data sources, SaaS applications
        \"\"\"
        
        import requests
        
        headers = {
            'Authorization': f"Bearer {api_config['api_token']}",
            'Content-Type': 'application/json'
        }
        
        all_data = []
        page = 1
        
        while True:
            # Paginated API calls
            response = requests.get(
                f"{api_config['base_url']}/api/data",
                headers=headers,
                params={
                    'page': page,
                    'limit': api_config.get('page_size', 1000),
                    'modified_since': api_config.get('last_sync_time')
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if not data.get('results'):
                    break  # No more data
                
                all_data.extend(data['results'])
                page += 1
                
                # Rate limiting
                import time
                time.sleep(api_config.get('rate_limit_delay', 0.1))
            else:
                print(f"API request failed: {response.status_code}")
                break
        
        print(f"Extracted {len(all_data)} records from API")
        return all_data
    
    def file_extraction(self, s3_config):
        \"\"\"
        File-based extraction - extract from files in S3
        Best for: Batch file processing, data lake ingestion
        \"\"\"
        
        # List files in S3 bucket
        response = self.s3.list_objects_v2(
            Bucket=s3_config['bucket'],
            Prefix=s3_config['prefix']
        )
        
        all_data = []
        
        for obj in response.get('Contents', []):
            file_key = obj['Key']
            
            # Skip if file already processed
            if self.is_file_processed(file_key):
                continue
            
            print(f"Processing file: {file_key}")
            
            # Download and process file
            file_obj = self.s3.get_object(Bucket=s3_config['bucket'], Key=file_key)
            
            if file_key.endswith('.csv'):
                df = pd.read_csv(file_obj['Body'])
                all_data.extend(df.to_dict('records'))
            elif file_key.endswith('.json'):
                data = json.loads(file_obj['Body'].read())
                all_data.extend(data if isinstance(data, list) else [data])
            
            # Mark file as processed
            self.mark_file_processed(file_key)
        
        print(f"Extracted {len(all_data)} records from {len(response.get('Contents', []))} files")
        return all_data
    
    def update_extraction_bookmark(self, bookmark_key, timestamp):
        \"\"\"Update extraction bookmark for incremental processing\"\"\"
        
        # Store bookmark in DynamoDB or S3
        self.s3.put_object(
            Bucket='my-bookmarks-bucket',
            Key=f'extraction-bookmarks/{bookmark_key}',
            Body=timestamp
        )
    
    def process_streaming_record(self, record):
        \"\"\"Process individual streaming record\"\"\"
        
        # Add your record processing logic here
        print(f"Processing record: {record.get('id', 'unknown')}")
    
    def is_file_processed(self, file_key):
        \"\"\"Check if file has been processed before\"\"\"
        
        try:
            self.s3.head_object(
                Bucket='my-processed-files-bucket',
                Key=f'processed/{file_key}'
            )
            return True
        except:
            return False
    
    def mark_file_processed(self, file_key):
        \"\"\"Mark file as processed\"\"\"
        
        self.s3.put_object(
            Bucket='my-processed-files-bucket',
            Key=f'processed/{file_key}',
            Body=str(datetime.now())
        )

# Example usage
if __name__ == "__main__":
    extractor = DataExtractor()
    
    # Configure different extraction methods based on requirements
    
    # For large historical data loads
    full_config = {
        'table_name': 'orders',
        'start_date': '2023-01-01',
        'cluster_arn': 'arn:aws:rds:us-east-1:123456789:cluster:my-cluster',
        'secret_arn': 'arn:aws:secretsmanager:us-east-1:123456789:secret:db-secret',
        'database': 'production'
    }
    
    # For regular batch updates
    incremental_config = {
        **full_config,
        'bookmark_key': 'orders_extraction'
    }
    
    # Execute appropriate extraction strategy
    print("Executing data extraction strategies...")
    
    # Full extraction for initial load
    # full_data = extractor.full_extraction(full_config)
    
    # Incremental extraction for regular updates
    # last_sync = datetime.now() - timedelta(hours=1)
    # incremental_data = extractor.incremental_extraction(incremental_config, last_sync)
    
    print("Data extraction completed!")
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def data_transformation_tab():
    """Content for Data Transformation tab"""
    st.markdown("# 🔧 Data Transformation")
    st.markdown("*Converting and enriching data for analysis and storage*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Data transformation** converts data from its original format into a format suitable for analysis or storage. 
    It includes cleaning, enriching, formatting, and validating data to ensure quality and consistency.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Format Comparison
    st.markdown("## 📋 Data Format Optimization")
    st.plotly_chart(create_data_formats_comparison(), use_container_width=True)
    
    # Interactive Transformation Workshop
    st.markdown("## 🛠️ Interactive Data Transformation Workshop")
    
    # Sample data for transformation
    sample_data = {
        'customer_id': [1, 2, 3, 4, 5],
        'name': ['John Doe', 'jane smith', 'BOB WILSON', 'Alice Johnson', 'charlie brown'],
        'email': ['john.doe@email.com', 'JANE@COMPANY.COM', 'bob@invalid', 'alice@test.co.uk', ''],
        'phone': ['555-123-4567', '5551234567', '555.123.4567', '+1-555-123-4567', None],
        'purchase_amount': [150.50, 299.99, 75.00, 525.25, 89.99],
        'purchase_date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19'],
        'customer_segment': ['premium', 'standard', 'premium', 'gold', 'standard']
    }
    
    df_sample = pd.DataFrame(sample_data)
    
    st.markdown("### 📊 Sample Raw Data")
    st.dataframe(df_sample, use_container_width=True)
    
    # Transformation options
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔧 Select Transformations")
        transformations = st.multiselect("Apply Transformations:", [
            "🧹 Standardize Names (Title Case)",
            "📧 Clean Email Addresses",
            "📞 Normalize Phone Numbers", 
            "💰 Add Purchase Categories",
            "📅 Extract Date Components",
            "🎭 Mask Sensitive Data",
            "🔍 Add Data Quality Scores"
        ])
    
    with col2:
        st.markdown("### ⚙️ Transformation Settings")
        name_format = st.selectbox("Name Format:", ["Title Case", "Upper Case", "Lower Case"])
        phone_format = st.selectbox("Phone Format:", ["(555) 123-4567", "555-123-4567", "5551234567"])
        mask_emails = st.checkbox("Mask email domains", value=False)
        
    if st.button("🚀 Apply Transformations", use_container_width=True):
        
        # Create transformed dataframe
        df_transformed = df_sample.copy()
        
        if "🧹 Standardize Names (Title Case)" in transformations:
            df_transformed['name'] = df_transformed['name'].str.title()
        
        if "📧 Clean Email Addresses" in transformations:
            df_transformed['email'] = df_transformed['email'].str.lower().str.strip()
            df_transformed['email'] = df_transformed['email'].replace('', 'unknown@example.com')
        
        if "📞 Normalize Phone Numbers" in transformations:
            # Remove all non-numeric characters, then format
            df_transformed['phone_clean'] = df_transformed['phone'].str.replace(r'[^\d]', '', regex=True)
            df_transformed['phone_formatted'] = df_transformed['phone_clean'].apply(
                lambda x: f"({x[:3]}) {x[3:6]}-{x[6:]}" if pd.notna(x) and len(str(x)) == 10 else "Invalid"
            )
        
        if "💰 Add Purchase Categories" in transformations:
            df_transformed['purchase_category'] = pd.cut(
                df_transformed['purchase_amount'], 
                bins=[0, 100, 300, float('inf')], 
                labels=['Low', 'Medium', 'High']
            )
        
        if "📅 Extract Date Components" in transformations:
            df_transformed['purchase_date'] = pd.to_datetime(df_transformed['purchase_date'])
            df_transformed['purchase_year'] = df_transformed['purchase_date'].dt.year
            df_transformed['purchase_month'] = df_transformed['purchase_date'].dt.month
            df_transformed['purchase_day_of_week'] = df_transformed['purchase_date'].dt.day_name()
        
        if "🎭 Mask Sensitive Data" in transformations and mask_emails:
            df_transformed['email_masked'] = df_transformed['email'].apply(
                lambda x: x[:3] + "***@" + x.split('@')[1] if '@' in str(x) else x
            )
        
        if "🔍 Add Data Quality Scores" in transformations:
            def calculate_quality_score(row):
                score = 0
                if pd.notna(row['name']) and row['name'].strip(): score += 1
                if '@' in str(row['email']) and '.' in str(row['email']): score += 1
                if pd.notna(row.get('phone_clean')) and len(str(row.get('phone_clean', ''))) == 10: score += 1
                if pd.notna(row['purchase_amount']) and row['purchase_amount'] > 0: score += 1
                return score / 4
            
            df_transformed['data_quality_score'] = df_transformed.apply(calculate_quality_score, axis=1)
        
        st.markdown("### ✨ Transformed Data")
        st.dataframe(df_transformed, use_container_width=True)
        
        # Show transformation summary
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 📊 Transformation Summary
        
        **Transformations Applied**: {len(transformations)}
        **Records Processed**: {len(df_transformed)}
        **New Columns Added**: {len(df_transformed.columns) - len(df_sample.columns)}
        **Data Quality Improvement**: 
        - Before: {np.random.randint(60, 75)}%
        - After: {np.random.randint(85, 95)}%
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Transformation Performance Analysis
    st.markdown("## 📊 Transformation Performance Analysis")
    
    transformation_metrics = {
        'Transformation Type': ['Data Cleaning', 'Format Conversion', 'Data Enrichment', 'Validation', 'Aggregation'],
        'Processing Time (ms/record)': [2.5, 1.8, 5.2, 3.1, 4.7],
        'Memory Usage (MB/1M records)': [50, 30, 120, 75, 95],
        'CPU Usage (%)': [15, 10, 35, 25, 40],
        'Error Reduction (%)': [85, 20, 5, 95, 10]
    }
    
    df_metrics = pd.DataFrame(transformation_metrics)
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Processing Time', 'Memory Usage', 'CPU Usage', 'Error Reduction'),
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "bar"}]]
    )
    
    # Processing time
    fig.add_trace(go.Bar(x=df_metrics['Transformation Type'], y=df_metrics['Processing Time (ms/record)'], 
                        marker_color=AWS_COLORS['primary'], name='Time'), row=1, col=1)
    
    # Memory usage
    fig.add_trace(go.Bar(x=df_metrics['Transformation Type'], y=df_metrics['Memory Usage (MB/1M records)'], 
                        marker_color=AWS_COLORS['light_blue'], name='Memory'), row=1, col=2)
    
    # CPU usage
    fig.add_trace(go.Bar(x=df_metrics['Transformation Type'], y=df_metrics['CPU Usage (%)'], 
                        marker_color=AWS_COLORS['warning'], name='CPU'), row=2, col=1)
    
    # Error reduction
    fig.add_trace(go.Bar(x=df_metrics['Transformation Type'], y=df_metrics['Error Reduction (%)'], 
                        marker_color=AWS_COLORS['success'], name='Error Reduction'), row=2, col=2)
    
    fig.update_layout(height=600, showlegend=False, title_text="Data Transformation Performance Metrics")
    st.plotly_chart(fig, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Advanced Data Transformations")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Advanced data transformation using PySpark and AWS Glue
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import re

class AdvancedDataTransformer:
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def data_cleaning_transformations(self, df):
        \"\"\"Comprehensive data cleaning operations\"\"\"
        
        print("Applying data cleaning transformations...")
        
        # Remove duplicate records
        df_clean = df.dropDuplicates()
        
        # Handle null values with business logic
        df_clean = df_clean.fillna({
            'customer_name': 'Unknown Customer',
            'email': 'no-email@example.com',
            'phone': '000-000-0000',
            'purchase_amount': 0.0
        })
        
        # Standardize text fields
        df_clean = df_clean.withColumn(
            'customer_name_clean',
            F.initcap(F.trim(F.regexp_replace('customer_name', r'[^a-zA-Z\\s]', '')))
        )
        
        # Clean and validate email addresses
        df_clean = df_clean.withColumn(
            'email_clean',
            F.lower(F.trim(F.col('email')))
        ).withColumn(
            'email_valid',
            F.col('email_clean').rlike(r'^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$')
        )
        
        # Normalize phone numbers
        df_clean = df_clean.withColumn(
            'phone_digits_only',
            F.regexp_replace('phone', r'[^0-9]', '')
        ).withColumn(
            'phone_normalized',
            F.when(F.length('phone_digits_only') == 10,
                   F.concat(F.lit('('), F.substring('phone_digits_only', 1, 3), F.lit(') '),
                           F.substring('phone_digits_only', 4, 3), F.lit('-'),
                           F.substring('phone_digits_only', 7, 4)))
             .otherwise(F.lit('Invalid Phone'))
        )
        
        return df_clean
    
    def data_enrichment_transformations(self, df):
        \"\"\"Add calculated fields and business intelligence\"\"\"
        
        print("Applying data enrichment transformations...")
        
        # Add time-based features
        df_enriched = df.withColumn(
            'purchase_date_parsed', F.to_date('purchase_date', 'yyyy-MM-dd')
        ).withColumn(
            'purchase_year', F.year('purchase_date_parsed')
        ).withColumn(
            'purchase_month', F.month('purchase_date_parsed')
        ).withColumn(
            'purchase_quarter', F.quarter('purchase_date_parsed')
        ).withColumn(
            'purchase_day_of_week', F.dayofweek('purchase_date_parsed')
        ).withColumn(
            'purchase_day_name', 
            F.when(F.col('purchase_day_of_week') == 1, 'Sunday')
             .when(F.col('purchase_day_of_week') == 2, 'Monday')
             .when(F.col('purchase_day_of_week') == 3, 'Tuesday')
             .when(F.col('purchase_day_of_week') == 4, 'Wednesday')
             .when(F.col('purchase_day_of_week') == 5, 'Thursday')
             .when(F.col('purchase_day_of_week') == 6, 'Friday')
             .otherwise('Saturday')
        )
        
        # Customer segmentation based on purchase behavior
        window_spec = Window.partitionBy('customer_id').orderBy('purchase_date_parsed')
        
        df_enriched = df_enriched.withColumn(
            'customer_total_spent',
            F.sum('purchase_amount').over(Window.partitionBy('customer_id'))
        ).withColumn(
            'customer_order_count',
            F.count('*').over(Window.partitionBy('customer_id'))
        ).withColumn(
            'customer_avg_order_value',
            F.col('customer_total_spent') / F.col('customer_order_count')
        )
        
        # Customer lifetime value calculation
        df_enriched = df_enriched.withColumn(
            'customer_segment',
            F.when(F.col('customer_total_spent') > 1000, 'High Value')
             .when(F.col('customer_total_spent') > 500, 'Medium Value')
             .otherwise('Low Value')
        )
        
        # RFM Analysis (Recency, Frequency, Monetary)
        current_date = F.current_date()
        
        df_enriched = df_enriched.withColumn(
            'days_since_last_purchase',
            F.datediff(current_date, 
                      F.max('purchase_date_parsed').over(Window.partitionBy('customer_id')))
        ).withColumn(
            'recency_score',
            F.when(F.col('days_since_last_purchase') <= 30, 5)
             .when(F.col('days_since_last_purchase') <= 90, 4)
             .when(F.col('days_since_last_purchase') <= 180, 3)
             .when(F.col('days_since_last_purchase') <= 365, 2)
             .otherwise(1)
        ).withColumn(
            'frequency_score',
            F.when(F.col('customer_order_count') >= 10, 5)
             .when(F.col('customer_order_count') >= 5, 4)
             .when(F.col('customer_order_count') >= 3, 3)
             .when(F.col('customer_order_count') >= 2, 2)
             .otherwise(1)
        ).withColumn(
            'monetary_score',
            F.when(F.col('customer_total_spent') >= 1000, 5)
             .when(F.col('customer_total_spent') >= 500, 4)
             .when(F.col('customer_total_spent') >= 200, 3)
             .when(F.col('customer_total_spent') >= 100, 2)
             .otherwise(1)
        )
        
        # Combined RFM score
        df_enriched = df_enriched.withColumn(
            'rfm_score',
            F.col('recency_score') + F.col('frequency_score') + F.col('monetary_score')
        ).withColumn(
            'rfm_segment',
            F.when(F.col('rfm_score') >= 12, 'Champions')
             .when(F.col('rfm_score') >= 9, 'Loyal Customers')
             .when(F.col('rfm_score') >= 6, 'Potential Loyalists')
             .otherwise('At Risk')
        )
        
        return df_enriched
    
    def data_validation_transformations(self, df):
        \"\"\"Add data quality and validation checks\"\"\"
        
        print("Applying data validation transformations...")
        
        # Data quality scoring
        df_validated = df.withColumn(
            'email_quality_score',
            F.when(F.col('email_valid') == True, 1.0).otherwise(0.0)
        ).withColumn(
            'phone_quality_score',
            F.when(F.col('phone_normalized') != 'Invalid Phone', 1.0).otherwise(0.0)
        ).withColumn(
            'name_quality_score',
            F.when((F.col('customer_name_clean').isNotNull()) & 
                   (F.length('customer_name_clean') > 2), 1.0).otherwise(0.0)
        ).withColumn(
            'purchase_quality_score',
            F.when((F.col('purchase_amount') > 0) & 
                   (F.col('purchase_amount') < 10000), 1.0).otherwise(0.0)
        )
        
        # Overall data quality score
        df_validated = df_validated.withColumn(
            'overall_quality_score',
            (F.col('email_quality_score') + 
             F.col('phone_quality_score') + 
             F.col('name_quality_score') + 
             F.col('purchase_quality_score')) / 4
        )
        
        # Data completeness flags
        df_validated = df_validated.withColumn(
            'is_complete_record',
            (F.col('overall_quality_score') >= 0.75)
        )
        
        # Outlier detection for purchase amounts
        # Calculate quartiles for outlier detection
        quantiles = df.select(
            F.expr('percentile_approx(purchase_amount, 0.25)').alias('Q1'),
            F.expr('percentile_approx(purchase_amount, 0.75)').alias('Q3')
        ).collect()[0]
        
        Q1, Q3 = quantiles['Q1'], quantiles['Q3']
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        df_validated = df_validated.withColumn(
            'is_purchase_outlier',
            (F.col('purchase_amount') < lower_bound) | 
            (F.col('purchase_amount') > upper_bound)
        )
        
        return df_validated
    
    def format_conversion_transformations(self, df):
        \"\"\"Convert data formats for optimal storage and querying\"\"\"
        
        print("Applying format conversion transformations...")
        
        # Optimize data types
        df_optimized = df.withColumn(
            'customer_id', F.col('customer_id').cast(IntegerType())
        ).withColumn(
            'purchase_amount', F.col('purchase_amount').cast(DecimalType(10, 2))
        ).withColumn(
            'purchase_date_parsed', F.col('purchase_date_parsed').cast(DateType())
        )
        
        # Create nested structures for related data
        df_optimized = df_optimized.withColumn(
            'customer_info',
            F.struct(
                F.col('customer_name_clean').alias('name'),
                F.col('email_clean').alias('email'),
                F.col('phone_normalized').alias('phone'),
                F.col('customer_segment').alias('segment')
            )
        ).withColumn(
            'purchase_info',
            F.struct(
                F.col('purchase_amount').alias('amount'),
                F.col('purchase_date_parsed').alias('date'),
                F.col('purchase_quarter').alias('quarter'),
                F.col('purchase_day_name').alias('day_of_week')
            )
        ).withColumn(
            'quality_metrics',
            F.struct(
                F.col('overall_quality_score').alias('score'),
                F.col('is_complete_record').alias('is_complete'),
                F.col('is_purchase_outlier').alias('is_outlier')
            )
        )
        
        return df_optimized
    
    def aggregate_transformations(self, df):
        \"\"\"Create aggregated views for analytics\"\"\"
        
        print("Creating aggregated transformations...")
        
        # Customer-level aggregations
        customer_summary = df.groupBy('customer_id', 'customer_name_clean') \
            .agg(
                F.count('*').alias('total_orders'),
                F.sum('purchase_amount').alias('total_spent'),
                F.avg('purchase_amount').alias('avg_order_value'),
                F.min('purchase_date_parsed').alias('first_purchase_date'),
                F.max('purchase_date_parsed').alias('last_purchase_date'),
                F.first('customer_segment').alias('customer_segment'),
                F.first('rfm_segment').alias('rfm_segment'),
                F.avg('overall_quality_score').alias('avg_data_quality')
            )
        
        # Time-based aggregations
        monthly_summary = df.groupBy('purchase_year', 'purchase_month') \
            .agg(
                F.count('*').alias('total_orders'),
                F.sum('purchase_amount').alias('total_revenue'),
                F.avg('purchase_amount').alias('avg_order_value'),
                F.countDistinct('customer_id').alias('unique_customers'),
                F.avg('overall_quality_score').alias('avg_data_quality')
            )
        
        # Segment-based aggregations  
        segment_summary = df.groupBy('customer_segment', 'rfm_segment') \
            .agg(
                F.count('*').alias('total_orders'),
                F.sum('purchase_amount').alias('total_revenue'),
                F.avg('purchase_amount').alias('avg_order_value'),
                F.countDistinct('customer_id').alias('unique_customers')
            )
        
        return customer_summary, monthly_summary, segment_summary

# Example usage in AWS Glue job
def main():
    \"\"\"Main transformation execution\"\"\"
    
    # Initialize Spark session
    spark = SparkSession.builder \\
        .appName("AdvancedDataTransformation") \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
        .getOrCreate()
    
    transformer = AdvancedDataTransformer(spark)
    
    # Read source data
    source_df = spark.read \\
        .option("header", "true") \\
        .option("inferSchema", "true") \\
        .csv("s3://my-bucket/raw-data/customer-orders/")
    
    print(f"Source data count: {source_df.count()}")
    
    # Apply transformations in sequence
    cleaned_df = transformer.data_cleaning_transformations(source_df)
    enriched_df = transformer.data_enrichment_transformations(cleaned_df)
    validated_df = transformer.data_validation_transformations(enriched_df)
    optimized_df = transformer.format_conversion_transformations(validated_df)
    
    # Create aggregated views
    customer_agg, monthly_agg, segment_agg = transformer.aggregate_transformations(optimized_df)
    
    # Write results to different targets
    print("Writing transformed data...")
    
    # Write detailed transformed data to data lake
    optimized_df.write \\
        .mode("overwrite") \\
        .partitionBy("purchase_year", "purchase_month") \\
        .parquet("s3://my-data-lake/transformed-orders/")
    
    # Write aggregated data for analytics
    customer_agg.write.mode("overwrite") \\
        .parquet("s3://my-data-lake/customer-summary/")
    
    monthly_agg.write.mode("overwrite") \\
        .parquet("s3://my-data-lake/monthly-summary/")
    
    segment_agg.write.mode("overwrite") \\
        .parquet("s3://my-data-lake/segment-summary/")
    
    print("Data transformation completed successfully!")
    
    # Print transformation statistics
    print(f"Total records processed: {optimized_df.count()}")
    print(f"Average data quality score: {optimized_df.agg(F.avg('overall_quality_score')).collect()[0][0]:.2f}")
    
    spark.stop()

if __name__ == "__main__":
    main()
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def data_loading_tab():
    """Content for Data Loading tab"""
    st.markdown("# 📤 Data Loading")
    st.markdown("*Loading transformed data into target systems efficiently*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Data loading** is the final step in the ETL process where transformed data is moved from staging areas 
    into target systems like data warehouses, data lakes, or operational databases. It's like organizing 
    your processed groceries into the right pantry shelves.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Loading Strategies Comparison
    st.markdown("## 🚛 Data Loading Strategies")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗂️ Full Load
        
        **Process:**
        - Load complete dataset every time
        - Replace all existing data
        - Simple but resource-intensive
        
        **Best For:**
        - Initial data loads
        - Small datasets (< 1GB)
        - Historical data migration
        - Data integrity requirements
        
        **Pros:** Simple, ensures data consistency
        **Cons:** High resource usage, longer processing time
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📈 Incremental Load
        
        **Process:**
        - Load only new/changed data
        - Append or update existing records
        - Efficient and scalable
        
        **Best For:**
        - Regular data updates
        - Large datasets (> 1GB)
        - Real-time requirements
        - Cost optimization
        
        **Pros:** Fast, efficient, cost-effective
        **Cons:** Complex logic, requires change tracking
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Loading Strategy Planner
    st.markdown("## 🛠️ Interactive Loading Strategy Planner")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Data Characteristics")
        data_volume = st.selectbox("Data Volume:", [
            "Small (< 100MB)", "Medium (100MB - 1GB)", 
            "Large (1GB - 10GB)", "Very Large (> 10GB)"
        ])
        
        update_frequency = st.selectbox("Update Frequency:", [
            "Real-time", "Every 15 minutes", "Hourly", "Daily", "Weekly"
        ])
        
        data_volatility = st.selectbox("Data Change Rate:", [
            "High (> 50% records change)", "Medium (10-50% change)", 
            "Low (< 10% change)", "Append-only"
        ])
    
    with col2:
        st.markdown("### Target System")
        target_system = st.selectbox("Target System:", [
            "🏢 Amazon Redshift (Data Warehouse)",
            "📊 Amazon S3 (Data Lake)", 
            "🗄️ RDS Database",
            "📈 Amazon DynamoDB",
            "🔍 Amazon OpenSearch"
        ])
        
        business_criticality = st.selectbox("Business Criticality:", [
            "Critical (Zero downtime)", "High (Minimal downtime)", 
            "Medium (Scheduled windows)", "Low (Flexible timing)"
        ])
        
        consistency_requirement = st.selectbox("Consistency Requirement:", [
            "Strong (ACID compliance)", "Eventual consistency", "Best effort"
        ])
    
    if st.button("🎯 Recommend Loading Strategy", use_container_width=True):
        
        # Strategy recommendation logic
        if data_volume in ["Large (1GB - 10GB)", "Very Large (> 10GB)"] and data_volatility == "Low (< 10% change)":
            strategy = "Incremental Load"
            method = "Delta/CDC-based loading"
        elif update_frequency in ["Real-time", "Every 15 minutes"] and business_criticality == "Critical (Zero downtime)":
            strategy = "Streaming Load"
            method = "Real-time streaming ingestion"
        elif data_volume == "Small (< 100MB)" and update_frequency in ["Daily", "Weekly"]:
            strategy = "Full Load"
            method = "Complete data replacement"
        else:
            strategy = "Hybrid Load"
            method = "Combination of full and incremental"
        
        # Estimate performance metrics
        volume_factor = {"Small (< 100MB)": 1, "Medium (100MB - 1GB)": 5, 
                        "Large (1GB - 10GB)": 25, "Very Large (> 10GB)": 100}
        
        estimated_time = volume_factor[data_volume] * (2 if strategy == "Full Load" else 0.5)
        estimated_cost = volume_factor[data_volume] * 0.10
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 🎯 Recommended Loading Strategy
        
        **Strategy**: {strategy}
        **Method**: {method}
        **Target**: {target_system}
        
        **Performance Estimates:**
        - ⏱️ **Processing Time**: ~{estimated_time} minutes
        - 💰 **Estimated Cost**: ${estimated_cost:.2f} per run
        - 🔄 **Update Frequency**: {update_frequency}
        - 📊 **Data Volume**: {data_volume}
        
        **Implementation Components:**
        - AWS Glue jobs for data transformation
        - S3 staging area for intermediate storage
        - CloudWatch monitoring and alerting
        - Data quality validation checkpoints
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Loading Performance Metrics
    st.markdown("## 📊 Loading Performance Analysis")
    
    # Sample performance data
    performance_scenarios = {
        'Scenario': ['Full Load - Small Data', 'Full Load - Large Data', 
                    'Incremental Load - Small Changes', 'Incremental Load - Large Changes',
                    'Streaming Load - Real-time'],
        'Processing Time (min)': [5, 120, 2, 15, 0.1],
        'Cost per Run ($)': [2, 50, 0.5, 8, 0.01],
        'Network Usage (GB)': [0.5, 100, 0.1, 10, 0.001],
        'Target System Load (%)': [80, 95, 20, 60, 5]
    }
    
    df_performance = pd.DataFrame(performance_scenarios)
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Processing Time Comparison', 'Cost Analysis', 'Network Usage', 'System Load Impact'),
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "bar"}]]
    )
    
    colors = [AWS_COLORS['warning'], AWS_COLORS['primary'], AWS_COLORS['success'], 
              AWS_COLORS['light_blue'], AWS_COLORS['purple']]
    
    # Processing time
    fig.add_trace(go.Bar(x=df_performance['Scenario'], y=df_performance['Processing Time (min)'], 
                        marker_color=colors, name='Time'), row=1, col=1)
    
    # Cost
    fig.add_trace(go.Bar(x=df_performance['Scenario'], y=df_performance['Cost per Run ($)'], 
                        marker_color=colors, name='Cost'), row=1, col=2)
    
    # Network usage
    fig.add_trace(go.Bar(x=df_performance['Scenario'], y=df_performance['Network Usage (GB)'], 
                        marker_color=colors, name='Network'), row=2, col=1)
    
    # System load
    fig.add_trace(go.Bar(x=df_performance['Scenario'], y=df_performance['Target System Load (%)'], 
                        marker_color=colors, name='Load'), row=2, col=2)
    
    fig.update_layout(height=600, showlegend=False, title_text="Data Loading Performance Comparison")
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)
    
    # Loading Best Practices
    st.markdown("## 💡 Data Loading Best Practices")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Batch Size Optimization
        - **Right-size batch processing**
        - Balance between throughput and memory
        - Consider target system capabilities
        - **Example**: 1000-10000 records per batch
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔍 Error Handling
        - **Implement retry mechanisms**
        - Dead letter queues for failed records
        - Data validation before loading
        - **Example**: Exponential backoff strategy
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Monitoring & Alerting
        - **Track loading metrics**
        - Set up failure notifications
        - Monitor data freshness
        - **Example**: CloudWatch dashboards
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Advanced Data Loading")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Advanced data loading strategies using AWS services
import boto3
import pandas as pd
from datetime import datetime, timedelta
import json
import logging
from botocore.exceptions import ClientError

class AdvancedDataLoader:
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.glue = boto3.client('glue')
        self.redshift_data = boto3.client('redshift-data')
        self.dynamodb = boto3.resource('dynamodb')
        self.cloudwatch = boto3.client('cloudwatch')
        self.logger = self._setup_logging()
    
    def _setup_logging(self):
        \"\"\"Set up logging configuration\"\"\"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def full_load_to_redshift(self, source_s3_path, target_table, 
                             cluster_identifier, database, 
                             copy_options=None):
        \"\"\"
        Full load strategy - replace all data in target table
        \"\"\"
        
        self.logger.info(f"Starting full load to Redshift table: {target_table}")
        
        try:
            # Create staging table for atomic operation
            staging_table = f"{target_table}_staging_{int(datetime.now().timestamp())}"
            
            # Create staging table with same structure as target
            create_staging_sql = f\"\"\"
            CREATE TABLE {staging_table} (LIKE {target_table});
            \"\"\"
            
            response = self.redshift_data.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                Sql=create_staging_sql
            )
            
            query_id = response['Id']
            self._wait_for_query_completion(query_id)
            
            # Load data into staging table using COPY command
            copy_options = copy_options or "CSV IGNOREHEADER 1 DELIMITER ','"
            
            copy_sql = f\"\"\"
            COPY {staging_table}
            FROM '{source_s3_path}'
            IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftCopyRole'
            {copy_options};
            \"\"\"
            
            response = self.redshift_data.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                Sql=copy_sql
            )
            
            query_id = response['Id']
            self._wait_for_query_completion(query_id)
            
            # Atomic swap - rename tables
            swap_sql = f\"\"\"
            BEGIN;
            ALTER TABLE {target_table} RENAME TO {target_table}_old;
            ALTER TABLE {staging_table} RENAME TO {target_table};
            DROP TABLE {target_table}_old;
            COMMIT;
            \"\"\"
            
            response = self.redshift_data.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                Sql=swap_sql
            )
            
            query_id = response['Id']
            self._wait_for_query_completion(query_id)
            
            self.logger.info(f"Full load completed successfully for {target_table}")
            
        except Exception as e:
            self.logger.error(f"Full load failed: {str(e)}")
            # Cleanup staging table if exists
            try:
                cleanup_sql = f"DROP TABLE IF EXISTS {staging_table};"
                self.redshift_data.execute_statement(
                    ClusterIdentifier=cluster_identifier,
                    Database=database,
                    Sql=cleanup_sql
                )
            except:
                pass
            raise e
    
    def incremental_load_to_redshift(self, source_s3_path, target_table,
                                   cluster_identifier, database,
                                   merge_key, last_updated_column=None):
        \"\"\"
        Incremental load strategy - merge new/changed records
        \"\"\"
        
        self.logger.info(f"Starting incremental load to Redshift table: {target_table}")
        
        try:
            # Create temporary table for new data
            temp_table = f"{target_table}_temp_{int(datetime.now().timestamp())}"
            
            # Create temp table structure
            create_temp_sql = f\"\"\"
            CREATE TEMP TABLE {temp_table} (LIKE {target_table});
            \"\"\"
            
            response = self.redshift_data.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                Sql=create_temp_sql
            )
            
            query_id = response['Id']
            self._wait_for_query_completion(query_id)
            
            # Load new data into temp table
            copy_sql = f\"\"\"
            COPY {temp_table}
            FROM '{source_s3_path}'
            IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftCopyRole'
            CSV IGNOREHEADER 1 DELIMITER ',';
            \"\"\"
            
            response = self.redshift_data.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                Sql=copy_sql
            )
            
            query_id = response['Id']
            self._wait_for_query_completion(query_id)
            
            # Perform UPSERT operation
            upsert_sql = f\"\"\"
            BEGIN;
            
            -- Delete existing records that will be updated
            DELETE FROM {target_table}
            WHERE {merge_key} IN (SELECT {merge_key} FROM {temp_table});
            
            -- Insert all records from temp table
            INSERT INTO {target_table}
            SELECT * FROM {temp_table};
            
            COMMIT;
            \"\"\"
            
            response = self.redshift_data.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                Sql=upsert_sql
            )
            
            query_id = response['Id']
            self._wait_for_query_completion(query_id)
            
            # Get count of processed records
            count_sql = f"SELECT COUNT(*) FROM {temp_table};"
            response = self.redshift_data.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                Sql=count_sql
            )
            
            query_id = response['Id']
            result = self._get_query_result(query_id)
            record_count = result['Records'][0][0]['longValue']
            
            self.logger.info(f"Incremental load completed. Processed {record_count} records")
            
        except Exception as e:
            self.logger.error(f"Incremental load failed: {str(e)}")
            raise e
    
    def streaming_load_to_s3(self, data_stream, s3_bucket, s3_prefix,
                           partition_keys=None, file_format='parquet'):
        \"\"\"
        Streaming load strategy - continuous data ingestion
        \"\"\"
        
        self.logger.info("Starting streaming load to S3")
        
        batch_size = 1000
        batch_data = []
        
        try:
            for record in data_stream:
                batch_data.append(record)
                
                # Process batch when it reaches target size
                if len(batch_data) >= batch_size:
                    self._write_batch_to_s3(
                        batch_data, s3_bucket, s3_prefix, 
                        partition_keys, file_format
                    )
                    batch_data = []
            
            # Process remaining records
            if batch_data:
                self._write_batch_to_s3(
                    batch_data, s3_bucket, s3_prefix, 
                    partition_keys, file_format
                )
            
            self.logger.info("Streaming load completed successfully")
            
        except Exception as e:
            self.logger.error(f"Streaming load failed: {str(e)}")
            raise e
    
    def bulk_load_to_dynamodb(self, data_records, table_name, 
                             batch_size=25, max_retries=3):
        \"\"\"
        Bulk load strategy for DynamoDB with error handling
        \"\"\"
        
        self.logger.info(f"Starting bulk load to DynamoDB table: {table_name}")
        
        table = self.dynamodb.Table(table_name)
        
        try:
            total_records = len(data_records)
            processed_records = 0
            failed_records = []
            
            # Process records in batches
            for i in range(0, total_records, batch_size):
                batch = data_records[i:i + batch_size]
                
                # Prepare batch write request
                with table.batch_writer() as batch_writer:
                    for record in batch:
                        try:
                            batch_writer.put_item(Item=record)
                            processed_records += 1
                        except Exception as e:
                            self.logger.warning(f"Failed to write record: {record}. Error: {e}")
                            failed_records.append(record)
                
                # Log progress
                if i % (batch_size * 10) == 0:
                    self.logger.info(f"Processed {processed_records}/{total_records} records")
            
            # Retry failed records
            if failed_records and max_retries > 0:
                self.logger.info(f"Retrying {len(failed_records)} failed records")
                self.bulk_load_to_dynamodb(
                    failed_records, table_name, batch_size, max_retries - 1
                )
            
            self.logger.info(f"Bulk load completed. Processed {processed_records} records")
            
        except Exception as e:
            self.logger.error(f"Bulk load to DynamoDB failed: {str(e)}")
            raise e
    
    def _write_batch_to_s3(self, batch_data, bucket, prefix, 
                          partition_keys=None, file_format='parquet'):
        \"\"\"Write batch of data to S3 with partitioning\"\"\"
        
        df = pd.DataFrame(batch_data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create partition path if specified
        if partition_keys:
            partition_path = []
            for key in partition_keys:
                if key in df.columns:
                    partition_value = df[key].iloc[0]  # Use first record's value
                    partition_path.append(f"{key}={partition_value}")
            
            s3_key = f"{prefix}/{''.join(partition_path)}/data_{timestamp}.{file_format}"
        else:
            s3_key = f"{prefix}/data_{timestamp}.{file_format}"
        
        # Write data in specified format
        if file_format == 'parquet':
            import pyarrow.parquet as pq
            import pyarrow as pa
            
            table = pa.Table.from_pandas(df)
            buffer = pa.BufferOutputStream()
            pq.write_table(table, buffer)
            data = buffer.getvalue().to_pybytes()
        else:  # CSV format
            data = df.to_csv(index=False).encode('utf-8')
        
        # Upload to S3
        self.s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=data
        )
        
        self.logger.info(f"Batch written to s3://{bucket}/{s3_key}")
    
    def _wait_for_query_completion(self, query_id, max_wait_time=300):
        \"\"\"Wait for Redshift query to complete\"\"\"
        
        start_time = datetime.now()
        
        while (datetime.now() - start_time).seconds < max_wait_time:
            response = self.redshift_data.describe_statement(Id=query_id)
            status = response['Status']
            
            if status == 'FINISHED':
                return
            elif status in ['FAILED', 'ABORTED']:
                error_message = response.get('Error', 'Unknown error')
                raise Exception(f"Query failed with status {status}: {error_message}")
            
            # Wait before checking again
            import time
            time.sleep(5)
        
        raise Exception(f"Query timed out after {max_wait_time} seconds")
    
    def _get_query_result(self, query_id):
        \"\"\"Get query result from Redshift\"\"\"
        
        response = self.redshift_data.get_statement_result(Id=query_id)
        return response
    
    def monitor_loading_performance(self, job_name, metrics):
        \"\"\"Send custom metrics to CloudWatch\"\"\"
        
        try:
            self.cloudwatch.put_metric_data(
                Namespace='DataLoading',
                MetricData=[
                    {
                        'MetricName': 'RecordsProcessed',
                        'Value': metrics.get('records_processed', 0),
                        'Unit': 'Count',
                        'Dimensions': [
                            {
                                'Name': 'JobName',
                                'Value': job_name
                            }
                        ]
                    },
                    {
                        'MetricName': 'ProcessingDuration',
                        'Value': metrics.get('duration_seconds', 0),
                        'Unit': 'Seconds',
                        'Dimensions': [
                            {
                                'Name': 'JobName',
                                'Value': job_name
                            }
                        ]
                    },
                    {
                        'MetricName': 'ErrorRate',
                        'Value': metrics.get('error_rate', 0),
                        'Unit': 'Percent',
                        'Dimensions': [
                            {
                                'Name': 'JobName',
                                'Value': job_name
                            }
                        ]
                    }
                ]
            )
            
            self.logger.info(f"Metrics sent to CloudWatch for job: {job_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to send metrics to CloudWatch: {str(e)}")

# Example usage
def main():
    \"\"\"Example usage of advanced data loading strategies\"\"\"
    
    loader = AdvancedDataLoader()
    
    # Example 1: Full load to Redshift
    loader.full_load_to_redshift(
        source_s3_path='s3://my-bucket/transformed-data/',
        target_table='customer_orders',
        cluster_identifier='my-redshift-cluster',
        database='analytics_db'
    )
    
    # Example 2: Incremental load to Redshift
    loader.incremental_load_to_redshift(
        source_s3_path='s3://my-bucket/incremental-data/',
        target_table='customer_orders',
        cluster_identifier='my-redshift-cluster',
        database='analytics_db',
        merge_key='order_id'
    )
    
    # Example 3: Bulk load to DynamoDB
    sample_records = [
        {'id': '1', 'name': 'John Doe', 'email': 'john@example.com'},
        {'id': '2', 'name': 'Jane Smith', 'email': 'jane@example.com'}
    ]
    
    loader.bulk_load_to_dynamodb(
        data_records=sample_records,
        table_name='customers'
    )
    
    print("All loading operations completed!")

if __name__ == "__main__":
    main()
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def aws_glue_tab():
    """Content for AWS Glue tab"""
    st.markdown("# ⚙️ AWS Glue for ETL Processing")
    st.markdown("*Serverless data integration service for analytics*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **AWS Glue** is a fully managed extract, transform, and load (ETL) service that makes it easy to 
    prepare and load data for analytics. It's like having a smart assistant that discovers, catalogs, 
    and transforms your data automatically.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # AWS Glue Architecture
    st.markdown("## 🏗️ AWS Glue Architecture")
    common.mermaid(create_glue_architecture_mermaid(), height=400)
    
    # Interactive Glue Job Builder
    st.markdown("## 🛠️ Interactive AWS Glue Job Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Source Configuration")
        source_type = st.selectbox("Data Source Type:", [
            "🗄️ Amazon RDS (MySQL/PostgreSQL)",
            "📊 Amazon Redshift", 
            "🗂️ Amazon S3 (CSV/JSON/Parquet)",
            "🏢 Amazon DynamoDB",
            "📁 On-premises Database (via VPC)"
        ])
        
        source_format = st.selectbox("Source Format:", [
            "CSV with Headers", "JSON Lines", "Parquet", "ORC", "Avro", "XML"
        ])
        
        crawler_enabled = st.checkbox("Enable Crawler for Schema Discovery", value=True)
    
    with col2:
        st.markdown("### Job Configuration")
        job_type = st.selectbox("Job Type:", [
            "🐍 Python Shell", "🔥 Spark ETL", "📊 Streaming ETL", "🎨 Visual ETL"
        ])
        
        worker_type = st.selectbox("Worker Type:", [
            "Standard (2 vCPU, 8GB RAM)", "G.1x (1 vCPU, 4GB RAM)", "G.2x (2 vCPU, 8GB RAM)"
        ])
        
        max_capacity = st.slider("Max Workers:", 2, 100, 10)
    
    # Transformation Configuration
    st.markdown("### Transformation Configuration")
    col3, col4 = st.columns(2)
    
    with col3:
        transformations = st.multiselect("Select Transformations:", [
            "🧹 Drop Null Fields", "🔄 Rename Fields", "📊 Apply Mapping",
            "🔗 Join Data Sources", "🎭 Mask Sensitive Data", "📈 Aggregate Data",
            "🔍 Filter Records", "📋 Add Derived Columns"
        ])
    
    with col4:
        target_format = st.selectbox("Target Format:", [
            "Parquet (Optimized)", "ORC (Columnar)", "JSON", "CSV", "Delta Lake"
        ])
        
        compression = st.selectbox("Compression:", [
            "GZIP", "Snappy", "BZIP2", "LZ4", "None"
        ])
    
    if st.button("🚀 Generate Glue Job (Simulation)", use_container_width=True):
        
        # Calculate estimated metrics
        job_complexity = len(transformations) * 2 + (5 if job_type == "🔥 Spark ETL" else 3)
        estimated_time = job_complexity * 3 + max_capacity * 0.5
        estimated_cost = max_capacity * 0.44 * (estimated_time / 60)  # $0.44 per DPU-hour
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ AWS Glue Job Generated Successfully!
        
        **Job Configuration:**
        - **Name**: etl-job-{np.random.randint(1000, 9999)}
        - **Type**: {job_type}
        - **Source**: {source_type} ({source_format})
        - **Target**: {target_format} with {compression} compression
        - **Workers**: {max_capacity} x {worker_type}
        
        **Transformations**: {len(transformations)} operations
        {chr(10).join([f"  • {t}" for t in transformations]) if transformations else "  • No transformations selected"}
        
        **Performance Estimates:**
        - ⏱️ **Estimated Runtime**: ~{estimated_time:.1f} minutes
        - 💰 **Estimated Cost**: ${estimated_cost:.2f} per run
        - 📊 **DPU Hours**: {max_capacity * (estimated_time / 60):.2f}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Glue Components Deep Dive
    st.markdown("## 🧩 AWS Glue Components")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📋 Crawlers
        - **Automatic schema discovery**
        - Connect to data stores
        - Populate Data Catalog
        - Handle schema changes
        - **Example**: Crawl S3 bucket nightly
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Data Catalog
        - **Central metadata repository**
        - Table definitions and schemas
        - Partition information
        - Integration with analytics services
        - **Example**: Athena query catalog
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔧 ETL Jobs
        - **Data transformation logic**
        - Python or Scala scripts
        - Serverless Apache Spark
        - Visual ETL editor available
        - **Example**: Daily data processing
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Glue vs Traditional ETL
    st.markdown("## ⚔️ AWS Glue vs Traditional ETL")
    
    comparison_data = {
        'Feature': ['Setup Time', 'Infrastructure Management', 'Scaling', 
                   'Cost Model', 'Maintenance', 'Integration'],
        'Traditional ETL': ['Weeks', 'Manual', 'Complex', 'Fixed', 'High', 'Limited'],
        'AWS Glue': ['Minutes', 'Serverless', 'Automatic', 'Pay-per-use', 'Minimal', 'Native AWS']
    }
    
    df_comparison = pd.DataFrame(comparison_data)
    
    fig = go.Figure(data=[
        go.Bar(name='Traditional ETL', x=df_comparison['Feature'], 
               y=[3, 4, 4, 3, 4, 2], marker_color=AWS_COLORS['warning']),
        go.Bar(name='AWS Glue', x=df_comparison['Feature'], 
               y=[10, 10, 9, 8, 9, 10], marker_color=AWS_COLORS['success'])
    ])
    
    fig.update_layout(
        title='AWS Glue vs Traditional ETL (Advantage Score 1-10)',
        xaxis_title='Features',
        yaxis_title='Advantage Score',
        barmode='group'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Supported Data Sources
    st.markdown("## 🔗 Supported Data Sources")
    
    data_sources = {
        'Category': ['Databases', 'Data Warehouses', 'File Systems', 'Streaming', 'SaaS'],
        'Sources': [
            'RDS, Aurora, DynamoDB, DocumentDB',
            'Redshift, Snowflake, BigQuery',
            'S3, HDFS, Local Files',
            'Kinesis, MSK, Kafka',
            'Salesforce, ServiceNow, SAP'
        ],
        'Connection Count': [15, 8, 12, 6, 25]
    }
    
    df_sources = pd.DataFrame(data_sources)
    
    fig = px.pie(df_sources, values='Connection Count', names='Category', 
                 title='AWS Glue Data Source Distribution',
                 color_discrete_sequence=[AWS_COLORS['primary'], AWS_COLORS['light_blue'], 
                                        AWS_COLORS['success'], AWS_COLORS['warning'], AWS_COLORS['purple']])
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: AWS Glue ETL Job")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Complete AWS Glue ETL job for customer data processing
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging

# Set up logging
msg_format = '%(asctime)s %(levelname)s %(name)s: %(message)s'
datetime_format = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=msg_format, datefmt=datetime_format)
logger = logging.getLogger("GlueETLJob")
logger.setLevel(logging.INFO)

class CustomerDataProcessor:
    \"\"\"Enhanced customer data processing with AWS Glue\"\"\"
    
    def __init__(self, glue_context, spark_context, job):
        self.glue_context = glue_context
        self.spark_context = spark_context
        self.spark = glue_context.spark_session
        self.job = job
        logger.info("CustomerDataProcessor initialized")
    
    def extract_customer_data(self, database_name, table_name):
        \"\"\"Extract customer data from AWS Glue Data Catalog\"\"\"
        
        logger.info(f"Extracting data from {database_name}.{table_name}")
        
        # Create dynamic frame from Data Catalog
        customer_dynf = self.glue_context.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name,
            transformation_ctx="customer_source"
        )
        
        logger.info(f"Extracted {customer_dynf.count()} customer records")
        return customer_dynf
    
    def extract_orders_data(self, s3_path, format_type="json"):
        \"\"\"Extract orders data from S3\"\"\"
        
        logger.info(f"Extracting orders data from {s3_path}")
        
        # Create dynamic frame from S3
        orders_dynf = self.glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [s3_path],
                "recurse": True
            },
            format=format_type,
            transformation_ctx="orders_source"
        )
        
        logger.info(f"Extracted {orders_dynf.count()} order records")
        return orders_dynf
    
    def clean_customer_data(self, customer_dynf):
        \"\"\"Apply data quality transformations to customer data\"\"\"
        
        logger.info("Starting customer data cleaning")
        
        # Convert to DataFrame for complex transformations
        customer_df = customer_dynf.toDF()
        
        # Data Quality Checks and Cleaning
        customer_clean = customer_df.filter(
            # Remove records with null customer_id
            F.col("customer_id").isNotNull() &
            # Filter out test customers
            (~F.col("email").like("%test%")) &
            # Valid customer creation date
            (F.col("created_date") >= "2020-01-01")
        )
        
        # Standardize customer names
        customer_clean = customer_clean.withColumn(
            "customer_name_clean",
            F.regexp_replace(
                F.initcap(F.trim(F.col("customer_name"))),
                r"[^a-zA-Z\\s'-]", ""
            )
        )
        
        # Clean and validate email addresses
        customer_clean = customer_clean.withColumn(
            "email_clean",
            F.lower(F.trim(F.col("email")))
        ).withColumn(
            "email_domain",
            F.regexp_extract(F.col("email_clean"), r"@([\\w.-]+)", 1)
        ).withColumn(
            "is_valid_email",
            F.col("email_clean").rlike(r"^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$")
        )
        
        # Phone number standardization
        customer_clean = customer_clean.withColumn(
            "phone_digits",
            F.regexp_replace(F.col("phone"), r"[^0-9]", "")
        ).withColumn(
            "phone_formatted",
            F.when(F.length("phone_digits") == 10,
                   F.concat(F.lit("("), F.substring("phone_digits", 1, 3), F.lit(") "),
                           F.substring("phone_digits", 4, 3), F.lit("-"),
                           F.substring("phone_digits", 7, 4)))
             .otherwise(F.lit("Invalid"))
        )
        
        # Add data quality score
        customer_clean = customer_clean.withColumn(
            "data_quality_score",
            (F.col("is_valid_email").cast("int") +
             F.when(F.col("phone_formatted") != "Invalid", 1).otherwise(0) +
             F.when(F.col("customer_name_clean").isNotNull() & 
                    (F.length("customer_name_clean") > 2), 1).otherwise(0)) / 3.0
        )
        
        logger.info(f"Customer data cleaning completed. Records after cleaning: {customer_clean.count()}")
        
        # Convert back to DynamicFrame
        return DynamicFrame.fromDF(customer_clean, self.glue_context, "customer_cleaned")
    
    def enrich_orders_data(self, orders_dynf):
        \"\"\"Enrich orders data with calculated fields\"\"\"
        
        logger.info("Starting orders data enrichment")
        
        # Convert to DataFrame
        orders_df = orders_dynf.toDF()
        
        # Parse order date and add time-based features
        orders_enriched = orders_df.withColumn(
            "order_date_parsed", F.to_date("order_date", "yyyy-MM-dd")
        ).withColumn(
            "order_year", F.year("order_date_parsed")
        ).withColumn(
            "order_month", F.month("order_date_parsed")
        ).withColumn(
            "order_quarter", F.quarter("order_date_parsed")
        ).withColumn(
            "order_day_of_week", F.dayofweek("order_date_parsed")
        )
        
        # Add business day indicator
        orders_enriched = orders_enriched.withColumn(
            "is_business_day",
            F.when(F.col("order_day_of_week").between(2, 6), True).otherwise(False)
        )
        
        # Categorize order amounts
        orders_enriched = orders_enriched.withColumn(
            "order_category",
            F.when(F.col("order_amount") < 50, "Small")
             .when(F.col("order_amount") < 200, "Medium")
             .when(F.col("order_amount") < 500, "Large")
             .otherwise("Premium")
        )
        
        # Calculate discount percentage if applicable
        orders_enriched = orders_enriched.withColumn(
            "discount_percentage",
            F.when((F.col("original_amount").isNotNull()) & (F.col("original_amount") > 0),
                   ((F.col("original_amount") - F.col("order_amount")) / F.col("original_amount")) * 100)
             .otherwise(0)
        )
        
        logger.info(f"Orders data enrichment completed")
        
        return DynamicFrame.fromDF(orders_enriched, self.glue_context, "orders_enriched")
    
    def join_customer_orders(self, customer_dynf, orders_dynf):
        \"\"\"Join customer and orders data\"\"\"
        
        logger.info("Joining customer and orders data")
        
        # Perform join transformation
        joined_dynf = Join.apply(
            customer_dynf, orders_dynf,
            keys1=["customer_id"], keys2=["customer_id"],
            transformation_ctx="join_customers_orders"
        )
        
        logger.info(f"Join completed. Joined records: {joined_dynf.count()}")
        
        return joined_dynf
    
    def create_customer_aggregations(self, joined_dynf):
        \"\"\"Create customer-level aggregations\"\"\"
        
        logger.info("Creating customer aggregations")
        
        # Convert to DataFrame for aggregations
        joined_df = joined_dynf.toDF()
        
        # Customer-level aggregations
        customer_agg = joined_df.groupBy(
            "customer_id", "customer_name_clean", "email_clean", 
            "phone_formatted", "data_quality_score"
        ).agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_amount").alias("total_spent"),
            F.avg("order_amount").alias("avg_order_value"),
            F.min("order_date_parsed").alias("first_order_date"),
            F.max("order_date_parsed").alias("last_order_date"),
            F.countDistinct("order_date_parsed").alias("unique_order_days"),
            F.sum(F.when(F.col("order_category") == "Premium", 1).otherwise(0)).alias("premium_orders"),
            F.avg("discount_percentage").alias("avg_discount_received")
        )
        
        # Calculate customer lifetime value and segments
        customer_agg = customer_agg.withColumn(
            "days_as_customer",
            F.datediff("last_order_date", "first_order_date") + 1
        ).withColumn(
            "order_frequency",
            F.col("total_orders") / (F.col("days_as_customer") / 30.44)  # Orders per month
        ).withColumn(
            "customer_ltv_segment",
            F.when(F.col("total_spent") >= 1000, "High Value")
             .when(F.col("total_spent") >= 500, "Medium Value")
             .otherwise("Low Value")
        ).withColumn(
            "customer_activity_segment",
            F.when(F.col("order_frequency") >= 2, "Very Active")
             .when(F.col("order_frequency") >= 1, "Active")
             .when(F.col("order_frequency") >= 0.5, "Moderate")
             .otherwise("Inactive")
        )
        
        logger.info(f"Customer aggregations completed")
        
        return DynamicFrame.fromDF(customer_agg, self.glue_context, "customer_aggregations")
    
    def apply_data_quality_filters(self, dynf, min_quality_score=0.7):
        \"\"\"Filter records based on data quality score\"\"\"
        
        logger.info(f"Applying data quality filter (min score: {min_quality_score})")
        
        # Apply filter transformation
        filtered_dynf = Filter.apply(
            frame=dynf,
            f=lambda x: x["data_quality_score"] >= min_quality_score,
            transformation_ctx="quality_filter"
        )
        
        logger.info(f"Quality filter applied. Records after filter: {filtered_dynf.count()}")
        
        return filtered_dynf
    
    def write_to_s3_partitioned(self, dynf, s3_path, partition_keys=None, 
                               format_type="glueparquet", compression="snappy"):
        \"\"\"Write data to S3 with partitioning\"\"\"
        
        logger.info(f"Writing data to {s3_path}")
        
        # Prepare write options
        write_options = {
            "path": s3_path,
            "compression": compression
        }
        
        if partition_keys:
            write_options["partitionKeys"] = partition_keys
            logger.info(f"Partitioning by: {partition_keys}")
        
        # Write to S3
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynf,
            connection_type="s3",
            connection_options=write_options,
            format=format_type,
            transformation_ctx="s3_write"
        )
        
        logger.info("Data written to S3 successfully")
    
    def write_to_redshift(self, dynf, connection_name, table_name, 
                         redshift_tmp_dir, write_mode="overwrite"):
        \"\"\"Write data to Amazon Redshift\"\"\"
        
        logger.info(f"Writing data to Redshift table: {table_name}")
        
        # Write to Redshift
        self.glue_context.write_dynamic_frame.from_jdbc_conf(
            frame=dynf,
            catalog_connection=connection_name,
            connection_options={
                "dbtable": table_name,
                "database": "analytics_db"
            },
            redshift_tmp_dir=redshift_tmp_dir,
            transformation_ctx="redshift_write"
        )
        
        logger.info("Data written to Redshift successfully")

def main():
    \"\"\"Main ETL job execution\"\"\"
    
    # Get job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'customer_database', 'customer_table',
        'orders_s3_path', 'output_s3_path', 'redshift_connection'
    ])
    
    # Initialize Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Initialize processor
    processor = CustomerDataProcessor(glueContext, sc, job)
    
    try:
        logger.info("Starting ETL job execution")
        
        # Extract data
        customer_data = processor.extract_customer_data(
            args['customer_database'], args['customer_table']
        )
        
        orders_data = processor.extract_orders_data(args['orders_s3_path'])
        
        # Clean and enrich data
        clean_customers = processor.clean_customer_data(customer_data)
        enriched_orders = processor.enrich_orders_data(orders_data)
        
        # Join datasets
        joined_data = processor.join_customer_orders(clean_customers, enriched_orders)
        
        # Apply quality filters
        quality_filtered = processor.apply_data_quality_filters(joined_data, 0.75)
        
        # Create aggregations
        customer_summary = processor.create_customer_aggregations(quality_filtered)
        
        # Write outputs
        # Detailed data to data lake
        processor.write_to_s3_partitioned(
            quality_filtered,
            f"{args['output_s3_path']}/detailed-customer-orders/",
            partition_keys=["order_year", "order_month"]
        )
        
        # Summary data to data lake
        processor.write_to_s3_partitioned(
            customer_summary,
            f"{args['output_s3_path']}/customer-summary/",
            format_type="glueparquet"
        )
        
        # Summary data to Redshift for BI
        processor.write_to_redshift(
            customer_summary,
            args['redshift_connection'],
            "customer_summary",
            f"{args['output_s3_path']}/redshift-temp/"
        )
        
        logger.info("ETL job completed successfully")
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise e
    
    finally:
        job.commit()

if __name__ == "__main__":
    main()
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def glue_data_sources_tab():
    """Content for Glue Data Sources tab"""
    st.markdown("# 🔗 AWS Glue Data Sources & Connections")
    st.markdown("*Connecting to diverse data sources with AWS Glue*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **AWS Glue Data Sources** enable connectivity to 80+ data sources including databases, 
    data warehouses, SaaS applications, and file systems. Think of Glue as a universal translator 
    that can speak to any data system.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Pipeline Architecture
    st.markdown("## 🏗️ Complete Data Pipeline Architecture")
    common.mermaid(create_data_pipeline_mermaid(), height=350)
    
    # Interactive Connection Builder
    st.markdown("## 🛠️ Interactive Glue Connection Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Connection Details")
        connection_type = st.selectbox("Connection Type:", [
            "🗄️ RDS MySQL", "🐘 RDS PostgreSQL", "🏢 Amazon Redshift",
            "📊 Amazon DynamoDB", "🌐 JDBC Database", "📁 Amazon S3",
            "☁️ Snowflake", "📈 MongoDB", "🔄 Apache Kafka"
        ])
        
        connection_name = st.text_input("Connection Name:", f"conn-{connection_type.lower().replace(' ', '-')}")
        
        if "RDS" in connection_type or "JDBC" in connection_type or "Redshift" in connection_type:
            host = st.text_input("Database Host:", "database.cluster-xxxxx.us-east-1.rds.amazonaws.com")
            port = st.number_input("Port:", 1, 65535, 3306 if "MySQL" in connection_type else 5432)
            database_name = st.text_input("Database Name:", "production")
    
    with col2:
        st.markdown("### Security Configuration")
        vpc_id = st.selectbox("VPC:", ["Default VPC", "Custom VPC (vpc-12345678)"])
        
        security_groups = st.multiselect("Security Groups:", [
            "sg-default", "sg-database-access", "sg-glue-jobs"
        ], default=["sg-database-access"])
        
        secret_manager = st.text_input("AWS Secrets Manager ARN:", 
                                     "arn:aws:secretsmanager:us-east-1:123456789:secret:db-credentials")
        
        ssl_enabled = st.checkbox("Enable SSL/TLS", value=True)
    
    # Advanced Connection Options
    with st.expander("🔧 Advanced Connection Options", expanded=False):
        col3, col4 = st.columns(2)
        
        with col3:
            connection_properties = st.text_area("Custom Connection Properties:", 
                                               "useSSL=true\nautoreconnect=true\ncharacterEncoding=utf8")
        
        with col4:
            availability_zone = st.selectbox("Availability Zone:", [
                "us-east-1a", "us-east-1b", "us-east-1c"
            ])
            
            connection_timeout = st.number_input("Connection Timeout (seconds):", 1, 300, 30)
    
    if st.button("🔗 Create Glue Connection (Simulation)", use_container_width=True):
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ AWS Glue Connection Created Successfully!
        
        **Connection Details:**
        - **Name**: {connection_name}
        - **Type**: {connection_type}
        - **Status**: Available
        - **Connection ARN**: arn:aws:glue:us-east-1:123456789:connection/{connection_name}
        
        **Network Configuration:**
        - **VPC**: {vpc_id}
        - **Security Groups**: {', '.join(security_groups)}
        - **SSL Enabled**: {ssl_enabled}
        - **Availability Zone**: {availability_zone}
        
        **Next Steps:**
        1. Test the connection
        2. Create crawlers to discover schema
        3. Build ETL jobs using this connection
        4. Set up monitoring and alerting
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Source Capabilities Matrix
    st.markdown("## 📊 Data Source Capabilities Matrix")
    
    capabilities_data = {
        'Data Source': ['Amazon RDS', 'Amazon S3', 'Amazon Redshift', 'DynamoDB', 'Snowflake', 'MongoDB'],
        'Read': ['✅', '✅', '✅', '✅', '✅', '✅'],
        'Write': ['✅', '✅', '✅', '✅', '🔶', '🔶'],
        'Streaming': ['❌', '🔶', '❌', '✅', '❌', '🔶'],
        'Schema Evolution': ['✅', '🔶', '✅', '✅', '✅', '✅'],
        'Partitioning': ['✅', '✅', '✅', '❌', '✅', '✅'],
        'Compression': ['🔶', '✅', '✅', '❌', '✅', '✅']
    }
    
    df_capabilities = pd.DataFrame(capabilities_data)
    
    # Style the dataframe
    def highlight_capabilities(val):
        if val == '✅':
            return 'background-color: #3FB34F; color: white'
        elif val == '🔶':
            return 'background-color: #FF9900; color: white'
        elif val == '❌':
            return 'background-color: #FF6B35; color: white'
        return ''
    
    st.dataframe(df_capabilities.style.applymap(highlight_capabilities), use_container_width=True)
    
    st.markdown("""
    **Legend:** ✅ Full Support | 🔶 Partial Support | ❌ Not Supported
    """)
    
    # JDBC/ODBC Connections
    st.markdown("## 🌉 JDBC/ODBC Connections")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔗 JDBC (Java Database Connectivity)
        
        **What it is:**
        - Standard API for connecting Java applications to databases
        - Used by AWS Glue for database connections
        - Requires JDBC drivers for specific databases
        
        **Supported Databases:**
        - MySQL, PostgreSQL, Oracle, SQL Server
        - Amazon RDS, Amazon Redshift
        - Teradata, IBM Db2, SAP HANA
        
        **Connection String Example:**
        ```
        jdbc:mysql://host:3306/database?useSSL=true
        ```
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔗 ODBC (Open Database Connectivity)
        
        **What it is:**
        - Standard API for accessing database management systems
        - Language-independent interface
        - Widely supported across platforms
        
        **AWS Services using ODBC:**
        - Amazon Athena
        - Amazon Redshift
        - Amazon QuickSight
        
        **Connection String Example:**
        ```
        Driver={PostgreSQL};Server=host;Port=5432;Database=db;
        ```
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Streaming Sources
    st.markdown("## 📡 Streaming Data Sources")
    
    streaming_data = {
        'Service': ['Amazon Kinesis Data Streams', 'Amazon MSK (Kafka)', 'Amazon Kinesis Data Firehose'],
        'Real-time Processing': ['Yes', 'Yes', 'Near real-time'],
        'Batch Size': ['Configurable', 'Configurable', 'Fixed (1-128 MB)'],
        'Max Retention': ['365 days', '7 days (default)', 'N/A (delivery service)'],
        'Use Case': ['Real-time analytics', 'Event streaming', 'Data lake ingestion']
    }
    
    df_streaming = pd.DataFrame(streaming_data)
    st.dataframe(df_streaming, use_container_width=True)
    
    # Performance Optimization Tips
    st.markdown("## ⚡ Connection Performance Optimization")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚀 Network Optimization
        - **Use VPC endpoints** for S3 connections
        - **Place Glue jobs** in same AZ as data sources
        - **Configure connection pooling** for databases
        - **Enable compression** for data transfer
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗄️ Database Optimization
        - **Use read replicas** for production databases
        - **Implement connection limits** to prevent overload
        - **Create database indexes** for filtered queries
        - **Use columnar formats** where possible
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Query Optimization
        - **Use pushdown predicates** to filter at source
        - **Limit result sets** with WHERE clauses
        - **Use incremental processing** for large datasets
        - **Partition data** by common query patterns
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Glue Data Sources & Connections")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Comprehensive AWS Glue data source connections and processing
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class MultiSourceDataConnector:
    \"\"\"Handle connections to multiple data sources\"\"\"
    
    def __init__(self, glue_context):
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        
    def connect_to_rds_mysql(self, connection_name, database, table):
        \"\"\"Connect to RDS MySQL database\"\"\"
        
        logger.info(f"Connecting to MySQL: {database}.{table}")
        
        # Create dynamic frame from catalog
        mysql_dynf = self.glue_context.create_dynamic_frame.from_catalog(
            database=database,
            table_name=table,
            connection_name=connection_name,
            transformation_ctx="mysql_source"
        )
        
        # Alternative: Direct JDBC connection
        mysql_direct = self.glue_context.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options={
                "url": "jdbc:mysql://database.cluster-xxxxx.us-east-1.rds.amazonaws.com:3306/production",
                "dbtable": "customers",
                "user": "glue_user",
                "password": "secure_password",
                "customJdbcDriverS3Path": "s3://my-glue-assets/mysql-connector-java-8.0.33.jar",
                "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver"
            },
            transformation_ctx="mysql_direct"
        )
        
        logger.info(f"MySQL connection established. Record count: {mysql_dynf.count()}")
        return mysql_dynf
    
    def connect_to_redshift(self, connection_name, database, table, query=None):
        \"\"\"Connect to Amazon Redshift\"\"\"
        
        logger.info(f"Connecting to Redshift: {database}.{table}")
        
        if query:
            # Use custom query for complex data extraction
            redshift_dynf = self.glue_context.create_dynamic_frame.from_options(
                connection_type="redshift",
                connection_options={
                    "url": "jdbc:redshift://my-cluster.xxxxx.us-east-1.redshift.amazonaws.com:5439/analytics",
                    "dbtable": f"({query}) as custom_query",
                    "user": "glue_user",
                    "password": "secure_password",
                    "redshiftTmpDir": "s3://my-temp-bucket/redshift-temp/"
                },
                transformation_ctx="redshift_query"
            )
        else:
            # Standard table connection
            redshift_dynf = self.glue_context.create_dynamic_frame.from_catalog(
                database=database,
                table_name=table,
                connection_name=connection_name,
                push_down_predicate="order_date >= '2024-01-01'",  # Server-side filtering
                transformation_ctx="redshift_source"
            )
        
        logger.info(f"Redshift connection established. Record count: {redshift_dynf.count()}")
        return redshift_dynf
    
    def connect_to_s3_files(self, s3_paths, file_format="json", schema=None):
        \"\"\"Connect to files in Amazon S3\"\"\"
        
        logger.info(f"Connecting to S3 files: {s3_paths}")
        
        connection_options = {"paths": s3_paths}
        
        # Add format-specific options
        if file_format == "csv":
            connection_options.update({
                "separator": ",",
                "quoteChar": '"',
                "withHeader": True,
                "optimizePerformance": True
            })
        elif file_format == "json":
            connection_options.update({
                "jsonPath": "$[*]",  # JSONPath for nested data
                "multiline": False
            })
        elif file_format == "parquet":
            connection_options.update({
                "useS3ListImplementation": True,  # Better performance for large datasets
                "optimizePerformance": True
            })
        
        s3_dynf = self.glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options=connection_options,
            format=file_format,
            transformation_ctx="s3_source"
        )
        
        # Apply schema if provided
        if schema:
            s3_dynf = s3_dynf.apply_mapping(schema)
        
        logger.info(f"S3 connection established. Record count: {s3_dynf.count()}")
        return s3_dynf
    
    def connect_to_dynamodb(self, table_name, scan_rate=0.5):
        \"\"\"Connect to Amazon DynamoDB\"\"\"
        
        logger.info(f"Connecting to DynamoDB table: {table_name}")
        
        # Read from DynamoDB with controlled scan rate
        dynamodb_dynf = self.glue_context.create_dynamic_frame.from_options(
            connection_type="dynamodb",
            connection_options={
                "dynamodb.input.tableName": table_name,
                "dynamodb.throughput.read.percent": str(scan_rate),  # Control read capacity usage
                "dynamodb.splits": "100"  # Number of parallel scans
            },
            transformation_ctx="dynamodb_source"
        )
        
        logger.info(f"DynamoDB connection established. Record count: {dynamodb_dynf.count()}")
        return dynamodb_dynf
    
    def connect_to_kinesis_stream(self, stream_name, starting_position="LATEST"):
        \"\"\"Connect to Amazon Kinesis Data Streams\"\"\"
        
        logger.info(f"Connecting to Kinesis stream: {stream_name}")
        
        # For streaming ETL jobs
        kinesis_dynf = self.glue_context.create_dynamic_frame.from_options(
            connection_type="kinesis",
            connection_options={
                "streamName": stream_name,
                "startingPosition": starting_position,
                "inferSchema": "true",
                "classification": "json"
            },
            transformation_ctx="kinesis_source"
        )
        
        logger.info("Kinesis streaming connection established")
        return kinesis_dynf
    
    def connect_to_mongodb(self, connection_string, database, collection):
        \"\"\"Connect to MongoDB (via custom connector)\"\"\"
        
        logger.info(f"Connecting to MongoDB: {database}.{collection}")
        
        # MongoDB connection requires custom connector
        mongodb_dynf = self.glue_context.create_dynamic_frame.from_options(
            connection_type="mongodb",
            connection_options={
                "uri": connection_string,
                "database": database,
                "collection": collection,
                "batchSize": "1000",
                "partitioner": "MongoSamplePartitioner",
                "partitionerOptions.partitionSizeMB": "64"
            },
            transformation_ctx="mongodb_source"
        )
        
        logger.info(f"MongoDB connection established. Record count: {mongodb_dynf.count()}")
        return mongodb_dynf
    
    def test_connection_performance(self, dynf, sample_size=1000):
        \"\"\"Test data source connection performance\"\"\"
        
        import time
        
        logger.info("Testing connection performance...")
        
        start_time = time.time()
        
        # Test data sampling
        sample_data = dynf.toDF().sample(False, 0.1).limit(sample_size).collect()
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"Performance test completed:")
        logger.info(f"  - Sample size: {len(sample_data)} records")
        logger.info(f"  - Duration: {duration:.2f} seconds")
        logger.info(f"  - Throughput: {len(sample_data)/duration:.2f} records/second")
        
        return {
            "sample_size": len(sample_data),
            "duration_seconds": duration,
            "throughput_rps": len(sample_data)/duration
        }

class DataSourceJobProcessor:
    \"\"\"Process data from multiple sources\"\"\"
    
    def __init__(self, glue_context):
        self.glue_context = glue_context
        self.connector = MultiSourceDataConnector(glue_context)
    
    def multi_source_etl_pipeline(self):
        \"\"\"Process data from multiple sources\"\"\"
        
        logger.info("Starting multi-source ETL pipeline")
        
        # Connect to different data sources
        
        # 1. Customer data from RDS MySQL
        customers_dynf = self.connector.connect_to_rds_mysql(
            connection_name="mysql-production",
            database="customer_db",
            table="customers"
        )
        
        # 2. Order data from S3 (JSON files)
        orders_dynf = self.connector.connect_to_s3_files(
            s3_paths=["s3://my-data-bucket/orders/2024/"],
            file_format="json"
        )
        
        # 3. Product catalog from DynamoDB
        products_dynf = self.connector.connect_to_dynamodb(
            table_name="product-catalog",
            scan_rate=0.25  # Use 25% of read capacity
        )
        
        # 4. Real-time events from Kinesis (for streaming jobs)
        # events_dynf = self.connector.connect_to_kinesis_stream("user-events")
        
        # Data transformations and joins
        logger.info("Applying transformations...")
        
        # Clean customer data
        customers_clean = self.clean_customer_data(customers_dynf)
        
        # Enrich order data with product information
        orders_enriched = self.enrich_orders_with_products(orders_dynf, products_dynf)
        
        # Join customers with orders
        final_dataset = Join.apply(
            customers_clean, orders_enriched,
            keys1=["customer_id"], keys2=["customer_id"],
            transformation_ctx="customer_order_join"
        )
        
        # Write results to different targets
        logger.info("Writing results...")
        
        # Write to data lake (S3)
        self.write_to_data_lake(final_dataset)
        
        # Write to data warehouse (Redshift)
        self.write_to_data_warehouse(final_dataset)
        
        logger.info("Multi-source ETL pipeline completed successfully")
    
    def clean_customer_data(self, customers_dynf):
        \"\"\"Clean customer data\"\"\"
        
        # Apply data quality transformations
        customers_clean = Filter.apply(
            frame=customers_dynf,
            f=lambda x: x["email"] is not None and "@" in x["email"],
            transformation_ctx="filter_valid_emails"
        )
        
        # Apply field mapping
        customers_mapped = ApplyMapping.apply(
            frame=customers_clean,
            mappings=[
                ("customer_id", "long", "customer_id", "long"),
                ("first_name", "string", "first_name", "string"),
                ("last_name", "string", "last_name", "string"),
                ("email", "string", "email_address", "string"),
                ("created_date", "string", "registration_date", "timestamp")
            ],
            transformation_ctx="map_customer_fields"
        )
        
        return customers_mapped
    
    def enrich_orders_with_products(self, orders_dynf, products_dynf):
        \"\"\"Enrich order data with product information\"\"\"
        
        # Join orders with product catalog
        enriched_orders = Join.apply(
            orders_dynf, products_dynf,
            keys1=["product_id"], keys2=["product_id"],
            transformation_ctx="orders_products_join"
        )
        
        return enriched_orders
    
    def write_to_data_lake(self, dynf):
        \"\"\"Write data to S3 data lake\"\"\"
        
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynf,
            connection_type="s3",
            connection_options={
                "path": "s3://my-data-lake/processed/customer-orders/",
                "partitionKeys": ["registration_year", "registration_month"]
            },
            format="glueparquet",
            transformation_ctx="write_data_lake"
        )
    
    def write_to_data_warehouse(self, dynf):
        \"\"\"Write data to Redshift data warehouse\"\"\"
        
        self.glue_context.write_dynamic_frame.from_jdbc_conf(
            frame=dynf,
            catalog_connection="redshift-analytics",
            connection_options={
                "dbtable": "customer_order_summary",
                "database": "analytics"
            },
            redshift_tmp_dir="s3://my-temp-bucket/redshift-temp/",
            transformation_ctx="write_data_warehouse"
        )

def main():
    \"\"\"Main job execution\"\"\"
    
    # Get job arguments
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    # Initialize Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    try:
        # Initialize processor
        processor = DataSourceJobProcessor(glueContext)
        
        # Execute multi-source ETL pipeline
        processor.multi_source_etl_pipeline()
        
        logger.info("Job completed successfully")
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise e
    
    finally:
        job.commit()

if __name__ == "__main__":
    main()
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def etl_pipeline_s3_glue_tab():
    """Content for ETL Pipeline with S3 and Glue tab"""
    st.markdown("# 🏗️ Building ETL Pipelines with S3 & Glue")
    st.markdown("*Complete guide to building production-ready ETL pipelines*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **ETL Pipeline with S3 & Glue** combines Amazon S3's scalable storage with AWS Glue's 
    serverless ETL capabilities to create cost-effective, scalable data processing pipelines. 
    It's like having a smart factory that automatically processes raw materials into finished products.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Pipeline Builder
    st.markdown("## 🛠️ Interactive ETL Pipeline Builder")
    
    # Pipeline Configuration
    st.markdown("### 🔧 Pipeline Configuration")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        pipeline_name = st.text_input("Pipeline Name:", "customer-data-pipeline")
        source_bucket = st.text_input("Source S3 Bucket:", "my-raw-data-bucket")
        source_format = st.selectbox("Source Format:", ["CSV", "JSON", "Parquet", "XML", "Avro"])
    
    with col2:
        target_bucket = st.text_input("Target S3 Bucket:", "my-processed-data-bucket")
        target_format = st.selectbox("Target Format:", ["Parquet", "ORC", "JSON", "CSV"])
        compression = st.selectbox("Compression:", ["GZIP", "Snappy", "BZIP2", "None"])
    
    with col3:
        schedule = st.selectbox("Schedule:", ["Hourly", "Daily", "Weekly", "On-demand", "Event-driven"])
        glue_version = st.selectbox("Glue Version:", ["Glue 4.0", "Glue 3.0", "Glue 2.0"])
        worker_type = st.selectbox("Worker Type:", ["G.1X", "G.2X", "Standard"])
    
    # Data Processing Options
    st.markdown("### 🔄 Data Processing Options")
    col4, col5 = st.columns(2)
    
    with col4:
        data_quality_checks = st.multiselect("Data Quality Checks:", [
            "🔍 Null Value Detection", "📊 Data Type Validation", "📈 Statistical Profiling",
            "🎭 PII Detection", "🔗 Referential Integrity", "📏 Range Validation"
        ])
        
        partitioning_strategy = st.selectbox("Partitioning Strategy:", [
            "By Date (year/month/day)", "By Category", "By Region", "By Customer Segment", "No Partitioning"
        ])
    
    with col5:
        transformations = st.multiselect("Transformations:", [
            "🧹 Data Cleansing", "🔄 Format Conversion", "📊 Data Aggregation",
            "🔗 Data Enrichment", "🎭 Data Masking", "📈 Calculated Fields"
        ])
        
        error_handling = st.selectbox("Error Handling:", [
            "Stop on Error", "Skip Bad Records", "Quarantine Errors", "Log and Continue"
        ])
    
    # Advanced Configuration
    with st.expander("⚙️ Advanced Configuration", expanded=False):
        col6, col7 = st.columns(2)
        
        with col6:
            crawler_enabled = st.checkbox("Enable Automatic Crawlers", value=True)
            bookmarks_enabled = st.checkbox("Enable Job Bookmarks", value=True)
            monitoring_enabled = st.checkbox("Enable CloudWatch Monitoring", value=True)
        
        with col7:
            max_retries = st.number_input("Max Retries:", 0, 10, 3)
            timeout_minutes = st.number_input("Timeout (minutes):", 5, 480, 60)
            max_concurrent_runs = st.number_input("Max Concurrent Runs:", 1, 10, 1)
    
    if st.button("🚀 Generate ETL Pipeline (Simulation)", use_container_width=True):
        
        # Calculate pipeline metrics
        complexity_score = len(transformations) * 2 + len(data_quality_checks) * 1.5
        estimated_time = complexity_score * 5 + (20 if target_format == "Parquet" else 10)
        estimated_cost = complexity_score * 0.25 + (0.44 * estimated_time / 60)  # Glue DPU pricing
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ ETL Pipeline Generated Successfully!
        
        **Pipeline Details:**
        - **Name**: {pipeline_name}
        - **Schedule**: {schedule}
        - **Glue Version**: {glue_version}
        - **Workers**: {worker_type}
        
        **Data Flow:**
        - **Source**: s3://{source_bucket}/ ({source_format})
        - **Target**: s3://{target_bucket}/ ({target_format} with {compression})
        - **Partitioning**: {partitioning_strategy}
        
        **Processing:**
        - **Transformations**: {len(transformations)} operations
        - **Quality Checks**: {len(data_quality_checks)} validations
        - **Error Handling**: {error_handling}
        
        **Performance Estimates:**
        - ⏱️ **Runtime**: ~{estimated_time:.0f} minutes
        - 💰 **Cost**: ${estimated_cost:.2f} per run
        - 📊 **Complexity Score**: {complexity_score:.1f}/20
        
        **Components Created:**
        - AWS Glue Job definition
        - IAM roles and policies
        - CloudWatch log groups
        - Data Catalog tables
        - S3 bucket policies
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Pipeline Architecture Visualization
    st.markdown("## 🏗️ Pipeline Architecture")
    
    architecture_mermaid = """
    graph TD
        A[📁 Raw Data S3] --> B[🔍 Glue Crawler]
        B --> C[📊 Data Catalog]
        C --> D[⚙️ Glue ETL Job]
        
        D --> E[🧹 Data Cleaning]
        D --> F[🔄 Data Transformation]
        D --> G[📊 Data Validation]
        
        E --> H[📝 CloudWatch Logs]
        F --> H
        G --> H
        
        E --> I[🗂️ Processed Data S3]
        F --> I
        G --> I
        
        I --> J[🏢 Amazon Redshift]
        I --> K[📈 Amazon Athena]
        I --> L[📊 QuickSight]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style D fill:#4B9EDB,stroke:#232F3E,color:#fff
        style I fill:#3FB34F,stroke:#232F3E,color:#fff
    """
    
    common.mermaid(architecture_mermaid, height=400)
    
    # Performance Optimization Strategies
    st.markdown("## ⚡ Performance Optimization Strategies")
    
    optimization_data = {
        'Strategy': ['File Size Optimization', 'Partitioning', 'Columnar Format', 
                    'Compression', 'Predicate Pushdown', 'Parallel Processing'],
        'Performance Impact': [85, 90, 95, 70, 88, 92],
        'Implementation Effort': [30, 50, 40, 20, 60, 35],
        'Cost Reduction (%)': [25, 40, 60, 30, 45, 35]
    }
    
    df_optimization = pd.DataFrame(optimization_data)
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df_optimization['Implementation Effort'],
        y=df_optimization['Performance Impact'],
        mode='markers+text',
        text=df_optimization['Strategy'],
        textposition="top center",
        marker=dict(
            size=df_optimization['Cost Reduction (%)'],
            color=df_optimization['Performance Impact'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Performance Impact"),
            sizemode='diameter',
            sizeref=2
        ),
        name='Optimization Strategies'
    ))
    
    fig.update_layout(
        title='ETL Performance Optimization: Effort vs Impact',
        xaxis_title='Implementation Effort',
        yaxis_title='Performance Impact',
        height=500
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Cost Analysis
    st.markdown("## 💰 ETL Pipeline Cost Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Monthly Processing Volume")
        data_volume_gb = st.slider("Data Volume (GB/day):", 1, 1000, 100)
        processing_frequency = st.selectbox("Processing Frequency:", ["Hourly", "Daily", "Weekly"])
        
        # Calculate monthly metrics
        if processing_frequency == "Hourly":
            monthly_runs = 30 * 24
        elif processing_frequency == "Daily":
            monthly_runs = 30
        else:  # Weekly
            monthly_runs = 4
        
        monthly_data_gb = data_volume_gb * monthly_runs
    
    with col2:
        st.markdown("### Cost Components")
        
        # AWS Glue costs (simplified)
        glue_dpu_hours = monthly_runs * 0.5  # Assume 0.5 DPU-hour per run
        glue_cost = glue_dpu_hours * 0.44
        
        # S3 storage costs
        s3_storage_cost = monthly_data_gb * 0.023  # $0.023 per GB
        
        # Data transfer costs
        data_transfer_cost = monthly_data_gb * 0.09  # $0.09 per GB
        
        total_cost = glue_cost + s3_storage_cost + data_transfer_cost
        
        # Create cost breakdown
        cost_data = pd.DataFrame({
            'Component': ['AWS Glue Processing', 'S3 Storage', 'Data Transfer'],
            'Monthly Cost ($)': [glue_cost, s3_storage_cost, data_transfer_cost]
        })
        
        fig_cost = px.pie(cost_data, values='Monthly Cost ($)', names='Component',
                         title=f'Monthly Cost Breakdown (${total_cost:.2f})',
                         color_discrete_sequence=[AWS_COLORS['primary'], AWS_COLORS['light_blue'], AWS_COLORS['warning']])
        
        st.plotly_chart(fig_cost, use_container_width=True)
    
    # Display cost summary
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Cost Summary
    - **Monthly Data Volume**: {monthly_data_gb:,.0f} GB
    - **Processing Runs**: {monthly_runs} per month
    - **Total Monthly Cost**: ${total_cost:.2f}
    - **Cost per GB**: ${total_cost/monthly_data_gb:.4f}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Best Practices
    st.markdown("## 💡 ETL Pipeline Best Practices")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗂️ Data Organization
        - **Use consistent naming conventions**
        - **Implement logical partitioning strategy**
        - **Maintain data lineage documentation**
        - **Version control your ETL scripts**
        - **Separate raw, processed, and enriched data**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔍 Monitoring & Alerting
        - **Set up CloudWatch dashboards**
        - **Create SNS alerts for failures**
        - **Monitor data quality metrics**
        - **Track processing times and costs**
        - **Implement data freshness checks**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔒 Security & Compliance
        - **Use IAM roles with least privilege**
        - **Enable S3 bucket encryption**
        - **Implement data classification**
        - **Set up access logging**
        - **Regular security audits**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Complete ETL Pipeline")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Complete production-ready ETL pipeline with S3 and AWS Glue
import sys
import json
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
import boto3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProductionETLPipeline:
    \"\"\"Production-ready ETL pipeline for customer order processing\"\"\"
    
    def __init__(self, glue_context, job_args):
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.job_args = job_args
        self.s3_client = boto3.client('s3')
        self.cloudwatch = boto3.client('cloudwatch')
        
        # Configuration
        self.source_bucket = job_args.get('source_bucket', 'raw-data-bucket')
        self.target_bucket = job_args.get('target_bucket', 'processed-data-bucket')
        self.error_bucket = job_args.get('error_bucket', 'error-data-bucket')
        self.temp_bucket = job_args.get('temp_bucket', 'temp-processing-bucket')
        
        logger.info("ProductionETLPipeline initialized")
    
    def validate_input_data(self, dynf, table_name):
        \"\"\"Comprehensive data validation\"\"\"
        
        logger.info(f"Starting data validation for {table_name}")
        
        # Convert to DataFrame for validation
        df = dynf.toDF()
        total_records = df.count()
        
        validation_results = {
            'table_name': table_name,
            'total_records': total_records,
            'validation_timestamp': datetime.now().isoformat(),
            'checks': {}
        }
        
        # Check for null values in critical fields
        critical_fields = ['customer_id', 'order_id', 'order_date'] if 'order' in table_name.lower() else ['customer_id', 'email']
        
        for field in critical_fields:
            if field in df.columns:
                null_count = df.filter(F.col(field).isNull()).count()
                null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                
                validation_results['checks'][f'{field}_null_check'] = {
                    'null_count': null_count,
                    'null_percentage': null_percentage,
                    'passed': null_percentage < 5.0  # Allow up to 5% null values
                }
        
        # Data type validation
        for field in df.columns:
            try:
                if 'date' in field.lower():
                    # Validate date format
                    date_errors = df.filter(~F.col(field).rlike(r'\\d{4}-\\d{2}-\\d{2}')).count()
                    validation_results['checks'][f'{field}_date_format'] = {
                        'error_count': date_errors,
                        'passed': date_errors == 0
                    }
                elif 'email' in field.lower():
                    # Validate email format
                    email_errors = df.filter(~F.col(field).rlike(r'^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$')).count()
                    validation_results['checks'][f'{field}_email_format'] = {
                        'error_count': email_errors,
                        'passed': email_errors < total_records * 0.1  # Allow 10% invalid emails
                    }
            except Exception as e:
                logger.warning(f"Validation check failed for {field}: {str(e)}")
        
        # Statistical validation
        numeric_columns = [f.name for f in df.schema.fields if f.dataType in [IntegerType(), LongType(), DoubleType(), FloatType()]]
        
        for col in numeric_columns:
            try:
                stats = df.select(col).describe().collect()
                mean_val = float([row for row in stats if row[0] == 'mean'][0][1])
                std_val = float([row for row in stats if row[0] == 'stddev'][0][1])
                
                # Check for anomalies (values beyond 3 standard deviations)
                if std_val > 0:
                    anomaly_count = df.filter(
                        (F.col(col) < (mean_val - 3 * std_val)) |
                        (F.col(col) > (mean_val + 3 * std_val))
                    ).count()
                    
                    validation_results['checks'][f'{col}_anomaly_check'] = {
                        'anomaly_count': anomaly_count,
                        'anomaly_percentage': (anomaly_count / total_records) * 100,
                        'passed': anomaly_count < total_records * 0.05  # Allow 5% anomalies
                    }
            except Exception as e:
                logger.warning(f"Statistical validation failed for {col}: {str(e)}")
        
        # Overall validation status
        all_checks_passed = all(check.get('passed', False) for check in validation_results['checks'].values())
        validation_results['overall_status'] = 'PASSED' if all_checks_passed else 'FAILED'
        
        # Log validation results
        logger.info(f"Data validation completed for {table_name}")
        logger.info(f"Status: {validation_results['overall_status']}")
        logger.info(f"Total records: {total_records}")
        
        # Store validation results in S3
        self._store_validation_results(validation_results)
        
        # Send metrics to CloudWatch
        self._send_validation_metrics(validation_results)
        
        if not all_checks_passed:
            logger.warning("Data validation failures detected - check validation report")
        
        return validation_results
    
    def process_customer_data(self):
        \"\"\"Process customer data with comprehensive error handling\"\"\"
        
        logger.info("Starting customer data processing")
        
        try:
            # Read raw customer data from S3
            customer_dynf = self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    "paths": [f"s3://{self.source_bucket}/customers/"],
                    "recurse": True,
                    "groupFiles": "inPartition",
                    "groupSize": "134217728"  # 128 MB groups for optimal processing
                },
                format="json",
                transformation_ctx="customer_source"
            )
            
            # Validate input data
            validation_results = self.validate_input_data(customer_dynf, "customers")
            
            if validation_results['overall_status'] == 'FAILED':
                logger.error("Customer data validation failed - stopping processing")
                raise ValueError("Data validation failed")
            
            # Data cleaning and transformation
            customer_clean = self._clean_customer_data(customer_dynf)
            
            # Apply business rules
            customer_enhanced = self._enhance_customer_data(customer_clean)
            
            # Data quality scoring
            customer_scored = self._add_data_quality_scores(customer_enhanced)
            
            # Write processed data to S3 with partitioning
            self._write_partitioned_data(
                customer_scored,
                f"s3://{self.target_bucket}/customers/",
                partition_keys=["registration_year", "customer_segment"],
                format_type="glueparquet"
            )
            
            logger.info("Customer data processing completed successfully")
            return customer_scored
            
        except Exception as e:
            logger.error(f"Customer data processing failed: {str(e)}")
            self._handle_processing_error(e, "customer_processing")
            raise e
    
    def process_order_data(self):
        \"\"\"Process order data with incremental loading\"\"\"
        
        logger.info("Starting order data processing")
        
        try:
            # Get processing date range for incremental loading
            processing_date = self.job_args.get('processing_date', datetime.now().strftime('%Y-%m-%d'))
            
            # Read incremental order data
            order_dynf = self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    "paths": [f"s3://{self.source_bucket}/orders/year={processing_date[:4]}/month={processing_date[5:7]}/day={processing_date[8:10]}/"],
                    "recurse": True
                },
                format="json",
                transformation_ctx="order_source"
            )
            
            # Validate input data
            validation_results = self.validate_input_data(order_dynf, "orders")
            
            if validation_results['overall_status'] == 'FAILED':
                logger.warning("Order data validation failed - processing with error handling")
            
            # Data cleaning and transformation
            order_clean = self._clean_order_data(order_dynf)
            
            # Add calculated fields
            order_enhanced = self._enhance_order_data(order_clean)
            
            # Aggregate order metrics
            order_aggregated = self._aggregate_order_metrics(order_enhanced)
            
            # Write processed orders
            self._write_partitioned_data(
                order_enhanced,
                f"s3://{self.target_bucket}/orders/",
                partition_keys=["order_year", "order_month"],
                format_type="glueparquet"
            )
            
            # Write order aggregations
            self._write_partitioned_data(
                order_aggregated,
                f"s3://{self.target_bucket}/order-aggregations/",
                partition_keys=["order_date"],
                format_type="glueparquet"
            )
            
            logger.info("Order data processing completed successfully")
            return order_enhanced, order_aggregated
            
        except Exception as e:
            logger.error(f"Order data processing failed: {str(e)}")
            self._handle_processing_error(e, "order_processing")
            raise e
    
    def create_customer_360_view(self, customer_dynf, order_dynf):
        \"\"\"Create comprehensive customer 360-degree view\"\"\"
        
        logger.info("Creating customer 360-degree view")
        
        try:
            # Join customer and order data
            customer_360 = Join.apply(
                customer_dynf, order_dynf,
                keys1=["customer_id"], keys2=["customer_id"],
                transformation_ctx="customer_360_join"
            )
            
            # Convert to DataFrame for complex aggregations
            customer_360_df = customer_360.toDF()
            
            # Create comprehensive customer metrics
            customer_summary = customer_360_df.groupBy(
                "customer_id", "first_name", "last_name", "email", 
                "registration_date", "customer_segment"
            ).agg(
                # Order metrics
                F.count("order_id").alias("total_orders"),
                F.sum("order_amount").alias("total_spent"),
                F.avg("order_amount").alias("avg_order_value"),
                F.min("order_date").alias("first_order_date"),
                F.max("order_date").alias("last_order_date"),
                
                # Product preferences
                F.collect_set("product_category").alias("preferred_categories"),
                F.mode("product_category").alias("top_category"),
                
                # Behavioral metrics
                F.countDistinct("order_date").alias("unique_order_days"),
                F.avg("days_between_orders").alias("avg_days_between_orders"),
                
                # Financial metrics
                F.max("order_amount").alias("largest_order"),
                F.sum(F.when(F.col("discount_amount") > 0, 1).otherwise(0)).alias("orders_with_discount"),
                F.avg("discount_percentage").alias("avg_discount_rate")
            )
            
            # Add derived customer insights
            customer_insights = customer_summary.withColumn(
                "customer_lifetime_value",
                F.col("total_spent")
            ).withColumn(
                "customer_frequency_segment",
                F.when(F.col("total_orders") >= 20, "Very Frequent")
                 .when(F.col("total_orders") >= 10, "Frequent")
                 .when(F.col("total_orders") >= 5, "Regular")
                 .otherwise("Occasional")
            ).withColumn(
                "customer_value_segment",
                F.when(F.col("total_spent") >= 1000, "High Value")
                 .when(F.col("total_spent") >= 500, "Medium Value")
                 .otherwise("Low Value")
            ).withColumn(
                "days_since_last_order",
                F.datediff(F.current_date(), F.col("last_order_date"))
            ).withColumn(
                "customer_status",
                F.when(F.col("days_since_last_order") <= 30, "Active")
                 .when(F.col("days_since_last_order") <= 90, "Lapsing")
                 .otherwise("Inactive")
            )
            
            # Convert back to DynamicFrame
            customer_360_final = DynamicFrame.fromDF(
                customer_insights, self.glue_context, "customer_360_view"
            )
            
            # Write customer 360 view
            self._write_partitioned_data(
                customer_360_final,
                f"s3://{self.target_bucket}/customer-360/",
                partition_keys=["customer_value_segment", "customer_status"],
                format_type="glueparquet"
            )
            
            logger.info("Customer 360-degree view created successfully")
            return customer_360_final
            
        except Exception as e:
            logger.error(f"Customer 360 view creation failed: {str(e)}")
            self._handle_processing_error(e, "customer_360_creation")
            raise e
    
    def _clean_customer_data(self, dynf):
        \"\"\"Clean customer data\"\"\"
        
        # Remove duplicates
        clean_dynf = DropDuplicates.apply(dynf, transformation_ctx="remove_duplicates")
        
        # Filter out invalid records
        clean_dynf = Filter.apply(
            frame=clean_dynf,
            f=lambda x: x["customer_id"] is not None and x["email"] is not None,
            transformation_ctx="filter_invalid_customers"
        )
        
        # Apply field mapping and cleaning
        clean_dynf = ApplyMapping.apply(
            frame=clean_dynf,
            mappings=[
                ("customer_id", "long", "customer_id", "long"),
                ("first_name", "string", "first_name", "string"),
                ("last_name", "string", "last_name", "string"),
                ("email", "string", "email", "string"),
                ("phone", "string", "phone", "string"),
                ("registration_date", "string", "registration_date", "timestamp"),
                ("address", "string", "address", "string"),
                ("city", "string", "city", "string"),
                ("state", "string", "state", "string"),
                ("zip_code", "string", "zip_code", "string")
            ],
            transformation_ctx="map_customer_fields"
        )
        
        return clean_dynf
    
    def _enhance_customer_data(self, dynf):
        \"\"\"Add enhancements to customer data\"\"\"
        
        # Convert to DataFrame for complex transformations
        df = dynf.toDF()
        
        # Add derived fields
        enhanced_df = df.withColumn(
            "full_name", F.concat_ws(" ", "first_name", "last_name")
        ).withColumn(
            "email_domain", F.split("email", "@")[1]
        ).withColumn(
            "registration_year", F.year("registration_date")
        ).withColumn(
            "registration_month", F.month("registration_date")
        ).withColumn(
            "customer_age_days", F.datediff(F.current_date(), "registration_date")
        ).withColumn(
            "customer_segment",
            F.when(F.col("customer_age_days") > 365, "Established")
             .when(F.col("customer_age_days") > 90, "Growing")
             .otherwise("New")
        )
        
        return DynamicFrame.fromDF(enhanced_df, self.glue_context, "customer_enhanced")
    
    def _add_data_quality_scores(self, dynf):
        \"\"\"Add data quality scores to records\"\"\"
        
        df = dynf.toDF()
        
        # Calculate quality score based on completeness and validity
        quality_df = df.withColumn(
            "completeness_score",
            (F.when(F.col("first_name").isNotNull(), 1).otherwise(0) +
             F.when(F.col("last_name").isNotNull(), 1).otherwise(0) +
             F.when(F.col("email").isNotNull(), 1).otherwise(0) +
             F.when(F.col("phone").isNotNull(), 1).otherwise(0) +
             F.when(F.col("address").isNotNull(), 1).otherwise(0)) / 5.0
        ).withColumn(
            "validity_score",
            (F.when(F.col("email").rlike(r"^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"), 1).otherwise(0) +
             F.when(F.length("phone") >= 10, 1).otherwise(0) +
             F.when(F.col("registration_date").isNotNull(), 1).otherwise(0)) / 3.0
        ).withColumn(
            "overall_quality_score",
            (F.col("completeness_score") + F.col("validity_score")) / 2.0
        )
        
        return DynamicFrame.fromDF(quality_df, self.glue_context, "customer_quality_scored")
    
    def _write_partitioned_data(self, dynf, s3_path, partition_keys=None, format_type="glueparquet"):
        \"\"\"Write data to S3 with optional partitioning\"\"\"
        
        connection_options = {
            "path": s3_path,
            "compression": "snappy"
        }
        
        if partition_keys:
            connection_options["partitionKeys"] = partition_keys
        
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynf,
            connection_type="s3",
            connection_options=connection_options,
            format=format_type,
            transformation_ctx=f"write_{format_type}"
        )
        
        logger.info(f"Data written to {s3_path}")
    
    def _store_validation_results(self, validation_results):
        \"\"\"Store validation results in S3\"\"\"
        
        try:
            results_key = f"validation-reports/{validation_results['table_name']}/{datetime.now().strftime('%Y/%m/%d')}/validation-{int(datetime.now().timestamp())}.json"
            
            self.s3_client.put_object(
                Bucket=self.temp_bucket,
                Key=results_key,
                Body=json.dumps(validation_results, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"Validation results stored: s3://{self.temp_bucket}/{results_key}")
            
        except Exception as e:
            logger.error(f"Failed to store validation results: {str(e)}")
    
    def _send_validation_metrics(self, validation_results):
        \"\"\"Send validation metrics to CloudWatch\"\"\"
        
        try:
            metrics_data = []
            
            # Overall metrics
            metrics_data.append({
                'MetricName': 'ValidationStatus',
                'Value': 1.0 if validation_results['overall_status'] == 'PASSED' else 0.0,
                'Unit': 'None',
                'Dimensions': [
                    {'Name': 'TableName', 'Value': validation_results['table_name']},
                    {'Name': 'Pipeline', 'Value': 'customer-order-etl'}
                ]
            })
            
            metrics_data.append({
                'MetricName': 'RecordCount',
                'Value': float(validation_results['total_records']),
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'TableName', 'Value': validation_results['table_name']},
                    {'Name': 'Pipeline', 'Value': 'customer-order-etl'}
                ]
            })
            
            # Send metrics to CloudWatch
            self.cloudwatch.put_metric_data(
                Namespace='ETL/DataQuality',
                MetricData=metrics_data
            )
            
            logger.info("Validation metrics sent to CloudWatch")
            
        except Exception as e:
            logger.error(f"Failed to send validation metrics: {str(e)}")
    
    def _handle_processing_error(self, error, process_name):
        \"\"\"Handle processing errors\"\"\"
        
        logger.error(f"Processing error in {process_name}: {str(error)}")
        
        # Send error metric to CloudWatch
        try:
            self.cloudwatch.put_metric_data(
                Namespace='ETL/Errors',
                MetricData=[
                    {
                        'MetricName': 'ProcessingError',
                        'Value': 1.0,
                        'Unit': 'Count',
                        'Dimensions': [
                            {'Name': 'ProcessName', 'Value': process_name},
                            {'Name': 'Pipeline', 'Value': 'customer-order-etl'}
                        ]
                    }
                ]
            )
        except Exception as e:
            logger.error(f"Failed to send error metrics: {str(e)}")

def main():
    \"\"\"Main ETL pipeline execution\"\"\"
    
    # Get job arguments
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'source_bucket', 'target_bucket', 
        'error_bucket', 'temp_bucket', 'processing_date'
    ])
    
    # Initialize Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Initialize ETL pipeline
    pipeline = ProductionETLPipeline(glueContext, args)
    
    try:
        logger.info("Starting production ETL pipeline")
        
        # Process customer data
        customers = pipeline.process_customer_data()
        
        # Process order data
        orders, order_aggs = pipeline.process_order_data()
        
        # Create customer 360 view
        customer_360 = pipeline.create_customer_360_view(customers, orders)
        
        logger.info("Production ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise e
    
    finally:
        job.commit()

if __name__ == "__main__":
    main()
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def main():
    """Main application function"""
    # Apply styling
    apply_custom_styles()
    
    # Initialize session
    common.initialize_session_state()
    
    # Create sidebar
    create_sidebar()
    
    # Main header
    st.markdown("""
    # 🔄 AWS Data Transformation & Processing
    ### Master AWS data transformation, ETL processes, and pipeline development
    """)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "🔄 Transform & Process", 
        "📊 ETL Concepts",
        "🔍 Data Extraction", 
        "🔧 Data Transformation",
        "📤 Data Loading",
        "⚙️ AWS Glue",
        "🏗️ ETL Pipeline S3 & Glue"
    ])
    
    with tab1:
        transform_process_data_tab()
    
    with tab2:
        etl_concepts_tab()
    
    with tab3:
        data_extraction_tab()
        
    with tab4:
        data_transformation_tab()
        
    with tab5:
        data_loading_tab()
        
    with tab6:
        aws_glue_tab()
        
    with tab7:
        etl_pipeline_s3_glue_tab()
    
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
