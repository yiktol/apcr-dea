
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import utils.common as common
import utils.authenticate as authenticate
import json
import random

# Page configuration
st.set_page_config(
    page_title="AWS Data Querying & Analytics",
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
    'success': '#3FB34F',
    'danger': '#D13212'
}

common.initialize_session_state()

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
            margin-top: 3rem;
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
        
        .feature-card {{
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
            border-left: 5px solid {AWS_COLORS['light_blue']};
        }}
        
        .warning-box {{
            background-color: #FFF3CD;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 15px;
            border-left: 5px solid {AWS_COLORS['primary']};
        }}
        
        .success-box {{
            background-color: #D4EDDA;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 15px;
            border-left: 5px solid {AWS_COLORS['success']};
        }}
        
        .performance-metric {{
            background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
            padding: 15px;
            border-radius: 10px;
            color: white;
            margin: 10px 0;
            text-align: center;
        }}
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    if 'session_started' not in st.session_state:
        st.session_state.session_started = True
        st.session_state.queries_executed = 0
        st.session_state.workgroups_created = 0
        st.session_state.federated_queries = 0
        st.session_state.data_scanned_gb = 0

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🔍 Amazon Athena - Serverless query engine for S3 data
            - 🌐 Athena Federated Query - Query across multiple data sources
            - 👥 Athena Workgroups - Isolate workloads and manage costs
            
            **Learning Objectives:**
            - Master serverless querying with Athena
            - Understand federated query capabilities
            - Learn workgroup management strategies
            - Explore cost optimization techniques
            """)
        

def create_athena_architecture():
    """Create Athena architecture diagram"""
    return """
    graph TB
        subgraph "Data Sources"
            S3[(Amazon S3<br/>Data Lake)]
            GLUE[AWS Glue<br/>Data Catalog]
        end
        
        subgraph "Amazon Athena"
            ATHENA[Athena Query Engine<br/>Presto-based]
            RESULTS[Query Results<br/>S3 Storage]
        end
        
        subgraph "Supported Formats"
            CSV[CSV Files]
            JSON[JSON Files]
            ORC[ORC Files]
            PARQUET[Parquet Files]
            AVRO[Avro Files]
        end
        
        subgraph "Integration Services"
            QUICKSIGHT[Amazon QuickSight<br/>Visualization]
            JUPYTER[Jupyter Notebooks]
            BI[BI Tools<br/>JDBC/ODBC]
        end
        
        S3 --> ATHENA
        GLUE --> ATHENA
        ATHENA --> RESULTS
        
        CSV --> S3
        JSON --> S3
        ORC --> S3
        PARQUET --> S3
        AVRO --> S3
        
        ATHENA --> QUICKSIGHT
        ATHENA --> JUPYTER
        ATHENA --> BI
        
        style ATHENA fill:#FF9900,stroke:#232F3E,color:#fff
        style S3 fill:#3FB34F,stroke:#232F3E,color:#fff
        style GLUE fill:#4B9EDB,stroke:#232F3E,color:#fff
        style QUICKSIGHT fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_federated_query_architecture():
    """Create federated query architecture diagram"""
    return """
    graph TB
        subgraph "Amazon Athena"
            ATHENA[Athena Query Engine]
            FEDERATION[Federated Query<br/>Processor]
        end
        
        subgraph "Lambda Connectors"
            LAMBDA1[RDS Connector<br/>Lambda Function]
            LAMBDA2[DynamoDB Connector<br/>Lambda Function]
            LAMBDA3[ElastiCache Connector<br/>Lambda Function]
            LAMBDA4[Custom Connector<br/>Lambda Function]
        end
        
        subgraph "Data Sources"
            RDS[(Amazon RDS<br/>PostgreSQL/MySQL)]
            DYNAMO[(Amazon DynamoDB<br/>NoSQL)]
            REDIS[(Amazon ElastiCache<br/>Redis)]
            CUSTOM[(Custom Data Source<br/>API/Database)]
        end
        
        subgraph "Query Processing"
            PUSHDOWN[Predicate Pushdown<br/>Optimization]
            PROJECTION[Projection Pushdown<br/>Column Selection]
            CACHE[Result Caching<br/>Performance]
        end
        
        ATHENA --> FEDERATION
        FEDERATION --> LAMBDA1
        FEDERATION --> LAMBDA2
        FEDERATION --> LAMBDA3
        FEDERATION --> LAMBDA4
        
        LAMBDA1 --> RDS
        LAMBDA2 --> DYNAMO
        LAMBDA3 --> REDIS
        LAMBDA4 --> CUSTOM
        
        FEDERATION --> PUSHDOWN
        FEDERATION --> PROJECTION
        FEDERATION --> CACHE
        
        style ATHENA fill:#FF9900,stroke:#232F3E,color:#fff
        style FEDERATION fill:#4B9EDB,stroke:#232F3E,color:#fff
        style LAMBDA1 fill:#3FB34F,stroke:#232F3E,color:#fff
        style LAMBDA2 fill:#3FB34F,stroke:#232F3E,color:#fff
        style LAMBDA3 fill:#3FB34F,stroke:#232F3E,color:#fff
        style LAMBDA4 fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_workgroup_architecture():
    """Create workgroup architecture diagram"""
    return """
    graph TB
        subgraph "Athena Workgroups"
            WG1[Production Workgroup<br/>Cost Controls<br/>Data Limits]
            WG2[Development Workgroup<br/>Query Monitoring<br/>Result Encryption]
            WG3[Analytics Workgroup<br/>CloudWatch Metrics<br/>Custom Settings]
        end
        
        subgraph "Access Control"
            IAM[IAM Policies<br/>Resource-based]
            RBAC[Role-based Access<br/>Team Separation]
        end
        
        subgraph "Cost Management"
            LIMITS[Data Scan Limits<br/>Per Query/Workgroup]
            ALERTS[CloudWatch Alarms<br/>Cost Notifications]
            BILLING[Cost Allocation<br/>Tags & Reporting]
        end
        
        subgraph "Results & Monitoring"
            S3_RESULTS[S3 Result Buckets<br/>Per Workgroup]
            CLOUDWATCH[CloudWatch Metrics<br/>Query Performance]
            LOGS[Query History<br/>Audit Trail]
        end
        
        WG1 --> IAM
        WG2 --> IAM
        WG3 --> IAM
        
        IAM --> RBAC
        
        WG1 --> LIMITS
        WG2 --> LIMITS
        WG3 --> LIMITS
        
        LIMITS --> ALERTS
        ALERTS --> BILLING
        
        WG1 --> S3_RESULTS
        WG2 --> S3_RESULTS
        WG3 --> S3_RESULTS
        
        S3_RESULTS --> CLOUDWATCH
        CLOUDWATCH --> LOGS
        
        style WG1 fill:#FF9900,stroke:#232F3E,color:#fff
        style WG2 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style WG3 fill:#3FB34F,stroke:#232F3E,color:#fff
        style IAM fill:#232F3E,stroke:#FF9900,color:#fff
    """

def athena_tab():
    """Content for Amazon Athena tab"""
    st.markdown("## 🔍 Amazon Athena")
    st.markdown("*Serverless query engine for analyzing data in Amazon S3 using standard SQL*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Capabilities
    Amazon Athena is a serverless, interactive query service that makes it easy to analyze data in Amazon S3 using standard SQL:
    - **Serverless**: No infrastructure to manage or provision
    - **Pay-per-query**: Only pay for queries you run
    - **Standard SQL**: Uses Presto engine with ANSI SQL support
    - **Multiple formats**: CSV, JSON, ORC, Avro, Parquet support
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ Athena Architecture")
    common.mermaid(create_athena_architecture(), height=700)
    
    # Interactive query builder
    st.markdown("#### 🎮 Interactive Query Builder")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        data_source = st.selectbox("Data Source", [
            "sales_data", "customer_logs", "web_analytics", "iot_sensor_data"
        ])
        
        file_format = st.selectbox("File Format", [
            "Parquet", "CSV", "JSON", "ORC", "Avro"
        ])
    
    with col2:
        query_type = st.selectbox("Query Type", [
            "SELECT - Data Retrieval", 
            "AGGREGATE - Grouping & Metrics",
            "JOIN - Multiple Tables",
            "WINDOW - Analytics Functions"
        ])
        
        compression = st.selectbox("Compression", [
            "GZIP", "SNAPPY", "LZO", "BROTLI", "None"
        ])
    
    with col3:
        partition_projection = st.checkbox("Partition Projection", value=True)
        columnar_format = st.checkbox("Columnar Format", value=file_format in ['Parquet', 'ORC'])
    
    # Generate query example
    query_example = generate_athena_query(data_source, query_type, file_format, partition_projection)
    estimated_cost = calculate_query_cost(query_example, file_format, columnar_format)
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Query Analysis
    **Data Source**: {data_source}  
    **Format**: {file_format} with {compression} compression  
    **Query Type**: {query_type}  
    **Estimated Cost**: ${estimated_cost:.4f}  
    **Performance**: {'Optimized' if columnar_format else 'Standard'}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Show generated query
    st.markdown("#### 💻 Generated Query")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code(query_example, language='sql')
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Execute query simulation
    if st.button("🚀 Execute Query Simulation", use_container_width=True):
        simulate_query_execution(query_example, file_format)
    
    # Performance optimization tips
    st.markdown("#### ⚡ Performance Optimization Tips")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📁 Data Format Optimization
        **Best Practices:**
        - Use columnar formats (Parquet, ORC)
        - Enable compression (SNAPPY, GZIP)
        - Partition data by common filters
        - Use appropriate data types
        
        **Performance Impact:**
        - Parquet: 3-10x faster than CSV
        - Compression: 50-80% storage savings
        - Partitioning: 100x query speedup
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔍 Query Optimization
        **Techniques:**
        - Use LIMIT for exploration
        - Filter early with WHERE clauses
        - Avoid SELECT * queries
        - Use appropriate JOINs
        
        **Cost Optimization:**
        - Partition projection saves 50-90% costs
        - Columnar formats reduce scan volume
        - Compression reduces data transfer
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Athena Query Examples")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "Basic Queries", 
        "Advanced Analytics", 
        "Performance Tuning", 
        "Data Catalog Integration"
    ])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Create external table for S3 data
CREATE EXTERNAL TABLE sales_data (
    order_id string,
    customer_id string,
    product_id string,
    order_date date,
    quantity int,
    unit_price decimal(10,2),
    total_amount decimal(12,2),
    region string
)
PARTITIONED BY (
    year int,
    month int,
    day int
)
STORED AS PARQUET
LOCATION 's3://my-data-bucket/sales-data/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.year.type' = 'integer',
    'projection.year.range' = '2020,2025',
    'projection.month.type' = 'integer',
    'projection.month.range' = '1,12',
    'projection.day.type' = 'integer',
    'projection.day.range' = '1,31',
    'storage.location.template' = 's3://my-data-bucket/sales-data/year=${year}/month=${month}/day=${day}/'
);

-- Basic SELECT query
SELECT 
    order_id,
    customer_id,
    product_id,
    order_date,
    total_amount
FROM sales_data
WHERE year = 2024 
    AND month = 12
    AND total_amount > 100
ORDER BY total_amount DESC
LIMIT 100;

-- Aggregation query
SELECT 
    region,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    MIN(order_date) as first_order,
    MAX(order_date) as last_order
FROM sales_data
WHERE year = 2024
GROUP BY region
ORDER BY total_revenue DESC;

-- Date-based analysis
SELECT 
    order_date,
    COUNT(*) as daily_orders,
    SUM(total_amount) as daily_revenue,
    AVG(total_amount) as avg_daily_order
FROM sales_data
WHERE year = 2024 
    AND month = 12
GROUP BY order_date
ORDER BY order_date;

-- Top products analysis
SELECT 
    product_id,
    COUNT(*) as order_frequency,
    SUM(quantity) as total_quantity_sold,
    SUM(total_amount) as product_revenue,
    AVG(unit_price) as avg_unit_price
FROM sales_data
WHERE year = 2024
GROUP BY product_id
ORDER BY product_revenue DESC
LIMIT 50;

-- Customer segmentation
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as customer_ltv,
    AVG(total_amount) as avg_order_value,
    DATE_DIFF('day', MIN(order_date), MAX(order_date)) as customer_tenure_days,
    CASE 
        WHEN SUM(total_amount) > 10000 THEN 'Premium'
        WHEN SUM(total_amount) > 5000 THEN 'Gold'
        WHEN SUM(total_amount) > 1000 THEN 'Silver'
        ELSE 'Bronze'
    END as customer_segment
FROM sales_data
WHERE year = 2024
GROUP BY customer_id
ORDER BY customer_ltv DESC;
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Advanced analytics with window functions
SELECT 
    order_date,
    region,
    total_amount,
    -- Running total
    SUM(total_amount) OVER (
        PARTITION BY region 
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total,
    -- Moving average (7-day)
    AVG(total_amount) OVER (
        PARTITION BY region 
        ORDER BY order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7day,
    -- Rank within region
    RANK() OVER (
        PARTITION BY region 
        ORDER BY total_amount DESC
    ) as revenue_rank,
    -- Previous day comparison
    LAG(total_amount, 1) OVER (
        PARTITION BY region 
        ORDER BY order_date
    ) as prev_day_amount,
    -- Growth rate
    ROUND(
        (total_amount - LAG(total_amount, 1) OVER (
            PARTITION BY region 
            ORDER BY order_date
        )) / LAG(total_amount, 1) OVER (
            PARTITION BY region 
            ORDER BY order_date
        ) * 100, 2
    ) as growth_rate_pct
FROM (
    SELECT 
        order_date,
        region,
        SUM(total_amount) as total_amount
    FROM sales_data
    WHERE year = 2024
    GROUP BY order_date, region
) daily_sales
ORDER BY region, order_date;

-- Customer cohort analysis
WITH customer_orders AS (
    SELECT 
        customer_id,
        order_date,
        total_amount,
        MIN(order_date) OVER (PARTITION BY customer_id) as first_order_date
    FROM sales_data
    WHERE year = 2024
),
cohort_data AS (
    SELECT 
        customer_id,
        first_order_date,
        order_date,
        total_amount,
        DATE_DIFF('month', first_order_date, order_date) as period_number
    FROM customer_orders
)
SELECT 
    DATE_TRUNC('month', first_order_date) as cohort_month,
    period_number,
    COUNT(DISTINCT customer_id) as customers,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_revenue_per_customer
FROM cohort_data
GROUP BY cohort_month, period_number
ORDER BY cohort_month, period_number;

-- Time series analysis with trend detection
WITH daily_metrics AS (
    SELECT 
        order_date,
        COUNT(*) as order_count,
        SUM(total_amount) as daily_revenue,
        AVG(total_amount) as avg_order_value
    FROM sales_data
    WHERE year = 2024
    GROUP BY order_date
),
trend_analysis AS (
    SELECT 
        order_date,
        daily_revenue,
        -- 7-day moving average
        AVG(daily_revenue) OVER (
            ORDER BY order_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as ma_7day,
        -- 30-day moving average
        AVG(daily_revenue) OVER (
            ORDER BY order_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as ma_30day,
        -- Year-over-year comparison
        LAG(daily_revenue, 365) OVER (ORDER BY order_date) as yoy_revenue
    FROM daily_metrics
)
SELECT 
    order_date,
    daily_revenue,
    ma_7day,
    ma_30day,
    yoy_revenue,
    CASE 
        WHEN daily_revenue > ma_7day * 1.2 THEN 'High'
        WHEN daily_revenue < ma_7day * 0.8 THEN 'Low'
        ELSE 'Normal'
    END as revenue_signal,
    ROUND(
        (daily_revenue - yoy_revenue) / yoy_revenue * 100, 2
    ) as yoy_growth_pct
FROM trend_analysis
WHERE order_date >= DATE '2024-01-01'
ORDER BY order_date;

-- Advanced product analytics
SELECT 
    product_id,
    -- Basic metrics
    COUNT(*) as total_orders,
    SUM(quantity) as total_quantity,
    SUM(total_amount) as total_revenue,
    AVG(unit_price) as avg_unit_price,
    
    -- Advanced metrics
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT order_date) as selling_days,
    
    -- Statistical measures
    STDDEV(total_amount) as revenue_volatility,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) as median_order_value,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_amount) as p95_order_value,
    
    -- Business metrics
    SUM(total_amount) / COUNT(DISTINCT customer_id) as revenue_per_customer,
    COUNT(*) / COUNT(DISTINCT customer_id) as orders_per_customer,
    
    -- Trend indicators
    CORR(
        EXTRACT(DOY FROM order_date), 
        total_amount
    ) as seasonal_correlation
FROM sales_data
WHERE year = 2024
GROUP BY product_id
HAVING COUNT(*) >= 10  -- Filter for products with sufficient data
ORDER BY total_revenue DESC;
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Performance tuning techniques for Athena

-- 1. Partition projection for cost optimization
-- Instead of: MSCK REPAIR TABLE or ALTER TABLE ADD PARTITION
CREATE EXTERNAL TABLE optimized_sales (
    order_id string,
    customer_id string,
    total_amount decimal(12,2)
)
PARTITIONED BY (
    year int,
    month int,
    region string
)
STORED AS PARQUET
LOCATION 's3://my-bucket/optimized-sales/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.year.type' = 'integer',
    'projection.year.range' = '2020,2025',
    'projection.month.type' = 'integer', 
    'projection.month.range' = '1,12',
    'projection.region.type' = 'enum',
    'projection.region.values' = 'us-east,us-west,eu-west,ap-south',
    'storage.location.template' = 's3://my-bucket/optimized-sales/year=${year}/month=${month}/region=${region}/'
);

-- 2. Query optimization with proper WHERE clauses
-- Good: Partition filters first
SELECT customer_id, SUM(total_amount) as total_spent
FROM optimized_sales
WHERE year = 2024 
    AND month IN (10, 11, 12)  -- Partition filters
    AND region = 'us-east'     -- Partition filter
    AND total_amount > 100     -- Data filter
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 100;

-- Bad: Expensive full table scan
-- SELECT customer_id, SUM(total_amount) as total_spent
-- FROM optimized_sales
-- WHERE total_amount > 100
-- GROUP BY customer_id;

-- 3. Use UNION ALL instead of UNION (when duplicates don't matter)
SELECT 'Q1' as quarter, SUM(total_amount) as revenue
FROM optimized_sales
WHERE year = 2024 AND month IN (1,2,3)

UNION ALL

SELECT 'Q2' as quarter, SUM(total_amount) as revenue
FROM optimized_sales
WHERE year = 2024 AND month IN (4,5,6)

UNION ALL

SELECT 'Q3' as quarter, SUM(total_amount) as revenue
FROM optimized_sales
WHERE year = 2024 AND month IN (7,8,9)

UNION ALL

SELECT 'Q4' as quarter, SUM(total_amount) as revenue
FROM optimized_sales
WHERE year = 2024 AND month IN (10,11,12);

-- 4. Optimize JOIN operations
-- Use smaller table as the right side of JOIN
SELECT 
    s.customer_id,
    c.customer_name,
    s.total_amount
FROM optimized_sales s
JOIN (
    SELECT customer_id, customer_name
    FROM customer_dim
    WHERE is_active = true
) c ON s.customer_id = c.customer_id
WHERE s.year = 2024
    AND s.month = 12;

-- 5. Use approximate functions for large datasets
SELECT 
    region,
    COUNT(*) as exact_count,
    APPROX_COUNT_DISTINCT(customer_id) as approx_unique_customers,
    APPROX_PERCENTILE(total_amount, 0.5) as approx_median,
    APPROX_PERCENTILE(total_amount, 0.95) as approx_p95
FROM optimized_sales
WHERE year = 2024
GROUP BY region;

-- 6. Create materialized views for frequently accessed data
CREATE TABLE daily_sales_summary AS
SELECT 
    order_date,
    region,
    COUNT(*) as order_count,
    SUM(total_amount) as daily_revenue,
    AVG(total_amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM optimized_sales
WHERE year = 2024
GROUP BY order_date, region;

-- 7. Use CTAS (Create Table As Select) for better performance
CREATE TABLE top_customers_2024 
WITH (
    external_location = 's3://my-bucket/top-customers/',
    format = 'PARQUET',
    partitioned_by = ARRAY['region']
)
AS
SELECT 
    customer_id,
    region,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent,
    AVG(total_amount) as avg_order_value
FROM optimized_sales
WHERE year = 2024
GROUP BY customer_id, region
HAVING SUM(total_amount) > 5000
ORDER BY total_spent DESC;

-- 8. Use columnar storage benefits
-- Query only necessary columns
SELECT 
    customer_id,
    total_amount,
    order_date
FROM optimized_sales
WHERE year = 2024
    AND month = 12
    AND total_amount > 500;

-- Instead of: SELECT * FROM optimized_sales WHERE ...

-- 9. Optimize data types
-- Use appropriate sizes: INT instead of BIGINT, VARCHAR(50) instead of VARCHAR(255)
CREATE EXTERNAL TABLE type_optimized_sales (
    order_id bigint,           -- Use BIGINT only if needed
    customer_id int,           -- INT is sufficient for customer IDs
    product_id smallint,       -- SMALLINT for product catalogs < 32K
    quantity tinyint,          -- TINYINT for small quantities
    unit_price decimal(8,2),   -- Appropriate precision
    total_amount decimal(10,2), -- Appropriate precision
    order_date date,           -- DATE instead of TIMESTAMP if time not needed
    region varchar(10)         -- Fixed size for known values
)
STORED AS PARQUET
LOCATION 's3://my-bucket/type-optimized-sales/';

-- 10. Query result caching
-- Athena automatically caches query results for 24 hours
-- Identical queries will return cached results
-- To force fresh query, modify the query slightly or use different workgroup
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab4:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- AWS Glue Data Catalog integration with Athena

-- 1. Creating databases and tables via Glue Catalog
-- Note: This is typically done through AWS Glue Console or API

-- Python script to create Glue table
import boto3

glue_client = boto3.client('glue')

# Create database
glue_client.create_database(
    DatabaseInput={
        'Name': 'sales_analytics',
        'Description': 'Sales data for analytics'
    }
)

# Create table
glue_client.create_table(
    DatabaseName='sales_analytics',
    TableInput={
        'Name': 'sales_fact',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'order_id', 'Type': 'string'},
                {'Name': 'customer_id', 'Type': 'string'},
                {'Name': 'product_id', 'Type': 'string'},
                {'Name': 'order_date', 'Type': 'date'},
                {'Name': 'total_amount', 'Type': 'decimal(12,2)'},
                {'Name': 'region', 'Type': 'string'}
            ],
            'Location': 's3://my-data-bucket/sales-data/',
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                'Parameters': {
                    'field.delim': ','
                }
            }
        },
        'PartitionKeys': [
            {'Name': 'year', 'Type': 'int'},
            {'Name': 'month', 'Type': 'int'}
        ]
    }
)

-- 2. Querying Glue Catalog metadata from Athena
-- Show all databases
SHOW DATABASES;

-- Show tables in a database
SHOW TABLES IN sales_analytics;

-- Describe table structure
DESCRIBE sales_analytics.sales_fact;

-- Show table properties
SHOW TBLPROPERTIES sales_analytics.sales_fact;

-- Show partitions
SHOW PARTITIONS sales_analytics.sales_fact;

-- 3. Working with Glue Crawler-discovered tables
-- Query data discovered by Glue Crawler
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT customer_id) as unique_customers,
    MIN(order_date) as earliest_order,
    MAX(order_date) as latest_order,
    SUM(total_amount) as total_revenue
FROM sales_analytics.sales_fact
WHERE year = 2024;

-- 4. Using Glue Data Catalog for schema evolution
-- Add new column to table (via Glue API)
-- Then query with new column
SELECT 
    order_id,
    customer_id,
    total_amount,
    -- New column added via schema evolution
    COALESCE(promotion_code, 'NO_PROMOTION') as promotion_code
FROM sales_analytics.sales_fact
WHERE year = 2024 AND month = 12;

-- 5. Cross-database queries using Glue Catalog
SELECT 
    s.customer_id,
    s.total_amount,
    c.customer_name,
    c.customer_segment,
    p.product_name,
    p.category
FROM sales_analytics.sales_fact s
JOIN customer_analytics.customer_dim c
    ON s.customer_id = c.customer_id
JOIN product_catalog.product_dim p
    ON s.product_id = p.product_id
WHERE s.year = 2024
    AND s.month = 12
    AND c.is_active = true;

-- 6. Working with nested data structures
-- Query JSON data cataloged by Glue
SELECT 
    event_id,
    user_id,
    event_timestamp,
    -- Extract nested JSON fields
    event_data.action as user_action,
    event_data.page_url as page_visited,
    event_data.session_id as session_identifier,
    -- Array operations
    CARDINALITY(event_data.tags) as tag_count,
    -- Map operations
    event_data.properties['campaign'] as campaign_id
FROM web_analytics.user_events
WHERE year = 2024
    AND month = 12
    AND event_data.action = 'page_view';

-- 7. Partition management through Glue
-- Add partition (typically done via Glue API)
ALTER TABLE sales_analytics.sales_fact 
ADD PARTITION (year=2024, month=12)
LOCATION 's3://my-data-bucket/sales-data/year=2024/month=12/';

-- Drop partition
ALTER TABLE sales_analytics.sales_fact 
DROP PARTITION (year=2023, month=1);

-- 8. Table maintenance operations
-- Update table statistics
ANALYZE TABLE sales_analytics.sales_fact COMPUTE STATISTICS;

-- 9. Cross-account catalog sharing
-- Query shared catalog from another account
SELECT 
    product_id,
    product_name,
    category,
    COUNT(*) as order_count
FROM shared_catalog.product_reference
WHERE is_active = true
GROUP BY product_id, product_name, category;

-- 10. Integration with other AWS services
-- Create table that references data processed by other services
CREATE EXTERNAL TABLE kinesis_processed_data (
    event_id string,
    processed_timestamp timestamp,
    aggregated_metrics struct<
        page_views: int,
        unique_users: int,
        bounce_rate: double
    >
)
STORED AS PARQUET
LOCATION 's3://processed-data-bucket/kinesis-output/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.date.type' = 'date',
    'projection.date.range' = '2024-01-01,2025-12-31',
    'projection.date.format' = 'yyyy-MM-dd',
    'storage.location.template' = 's3://processed-data-bucket/kinesis-output/date=${date}/'
);

-- Query the processed data
SELECT 
    DATE_TRUNC('day', processed_timestamp) as processing_date,
    SUM(aggregated_metrics.page_views) as total_page_views,
    SUM(aggregated_metrics.unique_users) as total_unique_users,
    AVG(aggregated_metrics.bounce_rate) as avg_bounce_rate
FROM kinesis_processed_data
WHERE processed_timestamp >= TIMESTAMP '2024-12-01 00:00:00'
GROUP BY DATE_TRUNC('day', processed_timestamp)
ORDER BY processing_date;
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)

def federated_query_tab():
    """Content for Athena Federated Query tab"""
    st.markdown("## 🌐 Athena Federated Query")
    st.markdown("*Query any data source with unified SQL using Lambda-based connectors*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Federated Query Capabilities
    Athena Federated Query enables you to run SQL queries across multiple data sources:
    - **Unified SQL Interface**: Query different data sources with single SQL statement
    - **Lambda Connectors**: 25+ pre-built connectors for popular data sources
    - **Custom Connectors**: Build your own for proprietary data sources
    - **Query Optimization**: Predicate and projection pushdown for performance
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ Federated Query Architecture")
    common.mermaid(create_federated_query_architecture(), height=600)
    
    # Interactive connector explorer
    st.markdown("#### 🔌 Available Data Source Connectors")
    
    # Create connector data
    connectors_data = {
        'Connector': [
            'Amazon DynamoDB', 'Amazon RDS MySQL', 'Amazon RDS PostgreSQL',
            'Amazon ElastiCache Redis', 'Amazon DocumentDB', 'Amazon Timestream',
            'Apache HBase', 'Google BigQuery', 'Microsoft SQL Server',
            'Oracle Database', 'Apache Kafka', 'Amazon CloudWatch Logs'
        ],
        'Category': [
            'NoSQL', 'Relational', 'Relational', 'Cache', 'Document', 'Time Series',
            'NoSQL', 'Analytics', 'Relational', 'Relational', 'Streaming', 'Logs'
        ],
        'Use Case': [
            'Real-time lookups', 'Operational data', 'Analytical queries',
            'Session data', 'Content management', 'IoT metrics',
            'Big data storage', 'Data warehouse', 'Enterprise data',
            'Legacy systems', 'Event streams', 'Application logs'
        ],
        'Performance': [
            'High', 'Medium', 'High', 'Very High', 'Medium', 'High',
            'Medium', 'High', 'Medium', 'Medium', 'Medium', 'Low'
        ]
    }
    
    df_connectors = pd.DataFrame(connectors_data)
    
    # Filter options
    col1, col2 = st.columns(2)
    with col1:
        category_filter = st.selectbox("Filter by Category", 
                                     ['All'] + list(df_connectors['Category'].unique()))
    with col2:
        performance_filter = st.selectbox("Filter by Performance",
                                        ['All'] + list(df_connectors['Performance'].unique()))
    
    # Apply filters
    filtered_df = df_connectors.copy()
    if category_filter != 'All':
        filtered_df = filtered_df[filtered_df['Category'] == category_filter]
    if performance_filter != 'All':
        filtered_df = filtered_df[filtered_df['Performance'] == performance_filter]
    
    st.dataframe(filtered_df, use_container_width=True)
    
    # Interactive query builder
    st.markdown("#### 🎮 Federated Query Builder")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        primary_source = st.selectbox("Primary Data Source", [
            "S3 Data Lake", "Amazon RDS", "Amazon DynamoDB", "Amazon ElastiCache"
        ])
        
        join_type = st.selectbox("Join Type", [
            "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"
        ])
    
    with col2:
        secondary_source = st.selectbox("Secondary Data Source", [
            "Amazon DynamoDB", "Amazon RDS", "Amazon ElastiCache", "S3 Data Lake"
        ])
        
        optimization = st.selectbox("Query Optimization", [
            "Predicate Pushdown", "Projection Pushdown", "Both", "None"
        ])
    
    with col3:
        result_format = st.selectbox("Result Handling", [
            "Direct Return", "Cache Results", "Store in S3", "Stream Processing"
        ])
    
    # Generate federated query
    federated_query = generate_federated_query(primary_source, secondary_source, join_type, optimization)
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Federated Query Configuration
    **Primary Source**: {primary_source}  
    **Secondary Source**: {secondary_source}  
    **Join Strategy**: {join_type}  
    **Optimization**: {optimization}  
    **Result Handling**: {result_format}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Show generated query
    st.markdown("#### 💻 Generated Federated Query")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code(federated_query, language='sql')
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Execute federated query simulation
    if st.button("🚀 Execute Federated Query Simulation", use_container_width=True):
        simulate_federated_query_execution(primary_source, secondary_source)
    
    # Performance considerations
    st.markdown("#### ⚡ Performance Optimization")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔍 Query Pushdown Strategies
        **Predicate Pushdown:**
        - Filters applied at source
        - Reduces data transfer
        - Improves query performance
        
        **Projection Pushdown:**
        - Select only needed columns
        - Minimizes network I/O
        - Reduces Lambda execution time
        
        **Best Practices:**
        - Use specific WHERE clauses
        - Avoid SELECT * queries
        - Leverage source-specific optimizations
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 💰 Cost Optimization
        **Lambda Costs:**
        - Pay per connector execution
        - Optimize connector memory/timeout
        - Use result caching
        
        **Data Transfer:**
        - Minimize cross-region queries
        - Use efficient data formats
        - Implement pagination
        
        **Source Costs:**
        - Consider source database costs
        - Optimize source queries
        - Use read replicas when possible
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Federated Query Examples")
    
    tab1, tab2, tab3 = st.tabs([
        "Connector Setup", 
        "Query Examples", 
        "Custom Connectors"
    ])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Setting up Athena Federated Query Connectors

# 1. Deploy Lambda connector from AWS Serverless Application Repository
import boto3

serverless_repo = boto3.client('serverlessrepo')

# Deploy DynamoDB connector
response = serverless_repo.create_cloud_formation_template(
    ApplicationId='arn:aws:serverlessrepo:us-east-1:292517598671:applications/AthenaDynamoDBConnector',
    SemanticVersion='2022.47.1'
)

# Create CloudFormation stack
cf_client = boto3.client('cloudformation')
cf_client.create_stack(
    StackName='athena-dynamodb-connector',
    TemplateURL=response['TemplateUrl'],
    Parameters=[
        {
            'ParameterKey': 'LambdaFunctionName',
            'ParameterValue': 'athena-dynamodb-connector'
        },
        {
            'ParameterKey': 'DisableSpillEncryption',
            'ParameterValue': 'false'
        }
    ],
    Capabilities=['CAPABILITY_IAM']
)

# 2. Create data source in Athena
CREATE EXTERNAL TABLE dynamo_table
USING EXTERNAL FUNCTION lambda:athena-dynamodb-connector
LOCATION "dynamodb://my-table"
TBLPROPERTIES (
    "projection.enabled" = "true",
    "projection.partition_key.type" = "string",
    "projection.partition_key.values" = "2024-01,2024-02,2024-03"
);

# 3. IAM permissions for connector
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeTable",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:GetItem",
                "dynamodb:BatchGetItem"
            ],
            "Resource": "arn:aws:dynamodb:*:*:table/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::athena-spill-bucket/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "athena:GetQueryExecution",
                "athena:GetQueryResults"
            ],
            "Resource": "*"
        }
    ]
}

# 4. RDS connector setup
# Install RDS connector
aws serverlessrepo create-cloud-formation-template \
    --application-id arn:aws:serverlessrepo:us-east-1:292517598671:applications/AthenaJdbcConnector \
    --semantic-version 2022.47.1

# Create RDS data source
CREATE EXTERNAL TABLE rds_orders
USING EXTERNAL FUNCTION lambda:athena-jdbc-connector
LOCATION "jdbc:mysql://rds-endpoint:3306/production"
TBLPROPERTIES (
    "connection_string" = "jdbc:mysql://rds-endpoint:3306/production",
    "secret_manager_secret" = "rds-credentials",
    "table_name" = "orders"
);

# 5. ElastiCache Redis connector
# Deploy Redis connector
aws serverlessrepo create-cloud-formation-template \
    --application-id arn:aws:serverlessrepo:us-east-1:292517598671:applications/AthenaRedisConnector

# Create Redis data source
CREATE EXTERNAL TABLE redis_cache
USING EXTERNAL FUNCTION lambda:athena-redis-connector
LOCATION "redis://elasticache-endpoint:6379"
TBLPROPERTIES (
    "redis_endpoint" = "elasticache-endpoint:6379",
    "key_prefix" = "user_sessions:",
    "value_type" = "json"
);

# 6. Custom connector configuration
# Environment variables for Lambda connector
{
    "spill_bucket": "athena-federation-spill-bucket",
    "disable_spill_encryption": "false",
    "kms_key_id": "alias/athena-federation-key",
    "lambda_timeout": "900",
    "lambda_memory": "3008"
}

# 7. Monitoring and logging
# Enable CloudWatch logging for connector
LAMBDA_LOG_LEVEL = "INFO"
LAMBDA_METRICS_ENABLED = "true"

# CloudWatch metrics to monitor:
# - Lambda duration
# - Lambda errors
# - Data transfer volume
# - Query execution time

# 8. Testing connector connectivity
# Test DynamoDB connector
SELECT COUNT(*) FROM dynamo_table LIMIT 1;

# Test RDS connector
SELECT COUNT(*) FROM rds_orders LIMIT 1;

# Test Redis connector
SELECT COUNT(*) FROM redis_cache LIMIT 1;

# 9. Connector maintenance
# Update connector version
aws serverlessrepo update-application \
    --application-id arn:aws:serverlessrepo:us-east-1:292517598671:applications/AthenaDynamoDBConnector \
    --semantic-version 2023.1.1

# Monitor connector performance
aws logs filter-log-events \
    --log-group-name /aws/lambda/athena-dynamodb-connector \
    --start-time 1640995200000 \
    --filter-pattern "ERROR"

# 10. Cleanup connectors
# Delete CloudFormation stack
aws cloudformation delete-stack \
    --stack-name athena-dynamodb-connector

# Remove data source
DROP TABLE dynamo_table;
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Comprehensive Federated Query Examples

-- 1. Join S3 data with DynamoDB for real-time enrichment
SELECT 
    s3_sales.order_id,
    s3_sales.customer_id,
    s3_sales.product_id,
    s3_sales.order_date,
    s3_sales.quantity,
    s3_sales.amount,
    dynamo_customers.customer_name,
    dynamo_customers.customer_segment,
    dynamo_customers.loyalty_tier
FROM "s3_data_catalog"."sales_fact" s3_sales
JOIN "lambda:athena-dynamodb-connector"."customers" dynamo_customers
    ON s3_sales.customer_id = dynamo_customers.customer_id
WHERE s3_sales.order_date >= DATE '2024-12-01'
    AND dynamo_customers.is_active = true
ORDER BY s3_sales.amount DESC
LIMIT 1000;

-- 2. Combine RDS operational data with S3 analytics data
SELECT 
    rds_orders.order_id,
    rds_orders.status,
    rds_orders.created_at,
    s3_analytics.page_views,
    s3_analytics.session_duration,
    s3_analytics.conversion_rate
FROM "lambda:athena-jdbc-connector"."orders" rds_orders
LEFT JOIN "s3_data_catalog"."web_analytics" s3_analytics
    ON rds_orders.session_id = s3_analytics.session_id
WHERE rds_orders.created_at >= TIMESTAMP '2024-12-01 00:00:00'
    AND rds_orders.status IN ('pending', 'processing')
ORDER BY rds_orders.created_at DESC;

-- 3. Real-time cache lookup with historical data
SELECT 
    s3_events.user_id,
    s3_events.event_timestamp,
    s3_events.event_type,
    redis_sessions.session_start_time,
    redis_sessions.current_page,
    redis_sessions.cart_items,
    CASE 
        WHEN redis_sessions.user_id IS NOT NULL THEN 'Active'
        ELSE 'Inactive'
    END as session_status
FROM "s3_data_catalog"."user_events" s3_events
LEFT JOIN "lambda:athena-redis-connector"."user_sessions" redis_sessions
    ON s3_events.user_id = redis_sessions.user_id
WHERE s3_events.event_timestamp >= NOW() - INTERVAL '1' HOUR
ORDER BY s3_events.event_timestamp DESC;

-- 4. Cross-database analytics query
SELECT 
    p.product_name,
    p.category,
    COUNT(o.order_id) as order_count,
    SUM(o.amount) as total_revenue,
    AVG(o.amount) as avg_order_value,
    COUNT(DISTINCT o.customer_id) as unique_customers
FROM "lambda:athena-jdbc-connector"."products" p
JOIN "lambda:athena-jdbc-connector"."orders" o
    ON p.product_id = o.product_id
WHERE o.order_date >= DATE '2024-11-01'
GROUP BY p.product_name, p.category
HAVING COUNT(o.order_id) > 10
ORDER BY total_revenue DESC;

-- 5. Time-series data federation with Timestream
SELECT 
    ts.time,
    ts.device_id,
    ts.temperature,
    ts.humidity,
    d.device_name,
    d.location,
    d.device_type,
    CASE 
        WHEN ts.temperature > 35 THEN 'High'
        WHEN ts.temperature < 10 THEN 'Low'  
        ELSE 'Normal'
    END as temperature_status
FROM "lambda:athena-timestream-connector"."iot_readings" ts
JOIN "lambda:athena-jdbc-connector"."devices" d
    ON ts.device_id = d.device_id
WHERE ts.time >= NOW() - INTERVAL '24' HOUR
    AND d.is_active = true
ORDER BY ts.time DESC;

-- 6. Document database integration with DocumentDB
SELECT 
    doc_products.product_id,
    doc_products.product_name,
    doc_products.specifications.color,
    doc_products.specifications.size,
    doc_products.inventory_count,
    s3_sales.units_sold,
    s3_sales.revenue
FROM "lambda:athena-documentdb-connector"."products" doc_products
JOIN (
    SELECT 
        product_id,
        SUM(quantity) as units_sold,
        SUM(amount) as revenue
    FROM "s3_data_catalog"."sales_fact"
    WHERE order_date >= DATE '2024-12-01'
    GROUP BY product_id
) s3_sales ON doc_products.product_id = s3_sales.product_id
WHERE doc_products.inventory_count > 0
ORDER BY s3_sales.units_sold DESC;

-- 7. Multi-source customer analytics
WITH customer_profile AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.email,
        c.registration_date,
        c.customer_segment
    FROM "lambda:athena-jdbc-connector"."customers" c
    WHERE c.is_active = true
),
purchase_history AS (
    SELECT 
        customer_id,
        COUNT(*) as total_orders,
        SUM(amount) as total_spent,
        AVG(amount) as avg_order_value,
        MAX(order_date) as last_order_date
    FROM "s3_data_catalog"."sales_fact"
    WHERE order_date >= DATE '2024-01-01'
    GROUP BY customer_id
),
session_activity AS (
    SELECT 
        user_id as customer_id,
        COUNT(*) as session_count,
        AVG(session_duration) as avg_session_duration,
        MAX(last_activity) as last_activity_time
    FROM "lambda:athena-redis-connector"."user_sessions"
    WHERE last_activity >= NOW() - INTERVAL '30' DAY
    GROUP BY user_id
)
SELECT 
    cp.customer_id,
    cp.customer_name,
    cp.customer_segment,
    COALESCE(ph.total_orders, 0) as total_orders,
    COALESCE(ph.total_spent, 0) as total_spent,
    COALESCE(ph.avg_order_value, 0) as avg_order_value,
    COALESCE(sa.session_count, 0) as recent_sessions,
    COALESCE(sa.avg_session_duration, 0) as avg_session_duration,
    CASE 
        WHEN sa.last_activity_time >= NOW() - INTERVAL '7' DAY THEN 'Active'
        WHEN sa.last_activity_time >= NOW() - INTERVAL '30' DAY THEN 'Recently Active'
        ELSE 'Inactive'
    END as activity_status
FROM customer_profile cp
LEFT JOIN purchase_history ph ON cp.customer_id = ph.customer_id
LEFT JOIN session_activity sa ON cp.customer_id = sa.customer_id
ORDER BY ph.total_spent DESC NULLS LAST;

-- 8. Real-time inventory management
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    dynamo_inventory.current_stock,
    dynamo_inventory.reserved_stock,
    dynamo_inventory.available_stock,
    recent_sales.units_sold_24h,
    recent_sales.revenue_24h,
    CASE 
        WHEN dynamo_inventory.available_stock < recent_sales.units_sold_24h THEN 'Low Stock'
        WHEN dynamo_inventory.available_stock > recent_sales.units_sold_24h * 7 THEN 'Overstocked'
        ELSE 'Optimal'
    END as stock_status
FROM "lambda:athena-jdbc-connector"."products" p
JOIN "lambda:athena-dynamodb-connector"."inventory" dynamo_inventory
    ON p.product_id = dynamo_inventory.product_id
LEFT JOIN (
    SELECT 
        product_id,
        SUM(quantity) as units_sold_24h,
        SUM(amount) as revenue_24h
    FROM "s3_data_catalog"."sales_fact"
    WHERE order_date >= CURRENT_DATE - INTERVAL '1' DAY
    GROUP BY product_id
) recent_sales ON p.product_id = recent_sales.product_id
WHERE p.is_active = true
ORDER BY stock_status, recent_sales.units_sold_24h DESC;

-- 9. Cross-source fraud detection
SELECT 
    o.order_id,
    o.customer_id,
    o.amount,
    o.payment_method,
    o.order_timestamp,
    c.customer_name,
    c.customer_segment,
    redis_risk.risk_score,
    redis_risk.risk_factors,
    CASE 
        WHEN redis_risk.risk_score > 0.8 THEN 'High Risk'
        WHEN redis_risk.risk_score > 0.5 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_level
FROM "lambda:athena-jdbc-connector"."orders" o
JOIN "lambda:athena-jdbc-connector"."customers" c
    ON o.customer_id = c.customer_id
LEFT JOIN "lambda:athena-redis-connector"."risk_scores" redis_risk
    ON o.order_id = redis_risk.order_id
WHERE o.order_timestamp >= NOW() - INTERVAL '2' HOUR
    AND (redis_risk.risk_score > 0.5 OR redis_risk.risk_score IS NULL)
ORDER BY redis_risk.risk_score DESC NULLS LAST;

-- 10. Performance monitoring across sources
SELECT 
    'DynamoDB' as source_type,
    COUNT(*) as query_count,
    AVG(execution_time_ms) as avg_execution_time,
    MAX(execution_time_ms) as max_execution_time,
    SUM(data_scanned_bytes) as total_data_scanned
FROM "lambda:athena-dynamodb-connector"."query_metrics"
WHERE query_date >= CURRENT_DATE - INTERVAL '1' DAY

UNION ALL

SELECT 
    'RDS' as source_type,
    COUNT(*) as query_count,
    AVG(execution_time_ms) as avg_execution_time,
    MAX(execution_time_ms) as max_execution_time,
    SUM(data_scanned_bytes) as total_data_scanned
FROM "lambda:athena-jdbc-connector"."query_metrics"
WHERE query_date >= CURRENT_DATE - INTERVAL '1' DAY

UNION ALL

SELECT 
    'Redis' as source_type,
    COUNT(*) as query_count,
    AVG(execution_time_ms) as avg_execution_time,
    MAX(execution_time_ms) as max_execution_time,
    SUM(data_scanned_bytes) as total_data_scanned
FROM "lambda:athena-redis-connector"."query_metrics"
WHERE query_date >= CURRENT_DATE - INTERVAL '1' DAY

ORDER BY avg_execution_time DESC;
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Building Custom Athena Federated Query Connectors

# 1. Custom connector architecture using Query Federation SDK
import json
from pyathena.federation import (
    FederationHandler,
    MetadataHandler,
    RecordHandler,
    MetadataRequest,
    RecordRequest,
    TableName,
    Schema,
    SupportedType
)
from pyathena.federation.utils import (
    build_metadata_response,
    build_record_response
)

class CustomDataSourceConnector:
    def __init__(self):
        self.metadata_handler = CustomMetadataHandler()
        self.record_handler = CustomRecordHandler()
    
    def lambda_handler(self, event, context):
        """Main Lambda handler for custom connector"""
        request_type = event.get('requestType')
        
        if request_type == 'MetadataRequest':
            return self.metadata_handler.handle_request(event)
        elif request_type == 'RecordRequest':
            return self.record_handler.handle_request(event)
        else:
            raise ValueError(f"Unsupported request type: {request_type}")

class CustomMetadataHandler(MetadataHandler):
    """Handle metadata requests for custom data source"""
    
    def list_catalogs(self, request):
        """List available catalogs"""
        return build_metadata_response(
            request,
            catalogs=['custom_catalog']
        )
    
    def list_schemas(self, request):
        """List available schemas"""
        return build_metadata_response(
            request,
            schemas=['api_data', 'file_data', 'custom_schema']
        )
    
    def list_tables(self, request):
        """List available tables in schema"""
        schema_name = request.schema_name
        
        if schema_name == 'api_data':
            tables = ['customers', 'orders', 'products']
        elif schema_name == 'file_data':
            tables = ['log_files', 'configuration']
        else:
            tables = ['custom_table']
        
        return build_metadata_response(
            request,
            tables=tables
        )
    
    def describe_table(self, request):
        """Describe table schema"""
        table_name = request.table_name.name
        
        if table_name == 'customers':
            schema = Schema([
                ('customer_id', SupportedType.STRING),
                ('customer_name', SupportedType.STRING),
                ('email', SupportedType.STRING),
                ('registration_date', SupportedType.DATE),
                ('is_active', SupportedType.BOOLEAN)
            ])
        elif table_name == 'orders':
            schema = Schema([
                ('order_id', SupportedType.STRING),
                ('customer_id', SupportedType.STRING),
                ('order_date', SupportedType.TIMESTAMP),
                ('amount', SupportedType.DECIMAL),
                ('status', SupportedType.STRING)
            ])
        else:
            schema = Schema([
                ('id', SupportedType.STRING),
                ('data', SupportedType.STRING)
            ])
        
        return build_metadata_response(
            request,
            schema=schema
        )

class CustomRecordHandler(RecordHandler):
    """Handle record requests for custom data source"""
    
    def __init__(self):
        self.data_source = CustomDataSource()
    
    def read_records(self, request):
        """Read records from custom data source"""
        table_name = request.table_name.name
        
        # Apply predicate pushdown
        filters = self._extract_filters(request)
        
        # Apply projection pushdown
        columns = self._extract_columns(request)
        
        # Fetch data from custom source
        records = self.data_source.get_records(
            table_name=table_name,
            filters=filters,
            columns=columns
        )
        
        return build_record_response(request, records)
    
    def _extract_filters(self, request):
        """Extract WHERE clause filters for pushdown"""
        filters = {}
        
        if hasattr(request, 'constraints'):
            for constraint in request.constraints:
                column = constraint.column_name
                operator = constraint.operator
                value = constraint.value
                
                filters[column] = {
                    'operator': operator,
                    'value': value
                }
        
        return filters
    
    def _extract_columns(self, request):
        """Extract SELECT columns for projection pushdown"""
        if hasattr(request, 'columns'):
            return list(request.columns)
        return ['*']

class CustomDataSource:
    """Custom data source implementation"""
    
    def __init__(self):
        self.api_client = CustomAPIClient()
        self.file_reader = CustomFileReader()
    
    def get_records(self, table_name, filters=None, columns=None):
        """Fetch records from custom data source"""
        
        if table_name == 'customers':
            return self._get_customers(filters, columns)
        elif table_name == 'orders':
            return self._get_orders(filters, columns)
        elif table_name == 'products':
            return self._get_products(filters, columns)
        else:
            return []
    
    def _get_customers(self, filters, columns):
        """Get customer data from API"""
        
        # Build API query with filters
        api_filters = self._build_api_filters(filters)
        
        # Fetch data from API
        response = self.api_client.get_customers(
            filters=api_filters,
            fields=columns
        )
        
        # Transform to Athena format
        records = []
        for item in response.get('customers', []):
            record = {
                'customer_id': item.get('id'),
                'customer_name': item.get('name'),
                'email': item.get('email'),
                'registration_date': item.get('created_at'),
                'is_active': item.get('active', True)
            }
            records.append(record)
        
        return records
    
    def _get_orders(self, filters, columns):
        """Get order data from API"""
        
        api_filters = self._build_api_filters(filters)
        
        response = self.api_client.get_orders(
            filters=api_filters,
            fields=columns
        )
        
        records = []
        for item in response.get('orders', []):
            record = {
                'order_id': item.get('id'),
                'customer_id': item.get('customer_id'),
                'order_date': item.get('created_at'),
                'amount': float(item.get('total', 0)),
                'status': item.get('status')
            }
            records.append(record)
        
        return records
    
    def _build_api_filters(self, filters):
        """Convert Athena filters to API format"""
        api_filters = {}
        
        if not filters:
            return api_filters
        
        for column, filter_info in filters.items():
            operator = filter_info['operator']
            value = filter_info['value']
            
            if operator == 'EQUAL':
                api_filters[column] = value
            elif operator == 'GREATER_THAN':
                api_filters[f'{column}_gt'] = value
            elif operator == 'LESS_THAN':
                api_filters[f'{column}_lt'] = value
            elif operator == 'IN':
                api_filters[f'{column}_in'] = value
        
        return api_filters

class CustomAPIClient:
    """Client for custom API integration"""
    
    def __init__(self):
        self.base_url = "https://api.example.com"
        self.api_key = self._get_api_key()
    
    def _get_api_key(self):
        """Get API key from environment or Secrets Manager"""
        import os
        return os.environ.get('CUSTOM_API_KEY')
    
    def get_customers(self, filters=None, fields=None):
        """Fetch customers from API"""
        import requests
        
        url = f"{self.base_url}/customers"
        headers = {'Authorization': f'Bearer {self.api_key}'}
        params = filters or {}
        
        if fields and fields != ['*']:
            params['fields'] = ','.join(fields)
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def get_orders(self, filters=None, fields=None):
        """Fetch orders from API"""
        import requests
        
        url = f"{self.base_url}/orders"
        headers = {'Authorization': f'Bearer {self.api_key}'}
        params = filters or {}
        
        if fields and fields != ['*']:
            params['fields'] = ','.join(fields)
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        return response.json()

# 2. Lambda function deployment template
lambda_template = {
    "FunctionName": "athena-custom-connector",
    "Runtime": "python3.9",
    "Handler": "lambda_function.lambda_handler",
    "Role": "arn:aws:iam::123456789012:role/athena-connector-role",
    "Code": {
        "ZipFile": b"# Lambda function code here"
    },
    "Environment": {
        "Variables": {
            "spill_bucket": "athena-spill-bucket",
            "disable_spill_encryption": "false",
            "custom_api_endpoint": "https://api.example.com",
            "custom_api_key": "secret_key"
        }
    },
    "Timeout": 900,
    "MemorySize": 3008
}

# 3. CloudFormation template for connector deployment
cloudformation_template = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "Custom Athena Federated Query Connector",
    "Parameters": {
        "LambdaFunctionName": {
            "Type": "String",
            "Default": "athena-custom-connector"
        },
        "SpillBucket": {
            "Type": "String",
            "Description": "S3 bucket for spill data"
        }
    },
    "Resources": {
        "ConnectorFunction": {
            "Type": "AWS::Lambda::Function",
            "Properties": {
                "FunctionName": {"Ref": "LambdaFunctionName"},
                "Runtime": "python3.9",
                "Handler": "lambda_function.lambda_handler",
                "Role": {"Fn::GetAtt": ["ConnectorRole", "Arn"]},
                "Code": {
                    "ZipFile": "# Lambda function code"
                },
                "Environment": {
                    "Variables": {
                        "spill_bucket": {"Ref": "SpillBucket"}
                    }
                },
                "Timeout": 900,
                "MemorySize": 3008
            }
        },
        "ConnectorRole": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "lambda.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole"
                        }
                    ]
                },
                "ManagedPolicyArns": [
                    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                ],
                "Policies": [
                    {
                        "PolicyName": "ConnectorPolicy",
                        "PolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "s3:GetObject",
                                        "s3:PutObject",
                                        "s3:DeleteObject"
                                    ],
                                    "Resource": {
                                        "Fn::Sub": "arn:aws:s3:::${SpillBucket}/*"
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }
    }
}

# 4. Testing custom connector
def test_custom_connector():
    """Test custom connector functionality"""
    
    # Test metadata requests
    metadata_request = {
        "requestType": "MetadataRequest",
        "operation": "ListTables",
        "catalogName": "custom_catalog",
        "schemaName": "api_data"
    }
    
    # Test record requests
    record_request = {
        "requestType": "RecordRequest",
        "operation": "ReadRecords",
        "catalogName": "custom_catalog",
        "schemaName": "api_data",
        "tableName": "customers",
        "constraints": [
            {
                "columnName": "is_active",
                "operator": "EQUAL",
                "value": True
            }
        ]
    }
    
    # Execute tests
    connector = CustomDataSourceConnector()
    
    print("Testing metadata request...")
    metadata_response = connector.lambda_handler(metadata_request, {})
    print(f"Metadata response: {metadata_response}")
    
    print("Testing record request...")
    record_response = connector.lambda_handler(record_request, {})
    print(f"Record response: {record_response}")

# 5. Deployment script
def deploy_custom_connector():
    """Deploy custom connector to AWS"""
    
    import boto3
    import zipfile
    import os
    
    # Create deployment package
    with zipfile.ZipFile('connector.zip', 'w') as zip_file:
        zip_file.write('lambda_function.py')
        zip_file.write('custom_connector.py')
        # Add other required files
    
    # Deploy Lambda function
    lambda_client = boto3.client('lambda')
    
    with open('connector.zip', 'rb') as zip_file:
        lambda_client.create_function(
            FunctionName='athena-custom-connector',
            Runtime='python3.9',
            Role='arn:aws:iam::123456789012:role/athena-connector-role',
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_file.read()},
            Environment={
                'Variables': {
                    'spill_bucket': 'athena-spill-bucket',
                    'custom_api_endpoint': 'https://api.example.com'
                }
            },
            Timeout=900,
            MemorySize=3008
        )
    
    print("Custom connector deployed successfully!")

if __name__ == "__main__":
    # Test the connector
    test_custom_connector()
    
    # Deploy to AWS (uncomment to deploy)
    # deploy_custom_connector()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def workgroups_tab():
    """Content for Athena Workgroups tab"""
    st.markdown("## 👥 Athena Workgroups")
    st.markdown("*Isolate workloads, control user access, and manage query usage and costs*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Workgroup Management Capabilities
    Athena Workgroups provide comprehensive query management and cost control:
    - **Workload Isolation**: Separate queries by teams, applications, or environments
    - **Cost Controls**: Set per-query and per-workgroup data scan limits
    - **Access Management**: Control who can run queries and access results
    - **Query Monitoring**: Track metrics and performance across workgroups
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ Workgroup Architecture")
    common.mermaid(create_workgroup_architecture(), height=650)
    
    # Interactive workgroup builder
    st.markdown("#### 🎮 Workgroup Configuration Builder")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        workgroup_name = st.text_input("Workgroup Name", value="analytics-team")
        workgroup_type = st.selectbox("Workgroup Type", [
            "Production", "Development", "Testing", "Analytics", "Data Science"
        ])
        
    with col2:
        per_query_limit = st.number_input("Per-Query Data Limit (GB)", min_value=1, max_value=1000, value=100)
        workgroup_limit = st.number_input("Workgroup Data Limit (GB)", min_value=100, max_value=10000, value=1000)
        
    with col3:
        result_encryption = st.checkbox("Enable Result Encryption", value=True)
        enforce_workgroup_config = st.checkbox("Enforce Workgroup Configuration", value=True)
    
    # Additional settings
    st.markdown("#### ⚙️ Advanced Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        result_bucket = st.text_input("Results S3 Bucket", value=f"athena-results-{workgroup_name}")
        result_prefix = st.text_input("Results S3 Prefix", value=f"workgroup-{workgroup_name}/")
        
    with col2:
        publish_metrics = st.checkbox("Publish CloudWatch Metrics", value=True)
        requester_pays = st.checkbox("Requester Pays for S3", value=False)
    
    # Generate workgroup configuration
    workgroup_config = generate_workgroup_config(
        workgroup_name, workgroup_type, per_query_limit, workgroup_limit,
        result_encryption, enforce_workgroup_config, result_bucket, result_prefix
    )
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Workgroup Configuration Summary
    **Name**: {workgroup_name}  
    **Type**: {workgroup_type}  
    **Per-Query Limit**: {per_query_limit} GB  
    **Workgroup Limit**: {workgroup_limit} GB  
    **Result Location**: s3://{result_bucket}/{result_prefix}  
    **Security**: {'Encrypted' if result_encryption else 'Unencrypted'}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Create workgroup simulation
    if st.button("🚀 Create Workgroup Simulation", use_container_width=True):
        simulate_workgroup_creation(workgroup_config)
    
    # Workgroup comparison
    st.markdown("#### 📊 Workgroup Types Comparison")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏭 Production Workgroup
        **Characteristics:**
        - Strict cost controls
        - Enhanced monitoring
        - Result encryption required
        - Limited user access
        
        **Settings:**
        - Per-query limit: 500 GB
        - Workgroup limit: 10 TB
        - CloudWatch metrics: Enabled
        - Override client settings: Yes
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛠️ Development Workgroup
        **Characteristics:**
        - Moderate cost controls
        - Flexible configuration
        - Team-based access
        - Development-friendly
        
        **Settings:**
        - Per-query limit: 100 GB
        - Workgroup limit: 1 TB
        - CloudWatch metrics: Enabled
        - Override client settings: No
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📈 Analytics Workgroup
        **Characteristics:**
        - High data scan limits
        - Performance optimized
        - Analytics team access
        - Cost monitoring
        
        **Settings:**
        - Per-query limit: 1 TB
        - Workgroup limit: 50 TB
        - CloudWatch metrics: Enabled
        - Override client settings: Yes
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Cost management features
    st.markdown("#### 💰 Cost Management Features")
    
    # Create cost monitoring dashboard
    cost_data = generate_cost_data()
    
    # Cost metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="performance-metric">', unsafe_allow_html=True)
        st.markdown(f"""
        **Monthly Cost**  
        ${cost_data['monthly_cost']:.2f}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="performance-metric">', unsafe_allow_html=True)
        st.markdown(f"""
        **Data Scanned**  
        {cost_data['data_scanned_tb']:.2f} TB
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="performance-metric">', unsafe_allow_html=True)
        st.markdown(f"""
        **Queries Run**  
        {cost_data['queries_run']:,}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="performance-metric">', unsafe_allow_html=True)
        st.markdown(f"""
        **Avg Cost/Query**  
        ${cost_data['avg_cost_per_query']:.4f}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Cost trend visualization
    st.markdown("#### 📈 Cost Trend Analysis")
    
    fig = px.line(
        cost_data['daily_costs'],
        x='date',
        y='cost',
        title='Daily Query Costs by Workgroup',
        color='workgroup'
    )
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color=AWS_COLORS['secondary'])
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Code examples
    st.markdown("#### 💻 Workgroup Management Examples")
    
    tab1, tab2, tab3 = st.tabs([
        "Workgroup Creation", 
        "Cost Controls", 
        "Monitoring & Alerts"
    ])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Creating and Managing Athena Workgroups

import boto3
import json

athena_client = boto3.client('athena')

# 1. Create Production Workgroup
def create_production_workgroup():
    """Create production workgroup with strict controls"""
    
    response = athena_client.create_work_group(
        Name='production-analytics',
        Description='Production analytics workgroup with strict cost controls',
        Configuration={
            'ResultConfiguration': {
                'OutputLocation': 's3://athena-results-production/',
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            },
            'EnforceWorkGroupConfiguration': True,
            'PublishCloudWatchMetrics': True,
            'BytesScannedCutoffPerQuery': 500 * 1024 * 1024 * 1024,  # 500 GB
            'RequesterPaysEnabled': False
        },
        Tags=[
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Team', 'Value': 'Analytics'},
            {'Key': 'CostCenter', 'Value': 'DataEngineering'}
        ]
    )
    
    return response['WorkGroup']

# 2. Create Development Workgroup
def create_development_workgroup():
    """Create development workgroup with flexible settings"""
    
    response = athena_client.create_work_group(
        Name='development-team',
        Description='Development workgroup for testing and experimentation',
        Configuration={
            'ResultConfiguration': {
                'OutputLocation': 's3://athena-results-development/',
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            },
            'EnforceWorkGroupConfiguration': False,  # Allow client overrides
            'PublishCloudWatchMetrics': True,
            'BytesScannedCutoffPerQuery': 100 * 1024 * 1024 * 1024,  # 100 GB
            'RequesterPaysEnabled': False
        },
        Tags=[
            {'Key': 'Environment', 'Value': 'Development'},
            {'Key': 'Team', 'Value': 'DataScience'},
            {'Key': 'CostCenter', 'Value': 'Development'}
        ]
    )
    
    return response['WorkGroup']

# 3. Create Data Science Workgroup
def create_data_science_workgroup():
    """Create data science workgroup with high limits"""
    
    response = athena_client.create_work_group(
        Name='data-science-team',
        Description='Data science workgroup for large-scale analytics',
        Configuration={
            'ResultConfiguration': {
                'OutputLocation': 's3://athena-results-datascience/',
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_KMS',
                    'KmsKey': 'arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012'
                }
            },
            'EnforceWorkGroupConfiguration': True,
            'PublishCloudWatchMetrics': True,
            'BytesScannedCutoffPerQuery': 1024 * 1024 * 1024 * 1024,  # 1 TB
            'RequesterPaysEnabled': False
        },
        Tags=[
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Team', 'Value': 'DataScience'},
            {'Key': 'CostCenter', 'Value': 'DataScience'}
        ]
    )
    
    return response['WorkGroup']

# 4. Update Workgroup Configuration
def update_workgroup_settings(workgroup_name, new_limit_gb):
    """Update workgroup data scan limits"""
    
    response = athena_client.update_work_group(
        WorkGroup=workgroup_name,
        Description='Updated workgroup configuration',
        ConfigurationUpdates={
            'BytesScannedCutoffPerQuery': new_limit_gb * 1024 * 1024 * 1024,
            'PublishCloudWatchMetrics': True,
            'EnforceWorkGroupConfiguration': True
        }
    )
    
    return response

# 5. List and Describe Workgroups
def list_workgroups():
    """List all workgroups"""
    
    response = athena_client.list_work_groups()
    
    workgroups = []
    for wg in response['WorkGroups']:
        workgroup_detail = athena_client.get_work_group(
            WorkGroup=wg['Name']
        )
        workgroups.append(workgroup_detail['WorkGroup'])
    
    return workgroups

# 6. Get Workgroup Details
def get_workgroup_details(workgroup_name):
    """Get detailed workgroup information"""
    
    response = athena_client.get_work_group(
        WorkGroup=workgroup_name
    )
    
    workgroup = response['WorkGroup']
    
    details = {
        'name': workgroup['Name'],
        'description': workgroup.get('Description', ''),
        'state': workgroup['State'],
        'creation_time': workgroup['CreationTime'],
        'configuration': workgroup.get('Configuration', {}),
        'tags': workgroup.get('Tags', [])
    }
    
    return details

# 7. Set Workgroup as Default
def set_default_workgroup(workgroup_name):
    """Set workgroup as default for queries"""
    
    # This is typically done through IAM policies or client configuration
    # Example IAM policy to enforce workgroup usage
    
    iam_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "athena:*"
                ],
                "Resource": "*",
                "Condition": {
                    "StringEquals": {
                        "athena:WorkGroup": workgroup_name
                    }
                }
            }
        ]
    }
    
    return iam_policy

# 8. Create Workgroup with IAM Integration
def create_workgroup_with_iam(workgroup_name, team_name):
    """Create workgroup with associated IAM resources"""
    
    # Create workgroup
    workgroup = athena_client.create_work_group(
        Name=workgroup_name,
        Description=f'Workgroup for {team_name} team',
        Configuration={
            'ResultConfiguration': {
                'OutputLocation': f's3://athena-results-{workgroup_name}/',
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            },
            'EnforceWorkGroupConfiguration': True,
            'PublishCloudWatchMetrics': True,
            'BytesScannedCutoffPerQuery': 200 * 1024 * 1024 * 1024  # 200 GB
        }
    )
    
    # Create IAM role for workgroup
    iam_client = boto3.client('iam')
    
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:root"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    iam_client.create_role(
        RoleName=f'AthenaWorkgroupRole-{workgroup_name}',
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description=f'IAM role for Athena workgroup {workgroup_name}'
    )
    
    # Create IAM policy for workgroup access
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "athena:BatchGetQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:GetQueryResultsStream",
                    "athena:StartQueryExecution",
                    "athena:StopQueryExecution"
                ],
                "Resource": f"arn:aws:athena:*:*:workgroup/{workgroup_name}"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::athena-results-{workgroup_name}",
                    f"arn:aws:s3:::athena-results-{workgroup_name}/*"
                ]
            }
        ]
    }
    
    iam_client.create_policy(
        PolicyName=f'AthenaWorkgroupPolicy-{workgroup_name}',
        PolicyDocument=json.dumps(policy_document),
        Description=f'Policy for Athena workgroup {workgroup_name}'
    )
    
    return workgroup

# 9. Delete Workgroup
def delete_workgroup(workgroup_name, force=False):
    """Delete workgroup (optionally force delete)"""
    
    try:
        response = athena_client.delete_work_group(
            WorkGroup=workgroup_name,
            RecursiveDeleteOption=force
        )
        return response
    except Exception as e:
        print(f"Error deleting workgroup: {e}")
        return None

# 10. Workgroup Usage Examples
def demonstrate_workgroup_usage():
    """Demonstrate workgroup management"""
    
    print("Creating workgroups...")
    
    # Create workgroups
    prod_wg = create_production_workgroup()
    dev_wg = create_development_workgroup()
    ds_wg = create_data_science_workgroup()
    
    print(f"Created workgroups: {prod_wg['Name']}, {dev_wg['Name']}, {ds_wg['Name']}")
    
    # List workgroups
    workgroups = list_workgroups()
    print(f"Total workgroups: {len(workgroups)}")
    
    # Get workgroup details
    for wg in workgroups:
        details = get_workgroup_details(wg['Name'])
        print(f"Workgroup: {details['name']}, State: {details['state']}")
    
    # Update workgroup
    update_workgroup_settings('development-team', 150)  # 150 GB limit
    print("Updated development workgroup limits")

# Execute workgroup management
if __name__ == "__main__":
    demonstrate_workgroup_usage()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Athena Workgroup Cost Controls and Monitoring

import boto3
import json
from datetime import datetime, timedelta

athena_client = boto3.client('athena')
cloudwatch_client = boto3.client('cloudwatch')

# 1. Set up comprehensive cost controls
def configure_cost_controls(workgroup_name, daily_limit_gb, monthly_limit_gb):
    """Configure comprehensive cost controls for workgroup"""
    
    # Update workgroup with cost limits
    response = athena_client.update_work_group(
        WorkGroup=workgroup_name,
        ConfigurationUpdates={
            'BytesScannedCutoffPerQuery': daily_limit_gb * 1024 * 1024 * 1024,
            'PublishCloudWatchMetrics': True,
            'EnforceWorkGroupConfiguration': True,
            'ResultConfiguration': {
                'OutputLocation': f's3://athena-results-{workgroup_name}/',
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            }
        }
    )
    
    # Create CloudWatch alarm for daily costs
    cloudwatch_client.put_metric_alarm(
        AlarmName=f'athena-{workgroup_name}-daily-cost-alarm',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=1,
        MetricName='DataScannedInBytes',
        Namespace='AWS/Athena',
        Period=86400,  # 24 hours
        Statistic='Sum',
        Threshold=daily_limit_gb * 1024 * 1024 * 1024,  # Convert to bytes
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-west-2:123456789012:athena-cost-alerts'
        ],
        AlarmDescription=f'Daily data scan limit exceeded for {workgroup_name}',
        Dimensions=[
            {
                'Name': 'WorkGroup',
                'Value': workgroup_name
            }
        ]
    )
    
    # Create CloudWatch alarm for monthly costs
    cloudwatch_client.put_metric_alarm(
        AlarmName=f'athena-{workgroup_name}-monthly-cost-alarm',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=1,
        MetricName='DataScannedInBytes',
        Namespace='AWS/Athena',
        Period=2592000,  # 30 days
        Statistic='Sum',
        Threshold=monthly_limit_gb * 1024 * 1024 * 1024,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-west-2:123456789012:athena-cost-alerts'
        ],
        AlarmDescription=f'Monthly data scan limit exceeded for {workgroup_name}',
        Dimensions=[
            {
                'Name': 'WorkGroup',
                'Value': workgroup_name
            }
        ]
    )
    
    return response

# 2. Monitor workgroup costs and usage
def monitor_workgroup_costs(workgroup_name, days=30):
    """Monitor workgroup costs and usage patterns"""
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)
    
    # Get data scanned metrics
    data_scanned = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/Athena',
        MetricName='DataScannedInBytes',
        Dimensions=[
            {
                'Name': 'WorkGroup',
                'Value': workgroup_name
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=86400,  # Daily
        Statistics=['Sum']
    )
    
    # Get query execution metrics
    query_count = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/Athena',
        MetricName='QueryExecutionTime',
        Dimensions=[
            {
                'Name': 'WorkGroup',
                'Value': workgroup_name
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=86400,
        Statistics=['SampleCount']
    )
    
    # Calculate costs (Athena pricing: $5 per TB scanned)
    total_bytes_scanned = sum(point['Sum'] for point in data_scanned['Datapoints'])
    total_tb_scanned = total_bytes_scanned / (1024 ** 4)  # Convert to TB
    estimated_cost = total_tb_scanned * 5  # $5 per TB
    
    total_queries = sum(point['SampleCount'] for point in query_count['Datapoints'])
    
    report = {
        'workgroup_name': workgroup_name,
        'period_days': days,
        'total_queries': int(total_queries),
        'total_tb_scanned': round(total_tb_scanned, 3),
        'estimated_cost_usd': round(estimated_cost, 2),
        'avg_cost_per_query': round(estimated_cost / max(total_queries, 1), 4),
        'daily_breakdown': []
    }
    
    # Add daily breakdown
    for point in data_scanned['Datapoints']:
        daily_tb = point['Sum'] / (1024 ** 4)
        daily_cost = daily_tb * 5
        report['daily_breakdown'].append({
            'date': point['Timestamp'].strftime('%Y-%m-%d'),
            'tb_scanned': round(daily_tb, 3),
            'cost_usd': round(daily_cost, 2)
        })
    
    return report

# 3. Create cost budgets and alerts
def create_cost_budget(workgroup_name, monthly_budget_usd):
    """Create AWS Budget for workgroup costs"""
    
    budgets_client = boto3.client('budgets')
    
    # Create budget
    budget_name = f'athena-{workgroup_name}-budget'
    
    budget = {
        'BudgetName': budget_name,
        'BudgetLimit': {
            'Amount': str(monthly_budget_usd),
            'Unit': 'USD'
        },
        'TimeUnit': 'MONTHLY',
        'TimeWindow': {
            'Start': datetime.utcnow().replace(day=1),
            'End': datetime.utcnow().replace(day=28) + timedelta(days=4)
        },
        'BudgetType': 'COST',
        'CostFilters': {
            'Service': ['Amazon Athena']
        }
    }
    
    subscribers = [
        {
            'SubscriptionType': 'EMAIL',
            'Address': 'data-team@company.com'
        }
    ]
    
    notifications = [
        {
            'NotificationType': 'ACTUAL',
            'ComparisonOperator': 'GREATER_THAN',
            'Threshold': 80,  # 80% of budget
            'ThresholdType': 'PERCENTAGE',
            'NotificationState': 'ALARM'
        },
        {
            'NotificationType': 'FORECASTED',
            'ComparisonOperator': 'GREATER_THAN',
            'Threshold': 100,  # 100% of budget
            'ThresholdType': 'PERCENTAGE',
            'NotificationState': 'ALARM'
        }
    ]
    
    try:
        response = budgets_client.create_budget(
            AccountId=boto3.client('sts').get_caller_identity()['Account'],
            Budget=budget,
            NotificationsWithSubscribers=[
                {
                    'Notification': notification,
                    'Subscribers': subscribers
                }
                for notification in notifications
            ]
        )
        return response
    except Exception as e:
        print(f"Error creating budget: {e}")
        return None

# 4. Implement query cost tracking
def track_query_costs(workgroup_name, query_execution_id):
    """Track individual query costs"""
    
    # Get query execution details
    response = athena_client.get_query_execution(
        QueryExecutionId=query_execution_id
    )
    
    query_execution = response['QueryExecution']
    
    # Extract cost-related information
    data_scanned_bytes = query_execution['Statistics'].get('DataScannedInBytes', 0)
    execution_time_ms = query_execution['Statistics'].get('EngineExecutionTimeInMillis', 0)
    
    # Calculate costs
    data_scanned_gb = data_scanned_bytes / (1024 ** 3)
    data_scanned_tb = data_scanned_bytes / (1024 ** 4)
    query_cost = data_scanned_tb * 5  # $5 per TB
    
    query_info = {
        'query_execution_id': query_execution_id,
        'workgroup': workgroup_name,
        'query_string': query_execution['Query'][:100] + '...' if len(query_execution['Query']) > 100 else query_execution['Query'],
        'status': query_execution['Status']['State'],
        'submission_time': query_execution['Status']['SubmissionDateTime'],
        'completion_time': query_execution['Status'].get('CompletionDateTime'),
        'execution_time_seconds': execution_time_ms / 1000,
        'data_scanned_bytes': data_scanned_bytes,
        'data_scanned_gb': round(data_scanned_gb, 3),
        'data_scanned_tb': round(data_scanned_tb, 6),
        'estimated_cost_usd': round(query_cost, 6)
    }
    
    return query_info

# 5. Generate cost optimization recommendations
def generate_cost_optimization_recommendations(workgroup_name):
    """Generate cost optimization recommendations"""
    
    # Get recent query history
    recent_queries = athena_client.list_query_executions(
        WorkGroup=workgroup_name,
        MaxResults=100
    )
    
    recommendations = []
    total_data_scanned = 0
    expensive_queries = []
    
    for query_id in recent_queries['QueryExecutionIds']:
        query_info = track_query_costs(workgroup_name, query_id)
        total_data_scanned += query_info['data_scanned_tb']
        
        if query_info['estimated_cost_usd'] > 1.0:  # Expensive queries
            expensive_queries.append(query_info)
    
    # Generate recommendations
    if total_data_scanned > 10:  # More than 10 TB scanned
        recommendations.append({
            'category': 'Data Format',
            'recommendation': 'Consider converting data to columnar formats (Parquet, ORC)',
            'potential_savings': '50-80% reduction in data scanned'
        })
    
    if len(expensive_queries) > 5:
        recommendations.append({
            'category': 'Query Optimization',
            'recommendation': 'Review and optimize expensive queries',
            'potential_savings': '30-60% reduction in costs'
        })
    
    recommendations.append({
        'category': 'Partitioning',
        'recommendation': 'Implement partition projection for time-based queries',
        'potential_savings': '70-90% reduction in data scanned'
    })
    
    recommendations.append({
        'category': 'Result Caching',
        'recommendation': 'Leverage Athena result caching for repeated queries',
        'potential_savings': '100% savings on cached queries'
    })
    
    return {
        'workgroup_name': workgroup_name,
        'total_data_scanned_tb': round(total_data_scanned, 3),
        'expensive_queries_count': len(expensive_queries),
        'recommendations': recommendations
    }

# 6. Automated cost control actions
def automated_cost_control_actions(workgroup_name, cost_threshold_usd):
    """Implement automated cost control actions"""
    
    def lambda_handler(event, context):
        """Lambda function for automated cost controls"""
        
        # Check current month costs
        report = monitor_workgroup_costs(workgroup_name, days=30)
        
        if report['estimated_cost_usd'] > cost_threshold_usd:
            # Reduce query limits
            athena_client.update_work_group(
                WorkGroup=workgroup_name,
                ConfigurationUpdates={
                    'BytesScannedCutoffPerQuery': 50 * 1024 * 1024 * 1024,  # 50 GB
                    'EnforceWorkGroupConfiguration': True
                }
            )
            
            # Send notification
            sns_client = boto3.client('sns')
            sns_client.publish(
                TopicArn='arn:aws:sns:us-west-2:123456789012:athena-cost-alerts',
                Message=f'Cost threshold exceeded for workgroup {workgroup_name}. Query limits reduced.',
                Subject=f'Athena Cost Alert - {workgroup_name}'
            )
            
            return {
                'statusCode': 200,
                'body': json.dumps('Cost controls activated')
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps('Cost within limits')
        }
    
    return lambda_handler

# 7. Cost reporting dashboard
def generate_cost_dashboard_data(workgroup_names):
    """Generate data for cost reporting dashboard"""
    
    dashboard_data = {
        'workgroups': [],
        'total_cost': 0,
        'total_queries': 0,
        'total_data_scanned': 0
    }
    
    for workgroup_name in workgroup_names:
        report = monitor_workgroup_costs(workgroup_name, days=30)
        dashboard_data['workgroups'].append(report)
        dashboard_data['total_cost'] += report['estimated_cost_usd']
        dashboard_data['total_queries'] += report['total_queries']
        dashboard_data['total_data_scanned'] += report['total_tb_scanned']
    
    return dashboard_data

# 8. Usage examples
def demonstrate_cost_controls():
    """Demonstrate cost control implementation"""
    
    workgroup_name = 'analytics-team'
    
    # Configure cost controls
    configure_cost_controls(workgroup_name, daily_limit_gb=100, monthly_limit_gb=2000)
    
    # Monitor costs
    cost_report = monitor_workgroup_costs(workgroup_name, days=30)
    print(f"Cost Report: {cost_report}")
    
    # Create budget
    create_cost_budget(workgroup_name, monthly_budget_usd=500)
    
    # Generate recommendations
    recommendations = generate_cost_optimization_recommendations(workgroup_name)
    print(f"Recommendations: {recommendations}")
    
    # Generate dashboard data
    dashboard_data = generate_cost_dashboard_data([workgroup_name])
    print(f"Dashboard Data: {dashboard_data}")

if __name__ == "__main__":
    demonstrate_cost_controls()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Athena Workgroup Monitoring and Alerting

import boto3
import json
from datetime import datetime, timedelta

cloudwatch_client = boto3.client('cloudwatch')
sns_client = boto3.client('sns')
athena_client = boto3.client('athena')

# 1. Comprehensive monitoring setup
def setup_comprehensive_monitoring(workgroup_name):
    """Set up comprehensive monitoring for Athena workgroup"""
    
    # Create CloudWatch dashboard
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "properties": {
                    "metrics": [
                        ["AWS/Athena", "DataScannedInBytes", "WorkGroup", workgroup_name],
                        ["AWS/Athena", "QueryExecutionTime", "WorkGroup", workgroup_name],
                        ["AWS/Athena", "ProcessedBytes", "WorkGroup", workgroup_name]
                    ],
                    "period": 300,
                    "stat": "Sum",
                    "region": "us-west-2",
                    "title": f"Athena Metrics - {workgroup_name}",
                    "view": "timeSeries"
                }
            },
            {
                "type": "metric",
                "properties": {
                    "metrics": [
                        ["AWS/Athena", "QueryExecutionTime", "WorkGroup", workgroup_name, {"stat": "Average"}],
                        ["AWS/Athena", "QueryExecutionTime", "WorkGroup", workgroup_name, {"stat": "Maximum"}]
                    ],
                    "period": 300,
                    "region": "us-west-2",
                    "title": f"Query Performance - {workgroup_name}",
                    "view": "timeSeries"
                }
            },
            {
                "type": "log",
                "properties": {
                    "query": f"SOURCE '/aws/athena/{workgroup_name}' | fields @timestamp, @message | sort @timestamp desc | limit 100",
                    "region": "us-west-2",
                    "title": f"Recent Query Logs - {workgroup_name}",
                    "view": "table"
                }
            }
        ]
    }
    
    cloudwatch_client.put_dashboard(
        DashboardName=f'AthenaWorkgroup-{workgroup_name}',
        DashboardBody=json.dumps(dashboard_body)
    )
    
    return "Dashboard created successfully"

# 2. Create comprehensive alerting system
def create_alerting_system(workgroup_name, sns_topic_arn):
    """Create comprehensive alerting system"""
    
    alerts = [
        {
            'name': f'athena-{workgroup_name}-high-cost',
            'metric': 'DataScannedInBytes',
            'threshold': 500 * 1024 * 1024 * 1024,  # 500 GB
            'comparison': 'GreaterThanThreshold',
            'period': 3600,  # 1 hour
            'evaluation_periods': 1,
            'description': 'High data scan volume detected'
        },
        {
            'name': f'athena-{workgroup_name}-slow-queries',
            'metric': 'QueryExecutionTime',
            'threshold': 300000,  # 5 minutes in milliseconds
            'comparison': 'GreaterThanThreshold',
            'period': 300,
            'evaluation_periods': 2,
            'description': 'Slow query execution detected'
        },
        {
            'name': f'athena-{workgroup_name}-failed-queries',
            'metric': 'QueryExecutionTime',
            'threshold': 10,
            'comparison': 'LessThanThreshold',
            'period': 300,
            'evaluation_periods': 1,
            'description': 'Query failures detected'
        },
        {
            'name': f'athena-{workgroup_name}-high-volume',
            'metric': 'QueryExecutionTime',
            'statistic': 'SampleCount',
            'threshold': 100,
            'comparison': 'GreaterThanThreshold',
            'period': 3600,
            'evaluation_periods': 1,
            'description': 'High query volume detected'
        }
    ]
    
    created_alarms = []
    
    for alert in alerts:
        try:
            cloudwatch_client.put_metric_alarm(
                AlarmName=alert['name'],
                ComparisonOperator=alert['comparison'],
                EvaluationPeriods=alert['evaluation_periods'],
                MetricName=alert['metric'],
                Namespace='AWS/Athena',
                Period=alert['period'],
                Statistic=alert.get('statistic', 'Sum'),
                Threshold=alert['threshold'],
                ActionsEnabled=True,
                AlarmActions=[sns_topic_arn],
                AlarmDescription=alert['description'],
                Dimensions=[
                    {
                        'Name': 'WorkGroup',
                        'Value': workgroup_name
                    }
                ]
            )
            created_alarms.append(alert['name'])
        except Exception as e:
            print(f"Error creating alarm {alert['name']}: {e}")
    
    return created_alarms

# 3. Real-time query monitoring
def monitor_query_performance(workgroup_name, hours=24):
    """Monitor query performance in real-time"""
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)
    
    # Get query execution metrics
    query_metrics = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/Athena',
        MetricName='QueryExecutionTime',
        Dimensions=[
            {
                'Name': 'WorkGroup',
                'Value': workgroup_name
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=300,  # 5 minutes
        Statistics=['Average', 'Maximum', 'Sum', 'SampleCount']
    )
    
    # Get data scanned metrics
    data_metrics = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/Athena',
        MetricName='DataScannedInBytes',
        Dimensions=[
            {
                'Name': 'WorkGroup',
                'Value': workgroup_name
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=300,
        Statistics=['Sum']
    )
    
    # Analyze performance trends
    performance_analysis = {
        'workgroup': workgroup_name,
        'monitoring_period_hours': hours,
        'query_performance': {
            'total_queries': sum(point['SampleCount'] for point in query_metrics['Datapoints']),
            'avg_execution_time_ms': sum(point['Average'] for point in query_metrics['Datapoints']) / max(len(query_metrics['Datapoints']), 1),
            'max_execution_time_ms': max((point['Maximum'] for point in query_metrics['Datapoints']), default=0),
            'slow_queries_count': sum(1 for point in query_metrics['Datapoints'] if point['Maximum'] > 60000)  # > 1 minute
        },
        'data_usage': {
            'total_data_scanned_gb': sum(point['Sum'] for point in data_metrics['Datapoints']) / (1024 ** 3),
            'avg_data_per_query_mb': (sum(point['Sum'] for point in data_metrics['Datapoints']) / (1024 ** 2)) / max(sum(point['SampleCount'] for point in query_metrics['Datapoints']), 1)
        }
    }
    
    return performance_analysis

# 4. Automated anomaly detection
def setup_anomaly_detection(workgroup_name):
    """Set up anomaly detection for workgroup metrics"""
    
    # Create anomaly detector for data scanned
    cloudwatch_client.put_anomaly_detector(
        Namespace='AWS/Athena',
        MetricName='DataScannedInBytes',
        Dimensions=[
            {
                'Name': 'WorkGroup',
                'Value': workgroup_name
            }
        ],
        Stat='Sum'
    )
    
    # Create anomaly detector for query execution time
    cloudwatch_client.put_anomaly_detector(
        Namespace='AWS/Athena',
        MetricName='QueryExecutionTime',
        Dimensions=[
            {
                'Name': 'WorkGroup',
                'Value': workgroup_name
            }
        ],
        Stat='Average'
    )
    
    # Create alarms for anomaly detection
    cloudwatch_client.put_metric_alarm(
        AlarmName=f'athena-{workgroup_name}-data-scan-anomaly',
        ComparisonOperator='LessThanLowerOrGreaterThanUpperThreshold',
        EvaluationPeriods=2,
        Metrics=[
            {
                'Id': 'm1',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/Athena',
                        'MetricName': 'DataScannedInBytes',
                        'Dimensions': [
                            {
                                'Name': 'WorkGroup',
                                'Value': workgroup_name
                            }
                        ]
                    },
                    'Period': 300,
                    'Stat': 'Sum'
                }
            },
            {
                'Id': 'ad1',
                'Expression': 'ANOMALY_DETECTION_FUNCTION(m1, 2)'
            }
        ],
        ThresholdMetricId='ad1',
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-west-2:123456789012:athena-anomaly-alerts'
        ],
        AlarmDescription=f'Anomaly detected in data scan patterns for {workgroup_name}'
    )
    
    return "Anomaly detection configured"

# 5. Custom metrics and logging
def setup_custom_metrics(workgroup_name):
    """Set up custom metrics for enhanced monitoring"""
    
    def publish_custom_metrics(query_execution_id, workgroup_name):
        """Publish custom metrics for query execution"""
        
        # Get query details
        query_details = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        
        query_execution = query_details['QueryExecution']
        
        # Extract metrics
        data_scanned_bytes = query_execution['Statistics'].get('DataScannedInBytes', 0)
        execution_time_ms = query_execution['Statistics'].get('EngineExecutionTimeInMillis', 0)
        
        # Calculate custom metrics
        cost_per_query = (data_scanned_bytes / (1024 ** 4)) * 5  # $5 per TB
        efficiency_score = data_scanned_bytes / max(execution_time_ms, 1)  # bytes per ms
        
        # Publish custom metrics
        cloudwatch_client.put_metric_data(
            Namespace='Custom/Athena',
            MetricData=[
                {
                    'MetricName': 'CostPerQuery',
                    'Value': cost_per_query,
                    'Unit': 'None',
                    'Dimensions': [
                        {
                            'Name': 'WorkGroup',
                            'Value': workgroup_name
                        }
                    ]
                },
                {
                    'MetricName': 'QueryEfficiency',
                    'Value': efficiency_score,
                    'Unit': 'None',
                    'Dimensions': [
                        {
                            'Name': 'WorkGroup',
                            'Value': workgroup_name
                        }
                    ]
                }
            ]
        )
    
    return publish_custom_metrics

# 6. Automated reporting
def generate_monitoring_report(workgroup_name, days=7):
    """Generate comprehensive monitoring report"""
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)
    
    # Get all relevant metrics
    metrics = [
        'DataScannedInBytes',
        'QueryExecutionTime',
        'ProcessedBytes'
    ]
    
    report = {
        'workgroup': workgroup_name,
        'report_period': f'{days} days',
        'generated_at': end_time.isoformat(),
        'metrics': {}
    }
    
    for metric in metrics:
        metric_data = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/Athena',
            MetricName=metric,
            Dimensions=[
                {
                    'Name': 'WorkGroup',
                    'Value': workgroup_name
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,  # Daily
            Statistics=['Sum', 'Average', 'Maximum', 'SampleCount']
        )
        
        report['metrics'][metric] = {
            'datapoints': len(metric_data['Datapoints']),
            'total': sum(point['Sum'] for point in metric_data['Datapoints']),
            'average': sum(point['Average'] for point in metric_data['Datapoints']) / max(len(metric_data['Datapoints']), 1),
            'maximum': max((point['Maximum'] for point in metric_data['Datapoints']), default=0),
            'sample_count': sum(point['SampleCount'] for point in metric_data['Datapoints'])
        }
    
    # Calculate derived metrics
    total_data_scanned_tb = report['metrics']['DataScannedInBytes']['total'] / (1024 ** 4)
    estimated_cost = total_data_scanned_tb * 5
    total_queries = report['metrics']['QueryExecutionTime']['sample_count']
    
    report['summary'] = {
        'total_queries': int(total_queries),
        'total_data_scanned_tb': round(total_data_scanned_tb, 3),
        'estimated_cost_usd': round(estimated_cost, 2),
        'avg_cost_per_query': round(estimated_cost / max(total_queries, 1), 4),
        'avg_execution_time_ms': report['metrics']['QueryExecutionTime']['average']
    }
    
    return report

# 7. AlertManager integration
def setup_alertmanager_integration(workgroup_name):
    """Set up AlertManager-style integration"""
    
    def lambda_alert_handler(event, context):
        """Lambda function to handle CloudWatch alarms"""
        
        # Parse CloudWatch alarm message
        message = json.loads(event['Records'][0]['Sns']['Message'])
        
        alert_details = {
            'workgroup': workgroup_name,
            'alarm_name': message['AlarmName'],
            'state': message['NewStateValue'],
            'reason': message['NewStateReason'],
            'timestamp': message['StateChangeTime'],
            'metric_name': message['MetricName'],
            'threshold': message['Threshold']
        }
        
        # Determine severity
        if 'cost' in message['AlarmName'].lower():
            severity = 'critical'
        elif 'slow' in message['AlarmName'].lower():
            severity = 'warning'
        else:
            severity = 'info'
        
        # Send to multiple channels
        channels = [
            {'type': 'email', 'endpoint': 'data-team@company.com'},
            {'type': 'slack', 'endpoint': 'https://hooks.slack.com/services/...'},
            {'type': 'pagerduty', 'endpoint': 'https://events.pagerduty.com/...'}
        ]
        
        for channel in channels:
            if channel['type'] == 'email':
                send_email_alert(alert_details, channel['endpoint'])
            elif channel['type'] == 'slack':
                send_slack_alert(alert_details, channel['endpoint'])
            elif channel['type'] == 'pagerduty' and severity == 'critical':
                send_pagerduty_alert(alert_details, channel['endpoint'])
        
        return {
            'statusCode': 200,
            'body': json.dumps('Alert processed successfully')
        }
    
    return lambda_alert_handler

# 8. Usage examples
def demonstrate_monitoring_setup():
    """Demonstrate comprehensive monitoring setup"""
    
    workgroup_name = 'analytics-team'
    sns_topic_arn = 'arn:aws:sns:us-west-2:123456789012:athena-alerts'
    
    # Set up monitoring
    setup_comprehensive_monitoring(workgroup_name)
    
    # Create alerting system
    created_alarms = create_alerting_system(workgroup_name, sns_topic_arn)
    print(f"Created alarms: {created_alarms}")
    
    # Set up anomaly detection
    setup_anomaly_detection(workgroup_name)
    
    # Generate performance report
    performance_report = monitor_query_performance(workgroup_name, hours=24)
    print(f"Performance Report: {performance_report}")
    
    # Generate monitoring report
    monitoring_report = generate_monitoring_report(workgroup_name, days=7)
    print(f"Monitoring Report: {monitoring_report}")

if __name__ == "__main__":
    demonstrate_monitoring_setup()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

# Helper functions
def generate_athena_query(data_source, query_type, file_format, partition_projection):
    """Generate sample Athena query based on parameters"""
    
    base_table = f"CREATE EXTERNAL TABLE {data_source} ("
    
    if data_source == "sales_data":
        columns = """
    order_id string,
    customer_id string,
    product_id string,
    order_date date,
    quantity int,
    unit_price decimal(10,2),
    total_amount decimal(12,2),
    region string"""
    elif data_source == "customer_logs":
        columns = """
    log_id string,
    customer_id string,
    timestamp timestamp,
    action string,
    page_url string,
    session_id string"""
    else:
        columns = """
    event_id string,
    user_id string,
    event_type string,
    event_timestamp timestamp,
    properties map<string,string>"""
    
    base_table += columns + "\n)"
    
    # Add partitioning
    if partition_projection:
        base_table += "\nPARTITIONED BY (year int, month int, day int)"
    
    # Add storage format
    if file_format == "Parquet":
        base_table += "\nSTORED AS PARQUET"
    elif file_format == "ORC":
        base_table += "\nSTORED AS ORC"
    else:
        base_table += f"\nSTORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'"
    
    # Add location
    base_table += f"\nLOCATION 's3://my-data-bucket/{data_source}/'"
    
    # Add table properties for partition projection
    if partition_projection:
        base_table += """
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.year.type' = 'integer',
    'projection.year.range' = '2020,2025',
    'projection.month.type' = 'integer',
    'projection.month.range' = '1,12',
    'projection.day.type' = 'integer',
    'projection.day.range' = '1,31',
    'storage.location.template' = 's3://my-data-bucket/{data_source}/year=${{year}}/month=${{month}}/day=${{day}}/'
);"""
    
    # Add sample query based on type
    if query_type == "SELECT - Data Retrieval":
        sample_query = f"""
-- Sample SELECT query
SELECT *
FROM {data_source}
WHERE year = 2024 AND month = 12
LIMIT 100;"""
    elif query_type == "AGGREGATE - Grouping & Metrics":
        sample_query = f"""
-- Sample AGGREGATE query
SELECT 
    region,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value
FROM {data_source}
WHERE year = 2024
GROUP BY region
ORDER BY total_revenue DESC;"""
    else:
        sample_query = f"""
-- Sample complex query
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent
FROM {data_source}
WHERE year = 2024
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 50;"""
    
    return base_table + "\n" + sample_query

def calculate_query_cost(query, file_format, columnar_format):
    """Calculate estimated query cost"""
    
    # Base cost factors
    base_cost_per_gb = 0.005  # $5 per TB = $0.005 per GB
    
    # Estimated data scan (simplified)
    estimated_scan_gb = 10  # Default 10 GB
    
    # Adjust for format efficiency
    if columnar_format:
        estimated_scan_gb *= 0.3  # 70% reduction for columnar
    
    if file_format in ['Parquet', 'ORC']:
        estimated_scan_gb *= 0.5  # Additional 50% reduction
    
    # Calculate cost
    estimated_cost = estimated_scan_gb * base_cost_per_gb
    
    return estimated_cost

def simulate_query_execution(query, file_format):
    """Simulate query execution"""
    
    # Update session state
    st.session_state.queries_executed += 1
    st.session_state.data_scanned_gb += random.uniform(1, 50)
    
    # Simulate execution metrics
    execution_time = random.uniform(2, 30)  # 2-30 seconds
    data_scanned = random.uniform(5, 100)   # 5-100 GB
    
    # Show results
    st.success(f"✅ Query executed successfully!")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Execution Time", f"{execution_time:.2f}s")
    with col2:
        st.metric("Data Scanned", f"{data_scanned:.2f} GB")
    with col3:
        st.metric("Estimated Cost", f"${data_scanned * 0.005:.4f}")
    
    # Show sample results
    st.markdown("#### 📊 Sample Query Results")
    
    if "sales_data" in query:
        sample_data = pd.DataFrame({
            'order_id': ['ORD001', 'ORD002', 'ORD003'],
            'customer_id': ['CUST001', 'CUST002', 'CUST003'],
            'total_amount': [125.50, 89.99, 234.75],
            'region': ['us-east', 'us-west', 'eu-west']
        })
    else:
        sample_data = pd.DataFrame({
            'id': ['001', '002', '003'],
            'timestamp': pd.date_range('2024-12-01', periods=3),
            'value': [100, 200, 300]
        })
    
    st.dataframe(sample_data, use_container_width=True)

def generate_federated_query(primary_source, secondary_source, join_type, optimization):
    """Generate federated query example"""
    
    # Map sources to connector syntax
    source_mapping = {
        "S3 Data Lake": '"s3_catalog"."sales_data"',
        "Amazon RDS": '"lambda:athena-jdbc-connector"."orders"',
        "Amazon DynamoDB": '"lambda:athena-dynamodb-connector"."customers"',
        "Amazon ElastiCache": '"lambda:athena-redis-connector"."sessions"'
    }
    
    primary_table = source_mapping.get(primary_source, '"s3_catalog"."default_table"')
    secondary_table = source_mapping.get(secondary_source, '"lambda:connector"."default_table"')
    
    # Generate JOIN query
    query = f"""-- Federated Query Example
-- Primary Source: {primary_source}
-- Secondary Source: {secondary_source}
-- Optimization: {optimization}

SELECT 
    p.order_id,
    p.customer_id,
    p.order_date,
    p.total_amount,
    s.customer_name,
    s.customer_segment,
    s.loyalty_tier
FROM {primary_table} p
{join_type} {secondary_table} s
    ON p.customer_id = s.customer_id
WHERE p.order_date >= DATE '2024-12-01'
    AND p.total_amount > 100
ORDER BY p.total_amount DESC
LIMIT 1000;

-- Performance Notes:
-- {optimization} will be applied to optimize data transfer
-- Lambda connectors handle predicate pushdown automatically
-- Results are cached for 24 hours by default"""
    
    return query

def simulate_federated_query_execution(primary_source, secondary_source):
    """Simulate federated query execution"""
    
    # Update session state
    st.session_state.federated_queries += 1
    st.session_state.data_scanned_gb += random.uniform(5, 25)
    
    # Simulate execution metrics
    execution_time = random.uniform(10, 60)  # 10-60 seconds (slower than regular queries)
    data_scanned = random.uniform(10, 50)    # 10-50 GB
    lambda_executions = random.randint(2, 10)
    
    # Show results
    st.success(f"✅ Federated query executed successfully!")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Execution Time", f"{execution_time:.2f}s")
    with col2:
        st.metric("Data Scanned", f"{data_scanned:.2f} GB")
    with col3:
        st.metric("Lambda Executions", lambda_executions)
    with col4:
        st.metric("Estimated Cost", f"${data_scanned * 0.005 + lambda_executions * 0.0000002:.4f}")
    
    # Show sample federated results
    st.markdown("#### 📊 Federated Query Results")
    
    federated_data = pd.DataFrame({
        'order_id': ['ORD001', 'ORD002', 'ORD003'],
        'customer_id': ['CUST001', 'CUST002', 'CUST003'],
        'total_amount': [125.50, 89.99, 234.75],
        'customer_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'customer_segment': ['Premium', 'Standard', 'Premium'],
        'source_combination': [f'{primary_source} + {secondary_source}'] * 3
    })
    
    st.dataframe(federated_data, use_container_width=True)

def generate_workgroup_config(name, wg_type, per_query_limit, workgroup_limit, encryption, enforce_config, bucket, prefix):
    """Generate workgroup configuration"""
    
    config = {
        'Name': name,
        'Description': f'{wg_type} workgroup for query isolation and cost control',
        'Configuration': {
            'ResultConfiguration': {
                'OutputLocation': f's3://{bucket}/{prefix}',
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3' if encryption else 'NONE'
                }
            },
            'EnforceWorkGroupConfiguration': enforce_config,
            'PublishCloudWatchMetrics': True,
            'BytesScannedCutoffPerQuery': per_query_limit * 1024 * 1024 * 1024,  # Convert to bytes
            'RequesterPaysEnabled': False
        },
        'Tags': [
            {'Key': 'Environment', 'Value': wg_type},
            {'Key': 'Team', 'Value': 'Analytics'},
            {'Key': 'CostCenter', 'Value': 'DataEngineering'}
        ]
    }
    
    return config

def simulate_workgroup_creation(config):
    """Simulate workgroup creation"""
    
    # Update session state
    st.session_state.workgroups_created += 1
    
    # Show creation success
    st.success(f"✅ Workgroup '{config['Name']}' created successfully!")
    
    # Show configuration details
    st.markdown("#### ⚙️ Workgroup Configuration")
    st.json(config)
    
    # Show IAM policy example
    st.markdown("#### 🔒 Suggested IAM Policy")
    st.code(f'''
{{
    "Version": "2012-10-17",
    "Statement": [
        {{
            "Effect": "Allow",
            "Action": [
                "athena:BatchGetQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StartQueryExecution",
                "athena:StopQueryExecution"
            ],
            "Resource": "arn:aws:athena:*:*:workgroup/{config['Name']}"
        }},
        {{
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::{config['Configuration']['ResultConfiguration']['OutputLocation'].replace('s3://', '')}*"
        }}
    ]
}}''', language='json')

def generate_cost_data():
    """Generate sample cost data for visualization"""
    
    # Generate daily cost data
    dates = pd.date_range(start='2024-11-01', end='2024-12-01', freq='D')
    workgroups = ['production', 'development', 'analytics']
    
    daily_costs = []
    for date in dates:
        for wg in workgroups:
            cost = random.uniform(5, 50) * (1.5 if wg == 'production' else 1.0)
            daily_costs.append({
                'date': date,
                'workgroup': wg,
                'cost': cost
            })
    
    total_cost = sum(item['cost'] for item in daily_costs)
    total_queries = random.randint(5000, 15000)
    data_scanned_tb = total_cost / 5  # $5 per TB
    
    return {
        'monthly_cost': total_cost,
        'data_scanned_tb': data_scanned_tb,
        'queries_run': total_queries,
        'avg_cost_per_query': total_cost / total_queries,
        'daily_costs': pd.DataFrame(daily_costs)
    }

def main():
    """Main application function"""
    # Initialize mermaid
    common.initialize_mermaid()
    
    # Apply styling
    apply_custom_styles()
    
    # Initialize session
    initialize_session_state()
    
    # Create sidebar
    create_sidebar()
    
    # Main header
    st.markdown("""
    # 🔍 AWS Data Querying & Analytics
    <div class='info-box'>
    Master serverless data querying with Amazon Athena, including federated queries across multiple data sources and workgroup management for cost control and access management.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs([
        "🔍 Athena",
        "🌐 Athena Federated Query", 
        "👥 Athena Workgroups"
    ])
    
    with tab1:
        athena_tab()
    
    with tab2:
        federated_query_tab()
    
    with tab3:
        workgroups_tab()
    
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
