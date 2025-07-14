import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import utils.common as common
import utils.authenticate as authenticate

# Page configuration
st.set_page_config(
    page_title="AWS Data Ingestion Learning Hub",
    page_icon="📊",
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
    'warning': '#FF6B35'
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
        
        .comparison-card {{
            background: white;
            padding: 20px;
            border-radius: 12px;
            border: 2px solid {AWS_COLORS['light_blue']};
            margin: 15px 0;
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
            - 📊 Data Ingestion Fundamentals
            - 🏗️ Data Analytics Overview on AWS
            - ⚡ Batch and Stream Processing
            - 🌊 Amazon Kinesis Data Streams
            - 🚀 Amazon Data Firehose
            - 🔄 AWS Lambda Integration
            - 📱 IoT Data Monitoring
            - 🗄️ DynamoDB Streams
            
            **Learning Objectives:**
            - Understand data ingestion patterns
            - Learn streaming vs batch processing
            - Master AWS data services
            - Implement real-world data pipelines
            """)

def create_data_ingestion_overview_mermaid():
    """Create mermaid diagram for data ingestion overview"""
    return """
    graph TD
        A[📊 Data Sources] --> B[🔄 Data Ingestion]
        B --> C[⚙️ Data Processing]
        C --> D[🗄️ Data Storage]
        D --> E[📈 Data Analytics]
        
        A --> A1[🌐 Web APIs]
        A --> A2[📱 Mobile Apps]
        A --> A3[🏭 IoT Devices]
        A --> A4[🗂️ Databases]
        
        B --> B1[🌊 Streaming]
        B --> B2[📦 Batch]
        B --> B3[⚡ Real-time]
        
        C --> C1[🧹 Data Cleaning]
        C --> C2[🔧 Transformation]
        C --> C3[📊 Enrichment]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#232F3E,stroke:#FF9900,color:#fff
        style E fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_batch_vs_stream_mermaid():
    """Create mermaid diagram comparing batch and stream processing"""
    return """
    graph LR
        subgraph "Batch Processing"
            B1[📦 Large Data Sets] --> B2[⏰ Scheduled Processing]
            B2 --> B3[📊 Complete Analysis]
            B3 --> B4[📋 Reports & Insights]
        end
        
        subgraph "Stream Processing"
            S1[🌊 Live Data Stream] --> S2[⚡ Real-time Processing]
            S2 --> S3[🚨 Immediate Actions]
            S3 --> S4[📱 Live Dashboards]
        end
        
        B1 -.->|Minutes to Hours| B4
        S1 -.->|Milliseconds to Seconds| S4
        
        style B1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style B2 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style B3 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style B4 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style S1 fill:#FF9900,stroke:#232F3E,color:#fff
        style S2 fill:#FF9900,stroke:#232F3E,color:#fff
        style S3 fill:#FF9900,stroke:#232F3E,color:#fff
        style S4 fill:#FF9900,stroke:#232F3E,color:#fff
    """

def create_kinesis_data_streams_mermaid():
    """Create mermaid diagram for Kinesis Data Streams"""
    return """
    graph TD
        A[📱 Data Producers] --> B[🌊 Kinesis Data Streams]
        B --> C[📊 Data Consumers]
        
        A --> A1[Mobile Apps]
        A --> A2[Web Applications]
        A --> A3[IoT Sensors]
        A --> A4[Log Files]
        
        B --> B1[Shard 1]
        B --> B2[Shard 2]
        B --> B3[Shard 3]
        B --> B4[Shard N]
        
        C --> C1[🔧 AWS Lambda]
        C --> C2[📈 Kinesis Analytics]
        C --> C3[🚀 Kinesis Firehose]
        C --> C4[🖥️ EC2 Applications]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_lambda_kinesis_mermaid():
    """Create mermaid diagram for Lambda with Kinesis"""
    return """
    graph LR
        A[🌊 Kinesis Data Stream] --> B[⚡ Event Source Mapping]
        B --> C[🔧 Lambda Function]
        C --> D[📊 Process Records]
        D --> E[🗄️ Store Results]
        
        A --> A1[Shard 1: Records 1-100]
        A --> A2[Shard 2: Records 101-200]
        A --> A3[Shard 3: Records 201-300]
        
        C --> C1[Function Instance 1]
        C --> C2[Function Instance 2]
        C --> C3[Function Instance 3]
        
        E --> E1[📊 DynamoDB]
        E --> E2[🗄️ S3]
        E --> E3[🔔 SNS]
        
        style A fill:#4B9EDB,stroke:#232F3E,color:#fff
        style B fill:#FF9900,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#232F3E,stroke:#FF9900,color:#fff
        style E fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_streaming_comparison_chart():
    """Create comparison chart for streaming services"""
    data = {
        'Service': ['Kinesis Data Streams', 'MSK (Kafka)', 'Kinesis Data Firehose'],
        'Ease of Use': [8, 6, 10],
        'Throughput': [9, 10, 8],
        'Latency (Lower is Better)': [8, 9, 6],
        'Management Overhead': [9, 7, 10],
        'Cost Effectiveness': [8, 7, 9]
    }
    
    df = pd.DataFrame(data)
    
    fig = go.Figure()
    
    for i, service in enumerate(df['Service']):
        fig.add_trace(go.Scatterpolar(
            r=[df.iloc[i]['Ease of Use'], df.iloc[i]['Throughput'], 
               df.iloc[i]['Latency (Lower is Better)'], df.iloc[i]['Management Overhead'],
               df.iloc[i]['Cost Effectiveness']],
            theta=['Ease of Use', 'Throughput', 'Latency', 'Management', 'Cost'],
            fill='toself',
            name=service,
            line_color=[AWS_COLORS['primary'], AWS_COLORS['light_blue'], AWS_COLORS['success']][i]
        ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 10]
            )),
        showlegend=True,
        title="AWS Streaming Services Comparison"
    )
    
    return fig

def data_ingestion_fundamentals_tab():
    """Content for data ingestion fundamentals tab"""
    st.markdown("# 📊 Data Ingestion Fundamentals")
    st.markdown("*Learn the core concepts of bringing data into AWS*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What is Data Ingestion?
    
    **Data ingestion** is the process of bringing data from various sources into a system for further 
    processing or storage. Think of it like gathering ingredients for a recipe - you need to collect 
    all the necessary items before you can start cooking.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Ingestion Overview
    st.markdown("## 🏗️ Data Ingestion Process Overview")
    common.mermaid(create_data_ingestion_overview_mermaid(), height=400)
    
    # Interactive Data Ingestion Simulator
    st.markdown("## 🔧 Interactive Data Ingestion Simulator")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        data_source = st.selectbox("Choose Data Source:", [
            "📱 Mobile Application",
            "🌐 Web API",
            "🏭 IoT Sensor Network", 
            "🗄️ Database System",
            "📊 Log Files"
        ])
    
    with col2:
        ingestion_pattern = st.selectbox("Ingestion Pattern:", [
            "🌊 Real-time Streaming",
            "📦 Batch Processing",
            "⚡ Micro-batch",
            "📅 Scheduled Ingestion"
        ])
    
    with col3:
        data_volume = st.slider("Data Volume (GB/hour):", 0.1, 1000.0, 10.0, step=0.1)
    
    if st.button("🚀 Simulate Data Ingestion", use_container_width=True):
        
        # Generate realistic metrics based on inputs
        latency = {
            "🌊 Real-time Streaming": f"{np.random.randint(50, 200)} ms",
            "📦 Batch Processing": f"{np.random.randint(5, 30)} minutes", 
            "⚡ Micro-batch": f"{np.random.randint(1, 10)} seconds",
            "📅 Scheduled Ingestion": f"{np.random.randint(1, 6)} hours"
        }
        
        throughput = data_volume * np.random.uniform(0.8, 1.2)
        cost_per_gb = np.random.uniform(0.01, 0.05)
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Data Ingestion Simulation Results
        
        **Configuration:**
        - **Source**: {data_source}
        - **Pattern**: {ingestion_pattern}
        - **Volume**: {data_volume} GB/hour
        
        **Performance Metrics:**
        - **Latency**: {latency[ingestion_pattern]}
        - **Throughput**: {throughput:.2f} GB/hour
        - **Estimated Cost**: ${cost_per_gb * data_volume:.3f}/hour
        
        **Recommended AWS Services**: Kinesis Data Streams, Kinesis Data Firehose
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Ingestion Patterns
    st.markdown("## 📋 Data Ingestion Patterns")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🌊 Streaming Data Ingestion
        
        **Characteristics:**
        - Continuous data flow
        - Low latency (milliseconds to seconds)
        - Real-time processing
        - Variable data rates
        
        **Use Cases:**
        - IoT sensor data
        - Social media feeds
        - Financial transactions
        - Gaming telemetry
        
        **AWS Services:**
        - Amazon Kinesis Data Streams
        - Amazon MSK (Managed Kafka)
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📦 Batch Data Ingestion
        
        **Characteristics:**
        - Large data volumes
        - Scheduled processing
        - Higher latency (minutes to hours)
        - Cost-effective for large datasets
        
        **Use Cases:**
        - Daily sales reports
        - Log file processing
        - Database dumps
        - Historical data migration
        
        **AWS Services:**
        - AWS Glue
        - Amazon EMR
        - AWS Data Pipeline
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Replayability and Fault Tolerance
    st.markdown("## 🔄 Replayability and Fault Tolerance")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Stateful Transactions
        
        **Characteristics:**
        - Maintain state between operations
        - Support for transactions
        - Exactly-once processing guarantees
        
        **Examples:**
        - Database transactions
        - Financial transfers
        - Order processing systems
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚡ Stateless Transactions
        
        **Characteristics:**
        - No state maintained between operations
        - Independent processing
        - Higher throughput, simpler scaling
        
        **Examples:**
        - Log processing
        - Message queues
        - API requests
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Data Ingestion with Python")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
import boto3
import json
from datetime import datetime

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

def ingest_data_to_kinesis(stream_name, data_records):
    """
    Ingest data records to Kinesis Data Stream
    """
    try:
        records = []
        for record in data_records:
            # Prepare record for Kinesis
            kinesis_record = {
                'Data': json.dumps({
                    'timestamp': datetime.now().isoformat(),
                    'data': record,
                    'source': 'python-app'
                }),
                'PartitionKey': str(hash(record.get('user_id', 'default')))
            }
            records.append(kinesis_record)
        
        # Send records to Kinesis
        response = kinesis_client.put_records(
            Records=records,
            StreamName=stream_name
        )
        
        # Check for failures
        failed_records = []
        for i, record_result in enumerate(response['Records']):
            if 'ErrorCode' in record_result:
                failed_records.append(records[i])
        
        print(f"Successfully ingested {len(records) - len(failed_records)} records")
        if failed_records:
            print(f"Failed to ingest {len(failed_records)} records")
            
        return response
        
    except Exception as e:
        print(f"Error ingesting data: {str(e)}")
        return None

# Example usage
sample_data = [
    {'user_id': '12345', 'event': 'page_view', 'page': '/home'},
    {'user_id': '67890', 'event': 'purchase', 'amount': 99.99},
    {'user_id': '12345', 'event': 'logout', 'session_duration': 1800}
]

# Ingest data
result = ingest_data_to_kinesis('my-data-stream', sample_data)
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def aws_analytics_overview_tab():
    """Content for AWS Analytics Overview tab"""
    st.markdown("# 🏗️ AWS Data Analytics Overview")
    st.markdown("*Comprehensive view of AWS data analytics services*")
    
    # Analytics Architecture
    st.markdown("## 🏛️ Data Analytics Architecture on AWS")
    
    # Create a comprehensive analytics overview
    analytics_mermaid = """
    graph TD
        subgraph "Data Ingestion"
            I1[🌊 Real-time: Kinesis Data Streams]
            I2[🚀 Batch: Kinesis Data Firehose]
            I3[📱 Apps: Amazon AppFlow]
            I4[🔄 Stream: Amazon MSK]
        end
        
        subgraph "Data Processing" 
            P1[⚡ Batch: AWS Glue]
            P2[🖥️ Big Data: Amazon EMR]
            P3[🔧 Serverless: AWS Lambda]
            P4[🧹 Interactive: AWS Glue DataBrew]
        end
        
        subgraph "Data Lake"
            L1[🗄️ Storage: Amazon S3]
            L2[🏗️ Setup: AWS Lake Formation]
            L3[📋 Catalog: AWS Glue Data Catalog]
        end
        
        subgraph "Analytics"
            A1[🔍 Query: Amazon Athena]
            A2[📊 Warehouse: Amazon Redshift]
            A3[📈 Visualization: QuickSight]
            A4[🔎 Search: OpenSearch]
        end
        
        I1 --> P1
        I2 --> P2
        I3 --> P3
        I4 --> P4
        
        P1 --> L1
        P2 --> L2
        P3 --> L3
        P4 --> L1
        
        L1 --> A1
        L2 --> A2
        L3 --> A3
        L1 --> A4
        
        style I1 fill:#FF9900,stroke:#232F3E,color:#fff
        style P1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style L1 fill:#3FB34F,stroke:#232F3E,color:#fff
        style A1 fill:#FF6B35,stroke:#232F3E,color:#fff
    """
    
    common.mermaid(analytics_mermaid, height=500)
    
    # Interactive Service Selector
    st.markdown("## 🎛️ AWS Analytics Service Selector")
    
    col1, col2 = st.columns(2)
    
    with col1:
        use_case = st.selectbox("Select Your Use Case:", [
            "🌊 Real-time Analytics",
            "📊 Business Intelligence",
            "🔍 Ad-hoc Queries", 
            "🧠 Machine Learning",
            "📱 IoT Data Processing",
            "📈 Data Warehousing"
        ])
        
        data_size = st.selectbox("Data Volume:", [
            "📝 Small (< 1TB)",
            "📚 Medium (1-100TB)",
            "🏢 Large (100TB-1PB)",
            "🌐 Very Large (> 1PB)"
        ])
    
    with col2:
        latency_req = st.selectbox("Latency Requirements:", [
            "⚡ Real-time (< 1 second)",
            "🚀 Near real-time (1-60 seconds)",
            "⏰ Batch (minutes to hours)",
            "📅 Scheduled (daily/weekly)"
        ])
        
        complexity = st.selectbox("Processing Complexity:", [
            "🔧 Simple transformations",
            "📊 Complex analytics",
            "🧠 Machine learning",
            "🔄 Multi-stage pipelines"  
        ])
    
    if st.button("🎯 Get Service Recommendations", use_container_width=True):
        
        # Service recommendation logic
        recommendations = []
        
        if "Real-time" in use_case or "Real-time" in latency_req:
            recommendations.extend(["Amazon Kinesis Data Streams", "AWS Lambda", "Amazon Kinesis Analytics"])
        
        if "Business Intelligence" in use_case or "Data Warehousing" in use_case:
            recommendations.extend(["Amazon Redshift", "Amazon QuickSight"])
        
        if "Ad-hoc" in use_case:
            recommendations.extend(["Amazon Athena", "Amazon S3"])
        
        if "Machine Learning" in use_case:
            recommendations.extend(["Amazon SageMaker", "Amazon EMR"])
        
        if "Large" in data_size or "Very Large" in data_size:
            recommendations.extend(["Amazon EMR", "AWS Glue"])
        
        # Remove duplicates and limit to top 4
        recommendations = list(dict.fromkeys(recommendations))[:4]
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 🎯 Recommended AWS Services
        
        **Based on your requirements:**
        - **Use Case**: {use_case}
        - **Data Volume**: {data_size}
        - **Latency**: {latency_req}
        - **Complexity**: {complexity}
        
        **Recommended Services:**
        {chr(10).join([f'- **{service}**' for service in recommendations])}
        
        💡 **Next Steps**: Start with a proof-of-concept using these services
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Service Categories
    st.markdown("## 📚 AWS Analytics Service Categories")
    
    # Create tabs for different service categories
    cat_tab1, cat_tab2, cat_tab3, cat_tab4 = st.tabs([
        "📥 Data Ingestion", "⚙️ Data Processing", "🗄️ Data Storage", "📊 Analytics"
    ])
    
    with cat_tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 🌊 Amazon Kinesis Data Streams
            - **Use Case**: Real-time data streaming
            - **Capacity**: Gigabytes per second
            - **Durability**: 24 hours to 365 days
            - **Scaling**: Shard-based scaling
            """)
            st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 📱 Amazon AppFlow
            - **Use Case**: SaaS integration
            - **Sources**: Salesforce, ServiceNow, etc.
            - **Scheduling**: Event-driven or scheduled
            - **Security**: Encryption in transit and at rest
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 🚀 Amazon Kinesis Data Firehose
            - **Use Case**: Load streaming data to data stores
            - **Destinations**: S3, Redshift, OpenSearch
            - **Transformation**: Built-in data transformation
            - **Buffer**: Time and size-based buffering
            """)
            st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 🔄 Amazon MSK
            - **Use Case**: Apache Kafka workloads
            - **Compatibility**: Fully compatible with Kafka
            - **Management**: Fully managed service
            - **Integration**: Connect with other AWS services
            """)
            st.markdown('</div>', unsafe_allow_html=True)
    
    with cat_tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### ⚡ AWS Glue
            - **Use Case**: ETL and data preparation
            - **Serverless**: No infrastructure management
            - **Catalog**: Automatic schema discovery
            - **Languages**: Python and Scala support
            """)
            st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 🔧 AWS Lambda
            - **Use Case**: Event-driven processing
            - **Scale**: Automatic scaling
            - **Languages**: Multiple runtime support
            - **Integration**: Native AWS service integration
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 🖥️ Amazon EMR
            - **Use Case**: Big data processing
            - **Frameworks**: Spark, Hadoop, Hive, etc.
            - **Scaling**: Cluster auto-scaling
            - **Cost**: Spot instance support
            """)
            st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 🧹 AWS Glue DataBrew
            - **Use Case**: Visual data preparation
            - **Interface**: No-code transformations
            - **Profiling**: Data quality insights
            - **Recipes**: Reusable transformation recipes
            """)
            st.markdown('</div>', unsafe_allow_html=True)
    
    with cat_tab3:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 🗄️ Amazon S3
            - **Use Case**: Data lake storage
            - **Durability**: 99.999999999% (11 9's)
            - **Classes**: Multiple storage classes
            - **Integration**: Native analytics integration
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 🏗️ AWS Lake Formation
            - **Use Case**: Data lake setup and governance
            - **Security**: Fine-grained access control
            - **Catalog**: Centralized data catalog
            - **Automation**: Automated data lake creation
            """)
            st.markdown('</div>', unsafe_allow_html=True)
    
    with cat_tab4:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 🔍 Amazon Athena
            - **Use Case**: Serverless SQL queries
            - **Data Sources**: S3, other data sources
            - **Pricing**: Pay per query
            - **Performance**: Optimized for analytics
            """)
            st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 🔎 Amazon OpenSearch
            - **Use Case**: Search and analytics
            - **Real-time**: Log analytics and monitoring
            - **Visualization**: Built-in dashboards
            - **Scale**: Petabyte-scale search
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 📊 Amazon Redshift
            - **Use Case**: Data warehousing
            - **Performance**: Columnar storage
            - **Scaling**: Petabyte-scale
            - **Integration**: BI tool integration
            """)
            st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown('<div class="concept-card">', unsafe_allow_html=True)
            st.markdown("""
            ### 📈 Amazon QuickSight
            - **Use Case**: Business intelligence
            - **ML**: Built-in ML insights
            - **Sharing**: Dashboard sharing
            - **Pricing**: Pay-per-session or user
            """)
            st.markdown('</div>', unsafe_allow_html=True)

def batch_stream_processing_tab():
    """Content for batch and stream processing architectures tab"""
    st.markdown("# ⚡ Batch and Stream Processing Architectures")
    st.markdown("*Understanding the two fundamental data processing paradigms*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Processing Paradigms
    
    **Batch Processing** is like baking a huge batch of cookies all at once - you gather all ingredients, 
    mix them together, bake the entire batch, and enjoy the finished product.
    
    **Stream Processing** is like a constant flow of cookie dough coming through a machine - you can 
    process each piece as it arrives and make immediate adjustments.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Batch vs Stream Comparison
    st.markdown("## ⚖️ Batch vs Stream Processing")
    common.mermaid(create_batch_vs_stream_mermaid(), height=300)
    
    # Detailed Comparison Table
    st.markdown("## 📊 Detailed Comparison")
    
    comparison_data = {
        'Aspect': ['Scope', 'Size', 'Performance', 'Complexity', 'Cost', 'Use Cases'],
        'Batch Processing': [
            'Query entire dataset',
            'Large batches of data', 
            'Minutes to hours latency',
            'Simpler to implement',
            'Lower cost per record',
            'Reports, ETL, Historical analysis'
        ],
        'Stream Processing': [
            'Filter recent events/time windows',
            'Individual events or micro-batches',
            'Milliseconds to seconds',
            'More complex to implement', 
            'Higher cost per record',
            'Real-time alerts, Live dashboards, Fraud detection'
        ]
    }
    
    df_comparison = pd.DataFrame(comparison_data)
    st.dataframe(df_comparison, use_container_width=True)
    
    # Interactive Processing Type Selector
    st.markdown("## 🎛️ Processing Type Decision Matrix")
    
    col1, col2 = st.columns(2)
    
    with col1:
        data_freshness = st.selectbox("How fresh does your data need to be?", [
            "⚡ Real-time (seconds)",
            "🚀 Near real-time (minutes)", 
            "⏰ Periodic (hours)",
            "📅 Scheduled (daily/weekly)"
        ])
        
        data_volume = st.selectbox("What's your data volume?", [
            "📝 Small (MB to GB)",
            "📚 Medium (GB to TB)",
            "🏢 Large (TB to PB)",
            "🌐 Continuous stream"
        ])
    
    with col2:
        use_case_type = st.selectbox("Primary use case:", [
            "🚨 Real-time monitoring",
            "📊 Business reporting",
            "🔍 Data exploration",
            "🧠 Machine learning training",
            "📈 Dashboard analytics"
        ])
        
        budget_priority = st.selectbox("Budget priority:", [
            "💰 Cost optimization",
            "⚡ Performance optimization",
            "🔧 Ease of use",
            "📈 Scalability"
        ])
    
    if st.button("🎯 Get Processing Recommendation", use_container_width=True):
        
        # Decision logic
        stream_score = 0
        batch_score = 0
        
        # Scoring based on inputs
        if "Real-time" in data_freshness:
            stream_score += 3
        elif "Scheduled" in data_freshness:
            batch_score += 3
        
        if "Continuous" in data_volume:
            stream_score += 2
        elif "Large" in data_volume:
            batch_score += 2
        
        if "monitoring" in use_case_type or "Dashboard" in use_case_type:
            stream_score += 2
        elif "reporting" in use_case_type or "exploration" in use_case_type:
            batch_score += 2
        
        if "Performance" in budget_priority:
            stream_score += 1
        elif "Cost" in budget_priority:
            batch_score += 1
        
        # Determine recommendation
        if stream_score > batch_score:
            recommendation = "🌊 Stream Processing"
            services = ["Amazon Kinesis Data Streams", "AWS Lambda", "Amazon Kinesis Analytics"]
            color_class = "highlight-box"
        else:
            recommendation = "📦 Batch Processing" 
            services = ["AWS Glue", "Amazon EMR", "AWS Batch"]
            color_class = "warning-box"
        
        st.markdown(f'<div class="{color_class}">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 🎯 Recommendation: {recommendation}
        
        **Based on your requirements:**
        - **Data Freshness**: {data_freshness}
        - **Data Volume**: {data_volume}
        - **Use Case**: {use_case_type}
        - **Priority**: {budget_priority}
        
        **Recommended AWS Services:**
        {chr(10).join([f'- **{service}**' for service in services])}
        
        **Score**: Stream ({stream_score}) vs Batch ({batch_score})
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # AWS Stream Processing Services
    st.markdown("## 🌊 AWS Stream Processing Services")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🌊 Kinesis Data Streams
        - **Latency**: 70ms with Enhanced Fan-Out
        - **Throughput**: 1000 records/sec per shard
        - **Retention**: 24 hours to 365 days
        - **Use Case**: Custom stream processing
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚀 Kinesis Data Firehose
        - **Latency**: 60-900 seconds
        - **Throughput**: Can scale to GBs/second  
        - **Destinations**: S3, Redshift, OpenSearch
        - **Use Case**: Data lake ingestion
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Kinesis Data Analytics
        - **SQL**: Standard SQL for streaming
        - **Apache Flink**: Advanced stream processing
        - **Integration**: Native Kinesis integration
        - **Use Case**: Real-time analytics
        """)
        st.markdown('</div>', unsafe_allow_html=True)

def kinesis_services_tab():
    """Content for Kinesis Data Streams and Firehose tab"""
    st.markdown("# 🌊 Amazon Kinesis Services")
    st.markdown("*Collect, process, and analyze real-time streaming data*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Amazon Kinesis Overview
    
    **Amazon Kinesis** makes it easy to collect, process, and analyze real-time streaming data. 
    Think of it as a powerful river system that can handle massive amounts of data flowing 
    from multiple tributaries (data sources) to various destinations.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Kinesis Data Streams Architecture
    st.markdown("## 🏗️ Kinesis Data Streams Architecture")
    common.mermaid(create_kinesis_data_streams_mermaid(), height=350)
    
    # Interactive Kinesis Configuration
    st.markdown("## 🔧 Interactive Kinesis Stream Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        stream_name = st.text_input("Stream Name:", "my-data-stream")
        shard_count = st.slider("Number of Shards:", 1, 100, 2)
        retention_period = st.selectbox("Retention Period:", [
            "24 hours", "7 days", "30 days", "365 days"
        ])
    
    with col2:
        expected_records_per_sec = st.number_input("Expected Records/sec:", 1, 100000, 1000)
        record_size_kb = st.slider("Average Record Size (KB):", 1, 1000, 10)
        
        # Calculate throughput
        total_throughput_mb = (expected_records_per_sec * record_size_kb) / 1024
        shards_needed = max(1, int(total_throughput_mb / 1))  # 1MB/sec per shard limit
    
    # Show calculations
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Stream Calculations
    
    **Throughput Analysis:**
    - **Total Throughput**: {total_throughput_mb:.2f} MB/sec
    - **Shards Needed**: {shards_needed} (based on 1MB/sec limit per shard)
    - **Current Configuration**: {shard_count} shards
    - **Status**: {"✅ Adequate" if shard_count >= shards_needed else "⚠️ Need more shards"}
    
    **Cost Estimation** (US East):
    - **Shard Hours**: ${shard_count * 0.015:.3f}/hour
    - **PUT Payload Units**: ~${(expected_records_per_sec * 3600 * 0.000014):.3f}/hour
    - **Total**: ~${(shard_count * 0.015) + (expected_records_per_sec * 3600 * 0.000014):.3f}/hour
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    if st.button("🚀 Create Kinesis Stream (Simulation)", use_container_width=True):
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Kinesis Stream Created Successfully!
        
        **Stream Details:**
        - **Name**: {stream_name}
        - **Shards**: {shard_count}
        - **Retention**: {retention_period}
        - **Capacity**: {shard_count} MB/sec, {shard_count * 1000} records/sec
        - **ARN**: arn:aws:kinesis:us-east-1:123456789:stream/{stream_name}
        
        🔗 **Consumers can now connect to this stream!**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Service Comparison
    st.markdown("## ⚖️ Kinesis Data Streams vs Kinesis Data Firehose")
    
    service_comparison = {
        'Feature': [
            'Processing Time', 'Stream Storage', 'Data Transformation', 
            'Data Compression', 'Data Producers', 'Data Consumers'
        ],
        'Kinesis Data Streams': [
            'As fast as 70ms', 
            'In shards, 24 hours to 365 days',
            'None (consumer responsibility)',
            'None',
            'KPL, AWS SDK, CloudWatch, IoT',
            'Lambda, Analytics, Firehose, Custom apps'
        ],
        'Kinesis Data Firehose': [
            '60-900 seconds',
            'Max 128MB buffer, 900 seconds',
            'AWS Lambda and AWS Glue',
            'gzip, Snappy, Zip',
            'Kinesis Agent, KPL, SDK, CloudWatch, IoT',
            'S3, Redshift, OpenSearch, Splunk, Analytics'
        ]
    }
    
    df_service_comparison = pd.DataFrame(service_comparison)
    st.dataframe(df_service_comparison, use_container_width=True)
    
    # Streaming Services Comparison Chart
    st.markdown("## 📊 AWS Streaming Services Radar Chart")
    st.plotly_chart(create_streaming_comparison_chart(), use_container_width=True)
    
    # Firehose Configuration Simulator
    st.markdown("## 🚀 Kinesis Data Firehose Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        firehose_name = st.text_input("Firehose Name:", "my-firehose-stream")
        source_type = st.selectbox("Source:", [
            "Direct PUT", "Kinesis Data Stream", "MSK", "Amazon MQ"
        ])
        destination = st.selectbox("Destination:", [
            "Amazon S3", "Amazon Redshift", "Amazon OpenSearch", "Splunk"
        ])
    
    with col2:
        buffer_size = st.slider("Buffer Size (MB):", 1, 128, 5)
        buffer_interval = st.slider("Buffer Interval (seconds):", 60, 900, 300)
        compression = st.selectbox("Compression:", ["None", "GZIP", "Snappy", "ZIP"])
    
    # Transform data option
    enable_transform = st.checkbox("Enable Data Transformation")
    if enable_transform:
        transform_type = st.selectbox("Transformation Type:", [
            "AWS Lambda Function", "Convert Record Format", "Both"
        ])
    
    if st.button("🚀 Create Firehose Stream (Simulation)", use_container_width=True):
        
        delivery_frequency = f"Every {buffer_interval} seconds or {buffer_size} MB"
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Kinesis Data Firehose Created Successfully!
        
        **Stream Details:**
        - **Name**: {firehose_name}
        - **Source**: {source_type}
        - **Destination**: {destination}
        - **Buffer**: {buffer_size} MB / {buffer_interval} seconds
        - **Compression**: {compression}
        - **Transformation**: {"Enabled" if enable_transform else "Disabled"}
        
        **Delivery**: {delivery_frequency}
        
        📊 **Your data will be automatically delivered to {destination}!**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Using Both Kinesis Services")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
import boto3
import json
from datetime import datetime
import time

# Initialize clients
kinesis_client = boto3.client('kinesis')
firehose_client = boto3.client('firehose')

# Example 1: Send data to Kinesis Data Streams
def send_to_kinesis_stream(stream_name, data):
    try:
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps({
                'timestamp': datetime.now().isoformat(),
                'data': data,
                'event_type': 'user_action'
            }),
            PartitionKey=str(data.get('user_id', 'anonymous'))
        )
        
        print(f"Record sent to stream: {response['ShardId']}")
        return response
        
    except Exception as e:
        print(f"Error sending to Kinesis Stream: {e}")
        return None

# Example 2: Send data to Kinesis Data Firehose  
def send_to_firehose(delivery_stream_name, data):
    try:
        response = firehose_client.put_record(
            DeliveryStreamName=delivery_stream_name,
            Record={
                'Data': json.dumps({
                    'timestamp': datetime.now().isoformat(),
                    'data': data,
                    'processed_by': 'firehose'
                }) + '\\n'  # Important: Add newline for S3
            }
        )
        
        print(f"Record sent to Firehose: {response['RecordId']}")
        return response
        
    except Exception as e:
        print(f"Error sending to Firehose: {e}")
        return None

# Example usage
sample_event = {
    'user_id': 'user_12345',
    'action': 'page_view',
    'page': '/products',
    'session_id': 'sess_67890'
}

# Send to Kinesis Data Streams for real-time processing
send_to_kinesis_stream('my-realtime-stream', sample_event)

# Send to Kinesis Data Firehose for data lake storage
send_to_firehose('my-s3-delivery-stream', sample_event)

# Batch processing example
def send_batch_to_kinesis(stream_name, records):
    batch_records = []
    
    for record in records:
        batch_records.append({
            'Data': json.dumps(record),
            'PartitionKey': str(record.get('user_id', 'default'))
        })
    
    response = kinesis_client.put_records(
        Records=batch_records,
        StreamName=stream_name
    )
    
    print(f"Sent {len(batch_records)} records in batch")
    return response

# Generate sample batch data
batch_data = [
    {'user_id': f'user_{i}', 'action': 'purchase', 'amount': i * 10}
    for i in range(1, 11)
]

send_batch_to_kinesis('my-realtime-stream', batch_data)
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def lambda_kinesis_integration_tab():
    """Content for Lambda-Kinesis integration and IoT monitoring tab"""
    st.markdown("# 🔧 AWS Lambda with Kinesis Integration")
    st.markdown("*Process streaming data with serverless functions*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Lambda-Kinesis Integration
    
    **AWS Lambda** can automatically process records from Kinesis Data Streams in real-time. 
    Think of Lambda as workers stationed along a conveyor belt (Kinesis stream), where each 
    worker processes items as they flow by.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Lambda-Kinesis Architecture
    st.markdown("## 🏗️ Lambda-Kinesis Processing Architecture")
    common.mermaid(create_lambda_kinesis_mermaid(), height=300)
    
    # Interactive Lambda Function Builder
    st.markdown("## 🛠️ Interactive Lambda Function Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        function_name = st.text_input("Lambda Function Name:", "kinesis-data-processor")
        runtime = st.selectbox("Runtime:", [
            "python3.9", "python3.8", "nodejs18.x", "java11", "go1.x"
        ])
        memory_size = st.selectbox("Memory (MB):", [128, 256, 512, 1024, 2048, 3008])
    
    with col2:
        batch_size = st.slider("Batch Size (records):", 1, 10000, 100)
        starting_position = st.selectbox("Starting Position:", [
            "TRIM_HORIZON", "LATEST", "AT_TIMESTAMP"
        ])
        parallel_factor = st.slider("Parallelization Factor:", 1, 10, 1)
    
    # Error handling configuration
    st.markdown("### 🛡️ Error Handling")
    col3, col4 = st.columns(2)
    
    with col3:
        max_retry_attempts = st.slider("Max Retry Attempts:", 0, 10, 3)
        max_record_age = st.slider("Maximum Record Age (hours):", 1, 168, 24)
    
    with col4:
        on_failure_destination = st.selectbox("On-Failure Destination:", [
            "None", "SQS Queue", "SNS Topic"
        ])
        bisect_batch_on_error = st.checkbox("Bisect Batch on Function Error", value=True)
    
    if st.button("🚀 Deploy Lambda Function (Simulation)", use_container_width=True):
        
        # Calculate processing capacity
        records_per_second = batch_size * parallel_factor / 5  # Assuming 5 second processing time
        cost_per_hour = (memory_size / 1024) * 0.0000166667 * 3600  # Lambda pricing
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Lambda Function Deployed Successfully!
        
        **Function Configuration:**
        - **Name**: {function_name}
        - **Runtime**: {runtime}
        - **Memory**: {memory_size} MB
        - **Batch Size**: {batch_size} records
        - **Parallelization**: {parallel_factor}x
        
        **Performance Estimates:**
        - **Processing Capacity**: ~{records_per_second:.0f} records/second
        - **Estimated Cost**: ${cost_per_hour:.6f}/hour (compute only)
        
        **Error Handling:**
        - **Retries**: {max_retry_attempts}
        - **Max Age**: {max_record_age} hours
        - **Failure Destination**: {on_failure_destination}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # IoT Monitoring Example
    st.markdown("## 📱 IoT Device Monitoring Example")
    
    # IoT Architecture
    iot_mermaid = """
    graph LR
        A[🏭 IoT Sensors] --> B[📡 AWS IoT Core]
        B --> C[🌊 Kinesis Data Streams]
        C --> D[🔧 Lambda Function]
        D --> E[📊 Process Data]
        E --> F[🗄️ DynamoDB]
        E --> G[📈 CloudWatch]
        E --> H[🚨 SNS Alerts]
        
        A --> A1[Temperature Sensors]
        A --> A2[Humidity Sensors] 
        A --> A3[Motion Detectors]
        
        E --> E1[Calculate Averages]
        E --> E2[Detect Anomalies]
        E --> E3[Generate Alerts]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style C fill:#4B9EDB,stroke:#232F3E,color:#fff
        style D fill:#3FB34F,stroke:#232F3E,color:#fff
        style F fill:#232F3E,stroke:#FF9900,color:#fff
    """
    
    common.mermaid(iot_mermaid, height=300)
    
    # IoT Simulation
    st.markdown("## 🎮 IoT Data Processing Simulation")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        num_sensors = st.slider("Number of IoT Sensors:", 10, 1000, 100)
        sensor_type = st.selectbox("Sensor Type:", [
            "🌡️ Temperature", "💧 Humidity", "🏃 Motion", "💡 Light"
        ])
    
    with col2:
        data_frequency = st.selectbox("Data Frequency:", [
            "Every second", "Every 5 seconds", "Every minute", "Every 5 minutes"
        ])
        
        # Calculate data volume
        freq_map = {"Every second": 1, "Every 5 seconds": 5, "Every minute": 60, "Every 5 minutes": 300}
        records_per_hour = num_sensors * (3600 / freq_map[data_frequency])
    
    with col3:
        alert_threshold = st.slider("Alert Threshold:", 1, 100, 25)
        processing_window = st.selectbox("Processing Window:", [
            "10 seconds", "1 minute", "5 minutes", "15 minutes"
        ])
    
    if st.button("📊 Start IoT Monitoring Simulation", use_container_width=True):
        
        # Generate simulated sensor data
        np.random.seed(42)  # For consistent results
        
        # Simulate sensor readings
        if "Temperature" in sensor_type:
            normal_range = (20, 25)
            unit = "°C"
        elif "Humidity" in sensor_type:
            normal_range = (40, 60)
            unit = "%"
        elif "Motion" in sensor_type:
            normal_range = (0, 1)
            unit = "detected"
        else:  # Light
            normal_range = (200, 800)
            unit = "lux"
        
        current_readings = np.random.normal(
            (normal_range[0] + normal_range[1]) / 2, 
            (normal_range[1] - normal_range[0]) / 6, 
            num_sensors
        )
        
        # Identify anomalies
        anomalies = np.sum((current_readings < normal_range[0]) | (current_readings > normal_range[1]))
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 📊 IoT Monitoring Simulation Results
        
        **System Configuration:**
        - **Sensors**: {num_sensors} {sensor_type} sensors
        - **Frequency**: {data_frequency}
        - **Data Volume**: {records_per_hour:,.0f} records/hour
        
        **Current Status:**
        - **Average Reading**: {np.mean(current_readings):.2f} {unit}
        - **Normal Range**: {normal_range[0]}-{normal_range[1]} {unit}
        - **Anomalies Detected**: {anomalies} sensors
        - **System Health**: {"🟢 Normal" if anomalies <= alert_threshold else "🔴 Alert"}
        
        **Actions Taken:**
        - Stored {num_sensors} readings in DynamoDB
        - Sent {anomalies} alerts via SNS
        - Updated CloudWatch metrics
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Create a simple visualization
        fig = go.Figure()
        
        # Normal readings
        normal_readings = current_readings[
            (current_readings >= normal_range[0]) & (current_readings <= normal_range[1])
        ]
        anomaly_readings = current_readings[
            (current_readings < normal_range[0]) | (current_readings > normal_range[1])
        ]
        
        fig.add_trace(go.Scatter(
            y=normal_readings,
            mode='markers',
            name='Normal Readings',
            marker=dict(color=AWS_COLORS['success'], size=8)
        ))
        
        if len(anomaly_readings) > 0:
            fig.add_trace(go.Scatter(
                y=anomaly_readings,
                mode='markers', 
                name='Anomalies',
                marker=dict(color=AWS_COLORS['warning'], size=10, symbol='x')
            ))
        
        fig.update_layout(
            title=f"{sensor_type} Sensor Readings",
            yaxis_title=f"Reading ({unit})",
            xaxis_title="Sensor Index"
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Stream Processing Architecture Example
    st.markdown("## 🌊 Complete Stream Processing Architecture")
    
    stream_arch_mermaid = """
    graph TD
        A[📊 Raw Sensor Data] --> B[🌊 Streaming Collection]
        B --> C[⚙️ Stream Processing Layer]
        C --> D[🗄️ Serving Layer]
        D --> E[📊 Data Queries]
        D --> F[📈 Data Visualization]
        
        B --> B1[Amazon Data Firehose]
        C --> C1[Amazon Kinesis Data Analytics]
        D --> D1[Amazon S3]
        
        B1 --> |Raw Data| D1
        C1 --> |Filtered Data| B2[Amazon Data Firehose]
        B2 --> D1
        
        E --> E1[Amazon Athena]
        F --> F1[Amazon QuickSight]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#232F3E,stroke:#FF9900,color:#fff
    """
    
    common.mermaid(stream_arch_mermaid, height=350)
    
    # Resharding Example
    st.markdown("## 🔄 Kinesis Stream Resharding")
    
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 When to Reshard
    
    **Resharding** allows you to increase or decrease the number of shards in your stream to adapt to 
    changing data flow rates:
    
    - **Shard Split**: Divide one shard into two (increase capacity)
    - **Shard Merge**: Combine two shards into one (reduce cost)
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # DynamoDB Streams
    st.markdown("## 🗄️ Amazon DynamoDB Streams")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 DynamoDB Streams Features
        
        - **Change Capture**: Captures data modification events
        - **Ordering**: Maintains order of changes
        - **Durability**: 24-hour retention
        - **Integration**: Native Lambda integration
        
        **Stream Records Include:**
        - Keys of modified item
        - Before and after images
        - Type of modification (INSERT, MODIFY, REMOVE)
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 Common Use Cases
        
        - **Replication**: Cross-region replication
        - **Analytics**: Real-time analytics on changes
        - **Auditing**: Change tracking and compliance
        - **Notifications**: Send alerts on data changes
        - **Materialized Views**: Update derived tables
        - **Search Indexing**: Update search indexes
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Lambda Function for Kinesis")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
import json
import boto3
from datetime import datetime

# Initialize AWS services
dynamodb = boto3.resource('dynamodb')
cloudwatch = boto3.client('cloudwatch')
sns = boto3.client('sns')

def lambda_handler(event, context):
    '''
    Process Kinesis Data Stream records
    '''
    
    # Process each record in the batch
    processed_records = 0
    anomalies_detected = 0
    
    for record in event['Records']:
        # Decode the data
        payload = json.loads(
            boto3.client('kinesis').decode(record['kinesis']['data'])
        )
        
        # Process IoT sensor data
        sensor_id = payload.get('sensor_id')
        temperature = payload.get('temperature')
        timestamp = payload.get('timestamp')
        
        # Store in DynamoDB
        table = dynamodb.Table('sensor-readings')
        table.put_item(
            Item={
                'sensor_id': sensor_id,
                'timestamp': timestamp,
                'temperature': temperature,
                'processed_at': datetime.now().isoformat()
            }
        )
        
        # Check for anomalies
        if temperature > 30 or temperature < 10:
            anomalies_detected += 1
            
            # Send alert
            sns.publish(
                TopicArn='arn:aws:sns:us-east-1:123456789:temperature-alerts',
                Message=f'Temperature anomaly detected: {temperature}°C on sensor {sensor_id}',
                Subject='IoT Temperature Alert'
            )
        
        # Send metrics to CloudWatch
        cloudwatch.put_metric_data(
            Namespace='IoT/Sensors',
            MetricData=[
                {
                    'MetricName': 'Temperature',
                    'Dimensions': [
                        {
                            'Name': 'SensorId',
                            'Value': str(sensor_id)
                        }
                    ],
                    'Value': temperature,
                    'Unit': 'Count',
                    'Timestamp': datetime.now()
                }
            ]
        )
        
        processed_records += 1
    
    # Return processing summary
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed_records': processed_records,
            'anomalies_detected': anomalies_detected,
            'timestamp': datetime.now().isoformat()
        })
    }

# DynamoDB Streams processor function
def dynamodb_stream_handler(event, context):
    '''
    Process DynamoDB Stream records
    '''
    
    for record in event['Records']:
        event_name = record['eventName']  # INSERT, MODIFY, REMOVE
        
        if event_name == 'INSERT':
            # New item added
            new_image = record['dynamodb']['NewImage']
            print(f"New item added: {new_image}")
            
        elif event_name == 'MODIFY':
            # Item updated
            old_image = record['dynamodb']['OldImage']
            new_image = record['dynamodb']['NewImage']
            print(f"Item updated from {old_image} to {new_image}")
            
        elif event_name == 'REMOVE':
            # Item deleted
            old_image = record['dynamodb']['OldImage']
            print(f"Item deleted: {old_image}")
    
    return {'statusCode': 200}
    """, language='python')
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
    # 📊 AWS Data Ingestion
    ### Learn to collect, process, and analyze streaming data on AWS
    """)
    
    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Data Ingestion Fundamentals", 
        "🏗️ AWS Analytics Overview",
        "⚡ Batch & Stream Processing", 
        "🌊 Kinesis Services & Lambda Integration"
    ])
    
    with tab1:
        data_ingestion_fundamentals_tab()
    
    with tab2:
        aws_analytics_overview_tab()
    
    with tab3:
        batch_stream_processing_tab()
        
    with tab4:
        kinesis_services_tab()
        st.markdown("---")
        lambda_kinesis_integration_tab()
    
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
