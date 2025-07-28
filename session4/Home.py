
import streamlit as st
import plotly.graph_objects as go
import utils.common as common
import utils.authenticate as authenticate


# Page configuration
st.set_page_config(
    page_title="Data Engineer Associate - Session 4",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded"
)
    

common.initialize_session_state()

with st.sidebar:
    common.render_sidebar()

def main():

    # Custom CSS for AWS styling
    st.markdown("""
    <style>
    /* AWS Color Scheme */
    :root {
        --aws-orange: #FF9900;
        --aws-blue: #232F3E;
        --aws-light-blue: #4B9CD3;
        --aws-gray: #879196;
        --aws-white: #FFFFFF;
    }
    
    /* Main container styling */
    .main-header {
        background: linear-gradient(135deg, #232F3E 0%, #4B9CD3 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 1rem;
    }
    
    .program-card {
        background: white;
        padding: 1.5rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #FF9900;
        margin-bottom: 1rem;
    }
    
    .roadmap-item {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        border: 1px solid #e9ecef;
        transition: all 0.3s ease;
    }
    
    .roadmap-item:hover {
        background: #e3f2fd;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
    }
    
    .current-session {
        background: linear-gradient(135deg, #FF9900 0%, #FFB84D 100%);
        color: black;
        font-weight: bold;
    }
    
    .learning-outcome {
        background: #e8f5e8;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #4caf50;
        margin: 0.5rem 0;
    }
    
    .training-item {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #ddd;
        margin: 0.5rem 0;
        display: flex;
        align-items: center;
    }
    
    .training-icon {
        color: #FF9900;
        font-size: 1.5rem;
        margin-right: 1rem;
    }
    
    .service-card {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 1.5rem;
        border-radius: 8px;
        border-left: 4px solid #4B9CD3;
        margin-bottom: 1rem;
    }
    
    .emr-feature {
        background: #fff3cd;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #ffc107;
        margin: 0.5rem 0;
    }
    
    .monitoring-card {
        background: #d1ecf1;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #17a2b8;
        margin: 0.5rem 0;
    }
    
    .comparison-table {
        background: white;
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 1rem 0;
    }
    
    .code-example {
        background: #2d3748;
        color: #e2e8f0;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
        font-family: 'Courier New', monospace;
        border-left: 4px solid #FF9900;
    }
    
    .footer {
        text-align: center;
        padding: 1rem;
        background-color: #232F3E;
        color: white;
        margin-top: 1rem;
        border-radius: 8px;
    }
    
    /* Responsive design */
    @media (max-width: 768px) {
        .main-header {
            padding: 1rem;
        }
        .program-card {
            padding: 1rem;
        }
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Main header
    st.markdown("""
    <div class="main-header">
        <h1>AWS Partner Certification Readiness</h1>
        <h2>Data Engineer - Associate</h2>
        <h3>Content Review Session 4: Domain 3 - Data Operations and Support</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Program Check-in section
    st.markdown("## 📋 Program Check-in")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>Welcome to Session 4!</h4>
            <p>We're in the final stretch! This session focuses on Data Operations and Support - 
            automating data processing, analyzing data, maintaining and monitoring pipelines, 
            and ensuring data quality. You'll learn about EMR, orchestration, analytics services, 
            and monitoring strategies.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Progress indicator
        progress_value = 80  # 4th session out of 5
        st.metric("Program Progress", f"{progress_value}%", "Session 4 of 5")
        st.progress(progress_value / 100)
    
    # Program Roadmap
    st.markdown("## 🗺️ Program Roadmap")
    
    # Your Learning Journey - Mermaid Diagram
    st.markdown("### Your Learning Journey")
    
    mermaid_code = """
    graph LR
        A[✅ Session 1<br/>AWS & Data<br/>Fundamentals] --> B[✅ Session 2<br/>Data Ingestion &<br/>Transformation]
        B --> C[✅ Session 3<br/>Data Store<br/>Management]
        C --> D[🎯 Session 4<br/>Data Operations<br/>& Support]
        D --> E[📅 Session 5<br/>Data Security &<br/>Governance]
        E --> F[🏆 Certification<br/>Exam Ready]
        
        classDef completed fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef current fill:#FF9900,stroke:#232F3E,stroke-width:3px,color:#fff
        classDef upcoming fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef target fill:#6f42c1,stroke:#232F3E,stroke-width:2px,color:#fff
        
        class A,B,C completed
        class D current
        class E upcoming
        class F target
    """
    
    common.mermaid(mermaid_code, height=150, show_controls=False)
    
    # Roadmap details
    roadmap_data = [
        {
            "session": "Content Review 1",
            "topic": "AWS & Data Fundamentals",
            "status": "completed",
            "description": "✅ Core AWS services, compute, networking, storage, and data fundamentals"
        },
        {
            "session": "Content Review 2", 
            "topic": "Domain 1: Data Ingestion and Transformation",
            "status": "completed",
            "description": "✅ Data ingestion patterns, transformation techniques, and processing frameworks"
        },
        {
            "session": "Content Review 3",
            "topic": "Domain 2: Data Store Management", 
            "status": "completed",
            "description": "✅ Data warehouses, data lakes, catalogs, and lifecycle management"
        },
        {
            "session": "Content Review 4",
            "topic": "Domain 3: Data Operations and Support",
            "status": "current", 
            "description": "🎯 Automation, analytics, monitoring, and data quality assurance"
        },
        {
            "session": "Content Review 5",
            "topic": "Domain 4: Data Security and Governance",
            "status": "upcoming",
            "description": "📅 Security controls, compliance, and data governance frameworks"
        }
    ]
    
    for i, item in enumerate(roadmap_data):
        if item['status'] == 'current':
            status_class = "current-session"
            status_icon = "🎯"
        elif item['status'] == 'completed':
            status_class = "roadmap-item"
            status_icon = "✅"
        else:
            status_class = "roadmap-item"
            status_icon = "📅"
        
        st.markdown(f"""
        <div class="roadmap-item {status_class}">
            <h4>{status_icon} {item['session']}: {item['topic']}</h4>
            <p>{item['description']}</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Today's Learning Outcomes
    st.markdown("## 🎯 Today's Learning Outcomes")
    st.markdown("**During this session, we will cover:**")
    
    learning_outcomes = [
        "Task Statement 3.1: Automate data processing by using AWS services",
        "Task Statement 3.2: Analyze data by using AWS services", 
        "Task Statement 3.3: Maintain and monitor data pipelines",
        "Task Statement 3.4: Ensure data quality"
    ]
    
    for i, outcome in enumerate(learning_outcomes):
        st.markdown(f"""
        <div class="learning-outcome">
            <strong>✅ {outcome}</strong>
        </div>
        """, unsafe_allow_html=True)
    
    # Task Statement 3.1: Automate data processing
    st.markdown("## 🤖 Task Statement 3.1: Automate Data Processing")
    
    # Amazon EMR Deep Dive
    st.markdown("### 🐘 Amazon Elastic MapReduce (EMR)")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 What is Amazon EMR?</h4>
            <p>Amazon EMR is a cloud big data platform for processing vast amounts of data using 
            open source tools such as Apache Spark, Hadoop, HBase, Flink, Hudi, and Presto. 
            EMR makes it easy to set up, operate, and scale your big data environments.</p>
            <h5>Key Benefits:</h5>
            <ul>
                <li><strong>Easy to use:</strong> Launch clusters in minutes</li>
                <li><strong>Cost effective:</strong> Pay only for what you use</li>
                <li><strong>Flexible:</strong> Multiple deployment options</li>
                <li><strong>Reliable:</strong> Automatic failure handling</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # EMR cluster architecture diagram
        st.markdown("### EMR Cluster Architecture")
        emr_mermaid = """
        graph TD
            A[Primary Node<br/>YARN ResourceManager<br/>HDFS NameNode] --> B[Core Node 1<br/>YARN NodeManager<br/>HDFS DataNode]
            A --> C[Core Node 2<br/>YARN NodeManager<br/>HDFS DataNode]
            A --> D[Task Node 1<br/>Processing Only]
            A --> E[Task Node 2<br/>Processing Only]
            
            classDef primary fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef core fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef task fill:#28a745,stroke:#232F3E,stroke-width:1px,color:#fff
            
            class A primary
            class B,C core
            class D,E task
        """
        common.mermaid(emr_mermaid, height=300, show_controls=False)
    
    # EMR Node Types
    st.markdown("### 🔧 EMR Node Types and Functions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="emr-feature">
            <h5>🎛️ Primary Node</h5>
            <ul>
                <li>YARN ResourceManager</li>
                <li>HDFS NameNode</li>
                <li>Coordinates cluster resources</li>
                <li>Manages job scheduling</li>
                <li>Single point of coordination</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="emr-feature">
            <h5>💾 Core Nodes</h5>
            <ul>
                <li>YARN NodeManager</li>
                <li>HDFS DataNode</li>
                <li>Run tasks and store data</li>
                <li>HDFS storage for durability</li>
                <li>Essential for cluster operation</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="emr-feature">
            <h5>⚡ Task Nodes</h5>
            <ul>
                <li>Processing tasks only</li>
                <li>No HDFS storage</li>
                <li>Temporary storage for jobs</li>
                <li>Optional for scaling</li>
                <li>Cost-effective with Spot</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # EMR Storage Options
    st.markdown("### 🗄️ EMR Data Storage Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="service-card">
            <h5>📂 HDFS (Hadoop Distributed File System)</h5>
            <ul>
                <li><strong>Local Storage:</strong> Uses EBS volumes on cluster nodes</li>
                <li><strong>Replication:</strong> Data replicated 3x by default</li>
                <li><strong>Performance:</strong> Low latency access</li>
                <li><strong>Ephemeral:</strong> Data lost when cluster terminates</li>
                <li><strong>Use Case:</strong> Iterative processing, temporary data</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="service-card">
            <h5>☁️ EMRFS (EMR File System)</h5>
            <ul>
                <li><strong>S3 Integration:</strong> Direct read/write to S3</li>
                <li><strong>Durability:</strong> 99.999999999% (11 9's)</li>
                <li><strong>Scalability:</strong> Virtually unlimited storage</li>
                <li><strong>Cost-effective:</strong> Pay for storage used</li>
                <li><strong>Use Case:</strong> Persistent data, data lakes</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Instance Configuration
    st.markdown("### ⚙️ EMR Instance Configuration Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>📦 Instance Groups (Default)</h4>
            <ul>
                <li>Single EC2 instance type per group</li>
                <li>Same purchasing option (On-Demand/Spot)</li>
                <li>Manual or automatic scaling</li>
                <li>Up to 48 task instance groups</li>
                <li>Simpler configuration</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="program-card">
            <h4>🚢 Instance Fleets</h4>
            <ul>
                <li>Up to 30 EC2 instance types per fleet</li>
                <li>Mixed purchasing options</li>
                <li>Target capacity-based</li>
                <li>Allocation strategies available</li>
                <li>Better Spot Instance management</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # EMR vs Glue Comparison
    st.markdown("### 🆚 Spark on Amazon EMR vs AWS Glue ETL")
    
    comparison_data = {
        "Consideration": ["Responsibility model", "Degree of control", "Data model", "Scalability", "Pricing"],
        "Spark on Amazon EMR": ["Fully managed", "More flexibility", "Schema before load", "Policy-based or managed scaling", "Stable cost for consistent workloads"],
        "AWS Glue ETL": ["Serverless", "More automation", "Schema on read", "Specify max workers and concurrency", "Pay for use based on needs"]
    }
    
    st.markdown("""
    <div class="comparison-table">
        <table style="width:100%; border-collapse: collapse;">
            <thead style="background-color: #232F3E; color: white;">
                <tr>
                    <th style="padding: 12px; text-align: left;">Consideration</th>
                    <th style="padding: 12px; text-align: left;">Spark on Amazon EMR</th>
                    <th style="padding: 12px; text-align: left;">AWS Glue ETL</th>
                </tr>
            </thead>
            <tbody>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;"><strong>Responsibility model</strong></td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Fully managed</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Serverless</td>
                </tr>
                <tr>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;"><strong>Degree of control</strong></td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">More flexibility</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">More automation</td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;"><strong>Data model</strong></td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Schema before load</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Schema on read</td>
                </tr>
                <tr>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;"><strong>Scalability</strong></td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Policy-based or managed scaling</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Specify max workers and concurrency</td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 12px;"><strong>Pricing</strong></td>
                    <td style="padding: 12px;">Stable cost for consistent workloads</td>
                    <td style="padding: 12px;">Pay for use based on needs</td>
                </tr>
            </tbody>
        </table>
    </div>
    """, unsafe_allow_html=True)
    
    # Event-Driven Architecture
    st.markdown("### 🔄 Amazon EventBridge for Event-Driven Processing")
    
    st.markdown("""
    <div class="program-card">
        <h4>🌟 What is Amazon EventBridge?</h4>
        <p>EventBridge is a serverless service for building event-driven, loosely-coupled applications 
        by routing events between sources and targets. Event buses receive events from many sources 
        and deliver to multiple targets, with optional event transformation.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # EventBridge Architecture
    eventbridge_mermaid = """
    graph LR
        A[Event Sources] --> B[EventBridge]
        B --> C[Event Rules]
        C --> D[Target Services]
        
        E[S3 Events] --> B
        F[Schedule/Cron] --> B
        G[Custom Apps] --> B
        
        C --> H[Lambda Functions]
        C --> I[Step Functions]
        C --> J[SQS/SNS]
        C --> K[Kinesis]
        
        classDef source fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef service fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef target fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
        
        class A,E,F,G source
        class B,C service
        class D,H,I,J,K target
    """
    
    common.mermaid(eventbridge_mermaid, height=500, show_controls=False)
    
    # Redshift Data API
    st.markdown("### 🔌 Amazon Redshift Data API")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 Redshift Data API Benefits</h4>
            <p>The Amazon Redshift Data API enables you to interact with Amazon Redshift without 
            managing persistent connections. Perfect for serverless applications and event-driven architectures.</p>
            <h5>Key Features:</h5>
            <ul>
                <li><strong>No persistent connections</strong> - HTTP-based API calls</li>
                <li><strong>Secure</strong> - Uses AWS IAM or Secrets Manager</li>
                <li><strong>Asynchronous</strong> - Non-blocking query execution</li>
                <li><strong>Integrated</strong> - Works with Lambda, SageMaker, Cloud9</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Data API Architecture")
        data_api_mermaid = """
        graph TD
            A[Lambda Function] --> B[Redshift Data API]
            C[SageMaker] --> B
            D[EventBridge] --> A
            B --> E[Amazon Redshift]
            E --> F[Query Results]
            B --> G[S3 Bucket]
            
            classDef compute fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef api fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef storage fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
            
            class A,C,D compute
            class B,E api
            class F,G storage
        """
        common.mermaid(data_api_mermaid, height=300, show_controls=False)
    
    # Code Example
    st.markdown("### 💻 Redshift Data API Code Examples")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Using the COPY command with Data API:**")
        st.markdown("""
        <div class="code-example">
import boto3

client = boto3.client('redshift-data')

response = client.execute_statement(
    ClusterIdentifier='my-cluster',
    Database='dev',
    DbUser='awsuser',
    Sql='''
    COPY sales FROM 's3://mybucket/data/'
    IAM_ROLE 'arn:aws:iam::account:role/RedshiftRole'
    FORMAT AS PARQUET;
    '''
)

query_id = response['Id']
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("**Running SQL queries with Data API:**")
        st.markdown("""
        <div class="code-example">
import boto3

client = boto3.client('redshift-data')

# Execute query
response = client.execute_statement(
    ClusterIdentifier='my-cluster',
    Database='dev',
    DbUser='awsuser',
    Sql='SELECT COUNT(*) FROM sales'
)

# Get results
result = client.get_statement_result(
    Id=response['Id']
)
        </div>
        """, unsafe_allow_html=True)
    
    # Task Statement 3.2: Analyze data
    st.markdown("## 📊 Task Statement 3.2: Analyze Data by Using AWS Services")
    
    # Amazon Athena
    st.markdown("### 🔍 Amazon Athena - Serverless Query Engine")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 What is Amazon Athena?</h4>
            <p>Amazon Athena is an interactive query service that makes it easy to analyze data 
            directly in Amazon S3 using standard SQL. No need to load data - query it where it sits!</p>
            <h5>Key Benefits:</h5>
            <ul>
                <li><strong>Serverless:</strong> No infrastructure to manage</li>
                <li><strong>Pay per query:</strong> Only pay for queries you run</li>
                <li><strong>Fast:</strong> Get results in seconds</li>
                <li><strong>Standard SQL:</strong> Based on Presto engine</li>
                <li><strong>Integrated:</strong> Works with AWS Glue Data Catalog</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Supported File Formats")
        st.markdown("""
        <div class="service-card">
            <ul>
                <li>📄 <strong>CSV</strong> - Comma-separated values</li>
                <li>🔧 <strong>JSON</strong> - JavaScript Object Notation</li>
                <li>🗜️ <strong>ORC</strong> - Optimized Row Columnar</li>
                <li>📋 <strong>Avro</strong> - Data serialization system</li>
                <li>📊 <strong>Parquet</strong> - Columnar storage (recommended)</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Athena Federated Query
    st.markdown("### 🌐 Athena Federated Query")
    
    st.markdown("""
    <div class="program-card">
        <h4>🔗 Query Across Multiple Data Sources</h4>
        <p>Athena Federated Query enables you to run SQL queries across data stored in different 
        data sources using Lambda-based connectors. Query relational databases, NoSQL databases, 
        and custom data sources with familiar SQL.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Federated Query Architecture
    federated_mermaid = """
    graph TB
        A[Athena Console] --> B[Athena Query Engine]
        B --> C[Lambda Connectors]
        
        C --> D[DynamoDB Connector]
        C --> E[RDS Connector]
        C --> F[DocumentDB Connector]
        C --> G[Custom Connector]
        
        D --> H[DynamoDB Tables]
        E --> I[RDS Databases]
        F --> J[DocumentDB Collections]
        G --> K[Proprietary Data Sources]
        
        classDef athena fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef connector fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef datasource fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
        
        class A,B athena
        class C,D,E,F,G connector
        class H,I,J,K datasource
    """
    
    common.mermaid(federated_mermaid, height=600, show_controls=False)
    
    # Athena Workgroups
    st.markdown("### 👥 Athena Workgroups")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="service-card">
            <h5>🎯 Workgroup Benefits</h5>
            <ul>
                <li><strong>Isolate workloads</strong> - Separate teams and applications</li>
                <li><strong>Control access</strong> - IAM-based permissions</li>
                <li><strong>Manage costs</strong> - Set query limits and budgets</li>
                <li><strong>Track usage</strong> - Monitor query metrics</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="service-card">
            <h5>⚙️ Workgroup Features</h5>
            <ul>
                <li><strong>Query history</strong> - Separate logs per workgroup</li>
                <li><strong>Result location</strong> - Configure S3 output location</li>
                <li><strong>Data limits</strong> - Set per-query scan limits</li>
                <li><strong>CloudWatch metrics</strong> - Custom dashboards and alarms</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Amazon QuickSight
    st.markdown("### 📈 Amazon QuickSight - Business Intelligence Service")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 What is Amazon QuickSight?</h4>
            <p>Amazon QuickSight is a cloud-scale business intelligence service that delivers fast, 
            responsive insights with machine learning-powered features. Create interactive dashboards 
            and embed analytics into your applications.</p>
            <h5>Key Features:</h5>
            <ul>
                <li><strong>Serverless:</strong> Scales automatically to thousands of users</li>
                <li><strong>SPICE engine:</strong> Super-fast, in-memory calculations</li>
                <li><strong>ML insights:</strong> Anomaly detection and forecasting</li>
                <li><strong>Natural language:</strong> Ask questions with QuickSight Q</li>
                <li><strong>Embeddable:</strong> Integrate dashboards into applications</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### SPICE Engine Benefits")
        st.markdown("""
        <div class="service-card">
            <h5>⚡ SPICE (Super-fast, Parallel, In-memory Calculation Engine)</h5>
            <ul>
                <li><strong>Faster processing</strong> - In-memory calculations</li>
                <li><strong>Reduced wait time</strong> - vs. direct queries</li>
                <li><strong>Cost savings</strong> - Reuse imported data</li>
                <li><strong>Scalability</strong> - Handles large datasets efficiently</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # QuickSight Data Flow
    quicksight_mermaid = """
    graph LR
        A[Data Sources] --> B[QuickSight]
        B --> C[SPICE Engine]
        B --> D[Direct Query]
        C --> E[Dashboards]
        D --> E
        E --> F[Embedded Analytics]
        
        G[S3] --> A
        H[Athena] --> A
        I[Redshift] --> A
        J[RDS] --> A
        
        classDef source fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef service fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef output fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
        
        class A,G,H,I,J source
        class B,C,D service
        class E,F output
    """
    
    common.mermaid(quicksight_mermaid, height=400, show_controls=False)
    
    # Task Statement 3.3: Maintain and monitor data pipelines
    st.markdown("## 🔧 Task Statement 3.3: Maintain and Monitor Data Pipelines")
    
    # Data Pipeline Orchestration
    st.markdown("### 🎭 Data Pipeline Orchestration Options")
    
    pipeline_comparison = """
    <div class="comparison-table">
        <table style="width:100%; border-collapse: collapse;">
            <thead style="background-color: #232F3E; color: white;">
                <tr>
                    <th style="padding: 12px; text-align: left;">Factor</th>
                    <th style="padding: 12px; text-align: left;">AWS Glue Workflow</th>
                    <th style="padding: 12px; text-align: left;">AWS Step Functions</th>
                    <th style="padding: 12px; text-align: left;">Amazon MWAA</th>
                </tr>
            </thead>
            <tbody>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;"><strong>Use case</strong></td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Mostly AWS Glue jobs and crawlers</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Integration with different services</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Reuse existing Airflow assets</td>
                </tr>
                <tr>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;"><strong>Infrastructure</strong></td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Serverless</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Serverless</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Managed service</td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 12px;"><strong>Building pipelines</strong></td>
                    <td style="padding: 12px;">Glue jobs (Python/Scala) and crawlers</td>
                    <td style="padding: 12px;">Visual console + Lambda integration</td>
                    <td style="padding: 12px;">DAGs defined in Python files</td>
                </tr>
            </tbody>
        </table>
    </div>
    """
    st.markdown(pipeline_comparison, unsafe_allow_html=True)
    
    # AWS Glue Workflow
    st.markdown("### 🕷️ AWS Glue Workflow")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 Glue Workflow Features</h4>
            <p>Create and visualize complex ETL workflows in AWS Glue with built-in monitoring 
            and dependency management.</p>
            <h5>Workflow Triggers:</h5>
            <ul>
                <li><strong>Schedules:</strong> Cron-based execution</li>
                <li><strong>On-demand:</strong> Manual triggering</li>
                <li><strong>EventBridge events:</strong> Event-driven execution</li>
            </ul>
            <h5>Monitoring:</h5>
            <ul>
                <li>Visual graph of execution progress</li>
                <li>Component dependencies visualization</li>
                <li>Real-time status tracking</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Glue Workflow Components")
        glue_workflow_mermaid = """
        graph TD
            A[Trigger] --> B[Crawler 1]
            A --> C[Job 1]
            B --> D[Job 2]
            C --> D
            D --> E[Job 3]
            E --> F[Crawler 2]
            
            classDef trigger fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef crawler fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef job fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
            
            class A trigger
            class B,F crawler
            class C,D,E job
        """
        common.mermaid(glue_workflow_mermaid, height=600, show_controls=False)
    
    # Monitoring and Observability
    st.markdown("### 👁️ Monitoring Data Lakes and Pipelines")
    
    st.markdown("""
    <div class="program-card">
        <h4>🎯 Comprehensive Monitoring Strategy</h4>
        <p>Effective data lake monitoring requires observability across the entire pipeline - 
        from ingestion through processing to consumption. Use AWS services to create automated 
        monitoring and alerting systems.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="monitoring-card">
            <h5>📊 Amazon CloudWatch</h5>
            <ul>
                <li><strong>Metrics:</strong> Performance and utilization</li>
                <li><strong>Logs:</strong> Application and system logs</li>
                <li><strong>Alarms:</strong> Threshold-based notifications</li>
                <li><strong>Dashboards:</strong> Visual monitoring</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="monitoring-card">
            <h5>🔍 AWS CloudTrail</h5>
            <ul>
                <li><strong>API calls:</strong> Track all AWS API usage</li>
                <li><strong>Audit logs:</strong> Security and compliance</li>
                <li><strong>Event history:</strong> 90-day retention</li>
                <li><strong>S3 delivery:</strong> Long-term storage</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="monitoring-card">
            <h5>⚡ CloudWatch Events/EventBridge</h5>
            <ul>
                <li><strong>Real-time events:</strong> Near real-time delivery</li>
                <li><strong>Event routing:</strong> Target multiple services</li>
                <li><strong>Custom rules:</strong> Pattern matching</li>
                <li><strong>Automated actions:</strong> Event-driven responses</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Monitoring Architecture
    monitoring_mermaid = """
    graph TB
        A[Data Sources] --> B[Ingestion Layer]
        B --> C[Processing Layer]
        C --> D[Storage Layer]
        D --> E[Analytics Layer]
        
        F[CloudWatch] --> G[Metrics & Logs]
        H[CloudTrail] --> I[API Audit Logs]
        J[EventBridge] --> K[Event Rules & Actions]
        
        B --> F
        C --> F
        D --> F
        E --> F
        
        F --> L[Alarms & Notifications]
        
        classDef data fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef monitoring fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef output fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
        
        class A,B,C,D,E data
        class F,G,H,I,J,K monitoring
        class L output
    """
    
    st.markdown("### Monitoring Architecture Overview")
    common.mermaid(monitoring_mermaid, height=700, show_controls=False)
    
    # Task Statement 3.4: Ensure data quality
    st.markdown("## ✅ Task Statement 3.4: Ensure Data Quality")
    
    # AWS Glue DataBrew
    st.markdown("### 🧪 AWS Glue DataBrew - Visual Data Preparation")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 What is AWS Glue DataBrew?</h4>
            <p>AWS Glue DataBrew is a visual data preparation tool that simplifies cleaning, 
            transforming, and preparing data for analysis and machine learning. Create data 
            transformation recipes without writing code.</p>
            <h5>Key Features:</h5>
            <ul>
                <li><strong>Visual interface:</strong> Point-and-click data preparation</li>
                <li><strong>250+ transformations:</strong> Built-in data cleaning functions</li>
                <li><strong>Data profiling:</strong> Automatic data quality assessment</li>
                <li><strong>Recipe sharing:</strong> Reusable transformation templates</li>
                <li><strong>Scale processing:</strong> Handle datasets of any size</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### DataBrew Workflow")
        databrew_mermaid = """
        graph TD
            A[Data Sources] --> B[DataBrew Project]
            B --> C[Data Profiling]
            C --> D[Recipe Creation]
            D --> E[Data Transformation]
            E --> F[Quality Validation]
            F --> G[Output Data]
            
            classDef source fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef process fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef output fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
            
            class A source
            class B,C,D,E,F process
            class G output
        """
        common.mermaid(databrew_mermaid, height=700, show_controls=False)
    
    # DataBrew vs Glue Studio
    st.markdown("### 🆚 AWS Glue DataBrew vs AWS Glue Studio")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="service-card">
            <h5>🧪 AWS Glue DataBrew</h5>
            <ul>
                <li><strong>Purpose:</strong> Visual data preparation and cleaning</li>
                <li><strong>Target users:</strong> Data analysts, non-coders</li>
                <li><strong>Interface:</strong> Recipe-based, drag-and-drop</li>
                <li><strong>Focus:</strong> Data quality and preparation</li>
                <li><strong>Profiling:</strong> Built-in data quality assessment</li>
                <li><strong>Recipes:</strong> Reusable transformation templates</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="service-card">
            <h5>🏗️ AWS Glue Studio</h5>
            <ul>
                <li><strong>Purpose:</strong> Visual ETL job authoring</li>
                <li><strong>Target users:</strong> Data engineers, developers</li>
                <li><strong>Interface:</strong> Visual DAG builder</li>
                <li><strong>Focus:</strong> Complete ETL pipeline creation</li>
                <li><strong>Code generation:</strong> PySpark/Scala output</li>
                <li><strong>Monitoring:</strong> Job execution tracking</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Amazon SageMaker Data Wrangler
    st.markdown("### 🔬 Amazon SageMaker Data Wrangler")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 SageMaker Data Wrangler for ML</h4>
            <p>Amazon SageMaker Data Wrangler is designed specifically for machine learning data 
            preparation. It provides advanced features for feature engineering and ML-focused 
            data transformations.</p>
            <h5>ML-Focused Features:</h5>
            <ul>
                <li><strong>Feature engineering:</strong> Advanced encoding options</li>
                <li><strong>Data visualization:</strong> ML-specific charts and insights</li>
                <li><strong>Quick modeling:</strong> Test models on prepared data</li>
                <li><strong>SageMaker integration:</strong> Seamless ML workflow</li>
                <li><strong>Custom code:</strong> Bring your own transformations</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Data Wrangler vs DataBrew")
        st.markdown("""
        <div class="service-card">
            <h5>Key Differences</h5>
            <ul>
                <li><strong>Data Wrangler:</strong> ML-focused with SageMaker integration</li>
                <li><strong>DataBrew:</strong> General ETL and data preparation</li>
                <li><strong>Encoding options:</strong> More in Data Wrangler</li>
                <li><strong>Custom code:</strong> Supported in Data Wrangler</li>
                <li><strong>Target use case:</strong> ML vs general analytics</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Data Quality Framework
    st.markdown("### 📋 Data Quality Framework")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 Data Quality Dimensions</h4>
            <ul>
                <li><strong>Completeness:</strong> No missing or null values</li>
                <li><strong>Consistency:</strong> Data follows defined formats</li>
                <li><strong>Accuracy:</strong> Data reflects real-world values</li>
                <li><strong>Integrity:</strong> Referential and business rules</li>
                <li><strong>Validity:</strong> Data conforms to defined schemas</li>
                <li><strong>Timeliness:</strong> Data is current and available</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="program-card">
            <h4>🔧 Data Quality Techniques</h4>
            <ul>
                <li><strong>Data profiling:</strong> Statistical analysis and patterns</li>
                <li><strong>Data sampling:</strong> Representative subset analysis</li>
                <li><strong>Validation rules:</strong> Business logic enforcement</li>
                <li><strong>Anomaly detection:</strong> Outlier identification</li>
                <li><strong>Data lineage:</strong> Track data flow and transformations</li>
                <li><strong>Monitoring:</strong> Continuous quality assessment</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Weekly Digital Training Curriculum
    st.markdown("## 📚 Weekly Digital Training Curriculum")
    st.markdown("**You can do it! Complete this week's digital training assignments:**")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.markdown("### AWS Skill Builder Learning Plan Courses")
        
        required_courses = [
            "Build with Amazon EC2",
            "AWS Identity and Access Management – Basics",
            "Getting Started with AWS Storage",
            "Amazon Simple Storage Service (S3) Storage Classes Deep Dive"
        ]
        
        for course in required_courses:
            st.markdown(f"""
            <div class="training-item">
                <div class="training-icon">📖</div>
                <div>
                    <strong>{course}</strong><br>
                    <small>Required Course</small>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Companion Learning Plan (Optional)")
        
        optional_labs = [
            "Complete Lab – EMR File System Client-side Encryption Using AWS KMS-managed Keys",
            "Complete Lab – Analyze Big Data with Hadoop"
        ]
        
        for lab in optional_labs:
            st.markdown(f"""
            <div class="training-item">
                <div class="training-icon">🧪</div>
                <div>
                    <strong>{lab}</strong><br>
                    <small>Hands-on Practice</small>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    # Action Items and Next Steps
    st.markdown("## 🚀 Action Items & Next Steps")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>📝 This Week's Focus</h4>
            <ul>
                <li>Master EMR cluster architecture and configuration</li>
                <li>Practice with Redshift Data API for automation</li>
                <li>Explore Athena federated queries</li>
                <li>Set up monitoring and alerting strategies</li>
                <li>Learn data quality assessment techniques</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 Key Study Areas</h4>
            <ul>
                <li>EMR node types and storage options (HDFS vs EMRFS)</li>
                <li>EventBridge rules and event-driven architectures</li>
                <li>QuickSight SPICE engine and embedded analytics</li>
                <li>CloudWatch metrics, logs, and alarms</li>
                <li>Data quality dimensions and validation techniques</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # AWS Skill Builder Subscription
    with st.expander("🎓 Optional AWS Skill Builder Subscription"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **Free Digital Training** 🆓
            - 600+ digital courses
            - Learning plans
            - 10 Practice Question Sets
            - AWS Cloud Quest (Foundational)
            """)
        
        with col2:
            st.markdown("""
            **Individual Subscription** 💰
            - Everything in free, plus:
            - AWS SimuLearn (200+ trainings)
            - Official Practice Exams
            - Unlimited hands-on labs
            - AWS Jam Journeys
            - **$29 USD/month or $449 USD/year**
            """)
        
        st.markdown("""
        **Recommended for this program:**
        - **AWS Cloud Quest** for additional lab work
        - **Companion Learning Plan** for curated labs
        - **Practice Exams** to test readiness
        """)
    
    # Helpful Resources
    with st.expander("📚 Additional Resources"):
        st.markdown("""
        **Documentation Links:**
        - [Amazon EMR Architecture Guide](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-overview-arch.html)
        - [EMR Instance Groups vs Instance Fleets](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-instance-group-configuration.html)
        - [Amazon Redshift Data API](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html)
        - [Athena Federated Query](https://aws.amazon.com/blogs/big-data/query-any-data-source-with-amazon-athenas-new-federated-query/)
        - [Amazon QuickSight Documentation](https://docs.aws.amazon.com/quicksight/)
        - [AWS Glue DataBrew User Guide](https://docs.aws.amazon.com/databrew/)
        - [SageMaker Data Wrangler](https://docs.aws.amazon.com/sagemaker/latest/dg/data-wrangler.html)
        - [CloudWatch Monitoring Guide](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/)
        
        **Helpful Blog Posts:**
        - [EventBridge and Kinesis Data Firehose Integration](https://aws.amazon.com/blogs/big-data/audit-aws-service-events-with-amazon-eventbridge-and-amazon-kinesis-data-firehose/)
        - [Building Event-Driven Applications with Redshift Data API](https://aws.amazon.com/blogs/big-data/building-an-event-driven-application-with-aws-lambda-and-the-amazon-redshift-data-api/)
        - [Athena Workgroups for Cost Management](https://aws.amazon.com/blogs/big-data/separating-queries-and-managing-costs-using-amazon-athena-workgroups/)
        - [Data Processing Options for AI/ML](https://aws.amazon.com/blogs/machine-learning/data-processing-options-for-ai-ml/)
        """)
    
    # Final Encouragement
    st.markdown("## 🎯 You're Almost There!")
    
    st.markdown("""
    <div class="program-card">
        <h4>🏆 Certification Journey Progress</h4>
        <p>Congratulations! You've completed 4 out of 5 content review sessions. You now have 
        comprehensive knowledge of data fundamentals, ingestion, transformation, storage management, 
        and operations. Only one more session to go before you're exam-ready!</p>
        <p><strong>Next Session Preview:</strong> Domain 4 will cover Data Security and Governance - 
        the final piece of your Data Engineer Associate certification puzzle. Topics include 
        encryption, access controls, compliance, and data governance frameworks.</p>
        <p><strong>Keep up the great work!</strong> 🌟</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Footer
    st.markdown("""
    <div class="footer">
        <p>© 2025, Amazon Web Services, Inc. or its affiliates. All rights reserved.</p>
        <p><strong>Thank you for attending this session!</strong></p>
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
