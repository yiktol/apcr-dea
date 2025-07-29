
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import utils.common as common
import utils.authenticate as authenticate
import json
import boto3
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="AWS EMR & Big Data Processing",
    page_icon="🐘",
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
    'warning': '#FFA500'
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
        
        .node-card {{
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
            border-left: 5px solid {AWS_COLORS['warning']};
        }}
        
        .success-box {{
            background-color: #D4EDDA;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 15px;
            border-left: 5px solid {AWS_COLORS['success']};
        }}
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    common.initialize_session_state()
    if 'emr_configurations' not in st.session_state:
        st.session_state.emr_configurations = []
        st.session_state.storage_setups = []
        st.session_state.processing_jobs = 0

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🐘 Amazon Elastic MapReduce (EMR) - Managed Hadoop framework service
            - 💾 EMR Data Storage Options - HDFS vs EMRFS with S3 integration
            - 🔧 Node Configuration Options - Instance groups vs instance fleets
            - 🔄 Transforming Data in EMR - Step-by-step data processing workflows
            - ⚖️ EMR vs Glue ETL - Comparison of big data processing services
            
            **Learning Objectives:**
            - Understand EMR cluster architecture and components
            - Master data storage strategies for big data workloads
            - Learn optimal node configuration for different use cases
            - Explore data transformation patterns and workflows
            - Make informed decisions between EMR and Glue ETL
            """)

def create_emr_architecture_diagram():
    """Create EMR cluster architecture diagram"""
    return """
    graph TB
        subgraph "EMR Cluster"
            PRIMARY[Primary Node<br/>🎯 YARN ResourceManager<br/>📊 HDFS NameNode<br/>🎭 Coordinates Tasks]
            
            subgraph "Core Nodes"
                CORE1[Core Node 1<br/>⚙️ YARN NodeManager<br/>💾 HDFS DataNode<br/>🔄 Processing + Storage]
                CORE2[Core Node 2<br/>⚙️ YARN NodeManager<br/>💾 HDFS DataNode<br/>🔄 Processing + Storage]
                CORE3[Core Node 3<br/>⚙️ YARN NodeManager<br/>💾 HDFS DataNode<br/>🔄 Processing + Storage]
            end
            
            subgraph "Task Nodes (Optional)"
                TASK1[Task Node 1<br/>⚙️ YARN NodeManager<br/>🚀 Processing Only<br/>💨 Ephemeral Storage]
                TASK2[Task Node 2<br/>⚙️ YARN NodeManager<br/>🚀 Processing Only<br/>💨 Ephemeral Storage]
            end
        end
        
        CLIENT[Client Applications<br/>📱 Spark Jobs<br/>🗄️ Hive Queries<br/>🐷 Pig Scripts] --> PRIMARY
        
        PRIMARY --> CORE1
        PRIMARY --> CORE2
        PRIMARY --> CORE3
        PRIMARY --> TASK1
        PRIMARY --> TASK2
        
        S3[(Amazon S3<br/>📦 Data Lake<br/>🔄 EMRFS)]
        
        CORE1 -.-> S3
        CORE2 -.-> S3
        CORE3 -.-> S3
        
        style PRIMARY fill:#FF9900,stroke:#232F3E,color:#fff
        style CORE1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CORE2 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CORE3 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style TASK1 fill:#3FB34F,stroke:#232F3E,color:#fff
        style TASK2 fill:#3FB34F,stroke:#232F3E,color:#fff
        style S3 fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_storage_comparison_diagram():
    """Create HDFS vs EMRFS comparison diagram"""
    return """
    graph LR
        subgraph "HDFS Storage"
            PRIMARY1[Primary Node<br/>📊 NameNode]
            CORE1_H[Core Node 1<br/>💾 DataNode]
            CORE2_H[Core Node 2<br/>💾 DataNode]
            CORE3_H[Core Node 3<br/>💾 DataNode]
            
            PRIMARY1 --> CORE1_H
            PRIMARY1 --> CORE2_H  
            PRIMARY1 --> CORE3_H
        end
        
        subgraph "EMRFS Storage"
            PRIMARY2[Primary Node<br/>🎯 Coordinator]
            CORE1_E[Core Node 1<br/>⚙️ Processor]
            CORE2_E[Core Node 2<br/>⚙️ Processor]
            CORE3_E[Core Node 3<br/>⚙️ Processor]
            S3_STORAGE[(Amazon S3<br/>☁️ Durable Storage<br/>🔄 EMRFS Layer)]
            
            PRIMARY2 --> CORE1_E
            PRIMARY2 --> CORE2_E
            PRIMARY2 --> CORE3_E
            
            CORE1_E -.-> S3_STORAGE
            CORE2_E -.-> S3_STORAGE
            CORE3_E -.-> S3_STORAGE
        end
        
        style PRIMARY1 fill:#FF9900,stroke:#232F3E,color:#fff
        style PRIMARY2 fill:#FF9900,stroke:#232F3E,color:#fff
        style CORE1_H fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CORE2_H fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CORE3_H fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CORE1_E fill:#3FB34F,stroke:#232F3E,color:#fff
        style CORE2_E fill:#3FB34F,stroke:#232F3E,color:#fff
        style CORE3_E fill:#3FB34F,stroke:#232F3E,color:#fff
        style S3_STORAGE fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_data_processing_flow():
    """Create EMR data processing workflow diagram"""
    return """
    graph TD
        START[📥 Raw Data Ingestion<br/>S3 Input Bucket] --> STEP1[Step 1: Data Validation<br/>🔍 Check Schema<br/>✅ Validate Format]
        
        STEP1 --> STEP2[Step 2: Data Cleaning<br/>🧹 Remove Nulls<br/>🔧 Fix Formatting<br/>📊 Standardize Types]
        
        STEP2 --> STEP3[Step 3: Data Transformation<br/>🔄 Apply Business Logic<br/>📈 Aggregate Data<br/>🏷️ Add Derived Fields]
        
        STEP3 --> STEP4[Step 4: Data Enrichment<br/>🔗 Join with Reference Data<br/>📍 Geocoding<br/>🕰️ Time Zone Conversion]
        
        STEP4 --> STEP5[Step 5: Quality Checks<br/>📏 Data Validation<br/>📊 Statistical Analysis<br/>🚨 Anomaly Detection]
        
        STEP5 --> OUTPUT[📤 Processed Data Output<br/>S3 Output Bucket<br/>🗄️ Data Warehouse<br/>📊 Analytics Ready]
        
        subgraph "Processing Details"
            SPARK[🔥 Spark Engine<br/>Distributed Processing]
            HIVE[🐝 Hive Engine<br/>SQL-like Queries]
            HADOOP[🐘 Hadoop MapReduce<br/>Batch Processing]
        end
        
        STEP2 -.-> SPARK
        STEP3 -.-> HIVE
        STEP4 -.-> HADOOP
        
        style START fill:#FF9900,stroke:#232F3E,color:#fff
        style OUTPUT fill:#3FB34F,stroke:#232F3E,color:#fff
        style STEP1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style STEP2 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style STEP3 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style STEP4 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style STEP5 fill:#4B9EDB,stroke:#232F3E,color:#fff
    """

def emr_overview_tab():
    """Content for Amazon EMR overview tab"""
    st.markdown("## 🐘 Amazon Elastic MapReduce (EMR)")
    st.markdown("*Managed cluster platform that simplifies running big data frameworks on AWS*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What is Amazon EMR?
    Amazon EMR is a cloud big data platform for processing vast amounts of data using open source tools such as:
    - **Apache Spark** - Fast, general-purpose cluster computing
    - **Apache Hadoop** - Distributed storage and processing
    - **Apache Hive** - Data warehouse software for querying large datasets
    - **Apache HBase** - Non-relational database for big data
    - **Presto** - High performance, distributed SQL query engine
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # EMR Architecture
    st.markdown("#### 🏗️ EMR Cluster Architecture")
    common.mermaid(create_emr_architecture_diagram(), height=800)
    
    # Interactive cluster configurator
    st.markdown("#### 🎛️ Interactive EMR Cluster Configurator")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("##### Cluster Configuration")
        cluster_name = st.text_input("Cluster Name", value="my-analytics-cluster")
        emr_version = st.selectbox("EMR Release", ["emr-6.15.0", "emr-6.14.0", "emr-6.13.0", "emr-7.0.0"])
        applications = st.multiselect("Applications", 
                                    ["Spark", "Hadoop", "Hive", "HBase", "Presto", "Zeppelin", "Jupyter"], 
                                    default=["Spark", "Hadoop", "Hive"])
    
    with col2:
        st.markdown("##### Node Configuration")
        primary_instance = st.selectbox("Primary Node", ["m5.xlarge", "m5.2xlarge", "m5.4xlarge"])
        core_instance = st.selectbox("Core Nodes", ["m5.xlarge", "m5.2xlarge", "m5.4xlarge", "r5.xlarge"])
        core_count = st.slider("Core Node Count", 1, 10, 3)
        task_count = st.slider("Task Node Count (Optional)", 0, 20, 0)
    
    with col3:
        st.markdown("##### Storage & Pricing")
        storage_option = st.selectbox("Storage Strategy", ["HDFS", "EMRFS (S3)", "Mixed"])
        instance_purchasing = st.selectbox("Purchasing Option", ["On-Demand", "Spot", "Mixed"])
        
        # Calculate estimated cost
        estimated_cost = calculate_emr_cost(primary_instance, core_instance, core_count, task_count, instance_purchasing)
        st.metric("Estimated Cost/Hour", f"${estimated_cost:.2f}")
    
    # Display cluster summary
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Cluster Configuration Summary
    **Cluster Name**: {cluster_name}  
    **EMR Version**: {emr_version}  
    **Applications**: {', '.join(applications)}  
    **Total Nodes**: {1 + core_count + task_count} ({1} primary + {core_count} core + {task_count} task)  
    **Storage Strategy**: {storage_option}  
    **Estimated Monthly Cost**: ${estimated_cost * 24 * 30:.2f}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Node types explanation
    st.markdown("#### 🔧 EMR Node Types Explained")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="node-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 Primary Node
        **Role**: Master coordinator
        
        **Responsibilities**:
        - YARN ResourceManager
        - HDFS NameNode  
        - Distributes work
        - Monitors cluster health
        
        **Count**: Always 1
        **Critical**: Yes - cluster fails if down
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="node-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚙️ Core Nodes
        **Role**: Worker + storage
        
        **Responsibilities**:
        - YARN NodeManager
        - HDFS DataNode
        - Execute tasks
        - Store data in HDFS
        
        **Count**: 1 or more
        **Critical**: Data loss if removed
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="node-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚀 Task Nodes
        **Role**: Additional processing
        
        **Responsibilities**:
        - YARN NodeManager only
        - Execute tasks
        - No HDFS storage
        - Scale compute capacity
        
        **Count**: 0 or more (optional)
        **Critical**: No - safe to add/remove
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Use cases
    st.markdown("#### 🎯 Common EMR Use Cases")
    
    use_cases = {
        "Data Processing & ETL": {
            "description": "Transform and prepare data for analytics",
            "tools": ["Spark", "Hadoop MapReduce"],
            "example": "Processing daily log files, cleaning customer data"
        },
        "Big Data Analytics": {
            "description": "Run complex analytical queries on large datasets", 
            "tools": ["Spark SQL", "Hive", "Presto"],
            "example": "Customer behavior analysis, sales reporting"
        },
        "Machine Learning": {
            "description": "Train ML models on large datasets",
            "tools": ["Spark MLlib", "TensorFlow", "MXNet"],
            "example": "Recommendation engines, fraud detection"
        },
        "Real-time Analytics": {
            "description": "Process streaming data in real-time",
            "tools": ["Spark Streaming", "Kafka", "Kinesis"],
            "example": "IoT sensor data processing, clickstream analysis"
        }
    }
    
    for use_case, details in use_cases.items():
        with st.expander(f"📋 {use_case}"):
            st.markdown(f"""
            **Description**: {details['description']}  
            **Recommended Tools**: {', '.join(details['tools'])}  
            **Example**: {details['example']}
            """)
    
    # Code example
    st.markdown("#### 💻 EMR Cluster Creation Example")
    
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code(f'''
# Create EMR cluster using AWS CLI
aws emr create-cluster \\
    --name "{cluster_name}" \\
    --release-label {emr_version} \\
    --applications Name=Spark Name=Hadoop Name=Hive \\
    --instance-type {primary_instance} \\
    --instance-count {1 + core_count} \\
    --use-default-roles \\
    --ec2-attributes KeyName=my-key-pair \\
    --log-uri s3://my-emr-logs/ \\
    --enable-debugging

# Python Boto3 example
import boto3

emr = boto3.client('emr')

response = emr.run_job_flow(
    Name='{cluster_name}',
    ReleaseLabel='{emr_version}',
    Instances={{
        'InstanceGroups': [
            {{
                'Name': 'Primary',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': '{primary_instance}',
                'InstanceCount': 1
            }},
            {{
                'Name': 'Core',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE', 
                'InstanceType': '{core_instance}',
                'InstanceCount': {core_count}
            }}
        ],
        'Ec2KeyName': 'my-key-pair',
        'KeepJobFlowAliveWhenNoSteps': True
    }},
    Applications=[
        {{'Name': app}} for app in {applications}
    ],
    LogUri='s3://my-emr-logs/',
    ServiceRole='EMR_DefaultRole',
    JobFlowRole='EMR_EC2_DefaultRole'
)

cluster_id = response['JobFlowId']
print(f"Cluster created with ID: {{cluster_id}}")
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Update session state
    if st.button("🚀 Save Cluster Configuration"):
        config = {
            'name': cluster_name,
            'version': emr_version,
            'applications': applications,
            'nodes': f"{1 + core_count + task_count}",
            'cost_per_hour': estimated_cost
        }
        st.session_state.emr_configurations.append(config)
        st.session_state.emr_clusters_created += 1
        st.success(f"Cluster configuration '{cluster_name}' saved!")

def emr_storage_options_tab():
    """Content for EMR data storage options tab"""
    st.markdown("## 💾 Amazon EMR Data Storage Options")
    st.markdown("*Choose between HDFS for performance or EMRFS for durability and cost optimization*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Storage Strategy Decision
    EMR offers two primary data storage approaches:
    - **HDFS (Hadoop Distributed File System)** - Local storage across cluster nodes
    - **EMRFS (EMR File System)** - Direct integration with Amazon S3
    - **Hybrid Approach** - Combine both for optimal performance and durability
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Storage comparison diagram
    st.markdown("#### 🔄 HDFS vs EMRFS Architecture")
    common.mermaid(create_storage_comparison_diagram(), height=900)
    
    # Interactive storage calculator
    st.markdown("#### 📊 Storage Strategy Calculator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Workload Characteristics")
        data_size_tb = st.number_input("Dataset Size (TB)", min_value=0.1, max_value=1000.0, value=5.0, step=0.1)
        read_pattern = st.selectbox("Data Access Pattern", 
                                  ["One-time processing", "Multiple iterations", "Random access", "Sequential scan"])
        durability_requirement = st.selectbox("Durability Requirement", 
                                           ["Temporary processing", "Important data", "Critical data"])
        processing_frequency = st.selectbox("Processing Frequency", 
                                          ["One-time", "Daily", "Hourly", "Streaming"])
    
    with col2:
        st.markdown("##### Performance Requirements")
        latency_requirement = st.selectbox("Latency Requirement", ["Low latency critical", "Standard", "High latency acceptable"])
        throughput_requirement = st.selectbox("Throughput Requirement", ["High throughput", "Standard", "Low throughput"])
        cost_sensitivity = st.selectbox("Cost Sensitivity", ["Cost optimization critical", "Balanced", "Performance over cost"])
    
    # Generate recommendation
    storage_recommendation = get_storage_recommendation(
        read_pattern, durability_requirement, latency_requirement, cost_sensitivity
    )
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📋 Storage Strategy Recommendation
    **Primary Storage**: {storage_recommendation['primary']}  
    **Secondary Storage**: {storage_recommendation['secondary']}  
    **Replication Strategy**: {storage_recommendation['replication']}  
    **Estimated Cost/Month**: ${calculate_storage_cost(data_size_tb, storage_recommendation):.2f}  
    **Performance Profile**: {storage_recommendation['performance']}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Detailed comparison table
    st.markdown("#### ⚖️ HDFS vs EMRFS Detailed Comparison")
    
    comparison_data = {
        'Aspect': [
            'Storage Location', 'Durability', 'Performance', 'Cost', 'Scalability',
            'Data Persistence', 'Replication', 'Best Use Case', 'Latency', 'Throughput'
        ],
        'HDFS': [
            'Local EBS volumes', 'Cluster lifetime only', 'Very High', 'Higher (3x replication)', 'Limited by nodes',
            'Ephemeral', '3x by default', 'Iterative processing', 'Lowest', 'Highest'
        ],
        'EMRFS': [
            'Amazon S3', 'Highly durable (99.999999999%)', 'High', 'Lower', 'Unlimited',
            'Permanent', 'Built into S3', 'One-time processing', 'Low', 'High'
        ]
    }
    
    df_comparison = pd.DataFrame(comparison_data)
    st.dataframe(df_comparison, use_container_width=True)
    
    # Performance metrics visualization
    st.markdown("#### 📈 Performance Characteristics")
    
    # Create performance comparison chart
    metrics = ['Read Throughput', 'Write Throughput', 'Random Access', 'Sequential Access', 'Cost Efficiency']
    hdfs_scores = [95, 90, 85, 98, 60]
    emrfs_scores = [85, 80, 75, 90, 95]
    
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=hdfs_scores,
        theta=metrics,
        fill='toself',
        name='HDFS',
        line_color=AWS_COLORS['primary']
    ))
    fig.add_trace(go.Scatterpolar(
        r=emrfs_scores,
        theta=metrics,
        fill='toself',
        name='EMRFS',
        line_color=AWS_COLORS['light_blue']
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )),
        showlegend=True,
        title="HDFS vs EMRFS Performance Comparison"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Configuration examples
    st.markdown("#### 💻 Storage Configuration Examples")
    
    tab1, tab2, tab3 = st.tabs(["HDFS Configuration", "EMRFS Configuration", "Hybrid Setup"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# HDFS Configuration for High-Performance Workloads

# EMR cluster with HDFS optimization
aws emr create-cluster \\
    --name "hdfs-optimized-cluster" \\
    --release-label emr-6.15.0 \\
    --applications Name=Spark Name=Hadoop \\
    --instance-groups '[
        {
            "Name": "Primary",
            "InstanceRole": "MASTER",
            "InstanceType": "m5.2xlarge",
            "InstanceCount": 1,
            "EbsConfiguration": {
                "EbsBlockDeviceConfigs": [
                    {
                        "VolumeSpecification": {
                            "VolumeType": "gp3",
                            "SizeInGB": 500,
                            "Iops": 3000
                        },
                        "VolumesPerInstance": 1
                    }
                ]
            }
        },
        {
            "Name": "Core",
            "InstanceRole": "CORE", 
            "InstanceType": "r5.4xlarge",
            "InstanceCount": 4,
            "EbsConfiguration": {
                "EbsBlockDeviceConfigs": [
                    {
                        "VolumeSpecification": {
                            "VolumeType": "gp3",
                            "SizeInGB": 1000,
                            "Iops": 3000
                        },
                        "VolumesPerInstance": 2
                    }
                ]
            }
        }
    ]' \\
    --configurations '[
        {
            "Classification": "hdfs-site",
            "Properties": {
                "dfs.replication": "3",
                "dfs.block.size": "268435456",
                "dfs.namenode.handler.count": "100"
            }
        },
        {
            "Classification": "yarn-site", 
            "Properties": {
                "yarn.nodemanager.resource.memory-mb": "57344",
                "yarn.scheduler.maximum-allocation-mb": "57344"
            }
        }
    ]'

# Spark job using HDFS
spark-submit \\
    --class com.example.DataProcessor \\
    --master yarn \\
    --deploy-mode cluster \\
    --driver-memory 4g \\
    --executor-memory 8g \\
    --executor-cores 4 \\
    --num-executors 12 \\
    my-spark-app.jar \\
    hdfs:///user/input/data/ \\
    hdfs:///user/output/results/

# PySpark example with HDFS
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("HDFS Processing") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .getOrCreate()

# Read from HDFS
df = spark.read.parquet("hdfs:///data/sales/")

# Process data (benefits from local storage speed)
processed_df = df.groupBy("category", "date") \\
    .agg({"amount": "sum", "quantity": "count"}) \\
    .orderBy("date")

# Write back to HDFS (faster than S3 for intermediate results)
processed_df.write \\
    .mode("overwrite") \\
    .parquet("hdfs:///data/processed/sales_summary/")

# Copy final results to S3 for durability
processed_df.write \\
    .mode("overwrite") \\
    .parquet("s3://my-bucket/analytics/sales_summary/")

spark.stop()
        ''', language='bash')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# EMRFS Configuration for Cost-Optimized Workloads

# EMR cluster optimized for S3 access
aws emr create-cluster \\
    --name "emrfs-optimized-cluster" \\
    --release-label emr-6.15.0 \\
    --applications Name=Spark Name=Hive \\
    --instance-groups '[
        {
            "Name": "Primary",
            "InstanceRole": "MASTER",
            "InstanceType": "m5.xlarge", 
            "InstanceCount": 1
        },
        {
            "Name": "Core",
            "InstanceRole": "CORE",
            "InstanceType": "c5.2xlarge",
            "InstanceCount": 3
        }
    ]' \\
    --configurations '[
        {
            "Classification": "emrfs-site",
            "Properties": {
                "fs.s3.consistent": "true",
                "fs.s3.consistent.retryPolicyType": "exponential",
                "fs.s3.consistent.retryPeriodSeconds": "10",
                "fs.s3.consistent.retryCount": "5",
                "fs.s3.maxConnections": "500"
            }
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.hadoop.fs.s3a.multipart.size": "104857600",
                "spark.hadoop.fs.s3a.fast.upload": "true"
            }
        }
    ]'

# PySpark job using EMRFS
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \\
    .appName("EMRFS Processing") \\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \\
    .getOrCreate()

# Read directly from S3 (no data movement to cluster)
input_path = "s3://my-data-lake/raw/sales/"
df = spark.read \\
    .option("inferSchema", "true") \\
    .option("header", "true") \\
    .csv(input_path)

# Optimize for S3 processing
df = df.repartition(200)  # Optimize partition count for S3

# Process data
daily_sales = df.groupBy("date", "region") \\
    .agg(
        sum("amount").alias("total_sales"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("amount").alias("avg_order_size")
    ) \\
    .orderBy("date", "region")

# Write directly to S3 (persistent storage)
output_path = "s3://my-data-lake/processed/daily_sales/"
daily_sales.write \\
    .mode("overwrite") \\
    .partitionBy("date") \\
    .parquet(output_path)

# Create external Hive table pointing to S3 data
spark.sql(f"""
    CREATE TABLE daily_sales_table (
        date string,
        region string,
        total_sales double,
        unique_customers long,
        avg_order_size double
    )
    USING PARQUET
    LOCATION '{output_path}'
    PARTITIONED BY (date)
""")

spark.stop()

# S3DistCp for efficient data movement between S3 and HDFS
aws emr add-steps \\
    --cluster-id j-XXXXXXXXXXXXX \\
    --steps '[
        {
            "Name": "Copy S3 to HDFS",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "s3-dist-cp",
                    "--src", "s3://source-bucket/data/",
                    "--dest", "hdfs:///tmp/processing/",
                    "--srcPattern", ".*\\.parquet$",
                    "--targetSize", "1024"
                ]
            }
        }
    ]'
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Hybrid Storage Strategy - Best of Both Worlds

# Use HDFS for active processing, S3 for archival and input/output
# This approach optimizes for both performance and cost

# PySpark Hybrid Processing Example
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \\
    .appName("Hybrid Storage Processing") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .getOrCreate()

# Step 1: Load raw data from S3 to HDFS for processing
print("Loading data from S3 to HDFS...")
raw_data = spark.read.parquet("s3://data-lake/raw/transactions/")

# Cache frequently accessed data in HDFS for performance
raw_data.write.mode("overwrite").parquet("hdfs:///tmp/transactions/")
raw_data_hdfs = spark.read.parquet("hdfs:///tmp/transactions/")
raw_data_hdfs.cache()  # Keep in memory for multiple operations

# Step 2: Heavy processing using HDFS (fast local access)
print("Processing data using HDFS...")

# Complex aggregations that benefit from local storage speed
customer_metrics = raw_data_hdfs.groupBy("customer_id") \\
    .agg(
        sum("amount").alias("total_spent"),
        count("*").alias("transaction_count"),
        countDistinct("product_id").alias("unique_products"),
        datediff(max("transaction_date"), min("transaction_date")).alias("customer_lifetime_days")
    )

# Join with reference data (also cached in HDFS)
product_data = spark.read.parquet("s3://data-lake/reference/products/")
product_data.write.mode("overwrite").parquet("hdfs:///tmp/products/")
product_data_hdfs = spark.read.parquet("hdfs:///tmp/products/")
product_data_hdfs.cache()

# Heavy join operations benefit from HDFS speed
enriched_transactions = raw_data_hdfs.join(
    product_data_hdfs,
    raw_data_hdfs.product_id == product_data_hdfs.id,
    "left"
).select(
    raw_data_hdfs["*"],
    product_data_hdfs.category,
    product_data_hdfs.brand,
    product_data_hdfs.profit_margin
)

# Step 3: Intermediate results stored in HDFS for iterative processing
print("Storing intermediate results in HDFS...")
enriched_transactions.write \\
    .mode("overwrite") \\
    .parquet("hdfs:///tmp/enriched_transactions/")

# Step 4: Multiple analysis iterations using HDFS data
enriched_hdfs = spark.read.parquet("hdfs:///tmp/enriched_transactions/")

# Analysis 1: Category performance
category_analysis = enriched_hdfs.groupBy("category", "date") \\
    .agg(
        sum("amount").alias("revenue"),
        sum("profit_margin").alias("profit"),
        countDistinct("customer_id").alias("customers")
    )

# Analysis 2: Customer segmentation  
customer_segments = enriched_hdfs.groupBy("customer_id") \\
    .agg(
        sum("amount").alias("total_value"),
        count("*").alias("frequency"),
        max("transaction_date").alias("last_purchase")
    ) \\
    .withColumn("segment", 
        when(col("total_value") > 1000, "VIP")
        .when(col("total_value") > 500, "Premium") 
        .otherwise("Standard")
    )

# Step 5: Final results written to S3 for persistence and sharing
print("Writing final results to S3...")

# Long-term storage in S3 with proper partitioning
category_analysis.write \\
    .mode("overwrite") \\
    .partitionBy("date") \\
    .parquet("s3://analytics-results/category_performance/")

customer_segments.write \\
    .mode("overwrite") \\
    .partitionBy("segment") \\
    .parquet("s3://analytics-results/customer_segments/")

# Step 6: Cleanup HDFS temporary data
print("Cleaning up HDFS temporary data...")
import subprocess

subprocess.run([
    "hdfs", "dfs", "-rm", "-r", "-f", 
    "/tmp/transactions/", "/tmp/products/", "/tmp/enriched_transactions/"
])

# Step 7: Archive raw data with S3 lifecycle policies
# Configure S3 lifecycle to transition old data to cheaper storage classes
lifecycle_config = {
    "Rules": [
        {
            "Status": "Enabled",
            "Filter": {"Prefix": "raw/transactions/"},
            "Transitions": [
                {
                    "Days": 30,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "Days": 365, 
                    "StorageClass": "GLACIER"
                }
            ]
        }
    ]
}

print("Hybrid processing complete!")
print("Benefits achieved:")
print("- Fast processing using HDFS local storage")
print("- Cost-effective long-term storage in S3") 
print("- Data durability through S3 replication")
print("- Ability to scale compute independently of storage")

spark.stop()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Save configuration
    if st.button("💾 Save Storage Configuration"):
        config = {
            'data_size_tb': data_size_tb,
            'strategy': storage_recommendation['primary'],
            'estimated_cost': calculate_storage_cost(data_size_tb, storage_recommendation),
            'use_case': read_pattern
        }
        st.session_state.storage_setups.append(config)
        st.success("Storage configuration saved!")

def node_configuration_tab():
    """Content for EMR node configuration options tab"""
    st.markdown("## 🔧 EMR Node Configuration Options")
    st.markdown("*Choose between Instance Groups and Instance Fleets for optimal cost and performance*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Node Configuration Strategies
    EMR offers two approaches for configuring cluster nodes:
    - **Instance Groups** - Simple, uniform instance types for each node role
    - **Instance Fleets** - Flexible, mixed instance types with advanced allocation strategies
    - **Spot Instance Integration** - Significant cost savings with both approaches
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Configuration comparison diagram
    configuration_diagram = """
    graph TB
        subgraph "Instance Groups"
            IG_PRIMARY[Primary Group<br/>🎯 1x m5.2xlarge<br/>💰 On-Demand]
            IG_CORE[Core Group<br/>⚙️ 3x r5.xlarge<br/>💰 On-Demand]
            IG_TASK1[Task Group 1<br/>🚀 5x c5.large<br/>💸 Spot Instances]
            IG_TASK2[Task Group 2<br/>🚀 3x m5.large<br/>💸 Spot Instances]
        end
        
        subgraph "Instance Fleets"
            IF_PRIMARY[Primary Fleet<br/>🎯 Target: 1 unit<br/>📋 m5.xlarge, m5.2xlarge, m4.xlarge]
            IF_CORE[Core Fleet<br/>⚙️ Target: 8 units<br/>📋 r5.xlarge, r4.xlarge, m5.xlarge<br/>💰 50% On-Demand, 50% Spot]
            IF_TASK[Task Fleet<br/>🚀 Target: 20 units<br/>📋 c5.large, c4.large, m5.large<br/>💸 100% Spot]
        end
        
        WORKLOAD[Processing Workload] --> IG_PRIMARY
        WORKLOAD --> IF_PRIMARY
        
        style IG_PRIMARY fill:#FF9900,stroke:#232F3E,color:#fff
        style IG_CORE fill:#4B9EDB,stroke:#232F3E,color:#fff
        style IG_TASK1 fill:#3FB34F,stroke:#232F3E,color:#fff
        style IG_TASK2 fill:#3FB34F,stroke:#232F3E,color:#fff
        style IF_PRIMARY fill:#FF9900,stroke:#232F3E,color:#fff
        style IF_CORE fill:#4B9EDB,stroke:#232F3E,color:#fff
        style IF_TASK fill:#3FB34F,stroke:#232F3E,color:#fff
    """
    
    st.markdown("#### 🔄 Instance Groups vs Instance Fleets")
    common.mermaid(configuration_diagram, height=350)
    
    # Interactive configuration builder
    st.markdown("#### 🎛️ EMR Configuration Builder")
    
    config_type = st.radio("Configuration Type", ["Instance Groups", "Instance Fleets"], horizontal=True)
    
    if config_type == "Instance Groups":
        st.markdown("##### Instance Groups Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Primary Node**")
            primary_type = st.selectbox("Instance Type", ["m5.xlarge", "m5.2xlarge", "m5.4xlarge"], key="primary_ig")
            primary_pricing = st.selectbox("Pricing", ["On-Demand", "Spot"], key="primary_pricing")
            
            st.markdown("**Core Nodes**")
            core_type = st.selectbox("Instance Type", ["r5.xlarge", "r5.2xlarge", "m5.xlarge"], key="core_ig")
            core_count = st.slider("Count", 1, 10, 3, key="core_count_ig")
            core_pricing = st.selectbox("Pricing", ["On-Demand", "Spot"], key="core_pricing")
        
        with col2:
            st.markdown("**Task Nodes (Optional)**")
            task_enabled = st.checkbox("Enable Task Nodes")
            
            if task_enabled:
                task_type = st.selectbox("Instance Type", ["c5.large", "c5.xlarge", "m5.large"], key="task_ig")
                task_count = st.slider("Count", 0, 20, 5, key="task_count_ig")
                task_pricing = st.selectbox("Pricing", ["Spot", "On-Demand"], key="task_pricing")
            else:
                task_type, task_count, task_pricing = None, 0, None
        
        # Calculate instance groups cost
        config_cost = calculate_instance_groups_cost(
            primary_type, primary_pricing,
            core_type, core_count, core_pricing,
            task_type, task_count, task_pricing
        )
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 📊 Instance Groups Configuration
        **Primary**: 1x {primary_type} ({primary_pricing})  
        **Core**: {core_count}x {core_type} ({core_pricing})  
        **Task**: {task_count}x {task_type if task_type else 'None'} ({task_pricing if task_pricing else 'N/A'})  
        **Total Nodes**: {1 + core_count + task_count}  
        **Estimated Cost/Hour**: ${config_cost:.2f}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    else:  # Instance Fleets
        st.markdown("##### Instance Fleets Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Primary Fleet**")
            primary_types = st.multiselect("Instance Types", 
                                         ["m5.xlarge", "m5.2xlarge", "m4.xlarge", "m4.2xlarge"], 
                                         default=["m5.xlarge", "m5.2xlarge"], key="primary_fleet")
            
            st.markdown("**Core Fleet**")
            core_types = st.multiselect("Instance Types",
                                      ["r5.xlarge", "r5.2xlarge", "m5.xlarge", "m5.2xlarge"],
                                      default=["r5.xlarge", "m5.xlarge"], key="core_fleet")
            core_target_capacity = st.slider("Target Capacity (vCPU hours)", 10, 100, 32, key="core_capacity")
            core_ondemand_pct = st.slider("On-Demand %", 0, 100, 50, key="core_ondemand")
        
        with col2:
            st.markdown("**Task Fleet**")
            task_fleet_enabled = st.checkbox("Enable Task Fleet")
            
            if task_fleet_enabled:
                task_types = st.multiselect("Instance Types",
                                          ["c5.large", "c5.xlarge", "m5.large", "c4.large"],
                                          default=["c5.large", "m5.large"], key="task_fleet")
                task_target_capacity = st.slider("Target Capacity (vCPU hours)", 0, 200, 40, key="task_capacity")
                task_ondemand_pct = st.slider("On-Demand %", 0, 100, 0, key="task_ondemand")
            else:
                task_types, task_target_capacity, task_ondemand_pct = [], 0, 0
        
        # Calculate instance fleets cost
        fleet_cost = calculate_instance_fleets_cost(
            len(primary_types), core_target_capacity, core_ondemand_pct,
            task_target_capacity, task_ondemand_pct
        )
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 📊 Instance Fleets Configuration
        **Primary Fleet**: {len(primary_types)} instance types  
        **Core Fleet**: {core_target_capacity} vCPU hours ({core_ondemand_pct}% On-Demand)  
        **Task Fleet**: {task_target_capacity} vCPU hours ({task_ondemand_pct}% On-Demand)  
        **Spot Savings**: ~{calculate_spot_savings(core_ondemand_pct, task_ondemand_pct):.0f}%  
        **Estimated Cost/Hour**: ${fleet_cost:.2f}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Configuration comparison table
    st.markdown("#### ⚖️ Instance Groups vs Instance Fleets Comparison")
    
    comparison_data = {
        'Feature': [
            'Configuration Complexity', 'Instance Type Flexibility', 'Spot Instance Integration',
            'Cost Optimization', 'Fault Tolerance', 'Management Overhead', 'Best Use Case'
        ],
        'Instance Groups': [
            'Simple - one type per group', 'Limited - uniform instances', 'Basic spot support',
            'Good', 'Standard', 'Low', 'Predictable workloads'
        ],
        'Instance Fleets': [
            'Advanced - mixed types', 'High - multiple types per fleet', 'Advanced allocation strategies',
            'Excellent', 'High - diversified instances', 'Medium', 'Variable workloads, cost optimization'
        ]
    }
    
    df_comparison = pd.DataFrame(comparison_data)
    st.dataframe(df_comparison, use_container_width=True)
    
    # Spot instance strategies
    st.markdown("#### 💸 Spot Instance Strategies")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Spot Instance Best Practices
        
        **For Task Nodes:**
        - Use 100% Spot Instances (safe to interrupt)
        - Diversify across multiple instance types
        - Set appropriate timeout actions
        - Use Instance Fleets for better allocation
        
        **For Core Nodes:**
        - Mix On-Demand (50%) + Spot (50%)
        - Ensure minimum On-Demand capacity
        - Use allocation strategy: `diversified`
        - Monitor interruption rates
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="warning-box">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚠️ Spot Instance Considerations
        
        **Not Recommended For:**
        - Primary nodes (single point of failure)
        - HDFS-only workloads without S3 backup
        - Time-critical jobs with tight SLAs
        - Workloads sensitive to interruptions
        
        **Risk Mitigation:**
        - Use EMRFS for data durability
        - Implement checkpointing in applications
        - Monitor spot price history
        - Have fallback to On-Demand instances
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Configuration Examples")
    
    tab1, tab2, tab3 = st.tabs(["Instance Groups", "Instance Fleets", "Spot Optimization"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Instance Groups Configuration with AWS CLI

aws emr create-cluster \\
    --name "instance-groups-cluster" \\
    --release-label emr-6.15.0 \\
    --applications Name=Spark Name=Hadoop \\
    --use-default-roles \\
    --instance-groups '[
        {
            "Name": "Primary", 
            "InstanceRole": "MASTER",
            "InstanceType": "m5.2xlarge",
            "InstanceCount": 1,
            "Market": "ON_DEMAND",
            "EbsConfiguration": {
                "EbsBlockDeviceConfigs": [
                    {
                        "VolumeSpecification": {
                            "VolumeType": "gp3",
                            "SizeInGB": 100
                        },
                        "VolumesPerInstance": 1
                    }
                ]
            }
        },
        {
            "Name": "Core",
            "InstanceRole": "CORE",
            "InstanceType": "r5.xlarge", 
            "InstanceCount": 3,
            "Market": "ON_DEMAND",
            "EbsConfiguration": {
                "EbsBlockDeviceConfigs": [
                    {
                        "VolumeSpecification": {
                            "VolumeType": "gp3",
                            "SizeInGB": 200
                        },
                        "VolumesPerInstance": 2
                    }
                ]
            }
        },
        {
            "Name": "Task-Spot",
            "InstanceRole": "TASK",
            "InstanceType": "c5.xlarge",
            "InstanceCount": 5,
            "Market": "SPOT",
            "BidPrice": "0.10"
        }
    ]'

# Python Boto3 Example
import boto3

emr = boto3.client('emr')

instance_groups = [
    {
        'Name': 'Primary',
        'InstanceRole': 'MASTER',
        'InstanceType': 'm5.2xlarge',
        'InstanceCount': 1,
        'Market': 'ON_DEMAND'
    },
    {
        'Name': 'Core',
        'InstanceRole': 'CORE', 
        'InstanceType': 'r5.xlarge',
        'InstanceCount': 3,
        'Market': 'ON_DEMAND'
    },
    {
        'Name': 'Task',
        'InstanceRole': 'TASK',
        'InstanceType': 'c5.xlarge', 
        'InstanceCount': 5,
        'Market': 'SPOT',
        'BidPrice': '0.10',
        'Configurations': [
            {
                'Classification': 'yarn-site',
                'Properties': {
                    'yarn.nodemanager.vmem-check-enabled': 'false'
                }
            }
        ]
    }
]

# Auto-scaling configuration for instance groups
auto_scaling_policy = {
    'Constraints': {
        'MinCapacity': 1,
        'MaxCapacity': 10
    },
    'Rules': [
        {
            'Name': 'Scale-out',
            'Description': 'Scale out when memory utilization exceeds 75%',
            'Action': {
                'SimpleScalingPolicyConfiguration': {
                    'AdjustmentType': 'CHANGE_IN_CAPACITY',
                    'ScalingAdjustment': 2,
                    'CoolDown': 300
                }
            },
            'Trigger': {
                'CloudWatchAlarmDefinition': {
                    'ComparisonOperator': 'GREATER_THAN',
                    'EvaluationPeriods': 2,
                    'MetricName': 'MemoryPercentage',
                    'Namespace': 'AWS/ElasticMapReduce',
                    'Period': 300,
                    'Statistic': 'AVERAGE',
                    'Threshold': 75.0,
                    'Unit': 'PERCENT'
                }
            }
        }
    ]
}

cluster_response = emr.run_job_flow(
    Name='InstanceGroupsCluster',
    ReleaseLabel='emr-6.15.0',
    Instances={
        'InstanceGroups': instance_groups,
        'Ec2KeyName': 'my-key-pair',
        'KeepJobFlowAliveWhenNoSteps': True
    },
    Applications=[
        {'Name': 'Spark'},
        {'Name': 'Hadoop'},
        {'Name': 'Hive'}
    ],
    LogUri='s3://my-emr-logs/',
    ServiceRole='EMR_DefaultRole',
    JobFlowRole='EMR_EC2_DefaultRole'
)

cluster_id = cluster_response['JobFlowId']
print(f"Cluster created: {cluster_id}")

# Apply auto-scaling to task group
emr.put_auto_scaling_policy(
    ClusterId=cluster_id,
    InstanceGroupId='ig-task',  # Replace with actual instance group ID
    AutoScalingPolicy=auto_scaling_policy
)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Instance Fleets Configuration for Advanced Optimization

aws emr create-cluster \\
    --name "instance-fleets-cluster" \\
    --release-label emr-6.15.0 \\
    --applications Name=Spark Name=Hadoop \\
    --use-default-roles \\
    --instance-fleets '[
        {
            "Name": "PrimaryFleet",
            "InstanceFleetType": "MASTER",
            "TargetOnDemandCapacity": 1,
            "TargetSpotCapacity": 0,
            "InstanceTypeConfigs": [
                {
                    "InstanceType": "m5.xlarge",
                    "WeightedCapacity": 1,
                    "EbsConfiguration": {
                        "EbsBlockDeviceConfigs": [
                            {
                                "VolumeSpecification": {
                                    "VolumeType": "gp3",
                                    "SizeInGB": 100
                                },
                                "VolumesPerInstance": 1
                            }
                        ]
                    }
                },
                {
                    "InstanceType": "m5.2xlarge",
                    "WeightedCapacity": 2
                }
            ]
        },
        {
            "Name": "CoreFleet",
            "InstanceFleetType": "CORE",
            "TargetOnDemandCapacity": 4,
            "TargetSpotCapacity": 4,
            "LaunchSpecifications": {
                "SpotSpecification": {
                    "TimeoutDurationMinutes": 60,
                    "TimeoutAction": "SWITCH_TO_ON_DEMAND",
                    "AllocationStrategy": "diversified"
                },
                "OnDemandSpecification": {
                    "AllocationStrategy": "lowest-price"
                }
            },
            "InstanceTypeConfigs": [
                {
                    "InstanceType": "r5.xlarge",
                    "WeightedCapacity": 2,
                    "BidPriceAsPercentageOfOnDemandPrice": 50
                },
                {
                    "InstanceType": "r5.2xlarge", 
                    "WeightedCapacity": 4,
                    "BidPriceAsPercentageOfOnDemandPrice": 50
                },
                {
                    "InstanceType": "m5.xlarge",
                    "WeightedCapacity": 2,
                    "BidPriceAsPercentageOfOnDemandPrice": 50
                }
            ]
        },
        {
            "Name": "TaskFleet",
            "InstanceFleetType": "TASK", 
            "TargetOnDemandCapacity": 0,
            "TargetSpotCapacity": 10,
            "LaunchSpecifications": {
                "SpotSpecification": {
                    "TimeoutDurationMinutes": 30,
                    "TimeoutAction": "TERMINATE_CLUSTER",
                    "AllocationStrategy": "capacity-optimized"
                }
            },
            "InstanceTypeConfigs": [
                {
                    "InstanceType": "c5.large",
                    "WeightedCapacity": 1,
                    "BidPriceAsPercentageOfOnDemandPrice": 60
                },
                {
                    "InstanceType": "c5.xlarge",
                    "WeightedCapacity": 2,
                    "BidPriceAsPercentageOfOnDemandPrice": 60
                },
                {
                    "InstanceType": "m5.large",
                    "WeightedCapacity": 1,
                    "BidPriceAsPercentageOfOnDemandPrice": 60
                },
                {
                    "InstanceType": "c4.large",
                    "WeightedCapacity": 1,
                    "BidPriceAsPercentageOfOnDemandPrice": 70
                }
            ]
        }
    ]'

# Python Boto3 Instance Fleets Configuration
import boto3

emr = boto3.client('emr')

instance_fleets = [
    {
        'Name': 'PrimaryFleet',
        'InstanceFleetType': 'MASTER',
        'TargetOnDemandCapacity': 1,
        'TargetSpotCapacity': 0,
        'InstanceTypeConfigs': [
            {
                'InstanceType': 'm5.xlarge',
                'WeightedCapacity': 1
            },
            {
                'InstanceType': 'm5.2xlarge', 
                'WeightedCapacity': 2
            }
        ]
    },
    {
        'Name': 'CoreFleet',
        'InstanceFleetType': 'CORE',
        'TargetOnDemandCapacity': 6,
        'TargetSpotCapacity': 6,
        'LaunchSpecifications': {
            'SpotSpecification': {
                'TimeoutDurationMinutes': 60,
                'TimeoutAction': 'SWITCH_TO_ON_DEMAND',
                'AllocationStrategy': 'diversified'
            },
            'OnDemandSpecification': {
                'AllocationStrategy': 'lowest-price'
            }
        },
        'InstanceTypeConfigs': [
            {
                'InstanceType': 'r5.xlarge',
                'WeightedCapacity': 2,
                'BidPriceAsPercentageOfOnDemandPrice': 50
            },
            {
                'InstanceType': 'r5.2xlarge',
                'WeightedCapacity': 4, 
                'BidPriceAsPercentageOfOnDemandPrice': 50
            },
            {
                'InstanceType': 'm5.xlarge',
                'WeightedCapacity': 2,
                'BidPriceAsPercentageOfOnDemandPrice': 50
            }
        ]
    },
    {
        'Name': 'TaskFleet',
        'InstanceFleetType': 'TASK',
        'TargetOnDemandCapacity': 0,
        'TargetSpotCapacity': 20,
        'LaunchSpecifications': {
            'SpotSpecification': {
                'TimeoutDurationMinutes': 30,
                'TimeoutAction': 'TERMINATE_CLUSTER',
                'AllocationStrategy': 'capacity-optimized'
            }
        },
        'InstanceTypeConfigs': [
            {
                'InstanceType': 'c5.large',
                'WeightedCapacity': 1,
                'BidPriceAsPercentageOfOnDemandPrice': 60
            },
            {
                'InstanceType': 'c5.xlarge',
                'WeightedCapacity': 2,
                'BidPriceAsPercentageOfOnDemandPrice': 60
            }
        ]
    }
]

cluster_response = emr.run_job_flow(
    Name='InstanceFleetsCluster',
    ReleaseLabel='emr-6.15.0',
    Instances={
        'InstanceFleets': instance_fleets,
        'Ec2KeyName': 'my-key-pair',
        'KeepJobFlowAliveWhenNoSteps': True
    },
    Applications=[
        {'Name': 'Spark'},
        {'Name': 'Hadoop'}
    ],
    LogUri='s3://my-emr-logs/',
    ServiceRole='EMR_DefaultRole',
    JobFlowRole='EMR_EC2_DefaultRole'
)

print(f"Instance Fleets cluster created: {cluster_response['JobFlowId']}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Advanced Spot Instance Optimization Strategies

import boto3
import json
from datetime import datetime, timedelta

# Spot price analysis and optimization
def analyze_spot_prices(instance_types, availability_zones, days=7):
    """Analyze spot price history to optimize instance selection"""
    
    ec2 = boto3.client('ec2')
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)
    
    spot_prices = {}
    
    for instance_type in instance_types:
        for az in availability_zones:
            try:
                response = ec2.describe_spot_price_history(
                    InstanceTypes=[instance_type],
                    ProductDescriptions=['Linux/UNIX'],
                    AvailabilityZone=az,
                    StartTime=start_time,
                    EndTime=end_time
                )
                
                prices = [float(price['SpotPrice']) for price in response['SpotPriceHistory']]
                if prices:
                    spot_prices[f"{instance_type}-{az}"] = {
                        'avg_price': sum(prices) / len(prices),
                        'min_price': min(prices),
                        'max_price': max(prices),
                        'current_price': prices[0] if prices else None
                    }
            except Exception as e:
                print(f"Error getting spot prices for {instance_type} in {az}: {e}")
    
    return spot_prices

# Optimized Fleet Configuration Based on Spot Analysis
def create_optimized_fleet_config(spot_analysis):
    """Create fleet config optimized for current spot market conditions"""
    
    # Sort instances by average spot price
    sorted_instances = sorted(
        spot_analysis.items(),
        key=lambda x: x[1]['avg_price']
    )
    
    # Core fleet with mixed pricing (conservative)
    core_fleet = {
        'Name': 'OptimizedCoreFleet',
        'InstanceFleetType': 'CORE',
        'TargetOnDemandCapacity': 8,  # Ensure minimum capacity
        'TargetSpotCapacity': 8,      # Additional capacity via spot
        'LaunchSpecifications': {
            'SpotSpecification': {
                'TimeoutDurationMinutes': 120,  # Longer timeout for core nodes
                'TimeoutAction': 'SWITCH_TO_ON_DEMAND',
                'AllocationStrategy': 'diversified'
            }
        },
        'InstanceTypeConfigs': []
    }
    
    # Task fleet - aggressive spot usage (high savings)
    task_fleet = {
        'Name': 'OptimizedTaskFleet',
        'InstanceFleetType': 'TASK',
        'TargetOnDemandCapacity': 0,   # No on-demand for task nodes
        'TargetSpotCapacity': 40,      # All spot capacity
        'LaunchSpecifications': {
            'SpotSpecification': {
                'TimeoutDurationMinutes': 60,
                'TimeoutAction': 'TERMINATE_CLUSTER',
                'AllocationStrategy': 'capacity-optimized'
            }
        },
        'InstanceTypeConfigs': []
    }
    
    # Add instance types with competitive spot pricing
    for instance_az, pricing in sorted_instances[:6]:  # Top 6 cheapest
        instance_type = instance_az.split('-')[0]
        
        # Determine bid price as percentage of on-demand
        bid_percentage = min(80, max(30, int(pricing['avg_price'] * 100)))
        
        instance_config = {
            'InstanceType': instance_type,
            'WeightedCapacity': get_weighted_capacity(instance_type),
            'BidPriceAsPercentageOfOnDemandPrice': bid_percentage
        }
        
        core_fleet['InstanceTypeConfigs'].append(instance_config)
        task_fleet['InstanceTypeConfigs'].append(instance_config)
    
    return [core_fleet, task_fleet]

def get_weighted_capacity(instance_type):
    """Map instance types to weighted capacity based on vCPUs"""
    capacity_map = {
        'c5.large': 1, 'c5.xlarge': 2, 'c5.2xlarge': 4,
        'm5.large': 1, 'm5.xlarge': 2, 'm5.2xlarge': 4,
        'r5.large': 1, 'r5.xlarge': 2, 'r5.2xlarge': 4
    }
    return capacity_map.get(instance_type, 2)

# Spot interruption handling
def setup_spot_interruption_handling():
    """Configure applications to handle spot interruptions gracefully"""
    
    configurations = [
        {
            'Classification': 'spark-defaults',
            'Properties': {
                # Enable checkpointing for fault tolerance
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.adaptive.skewJoin.enabled': 'true',
                
                # Configure dynamic allocation
                'spark.dynamicAllocation.enabled': 'true',
                'spark.dynamicAllocation.minExecutors': '2',
                'spark.dynamicAllocation.maxExecutors': '100',
                'spark.dynamicAllocation.initialExecutors': '10',
                
                # Spot-friendly settings
                'spark.task.maxFailures': '3',
                'spark.stage.maxConsecutiveAttempts': '8',
                'spark.blacklist.enabled': 'true',
                'spark.speculation': 'true'
            }
        },
        {
            'Classification': 'yarn-site',
            'Properties': {
                # Handle node failures gracefully
                'yarn.resourcemanager.am.max-attempts': '4',
                'yarn.nodemanager.disk-health-checker.enable': 'true',
                'yarn.application.classpath': '$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*'
            }
        }
    ]
    
    return configurations

# Monitor spot interruptions
def monitor_spot_interruptions(cluster_id):
    """Monitor and respond to spot instance interruptions"""
    
    emr = boto3.client('emr')
    cloudwatch = boto3.client('cloudwatch')
    
    # Get cluster instances
    response = emr.list_instances(ClusterId=cluster_id)
    
    spot_instances = [
        instance for instance in response['Instances']
        if instance.get('Market') == 'SPOT'
    ]
    
    print(f"Monitoring {len(spot_instances)} spot instances")
    
    # Set up CloudWatch alarms for spot interruptions
    for instance in spot_instances:
        instance_id = instance['Ec2InstanceId']
        
        cloudwatch.put_metric_alarm(
            AlarmName=f'SpotInterruption-{instance_id}',
            ComparisonOperator='GreaterThanThreshold',
            EvaluationPeriods=1,
            MetricName='SpotInstanceTerminating',
            Namespace='AWS/EC2',
            Period=60,
            Statistic='Maximum',
            Threshold=0.0,
            ActionsEnabled=True,
            AlarmActions=[
                'arn:aws:sns:us-west-2:123456789012:spot-interruption-topic'
            ],
            AlarmDescription='Alert on spot instance interruption',
            Dimensions=[
                {
                    'Name': 'InstanceId',
                    'Value': instance_id
                },
            ]
        )

# Example usage
if __name__ == "__main__":
    # Analyze current spot market
    instance_types = ['c5.large', 'c5.xlarge', 'm5.large', 'm5.xlarge', 'r5.large', 'r5.xlarge']
    availability_zones = ['us-west-2a', 'us-west-2b', 'us-west-2c']
    
    spot_analysis = analyze_spot_prices(instance_types, availability_zones)
    
    # Create optimized configuration
    optimized_fleets = create_optimized_fleet_config(spot_analysis)
    
    # Print configuration summary
    print("Optimized Fleet Configuration:")
    print(json.dumps(optimized_fleets, indent=2))
    
    print("\\nSpot Price Analysis:")
    for instance, pricing in spot_analysis.items():
        print(f"{instance}: Avg ${pricing['avg_price']:.4f}, Current ${pricing['current_price']:.4f}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def data_transformation_tab():
    """Content for transforming data in Amazon EMR tab"""
    st.markdown("## 🔄 Transforming Data in Amazon EMR")
    st.markdown("*Step-by-step approach to processing and transforming big data using EMR frameworks*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 EMR Data Processing Workflow
    EMR processes data through orchestrated steps that execute in sequence:
    - **Step-based Processing** - Each step is a unit of work with specific input/output
    - **Distributed Execution** - Work is distributed across cluster nodes for parallel processing
    - **Framework Integration** - Support for Spark, Hadoop MapReduce, Hive, and other frameworks
    - **Error Handling** - Built-in retry logic and failure management
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data processing flow
    st.markdown("#### 🔄 EMR Data Processing Flow")
    common.mermaid(create_data_processing_flow(), height=1300)
    
    # Interactive step builder
    st.markdown("#### 🛠️ Interactive EMR Step Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Processing Configuration")
        processing_framework = st.selectbox("Processing Framework", 
                                          ["Apache Spark", "Hadoop MapReduce", "Apache Hive", "Apache Pig"])
        data_format = st.selectbox("Input Data Format", 
                                 ["Parquet", "JSON", "CSV", "Avro", "ORC"])
        data_size = st.selectbox("Dataset Size", 
                               ["< 1 GB", "1-10 GB", "10-100 GB", "100GB-1TB", "> 1 TB"])
        
    with col2:
        st.markdown("##### Transformation Operations")
        operations = st.multiselect("Select Operations", [
            "Data Cleansing", "Schema Validation", "Data Deduplication",
            "Format Conversion", "Aggregation", "Joins", "Filtering",
            "Data Enrichment", "Partitioning", "Compression"
        ], default=["Data Cleansing", "Aggregation"])
        
        output_format = st.selectbox("Output Format", 
                                   ["Parquet", "Delta Lake", "JSON", "CSV", "Avro"])
    
    # Generate processing recommendation
    processing_config = generate_processing_recommendation(
        processing_framework, data_format, data_size, operations
    )
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Processing Configuration Recommendation
    **Framework**: {processing_framework}  
    **Recommended Cluster Size**: {processing_config['cluster_size']}  
    **Estimated Processing Time**: {processing_config['processing_time']}  
    **Memory Configuration**: {processing_config['memory_config']}  
    **Parallelism Level**: {processing_config['parallelism']}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Step-by-step transformation example
    st.markdown("#### 📋 Step-by-Step Data Transformation Example")
    
    # Create tabs for different transformation steps
    step_tabs = st.tabs(["Step 1: Data Ingestion", "Step 2: Data Validation", "Step 3: Transformation", 
                        "Step 4: Aggregation", "Step 5: Output"])
    
    with step_tabs[0]:
        st.markdown("##### 📥 Data Ingestion from S3")
        st.markdown('<div class="info-box">', unsafe_allow_html=True)
        st.markdown("""
        **Objective**: Load raw data from S3 into EMR for processing
        - **Input**: Raw CSV files in S3 bucket
        - **Processing**: Schema inference and data type detection
        - **Output**: Structured DataFrame ready for transformation
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Step 1: Data Ingestion - PySpark Example
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder \\
    .appName("EMR Data Transformation Pipeline") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .getOrCreate()

# Define schema for better performance (optional but recommended)
sales_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("amount", DecimalType(10,2), True),
    StructField("quantity", IntegerType(), True),
    StructField("store_id", StringType(), True),
    StructField("payment_method", StringType(), True)
])

# Read data from S3 with schema
print("Step 1: Reading data from S3...")
raw_df = spark.read \\
    .option("header", "true") \\
    .option("multiline", "true") \\
    .option("escape", '"') \\
    .schema(sales_schema) \\
    .csv("s3://my-data-lake/raw/sales/2024/")

# Basic data exploration
print(f"Records loaded: {raw_df.count():,}")
print("Schema:")
raw_df.printSchema()

print("Sample data:")
raw_df.show(5, truncate=False)

# Check for null values
null_counts = raw_df.select([
    sum(col(c).isNull().cast("int")).alias(c) 
    for c in raw_df.columns
]).collect()[0].asDict()

print("Null value counts:")
for column, null_count in null_counts.items():
    if null_count > 0:
        print(f"  {column}: {null_count:,}")

# Partition data for better performance in subsequent steps
print("Repartitioning data for optimal processing...")
partitioned_df = raw_df.repartition(200)  # Adjust based on cluster size
partitioned_df.cache()  # Cache for reuse in multiple steps

print("Step 1 completed: Data successfully ingested and partitioned")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with step_tabs[1]:
        st.markdown("##### ✅ Data Validation and Quality Checks")
        st.markdown('<div class="info-box">', unsafe_allow_html=True)
        st.markdown("""
        **Objective**: Validate data quality and identify issues
        - **Input**: Raw DataFrame from Step 1
        - **Processing**: Data quality rules and validation logic
        - **Output**: Clean DataFrame with quality metrics
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Step 2: Data Validation and Quality Checks
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("Step 2: Starting data validation...")

# Use the cached DataFrame from Step 1
validated_df = partitioned_df

# Convert string date to proper date type
validated_df = validated_df.withColumn(
    "transaction_date", 
    to_date(col("transaction_date"), "yyyy-MM-dd")
)

# Data quality checks
print("Performing data quality checks...")

# 1. Check for invalid dates
invalid_dates = validated_df.filter(col("transaction_date").isNull()).count()
print(f"Invalid dates found: {invalid_dates:,}")

# 2. Check for negative amounts
negative_amounts = validated_df.filter(col("amount") < 0).count()
print(f"Negative amounts found: {negative_amounts:,}")

# 3. Check for invalid quantities
invalid_quantities = validated_df.filter(
    (col("quantity") <= 0) | (col("quantity") > 1000)
).count()
print(f"Invalid quantities found: {invalid_quantities:,}")

# 4. Check for duplicate transactions
total_records = validated_df.count()
unique_transactions = validated_df.select("transaction_id").distinct().count()
duplicates = total_records - unique_transactions
print(f"Duplicate transactions found: {duplicates:,}")

# Apply data cleaning rules
print("Applying data cleaning rules...")

# Remove records with critical issues
clean_df = validated_df.filter(
    (col("transaction_date").isNotNull()) &
    (col("amount") >= 0) &
    (col("quantity") > 0) &
    (col("quantity") <= 1000) &
    (col("transaction_id").isNotNull())
)

# Handle missing values with business rules
clean_df = clean_df.fillna({
    "payment_method": "UNKNOWN",
    "store_id": "ONLINE"
})

# Add data quality flags
clean_df = clean_df.withColumn(
    "quality_score",
    when(col("customer_id").isNull(), 0.7)
    .when(col("product_id").isNull(), 0.8)
    .otherwise(1.0)
)

# Remove exact duplicates if any
clean_df = clean_df.dropDuplicates(["transaction_id"])

# Data validation summary
cleaned_count = clean_df.count()
removed_count = total_records - cleaned_count
removal_percentage = (removed_count / total_records) * 100

print("Data validation summary:")
print(f"  Original records: {total_records:,}")
print(f"  Clean records: {cleaned_count:,}")
print(f"  Removed records: {removed_count:,} ({removal_percentage:.2f}%)")

# Cache the cleaned data for next steps
clean_df.cache()
print("Step 2 completed: Data validation and cleaning finished")

# Optional: Write quality report to S3
quality_report = spark.createDataFrame([
    ("total_records", total_records),
    ("clean_records", cleaned_count),
    ("removed_records", removed_count),
    ("invalid_dates", invalid_dates),
    ("negative_amounts", negative_amounts),
    ("invalid_quantities", invalid_quantities),
    ("duplicates", duplicates)
], ["metric", "value"])

quality_report.coalesce(1).write \\
    .mode("overwrite") \\
    .option("header", "true") \\
    .csv("s3://my-data-lake/quality-reports/validation-summary/")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with step_tabs[2]:
        st.markdown("##### 🔧 Data Transformation and Enrichment")
        st.markdown('<div class="info-box">', unsafe_allow_html=True)
        st.markdown("""
        **Objective**: Apply business logic and enrich data
        - **Input**: Clean DataFrame from Step 2
        - **Processing**: Business rules, calculations, and enrichment
        - **Output**: Transformed DataFrame with derived fields
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Step 3: Data Transformation and Enrichment
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F

print("Step 3: Starting data transformation and enrichment...")

# Use the cleaned DataFrame from Step 2
enriched_df = clean_df

# 1. Add time-based derived fields
print("Adding time-based fields...")
enriched_df = enriched_df.withColumn("year", year(col("transaction_date"))) \\
    .withColumn("month", month(col("transaction_date"))) \\
    .withColumn("day_of_week", dayofweek(col("transaction_date"))) \\
    .withColumn("quarter", quarter(col("transaction_date"))) \\
    .withColumn("is_weekend", 
        when(col("day_of_week").isin([1, 7]), True).otherwise(False)
    )

# 2. Calculate business metrics
print("Calculating business metrics...")
enriched_df = enriched_df.withColumn(
    "total_value", col("amount") * col("quantity")
).withColumn(
    "unit_price", col("amount") / col("quantity")
)

# 3. Add customer segmentation (using window functions)
print("Adding customer segmentation...")

# Calculate customer lifetime value and transaction frequency
customer_window = Window.partitionBy("customer_id")

enriched_df = enriched_df.withColumn(
    "customer_total_value", 
    sum("total_value").over(customer_window)
).withColumn(
    "customer_transaction_count",
    count("transaction_id").over(customer_window)
).withColumn(
    "customer_avg_order_value",
    avg("total_value").over(customer_window)
)

# Create customer segments based on behavior
enriched_df = enriched_df.withColumn(
    "customer_segment",
    when(col("customer_total_value") >= 5000, "VIP")
    .when(col("customer_total_value") >= 1000, "Premium")
    .when(col("customer_total_value") >= 300, "Regular")
    .otherwise("New")
)

# 4. Product category analysis
print("Adding product analysis...")

# Assuming we have a product lookup table in S3
product_lookup = spark.read \\
    .option("header", "true") \\
    .csv("s3://my-data-lake/reference/products/") \\
    .select("product_id", "category", "brand", "cost_price", "margin")

# Join with product information
enriched_df = enriched_df.join(
    product_lookup, 
    on="product_id", 
    how="left"
).fillna({
    "category": "Unknown",
    "brand": "Unknown", 
    "cost_price": 0,
    "margin": 0.2
})

# Calculate profit metrics
enriched_df = enriched_df.withColumn(
    "estimated_profit", 
    col("total_value") * col("margin")
).withColumn(
    "profit_margin_category",
    when(col("margin") >= 0.4, "High Margin")
    .when(col("margin") >= 0.2, "Medium Margin")
    .otherwise("Low Margin")
)

# 5. Store performance metrics
print("Adding store performance metrics...")

store_window = Window.partitionBy("store_id", "transaction_date")

enriched_df = enriched_df.withColumn(
    "daily_store_revenue",
    sum("total_value").over(store_window)
).withColumn(
    "daily_store_transactions",
    count("transaction_id").over(store_window)
)

# 6. Payment method analysis
print("Adding payment method insights...")
enriched_df = enriched_df.withColumn(
    "payment_category",
    when(col("payment_method").isin(["CREDIT_CARD", "DEBIT_CARD"]), "Card")
    .when(col("payment_method") == "CASH", "Cash")
    .when(col("payment_method").isin(["PAYPAL", "APPLE_PAY", "GOOGLE_PAY"]), "Digital")
    .otherwise("Other")
)

# 7. Add data lineage and processing metadata
print("Adding processing metadata...")
enriched_df = enriched_df.withColumn(
    "processing_timestamp", current_timestamp()
).withColumn(
    "processing_date", current_date()
).withColumn(
    "data_source", lit("S3_RAW_SALES")
).withColumn(
    "processing_version", lit("v1.0")
)

# Check the enriched data
print("Transformation summary:")
print(f"  Records processed: {enriched_df.count():,}")
print(f"  Columns after enrichment: {len(enriched_df.columns)}")

# Show sample of enriched data
print("Sample enriched data:")
enriched_df.select(
    "transaction_id", "customer_segment", "category", 
    "total_value", "estimated_profit", "quarter", "is_weekend"
).show(5)

# Cache the enriched data for aggregation step
enriched_df.cache()
print("Step 3 completed: Data transformation and enrichment finished")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with step_tabs[3]:
        st.markdown("##### 📊 Data Aggregation and Analytics")
        st.markdown('<div class="info-box">', unsafe_allow_html=True)
        st.markdown("""
        **Objective**: Create analytical summaries and aggregations
        - **Input**: Enriched DataFrame from Step 3
        - **Processing**: Group by operations and analytical functions
        - **Output**: Aggregated datasets for different business needs
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Step 4: Data Aggregation and Analytics
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("Step 4: Starting data aggregation and analytics...")

# Use the enriched DataFrame from Step 3
source_df = enriched_df

# 1. Daily Sales Summary
print("Creating daily sales summary...")
daily_summary = source_df.groupBy("transaction_date", "store_id") \\
    .agg(
        sum("total_value").alias("daily_revenue"),
        count("transaction_id").alias("transaction_count"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("total_value").alias("avg_order_value"),
        sum("estimated_profit").alias("daily_profit"),
        avg("estimated_profit").alias("avg_profit_per_transaction")
    ) \\
    .withColumn("profit_margin_pct", 
        round((col("daily_profit") / col("daily_revenue")) * 100, 2)
    ) \\
    .orderBy("transaction_date", "store_id")

print(f"Daily summary records: {daily_summary.count():,}")

# 2. Customer Segment Analysis
print("Creating customer segment analysis...")
customer_segment_summary = source_df.groupBy("customer_segment", "month", "year") \\
    .agg(
        countDistinct("customer_id").alias("customer_count"),
        sum("total_value").alias("segment_revenue"),
        avg("total_value").alias("avg_order_value"),
        sum("quantity").alias("total_items_sold"),
        avg("customer_total_value").alias("avg_customer_lifetime_value")
    ) \\
    .withColumn("revenue_per_customer",
        round(col("segment_revenue") / col("customer_count"), 2)
    ) \\
    .orderBy("year", "month", "customer_segment")

print(f"Customer segment records: {customer_segment_summary.count():,}")

# 3. Product Category Performance
print("Creating product category analysis...")
category_performance = source_df.groupBy("category", "brand", "quarter", "year") \\
    .agg(
        sum("total_value").alias("category_revenue"),
        sum("quantity").alias("units_sold"),
        sum("estimated_profit").alias("category_profit"),
        countDistinct("customer_id").alias("unique_customers"),
        count("transaction_id").alias("transaction_count"),
        avg("unit_price").alias("avg_unit_price")
    ) \\
    .withColumn("profit_margin_pct",
        round((col("category_profit") / col("category_revenue")) * 100, 2)
    ) \\
    .withColumn("revenue_per_customer",
        round(col("category_revenue") / col("unique_customers"), 2)
    ) \\
    .orderBy("year", "quarter", desc("category_revenue"))

print(f"Category performance records: {category_performance.count():,}")

# 4. Payment Method Trends
print("Creating payment method analysis...")
payment_trends = source_df.groupBy("payment_category", "month", "year", "customer_segment") \\
    .agg(
        count("transaction_id").alias("transaction_count"),
        sum("total_value").alias("payment_method_revenue"),
        avg("total_value").alias("avg_transaction_value"),
        countDistinct("customer_id").alias("unique_customers")
    ) \\
    .withColumn("transactions_per_customer",
        round(col("transaction_count") / col("unique_customers"), 2)
    ) \\
    .orderBy("year", "month", "payment_category")

print(f"Payment trends records: {payment_trends.count():,}")

# 5. Weekend vs Weekday Analysis
print("Creating weekend vs weekday analysis...")
weekend_analysis = source_df.groupBy("is_weekend", "store_id", "month", "year") \\
    .agg(
        sum("total_value").alias("revenue"),
        count("transaction_id").alias("transaction_count"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("total_value").alias("avg_order_value")
    ) \\
    .withColumn("day_type", 
        when(col("is_weekend"), "Weekend").otherwise("Weekday")
    ) \\
    .orderBy("year", "month", "store_id", "is_weekend")

print(f"Weekend analysis records: {weekend_analysis.count():,}")

# 6. Top Performers Analysis
print("Creating top performers analysis...")

# Top customers by revenue
top_customers = source_df.groupBy("customer_id", "customer_segment") \\
    .agg(
        sum("total_value").alias("total_spent"),
        count("transaction_id").alias("total_transactions"),
        max("transaction_date").alias("last_purchase_date"),
        countDistinct("product_id").alias("unique_products_bought"),
        avg("total_value").alias("avg_order_value")
    ) \\
    .withColumn("days_since_last_purchase",
        datediff(current_date(), col("last_purchase_date"))
    ) \\
    .orderBy(desc("total_spent")) \\
    .limit(1000)

# Top products by revenue  
top_products = source_df.groupBy("product_id", "category", "brand") \\
    .agg(
        sum("total_value").alias("total_revenue"),
        sum("quantity").alias("total_quantity_sold"),
        sum("estimated_profit").alias("total_profit"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("unit_price").alias("avg_selling_price")
    ) \\
    .withColumn("profit_margin_pct",
        round((col("total_profit") / col("total_revenue")) * 100, 2)
    ) \\
    .orderBy(desc("total_revenue")) \\
    .limit(500)

print(f"Top customers: {top_customers.count():,}")
print(f"Top products: {top_products.count():,}")

# 7. Monthly Cohort Analysis (simplified)
print("Creating cohort analysis...")
cohort_data = source_df.withColumn("customer_first_purchase_month",
    date_format(
        first(col("transaction_date")).over(
            Window.partitionBy("customer_id").orderBy("transaction_date")
        ), "yyyy-MM"
    )
).withColumn("transaction_month", 
    date_format(col("transaction_date"), "yyyy-MM")
)

cohort_summary = cohort_data.groupBy("customer_first_purchase_month", "transaction_month") \\
    .agg(
        countDistinct("customer_id").alias("active_customers"),
        sum("total_value").alias("cohort_revenue")
    ) \\
    .orderBy("customer_first_purchase_month", "transaction_month")

print(f"Cohort analysis records: {cohort_summary.count():,}")

# Display sample results
print("\\nSample aggregated data:")
print("Daily Summary (last 5 days):")
daily_summary.orderBy(desc("transaction_date")).show(5)

print("\\nCustomer Segment Summary:")
customer_segment_summary.show(10)

# Cache aggregated results for output step
daily_summary.cache()
customer_segment_summary.cache()
category_performance.cache()
payment_trends.cache()
weekend_analysis.cache()
top_customers.cache()
top_products.cache()
cohort_summary.cache()

print("Step 4 completed: Data aggregation and analytics finished")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with step_tabs[4]:
        st.markdown("##### 📤 Data Output and Storage")
        st.markdown('<div class="info-box">', unsafe_allow_html=True)
        st.markdown("""
        **Objective**: Store processed data for consumption by analytics tools
        - **Input**: Aggregated DataFrames from Step 4
        - **Processing**: Partitioning, compression, and format optimization
        - **Output**: Analytics-ready datasets in S3 data lake
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Step 5: Data Output and Storage
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("Step 5: Starting data output and storage...")

# Define output configuration
output_base_path = "s3://my-data-lake/processed/sales-analytics/"
current_date = date.today().strftime("%Y-%m-%d")

# 1. Write Daily Sales Summary
print("Writing daily sales summary...")
daily_summary.coalesce(10) \\  # Optimize file size
    .write \\
    .mode("overwrite") \\
    .partitionBy("transaction_date") \\
    .parquet(f"{output_base_path}daily-summary/")

# Also create a CSV version for easy consumption
daily_summary.coalesce(1) \\
    .write \\
    .mode("overwrite") \\
    .option("header", "true") \\
    .csv(f"{output_base_path}daily-summary-csv/")

print("Daily summary written successfully")

# 2. Write Customer Segment Analysis
print("Writing customer segment analysis...")
customer_segment_summary.coalesce(5) \\
    .write \\
    .mode("overwrite") \\
    .partitionBy("year", "customer_segment") \\
    .parquet(f"{output_base_path}customer-segments/")

# Create aggregated view for dashboards
customer_segment_current = customer_segment_summary \\
    .filter(col("year") == year(current_date())) \\
    .groupBy("customer_segment") \\
    .agg(
        sum("customer_count").alias("total_customers"),
        sum("segment_revenue").alias("total_revenue"),
        avg("avg_order_value").alias("overall_avg_order_value")
    )

customer_segment_current.coalesce(1) \\
    .write \\
    .mode("overwrite") \\
    .option("header", "true") \\
    .csv(f"{output_base_path}customer-segments-current/")

print("Customer segment analysis written successfully")

# 3. Write Product Category Performance
print("Writing product category performance...")
category_performance.coalesce(8) \\
    .write \\
    .mode("overwrite") \\
    .partitionBy("year", "quarter") \\
    .parquet(f"{output_base_path}category-performance/")

# Create top categories summary
top_categories = category_performance \\
    .filter(col("year") == year(current_date())) \\
    .groupBy("category") \\
    .agg(
        sum("category_revenue").alias("total_revenue"),
        sum("units_sold").alias("total_units"),
        avg("profit_margin_pct").alias("avg_profit_margin")
    ) \\
    .orderBy(desc("total_revenue")) \\
    .limit(20)

top_categories.coalesce(1) \\
    .write \\
    .mode("overwrite") \\
    .option("header", "true") \\
    .csv(f"{output_base_path}top-categories/")

print("Category performance written successfully")

# 4. Write Payment Trends
print("Writing payment trends...")
payment_trends.coalesce(5) \\
    .write \\
    .mode("overwrite") \\
    .partitionBy("year", "month") \\
    .parquet(f"{output_base_path}payment-trends/")

print("Payment trends written successfully")

# 5. Write Weekend Analysis
print("Writing weekend analysis...")
weekend_analysis.coalesce(3) \\
    .write \\
    .mode("overwrite") \\
    .partitionBy("year", "month") \\
    .parquet(f"{output_base_path}weekend-analysis/")

print("Weekend analysis written successfully")

# 6. Write Top Performers
print("Writing top performers...")

# Top customers
top_customers.coalesce(2) \\
    .write \\
    .mode("overwrite") \\
    .parquet(f"{output_base_path}top-customers/")

# Top products  
top_products.coalesce(2) \\
    .write \\
    .mode("overwrite") \\
    .parquet(f"{output_base_path}top-products/")

print("Top performers written successfully")

# 7. Write Cohort Analysis
print("Writing cohort analysis...")
cohort_summary.coalesce(5) \\
    .write \\
    .mode("overwrite") \\
    .parquet(f"{output_base_path}cohort-analysis/")

print("Cohort analysis written successfully")

# 8. Create Data Catalog entries (Hive/Glue tables)
print("Creating external tables for data catalog...")

# Create external table for daily summary
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS daily_sales_summary (
        transaction_date date,
        store_id string,
        daily_revenue decimal(12,2),
        transaction_count bigint,
        unique_customers bigint,
        avg_order_value decimal(10,2),
        daily_profit decimal(12,2),
        avg_profit_per_transaction decimal(10,2),
        profit_margin_pct decimal(5,2)
    )
    USING PARQUET
    PARTITIONED BY (transaction_date)
    LOCATION '{output_base_path}daily-summary/'
""")

# Repair partitions to make data discoverable
spark.sql("MSCK REPAIR TABLE daily_sales_summary")

# Create external table for customer segments
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS customer_segment_analysis (
        customer_segment string,
        month int,
        year int,
        customer_count bigint,
        segment_revenue decimal(15,2),
        avg_order_value decimal(10,2),
        total_items_sold bigint,
        avg_customer_lifetime_value decimal(12,2),
        revenue_per_customer decimal(10,2)
    )
    USING PARQUET
    PARTITIONED BY (year, customer_segment)
    LOCATION '{output_base_path}customer-segments/'
""")

spark.sql("MSCK REPAIR TABLE customer_segment_analysis")

print("External tables created successfully")

# 9. Generate processing summary report
print("Generating processing summary report...")

processing_summary = spark.createDataFrame([
    ("daily_summary", daily_summary.count()),
    ("customer_segments", customer_segment_summary.count()),
    ("category_performance", category_performance.count()), 
    ("payment_trends", payment_trends.count()),
    ("weekend_analysis", weekend_analysis.count()),
    ("top_customers", top_customers.count()),
    ("top_products", top_products.count()),
    ("cohort_analysis", cohort_summary.count())
], ["dataset", "record_count"])

processing_summary = processing_summary.withColumn(
    "processing_timestamp", current_timestamp()
).withColumn(
    "processing_date", lit(current_date)
)

processing_summary.coalesce(1) \\
    .write \\
    .mode("overwrite") \\
    .option("header", "true") \\
    .csv(f"{output_base_path}processing-summary/")

print("Processing summary report generated")

# 10. Clean up cached DataFrames
print("Cleaning up cached data...")
spark.catalog.clearCache()

# Final processing statistics
print("\\n" + "="*60)
print("EMR DATA PROCESSING PIPELINE COMPLETED SUCCESSFULLY")
print("="*60)
print(f"Processing Date: {current_date}")
print(f"Output Location: {output_base_path}")
print("\\nDatasets Created:")
processing_summary.show(truncate=False)

print("\\nAll data has been processed and stored in S3 data lake")
print("External tables created in Hive/Glue catalog for query access")
print("Data is ready for consumption by BI tools and analytics applications")

# Stop Spark session
spark.stop()
print("Step 5 completed: Data output and storage finished")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Performance optimization tips
    st.markdown("#### ⚡ EMR Performance Optimization Tips")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Best Practices
        
        **Cluster Configuration:**
        - Use appropriate instance types for workload
        - Enable auto-scaling for variable workloads
        - Configure spot instances for cost savings
        
        **Data Processing:**
        - Partition data appropriately
        - Use columnar formats (Parquet, ORC)
        - Cache frequently accessed data
        - Optimize join strategies
        
        **Spark Optimization:**
        - Enable Adaptive Query Execution (AQE)
        - Tune executor memory and cores
        - Use broadcast joins for small tables
        - Persist intermediate results
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="warning-box">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚠️ Common Pitfalls
        
        **Resource Issues:**
        - Under-provisioned memory causing OOM errors
        - Too many small files degrading performance
        - Inadequate parallelism for large datasets
        
        **Data Issues:**
        - Skewed data causing hot spots
        - Unnecessary data shuffling
        - Reading entire datasets when filtering early
        
        **Configuration Issues:**
        - Default Spark configurations
        - Not using compression
        - Inadequate partitioning strategies
        - Missing statistics for optimization
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Save processing job
    if st.button("🚀 Save Processing Configuration"):
        job_config = {
            'framework': processing_framework,
            'operations': operations,
            'data_size': data_size,
            'estimated_time': processing_config['processing_time']
        }
        st.session_state.processing_jobs += 1
        st.success(f"Processing job configuration saved! Total jobs: {st.session_state.processing_jobs}")

def emr_vs_glue_tab():
    """Content for EMR vs Glue ETL comparison tab"""
    st.markdown("## ⚖️ Considerations in Choosing Spark on Amazon EMR or AWS Glue ETL")
    st.markdown("*Make informed decisions between EMR and Glue based on your specific requirements*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Decision Framework
    Choose between EMR and Glue ETL based on these key factors:
    - **Responsibility Model** - How much control and management do you want?
    - **Flexibility vs Simplicity** - Do you need custom configurations or prefer automation?
    - **Cost Model** - Consistent workloads vs variable usage patterns
    - **Technical Expertise** - Team experience with big data technologies
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive decision tree
    st.markdown("#### 🌳 Interactive Decision Tree")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Requirements Assessment")
        workload_type = st.selectbox("Workload Type", [
            "Batch ETL processing", "Real-time streaming", "Interactive analytics", 
            "Machine learning training", "Ad-hoc data exploration"
        ])
        
        data_volume = st.selectbox("Data Volume", [
            "< 100 GB", "100 GB - 1 TB", "1 TB - 10 TB", "> 10 TB"
        ])
        
        processing_frequency = st.selectbox("Processing Frequency", [
            "One-time", "Daily", "Hourly", "Continuous streaming", "On-demand"
        ])
        
        team_expertise = st.selectbox("Team Expertise Level", [
            "Beginners", "Intermediate", "Advanced Spark users", "Big data experts"
        ])
    
    with col2:
        st.markdown("##### Technical Preferences")
        management_preference = st.selectbox("Management Preference", [
            "Fully managed/serverless", "Some configuration control", 
            "Full infrastructure control", "Flexible based on needs"
        ])
        
        customization_needs = st.selectbox("Customization Requirements", [
            "Standard ETL only", "Some custom logic", "Heavy customization", "Framework flexibility"
        ])
        
        cost_priority = st.selectbox("Cost Priority", [
            "Minimize operational overhead", "Optimize for usage-based costs", 
            "Predictable monthly costs", "Maximum cost efficiency"
        ])
        
        timeline_urgency = st.selectbox("Implementation Timeline", [
            "Quick prototype needed", "Standard project timeline", "Long-term strategic solution"
        ])
    
    # Generate recommendation
    recommendation = generate_emr_glue_recommendation(
        workload_type, data_volume, processing_frequency, team_expertise,
        management_preference, customization_needs, cost_priority
    )
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Recommendation: {recommendation['choice']}
    **Confidence**: {recommendation['confidence']}%  
    **Primary Reasons**: {', '.join(recommendation['reasons'])}  
    **Estimated Monthly Cost**: {recommendation['cost_estimate']}  
    **Implementation Effort**: {recommendation['implementation_effort']}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Detailed comparison table
    st.markdown("#### 📋 Detailed Feature Comparison")
    
    # Create comprehensive comparison
    comparison_categories = {
        "Infrastructure & Management": {
            "EMR": "User manages cluster lifecycle, scaling, patches",
            "Glue": "Fully managed, serverless, auto-scaling"
        },
        "Pricing Model": {
            "EMR": "Instance hours + EMR service fees",
            "Glue": "Pay per DPU hour consumed"
        },
        "Startup Time": {
            "EMR": "5-10 minutes cluster startup",
            "Glue": "< 1 minute job startup"
        },
        "Flexibility": {
            "EMR": "Full Spark configuration control",
            "Glue": "Limited configuration options"
        },
        "Custom Libraries": {
            "EMR": "Install any library/framework",
            "Glue": "Pre-installed libraries + limited custom"
        },
        "Data Catalog": {
            "EMR": "Optional integration with Glue Catalog",
            "Glue": "Native Glue Catalog integration"
        },
        "Monitoring": {
            "EMR": "CloudWatch + Spark UI + custom tools",
            "Glue": "CloudWatch + Glue console metrics"
        },
        "IDE Support": {
            "EMR": "Jupyter, Zeppelin, any IDE",
            "Glue": "Glue Studio, notebooks, dev endpoints"
        }
    }
    
    # Display comparison in tabs
    comp_tabs = st.tabs(list(comparison_categories.keys()))
    
    for i, (category, comparisons) in enumerate(comparison_categories.items()):
        with comp_tabs[i]:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown('<div class="node-card">', unsafe_allow_html=True)
                st.markdown(f"""
                ### 🐘 Amazon EMR
                {comparisons['EMR']}
                """)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="node-card">', unsafe_allow_html=True)
                st.markdown(f"""
                ### 🔧 AWS Glue ETL
                {comparisons['Glue']}
                """)
                st.markdown('</div>', unsafe_allow_html=True)
    
    # Use case scenarios
    st.markdown("#### 🎯 Use Case Scenarios")
    
    scenario_col1, scenario_col2 = st.columns(2)
    
    with scenario_col1:
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🐘 Choose EMR When:
        
        **Complex Analytics & ML:**
        - Advanced Spark configurations needed
        - Custom machine learning pipelines
        - Multiple processing frameworks (Spark, Hive, Presto)
        
        **Long-Running Workloads:**
        - Clusters running for hours/days
        - Interactive data science workloads
        - Cost advantages for sustained usage
        
        **Maximum Flexibility:**
        - Custom libraries and dependencies
        - Specific OS or runtime requirements
        - Integration with non-AWS tools
        
        **Expert Teams:**
        - Experienced Spark developers
        - Need full infrastructure control
        - Custom monitoring and optimization
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with scenario_col2:
        st.markdown('<div class="info-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔧 Choose Glue When:
        
        **Simple ETL Jobs:**
        - Standard data transformations
        - Schema evolution and cataloging
        - Quick development and deployment
        
        **Event-Driven Processing:**
        - Triggered by S3 events
        - Irregular processing schedules
        - Short-running jobs (< 1 hour)
        
        **Minimal Operations:**
        - Limited DevOps resources
        - Focus on business logic not infrastructure
        - Built-in governance and lineage
        
        **AWS-Native Integration:**
        - Heavy use of AWS services
        - Data catalog requirements
        - Simplified architecture
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Cost analysis
    st.markdown("#### 💰 Cost Analysis Scenarios")
    
    # Interactive cost calculator
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Workload Parameters")
        job_duration_hours = st.slider("Job Duration (hours)", 0.1, 24.0, 2.0, 0.1)
        jobs_per_month = st.slider("Jobs per Month", 1, 1000, 30)
        data_processed_gb = st.slider("Data Processed per Job (GB)", 1, 10000, 100)
    
    with col2:
        st.markdown("##### Cost Comparison")
        
        # Calculate costs
        emr_cost = calculate_emr_monthly_cost(job_duration_hours, jobs_per_month)
        glue_cost = calculate_glue_monthly_cost(job_duration_hours, jobs_per_month, data_processed_gb)
        
        st.metric("EMR Monthly Cost", f"${emr_cost:.2f}")
        st.metric("Glue Monthly Cost", f"${glue_cost:.2f}")
        
        savings = abs(emr_cost - glue_cost)
        cheaper_option = "EMR" if emr_cost < glue_cost else "Glue"
        st.metric(f"{cheaper_option} Savings", f"${savings:.2f}")
    
    # Create cost comparison chart
    fig = go.Figure(data=[
        go.Bar(name='Amazon EMR', x=['Infrastructure', 'Service Fee', 'Storage'], 
               y=[emr_cost * 0.7, emr_cost * 0.2, emr_cost * 0.1]),
        go.Bar(name='AWS Glue', x=['Compute (DPU)', 'Data Catalog', 'Storage'], 
               y=[glue_cost * 0.8, glue_cost * 0.1, glue_cost * 0.1])
    ])
    
    fig.update_layout(
        title='Monthly Cost Breakdown',
        xaxis_title='Cost Components',
        yaxis_title='Cost ($)',
        barmode='group'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Migration considerations
    st.markdown("#### 🔄 Migration Considerations")
    
    migration_tabs = st.tabs(["EMR to Glue", "Glue to EMR", "Hybrid Approach"])
    
    with migration_tabs[0]:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Migrating from EMR to AWS Glue ETL

# Original EMR Spark job
# spark-submit --class MyETLJob \\
#   --master yarn \\
#   --deploy-mode cluster \\
#   my-etl-job.jar

# 1. Convert to Glue-compatible PySpark
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])
job.init(args['JOB_NAME'], args)

# Read from Glue Catalog instead of direct S3
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="my_database",
    table_name="raw_data_table"
)

# Convert to Spark DataFrame
df = datasource.toDF()

# Apply transformations (similar to EMR)
transformed_df = df.filter(df.amount > 0) \\
    .groupBy("customer_id", "date") \\
    .agg({"amount": "sum", "quantity": "count"})

# Convert back to Glue DynamicFrame
dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_data")

# Write to S3 with Glue catalog update
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": args['output_path'],
        "partitionKeys": ["date"]
    },
    format="parquet"
)

job.commit()

# Migration checklist:
# ✓ Replace custom libraries with Glue-supported ones
# ✓ Use Glue Catalog instead of Hive metastore
# ✓ Convert Scala/Java to PySpark if needed
# ✓ Adjust memory and configuration for Glue limits
# ✓ Update scheduling from EMR steps to Glue triggers
# ✓ Modify monitoring from EMR logs to CloudWatch
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with migration_tabs[1]:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Migrating from AWS Glue to EMR (when you need more control)

# Reasons for migration:
# - Need custom Spark configurations
# - Require additional libraries not available in Glue
# - Long-running interactive workloads
# - Cost optimization for sustained usage

# 1. Convert Glue job to standard PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session with custom configuration
spark = SparkSession.builder \\
    .appName("Migrated from Glue") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
    .config("spark.sql.parquet.compression.codec", "snappy") \\
    .getOrCreate()

# Read data directly from S3 (instead of Glue Catalog)
df = spark.read.parquet("s3://my-bucket/input/")

# Apply same transformations
transformed_df = df.filter(df.amount > 0) \\
    .groupBy("customer_id", "date") \\
    .agg(sum("amount").alias("total_amount"), 
         count("quantity").alias("transaction_count"))

# Write results
transformed_df.write \\
    .mode("overwrite") \\
    .partitionBy("date") \\
    .parquet("s3://my-bucket/output/")

spark.stop()

# EMR cluster configuration for Glue migration
aws emr create-cluster \\
    --name "glue-migration-cluster" \\
    --release-label emr-6.15.0 \\
    --applications Name=Spark \\
    --instance-groups '[
        {
            "Name": "Primary",
            "InstanceRole": "MASTER", 
            "InstanceType": "m5.xlarge",
            "InstanceCount": 1
        },
        {
            "Name": "Core",
            "InstanceRole": "CORE",
            "InstanceType": "r5.xlarge", 
            "InstanceCount": 3
        }
    ]' \\
    --configurations '[
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            }
        }
    ]' \\
    --auto-scaling-role EMR_AutoScaling_DefaultRole \\
    --service-role EMR_DefaultRole \\
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole

# Migration benefits:
# ✓ Custom Spark configurations
# ✓ Additional libraries and frameworks  
# ✓ Long-running clusters for interactive work
# ✓ Cost savings for sustained workloads
# ✓ Full infrastructure control
        ''', language='bash')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with migration_tabs[2]:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Hybrid Approach: Using EMR and Glue Together

# Strategy: Use each service for its strengths
# - Glue for simple, regular ETL jobs
# - EMR for complex analytics and ML workloads
# - Shared data catalog and S3 storage

# Example Architecture:

# 1. Glue ETL for data ingestion and cleaning
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Daily ETL job in Glue (cost-effective for short runs)
glueContext = GlueContext(SparkContext())

# Read raw data
raw_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://raw-data/daily/"]},
    format="json"
)

# Basic cleaning and validation
cleaned_data = raw_data.filter(lambda x: x["amount"] is not None and x["amount"] > 0)

# Write cleaned data for EMR processing
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_data,
    connection_type="s3", 
    connection_options={"path": "s3://processed-data/clean/"},
    format="parquet"
)

# 2. EMR for complex analytics (long-running cluster)
# PySpark job on EMR for advanced analytics
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder \\
    .appName("Advanced Analytics on EMR") \\
    .getOrCreate()

# Read cleaned data from Glue ETL output
clean_df = spark.read.parquet("s3://processed-data/clean/")

# Advanced analytics (customer segmentation)
feature_cols = ["total_spend", "frequency", "recency"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
feature_df = assembler.transform(clean_df)

kmeans = KMeans(k=5, seed=42)
model = kmeans.fit(feature_df)
segmented_df = model.transform(feature_df)

# Write results back to shared storage
segmented_df.write \\
    .mode("overwrite") \\
    .parquet("s3://analytics-results/customer-segments/")

# 3. Orchestration with Step Functions
{
    "Comment": "Hybrid ETL Pipeline",
    "StartAt": "GlueETLJob",
    "States": {
        "GlueETLJob": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": "daily-data-cleaning"
            },
            "Next": "CheckEMRCluster"
        },
        "CheckEMRCluster": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.JobRunState",
                    "StringEquals": "SUCCEEDED",
                    "Next": "EMRAnalyticsStep"
                }
            ],
            "Default": "FailureHandler"
        },
        "EMRAnalyticsStep": {
            "Type": "Task",
            "Resource": "arn:aws:states:::elasticmapreduce:addStep.sync",
            "Parameters": {
                "ClusterId": "j-CLUSTERID",
                "Step": {
                    "Name": "Advanced Analytics",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "spark-submit",
                            "s3://scripts/advanced-analytics.py"
                        ]
                    }
                }
            },
            "End": true
        }
    }
}

# Benefits of hybrid approach:
# ✓ Cost optimization - right tool for each job
# ✓ Operational simplicity - Glue for routine tasks
# ✓ Flexibility - EMR for complex requirements  
# ✓ Shared catalog and storage layer
# ✓ Gradual migration path between services

# Shared infrastructure components:
# - S3 data lake for all data storage
# - Glue Data Catalog for metadata management
# - IAM roles and policies for access control
# - CloudWatch for monitoring both services
# - Step Functions for orchestration
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

# Helper functions for calculations and recommendations

def calculate_emr_cost(primary_type, core_type, core_count, task_count, pricing_model):
    """Calculate estimated EMR cost per hour"""
    # Sample pricing (actual prices vary by region)
    instance_prices = {
        'm5.xlarge': 0.192, 'm5.2xlarge': 0.384, 'm5.4xlarge': 0.768,
        'r5.xlarge': 0.252, 'r5.2xlarge': 0.504, 'c5.large': 0.085,
        'c5.xlarge': 0.17, 'm5.large': 0.096
    }
    
    emr_service_fee = 0.027  # per instance hour
    
    primary_cost = instance_prices.get(primary_type, 0.2) + emr_service_fee
    core_cost = (instance_prices.get(core_type, 0.25) + emr_service_fee) * core_count
    task_cost = (instance_prices.get('c5.xlarge', 0.17) + emr_service_fee) * task_count
    
    if pricing_model == "Spot":
        core_cost *= 0.4  # 60% savings for spot
        task_cost *= 0.3  # 70% savings for spot
    
    return primary_cost + core_cost + task_cost

def get_storage_recommendation(read_pattern, durability, latency, cost_sensitivity):
    """Generate storage strategy recommendation"""
    
    if read_pattern == "Multiple iterations" and latency == "Low latency critical":
        return {
            'primary': 'HDFS',
            'secondary': 'EMRFS backup',
            'replication': '3x local replication',
            'performance': 'Optimized for speed'
        }
    elif cost_sensitivity == "Cost optimization critical":
        return {
            'primary': 'EMRFS',
            'secondary': 'S3 lifecycle policies',
            'replication': 'S3 built-in durability',
            'performance': 'Cost-optimized'
        }
    else:
        return {
            'primary': 'Hybrid (HDFS + EMRFS)',
            'secondary': 'S3 for durability',
            'replication': 'Mixed strategy',
            'performance': 'Balanced'
        }

def calculate_storage_cost(data_size_tb, recommendation):
    """Calculate monthly storage cost"""
    base_cost_per_tb = 23  # S3 Standard
    
    if recommendation['primary'] == 'HDFS':
        # EBS storage cost (3x replication)
        return data_size_tb * 3 * 100  # $100/TB/month for EBS
    elif recommendation['primary'] == 'EMRFS':
        return data_size_tb * base_cost_per_tb
    else:
        return data_size_tb * (base_cost_per_tb + 50)  # Hybrid cost

def calculate_instance_groups_cost(primary_type, primary_pricing, core_type, core_count, 
                                core_pricing, task_type, task_count, task_pricing):
    """Calculate instance groups configuration cost"""
    
    instance_prices = {
        'm5.xlarge': 0.192, 'm5.2xlarge': 0.384, 'm5.4xlarge': 0.768,
        'r5.xlarge': 0.252, 'r5.2xlarge': 0.504, 'c5.large': 0.085,
        'c5.xlarge': 0.17, 'm5.large': 0.096
    }
    
    emr_fee = 0.027
    
    primary_cost = instance_prices.get(primary_type, 0.2) + emr_fee
    if primary_pricing == "Spot":
        primary_cost *= 0.4
    
    core_cost = (instance_prices.get(core_type, 0.25) + emr_fee) * core_count
    if core_pricing == "Spot":
        core_cost *= 0.4
    
    task_cost = 0
    if task_type and task_count > 0:
        task_cost = (instance_prices.get(task_type, 0.17) + emr_fee) * task_count
        if task_pricing == "Spot":
            task_cost *= 0.3
    
    return primary_cost + core_cost + task_cost

def calculate_instance_fleets_cost(primary_types_count, core_capacity, core_ondemand_pct, 
                                task_capacity, task_ondemand_pct):
    """Calculate instance fleets configuration cost"""
    
    base_core_cost = core_capacity * 0.15  # Average vCPU cost
    core_spot_savings = (100 - core_ondemand_pct) / 100 * 0.6
    core_cost = base_core_cost * (1 - core_spot_savings)
    
    base_task_cost = task_capacity * 0.12  # Task-optimized vCPU cost
    task_spot_savings = (100 - task_ondemand_pct) / 100 * 0.7
    task_cost = base_task_cost * (1 - task_spot_savings)
    
    primary_cost = 0.22  # Fixed primary cost
    
    return primary_cost + core_cost + task_cost

def calculate_spot_savings(core_ondemand_pct, task_ondemand_pct):
    """Calculate overall spot savings percentage"""
    avg_spot_usage = (100 - (core_ondemand_pct + task_ondemand_pct) / 2)
    return avg_spot_usage * 0.65  # Average 65% savings with spot

def generate_processing_recommendation(framework, data_format, data_size, operations):
    """Generate processing configuration recommendation"""
    
    size_multipliers = {
        "< 1 GB": 1, "1-10 GB": 2, "10-100 GB": 5, "100GB-1TB": 10, "> 1 TB": 20
    }
    
    complexity_score = len(operations) * size_multipliers.get(data_size, 1)
    
    if complexity_score < 5:
        cluster_size = "Small (3 nodes)"
        processing_time = "< 30 minutes"
        memory_config = "Standard (8GB executors)"
        parallelism = "Low (50 partitions)"
    elif complexity_score < 15:
        cluster_size = "Medium (5-8 nodes)"
        processing_time = "30 minutes - 2 hours"
        memory_config = "Optimized (16GB executors)"
        parallelism = "Medium (200 partitions)"
    else:
        cluster_size = "Large (10+ nodes)"
        processing_time = "2+ hours"
        memory_config = "High memory (32GB executors)"
        parallelism = "High (500+ partitions)"
    
    return {
        'cluster_size': cluster_size,
        'processing_time': processing_time,
        'memory_config': memory_config,
        'parallelism': parallelism
    }

def generate_emr_glue_recommendation(workload_type, data_volume, frequency, expertise,
                                  management_pref, customization, cost_priority):
    """Generate EMR vs Glue recommendation based on requirements"""
    
    emr_score = 0
    glue_score = 0
    reasons = []
    
    # Workload type scoring
    if workload_type in ["Interactive analytics", "Machine learning training"]:
        emr_score += 3
        reasons.append("EMR better for interactive/ML workloads")
    elif workload_type == "Batch ETL processing":
        glue_score += 2
        reasons.append("Glue optimized for ETL")
    
    # Data volume scoring
    if data_volume == "> 10 TB":
        emr_score += 2
        reasons.append("EMR handles very large datasets efficiently")
    elif data_volume in ["< 100 GB", "100 GB - 1 TB"]:
        glue_score += 1
    
    # Management preference
    if management_pref == "Fully managed/serverless":
        glue_score += 3
        reasons.append("Glue is fully serverless")
    elif management_pref == "Full infrastructure control":
        emr_score += 3
        reasons.append("EMR provides full control")
    
    # Customization needs
    if customization in ["Heavy customization", "Framework flexibility"]:
        emr_score += 2
        reasons.append("EMR supports extensive customization")
    elif customization == "Standard ETL only":
        glue_score += 2
        reasons.append("Glue perfect for standard ETL")
    
    # Team expertise
    if expertise in ["Advanced Spark users", "Big data experts"]:
        emr_score += 1
    elif expertise == "Beginners":
        glue_score += 2
        reasons.append("Glue easier for beginners")
    
    # Final decision
    if emr_score > glue_score:
        choice = "Amazon EMR"
        confidence = min(95, 60 + (emr_score - glue_score) * 5)
        cost_estimate = "$800-2000/month"
        implementation_effort = "Medium-High"
    else:
        choice = "AWS Glue ETL"
        confidence = min(95, 60 + (glue_score - emr_score) * 5)
        cost_estimate = "$200-800/month"
        implementation_effort = "Low-Medium"
    
    return {
        'choice': choice,
        'confidence': confidence,
        'reasons': reasons[:3],  # Top 3 reasons
        'cost_estimate': cost_estimate,
        'implementation_effort': implementation_effort
    }

def calculate_emr_monthly_cost(job_duration, jobs_per_month):
    """Calculate EMR monthly cost"""
    hourly_cost = 1.50  # Average cluster cost per hour
    return job_duration * jobs_per_month * hourly_cost

def calculate_glue_monthly_cost(job_duration, jobs_per_month, data_gb):
    """Calculate Glue monthly cost"""
    dpu_hours = job_duration * max(1, data_gb // 100)  # 1 DPU per 100GB
    cost_per_dpu_hour = 0.44
    return dpu_hours * jobs_per_month * cost_per_dpu_hour

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
    # 🐘 AWS EMR & Big Data Processing
    <div class='info-box'>
    Learn Amazon EMR architecture, data storage strategies, node configurations, transformation workflows, and make informed decisions between EMR and AWS Glue ETL for your big data processing needs.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🐘 Amazon EMR Overview",
        "💾 EMR Data Storage Options", 
        "🔧 Node Configuration Options",
        "🔄 Transforming Data in EMR",
        "⚖️ EMR vs Glue ETL Comparison"
    ])
    
    with tab1:
        emr_overview_tab()
    
    with tab2:
        emr_storage_options_tab()
    
    with tab3:
        node_configuration_tab()
    
    with tab4:
        data_transformation_tab()
    
    with tab5:
        emr_vs_glue_tab()
    
    # Footer
    st.markdown("""
    <div class="footer">
        <p>© 2025, Amazon Web Services, Inc. or its affiliates. All rights reserved.</p>
    </div>
    """, unsafe_allow_html=True)

# Main execution flow
if __name__ == "__main__":
    if 'localhost' in st.context.headers.get("host", ""):
        main()
    else:
        # First check authentication
        is_authenticated = authenticate.login()
        
        # If authenticated, show the main app content
        if is_authenticated:
            main()
