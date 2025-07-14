
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
    page_title="AWS Storage & Databases",
    page_icon="🗄️",
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
    'purple': '#7B68EE',
    'teal': '#20B2AA'
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
        
        .success-box {{
            background: linear-gradient(135deg, {AWS_COLORS['success']} 0%, {AWS_COLORS['light_blue']} 100%);
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
        
        .database-card {{
            background: linear-gradient(135deg, {AWS_COLORS['purple']} 0%, {AWS_COLORS['teal']} 100%);
            padding: 20px;
            border-radius: 12px;
            color: white;
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
            - 🗂️ Managing Data Within AWS - Data management strategies
            - 🔄 Amazon MemoryDB for Redis - In-memory data store
            - 🌐 Amazon Neptune - Graph database service
            - 📊 Amazon Keyspaces - Apache Cassandra compatible
            - 📄 Amazon DocumentDB - MongoDB compatible
            
            **Learning Objectives:**
            - Understand different AWS database options
            - Learn when to use specific database types
            - Explore modern database architectures
            - Practice with interactive examples and queries
            """)

def create_data_management_overview():
    """Create mermaid diagram for data management overview"""
    return """
    graph TD
        A[Data Management in AWS] --> B[Relational Databases]
        A --> C[NoSQL Databases]
        A --> D[In-Memory Databases]
        A --> E[Graph Databases]
        A --> F[Time Series Databases]
        
        B --> B1[Amazon RDS]
        B --> B2[Amazon Aurora]
        
        C --> C1[Amazon DynamoDB]
        C --> C2[Amazon DocumentDB]
        C --> C3[Amazon Keyspaces]
        
        D --> D1[Amazon ElastiCache]
        D --> D2[Amazon MemoryDB]
        
        E --> E1[Amazon Neptune]
        
        F --> F1[Amazon Timestream]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#7B68EE,stroke:#232F3E,color:#fff
        style E fill:#20B2AA,stroke:#232F3E,color:#fff
        style F fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_memorydb_architecture():
    """Create mermaid diagram for MemoryDB architecture"""
    return """
    graph TD
        A[Amazon MemoryDB for Redis] --> B[Primary Cluster]
        B --> C[Shard 1]
        B --> D[Shard 2]
        B --> E[Shard N]
        
        C --> C1[Primary Node]
        C --> C2[Replica Node 1]
        C --> C3[Replica Node 2]
        
        D --> D1[Primary Node]
        D --> D2[Replica Node 1]
        
        E --> E1[Primary Node]
        E --> E2[Replica Node 1]
        
        A --> F[Multi-AZ Durability]
        F --> G[Transaction Log]
        F --> H[Automatic Backups]
        
        A --> I[Redis APIs]
        I --> J[Redis Commands]
        I --> K[Data Structures]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style F fill:#3FB34F,stroke:#232F3E,color:#fff
        style I fill:#7B68EE,stroke:#232F3E,color:#fff
    """

def create_neptune_architecture():
    """Create mermaid diagram for Neptune architecture"""
    return """
    graph TD
        A[Amazon Neptune] --> B[Graph Database Engine]
        B --> C[Property Graph]
        B --> D[RDF Graph]
        
        C --> C1[Gremlin API]
        C1 --> C2[Vertices & Edges]
        C1 --> C3[Graph Traversals]
        
        D --> D1[SPARQL API]
        D1 --> D2[Triples Store]
        D1 --> D3[Semantic Queries]
        
        A --> E[Cluster Architecture]
        E --> F[Writer Instance]
        E --> G[Reader Instance 1]
        E --> H[Reader Instance N]
        
        A --> I[Storage Layer]
        I --> J[6 Copies Across 3 AZs]
        I --> K[Auto-Scaling Storage]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style E fill:#3FB34F,stroke:#232F3E,color:#fff
        style I fill:#20B2AA,stroke:#232F3E,color:#fff
    """

def create_keyspaces_architecture():
    """Create mermaid diagram for Keyspaces architecture"""
    return """
    graph TD
        A[Amazon Keyspaces] --> B[Cassandra Compatible]
        B --> C[CQL API]
        B --> D[Cassandra Drivers]
        
        A --> E[Serverless Architecture]
        E --> F[On-Demand Scaling]
        E --> G[Pay-per-Request]
        E --> H[No Infrastructure Management]
        
        A --> I[Data Distribution]
        I --> J[Partition Key]
        I --> K[Clustering Key]
        I --> L[Multi-AZ Replication]
        
        A --> M[Features]
        M --> N[Point-in-Time Recovery]
        M --> O[Encryption at Rest]
        M --> P[VPC Endpoints]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style E fill:#3FB34F,stroke:#232F3E,color:#fff
        style I fill:#7B68EE,stroke:#232F3E,color:#fff
        style M fill:#20B2AA,stroke:#232F3E,color:#fff
    """

def create_documentdb_architecture():
    """Create mermaid diagram for DocumentDB architecture"""
    return """
    graph TD
        A[Amazon DocumentDB] --> B[MongoDB Compatible]
        B --> C[MongoDB APIs]
        B --> D[MongoDB Drivers]
        B --> E[MongoDB Query Language]
        
        A --> F[Cluster Architecture]
        F --> G[Primary Instance]
        F --> H[Replica Instance 1]
        F --> I[Replica Instance N]
        
        A --> J[Storage Architecture]
        J --> K[Distributed Storage]
        J --> L[6 Copies Across 3 AZs]
        J --> M[Auto-Scaling Up to 64TB]
        
        A --> N[Features]
        N --> O[ACID Transactions]
        N --> P[Change Streams]
        N --> Q[Full-Text Search]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style F fill:#3FB34F,stroke:#232F3E,color:#fff
        style J fill:#7B68EE,stroke:#232F3E,color:#fff
        style N fill:#20B2AA,stroke:#232F3E,color:#fff
    """

def managing_data_tab():
    """Content for Managing Data Within AWS tab"""
    st.markdown("# 🗂️ Managing Data Within AWS")
    st.markdown("*Comprehensive data management strategies and database selection*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Data Management in AWS** involves choosing the right database for your specific use case, 
    considering factors like data structure, access patterns, scalability requirements, and consistency needs.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Management Overview
    st.markdown("## 🏗️ AWS Database Services Overview")
    common.mermaid(create_data_management_overview(), height=400)
    
    # Interactive Database Selector
    st.markdown("## 🔍 Interactive Database Selector")
    st.markdown("Answer a few questions to get database recommendations:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        data_structure = st.selectbox("What type of data structure do you have?", [
            "Structured (Tables with relationships)",
            "Semi-structured (JSON documents)",
            "Key-value pairs",
            "Graph relationships",
            "Time-series data"
        ])
        
        consistency = st.selectbox("What consistency model do you need?", [
            "Strong consistency (ACID)",
            "Eventual consistency",
            "Flexible consistency"
        ])
    
    with col2:
        scale = st.selectbox("What is your expected scale?", [
            "Small to medium (< 1TB)",
            "Large (1TB - 100TB)",
            "Very large (> 100TB)"
        ])
        
        workload = st.selectbox("What is your primary workload?", [
            "Read-heavy",
            "Write-heavy",
            "Mixed read/write",
            "Analytics/Complex queries",
            "Real-time/Low latency"
        ])
    
    if st.button("🎯 Get Database Recommendations", use_container_width=True):
        recommendations = get_database_recommendations(data_structure, consistency, scale, workload)
        
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 🎯 Recommended Database Services
        
        **Primary Recommendation:** {recommendations['primary']['name']}
        - **Why:** {recommendations['primary']['reason']}
        - **Use Case:** {recommendations['primary']['use_case']}
        
        **Alternative Options:**
        """)
        for alt in recommendations['alternatives']:
            st.markdown(f"- **{alt['name']}**: {alt['reason']}")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Database Comparison Matrix
    st.markdown("## 📊 Database Services Comparison")
    
    comparison_data = {
        'Service': ['Amazon RDS', 'Amazon DynamoDB', 'Amazon DocumentDB', 'Amazon Neptune', 'Amazon Keyspaces', 'Amazon MemoryDB'],
        'Type': ['Relational', 'NoSQL (Key-Value)', 'Document', 'Graph', 'Wide Column', 'In-Memory'],
        'Consistency': ['Strong', 'Eventual/Strong', 'Strong', 'Strong', 'Eventual', 'Strong'],
        'Scale': ['Vertical', 'Horizontal', 'Vertical', 'Vertical', 'Horizontal', 'Horizontal'],
        'Best For': ['OLTP', 'High Scale Apps', 'Content Mgmt', 'Social Networks', 'IoT/Analytics', 'Caching/Gaming']
    }
    
    df_comparison = pd.DataFrame(comparison_data)
    st.dataframe(df_comparison, use_container_width=True)
    
    # Data Migration Strategies
    st.markdown("## 🔄 Data Migration Strategies")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📦 Lift and Shift
        - **Move existing databases as-is**
        - Minimal changes required
        - Quick migration path
        - **Tools**: AWS DMS, AWS SCT
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Re-platform
        - **Switch database engines**
        - Optimize for cloud
        - Better performance/cost
        - **Example**: Oracle to Aurora
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏗️ Re-architect
        - **Complete application redesign**
        - Microservices approach
        - Polyglot persistence
        - **Example**: Monolith to NoSQL
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Performance Optimization
    st.markdown("## ⚡ Performance Optimization Strategies")
    
    # Performance metrics visualization
    performance_data = {
        'Strategy': ['Indexing', 'Read Replicas', 'Caching', 'Partitioning', 'Connection Pooling'],
        'Read Performance': [8, 9, 10, 7, 6],
        'Write Performance': [6, 5, 8, 9, 7],
        'Cost Impact': [2, 6, 4, 3, 1]
    }
    
    df_perf = pd.DataFrame(performance_data)
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_perf['Strategy'], y=df_perf['Read Performance'], 
                            mode='lines+markers', name='Read Performance', 
                            line=dict(color=AWS_COLORS['primary'], width=3)))
    fig.add_trace(go.Scatter(x=df_perf['Strategy'], y=df_perf['Write Performance'], 
                            mode='lines+markers', name='Write Performance',
                            line=dict(color=AWS_COLORS['light_blue'], width=3)))
    fig.add_trace(go.Scatter(x=df_perf['Strategy'], y=df_perf['Cost Impact'], 
                            mode='lines+markers', name='Cost Impact',
                            line=dict(color=AWS_COLORS['warning'], width=3)))
    
    fig.update_layout(title='Database Performance Optimization Strategies',
                      xaxis_title='Optimization Strategy',
                      yaxis_title='Impact Score (1-10)',
                      height=400)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Database Migration with AWS DMS")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# AWS Database Migration Service (DMS) setup
import boto3
import json

dms = boto3.client('dms')

# Create replication instance
replication_instance = dms.create_replication_instance(
    ReplicationInstanceIdentifier='my-dms-instance',
    ReplicationInstanceClass='dms.t3.micro',
    AllocatedStorage=20,
    VpcSecurityGroupIds=['sg-12345678'],
    ReplicationSubnetGroupIdentifier='my-subnet-group',
    MultiAZ=False,
    PubliclyAccessible=False,
    Tags=[
        {'Key': 'Name', 'Value': 'MyDMSInstance'},
        {'Key': 'Environment', 'Value': 'Production'}
    ]
)

# Create source endpoint (MySQL)
source_endpoint = dms.create_endpoint(
    EndpointIdentifier='mysql-source',
    EndpointType='source',
    EngineName='mysql',
    Username='admin',
    Password='password123',
    ServerName='mysql.example.com',
    Port=3306,
    DatabaseName='production_db'
)

# Create target endpoint (Aurora PostgreSQL)
target_endpoint = dms.create_endpoint(
    EndpointIdentifier='aurora-target',
    EndpointType='target',
    EngineName='aurora-postgresql',
    Username='postgres',
    Password='newpassword123',
    ServerName='aurora-cluster.cluster-xyz.us-east-1.rds.amazonaws.com',
    Port=5432,
    DatabaseName='migrated_db'
)

# Create migration task
migration_task = dms.create_replication_task(
    ReplicationTaskIdentifier='mysql-to-aurora-migration',
    SourceEndpointArn=source_endpoint['Endpoint']['EndpointArn'],
    TargetEndpointArn=target_endpoint['Endpoint']['EndpointArn'],
    ReplicationInstanceArn=replication_instance['ReplicationInstance']['ReplicationInstanceArn'],
    MigrationType='full-load-and-cdc',  # Full load + ongoing replication
    TableMappings=json.dumps({
        "rules": [
            {
                "rule-type": "selection",
                "rule-id": "1",
                "rule-name": "1",
                "object-locator": {
                    "schema-name": "production",
                    "table-name": "%"
                },
                "rule-action": "include"
            }
        ]
    })
)

print("Database migration setup completed!")
print(f"Replication Instance: {replication_instance['ReplicationInstance']['ReplicationInstanceIdentifier']}")
print(f"Migration Task: {migration_task['ReplicationTask']['ReplicationTaskIdentifier']}")

# Start migration task
dms.start_replication_task(
    ReplicationTaskArn=migration_task['ReplicationTask']['ReplicationTaskArn'],
    StartReplicationTaskType='start-replication'
)

print("Migration started successfully!")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def get_database_recommendations(data_structure, consistency, scale, workload):
    """Get database recommendations based on user inputs"""
    recommendations = {
        'primary': {'name': '', 'reason': '', 'use_case': ''},
        'alternatives': []
    }
    
    # Simple recommendation logic
    if data_structure == "Structured (Tables with relationships)":
        recommendations['primary'] = {
            'name': 'Amazon Aurora',
            'reason': 'Perfect for relational data with high performance needs',
            'use_case': 'E-commerce, ERP systems, financial applications'
        }
        recommendations['alternatives'] = [
            {'name': 'Amazon RDS', 'reason': 'Traditional relational database option'},
            {'name': 'Amazon DynamoDB', 'reason': 'If you can denormalize your data'}
        ]
    elif data_structure == "Semi-structured (JSON documents)":
        recommendations['primary'] = {
            'name': 'Amazon DocumentDB',
            'reason': 'Native JSON document storage with MongoDB compatibility',
            'use_case': 'Content management, catalogs, user profiles'
        }
        recommendations['alternatives'] = [
            {'name': 'Amazon DynamoDB', 'reason': 'For simple document storage with high scale'},
            {'name': 'Amazon OpenSearch', 'reason': 'If you need full-text search capabilities'}
        ]
    elif data_structure == "Graph relationships":
        recommendations['primary'] = {
            'name': 'Amazon Neptune',
            'reason': 'Purpose-built for graph databases and complex relationships',
            'use_case': 'Social networks, recommendation engines, fraud detection'
        }
        recommendations['alternatives'] = [
            {'name': 'Amazon DocumentDB', 'reason': 'Can handle simple graph-like data'},
            {'name': 'Amazon DynamoDB', 'reason': 'With adjacency list patterns'}
        ]
    else:
        recommendations['primary'] = {
            'name': 'Amazon DynamoDB',
            'reason': 'Highly scalable NoSQL database for various data patterns',
            'use_case': 'Web applications, mobile backends, gaming'
        }
        recommendations['alternatives'] = [
            {'name': 'Amazon Keyspaces', 'reason': 'For wide-column data patterns'},
            {'name': 'Amazon MemoryDB', 'reason': 'For high-performance caching needs'}
        ]
    
    return recommendations

def memorydb_tab():
    """Content for Amazon MemoryDB for Redis tab"""
    st.markdown("# 🔄 Amazon MemoryDB for Redis")
    st.markdown("*Ultra-fast, fully managed, Redis-compatible in-memory database*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Amazon MemoryDB for Redis** is a durable, in-memory database service that delivers ultra-fast performance 
    with microsecond read latency, single-digit millisecond write latency, and high throughput.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # MemoryDB Architecture
    st.markdown("## 🏗️ MemoryDB Architecture")
    common.mermaid(create_memorydb_architecture(), height=400)
    
    # Interactive Redis Command Simulator
    st.markdown("## 💻 Interactive Redis Command Simulator")
    
    if 'redis_data' not in st.session_state:
        st.session_state.redis_data = {}
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("### Redis Commands")
        command_type = st.selectbox("Command Type:", [
            "String Operations", "Hash Operations", "List Operations", 
            "Set Operations", "Sorted Set Operations"
        ])
        
        if command_type == "String Operations":
            operation = st.selectbox("Operation:", ["SET", "GET", "INCR", "DECR"])
            key = st.text_input("Key:", "mykey")
            
            if operation in ["SET"]:
                value = st.text_input("Value:", "myvalue")
            
            if st.button(f"Execute {operation}", key="string_op"):
                result = execute_redis_command(operation, key, value if operation == "SET" else None)
                st.success(f"Result: {result}")
        
        elif command_type == "Hash Operations":
            operation = st.selectbox("Operation:", ["HSET", "HGET", "HGETALL"])
            hash_key = st.text_input("Hash Key:", "user:1001")
            
            if operation in ["HSET", "HGET"]:
                field = st.text_input("Field:", "name")
                if operation == "HSET":
                    field_value = st.text_input("Value:", "John Doe")
            
            if st.button(f"Execute {operation}", key="hash_op"):
                if operation == "HSET":
                    result = execute_redis_command(operation, hash_key, field, field_value)
                elif operation == "HGET":
                    result = execute_redis_command(operation, hash_key, field)
                else:
                    result = execute_redis_command(operation, hash_key)
                st.success(f"Result: {result}")
    
    with col2:
        st.markdown("### Current Data Store")
        if st.session_state.redis_data:
            st.json(st.session_state.redis_data)
        else:
            st.info("No data stored yet. Execute some commands!")
        
        if st.button("Clear Data Store"):
            st.session_state.redis_data = {}
            st.success("Data store cleared!")
    
    # Performance Comparison
    st.markdown("## ⚡ Performance Comparison")
    
    performance_data = {
        'Database Type': ['Traditional RDBMS', 'DynamoDB', 'ElastiCache', 'MemoryDB'],
        'Read Latency (ms)': [10, 5, 0.2, 0.1],
        'Write Latency (ms)': [15, 10, 0.5, 1],
        'Throughput (ops/sec)': [1000, 10000, 100000, 160000],
        'Durability': ['High', 'High', 'None', 'High']
    }
    
    df_perf = pd.DataFrame(performance_data)
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Latency Comparison', 'Throughput Comparison'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    fig.add_trace(
        go.Bar(x=df_perf['Database Type'], y=df_perf['Read Latency (ms)'], 
               name='Read Latency', marker_color=AWS_COLORS['primary']),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(x=df_perf['Database Type'], y=df_perf['Throughput (ops/sec)'], 
               name='Throughput', marker_color=AWS_COLORS['light_blue']),
        row=1, col=2
    )
    
    fig.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    
    # Use Cases
    st.markdown("## 🌟 MemoryDB Use Cases")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="database-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎮 Gaming Leaderboards
        - **Real-time scoring**
        - Sorted sets for rankings
        - Millisecond response times
        - **Example**: Multiplayer game scores
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="database-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 💬 Real-time Chat
        - **Message queues**
        - Pub/sub messaging
        - Session management
        - **Example**: Live chat applications
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="database-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛒 Shopping Carts
        - **Session storage**
        - Fast read/write access
        - Automatic expiration
        - **Example**: E-commerce platforms
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Cluster Configuration
    st.markdown("## 🔧 Interactive Cluster Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        cluster_name = st.text_input("Cluster Name:", "my-memorydb-cluster")
        node_type = st.selectbox("Node Type:", [
            "db.t4g.small (2 vCPU, 1.37 GB)",
            "db.r6g.large (2 vCPU, 16 GB)",
            "db.r6g.xlarge (4 vCPU, 32 GB)",
            "db.r6g.2xlarge (8 vCPU, 64 GB)"
        ])
        num_shards = st.slider("Number of Shards:", 1, 500, 2)
    
    with col2:
        replicas_per_shard = st.slider("Replicas per Shard:", 0, 5, 1)
        snapshot_retention = st.slider("Snapshot Retention (days):", 0, 35, 7)
        maintenance_window = st.selectbox("Maintenance Window:", [
            "sun:05:00-sun:06:00", "mon:05:00-mon:06:00", "tue:05:00-tue:06:00"
        ])
    
    if st.button("🚀 Create MemoryDB Cluster (Simulation)", use_container_width=True):
        # Calculate estimated capacity and cost
        memory_per_node = {"db.t4g.small": 1.37, "db.r6g.large": 16, "db.r6g.xlarge": 32, "db.r6g.2xlarge": 64}
        cost_per_hour = {"db.t4g.small": 0.084, "db.r6g.large": 0.285, "db.r6g.xlarge": 0.570, "db.r6g.2xlarge": 1.140}
        
        node_key = node_type.split()[0]
        total_nodes = num_shards * (1 + replicas_per_shard)
        total_memory = total_nodes * memory_per_node[node_key]
        estimated_cost = total_nodes * cost_per_hour[node_key]
        
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ MemoryDB Cluster Configuration Ready!
        
        **Cluster Details:**
        - **Name**: {cluster_name}
        - **Node Type**: {node_type}
        - **Total Nodes**: {total_nodes} ({num_shards} shards × {1 + replicas_per_shard} nodes)
        - **Total Memory**: {total_memory:.2f} GB
        - **Estimated Cost**: ${estimated_cost:.3f}/hour
        - **Snapshot Retention**: {snapshot_retention} days
        
        🔄 **High Availability**: Multi-AZ with automatic failover
        ⚡ **Performance**: Sub-millisecond latency
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Using MemoryDB with Python")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Connect to MemoryDB using Redis client
import redis
import json
from datetime import datetime, timedelta

# Connect to MemoryDB cluster
redis_client = redis.Redis(
    host='clustercfg.my-memorydb-cluster.xyz.memorydb.us-east-1.amazonaws.com',
    port=6379,
    decode_responses=True,
    ssl=True,
    ssl_cert_reqs=None
)

# Example 1: Gaming leaderboard
def update_leaderboard(player_id, score):
    # Add player score to sorted set
    redis_client.zadd('game_leaderboard', {player_id: score})
    
    # Get top 10 players
    top_players = redis_client.zrevrange('game_leaderboard', 0, 9, withscores=True)
    return top_players

# Update scores
update_leaderboard('player1', 1500)
update_leaderboard('player2', 2000)
update_leaderboard('player3', 1750)

print("Top players:", update_leaderboard('player4', 1800))

# Example 2: Session management
def store_user_session(user_id, session_data, expiry_minutes=30):
    session_key = f'session:{user_id}'
    
    # Store session data as hash
    redis_client.hmset(session_key, session_data)
    
    # Set expiration
    redis_client.expire(session_key, expiry_minutes * 60)
    
    return session_key

def get_user_session(user_id):
    session_key = f'session:{user_id}'
    return redis_client.hgetall(session_key)

# Store user session
session_data = {
    'user_id': '12345',
    'username': 'john_doe',
    'login_time': datetime.now().isoformat(),
    'preferences': json.dumps({'theme': 'dark', 'language': 'en'})
}

store_user_session('12345', session_data)
print("User session:", get_user_session('12345'))

# Example 3: Real-time analytics
def track_page_view(page_path):
    # Increment page view counter
    daily_key = f'pageviews:{datetime.now().strftime("%Y-%m-%d")}'
    redis_client.hincrby(daily_key, page_path, 1)
    
    # Set expiration for 30 days
    redis_client.expire(daily_key, 30 * 24 * 60 * 60)

def get_popular_pages(date_str):
    daily_key = f'pageviews:{date_str}'
    page_views = redis_client.hgetall(daily_key)
    
    # Sort by views
    sorted_pages = sorted(page_views.items(), key=lambda x: int(x[1]), reverse=True)
    return sorted_pages[:10]

# Track some page views
track_page_view('/home')
track_page_view('/products')
track_page_view('/home')

print("Popular pages:", get_popular_pages(datetime.now().strftime("%Y-%m-%d")))

# Example 4: Distributed locking
def acquire_lock(lock_name, timeout=10):
    lock_key = f'lock:{lock_name}'
    identifier = str(datetime.now().timestamp())
    
    # Try to acquire lock with expiration
    if redis_client.set(lock_key, identifier, nx=True, ex=timeout):
        return identifier
    return None

def release_lock(lock_name, identifier):
    lock_key = f'lock:{lock_name}'
    # Only release if we own the lock
    if redis_client.get(lock_key) == identifier:
        redis_client.delete(lock_key)
        return True
    return False

# Use distributed lock
lock_id = acquire_lock('inventory_update')
if lock_id:
    try:
        # Critical section - update inventory
        print("Updating inventory...")
        # ... perform inventory update ...
    finally:
        release_lock('inventory_update', lock_id)
        print("Lock released")
else:
    print("Could not acquire lock")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def execute_redis_command(operation, key, field=None, value=None):
    """Simulate Redis command execution"""
    if operation == "SET":
        st.session_state.redis_data[key] = value
        return "OK"
    elif operation == "GET":
        return st.session_state.redis_data.get(key, "(nil)")
    elif operation == "HSET":
        if key not in st.session_state.redis_data:
            st.session_state.redis_data[key] = {}
        st.session_state.redis_data[key][field] = value
        return "1"
    elif operation == "HGET":
        return st.session_state.redis_data.get(key, {}).get(field, "(nil)")
    elif operation == "HGETALL":
        return st.session_state.redis_data.get(key, {})
    return "Command executed"

def neptune_tab():
    """Content for Amazon Neptune tab"""
    st.markdown("# 🌐 Amazon Neptune")
    st.markdown("*Fast, reliable graph database for connected data*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Amazon Neptune** is a fast, reliable, fully managed graph database service that makes it easy 
    to build and run applications that work with highly connected datasets using popular graph query languages.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Neptune Architecture
    st.markdown("## 🏗️ Neptune Architecture")
    common.mermaid(create_neptune_architecture(), height=450)
    
    # Graph vs Traditional Database
    st.markdown("## 🆚 Graph Database vs Traditional Database")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="service-comparison">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Traditional Relational Database
        
        **Finding friends of friends:**
        ```sql
        SELECT DISTINCT f2.friend_id
        FROM friendships f1
        JOIN friendships f2 ON f1.friend_id = f2.user_id
        WHERE f1.user_id = 'user123'
        AND f2.friend_id != 'user123'
        ```
        
        **Challenges:**
        - Multiple JOINs required
        - Performance degrades with depth
        - Complex queries for relationships
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="service-comparison">', unsafe_allow_html=True)
        st.markdown("""
        ### 🕸️ Graph Database (Gremlin)
        
        **Finding friends of friends:**
        ```gremlin
        g.V('user123')
         .out('friend')
         .out('friend')
         .dedup()
         .valueMap()
        ```
        
        **Advantages:**
        - Natural relationship traversal
        - Consistent performance at any depth
        - Intuitive query patterns
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Graph Query Builder
    st.markdown("## 🔍 Interactive Graph Query Builder")
    
    # Sample graph data
    sample_data = {
        'users': [
            {'id': 'alice', 'name': 'Alice', 'age': 28, 'city': 'Seattle'},
            {'id': 'bob', 'name': 'Bob', 'age': 32, 'city': 'Portland'},
            {'id': 'charlie', 'name': 'Charlie', 'age': 25, 'city': 'Seattle'},
            {'id': 'diana', 'name': 'Diana', 'age': 30, 'city': 'San Francisco'}
        ],
        'relationships': [
            {'from': 'alice', 'to': 'bob', 'type': 'friend', 'since': '2020'},
            {'from': 'bob', 'to': 'charlie', 'type': 'friend', 'since': '2021'},
            {'from': 'alice', 'to': 'charlie', 'type': 'colleague', 'since': '2019'},
            {'from': 'charlie', 'to': 'diana', 'type': 'friend', 'since': '2022'}
        ]
    }
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("### Query Builder")
        query_type = st.selectbox("Query Type:", [
            "Find Friends", "Find Friends of Friends", "Find Colleagues", 
            "Find People in City", "Shortest Path"
        ])
        
        start_person = st.selectbox("Starting Person:", ['alice', 'bob', 'charlie', 'diana'])
        
        if query_type == "Shortest Path":
            end_person = st.selectbox("End Person:", ['alice', 'bob', 'charlie', 'diana'])
        
        if st.button("🔍 Execute Query"):
            result = execute_graph_query(query_type, start_person, 
                                       end_person if query_type == "Shortest Path" else None,
                                       sample_data)
            st.json(result)
    
    with col2:
        st.markdown("### Graph Visualization")
        
        # Create network graph visualization
        import plotly.graph_objects as go
        import networkx as nx
        
        G = nx.Graph()
        
        # Add nodes
        for user in sample_data['users']:
            G.add_node(user['id'], name=user['name'], city=user['city'])
        
        # Add edges
        for rel in sample_data['relationships']:
            G.add_edge(rel['from'], rel['to'], type=rel['type'])
        
        # Get positions
        pos = nx.spring_layout(G)
        
        # Create edge traces
        edge_x = []
        edge_y = []
        for edge in G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
        
        edge_trace = go.Scatter(x=edge_x, y=edge_y,
                               line=dict(width=2, color=AWS_COLORS['light_blue']),
                               hoverinfo='none',
                               mode='lines')
        
        # Create node traces
        node_x = []
        node_y = []
        node_text = []
        for node in G.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            node_text.append(G.nodes[node]['name'])
        
        node_trace = go.Scatter(x=node_x, y=node_y,
                               mode='markers+text',
                               hoverinfo='text',
                               text=node_text,
                               textposition="middle center",
                               marker=dict(size=50,
                                         color=AWS_COLORS['primary'],
                                         line=dict(width=2, color='white')))
        
        fig = go.Figure(data=[edge_trace, node_trace],
                       layout=go.Layout(
                           title='Sample Social Network Graph',
                           showlegend=False,
                           hovermode='closest',
                           margin=dict(b=20,l=5,r=5,t=40),
                           annotations=[ dict(
                               text="Nodes: Users, Edges: Relationships",
                               showarrow=False,
                               xref="paper", yref="paper",
                               x=0.005, y=-0.002,
                               xanchor="left", yanchor="bottom",
                               font=dict(color="gray", size=12)
                           )],
                           xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                           yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Neptune Use Cases
    st.markdown("## 🌟 Neptune Use Cases")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="database-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🤝 Social Networks
        - **Friend recommendations**
        - Mutual connections
        - Community detection
        - **Example**: LinkedIn, Facebook
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="database-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛡️ Fraud Detection
        - **Pattern recognition**
        - Anomaly detection
        - Risk scoring
        - **Example**: Credit card fraud
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown ('<div class="database-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 Recommendation Engines
        - **Content filtering**
        - User behavior analysis
        - Product suggestions
        - **Example**: Amazon, Netflix
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Performance Metrics
    st.markdown("## 📈 Neptune Performance Characteristics")
    
    perf_data = {
        'Query Type': ['Single Vertex Lookup', '1-Hop Traversal', '2-Hop Traversal', '3-Hop Traversal', 'Complex Pattern'],
        'Response Time (ms)': [2, 5, 15, 45, 100],
        'Scalability': ['Excellent', 'Excellent', 'Very Good', 'Good', 'Moderate']
    }
    
    df_perf = pd.DataFrame(perf_data)
    
    fig = px.bar(df_perf, x='Query Type', y='Response Time (ms)',
                 title='Neptune Query Performance by Complexity',
                 color='Response Time (ms)',
                 color_continuous_scale=['green', 'yellow', 'red'])
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Neptune with Gremlin")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Connect to Neptune using Gremlin Python
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import json

# Connect to Neptune
graph = Graph()
connection = DriverRemoteConnection('wss://your-neptune-cluster.cluster-xyz.us-east-1.neptune.amazonaws.com:8182/gremlin','g')
g = graph.traversal().withRemote(connection)

# Example 1: Create a social network
def create_social_network():
    # Add users
    alice = g.addV('person').property('name', 'Alice').property('age', 28).next()
    bob = g.addV('person').property('name', 'Bob').property('age', 32).next()
    charlie = g.addV('person').property('name', 'Charlie').property('age', 25).next()
    
    # Add friendships
    g.V(alice).addE('friend').to(g.V(bob)).property('since', '2020').next()
    g.V(bob).addE('friend').to(g.V(charlie)).property('since', '2021').next()
    g.V(alice).addE('friend').to(g.V(charlie)).property('since', '2019').next()
    
    print("Social network created successfully!")

# Example 2: Find mutual friends
def find_mutual_friends(person1_name, person2_name):
    mutual_friends = (g.V().has('person', 'name', person1_name)
                       .out('friend')
                       .where(__.in_('friend').has('name', person2_name))
                       .values('name')
                       .toList())
    return mutual_friends

# Example 3: Friend recommendations (friends of friends)
def recommend_friends(person_name, limit=5):
    recommendations = (g.V().has('person', 'name', person_name)
                        .out('friend')
                        .out('friend')
                        .where(__.not_(__.in_('friend').has('name', person_name)))
                        .dedup()
                        .limit(limit)
                        .values('name')
                        .toList())
    return recommendations

# Example 4: Find shortest path between two people
def shortest_path(start_person, end_person):
    path = (g.V().has('person', 'name', start_person)
             .repeat(__.out('friend').simplePath())
             .until(__.has('name', end_person))
             .path()
             .by(__.values('name'))
             .limit(1)
             .next())
    return path

# Example 5: Community detection using modularity
def find_communities():
    # Get all friendship connections
    edges = (g.E().hasLabel('friend')
             .project('source', 'target')
             .by(__.outV().values('name'))
             .by(__.inV().values('name'))
             .toList())
    
    return edges

# Example 6: Fraud detection pattern
def detect_suspicious_patterns():
    # Find users who made transactions with multiple flagged accounts
    suspicious_users = (g.V().hasLabel('account')
                         .where(__.out('transaction')
                               .hasLabel('account')
                               .has('flagged', True)
                               .count()
                               .is_(__.gt(3)))
                         .values('account_id')
                         .toList())
    
    return suspicious_users

# Example 7: Product recommendations
def product_recommendations(user_id, category=None):
    query = (g.V().has('user', 'id', user_id)
             .out('purchased')
             .in_('purchased')
             .out('purchased'))
    
    if category:
        query = query.has('category', category)
    
    recommendations = (query.where(__.not_(__.in_('purchased')
                                         .has('user', 'id', user_id)))
                           .groupCount()
                           .unfold()
                           .order().by(__.select(values), desc)
                           .limit(10)
                           .select(keys)
                           .values('name')
                           .toList())
    
    return recommendations

# Usage examples
try:
    # Create the network
    create_social_network()
    
    # Find mutual friends
    mutual = find_mutual_friends('Alice', 'Charlie')
    print(f"Mutual friends of Alice and Charlie: {mutual}")
    
    # Get friend recommendations
    recommendations = recommend_friends('Alice')
    print(f"Friend recommendations for Alice: {recommendations}")
    
    # Find shortest path
    path = shortest_path('Alice', 'Charlie')
    print(f"Shortest path from Alice to Charlie: {path}")
    
finally:
    # Close connection
    connection.close()
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def execute_graph_query(query_type, start_person, end_person, sample_data):
    """Simulate graph query execution"""
    users = {u['id']: u for u in sample_data['users']}
    
    if query_type == "Find Friends":
        friends = [rel['to'] for rel in sample_data['relationships'] 
                  if rel['from'] == start_person and rel['type'] == 'friend']
        return [users[f] for f in friends if f in users]
    
    elif query_type == "Find Colleagues":
        colleagues = [rel['to'] for rel in sample_data['relationships'] 
                     if rel['from'] == start_person and rel['type'] == 'colleague']
        return [users[c] for c in colleagues if c in users]
    
    elif query_type == "Find People in City":
        city = users[start_person]['city']
        return [u for u in sample_data['users'] if u['city'] == city and u['id'] != start_person]
    
    else:
        return {"message": f"Query executed: {query_type} from {start_person}"}

def keyspaces_tab():
    """Content for Amazon Keyspaces tab"""
    st.markdown("# 📊 Amazon Keyspaces (for Apache Cassandra)")
    st.markdown("*Serverless, scalable, and highly available Cassandra-compatible database*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Amazon Keyspaces** is a scalable, highly available, and managed Apache Cassandra-compatible database service. 
    It's designed for applications that need fast, predictable performance at any scale.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Keyspaces Architecture
    st.markdown("## 🏗️ Keyspaces Architecture")
    common.mermaid(create_keyspaces_architecture(), height=400)
    
    # Cassandra Data Model Interactive Demo
    st.markdown("## 🗃️ Interactive Cassandra Data Model")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Table Schema Builder")
        keyspace_name = st.text_input("Keyspace Name:", "ecommerce")
        table_name = st.text_input("Table Name:", "user_activities")
        
        st.markdown("**Partition Key (Required):**")
        partition_key = st.text_input("Partition Key:", "user_id")
        partition_type = st.selectbox("Data Type:", ["TEXT", "UUID", "INT", "BIGINT"], index=0)
        
        st.markdown("**Clustering Key (Optional):**")
        clustering_key = st.text_input("Clustering Key:", "activity_time")
        clustering_type = st.selectbox("Data Type:", ["TIMESTAMP", "TEXT", "INT", "UUID"], index=0, key="clustering")
        clustering_order = st.selectbox("Order:", ["ASC", "DESC"])
        
        st.markdown("**Additional Columns:**")
        columns = st.text_area("Columns (name:type, one per line):", 
                              "activity_type:TEXT\nproduct_id:UUID\nmetadata:MAP<TEXT,TEXT>")
    
    with col2:
        st.markdown("### Generated CQL")
        
        # Generate CQL CREATE TABLE statement
        cql_statement = f"""CREATE TABLE {keyspace_name}.{table_name} (
    {partition_key} {partition_type}"""
        
        if clustering_key:
            cql_statement += f",\n    {clustering_key} {clustering_type}"
        
        if columns:
            for col in columns.split('\n'):
                if ':' in col:
                    col_name, col_type = col.split(':')
                    cql_statement += f",\n    {col_name.strip()} {col_type.strip()}"
        
        cql_statement += f",\n    PRIMARY KEY ({partition_key}"
        if clustering_key:
            cql_statement += f", {clustering_key}"
        cql_statement += ")"
        
        if clustering_key and clustering_order == "DESC":
            cql_statement += f"\n) WITH CLUSTERING ORDER BY ({clustering_key} {clustering_order})"
        else:
            cql_statement += ")"
        
        st.code(cql_statement, language='sql')
        
        if st.button("📝 Validate Schema"):
            st.success("✅ Schema is valid for Keyspaces!")
            st.info(f"📊 Estimated partition size: {'Small' if len(columns.split('\\n')) < 10 else 'Medium'}")
    
    # Performance Characteristics
    st.markdown("## ⚡ Performance Characteristics")
    
    # Capacity modes comparison
    capacity_data = {
        'Capacity Mode': ['On-Demand', 'Provisioned'],
        'Read Latency': ['Single-digit ms', 'Single-digit ms'],
        'Write Latency': ['Single-digit ms', 'Single-digit ms'],
        'Scaling': ['Automatic', 'Manual/Auto'],
        'Cost Model': ['Pay-per-request', 'Pay-per-capacity'],
        'Best For': ['Unpredictable traffic', 'Predictable traffic']
    }
    
    df_capacity = pd.DataFrame(capacity_data)
    st.table(df_capacity)
    
    # Interactive Query Builder
    st.markdown("## 🔍 Interactive CQL Query Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        query_type = st.selectbox("Query Type:", [
            "SELECT", "INSERT", "UPDATE", "DELETE"
        ])
        
        table_for_query = st.text_input("Table:", "ecommerce.user_activities")
        
        if query_type == "SELECT":
            columns_select = st.text_input("Columns (comma-separated):", "*")
            where_clause = st.text_area("WHERE clause:", "user_id = 'user123'\nAND activity_time > '2024-01-01'")
            limit = st.number_input("LIMIT:", 0, 1000, 0)
            
        elif query_type == "INSERT":
            columns_insert = st.text_input("Columns:", "user_id, activity_time, activity_type")
            values_insert = st.text_input("Values:", "'user123', NOW(), 'purchase'")
            
        elif query_type == "UPDATE":
            set_clause = st.text_input("SET:", "metadata = metadata + {'source': 'mobile'}")
            where_update = st.text_input("WHERE:", "user_id = 'user123' AND activity_time = '2024-07-14 10:00:00'")
    
    with col2:
        st.markdown("### Generated CQL Query")
        
        if query_type == "SELECT":
            cql_query = f"SELECT {columns_select}\nFROM {table_for_query}"
            if where_clause:
                formatted_where = where_clause.replace('\n', '\n    AND ')
                cql_query += f"\nWHERE {formatted_where}"
            if limit > 0:
                cql_query += f"\nLIMIT {limit}"
                
        elif query_type == "INSERT":
            cql_query = f"INSERT INTO {table_for_query}\n({columns_insert})\nVALUES ({values_insert})"
            
        elif query_type == "UPDATE":
            cql_query = f"UPDATE {table_for_query}\nSET {set_clause}\nWHERE {where_update}"
            
        else:  # DELETE
            cql_query = f"DELETE FROM {table_for_query}\nWHERE user_id = 'user123'"
        
        st.code(cql_query, language='sql')
        
        if st.button("🚀 Execute Query (Simulation)", key="sim1"):
            st.success("✅ Query executed successfully!")
            if query_type == "SELECT":
                st.json({
                    "rows_returned": np.random.randint(1, 100),
                    "execution_time": f"{np.random.uniform(1, 10):.2f} ms",
                    "read_capacity_consumed": np.random.randint(1, 10)
                })
    
    # Use Cases
    st.markdown("## 🌟 Keyspaces Use Cases")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="database-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 IoT Applications
        - **Time-series data**
        - Sensor readings
        - High write throughput
        - **Example**: Smart city sensors
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="database-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📱 Mobile & Web Apps
        - **User activity tracking**
        - Session management
        - Real-time features
        - **Example**: Social media feeds
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="database-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎮 Gaming
        - **Player statistics**
        - Leaderboards
        - Game state storage
        - **Example**: Multiplayer games
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Migration from Cassandra
    st.markdown("## 🔄 Migration from Apache Cassandra")
    
    migration_steps = [
        "Assess current Cassandra schema and queries",
        "Create equivalent keyspace and tables in Amazon Keyspaces",
        "Test application compatibility with Keyspaces",
        "Migrate data using AWS Database Migration Service",
        "Update application connection strings",
        "Perform thorough testing and validation"
    ]
    
    for i, step in enumerate(migration_steps, 1):
        st.markdown(f"**{i}.** {step}")
    
    # Code Example
    st.markdown("## 💻 Code Example: Using Keyspaces with Python")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Connect to Amazon Keyspaces using Cassandra driver
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import ssl
from datetime import datetime
import uuid

# Configure SSL context for SigV4 authentication
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Set up authentication (using service-specific credentials)
auth_provider = PlainTextAuthProvider(
    username='your-service-specific-username',
    password='your-service-specific-password'
)

# Create cluster connection
cluster = Cluster(
    ['cassandra.us-east-1.amazonaws.com'],
    port=9142,
    auth_provider=auth_provider,
    ssl_context=ssl_context
)

session = cluster.connect()

# Create keyspace
create_keyspace = '''
CREATE KEYSPACE IF NOT EXISTS ecommerce
WITH REPLICATION = {
    'class': 'SingleRegionStrategy',
    'replication_factor': 3
}
'''
session.execute(create_keyspace)
session.set_keyspace('ecommerce')

# Create table for user activities
create_table = '''
CREATE TABLE IF NOT EXISTS user_activities (
    user_id TEXT,
    activity_time TIMESTAMP,
    activity_type TEXT,
    product_id UUID,
    amount DECIMAL,
    metadata MAP<TEXT, TEXT>,
    PRIMARY KEY (user_id, activity_time)
) WITH CLUSTERING ORDER BY (activity_time DESC)
'''
session.execute(create_table)

# Example 1: Insert user activity
def record_user_activity(user_id, activity_type, product_id=None, amount=None, metadata=None):
    insert_stmt = '''
    INSERT INTO user_activities (user_id, activity_time, activity_type, product_id, amount, metadata)
    VALUES (?, ?, ?, ?, ?, ?)
    '''
    
    session.execute(insert_stmt, [
        user_id,
        datetime.now(),
        activity_type,
        product_id,
        amount,
        metadata or {}
    ])

# Record some activities
record_user_activity(
    user_id='user123',
    activity_type='purchase',
    product_id=uuid.uuid4(),
    amount=29.99,
    metadata={'source': 'mobile_app', 'campaign': 'summer_sale'}
)

record_user_activity(
    user_id='user123',
    activity_type='view',
    product_id=uuid.uuid4(),
    metadata={'category': 'electronics', 'source': 'web'}
)

# Example 2: Query user activities
def get_user_activities(user_id, limit=10):
    select_stmt = '''
    SELECT * FROM user_activities
    WHERE user_id = ?
    ORDER BY activity_time DESC
    LIMIT ?
    '''
    
    result = session.execute(select_stmt, [user_id, limit])
    return list(result)

# Get recent activities
activities = get_user_activities('user123')
print(f"Found {len(activities)} activities for user123")

for activity in activities:
    print(f"- {activity.activity_time}: {activity.activity_type}")

# Example 3: Time-range queries
def get_activities_in_range(user_id, start_time, end_time):
    select_stmt = '''
    SELECT * FROM user_activities
    WHERE user_id = ?
    AND activity_time >= ?
    AND activity_time <= ?
    ORDER BY activity_time DESC
    '''
    
    result = session.execute(select_stmt, [user_id, start_time, end_time])
    return list(result)

# Example 4: Update activity metadata
def update_activity_metadata(user_id, activity_time, new_metadata):
    update_stmt = '''
    UPDATE user_activities
    SET metadata = metadata + ?
    WHERE user_id = ? AND activity_time = ?
    '''
    
    session.execute(update_stmt, [new_metadata, user_id, activity_time])

# Example 5: Batch operations for better performance
from cassandra.query import BatchStatement

def batch_record_activities(activities):
    batch = BatchStatement()
    
    insert_stmt = session.prepare('''
        INSERT INTO user_activities (user_id, activity_time, activity_type, product_id, amount, metadata)
        VALUES (?, ?, ?, ?, ?, ?)
    ''')
    
    for activity in activities:
        batch.add(insert_stmt, activity)
    
    session.execute(batch)

# Batch insert multiple activities
batch_activities = [
    ['user456', datetime.now(), 'login', None, None, {'ip': '192.168.1.1'}],
    ['user456', datetime.now(), 'search', None, None, {'query': 'laptop'}],
    ['user456', datetime.now(), 'view', uuid.uuid4(), None, {'category': 'computers'}]
]

batch_record_activities(batch_activities)

# Example 6: Create secondary index (Global Secondary Index)
create_gsi = '''
CREATE INDEX IF NOT EXISTS activity_type_idx
ON user_activities (activity_type)
'''
session.execute(create_gsi)

# Query by activity type (uses GSI)
def get_activities_by_type(activity_type, limit=100):
    select_stmt = '''
    SELECT * FROM user_activities
    WHERE activity_type = ?
    LIMIT ?
    '''
    
    result = session.execute(select_stmt, [activity_type, limit])
    return list(result)

# Find all purchase activities
purchases = get_activities_by_type('purchase')
print(f"Found {len(purchases)} purchase activities")

# Example 7: Time-to-Live (TTL) for automatic data expiration
def record_session_activity(user_id, session_data, ttl_seconds=3600):
    insert_stmt = '''
    INSERT INTO user_activities (user_id, activity_time, activity_type, metadata)
    VALUES (?, ?, ?, ?)
    USING TTL ?
    '''
    
    session.execute(insert_stmt, [
        user_id,
        datetime.now(),
        'session_data',
        session_data,
        ttl_seconds
    ])

# Record session that expires in 1 hour
record_session_activity('user789', {'session_id': 'abc123', 'login_time': '2024-07-14T10:00:00'})

# Clean up
cluster.shutdown()
print("Connection closed successfully")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def documentdb_tab():
    """Content for Amazon DocumentDB tab"""
    st.markdown("# 📄 Amazon DocumentDB (MongoDB Compatible)")
    st.markdown("*Fast, scalable, highly available MongoDB-compatible document database*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Amazon DocumentDB** is a fast, scalable, highly available, and fully managed document database service 
    that supports MongoDB workloads. It's designed to give you the performance, scalability, and availability you need.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # DocumentDB Architecture
    st.markdown("## 🏗️ DocumentDB Architecture")
    common.mermaid(create_documentdb_architecture(), height=400)
    
    # Interactive Document Builder
    st.markdown("## 📝 Interactive Document Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Document Structure")
        collection_name = st.text_input("Collection Name:", "products")
        
        # Document fields
        st.markdown("**Document Fields:**")
        doc_name = st.text_input("Product Name:", "Wireless Headphones")
        doc_price = st.number_input("Price:", 0.0, 10000.0, 199.99)
        doc_category = st.selectbox("Category:", ["Electronics", "Clothing", "Books", "Home", "Sports"])
        
        # Array field
        tags = st.text_input("Tags (comma-separated):", "wireless, bluetooth, audio, premium")
        
        # Nested object
        st.markdown("**Specifications (Nested Object):**")
        color = st.text_input("Color:", "Black")
        weight = st.text_input("Weight:", "250g")
        battery = st.text_input("Battery Life:", "20 hours")
        
        # Additional fields
        in_stock = st.checkbox("In Stock", value=True)
        rating = st.slider("Rating:", 1.0, 5.0, 4.5, 0.1)
    
    with col2:
        st.markdown("### Generated MongoDB Document")
        
        # Build the document
        document = {
            "_id": f"prod_{np.random.randint(1000, 9999)}",
            "name": doc_name,
            "price": doc_price,
            "category": doc_category,
            "tags": [tag.strip() for tag in tags.split(',') if tag.strip()],
            "specifications": {
                "color": color,
                "weight": weight,
                "battery_life": battery
            },
            "in_stock": in_stock,
            "rating": rating,
            "created_at": datetime.now().isoformat(),
            "reviews_count": np.random.randint(0, 1000)
        }
        
        st.json(document)
        
        if st.button("💾 Save Document (Simulation)"):
            st.success("✅ Document saved to collection: " + collection_name)
            st.info(f"🆔 Document ID: {document['_id']}")
    
    # Interactive Query Builder
    st.markdown("## 🔍 Interactive MongoDB Query Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        query_type = st.selectbox("Query Operation:", [
            "find", "findOne", "aggregate", "updateOne", "deleteOne", "insertOne"
        ])
        
        collection_for_query = st.text_input("Collection:", "products")
        
        if query_type in ["find", "findOne"]:
            filter_query = st.text_area("Filter (JSON):", '{"category": "Electronics", "price": {"$lt": 300}}')
            projection = st.text_input("Projection (fields to return):", '{"name": 1, "price": 1, "_id": 0}')
            if query_type == "find":
                limit_docs = st.number_input("Limit:", 0, 100, 10)
                sort_field = st.text_input("Sort by:", "price")
                sort_order = st.selectbox("Sort order:", ["asc (1)", "desc (-1)"])
        
        elif query_type == "aggregate":
            pipeline = st.text_area("Aggregation Pipeline (JSON array):", '''[
  {"$match": {"category": "Electronics"}},
  {"$group": {"_id": "$category", "avg_price": {"$avg": "$price"}, "count": {"$sum": 1}}},
  {"$sort": {"avg_price": -1}}
]''')
        
        elif query_type == "updateOne":
            update_filter = st.text_area("Filter:", '{"_id": "prod_1234"}')
            update_doc = st.text_area("Update:", '{"$set": {"price": 179.99, "on_sale": true}}')
    
    with col2:
        st.markdown("### Generated MongoDB Query")
        
        if query_type == "find":
            mongo_query = f"db.{collection_for_query}.find(\n"
            mongo_query += f"  {filter_query},\n"
            mongo_query += f"  {projection}\n"
            mongo_query += ")"
            if sort_field:
                sort_val = 1 if "asc" in sort_order else -1
                mongo_query += f".sort({{\"{sort_field}\": {sort_val}}})"
            if limit_docs > 0:
                mongo_query += f".limit({limit_docs})"
        
        elif query_type == "aggregate":
            mongo_query = f"db.{collection_for_query}.aggregate({pipeline})"
        
        elif query_type == "updateOne":
            mongo_query = f"db.{collection_for_query}.updateOne(\n"
            mongo_query += f"  {update_filter},\n"
            mongo_query += f"  {update_doc}\n"
            mongo_query += ")"
        
        else:
            mongo_query = f"db.{collection_for_query}.{query_type}(...)"
        
        st.code(mongo_query, language='javascript')
        
        if st.button("🚀 Execute Query (Simulation)"):
            if query_type in ["find", "findOne"]:
                result = {
                    "documents_found": np.random.randint(1, 50),
                    "execution_time": f"{np.random.uniform(1, 25):.2f} ms",
                    "index_used": "category_1_price_1"
                }
            elif query_type == "aggregate":
                result = {
                    "pipeline_stages": 3,
                    "documents_processed": np.random.randint(100, 1000),
                    "execution_time": f"{np.random.uniform(5, 50):.2f} ms"
                }
            else:
                result = {
                    "acknowledged": True,
                    "matched_count": 1,
                    "modified_count": 1
                }
            
            st.success("✅ Query executed successfully!")
            st.json(result)
    
    # Performance Optimization
    st.markdown("## ⚡ Performance Optimization Strategies")
    
    optimization_data = {
        'Strategy': ['Indexing', 'Read Replicas', 'Connection Pooling', 'Query Optimization', 'Sharding Pattern'],
        'Performance Impact': [9, 8, 7, 9, 10],
        'Implementation Complexity': [3, 5, 4, 7, 9],
        'Cost Impact': [2, 6, 1, 1, 8]
    }
    
    df_opt = pd.DataFrame(optimization_data)
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df_opt['Implementation Complexity'],
        y=df_opt['Performance Impact'],
        mode='markers+text',
        text=df_opt['Strategy'],
        textposition="top center",
        marker=dict(
            size=df_opt['Cost Impact'],
            sizemode='diameter',
            sizeref=2.*max(df_opt['Cost Impact'])/(40.**2),
            sizemin=4,
            color=AWS_COLORS['primary'],
            opacity=0.7
        ),
        name='Optimization Strategies'
    ))
    
    fig.update_layout(
        title='DocumentDB Performance Optimization Strategies',
        xaxis_title='Implementation Complexity (1-10)',
        yaxis_title='Performance Impact (1-10)',
        annotations=[
            dict(text="Bubble size = Cost Impact", showarrow=False, x=0.02, y=0.98, xref="paper", yref="paper")
        ]
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Use Cases and Examples
    st.markdown("## 🌟 DocumentDB Use Cases")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="database-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📰 Content Management
        - **Flexible schema**
        - Rich document queries
        - Full-text search integration
        - **Example**: CMS, blogs, wikis
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="database-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛒 Product Catalogs
        - **Varied product attributes**
        - Hierarchical categories
        - Inventory management
        - **Example**: E-commerce platforms
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="database-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 👤 User Profiles
        - **Dynamic user data**
        - Personalization data
        - Activity tracking
        - **Example**: Social platforms
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Migration from MongoDB
    st.markdown("## 🔄 Migration from MongoDB")
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown("""
    ### 📋 Migration Steps
    
    1. **Assessment**: Analyze existing MongoDB deployment and usage patterns
    2. **Schema Review**: Identify any unsupported features or operators
    3. **Test Environment**: Set up DocumentDB cluster for testing
    4. **Data Migration**: Use AWS Database Migration Service or mongodump/mongorestore
    5. **Application Testing**: Verify application compatibility
    6. **Performance Optimization**: Create appropriate indexes and tune performance
    7. **Cutover**: Switch applications to use DocumentDB
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Compatibility Matrix
    st.markdown("## ✅ MongoDB Compatibility Matrix")
    
    compatibility_data = {
        'Feature': ['Core CRUD Operations', 'Aggregation Pipeline', 'Indexes', 'Transactions', 'Change Streams', 'GridFS'],
        'DocumentDB Support': ['✅ Full', '✅ Full', '✅ Full', '✅ Full', '✅ Full', '❌ Not Supported'],
        'Notes': [
            'All basic operations supported',
            'Most pipeline stages supported',
            'Compound, partial, sparse indexes',
            'ACID transactions with retries',
            'Real-time data change notifications',
            'Use S3 for large file storage instead'
        ]
    }
    
    df_compat = pd.DataFrame(compatibility_data)
    st.table(df_compat)
    
    # Code Example
    st.markdown("## 💻 Code Example: Using DocumentDB with Python")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Connect to Amazon DocumentDB using PyMongo
import pymongo
from pymongo import MongoClient
import ssl
from datetime import datetime
import json

# DocumentDB connection string (replace with your cluster endpoint)
connection_string = "mongodb://username:password@docdb-cluster.cluster-xyz.docdb.us-east-1.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-ca-2019-root.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"

# Create MongoDB client
client = MongoClient(connection_string)

# Select database and collection
db = client['ecommerce']
products_collection = db['products']
users_collection = db['users']
orders_collection = db['orders']

# Example 1: Insert product documents
def insert_products():
    products = [
        {
            "name": "Wireless Bluetooth Headphones",
            "price": 199.99,
            "category": "Electronics",
            "brand": "AudioTech",
            "specifications": {
                "color": "Black",
                "weight": "250g",
                "battery_life": "20 hours",
                "connectivity": ["Bluetooth 5.0", "3.5mm Jack"]
            },
            "tags": ["wireless", "bluetooth", "audio", "premium"],
            "inventory": {
                "in_stock": True,
                "quantity": 150,
                "warehouse_location": "US-WEST"
            },
            "rating": 4.5,
            "reviews_count": 324,
            "created_at": datetime.now()
        },
        {
            "name": "Smart Fitness Watch",
            "price": 299.99,
            "category": "Electronics",
            "brand": "FitTrack",
            "specifications": {
                "display": "1.4 inch OLED",
                "water_resistance": "50m",
                "sensors": ["Heart Rate", "GPS", "Accelerometer"],
                "battery_life": "7 days"
            },
            "tags": ["fitness", "smartwatch", "health", "gps"],
            "inventory": {
                "in_stock": True,
                "quantity": 85,
                "warehouse_location": "US-EAST"
            },
            "rating": 4.2,
            "reviews_count": 156,
            "created_at": datetime.now()
        }
    ]
    
    result = products_collection.insert_many(products)
    print(f"Inserted {len(result.inserted_ids)} products")
    return result.inserted_ids

# Example 2: Complex queries with aggregation
def get_category_analytics(category):
    pipeline = [
        # Match products in category
        {"$match": {"category": category}},
        
        # Group by brand and calculate metrics
        {
            "$group": {
                "_id": "$brand",
                "product_count": {"$sum": 1},
                "avg_price": {"$avg": "$price"},
                "avg_rating": {"$avg": "$rating"},
                "total_inventory": {"$sum": "$inventory.quantity"}
            }
        },
        
        # Sort by product count descending
        {"$sort": {"product_count": -1}},
        
        # Add calculated fields
        {
            "$addFields": {
                "category": category,
                "avg_price_rounded": {"$round": ["$avg_price", 2]},
                "avg_rating_rounded": {"$round": ["$avg_rating", 1]}
            }
        }
    ]
    
    result = list(products_collection.aggregate(pipeline))
    return result

# Example 3: Text search with indexes
def setup_text_search():
    # Create text index for search functionality
    products_collection.create_index([
        ("name", "text"),
        ("tags", "text"),
        ("category", "text")
    ])
    
    print("Text search index created")

def search_products(search_term, limit=10):
    # Use text search
    results = products_collection.find(
        {"$text": {"$search": search_term}},
        {"score": {"$meta": "textScore"}}
    ).sort([("score", {"$meta": "textScore"})]).limit(limit)
    
    return list(results)

# Example 4: User behavior tracking
def track_user_activity(user_id, activity_type, product_id=None, metadata=None):
    user_activity = {
        "user_id": user_id,
        "activity_type": activity_type,
        "product_id": product_id,
        "metadata": metadata or {},
        "timestamp": datetime.now(),
        "session_id": metadata.get("session_id") if metadata else None
    }
    
    # Use upsert to update user document with new activity
    users_collection.update_one(
        {"user_id": user_id},
        {
            "$push": {
                "recent_activities": {
                    "$each": [user_activity],
                    "$sort": {"timestamp": -1},
                    "$slice": 50  # Keep only last 50 activities
                }
            },
            "$set": {
                "last_active": datetime.now(),
                "updated_at": datetime.now()
            },
            "$setOnInsert": {
                "created_at": datetime.now(),
                "user_id": user_id
            }
        },
        upsert=True
    )

# Example 5: Transaction processing with ACID guarantees
def process_order(user_id, items, payment_info):
    # Start a transaction
    with client.start_session() as session:
        with session.start_transaction():
            try:
                # Calculate total and verify inventory
                total_amount = 0
                order_items = []
                
                for item in items:
                    product = products_collection.find_one(
                        {"_id": item["product_id"]},
                        session=session
                    )
                    
                    if not product:
                        raise ValueError(f"Product {item['product_id']} not found")
                    
                    if product["inventory"]["quantity"] < item["quantity"]:
                        raise ValueError(f"Insufficient inventory for {product['name']}")
                    
                    # Update inventory
                    products_collection.update_one(
                        {"_id": item["product_id"]},
                        {"$inc": {"inventory.quantity": -item["quantity"]}},
                        session=session
                    )
                    
                    item_total = product["price"] * item["quantity"]
                    total_amount += item_total
                    
                    order_items.append({
                        "product_id": item["product_id"],
                        "product_name": product["name"],
                        "quantity": item["quantity"],
                        "unit_price": product["price"],
                        "total_price": item_total
                    })
                
                # Create order document
                order = {
                    "user_id": user_id,
                    "items": order_items,
                    "total_amount": total_amount,
                    "payment_info": payment_info,
                    "status": "confirmed",
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
                
                order_result = orders_collection.insert_one(order, session=session)
                
                # Update user's order history
                users_collection.update_one(
                    {"user_id": user_id},
                    {
                        "$push": {"order_history": order_result.inserted_id},
                        "$inc": {"total_orders": 1, "total_spent": total_amount}
                    },
                    session=session
                )
                
                print(f"Order {order_result.inserted_id} processed successfully")
                return order_result.inserted_id
                
            except Exception as e:
                print(f"Order processing failed: {e}")
                # Transaction will be automatically aborted
                raise

# Example 6: Change streams for real-time updates
def monitor_product_changes():
    # Watch for changes in products collection
    change_stream = products_collection.watch([
        {"$match": {"operationType": {"$in": ["update", "insert", "delete"]}}}
    ])
    
    print("Monitoring product changes...")
    
    try:
        for change in change_stream:
            print(f"Change detected: {change['operationType']}")
            if change['operationType'] == 'update':
                print(f"Product ID: {change['documentKey']['_id']}")
                print(f"Updated fields: {change.get('updateDescription', {}).get('updatedFields', {})}")
            elif change['operationType'] == 'insert':
                print(f"New product: {change['fullDocument']['name']}")
    except KeyboardInterrupt:
        change_stream.close()

# Usage examples
if __name__ == "__main__":
    # Insert sample products
    product_ids = insert_products()
    
    # Setup search
    setup_text_search()
    
    # Get analytics
    electronics_analytics = get_category_analytics("Electronics")
    print("Electronics Category Analytics:")
    for brand_data in electronics_analytics:
        print(f"- {brand_data['_id']}: {brand_data['product_count']} products, avg price: ${brand_data['avg_price_rounded']}")
    
    # Search products
    search_results = search_products("bluetooth wireless")
    print(f"Found {len(search_results)} products for 'bluetooth wireless'")
    
    # Track user activity
    track_user_activity(
        user_id="user123",
        activity_type="product_view",
        product_id=product_ids[0],
        metadata={"source": "mobile_app", "session_id": "sess_456"}
    )
    
    # Process an order
    try:
        order_id = process_order(
            user_id="user123",
            items=[
                {"product_id": product_ids[0], "quantity": 1},
                {"product_id": product_ids[1], "quantity": 1}
            ],
            payment_info={"method": "credit_card", "last_four": "1234"}
        )
        print(f"Order created: {order_id}")
    except Exception as e:
        print(f"Order failed: {e}")

    # Close connection
    client.close()
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
    # 🗄️ AWS Storage & Databases
    ### Master modern database technologies and data management strategies in AWS
    """)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🗂️ Managing Data Within AWS",
        "🔄 Amazon MemoryDB",
        "🌐 Amazon Neptune", 
        "📊 Amazon Keyspaces",
        "📄 Amazon DocumentDB"
    ])
    
    with tab1:
        managing_data_tab()
    
    with tab2:
        memorydb_tab()
    
    with tab3:
        neptune_tab()
    
    with tab4:
        keyspaces_tab()
        
    with tab5:
        documentdb_tab()
    
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
