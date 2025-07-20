
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
    page_title="AWS Data Store Management",
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
        
        .storage-tier {{
            background: white;
            padding: 15px;
            border-radius: 10px;
            border: 2px solid {AWS_COLORS['light_blue']};
            margin: 10px 0;
            text-align: center;
        }}
        .info-box {{
            background-color: #E6F2FF;
            padding: 10px;
            border-radius: 10px;
            margin-bottom: 10px;
            border-left: 5px solid #00A1C9;
        }}
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    common.initialize_session_state()
    if 'session_started' not in st.session_state:
        st.session_state.session_started = True
        st.session_state.storage_explored = []
        st.session_state.concepts_learned = []

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🗄️ Managing Data Within AWS - Storage types overview
            - 📦 Amazon S3 - Object storage with multiple tiers
            - 📂 Amazon EFS - Elastic File System
            - 💾 Amazon EBS - Elastic Block Store
            - 🔄 AWS Transfer Family - File transfer protocols
            - 🏭 Amazon Redshift - Data warehousing solution
            
            **Learning Objectives:**
            - Understand different AWS storage types
            - Learn about data store characteristics and use cases
            - Explore storage optimization and cost management
            - Master data warehousing concepts
            """)

def create_storage_types_mermaid():
    """Create mermaid diagram for AWS storage types"""
    return """
    graph TB
        A[AWS Data Storage] --> B[Object Storage]
        A --> C[Block Storage]
        A --> D[File Storage]
        
        B --> E[Amazon S3]
        B --> F[S3 Glacier]
        
        C --> G[Amazon EBS]
        
        D --> H[Amazon EFS]
        D --> I[Amazon FSx]
        
        E --> E1[Documents, Images, Backups]
        G --> G1[EC2 Instance Storage]
        H --> H1[Shared File Systems]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#4B9EDB,stroke:#232F3E,color:#fff
        style D fill:#4B9EDB,stroke:#232F3E,color:#fff
        style E fill:#3FB34F,stroke:#232F3E,color:#fff
        style G fill:#3FB34F,stroke:#232F3E,color:#fff
        style H fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_s3_tiers_mermaid():
    """Create mermaid diagram for S3 storage tiers"""
    return """
    graph LR
        A[S3 Standard] --> B[S3 Intelligent-Tiering]
        B --> C[S3 Standard-IA]
        C --> D[S3 One Zone-IA]
        D --> E[S3 Glacier Flexible]
        E --> F[S3 Glacier Deep Archive]
        
        A1[Frequently Accessed<br/>Milliseconds] --> A
        B1[Unknown Access Patterns<br/>Automatic Tiering] --> B
        C1[Infrequently Accessed<br/>30 day minimum] --> C
        D1[Single AZ<br/>Recreatable Data] --> D
        E1[Archive Data<br/>1-5 minutes retrieval] --> E
        F1[Long-term Archive<br/>12 hours retrieval] --> F
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#3FB34F,stroke:#232F3E,color:#fff
        style E fill:#232F3E,stroke:#FF9900,color:#fff
        style F fill:#1B2631,stroke:#FF9900,color:#fff
    """

def create_redshift_architecture_mermaid():
    """Create mermaid diagram for Redshift architecture"""
    return """
    graph TB
        subgraph "Amazon Redshift Cluster"
            LN[Leader Node<br/>📊 Query Planning<br/>🔄 Coordination]
            
            subgraph "Compute Nodes"
                CN1[Compute Node 1<br/>Slice 1<br/>Slice 2]
                CN2[Compute Node 2<br/>Slice 3<br/>Slice 4]
                CN3[Compute Node 3<br/>Slice 5<br/>Slice 6]
            end
        end
        
        subgraph "RA3 Architecture"
            S3[Amazon S3<br/>Managed Storage]
        end
        
        CLIENT[SQL Client] --> LN
        LN --> CN1
        LN --> CN2
        LN --> CN3
        
        CN1 -.-> S3
        CN2 -.-> S3
        CN3 -.-> S3
        
        style LN fill:#FF9900,stroke:#232F3E,color:#fff
        style CN1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CN2 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CN3 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style S3 fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_storage_cost_chart():
    """Create interactive storage cost comparison chart"""
    storage_data = {
        'Storage Class': ['S3 Standard', 'S3 Standard-IA', 'S3 One Zone-IA', 
                         'S3 Glacier Flexible', 'S3 Glacier Deep Archive'],
        'Cost per GB/Month ($)': [0.023, 0.0125, 0.01, 0.004, 0.00099],
        'Retrieval Cost ($)': [0, 0.01, 0.01, 0.03, 0.05],
        'First Byte Latency': ['Milliseconds', 'Milliseconds', 'Milliseconds', 
                              '1-5 minutes', '12 hours']
    }
    
    df = pd.DataFrame(storage_data)
    
    fig = px.bar(df, x='Storage Class', y='Cost per GB/Month ($)',
                 title='S3 Storage Class Cost Comparison',
                 color='Cost per GB/Month ($)',
                 color_continuous_scale=[[0, AWS_COLORS['success']], 
                                       [0.5, AWS_COLORS['primary']], 
                                       [1, AWS_COLORS['secondary']]])
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_color=AWS_COLORS['secondary'],
        xaxis_tickangle=-45
    )
    
    return fig

def create_lifecycle_simulation():
    """Create S3 lifecycle policy simulation"""
    st.markdown("##### 🔄 Interactive S3 Lifecycle Simulation")
    
    col1, col2 = st.columns(2)
    
    with col1:
        days_old = st.slider("Object Age (Days)", 0, 365, 30)
        object_type = st.selectbox("Object Type", 
                                  ["Frequently Accessed", "Infrequently Accessed", "Archive"])
    
    with col2:
        auto_tiering = st.checkbox("Enable Intelligent Tiering", value=True)
        
    # Determine current storage class based on age and settings
    if auto_tiering:
        if days_old < 30:
            current_class = "S3 Standard"
            color = AWS_COLORS['primary']
        elif days_old < 90:
            current_class = "S3 Standard-IA" 
            color = AWS_COLORS['light_blue']
        elif days_old < 180:
            current_class = "S3 Glacier Flexible"
            color = AWS_COLORS['secondary']
        else:
            current_class = "S3 Glacier Deep Archive"
            color = AWS_COLORS['dark_blue']
    else:
        current_class = "S3 Standard"
        color = AWS_COLORS['primary']
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### Current Storage Status
    **Object Age**: {days_old} days  
    **Current Storage Class**: {current_class}  
    **Auto-Tiering**: {'Enabled' if auto_tiering else 'Disabled'}  
    **Monthly Cost (per GB)**: ${get_storage_cost(current_class):.4f}
    """)
    st.markdown('</div>', unsafe_allow_html=True)

def get_storage_cost(storage_class):
    """Get cost for storage class"""
    costs = {
        "S3 Standard": 0.023,
        "S3 Standard-IA": 0.0125,
        "S3 Glacier Flexible": 0.004,
        "S3 Glacier Deep Archive": 0.00099
    }
    return costs.get(storage_class, 0.023)

def managing_data_tab():
    """Content for Managing Data Within AWS tab"""
    st.markdown("## 🗄️ Managing Data Within AWS")
    st.markdown("*Understanding the three fundamental storage types in AWS*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    AWS provides three primary storage types, each optimized for different use cases:
    - **Object Storage**: For files, documents, and unstructured data
    - **Block Storage**: For databases and file systems
    - **File Storage**: For shared access across multiple systems
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Storage types architecture
    st.markdown("#### 🏗️ AWS Storage Architecture")
    common.mermaid(create_storage_types_mermaid(), height=500)
    
    # Interactive storage comparison
    st.markdown("#### 📊 Storage Type Comparison")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="storage-tier">', unsafe_allow_html=True)
        st.markdown("""
        ### 📦 Object Storage
        **Services**: S3, S3 Glacier  
        **Use Cases**: 
        - Static websites
        - Content distribution
        - Data archiving
        - Application data
        
        **Characteristics**:
        - Virtually unlimited scale
        - REST API access
        - Metadata storage
        - Web accessible
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="storage-tier">', unsafe_allow_html=True)
        st.markdown("""
        ### 💾 Block Storage
        **Services**: EBS  
        **Use Cases**:
        - Database storage
        - EC2 boot volumes
        - High-performance workloads
        - Enterprise applications
        
        **Characteristics**:
        - High IOPS performance
        - Attachable to EC2
        - Snapshots to S3
        - Different volume types
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="storage-tier">', unsafe_allow_html=True)
        st.markdown("""
        ### 📂 File Storage
        **Services**: EFS, FSx  
        **Use Cases**:
        - Shared application data
        - Content repositories
        - Web serving
        - Data analytics
        
        **Characteristics**:
        - POSIX-compliant
        - Concurrent access
        - Automatic scaling
        - Network attached
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive decision tree
    st.markdown("#### 🤔 Which Storage Type Should You Choose?")
    
    use_case = st.selectbox("Select your use case:", [
        "Static website hosting",
        "Database for EC2 instance", 
        "Shared file system for multiple servers",
        "Long-term data archiving",
        "High-performance computing",
        "Content distribution"
    ])
    
    recommendations = {
        "Static website hosting": ("S3 Standard", "Object storage is perfect for static websites with global access"),
        "Database for EC2 instance": ("EBS", "Block storage provides high IOPS for database workloads"),
        "Shared file system for multiple servers": ("EFS", "File storage allows concurrent access from multiple instances"),
        "Long-term data archiving": ("S3 Glacier", "Object storage with archival tiers for cost optimization"),
        "High-performance computing": ("EBS", "Block storage with provisioned IOPS for HPC workloads"),
        "Content distribution": ("S3 + CloudFront", "Object storage with CDN for global content delivery")
    }
    
    if use_case in recommendations:
        service, explanation = recommendations[use_case]
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Recommended Solution: {service}
        {explanation}
        """)
        st.markdown('</div>', unsafe_allow_html=True)

def s3_tab():
    """Content for Amazon S3 tab"""
    st.markdown("## 📦 Amazon Simple Storage Service (S3)")
    st.markdown("*Infinitely scalable, highly durable object storage in the AWS Cloud*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    Amazon S3 is object storage built to store and retrieve any amount of data from anywhere. 
    It offers industry-leading scalability, data availability, security, and performance with multiple storage classes.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # S3 Storage Classes diagram
    st.markdown("#### 🏗️ S3 Storage Classes")
    common.mermaid(create_s3_tiers_mermaid(), height=450)
    
    # Interactive cost comparison
    st.markdown("#### 💰 Storage Cost Analysis")
    st.plotly_chart(create_storage_cost_chart(), use_container_width=True)
    
    # Storage classes details
    st.markdown("#### 📊 Storage Classes Comparison")
    
    storage_classes_data = {
        'Storage Class': ['S3 Standard', 'S3 Intelligent-Tiering', 'S3 Standard-IA', 
                         'S3 One Zone-IA', 'S3 Glacier Flexible', 'S3 Glacier Deep Archive'],
        'Use Case': ['Active data', 'Unknown patterns', 'Infrequent access', 
                    'Recreatable data', 'Archive', 'Long-term archive'],
        'Availability': ['99.99%', '99.9%', '99.9%', '99.5%', '99.99%', '99.99%'],
        'Durability': ['99.999999999%'] * 6,
        'Min Storage Duration': ['None', 'None', '30 days', '30 days', '90 days', '180 days'],
        'Retrieval Time': ['Milliseconds'] * 4 + ['1-5 minutes', '12 hours']
    }
    
    df_storage = pd.DataFrame(storage_classes_data)
    st.dataframe(df_storage, use_container_width=True)
    
    # Interactive lifecycle simulation
    create_lifecycle_simulation()
    
    # Code examples
    st.markdown("#### 💻 Code Examples")
    
    tab_code1, tab_code2, tab_code3 = st.tabs(["Basic Operations", "Lifecycle Policy", "Cross-Region Replication"])
    
    with tab_code1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# AWS CLI - Basic S3 operations
# Create bucket
aws s3 mb s3://my-unique-bucket-name

# Upload file
aws s3 cp myfile.txt s3://my-unique-bucket-name/

# List objects
aws s3 ls s3://my-unique-bucket-name/

# Python Boto3 - S3 operations
import boto3

s3_client = boto3.client('s3')

# Create bucket
s3_client.create_bucket(
    Bucket='my-unique-bucket-name',
    CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
)

# Upload file with storage class
s3_client.upload_file(
    'local-file.txt', 
    'my-unique-bucket-name', 
    'remote-file.txt',
    ExtraArgs={'StorageClass': 'STANDARD_IA'}
)

# List objects
response = s3_client.list_objects_v2(Bucket='my-unique-bucket-name')
for obj in response.get('Contents', []):
    print(f"Object: {obj['Key']}, Size: {obj['Size']}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab_code2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# S3 Lifecycle Policy Configuration
{
    "Rules": [
        {
            "ID": "OptimizeStorage",
            "Status": "Enabled",
            "Filter": {"Prefix": "documents/"},
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
                "Days": 2555  # 7 years
            }
        }
    ]
}

# Apply lifecycle policy using Python
import boto3
import json

s3_client = boto3.client('s3')

lifecycle_config = {
    'Rules': [
        {
            'ID': 'OptimizeStorage',
            'Status': 'Enabled',
            'Filter': {'Prefix': 'documents/'},
            'Transitions': [
                {'Days': 30, 'StorageClass': 'STANDARD_IA'},
                {'Days': 90, 'StorageClass': 'GLACIER'},
                {'Days': 365, 'StorageClass': 'DEEP_ARCHIVE'}
            ]
        }
    ]
}

s3_client.put_bucket_lifecycle_configuration(
    Bucket='my-bucket',
    LifecycleConfiguration=lifecycle_config
)
        ''', language='json')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab_code3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Cross-Region Replication Configuration
import boto3

s3_client = boto3.client('s3')

# Create replication configuration
replication_config = {
    'Role': 'arn:aws:iam::123456789012:role/replication-role',
    'Rules': [
        {
            'ID': 'ReplicateToWest',
            'Status': 'Enabled',
            'Priority': 1,
            'Filter': {'Prefix': 'important/'},
            'Destination': {
                'Bucket': 'arn:aws:s3:::destination-bucket',
                'StorageClass': 'STANDARD_IA'
            }
        }
    ]
}

# Apply replication configuration
s3_client.put_bucket_replication(
    Bucket='source-bucket',
    ReplicationConfiguration=replication_config
)

print("Cross-region replication configured successfully!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def efs_tab():
    """Content for Amazon EFS tab"""
    st.markdown("## 📂 Amazon Elastic File System (EFS)")
    st.markdown("*Simple, serverless, set-and-forget, elastic file system*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    Amazon EFS provides scalable file storage for use with Amazon EC2. It grows and shrinks automatically 
    as you add and remove files, providing shared access across multiple availability zones.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # EFS Architecture diagram
    efs_diagram = """
    graph TB
        subgraph "VPC"
            subgraph "AZ-1"
                EC2_1[EC2 Instance]
                MT_1[Mount Target]
            end
            
            subgraph "AZ-2"
                EC2_2[EC2 Instance]
                MT_2[Mount Target]
            end
            
            subgraph "AZ-3"
                EC2_3[EC2 Instance]
                MT_3[Mount Target]
            end
        end
        
        EFS[Amazon EFS<br/>File System]
        
        EC2_1 --> MT_1
        EC2_2 --> MT_2
        EC2_3 --> MT_3
        
        MT_1 --> EFS
        MT_2 --> EFS
        MT_3 --> EFS
        
        style EFS fill:#FF9900,stroke:#232F3E,color:#fff
        style EC2_1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style EC2_2 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style EC2_3 fill:#4B9EDB,stroke:#232F3E,color:#fff
    """
    
    st.markdown("#### 🏗️ EFS Architecture")
    common.mermaid(efs_diagram, height=500)
    
    # EFS Features and Benefits
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Key Features
        - **Fully Managed**: No infrastructure to manage
        - **Elastic**: Automatically scales up/down
        - **POSIX-compliant**: Standard file system interface
        - **Multi-AZ**: Built-in high availability
        - **Concurrent Access**: Share across multiple instances
        - **Performance Modes**: General Purpose & Max I/O
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 Use Cases
        - **Content Management**: Shared repositories
        - **Web Serving**: Scalable web applications
        - **Data Analytics**: Big data and ML workloads
        - **Application Development**: Shared development environments
        - **Database Backups**: Shared backup storage
        - **Container Storage**: Persistent volumes for containers
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive performance calculator
    st.markdown("#### ⚡ EFS Performance Calculator")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        file_size_gb = st.number_input("File System Size (GB)", min_value=1, max_value=10000, value=100)
    with col2:
        performance_mode = st.selectbox("Performance Mode", ["General Purpose", "Max I/O"])
    with col3:
        throughput_mode = st.selectbox("Throughput Mode", ["Bursting", "Provisioned"])
    
    # Calculate performance metrics
    if performance_mode == "General Purpose":
        max_iops = 7000
        latency = "Single-digit milliseconds"
    else:
        max_iops = 500000
        latency = "Low double-digit milliseconds"
    
    if throughput_mode == "Bursting":
        throughput = min(100 + (file_size_gb * 0.05), 500)  # 50 MB/s per TB up to 500 MB/s
    else:
        throughput = st.slider("Provisioned Throughput (MB/s)", 1, 1000, 500)
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Performance Estimates
    **File System Size**: {file_size_gb} GB  
    **Performance Mode**: {performance_mode}  
    **Max IOPS**: {max_iops:,}  
    **Throughput**: {throughput:.1f} MB/s  
    **Latency**: {latency}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Code Examples")
    
    tab1, tab2 = st.tabs(["Creating EFS", "Mounting EFS"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create EFS file system using AWS CLI
aws efs create-file-system \
    --performance-mode generalPurpose \
    --throughput-mode bursting \
    --encrypted \
    --tags Key=Name,Value=MyEFS

# Python Boto3 - Create EFS file system
import boto3

efs_client = boto3.client('efs')

# Create file system
response = efs_client.create_file_system(
    PerformanceMode='generalPurpose',
    ThroughputMode='bursting',
    Encrypted=True,
    Tags=[
        {
            'Key': 'Name',
            'Value': 'MyApplicationEFS'
        }
    ]
)

file_system_id = response['FileSystemId']
print(f"Created EFS: {file_system_id}")

# Create mount targets in each AZ
ec2_client = boto3.client('ec2')
subnets = ec2_client.describe_subnets()

for subnet in subnets['Subnets']:
    efs_client.create_mount_target(
        FileSystemId=file_system_id,
        SubnetId=subnet['SubnetId']
    )
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Mount EFS on EC2 instance (Linux)

# Install EFS utils
sudo yum install -y amazon-efs-utils

# Create mount point
sudo mkdir /mnt/efs

# Mount using EFS mount helper (recommended)
sudo mount -t efs fs-12345678:/ /mnt/efs

# Or mount using NFS
sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 \
    fs-12345678.efs.us-west-2.amazonaws.com:/ /mnt/efs

# Add to /etc/fstab for persistent mounting
echo "fs-12345678.efs.us-west-2.amazonaws.com:/ /mnt/efs efs defaults,_netdev" >> /etc/fstab

# Test the mount
cd /mnt/efs
sudo mkdir test-directory
echo "Hello EFS!" | sudo tee test-file.txt

# Verify from another instance
# The file should be visible from any instance with EFS mounted!
        ''', language='bash')
        st.markdown('</div>', unsafe_allow_html=True)

def ebs_tab():
    """Content for Amazon EBS tab"""
    st.markdown("## 💾 Amazon Elastic Block Store (EBS)")
    st.markdown("*Easy to use, high performance block storage at any scale*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    Amazon EBS provides persistent block-level storage volumes for use with EC2 instances. 
    EBS volumes are network-attached storage that persist independently from EC2 instance life cycles.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # EBS Architecture diagram
    ebs_diagram = """
    graph LR
        subgraph "Availability Zone"
            EC2[EC2 Instance<br/>i-1234567890abcdef0]
            
            subgraph "EBS Volumes"
                ROOT[Root Volume<br/>gp3: 30 GB<br/>/dev/sda1]
                DATA[Data Volume<br/>io2: 500 GB<br/>/dev/sdf]
                BACKUP[Backup Volume<br/>sc1: 1 TB<br/>/dev/sdg]
            end
        end
        
        SNAP[EBS Snapshots<br/>in Amazon S3]
        
        EC2 --> ROOT
        EC2 --> DATA
        EC2 --> BACKUP
        
        ROOT -.-> SNAP
        DATA -.-> SNAP
        BACKUP -.-> SNAP
        
        style EC2 fill:#FF9900,stroke:#232F3E,color:#fff
        style ROOT fill:#4B9EDB,stroke:#232F3E,color:#fff
        style DATA fill:#3FB34F,stroke:#232F3E,color:#fff
        style BACKUP fill:#232F3E,stroke:#FF9900,color:#fff
        style SNAP fill:#1B2631,stroke:#FF9900,color:#fff
    """
    
    st.markdown("#### 🏗️ EBS Architecture")
    common.mermaid(ebs_diagram, height=600)
    
    # EBS Volume Types
    st.markdown("#### 📊 EBS Volume Types Comparison")
    
    volume_data = {
        'Volume Type': ['gp3', 'gp2', 'io2', 'io1', 'st1', 'sc1'],
        'Category': ['General Purpose SSD', 'General Purpose SSD', 'Provisioned IOPS SSD', 
                    'Provisioned IOPS SSD', 'Throughput Optimized HDD', 'Cold HDD'],
        'Max IOPS': ['16,000', '16,000', '64,000', '64,000', '500', '250'],
        'Max Throughput': ['1,000 MB/s', '250 MB/s', '1,000 MB/s', '1,000 MB/s', '500 MB/s', '250 MB/s'],
        'Use Case': ['Balanced price/performance', 'General workloads', 'Mission-critical apps', 
                    'High IOPS databases', 'Big data analytics', 'Infrequent access']
    }
    
    df_volumes = pd.DataFrame(volume_data)
    st.dataframe(df_volumes, use_container_width=True)
    
    # Interactive volume calculator
    st.markdown("#### 🧮 EBS Volume Calculator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        volume_type = st.selectbox("Volume Type", ['gp3', 'gp2', 'io2', 'io1', 'st1', 'sc1'])
        volume_size = st.slider("Volume Size (GB)", 1, 16384, 100)
    
    with col2:
        if volume_type in ['io2', 'io1']:
            provisioned_iops = st.slider("Provisioned IOPS", 100, 64000, 3000)
        else:
            provisioned_iops = 0
        
        if volume_type == 'gp3':
            provisioned_throughput = st.slider("Provisioned Throughput (MB/s)", 125, 1000, 125)
        else:
            provisioned_throughput = 0
    
    # Calculate costs (simplified)
    storage_cost = volume_size * 0.08  # Approximate cost per GB per month
    iops_cost = provisioned_iops * 0.005 if provisioned_iops > 0 else 0
    throughput_cost = (provisioned_throughput - 125) * 0.04 if provisioned_throughput > 125 else 0
    
    total_cost = storage_cost + iops_cost + throughput_cost
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 💰 Monthly Cost Estimate
    **Volume Type**: {volume_type}  
    **Size**: {volume_size} GB  
    **Storage Cost**: ${storage_cost:.2f}  
    **IOPS Cost**: ${iops_cost:.2f}  
    **Throughput Cost**: ${throughput_cost:.2f}  
    **Total Monthly Cost**: ${total_cost:.2f}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Code Examples")
    
    tab1, tab2, tab3 = st.tabs(["Create Volume", "Attach Volume", "Snapshot Management"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create EBS volume using AWS CLI
aws ec2 create-volume \
    --size 100 \
    --volume-type gp3 \
    --availability-zone us-west-2a \
    --iops 3000 \
    --throughput 125 \
    --tag-specifications 'ResourceType=volume,Tags=[{Key=Name,Value=MyDataVolume}]'

# Python Boto3 - Create and manage EBS volumes
import boto3

ec2_client = boto3.client('ec2')

# Create volume
response = ec2_client.create_volume(
    AvailabilityZone='us-west-2a',
    Size=100,
    VolumeType='gp3',
    Iops=3000,
    Throughput=125,
    TagSpecifications=[
        {
            'ResourceType': 'volume',
            'Tags': [
                {'Key': 'Name', 'Value': 'MyDataVolume'},
                {'Key': 'Environment', 'Value': 'Production'}
            ]
        }
    ]
)

volume_id = response['VolumeId']
print(f"Created volume: {volume_id}")

# Wait for volume to be available
waiter = ec2_client.get_waiter('volume_available')
waiter.wait(VolumeIds=[volume_id])
print("Volume is now available!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Attach EBS volume to EC2 instance
import boto3

ec2_client = boto3.client('ec2')

# Attach volume to instance
response = ec2_client.attach_volume(
    Device='/dev/sdf',  # Device name
    InstanceId='i-1234567890abcdef0',
    VolumeId='vol-1234567890abcdef0'
)

print(f"Attaching volume to instance: {response['State']}")

# After attachment, format and mount on the instance
# SSH into your EC2 instance and run:

"""
# Check if volume is attached
lsblk

# Create file system (one time only)
sudo mkfs -t xfs /dev/xvdf

# Create mount point
sudo mkdir /data

# Mount the volume
sudo mount /dev/xvdf /data

# Add to /etc/fstab for persistent mounting
echo "/dev/xvdf /data xfs defaults,nofail 0 2" | sudo tee -a /etc/fstab

# Verify mount
df -h
''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# EBS Snapshot management
import boto3
from datetime import datetime, timedelta

ec2_client = boto3.client('ec2')

# Create snapshot
def create_snapshot(volume_id, description):
    response = ec2_client.create_snapshot(
        VolumeId=volume_id,
        Description=description,
        TagSpecifications=[
            {
                'ResourceType': 'snapshot',
                'Tags': [
                    {'Key': 'Name', 'Value': f'Snapshot-{volume_id}'},
                    {'Key': 'Backup', 'Value': 'Daily'}
                ]
            }
        ]
    )
    return response['SnapshotId']

# Automated backup function
def backup_volume(volume_id):
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    description = f"Automated backup of {volume_id} - {timestamp}"
    
    snapshot_id = create_snapshot(volume_id, description)
    print(f"Created snapshot: {snapshot_id}")
    
    return snapshot_id

# Delete old snapshots (retention policy)
def cleanup_old_snapshots(days=30):
    cutoff_date = datetime.now() - timedelta(days=days)
    
    response = ec2_client.describe_snapshots(OwnerIds=['self'])
    
    for snapshot in response['Snapshots']:
        if snapshot['StartTime'].replace(tzinfo=None) < cutoff_date:
            print(f"Deleting old snapshot: {snapshot['SnapshotId']}")
            ec2_client.delete_snapshot(SnapshotId=snapshot['SnapshotId'])

# Example usage
volume_id = 'vol-1234567890abcdef0'
backup_volume(volume_id)
cleanup_old_snapshots(30)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def transfer_family_tab():
    """Content for AWS Transfer Family tab"""
    st.markdown("## 🔄 AWS Transfer Family")
    st.markdown("*Easily manage and share data with simple, secure, and scalable file transfers*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    AWS Transfer Family securely scales your recurring business-to-business file transfers to AWS Storage services 
    using SFTP, FTPS, FTP, and AS2 protocols with fully managed infrastructure.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Transfer Family Architecture
    transfer_diagram = """
    graph TB
        subgraph "External Partners"
            SFTP_CLIENT[SFTP Client]
            FTPS_CLIENT[FTPS Client]
            FTP_CLIENT[FTP Client]
            AS2_CLIENT[AS2 Client]
        end
        
        subgraph "AWS Transfer Family"
            TRANSFER[Transfer Server<br/>Managed Endpoint]
        end
        
        subgraph "AWS Storage"
            S3[Amazon S3<br/>Bucket]
            EFS[Amazon EFS<br/>File System]
        end
        
        subgraph "Authentication"
            IAM[IAM Roles]
            LDAP[LDAP/AD]
            LAMBDA[Custom Lambda]
        end
        
        SFTP_CLIENT --> TRANSFER
        FTPS_CLIENT --> TRANSFER
        FTP_CLIENT --> TRANSFER
        AS2_CLIENT --> TRANSFER
        
        TRANSFER --> S3
        TRANSFER --> EFS
        
        TRANSFER -.-> IAM
        TRANSFER -.-> LDAP
        TRANSFER -.-> LAMBDA
        
        style TRANSFER fill:#FF9900,stroke:#232F3E,color:#fff
        style S3 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style EFS fill:#3FB34F,stroke:#232F3E,color:#fff
    """
    
    st.markdown("#### 🏗️ Transfer Family Architecture")
    common.mermaid(transfer_diagram, height=550)
    
    # Protocol comparison
    st.markdown("#### 📡 Supported Protocols")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔐 Secure Protocols
        **SFTP (SSH File Transfer Protocol)**
        - Encrypted file transfers
        - Public key authentication
        - Most commonly used
        
        **FTPS (FTP over SSL/TLS)**  
        - FTP with SSL/TLS encryption
        - Client certificate authentication
        - Industry standard for B2B
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📨 Business Protocols
        **AS2 (Applicability Statement 2)**
        - EDI and B2B messaging
        - Message integrity and non-repudiation
        - Healthcare and retail industries
        
        **FTP (File Transfer Protocol)**
        - Legacy protocol support
        - Not recommended for sensitive data
        - Unencrypted transfers
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive protocol selector
    st.markdown("#### 🤔 Protocol Recommendation Engine")
    
    col1, col2 = st.columns(2)
    
    with col1:
        use_case = st.selectbox("Select your use case:", [
            "Secure file sharing with partners",
            "EDI transactions with retailers", 
            "Legacy system integration",
            "Automated data exchange",
            "Compliance-heavy industry",
            "High-volume B2B transfers"
        ])
    
    with col2:
        security_requirement = st.selectbox("Security Requirements:", [
            "High - Encryption required",
            "Medium - Standard security",
            "Legacy - Minimal security"
        ])
    
    # Protocol recommendations
    recommendations = {
        ("Secure file sharing with partners", "High - Encryption required"): 
            ("SFTP", "Perfect for secure partner file sharing with public key authentication"),
        ("EDI transactions with retailers", "High - Encryption required"): 
            ("AS2", "Industry standard for EDI with message integrity and receipts"),
        ("Legacy system integration", "Legacy - Minimal security"): 
            ("FTP", "Supports legacy systems but consider upgrading to SFTP"),
        ("Automated data exchange", "High - Encryption required"): 
            ("SFTP", "Reliable automation with secure key-based authentication"),
        ("Compliance-heavy industry", "High - Encryption required"): 
            ("AS2", "Provides audit trails and non-repudiation for compliance"),
        ("High-volume B2B transfers", "High - Encryption required"): 
            ("FTPS", "Efficient for high-volume encrypted transfers")
    }
    
    key = (use_case, security_requirement)
    if key in recommendations:
        protocol, explanation = recommendations[key]
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Recommended Protocol: {protocol}
        {explanation}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Use cases and benefits
    st.markdown("#### 🌟 Real-World Use Cases")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏥 Healthcare
        **HIPAA Compliance**
        - Secure patient data exchange
        - AS2 with encryption
        - Audit logging
        - Partner integrations
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛒 Retail/E-commerce
        **Supply Chain**
        - EDI document exchange
        - Inventory updates
        - Partner onboarding
        - Automated processing
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏦 Financial Services
        **Regulatory Compliance**
        - Secure file transfers
        - Audit requirements
        - Data encryption
        - Access controls
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Code Examples")
    
    tab1, tab2 = st.tabs(["Setup Transfer Server", "User Management"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create AWS Transfer Family server using AWS CLI
aws transfer create-server \
    --protocols SFTP FTPS \
    --identity-provider-type SERVICE_MANAGED \
    --logging-role arn:aws:iam::123456789012:role/TransferLoggingRole \
    --tags Key=Name,Value=MyTransferServer

# Python Boto3 - Create Transfer Family server
import boto3

transfer_client = boto3.client('transfer')

# Create SFTP server
response = transfer_client.create_server(
    Protocols=['SFTP', 'FTPS'],
    IdentityProviderType='SERVICE_MANAGED',
    LoggingRole='arn:aws:iam::123456789012:role/TransferLoggingRole',
    Tags=[
        {
            'Key': 'Name',
            'Value': 'ProductionTransferServer'
        },
        {
            'Key': 'Environment', 
            'Value': 'Production'
        }
    ]
)

server_id = response['ServerId']
print(f"Created Transfer server: {server_id}")

# Wait for server to be online
waiter = transfer_client.get_waiter('server_online')
waiter.wait(ServerId=server_id)
print("Server is now online!")

# Get server endpoint
server_details = transfer_client.describe_server(ServerId=server_id)
endpoint = server_details['Server']['Endpoint']
print(f"Server endpoint: {endpoint}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create Transfer Family user with S3 access
import boto3
import json

transfer_client = boto3.client('transfer')

# Define user policy for S3 access
user_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::my-transfer-bucket/*"
        },
        {
            "Effect": "Allow", 
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::my-transfer-bucket",
            "Condition": {
                "StringLike": {
                    "s3:prefix": "home/${transfer:UserName}/*"
                }
            }
        }
    ]
}

# Create user
response = transfer_client.create_user(
    ServerId='s-1234567890abcdef0',
    UserName='partner-user',
    Role='arn:aws:iam::123456789012:role/TransferUserRole',
    HomeDirectory='/my-transfer-bucket/home/partner-user',
    Policy=json.dumps(user_policy),
    SshPublicKeyBody="""ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC... partner-user@company.com""",
    Tags=[
        {
            'Key': 'Partner',
            'Value': 'CompanyXYZ'
        }
    ]
)

print(f"Created user: {response['UserName']}")

# Test connection (from client side)
"""
# Connect using SFTP client
sftp -i /path/to/private-key partner-user@s-1234567890abcdef0.server.transfer.us-west-2.amazonaws.com

# Upload file
put local-file.txt remote-file.txt

# Download file  
get remote-file.txt local-file.txt
"""
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def redshift_tab():
    """Content for Amazon Redshift tab"""
    st.markdown("## 🏭 Amazon Redshift")
    st.markdown("*Power data driven decisions with the best price-performance cloud data warehouse*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    Amazon Redshift is a fully managed, petabyte-scale data warehouse service that provides 
    efficient storage, high-performance query processing, and agile scaling for analytics workloads.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Redshift Architecture
    st.markdown("#### 🏗️ Redshift Cluster Architecture")
    common.mermaid(create_redshift_architecture_mermaid(), height=700)
    
    # Node types comparison
    st.markdown("#### 📊 Node Types Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 💽 DC2 Nodes (Legacy)
        **Characteristics:**
        - Compute and storage scale together
        - SSD-based local storage
        - Fixed storage per node
        
        **Limitations:**
        - Must add nodes to increase storage
        - Pay for unused compute
        - Storage limited by node count
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚀 RA3 Nodes (Recommended)
        **Characteristics:**
        - Separate compute and storage scaling
        - Managed storage in S3
        - Pay only for what you use
        
        **Benefits:**
        - Scale compute independently
        - Automatic data tiering
        - Better price-performance
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive cluster calculator
    st.markdown("#### 🧮 Redshift Cluster Calculator")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        node_type = st.selectbox("Node Type", ["ra3.xlplus", "ra3.4xlarge", "ra3.16xlarge", "dc2.large", "dc2.8xlarge"])
        node_count = st.slider("Number of Nodes", 1, 32, 3)
    
    with col2:
        data_size_tb = st.slider("Data Size (TB)", 1, 100, 10)
        query_complexity = st.selectbox("Query Complexity", ["Simple", "Medium", "Complex"])
    
    with col3:
        concurrency = st.slider("Concurrent Users", 1, 50, 10)
    
    # Calculate estimated performance and cost
    node_specs = {
        "ra3.xlplus": {"vcpu": 4, "memory": 32, "storage": "Managed", "cost": 0.75},
        "ra3.4xlarge": {"vcpu": 12, "memory": 96, "storage": "Managed", "cost": 3.26}, 
        "ra3.16xlarge": {"vcpu": 48, "memory": 384, "storage": "Managed", "cost": 13.04},
        "dc2.large": {"vcpu": 2, "memory": 15, "storage": "160GB SSD", "cost": 0.25},
        "dc2.8xlarge": {"vcpu": 32, "memory": 244, "storage": "2560GB SSD", "cost": 4.80}
    }
    
    specs = node_specs[node_type]
    monthly_cost = specs["cost"] * node_count * 24 * 30
    
    # Storage cost for RA3
    if "ra3" in node_type:
        storage_cost = data_size_tb * 1024 * 0.024  # $0.024 per GB per month
        monthly_cost += storage_cost
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 💰 Cluster Configuration Summary
    **Node Type**: {node_type}  
    **Cluster Size**: {node_count} nodes  
    **Total vCPUs**: {specs['vcpu'] * node_count}  
    **Total Memory**: {specs['memory'] * node_count} GB  
    **Estimated Monthly Cost**: ${monthly_cost:.2f}  
    **Recommended for**: {data_size_tb}TB data, {concurrency} concurrent users
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Redshift features
    st.markdown("#### ⚡ Advanced Features")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔍 Redshift Spectrum
        - Query data directly in S3
        - No data movement required
        - Separate compute scaling
        - Supports multiple formats
        
        ### 🔗 Federated Queries
        - Query operational databases
        - Real-time data integration
        - Supports RDS and Aurora
        - Reduces ETL complexity
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Columnar Storage
        - Optimized for analytics
        - Better compression
        - Faster query performance
        - Reduced I/O operations
        
        ### 🎯 SUPER Data Type
        - Semi-structured data support
        - JSON document storage
        - PartiQL query language
        - Schema flexibility
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Code Examples")
    
    tab1, tab2, tab3 = st.tabs(["Cluster Creation", "Data Loading", "Query Optimization"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create Redshift cluster using AWS CLI
aws redshift create-cluster \
    --cluster-identifier my-redshift-cluster \
    --db-name mydb \
    --master-username admin \
    --master-user-password MyPassword123 \
    --node-type ra3.xlplus \
    --cluster-type multi-node \
    --number-of-nodes 3 \
    --vpc-security-group-ids sg-12345678

# Python Boto3 - Create Redshift cluster
import boto3

redshift_client = boto3.client('redshift')

# Create cluster
response = redshift_client.create_cluster(
    ClusterIdentifier='production-analytics',
    DBName='analytics',
    MasterUsername='admin',
    MasterUserPassword='SecurePassword123!',
    NodeType='ra3.4xlarge',
    ClusterType='multi-node',
    NumberOfNodes=3,
    VpcSecurityGroupIds=['sg-12345678'],
    ClusterSubnetGroupName='my-subnet-group',
    PubliclyAccessible=False,
    Encrypted=True,
    Tags=[
        {
            'Key': 'Environment',
            'Value': 'Production'
        },
        {
            'Key': 'Team',
            'Value': 'Analytics'
        }
    ]
)

cluster_id = response['Cluster']['ClusterIdentifier']
print(f"Creating cluster: {cluster_id}")

# Wait for cluster to be available
waiter = redshift_client.get_waiter('cluster_available')
waiter.wait(ClusterIdentifier=cluster_id)
print("Cluster is now available!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Load data from S3 using COPY command
COPY sales_data FROM 's3://my-data-bucket/sales/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3Role'  
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
REGION 'us-west-2';

-- Load JSON data
COPY user_events FROM 's3://my-data-bucket/events/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3Role'
JSON 'auto'
REGION 'us-west-2';

-- Load with error handling
COPY product_catalog FROM 's3://my-data-bucket/products/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3Role'
CSV
MAXERROR 100
ACCEPTINVCHARS;

-- Check load status
SELECT query, filename, line_number, colname, type, err_reason
FROM stl_load_errors
WHERE query = pg_last_copy_id()
ORDER BY query DESC;

-- Python script for automated loading
import boto3
import psycopg2

def load_data_to_redshift(s3_path, table_name, iam_role):
    # Connect to Redshift
    conn = psycopg2.connect(
        host='my-cluster.abc123.us-west-2.redshift.amazonaws.com',
        port=5439,
        dbname='analytics',
        user='admin',
        password='password'
    )
    
    cursor = conn.cursor()
    
    # Execute COPY command
    copy_sql = f"""
    COPY {table_name} FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    CSV DELIMITER ','
    IGNOREHEADER 1
    REGION 'us-west-2';
    """
    
    cursor.execute(copy_sql)
    conn.commit()
    
    print(f"Data loaded successfully to {table_name}")
    
    cursor.close()
    conn.close()

# Usage
load_data_to_redshift(
    's3://my-bucket/sales-data/',
    'sales_fact',
    'arn:aws:iam::123456789012:role/RedshiftS3Role'
)
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Table design best practices
CREATE TABLE sales_fact (
    sale_id BIGINT IDENTITY(1,1),
    customer_id INTEGER,
    product_id INTEGER,
    sale_date DATE,
    amount DECIMAL(10,2),
    quantity INTEGER,
    region VARCHAR(50)
)
-- Distribution key for even data distribution
DISTKEY(customer_id)
-- Sort key for query optimization
SORTKEY(sale_date, customer_id)
-- Automatic compression
ENCODE AUTO;

-- Create dimension table with ALL distribution
CREATE TABLE product_dim (
    product_id INTEGER,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2)
)
-- Distribute to all nodes (small lookup table)
DISTSTYLE ALL
SORTKEY(product_id);

-- Analyze table statistics for query optimizer
ANALYZE sales_fact;
ANALYZE product_dim;

-- Query optimization techniques
-- Use columnar storage benefits
SELECT 
    region,
    DATE_TRUNC('month', sale_date) as month,
    SUM(amount) as total_sales,
    COUNT(*) as transaction_count
FROM sales_fact
WHERE sale_date >= '2024-01-01'
    AND sale_date < '2025-01-01'
GROUP BY region, DATE_TRUNC('month', sale_date)
ORDER BY region, month;

-- Use Redshift Spectrum for data lake queries
CREATE EXTERNAL SCHEMA spectrum_schema
FROM DATA CATALOG
DATABASE 'my_glue_database'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftSpectrumRole';

-- Query S3 data with Spectrum
SELECT 
    f.region,
    s.product_category,
    SUM(f.amount) as total_sales
FROM sales_fact f
JOIN spectrum_schema.s3_product_catalog s
    ON f.product_id = s.product_id
WHERE f.sale_date >= CURRENT_DATE - 30
GROUP BY f.region, s.product_category;

-- Monitor query performance
SELECT query, elapsed, rows, bytes
FROM svl_query_summary 
WHERE query = [query_id]
ORDER BY query;
        ''', language='sql')
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
    # 🗄️ AWS Data Store Management
    <div class='info-box'>Evaluate and implement appropriate AWS storage solutions (S3, EFS, EBS) and data management services (Transfer Family, Redshift) based on specific use case requirements, performance needs, and cost optimization goals.
    </div>""", unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🗄️ Managing Data Within AWS",
        "📦 Amazon S3", 
        "📂 Amazon EFS",
        "💾 Amazon EBS",
        "🔄 AWS Transfer Family",
        "🏭 Amazon Redshift"
    ])
    
    with tab1:
        managing_data_tab()
    
    with tab2:
        s3_tab()
    
    with tab3:
        efs_tab()
    
    with tab4:
        ebs_tab()
    
    with tab5:
        transfer_family_tab()
    
    with tab6:
        redshift_tab()
    
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
