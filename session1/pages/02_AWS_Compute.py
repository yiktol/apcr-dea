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
    page_title="AWS Compute Services Learning Hub",
    page_icon="💻",
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
        
        .service-comparison {{
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
            - 🖥️ Amazon EC2 - Virtual servers in the cloud
            - 📦 Amazon Machine Images (AMI) - Server templates
            - 🐳 Amazon ECS - Container orchestration service
            - ☸️ Amazon EKS - Managed Kubernetes service
            - 🚀 AWS Fargate - Serverless container compute
            
            **Learning Objectives:**
            - Understand different AWS compute options
            - Learn when to use each service
            - Explore container vs virtual machine concepts
            - Practice with interactive examples and code
            """)
        
def create_ec2_architecture_mermaid():
    """Create mermaid diagram for EC2 architecture"""
    return """
    graph TD
        A[Amazon EC2] --> B[Virtual Machine]
        B --> C[Choose AMI]
        B --> D[Select Instance Type]
        B --> E[Configure Storage]
        B --> F[Security Groups]
        
        C --> C1[Amazon Linux 2]
        C --> C2[Ubuntu]
        C --> C3[Windows Server]
        C --> C4[Custom AMI]
        
        D --> D1[t3.micro - General Purpose]
        D --> D2[c5.large - Compute Optimized]
        D --> D3[r5.xlarge - Memory Optimized]
        D --> D4[m5.2xlarge - Balanced]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#3FB34F,stroke:#232F3E,color:#fff
        style E fill:#3FB34F,stroke:#232F3E,color:#fff
        style F fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_ami_lifecycle_mermaid():
    """Create mermaid diagram for AMI lifecycle"""
    return """
    graph LR
        A[📦 Base AMI] --> B[🖥️ Launch EC2 Instance]
        B --> C[⚙️ Install Software & Configure]
        C --> D[📸 Create Image/Snapshot]
        D --> E[🗂️ Register Custom AMI]
        E --> F[🚀 Launch Multiple Instances]
        
        F --> G[🔄 Update & Iterate]
        G --> C
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#FF9900,stroke:#232F3E,color:#fff
        style E fill:#4B9EDB,stroke:#232F3E,color:#fff
        style F fill:#3FB34F,stroke:#232F3E,color:#fff
        style G fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_container_vs_vm_mermaid():
    """Create mermaid diagram comparing containers vs VMs"""
    return """
    graph TD
        subgraph "Virtual Machines"
            VM1[Application A]
            VM2[Application B]
            VM3[Application C]
            OS1[Guest OS]
            OS2[Guest OS]
            OS3[Guest OS]
            HYP[Hypervisor]
            HOS[Host Operating System]
            HW1[Physical Hardware]
            
            VM1 --> OS1
            VM2 --> OS2
            VM3 --> OS3
            OS1 --> HYP
            OS2 --> HYP
            OS3 --> HYP
            HYP --> HOS
            HOS --> HW1
        end
        
        subgraph "Containers"
            C1[Application A]
            C2[Application B]
            C3[Application C]
            CE[Container Engine/Docker]
            COS[Host Operating System]
            HW2[Physical Hardware]
            
            C1 --> CE
            C2 --> CE
            C3 --> CE
            CE --> COS
            COS --> HW2
        end
        
        style VM1 fill:#FF9900,stroke:#232F3E,color:#fff
        style VM2 fill:#FF9900,stroke:#232F3E,color:#fff
        style VM3 fill:#FF9900,stroke:#232F3E,color:#fff
        style C1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C2 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C3 fill:#4B9EDB,stroke:#232F3E,color:#fff
    """

def create_ecs_architecture_mermaid():
    """Create mermaid diagram for ECS architecture"""
    return """
    graph TD
        A[Amazon ECS] --> B[ECS Cluster]
        B --> C[EC2 Launch Type]
        B --> D[Fargate Launch Type]
        
        C --> E[EC2 Instances]
        E --> F[ECS Agent]
        F --> G[Docker Containers]
        
        D --> H[Serverless Containers]
        
        B --> I[Task Definitions]
        I --> J[Container Image]
        I --> K[CPU/Memory]
        I --> L[Network Config]
        
        B --> M[Services]
        M --> N[Load Balancer]
        M --> O[Auto Scaling]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#3FB34F,stroke:#232F3E,color:#fff
        style M fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_eks_architecture_mermaid():
    """Create mermaid diagram for EKS architecture"""
    return """
    graph TD
        A[Amazon EKS] --> B[Control Plane]
        A --> C[Worker Nodes]
        
        B --> D[API Server]
        B --> E[etcd]
        B --> F[Controller Manager]
        B --> G[Scheduler]
        
        C --> H[EC2 Instances]
        C --> I[Fargate Pods]
        
        H --> J[kubelet]
        H --> K[Container Runtime]
        H --> L[Pods]
        
        I --> M[Serverless Pods]
        
        B --> N[kubectl/API]
        N --> O[Deploy Applications]
        O --> P[Services & Ingress]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style H fill:#232F3E,stroke:#FF9900,color:#fff
        style I fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_instance_type_comparison():
    """Create comparison chart for EC2 instance types"""
    data = {
        'Instance Type': ['t3.micro', 't3.small', 'm5.large', 'c5.xlarge', 'r5.xlarge'],
        'vCPUs': [2, 2, 2, 4, 4],
        'Memory (GB)': [1, 2, 8, 8, 32],
        'Storage': ['EBS Only', 'EBS Only', 'EBS Only', 'EBS Only', 'EBS Only'],
        'Use Case': ['Low Traffic', 'Small Apps', 'General Purpose', 'Compute Intensive', 'Memory Intensive'],
        'Cost ($/hour)': [0.0104, 0.0208, 0.096, 0.192, 0.252]
    }
    
    df = pd.DataFrame(data)
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('vCPUs Comparison', 'Memory Comparison', 'Cost Comparison', 'Use Cases'),
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "table"}]]
    )
    
    # vCPUs comparison
    fig.add_trace(go.Bar(x=df['Instance Type'], y=df['vCPUs'], 
                        marker_color=AWS_COLORS['primary'], name='vCPUs'),
                  row=1, col=1)
    
    # Memory comparison
    fig.add_trace(go.Bar(x=df['Instance Type'], y=df['Memory (GB)'], 
                        marker_color=AWS_COLORS['light_blue'], name='Memory'),
                  row=1, col=2)
    
    # Cost comparison
    fig.add_trace(go.Bar(x=df['Instance Type'], y=df['Cost ($/hour)'], 
                        marker_color=AWS_COLORS['warning'], name='Cost'),
                  row=2, col=1)
    
    # Use cases table
    fig.add_trace(go.Table(
        header=dict(values=['Instance Type', 'Use Case'],
                   fill_color=AWS_COLORS['primary'],
                   font_color='white'),
        cells=dict(values=[df['Instance Type'], df['Use Case']],
                  fill_color=AWS_COLORS['light_gray'])),
        row=2, col=2)
    
    fig.update_layout(height=600, showlegend=False, title_text="EC2 Instance Types Comparison")
    
    return fig

def amazon_ec2_tab():
    """Content for Amazon EC2 tab"""
    st.markdown("# 🖥️ Amazon EC2")
    st.markdown("*Secure and resizable compute capacity to support virtually any workload*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Amazon Elastic Compute Cloud (EC2)** provides secure, resizable compute capacity in the cloud. 
    It's like having virtual servers that you can launch, configure, and scale on-demand.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # EC2 Architecture
    st.markdown("## 🏗️ EC2 Architecture Overview")
    common.mermaid(create_ec2_architecture_mermaid(), height=200)
    
    # Interactive EC2 Instance Builder
    st.markdown("## 🔧 Interactive EC2 Instance Builder")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        ami_choice = st.selectbox("Choose AMI:", [
            "Amazon Linux 2", "Ubuntu 20.04", "Windows Server 2022", "Red Hat Enterprise Linux"
        ])
    
    with col2:
        instance_type = st.selectbox("Instance Type:", [
            "t3.micro (1 vCPU, 1GB RAM)",
            "t3.small (1 vCPU, 2GB RAM)", 
            "m5.large (2 vCPU, 8GB RAM)",
            "c5.xlarge (4 vCPU, 8GB RAM)"
        ])
    
    with col3:
        storage = st.slider("Storage (GB):", 8, 100, 20)
    
    if st.button("🚀 Launch Instance (Simulation)", use_container_width=True):
       
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Instance Configured Successfully!
        
        **Instance Details:**
        - **AMI**: {ami_choice}
        - **Type**: {instance_type}
        - **Storage**: {storage} GB EBS
        - **Instance ID**: i-{np.random.randint(100000000, 999999999):x}
        - **Status**: Running
        
        **Estimated Cost**: ${np.random.uniform(0.01, 0.25):.3f}/hour
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Instance Types Comparison
    st.markdown("## 📊 Instance Types Comparison")
    st.plotly_chart(create_instance_type_comparison(), use_container_width=True)
    
    # Use Cases
    st.markdown("## 🌟 Common Use Cases")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🌐 Web Applications
        - Host websites and web applications
        - Scale automatically with demand
        - Load balance across multiple instances
        - **Example**: E-commerce platform
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗄️ Database Servers
        - Run database workloads
        - High IOPS for fast data access
        - Memory-optimized instances
        - **Example**: MySQL, PostgreSQL
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🧠 Machine Learning
        - GPU instances for training
        - High-performance computing
        - Scalable model inference
        - **Example**: Deep learning models
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Launch EC2 Instance")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Launch EC2 instance using Boto3
import boto3

ec2 = boto3.resource('ec2', region_name='us-east-1')

# Launch instance
instances = ec2.create_instances(
    ImageId='ami-0abcdef1234567890',  # Amazon Linux 2 AMI
    MinCount=1,
    MaxCount=1,
    InstanceType='t3.micro',
    KeyName='my-key-pair',
    SecurityGroupIds=['sg-12345678'],
    SubnetId='subnet-12345678',
    UserData='''#!/bin/bash
    yum update -y
    yum install -y httpd
    systemctl start httpd
    systemctl enable httpd
    echo "<h1>Hello from EC2!</h1>" > /var/www/html/index.html
    ''',
    TagSpecifications=[
        {
            'ResourceType': 'instance',
            'Tags': [
                {'Key': 'Name', 'Value': 'My Web Server'},
                {'Key': 'Environment', 'Value': 'Production'}
            ]
        }
    ]
)

instance = instances[0]
print(f"Instance {instance.id} is launching...")

# Wait for instance to be running
instance.wait_until_running()
print(f"Instance {instance.id} is now running!")
print(f"Public IP: {instance.public_ip_address}")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def amazon_ami_tab():
    """Content for Amazon Machine Images tab"""
    st.markdown("# 📦 Amazon Machine Images (AMI)")
    st.markdown("*Critical information needed when launching EC2 instances*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    An **Amazon Machine Image (AMI)** is like a blueprint or template that contains all the information 
    needed to launch an EC2 instance, including the operating system, applications, and configurations.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # AMI Lifecycle
    st.markdown("## 🔄 AMI Lifecycle")
    common.mermaid(create_ami_lifecycle_mermaid(), height=150)
    
    # Interactive AMI Builder
    st.markdown("## 🛠️ Interactive AMI Creator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        base_os = st.selectbox("Choose Base OS:", [
            "Amazon Linux 2", "Ubuntu 20.04 LTS", "Windows Server 2022", "CentOS 8"
        ])
        
        applications = st.multiselect("Select Applications to Install:", [
            "Apache Web Server", "Nginx", "MySQL", "PostgreSQL", "Docker", 
            "Node.js", "Python 3.9", "Java 11", "Git"
        ])
    
    with col2:
        configurations = st.multiselect("Additional Configurations:", [
            "Auto-start services", "Security hardening", "Monitoring agents",
            "Log forwarding", "SSL certificates", "Backup scripts"
        ])
        
        ami_name = st.text_input("AMI Name:", "my-custom-ami-v1.0")
    
    if st.button("🏗️ Create Custom AMI (Simulation)", use_container_width=True):
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Custom AMI Created Successfully!
        
        **AMI Details:**
        - **Name**: {ami_name}
        - **Base OS**: {base_os}
        - **Applications**: {', '.join(applications) if applications else 'None'}
        - **Configurations**: {', '.join(configurations) if configurations else 'None'}
        - **AMI ID**: ami-{np.random.randint(100000000, 999999999):x}
        - **Status**: Available
        
        🚀 You can now launch multiple instances from this AMI!
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # AMI Types Comparison
    st.markdown("## 📋 AMI Types Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏪 AWS Marketplace AMIs
        - **Pre-configured solutions**
        - Third-party software included
        - Often paid (usage-based pricing)
        - **Examples**: WordPress, MongoDB, SAP
        
        ### 🌐 Community AMIs  
        - **Free AMIs shared by community**
        - Use with caution (verify security)
        - Wide variety of configurations
        - **Examples**: Specialized Linux distros
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏢 AWS Public AMIs
        - **Official AMIs from AWS**
        - Regularly updated and patched
        - Free to use (pay for EC2 usage)
        - **Examples**: Amazon Linux, Ubuntu
        
        ### 👤 My AMIs
        - **Custom AMIs you create**
        - Your applications pre-installed
        - Private by default
        - **Examples**: Your web app stack
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Best Practices
    st.markdown("## 💡 AMI Best Practices")
    
    practices_data = {
        'Practice': [
            'Regular Updates', 'Security Patches', 'Minimal Install', 
            'Documentation', 'Version Control', 'Testing'
        ],
        'Description': [
            'Keep base AMIs updated with latest packages',
            'Apply security patches before creating AMI',
            'Only install necessary software to reduce attack surface',
            'Document what\'s included in each AMI version',
            'Use semantic versioning for AMI names',
            'Test AMIs thoroughly before production use'
        ],
        'Impact': [
            'High', 'Critical', 'Medium', 'Medium', 'High', 'Critical'
        ]
    }
    
    df_practices = pd.DataFrame(practices_data)
    
    fig = px.bar(df_practices, x='Practice', y=[1]*len(df_practices), 
                 color='Impact', 
                 title='AMI Management Best Practices',
                 color_discrete_map={
                     'Critical': AWS_COLORS['warning'],
                     'High': AWS_COLORS['primary'],
                     'Medium': AWS_COLORS['light_blue']
                 })
    fig.update_layout(showlegend=True, yaxis_title='Importance')
    st.plotly_chart(fig, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Creating and Using AMIs")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Create AMI from existing instance
import boto3

ec2 = boto3.client('ec2')

# Create AMI from running instance
response = ec2.create_image(
    InstanceId='i-1234567890abcdef0',
    Name='my-web-server-v1.0',
    Description='Web server with Apache and PHP configured',
    NoReboot=True,  # Don't reboot instance during AMI creation
    TagSpecifications=[
        {
            'ResourceType': 'image',
            'Tags': [
                {'Key': 'Name', 'Value': 'WebServer-AMI'},
                {'Key': 'Version', 'Value': '1.0'},
                {'Key': 'Environment', 'Value': 'Production'}
            ]
        }
    ]
)

ami_id = response['ImageId']
print(f"AMI creation started: {ami_id}")

# Wait for AMI to be available
waiter = ec2.get_waiter('image_available')
waiter.wait(ImageIds=[ami_id])
print(f"AMI {ami_id} is now available!")

# Launch instance from custom AMI
ec2_resource = boto3.resource('ec2')
instances = ec2_resource.create_instances(
    ImageId=ami_id,
    MinCount=1,
    MaxCount=3,  # Launch 3 identical instances
    InstanceType='t3.small'
)

print(f"Launched {len(instances)} instances from custom AMI")
for instance in instances:
    print(f"Instance ID: {instance.id}")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def amazon_ecs_tab():
    """Content for Amazon Elastic Container Service tab"""
    st.markdown("# 🐳 Amazon Elastic Container Service")
    st.markdown("*Highly secure, reliable, and scalable way to run containers*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Amazon ECS** is a fully managed container orchestration service that helps you easily deploy, 
    manage, and scale containerized applications. Think of it as a smart manager for your containerized applications.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Container vs VM Comparison
    st.markdown("## 🆚 Containers vs Virtual Machines")
    common.mermaid(create_container_vs_vm_mermaid(), height=400)
    
    # ECS Architecture
    st.markdown("## 🏗️ ECS Architecture")
    common.mermaid(create_ecs_architecture_mermaid(), height=450)
    
    # Interactive ECS Service Builder
    st.markdown("## 🔧 Interactive ECS Service Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        cluster_name = st.text_input("Cluster Name:", "my-ecs-cluster")
        launch_type = st.selectbox("Launch Type:", ["Fargate (Serverless)", "EC2 (Manage instances)"])
        container_image = st.selectbox("Container Image:", [
            "nginx:latest", "httpd:latest", "node:16-alpine", "python:3.9-slim"
        ])
    
    with col2:
        task_cpu = st.selectbox("Task CPU:", ["256 (.25 vCPU)", "512 (.5 vCPU)", "1024 (1 vCPU)"])
        task_memory = st.selectbox("Task Memory:", ["512 MB", "1024 MB", "2048 MB"])
        desired_count = st.slider("Desired Task Count:", 1, 10, 2)
    
    if st.button("🚀 Deploy ECS Service (Simulation)", use_container_width=True):
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ ECS Service Deployed Successfully!
        
        **Service Details:**
        - **Cluster**: {cluster_name}
        - **Launch Type**: {launch_type}
        - **Container**: {container_image}
        - **CPU/Memory**: {task_cpu} / {task_memory}
        - **Running Tasks**: {desired_count}
        - **Service ARN**: arn:aws:ecs:us-east-1:123456789:service/{cluster_name}/my-service
        
        🔄 ECS will maintain {desired_count} healthy tasks automatically!
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # ECS Components Deep Dive
    st.markdown("## 🧩 ECS Components")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏢 Cluster
        - **Logical grouping** of compute resources
        - Can span multiple AZs
        - Manages capacity and placement
        - **Example**: Production-Web-Cluster
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📋 Task Definition
        - **Blueprint** for your application
        - Specifies container images, CPU, memory
        - Environment variables and networking
        - **Example**: Web-App-Task-Def:1
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚙️ Service
        - **Ensures desired number** of tasks running
        - Handles load balancing and health checks
        - Automatic replacement of failed tasks
        - **Example**: my-web-service
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Launch Types Comparison
    st.markdown("## 🚀 Launch Types Comparison")
    
    comparison_data = {
        'Feature': ['Server Management', 'Pricing Model', 'Control Level', 'Scaling Speed', 'Use Case'],
        'Fargate': ['None (Serverless)', 'Pay per task', 'Less control', 'Fast', 'Simple containerized apps'],
        'EC2': ['You manage', 'Pay for instances', 'Full control', 'Moderate', 'Custom requirements']
    }
    
    df_comparison = pd.DataFrame(comparison_data)
    st.table(df_comparison)
    
    # Code Example
    st.markdown("## 💻 Code Example: ECS Task Definition")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Create ECS task definition and service
import boto3
import json

ecs = boto3.client('ecs')

# Define task definition
task_definition = {
    "family": "my-web-app",
    "networkMode": "awsvpc",
    "requiresCompatibilities": ["FARGATE"],
    "cpu": "256",
    "memory": "512",
    "executionRoleArn": "arn:aws:iam::123456789:role/ecsTaskExecutionRole",
    "containerDefinitions": [
        {
            "name": "web-container",
            "image": "nginx:latest",
            "portMappings": [
                {
                    "containerPort": 80,
                    "protocol": "tcp"
                }
            ],
            "essential": True,
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": "/ecs/my-web-app",
                    "awslogs-region": "us-east-1",
                    "awslogs-stream-prefix": "ecs"
                }
            }
        }
    ]
}

# Register task definition
response = ecs.register_task_definition(**task_definition)
task_def_arn = response['taskDefinition']['taskDefinitionArn']
print(f"Task definition registered: {task_def_arn}")

# Create ECS service
service_response = ecs.create_service(
    cluster='my-cluster',
    serviceName='my-web-service',
    taskDefinition=task_def_arn,
    desiredCount=2,
    launchType='FARGATE',
    networkConfiguration={
        'awsvpcConfiguration': {
            'subnets': ['subnet-12345', 'subnet-67890'],
            'securityGroups': ['sg-abcdef'],
            'assignPublicIp': 'ENABLED'
        }
    }
)

print(f"Service created: {service_response['service']['serviceName']}")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def amazon_eks_tab():
    """Content for Amazon Elastic Kubernetes Service tab"""
    st.markdown("# ☸️ Amazon Elastic Kubernetes Service")
    st.markdown("*The most trusted way to start, run, and scale Kubernetes*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Amazon EKS** is a managed Kubernetes service that runs Kubernetes control plane across multiple 
    Availability Zones. It's like having a team of Kubernetes experts managing your cluster infrastructure.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # EKS Architecture
    st.markdown("## 🏗️ EKS Architecture")
    common.mermaid(create_eks_architecture_mermaid(), height=350)
    
    # Interactive EKS Cluster Builder
    st.markdown("## 🔧 Interactive EKS Cluster Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        cluster_name = st.text_input("Cluster Name:", "my-eks-cluster")
        k8s_version = st.selectbox("Kubernetes Version:", ["1.27", "1.26", "1.25"])
        node_type = st.selectbox("Worker Node Type:", [
            "Managed Node Groups", "Fargate Profiles", "Self-managed Nodes"
        ])
    
    with col2:
        instance_types = st.multiselect("Instance Types:", [
            "t3.medium", "t3.large", "m5.large", "m5.xlarge", "c5.large"
        ], default=["t3.medium"])
        
        min_nodes = st.number_input("Min Nodes:", 1, 10, 1)
        max_nodes = st.number_input("Max Nodes:", 1, 100, 5)
    
    # Networking configuration
    st.markdown("### 🌐 Networking Configuration")
    col3, col4 = st.columns(2)
    
    with col3:
        vpc_config = st.selectbox("VPC:", ["Create new VPC", "Use existing VPC"])
        public_access = st.checkbox("Enable public API server access", value=True)
    
    with col4:
        private_access = st.checkbox("Enable private API server access", value=True)
        logging_types = st.multiselect("CloudWatch Logging:", [
            "API", "Audit", "Authenticator", "ControllerManager", "Scheduler"
        ])
    
    if st.button("🚀 Create EKS Cluster (Simulation)", use_container_width=True):
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ EKS Cluster Configuration Complete!
        
        **Cluster Details:**
        - **Name**: {cluster_name}
        - **Kubernetes Version**: {k8s_version}
        - **Worker Nodes**: {node_type}
        - **Instance Types**: {', '.join(instance_types)}
        - **Auto Scaling**: {min_nodes} - {max_nodes} nodes
        - **API Access**: Public: {public_access}, Private: {private_access}
        
        ⏱️ **Estimated Creation Time**: 10-15 minutes
        💰 **Estimated Cost**: $0.10/hour (control plane) + worker node costs
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # EKS vs ECS Comparison
    st.markdown("## ⚔️ EKS vs ECS Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="service-comparison">', unsafe_allow_html=True)
        st.markdown("""
        ### ☸️ Amazon EKS
        
        **Best for:**
        - Teams familiar with Kubernetes
        - Complex microservices architectures  
        - Multi-cloud deployments
        - Need for Kubernetes ecosystem tools
        
        **Characteristics:**
        - Uses standard Kubernetes APIs
        - Steeper learning curve
        - More flexibility and control
        - Larger ecosystem of tools
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="service-comparison">', unsafe_allow_html=True)
        st.markdown("""
        ### 🐳 Amazon ECS
        
        **Best for:**
        - AWS-native applications
        - Teams new to containers
        - Simple container orchestration
        - Tight AWS service integration
        
        **Characteristics:**
        - AWS-specific APIs
        - Easier to get started
        - Less complexity
        - Deep AWS integration
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Kubernetes Concepts
    st.markdown("## 📚 Key Kubernetes Concepts")
    
    k8s_concepts = {
        'Concept': ['Pod', 'Deployment', 'Service', 'Ingress', 'ConfigMap', 'Secret'],
        'Description': [
            'Smallest deployable unit, contains one or more containers',
            'Manages replicated applications and rolling updates',
            'Exposes pods as network service with load balancing',
            'Manages external access and SSL termination',
            'Stores non-sensitive configuration data',
            'Stores sensitive data like passwords and API keys'
        ],
        'Use Case': [
            'Running application containers',
            'Scalable web applications',
            'Service discovery and load balancing',
            'HTTP routing and SSL certificates',
            'Application configuration',
            'Database credentials, API tokens'
        ]
    }
    
    df_k8s = pd.DataFrame(k8s_concepts)
    st.dataframe(df_k8s, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Deploy to EKS")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Deploy application to EKS using kubectl and Python Kubernetes client
from kubernetes import client, config
import yaml

# Load kubeconfig (assumes AWS CLI and kubectl configured)
config.load_kube_config()

# Create Kubernetes API client
v1 = client.AppsV1Api()
core_v1 = client.CoreV1Api()

# Define deployment manifest
deployment_manifest = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {
        "name": "my-web-app",
        "labels": {"app": "web"}
    },
    "spec": {
        "replicas": 3,
        "selector": {"matchLabels": {"app": "web"}},
        "template": {
            "metadata": {"labels": {"app": "web"}},
            "spec": {
                "containers": [{
                    "name": "web-container",
                    "image": "nginx:1.20",
                    "ports": [{"containerPort": 80}],
                    "resources": {
                        "requests": {"memory": "64Mi", "cpu": "250m"},
                        "limits": {"memory": "128Mi", "cpu": "500m"}
                    }
                }]
            }
        }
    }
}

# Create deployment
try:
    v1.create_namespaced_deployment(
        body=deployment_manifest,
        namespace="default"
    )
    print("Deployment created successfully!")
except Exception as e:
    print(f"Error creating deployment: {e}")

# Create service to expose the deployment
service_manifest = {
    "apiVersion": "v1",
    "kind": "Service",
    "metadata": {"name": "my-web-service"},
    "spec": {
        "selector": {"app": "web"},
        "ports": [{"port": 80, "targetPort": 80}],
        "type": "LoadBalancer"
    }
}

try:
    core_v1.create_namespaced_service(
        body=service_manifest,
        namespace="default"
    )
    print("Service created successfully!")
except Exception as e:
    print(f"Error creating service: {e}")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def aws_fargate_tab():
    """Content for AWS Fargate tab"""
    st.markdown("# 🚀 AWS Fargate")
    st.markdown("*Serverless compute for containers*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **AWS Fargate** is a serverless compute engine that lets you run containers without managing servers or clusters. 
    It's like having a valet service for your containers - you just specify what you want, and Fargate handles everything else.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Fargate Benefits Visualization
    st.markdown("## ✨ Fargate Benefits")
    
    benefits_data = {
        'Benefit': ['No Server Management', 'Pay-per-Use', 'Automatic Scaling', 
                   'Security Isolation', 'Fast Startup', 'Resource Efficiency'],
        'Traditional Approach': [2, 3, 2, 3, 2, 2],
        'With Fargate': [10, 9, 10, 9, 9, 10],
        'Category': ['Operations', 'Cost', 'Performance', 'Security', 'Performance', 'Efficiency']
    }
    
    df_benefits = pd.DataFrame(benefits_data)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Traditional Approach',
        x=df_benefits['Benefit'],
        y=df_benefits['Traditional Approach'],
        marker_color=AWS_COLORS['secondary']
    ))
    
    fig.add_trace(go.Bar(
        name='With Fargate',
        x=df_benefits['Benefit'],
        y=df_benefits['With Fargate'],
        marker_color=AWS_COLORS['primary']
    ))
    
    fig.update_layout(
        title='Fargate vs Traditional Container Management',
        xaxis_title='Benefits',
        yaxis_title='Score (1-10)',
        barmode='group',
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Interactive Fargate Task Builder
    st.markdown("## 🛠️ Interactive Fargate Task Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Container Configuration")
        container_name = st.text_input("Container Name:", "my-app-container")
        image_uri = st.selectbox("Container Image:", [
            "nginx:latest",
            "httpd:2.4", 
            "node:16-alpine",
            "python:3.9-slim",
            "Custom ECR Image"
        ])
        
        port = st.number_input("Container Port:", 1, 65535, 80)
    
    with col2:
        st.markdown("### Resource Allocation")
        cpu_units = st.selectbox("CPU (vCPU):", [
            "256 (.25 vCPU)", "512 (.5 vCPU)", "1024 (1 vCPU)", 
            "2048 (2 vCPU)", "4096 (4 vCPU)"
        ])
        
        memory = st.selectbox("Memory (MB):", [
            "512", "1024", "2048", "4096", "8192", "16384"
        ])
        
        storage = st.slider("Ephemeral Storage (GB):", 20, 200, 20)
    
    # Environment variables
    st.markdown("### Environment Variables")
    env_vars = st.text_area("Environment Variables (KEY=VALUE, one per line):", 
                           "NODE_ENV=production\nPORT=3000")
    
    if st.button("🚀 Deploy Fargate Task (Simulation)", use_container_width=True):
        
        # Calculate estimated cost
        cpu_cost = float(cpu_units.split()[0]) * 0.04048  # $0.04048 per vCPU per hour
        memory_cost = float(memory) / 1024 * 0.004445    # $0.004445 per GB per hour
        hourly_cost = cpu_cost + memory_cost
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Fargate Task Deployed Successfully!
        
        **Task Details:**
        - **Container**: {container_name}
        - **Image**: {image_uri}
        - **Resources**: {cpu_units}, {memory} MB RAM
        - **Storage**: {storage} GB ephemeral
        - **Port**: {port}
        - **Task ARN**: arn:aws:ecs:us-east-1:123456789:task/my-cluster/{np.random.randint(100000, 999999)}
        
        💰 **Estimated Cost**: ${hourly_cost:.4f}/hour
        ⚡ **Cold Start**: < 30 seconds
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Use Cases
    st.markdown("## 🌟 Fargate Use Cases")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🤖 AI/ML Applications
        - **Flexible development environments**
        - On-demand compute for training
        - Isolated model inference
        - **Example**: Jupyter notebooks, model serving
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Data Processing
        - **Batch job processing**
        - ETL pipelines
        - Scale up to 16 vCPU, 120GB memory
        - **Example**: Log analysis, data transformation
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🌐 Web Applications
        - **Microservices architectures**
        - API backends
        - Seasonal traffic handling
        - **Example**: REST APIs, web services
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Fargate vs EC2 Cost Comparison
    st.markdown("## 💰 Cost Comparison: Fargate vs EC2")
    
    hours = st.slider("Usage Hours per Month:", 0, 744, 100)  # 744 = hours in a month
    
    # Sample calculations (simplified)
    fargate_cost = hours * 0.04048 * 0.25  # 0.25 vCPU
    fargate_memory_cost = hours * 0.004445 * 0.5  # 0.5 GB
    total_fargate = fargate_cost + fargate_memory_cost
    
    ec2_cost = hours * 0.0464  # t3.micro on-demand pricing
    
    cost_data = pd.DataFrame({
        'Service': ['Fargate', 'EC2 (t3.micro)'],
        'Monthly Cost': [total_fargate, ec2_cost],
        'Break-even Hours': [total_fargate/0.0464, ec2_cost/0.0464]
    })
    
    fig = px.bar(cost_data, x='Service', y='Monthly Cost', 
                 title=f'Monthly Cost Comparison ({hours} hours usage)',
                 color='Service',
                 color_discrete_sequence=[AWS_COLORS['primary'], AWS_COLORS['light_blue']])
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown('<div class="warning-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 💡 Cost Analysis
    
    **For {hours} hours/month:**
    - **Fargate**: ${total_fargate:.2f}/month (serverless, pay-per-use)
    - **EC2**: ${ec2_cost:.2f}/month (always running)
    
    **Recommendation**: 
    - Use **Fargate** for variable/intermittent workloads  
    - Use **EC2** for consistent, always-on applications
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Fargate with ECS")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Deploy containerized application using Fargate
import boto3
import json

ecs = boto3.client('ecs')

# Create task definition for Fargate
task_definition = {
    "family": "my-fargate-app",
    "networkMode": "awsvpc",
    "requiresCompatibilities": ["FARGATE"],
    "cpu": "256",          # .25 vCPU
    "memory": "512",       # 512 MB
    "executionRoleArn": "arn:aws:iam::123456789:role/ecsTaskExecutionRole",
    "containerDefinitions": [
        {
            "name": "web-app",
            "image": "nginx:latest",
            "cpu": 256,
            "memory": 512,
            "essential": True,
            "portMappings": [
                {
                    "containerPort": 80,
                    "protocol": "tcp"
                }
            ],
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": "/ecs/fargate-app",
                    "awslogs-region": "us-east-1",
                    "awslogs-stream-prefix": "ecs"
                }
            },
            "environment": [
                {"name": "NODE_ENV", "value": "production"},
                {"name": "PORT", "value": "80"}
            ]
        }
    ]
}

# Register task definition
response = ecs.register_task_definition(**task_definition)
print(f"Task definition registered: {response['taskDefinition']['taskDefinitionArn']}")

# Run task on Fargate
run_response = ecs.run_task(
    cluster='my-fargate-cluster',
    taskDefinition='my-fargate-app:1',
    launchType='FARGATE',
    networkConfiguration={
        'awsvpcConfiguration': {
            'subnets': ['subnet-12345', 'subnet-67890'],
            'securityGroups': ['sg-abcdef'],
            'assignPublicIp': 'ENABLED'
        }
    },
    count=1
)

task_arn = run_response['tasks'][0]['taskArn']
print(f"Fargate task started: {task_arn}")

# The task will start automatically without any server management!
print("✅ Your containerized application is now running serverlessly on Fargate!")
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
    # 💻 AWS Compute Services
    ### Master AWS compute fundamentals from virtual machines to serverless containers
    """)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🖥️ Amazon EC2", 
        "📦 AMI", 
        "🐳 Amazon ECS",
        "☸️ Amazon EKS",
        "🚀 AWS Fargate"
    ])
    
    with tab1:
        amazon_ec2_tab()
    
    with tab2:
        amazon_ami_tab()
    
    with tab3:
        amazon_ecs_tab()
    
    with tab4:
        amazon_eks_tab()
        
    with tab5:
        aws_fargate_tab()
    
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


