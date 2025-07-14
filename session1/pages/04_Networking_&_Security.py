
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
    page_title="AWS Networking & Security Learning Hub",
    page_icon="🔐",
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
        
        .danger-box {{
            background: linear-gradient(135deg, {AWS_COLORS['danger']} 0%, {AWS_COLORS['warning']} 100%);
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
        
        .security-card {{
            background: white;
            padding: 20px;
            border-radius: 12px;
            border: 2px solid {AWS_COLORS['light_blue']};
            margin: 15px 0;
        }}
        
        .responsibility-aws {{
            background: linear-gradient(135deg, {AWS_COLORS['primary']} 0%, {AWS_COLORS['light_blue']} 100%);
            padding: 15px;
            border-radius: 10px;
            color: white;
            margin: 10px 0;
        }}
        
        .responsibility-customer {{
            background: linear-gradient(135deg, {AWS_COLORS['success']} 0%, {AWS_COLORS['light_blue']} 100%);
            padding: 15px;
            border-radius: 10px;
            color: white;
            margin: 10px 0;
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
            - 🤝 AWS Shared Responsibility Model - Understanding security boundaries
            - 🏗️ Amazon VPC - Virtual networking in the cloud
            - 🔒 Security Groups & NACLs - Network access control
            - 👤 AWS IAM - Identity and access management
            
            **Learning Objectives:**
            - Understand AWS security fundamentals
            - Learn networking concepts and implementation
            - Master access control and permissions
            - Practice with interactive examples and scenarios
            """)

def create_shared_responsibility_mermaid():
    """Create mermaid diagram for AWS Shared Responsibility Model"""
    return """
    graph TD
        A[AWS Shared Responsibility Model] --> B[AWS Responsibility]
        A --> C[Customer Responsibility]
        
        B --> B1[Physical Infrastructure]
        B --> B2[Network Controls]
        B --> B3[Host Operating System]
        B --> B4[Hypervisor]
        B --> B5[Managed Services]
        
        C --> C1[Guest Operating System]
        C --> C2[Application Software]
        C --> C3[Data Encryption]
        C --> C4[Network Traffic Protection]
        C --> C5[Identity & Access Management]
        C --> C6[Firewall Configuration]
        
        B1 --> B11[Data Centers]
        B1 --> B12[Physical Security]
        B2 --> B21[Network Infrastructure]
        B2 --> B22[DDoS Protection]
        
        C1 --> C11[OS Updates & Patches]
        C1 --> C12[Antivirus]
        C3 --> C31[Data in Transit]
        C3 --> C32[Data at Rest]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_vpc_architecture_mermaid():
    """Create mermaid diagram for VPC architecture"""
    return """
    graph TD
        A[Amazon VPC] --> B[Availability Zone 1]
        A --> C[Availability Zone 2]
        A --> D[Internet Gateway]
        A --> E[NAT Gateway]
        A --> F[VPC Endpoints]
        
        B --> B1[Public Subnet]
        B --> B2[Private Subnet]
        B --> B3[Database Subnet]
        
        C --> C1[Public Subnet]
        C --> C2[Private Subnet]
        C --> C3[Database Subnet]
        
        B1 --> B11[Web Servers]
        B2 --> B22[App Servers]
        B3 --> B33[Database]
        
        C1 --> C11[Web Servers]
        C2 --> C22[App Servers]
        C3 --> C33[Database]
        
        D --> G[Internet]
        E --> D
        B2 --> E
        C2 --> E
        
        F --> H[AWS Services]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#4B9EDB,stroke:#232F3E,color:#fff
        style D fill:#3FB34F,stroke:#232F3E,color:#fff
        style E fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_security_groups_vs_nacls_mermaid():
    """Create mermaid diagram comparing Security Groups vs NACLs"""
    return """
    graph LR
        subgraph "Security Groups"
            SG[Security Group]
            SG --> SG1[Instance Level]
            SG --> SG2[Stateful]
            SG --> SG3[Allow Rules Only]
            SG --> SG4[All Rules Evaluated]
        end
        
        subgraph "Network ACLs"
            NACL[Network ACL]
            NACL --> NACL1[Subnet Level]
            NACL --> NACL2[Stateless]
            NACL --> NACL3[Allow & Deny Rules]
            NACL --> NACL4[Rules by Number Order]
        end
        
        subgraph "Traffic Flow"
            T1[Internet] --> T2[NACL Check]
            T2 --> T3[Security Group Check]
            T3 --> T4[EC2 Instance]
        end
        
        style SG fill:#FF9900,stroke:#232F3E,color:#fff
        style NACL fill:#4B9EDB,stroke:#232F3E,color:#fff
        style T2 fill:#3FB34F,stroke:#232F3E,color:#fff
        style T3 fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_iam_structure_mermaid():
    """Create mermaid diagram for IAM structure"""
    return """
    graph TD
        A[AWS Account Root] --> B[IAM Users]
        A --> C[IAM Groups]
        A --> D[IAM Roles]
        A --> E[IAM Policies]
        
        B --> B1[Individual Users]
        B --> B2[Programmatic Access]
        B --> B3[Console Access]
        
        C --> C1[Admin Group]
        C --> C2[Developer Group]
        C --> C3[Read-Only Group]
        
        D --> D1[EC2 Service Role]
        D --> D2[Lambda Execution Role]
        D --> D3[Cross-Account Role]
        
        E --> E1[AWS Managed Policies]
        E --> E2[Customer Managed Policies]
        E --> E3[Inline Policies]
        
        C1 --> B
        C2 --> B
        C3 --> B
        
        E1 --> C
        E2 --> C
        E3 --> B
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#FF6B35,stroke:#232F3E,color:#fff
        style E fill:#232F3E,stroke:#FF9900,color:#fff
    """

def shared_responsibility_tab():
    """Content for AWS Shared Responsibility Model tab"""
    st.markdown("# 🤝 AWS Shared Responsibility Model")
    st.markdown("*Security and compliance is a shared responsibility between AWS and the customer*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    The **AWS Shared Responsibility Model** defines who is responsible for what in the AWS Cloud.
    AWS secures the cloud infrastructure, while customers secure their data and applications in the cloud.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Shared Responsibility Model Diagram
    st.markdown("## 🏗️ Shared Responsibility Model Overview")
    common.mermaid(create_shared_responsibility_mermaid(), height=400)
    
    # Interactive Responsibility Checker
    st.markdown("## 🔍 Interactive Responsibility Checker")
    
    scenarios = [
        {
            "scenario": "Patching the hypervisor on EC2 instances",
            "responsible": "AWS",
            "explanation": "AWS manages the physical infrastructure and hypervisor layer"
        },
        {
            "scenario": "Updating the guest operating system on EC2",
            "responsible": "Customer", 
            "explanation": "Customers are responsible for managing their guest OS and applications"
        },
        {
            "scenario": "Physical security of data centers",
            "responsible": "AWS",
            "explanation": "AWS handles all physical security aspects of their data centers"
        },
        {
            "scenario": "Encrypting data in S3 buckets",
            "responsible": "Customer",
            "explanation": "Customers choose whether and how to encrypt their data"
        },
        {
            "scenario": "Network traffic protection (SSL/TLS)",
            "responsible": "Customer",
            "explanation": "Customers must implement encryption for data in transit"
        },
        {
            "scenario": "RDS database engine patches",
            "responsible": "AWS",
            "explanation": "For managed services like RDS, AWS handles infrastructure patches"
        }
    ]
    
    selected_scenario = st.selectbox("Choose a security scenario:", 
                                   [s["scenario"] for s in scenarios])
    
    col1, col2 = st.columns(2)
    
    with col1:
        user_choice = st.radio("Who is responsible?", ["AWS", "Customer"])
    
    with col2:
        if st.button("Check Answer", use_container_width=True):
            scenario_data = next(s for s in scenarios if s["scenario"] == selected_scenario)
            
            if user_choice == scenario_data["responsible"]:
                st.markdown('<div class="success-box">', unsafe_allow_html=True)
                st.markdown(f"""
                ### ✅ Correct!
                **{scenario_data["responsible"]}** is responsible for: {selected_scenario}
                
                **Explanation**: {scenario_data["explanation"]}
                """)
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="danger-box">', unsafe_allow_html=True)
                st.markdown(f"""
                ### ❌ Incorrect
                **{scenario_data["responsible"]}** is actually responsible for: {selected_scenario}
                
                **Explanation**: {scenario_data["explanation"]}
                """)
                st.markdown('</div>', unsafe_allow_html=True)
    
    # Responsibility Breakdown by Service Type
    st.markdown("## 📊 Responsibility by Service Type")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="responsibility-aws">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏢 Infrastructure Services (IaaS)
        **Examples**: EC2, VPC, EBS
        
        **AWS Responsibilities:**
        - Physical infrastructure
        - Network controls
        - Host OS and hypervisor
        
        **Customer Responsibilities:**
        - Guest OS updates
        - Applications and data
        - Security groups and firewalls
        - Identity and access management
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="responsibility-customer">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚙️ Platform Services (PaaS)
        **Examples**: RDS, ElastiCache, EMR
        
        **AWS Responsibilities:**
        - Operating system patching
        - Database software installation
        - Automatic backups
        
        **Customer Responsibilities:**
        - Database user accounts
        - Database-level permissions
        - Network access rules
        - Data encryption
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="responsibility-aws">', unsafe_allow_html=True)
        st.markdown("""
        ### ☁️ Software Services (SaaS)
        **Examples**: S3, SES, SQS
        
        **AWS Responsibilities:**
        - Service operation
        - Infrastructure management
        - Service availability
        
        **Customer Responsibilities:**
        - Data classification
        - Identity and access management
        - Usage in compliance with policies
        - Client-side encryption
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Security Best Practices
    st.markdown("## 🛡️ Security Best Practices")
    
    practices_data = {
        'Category': ['Identity', 'Data', 'Network', 'Logging', 'Infrastructure'],
        'Best Practice': [
            'Use IAM roles instead of root account',
            'Encrypt data at rest and in transit',
            'Use Security Groups as firewalls',
            'Enable CloudTrail for audit logs',
            'Keep systems patched and updated'
        ],
        'Priority': ['Critical', 'Critical', 'High', 'High', 'Medium'],
        'Effort': ['Low', 'Medium', 'Low', 'Low', 'High']
    }
    
    df_practices = pd.DataFrame(practices_data)
    
    fig = px.scatter(df_practices, x='Effort', y='Priority', 
                     size=[5, 5, 4, 4, 3], color='Category',
                     title='Security Best Practices: Priority vs Implementation Effort',
                     hover_data=['Best Practice'])
    
    fig.update_layout(
        xaxis_title='Implementation Effort',
        yaxis_title='Priority Level',
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Security Configuration")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Security configuration following shared responsibility model
import boto3
import json

# Example: Customer responsibilities for EC2 security
ec2 = boto3.client('ec2')
iam = boto3.client('iam')

# 1. Create security group with least privilege access
def create_secure_security_group():
    response = ec2.create_security_group(
        GroupName='secure-web-sg',
        Description='Secure web server security group',
        VpcId='vpc-12345678'
    )
    
    sg_id = response['GroupId']
    
    # Only allow HTTPS traffic (port 443)
    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': 443,
                'ToPort': 443,
                'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'HTTPS access'}]
            },
            {
                'IpProtocol': 'tcp',
                'FromPort': 22,
                'ToPort': 22,
                'IpRanges': [{'CidrIp': '10.0.0.0/8', 'Description': 'SSH from internal'}]
            }
        ]
    )
    
    return sg_id

# 2. Create IAM role with minimal permissions (customer responsibility)
def create_minimal_role():
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    # Create role
    iam.create_role(
        RoleName='minimal-ec2-role',
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description='Minimal permissions for EC2 instance'
    )
    
    # Attach minimal policy (only CloudWatch logs)
    minimal_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            }
        ]
    }
    
    iam.put_role_policy(
        RoleName='minimal-ec2-role',
        PolicyName='CloudWatchLogsPolicy',
        PolicyDocument=json.dumps(minimal_policy)
    )

# 3. Launch EC2 with security best practices
def launch_secure_instance(sg_id):
    user_data_script = '''#!/bin/bash
    # Customer responsibility: Secure the OS
    yum update -y
    yum install -y amazon-cloudwatch-agent
    
    # Enable automatic security updates
    echo "0 2 * * * root /usr/bin/yum update -y --security" >> /etc/crontab
    
    # Configure CloudWatch agent for monitoring
    /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \\
        -a fetch-config -m ec2 -c ssm:CloudWatch-Config -s
    '''
    
    response = ec2.run_instances(
        ImageId='ami-0abcdef1234567890',  # Use latest AMI
        MinCount=1,
        MaxCount=1,
        InstanceType='t3.micro',
        SecurityGroupIds=[sg_id],
        IamInstanceProfile={'Name': 'minimal-ec2-role'},
        UserData=user_data_script,
        Monitoring={'Enabled': True},  # Enable detailed monitoring
        EbsOptimized=True,
        BlockDeviceMappings=[
            {
                'DeviceName': '/dev/xvda',
                'Ebs': {
                    'VolumeSize': 8,
                    'VolumeType': 'gp3',
                    'Encrypted': True  # Customer responsibility: encrypt data
                }
            }
        ]
    )
    
    return response['Instances'][0]['InstanceId']

# Execute security setup
print("Setting up secure infrastructure...")
sg_id = create_secure_security_group()
create_minimal_role()
instance_id = launch_secure_instance(sg_id)

print(f"✅ Secure instance launched: {instance_id}")
print("🔒 Security measures implemented:")
print("  - Encrypted EBS volume")
print("  - Minimal IAM permissions")
print("  - Restricted security group")
print("  - Automatic OS updates")
print("  - CloudWatch monitoring enabled")
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def amazon_vpc_tab():
    """Content for Amazon VPC tab"""
    st.markdown("# 🏗️ Amazon Virtual Private Cloud (VPC)")
    st.markdown("*Provision a logically isolated section of AWS Cloud*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Amazon VPC** lets you provision a logically isolated section of AWS Cloud where you can launch
    AWS resources in a virtual network that you define. Think of it as your own private data center in the cloud.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # VPC Architecture
    st.markdown("## 🏗️ VPC Architecture Overview")
    common.mermaid(create_vpc_architecture_mermaid(), height=400)
    
    # Interactive VPC Builder
    st.markdown("## 🔧 Interactive VPC Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Basic Configuration")
        vpc_name = st.text_input("VPC Name:", "my-custom-vpc")
        cidr_block = st.selectbox("VPC CIDR Block:", [
            "10.0.0.0/16 (65,536 IPs)",
            "172.16.0.0/16 (65,536 IPs)", 
            "192.168.0.0/16 (65,536 IPs)",
            "10.0.0.0/24 (256 IPs)"
        ])
        
        num_azs = st.slider("Number of Availability Zones:", 1, 4, 2)
        
    with col2:
        st.markdown("### Advanced Options")
        enable_dns = st.checkbox("Enable DNS hostname resolution", value=True)
        enable_nat = st.checkbox("Enable NAT Gateway for private subnets", value=True)
        
        vpc_endpoints = st.multiselect("VPC Endpoints:", [
            "S3", "DynamoDB", "EC2", "Lambda", "SQS", "SNS"
        ])
    
    # Subnet Configuration
    st.markdown("### Subnet Configuration")
    subnet_config = []
    
    for i in range(num_azs):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            public_cidr = st.text_input(f"Public Subnet AZ-{i+1}:", f"10.0.{i*4+1}.0/24")
        with col2:
            private_cidr = st.text_input(f"Private Subnet AZ-{i+1}:", f"10.0.{i*4+2}.0/24")
        with col3:
            db_cidr = st.text_input(f"DB Subnet AZ-{i+1}:", f"10.0.{i*4+3}.0/24")
        
        subnet_config.append({
            'az': i+1,
            'public': public_cidr,
            'private': private_cidr,
            'database': db_cidr
        })
    
    if st.button("🚀 Create VPC (Simulation)", use_container_width=True):
        
        # Calculate total IPs
        base_cidr = cidr_block.split()[0]
        total_ips = int(cidr_block.split('(')[1].split()[0].replace(',', ''))
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ VPC Created Successfully!
        
        **VPC Details:**
        - **Name**: {vpc_name}
        - **CIDR Block**: {base_cidr}
        - **Total IP Addresses**: {total_ips:,}
        - **Availability Zones**: {num_azs}
        - **DNS Resolution**: {'Enabled' if enable_dns else 'Disabled'}
        - **NAT Gateway**: {'Enabled' if enable_nat else 'Disabled'}
        - **VPC ID**: vpc-{np.random.randint(100000000, 999999999):x}
        
        **Subnets Created**: {num_azs * 3} subnets (Public, Private, Database per AZ)
        **VPC Endpoints**: {', '.join(vpc_endpoints) if vpc_endpoints else 'None'}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # VPC Components Deep Dive
    st.markdown("## 🧩 VPC Components")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🌐 Internet Gateway
        - **Enables internet access** for public subnets
        - Horizontally scaled and redundant
        - No bandwidth constraints
        - **Use Case**: Web servers, public-facing apps
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚪 NAT Gateway
        - **Outbound internet access** for private subnets
        - Managed by AWS (high availability)
        - Bandwidth up to 45 Gbps
        - **Use Case**: Private servers need internet for updates
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔗 VPC Endpoints
        - **Private connection** to AWS services
        - No internet gateway required
        - Gateway endpoints (S3, DynamoDB)
        - Interface endpoints (other services)
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛣️ Route Tables
        - **Controls network traffic** routing
        - Associated with subnets
        - Default route: 0.0.0.0/0
        - **Example**: Route to Internet Gateway
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🌉 VPC Peering
        - **Connect VPCs** privately
        - Cross-region and cross-account
        - Non-overlapping CIDR blocks
        - **Use Case**: Multi-VPC architectures
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏢 VPN Gateway
        - **Secure connection** to on-premises
        - IPSec VPN tunnels
        - AWS Direct Connect integration
        - **Use Case**: Hybrid cloud architectures
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # CIDR Block Calculator
    st.markdown("## 🧮 CIDR Block Calculator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        cidr_input = st.text_input("Enter CIDR Block:", "10.0.0.0/24")
        
        if st.button("Calculate CIDR Details"):
            try:
                network, prefix = cidr_input.split('/')
                prefix_len = int(prefix)
                host_bits = 32 - prefix_len
                num_addresses = 2 ** host_bits
                usable_addresses = num_addresses - 2  # Subtract network and broadcast
                
                st.markdown('<div class="success-box">', unsafe_allow_html=True)
                st.markdown(f"""
                ### 📊 CIDR Analysis: {cidr_input}
                
                - **Network Address**: {network}
                - **Prefix Length**: /{prefix_len}
                - **Host Bits**: {host_bits}
                - **Total Addresses**: {num_addresses:,}
                - **Usable Host Addresses**: {usable_addresses:,}
                - **Subnet Mask**: {'.'.join([str((0xffffffff << (32 - prefix_len) >> i) & 0xff) for i in [24, 16, 8, 0]])}
                """)
                st.markdown('</div>', unsafe_allow_html=True)
            except:
                st.error("Invalid CIDR format. Use format like 10.0.0.0/24")
    
    with col2:
        st.markdown("### Common CIDR Blocks")
        
        cidr_examples = pd.DataFrame({
            'CIDR': ['/8', '/16', '/20', '/24', '/28'],
            'Addresses': ['16.7M', '65,536', '4,096', '256', '16'],
            'Use Case': ['Very Large', 'Large VPC', 'Medium VPC', 'Small Subnet', 'Micro Subnet']
        })
        
        st.dataframe(cidr_examples, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: VPC Creation")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Create VPC with public and private subnets
import boto3

ec2 = boto3.client('ec2')
ec2_resource = boto3.resource('ec2')

def create_vpc_with_subnets():
    # Create VPC
    vpc_response = ec2.create_vpc(
        CidrBlock='10.0.0.0/16',
        EnableDnsHostnames=True,
        EnableDnsSupport=True,
        TagSpecifications=[
            {
                'ResourceType': 'vpc',
                'Tags': [
                    {'Key': 'Name', 'Value': 'my-custom-vpc'},
                    {'Key': 'Environment', 'Value': 'production'}
                ]
            }
        ]
    )
    
    vpc_id = vpc_response['Vpc']['VpcId']
    print(f"Created VPC: {vpc_id}")
    
    # Get availability zones
    azs = ec2.describe_availability_zones()['AvailabilityZones']
    
    # Create Internet Gateway
    igw_response = ec2.create_internet_gateway(
        TagSpecifications=[
            {
                'ResourceType': 'internet-gateway',
                'Tags': [{'Key': 'Name', 'Value': 'my-vpc-igw'}]
            }
        ]
    )
    igw_id = igw_response['InternetGateway']['InternetGatewayId']
    
    # Attach Internet Gateway to VPC
    ec2.attach_internet_gateway(
        InternetGatewayId=igw_id,
        VpcId=vpc_id
    )
    
    # Create subnets in multiple AZs
    subnets = []
    for i, az in enumerate(azs[:2]):  # Use first 2 AZs
        # Public subnet
        public_subnet = ec2.create_subnet(
            VpcId=vpc_id,
            CidrBlock=f'10.0.{i*4+1}.0/24',
            AvailabilityZone=az['ZoneName'],
            TagSpecifications=[
                {
                    'ResourceType': 'subnet',
                    'Tags': [
                        {'Key': 'Name', 'Value': f'public-subnet-{i+1}'},
                        {'Key': 'Type', 'Value': 'Public'}
                    ]
                }
            ]
        )
        
        # Private subnet
        private_subnet = ec2.create_subnet(
            VpcId=vpc_id,
            CidrBlock=f'10.0.{i*4+2}.0/24',
            AvailabilityZone=az['ZoneName'],
            TagSpecifications=[
                {
                    'ResourceType': 'subnet',
                    'Tags': [
                        {'Key': 'Name', 'Value': f'private-subnet-{i+1}'},
                        {'Key': 'Type', 'Value': 'Private'}
                    ]
                }
            ]
        )
        
        # Database subnet
        db_subnet = ec2.create_subnet(
            VpcId=vpc_id,
            CidrBlock=f'10.0.{i*4+3}.0/24',
            AvailabilityZone=az['ZoneName'],
            TagSpecifications=[
                {
                    'ResourceType': 'subnet',
                    'Tags': [
                        {'Key': 'Name', 'Value': f'database-subnet-{i+1}'},
                        {'Key': 'Type', 'Value': 'Database'}
                    ]
                }
            ]
        )
        
        subnets.extend([
            {
                'type': 'public',
                'id': public_subnet['Subnet']['SubnetId'],
                'az': az['ZoneName']
            },
            {
                'type': 'private', 
                'id': private_subnet['Subnet']['SubnetId'],
                'az': az['ZoneName']
            },
            {
                'type': 'database',
                'id': db_subnet['Subnet']['SubnetId'],
                'az': az['ZoneName']
            }
        ])
    
    # Create NAT Gateway for private subnets
    public_subnet_id = [s['id'] for s in subnets if s['type'] == 'public'][0]
    
    # Allocate Elastic IP for NAT Gateway
    eip_response = ec2.allocate_address(Domain='vpc')
    allocation_id = eip_response['AllocationId']
    
    # Create NAT Gateway
    nat_response = ec2.create_nat_gateway(
        SubnetId=public_subnet_id,
        AllocationId=allocation_id,
        TagSpecifications=[
            {
                'ResourceType': 'nat-gateway',
                'Tags': [{'Key': 'Name', 'Value': 'my-vpc-nat'}]
            }
        ]
    )
    nat_id = nat_response['NatGateway']['NatGatewayId']
    
    # Create route tables
    # Public route table
    public_rt = ec2.create_route_table(
        VpcId=vpc_id,
        TagSpecifications=[
            {
                'ResourceType': 'route-table',
                'Tags': [{'Key': 'Name', 'Value': 'public-route-table'}]
            }
        ]
    )
    public_rt_id = public_rt['RouteTable']['RouteTableId']
    
    # Add route to Internet Gateway
    ec2.create_route(
        RouteTableId=public_rt_id,
        DestinationCidrBlock='0.0.0.0/0',
        GatewayId=igw_id
    )
    
    # Private route table
    private_rt = ec2.create_route_table(
        VpcId=vpc_id,
        TagSpecifications=[
            {
                'ResourceType': 'route-table',
                'Tags': [{'Key': 'Name', 'Value': 'private-route-table'}]
            }
        ]
    )
    private_rt_id = private_rt['RouteTable']['RouteTableId']
    
    # Add route to NAT Gateway (wait for NAT Gateway to be available)
    waiter = ec2.get_waiter('nat_gateway_available')
    waiter.wait(NatGatewayIds=[nat_id])
    
    ec2.create_route(
        RouteTableId=private_rt_id,
        DestinationCidrBlock='0.0.0.0/0',
        NatGatewayId=nat_id
    )
    
    # Associate subnets with route tables
    for subnet in subnets:
        if subnet['type'] == 'public':
            ec2.associate_route_table(
                SubnetId=subnet['id'],
                RouteTableId=public_rt_id
            )
        else:
            ec2.associate_route_table(
                SubnetId=subnet['id'],
                RouteTableId=private_rt_id
            )
    
    print("✅ VPC infrastructure created successfully!")
    print(f"VPC ID: {vpc_id}")
    print(f"Internet Gateway: {igw_id}")
    print(f"NAT Gateway: {nat_id}")
    print(f"Subnets created: {len(subnets)}")
    
    return {
        'vpc_id': vpc_id,
        'igw_id': igw_id,
        'nat_id': nat_id,
        'subnets': subnets
    }

# Create the VPC infrastructure
vpc_info = create_vpc_with_subnets()
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def security_groups_nacls_tab():
    """Content for Security Groups & NACLs tab"""
    st.markdown("# 🔒 Security Groups & Network ACLs")
    st.markdown("*Your virtual firewalls for controlling network traffic*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    AWS provides two layers of network security: **Security Groups** (instance-level firewalls) and 
    **Network ACLs** (subnet-level firewalls). They work together to provide defense in depth.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Security Groups vs NACLs Comparison
    st.markdown("## ⚔️ Security Groups vs Network ACLs")
    common.mermaid(create_security_groups_vs_nacls_mermaid(), height=300)
    
    # Interactive Security Group Builder
    st.markdown("## 🛡️ Interactive Security Group Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Basic Configuration")
        sg_name = st.text_input("Security Group Name:", "web-server-sg")
        sg_description = st.text_area("Description:", "Security group for web servers")
        
        st.markdown("### Inbound Rules")
        inbound_rules = []
        
        # Common rule templates
        rule_templates = {
            "HTTP (80)": {"port": 80, "protocol": "tcp", "source": "0.0.0.0/0"},
            "HTTPS (443)": {"port": 443, "protocol": "tcp", "source": "0.0.0.0/0"},
            "SSH (22)": {"port": 22, "protocol": "tcp", "source": "10.0.0.0/8"},
            "RDP (3389)": {"port": 3389, "protocol": "tcp", "source": "10.0.0.0/16"},
            "MySQL (3306)": {"port": 3306, "protocol": "tcp", "source": "sg-app-servers"},
            "Custom": {"port": 8080, "protocol": "tcp", "source": "0.0.0.0/0"}
        }
        
        selected_rules = st.multiselect(
            "Select inbound rules:",
            list(rule_templates.keys()),
            default=["HTTPS (443)", "SSH (22)"]
        )
    
    with col2:
        st.markdown("### Rule Details")
        
        for rule in selected_rules:
            with st.expander(f"Configure {rule}", expanded=False):
                template = rule_templates[rule]
                
                col_a, col_b = st.columns(2)
                with col_a:
                    port = st.number_input(f"Port ({rule}):", value=template["port"], key=f"port_{rule}")
                    protocol = st.selectbox(f"Protocol ({rule}):", ["tcp", "udp", "icmp"], 
                                          index=["tcp", "udp", "icmp"].index(template["protocol"]), 
                                          key=f"proto_{rule}")
                
                with col_b:
                    source = st.text_input(f"Source ({rule}):", value=template["source"], key=f"source_{rule}")
                
                inbound_rules.append({
                    "name": rule,
                    "port": port,
                    "protocol": protocol,
                    "source": source
                })
    
    if st.button("🚀 Create Security Group (Simulation)", use_container_width=True):
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Security Group Created Successfully!
        
        **Security Group Details:**
        - **Name**: {sg_name}
        - **Description**: {sg_description}
        - **Security Group ID**: sg-{np.random.randint(100000000, 999999999):x}
        - **VPC**: vpc-default
        
        **Inbound Rules** ({len(inbound_rules)} rules):
        """)
        
        for rule in inbound_rules:
            st.markdown(f"- **{rule['name']}**: {rule['protocol'].upper()}/{rule['port']} from {rule['source']}")
        
        st.markdown("""
        **Outbound Rules**: All traffic allowed (0.0.0.0/0)
        
        🔒 **Security Group is now ready to protect your EC2 instances!**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Detailed Comparison Table
    st.markdown("## 📊 Detailed Feature Comparison")
    
    comparison_data = {
        'Feature': [
            'Level of Operation', 'State', 'Rules Type', 'Rule Processing',
            'Default Behavior', 'Instance Association', 'Rule Changes'
        ],
        'Security Groups': [
            'Instance Level', 'Stateful', 'Allow Only', 'All Rules Evaluated',
            'Deny All Inbound', 'Multiple SGs per Instance', 'Applied Immediately'
        ],
        'Network ACLs': [
            'Subnet Level', 'Stateless', 'Allow & Deny', 'First Match Wins',
            'Allow All by Default', 'One NACL per Subnet', 'Applied Immediately'
        ]
    }
    
    df_comparison = pd.DataFrame(comparison_data)
    st.table(df_comparison)
    
    # Traffic Flow Simulation
    st.markdown("## 🚦 Traffic Flow Simulation")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Simulate Network Request")
        source_ip = st.text_input("Source IP:", "203.0.113.10")
        dest_port = st.selectbox("Destination Port:", [80, 443, 22, 3306, 8080])
        protocol = st.selectbox("Protocol:", ["TCP", "UDP"])
    
    with col2:
        st.markdown("### Security Configuration")
        nacl_allow = st.checkbox("NACL allows traffic", value=True)
        sg_rule_exists = st.checkbox("Security Group has matching allow rule", value=True)
    
    if st.button("🔍 Test Traffic Flow"):
        
        # Simulate traffic evaluation
        if not nacl_allow:
            st.markdown('<div class="danger-box">', unsafe_allow_html=True)
            st.markdown(f"""
            ### ❌ Traffic BLOCKED at Network ACL
            
            **Request Details:**
            - Source: {source_ip}
            - Destination Port: {dest_port}
            - Protocol: {protocol}
            
            **Reason**: Network ACL denies this traffic at the subnet level.
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        elif not sg_rule_exists:
            st.markdown('<div class="warning-box">', unsafe_allow_html=True)
            st.markdown(f"""
            ### ⚠️ Traffic BLOCKED at Security Group
            
            **Request Details:**
            - Source: {source_ip}  
            - Destination Port: {dest_port}
            - Protocol: {protocol}
            
            **Reason**: No matching allow rule in Security Group (default deny).
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="success-box">', unsafe_allow_html=True)
            st.markdown(f"""
            ### ✅ Traffic ALLOWED
            
            **Request Details:**  
            - Source: {source_ip}
            - Destination Port: {dest_port}
            - Protocol: {protocol}
            
            **Flow**: Internet → NACL (✓) → Security Group (✓) → EC2 Instance
            
            Return traffic automatically allowed (stateful Security Group).
            """)
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Best Practices
    st.markdown("## 💡 Security Best Practices")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="security-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛡️ Security Group Best Practices
        
        1. **Least Privilege**: Only allow necessary ports and sources
        2. **Reference Other Security Groups**: Use SG IDs instead of IP ranges
        3. **Descriptive Names**: Use clear, consistent naming conventions
        4. **Regular Audits**: Review rules quarterly
        5. **Source Restrictions**: Avoid 0.0.0.0/0 for SSH/RDP
        6. **Use Multiple Groups**: Separate concerns (web, app, db)
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="security-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔐 Network ACL Best Practices
        
        1. **Default Allow**: Start with default NACL for simplicity
        2. **Rule Numbering**: Use gaps (100, 200, 300) for future rules
        3. **Ephemeral Ports**: Allow return traffic (1024-65535)
        4. **Explicit Deny**: Use specific deny rules when needed
        5. **Testing**: Test connectivity after NACL changes
        6. **Documentation**: Document custom NACL configurations
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Common Security Group Patterns
    st.markdown("## 🏗️ Common Security Group Patterns")
    
    patterns_data = {
        'Pattern': ['Web Tier', 'Application Tier', 'Database Tier', 'Bastion Host', 'Load Balancer'],
        'Inbound Rules': [
            'HTTP (80), HTTPS (443) from 0.0.0.0/0',
            'App Port (8080) from web-sg',
            'MySQL/PostgreSQL from app-sg',
            'SSH (22) from admin IPs',
            'HTTP/HTTPS from 0.0.0.0/0'
        ],
        'Outbound Rules': [
            'All to app-sg',
            'All to db-sg, HTTPS to 0.0.0.0/0',
            'None (database should not initiate)',
            'SSH to private subnets',
            'All to web-sg'
        ]
    }
    
    df_patterns = pd.DataFrame(patterns_data)
    st.dataframe(df_patterns, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Security Groups & NACLs")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Create layered security with Security Groups and NACLs
import boto3

ec2 = boto3.client('ec2')

def create_layered_security(vpc_id, subnet_id):
    # 1. Create Web Server Security Group
    web_sg = ec2.create_security_group(
        GroupName='web-servers-sg',
        Description='Security group for web servers',
        VpcId=vpc_id,
        TagSpecifications=[
            {
                'ResourceType': 'security-group',
                'Tags': [
                    {'Key': 'Name', 'Value': 'WebServers-SG'},
                    {'Key': 'Tier', 'Value': 'Web'}
                ]
            }
        ]
    )
    web_sg_id = web_sg['GroupId']
    
    # Add inbound rules to web security group
    ec2.authorize_security_group_ingress(
        GroupId=web_sg_id,
        IpPermissions=[
            # HTTPS from internet
            {
                'IpProtocol': 'tcp',
                'FromPort': 443,
                'ToPort': 443,
                'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'HTTPS from internet'}]
            },
            # HTTP from internet (redirect to HTTPS)
            {
                'IpProtocol': 'tcp',
                'FromPort': 80,
                'ToPort': 80,
                'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'HTTP from internet'}]
            },
            # SSH from bastion host
            {
                'IpProtocol': 'tcp',
                'FromPort': 22,
                'ToPort': 22,
                'IpRanges': [{'CidrIp': '10.0.1.0/24', 'Description': 'SSH from bastion subnet'}]
            }
        ]
    )
    
    # 2. Create Application Security Group
    app_sg = ec2.create_security_group(
        GroupName='app-servers-sg',
        Description='Security group for application servers',
        VpcId=vpc_id,
        TagSpecifications=[
            {
                'ResourceType': 'security-group',
                'Tags': [
                    {'Key': 'Name', 'Value': 'AppServers-SG'},
                    {'Key': 'Tier', 'Value': 'Application'}
                ]
            }
        ]
    )
    app_sg_id = app_sg['GroupId']
    
    # Add inbound rules to application security group
    ec2.authorize_security_group_ingress(
        GroupId=app_sg_id,
        IpPermissions=[
            # Application port from web servers only
            {
                'IpProtocol': 'tcp',
                'FromPort': 8080,
                'ToPort': 8080,
                'UserIdGroupPairs': [
                    {
                        'GroupId': web_sg_id,
                        'Description': 'App traffic from web servers'
                    }
                ]
            },
            # SSH from bastion
            {
                'IpProtocol': 'tcp',
                'FromPort': 22,
                'ToPort': 22,
                'IpRanges': [{'CidrIp': '10.0.1.0/24', 'Description': 'SSH from bastion'}]
            }
        ]
    )
    
    # 3. Create Database Security Group
    db_sg = ec2.create_security_group(
        GroupName='database-sg',
        Description='Security group for database servers',
        VpcId=vpc_id,
        TagSpecifications=[
            {
                'ResourceType': 'security-group',
                'Tags': [
                    {'Key': 'Name', 'Value': 'Database-SG'},
                    {'Key': 'Tier', 'Value': 'Database'}
                ]
            }
        ]
    )
    db_sg_id = db_sg['GroupId']
    
    # Add inbound rules to database security group
    ec2.authorize_security_group_ingress(
        GroupId=db_sg_id,
        IpPermissions=[
            # MySQL from application servers only
            {
                'IpProtocol': 'tcp',
                'FromPort': 3306,
                'ToPort': 3306,
                'UserIdGroupPairs': [
                    {
                        'GroupId': app_sg_id,
                        'Description': 'MySQL from app servers'
                    }
                ]
            }
        ]
    )
    
    # Remove default outbound rule from database security group
    # (Databases shouldn't initiate outbound connections)
    ec2.revoke_security_group_egress(
        GroupId=db_sg_id,
        IpPermissions=[
            {
                'IpProtocol': '-1',
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            }
        ]
    )
    
    # 4. Create Custom Network ACL for additional security
    nacl_response = ec2.create_network_acl(
        VpcId=vpc_id,
        TagSpecifications=[
            {
                'ResourceType': 'network-acl',
                'Tags': [
                    {'Key': 'Name', 'Value': 'WebTier-NACL'},
                    {'Key': 'Purpose', 'Value': 'Additional web tier protection'}
                ]
            }
        ]
    )
    nacl_id = nacl_response['NetworkAcl']['NetworkAclId']
    
    # Add NACL entries (more restrictive than security groups)
    nacl_entries = [
        # Allow HTTPS inbound
        {
            'RuleNumber': 100,
            'Protocol': '6',  # TCP
            'RuleAction': 'allow',
            'CidrBlock': '0.0.0.0/0',
            'PortRange': {'From': 443, 'To': 443}
        },
        # Allow HTTP inbound (for redirects)
        {
            'RuleNumber': 110,
            'Protocol': '6',  # TCP
            'RuleAction': 'allow',
            'CidrBlock': '0.0.0.0/0',
            'PortRange': {'From': 80, 'To': 80}
        },
        # Allow ephemeral ports for return traffic
        {
            'RuleNumber': 120,
            'Protocol': '6',  # TCP
            'RuleAction': 'allow',
            'CidrBlock': '0.0.0.0/0',
            'PortRange': {'From': 1024, 'To': 65535}
        },
        # Block known bad IP (example)
        {
            'RuleNumber': 50,
            'Protocol': '-1',  # All
            'RuleAction': 'deny',
            'CidrBlock': '198.51.100.0/24'
        }
    ]
    
    # Create inbound NACL entries
    for entry in nacl_entries:
        ec2.create_network_acl_entry(
            NetworkAclId=nacl_id,
            RuleNumber=entry['RuleNumber'],
            Protocol=entry['Protocol'],
            RuleAction=entry['RuleAction'],
            CidrBlock=entry['CidrBlock'],
            PortRange=entry.get('PortRange')
        )
    
    # Create corresponding outbound entries (stateless)
    outbound_entries = [
        # Allow HTTPS outbound
        {
            'RuleNumber': 100,
            'Protocol': '6',
            'RuleAction': 'allow',
            'CidrBlock': '0.0.0.0/0',
            'PortRange': {'From': 443, 'To': 443}
        },
        # Allow HTTP outbound
        {
            'RuleNumber': 110,
            'Protocol': '6',
            'RuleAction': 'allow',
            'CidrBlock': '0.0.0.0/0',
            'PortRange': {'From': 80, 'To': 80}
        },
        # Allow ephemeral ports outbound
        {
            'RuleNumber': 120,
            'Protocol': '6',
            'RuleAction': 'allow',
            'CidrBlock': '0.0.0.0/0',
            'PortRange': {'From': 1024, 'To': 65535}
        }
    ]
    
    for entry in outbound_entries:
        ec2.create_network_acl_entry(
            NetworkAclId=nacl_id,
            RuleNumber=entry['RuleNumber'],
            Protocol=entry['Protocol'],
            RuleAction=entry['RuleAction'],
            CidrBlock=entry['CidrBlock'],
            PortRange=entry.get('PortRange'),
            Egress=True
        )
    
    # Associate NACL with subnet
    ec2.replace_network_acl_association(
        AssociationId=nacl_id,
        NetworkAclId=nacl_id
    )
    
    print("✅ Layered security implemented successfully!")
    print(f"Web Security Group: {web_sg_id}")
    print(f"App Security Group: {app_sg_id}")
    print(f"Database Security Group: {db_sg_id}")
    print(f"Custom Network ACL: {nacl_id}")
    
    return {
        'web_sg_id': web_sg_id,
        'app_sg_id': app_sg_id,
        'db_sg_id': db_sg_id,
        'nacl_id': nacl_id
    }

# Example usage
# security_config = create_layered_security('vpc-12345678', 'subnet-12345678')
    """, language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def aws_iam_tab():
    """Content for AWS IAM tab"""
    st.markdown("# 👤 AWS Identity and Access Management")
    st.markdown("*Securely manage access to AWS services and resources*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **AWS IAM** enables you to manage access to AWS services and resources securely. It's like being the 
    security guard for your AWS account - controlling who can do what, when, and where.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # IAM Structure
    st.markdown("## 🏗️ IAM Structure Overview")
    common.mermaid(create_iam_structure_mermaid(), height=400)
    
    # Interactive IAM Policy Builder
    st.markdown("## 🔧 Interactive IAM Policy Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Policy Configuration")
        policy_name = st.text_input("Policy Name:", "MyCustomPolicy")
        policy_description = st.text_area("Description:", "Custom policy for specific access requirements")
        
        # Policy effect
        effect = st.selectbox("Effect:", ["Allow", "Deny"])
        
        # AWS Services
        services = st.multiselect("AWS Services:", [
            "s3", "ec2", "iam", "rds", "lambda", "cloudwatch", "dynamodb", "sns", "sqs"
        ], default=["s3", "ec2"])
    
    with col2:
        st.markdown("### Actions & Resources")
        
        # Actions based on selected services
        all_actions = []
        for service in services:
            if service == "s3":
                all_actions.extend(["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"])
            elif service == "ec2":
                all_actions.extend(["ec2:DescribeInstances", "ec2:StartInstances", "ec2:StopInstances"])
            elif service == "iam":
                all_actions.extend(["iam:ListUsers", "iam:CreateUser", "iam:AttachUserPolicy"])
        
        selected_actions = st.multiselect("Actions:", all_actions)
        
        # Resources
        resource_arn = st.text_input("Resource ARN:", "arn:aws:s3:::my-bucket/*")
        
        # Conditions
        add_conditions = st.checkbox("Add Conditions")
        if add_conditions:
            condition_key = st.selectbox("Condition Key:", [
                "aws:RequestedRegion", "aws:username", "aws:CurrentTime", "IpAddress"
            ])
            condition_value = st.text_input("Condition Value:", "us-east-1")
    
    if st.button("🚀 Generate IAM Policy (Simulation)", use_container_width=True):
        
        # Generate policy JSON
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": effect,
                    "Action": selected_actions if selected_actions else ["s3:GetObject"],
                    "Resource": resource_arn if resource_arn else "*"
                }
            ]
        }
        
        # Add conditions if specified
        if add_conditions and condition_key and condition_value:
            if condition_key == "IpAddress":
                policy_document["Statement"][0]["Condition"] = {
                    "IpAddress": {"aws:SourceIp": condition_value}
                }
            else:
                policy_document["Statement"][0]["Condition"] = {
                    "StringEquals": {condition_key: condition_value}
                }
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ IAM Policy Generated Successfully!
        
        **Policy Details:**
        - **Name**: {policy_name}
        - **Effect**: {effect}
        - **Actions**: {len(selected_actions) if selected_actions else 1}
        - **Services**: {', '.join(services)}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown("### Generated Policy JSON:")
        st.code(str(policy_document).replace("'", '"'), language='json')
    
    # IAM Best Practices
    st.markdown("## 🛡️ IAM Security Best Practices")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="security-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔐 Access Control
        
        1. **Principle of Least Privilege**
           - Grant minimum permissions needed
           - Review permissions regularly
        
        2. **Use IAM Roles**
           - For AWS services and applications
           - Avoid long-term access keys
        
        3. **Multi-Factor Authentication**
           - Enable MFA for all users
           - Especially for privileged accounts
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="security-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 👥 User Management
        
        1. **Individual IAM Users**
           - One user per person
           - No shared accounts
        
        2. **IAM Groups**
           - Organize users by job function
           - Attach policies to groups
        
        3. **Strong Password Policy**
           - Enforce complexity requirements
           - Regular password rotation
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="security-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Monitoring & Compliance
        
        1. **Access Logging**
           - Enable CloudTrail
           - Monitor API calls
        
        2. **Access Analysis**
           - Use IAM Access Analyzer
           - Review unused permissions
        
        3. **Credential Rotation**
           - Rotate access keys regularly
           - Use AWS Systems Manager for secrets
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Common IAM Patterns
    st.markdown("## 🏗️ Common IAM Patterns")
    
    # Interactive pattern explorer
    pattern_choice = st.selectbox("Explore IAM Pattern:", [
        "Developer Access", "Read-Only Analyst", "Database Administrator", 
        "Security Auditor", "Cross-Account Access"
    ])
    
    patterns = {
        "Developer Access": {
            "description": "Developers need access to development resources but not production",
            "policies": ["AmazonEC2FullAccess", "AmazonS3FullAccess", "CloudWatchReadOnlyAccess"],
            "conditions": "Only in dev/test regions and during business hours",
            "example_policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["ec2:*", "s3:*"],
                        "Resource": "*",
                        "Condition": {
                            "StringEquals": {"aws:RequestedRegion": ["us-east-1", "us-west-2"]},
                            "DateGreaterThan": {"aws:CurrentTime": "08:00Z"},
                            "DateLessThan": {"aws:CurrentTime": "18:00Z"}
                        }
                    }
                ]
            }
        },
        "Read-Only Analyst": {
            "description": "Analysts need read access to gather insights and create reports",
            "policies": ["ReadOnlyAccess", "CloudWatchReadOnlyAccess"],
            "conditions": "Read-only access to all services",
            "example_policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["*:Describe*", "*:List*", "*:Get*"],
                        "Resource": "*"
                    }
                ]
            }
        }
    }
    
    if pattern_choice in patterns:
        pattern = patterns[pattern_choice]
        
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown(f"""
        ### {pattern_choice} Pattern
        
        **Description**: {pattern['description']}
        
        **Suggested Policies**: {', '.join(pattern['policies'])}
        
        **Conditions**: {pattern['conditions']}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown("### Example Policy:")
        st.code(str(pattern['example_policy']).replace("'", '"'), language='json')
    
    # IAM Policy Simulator
    st.markdown("## 🧪 IAM Policy Simulator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Test Scenario")
        test_user = st.text_input("IAM User/Role:", "developer-user")
        test_action = st.selectbox("Action to Test:", [
            "s3:GetObject", "ec2:StartInstances", "iam:CreateUser", 
            "rds:CreateDBInstance", "lambda:InvokeFunction"
        ])
        test_resource = st.text_input("Resource ARN:", "arn:aws:s3:::my-bucket/file.txt")
    
    with col2:
        st.markdown("### User Permissions")
        user_policies = st.multiselect("Attached Policies:", [
            "AmazonS3ReadOnlyAccess", "AmazonEC2ReadOnlyAccess", 
            "PowerUserAccess", "ReadOnlyAccess", "Custom-Dev-Policy"
        ])
        
        user_region = st.selectbox("User's Region:", ["us-east-1", "us-west-2", "eu-west-1"])
    
    if st.button("🔍 Simulate Policy Evaluation"):
        # Simple simulation logic
        allowed = False
        
        if test_action.startswith("s3:") and "AmazonS3ReadOnlyAccess" in user_policies:
            allowed = True
        elif test_action.startswith("ec2:") and ("AmazonEC2ReadOnlyAccess" in user_policies or "PowerUserAccess" in user_policies):
            allowed = True
        elif "PowerUserAccess" in user_policies and not test_action.startswith("iam:"):
            allowed = True
        elif "ReadOnlyAccess" in user_policies and any(x in test_action for x in ["Get", "List", "Describe"]):
            allowed = True
        
        if allowed:
            st.markdown('<div class="success-box">', unsafe_allow_html=True)
            st.markdown(f"""
            ### ✅ Action ALLOWED
            
            **Evaluation Result**: {test_user} can perform {test_action} on {test_resource}
            
            **Matching Policy**: {', '.join(user_policies)}
            **Region**: {user_region}
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="danger-box">', unsafe_allow_html=True)
            st.markdown(f"""
            ### ❌ Action DENIED
            
            **Evaluation Result**: {test_user} cannot perform {test_action} on {test_resource}
            
            **Reason**: No matching allow policy found
            **Current Policies**: {', '.join(user_policies) if user_policies else 'None'}
            """)
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Complete IAM Setup")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code("""
# Complete IAM setup with users, groups, roles, and policies
import boto3
import json

iam = boto3.client('iam')

def setup_complete_iam_structure():
    # 1. Create IAM Groups
    groups = [
        {
            'name': 'Developers',
            'description': 'Development team with limited production access'
        },
        {
            'name': 'Analysts', 
            'description': 'Business analysts with read-only access'
        },
        {
            'name': 'Administrators',
            'description': 'System administrators with full access'
        }
    ]
    
    created_groups = []
    for group in groups:
        try:
            iam.create_group(
                GroupName=group['name'],
                Path='/company/'
            )
            created_groups.append(group['name'])
            print(f"Created group: {group['name']}")
        except iam.exceptions.EntityAlreadyExistsException:
            print(f"Group {group['name']} already exists")
    
    # 2. Create Custom Policies
    developer_policy = {
        "Version": "2012-10-17",
        "Statement": [
            # Allow full access to development resources
            {
                "Effect": "Allow",
                "Action": [
                    "ec2:*",
                    "s3:*",
                    "lambda:*",
                    "logs:*"
                ],
                "Resource": "*",
                "Condition": {
                    "StringEquals": {
                        "aws:RequestedRegion": ["us-east-1", "us-west-2"]
                    },
                    "ForAllValues:StringLike": {
                        "aws:ResourceTag/Environment": ["dev", "test", "staging"]
                    }
                }
            },
            # Deny production access
            {
                "Effect": "Deny",
                "Action": "*",
                "Resource": "*",
                "Condition": {
                    "StringEquals": {
                        "aws:ResourceTag/Environment": "production"
                    }
                }
            }
        ]
    }
    
    # Create developer policy
    try:
        iam.create_policy(
            PolicyName='DeveloperRestrictedAccess',
            Path='/company/',
            PolicyDocument=json.dumps(developer_policy),
            Description='Restricted access for developers'
        )
        print("Created DeveloperRestrictedAccess policy")
    except iam.exceptions.EntityAlreadyExistsException:
        print("DeveloperRestrictedAccess policy already exists")
    
    # Create analyst policy (read-only)
    analyst_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "*:Describe*",
                    "*:List*", 
                    "*:Get*",
                    "cloudwatch:*",
                    "logs:*"
                ],
                "Resource": "*"
            }
        ]
    }
    
    try:
        iam.create_policy(
            PolicyName='AnalystReadOnlyAccess',
            Path='/company/',
            PolicyDocument=json.dumps(analyst_policy),
            Description='Read-only access for business analysts'
        )
        print("Created AnalystReadOnlyAccess policy")
    except iam.exceptions.EntityAlreadyExistsException:
        print("AnalystReadOnlyAccess policy already exists")
    
    # 3. Attach policies to groups
    policy_mappings = [
        ('Developers', 'arn:aws:iam::123456789012:policy/company/DeveloperRestrictedAccess'),
        ('Analysts', 'arn:aws:iam::123456789012:policy/company/AnalystReadOnlyAccess'),
        ('Administrators', 'arn:aws:iam::aws:policy/AdministratorAccess')
    ]
    
    for group_name, policy_arn in policy_mappings:
        try:
            iam.attach_group_policy(
                GroupName=group_name,
                PolicyArn=policy_arn
            )
            print(f"Attached policy to {group_name}")
        except Exception as e:
            print(f"Error attaching policy to {group_name}: {e}")
    
    # 4. Create IAM Users
    users = [
        {'username': 'john.developer', 'group': 'Developers'},
        {'username': 'jane.analyst', 'group': 'Analysts'},
        {'username': 'admin.user', 'group': 'Administrators'}
    ]
    
    for user in users:
        try:
            # Create user
            iam.create_user(
                UserName=user['username'],
                Path='/company/',
                Tags=[
                    {'Key': 'Department', 'Value': user['group']},
                    {'Key': 'CreatedBy', 'Value': 'IAM-Setup-Script'}
                ]
            )
            
            # Add user to group
            iam.add_user_to_group(
                GroupName=user['group'],
                UserName=user['username']
            )
            
            # Create login profile (console access)
            iam.create_login_profile(
                UserName=user['username'],
                Password='TempPassword123!',
                PasswordResetRequired=True
            )
            
            print(f"Created user: {user['username']}")
            
        except iam.exceptions.EntityAlreadyExistsException:
            print(f"User {user['username']} already exists")
    
    # 5. Create Service Roles
    # EC2 instance role
    ec2_assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        iam.create_role(
            RoleName='EC2-CloudWatch-Role',
            AssumeRolePolicyDocument=json.dumps(ec2_assume_role_policy),
            Path='/service/',
            Description='Allows EC2 instances to write to CloudWatch'
        )
        
        # Attach CloudWatch agent policy
        iam.attach_role_policy(
            RoleName='EC2-CloudWatch-Role',
            PolicyArn='arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy'
        )
        
        # Create instance profile
        iam.create_instance_profile(
            InstanceProfileName='EC2-CloudWatch-Profile',
            Path='/service/'
        )
        
        iam.add_role_to_instance_profile(
            InstanceProfileName='EC2-CloudWatch-Profile',
            RoleName='EC2-CloudWatch-Role'
        )
        
        print("Created EC2 service role and instance profile")
        
    except iam.exceptions.EntityAlreadyExistsException:
        print("EC2 service role already exists")
    
    # 6. Create Cross-Account Role
    cross_account_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::TRUSTED-ACCOUNT-ID:root"
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": "unique-external-id-12345"
                    }
                }
            }
        ]
    }
    
    try:
        iam.create_role(
            RoleName='CrossAccountAccessRole',
            AssumeRolePolicyDocument=json.dumps(cross_account_policy),
            Path='/cross-account/',
            Description='Allows trusted account to access specific resources'
        )
        
        # Attach limited permissions
        iam.attach_role_policy(
            RoleName='CrossAccountAccessRole',
            PolicyArn='arn:aws:iam::aws:policy/ReadOnlyAccess'
        )
        
        print("Created cross-account access role")
        
    except iam.exceptions.EntityAlreadyExistsException:
        print("Cross-account role already exists")
    
    # 7. Set up password policy
    try:
        iam.update_account_password_policy(
            MinimumPasswordLength=12,
            RequireSymbols=True,
            RequireNumbers=True,
            RequireUppercaseCharacters=True,
            RequireLowercaseCharacters=True,
            AllowUsersToChangePassword=True,
            MaxPasswordAge=90,
            PasswordReusePrevention=5,
            HardExpiry=False
        )
        print("Updated account password policy")
    except Exception as e:
        print(f"Error updating password policy: {e}")
    
    print("\n✅ Complete IAM structure setup finished!")
    print("📝 Next steps:")
    print("  1. Enable MFA for all users")
    print("  2. Set up CloudTrail for API logging")
    print("  3. Review and test permissions")
    print("  4. Set up access key rotation schedule")
    
    return {
        'groups_created': created_groups,
        'users_created': [u['username'] for u in users],
        'roles_created': ['EC2-CloudWatch-Role', 'CrossAccountAccessRole']
    }

# Execute the setup
# iam_setup = setup_complete_iam_structure()
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
    # 🔐 AWS Networking & Security
    ### Master AWS security fundamentals and networking concepts
    """)
    
    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🤝 Shared Responsibility", 
        "🏗️ Amazon VPC", 
        "🔒 Security Groups & NACLs",
        "👤 AWS IAM"
    ])
    
    with tab1:
        shared_responsibility_tab()
    
    with tab2:
        amazon_vpc_tab()
    
    with tab3:
        security_groups_nacls_tab()
        
    with tab4:
        aws_iam_tab()
    
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
