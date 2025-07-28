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
    page_title="AWS VPC & Network Security",
    page_icon="🔒",
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
    'warning': '#FFC107',
    'danger': '#DC3545'
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
        
        .network-component {{
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
        
        .security-rule {{
            background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
            padding: 10px;
            border-radius: 8px;
            color: white;
            margin: 5px 0;
        }}
        
        .blocked-rule {{
            background: linear-gradient(135deg, #dc3545 0%, #fd7e14 100%);
            padding: 10px;
            border-radius: 8px;
            color: white;
            margin: 5px 0;
        }}
        
        .subnet-card {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 10px;
            border: 1px solid #dee2e6;
            margin: 10px 0;
        }}
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    common.initialize_session_state()
    if 'vpc_configurations' not in st.session_state:
        st.session_state.vpc_configurations = []
    if 'security_groups' not in st.session_state:
        st.session_state.security_groups = {}
    if 'vpc_connections' not in st.session_state:
        st.session_state.vpc_connections = {}
    if 'nacl_rules' not in st.session_state:
        st.session_state.nacl_rules = []

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🏗️ Amazon Virtual Private Cloud (VPC) - Isolated network environment
            - 🔒 Security Groups & NACLs - Network security controls
            - 🔗 VPC Endpoints - Private connectivity to AWS services
            - 🌐 VPC Peering - Direct connections between VPCs
            
            **Learning Objectives:**
            - Understand VPC architecture and components
            - Master security group and NACL configurations
            - Learn private connectivity patterns
            - Explore VPC-to-VPC communication options
            """)

def create_vpc_architecture_diagram():
    """Create VPC architecture diagram"""
    return """
    graph TB
        subgraph "Region: us-west-2"
            VPC["VPC: 10.0.0.0/16<br/>🏗️ Virtual Private Cloud"]
            
            subgraph "Availability Zone A"
                PUB_A["Public Subnet A<br/>10.0.1.0/24<br/>🌐 Internet Access"]
                PRIV_A["Private Subnet A<br/>10.0.2.0/24<br/>🔒 Internal Only"]
                
                EC2_PUB_A["EC2 Instance<br/>Web Server<br/>10.0.1.10"]
                EC2_PRIV_A["EC2 Instance<br/>Database<br/>10.0.2.10"]
            end
            
            subgraph "Availability Zone B"
                PUB_B["Public Subnet B<br/>10.0.3.0/24<br/>🌐 Internet Access"]
                PRIV_B["Private Subnet B<br/>10.0.4.0/24<br/>🔒 Internal Only"]
                
                EC2_PUB_B["EC2 Instance<br/>Web Server<br/>10.0.3.10"]
                EC2_PRIV_B["EC2 Instance<br/>Database<br/>10.0.4.10"]
            end
            
            IGW["Internet Gateway<br/>🌐 Internet Access"]
            NAT_A["NAT Gateway A<br/>🔄 Outbound Internet"]
            NAT_B["NAT Gateway B<br/>🔄 Outbound Internet"]
            
            RT_PUB["Public Route Table<br/>📋 Internet Routes"]
            RT_PRIV_A["Private Route Table A<br/>📋 NAT Routes"]
            RT_PRIV_B["Private Route Table B<br/>📋 NAT Routes"]
        end
        
        INTERNET["🌐 Internet"]
        
        VPC --- PUB_A
        VPC --- PRIV_A
        VPC --- PUB_B
        VPC --- PRIV_B
        
        PUB_A --- EC2_PUB_A
        PRIV_A --- EC2_PRIV_A
        PUB_B --- EC2_PUB_B
        PRIV_B --- EC2_PRIV_B
        
        PUB_A --- NAT_A
        PUB_B --- NAT_B
        
        IGW --- INTERNET
        VPC --- IGW
        
        RT_PUB --- PUB_A
        RT_PUB --- PUB_B
        RT_PRIV_A --- PRIV_A
        RT_PRIV_B --- PRIV_B
        
        RT_PUB --- IGW
        RT_PRIV_A --- NAT_A
        RT_PRIV_B --- NAT_B
        
        style VPC fill:#FF9900,stroke:#232F3E,color:#fff
        style PUB_A fill:#4B9EDB,stroke:#232F3E,color:#fff
        style PUB_B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style PRIV_A fill:#3FB34F,stroke:#232F3E,color:#fff
        style PRIV_B fill:#3FB34F,stroke:#232F3E,color:#fff
        style IGW fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_security_groups_nacl_diagram():
    """Create security groups and NACLs diagram"""
    return """
    graph TB
        subgraph "VPC: 10.0.0.0/16"
            subgraph "Subnet: 10.0.1.0/24"
                NACL["Network ACL<br/>🔥 Subnet-level Firewall<br/>Stateless"]
                
                subgraph "Instance Level"
                    EC2_1["EC2 Instance<br/>Web Server<br/>10.0.1.10"]
                    SG_WEB["Security Group: Web-SG<br/>🛡️ Instance-level Firewall<br/>Stateful"]
                    
                    EC2_2["EC2 Instance<br/>Database<br/>10.0.1.20"]
                    SG_DB["Security Group: DB-SG<br/>🛡️ Instance-level Firewall<br/>Stateful"]
                end
            end
        end
        
        USER["👤 User Request<br/>HTTPS (Port 443)"]
        ADMIN["👨‍💻 Admin SSH<br/>SSH (Port 22)"]
        
        USER --> NACL
        ADMIN --> NACL
        
        NACL --> SG_WEB
        NACL --> SG_DB
        
        SG_WEB --> EC2_1
        SG_DB --> EC2_2
        
        EC2_1 --> EC2_2
        
        style NACL fill:#FF9900,stroke:#232F3E,color:#fff
        style SG_WEB fill:#4B9EDB,stroke:#232F3E,color:#fff
        style SG_DB fill:#3FB34F,stroke:#232F3E,color:#fff
        style EC2_1 fill:#232F3E,stroke:#FF9900,color:#fff
        style EC2_2 fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_vpc_endpoints_diagram():
    """Create VPC endpoints diagram"""
    return """
    graph TB
        subgraph "VPC: 10.0.0.0/16"
            subgraph "Private Subnet"
                EC2["EC2 Instance<br/>Application Server<br/>10.0.2.10"]
                LAMBDA["Lambda Function<br/>Data Processing"]
            end
            
            subgraph "VPC Endpoints"
                EP_S3["S3 Gateway Endpoint<br/>📦 S3 Access<br/>No Internet Required"]
                EP_DDB["DynamoDB Gateway Endpoint<br/>🗄️ DynamoDB Access<br/>No Internet Required"]
                EP_SQS["SQS Interface Endpoint<br/>📨 SQS Access<br/>Private IP"]
                EP_SNS["SNS Interface Endpoint<br/>📢 SNS Access<br/>Private IP"]
            end
            
            ROUTE_TABLE["Route Table<br/>📋 Routes to Endpoints"]
        end
        
        subgraph "AWS Services"
            S3["Amazon S3<br/>📦 Object Storage"]
            DDB["Amazon DynamoDB<br/>🗄️ NoSQL Database"]
            SQS["Amazon SQS<br/>📨 Message Queue"]
            SNS["Amazon SNS<br/>📢 Notifications"]
        end
        
        EC2 --> EP_S3
        EC2 --> EP_DDB
        EC2 --> EP_SQS
        EC2 --> EP_SNS
        
        LAMBDA --> EP_S3
        LAMBDA --> EP_DDB
        LAMBDA --> EP_SQS
        LAMBDA --> EP_SNS
        
        EP_S3 --> S3
        EP_DDB --> DDB
        EP_SQS --> SQS
        EP_SNS --> SNS
        
        ROUTE_TABLE --> EP_S3
        ROUTE_TABLE --> EP_DDB
        
        style EC2 fill:#FF9900,stroke:#232F3E,color:#fff
        style LAMBDA fill:#FF9900,stroke:#232F3E,color:#fff
        style EP_S3 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style EP_DDB fill:#4B9EDB,stroke:#232F3E,color:#fff
        style EP_SQS fill:#3FB34F,stroke:#232F3E,color:#fff
        style EP_SNS fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_vpc_peering_diagram():
    """Create VPC peering diagram"""
    return """
    graph TB
        subgraph "Region: us-west-2"
            subgraph "Production VPC"
                VPC_PROD["VPC-Production<br/>10.0.0.0/16"]
                SUBNET_PROD["Private Subnet<br/>10.0.1.0/24"]
                EC2_PROD["Production App<br/>10.0.1.10"]
                RT_PROD["Route Table<br/>📋 Peering Routes"]
            end
            
            subgraph "Development VPC"
                VPC_DEV["VPC-Development<br/>10.1.0.0/16"]
                SUBNET_DEV["Private Subnet<br/>10.1.1.0/24"]
                EC2_DEV["Development App<br/>10.1.1.10"]
                RT_DEV["Route Table<br/>📋 Peering Routes"]
            end
        end
        
        subgraph "Region: us-east-1"
            subgraph "DR VPC"
                VPC_DR["VPC-DR<br/>10.2.0.0/16"]
                SUBNET_DR["Private Subnet<br/>10.2.1.0/24"]
                EC2_DR["DR App<br/>10.2.1.10"]
                RT_DR["Route Table<br/>📋 Peering Routes"]
            end
        end
        
        PEER_1["VPC Peering Connection<br/>🔗 Same Region<br/>pcx-12345678"]
        PEER_2["VPC Peering Connection<br/>🔗 Cross Region<br/>pcx-87654321"]
        
        VPC_PROD --- PEER_1
        VPC_DEV --- PEER_1
        
        VPC_PROD --- PEER_2
        VPC_DR --- PEER_2
        
        VPC_PROD --- SUBNET_PROD
        VPC_DEV --- SUBNET_DEV
        VPC_DR --- SUBNET_DR
        
        SUBNET_PROD --- EC2_PROD
        SUBNET_DEV --- EC2_DEV
        SUBNET_DR --- EC2_DR
        
        RT_PROD --- SUBNET_PROD
        RT_DEV --- SUBNET_DEV
        RT_DR --- SUBNET_DR
        
        style VPC_PROD fill:#FF9900,stroke:#232F3E,color:#fff
        style VPC_DEV fill:#4B9EDB,stroke:#232F3E,color:#fff
        style VPC_DR fill:#3FB34F,stroke:#232F3E,color:#fff
        style PEER_1 fill:#232F3E,stroke:#FF9900,color:#fff
        style PEER_2 fill:#232F3E,stroke:#FF9900,color:#fff
    """

def vpc_tab():
    """Amazon Virtual Private Cloud (VPC) tab content"""
    st.markdown("## 🏗️ Amazon Virtual Private Cloud (VPC)")
    st.markdown("*Provision a Logically Isolated Section of the AWS Cloud*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What is Amazon VPC?
    Amazon Virtual Private Cloud (VPC) gives you complete control over your virtual networking environment:
    - **Logical Isolation**: Your own private section of the AWS cloud
    - **Custom IP Ranges**: Choose your own IP address ranges (CIDR blocks)
    - **Subnets**: Divide your VPC into public and private subnets
    - **Route Tables**: Control network traffic routing
    - **Security**: Multiple layers of security including security groups and NACLs
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # VPC Architecture
    st.markdown("#### 🏗️ VPC Architecture Overview")
    common.mermaid(create_vpc_architecture_diagram(), height=800)
    
    # Interactive VPC Builder
    st.markdown("#### 🎨 Interactive VPC Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### VPC Configuration")
        vpc_name = st.text_input("VPC Name", value="MyVPC")
        vpc_cidr = st.selectbox("VPC CIDR Block", [
            "10.0.0.0/16", "172.16.0.0/16", "192.168.0.0/16", "10.1.0.0/16"
        ])
        region = st.selectbox("AWS Region", [
            "us-west-2", "us-east-1", "eu-west-1", "ap-southeast-1"
        ])
        
    with col2:
        st.markdown("##### Subnet Configuration")
        num_azs = st.slider("Number of Availability Zones", 1, 3, 2)
        subnet_type = st.multiselect("Subnet Types", [
            "Public Subnet", "Private Subnet", "Database Subnet"
        ], default=["Public Subnet", "Private Subnet"])
        
    # Generate subnet configuration
    subnets = generate_subnet_config(vpc_cidr, num_azs, subnet_type)
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Generated VPC Configuration
    **VPC Name**: {vpc_name}  
    **CIDR Block**: {vpc_cidr}  
    **Region**: {region}  
    **Availability Zones**: {num_azs}  
    **Total Subnets**: {len(subnets)}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Display subnet details
    st.markdown("#### 📋 Subnet Configuration Details")
    
    subnet_data = []
    for subnet in subnets:
        subnet_data.append({
            'Subnet Name': subnet['name'],
            'CIDR Block': subnet['cidr'],
            'Availability Zone': subnet['az'],
            'Type': subnet['type'],
            'Route Table': subnet['route_table']
        })
    
    df_subnets = pd.DataFrame(subnet_data)
    st.dataframe(df_subnets, use_container_width=True)
    
    # VPC Components
    st.markdown("#### 🔧 VPC Components")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="network-component">', unsafe_allow_html=True)
        st.markdown("""
        ### 🌐 Internet Gateway
        **Purpose**: Enables internet access for public subnets
        
        **Features**:
        - Horizontally scaled
        - Redundant & highly available
        - Attached to VPC
        - Required for public subnets
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="network-component">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 NAT Gateway
        **Purpose**: Enables outbound internet access for private subnets
        
        **Features**:
        - Managed NAT service
        - Placed in public subnet
        - Elastic IP required
        - AZ-specific
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="network-component">', unsafe_allow_html=True)
        st.markdown("""
        ### 📋 Route Tables
        **Purpose**: Control network traffic routing
        
        **Features**:
        - Associated with subnets
        - Default and custom routes
        - Longest prefix match
        - Local route (automatic)
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 VPC Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["CloudFormation", "Terraform", "AWS CLI"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# CloudFormation Template for VPC with Public and Private Subnets

AWSTemplateFormatVersion: '2010-09-09'
Description: 'VPC with public and private subnets across two AZs'

Parameters:
  VpcCIDR:
    Description: CIDR block for the VPC
    Type: String
    Default: 10.0.0.0/16
  
  PublicSubnet1CIDR:
    Description: CIDR block for public subnet in AZ1
    Type: String
    Default: 10.0.1.0/24
    
  PublicSubnet2CIDR:
    Description: CIDR block for public subnet in AZ2
    Type: String
    Default: 10.0.3.0/24
    
  PrivateSubnet1CIDR:
    Description: CIDR block for private subnet in AZ1
    Type: String
    Default: 10.0.2.0/24
    
  PrivateSubnet2CIDR:
    Description: CIDR block for private subnet in AZ2
    Type: String
    Default: 10.0.4.0/24

Resources:
  # VPC
  MyVPC:
    Type: AWS::EC2::VPC
    Properties:
      CidrBlock: !Ref VpcCIDR
      EnableDnsHostnames: true
      EnableDnsSupport: true
      Tags:
        - Key: Name
          Value: MyVPC

  # Internet Gateway
  InternetGateway:
    Type: AWS::EC2::InternetGateway
    Properties:
      Tags:
        - Key: Name
          Value: MyVPC-IGW

  # Attach Internet Gateway to VPC
  InternetGatewayAttachment:
    Type: AWS::EC2::VPCGatewayAttachment
    Properties:
      InternetGatewayId: !Ref InternetGateway
      VpcId: !Ref MyVPC

  # Public Subnet in AZ1
  PublicSubnet1:
    Type: AWS::EC2::Subnet
    Properties:
      VpcId: !Ref MyVPC
      AvailabilityZone: !Select [0, !GetAZs '']
      CidrBlock: !Ref PublicSubnet1CIDR
      MapPublicIpOnLaunch: true
      Tags:
        - Key: Name
          Value: Public Subnet AZ1

  # Public Subnet in AZ2
  PublicSubnet2:
    Type: AWS::EC2::Subnet
    Properties:
      VpcId: !Ref MyVPC
      AvailabilityZone: !Select [1, !GetAZs '']
      CidrBlock: !Ref PublicSubnet2CIDR
      MapPublicIpOnLaunch: true
      Tags:
        - Key: Name
          Value: Public Subnet AZ2

  # Private Subnet in AZ1
  PrivateSubnet1:
    Type: AWS::EC2::Subnet
    Properties:
      VpcId: !Ref MyVPC
      AvailabilityZone: !Select [0, !GetAZs '']
      CidrBlock: !Ref PrivateSubnet1CIDR
      Tags:
        - Key: Name
          Value: Private Subnet AZ1

  # Private Subnet in AZ2
  PrivateSubnet2:
    Type: AWS::EC2::Subnet
    Properties:
      VpcId: !Ref MyVPC
      AvailabilityZone: !Select [1, !GetAZs '']
      CidrBlock: !Ref PrivateSubnet2CIDR
      Tags:
        - Key: Name
          Value: Private Subnet AZ2

  # NAT Gateway 1 EIP
  NatGateway1EIP:
    Type: AWS::EC2::EIP
    DependsOn: InternetGatewayAttachment
    Properties:
      Domain: vpc

  # NAT Gateway 2 EIP
  NatGateway2EIP:
    Type: AWS::EC2::EIP
    DependsOn: InternetGatewayAttachment
    Properties:
      Domain: vpc

  # NAT Gateway 1
  NatGateway1:
    Type: AWS::EC2::NatGateway
    Properties:
      AllocationId: !GetAtt NatGateway1EIP.AllocationId
      SubnetId: !Ref PublicSubnet1
      Tags:
        - Key: Name
          Value: NAT Gateway AZ1

  # NAT Gateway 2
  NatGateway2:
    Type: AWS::EC2::NatGateway
    Properties:
      AllocationId: !GetAtt NatGateway2EIP.AllocationId
      SubnetId: !Ref PublicSubnet2
      Tags:
        - Key: Name
          Value: NAT Gateway AZ2

  # Public Route Table
  PublicRouteTable:
    Type: AWS::EC2::RouteTable
    Properties:
      VpcId: !Ref MyVPC
      Tags:
        - Key: Name
          Value: Public Route Table

  # Default Public Route
  DefaultPublicRoute:
    Type: AWS::EC2::Route
    DependsOn: InternetGatewayAttachment
    Properties:
      RouteTableId: !Ref PublicRouteTable
      DestinationCidrBlock: 0.0.0.0/0
      GatewayId: !Ref InternetGateway

  # Associate Public Subnet 1 with Public Route Table
  PublicSubnet1RouteTableAssociation:
    Type: AWS::EC2::SubnetRouteTableAssociation
    Properties:
      RouteTableId: !Ref PublicRouteTable
      SubnetId: !Ref PublicSubnet1

  # Associate Public Subnet 2 with Public Route Table
  PublicSubnet2RouteTableAssociation:
    Type: AWS::EC2::SubnetRouteTableAssociation
    Properties:
      RouteTableId: !Ref PublicRouteTable
      SubnetId: !Ref PublicSubnet2

  # Private Route Table 1
  PrivateRouteTable1:
    Type: AWS::EC2::RouteTable
    Properties:
      VpcId: !Ref MyVPC
      Tags:
        - Key: Name
          Value: Private Route Table AZ1

  # Default Private Route 1
  DefaultPrivateRoute1:
    Type: AWS::EC2::Route
    Properties:
      RouteTableId: !Ref PrivateRouteTable1
      DestinationCidrBlock: 0.0.0.0/0
      NatGatewayId: !Ref NatGateway1

  # Associate Private Subnet 1 with Private Route Table 1
  PrivateSubnet1RouteTableAssociation:
    Type: AWS::EC2::SubnetRouteTableAssociation
    Properties:
      RouteTableId: !Ref PrivateRouteTable1
      SubnetId: !Ref PrivateSubnet1

Outputs:
  VPC:
    Description: VPC ID
    Value: !Ref MyVPC
    Export:
      Name: !Sub ${AWS::StackName}-VPC

  PublicSubnets:
    Description: Public subnet IDs
    Value: !Join [",", [!Ref PublicSubnet1, !Ref PublicSubnet2]]
    Export:
      Name: !Sub ${AWS::StackName}-PublicSubnets

  PrivateSubnets:
    Description: Private subnet IDs
    Value: !Join [",", [!Ref PrivateSubnet1, !Ref PrivateSubnet2]]
    Export:
      Name: !Sub ${AWS::StackName}-PrivateSubnets
        ''', language='yaml')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Terraform Configuration for VPC

# Variables
variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "Availability zones"
  type        = list(string)
  default     = ["us-west-2a", "us-west-2b"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.3.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.2.0/24", "10.0.4.0/24"]
}

# Data sources
data "aws_availability_zones" "available" {
  state = "available"
}

# VPC
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "MyVPC"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "MyVPC-IGW"
  }
}

# Public Subnets
resource "aws_subnet" "public" {
  count = length(var.public_subnet_cidrs)

  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "Public Subnet ${count.index + 1}"
    Type = "Public"
  }
}

# Private Subnets
resource "aws_subnet" "private" {
  count = length(var.private_subnet_cidrs)

  vpc_id            = aws_vpc.main.id
  cidr_block        = var.private_subnet_cidrs[count.index]
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Name = "Private Subnet ${count.index + 1}"
    Type = "Private"
  }
}

# Elastic IPs for NAT Gateways
resource "aws_eip" "nat" {
  count = length(aws_subnet.public)

  domain = "vpc"
  depends_on = [aws_internet_gateway.main]

  tags = {
    Name = "NAT Gateway EIP ${count.index + 1}"
  }
}

# NAT Gateways
resource "aws_nat_gateway" "main" {
  count = length(aws_subnet.public)

  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id

  tags = {
    Name = "NAT Gateway ${count.index + 1}"
  }

  depends_on = [aws_internet_gateway.main]
}

# Route Tables
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "Public Route Table"
  }
}

resource "aws_route_table" "private" {
  count = length(aws_subnet.private)

  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main[count.index].id
  }

  tags = {
    Name = "Private Route Table ${count.index + 1}"
  }
}

# Route Table Associations
resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count = length(aws_subnet.private)

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

# Outputs
output "vpc_id" {
  description = "ID of the VPC"
  value       = aws_vpc.main.id
}

output "public_subnet_ids" {
  description = "IDs of the public subnets"
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "IDs of the private subnets"
  value       = aws_subnet.private[*].id
}

output "internet_gateway_id" {
  description = "ID of the Internet Gateway"
  value       = aws_internet_gateway.main.id
}

output "nat_gateway_ids" {
  description = "IDs of the NAT Gateways"
  value       = aws_nat_gateway.main[*].id
}
        ''', language='hcl')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
#!/bin/bash

# AWS CLI Commands for VPC Creation

# Set variables
VPC_CIDR="10.0.0.0/16"
PUBLIC_SUBNET_1_CIDR="10.0.1.0/24"
PUBLIC_SUBNET_2_CIDR="10.0.3.0/24"
PRIVATE_SUBNET_1_CIDR="10.0.2.0/24"
PRIVATE_SUBNET_2_CIDR="10.0.4.0/24"
REGION="us-west-2"

# Create VPC
echo "Creating VPC..."
VPC_ID=$(aws ec2 create-vpc \
    --cidr-block $VPC_CIDR \
    --region $REGION \
    --tag-specifications 'ResourceType=vpc,Tags=[{Key=Name,Value=MyVPC}]' \
    --query 'Vpc.VpcId' \
    --output text)

echo "VPC Created: $VPC_ID"

# Enable DNS hostnames
aws ec2 modify-vpc-attribute \
    --vpc-id $VPC_ID \
    --enable-dns-hostnames \
    --region $REGION

# Create Internet Gateway
echo "Creating Internet Gateway..."
IGW_ID=$(aws ec2 create-internet-gateway \
    --region $REGION \
    --tag-specifications 'ResourceType=internet-gateway,Tags=[{Key=Name,Value=MyVPC-IGW}]' \
    --query 'InternetGateway.InternetGatewayId' \
    --output text)

echo "Internet Gateway Created: $IGW_ID"

# Attach Internet Gateway to VPC
aws ec2 attach-internet-gateway \
    --internet-gateway-id $IGW_ID \
    --vpc-id $VPC_ID \
    --region $REGION

# Get Availability Zones
AZ_1=$(aws ec2 describe-availability-zones \
    --region $REGION \
    --query 'AvailabilityZones[0].ZoneName' \
    --output text)

AZ_2=$(aws ec2 describe-availability-zones \
    --region $REGION \
    --query 'AvailabilityZones[1].ZoneName' \
    --output text)

# Create Public Subnet 1
echo "Creating Public Subnet 1..."
PUBLIC_SUBNET_1_ID=$(aws ec2 create-subnet \
    --vpc-id $VPC_ID \
    --cidr-block $PUBLIC_SUBNET_1_CIDR \
    --availability-zone $AZ_1 \
    --region $REGION \
    --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=Public Subnet AZ1}]' \
    --query 'Subnet.SubnetId' \
    --output text)

# Create Public Subnet 2
echo "Creating Public Subnet 2..."
PUBLIC_SUBNET_2_ID=$(aws ec2 create-subnet \
    --vpc-id $VPC_ID \
    --cidr-block $PUBLIC_SUBNET_2_CIDR \
    --availability-zone $AZ_2 \
    --region $REGION \
    --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=Public Subnet AZ2}]' \
    --query 'Subnet.SubnetId' \
    --output text)

# Create Private Subnet 1
echo "Creating Private Subnet 1..."
PRIVATE_SUBNET_1_ID=$(aws ec2 create-subnet \
    --vpc-id $VPC_ID \
    --cidr-block $PRIVATE_SUBNET_1_CIDR \
    --availability-zone $AZ_1 \
    --region $REGION \
    --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=Private Subnet AZ1}]' \
    --query 'Subnet.SubnetId' \
    --output text)

# Create Private Subnet 2
echo "Creating Private Subnet 2..."
PRIVATE_SUBNET_2_ID=$(aws ec2 create-subnet \
    --vpc-id $VPC_ID \
    --cidr-block $PRIVATE_SUBNET_2_CIDR \
    --availability-zone $AZ_2 \
    --region $REGION \
    --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=Private Subnet AZ2}]' \
    --query 'Subnet.SubnetId' \
    --output text)

# Enable auto-assign public IP for public subnets
aws ec2 modify-subnet-attribute \
    --subnet-id $PUBLIC_SUBNET_1_ID \
    --map-public-ip-on-launch \
    --region $REGION

aws ec2 modify-subnet-attribute \
    --subnet-id $PUBLIC_SUBNET_2_ID \
    --map-public-ip-on-launch \
    --region $REGION

# Create Elastic IPs for NAT Gateways
echo "Creating Elastic IPs for NAT Gateways..."
EIP_1_ID=$(aws ec2 allocate-address \
    --domain vpc \
    --region $REGION \
    --tag-specifications 'ResourceType=elastic-ip,Tags=[{Key=Name,Value=NAT Gateway EIP 1}]' \
    --query 'AllocationId' \
    --output text)

EIP_2_ID=$(aws ec2 allocate-address \
    --domain vpc \
    --region $REGION \
    --tag-specifications 'ResourceType=elastic-ip,Tags=[{Key=Name,Value=NAT Gateway EIP 2}]' \
    --query 'AllocationId' \
    --output text)

# Create NAT Gateways
echo "Creating NAT Gateways..."
NAT_GW_1_ID=$(aws ec2 create-nat-gateway \
    --subnet-id $PUBLIC_SUBNET_1_ID \
    --allocation-id $EIP_1_ID \
    --region $REGION \
    --tag-specifications 'ResourceType=nat-gateway,Tags=[{Key=Name,Value=NAT Gateway AZ1}]' \
    --query 'NatGateway.NatGatewayId' \
    --output text)

NAT_GW_2_ID=$(aws ec2 create-nat-gateway \
    --subnet-id $PUBLIC_SUBNET_2_ID \
    --allocation-id $EIP_2_ID \
    --region $REGION \
    --tag-specifications 'ResourceType=nat-gateway,Tags=[{Key=Name,Value=NAT Gateway AZ2}]' \
    --query 'NatGateway.NatGatewayId' \
    --output text)

# Wait for NAT Gateways to be available
echo "Waiting for NAT Gateways to be available..."
aws ec2 wait nat-gateway-available --nat-gateway-ids $NAT_GW_1_ID --region $REGION
aws ec2 wait nat-gateway-available --nat-gateway-ids $NAT_GW_2_ID --region $REGION

# Create Route Tables
echo "Creating Route Tables..."
PUBLIC_RT_ID=$(aws ec2 create-route-table \
    --vpc-id $VPC_ID \
    --region $REGION \
    --tag-specifications 'ResourceType=route-table,Tags=[{Key=Name,Value=Public Route Table}]' \
    --query 'RouteTable.RouteTableId' \
    --output text)

PRIVATE_RT_1_ID=$(aws ec2 create-route-table \
    --vpc-id $VPC_ID \
    --region $REGION \
    --tag-specifications 'ResourceType=route-table,Tags=[{Key=Name,Value=Private Route Table AZ1}]' \
    --query 'RouteTable.RouteTableId' \
    --output text)

PRIVATE_RT_2_ID=$(aws ec2 create-route-table \
    --vpc-id $VPC_ID \
    --region $REGION \
    --tag-specifications 'ResourceType=route-table,Tags=[{Key=Name,Value=Private Route Table AZ2}]' \
    --query 'RouteTable.RouteTableId' \
    --output text)

# Create Routes
echo "Creating Routes..."
# Public route to Internet Gateway
aws ec2 create-route \
    --route-table-id $PUBLIC_RT_ID \
    --destination-cidr-block 0.0.0.0/0 \
    --gateway-id $IGW_ID \
    --region $REGION

# Private routes to NAT Gateways
aws ec2 create-route \
    --route-table-id $PRIVATE_RT_1_ID \
    --destination-cidr-block 0.0.0.0/0 \
    --nat-gateway-id $NAT_GW_1_ID \
    --region $REGION

aws ec2 create-route \
    --route-table-id $PRIVATE_RT_2_ID \
    --destination-cidr-block 0.0.0.0/0 \
    --nat-gateway-id $NAT_GW_2_ID \
    --region $REGION

# Associate Route Tables with Subnets
echo "Associating Route Tables with Subnets..."
aws ec2 associate-route-table \
    --route-table-id $PUBLIC_RT_ID \
    --subnet-id $PUBLIC_SUBNET_1_ID \
    --region $REGION

aws ec2 associate-route-table \
    --route-table-id $PUBLIC_RT_ID \
    --subnet-id $PUBLIC_SUBNET_2_ID \
    --region $REGION

aws ec2 associate-route-table \
    --route-table-id $PRIVATE_RT_1_ID \
    --subnet-id $PRIVATE_SUBNET_1_ID \
    --region $REGION

aws ec2 associate-route-table \
    --route-table-id $PRIVATE_RT_2_ID \
    --subnet-id $PRIVATE_SUBNET_2_ID \
    --region $REGION

echo "VPC Creation Complete!"
echo "VPC ID: $VPC_ID"
echo "Public Subnet 1 ID: $PUBLIC_SUBNET_1_ID"
echo "Public Subnet 2 ID: $PUBLIC_SUBNET_2_ID"
echo "Private Subnet 1 ID: $PRIVATE_SUBNET_1_ID"
echo "Private Subnet 2 ID: $PRIVATE_SUBNET_2_ID"
        ''', language='bash')
        st.markdown('</div>', unsafe_allow_html=True)

def security_groups_nacl_tab():
    """Security Groups & NACLs tab content"""
    st.markdown("## 🔒 Security Groups & NACLs")
    st.markdown("*Two AWS features to increase security in your VPC: security groups and network ACLs*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Network Security Layers
    AWS provides multiple layers of network security:
    - **Security Groups**: Stateful firewall at the instance level
    - **Network ACLs**: Stateless firewall at the subnet level
    - **Defense in Depth**: Multiple security layers for comprehensive protection
    - **Principle of Least Privilege**: Only allow necessary traffic
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Security architecture
    st.markdown("#### 🛡️ Security Architecture")
    common.mermaid(create_security_groups_nacl_diagram(), height=600)
    
    # Security Groups vs NACLs comparison
    st.markdown("#### ⚖️ Security Groups vs Network ACLs")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛡️ Security Groups
        **Operates at**: Instance level  
        **State**: Stateful (return traffic automatically allowed)  
        **Rules**: Allow rules only  
        **Rule Processing**: All rules evaluated before decision  
        **Scope**: Specific to instance (explicit association)
        
        **Use Cases**:
        - Application-level security
        - Instance-specific rules
        - Dynamic security policies
        - Microservice isolation
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔥 Network ACLs
        **Operates at**: Subnet level  
        **State**: Stateless (return traffic must be explicitly allowed)  
        **Rules**: Allow and deny rules  
        **Rule Processing**: Rules processed in order (lowest number first)  
        **Scope**: All instances in associated subnets
        
        **Use Cases**:
        - Subnet-level security
        - Explicit deny rules
        - Compliance requirements
        - Network-wide policies
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Security Rule Builder
    st.markdown("#### 🎮 Interactive Security Rule Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Security Group Configuration")
        sg_name = st.text_input("Security Group Name", value="WebServer-SG")
        sg_description = st.text_area("Description", value="Security group for web servers")
        
        # Inbound rules
        st.markdown("**Inbound Rules**")
        inbound_rules = []
        
        num_inbound = st.number_input("Number of Inbound Rules", min_value=1, max_value=10, value=3)
        
        for i in range(num_inbound):
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                protocol = st.selectbox(f"Protocol {i+1}", ["HTTP", "HTTPS", "SSH", "MySQL", "PostgreSQL", "Custom"], key=f"in_protocol_{i}")
            with col_b:
                port = get_default_port(protocol) if protocol != "Custom" else st.number_input(f"Port {i+1}", min_value=1, max_value=65535, value=80, key=f"in_port_{i}")
            with col_c:
                source = st.selectbox(f"Source {i+1}", ["0.0.0.0/0", "10.0.0.0/16", "My IP", "Custom"], key=f"in_source_{i}")
            
            inbound_rules.append({
                'protocol': protocol,
                'port': port,
                'source': source,
                'description': f"{protocol} access"
            })
    
    with col2:
        st.markdown("##### Network ACL Configuration")
        nacl_name = st.text_input("Network ACL Name", value="WebTier-NACL")
        
        # NACL rules
        st.markdown("**NACL Rules**")
        nacl_rules = []
        
        num_nacl = st.number_input("Number of NACL Rules", min_value=1, max_value=10, value=4)
        
        for i in range(num_nacl):
            col_a, col_b, col_c, col_d = st.columns(4)
            with col_a:
                rule_number = st.number_input(f"Rule # {i+1}", min_value=100, max_value=32766, value=100*(i+1), key=f"nacl_rule_{i}")
            with col_b:
                action = st.selectbox(f"Action {i+1}", ["ALLOW", "DENY"], key=f"nacl_action_{i}")
            with col_c:
                protocol = st.selectbox(f"Protocol {i+1}", ["HTTP", "HTTPS", "SSH", "ALL"], key=f"nacl_protocol_{i}")
            with col_d:
                source = st.selectbox(f"Source {i+1}", ["0.0.0.0/0", "10.0.0.0/16", "Custom"], key=f"nacl_source_{i}")
            
            nacl_rules.append({
                'rule_number': rule_number,
                'action': action,
                'protocol': protocol,
                'source': source
            })
    
    # Display generated rules
    st.markdown("#### 📋 Generated Security Configuration")
    
    tab1, tab2 = st.tabs(["Security Group Rules", "Network ACL Rules"])
    
    with tab1:
        st.markdown(f"**Security Group**: {sg_name}")
        st.markdown(f"**Description**: {sg_description}")
        
        # Inbound rules table
        inbound_data = []
        for rule in inbound_rules:
            inbound_data.append({
                'Type': rule['protocol'],
                'Port': rule['port'],
                'Source': rule['source'],
                'Description': rule['description']
            })
        
        df_inbound = pd.DataFrame(inbound_data)
        st.dataframe(df_inbound, use_container_width=True)
        
        # Security group evaluation
        st.markdown("##### Security Group Evaluation")
        evaluate_security_group_rules(inbound_rules)
    
    with tab2:
        st.markdown(f"**Network ACL**: {nacl_name}")
        
        # NACL rules table
        nacl_data = []
        for rule in nacl_rules:
            nacl_data.append({
                'Rule #': rule['rule_number'],
                'Action': rule['action'],
                'Protocol': rule['protocol'],
                'Source': rule['source']
            })
        
        df_nacl = pd.DataFrame(nacl_data)
        st.dataframe(df_nacl, use_container_width=True)
        
        # NACL evaluation
        st.markdown("##### Network ACL Evaluation")
        evaluate_nacl_rules(nacl_rules)
    
    # Common security patterns
    st.markdown("#### 🔧 Common Security Patterns")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="network-component">', unsafe_allow_html=True)
        st.markdown("""
        ### 🌐 Web Tier Security
        **Inbound Rules**:
        - HTTP (80) from 0.0.0.0/0
        - HTTPS (443) from 0.0.0.0/0
        - SSH (22) from Bastion SG
        
        **Outbound Rules**:
        - MySQL (3306) to DB SG
        - HTTPS (443) to 0.0.0.0/0
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="network-component">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗄️ Database Tier Security
        **Inbound Rules**:
        - MySQL (3306) from Web SG
        - PostgreSQL (5432) from App SG
        - SSH (22) from Bastion SG
        
        **Outbound Rules**:
        - HTTPS (443) to 0.0.0.0/0
        - NTP (123) to 0.0.0.0/0
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="network-component">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 Bastion Host Security
        **Inbound Rules**:
        - SSH (22) from Admin IP
        - RDP (3389) from Admin IP
        
        **Outbound Rules**:
        - SSH (22) to Private SGs
        - RDP (3389) to Private SGs
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Security Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Security Group Creation", "Network ACL Configuration", "Security Testing"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create Security Groups using AWS CLI

# Web Server Security Group
aws ec2 create-security-group \
    --group-name WebServer-SG \
    --description "Security group for web servers" \
    --vpc-id vpc-12345678 \
    --region us-west-2

# Get the security group ID
WEB_SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=WebServer-SG" \
    --query 'SecurityGroups[0].GroupId' \
    --output text)

# Add inbound rules to Web Server Security Group
# HTTP access from anywhere
aws ec2 authorize-security-group-ingress \
    --group-id $WEB_SG_ID \
    --protocol tcp \
    --port 80 \
    --cidr 0.0.0.0/0

# HTTPS access from anywhere
aws ec2 authorize-security-group-ingress \
    --group-id $WEB_SG_ID \
    --protocol tcp \
    --port 443 \
    --cidr 0.0.0.0/0

# SSH access from specific IP
aws ec2 authorize-security-group-ingress \
    --group-id $WEB_SG_ID \
    --protocol tcp \
    --port 22 \
    --cidr 203.0.113.0/24

# Database Security Group
aws ec2 create-security-group \
    --group-name Database-SG \
    --description "Security group for database servers" \
    --vpc-id vpc-12345678 \
    --region us-west-2

DB_SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=Database-SG" \
    --query 'SecurityGroups[0].GroupId' \
    --output text)

# MySQL access from Web Server Security Group
aws ec2 authorize-security-group-ingress \
    --group-id $DB_SG_ID \
    --protocol tcp \
    --port 3306 \
    --source-group $WEB_SG_ID

# PostgreSQL access from Web Server Security Group
aws ec2 authorize-security-group-ingress \
    --group-id $DB_SG_ID \
    --protocol tcp \
    --port 5432 \
    --source-group $WEB_SG_ID

# Bastion Host Security Group
aws ec2 create-security-group \
    --group-name Bastion-SG \
    --description "Security group for bastion hosts" \
    --vpc-id vpc-12345678 \
    --region us-west-2

BASTION_SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=Bastion-SG" \
    --query 'SecurityGroups[0].GroupId' \
    --output text)

# SSH access from admin IP ranges
aws ec2 authorize-security-group-ingress \
    --group-id $BASTION_SG_ID \
    --protocol tcp \
    --port 22 \
    --cidr 198.51.100.0/24

# Add SSH from bastion to other security groups
aws ec2 authorize-security-group-ingress \
    --group-id $WEB_SG_ID \
    --protocol tcp \
    --port 22 \
    --source-group $BASTION_SG_ID

aws ec2 authorize-security-group-ingress \
    --group-id $DB_SG_ID \
    --protocol tcp \
    --port 22 \
    --source-group $BASTION_SG_ID

# Python script for advanced security group management
import boto3

ec2 = boto3.client('ec2')

def create_tiered_security_groups(vpc_id):
    """Create a complete set of security groups for 3-tier architecture"""
    
    # Web Tier Security Group
    web_sg = ec2.create_security_group(
        GroupName='WebTier-SG',
        Description='Security group for web tier',
        VpcId=vpc_id
    )
    web_sg_id = web_sg['GroupId']
    
    # Application Tier Security Group
    app_sg = ec2.create_security_group(
        GroupName='AppTier-SG',
        Description='Security group for application tier',
        VpcId=vpc_id
    )
    app_sg_id = app_sg['GroupId']
    
    # Database Tier Security Group
    db_sg = ec2.create_security_group(
        GroupName='DBTier-SG',
        Description='Security group for database tier',
        VpcId=vpc_id
    )
    db_sg_id = db_sg['GroupId']
    
    # Web Tier Rules
    ec2.authorize_security_group_ingress(
        GroupId=web_sg_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': 80,
                'ToPort': 80,
                'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'HTTP from anywhere'}]
            },
            {
                'IpProtocol': 'tcp',
                'FromPort': 443,
                'ToPort': 443,
                'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'HTTPS from anywhere'}]
            }
        ]
    )
    
    # Application Tier Rules
    ec2.authorize_security_group_ingress(
        GroupId=app_sg_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': 8080,
                'ToPort': 8080,
                'UserIdGroupPairs': [{'GroupId': web_sg_id, 'Description': 'App port from web tier'}]
            },
            {
                'IpProtocol': 'tcp',
                'FromPort': 8443,
                'ToPort': 8443,
                'UserIdGroupPairs': [{'GroupId': web_sg_id, 'Description': 'Secure app port from web tier'}]
            }
        ]
    )
    
    # Database Tier Rules
    ec2.authorize_security_group_ingress(
        GroupId=db_sg_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': 3306,
                'ToPort': 3306,
                'UserIdGroupPairs': [{'GroupId': app_sg_id, 'Description': 'MySQL from app tier'}]
            },
            {
                'IpProtocol': 'tcp',
                'FromPort': 5432,
                'ToPort': 5432,
                'UserIdGroupPairs': [{'GroupId': app_sg_id, 'Description': 'PostgreSQL from app tier'}]
            }
        ]
    )
    
    return {
        'web_sg_id': web_sg_id,
        'app_sg_id': app_sg_id,
        'db_sg_id': db_sg_id
    }

# Usage
vpc_id = 'vpc-12345678'
security_groups = create_tiered_security_groups(vpc_id)
print(f"Created security groups: {security_groups}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Network ACL Configuration Examples

# Create custom Network ACL
aws ec2 create-network-acl \
    --vpc-id vpc-12345678 \
    --tag-specifications 'ResourceType=network-acl,Tags=[{Key=Name,Value=WebTier-NACL}]' \
    --region us-west-2

# Get the Network ACL ID
NACL_ID=$(aws ec2 describe-network-acls \
    --filters "Name=tag:Name,Values=WebTier-NACL" \
    --query 'NetworkAcls[0].NetworkAclId' \
    --output text)

# INBOUND RULES
# Rule 100: Allow HTTP from anywhere
aws ec2 create-network-acl-entry \
    --network-acl-id $NACL_ID \
    --rule-number 100 \
    --protocol tcp \
    --rule-action allow \
    --port-range From=80,To=80 \
    --cidr-block 0.0.0.0/0

# Rule 110: Allow HTTPS from anywhere
aws ec2 create-network-acl-entry \
    --network-acl-id $NACL_ID \
    --rule-number 110 \
    --protocol tcp \
    --rule-action allow \
    --port-range From=443,To=443 \
    --cidr-block 0.0.0.0/0

# Rule 120: Allow SSH from admin network
aws ec2 create-network-acl-entry \
    --network-acl-id $NACL_ID \
    --rule-number 120 \
    --protocol tcp \
    --rule-action allow \
    --port-range From=22,To=22 \
    --cidr-block 203.0.113.0/24

# Rule 130: Allow ephemeral ports for return traffic
aws ec2 create-network-acl-entry \
    --network-acl-id $NACL_ID \
    --rule-number 130 \
    --protocol tcp \
    --rule-action allow \
    --port-range From=1024,To=65535 \
    --cidr-block 0.0.0.0/0

# Rule 140: Deny all other traffic (explicit)
aws ec2 create-network-acl-entry \
    --network-acl-id $NACL_ID \
    --rule-number 140 \
    --protocol tcp \
    --rule-action deny \
    --port-range From=1,To=65535 \
    --cidr-block 0.0.0.0/0

# OUTBOUND RULES
# Rule 100: Allow HTTP outbound
aws ec2 create-network-acl-entry \
    --network-acl-id $NACL_ID \
    --rule-number 100 \
    --protocol tcp \
    --rule-action allow \
    --port-range From=80,To=80 \
    --cidr-block 0.0.0.0/0 \
    --egress

# Rule 110: Allow HTTPS outbound
aws ec2 create-network-acl-entry \
    --network-acl-id $NACL_ID \
    --rule-number 110 \
    --protocol tcp \
    --rule-action allow \
    --port-range From=443,To=443 \
    --cidr-block 0.0.0.0/0 \
    --egress

# Rule 120: Allow ephemeral ports outbound
aws ec2 create-network-acl-entry \
    --network-acl-id $NACL_ID \
    --rule-number 120 \
    --protocol tcp \
    --rule-action allow \
    --port-range From=1024,To=65535 \
    --cidr-block 0.0.0.0/0 \
    --egress

# Rule 130: Allow MySQL to database subnet
aws ec2 create-network-acl-entry \
    --network-acl-id $NACL_ID \
    --rule-number 130 \
    --protocol tcp \
    --rule-action allow \
    --port-range From=3306,To=3306 \
    --cidr-block 10.0.2.0/24 \
    --egress

# Associate Network ACL with subnet
aws ec2 associate-network-acl \
    --network-acl-id $NACL_ID \
    --subnet-id subnet-12345678

# Python script for advanced NACL management
import boto3

ec2 = boto3.client('ec2')

def create_restrictive_nacl(vpc_id, subnet_id):
    """Create a restrictive Network ACL for high-security subnet"""
    
    # Create Network ACL
    nacl_response = ec2.create_network_acl(
        VpcId=vpc_id,
        TagSpecifications=[
            {
                'ResourceType': 'network-acl',
                'Tags': [{'Key': 'Name', 'Value': 'HighSecurity-NACL'}]
            }
        ]
    )
    
    nacl_id = nacl_response['NetworkAcl']['NetworkAclId']
    
    # Inbound rules for high-security subnet
    inbound_rules = [
        {
            'RuleNumber': 100,
            'Protocol': '6',  # TCP
            'RuleAction': 'allow',
            'PortRange': {'From': 443, 'To': 443},
            'CidrBlock': '10.0.0.0/16'  # Only from VPC
        },
        {
            'RuleNumber': 110,
            'Protocol': '6',  # TCP
            'RuleAction': 'allow',
            'PortRange': {'From': 22, 'To': 22},
            'CidrBlock': '10.0.1.0/24'  # Only from bastion subnet
        },
        {
            'RuleNumber': 120,
            'Protocol': '6',  # TCP
            'RuleAction': 'allow',
            'PortRange': {'From': 1024, 'To': 65535},
            'CidrBlock': '0.0.0.0/0'  # Ephemeral ports
        },
        {
            'RuleNumber': 200,
            'Protocol': '6',  # TCP
            'RuleAction': 'deny',
            'PortRange': {'From': 1, 'To': 65535},
            'CidrBlock': '0.0.0.0/0'  # Deny all other
        }
    ]
    
    # Outbound rules
    outbound_rules = [
        {
            'RuleNumber': 100,
            'Protocol': '6',  # TCP
            'RuleAction': 'allow',
            'PortRange': {'From': 443, 'To': 443},
            'CidrBlock': '0.0.0.0/0'  # HTTPS outbound
        },
        {
            'RuleNumber': 110,
            'Protocol': '6',  # TCP
            'RuleAction': 'allow',
            'PortRange': {'From': 3306, 'To': 3306},
            'CidrBlock': '10.0.3.0/24'  # MySQL to DB subnet
        },
        {
            'RuleNumber': 120,
            'Protocol': '6',  # TCP
            'RuleAction': 'allow',
            'PortRange': {'From': 1024, 'To': 65535},
            'CidrBlock': '0.0.0.0/0'  # Ephemeral ports
        },
        {
            'RuleNumber': 200,
            'Protocol': '6',  # TCP
            'RuleAction': 'deny',
            'PortRange': {'From': 1, 'To': 65535},
            'CidrBlock': '0.0.0.0/0'  # Deny all other
        }
    ]
    
    # Create inbound rules
    for rule in inbound_rules:
        ec2.create_network_acl_entry(
            NetworkAclId=nacl_id,
            RuleNumber=rule['RuleNumber'],
            Protocol=rule['Protocol'],
            RuleAction=rule['RuleAction'],
            PortRange=rule['PortRange'],
            CidrBlock=rule['CidrBlock']
        )
    
    # Create outbound rules
    for rule in outbound_rules:
        ec2.create_network_acl_entry(
            NetworkAclId=nacl_id,
            RuleNumber=rule['RuleNumber'],
            Protocol=rule['Protocol'],
            RuleAction=rule['RuleAction'],
            PortRange=rule['PortRange'],
            CidrBlock=rule['CidrBlock'],
            Egress=True
        )
    
    # Associate with subnet
    ec2.associate_network_acl(
        NetworkAclId=nacl_id,
        SubnetId=subnet_id
    )
    
    return nacl_id

# Usage
vpc_id = 'vpc-12345678'
subnet_id = 'subnet-87654321'
nacl_id = create_restrictive_nacl(vpc_id, subnet_id)
print(f"Created restrictive NACL: {nacl_id}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Security Testing and Validation Scripts

# Test security group connectivity
#!/bin/bash

# Function to test port connectivity
test_port_connectivity() {
    local host=$1
    local port=$2
    local timeout=5
    
    echo "Testing connectivity to $host:$port..."
    
    if timeout $timeout bash -c "cat < /dev/null > /dev/tcp/$host/$port"; then
        echo "✅ Port $port is OPEN on $host"
        return 0
    else
        echo "❌ Port $port is CLOSED on $host"
        return 1
    fi
}

# Test web server connectivity
echo "🔍 Testing Web Server Security Group..."
test_port_connectivity "10.0.1.10" 80    # HTTP
test_port_connectivity "10.0.1.10" 443   # HTTPS
test_port_connectivity "10.0.1.10" 22    # SSH (should fail from external)
test_port_connectivity "10.0.1.10" 3306  # MySQL (should fail)

# Test database server connectivity
echo "🔍 Testing Database Server Security Group..."
test_port_connectivity "10.0.2.10" 3306  # MySQL (should fail from web tier)
test_port_connectivity "10.0.2.10" 22    # SSH (should fail from external)

# Python script for comprehensive security testing
import boto3
import socket
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

def test_security_group_rules(ec2_client, security_group_id):
    """Analyze security group rules for potential issues"""
    
    try:
        response = ec2_client.describe_security_groups(GroupIds=[security_group_id])
        sg = response['SecurityGroups'][0]
        
        print(f"🔍 Analyzing Security Group: {sg['GroupName']}")
        print(f"Description: {sg['Description']}")
        print(f"VPC ID: {sg['VpcId']}")
        
        # Check inbound rules
        print("\n📥 Inbound Rules Analysis:")
        for rule in sg['IpPermissions']:
            analyze_security_rule(rule, 'Inbound')
        
        # Check outbound rules
        print("\n📤 Outbound Rules Analysis:")
        for rule in sg['IpPermissionsEgress']:
            analyze_security_rule(rule, 'Outbound')
            
    except Exception as e:
        print(f"❌ Error analyzing security group: {e}")

def analyze_security_rule(rule, direction):
    """Analyze individual security group rule"""
    
    protocol = rule.get('IpProtocol', '')
    from_port = rule.get('FromPort', '')
    to_port = rule.get('ToPort', '')
    
    # Check for overly permissive rules
    for ip_range in rule.get('IpRanges', []):
        cidr = ip_range.get('CidrIp', '')
        if cidr == '0.0.0.0/0':
            if protocol == 'tcp' and from_port in [22, 3389]:
                print(f"⚠️  WARNING: {direction} rule allows {protocol}:{from_port} from anywhere")
            elif protocol == '-1':
                print(f"🚨 CRITICAL: {direction} rule allows ALL traffic from anywhere")
            else:
                print(f"ℹ️  {direction}: {protocol}:{from_port}-{to_port} from {cidr}")
        else:
            print(f"✅ {direction}: {protocol}:{from_port}-{to_port} from {cidr}")

def test_nacl_rules(ec2_client, nacl_id):
    """Test Network ACL rules for proper configuration"""
    
    try:
        response = ec2_client.describe_network_acls(NetworkAclIds=[nacl_id])
        nacl = response['NetworkAcls'][0]
        
        print(f"🔍 Analyzing Network ACL: {nacl_id}")
        
        # Analyze inbound rules
        print("\n📥 Inbound NACL Rules:")
        inbound_rules = [rule for rule in nacl['Entries'] if not rule['Egress']]
        inbound_rules.sort(key=lambda x: x['RuleNumber'])
        
        for rule in inbound_rules:
            analyze_nacl_rule(rule, 'Inbound')
        
        # Analyze outbound rules
        print("\n📤 Outbound NACL Rules:")
        outbound_rules = [rule for rule in nacl['Entries'] if rule['Egress']]
        outbound_rules.sort(key=lambda x: x['RuleNumber'])
        
        for rule in outbound_rules:
            analyze_nacl_rule(rule, 'Outbound')
            
    except Exception as e:
        print(f"❌ Error analyzing Network ACL: {e}")

def analyze_nacl_rule(rule, direction):
    """Analyze individual NACL rule"""
    
    rule_number = rule['RuleNumber']
    protocol = rule['Protocol']
    action = rule['RuleAction']
    cidr = rule['CidrBlock']
    
    port_range = ""
    if 'PortRange' in rule:
        port_range = f":{rule['PortRange']['From']}-{rule['PortRange']['To']}"
    
    if action == 'deny':
        print(f"🚫 Rule {rule_number}: {direction} DENY {protocol}{port_range} from {cidr}")
    else:
        print(f"✅ Rule {rule_number}: {direction} ALLOW {protocol}{port_range} from {cidr}")

def port_scan_test(target_ip, ports):
    """Perform port scan to test actual connectivity"""
    
    print(f"\n🔍 Port scanning {target_ip}...")
    
    def scan_port(port):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((target_ip, port))
            sock.close()
            return port, result == 0
        except Exception:
            return port, False
    
    with ThreadPoolExecutor(max_workers=50) as executor:
        future_to_port = {executor.submit(scan_port, port): port for port in ports}
        
        for future in as_completed(future_to_port):
            port, is_open = future.result()
            if is_open:
                print(f"✅ Port {port} is OPEN")
            else:
                print(f"❌ Port {port} is CLOSED")

def security_compliance_check(ec2_client, vpc_id):
    """Check VPC security compliance"""
    
    print(f"🔍 Security Compliance Check for VPC: {vpc_id}")
    
    # Check for default security groups
    response = ec2_client.describe_security_groups(
        Filters=[
            {'Name': 'vpc-id', 'Values': [vpc_id]},
            {'Name': 'group-name', 'Values': ['default']}
        ]
    )
    
    for sg in response['SecurityGroups']:
        print(f"\n⚠️  Found default security group: {sg['GroupId']}")
        
        # Check if default SG has rules
        if sg['IpPermissions'] or sg['IpPermissionsEgress']:
            print("🚨 WARNING: Default security group has rules - consider removing")
        else:
            print("✅ Default security group has no rules")
    
    # Check for overly permissive rules
    all_sgs = ec2_client.describe_security_groups(
        Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
    )
    
    for sg in all_sgs['SecurityGroups']:
        for rule in sg['IpPermissions']:
            for ip_range in rule.get('IpRanges', []):
                if ip_range.get('CidrIp') == '0.0.0.0/0':
                    if rule.get('FromPort') == 22:
                        print(f"🚨 CRITICAL: SSH open to world in {sg['GroupId']}")
                    elif rule.get('FromPort') == 3389:
                        print(f"🚨 CRITICAL: RDP open to world in {sg['GroupId']}")

# Main execution
if __name__ == "__main__":
    ec2 = boto3.client('ec2')
    
    # Test specific security group
    sg_id = 'sg-12345678'
    test_security_group_rules(ec2, sg_id)
    
    # Test specific NACL
    nacl_id = 'acl-87654321'
    test_nacl_rules(ec2, nacl_id)
    
    # Port scan test
    target_ip = '10.0.1.10'
    common_ports = [22, 23, 25, 53, 80, 110, 143, 443, 993, 995, 3306, 5432]
    port_scan_test(target_ip, common_ports)
    
    # Security compliance check
    vpc_id = 'vpc-12345678'
    security_compliance_check(ec2, vpc_id)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def vpc_endpoints_tab():
    """VPC Endpoints tab content"""
    st.markdown("## 🔗 Connect Privately to public AWS Services")
    st.markdown("*VPC Endpoints – Interface and Gateway*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What are VPC Endpoints?
    VPC Endpoints enable private connectivity between your VPC and AWS services:
    - **No Internet Gateway Required**: Direct connection to AWS services
    - **Traffic Stays in AWS Network**: Enhanced security and performance
    - **Two Types**: Gateway endpoints and Interface endpoints
    - **Cost Optimization**: Reduce NAT Gateway costs and data transfer charges
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # VPC Endpoints architecture
    st.markdown("#### 🏗️ VPC Endpoints Architecture")
    common.mermaid(create_vpc_endpoints_diagram(), height=700)
    
    # Endpoint types comparison
    st.markdown("#### ⚖️ Gateway vs Interface Endpoints")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚪 Gateway Endpoints
        **Supported Services**: S3, DynamoDB  
        **Connection Method**: Route table entries  
        **Cost**: No additional charges  
        **DNS**: Uses service public DNS names  
        **Availability**: Regional resource
        
        **How it works**:
        - Route table entry directs traffic to endpoint
        - Prefix list identifies service IP ranges
        - No additional ENI in subnet
        - Policies control access
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔌 Interface Endpoints
        **Supported Services**: 100+ AWS services  
        **Connection Method**: Elastic Network Interface (ENI)  
        **Cost**: $0.01 per hour per endpoint  
        **DNS**: Private DNS names or public DNS  
        **Availability**: AZ-specific
        
        **How it works**:
        - ENI with private IP in your subnet
        - DNS resolution to private IP
        - Security groups control access
        - Powered by AWS PrivateLink
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive endpoint configurator
    st.markdown("#### 🎮 Interactive Endpoint Configurator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Endpoint Configuration")
        endpoint_type = st.selectbox("Endpoint Type", ["Gateway", "Interface"])
        
        if endpoint_type == "Gateway":
            service = st.selectbox("Service", ["S3", "DynamoDB"])
            endpoint_cost = "$0.00"
            dns_type = "Public DNS names"
        else:
            service = st.selectbox("Service", [
                "EC2", "S3", "DynamoDB", "SQS", "SNS", "Lambda", "ECS", "ECR",
                "CloudWatch", "CloudTrail", "Systems Manager", "Secrets Manager"
            ])
            endpoint_cost = "$0.01/hour"
            dns_type = "Private DNS names"
        
        vpc_id = st.selectbox("VPC", ["vpc-12345678 (10.0.0.0/16)", "vpc-87654321 (172.16.0.0/16)"])
        
        if endpoint_type == "Interface":
            num_azs = st.slider("Number of AZs", 1, 3, 2)
            enable_private_dns = st.checkbox("Enable Private DNS", value=True)
    
    with col2:
        st.markdown("##### Policy Configuration")
        policy_type = st.selectbox("Access Policy", ["Full Access", "Restricted Access", "Custom"])
        
        if policy_type == "Restricted Access":
            allowed_actions = st.multiselect("Allowed Actions", [
                "s3:GetObject", "s3:PutObject", "s3:ListBucket",
                "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:Query",
                "sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage"
            ])
            
            resource_restriction = st.selectbox("Resource Restriction", [
                "All Resources", "Specific Buckets/Tables", "By Resource Tags"
            ])
    
    # Display endpoint configuration
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Endpoint Configuration Summary
    **Endpoint Type**: {endpoint_type}  
    **Service**: {service}  
    **Cost**: {endpoint_cost}  
    **DNS Resolution**: {dns_type}  
    **Policy**: {policy_type}  
    **Estimated Monthly Cost**: {calculate_endpoint_cost(endpoint_type, service, num_azs if endpoint_type == "Interface" else 1)}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Supported services
    st.markdown("#### 📋 Supported AWS Services")
    
    # Create services dataframe
    services_data = create_vpc_endpoints_services_data()
    
    # Filter by endpoint type
    if endpoint_type == "Gateway":
        filtered_services = services_data[services_data['Endpoint Type'] == 'Gateway']
    else:
        filtered_services = services_data[services_data['Endpoint Type'] == 'Interface']
    
    st.dataframe(filtered_services, use_container_width=True)
    
    # Benefits section
    st.markdown("#### 🎯 VPC Endpoints Benefits")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔒 Enhanced Security
        - Traffic never leaves AWS network
        - No internet gateway required
        - Private IP addresses only
        - Fine-grained access control
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚀 Improved Performance
        - Reduced latency
        - Higher bandwidth
        - No NAT gateway bottleneck
        - Direct service connectivity
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 💰 Cost Optimization
        - No NAT gateway charges
        - Reduced data transfer costs
        - No internet gateway traffic
        - Predictable pricing
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 VPC Endpoints Implementation")
    
    tab1, tab2, tab3 = st.tabs(["Gateway Endpoints", "Interface Endpoints", "Endpoint Policies"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create Gateway Endpoints for S3 and DynamoDB

# S3 Gateway Endpoint
aws ec2 create-vpc-endpoint \
    --vpc-id vpc-12345678 \
    --service-name com.amazonaws.us-west-2.s3 \
    --route-table-ids rtb-12345678 rtb-87654321 \
    --policy-document file://s3-endpoint-policy.json

# DynamoDB Gateway Endpoint
aws ec2 create-vpc-endpoint \
    --vpc-id vpc-12345678 \
    --service-name com.amazonaws.us-west-2.dynamodb \
    --route-table-ids rtb-12345678 rtb-87654321 \
    --policy-document file://dynamodb-endpoint-policy.json

# S3 Gateway Endpoint Policy (s3-endpoint-policy.json)
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::my-company-bucket",
                "arn:aws:s3:::my-company-bucket/*"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:PrincipalVpc": "vpc-12345678"
                }
            }
        }
    ]
}

# DynamoDB Gateway Endpoint Policy (dynamodb-endpoint-policy.json)
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:UpdateItem",
                "dynamodb:DeleteItem"
            ],
            "Resource": [
                "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable",
                "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable/*"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:PrincipalVpc": "vpc-12345678"
                }
            }
        }
    ]
}

# Python script to create Gateway Endpoints
import boto3
import json

def create_gateway_endpoints(vpc_id, route_table_ids):
    """Create S3 and DynamoDB Gateway Endpoints"""
    
    ec2 = boto3.client('ec2')
    
    # S3 Gateway Endpoint
    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::*"
                ],
                "Condition": {
                    "StringEquals": {
                        "aws:PrincipalVpc": vpc_id
                    }
                }
            }
        ]
    }
    
    s3_endpoint = ec2.create_vpc_endpoint(
        VpcId=vpc_id,
        ServiceName='com.amazonaws.us-west-2.s3',
        RouteTableIds=route_table_ids,
        PolicyDocument=json.dumps(s3_policy)
    )
    
    # DynamoDB Gateway Endpoint
    dynamodb_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": [
                    "dynamodb:*"
                ],
                "Resource": "*",
                "Condition": {
                    "StringEquals": {
                        "aws:PrincipalVpc": vpc_id
                    }
                }
            }
        ]
    }
    
    dynamodb_endpoint = ec2.create_vpc_endpoint(
        VpcId=vpc_id,
        ServiceName='com.amazonaws.us-west-2.dynamodb',
        RouteTableIds=route_table_ids,
        PolicyDocument=json.dumps(dynamodb_policy)
    )
    
    return {
        's3_endpoint_id': s3_endpoint['VpcEndpoint']['VpcEndpointId'],
        'dynamodb_endpoint_id': dynamodb_endpoint['VpcEndpoint']['VpcEndpointId']
    }

# Verify endpoint creation
def verify_gateway_endpoints(vpc_id):
    """Verify Gateway Endpoints are working"""
    
    ec2 = boto3.client('ec2')
    
    # List VPC endpoints
    response = ec2.describe_vpc_endpoints(
        Filters=[
            {'Name': 'vpc-id', 'Values': [vpc_id]},
            {'Name': 'vpc-endpoint-type', 'Values': ['Gateway']}
        ]
    )
    
    for endpoint in response['VpcEndpoints']:
        print(f"Endpoint ID: {endpoint['VpcEndpointId']}")
        print(f"Service: {endpoint['ServiceName']}")
        print(f"State: {endpoint['State']}")
        print(f"Route Tables: {endpoint['RouteTableIds']}")
        print("---")

# Usage example
vpc_id = 'vpc-12345678'
route_table_ids = ['rtb-12345678', 'rtb-87654321']

endpoints = create_gateway_endpoints(vpc_id, route_table_ids)
print(f"Created endpoints: {endpoints}")

verify_gateway_endpoints(vpc_id)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create Interface Endpoints for various AWS services

# SQS Interface Endpoint
aws ec2 create-vpc-endpoint \
    --vpc-id vpc-12345678 \
    --service-name com.amazonaws.us-west-2.sqs \
    --vpc-endpoint-type Interface \
    --subnet-ids subnet-12345678 subnet-87654321 \
    --security-group-ids sg-12345678 \
    --private-dns-enabled \
    --policy-document file://sqs-endpoint-policy.json

# SNS Interface Endpoint
aws ec2 create-vpc-endpoint \
    --vpc-id vpc-12345678 \
    --service-name com.amazonaws.us-west-2.sns \
    --vpc-endpoint-type Interface \
    --subnet-ids subnet-12345678 subnet-87654321 \
    --security-group-ids sg-12345678 \
    --private-dns-enabled

# Lambda Interface Endpoint
aws ec2 create-vpc-endpoint \
    --vpc-id vpc-12345678 \
    --service-name com.amazonaws.us-west-2.lambda \
    --vpc-endpoint-type Interface \
    --subnet-ids subnet-12345678 subnet-87654321 \
    --security-group-ids sg-12345678 \
    --private-dns-enabled

# Create security group for VPC endpoints
aws ec2 create-security-group \
    --group-name VPCEndpoints-SG \
    --description "Security group for VPC endpoints" \
    --vpc-id vpc-12345678

# Get security group ID
VPC_ENDPOINT_SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=VPCEndpoints-SG" \
    --query 'SecurityGroups[0].GroupId' \
    --output text)

# Allow HTTPS traffic to VPC endpoints
aws ec2 authorize-security-group-ingress \
    --group-id $VPC_ENDPOINT_SG_ID \
    --protocol tcp \
    --port 443 \
    --source-group $VPC_ENDPOINT_SG_ID

# Allow traffic from application security groups
aws ec2 authorize-security-group-ingress \
    --group-id $VPC_ENDPOINT_SG_ID \
    --protocol tcp \
    --port 443 \
    --source-group sg-app-servers

# Python script for comprehensive Interface Endpoints setup
import boto3
import json

def create_interface_endpoints(vpc_id, subnet_ids, security_group_ids):
    """Create Interface Endpoints for common AWS services"""
    
    ec2 = boto3.client('ec2')
    
    # List of services to create endpoints for
    services = [
        'com.amazonaws.us-west-2.sqs',
        'com.amazonaws.us-west-2.sns',
        'com.amazonaws.us-west-2.lambda',
        'com.amazonaws.us-west-2.logs',
        'com.amazonaws.us-west-2.monitoring',
        'com.amazonaws.us-west-2.ssm',
        'com.amazonaws.us-west-2.secretsmanager',
        'com.amazonaws.us-west-2.kms'
    ]
    
    created_endpoints = []
    
    for service in services:
        try:
            # Create restrictive policy for each service
            policy = create_service_policy(service, vpc_id)
            
            endpoint = ec2.create_vpc_endpoint(
                VpcId=vpc_id,
                ServiceName=service,
                VpcEndpointType='Interface',
                SubnetIds=subnet_ids,
                SecurityGroupIds=security_group_ids,
                PrivateDnsEnabled=True,
                PolicyDocument=json.dumps(policy)
            )
            
            created_endpoints.append({
                'service': service,
                'endpoint_id': endpoint['VpcEndpoint']['VpcEndpointId']
            })
            
            print(f"✅ Created endpoint for {service}")
            
        except Exception as e:
            print(f"❌ Failed to create endpoint for {service}: {e}")
    
    return created_endpoints

def create_service_policy(service_name, vpc_id):
    """Create restrictive policy for VPC endpoint"""
    
    service_short = service_name.split('.')[-1]
    
    # Common policy template
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": f"{service_short}:*",
                "Resource": "*",
                "Condition": {
                    "StringEquals": {
                        "aws:PrincipalVpc": vpc_id
                    }
                }
            }
        ]
    }
    
    # Service-specific policies
    if service_short == 'sqs':
        policy["Statement"][0]["Action"] = [
            "sqs:SendMessage",
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes"
        ]
    elif service_short == 'sns':
        policy["Statement"][0]["Action"] = [
            "sns:Publish",
            "sns:Subscribe",
            "sns:Unsubscribe"
        ]
    elif service_short == 'lambda':
        policy["Statement"][0]["Action"] = [
            "lambda:InvokeFunction",
            "lambda:GetFunction"
        ]
    elif service_short == 'logs':
        policy["Statement"][0]["Action"] = [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents"
        ]
    
    return policy

def create_endpoint_security_group(vpc_id):
    """Create security group for VPC endpoints"""
    
    ec2 = boto3.client('ec2')
    
    # Create security group
    sg_response = ec2.create_security_group(
        GroupName='VPCEndpoints-SG',
        Description='Security group for VPC endpoints',
        VpcId=vpc_id
    )
    
    sg_id = sg_response['GroupId']
    
    # Allow HTTPS traffic from VPC CIDR
    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': 443,
                'ToPort': 443,
                'IpRanges': [{'CidrIp': '10.0.0.0/16', 'Description': 'HTTPS from VPC'}]
            }
        ]
    )
    
    return sg_id

def monitor_endpoint_health(vpc_id):
    """Monitor VPC endpoint health and usage"""
    
    ec2 = boto3.client('ec2')
    cloudwatch = boto3.client('cloudwatch')
    
    # Get all VPC endpoints
    response = ec2.describe_vpc_endpoints(
        Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
    )
    
    for endpoint in response['VpcEndpoints']:
        endpoint_id = endpoint['VpcEndpointId']
        service_name = endpoint['ServiceName']
        state = endpoint['State']
        
        print(f"Endpoint: {endpoint_id}")
        print(f"Service: {service_name}")
        print(f"State: {state}")
        
        # Check CloudWatch metrics
        try:
            metrics = cloudwatch.get_metric_statistics(
                Namespace='AWS/VPC',
                MetricName='PacketDropCount',
                Dimensions=[
                    {'Name': 'VpcEndpointId', 'Value': endpoint_id}
                ],
                StartTime=datetime.utcnow() - timedelta(hours=1),
                EndTime=datetime.utcnow(),
                Period=300,
                Statistics=['Sum']
            )
            
            if metrics['Datapoints']:
                print(f"Packet Drops: {metrics['Datapoints'][-1]['Sum']}")
            
        except Exception as e:
            print(f"Error getting metrics: {e}")
        
        print("---")

# Usage example
vpc_id = 'vpc-12345678'
subnet_ids = ['subnet-12345678', 'subnet-87654321']

# Create security group for endpoints
sg_id = create_endpoint_security_group(vpc_id)
print(f"Created security group: {sg_id}")

# Create interface endpoints
endpoints = create_interface_endpoints(vpc_id, subnet_ids, [sg_id])
print(f"Created {len(endpoints)} endpoints")

# Monitor endpoint health
monitor_endpoint_health(vpc_id)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# VPC Endpoint Policies - Advanced Examples

# Restrictive S3 Gateway Endpoint Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::my-company-bucket/*"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:PrincipalVpc": "vpc-12345678"
                },
                "StringLike": {
                    "aws:PrincipalArn": "arn:aws:iam::123456789012:role/MyApp-*"
                }
            }
        },
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::my-company-bucket"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:PrincipalVpc": "vpc-12345678"
                }
            }
        },
        {
            "Effect": "Deny",
            "Principal": "*",
            "Action": [
                "s3:DeleteObject",
                "s3:DeleteBucket"
            ],
            "Resource": [
                "arn:aws:s3:::my-company-bucket",
                "arn:aws:s3:::my-company-bucket/*"
            ]
        }
    ]
}

# Time-based SQS Interface Endpoint Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "sqs:SendMessage",
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "aws:PrincipalVpc": "vpc-12345678"
                },
                "DateGreaterThan": {
                    "aws:CurrentTime": "2024-01-01T00:00:00Z"
                },
                "DateLessThan": {
                    "aws:CurrentTime": "2025-12-31T23:59:59Z"
                },
                "ForAllValues:StringEquals": {
                    "aws:RequestedRegion": "us-west-2"
                }
            }
        }
    ]
}

# IP-based Lambda Interface Endpoint Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "aws:PrincipalVpc": "vpc-12345678"
                },
                "IpAddress": {
                    "aws:SourceIp": [
                        "10.0.1.0/24",
                        "10.0.2.0/24"
                    ]
                }
            }
        },
        {
            "Effect": "Deny",
            "Principal": "*",
            "Action": "*",
            "Resource": "*",
            "Condition": {
                "StringNotEquals": {
                    "aws:PrincipalVpc": "vpc-12345678"
                }
            }
        }
    ]
}

# Python script for dynamic policy generation
import boto3
import json
from datetime import datetime, timedelta

def generate_endpoint_policy(service_type, config):
    """Generate VPC endpoint policy based on service and configuration"""
    
    base_policy = {
        "Version": "2012-10-17",
        "Statement": []
    }
    
    if service_type == 's3':
        base_policy["Statement"] = generate_s3_policy(config)
    elif service_type == 'sqs':
        base_policy["Statement"] = generate_sqs_policy(config)
    elif service_type == 'lambda':
        base_policy["Statement"] = generate_lambda_policy(config)
    elif service_type == 'logs':
        base_policy["Statement"] = generate_logs_policy(config)
    
    return base_policy

def generate_s3_policy(config):
    """Generate S3-specific endpoint policy"""
    
    statements = []
    
    # Allow specific actions on specific buckets
    if config.get('allowed_buckets'):
        statements.append({
            "Effect": "Allow",
            "Principal": "*",
            "Action": config.get('allowed_actions', ['s3:GetObject', 's3:PutObject']),
            "Resource": [
                f"arn:aws:s3:::{bucket}/*" for bucket in config['allowed_buckets']
            ],
            "Condition": {
                "StringEquals": {
                    "aws:PrincipalVpc": config['vpc_id']
                }
            }
        })
        
        # Allow ListBucket on the buckets
        statements.append({
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:ListBucket",
            "Resource": [
                f"arn:aws:s3:::{bucket}" for bucket in config['allowed_buckets']
            ],
            "Condition": {
                "StringEquals": {
                    "aws:PrincipalVpc": config['vpc_id']
                }
            }
        })
    
    # Deny dangerous actions
    if config.get('deny_destructive', True):
        statements.append({
            "Effect": "Deny",
            "Principal": "*",
            "Action": [
                "s3:DeleteObject",
                "s3:DeleteBucket",
                "s3:PutBucketPolicy",
                "s3:DeleteBucketPolicy"
            ],
            "Resource": "*"
        })
    
    return statements

def generate_sqs_policy(config):
    """Generate SQS-specific endpoint policy"""
    
    statements = []
    
    # Allow SQS operations
    statements.append({
        "Effect": "Allow",
        "Principal": "*",
        "Action": [
            "sqs:SendMessage",
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes"
        ],
        "Resource": "*",
        "Condition": {
            "StringEquals": {
                "aws:PrincipalVpc": config['vpc_id']
            }
        }
    })
    
    # Add time-based restrictions if configured
    if config.get('time_restrictions'):
        statements[0]["Condition"]["DateGreaterThan"] = {
            "aws:CurrentTime": config['time_restrictions']['start']
        }
        statements[0]["Condition"]["DateLessThan"] = {
            "aws:CurrentTime": config['time_restrictions']['end']
        }
    
    # Add IP-based restrictions if configured
    if config.get('allowed_subnets'):
        statements[0]["Condition"]["IpAddress"] = {
            "aws:SourceIp": config['allowed_subnets']
        }
    
    return statements

def generate_lambda_policy(config):
    """Generate Lambda-specific endpoint policy"""
    
    statements = []
    
    # Allow Lambda invocation
    statements.append({
        "Effect": "Allow",
        "Principal": "*",
        "Action": [
            "lambda:InvokeFunction"
        ],
        "Resource": "*",
        "Condition": {
            "StringEquals": {
                "aws:PrincipalVpc": config['vpc_id']
            }
        }
    })
    
    # Restrict to specific functions if configured
    if config.get('allowed_functions'):
        statements[0]["Resource"] = [
            f"arn:aws:lambda:{config['region']}:{config['account_id']}:function:{func}"
            for func in config['allowed_functions']
        ]
    
    return statements

def generate_logs_policy(config):
    """Generate CloudWatch Logs-specific endpoint policy"""
    
    statements = []
    
    # Allow logs operations
    statements.append({
        "Effect": "Allow",
        "Principal": "*",
        "Action": [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:DescribeLogGroups",
            "logs:DescribeLogStreams"
        ],
        "Resource": "*",
        "Condition": {
            "StringEquals": {
                "aws:PrincipalVpc": config['vpc_id']
            }
        }
    })
    
    # Restrict to specific log groups if configured
    if config.get('allowed_log_groups'):
        statements[0]["Resource"] = [
            f"arn:aws:logs:{config['region']}:{config['account_id']}:log-group:{group}:*"
            for group in config['allowed_log_groups']
        ]
    
    return statements

def apply_endpoint_policy(endpoint_id, policy):
    """Apply policy to existing VPC endpoint"""
    
    ec2 = boto3.client('ec2')
    
    try:
        ec2.modify_vpc_endpoint(
            VpcEndpointId=endpoint_id,
            PolicyDocument=json.dumps(policy)
        )
        print(f"✅ Applied policy to endpoint {endpoint_id}")
        return True
    except Exception as e:
        print(f"❌ Failed to apply policy: {e}")
        return False

def validate_endpoint_policy(policy):
    """Validate VPC endpoint policy"""
    
    validation_errors = []
    
    # Check for required fields
    if 'Version' not in policy:
        validation_errors.append("Missing 'Version' field")
    
    if 'Statement' not in policy:
        validation_errors.append("Missing 'Statement' field")
    
    # Check statements
    for i, statement in enumerate(policy.get('Statement', [])):
        if 'Effect' not in statement:
            validation_errors.append(f"Statement {i}: Missing 'Effect' field")
        
        if 'Principal' not in statement:
            validation_errors.append(f"Statement {i}: Missing 'Principal' field")
        
        if 'Action' not in statement:
            validation_errors.append(f"Statement {i}: Missing 'Action' field")
        
        # Check for VPC condition
        if 'Condition' in statement:
            conditions = statement['Condition']
            if 'StringEquals' in conditions:
                if 'aws:PrincipalVpc' not in conditions['StringEquals']:
                    validation_errors.append(f"Statement {i}: Missing VPC condition")
        else:
            validation_errors.append(f"Statement {i}: Missing VPC condition")
    
    return validation_errors

# Usage examples
config_s3 = {
    'vpc_id': 'vpc-12345678',
    'allowed_buckets': ['my-app-bucket', 'my-logs-bucket'],
    'allowed_actions': ['s3:GetObject', 's3:PutObject'],
    'deny_destructive': True
}

config_sqs = {
    'vpc_id': 'vpc-12345678',
    'time_restrictions': {
        'start': '2024-01-01T00:00:00Z',
        'end': '2025-12-31T23:59:59Z'
    },
    'allowed_subnets': ['10.0.1.0/24', '10.0.2.0/24']
}

# Generate policies
s3_policy = generate_endpoint_policy('s3', config_s3)
sqs_policy = generate_endpoint_policy('sqs', config_sqs)

# Validate policies
s3_errors = validate_endpoint_policy(s3_policy)
sqs_errors = validate_endpoint_policy(sqs_policy)

if not s3_errors:
    print("✅ S3 policy is valid")
    print(json.dumps(s3_policy, indent=2))
else:
    print(f"❌ S3 policy errors: {s3_errors}")

if not sqs_errors:
    print("✅ SQS policy is valid")
    print(json.dumps(sqs_policy, indent=2))
else:
    print(f"❌ SQS policy errors: {sqs_errors}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def vpc_peering_tab():
    """VPC Peering tab content"""
    st.markdown("## 🌐 How to connect directly to other VPCs?")
    st.markdown("*VPC Peering - Scalable and highly available*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What is VPC Peering?
    VPC Peering enables direct network connectivity between VPCs:
    - **Direct Connection**: Private connectivity between VPCs
    - **Cross-Account**: Connect VPCs across different AWS accounts
    - **Cross-Region**: Connect VPCs in different AWS regions
    - **Bi-directional**: Traffic flows in both directions
    - **No Transitive Routing**: Full mesh required for multi-VPC connectivity
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # VPC Peering architecture
    st.markdown("#### 🏗️ VPC Peering Architecture")
    common.mermaid(create_vpc_peering_diagram(), height=800)
    
    # Peering characteristics
    st.markdown("#### ⚖️ VPC Peering Characteristics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="network-component">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Supported Features
        - **Scalable**: Handles high bandwidth
        - **Highly Available**: Built-in redundancy
        - **Cross-Account**: Inter-account connectivity
        - **Cross-Region**: Different AWS regions
        - **Bi-directional**: Traffic in both directions
        - **Security Group References**: Reference remote SGs
        - **Route Table Control**: Selective routing
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="network-component">', unsafe_allow_html=True)
        st.markdown("""
        ### ❌ Limitations
        - **No Transitive Routing**: No routing through peers
        - **Full Mesh Required**: For multi-VPC connectivity
        - **No Overlapping IP**: CIDR blocks cannot overlap
        - **DNS Resolution**: Must be configured
        - **Connection Limits**: Max 125 peering connections
        - **Bandwidth Limits**: No additional bandwidth provision
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive peering configurator
    st.markdown("#### 🎮 Interactive Peering Configurator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Source VPC Configuration")
        source_vpc_name = st.text_input("Source VPC Name", value="Production-VPC")
        source_vpc_cidr = st.selectbox("Source VPC CIDR", ["10.0.0.0/16", "172.16.0.0/16", "192.168.0.0/16"])
        source_account = st.text_input("Source Account ID", value="123456789012")
        source_region = st.selectbox("Source Region", ["us-west-2", "us-east-1", "eu-west-1"])
        
    with col2:
        st.markdown("##### Target VPC Configuration")
        target_vpc_name = st.text_input("Target VPC Name", value="Development-VPC")
        target_vpc_cidr = st.selectbox("Target VPC CIDR", ["10.1.0.0/16", "172.17.0.0/16", "192.169.0.0/16"])
        target_account = st.text_input("Target Account ID", value="123456789012")
        target_region = st.selectbox("Target Region", ["us-west-2", "us-east-1", "eu-west-1"])
    
    # Peering configuration
    st.markdown("##### Peering Configuration")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        dns_resolution = st.checkbox("Enable DNS Resolution", value=True)
        dns_hostnames = st.checkbox("Enable DNS Hostnames", value=True)
        
    with col2:
        cross_account = source_account != target_account
        cross_region = source_region != target_region
        st.write(f"Cross-Account: {'Yes' if cross_account else 'No'}")
        st.write(f"Cross-Region: {'Yes' if cross_region else 'No'}")
        
    with col3:
        overlapping_cidrs = check_cidr_overlap(source_vpc_cidr, target_vpc_cidr)
        st.write(f"CIDR Overlap: {'Yes ❌' if overlapping_cidrs else 'No ✅'}")
        estimated_cost = calculate_peering_cost(cross_region, target_region)
        st.write(f"Estimated Cost: {estimated_cost}")
    
    # Display peering summary
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Peering Connection Summary
    **Connection Type**: {'Cross-Region' if cross_region else 'Same Region'} {'Cross-Account' if cross_account else 'Same Account'}  
    **Source**: {source_vpc_name} ({source_vpc_cidr}) in {source_region}  
    **Target**: {target_vpc_name} ({target_vpc_cidr}) in {target_region}  
    **Status**: {'❌ Cannot create - CIDR overlap' if overlapping_cidrs else '✅ Ready to create'}  
    **DNS Features**: {'Enabled' if dns_resolution and dns_hostnames else 'Disabled'}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    if not overlapping_cidrs:
        # Route planning
        st.markdown("#### 🛣️ Route Planning")
        
        route_data = generate_peering_routes(source_vpc_cidr, target_vpc_cidr)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Routes for Source VPC**")
            source_routes = pd.DataFrame(route_data['source_routes'])
            st.dataframe(source_routes, use_container_width=True)
        
        with col2:
            st.markdown("**Routes for Target VPC**")
            target_routes = pd.DataFrame(route_data['target_routes'])
            st.dataframe(target_routes, use_container_width=True)
    
    # Peering alternatives
    st.markdown("#### 🔄 VPC Connectivity Alternatives")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="network-component">', unsafe_allow_html=True)
        st.markdown("""
        ### 🌐 Transit Gateway
        **Best For**: Hub-and-spoke connectivity
        
        **Features**:
        - Central hub for VPC connections
        - Transitive routing
        - Scalable to 5,000 VPCs
        - Cross-region peering
        - Route tables and policies
        
        **Cost**: $0.05/hour + $0.02/GB
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="network-component">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔗 Site-to-Site VPN
        **Best For**: On-premises connectivity
        
        **Features**:
        - Encrypted tunnel
        - Customer Gateway required
        - Virtual Private Gateway
        - BGP routing support
        - Redundant connections
        
        **Cost**: $0.05/hour + data transfer
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="network-component">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚡ Direct Connect
        **Best For**: High bandwidth, low latency
        
        **Features**:
        - Dedicated network connection
        - Consistent performance
        - Virtual interfaces (VIFs)
        - Multiple VPC access
        - Hybrid connectivity
        
        **Cost**: Port charges + data transfer
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 VPC Peering Implementation")
    
    tab1, tab2, tab3 = st.tabs(["Create Peering", "Configure Routes", "Monitor & Troubleshoot"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create VPC Peering Connections

# Same Region, Same Account Peering
aws ec2 create-vpc-peering-connection \
    --vpc-id vpc-12345678 \
    --peer-vpc-id vpc-87654321 \
    --tag-specifications 'ResourceType=vpc-peering-connection,Tags=[{Key=Name,Value=Prod-Dev-Peering}]'

# Cross-Region Peering
aws ec2 create-vpc-peering-connection \
    --vpc-id vpc-12345678 \
    --peer-vpc-id vpc-87654321 \
    --peer-region us-east-1 \
    --tag-specifications 'ResourceType=vpc-peering-connection,Tags=[{Key=Name,Value=Cross-Region-Peering}]'

# Cross-Account Peering
aws ec2 create-vpc-peering-connection \
    --vpc-id vpc-12345678 \
    --peer-vpc-id vpc-87654321 \
    --peer-owner-id 987654321098 \
    --tag-specifications 'ResourceType=vpc-peering-connection,Tags=[{Key=Name,Value=Cross-Account-Peering}]'

# Accept Peering Connection (must be done by peer VPC owner)
aws ec2 accept-vpc-peering-connection \
    --vpc-peering-connection-id pcx-12345678

# Enable DNS Resolution for Peering
aws ec2 modify-vpc-peering-connection-options \
    --vpc-peering-connection-id pcx-12345678 \
    --accepter-peering-connection-options AllowDnsResolutionFromRemoteVpc=true \
    --requester-peering-connection-options AllowDnsResolutionFromRemoteVpc=true

# Python script for comprehensive peering setup
import boto3
import json
import time

def create_vpc_peering_connection(config):
    """Create VPC peering connection with comprehensive configuration"""
    
    ec2 = boto3.client('ec2', region_name=config['source_region'])
    
    # Create peering connection
    peering_params = {
        'VpcId': config['source_vpc_id'],
        'PeerVpcId': config['target_vpc_id'],
        'TagSpecifications': [
            {
                'ResourceType': 'vpc-peering-connection',
                'Tags': [
                    {'Key': 'Name', 'Value': config['peering_name']},
                    {'Key': 'Environment', 'Value': config.get('environment', 'production')}
                ]
            }
        ]
    }
    
    # Add cross-region parameter if needed
    if config.get('target_region') != config['source_region']:
        peering_params['PeerRegion'] = config['target_region']
    
    # Add cross-account parameter if needed
    if config.get('target_account_id'):
        peering_params['PeerOwnerId'] = config['target_account_id']
    
    try:
        response = ec2.create_vpc_peering_connection(**peering_params)
        peering_id = response['VpcPeeringConnection']['VpcPeeringConnectionId']
        
        print(f"✅ Created peering connection: {peering_id}")
        
        # Wait for peering connection to be available
        waiter = ec2.get_waiter('vpc_peering_connection_exists')
        waiter.wait(VpcPeeringConnectionIds=[peering_id])
        
        # Configure DNS options if in same account
        if not config.get('target_account_id'):
            configure_dns_options(ec2, peering_id)
        
        return peering_id
        
    except Exception as e:
        print(f"❌ Failed to create peering connection: {e}")
        return None

def configure_dns_options(ec2_client, peering_id):
    """Configure DNS resolution options for peering connection"""
    
    try:
        ec2_client.modify_vpc_peering_connection_options(
            VpcPeeringConnectionId=peering_id,
            AccepterPeeringConnectionOptions={
                'AllowDnsResolutionFromRemoteVpc': True
            },
            RequesterPeeringConnectionOptions={
                'AllowDnsResolutionFromRemoteVpc': True
            }
        )
        print(f"✅ Configured DNS options for {peering_id}")
        
    except Exception as e:
        print(f"❌ Failed to configure DNS options: {e}")

def accept_peering_connection(peering_id, region):
    """Accept peering connection from peer VPC"""
    
    ec2 = boto3.client('ec2', region_name=region)
    
    try:
        ec2.accept_vpc_peering_connection(
            VpcPeeringConnectionId=peering_id
        )
        print(f"✅ Accepted peering connection: {peering_id}")
        
        # Wait for connection to be active
        waiter = ec2.get_waiter('vpc_peering_connection_exists')
        waiter.wait(VpcPeeringConnectionIds=[peering_id])
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to accept peering connection: {e}")
        return False

def create_full_mesh_peering(vpcs):
    """Create full mesh peering between multiple VPCs"""
    
    peering_connections = []
    
    for i, vpc1 in enumerate(vpcs):
        for j, vpc2 in enumerate(vpcs):
            if i < j:  # Avoid duplicate connections
                config = {
                    'source_vpc_id': vpc1['vpc_id'],
                    'source_region': vpc1['region'],
                    'target_vpc_id': vpc2['vpc_id'],
                    'target_region': vpc2['region'],
                    'target_account_id': vpc2.get('account_id'),
                    'peering_name': f"{vpc1['name']}-to-{vpc2['name']}",
                    'environment': vpc1.get('environment', 'production')
                }
                
                peering_id = create_vpc_peering_connection(config)
                if peering_id:
                    peering_connections.append({
                        'peering_id': peering_id,
                        'source_vpc': vpc1,
                        'target_vpc': vpc2
                    })
    
    return peering_connections

def get_peering_status(peering_id, region):
    """Get detailed status of peering connection"""
    
    ec2 = boto3.client('ec2', region_name=region)
    
    try:
        response = ec2.describe_vpc_peering_connections(
            VpcPeeringConnectionIds=[peering_id]
        )
        
        peering = response['VpcPeeringConnections'][0]
        
        status_info = {
            'peering_id': peering_id,
            'state': peering['Status']['Code'],
            'message': peering['Status']['Message'],
            'requester_vpc': peering['RequesterVpcInfo']['VpcId'],
            'accepter_vpc': peering['AccepterVpcInfo']['VpcId'],
            'requester_region': peering['RequesterVpcInfo']['Region'],
            'accepter_region': peering['AccepterVpcInfo']['Region'],
            'creation_time': peering['CreationTime'],
            'tags': peering.get('Tags', [])
        }
        
        return status_info
        
    except Exception as e:
        print(f"❌ Failed to get peering status: {e}")
        return None

# Usage examples
# Same region, same account peering
same_region_config = {
    'source_vpc_id': 'vpc-12345678',
    'source_region': 'us-west-2',
    'target_vpc_id': 'vpc-87654321',
    'target_region': 'us-west-2',
    'peering_name': 'Prod-Dev-Peering',
    'environment': 'production'
}

# Cross-region peering
cross_region_config = {
    'source_vpc_id': 'vpc-12345678',
    'source_region': 'us-west-2',
    'target_vpc_id': 'vpc-abcdef12',
    'target_region': 'us-east-1',
    'peering_name': 'West-East-Peering',
    'environment': 'production'
}

# Cross-account peering
cross_account_config = {
    'source_vpc_id': 'vpc-12345678',
    'source_region': 'us-west-2',
    'target_vpc_id': 'vpc-87654321',
    'target_region': 'us-west-2',
    'target_account_id': '987654321098',
    'peering_name': 'Cross-Account-Peering',
    'environment': 'production'
}

# Create peering connections
peering_id_1 = create_vpc_peering_connection(same_region_config)
peering_id_2 = create_vpc_peering_connection(cross_region_config)
peering_id_3 = create_vpc_peering_connection(cross_account_config)

# Accept peering connections (if needed)
if peering_id_1:
    accept_peering_connection(peering_id_1, 'us-west-2')

# Full mesh example
vpcs = [
    {'vpc_id': 'vpc-12345678', 'region': 'us-west-2', 'name': 'Production'},
    {'vpc_id': 'vpc-87654321', 'region': 'us-west-2', 'name': 'Development'},
    {'vpc_id': 'vpc-abcdef12', 'region': 'us-east-1', 'name': 'DR'},
    {'vpc_id': 'vpc-fedcba21', 'region': 'eu-west-1', 'name': 'Europe'}
]

mesh_peering = create_full_mesh_peering(vpcs)
print(f"Created {len(mesh_peering)} peering connections for full mesh")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Configure Routes for VPC Peering

# Get route table IDs for source VPC
SOURCE_VPC_RT_IDS=$(aws ec2 describe-route-tables \
    --filters "Name=vpc-id,Values=vpc-12345678" \
    --query 'RouteTables[].RouteTableId' \
    --output text)

# Get route table IDs for target VPC
TARGET_VPC_RT_IDS=$(aws ec2 describe-route-tables \
    --filters "Name=vpc-id,Values=vpc-87654321" \
    --query 'RouteTables[].RouteTableId' \
    --output text)

# Add routes to source VPC route tables
for rt_id in $SOURCE_VPC_RT_IDS; do
    aws ec2 create-route \
        --route-table-id $rt_id \
        --destination-cidr-block 10.1.0.0/16 \
        --vpc-peering-connection-id pcx-12345678
done

# Add routes to target VPC route tables
for rt_id in $TARGET_VPC_RT_IDS; do
    aws ec2 create-route \
        --route-table-id $rt_id \
        --destination-cidr-block 10.0.0.0/16 \
        --vpc-peering-connection-id pcx-12345678
done

# Python script for comprehensive route management
import boto3
import ipaddress

def add_peering_routes(peering_id, source_vpc_id, target_vpc_id, regions):
    """Add routes for VPC peering connection"""
    
    # Get VPC information
    source_ec2 = boto3.client('ec2', region_name=regions['source'])
    target_ec2 = boto3.client('ec2', region_name=regions['target'])
    
    # Get VPC CIDR blocks
    source_vpc_info = get_vpc_info(source_ec2, source_vpc_id)
    target_vpc_info = get_vpc_info(target_ec2, target_vpc_id)
    
    # Get route tables
    source_route_tables = get_route_tables(source_ec2, source_vpc_id)
    target_route_tables = get_route_tables(target_ec2, target_vpc_id)
    
    # Add routes from source to target
    for rt in source_route_tables:
        for cidr in target_vpc_info['cidr_blocks']:
            add_route(source_ec2, rt['RouteTableId'], cidr, peering_id)
    
    # Add routes from target to source
    for rt in target_route_tables:
        for cidr in source_vpc_info['cidr_blocks']:
            add_route(target_ec2, rt['RouteTableId'], cidr, peering_id)
    
    print(f"✅ Added peering routes for {peering_id}")

def get_vpc_info(ec2_client, vpc_id):
    """Get VPC information including CIDR blocks"""
    
    try:
        response = ec2_client.describe_vpcs(VpcIds=[vpc_id])
        vpc = response['Vpcs'][0]
        
        # Get primary CIDR block
        cidr_blocks = [vpc['CidrBlock']]
        
        # Get additional CIDR blocks
        for cidr_assoc in vpc.get('CidrBlockAssociationSet', []):
            if cidr_assoc['CidrBlockState']['State'] == 'associated':
                cidr_blocks.append(cidr_assoc['CidrBlock'])
        
        return {
            'vpc_id': vpc_id,
            'cidr_blocks': cidr_blocks,
            'state': vpc['State']
        }
        
    except Exception as e:
        print(f"❌ Failed to get VPC info: {e}")
        return None

def get_route_tables(ec2_client, vpc_id):
    """Get all route tables for a VPC"""
    
    try:
        response = ec2_client.describe_route_tables(
            Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
        )
        return response['RouteTables']
        
    except Exception as e:
        print(f"❌ Failed to get route tables: {e}")
        return []

def add_route(ec2_client, route_table_id, destination_cidr, peering_id):
    """Add route to route table"""
    
    try:
        ec2_client.create_route(
            RouteTableId=route_table_id,
            DestinationCidrBlock=destination_cidr,
            VpcPeeringConnectionId=peering_id
        )
        print(f"✅ Added route {destination_cidr} to {route_table_id}")
        
    except Exception as e:
        if 'RouteAlreadyExists' in str(e):
            print(f"ℹ️  Route {destination_cidr} already exists in {route_table_id}")
        else:
            print(f"❌ Failed to add route: {e}")

def create_selective_routes(peering_id, source_config, target_config):
    """Create selective routes for specific subnets only"""
    
    source_ec2 = boto3.client('ec2', region_name=source_config['region'])
    target_ec2 = boto3.client('ec2', region_name=target_config['region'])
    
    # Add routes from source to target (selective subnets)
    for subnet_cidr in source_config['allowed_subnets']:
        # Get route table for this subnet
        subnet_rt = get_subnet_route_table(source_ec2, subnet_cidr, source_config['vpc_id'])
        
        if subnet_rt:
            for target_cidr in target_config['accessible_cidrs']:
                add_route(source_ec2, subnet_rt['RouteTableId'], target_cidr, peering_id)
    
    # Add routes from target to source (selective subnets)
    for subnet_cidr in target_config['allowed_subnets']:
        # Get route table for this subnet
        subnet_rt = get_subnet_route_table(target_ec2, subnet_cidr, target_config['vpc_id'])
        
        if subnet_rt:
            for source_cidr in source_config['accessible_cidrs']:
                add_route(target_ec2, subnet_rt['RouteTableId'], source_cidr, peering_id)

def get_subnet_route_table(ec2_client, subnet_cidr, vpc_id):
    """Get route table for a specific subnet"""
    
    try:
        # First find the subnet ID
        subnets = ec2_client.describe_subnets(
            Filters=[
                {'Name': 'vpc-id', 'Values': [vpc_id]},
                {'Name': 'cidr-block', 'Values': [subnet_cidr]}
            ]
        )
        
        if not subnets['Subnets']:
            return None
            
        subnet_id = subnets['Subnets'][0]['SubnetId']
        
        # Get route table associations
        route_tables = ec2_client.describe_route_tables(
            Filters=[
                {'Name': 'association.subnet-id', 'Values': [subnet_id]}
            ]
        )
        
        if route_tables['RouteTables']:
            return route_tables['RouteTables'][0]
        
        # If no explicit association, get main route table
        main_rt = ec2_client.describe_route_tables(
            Filters=[
                {'Name': 'vpc-id', 'Values': [vpc_id]},
                {'Name': 'association.main', 'Values': ['true']}
            ]
        )
        
        return main_rt['RouteTables'][0] if main_rt['RouteTables'] else None
        
    except Exception as e:
        print(f"❌ Failed to get subnet route table: {e}")
        return None

def validate_peering_routes(peering_id, source_vpc_id, target_vpc_id, regions):
    """Validate that peering routes are properly configured"""
    
    source_ec2 = boto3.client('ec2', region_name=regions['source'])
    target_ec2 = boto3.client('ec2', region_name=regions['target'])
    
    validation_results = {
        'source_routes': [],
        'target_routes': [],
        'issues': []
    }
    
    # Get expected CIDR blocks
    source_vpc_info = get_vpc_info(source_ec2, source_vpc_id)
    target_vpc_info = get_vpc_info(target_ec2, target_vpc_id)
    
    # Check source VPC routes
    source_route_tables = get_route_tables(source_ec2, source_vpc_id)
    for rt in source_route_tables:
        for route in rt['Routes']:
            if route.get('VpcPeeringConnectionId') == peering_id:
                validation_results['source_routes'].append({
                    'route_table_id': rt['RouteTableId'],
                    'destination': route['DestinationCidrBlock'],
                    'state': route['State']
                })
    
    # Check target VPC routes
    target_route_tables = get_route_tables(target_ec2, target_vpc_id)
    for rt in target_route_tables:
        for route in rt['Routes']:
            if route.get('VpcPeeringConnectionId') == peering_id:
                validation_results['target_routes'].append({
                    'route_table_id': rt['RouteTableId'],
                    'destination': route['DestinationCidrBlock'],
                    'state': route['State']
                })
    
    # Check for missing routes
    expected_source_routes = len(source_route_tables) * len(target_vpc_info['cidr_blocks'])
    expected_target_routes = len(target_route_tables) * len(source_vpc_info['cidr_blocks'])
    
    if len(validation_results['source_routes']) < expected_source_routes:
        validation_results['issues'].append(
            f"Missing routes in source VPC: expected {expected_source_routes}, found {len(validation_results['source_routes'])}"
        )
    
    if len(validation_results['target_routes']) < expected_target_routes:
        validation_results['issues'].append(
            f"Missing routes in target VPC: expected {expected_target_routes}, found {len(validation_results['target_routes'])}"
        )
    
    return validation_results

# Usage examples
regions = {
    'source': 'us-west-2',
    'target': 'us-east-1'
}

# Add basic peering routes
add_peering_routes('pcx-12345678', 'vpc-12345678', 'vpc-87654321', regions)

# Selective routing configuration
source_config = {
    'vpc_id': 'vpc-12345678',
    'region': 'us-west-2',
    'allowed_subnets': ['10.0.1.0/24', '10.0.2.0/24'],  # Only these subnets
    'accessible_cidrs': ['10.1.1.0/24']  # Can only access this target subnet
}

target_config = {
    'vpc_id': 'vpc-87654321',
    'region': 'us-east-1',
    'allowed_subnets': ['10.1.1.0/24'],  # Only this subnet
    'accessible_cidrs': ['10.0.1.0/24', '10.0.2.0/24']  # Can access these source subnets
}

# Create selective routes
create_selective_routes('pcx-12345678', source_config, target_config)

# Validate routes
validation = validate_peering_routes('pcx-12345678', 'vpc-12345678', 'vpc-87654321', regions)
print(f"Source routes: {len(validation['source_routes'])}")
print(f"Target routes: {len(validation['target_routes'])}")
if validation['issues']:
    print(f"Issues found: {validation['issues']}")
else:
    print("✅ All routes validated successfully")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Monitor and Troubleshoot VPC Peering

# Check peering connection status
aws ec2 describe-vpc-peering-connections \
    --filters "Name=status-code,Values=active" \
    --query 'VpcPeeringConnections[*].[VpcPeeringConnectionId,Status.Code,RequesterVpcInfo.VpcId,AccepterVpcInfo.VpcId]' \
    --output table

# Check specific peering connection
aws ec2 describe-vpc-peering-connections \
    --vpc-peering-connection-ids pcx-12345678 \
    --query 'VpcPeeringConnections[0].[VpcPeeringConnectionId,Status.Code,Status.Message,RequesterVpcInfo.VpcId,AccepterVpcInfo.VpcId]' \
    --output table

# Test connectivity using VPC Flow Logs
aws ec2 create-flow-logs \
    --resource-type VPC \
    --resource-ids vpc-12345678 \
    --traffic-type ALL \
    --log-destination-type cloud-watch-logs \
    --log-group-name VPCFlowLogs

# Python script for comprehensive monitoring
import boto3
import json
from datetime import datetime, timedelta

def monitor_peering_connections(region):
    """Monitor all VPC peering connections in a region"""
    
    ec2 = boto3.client('ec2', region_name=region)
    
    try:
        response = ec2.describe_vpc_peering_connections()
        
        print(f"🔍 Monitoring VPC Peering Connections in {region}")
        print("=" * 60)
        
        for peering in response['VpcPeeringConnections']:
            analyze_peering_connection(peering)
            print("-" * 40)
            
    except Exception as e:
        print(f"❌ Failed to monitor peering connections: {e}")

def analyze_peering_connection(peering):
    """Analyze individual peering connection"""
    
    peering_id = peering['VpcPeeringConnectionId']
    status = peering['Status']['Code']
    message = peering['Status']['Message']
    
    print(f"Peering ID: {peering_id}")
    print(f"Status: {status}")
    print(f"Message: {message}")
    
    # Analyze requester VPC
    requester = peering['RequesterVpcInfo']
    print(f"Requester VPC: {requester['VpcId']} ({requester['CidrBlock']})")
    print(f"Requester Region: {requester['Region']}")
    
    # Analyze accepter VPC
    accepter = peering['AccepterVpcInfo']
    print(f"Accepter VPC: {accepter['VpcId']} ({accepter['CidrBlock']})")
    print(f"Accepter Region: {accepter['Region']}")
    
    # Check for common issues
    issues = []
    
    if status == 'pending-acceptance':
        issues.append("⚠️  Peering connection pending acceptance")
    elif status == 'failed':
        issues.append("🚨 Peering connection failed")
    elif status == 'rejected':
        issues.append("❌ Peering connection rejected")
    
    # Check for CIDR overlap
    if check_cidr_overlap(requester['CidrBlock'], accepter['CidrBlock']):
        issues.append("🚨 CIDR blocks overlap")
    
    if issues:
        print("Issues detected:")
        for issue in issues:
            print(f"  {issue}")
    else:
        print("✅ No issues detected")

def test_peering_connectivity(source_vpc_id, target_vpc_id, regions):
    """Test connectivity between peered VPCs"""
    
    source_ec2 = boto3.client('ec2', region_name=regions['source'])
    target_ec2 = boto3.client('ec2', region_name=regions['target'])
    
    print(f"🔍 Testing connectivity between {source_vpc_id} and {target_vpc_id}")
    
    # Get instances in both VPCs
    source_instances = get_vpc_instances(source_ec2, source_vpc_id)
    target_instances = get_vpc_instances(target_ec2, target_vpc_id)
    
    print(f"Source VPC instances: {len(source_instances)}")
    print(f"Target VPC instances: {len(target_instances)}")
    
    # Check route tables
    source_routes = check_peering_routes(source_ec2, source_vpc_id, target_vpc_id)
    target_routes = check_peering_routes(target_ec2, target_vpc_id, source_vpc_id)
    
    print(f"Source VPC peering routes: {len(source_routes)}")
    print(f"Target VPC peering routes: {len(target_routes)}")
    
    # Check security groups
    connectivity_issues = check_security_groups(source_ec2, target_ec2, source_instances, target_instances)
    
    if connectivity_issues:
        print("🚨 Security group issues detected:")
        for issue in connectivity_issues:
            print(f"  {issue}")
    else:
        print("✅ Security groups allow connectivity")

def get_vpc_instances(ec2_client, vpc_id):
    """Get all instances in a VPC"""
    
    try:
        response = ec2_client.describe_instances(
            Filters=[
                {'Name': 'vpc-id', 'Values': [vpc_id]},
                {'Name': 'instance-state-name', 'Values': ['running']}
            ]
        )
        
        instances = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                instances.append({
                    'instance_id': instance['InstanceId'],
                    'private_ip': instance['PrivateIpAddress'],
                    'subnet_id': instance['SubnetId'],
                    'security_groups': instance['SecurityGroups']
                })
        
        return instances
        
    except Exception as e:
        print(f"❌ Failed to get VPC instances: {e}")
        return []

def check_peering_routes(ec2_client, vpc_id, peer_vpc_id):
    """Check if peering routes are configured"""
    
    try:
        # Get peer VPC CIDR
        peer_vpc = ec2_client.describe_vpcs(VpcIds=[peer_vpc_id])
        peer_cidr = peer_vpc['Vpcs'][0]['CidrBlock']
        
        # Get route tables
        route_tables = ec2_client.describe_route_tables(
            Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
        )
        
        peering_routes = []
        for rt in route_tables['RouteTables']:
            for route in rt['Routes']:
                if (route.get('VpcPeeringConnectionId') and 
                    route.get('DestinationCidrBlock') == peer_cidr):
                    peering_routes.append({
                        'route_table_id': rt['RouteTableId'],
                        'destination': route['DestinationCidrBlock'],
                        'peering_id': route['VpcPeeringConnectionId'],
                        'state': route['State']
                    })
        
        return peering_routes
        
    except Exception as e:
        print(f"❌ Failed to check peering routes: {e}")
        return []

def check_security_groups(source_ec2, target_ec2, source_instances, target_instances):
    """Check security group rules for peering connectivity"""
    
    issues = []
    
    # Get target VPC CIDR
    if not target_instances:
        return issues
    
    target_subnet_response = target_ec2.describe_subnets(
        SubnetIds=[target_instances[0]['subnet_id']]
    )
    target_vpc_id = target_subnet_response['Subnets'][0]['VpcId']
    
    target_vpc_response = target_ec2.describe_vpcs(VpcIds=[target_vpc_id])
    target_cidr = target_vpc_response['Vpcs'][0]['CidrBlock']
    
    # Check source instance security groups
    for instance in source_instances:
        for sg in instance['security_groups']:
            sg_details = source_ec2.describe_security_groups(
                GroupIds=[sg['GroupId']]
            )
            
            # Check outbound rules
            has_outbound_rule = False
            for rule in sg_details['SecurityGroups'][0]['IpPermissionsEgress']:
                for ip_range in rule.get('IpRanges', []):
                    if ip_range.get('CidrIp') in ['0.0.0.0/0', target_cidr]:
                        has_outbound_rule = True
                        break
            
            if not has_outbound_rule:
                issues.append(f"Instance {instance['instance_id']} SG {sg['GroupId']} lacks outbound rule to {target_cidr}")
    
    return issues

def create_peering_monitoring_dashboard(peering_connections):
    """Create CloudWatch dashboard for peering monitoring"""
    
    cloudwatch = boto3.client('cloudwatch')
    
    widgets = []
    
    for i, peering in enumerate(peering_connections):
        widget = {
            "type": "metric",
            "properties": {
                "metrics": [
                    ["AWS/VPC", "PacketDropCount", "VpcPeeringConnectionId", peering['peering_id']],
                    ["AWS/VPC", "BytesTransferred", "VpcPeeringConnectionId", peering['peering_id']]
                ],
                "period": 300,
                "stat": "Sum",
                "region": peering['region'],
                "title": f"Peering Connection {peering['peering_id']}"
            }
        }
        widgets.append(widget)
    
    dashboard_body = {
        "widgets": widgets
    }
    
    try:
        cloudwatch.put_dashboard(
            DashboardName='VPC-Peering-Monitoring',
            DashboardBody=json.dumps(dashboard_body)
        )
        print("✅ Created peering monitoring dashboard")
        
    except Exception as e:
        print(f"❌ Failed to create dashboard: {e}")

def troubleshoot_peering_issues(peering_id, regions):
    """Comprehensive troubleshooting for peering issues"""
    
    print(f"🔍 Troubleshooting peering connection: {peering_id}")
    
    # Check peering connection status
    for region in regions:
        ec2 = boto3.client('ec2', region_name=region)
        
        try:
            response = ec2.describe_vpc_peering_connections(
                VpcPeeringConnectionIds=[peering_id]
            )
            
            if response['VpcPeeringConnections']:
                peering = response['VpcPeeringConnections'][0]
                print(f"Found peering connection in {region}")
                print(f"Status: {peering['Status']['Code']}")
                print(f"Message: {peering['Status']['Message']}")
                
                # Check DNS options
                if 'AccepterVpcInfo' in peering:
                    accepter_dns = peering['AccepterVpcInfo'].get('PeeringOptions', {})
                    print(f"Accepter DNS resolution: {accepter_dns.get('AllowDnsResolutionFromRemoteVpc', 'Not set')}")
                
                if 'RequesterVpcInfo' in peering:
                    requester_dns = peering['RequesterVpcInfo'].get('PeeringOptions', {})
                    print(f"Requester DNS resolution: {requester_dns.get('AllowDnsResolutionFromRemoteVpc', 'Not set')}")
                
                break
                
        except Exception as e:
            print(f"Error checking {region}: {e}")
    
    # Additional troubleshooting steps
    troubleshooting_steps = [
        "1. Verify peering connection is in 'active' state",
        "2. Check route tables have correct peering routes",
        "3. Verify security groups allow required traffic",
        "4. Confirm NACLs are not blocking traffic",
        "5. Check DNS resolution settings if using hostnames",
        "6. Verify no CIDR block overlap between VPCs",
        "7. Test connectivity using VPC Flow Logs",
        "8. Check for any AWS service limits"
    ]
    
    print("\n📋 Troubleshooting checklist:")
    for step in troubleshooting_steps:
        print(f"  {step}")

# Usage examples
regions = ['us-west-2', 'us-east-1']

# Monitor all peering connections
for region in regions:
    monitor_peering_connections(region)

# Test specific peering connectivity
test_peering_connectivity('vpc-12345678', 'vpc-87654321', {
    'source': 'us-west-2',
    'target': 'us-east-1'
})

# Troubleshoot specific peering connection
troubleshoot_peering_issues('pcx-12345678', regions)

# Create monitoring dashboard
peering_connections = [
    {'peering_id': 'pcx-12345678', 'region': 'us-west-2'},
    {'peering_id': 'pcx-87654321', 'region': 'us-east-1'}
]

create_peering_monitoring_dashboard(peering_connections)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

# Helper functions
def generate_subnet_config(vpc_cidr, num_azs, subnet_types):
    """Generate subnet configuration based on VPC CIDR and requirements"""
    subnets = []
    
    # Convert CIDR to network object
    network = ipaddress.IPv4Network(vpc_cidr)
    
    # Calculate subnet size (assuming /24 subnets)
    subnet_size = 24
    
    subnet_index = 0
    for az in range(num_azs):
        for subnet_type in subnet_types:
            subnet_cidr = str(list(network.subnets(new_prefix=subnet_size))[subnet_index])
            
            # Determine route table
            if subnet_type == "Public Subnet":
                route_table = "Public Route Table"
            elif subnet_type == "Private Subnet":
                route_table = f"Private Route Table AZ{az + 1}"
            else:
                route_table = f"Database Route Table AZ{az + 1}"
            
            subnets.append({
                'name': f"{subnet_type} AZ{az + 1}",
                'cidr': subnet_cidr,
                'az': f"us-west-2{'abc'[az]}",
                'type': subnet_type,
                'route_table': route_table
            })
            
            subnet_index += 1
    
    return subnets

def get_default_port(protocol):
    """Get default port for protocol"""
    ports = {
        "HTTP": 80,
        "HTTPS": 443,
        "SSH": 22,
        "MySQL": 3306,
        "PostgreSQL": 5432
    }
    return ports.get(protocol, 80)

def evaluate_security_group_rules(rules):
    """Evaluate security group rules for security best practices"""
    for rule in rules:
        if rule['source'] == '0.0.0.0/0':
            if rule['protocol'] in ['SSH', 'MySQL', 'PostgreSQL']:
                st.markdown(f'<div class="blocked-rule">⚠️ {rule["protocol"]} open to internet - Security risk!</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="security-rule">✅ {rule["protocol"]} from anywhere - Common for web services</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="security-rule">✅ {rule["protocol"]} from {rule["source"]} - Restricted access</div>', unsafe_allow_html=True)

def evaluate_nacl_rules(rules):
    """Evaluate NACL rules for proper configuration"""
    # Sort rules by rule number
    sorted_rules = sorted(rules, key=lambda x: x['rule_number'])
    
    for rule in sorted_rules:
        if rule['action'] == 'ALLOW':
            st.markdown(f'<div class="security-rule">✅ Rule {rule["rule_number"]}: ALLOW {rule["protocol"]} from {rule["source"]}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="blocked-rule">🚫 Rule {rule["rule_number"]}: DENY {rule["protocol"]} from {rule["source"]}</div>', unsafe_allow_html=True)

def calculate_endpoint_cost(endpoint_type, service, num_azs):
    """Calculate estimated monthly cost for VPC endpoint"""
    if endpoint_type == "Gateway":
        return "Free"
    else:
        hourly_cost = 0.01 * num_azs  # $0.01 per hour per AZ
        monthly_cost = hourly_cost * 24 * 30
        return f"${monthly_cost:.2f}/month"

def create_vpc_endpoints_services_data():
    """Create VPC endpoints services data"""
    data = {
        'Service': [
            'Amazon S3', 'Amazon DynamoDB', 'Amazon SQS', 'Amazon SNS',
            'AWS Lambda', 'Amazon CloudWatch', 'Amazon EC2', 'AWS Systems Manager',
            'AWS Secrets Manager', 'Amazon ECS', 'Amazon ECR', 'AWS KMS'
        ],
        'Endpoint Type': [
            'Gateway', 'Gateway', 'Interface', 'Interface',
            'Interface', 'Interface', 'Interface', 'Interface',
            'Interface', 'Interface', 'Interface', 'Interface'
        ],
        'Cost': [
            '$0.00', '$0.00', '$0.01/hour', '$0.01/hour',
            '$0.01/hour', '$0.01/hour', '$0.01/hour', '$0.01/hour',
            '$0.01/hour', '$0.01/hour', '$0.01/hour', '$0.01/hour'
        ],
        'Use Case': [
            'Object storage access', 'NoSQL database access', 'Message queuing', 'Notifications',
            'Function execution', 'Monitoring & logging', 'Instance management', 'Systems management',
            'Secrets management', 'Container service', 'Container registry', 'Key management'
        ]
    }
    return pd.DataFrame(data)

def check_cidr_overlap(cidr1, cidr2):
    """Check if two CIDR blocks overlap"""
    try:
        network1 = ipaddress.IPv4Network(cidr1)
        network2 = ipaddress.IPv4Network(cidr2)
        return network1.overlaps(network2)
    except:
        return False

def calculate_peering_cost(cross_region, target_region):
    """Calculate estimated peering cost"""
    if cross_region:
        return "$0.02/GB data transfer"
    else:
        return "Free (same region)"

def generate_peering_routes(source_cidr, target_cidr):
    """Generate peering routes for both VPCs"""
    return {
        'source_routes': [
            {'Destination': target_cidr, 'Target': 'pcx-12345678', 'Status': 'Active'},
            {'Destination': '0.0.0.0/0', 'Target': 'igw-12345678', 'Status': 'Active'}
        ],
        'target_routes': [
            {'Destination': source_cidr, 'Target': 'pcx-12345678', 'Status': 'Active'},
            {'Destination': '0.0.0.0/0', 'Target': 'igw-87654321', 'Status': 'Active'}
        ]
    }

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
    # 🔒 AWS VPC & Network Security
    <div class='info-box'>
    Master Amazon Virtual Private Cloud (VPC) architecture, security groups, network ACLs, VPC endpoints, and VPC peering to build secure, scalable AWS network infrastructures.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🏗️ Amazon Virtual Private Cloud (VPC)",
        "🔒 Security Groups & NACLs",
        "🔗 Connect Privately to public AWS Services",
        "🌐 How to connect directly to other VPCs?"
    ])
    
    with tab1:
        vpc_tab()
    
    with tab2:
        security_groups_nacl_tab()
    
    with tab3:
        vpc_endpoints_tab()
    
    with tab4:
        vpc_peering_tab()
    
    # Footer
    st.markdown("""
    <div class="footer">
        <p>© 2025, Amazon Web Services, Inc. or its affiliates. All rights reserved.</p>
    </div>
    """, unsafe_allow_html=True)

# Import required for IP address operations
import ipaddress

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
