
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import utils.common as common
import utils.authenticate as authenticate
import json

# Page configuration
st.set_page_config(
    page_title="AWS IAM Security & Access Management",
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
    'warning': '#FFA500',
    'danger': '#FF6B6B'
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
        
        .policy-card {{
            background: white;
            padding: 15px;
            border-radius: 10px;
            border: 2px solid {AWS_COLORS['light_blue']};
            margin: 10px 0;
        }}
        
        .decision-box {{
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
        
        .deny-box {{
            background-color: #FFE6E6;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 15px;
            border-left: 5px solid {AWS_COLORS['danger']};
        }}
        
        .allow-box {{
            background-color: #E6FFE6;
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
    if 'session_started' not in st.session_state:
        st.session_state.session_started = True
        st.session_state.policies_created = []
        st.session_state.iam_exercises_completed = []

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🔐 What is IAM? - Identity and Access Management fundamentals
            - 👥 IAM Users and Groups - Building blocks of AWS security
            - 📋 Policy Interpretation Deep Dive! - Understanding IAM policies
            - ⚖️ Policy Interpretation – Deny vs Allow - Decision flow logic
            
            **Learning Objectives:**
            - Master AWS IAM core concepts and components
            - Understand policy syntax and evaluation
            - Learn explicit vs implicit permissions
            - Explore policy decision flow and evaluation order
            """)

def create_iam_architecture_mermaid():
    """Create mermaid diagram for IAM architecture"""
    return """
    graph TB
        subgraph "AWS Account"
            ROOT[Root User<br/>👑 Complete Access]
            
            subgraph "IAM Users"
                USER1[👤 User 1]
                USER2[👤 User 2]
                USER3[👤 User 3]
            end
            
            subgraph "IAM Groups"
                GROUP1[👥 Developers]
                GROUP2[👥 Administrators]
                GROUP3[👥 Read-Only Users]
            end
            
            subgraph "IAM Roles"
                ROLE1[🎭 EC2 Service Role]
                ROLE2[🎭 Lambda Execution Role]
                ROLE3[🎭 Cross-Account Role]
            end
            
            subgraph "IAM Policies"
                POLICY1[📄 Managed Policy]
                POLICY2[📄 Inline Policy]
                POLICY3[📄 Customer Managed]
            end
        end
        
        USER1 --> GROUP1
        USER2 --> GROUP2
        USER3 --> GROUP3
        
        GROUP1 --> POLICY1
        GROUP2 --> POLICY2
        GROUP3 --> POLICY3
        
        ROLE1 --> POLICY1
        ROLE2 --> POLICY2
        ROLE3 --> POLICY3
        
        style ROOT fill:#FF9900,stroke:#232F3E,color:#fff
        style GROUP1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style GROUP2 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style GROUP3 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style POLICY1 fill:#3FB34F,stroke:#232F3E,color:#fff
        style POLICY2 fill:#3FB34F,stroke:#232F3E,color:#fff
        style POLICY3 fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_policy_evaluation_flow():
    """Create mermaid diagram for policy evaluation flow"""
    return """
    graph TD
        START[Request Made] --> DEFAULT[Default: Implicit DENY]
        DEFAULT --> IDENTITY{Identity-based<br/>Policy Allow?}
        
        IDENTITY -->|No| RESOURCE{Resource-based<br/>Policy Allow?}
        IDENTITY -->|Yes| BOUNDARY{Permissions<br/>Boundary Check}
        
        RESOURCE -->|No| DENY1[❌ IMPLICIT DENY]
        RESOURCE -->|Yes| EXPLICIT{Explicit DENY<br/>anywhere?}
        
        BOUNDARY -->|Pass| SCP{Service Control<br/>Policy Check}
        BOUNDARY -->|Fail| DENY2[❌ BOUNDARY DENY]
        
        SCP -->|Pass| SESSION{Session Policy<br/>Check}
        SCP -->|Fail| DENY3[❌ SCP DENY]
        
        SESSION -->|Pass| EXPLICIT
        SESSION -->|Fail| DENY4[❌ SESSION DENY]
        
        EXPLICIT -->|Yes| DENY5[❌ EXPLICIT DENY]
        EXPLICIT -->|No| ALLOW[✅ ALLOW]
        
        style START fill:#FF9900,stroke:#232F3E,color:#fff
        style DEFAULT fill:#FFF3CD,stroke:#FF9900,color:#000
        style ALLOW fill:#3FB34F,stroke:#232F3E,color:#fff
        style DENY1 fill:#FF6B6B,stroke:#232F3E,color:#fff
        style DENY2 fill:#FF6B6B,stroke:#232F3E,color:#fff
        style DENY3 fill:#FF6B6B,stroke:#232F3E,color:#fff
        style DENY4 fill:#FF6B6B,stroke:#232F3E,color:#fff
        style DENY5 fill:#FF6B6B,stroke:#232F3E,color:#fff
    """

def what_is_iam_tab():
    """Content for What is IAM? tab"""
    st.markdown("## 🔐 What is IAM?")
    st.markdown("*Identity and Access Management (IAM) - Securely control access to AWS resources*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 AWS Identity and Access Management (IAM)
    IAM is a web service that helps you securely control access to AWS resources. With IAM, you can centrally manage permissions that control which AWS resources users can access. You use IAM to control who is **authenticated** (signed in) and **authorized** (has permissions) to use resources.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # IAM Architecture
    st.markdown("#### 🏗️ IAM Architecture Overview")
    common.mermaid(create_iam_architecture_mermaid(), height=800)
    
    # Core Components
    st.markdown("#### 🔧 Core IAM Components")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 👤 IAM Users
        An IAM user is an entity that you create in AWS to represent the person or application that uses it to interact with AWS.
        
        **Key Features:**
        - Permanent long-term credentials
        - Can have console and/or programmatic access
        - Can belong to multiple groups
        - Maximum 5000 users per AWS account
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎭 IAM Roles
        An IAM role is an IAM identity with specific permissions, but unlike IAM users, roles are assumed temporarily.
        
        **Key Features:**
        - Temporary security credentials
        - Trusted entities can assume roles
        - Cross-account access capability
        - Service-to-service access
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 👥 IAM Groups
        An IAM group is a collection of IAM users. Groups let you specify permissions for multiple users at once.
        
        **Key Features:**
        - Simplify permission management
        - Users can belong to multiple groups
        - Groups cannot be nested
        - No default group exists
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📋 IAM Policies
        Policies define permissions and are written in JSON. They specify what actions are allowed or denied on which resources.
        
        **Key Features:**
        - JSON-based permission documents
        - Can be attached to users, groups, or roles
        - Managed or inline policies
        - Version controlled
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive IAM Simulator
    st.markdown("#### 🎮 Interactive IAM Access Simulator")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        entity_type = st.selectbox("Entity Type", ["IAM User", "IAM Role", "Root User"])
        entity_name = st.text_input("Entity Name", value="john-developer")
    
    with col2:
        action = st.selectbox("AWS Action", [
            "s3:GetObject", "s3:PutObject", "ec2:DescribeInstances", 
            "rds:CreateDatabase", "iam:CreateUser", "cloudformation:CreateStack"
        ])
        
    with col3:
        resource = st.text_input("Resource ARN", value="arn:aws:s3:::my-bucket/*")
    
    # Simulate access decision
    access_result = simulate_iam_access(entity_type, entity_name, action, resource)
    
    if access_result['allowed']:
        st.markdown('<div class="allow-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ ACCESS GRANTED
        **Entity**: {entity_name} ({entity_type})  
        **Action**: {action}  
        **Resource**: {resource}  
        **Reason**: {access_result['reason']}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="deny-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ❌ ACCESS DENIED
        **Entity**: {entity_name} ({entity_type})  
        **Action**: {action}  
        **Resource**: {resource}  
        **Reason**: {access_result['reason']}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # IAM Best Practices
    st.markdown("#### ⭐ IAM Security Best Practices")
    
    practices_col1, practices_col2 = st.columns(2)
    
    with practices_col1:
        st.markdown('<div class="decision-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛡️ Security Fundamentals
        - **Lock away your AWS account root user access keys**
        - **Create individual IAM users**
        - **Use groups to assign permissions to IAM users**
        - **Grant least privilege**
        - **Get started using permissions with AWS managed policies**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with practices_col2:
        st.markdown('<div class="decision-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Operational Security
        - **Use customer managed policies instead of inline policies**
        - **Use roles for applications that run on Amazon EC2 instances**
        - **Use roles to delegate permissions**
        - **Do not share access keys**
        - **Rotate credentials regularly**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Examples
    st.markdown("#### 💻 IAM Code Examples")
    
    tab1, tab2, tab3 = st.tabs(["Creating Users", "Managing Groups", "Working with Roles"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code("""
# Create IAM User using AWS CLI
aws iam create-user --user-name john-developer

# Set user password for console access
aws iam create-login-profile --user-name john-developer --password TempPassword123! --password-reset-required

# Create access keys for programmatic access
aws iam create-access-key --user-name john-developer

# Python Boto3 - Create IAM User
import boto3

iam = boto3.client('iam')

def create_iam_user(username, initial_password=None):
    try:
        # Create user
        response = iam.create_user(
            UserName=username,
            Tags=[
                {
                    'Key': 'Department',
                    'Value': 'Development'
                },
                {
                    'Key': 'Project', 
                    'Value': 'WebApp'
                }
            ]
        )
        
        print(f"User {username} created successfully")
        
        # Create login profile if password provided
        if initial_password:
            iam.create_login_profile(
                UserName=username,
                Password=initial_password,
                PasswordResetRequired=True
            )
            print(f"Console access enabled for {username}")
        
        # Create access keys for programmatic access
        keys = iam.create_access_key(UserName=username)
        
        return {
            'username': username,
            'access_key_id': keys['AccessKey']['AccessKeyId'],
            'secret_access_key': keys['AccessKey']['SecretAccessKey'],
            'status': 'success'
        }
        
    except Exception as e:
        print(f"Error creating user: {e}")
        return {'status': 'error', 'message': str(e)}

# Usage
user_info = create_iam_user('john-developer', 'TempPassword123!')
print(f"Access Key ID: {user_info.get('access_key_id')}")

# Attach policy to user
def attach_user_policy(username, policy_arn):
    try:
        iam.attach_user_policy(
            UserName=username,
            PolicyArn=policy_arn
        )
        print(f"Policy {policy_arn} attached to {username}")
    except Exception as e:
        print(f"Error attaching policy: {e}")

# Attach ReadOnlyAccess policy
attach_user_policy('john-developer', 'arn:aws:iam::aws:policy/ReadOnlyAccess')
        """, language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code("""
# Create IAM Group using AWS CLI
aws iam create-group --group-name Developers

# Add user to group
aws iam add-user-to-group --group-name Developers --user-name john-developer

# Python Boto3 - Group Management
import boto3

iam = boto3.client('iam')

def create_iam_group(group_name, description=""):
    try:
        # Create group
        response = iam.create_group(
            GroupName=group_name,
            Path='/'
        )
        
        print(f"Group {group_name} created successfully")
        return response
        
    except Exception as e:
        print(f"Error creating group: {e}")
        return None

def add_user_to_group(username, group_name):
    try:
        iam.add_user_to_group(
            GroupName=group_name,
            UserName=username
        )
        print(f"User {username} added to group {group_name}")
    except Exception as e:
        print(f"Error adding user to group: {e}")

def attach_group_policy(group_name, policy_arn):
    try:
        iam.attach_group_policy(
            GroupName=group_name,
            PolicyArn=policy_arn
        )
        print(f"Policy {policy_arn} attached to group {group_name}")
    except Exception as e:
        print(f"Error attaching policy to group: {e}")

# Create development team structure
groups_and_policies = {
    'Developers': 'arn:aws:iam::aws:policy/PowerUserAccess',
    'ReadOnlyUsers': 'arn:aws:iam::aws:policy/ReadOnlyAccess',
    'Administrators': 'arn:aws:iam::aws:policy/AdministratorAccess'
}

for group_name, policy_arn in groups_and_policies.items():
    create_iam_group(group_name)
    attach_group_policy(group_name, policy_arn)

# Add users to appropriate groups
user_group_assignments = [
    ('john-developer', 'Developers'),
    ('mary-analyst', 'ReadOnlyUsers'),
    ('admin-user', 'Administrators')
]

for username, group_name in user_group_assignments:
    add_user_to_group(username, group_name)

# List group members
def list_group_members(group_name):
    try:
        response = iam.get_group(GroupName=group_name)
        users = response['Users']
        
        print(f"Members of {group_name}:")
        for user in users:
            print(f"  - {user['UserName']} (Created: {user['CreateDate']})")
            
    except Exception as e:
        print(f"Error listing group members: {e}")

list_group_members('Developers')
        """, language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code("""
# Create IAM Role using AWS CLI
aws iam create-role --role-name EC2-S3-Access-Role --assume-role-policy-document file://trust-policy.json

# Python Boto3 - Role Management
import boto3
import json

iam = boto3.client('iam')

def create_service_role(role_name, service, description=""):
    # Trust policy for EC2 service
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": service
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description=description,
            Tags=[
                {
                    'Key': 'Purpose',
                    'Value': 'ServiceRole'
                }
            ]
        )
        
        print(f"Role {role_name} created successfully")
        return response['Role']['Arn']
        
    except Exception as e:
        print(f"Error creating role: {e}")
        return None

def create_cross_account_role(role_name, trusted_account_id):
    # Trust policy for cross-account access
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{trusted_account_id}:root"
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": "unique-external-id"
                    }
                }
            }
        ]
    }
    
    try:
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Cross-account access role"
        )
        
        print(f"Cross-account role {role_name} created successfully")
        return response['Role']['Arn']
        
    except Exception as e:
        print(f"Error creating cross-account role: {e}")
        return None

# Create EC2 instance role for S3 access
ec2_role_arn = create_service_role(
    'EC2-S3-Access-Role',
    'ec2.amazonaws.com',
    'Allows EC2 instances to access S3 buckets'
)

# Attach policy to role
if ec2_role_arn:
    iam.attach_role_policy(
        RoleName='EC2-S3-Access-Role',
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )

# Create instance profile (required for EC2)
def create_instance_profile(role_name):
    try:
        # Create instance profile
        iam.create_instance_profile(
            InstanceProfileName=role_name
        )
        
        # Add role to instance profile
        iam.add_role_to_instance_profile(
            InstanceProfileName=role_name,
            RoleName=role_name
        )
        
        print(f"Instance profile {role_name} created and role attached")
        
    except Exception as e:
        print(f"Error creating instance profile: {e}")

create_instance_profile('EC2-S3-Access-Role')

# Assume role example (for cross-account access)
sts = boto3.client('sts')

def assume_role(role_arn, session_name, external_id=None):
    try:
        assume_role_kwargs = {
            'RoleArn': role_arn,
            'RoleSessionName': session_name
        }
        
        if external_id:
            assume_role_kwargs['ExternalId'] = external_id
        
        response = sts.assume_role(**assume_role_kwargs)
        credentials = response['Credentials']
        
        # Create session with assumed role credentials
        session = boto3.Session(
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )
        
        print(f"Successfully assumed role: {role_arn}")
        return session
        
    except Exception as e:
        print(f"Error assuming role: {e}")
        return None

# Usage
assumed_session = assume_role(
    'arn:aws:iam::123456789012:role/CrossAccountRole',
    'AssumedRoleSession'
)

if assumed_session:
    # Use the assumed role session to access resources
    s3 = assumed_session.client('s3')
    buckets = s3.list_buckets()
    print(f"Accessible buckets: {[b['Name'] for b in buckets['Buckets']]}")
        """, language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def iam_users_and_groups_tab():
    """Content for IAM Users and Groups tab"""
    st.markdown("## 👥 IAM Users and Groups")
    st.markdown("*The building blocks of AWS Identity and Access Management*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Users and Groups - Core Building Blocks
    IAM users and groups form the foundation of access management in AWS. Users represent individual people or applications, while groups provide a way to organize users and apply common permissions efficiently.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive User/Group Builder
    st.markdown("#### 🏗️ Interactive User & Group Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### 👤 Create IAM User")
        username = st.text_input("Username", value="developer-john")
        access_type = st.multiselect("Access Type", ["Console Access", "Programmatic Access"], default=["Console Access"])
        user_tags = st.text_area("User Tags (JSON)", value='{"Department": "Engineering", "Team": "Backend"}')
        
        if st.button("Create User", key="create_user"):
            user_config = create_user_configuration(username, access_type, user_tags)
            st.session_state.users_created = st.session_state.get('users_created', [])
            st.session_state.users_created.append(user_config)
            st.success(f"User configuration created for: {username}")
    
    with col2:
        st.markdown("##### 👥 Create IAM Group")
        groupname = st.text_input("Group Name", value="Developers")
        group_description = st.text_input("Description", value="Development team members")
        managed_policies = st.multiselect("Attach Managed Policies", [
            "ReadOnlyAccess", "PowerUserAccess", "IAMReadOnlyAccess", 
            "AmazonS3ReadOnlyAccess", "AmazonEC2ReadOnlyAccess"
        ])
        
        if st.button("Create Group", key="create_group"):
            group_config = create_group_configuration(groupname, group_description, managed_policies)
            st.session_state.groups_created = st.session_state.get('groups_created', [])
            st.session_state.groups_created.append(group_config)
            st.success(f"Group configuration created: {groupname}")
    
    # Display created configurations
    if st.session_state.get('users_created') or st.session_state.get('groups_created'):
        st.markdown("#### 📋 Created Configurations")
        
        if st.session_state.get('users_created'):
            st.markdown("##### Users:")
            for user in st.session_state.users_created:
                st.markdown('<div class="policy-card">', unsafe_allow_html=True)
                st.markdown(f"""
                **Username**: {user['username']}  
                **Access Types**: {', '.join(user['access_types'])}  
                **Tags**: {user['tags']}
                """)
                st.markdown('</div>', unsafe_allow_html=True)
        
        if st.session_state.get('groups_created'):
            st.markdown("##### Groups:")
            for group in st.session_state.groups_created:
                st.markdown('<div class="policy-card">', unsafe_allow_html=True)
                st.markdown(f"""
                **Group Name**: {group['name']}  
                **Description**: {group['description']}  
                **Policies**: {', '.join(group['policies'])}
                """)
                st.markdown('</div>', unsafe_allow_html=True)
    
    # User vs Group Comparison
    st.markdown("#### ⚖️ Users vs Groups - When to Use What?")
    
    comparison_col1, comparison_col2 = st.columns(2)
    
    with comparison_col1:
        st.markdown('<div class="decision-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 👤 Use IAM Users When:
        - **Individual access needed**: Specific person needs AWS access
        - **Unique permissions required**: User needs special permissions different from any group
        - **Service accounts**: Applications or services need dedicated credentials
        - **Temporary contractors**: Short-term access that doesn't fit existing groups
        - **Break-glass scenarios**: Emergency access accounts
        
        **Example Use Cases:**
        - Database administrator with special RDS permissions
        - CI/CD pipeline service account
        - External consultant with limited access
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with comparison_col2:
        st.markdown('<div class="decision-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 👥 Use IAM Groups When:
        - **Common permissions**: Multiple users need same set of permissions
        - **Role-based access**: Organizing by job function or department
        - **Simplified management**: Easier to manage permissions for many users
        - **Scalable access control**: Adding/removing users frequently
        - **Compliance requirements**: Need to track access by role/function
        
        **Example Use Cases:**
        - All developers need S3 and EC2 access
        - Finance team needs billing and cost management access
        - QA team needs read-only access to production resources
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # User Lifecycle Management
    st.markdown("#### 🔄 User Lifecycle Management")
    
    lifecycle_flow = """
    graph LR
        CREATE[👤 Create User] --> ASSIGN[👥 Assign to Groups]
        ASSIGN --> ACTIVATE[🔑 Activate Access]
        ACTIVATE --> MONITOR[👁️ Monitor Usage]
        MONITOR --> UPDATE[🔄 Update Permissions]
        UPDATE --> ROTATE[🔧 Rotate Credentials]
        ROTATE --> REVIEW[📊 Review Access]
        REVIEW --> DEACTIVATE[⏸️ Deactivate/Delete]
        
        UPDATE --> MONITOR
        ROTATE --> MONITOR
        REVIEW --> UPDATE
        
        style CREATE fill:#FF9900,stroke:#232F3E,color:#fff
        style ASSIGN fill:#4B9EDB,stroke:#232F3E,color:#fff
        style MONITOR fill:#3FB34F,stroke:#232F3E,color:#fff
        style DEACTIVATE fill:#FF6B6B,stroke:#232F3E,color:#fff
    """
    
    common.mermaid(lifecycle_flow, height=200)
    
    # Access Patterns Analysis
    st.markdown("#### 📊 Common Access Patterns")
    
    # Create sample data for access patterns
    pattern_data = create_access_pattern_data()
    
    fig = px.bar(
        pattern_data, 
        x='Pattern', 
        y='Usage_Percentage',
        color='Complexity',
        title="Common IAM Access Patterns",
        color_discrete_map={
            'Low': AWS_COLORS['success'],
            'Medium': AWS_COLORS['warning'], 
            'High': AWS_COLORS['danger']
        }
    )
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Best Practices for Users and Groups
    st.markdown("#### ⭐ Best Practices for Users and Groups")
    
    best_practices_tabs = st.tabs(["User Management", "Group Organization", "Security Guidelines"])
    
    with best_practices_tabs[0]:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 👤 User Management Best Practices
        
        **🔐 Authentication Setup:**
        - Enable MFA for all human users
        - Use strong password policies
        - Rotate access keys regularly (every 90 days)
        - Never share IAM user credentials
        
        **📋 Permission Management:**
        - Follow principle of least privilege
        - Avoid attaching policies directly to users
        - Use groups to manage user permissions
        - Review user access regularly
        
        **🏷️ Organization:**
        - Use consistent naming conventions
        - Apply meaningful tags to users
        - Document user purpose and owner
        - Remove unused user accounts promptly
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with best_practices_tabs[1]:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 👥 Group Organization Best Practices
        
        **🏗️ Structure Design:**
        - Organize by job function, not department
        - Create focused, single-purpose groups
        - Use descriptive group names
        - Document group purpose and permissions
        
        **📊 Common Group Patterns:**
        - **Administrators**: Full account access
        - **PowerUsers**: All services except IAM
        - **Developers**: Development resources access
        - **ReadOnlyUsers**: View-only access
        - **BillingAdmins**: Billing and cost management
        
        **🔄 Management:**
        - Review group membership monthly
        - Audit group permissions quarterly
        - Use managed policies where possible
        - Avoid nested group structures (not supported)
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with best_practices_tabs[2]:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛡️ Security Guidelines
        
        **🔒 Access Control:**
        - Never use root account for daily operations
        - Implement strong password policy
        - Require MFA for sensitive operations
        - Use temporary credentials where possible
        
        **📝 Monitoring and Auditing:**
        - Enable CloudTrail for all API calls
        - Set up CloudWatch alarms for unusual activity
        - Regular access reviews and cleanup
        - Monitor failed authentication attempts
        
        **🚨 Incident Response:**
        - Have process for compromised credentials
        - Know how to quickly disable user access
        - Maintain emergency break-glass procedures
        - Document incident response procedures
        """)
        st.markdown('</div>', unsafe_allow_html=True)

def policy_interpretation_deep_dive_tab():
    """Content for Policy Interpretation Deep Dive! tab"""
    st.markdown("## 📋 Policy Interpretation Deep Dive!")
    st.markdown("*IAM Policies are the bedrock of strong IAM security. Understanding how policies work is critical for success*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Understanding IAM Policies
    IAM policies are JSON documents that define permissions. There are six types of AWS policies:
    - **Identity-based policies** - Attached to users, groups, or roles
    - **Resource-based policies** - Attached to resources (S3 buckets, etc.)
    - **Permissions boundaries** - Set maximum permissions for entities
    - **Organizations SCPs** - Service Control Policies for accounts
    - **ACLs** - Access Control Lists (legacy)
    - **Session policies** - Temporary permission constraints
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Policy Builder
    st.markdown("#### 🏗️ Interactive Policy Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Policy Configuration")
        policy_effect = st.selectbox("Effect", ["Allow", "Deny"])
        policy_action = st.multiselect("Actions", [
            "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
            "ec2:DescribeInstances", "ec2:StartInstances", "ec2:StopInstances",
            "rds:DescribeDBInstances", "iam:GetUser", "logs:CreateLogGroup"
        ], default=["s3:GetObject"])
        
        resource_arn = st.text_input("Resource ARN", value="arn:aws:s3:::my-bucket/*", key="resource_arn")
        
    with col2:
        st.markdown("##### Additional Options")
        add_condition = st.checkbox("Add Condition")
        if add_condition:
            condition_key = st.selectbox("Condition Key", [
                "aws:SourceIp", "aws:RequestedRegion", "aws:SecureTransport",
                "s3:ExistingObjectTag/Department", "StringEquals", "DateLessThan"
            ])
            condition_value = st.text_input("Condition Value", value="203.0.113.0/24")
        
        principal_type = st.selectbox("Principal (for resource-based policies)", [
            "None", "AWS Account", "IAM User", "IAM Role", "Service"
        ])
    
    # Generate policy JSON
    policy_json = generate_policy_json(policy_effect, policy_action, resource_arn, 
                                     add_condition, condition_key if add_condition else None, 
                                     condition_value if add_condition else None, principal_type)
    
    st.markdown("##### Generated Policy:")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code(policy_json, language='json')
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Policy Analysis
    analysis = analyze_policy(policy_json)
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Policy Analysis
    **Policy Type**: {analysis['type']}  
    **Security Level**: {analysis['security_level']}  
    **Scope**: {analysis['scope']}  
    **Recommendations**: {analysis['recommendations']}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Real-world Policy Examples
    st.markdown("#### 🌟 Real-world Policy Examples")
    
    example_tabs = st.tabs(["S3 Access Control", "EC2 Management", "Cross-Account Access", "Conditional Access"])
    
    with example_tabs[0]:
        st.markdown("##### S3 Bucket Access Control Policies")
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code("""
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowListBucket",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::company-data-bucket"
        },
        {
            "Sid": "AllowObjectAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": "arn:aws:s3:::company-data-bucket/user-data/${aws:username}/*",
            "Condition": {
                "StringEquals": {
                    "s3:ExistingObjectTag/Owner": "${aws:username}"
                }
            }
        },
        {
            "Sid": "DenyDeleteAccess",
            "Effect": "Deny",
            "Action": "s3:DeleteObject",
            "Resource": "arn:aws:s3:::company-data-bucket/*",
            "Condition": {
                "StringNotEquals": {
                    "aws:username": "admin"
                }
            }
        }
    ]
}

# Resource-based bucket policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CrossAccountAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123456789012:role/DataAnalysisRole"
            },
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::shared-analytics-bucket",
                "arn:aws:s3:::shared-analytics-bucket/*"
            ],
            "Condition": {
                "StringEquals": {
                    "s3:x-amz-server-side-encryption": "AES256"
                }
            }
        }
    ]
}
        """, language='json')
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="decision-box">', unsafe_allow_html=True)
        st.markdown("""
        **Policy Breakdown:**
        - **Statement 1**: Allows listing bucket contents and getting bucket location
        - **Statement 2**: Allows get/put operations only on objects under user's folder, and only if they own the object
        - **Statement 3**: Explicitly denies delete operations for non-admin users (explicit deny overrides any allow)
        - **Resource Policy**: Allows cross-account access from specific role with encryption requirement
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with example_tabs[1]:
        st.markdown("##### EC2 Instance Management Policies")
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code("""
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowEC2ReadAccess",
            "Effect": "Allow",
            "Action": [
                "ec2:Describe*",
                "ec2:Get*",
                "ec2:List*"
            ],
            "Resource": "*"
        },
        {
            "Sid": "AllowInstanceManagement",
            "Effect": "Allow",
            "Action": [
                "ec2:StartInstances",
                "ec2:StopInstances",
                "ec2:RebootInstances"
            ],
            "Resource": "arn:aws:ec2:*:*:instance/*",
            "Condition": {
                "StringEquals": {
                    "ec2:ResourceTag/Environment": ["Development", "Testing"]
                }
            }
        },
        {
            "Sid": "AllowCreateTaggedInstances",
            "Effect": "Allow",
            "Action": [
                "ec2:RunInstances"
            ],
            "Resource": [
                "arn:aws:ec2:*:*:instance/*",
                "arn:aws:ec2:*:*:volume/*",
                "arn:aws:ec2:*:*:network-interface/*"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": ["us-east-1", "us-west-2"],
                    "ec2:InstanceType": ["t3.micro", "t3.small", "t3.medium"]
                },
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": ["Environment", "Owner", "CostCenter"]
                }
            }
        },
        {
            "Sid": "AllowAMIAndKeyPairAccess",
            "Effect": "Allow",
            "Action": [
                "ec2:RunInstances"
            ],
            "Resource": [
                "arn:aws:ec2:*:*:image/*",
                "arn:aws:ec2:*:*:key-pair/*",
                "arn:aws:ec2:*:*:security-group/*",
                "arn:aws:ec2:*:*:subnet/*"
            ]
        },
        {
            "Sid": "DenyInstanceTermination",
            "Effect": "Deny",
            "Action": "ec2:TerminateInstances",
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "ec2:ResourceTag/Environment": "Production"
                }
            }
        }
    ]
}
        """, language='json')
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="decision-box">', unsafe_allow_html=True)
        st.markdown("""
        **Policy Features:**
        - **Read Access**: Full describe/get/list permissions for visibility
        - **Instance Control**: Start/stop/reboot limited to Dev/Test environments
        - **Launch Restrictions**: Only specific instance types in approved regions
        - **Tagging Requirements**: Mandatory tags for governance and billing
        - **Production Protection**: Explicit deny prevents terminating production instances
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with example_tabs[2]:
        st.markdown("##### Cross-Account Access Policies")
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code("""
# Trust Policy for Cross-Account Role
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::123456789012:role/DataScientistRole",
                    "arn:aws:iam::123456789012:user/john-analyst"
                ]
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "unique-external-id-12345"
                },
                "IpAddress": {
                    "aws:SourceIp": ["203.0.113.0/24", "198.51.100.0/24"]
                },
                "DateGreaterThan": {
                    "aws:CurrentTime": "2024-01-01T00:00:00Z"
                },
                "DateLessThan": {
                    "aws:CurrentTime": "2024-12-31T23:59:59Z"
                }
            }
        }
    ]
}

# Permission Policy for Cross-Account Role
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CrossAccountS3Access",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::shared-analytics-data",
                "arn:aws:s3:::shared-analytics-data/*"
            ]
        },
        {
            "Sid": "CrossAccountRedshiftAccess",
            "Effect": "Allow",
            "Action": [
                "redshift:DescribeClusters",
                "redshift:GetClusterCredentials"
            ],
            "Resource": "arn:aws:redshift:us-east-1:*:dbuser:analytics-cluster/analyst_${aws:userid}"
        },
        {
            "Sid": "LoggingAccess",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:log-group:/cross-account-access/*"
        }
    ]
}

# Resource-based policy example (S3 bucket in target account)
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CrossAccountBucketAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::987654321098:role/CrossAccountAnalysisRole"
            },
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::target-account-data",
                "arn:aws:s3:::target-account-data/shared/*"
            ],
            "Condition": {
                "StringEquals": {
                    "s3:x-amz-server-side-encryption": "aws:kms"
                },
                "StringLike": {
                    "s3:x-amz-server-side-encryption-aws-kms-key-id": "arn:aws:kms:us-east-1:*:key/*"
                }
            }
        }
    ]
}
        """, language='json')
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="decision-box">', unsafe_allow_html=True)
        st.markdown("""
        **Cross-Account Security Features:**
        - **External ID**: Additional security layer to prevent confused deputy problem
        - **IP Restrictions**: Limit access to specific network ranges
        - **Time Boundaries**: Temporary access with expiration dates
        - **Encryption Requirements**: Ensure data protection in transit and at rest
        - **User Mapping**: Dynamic username mapping for database access
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with example_tabs[3]:
        st.markdown("##### Conditional Access Policies")
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code("""
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "TimeBasedAccess",
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*",
            "Condition": {
                "DateGreaterThan": {
                    "aws:CurrentTime": "08:00Z"
                },
                "DateLessThan": {
                    "aws:CurrentTime": "18:00Z"
                },
                "ForAllValues:StringEquals": {
                    "aws:RequestedRegion": ["us-east-1", "us-west-2"]
                }
            }
        },
        {
            "Sid": "MFARequiredForSensitiveActions",
            "Effect": "Allow",
            "Action": [
                "iam:*",
                "ec2:TerminateInstances",
                "rds:DeleteDBInstance",
                "s3:DeleteBucket"
            ],
            "Resource": "*",
            "Condition": {
                "Bool": {
                    "aws:MultiFactorAuthPresent": "true"
                },
                "NumericLessThanEquals": {
                    "aws:MultiFactorAuthAge": "3600"
                }
            }
        },
        {
            "Sid": "IPBasedAccess",
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "ec2:*"
            ],
            "Resource": "*",
            "Condition": {
                "IpAddress": {
                    "aws:SourceIp": [
                        "203.0.113.0/24",
                        "198.51.100.0/24"
                    ]
                }
            }
        },
        {
            "Sid": "TagBasedAccess",
            "Effect": "Allow",
            "Action": [
                "ec2:StartInstances",
                "ec2:StopInstances"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "ec2:ResourceTag/Owner": "${aws:username}",
                    "ec2:ResourceTag/Department": "${aws:PrincipalTag/Department}"
                }
            }
        },
        {
            "Sid": "SSLOnlyAccess",
            "Effect": "Deny",
            "Action": "*",
            "Resource": "*",
            "Condition": {
                "Bool": {
                    "aws:SecureTransport": "false"
                }
            }
        },
        {
            "Sid": "ResourceTypeRestrictions",
            "Effect": "Allow",
            "Action": "ec2:RunInstances",
            "Resource": "*",
            "Condition": {
                "ForAllValues:StringEquals": {
                    "ec2:InstanceType": [
                        "t3.micro",
                        "t3.small",
                        "t3.medium"
                    ]
                },
                "StringLike": {
                    "ec2:ImageId": "ami-0abcdef1234567890"
                }
            }
        }
    ]
}
        """, language='json')
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="decision-box">', unsafe_allow_html=True)
        st.markdown("""
        **Conditional Access Features:**
        - **Time-based**: Restrict access to business hours (8 AM - 6 PM UTC)
        - **MFA Requirements**: Sensitive actions require recent MFA authentication
        - **IP Restrictions**: Limit access to corporate network ranges
        - **Tag-based Controls**: Users can only manage resources they own
        - **SSL Enforcement**: Deny all non-encrypted requests
        - **Resource Constraints**: Limit instance types and AMIs for cost control
        """)
        st.markdown('</div>', unsafe_allow_html=True)

def policy_interpretation_deny_vs_allow_tab():
    """Content for Policy Interpretation – Deny vs Allow tab"""
    st.markdown("## ⚖️ Policy Interpretation – Deny vs Allow")
    st.markdown("*Understanding the policy evaluation flow is critical for AWS security and exam success*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Policy Evaluation Fundamentals
    AWS evaluates policies in a specific order, and understanding this flow is crucial:
    - **Default**: All requests are **implicitly denied** (except root user)
    - **Explicit Allow**: Identity-based or resource-based policies can allow access
    - **Explicit Deny**: Any explicit deny always overrides any allow
    - **No Implicit Allow**: There is no such thing as implicit allow in AWS
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Policy Evaluation Flow
    st.markdown("#### 🔄 Policy Evaluation Decision Flow")
    common.mermaid(create_policy_evaluation_flow(), height=1200)
    
    # Interactive Policy Evaluator
    st.markdown("#### 🎮 Interactive Policy Evaluation Simulator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Request Details")
        principal = st.selectbox("Principal", ["IAM User", "IAM Role", "Root User", "Service"])
        action = st.selectbox("Action", [
            "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
            "ec2:DescribeInstances", "ec2:TerminateInstances",
            "iam:CreateUser", "rds:DeleteDBInstance"
        ])
        resource = st.text_input("Resource", value="arn:aws:s3:::my-bucket/file.txt")
        
    with col2:
        st.markdown("##### Policy Conditions")
        has_identity_allow = st.checkbox("Identity-based Policy Allow", value=True)
        has_resource_allow = st.checkbox("Resource-based Policy Allow", value=False)
        has_explicit_deny = st.checkbox("Explicit Deny Present", value=False)
        has_scp_allow = st.checkbox("SCP Allows Action", value=True)
        has_permission_boundary = st.checkbox("Permission Boundary Applied", value=False)
        boundary_allows = st.checkbox("Boundary Allows Action", value=True, disabled=not has_permission_boundary)
    
    # Evaluate the request
    evaluation_result = evaluate_policy_request(
        principal, action, resource, has_identity_allow, has_resource_allow,
        has_explicit_deny, has_scp_allow, has_permission_boundary, boundary_allows
    )
    
    # Display evaluation result
    if evaluation_result['decision'] == 'ALLOW':
        st.markdown('<div class="allow-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ ACCESS GRANTED
        **Decision**: {evaluation_result['decision']}  
        **Reason**: {evaluation_result['reason']}  
        **Evaluation Path**: {evaluation_result['path']}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="deny-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ❌ ACCESS DENIED
        **Decision**: {evaluation_result['decision']}  
        **Reason**: {evaluation_result['reason']}  
        **Evaluation Path**: {evaluation_result['path']}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Policy Types Deep Dive
    st.markdown("#### 📚 Six Types of AWS Policies")
    
    policy_types_col1, policy_types_col2 = st.columns(2)
    
    with policy_types_col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 1. 🆔 Identity-based Policies
        **Attached to**: Users, Groups, or Roles  
        **Purpose**: Define what actions the identity can perform  
        **Evaluation**: First in the evaluation chain  
        
        **Example**: Policy attached to a user allowing S3 access
        
        ### 2. 📦 Resource-based Policies
        **Attached to**: Resources (S3 buckets, KMS keys, etc.)  
        **Purpose**: Define who can access the resource  
        **Evaluation**: Can grant access even without identity policy  
        
        **Example**: S3 bucket policy allowing cross-account access
        
        ### 3. 🔒 Permissions Boundaries
        **Attached to**: Users or Roles  
        **Purpose**: Set maximum permissions (filter)  
        **Evaluation**: Can only restrict, never grant additional permissions  
        
        **Example**: Ensure developers can't create IAM users
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with policy_types_col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 4. 🏢 Organizations SCPs
        **Attached to**: AWS Organizations accounts/OUs  
        **Purpose**: Central governance and compliance  
        **Evaluation**: Can only restrict, never grant permissions  
        
        **Example**: Prevent any account from leaving specific regions
        
        ### 5. 📋 Access Control Lists (ACLs)
        **Attached to**: S3 buckets and objects  
        **Purpose**: Legacy access control mechanism  
        **Evaluation**: Evaluated independently from IAM  
        
        **Example**: S3 object ACL granting public read access
        
        ### 6. 🎭 Session Policies
        **Used with**: Temporary credentials (AssumeRole)  
        **Purpose**: Further restrict permissions for the session  
        **Evaluation**: Can only restrict, never grant additional permissions  
        
        **Example**: Limit assumed role to specific S3 bucket only
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Common Policy Evaluation Scenarios
    st.markdown("#### 🎯 Common Evaluation Scenarios")
    
    scenario_tabs = st.tabs(["Implicit Deny", "Explicit Deny Wins", "Cross-Account Access", "Permission Boundaries"])
    
    with scenario_tabs[0]:
        st.markdown("##### Scenario 1: Implicit Deny (Default)")
        
        st.markdown('<div class="deny-box">', unsafe_allow_html=True)
        st.markdown("""
        ### ❌ Result: IMPLICIT DENY
        **Situation**: User tries to access S3 bucket but has no policies attached  
        **Evaluation**: No explicit allow found → Default to implicit deny  
        **Lesson**: All access starts with implicit deny - you must explicitly allow
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code("""
# User has NO policies attached
# Request: s3:GetObject on arn:aws:s3:::my-bucket/file.txt

Evaluation Flow:
1. Default: IMPLICIT DENY
2. Check Identity-based policies: NONE FOUND
3. Check Resource-based policies: NONE FOUND  
4. No explicit allows found
→ Result: ACCESS DENIED (Implicit Deny)

# To fix this, attach a policy like:
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::my-bucket/*"
        }
    ]
}
        """, language='json')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with scenario_tabs[1]:
        st.markdown("##### Scenario 2: Explicit Deny Always Wins")
        
        st.markdown('<div class="deny-box">', unsafe_allow_html=True)
        st.markdown("""
        ### ❌ Result: EXPLICIT DENY OVERRIDES ALLOW
        **Situation**: User has both Allow and Deny policies for the same action  
        **Evaluation**: Explicit deny found → Access denied regardless of allows  
        **Lesson**: Use explicit deny for strong security boundaries
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code("""
# User has TWO policies attached:

# Policy 1: Allow S3 Access
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": "*"
        }
    ]
}

# Policy 2: Deny Production Bucket Access  
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Deny",
            "Action": "s3:*",
            "Resource": "arn:aws:s3:::production-bucket/*",
            "Condition": {
                "StringNotEquals": {
                    "aws:username": "admin"
                }
            }
        }
    ]
}

# Request: s3:GetObject on arn:aws:s3:::production-bucket/file.txt

Evaluation Flow:
1. Check for explicit allows: FOUND (Policy 1)
2. Check for explicit denies: FOUND (Policy 2)
→ Result: ACCESS DENIED (Explicit Deny Wins)
        """, language='json')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with scenario_tabs[2]:
        st.markdown("##### Scenario 3: Cross-Account Access")
        
        st.markdown('<div class="allow-box">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Result: RESOURCE-BASED POLICY GRANTS ACCESS
        **Situation**: External user accesses resource via resource-based policy  
        **Evaluation**: Resource-based policy can grant access without identity policy  
        **Lesson**: Resource-based policies enable cross-account access
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code("""
# Account A: User with NO S3 policies
# Account B: S3 bucket with resource-based policy

# S3 Bucket Policy (Account B):
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CrossAccountAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::ACCOUNT-A:user/external-user"
            },
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::cross-account-bucket",
                "arn:aws:s3:::cross-account-bucket/*"
            ]
        }
    ]
}

# Request: User from Account A tries s3:GetObject

Evaluation Flow:
1. Check Identity-based policies: NONE (user has no policies)
2. Check Resource-based policies: FOUND (bucket policy allows)
3. No explicit denies found
→ Result: ACCESS GRANTED (Resource-based Policy)
        """, language='json')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with scenario_tabs[3]:
        st.markdown("##### Scenario 4: Permissions Boundaries")
        
        st.markdown('<div class="deny-box">', unsafe_allow_html=True)
        st.markdown("""
        ### ❌ Result: PERMISSION BOUNDARY BLOCKS ACCESS
        **Situation**: User has allow policy but permission boundary restricts action  
        **Evaluation**: Both identity policy AND boundary must allow  
        **Lesson**: Permission boundaries are filters that can only restrict
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code("""
# User Identity Policy (Attached to User):
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*"
        }
    ]
}

# Permission Boundary (Applied to User):
{
    "Version": "2012-10-17", 
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "ec2:Describe*",
                "rds:Describe*"
            ],
            "Resource": "*"
        }
    ]
}

# Request: iam:CreateUser (create IAM user)

Evaluation Flow:
1. Check Identity policy: ALLOWS (wildcard allows all)
2. Check Permission Boundary: DOES NOT ALLOW (iam actions not listed)
3. Both must allow for access → Boundary blocks
→ Result: ACCESS DENIED (Permission Boundary Restriction)

# The user CAN do s3:GetObject because:
# - Identity policy allows it (wildcard)
# - Permission boundary allows it (s3:* included)
# Both conditions are satisfied ✓
        """, language='json')
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Practical Exam Tips
    st.markdown("#### 🎓 AWS Certification Exam Tips")
    
    exam_tips_col1, exam_tips_col2 = st.columns(2)
    
    with exam_tips_col1:
        st.markdown('<div class="warning-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🧠 Key Points to Remember
        
        **Policy Evaluation Order:**
        1. Default: Implicit Deny
        2. Identity-based policies (can allow)
        3. Resource-based policies (can allow)  
        4. Permission boundaries (filter only)
        5. SCPs (filter only)
        6. Session policies (filter only)
        7. Explicit deny check (always wins)
        
        **Important Rules:**
        - Explicit deny always overrides allow
        - No such thing as implicit allow
        - Root user bypasses all IAM policies (except SCPs)
        - Resource-based policies can grant cross-account access
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with exam_tips_col2:
        st.markdown('<div class="warning-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 📝 Common Exam Question Patterns
        
        **Scenario-based Questions:**
        - "User can't access S3 bucket despite having policy"
        - "How to prevent developers from creating IAM users?"
        - "Cross-account access not working"
        - "Resource-based vs identity-based policies"
        
        **Answer Strategy:**
        1. Identify all policies involved
        2. Check for explicit denies first
        3. Verify both identity and resource policies
        4. Consider permission boundaries/SCPs
        5. Remember: explicit deny always wins
        
        **Red Flag Words:**
        - "Implicit allow" (doesn't exist)
        - "Deny overrides deny" (deny always wins)
        - "Root affected by IAM" (only SCPs affect root)
        """)
        st.markdown('</div>', unsafe_allow_html=True)

# Helper Functions
def simulate_iam_access(entity_type, entity_name, action, resource):
    """Simulate IAM access decision"""
    if entity_type == "Root User":
        return {
            'allowed': True,
            'reason': 'Root user has full access (except SCP restrictions)'
        }
    
    # Simple simulation logic
    if 'GetObject' in action or 'Describe' in action:
        return {
            'allowed': True,
            'reason': 'Read operations typically allowed with basic policies'
        }
    elif 'Delete' in action or 'Terminate' in action:
        return {
            'allowed': False,
            'reason': 'Destructive operations require explicit permissions'
        }
    else:
        return {
            'allowed': False,
            'reason': 'No explicit allow policy found - implicit deny'
        }

def create_user_configuration(username, access_types, tags):
    """Create user configuration object"""
    return {
        'username': username,
        'access_types': access_types,
        'tags': tags,
        'created_at': st.session_state.get('session_started', True)
    }

def create_group_configuration(groupname, description, policies):
    """Create group configuration object"""
    return {
        'name': groupname,
        'description': description,
        'policies': policies,
        'created_at': st.session_state.get('session_started', True)
    }

def create_access_pattern_data():
    """Create sample data for access patterns visualization"""
    return pd.DataFrame({
        'Pattern': [
            'Single User Direct Policy',
            'Group-based Permissions',
            'Role-based Access',
            'Cross-account Access',
            'Service-to-service',
            'Temporary Access'
        ],
        'Usage_Percentage': [15, 45, 25, 8, 5, 2],
        'Complexity': ['Low', 'Low', 'Medium', 'High', 'Medium', 'High']
    })

def generate_policy_json(effect, actions, resource, add_condition, condition_key, condition_value, principal_type):
    """Generate policy JSON based on inputs"""
    statement = {
        "Effect": effect,
        "Action": actions,
        "Resource": resource
    }
    
    if principal_type != "None":
        principal_map = {
            "AWS Account": "arn:aws:iam::123456789012:root",
            "IAM User": "arn:aws:iam::123456789012:user/example-user",
            "IAM Role": "arn:aws:iam::123456789012:role/example-role",
            "Service": "s3.amazonaws.com"
        }
        statement["Principal"] = {
            "AWS" if principal_type != "Service" else "Service": principal_map[principal_type]
        }
    
    if add_condition and condition_key and condition_value:
        statement["Condition"] = {
            "StringEquals" if "StringEquals" in condition_key else "IpAddress": {
                condition_key: condition_value
            }
        }
    
    policy = {
        "Version": "2012-10-17",
        "Statement": [statement]
    }
    
    return json.dumps(policy, indent=2)

def analyze_policy(policy_json):
    """Analyze policy and provide feedback"""
    try:
        policy = json.loads(policy_json)
        statement = policy['Statement'][0]
        
        # Determine policy type
        policy_type = "Resource-based" if 'Principal' in statement else "Identity-based"
        
        # Assess security level
        if statement['Effect'] == 'Deny':
            security_level = "High - Explicit Deny"
        elif '*' in str(statement.get('Action', [])):
            security_level = "Low - Wildcard Actions"
        else:
            security_level = "Medium - Specific Actions"
        
        # Determine scope
        if statement.get('Resource') == '*':
            scope = "All Resources"
        else:
            scope = "Specific Resources"
        
        # Generate recommendations
        recommendations = []
        if '*' in str(statement.get('Action', [])):
            recommendations.append("Consider limiting to specific actions")
        if statement.get('Resource') == '*':
            recommendations.append("Consider limiting to specific resources")
        if 'Condition' not in statement:
            recommendations.append("Consider adding conditions for enhanced security")
        
        return {
            'type': policy_type,
            'security_level': security_level,
            'scope': scope,
            'recommendations': ', '.join(recommendations) if recommendations else "Policy looks good"
        }
    except:
        return {
            'type': "Invalid JSON",
            'security_level': "Cannot analyze",
            'scope': "Cannot analyze", 
            'recommendations': "Fix JSON syntax errors"
        }

def evaluate_policy_request(principal, action, resource, identity_allow, resource_allow, 
                          explicit_deny, scp_allow, has_boundary, boundary_allows):
    """Evaluate policy request through AWS decision flow"""
    
    evaluation_path = []
    
    # Step 1: Default implicit deny
    evaluation_path.append("1. Default: Implicit Deny")
    
    # Step 2: Check for explicit deny first (always wins)
    if explicit_deny:
        return {
            'decision': 'DENY',
            'reason': 'Explicit deny policy found - overrides any allows',
            'path': ' → '.join(evaluation_path + ["2. Explicit Deny Found → ACCESS DENIED"])
        }
    
    # Step 3: Check for allows
    has_allow = False
    
    if identity_allow:
        evaluation_path.append("2. Identity-based Policy: ALLOW")
        has_allow = True
    elif resource_allow:
        evaluation_path.append("2. Resource-based Policy: ALLOW") 
        has_allow = True
    else:
        evaluation_path.append("2. No Allow Policies Found")
    
    if not has_allow:
        return {
            'decision': 'DENY',
            'reason': 'No explicit allow policies found - implicit deny applies',
            'path': ' → '.join(evaluation_path + ["→ ACCESS DENIED"])
        }
    
    # Step 4: Check permission boundary
    if has_boundary:
        if boundary_allows:
            evaluation_path.append("3. Permission Boundary: ALLOWS")
        else:
            evaluation_path.append("3. Permission Boundary: BLOCKS")
            return {
                'decision': 'DENY',
                'reason': 'Permission boundary does not allow this action',
                'path': ' → '.join(evaluation_path + ["→ ACCESS DENIED"])
            }
    
    # Step 5: Check SCP
    if not scp_allow:
        evaluation_path.append("4. SCP: BLOCKS")
        return {
            'decision': 'DENY',
            'reason': 'Service Control Policy does not allow this action',
            'path': ' → '.join(evaluation_path + ["→ ACCESS DENIED"])
        }
    else:
        evaluation_path.append("4. SCP: ALLOWS")
    
    # Step 6: Final check - no explicit deny found
    evaluation_path.append("5. No Explicit Deny → ACCESS GRANTED")
    
    return {
        'decision': 'ALLOW',
        'reason': 'All policy checks passed - access granted',
        'path': ' → '.join(evaluation_path)
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
    # 🔐 AWS IAM Security & Access Management
    <div class='decision-box'>
    Master AWS Identity and Access Management (IAM) concepts, policy interpretation, and security best practices for controlling access to AWS resources.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🔐 What is IAM?",
        "👥 IAM Users and Groups", 
        "📋 Policy Interpretation Deep Dive!",
        "⚖️ Policy Interpretation – Deny vs Allow"
    ])
    
    with tab1:
        what_is_iam_tab()
    
    with tab2:
        iam_users_and_groups_tab()
    
    with tab3:
        policy_interpretation_deep_dive_tab()
    
    with tab4:
        policy_interpretation_deny_vs_allow_tab()
    
    # Footer
    st.markdown("""
    <div class="footer">
        <p>© 2025, Amazon Web Services, Inc. or its affiliates. All rights reserved.</p>
    </div>
    """, unsafe_allow_html=True)

# Main execution flow
if __name__ == "__main__":
    if 'localhost' in st.context.headers.get("host", "localhost"):
        main()
    else:
        # First check authentication
        is_authenticated = authenticate.login()
        
        # If authenticated, show the main app content
        if is_authenticated:
            main()
