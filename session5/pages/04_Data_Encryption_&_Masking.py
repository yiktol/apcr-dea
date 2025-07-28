
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import utils.common as common
import utils.authenticate as authenticate
import json
import base64
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="AWS Data Security & Encryption",
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
        
        .security-card {{
            background: linear-gradient(135deg, {AWS_COLORS['success']} 0%, {AWS_COLORS['light_blue']} 100%);
            padding: 20px;
            border-radius: 15px;
            color: white;
            margin: 15px 0;
        }}
        
        .warning-card {{
            background: linear-gradient(135deg, {AWS_COLORS['warning']} 0%, {AWS_COLORS['primary']} 100%);
            padding: 20px;
            border-radius: 15px;
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
        
        .encryption-feature {{
            background: white;
            padding: 15px;
            border-radius: 10px;
            border: 2px solid {AWS_COLORS['success']};
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
            border-left: 5px solid {AWS_COLORS['warning']};
        }}
        
        .danger-box {{
            background-color: #F8D7DA;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 15px;
            border-left: 5px solid {AWS_COLORS['danger']};
        }}
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    common.initialize_session_state()
    if 'session_started' not in st.session_state:
        st.session_state.session_started = True
        st.session_state.encryption_policies = []
        st.session_state.kms_keys = []
        st.session_state.security_score = 0

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🔐 AWS Key Management Service (KMS) - Centralized key management and encryption
            - 🏢 Amazon Redshift Encryption - Database encryption at rest and in transit
            - 📊 AWS Glue Data Catalog Encryption - Metadata protection and security
            - 🔥 Amazon EMR Encryption - Big data processing security
            
            **Learning Objectives:**
            - Master AWS KMS for comprehensive key management
            - Implement Redshift encryption best practices
            - Secure Glue Data Catalog with proper encryption
            - Configure EMR encryption for big data workloads
            """)

def create_kms_architecture():
    """Create KMS architecture diagram"""
    return """
    graph TB
        subgraph "AWS KMS Service"
            KMS[AWS Key Management Service]
            CMK[Customer Master Keys]
            HSM[Hardware Security Module]
        end
        
        subgraph "Key Types"
            AWS_MANAGED[AWS Managed Keys]
            CUSTOMER_MANAGED[Customer Managed Keys]
            CUSTOMER_PROVIDED[Customer Provided Keys]
        end
        
        subgraph "AWS Services"
            S3[Amazon S3]
            RDS[Amazon RDS]
            REDSHIFT[Amazon Redshift]
            GLUE[AWS Glue]
            EMR[Amazon EMR]
            EBS[Amazon EBS]
        end
        
        subgraph "Operations"
            ENCRYPT[Encrypt Data]
            DECRYPT[Decrypt Data]
            GENERATE[Generate Keys]
            ROTATE[Key Rotation]
            AUDIT[CloudTrail Audit]
        end
        
        KMS --> CMK
        CMK --> HSM
        
        CMK --> AWS_MANAGED
        CMK --> CUSTOMER_MANAGED
        CMK --> CUSTOMER_PROVIDED
        
        CUSTOMER_MANAGED --> S3
        CUSTOMER_MANAGED --> RDS
        CUSTOMER_MANAGED --> REDSHIFT
        CUSTOMER_MANAGED --> GLUE
        CUSTOMER_MANAGED --> EMR
        CUSTOMER_MANAGED --> EBS
        
        KMS --> ENCRYPT
        KMS --> DECRYPT
        KMS --> GENERATE
        KMS --> ROTATE
        KMS --> AUDIT
        
        style KMS fill:#FF9900,stroke:#232F3E,color:#fff
        style CMK fill:#4B9EDB,stroke:#232F3E,color:#fff
        style HSM fill:#3FB34F,stroke:#232F3E,color:#fff
        style CUSTOMER_MANAGED fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_redshift_encryption_flow():
    """Create Redshift encryption flow diagram"""
    return """
    graph TB
        subgraph "Data Sources"
            APP[Applications]
            ETL[ETL Processes]
            BI[BI Tools]
        end
        
        subgraph "Encryption in Transit"
            SSL[SSL/TLS Connection]
            VPC[VPC Endpoints]
        end
        
        subgraph "Amazon Redshift Cluster"
            LEADER[Leader Node]
            COMPUTE[Compute Nodes]
            
            subgraph "Encryption at Rest"
                AES[AES-256 Encryption]
                KMS_KEY[KMS Customer Key]
                DATA_BLOCKS[Encrypted Data Blocks]
            end
        end
        
        subgraph "Storage"
            S3_BACKUP[S3 Encrypted Backups]
            SNAPSHOTS[Encrypted Snapshots]
        end
        
        subgraph "Monitoring"
            CLOUDTRAIL[CloudTrail Logs]
            CLOUDWATCH[CloudWatch Metrics]
        end
        
        APP --> SSL
        ETL --> SSL
        BI --> SSL
        
        SSL --> VPC
        VPC --> LEADER
        
        LEADER --> COMPUTE
        COMPUTE --> AES
        AES --> KMS_KEY
        KMS_KEY --> DATA_BLOCKS
        
        DATA_BLOCKS --> S3_BACKUP
        DATA_BLOCKS --> SNAPSHOTS
        
        KMS_KEY --> CLOUDTRAIL
        AES --> CLOUDWATCH
        
        style SSL fill:#3FB34F,stroke:#232F3E,color:#fff
        style AES fill:#FF9900,stroke:#232F3E,color:#fff
        style KMS_KEY fill:#4B9EDB,stroke:#232F3E,color:#fff
        style DATA_BLOCKS fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_glue_encryption_diagram():
    """Create Glue encryption architecture diagram"""
    return """
    graph TB
        subgraph "Data Sources"
            JDBC[JDBC Sources]
            S3_SRC[S3 Data Sources]
            STREAMING[Streaming Data]
        end
        
        subgraph "AWS Glue Service"
            CRAWLER[Glue Crawlers]
            CATALOG[Data Catalog]
            JOBS[Glue Jobs]
            
            subgraph "Encryption Components"
                CATALOG_ENC[Catalog Encryption]
                JOB_ENC[Job Encryption]
                CONN_ENC[Connection Encryption]
            end
        end
        
        subgraph "Encryption at Rest"
            KMS_CATALOG[KMS for Catalog]
            KMS_JOBS[KMS for Jobs]
            S3_ENC[S3 Encryption]
        end
        
        subgraph "Encryption in Transit"
            SSL_CONN[SSL Connections]
            TLS_COMM[TLS Communication]
        end
        
        subgraph "Output"
            TARGET_S3[Encrypted S3 Output]
            DATA_WAREHOUSE[Encrypted Data Warehouse]
        end
        
        JDBC --> CRAWLER
        S3_SRC --> CRAWLER
        STREAMING --> JOBS
        
        CRAWLER --> CATALOG
        CATALOG --> JOBS
        
        CATALOG --> CATALOG_ENC
        JOBS --> JOB_ENC
        CRAWLER --> CONN_ENC
        
        CATALOG_ENC --> KMS_CATALOG
        JOB_ENC --> KMS_JOBS
        CONN_ENC --> SSL_CONN
        
        JOB_ENC --> S3_ENC
        SSL_CONN --> TLS_COMM
        
        JOBS --> TARGET_S3
        JOBS --> DATA_WAREHOUSE
        
        style CATALOG fill:#FF9900,stroke:#232F3E,color:#fff
        style CATALOG_ENC fill:#3FB34F,stroke:#232F3E,color:#fff
        style KMS_CATALOG fill:#4B9EDB,stroke:#232F3E,color:#fff
        style SSL_CONN fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_emr_encryption_architecture():
    """Create EMR encryption architecture diagram"""
    return """
    graph TB
        subgraph "Data Sources"
            HDFS_SRC[HDFS Data]
            S3_DATA[S3 Data]
            EXTERNAL[External Sources]
        end
        
        subgraph "EMR Cluster"
            MASTER[Master Node]
            CORE[Core Nodes]
            TASK[Task Nodes]
            
            subgraph "Encryption Types"
                EBS_ENC[EBS Encryption]
                LOCAL_ENC[Local Disk Encryption]
                TRANSIT_ENC[In-Transit Encryption]
            end
        end
        
        subgraph "Storage Encryption"
            HDFS_ENC[HDFS Encryption]
            S3_ENC[S3 Server-Side Encryption]
            EMR_FS[EMRFS Encryption]
        end
        
        subgraph "Application Layer"
            SPARK[Apache Spark]
            HIVE[Apache Hive]
            HADOOP[Hadoop MapReduce]
            HBASE[Apache HBase]
        end
        
        subgraph "Key Management"
            KMS_EMR[KMS Keys]
            CUSTOM_KEYS[Custom Provider]
            HDFS_KEYS[HDFS Encryption Zones]
        end
        
        subgraph "Output"
            ENCRYPTED_OUTPUT[Encrypted Results]
            SECURE_S3[Secure S3 Storage]
        end
        
        HDFS_SRC --> MASTER
        S3_DATA --> CORE
        EXTERNAL --> TASK
        
        MASTER --> EBS_ENC
        CORE --> LOCAL_ENC
        TASK --> TRANSIT_ENC
        
        EBS_ENC --> HDFS_ENC
        LOCAL_ENC --> S3_ENC
        TRANSIT_ENC --> EMR_FS
        
        HDFS_ENC --> SPARK
        S3_ENC --> HIVE
        EMR_FS --> HADOOP
        TRANSIT_ENC --> HBASE
        
        SPARK --> KMS_EMR
        HIVE --> CUSTOM_KEYS
        HADOOP --> HDFS_KEYS
        
        KMS_EMR --> ENCRYPTED_OUTPUT
        CUSTOM_KEYS --> SECURE_S3
        
        style MASTER fill:#FF9900,stroke:#232F3E,color:#fff
        style EBS_ENC fill:#3FB34F,stroke:#232F3E,color:#fff
        style KMS_EMR fill:#4B9EDB,stroke:#232F3E,color:#fff
        style ENCRYPTED_OUTPUT fill:#232F3E,stroke:#FF9900,color:#fff
    """

def kms_tab():
    """AWS Key Management Service tab content"""
    st.markdown("## 🔐 AWS Key Management Service (KMS)")
    st.markdown("*Centralized key management and encryption for AWS services and applications*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 KMS Core Capabilities
    AWS KMS is a managed service that helps you create and control cryptographic keys:
    - **Centralized Key Management**: Create, manage, and control encryption keys
    - **Hardware Security Modules**: Keys are protected by FIPS 140-2 Level 2 HSMs
    - **Service Integration**: Seamlessly integrates with 100+ AWS services
    - **Audit Trail**: All key usage is logged in AWS CloudTrail
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # KMS Architecture
    st.markdown("#### 🏗️ KMS Architecture Overview")
    common.mermaid(create_kms_architecture(), height=700)
    
    # Interactive KMS configuration
    st.markdown("#### 🎨 KMS Key Configuration Simulator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        key_type = st.selectbox("Key Type", ["Customer Managed", "AWS Managed", "Customer Provided"])
        key_usage = st.selectbox("Key Usage", ["ENCRYPT_DECRYPT", "SIGN_VERIFY", "GENERATE_VERIFY_MAC"])
        key_spec = st.selectbox("Key Spec", ["SYMMETRIC_DEFAULT", "RSA_2048", "RSA_3072", "RSA_4096", "ECC_NIST_P256"])
        
    with col2:
        key_origin = st.selectbox("Key Origin", ["AWS_KMS", "EXTERNAL", "AWS_CLOUDHSM"])
        rotation_enabled = st.checkbox("Enable Automatic Rotation", value=True)
        multi_region = st.checkbox("Multi-Region Key", value=False)
    
    # Generate key policy
    key_policy = generate_key_policy(key_type, key_usage, rotation_enabled)
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 🔑 Generated KMS Key Configuration
    **Key Type**: {key_type}  
    **Key Usage**: {key_usage}  
    **Key Spec**: {key_spec}  
    **Auto Rotation**: {'Enabled' if rotation_enabled else 'Disabled'}  
    **Multi-Region**: {'Yes' if multi_region else 'No'}  
    **Estimated Cost**: ${calculate_kms_cost(key_type, rotation_enabled, multi_region):.2f}/month
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Key management best practices
    st.markdown("#### 📋 KMS Best Practices")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="encryption-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔐 Key Security
        - **Principle of Least Privilege**
        - **Separate keys per environment**
        - **Key rotation policies**
        - **Cross-account access control**
        - **Regular access reviews**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="encryption-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Monitoring & Auditing
        - **CloudTrail integration**
        - **CloudWatch metrics**
        - **Key usage tracking**
        - **Access pattern analysis**
        - **Compliance reporting**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="encryption-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏢 Organizational Controls
        - **Key policies and IAM**
        - **Service control policies**
        - **Multi-region strategies**
        - **Backup and recovery**
        - **Cost optimization**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # KMS key types comparison
    st.markdown("#### 🔧 KMS Key Types Comparison")
    
    key_types_data = {
        'Key Type': ['AWS Managed', 'Customer Managed', 'Customer Provided'],
        'Control Level': ['Limited', 'Full', 'Full'],
        'Rotation': ['Automatic (3 years)', 'Configurable', 'Manual'],
        'Cost': ['Free', '$1/month + usage', '$1/month + usage'],
        'Cross-Account': ['No', 'Yes', 'Yes'],
        'Key Policy': ['AWS Managed', 'Customer Managed', 'Customer Managed'],
        'Best For': ['Simple use cases', 'Production workloads', 'Compliance requirements']
    }
    
    df_key_types = pd.DataFrame(key_types_data)
    st.dataframe(df_key_types, use_container_width=True)
    
    # Code examples
    st.markdown("#### 💻 KMS Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Key Creation", "Encryption Operations", "Key Management"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code(f'''
# AWS KMS Key Creation and Management

import boto3
import json
from botocore.exceptions import ClientError

# Initialize KMS client
kms_client = boto3.client('kms')

def create_customer_managed_key(key_description, key_usage='ENCRYPT_DECRYPT'):
    """Create a new customer-managed KMS key"""
    
    try:
        # Define key policy
        key_policy = {{
            "Version": "2012-10-17",
            "Statement": [
                {{
                    "Sid": "Enable IAM User Permissions",
                    "Effect": "Allow",
                    "Principal": {{
                        "AWS": "arn:aws:iam::123456789012:root"
                    }},
                    "Action": "kms:*",
                    "Resource": "*"
                }},
                {{
                    "Sid": "Allow use of the key",
                    "Effect": "Allow",
                    "Principal": {{
                        "AWS": [
                            "arn:aws:iam::123456789012:role/DataEngineerRole",
                            "arn:aws:iam::123456789012:role/RedshiftServiceRole"
                        ]
                    }},
                    "Action": [
                        "kms:Encrypt",
                        "kms:Decrypt",
                        "kms:ReEncrypt*",
                        "kms:GenerateDataKey*",
                        "kms:DescribeKey"
                    ],
                    "Resource": "*"
                }},
                {{
                    "Sid": "Allow attachment of persistent resources",
                    "Effect": "Allow",
                    "Principal": {{
                        "AWS": [
                            "arn:aws:iam::123456789012:role/DataEngineerRole"
                        ]
                    }},
                    "Action": [
                        "kms:CreateGrant",
                        "kms:ListGrants",
                        "kms:RevokeGrant"
                    ],
                    "Resource": "*",
                    "Condition": {{
                        "Bool": {{
                            "kms:GrantIsForAWSResource": "true"
                        }}
                    }}
                }}
            ]
        }}
        
        # Create the key
        response = kms_client.create_key(
            Description=key_description,
            KeyUsage=key_usage,
            KeySpec='SYMMETRIC_DEFAULT',
            Origin='AWS_KMS',
            MultiRegion=False,
            Policy=json.dumps(key_policy),
            Tags=[
                {{
                    'TagKey': 'Environment',
                    'TagValue': 'Production'
                }},
                {{
                    'TagKey': 'Purpose',
                    'TagValue': 'DataEncryption'
                }},
                {{
                    'TagKey': 'Owner',
                    'TagValue': 'DataTeam'
                }}
            ]
        )
        
        key_id = response['KeyMetadata']['KeyId']
        key_arn = response['KeyMetadata']['Arn']
        
        print(f"✅ KMS Key created successfully:")
        print(f"   Key ID: {{key_id}}")
        print(f"   Key ARN: {{key_arn}}")
        
        return key_id, key_arn
        
    except ClientError as e:
        print(f"❌ Error creating KMS key: {{e}}")
        return None, None

def create_key_alias(key_id, alias_name):
    """Create an alias for the KMS key"""
    
    try:
        kms_client.create_alias(
            AliasName=f'alias/{{alias_name}}',
            TargetKeyId=key_id
        )
        print(f"✅ Alias created: alias/{{alias_name}}")
        
    except ClientError as e:
        print(f"❌ Error creating alias: {{e}}")

def enable_key_rotation(key_id):
    """Enable automatic key rotation"""
    
    try:
        kms_client.enable_key_rotation(KeyId=key_id)
        print(f"✅ Key rotation enabled for {{key_id}}")
        
    except ClientError as e:
        print(f"❌ Error enabling rotation: {{e}}")

def get_key_rotation_status(key_id):
    """Check key rotation status"""
    
    try:
        response = kms_client.get_key_rotation_status(KeyId=key_id)
        return response['KeyRotationEnabled']
        
    except ClientError as e:
        print(f"❌ Error checking rotation status: {{e}}")
        return False

# Example usage - Create keys for different services
def setup_data_encryption_keys():
    """Set up KMS keys for data services"""
    
    print("🔐 Setting up AWS KMS keys for data encryption")
    print("=" * 50)
    
    # Create key for Redshift
    redshift_key_id, redshift_key_arn = create_customer_managed_key(
        "Redshift cluster encryption key",
        "ENCRYPT_DECRYPT"
    )
    
    if redshift_key_id:
        create_key_alias(redshift_key_id, "redshift-encryption-key")
        enable_key_rotation(redshift_key_id)
    
    # Create key for Glue Data Catalog
    glue_key_id, glue_key_arn = create_customer_managed_key(
        "Glue Data Catalog encryption key",
        "ENCRYPT_DECRYPT"
    )
    
    if glue_key_id:
        create_key_alias(glue_key_id, "glue-catalog-encryption-key")
        enable_key_rotation(glue_key_id)
    
    # Create key for EMR
    emr_key_id, emr_key_arn = create_customer_managed_key(
        "EMR cluster encryption key",
        "ENCRYPT_DECRYPT"
    )
    
    if emr_key_id:
        create_key_alias(emr_key_id, "emr-encryption-key")
        enable_key_rotation(emr_key_id)
    
    return {{
        'redshift': {{'key_id': redshift_key_id, 'key_arn': redshift_key_arn}},
        'glue': {{'key_id': glue_key_id, 'key_arn': glue_key_arn}},
        'emr': {{'key_id': emr_key_id, 'key_arn': emr_key_arn}}
    }}

# Run the setup
encryption_keys = setup_data_encryption_keys()

# Display results
for service, key_info in encryption_keys.items():
    if key_info['key_id']:
        rotation_enabled = get_key_rotation_status(key_info['key_id'])
        print(f"\\n{{service.upper()}} Encryption Key:")
        print(f"  Key ID: {{key_info['key_id']}}")
        print(f"  Rotation: {{'Enabled' if rotation_enabled else 'Disabled'}}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# KMS Encryption and Decryption Operations

import boto3
import base64
from botocore.exceptions import ClientError

kms_client = boto3.client('kms')

def encrypt_data(key_id, plaintext_data, encryption_context=None):
    """Encrypt data using KMS"""
    
    try:
        # Convert string to bytes if necessary
        if isinstance(plaintext_data, str):
            plaintext_data = plaintext_data.encode('utf-8')
        
        # Encrypt the data
        response = kms_client.encrypt(
            KeyId=key_id,
            Plaintext=plaintext_data,
            EncryptionContext=encryption_context or {}
        )
        
        # Return base64 encoded ciphertext
        ciphertext_blob = response['CiphertextBlob']
        encoded_ciphertext = base64.b64encode(ciphertext_blob).decode('utf-8')
        
        print(f"✅ Data encrypted successfully")
        print(f"   Key ID: {response['KeyId']}")
        print(f"   Ciphertext (base64): {encoded_ciphertext[:50]}...")
        
        return encoded_ciphertext
        
    except ClientError as e:
        print(f"❌ Encryption error: {e}")
        return None

def decrypt_data(ciphertext_blob, encryption_context=None):
    """Decrypt data using KMS"""
    
    try:
        # Decode base64 ciphertext
        if isinstance(ciphertext_blob, str):
            ciphertext_blob = base64.b64decode(ciphertext_blob)
        
        # Decrypt the data
        response = kms_client.decrypt(
            CiphertextBlob=ciphertext_blob,
            EncryptionContext=encryption_context or {}
        )
        
        # Convert bytes back to string
        plaintext = response['Plaintext'].decode('utf-8')
        
        print(f"✅ Data decrypted successfully")
        print(f"   Key ID: {response['KeyId']}")
        print(f"   Plaintext: {plaintext}")
        
        return plaintext
        
    except ClientError as e:
        print(f"❌ Decryption error: {e}")
        return None

def generate_data_key(key_id, key_spec='AES_256'):
    """Generate a data key for envelope encryption"""
    
    try:
        response = kms_client.generate_data_key(
            KeyId=key_id,
            KeySpec=key_spec
        )
        
        plaintext_key = response['Plaintext']
        ciphertext_key = response['CiphertextBlob']
        
        print(f"✅ Data key generated successfully")
        print(f"   Key ID: {response['KeyId']}")
        print(f"   Plaintext key length: {len(plaintext_key)} bytes")
        
        return plaintext_key, ciphertext_key
        
    except ClientError as e:
        print(f"❌ Data key generation error: {e}")
        return None, None

def envelope_encryption_example():
    """Demonstrate envelope encryption pattern"""
    
    print("🔐 Envelope Encryption Example")
    print("=" * 40)
    
    # Use the Redshift key for this example
    key_alias = 'alias/redshift-encryption-key'
    
    # Large data to encrypt (simulated)
    large_data = "This is a large dataset that would typically be stored in S3 or database" * 1000
    
    # Step 1: Generate a data key
    print("Step 1: Generate data key")
    plaintext_key, encrypted_key = generate_data_key(key_alias)
    
    if not plaintext_key:
        return
    
    # Step 2: Encrypt large data with data key (using AES locally)
    print("\\nStep 2: Encrypt data with data key")
    from cryptography.fernet import Fernet
    
    # Use first 32 bytes of plaintext key for Fernet
    fernet_key = base64.urlsafe_b64encode(plaintext_key[:32])
    f = Fernet(fernet_key)
    
    encrypted_data = f.encrypt(large_data.encode())
    
    print(f"✅ Large data encrypted locally")
    print(f"   Original size: {len(large_data)} bytes")
    print(f"   Encrypted size: {len(encrypted_data)} bytes")
    
    # Step 3: Store encrypted data and encrypted key
    print("\\nStep 3: Store encrypted data and encrypted key")
    
    # In practice, you would store these in S3, database, etc.
    storage_record = {
        'encrypted_data': base64.b64encode(encrypted_data).decode(),
        'encrypted_key': base64.b64encode(encrypted_key).decode(),
        'algorithm': 'AES-256-GCM',
        'key_id': key_alias
    }
    
    print(f"✅ Storage record created")
    
    # Step 4: Decrypt process
    print("\\nStep 4: Decrypt process")
    
    # Decrypt the data key first
    decrypted_key_response = kms_client.decrypt(
        CiphertextBlob=encrypted_key
    )
    recovered_plaintext_key = decrypted_key_response['Plaintext']
    
    # Use recovered key to decrypt data
    recovered_fernet_key = base64.urlsafe_b64encode(recovered_plaintext_key[:32])
    recovered_f = Fernet(recovered_fernet_key)
    
    decrypted_data = recovered_f.decrypt(encrypted_data).decode()
    
    print(f"✅ Data decrypted successfully")
    print(f"   Decrypted size: {len(decrypted_data)} bytes")
    print(f"   Data matches: {decrypted_data == large_data}")
    
    return storage_record

# Example usage scenarios
def database_encryption_example():
    """Example of encrypting database connection strings"""
    
    print("\\n🗃️ Database Encryption Example")
    print("=" * 40)
    
    # Database connection details
    db_config = {
        'host': 'redshift-cluster.abc123.us-west-2.redshift.amazonaws.com',
        'port': 5439,
        'database': 'analytics',
        'username': 'admin',
        'password': 'SuperSecretPassword123!'
    }
    
    # Encryption context for additional security
    encryption_context = {
        'service': 'redshift',
        'environment': 'production',
        'purpose': 'database-connection'
    }
    
    # Encrypt sensitive configuration
    key_alias = 'alias/redshift-encryption-key'
    
    encrypted_config = {}
    for key, value in db_config.items():
        if key in ['password', 'username']:  # Encrypt sensitive fields
            encrypted_value = encrypt_data(key_alias, value, encryption_context)
            encrypted_config[key] = encrypted_value
        else:
            encrypted_config[key] = value
    
    print(f"\\n✅ Database configuration encrypted")
    print(f"   Encrypted fields: {list(encrypted_config.keys())}")
    
    # Decrypt for use
    decrypted_config = {}
    for key, value in encrypted_config.items():
        if key in ['password', 'username']:
            decrypted_value = decrypt_data(value, encryption_context)
            decrypted_config[key] = decrypted_value
        else:
            decrypted_config[key] = value
    
    print(f"\\n✅ Configuration decrypted for use")
    
    return encrypted_config, decrypted_config

# Run examples
envelope_record = envelope_encryption_example()
encrypted_db_config, decrypted_db_config = database_encryption_example()

print("\\n🎉 KMS encryption operations completed successfully!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# KMS Key Management and Monitoring

import boto3
import json
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

kms_client = boto3.client('kms')
cloudtrail_client = boto3.client('cloudtrail')
cloudwatch_client = boto3.client('cloudwatch')

def list_kms_keys():
    """List all KMS keys in the account"""
    
    try:
        response = kms_client.list_keys()
        keys = response['Keys']
        
        print(f"📋 Found {len(keys)} KMS keys:")
        
        for key in keys:
            key_id = key['KeyId']
            
            # Get key details
            key_info = kms_client.describe_key(KeyId=key_id)
            key_metadata = key_info['KeyMetadata']
            
            # Get aliases
            aliases = kms_client.list_aliases(KeyId=key_id)
            alias_names = [alias['AliasName'] for alias in aliases['Aliases']]
            
            print(f"\\n🔑 Key: {key_id}")
            print(f"   Description: {key_metadata.get('Description', 'No description')}")
            print(f"   State: {key_metadata['KeyState']}")
            print(f"   Usage: {key_metadata['KeyUsage']}")
            print(f"   Created: {key_metadata['CreationDate']}")
            print(f"   Aliases: {', '.join(alias_names) if alias_names else 'None'}")
            
            # Check rotation status
            try:
                rotation_status = kms_client.get_key_rotation_status(KeyId=key_id)
                print(f"   Rotation: {'Enabled' if rotation_status['KeyRotationEnabled'] else 'Disabled'}")
            except:
                print(f"   Rotation: Not applicable")
        
        return keys
        
    except ClientError as e:
        print(f"❌ Error listing keys: {e}")
        return []

def audit_key_usage(key_id, days_back=7):
    """Audit key usage from CloudTrail"""
    
    try:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days_back)
        
        # Look up CloudTrail events for KMS key usage
        response = cloudtrail_client.lookup_events(
            LookupAttributes=[
                {
                    'AttributeKey': 'ResourceName',
                    'AttributeValue': key_id
                }
            ],
            StartTime=start_time,
            EndTime=end_time
        )
        
        events = response['Events']
        
        print(f"\\n🔍 Key Usage Audit for {key_id}")
        print(f"   Period: {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")
        print(f"   Events found: {len(events)}")
        
        # Group events by event name
        event_summary = {}
        for event in events:
            event_name = event['EventName']
            event_summary[event_name] = event_summary.get(event_name, 0) + 1
        
        print(f"\\n📊 Event Summary:")
        for event_name, count in sorted(event_summary.items()):
            print(f"   {event_name}: {count}")
        
        # Show recent events
        print(f"\\n📋 Recent Events:")
        for event in events[:5]:  # Show last 5 events
            print(f"   {event['EventTime']}: {event['EventName']} by {event.get('Username', 'Unknown')}")
        
        return events
        
    except ClientError as e:
        print(f"❌ Error auditing key usage: {e}")
        return []

def monitor_kms_metrics():
    """Monitor KMS CloudWatch metrics"""
    
    try:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
        
        # Get KMS API call metrics
        response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/KMS',
            MetricName='NumberOfRequestsSucceeded',
            Dimensions=[],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,  # 1 hour periods
            Statistics=['Sum']
        )
        
        datapoints = response['Datapoints']
        
        print(f"\\n📈 KMS Metrics (Last 24 hours):")
        print(f"   Total successful requests: {sum(dp['Sum'] for dp in datapoints)}")
        
        # Get error metrics
        error_response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/KMS',
            MetricName='NumberOfRequestsFailed',
            Dimensions=[],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,
            Statistics=['Sum']
        )
        
        error_datapoints = error_response['Datapoints']
        total_errors = sum(dp['Sum'] for dp in error_datapoints)
        
        print(f"   Total failed requests: {total_errors}")
        
        if total_errors > 0:
            print(f"   ⚠️  Error rate needs investigation!")
        
        return datapoints, error_datapoints
        
    except ClientError as e:
        print(f"❌ Error getting metrics: {e}")
        return [], []

def key_policy_analysis(key_id):
    """Analyze key policy for security best practices"""
    
    try:
        response = kms_client.get_key_policy(
            KeyId=key_id,
            PolicyName='default'
        )
        
        policy = json.loads(response['Policy'])
        
        print(f"\\n🔐 Key Policy Analysis for {key_id}")
        print("=" * 50)
        
        # Check for common security issues
        issues = []
        recommendations = []
        
        # Check for overly permissive policies
        for statement in policy['Statement']:
            if statement['Effect'] == 'Allow':
                principals = statement.get('Principal', {})
                actions = statement.get('Action', [])
                
                # Check for wildcard principals
                if principals == '*' or (isinstance(principals, dict) and principals.get('AWS') == '*'):
                    issues.append("❌ Wildcard (*) principal found - overly permissive")
                
                # Check for wildcard actions
                if 'kms:*' in actions:
                    if not (isinstance(principals, dict) and 
                           principals.get('AWS', '').endswith(':root')):
                        issues.append("❌ Wildcard actions without root principal restriction")
                
                # Check for decrypt permissions
                decrypt_actions = ['kms:Decrypt', 'kms:GenerateDataKey']
                if any(action in actions for action in decrypt_actions):
                    recommendations.append("✅ Decrypt permissions found - ensure minimal principals")
        
        # Check for conditions
        conditions_used = False
        for statement in policy['Statement']:
            if 'Condition' in statement:
                conditions_used = True
                break
        
        if not conditions_used:
            recommendations.append("💡 Consider adding conditions for enhanced security")
        
        # Display results
        if issues:
            print("\\n🚨 Security Issues Found:")
            for issue in issues:
                print(f"   {issue}")
        
        if recommendations:
            print("\\n💡 Recommendations:")
            for rec in recommendations:
                print(f"   {rec}")
        
        if not issues and not recommendations:
            print("\\n✅ Key policy appears to follow security best practices")
        
        return policy, issues, recommendations
        
    except ClientError as e:
        print(f"❌ Error analyzing key policy: {e}")
        return None, [], []

def generate_key_management_report():
    """Generate comprehensive key management report"""
    
    print("🔐 KMS Key Management Report")
    print("=" * 60)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get all keys
    keys = list_kms_keys()
    
    # Summary statistics
    total_keys = len(keys)
    customer_managed = 0
    aws_managed = 0
    rotation_enabled = 0
    
    for key in keys:
        key_id = key['KeyId']
        
        try:
            key_info = kms_client.describe_key(KeyId=key_id)
            key_metadata = key_info['KeyMetadata']
            
            if key_metadata['KeyManager'] == 'CUSTOMER':
                customer_managed += 1
            else:
                aws_managed += 1
            
            # Check rotation
            try:
                rotation_status = kms_client.get_key_rotation_status(KeyId=key_id)
                if rotation_status['KeyRotationEnabled']:
                    rotation_enabled += 1
            except:
                pass  # AWS managed keys or unsupported key types
                
        except ClientError:
            continue
    
    print(f"\\n📊 Summary Statistics:")
    print(f"   Total Keys: {total_keys}")
    print(f"   Customer Managed: {customer_managed}")
    print(f"   AWS Managed: {aws_managed}")
    print(f"   Rotation Enabled: {rotation_enabled}")
    
    # Monitor metrics
    success_metrics, error_metrics = monitor_kms_metrics()
    
    # Security recommendations
    print(f"\\n🔒 Security Recommendations:")
    print(f"   1. Enable rotation for all customer-managed keys")
    print(f"   2. Review key policies regularly")
    print(f"   3. Monitor key usage patterns")
    print(f"   4. Use separate keys per service/environment")
    print(f"   5. Implement least privilege access")
    
    return {
        'total_keys': total_keys,
        'customer_managed': customer_managed,
        'aws_managed': aws_managed,
        'rotation_enabled': rotation_enabled,
        'success_metrics': success_metrics,
        'error_metrics': error_metrics
    }

# Run key management operations
print("🚀 Starting KMS Key Management Operations")
print("=" * 50)

# Generate comprehensive report
report = generate_key_management_report()

# Example: Audit specific key
# audit_key_usage('alias/redshift-encryption-key')

# Example: Analyze key policy
# key_policy_analysis('alias/redshift-encryption-key')

print("\\n✅ KMS management operations completed!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def redshift_encryption_tab():
    """Amazon Redshift Encryption tab content"""
    st.markdown("## 🏢 Amazon Redshift Encryption")
    st.markdown("*Comprehensive encryption at rest and in transit for your data warehouse*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Redshift Encryption Features
    Amazon Redshift provides multiple layers of encryption protection:
    - **Encryption at Rest**: AES-256 encryption for data blocks and metadata
    - **Encryption in Transit**: SSL/TLS for all connections
    - **Key Management**: AWS KMS or HSM integration
    - **Snapshot Encryption**: Automatic encryption of backups and snapshots
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Redshift encryption architecture
    st.markdown("#### 🏗️ Redshift Encryption Architecture")
    common.mermaid(create_redshift_encryption_flow(), height=700)
    
    # Interactive encryption configuration
    st.markdown("#### ⚙️ Redshift Encryption Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        encryption_at_rest = st.checkbox("Enable Encryption at Rest", value=True)
        if encryption_at_rest:
            key_management = st.selectbox("Key Management", ["AWS KMS", "Hardware Security Module (HSM)"])
            if key_management == "AWS KMS":
                kms_key_type = st.selectbox("KMS Key Type", ["AWS Managed", "Customer Managed"])
    
    with col2:
        encryption_in_transit = st.checkbox("Enable Encryption in Transit", value=True)
        if encryption_in_transit:
            ssl_mode = st.selectbox("SSL Mode", ["require", "prefer", "allow"])
            vpc_endpoints = st.checkbox("Use VPC Endpoints", value=True)
    
    # Calculate security score
    security_score = calculate_redshift_security_score(
        encryption_at_rest, encryption_in_transit, 
        key_management if encryption_at_rest else None,
        ssl_mode if encryption_in_transit else None
    )
    
    st.markdown('<div class="security-card">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 🛡️ Redshift Security Configuration
    **Encryption at Rest**: {'✅ Enabled' if encryption_at_rest else '❌ Disabled'}  
    **Encryption in Transit**: {'✅ Enabled' if encryption_in_transit else '❌ Disabled'}  
    **Key Management**: {key_management if encryption_at_rest else 'Not configured'}  
    **SSL Mode**: {ssl_mode if encryption_in_transit else 'Not configured'}  
    **Security Score**: {security_score}/100
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Encryption performance impact
    st.markdown("#### ⚡ Encryption Performance Impact")
    
    performance_data = {
        'Operation': ['SELECT Queries', 'INSERT Operations', 'COPY Commands', 'VACUUM Operations', 'Backup/Restore'],
        'Without Encryption': [100, 100, 100, 100, 100],
        'With Encryption': [98, 95, 92, 85, 80],
        'Performance Impact': ['Minimal', 'Low', 'Low', 'Moderate', 'Moderate']
    }
    
    df_performance = pd.DataFrame(performance_data)
    
    # Create performance chart
    fig = px.bar(df_performance, x='Operation', y=['Without Encryption', 'With Encryption'],
                 title='Redshift Performance with Encryption',
                 color_discrete_map={'Without Encryption': AWS_COLORS['light_blue'], 
                                   'With Encryption': AWS_COLORS['primary']})
    fig.update_layout(yaxis_title="Performance (%)", showlegend=True)
    st.plotly_chart(fig, use_container_width=True)
    
    # Encryption best practices
    st.markdown("#### 📋 Redshift Encryption Best Practices")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="encryption-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔐 At Rest Encryption
        - **Enable during cluster creation**
        - **Use customer-managed KMS keys**
        - **Enable automatic key rotation**
        - **Encrypt existing clusters via migration**
        - **Verify snapshot encryption**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="encryption-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔗 In Transit Encryption
        - **Force SSL connections**
        - **Use VPC endpoints**
        - **Configure parameter groups**
        - **Update connection strings**
        - **Monitor SSL usage**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Redshift Encryption Implementation")
    
    tab1, tab2, tab3 = st.tabs(["Cluster Creation", "Encryption Migration", "Monitoring"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create Encrypted Redshift Cluster

import boto3
import json
from botocore.exceptions import ClientError

redshift_client = boto3.client('redshift')
kms_client = boto3.client('kms')

def create_encrypted_redshift_cluster():
    """Create a new Redshift cluster with encryption enabled"""
    
    # First, create KMS key for Redshift encryption
    kms_key_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Enable IAM User Permissions",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::123456789012:root"
                },
                "Action": "kms:*",
                "Resource": "*"
            },
            {
                "Sid": "Allow Redshift Service",
                "Effect": "Allow",
                "Principal": {
                    "Service": "redshift.amazonaws.com"
                },
                "Action": [
                    "kms:Decrypt",
                    "kms:GenerateDataKey",
                    "kms:CreateGrant"
                ],
                "Resource": "*"
            }
        ]
    }
    
    try:
        # Create KMS key
        kms_response = kms_client.create_key(
            Description="Redshift cluster encryption key",
            KeyUsage='ENCRYPT_DECRYPT',
            KeySpec='SYMMETRIC_DEFAULT',
            Policy=json.dumps(kms_key_policy),
            Tags=[
                {
                    'TagKey': 'Service',
                    'TagValue': 'Redshift'
                },
                {
                    'TagKey': 'Environment',
                    'TagValue': 'Production'
                }
            ]
        )
        
        kms_key_id = kms_response['KeyMetadata']['KeyId']
        print(f"✅ KMS key created: {kms_key_id}")
        
        # Create key alias
        kms_client.create_alias(
            AliasName='alias/redshift-production-encryption',
            TargetKeyId=kms_key_id
        )
        
        # Enable key rotation
        kms_client.enable_key_rotation(KeyId=kms_key_id)
        print("✅ Key rotation enabled")
        
    except ClientError as e:
        print(f"❌ KMS key creation failed: {e}")
        return None
    
    # Create encrypted Redshift cluster
    try:
        cluster_response = redshift_client.create_cluster(
            ClusterIdentifier='encrypted-analytics-cluster',
            NodeType='dc2.large',
            MasterUsername='admin',
            MasterUserPassword='SecurePassword123!',
            DBName='analytics',
            ClusterType='multi-node',
            NumberOfNodes=3,
            
            # Encryption configuration
            Encrypted=True,
            KmsKeyId=kms_key_id,
            
            # Network configuration
            VpcSecurityGroupIds=[
                'sg-12345678'  # Replace with your security group
            ],
            ClusterSubnetGroupName='redshift-subnet-group',
            PubliclyAccessible=False,
            
            # Parameter group for SSL enforcement
            ClusterParameterGroupName='redshift-ssl-parameter-group',
            
            # Backup configuration
            AutomatedSnapshotRetentionPeriod=7,
            PreferredMaintenanceWindow='sun:05:00-sun:06:00',
            
            # Tags
            Tags=[
                {
                    'Key': 'Environment',
                    'Value': 'Production'
                },
                {
                    'Key': 'DataClassification',
                    'Value': 'Confidential'
                },
                {
                    'Key': 'Encryption',
                    'Value': 'Enabled'
                }
            ]
        )
        
        cluster_id = cluster_response['Cluster']['ClusterIdentifier']
        print(f"✅ Encrypted Redshift cluster created: {cluster_id}")
        
        return cluster_id, kms_key_id
        
    except ClientError as e:
        print(f"❌ Cluster creation failed: {e}")
        return None, None

def create_ssl_parameter_group():
    """Create parameter group to enforce SSL connections"""
    
    try:
        # Create parameter group
        redshift_client.create_cluster_parameter_group(
            ParameterGroupName='redshift-ssl-parameter-group',
            ParameterGroupFamily='redshift-1.0',
            Description='Parameter group to enforce SSL connections'
        )
        
        # Set SSL requirement
        redshift_client.modify_cluster_parameter_group(
            ParameterGroupName='redshift-ssl-parameter-group',
            Parameters=[
                {
                    'ParameterName': 'require_ssl',
                    'ParameterValue': 'true',
                    'Description': 'Require SSL connections'
                }
            ]
        )
        
        print("✅ SSL parameter group created")
        
    except ClientError as e:
        print(f"❌ Parameter group creation failed: {e}")

def setup_vpc_endpoint():
    """Create VPC endpoint for Redshift"""
    
    ec2_client = boto3.client('ec2')
    
    try:
        # Create VPC endpoint for Redshift
        response = ec2_client.create_vpc_endpoint(
            VpcId='vpc-12345678',  # Replace with your VPC ID
            ServiceName='com.amazonaws.us-west-2.redshift',
            VpcEndpointType='Interface',
            SubnetIds=[
                'subnet-12345678',  # Replace with your subnet IDs
                'subnet-87654321'
            ],
            SecurityGroupIds=[
                'sg-12345678'  # Replace with your security group
            ],
            
            # Policy to allow Redshift operations
            PolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": [
                            "redshift:DescribeClusters",
                            "redshift:GetClusterCredentials"
                        ],
                        "Resource": "*"
                    }
                ]
            }),
            
            # Tags
            TagSpecifications=[
                {
                    'ResourceType': 'vpc-endpoint',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': 'Redshift-VPC-Endpoint'
                        },
                        {
                            'Key': 'Service',
                            'Value': 'Redshift'
                        }
                    ]
                }
            ]
        )
        
        endpoint_id = response['VpcEndpoint']['VpcEndpointId']
        print(f"✅ VPC endpoint created: {endpoint_id}")
        
        return endpoint_id
        
    except ClientError as e:
        print(f"❌ VPC endpoint creation failed: {e}")
        return None

def configure_database_encryption():
    """Configure database-level encryption settings"""
    
    connection_string = """
    # Python connection with SSL
    import psycopg2
    
    connection = psycopg2.connect(
        host='encrypted-analytics-cluster.abc123.us-west-2.redshift.amazonaws.com',
        port=5439,
        database='analytics',
        user='admin',
        password='SecurePassword123!',
        sslmode='require',  # Force SSL
        sslrootcert='redshift-ca-cert.pem'  # CA certificate
    )
    """
    
    jdbc_string = """
    # JDBC connection with SSL
    jdbc:redshift://encrypted-analytics-cluster.abc123.us-west-2.redshift.amazonaws.com:5439/analytics?ssl=true&sslmode=require&sslrootcert=redshift-ca-cert.pem
    """
    
    print("🔐 Database Connection Configuration")
    print("=" * 50)
    print("Python Connection:")
    print(connection_string)
    print("\\nJDBC Connection:")
    print(jdbc_string)
    
    # Example SQL for encryption verification
    verification_sql = """
    -- Verify encryption status
    SELECT 
        schemaname,
        tablename,
        encoded
    FROM pg_table_def
    WHERE schemaname = 'public'
    ORDER BY tablename;
    
    -- Check SSL connection
    SELECT 
        usename,
        client_addr,
        ssl,
        ssl_version,
        ssl_cipher
    FROM pg_stat_ssl 
    JOIN pg_stat_activity ON pg_stat_ssl.pid = pg_stat_activity.pid
    WHERE usename IS NOT NULL;
    """
    
    print("\\nEncryption Verification SQL:")
    print(verification_sql)

# Execute setup
print("🚀 Setting up Encrypted Redshift Cluster")
print("=" * 50)

# Create SSL parameter group
create_ssl_parameter_group()

# Create VPC endpoint
vpc_endpoint_id = setup_vpc_endpoint()

# Create encrypted cluster
cluster_id, kms_key_id = create_encrypted_redshift_cluster()

if cluster_id:
    print(f"\\n✅ Encrypted Redshift setup completed!")
    print(f"   Cluster ID: {cluster_id}")
    print(f"   KMS Key ID: {kms_key_id}")
    print(f"   VPC Endpoint: {vpc_endpoint_id}")
    
    # Configure database connections
    configure_database_encryption()
else:
    print("❌ Setup failed - check error messages above")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Migrate Existing Redshift Cluster to Encrypted

import boto3
import time
from botocore.exceptions import ClientError

redshift_client = boto3.client('redshift')

def migrate_cluster_to_encrypted(cluster_identifier, kms_key_id):
    """Migrate an existing unencrypted cluster to encrypted"""
    
    print(f"🔄 Starting encryption migration for cluster: {cluster_identifier}")
    print("=" * 60)
    
    try:
        # Step 1: Get current cluster information
        print("Step 1: Getting cluster information...")
        
        response = redshift_client.describe_clusters(
            ClusterIdentifier=cluster_identifier
        )
        
        cluster = response['Clusters'][0]
        
        print(f"   Current encryption status: {cluster.get('Encrypted', False)}")
        print(f"   Cluster status: {cluster['ClusterStatus']}")
        print(f"   Node type: {cluster['NodeType']}")
        print(f"   Number of nodes: {cluster['NumberOfNodes']}")
        
        if cluster.get('Encrypted', False):
            print("   ✅ Cluster is already encrypted!")
            return True
        
        # Step 2: Create encrypted snapshot
        print("\\nStep 2: Creating encrypted snapshot...")
        
        snapshot_id = f"{cluster_identifier}-encrypted-migration-{int(time.time())}"
        
        redshift_client.create_cluster_snapshot(
            SnapshotIdentifier=snapshot_id,
            ClusterIdentifier=cluster_identifier,
            Tags=[
                {
                    'Key': 'Purpose',
                    'Value': 'EncryptionMigration'
                },
                {
                    'Key': 'OriginalCluster',
                    'Value': cluster_identifier
                }
            ]
        )
        
        # Wait for snapshot to complete
        print("   Waiting for snapshot to complete...")
        snapshot_ready = False
        max_attempts = 60  # 30 minutes max
        attempts = 0
        
        while not snapshot_ready and attempts < max_attempts:
            snapshot_response = redshift_client.describe_cluster_snapshots(
                SnapshotIdentifier=snapshot_id
            )
            
            snapshot_status = snapshot_response['Snapshots'][0]['Status']
            print(f"   Snapshot status: {snapshot_status}")
            
            if snapshot_status == 'available':
                snapshot_ready = True
            elif snapshot_status == 'failed':
                print("   ❌ Snapshot creation failed!")
                return False
            else:
                time.sleep(30)
                attempts += 1
        
        if not snapshot_ready:
            print("   ❌ Snapshot creation timed out!")
            return False
        
        print("   ✅ Snapshot created successfully")
        
        # Step 3: Copy snapshot with encryption
        print("\\nStep 3: Creating encrypted copy of snapshot...")
        
        encrypted_snapshot_id = f"{snapshot_id}-encrypted"
        
        redshift_client.copy_cluster_snapshot(
            SourceSnapshotIdentifier=snapshot_id,
            TargetSnapshotIdentifier=encrypted_snapshot_id,
            KmsKeyId=kms_key_id,
            Tags=[
                {
                    'Key': 'Encrypted',
                    'Value': 'true'
                },
                {
                    'Key': 'Purpose',
                    'Value': 'EncryptionMigration'
                }
            ]
        )
        
        # Wait for encrypted snapshot
        print("   Waiting for encrypted snapshot...")
        encrypted_snapshot_ready = False
        attempts = 0
        
        while not encrypted_snapshot_ready and attempts < max_attempts:
            encrypted_response = redshift_client.describe_cluster_snapshots(
                SnapshotIdentifier=encrypted_snapshot_id
            )
            
            encrypted_status = encrypted_response['Snapshots'][0]['Status']
            print(f"   Encrypted snapshot status: {encrypted_status}")
            
            if encrypted_status == 'available':
                encrypted_snapshot_ready = True
            elif encrypted_status == 'failed':
                print("   ❌ Encrypted snapshot creation failed!")
                return False
            else:
                time.sleep(30)
                attempts += 1
        
        if not encrypted_snapshot_ready:
            print("   ❌ Encrypted snapshot creation timed out!")
            return False
        
        print("   ✅ Encrypted snapshot created successfully")
        
        # Step 4: Prepare for cluster replacement
        print("\\nStep 4: Preparing for cluster replacement...")
        
        # Store original cluster configuration
        original_config = {
            'cluster_identifier': cluster_identifier,
            'node_type': cluster['NodeType'],
            'number_of_nodes': cluster['NumberOfNodes'],
            'db_name': cluster['DBName'],
            'master_username': cluster['MasterUsername'],
            'vpc_security_groups': [sg['VpcSecurityGroupId'] for sg in cluster['VpcSecurityGroups']],
            'cluster_subnet_group': cluster.get('ClusterSubnetGroupName'),
            'publicly_accessible': cluster['PubliclyAccessible'],
            'automated_snapshot_retention_period': cluster['AutomatedSnapshotRetentionPeriod'],
            'preferred_maintenance_window': cluster['PreferredMaintenanceWindow'],
            'cluster_parameter_group': cluster['ClusterParameterGroups'][0]['ParameterGroupName'] if cluster['ClusterParameterGroups'] else None,
            'tags': cluster.get('Tags', [])
        }
        
        # Step 5: Delete original cluster (with final snapshot)
        print("\\nStep 5: Deleting original cluster...")
        
        final_snapshot_id = f"{cluster_identifier}-final-backup-{int(time.time())}"
        
        redshift_client.delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=False,
            FinalClusterSnapshotIdentifier=final_snapshot_id
        )
        
        # Wait for cluster deletion
        print("   Waiting for cluster deletion...")
        cluster_deleted = False
        attempts = 0
        
        while not cluster_deleted and attempts < max_attempts:
            try:
                redshift_client.describe_clusters(
                    ClusterIdentifier=cluster_identifier
                )
                print("   Cluster still exists, waiting...")
                time.sleep(30)
                attempts += 1
            except ClientError as e:
                if 'ClusterNotFound' in str(e):
                    cluster_deleted = True
                else:
                    print(f"   Error checking cluster status: {e}")
                    time.sleep(30)
                    attempts += 1
        
        if not cluster_deleted:
            print("   ❌ Cluster deletion timed out!")
            return False
        
        print("   ✅ Original cluster deleted")
        
        # Step 6: Restore from encrypted snapshot
        print("\\nStep 6: Restoring encrypted cluster...")
        
        redshift_client.restore_from_cluster_snapshot(
            ClusterIdentifier=cluster_identifier,  # Same identifier
            SnapshotIdentifier=encrypted_snapshot_id,
            
            # Apply original configuration
            NodeType=original_config['node_type'],
            NumberOfNodes=original_config['number_of_nodes'],
            VpcSecurityGroupIds=original_config['vpc_security_groups'],
            ClusterSubnetGroupName=original_config['cluster_subnet_group'],
            PubliclyAccessible=original_config['publicly_accessible'],
            AutomatedSnapshotRetentionPeriod=original_config['automated_snapshot_retention_period'],
            PreferredMaintenanceWindow=original_config['preferred_maintenance_window'],
            ClusterParameterGroupName=original_config['cluster_parameter_group'],
            
            # KMS key for encryption
            KmsKeyId=kms_key_id,
            
            Tags=original_config['tags'] + [
                {
                    'Key': 'EncryptionMigrated',
                    'Value': 'true'
                },
                {
                    'Key': 'MigrationDate',
                    'Value': time.strftime('%Y-%m-%d')
                }
            ]
        )
        
        # Wait for cluster to be available
        print("   Waiting for encrypted cluster to be available...")
        cluster_available = False
        attempts = 0
        
        while not cluster_available and attempts < max_attempts:
            try:
                cluster_response = redshift_client.describe_clusters(
                    ClusterIdentifier=cluster_identifier
                )
                
                cluster_status = cluster_response['Clusters'][0]['ClusterStatus']
                print(f"   Cluster status: {cluster_status}")
                
                if cluster_status == 'available':
                    cluster_available = True
                elif cluster_status == 'failed':
                    print("   ❌ Cluster restoration failed!")
                    return False
                else:
                    time.sleep(30)
                    attempts += 1
                    
            except ClientError as e:
                print(f"   Error checking cluster: {e}")
                time.sleep(30)
                attempts += 1
        
        if not cluster_available:
            print("   ❌ Cluster restoration timed out!")
            return False
        
        print("   ✅ Encrypted cluster restored successfully")
        
        # Step 7: Verify encryption
        print("\\nStep 7: Verifying encryption...")
        
        final_response = redshift_client.describe_clusters(
            ClusterIdentifier=cluster_identifier
        )
        
        final_cluster = final_response['Clusters'][0]
        
        if final_cluster.get('Encrypted', False):
            print("   ✅ Cluster encryption verified!")
            print(f"   KMS Key ID: {final_cluster.get('KmsKeyId')}")
        else:
            print("   ❌ Encryption verification failed!")
            return False
        
        # Step 8: Cleanup temporary snapshots
        print("\\nStep 8: Cleaning up temporary snapshots...")
        
        try:
            redshift_client.delete_cluster_snapshot(
                SnapshotIdentifier=snapshot_id
            )
            print(f"   ✅ Deleted snapshot: {snapshot_id}")
        except ClientError as e:
            print(f"   ⚠️  Could not delete snapshot {snapshot_id}: {e}")
        
        try:
            redshift_client.delete_cluster_snapshot(
                SnapshotIdentifier=encrypted_snapshot_id
            )
            print(f"   ✅ Deleted encrypted snapshot: {encrypted_snapshot_id}")
        except ClientError as e:
            print(f"   ⚠️  Could not delete snapshot {encrypted_snapshot_id}: {e}")
        
        print("\\n🎉 Encryption migration completed successfully!")
        print(f"   Cluster: {cluster_identifier}")
        print(f"   Encryption: Enabled")
        print(f"   KMS Key: {kms_key_id}")
        print(f"   Final backup: {final_snapshot_id}")
        
        return True
        
    except ClientError as e:
        print(f"❌ Migration failed: {e}")
        return False

def verify_encryption_status(cluster_identifier):
    """Verify the encryption status of a cluster"""
    
    try:
        response = redshift_client.describe_clusters(
            ClusterIdentifier=cluster_identifier
        )
        
        cluster = response['Clusters'][0]
        
        print(f"\\n🔍 Encryption Status for {cluster_identifier}")
        print("=" * 50)
        print(f"   Encrypted: {cluster.get('Encrypted', False)}")
        print(f"   KMS Key ID: {cluster.get('KmsKeyId', 'None')}")
        print(f"   Cluster Status: {cluster['ClusterStatus']}")
        
        # Check snapshots encryption
        snapshots = redshift_client.describe_cluster_snapshots(
            ClusterIdentifier=cluster_identifier
        )
        
        encrypted_snapshots = 0
        total_snapshots = len(snapshots['Snapshots'])
        
        for snapshot in snapshots['Snapshots']:
            if snapshot.get('Encrypted', False):
                encrypted_snapshots += 1
        
        print(f"   Encrypted Snapshots: {encrypted_snapshots}/{total_snapshots}")
        
        return cluster.get('Encrypted', False)
        
    except ClientError as e:
        print(f"❌ Error checking encryption status: {e}")
        return False

# Example usage
if __name__ == "__main__":
    # Replace with your actual cluster identifier and KMS key ID
    cluster_id = "my-analytics-cluster"
    kms_key_id = "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"
    
    # Check current encryption status
    is_encrypted = verify_encryption_status(cluster_id)
    
    if not is_encrypted:
        print(f"\\n🔄 Starting encryption migration for {cluster_id}")
        
        # Perform migration
        success = migrate_cluster_to_encrypted(cluster_id, kms_key_id)
        
        if success:
            print("\\n✅ Migration completed successfully!")
            verify_encryption_status(cluster_id)
        else:
            print("\\n❌ Migration failed!")
    else:
        print(f"\\n✅ Cluster {cluster_id} is already encrypted!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Redshift Encryption Monitoring and Compliance

import boto3
import json
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

redshift_client = boto3.client('redshift')
cloudtrail_client = boto3.client('cloudtrail')
cloudwatch_client = boto3.client('cloudwatch')

def monitor_redshift_encryption():
    """Monitor encryption status across all Redshift clusters"""
    
    try:
        # Get all clusters
        response = redshift_client.describe_clusters()
        clusters = response['Clusters']
        
        print("🔍 Redshift Encryption Status Report")
        print("=" * 60)
        print(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total clusters: {len(clusters)}")
        
        encrypted_count = 0
        unencrypted_count = 0
        encryption_details = []
        
        for cluster in clusters:
            cluster_id = cluster['ClusterIdentifier']
            is_encrypted = cluster.get('Encrypted', False)
            kms_key_id = cluster.get('KmsKeyId')
            
            if is_encrypted:
                encrypted_count += 1
                status = "✅ Encrypted"
            else:
                unencrypted_count += 1
                status = "❌ Not Encrypted"
            
            encryption_details.append({
                'cluster_id': cluster_id,
                'encrypted': is_encrypted,
                'kms_key_id': kms_key_id,
                'status': cluster['ClusterStatus'],
                'node_type': cluster['NodeType'],
                'nodes': cluster['NumberOfNodes']
            })
            
            print(f"\\n📊 Cluster: {cluster_id}")
            print(f"   Encryption: {status}")
            print(f"   KMS Key: {kms_key_id or 'None'}")
            print(f"   Status: {cluster['ClusterStatus']}")
            print(f"   Configuration: {cluster['NodeType']} x {cluster['NumberOfNodes']}")
        
        print(f"\\n📋 Summary:")
        print(f"   Encrypted clusters: {encrypted_count}")
        print(f"   Unencrypted clusters: {unencrypted_count}")
        print(f"   Compliance rate: {(encrypted_count / len(clusters) * 100):.1f}%")
        
        # Check snapshots encryption
        print(f"\\n🔍 Snapshot Encryption Status:")
        check_snapshot_encryption()
        
        return encryption_details
        
    except ClientError as e:
        print(f"❌ Error monitoring encryption: {e}")
        return []

def check_snapshot_encryption():
    """Check encryption status of all snapshots"""
    
    try:
        response = redshift_client.describe_cluster_snapshots()
        snapshots = response['Snapshots']
        
        encrypted_snapshots = 0
        total_snapshots = len(snapshots)
        
        for snapshot in snapshots:
            if snapshot.get('Encrypted', False):
                encrypted_snapshots += 1
        
        print(f"   Total snapshots: {total_snapshots}")
        print(f"   Encrypted snapshots: {encrypted_snapshots}")
        print(f"   Snapshot encryption rate: {(encrypted_snapshots / total_snapshots * 100):.1f}%")
        
        # List unencrypted snapshots
        unencrypted_snapshots = [
            s['SnapshotIdentifier'] for s in snapshots 
            if not s.get('Encrypted', False)
        ]
        
        if unencrypted_snapshots:
            print(f"   ⚠️  Unencrypted snapshots:")
            for snapshot_id in unencrypted_snapshots[:5]:  # Show first 5
                print(f"      - {snapshot_id}")
            if len(unencrypted_snapshots) > 5:
                print(f"      ... and {len(unencrypted_snapshots) - 5} more")
        
    except ClientError as e:
        print(f"❌ Error checking snapshots: {e}")

def audit_kms_key_usage():
    """Audit KMS key usage for Redshift"""
    
    try:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=7)
        
        print(f"\\n🔍 KMS Key Usage Audit (Last 7 days)")
        print("=" * 50)
        
        # Look for KMS events related to Redshift
        response = cloudtrail_client.lookup_events(
            LookupAttributes=[
                {
                    'AttributeKey': 'EventName',
                    'AttributeValue': 'Decrypt'
                }
            ],
            StartTime=start_time,
            EndTime=end_time
        )
        
        redshift_events = []
        for event in response['Events']:
            if 'redshift' in event.get('CloudTrailEvent', '').lower():
                redshift_events.append(event)
        
        print(f"   KMS Decrypt events for Redshift: {len(redshift_events)}")
        
        # Group by user
        user_activity = {}
        for event in redshift_events:
            username = event.get('Username', 'Unknown')
            user_activity[username] = user_activity.get(username, 0) + 1
        
        print(f"   User Activity:")
        for user, count in sorted(user_activity.items(), key=lambda x: x[1], reverse=True):
            print(f"      {user}: {count} events")
        
        return redshift_events
        
    except ClientError as e:
        print(f"❌ Error auditing KMS usage: {e}")
        return []

def monitor_ssl_connections():
    """Monitor SSL connection compliance"""
    
    # This would typically be done by connecting to each cluster
    # and running queries against system tables
    
    ssl_monitoring_sql = """
    -- Monitor SSL connections
    SELECT 
        usename,
        client_addr,
        ssl,
        ssl_version,
        ssl_cipher,
        application_name,
        backend_start
    FROM pg_stat_ssl 
    JOIN pg_stat_activity ON pg_stat_ssl.pid = pg_stat_activity.pid
    WHERE usename IS NOT NULL
    ORDER BY backend_start DESC;
    
    -- Count SSL vs non-SSL connections
    SELECT 
        ssl,
        COUNT(*) as connection_count
    FROM pg_stat_ssl 
    JOIN pg_stat_activity ON pg_stat_ssl.pid = pg_stat_activity.pid
    WHERE usename IS NOT NULL
    GROUP BY ssl;
    """
    
    print(f"\\n🔐 SSL Connection Monitoring")
    print("=" * 50)
    print("Execute the following SQL on each Redshift cluster:")
    print(ssl_monitoring_sql)
    
    # Example of what you might see:
    example_results = {
        'total_connections': 25,
        'ssl_connections': 23,
        'non_ssl_connections': 2,
        'ssl_compliance_rate': 92.0
    }
    
    print(f"\\nExample Results:")
    print(f"   Total connections: {example_results['total_connections']}")
    print(f"   SSL connections: {example_results['ssl_connections']}")
    print(f"   Non-SSL connections: {example_results['non_ssl_connections']}")
    print(f"   SSL compliance rate: {example_results['ssl_compliance_rate']:.1f}%")
    
    if example_results['non_ssl_connections'] > 0:
        print(f"   ⚠️  {example_results['non_ssl_connections']} non-SSL connections found!")
        print(f"   Action: Review parameter groups and connection strings")

def generate_compliance_report():
    """Generate comprehensive compliance report"""
    
    print("\\n📋 Redshift Encryption Compliance Report")
    print("=" * 60)
    
    # Monitor encryption status
    encryption_details = monitor_redshift_encryption()
    
    # Audit KMS usage
    kms_events = audit_kms_key_usage()
    
    # Monitor SSL connections
    monitor_ssl_connections()
    
    # Generate recommendations
    print(f"\\n💡 Recommendations:")
    
    unencrypted_clusters = [
        cluster for cluster in encryption_details 
        if not cluster['encrypted']
    ]
    
    if unencrypted_clusters:
        print(f"   1. Enable encryption for {len(unencrypted_clusters)} clusters:")
        for cluster in unencrypted_clusters[:3]:  # Show first 3
            print(f"      - {cluster['cluster_id']}")
        if len(unencrypted_clusters) > 3:
            print(f"      ... and {len(unencrypted_clusters) - 3} more")
    
    print(f"   2. Enable automatic key rotation for all KMS keys")
    print(f"   3. Enforce SSL connections via parameter groups")
    print(f"   4. Regular encryption status monitoring")
    print(f"   5. Implement CloudWatch alarms for encryption events")
    
    # Compliance scoring
    total_clusters = len(encryption_details)
    encrypted_clusters = sum(1 for c in encryption_details if c['encrypted'])
    
    compliance_score = (encrypted_clusters / total_clusters * 100) if total_clusters > 0 else 0
    
    print(f"\\n🏆 Compliance Score: {compliance_score:.1f}%")
    
    if compliance_score >= 90:
        print("   ✅ Excellent - Strong encryption compliance")
    elif compliance_score >= 70:
        print("   ⚠️  Good - Minor improvements needed")
    else:
        print("   ❌ Poor - Immediate action required")
    
    return {
        'encryption_details': encryption_details,
        'kms_events': kms_events,
        'compliance_score': compliance_score,
        'unencrypted_clusters': unencrypted_clusters
    }

def setup_cloudwatch_alarms():
    """Set up CloudWatch alarms for encryption monitoring"""
    
    try:
        # Create alarm for KMS key usage
        cloudwatch_client.put_metric_alarm(
            AlarmName='Redshift-KMS-Usage-High',
            ComparisonOperator='GreaterThanThreshold',
            EvaluationPeriods=1,
            MetricName='NumberOfRequestsSucceeded',
            Namespace='AWS/KMS',
            Period=300,
            Statistic='Sum',
            Threshold=1000.0,
            ActionsEnabled=True,
            AlarmActions=[
                'arn:aws:sns:us-west-2:123456789012:security-alerts'
            ],
            AlarmDescription='High KMS usage detected for Redshift',
            Dimensions=[
                {
                    'Name': 'KeyId',
                    'Value': 'alias/redshift-encryption-key'
                }
            ]
        )
        
        print("✅ CloudWatch alarms configured")
        
    except ClientError as e:
        print(f"❌ Error setting up alarms: {e}")

# Run comprehensive monitoring
if __name__ == "__main__":
    print("🚀 Starting Redshift Encryption Monitoring")
    print("=" * 60)
    
    # Generate compliance report
    compliance_report = generate_compliance_report()
    
    # Set up monitoring
    setup_cloudwatch_alarms()
    
    print("\\n✅ Monitoring setup completed!")
    print(f"   Compliance Score: {compliance_report['compliance_score']:.1f}%")
    print(f"   Clusters monitored: {len(compliance_report['encryption_details'])}")
    print(f"   KMS events analyzed: {len(compliance_report['kms_events'])}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def glue_encryption_tab():
    """AWS Glue Data Catalog Encryption tab content"""
    st.markdown("## 📊 AWS Glue Data Catalog Encryption")
    st.markdown("*Comprehensive encryption for metadata, ETL jobs, and data processing*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Glue Encryption Components
    AWS Glue provides multiple encryption options for comprehensive data protection:
    - **Data Catalog Encryption**: Protect metadata stored in the Glue Data Catalog
    - **ETL Job Encryption**: Encrypt data processed by Glue jobs
    - **Connection Encryption**: Secure connections to data sources
    - **S3 Encryption**: Integrate with S3 server-side encryption
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Glue encryption architecture
    st.markdown("#### 🏗️ Glue Encryption Architecture")
    common.mermaid(create_glue_encryption_diagram(), height=700)
    
    # Interactive encryption setup
    st.markdown("#### ⚙️ Glue Encryption Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        catalog_encryption = st.checkbox("Enable Data Catalog Encryption", value=True)
        if catalog_encryption:
            catalog_key_type = st.selectbox("Catalog Key Type", ["AWS Managed", "Customer Managed"])
        
        job_encryption = st.checkbox("Enable ETL Job Encryption", value=True)
        if job_encryption:
            job_key_type = st.selectbox("Job Key Type", ["AWS Managed", "Customer Managed"])
    
    with col2:
        s3_encryption = st.checkbox("Enable S3 Encryption", value=True)
        if s3_encryption:
            s3_encryption_mode = st.selectbox("S3 Encryption Mode", ["SSE-S3", "SSE-KMS", "CSE-KMS"])
        
        cloudwatch_encryption = st.checkbox("Enable CloudWatch Logs Encryption", value=True)
    
    # Calculate encryption coverage
    encryption_coverage = calculate_glue_encryption_coverage(
        catalog_encryption, job_encryption, s3_encryption, cloudwatch_encryption
    )
    
    st.markdown('<div class="security-card">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 🔐 Glue Encryption Configuration
    **Data Catalog**: {'✅ Encrypted' if catalog_encryption else '❌ Not Encrypted'}  
    **ETL Jobs**: {'✅ Encrypted' if job_encryption else '❌ Not Encrypted'}  
    **S3 Data**: {'✅ Encrypted' if s3_encryption else '❌ Not Encrypted'}  
    **CloudWatch Logs**: {'✅ Encrypted' if cloudwatch_encryption else '❌ Not Encrypted'}  
    **Encryption Coverage**: {encryption_coverage}%
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Encryption options comparison
    st.markdown("#### 🔒 Encryption Options Comparison")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="encryption-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 📚 Data Catalog Encryption
        **What's Encrypted:**
        - Database metadata
        - Table schemas
        - Partition information
        - Column statistics
        - User-defined functions
        - Connection parameters
        
        **Key Options:**
        - AWS managed keys
        - Customer managed keys
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="encryption-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚙️ ETL Job Encryption
        **What's Encrypted:**
        - Job bookmarks
        - Temporary files
        - Shuffle data
        - Error logs
        - Job metrics
        - Script parameters
        
        **Encryption Types:**
        - Server-side encryption
        - Client-side encryption
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="encryption-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗂️ S3 Integration
        **Encryption Modes:**
        - SSE-S3 (S3 managed)
        - SSE-KMS (KMS managed)
        - CSE-KMS (Client-side)
        
        **Benefits:**
        - Automatic encryption
        - Key rotation
        - Access control
        - Audit trail
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Encryption workflow
    st.markdown("#### 🔄 Glue Encryption Workflow")
    
    workflow_steps = [
        "1. **Data Ingestion**: Crawlers discover encrypted data sources",
        "2. **Metadata Storage**: Catalog stores encrypted metadata using KMS",
        "3. **Job Processing**: ETL jobs encrypt temporary and output data",
        "4. **Data Output**: Results written to encrypted S3 locations",
        "5. **Monitoring**: CloudWatch logs encrypted for audit compliance"
    ]
    
    for step in workflow_steps:
        st.markdown(f"- {step}")
    
    # Code examples
    st.markdown("#### 💻 Glue Encryption Implementation")
    
    tab1, tab2, tab3 = st.tabs(["Security Configuration", "Encrypted Jobs", "Monitoring"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# AWS Glue Encryption Configuration

import boto3
import json
from botocore.exceptions import ClientError

glue_client = boto3.client('glue')
kms_client = boto3.client('kms')

def create_glue_encryption_keys():
    """Create KMS keys for different Glue encryption needs"""
    
    print("🔐 Creating KMS keys for Glue encryption")
    print("=" * 50)
    
    keys = {}
    
    # Key for Data Catalog encryption
    catalog_key_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Enable IAM User Permissions",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::123456789012:root"
                },
                "Action": "kms:*",
                "Resource": "*"
            },
            {
                "Sid": "Allow Glue Service",
                "Effect": "Allow",
                "Principal": {
                    "Service": "glue.amazonaws.com"
                },
                "Action": [
                    "kms:Decrypt",
                    "kms:GenerateDataKey",
                    "kms:CreateGrant"
                ],
                "Resource": "*"
            }
        ]
    }
    
    try:
        # Create Data Catalog encryption key
        catalog_response = kms_client.create_key(
            Description="Glue Data Catalog encryption key",
            KeyUsage='ENCRYPT_DECRYPT',
            KeySpec='SYMMETRIC_DEFAULT',
            Policy=json.dumps(catalog_key_policy),
            Tags=[
                {'TagKey': 'Service', 'TagValue': 'Glue'},
                {'TagKey': 'Purpose', 'TagValue': 'DataCatalog'}
            ]
        )
        
        catalog_key_id = catalog_response['KeyMetadata']['KeyId']
        keys['catalog'] = catalog_key_id
        
        # Create alias
        kms_client.create_alias(
            AliasName='alias/glue-catalog-encryption',
            TargetKeyId=catalog_key_id
        )
        
        print(f"✅ Data Catalog key created: {catalog_key_id}")
        
        # Create ETL Job encryption key
        job_response = kms_client.create_key(
            Description="Glue ETL Job encryption key",
            KeyUsage='ENCRYPT_DECRYPT',
            KeySpec='SYMMETRIC_DEFAULT',
            Policy=json.dumps(catalog_key_policy),  # Same policy
            Tags=[
                {'TagKey': 'Service', 'TagValue': 'Glue'},
                {'TagKey': 'Purpose', 'TagValue': 'ETLJobs'}
            ]
        )
        
        job_key_id = job_response['KeyMetadata']['KeyId']
        keys['job'] = job_key_id
        
        # Create alias
        kms_client.create_alias(
            AliasName='alias/glue-job-encryption',
            TargetKeyId=job_key_id
        )
        
        print(f"✅ ETL Job key created: {job_key_id}")
        
        # Enable rotation for both keys
        for key_id in [catalog_key_id, job_key_id]:
            kms_client.enable_key_rotation(KeyId=key_id)
        
        print("✅ Key rotation enabled for both keys")
        
        return keys
        
    except ClientError as e:
        print(f"❌ Error creating keys: {e}")
        return {}

def configure_glue_encryption(catalog_key_id, job_key_id):
    """Configure Glue encryption settings"""
    
    try:
        # Enable Data Catalog encryption
        glue_client.put_data_catalog_encryption_settings(
            DataCatalogEncryptionSettings={
                'EncryptionAtRest': {
                    'CatalogEncryptionMode': 'SSE-KMS',
                    'SseAwsKmsKeyId': catalog_key_id
                },
                'ConnectionPasswordEncryption': {
                    'ReturnConnectionPasswordEncrypted': True,
                    'AwsKmsKeyId': catalog_key_id
                }
            }
        )
        
        print("✅ Data Catalog encryption enabled")
        
        # Create security configuration for ETL jobs
        security_config = {
            'Name': 'GlueJobEncryptionConfig',
            'EncryptionConfiguration': {
                'S3Encryption': [
                    {
                        'S3EncryptionMode': 'SSE-KMS',
                        'KmsKeyArn': f'arn:aws:kms:us-west-2:123456789012:key/{job_key_id}'
                    }
                ],
                'CloudWatchEncryption': {
                    'CloudWatchEncryptionMode': 'SSE-KMS',
                    'KmsKeyArn': f'arn:aws:kms:us-west-2:123456789012:key/{job_key_id}'
                },
                'JobBookmarksEncryption': {
                    'JobBookmarksEncryptionMode': 'CSE-KMS',
                    'KmsKeyArn': f'arn:aws:kms:us-west-2:123456789012:key/{job_key_id}'
                }
            }
        }
        
        glue_client.create_security_configuration(**security_config)
        
        print("✅ Security configuration created")
        
        return security_config['Name']
        
    except ClientError as e:
        print(f"❌ Error configuring encryption: {e}")
        return None

def create_encrypted_database():
    """Create a database in the encrypted Data Catalog"""
    
    try:
        database_input = {
            'Name': 'encrypted_analytics_db',
            'Description': 'Database with encrypted metadata',
            'Parameters': {
                'classification': 'confidential',
                'encryption': 'enabled'
            }
        }
        
        glue_client.create_database(
            DatabaseInput=database_input
        )
        
        print("✅ Encrypted database created")
        
        return database_input['Name']
        
    except ClientError as e:
        print(f"❌ Error creating database: {e}")
        return None

def create_encrypted_table():
    """Create a table with encrypted metadata"""
    
    database_name = 'encrypted_analytics_db'
    
    try:
        table_input = {
            'Name': 'customer_data',
            'StorageDescriptor': {
                'Columns': [
                    {
                        'Name': 'customer_id',
                        'Type': 'bigint',
                        'Comment': 'Unique customer identifier'
                    },
                    {
                        'Name': 'customer_name',
                        'Type': 'string',
                        'Comment': 'Customer full name - PII'
                    },
                    {
                        'Name': 'email',
                        'Type': 'string',
                        'Comment': 'Customer email - PII'
                    },
                    {
                        'Name': 'registration_date',
                        'Type': 'date',
                        'Comment': 'Account registration date'
                    }
                ],
                'Location': 's3://encrypted-data-bucket/customer-data/',
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
                {
                    'Name': 'year',
                    'Type': 'string'
                },
                {
                    'Name': 'month',
                    'Type': 'string'
                }
            ],
            'Parameters': {
                'classification': 'csv',
                'encryption': 'enabled',
                'data_classification': 'PII'
            },
            'Description': 'Customer data table with encrypted metadata'
        }
        
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput=table_input
        )
        
        print("✅ Encrypted table created")
        
        return table_input['Name']
        
    except ClientError as e:
        print(f"❌ Error creating table: {e}")
        return None

def verify_encryption_settings():
    """Verify Glue encryption configuration"""
    
    try:
        # Check Data Catalog encryption
        catalog_response = glue_client.get_data_catalog_encryption_settings()
        
        encryption_settings = catalog_response['DataCatalogEncryptionSettings']
        
        print("🔍 Data Catalog Encryption Status:")
        print("=" * 50)
        
        # Check encryption at rest
        encryption_at_rest = encryption_settings.get('EncryptionAtRest', {})
        catalog_mode = encryption_at_rest.get('CatalogEncryptionMode', 'DISABLED')
        
        print(f"   Catalog Encryption Mode: {catalog_mode}")
        
        if catalog_mode == 'SSE-KMS':
            kms_key = encryption_at_rest.get('SseAwsKmsKeyId')
            print(f"   KMS Key: {kms_key}")
        
        # Check connection password encryption
        conn_encryption = encryption_settings.get('ConnectionPasswordEncryption', {})
        password_encrypted = conn_encryption.get('ReturnConnectionPasswordEncrypted', False)
        
        print(f"   Connection Password Encryption: {'Enabled' if password_encrypted else 'Disabled'}")
        
        # Check security configurations
        security_configs = glue_client.get_security_configurations()
        
        print(f"\\n🔐 Security Configurations:")
        for config in security_configs['SecurityConfigurations']:
            name = config['Name']
            encryption_config = config['EncryptionConfiguration']
            
            print(f"   Configuration: {name}")
            
            # S3 encryption
            s3_encryption = encryption_config.get('S3Encryption', [])
            if s3_encryption:
                for s3_config in s3_encryption:
                    mode = s3_config.get('S3EncryptionMode')
                    print(f"      S3 Encryption: {mode}")
            
            # CloudWatch encryption
            cw_encryption = encryption_config.get('CloudWatchEncryption', {})
            if cw_encryption:
                cw_mode = cw_encryption.get('CloudWatchEncryptionMode')
                print(f"      CloudWatch Encryption: {cw_mode}")
            
            # Job bookmarks encryption
            jb_encryption = encryption_config.get('JobBookmarksEncryption', {})
            if jb_encryption:
                jb_mode = jb_encryption.get('JobBookmarksEncryptionMode')
                print(f"      Job Bookmarks Encryption: {jb_mode}")
        
        return True
        
    except ClientError as e:
        print(f"❌ Error verifying encryption: {e}")
        return False

# Main setup function
def setup_glue_encryption():
    """Complete Glue encryption setup"""
    
    print("🚀 Setting up AWS Glue Encryption")
    print("=" * 50)
    
    # Step 1: Create encryption keys
    keys = create_glue_encryption_keys()
    
    if not keys:
        print("❌ Failed to create encryption keys")
        return False
    
    # Step 2: Configure encryption
    security_config_name = configure_glue_encryption(
        keys['catalog'], 
        keys['job']
    )
    
    if not security_config_name:
        print("❌ Failed to configure encryption")
        return False
    
    # Step 3: Create encrypted database
    database_name = create_encrypted_database()
    
    if not database_name:
        print("❌ Failed to create encrypted database")
        return False
    
    # Step 4: Create encrypted table
    table_name = create_encrypted_table()
    
    if not table_name:
        print("❌ Failed to create encrypted table")
        return False
    
    # Step 5: Verify configuration
    if verify_encryption_settings():
        print("\\n✅ Glue encryption setup completed successfully!")
        print(f"   Data Catalog Key: {keys['catalog']}")
        print(f"   ETL Job Key: {keys['job']}")
        print(f"   Security Configuration: {security_config_name}")
        print(f"   Database: {database_name}")
        print(f"   Table: {table_name}")
        return True
    else:
        print("❌ Encryption verification failed")
        return False

# Execute setup
if __name__ == "__main__":
    success = setup_glue_encryption()
    
    if success:
        print("\\n🎉 AWS Glue is now fully encrypted!")
    else:
        print("\\n❌ Encryption setup failed - check error messages above")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create Encrypted Glue ETL Jobs

import boto3
import json
from botocore.exceptions import ClientError

glue_client = boto3.client('glue')

def create_encrypted_etl_job():
    """Create an ETL job with encryption enabled"""
    
    try:
        # ETL script for processing encrypted data
        etl_script = """
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_database', 'input_table', 'output_path'])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from encrypted Data Catalog
print("Reading from encrypted Data Catalog...")
datasource = glueContext.create_dynamic_frame.from_catalog(
    database=args['input_database'],
    table_name=args['input_table'],
    transformation_ctx="datasource"
)

print(f"Records read: {datasource.count()}")

# Data transformations
print("Applying transformations...")

# Convert to DataFrame for complex transformations
df = datasource.toDF()

# Example transformations with PII handling
transformed_df = df.withColumn(
    "customer_name_masked", 
    F.regexp_replace(F.col("customer_name"), r"(?<=.{2}).*(?=.{2})", "*")
).withColumn(
    "email_masked",
    F.regexp_replace(F.col("email"), r"(?<=.{2}).*(?=@)", "*")
).withColumn(
    "processed_date",
    F.current_timestamp()
).withColumn(
    "data_classification",
    F.lit("PII_MASKED")
)

# Convert back to DynamicFrame
transformed_dynamic_frame = DynamicFrame.fromDF(
    transformed_df, 
    glueContext, 
    "transformed_data"
)

# Write to encrypted S3 location
print("Writing to encrypted S3...")
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": args['output_path'],
        "partitionKeys": ["year", "month"]
    },
    format="glueparquet",
    format_options={
        "compression": "snappy"
    },
    transformation_ctx="datasink"
)

print("Job completed successfully!")
job.commit()
        """
        
        # Create the ETL job
        job_response = glue_client.create_job(
            Name='encrypted-customer-data-processing',
            Description='ETL job with full encryption for customer data processing',
            Role='arn:aws:iam::123456789012:role/GlueETLRole',
            Command={
                'Name': 'glueetl',
                'ScriptLocation': 's3://glue-scripts-bucket/encrypted-etl-job.py',
                'PythonVersion': '3'
            },
            DefaultArguments={
                '--job-language': 'python',
                '--input_database': 'encrypted_analytics_db',
                '--input_table': 'customer_data',
                '--output_path': 's3://encrypted-output-bucket/processed-data/',
                '--enable-metrics': 'true',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-spark-ui': 'true',
                '--spark-event-logs-path': 's3://encrypted-logs-bucket/spark-logs/',
                '--TempDir': 's3://encrypted-temp-bucket/temp-data/'
            },
            MaxRetries=2,
            Timeout=60,  # 1 hour timeout
            MaxCapacity=10,
            
            # Security configuration with encryption
            SecurityConfiguration='GlueJobEncryptionConfig',
            
            # Tags for governance
            Tags={
                'Environment': 'Production',
                'DataClassification': 'PII',
                'Encryption': 'Enabled',
                'Owner': 'DataEngineering'
            }
        )
        
        job_name = job_response['Name']
        print(f"✅ Encrypted ETL job created: {job_name}")
        
        return job_name
        
    except ClientError as e:
        print(f"❌ Error creating ETL job: {e}")
        return None

def create_encrypted_crawler():
    """Create a crawler with encryption to discover encrypted data"""
    
    try:
        crawler_response = glue_client.create_crawler(
            Name='encrypted-data-crawler',
            Role='arn:aws:iam::123456789012:role/GlueServiceRole',
            DatabaseName='encrypted_analytics_db',
            Description='Crawler for encrypted data sources',
            
            # Targets - encrypted S3 locations
            Targets={
                'S3Targets': [
                    {
                        'Path': 's3://encrypted-raw-data-bucket/customer-data/',
                        'Exclusions': ['*.tmp', '*.log']
                    },
                    {
                        'Path': 's3://encrypted-raw-data-bucket/order-data/',
                        'Exclusions': ['*.tmp', '*.log']
                    }
                ]
            },
            
            # Schedule to run nightly
            Schedule='cron(0 2 * * ? *)',  # 2 AM daily
            
            # Schema change policy
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'LOG'
            },
            
            # Configuration for encrypted catalog
            Configuration=json.dumps({
                'Version': 1.0,
                'CrawlerOutput': {
                    'Partitions': {
                        'AddOrUpdateBehavior': 'InheritFromTable'
                    }
                },
                'Grouping': {
                    'TableGroupingPolicy': 'CombineCompatibleSchemas'
                }
            }),
            
            # Tags
            Tags={
                'Environment': 'Production',
                'Purpose': 'DataDiscovery',
                'Encryption': 'Enabled'
            }
        )
        
        crawler_name = crawler_response['Name']
        print(f"✅ Encrypted crawler created: {crawler_name}")
        
        return crawler_name
        
    except ClientError as e:
        print(f"❌ Error creating crawler: {e}")
        return None

def run_encrypted_etl_job(job_name):
    """Run the encrypted ETL job"""
    
    try:
        # Start the job
        job_run_response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                '--input_database': 'encrypted_analytics_db',
                '--input_table': 'customer_data',
                '--output_path': 's3://encrypted-output-bucket/processed-data/',
                '--additional-python-modules': 'boto3==1.26.137'
            }
        )
        
        job_run_id = job_run_response['JobRunId']
        print(f"✅ ETL job started: {job_run_id}")
        
        # Monitor job progress
        import time
        
        while True:
            job_run_status = glue_client.get_job_run(
                JobName=job_name,
                RunId=job_run_id
            )
            
            job_state = job_run_status['JobRun']['JobRunState']
            print(f"   Job status: {job_state}")
            
            if job_state == 'SUCCEEDED':
                print("✅ Job completed successfully!")
                
                # Get job statistics
                job_run = job_run_status['JobRun']
                print(f"   Execution time: {job_run.get('ExecutionTime', 'N/A')} seconds")
                print(f"   DPU seconds: {job_run.get('DPUSeconds', 'N/A')}")
                
                break
            elif job_state == 'FAILED':
                print("❌ Job failed!")
                error_message = job_run_status['JobRun'].get('ErrorMessage', 'Unknown error')
                print(f"   Error: {error_message}")
                break
            elif job_state in ['RUNNING', 'STARTING']:
                time.sleep(30)  # Wait 30 seconds before checking again
            else:
                print(f"   Unexpected job state: {job_state}")
                break
        
        return job_run_id
        
    except ClientError as e:
        print(f"❌ Error running ETL job: {e}")
        return None

def create_encrypted_connection():
    """Create an encrypted connection to external data source"""
    
    try:
        connection_response = glue_client.create_connection(
            ConnectionInput={
                'Name': 'encrypted-postgres-connection',
                'Description': 'Encrypted connection to PostgreSQL database',
                'ConnectionType': 'JDBC',
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': 'jdbc:postgresql://postgres-server.amazonaws.com:5432/analytics',
                    'USERNAME': 'glue_user',
                    'PASSWORD': 'encrypted_password',  # This will be encrypted by KMS
                    'JDBC_DRIVER_JAR_URI': 's3://glue-drivers/postgresql-42.3.1.jar',
                    'JDBC_DRIVER_CLASS_NAME': 'org.postgresql.Driver',
                    'JDBC_ENFORCE_SSL': 'true'
                },
                'PhysicalConnectionRequirements': {
                    'SubnetId': 'subnet-12345678',
                    'SecurityGroupIdList': ['sg-12345678'],
                    'AvailabilityZone': 'us-west-2a'
                }
            }
        )
        
        connection_name = connection_response['Name']
        print(f"✅ Encrypted connection created: {connection_name}")
        
        return connection_name
        
    except ClientError as e:
        print(f"❌ Error creating connection: {e}")
        return None

def monitor_encrypted_jobs():
    """Monitor encrypted Glue jobs"""
    
    try:
        # Get all jobs
        jobs_response = glue_client.get_jobs()
        
        print("📊 Encrypted Glue Jobs Status")
        print("=" * 50)
        
        for job in jobs_response['Jobs']:
            job_name = job['Name']
            security_config = job.get('SecurityConfiguration')
            
            print(f"\\n🔧 Job: {job_name}")
            print(f"   Security Configuration: {security_config or 'None'}")
            print(f"   Role: {job['Role']}")
            print(f"   Max Capacity: {job.get('MaxCapacity', 'N/A')}")
            
            # Get recent job runs
            job_runs_response = glue_client.get_job_runs(
                JobName=job_name,
                MaxResults=5
            )
            
            print(f"   Recent Runs:")
            for run in job_runs_response['JobRuns']:
                run_id = run['Id']
                state = run['JobRunState']
                started = run.get('StartedOn', 'N/A')
                
                print(f"      {run_id}: {state} (Started: {started})")
        
        return True
        
    except ClientError as e:
        print(f"❌ Error monitoring jobs: {e}")
        return False

# Main execution
def setup_encrypted_glue_jobs():
    """Set up complete encrypted Glue job environment"""
    
    print("🚀 Setting up Encrypted Glue ETL Jobs")
    print("=" * 50)
    
    # Create encrypted ETL job
    job_name = create_encrypted_etl_job()
    
    if not job_name:
        print("❌ Failed to create ETL job")
        return False
    
    # Create encrypted crawler
    crawler_name = create_encrypted_crawler()
    
    if not crawler_name:
        print("❌ Failed to create crawler")
        return False
    
    # Create encrypted connection
    connection_name = create_encrypted_connection()
    
    if not connection_name:
        print("❌ Failed to create connection")
        return False
    
    # Run the crawler first to discover data
    print("\\n🔍 Starting crawler to discover encrypted data...")
    
    try:
        glue_client.start_crawler(Name=crawler_name)
        print(f"✅ Crawler started: {crawler_name}")
    except ClientError as e:
        print(f"❌ Error starting crawler: {e}")
    
    # Wait a bit and then run the ETL job
    print("\\n⚙️ Starting encrypted ETL job...")
    
    job_run_id = run_encrypted_etl_job(job_name)
    
    if job_run_id:
        print(f"\\n✅ Encrypted Glue environment setup completed!")
        print(f"   ETL Job: {job_name}")
        print(f"   Crawler: {crawler_name}")
        print(f"   Connection: {connection_name}")
        print(f"   Job Run ID: {job_run_id}")
        
        # Monitor jobs
        monitor_encrypted_jobs()
        
        return True
    else:
        print("❌ Failed to run ETL job")
        return False

# Execute setup
if __name__ == "__main__":
    success = setup_encrypted_glue_jobs()
    
    if success:
        print("\\n🎉 Encrypted Glue ETL environment is ready!")
    else:
        print("\\n❌ Setup failed - check error messages above")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Monitor Glue Encryption and Compliance

import boto3
import json
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

glue_client = boto3.client('glue')
cloudwatch_client = boto3.client('cloudwatch')
cloudtrail_client = boto3.client('cloudtrail')

def audit_glue_encryption_compliance():
    """Comprehensive audit of Glue encryption compliance"""
    
    print("🔍 Glue Encryption Compliance Audit")
    print("=" * 60)
    print(f"Audit date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    compliance_report = {
        'data_catalog': {},
        'security_configurations': [],
        'jobs': [],
        'crawlers': [],
        'connections': [],
        'compliance_score': 0
    }
    
    # 1. Check Data Catalog encryption
    print("\\n1️⃣ Data Catalog Encryption Status")
    print("-" * 40)
    
    try:
        catalog_response = glue_client.get_data_catalog_encryption_settings()
        encryption_settings = catalog_response['DataCatalogEncryptionSettings']
        
        # Check encryption at rest
        encryption_at_rest = encryption_settings.get('EncryptionAtRest', {})
        catalog_mode = encryption_at_rest.get('CatalogEncryptionMode', 'DISABLED')
        
        compliance_report['data_catalog']['encryption_mode'] = catalog_mode
        compliance_report['data_catalog']['compliant'] = catalog_mode == 'SSE-KMS'
        
        if catalog_mode == 'SSE-KMS':
            kms_key = encryption_at_rest.get('SseAwsKmsKeyId')
            print(f"   ✅ Data Catalog encryption: ENABLED")
            print(f"   KMS Key: {kms_key}")
            compliance_report['data_catalog']['kms_key'] = kms_key
        else:
            print(f"   ❌ Data Catalog encryption: DISABLED")
        
        # Check connection password encryption
        conn_encryption = encryption_settings.get('ConnectionPasswordEncryption', {})
        password_encrypted = conn_encryption.get('ReturnConnectionPasswordEncrypted', False)
        
        compliance_report['data_catalog']['password_encryption'] = password_encrypted
        
        if password_encrypted:
            print(f"   ✅ Connection password encryption: ENABLED")
        else:
            print(f"   ❌ Connection password encryption: DISABLED")
        
    except ClientError as e:
        print(f"   ❌ Error checking Data Catalog encryption: {e}")
    
    # 2. Check Security Configurations
    print("\\n2️⃣ Security Configurations")
    print("-" * 40)
    
    try:
        security_configs = glue_client.get_security_configurations()
        
        for config in security_configs['SecurityConfigurations']:
            config_name = config['Name']
            encryption_config = config['EncryptionConfiguration']
            
            config_report = {
                'name': config_name,
                's3_encryption': False,
                'cloudwatch_encryption': False,
                'job_bookmarks_encryption': False,
                'compliant': False
            }
            
            print(f"   🔐 Configuration: {config_name}")
            
            # Check S3 encryption
            s3_encryption = encryption_config.get('S3Encryption', [])
            if s3_encryption:
                for s3_config in s3_encryption:
                    s3_mode = s3_config.get('S3EncryptionMode')
                    print(f"      S3 Encryption: {s3_mode}")
                    config_report['s3_encryption'] = s3_mode in ['SSE-KMS', 'CSE-KMS']
            
            # Check CloudWatch encryption
            cw_encryption = encryption_config.get('CloudWatchEncryption', {})
            if cw_encryption:
                cw_mode = cw_encryption.get('CloudWatchEncryptionMode')
                print(f"      CloudWatch Encryption: {cw_mode}")
                config_report['cloudwatch_encryption'] = cw_mode == 'SSE-KMS'
            
            # Check Job bookmarks encryption
            jb_encryption = encryption_config.get('JobBookmarksEncryption', {})
            if jb_encryption:
                jb_mode = jb_encryption.get('JobBookmarksEncryptionMode')
                print(f"      Job Bookmarks Encryption: {jb_mode}")
                config_report['job_bookmarks_encryption'] = jb_mode == 'CSE-KMS'
            
            # Determine compliance
            config_report['compliant'] = (
                config_report['s3_encryption'] and 
                config_report['cloudwatch_encryption'] and 
                config_report['job_bookmarks_encryption']
            )
            
            compliance_report['security_configurations'].append(config_report)
        
        print(f"   Total configurations: {len(security_configs['SecurityConfigurations'])}")
        
    except ClientError as e:
        print(f"   ❌ Error checking security configurations: {e}")
    
    # 3. Check Jobs encryption
    print("\\n3️⃣ ETL Jobs Encryption Status")
    print("-" * 40)
    
    try:
        jobs_response = glue_client.get_jobs()
        
        for job in jobs_response['Jobs']:
            job_name = job['Name']
            security_config = job.get('SecurityConfiguration')
            
            job_report = {
                'name': job_name,
                'security_configuration': security_config,
                'compliant': security_config is not None
            }
            
            if security_config:
                print(f"   ✅ Job: {job_name} (Security Config: {security_config})")
            else:
                print(f"   ❌ Job: {job_name} (No Security Configuration)")
            
            compliance_report['jobs'].append(job_report)
        
        job_compliance = sum(1 for job in compliance_report['jobs'] if job['compliant'])
        total_jobs = len(compliance_report['jobs'])
        
        print(f"   Jobs with encryption: {job_compliance}/{total_jobs}")
        
    except ClientError as e:
        print(f"   ❌ Error checking jobs: {e}")
    
    # 4. Check Crawlers
    print("\\n4️⃣ Crawlers Status")
    print("-" * 40)
    
    try:
        crawlers_response = glue_client.get_crawlers()
        
        for crawler in crawlers_response['Crawlers']:
            crawler_name = crawler['Name']
            database_name = crawler.get('DatabaseName')
            
            crawler_report = {
                'name': crawler_name,
                'database': database_name,
                'compliant': True  # Crawlers use Data Catalog encryption
            }
            
            print(f"   🔍 Crawler: {crawler_name} (Database: {database_name})")
            
            compliance_report['crawlers'].append(crawler_report)
        
        print(f"   Total crawlers: {len(crawlers_response['Crawlers'])}")
        
    except ClientError as e:
        print(f"   ❌ Error checking crawlers: {e}")
    
    # 5. Check Connections
    print("\\n5️⃣ Connections Encryption Status")
    print("-" * 40)
    
    try:
        connections_response = glue_client.get_connections()
        
        for connection in connections_response['ConnectionList']:
            connection_name = connection['Name']
            connection_type = connection['ConnectionType']
            
            connection_report = {
                'name': connection_name,
                'type': connection_type,
                'compliant': True  # Connections use Data Catalog encryption for passwords
            }
            
            print(f"   🔗 Connection: {connection_name} (Type: {connection_type})")
            
            compliance_report['connections'].append(connection_report)
        
        print(f"   Total connections: {len(connections_response['ConnectionList'])}")
        
    except ClientError as e:
        print(f"   ❌ Error checking connections: {e}")
    
    # Calculate overall compliance score
    compliance_factors = []
    
    if compliance_report['data_catalog']:
        compliance_factors.append(compliance_report['data_catalog'].get('compliant', False))
    
    if compliance_report['security_configurations']:
        config_compliance = sum(1 for config in compliance_report['security_configurations'] if config['compliant'])
        total_configs = len(compliance_report['security_configurations'])
        compliance_factors.append(config_compliance / total_configs if total_configs > 0 else 0)
    
    if compliance_report['jobs']:
        job_compliance = sum(1 for job in compliance_report['jobs'] if job['compliant'])
        total_jobs = len(compliance_report['jobs'])
        compliance_factors.append(job_compliance / total_jobs if total_jobs > 0 else 0)
    
    overall_compliance = sum(compliance_factors) / len(compliance_factors) if compliance_factors else 0
    compliance_report['compliance_score'] = overall_compliance * 100
    
    print(f"\\n🏆 Overall Compliance Score: {compliance_report['compliance_score']:.1f}%")
    
    return compliance_report

def monitor_glue_encryption_metrics():
    """Monitor CloudWatch metrics for Glue encryption"""
    
    try:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
        
        print("\\n📈 Glue Encryption Metrics (Last 24 hours)")
        print("=" * 50)
        
        # Get job success metrics
        job_success_response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/Glue',
            MetricName='glue.driver.aggregate.numCompletedTasks',
            Dimensions=[],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,
            Statistics=['Sum']
        )
        
        total_tasks = sum(dp['Sum'] for dp in job_success_response['Datapoints'])
        print(f"   Completed tasks: {total_tasks}")
        
        # Get job failure metrics
        job_failure_response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/Glue',
            MetricName='glue.driver.aggregate.numFailedTasks',
            Dimensions=[],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,
            Statistics=['Sum']
        )
        
        failed_tasks = sum(dp['Sum'] for dp in job_failure_response['Datapoints'])
        print(f"   Failed tasks: {failed_tasks}")
        
        # Calculate success rate
        if total_tasks > 0:
            success_rate = ((total_tasks - failed_tasks) / total_tasks) * 100
            print(f"   Success rate: {success_rate:.1f}%")
        
        return {
            'total_tasks': total_tasks,
            'failed_tasks': failed_tasks,
            'success_rate': success_rate if total_tasks > 0 else 0
        }
        
    except ClientError as e:
        print(f"❌ Error getting metrics: {e}")
        return {}

def audit_kms_usage_for_glue():
    """Audit KMS key usage for Glue services"""
    
    try:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=7)
        
        print("\\n🔐 KMS Usage Audit for Glue (Last 7 days)")
        print("=" * 50)
        
        # Look for KMS events related to Glue
        response = cloudtrail_client.lookup_events(
            LookupAttributes=[
                {
                    'AttributeKey': 'EventName',
                    'AttributeValue': 'GenerateDataKey'
                }
            ],
            StartTime=start_time,
            EndTime=end_time
        )
        
        glue_events = []
        for event in response['Events']:
            cloud_trail_event = json.loads(event.get('CloudTrailEvent', '{}'))
            user_identity = cloud_trail_event.get('userIdentity', {})
            
            if (user_identity.get('type') == 'AssumedRole' and 
                'glue' in user_identity.get('arn', '').lower()):
                glue_events.append(event)
        
        print(f"   KMS GenerateDataKey events from Glue: {len(glue_events)}")
        
        # Group by day
        daily_usage = {}
        for event in glue_events:
            event_date = event['EventTime'].date()
            daily_usage[event_date] = daily_usage.get(event_date, 0) + 1
        
        print(f"   Daily KMS usage:")
        for date, count in sorted(daily_usage.items()):
            print(f"      {date}: {count} requests")
        
        return glue_events
        
    except ClientError as e:
        print(f"❌ Error auditing KMS usage: {e}")
        return []

def generate_encryption_recommendations(compliance_report):
    """Generate recommendations based on compliance audit"""
    
    print("\\n💡 Encryption Recommendations")
    print("=" * 50)
    
    recommendations = []
    
    # Data Catalog recommendations
    if not compliance_report['data_catalog'].get('compliant', False):
        recommendations.append("Enable Data Catalog encryption with customer-managed KMS key")
    
    if not compliance_report['data_catalog'].get('password_encryption', False):
        recommendations.append("Enable connection password encryption")
    
    # Security configuration recommendations
    non_compliant_configs = [
        config for config in compliance_report['security_configurations'] 
        if not config['compliant']
    ]
    
    if non_compliant_configs:
        recommendations.append(f"Update {len(non_compliant_configs)} security configurations to enable all encryption options")
    
    # Job recommendations
    non_compliant_jobs = [
        job for job in compliance_report['jobs'] 
        if not job['compliant']
    ]
    
    if non_compliant_jobs:
        recommendations.append(f"Add security configurations to {len(non_compliant_jobs)} ETL jobs")
    
    # General recommendations
    recommendations.extend([
        "Enable automatic KMS key rotation",
        "Implement CloudWatch monitoring for encryption events",
        "Regular compliance audits and reporting",
        "Use least privilege IAM policies for KMS access"
    ])
    
    for i, recommendation in enumerate(recommendations, 1):
        print(f"   {i}. {recommendation}")
    
    return recommendations

# Main monitoring function
def monitor_glue_encryption():
    """Comprehensive Glue encryption monitoring"""
    
    print("🚀 Starting Glue Encryption Monitoring")
    print("=" * 60)
    
    # Run compliance audit
    compliance_report = audit_glue_encryption_compliance()
    
    # Monitor metrics
    encryption_metrics = monitor_glue_encryption_metrics()
    
    # Audit KMS usage
    kms_events = audit_kms_usage_for_glue()
    
    # Generate recommendations
    recommendations = generate_encryption_recommendations(compliance_report)
    
    # Summary
    print(f"\\n📊 Monitoring Summary")
    print("=" * 50)
    print(f"   Compliance Score: {compliance_report['compliance_score']:.1f}%")
    print(f"   Total Jobs: {len(compliance_report['jobs'])}")
    print(f"   Encrypted Jobs: {sum(1 for job in compliance_report['jobs'] if job['compliant'])}")
    print(f"   Security Configurations: {len(compliance_report['security_configurations'])}")
    print(f"   KMS Events (7 days): {len(kms_events)}")
    print(f"   Recommendations: {len(recommendations)}")
    
    return {
        'compliance_report': compliance_report,
        'encryption_metrics': encryption_metrics,
        'kms_events': kms_events,
        'recommendations': recommendations
    }

# Execute monitoring
if __name__ == "__main__":
    monitoring_results = monitor_glue_encryption()
    
    print("\\n✅ Glue encryption monitoring completed!")
    print("   Review the compliance report and implement recommendations.")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def emr_encryption_tab():
    """Amazon EMR Encryption tab content"""
    st.markdown("## 🔥 Amazon EMR Encryption")
    st.markdown("*Comprehensive encryption for big data processing with Apache Spark, Hadoop, and more*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 EMR Encryption Layers
    Amazon EMR provides multiple layers of encryption for comprehensive data protection:
    - **EBS Encryption**: Encrypt EBS volumes attached to cluster instances
    - **Local Disk Encryption**: Encrypt local instance store volumes
    - **In-Transit Encryption**: Encrypt data in motion between nodes and applications
    - **At-Rest Encryption**: Encrypt data stored in HDFS and S3
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # EMR encryption architecture
    st.markdown("#### 🏗️ EMR Encryption Architecture")
    common.mermaid(create_emr_encryption_architecture(), height=700)
    
    # Interactive encryption configuration
    st.markdown("#### ⚙️ EMR Encryption Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        ebs_encryption = st.checkbox("Enable EBS Encryption", value=True)
        if ebs_encryption:
            ebs_key_type = st.selectbox("EBS Key Type", ["AWS Managed", "Customer Managed"])
        
        local_disk_encryption = st.checkbox("Enable Local Disk Encryption", value=True)
        if local_disk_encryption:
            local_disk_key_type = st.selectbox("Local Disk Key Type", ["AWS Managed", "Customer Managed"])
    
    with col2:
        in_transit_encryption = st.checkbox("Enable In-Transit Encryption", value=True)
        if in_transit_encryption:
            tls_version = st.selectbox("TLS Version", ["TLS 1.2", "TLS 1.3"])
        
        at_rest_encryption = st.checkbox("Enable At-Rest Encryption", value=True)
        if at_rest_encryption:
            at_rest_provider = st.selectbox("At-Rest Provider", ["HDFS", "S3"])
    
    # Calculate encryption score
    encryption_score = calculate_emr_encryption_score(
        ebs_encryption, local_disk_encryption, in_transit_encryption, at_rest_encryption
    )
    
    st.markdown('<div class="security-card">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 🛡️ EMR Encryption Configuration
    **EBS Encryption**: {'✅ Enabled' if ebs_encryption else '❌ Disabled'}  
    **Local Disk Encryption**: {'✅ Enabled' if local_disk_encryption else '❌ Disabled'}  
    **In-Transit Encryption**: {'✅ Enabled' if in_transit_encryption else '❌ Disabled'}  
    **At-Rest Encryption**: {'✅ Enabled' if at_rest_encryption else '❌ Disabled'}  
    **Encryption Score**: {encryption_score}/100
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Encryption types comparison
    st.markdown("#### 🔒 EMR Encryption Types")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="encryption-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 💾 Storage Encryption
        **EBS Volumes:**
        - Root and additional volumes
        - KMS or customer-provided keys
        - Transparent to applications
        
        **Local Disks:**
        - Instance store volumes
        - HDFS data directories
        - Temp and work directories
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="encryption-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔗 Network Encryption
        **In-Transit:**
        - TLS for HTTP/HTTPS
        - Node-to-node communication
        - Client-to-cluster connections
        
        **At-Rest:**
        - HDFS transparent encryption
        - S3 server-side encryption
        - Application-level encryption
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Application-specific encryption
    st.markdown("#### 🛠️ Application Encryption Support")
    
    app_encryption_data = {
        'Application': ['Apache Spark', 'Apache Hive', 'Apache HBase', 'Hadoop MapReduce', 'Presto', 'Apache Flink'],
        'In-Transit': ['✅ TLS 1.2+', '✅ TLS 1.2+', '✅ TLS 1.2+', '✅ TLS 1.2+', '✅ TLS 1.2+', '✅ TLS 1.2+'],
        'At-Rest': ['✅ HDFS/S3', '✅ HDFS/S3', '✅ HDFS/S3', '✅ HDFS/S3', '✅ HDFS/S3', '✅ HDFS/S3'],
        'Key Management': ['KMS/Custom', 'KMS/Custom', 'KMS/Custom', 'KMS/Custom', 'KMS/Custom', 'KMS/Custom']
    }
    
    df_app_encryption = pd.DataFrame(app_encryption_data)
    st.dataframe(df_app_encryption, use_container_width=True)
    
    # Performance impact analysis
    st.markdown("#### ⚡ Encryption Performance Impact")
    
    performance_data = {
        'Operation': ['Spark SQL', 'HDFS Read/Write', 'S3 Operations', 'Network Transfer', 'Cluster Launch'],
        'No Encryption': [100, 100, 100, 100, 100],
        'With Encryption': [95, 85, 88, 80, 90],
        'Impact Level': ['Low', 'Medium', 'Medium', 'High', 'Low']
    }
    
    df_performance = pd.DataFrame(performance_data)
    
    # Create performance comparison chart
    fig = px.bar(df_performance, x='Operation', y=['No Encryption', 'With Encryption'],
                 title='EMR Performance Impact of Encryption',
                 color_discrete_map={'No Encryption': AWS_COLORS['light_blue'], 
                                   'With Encryption': AWS_COLORS['primary']})
    fig.update_layout(yaxis_title="Performance (%)", showlegend=True)
    st.plotly_chart(fig, use_container_width=True)
    
    # Code examples
    st.markdown("#### 💻 EMR Encryption Implementation")
    
    tab1, tab2, tab3 = st.tabs(["Cluster Creation", "Security Configuration", "Monitoring"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create Encrypted EMR Cluster

import boto3
import json
from botocore.exceptions import ClientError

emr_client = boto3.client('emr')
kms_client = boto3.client('kms')

def create_emr_encryption_keys():
    """Create KMS keys for EMR encryption"""
    
    print("🔐 Creating KMS keys for EMR encryption")
    print("=" * 50)
    
    # EMR service key policy
    emr_key_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Enable IAM User Permissions",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::123456789012:root"
                },
                "Action": "kms:*",
                "Resource": "*"
            },
            {
                "Sid": "Allow EMR Service",
                "Effect": "Allow",
                "Principal": {
                    "Service": "elasticmapreduce.amazonaws.com"
                },
                "Action": [
                    "kms:Decrypt",
                    "kms:GenerateDataKey",
                    "kms:CreateGrant"
                ],
                "Resource": "*"
            },
            {
                "Sid": "Allow EC2 Service",
                "Effect": "Allow",
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                },
                "Action": [
                    "kms:Decrypt",
                    "kms:GenerateDataKey",
                    "kms:CreateGrant"
                ],
                "Resource": "*"
            }
        ]
    }
    
    keys = {}
    
    try:
        # Create key for EBS encryption
        ebs_response = kms_client.create_key(
            Description="EMR EBS encryption key",
            KeyUsage='ENCRYPT_DECRYPT',
            KeySpec='SYMMETRIC_DEFAULT',
            Policy=json.dumps(emr_key_policy),
            Tags=[
                {'TagKey': 'Service', 'TagValue': 'EMR'},
                {'TagKey': 'Purpose', 'TagValue': 'EBS'}
            ]
        )
        
        ebs_key_id = ebs_response['KeyMetadata']['KeyId']
        keys['ebs'] = ebs_key_id
        
        # Create alias
        kms_client.create_alias(
            AliasName='alias/emr-ebs-encryption',
            TargetKeyId=ebs_key_id
        )
        
        print(f"✅ EBS encryption key created: {ebs_key_id}")
        
        # Create key for local disk encryption
        local_response = kms_client.create_key(
            Description="EMR local disk encryption key",
            KeyUsage='ENCRYPT_DECRYPT',
            KeySpec='SYMMETRIC_DEFAULT',
            Policy=json.dumps(emr_key_policy),
            Tags=[
                {'TagKey': 'Service', 'TagValue': 'EMR'},
                {'TagKey': 'Purpose', 'TagValue': 'LocalDisk'}
            ]
        )
        
        local_key_id = local_response['KeyMetadata']['KeyId']
        keys['local'] = local_key_id
        
        # Create alias
        kms_client.create_alias(
            AliasName='alias/emr-local-disk-encryption',
            TargetKeyId=local_key_id
        )
        
        print(f"✅ Local disk encryption key created: {local_key_id}")
        
        # Enable rotation
        for key_id in [ebs_key_id, local_key_id]:
            kms_client.enable_key_rotation(KeyId=key_id)
        
        print("✅ Key rotation enabled for both keys")
        
        return keys
        
    except ClientError as e:
        print(f"❌ Error creating keys: {e}")
        return {}

def create_emr_security_configuration():
    """Create EMR security configuration with encryption"""
    
    try:
        # Security configuration with all encryption options
        security_config = {
            'Name': 'EMREncryptionConfig',
            'SecurityConfiguration': json.dumps({
                'EncryptionConfiguration': {
                    'EnableInTransitEncryption': True,
                    'EnableAtRestEncryption': True,
                    'InTransitEncryptionConfiguration': {
                        'TLSCertificateConfiguration': {
                            'CertificateProviderType': 'PEM',
                            'S3Object': 's3://emr-certificates/server.pem'
                        }
                    },
                    'AtRestEncryptionConfiguration': {
                        'S3EncryptionConfiguration': {
                            'EncryptionMode': 'SSE-KMS',
                            'AwsKmsKey': 'alias/emr-s3-encryption'
                        },
                        'LocalDiskEncryptionConfiguration': {
                            'EncryptionKeyProviderType': 'AwsKms',
                            'AwsKmsKey': 'alias/emr-local-disk-encryption'
                        }
                    }
                },
                'AuthenticationConfiguration': {
                    'KerberosConfiguration': {
                        'Provider': 'ClusterDedicatedKdc',
                        'ClusterDedicatedKdcConfiguration': {
                            'TicketLifetimeInHours': 24,
                            'CrossRealmTrustConfiguration': {
                                'Realm': 'EMR.LOCAL',
                                'Domain': 'emr.local',
                                'AdminServer': 'emr.local',
                                'KdcServer': 'emr.local'
                            }
                        }
                    }
                }
            })
        }
        
        response = emr_client.create_security_configuration(**security_config)
        
        config_name = response['Name']
        print(f"✅ Security configuration created: {config_name}")
        
        return config_name
        
    except ClientError as e:
        print(f"❌ Error creating security configuration: {e}")
        return None

def create_encrypted_emr_cluster(security_config_name, ebs_key_id, local_key_id):
    """Create fully encrypted EMR cluster"""
    
    try:
        # Cluster configuration
        cluster_config = {
            'Name': 'encrypted-analytics-cluster',
            'ReleaseLabel': 'emr-6.10.0',
            'Applications': [
                {'Name': 'Spark'},
                {'Name': 'Hive'},
                {'Name': 'Hadoop'},
                {'Name': 'Hue'},
                {'Name': 'Zeppelin'},
                {'Name': 'Livy'}
            ],
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': 'Master',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp3',
                                        'SizeInGB': 100,
                                        'Encrypted': True,
                                        'KmsKeyId': ebs_key_id
                                    },
                                    'VolumesPerInstance': 1
                                }
                            ]
                        }
                    },
                    {
                        'Name': 'Core',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 2,
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp3',
                                        'SizeInGB': 200,
                                        'Encrypted': True,
                                        'KmsKeyId': ebs_key_id
                                    },
                                    'VolumesPerInstance': 2
                                }
                            ]
                        }
                    },
                    {
                        'Name': 'Task',
                        'Market': 'SPOT',
                        'InstanceRole': 'TASK',
                        'InstanceType': 'm5.large',
                        'InstanceCount': 3,
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp3',
                                        'SizeInGB': 100,
                                        'Encrypted': True,
                                        'KmsKeyId': ebs_key_id
                                    },
                                    'VolumesPerInstance': 1
                                }
                            ]
                        }
                    }
                ],
                'Ec2KeyName': 'emr-key-pair',
                'Ec2SubnetId': 'subnet-12345678',
                'EmrManagedMasterSecurityGroup': 'sg-master-12345678',
                'EmrManagedSlaveSecurityGroup': 'sg-slave-12345678',
                'AdditionalMasterSecurityGroups': ['sg-additional-12345678'],
                'AdditionalSlaveSecurityGroups': ['sg-additional-12345678']
            },
            'ServiceRole': 'EMR_DefaultRole',
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
            
            # Security configuration
            'SecurityConfiguration': security_config_name,
            
            # Logging
            'LogUri': 's3://emr-logs-bucket/cluster-logs/',
            
            # Bootstrap actions
            'BootstrapActions': [
                {
                    'Name': 'Install Additional Packages',
                    'ScriptBootstrapAction': {
                        'Path': 's3://emr-bootstrap-bucket/install-packages.sh',
                        'Args': ['--enable-encryption-monitoring']
                    }
                }
            ],
            
            # Configuration
            'Configurations': [
                {
                    'Classification': 'spark-defaults',
                    'Properties': {
                        'spark.sql.execution.arrow.pyspark.enabled': 'true',
                        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                        'spark.sql.adaptive.enabled': 'true',
                        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                        # Encryption-specific configurations
                        'spark.ssl.enabled': 'true',
                        'spark.ssl.protocol': 'TLSv1.2',
                        'spark.authenticate': 'true',
                        'spark.authenticate.enableSaslEncryption': 'true',
                        'spark.io.encryption.enabled': 'true',
                        'spark.network.crypto.enabled': 'true'
                    }
                },
                {
                    'Classification': 'hdfs-site',
                    'Properties': {
                        'dfs.encrypt.data.transfer': 'true',
                        'dfs.encrypt.data.transfer.algorithm': 'AES/CTR/NoPadding',
                        'dfs.encryption.key.provider.uri': f'kms://https://kms.us-west-2.amazonaws.com:443/kms'
                    }
                },
                {
                    'Classification': 'core-site',
                    'Properties': {
                        'hadoop.security.authentication': 'kerberos',
                        'hadoop.security.authorization': 'true',
                        'hadoop.rpc.protection': 'privacy'
                    }
                }
            ],
            
            # Tags
            'Tags': [
                {'Key': 'Environment', 'Value': 'Production'},
                {'Key': 'DataClassification', 'Value': 'Confidential'},
                {'Key': 'Encryption', 'Value': 'Enabled'},
                {'Key': 'Owner', 'Value': 'DataEngineering'}
            ]
        }
        
        response = emr_client.run_job_flow(**cluster_config)
        
        cluster_id = response['JobFlowId']
        print(f"✅ Encrypted EMR cluster created: {cluster_id}")
        
        return cluster_id
        
    except ClientError as e:
        print(f"❌ Error creating EMR cluster: {e}")
        return None

def wait_for_cluster_ready(cluster_id, timeout_minutes=30):
    """Wait for cluster to be ready"""
    
    import time
    
    print(f"⏳ Waiting for cluster {cluster_id} to be ready...")
    
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    
    while time.time() - start_time < timeout_seconds:
        try:
            response = emr_client.describe_cluster(ClusterId=cluster_id)
            cluster_state = response['Cluster']['Status']['State']
            
            print(f"   Cluster state: {cluster_state}")
            
            if cluster_state == 'WAITING':
                print("✅ Cluster is ready!")
                return True
            elif cluster_state in ['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS']:
                print(f"❌ Cluster failed with state: {cluster_state}")
                return False
            else:
                time.sleep(60)  # Check every minute
                
        except ClientError as e:
            print(f"❌ Error checking cluster status: {e}")
            return False
    
    print(f"❌ Cluster setup timed out after {timeout_minutes} minutes")
    return False

# Main setup function
def setup_encrypted_emr_cluster():
    """Complete encrypted EMR cluster setup"""
    
    print("🚀 Setting up Encrypted EMR Cluster")
    print("=" * 50)
    
    # Step 1: Create encryption keys
    keys = create_emr_encryption_keys()
    
    if not keys:
        print("❌ Failed to create encryption keys")
        return False
    
    # Step 2: Create security configuration
    security_config_name = create_emr_security_configuration()
    
    if not security_config_name:
        print("❌ Failed to create security configuration")
        return False
    
    # Step 3: Create encrypted cluster
    cluster_id = create_encrypted_emr_cluster(
        security_config_name,
        keys['ebs'],
        keys['local']
    )
    
    if not cluster_id:
        print("❌ Failed to create EMR cluster")
        return False
    
    # Step 4: Wait for cluster to be ready
    if wait_for_cluster_ready(cluster_id):
        print(f"\\n✅ Encrypted EMR cluster setup completed!")
        print(f"   Cluster ID: {cluster_id}")
        print(f"   EBS Key: {keys['ebs']}")
        print(f"   Local Disk Key: {keys['local']}")
        print(f"   Security Configuration: {security_config_name}")
        
        return True
    else:
        print("❌ Cluster setup failed")
        return False

# Execute setup
if __name__ == "__main__":
    success = setup_encrypted_emr_cluster()
    
    if success:
        print("\\n🎉 Encrypted EMR cluster is ready for big data processing!")
    else:
        print("\\n❌ Setup failed - check error messages above")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# EMR Security Configuration and Advanced Encryption

import boto3
import json
from botocore.exceptions import ClientError

emr_client = boto3.client('emr')

def create_advanced_security_configuration():
    """Create advanced security configuration with multiple encryption options"""
    
    try:
        # Advanced security configuration
        security_config = {
            'Name': 'EMRAdvancedEncryptionConfig',
            'SecurityConfiguration': json.dumps({
                'EncryptionConfiguration': {
                    'EnableInTransitEncryption': True,
                    'EnableAtRestEncryption': True,
                    'InTransitEncryptionConfiguration': {
                        'TLSCertificateConfiguration': {
                            'CertificateProviderType': 'PEM',
                            'S3Object': 's3://emr-certificates/server.pem'
                        }
                    },
                    'AtRestEncryptionConfiguration': {
                        'S3EncryptionConfiguration': {
                            'EncryptionMode': 'SSE-KMS',
                            'AwsKmsKey': 'alias/emr-s3-encryption'
                        },
                        'LocalDiskEncryptionConfiguration': {
                            'EncryptionKeyProviderType': 'AwsKms',
                            'AwsKmsKey': 'alias/emr-local-disk-encryption'
                        }
                    }
                },
                'AuthenticationConfiguration': {
                    'KerberosConfiguration': {
                        'Provider': 'ClusterDedicatedKdc',
                        'ClusterDedicatedKdcConfiguration': {
                            'TicketLifetimeInHours': 24,
                            'CrossRealmTrustConfiguration': {
                                'Realm': 'EMR.LOCAL',
                                'Domain': 'emr.local',
                                'AdminServer': 'emr.local',
                                'KdcServer': 'emr.local'
                            }
                        }
                    }
                },
                'AuthorizationConfiguration': {
                    'IAMConfiguration': {
                        'EnableApplicationScopedIAMRole': True,
                        'ApplicationScopedIAMRoleConfiguration': {
                            'PropagateSourceIdentity': True
                        }
                    },
                    'LakeFormationConfiguration': {
                        'AuthorizedSessionTagValue': 'EMRStudio'
                    }
                }
            })
        }
        
        response = emr_client.create_security_configuration(**security_config)
        
        config_name = response['Name']
        print(f"✅ Advanced security configuration created: {config_name}")
        
        return config_name
        
    except ClientError as e:
        print(f"❌ Error creating security configuration: {e}")
        return None

def configure_hdfs_encryption():
    """Configure HDFS encryption zones"""
    
    # This configuration would be applied via EMR steps
    hdfs_encryption_config = {
        'Classification': 'hdfs-site',
        'Properties': {
            # Enable data transfer encryption
            'dfs.encrypt.data.transfer': 'true',
            'dfs.encrypt.data.transfer.algorithm': 'AES/CTR/NoPadding',
            'dfs.encrypt.data.transfer.cipher.suites': 'AES/CTR/NoPadding',
            
            # Key management
            'dfs.encryption.key.provider.uri': 'kms://https://kms.us-west-2.amazonaws.com:443/kms',
            'hadoop.security.key.provider.path': 'kms://https://kms.us-west-2.amazonaws.com:443/kms',
            
            # Encryption zone configuration
            'dfs.namenode.list.encryption.zones.num.responses': '100',
            'dfs.encryption.key.default.cipher': 'AES/CTR/NoPadding',
            
            # Additional security
            'dfs.block.access.token.enable': 'true',
            'dfs.namenode.delegation.token.max-lifetime': '604800000',
            'dfs.namenode.delegation.token.renew-interval': '86400000'
        }
    }
    
    # Script to create encryption zones
    encryption_zone_script = """
#!/bin/bash

# Create encryption zones for different data types
echo "Creating HDFS encryption zones..."

# Create key for customer data
hadoop key create customer_data_key -size 256 -cipher AES/CTR/NoPadding

# Create key for analytics data
hadoop key create analytics_key -size 256 -cipher AES/CTR/NoPadding

# Create key for logs
hadoop key create logs_key -size 256 -cipher AES/CTR/NoPadding

# Create directories
hdfs dfs -mkdir -p /encrypted/customer_data
hdfs dfs -mkdir -p /encrypted/analytics
hdfs dfs -mkdir -p /encrypted/logs

# Create encryption zones
hdfs crypto -createZone -keyName customer_data_key -path /encrypted/customer_data
hdfs crypto -createZone -keyName analytics_key -path /encrypted/analytics
hdfs crypto -createZone -keyName logs_key -path /encrypted/logs

# Set permissions
hdfs dfs -chmod 750 /encrypted/customer_data
hdfs dfs -chmod 750 /encrypted/analytics
hdfs dfs -chmod 755 /encrypted/logs

echo "Encryption zones created successfully!"

# List encryption zones
hdfs crypto -listZones

# Test encryption
echo "Testing encryption..."
hdfs dfs -put /etc/hosts /encrypted/customer_data/test_file
hdfs dfs -ls /encrypted/customer_data/
    """
    
    print("📁 HDFS Encryption Configuration:")
    print(json.dumps(hdfs_encryption_config, indent=2))
    
    print("\\n🔐 Encryption Zone Setup Script:")
    print(encryption_zone_script)
    
    return hdfs_encryption_config, encryption_zone_script

def configure_spark_encryption():
    """Configure Spark encryption settings"""
    
    spark_encryption_config = {
        'Classification': 'spark-defaults',
        'Properties': {
            # SSL/TLS configuration
            'spark.ssl.enabled': 'true',
            'spark.ssl.protocol': 'TLSv1.2',
            'spark.ssl.keyStore': '/etc/ssl/spark/spark-keystore.jks',
            'spark.ssl.keyStorePassword': 'sparkssl',
            'spark.ssl.trustStore': '/etc/ssl/spark/spark-truststore.jks',
            'spark.ssl.trustStorePassword': 'sparkssl',
            
            # Authentication
            'spark.authenticate': 'true',
            'spark.authenticate.secret': 'spark-secret-key',
            'spark.authenticate.enableSaslEncryption': 'true',
            
            # I/O encryption
            'spark.io.encryption.enabled': 'true',
            'spark.io.encryption.keySizeBits': '256',
            'spark.io.encryption.keyFactoryAlgorithm': 'PBKDF2WithHmacSHA1',
            
            # Network encryption
            'spark.network.crypto.enabled': 'true',
            'spark.network.crypto.keyLength': '256',
            'spark.network.crypto.keyFactoryAlgorithm': 'PBKDF2WithHmacSHA1',
            
            # RPC encryption
            'spark.rpc.encryption.enabled': 'true',
            'spark.rpc.encryption.keySizeBits': '256',
            'spark.rpc.encryption.keyFactoryAlgorithm': 'PBKDF2WithHmacSHA1',
            
            # Local storage encryption
            'spark.local.dir.encryption.enabled': 'true',
            
            # SQL encryption
            'spark.sql.execution.arrow.pyspark.enabled': 'true',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            
            # Serialization
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.kryo.registrator': 'org.apache.spark.serializer.KryoRegistrator',
            
            # Event logging (encrypted)
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': 's3://emr-encrypted-logs/spark-events/',
            'spark.eventLog.rolling.enabled': 'true',
            'spark.eventLog.rolling.maxFileSize': '128m',
            
            # History server
            'spark.history.fs.logDirectory': 's3://emr-encrypted-logs/spark-events/',
            'spark.history.ui.port': '18080'
        }
    }
    
    print("⚡ Spark Encryption Configuration:")
    print(json.dumps(spark_encryption_config, indent=2))
    
    return spark_encryption_config

def configure_hive_encryption():
    """Configure Hive encryption settings"""
    
    hive_encryption_config = {
        'Classification': 'hive-site',
        'Properties': {
            # Metastore encryption
            'hive.metastore.uris': 'thrift://localhost:9083',
            'hive.metastore.client.connect.retry.delay': '5s',
            'hive.metastore.client.socket.timeout': '1800s',
            
            # SSL configuration
            'hive.server2.use.SSL': 'true',
            'hive.server2.keystore.path': '/etc/ssl/hive/hive-keystore.jks',
            'hive.server2.keystore.password': 'hivessl',
            
            # Authentication
            'hive.server2.authentication': 'KERBEROS',
            'hive.server2.authentication.kerberos.principal': 'hive/_HOST@EMR.LOCAL',
            'hive.server2.authentication.kerberos.keytab': '/etc/hive/hive.keytab',
            
            # Authorization
            'hive.security.authorization.enabled': 'true',
            'hive.security.authorization.manager': 'org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory',
            'hive.security.authenticator.manager': 'org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator',
            
            # Encryption
            'hive.exec.compress.output': 'true',
            'hive.exec.compress.intermediate': 'true',
            'mapred.output.compression.codec': 'org.apache.hadoop.io.compress.GzipCodec',
            'mapred.output.compression.type': 'BLOCK',
            
            # Column-level encryption
            'hive.exec.column.encryption.enabled': 'true',
            'hive.exec.column.encryption.algorithm': 'AES',
            'hive.exec.column.encryption.key.provider': 'hadoop.kms',
            
            # Temporary files
            'hive.exec.scratchdir': '/tmp/hive-encrypted',
            'hive.exec.local.scratchdir': '/tmp/hive-local-encrypted'
        }
    }
    
    print("🐝 Hive Encryption Configuration:")
    print(json.dumps(hive_encryption_config, indent=2))
    
    return hive_encryption_config

def create_encryption_bootstrap_script():
    """Create bootstrap script for additional encryption setup"""
    
    bootstrap_script = """#!/bin/bash

# EMR Encryption Bootstrap Script
echo "Starting EMR encryption bootstrap..."

# Update system
sudo yum update -y

# Install additional security tools
sudo yum install -y openssl stunnel4

# Create SSL certificate directories
sudo mkdir -p /etc/ssl/spark
sudo mkdir -p /etc/ssl/hive
sudo mkdir -p /etc/ssl/hadoop

# Generate SSL certificates for Spark
sudo openssl req -x509 -newkey rsa:4096 -keyout /etc/ssl/spark/spark-key.pem -out /etc/ssl/spark/spark-cert.pem -days 365 -nodes -subj "/CN=spark-cluster"

# Create Java keystore for Spark
sudo keytool -genkeypair -alias spark -keyalg RSA -keysize 4096 -keystore /etc/ssl/spark/spark-keystore.jks -storepass sparkssl -keypass sparkssl -dname "CN=spark-cluster"

# Create truststore
sudo keytool -export -alias spark -file /etc/ssl/spark/spark-cert.crt -keystore /etc/ssl/spark/spark-keystore.jks -storepass sparkssl
sudo keytool -import -alias spark -file /etc/ssl/spark/spark-cert.crt -keystore /etc/ssl/spark/spark-truststore.jks -storepass sparkssl -noprompt

# Generate SSL certificates for Hive
sudo openssl req -x509 -newkey rsa:4096 -keyout /etc/ssl/hive/hive-key.pem -out /etc/ssl/hive/hive-cert.pem -days 365 -nodes -subj "/CN=hive-metastore"

# Create Java keystore for Hive
sudo keytool -genkeypair -alias hive -keyalg RSA -keysize 4096 -keystore /etc/ssl/hive/hive-keystore.jks -storepass hivessl -keypass hivessl -dname "CN=hive-metastore"

# Set proper permissions
sudo chmod 600 /etc/ssl/spark/spark-key.pem
sudo chmod 600 /etc/ssl/hive/hive-key.pem
sudo chmod 644 /etc/ssl/spark/spark-cert.pem
sudo chmod 644 /etc/ssl/hive/hive-cert.pem

# Create encryption monitoring script
sudo cat > /usr/local/bin/check-encryption.sh << 'EOF'
#!/bin/bash

echo "=== EMR Encryption Status Check ==="
echo "Date: $(date)"
echo

# Check EBS encryption
echo "1. EBS Volume Encryption:"
lsblk -f | grep -E "crypto|luks" && echo "   ✅ EBS volumes encrypted" || echo "   ❌ EBS volumes not encrypted"

# Check HDFS encryption
echo "2. HDFS Encryption Zones:"
hdfs crypto -listZones 2>/dev/null && echo "   ✅ HDFS encryption zones configured" || echo "   ❌ HDFS encryption zones not found"

# Check Spark encryption
echo "3. Spark Encryption:"
spark-submit --conf spark.sql.execution.arrow.pyspark.enabled=true --conf spark.ssl.enabled=true --version 2>/dev/null && echo "   ✅ Spark encryption enabled" || echo "   ❌ Spark encryption not enabled"

# Check network encryption
echo "4. Network Encryption:"
sudo netstat -tlnp | grep -E ":8080|:8088|:9000" | head -5
echo "   Check for encrypted connections above"

# Check log encryption
echo "5. Log Encryption:"
aws s3 ls s3://emr-encrypted-logs/ --region us-west-2 2>/dev/null && echo "   ✅ Encrypted log storage accessible" || echo "   ❌ Encrypted log storage not accessible"

echo "=== End of Encryption Check ==="
EOF

sudo chmod +x /usr/local/bin/check-encryption.sh

# Create cron job for encryption monitoring
echo "0 */6 * * * /usr/local/bin/check-encryption.sh >> /var/log/encryption-check.log 2>&1" | sudo crontab -

# Install CloudWatch agent for monitoring
sudo yum install -y amazon-cloudwatch-agent

# Configure CloudWatch agent
sudo cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << 'EOF'
{
    "agent": {
        "metrics_collection_interval": 60,
        "run_as_user": "root"
    },
    "logs": {
        "logs_collected": {
            "files": {
                "collect_list": [
                    {
                        "file_path": "/var/log/encryption-check.log",
                        "log_group_name": "/aws/emr/encryption-monitoring",
                        "log_stream_name": "{instance_id}-encryption-check"
                    }
                ]
            }
        }
    },
    "metrics": {
        "namespace": "AWS/EMR/Encryption",
        "metrics_collected": {
            "cpu": {
                "measurement": [
                    "cpu_usage_idle",
                    "cpu_usage_iowait",
                    "cpu_usage_user",
                    "cpu_usage_system"
                ],
                "metrics_collection_interval": 60
            },
            "disk": {
                "measurement": [
                    "used_percent"
                ],
                "metrics_collection_interval": 60,
                "resources": [
                    "*"
                ]
            },
            "mem": {
                "measurement": [
                    "mem_used_percent"
                ],
                "metrics_collection_interval": 60
            }
        }
    }
}
EOF

# Start CloudWatch agent
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s

# Run initial encryption check
/usr/local/bin/check-encryption.sh

echo "EMR encryption bootstrap completed successfully!"
    """
    
    print("🚀 EMR Encryption Bootstrap Script:")
    print(bootstrap_script)
    
    # Save to S3 for use in cluster creation
    print("\\n📝 Save this script to S3 as 'emr-encryption-bootstrap.sh'")
    print("   Use it in the BootstrapActions of your EMR cluster")
    
    return bootstrap_script

# Main configuration function
def setup_emr_encryption_configurations():
    """Set up all EMR encryption configurations"""
    
    print("🔐 Setting up EMR Encryption Configurations")
    print("=" * 60)
    
    # Create advanced security configuration
    security_config = create_advanced_security_configuration()
    
    # Configure HDFS encryption
    hdfs_config, hdfs_script = configure_hdfs_encryption()
    
    # Configure Spark encryption
    spark_config = configure_spark_encryption()
    
    # Configure Hive encryption
    hive_config = configure_hive_encryption()
    
    # Create bootstrap script
    bootstrap_script = create_encryption_bootstrap_script()
    
    print(f"\\n✅ EMR encryption configurations created!")
    print(f"   Security Configuration: {security_config}")
    print(f"   HDFS Encryption: Configured")
    print(f"   Spark Encryption: Configured")
    print(f"   Hive Encryption: Configured")
    print(f"   Bootstrap Script: Ready")
    
    return {
        'security_config': security_config,
        'hdfs_config': hdfs_config,
        'spark_config': spark_config,
        'hive_config': hive_config,
        'bootstrap_script': bootstrap_script
    }

# Execute configuration setup
if __name__ == "__main__":
    configurations = setup_emr_encryption_configurations()
    
    print("\\n🎉 EMR encryption configurations ready for deployment!")
    print("   Apply these configurations when creating your EMR cluster")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# EMR Encryption Monitoring and Compliance

import boto3
import json
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

emr_client = boto3.client('emr')
cloudwatch_client = boto3.client('cloudwatch')
ec2_client = boto3.client('ec2')

def audit_emr_encryption_compliance():
    """Comprehensive audit of EMR encryption compliance"""
    
    print("🔍 EMR Encryption Compliance Audit")
    print("=" * 60)
    print(f"Audit date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    compliance_report = {
        'clusters': [],
        'security_configurations': [],
        'overall_score': 0,
        'recommendations': []
    }
    
    # 1. Check Security Configurations
    print("\\n1️⃣ Security Configurations Analysis")
    print("-" * 50)
    
    try:
        security_configs = emr_client.list_security_configurations()
        
        for config in security_configs['SecurityConfigurations']:
            config_name = config['Name']
            
            # Get detailed configuration
            config_detail = emr_client.describe_security_configuration(
                Name=config_name
            )
            
            security_config = json.loads(config_detail['SecurityConfiguration'])
            
            config_report = {
                'name': config_name,
                'in_transit_encryption': False,
                'at_rest_encryption': False,
                'authentication': False,
                'authorization': False,
                'compliant': False
            }
            
            print(f"   🔐 Configuration: {config_name}")
            
            # Check encryption settings
            encryption_config = security_config.get('EncryptionConfiguration', {})
            
            if encryption_config.get('EnableInTransitEncryption'):
                config_report['in_transit_encryption'] = True
                print("      ✅ In-transit encryption: ENABLED")
            else:
                print("      ❌ In-transit encryption: DISABLED")
            
            if encryption_config.get('EnableAtRestEncryption'):
                config_report['at_rest_encryption'] = True
                print("      ✅ At-rest encryption: ENABLED")
            else:
                print("      ❌ At-rest encryption: DISABLED")
            
            # Check authentication
            auth_config = security_config.get('AuthenticationConfiguration', {})
            if auth_config.get('KerberosConfiguration'):
                config_report['authentication'] = True
                print("      ✅ Kerberos authentication: ENABLED")
            else:
                print("      ❌ Kerberos authentication: DISABLED")
            
            # Check authorization
            authz_config = security_config.get('AuthorizationConfiguration', {})
            if authz_config:
                config_report['authorization'] = True
                print("      ✅ Authorization: CONFIGURED")
            else:
                print("      ❌ Authorization: NOT CONFIGURED")
            
            # Determine compliance
            config_report['compliant'] = (
                config_report['in_transit_encryption'] and 
                config_report['at_rest_encryption']
            )
            
            compliance_report['security_configurations'].append(config_report)
        
        compliant_configs = sum(1 for c in compliance_report['security_configurations'] if c['compliant'])
        total_configs = len(compliance_report['security_configurations'])
        
        print(f"   Summary: {compliant_configs}/{total_configs} configurations compliant")
        
    except ClientError as e:
        print(f"   ❌ Error checking security configurations: {e}")
    
    # 2. Check Active Clusters
    print("\\n2️⃣ Active Clusters Analysis")
    print("-" * 50)
    
    try:
        clusters = emr_client.list_clusters(
            ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
        )
        
        for cluster in clusters['Clusters']:
            cluster_id = cluster['Id']
            cluster_name = cluster['Name']
            
            # Get detailed cluster information
            cluster_detail = emr_client.describe_cluster(ClusterId=cluster_id)
            cluster_info = cluster_detail['Cluster']
            
            cluster_report = {
                'id': cluster_id,
                'name': cluster_name,
                'state': cluster_info['Status']['State'],
                'security_config': cluster_info.get('SecurityConfiguration'),
                'ebs_encryption': False,
                'instance_encryption': False,
                'compliant': False
            }
            
            print(f"   🔧 Cluster: {cluster_name} ({cluster_id})")
            print(f"      State: {cluster_info['Status']['State']}")
            
            # Check security configuration
            security_config = cluster_info.get('SecurityConfiguration')
            if security_config:
                print(f"      ✅ Security Configuration: {security_config}")
                cluster_report['security_config'] = security_config
            else:
                print(f"      ❌ Security Configuration: None")
            
            # Check EBS encryption
            instance_groups = cluster_info.get('Ec2InstanceAttributes', {})
            if check_ebs_encryption(cluster_id):
                cluster_report['ebs_encryption'] = True
                print("      ✅ EBS Encryption: ENABLED")
            else:
                print("      ❌ EBS Encryption: NOT VERIFIED")
            
            # Check instance encryption
            if security_config and cluster_report['ebs_encryption']:
                cluster_report['instance_encryption'] = True
                print("      ✅ Instance Encryption: ENABLED")
            else:
                print("      ❌ Instance Encryption: DISABLED")
            
            # Determine compliance
            cluster_report['compliant'] = (
                security_config is not None and 
                cluster_report['ebs_encryption']
            )
            
            compliance_report['clusters'].append(cluster_report)
        
        compliant_clusters = sum(1 for c in compliance_report['clusters'] if c['compliant'])
        total_clusters = len(compliance_report['clusters'])
        
        print(f"   Summary: {compliant_clusters}/{total_clusters} clusters compliant")
        
    except ClientError as e:
        print(f"   ❌ Error checking clusters: {e}")
    
    # 3. Calculate Overall Compliance Score
    print("\\n3️⃣ Compliance Score Calculation")
    print("-" * 50)
    
    score_factors = []
    
    # Security configuration score
    if compliance_report['security_configurations']:
        config_score = sum(1 for c in compliance_report['security_configurations'] if c['compliant'])
        config_total = len(compliance_report['security_configurations'])
        score_factors.append(config_score / config_total)
    
    # Cluster compliance score
    if compliance_report['clusters']:
        cluster_score = sum(1 for c in compliance_report['clusters'] if c['compliant'])
        cluster_total = len(compliance_report['clusters'])
        score_factors.append(cluster_score / cluster_total)
    
    overall_score = sum(score_factors) / len(score_factors) if score_factors else 0
    compliance_report['overall_score'] = overall_score * 100
    
    print(f"   Overall Compliance Score: {compliance_report['overall_score']:.1f}%")
    
    # 4. Generate Recommendations
    recommendations = []
    
    non_compliant_configs = [c for c in compliance_report['security_configurations'] if not c['compliant']]
    if non_compliant_configs:
        recommendations.append(f"Update {len(non_compliant_configs)} security configurations to enable all encryption")
    
    non_compliant_clusters = [c for c in compliance_report['clusters'] if not c['compliant']]
    if non_compliant_clusters:
        recommendations.append(f"Apply security configurations to {len(non_compliant_clusters)} clusters")
    
    recommendations.extend([
        "Enable automatic KMS key rotation",
        "Implement CloudWatch monitoring for encryption events",
        "Regular encryption compliance audits",
        "Use dedicated security configurations per environment"
    ])
    
    compliance_report['recommendations'] = recommendations
    
    print(f"\\n💡 Recommendations:")
    for i, rec in enumerate(recommendations, 1):
        print(f"   {i}. {rec}")
    
    return compliance_report

def check_ebs_encryption(cluster_id):
    """Check if EBS volumes are encrypted for EMR cluster"""
    
    try:
        # Get cluster instances
        cluster_detail = emr_client.describe_cluster(ClusterId=cluster_id)
        cluster_info = cluster_detail['Cluster']
        
        # Get instance IDs
        instance_groups = emr_client.list_instance_groups(ClusterId=cluster_id)
        
        instance_ids = []
        for group in instance_groups['InstanceGroups']:
            instances = emr_client.list_instances(
                ClusterId=cluster_id,
                InstanceGroupId=group['Id']
            )
            for instance in instances['Instances']:
                instance_ids.append(instance['Ec2InstanceId'])
        
        if not instance_ids:
            return False
        
        # Check EBS volumes
        instances = ec2_client.describe_instances(InstanceIds=instance_ids)
        
        for reservation in instances['Reservations']:
            for instance in reservation['Instances']:
                for bdm in instance.get('BlockDeviceMappings', []):
                    ebs = bdm.get('Ebs', {})
                    if not ebs.get('Encrypted', False):
                        return False
        
        return True
        
    except ClientError as e:
        print(f"Error checking EBS encryption: {e}")
        return False

def monitor_emr_encryption_metrics():
    """Monitor CloudWatch metrics for EMR encryption"""
    
    try:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
        
        print("\\n📈 EMR Encryption Metrics (Last 24 hours)")
        print("=" * 50)
        
        # Get active clusters
        clusters = emr_client.list_clusters(
            ClusterStates=['RUNNING', 'WAITING']
        )
        
        for cluster in clusters['Clusters']:
            cluster_id = cluster['Id']
            cluster_name = cluster['Name']
            
            print(f"\\n   📊 Cluster: {cluster_name}")
            
            # Get cluster metrics
            metrics = [
                'AppsRunning',
                'AppsPending',
                'AppsCompleted',
                'AppsFailed'
            ]
            
            for metric in metrics:
                try:
                    response = cloudwatch_client.get_metric_statistics(
                        Namespace='AWS/ElasticMapReduce',
                        MetricName=metric,
                        Dimensions=[
                            {
                                'Name': 'JobFlowId',
                                'Value': cluster_id
                            }
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=3600,
                        Statistics=['Average']
                    )
                    
                    if response['Datapoints']:
                        avg_value = sum(dp['Average'] for dp in response['Datapoints']) / len(response['Datapoints'])
                        print(f"      {metric}: {avg_value:.1f}")
                    else:
                        print(f"      {metric}: No data")
                        
                except ClientError as e:
                    print(f"      {metric}: Error - {e}")
        
        return True
        
    except ClientError as e:
        print(f"❌ Error monitoring metrics: {e}")
        return False

def create_encryption_dashboard():
    """Create CloudWatch dashboard for EMR encryption monitoring"""
    
    try:
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["AWS/ElasticMapReduce", "AppsRunning", "JobFlowId", "j-XXXXXXXXXX"],
                            ["AWS/ElasticMapReduce", "AppsCompleted", "JobFlowId", "j-XXXXXXXXXX"],
                            ["AWS/ElasticMapReduce", "AppsFailed", "JobFlowId", "j-XXXXXXXXXX"]
                        ],
                        "period": 300,
                        "stat": "Average",
                        "region": "us-west-2",
                        "title": "EMR Application Metrics"
                    }
                },
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["AWS/KMS", "NumberOfRequestsSucceeded", "KeyId", "alias/emr-ebs-encryption"],
                            ["AWS/KMS", "NumberOfRequestsFailed", "KeyId", "alias/emr-ebs-encryption"]
                        ],
                        "period": 300,
                        "stat": "Sum",
                        "region": "us-west-2",
                        "title": "KMS Key Usage for EMR"
                    }
                },
                {
                    "type": "log",
                    "properties": {
                        "query": "SOURCE '/aws/emr/encryption-monitoring' | fields @timestamp, @message\\n| filter @message like /ERROR/\\n| sort @timestamp desc\\n| limit 20",
                        "region": "us-west-2",
                        "title": "EMR Encryption Errors"
                    }
                }
            ]
        }
        
        cloudwatch_client.put_dashboard(
            DashboardName='EMR-Encryption-Monitoring',
            DashboardBody=json.dumps(dashboard_body)
        )
        
        print("✅ CloudWatch dashboard created: EMR-Encryption-Monitoring")
        
    except ClientError as e:
        print(f"❌ Error creating dashboard: {e}")

def setup_encryption_alarms():
    """Set up CloudWatch alarms for encryption monitoring"""
    
    try:
        # Alarm for KMS key usage
        cloudwatch_client.put_metric_alarm(
            AlarmName='EMR-KMS-Usage-High',
            ComparisonOperator='GreaterThanThreshold',
            EvaluationPeriods=1,
            MetricName='NumberOfRequestsSucceeded',
            Namespace='AWS/KMS',
            Period=300,
            Statistic='Sum',
            Threshold=1000.0,
            ActionsEnabled=True,
            AlarmActions=[
                'arn:aws:sns:us-west-2:123456789012:emr-alerts'
            ],
            AlarmDescription='High KMS usage detected for EMR',
            Dimensions=[
                {
                    'Name': 'KeyId',
                    'Value': 'alias/emr-ebs-encryption'
                }
            ]
        )
        
        # Alarm for failed applications
        cloudwatch_client.put_metric_alarm(
            AlarmName='EMR-Applications-Failed',
            ComparisonOperator='GreaterThanThreshold',
            EvaluationPeriods=2,
            MetricName='AppsFailed',
            Namespace='AWS/ElasticMapReduce',
            Period=300,
            Statistic='Sum',
            Threshold=5.0,
            ActionsEnabled=True,
            AlarmActions=[
                'arn:aws:sns:us-west-2:123456789012:emr-alerts'
            ],
            AlarmDescription='High number of failed EMR applications',
            Dimensions=[
                {
                    'Name': 'JobFlowId',
                    'Value': 'j-XXXXXXXXXX'  # Replace with actual cluster ID
                }
            ]
        )
        
        print("✅ CloudWatch alarms configured")
        
    except ClientError as e:
        print(f"❌ Error setting up alarms: {e}")

# Main monitoring function
def monitor_emr_encryption():
    """Comprehensive EMR encryption monitoring"""
    
    print("🚀 Starting EMR Encryption Monitoring")
    print("=" * 60)
    
    # Run compliance audit
    compliance_report = audit_emr_encryption_compliance()
    
    # Monitor metrics
    monitor_emr_encryption_metrics()
    
    # Create dashboard
    create_encryption_dashboard()
    
    # Set up alarms
    setup_encryption_alarms()
    
    # Summary
    print(f"\\n📊 Monitoring Summary")
    print("=" * 50)
    print(f"   Compliance Score: {compliance_report['overall_score']:.1f}%")
    print(f"   Total Clusters: {len(compliance_report['clusters'])}")
    print(f"   Compliant Clusters: {sum(1 for c in compliance_report['clusters'] if c['compliant'])}")
    print(f"   Security Configurations: {len(compliance_report['security_configurations'])}")
    print(f"   Recommendations: {len(compliance_report['recommendations'])}")
    
    return compliance_report

# Execute monitoring
if __name__ == "__main__":
    monitoring_results = monitor_emr_encryption()
    
    print("\\n✅ EMR encryption monitoring completed!")
    print("   Review the compliance report and CloudWatch dashboard.")
    print("   Address any recommendations to improve security posture.")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

# Helper functions
def generate_key_policy(key_type, key_usage, rotation_enabled):
    """Generate appropriate KMS key policy"""
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Enable IAM User Permissions",
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                "Action": "kms:*",
                "Resource": "*"
            }
        ]
    }
    
    if key_type == "Customer Managed":
        policy["Statement"].append({
            "Sid": "Allow use of the key",
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:role/DataRole"},
            "Action": [
                "kms:Encrypt",
                "kms:Decrypt",
                "kms:ReEncrypt*",
                "kms:GenerateDataKey*",
                "kms:DescribeKey"
            ],
            "Resource": "*"
        })
    
    return policy

def calculate_kms_cost(key_type, rotation_enabled, multi_region):
    """Calculate estimated KMS cost"""
    base_cost = 1.0  # $1 per month per key
    
    if key_type == "AWS Managed":
        base_cost = 0.0
    
    if multi_region:
        base_cost *= 2  # Multi-region keys cost more
    
    # Add usage-based costs (estimated)
    usage_cost = 0.03  # $0.03 per 10,000 requests
    
    return base_cost + usage_cost

def calculate_redshift_security_score(encryption_at_rest, encryption_in_transit, key_management, ssl_mode):
    """Calculate Redshift security score"""
    score = 0
    
    if encryption_at_rest:
        score += 40
        if key_management == "AWS KMS":
            score += 10
    
    if encryption_in_transit:
        score += 30
        if ssl_mode == "require":
            score += 20
    
    return score

def calculate_glue_encryption_coverage(catalog, job, s3, cloudwatch):
    """Calculate Glue encryption coverage percentage"""
    total_components = 4
    encrypted_components = sum([catalog, job, s3, cloudwatch])
    return int((encrypted_components / total_components) * 100)

def calculate_emr_encryption_score(ebs, local_disk, in_transit, at_rest):
    """Calculate EMR encryption score"""
    components = [ebs, local_disk, in_transit, at_rest]
    return int((sum(components) / len(components)) * 100)

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
    # 🔐 AWS Data Security & Encryption
    <div class='info-box'>
    Master AWS encryption services and implement comprehensive security for your data infrastructure including KMS, Redshift, Glue, and EMR encryption strategies.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🔐 AWS Key Management Service (KMS)",
        "🏢 Amazon Redshift Encryption",
        "📊 AWS Glue Data Catalog Encryption", 
        "🔥 Amazon EMR Encryption"
    ])
    
    with tab1:
        kms_tab()
    
    with tab2:
        redshift_encryption_tab()
    
    with tab3:
        glue_encryption_tab()
    
    with tab4:
        emr_encryption_tab()
    
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
