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
    page_title="AWS Data Security & Governance",
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
    'warning': '#FFB84D',
    'error': '#FF6B6B'
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
            border-radius: 10px;
            padding: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .stTabs [data-baseweb="tab"] {{
            height: 50px;
            padding: 0px 20px;
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
        
        .security-card {{
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
        
        .code-container {{
            background-color: {AWS_COLORS['dark_blue']};
            color: white;
            padding: 20px;
            border-radius: 10px;
            border-left: 4px solid {AWS_COLORS['primary']};
            margin: 15px 0;
        }}
        
        .service-feature {{
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
            border-left: 5px solid {AWS_COLORS['light_blue']};
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
        
        .footer {{
            text-align: center;
            padding: 1rem;
            background-color: {AWS_COLORS['secondary']};
            color: white;
            margin-top: 1rem;
            border-radius: 8px;
        }}
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    common.initialize_mermaid()
    common.initialize_session_state()
    
    if 'session_started' not in st.session_state:
        st.session_state.session_started = True
        st.session_state.secrets_created = []
        st.session_state.parameters_created = []
        st.session_state.iam_roles_created = []
        st.session_state.data_lake_components = []
        st.session_state.lake_formation_configured = False

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🔐 AWS Secrets Manager - Secure credential management
            - 📊 AWS Systems Manager Parameter Store - Configuration management
            - 🎭 IAM Roles - Secure access without credentials
            - 🏗️ Manual Data Lake Building - Traditional approach
            - 🚀 AWS Lake Formation - Simplified data lake creation
            - 🛡️ Lake Formation Permissions - Granular access control
            
            **Learning Objectives:**
            - Master AWS security best practices
            - Understand credential management strategies
            - Learn data lake architecture patterns
            - Explore permission management techniques
            """)
        


def create_secrets_manager_architecture():
    """Create mermaid diagram for Secrets Manager architecture"""
    return """
    graph TB
        APP[Applications] --> SM[AWS Secrets Manager]
        SM --> KMS[AWS KMS<br/>Encryption]
        SM --> CWL[CloudWatch Logs<br/>Audit Trail]
        SM --> IAM[IAM Policies<br/>Access Control]
        
        subgraph "Secret Types"
            DB[Database Credentials]
            API[API Keys]
            TOKENS[OAuth Tokens]
            CERTS[SSL Certificates]
        end
        
        SM --> DB
        SM --> API
        SM --> TOKENS
        SM --> CERTS
        
        subgraph "Rotation"
            AUTO[Automatic Rotation]
            LAMBDA[Lambda Functions]
            SCHEDULE[Scheduled Rotation]
        end
        
        SM --> AUTO
        AUTO --> LAMBDA
        AUTO --> SCHEDULE
        
        style SM fill:#FF9900,stroke:#232F3E,color:#fff
        style KMS fill:#4B9EDB,stroke:#232F3E,color:#fff
        style AUTO fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_parameter_store_hierarchy():
    """Create mermaid diagram for Parameter Store hierarchy"""
    return """
    graph TB
        ROOT[Parameter Store Root]
        
        subgraph "Production Environment"
            PROD[/prod/]
            PROD_DB[/prod/database/]
            PROD_DB_HOST[/prod/database/host]
            PROD_DB_PORT[/prod/database/port]
            PROD_API[/prod/api/]
            PROD_API_KEY[/prod/api/key]
        end
        
        subgraph "Development Environment"
            DEV[/dev/]
            DEV_DB[/dev/database/]
            DEV_DB_HOST[/dev/database/host]
            DEV_DB_PORT[/dev/database/port]
            DEV_API[/dev/api/]
            DEV_API_KEY[/dev/api/key]
        end
        
        ROOT --> PROD
        ROOT --> DEV
        
        PROD --> PROD_DB
        PROD --> PROD_API
        PROD_DB --> PROD_DB_HOST
        PROD_DB --> PROD_DB_PORT
        PROD_API --> PROD_API_KEY
        
        DEV --> DEV_DB
        DEV --> DEV_API
        DEV_DB --> DEV_DB_HOST
        DEV_DB --> DEV_DB_PORT
        DEV_API --> DEV_API_KEY
        
        style ROOT fill:#FF9900,stroke:#232F3E,color:#fff
        style PROD fill:#3FB34F,stroke:#232F3E,color:#fff
        style DEV fill:#4B9EDB,stroke:#232F3E,color:#fff
    """

def create_iam_roles_flow():
    """Create mermaid diagram for IAM roles flow"""
    return """
    graph LR
        USER[User/Service] --> ASSUME[Assume Role]
        ASSUME --> STS[AWS STS]
        STS --> TEMP[Temporary Credentials]
        TEMP --> ACCESS[Access AWS Resources]
        
        subgraph "IAM Role Components"
            TRUST[Trust Policy<br/>Who can assume]
            PERMISSION[Permission Policy<br/>What can be done]
            CONDITIONS[Conditions<br/>When and how]
        end
        
        ASSUME --> TRUST
        TRUST --> PERMISSION
        PERMISSION --> CONDITIONS
        
        subgraph "Use Cases"
            EC2[EC2 Instance Role]
            LAMBDA[Lambda Execution Role]
            CROSS[Cross-Account Access]
            FEDERATED[Federated Users]
        end
        
        ACCESS --> EC2
        ACCESS --> LAMBDA
        ACCESS --> CROSS
        ACCESS --> FEDERATED
        
        style STS fill:#FF9900,stroke:#232F3E,color:#fff
        style TRUST fill:#4B9EDB,stroke:#232F3E,color:#fff
        style PERMISSION fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_data_lake_architecture():
    """Create mermaid diagram for manual data lake architecture"""
    return """
    graph TB
        subgraph "Data Sources"
            DB[(Databases)]
            IOT[IoT Devices]
            MOBILE[Mobile Apps]
            LOGS[Application Logs]
        end
        
        subgraph "Ingestion Layer"
            KINESIS[Amazon Kinesis]
            DMS[AWS DMS]
            GLUE[AWS Glue ETL]
            LAMBDA[Lambda Functions]
        end
        
        subgraph "Storage Layer"
            S3_RAW[S3 Raw Zone]
            S3_PROCESSED[S3 Processed Zone]
            S3_CURATED[S3 Curated Zone]
        end
        
        subgraph "Processing Layer"
            EMR[Amazon EMR]
            GLUE_JOBS[Glue Jobs]
            ATHENA[Amazon Athena]
        end
        
        subgraph "Catalog & Governance"
            CATALOG[Glue Data Catalog]
            IAM_POLICIES[IAM Policies]
            ENCRYPTION[KMS Encryption]
        end
        
        subgraph "Analytics Layer"
            QUICKSIGHT[QuickSight]
            REDSHIFT[Redshift]
            SAGEMAKER[SageMaker]
        end
        
        DB --> DMS
        IOT --> KINESIS
        MOBILE --> LAMBDA
        LOGS --> GLUE
        
        DMS --> S3_RAW
        KINESIS --> S3_RAW
        LAMBDA --> S3_RAW
        GLUE --> S3_RAW
        
        S3_RAW --> EMR
        EMR --> S3_PROCESSED
        S3_PROCESSED --> GLUE_JOBS
        GLUE_JOBS --> S3_CURATED
        
        S3_CURATED --> CATALOG
        S3_CURATED --> ATHENA
        ATHENA --> QUICKSIGHT
        S3_CURATED --> REDSHIFT
        S3_CURATED --> SAGEMAKER
        
        CATALOG --> IAM_POLICIES
        S3_RAW --> ENCRYPTION
        S3_PROCESSED --> ENCRYPTION
        S3_CURATED --> ENCRYPTION
        
        style S3_RAW fill:#3FB34F,stroke:#232F3E,color:#fff
        style S3_PROCESSED fill:#4B9EDB,stroke:#232F3E,color:#fff
        style S3_CURATED fill:#FF9900,stroke:#232F3E,color:#fff
        style CATALOG fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_lake_formation_architecture():
    """Create mermaid diagram for Lake Formation architecture"""
    return """
    graph TB
        subgraph "AWS Lake Formation"
            LF[Lake Formation Service]
            BLUEPRINTS[Blueprints & Workflows]
            PERMISSIONS[Centralized Permissions]
            CATALOG[Integrated Data Catalog]
        end
        
        subgraph "Data Sources"
            RDS[(Amazon RDS)]
            S3_SOURCE[S3 Data Sources]
            STREAMING[Streaming Data]
        end
        
        subgraph "Automated Ingestion"
            CRAWLERS[Automated Crawlers]
            ETL_AUTO[Automated ETL]
            SCHEMA[Schema Discovery]
        end
        
        subgraph "Secure Data Lake"
            S3_LAKE[S3 Data Lake]
            ENCRYPTION[Automatic Encryption]
            ACCESS[Fine-grained Access]
        end
        
        subgraph "Analytics Services"
            ATHENA[Amazon Athena]
            REDSHIFT[Amazon Redshift]
            EMR[Amazon EMR]
            QUICKSIGHT[Amazon QuickSight]
        end
        
        RDS --> LF
        S3_SOURCE --> LF
        STREAMING --> LF
        
        LF --> BLUEPRINTS
        LF --> PERMISSIONS
        LF --> CATALOG
        
        BLUEPRINTS --> CRAWLERS
        BLUEPRINTS --> ETL_AUTO
        BLUEPRINTS --> SCHEMA
        
        CRAWLERS --> S3_LAKE
        ETL_AUTO --> S3_LAKE
        
        S3_LAKE --> ENCRYPTION
        S3_LAKE --> ACCESS
        
        PERMISSIONS --> ACCESS
        
        S3_LAKE --> ATHENA
        S3_LAKE --> REDSHIFT
        S3_LAKE --> EMR
        S3_LAKE --> QUICKSIGHT
        
        CATALOG --> ATHENA
        CATALOG --> REDSHIFT
        CATALOG --> EMR
        CATALOG --> QUICKSIGHT
        
        style LF fill:#FF9900,stroke:#232F3E,color:#fff
        style PERMISSIONS fill:#232F3E,stroke:#FF9900,color:#fff
        style S3_LAKE fill:#3FB34F,stroke:#232F3E,color:#fff
        style ENCRYPTION fill:#4B9EDB,stroke:#232F3E,color:#fff
    """

def secrets_manager_tab():
    """Content for AWS Secrets Manager tab"""
    st.markdown("## 🔐 AWS Secrets Manager")
    st.markdown("*Centrally manage, retrieve, and rotate database credentials, API keys, and other secrets*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Why Use AWS Secrets Manager?
    - **Eliminate Hard-coded Secrets**: No more passwords in source code
    - **Automatic Rotation**: Built-in rotation for RDS, DocumentDB, and Redshift
    - **Fine-grained Access Control**: IAM policies control who can access secrets
    - **Audit Trail**: CloudTrail logs all secret access attempts
    - **Encryption at Rest**: All secrets encrypted with AWS KMS
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ Secrets Manager Architecture")
    common.mermaid(create_secrets_manager_architecture(), height=600)
    
    # Interactive secret creator
    st.markdown("#### 🔧 Interactive Secret Creator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        secret_name = st.text_input("Secret Name", value="prod/myapp/database")
        secret_type = st.selectbox("Secret Type", [
            "Database credentials",
            "API key",
            "OAuth token",
            "SSL certificate",
            "Custom secret"
        ])
        
    with col2:
        description = st.text_area("Description", value="Production database credentials for MyApp")
        rotation_enabled = st.checkbox("Enable Automatic Rotation", value=True)
        if rotation_enabled:
            rotation_days = st.slider("Rotation Interval (days)", 1, 365, 30)
    
    # Secret value input
    if secret_type == "Database credentials":
        username = st.text_input("Database Username", value="admin")
        password = st.text_input("Database Password", type="password", value="SecurePass123!")
        host = st.text_input("Database Host", value="mydb.cluster-xyz.us-west-2.rds.amazonaws.com")
        port = st.number_input("Port", value=5432)
        
        secret_value = {
            "username": username,
            "password": password,
            "host": host,
            "port": port,
            "engine": "postgresql"
        }
    else:
        secret_value = st.text_area("Secret Value", value="your-secret-value-here", type="password")
    
    if st.button("🔐 Create Secret", type="primary"):
        if secret_name and secret_value:
            st.session_state.secrets_created.append({
                "name": secret_name,
                "type": secret_type,
                "description": description,
                "rotation": rotation_enabled,
                "created_at": datetime.now()
            })
            st.success(f"✅ Secret '{secret_name}' created successfully!")
        else:
            st.error("Please fill in all required fields")
    
    # Display created secrets
    if st.session_state.secrets_created:
        st.markdown("#### 📝 Created Secrets")
        secrets_df = pd.DataFrame(st.session_state.secrets_created)
        st.dataframe(secrets_df, use_container_width=True)
    
    # Best practices
    st.markdown("#### 🏆 Best Practices")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Do's
        - Use descriptive secret names with hierarchy
        - Enable automatic rotation for databases
        - Use least privilege IAM policies
        - Monitor secret access with CloudTrail
        - Use resource-based policies for cross-account access
        - Implement proper error handling in applications
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="warning-box">', unsafe_allow_html=True)
        st.markdown("""
        ### ❌ Don'ts
        - Don't store secrets in environment variables
        - Don't hard-code secrets in source code
        - Don't use the same secret across environments
        - Don't ignore rotation failures
        - Don't grant overly broad permissions
        - Don't forget to delete unused secrets
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Code Examples")
    
    tab1, tab2, tab3 = st.tabs(["Python/Boto3", "AWS CLI", "CloudFormation"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
import json
from botocore.exceptions import ClientError

# Initialize Secrets Manager client
secrets_client = boto3.client('secretsmanager', region_name='us-west-2')

# Create a new secret
def create_database_secret():
    secret_name = "prod/myapp/database"
    secret_value = {
        "username": "admin",
        "password": "SecurePassword123!",
        "host": "mydb.cluster-xyz.us-west-2.rds.amazonaws.com",
        "port": 5432,
        "engine": "postgresql",
        "dbname": "production"
    }
    
    try:
        response = secrets_client.create_secret(
            Name=secret_name,
            Description="Production database credentials for MyApp",
            SecretString=json.dumps(secret_value),
            KmsKeyId="alias/aws/secretsmanager",  # Use default KMS key
            Tags=[
                {'Key': 'Environment', 'Value': 'Production'},
                {'Key': 'Application', 'Value': 'MyApp'},
                {'Key': 'Team', 'Value': 'DataEngineering'}
            ]
        )
        print(f"Secret created: {response['ARN']}")
        return response['ARN']
    except ClientError as e:
        print(f"Error creating secret: {e}")
        return None

# Retrieve secret value
def get_secret_value(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        return secret_data
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        return None

# Update secret value
def update_secret_value(secret_name, new_value):
    try:
        response = secrets_client.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(new_value)
        )
        print(f"Secret updated: {response['ARN']}")
        return True
    except ClientError as e:
        print(f"Error updating secret: {e}")
        return False

# Enable automatic rotation
def enable_automatic_rotation(secret_name, lambda_function_arn):
    try:
        response = secrets_client.update_secret(
            SecretId=secret_name,
            Description="Database secret with automatic rotation enabled"
        )
        
        # Configure rotation
        rotation_response = secrets_client.rotate_secret(
            SecretId=secret_name,
            RotationLambdaArn=lambda_function_arn,
            RotationRules={
                'AutomaticallyAfterDays': 30
            }
        )
        print(f"Automatic rotation enabled for {secret_name}")
        return True
    except ClientError as e:
        print(f"Error enabling rotation: {e}")
        return False

# Database connection using Secrets Manager
def get_database_connection():
    import psycopg2
    
    # Get database credentials from Secrets Manager
    secret_name = "prod/myapp/database"
    secret_data = get_secret_value(secret_name)
    
    if secret_data:
        try:
            # Create database connection
            connection = psycopg2.connect(
                host=secret_data['host'],
                port=secret_data['port'],
                database=secret_data['dbname'],
                user=secret_data['username'],
                password=secret_data['password']
            )
            print("Database connection established successfully")
            return connection
        except Exception as e:
            print(f"Database connection failed: {e}")
            return None
    else:
        print("Failed to retrieve database credentials")
        return None

# Example usage
if __name__ == "__main__":
    # Create secret
    secret_arn = create_database_secret()
    
    if secret_arn:
        # Retrieve and use secret
        db_credentials = get_secret_value("prod/myapp/database")
        print(f"Retrieved credentials for: {db_credentials['username']}")
        
        # Get database connection
        conn = get_database_connection()
        if conn:
            # Use the connection for database operations
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            print(f"Connected to: {version[0]}")
            cursor.close()
            conn.close()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create a new secret
aws secretsmanager create-secret \
    --name "prod/myapp/database" \
    --description "Production database credentials" \
    --secret-string '{
        "username": "admin",
        "password": "SecurePassword123!",
        "host": "mydb.cluster-xyz.us-west-2.rds.amazonaws.com",
        "port": 5432,
        "engine": "postgresql"
    }' \
    --tags '[
        {"Key": "Environment", "Value": "Production"},
        {"Key": "Application", "Value": "MyApp"}
    ]'

# Retrieve secret value
aws secretsmanager get-secret-value \
    --secret-id "prod/myapp/database" \
    --query SecretString \
    --output text

# Update secret value
aws secretsmanager update-secret \
    --secret-id "prod/myapp/database" \
    --secret-string '{
        "username": "admin",
        "password": "NewSecurePassword456!",
        "host": "mydb.cluster-xyz.us-west-2.rds.amazonaws.com",
        "port": 5432,
        "engine": "postgresql"
    }'

# Enable automatic rotation
aws secretsmanager rotate-secret \
    --secret-id "prod/myapp/database" \
    --rotation-lambda-arn "arn:aws:lambda:us-west-2:123456789012:function:SecretsManagerRDSPostgreSQLRotationSingleUser" \
    --rotation-rules AutomaticallyAfterDays=30

# List all secrets
aws secretsmanager list-secrets \
    --filters Key=tag-key,Values=Environment \
    --query 'SecretList[?Tags[?Key==`Environment` && Value==`Production`]]'

# Create secret for API key
aws secretsmanager create-secret \
    --name "prod/myapp/api-key" \
    --description "Third-party API key" \
    --secret-string "sk-1234567890abcdef"

# Create secret with automatic rotation for RDS
aws secretsmanager create-secret \
    --name "prod/rds/master-password" \
    --description "RDS master password with rotation" \
    --generate-random-password \
    --password-length 32 \
    --exclude-punctuation

# Schedule immediate rotation
aws secretsmanager rotate-secret \
    --secret-id "prod/myapp/database" \
    --force-rotate-immediately

# Get secret metadata (without secret value)
aws secretsmanager describe-secret \
    --secret-id "prod/myapp/database"

# Delete secret (with recovery window)
aws secretsmanager delete-secret \
    --secret-id "prod/myapp/database" \
    --recovery-window-in-days 7

# Restore deleted secret
aws secretsmanager restore-secret \
    --secret-id "prod/myapp/database"

# Batch retrieve secrets
aws secretsmanager batch-get-secret-value \
    --secret-id-list "prod/myapp/database" "prod/myapp/api-key"
        ''', language='bash')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# CloudFormation Template for Secrets Manager
AWSTemplateFormatVersion: '2010-09-09'
Description: 'AWS Secrets Manager setup with automatic rotation'

Parameters:
  DatabaseUsername:
    Type: String
    Default: admin
    Description: Database master username
  
  DatabasePassword:
    Type: String
    NoEcho: true
    Description: Database master password
    MinLength: 8

Resources:
  # Database Secret
  DatabaseSecret:
    Type: AWS::SecretsManager::Secret
    Properties:
      Name: !Sub '${AWS::StackName}/database/credentials'
      Description: 'Database credentials with automatic rotation'
      SecretString: !Sub |
        {
          "username": "${DatabaseUsername}",
          "password": "${DatabasePassword}",
          "host": "${DatabaseCluster.Endpoint.Address}",
          "port": ${DatabaseCluster.Endpoint.Port},
          "engine": "postgresql",
          "dbname": "production"
        }
      KmsKeyId: !Ref SecretKMSKey
      Tags:
        - Key: Environment
          Value: Production
        - Key: Application
          Value: MyApp

  # KMS Key for Secrets Encryption
  SecretKMSKey:
    Type: AWS::KMS::Key
    Properties:
      Description: 'KMS Key for Secrets Manager encryption'
      KeyPolicy:
        Statement:
          - Sid: Enable IAM User Permissions
            Effect: Allow
            Principal:
              AWS: !Sub 'arn:aws:iam::${AWS::AccountId}:root'
            Action: 'kms:*'
            Resource: '*'
          - Sid: Allow Secrets Manager
            Effect: Allow
            Principal:
              Service: secretsmanager.amazonaws.com
            Action:
              - kms:Decrypt
              - kms:DescribeKey
              - kms:Encrypt
              - kms:GenerateDataKey*
              - kms:ReEncrypt*
            Resource: '*'

  # KMS Key Alias
  SecretKMSKeyAlias:
    Type: AWS::KMS::Alias
    Properties:
      AliasName: !Sub 'alias/${AWS::StackName}-secrets-key'
      TargetKeyId: !Ref SecretKMSKey

  # Lambda Function for Rotation
  RotationLambda:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub '${AWS::StackName}-secrets-rotation'
      Runtime: python3.9
      Handler: lambda_function.lambda_handler
      Role: !GetAtt RotationLambdaRole.Arn
      Code:
        ZipFile: |
          import boto3
          import json
          import logging
          
          logger = logging.getLogger()
          logger.setLevel(logging.INFO)
          
          def lambda_handler(event, context):
              # Rotation logic here
              logger.info(f"Rotation event: {json.dumps(event)}")
              # Implementation depends on database type
              return {"statusCode": 200}
      Environment:
        Variables:
          SECRETS_MANAGER_ENDPOINT: !Sub 'https://secretsmanager.${AWS::Region}.amazonaws.com'

  # IAM Role for Rotation Lambda
  RotationLambdaRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
      Policies:
        - PolicyName: SecretsManagerRotationPolicy
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - secretsmanager:DescribeSecret
                  - secretsmanager:GetSecretValue
                  - secretsmanager:PutSecretValue
                  - secretsmanager:UpdateSecretVersionStage
                Resource: !Ref DatabaseSecret
              - Effect: Allow
                Action:
                  - kms:Decrypt
                  - kms:GenerateDataKey
                Resource: !GetAtt SecretKMSKey.Arn

  # Secret Rotation Configuration
  DatabaseSecretRotation:
    Type: AWS::SecretsManager::RotationSchedule
    Properties:
      SecretId: !Ref DatabaseSecret
      RotationLambdaArn: !GetAtt RotationLambda.Arn
      RotationRules:
        AutomaticallyAfterDays: 30

  # Resource Policy for Cross-Account Access
  DatabaseSecretResourcePolicy:
    Type: AWS::SecretsManager::ResourcePolicy
    Properties:
      SecretId: !Ref DatabaseSecret
      ResourcePolicy:
        Version: '2012-10-17'
        Statement:
          - Sid: AllowCrossAccountAccess
            Effect: Allow
            Principal:
              AWS: !Sub 'arn:aws:iam::${TrustedAccountId}:root'
            Action:
              - secretsmanager:GetSecretValue
            Resource: '*'
            Condition:
              StringEquals:
                'secretsmanager:ResourceTag/Environment': 'Production'

Outputs:
  DatabaseSecretArn:
    Description: 'ARN of the database secret'
    Value: !Ref DatabaseSecret
    Export:
      Name: !Sub '${AWS::StackName}-DatabaseSecret-Arn'
  
  SecretKMSKeyId:
    Description: 'KMS Key ID for secrets encryption'
    Value: !Ref SecretKMSKey
    Export:
      Name: !Sub '${AWS::StackName}-SecretKMSKey-Id'
        ''', language='yaml')
        st.markdown('</div>', unsafe_allow_html=True)

def parameter_store_tab():
    """Content for AWS Systems Manager Parameter Store tab"""
    st.markdown("## 📊 AWS Systems Manager Parameter Store")
    st.markdown("*Hierarchical storage for configuration data and secrets management*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Parameter Store Benefits
    - **Hierarchical Organization**: Organize parameters in a tree structure
    - **Version Control**: Track parameter changes over time
    - **Secure String Support**: Encrypt sensitive parameters with KMS
    - **Cost Effective**: Standard parameters are free, advanced parameters for high throughput
    - **Integration**: Native integration with other AWS services
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Parameter hierarchy diagram
    st.markdown("#### 🌳 Parameter Hierarchy Structure")
    common.mermaid(create_parameter_store_hierarchy(), height=600)
    
    # Interactive parameter creator
    st.markdown("#### ⚙️ Interactive Parameter Creator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        parameter_name = st.text_input("Parameter Name", value="/prod/myapp/database/host")
        parameter_type = st.selectbox("Parameter Type", ["String", "StringList", "SecureString"])
        parameter_tier = st.selectbox("Parameter Tier", ["Standard", "Advanced", "Intelligent-Tiering"])
        
    with col2:
        parameter_value = st.text_area("Parameter Value", value="mydb.cluster-xyz.us-west-2.rds.amazonaws.com")
        description = st.text_input("Description", value="Production database host endpoint")
        
        if parameter_type == "SecureString":
            kms_key = st.selectbox("KMS Key", ["alias/aws/ssm", "alias/parameter-store-key", "Custom KMS Key"])
    
    # Tags
    st.markdown("##### 🏷️ Parameter Tags")
    col1, col2, col3 = st.columns(3)
    with col1:
        tag_env = st.selectbox("Environment", ["prod", "dev", "test", "staging"])
    with col2:
        tag_app = st.text_input("Application", value="myapp")
    with col3:
        tag_team = st.text_input("Team", value="data-engineering")
    
    if st.button("📝 Create Parameter", type="primary"):
        if parameter_name and parameter_value:
            st.session_state.parameters_created.append({
                "name": parameter_name,
                "type": parameter_type,
                "tier": parameter_tier,
                "value": parameter_value[:50] + "..." if len(parameter_value) > 50 else parameter_value,
                "environment": tag_env,
                "application": tag_app,
                "created_at": datetime.now()
            })
            st.success(f"✅ Parameter '{parameter_name}' created successfully!")
        else:
            st.error("Please fill in all required fields")
    
    # Display created parameters
    if st.session_state.parameters_created:
        st.markdown("#### 📋 Created Parameters")
        params_df = pd.DataFrame(st.session_state.parameters_created)
        st.dataframe(params_df, use_container_width=True)
    
    # Parameter types comparison
    st.markdown("#### 📊 Parameter Types Comparison")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="service-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 📝 String
        **Use Cases:**
        - Database hostnames
        - API endpoints
        - Configuration values
        - Feature flags
        
        **Characteristics:**
        - Plain text storage
        - Up to 4 KB value size
        - Supports standard & advanced tiers
        - Version history available
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="service-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 📋 StringList
        **Use Cases:**
        - List of database hosts
        - Multiple API endpoints
        - Comma-separated values
        - Configuration arrays
        
        **Characteristics:**
        - Comma-separated values
        - Up to 4 KB total size
        - Processed as array by applications
        - Version history available
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="service-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔐 SecureString
        **Use Cases:**
        - Database passwords
        - API keys
        - SSL certificates
        - Sensitive configuration
        
        **Characteristics:**
        - KMS encrypted storage
        - Up to 4 KB value size
        - Requires KMS permissions
        - Audit trail in CloudTrail
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Python/Boto3", "AWS CLI", "CloudFormation"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
import json
from botocore.exceptions import ClientError

# Initialize SSM client
ssm_client = boto3.client('ssm', region_name='us-west-2')

class ParameterStoreManager:
    def __init__(self, region='us-west-2'):
        self.ssm_client = boto3.client('ssm', region_name=region)
    
    def create_parameter(self, name, value, param_type='String', description='', kms_key_id=None, tags=None):
        """Create a new parameter in Parameter Store"""
        try:
            params = {
                'Name': name,
                'Value': value,
                'Type': param_type,
                'Description': description,
                'Overwrite': False,
                'Tags': tags or []
            }
            
            if param_type == 'SecureString' and kms_key_id:
                params['KeyId'] = kms_key_id
            
            response = self.ssm_client.put_parameter(**params)
            print(f"Parameter created successfully: {name}")
            return response
        except ClientError as e:
            print(f"Error creating parameter: {e}")
            return None
    
    def get_parameter(self, name, decrypt=True):
        """Retrieve a parameter value"""
        try:
            response = self.ssm_client.get_parameter(
                Name=name,
                WithDecryption=decrypt
            )
            return response['Parameter']['Value']
        except ClientError as e:
            print(f"Error retrieving parameter {name}: {e}")
            return None
    
    def get_parameters_by_path(self, path, recursive=True, decrypt=True):
        """Retrieve multiple parameters by path"""
        try:
            parameters = {}
            paginator = self.ssm_client.get_paginator('get_parameters_by_path')
            
            for page in paginator.paginate(
                Path=path,
                Recursive=recursive,
                WithDecryption=decrypt
            ):
                for param in page['Parameters']:
                    parameters[param['Name']] = param['Value']
            
            return parameters
        except ClientError as e:
            print(f"Error retrieving parameters by path {path}: {e}")
            return {}
    
    def update_parameter(self, name, value, description=None):
        """Update an existing parameter"""
        try:
            params = {
                'Name': name,
                'Value': value,
                'Overwrite': True
            }
            
            if description:
                params['Description'] = description
            
            response = self.ssm_client.put_parameter(**params)
            print(f"Parameter updated successfully: {name}")
            return response
        except ClientError as e:
            print(f"Error updating parameter: {e}")
            return None
    
    def delete_parameter(self, name):
        """Delete a parameter"""
        try:
            response = self.ssm_client.delete_parameter(Name=name)
            print(f"Parameter deleted successfully: {name}")
            return response
        except ClientError as e:
            print(f"Error deleting parameter: {e}")
            return None
    
    def get_parameter_history(self, name):
        """Get parameter version history"""
        try:
            response = self.ssm_client.get_parameter_history(Name=name)
            return response['Parameters']
        except ClientError as e:
            print(f"Error retrieving parameter history: {e}")
            return []

# Example usage
def setup_application_parameters():
    """Set up parameters for a typical application"""
    ps_manager = ParameterStoreManager()
    
    # Database configuration
    ps_manager.create_parameter(
        name='/prod/myapp/database/host',
        value='mydb.cluster-xyz.us-west-2.rds.amazonaws.com',
        param_type='String',
        description='Production database host',
        tags=[
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Application', 'Value': 'MyApp'},
            {'Key': 'Component', 'Value': 'Database'}
        ]
    )
    
    ps_manager.create_parameter(
        name='/prod/myapp/database/port',
        value='5432',
        param_type='String',
        description='Production database port'
    )
    
    # Secure database password
    ps_manager.create_parameter(
        name='/prod/myapp/database/password',
        value='SecurePassword123!',
        param_type='SecureString',
        description='Production database password',
        kms_key_id='alias/parameter-store-key'
    )
    
    # API configuration
    ps_manager.create_parameter(
        name='/prod/myapp/api/endpoints',
        value='https://api1.example.com,https://api2.example.com',
        param_type='StringList',
        description='Production API endpoints'
    )
    
    # Feature flags
    ps_manager.create_parameter(
        name='/prod/myapp/features/new_dashboard',
        value='true',
        param_type='String',
        description='Enable new dashboard feature'
    )

def get_application_config():
    """Retrieve application configuration from Parameter Store"""
    ps_manager = ParameterStoreManager()
    
    # Get all parameters for the application
    config = ps_manager.get_parameters_by_path('/prod/myapp/', recursive=True)
    
    # Process the configuration
    app_config = {}
    for param_name, param_value in config.items():
        # Convert parameter path to nested dictionary structure
        path_parts = param_name.split('/')[3:]  # Remove /prod/myapp/
        
        current_dict = app_config
        for part in path_parts[:-1]:
            if part not in current_dict:
                current_dict[part] = {}
            current_dict = current_dict[part]
        
        # Handle StringList parameters
        if ',' in param_value:
            current_dict[path_parts[-1]] = param_value.split(',')
        else:
            current_dict[path_parts[-1]] = param_value
    
    return app_config

# Database connection using Parameter Store
def get_database_connection():
    import psycopg2
    ps_manager = ParameterStoreManager()
    
    try:
        # Get database configuration from Parameter Store
        host = ps_manager.get_parameter('/prod/myapp/database/host')
        port = int(ps_manager.get_parameter('/prod/myapp/database/port'))
        username = ps_manager.get_parameter('/prod/myapp/database/username')
        password = ps_manager.get_parameter('/prod/myapp/database/password', decrypt=True)
        database = ps_manager.get_parameter('/prod/myapp/database/name')
        
        # Create database connection
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password
        )
        
        print("Database connection established using Parameter Store")
        return connection
    except Exception as e:
        print(f"Failed to establish database connection: {e}")
        return None

# Example execution
if __name__ == "__main__":
    # Setup parameters
    setup_application_parameters()
    
    # Retrieve configuration
    config = get_application_config()
    print("Application Configuration:")
    print(json.dumps(config, indent=2))
    
    # Get database connection
    db_conn = get_database_connection()
    if db_conn:
        print("Successfully connected to database")
        db_conn.close()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create different types of parameters

# Standard String parameter
aws ssm put-parameter \
    --name "/prod/myapp/database/host" \
    --value "mydb.cluster-xyz.us-west-2.rds.amazonaws.com" \
    --type "String" \
    --description "Production database host" \
    --tags "Key=Environment,Value=Production" "Key=Application,Value=MyApp"

# SecureString parameter with KMS encryption
aws ssm put-parameter \
    --name "/prod/myapp/database/password" \
    --value "SecurePassword123!" \
    --type "SecureString" \
    --key-id "alias/parameter-store-key" \
    --description "Production database password"

# StringList parameter
aws ssm put-parameter \
    --name "/prod/myapp/api/endpoints" \
    --value "https://api1.example.com,https://api2.example.com" \
    --type "StringList" \
    --description "Production API endpoints"

# Advanced tier parameter (for high throughput)
aws ssm put-parameter \
    --name "/prod/myapp/config/settings" \
    --value "$(cat config.json)" \
    --type "String" \
    --tier "Advanced" \
    --description "Application configuration JSON"

# Retrieve a single parameter
aws ssm get-parameter \
    --name "/prod/myapp/database/host" \
    --query "Parameter.Value" \
    --output text

# Retrieve SecureString parameter with decryption
aws ssm get-parameter \
    --name "/prod/myapp/database/password" \
    --with-decryption \
    --query "Parameter.Value" \
    --output text

# Retrieve multiple parameters
aws ssm get-parameters \
    --names "/prod/myapp/database/host" "/prod/myapp/database/port" \
    --with-decryption

# Get parameters by path (hierarchical retrieval)
aws ssm get-parameters-by-path \
    --path "/prod/myapp/database" \
    --recursive \
    --with-decryption

# Get parameters by path with filtering
aws ssm get-parameters-by-path \
    --path "/prod/myapp" \
    --recursive \
    --parameter-filters "Key=Type,Values=SecureString"

# Update existing parameter
aws ssm put-parameter \
    --name "/prod/myapp/database/port" \
    --value "5433" \
    --type "String" \
    --overwrite

# Get parameter history
aws ssm get-parameter-history \
    --name "/prod/myapp/database/password" \
    --with-decryption

# Add tags to existing parameter
aws ssm add-tags-to-resource \
    --resource-type "Parameter" \
    --resource-id "/prod/myapp/database/host" \
    --tags "Key=Team,Value=DataEngineering" "Key=CostCenter,Value=Engineering"

# List parameters with filtering
aws ssm describe-parameters \
    --parameter-filters "Key=Type,Values=SecureString" \
    --query "Parameters[*].[Name,Type,LastModifiedDate]" \
    --output table

# Delete parameter
aws ssm delete-parameter \
    --name "/prod/myapp/database/old_password"

# Get parameters by tag
aws ssm describe-parameters \
    --parameter-filters "Key=tag:Environment,Values=Production" \
    --query "Parameters[*].[Name,Type,Description]" \
    --output table

# Bulk parameter operations using AWS CLI and jq
# Create multiple parameters from JSON file
cat << EOF > parameters.json
[
  {
    "name": "/prod/myapp/redis/host",
    "value": "redis.cluster-xyz.cache.amazonaws.com",
    "type": "String",
    "description": "Redis cluster endpoint"
  },
  {
    "name": "/prod/myapp/redis/port",
    "value": "6379",
    "type": "String",
    "description": "Redis port"
  }
]
EOF

# Create parameters from JSON
cat parameters.json | jq -r '.[] | "aws ssm put-parameter --name " + .name + " --value " + .value + " --type " + .type + " --description \"" + .description + "\""' | bash

# Export all parameters for backup
aws ssm get-parameters-by-path \
    --path "/prod/myapp" \
    --recursive \
    --with-decryption \
    --query "Parameters[*].{Name:Name,Value:Value,Type:Type}" \
    --output json > parameter_backup.json
        ''', language='bash')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# CloudFormation Template for Parameter Store
AWSTemplateFormatVersion: '2010-09-09'
Description: 'AWS Systems Manager Parameter Store configuration'

Parameters:
  Environment:
    Type: String
    Default: prod
    AllowedValues: [dev, test, prod]
    Description: Environment name
  
  ApplicationName:
    Type: String
    Default: myapp
    Description: Application name

Resources:
  # KMS Key for SecureString parameters
  ParameterStoreKMSKey:
    Type: AWS::KMS::Key
    Properties:
      Description: 'KMS Key for Parameter Store SecureString encryption'
      KeyPolicy:
        Statement:
          - Sid: Enable IAM User Permissions
            Effect: Allow
            Principal:
              AWS: !Sub 'arn:aws:iam::${AWS::AccountId}:root'
            Action: 'kms:*'
            Resource: '*'
          - Sid: Allow Parameter Store
            Effect: Allow
            Principal:
              Service: ssm.amazonaws.com
            Action:
              - kms:Decrypt
              - kms:GenerateDataKey
            Resource: '*'

  # KMS Key Alias
  ParameterStoreKMSKeyAlias:
    Type: AWS::KMS::Alias
    Properties:
      AliasName: !Sub 'alias/${ApplicationName}-parameter-store-key'
      TargetKeyId: !Ref ParameterStoreKMSKey

  # Database Host Parameter
  DatabaseHostParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub '/${Environment}/${ApplicationName}/database/host'
      Type: String
      Value: !Sub '${DatabaseCluster.Endpoint.Address}'
      Description: 'Database cluster endpoint'
      Tags:
        Environment: !Ref Environment
        Application: !Ref ApplicationName
        Component: Database

  # Database Port Parameter
  DatabasePortParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub '/${Environment}/${ApplicationName}/database/port'
      Type: String
      Value: '5432'
      Description: 'Database port number'
      Tags:
        Environment: !Ref Environment
        Application: !Ref ApplicationName

  # Database Name Parameter
  DatabaseNameParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub '/${Environment}/${ApplicationName}/database/name'
      Type: String
      Value: !Ref ApplicationName
      Description: 'Database name'
      Tags:
        Environment: !Ref Environment
        Application: !Ref ApplicationName

  # Database Username Parameter
  DatabaseUsernameParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub '/${Environment}/${ApplicationName}/database/username'
      Type: String
      Value: admin
      Description: 'Database username'
      Tags:
        Environment: !Ref Environment
        Application: !Ref ApplicationName

  # Database Password (SecureString)
  DatabasePasswordParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub '/${Environment}/${ApplicationName}/database/password'
      Type: SecureString
      KeyId: !Ref ParameterStoreKMSKey
      Value: !Ref DatabasePassword
      Description: 'Database password (encrypted)'
      Tags:
        Environment: !Ref Environment
        Application: !Ref ApplicationName

  # API Endpoints (StringList)
  APIEndpointsParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub '/${Environment}/${ApplicationName}/api/endpoints'
      Type: StringList
      Value: 'https://api1.example.com,https://api2.example.com'
      Description: 'API endpoints list'
      Tags:
        Environment: !Ref Environment
        Application: !Ref ApplicationName

  # Application Configuration (JSON)
  AppConfigParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub '/${Environment}/${ApplicationName}/config/settings'
      Type: String
      Tier: Advanced
      Value: !Sub |
        {
          "logging": {
            "level": "INFO",
            "format": "json"
          },
          "features": {
            "new_dashboard": true,
            "beta_features": false
          },
          "cache": {
            "ttl": 3600,
            "max_size": 1000
          },
          "monitoring": {
            "enabled": true,
            "interval": 60
          }
        }
      Description: 'Application configuration JSON'
      Tags:
        Environment: !Ref Environment
        Application: !Ref ApplicationName

  # Feature Flags
  FeatureNewDashboardParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub '/${Environment}/${ApplicationName}/features/new_dashboard'
      Type: String
      Value: 'true'
      Description: 'Enable new dashboard feature'
      Tags:
        Environment: !Ref Environment
        Application: !Ref ApplicationName
        Component: FeatureFlag

  # Monitoring Configuration
  MonitoringEnabledParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub '/${Environment}/${ApplicationName}/monitoring/enabled'
      Type: String
      Value: 'true'
      Description: 'Enable application monitoring'
      Tags:
        Environment: !Ref Environment
        Application: !Ref ApplicationName

  # Log Level Parameter
  LogLevelParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub '/${Environment}/${ApplicationName}/logging/level'
      Type: String
      Value: 'INFO'
      Description: 'Application log level'
      Tags:
        Environment: !Ref Environment
        Application: !Ref ApplicationName

  # IAM Role for Parameter Store Access
  ParameterStoreAccessRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub '${ApplicationName}-parameter-store-role'
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: 
                - ec2.amazonaws.com
                - lambda.amazonaws.com
            Action: sts:AssumeRole
      Policies:
        - PolicyName: ParameterStoreReadPolicy
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - ssm:GetParameter
                  - ssm:GetParameters
                  - ssm:GetParametersByPath
                Resource: 
                  - !Sub 'arn:aws:ssm:${AWS::Region}:${AWS::AccountId}:parameter/${Environment}/${ApplicationName}/*'
              - Effect: Allow
                Action:
                  - kms:Decrypt
                Resource: !GetAtt ParameterStoreKMSKey.Arn

Outputs:
  ParameterStoreKMSKeyId:
    Description: 'KMS Key ID for Parameter Store encryption'
    Value: !Ref ParameterStoreKMSKey
    Export:
      Name: !Sub '${AWS::StackName}-ParameterStoreKMSKey'

  ParameterStoreAccessRoleArn:
    Description: 'IAM Role ARN for Parameter Store access'
    Value: !GetAtt ParameterStoreAccessRole.Arn
    Export:
      Name: !Sub '${AWS::StackName}-ParameterStoreRole'

  DatabaseHostParameterName:
    Description: 'Database host parameter name'
    Value: !Ref DatabaseHostParameter
    Export:
      Name: !Sub '${AWS::StackName}-DatabaseHost-Parameter'
        ''', language='yaml')
        st.markdown('</div>', unsafe_allow_html=True)

def iam_roles_tab():
    """Content for IAM Roles tab"""
    st.markdown("## 🎭 The Importance of IAM Roles")
    st.markdown("*Securely access AWS resources without long-term credentials*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Why IAM Roles Are Essential
    - **No Long-term Credentials**: Temporary credentials that automatically rotate
    - **Principle of Least Privilege**: Grant only the minimum permissions needed
    - **Cross-Account Access**: Securely access resources across AWS accounts
    - **Service Integration**: Enable AWS services to access other services on your behalf
    - **Audit Trail**: All role assumptions are logged in CloudTrail
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # IAM Roles flow diagram
    st.markdown("#### 🔄 IAM Roles Flow")
    common.mermaid(create_iam_roles_flow(), height=600)
    
    # Interactive role builder
    st.markdown("#### 🛠️ Interactive IAM Role Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        role_name = st.text_input("Role Name", value="DataLakeAccessRole")
        role_description = st.text_area("Role Description", value="Role for accessing data lake resources")
        
        trust_entity = st.selectbox("Who can assume this role?", [
            "AWS Service (EC2, Lambda, etc.)",
            "IAM User",
            "Cross-Account Access",
            "Web Identity (OIDC)",
            "SAML Federation"
        ])
        
        if trust_entity == "AWS Service (EC2, Lambda, etc.)":
            service = st.selectbox("AWS Service", ["ec2.amazonaws.com", "lambda.amazonaws.com", "glue.amazonaws.com", "redshift.amazonaws.com"])
        elif trust_entity == "Cross-Account Access":
            account_id = st.text_input("Trusted Account ID", value="123456789012")
    
    with col2:
        permissions = st.multiselect("Permissions", [
            "S3 Read Access",
            "S3 Write Access", 
            "Glue Catalog Access",
            "Athena Query Access",
            "Redshift Access",
            "KMS Decrypt",
            "CloudWatch Logs"
        ], default=["S3 Read Access", "Glue Catalog Access"])
        
        conditions = st.multiselect("Additional Conditions", [
            "Require MFA",
            "Time-based Access",
            "IP Address Restriction",
            "Source VPC Restriction"
        ])
    
    if st.button("🎭 Create IAM Role", type="primary"):
        if role_name and permissions:
            st.session_state.iam_roles_created.append({
                "name": role_name,
                "description": role_description,
                "trust_entity": trust_entity,
                "permissions": permissions,
                "conditions": conditions,
                "created_at": datetime.now()
            })
            st.success(f"✅ IAM Role '{role_name}' created successfully!")
        else:
            st.error("Please provide role name and select permissions")
    
    # Display created roles
    if st.session_state.iam_roles_created:
        st.markdown("#### 📋 Created IAM Roles")
        for role in st.session_state.iam_roles_created:
            with st.expander(f"🎭 {role['name']}"):
                st.write(f"**Description:** {role['description']}")
                st.write(f"**Trust Entity:** {role['trust_entity']}")
                st.write(f"**Permissions:** {', '.join(role['permissions'])}")
                if role['conditions']:
                    st.write(f"**Conditions:** {', '.join(role['conditions'])}")
                st.write(f"**Created:** {role['created_at'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Role types and use cases
    st.markdown("#### 🎯 Common IAM Role Patterns")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="service-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 🖥️ Service Roles
        **For AWS Services:**
        - **EC2 Instance Roles**: Access AWS services from EC2
        - **Lambda Execution Roles**: Enable Lambda functions to access AWS resources
        - **Glue Service Roles**: Allow Glue to access data sources and targets
        - **Redshift Roles**: Enable Redshift to access S3 for COPY/UNLOAD
        
        **Benefits:**
        - No credentials in code
        - Automatic credential rotation
        - Integrated with AWS services
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="service-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔗 Cross-Account Roles
        **For Multi-Account Access:**
        - **Centralized Logging**: Log aggregation across accounts
        - **Shared Services**: Access shared resources
        - **Deployment Pipelines**: Deploy across environments
        - **Data Sharing**: Share data lake resources
        
        **Benefits:**
        - Secure cross-account access
        - Centralized permission management
        - Audit trail across accounts
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 IAM Role Implementation")
    
    tab1, tab2, tab3 = st.tabs(["Trust Policies", "Permission Policies", "Assuming Roles"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Trust Policy Examples

# 1. EC2 Instance Role Trust Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

# 2. Lambda Execution Role Trust Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

# 3. Cross-Account Access Trust Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123456789012:root"
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

# 4. Federated User Trust Policy (OIDC)
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE"
            },
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringEquals": {
                    "oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE:sub": "system:serviceaccount:default:my-service-account",
                    "oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE:aud": "sts.amazonaws.com"
                }
            }
        }
    ]
}

# 5. Time-based Access Trust Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123456789012:user/DataAnalyst"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "DateGreaterThan": {
                    "aws:CurrentTime": "2024-01-01T00:00:00Z"
                },
                "DateLessThan": {
                    "aws:CurrentTime": "2024-12-31T23:59:59Z"
                },
                "IpAddress": {
                    "aws:SourceIp": "203.0.113.0/24"
                }
            }
        }
    ]
}

# 6. MFA Required Trust Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123456789012:user/AdminUser"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "Bool": {
                    "aws:MultiFactorAuthPresent": "true"
                },
                "NumericLessThan": {
                    "aws:MultiFactorAuthAge": "3600"
                }
            }
        }
    ]
}

# 7. VPC Endpoint Restricted Trust Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "aws:SourceVpce": "vpce-1a2b3c4d"
                }
            }
        }
    ]
}

# 8. Multiple Services Trust Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "ec2.amazonaws.com",
                    "lambda.amazonaws.com",
                    "glue.amazonaws.com"
                ]
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
        ''', language='json')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Permission Policy Examples for Data Engineering Roles

# 1. Data Lake Read Access Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::my-data-lake",
                "arn:aws:s3:::my-data-lake/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartitions"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StartQueryExecution"
            ],
            "Resource": "*"
        }
    ]
}

# 2. ETL Job Execution Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::source-bucket",
                "arn:aws:s3:::source-bucket/*",
                "arn:aws:s3:::target-bucket",
                "arn:aws:s3:::target-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:CreatePartition",
                "glue:UpdatePartition"
            ],
            "Resource": "*"
        },
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

# 3. Redshift Data Access Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::data-warehouse-bucket",
                "arn:aws:s3:::data-warehouse-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "redshift:GetClusterCredentials"
            ],
            "Resource": [
                "arn:aws:redshift:*:*:dbuser:*/redshift_user",
                "arn:aws:redshift:*:*:dbname:*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "kms:Decrypt",
                "kms:GenerateDataKey"
            ],
            "Resource": "arn:aws:kms:*:*:key/key-id",
            "Condition": {
                "StringEquals": {
                    "kms:ViaService": "s3.us-west-2.amazonaws.com"
                }
            }
        }
    ]
}

# 4. Secrets Manager Access Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue"
            ],
            "Resource": [
                "arn:aws:secretsmanager:*:*:secret:prod/database/*",
                "arn:aws:secretsmanager:*:*:secret:prod/api-keys/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "kms:Decrypt"
            ],
            "Resource": "arn:aws:kms:*:*:key/*",
            "Condition": {
                "StringEquals": {
                    "kms:ViaService": "secretsmanager.us-west-2.amazonaws.com"
                }
            }
        }
    ]
}

# 5. Parameter Store Access Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ssm:GetParameter",
                "ssm:GetParameters",
                "ssm:GetParametersByPath"
            ],
            "Resource": [
                "arn:aws:ssm:*:*:parameter/prod/myapp/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "kms:Decrypt"
            ],
            "Resource": "arn:aws:kms:*:*:key/*",
            "Condition": {
                "StringEquals": {
                    "kms:ViaService": "ssm.us-west-2.amazonaws.com"
                }
            }
        }
    ]
}

# 6. Lambda Function Policy (Data Processing)
{
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
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": "arn:aws:s3:::lambda-processing-bucket/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:GetItem",
                "dynamodb:UpdateItem",
                "dynamodb:Query"
            ],
            "Resource": "arn:aws:dynamodb:*:*:table/ProcessingStatus"
        }
    ]
}

# 7. Cross-Account Data Sharing Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::shared-data-bucket",
                "arn:aws:s3:::shared-data-bucket/*"
            ],
            "Condition": {
                "StringEquals": {
                    "s3:ExistingObjectTag/Environment": "Shared"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess"
            ],
            "Resource": "*"
        }
    ]
}
        ''', language='json')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
from botocore.exceptions import ClientError
import json
import time

class IAMRoleManager:
    def __init__(self, region='us-west-2'):
        self.sts_client = boto3.client('sts', region_name=region)
        self.iam_client = boto3.client('iam', region_name=region)
    
    def assume_role(self, role_arn, session_name, duration_seconds=3600, external_id=None):
        """Assume an IAM role and return temporary credentials"""
        try:
            assume_role_params = {
                'RoleArn': role_arn,
                'RoleSessionName': session_name,
                'DurationSeconds': duration_seconds
            }
            
            if external_id:
                assume_role_params['ExternalId'] = external_id
            
            response = self.sts_client.assume_role(**assume_role_params)
            
            credentials = response['Credentials']
            return {
                'AccessKeyId': credentials['AccessKeyId'],
                'SecretAccessKey': credentials['SecretAccessKey'],
                'SessionToken': credentials['SessionToken'],
                'Expiration': credentials['Expiration']
            }
        except ClientError as e:
            print(f"Error assuming role: {e}")
            return None
    
    def create_session_with_role(self, role_arn, session_name, duration_seconds=3600):
        """Create a boto3 session using assumed role credentials"""
        credentials = self.assume_role(role_arn, session_name, duration_seconds)
        
        if credentials:
            return boto3.Session(
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
        return None
    
    def get_caller_identity(self):
        """Get current caller identity"""
        try:
            response = self.sts_client.get_caller_identity()
            return response
        except ClientError as e:
            print(f"Error getting caller identity: {e}")
            return None

# Example 1: Basic Role Assumption
def assume_data_lake_role():
    """Assume a data lake access role"""
    role_manager = IAMRoleManager()
    
    role_arn = "arn:aws:iam::123456789012:role/DataLakeAccessRole"
    session_name = "DataLakeAccess-Session"
    
    # Get current identity
    current_identity = role_manager.get_caller_identity()
    print(f"Current identity: {current_identity['Arn']}")
    
    # Assume the role
    credentials = role_manager.assume_role(role_arn, session_name)
    
    if credentials:
        print("Successfully assumed role!")
        print(f"Access Key: {credentials['AccessKeyId']}")
        print(f"Expires: {credentials['Expiration']}")
        
        # Create a new session with the assumed role
        session = boto3.Session(
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )
        
        # Use the session to access S3
        s3_client = session.client('s3')
        
        try:
            # List buckets accessible with the assumed role
            response = s3_client.list_buckets()
            print(f"Accessible buckets: {[b['Name'] for b in response['Buckets']]}")
        except ClientError as e:
            print(f"Error accessing S3: {e}")
    else:
        print("Failed to assume role")

# Example 2: Cross-Account Access
def cross_account_access():
    """Access resources in another AWS account"""
    role_manager = IAMRoleManager()
    
    # Cross-account role ARN
    cross_account_role_arn = "arn:aws:iam::987654321098:role/CrossAccountDataAccess"
    session_name = "CrossAccountAccess"
    external_id = "unique-external-id-12345"
    
    # Assume cross-account role
    session = role_manager.create_session_with_role(
        cross_account_role_arn, 
        session_name, 
        duration_seconds=3600
    )
    
    if session:
        print("Successfully assumed cross-account role!")
        
        # Access cross-account resources
        s3_client = session.client('s3')
        glue_client = session.client('glue')
        
        try:
            # List databases in the cross-account Glue catalog
            response = glue_client.get_databases()
            print(f"Cross-account databases: {[db['Name'] for db in response['DatabaseList']]}")
        except ClientError as e:
            print(f"Error accessing cross-account Glue: {e}")

# Example 3: Lambda Function Role Usage
def lambda_handler(event, context):
    """Lambda function using IAM role for AWS service access"""
    
    # Lambda automatically assumes the execution role
    # No need to manually assume role
    
    # Access AWS services directly
    s3_client = boto3.client('s3')
    secrets_client = boto3.client('secretsmanager')
    
    try:
        # Get database credentials from Secrets Manager
        secret_response = secrets_client.get_secret_value(
            SecretId='prod/database/credentials'
        )
        db_credentials = json.loads(secret_response['SecretString'])
        
        # Process data from S3
        s3_response = s3_client.get_object(
            Bucket='data-processing-bucket',
            Key='input/data.json'
        )
        
        # Process the data...
        processed_data = process_data(s3_response['Body'].read())
        
        # Write results back to S3
        s3_client.put_object(
            Bucket='data-processing-bucket',
            Key='output/processed_data.json',
            Body=json.dumps(processed_data)
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data processing completed successfully')
        }
        
    except ClientError as e:
        print(f"AWS service error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

# Example 4: Temporary Elevated Access
def temporary_admin_access():
    """Use a role for temporary elevated access"""
    role_manager = IAMRoleManager()
    
    admin_role_arn = "arn:aws:iam::123456789012:role/TemporaryAdminRole"
    session_name = "EmergencyAccess"
    
    # Assume role for 15 minutes only
    session = role_manager.create_session_with_role(
        admin_role_arn, 
        session_name, 
        duration_seconds=900  # 15 minutes
    )
    
    if session:
        print("Temporary admin access granted")
        
        # Perform administrative tasks
        iam_client = session.client('iam')
        ec2_client = session.client('ec2')
        
        try:
            # Example: Stop all instances in emergency
            instances = ec2_client.describe_instances()
            for reservation in instances['Reservations']:
                for instance in reservation['Instances']:
                    if instance['State']['Name'] == 'running':
                        ec2_client.stop_instances(InstanceIds=[instance['InstanceId']])
                        print(f"Stopped instance: {instance['InstanceId']}")
        
        except ClientError as e:
            print(f"Error during emergency operations: {e}")
        
        print("Temporary access will expire in 15 minutes")

# Example 5: Service-to-Service Communication
def service_to_service_communication():
    """EC2 instance accessing other AWS services via instance role"""
    
    # EC2 instance automatically has access to instance metadata
    # to retrieve temporary credentials
    
    try:
        # Create clients - credentials automatically retrieved from instance metadata
        s3_client = boto3.client('s3')
        rds_client = boto3.client('rds')
        
        # Access S3 bucket
        response = s3_client.list_objects_v2(Bucket='application-data')
        print(f"Found {response.get('KeyCount', 0)} objects in S3")
        
        # Access RDS
        db_instances = rds_client.describe_db_instances()
        print(f"Found {len(db_instances['DBInstances'])} RDS instances")
        
    except ClientError as e:
        print(f"Service communication error: {e}")

# Example 6: Role Chain (Role Assumption Chain)
def role_chain_example():
    """Demonstrate role chaining for complex access patterns"""
    role_manager = IAMRoleManager()
    
    # First, assume a role that can assume other roles
    intermediate_role_arn = "arn:aws:iam::123456789012:role/IntermediateRole"
    session = role_manager.create_session_with_role(
        intermediate_role_arn, 
        "IntermediateAccess"
    )
    
    if session:
        # Now use the intermediate role to assume the final role
        sts_client = session.client('sts')
        
        try:
            final_role_response = sts_client.assume_role(
                RoleArn="arn:aws:iam::123456789012:role/FinalAccessRole",
                RoleSessionName="FinalAccess"
            )
            
            # Create session with final role credentials
            final_credentials = final_role_response['Credentials']
            final_session = boto3.Session(
                aws_access_key_id=final_credentials['AccessKeyId'],
                aws_secret_access_key=final_credentials['SecretAccessKey'],
                aws_session_token=final_credentials['SessionToken']
            )
            
            # Access resources with final role
            s3_client = final_session.client('s3')
            # ... perform operations
            
        except ClientError as e:
            print(f"Role chain error: {e}")

# Usage examples
if __name__ == "__main__":
    # Test different role assumption patterns
    print("1. Basic role assumption:")
    assume_data_lake_role()
    
    print("\n2. Cross-account access:")
    cross_account_access()
    
    print("\n3. Temporary admin access:")
    temporary_admin_access()
    
    print("\n4. Service communication:")
    service_to_service_communication()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def manual_data_lake_tab():
    """Content for Manual Data Lake Building tab"""
    st.markdown("## 🏗️ Building a Data Lake Manually")
    st.markdown("*Traditional approach to data lake construction with full control over each component*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Manual Data Lake Construction
    Building a data lake manually requires careful orchestration of multiple AWS services:
    - **Ingestion**: Multiple methods for data ingestion from various sources
    - **Storage**: Organized S3 structure with proper partitioning and formats
    - **Processing**: ETL pipelines using Glue, EMR, or Lambda
    - **Cataloging**: Metadata management with Glue Data Catalog
    - **Security**: IAM policies, encryption, and access controls
    - **Monitoring**: CloudWatch, CloudTrail for observability
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data lake architecture
    st.markdown("#### 🏗️ Manual Data Lake Architecture")
    common.mermaid(create_data_lake_architecture(), height=800)
    
    # Interactive data lake builder
    st.markdown("#### 🎯 Interactive Data Lake Component Builder")
    
    # Component selection
    st.markdown("### Select Data Lake Components")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("##### 📥 Ingestion Layer")
        ingestion_services = st.multiselect("Ingestion Services", [
            "Amazon Kinesis Data Streams",
            "Amazon Kinesis Data Firehose", 
            "AWS Data Migration Service",
            "AWS Glue ETL Jobs",
            "Lambda Functions",
            "Direct Upload to S3"
        ], default=["Amazon Kinesis Data Firehose", "AWS Glue ETL Jobs"])
    
    with col2:
        st.markdown("##### 🗄️ Storage Layer")
        storage_zones = st.multiselect("Storage Zones", [
            "Raw Data Zone",
            "Processed Data Zone",
            "Curated Data Zone",
            "Archive Zone"
        ], default=["Raw Data Zone", "Processed Data Zone", "Curated Data Zone"])
        
        file_formats = st.multiselect("File Formats", [
            "Parquet", "JSON", "CSV", "Avro", "ORC"
        ], default=["Parquet", "JSON"])
    
    with col3:
        st.markdown("##### ⚙️ Processing Layer")
        processing_services = st.multiselect("Processing Services", [
            "AWS Glue Jobs",
            "Amazon EMR",
            "AWS Lambda",
            "Amazon Athena",
            "Apache Spark on EMR"
        ], default=["AWS Glue Jobs", "Amazon Athena"])
    
    # Analytics and security
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### 📊 Analytics Layer")
        analytics_services = st.multiselect("Analytics Services", [
            "Amazon Athena",
            "Amazon Redshift",
            "Amazon QuickSight",
            "Amazon SageMaker",
            "Amazon EMR Notebooks"
        ], default=["Amazon Athena", "Amazon QuickSight"])
    
    with col2:
        st.markdown("##### 🔒 Security & Governance")
        security_components = st.multiselect("Security Components", [
            "AWS IAM Policies",
            "AWS KMS Encryption",
            "AWS CloudTrail",
            "AWS Config",
            "VPC Endpoints",
            "S3 Bucket Policies"
        ], default=["AWS IAM Policies", "AWS KMS Encryption", "AWS CloudTrail"])
    
    if st.button("🏗️ Build Data Lake Architecture", type="primary"):
        components = {
            "ingestion": ingestion_services,
            "storage": storage_zones,
            "formats": file_formats,
            "processing": processing_services,
            "analytics": analytics_services,
            "security": security_components,
            "created_at": datetime.now()
        }
        st.session_state.data_lake_components.append(components)
        st.success("✅ Data Lake architecture defined successfully!")
    
    # Display architecture summary
    if st.session_state.data_lake_components:
        st.markdown("#### 📋 Data Lake Architecture Summary")
        latest_architecture = st.session_state.data_lake_components[-1]
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Ingestion Services", len(latest_architecture["ingestion"]))
            st.metric("Storage Zones", len(latest_architecture["storage"]))
            st.metric("Processing Services", len(latest_architecture["processing"]))
        
        with col2:
            st.metric("Analytics Services", len(latest_architecture["analytics"]))
            st.metric("Security Components", len(latest_architecture["security"]))
            st.metric("File Formats", len(latest_architecture["formats"]))
    
    # Data lake layers breakdown
    st.markdown("#### 📊 Data Lake Layers Deep Dive")
    
    tab1, tab2, tab3, tab4 = st.tabs(["🏗️ Ingestion", "🗄️ Storage", "⚙️ Processing", "🔒 Security"])
    
    with tab1:
        st.markdown("### 📥 Data Ingestion Layer")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="service-feature">', unsafe_allow_html=True)
            st.markdown("""
            #### 🌊 Streaming Data
            **Amazon Kinesis Data Streams**
            - Real-time data streaming
            - Scalable shard-based architecture
            - Millisecond latency
            
            **Amazon Kinesis Data Firehose**
            - Fully managed delivery service
            - Automatic scaling
            - Built-in data transformation
            - Direct delivery to S3, Redshift, ElasticSearch
            """)
            st.markdown('</div>', unsafe_allow_html=True)
            
        with col2:
            st.markdown('<div class="service-feature">', unsafe_allow_html=True)
            st.markdown("""
            #### 📦 Batch Data
            **AWS Data Migration Service**
            - Database migration and replication
            - Continuous data replication
            - Minimal downtime
            
            **AWS Glue ETL**
            - Serverless ETL service
            - Automatic schema discovery
            - Visual ETL designer
            - Built-in data catalog integration
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Code example for ingestion
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Sample ingestion setup using AWS Glue
import boto3
import json

glue_client = boto3.client('glue')

# Create Glue job for data ingestion
def create_ingestion_job():
    job_definition = {
        'Name': 'data-lake-ingestion-job',
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': 's3://my-glue-scripts/ingestion_job.py',
            'PythonVersion': '3'
        },
        'DefaultArguments': {
            '--job-language': 'python',
            '--enable-continuous-cloudwatch-log': 'true',
            '--source-bucket': 'source-data-bucket',
            '--target-bucket': 'data-lake-raw-zone',
            '--catalog-database': 'data_lake_catalog'
        },
        'MaxRetries': 3,
        'Timeout': 60,
        'GlueVersion': '3.0'
    }
    
    response = glue_client.create_job(**job_definition)
    return response['Name']

# Kinesis Data Firehose delivery stream
def create_firehose_stream():
    firehose_client = boto3.client('firehose')
    
    stream_config = {
        'DeliveryStreamName': 'data-lake-ingestion-stream',
        'DeliveryStreamType': 'DirectPut',
        'S3DestinationConfiguration': {
            'RoleARN': 'arn:aws:iam::123456789012:role/FirehoseRole',
            'BucketARN': 'arn:aws:s3:::data-lake-raw-zone',
            'Prefix': 'streaming-data/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/',
            'ErrorOutputPrefix': 'errors/',
            'BufferingHints': {
                'SizeInMBs': 128,
                'IntervalInSeconds': 60
            },
            'CompressionFormat': 'GZIP',
            'DataFormatConversionConfiguration': {
                'Enabled': True,
                'OutputFormatConfiguration': {
                    'Serializer': {
                        'ParquetSerDe': {}
                    }
                }
            }
        }
    }
    
    response = firehose_client.create_delivery_stream(**stream_config)
    return response['DeliveryStreamARN']
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown("### 🗄️ Storage Layer Organization")
        
        # Storage zones visualization
        storage_zones_data = {
            'Zone': ['Raw Zone', 'Processed Zone', 'Curated Zone', 'Archive Zone'],
            'Purpose': [
                'Unprocessed source data',
                'Cleaned and transformed data',
                'Business-ready datasets',
                'Long-term storage'
            ],
            'Format': ['Original format', 'Parquet/ORC', 'Parquet', 'Compressed'],
            'Retention': ['7 days', '30 days', '5 years', 'Indefinite'],
            'Access Pattern': ['Write-heavy', 'Read/Write', 'Read-heavy', 'Rare access']
        }
        
        st.dataframe(pd.DataFrame(storage_zones_data), use_container_width=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# S3 Bucket Structure and Policies
{
    "data-lake-raw-zone": {
        "path_structure": "s3://data-lake-raw-zone/source={source}/year={year}/month={month}/day={day}/",
        "lifecycle_policy": {
            "transition_to_ia": "30 days",
            "transition_to_glacier": "90 days",
            "expiration": "2555 days"
        },
        "encryption": "aws:kms",
        "versioning": "Enabled"
    },
    "data-lake-processed-zone": {
        "path_structure": "s3://data-lake-processed-zone/dataset={dataset}/year={year}/month={month}/",
        "file_format": "parquet",
        "compression": "snappy",
        "partitioning": "date-based"
    },
    "data-lake-curated-zone": {
        "path_structure": "s3://data-lake-curated-zone/domain={domain}/dataset={dataset}/",
        "optimizations": [
            "columnar_format",
            "predicate_pushdown",
            "partition_pruning"
        ],
        "access_control": "fine_grained"
    }
}

# CloudFormation template for S3 bucket setup
Resources:
  DataLakeRawZone:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub '${AWS::StackName}-data-lake-raw-zone'
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - ServerSideEncryptionByDefault:
              SSEAlgorithm: aws:kms
              KMSMasterKeyID: !Ref DataLakeKMSKey
      VersioningConfiguration:
        Status: Enabled
      LifecycleConfiguration:
        Rules:
          - Id: DataLakeLifecycleRule
            Status: Enabled
            Transitions:
              - TransitionInDays: 30
                StorageClass: STANDARD_IA
              - TransitionInDays: 90
                StorageClass: GLACIER
            ExpirationInDays: 2555
      NotificationConfiguration:
        LambdaConfigurations:
          - Event: s3:ObjectCreated:*
            Function: !GetAtt DataProcessingLambda.Arn
        ''', language='yaml')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown("### ⚙️ Processing Layer")
        
        processing_comparison = {
            'Service': ['AWS Glue', 'Amazon EMR', 'AWS Lambda', 'Amazon Athena'],
            'Best For': [
                'ETL jobs, Data cataloging',
                'Big data processing, Spark/Hadoop',
                'Lightweight processing, Event-driven',
                'Ad-hoc queries, SQL analytics'
            ],
            'Scaling': ['Automatic', 'Manual/Auto', 'Automatic', 'Automatic'],
            'Cost Model': ['Per DPU-hour', 'Per instance-hour', 'Per invocation', 'Per query'],
            'Max Runtime': ['48 hours', 'Unlimited', '15 minutes', '30 minutes']
        }
        
        st.dataframe(pd.DataFrame(processing_comparison), use_container_width=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# AWS Glue ETL Job Example
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'target_bucket'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from S3 (Raw Zone)
source_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [f"s3://{args['source_bucket']}/raw-data/"],
        "recurse": True
    },
    transformation_ctx="source_df"
)

# Data transformations
def process_data(dynamic_frame):
    # Convert to Spark DataFrame for complex operations
    df = dynamic_frame.toDF()
    
    # Data cleaning and transformation
    df_cleaned = df.filter(df.amount > 0) \
                   .withColumn("processed_date", current_timestamp()) \
                   .withColumn("year", year(df.transaction_date)) \
                   .withColumn("month", month(df.transaction_date))
    
    # Convert back to DynamicFrame
    return DynamicFrame.fromDF(df_cleaned, glueContext, "processed_df")

# Apply transformations
processed_df = process_data(source_df)

# Write to S3 (Processed Zone) in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=processed_df,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": f"s3://{args['target_bucket']}/processed-data/",
        "partitionKeys": ["year", "month"]
    },
    transformation_ctx="output"
)

# Update Glue Data Catalog
glueContext.write_dynamic_frame.from_catalog(
    frame=processed_df,
    database="data_lake_catalog",
    table_name="processed_transactions",
    transformation_ctx="catalog_output"
)

job.commit()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab4:
        st.markdown("### 🔒 Security and Governance")
        
        security_layers = {
            'Layer': ['Identity & Access', 'Encryption', 'Network', 'Monitoring', 'Compliance'],
            'Components': [
                'IAM Policies, Roles, Users',
                'KMS, S3 Encryption, TLS',
                'VPC, Security Groups, NACLs',
                'CloudTrail, CloudWatch, Config',
                'Data Classification, Retention'
            ],
            'Implementation': [
                'Least privilege access',
                'Encryption at rest and in transit',
                'Network segmentation',
                'Continuous monitoring',
                'Automated compliance checking'
            ]
        }
        
        st.dataframe(pd.DataFrame(security_layers), use_container_width=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Comprehensive Data Lake Security Setup

# S3 Bucket Policy for Data Lake
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DenyInsecureConnections",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::data-lake-bucket",
                "arn:aws:s3:::data-lake-bucket/*"
            ],
            "Condition": {
                "Bool": {
                    "aws:SecureTransport": "false"
                }
            }
        },
        {
            "Sid": "AllowDataLakeAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123456789012:role/DataLakeAccessRole"
            },
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::data-lake-bucket",
                "arn:aws:s3:::data-lake-bucket/*"
            ],
            "Condition": {
                "StringEquals": {
                    "s3:x-amz-server-side-encryption": "aws:kms"
                }
            }
        }
    ]
}

# KMS Key Policy for Data Lake Encryption
{
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
            "Sid": "Allow S3 Service",
            "Effect": "Allow",
            "Principal": {
                "Service": "s3.amazonaws.com"
            },
            "Action": [
                "kms:Decrypt",
                "kms:GenerateDataKey"
            ],
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
                "kms:GenerateDataKey"
            ],
            "Resource": "*"
        }
    ]
}

# CloudTrail Configuration for Data Lake Auditing
{
    "TrailName": "DataLakeAuditTrail",
    "S3BucketName": "data-lake-audit-logs",
    "EventSelectors": [
        {
            "ReadWriteType": "All",
            "IncludeManagementEvents": true,
            "DataResources": [
                {
                    "Type": "AWS::S3::Object",
                    "Values": ["arn:aws:s3:::data-lake-*/*"]
                },
                {
                    "Type": "AWS::S3::Bucket",
                    "Values": ["arn:aws:s3:::data-lake-*"]
                }
            ]
        }
    ],
    "InsightSelectors": [
        {
            "InsightType": "ApiCallRateInsight"
        }
    ]
}
        ''', language='json')
        st.markdown('</div>', unsafe_allow_html=True)

def lake_formation_tab():
    """Content for AWS Lake Formation tab"""
    st.markdown("## 🚀 AWS Lake Formation")
    st.markdown("*Simplified data lake creation and management with built-in security and governance*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 AWS Lake Formation Benefits
    AWS Lake Formation simplifies the complex process of building and managing data lakes:
    - **Automated Setup**: Blueprints automate common ingestion patterns
    - **Centralized Security**: Single place to manage data lake permissions
    - **Built-in Governance**: Automatic data discovery and cataloging
    - **Fine-grained Access Control**: Table, column, and row-level security
    - **Integration**: Works seamlessly with analytics services
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Lake Formation architecture
    st.markdown("#### 🏗️ AWS Lake Formation Architecture")
    common.mermaid(create_lake_formation_architecture(), height=700)
    
    # Interactive Lake Formation setup
    st.markdown("#### ⚙️ Interactive Lake Formation Setup")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### 🗄️ Data Sources")
        data_sources = st.multiselect("Select Data Sources", [
            "Amazon RDS", "Amazon S3", "Amazon Redshift", 
            "Amazon Aurora", "Amazon DocumentDB", "On-premises databases"
        ], default=["Amazon RDS", "Amazon S3"])
        
        blueprint_type = st.selectbox("Blueprint Type", [
            "Database Snapshot", "Incremental Database", "Log File", "Cloudtrail Logs"
        ])
        
    with col2:
        st.markdown("##### 📊 Target Configuration")
        target_database = st.text_input("Target Database Name", value="sales_data_lake")
        target_s3_path = st.text_input("S3 Path", value="s3://my-data-lake/sales/")
        
        data_format = st.selectbox("Data Format", ["Parquet", "JSON", "CSV", "Avro"])
        
        partitioning = st.multiselect("Partitioning Columns", [
            "year", "month", "day", "region", "product_category"
        ], default=["year", "month"])
    
    # Permissions configuration
    st.markdown("##### 🛡️ Permissions Configuration")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        principals = st.multiselect("Grant Access To", [
            "Data Analysts", "Data Scientists", "BI Developers", 
            "External Partners", "Audit Team"
        ], default=["Data Analysts", "Data Scientists"])
        
    with col2:
        permissions = st.multiselect("Permissions", [
            "SELECT", "INSERT", "DELETE", "ALTER", "CREATE_TABLE", "DROP"
        ], default=["SELECT"])
        
    with col3:
        resource_scope = st.selectbox("Resource Scope", [
            "Entire Database", "Specific Tables", "Specific Columns", "Filtered Rows"
        ])
    
    if st.button("🚀 Configure Lake Formation", type="primary"):
        st.session_state.lake_formation_configured = True
        st.success("✅ Lake Formation configured successfully!")
        
        # Show configuration summary
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 📋 Lake Formation Configuration Summary
        **Data Sources**: {', '.join(data_sources)}  
        **Blueprint**: {blueprint_type}  
        **Target Database**: {target_database}  
        **Data Format**: {data_format}  
        **Partitioning**: {', '.join(partitioning)}  
        **Principals**: {', '.join(principals)}  
        **Permissions**: {', '.join(permissions)}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Lake Formation vs Manual comparison
    st.markdown("#### ⚖️ Lake Formation vs Manual Implementation")
    
    comparison_data = {
        'Aspect': [
            'Setup Time', 'Security Management', 'Data Discovery', 
            'Permission Management', 'Schema Evolution', 'Monitoring',
            'Cost Management', 'Compliance'
        ],
        'Manual Approach': [
            'Weeks to months', 'Complex IAM policies', 'Manual crawlers',
            'Multiple permission systems', 'Manual schema updates', 'Multiple tools',
            'Manual optimization', 'Manual auditing'
        ],
        'Lake Formation': [
            'Hours to days', 'Centralized control', 'Automatic discovery',
            'Single permission model', 'Automatic handling', 'Built-in monitoring',
            'Automatic optimization', 'Built-in compliance'
        ],
        'Improvement': [
            '10x faster', '5x simpler', '3x faster', '8x simpler',
            '5x faster', '3x simpler', '2x efficient', '4x easier'
        ]
    }
    
    comparison_df = pd.DataFrame(comparison_data)
    st.dataframe(comparison_df, use_container_width=True)
    
    # Lake Formation features
    st.markdown("#### ⭐ Key Features Deep Dive")
    
    tab1, tab2, tab3 = st.tabs(["🔄 Blueprints", "🛡️ Security", "📊 Integration"])
    
    with tab1:
        st.markdown("### 🔄 Lake Formation Blueprints")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="service-feature">', unsafe_allow_html=True)
            st.markdown("""
            #### 🗃️ Database Blueprints
            **Full Database Ingestion**
            - Complete database snapshot
            - All tables and relationships
            - Automatic schema mapping
            - Incremental updates
            
            **Incremental Database**
            - Change data capture
            - Real-time synchronization
            - Minimal impact on source
            - Automatic conflict resolution
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="service-feature">', unsafe_allow_html=True)
            st.markdown("""
            #### 📄 Log File Blueprints
            **Application Logs**
            - Structured log parsing
            - Automatic field extraction
            - Error handling and retry
            - Compression and optimization
            
            **CloudTrail Logs**
            - AWS API call analysis
            - Security event processing
            - Compliance reporting
            - Anomaly detection
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
import json

# Lake Formation Blueprint Creation
lakeformation_client = boto3.client('lakeformation')

def create_database_blueprint():
    """Create a blueprint for database ingestion"""
    
    blueprint_config = {
        'Name': 'sales-database-ingestion',
        'Description': 'Ingest sales database into data lake',
        'BlueprintLocation': 's3://my-blueprints/database-ingestion/',
        'BlueprintParameterSpec': json.dumps({
            'source_database': {
                'type': 'RDS',
                'connection_name': 'sales-rds-connection',
                'database_name': 'sales_prod',
                'tables': ['customers', 'orders', 'products', 'order_items']
            },
            'target_configuration': {
                's3_path': 's3://my-data-lake/sales/',
                'database_name': 'sales_data_lake',
                'format': 'parquet',
                'compression': 'snappy',
                'partitioning': ['year', 'month']
            },
            'transformation_options': {
                'data_cleaning': True,
                'deduplication': True,
                'schema_evolution': True,
                'data_quality_checks': True
            }
        })
    }
    
    try:
        response = lakeformation_client.create_blueprint(**blueprint_config)
        print(f"Blueprint created: {response['BlueprintName']}")
        return response
    except Exception as e:
        print(f"Error creating blueprint: {e}")
        return None

def create_log_file_blueprint():
    """Create a blueprint for log file ingestion"""
    
    blueprint_config = {
        'Name': 'application-logs-ingestion',
        'Description': 'Ingest application logs into data lake',
        'BlueprintLocation': 's3://my-blueprints/log-ingestion/',
        'BlueprintParameterSpec': json.dumps({
            'source_configuration': {
                'log_format': 'json',
                's3_source_path': 's3://application-logs/access-logs/',
                'log_pattern': '*.log',
                'exclusion_patterns': ['*.tmp', '*.bak']
            },
            'target_configuration': {
                's3_path': 's3://my-data-lake/logs/',
                'database_name': 'application_logs',
                'table_name': 'access_logs',
                'format': 'parquet',
                'partitioning': ['year', 'month', 'day', 'hour']
            },
            'parsing_configuration': {
                'field_mappings': {
                    'timestamp': 'event_time',
                    'ip_address': 'client_ip',
                    'user_agent': 'user_agent',
                    'status_code': 'http_status'
                },
                'field_transformations': {
                    'event_time': 'timestamp',
                    'http_status': 'integer',
                    'response_size': 'bigint'
                }
            }
        })
    }
    
    try:
        response = lakeformation_client.create_blueprint(**blueprint_config)
        print(f"Log blueprint created: {response['BlueprintName']}")
        return response
    except Exception as e:
        print(f"Error creating log blueprint: {e}")
        return None

def run_blueprint():
    """Execute a blueprint to start data ingestion"""
    
    workflow_config = {
        'Name': 'sales-data-ingestion-workflow',
        'Description': 'Automated sales data ingestion workflow',
        'BlueprintName': 'sales-database-ingestion',
        'ScheduleConfig': {
            'ScheduleExpression': 'cron(0 2 * * ? *)',  # Daily at 2 AM
            'StartOnCreation': True
        },
        'WorkflowParameterSpec': json.dumps({
            'connection_name': 'sales-rds-connection',
            'source_database': 'sales_prod',
            'target_s3_path': 's3://my-data-lake/sales/',
            'target_database': 'sales_data_lake',
            'data_format': 'parquet',
            'partitioning_keys': ['year', 'month'],
            'compression': 'snappy',
            'enable_bookmarks': True,
            'data_quality_rules': [
                'customers.customer_id IS NOT NULL',
                'orders.order_date >= CURRENT_DATE - INTERVAL 1 YEAR',
                'products.price > 0'
            ]
        })
    }
    
    try:
        response = lakeformation_client.start_blueprint_run(**workflow_config)
        print(f"Blueprint run started: {response['RunId']}")
        return response
    except Exception as e:
        print(f"Error starting blueprint run: {e}")
        return None

# Monitor blueprint execution
def monitor_blueprint_run(run_id):
    """Monitor the progress of a blueprint run"""
    
    try:
        response = lakeformation_client.get_blueprint_run(
            BlueprintName='sales-database-ingestion',
            RunId=run_id
        )
        
        run_status = response['BlueprintRun']
        print(f"Run Status: {run_status['State']}")
        print(f"Completed Steps: {run_status.get('CompletedSteps', 0)}")
        print(f"Total Steps: {run_status.get('TotalSteps', 0)}")
        
        if run_status['State'] == 'FAILED':
            print(f"Error Message: {run_status.get('ErrorMessage', 'Unknown error')}")
        
        return run_status
    except Exception as e:
        print(f"Error monitoring blueprint run: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Create blueprints
    db_blueprint = create_database_blueprint()
    log_blueprint = create_log_file_blueprint()
    
    # Run blueprint
    if db_blueprint:
        run_response = run_blueprint()
        if run_response:
            # Monitor progress
            import time
            run_id = run_response['RunId']
            while True:
                status = monitor_blueprint_run(run_id)
                if status and status['State'] in ['SUCCEEDED', 'FAILED']:
                    break
                time.sleep(30)  # Check every 30 seconds
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown("### 🛡️ Lake Formation Security Model")
        
        st.markdown('<div class="info-box">', unsafe_allow_html=True)
        st.markdown("""
        **Lake Formation Security Layers:**
        - **IAM Permissions**: Control access to Lake Formation APIs and resources
        - **Data Location Permissions**: Control who can create databases and tables
        - **Data Permissions**: Fine-grained access to databases, tables, and columns
        - **Tag-based Access**: Use tags for dynamic permission management
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Permission levels
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="service-feature">', unsafe_allow_html=True)
            st.markdown("""
            #### 🗄️ Database Level
            - CREATE_DATABASE
            - ALTER_DATABASE
            - DROP_DATABASE
            - DESCRIBE_DATABASE
            
            #### 📊 Table Level
            - CREATE_TABLE
            - ALTER_TABLE
            - DROP_TABLE
            - SELECT
            - INSERT
            - DELETE
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="service-feature">', unsafe_allow_html=True)
            st.markdown("""
            #### 📋 Column Level
            - Column-specific SELECT
            - EXCLUDE on sensitive columns
            - Data masking
            - Conditional access
            
            #### 🔖 Tag-based Access
            - Dynamic permissions
            - Automated access control
            - Inheritance patterns
            - Bulk permission management
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Lake Formation Permissions Management
import boto3

lakeformation_client = boto3.client('lakeformation')

def grant_database_permissions():
    """Grant database-level permissions"""
    
    permission_config = {
        'Principal': {
            'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789012:role/DataAnalystRole'
        },
        'Resource': {
            'Database': {
                'Name': 'sales_data_lake'
            }
        },
        'Permissions': ['CREATE_TABLE', 'ALTER_DATABASE', 'DROP_TABLE'],
        'PermissionsWithGrantOption': ['CREATE_TABLE']
    }
    
    try:
        response = lakeformation_client.grant_permissions(**permission_config)
        print("Database permissions granted successfully")
        return response
    except Exception as e:
        print(f"Error granting database permissions: {e}")
        return None

def grant_table_permissions():
    """Grant table-level permissions with column filtering"""
    
    permission_config = {
        'Principal': {
            'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789012:role/DataAnalystRole'
        },
        'Resource': {
            'TableWithColumns': {
                'DatabaseName': 'sales_data_lake',
                'Name': 'customers',
                'ColumnNames': ['customer_id', 'first_name', 'last_name', 'email'],
                'ColumnWildcard': {
                    'ExcludedColumnNames': ['ssn', 'credit_card_number']
                }
            }
        },
        'Permissions': ['SELECT'],
        'PermissionsWithGrantOption': []
    }
    
    try:
        response = lakeformation_client.grant_permissions(**permission_config)
        print("Table permissions granted successfully")
        return response
    except Exception as e:
        print(f"Error granting table permissions: {e}")
        return None

def create_data_filter():
    """Create row-level security filter"""
    
    filter_config = {
        'Name': 'regional_sales_filter',
        'TableName': 'sales_orders',
        'DatabaseName': 'sales_data_lake',
        'RowFilter': {
            'FilterExpression': 'region = ${aws:PrincipalTag/Region}',
            'AllRowsWildcard': {}
        }
    }
    
    try:
        response = lakeformation_client.create_data_cells_filter(**filter_config)
        print("Data filter created successfully")
        return response
    except Exception as e:
        print(f"Error creating data filter: {e}")
        return None

def grant_tag_based_permissions():
    """Grant permissions using LF-Tags"""
    
    # First, create LF-Tags
    tag_config = {
        'TagKey': 'Department',
        'TagValues': ['Sales', 'Marketing', 'Finance', 'HR']
    }
    
    try:
        lakeformation_client.create_lf_tag(**tag_config)
        print("LF-Tag created successfully")
    except Exception as e:
        print(f"LF-Tag might already exist: {e}")
    
    # Grant permissions based on tags
    permission_config = {
        'Principal': {
            'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789012:role/SalesTeamRole'
        },
        'Resource': {
            'LFTag': {
                'TagKey': 'Department',
                'TagValues': ['Sales']
            }
        },
        'Permissions': ['SELECT', 'INSERT'],
        'PermissionsWithGrantOption': []
    }
    
    try:
        response = lakeformation_client.grant_permissions(**permission_config)
        print("Tag-based permissions granted successfully")
        return response
    except Exception as e:
        print(f"Error granting tag-based permissions: {e}")
        return None

def list_permissions():
    """List all permissions for a principal"""
    
    try:
        response = lakeformation_client.list_permissions(
            Principal={
                'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789012:role/DataAnalystRole'
            },
            ResourceType='DATABASE'
        )
        
        print("Permissions for DataAnalystRole:")
        for permission in response['PrincipalResourcePermissions']:
            resource = permission['Resource']
            permissions = permission['Permissions']
            print(f"Resource: {resource}, Permissions: {permissions}")
        
        return response
    except Exception as e:
        print(f"Error listing permissions: {e}")
        return None

def revoke_permissions():
    """Revoke permissions from a principal"""
    
    revoke_config = {
        'Principal': {
            'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789012:role/DataAnalystRole'
        },
        'Resource': {
            'Table': {
                'DatabaseName': 'sales_data_lake',
                'Name': 'customers'
            }
        },
        'Permissions': ['INSERT', 'DELETE']
    }
    
    try:
        response = lakeformation_client.revoke_permissions(**revoke_config)
        print("Permissions revoked successfully")
        return response
    except Exception as e:
        print(f"Error revoking permissions: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Grant various types of permissions
    grant_database_permissions()
    grant_table_permissions()
    create_data_filter()
    grant_tag_based_permissions()
    
    # List and manage permissions
    list_permissions()
    # revoke_permissions()  # Uncomment if needed
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown("### 📊 Analytics Service Integration")
        
        integration_services = {
            'Service': ['Amazon Athena', 'Amazon Redshift', 'Amazon EMR', 'Amazon QuickSight', 'Amazon SageMaker'],
            'Integration Type': ['Native', 'Redshift Spectrum', 'EMR Notebooks', 'Direct Connect', 'Feature Store'],
            'Use Case': [
                'SQL queries on data lake',
                'Data warehouse + data lake',
                'Big data processing',
                'Business intelligence',
                'Machine learning'
            ],
            'Benefits': [
                'Serverless, pay-per-query',
                'Familiar SQL interface',
                'Scalable compute',
                'Visual dashboards',
                'ML model training'
            ]
        }
        
        st.dataframe(pd.DataFrame(integration_services), use_container_width=True)
        
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Integration Examples with Analytics Services

# 1. Amazon Athena Integration
import boto3

athena_client = boto3.client('athena')

def query_data_lake_with_athena():
    """Query data lake using Amazon Athena"""
    
    query = """
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(total_amount) as total_spent
    FROM sales_data_lake.orders
    WHERE order_date >= DATE('2024-01-01')
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 100
    """
    
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': 'sales_data_lake'
            },
            ResultConfiguration={
                'OutputLocation': 's3://query-results-bucket/athena-results/'
            }
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Query started: {query_execution_id}")
        
        # Wait for query completion
        waiter = athena_client.get_waiter('query_succeeded')
        waiter.wait(QueryExecutionId=query_execution_id)
        
        # Get results
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        return results
        
    except Exception as e:
        print(f"Error executing Athena query: {e}")
        return None

# 2. Amazon Redshift Integration
def setup_redshift_spectrum():
    """Set up Redshift Spectrum to query data lake"""
    
    redshift_client = boto3.client('redshift')
    
    # Create external schema
    external_schema_sql = """
    CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_schema
    FROM DATA CATALOG
    DATABASE 'sales_data_lake'
    IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftSpectrumRole'
    CREATE EXTERNAL DATABASE IF NOT EXISTS;
    """
    
    # Create external table
    external_table_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS spectrum_schema.orders (
        order_id VARCHAR(50),
        customer_id VARCHAR(50),
        order_date DATE,
        total_amount DECIMAL(10,2),
        status VARCHAR(20)
    )
    STORED AS PARQUET
    LOCATION 's3://my-data-lake/sales/orders/'
    """
    
    print("Redshift Spectrum setup SQL:")
    print(external_schema_sql)
    print(external_table_sql)
    
    return external_schema_sql, external_table_sql

# 3. Amazon EMR Integration
def create_emr_cluster_for_data_lake():
    """Create EMR cluster for data lake processing"""
    
    emr_client = boto3.client('emr')
    
    cluster_config = {
        'Name': 'DataLakeProcessingCluster',
        'ReleaseLabel': 'emr-6.9.0',
        'Instances': {
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.large',
            'InstanceCount': 3,
            'Ec2KeyName': 'my-key-pair',
            'Ec2SubnetId': 'subnet-12345',
            'EmrManagedMasterSecurityGroup': 'sg-master',
            'EmrManagedSlaveSecurityGroup': 'sg-slave'
        },
        'Applications': [
            {'Name': 'Spark'},
            {'Name': 'Hadoop'},
            {'Name': 'Hive'},
            {'Name': 'Hue'}
        ],
        'ServiceRole': 'EMR_DefaultRole',
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'LogUri': 's3://emr-logs-bucket/logs/',
        'BootstrapActions': [
            {
                'Name': 'Install Additional Packages',
                'ScriptBootstrapAction': {
                    'Path': 's3://bootstrap-scripts/install-packages.sh'
                }
            }
        ],
        'Steps': [
            {
                'Name': 'Data Lake Processing Step',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--class', 'com.example.DataLakeProcessor',
                        's3://spark-apps/data-lake-processor.jar',
                        '--input', 's3://my-data-lake/raw/',
                        '--output', 's3://my-data-lake/processed/'
                    ]
                }
            }
        ]
    }
    
    try:
        response = emr_client.run_job_flow(**cluster_config)
        cluster_id = response['JobFlowId']
        print(f"EMR cluster created: {cluster_id}")
        return cluster_id
    except Exception as e:
        print(f"Error creating EMR cluster: {e}")
        return None

# 4. Amazon SageMaker Integration
def create_sagemaker_feature_store():
    """Create SageMaker Feature Store from data lake"""
    
    sagemaker_client = boto3.client('sagemaker')
    
    feature_group_config = {
        'FeatureGroupName': 'customer-features',
        'RecordIdentifierFeatureName': 'customer_id',
        'EventTimeFeatureName': 'event_time',
        'FeatureDefinitions': [
            {
                'FeatureName': 'customer_id',
                'FeatureType': 'String'
            },
            {
                'FeatureName': 'total_orders',
                'FeatureType': 'Integral'
            },
            {
                'FeatureName': 'total_spent',
                'FeatureType': 'Fractional'
            },
            {
                'FeatureName': 'last_order_date',
                'FeatureType': 'String'
            },
            {
                'FeatureName': 'event_time',
                'FeatureType': 'Fractional'
            }
        ],
        'OnlineStoreConfig': {
            'EnableOnlineStore': True
        },
        'OfflineStoreConfig': {
            'S3StorageConfig': {
                'S3Uri': 's3://sagemaker-feature-store/customer-features/',
                'KmsKeyId': 'arn:aws:kms:us-west-2:123456789012:key/key-id'
            },
            'DisableGlueTableCreation': False,
            'DataCatalogConfig': {
                'TableName': 'customer_features',
                'Catalog': 'AwsDataCatalog',
                'Database': 'sales_data_lake'
            }
        },
        'RoleArn': 'arn:aws:iam::123456789012:role/SageMakerFeatureStoreRole',
        'Description': 'Customer features derived from data lake'
    }
    
    try:
        response = sagemaker_client.create_feature_group(**feature_group_config)
        print(f"Feature group created: {response['FeatureGroupArn']}")
        return response
    except Exception as e:
        print(f"Error creating feature group: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Query with Athena
    athena_results = query_data_lake_with_athena()
    
    # Setup Redshift Spectrum
    schema_sql, table_sql = setup_redshift_spectrum()
    
    # Create EMR cluster
    cluster_id = create_emr_cluster_for_data_lake()
    
    # Create SageMaker Feature Store
    feature_group = create_sagemaker_feature_store()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def lake_formation_permissions_tab():
    """Content for Lake Formation Permissions tab"""
    st.markdown("## 🛡️ AWS Lake Formation Permissions")
    st.markdown("*Fine-grained access control for data lakes with centralized permission management*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Lake Formation Permission Model
    Lake Formation provides a unified security model that simplifies data lake access control:
    - **Centralized Management**: Single place to manage all data lake permissions
    - **Fine-grained Control**: Database, table, column, and row-level permissions
    - **Tag-based Access**: Dynamic permissions using resource tags
    - **Cross-service Integration**: Works with Athena, Redshift, EMR, and more
    - **Audit Trail**: Complete visibility into permission grants and usage
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Permission hierarchy diagram
    permission_hierarchy = """
    graph TB
        subgraph "Lake Formation Permissions"
            LF[Lake Formation<br/>Permission Engine]
            
            subgraph "Permission Types"
                DATA_LOC[Data Location<br/>Permissions]
                METADATA[Metadata<br/>Permissions]
                DATA_PERM[Data<br/>Permissions]
            end
            
            subgraph "Resource Hierarchy"
                CATALOG[Data Catalog]
                DATABASE[Database]
                TABLE[Table]
                COLUMN[Column]
                ROW[Row Filter]
            end
            
            subgraph "Principals"
                IAM_USER[IAM Users]
                IAM_ROLE[IAM Roles]
                FEDERATED[Federated Users]
                SERVICE[AWS Services]
            end
            
            subgraph "Access Methods"
                ATHENA[Amazon Athena]
                REDSHIFT[Amazon Redshift]
                EMR[Amazon EMR]
                GLUE[AWS Glue]
                QUICKSIGHT[Amazon QuickSight]
            end
        end
        
        LF --> DATA_LOC
        LF --> METADATA
        LF --> DATA_PERM
        
        DATA_LOC --> CATALOG
        METADATA --> DATABASE
        METADATA --> TABLE
        DATA_PERM --> COLUMN
        DATA_PERM --> ROW
        
        IAM_USER --> LF
        IAM_ROLE --> LF
        FEDERATED --> LF
        SERVICE --> LF
        
        LF --> ATHENA
        LF --> REDSHIFT
        LF --> EMR
        LF --> GLUE
        LF --> QUICKSIGHT
        
        style LF fill:#FF9900,stroke:#232F3E,color:#fff
        style DATA_PERM fill:#3FB34F,stroke:#232F3E,color:#fff
        style COLUMN fill:#4B9EDB,stroke:#232F3E,color:#fff
        style ROW fill:#232F3E,stroke:#FF9900,color:#fff
    """
    
    st.markdown("#### 🏗️ Permission Hierarchy & Flow")
    common.mermaid(permission_hierarchy, height=600)
    
    # Interactive permission manager
    st.markdown("#### 🔧 Interactive Permission Manager")
    
    # Permission configuration
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("##### 👥 Principal")
        principal_type = st.selectbox("Principal Type", [
            "IAM Role", "IAM User", "Federated User", "AWS Service"
        ])
        
        if principal_type == "IAM Role":
            principal_name = st.text_input("Role Name", value="DataAnalystRole")
        elif principal_type == "IAM User":
            principal_name = st.text_input("User Name", value="john.doe")
        elif principal_type == "AWS Service":
            principal_name = st.selectbox("Service", ["glue.amazonaws.com", "athena.amazonaws.com"])
    
    with col2:
        st.markdown("##### 🗄️ Resource")
        resource_type = st.selectbox("Resource Type", [
            "Database", "Table", "Table with Columns", "LF-Tag"
        ])
        
        database_name = st.text_input("Database Name", value="sales_data_lake")
        
        if resource_type in ["Table", "Table with Columns"]:
            table_name = st.text_input("Table Name", value="customers")
            
        if resource_type == "Table with Columns":
            column_access = st.selectbox("Column Access", [
                "All Columns", "Specific Columns", "Exclude Columns"
            ])
            
            if column_access == "Specific Columns":
                columns = st.multiselect("Columns", [
                    "customer_id", "first_name", "last_name", "email", 
                    "phone", "address", "city", "state", "zip_code"
                ], default=["customer_id", "first_name", "last_name", "email"])
            elif column_access == "Exclude Columns":
                excluded_columns = st.multiselect("Excluded Columns", [
                    "ssn", "credit_card_number", "bank_account", "salary"
                ], default=["ssn", "credit_card_number"])
    
    with col3:
        st.markdown("##### 🔑 Permissions")
        permission_level = st.selectbox("Permission Level", [
            "Read Only", "Read/Write", "Admin", "Custom"
        ])
        
        if permission_level == "Custom":
            custom_permissions = st.multiselect("Custom Permissions", [
                "SELECT", "INSERT", "DELETE", "ALTER", "DROP", 
                "CREATE_TABLE", "DESCRIBE"
            ])
        
        grant_option = st.checkbox("Grant Option", help="Allow principal to grant permissions to others")
    
    # Advanced options
    st.markdown("##### ⚙️ Advanced Options")
    col1, col2 = st.columns(2)
    
    with col1:
        row_filter = st.text_input("Row Filter Expression", 
                                 placeholder="region = 'US' AND status = 'active'")
        
    with col2:
        condition = st.text_input("Condition", 
                                placeholder="aws:username = 'specificuser'")
    
    # Create permission
    if st.button("🛡️ Grant Permission", type="primary"):
        permission_summary = {
            "principal": f"{principal_type}: {principal_name}",
            "resource": f"{resource_type}: {database_name}" + (f".{table_name}" if resource_type != "Database" else ""),
            "permissions": permission_level,
            "row_filter": row_filter if row_filter else "None",
            "condition": condition if condition else "None",
            "grant_option": grant_option,
            "created_at": datetime.now()
        }
        
        st.success("✅ Permission granted successfully!")
        st.json(permission_summary)
    
    # Permission patterns
    st.markdown("#### 🎯 Common Permission Patterns")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="service-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 👥 Team-based Access
        **Data Analysts Team**
        - SELECT on all tables
        - Column-level restrictions
        - No sensitive data access
        
        **Data Scientists Team**
        - SELECT on curated datasets
        - CREATE_TABLE for experiments
        - Time-limited access
        
        **Business Users**
        - SELECT on business views
        - Pre-aggregated data only
        - Regional data filtering
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="service-feature">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔐 Security Patterns
        **Principle of Least Privilege**
        - Minimal required permissions
        - Regular permission audits
        - Temporary elevated access
        
        **Data Classification**
        - Public data: Open access
        - Internal data: Employee access
        - Confidential: Restricted access
        - Secret: Admin access only
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Permission Management Code Examples")
    
    tab1, tab2, tab3 = st.tabs(["🔑 Grant Permissions", "🏷️ Tag-based Access", "📊 Audit & Monitor"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
import boto3
import json
from datetime import datetime, timedelta

class LakeFormationPermissionManager:
    def __init__(self, region='us-west-2'):
        self.lf_client = boto3.client('lakeformation', region_name=region)
        self.iam_client = boto3.client('iam', region_name=region)
    
    def grant_database_permissions(self, principal_arn, database_name, permissions):
        """Grant database-level permissions"""
        
        try:
            response = self.lf_client.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': principal_arn
                },
                Resource={
                    'Database': {
                        'Name': database_name
                    }
                },
                Permissions=permissions,
                PermissionsWithGrantOption=[]
            )
            
            print(f"Database permissions granted to {principal_arn}")
            return response
        except Exception as e:
            print(f"Error granting database permissions: {e}")
            return None
    
    def grant_table_permissions(self, principal_arn, database_name, table_name, permissions):
        """Grant table-level permissions"""
        
        try:
            response = self.lf_client.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': principal_arn
                },
                Resource={
                    'Table': {
                        'DatabaseName': database_name,
                        'Name': table_name
                    }
                },
                Permissions=permissions,
                PermissionsWithGrantOption=[]
            )
            
            print(f"Table permissions granted to {principal_arn}")
            return response
        except Exception as e:
            print(f"Error granting table permissions: {e}")
            return None
    
    def grant_column_permissions(self, principal_arn, database_name, table_name, 
                               included_columns=None, excluded_columns=None, permissions=['SELECT']):
        """Grant column-level permissions"""
        
        resource_config = {
            'TableWithColumns': {
                'DatabaseName': database_name,
                'Name': table_name
            }
        }
        
        if included_columns:
            resource_config['TableWithColumns']['ColumnNames'] = included_columns
        elif excluded_columns:
            resource_config['TableWithColumns']['ColumnWildcard'] = {
                'ExcludedColumnNames': excluded_columns
            }
        else:
            resource_config['TableWithColumns']['ColumnWildcard'] = {}
        
        try:
            response = self.lf_client.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': principal_arn
                },
                Resource=resource_config,
                Permissions=permissions,
                PermissionsWithGrantOption=[]
            )
            
            print(f"Column permissions granted to {principal_arn}")
            return response
        except Exception as e:
            print(f"Error granting column permissions: {e}")
            return None
    
    def create_row_filter(self, database_name, table_name, filter_name, filter_expression):
        """Create row-level security filter"""
        
        try:
            response = self.lf_client.create_data_cells_filter(
                TableName=table_name,
                DatabaseName=database_name,
                Name=filter_name,
                RowFilter={
                    'FilterExpression': filter_expression
                }
            )
            
            print(f"Row filter created: {filter_name}")
            return response
        except Exception as e:
            print(f"Error creating row filter: {e}")
            return None
    
    def grant_conditional_permissions(self, principal_arn, database_name, table_name, 
                                    permissions, condition_expression):
        """Grant permissions with conditions"""
        
        # Note: Conditions are typically handled through IAM policies
        # This is an example of how to structure the permission grant
        
        try:
            response = self.lf_client.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': principal_arn
                },
                Resource={
                    'Table': {
                        'DatabaseName': database_name,
                        'Name': table_name
                    }
                },
                Permissions=permissions,
                PermissionsWithGrantOption=[]
            )
            
            # Log the condition for reference
            print(f"Conditional permissions granted to {principal_arn}")
            print(f"Condition: {condition_expression}")
            return response
        except Exception as e:
            print(f"Error granting conditional permissions: {e}")
            return None

# Example usage patterns
def setup_data_analyst_permissions():
    """Set up permissions for data analyst role"""
    
    perm_manager = LakeFormationPermissionManager()
    analyst_role_arn = "arn:aws:iam::123456789012:role/DataAnalystRole"
    
    # Database access
    perm_manager.grant_database_permissions(
        principal_arn=analyst_role_arn,
        database_name="sales_data_lake",
        permissions=["DESCRIBE"]
    )
    
    # Table access with column restrictions
    perm_manager.grant_column_permissions(
        principal_arn=analyst_role_arn,
        database_name="sales_data_lake",
        table_name="customers",
        excluded_columns=["ssn", "credit_card_number", "bank_account"],
        permissions=["SELECT"]
    )
    
    # Full access to aggregated tables
    perm_manager.grant_table_permissions(
        principal_arn=analyst_role_arn,
        database_name="sales_data_lake",
        table_name="monthly_sales_summary",
        permissions=["SELECT"]
    )

def setup_data_scientist_permissions():
    """Set up permissions for data scientist role"""
    
    perm_manager = LakeFormationPermissionManager()
    scientist_role_arn = "arn:aws:iam::123456789012:role/DataScientistRole"
    
    # Broader database access
    perm_manager.grant_database_permissions(
        principal_arn=scientist_role_arn,
        database_name="sales_data_lake",
        permissions=["CREATE_TABLE", "DESCRIBE"]
    )
    
    # Access to curated datasets
    perm_manager.grant_table_permissions(
        principal_arn=scientist_role_arn,
        database_name="sales_data_lake",
        table_name="customer_features",
        permissions=["SELECT", "INSERT"]
    )
    
    # Create experimental tables
    perm_manager.grant_database_permissions(
        principal_arn=scientist_role_arn,
        database_name="experiments",
        permissions=["CREATE_TABLE", "DROP_TABLE", "ALTER_TABLE"]
    )

def setup_regional_access():
    """Set up region-based access control"""
    
    perm_manager = LakeFormationPermissionManager()
    
    # Create region-based row filters
    perm_manager.create_row_filter(
        database_name="sales_data_lake",
        table_name="orders",
        filter_name="us_orders_filter",
        filter_expression="region = 'US'"
    )
    
    perm_manager.create_row_filter(
        database_name="sales_data_lake",
        table_name="orders",
        filter_name="eu_orders_filter",
        filter_expression="region = 'EU'"
    )
    
    # Grant permissions to regional teams
    us_team_role = "arn:aws:iam::123456789012:role/USTeamRole"
    eu_team_role = "arn:aws:iam::123456789012:role/EUTeamRole"
    
    perm_manager.grant_table_permissions(
        principal_arn=us_team_role,
        database_name="sales_data_lake",
        table_name="orders",
        permissions=["SELECT"]
    )
    
    perm_manager.grant_table_permissions(
        principal_arn=eu_team_role,
        database_name="sales_data_lake",
        table_name="orders",
        permissions=["SELECT"]
    )

def setup_time_based_access():
    """Set up time-based access control"""
    
    perm_manager = LakeFormationPermissionManager()
    
    # Note: Time-based access is typically handled through IAM policies
    # This example shows how to structure the Lake Formation permissions
    
    contractor_role_arn = "arn:aws:iam::123456789012:role/ContractorRole"
    
    # Grant temporary access (managed through IAM policy conditions)
    perm_manager.grant_conditional_permissions(
        principal_arn=contractor_role_arn,
        database_name="sales_data_lake",
        table_name="public_datasets",
        permissions=["SELECT"],
        condition_expression="DateLessThan(aws:CurrentTime, '2024-12-31T23:59:59Z')"
    )

# Example execution
if __name__ == "__main__":
    print("Setting up Lake Formation permissions...")
    
    # Setup different role permissions
    setup_data_analyst_permissions()
    setup_data_scientist_permissions()
    setup_regional_access()
    setup_time_based_access()
    
    print("Permission setup completed!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Tag-based Access Control with Lake Formation
import boto3
import json

class LakeFormationTagManager:
    def __init__(self, region='us-west-2'):
        self.lf_client = boto3.client('lakeformation', region_name=region)
    
    def create_lf_tags(self):
        """Create Lake Formation tags for access control"""
        
        tags_to_create = [
            {
                'TagKey': 'Department',
                'TagValues': ['Sales', 'Marketing', 'Finance', 'HR', 'IT']
            },
            {
                'TagKey': 'DataClassification',
                'TagValues': ['Public', 'Internal', 'Confidential', 'Secret']
            },
            {
                'TagKey': 'Region',
                'TagValues': ['US', 'EU', 'APAC', 'Global']
            },
            {
                'TagKey': 'Environment',
                'TagValues': ['Dev', 'Test', 'Prod']
            }
        ]
        
        for tag_config in tags_to_create:
            try:
                response = self.lf_client.create_lf_tag(**tag_config)
                print(f"Created LF-Tag: {tag_config['TagKey']}")
            except Exception as e:
                if "AlreadyExistsException" in str(e):
                    print(f"LF-Tag already exists: {tag_config['TagKey']}")
                else:
                    print(f"Error creating LF-Tag {tag_config['TagKey']}: {e}")
    
    def assign_tags_to_resource(self, database_name, table_name=None, tags=None):
        """Assign LF-Tags to database or table"""
        
        if table_name:
            resource = {
                'Table': {
                    'DatabaseName': database_name,
                    'Name': table_name
                }
            }
        else:
            resource = {
                'Database': {
                    'Name': database_name
                }
            }
        
        try:
            response = self.lf_client.add_lf_tags_to_resource(
                Resource=resource,
                LFTags=tags or []
            )
            
            resource_name = f"{database_name}.{table_name}" if table_name else database_name
            print(f"Tags assigned to {resource_name}")
            return response
        except Exception as e:
            print(f"Error assigning tags: {e}")
            return None
    
    def grant_tag_based_permissions(self, principal_arn, tag_key, tag_values, permissions):
        """Grant permissions based on LF-Tags"""
        
        try:
            response = self.lf_client.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': principal_arn
                },
                Resource={
                    'LFTag': {
                        'TagKey': tag_key,
                        'TagValues': tag_values
                    }
                },
                Permissions=permissions,
                PermissionsWithGrantOption=[]
            )
            
            print(f"Tag-based permissions granted to {principal_arn}")
            return response
        except Exception as e:
            print(f"Error granting tag-based permissions: {e}")
            return None
    
    def create_tag_expression_permissions(self, principal_arn, tag_expression, permissions):
        """Grant permissions based on tag expressions"""
        
        try:
            response = self.lf_client.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': principal_arn
                },
                Resource={
                    'LFTagPolicy': {
                        'ResourceType': 'TABLE',
                        'Expression': tag_expression
                    }
                },
                Permissions=permissions,
                PermissionsWithGrantOption=[]
            )
            
            print(f"Tag expression permissions granted to {principal_arn}")
            return response
        except Exception as e:
            print(f"Error granting tag expression permissions: {e}")
            return None

# Example: Comprehensive Tag-based Setup
def setup_comprehensive_tag_based_access():
    """Set up comprehensive tag-based access control"""
    
    tag_manager = LakeFormationTagManager()
    
    # Step 1: Create LF-Tags
    tag_manager.create_lf_tags()
    
    # Step 2: Tag resources
    # Tag sales database
    tag_manager.assign_tags_to_resource(
        database_name="sales_data_lake",
        tags=[
            {'TagKey': 'Department', 'TagValues': ['Sales']},
            {'TagKey': 'DataClassification', 'TagValues': ['Internal']},
            {'TagKey': 'Environment', 'TagValues': ['Prod']}
        ]
    )
    
    # Tag customer table with sensitive data
    tag_manager.assign_tags_to_resource(
        database_name="sales_data_lake",
        table_name="customers",
        tags=[
            {'TagKey': 'Department', 'TagValues': ['Sales']},
            {'TagKey': 'DataClassification', 'TagValues': ['Confidential']},
            {'TagKey': 'Region', 'TagValues': ['Global']}
        ]
    )
    
    # Tag financial data
    tag_manager.assign_tags_to_resource(
        database_name="sales_data_lake",
        table_name="financial_transactions",
        tags=[
            {'TagKey': 'Department', 'TagValues': ['Finance']},
            {'TagKey': 'DataClassification', 'TagValues': ['Secret']},
            {'TagKey': 'Region', 'TagValues': ['Global']}
        ]
    )
    
    # Step 3: Grant tag-based permissions
    
    # Sales team - access to sales department data
    tag_manager.grant_tag_based_permissions(
        principal_arn="arn:aws:iam::123456789012:role/SalesTeamRole",
        tag_key="Department",
        tag_values=["Sales"],
        permissions=["SELECT", "INSERT"]
    )
    
    # Finance team - access to finance department data
    tag_manager.grant_tag_based_permissions(
        principal_arn="arn:aws:iam::123456789012:role/FinanceTeamRole",
        tag_key="Department",
        tag_values=["Finance"],
        permissions=["SELECT", "INSERT", "DELETE"]
    )
    
    # Data classification based access
    tag_manager.grant_tag_based_permissions(
        principal_arn="arn:aws:iam::123456789012:role/DataAnalystRole",
        tag_key="DataClassification",
        tag_values=["Public", "Internal"],
        permissions=["SELECT"]
    )
    
    # Step 4: Complex tag expressions
    
    # Grant access to US sales data only
    us_sales_expression = [
        {
            'TagKey': 'Department',
            'TagValues': ['Sales']
        },
        {
            'TagKey': 'Region',
            'TagValues': ['US']
        }
    ]
    
    tag_manager.create_tag_expression_permissions(
        principal_arn="arn:aws:iam::123456789012:role/USSalesRole",
        tag_expression=us_sales_expression,
        permissions=["SELECT", "INSERT"]
    )

def setup_dynamic_permissions():
    """Set up dynamic permissions that change based on user attributes"""
    
    tag_manager = LakeFormationTagManager()
    
    # Create role-based permissions that match user's department
    department_roles = [
        ("arn:aws:iam::123456789012:role/SalesManagerRole", "Sales"),
        ("arn:aws:iam::123456789012:role/MarketingManagerRole", "Marketing"),
        ("arn:aws:iam::123456789012:role/FinanceManagerRole", "Finance"),
        ("arn:aws:iam::123456789012:role/HRManagerRole", "HR")
    ]
    
    for role_arn, department in department_roles:
        # Managers get full access to their department's data
        tag_manager.grant_tag_based_permissions(
            principal_arn=role_arn,
            tag_key="Department",
            tag_values=[department],
            permissions=["SELECT", "INSERT", "DELETE", "ALTER"]
        )
        
        # Managers also get read access to public data
        tag_manager.grant_tag_based_permissions(
            principal_arn=role_arn,
            tag_key="DataClassification",
            tag_values=["Public"],
            permissions=["SELECT"]
        )

def audit_tag_based_permissions():
    """Audit tag-based permissions"""
    
    lf_client = boto3.client('lakeformation')
    
    try:
        # List all LF-Tags
        response = lf_client.list_lf_tags()
        print("Existing LF-Tags:")
        for tag in response['LFTags']:
            print(f"  {tag['TagKey']}: {tag['TagValues']}")
        
        # List permissions for each tag
        for tag in response['LFTags']:
            print(f"\nPermissions for tag {tag['TagKey']}:")
            
            perm_response = lf_client.list_permissions(
                ResourceType='LF_TAG',
                Resource={
                    'LFTag': {
                        'TagKey': tag['TagKey'],
                        'TagValues': tag['TagValues']
                    }
                }
            )
            
            for permission in perm_response['PrincipalResourcePermissions']:
                principal = permission['Principal']['DataLakePrincipalIdentifier']
                perms = permission['Permissions']
                print(f"  {principal}: {perms}")
        
    except Exception as e:
        print(f"Error auditing permissions: {e}")

def cleanup_tag_permissions():
    """Clean up tag-based permissions"""
    
    tag_manager = LakeFormationTagManager()
    
    # List of principals to clean up
    principals_to_cleanup = [
        "arn:aws:iam::123456789012:role/TempRole",
        "arn:aws:iam::123456789012:role/ContractorRole"
    ]
    
    for principal in principals_to_cleanup:
        try:
            # List permissions for this principal
            response = tag_manager.lf_client.list_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': principal
                }
            )
            
            # Revoke each permission
            for permission in response['PrincipalResourcePermissions']:
                tag_manager.lf_client.revoke_permissions(
                    Principal=permission['Principal'],
                    Resource=permission['Resource'],
                    Permissions=permission['Permissions']
                )
                
                print(f"Revoked permissions for {principal}")
                
        except Exception as e:
            print(f"Error cleaning up permissions for {principal}: {e}")

# Example execution
if __name__ == "__main__":
    print("Setting up tag-based access control...")
    
    # Complete setup
    setup_comprehensive_tag_based_access()
    setup_dynamic_permissions()
    
    # Audit current state
    print("\nAuditing current permissions...")
    audit_tag_based_permissions()
    
    # Cleanup if needed
    # cleanup_tag_permissions()
    
    print("\nTag-based access control setup completed!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Lake Formation Audit and Monitoring
import boto3
import json
from datetime import datetime, timedelta
import pandas as pd

class LakeFormationAuditor:
    def __init__(self, region='us-west-2'):
        self.lf_client = boto3.client('lakeformation', region_name=region)
        self.cloudtrail_client = boto3.client('cloudtrail', region_name=region)
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region)
    
    def audit_all_permissions(self):
        """Comprehensive audit of all Lake Formation permissions"""
        
        audit_report = {
            'database_permissions': [],
            'table_permissions': [],
            'column_permissions': [],
            'tag_permissions': [],
            'summary': {}
        }
        
        try:
            # Get all permissions
            response = self.lf_client.list_permissions()
            
            for permission in response['PrincipalResourcePermissions']:
                principal = permission['Principal']['DataLakePrincipalIdentifier']
                resource = permission['Resource']
                perms = permission['Permissions']
                grantable = permission.get('PermissionsWithGrantOption', [])
                
                # Categorize by resource type
                if 'Database' in resource:
                    audit_report['database_permissions'].append({
                        'principal': principal,
                        'database': resource['Database']['Name'],
                        'permissions': perms,
                        'grantable': grantable
                    })
                elif 'Table' in resource:
                    audit_report['table_permissions'].append({
                        'principal': principal,
                        'database': resource['Table']['DatabaseName'],
                        'table': resource['Table']['Name'],
                        'permissions': perms,
                        'grantable': grantable
                    })
                elif 'TableWithColumns' in resource:
                    audit_report['column_permissions'].append({
                        'principal': principal,
                        'database': resource['TableWithColumns']['DatabaseName'],
                        'table': resource['TableWithColumns']['Name'],
                        'columns': resource['TableWithColumns'].get('ColumnNames', 'ALL'),
                        'permissions': perms,
                        'grantable': grantable
                    })
                elif 'LFTag' in resource:
                    audit_report['tag_permissions'].append({
                        'principal': principal,
                        'tag_key': resource['LFTag']['TagKey'],
                        'tag_values': resource['LFTag']['TagValues'],
                        'permissions': perms,
                        'grantable': grantable
                    })
            
            # Generate summary
            audit_report['summary'] = {
                'total_permissions': len(response['PrincipalResourcePermissions']),
                'database_permissions': len(audit_report['database_permissions']),
                'table_permissions': len(audit_report['table_permissions']),
                'column_permissions': len(audit_report['column_permissions']),
                'tag_permissions': len(audit_report['tag_permissions']),
                'unique_principals': len(set([p['Principal']['DataLakePrincipalIdentifier'] 
                                            for p in response['PrincipalResourcePermissions']]))
            }
            
            return audit_report
            
        except Exception as e:
            print(f"Error during audit: {e}")
            return None
    
    def generate_permission_report(self, audit_report):
        """Generate a formatted permission report"""
        
        print("="*80)
        print("LAKE FORMATION PERMISSIONS AUDIT REPORT")
        print("="*80)
        print(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Summary
        summary = audit_report['summary']
        print("SUMMARY:")
        print(f"  Total Permissions: {summary['total_permissions']}")
        print(f"  Unique Principals: {summary['unique_principals']}")
        print(f"  Database Permissions: {summary['database_permissions']}")
        print(f"  Table Permissions: {summary['table_permissions']}")
        print(f"  Column Permissions: {summary['column_permissions']}")
        print(f"  Tag-based Permissions: {summary['tag_permissions']}")
        print()
        
        # Database permissions
        if audit_report['database_permissions']:
            print("DATABASE PERMISSIONS:")
            for perm in audit_report['database_permissions']:
                print(f"  {perm['principal']}")
                print(f"    Database: {perm['database']}")
                print(f"    Permissions: {', '.join(perm['permissions'])}")
                if perm['grantable']:
                    print(f"    Grantable: {', '.join(perm['grantable'])}")
                print()
        
        # Table permissions
        if audit_report['table_permissions']:
            print("TABLE PERMISSIONS:")
            for perm in audit_report['table_permissions']:
                print(f"  {perm['principal']}")
                print(f"    Table: {perm['database']}.{perm['table']}")
                print(f"    Permissions: {', '.join(perm['permissions'])}")
                print()
        
        # Column permissions
        if audit_report['column_permissions']:
            print("COLUMN-LEVEL PERMISSIONS:")
            for perm in audit_report['column_permissions']:
                print(f"  {perm['principal']}")
                print(f"    Table: {perm['database']}.{perm['table']}")
                print(f"    Columns: {perm['columns']}")
                print(f"    Permissions: {', '.join(perm['permissions'])}")
                print()
        
        # Tag permissions
        if audit_report['tag_permissions']:
            print("TAG-BASED PERMISSIONS:")
            for perm in audit_report['tag_permissions']:
                print(f"  {perm['principal']}")
                print(f"    Tag: {perm['tag_key']} = {perm['tag_values']}")
                print(f"    Permissions: {', '.join(perm['permissions'])}")
                print()
    
    def monitor_access_patterns(self, days_back=7):
        """Monitor data access patterns using CloudTrail"""
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days_back)
        
        access_patterns = {
            'total_queries': 0,
            'users': {},
            'databases': {},
            'tables': {},
            'query_types': {},
            'errors': []
        }
        
        try:
            # Query CloudTrail for Lake Formation events
            response = self.cloudtrail_client.lookup_events(
                LookupAttributes=[
                    {
                        'AttributeKey': 'EventSource',
                        'AttributeValue': 'lakeformation.amazonaws.com'
                    }
                ],
                StartTime=start_time,
                EndTime=end_time
            )
            
            for event in response['Events']:
                event_name = event['EventName']
                username = event.get('Username', 'Unknown')
                
                # Parse event details
                if event_name in ['GetDataAccess', 'GetTableObjects']:
                    access_patterns['total_queries'] += 1
                    
                    # Count by user
                    if username not in access_patterns['users']:
                        access_patterns['users'][username] = 0
                    access_patterns['users'][username] += 1
                    
                    # Count by query type
                    if event_name not in access_patterns['query_types']:
                        access_patterns['query_types'][event_name] = 0
                    access_patterns['query_types'][event_name] += 1
                
                # Track errors
                if event.get('ErrorCode'):
                    access_patterns['errors'].append({
                        'timestamp': event['EventTime'],
                        'user': username,
                        'error': event.get('ErrorCode'),
                        'message': event.get('ErrorMessage', 'Unknown error')
                    })
            
            return access_patterns
            
        except Exception as e:
            print(f"Error monitoring access patterns: {e}")
            return None
    
    def generate_access_report(self, access_patterns):
        """Generate access pattern report"""
        
        print("="*80)
        print("DATA ACCESS PATTERNS REPORT")
        print("="*80)
        print(f"Period: Last 7 days")
        print(f"Total Queries: {access_patterns['total_queries']}")
        print()
        
        # Top users
        if access_patterns['users']:
            print("TOP USERS BY QUERY COUNT:")
            sorted_users = sorted(access_patterns['users'].items(), 
                                key=lambda x: x[1], reverse=True)
            for user, count in sorted_users[:10]:
                print(f"  {user}: {count} queries")
            print()
        
        # Query types
        if access_patterns['query_types']:
            print("QUERY TYPES:")
            for query_type, count in access_patterns['query_types'].items():
                print(f"  {query_type}: {count}")
            print()
        
        # Errors
        if access_patterns['errors']:
            print("RECENT ERRORS:")
            for error in access_patterns['errors'][-10:]:  # Last 10 errors
                print(f"  {error['timestamp']}: {error['user']} - {error['error']}")
            print()
    
    def check_compliance(self, audit_report):
        """Check compliance against security policies"""
        
        compliance_issues = []
        
        # Check for overly broad permissions
        for perm in audit_report['database_permissions']:
            if 'ALL' in perm['permissions']:
                compliance_issues.append({
                    'severity': 'HIGH',
                    'issue': 'Overly broad database permissions',
                    'details': f"{perm['principal']} has ALL permissions on {perm['database']}"
                })
        
        # Check for users with direct access (should use roles)
        for perm in audit_report['database_permissions'] + audit_report['table_permissions']:
            if ':user/' in perm['principal']:
                compliance_issues.append({
                    'severity': 'MEDIUM',
                    'issue': 'Direct user access',
                    'details': f"User {perm['principal']} has direct access, should use roles"
                })
        
        # Check for permissions without grant option controls
        for perm in audit_report['database_permissions']:
            if perm['grantable'] and 'ALL' in perm['grantable']:
                compliance_issues.append({
                    'severity': 'HIGH',
                    'issue': 'Unrestricted grant permissions',
                    'details': f"{perm['principal']} can grant ALL permissions"
                })
        
        return compliance_issues
    
    def generate_compliance_report(self, compliance_issues):
        """Generate compliance report"""
        
        print("="*80)
        print("COMPLIANCE REPORT")
        print("="*80)
        
        if not compliance_issues:
            print("✅ No compliance issues found")
            return
        
        # Group by severity
        high_issues = [i for i in compliance_issues if i['severity'] == 'HIGH']
        medium_issues = [i for i in compliance_issues if i['severity'] == 'MEDIUM']
        low_issues = [i for i in compliance_issues if i['severity'] == 'LOW']
        
        print(f"Total Issues: {len(compliance_issues)}")
        print(f"High Priority: {len(high_issues)}")
        print(f"Medium Priority: {len(medium_issues)}")
        print(f"Low Priority: {len(low_issues)}")
        print()
        
        # High priority issues
        if high_issues:
            print("🚨 HIGH PRIORITY ISSUES:")
            for issue in high_issues:
                print(f"  Issue: {issue['issue']}")
                print(f"  Details: {issue['details']}")
                print()
        
        # Medium priority issues
        if medium_issues:
            print("⚠️  MEDIUM PRIORITY ISSUES:")
            for issue in medium_issues:
                print(f"  Issue: {issue['issue']}")
                print(f"  Details: {issue['details']}")
                print()
    
    def create_monitoring_dashboard(self):
        """Create CloudWatch dashboard for Lake Formation monitoring"""
        
        dashboard_config = {
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["AWS/LakeFormation", "PermissionGrants"],
                            ["AWS/LakeFormation", "PermissionRevocations"],
                            ["AWS/LakeFormation", "DataAccessRequests"]
                        ],
                        "period": 300,
                        "stat": "Sum",
                        "region": "us-west-2",
                        "title": "Lake Formation Activity"
                    }
                },
                {
                    "type": "log",
                    "properties": {
                        "query": "SOURCE '/aws/lakeformation/audit'\n| fields @timestamp, eventName, sourceIPAddress, userIdentity.principalId\n| filter eventName like /Grant|Revoke/\n| stats count() by eventName",
                        "region": "us-west-2",
                        "title": "Permission Changes",
                        "view": "table"
                    }
                }
            ]
        }
        
        try:
            response = self.cloudwatch_client.put_dashboard(
                DashboardName='LakeFormationMonitoring',
                DashboardBody=json.dumps(dashboard_config)
            )
            print("Monitoring dashboard created successfully")
            return response
        except Exception as e:
            print(f"Error creating dashboard: {e}")
            return None

# Example usage
def run_comprehensive_audit():
    """Run comprehensive audit and monitoring"""
    
    auditor = LakeFormationAuditor()
    
    # Audit permissions
    print("Running permissions audit...")
    audit_report = auditor.audit_all_permissions()
    
    if audit_report:
        auditor.generate_permission_report(audit_report)
        
        # Check compliance
        compliance_issues = auditor.check_compliance(audit_report)
        auditor.generate_compliance_report(compliance_issues)
    
    # Monitor access patterns
    print("Analyzing access patterns...")
    access_patterns = auditor.monitor_access_patterns()
    
    if access_patterns:
        auditor.generate_access_report(access_patterns)
    
    # Create dashboard
    print("Creating monitoring dashboard...")
    auditor.create_monitoring_dashboard()

if __name__ == "__main__":
    run_comprehensive_audit()
        ''', language='python')
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
    # 🔐 AWS Data Security & Governance
    <div class='info-box'>
    Master AWS security services and data governance techniques for building secure, compliant data platforms. Learn credential management, access control, and data lake security best practices.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🔐 AWS Secrets Manager",
        "📊 AWS Systems Manager Parameter Store",
        "🎭 The Importance of IAM Roles", 
        "🏗️ Building a Data Lake Manually",
        "🚀 AWS Lake Formation",
        "🛡️ AWS Lake Formation Permissions"
    ])
    
    with tab1:
        secrets_manager_tab()
    
    with tab2:
        parameter_store_tab()
    
    with tab3:
        iam_roles_tab()
    
    with tab4:
        manual_data_lake_tab()
    
    with tab5:
        lake_formation_tab()
    
    with tab6:
        lake_formation_permissions_tab()
    
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
