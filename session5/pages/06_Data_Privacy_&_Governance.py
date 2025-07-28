
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
import random

# Page configuration
st.set_page_config(
    page_title="AWS Data Security & Governance",
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
    'warning': '#FFB347',
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
        
        .feature-card {{
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
        st.session_state.config_rules_created = []
        st.session_state.macie_findings = []
        st.session_state.redshift_shares = []

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 📊 AWS Config - Track resource inventory & configuration changes
            - 🔍 Amazon Macie - Discover and protect sensitive data using ML
            - 🤝 Data Sharing in Amazon Redshift - Secure live data sharing
            
            **Learning Objectives:**
            - Master AWS Config for compliance and governance
            - Learn to identify PII and sensitive data with Macie
            - Implement secure data sharing in Redshift
            - Understand data security and governance best practices
            """)

def create_config_architecture():
    """Create mermaid diagram for AWS Config architecture"""
    return """
    graph TB
        subgraph "AWS Account"
            RESOURCES[AWS Resources<br/>🖥️ EC2<br/>🗃️ S3<br/>🔑 IAM<br/>🌐 VPC]
            CONFIG[AWS Config<br/>📊 Configuration<br/>Recording]
            RULES[Config Rules<br/>✅ Compliance<br/>Evaluation]
        end
        
        subgraph "Storage & Analytics"
            S3[S3 Bucket<br/>📦 Configuration<br/>History]
            SNS[SNS Topic<br/>📢 Notifications]
            ATHENA[Amazon Athena<br/>📈 Query Config<br/>Data]
        end
        
        subgraph "Monitoring"
            CW[CloudWatch<br/>📊 Metrics]
            DASHBOARD[Config<br/>Dashboard]
        end
        
        RESOURCES --> CONFIG
        CONFIG --> RULES
        CONFIG --> S3
        RULES --> SNS
        S3 --> ATHENA
        CONFIG --> CW
        CW --> DASHBOARD
        RULES --> DASHBOARD
        
        style CONFIG fill:#FF9900,stroke:#232F3E,color:#fff
        style RULES fill:#4B9EDB,stroke:#232F3E,color:#fff
        style S3 fill:#3FB34F,stroke:#232F3E,color:#fff
        style DASHBOARD fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_macie_architecture():
    """Create mermaid diagram for Amazon Macie architecture"""
    return """
    graph TB
        subgraph "Data Sources"
            S3_BUCKETS[S3 Buckets<br/>📁 Documents<br/>📊 Databases<br/>📋 Logs]
        end
        
        subgraph "Amazon Macie"
            ML_ENGINE[Machine Learning<br/>Engine<br/>🤖 Pattern Detection]
            CLASSIFIERS[Built-in<br/>Classifiers<br/>🔍 PII Detection]
            CUSTOM_TYPES[Custom Data<br/>Types<br/>⚙️ Business Rules]
        end
        
        subgraph "Analysis & Classification"
            DISCOVERY[Data Discovery<br/>🔍 Content Analysis]
            CLASSIFICATION[Data Classification<br/>🏷️ Sensitivity Levels]
            FINDINGS[Security Findings<br/>⚠️ Risk Assessment]
        end
        
        subgraph "Security & Compliance"
            ALERTS[Security Alerts<br/>🚨 EventBridge]
            DASHBOARD[Macie Dashboard<br/>📊 Risk Overview]
            REMEDIATION[Automated<br/>Remediation<br/>🔧 Lambda]
        end
        
        S3_BUCKETS --> ML_ENGINE
        ML_ENGINE --> CLASSIFIERS
        ML_ENGINE --> CUSTOM_TYPES
        CLASSIFIERS --> DISCOVERY
        CUSTOM_TYPES --> DISCOVERY
        DISCOVERY --> CLASSIFICATION
        CLASSIFICATION --> FINDINGS
        FINDINGS --> ALERTS
        FINDINGS --> DASHBOARD
        ALERTS --> REMEDIATION
        
        style ML_ENGINE fill:#FF9900,stroke:#232F3E,color:#fff
        style CLASSIFIERS fill:#4B9EDB,stroke:#232F3E,color:#fff
        style FINDINGS fill:#232F3E,stroke:#FF9900,color:#fff
        style ALERTS fill:#FF6B6B,stroke:#232F3E,color:#fff
    """

def create_redshift_sharing_architecture():
    """Create mermaid diagram for Redshift data sharing architecture"""
    return """
    graph TB
        subgraph "Producer Account"
            PROD_CLUSTER[Producer Cluster<br/>🏭 Data Processing<br/>ETL Workloads]
            DATASHARE[Data Share<br/>📤 Live Data<br/>Access Control]
        end
        
        subgraph "Consumer Account 1"
            CONS1_CLUSTER[Consumer Cluster<br/>📊 BI Analytics]
            CONS1_VIEWS[Shared Views<br/>👁️ Live Data<br/>Access]
        end
        
        subgraph "Consumer Account 2" 
            CONS2_WG[Serverless<br/>Workgroup<br/>⚡ Ad-hoc<br/>Queries]
            CONS2_VIEWS[Shared Views<br/>👁️ Live Data<br/>Access]
        end
        
        subgraph "Cross-Region Consumer"
            CROSS_CLUSTER[Cross-Region<br/>Cluster<br/>🌍 Global Access]
            CROSS_VIEWS[Shared Views<br/>👁️ Live Data<br/>Access]
        end
        
        subgraph "Security & Governance"
            IAM[IAM Policies<br/>🔐 Access Control]
            GRANTS[Share Grants<br/>✅ Permissions]
            AUDIT[CloudTrail<br/>📋 Audit Logs]
        end
        
        PROD_CLUSTER --> DATASHARE
        DATASHARE --> CONS1_VIEWS
        DATASHARE --> CONS2_VIEWS  
        DATASHARE --> CROSS_VIEWS
        CONS1_VIEWS --> CONS1_CLUSTER
        CONS2_VIEWS --> CONS2_WG
        CROSS_VIEWS --> CROSS_CLUSTER
        
        IAM --> DATASHARE
        GRANTS --> DATASHARE
        DATASHARE --> AUDIT
        
        style PROD_CLUSTER fill:#FF9900,stroke:#232F3E,color:#fff
        style DATASHARE fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CONS1_CLUSTER fill:#3FB34F,stroke:#232F3E,color:#fff
        style CONS2_WG fill:#3FB34F,stroke:#232F3E,color:#fff
        style CROSS_CLUSTER fill:#3FB34F,stroke:#232F3E,color:#fff
        style IAM fill:#232F3E,stroke:#FF9900,color:#fff
    """

def aws_config_tab():
    """Content for AWS Config tab"""
    st.markdown("## 📊 AWS Config")
    st.markdown("*Track resource inventory & changes - Assess, audit, and evaluate AWS resource configurations*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What is AWS Config?
    AWS Config is a fully managed service that provides:
    - **Resource Inventory**: Complete inventory of AWS resources with configuration details
    - **Configuration History**: Track changes to resource configurations over time  
    - **Configuration Change Notifications**: Get notified when resources change
    - **Compliance Monitoring**: Evaluate configurations against desired configurations
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ AWS Config Architecture")
    common.mermaid(create_config_architecture(), height=650)
    
    # Interactive Config Rule Builder
    st.markdown("#### 🛠️ Interactive Config Rule Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Rule Configuration")
        rule_name = st.text_input("Rule Name", value="s3-bucket-encryption-enabled")
        resource_type = st.selectbox("Resource Type", [
            "AWS::S3::Bucket", "AWS::EC2::Instance", "AWS::IAM::Role", 
            "AWS::RDS::DBInstance", "AWS::Lambda::Function"
        ])
        trigger_type = st.selectbox("Evaluation Trigger", [
            "Configuration Changes", "Periodic", "Configuration Changes and Periodic"
        ])
    
    with col2:
        st.markdown("##### Rule Parameters")
        if resource_type == "AWS::S3::Bucket":
            encryption_type = st.selectbox("Required Encryption", ["AES256", "aws:kms", "Any"])
        elif resource_type == "AWS::EC2::Instance":
            instance_type = st.multiselect("Allowed Instance Types", ["t3.micro", "t3.small", "m5.large", "m5.xlarge"])
        
        severity = st.selectbox("Compliance Severity", ["CRITICAL", "HIGH", "MEDIUM", "LOW"])
    
    # Generate rule configuration
    rule_config = generate_config_rule(rule_name, resource_type, trigger_type, severity)
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📋 Generated Config Rule
    **Rule Name**: {rule_name}  
    **Resource Type**: {resource_type}  
    **Trigger**: {trigger_type}  
    **Severity**: {severity}  
    **Estimated Resources Evaluated**: {estimate_resources(resource_type)}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Config capabilities
    st.markdown("#### ⭐ AWS Config Key Capabilities")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Configuration Recording
        - **All Resources**: Comprehensive resource tracking
        - **Selected Resources**: Focus on specific resource types
        - **Global Resources**: IAM roles, users, policies
        - **Configuration History**: Point-in-time snapshots
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ✅ Compliance Monitoring
        - **Pre-built Rules**: 200+ managed rules
        - **Custom Rules**: Lambda-based custom logic
        - **Remediation**: Automated fix actions
        - **Continuous Evaluation**: Real-time compliance
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📈 Advanced Analytics
        - **Configuration Aggregator**: Multi-account/region view
        - **Advanced Queries**: SQL-based resource queries
        - **Conformance Packs**: Predefined rule sets
        - **Organization Rules**: Account-wide governance
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Sample compliance dashboard
    st.markdown("#### 📊 Config Compliance Dashboard")
    
    # Generate sample compliance data
    compliance_data = generate_compliance_data()
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Compliance overview pie chart
        fig_pie = px.pie(
            values=list(compliance_data['status_counts'].values()),
            names=list(compliance_data['status_counts'].keys()),
            title="Overall Compliance Status",
            color_discrete_map={
                'COMPLIANT': AWS_COLORS['success'],
                'NON_COMPLIANT': AWS_COLORS['danger'],
                'NOT_APPLICABLE': AWS_COLORS['light_gray']
            }
        )
        fig_pie.update_layout(height=400)
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Compliance trends over time
        fig_trend = px.line(
            compliance_data['trends'],
            x='date',
            y='compliance_percentage',
            title="Compliance Trend (Last 30 Days)",
            markers=True
        )
        fig_trend.update_traces(line_color=AWS_COLORS['primary'])
        fig_trend.update_layout(height=400)
        st.plotly_chart(fig_trend, use_container_width=True)
    
    # Code examples
    st.markdown("#### 💻 AWS Config Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Setup Config", "Custom Rules", "Compliance Query"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# AWS Config Setup and Configuration
import boto3
import json

# Initialize AWS Config client
config_client = boto3.client('config')

def setup_aws_config():
    """Set up AWS Config for the first time"""
    
    # 1. Create S3 bucket for configuration history
    s3_bucket_name = 'aws-config-bucket-your-account-id'
    
    # 2. Create IAM service-linked role for Config
    iam_client = boto3.client('iam')
    
    try:
        # Config service-linked role is created automatically
        # when you enable Config for the first time
        pass
    except Exception as e:
        print(f"Service role creation: {e}")
    
    # 3. Create configuration recorder
    recorder_config = {
        'name': 'default',
        'roleARN': 'arn:aws:iam::123456789012:role/aws-service-role/config.amazonaws.com/AWSServiceRoleForConfig',
        'recordingGroup': {
            'allSupported': True,
            'includeGlobalResourceTypes': True,
            'recordingModeOverrides': [
                {
                    'resourceTypes': ['AWS::S3::Bucket'],
                    'recordingMode': {
                        'recordingFrequency': 'CONTINUOUS'
                    }
                }
            ]
        }
    }
    
    try:
        config_client.put_configuration_recorder(**recorder_config)
        print("✅ Configuration recorder created successfully")
    except Exception as e:
        print(f"❌ Error creating configuration recorder: {e}")
    
    # 4. Create delivery channel
    delivery_channel_config = {
        'name': 'default',
        's3BucketName': s3_bucket_name,
        's3KeyPrefix': 'config',
        'configSnapshotDeliveryProperties': {
            'deliveryFrequency': 'TwentyFour_Hours'
        }
    }
    
    try:
        config_client.put_delivery_channel(**delivery_channel_config)
        print("✅ Delivery channel created successfully")
    except Exception as e:
        print(f"❌ Error creating delivery channel: {e}")
    
    # 5. Start configuration recorder
    try:
        config_client.start_configuration_recorder(
            ConfigurationRecorderName='default'
        )
        print("✅ Configuration recorder started")
    except Exception as e:
        print(f"❌ Error starting configuration recorder: {e}")

def create_config_rules():
    """Create essential Config rules for security and compliance"""
    
    rules = [
        {
            'ConfigRuleName': 's3-bucket-public-read-prohibited',
            'Description': 'Checks that your S3 buckets do not allow public read access',
            'Source': {
                'Owner': 'AWS',
                'SourceIdentifier': 'S3_BUCKET_PUBLIC_READ_PROHIBITED'
            },
            'Scope': {
                'ComplianceResourceTypes': ['AWS::S3::Bucket']
            }
        },
        {
            'ConfigRuleName': 'ec2-security-group-attached-to-eni',
            'Description': 'Checks that security groups are attached to EC2 instances or ENIs',
            'Source': {
                'Owner': 'AWS',
                'SourceIdentifier': 'EC2_SECURITY_GROUP_ATTACHED_TO_ENI'
            },
            'Scope': {
                'ComplianceResourceTypes': ['AWS::EC2::SecurityGroup']
            }
        },
        {
            'ConfigRuleName': 'rds-storage-encrypted',
            'Description': 'Checks whether storage encryption is enabled for your RDS DB instances',
            'Source': {
                'Owner': 'AWS',
                'SourceIdentifier': 'RDS_STORAGE_ENCRYPTED'
            },
            'Scope': {
                'ComplianceResourceTypes': ['AWS::RDS::DBInstance']
            }
        },
        {
            'ConfigRuleName': 'iam-password-policy',
            'Description': 'Checks whether the account password policy meets specified requirements',
            'Source': {
                'Owner': 'AWS',
                'SourceIdentifier': 'IAM_PASSWORD_POLICY'
            },
            'InputParameters': json.dumps({
                'RequireUppercaseCharacters': 'true',
                'RequireLowercaseCharacters': 'true',
                'RequireNumbers': 'true',
                'MinimumPasswordLength': '14'
            })
        }
    ]
    
    created_rules = []
    for rule in rules:
        try:
            config_client.put_config_rule(ConfigRule=rule)
            created_rules.append(rule['ConfigRuleName'])
            print(f"✅ Created rule: {rule['ConfigRuleName']}")
        except Exception as e:
            print(f"❌ Error creating rule {rule['ConfigRuleName']}: {e}")
    
    return created_rules

def get_compliance_summary():
    """Get compliance summary for all Config rules"""
    
    try:
        # Get compliance summary by Config rule
        response = config_client.get_compliance_summary_by_config_rule()
        
        summary = {
            'compliant_rules': response['ComplianceSummary']['ComplianceByConfigRule']['ComplianceType']['COMPLIANT'],
            'non_compliant_rules': response['ComplianceSummary']['ComplianceByConfigRule']['ComplianceType']['NON_COMPLIANT'],
            'not_applicable_rules': response['ComplianceSummary']['ComplianceByConfigRule']['ComplianceType'].get('NOT_APPLICABLE', 0)
        }
        
        print("📊 Compliance Summary:")
        print(f"  Compliant Rules: {summary['compliant_rules']}")
        print(f"  Non-Compliant Rules: {summary['non_compliant_rules']}")
        print(f"  Not Applicable: {summary['not_applicable_rules']}")
        
        return summary
        
    except Exception as e:
        print(f"❌ Error getting compliance summary: {e}")
        return None

def get_configuration_history(resource_type, resource_id):
    """Get configuration history for a specific resource"""
    
    try:
        response = config_client.get_resource_config_history(
            resourceType=resource_type,
            resourceId=resource_id,
            limit=10
        )
        
        config_items = response['configurationItems']
        
        print(f"📋 Configuration History for {resource_type}/{resource_id}:")
        for item in config_items:
            print(f"  {item['configurationItemCaptureTime']}: {item['configurationItemStatus']}")
            if 'relationships' in item:
                print(f"    Related Resources: {len(item['relationships'])}")
        
        return config_items
        
    except Exception as e:
        print(f"❌ Error getting configuration history: {e}")
        return []

# Execute Config setup
if __name__ == "__main__":
    print("🚀 Setting up AWS Config...")
    
    # Initial setup
    setup_aws_config()
    
    # Create essential rules
    print("\\n📝 Creating Config Rules...")
    created_rules = create_config_rules()
    
    # Get compliance summary
    print("\\n📊 Getting Compliance Summary...")
    compliance = get_compliance_summary()
    
    # Example: Get configuration history for an S3 bucket
    print("\\n📋 Getting Configuration History...")
    history = get_configuration_history('AWS::S3::Bucket', 'my-example-bucket')
    
    print("\\n✅ AWS Config setup complete!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Custom AWS Config Rules using Lambda
import boto3
import json

def lambda_handler(event, context):
    """
    Custom Config rule to check if EC2 instances have specific tags
    This Lambda function evaluates EC2 instances for required tags
    """
    
    # Initialize AWS clients
    config_client = boto3.client('config')
    ec2_client = boto3.client('ec2')
    
    # Get the configuration item from the event
    config_item = event['configurationItem']
    
    # Check if this is an EC2 instance
    if config_item['resourceType'] != 'AWS::EC2::Instance':
        return {
            'compliance_type': 'NOT_APPLICABLE',
            'annotation': 'Rule only applies to EC2 instances'
        }
    
    # Required tags for compliance
    required_tags = {
        'Environment': ['Production', 'Staging', 'Development'],
        'Owner': None,  # Any value acceptable
        'CostCenter': None,
        'Purpose': None
    }
    
    # Get current tags from configuration item
    current_tags = {}
    if 'tags' in config_item['configuration']:
        for tag in config_item['configuration']['tags']:
            current_tags[tag['key']] = tag['value']
    
    # Evaluate compliance
    missing_tags = []
    invalid_values = []
    
    for required_tag, valid_values in required_tags.items():
        if required_tag not in current_tags:
            missing_tags.append(required_tag)
        elif valid_values and current_tags[required_tag] not in valid_values:
            invalid_values.append(f"{required_tag}={current_tags[required_tag]}")
    
    # Determine compliance
    if missing_tags or invalid_values:
        compliance_type = 'NON_COMPLIANT'
        issues = []
        if missing_tags:
            issues.append(f"Missing tags: {', '.join(missing_tags)}")
        if invalid_values:
            issues.append(f"Invalid values: {', '.join(invalid_values)}")
        annotation = '; '.join(issues)
    else:
        compliance_type = 'COMPLIANT'
        annotation = 'All required tags present with valid values'
    
    # Submit evaluation result
    evaluation = {
        'ComplianceResourceType': config_item['resourceType'],
        'ComplianceResourceId': config_item['resourceId'],
        'ComplianceType': compliance_type,
        'Annotation': annotation,
        'OrderingTimestamp': config_item['configurationItemCaptureTime']
    }
    
    # Put evaluation result back to Config
    config_client.put_evaluations(
        Evaluations=[evaluation],
        ResultToken=event['resultToken']
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Evaluation completed: {compliance_type}')
    }

# Custom Config Rule for S3 Bucket Lifecycle Configuration
def s3_lifecycle_rule_handler(event, context):
    """
    Custom rule to ensure S3 buckets have lifecycle configuration
    """
    
    config_client = boto3.client('config')
    s3_client = boto3.client('s3')
    
    config_item = event['configurationItem']
    
    if config_item['resourceType'] != 'AWS::S3::Bucket':
        return {'compliance_type': 'NOT_APPLICABLE'}
    
    bucket_name = config_item['resourceName']
    
    try:
        # Check if lifecycle configuration exists
        lifecycle_response = s3_client.get_bucket_lifecycle_configuration(
            Bucket=bucket_name
        )
        
        rules = lifecycle_response.get('Rules', [])
        
        # Check for specific lifecycle requirements
        has_transition_rule = False
        has_expiration_rule = False
        
        for rule in rules:
            if rule.get('Status') == 'Enabled':
                if 'Transitions' in rule:
                    has_transition_rule = True
                if 'Expiration' in rule:
                    has_expiration_rule = True
        
        if has_transition_rule and has_expiration_rule:
            compliance_type = 'COMPLIANT'
            annotation = 'Bucket has both transition and expiration rules'
        else:
            compliance_type = 'NON_COMPLIANT'
            missing_rules = []
            if not has_transition_rule:
                missing_rules.append('transition rule')
            if not has_expiration_rule:
                missing_rules.append('expiration rule')
            annotation = f"Missing: {', '.join(missing_rules)}"
            
    except s3_client.exceptions.NoSuchLifecycleConfiguration:
        compliance_type = 'NON_COMPLIANT'
        annotation = 'No lifecycle configuration found'
    except Exception as e:
        compliance_type = 'NON_COMPLIANT'
        annotation = f'Error checking lifecycle configuration: {str(e)}'
    
    # Submit evaluation
    evaluation = {
        'ComplianceResourceType': config_item['resourceType'],
        'ComplianceResourceId': config_item['resourceId'],
        'ComplianceType': compliance_type,
        'Annotation': annotation,
        'OrderingTimestamp': config_item['configurationItemCaptureTime']
    }
    
    config_client.put_evaluations(
        Evaluations=[evaluation],
        ResultToken=event['resultToken']
    )
    
    return {'statusCode': 200}

# Deploy Custom Config Rule
def deploy_custom_config_rule():
    """Deploy a custom Config rule with Lambda function"""
    
    import zipfile
    import io
    
    # Create Lambda function
    lambda_client = boto3.client('lambda')
    
    # Create deployment package (in practice, use proper packaging)
    lambda_code = """
def lambda_handler(event, context):
    # Your custom rule logic here
    return {"statusCode": 200}
    """
    
    # Create the Lambda function
    try:
        lambda_response = lambda_client.create_function(
            FunctionName='custom-config-rule-function',
            Runtime='python3.9',
            Role='arn:aws:iam::123456789012:role/config-lambda-role',
            Handler='index.lambda_handler',
            Code={'ZipFile': lambda_code.encode()},
            Description='Custom Config rule for tagging compliance'
        )
        
        lambda_arn = lambda_response['FunctionArn']
        
        # Create the Config rule
        config_client = boto3.client('config')
        
        rule_config = {
            'ConfigRuleName': 'custom-ec2-tagging-rule',
            'Description': 'Checks if EC2 instances have required tags',
            'Source': {
                'Owner': 'AWS_LAMBDA',
                'SourceIdentifier': lambda_arn
            },
            'Scope': {
                'ComplianceResourceTypes': ['AWS::EC2::Instance']
            }
        }
        
        config_client.put_config_rule(ConfigRule=rule_config)
        
        # Grant Config permission to invoke Lambda
        lambda_client.add_permission(
            FunctionName='custom-config-rule-function',
            StatementId='config-invoke-permission',
            Action='lambda:InvokeFunction',
            Principal='config.amazonaws.com'
        )
        
        print("✅ Custom Config rule deployed successfully")
        
    except Exception as e:
        print(f"❌ Error deploying custom rule: {e}")

# Example usage
if __name__ == "__main__":
    deploy_custom_config_rule()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- AWS Config Advanced Queries using SQL
-- Query all non-compliant resources across multiple resource types

-- 1. Find all S3 buckets without encryption
SELECT 
    resourceId,
    resourceName,
    resourceType,
    configurationItemCaptureTime,
    configurationItemStatus,
    configuration.serverSideEncryptionConfiguration
FROM aws_config_configuration_history
WHERE resourceType = 'AWS::S3::Bucket'
    AND configurationItemStatus = 'ResourceDiscovered'
    AND (configuration.serverSideEncryptionConfiguration IS NULL 
         OR configuration.serverSideEncryptionConfiguration.rules IS NULL)
ORDER BY configurationItemCaptureTime DESC;

-- 2. Find EC2 instances without required tags
SELECT 
    resourceId,
    resourceName,
    accountId,
    awsRegion,
    configuration.tags
FROM aws_config_configuration_history
WHERE resourceType = 'AWS::EC2::Instance'
    AND configurationItemStatus IN ('ResourceDiscovered', 'OK')
    AND (
        NOT EXISTS (
            SELECT 1 FROM unnest(configuration.tags) AS t(tag) 
            WHERE tag.key = 'Environment'
        )
        OR NOT EXISTS (
            SELECT 1 FROM unnest(configuration.tags) AS t(tag) 
            WHERE tag.key = 'Owner'
        )
    );

-- 3. Security groups with overly permissive rules
SELECT 
    resourceId,
    resourceName,
    accountId,
    awsRegion,
    rule.ipProtocol,
    rule.fromPort,
    rule.toPort,
    rule.ipRanges
FROM aws_config_configuration_history
CROSS JOIN unnest(configuration.ipPermissions) AS t(rule)
WHERE resourceType = 'AWS::EC2::SecurityGroup'
    AND configurationItemStatus = 'ResourceDiscovered'
    AND EXISTS (
        SELECT 1 FROM unnest(rule.ipRanges) AS r(range)
        WHERE range.cidrIp = '0.0.0.0/0'
    )
    AND (rule.fromPort = 22 OR rule.fromPort = 3389 OR rule.fromPort = 80);

-- 4. RDS instances without encryption
SELECT 
    resourceId,
    resourceName,
    configuration.engine,
    configuration.storageEncrypted,
    configuration.multiAZ,
    configuration.publiclyAccessible
FROM aws_config_configuration_history
WHERE resourceType = 'AWS::RDS::DBInstance'
    AND configurationItemStatus = 'ResourceDiscovered'
    AND configuration.storageEncrypted = false;

-- 5. IAM users with console access but no MFA
SELECT 
    resourceId,
    resourceName,
    configuration.passwordLastUsed,
    configuration.mfaDevices
FROM aws_config_configuration_history
WHERE resourceType = 'AWS::IAM::User'
    AND configurationItemStatus = 'ResourceDiscovered'
    AND configuration.passwordLastUsed IS NOT NULL
    AND (configuration.mfaDevices IS NULL 
         OR json_array_length(configuration.mfaDevices) = 0);

-- 6. Lambda functions with outdated runtimes
SELECT 
    resourceId,
    resourceName,
    configuration.runtime,
    configuration.lastModified,
    configurationItemCaptureTime
FROM aws_config_configuration_history
WHERE resourceType = 'AWS::Lambda::Function'
    AND configurationItemStatus = 'ResourceDiscovered'
    AND configuration.runtime IN ('python2.7', 'nodejs8.10', 'nodejs10.x', 'dotnetcore2.1');

-- 7. Network ACLs with broad allow rules
SELECT 
    resourceId,
    resourceName,
    entry.ruleNumber,
    entry.protocol,
    entry.cidrBlock,
    entry.ruleAction
FROM aws_config_configuration_history
CROSS JOIN unnest(configuration.entries) AS t(entry)
WHERE resourceType = 'AWS::EC2::NetworkAcl'
    AND configurationItemStatus = 'ResourceDiscovered'
    AND entry.cidrBlock = '0.0.0.0/0'
    AND entry.ruleAction = 'allow'
    AND entry.ruleNumber < 100;

-- 8. Cost optimization: Underutilized resources
SELECT 
    resourceType,
    COUNT(*) as resource_count,
    accountId,
    awsRegion
FROM aws_config_configuration_history
WHERE resourceType IN (
    'AWS::EC2::Instance',
    'AWS::RDS::DBInstance', 
    'AWS::ElastiCache::CacheCluster'
)
    AND configurationItemStatus = 'ResourceDiscovered'
    AND configurationItemCaptureTime >= current_timestamp - interval '7' day
GROUP BY resourceType, accountId, awsRegion
ORDER BY resource_count DESC;

-- 9. Compliance trend analysis
SELECT 
    DATE_TRUNC('day', configurationItemCaptureTime) as date,
    resourceType,
    COUNT(*) as total_resources,
    SUM(CASE WHEN configurationItemStatus = 'ResourceDiscovered' THEN 1 ELSE 0 END) as active_resources
FROM aws_config_configuration_history
WHERE configurationItemCaptureTime >= current_timestamp - interval '30' day
GROUP BY DATE_TRUNC('day', configurationItemCaptureTime), resourceType
ORDER BY date DESC, resourceType;

-- 10. Cross-account resource analysis
SELECT 
    accountId,
    resourceType,
    COUNT(*) as resource_count,
    COUNT(DISTINCT awsRegion) as regions_used
FROM aws_config_configuration_history
WHERE configurationItemStatus = 'ResourceDiscovered'
GROUP BY accountId, resourceType
HAVING COUNT(*) > 10
ORDER BY resource_count DESC;

-- Python script to execute Config queries
import boto3

def execute_config_query(query, max_results=100):
    """Execute advanced query against Config aggregator"""
    
    config_client = boto3.client('config')
    
    try:
        response = config_client.select_aggregate_resource_config(
            Expression=query,
            ConfigurationAggregatorName='organization-aggregator',
            MaxResults=max_results
        )
        
        results = response.get('Results', [])
        
        print(f"Query returned {len(results)} results")
        
        # Process and display results
        for result in results:
            resource_data = json.loads(result)
            print(f"Resource: {resource_data.get('resourceId', 'Unknown')}")
            print(f"Type: {resource_data.get('resourceType', 'Unknown')}")
            print(f"Account: {resource_data.get('accountId', 'Unknown')}")
            print("-" * 40)
        
        return results
        
    except Exception as e:
        print(f"Error executing query: {e}")
        return []

# Example usage
unencrypted_s3_query = """
SELECT resourceId, resourceName, accountId, awsRegion
WHERE resourceType = 'AWS::S3::Bucket'
AND configuration.serverSideEncryptionConfiguration IS NULL
"""

results = execute_config_query(unencrypted_s3_query)
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)

def amazon_macie_tab():
    """Content for Amazon Macie tab"""
    st.markdown("## 🔍 Amazon Macie")
    st.markdown("*Discover and protect sensitive data using machine learning and pattern matching*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What is Amazon Macie?
    Amazon Macie is a data security service that:
    - **Discovers Sensitive Data**: Uses ML to identify PII, PHI, financial data, and more
    - **Continuous Monitoring**: Evaluates S3 environment for security risks
    - **Automated Classification**: Applies sensitivity labels to discovered data
    - **Security Insights**: Provides actionable findings and remediation guidance
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ Amazon Macie Architecture")
    common.mermaid(create_macie_architecture(), height=700)
    
    # Interactive Sensitive Data Discovery
    st.markdown("#### 🔍 Sensitive Data Discovery Simulator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Data Discovery Job Configuration")
        bucket_selection = st.selectbox("S3 Buckets to Scan", [
            "All buckets in account", "Specific buckets", "Buckets by tag"
        ])
        
        if bucket_selection == "Specific buckets":
            selected_buckets = st.multiselect("Select Buckets", [
                "customer-data-prod", "employee-records", "financial-reports", 
                "application-logs", "backup-archives"
            ])
        
        scan_frequency = st.selectbox("Scan Frequency", [
            "One-time", "Daily", "Weekly", "Monthly"
        ])
    
    with col2:
        st.markdown("##### Data Types to Identify")
        data_types = st.multiselect("Sensitive Data Types", [
            "Credit Card Numbers", "Social Security Numbers", "Email Addresses",
            "Phone Numbers", "Banking Information", "Driver License Numbers",
            "Passport Numbers", "Medical Record Numbers", "Custom Patterns"
        ], default=["Credit Card Numbers", "Social Security Numbers", "Email Addresses"])
        
        sensitivity_level = st.selectbox("Minimum Sensitivity", ["HIGH", "MEDIUM", "LOW"])
    
    # Generate discovery results
    discovery_results = generate_macie_findings(data_types, sensitivity_level)
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📊 Data Discovery Results
    **Buckets Scanned**: {discovery_results['buckets_scanned']}  
    **Objects Analyzed**: {discovery_results['objects_analyzed']:,}  
    **Sensitive Files Found**: {discovery_results['sensitive_files']}  
    **High Risk Findings**: {discovery_results['high_risk_findings']}  
    **Estimated Cost**: ${discovery_results['estimated_cost']:.2f}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Macie findings dashboard
    st.markdown("#### 📊 Macie Security Findings Dashboard")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Findings by severity
        severity_data = discovery_results['findings_by_severity']
        fig_severity = px.bar(
            x=list(severity_data.keys()),
            y=list(severity_data.values()),
            title="Findings by Severity",
            color=list(severity_data.keys()),
            color_discrete_map={
                'HIGH': AWS_COLORS['danger'],
                'MEDIUM': AWS_COLORS['warning'],
                'LOW': AWS_COLORS['light_blue']
            }
        )
        fig_severity.update_layout(height=400)
        st.plotly_chart(fig_severity, use_container_width=True)
    
    with col2:
        # Data types found
        data_types_found = discovery_results['data_types_distribution']
        fig_types = px.pie(
            values=list(data_types_found.values()),
            names=list(data_types_found.keys()),
            title="Sensitive Data Types Discovered"
        )
        fig_types.update_layout(height=400)
        st.plotly_chart(fig_types, use_container_width=True)
    
    # Detailed findings table
    st.markdown("#### 📋 Detailed Security Findings")
    findings_df = create_findings_dataframe(discovery_results)
    st.dataframe(findings_df, use_container_width=True)
    
    # Macie features breakdown
    st.markdown("#### ⭐ Amazon Macie Key Features")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🤖 Machine Learning Detection
        - **Pre-trained Models**: 150+ data type identifiers
        - **Pattern Recognition**: Advanced regex and ML models
        - **Context Analysis**: Understands data relationships
        - **Custom Classifiers**: Business-specific data types
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔒 Security Assessment
        - **Bucket Policy Analysis**: Permission evaluation
        - **Access Control Review**: Public/private analysis
        - **Encryption Status**: At-rest encryption checks
        - **Risk Scoring**: Automated risk assessment
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📈 Monitoring & Alerts
        - **EventBridge Integration**: Real-time notifications
        - **Finding Suppression**: Reduce false positives
        - **Automated Remediation**: Lambda-triggered actions
        - **Compliance Reporting**: Governance dashboards
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 Amazon Macie Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Setup Macie", "Data Discovery Job", "Automated Remediation"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Amazon Macie Setup and Configuration
import boto3
import json
from datetime import datetime, timedelta

# Initialize Macie client
macie_client = boto3.client('macie2')

def enable_macie():
    """Enable Amazon Macie for the account"""
    
    try:
        # Enable Macie
        response = macie_client.enable_macie(
            findingPublishingFrequency='FIFTEEN_MINUTES',
            status='ENABLED'
        )
        
        print("✅ Amazon Macie enabled successfully")
        return response
        
    except Exception as e:
        print(f"❌ Error enabling Macie: {e}")
        return None

def setup_macie_member_accounts():
    """Set up Macie for organization member accounts"""
    
    member_accounts = [
        {'accountId': '111111111111', 'email': 'security@company.com'},
        {'accountId': '222222222222', 'email': 'dev-team@company.com'},
        {'accountId': '333333333333', 'email': 'prod-team@company.com'}
    ]
    
    try:
        # Create member associations
        response = macie_client.create_member(
            account=member_accounts[0]  # Example for one account
        )
        
        # Send invitation
        macie_client.create_invitations(
            accountIds=[account['accountId'] for account in member_accounts],
            disableEmailNotification=False,
            message='Join our organization Macie setup for data protection'
        )
        
        print("✅ Member account invitations sent")
        
    except Exception as e:
        print(f"❌ Error setting up member accounts: {e}")

def configure_s3_buckets_for_macie():
    """Configure S3 buckets for Macie monitoring"""
    
    s3_client = boto3.client('s3')
    
    try:
        # Get list of S3 buckets
        buckets_response = s3_client.list_buckets()
        buckets = buckets_response['Buckets']
        
        # Classify buckets by sensitivity
        bucket_classifications = []
        
        for bucket in buckets:
            bucket_name = bucket['Name']
            
            # Determine classification based on naming patterns
            if any(keyword in bucket_name.lower() for keyword in ['prod', 'customer', 'financial']):
                classification_type = 'SENSITIVE'
                monitoring_frequency = 'DAILY'
            elif any(keyword in bucket_name.lower() for keyword in ['dev', 'test', 'temp']):
                classification_type = 'INTERNAL'  
                monitoring_frequency = 'WEEKLY'
            else:
                classification_type = 'PUBLIC'
                monitoring_frequency = 'MONTHLY'
            
            bucket_classifications.append({
                'bucket_name': bucket_name,
                'classification': classification_type,
                'monitoring': monitoring_frequency
            })
        
        print("📊 S3 Bucket Classifications:")
        for bucket_class in bucket_classifications:
            print(f"  {bucket_class['bucket_name']}: {bucket_class['classification']} ({bucket_class['monitoring']})")
        
        return bucket_classifications
        
    except Exception as e:
        print(f"❌ Error classifying buckets: {e}")
        return []

def create_custom_data_identifier():
    """Create custom data identifier for business-specific patterns"""
    
    try:
        # Example: Custom employee ID pattern
        response = macie_client.create_custom_data_identifier(
            name='company-employee-id',
            description='Detects company employee ID format: EMP-XXXXX',
            regex=r'EMP-[0-9]{5}',
            keywords=['employee', 'staff', 'worker', 'personnel'],
            maximumMatchDistance=50,
            tags={
                'Purpose': 'Employee Data Protection',
                'DataType': 'Internal-ID',
                'Sensitivity': 'Medium'
            }
        )
        
        custom_id = response['customDataIdentifierId']
        print(f"✅ Custom data identifier created: {custom_id}")
        
        # Example: Financial account pattern
        financial_response = macie_client.create_custom_data_identifier(
            name='company-account-number',
            description='Detects company account numbers: ACC-XXXXXXXXX',
            regex=r'ACC-[0-9]{9}',
            keywords=['account', 'financial', 'banking', 'payment'],
            maximumMatchDistance=30,
            tags={
                'Purpose': 'Financial Data Protection',
                'DataType': 'Account-Number',
                'Sensitivity': 'High'
            }
        )
        
        return [custom_id, financial_response['customDataIdentifierId']]
        
    except Exception as e:
        print(f"❌ Error creating custom data identifier: {e}")
        return []

def configure_finding_filters():
    """Configure finding filters to reduce noise"""
    
    try:
        # Filter to suppress findings for test buckets
        test_filter = macie_client.create_findings_filter(
            name='suppress-test-bucket-findings',
            description='Suppress findings from test and development buckets',
            findingCriteria={
                'criterion': {
                    'resourcesAffected.s3Bucket.name': {
                        'contains': ['test', 'dev', 'sandbox']
                    },
                    'severity.description': {
                        'eq': ['Low']
                    }
                }
            },
            action='ARCHIVE',
            tags={
                'FilterType': 'Test-Environment',
                'Purpose': 'Noise-Reduction'
            }
        )
        
        # Filter for high-priority findings
        priority_filter = macie_client.create_findings_filter(
            name='high-priority-findings',
            description='Archive low-severity findings in non-production buckets',
            findingCriteria={
                'criterion': {
                    'severity.description': {
                        'eq': ['High', 'Critical']
                    }
                }
            },
            action='NOOP',  # Keep these findings active
            tags={
                'FilterType': 'High-Priority',
                'Purpose': 'Security-Focus'
            }
        )
        
        print("✅ Finding filters configured")
        return [test_filter, priority_filter]
        
    except Exception as e:
        print(f"❌ Error configuring finding filters: {e}")
        return []

def setup_eventbridge_integration():
    """Set up EventBridge integration for Macie findings"""
    
    events_client = boto3.client('events')
    
    try:
        # Create EventBridge rule for high-severity findings
        rule_response = events_client.put_rule(
            Name='macie-high-severity-findings',
            EventPattern=json.dumps({
                'source': ['aws.macie'],
                'detail-type': ['Macie Finding'],
                'detail': {
                    'severity': {
                        'description': ['High', 'Critical']
                    }
                }
            }),
            State='ENABLED',
            Description='Trigger on high-severity Macie findings'
        )
        
        # Add SNS target for notifications
        sns_client = boto3.client('sns')
        topic_arn = 'arn:aws:sns:us-west-2:123456789012:security-alerts'
        
        events_client.put_targets(
            Rule='macie-high-severity-findings',
            Targets=[
                {
                    'Id': '1',
                    'Arn': topic_arn,
                    'MessageGroupId': 'macie-findings'
                }
            ]
        )
        
        print("✅ EventBridge integration configured")
        
    except Exception as e:
        print(f"❌ Error setting up EventBridge: {e}")

# Execute Macie setup
if __name__ == "__main__":
    print("🚀 Setting up Amazon Macie...")
    
    # Enable Macie
    enable_response = enable_macie()
    
    # Configure S3 buckets
    print("\\n📊 Configuring S3 buckets...")
    bucket_config = configure_s3_buckets_for_macie()
    
    # Create custom data identifiers
    print("\\n🔍 Creating custom data identifiers...")
    custom_identifiers = create_custom_data_identifier()
    
    # Configure finding filters
    print("\\n🔧 Configuring finding filters...")
    filters = configure_finding_filters()
    
    # Set up EventBridge integration
    print("\\n📡 Setting up EventBridge integration...")
    setup_eventbridge_integration()
    
    print("\\n✅ Amazon Macie setup complete!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Amazon Macie Data Discovery Job Creation and Management
import boto3
import json
from datetime import datetime, timedelta

macie_client = boto3.client('macie2')

def create_comprehensive_discovery_job():
    """Create a comprehensive data discovery job"""
    
    # Define S3 buckets to include/exclude
    s3_job_definition = {
        'buckets': [
            {
                'accountId': '123456789012',
                'buckets': ['customer-data-prod', 'employee-records', 'financial-reports']
            }
        ],
        'scoping': {
            'excludes': {
                'and': [
                    {
                        'simpleScopeTerm': {
                            'comparator': 'CONTAINS',
                            'key': 'OBJECT_KEY',
                            'values': ['temp/', 'cache/', 'logs/']
                        }
                    }
                ]
            },
            'includes': {
                'and': [
                    {
                        'simpleScopeTerm': {
                            'comparator': 'CONTAINS',
                            'key': 'OBJECT_EXTENSION',
                            'values': ['csv', 'json', 'txt', 'xml', 'xlsx', 'pdf']
                        }
                    }
                ]
            }
        }
    }
    
    # Configure data identifiers
    managed_data_identifier_ids = [
        'CREDIT_CARD_NUMBER',
        'SSN',
        'EMAIL_ADDRESS',
        'PHONE_NUMBER',
        'BANK_ACCOUNT_NUMBER',
        'DRIVERS_LICENSE',
        'PASSPORT_NUMBER'
    ]
    
    try:
        # Create the discovery job
        response = macie_client.create_classification_job(
            name='comprehensive-pii-discovery',
            description='Comprehensive PII discovery across production data stores',
            jobType='ONE_TIME',  # or 'SCHEDULED'
            s3JobDefinition=s3_job_definition,
            managedDataIdentifierIds=managed_data_identifier_ids,
            samplingPercentage=100,  # Scan 100% of qualifying objects
            tags={
                'Environment': 'Production',
                'Purpose': 'Data-Discovery',
                'Compliance': 'GDPR-CCPA',
                'Owner': 'Security-Team'
            }
        )
        
        job_id = response['jobId']
        print(f"✅ Discovery job created: {job_id}")
        
        return job_id
        
    except Exception as e:
        print(f"❌ Error creating discovery job: {e}")
        return None

def create_scheduled_discovery_job():
    """Create a scheduled discovery job for ongoing monitoring"""
    
    # Calculate schedule - run weekly on Sundays at 2 AM UTC
    schedule_expression = {
        'dailySchedule': {},  # Can be empty for daily
        'weeklySchedule': {
            'dayOfWeek': 'SUNDAY'
        },
        'monthlySchedule': {
            'dayOfMonth': 1  # First day of each month
        }
    }
    
    # More targeted S3 scope for scheduled jobs
    s3_definition = {
        'buckets': [
            {
                'accountId': '123456789012',
                'buckets': ['new-customer-uploads', 'daily-transaction-files']
            }
        ],
        'scoping': {
            'includes': {
                'and': [
                    {
                        'simpleScopeTerm': {
                            'comparator': 'GT',
                            'key': 'OBJECT_LAST_MODIFIED_DATE',
                            'values': [(datetime.now() - timedelta(days=7)).isoformat()]
                        }
                    },
                    {
                        'simpleScopeTerm': {
                            'comparator': 'LT',
                            'key': 'OBJECT_SIZE',
                            'values': ['1073741824']  # 1GB limit
                        }
                    }
                ]
            }
        }
    }
    
    try:
        response = macie_client.create_classification_job(
            name='weekly-incremental-scan',
            description='Weekly scan of new customer data uploads',
            jobType='SCHEDULED',
            scheduleFrequency=schedule_expression,
            s3JobDefinition=s3_definition,
            managedDataIdentifierIds=['CREDIT_CARD_NUMBER', 'SSN', 'EMAIL_ADDRESS'],
            samplingPercentage=50,  # Sample 50% for efficiency
            tags={
                'Schedule': 'Weekly',
                'Type': 'Incremental',
                'Priority': 'Medium'
            }
        )
        
        print(f"✅ Scheduled discovery job created: {response['jobId']}")
        return response['jobId']
        
    except Exception as e:
        print(f"❌ Error creating scheduled job: {e}")
        return None

def monitor_discovery_job(job_id):
    """Monitor the progress of a discovery job"""
    
    try:
        while True:
            # Get job status
            response = macie_client.describe_classification_job(jobId=job_id)
            
            job_status = response['jobStatus']
            print(f"Job Status: {job_status}")
            
            if job_status in ['COMPLETE', 'CANCELLED', 'USER_PAUSED']:
                break
            elif job_status == 'RUNNING':
                # Get statistics if available
                stats = response.get('statistics', {})
                if stats:
                    print(f"  Objects processed: {stats.get('approximateNumberOfObjectsToProcess', 0)}")
                    print(f"  Objects analyzed: {stats.get('numberOfRuns', 0)}")
            
            # Wait before next check
            time.sleep(30)
        
        # Get final statistics
        if job_status == 'COMPLETE':
            stats = response.get('statistics', {})
            print("\\n📊 Job Complete - Final Statistics:")
            print(f"  Objects processed: {stats.get('approximateNumberOfObjectsToProcess', 0)}")
            print(f"  Sensitive objects found: {stats.get('approximateNumberOfObjectsWithFindings', 0)}")
            
        return response
        
    except Exception as e:
        print(f"❌ Error monitoring job: {e}")
        return None

def get_discovery_job_results(job_id):
    """Retrieve and analyze results from a discovery job"""
    
    try:
        # Get findings related to this job
        findings_response = macie_client.list_findings(
            findingCriteria={
                'criterion': {
                    'classificationDetails.jobId': {
                        'eq': [job_id]
                    }
                }
            },
            maxResults=50
        )
        
        findings = findings_response.get('findingIds', [])
        
        if not findings:
            print("No findings generated from this job")
            return []
        
        # Get detailed finding information
        detailed_findings = []
        for finding_id in findings:
            finding_response = macie_client.get_findings(
                findingIds=[finding_id]
            )
            
            if finding_response['findings']:
                finding = finding_response['findings'][0]
                
                detailed_finding = {
                    'id': finding['id'],
                    'type': finding['type'],
                    'severity': finding['severity']['description'],
                    'bucket': finding['resourcesAffected']['s3Bucket']['name'],
                    'object_key': finding['resourcesAffected']['s3Object']['key'],
                    'data_types': [
                        dt['detections'][0]['type'] 
                        for dt in finding.get('classificationDetails', {}).get('result', {}).get('sensitiveData', [])
                        if dt.get('detections')
                    ],
                    'count': sum([
                        dt['detections'][0]['count']
                        for dt in finding.get('classificationDetails', {}).get('result', {}).get('sensitiveData', [])
                        if dt.get('detections')
                    ])
                }
                
                detailed_findings.append(detailed_finding)
        
        # Analyze results
        print("\\n🔍 Discovery Job Results Analysis:")
        print(f"Total findings: {len(detailed_findings)}")
        
        # Group by severity
        severity_counts = {}
        for finding in detailed_findings:
            severity = finding['severity']
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
        
        print("\\nFindings by severity:")
        for severity, count in severity_counts.items():
            print(f"  {severity}: {count}")
        
        # Group by data type
        data_type_counts = {}
        for finding in detailed_findings:
            for data_type in finding['data_types']:
                data_type_counts[data_type] = data_type_counts.get(data_type, 0) + 1
        
        print("\\nSensitive data types found:")
        for data_type, count in data_type_counts.items():
            print(f"  {data_type}: {count} occurrences")
        
        return detailed_findings
        
    except Exception as e:
        print(f"❌ Error retrieving job results: {e}")
        return []

def export_discovery_results(job_id, findings):
    """Export discovery results to various formats"""
    
    import csv
    import json
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Export to CSV
    csv_filename = f"macie_findings_{job_id}_{timestamp}.csv"
    with open(csv_filename, 'w', newline='') as csvfile:
        fieldnames = ['finding_id', 'severity', 'bucket', 'object_key', 'data_types', 'sensitive_count']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for finding in findings:
            writer.writerow({
                'finding_id': finding['id'],
                'severity': finding['severity'],
                'bucket': finding['bucket'],
                'object_key': finding['object_key'],
                'data_types': ', '.join(finding['data_types']),
                'sensitive_count': finding['count']
            })
    
    print(f"✅ Results exported to {csv_filename}")
    
    # Export to JSON for programmatic processing
    json_filename = f"macie_findings_{job_id}_{timestamp}.json"
    with open(json_filename, 'w') as jsonfile:
        json.dump(findings, jsonfile, indent=2, default=str)
    
    print(f"✅ Results exported to {json_filename}")
    
    return csv_filename, json_filename

# Execute discovery job workflow
if __name__ == "__main__":
    print("🚀 Starting Macie Data Discovery Job...")
    
    # Create comprehensive discovery job
    job_id = create_comprehensive_discovery_job()
    
    if job_id:
        print(f"\\n⏳ Monitoring job progress: {job_id}")
        final_status = monitor_discovery_job(job_id)
        
        print("\\n📊 Retrieving job results...")
        findings = get_discovery_job_results(job_id)
        
        if findings:
            print("\\n📤 Exporting results...")
            export_discovery_results(job_id, findings)
    
    # Also create a scheduled job for ongoing monitoring
    print("\\n📅 Creating scheduled discovery job...")
    scheduled_job_id = create_scheduled_discovery_job()
    
    print("\\n✅ Macie data discovery setup complete!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Automated Remediation for Macie Findings
import boto3
import json
import logging
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function to automatically remediate Macie findings
    Triggered by EventBridge when Macie finding is created
    """
    
    # Parse the Macie finding from EventBridge event
    macie_finding = event['detail']
    
    # Initialize AWS clients
    s3_client = boto3.client('s3')
    sns_client = boto3.client('sns')
    
    finding_id = macie_finding['id']
    finding_type = macie_finding['type']
    severity = macie_finding['severity']['description']
    
    # Extract S3 object information
    s3_bucket = macie_finding['resourcesAffected']['s3Bucket']['name']
    s3_object_key = macie_finding['resourcesAffected']['s3Object']['key']
    
    logger.info(f"Processing Macie finding: {finding_id}")
    logger.info(f"Finding type: {finding_type}, Severity: {severity}")
    logger.info(f"Affected object: s3://{s3_bucket}/{s3_object_key}")
    
    # Determine remediation action based on severity and finding type
    remediation_actions = determine_remediation_actions(finding_type, severity, s3_bucket)
    
    results = []
    for action in remediation_actions:
        try:
            if action['type'] == 'QUARANTINE':
                result = quarantine_sensitive_object(s3_client, s3_bucket, s3_object_key)
            elif action['type'] == 'ENCRYPT':
                result = encrypt_sensitive_object(s3_client, s3_bucket, s3_object_key)
            elif action['type'] == 'RESTRICT_ACCESS':
                result = restrict_object_access(s3_client, s3_bucket, s3_object_key)
            elif action['type'] == 'NOTIFY':
                result = send_security_notification(sns_client, macie_finding, action['recipients'])
            elif action['type'] == 'TAG':
                result = tag_sensitive_object(s3_client, s3_bucket, s3_object_key, macie_finding)
            else:
                result = {'status': 'SKIPPED', 'reason': f'Unknown action type: {action["type"]}'}
            
            results.append({
                'action': action['type'],
                'status': result['status'],
                'details': result.get('details', '')
            })
            
        except Exception as e:
            logger.error(f"Error executing {action['type']}: {str(e)}")
            results.append({
                'action': action['type'],
                'status': 'FAILED',
                'details': str(e)
            })
    
    # Log remediation results
    logger.info(f"Remediation completed for finding {finding_id}: {results}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'finding_id': finding_id,
            'remediation_actions': results
        })
    }

def determine_remediation_actions(finding_type, severity, bucket_name):
    """Determine appropriate remediation actions based on finding characteristics"""
    
    actions = []
    
    # High-severity findings require immediate action
    if severity == 'HIGH':
        actions.extend([
            {'type': 'QUARANTINE', 'priority': 1},
            {'type': 'NOTIFY', 'recipients': ['security-team', 'data-owners'], 'priority': 1},
            {'type': 'TAG', 'priority': 2}
        ])
    
    # Medium-severity findings
    elif severity == 'MEDIUM':
        if 'production' in bucket_name.lower() or 'prod' in bucket_name.lower():
            actions.extend([
                {'type': 'ENCRYPT', 'priority': 1},
                {'type': 'RESTRICT_ACCESS', 'priority': 2},
                {'type': 'NOTIFY', 'recipients': ['data-owners'], 'priority': 3}
            ])
        else:
            actions.extend([
                {'type': 'TAG', 'priority': 1},
                {'type': 'NOTIFY', 'recipients': ['data-owners'], 'priority': 2}
            ])
    
    # Low-severity findings
    else:
        actions.extend([
            {'type': 'TAG', 'priority': 1}
        ])
    
    # PII-specific actions
    if 'PII' in finding_type:
        actions.append({'type': 'ENCRYPT', 'priority': 1})
    
    # Sort by priority
    actions.sort(key=lambda x: x.get('priority', 10))
    
    return actions

def quarantine_sensitive_object(s3_client, bucket_name, object_key):
    """Move sensitive object to quarantine bucket"""
    
    quarantine_bucket = f"{bucket_name}-quarantine"
    
    try:
        # Create quarantine bucket if it doesn't exist
        try:
            s3_client.head_bucket(Bucket=quarantine_bucket)
        except:
            s3_client.create_bucket(
                Bucket=quarantine_bucket,
                CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
            )
            
            # Apply restrictive bucket policy
            quarantine_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "RestrictAccess",
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{quarantine_bucket}/*",
                        "Condition": {
                            "StringNotEquals": {
                                "aws:PrincipalArn": "arn:aws:iam::123456789012:role/SecurityTeamRole"
                            }
                        }
                    }
                ]
            }
            
            s3_client.put_bucket_policy(
                Bucket=quarantine_bucket,
                Policy=json.dumps(quarantine_policy)
            )
        
        # Copy object to quarantine
        copy_source = {'Bucket': bucket_name, 'Key': object_key}
        quarantine_key = f"quarantined/{datetime.now().strftime('%Y/%m/%d')}/{object_key}"
        
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=quarantine_bucket,
            Key=quarantine_key,
            TaggingDirective='REPLACE',
            Tagging='Status=Quarantined&Reason=MacieFinding&Date=' + datetime.now().isoformat()
        )
        
        # Delete original object
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        
        return {
            'status': 'SUCCESS',
            'details': f'Object moved to quarantine: s3://{quarantine_bucket}/{quarantine_key}'
        }
        
    except Exception as e:
        return {
            'status': 'FAILED',
            'details': f'Quarantine failed: {str(e)}'
        }

def encrypt_sensitive_object(s3_client, bucket_name, object_key):
    """Apply server-side encryption to sensitive object"""
    
    try:
        # Get object metadata
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        
        # Check if already encrypted
        if response.get('ServerSideEncryption'):
            return {
                'status': 'SKIPPED',
                'details': 'Object already encrypted'
            }
        
        # Copy object with encryption
        copy_source = {'Bucket': bucket_name, 'Key': object_key}
        
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=object_key,
            ServerSideEncryption='aws:kms',
            SSEKMSKeyId='arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012',
            MetadataDirective='REPLACE',
            Metadata={
                'MacieFindingRemediation': 'Encrypted',
                'EncryptionDate': datetime.now().isoformat()
            }
        )
        
        return {
            'status': 'SUCCESS',
            'details': 'Object encrypted with KMS'
        }
        
    except Exception as e:
        return {
            'status': 'FAILED',
            'details': f'Encryption failed: {str(e)}'
        }

def restrict_object_access(s3_client, bucket_name, object_key):
    """Apply restrictive ACL to sensitive object"""
    
    try:
        # Apply private ACL
        s3_client.put_object_acl(
            Bucket=bucket_name,
            Key=object_key,
            ACL='private'
        )
        
        # Add object-level access restriction
        s3_client.put_object_tagging(
            Bucket=bucket_name,
            Key=object_key,
            Tagging={
                'TagSet': [
                    {'Key': 'AccessLevel', 'Value': 'Restricted'},
                    {'Key': 'MacieRemediation', 'Value': 'AccessRestricted'},
                    {'Key': 'LastModified', 'Value': datetime.now().isoformat()}
                ]
            }
        )
        
        return {
            'status': 'SUCCESS',
            'details': 'Object access restricted'
        }
        
    except Exception as e:
        return {
            'status': 'FAILED',
            'details': f'Access restriction failed: {str(e)}'
        }

def send_security_notification(sns_client, macie_finding, recipients):
    """Send notification about Macie finding"""
    
    try:
        # Determine SNS topic based on recipients
        topic_mapping = {
            'security-team': 'arn:aws:sns:us-west-2:123456789012:security-alerts',
            'data-owners': 'arn:aws:sns:us-west-2:123456789012:data-governance-alerts'
        }
        
        notifications_sent = []
        
        for recipient in recipients:
            topic_arn = topic_mapping.get(recipient)
            if not topic_arn:
                continue
            
            message = create_notification_message(macie_finding)
            
            sns_client.publish(
                TopicArn=topic_arn,
                Subject=f"Macie Security Finding - {macie_finding['severity']['description']} Severity",
                Message=message
            )
            
            notifications_sent.append(recipient)
        
        return {
            'status': 'SUCCESS',
            'details': f'Notifications sent to: {", ".join(notifications_sent)}'
        }
        
    except Exception as e:
        return {
            'status': 'FAILED',
            'details': f'Notification failed: {str(e)}'
        }

def create_notification_message(macie_finding):
    """Create detailed notification message"""
    
    s3_bucket = macie_finding['resourcesAffected']['s3Bucket']['name']
    s3_object = macie_finding['resourcesAffected']['s3Object']['key']
    
    message = f"""
AMAZON MACIE SECURITY FINDING ALERT

Finding ID: {macie_finding['id']}
Severity: {macie_finding['severity']['description']}
Finding Type: {macie_finding['type']}

Affected Resource:
- S3 Bucket: {s3_bucket}
- S3 Object: {s3_object}

Description: {macie_finding.get('description', 'N/A')}

Sensitive Data Types Detected:
"""
    
    # Add details about sensitive data found
    if 'classificationDetails' in macie_finding:
        sensitive_data = macie_finding['classificationDetails'].get('result', {}).get('sensitiveData', [])
        for data_type in sensitive_data:
            for detection in data_type.get('detections', []):
                message += f"- {detection['type']}: {detection['count']} occurrences\\n"
    
    message += f"""
Recommended Actions:
1. Review the affected object for business necessity
2. Implement appropriate access controls
3. Consider encryption or data masking
4. Update data handling procedures

Finding Details: https://console.aws.amazon.com/macie/home?region=us-west-2#/findings/{macie_finding['id']}

This is an automated alert from Amazon Macie.
"""
    
    return message

def tag_sensitive_object(s3_client, bucket_name, object_key, macie_finding):
    """Tag object with Macie finding information"""
    
    try:
        # Prepare tags
        tags = [
            {'Key': 'MacieFindingId', 'Value': macie_finding['id']},
            {'Key': 'MacieSeverity', 'Value': macie_finding['severity']['description']},
            {'Key': 'MacieType', 'Value': macie_finding['type']},
            {'Key': 'MacieRemediationDate', 'Value': datetime.now().isoformat()},
            {'Key': 'DataClassification', 'Value': 'Sensitive'}
        ]
        
        # Add data type specific tags
        if 'classificationDetails' in macie_finding:
            sensitive_data = macie_finding['classificationDetails'].get('result', {}).get('sensitiveData', [])
            data_types = []
            for data_type in sensitive_data:
                for detection in data_type.get('detections', []):
                    data_types.append(detection['type'])
            
            if data_types:
                tags.append({'Key': 'SensitiveDataTypes', 'Value': ','.join(set(data_types))})
        
        s3_client.put_object_tagging(
            Bucket=bucket_name,
            Key=object_key,
            Tagging={'TagSet': tags}
        )
        
        return {
            'status': 'SUCCESS',
            'details': f'Object tagged with {len(tags)} Macie-related tags'
        }
        
    except Exception as e:
        return {
            'status': 'FAILED',
            'details': f'Tagging failed: {str(e)}'
        }

# CloudFormation template for automated remediation setup
remediation_template = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "Automated Macie finding remediation infrastructure",
    "Resources": {
        "MacieRemediationRole": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "lambda.amazonaws.com"},
                            "Action": "sts:AssumeRole"
                        }
                    ]
                },
                "ManagedPolicyArns": [
                    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                ],
                "Policies": [
                    {
                        "PolicyName": "MacieRemediationPolicy",
                        "PolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "s3:GetObject",
                                        "s3:PutObject",
                                        "s3:DeleteObject",
                                        "s3:PutObjectAcl",
                                        "s3:PutObjectTagging",
                                        "s3:CreateBucket",
                                        "s3:PutBucketPolicy"
                                    ],
                                    "Resource": "*"
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "sns:Publish"
                                    ],
                                    "Resource": "arn:aws:sns:*:*:*"
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "kms:Encrypt",
                                        "kms:Decrypt",
                                        "kms:GenerateDataKey"
                                    ],
                                    "Resource": "*"
                                }
                            ]
                        }
                    }
                ]
            }
        }
    }
}
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def data_sharing_redshift_tab():
    """Content for Data Sharing in Amazon Redshift tab"""
    st.markdown("## 🤝 Data Sharing - Amazon Redshift")
    st.markdown("*Securely share access to live data across clusters, workgroups, AWS accounts, and regions*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What is Redshift Data Sharing?
    Amazon Redshift data sharing enables secure sharing of live data across:
    - **Cross-Cluster**: Share between provisioned clusters
    - **Cross-Account**: Share across different AWS accounts
    - **Cross-Region**: Share data across AWS regions  
    - **Serverless Integration**: Share with Redshift Serverless workgroups
    - **Zero-Copy**: No data movement or duplication required
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ Redshift Data Sharing Architecture")
    common.mermaid(create_redshift_sharing_architecture(), height=700)
    
    # Interactive Data Sharing Builder
    st.markdown("#### 🔧 Interactive Data Sharing Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Producer Configuration")
        producer_cluster = st.text_input("Producer Cluster ID", value="data-warehouse-prod")
        producer_account = st.text_input("Producer Account ID", value="123456789012")
        
        tables_to_share = st.multiselect("Tables to Share", [
            "sales.fact_orders", "sales.dim_customers", "sales.dim_products",
            "finance.monthly_revenue", "marketing.campaign_metrics"
        ], default=["sales.fact_orders", "sales.dim_customers"])
        
        share_name = st.text_input("Data Share Name", value="sales-analytics-share")
    
    with col2:
        st.markdown("##### Consumer Configuration")
        consumer_type = st.selectbox("Consumer Type", [
            "Same Account Cluster", "Cross-Account Cluster", "Cross-Region Cluster", "Serverless Workgroup"
        ])
        
        if consumer_type == "Cross-Account Cluster":
            consumer_account = st.text_input("Consumer Account ID", value="987654321098")
        else:
            consumer_account = producer_account
            
        consumer_cluster = st.text_input("Consumer Cluster/Workgroup", value="analytics-cluster")
        database_alias = st.text_input("Database Alias", value="shared_sales_data")
    
    # Generate sharing configuration
    sharing_config = generate_sharing_config(
        producer_cluster, producer_account, tables_to_share, share_name,
        consumer_type, consumer_account, consumer_cluster, database_alias
    )
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📋 Data Sharing Configuration
    **Share Name**: {share_name}  
    **Producer**: {producer_cluster} ({producer_account})  
    **Consumer**: {consumer_cluster} ({consumer_account})  
    **Tables Shared**: {len(tables_to_share)} tables  
    **Estimated Setup Time**: {sharing_config['setup_time']}  
    **Monthly Cost**: {sharing_config['estimated_cost']}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data sharing use cases
    st.markdown("#### 🎯 Common Data Sharing Use Cases")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏭 ETL Workload Isolation
        - **Central ETL Cluster**: Process and transform data
        - **Separate BI Clusters**: Dedicated analytics workloads  
        - **Cost Optimization**: Right-size compute for each use case
        - **Performance Isolation**: Prevent query interference
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🤝 Cross-Team Collaboration
        - **Department Isolation**: Separate billing and access
        - **Shared Insights**: Common data across business units
        - **Governance**: Centralized data management
        - **Self-Service**: Teams query data independently
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🌍 Multi-Region Analytics  
        - **Global Data Access**: Share across regions
        - **Disaster Recovery**: Backup analytics capability
        - **Local Processing**: Reduce network latency
        - **Compliance**: Keep data in required regions
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Sample sharing metrics
    st.markdown("#### 📊 Data Sharing Metrics Dashboard")
    
    # Generate sample metrics
    sharing_metrics = generate_sharing_metrics()
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Share usage over time
        fig_usage = px.line(
            sharing_metrics['usage_trend'],
            x='date',
            y='queries_per_day',
            title="Data Share Usage Trend",
            markers=True
        )
        fig_usage.update_traces(line_color=AWS_COLORS['primary'])
        fig_usage.update_layout(height=400)
        st.plotly_chart(fig_usage, use_container_width=True)
    
    with col2:
        # Consumer activity
        fig_consumers = px.bar(
            x=list(sharing_metrics['consumer_activity'].keys()),
            y=list(sharing_metrics['consumer_activity'].values()),
            title="Queries by Consumer",
            color=list(sharing_metrics['consumer_activity'].values()),
            color_continuous_scale=['lightblue', AWS_COLORS['primary']]
        )
        fig_consumers.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_consumers, use_container_width=True)
    
    # Sharing permissions matrix
    st.markdown("#### 🔐 Data Sharing Permissions Matrix")
    
    permissions_df = create_sharing_permissions_matrix()
    st.dataframe(permissions_df, use_container_width=True)
    
    # Code examples
    st.markdown("#### 💻 Redshift Data Sharing Implementation")
    
    tab1, tab2, tab3 = st.tabs(["Create Data Share", "Consumer Setup", "Monitoring & Management"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Amazon Redshift Data Sharing - Producer Setup

-- 1. Create a data share on the producer cluster
CREATE DATASHARE sales_analytics_share;

-- 2. Add schemas to the data share
ALTER DATASHARE sales_analytics_share 
ADD SCHEMA sales;

ALTER DATASHARE sales_analytics_share 
ADD SCHEMA marketing;

-- 3. Add specific tables to the data share
ALTER DATASHARE sales_analytics_share 
ADD TABLE sales.fact_orders;

ALTER DATASHARE sales_analytics_share 
ADD TABLE sales.dim_customers;

ALTER DATASHARE sales_analytics_share 
ADD TABLE sales.dim_products;

ALTER DATASHARE sales_analytics_share 
ADD TABLE marketing.campaign_metrics;

-- 4. Add views to the data share (optional)
ALTER DATASHARE sales_analytics_share 
ADD TABLE sales.monthly_sales_summary;  -- This is actually a view

-- 5. Grant usage on the data share to consumer account
GRANT USAGE ON DATASHARE sales_analytics_share 
TO ACCOUNT '987654321098';

-- Alternative: Grant to a specific cluster in same account
-- GRANT USAGE ON DATASHARE sales_analytics_share 
-- TO NAMESPACE 'guid-of-consumer-cluster';

-- 6. View current data shares (producer perspective)
SELECT 
    share_name,
    share_owner,
    created_date,
    is_publicaccessible,
    share_acl,
    producer_namespace
FROM svv_datashares
WHERE share_owner = current_user;

-- 7. View what objects are included in a data share
SELECT 
    share_name,
    object_type,
    object_name,
    include_new
FROM svv_datashare_objects
WHERE share_name = 'sales_analytics_share';

-- Python code to create data share programmatically
import boto3

def create_redshift_data_share():
    """Create and configure a Redshift data share"""
    
    redshift_client = boto3.client('redshift-data')
    
    # Connection parameters
    cluster_id = 'data-warehouse-prod'
    database = 'analytics'
    
    # 1. Create the data share
    create_share_sql = """
    CREATE DATASHARE sales_analytics_share;
    """
    
    response = redshift_client.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        Sql=create_share_sql
    )
    
    print(f"Create share query ID: {response['Id']}")
    
    # 2. Add objects to the share
    add_objects_sql = """
    ALTER DATASHARE sales_analytics_share ADD SCHEMA sales;
    ALTER DATASHARE sales_analytics_share ADD TABLE sales.fact_orders;
    ALTER DATASHARE sales_analytics_share ADD TABLE sales.dim_customers;
    ALTER DATASHARE sales_analytics_share ADD TABLE sales.dim_products;
    """
    
    response = redshift_client.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        Sql=add_objects_sql
    )
    
    print(f"Add objects query ID: {response['Id']}")
    
    # 3. Grant access to consumer account
    grant_access_sql = """
    GRANT USAGE ON DATASHARE sales_analytics_share 
    TO ACCOUNT '987654321098';
    """
    
    response = redshift_client.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        Sql=grant_access_sql
    )
    
    print(f"Grant access query ID: {response['Id']}")
    
    return "Data share created successfully"

# Advanced data share with row-level security
def create_secure_data_share():
    """Create data share with row-level security"""
    
    # First, create a view with row-level security
    create_secure_view_sql = """
    -- Create a secure view that filters data based on user
    CREATE VIEW sales.secure_customer_orders AS
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount,
        product_category
    FROM sales.fact_orders
    WHERE 
        -- Only show orders from last 2 years for shared access
        order_date >= DATEADD(year, -2, GETDATE())
        -- Filter out sensitive product categories
        AND product_category NOT IN ('Internal Testing', 'Employee Purchases')
        -- Mask sensitive amount information for high-value orders
        AND (total_amount <= 10000 OR current_user() = 'admin');
    """
    
    # Add the secure view to data share instead of raw table
    add_secure_view_sql = """
    ALTER DATASHARE sales_analytics_share 
    ADD TABLE sales.secure_customer_orders;
    """
    
    return create_secure_view_sql, add_secure_view_sql

# Monitor data share usage
def monitor_data_share_usage():
    """Monitor data share usage and performance"""
    
    monitor_sql = """
    -- Check data share usage statistics
    SELECT 
        ds.share_name,
        ds.producer_namespace,
        COUNT(DISTINCT dsu.consumer_namespace) as consumer_count,
        SUM(dsu.queries_run) as total_queries,
        MAX(dsu.last_query_time) as last_access_time
    FROM svv_datashares ds
    LEFT JOIN svv_datashare_usage dsu ON ds.share_name = dsu.share_name
    WHERE ds.share_owner = current_user()
    GROUP BY ds.share_name, ds.producer_namespace
    ORDER BY total_queries DESC;
    
    -- Check which consumers are most active
    SELECT 
        consumer_namespace,
        consumer_account,
        share_name,
        queries_run,
        last_query_time,
        bytes_transferred
    FROM svv_datashare_usage
    WHERE share_name = 'sales_analytics_share'
    ORDER BY queries_run DESC;
    
    -- Monitor shared object access patterns
    SELECT 
        object_name,
        object_type,
        COUNT(*) as access_count,
        MAX(query_start_time) as last_accessed
    FROM stl_query sq
    JOIN svv_datashare_objects sdo ON sq.querytxt LIKE '%' || sdo.object_name || '%'
    WHERE sdo.share_name = 'sales_analytics_share'
        AND sq.starttime >= DATEADD(day, -7, GETDATE())
    GROUP BY object_name, object_type
    ORDER BY access_count DESC;
    """
    
    return monitor_sql

# Execute the data share creation
if __name__ == "__main__":
    print("🚀 Creating Redshift Data Share...")
    
    # Create the basic data share
    result = create_redshift_data_share()
    print(result)
    
    # Create secure views
    secure_view_sql, add_view_sql = create_secure_data_share()
    print("Secure view SQL prepared")
    
    # Set up monitoring
    monitoring_sql = monitor_data_share_usage()
    print("Monitoring queries prepared")
    
    print("✅ Data share setup complete!")
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Amazon Redshift Data Sharing - Consumer Setup

-- 1. View available data shares (consumer account)
SELECT 
    share_name,
    producer_account,
    producer_namespace,
    share_status,
    created_date
FROM svv_datashares
WHERE share_acl LIKE '%consumer%' OR producer_account != current_account();

-- 2. Create database from shared data
CREATE DATABASE shared_sales_data 
FROM DATASHARE sales_analytics_share 
OF ACCOUNT '123456789012' 
NAMESPACE 'producer-cluster-guid';

-- Alternative: Create from same-account share
-- CREATE DATABASE shared_sales_data 
-- FROM DATASHARE sales_analytics_share 
-- OF NAMESPACE 'producer-cluster-guid';

-- 3. Grant permissions to users/groups in consumer cluster
GRANT USAGE ON DATABASE shared_sales_data TO GROUP analytics_team;
GRANT USAGE ON SCHEMA shared_sales_data.sales TO GROUP analytics_team;
GRANT SELECT ON ALL TABLES IN SCHEMA shared_sales_data.sales TO GROUP analytics_team;

-- More specific permissions
GRANT SELECT ON shared_sales_data.sales.fact_orders TO USER analyst1;
GRANT SELECT ON shared_sales_data.sales.dim_customers TO GROUP marketing_team;

-- 4. Query shared data
SELECT 
    c.customer_name,
    c.customer_segment,
    SUM(o.total_amount) as total_purchases,
    COUNT(o.order_id) as order_count
FROM shared_sales_data.sales.fact_orders o
JOIN shared_sales_data.sales.dim_customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
GROUP BY c.customer_name, c.customer_segment
ORDER BY total_purchases DESC
LIMIT 100;

-- 5. Create local views based on shared data
CREATE VIEW local_views.customer_summary AS
SELECT 
    customer_id,
    customer_name,
    customer_segment,
    registration_date,
    last_order_date,
    total_lifetime_value
FROM shared_sales_data.sales.dim_customers
WHERE customer_segment IN ('Premium', 'Enterprise');

-- 6. Join shared data with local data
SELECT 
    shared.customer_name,
    shared.order_date,
    shared.total_amount,
    local.customer_satisfaction_score,
    local.support_tickets_count
FROM shared_sales_data.sales.fact_orders shared
JOIN local_analytics.customer_metrics local 
    ON shared.customer_id = local.customer_id
WHERE shared.order_date >= DATEADD(month, -3, GETDATE());

-- Python code for consumer setup
import boto3
import time

def setup_data_share_consumer():
    """Set up data share access on consumer cluster"""
    
    redshift_client = boto3.client('redshift-data')
    
    # Consumer cluster connection
    consumer_cluster_id = 'analytics-cluster'
    consumer_database = 'analytics'
    
    # 1. Check available data shares
    list_shares_sql = """
    SELECT 
        share_name,
        producer_account,
        producer_namespace,
        share_status
    FROM svv_datashares
    WHERE producer_account = '123456789012';
    """
    
    response = redshift_client.execute_statement(
        ClusterIdentifier=consumer_cluster_id,
        Database=consumer_database,
        Sql=list_shares_sql
    )
    
    # Wait for query to complete and get results
    query_id = response['Id']
    time.sleep(5)  # Wait for query completion
    
    results = redshift_client.get_statement_result(Id=query_id)
    print("Available data shares:")
    for record in results['Records']:
        print(f"  {record[0]['stringValue']} from {record[1]['stringValue']}")
    
    # 2. Create database from data share
    create_db_sql = """
    CREATE DATABASE shared_sales_data 
    FROM DATASHARE sales_analytics_share 
    OF ACCOUNT '123456789012' 
    NAMESPACE 'arn:aws:redshift:us-west-2:123456789012:namespace:guid-here';
    """
    
    response = redshift_client.execute_statement(
        ClusterIdentifier=consumer_cluster_id,
        Database=consumer_database,
        Sql=create_db_sql
    )
    
    print(f"Create database query ID: {response['Id']}")
    
    # 3. Set up permissions
    permissions_sql = """
    GRANT USAGE ON DATABASE shared_sales_data TO GROUP analysts;
    GRANT USAGE ON SCHEMA shared_sales_data.sales TO GROUP analysts;
    GRANT SELECT ON ALL TABLES IN SCHEMA shared_sales_data.sales TO GROUP analysts;
    """
    
    response = redshift_client.execute_statement(
        ClusterIdentifier=consumer_cluster_id,
        Database=consumer_database,
        Sql=permissions_sql
    )
    
    print(f"Permissions setup query ID: {response['Id']}")
    
    return "Consumer setup completed"

def create_analytics_views():
    """Create analytical views using shared data"""
    
    views_sql = [
        # Customer segmentation view
        """
        CREATE VIEW analytics.customer_segments AS
        SELECT 
            customer_segment,
            COUNT(*) as customer_count,
            AVG(total_lifetime_value) as avg_lifetime_value,
            SUM(total_purchases) as segment_revenue
        FROM (
            SELECT 
                c.customer_id,
                c.customer_segment,
                c.total_lifetime_value,
                SUM(o.total_amount) as total_purchases
            FROM shared_sales_data.sales.dim_customers c
            LEFT JOIN shared_sales_data.sales.fact_orders o 
                ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.customer_segment, c.total_lifetime_value
        )
        GROUP BY customer_segment;
        """,
        
        # Monthly sales trend view
        """
        CREATE VIEW analytics.monthly_sales_trend AS
        SELECT 
            DATE_TRUNC('month', order_date) as month,
            COUNT(*) as order_count,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM shared_sales_data.sales.fact_orders
        WHERE order_date >= DATEADD(year, -2, GETDATE())
        GROUP BY DATE_TRUNC('month', order_date)
        ORDER BY month;
        """,
        
        # Product performance view
        """
        CREATE VIEW analytics.product_performance AS
        SELECT 
            p.product_category,
            p.product_name,
            COUNT(o.order_id) as sales_count,
            SUM(o.quantity) as units_sold,
            SUM(o.total_amount) as revenue,
            AVG(o.total_amount) as avg_sale_amount
        FROM shared_sales_data.sales.fact_orders o
        JOIN shared_sales_data.sales.dim_products p 
            ON o.product_id = p.product_id
        WHERE o.order_date >= DATEADD(month, -12, GETDATE())
        GROUP BY p.product_category, p.product_name
        HAVING COUNT(o.order_id) >= 10  -- Only products with meaningful sales
        ORDER BY revenue DESC;
        """
    ]
    
    return views_sql

def monitor_consumer_usage():
    """Monitor data share usage from consumer perspective"""
    
    monitoring_queries = {
        'query_performance': """
        -- Check performance of queries against shared data
        SELECT 
            userid,
            query,
            starttime,
            endtime,
            DATEDIFF(seconds, starttime, endtime) as duration_seconds,
            rows_returned,
            bytes_scanned
        FROM stl_query
        WHERE querytxt LIKE '%shared_sales_data%'
            AND starttime >= DATEADD(day, -7, GETDATE())
        ORDER BY duration_seconds DESC
        LIMIT 20;
        """,
        
        'data_freshness': """
        -- Check data freshness in shared tables
        SELECT 
            'fact_orders' as table_name,
            MAX(order_date) as latest_data_date,
            DATEDIFF(hour, MAX(order_date), GETDATE()) as hours_behind
        FROM shared_sales_data.sales.fact_orders
        UNION ALL
        SELECT 
            'dim_customers' as table_name,
            MAX(last_updated) as latest_data_date,
            DATEDIFF(hour, MAX(last_updated), GETDATE()) as hours_behind
        FROM shared_sales_data.sales.dim_customers;
        """,
        
        'access_patterns': """
        -- Analyze access patterns for optimization
        SELECT 
            DATE_TRUNC('day', starttime) as access_date,
            COUNT(*) as query_count,
            COUNT(DISTINCT userid) as unique_users,
            AVG(DATEDIFF(seconds, starttime, endtime)) as avg_duration
        FROM stl_query
        WHERE querytxt LIKE '%shared_sales_data%'
            AND starttime >= DATEADD(month, -1, GETDATE())
        GROUP BY DATE_TRUNC('day', starttime)
        ORDER BY access_date DESC;
        """
    }
    
    return monitoring_queries

# Execute consumer setup
if __name__ == "__main__":
    print("🚀 Setting up Data Share Consumer...")
    
    # Set up consumer access
    consumer_result = setup_data_share_consumer()
    print(consumer_result)
    
    # Create analytical views
    print("\\n📊 Creating analytical views...")
    analytics_views = create_analytics_views()
    for i, view_sql in enumerate(analytics_views, 1):
        print(f"View {i} SQL prepared")
    
    # Set up monitoring
    print("\\n📈 Setting up monitoring...")
    monitoring_queries = monitor_consumer_usage()
    print(f"Prepared {len(monitoring_queries)} monitoring queries")
    
    print("\\n✅ Consumer setup complete!")
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Redshift Data Sharing Monitoring and Management
import boto3
import json
import pandas as pd
from datetime import datetime, timedelta

class RedshiftDataShareManager:
    """Comprehensive data share monitoring and management"""
    
    def __init__(self, cluster_id, database='dev'):
        self.redshift_client = boto3.client('redshift-data')
        self.redshift_mgmt = boto3.client('redshift')
        self.cloudwatch = boto3.client('cloudwatch')
        self.cluster_id = cluster_id
        self.database = database
        
    def get_data_share_overview(self):
        """Get comprehensive overview of all data shares"""
        
        overview_sql = """
        -- Producer shares overview
        SELECT 
            'PRODUCER' as role,
            share_name,
            producer_namespace,
            created_date,
            is_publicaccessible,
            share_acl
        FROM svv_datashares
        WHERE share_owner = current_user()
        
        UNION ALL
        
        -- Consumer shares overview  
        SELECT 
            'CONSUMER' as role,
            share_name,
            producer_namespace,
            created_date,
            false as is_publicaccessible,
            '' as share_acl
        FROM svv_datashares
        WHERE share_owner != current_user()
        ORDER BY role, share_name;
        """
        
        response = self.redshift_client.execute_statement(
            ClusterIdentifier=self.cluster_id,
            Database=self.database,
            Sql=overview_sql
        )
        
        return self._get_query_results(response['Id'])
    
    def analyze_share_usage(self, share_name=None):
        """Analyze usage patterns for data shares"""
        
        if share_name:
            where_clause = f"WHERE share_name = '{share_name}'"
        else:
            where_clause = ""
        
        usage_sql = f"""
        SELECT 
            share_name,
            consumer_namespace,
            consumer_account,
            queries_run,
            bytes_transferred,
            last_query_time,
            DATEDIFF(day, last_query_time, GETDATE()) as days_since_last_access
        FROM svv_datashare_usage
        {where_clause}
        ORDER BY queries_run DESC;
        """
        
        response = self.redshift_client.execute_statement(
            ClusterIdentifier=self.cluster_id,
            Database=self.database,
            Sql=usage_sql
        )
        
        return self._get_query_results(response['Id'])
    
    def monitor_performance_impact(self):
        """Monitor performance impact of data sharing"""
        
        performance_sql = """
        -- Query performance for shared vs local data
        WITH shared_queries AS (
            SELECT 
                'SHARED' as data_type,
                COUNT(*) as query_count,
                AVG(DATEDIFF(seconds, starttime, endtime)) as avg_duration,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY DATEDIFF(seconds, starttime, endtime)) as p95_duration,
                AVG(rows) as avg_rows_returned
            FROM stl_query
            WHERE querytxt LIKE '%shared_%'
                AND starttime >= DATEADD(day, -7, GETDATE())
                AND userid != 1  -- Exclude system queries
        ),
        local_queries AS (
            SELECT 
                'LOCAL' as data_type,
                COUNT(*) as query_count,
                AVG(DATEDIFF(seconds, starttime, endtime)) as avg_duration,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY DATEDIFF(seconds, starttime, endtime)) as p95_duration,
                AVG(rows) as avg_rows_returned
            FROM stl_query
            WHERE querytxt NOT LIKE '%shared_%'
                AND starttime >= DATEADD(day, -7, GETDATE())
                AND userid != 1
        )
        SELECT * FROM shared_queries
        UNION ALL
        SELECT * FROM local_queries;
        """
        
        response = self.redshift_client.execute_statement(
            ClusterIdentifier=self.cluster_id,
            Database=self.database,
            Sql=performance_sql
        )
        
        return self._get_query_results(response['Id'])
    
    def audit_share_permissions(self):
        """Audit data share permissions and access controls"""
        
        audit_sql = """
        -- Comprehensive permission audit
        SELECT 
            ds.share_name,
            ds.producer_namespace,
            ds.share_acl,
            dso.object_type,
            dso.object_name,
            CASE 
                WHEN dso.object_type = 'table' THEN 'Full table access'
                WHEN dso.object_type = 'view' THEN 'Filtered access'
                ELSE dso.object_type
            END as access_level,
            -- Check for sensitive data indicators
            CASE 
                WHEN LOWER(dso.object_name) LIKE '%customer%' 
                  OR LOWER(dso.object_name) LIKE '%personal%'
                  OR LOWER(dso.object_name) LIKE '%financial%' THEN 'SENSITIVE'
                ELSE 'STANDARD'
            END as data_sensitivity
        FROM svv_datashares ds
        JOIN svv_datashare_objects dso ON ds.share_name = dso.share_name
        WHERE ds.share_owner = current_user()
        ORDER BY data_sensitivity DESC, ds.share_name, dso.object_name;
        """
        
        response = self.redshift_client.execute_statement(
            ClusterIdentifier=self.cluster_id,
            Database=self.database,
            Sql=audit_sql
        )
        
        return self._get_query_results(response['Id'])
    
    def optimize_shared_queries(self):
        """Identify optimization opportunities for shared data queries"""
        
        optimization_sql = """
        -- Find slow queries against shared data
        SELECT 
            userid,
            querytxt,
            starttime,
            DATEDIFF(seconds, starttime, endtime) as duration_seconds,
            rows,
            bytes,
            -- Suggest optimizations based on query patterns
            CASE 
                WHEN querytxt LIKE '%ORDER BY%' AND querytxt NOT LIKE '%LIMIT%' 
                    THEN 'Consider adding LIMIT clause'
                WHEN querytxt LIKE '%SELECT *%' 
                    THEN 'Select only required columns'
                WHEN querytxt LIKE '%WHERE%' AND querytxt LIKE '%OR%'
                    THEN 'Consider using UNION for OR conditions'
                WHEN DATEDIFF(seconds, starttime, endtime) > 300
                    THEN 'Query timeout risk - consider breaking into smaller queries'
                ELSE 'Query appears optimized'
            END as optimization_suggestion
        FROM stl_query
        WHERE querytxt LIKE '%shared_%'
            AND starttime >= DATEADD(day, -7, GETDATE())
            AND userid != 1
            AND DATEDIFF(seconds, starttime, endtime) > 30  -- Focus on slower queries
        ORDER BY duration_seconds DESC
        LIMIT 50;
        """
        
        response = self.redshift_client.execute_statement(
            ClusterIdentifier=self.cluster_id,
            Database=self.database,
            Sql=optimization_sql
        )
        
        return self._get_query_results(response['Id'])
    
    def manage_share_lifecycle(self, share_name, action, **kwargs):
        """Manage data share lifecycle operations"""
        
        if action == 'ADD_TABLE':
            sql = f"ALTER DATASHARE {share_name} ADD TABLE {kwargs['table_name']};"
            
        elif action == 'REMOVE_TABLE':
            sql = f"ALTER DATASHARE {share_name} REMOVE TABLE {kwargs['table_name']};"
            
        elif action == 'GRANT_ACCESS':
            if 'account_id' in kwargs:
                sql = f"GRANT USAGE ON DATASHARE {share_name} TO ACCOUNT '{kwargs['account_id']}';"
            else:
                sql = f"GRANT USAGE ON DATASHARE {share_name} TO NAMESPACE '{kwargs['namespace']}';"
                
        elif action == 'REVOKE_ACCESS':
            if 'account_id' in kwargs:
                sql = f"REVOKE USAGE ON DATASHARE {share_name} FROM ACCOUNT '{kwargs['account_id']}';"
            else:
                sql = f"REVOKE USAGE ON DATASHARE {share_name} FROM NAMESPACE '{kwargs['namespace']}';"
                
        elif action == 'DROP_SHARE':
            sql = f"DROP DATASHARE {share_name};"
            
        else:
            raise ValueError(f"Unknown action: {action}")
        
        response = self.redshift_client.execute_statement(
            ClusterIdentifier=self.cluster_id,
            Database=self.database,
            Sql=sql
        )
        
        return response['Id']
    
    def generate_cost_analysis(self):
        """Generate cost analysis for data sharing"""
        
        cost_sql = """
        -- Cost analysis based on usage patterns
        SELECT 
            share_name,
            consumer_account,
            queries_run,
            bytes_transferred,
            -- Estimate compute cost impact
            CASE 
                WHEN queries_run > 10000 THEN 'HIGH_IMPACT'
                WHEN queries_run > 1000 THEN 'MEDIUM_IMPACT'
                ELSE 'LOW_IMPACT'
            END as compute_impact,
            -- Estimate network cost (cross-region)
            CASE 
                WHEN bytes_transferred > 1073741824 THEN 'SIGNIFICANT_NETWORK_COST'  -- 1GB
                WHEN bytes_transferred > 107374182 THEN 'MODERATE_NETWORK_COST'     -- 100MB
                ELSE 'MINIMAL_NETWORK_COST'
            END as network_cost_impact,
            last_query_time
        FROM svv_datashare_usage
        ORDER BY bytes_transferred DESC, queries_run DESC;
        """
        
        response = self.redshift_client.execute_statement(
            ClusterIdentifier=self.cluster_id,
            Database=self.database,
            Sql=cost_sql
        )
        
        return self._get_query_results(response['Id'])
    
    def setup_monitoring_alerts(self):
        """Set up CloudWatch alarms for data sharing metrics"""
        
        # Create custom metric for share usage
        self.cloudwatch.put_metric_data(
            Namespace='RedshiftDataSharing',
            MetricData=[
                {
                    'MetricName': 'ShareQueriesPerHour',
                    'Dimensions': [
                        {
                            'Name': 'ClusterIdentifier',
                            'Value': self.cluster_id
                        }
                    ],
                    'Unit': 'Count',
                    'Value': 0  # This would be populated by regular monitoring
                }
            ]
        )
        
        # Create alarm for excessive usage
        self.cloudwatch.put_metric_alarm(
            AlarmName=f'RedshiftDataShare-HighUsage-{self.cluster_id}',
            ComparisonOperator='GreaterThanThreshold',
            EvaluationPeriods=2,
            MetricName='ShareQueriesPerHour',
            Namespace='RedshiftDataSharing',
            Period=3600,  # 1 hour
            Statistic='Sum',
            Threshold=1000.0,
            ActionsEnabled=True,
            AlarmActions=[
                'arn:aws:sns:us-west-2:123456789012:redshift-alerts'
            ],
            AlarmDescription='Alarm when data share usage is unusually high',
            Dimensions=[
                {
                    'Name': 'ClusterIdentifier',
                    'Value': self.cluster_id
                }
            ]
        )
        
        return "Monitoring alerts configured"
    
    def _get_query_results(self, query_id):
        """Helper method to retrieve query results"""
        import time
        
        # Wait for query to complete
        max_wait = 30
        wait_count = 0
        
        while wait_count < max_wait:
            status_response = self.redshift_client.describe_statement(Id=query_id)
            status = status_response['Status']
            
            if status == 'FINISHED':
                break
            elif status in ['FAILED', 'ABORTED']:
                raise Exception(f"Query failed with status: {status}")
            
            time.sleep(1)
            wait_count += 1
        
        # Get results
        results = self.redshift_client.get_statement_result(Id=query_id)
        return results
    
    def generate_governance_report(self):
        """Generate comprehensive governance report"""
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'cluster_id': self.cluster_id,
            'share_overview': self.get_data_share_overview(),
            'usage_analysis': self.analyze_share_usage(),
            'performance_impact': self.monitor_performance_impact(),
            'permission_audit': self.audit_share_permissions(),
            'cost_analysis': self.generate_cost_analysis(),
            'optimization_opportunities': self.optimize_shared_queries()
        }
        
        return report

# Usage example
if __name__ == "__main__":
    # Initialize manager
    share_manager = RedshiftDataShareManager('my-redshift-cluster')
    
    print("🚀 Starting comprehensive data share analysis...")
    
    # Generate full governance report
    governance_report = share_manager.generate_governance_report()
    
    # Save report
    with open(f"data_share_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
        json.dump(governance_report, f, indent=2, default=str)
    
    # Set up monitoring
    print("📊 Setting up monitoring alerts...")
    share_manager.setup_monitoring_alerts()
    
    print("✅ Data sharing monitoring and management setup complete!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

# Helper functions for data generation and calculations
def generate_config_rule(rule_name, resource_type, trigger_type, severity):
    """Generate Config rule configuration"""
    return {
        'rule_name': rule_name,
        'resource_type': resource_type,
        'trigger_type': trigger_type,
        'severity': severity,
        'estimated_cost': '$15-50/month',
        'compliance_scope': 'Account-wide'
    }

def estimate_resources(resource_type):
    """Estimate number of resources for evaluation"""
    estimates = {
        "AWS::S3::Bucket": "50-200 buckets",
        "AWS::EC2::Instance": "100-500 instances", 
        "AWS::IAM::Role": "200-1000 roles",
        "AWS::RDS::DBInstance": "10-50 instances",
        "AWS::Lambda::Function": "100-1000 functions"
    }
    return estimates.get(resource_type, "Unknown")

def generate_compliance_data():
    """Generate sample compliance data"""
    dates = pd.date_range(start='2024-06-15', end='2024-07-14', freq='D')
    
    return {
        'status_counts': {
            'COMPLIANT': 156,
            'NON_COMPLIANT': 23,
            'NOT_APPLICABLE': 8
        },
        'trends': pd.DataFrame({
            'date': dates,
            'compliance_percentage': np.random.normal(83, 3, len(dates))
        })
    }

def generate_macie_findings(data_types, sensitivity_level):
    """Generate sample Macie findings"""
    base_findings = 45 if sensitivity_level == "HIGH" else 78 if sensitivity_level == "MEDIUM" else 124
    
    return {
        'buckets_scanned': 12,
        'objects_analyzed': random.randint(50000, 200000),
        'sensitive_files': base_findings,
        'high_risk_findings': random.randint(5, 15),
        'estimated_cost': random.uniform(25.50, 89.75),
        'findings_by_severity': {
            'HIGH': random.randint(5, 15),
            'MEDIUM': random.randint(15, 35),
            'LOW': random.randint(25, 45)
        },
        'data_types_distribution': {
            'Credit Card Numbers': random.randint(8, 25),
            'Social Security Numbers': random.randint(5, 18),
            'Email Addresses': random.randint(15, 40),
            'Phone Numbers': random.randint(10, 30),
            'Banking Information': random.randint(3, 12)
        }
    }

def create_findings_dataframe(discovery_results):
    """Create detailed findings dataframe"""
    findings_data = []
    severities = ['HIGH', 'MEDIUM', 'LOW']
    buckets = ['customer-data-prod', 'employee-records', 'financial-reports', 'application-logs']
    
    for i in range(15):  # Generate 15 sample findings
        findings_data.append({
            'Finding ID': f'finding-{i+1:03d}',
            'Severity': random.choice(severities),
            'S3 Bucket': random.choice(buckets),
            'S3 Object': f'data/file_{i+1}.csv',
            'Data Type': random.choice(['Credit Card', 'SSN', 'Email', 'Phone', 'Bank Account']),
            'Occurrences': random.randint(1, 50),
            'Risk Score': random.randint(1, 100)
        })
    
    return pd.DataFrame(findings_data)

def generate_sharing_config(producer_cluster, producer_account, tables_to_share, share_name,
                           consumer_type, consumer_account, consumer_cluster, database_alias):
    """Generate data sharing configuration"""
    setup_times = {
        "Same Account Cluster": "15 minutes",
        "Cross-Account Cluster": "30 minutes",
        "Cross-Region Cluster": "45 minutes",
        "Serverless Workgroup": "20 minutes"
    }
    
    costs = {
        "Same Account Cluster": "No additional cost",
        "Cross-Account Cluster": "No additional cost",
        "Cross-Region Cluster": "$0.02/GB transfer",
        "Serverless Workgroup": "No additional cost"
    }
    
    return {
        'setup_time': setup_times.get(consumer_type, "30 minutes"),
        'estimated_cost': costs.get(consumer_type, "No additional cost")
    }

def generate_sharing_metrics():
    """Generate sample sharing metrics"""
    dates = pd.date_range(start='2024-06-15', end='2024-07-14', freq='D')
    
    return {
        'usage_trend': pd.DataFrame({
            'date': dates,
            'queries_per_day': np.random.poisson(150, len(dates))
        }),
        'consumer_activity': {
            'analytics-cluster': 450,
            'bi-workgroup': 320,
            'dev-cluster': 180,
            'marketing-cluster': 95
        }
    }

def create_sharing_permissions_matrix():
    """Create sharing permissions matrix"""
    data = {
        'Data Share': ['sales-analytics-share', 'marketing-data-share', 'finance-reports-share'],
        'Producer Account': ['123456789012', '123456789012', '555666777888'],
        'Consumer Account': ['987654321098', '111222333444', '123456789012'],
        'Access Level': ['Read-Only', 'Read-Only', 'Read-Only'],
        'Tables Shared': [4, 6, 2],
        'Last Accessed': ['2024-07-14', '2024-07-13', '2024-07-12'],
        'Query Count (7d)': [1250, 890, 340]
    }
    return pd.DataFrame(data)

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
    # 🔒 AWS Data Security & Governance
    <div class='info-box'>
    Master AWS data security and governance services including AWS Config for compliance monitoring, Amazon Macie for sensitive data discovery, and Redshift data sharing for secure collaboration.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs([
        "📊 AWS Config",
        "🔍 Amazon Macie", 
        "🤝 Data Sharing – Amazon Redshift"
    ])
    
    with tab1:
        aws_config_tab()
    
    with tab2:
        amazon_macie_tab()
    
    with tab3:
        data_sharing_redshift_tab()
    
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
