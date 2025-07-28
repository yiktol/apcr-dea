
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
    page_title="AWS Monitoring & Logging Services",
    page_icon="📊",
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
            border-left: 5px solid {AWS_COLORS['primary']};
        }}

        .service-overview {{
            background: linear-gradient(135deg, #FF9900 0%, #4B9EDB 100%);
            padding: 20px;
            border-radius: 15px;
            color: white;
            margin: 20px 0;
        }}
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    common.initialize_session_state()
    if 'session_started' not in st.session_state:
        st.session_state.session_started = True
        st.session_state.trails_configured = []
        st.session_state.logs_analyzed = 0
        st.session_state.dashboards_created = 0

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🔍 Amazon CloudTrail - API call tracking and audit logging
            - 🏞️ Amazon CloudTrail Lake - SQL-based queries on events
            - 📊 Amazon CloudWatch - Observability and monitoring
            - 📝 Amazon CloudWatch Logs - Centralized log management
            
            **Learning Objectives:**
            - Master AWS audit logging with CloudTrail
            - Learn advanced event querying with CloudTrail Lake
            - Implement comprehensive monitoring with CloudWatch
            - Centralize log management with CloudWatch Logs
            """)

def create_cloudtrail_architecture():
    """Create CloudTrail architecture diagram"""
    return """
    graph TB
        subgraph "AWS Services"
            API[AWS APIs]
            CONSOLE[AWS Console]
            CLI[AWS CLI]
            SDK[AWS SDK]
        end
        
        subgraph "CloudTrail"
            CT[CloudTrail Service]
            EVENTS[Event Processing]
        end
        
        subgraph "Storage & Analysis"
            S3[(Amazon S3<br/>Event Storage)]
            CWL[CloudWatch Logs]
            LAKE[CloudTrail Lake]
            ATHENA[Amazon Athena]
        end
        
        subgraph "Monitoring & Alerts"
            CW[CloudWatch<br/>Metrics]
            SNS[SNS<br/>Notifications]
            EVENTBRIDGE[EventBridge<br/>Rules]
        end
        
        API --> CT
        CONSOLE --> CT
        CLI --> CT
        SDK --> CT
        
        CT --> EVENTS
        EVENTS --> S3
        EVENTS --> CWL
        EVENTS --> LAKE
        
        S3 --> ATHENA
        CWL --> CW
        CT --> EVENTBRIDGE
        EVENTBRIDGE --> SNS
        
        style CT fill:#FF9900,stroke:#232F3E,color:#fff
        style S3 fill:#3FB34F,stroke:#232F3E,color:#fff
        style LAKE fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CW fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_cloudwatch_architecture():
    """Create CloudWatch architecture diagram"""
    return """
    graph TB
        subgraph "Data Sources"
            EC2[EC2 Instances]
            RDS[RDS Databases]
            LAMBDA[Lambda Functions]
            ELB[Load Balancers]
            CUSTOM[Custom Applications]
        end
        
        subgraph "CloudWatch Core"
            METRICS[Metrics Collection]
            LOGS[Logs Collection]
            EVENTS[Events & Rules]
        end
        
        subgraph "Analysis & Visualization"
            DASH[Dashboards]
            INSIGHTS[Logs Insights]
            ALARMS[CloudWatch Alarms]
        end
        
        subgraph "Actions"
            SCALING[Auto Scaling]
            SNS_NOTIFY[SNS Notifications]
            LAMBDA_ACTION[Lambda Functions]
        end
        
        EC2 --> METRICS
        RDS --> METRICS
        LAMBDA --> METRICS
        ELB --> METRICS
        CUSTOM --> METRICS
        
        EC2 --> LOGS
        LAMBDA --> LOGS
        CUSTOM --> LOGS
        
        METRICS --> DASH
        LOGS --> INSIGHTS
        METRICS --> ALARMS
        
        ALARMS --> SCALING
        ALARMS --> SNS_NOTIFY
        EVENTS --> LAMBDA_ACTION
        
        style METRICS fill:#FF9900,stroke:#232F3E,color:#fff
        style LOGS fill:#4B9EDB,stroke:#232F3E,color:#fff
        style ALARMS fill:#232F3E,stroke:#FF9900,color:#fff
    """

def cloudtrail_tab():
    """Content for Amazon CloudTrail tab"""
    st.markdown("## 🔍 Amazon CloudTrail")
    st.markdown("*Track API calls and user activity across your AWS infrastructure*")
    
    # Service overview
    st.markdown('<div class="service-overview">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What is Amazon CloudTrail?
    CloudTrail logs, continuously monitors, and retains account activity related to actions across your AWS infrastructure, 
    giving you control over storage, analysis, and remediation actions. It provides complete audit trails for compliance, 
    security analysis, and operational troubleshooting.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ CloudTrail Architecture")
    common.mermaid(create_cloudtrail_architecture(), height=600)
    
    # Key features
    st.markdown("#### ⭐ Key Features")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📝 Audit Activity
        Monitor, store, and validate activity events for authenticity. 
        Generate audit reports required by internal policies and 
        external regulations.
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔒 Security Monitoring
        Detect unauthorized access using Who, What, and When 
        information in CloudTrail Events. Respond with rules-based 
        EventBridge alerts and automated workflows.
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Operational Insights
        Analyze user and API activity patterns to optimize 
        operations and troubleshoot issues across your 
        AWS environment.
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive CloudTrail event simulator
    st.markdown("#### 🎮 CloudTrail Event Simulator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        event_source = st.selectbox("Event Source", [
            "s3.amazonaws.com", "ec2.amazonaws.com", "iam.amazonaws.com", 
            "lambda.amazonaws.com", "rds.amazonaws.com", "cloudformation.amazonaws.com"
        ])
        
        event_name = st.selectbox("Event Name", get_events_for_service(event_source))
        
    with col2:
        user_identity = st.selectbox("User Identity Type", [
            "IAMUser", "AssumedRole", "Root", "FederatedUser", "SAMLUser"
        ])
        
        aws_region = st.selectbox("AWS Region", [
            "us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"
        ])
    
    # Generate sample CloudTrail event
    sample_event = generate_cloudtrail_event(event_source, event_name, user_identity, aws_region)
    
    st.markdown("##### 📋 Generated CloudTrail Event")
    st.json(sample_event)
    
    # CloudTrail configuration examples
    st.markdown("#### 💻 CloudTrail Configuration Examples")
    
    tab1, tab2, tab3 = st.tabs(["Basic Setup", "Multi-Region Trail", "Advanced Configuration"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create a basic CloudTrail using AWS CLI
aws cloudtrail create-trail \
    --name MyCloudTrail \
    --s3-bucket-name my-cloudtrail-bucket \
    --s3-key-prefix logs/ \
    --include-global-service-events \
    --is-multi-region-trail \
    --enable-log-file-validation

# Start logging
aws cloudtrail start-logging --name MyCloudTrail

# Python Boto3 - Basic CloudTrail Setup
import boto3
import json

def create_basic_cloudtrail():
    """Create a basic CloudTrail configuration"""
    
    cloudtrail = boto3.client('cloudtrail')
    s3 = boto3.client('s3')
    
    # Create S3 bucket for CloudTrail logs
    bucket_name = 'my-company-cloudtrail-logs'
    
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Created S3 bucket: {bucket_name}")
    except Exception as e:
        print(f"Bucket creation failed: {e}")
    
    # Bucket policy for CloudTrail
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AWSCloudTrailAclCheck",
                "Effect": "Allow",
                "Principal": {"Service": "cloudtrail.amazonaws.com"},
                "Action": "s3:GetBucketAcl",
                "Resource": f"arn:aws:s3:::{bucket_name}"
            },
            {
                "Sid": "AWSCloudTrailWrite",
                "Effect": "Allow", 
                "Principal": {"Service": "cloudtrail.amazonaws.com"},
                "Action": "s3:PutObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/*",
                "Condition": {
                    "StringEquals": {"s3:x-amz-acl": "bucket-owner-full-control"}
                }
            }
        ]
    }
    
    # Apply bucket policy
    s3.put_bucket_policy(
        Bucket=bucket_name,
        Policy=json.dumps(bucket_policy)
    )
    
    # Create CloudTrail
    response = cloudtrail.create_trail(
        Name='CompanyAuditTrail',
        S3BucketName=bucket_name,
        S3KeyPrefix='AWSLogs/',
        IncludeGlobalServiceEvents=True,
        IsMultiRegionTrail=True,
        EnableLogFileValidation=True,
        Tags=[
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Purpose', 'Value': 'Security Audit'}
        ]
    )
    
    # Start logging
    cloudtrail.start_logging(Name='CompanyAuditTrail')
    
    print("CloudTrail created and started successfully")
    return response['TrailARN']

# Usage
trail_arn = create_basic_cloudtrail()
print(f"Trail ARN: {trail_arn}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Multi-Region CloudTrail with Advanced Features
import boto3
import json

def create_advanced_cloudtrail():
    """Create advanced multi-region CloudTrail with data events"""
    
    cloudtrail = boto3.client('cloudtrail')
    
    # Advanced event selectors for data events
    event_selectors = [
        {
            'ReadWriteType': 'All',
            'IncludeManagementEvents': True,
            'DataResources': [
                {
                    'Type': 'AWS::S3::Object',
                    'Values': ['arn:aws:s3:::sensitive-data-bucket/*']
                },
                {
                    'Type': 'AWS::Lambda::Function',
                    'Values': ['arn:aws:lambda:*:*:function:*']
                }
            ]
        }
    ]
    
    # Create trail with advanced configuration
    response = cloudtrail.create_trail(
        Name='AdvancedSecurityTrail',
        S3BucketName='advanced-cloudtrail-logs',
        S3KeyPrefix='SecurityLogs/',
        IncludeGlobalServiceEvents=True,
        IsMultiRegionTrail=True,
        EnableLogFileValidation=True,
        CloudWatchLogsLogGroupArn='arn:aws:logs:us-east-1:123456789012:log-group:CloudTrailLogGroup:*',
        CloudWatchLogsRoleArn='arn:aws:iam::123456789012:role/CloudTrailLogsRole',
        KMSKeyId='arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012',
        Tags=[
            {'Key': 'CriticalityLevel', 'Value': 'High'},
            {'Key': 'ComplianceRequired', 'Value': 'True'}
        ]
    )
    
    # Configure event selectors
    cloudtrail.put_event_selectors(
        TrailName='AdvancedSecurityTrail',
        EventSelectors=event_selectors
    )
    
    # Create CloudWatch alarm for suspicious activity
    cloudwatch = boto3.client('cloudwatch')
    
    cloudwatch.put_metric_alarm(
        AlarmName='UnauthorizedAPICallsAlarm',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=1,
        MetricName='UnauthorizedAPICalls',
        Namespace='CloudWatchLogMetrics',
        Period=300,
        Statistic='Sum',
        Threshold=1.0,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-east-1:123456789012:security-alerts'
        ],
        AlarmDescription='Alert on unauthorized API calls',
        Dimensions=[
            {
                'Name': 'TrailName',
                'Value': 'AdvancedSecurityTrail'
            }
        ]
    )
    
    # Start logging
    cloudtrail.start_logging(Name='AdvancedSecurityTrail')
    
    print("Advanced CloudTrail configured successfully")
    return response

# Create metric filter for failed logins
def create_security_metric_filters():
    """Create CloudWatch metric filters for security events"""
    
    logs_client = boto3.client('logs')
    
    # Metric filter for failed console logins
    logs_client.put_metric_filter(
        logGroupName='CloudTrailLogGroup',
        filterName='FailedConsoleLogins',
        filterPattern='{ ($.errorCode = "*UnauthorizedOperation") || ($.errorCode = "AccessDenied*") }',
        metricTransformations=[
            {
                'metricName': 'FailedLogins',
                'metricNamespace': 'CloudWatchLogMetrics',
                'metricValue': '1',
                'defaultValue': 0
            }
        ]
    )
    
    # Metric filter for root account usage
    logs_client.put_metric_filter(
        logGroupName='CloudTrailLogGroup',
        filterName='RootAccountUsage',
        filterPattern='{ $.userIdentity.type = "Root" && $.userIdentity.invokedBy NOT EXISTS && $.eventType != "AwsServiceEvent" }',
        metricTransformations=[
            {
                'metricName': 'RootAccountUsage',
                'metricNamespace': 'CloudWatchLogMetrics',
                'metricValue': '1',
                'defaultValue': 0
            }
        ]
    )
    
    print("Security metric filters created")

# Execute advanced setup
create_advanced_cloudtrail()
create_security_metric_filters()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Advanced CloudTrail Analysis and Alerting
import boto3
import json
from datetime import datetime, timedelta

def analyze_cloudtrail_events():
    """Analyze CloudTrail events for security insights"""
    
    cloudtrail = boto3.client('cloudtrail')
    
    # Look up recent events
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)
    
    # Analyze API call patterns
    events = cloudtrail.lookup_events(
        LookupAttributes=[
            {
                'AttributeKey': 'EventName',
                'AttributeValue': 'ConsoleLogin'
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        MaxItems=50
    )
    
    print("Recent Console Login Events:")
    for event in events['Events']:
        username = event.get('Username', 'Unknown')
        source_ip = event.get('SourceIPAddress', 'Unknown')
        event_time = event.get('EventTime')
        print(f"  {event_time}: {username} from {source_ip}")
    
    # Analyze failed API calls
    failed_events = cloudtrail.lookup_events(
        LookupAttributes=[
            {
                'AttributeKey': 'EventName',
                'AttributeValue': 'AssumeRole'
            }
        ],
        StartTime=start_time,
        EndTime=end_time
    )
    
    failure_count = 0
    for event in failed_events['Events']:
        if 'errorCode' in str(event):
            failure_count += 1
    
    print(f"Failed AssumeRole attempts in last 24h: {failure_count}")
    
    return {
        'login_events': len(events['Events']),
        'failed_assume_role': failure_count
    }

def create_custom_cloudtrail_dashboard():
    """Create CloudWatch dashboard for CloudTrail monitoring"""
    
    cloudwatch = boto3.client('cloudwatch')
    
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "properties": {
                    "metrics": [
                        ["AWS/CloudTrail", "DataEvents", "TrailName", "AdvancedSecurityTrail"],
                        [".", "ManagementEvents", ".", "."]
                    ],
                    "period": 300,
                    "stat": "Sum",
                    "region": "us-east-1",
                    "title": "CloudTrail Event Volume"
                }
            },
            {
                "type": "log",
                "properties": {
                    "query": "SOURCE '/aws/cloudtrail/CloudTrailLogGroup'\n| fields @timestamp, eventName, userIdentity.type, sourceIPAddress\n| filter eventName like /Console/\n| stats count() by userIdentity.type",
                    "region": "us-east-1",
                    "title": "Console Access by User Type"
                }
            },
            {
                "type": "metric",
                "properties": {
                    "metrics": [
                        ["CloudWatchLogMetrics", "FailedLogins"],
                        [".", "RootAccountUsage"],
                        [".", "UnauthorizedAPICalls"]
                    ],
                    "period": 300,
                    "stat": "Sum",
                    "region": "us-east-1", 
                    "title": "Security Metrics"
                }
            }
        ]
    }
    
    cloudwatch.put_dashboard(
        DashboardName='CloudTrailSecurityDashboard',
        DashboardBody=json.dumps(dashboard_body)
    )
    
    print("CloudTrail security dashboard created")

def setup_automated_response():
    """Setup automated response to security events"""
    
    # Lambda function for automated response
    lambda_function_code = """
import json
import boto3

def lambda_handler(event, context):
    """Respond to CloudTrail security alerts"""
    
    # Parse CloudWatch alarm
    message = json.loads(event['Records'][0]['Sns']['Message'])
    alarm_name = message['AlarmName']
    
    if alarm_name == 'UnauthorizedAPICallsAlarm':
        # Disable problematic IAM user
        iam = boto3.client('iam')
        
        # This would contain logic to identify and disable user
        # Based on CloudTrail event analysis
        
        print(f"Security incident detected: {alarm_name}")
        
        # Send detailed alert
        sns = boto3.client('sns')
        sns.publish(
            TopicArn='arn:aws:sns:us-east-1:123456789012:security-team',
            Subject='URGENT: Security Incident Detected',
            Message=f'Automated response triggered for {alarm_name}. Please investigate immediately.'
        )
    
    return {'statusCode': 200, 'body': 'Response executed'}
    """
    
    # EventBridge rule for real-time response
    events_client = boto3.client('events')
    
    events_client.put_rule(
        Name='CloudTrailSecurityRule',
        EventPattern=json.dumps({
            "source": ["aws.cloudtrail"],
            "detail-type": ["AWS API Call via CloudTrail"],
            "detail": {
                "eventName": ["CreateUser", "AttachUserPolicy", "CreateRole"],
                "errorCode": ["UnauthorizedOperation", "AccessDenied"]
            }
        }),
        State='ENABLED',
        Description='Respond to suspicious CloudTrail events'
    )
    
    print("Automated security response configured")

# Execute analysis and setup monitoring
analysis_results = analyze_cloudtrail_events()
create_custom_cloudtrail_dashboard()
setup_automated_response()

print(f"Analysis complete: {analysis_results}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def cloudtrail_lake_tab():
    """Content for Amazon CloudTrail Lake tab"""
    st.markdown("## 🏞️ Amazon CloudTrail Lake")
    st.markdown("*Run SQL-based queries on your CloudTrail events for advanced analysis*")
    
    # Service overview
    st.markdown('<div class="service-overview">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What is CloudTrail Lake?
    AWS CloudTrail Lake lets you run SQL-based queries on your events. It converts existing events in row-based JSON format 
    to Apache ORC format for efficient querying. Events are aggregated into immutable event data stores that you can query 
    using SQL across multiple accounts and regions.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # CloudTrail Lake architecture
    lake_architecture = """
    graph TB
        subgraph "Event Sources"
            CT[CloudTrail Events]
            CONFIG[AWS Config Items]
            AUDIT[Audit Manager Evidence]
            EXTERNAL[External Events]
        end
        
        subgraph "CloudTrail Lake"
            EDS[Event Data Store]
            ORC[Apache ORC Format]
            QUERY[SQL Query Engine]
        end
        
        subgraph "Analysis & Visualization"
            CONSOLE[Lake Console]
            ATHENA[Amazon Athena<br/>Federation]
            GLUE[AWS Glue<br/>Data Catalog]
            DASH[CloudTrail Lake<br/>Dashboards]
        end
        
        CT --> EDS
        CONFIG --> EDS
        AUDIT --> EDS
        EXTERNAL --> EDS
        
        EDS --> ORC
        ORC --> QUERY
        
        QUERY --> CONSOLE
        EDS --> ATHENA
        ATHENA --> GLUE
        QUERY --> DASH
        
        style EDS fill:#FF9900,stroke:#232F3E,color:#fff
        style QUERY fill:#4B9EDB,stroke:#232F3E,color:#fff
        style ORC fill:#3FB34F,stroke:#232F3E,color:#fff
    """
    
    st.markdown("#### 🏗️ CloudTrail Lake Architecture")
    common.mermaid(lake_architecture, height=500)
    
    # Key capabilities
    st.markdown("#### ⭐ Key Capabilities")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔍 Advanced Querying
        - **SQL-based analysis** of CloudTrail events
        - **Cross-account and cross-region** queries
        - **Time-based filtering** and aggregations
        - **Complex joins** across multiple event types
        
        ### 📊 Event Data Types
        - CloudTrail management & data events
        - CloudTrail Insights events  
        - AWS Config configuration items
        - AWS Audit Manager evidence
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 Use Cases
        - **Security investigations** and forensics
        - **Compliance reporting** and auditing
        - **Operational analysis** and troubleshooting
        - **Cost optimization** through usage analysis
        
        ### 🔒 Security Features
        - **Encryption by default** with CloudTrail
        - **Custom KMS keys** support
        - **Tag-based access control**
        - **Immutable event storage**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive query builder
    st.markdown("#### 🎮 CloudTrail Lake Query Builder")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        query_type = st.selectbox("Query Type", [
            "Security Analysis", "User Activity", "API Usage", "Error Analysis", "Cost Analysis"
        ])
    
    with col2:
        time_range = st.selectbox("Time Range", [
            "Last 1 hour", "Last 24 hours", "Last 7 days", "Last 30 days", "Custom"
        ])
    
    with col3:
        event_category = st.selectbox("Event Category", [
            "Management Events", "Data Events", "Insights Events", "All Events"
        ])
    
    # Generate sample query
    sample_query = generate_lake_query(query_type, time_range, event_category)
    
    st.markdown("##### 📝 Generated SQL Query")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code(sample_query, language='sql')
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Query examples
    st.markdown("#### 💻 CloudTrail Lake Query Examples")
    
    tab1, tab2, tab3 = st.tabs(["Security Queries", "Operational Queries", "Compliance Queries"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Security Analysis Queries for CloudTrail Lake

-- 1. Detect Failed Console Login Attempts
SELECT 
    eventTime,
    userIdentity.type as userType,
    userIdentity.userName,
    sourceIPAddress,
    userAgent,
    errorCode,
    errorMessage
FROM cloudtrail_events
WHERE eventName = 'ConsoleLogin'
  AND errorCode IS NOT NULL
  AND eventTime >= timestamp '2024-07-01 00:00:00'
ORDER BY eventTime DESC
LIMIT 100;

-- 2. Monitor Root Account Usage
SELECT 
    eventTime,
    eventName,
    sourceIPAddress,
    userAgent,
    resources
FROM cloudtrail_events  
WHERE userIdentity.type = 'Root'
  AND userIdentity.invokedBy IS NULL
  AND eventType != 'AwsServiceEvent'
  AND eventTime >= current_timestamp - interval '30' day
ORDER BY eventTime DESC;

-- 3. Identify Unusual API Activity
WITH api_baseline AS (
    SELECT 
        eventName,
        COUNT(*) as baseline_count
    FROM cloudtrail_events
    WHERE eventTime >= current_timestamp - interval '30' day
      AND eventTime < current_timestamp - interval '7' day
    GROUP BY eventName
),
recent_activity AS (
    SELECT 
        eventName,
        COUNT(*) as recent_count,
        COUNT(DISTINCT sourceIPAddress) as unique_ips
    FROM cloudtrail_events  
    WHERE eventTime >= current_timestamp - interval '7' day
    GROUP BY eventName
)
SELECT 
    r.eventName,
    r.recent_count,
    b.baseline_count,
    (r.recent_count * 100.0 / NULLIF(b.baseline_count, 0)) - 100 as percent_increase,
    r.unique_ips
FROM recent_activity r
LEFT JOIN api_baseline b ON r.eventName = b.eventName
WHERE r.recent_count > COALESCE(b.baseline_count * 2, 10)
ORDER BY percent_increase DESC;

-- 4. Track Privilege Escalation Attempts
SELECT 
    eventTime,
    userIdentity.userName,
    userIdentity.type,
    eventName,
    sourceIPAddress,
    errorCode,
    CASE 
        WHEN eventName LIKE '%Policy%' THEN 'Policy Management'
        WHEN eventName LIKE '%Role%' THEN 'Role Management' 
        WHEN eventName LIKE '%User%' THEN 'User Management'
        ELSE 'Other'
    END as activity_category
FROM cloudtrail_events
WHERE eventName IN (
    'AttachUserPolicy', 'AttachRolePolicy', 'CreateUser', 
    'CreateRole', 'PutUserPolicy', 'PutRolePolicy',
    'AddUserToGroup', 'CreateGroup', 'CreateSAMLProvider'
)
  AND eventTime >= current_timestamp - interval '24' hour
ORDER BY eventTime DESC;

-- 5. Analyze Access Patterns by IP Address
SELECT 
    sourceIPAddress,
    COUNT(DISTINCT userIdentity.userName) as unique_users,
    COUNT(*) as total_events,
    MIN(eventTime) as first_seen,
    MAX(eventTime) as last_seen,
    COUNT(DISTINCT eventName) as unique_api_calls,
    ARRAY_AGG(DISTINCT userIdentity.type) as user_types
FROM cloudtrail_events
WHERE eventTime >= current_timestamp - interval '7' day
  AND sourceIPAddress NOT LIKE '10.%'  -- Exclude internal IPs
  AND sourceIPAddress NOT LIKE '172.16.%'
  AND sourceIPAddress NOT LIKE '192.168.%'
GROUP BY sourceIPAddress
HAVING COUNT(*) > 100  -- Focus on high-activity IPs
ORDER BY total_events DESC;

-- 6. Monitor Service-to-Service Communication
SELECT 
    eventName,
    userIdentity.type,
    userIdentity.principalId,
    COUNT(*) as call_count,
    COUNT(DISTINCT sourceIPAddress) as source_ips,
    AVG(CASE WHEN errorCode IS NOT NULL THEN 1.0 ELSE 0.0 END) as error_rate
FROM cloudtrail_events
WHERE userIdentity.type IN ('AssumedRole', 'AWSService', 'AWSAccount')
  AND eventTime >= current_timestamp - interval '1' day
GROUP BY eventName, userIdentity.type, userIdentity.principalId
HAVING call_count > 50
ORDER BY call_count DESC;

-- 7. Detect Data Exfiltration Attempts
SELECT 
    eventTime,
    eventName,
    userIdentity.userName,
    sourceIPAddress,
    resources[1].resourceName as resource_accessed,
    responseElements
FROM cloudtrail_events
WHERE eventName IN (
    'GetObject', 'DownloadDBClusterSnapshot', 'DownloadDBSnapshot',
    'CreateDBClusterSnapshot', 'CreateSnapshot', 'CopySnapshot'
)
  AND eventTime >= current_timestamp - interval '24' hour
  AND (
    responseElements IS NOT NULL OR 
    sourceIPAddress NOT IN (
        SELECT DISTINCT sourceIPAddress 
        FROM cloudtrail_events 
        WHERE eventTime >= current_timestamp - interval '30' day
        GROUP BY sourceIPAddress 
        HAVING COUNT(*) > 10
    )
  )
ORDER BY eventTime DESC;
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Operational Analysis Queries for CloudTrail Lake

-- 1. API Call Volume Analysis
SELECT 
    DATE_TRUNC('hour', eventTime) as hour,
    eventName,
    COUNT(*) as call_count,
    COUNT(DISTINCT userIdentity.userName) as unique_users,
    SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) as error_count
FROM cloudtrail_events
WHERE eventTime >= current_timestamp - interval '7' day
GROUP BY DATE_TRUNC('hour', eventTime), eventName
HAVING COUNT(*) > 100
ORDER BY hour DESC, call_count DESC;

-- 2. Resource Creation and Deletion Trends
WITH resource_events AS (
    SELECT 
        eventTime,
        eventName,
        CASE 
            WHEN eventName LIKE 'Create%' OR eventName LIKE 'Run%' THEN 'Create'
            WHEN eventName LIKE 'Delete%' OR eventName LIKE 'Terminate%' THEN 'Delete'
            WHEN eventName LIKE 'Start%' THEN 'Start'
            WHEN eventName LIKE 'Stop%' THEN 'Stop'
            ELSE 'Other'
        END as action_type,
        CASE 
            WHEN eventSource = 'ec2.amazonaws.com' THEN 'EC2'
            WHEN eventSource = 's3.amazonaws.com' THEN 'S3'
            WHEN eventSource = 'rds.amazonaws.com' THEN 'RDS'
            WHEN eventSource = 'lambda.amazonaws.com' THEN 'Lambda'
            ELSE eventSource
        END as service
    FROM cloudtrail_events
    WHERE eventTime >= current_timestamp - interval '30' day
      AND (eventName LIKE 'Create%' OR eventName LIKE 'Delete%' 
           OR eventName LIKE 'Run%' OR eventName LIKE 'Terminate%'
           OR eventName LIKE 'Start%' OR eventName LIKE 'Stop%')
)
SELECT 
    DATE_TRUNC('day', eventTime) as day,
    service,
    action_type,
    COUNT(*) as event_count
FROM resource_events
GROUP BY DATE_TRUNC('day', eventTime), service, action_type
ORDER BY day DESC, service, action_type;

-- 3. User Activity Patterns
SELECT 
    userIdentity.userName,
    userIdentity.type,
    DATE_TRUNC('day', eventTime) as day,
    COUNT(DISTINCT eventName) as unique_apis,
    COUNT(*) as total_calls,
    MIN(eventTime) as first_activity,
    MAX(eventTime) as last_activity,
    COUNT(DISTINCT sourceIPAddress) as unique_ips
FROM cloudtrail_events
WHERE eventTime >= current_timestamp - interval '30' day
  AND userIdentity.type IN ('IAMUser', 'AssumedRole')
GROUP BY userIdentity.userName, userIdentity.type, DATE_TRUNC('day', eventTime)
ORDER BY day DESC, total_calls DESC;

-- 4. Service Utilization Analysis
SELECT 
    eventSource,
    DATE_TRUNC('day', eventTime) as day,
    COUNT(*) as api_calls,
    COUNT(DISTINCT eventName) as unique_operations,
    COUNT(DISTINCT userIdentity.userName) as unique_users,
    SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) as errors,
    ROUND(
        SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as error_percentage
FROM cloudtrail_events
WHERE eventTime >= current_timestamp - interval '30' day
GROUP BY eventSource, DATE_TRUNC('day', eventTime)
HAVING COUNT(*) > 10
ORDER BY day DESC, api_calls DESC;

-- 5. Performance and Error Analysis
WITH api_performance AS (
    SELECT 
        eventName,
        eventSource,
        COUNT(*) as total_calls,
        SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) as error_count,
        ARRAY_AGG(DISTINCT errorCode) FILTER (WHERE errorCode IS NOT NULL) as error_types
    FROM cloudtrail_events
    WHERE eventTime >= current_timestamp - interval '7' day
    GROUP BY eventName, eventSource
)
SELECT 
    eventSource,
    eventName,
    total_calls,
    error_count,
    ROUND(error_count * 100.0 / total_calls, 2) as error_rate,
    error_types
FROM api_performance  
WHERE total_calls > 50
ORDER BY error_rate DESC, total_calls DESC;

-- 6. Regional Activity Distribution
SELECT 
    awsRegion,
    eventSource,
    COUNT(*) as event_count,
    COUNT(DISTINCT userIdentity.userName) as unique_users,
    COUNT(DISTINCT DATE_TRUNC('day', eventTime)) as active_days
FROM cloudtrail_events
WHERE eventTime >= current_timestamp - interval '30' day
  AND awsRegion IS NOT NULL
GROUP BY awsRegion, eventSource
ORDER BY awsRegion, event_count DESC;

-- 7. Resource Access Patterns
SELECT 
    resources[1].resourceType as resource_type,
    resources[1].resourceName as resource_name,
    eventName,
    COUNT(*) as access_count,
    COUNT(DISTINCT userIdentity.userName) as unique_users,
    COUNT(DISTINCT sourceIPAddress) as unique_ips,
    MIN(eventTime) as first_access,
    MAX(eventTime) as last_access
FROM cloudtrail_events
WHERE eventTime >= current_timestamp - interval '7' day
  AND array_length(resources, 1) > 0
  AND resources[1].resourceType IS NOT NULL
GROUP BY resources[1].resourceType, resources[1].resourceName, eventName
HAVING COUNT(*) > 10
ORDER BY access_count DESC;

-- 8. Automation vs Manual Activity
SELECT 
    DATE_TRUNC('hour', eventTime) as hour,
    CASE 
        WHEN userAgent LIKE '%aws-cli%' THEN 'AWS CLI'
        WHEN userAgent LIKE '%Boto3%' OR userAgent LIKE '%boto3%' THEN 'Python SDK'
        WHEN userAgent LIKE '%aws-sdk%' THEN 'AWS SDK'
        WHEN userAgent LIKE '%console.aws.amazon.com%' THEN 'Console'
        WHEN userIdentity.type = 'AssumedRole' AND userAgent NOT LIKE '%console%' THEN 'Automated'
        ELSE 'Other'
    END as access_method,
    COUNT(*) as event_count,
    COUNT(DISTINCT userIdentity.userName) as unique_users
FROM cloudtrail_events
WHERE eventTime >= current_timestamp - interval '24' hour
GROUP BY DATE_TRUNC('hour', eventTime), access_method
ORDER BY hour DESC, event_count DESC;
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
-- Compliance and Audit Queries for CloudTrail Lake

-- 1. Administrative Actions Audit
SELECT 
    eventTime,
    userIdentity.userName,
    userIdentity.type,
    eventName,
    eventSource,
    sourceIPAddress,
    responseElements,
    CASE 
        WHEN eventName LIKE '%Policy%' THEN 'Access Policy Changes'
        WHEN eventName LIKE '%User%' OR eventName LIKE '%Group%' THEN 'User Management'
        WHEN eventName LIKE '%Role%' THEN 'Role Management'
        WHEN eventName LIKE '%Key%' THEN 'Key Management'
        WHEN eventName LIKE '%Bucket%' THEN 'Storage Management'
        ELSE 'Other Administrative'
    END as action_category
FROM cloudtrail_events
WHERE eventName IN (
    'CreateUser', 'DeleteUser', 'AttachUserPolicy', 'DetachUserPolicy',
    'CreateRole', 'DeleteRole', 'AttachRolePolicy', 'DetachRolePolicy',
    'CreatePolicy', 'DeletePolicy', 'CreateGroup', 'DeleteGroup',
    'CreateBucket', 'DeleteBucket', 'PutBucketPolicy', 'DeleteBucketPolicy',
    'CreateKey', 'DisableKey', 'EnableKey', 'ScheduleKeyDeletion'
)
  AND eventTime >= current_timestamp - interval '90' day
ORDER BY eventTime DESC;

-- 2. Data Access Audit (GDPR/CCPA Compliance)
SELECT 
    eventTime,
    userIdentity.userName,
    userIdentity.type,
    eventName,
    resources[1].resourceName as data_resource,
    sourceIPAddress,
    CASE 
        WHEN eventName IN ('GetObject', 'ListObjects') THEN 'Data Read'
        WHEN eventName IN ('PutObject', 'CopyObject') THEN 'Data Write'
        WHEN eventName IN ('DeleteObject', 'DeleteObjects') THEN 'Data Delete'
        ELSE 'Other Data Operation'
    END as data_operation_type
FROM cloudtrail_events
WHERE eventSource = 's3.amazonaws.com'
  AND eventName IN ('GetObject', 'PutObject', 'DeleteObject', 'ListObjects', 'CopyObject', 'DeleteObjects')
  AND eventTime >= current_timestamp - interval '180' day
  AND (
    resources[1].resourceName LIKE '%customer-data%' OR
    resources[1].resourceName LIKE '%personal%' OR
    resources[1].resourceName LIKE '%pii%'
  )
ORDER BY eventTime DESC;

-- 3. Encryption and Security Controls Audit
SELECT 
    eventTime,
    userIdentity.userName,
    eventName,
    eventSource,
    COALESCE(
        requestParameters.serverSideEncryption,
        requestParameters.encryption,
        'Not Specified'
    ) as encryption_status,
    CASE 
        WHEN eventName LIKE '%Encrypt%' THEN 'Encryption Configuration'
        WHEN eventName LIKE '%Key%' THEN 'Key Management'
        WHEN eventName LIKE '%SSL%' OR eventName LIKE '%TLS%' THEN 'Transport Security'
        ELSE 'Other Security'
    END as security_category
FROM cloudtrail_events
WHERE (
    eventName LIKE '%Encrypt%' OR eventName LIKE '%Key%' OR
    eventName LIKE '%SSL%' OR eventName LIKE '%TLS%' OR
    JSON_EXTRACT_SCALAR(requestParameters, '$.serverSideEncryption') IS NOT NULL
)
  AND eventTime >= current_timestamp - interval '90' day
ORDER BY eventTime DESC;

-- 4. Network Access and VPC Changes Audit
SELECT 
    eventTime,
    userIdentity.userName,
    eventName,
    eventSource,
    awsRegion,
    COALESCE(
        JSON_EXTRACT_SCALAR(requestParameters, '$.vpcId'),
        JSON_EXTRACT_SCALAR(responseElements, '$.vpcId')
    ) as vpc_id,
    CASE 
        WHEN eventName LIKE '%SecurityGroup%' THEN 'Security Group'
        WHEN eventName LIKE '%NetworkAcl%' THEN 'Network ACL'
        WHEN eventName LIKE '%RouteTable%' THEN 'Routing'
        WHEN eventName LIKE '%Gateway%' THEN 'Gateway'
        ELSE 'Other Network'
    END as network_component
FROM cloudtrail_events
WHERE eventSource = 'ec2.amazonaws.com'
  AND (
    eventName LIKE '%SecurityGroup%' OR eventName LIKE '%NetworkAcl%' OR
    eventName LIKE '%RouteTable%' OR eventName LIKE '%Gateway%' OR
    eventName LIKE '%Vpc%'
  )
  AND eventTime >= current_timestamp - interval '90' day
ORDER BY eventTime DESC;

-- 5. Privileged Access Review
WITH privileged_actions AS (
    SELECT 
        eventTime,
        userIdentity.userName,
        userIdentity.type,
        userIdentity.sessionContext.sessionIssuer.userName as assumed_role,
        eventName,
        eventSource,
        sourceIPAddress,
        CASE 
            WHEN userIdentity.type = 'Root' THEN 'Root User'
            WHEN eventName LIKE '%Policy%' THEN 'Policy Administration'
            WHEN eventName LIKE '%User%' OR eventName LIKE '%Group%' THEN 'User Administration'
            WHEN eventName LIKE '%Role%' THEN 'Role Administration'
            ELSE 'Other Privileged'
        END as privilege_type
    FROM cloudtrail_events
    WHERE (
        userIdentity.type = 'Root' OR
        eventName IN (
            'CreateUser', 'DeleteUser', 'AttachUserPolicy', 'DetachUserPolicy',
            'CreateRole', 'DeleteRole', 'AttachRolePolicy', 'DetachRolePolicy',
            'PutUserPolicy', 'DeleteUserPolicy', 'PutRolePolicy', 'DeleteRolePolicy'
        )
    )
      AND eventTime >= current_timestamp - interval '90' day
)
SELECT 
    privilege_type,
    COUNT(*) as action_count,
    COUNT(DISTINCT userIdentity.userName) as unique_users,
    COUNT(DISTINCT sourceIPAddress) as unique_source_ips,
    MIN(eventTime) as first_occurrence,
    MAX(eventTime) as last_occurrence
FROM privileged_actions
GROUP BY privilege_type
ORDER BY action_count DESC;

-- 6. Configuration Changes Timeline
SELECT 
    DATE_TRUNC('day', eventTime) as day,
    eventSource,
    COUNT(*) as configuration_changes,
    COUNT(DISTINCT userIdentity.userName) as users_making_changes,
    ARRAY_AGG(DISTINCT eventName ORDER BY eventName) as change_types
FROM cloudtrail_events
WHERE eventName IN (
    'CreateDBInstance', 'ModifyDBInstance', 'DeleteDBInstance',
    'CreateInstance', 'ModifyInstanceAttribute', 'TerminateInstances',
    'CreateBucket', 'PutBucketPolicy', 'DeleteBucket',
    'CreateFunction', 'UpdateFunctionConfiguration', 'DeleteFunction'
)
  AND eventTime >= current_timestamp - interval '90' day
GROUP BY DATE_TRUNC('day', eventTime), eventSource
ORDER BY day DESC, configuration_changes DESC;

-- 7. Cross-Account Activity Audit
SELECT 
    eventTime,
    userIdentity.type,
    userIdentity.principalId,
    recipientAccountId,
    eventName,
    eventSource,
    JSON_EXTRACT_SCALAR(requestParameters, '$.roleArn') as assumed_role_arn,
    CASE 
        WHEN recipientAccountId != userIdentity.accountId THEN 'Cross-Account'
        ELSE 'Same Account'
    END as account_relationship
FROM cloudtrail_events
WHERE (
    userIdentity.type = 'AssumedRole' AND
    JSON_EXTRACT_SCALAR(userIdentity, '$.principalId') LIKE '%AROA%'
) OR recipientAccountId != userIdentity.accountId
  AND eventTime >= current_timestamp - interval '30' day
ORDER BY eventTime DESC;

-- 8. Compliance Dashboard Summary
SELECT 
    'Administrative Actions' as audit_category,
    COUNT(*) as event_count,
    COUNT(DISTINCT userIdentity.userName) as unique_actors,
    MAX(eventTime) as last_occurrence
FROM cloudtrail_events
WHERE eventName IN (
    'CreateUser', 'DeleteUser', 'AttachUserPolicy', 'CreateRole', 'DeleteRole'
) AND eventTime >= current_timestamp - interval '30' day

UNION ALL

SELECT 
    'Data Access Events',
    COUNT(*),
    COUNT(DISTINCT userIdentity.userName),
    MAX(eventTime)
FROM cloudtrail_events
WHERE eventSource = 's3.amazonaws.com'
  AND eventName IN ('GetObject', 'PutObject', 'DeleteObject')
  AND eventTime >= current_timestamp - interval '30' day

UNION ALL

SELECT 
    'Security Changes',
    COUNT(*),
    COUNT(DISTINCT userIdentity.userName), 
    MAX(eventTime)
FROM cloudtrail_events
WHERE (eventName LIKE '%SecurityGroup%' OR eventName LIKE '%Key%')
  AND eventTime >= current_timestamp - interval '30' day

ORDER BY event_count DESC;
        ''', language='sql')
        st.markdown('</div>', unsafe_allow_html=True)

def cloudwatch_tab():
    """Content for Amazon CloudWatch tab"""
    st.markdown("## 📊 Amazon CloudWatch")
    st.markdown("*Observability of your AWS resources and applications on AWS and on-premises*")
    
    # Service overview
    st.markdown('<div class="service-overview">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What is Amazon CloudWatch?
    Amazon CloudWatch provides data and insights to monitor your applications, respond to system-wide performance changes, 
    optimize resource utilization, and get a unified view of operational health. It collects monitoring and operational 
    data in the form of logs, metrics, and events from AWS resources, applications, and services.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ CloudWatch Architecture")
    common.mermaid(create_cloudwatch_architecture(), height=600)
    
    # CloudWatch components
    st.markdown("#### ⭐ CloudWatch Components")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📈 Metrics & Alarms
        - **Custom and AWS metrics** collection
        - **Real-time monitoring** and alerting
        - **Automated responses** to threshold breaches
        - **Composite alarms** for complex conditions
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Dashboards
        - **Customizable visualizations** 
        - **Cross-region monitoring**
        - **Shared dashboards** for teams
        - **API-driven dashboard** creation
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📝 Events & Rules
        - **Real-time event stream** from AWS services
        - **Rules-based automation**
        - **Lambda function triggers**
        - **SNS notifications**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔍 Container Insights
        - **ECS and EKS monitoring**
        - **Performance metrics** collection
        - **Log aggregation**
        - **Anomaly detection**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive metrics dashboard
    st.markdown("#### 📊 Interactive Metrics Dashboard")
    
    # Generate sample metrics data
    dates = pd.date_range(start='2024-07-01', end='2024-07-14', freq='H')
    
    # CPU utilization data
    cpu_data = {
        'timestamp': dates,
        'EC2_CPU': np.random.normal(45, 15, len(dates)).clip(0, 100),
        'RDS_CPU': np.random.normal(25, 10, len(dates)).clip(0, 100),
        'Lambda_Duration': np.random.normal(150, 50, len(dates)).clip(0, 1000)
    }
    
    df_metrics = pd.DataFrame(cpu_data)
    
    # Create metrics visualization
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('EC2 CPU Utilization', 'RDS CPU Utilization', 
                       'Lambda Duration (ms)', 'Alarm Status'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"type": "indicator"}]]
    )
    
    # EC2 CPU
    fig.add_trace(
        go.Scatter(x=df_metrics['timestamp'], y=df_metrics['EC2_CPU'],
                  name='EC2 CPU %', line=dict(color=AWS_COLORS['primary'])),
        row=1, col=1
    )
    
    # RDS CPU
    fig.add_trace(
        go.Scatter(x=df_metrics['timestamp'], y=df_metrics['RDS_CPU'],
                  name='RDS CPU %', line=dict(color=AWS_COLORS['light_blue'])),
        row=1, col=2
    )
    
    # Lambda Duration
    fig.add_trace(
        go.Scatter(x=df_metrics['timestamp'], y=df_metrics['Lambda_Duration'],
                  name='Lambda ms', line=dict(color=AWS_COLORS['success'])),
        row=2, col=1
    )
    
    # Alarm indicator
    fig.add_trace(
        go.Indicator(
            mode = "gauge+number+delta",
            value = 42,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': "CPU Utilization"},
            delta = {'reference': 50},
            gauge = {
                'axis': {'range': [None, 100]},
                'bar': {'color': AWS_COLORS['primary']},
                'steps': [
                    {'range': [0, 50], 'color': AWS_COLORS['light_gray']},
                    {'range': [50, 80], 'color': "yellow"},
                    {'range': [80, 100], 'color': "red"}],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 80}}),
        row=2, col=2
    )
    
    fig.update_layout(height=600, showlegend=True, 
                     title_text="CloudWatch Metrics Dashboard")
    
    st.plotly_chart(fig, use_container_width=True)
    
    # CloudWatch code examples
    st.markdown("#### 💻 CloudWatch Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Custom Metrics", "Alarms & Notifications", "Dashboards"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Custom CloudWatch Metrics Implementation
import boto3
import time
import json
from datetime import datetime

cloudwatch = boto3.client('cloudwatch')

def publish_custom_metrics():
    """Publish custom application metrics to CloudWatch"""
    
    # Example: Business metrics
    cloudwatch.put_metric_data(
        Namespace='MyApp/Business',
        MetricData=[
            {
                'MetricName': 'OrdersProcessed',
                'Value': 150,
                'Unit': 'Count',
                'Timestamp': datetime.utcnow(),
                'Dimensions': [
                    {
                        'Name': 'Environment',
                        'Value': 'Production'
                    },
                    {
                        'Name': 'Service',
                        'Value': 'OrderProcessing'
                    }
                ]
            },
            {
                'MetricName': 'Revenue',
                'Value': 25000.50,
                'Unit': 'None',
                'Timestamp': datetime.utcnow(),
                'Dimensions': [
                    {
                        'Name': 'Environment', 
                        'Value': 'Production'
                    }
                ]
            }
        ]
    )
    
    # Example: Performance metrics
    cloudwatch.put_metric_data(
        Namespace='MyApp/Performance',
        MetricData=[
            {
                'MetricName': 'ResponseTime',
                'Value': 0.45,
                'Unit': 'Seconds',
                'Timestamp': datetime.utcnow(),
                'Dimensions': [
                    {
                        'Name': 'APIEndpoint',
                        'Value': '/api/users'
                    }
                ]
            },
            {
                'MetricName': 'ErrorRate',
                'Value': 0.02,
                'Unit': 'Percent',
                'Timestamp': datetime.utcnow()
            }
        ]
    )

def create_metric_filter():
    """Create CloudWatch metric filter from log data"""
    
    logs_client = boto3.client('logs')
    
    # Create metric filter for application errors
    logs_client.put_metric_filter(
        logGroupName='/aws/lambda/my-function',
        filterName='ErrorCount',
        filterPattern='ERROR',
        metricTransformations=[
            {
                'metricName': 'ApplicationErrors',
                'metricNamespace': 'MyApp/Errors',
                'metricValue': '1',
                'defaultValue': 0,
                'metricValueKey': None
            }
        ]
    )
    
    # Create metric filter for response times
    logs_client.put_metric_filter(
        logGroupName='/aws/lambda/my-function',
        filterName='ResponseTimeMetric',
        filterPattern='[timestamp, request_id, response_time]',
        metricTransformations=[
            {
                'metricName': 'ResponseTime',
                'metricNamespace': 'MyApp/Performance',
                'metricValue': '$response_time',
                'defaultValue': 0
            }
        ]
    )

def batch_publish_metrics():
    """Efficiently publish multiple metrics in batches"""
    
    # Batch multiple metrics together (up to 20 per call)
    metric_data = []
    
    # Simulate collecting various application metrics
    metrics = {
        'ActiveUsers': 1250,
        'DatabaseConnections': 45,
        'CacheHitRate': 0.85,
        'QueueDepth': 12,
        'DiskUsage': 0.67
    }
    
    for metric_name, value in metrics.items():
        metric_data.append({
            'MetricName': metric_name,
            'Value': value,
            'Unit': 'Count' if isinstance(value, int) else 'Percent',
            'Timestamp': datetime.utcnow(),
            'Dimensions': [
                {
                    'Name': 'Application',
                    'Value': 'WebApp'
                },
                {
                    'Name': 'Environment',
                    'Value': 'Production'
                }
            ]
        })
    
    # Publish batch
    cloudwatch.put_metric_data(
        Namespace='MyApp/System',
        MetricData=metric_data
    )

class CloudWatchMetricsCollector:
    """Class for structured metrics collection"""
    
    def __init__(self, namespace, dimensions=None):
        self.cloudwatch = boto3.client('cloudwatch')
        self.namespace = namespace
        self.default_dimensions = dimensions or []
        self.metric_buffer = []
    
    def add_metric(self, name, value, unit='Count', dimensions=None):
        """Add metric to buffer"""
        metric_dimensions = self.default_dimensions.copy()
        if dimensions:
            metric_dimensions.extend(dimensions)
        
        self.metric_buffer.append({
            'MetricName': name,
            'Value': value,
            'Unit': unit,
            'Timestamp': datetime.utcnow(),
            'Dimensions': metric_dimensions
        })
    
    def flush_metrics(self):
        """Send all buffered metrics to CloudWatch"""
        if not self.metric_buffer:
            return
        
        # Send in batches of 20 (CloudWatch limit)
        for i in range(0, len(self.metric_buffer), 20):
            batch = self.metric_buffer[i:i+20]
            
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=batch
            )
        
        # Clear buffer
        self.metric_buffer = []
        
    def add_timing_metric(self, name, start_time, dimensions=None):
        """Convenience method for timing metrics"""
        duration = time.time() - start_time
        self.add_metric(name, duration, 'Seconds', dimensions)

# Usage examples
def example_usage():
    # Basic metric publishing
    publish_custom_metrics()
    
    # Structured collector
    collector = CloudWatchMetricsCollector(
        'MyApp/eCommerce',
        [{'Name': 'Environment', 'Value': 'Production'}]
    )
    
    # Add various business metrics
    collector.add_metric('ProductViews', 1523, 'Count')
    collector.add_metric('CartConversions', 45, 'Count')
    collector.add_metric('AverageOrderValue', 67.89, 'None')
    
    # Add performance timing
    start = time.time()
    # ... some operation ...
    time.sleep(0.1)  # Simulate work
    collector.add_timing_metric('DatabaseQuery', start)
    
    # Send all metrics
    collector.flush_metrics()
    
    # Create metric filters
    create_metric_filter()

# Execute examples
example_usage()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# CloudWatch Alarms and Notifications
import boto3
import json

cloudwatch = boto3.client('cloudwatch')
sns = boto3.client('sns')

def create_basic_alarms():
    """Create basic CloudWatch alarms for common scenarios"""
    
    # High CPU utilization alarm
    cloudwatch.put_metric_alarm(
        AlarmName='HighCPUUtilization',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=2,
        MetricName='CPUUtilization',
        Namespace='AWS/EC2',
        Period=300,
        Statistic='Average',
        Threshold=80.0,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-east-1:123456789012:high-cpu-alert'
        ],
        AlarmDescription='Alert when CPU exceeds 80%',
        Dimensions=[
            {
                'Name': 'InstanceId',
                'Value': 'i-1234567890abcdef0'
            }
        ],
        Unit='Percent'
    )
    
    # Application error rate alarm
    cloudwatch.put_metric_alarm(
        AlarmName='HighErrorRate',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=3,
        MetricName='ApplicationErrors',
        Namespace='MyApp/Errors',
        Period=300,
        Statistic='Sum',
        Threshold=10,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-east-1:123456789012:error-alerts',
            'arn:aws:autoScaling:us-east-1:123456789012:scalingPolicy:policy-arn'
        ],
        AlarmDescription='Alert when error count exceeds 10 in 15 minutes',
        TreatMissingData='notBreaching'
    )

def create_composite_alarm():
    """Create composite alarm combining multiple conditions"""
    
    # First create individual alarms
    alarm_names = ['HighCPUUtilization', 'HighMemoryUtilization', 'HighDiskUsage']
    
    # Create composite alarm
    cloudwatch.put_composite_alarm(
        AlarmName='SystemHealthComposite',
        AlarmDescription='Composite alarm for overall system health',
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-east-1:123456789012:critical-alerts'
        ],
        AlarmRule=f"({alarm_names[0]}) OR ({alarm_names[1]}) OR ({alarm_names[2]})",
        Tags=[
            {
                'Key': 'Environment',
                'Value': 'Production'
            },
            {
                'Key': 'Team',
                'Value': 'DevOps'
            }
        ]
    )

def create_anomaly_detector_alarm():
    """Create alarm based on anomaly detection"""
    
    # Create anomaly detector
    cloudwatch.put_anomaly_detector(
        Namespace='AWS/ApplicationELB',
        MetricName='RequestCount',
        Dimensions=[
            {
                'Name': 'LoadBalancer',
                'Value': 'app/my-load-balancer/50dc6c495c0c9188'
            }
        ],
        Stat='Average'
    )
    
    # Create alarm using anomaly detection
    cloudwatch.put_metric_alarm(
        AlarmName='RequestCountAnomalyAlarm',
        ComparisonOperator='LessThanLowerOrGreaterThanUpperThreshold',
        EvaluationPeriods=2,
        Metrics=[
            {
                'Id': 'm1',
                'ReturnData': True,
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/ApplicationELB',
                        'MetricName': 'RequestCount',
                        'Dimensions': [
                            {
                                'Name': 'LoadBalancer',
                                'Value': 'app/my-load-balancer/50dc6c495c0c9188'
                            }
                        ]
                    },
                    'Period': 300,
                    'Stat': 'Average'
                }
            },
            {
                'Id': 'ad1',
                'Expression': 'ANOMALY_DETECTION_FUNCTION(m1, 2)'
            }
        ],
        ThresholdMetricId='ad1',
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-east-1:123456789012:anomaly-alerts'
        ],
        AlarmDescription='Alert on request count anomalies'
    )

def setup_autoscaling_alarm():
    """Create alarms for Auto Scaling integration"""
    
    # Scale-out alarm
    cloudwatch.put_metric_alarm(
        AlarmName='ScaleOutAlarm',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=2,
        MetricName='CPUUtilization',
        Namespace='AWS/EC2',
        Period=300,
        Statistic='Average',
        Threshold=70.0,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:autoscaling:us-east-1:123456789012:scalingPolicy:policy-name'
        ],
        AlarmDescription='Scale out when CPU > 70%',
        Dimensions=[
            {
                'Name': 'AutoScalingGroupName',
                'Value': 'my-asg'
            }
        ]
    )
    
    # Scale-in alarm
    cloudwatch.put_metric_alarm(
        AlarmName='ScaleInAlarm', 
        ComparisonOperator='LessThanThreshold',
        EvaluationPeriods=2,
        MetricName='CPUUtilization',
        Namespace='AWS/EC2',
        Period=300,
        Statistic='Average',
        Threshold=30.0,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:autoscaling:us-east-1:123456789012:scalingPolicy:scale-in-policy'
        ],
        AlarmDescription='Scale in when CPU < 30%',
        Dimensions=[
            {
                'Name': 'AutoScalingGroupName',
                'Value': 'my-asg'
            }
        ]
    )

def create_sns_integration():
    """Setup SNS topics and subscriptions for alarm notifications"""
    
    # Create SNS topic for critical alerts
    response = sns.create_topic(Name='CriticalAlerts')
    topic_arn = response['TopicArn']
    
    # Subscribe email to topic
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol='email',
        Endpoint='devops-team@company.com'
    )
    
    # Subscribe Lambda function for automated response
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol='lambda',
        Endpoint='arn:aws:lambda:us-east-1:123456789012:function:AlarmHandler'
    )
    
    # Create topic for warning alerts
    warning_topic = sns.create_topic(Name='WarningAlerts')
    warning_arn = warning_topic['TopicArn']
    
    # Subscribe Slack webhook
    sns.subscribe(
        TopicArn=warning_arn,
        Protocol='https',
        Endpoint='https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
    )
    
    return topic_arn, warning_arn

def lambda_alarm_handler():
    """Example Lambda function for handling CloudWatch alarms"""
    
    lambda_code = """
import json
import boto3

def lambda_handler(event, context):
    """Handle CloudWatch alarm notifications"""
    
    # Parse SNS message
    message = json.loads(event['Records'][0]['Sns']['Message'])
    
    alarm_name = message['AlarmName']
    new_state = message['NewStateValue']
    reason = message['NewStateReason']
    
    print(f"Alarm: {alarm_name}, State: {new_state}")
    
    # Automated responses based on alarm
    if alarm_name == 'HighCPUUtilization' and new_state == 'ALARM':
        # Trigger additional scaling or cleanup
        autoscaling = boto3.client('autoscaling')
        autoscaling.set_desired_capacity(
            AutoScalingGroupName='my-asg',
            DesiredCapacity=5,
            HonorCooldown=True
        )
        
    elif alarm_name == 'HighErrorRate' and new_state == 'ALARM':
        # Restart application or redirect traffic
        ecs = boto3.client('ecs')
        ecs.update_service(
            cluster='my-cluster',
            service='my-service',
            forceNewDeployment=True
        )
    
    # Always log to CloudWatch Logs
    print(f"Processed alarm: {alarm_name} - {reason}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Alarm processed successfully')
    }
    """
    
    return lambda_code

# Setup comprehensive alarm monitoring
def setup_complete_monitoring():
    """Setup complete alarm monitoring system"""
    
    print("Setting up CloudWatch alarms...")
    
    # Create SNS topics
    critical_topic, warning_topic = create_sns_integration()
    
    # Create basic alarms
    create_basic_alarms()
    
    # Create composite alarm
    create_composite_alarm()
    
    # Create anomaly detection
    create_anomaly_detector_alarm()
    
    # Setup autoscaling alarms
    setup_autoscaling_alarm()
    
    print("CloudWatch alarm monitoring setup complete!")
    
    return {
        'critical_topic_arn': critical_topic,
        'warning_topic_arn': warning_topic,
        'alarms_created': 6
    }

# Execute setup
monitoring_config = setup_complete_monitoring()
print(f"Monitoring configuration: {monitoring_config}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# CloudWatch Dashboards Creation and Management
import boto3
import json

cloudwatch = boto3.client('cloudwatch')

def create_comprehensive_dashboard():
    """Create comprehensive CloudWatch dashboard"""
    
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "x": 0,
                "y": 0,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/EC2", "CPUUtilization", "InstanceId", "i-1234567890abcdef0"],
                        [".", "NetworkIn", ".", "."],
                        [".", "NetworkOut", ".", "."]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": "us-east-1",
                    "title": "EC2 Instance Performance",
                    "period": 300,
                    "annotations": {
                        "horizontal": [
                            {
                                "label": "CPU Threshold",
                                "value": 80
                            }
                        ]
                    }
                }
            },
            {
                "type": "metric",
                "x": 12,
                "y": 0,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/ApplicationELB", "RequestCount", "LoadBalancer", "app/my-load-balancer/50dc6c495c0c9188"],
                        [".", "TargetResponseTime", ".", "."],
                        [".", "HTTPCode_Target_4XX_Count", ".", "."],
                        [".", "HTTPCode_Target_5XX_Count", ".", "."]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": "us-east-1",
                    "title": "Application Load Balancer Metrics",
                    "period": 300
                }
            },
            {
                "type": "log",
                "x": 0,
                "y": 6,
                "width": 24,
                "height": 6,
                "properties": {
                    "query": "SOURCE '/aws/lambda/my-function'\n| fields @timestamp, @message\n| filter @message like /ERROR/\n| sort @timestamp desc\n| limit 100",
                    "region": "us-east-1",
                    "title": "Recent Application Errors",
                    "view": "table"
                }
            },
            {
                "type": "metric",
                "x": 0,
                "y": 12,
                "width": 8,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/RDS", "CPUUtilization", "DBInstanceIdentifier", "mydb-instance"],
                        [".", "DatabaseConnections", ".", "."],
                        [".", "ReadLatency", ".", "."],
                        [".", "WriteLatency", ".", "."]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": "us-east-1",
                    "title": "RDS Performance",
                    "period": 300
                }
            },
            {
                "type": "number",
                "x": 8,
                "y": 12,
                "width": 8,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["MyApp/Business", "OrdersProcessed", "Environment", "Production"],
                        [".", "Revenue", ".", "."],
                        [".", "ActiveUsers", ".", "."]
                    ],
                    "view": "singleValue",
                    "region": "us-east-1",
                    "title": "Business Metrics",
                    "period": 3600,
                    "stat": "Sum"
                }
            },
            {
                "type": "metric",
                "x": 16,
                "y": 12,
                "width": 8,
                "height": 6,
                "properties": {
                    "view": "pie",
                    "metrics": [
                        ["AWS/Lambda", "Invocations", "FunctionName", "ProcessOrder"],
                        [".", ".", ".", "ProcessPayment"],
                        [".", ".", ".", "SendNotification"],
                        [".", ".", ".", "GenerateReport"]
                    ],
                    "region": "us-east-1",
                    "title": "Lambda Function Usage Distribution",
                    "period": 3600
                }
            }
        ]
    }
    
    # Create dashboard
    response = cloudwatch.put_dashboard(
        DashboardName='ComprehensiveMonitoring',
        DashboardBody=json.dumps(dashboard_body)
    )
    
    return response

def create_business_dashboard():
    """Create business-focused dashboard"""
    
    business_dashboard = {
        "widgets": [
            {
                "type": "metric",
                "x": 0,
                "y": 0,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["MyApp/Business", "OrdersProcessed", "Environment", "Production"],
                        [".", "OrdersCancelled", ".", "."],
                        [".", "OrdersReturned", ".", "."]
                    ],
                    "view": "timeSeries",
                    "stacked": True,
                    "region": "us-east-1",
                    "title": "Order Processing Trends",
                    "period": 3600,
                    "stat": "Sum"
                }
            },
            {
                "type": "metric", 
                "x": 12,
                "y": 0,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["MyApp/Business", "Revenue", "Environment", "Production"],
                        [".", "AverageOrderValue", ".", "."]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": "us-east-1",
                    "title": "Revenue Metrics",
                    "period": 3600,
                    "yAxis": {
                        "left": {
                            "min": 0
                        }
                    }
                }
            },
            {
                "type": "number",
                "x": 0,
                "y": 6,
                "width": 6,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["MyApp/Business", "ActiveUsers", "Environment", "Production"]
                    ],
                    "view": "singleValue",
                    "region": "us-east-1",
                    "title": "Active Users (24h)",
                    "period": 86400,
                    "stat": "Maximum"
                }
            },
            {
                "type": "number",
                "x": 6,
                "y": 6,
                "width": 6,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["MyApp/Business", "ConversionRate", "Environment", "Production"]
                    ],
                    "view": "singleValue",
                    "region": "us-east-1",
                    "title": "Conversion Rate (%)",
                    "period": 3600,
                    "stat": "Average"
                }
            },
            {
                "type": "log",
                "x": 12,
                "y": 6,
                "width": 12,
                "height": 6,
                "properties": {
                    "query": "SOURCE '/aws/lambda/process-orders'\n| fields @timestamp, orderId, customerType, orderValue\n| filter orderValue > 1000\n| stats sum(orderValue) as totalRevenue by customerType\n| sort totalRevenue desc",
                    "region": "us-east-1",
                    "title": "High-Value Orders by Customer Type",
                    "view": "table"
                }
            }
        ]
    }
    
    cloudwatch.put_dashboard(
        DashboardName='BusinessMetrics',
        DashboardBody=json.dumps(business_dashboard)
    )

def create_operational_dashboard():
    """Create operational/SRE focused dashboard"""
    
    ops_dashboard = {
        "widgets": [
            {
                "type": "metric",
                "x": 0,
                "y": 0,
                "width": 8,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["MyApp/Performance", "ResponseTime", "APIEndpoint", "/api/users"],
                        [".", ".", ".", "/api/orders"],
                        [".", ".", ".", "/api/products"]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": "us-east-1",
                    "title": "API Response Times",
                    "period": 300,
                    "annotations": {
                        "horizontal": [
                            {
                                "label": "SLA Threshold",
                                "value": 0.5
                            }
                        ]
                    }
                }
            },
            {
                "type": "metric",
                "x": 8,
                "y": 0,
                "width": 8,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["MyApp/Errors", "ApplicationErrors", "Service", "UserService"],
                        [".", ".", ".", "OrderService"],
                        [".", ".", ".", "PaymentService"]
                    ],
                    "view": "timeSeries",
                    "stacked": True,
                    "region": "us-east-1",
                    "title": "Error Rates by Service",
                    "period": 300
                }
            },
            {
                "type": "metric",
                "x": 16,
                "y": 0,
                "width": 8,
                "height": 6,
                "properties": {
                    "view": "gauge",
                    "metrics": [
                        ["MyApp/System", "SystemHealth"]
                    ],
                    "region": "us-east-1",
                    "title": "Overall System Health",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "min": 0,
                            "max": 100
                        }
                    }
                }
            }
        ]
    }
    
    cloudwatch.put_dashboard(
        DashboardName='OperationalHealth',
        DashboardBody=json.dumps(ops_dashboard)
    )

def create_custom_widgets():
    """Examples of advanced widget configurations"""
    
    # Mathematical expressions widget
    expression_widget = {
        "type": "metric",
        "properties": {
            "metrics": [
                ["AWS/ApplicationELB", "RequestCount", "LoadBalancer", "app/my-lb/123", {"id": "m1"}],
                ["AWS/ApplicationELB", "HTTPCode_Target_5XX_Count", "LoadBalancer", "app/my-lb/123", {"id": "m2"}],
                [{"expression": "m2/m1*100", "label": "Error Rate %", "id": "e1"}]
            ],
            "view": "timeSeries",
            "region": "us-east-1",
            "title": "Calculated Error Rate",
            "period": 300
        }
    }
    
    # Anomaly detection widget
    anomaly_widget = {
        "type": "metric",
        "properties": {
            "metrics": [
                ["AWS/EC2", "CPUUtilization", "InstanceId", "i-1234567890abcdef0", {"id": "m1"}],
                [{"expression": "ANOMALY_DETECTION_FUNCTION(m1, 2)", "id": "ad1", "label": "CPUUtilization (expected)"}]
            ],
            "view": "timeSeries",
            "region": "us-east-1",
            "title": "CPU Utilization with Anomaly Detection",
            "period": 300
        }
    }
    
    return expression_widget, anomaly_widget

def manage_dashboards():
    """Dashboard management operations"""
    
    # List all dashboards
    response = cloudwatch.list_dashboards()
    
    for dashboard in response['DashboardEntries']:
        print(f"Dashboard: {dashboard['DashboardName']}")
        print(f"  Size: {dashboard['Size']} bytes")
        print(f"  Last Modified: {dashboard['LastModified']}")
    
    # Get dashboard details
    dashboard_detail = cloudwatch.get_dashboard(
        DashboardName='ComprehensiveMonitoring'
    )
    
    # Update existing dashboard
    updated_body = json.loads(dashboard_detail['DashboardBody'])
    # Modify the dashboard body as needed
    
    cloudwatch.put_dashboard(
        DashboardName='ComprehensiveMonitoring',
        DashboardBody=json.dumps(updated_body)
    )

# Execute dashboard creation
def setup_all_dashboards():
    """Setup all monitoring dashboards"""
    
    print("Creating comprehensive dashboard...")
    create_comprehensive_dashboard()
    
    print("Creating business dashboard...")
    create_business_dashboard()
    
    print("Creating operational dashboard...")
    create_operational_dashboard()
    
    print("Dashboard setup complete!")

# Run setup
setup_all_dashboards()
manage_dashboards()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def cloudwatch_logs_tab():
    """Content for Amazon CloudWatch Logs tab"""
    st.markdown("## 📝 Amazon CloudWatch Logs")
    st.markdown("*Monitor, store, and access your log files from EC2, AWS CloudTrail, Route 53 and other sources*")
    
    # Service overview
    st.markdown('<div class="service-overview">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What is CloudWatch Logs?
    CloudWatch Logs enables you to centralize the logs from all of your systems, applications, and AWS services 
    in a single, highly scalable service. You can easily view, search for specific error codes or patterns, 
    filter based on specific fields, or archive them securely for future analysis.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # CloudWatch Logs architecture
    logs_architecture = """
    graph TB
        subgraph "Log Sources"
            EC2[EC2 Instances<br/>Applications]
            LAMBDA[AWS Lambda<br/>Functions]
            ROUTE53[Route 53<br/>DNS Queries]
            CLOUDTRAIL[CloudTrail<br/>API Logs]
            CUSTOM[Custom<br/>Applications]
        end
        
        subgraph "CloudWatch Logs"
            AGENT[CloudWatch Agent]
            GROUPS[Log Groups]
            STREAMS[Log Streams]
            RETENTION[Retention Policy]
        end
        
        subgraph "Analysis & Processing"
            INSIGHTS[Logs Insights<br/>Queries]
            FILTERS[Metric Filters]
            SUBSCRIPTIONS[Subscription Filters]
            EXPORT[Export to S3]
        end
        
        subgraph "Actions & Alerts"
            CW_ALARMS[CloudWatch<br/>Alarms]
            SNS[SNS<br/>Notifications]
            LAMBDA_TRIGGER[Lambda<br/>Triggers]
            ELASTICSEARCH[OpenSearch<br/>Service]
        end
        
        EC2 --> AGENT
        LAMBDA --> GROUPS
        ROUTE53 --> GROUPS
        CLOUDTRAIL --> GROUPS
        CUSTOM --> AGENT
        
        AGENT --> GROUPS
        GROUPS --> STREAMS
        STREAMS --> RETENTION
        
        GROUPS --> INSIGHTS
        GROUPS --> FILTERS
        GROUPS --> SUBSCRIPTIONS
        GROUPS --> EXPORT
        
        FILTERS --> CW_ALARMS
        CW_ALARMS --> SNS
        SUBSCRIPTIONS --> LAMBDA_TRIGGER
        SUBSCRIPTIONS --> ELASTICSEARCH
        
        style GROUPS fill:#FF9900,stroke:#232F3E,color:#fff
        style INSIGHTS fill:#4B9EDB,stroke:#232F3E,color:#fff
        style FILTERS fill:#3FB34F,stroke:#232F3E,color:#fff
    """
    
    st.markdown("#### 🏗️ CloudWatch Logs Architecture")
    common.mermaid(logs_architecture, height=600)
    
    # Key features
    st.markdown("#### ⭐ Key Features")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔍 Logs Insights
        - **Interactive log analysis** with SQL-like queries
        - **Real-time search** across log data
        - **Visualization** of query results
        - **Saved queries** for repeated analysis
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Live Tail
        - **Real-time log streaming** 
        - **Filter and highlight** specific terms
        - **Near real-time troubleshooting**
        - **Multiple log group** monitoring
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔒 Data Protection
        - **Audit and mask** sensitive data
        - **Data identifier** patterns
        - **Masking policies** for PII
        - **Compliance** with regulations
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📦 Log Classes
        - **Standard class** for real-time monitoring
        - **Infrequent Access** for cost-effective storage
        - **Flexible retention** policies
        - **Archive to S3** for long-term storage
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive log query builder
    st.markdown("#### 🔍 CloudWatch Logs Insights Query Builder")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        log_group = st.selectbox("Log Group", [
            "/aws/lambda/my-function",
            "/aws/apigateway/my-api", 
            "/var/log/apache/access.log",
            "/var/log/application.log"
        ])
    
    with col2:
        query_template = st.selectbox("Query Template", [
            "Recent Errors", "Performance Analysis", "IP Address Analysis", 
            "Custom Fields", "Time Range Filter"
        ])
    
    with col3:
        time_range = st.selectbox("Time Range", [
            "Last 1 hour", "Last 24 hours", "Last 7 days", "Custom"
        ])
    
    # Generate sample query
    sample_log_query = generate_logs_query(log_group, query_template, time_range)
    
    st.markdown("##### 📝 Generated Logs Insights Query")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code(sample_log_query, language='sql')
    st.markdown('</div>', unsafe_allow_html=True)
    
    # CloudWatch Logs code examples
    st.markdown("#### 💻 CloudWatch Logs Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Log Management", "Insights Queries", "Monitoring & Alerts"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# CloudWatch Logs Management and Configuration
import boto3
import json
import time
from datetime import datetime

logs_client = boto3.client('logs')

def create_log_group_and_streams():
    """Create log groups and streams with proper configuration"""
    
    # Create log group for application logs
    try:
        logs_client.create_log_group(
            logGroupName='/myapp/application',
            kmsKeyId='arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012',
            tags={
                'Environment': 'Production',
                'Application': 'MyWebApp',
                'LogType': 'Application'
            }
        )
        
        # Set retention policy
        logs_client.put_retention_policy(
            logGroupName='/myapp/application',
            retentionInDays=90
        )
        
        print("Log group '/myapp/application' created with 90 day retention")
        
    except logs_client.exceptions.ResourceAlreadyExistsException:
        print("Log group already exists")
    
    # Create log group for access logs
    try:
        logs_client.create_log_group(
            logGroupName='/myapp/access',
            tags={
                'Environment': 'Production',
                'LogType': 'Access'
            }
        )
        
        # Set to Infrequent Access log class for cost savings
        logs_client.put_retention_policy(
            logGroupName='/myapp/access',
            retentionInDays=365
        )
        
        print("Access log group created with 1 year retention")
        
    except Exception as e:
        print(f"Error creating access log group: {e}")
    
    # Create log streams
    timestamp = int(time.time() * 1000)
    
    try:
        logs_client.create_log_stream(
            logGroupName='/myapp/application',
            logStreamName=f'app-server-1-{timestamp}'
        )
        
        logs_client.create_log_stream(
            logGroupName='/myapp/application', 
            logStreamName=f'app-server-2-{timestamp}'
        )
        
        print("Log streams created successfully")
        
    except Exception as e:
        print(f"Error creating log streams: {e}")

def send_log_events():
    """Send log events to CloudWatch Logs"""
    
    # Sample application log events
    log_events = [
        {
            'timestamp': int(time.time() * 1000),
            'message': 'INFO: Application started successfully'
        },
        {
            'timestamp': int(time.time() * 1000) + 1000,
            'message': 'INFO: User authentication successful for user: john.doe'
        },
        {
            'timestamp': int(time.time() * 1000) + 2000,
            'message': 'ERROR: Database connection failed - Connection timeout after 30 seconds'
        },
        {
            'timestamp': int(time.time() * 1000) + 3000,
            'message': 'WARN: High memory usage detected - 85% utilization'
        }
    ]
    
    try:
        # Send log events
        response = logs_client.put_log_events(
            logGroupName='/myapp/application',
            logStreamName='app-server-1-' + str(int(time.time() * 1000)),
            logEvents=log_events
        )
        
        print(f"Log events sent successfully. Next sequence token: {response.get('nextSequenceToken')}")
        
    except Exception as e:
        print(f"Error sending log events: {e}")

def setup_metric_filters():
    """Create metric filters to extract metrics from logs"""
    
    # Metric filter for error count
    logs_client.put_metric_filter(
        logGroupName='/myapp/application',
        filterName='ErrorCount',
        filterPattern='ERROR',
        metricTransformations=[
            {
                'metricName': 'ApplicationErrors',
                'metricNamespace': 'MyApp/Logs',
                'metricValue': '1',
                'defaultValue': 0
            }
        ]
    )
    
    # Metric filter for response times
    logs_client.put_metric_filter(
        logGroupName='/myapp/access',
        filterName='ResponseTime',
        filterPattern='[timestamp, request_id, method, url, status_code, response_time]',
        metricTransformations=[
            {
                'metricName': 'ResponseTime',
                'metricNamespace': 'MyApp/Performance',
                'metricValue': '$response_time',
                'defaultValue': 0
            }
        ]
    )
    
    # Metric filter for HTTP status codes
    logs_client.put_metric_filter(
        logGroupName='/myapp/access',
        filterName='HTTP4xxErrors',
        filterPattern='[timestamp, request_id, method, url, status_code=4*, response_time]',
        metricTransformations=[
            {
                'metricName': 'HTTP4xxCount',
                'metricNamespace': 'MyApp/HTTP',
                'metricValue': '1',
                'defaultValue': 0
            }
        ]
    )
    
    logs_client.put_metric_filter(
        logGroupName='/myapp/access',
        filterName='HTTP5xxErrors',
        filterPattern='[timestamp, request_id, method, url, status_code=5*, response_time]',
        metricTransformations=[
            {
                'metricName': 'HTTP5xxCount',
                'metricNamespace': 'MyApp/HTTP',
                'metricValue': '1',
                'defaultValue': 0
            }
        ]
    )
    
    print("Metric filters created successfully")

def setup_subscription_filters():
    """Setup subscription filters for real-time log processing"""
    
    # Subscription filter to send logs to Lambda for processing
    logs_client.put_subscription_filter(
        logGroupName='/myapp/application',
        filterName='ErrorProcessing',
        filterPattern='ERROR',
        destinationArn='arn:aws:lambda:us-east-1:123456789012:function:ProcessLogErrors'
    )
    
    # Subscription filter to send logs to Kinesis Data Streams
    logs_client.put_subscription_filter(
        logGroupName='/myapp/access',
        filterName='AccessLogStreaming',
        filterPattern='',  # Send all logs
        destinationArn='arn:aws:kinesis:us-east-1:123456789012:stream/access-logs-stream',
        roleArn='arn:aws:iam::123456789012:role/CWLogsRole'
    )
    
    # Subscription filter to OpenSearch
    logs_client.put_subscription_filter(
        logGroupName='/myapp/application',
        filterName='OpenSearchIntegration',
        filterPattern='',
        destinationArn='arn:aws:es:us-east-1:123456789012:domain/logging/*'
    )
    
    print("Subscription filters configured")

def export_logs_to_s3():
    """Export log data to S3 for long-term archiving"""
    
    start_time = int((datetime.now().timestamp() - 86400) * 1000)  # 24 hours ago
    end_time = int(datetime.now().timestamp() * 1000)  # Now
    
    # Create export task
    response = logs_client.create_export_task(
        logGroupName='/myapp/application',
        fromTime=start_time,
        to=end_time,
        destination='my-log-archive-bucket',
        destinationPrefix='application-logs/',
        taskName='DailyLogExport'
    )
    
    task_id = response['taskId']
    print(f"Export task created: {task_id}")
    
    # Monitor export task status
    while True:
        task_status = logs_client.describe_export_tasks(
            taskId=task_id
        )
        
        status = task_status['exportTasks'][0]['status']['code']
        print(f"Export task status: {status}")
        
        if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            break
        
        time.sleep(30)
    
    return task_id

class CloudWatchLogsManager:
    """Class for managing CloudWatch Logs operations"""
    
    def __init__(self):
        self.logs_client = boto3.client('logs')
    
    def create_structured_logging(self, app_name, environment):
        """Create structured logging setup for an application"""
        
        log_groups = [
            f'/{app_name}/{environment}/application',
            f'/{app_name}/{environment}/access',
            f'/{app_name}/{environment}/error',
            f'/{app_name}/{environment}/audit'
        ]
        
        for log_group in log_groups:
            try:
                self.logs_client.create_log_group(
                    logGroupName=log_group,
                    tags={
                        'Application': app_name,
                        'Environment': environment,
                        'ManagedBy': 'CloudWatchLogsManager'
                    }
                )
                
                # Set appropriate retention based on log type
                retention_days = 30 if 'access' in log_group else 90
                
                self.logs_client.put_retention_policy(
                    logGroupName=log_group,
                    retentionInDays=retention_days
                )
                
                print(f"Created log group: {log_group}")
                
            except self.logs_client.exceptions.ResourceAlreadyExistsException:
                print(f"Log group already exists: {log_group}")
    
    def bulk_log_ingestion(self, log_group_name, log_stream_name, log_entries):
        """Efficiently send multiple log entries"""
        
        # CloudWatch Logs accepts max 10,000 events or 1MB per call
        batch_size = 100  # Conservative batch size
        
        for i in range(0, len(log_entries), batch_size):
            batch = log_entries[i:i + batch_size]
            
            log_events = [
                {
                    'timestamp': int(entry['timestamp'] * 1000),
                    'message': json.dumps(entry['message']) if isinstance(entry['message'], dict) else entry['message']
                }
                for entry in batch
            ]
            
            try:
                self.logs_client.put_log_events(
                    logGroupName=log_group_name,
                    logStreamName=log_stream_name,
                    logEvents=log_events
                )
            except Exception as e:
                print(f"Error sending batch {i//batch_size + 1}: {e}")

# Usage examples
def setup_comprehensive_logging():
    """Setup comprehensive logging infrastructure"""
    
    print("Setting up CloudWatch Logs infrastructure...")
    
    # Create log groups and streams
    create_log_group_and_streams()
    
    # Send sample log events
    send_log_events()
    
    # Setup metric filters
    setup_metric_filters()
    
    # Setup subscription filters
    setup_subscription_filters()
    
    # Export logs to S3
    export_task_id = export_logs_to_s3()
    
    # Use structured logging manager
    logs_manager = CloudWatchLogsManager()
    logs_manager.create_structured_logging('ecommerce-app', 'production')
    
    print("CloudWatch Logs setup complete!")
    
    return {
        'export_task_id': export_task_id,
        'status': 'completed'
    }

# Execute setup
setup_result = setup_comprehensive_logging()
print(f"Setup result: {setup_result}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# CloudWatch Logs Insights Queries - Advanced Examples
import boto3
from datetime import datetime, timedelta

logs_client = boto3.client('logs')

def execute_logs_insights_query(query_string, log_groups, start_time, end_time):
    """Execute a CloudWatch Logs Insights query"""
    
    # Start the query
    response = logs_client.start_query(
        logGroupNames=log_groups,
        startTime=int(start_time.timestamp()),
        endTime=int(end_time.timestamp()),
        queryString=query_string,
        limit=10000
    )
    
    query_id = response['queryId']
    print(f"Query started with ID: {query_id}")
    
    # Wait for query to complete
    while True:
        result = logs_client.get_query_results(queryId=query_id)
        
        if result['status'] == 'Complete':
            return result['results']
        elif result['status'] == 'Failed':
            print(f"Query failed: {result}")
            return None
        
        print("Query running...")
        time.sleep(2)

# 1. Error Analysis Queries
def analyze_application_errors():
    """Analyze application errors and patterns"""
    
    error_analysis_queries = {
        
        # Find most common errors
        "common_errors": """
            fields @timestamp, @message
            | filter @message like /ERROR/
            | stats count() as error_count by @message
            | sort error_count desc
            | limit 20
        """,
        
        # Error trends over time
        "error_trends": """
            fields @timestamp, @message
            | filter @message like /ERROR/
            | stats count() as errors by bin(5m)
            | sort @timestamp desc
        """,
        
        # Errors by user or session
        "errors_by_user": """
            fields @timestamp, @message
            | filter @message like /ERROR/
            | parse @message /user:(?<user_id>[^\\s]+)/
            | stats count() as error_count by user_id
            | sort error_count desc
            | limit 50
        """,
        
        # Database connection errors
        "db_connection_errors": """
            fields @timestamp, @message
            | filter @message like /database/ and @message like /connection/
            | parse @message /timeout:(?<timeout_ms>\\d+)/
            | stats count() as connection_errors, avg(timeout_ms) as avg_timeout by bin(1h)
            | sort @timestamp desc
        """
    }
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    results = {}
    for query_name, query_string in error_analysis_queries.items():
        print(f"Executing query: {query_name}")
        results[query_name] = execute_logs_insights_query(
            query_string, 
            ['/myapp/application'],
            start_time,
            end_time
        )
    
    return results

# 2. Performance Analysis Queries
def analyze_application_performance():
    """Analyze application performance metrics from logs"""
    
    performance_queries = {
        
        # Response time analysis
        "response_times": """
            fields @timestamp, @message
            | filter @message like /response_time/
            | parse @message /response_time:(?<response_time>\\d+\\.?\\d*)/
            | parse @message /endpoint:(?<endpoint>[^\\s]+)/
            | stats avg(response_time) as avg_response, 
                   max(response_time) as max_response,
                   min(response_time) as min_response,
                   count() as request_count by endpoint
            | sort avg_response desc
        """,
        
        # Slow queries identification
        "slow_queries": """
            fields @timestamp, @message
            | filter @message like /query_time/ and @message like /SELECT/
            | parse @message /query_time:(?<query_time>\\d+\\.?\\d*)/
            | parse @message /query:"(?<sql_query>[^"]+)"/
            | filter query_time > 1.0
            | stats avg(query_time) as avg_time, 
                   count() as occurrence_count by sql_query
            | sort avg_time desc
            | limit 20
        """,
        
        # Memory usage patterns
        "memory_usage": """
            fields @timestamp, @message
            | filter @message like /memory_usage/
            | parse @message /memory_usage:(?<memory_mb>\\d+)/
            | parse @message /process:(?<process_name>[^\\s]+)/
            | stats avg(memory_mb) as avg_memory,
                   max(memory_mb) as peak_memory by process_name, bin(10m)
            | sort @timestamp desc
        """,
        
        # API endpoint performance
        "api_performance": """
            fields @timestamp, @message
            | filter @message like /API/
            | parse @message /method:(?<http_method>\\w+)/
            | parse @message /endpoint:(?<api_endpoint>[^\\s]+)/
            | parse @message /status:(?<status_code>\\d+)/
            | parse @message /duration:(?<duration_ms>\\d+)/
            | stats count() as total_requests,
                   avg(duration_ms) as avg_duration,
                   sum(case status_code >= 400 when 1 else 0 end) as error_count
                   by api_endpoint, http_method
            | eval error_rate = error_count / total_requests * 100
            | sort total_requests desc
        """
    }
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=6)
    
    results = {}
    for query_name, query_string in performance_queries.items():
        print(f"Executing performance query: {query_name}")
        results[query_name] = execute_logs_insights_query(
            query_string,
            ['/myapp/application', '/myapp/access'],
            start_time,
            end_time
        )
    
    return results

# 3. Security Analysis Queries  
def analyze_security_events():
    """Analyze security-related events from logs"""
    
    security_queries = {
        
        # Failed login attempts
        "failed_logins": """
            fields @timestamp, @message
            | filter @message like /login/ and @message like /failed/
            | parse @message /ip:(?<source_ip>[\\d\\.]+)/
            | parse @message /user:(?<username>[^\\s]+)/
            | stats count() as failed_attempts by source_ip, username
            | sort failed_attempts desc
            | limit 100
        """,
        
        # Suspicious IP activity
        "suspicious_ips": """
            fields @timestamp, @message
            | parse @message /ip:(?<source_ip>[\\d\\.]+)/
            | filter source_ip != ""
            | stats count() as request_count, 
                   count_distinct(@timestamp) as unique_timestamps,
                   earliest(@timestamp) as first_seen,
                   latest(@timestamp) as last_seen
                   by source_ip
            | filter request_count > 1000
            | sort request_count desc
        """,
        
        # Privilege escalation attempts
        "privilege_escalation": """
            fields @timestamp, @message
            | filter @message like /sudo/ or @message like /admin/ or @message like /privilege/
            | parse @message /user:(?<user>[^\\s]+)/
            | parse @message /action:(?<action>[^\\s]+)/
            | stats count() as escalation_attempts by user, action
            | sort escalation_attempts desc
        """,
        
        # Unusual access patterns
        "unusual_access": """
            fields @timestamp, @message
            | filter @message like /access/
            | parse @message /user:(?<user>[^\\s]+)/
            | parse @message /resource:(?<resource>[^\\s]+)/
            | parse @message /time:(?<access_time>[\\d\\-\\s:]+)/
            | parse access_time /(?<hour>\\d{2}):/
            | stats count() as access_count by user, hour
            | filter hour >= 22 or hour <= 6  # After hours access
            | sort access_count desc
        """
    }
    
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)
    
    results = {}
    for query_name, query_string in security_queries.items():
        print(f"Executing security query: {query_name}")
        results[query_name] = execute_logs_insights_query(
            query_string,
            ['/myapp/application', '/myapp/audit'],
            start_time,
            end_time
        )
    
    return results

# 4. Business Intelligence Queries
def analyze_business_metrics():
    """Extract business insights from application logs"""
    
    business_queries = {
        
        # User activity analysis
        "user_activity": """
            fields @timestamp, @message
            | filter @message like /user_action/
            | parse @message /user:(?<user_id>[^\\s]+)/
            | parse @message /action:(?<action>[^\\s]+)/
            | parse @message /session:(?<session_id>[^\\s]+)/
            | stats count() as action_count,
                   count_distinct(session_id) as unique_sessions
                   by user_id, action
            | sort action_count desc
        """,
        
        # Sales conversion funnel
        "conversion_funnel": """
            fields @timestamp, @message
            | filter @message like /funnel/
            | parse @message /stage:(?<funnel_stage>[^\\s]+)/
            | parse @message /user:(?<user_id>[^\\s]+)/
            | stats count() as stage_count by funnel_stage
            | sort case 
                when funnel_stage = "landing" then 1
                when funnel_stage = "product_view" then 2  
                when funnel_stage = "add_to_cart" then 3
                when funnel_stage = "checkout" then 4
                when funnel_stage = "purchase" then 5
                else 6 end
        """,
        
        # Feature usage analytics
        "feature_usage": """
            fields @timestamp, @message
            | filter @message like /feature_used/
            | parse @message /feature:(?<feature_name>[^\\s]+)/
            | parse @message /user_type:(?<user_type>[^\\s]+)/
            | stats count() as usage_count by feature_name, user_type
            | sort usage_count desc
        """,
        
        # Revenue tracking
        "revenue_analysis": """
            fields @timestamp, @message
            | filter @message like /transaction/
            | parse @message /amount:(?<amount>[\\d\\.]+)/
            | parse @message /currency:(?<currency>[A-Z]{3})/
            | parse @message /product:(?<product_id>[^\\s]+)/
            | filter currency = "USD"
            | stats sum(amount) as total_revenue,
                   avg(amount) as avg_transaction,
                   count() as transaction_count
                   by product_id, bin(1h)
            | sort total_revenue desc
        """
    }
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=12)
    
    results = {}
    for query_name, query_string in business_queries.items():
        print(f"Executing business query: {query_name}")
        results[query_name] = execute_logs_insights_query(
            query_string,
            ['/myapp/application'],
            start_time,
            end_time
        )
    
    return results

def create_saved_queries():
    """Create saved queries for repeated analysis"""
    
    # Note: Saved queries are managed through the console
    # This function demonstrates how to organize query templates
    
    saved_query_templates = {
        "error_analysis": {
            "description": "Find and analyze application errors",
            "query": """
                fields @timestamp, @message, @logStream
                | filter @message like /ERROR/
                | stats count() by @message
                | sort count desc
                | limit 20
            """,
            "log_groups": ["/myapp/application"],
            "time_range": "24h"
        },
        
        "top_api_endpoints": {
            "description": "Most frequently accessed API endpoints",
            "query": """
                fields @timestamp, @message
                | parse @message /(?<method>GET|POST|PUT|DELETE)\\s+(?<endpoint>\\/[^\\s]+)/
                | stats count() as request_count by method, endpoint
                | sort request_count desc
                | limit 50
            """,
            "log_groups": ["/myapp/access"],
            "time_range": "1h"
        }
    }
    
    return saved_query_templates

# Execute comprehensive log analysis
def run_comprehensive_analysis():
    """Run comprehensive log analysis across all categories"""
    
    print("🔍 Starting comprehensive log analysis...")
    
    # Error analysis
    print("\n📊 Analyzing errors...")
    error_results = analyze_application_errors()
    
    # Performance analysis  
    print("\n⚡ Analyzing performance...")
    performance_results = analyze_application_performance()
    
    # Security analysis
    print("\n🔒 Analyzing security events...")
    security_results = analyze_security_events()
    
    # Business analysis
    print("\n💼 Analyzing business metrics...")
    business_results = analyze_business_metrics()
    
    print("\n✅ Comprehensive analysis complete!")
    
    return {
        'errors': error_results,
        'performance': performance_results,
        'security': security_results,
        'business': business_results
    }

# Run the analysis
analysis_results = run_comprehensive_analysis()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# CloudWatch Logs Monitoring and Alerting
import boto3
import json

logs_client = boto3.client('logs')
cloudwatch = boto3.client('cloudwatch')
sns = boto3.client('sns')

def setup_log_based_alarms():
    """Create CloudWatch alarms based on log metrics"""
    
    # Alarm for high error rate
    cloudwatch.put_metric_alarm(
        AlarmName='HighApplicationErrorRate',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=2,
        MetricName='ApplicationErrors',
        Namespace='MyApp/Logs',
        Period=300,
        Statistic='Sum',
        Threshold=50,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-east-1:123456789012:high-priority-alerts'
        ],
        AlarmDescription='Alert when application errors exceed 50 in 5 minutes',
        TreatMissingData='notBreaching'
    )
    
    # Alarm for HTTP 5xx errors
    cloudwatch.put_metric_alarm(
        AlarmName='HTTP5xxErrorSpike',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=3,
        MetricName='HTTP5xxCount',
        Namespace='MyApp/HTTP',
        Period=60,
        Statistic='Sum',
        Threshold=10,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-east-1:123456789012:critical-alerts',
            'arn:aws:lambda:us-east-1:123456789012:function:AutoRestartService'
        ],
        AlarmDescription='Alert on HTTP 5xx error spike'
    )
    
    # Alarm for slow response times
    cloudwatch.put_metric_alarm(
        AlarmName='SlowResponseTime',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=2,
        MetricName='ResponseTime',
        Namespace='MyApp/Performance',
        Period=300,
        Statistic='Average',
        Threshold=2.0,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-east-1:123456789012:performance-alerts'
        ],
        AlarmDescription='Alert when average response time exceeds 2 seconds'
    )

def create_log_anomaly_detection():
    """Setup anomaly detection for log-based metrics"""
    
    # Create anomaly detector for error rate
    cloudwatch.put_anomaly_detector(
        Namespace='MyApp/Logs',
        MetricName='ApplicationErrors',
        Stat='Average'
    )
    
    # Create alarm based on anomaly detection
    cloudwatch.put_metric_alarm(
        AlarmName='ApplicationErrorAnomalyAlarm',
        ComparisonOperator='LessThanLowerOrGreaterThanUpperThreshold',
        EvaluationPeriods=2,
        Metrics=[
            {
                'Id': 'm1',
                'ReturnData': True,
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'MyApp/Logs',
                        'MetricName': 'ApplicationErrors'
                    },
                    'Period': 300,
                    'Stat': 'Average'
                }
            },
            {
                'Id': 'ad1',
                'Expression': 'ANOMALY_DETECTION_FUNCTION(m1, 2)'
            }
        ],
        ThresholdMetricId='ad1',
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-east-1:123456789012:anomaly-alerts'
        ],
        AlarmDescription='Detect anomalous error patterns'
    )

def setup_real_time_log_processing():
    """Setup real-time log processing with Lambda"""
    
    # Lambda function code for processing logs
    lambda_function_code = """
import json
import gzip
import base64
import boto3

def lambda_handler(event, context):
    """Process CloudWatch Logs data in real-time"""
    
    # Decode and decompress log data
    compressed_payload = base64.b64decode(event['awslogs']['data'])
    uncompressed_payload = gzip.decompress(compressed_payload)
    log_data = json.loads(uncompressed_payload)
    
    sns = boto3.client('sns')
    
    # Process each log event
    for log_event in log_data['logEvents']:
        message = log_event['message']
        timestamp = log_event['timestamp']
        
        # Check for critical errors
        if 'CRITICAL' in message or 'FATAL' in message:
            alert_message = f"""
            CRITICAL ERROR DETECTED:
            Timestamp: {timestamp}
            Message: {message}
            Log Group: {log_data['logGroup']}
            Log Stream: {log_data['logStream']}
            """
            
            # Send immediate alert
            sns.publish(
                TopicArn='arn:aws:sns:us-east-1:123456789012:critical-alerts',
                Subject='CRITICAL: Application Error',
                Message=alert_message
            )
        
        # Check for security events
        if any(keyword in message.lower() for keyword in ['unauthorized', 'forbidden', 'breach']):
            security_alert = f"""
            SECURITY EVENT:
            Timestamp: {timestamp}
            Event: {message}
            Source: {log_data['logGroup']}
            """
            
            sns.publish(
                TopicArn='arn:aws:sns:us-east-1:123456789012:security-alerts',
                Subject='Security Event Detected',
                Message=security_alert
            )
    
    return {'statusCode': 200, 'body': 'Log events processed'}
    """
    
    # Create subscription filter to trigger Lambda
    logs_client.put_subscription_filter(
        logGroupName='/myapp/application',
        filterName='RealTimeProcessing',
        filterPattern='CRITICAL ERROR FATAL unauthorized forbidden breach',
        destinationArn='arn:aws:lambda:us-east-1:123456789012:function:LogProcessor'
    )

def create_log_dashboard():
    """Create CloudWatch dashboard for log monitoring"""
    
    dashboard_body = {
        "widgets": [
            {
                "type": "log",
                "x": 0,
                "y": 0,
                "width": 24,
                "height": 8,
                "properties": {
                    "query": "SOURCE '/myapp/application'\n| fields @timestamp, @message\n| filter @message like /ERROR/\n| sort @timestamp desc\n| limit 50",
                    "region": "us-east-1",
                    "title": "Recent Application Errors",
                    "view": "table"
                }
            },
            {
                "type": "metric",
                "x": 0,
                "y": 8,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["MyApp/Logs", "ApplicationErrors"],
                        ["MyApp/HTTP", "HTTP4xxCount"],
                        ["MyApp/HTTP", "HTTP5xxCount"]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": "us-east-1",
                    "title": "Error Trends",
                    "period": 300
                }
            },
            {
                "type": "log",
                "x": 12,
                "y": 8,
                "width": 12,
                "height": 6,
                "properties": {
                    "query": "SOURCE '/myapp/access'\n| parse @message /(?<ip>\\d+\\.\\d+\\.\\d+\\.\\d+)/\n| stats count() as requests by ip\n| sort requests desc\n| limit 20",
                    "region": "us-east-1",
                    "title": "Top IP Addresses",
                    "view": "table"
                }
            },
            {
                "type": "metric",
                "x": 0,
                "y": 14,
                "width": 24,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["MyApp/Performance", "ResponseTime"]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": "us-east-1",
                    "title": "Application Response Time",
                    "period": 300,
                    "annotations": {
                        "horizontal": [
                            {
                                "label": "SLA Threshold",
                                "value": 2.0
                            }
                        ]
                    }
                }
            }
        ]
    }
    
    cloudwatch.put_dashboard(
        DashboardName='LogMonitoring',
        DashboardBody=json.dumps(dashboard_body)
    )

def setup_log_retention_automation():
    """Automate log retention and lifecycle management"""
    
    # Lambda function for automated log group management
    lambda_code = """
import boto3
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """Manage log group retention and cleanup"""
    
    logs_client = boto3.client('logs')
    
    # Get all log groups
    paginator = logs_client.get_paginator('describe_log_groups')
    
    for page in paginator.paginate():
        for log_group in page['logGroups']:
            log_group_name = log_group['logGroupName']
            
            # Set retention policy based on log group name
            if '/debug/' in log_group_name:
                retention_days = 7
            elif '/access/' in log_group_name:
                retention_days = 90
            elif '/application/' in log_group_name:
                retention_days = 30
            else:
                retention_days = 14
            
            # Update retention policy if different
            current_retention = log_group.get('retentionInDays')
            if current_retention != retention_days:
                logs_client.put_retention_policy(
                    logGroupName=log_group_name,
                    retentionInDays=retention_days
                )
                print(f"Updated retention for {log_group_name} to {retention_days} days")
            
            # Delete empty log groups older than 30 days
            creation_time = datetime.fromtimestamp(log_group['creationTime'] / 1000)
            if creation_time < datetime.now() - timedelta(days=30):
                # Check if log group has any recent log streams
                streams = logs_client.describe_log_streams(
                    logGroupName=log_group_name,
                    limit=1
                )
                
                if not streams['logStreams']:
                    logs_client.delete_log_group(logGroupName=log_group_name)
                    print(f"Deleted empty log group: {log_group_name}")
    
    return {'statusCode': 200, 'body': 'Log retention management completed'}
    """

def create_log_alerts_for_business_metrics():
    """Create alerts for business-critical log events"""
    
    # Metric filter for payment failures
    logs_client.put_metric_filter(
        logGroupName='/myapp/application',
        filterName='PaymentFailures',
        filterPattern='[timestamp, request_id, level="ERROR", service="payment", ...]',
        metricTransformations=[
            {
                'metricName': 'PaymentFailures',
                'metricNamespace': 'MyApp/Business',
                'metricValue': '1',
                'defaultValue': 0
            }
        ]
    )
    
    # Alarm for payment failures
    cloudwatch.put_metric_alarm(
        AlarmName='HighPaymentFailureRate',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=2,
        MetricName='PaymentFailures',
        Namespace='MyApp/Business',
        Period=300,
        Statistic='Sum',
        Threshold=5,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-east-1:123456789012:business-critical'
        ],
        AlarmDescription='Alert when payment failures exceed 5 in 5 minutes'
    )
    
    # Metric filter for user registration events
    logs_client.put_metric_filter(
        logGroupName='/myapp/application',
        filterName='UserRegistrations',
        filterPattern='[timestamp, request_id, level="INFO", event="user_registered", ...]',
        metricTransformations=[
            {
                'metricName': 'UserRegistrations',
                'metricNamespace': 'MyApp/Business',
                'metricValue': '1',
                'defaultValue': 0
            }
        ]
    )

# Setup comprehensive log monitoring
def setup_complete_log_monitoring():
    """Setup complete log monitoring and alerting system"""
    
    print("🚀 Setting up comprehensive log monitoring...")
    
    # Setup log-based alarms
    setup_log_based_alarms()
    
    # Setup anomaly detection
    create_log_anomaly_detection()
    
    # Setup real-time processing
    setup_real_time_log_processing()
    
    # Create monitoring dashboard
    create_log_dashboard()
    
    # Setup retention automation
    setup_log_retention_automation()
    
    # Create business metric alerts
    create_log_alerts_for_business_metrics()
    
    print("✅ Complete log monitoring system deployed!")

# Execute complete setup
setup_complete_log_monitoring()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

# Helper functions
def get_events_for_service(service):
    """Get common events for AWS service"""
    event_mappings = {
        "s3.amazonaws.com": ["GetObject", "PutObject", "DeleteObject", "CreateBucket", "DeleteBucket"],
        "ec2.amazonaws.com": ["RunInstances", "TerminateInstances", "StartInstances", "StopInstances"],
        "iam.amazonaws.com": ["CreateUser", "DeleteUser", "AttachUserPolicy", "CreateRole"],
        "lambda.amazonaws.com": ["CreateFunction", "InvokeFunction", "UpdateFunctionCode"],
        "rds.amazonaws.com": ["CreateDBInstance", "DeleteDBInstance", "ModifyDBInstance"],
        "cloudformation.amazonaws.com": ["CreateStack", "UpdateStack", "DeleteStack"]
    }
    return event_mappings.get(service, ["GetObject", "PutObject"])

def generate_cloudtrail_event(event_source, event_name, user_identity, aws_region):
    """Generate sample CloudTrail event"""
    return {
        "eventVersion": "1.08",
        "userIdentity": {
            "type": user_identity,
            "principalId": "AIDACKCEVSQ6C2EXAMPLE",
            "arn": "arn:aws:iam::123456789012:user/johndoe",
            "accountId": "123456789012",
            "userName": "johndoe"
        },
        "eventTime": datetime.now().isoformat() + "Z",
        "eventSource": event_source,
        "eventName": event_name,
        "awsRegion": aws_region,
        "sourceIPAddress": "203.0.113.12",
        "userAgent": "aws-cli/2.0.55 Python/3.8.5",
        "resources": [
            {
                "resourceType": "AWS::S3::Bucket" if "s3" in event_source else "AWS::EC2::Instance",
                "resourceName": "example-bucket" if "s3" in event_source else "i-1234567890abcdef0"
            }
        ],
        "responseElements": None,
        "requestID": "12345678-1234-1234-1234-123456789012",
        "eventID": "87654321-4321-4321-4321-210987654321",
        "readOnly": event_name.startswith("Get") or event_name.startswith("List"),
        "eventType": "AwsApiCall",
        "managementEvent": True,
        "recipientAccountId": "123456789012"
    }

def generate_lake_query(query_type, time_range, event_category):
    """Generate CloudTrail Lake query based on selections"""
    
    time_filter = {
        "Last 1 hour": "eventTime >= '2024-07-14 13:00:00'",
        "Last 24 hours": "eventTime >= '2024-07-13 14:00:00'", 
        "Last 7 days": "eventTime >= '2024-07-07 14:00:00'",
        "Last 30 days": "eventTime >= '2024-06-14 14:00:00'"
    }.get(time_range, "eventTime >= '2024-07-13 14:00:00'")
    
    query_templates = {
        "Security Analysis": f"""
SELECT 
    eventTime,
    userIdentity.type as userType,
    userIdentity.userName,
    eventName,
    sourceIPAddress,
    errorCode
FROM cloudtrail_events
WHERE {time_filter}
  AND (errorCode IS NOT NULL OR eventName LIKE '%Login%')
ORDER BY eventTime DESC
LIMIT 100;
        """,
        
        "User Activity": f"""
SELECT 
    userIdentity.userName,
    COUNT(*) as eventCount,
    COUNT(DISTINCT eventName) as uniqueActions,
    MIN(eventTime) as firstActivity,
    MAX(eventTime) as lastActivity
FROM cloudtrail_events  
WHERE {time_filter}
  AND userIdentity.type = 'IAMUser'
GROUP BY userIdentity.userName
ORDER BY eventCount DESC;
        """,
        
        "API Usage": f"""
SELECT 
    eventSource,
    eventName, 
    COUNT(*) as callCount,
    COUNT(DISTINCT userIdentity.userName) as uniqueUsers
FROM cloudtrail_events
WHERE {time_filter}
GROUP BY eventSource, eventName
HAVING COUNT(*) > 10
ORDER BY callCount DESC;
        """,
        
        "Error Analysis": f"""
SELECT 
    errorCode,
    errorMessage,
    eventName,
    COUNT(*) as errorCount
FROM cloudtrail_events
WHERE {time_filter}
  AND errorCode IS NOT NULL
GROUP BY errorCode, errorMessage, eventName
ORDER BY errorCount DESC;
        """,
        
        "Cost Analysis": f"""
SELECT 
    eventSource,
    eventName,
    awsRegion,
    COUNT(*) as operationCount
FROM cloudtrail_events
WHERE {time_filter}
  AND eventName IN ('RunInstances', 'CreateBucket', 'CreateFunction', 'CreateDBInstance')
GROUP BY eventSource, eventName, awsRegion
ORDER BY operationCount DESC;
        """
    }
    
    return query_templates.get(query_type, query_templates["Security Analysis"])

def generate_logs_query(log_group, query_template, time_range):
    """Generate CloudWatch Logs Insights query"""
    
    query_templates = {
        "Recent Errors": f"""
fields @timestamp, @message, @logStream
| filter @message like /ERROR/
| sort @timestamp desc  
| limit 50
        """,
        
        "Performance Analysis": f"""
fields @timestamp, @message
| filter @message like /response_time/
| parse @message /response_time:(?<response_time>\\d+\\.?\\d*)/
| stats avg(response_time) as avg_response, max(response_time) as max_response by bin(5m)
| sort @timestamp desc
        """,
        
        "IP Address Analysis": f"""
fields @timestamp, @message
| parse @message /(?<ip>\\d+\\.\\d+\\.\\d+\\.\\d+)/  
| stats count() as request_count by ip
| sort request_count desc
| limit 20
        """,
        
        "Custom Fields": f"""
fields @timestamp, @message
| parse @message /user:(?<user_id>[^\\s]+)/
| parse @message /action:(?<action>[^\\s]+)/
| stats count() as action_count by user_id, action
| sort action_count desc
        """,
        
        "Time Range Filter": f"""
fields @timestamp, @message, @logStream
| filter @timestamp >= "2024-07-14T12:00:00.000Z"
| sort @timestamp desc
| limit 100
        """
    }
    
    return query_templates.get(query_template, query_templates["Recent Errors"])

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
    # 📊 AWS Monitoring & Logging Services
    <div class='info-box'>
    Master AWS audit logging, monitoring, and log management with CloudTrail, CloudTrail Lake, CloudWatch, and CloudWatch Logs for comprehensive observability and compliance.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🔍 Amazon CloudTrail",
        "🏞️ Amazon CloudTrail Lake", 
        "📊 Amazon CloudWatch",
        "📝 Amazon CloudWatch Logs"
    ])
    
    with tab1:
        cloudtrail_tab()
    
    with tab2:
        cloudtrail_lake_tab()
    
    with tab3:
        cloudwatch_tab()
    
    with tab4:
        cloudwatch_logs_tab()
    
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
