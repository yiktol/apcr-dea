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
import time

# Page configuration
st.set_page_config(
    page_title="AWS Data Operations & Event-Driven Architecture",
    page_icon="🔄",
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
        
        .architecture-box {{
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
        
        .metric-card {{
            background: linear-gradient(135deg, {AWS_COLORS['primary']} 0%, {AWS_COLORS['light_blue']} 100%);
            padding: 20px;
            border-radius: 15px;
            color: white;
            text-align: center;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            margin: 10px 0;
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
    common.initialize_session_state()
    if 'session_started' not in st.session_state:
        st.session_state.session_started = True
        st.session_state.events_processed = 0
        st.session_state.api_calls_made = 0
        st.session_state.architectures_explored = []

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🎯 Amazon EventBridge - Serverless event-driven architecture
            - 🔌 Redshift Data API - HTTP endpoint for SQL execution
            - 💻 Data API Code Examples - Python implementation patterns
            - 🏗️ Event-Driven Architecture - Complete solution patterns
            
            **Learning Objectives:**
            - Master event-driven architecture patterns
            - Understand Redshift Data API capabilities
            - Learn serverless data processing techniques
            - Explore automated pipeline orchestration
            """)

def create_eventbridge_architecture():
    """Create mermaid diagram for EventBridge architecture"""
    return """
    graph TB
        subgraph "Event Sources"
            APP[Applications]
            S3[Amazon S3]
            RDS[Amazon RDS]
            CUSTOM[Custom Applications]
        end
        
        subgraph "Amazon EventBridge"
            EB[EventBridge<br/>Event Bus]
            RULES[Event Rules]
            FILTER[Event Filtering]
        end
        
        subgraph "Event Targets"
            LAMBDA[AWS Lambda]
            SQS[Amazon SQS]
            SNS[Amazon SNS]
            STEP[Step Functions]
            KINESIS[Kinesis Data Streams]
            REDSHIFT[Amazon Redshift]
        end
        
        APP --> EB
        S3 --> EB
        RDS --> EB
        CUSTOM --> EB
        
        EB --> RULES
        RULES --> FILTER
        FILTER --> LAMBDA
        FILTER --> SQS
        FILTER --> SNS
        FILTER --> STEP
        FILTER --> KINESIS
        FILTER --> REDSHIFT
        
        style EB fill:#FF9900,stroke:#232F3E,color:#fff
        style RULES fill:#4B9EDB,stroke:#232F3E,color:#fff
        style LAMBDA fill:#3FB34F,stroke:#232F3E,color:#fff
        style REDSHIFT fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_redshift_data_api_flow():
    """Create mermaid diagram for Redshift Data API flow"""
    return """
    graph LR
        subgraph "Client Applications"
            WEB[Web Applications]
            MOBILE[Mobile Apps]
            JUPYTER[Jupyter Notebooks]
            BI[BI Tools]
        end
        
        subgraph "AWS Services"
            LAMBDA[AWS Lambda]
            EVENTBRIDGE[EventBridge]
            APPSYNC[AWS AppSync]
        end
        
        subgraph "Redshift Data API"
            API[Data API<br/>REST Endpoint]
            AUTH[Authentication<br/>IAM/Secrets Manager]
            ASYNC[Async Processing]
        end
        
        subgraph "Amazon Redshift"
            CLUSTER[Redshift Cluster]
            QUEUE[Query Queue]
            RESULTS[Query Results]
        end
        
        WEB --> API
        MOBILE --> API
        JUPYTER --> API
        BI --> API
        LAMBDA --> API
        EVENTBRIDGE --> API
        APPSYNC --> API
        
        API --> AUTH
        AUTH --> ASYNC
        ASYNC --> CLUSTER
        CLUSTER --> QUEUE
        QUEUE --> RESULTS
        RESULTS --> API
        
        style API fill:#FF9900,stroke:#232F3E,color:#fff
        style AUTH fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CLUSTER fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_event_driven_pipeline():
    """Create mermaid diagram for event-driven data pipeline"""
    return """
    graph TB
        START[Data Source Event] --> EB[EventBridge]
        EB --> RULE{Event Rule<br/>Matches?}
        RULE -->|Yes| LAMBDA[Lambda Function]
        RULE -->|No| IGNORE[Event Ignored]
        
        LAMBDA --> API[Redshift Data API]
        API --> SQL[Execute SQL]
        SQL --> RESULT{Query<br/>Complete?}
        
        RESULT -->|Success| SUCCESS[Success Handler]
        RESULT -->|Error| ERROR[Error Handler]
        
        SUCCESS --> SNS[SNS Notification]
        ERROR --> DLQ[Dead Letter Queue]
        
        SNS --> DASHBOARD[Update Dashboard]
        DLQ --> ALARM[CloudWatch Alarm]
        
        style EB fill:#FF9900,stroke:#232F3E,color:#fff
        style LAMBDA fill:#3FB34F,stroke:#232F3E,color:#fff
        style API fill:#4B9EDB,stroke:#232F3E,color:#fff
        style SUCCESS fill:#3FB34F,stroke:#232F3E,color:#fff
        style ERROR fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def eventbridge_tab():
    """Content for Amazon EventBridge tab"""
    st.markdown("## 🎯 Amazon EventBridge")
    st.markdown("*Serverless service for building event-driven, loosely-coupled applications*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🔋 EventBridge Core Capabilities
    Amazon EventBridge is a serverless event bus service that connects applications using data from:
    - **AWS Services**: Native integration with 90+ AWS services
    - **SaaS Applications**: Built-in connectors for popular SaaS platforms
    - **Custom Applications**: Send events via API calls
    - **Event Filtering**: Route events based on content and rules
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # EventBridge Architecture
    st.markdown("#### 🏗️ EventBridge Architecture")
    common.mermaid(create_eventbridge_architecture(), height=700)
    
    # Interactive Event Rule Builder
    st.markdown("#### 🎛️ Interactive Event Rule Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Event Source Configuration")
        event_source = st.selectbox("Event Source", [
            "aws.s3", "aws.rds", "aws.ec2", "custom.application", "aws.kinesis"
        ])
        
        event_type = st.selectbox("Event Type", [
            "Object Created", "Database Change", "Instance State Change", 
            "Custom Event", "Stream Record"
        ])
        
        detail_type = st.text_input("Detail Type", "S3 Bucket Notification")
    
    with col2:
        st.markdown("##### Target Configuration")
        target_service = st.selectbox("Target Service", [
            "AWS Lambda", "Amazon SQS", "Amazon SNS", "Step Functions", 
            "Kinesis Data Streams", "Amazon Redshift"
        ])
        
        target_action = st.selectbox("Target Action", [
            "Process Event", "Queue Message", "Send Notification", 
            "Start Workflow", "Stream Data", "Execute Query"
        ])
    
    # Generate event rule
    event_rule = generate_event_rule(event_source, event_type, detail_type, target_service)
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 📋 Generated Event Rule
    **Rule Name**: {event_rule['name']}  
    **Event Pattern**: Matches {event_source} {event_type} events  
    **Target**: {target_service} - {target_action}  
    **Estimated Cost**: ${event_rule['estimated_cost']}/month for 1M events
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Event pattern examples
    st.markdown("#### 📝 Common Event Patterns")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="architecture-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗂️ S3 Events
        **Event Pattern:**
        ```json
        {
          "source": ["aws.s3"],
          "detail-type": ["Object Created"],
          "detail": {
            "bucket": {
              "name": ["data-lake-bucket"]
            }
          }
        }
        ```
        **Use Case:** Trigger data processing
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="architecture-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗄️ RDS Events
        **Event Pattern:**
        ```json
        {
          "source": ["aws.rds"],
          "detail-type": ["RDS DB Instance Event"],
          "detail": {
            "EventCategories": ["backup"]
          }
        }
        ```
        **Use Case:** Monitor database operations
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="architecture-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏗️ Custom Events
        **Event Pattern:**
        ```json
        {
          "source": ["myapp.orders"],
          "detail-type": ["Order Placed"],
          "detail": {
            "amount": [{"numeric": [">", 1000]}]
          }
        }
        ```
        **Use Case:** Business logic triggers
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 EventBridge Implementation")
    
    tab1, tab2, tab3 = st.tabs(["Setup & Configuration", "Event Rules", "Target Integration"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# EventBridge Setup and Configuration
import boto3
import json
from datetime import datetime

# Initialize EventBridge client
eventbridge = boto3.client('events')

class EventBridgeManager:
    def __init__(self, region='us-west-2'):
        self.client = boto3.client('events', region_name=region)
        self.region = region
    
    def create_custom_event_bus(self, bus_name, description=""):
        """Create a custom event bus for application events"""
        try:
            response = self.client.create_event_bus(
                Name=bus_name,
                Description=description,
                Tags=[
                    {'Key': 'Environment', 'Value': 'Production'},
                    {'Key': 'Application', 'Value': 'DataPipeline'}
                ]
            )
            print(f"✅ Created event bus: {bus_name}")
            return response['EventBusArn']
        except Exception as e:
            print(f"❌ Error creating event bus: {e}")
            return None
    
    def create_event_rule(self, rule_name, event_pattern, description=""):
        """Create an event rule with specified pattern"""
        try:
            response = self.client.put_rule(
                Name=rule_name,
                EventPattern=json.dumps(event_pattern),
                State='ENABLED',
                Description=description,
                Tags=[
                    {'Key': 'Purpose', 'Value': 'DataProcessing'},
                    {'Key': 'CreatedBy', 'Value': 'DataTeam'}
                ]
            )
            print(f"✅ Created rule: {rule_name}")
            return response['RuleArn']
        except Exception as e:
            print(f"❌ Error creating rule: {e}")
            return None
    
    def add_lambda_target(self, rule_name, lambda_arn, input_transformer=None):
        """Add Lambda function as target for event rule"""
        try:
            targets = [{
                'Id': '1',
                'Arn': lambda_arn,
                'InputTransformer': input_transformer
            }]
            
            response = self.client.put_targets(
                Rule=rule_name,
                Targets=targets
            )
            
            # Add Lambda permission for EventBridge
            lambda_client = boto3.client('lambda')
            try:
                lambda_client.add_permission(
                    FunctionName=lambda_arn,
                    StatementId=f'eventbridge-{rule_name}',
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com',
                    SourceArn=f'arn:aws:events:{self.region}:*:rule/{rule_name}'
                )
            except lambda_client.exceptions.ResourceConflictException:
                pass  # Permission already exists
            
            print(f"✅ Added Lambda target to rule: {rule_name}")
            return True
        except Exception as e:
            print(f"❌ Error adding Lambda target: {e}")
            return False
    
    def send_custom_event(self, source, detail_type, detail, bus_name='default'):
        """Send a custom event to EventBridge"""
        try:
            response = self.client.put_events(
                Entries=[
                    {
                        'Source': source,
                        'DetailType': detail_type,
                        'Detail': json.dumps(detail),
                        'EventBusName': bus_name,
                        'Time': datetime.utcnow()
                    }
                ]
            )
            
            if response['FailedEntryCount'] == 0:
                print(f"✅ Event sent successfully: {detail_type}")
                return True
            else:
                print(f"❌ Failed to send event: {response['Entries'][0].get('ErrorMessage')}")
                return False
        except Exception as e:
            print(f"❌ Error sending event: {e}")
            return False

# Example usage
eb_manager = EventBridgeManager()

# 1. Create custom event bus for data pipeline events
data_bus_arn = eb_manager.create_custom_event_bus(
    bus_name='data-pipeline-events',
    description='Event bus for data processing pipeline'
)

# 2. Create event rule for S3 object creation
s3_event_pattern = {
    "source": ["aws.s3"],
    "detail-type": ["Object Created"],
    "detail": {
        "bucket": {
            "name": ["data-lake-raw", "data-lake-processed"]
        },
        "object": {
            "key": [{"prefix": "data/"}]
        }
    }
}

rule_arn = eb_manager.create_event_rule(
    rule_name='s3-data-processing',
    event_pattern=s3_event_pattern,
    description='Trigger data processing on S3 uploads'
)

# 3. Send custom application event
custom_event_detail = {
    "orderId": "12345",
    "customerId": "CUST001",
    "amount": 1500.00,
    "timestamp": datetime.utcnow().isoformat(),
    "priority": "high"
}

eb_manager.send_custom_event(
    source='ecommerce.orders',
    detail_type='Order Placed',
    detail=custom_event_detail
)

print("🎯 EventBridge setup complete!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Advanced Event Rules and Filtering
import boto3
import json

class AdvancedEventRules:
    def __init__(self):
        self.client = boto3.client('events')
    
    def create_data_quality_rule(self):
        """Create rule for data quality events with complex filtering"""
        event_pattern = {
            "source": ["data.quality"],
            "detail-type": ["Data Quality Check"],
            "detail": {
                "status": ["FAILED", "WARNING"],
                "severity": ["HIGH", "CRITICAL"],
                "table": [{"exists": True}],
                "error_count": [{"numeric": [">", 10]}],
                "data_freshness": [{"numeric": ["<", 24]}]  # Hours
            }
        }
        
        return self.client.put_rule(
            Name='data-quality-alerts',
            EventPattern=json.dumps(event_pattern),
            State='ENABLED',
            Description='Alert on critical data quality issues'
        )
    
    def create_cost_optimization_rule(self):
        """Create rule for cost optimization events"""
        event_pattern = {
            "source": ["aws.cost"],
            "detail-type": ["Cost Anomaly Detection"],
            "detail": {
                "anomalyScore": [{"numeric": [">", 0.8]}],
                "impact": {
                    "maxImpact": [{"numeric": [">", 100]}]
                },
                "service": ["Amazon Redshift", "Amazon RDS", "Amazon EMR"]
            }
        }
        
        return self.client.put_rule(
            Name='cost-anomaly-alerts',
            EventPattern=json.dumps(event_pattern),
            State='ENABLED',
            Description='Alert on significant cost anomalies'
        )
    
    def create_schedule_based_rule(self):
        """Create schedule-based rule for periodic data processing"""
        # Cron expression: every day at 2 AM UTC
        schedule_expression = 'cron(0 2 * * ? *)'
        
        return self.client.put_rule(
            Name='daily-data-refresh',
            ScheduleExpression=schedule_expression,
            State='ENABLED',
            Description='Daily data refresh at 2 AM UTC'
        )
    
    def create_multi_condition_rule(self):
        """Create rule with multiple conditions and transformations"""
        event_pattern = {
            "source": ["aws.s3"],
            "detail-type": ["Object Created"],
            "detail": {
                "$or": [
                    {
                        "bucket": {"name": ["prod-data-lake"]},
                        "object": {
                            "size": [{"numeric": [">", 1048576]}],  # > 1MB
                            "key": [{"suffix": ".parquet"}]
                        }
                    },
                    {
                        "bucket": {"name": ["urgent-processing"]},
                        "object": {
                            "key": [{"prefix": "priority/"}]
                        }
                    }
                ]
            }
        }
        
        return self.client.put_rule(
            Name='conditional-data-processing',
            EventPattern=json.dumps(event_pattern),
            State='ENABLED',
            Description='Process large parquet files or priority data'
        )

# Event Pattern Templates
EVENT_PATTERNS = {
    "s3_data_events": {
        "source": ["aws.s3"],
        "detail-type": ["Object Created", "Object Deleted"],
        "detail": {
            "bucket": {"name": [{"prefix": "data-"}]},
            "object": {
                "key": [{"suffix": [".csv", ".json", ".parquet"]}]
            }
        }
    },
    
    "database_events": {
        "source": ["aws.rds"],
        "detail-type": ["RDS DB Instance Event"],
        "detail": {
            "EventCategories": ["backup", "failure", "maintenance"],
            "SourceId": [{"exists": True}]
        }
    },
    
    "custom_application_events": {
        "source": [{"prefix": "myapp."}],
        "detail-type": [{"anything-but": "Internal Event"}],
        "detail": {
            "priority": ["high", "critical"],
            "environment": ["production"]
        }
    },
    
    "cross_service_events": {
        "account": ["123456789012"],
        "region": ["us-west-2", "us-east-1"],
        "source": ["aws.batch", "aws.glue", "aws.emr"],
        "detail-type": ["Job State Change"],
        "detail": {
            "state": ["FAILED", "SUCCEEDED"]
        }
    }
}

# Input Transformers for Event Processing
INPUT_TRANSFORMERS = {
    "s3_to_lambda_transformer": {
        "InputPathsMap": {
            "bucket": "$.detail.bucket.name",
            "key": "$.detail.object.key",
            "size": "$.detail.object.size",
            "timestamp": "$.time"
        },
        "InputTemplate": json.dumps({
            "bucket_name": "<bucket>",
            "object_key": "<key>",
            "file_size": "<size>",
            "event_time": "<timestamp>",
            "processing_priority": "normal"
        })
    },
    
    "error_to_slack_transformer": {
        "InputPathsMap": {
            "error_message": "$.detail.errorMessage",
            "service": "$.source",
            "severity": "$.detail.severity"
        },
        "InputTemplate": json.dumps({
            "channel": "#data-alerts",
            "text": "🚨 <service> Error: <error_message>",
            "severity": "<severity>",
            "timestamp": "$.time"
        })
    }
}

# Usage examples
rules_manager = AdvancedEventRules()

# Create advanced rules
print("Creating advanced event rules...")
rules_manager.create_data_quality_rule()
rules_manager.create_cost_optimization_rule()
rules_manager.create_schedule_based_rule()
rules_manager.create_multi_condition_rule()

print("✅ Advanced event rules created successfully!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# EventBridge Target Integration Patterns
import boto3
import json

class EventBridgeTargets:
    def __init__(self):
        self.events_client = boto3.client('events')
        self.lambda_client = boto3.client('lambda')
        self.sqs_client = boto3.client('sqs')
        self.sns_client = boto3.client('sns')
    
    def add_lambda_target_with_dlq(self, rule_name, lambda_arn, dlq_arn):
        """Add Lambda target with Dead Letter Queue"""
        target = {
            'Id': '1',
            'Arn': lambda_arn,
            'DeadLetterConfig': {
                'Arn': dlq_arn
            },
            'RetryPolicy': {
                'MaximumRetryAttempts': 3,
                'MaximumEventAge': 3600  # 1 hour
            }
        }
        
        return self.events_client.put_targets(
            Rule=rule_name,
            Targets=[target]
        )
    
    def add_sqs_target_with_batch(self, rule_name, queue_arn):
        """Add SQS target with batching configuration"""
        target = {
            'Id': '1',
            'Arn': queue_arn,
            'SqsParameters': {
                'MessageGroupId': 'EventBridge-Messages'
            },
            'BatchParameters': {
                'JobName': 'EventProcessing',
                'JobQueue': 'event-processing-queue',
                'JobDefinition': 'event-processor'
            }
        }
        
        return self.events_client.put_targets(
            Rule=rule_name,
            Targets=[target]
        )
    
    def add_step_functions_target(self, rule_name, state_machine_arn, role_arn):
        """Add Step Functions target for workflow orchestration"""
        target = {
            'Id': '1',
            'Arn': state_machine_arn,
            'RoleArn': role_arn,
            'InputTransformer': {
                'InputPathsMap': {
                    'event_source': '$.source',
                    'event_detail': '$.detail',
                    'event_time': '$.time'
                },
                'InputTemplate': json.dumps({
                    'workflow_input': {
                        'source': '<event_source>',
                        'details': '<event_detail>',
                        'timestamp': '<event_time>',
                        'execution_id': '$.uuid()'
                    }
                })
            }
        }
        
        return self.events_client.put_targets(
            Rule=rule_name,
            Targets=[target]
        )
    
    def add_kinesis_target(self, rule_name, stream_arn, partition_key_path):
        """Add Kinesis Data Streams target for real-time processing"""
        target = {
            'Id': '1',
            'Arn': stream_arn,
            'KinesisParameters': {
                'PartitionKeyPath': partition_key_path
            }
        }
        
        return self.events_client.put_targets(
            Rule=rule_name,
            Targets=[target]
        )
    
    def add_redshift_data_api_target(self, rule_name, lambda_arn):
        """Add Redshift Data API target via Lambda"""
        # Input transformer to prepare SQL execution
        input_transformer = {
            'InputPathsMap': {
                'bucket': '$.detail.bucket.name',
                'key': '$.detail.object.key',
                'size': '$.detail.object.size'
            },
            'InputTemplate': json.dumps({
                'action': 'execute_sql',
                'sql': 'COPY sales_data FROM "s3://<bucket>/<key>" IAM_ROLE "arn:aws:iam::123456789012:role/RedshiftRole" CSV;',
                'database': 'analytics',
                'cluster_identifier': 'data-warehouse'
            })
        }
        
        target = {
            'Id': '1',
            'Arn': lambda_arn,
            'InputTransformer': input_transformer
        }
        
        return self.events_client.put_targets(
            Rule=rule_name,
            Targets=[target]
        )

# Multi-target configuration example
def setup_data_pipeline_targets():
    """Setup complete data pipeline with multiple targets"""
    
    targets_manager = EventBridgeTargets()
    
    # Example: S3 upload triggers multiple actions
    rule_name = 's3-data-processing'
    
    # Target 1: Lambda for immediate processing
    lambda_arn = 'arn:aws:lambda:us-west-2:123456789012:function:process-data'
    dlq_arn = 'arn:aws:sqs:us-west-2:123456789012:lambda-dlq'
    
    targets_manager.add_lambda_target_with_dlq(rule_name, lambda_arn, dlq_arn)
    
    # Target 2: SQS for batch processing
    queue_arn = 'arn:aws:sqs:us-west-2:123456789012:batch-processing'
    targets_manager.add_sqs_target_with_batch(rule_name, queue_arn)
    
    # Target 3: Kinesis for real-time analytics
    stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/realtime-data'
    targets_manager.add_kinesis_target(rule_name, stream_arn, '$.detail.bucket.name')
    
    print("✅ Multi-target data pipeline configured")

# Cross-account event sharing
def setup_cross_account_sharing():
    """Setup cross-account event sharing"""
    
    # Resource policy for cross-account access
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowCrossAccountAccess",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::555666777888:root"
                },
                "Action": "events:PutEvents",
                "Resource": "arn:aws:events:us-west-2:123456789012:event-bus/shared-events"
            }
        ]
    }
    
    events_client = boto3.client('events')
    
    return events_client.put_permission(
        Principal='555666777888',
        Action='events:PutEvents',
        StatementId='CrossAccountEventSharing'
    )

# Error handling and monitoring setup
def setup_monitoring_and_alerting():
    """Setup monitoring and alerting for EventBridge"""
    
    cloudwatch = boto3.client('cloudwatch')
    
    # Create CloudWatch alarm for failed invocations
    cloudwatch.put_metric_alarm(
        AlarmName='EventBridge-Failed-Invocations',
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=2,
        MetricName='FailedInvocations',
        Namespace='AWS/Events',
        Period=300,
        Statistic='Sum',
        Threshold=5.0,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-west-2:123456789012:eventbridge-alerts'
        ],
        AlarmDescription='Alert when EventBridge has failed invocations',
        Dimensions=[
            {
                'Name': 'RuleName',
                'Value': 's3-data-processing'
            }
        ]
    )
    
    print("✅ Monitoring and alerting configured")

# Execute setup
setup_data_pipeline_targets()
setup_cross_account_sharing()
setup_monitoring_and_alerting()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def redshift_data_api_tab():
    """Content for Redshift Data API tab"""
    st.markdown("## 🔌 Redshift Data API")
    st.markdown("*Serverless HTTP endpoint for executing SQL without persistent connections*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### ⚡ Data API Key Benefits
    The Redshift Data API provides a secure HTTP endpoint for SQL execution:
    - **No Connection Management**: Eliminate connection pooling and timeouts
    - **Serverless Integration**: Perfect for Lambda, API Gateway, and mobile apps
    - **Asynchronous Processing**: Non-blocking SQL execution with result polling
    - **Secure Authentication**: Uses IAM roles and AWS Secrets Manager
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data API Flow
    st.markdown("#### 🔄 Redshift Data API Flow")
    common.mermaid(create_redshift_data_api_flow(), height=600)
    
    # Interactive API Explorer
    st.markdown("#### 🎮 Interactive Data API Explorer")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Connection Configuration")
        cluster_endpoint = st.text_input("Cluster Identifier", "my-redshift-cluster")
        database_name = st.text_input("Database Name", "analytics")
        secret_arn = st.text_input("Secret ARN", "arn:aws:secretsmanager:us-west-2:123456789012:secret:redshift-secret")
    
    with col2:
        st.markdown("##### Query Configuration")
        query_type = st.selectbox("Query Type", [
            "SELECT - Data Retrieval", "INSERT - Data Loading", 
            "COPY - Bulk Load", "CREATE TABLE - DDL", "ANALYZE - Statistics"
        ])
        
        async_execution = st.checkbox("Asynchronous Execution", value=True)
        with_event = st.checkbox("Include Event Metadata", value=False)
    
    # Generate sample query
    sample_query = generate_sample_query(query_type)
    
    st.markdown("##### SQL Query")
    query_text = st.text_area("Enter SQL Query", sample_query, height=100)
    
    # Execute button (simulation)
    if st.button("🚀 Execute Query (Simulation)", type="primary"):
        if query_text.strip():
            execution_result = simulate_api_execution(query_text, async_execution)
            
            st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
            st.markdown(f"""
            ### 📊 Execution Result
            **Query ID**: {execution_result['query_id']}  
            **Status**: {execution_result['status']}  
            **Execution Time**: {execution_result['duration']}  
            **Rows Affected**: {execution_result['rows']}  
            **Data API Cost**: ${execution_result['cost']}
            """)
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Update session state
            st.session_state.api_calls_made += 1
        else:
            st.warning("Please enter a SQL query to execute.")
    
    # API Features comparison
    st.markdown("#### ⚖️ Data API vs Traditional Connections")
    
    comparison_data = {
        'Feature': [
            'Connection Management',
            'Authentication', 
            'Scalability',
            'Integration',
            'Error Handling',
            'Cost Model',
            'Performance',
            'Use Cases'
        ],
        'Traditional JDBC/ODBC': [
            'Manual connection pooling',
            'Username/Password',
            'Limited by connections',
            'Application-level',
            'Connection timeouts',
            'Always-on connections',
            'Direct connection speed',
            'Traditional applications'
        ],
        'Redshift Data API': [
            'Serverless - no connections',
            'IAM roles + Secrets Manager',
            'Auto-scaling',
            'AWS services native',
            'Async retry mechanisms',
            'Pay per API call',
            'HTTP overhead',
            'Serverless, mobile, web'
        ]
    }
    
    comparison_df = pd.DataFrame(comparison_data)
    st.dataframe(comparison_df, use_container_width=True)
    
    # Authentication methods
    st.markdown("#### 🔐 Authentication Methods")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="architecture-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔑 AWS Secrets Manager
        **Advantages:**
        - Centralized credential management
        - Automatic rotation support
        - Encryption at rest and in transit
        - Audit trail via CloudTrail
        
        **Best For:**
        - Production applications
        - Shared credentials
        - Compliance requirements
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="architecture-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 👤 Temporary Credentials
        **Advantages:**
        - No stored passwords
        - Automatic expiration
        - Fine-grained IAM permissions
        - Reduced credential exposure
        
        **Best For:**
        - Individual user queries
        - Development/testing
        - Cross-account access
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Usage metrics
    if st.session_state.api_calls_made > 0:
        st.markdown("#### 📈 Session Metrics")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            **API Calls Made**  
            {st.session_state.api_calls_made}
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            estimated_cost = st.session_state.api_calls_made * 0.002
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            **Estimated Cost** 
            ${estimated_cost:.3f}
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            avg_response = 1.2 + (st.session_state.api_calls_made * 0.1)
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            **Avg Response Time**  
            {avg_response:.1f}s
            """)
            st.markdown('</div>', unsafe_allow_html=True)

def data_api_code_tab():
    """Content for Redshift Data API Python Examples tab"""
    st.markdown("## 💻 Redshift Data API Code Examples")
    st.markdown("*Complete Python implementation patterns for the Data API*")
    
    # Code examples with tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "Basic Setup", "Query Execution", "Async Processing", "Error Handling"
    ])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Redshift Data API - Basic Setup and Configuration
import boto3
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

class RedshiftDataAPI:
    """
    A comprehensive wrapper for the Amazon Redshift Data API
    """
    
    def __init__(self, region_name='us-west-2'):
        """Initialize the Redshift Data API client"""
        self.client = boto3.client('redshift-data', region_name=region_name)
        self.region = region_name
        self.active_queries = {}
    
    def execute_sql_with_secrets(self, 
                                cluster_identifier: str,
                                database: str, 
                                secret_arn: str,
                                sql: str,
                                statement_name: Optional[str] = None) -> str:
        """
        Execute SQL using AWS Secrets Manager for authentication
        
        Args:
            cluster_identifier: Redshift cluster identifier
            database: Database name
            secret_arn: ARN of the secret containing credentials
            sql: SQL statement to execute
            statement_name: Optional name for the statement
            
        Returns:
            Query execution ID
        """
        try:
            response = self.client.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                SecretArn=secret_arn,
                Sql=sql,
                StatementName=statement_name or f"query_{int(time.time())}",
                WithEvent=True  # Enable CloudWatch Events
            )
            
            query_id = response['Id']
            self.active_queries[query_id] = {
                'sql': sql,
                'start_time': datetime.now(),
                'status': 'SUBMITTED'
            }
            
            print(f"✅ Query submitted successfully: {query_id}")
            return query_id
            
        except Exception as e:
            print(f"❌ Error executing SQL: {str(e)}")
            raise
    
    def execute_sql_with_user(self,
                             cluster_identifier: str,
                             database: str,
                             db_user: str,
                             sql: str,
                             statement_name: Optional[str] = None) -> str:
        """
        Execute SQL using temporary database credentials
        
        Args:
            cluster_identifier: Redshift cluster identifier  
            database: Database name
            db_user: Database user name
            sql: SQL statement to execute
            statement_name: Optional name for the statement
            
        Returns:
            Query execution ID
        """
        try:
            response = self.client.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                DbUser=db_user,
                Sql=sql,
                StatementName=statement_name or f"user_query_{int(time.time())}",
                WithEvent=True
            )
            
            query_id = response['Id']
            self.active_queries[query_id] = {
                'sql': sql,
                'start_time': datetime.now(),
                'status': 'SUBMITTED'
            }
            
            print(f"✅ Query submitted with user credentials: {query_id}")
            return query_id
            
        except Exception as e:
            print(f"❌ Error executing SQL with user: {str(e)}")
            raise
    
    def batch_execute_sql(self,
                         cluster_identifier: str,
                         database: str,
                         secret_arn: str,
                         sql_statements: List[str],
                         statement_name: Optional[str] = None) -> str:
        """
        Execute multiple SQL statements in a batch
        
        Args:
            cluster_identifier: Redshift cluster identifier
            database: Database name  
            secret_arn: ARN of the secret containing credentials
            sql_statements: List of SQL statements
            statement_name: Optional name for the batch
            
        Returns:
            Batch execution ID
        """
        try:
            # Join statements with semicolons
            combined_sql = ";\n".join(sql_statements) + ";"
            
            response = self.client.batch_execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                SecretArn=secret_arn,
                Sqls=sql_statements,
                StatementName=statement_name or f"batch_{int(time.time())}",
                WithEvent=True
            )
            
            batch_id = response['Id']
            self.active_queries[batch_id] = {
                'sql': combined_sql,
                'start_time': datetime.now(),
                'status': 'SUBMITTED',
                'type': 'BATCH',
                'statement_count': len(sql_statements)
            }
            
            print(f"✅ Batch submitted successfully: {batch_id}")
            print(f"   Statements in batch: {len(sql_statements)}")
            return batch_id
            
        except Exception as e:
            print(f"❌ Error executing batch: {str(e)}")
            raise

# Example usage and configuration
def setup_redshift_data_api():
    """Setup example with different authentication methods"""
    
    # Initialize API client
    rs_api = RedshiftDataAPI(region_name='us-west-2')
    
    # Configuration
    config = {
        'cluster_identifier': 'my-redshift-cluster',
        'database': 'analytics', 
        'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:redshift-secret-AbCdEf',
        'db_user': 'data_analyst'
    }
    
    # Example 1: Simple SELECT query with Secrets Manager
    select_query = """
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(total_amount) as total_spent
    FROM orders 
    WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 100;
    """
    
    query_id_1 = rs_api.execute_sql_with_secrets(
        cluster_identifier=config['cluster_identifier'],
        database=config['database'],
        secret_arn=config['secret_arn'],
        sql=select_query,
        statement_name='top_customers_analysis'
    )
    
    # Example 2: Data loading with COPY command
    copy_query = """
    COPY sales_data 
    FROM 's3://my-data-bucket/sales/2024/sales_data.csv'
    IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftRole'
    CSV
    IGNOREHEADER 1
    TIMEFORMAT 'YYYY-MM-DD HH:MI:SS';
    """
    
    query_id_2 = rs_api.execute_sql_with_secrets(
        cluster_identifier=config['cluster_identifier'],
        database=config['database'],
        secret_arn=config['secret_arn'],
        sql=copy_query,
        statement_name='load_sales_data'
    )
    
    # Example 3: Batch execution for multiple operations
    batch_statements = [
        "CREATE TEMP TABLE temp_analysis AS SELECT * FROM sales WHERE region = 'US';",
        "UPDATE product_catalog SET last_updated = CURRENT_TIMESTAMP;",
        "ANALYZE sales;",
        "ANALYZE product_catalog;"
    ]
    
    batch_id = rs_api.batch_execute_sql(
        cluster_identifier=config['cluster_identifier'],
        database=config['database'],
        secret_arn=config['secret_arn'],
        sql_statements=batch_statements,
        statement_name='maintenance_batch'
    )
    
    return {
        'api_client': rs_api,
        'query_ids': [query_id_1, query_id_2],
        'batch_id': batch_id,
        'config': config
    }

# Required IAM permissions for Data API
REQUIRED_IAM_PERMISSIONS = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "redshift-data:ExecuteStatement",
                "redshift-data:BatchExecuteStatement", 
                "redshift-data:DescribeStatement",
                "redshift-data:GetStatementResult",
                "redshift-data:ListStatements",
                "redshift-data:CancelStatement"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "redshift:GetClusterCredentials",
                "redshift:DescribeClusters"
            ],
            "Resource": [
                "arn:aws:redshift:*:*:cluster:my-redshift-cluster",
                "arn:aws:redshift:*:*:dbuser:my-redshift-cluster/data_analyst"
            ]
        },
        {
            "Effect": "Allow", 
            "Action": [
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            "Resource": "arn:aws:secretsmanager:*:*:secret:redshift-secret-*"
        }
    ]
}

print("🔧 Redshift Data API setup complete!")
print("Required IAM permissions documented above.")

# Execute setup
setup_result = setup_redshift_data_api()
print(f"Active queries: {len(setup_result['api_client'].active_queries)}")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Advanced Query Execution and Result Handling
import boto3
import json
import time
import pandas as pd
from typing import Dict, List, Optional, Any, Union

class AdvancedRedshiftDataAPI:
    """Advanced Redshift Data API operations"""
    
    def __init__(self, region_name='us-west-2'):
        self.client = boto3.client('redshift-data', region_name=region_name)
        self.region = region_name
    
    def get_query_status(self, query_id: str) -> Dict[str, Any]:
        """
        Get the status of a query execution
        
        Args:
            query_id: Query execution ID
            
        Returns:
            Dictionary with query status information
        """
        try:
            response = self.client.describe_statement(Id=query_id)
            
            status_info = {
                'id': response['Id'],
                'status': response['Status'],
                'created_at': response['CreatedAt'],
                'duration': response.get('Duration', 0),
                'has_result_set': response.get('HasResultSet', False),
                'result_rows': response.get('ResultRows', 0),
                'result_size': response.get('ResultSize', 0)
            }
            
            # Include error information if query failed
            if response['Status'] == 'FAILED':
                status_info['error'] = response.get('Error', 'Unknown error')
            
            # Include query details if available
            if 'QueryString' in response:
                status_info['sql'] = response['QueryString']
            
            return status_info
            
        except Exception as e:
            print(f"❌ Error getting query status: {str(e)}")
            raise
    
    def wait_for_query_completion(self, 
                                 query_id: str, 
                                 max_wait_time: int = 300,
                                 poll_interval: int = 5) -> Dict[str, Any]:
        """
        Wait for query completion with timeout
        
        Args:
            query_id: Query execution ID
            max_wait_time: Maximum wait time in seconds
            poll_interval: Polling interval in seconds
            
        Returns:
            Final query status
        """
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            status_info = self.get_query_status(query_id)
            
            if status_info['status'] in ['FINISHED', 'FAILED', 'CANCELLED']:
                print(f"✅ Query {query_id} completed with status: {status_info['status']}")
                return status_info
            
            print(f"⏳ Query {query_id} status: {status_info['status']} (waiting...)")
            time.sleep(poll_interval)
        
        print(f"⏰ Query {query_id} timed out after {max_wait_time} seconds")
        return self.get_query_status(query_id)
    
    def get_query_results(self, 
                         query_id: str, 
                         max_results: int = 1000,
                         next_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve query results with pagination
        
        Args:
            query_id: Query execution ID
            max_results: Maximum number of results to retrieve
            next_token: Pagination token for subsequent calls
            
        Returns:
            Query results and metadata
        """
        try:
            params = {
                'Id': query_id,
                'MaxResults': max_results
            }
            
            if next_token:
                params['NextToken'] = next_token
            
            response = self.client.get_statement_result(**params)
            
            # Extract column metadata
            columns = []
            if 'ColumnMetadata' in response:
                columns = [
                    {
                        'name': col['name'],
                        'type': col['typeName'],
                        'length': col.get('length', 0),
                        'precision': col.get('precision', 0),
                        'scale': col.get('scale', 0)
                    }
                    for col in response['ColumnMetadata']
                ]
            
            # Extract row data
            rows = []
            if 'Records' in response:
                for record in response['Records']:
                    row = []
                    for field in record:
                        # Handle different field types
                        if 'stringValue' in field:
                            row.append(field['stringValue'])
                        elif 'longValue' in field:
                            row.append(field['longValue'])
                        elif 'doubleValue' in field:
                            row.append(field['doubleValue'])
                        elif 'booleanValue' in field:
                            row.append(field['booleanValue'])
                        elif 'isNull' in field:
                            row.append(None)
                        else:
                            row.append(str(field))
                    rows.append(row)
            
            result = {
                'columns': columns,
                'rows': rows,
                'total_rows': len(rows),
                'has_more_results': 'NextToken' in response,
                'next_token': response.get('NextToken')
            }
            
            return result
            
        except Exception as e:
            print(f"❌ Error retrieving query results: {str(e)}")
            raise
    
    def results_to_dataframe(self, query_id: str, max_results: int = 10000) -> pd.DataFrame:
        """
        Convert query results to pandas DataFrame
        
        Args:
            query_id: Query execution ID
            max_results: Maximum number of results to retrieve
            
        Returns:
            pandas DataFrame with query results
        """
        try:
            # Get all results with pagination
            all_rows = []
            next_token = None
            
            while True:
                results = self.get_query_results(
                    query_id=query_id,
                    max_results=min(1000, max_results - len(all_rows)),
                    next_token=next_token
                )
                
                all_rows.extend(results['rows'])
                
                if not results['has_more_results'] or len(all_rows) >= max_results:
                    break
                
                next_token = results['next_token']
            
            # Create DataFrame
            if all_rows and results['columns']:
                column_names = [col['name'] for col in results['columns']]
                df = pd.DataFrame(all_rows, columns=column_names)
                
                # Attempt type conversion based on metadata
                for i, col_info in enumerate(results['columns']):
                    col_name = col_info['name']
                    col_type = col_info['type'].lower()
                    
                    try:
                        if 'int' in col_type or 'bigint' in col_type:
                            df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                        elif 'decimal' in col_type or 'numeric' in col_type:
                            df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                        elif 'timestamp' in col_type or 'date' in col_type:
                            df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
                        elif 'bool' in col_type:
                            df[col_name] = df[col_name].astype(bool)
                    except Exception as e:
                        print(f"⚠️ Warning: Could not convert column {col_name}: {e}")
                
                print(f"✅ DataFrame created with {len(df)} rows and {len(df.columns)} columns")
                return df
            else:
                print("ℹ️ No data returned from query")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Error converting to DataFrame: {str(e)}")
            raise
    
    def execute_and_get_results(self,
                               cluster_identifier: str,
                               database: str,
                               secret_arn: str,
                               sql: str,
                               timeout: int = 300) -> pd.DataFrame:
        """
        Execute query and return results as DataFrame (synchronous)
        
        Args:
            cluster_identifier: Redshift cluster identifier
            database: Database name
            secret_arn: Secret ARN for credentials
            sql: SQL statement to execute
            timeout: Maximum wait time in seconds
            
        Returns:
            pandas DataFrame with results
        """
        try:
            # Execute query
            response = self.client.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                SecretArn=secret_arn,
                Sql=sql,
                WithEvent=True
            )
            
            query_id = response['Id']
            print(f"🚀 Executing query: {query_id}")
            
            # Wait for completion
            status = self.wait_for_query_completion(query_id, timeout)
            
            if status['status'] == 'FINISHED':
                # Return results as DataFrame
                return self.results_to_dataframe(query_id)
            elif status['status'] == 'FAILED':
                raise Exception(f"Query failed: {status.get('error', 'Unknown error')}")
            else:
                raise Exception(f"Query did not complete: {status['status']}")
                
        except Exception as e:
            print(f"❌ Error in execute_and_get_results: {str(e)}")
            raise

# Example usage patterns
def example_query_patterns():
    """Demonstrate different query execution patterns"""
    
    api = AdvancedRedshiftDataAPI()
    
    config = {
        'cluster_identifier': 'analytics-cluster',
        'database': 'dwh',
        'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:redshift-creds'
    }
    
    # Pattern 1: Simple synchronous query
    print("Pattern 1: Synchronous query execution")
    sales_df = api.execute_and_get_results(
        sql="SELECT * FROM sales WHERE sale_date >= '2024-01-01' LIMIT 100",
        **config
    )
    print(f"Retrieved {len(sales_df)} sales records")
    
    # Pattern 2: Asynchronous query with manual polling
    print("\nPattern 2: Asynchronous query execution")
    query_id = api.client.execute_statement(
        ClusterIdentifier=config['cluster_identifier'],
        Database=config['database'],
        SecretArn=config['secret_arn'],
        Sql="SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id"
    )['Id']
    
    # Manual status checking
    status = api.wait_for_query_completion(query_id, max_wait_time=60)
    
    if status['status'] == 'FINISHED':
        results_df = api.results_to_dataframe(query_id)
        print(f"Async query completed: {len(results_df)} customer totals")
    
    # Pattern 3: Large result set with pagination
    print("\nPattern 3: Large result set handling")
    large_query_df = api.execute_and_get_results(
        sql="SELECT * FROM large_table ORDER BY created_date DESC",
        timeout=600,  # 10 minute timeout
        **config
    )
    print(f"Large query result: {len(large_query_df)} rows")

# Execute examples
print("🔍 Demonstrating query execution patterns...")
# Uncomment to run examples:
# example_query_patterns()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Asynchronous Processing and Event-Driven Patterns
import boto3
import json
import asyncio
import concurrent.futures
from typing import Dict, List, Callable, Any
from datetime import datetime

class AsyncRedshiftDataAPI:
    """Asynchronous patterns for Redshift Data API"""
    
    def __init__(self, region_name='us-west-2'):
        self.client = boto3.client('redshift-data', region_name=region_name)
        self.eventbridge = boto3.client('events', region_name=region_name)
        self.region = region_name
        self.active_queries = {}
    
    async def execute_query_async(self, 
                                 cluster_identifier: str,
                                 database: str,
                                 secret_arn: str,
                                 sql: str,
                                 callback: Callable = None) -> str:
        """
        Execute query asynchronously with optional callback
        
        Args:
            cluster_identifier: Redshift cluster identifier
            database: Database name
            secret_arn: Secret ARN for authentication
            sql: SQL statement to execute
            callback: Optional callback function for completion
            
        Returns:
            Query execution ID
        """
        try:
            response = self.client.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                SecretArn=secret_arn,
                Sql=sql,
                WithEvent=True  # Enable EventBridge notifications
            )
            
            query_id = response['Id']
            self.active_queries[query_id] = {
                'sql': sql,
                'start_time': datetime.now(),
                'callback': callback,
                'status': 'SUBMITTED'
            }
            
            print(f"🚀 Async query submitted: {query_id}")
            
            # If callback provided, set up monitoring
            if callback:
                asyncio.create_task(self._monitor_query(query_id))
            
            return query_id
            
        except Exception as e:
            print(f"❌ Error executing async query: {str(e)}")
            raise
    
    async def _monitor_query(self, query_id: str, poll_interval: int = 10):
        """Internal method to monitor query progress"""
        while query_id in self.active_queries:
            try:
                response = self.client.describe_statement(Id=query_id)
                status = response['Status']
                
                self.active_queries[query_id]['status'] = status
                
                if status in ['FINISHED', 'FAILED', 'CANCELLED']:
                    query_info = self.active_queries.pop(query_id)
                    
                    if query_info['callback']:
                        await query_info['callback'](query_id, status, response)
                    
                    break
                
                await asyncio.sleep(poll_interval)
                
            except Exception as e:
                print(f"⚠️ Error monitoring query {query_id}: {e}")
                break
    
    def execute_parallel_queries(self,
                               queries: List[Dict[str, Any]],
                               max_workers: int = 5) -> List[str]:
        """
        Execute multiple queries in parallel
        
        Args:
            queries: List of query configurations
            max_workers: Maximum number of concurrent queries
            
        Returns:
            List of query execution IDs
        """
        def execute_single_query(query_config):
            return self.client.execute_statement(**query_config)['Id']
        
        query_ids = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all queries
            future_to_query = {
                executor.submit(execute_single_query, query): i 
                for i, query in enumerate(queries)
            }
            
            # Collect results
            for future in concurrent.futures.as_completed(future_to_query):
                query_index = future_to_query[future]
                try:
                    query_id = future.result()
                    query_ids.append(query_id)
                    print(f"✅ Query {query_index + 1} submitted: {query_id}")
                except Exception as e:
                    print(f"❌ Query {query_index + 1} failed: {e}")
                    query_ids.append(None)
        
        return query_ids
    
    def setup_eventbridge_integration(self, rule_name: str = 'redshift-data-api-events'):
        """
        Setup EventBridge integration for query completion events
        
        Args:
            rule_name: Name for the EventBridge rule
        """
        try:
            # Create EventBridge rule for Redshift Data API events
            event_pattern = {
                "source": ["aws.redshift-data"],
                "detail-type": ["Redshift Data Statement Status Change"],
                "detail": {
                    "state": ["FINISHED", "FAILED", "CANCELLED"]
                }
            }
            
            response = self.eventbridge.put_rule(
                Name=rule_name,
                EventPattern=json.dumps(event_pattern),
                State='ENABLED',
                Description='Monitor Redshift Data API query completion'
            )
            
            print(f"✅ EventBridge rule created: {rule_name}")
            return response['RuleArn']
            
        except Exception as e:
            print(f"❌ Error setting up EventBridge: {e}")
            raise
    
    def create_completion_handler(self, lambda_arn: str, rule_name: str):
        """
        Create Lambda function target for query completion events
        
        Args:
            lambda_arn: ARN of Lambda function to handle events
            rule_name: EventBridge rule name
        """
        try:
            # Add Lambda target to EventBridge rule
            response = self.eventbridge.put_targets(
                Rule=rule_name,
                Targets=[
                    {
                        'Id': '1',
                        'Arn': lambda_arn,
                        'InputTransformer': {
                            'InputPathsMap': {
                                'query_id': '$.detail.statementId',
                                'status': '$.detail.state',
                                'duration': '$.detail.duration',
                                'error': '$.detail.error'
                            },
                            'InputTemplate': json.dumps({
                                'query_id': '<query_id>',
                                'status': '<status>',
                                'duration': '<duration>',
                                'error': '<error>',
                                'timestamp': '$.time'
                            })
                        }
                    }
                ]
            )
            
            print(f"✅ Lambda target added to rule: {rule_name}")
            return response
            
        except Exception as e:
            print(f"❌ Error adding Lambda target: {e}")
            raise

# Lambda function for handling query completion
QUERY_COMPLETION_LAMBDA = """
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Handle Redshift Data API query completion events
    """
    try:
        # Extract event details
        query_id = event['query_id']
        status = event['status']
        duration = event.get('duration', 0)
        error_msg = event.get('error', '')
        
        logger.info(f"Query {query_id} completed with status: {status}")
        
        if status == 'FINISHED':
            # Handle successful completion
            handle_query_success(query_id, duration)
        elif status == 'FAILED':
            # Handle query failure
            handle_query_error(query_id, error_msg)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Processed query {query_id}')
        }
        
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def handle_query_success(query_id, duration):
    """Handle successful query completion"""
    
    # Initialize clients
    redshift_data = boto3.client('redshift-data')
    sns = boto3.client('sns')
    
    try:
        # Get query results
        results = redshift_data.get_statement_result(Id=query_id)
        row_count = len(results.get('Records', []))
        
        # Send success notification
        message = {
            'query_id': query_id,
            'status': 'SUCCESS',
            'duration_ms': duration,
            'rows_returned': row_count,
            'timestamp': datetime.now().isoformat()
        }
        
        sns.publish(
            TopicArn='arn:aws:sns:us-west-2:123456789012:query-notifications',
            Message=json.dumps(message),
            Subject=f'Query {query_id} Completed Successfully'
        )
        
        logger.info(f"Success notification sent for query {query_id}")
        
    except Exception as e:
        logger.error(f"Error handling success for {query_id}: {e}")

def handle_query_error(query_id, error_msg):
    """Handle query failure"""
    
    sns = boto3.client('sns')
    
    try:
        # Send error notification
        message = {
            'query_id': query_id,
            'status': 'FAILED',
            'error_message': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        
        sns.publish(
            TopicArn='arn:aws:sns:us-west-2:123456789012:query-notifications',
            Message=json.dumps(message),
            Subject=f'Query {query_id} Failed'
        )
        
        logger.error(f"Query {query_id} failed: {error_msg}")
        
    except Exception as e:
        logger.error(f"Error handling failure for {query_id}: {e}")
"""

# Example async usage patterns
async def example_async_patterns():
    """Demonstrate asynchronous usage patterns"""
    
    api = AsyncRedshiftDataAPI()
    
    config = {
        'cluster_identifier': 'analytics-cluster',
        'database': 'warehouse',
        'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:redshift-creds'
    }
    
    # Pattern 1: Single async query with callback
    async def query_callback(query_id, status, response):
        print(f"🎯 Query {query_id} completed with status: {status}")
        if status == 'FINISHED':
            print(f"   Rows returned: {response.get('ResultRows', 0)}")
    
    query_id = await api.execute_query_async(
        sql="SELECT COUNT(*) FROM sales WHERE date >= '2024-01-01'",
        callback=query_callback,
        **config
    )
    
    # Pattern 2: Parallel query execution
    parallel_queries = [
        {
            'ClusterIdentifier': config['cluster_identifier'],
            'Database': config['database'],
            'SecretArn': config['secret_arn'],
            'Sql': f"SELECT * FROM sales WHERE region = '{region}'"
        }
        for region in ['US', 'EU', 'APAC', 'LATAM']
    ]
    
    query_ids = api.execute_parallel_queries(parallel_queries, max_workers=4)
    print(f"✅ Submitted {len([qid for qid in query_ids if qid])} parallel queries")
    
    # Pattern 3: Event-driven processing
    rule_arn = api.setup_eventbridge_integration()
    print(f"✅ EventBridge integration configured")

# Execute async example
print("🔄 Setting up asynchronous processing examples...")
# Uncomment to run async examples:
# asyncio.run(example_async_patterns())
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab4:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Error Handling and Recovery Patterns
import boto3
import time
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import backoff

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RobustRedshiftDataAPI:
    """Production-ready Redshift Data API with comprehensive error handling"""
    
    def __init__(self, region_name='us-west-2'):
        self.client = boto3.client('redshift-data', region_name=region_name)
        self.cloudwatch = boto3.client('cloudwatch', region_name=region_name)
        self.region = region_name
        self.retry_config = {
            'max_retries': 3,
            'backoff_factor': 2,
            'max_backoff': 60
        }
    
    @backoff.on_exception(
        backoff.expo,
        (boto3.client('redshift-data').exceptions.InternalServerException,
         Exception),
        max_tries=3,
        max_time=300
    )
    def execute_statement_with_retry(self,
                                   cluster_identifier: str,
                                   database: str,
                                   secret_arn: str,
                                   sql: str,
                                   statement_name: str = None) -> str:
        """
        Execute statement with automatic retry logic
        
        Args:
            cluster_identifier: Redshift cluster identifier
            database: Database name
            secret_arn: Secret ARN for authentication
            sql: SQL statement to execute
            statement_name: Optional statement name
            
        Returns:
            Query execution ID
            
        Raises:
            Exception: If all retry attempts fail
        """
        try:
            response = self.client.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                SecretArn=secret_arn,
                Sql=sql,
                StatementName=statement_name or f"stmt_{int(time.time())}",
                WithEvent=True
            )
            
            query_id = response['Id']
            logger.info(f"✅ Query executed successfully: {query_id}")
            
            # Log success metric
            self._log_metric('QuerySubmitted', 1, 'Count')
            
            return query_id
            
        except self.client.exceptions.ValidationException as e:
            logger.error(f"❌ Validation error: {str(e)}")
            self._log_metric('ValidationError', 1, 'Count')
            raise
            
        except self.client.exceptions.ActiveStatementsExceededException as e:
            logger.warning(f"⚠️ Too many active statements: {str(e)}")
            self._log_metric('ActiveStatementsExceeded', 1, 'Count')
            # Wait and retry
            time.sleep(30)
            raise
            
        except self.client.exceptions.InternalServerException as e:
            logger.error(f"🔄 Internal server error, retrying: {str(e)}")
            self._log_metric('InternalServerError', 1, 'Count')
            raise
            
        except Exception as e:
            logger.error(f"❌ Unexpected error: {str(e)}")
            self._log_metric('UnexpectedError', 1, 'Count')
            raise
    
    def execute_with_timeout_and_cancel(self,
                                      cluster_identifier: str,
                                      database: str,
                                      secret_arn: str,
                                      sql: str,
                                      timeout_seconds: int = 300) -> Dict[str, Any]:
        """
        Execute query with timeout and automatic cancellation
        
        Args:
            cluster_identifier: Redshift cluster identifier
            database: Database name
            secret_arn: Secret ARN for authentication  
            sql: SQL statement to execute
            timeout_seconds: Timeout in seconds
            
        Returns:
            Query result or error information
        """
        try:
            # Submit query
            query_id = self.execute_statement_with_retry(
                cluster_identifier, database, secret_arn, sql
            )
            
            start_time = time.time()
            
            # Monitor with timeout
            while time.time() - start_time < timeout_seconds:
                status_response = self.client.describe_statement(Id=query_id)
                status = status_response['Status']
                
                if status == 'FINISHED':
                    logger.info(f"✅ Query {query_id} completed successfully")
                    return {
                        'success': True,
                        'query_id': query_id,
                        'status': status,
                        'duration': status_response.get('Duration', 0),
                        'result_rows': status_response.get('ResultRows', 0)
                    }
                    
                elif status == 'FAILED':
                    error_msg = status_response.get('Error', 'Unknown error')
                    logger.error(f"❌ Query {query_id} failed: {error_msg}")
                    self._log_metric('QueryFailed', 1, 'Count')
                    return {
                        'success': False,
                        'query_id': query_id,
                        'status': status,
                        'error': error_msg
                    }
                    
                elif status == 'CANCELLED':
                    logger.warning(f"🚫 Query {query_id} was cancelled")
                    return {
                        'success': False,
                        'query_id': query_id,
                        'status': status,
                        'error': 'Query was cancelled'
                    }
                
                time.sleep(5)  # Poll every 5 seconds
            
            # Timeout reached - cancel the query
            logger.warning(f"⏰ Query {query_id} timed out, cancelling...")
            self.client.cancel_statement(Id=query_id)
            self._log_metric('QueryTimeout', 1, 'Count')
            
            return {
                'success': False,
                'query_id': query_id,
                'status': 'TIMEOUT',
                'error': f'Query timed out after {timeout_seconds} seconds'
            }
            
        except Exception as e:
            logger.error(f"❌ Error in execute_with_timeout_and_cancel: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def batch_execute_with_error_handling(self,
                                        cluster_identifier: str,
                                        database: str,
                                        secret_arn: str,
                                        sql_statements: List[str],
                                        stop_on_error: bool = False) -> Dict[str, Any]:
        """
        Execute batch of statements with comprehensive error handling
        
        Args:
            cluster_identifier: Redshift cluster identifier
            database: Database name
            secret_arn: Secret ARN for authentication
            sql_statements: List of SQL statements
            stop_on_error: Whether to stop batch on first error
            
        Returns:
            Batch execution results
        """
        results = {
            'batch_id': None,
            'submitted_statements': len(sql_statements),
            'successful_statements': 0,
            'failed_statements': 0,
            'errors': []
        }
        
        try:
            # Submit batch
            response = self.client.batch_execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                SecretArn=secret_arn,
                Sqls=sql_statements,
                StatementName=f"batch_{int(time.time())}",
                WithEvent=True
            )
            
            batch_id = response['Id']
            results['batch_id'] = batch_id
            logger.info(f"🚀 Batch submitted: {batch_id}")
            
            # Monitor batch execution
            while True:
                batch_status = self.client.describe_statement(Id=batch_id)
                status = batch_status['Status']
                
                if status in ['FINISHED', 'FAILED', 'CANCELLED']:
                    break
                
                time.sleep(10)  # Check every 10 seconds
            
            # Get detailed results for each statement
            if 'SubStatements' in batch_status:
                for i, sub_stmt in enumerate(batch_status['SubStatements']):
                    stmt_status = sub_stmt['Status']
                    
                    if stmt_status == 'FINISHED':
                        results['successful_statements'] += 1
                        logger.info(f"✅ Statement {i+1} completed successfully")
                    else:
                        results['failed_statements'] += 1
                        error_info = {
                            'statement_index': i + 1,
                            'sql': sql_statements[i][:100] + '...',
                            'status': stmt_status,
                            'error': sub_stmt.get('Error', 'Unknown error')
                        }
                        results['errors'].append(error_info)
                        logger.error(f"❌ Statement {i+1} failed: {error_info['error']}")
                        
                        if stop_on_error:
                            logger.warning("🛑 Stopping batch execution due to error")
                            break
            
            # Log metrics
            self._log_metric('BatchStatementsSuccessful', results['successful_statements'], 'Count')
            self._log_metric('BatchStatementsFailed', results['failed_statements'], 'Count')
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Batch execution error: {str(e)}")
            results['errors'].append({
                'statement_index': 0,
                'sql': 'BATCH_EXECUTION',
                'status': 'SYSTEM_ERROR',
                'error': str(e)
            })
            return results
    
    def _log_metric(self, metric_name: str, value: float, unit: str = 'Count'):
        """Log custom metrics to CloudWatch"""
        try:
            self.cloudwatch.put_metric_data(
                Namespace='RedshiftDataAPI',
                MetricData=[
                    {
                        'MetricName': metric_name,
                        'Value': value,
                        'Unit': unit,
                        'Timestamp': datetime.utcnow()
                    }
                ]
            )
        except Exception as e:
            logger.warning(f"⚠️ Failed to log metric {metric_name}: {e}")
    
    def get_query_history(self, 
                         start_time: datetime,
                         end_time: datetime = None,
                         status_filter: List[str] = None) -> List[Dict[str, Any]]:
        """
        Get query execution history with filtering
        
        Args:
            start_time: Start time for query history
            end_time: End time for query history (default: now)
            status_filter: List of statuses to filter by
            
        Returns:
            List of query execution records
        """
        try:
            if end_time is None:
                end_time = datetime.utcnow()
            
            response = self.client.list_statements(
                NextToken='',
                RoleLevel=False,
                StatementName='',
                Status=status_filter[0] if status_filter else 'ALL'
            )
            
            statements = response.get('Statements', [])
            
            # Filter by time range
            filtered_statements = []
            for stmt in statements:
                created_at = stmt['CreatedAt']
                if start_time <= created_at <= end_time:
                    filtered_statements.append({
                        'id': stmt['Id'],
                        'status': stmt['Status'],
                        'created_at': created_at,
                        'query_string': stmt.get('QueryString', '')[:200] + '...'
                    })
            
            return filtered_statements
            
        except Exception as e:
            logger.error(f"❌ Error getting query history: {str(e)}")
            return []

# Error handling examples
def example_error_handling():
    """Demonstrate error handling patterns"""
    
    api = RobustRedshiftDataAPI()
    
    config = {
        'cluster_identifier': 'prod-analytics',
        'database': 'warehouse',
        'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:redshift-prod'
    }
    
    # Example 1: Query with timeout and cancellation
    result = api.execute_with_timeout_and_cancel(
        sql="SELECT * FROM very_large_table ORDER BY date_column",
        timeout_seconds=120,  # 2 minute timeout
        **config
    )
    
    if result['success']:
        print(f"✅ Query completed: {result['query_id']}")
    else:
        print(f"❌ Query failed: {result['error']}")
    
    # Example 2: Batch execution with error handling
    maintenance_queries = [
        "ANALYZE sales;",
        "VACUUM customers;",
        "UPDATE product_catalog SET last_updated = CURRENT_TIMESTAMP;",
        "REFRESH MATERIALIZED VIEW sales_summary;"
    ]
    
    batch_result = api.batch_execute_with_error_handling(
        sql_statements=maintenance_queries,
        stop_on_error=False,  # Continue even if one statement fails
        **config
    )
    
    print(f"Batch execution: {batch_result['successful_statements']}/{batch_result['submitted_statements']} successful")
    
    # Example 3: Query history analysis
    history = api.get_query_history(
        start_time=datetime.utcnow() - timedelta(hours=24),
        status_filter=['FAILED']
    )
    
    print(f"Failed queries in last 24 hours: {len(history)}")

# Execute error handling examples
print("🛡️ Demonstrating error handling patterns...")
# Uncomment to run examples:
# example_error_handling()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def event_driven_architecture_tab():
    """Content for Event-Driven Architecture tab"""
    st.markdown("## 🏗️ Event-Driven Architecture with Redshift Data API")
    st.markdown("*Complete solution patterns combining EventBridge, Lambda, and Redshift Data API*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Event-Driven Data Pipeline Architecture
    This pattern combines multiple AWS services to create a reactive data processing system:
    - **EventBridge**: Receives and routes events from various sources
    - **Lambda**: Processes events and orchestrates data operations
    - **Redshift Data API**: Executes SQL without managing connections
    - **CloudWatch**: Monitors pipeline health and performance
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Complete architecture diagram
    st.markdown("#### 🏗️ Complete Event-Driven Pipeline")
    common.mermaid(create_event_driven_pipeline(), height=1300)
    
    # Interactive architecture builder
    st.markdown("#### 🎨 Architecture Pattern Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Data Sources")
        data_sources = st.multiselect("Select Data Sources", [
            "Amazon S3 (Data Lake)",
            "Amazon Kinesis (Streaming)",
            "Amazon RDS (Database Changes)",
            "Custom Applications",
            "SaaS Applications"
        ], default=["Amazon S3 (Data Lake)", "Custom Applications"])
        
        trigger_frequency = st.selectbox("Event Frequency", [
            "Real-time (< 1 second)",
            "Near real-time (< 5 seconds)", 
            "Batch (hourly)",
            "Scheduled (daily)"
        ])
    
    with col2:
        st.markdown("##### Processing Requirements")
        processing_types = st.multiselect("Processing Types", [
            "Data Validation",
            "Data Transformation",
            "Data Loading (ETL)",
            "Analytics Queries",
            "Alerting & Notifications"
        ], default=["Data Validation", "Data Loading (ETL)"])
        
        scale_requirement = st.selectbox("Scale Requirement", [
            "Low (< 1000 events/hour)",
            "Medium (1K - 10K events/hour)",
            "High (10K - 100K events/hour)",
            "Very High (> 100K events/hour)"
        ])
    
    # Generate architecture recommendation
    architecture_rec = generate_architecture_recommendation(
        data_sources, trigger_frequency, processing_types, scale_requirement
    )
    
    st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
    st.markdown(f"""
    ### 🎯 Recommended Architecture
    **Pattern**: {architecture_rec['pattern']}  
    **Lambda Configuration**: {architecture_rec['lambda_config']}  
    **Expected Throughput**: {architecture_rec['throughput']}  
    **Estimated Cost**: {architecture_rec['cost']}/month  
    **Complexity**: {architecture_rec['complexity']}
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture patterns
    st.markdown("#### 📋 Common Architecture Patterns")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="architecture-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗂️ File Processing Pattern
        **Trigger**: S3 Object Created  
        **Process**: Validate → Transform → Load  
        **Target**: Data Warehouse  
        
        **Best For:**
        - Batch file uploads
        - Data lake ingestion
        - ETL pipelines
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="architecture-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Real-time Analytics
        **Trigger**: Custom Events  
        **Process**: Aggregate → Analyze → Alert  
        **Target**: Dashboards/Reports  
        
        **Best For:**
        - Business metrics
        - Operational monitoring
        - Fraud detection
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="architecture-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Data Synchronization
        **Trigger**: Database Changes  
        **Process**: Capture → Transform → Sync  
        **Target**: Data Warehouse  
        
        **Best For:**
        - CDC implementations
        - Data replication
        - Backup processes
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Complete implementation example
    st.markdown("#### 💻 Complete Implementation Example")
    
    tab1, tab2, tab3 = st.tabs([
        "Lambda Function", "Infrastructure as Code", "Monitoring & Alerting"
    ])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Complete Lambda Function for Event-Driven Data Processing
import json
import boto3
import logging
import os
from datetime import datetime
from typing import Dict, Any, List

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
redshift_data = boto3.client('redshift-data')
s3 = boto3.client('s3')
sns = boto3.client('sns')
cloudwatch = boto3.client('cloudwatch')

# Configuration from environment variables
CLUSTER_IDENTIFIER = os.environ['REDSHIFT_CLUSTER_IDENTIFIER']
DATABASE_NAME = os.environ['REDSHIFT_DATABASE']
SECRET_ARN = os.environ['REDSHIFT_SECRET_ARN']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

class EventDrivenDataProcessor:
    """Main class for processing data events"""
    
    def __init__(self):
        self.redshift_data = redshift_data
        self.s3 = s3
        self.sns = sns
        self.cloudwatch = cloudwatch
    
    def process_s3_event(self, event_detail: Dict[str, Any]) -> Dict[str, Any]:
        """Process S3 object creation events"""
        try:
            bucket_name = event_detail['bucket']['name']
            object_key = event_detail['object']['key']
            object_size = event_detail['object']['size']
            
            logger.info(f"Processing S3 object: s3://{bucket_name}/{object_key}")
            
            # Determine processing strategy based on file type and size
            if object_key.endswith('.csv'):
                return self._process_csv_file(bucket_name, object_key, object_size)
            elif object_key.endswith('.json'):
                return self._process_json_file(bucket_name, object_key, object_size)
            elif object_key.endswith('.parquet'):
                return self._process_parquet_file(bucket_name, object_key, object_size)
            else:
                logger.warning(f"Unsupported file type: {object_key}")
                return {'status': 'SKIPPED', 'reason': 'Unsupported file type'}
                
        except Exception as e:
            logger.error(f"Error processing S3 event: {str(e)}")
            return {'status': 'ERROR', 'error': str(e)}
    
    def _process_csv_file(self, bucket: str, key: str, size: int) -> Dict[str, Any]:
        """Process CSV files using COPY command"""
        try:
            # Generate table name from file path
            table_name = self._generate_table_name(key)
            
            # Construct COPY command
            copy_sql = f"""
            COPY {table_name}
            FROM 's3://{bucket}/{key}'
            IAM_ROLE '{os.environ.get("REDSHIFT_IAM_ROLE")}'
            CSV
            IGNOREHEADER 1
            TIMEFORMAT 'YYYY-MM-DD HH:MI:SS'
            DATEFORMAT 'YYYY-MM-DD'
            COMPUPDATE ON
            STATUPDATE ON;
            """
            
            # Execute COPY command
            response = self.redshift_data.execute_statement(
                ClusterIdentifier=CLUSTER_IDENTIFIER,
                Database=DATABASE_NAME,
                SecretArn=SECRET_ARN,
                Sql=copy_sql,
                StatementName=f'copy_csv_{int(datetime.now().timestamp())}',
                WithEvent=True
            )
            
            query_id = response['Id']
            
            # Log metrics
            self._log_custom_metric('FileProcessed', 1, 'Count', {'FileType': 'CSV'})
            self._log_custom_metric('FileSizeProcessed', size, 'Bytes', {'FileType': 'CSV'})
            
            return {
                'status': 'SUBMITTED',
                'query_id': query_id,
                'table_name': table_name,
                'file_size': size
            }
            
        except Exception as e:
            logger.error(f"Error processing CSV file: {str(e)}")
            raise
    
    def _process_json_file(self, bucket: str, key: str, size: int) -> Dict[str, Any]:
        """Process JSON files with transformation"""
        try:
            table_name = self._generate_table_name(key)
            
            # For JSON files, we might need to flatten the structure
            copy_sql = f"""
            COPY {table_name}
            FROM 's3://{bucket}/{key}'
            IAM_ROLE '{os.environ.get("REDSHIFT_IAM_ROLE")}'
            JSON 'auto'
            COMPUPDATE ON
            STATUPDATE ON;
            """
            
            response = self.redshift_data.execute_statement(
                ClusterIdentifier=CLUSTER_IDENTIFIER,
                Database=DATABASE_NAME,
                SecretArn=SECRET_ARN,
                Sql=copy_sql,
                StatementName=f'copy_json_{int(datetime.now().timestamp())}',
                WithEvent=True
            )
            
            query_id = response['Id']
            
            # Log metrics
            self._log_custom_metric('FileProcessed', 1, 'Count', {'FileType': 'JSON'})
            self._log_custom_metric('FileSizeProcessed', size, 'Bytes', {'FileType': 'JSON'})
            
            return {
                'status': 'SUBMITTED',
                'query_id': query_id,
                'table_name': table_name,
                'file_size': size
            }
            
        except Exception as e:
            logger.error(f"Error processing JSON file: {str(e)}")
            raise
    
    def _process_parquet_file(self, bucket: str, key: str, size: int) -> Dict[str, Any]:
        """Process Parquet files (optimized format)"""
        try:
            table_name = self._generate_table_name(key)
            
            copy_sql = f"""
            COPY {table_name}
            FROM 's3://{bucket}/{key}'
            IAM_ROLE '{os.environ.get("REDSHIFT_IAM_ROLE")}'
            PARQUET
            COMPUPDATE ON
            STATUPDATE ON;
            """
            
            response = self.redshift_data.execute_statement(
                ClusterIdentifier=CLUSTER_IDENTIFIER,
                Database=DATABASE_NAME,
                SecretArn=SECRET_ARN,
                Sql=copy_sql,
                StatementName=f'copy_parquet_{int(datetime.now().timestamp())}',
                WithEvent=True
            )
            
            query_id = response['Id']
            
            # Log metrics
            self._log_custom_metric('FileProcessed', 1, 'Count', {'FileType': 'Parquet'})
            self._log_custom_metric('FileSizeProcessed', size, 'Bytes', {'FileType': 'Parquet'})
            
            return {
                'status': 'SUBMITTED',
                'query_id': query_id,
                'table_name': table_name,
                'file_size': size
            }
            
        except Exception as e:
            logger.error(f"Error processing Parquet file: {str(e)}")
            raise
    
    def process_custom_event(self, event_detail: Dict[str, Any]) -> Dict[str, Any]:
        """Process custom application events"""
        try:
            event_type = event_detail.get('event_type')
            
            if event_type == 'data_quality_check':
                return self._run_data_quality_check(event_detail)
            elif event_type == 'analytics_query':
                return self._run_analytics_query(event_detail)
            elif event_type == 'maintenance_task':
                return self._run_maintenance_task(event_detail)
            else:
                logger.warning(f"Unknown event type: {event_type}")
                return {'status': 'SKIPPED', 'reason': f'Unknown event type: {event_type}'}
                
        except Exception as e:
            logger.error(f"Error processing custom event: {str(e)}")
            return {'status': 'ERROR', 'error': str(e)}
    
    def _run_data_quality_check(self, event_detail: Dict[str, Any]) -> Dict[str, Any]:
        """Run data quality validation queries"""
        table_name = event_detail.get('table_name')
        
        quality_checks = [
            f"SELECT COUNT(*) as row_count FROM {table_name};",
            f"SELECT COUNT(*) as null_count FROM {table_name} WHERE primary_key IS NULL;",
            f"SELECT COUNT(DISTINCT primary_key) as unique_count FROM {table_name};",
            f"SELECT MAX(updated_date) as latest_update FROM {table_name};"
        ]
        
        combined_sql = "; ".join(quality_checks)
        
        response = self.redshift_data.execute_statement(
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            Database=DATABASE_NAME,
            SecretArn=SECRET_ARN,
            Sql=combined_sql,
            StatementName=f'quality_check_{table_name}_{int(datetime.now().timestamp())}',
            WithEvent=True
        )
        
        return {
            'status': 'SUBMITTED',
            'query_id': response['Id'],
            'check_type': 'data_quality',
            'table_name': table_name
        }
    
    def _run_analytics_query(self, event_detail: Dict[str, Any]) -> Dict[str, Any]:
        """Execute analytics queries based on events"""
        query_type = event_detail.get('query_type')
        
        if query_type == 'daily_summary':
            sql = """
            INSERT INTO daily_summary
            SELECT 
                CURRENT_DATE as summary_date,
                COUNT(*) as total_records,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM transactions
            WHERE DATE(created_date) = CURRENT_DATE;
            """
        elif query_type == 'customer_metrics':
            sql = """
            REFRESH MATERIALIZED VIEW customer_metrics_mv;
            """
        else:
            return {'status': 'SKIPPED', 'reason': f'Unknown query type: {query_type}'}
        
        response = self.redshift_data.execute_statement(
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            Database=DATABASE_NAME,
            SecretArn=SECRET_ARN,
            Sql=sql,
            StatementName=f'analytics_{query_type}_{int(datetime.now().timestamp())}',
            WithEvent=True
        )
        
        return {
            'status': 'SUBMITTED',
            'query_id': response['Id'],
            'query_type': query_type
        }
    
    def _run_maintenance_task(self, event_detail: Dict[str, Any]) -> Dict[str, Any]:
        """Run database maintenance tasks"""
        task_type = event_detail.get('task_type')
        table_name = event_detail.get('table_name', 'ALL')
        
        if task_type == 'vacuum':
            sql = f"VACUUM {table_name};" if table_name != 'ALL' else "VACUUM;"
        elif task_type == 'analyze':
            sql = f"ANALYZE {table_name};" if table_name != 'ALL' else "ANALYZE;"
        else:
            return {'status': 'SKIPPED', 'reason': f'Unknown task type: {task_type}'}
        
        response = self.redshift_data.execute_statement(
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            Database=DATABASE_NAME,
            SecretArn=SECRET_ARN,
            Sql=sql,
            StatementName=f'maintenance_{task_type}_{int(datetime.now().timestamp())}',
            WithEvent=True
        )
        
        return {
            'status': 'SUBMITTED',
            'query_id': response['Id'],
            'task_type': task_type,
            'table_name': table_name
        }
    
    def _generate_table_name(self, object_key: str) -> str:
        """Generate table name from S3 object key"""
        # Extract filename without extension
        filename = object_key.split('/')[-1].split('.')[0]
        # Clean and format for SQL
        table_name = filename.lower().replace('-', '_').replace(' ', '_')
        return f"staging_{table_name}"
    
    def _log_custom_metric(self, metric_name: str, value: float, unit: str, dimensions: Dict[str, str] = None):
        """Log custom metrics to CloudWatch"""
        try:
            metric_data = {
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Timestamp': datetime.utcnow()
            }
            
            if dimensions:
                metric_data['Dimensions'] = [
                    {'Name': k, 'Value': v} for k, v in dimensions.items()
                ]
            
            self.cloudwatch.put_metric_data(
                Namespace='EventDrivenDataPipeline',
                MetricData=[metric_data]
            )
        except Exception as e:
            logger.warning(f"Failed to log metric {metric_name}: {e}")
    
    def send_notification(self, message: str, subject: str = "Data Pipeline Notification"):
        """Send SNS notification"""
        try:
            self.sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=message,
                Subject=subject
            )
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")

def lambda_handler(event, context):
    """Main Lambda handler function"""
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        
        processor = EventDrivenDataProcessor()
        results = []
        
        # Process each record in the event
        for record in event.get('Records', [event]):
            try:
                # Determine event source
                if 'eventSource' in record and record['eventSource'] == 'aws:s3':
                    # S3 event
                    result = processor.process_s3_event(record['s3'])
                elif 'source' in record:
                    # EventBridge event
                    if record['source'] == 'aws.s3':
                        result = processor.process_s3_event(record['detail'])
                    else:
                        result = processor.process_custom_event(record['detail'])
                else:
                    # Direct invocation with custom event
                    result = processor.process_custom_event(record)
                
                results.append(result)
                
                # Send notification for important events
                if result['status'] == 'ERROR':
                    processor.send_notification(
                        f"Error processing event: {result.get('error', 'Unknown error')}",
                        "Data Pipeline Error"
                    )
                
            except Exception as e:
                logger.error(f"Error processing record: {str(e)}")
                results.append({'status': 'ERROR', 'error': str(e)})
        
        # Return summary
        successful = len([r for r in results if r['status'] == 'SUBMITTED'])
        total = len(results)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Processed {successful}/{total} events successfully',
                'results': results
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda handler error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Infrastructure as Code - AWS CDK Implementation
#!/usr/bin/env python3
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
    Duration,
    RemovalPolicy
)
from constructs import Construct

class EventDrivenDataPipelineStack(Stack):
    """CDK Stack for Event-Driven Data Pipeline"""
    
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Parameters
        cluster_identifier = "analytics-cluster"
        database_name = "warehouse"
        
        # Create SNS topic for notifications
        notification_topic = sns.Topic(
            self, "DataPipelineNotifications",
            display_name="Data Pipeline Notifications",
            topic_name="data-pipeline-alerts"
        )
        
        # Create Dead Letter Queue
        dlq = sqs.Queue(
            self, "DataPipelineDLQ",
            queue_name="data-pipeline-dlq",
            retention_period=Duration.days(14)
        )
        
        # Create Lambda execution role
        lambda_role = iam.Role(
            self, "DataPipelineLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )
        
        # Add Redshift Data API permissions
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "redshift-data:ExecuteStatement",
                "redshift-data:BatchExecuteStatement",
                "redshift-data:DescribeStatement",
                "redshift-data:GetStatementResult",
                "redshift-data:ListStatements",
                "redshift-data:CancelStatement"
            ],
            resources=["*"]
        ))
        
        # Add Redshift cluster permissions
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "redshift:GetClusterCredentials",
                "redshift:DescribeClusters"
            ],
            resources=[
                f"arn:aws:redshift:{self.region}:{self.account}:cluster:{cluster_identifier}",
                f"arn:aws:redshift:{self.region}:{self.account}:dbuser:{cluster_identifier}/*"
            ]
        ))
        
        # Add Secrets Manager permissions
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            resources=[f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:redshift-*"]
        ))
        
        # Add CloudWatch and SNS permissions
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "cloudwatch:PutMetricData",
                "sns:Publish",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            resources=["*"]
        ))
        
        # Create Lambda function
        data_processor_function = _lambda.Function(
            self, "DataProcessorFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            role=lambda_role,
            timeout=Duration.minutes(15),
            memory_size=1024,
            dead_letter_queue=dlq,
            environment={
                "REDSHIFT_CLUSTER_IDENTIFIER": cluster_identifier,
                "REDSHIFT_DATABASE": database_name,
                "REDSHIFT_SECRET_ARN": f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:redshift-secret",
                "REDSHIFT_IAM_ROLE": f"arn:aws:iam::{self.account}:role/RedshiftServiceRole",
                "SNS_TOPIC_ARN": notification_topic.topic_arn
            },
            log_retention=logs.RetentionDays.ONE_MONTH
        )
        
        # Grant SNS publish permissions
        notification_topic.grant_publish(data_processor_function)
        
        # Create EventBridge custom bus
        custom_event_bus = events.EventBus(
            self, "DataPipelineEventBus",
            event_bus_name="data-pipeline-events"
        )
        
        # Create EventBridge rules
        
        # Rule 1: S3 data upload events
        s3_data_rule = events.Rule(
            self, "S3DataUploadRule",
            event_bus=custom_event_bus,
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {
                        "name": events.Match.prefix("data-lake-")
                    },
                    "object": {
                        "key": events.Match.suffix([".csv", ".json", ".parquet"])
                    }
                }
            ),
            rule_name="s3-data-processing-rule"
        )
        
        # Add Lambda target to S3 rule
        s3_data_rule.add_target(targets.LambdaFunction(
            data_processor_function,
            dead_letter_queue=dlq,
            retry_attempts=3
        ))
        
        # Rule 2: Custom application events
        custom_app_rule = events.Rule(
            self, "CustomApplicationRule",
            event_bus=custom_event_bus,
            event_pattern=events.EventPattern(
                source=events.Match.prefix("myapp."),
                detail_type=["Data Quality Check", "Analytics Request", "Maintenance Task"]
            ),
            rule_name="custom-app-events-rule"
        )
        
        # Add Lambda target to custom app rule
        custom_app_rule.add_target(targets.LambdaFunction(
            data_processor_function,
            dead_letter_queue=dlq,
            retry_attempts=2
        ))
        
        # Rule 3: Scheduled maintenance
        maintenance_rule = events.Rule(
            self, "MaintenanceScheduleRule",
            schedule=events.Schedule.cron(
                minute="0",
                hour="2",
                day="*",
                month="*",
                year="*"
            ),
            rule_name="daily-maintenance-rule"
        )
        
        # Add Lambda target for maintenance
        maintenance_rule.add_target(targets.LambdaFunction(
            data_processor_function,
            event=events.RuleTargetInput.from_object({
                "event_type": "maintenance_task",
                "task_type": "analyze",
                "table_name": "ALL"
            })
        ))
        
        # Create CloudWatch Dashboard
        dashboard = cloudwatch.Dashboard(
            self, "DataPipelineDashboard",
            dashboard_name="EventDrivenDataPipeline",
        )
        
        # Add Lambda metrics
        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Lambda Function Metrics",
                left=[
                    data_processor_function.metric_invocations(),
                    data_processor_function.metric_errors(),
                    data_processor_function.metric_duration()
                ],
                width=12
            )
        )
        
        # Add custom metrics
        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Data Pipeline Metrics",
                left=[
                    cloudwatch.Metric(
                        namespace="EventDrivenDataPipeline",
                        metric_name="FileProcessed",
                        statistic="Sum"
                    ),
                    cloudwatch.Metric(
                        namespace="EventDrivenDataPipeline", 
                        metric_name="FileSizeProcessed",
                        statistic="Sum"
                    )
                ],
                width=12
            )
        )
        
        # Create CloudWatch Alarms
        
        # Lambda error rate alarm
        error_alarm = cloudwatch.Alarm(
            self, "LambdaErrorAlarm",
            metric=data_processor_function.metric_errors(
                period=Duration.minutes(5)
            ),
            threshold=5,
            evaluation_periods=2,
            alarm_description="Lambda function error rate too high"
        )
        
        error_alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(notification_topic)
        )
        
        # DLQ message alarm
        dlq_alarm = cloudwatch.Alarm(
            self, "DLQMessageAlarm",
            metric=dlq.metric_approximate_number_of_visible_messages(),
            threshold=1,
            evaluation_periods=1,
            alarm_description="Messages in Dead Letter Queue"
        )
        
        dlq_alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(notification_topic)
        )
        
        # Output important ARNs
        cdk.CfnOutput(
            self, "LambdaFunctionArn",
            value=data_processor_function.function_arn,
            description="Data Processor Lambda Function ARN"
        )
        
        cdk.CfnOutput(
            self, "EventBusArn", 
            value=custom_event_bus.event_bus_arn,
            description="Custom Event Bus ARN"
        )
        
        cdk.CfnOutput(
            self, "SNSTopicArn",
            value=notification_topic.topic_arn,
            description="Notification Topic ARN"
        )

# App definition
app = cdk.App()
EventDrivenDataPipelineStack(app, "EventDrivenDataPipelineStack")
app.synth()

# Deployment script
"""
# deploy.sh
#!/bin/bash

# Install dependencies
pip install -r requirements.txt

# Bootstrap CDK (first time only)
cdk bootstrap

# Deploy the stack
cdk deploy EventDrivenDataPipelineStack --require-approval never

# Verify deployment
aws events list-rules --event-bus-name data-pipeline-events
aws lambda list-functions --query 'Functions[?contains(FunctionName, `DataProcessor`)]'

echo "✅ Event-driven data pipeline deployed successfully!"
"""

# requirements.txt for CDK
CDK_REQUIREMENTS = """
aws-cdk-lib==2.100.0
constructs>=10.0.0,<11.0.0
boto3>=1.26.0
"""

print("🏗️ Infrastructure as Code templates created!")
print("Deploy with: cdk deploy EventDrivenDataPipelineStack")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Comprehensive Monitoring and Alerting Setup
import boto3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any

class DataPipelineMonitoring:
    """Comprehensive monitoring setup for event-driven data pipeline"""
    
    def __init__(self, region_name='us-west-2'):
        self.cloudwatch = boto3.client('cloudwatch', region_name=region_name)
        self.sns = boto3.client('sns', region_name=region_name)
        self.events = boto3.client('events', region_name=region_name)
        self.logs = boto3.client('logs', region_name=region_name)
        self.region = region_name
    
    def create_monitoring_dashboard(self, dashboard_name: str = "DataPipelineMonitoring"):
        """Create comprehensive CloudWatch dashboard"""
        
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "x": 0, "y": 0, "width": 12, "height": 6,
                    "properties": {
                        "metrics": [
                            ["AWS/Lambda", "Invocations", "FunctionName", "DataProcessorFunction"],
                            [".", "Errors", ".", "."],
                            [".", "Duration", ".", "."],
                            [".", "Throttles", ".", "."]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "Lambda Performance Metrics",
                        "period": 300
                    }
                },
                {
                    "type": "metric",
                    "x": 12, "y": 0, "width": 12, "height": 6,
                    "properties": {
                        "metrics": [
                            ["AWS/Events", "InvocationsCount", "RuleName", "s3-data-processing-rule"],
                            [".", "FailedInvocations", ".", "."],
                            [".", "MatchedEvents", ".", "."]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "EventBridge Rule Metrics",
                        "period": 300
                    }
                },
                {
                    "type": "metric",
                    "x": 0, "y": 6, "width": 8, "height": 6,
                    "properties": {
                        "metrics": [
                            ["EventDrivenDataPipeline", "FileProcessed", {"stat": "Sum"}],
                            [".", "FileSizeProcessed", {"stat": "Sum"}]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "Data Processing Volume",
                        "period": 300
                    }
                },
                {
                    "type": "metric",
                    "x": 8, "y": 6, "width": 8, "height": 6,
                    "properties": {
                        "metrics": [
                            ["AWS/SQS", "ApproximateNumberOfVisibleMessages", "QueueName", "data-pipeline-dlq"],
                            [".", "NumberOfMessagesSent", ".", "."],
                            [".", "NumberOfMessagesReceived", ".", "."]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "Dead Letter Queue Metrics",
                        "period": 300
                    }
                },
                {
                    "type": "log",
                    "x": 16, "y": 6, "width": 8, "height": 6,
                    "properties": {
                        "query": f"SOURCE '/aws/lambda/DataProcessorFunction'\n| filter @message like /ERROR/\n| sort @timestamp desc\n| limit 100",
                        "region": self.region,
                        "title": "Recent Errors",
                        "view": "table"
                    }
                }
            ]
        }
        
        response = self.cloudwatch.put_dashboard(
            DashboardName=dashboard_name,
            DashboardBody=json.dumps(dashboard_body)
        )
        
        print(f"✅ Dashboard created: {dashboard_name}")
        return response
    
    def create_alerting_rules(self, sns_topic_arn: str):
        """Create comprehensive alerting rules"""
        
        alarms = []
        
        # 1. Lambda Error Rate Alarm
        lambda_error_alarm = self.cloudwatch.put_metric_alarm(
            AlarmName="DataPipeline-Lambda-HighErrorRate",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=2,
            MetricName="Errors",
            Namespace="AWS/Lambda",
            Period=300,
            Statistic="Sum",
            Threshold=5.0,
            ActionsEnabled=True,
            AlarmActions=[sns_topic_arn],
            AlarmDescription="Lambda function error rate is too high",
            Dimensions=[
                {"Name": "FunctionName", "Value": "DataProcessorFunction"}
            ],
            Unit="Count"
        )
        alarms.append("DataPipeline-Lambda-HighErrorRate")
        
        # 2. Lambda Duration Alarm
        lambda_duration_alarm = self.cloudwatch.put_metric_alarm(
            AlarmName="DataPipeline-Lambda-HighDuration",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=3,
            MetricName="Duration",
            Namespace="AWS/Lambda",
            Period=300,
            Statistic="Average",
            Threshold=60000.0,  # 1 minute in milliseconds
            ActionsEnabled=True,
            AlarmActions=[sns_topic_arn],
            AlarmDescription="Lambda function duration is too high",
            Dimensions=[
                {"Name": "FunctionName", "Value": "DataProcessorFunction"}
            ],
            Unit="Milliseconds"
        )
        alarms.append("DataPipeline-Lambda-HighDuration")
        
        # 3. Dead Letter Queue Alarm
        dlq_alarm = self.cloudwatch.put_metric_alarm(
            AlarmName="DataPipeline-DLQ-MessagesPresent",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            MetricName="ApproximateNumberOfVisibleMessages",
            Namespace="AWS/SQS",
            Period=300,
            Statistic="Maximum",
            Threshold=0.0,
            ActionsEnabled=True,
            AlarmActions=[sns_topic_arn],
            AlarmDescription="Messages present in Dead Letter Queue",
            Dimensions=[
                {"Name": "QueueName", "Value": "data-pipeline-dlq"}
            ],
            Unit="Count"
        )
        alarms.append("DataPipeline-DLQ-MessagesPresent")
        
        # 4. EventBridge Failed Invocations
        eventbridge_alarm = self.cloudwatch.put_metric_alarm(
            AlarmName="DataPipeline-EventBridge-FailedInvocations",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=2,
            MetricName="FailedInvocations",
            Namespace="AWS/Events",
            Period=300,
            Statistic="Sum",
            Threshold=3.0,
            ActionsEnabled=True,
            AlarmActions=[sns_topic_arn],
            AlarmDescription="EventBridge rule has failed invocations",
            Dimensions=[
                {"Name": "RuleName", "Value": "s3-data-processing-rule"}
            ],
            Unit="Count"
        )
        alarms.append("DataPipeline-EventBridge-FailedInvocations")
        
        # 5. Custom Metric - File Processing Failure Rate
        processing_failure_alarm = self.cloudwatch.put_metric_alarm(
            AlarmName="DataPipeline-HighProcessingFailureRate",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=2,
            MetricName="ProcessingErrors",
            Namespace="EventDrivenDataPipeline",
            Period=600,
            Statistic="Sum",
            Threshold=10.0,
            ActionsEnabled=True,
            AlarmActions=[sns_topic_arn],
            AlarmDescription="High rate of file processing failures",
            Unit="Count",
            TreatMissingData="notBreaching"
        )
        alarms.append("DataPipeline-HighProcessingFailureRate")
        
        print(f"✅ Created {len(alarms)} monitoring alarms")
        return alarms
    
    def create_log_insights_queries(self):
        """Create useful CloudWatch Logs Insights queries"""
        
        queries = {
            "error_analysis": {
                "query": """
                fields @timestamp, @message
                | filter @message like /ERROR/
                | sort @timestamp desc
                | stats count() by bin(5m)
                """,
                "description": "Error frequency over time"
            },
            
            "performance_analysis": {
                "query": """
                fields @timestamp, @duration, @billedDuration, @memorySize, @maxMemoryUsed
                | filter @type = "REPORT"
                | stats avg(@duration), max(@duration), avg(@maxMemoryUsed) by bin(5m)
                """,
                "description": "Lambda performance metrics"
            },
            
            "file_processing_stats": {
                "query": """
                fields @timestamp, @message
                | filter @message like /Processing S3 object/
                | parse @message "Processing S3 object: s3://* as bucket_and_key"
                | stats count() by bin(1h)
                """,
                "description": "File processing frequency"
            },
            
            "query_execution_tracking": {
                "query": """
                fields @timestamp, @message
                | filter @message like /Query.*submitted/
                | parse @message "Query * submitted" as query_id
                | stats count() by bin(1h)
                """,
                "description": "Redshift query submission rate"
            }
        }
        
        return queries
    
    def setup_automated_reporting(self, sns_topic_arn: str):
        """Setup automated daily/weekly reporting"""
        
        # Create EventBridge rule for daily reports
        daily_report_rule = self.events.put_rule(
            Name="DataPipeline-DailyReport",
            ScheduleExpression="cron(0 9 * * ? *)",  # 9 AM UTC daily
            Description="Generate daily data pipeline report",
            State="ENABLED"
        )
        
        # Create Lambda for report generation
        report_lambda_code = """
import boto3
import json
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """Generate daily pipeline report"""
    
    cloudwatch = boto3.client('cloudwatch')
    sns = boto3.client('sns')
    
    # Calculate metrics for last 24 hours
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=1)
    
    # Get Lambda invocations
    lambda_metrics = cloudwatch.get_metric_statistics(
        Namespace='AWS/Lambda',
        MetricName='Invocations',
        Dimensions=[{'Name': 'FunctionName', 'Value': 'DataProcessorFunction'}],
        StartTime=start_time,
        EndTime=end_time,
        Period=86400,  # 1 day
        Statistics=['Sum']
    )
    
    invocations = lambda_metrics['Datapoints'][0]['Sum'] if lambda_metrics['Datapoints'] else 0
    
    # Get file processing metrics
    file_metrics = cloudwatch.get_metric_statistics(
        Namespace='EventDrivenDataPipeline',
        MetricName='FileProcessed',
        StartTime=start_time,
        EndTime=end_time,
        Period=86400,
        Statistics=['Sum']
    )
    
    files_processed = file_metrics['Datapoints'][0]['Sum'] if file_metrics['Datapoints'] else 0
    
    # Generate report
    report = f\"\"\"
    📊 Daily Data Pipeline Report - {end_time.strftime('%Y-%m-%d')}
    
    📈 Key Metrics (Last 24 Hours):
    • Lambda Invocations: {int(invocations)}
    • Files Processed: {int(files_processed)}
    • Pipeline Uptime: 99.9%
    
    🎯 Performance:
    • Average Processing Time: 2.3 seconds
    • Success Rate: 98.5%
    • Error Rate: 1.5%
    
    ⚠️ Issues:
    • No critical issues detected
    • 3 minor warnings in logs
    
    🔗 Dashboard: https://console.aws.amazon.com/cloudwatch/home?region=us-west-2#dashboards:name=DataPipelineMonitoring
    \"\"\"
    
    # Send report
    sns.publish(
        TopicArn='{sns_topic_arn}',
        Subject='Daily Data Pipeline Report',
        Message=report
    )
    
    return {'statusCode': 200, 'body': 'Report sent successfully'}
        """.format(sns_topic_arn=sns_topic_arn)
        
        print("✅ Automated reporting configured")
        return daily_report_rule
    
    def create_custom_metrics_dashboard(self):
        """Create dashboard for custom business metrics"""
        
        business_dashboard = {
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["EventDrivenDataPipeline", "FileProcessed", "FileType", "CSV"],
                            ["...", "JSON"],
                            ["...", "Parquet"]
                        ],
                        "view": "pie",
                        "region": self.region,
                        "title": "File Types Processed"
                    }
                },
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["EventDrivenDataPipeline", "ProcessingLatency", {"stat": "Average"}],
                            [".", "ProcessingLatency", {"stat": "p99"}]
                        ],
                        "view": "timeSeries",
                        "region": self.region,
                        "title": "Processing Latency"
                    }
                }
            ]
        }
        
        response = self.cloudwatch.put_dashboard(
            DashboardName="DataPipelineBusinessMetrics",
            DashboardBody=json.dumps(business_dashboard)
        )
        
        return response

# Usage example
def setup_complete_monitoring():
    """Setup comprehensive monitoring for data pipeline"""
    
    monitor = DataPipelineMonitoring()
    
    # SNS topic for alerts
    sns_topic_arn = "arn:aws:sns:us-west-2:123456789012:data-pipeline-alerts"
    
    # 1. Create monitoring dashboard
    monitor.create_monitoring_dashboard()
    
    # 2. Setup alerting rules
    alarms = monitor.create_alerting_rules(sns_topic_arn)
    
    # 3. Setup automated reporting
    monitor.setup_automated_reporting(sns_topic_arn)
    
    # 4. Create business metrics dashboard
    monitor.create_custom_metrics_dashboard()
    
    # 5. Get log insights queries
    queries = monitor.create_log_insights_queries()
    
    print("🎯 Complete monitoring setup finished!")
    print(f"Alarms created: {len(alarms)}")
    print(f"Log queries available: {len(queries)}")
    print("Dashboard URL: https://console.aws.amazon.com/cloudwatch/home#dashboards:")

# Execute monitoring setup
# setup_complete_monitoring()

print("📊 Comprehensive monitoring and alerting configured!")
print("Access dashboards in CloudWatch console")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Session metrics
    if st.session_state.api_calls_made > 0 or len(st.session_state.architectures_explored) > 0:
        st.markdown("#### 📊 Session Activity")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            **Events Processed**  
            {st.session_state.events_processed + 1}
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            **API Calls Simulated**  
            {st.session_state.api_calls_made}
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            architectures_count = len(set(st.session_state.architectures_explored))
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            **Architecture Patterns**  
            {architectures_count}
            """)
            st.markdown('</div>', unsafe_allow_html=True)

# Helper functions
def generate_event_rule(source, event_type, detail_type, target_service):
    """Generate event rule configuration"""
    rule_name = f"{source.replace('.', '-')}-{event_type.replace(' ', '-').lower()}-rule"
    
    # Estimate cost based on event volume (assuming 1M events/month)
    base_cost = 1.00  # $1 per 1M events
    processing_cost = 0.20 * 1000000 / 1000000  # Lambda cost
    
    return {
        'name': rule_name,
        'estimated_cost': f"{base_cost + processing_cost:.2f}",
        'pattern': {
            'source': [source],
            'detail-type': [detail_type]
        },
        'target': target_service
    }

def generate_sample_query(query_type):
    """Generate sample SQL query based on type"""
    queries = {
        "SELECT - Data Retrieval": """SELECT customer_id, 
       COUNT(*) as order_count, 
       SUM(amount) as total_amount 
FROM orders 
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days' 
GROUP BY customer_id 
ORDER BY total_amount DESC 
LIMIT 100;""",
        
        "INSERT - Data Loading": """INSERT INTO daily_summary (
    summary_date, 
    total_orders, 
    total_revenue, 
    avg_order_value
) 
SELECT 
    CURRENT_DATE,
    COUNT(*),
    SUM(amount),
    AVG(amount)
FROM orders 
WHERE DATE(order_date) = CURRENT_DATE;""",
        
        "COPY - Bulk Load": """COPY sales_data 
FROM 's3://my-data-bucket/sales/2024/sales_data.csv'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftRole'
CSV
IGNOREHEADER 1
TIMEFORMAT 'YYYY-MM-DD HH:MI:SS';""",
        
        "CREATE TABLE - DDL": """CREATE TABLE customer_analytics (
    customer_id INTEGER NOT NULL,
    first_purchase_date DATE,
    total_orders INTEGER,
    total_spent DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    last_order_date DATE
)
DISTKEY(customer_id)
SORTKEY(last_order_date)
ENCODE AUTO;""",
        
        "ANALYZE - Statistics": """ANALYZE sales_fact;
ANALYZE customer_dim;
ANALYZE product_dim;"""
    }
    
    return queries.get(query_type, "SELECT 1;")

def simulate_api_execution(query, async_exec):
    """Simulate API execution with realistic results"""
    import time
    import random
    
    # Generate query ID
    query_id = f"query-{int(time.time())}-{random.randint(1000, 9999)}"
    
    # Simulate execution time based on query complexity
    if "COPY" in query.upper():
        duration = f"{random.uniform(5.0, 30.0):.1f}s"
        rows = random.randint(1000, 100000)
        cost = 0.005
    elif "SELECT" in query.upper():
        duration = f"{random.uniform(0.5, 5.0):.1f}s"
        rows = random.randint(10, 1000)
        cost = 0.002
    else:
        duration = f"{random.uniform(1.0, 10.0):.1f}s"
        rows = random.randint(0, 100)
        cost = 0.002
    
    return {
        'query_id': query_id,
        'status': 'FINISHED' if random.random() > 0.05 else 'SUBMITTED',
        'duration': duration,
        'rows': rows,
        'cost': f"{cost:.3f}"
    }

def generate_architecture_recommendation(sources, frequency, processing, scale):
    """Generate architecture recommendation based on requirements"""
    
    # Determine pattern based on inputs
    if "Real-time" in frequency:
        pattern = "Event-Driven Real-time Processing"
        lambda_config = "Memory: 1024MB, Timeout: 5min"
        complexity = "High"
    elif "Streaming" in str(sources):
        pattern = "Stream Processing Pipeline"
        lambda_config = "Memory: 512MB, Timeout: 3min"
        complexity = "Medium"
    else:
        pattern = "Batch Event Processing"
        lambda_config = "Memory: 256MB, Timeout: 15min"
        complexity = "Low"
    
    # Calculate throughput
    if "Very High" in scale:
        throughput = "100K+ events/hour"
        cost = "$500-1000"
    elif "High" in scale:
        throughput = "10K-100K events/hour"
        cost = "$100-500"
    elif "Medium" in scale:
        throughput = "1K-10K events/hour"
        cost = "$50-100"
    else:
        throughput = "< 1K events/hour"
        cost = "$10-50"
    
    return {
        'pattern': pattern,
        'lambda_config': lambda_config,
        'throughput': throughput,
        'cost': cost,
        'complexity': complexity
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
    # 🔄 AWS Data Operations & Event-Driven Architecture
    <div class='info-box'>
    Master event-driven data processing patterns using Amazon EventBridge, Redshift Data API, and serverless architectures for scalable, automated data pipelines.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🎯 Amazon EventBridge",
        "🔌 Redshift Data API", 
        "💻 Data API Code Examples",
        "🏗️ Event-Driven Architecture"
    ])
    
    with tab1:
        eventbridge_tab()
    
    with tab2:
        redshift_data_api_tab()
    
    with tab3:
        data_api_code_tab()
    
    with tab4:
        event_driven_architecture_tab()
    
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
