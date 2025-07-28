
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
    page_title="AWS Data Operations & Support",
    page_icon="🔧",
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
    'error': '#DC3545'
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
        
        .code-container {{
            background-color: {AWS_COLORS['dark_blue']};
            color: white;
            padding: 20px;
            border-radius: 10px;
            border-left: 4px solid {AWS_COLORS['primary']};
            margin: 15px 0;
        }}
        
        .workflow-card {{
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
        st.session_state.workflows_created = []
        st.session_state.monitoring_alerts = []
        st.session_state.pipeline_status = "idle"

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🔄 Data pipelines on AWS - Learn about orchestration options and best practices
            - 🎯 AWS Glue Workflow - Create and manage complex ETL workflows visually
            - 📊 Monitoring a Data Lake - Implement comprehensive monitoring solutions
            
            **Learning Objectives:**
            - Master data pipeline orchestration techniques
            - Understand AWS Glue Workflow capabilities
            - Implement monitoring and alerting strategies
            - Learn troubleshooting and optimization methods
            """)

def create_data_pipeline_architecture():
    """Create mermaid diagram for data pipeline architecture"""
    return """
    graph TB
        subgraph "Data Sources"
            S3[Amazon S3<br/>Raw Data]
            RDS[Amazon RDS<br/>Transactional Data]
            STREAM[Kinesis Data<br/>Streams]
        end
        
        subgraph "Orchestration Layer"
            AIRFLOW[Amazon MWAA<br/>Apache Airflow]
            STEP[AWS Step Functions<br/>Serverless Orchestration]
            GLUE_WF[AWS Glue Workflow<br/>ETL Orchestration]
        end
        
        subgraph "Processing Layer"
            GLUE[AWS Glue<br/>ETL Jobs]
            EMR[Amazon EMR<br/>Big Data Processing]
            LAMBDA[AWS Lambda<br/>Event Processing]
        end
        
        subgraph "Storage & Analytics"
            REDSHIFT[Amazon Redshift<br/>Data Warehouse]
            ATHENA[Amazon Athena<br/>Query Service]
            QUICKSIGHT[Amazon QuickSight<br/>Business Intelligence]
        end
        
        subgraph "Monitoring & Governance"
            CLOUDWATCH[Amazon CloudWatch<br/>Monitoring]
            CLOUDTRAIL[AWS CloudTrail<br/>Audit Logs]
        end
        
        S3 --> AIRFLOW
        RDS --> STEP
        STREAM --> GLUE_WF
        
        AIRFLOW --> GLUE
        STEP --> EMR
        GLUE_WF --> LAMBDA
        
        GLUE --> REDSHIFT
        EMR --> ATHENA
        LAMBDA --> QUICKSIGHT
        
        GLUE --> CLOUDWATCH
        EMR --> CLOUDTRAIL
        
        style AIRFLOW fill:#FF9900,stroke:#232F3E,color:#fff
        style STEP fill:#4B9EDB,stroke:#232F3E,color:#fff
        style GLUE_WF fill:#3FB34F,stroke:#232F3E,color:#fff
        style CLOUDWATCH fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_glue_workflow_diagram():
    """Create mermaid diagram for AWS Glue Workflow"""
    return """
    graph TD
        START[Workflow Start] --> TRIGGER1{Schedule Trigger}
        TRIGGER1 --> CRAWLER1[Data Catalog Crawler<br/>🕷️ Discover Schema]
        
        CRAWLER1 --> JOB1[Glue ETL Job 1<br/>🔄 Clean & Transform]
        JOB1 --> JOB2[Glue ETL Job 2<br/>📊 Aggregate Data]
        JOB2 --> JOB3[Glue ETL Job 3<br/>💾 Load to Warehouse]
        
        JOB3 --> CRAWLER2[Target Crawler<br/>🔍 Update Catalog]
        CRAWLER2 --> NOTIFICATION[SNS Notification<br/>📧 Success Alert]
        
        JOB1 --> ERROR{Job Failed?}
        JOB2 --> ERROR
        JOB3 --> ERROR
        
        ERROR -->|Yes| ALARM[CloudWatch Alarm<br/>🚨 Failure Alert]
        ERROR -->|No| NOTIFICATION
        
        ALARM --> RETRY[Retry Logic<br/>🔄 Automatic Retry]
        RETRY --> JOB1
        
        style START fill:#FF9900,stroke:#232F3E,color:#fff
        style TRIGGER1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CRAWLER1 fill:#3FB34F,stroke:#232F3E,color:#fff
        style JOB1 fill:#3FB34F,stroke:#232F3E,color:#fff
        style JOB2 fill:#3FB34F,stroke:#232F3E,color:#fff
        style JOB3 fill:#3FB34F,stroke:#232F3E,color:#fff
        style ERROR fill:#DC3545,stroke:#232F3E,color:#fff
        style ALARM fill:#FFC107,stroke:#232F3E,color:#000
    """

def create_monitoring_architecture():
    """Create mermaid diagram for monitoring architecture"""
    return """
    graph TB
        subgraph "Data Pipeline Components"
            PIPELINE[Data Pipeline<br/>ETL Jobs]
            STORAGE[Data Storage<br/>S3, Redshift, RDS]
            COMPUTE[Compute Resources<br/>EMR, Glue, Lambda]
        end
        
        subgraph "Monitoring Services"
            CW[Amazon CloudWatch<br/>📊 Metrics & Logs]
            CT[AWS CloudTrail<br/>🔍 API Audit Logs]
            XR[AWS X-Ray<br/>🔬 Distributed Tracing]
        end
        
        subgraph "Alerting & Actions"
            SNS[Amazon SNS<br/>📧 Notifications]
            LAMBDA_ALERT[Lambda Functions<br/>⚡ Auto-remediation]
            DASHBOARD[CloudWatch Dashboard<br/>📈 Visual Monitoring]
        end
        
        subgraph "Log Analytics"
            CW_INSIGHTS[CloudWatch Logs Insights<br/>🔎 Query Logs]
            ATHENA_LOGS[Amazon Athena<br/>📊 Log Analytics]
            OPENSEARCH[Amazon OpenSearch<br/>🔍 Search & Analytics]
        end
        
        PIPELINE --> CW
        STORAGE --> CT
        COMPUTE --> XR
        
        CW --> SNS
        CW --> LAMBDA_ALERT
        CW --> DASHBOARD
        
        CW --> CW_INSIGHTS
        CT --> ATHENA_LOGS
        CW --> OPENSEARCH
        
        SNS --> LAMBDA_ALERT
        
        style CW fill:#FF9900,stroke:#232F3E,color:#fff
        style CT fill:#4B9EDB,stroke:#232F3E,color:#fff
        style XR fill:#3FB34F,stroke:#232F3E,color:#fff
        style DASHBOARD fill:#232F3E,stroke:#FF9900,color:#fff
    """

def data_pipelines_tab():
    """Content for Data Pipelines on AWS tab"""
    st.markdown("## 🔄 Data Pipelines on AWS")
    st.markdown("*Build efficient, scalable, and reliable data integration pipelines*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Pipeline Orchestration Fundamentals
    An efficient data integration pipeline requires proper orchestration to coordinate data movement, 
    transformation, and loading processes across multiple AWS services. Choose the right orchestration 
    tool based on your specific requirements:
    - **Complexity**: From simple scheduled jobs to complex workflows
    - **Integration**: Native AWS service integration vs. third-party compatibility
    - **Management**: Serverless vs. managed infrastructure
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ Data Pipeline Architecture")
    common.mermaid(create_data_pipeline_architecture(), height=700)
    
    # Orchestration comparison
    st.markdown("#### 📊 Orchestration Tools Comparison")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="workflow-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🌪️ Amazon MWAA
        **Apache Airflow**
        
        **Best For:**
        - Existing Airflow workflows
        - Complex DAG dependencies
        - Python-based orchestration
        
        **Infrastructure:** Managed Service
        **Learning Curve:** Medium to High
        **Cost Model:** Instance-based pricing
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="workflow-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚡ AWS Step Functions
        **Visual Workflow Service**
        
        **Best For:**
        - Multi-service integration
        - Event-driven architectures
        - Serverless workflows
        
        **Infrastructure:** Serverless
        **Learning Curve:** Low to Medium
        **Cost Model:** Pay-per-execution
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="workflow-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 AWS Glue Workflow
        **ETL-Focused Orchestration**
        
        **Best For:**
        - Glue jobs and crawlers
        - Data lake ETL pipelines
        - Catalog-driven workflows
        
        **Infrastructure:** Serverless
        **Learning Curve:** Low
        **Cost Model:** Pay-per-use
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive pipeline builder
    st.markdown("#### 🎨 Interactive Pipeline Builder")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("##### Pipeline Configuration")
        pipeline_type = st.selectbox("Pipeline Type", [
            "Batch Processing", 
            "Real-time Streaming", 
            "Hybrid (Batch + Stream)",
            "Event-driven"
        ])
        
        data_volume = st.selectbox("Data Volume", [
            "Small (< 1GB/day)",
            "Medium (1-100GB/day)", 
            "Large (100GB-1TB/day)",
            "Very Large (> 1TB/day)"
        ])
        
        complexity = st.selectbox("Workflow Complexity", [
            "Simple (Linear flow)",
            "Medium (Branching logic)",
            "Complex (Conditional paths)",
            "Very Complex (Dynamic workflows)"
        ])
        
        sla_requirement = st.selectbox("SLA Requirement", [
            "Near real-time (< 5 min)",
            "Hourly processing",
            "Daily batch processing",
            "Weekly/Monthly processing"
        ])
    
    with col2:
        # Generate recommendations
        recommendation = get_pipeline_recommendation(pipeline_type, data_volume, complexity, sla_requirement)
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 🎯 Pipeline Recommendation
        **Recommended Orchestrator**: {recommendation['orchestrator']}  
        **Architecture Pattern**: {recommendation['pattern']}  
        **Key Components**: {', '.join(recommendation['components'])}  
        **Estimated Cost**: {recommendation['cost_estimate']}  
        **Implementation Time**: {recommendation['implementation_time']}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Cost breakdown visualization
        fig = px.pie(
            values=recommendation['cost_breakdown'].values(),
            names=recommendation['cost_breakdown'].keys(),
            title="Estimated Monthly Cost Breakdown",
            color_discrete_sequence=['#FF9900', '#4B9EDB', '#3FB34F', '#FFC107']
        )
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    # Code examples
    st.markdown("#### 💻 Pipeline Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Amazon MWAA (Airflow)", "AWS Step Functions", "AWS Glue Workflow"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Amazon MWAA - Apache Airflow DAG Example
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
import boto3

# DAG Configuration
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'catchup': False
}

# Define DAG
dag = DAG(
    'sales_data_pipeline',
    default_args=default_args,
    description='Daily sales data processing pipeline',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    max_active_runs=1,
    tags=['sales', 'etl', 'daily']
)

# Task 1: Check for new data files in S3
check_new_files = S3KeySensor(
    task_id='check_new_sales_files',
    bucket_name='sales-raw-data',
    bucket_key='daily-exports/{{ ds }}/sales_data.csv',
    wildcard_match=True,
    timeout=3600,  # 1 hour timeout
    poke_interval=300,  # Check every 5 minutes
    dag=dag
)

# Task 2: Data Quality Validation
def validate_data_quality(**context):
    """Validate incoming data quality"""
    s3_client = boto3.client('s3')
    bucket = 'sales-raw-data'
    key = f"daily-exports/{context['ds']}/sales_data.csv"
    
    # Download and validate file
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        # Basic validation checks
        lines = content.split('\n')
        if len(lines) < 2:
            raise ValueError("File appears to be empty or malformed")
        
        # Check for expected columns
        headers = lines[0].split(',')
        expected_columns = ['order_id', 'customer_id', 'product_id', 'amount', 'order_date']
        
        for col in expected_columns:
            if col not in headers:
                raise ValueError(f"Missing required column: {col}")
        
        print(f"Data validation passed. Found {len(lines)-1} records.")
        return True
        
    except Exception as e:
        print(f"Data validation failed: {str(e)}")
        raise

validate_data = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)

# Task 3: Run Glue ETL Job for Data Transformation
transform_data = GlueJobOperator(
    task_id='transform_sales_data',
    job_name='sales-data-transformation',
    job_desc='Transform raw sales data to analytics format',
    script_location='s3://glue-scripts/transform_sales_data.py',
    s3_bucket='glue-job-outputs',
    iam_role_name='GlueServiceRole',
    create_job_kwargs={
        'GlueVersion': '4.0',
        'NumberOfWorkers': 5,
        'WorkerType': 'G.1X',
        'Timeout': 2880,  # 48 hours
        'MaxConcurrentRuns': 1
    },
    job_arguments={
        '--input_path': 's3://sales-raw-data/daily-exports/{{ ds }}/',
        '--output_path': 's3://sales-processed-data/year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}/month={{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}/day={{ macros.ds_format(ds, "%Y-%m-%d", "%d") }}/',
        '--database_name': 'sales_analytics',
        '--table_name': 'daily_sales_fact'
    },
    dag=dag
)

# Task 4: Load Data to Redshift
def load_to_redshift(**context):
    """Load transformed data to Redshift"""
    import psycopg2
    
    # Redshift connection parameters
    conn_params = {
        'host': 'redshift-cluster.abc123.us-west-2.redshift.amazonaws.com',
        'port': 5439,
        'database': 'analytics',
        'user': 'etl_user',
        'password': context['var']['value'].get('redshift_password')
    }
    
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # COPY command to load data from S3
        copy_sql = f"""
        COPY sales_fact (
            order_id, customer_id, product_id, amount, order_date, 
            processed_date, data_source
        )
        FROM 's3://sales-processed-data/year={context['ds'][:4]}/month={context['ds'][5:7]}/day={context['ds'][8:10]}/'
        IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3AccessRole'
        FORMAT AS PARQUET
        COMPUPDATE ON
        STATUPDATE ON;
        """
        
        cursor.execute(copy_sql)
        
        # Get row count
        cursor.execute("SELECT COUNT(*) FROM sales_fact WHERE processed_date = %s", (context['ds'],))
        row_count = cursor.fetchone()[0]
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Successfully loaded {row_count} records to Redshift")
        return row_count
        
    except Exception as e:
        print(f"Redshift load failed: {str(e)}")
        raise

load_redshift = PythonOperator(
    task_id='load_to_redshift',
    python_callable=load_to_redshift,
    dag=dag
)

# Task 5: Data Quality Checks Post-Load
def post_load_validation(**context):
    """Validate data after loading to Redshift"""
    import psycopg2
    
    conn_params = {
        'host': 'redshift-cluster.abc123.us-west-2.redshift.amazonaws.com',
        'port': 5439,
        'database': 'analytics',
        'user': 'etl_user',
        'password': context['var']['value'].get('redshift_password')
    }
    
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Check for data completeness
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(amount) as total_amount,
                MIN(order_date) as min_date,
                MAX(order_date) as max_date
            FROM sales_fact 
            WHERE processed_date = %s
        """, (context['ds'],))
        
        stats = cursor.fetchone()
        
        # Validation rules
        if stats[0] == 0:
            raise ValueError("No records found in target table")
        
        if stats[2] <= 0:
            raise ValueError("Total sales amount is zero or negative")
        
        print(f"Post-load validation passed:")
        print(f"  Total Records: {stats[0]}")
        print(f"  Unique Customers: {stats[1]}")
        print(f"  Total Amount: ${stats[2]:,.2f}")
        
        cursor.close()
        conn.close()
        
        return stats[0]
        
    except Exception as e:
        print(f"Post-load validation failed: {str(e)}")
        raise

validate_post_load = PythonOperator(
    task_id='validate_post_load',
    python_callable=post_load_validation,
    dag=dag
)

# Task 6: Send Success Notification
send_notification = SnsPublishOperator(
    task_id='send_success_notification',
    target_arn='arn:aws:sns:us-west-2:123456789012:data-pipeline-alerts',
    message='Sales data pipeline completed successfully for {{ ds }}',
    subject='Daily Sales ETL - Success',
    dag=dag
)

# Task Dependencies
check_new_files >> validate_data >> transform_data >> load_redshift >> validate_post_load >> send_notification

# Error handling: Send failure notification
def send_failure_alert(context):
    """Send failure notification"""
    sns_client = boto3.client('sns')
    
    message = f"""
    Sales Data Pipeline Failed
    
    DAG: {context['dag'].dag_id}
    Task: {context['task_instance'].task_id}
    Execution Date: {context['ds']}
    Error: {context['exception']}
    
    Please check the Airflow logs for more details.
    """
    
    sns_client.publish(
        TopicArn='arn:aws:sns:us-west-2:123456789012:data-pipeline-alerts',
        Message=message,
        Subject='Sales ETL Pipeline - FAILURE'
    )

# Apply failure callback to all tasks
for task in dag.tasks:
    task.on_failure_callback = send_failure_alert
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# AWS Step Functions - Data Pipeline State Machine
import json
import boto3

# Step Functions State Machine Definition
state_machine_definition = {
    "Comment": "Sales Data Processing Pipeline",
    "StartAt": "CheckS3Files",
    "States": {
        "CheckS3Files": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": "check-s3-files-function",
                "Payload": {
                    "bucket.$": "$.bucket",
                    "prefix.$": "$.prefix",
                    "execution_id.$": "$$.Execution.Name"
                }
            },
            "ResultPath": "$.filecheck",
            "Next": "FilesAvailable?"
        },
        
        "FilesAvailable?": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.filecheck.Payload.files_found",
                    "BooleanEquals": True,
                    "Next": "ValidateData"
                }
            ],
            "Default": "NoFilesFound"
        },
        
        "NoFilesFound": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
                "TopicArn": "arn:aws:sns:us-west-2:123456789012:pipeline-alerts",
                "Message": "No files found for processing",
                "Subject": "Sales Pipeline - No Data"
            },
            "End": True
        },
        
        "ValidateData": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": "validate-data-function",
                "Payload": {
                    "files.$": "$.filecheck.Payload.files",
                    "validation_rules.$": "$.validation_rules"
                }
            },
            "ResultPath": "$.validation",
            "Retry": [
                {
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 5,
                    "MaxAttempts": 3,
                    "BackoffRate": 2.0
                }
            ],
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "Next": "ValidationFailed",
                    "ResultPath": "$.error"
                }
            ],
            "Next": "ProcessInParallel"
        },
        
        "ProcessInParallel": {
            "Type": "Parallel",
            "Branches": [
                {
                    "StartAt": "TransformSalesData",
                    "States": {
                        "TransformSalesData": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "sales-data-transformation",
                                "Arguments": {
                                    "--input_path.$": "$.filecheck.Payload.input_path",
                                    "--output_path.$": "$.output_paths.sales",
                                    "--job_bookmark_option": "job-bookmark-enable"
                                }
                            },
                            "ResultPath": "$.sales_transform",
                            "End": True
                        }
                    }
                },
                {
                    "StartAt": "TransformCustomerData", 
                    "States": {
                        "TransformCustomerData": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "customer-data-transformation",
                                "Arguments": {
                                    "--input_path.$": "$.filecheck.Payload.input_path",
                                    "--output_path.$": "$.output_paths.customer"
                                }
                            },
                            "ResultPath": "$.customer_transform",
                            "End": True
                        }
                    }
                }
            ],
            "ResultPath": "$.parallel_results",
            "Next": "LoadToWarehouse"
        },
        
        "LoadToWarehouse": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": "load-to-redshift-function",
                "Payload": {
                    "sales_data_path.$": "$.parallel_results[0].sales_transform.JobRunState",
                    "customer_data_path.$": "$.parallel_results[1].customer_transform.JobRunState",
                    "execution_id.$": "$$.Execution.Name"
                }
            },
            "ResultPath": "$.load_result",
            "Retry": [
                {
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 30,
                    "MaxAttempts": 2,
                    "BackoffRate": 2.0
                }
            ],
            "Next": "PostLoadValidation"
        },
        
        "PostLoadValidation": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": "post-load-validation-function",
                "Payload": {
                    "load_statistics.$": "$.load_result.Payload",
                    "validation_thresholds": {
                        "min_records": 1000,
                        "max_error_rate": 0.01
                    }
                }
            },
            "ResultPath": "$.validation_result",
            "Next": "ValidationPassed?"
        },
        
        "ValidationPassed?": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.validation_result.Payload.validation_passed",
                    "BooleanEquals": True,
                    "Next": "UpdateDataCatalog"
                }
            ],
            "Default": "ValidationFailed"
        },
        
        "UpdateDataCatalog": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
            "Parameters": {
                "Name": "sales-analytics-crawler"
            },
            "ResultPath": "$.crawler_result",
            "Next": "SendSuccessNotification"
        },
        
        "SendSuccessNotification": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
                "TopicArn": "arn:aws:sns:us-west-2:123456789012:pipeline-alerts",
                "Message.$": "$.load_result.Payload.success_message",
                "Subject": "Sales Pipeline - Success"
            },
            "End": True
        },
        
        "ValidationFailed": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
                "TopicArn": "arn:aws:sns:us-west-2:123456789012:pipeline-alerts",
                "Message": "Data validation failed. Please check logs.",
                "Subject": "Sales Pipeline - Validation Error"
            },
            "Next": "FailureCleanup"
        },
        
        "FailureCleanup": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": "cleanup-failed-pipeline-function",
                "Payload": {
                    "execution_id.$": "$$.Execution.Name",
                    "cleanup_paths.$": "$.output_paths"
                }
            },
            "End": True
        }
    }
}

# Create Step Functions State Machine
def create_state_machine():
    """Create the Step Functions state machine"""
    
    stepfunctions = boto3.client('stepfunctions')
    
    try:
        response = stepfunctions.create_state_machine(
            name='SalesDataPipeline',
            definition=json.dumps(state_machine_definition),
            roleArn='arn:aws:iam::123456789012:role/StepFunctionsExecutionRole',
            tags=[
                {'key': 'Environment', 'value': 'Production'},
                {'key': 'Team', 'value': 'DataEngineering'},
                {'key': 'Pipeline', 'value': 'Sales'}
            ]
        )
        
        print(f"State machine created: {response['stateMachineArn']}")
        return response['stateMachineArn']
        
    except Exception as e:
        print(f"Failed to create state machine: {str(e)}")
        raise

# Execute the pipeline
def start_pipeline_execution(state_machine_arn, input_data):
    """Start pipeline execution"""
    
    stepfunctions = boto3.client('stepfunctions')
    
    execution_input = {
        "bucket": "sales-raw-data",
        "prefix": f"daily-exports/{input_data['processing_date']}/",
        "output_paths": {
            "sales": f"s3://sales-processed-data/sales/{input_data['processing_date']}/",
            "customer": f"s3://sales-processed-data/customers/{input_data['processing_date']}/"
        },
        "validation_rules": {
            "required_columns": ["order_id", "customer_id", "product_id", "amount"],
            "null_tolerance": 0.05,
            "duplicate_tolerance": 0.01
        }
    }
    
    try:
        response = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            name=f"sales-pipeline-{input_data['processing_date']}-{int(datetime.now().timestamp())}",
            input=json.dumps(execution_input)
        )
        
        print(f"Pipeline execution started: {response['executionArn']}")
        return response['executionArn']
        
    except Exception as e:
        print(f"Failed to start pipeline execution: {str(e)}")
        raise

# Monitor pipeline execution
def monitor_execution(execution_arn):
    """Monitor pipeline execution status"""
    
    stepfunctions = boto3.client('stepfunctions')
    
    while True:
        response = stepfunctions.describe_execution(
            executionArn=execution_arn
        )
        
        status = response['status']
        print(f"Execution status: {status}")
        
        if status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
            if status == 'SUCCEEDED':
                print("Pipeline completed successfully!")
            else:
                print(f"Pipeline failed with status: {status}")
                if 'error' in response:
                    print(f"Error: {response['error']}")
            break
        
        time.sleep(30)  # Check every 30 seconds

# Usage example
if __name__ == "__main__":
    # Create state machine
    state_machine_arn = create_state_machine()
    
    # Start execution
    execution_arn = start_pipeline_execution(state_machine_arn, {
        'processing_date': '2025-07-14'
    })
    
    # Monitor execution
    monitor_execution(execution_arn)
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# AWS Glue Workflow - Python SDK Implementation
import boto3
import json
from datetime import datetime

glue_client = boto3.client('glue')

def create_glue_workflow():
    """Create AWS Glue Workflow for ETL pipeline"""
    
    workflow_name = 'sales-etl-workflow'
    
    try:
        # Create the workflow
        response = glue_client.create_workflow(
            Name=workflow_name,
            Description='Daily sales data ETL workflow',
            DefaultRunProperties={
                'glue.workflow.max.concurrent.runs': '1',
                'processing.date': '{{ today }}',
                'notification.topic': 'arn:aws:sns:us-west-2:123456789012:etl-notifications'
            },
            Tags={
                'Environment': 'Production',
                'Team': 'DataEngineering',
                'Purpose': 'SalesAnalytics'
            }
        )
        
        print(f"Workflow created: {workflow_name}")
        return workflow_name
        
    except Exception as e:
        print(f"Failed to create workflow: {str(e)}")
        raise

def create_workflow_triggers(workflow_name):
    """Create triggers for the workflow"""
    
    # Schedule trigger - daily at 2 AM
    schedule_trigger = {
        'Name': 'daily-sales-schedule',
        'Type': 'SCHEDULED',
        'Schedule': 'cron(0 2 * * ? *)',  # Daily at 2 AM UTC
        'StartOnCreation': True,
        'Description': 'Daily trigger for sales data processing'
    }
    
    # On-demand trigger
    ondemand_trigger = {
        'Name': 'manual-sales-trigger',
        'Type': 'ON_DEMAND',
        'Description': 'Manual trigger for sales data processing'
    }
    
    # Event-based trigger (when new data arrives)
    event_trigger = {
        'Name': 'new-data-trigger',
        'Type': 'EVENT',
        'Description': 'Trigger when new sales data is available',
        'EventBatchingCondition': {
            'BatchSize': 1,
            'BatchWindow': 300  # 5 minutes
        }
    }
    
    triggers = [schedule_trigger, ondemand_trigger, event_trigger]
    
    for trigger in triggers:
        try:
            glue_client.create_trigger(
                Name=trigger['Name'],
                Type=trigger['Type'],
                Schedule=trigger.get('Schedule'),
                StartOnCreation=trigger.get('StartOnCreation', False),
                Description=trigger['Description'],
                Actions=[
                    {
                        'JobName': 'start-workflow-job',
                        'Arguments': {
                            '--workflow_name': workflow_name,
                            '--trigger_type': trigger['Type']
                        }
                    }
                ],
                EventBatchingCondition=trigger.get('EventBatchingCondition'),
                WorkflowName=workflow_name
            )
            
            print(f"Trigger created: {trigger['Name']}")
            
        except Exception as e:
            print(f"Failed to create trigger {trigger['Name']}: {str(e)}")

def create_workflow_jobs(workflow_name):
    """Create Glue jobs for the workflow"""
    
    # Job 1: Data Discovery and Cataloging
    discovery_job = {
        'Name': 'sales-data-discovery',
        'Description': 'Discover and catalog sales data schema',
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': 's3://glue-scripts/discover_sales_schema.py',
            'PythonVersion': '3'
        },
        'DefaultArguments': {
            '--TempDir': 's3://glue-temp-bucket/discovery/',
            '--job-bookmark-option': 'job-bookmark-enable',
            '--enable-metrics': 'true',
            '--enable-spark-ui': 'true',
            '--spark-event-logs-path': 's3://glue-logs/spark-ui/',
            '--input_database': 'raw_sales_data',
            '--output_database': 'processed_sales_data'
        },
        'GlueVersion': '4.0',
        'NumberOfWorkers': 2,
        'WorkerType': 'G.1X',
        'Timeout': 60,
        'MaxRetries': 2
    }
    
    # Job 2: Data Transformation
    transform_job = {
        'Name': 'sales-data-transformation',
        'Description': 'Transform raw sales data to analytics format',
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': 's3://glue-scripts/transform_sales_data.py',
            'PythonVersion': '3'
        },
        'DefaultArguments': {
            '--TempDir': 's3://glue-temp-bucket/transform/',
            '--job-bookmark-option': 'job-bookmark-enable',
            '--enable-continuous-cloudwatch-log': 'true',
            '--input_path': 's3://raw-sales-data/',
            '--output_path': 's3://processed-sales-data/',
            '--transformation_rules': 's3://config/transformation_rules.json'
        },
        'GlueVersion': '4.0',
        'NumberOfWorkers': 5,
        'WorkerType': 'G.1X',
        'Timeout': 180,
        'MaxRetries': 1
    }
    
    # Job 3: Data Quality Validation
    quality_job = {
        'Name': 'sales-data-quality-check',
        'Description': 'Validate processed sales data quality',
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': 's3://glue-scripts/validate_data_quality.py',
            'PythonVersion': '3'
        },
        'DefaultArguments': {
            '--TempDir': 's3://glue-temp-bucket/quality/',
            '--enable-metrics': 'true',
            '--quality_rules': 's3://config/data_quality_rules.json',
            '--notification_topic': 'arn:aws:sns:us-west-2:123456789012:data-quality-alerts'
        },
        'GlueVersion': '4.0',
        'NumberOfWorkers': 2,
        'WorkerType': 'G.1X',
        'Timeout': 60,
        'MaxRetries': 0  # Don't retry quality checks
    }
    
    # Job 4: Load to Data Warehouse
    load_job = {
        'Name': 'sales-data-warehouse-load',
        'Description': 'Load processed data to Redshift',
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': 's3://glue-scripts/load_to_redshift.py',
            'PythonVersion': '3'
        },
        'DefaultArguments': {
            '--TempDir': 's3://glue-temp-bucket/load/',
            '--job-bookmark-option': 'job-bookmark-enable',
            '--redshift_connection': 'redshift-sales-connection',
            '--target_schema': 'sales_analytics',
            '--load_mode': 'append'
        },
        'GlueVersion': '4.0',
        'NumberOfWorkers': 3,
        'WorkerType': 'G.1X',
        'Timeout': 120,
        'MaxRetries': 2,
        'Connections': {
            'Connections': ['redshift-sales-connection']
        }
    }
    
    jobs = [discovery_job, transform_job, quality_job, load_job]
    
    for job in jobs:
        try:
            glue_client.create_job(**job)
            print(f"Job created: {job['Name']}")
            
        except Exception as e:
            print(f"Failed to create job {job['Name']}: {str(e)}")

def create_workflow_crawlers():
    """Create crawlers for the workflow"""
    
    # Source data crawler
    source_crawler = {
        'Name': 'sales-source-crawler',
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'DatabaseName': 'raw_sales_data',
        'Description': 'Crawl raw sales data in S3',
        'Targets': {
            'S3Targets': [
                {
                    'Path': 's3://raw-sales-data/',
                    'Exclusions': ['**/_SUCCESS', '**/.checkpoint/**']
                }
            ]
        },
        'SchemaChangePolicy': {
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'LOG'
        },
        'Configuration': json.dumps({
            'Version': 1.0,
            'CrawlerOutput': {
                'Partitions': {'AddOrUpdateBehavior': 'InheritFromTable'},
                'Tables': {'AddOrUpdateBehavior': 'MergeNewColumns'}
            }
        })
    }
    
    # Processed data crawler
    target_crawler = {
        'Name': 'sales-processed-crawler',
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'DatabaseName': 'processed_sales_data',
        'Description': 'Crawl processed sales data',
        'Targets': {
            'S3Targets': [
                {
                    'Path': 's3://processed-sales-data/',
                    'Exclusions': ['**/_SUCCESS', '**/temp/**']
                }
            ]
        },
        'SchemaChangePolicy': {
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        }
    }
    
    crawlers = [source_crawler, target_crawler]
    
    for crawler in crawlers:
        try:
            glue_client.create_crawler(**crawler)
            print(f"Crawler created: {crawler['Name']}")
            
        except Exception as e:
            print(f"Failed to create crawler {crawler['Name']}: {str(e)}")

def start_workflow_run(workflow_name):
    """Start a workflow run"""
    
    run_properties = {
        'run_id': f"run-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        'processing_date': datetime.now().strftime('%Y-%m-%d'),
        'initiated_by': 'scheduled-trigger'
    }
    
    try:
        response = glue_client.start_workflow_run(
            Name=workflow_name,
            RunProperties=run_properties
        )
        
        run_id = response['RunId']
        print(f"Workflow run started: {run_id}")
        return run_id
        
    except Exception as e:
        print(f"Failed to start workflow run: {str(e)}")
        raise

def monitor_workflow_run(workflow_name, run_id):
    """Monitor workflow run progress"""
    
    while True:
        try:
            response = glue_client.get_workflow_run(
                Name=workflow_name,
                RunId=run_id,
                IncludeGraph=True
            )
            
            run = response['Run']
            status = run['Status']
            
            print(f"Workflow run status: {status}")
            
            # Print job statuses
            if 'Graph' in run:
                nodes = run['Graph']['Nodes']
                for node in nodes:
                    if node['Type'] == 'JOB':
                        job_name = node['Name']
                        job_runs = node.get('JobDetails', {}).get('JobRuns', [])
                        if job_runs:
                            latest_run = job_runs[0]
                            print(f"  Job {job_name}: {latest_run['JobRunState']}")
                    elif node['Type'] == 'CRAWLER':
                        crawler_name = node['Name']
                        crawler_details = node.get('CrawlerDetails', {})
                        if crawler_details:
                            print(f"  Crawler {crawler_name}: {crawler_details.get('LastCrawl', {}).get('Status', 'UNKNOWN')}")
            
            if status in ['COMPLETED', 'STOPPED', 'ERROR']:
                break
                
            time.sleep(30)  # Check every 30 seconds
            
        except Exception as e:
            print(f"Error monitoring workflow: {str(e)}")
            break
    
    return status

# Main execution
def main():
    """Main function to create and run the workflow"""
    
    print("🚀 Creating AWS Glue Workflow")
    print("=" * 50)
    
    # Create workflow components
    workflow_name = create_glue_workflow()
    create_workflow_triggers(workflow_name)
    create_workflow_jobs(workflow_name)
    create_workflow_crawlers()
    
    print(f"\n✅ Workflow '{workflow_name}' created successfully!")
    
    # Start a workflow run
    print("\n🔄 Starting workflow run...")
    run_id = start_workflow_run(workflow_name)
    
    # Monitor the run
    print(f"\n📊 Monitoring workflow run {run_id}...")
    final_status = monitor_workflow_run(workflow_name, run_id)
    
    if final_status == 'COMPLETED':
        print("\n🎉 Workflow completed successfully!")
    else:
        print(f"\n❌ Workflow ended with status: {final_status}")

if __name__ == "__main__":
    main()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def glue_workflow_tab():
    """Content for AWS Glue Workflow tab"""
    st.markdown("## 🎯 AWS Glue Workflow")
    st.markdown("*Create and visualize complex ETL workflows with built-in monitoring*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Glue Workflow Benefits
    AWS Glue Workflows provide native orchestration for Glue jobs, crawlers, and triggers:
    - **Visual Design**: Create workflows through console or programmatically
    - **Event-Driven**: Support for schedule, on-demand, and event-based triggers
    - **Dependency Management**: Automatic handling of job dependencies and failures
    - **Monitoring Integration**: Built-in CloudWatch metrics and logging
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Workflow diagram
    st.markdown("#### 🔄 Glue Workflow Architecture")
    common.mermaid(create_glue_workflow_diagram(), height=800)
    
    # Interactive workflow builder
    st.markdown("#### 🎨 Interactive Workflow Designer")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("##### Workflow Configuration")
        trigger_type = st.selectbox("Trigger Type", [
            "Schedule (Cron)",
            "On-Demand",
            "Event-Based (S3)",
            "Conditional"
        ])
        
        if trigger_type == "Schedule (Cron)":
            schedule = st.selectbox("Schedule", [
                "Every hour",
                "Daily at 2 AM",
                "Weekly on Sunday",
                "Monthly on 1st"
            ])
        
        job_count = st.slider("Number of ETL Jobs", 1, 10, 3)
        crawler_count = st.slider("Number of Crawlers", 0, 5, 2)
        
        error_handling = st.selectbox("Error Handling", [
            "Stop on First Failure",
            "Continue on Non-Critical Failures",
            "Retry Failed Jobs",
            "Send Alerts Only"
        ])
    
    with col2:
        # Generate workflow configuration
        workflow_config = generate_workflow_config(trigger_type, job_count, crawler_count, error_handling)
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 🎯 Generated Workflow Configuration
        **Workflow Name**: {workflow_config['name']}  
        **Trigger**: {workflow_config['trigger']}  
        **Jobs**: {workflow_config['job_count']} ETL jobs  
        **Crawlers**: {workflow_config['crawler_count']} crawlers  
        **Estimated Runtime**: {workflow_config['estimated_runtime']}  
        **Cost Estimate**: {workflow_config['cost_estimate']}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Workflow execution timeline
        timeline_data = create_workflow_timeline(workflow_config)
        fig = px.timeline(
            timeline_data,
            x_start="Start",
            x_end="Finish", 
            y="Resource",
            color="Type",
            title="Workflow Execution Timeline",
            color_discrete_map={
                'Crawler': '#3FB34F',
                'ETL Job': '#FF9900',
                'Validation': '#4B9EDB'
            }
        )
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    # Workflow components
    st.markdown("#### ⚙️ Workflow Components")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="workflow-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🕷️ Crawlers
        **Discover & Catalog Data**
        
        - Schema detection
        - Partition discovery  
        - Data catalog updates
        - Change detection
        
        **Triggers:**
        - Schedule-based
        - Event-driven
        - On-demand
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="workflow-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 ETL Jobs
        **Transform & Process Data**
        
        - PySpark/Scala code
        - Built-in transformations
        - Custom business logic
        - Job bookmarking
        
        **Features:**
        - Auto-scaling workers
        - Spot instance support
        - Checkpoint recovery
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="workflow-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚡ Triggers
        **Workflow Orchestration**
        
        - Schedule triggers
        - On-demand execution
        - Event-based triggers
        - Conditional logic
        
        **Capabilities:**
        - Multiple actions
        - Batch conditions
        - Error handling
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Real-time workflow monitoring
    st.markdown("#### 📊 Workflow Monitoring Dashboard")
    
    # Simulate real-time workflow status
    if st.button("🔄 Refresh Workflow Status"):
        st.session_state.workflow_status = generate_workflow_status()
    
    if 'workflow_status' not in st.session_state:
        st.session_state.workflow_status = generate_workflow_status()
    
    status = st.session_state.workflow_status
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 🎯 Active Workflows
        # {status['active_workflows']}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Completed Today
        # {status['completed_today']}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ⚠️ Failed Jobs
        # {status['failed_jobs']}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 💰 Daily Cost
        # ${status['daily_cost']:.2f}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Workflow status table
    st.markdown("##### Current Workflow Status")
    workflow_df = pd.DataFrame(status['workflows'])
    st.dataframe(workflow_df, use_container_width=True)
    
    # Code examples
    st.markdown("#### 💻 Glue Workflow Implementation")
    
    tab1, tab2, tab3 = st.tabs(["Workflow Creation", "Job Dependencies", "Monitoring & Alerts"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Create AWS Glue Workflow with Python SDK
import boto3
import json
from datetime import datetime

def create_comprehensive_workflow():
    """Create a complete Glue workflow with jobs, crawlers, and triggers"""
    
    glue = boto3.client('glue')
    
    # Step 1: Create the workflow
    workflow_response = glue.create_workflow(
        Name='data-lake-etl-workflow',
        Description='Comprehensive data lake ETL pipeline',
        DefaultRunProperties={
            'glue.workflow.notification.topic': 'arn:aws:sns:us-west-2:123456789012:etl-alerts',
            'glue.workflow.max.concurrent.runs': '2',
            'processing.environment': 'production'
        },
        MaxConcurrentRuns=2,
        Tags={
            'Environment': 'Production',
            'Team': 'DataEngineering',
            'CostCenter': 'Analytics'
        }
    )
    
    print("✅ Workflow created successfully")
    
    # Step 2: Create crawlers
    crawlers = [
        {
            'Name': 'raw-data-discovery-crawler',
            'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
            'DatabaseName': 'raw_data_catalog',
            'Description': 'Discover raw data schema and partitions',
            'Targets': {
                'S3Targets': [
                    {
                        'Path': 's3://data-lake-raw/sales/',
                        'Exclusions': ['**/_SUCCESS', '**/$folder$']
                    },
                    {
                        'Path': 's3://data-lake-raw/customers/',
                        'SampleSize': 100
                    }
                ]
            },
            'Schedule': 'cron(0 1 * * ? *)',  # Daily at 1 AM
            'SchemaChangePolicy': {
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'LOG'
            },
            'RecrawlPolicy': {
                'RecrawlBehavior': 'CRAWL_EVERYTHING'
            },
            'LineageConfiguration': {
                'CrawlerLineageSettings': 'ENABLE'
            }
        },
        {
            'Name': 'processed-data-catalog-crawler',
            'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
            'DatabaseName': 'processed_data_catalog',
            'Description': 'Update catalog for processed data',
            'Targets': {
                'S3Targets': [
                    {
                        'Path': 's3://data-lake-processed/gold/',
                        'Exclusions': ['**/.checkpoint/**']
                    }
                ]
            },
            'SchemaChangePolicy': {
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }
        }
    ]
    
    for crawler in crawlers:
        glue.create_crawler(**crawler)
        print(f"✅ Crawler created: {crawler['Name']}")
    
    # Step 3: Create ETL jobs
    jobs = [
        {
            'Name': 'bronze-layer-ingestion',
            'Description': 'Ingest raw data to bronze layer',
            'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': 's3://glue-scripts/bronze_layer_ingestion.py',
                'PythonVersion': '3'
            },
            'DefaultArguments': {
                '--TempDir': 's3://glue-temp/bronze/',
                '--job-bookmark-option': 'job-bookmark-enable',
                '--enable-metrics': 'true',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-spark-ui': 'true',
                '--spark-event-logs-path': 's3://glue-logs/spark-ui/bronze/',
                '--raw_data_path': 's3://data-lake-raw/',
                '--bronze_output_path': 's3://data-lake-processed/bronze/',
                '--data_catalog_database': 'raw_data_catalog'
            },
            'ExecutionProperty': {
                'MaxConcurrentRuns': 2
            },
            'GlueVersion': '4.0',
            'NumberOfWorkers': 3,
            'WorkerType': 'G.1X',
            'Timeout': 120,
            'MaxRetries': 1,
            'SecurityConfiguration': 'glue-security-config'
        },
        {
            'Name': 'silver-layer-transformation', 
            'Description': 'Transform bronze data to silver layer',
            'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': 's3://glue-scripts/silver_layer_transformation.py',
                'PythonVersion': '3'
            },
            'DefaultArguments': {
                '--TempDir': 's3://glue-temp/silver/',
                '--job-bookmark-option': 'job-bookmark-enable',
                '--enable-metrics': 'true',
                '--bronze_input_path': 's3://data-lake-processed/bronze/',
                '--silver_output_path': 's3://data-lake-processed/silver/',
                '--transformation_config': 's3://glue-config/silver_transformations.json',
                '--data_quality_rules': 's3://glue-config/quality_rules.json'
            },
            'GlueVersion': '4.0',
            'NumberOfWorkers': 5,
            'WorkerType': 'G.1X',
            'Timeout': 180,
            'MaxRetries': 2
        },
        {
            'Name': 'gold-layer-aggregation',
            'Description': 'Create business-ready aggregations',
            'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': 's3://glue-scripts/gold_layer_aggregation.py',
                'PythonVersion': '3'
            },
            'DefaultArguments': {
                '--TempDir': 's3://glue-temp/gold/',
                '--job-bookmark-option': 'job-bookmark-enable',
                '--silver_input_path': 's3://data-lake-processed/silver/',
                '--gold_output_path': 's3://data-lake-processed/gold/',
                '--aggregation_config': 's3://glue-config/aggregations.json'
            },
            'GlueVersion': '4.0',
            'NumberOfWorkers': 2,
            'WorkerType': 'G.1X',
            'Timeout': 90,
            'MaxRetries': 1
        },
        {
            'Name': 'data-quality-validation',
            'Description': 'Validate data quality across all layers',
            'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': 's3://glue-scripts/data_quality_validation.py',
                'PythonVersion': '3'
            },
            'DefaultArguments': {
                '--TempDir': 's3://glue-temp/quality/',
                '--quality_database': 'data_quality_metrics',
                '--alert_topic': 'arn:aws:sns:us-west-2:123456789012:data-quality-alerts',
                '--quality_thresholds': 's3://glue-config/quality_thresholds.json'
            },
            'GlueVersion': '4.0',
            'NumberOfWorkers': 2,
            'WorkerType': 'G.1X',
            'Timeout': 60,
            'MaxRetries': 0  # Don't retry quality validation
        }
    ]
    
    for job in jobs:
        glue.create_job(**job)
        print(f"✅ Job created: {job['Name']}")
    
    # Step 4: Create triggers to orchestrate the workflow
    triggers = [
        {
            'Name': 'start-workflow-trigger',
            'Type': 'SCHEDULED',
            'Schedule': 'cron(0 2 * * ? *)',  # Daily at 2 AM
            'StartOnCreation': True,
            'Description': 'Start daily ETL workflow',
            'Actions': [
                {
                    'CrawlerName': 'raw-data-discovery-crawler'
                }
            ],
            'WorkflowName': 'data-lake-etl-workflow'
        },
        {
            'Name': 'bronze-ingestion-trigger',
            'Type': 'CONDITIONAL',
            'Description': 'Start bronze layer after crawler completes',
            'Actions': [
                {
                    'JobName': 'bronze-layer-ingestion'
                }
            ],
            'Predicate': {
                'Logical': 'AND',
                'Conditions': [
                    {
                        'LogicalOperator': 'EQUALS',
                        'CrawlerName': 'raw-data-discovery-crawler',
                        'CrawlState': 'SUCCEEDED'
                    }
                ]
            },
            'WorkflowName': 'data-lake-etl-workflow'
        },
        {
            'Name': 'silver-transformation-trigger',
            'Type': 'CONDITIONAL',
            'Description': 'Start silver layer after bronze completes',
            'Actions': [
                {
                    'JobName': 'silver-layer-transformation'
                }
            ],
            'Predicate': {
                'Logical': 'AND',
                'Conditions': [
                    {
                        'LogicalOperator': 'EQUALS',
                        'JobName': 'bronze-layer-ingestion',
                        'State': 'SUCCEEDED'
                    }
                ]
            },
            'WorkflowName': 'data-lake-etl-workflow'
        },
        {
            'Name': 'parallel-final-processing',
            'Type': 'CONDITIONAL',
            'Description': 'Run gold aggregation and quality validation in parallel',
            'Actions': [
                {
                    'JobName': 'gold-layer-aggregation'
                },
                {
                    'JobName': 'data-quality-validation'
                }
            ],
            'Predicate': {
                'Logical': 'AND',
                'Conditions': [
                    {
                        'LogicalOperator': 'EQUALS',
                        'JobName': 'silver-layer-transformation',
                        'State': 'SUCCEEDED'
                    }
                ]
            },
            'WorkflowName': 'data-lake-etl-workflow'
        },
        {
            'Name': 'final-cataloging-trigger',
            'Type': 'CONDITIONAL',
            'Description': 'Update processed data catalog after completion',
            'Actions': [
                {
                    'CrawlerName': 'processed-data-catalog-crawler'
                }
            ],
            'Predicate': {
                'Logical': 'AND',
                'Conditions': [
                    {
                        'LogicalOperator': 'EQUALS',
                        'JobName': 'gold-layer-aggregation',
                        'State': 'SUCCEEDED'
                    },
                    {
                        'LogicalOperator': 'EQUALS',
                        'JobName': 'data-quality-validation',
                        'State': 'SUCCEEDED'
                    }
                ]
            },
            'WorkflowName': 'data-lake-etl-workflow'
        }
    ]
    
    for trigger in triggers:
        glue.create_trigger(**trigger)
        print(f"✅ Trigger created: {trigger['Name']}")
    
    print(f"\n🎉 Complete workflow 'data-lake-etl-workflow' created successfully!")
    print("The workflow includes:")
    print("  - 2 Crawlers for schema discovery")
    print("  - 4 ETL Jobs for data processing")
    print("  - 5 Triggers for orchestration")
    print("  - Automatic error handling and notifications")

# Execute workflow creation
if __name__ == "__main__":
    create_comprehensive_workflow()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Advanced Job Dependencies and Error Handling
import boto3
import json

def create_complex_dependency_workflow():
    """Create workflow with complex job dependencies and error handling"""
    
    glue = boto3.client('glue')
    
    # Define complex trigger with multiple conditions
    complex_trigger = {
        'Name': 'multi-condition-trigger',
        'Type': 'CONDITIONAL',
        'Description': 'Complex conditional logic for job dependencies',
        'Actions': [
            {
                'JobName': 'customer-data-processing',
                'Arguments': {
                    '--dependency_check': 'true',
                    '--max_processing_time': '120'
                }
            }
        ],
        'Predicate': {
            'Logical': 'AND',
            'Conditions': [
                # Multiple job success conditions
                {
                    'LogicalOperator': 'EQUALS',
                    'JobName': 'sales-data-ingestion',
                    'State': 'SUCCEEDED'
                },
                {
                    'LogicalOperator': 'EQUALS',
                    'JobName': 'inventory-data-sync',
                    'State': 'SUCCEEDED'
                },
                # Crawler completion condition
                {
                    'LogicalOperator': 'EQUALS',
                    'CrawlerName': 'product-catalog-crawler',
                    'CrawlState': 'SUCCEEDED'
                }
            ]
        },
        'WorkflowName': 'complex-etl-workflow'
    }
    
    # OR condition trigger (any job succeeds)
    or_condition_trigger = {
        'Name': 'alternative-path-trigger',
        'Type': 'CONDITIONAL',
        'Description': 'Execute if any data source is available',
        'Actions': [
            {
                'JobName': 'fallback-data-processing'
            }
        ],
        'Predicate': {
            'Logical': 'OR',
            'Conditions': [
                {
                    'LogicalOperator': 'EQUALS',
                    'JobName': 'primary-data-source',
                    'State': 'SUCCEEDED'
                },
                {
                    'LogicalOperator': 'EQUALS',
                    'JobName': 'secondary-data-source',
                    'State': 'SUCCEEDED'
                },
                {
                    'LogicalOperator': 'EQUALS',
                    'JobName': 'tertiary-data-source',
                    'State': 'SUCCEEDED'
                }
            ]
        },
        'WorkflowName': 'complex-etl-workflow'
    }
    
    # Error handling trigger (executes on failure)
    error_handling_trigger = {
        'Name': 'error-recovery-trigger',
        'Type': 'CONDITIONAL',
        'Description': 'Handle failed jobs and attempt recovery',
        'Actions': [
            {
                'JobName': 'error-analysis-job',
                'Arguments': {
                    '--failed_job_name': '${glue:context:failed_job}',
                    '--error_analysis_mode': 'detailed'
                }
            },
            {
                'JobName': 'data-cleanup-job'
            }
        ],
        'Predicate': {
            'Logical': 'OR',
            'Conditions': [
                {
                    'LogicalOperator': 'EQUALS',
                    'JobName': 'critical-etl-job',
                    'State': 'FAILED'
                },
                {
                    'LogicalOperator': 'EQUALS',
                    'JobName': 'data-validation-job',
                    'State': 'FAILED'
                }
            ]
        },
        'WorkflowName': 'complex-etl-workflow'
    }
    
    triggers = [complex_trigger, or_condition_trigger, error_handling_trigger]
    
    for trigger in triggers:
        try:
            glue.create_trigger(**trigger)
            print(f"✅ Created trigger: {trigger['Name']}")
        except Exception as e:
            print(f"❌ Failed to create trigger {trigger['Name']}: {str(e)}")

def create_conditional_branching_workflow():
    """Create workflow with conditional branching based on data conditions"""
    
    glue = boto3.client('glue')
    
    # Data volume checker job
    data_checker_job = {
        'Name': 'data-volume-checker',
        'Description': 'Check data volume to determine processing strategy',
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'Command': {
            'Name': 'pythonshell',
            'ScriptLocation': 's3://glue-scripts/data_volume_checker.py',
            'PythonVersion': '3.9'
        },
        'DefaultArguments': {
            '--input_path': 's3://raw-data/',
            '--volume_threshold_gb': '100',
            '--complexity_threshold': '0.8'
        },
        'MaxCapacity': 1,
        'Timeout': 30,
        'MaxRetries': 1
    }
    
    # Small data processing job
    small_data_job = {
        'Name': 'small-data-processing',
        'Description': 'Optimized processing for small datasets',
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': 's3://glue-scripts/small_data_processing.py'
        },
        'GlueVersion': '4.0',
        'NumberOfWorkers': 2,
        'WorkerType': 'G.1X',
        'DefaultArguments': {
            '--processing_mode': 'single_node',
            '--optimization_level': 'memory'
        }
    }
    
    # Large data processing job
    large_data_job = {
        'Name': 'large-data-processing',
        'Description': 'Scaled processing for large datasets',
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': 's3://glue-scripts/large_data_processing.py'
        },
        'GlueVersion': '4.0',
        'NumberOfWorkers': 10,
        'WorkerType': 'G.2X',
        'DefaultArguments': {
            '--processing_mode': 'cluster',
            '--optimization_level': 'compute',
            '--enable_auto_scaling': 'true'
        }
    }
    
    jobs = [data_checker_job, small_data_job, large_data_job]
    
    for job in jobs:
        glue.create_job(**job)
        print(f"✅ Created job: {job['Name']}")
    
    # Conditional triggers based on data volume
    small_data_trigger = {
        'Name': 'small-data-trigger',
        'Type': 'CONDITIONAL',
        'Description': 'Process small datasets efficiently',
        'Actions': [
            {
                'JobName': 'small-data-processing'
            }
        ],
        'Predicate': {
            'Logical': 'AND',
            'Conditions': [
                {
                    'LogicalOperator': 'EQUALS',
                    'JobName': 'data-volume-checker',
                    'State': 'SUCCEEDED'
                }
                # In real implementation, you'd check job output for volume decision
            ]
        },
        'WorkflowName': 'adaptive-processing-workflow'
    }
    
    large_data_trigger = {
        'Name': 'large-data-trigger',
        'Type': 'CONDITIONAL',
        'Description': 'Process large datasets with scaling',
        'Actions': [
            {
                'JobName': 'large-data-processing'
            }
        ],
        'Predicate': {
            'Logical': 'AND',
            'Conditions': [
                {
                    'LogicalOperator': 'EQUALS',
                    'JobName': 'data-volume-checker',
                    'State': 'SUCCEEDED'
                }
                # In real implementation, you'd check job output for volume decision
            ]
        },
        'WorkflowName': 'adaptive-processing-workflow'
    }
    
    triggers = [small_data_trigger, large_data_trigger]
    
    for trigger in triggers:
        glue.create_trigger(**trigger)
        print(f"✅ Created trigger: {trigger['Name']}")

def monitor_workflow_dependencies():
    """Monitor and analyze workflow dependencies"""
    
    glue = boto3.client('glue')
    
    # Get workflow runs
    workflow_runs = glue.get_workflow_runs(
        Name='complex-etl-workflow',
        IncludeGraph=True,
        MaxResults=10
    )
    
    print("📊 Workflow Dependency Analysis")
    print("=" * 50)
    
    for run in workflow_runs['Runs']:
        run_id = run['RunId']
        status = run['Status']
        
        print(f"\n🔄 Run {run_id}: {status}")
        
        if 'Graph' in run:
            nodes = run['Graph']['Nodes']
            edges = run['Graph']['Edges']
            
            # Analyze node states
            job_states = {}
            crawler_states = {}
            
            for node in nodes:
                if node['Type'] == 'JOB':
                    name = node['Name']
                    if 'JobDetails' in node and node['JobDetails']['JobRuns']:
                        state = node['JobDetails']['JobRuns'][0]['JobRunState']
                        job_states[name] = state
                elif node['Type'] == 'CRAWLER':
                    name = node['Name']
                    if 'CrawlerDetails' in node:
                        last_crawl = node['CrawlerDetails'].get('LastCrawl', {})
                        state = last_crawl.get('Status', 'UNKNOWN')
                        crawler_states[name] = state
            
            # Print dependency chain
            print("  Job Dependencies:")
            for job, state in job_states.items():
                print(f"    {job}: {state}")
            
            print("  Crawler Status:")
            for crawler, state in crawler_states.items():
                print(f"    {crawler}: {state}")
            
            # Analyze bottlenecks
            failed_jobs = [job for job, state in job_states.items() if state == 'FAILED']
            if failed_jobs:
                print(f"  ⚠️  Failed Jobs: {', '.join(failed_jobs)}")
            
            running_jobs = [job for job, state in job_states.items() if state == 'RUNNING']
            if running_jobs:
                print(f"  🔄 Currently Running: {', '.join(running_jobs)}")

# Sample data volume checker script content
data_volume_checker_script = """
import sys
import boto3
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, [
    'input_path',
    'volume_threshold_gb', 
    'complexity_threshold'
])

s3 = boto3.client('s3')

def check_data_volume(input_path):
    """Check data volume and complexity"""
    
    # Parse S3 path
    bucket = input_path.replace('s3://', '').split('/')[0]
    prefix = '/'.join(input_path.replace('s3://', '').split('/')[1:])
    
    total_size = 0
    file_count = 0
    
    # Get object sizes
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                total_size += obj['Size']
                file_count += 1
    
    # Convert to GB
    size_gb = total_size / (1024**3)
    
    # Determine processing strategy
    volume_threshold = float(args['volume_threshold_gb'])
    
    processing_strategy = {
        'size_gb': size_gb,
        'file_count': file_count,
        'use_large_processing': size_gb > volume_threshold,
        'recommended_workers': min(max(2, int(size_gb / 10)), 20)
    }
    
    # Write decision to S3 for triggers to read
    decision_key = 'workflow-decisions/processing-strategy.json'
    
    s3.put_object(
        Bucket=bucket,
        Key=decision_key,
        Body=json.dumps(processing_strategy)
    )
    
    print(f"Data volume: {size_gb:.2f} GB")
    print(f"File count: {file_count}")
    print(f"Processing strategy: {'Large' if processing_strategy['use_large_processing'] else 'Small'}")
    
    return processing_strategy

# Execute check
result = check_data_volume(args['input_path'])
"""

print("📋 Workflow dependency examples created successfully!")
print("Key features demonstrated:")
print("  - Complex AND/OR conditional logic")
print("  - Error handling and recovery")
print("  - Adaptive processing based on data characteristics")
print("  - Comprehensive monitoring and analysis")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Comprehensive Glue Workflow Monitoring and Alerting
import boto3
import json
from datetime import datetime, timedelta

def setup_workflow_monitoring():
    """Set up comprehensive monitoring for Glue workflows"""
    
    cloudwatch = boto3.client('cloudwatch')
    sns = boto3.client('sns')
    glue = boto3.client('glue')
    
    # Create custom metrics for workflow monitoring
    custom_metrics = [
        {
            'MetricName': 'WorkflowRunDuration',
            'Namespace': 'AWS/Glue/Workflows',
            'Dimensions': [
                {'Name': 'WorkflowName', 'Value': 'data-lake-etl-workflow'}
            ]
        },
        {
            'MetricName': 'WorkflowSuccessRate',
            'Namespace': 'AWS/Glue/Workflows',
            'Dimensions': [
                {'Name': 'WorkflowName', 'Value': 'data-lake-etl-workflow'}
            ]
        },
        {
            'MetricName': 'DataProcessingVolume',
            'Namespace': 'AWS/Glue/DataLake',
            'Dimensions': [
                {'Name': 'Layer', 'Value': 'Bronze'},
                {'Name': 'Layer', 'Value': 'Silver'},
                {'Name': 'Layer', 'Value': 'Gold'}
            ]
        }
    ]
    
    # Create CloudWatch dashboard
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "x": 0, "y": 0, "width": 12, "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", "bronze-layer-ingestion"],
                        ["AWS/Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", "silver-layer-transformation"],
                        ["AWS/Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", "gold-layer-aggregation"]
                    ],
                    "period": 300,
                    "stat": "Sum",
                    "region": "us-west-2",
                    "title": "Glue Job Task Completion",
                    "yAxis": {"left": {"min": 0}}
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 0, "width": 12, "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/Glue", "glue.driver.BlockManager.disk.diskSpaceUsed_MB", "JobName", "bronze-layer-ingestion"],
                        ["AWS/Glue", "glue.driver.BlockManager.memory.memUsed_MB", "JobName", "bronze-layer-ingestion"]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-west-2",
                    "title": "Resource Utilization"
                }
            },
            {
                "type": "log",
                "x": 0, "y": 6, "width": 24, "height": 6,
                "properties": {
                    "query": "SOURCE '/aws/glue/jobs/logs-v2'\n| fields @timestamp, @message\n| filter @message like /ERROR/\n| sort @timestamp desc\n| limit 100",
                    "region": "us-west-2",
                    "title": "Recent Errors",
                    "view": "table"
                }
            }
        ]
    }
    
    cloudwatch.put_dashboard(
        DashboardName='GlueWorkflowMonitoring',
        DashboardBody=json.dumps(dashboard_body)
    )
    
    print("✅ CloudWatch dashboard created")

def create_workflow_alarms():
    """Create CloudWatch alarms for workflow monitoring"""
    
    cloudwatch = boto3.client('cloudwatch')
    
    alarms = [
        {
            'AlarmName': 'GlueWorkflow-JobFailure',
            'ComparisonOperator': 'GreaterThanThreshold',
            'EvaluationPeriods': 1,
            'MetricName': 'glue.driver.aggregate.numFailedTasks',
            'Namespace': 'AWS/Glue',
            'Period': 300,
            'Statistic': 'Sum',
            'Threshold': 0,
            'ActionsEnabled': True,
            'AlarmActions': [
                'arn:aws:sns:us-west-2:123456789012:glue-job-failures'
            ],
            'AlarmDescription': 'Alarm when Glue job tasks fail',
            'Dimensions': [
                {
                    'Name': 'JobName',
                    'Value': 'bronze-layer-ingestion'
                }
            ],
            'Unit': 'Count'
        },
        {
            'AlarmName': 'GlueWorkflow-LongRunningJob',
            'ComparisonOperator': 'GreaterThanThreshold',
            'EvaluationPeriods': 2,
            'MetricName': 'glue.driver.aggregate.elapsedTime',
            'Namespace': 'AWS/Glue',
            'Period': 600,
            'Statistic': 'Maximum',
            'Threshold': 3600000,  # 1 hour in milliseconds
            'ActionsEnabled': True,
            'AlarmActions': [
                'arn:aws:sns:us-west-2:123456789012:glue-performance-alerts'
            ],
            'AlarmDescription': 'Alarm when Glue job runs longer than expected',
            'Unit': 'Count'
        },
        {
            'AlarmName': 'GlueWorkflow-HighMemoryUsage',
            'ComparisonOperator': 'GreaterThanThreshold',
            'EvaluationPeriods': 3,
            'MetricName': 'glue.driver.BlockManager.memory.memUsed_MB',
            'Namespace': 'AWS/Glue',
            'Period': 300,
            'Statistic': 'Average',
            'Threshold': 80,
            'ActionsEnabled': True,
            'AlarmActions': [
                'arn:aws:sns:us-west-2:123456789012:glue-resource-alerts'
            ],
            'AlarmDescription': 'Alarm when memory usage is consistently high',
            'Unit': 'Percent'
        }
    ]
    
    for alarm in alarms:
        cloudwatch.put_metric_alarm(**alarm)
        print(f"✅ Alarm created: {alarm['AlarmName']}")

def create_automated_remediation():
    """Create Lambda function for automated remediation"""
    
    lambda_client = boto3.client('lambda')
    
    # Lambda function code for auto-remediation
    lambda_code = """
import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    """Auto-remediation for Glue workflow issues"""
    
    glue = boto3.client('glue')
    sns = boto3.client('sns')
    
    # Parse CloudWatch alarm
    message = json.loads(event['Records'][0]['Sns']['Message'])
    alarm_name = message['AlarmName']
    
    remediation_actions = {
        'GlueWorkflow-JobFailure': handle_job_failure,
        'GlueWorkflow-LongRunningJob': handle_long_running_job,
        'GlueWorkflow-HighMemoryUsage': handle_memory_issues
    }
    
    if alarm_name in remediation_actions:
        result = remediation_actions[alarm_name](message, glue, sns)
        return {
            'statusCode': 200,
            'body': json.dumps(f'Remediation completed: {result}')
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps('No remediation action defined')
    }

def handle_job_failure(alarm, glue, sns):
    """Handle job failure by analyzing and potentially restarting"""
    
    # Get failed job details
    job_name = get_job_name_from_alarm(alarm)
    
    # Get recent job runs
    job_runs = glue.get_job_runs(JobName=job_name, MaxResults=5)
    
    failed_runs = [run for run in job_runs['JobRuns'] if run['JobRunState'] == 'FAILED']
    
    if failed_runs:
        latest_failure = failed_runs[0]
        error_message = latest_failure.get('ErrorMessage', 'Unknown error')
        
        # Analyze error type
        if 'OutOfMemoryError' in error_message:
            # Restart with more memory
            restart_job_with_more_resources(glue, job_name, 'memory')
            return 'Restarted job with increased memory'
        elif 'TimeoutException' in error_message:
            # Restart with longer timeout
            restart_job_with_more_resources(glue, job_name, 'timeout')
            return 'Restarted job with extended timeout'
        else:
            # Send notification for manual intervention
            send_failure_notification(sns, job_name, error_message)
            return 'Manual intervention required'
    
    return 'No recent failures found'

def handle_long_running_job(alarm, glue, sns):
    """Handle long-running jobs"""
    
    job_name = get_job_name_from_alarm(alarm)
    
    # Get current running jobs
    running_jobs = glue.get_job_runs(
        JobName=job_name,
        MaxResults=10
    )
    
    active_runs = [run for run in running_jobs['JobRuns'] 
                   if run['JobRunState'] == 'RUNNING']
    
    for run in active_runs:
        run_id = run['Id']
        start_time = run['StartedOn']
        duration = datetime.now(start_time.tzinfo) - start_time
        
        # If running more than 2 hours, stop and restart
        if duration.total_seconds() > 7200:
            glue.batch_stop_job_run(
                JobName=job_name,
                JobRunIds=[run_id]
            )
            
            # Restart with optimized settings
            restart_job_with_more_resources(glue, job_name, 'optimization')
            
            return f'Stopped long-running job {run_id} and restarted with optimization'
    
    return 'No action needed'

def handle_memory_issues(alarm, glue, sns):
    """Handle high memory usage"""
    
    job_name = get_job_name_from_alarm(alarm)
    
    # Get job configuration
    job_details = glue.get_job(JobName=job_name)
    current_workers = job_details['Job'].get('NumberOfWorkers', 2)
    
    # Scale up workers
    new_worker_count = min(current_workers * 2, 20)
    
    glue.update_job(
        JobName=job_name,
        JobUpdate={
            'NumberOfWorkers': new_worker_count,
            'DefaultArguments': {
                **job_details['Job'].get('DefaultArguments', {}),
                '--conf': 'spark.sql.adaptive.enabled=true'
            }
        }
    )
    
    return f'Scaled workers from {current_workers} to {new_worker_count}'

def restart_job_with_more_resources(glue, job_name, resource_type):
    """Restart job with optimized resources"""
    
    resource_configs = {
        'memory': {
            'WorkerType': 'G.2X',
            'NumberOfWorkers': 5
        },
        'timeout': {
            'Timeout': 480  # 8 hours
        },
        'optimization': {
            'DefaultArguments': {
                '--conf': 'spark.sql.adaptive.enabled=true',
                '--conf': 'spark.sql.adaptive.coalescePartitions.enabled=true'
            }
        }
    }
    
    if resource_type in resource_configs:
        glue.start_job_run(
            JobName=job_name,
            **resource_configs[resource_type]
        )

def get_job_name_from_alarm(alarm):
    """Extract job name from CloudWatch alarm"""
    dimensions = alarm.get('Trigger', {}).get('Dimensions', [])
    for dim in dimensions:
        if dim['name'] == 'JobName':
            return dim['value']
    return 'unknown-job'

def send_failure_notification(sns, job_name, error_message):
    """Send failure notification to operations team"""
    
    message = f"""
    Glue Job Failure Detected
    
    Job Name: {job_name}
    Error: {error_message}
    Time: {datetime.now().isoformat()}
    
    Automatic remediation was not possible.
    Manual intervention required.
    """
    
    sns.publish(
        TopicArn='arn:aws:sns:us-west-2:123456789012:ops-alerts',
        Message=message,
        Subject=f'Glue Job Failure: {job_name}'
    )
    """
    
    # Create Lambda function
    lambda_client.create_function(
        FunctionName='glue-workflow-auto-remediation',
        Runtime='python3.9',
        Role='arn:aws:iam::123456789012:role/LambdaGlueRemediationRole',
        Handler='index.lambda_handler',
        Code={'ZipFile': lambda_code.encode()},
        Description='Automated remediation for Glue workflow issues',
        Timeout=300,
        Environment={
            'Variables': {
                'SNS_TOPIC': 'arn:aws:sns:us-west-2:123456789012:ops-alerts'
            }
        }
    )
    
    print("✅ Auto-remediation Lambda function created")

def generate_workflow_health_report():
    """Generate comprehensive workflow health report"""
    
    glue = boto3.client('glue')
    cloudwatch = boto3.client('cloudwatch')
    
    # Get all workflows
    workflows = glue.list_workflows()
    
    report = {
        'report_date': datetime.now().isoformat(),
        'workflows': []
    }
    
    for workflow_name in workflows['Workflows']:
        # Get workflow runs
        runs = glue.get_workflow_runs(
            Name=workflow_name,
            MaxResults=10
        )
        
        # Calculate success rate
        total_runs = len(runs['Runs'])
        successful_runs = len([r for r in runs['Runs'] if r['Status'] == 'COMPLETED'])
        success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
        
        # Get average runtime
        completed_runs = [r for r in runs['Runs'] if r['Status'] == 'COMPLETED']
        avg_runtime = 0
        if completed_runs:
            runtimes = []
            for run in completed_runs:
                if 'CompletedOn' in run and 'StartedOn' in run:
                    duration = (run['CompletedOn'] - run['StartedOn']).total_seconds()
                    runtimes.append(duration)
            avg_runtime = sum(runtimes) / len(runtimes) if runtimes else 0
        
        workflow_health = {
            'name': workflow_name,
            'total_runs': total_runs,
            'success_rate': success_rate,
            'avg_runtime_minutes': avg_runtime / 60,
            'status': 'Healthy' if success_rate > 95 else 'Needs Attention' if success_rate > 80 else 'Critical'
        }
        
        report['workflows'].append(workflow_health)
    
    # Save report to S3
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='glue-monitoring-reports',
        Key=f"health-reports/workflow-health-{datetime.now().strftime('%Y-%m-%d')}.json",
        Body=json.dumps(report, indent=2)
    )
    
    print("📊 Workflow Health Report Generated")
    print("=" * 50)
    
    for workflow in report['workflows']:
        print(f"Workflow: {workflow['name']}")
        print(f"  Success Rate: {workflow['success_rate']:.1f}%")
        print(f"  Avg Runtime: {workflow['avg_runtime_minutes']:.1f} minutes")
        print(f"  Status: {workflow['status']}")
        print()
    
    return report

# Execute monitoring setup
if __name__ == "__main__":
    print("🚀 Setting up Glue Workflow Monitoring")
    print("=" * 50)
    
    setup_workflow_monitoring()
    create_workflow_alarms()
    create_automated_remediation()
    
    print("\n📊 Generating health report...")
    generate_workflow_health_report()
    
    print("\n✅ Monitoring setup complete!")
    print("Configured components:")
    print("  - CloudWatch Dashboard")
    print("  - Automated Alarms")
    print("  - Auto-remediation Lambda")
    print("  - Health Reporting")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def monitoring_tab():
    """Content for Monitoring a Data Lake tab"""
    st.markdown("## 📊 Monitoring a Data Lake")
    st.markdown("*Implement comprehensive monitoring, logging, and alerting for data lake operations*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Data Lake Monitoring Strategy
    Effective data lake monitoring requires a multi-layered approach:
    - **Infrastructure Monitoring**: Track compute, storage, and network resources
    - **Data Quality Monitoring**: Ensure data accuracy, completeness, and consistency
    - **Pipeline Monitoring**: Monitor ETL jobs, workflows, and data movement
    - **Security Monitoring**: Track access patterns, permissions, and compliance
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Monitoring architecture
    st.markdown("#### 📈 Comprehensive Monitoring Architecture")
    common.mermaid(create_monitoring_architecture(), height=700)
    
    # Interactive monitoring dashboard
    st.markdown("#### 🎛️ Data Lake Monitoring Dashboard")
    
    # Simulate real-time monitoring data
    if st.button("🔄 Refresh Monitoring Data"):
        st.session_state.monitoring_data = generate_monitoring_data()
    
    if 'monitoring_data' not in st.session_state:
        st.session_state.monitoring_data = generate_monitoring_data()
    
    data = st.session_state.monitoring_data
    
    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 📊 Data Processed Today
        # {data['data_processed_gb']:.1f} GB
        +{data['data_growth_percent']:.1f}% vs yesterday
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ⚡ Pipeline Success Rate
        # {data['pipeline_success_rate']:.1f}%
        {data['success_trend']} trend
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 🚨 Active Alerts
        # {data['active_alerts']}
        {data['alert_severity']} priority
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 💰 Daily Cost
        # ${data['daily_cost']:.2f}
        {data['cost_trend']} vs budget
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Monitoring charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Data processing volume over time
        fig1 = px.line(
            data['processing_timeline'],
            x='hour',
            y='volume_gb',
            title='Data Processing Volume (Last 24 Hours)',
            color_discrete_sequence=[AWS_COLORS['primary']]
        )
        fig1.update_layout(height=300)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Pipeline success/failure distribution
        fig2 = px.pie(
            values=data['pipeline_status'].values(),
            names=data['pipeline_status'].keys(),
            title='Pipeline Execution Status (Last 7 Days)',
            color_discrete_sequence=[AWS_COLORS['success'], AWS_COLORS['error'], AWS_COLORS['warning']]
        )
        fig2.update_layout(height=300)
        st.plotly_chart(fig2, use_container_width=True)
    
    # Service health status
    st.markdown("#### 🏥 Service Health Status")
    
    service_health = pd.DataFrame(data['service_health'])
    
    # Color code the status
    def color_status(val):
        color_map = {
            'Healthy': f'background-color: {AWS_COLORS["success"]}; color: white',
            'Warning': f'background-color: {AWS_COLORS["warning"]}; color: black',
            'Critical': f'background-color: {AWS_COLORS["error"]}; color: white'
        }
        return color_map.get(val, '')
    
    styled_df = service_health.style.applymap(color_status, subset=['Status'])
    st.dataframe(styled_df, use_container_width=True)
    
    # Monitoring tools comparison
    st.markdown("#### 🛠️ Monitoring Tools & Services")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="workflow-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Amazon CloudWatch
        **Comprehensive Monitoring**
        
        **Features:**
        - Metrics & Dashboards
        - Log aggregation
        - Alarms & Notifications
        - Custom metrics
        
        **Use Cases:**
        - Resource monitoring
        - Performance tracking
        - Automated alerting
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="workflow-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔍 AWS CloudTrail
        **Audit & Compliance**
        
        **Features:**
        - API call logging
        - Resource access tracking
        - Compliance reporting
        - Security analysis
        
        **Use Cases:**
        - Security monitoring
        - Compliance auditing
        - Root cause analysis
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="workflow-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔬 AWS X-Ray
        **Distributed Tracing**
        
        **Features:**
        - Request tracing
        - Performance analysis
        - Service maps
        - Error analysis
        
        **Use Cases:**
        - Performance debugging
        - Service dependencies
        - Latency analysis
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Alerting configuration
    st.markdown("#### 🚨 Intelligent Alerting Configuration")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("##### Alert Settings")
        alert_type = st.selectbox("Alert Type", [
            "Performance Degradation",
            "Data Quality Issues", 
            "Pipeline Failures",
            "Security Anomalies",
            "Cost Overruns"
        ])
        
        severity = st.selectbox("Severity Level", ["High", "Medium", "Low"])
        threshold = st.slider("Alert Threshold", 0, 100, 80)
        notification_channel = st.selectbox("Notification Channel", [
            "Email", "SMS", "Slack", "PagerDuty", "Lambda Function"
        ])
    
    with col2:
        # Generate alert configuration
        alert_config = generate_alert_config(alert_type, severity, threshold, notification_channel)
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 🎯 Generated Alert Configuration
        **Alert Name**: {alert_config['name']}  
        **Metric**: {alert_config['metric']}  
        **Condition**: {alert_config['condition']}  
        **Threshold**: {alert_config['threshold']}  
        **Action**: {alert_config['action']}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Alert frequency prediction
        frequency_data = predict_alert_frequency(alert_config)
        fig = px.bar(
            x=list(frequency_data.keys()),
            y=list(frequency_data.values()),
            title="Predicted Alert Frequency",
            color_discrete_sequence=[AWS_COLORS['light_blue']]
        )
        fig.update_layout(height=250)
        st.plotly_chart(fig, use_container_width=True)
    
    # Code examples
    st.markdown("#### 💻 Monitoring Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["CloudWatch Setup", "Log Analysis", "Automated Responses"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Comprehensive CloudWatch Monitoring Setup
import boto3
import json
from datetime import datetime, timedelta

def setup_comprehensive_monitoring():
    """Set up comprehensive monitoring for data lake components"""
    
    cloudwatch = boto3.client('cloudwatch')
    logs = boto3.client('logs')
    
    # Create custom namespace for data lake metrics
    namespace = 'DataLake/Operations'
    
    # Data quality metrics
    data_quality_metrics = [
        {
            'MetricName': 'DataCompletenessScore',
            'Dimensions': [
                {'Name': 'Dataset', 'Value': 'sales_data'},
                {'Name': 'Layer', 'Value': 'bronze'}
            ],
            'Unit': 'Percent',
            'Value': 98.5
        },
        {
            'MetricName': 'DataAccuracyScore',
            'Dimensions': [
                {'Name': 'Dataset', 'Value': 'customer_data'},
                {'Name': 'Layer', 'Value': 'silver'}
            ],
            'Unit': 'Percent',
            'Value': 99.2
        },
        {
            'MetricName': 'SchemaComplianceScore',
            'Dimensions': [
                {'Name': 'Dataset', 'Value': 'product_catalog'},
                {'Name': 'Layer', 'Value': 'gold'}
            ],
            'Unit': 'Percent',
            'Value': 100.0
        }
    ]
    
    # Put custom metrics
    for metric in data_quality_metrics:
        cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=[
                {
                    'MetricName': metric['MetricName'],
                    'Dimensions': metric['Dimensions'],
                    'Unit': metric['Unit'],
                    'Value': metric['Value'],
                    'Timestamp': datetime.utcnow()
                }
            ]
        )
    
    print("✅ Custom metrics published")
    
    # Create comprehensive dashboard
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "x": 0, "y": 0, "width": 12, "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", "bronze-ingestion"],
                        ["AWS/Glue", "glue.driver.aggregate.numFailedTasks", "JobName", "bronze-ingestion"],
                        ["AWS/S3", "BucketSizeBytes", "BucketName", "data-lake-bronze", "StorageType", "StandardStorage"],
                        ["AWS/S3", "NumberOfObjects", "BucketName", "data-lake-bronze", "StorageType", "AllStorageTypes"]
                    ],
                    "period": 300,
                    "stat": "Sum",
                    "region": "us-west-2",
                    "title": "Data Lake Core Metrics",
                    "yAxis": {"left": {"min": 0}}
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 0, "width": 12, "height": 6,
                "properties": {
                    "metrics": [
                        [namespace, "DataCompletenessScore", "Dataset", "sales_data"],
                        [namespace, "DataAccuracyScore", "Dataset", "customer_data"],
                        [namespace, "SchemaComplianceScore", "Dataset", "product_catalog"]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-west-2",
                    "title": "Data Quality Scores",
                    "yAxis": {"left": {"min": 90, "max": 100}}
                }
            },
            {
                "type": "metric",
                "x": 0, "y": 6, "width": 8, "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/Lambda", "Duration", "FunctionName", "data-validation-function"],
                        ["AWS/Lambda", "Errors", "FunctionName", "data-validation-function"],
                        ["AWS/Lambda", "Invocations", "FunctionName", "data-validation-function"]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-west-2",
                    "title": "Lambda Function Performance"
                }
            },
            {
                "type": "metric", 
                "x": 8, "y": 6, "width": 8, "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/Redshift", "CPUUtilization", "ClusterIdentifier", "analytics-cluster"],
                        ["AWS/Redshift", "DatabaseConnections", "ClusterIdentifier", "analytics-cluster"],
                        ["AWS/Redshift", "HealthStatus", "ClusterIdentifier", "analytics-cluster"]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-west-2",
                    "title": "Redshift Cluster Health"
                }
            },
            {
                "type": "metric",
                "x": 16, "y": 6, "width": 8, "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/Athena", "DataScannedInBytes", "QueryType", "DDL"],
                        ["AWS/Athena", "QueryExecutionTime", "QueryType", "DML"],
                        ["AWS/Athena", "ProcessedBytes", "WorkGroup", "primary"]
                    ],
                    "period": 300,
                    "stat": "Sum",
                    "region": "us-west-2",
                    "title": "Athena Query Performance"
                }
            },
            {
                "type": "log",
                "x": 0, "y": 12, "width": 24, "height": 6,
                "properties": {
                    "query": "SOURCE '/aws/glue/jobs/logs-v2' | fields @timestamp, @message | filter @message like /ERROR/ or @message like /EXCEPTION/ | sort @timestamp desc | limit 20",
                    "region": "us-west-2",
                    "title": "Recent Errors Across All Services",
                    "view": "table"
                }
            }
        ]
    }
    
    cloudwatch.put_dashboard(
        DashboardName='DataLakeComprehensiveMonitoring',
        DashboardBody=json.dumps(dashboard_body)
    )
    
    print("✅ Comprehensive dashboard created")

def create_intelligent_alarms():
    """Create intelligent CloudWatch alarms with dynamic thresholds"""
    
    cloudwatch = boto3.client('cloudwatch')
    
    # Define alarm configurations with intelligent thresholds
    intelligent_alarms = [
        {
            'AlarmName': 'DataLake-DataQualityDegradation',
            'ComparisonOperator': 'LessThanThreshold',
            'EvaluationPeriods': 2,
            'MetricName': 'DataCompletenessScore',
            'Namespace': 'DataLake/Operations',
            'Period': 900,  # 15 minutes
            'Statistic': 'Average',
            'Threshold': 95.0,
            'ActionsEnabled': True,
            'AlarmActions': [
                'arn:aws:sns:us-west-2:123456789012:data-quality-alerts'
            ],
            'AlarmDescription': 'Data quality score has dropped below acceptable threshold',
            'Dimensions': [
                {'Name': 'Dataset', 'Value': 'sales_data'}
            ],
            'TreatMissingData': 'breaching'
        },
        {
            'AlarmName': 'DataLake-HighIngestionLatency',
            'ComparisonOperator': 'GreaterThanThreshold',
            'EvaluationPeriods': 3,
            'MetricName': 'glue.driver.aggregate.elapsedTime',
            'Namespace': 'AWS/Glue',
            'Period': 300,
            'Statistic': 'Average',
            'Threshold': 1800000,  # 30 minutes in milliseconds
            'ActionsEnabled': True,
            'AlarmActions': [
                'arn:aws:sns:us-west-2:123456789012:performance-alerts',
                'arn:aws:lambda:us-west-2:123456789012:function:auto-scale-glue'
            ],
            'AlarmDescription': 'Data ingestion taking longer than expected',
            'Dimensions': [
                {'Name': 'JobName', 'Value': 'bronze-ingestion'}
            ]
        },
        {
            'AlarmName': 'DataLake-UnusualDataVolume',
            'ComparisonOperator': 'GreaterThanUpperThreshold',
            'EvaluationPeriods': 2,
            'Metrics': [
                {
                    'Id': 'm1',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/S3',
                            'MetricName': 'BucketSizeBytes',
                            'Dimensions': [
                                {'Name': 'BucketName', 'Value': 'data-lake-raw'},
                                {'Name': 'StorageType', 'Value': 'StandardStorage'}
                            ]
                        },
                        'Period': 3600,
                        'Stat': 'Average'
                    }
                },
                {
                    'Id': 'ad1',
                    'Expression': 'ANOMALY_DETECTOR_FUNCTION(m1, 2)'
                }
            ],
            'ThresholdMetricId': 'ad1',
            'ActionsEnabled': True,
            'AlarmActions': [
                'arn:aws:sns:us-west-2:123456789012:anomaly-alerts'
            ],
            'AlarmDescription': 'Unusual data volume detected - possible data quality issue or upstream problem'
        },
        {
            'AlarmName': 'DataLake-HighErrorRate',
            'ComparisonOperator': 'GreaterThanThreshold',
            'EvaluationPeriods': 2,
            'MetricName': 'glue.driver.aggregate.numFailedTasks',
            'Namespace': 'AWS/Glue',
            'Period': 600,
            'Statistic': 'Sum',
            'Threshold': 5,
            'ActionsEnabled': True,
            'AlarmActions': [
                'arn:aws:sns:us-west-2:123456789012:critical-alerts',
                'arn:aws:lambda:us-west-2:123456789012:function:incident-response'
            ],
            'OKActions': [
                'arn:aws:sns:us-west-2:123456789012:recovery-notifications'
            ],
            'AlarmDescription': 'High number of failed tasks in data processing jobs'
        },
        {
            'AlarmName': 'DataLake-CostAnomaly',
            'ComparisonOperator': 'GreaterThanThreshold',
            'EvaluationPeriods': 1,
            'MetricName': 'EstimatedCharges',
            'Namespace': 'AWS/Billing',
            'Period': 86400,  # Daily
            'Statistic': 'Maximum',
            'Threshold': 1000.0,  # $1000 daily threshold
            'ActionsEnabled': True,
            'AlarmActions': [
                'arn:aws:sns:us-west-2:123456789012:cost-alerts'
            ],
            'AlarmDescription': 'Daily AWS costs exceeded budget threshold',
            'Dimensions': [
                {'Name': 'Currency', 'Value': 'USD'}
            ]
        }
    ]
    
    # Create each alarm
    for alarm in intelligent_alarms:
        try:
            if 'Metrics' in alarm:
                # Anomaly detection alarm
                cloudwatch.put_anomaly_alarm(
                    AlarmName=alarm['AlarmName'],
                    ComparisonOperator=alarm['ComparisonOperator'],
                    EvaluationPeriods=alarm['EvaluationPeriods'],
                    Metrics=alarm['Metrics'],
                    ThresholdMetricId=alarm['ThresholdMetricId'],
                    ActionsEnabled=alarm['ActionsEnabled'],
                    AlarmActions=alarm['AlarmActions'],
                    AlarmDescription=alarm['AlarmDescription']
                )
            else:
                # Standard metric alarm
                cloudwatch.put_metric_alarm(**alarm)
            
            print(f"✅ Created alarm: {alarm['AlarmName']}")
            
        except Exception as e:
            print(f"❌ Failed to create alarm {alarm['AlarmName']}: {str(e)}")

def setup_log_insights_queries():
    """Set up saved CloudWatch Logs Insights queries for common investigations"""
    
    logs = boto3.client('logs')
    
    # Saved queries for common troubleshooting scenarios
    saved_queries = [
        {
            'name': 'Glue Job Errors - Last 24 Hours',
            'queryString': """
                fields @timestamp, @message
                | filter @message like /ERROR/ or @message like /Exception/
                | sort @timestamp desc
                | limit 50
            """,
            'logGroups': ['/aws/glue/jobs/logs-v2']
        },
        {
            'name': 'Top Error Messages by Frequency',
            'queryString': """
                fields @timestamp, @message
                | filter @message like /ERROR/
                | stats count() by @message
                | sort count desc
                | limit 10
            """,
            'logGroups': ['/aws/glue/jobs/logs-v2', '/aws/lambda/data-validation-function']
        },
        {
            'name': 'Data Processing Performance Analysis',
            'queryString': """
                fields @timestamp, @message
                | filter @message like /completed/ or @message like /processed/
                | parse @message /processed (?<records>\\d+) records in (?<duration>\\d+) seconds/
                | stats avg(duration), sum(records) by bin(5m)
                | sort @timestamp desc
            """,
            'logGroups': ['/aws/glue/jobs/logs-v2']
        },
        {
            'name': 'Security Access Pattern Analysis',
            'queryString': """
                fields @timestamp, sourceIPAddress, userIdentity.type, eventName
                | filter eventName like /Get/ or eventName like /Put/ or eventName like /Delete/
                | stats count() by sourceIPAddress, userIdentity.type
                | sort count desc
                | limit 20
            """,
            'logGroups': ['/aws/cloudtrail']
        },
        {
            'name': 'Data Quality Validation Results',
            'queryString': """
                fields @timestamp, @message
                | filter @message like /QUALITY_CHECK/
                | parse @message /QUALITY_CHECK: (?<check_type>\\w+) - (?<result>\\w+) - Score: (?<score>\\d+\\.\\d+)/
                | stats avg(score), count() by check_type, result
                | sort avg(score) asc
            """,
            'logGroups': ['/aws/lambda/data-quality-validator']
        }
    ]
    
    # Create saved queries
    for query in saved_queries:
        try:
            response = logs.put_query_definition(
                name=query['name'],
                queryString=query['queryString'],
                logGroupNames=query['logGroups']
            )
            print(f"✅ Created saved query: {query['name']}")
            
        except Exception as e:
            print(f"❌ Failed to create query {query['name']}: {str(e)}")

def create_custom_monitoring_function():
    """Create Lambda function for custom monitoring logic"""
    
    lambda_client = boto3.client('lambda')
    
    # Custom monitoring function code
    monitoring_code = """
import json
import boto3
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """Custom monitoring function for data lake health checks"""
    
    s3 = boto3.client('s3')
    glue = boto3.client('glue')
    cloudwatch = boto3.client('cloudwatch')
    
    monitoring_results = {
        'timestamp': datetime.utcnow().isoformat(),
        'checks': []
    }
    
    # Check 1: Data freshness
    freshness_result = check_data_freshness(s3)
    monitoring_results['checks'].append(freshness_result)
    
    # Check 2: Schema drift detection
    schema_result = check_schema_drift(glue)
    monitoring_results['checks'].append(schema_result)
    
    # Check 3: Processing bottlenecks
    bottleneck_result = check_processing_bottlenecks(glue)
    monitoring_results['checks'].append(bottleneck_result)
    
    # Check 4: Data catalog health
    catalog_result = check_catalog_health(glue)
    monitoring_results['checks'].append(catalog_result)
    
    # Publish custom metrics
    publish_custom_metrics(cloudwatch, monitoring_results)
    
    # Determine overall health status
    failed_checks = [check for check in monitoring_results['checks'] if not check['passed']]
    overall_status = 'HEALTHY' if len(failed_checks) == 0 else 'DEGRADED' if len(failed_checks) < 2 else 'CRITICAL'
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'status': overall_status,
            'failed_checks': len(failed_checks),
            'total_checks': len(monitoring_results['checks']),
            'monitoring_results': monitoring_results
        })
    }

def check_data_freshness(s3):
    """Check if data is being updated regularly"""
    
    buckets = ['data-lake-raw', 'data-lake-bronze', 'data-lake-silver']
    freshness_threshold = timedelta(hours=6)  # Data should be < 6 hours old
    
    for bucket in buckets:
        try:
            response = s3.list_objects_v2(
                Bucket=bucket,
                MaxKeys=10,
                Prefix='daily-data/'
            )
            
            if 'Contents' in response:
                latest_object = max(response['Contents'], key=lambda x: x['LastModified'])
                age = datetime.now(latest_object['LastModified'].tzinfo) - latest_object['LastModified']
                
                if age > freshness_threshold:
                    return {
                        'check_name': 'data_freshness',
                        'passed': False,
                        'message': f'Data in {bucket} is {age.total_seconds()/3600:.1f} hours old',
                        'bucket': bucket
                    }
        
        except Exception as e:
            return {
                'check_name': 'data_freshness',
                'passed': False,
                'message': f'Error checking {bucket}: {str(e)}',
                'bucket': bucket
            }
    
    return {
        'check_name': 'data_freshness',
        'passed': True,
        'message': 'All data sources are fresh'
    }

def check_schema_drift(glue):
    """Detect schema changes that might break downstream processing"""
    
    try:
        # Get table schema from last week
        databases = ['raw_data_catalog', 'processed_data_catalog']
        
        for database in databases:
            tables = glue.get_tables(DatabaseName=database)
            
            for table in tables['TableList']:
                table_name = table['Name']
                
                # Check for recent schema changes
                versions = glue.get_table_versions(
                    DatabaseName=database,
                    TableName=table_name,
                    MaxResults=5
                )
                
                if len(versions['TableVersions']) > 1:
                    current_schema = versions['TableVersions'][0]['Table']['StorageDescriptor']['Columns']
                    previous_schema = versions['TableVersions'][1]['Table']['StorageDescriptor']['Columns']
                    
                    if len(current_schema) != len(previous_schema):
                        return {
                            'check_name': 'schema_drift',
                            'passed': False,
                            'message': f'Schema drift detected in {database}.{table_name}',
                            'table': f'{database}.{table_name}'
                        }
    
    except Exception as e:
        return {
            'check_name': 'schema_drift',
            'passed': False,
            'message': f'Error checking schema: {str(e)}'
        }
    
    return {
        'check_name': 'schema_drift',
        'passed': True,
        'message': 'No significant schema drift detected'
    }

def check_processing_bottlenecks(glue):
    """Identify processing bottlenecks in ETL jobs"""
    
    try:
        job_names = ['bronze-ingestion', 'silver-transformation', 'gold-aggregation']
        
        for job_name in job_names:
            job_runs = glue.get_job_runs(JobName=job_name, MaxResults=10)
            
            if job_runs['JobRuns']:
                # Check average execution time
                recent_runs = [run for run in job_runs['JobRuns'] 
                             if run['JobRunState'] == 'SUCCEEDED']
                
                if len(recent_runs) >= 3:
                    execution_times = []
                    for run in recent_runs[:5]:  # Last 5 successful runs
                        if 'CompletedOn' in run and 'StartedOn' in run:
                            duration = (run['CompletedOn'] - run['StartedOn']).total_seconds()
                            execution_times.append(duration)
                    
                    if execution_times:
                        avg_time = sum(execution_times) / len(execution_times)
                        latest_time = execution_times[0]
                        
                        # Alert if latest run took 50% longer than average
                        if latest_time > avg_time * 1.5:
                            return {
                                'check_name': 'processing_bottlenecks',
                                'passed': False,
                                'message': f'{job_name} execution time increased significantly',
                                'job_name': job_name,
                                'avg_time_minutes': avg_time / 60,
                                'latest_time_minutes': latest_time / 60
                            }
    
    except Exception as e:
        return {
            'check_name': 'processing_bottlenecks',
            'passed': False,
            'message': f'Error checking bottlenecks: {str(e)}'
        }
    
    return {
        'check_name': 'processing_bottlenecks',
        'passed': True,
        'message': 'No processing bottlenecks detected'
    }

def check_catalog_health(glue):
    """Check data catalog health and completeness"""
    
    try:
        databases = glue.get_databases()
        
        for database in databases['DatabaseList']:
            db_name = database['Name']
            
            # Check if database has tables
            tables = glue.get_tables(DatabaseName=db_name)
            
            if len(tables['TableList']) == 0:
                return {
                    'check_name': 'catalog_health',
                    'passed': False,
                    'message': f'Database {db_name} has no tables',
                    'database': db_name
                }
            
            # Check for tables with outdated statistics
            for table in tables['TableList']:
                if 'Parameters' in table and 'last_analyzed' in table['Parameters']:
                    last_analyzed = datetime.fromisoformat(table['Parameters']['last_analyzed'])
                    if datetime.now() - last_analyzed > timedelta(days=7):
                        return {
                            'check_name': 'catalog_health',
                            'passed': False,
                            'message': f'Table statistics outdated for {db_name}.{table["Name"]}',
                            'table': f'{db_name}.{table["Name"]}'
                        }
    
    except Exception as e:
        return {
            'check_name': 'catalog_health',
            'passed': False,
            'message': f'Error checking catalog: {str(e)}'
        }
    
    return {
        'check_name': 'catalog_health',
        'passed': True,
        'message': 'Data catalog is healthy'
    }

def publish_custom_metrics(cloudwatch, monitoring_results):
    """Publish custom monitoring metrics to CloudWatch"""
    
    namespace = 'DataLake/Health'
    
    passed_checks = len([check for check in monitoring_results['checks'] if check['passed']])
    total_checks = len(monitoring_results['checks'])
    health_score = (passed_checks / total_checks) * 100
    
    cloudwatch.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {
                'MetricName': 'HealthScore',
                'Value': health_score,
                'Unit': 'Percent',
                'Timestamp': datetime.utcnow()
            },
            {
                'MetricName': 'FailedChecks',
                'Value': total_checks - passed_checks,
                'Unit': 'Count',
                'Timestamp': datetime.utcnow() 
            }
        ]
    )
    """
    
    # Create the Lambda function
    lambda_client.create_function(
        FunctionName='data-lake-health-monitor',
        Runtime='python3.9',
        Role='arn:aws:iam::123456789012:role/DataLakeMonitoringRole',
        Handler='index.lambda_handler',
        Code={'ZipFile': monitoring_code.encode()},
        Description='Custom data lake health monitoring function',
        Timeout=300,
        Environment={
            'Variables': {
                'NOTIFICATION_TOPIC': 'arn:aws:sns:us-west-2:123456789012:health-alerts'
            }
        }
    )
    
    print("✅ Custom monitoring Lambda function created")

# Execute monitoring setup
if __name__ == "__main__":
    print("🚀 Setting up Comprehensive Data Lake Monitoring")
    print("=" * 60)
    
    setup_comprehensive_monitoring()
    create_intelligent_alarms()
    setup_log_insights_queries()
    create_custom_monitoring_function()
    
    print("\n✅ Monitoring setup complete!")
    print("Configured components:")
    print("  - Custom metrics and dashboard")
    print("  - Intelligent alarms with anomaly detection")
    print("  - Saved Log Insights queries")
    print("  - Custom health monitoring function")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Advanced Log Analysis and Troubleshooting
import boto3
import json
import re
from datetime import datetime, timedelta

def analyze_glue_job_performance():
    """Analyze Glue job performance using CloudWatch Logs Insights"""
    
    logs = boto3.client('logs')
    
    # Query to analyze job performance patterns
    performance_query = """
    fields @timestamp, @message
    | filter @message like /Job finished/
    | parse @message /Job finished with (?<status>\\w+) status in (?<duration>\\d+) seconds/
    | stats 
        count() as job_runs,
        avg(duration) as avg_duration,
        max(duration) as max_duration,
        min(duration) as min_duration
        by status
    | sort job_runs desc
    """
    
    # Execute the query
    query_response = logs.start_query(
        logGroupName='/aws/glue/jobs/logs-v2',
        startTime=int((datetime.now() - timedelta(days=7)).timestamp()),
        endTime=int(datetime.now().timestamp()),
        queryString=performance_query
    )
    
    query_id = query_response['queryId']
    
    # Wait for results
    import time
    while True:
        results = logs.get_query_results(queryId=query_id)
        if results['status'] == 'Complete':
            break
        time.sleep(2)
    
    print("📊 Glue Job Performance Analysis (Last 7 Days)")
    print("=" * 50)
    
    for result in results['results']:
        status = next(item['value'] for item in result if item['field'] == 'status')
        job_runs = next(item['value'] for item in result if item['field'] == 'job_runs')
        avg_duration = next(item['value'] for item in result if item['field'] == 'avg_duration')
        
        print(f"Status: {status}")
        print(f"  Job Runs: {job_runs}")
        print(f"  Avg Duration: {float(avg_duration):.1f} seconds")
        print()

def detect_error_patterns():
    """Detect and categorize error patterns in logs"""
    
    logs = boto3.client('logs')
    
    # Query to find and categorize errors
    error_analysis_query = """
    fields @timestamp, @message
    | filter @message like /ERROR/ or @message like /Exception/
    | parse @message /(?<error_type>\\w+Error|\\w+Exception)/
    | stats count() as occurrences by error_type
    | sort occurrences desc
    | limit 10
    """
    
    query_response = logs.start_query(
        logGroupNames=['/aws/glue/jobs/logs-v2', '/aws/lambda/data-processing'],
        startTime=int((datetime.now() - timedelta(hours=24)).timestamp()),
        endTime=int(datetime.now().timestamp()),
        queryString=error_analysis_query
    )
    
    # Get results
    import time
    query_id = query_response['queryId']
    
    while True:
        results = logs.get_query_results(queryId=query_id)
        if results['status'] == 'Complete':
            break
        time.sleep(2)
    
    print("🚨 Error Pattern Analysis (Last 24 Hours)")
    print("=" * 50)
    
    error_patterns = {}
    
    for result in results['results']:
        error_type = next((item['value'] for item in result if item['field'] == 'error_type'), 'Unknown')
        occurrences = int(next((item['value'] for item in result if item['field'] == 'occurrences'), '0'))
        
        error_patterns[error_type] = occurrences
        print(f"{error_type}: {occurrences} occurrences")
    
    # Provide troubleshooting suggestions
    provide_troubleshooting_suggestions(error_patterns)

def provide_troubleshooting_suggestions(error_patterns):
    """Provide troubleshooting suggestions based on error patterns"""
    
    print("\n💡 Troubleshooting Suggestions")
    print("=" * 50)
    
    suggestions = {
        'OutOfMemoryError': [
            "Increase worker memory allocation (use G.2X instead of G.1X)",
            "Enable dynamic scaling: --conf spark.sql.adaptive.enabled=true",
            "Optimize data partitioning to reduce shuffle operations",
            "Consider using column-based formats like Parquet"
        ],
        'TimeoutException': [
            "Increase job timeout configuration",
            "Check for data skew in processing",
            "Optimize SQL queries and joins",
            "Consider breaking large jobs into smaller chunks"
        ],
        'FileNotFoundException': [
            "Verify S3 bucket permissions and paths",
            "Check if source data files exist",
            "Review job bookmark settings",
            "Validate input path parameters"
        ],
        'AccessDeniedException': [
            "Review IAM role permissions",
            "Check S3 bucket policies",
            "Verify VPC and security group settings",
            "Ensure service roles have required permissions"
        ],
        'DataException': [
            "Implement data quality validation",
            "Add null value handling",
            "Check for data type mismatches",
            "Consider schema evolution handling"
        ]
    }
    
    for error_type, count in error_patterns.items():
        if error_type in suggestions and count > 0:
            print(f"\n🔧 {error_type} ({count} occurrences):")
            for suggestion in suggestions[error_type]:
                print(f"  • {suggestion}")

def analyze_data_processing_trends():
    """Analyze data processing trends and patterns"""
    
    logs = boto3.client('logs')
    
    # Query to analyze processing volume trends
    trend_query = """
    fields @timestamp, @message
    | filter @message like /processed/ and @message like /records/
    | parse @message /processed (?<records>\\d+) records/
    | stats sum(records) as total_records by bin(1h)
    | sort @timestamp desc
    """
    
    query_response = logs.start_query(
        logGroupName='/aws/glue/jobs/logs-v2',
        startTime=int((datetime.now() - timedelta(days=1)).timestamp()),
        endTime=int(datetime.now().timestamp()),
        queryString=trend_query
    )
    
    # Get results
    import time
    query_id = query_response['queryId']
    
    while True:
        results = logs.get_query_results(queryId=query_id)
        if results['status'] == 'Complete':
            break
        time.sleep(2)
    
    print("📈 Data Processing Trends (Last 24 Hours)")
    print("=" * 50)
    
    hourly_volumes = []
    
    for result in results['results']:
        timestamp = next((item['value'] for item in result if item['field'] == '@timestamp'), None)
        total_records = int(next((item['value'] for item in result if item['field'] == 'total_records'), '0'))
        
        if timestamp:
            hour = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).strftime('%H:00')
            hourly_volumes.append((hour, total_records))
            print(f"{hour}: {total_records:,} records processed")
    
    # Detect anomalies in processing volume
    detect_volume_anomalies(hourly_volumes)

def detect_volume_anomalies(hourly_volumes):
    """Detect anomalies in data processing volumes"""
    
    if len(hourly_volumes) < 5:
        return
    
    volumes = [vol[1] for vol in hourly_volumes]
    avg_volume = sum(volumes) / len(volumes)
    
    print(f"\n🔍 Volume Anomaly Detection")
    print(f"Average hourly volume: {avg_volume:,.0f} records")
    
    anomalies = []
    
    for hour, volume in hourly_volumes:
        # Flag as anomaly if volume is >2x or <0.5x the average
        if volume > avg_volume * 2:
            anomalies.append((hour, volume, "High"))
        elif volume < avg_volume * 0.5 and volume > 0:
            anomalies.append((hour, volume, "Low"))
    
    if anomalies:
        print("⚠️ Volume Anomalies Detected:")
        for hour, volume, anomaly_type in anomalies:
            print(f"  {hour}: {volume:,} records ({anomaly_type} - {volume/avg_volume:.1f}x normal)")
    else:
        print("✅ No significant volume anomalies detected")

def create_log_monitoring_dashboard():
    """Create a comprehensive log monitoring dashboard"""
    
    cloudwatch = boto3.client('cloudwatch')
    
    # Log-based dashboard
    log_dashboard = {
        "widgets": [
            {
                "type": "log",
                "x": 0, "y": 0, "width": 12, "height": 6,
                "properties": {
                    "query": "SOURCE '/aws/glue/jobs/logs-v2' | fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc | limit 20",
                    "region": "us-west-2",
                    "title": "Recent Errors - Glue Jobs",
                    "view": "table"
                }
            },
            {
                "type": "log",
                "x": 12, "y": 0, "width": 12, "height": 6, 
                "properties": {
                    "query": "SOURCE '/aws/lambda/data-validation' | fields @timestamp, @message | filter @message like /VALIDATION_FAILED/ | stats count() by bin(5m) | sort @timestamp desc",
                    "region": "us-west-2",
                    "title": "Data Validation Failures",
                    "view": "bar"
                }
            },
            {
                "type": "log",
                "x": 0, "y": 6, "width": 24, "height": 6,
                "properties": {
                    "query": "SOURCE '/aws/glue/jobs/logs-v2' | fields @timestamp, @message | filter @message like /processed/ | parse @message /processed (?<records>\\d+) records in (?<duration>\\d+) seconds/ | stats sum(records) as total_records, avg(duration) as avg_duration by bin(15m) | sort @timestamp desc",
                    "region": "us-west-2", 
                    "title": "Processing Performance Over Time",
                    "view": "line"
                }
            },
            {
                "type": "log",
                "x": 0, "y": 12, "width": 12, "height": 6,
                "properties": {
                    "query": "SOURCE '/aws/cloudtrail' | fields @timestamp, sourceIPAddress, userIdentity.type, eventName | filter eventName like /Delete/ or eventName like /Put/ | stats count() by sourceIPAddress | sort count desc | limit 10",
                    "region": "us-west-2",
                    "title": "Top API Activity by IP",
                    "view": "table"
                }
            },
            {
                "type": "log",
                "x": 12, "y": 12, "width": 12, "height": 6,
                "properties": {
                    "query": "SOURCE '/aws/lambda/data-quality-monitor' | fields @timestamp, @message | filter @message like /QUALITY_SCORE/ | parse @message /QUALITY_SCORE: (?<score>\\d+\\.\\d+)/ | stats avg(score) as avg_quality_score by bin(1h) | sort @timestamp desc",
                    "region": "us-west-2",
                    "title": "Data Quality Score Trends",
                    "view": "line"
                }
            }
        ]
    }
    
    cloudwatch.put_dashboard(
        DashboardName='DataLakeLogAnalytics',
        DashboardBody=json.dumps(log_dashboard)
    )
    
    print("✅ Log monitoring dashboard created")

def automated_log_analysis():
    """Run automated log analysis and generate insights"""
    
    print("🔍 Starting Automated Log Analysis")
    print("=" * 50)
    
    # Run all analysis functions
    analyze_glue_job_performance()
    detect_error_patterns()
    analyze_data_processing_trends()
    
    # Create monitoring dashboard
    create_log_monitoring_dashboard()
    
    # Generate summary report
    generate_log_analysis_report()

def generate_log_analysis_report():
    """Generate a comprehensive log analysis report"""
    
    report = {
        'report_timestamp': datetime.now().isoformat(),
        'analysis_period': '24 hours',
        'summary': {
            'total_errors_analyzed': 150,
            'top_error_type': 'TimeoutException',
            'performance_trend': 'Stable',
            'recommendations': [
                'Increase timeout for long-running jobs',
                'Optimize data partitioning strategy',
                'Implement better error retry logic',
                'Add more granular monitoring'
            ]
        },
        'health_score': 87.5,
        'next_analysis': (datetime.now() + timedelta(hours=4)).isoformat()
    }
    
    # Save report to S3
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='data-lake-monitoring',
        Key=f"log-analysis-reports/report-{datetime.now().strftime('%Y-%m-%d-%H')}.json",
        Body=json.dumps(report, indent=2)
    )
    
    print(f"📋 Log Analysis Report Generated")
    print(f"Health Score: {report['health_score']}/100")
    print(f"Top Issue: {report['summary']['top_error_type']}")
    print(f"Next Analysis: {report['next_analysis']}")

# Execute automated analysis
if __name__ == "__main__":
    automated_log_analysis()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# Automated Response and Remediation System
import boto3
import json
from datetime import datetime, timedelta

def create_incident_response_lambda():
    """Create Lambda function for automated incident response"""
    
    lambda_client = boto3.client('lambda')
    
    incident_response_code = """
import json
import boto3
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """Automated incident response for data lake issues"""
    
    # Parse CloudWatch alarm or SNS message
    if 'Records' in event and event['Records']:
        # SNS triggered
        message = json.loads(event['Records'][0]['Sns']['Message'])
        alarm_name = message.get('AlarmName', 'Unknown')
        new_state = message.get('NewStateValue', 'UNKNOWN')
    else:
        # Direct CloudWatch event
        alarm_name = event.get('AlarmName', 'Unknown')
        new_state = event.get('NewStateValue', 'UNKNOWN')
    
    if new_state == 'ALARM':
        response = handle_alarm(alarm_name, message if 'message' in locals() else event)
    else:
        response = {'action': 'no_action_required', 'reason': f'Alarm state: {new_state}'}
    
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }

def handle_alarm(alarm_name, alarm_data):
    """Handle specific alarm conditions"""
    
    glue = boto3.client('glue')
    s3 = boto3.client('s3')
    sns = boto3.client('sns')
    cloudwatch = boto3.client('cloudwatch')
    
    response_actions = {
        'DataLake-DataQualityDegradation': handle_data_quality_issue,
        'DataLake-HighIngestionLatency': handle_performance_issue,
        'DataLake-UnusualDataVolume': handle_volume_anomaly,
        'DataLake-HighErrorRate': handle_error_spike,
        'DataLake-CostAnomaly': handle_cost_spike
    }
    
    if alarm_name in response_actions:
        return response_actions[alarm_name](alarm_data, glue, s3, sns, cloudwatch)
    else:
        return handle_generic_alarm(alarm_name, alarm_data, sns)

def handle_data_quality_issue(alarm_data, glue, s3, sns, cloudwatch):
    """Respond to data quality degradation"""
    
    try:
        # Step 1: Quarantine potentially bad data
        quarantine_bucket = 'data-lake-quarantine'
        source_bucket = 'data-lake-bronze'
        
        # Move recent data to quarantine
        current_hour = datetime.now().strftime('%Y/%m/%d/%H')
        quarantine_key = f"quarantine/{current_hour}/"
        
        # Copy recent data to quarantine (implementation would list and copy objects)
        print(f"Quarantining data to {quarantine_bucket}/{quarantine_key}")
        
        # Step 2: Stop downstream processing
        job_names = ['silver-transformation', 'gold-aggregation']
        stopped_jobs = []
        
        for job_name in job_names:
            running_jobs = glue.get_job_runs(JobName=job_name, MaxResults=10)
            
            for run in running_jobs['JobRuns']:
                if run['JobRunState'] == 'RUNNING':
                    glue.batch_stop_job_run(
                        JobName=job_name,
                        JobRunIds=[run['Id']]
                    )
                    stopped_jobs.append(f"{job_name}:{run['Id']}")
        
        # Step 3: Trigger data quality validation job
        validation_response = glue.start_job_run(
            JobName='emergency-data-quality-check',
            Arguments={
                '--quarantine_path': f's3://{quarantine_bucket}/{quarantine_key}',
                '--validation_mode': 'emergency',
                '--alert_threshold': '90'
            }
        )
        
        # Step 4: Send notification
        notification_message = f"""
        🚨 Data Quality Issue Detected - Automated Response Activated
        
        Actions Taken:
        - Quarantined recent data to {quarantine_bucket}
        - Stopped downstream jobs: {', '.join(stopped_jobs)}
        - Started emergency validation job: {validation_response['JobRunId']}
        
        Next Steps:
        1. Review quarantined data
        2. Identify root cause of quality degradation
        3. Fix source data issues
        4. Restart processing pipeline
        
        Incident ID: {datetime.now().strftime('%Y%m%d-%H%M%S')}
        """
        
        sns.publish(
            TopicArn='arn:aws:sns:us-west-2:123456789012:critical-incidents',
            Subject='CRITICAL: Data Quality Issue - Automated Response',
            Message=notification_message
        )
        
        return {
            'action': 'data_quality_response',
            'quarantined_data': f'{quarantine_bucket}/{quarantine_key}',
            'stopped_jobs': stopped_jobs,
            'validation_job': validation_response['JobRunId']
        }
        
    except Exception as e:
        return {
            'action': 'response_failed',
            'error': str(e),
            'manual_intervention_required': True
        }

def handle_performance_issue(alarm_data, glue, s3, sns, cloudwatch):
    """Respond to performance degradation"""
    
    try:
        # Step 1: Get current job performance metrics
        job_name = 'bronze-ingestion'  # Extract from alarm data in real implementation
        
        job_runs = glue.get_job_runs(JobName=job_name, MaxResults=5)
        current_run = next((run for run in job_runs['JobRuns'] if run['JobRunState'] == 'RUNNING'), None)
        
        if current_run:
            # Step 2: Scale up the job
            updated_job = glue.update_job(
                JobName=job_name,
                JobUpdate={
                    'NumberOfWorkers': 10,  # Scale up workers
                    'WorkerType': 'G.2X',   # Use larger worker type
                    'DefaultArguments': {
                        '--conf': 'spark.sql.adaptive.enabled=true',
                        '--conf': 'spark.sql.adaptive.coalescePartitions.enabled=true',
                        '--conf': 'spark.sql.adaptive.skewJoin.enabled=true'
                    }
                }
            )
            
            # Step 3: Stop current slow job and restart with new configuration
            glue.batch_stop_job_run(
                JobName=job_name,
                JobRunIds=[current_run['Id']]
            )
            
            # Wait a moment then restart
            import time
            time.sleep(30)
            
            new_job_run = glue.start_job_run(
                JobName=job_name,
                Arguments={
                    '--performance_mode': 'optimized',
                    '--original_run_id': current_run['Id']
                }
            )
            
            return {
                'action': 'performance_optimization',
                'stopped_job': current_run['Id'],
                'restarted_job': new_job_run['JobRunId'],
                'optimization': 'scaled_up_workers_and_enabled_adaptive_query'
            }
        
        else:
            return {
                'action': 'no_running_job_found',
                'recommendation': 'Manual investigation required'
            }
    
    except Exception as e:
        return {
            'action': 'performance_response_failed',
            'error': str(e)
        }

def handle_volume_anomaly(alarm_data, glue, s3, sns, cloudwatch):
    """Respond to unusual data volume"""
    
    try:
        # Step 1: Investigate the volume change
        bucket_name = 'data-lake-raw'
        
        # Get recent object counts and sizes
        today = datetime.now().strftime('%Y/%m/%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
        
        today_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=f'daily-data/{today}/')
        yesterday_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=f'daily-data/{yesterday}/')
        
        today_count = today_objects.get('KeyCount', 0)
        yesterday_count = yesterday_objects.get('KeyCount', 0)
        
        volume_change = (today_count - yesterday_count) / yesterday_count if yesterday_count > 0 else 0
        
        if volume_change > 1.0:  # More than 100% increase
            # Unusual high volume - might need to scale processing
            response = scale_for_high_volume(glue)
        elif volume_change < -0.5:  # More than 50% decrease
            # Unusual low volume - might indicate upstream issue
            response = investigate_low_volume(sns)
        else:
            response = {'action': 'volume_within_acceptable_range'}
        
        response['volume_analysis'] = {
            'today_file_count': today_count,
            'yesterday_file_count': yesterday_count,
            'volume_change_percent': volume_change * 100
        }
        
        return response
    
    except Exception as e:
        return {
            'action': 'volume_analysis_failed',
            'error': str(e)
        }

def scale_for_high_volume(glue):
    """Scale processing capacity for high data volume"""
    
    # Scale up all processing jobs
    jobs_to_scale = ['bronze-ingestion', 'silver-transformation']
    scaled_jobs = []
    
    for job_name in jobs_to_scale:
        try:
            glue.update_job(
                JobName=job_name,
                JobUpdate={
                    'NumberOfWorkers': 15,
                    'WorkerType': 'G.2X',
                    'MaxRetries': 3
                }
            )
            scaled_jobs.append(job_name)
        except Exception as e:
            print(f"Failed to scale {job_name}: {e}")
    
    return {
        'action': 'scaled_for_high_volume',
        'scaled_jobs': scaled_jobs,
        'scaling_factor': '1.5x workers, upgraded to G.2X'
    }

def investigate_low_volume(sns):
    """Investigate and alert on low data volume"""
    
    alert_message = """
    📉 Low Data Volume Detected
    
    Possible causes:
    - Upstream data source issues
    - Data pipeline failures
    - Weekend/holiday reduced activity
    - Configuration changes
    
    Recommended actions:
    1. Check upstream data sources
    2. Verify pipeline configurations
    3. Review recent changes
    4. Contact data providers if needed
    """
    
    sns.publish(
        TopicArn='arn:aws:sns:us-west-2:123456789012:volume-alerts',
        Subject='Data Volume Alert: Unusually Low Volume',
        Message=alert_message
    )
    
    return {
        'action': 'low_volume_investigation',
        'alert_sent': True,
        'recommendation': 'Check upstream data sources'
    }

def handle_error_spike(alarm_data, glue, s3, sns, cloudwatch):
    """Handle spike in error rates"""
    
    try:
        # Get error details from recent job runs
        job_names = ['bronze-ingestion', 'silver-transformation', 'gold-aggregation']
        error_analysis = {}
        
        for job_name in job_names:
            job_runs = glue.get_job_runs(JobName=job_name, MaxResults=10)
            failed_runs = [run for run in job_runs['JobRuns'] if run['JobRunState'] == 'FAILED']
            
            if failed_runs:
                recent_failure = failed_runs[0]
                error_analysis[job_name] = {
                    'error_message': recent_failure.get('ErrorMessage', 'Unknown error'),
                    'failure_time': recent_failure['CompletedOn'].isoformat(),
                    'run_id': recent_failure['Id']
                }
        
        # Implement error-specific recovery
        if error_analysis:
            # Try to restart failed jobs with error handling
            recovery_jobs = []
            
            for job_name, error_info in error_analysis.items():
                if 'OutOfMemoryError' in error_info['error_message']:
                    # Restart with more memory
                    new_run = glue.start_job_run(
                        JobName=job_name,
                        WorkerType='G.2X',
                        NumberOfWorkers=3,
                        Arguments={
                            '--recovery_mode': 'true',
                            '--failed_run_id': error_info['run_id']
                        }
                    )
                    recovery_jobs.append(new_run['JobRunId'])
            
            return {
                'action': 'error_recovery_attempted',
                'error_analysis': error_analysis,
                'recovery_jobs': recovery_jobs
            }
        
        return {
            'action': 'no_recent_failures_found',
            'recommendation': 'Manual investigation of error spike source'
        }
    
    except Exception as e:
        return {
            'action': 'error_recovery_failed',
            'error': str(e)
        }

def handle_cost_spike(alarm_data, glue, s3, sns, cloudwatch):
    """Handle unexpected cost increases"""
    
    try:
        # Get current resource usage
        ce = boto3.client('ce')  # Cost Explorer
        
        # Get daily costs for last 7 days
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        cost_response = ce.get_cost_and_usage(
            TimePeriod={'Start': start_date, 'End': end_date},
            Granularity='DAILY',
            Metrics=['BlendedCost'],
            GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}]
        )
        
        # Analyze cost drivers
        service_costs = {}
        for result in cost_response['ResultsByTime']:
            for group in result['Groups']:
                service = group['Keys'][0]
                cost = float(group['Metrics']['BlendedCost']['Amount'])
                service_costs[service] = service_costs.get(service, 0) + cost
        
        # Identify top cost drivers
        top_services = sorted(service_costs.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Take cost optimization actions
        optimization_actions = []
        
        # If Glue is a top cost driver, optimize job configurations
        if any('Glue' in service for service, _ in top_services):
            # Scale down non-critical jobs
            non_critical_jobs = ['data-quality-validation', 'analytics-reporting']
            
            for job_name in non_critical_jobs:
                try:
                    glue.update_job(
                        JobName=job_name,
                        JobUpdate={
                            'NumberOfWorkers': 2,
                            'WorkerType': 'G.1X'
                        }
                    )
                    optimization_actions.append(f'Scaled down {job_name}')
                except:
                    pass
        
        # Send cost optimization report
        cost_report = f"""
        💰 Cost Spike Detected - Optimization Actions Taken
        
        Top Cost Drivers (Last 7 Days):
        {chr(10).join([f"- {service}: ${cost:.2f}" for service, cost in top_services[:3]])}
        
        Optimization Actions:
        {chr(10).join([f"- {action}" for action in optimization_actions])}
        
        Recommendations:
        - Review resource utilization
        - Consider reserved capacity
        - Optimize data processing schedules
        - Implement cost monitoring alerts
        """
        
        sns.publish(
            TopicArn='arn:aws:sns:us-west-2:123456789012:cost-alerts',
            Subject='Cost Optimization: Automated Actions Taken',
            Message=cost_report
        )
        
        return {
            'action': 'cost_optimization',
            'top_cost_drivers': dict(top_services[:3]),
            'optimization_actions': optimization_actions
        }
    
    except Exception as e:
        return {
            'action': 'cost_analysis_failed',
            'error': str(e)
        }

def handle_generic_alarm(alarm_name, alarm_data, sns):
    """Handle unrecognized alarms"""
    
    sns.publish(
        TopicArn='arn:aws:sns:us-west-2:123456789012:generic-alerts',
        Subject=f'Data Lake Alert: {alarm_name}',
        Message=f'Alarm {alarm_name} triggered but no automated response defined. Manual investigation required.'
    )
    
    return {
        'action': 'manual_intervention_required',
        'alarm_name': alarm_name,
        'notification_sent': True
    }
    """
    
    # Create the incident response Lambda function
    lambda_client.create_function(
        FunctionName='data-lake-incident-response',
        Runtime='python3.9',
        Role='arn:aws:iam::123456789012:role/IncidentResponseRole',
        Handler='index.lambda_handler',
        Code={'ZipFile': incident_response_code.encode()},
        Description='Automated incident response for data lake issues',
        Timeout=900,  # 15 minutes
        Environment={
            'Variables': {
                'CRITICAL_TOPIC': 'arn:aws:sns:us-west-2:123456789012:critical-incidents',
                'COST_BUDGET_THRESHOLD': '1000'
            }
        }
    )
    
    print("✅ Incident response Lambda function created")

def create_self_healing_system():
    """Create a self-healing system for common data lake issues"""
    
    lambda_client = boto3.client('lambda')
    
    self_healing_code = """
import json
import boto3
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """Self-healing system for data lake operations"""
    
    healing_actions = []
    
    # Check and heal common issues
    healing_actions.extend(heal_stuck_jobs())
    healing_actions.extend(heal_failed_crawlers())
    healing_actions.extend(heal_data_quality_issues())
    healing_actions.extend(optimize_resource_usage())
    
    # Generate healing report
    report = {
        'timestamp': datetime.utcnow().isoformat(),
        'healing_actions': healing_actions,
        'total_actions': len(healing_actions),
        'status': 'completed'
    }
    
    # Store healing report
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='data-lake-monitoring',
        Key=f"self-healing-reports/{datetime.now().strftime('%Y/%m/%d')}/healing-{datetime.now().strftime('%H%M%S')}.json",
        Body=json.dumps(report, indent=2)
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps(report)
    }

def heal_stuck_jobs():
    """Identify and restart stuck Glue jobs"""
    
    glue = boto3.client('glue')
    actions = []
    
    # Get all jobs
    jobs = glue.get_jobs()
    
    for job in jobs['Jobs']:
        job_name = job['Name']
        
        # Get recent job runs
        job_runs = glue.get_job_runs(JobName=job_name, MaxResults=5)
        
        for run in job_runs['JobRuns']:
            if run['JobRunState'] == 'RUNNING':
                # Check if job has been running too long
                start_time = run['StartedOn']
                runtime = datetime.now(start_time.tzinfo) - start_time
                
                # If running more than 4 hours, consider it stuck
                if runtime.total_seconds() > 14400:
                    try:
                        # Stop stuck job
                        glue.batch_stop_job_run(
                            JobName=job_name,
                            JobRunIds=[run['Id']]
                        )
                        
                        # Restart job
                        new_run = glue.start_job_run(
                            JobName=job_name,
                            Arguments={
                                '--healing_restart': 'true',
                                '--original_run_id': run['Id']
                            }
                        )
                        
                        actions.append({
                            'action': 'restart_stuck_job',
                            'job_name': job_name,
                            'stuck_run_id': run['Id'],
                            'new_run_id': new_run['JobRunId'],
                            'stuck_duration_hours': runtime.total_seconds() / 3600
                        })
                        
                    except Exception as e:
                        actions.append({
                            'action': 'failed_to_restart_job',
                            'job_name': job_name,
                            'error': str(e)
                        })
    
    return actions

def heal_failed_crawlers():
    """Restart failed crawlers automatically"""
    
    glue = boto3.client('glue')
    actions = []
    
    # Get all crawlers
    crawlers = glue.get_crawlers()
    
    for crawler in crawlers['Crawlers']:
        crawler_name = crawler['Name']
        
        # Check crawler state and last run
        if crawler['State'] == 'READY' and 'LastCrawl' in crawler:
            last_crawl = crawler['LastCrawl']
            
            if last_crawl['Status'] == 'FAILED':
                # Check how long ago it failed
                end_time = last_crawl['EndTime']
                failed_ago = datetime.now(end_time.tzinfo) - end_time
                
                # If failed more than 1 hour ago, try to restart
                if failed_ago.total_seconds() > 3600:
                    try:
                        glue.start_crawler(Name=crawler_name)
                        
                        actions.append({
                            'action': 'restart_failed_crawler',
                            'crawler_name': crawler_name,
                            'failure_time': end_time.isoformat(),
                            'error_message': last_crawl.get('ErrorMessage', 'Unknown error')
                        })
                        
                    except Exception as e:
                        actions.append({
                            'action': 'failed_to_restart_crawler',
                            'crawler_name': crawler_name,
                            'error': str(e)
                        })
    
    return actions

def heal_data_quality_issues():
    """Automatically fix common data quality issues"""
    
    actions = []
    
    # Check for quarantined data that can be processed
    s3 = boto3.client('s3')
    
    try:
        quarantine_objects = s3.list_objects_v2(
            Bucket='data-lake-quarantine',
            Prefix='quarantine/'
        )
        
        if 'Contents' in quarantine_objects:
            # Check if quarantined data is older than validation period
            validation_period = timedelta(hours=6)
            
            for obj in quarantine_objects['Contents']:
                quarantine_age = datetime.now(obj['LastModified'].tzinfo) - obj['LastModified']
                
                if quarantine_age > validation_period:
                    # Try to reprocess quarantined data
                    glue = boto3.client('glue')
                    
                    try:
                        reprocess_job = glue.start_job_run(
                            JobName='quarantine-data-reprocessor',
                            Arguments={
                                '--quarantine_object': obj['Key'],
                                '--reprocess_mode': 'healing'
                            }
                        )
                        
                        actions.append({
                            'action': 'reprocess_quarantined_data',
                            'object_key': obj['Key'],
                            'quarantine_age_hours': quarantine_age.total_seconds() / 3600,
                            'reprocess_job_id': reprocess_job['JobRunId']
                        })
                        
                    except Exception as e:
                        actions.append({
                            'action': 'failed_to_reprocess_quarantine',
                            'object_key': obj['Key'],
                            'error': str(e)
                        })
    
    except Exception as e:
        actions.append({
            'action': 'failed_quarantine_check',
            'error': str(e)
        })
    
    return actions

def optimize_resource_usage():
    """Automatically optimize resource usage"""
    
    actions = []
    glue = boto3.client('glue')
    
    # Get job metrics to identify underutilized jobs
    jobs = glue.get_jobs()
    
    for job in jobs['Jobs']:
        job_name = job['Name']
        current_workers = job.get('NumberOfWorkers', 2)
        
        # Get recent successful runs
        job_runs = glue.get_job_runs(JobName=job_name, MaxResults=10)
        successful_runs = [run for run in job_runs['JobRuns'] if run['JobRunState'] == 'SUCCEEDED']
        
        if len(successful_runs) >= 3:
            # Analyze resource utilization patterns
            execution_times = []
            for run in successful_runs[:5]:
                if 'CompletedOn' in run and 'StartedOn' in run:
                    duration = (run['CompletedOn'] - run['StartedOn']).total_seconds()
                    execution_times.append(duration)
            
            if execution_times:
                avg_time = sum(execution_times) / len(execution_times)
                
                # If job consistently finishes quickly, it might be over-provisioned
                if avg_time < 600 and current_workers > 2:  # Less than 10 minutes
                    try:
                        glue.update_job(
                            JobName=job_name,
                            JobUpdate={
                                'NumberOfWorkers': max(2, current_workers - 1)
                            }
                        )
                        
                        actions.append({
                            'action': 'optimize_worker_count',
                            'job_name': job_name,
                            'old_workers': current_workers,
                            'new_workers': max(2, current_workers - 1),
                            'avg_execution_time': avg_time
                        })
                        
                    except Exception as e:
                        actions.append({
                            'action': 'failed_worker_optimization',
                            'job_name': job_name,
                            'error': str(e)
                        })
    
    return actions
    """
    
    # Create self-healing Lambda function
    lambda_client.create_function(
        FunctionName='data-lake-self-healing',
        Runtime='python3.9',
        Role='arn:aws:iam::123456789012:role/SelfHealingRole',
        Handler='index.lambda_handler',
        Code={'ZipFile': self_healing_code.encode()},
        Description='Self-healing system for data lake operations',
        Timeout=600,
        Environment={
            'Variables': {
                'HEALING_ENABLED': 'true',
                'MONITORING_BUCKET': 'data-lake-monitoring'
            }
        }
    )
    
    print("✅ Self-healing Lambda function created")

def schedule_automated_responses():
    """Schedule automated response functions"""
    
    events = boto3.client('events')
    
    # Schedule self-healing to run every 4 hours
    events.put_rule(
        Name='data-lake-self-healing-schedule',
        ScheduleExpression='rate(4 hours)',
        State='ENABLED',
        Description='Run self-healing system every 4 hours'
    )
    
    events.put_targets(
        Rule='data-lake-self-healing-schedule',
        Targets=[
            {
                'Id': '1',
                'Arn': 'arn:aws:lambda:us-west-2:123456789012:function:data-lake-self-healing'
            }
        ]
    )
    
    print("✅ Automated response scheduling configured")

# Execute automated response setup
if __name__ == "__main__":
    print("🚀 Setting up Automated Response and Remediation")
    print("=" * 60)
    
    create_incident_response_lambda()
    create_self_healing_system()
    schedule_automated_responses()
    
    print("\n✅ Automated response system ready!")
    print("Configured components:")
    print("  - Incident response Lambda")
    print("  - Self-healing system")
    print("  - Automated scheduling")
    print("  - Multi-layered remediation")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

# Helper functions
def get_pipeline_recommendation(pipeline_type, data_volume, complexity, sla_requirement):
    """Generate pipeline recommendations based on requirements"""
    
    # Base recommendations
    recommendations = {
        'orchestrator': 'AWS Step Functions',
        'pattern': 'Event-driven',
        'components': ['Lambda', 'S3', 'SNS'],
        'cost_estimate': '$100-500/month',
        'implementation_time': '2-4 weeks'
    }
    
    # Adjust based on complexity
    if complexity in ['Complex (Conditional paths)', 'Very Complex (Dynamic workflows)']:
        recommendations['orchestrator'] = 'Amazon MWAA (Airflow)'
        recommendations['pattern'] = 'DAG-based with conditional logic'
        recommendations['components'] = ['Airflow', 'Glue', 'EMR', 'Redshift']
        recommendations['cost_estimate'] = '$500-2000/month'
        recommendations['implementation_time'] = '6-12 weeks'
    
    # Adjust based on data volume
    if 'Large' in data_volume:
        if 'EMR' not in recommendations['components']:
            recommendations['components'].append('EMR')
        recommendations['components'].append('Kinesis')
        
    # Adjust based on SLA
    if sla_requirement == 'Near real-time (< 5 min)':
        recommendations['pattern'] = 'Stream processing'
        recommendations['components'] = ['Kinesis', 'Lambda', 'DynamoDB']
        recommendations['orchestrator'] = 'Kinesis + Lambda'
    
    # Cost breakdown
    recommendations['cost_breakdown'] = {
        'Compute': 60,
        'Storage': 25,
        'Orchestration': 10,
        'Monitoring': 5
    }
    
    return recommendations

def generate_workflow_config(trigger_type, job_count, crawler_count, error_handling):
    """Generate workflow configuration"""
    
    config = {
        'name': f'workflow-{datetime.now().strftime("%Y%m%d")}',
        'trigger': trigger_type,
        'job_count': job_count,
        'crawler_count': crawler_count,
        'error_handling': error_handling
    }
    
    # Estimate runtime
    base_time = job_count * 15 + crawler_count * 10  # minutes
    config['estimated_runtime'] = f'{base_time} minutes'
    
    # Estimate cost
    base_cost = (job_count * 0.44 + crawler_count * 0.44) * 24  # DPU hours
    config['cost_estimate'] = f'${base_cost:.2f}/day'
    
    return config

def create_workflow_timeline(workflow_config):
    """Create workflow execution timeline"""
    
    start_time = datetime.now()
    timeline_data = []
    
    # Add crawlers
    current_time = start_time
    for i in range(workflow_config['crawler_count']):
        timeline_data.append({
            'Resource': f'Crawler {i+1}',
            'Start': current_time,
            'Finish': current_time + timedelta(minutes=10),
            'Type': 'Crawler'
        })
        current_time += timedelta(minutes=2)
    
    # Add ETL jobs
    job_start = start_time + timedelta(minutes=12)
    for i in range(workflow_config['job_count']):
        timeline_data.append({
            'Resource': f'ETL Job {i+1}',
            'Start': job_start + timedelta(minutes=i*5),
            'Finish': job_start + timedelta(minutes=i*5+15),
            'Type': 'ETL Job'
        })
    
    # Add validation
    validation_start = job_start + timedelta(minutes=workflow_config['job_count']*5+15)
    timeline_data.append({
        'Resource': 'Validation',
        'Start': validation_start,
        'Finish': validation_start + timedelta(minutes=5),
        'Type': 'Validation'
    })
    
    return timeline_data

def generate_workflow_status():
    """Generate simulated workflow status"""
    
    import random
    
    status = {
        'active_workflows': random.randint(3, 8),
        'completed_today': random.randint(12, 25),
        'failed_jobs': random.randint(0, 3),
        'daily_cost': random.uniform(45.0, 150.0)
    }
    
    # Workflow details
    workflow_names = ['sales-etl', 'customer-data', 'inventory-sync', 'analytics-prep', 'reporting-pipeline']
    statuses = ['Running', 'Completed', 'Failed', 'Scheduled']
    
    status['workflows'] = []
    for i in range(5):
        status['workflows'].append({
            'Workflow': workflow_names[i],
            'Status': random.choice(statuses),
            'Started': (datetime.now() - timedelta(hours=random.randint(1, 6))).strftime('%H:%M'),
            'Duration': f'{random.randint(15, 180)} min',
            'Next Run': (datetime.now() + timedelta(hours=random.randint(1, 24))).strftime('%H:%M')
        })
    
    return status

def generate_monitoring_data():
    """Generate simulated monitoring data"""
    
    import random
    
    data = {
        'data_processed_gb': random.uniform(100.0, 500.0),
        'data_growth_percent': random.uniform(-5.0, 15.0),
        'pipeline_success_rate': random.uniform(92.0, 99.5),
        'success_trend': random.choice(['Stable', 'Improving', 'Declining']),
        'active_alerts': random.randint(0, 5),
        'alert_severity': random.choice(['Low', 'Medium', 'High']),
        'daily_cost': random.uniform(75.0, 250.0),
        'cost_trend': random.choice(['Under budget', 'On budget', 'Over budget'])
    }
    
    # Processing timeline
    data['processing_timeline'] = []
    for hour in range(24):
        data['processing_timeline'].append({
            'hour': f'{hour:02d}:00',
            'volume_gb': random.uniform(10.0, 50.0)
        })
    
    # Pipeline status
    data['pipeline_status'] = {
        'Successful': random.randint(80, 95),
        'Failed': random.randint(2, 8),
        'Retrying': random.randint(1, 5)
    }
    
    # Service health
    services = ['S3', 'Glue', 'Redshift', 'Lambda', 'Athena', 'EMR']
    statuses = ['Healthy', 'Warning', 'Critical']
    data['service_health'] = []
    
    for service in services:
        data['service_health'].append({
            'Service': service,
            'Status': random.choice(statuses),
            'Uptime': f'{random.uniform(98.0, 100.0):.1f}%',
            'Avg Response': f'{random.randint(50, 500)}ms',
            'Last Check': f'{random.randint(1, 10)} min ago'
        })
    
    return data

def generate_alert_config(alert_type, severity, threshold, notification_channel):
    """Generate alert configuration"""
    
    alert_configs = {
        'Performance Degradation': {
            'name': 'High Latency Alert',
            'metric': 'AvgExecutionTime',
            'condition': 'GreaterThanThreshold',
            'threshold': f'{threshold}th percentile'
        },
        'Data Quality Issues': {
            'name': 'Data Quality Score Alert',
            'metric': 'DataQualityScore',
            'condition': 'LessThanThreshold',
            'threshold': f'{threshold}% quality score'
        },
        'Pipeline Failures': {
            'name': 'Job Failure Rate Alert',
            'metric': 'FailureRate',
            'condition': 'GreaterThanThreshold',
            'threshold': f'{threshold}% failure rate'
        },
        'Security Anomalies': {
            'name': 'Unusual Access Pattern Alert',
            'metric': 'AnomalousActivity',
            'condition': 'AnomalyDetection',
            'threshold': f'{threshold}% confidence'
        },
        'Cost Overruns': {
            'name': 'Budget Threshold Alert',
            'metric': 'DailyCost',
            'condition': 'GreaterThanThreshold',
            'threshold': f'${threshold} daily spend'
        }
    }
    
    config = alert_configs.get(alert_type, alert_configs['Performance Degradation'])
    config['action'] = f'Send {notification_channel} notification'
    
    return config

def predict_alert_frequency(alert_config):
    """Predict alert frequency"""
    
    import random
    
    # Simulate frequency based on alert type and threshold
    base_frequency = {
        'High Latency Alert': 3,
        'Data Quality Score Alert': 1,
        'Job Failure Rate Alert': 2,
        'Unusual Access Pattern Alert': 1,
        'Budget Threshold Alert': 4
    }
    
    frequency = base_frequency.get(alert_config['name'], 2)
    
    return {
        'Daily': frequency + random.randint(-1, 2),
        'Weekly': (frequency + random.randint(-1, 2)) * 7,
        'Monthly': (frequency + random.randint(-1, 2)) * 30
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
    # 🔧 AWS Data Operations & Support
    <div class='info-box'>
    Learn to build, orchestrate, and monitor robust data pipelines using AWS Glue Workflows, advanced monitoring techniques, and automated remediation strategies.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs([
        "🔄 Data pipelines on AWS",
        "🎯 AWS Glue Workflow", 
        "📊 Monitoring a Data Lake"
    ])
    
    with tab1:
        data_pipelines_tab()
    
    with tab2:
        glue_workflow_tab()
    
    with tab3:
        monitoring_tab()
    
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
