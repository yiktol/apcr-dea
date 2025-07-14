
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
    page_title="AWS Data Pipeline Orchestration Hub",
    page_icon="🔗",
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
        
        .architecture-card {{
            background: white;
            padding: 20px;
            border-radius: 15px;
            border: 2px solid {AWS_COLORS['primary']};
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
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
            - 🔗 Data Pipeline Orchestration Fundamentals
            - 🛠️ AWS Step Functions for Data Workflows
            - ⚡ Serverless Event-driven Architectures
            - 🔄 Workflow Types and Patterns
            - 📊 ETL Pipeline Orchestration Examples
            - 🏗️ Data Pipeline Design Best Practices
            
            **Learning Objectives:**
            - Understand data pipeline orchestration concepts
            - Learn AWS Step Functions workflows
            - Explore serverless orchestration patterns
            - Practice with interactive examples
            - Master event-driven data processing
            """)

def create_data_pipeline_overview_mermaid():
    """Create mermaid diagram for data pipeline overview"""
    return """
    graph LR
        A[📥 Data Sources] --> B[🔧 Orchestrator]
        B --> C[⚙️ ETL Jobs]
        B --> D[🔄 Data Processing]
        B --> E[📊 Analytics]
        C --> F[🗂️ Data Lake]
        D --> F
        E --> G[📈 Insights]
        
        B --> H{Error?}
        H -->|Yes| I[🚨 Retry/Alert]
        H -->|No| J[✅ Success]
        I --> B
        
        style A fill:#4B9EDB,stroke:#232F3E,color:#fff
        style B fill:#FF9900,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#3FB34F,stroke:#232F3E,color:#fff
        style E fill:#3FB34F,stroke:#232F3E,color:#fff
        style F fill:#232F3E,stroke:#FF9900,color:#fff
        style G fill:#4B9EDB,stroke:#232F3E,color:#fff
    """

def create_step_functions_architecture_mermaid():
    """Create mermaid diagram for Step Functions architecture"""
    return """
    graph TD
        A[📋 State Machine] --> B{Choice State}
        B -->|Condition 1| C[⚙️ AWS Glue Job]
        B -->|Condition 2| D[🔧 Lambda Function]
        B -->|Condition 3| E[📊 EMR Step]
        
        C --> F[📤 S3 Output]
        D --> F
        E --> F
        
        F --> G{Parallel Tasks}
        G --> H[📈 Athena Query]
        G --> I[🔄 Data Validation]
        G --> J[📧 SNS Notification]
        
        H --> K[✅ Success State]
        I --> K
        J --> K
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#3FB34F,stroke:#232F3E,color:#fff
        style E fill:#3FB34F,stroke:#232F3E,color:#fff
        style G fill:#4B9EDB,stroke:#232F3E,color:#fff
        style K fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_workflow_types_mermaid():
    """Create mermaid diagram comparing workflow types"""
    return """
    graph TD
        subgraph "Standard Workflows"
            A1[⏱️ Up to 1 Year Duration]
            A2[🎯 Exactly-Once Execution]
            A3[📊 Full Execution History]
            A4[🔍 Visual Debugging]
            A5[💰 $0.025 per state transition]
        end
        
        subgraph "Express Workflows"
            B1[⚡ Up to 5 Minutes Duration]
            B2[🔄 At-Least-Once Execution]
            B3[📝 CloudWatch Logs Only]
            B4[🚀 High Event Rate]
            B5[💸 $0.000001 per execution]
        end
        
        style A1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style A2 fill:#3FB34F,stroke:#232F3E,color:#fff
        style A3 fill:#FF9900,stroke:#232F3E,color:#fff
        style A4 fill:#232F3E,stroke:#FF9900,color:#fff
        style A5 fill:#FF6B35,stroke:#232F3E,color:#fff
        
        style B1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style B2 fill:#3FB34F,stroke:#232F3E,color:#fff
        style B3 fill:#FF9900,stroke:#232F3E,color:#fff
        style B4 fill:#232F3E,stroke:#FF9900,color:#fff
        style B5 fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_etl_orchestration_mermaid():
    """Create mermaid diagram for ETL orchestration"""
    return """
    graph TD
        A[🚀 Trigger Event] --> B[📋 Step Function Start]
        B --> C[🔍 Validate Input Data]
        C --> D{Data Valid?}
        D -->|No| E[📧 Send Alert]
        D -->|Yes| F[⚙️ Start Glue Job]
        
        F --> G[🔧 Extract Data]
        G --> H[🔄 Transform Data]
        H --> I[📤 Load to Target]
        
        I --> J{Parallel Processing}
        J --> K[📊 Update Catalog]
        J --> L[📈 Run Analytics]
        J --> M[🔔 Send Success Notification]
        
        K --> N[✅ Pipeline Complete]
        L --> N
        M --> N
        
        E --> O[❌ Pipeline Failed]
        
        style A fill:#4B9EDB,stroke:#232F3E,color:#fff
        style B fill:#FF9900,stroke:#232F3E,color:#fff
        style F fill:#3FB34F,stroke:#232F3E,color:#fff
        style J fill:#4B9EDB,stroke:#232F3E,color:#fff
        style N fill:#232F3E,stroke:#FF9900,color:#fff
        style O fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_serverless_event_mermaid():
    """Create mermaid diagram for serverless event-driven workflow"""
    return """
    graph LR
        A[📁 S3 Put Event] --> B[☁️ CloudTrail]
        B --> C[📨 EventBridge]
        C --> D[🔧 AWS Glue Workflow]
        
        D --> E[📋 Crawler Start]
        E --> F[🗂️ Update Data Catalog]
        F --> G[⚙️ ETL Job Trigger]
        
        G --> H{Batch Condition}
        H -->|5 Files| I[🚀 Start Processing]
        H -->|15 Min Timer| I
        
        I --> J[🔄 Transform Data]
        J --> K[📊 Load to Data Warehouse]
        K --> L[📧 Success Notification]
        
        style A fill:#4B9EDB,stroke:#232F3E,color:#fff
        style B fill:#3FB34F,stroke:#232F3E,color:#fff
        style C fill:#FF9900,stroke:#232F3E,color:#fff
        style D fill:#232F3E,stroke:#FF9900,color:#fff
        style H fill:#4B9EDB,stroke:#232F3E,color:#fff
        style I fill:#3FB34F,stroke:#232F3E,color:#fff
    """

def create_orchestration_services_comparison():
    """Create comparison chart for orchestration services"""
    data = {
        'Service': ['AWS Step Functions', 'AWS Glue Workflows', 'Amazon MWAA', 'EventBridge Rules'],
        'Use Case': ['Complex workflows', 'Glue-centric ETL', 'Apache Airflow DAGs', 'Event routing'],
        'Complexity': [8, 5, 9, 3],
        'Flexibility': [9, 6, 10, 4],
        'AWS Integration': [10, 9, 8, 10],
        'Learning Curve': [6, 4, 8, 2],
        'Cost Model': ['Per transition', 'Per job run', 'Per hour', 'Per rule/event']
    }
    
    df = pd.DataFrame(data)
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Complexity vs Flexibility', 'AWS Integration', 'Learning Curve', 'Service Comparison'),
        specs=[[{"type": "scatter"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "table"}]]
    )
    
    # Complexity vs Flexibility scatter
    fig.add_trace(go.Scatter(
        x=df['Complexity'], 
        y=df['Flexibility'],
        text=df['Service'],
        mode='markers+text',
        textposition='top center',
        marker=dict(size=15, color=AWS_COLORS['primary']),
        name='Services'
    ), row=1, col=1)
    
    # AWS Integration bar
    fig.add_trace(go.Bar(
        x=df['Service'], 
        y=df['AWS Integration'],
        marker_color=AWS_COLORS['light_blue'],
        name='AWS Integration'
    ), row=1, col=2)
    
    # Learning Curve bar
    fig.add_trace(go.Bar(
        x=df['Service'], 
        y=df['Learning Curve'],
        marker_color=AWS_COLORS['warning'],
        name='Learning Curve'
    ), row=2, col=1)
    
    # Summary table
    fig.add_trace(go.Table(
        header=dict(values=['Service', 'Use Case', 'Cost Model'],
                   fill_color=AWS_COLORS['primary'],
                   font_color='white'),
        cells=dict(values=[df['Service'], df['Use Case'], df['Cost Model']],
                  fill_color=AWS_COLORS['light_gray'])
    ), row=2, col=2)
    
    fig.update_layout(height=700, showlegend=False, title_text="AWS Data Orchestration Services Comparison")
    
    return fig

def orchestration_fundamentals_tab():
    """Content for Orchestration Fundamentals tab"""
    st.markdown("# 🔗 Data Pipeline Orchestration")
    st.markdown("*Coordinate and manage complex data workflows at scale*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Data Pipeline Orchestration** is the coordination of multiple data processing tasks, managing their 
    execution order, dependencies, error handling, and monitoring. Think of it as the conductor of a data orchestra, 
    ensuring each instrument (service) plays at the right time in harmony.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Pipeline Overview
    st.markdown("## 🏗️ Data Pipeline Architecture Overview")
    common.mermaid(create_data_pipeline_overview_mermaid(), height=300)
    
    # Why Orchestrate?
    st.markdown("## 🤔 Why Orchestrate Data Pipelines?")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 Benefits of Orchestration
        
        **🔄 Workflow Management**
        - Automate complex, multi-step processes
        - Handle dependencies between tasks
        - Ensure proper execution order
        
        **⚡ Scalability & Reliability**
        - Scale processing based on demand
        - Built-in error handling and retries
        - Monitor pipeline health and performance
        
        **💰 Cost Optimization**
        - Pay only for resources when needed
        - Optimize resource allocation
        - Automated scheduling and scaling
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚫 Without Orchestration
        
        **😵 Manual Coordination**
        - Manual triggering of each step
        - Error-prone process management
        - Difficult to track progress
        
        **🐌 Resource Waste**
        - Always-on infrastructure
        - Over-provisioned resources
        - Manual scaling decisions
        
        **🔥 Operational Overhead**
        - Complex monitoring setup
        - Manual error recovery
        - Difficult troubleshooting
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Pipeline Builder
    st.markdown("## 🛠️ Interactive Pipeline Builder")
    
    # Pipeline configuration
    col1, col2, col3 = st.columns(3)
    
    with col1:
        data_source = st.selectbox("Data Source:", [
            "S3 Bucket", "RDS Database", "DynamoDB Table", "Kinesis Stream", "API Endpoint"
        ])
        
        trigger_type = st.selectbox("Trigger Type:", [
            "Schedule (Cron)", "Event-driven", "Manual", "API Call"
        ])
    
    with col2:
        processing_steps = st.multiselect("Processing Steps:", [
            "Data Validation", "Data Cleaning", "Data Transformation", 
            "Data Enrichment", "Data Aggregation", "Data Quality Checks"
        ], default=["Data Validation", "Data Transformation"])
        
        parallel_processing = st.checkbox("Enable Parallel Processing", value=True, key="parallel_processing")
    
    with col3:
        output_destinations = st.multiselect("Output Destinations:", [
            "Data Lake (S3)", "Data Warehouse (Redshift)", "Database (RDS)", 
            "Analytics (Athena)", "ML Model", "API Endpoint"
        ], default=["Data Lake (S3)"])
        
        monitoring = st.checkbox("Enable Monitoring & Alerts", value=True, key="monitoring")
    
    if st.button("🚀 Generate Pipeline Configuration", use_container_width=True):
        
        # Generate estimated metrics
        complexity_score = len(processing_steps) * 2 + len(output_destinations)
        estimated_cost = complexity_score * 0.15 + (0.25 if parallel_processing else 0)
        execution_time = max(5, complexity_score * 3 - (2 if parallel_processing else 0))
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Pipeline Configuration Generated!
        
        **Pipeline Details:**
        - **Data Source**: {data_source}
        - **Trigger**: {trigger_type}
        - **Processing Steps**: {len(processing_steps)} steps configured
        - **Parallel Processing**: {'Enabled' if parallel_processing else 'Disabled'}
        - **Output Destinations**: {len(output_destinations)} targets
        - **Monitoring**: {'Enabled' if monitoring else 'Disabled'}
        
        **Estimated Metrics:**
        - **Complexity Score**: {complexity_score}/10
        - **Est. Execution Time**: {execution_time} minutes
        - **Est. Cost per Run**: ${estimated_cost:.2f}
        - **Recommended Service**: AWS Step Functions
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Common Orchestration Patterns
    st.markdown("## 🔄 Common Orchestration Patterns")
    
    patterns_data = {
        'Pattern': ['Sequential', 'Parallel', 'Conditional', 'Fan-out/Fan-in', 'Retry Logic'],
        'Description': [
            'Execute tasks one after another',
            'Execute multiple tasks simultaneously',
            'Branch execution based on conditions',
            'Distribute work then combine results',
            'Automatically retry failed tasks'
        ],
        'Use Case': [
            'ETL with dependencies',
            'Independent data processing',
            'Data quality validation',
            'Multi-source data aggregation',
            'Handling transient failures'
        ],
        'AWS Service': [
            'Step Functions Sequential',
            'Step Functions Parallel',
            'Step Functions Choice',
            'Step Functions Map',
            'Step Functions Retry'
        ]
    }
    
    df_patterns = pd.DataFrame(patterns_data)
    
    # Create interactive table
    st.dataframe(
        df_patterns,
        use_container_width=True,
        hide_index=True
    )
    
    # Real-world Example
    st.markdown("## 🌟 Real-world Example: E-commerce Analytics Pipeline")
    
    st.markdown('<div class="architecture-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🛒 Scenario: Daily Sales Analytics
    
    **Business Requirement**: Process daily sales data from multiple sources and generate business insights
    
    **Pipeline Steps:**
    1. **🕐 6:00 AM** - Scheduled trigger fires
    2. **📥 Extract** - Collect data from:
       - Sales database (RDS)
       - Web analytics (S3 logs)
       - Customer data (DynamoDB)
    3. **🔄 Transform** - Clean and join data using AWS Glue
    4. **📊 Load** - Store in data warehouse (Redshift)
    5. **📈 Analyze** - Run analytics queries (Athena)
    6. **📧 Notify** - Send reports to stakeholders
    
    **Orchestration Benefits:**
    - ✅ Fully automated daily execution
    - ✅ Error handling with automatic retries
    - ✅ Parallel processing reduces execution time
    - ✅ Cost optimization through serverless services
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Basic Orchestration Concepts")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Basic data pipeline orchestration concepts
import boto3
import json
from datetime import datetime

# Initialize AWS clients
stepfunctions = boto3.client('stepfunctions')
glue = boto3.client('glue')

# Define pipeline configuration
pipeline_config = {
    "name": "daily-sales-pipeline",
    "schedule": "cron(0 6 * * ? *)",  # Daily at 6 AM
    "steps": [
        {
            "name": "extract-sales-data",
            "type": "glue-job",
            "job_name": "extract-sales",
            "retry_attempts": 3
        },
        {
            "name": "validate-data-quality",
            "type": "lambda-function",
            "function_name": "data-quality-check",
            "timeout": 300
        },
        {
            "name": "transform-and-load",
            "type": "parallel",
            "branches": [
                {
                    "name": "transform-sales",
                    "type": "glue-job",
                    "job_name": "transform-sales-data"
                },
                {
                    "name": "enrich-customer-data",
                    "type": "glue-job", 
                    "job_name": "enrich-customer-data"
                }
            ]
        },
        {
            "name": "generate-analytics",
            "type": "athena-query",
            "query_file": "s3://my-bucket/queries/daily-analytics.sql"
        },
        {
            "name": "send-notification",
            "type": "sns-notification",
            "topic_arn": "arn:aws:sns:us-east-1:123456789:pipeline-notifications"
        }
    ]
}

def create_state_machine_definition(config):
    """
    Convert pipeline configuration to Step Functions state machine
    """
    definition = {
        "Comment": f"Data pipeline: {config['name']}",
        "StartAt": "StartPipeline",
        "States": {
            "StartPipeline": {
                "Type": "Pass",
                "Result": {"status": "starting", "timestamp": ""},
                "Next": "ExtractSalesData"
            },
            "ExtractSalesData": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "extract-sales"
                },
                "Retry": [
                    {
                        "ErrorEquals": ["States.TaskFailed"],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 3,
                        "BackoffRate": 2.0
                    }
                ],
                "Next": "ValidateDataQuality"
            },
            "ValidateDataQuality": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "data-quality-check"
                },
                "Next": "ParallelProcessing"
            },
            "ParallelProcessing": {
                "Type": "Parallel",
                "Branches": [
                    {
                        "StartAt": "TransformSales",
                        "States": {
                            "TransformSales": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": "transform-sales-data"
                                },
                                "End": True
                            }
                        }
                    },
                    {
                        "StartAt": "EnrichCustomerData", 
                        "States": {
                            "EnrichCustomerData": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": "enrich-customer-data"
                                },
                                "End": True
                            }
                        }
                    }
                ],
                "Next": "GenerateAnalytics"
            },
            "GenerateAnalytics": {
                "Type": "Task",
                "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
                "Parameters": {
                    "QueryString": "SELECT * FROM daily_sales_summary",
                    "ResultConfiguration": {
                        "OutputLocation": "s3://my-analytics-bucket/results/"
                    }
                },
                "Next": "SendNotification"
            },
            "SendNotification": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": "arn:aws:sns:us-east-1:123456789:pipeline-notifications",
                    "Message": "Daily sales pipeline completed successfully!"
                },
                "End": True
            }
        }
    }
    
    return json.dumps(definition, indent=2)

# Create the state machine
definition = create_state_machine_definition(pipeline_config)
print("State Machine Definition Created:")
print(definition)

# Deploy the state machine (example)
response = stepfunctions.create_state_machine(
    name=pipeline_config['name'],
    definition=definition,
    roleArn='arn:aws:iam::123456789:role/StepFunctionsExecutionRole'
)

print(f"State machine created: {response['stateMachineArn']}")
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def step_functions_orchestration_tab():
    """Content for Step Functions orchestration tab"""
    st.markdown("# 🛠️ AWS Step Functions Data Orchestration")
    st.markdown("*Coordinate distributed applications and microservices using visual workflows*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **AWS Step Functions** lets you coordinate multiple AWS services into serverless workflows so you can build and 
    update apps quickly. It's like having a visual flowchart that automatically executes your data pipeline steps.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Step Functions Architecture
    st.markdown("## 🏗️ Step Functions Architecture for Data Pipelines")
    common.mermaid(create_step_functions_architecture_mermaid(), height=400)
    
    # Workflow Types Comparison
    st.markdown("## ⚖️ Workflow Types Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="service-comparison">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Standard Workflows
        
        **Best for Data Engineering:**
        - Long-running ETL jobs
        - Complex data transformations
        - Detailed audit requirements
        - Sequential processing with checkpoints
        
        **Characteristics:**
        - ⏱️ Up to 1 year execution time
        - 🎯 Exactly-once execution
        - 📋 Full execution history
        - 💰 $0.025 per state transition
        
        **Use Cases:**
        - Daily/weekly batch processing
        - Data migration projects
        - Complex analytics workflows
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="service-comparison">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚡ Express Workflows
        
        **Best for Real-time Processing:**
        - Streaming data processing
        - High-volume event processing
        - Real-time analytics
        - IoT data ingestion
        
        **Characteristics:**
        - ⚡ Up to 5 minutes execution time
        - 🔄 At-least-once execution
        - 📝 CloudWatch Logs only
        - 💸 $0.000001 per execution
        
        **Use Cases:**
        - Real-time data transformation
        - Event-driven processing
        - Microservice orchestration
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Workflow Types Visual Comparison
    st.markdown("## 📊 Workflow Types Detailed Comparison")
    common.mermaid(create_workflow_types_mermaid(), height=300)
    
    # Interactive Workflow Builder
    st.markdown("## 🔧 Interactive Step Functions Workflow Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        workflow_name = st.text_input("Workflow Name:", "data-processing-workflow")
        workflow_type = st.selectbox("Workflow Type:", [
            "Standard (Long-running ETL)", 
            "Express (Real-time Processing)"
        ])
        
        trigger_source = st.selectbox("Trigger Source:", [
            "EventBridge Schedule", "S3 Event", "API Gateway", "Manual Execution", "Lambda Function"
        ])
    
    with col2:
        data_services = st.multiselect("AWS Data Services:", [
            "AWS Glue", "Amazon EMR", "AWS Lambda", "Amazon Athena", 
            "AWS Batch", "Amazon Redshift", "Amazon Kinesis"
        ], default=["AWS Glue", "Amazon Athena"])
        
        error_handling = st.multiselect("Error Handling:", [
            "Retry Logic", "Catch Errors", "Fallback Actions", "Dead Letter Queue"
        ], default=["Retry Logic", "Catch Errors"])
    
    # Advanced Configuration
    st.markdown("### Advanced Configuration")
    
    col3, col4 = st.columns(2)
    
    with col3:
        parallel_execution = st.checkbox("Enable Parallel Processing", value=True)
        conditional_logic = st.checkbox("Include Conditional Branches", value=False)
        
    with col4:
        max_retries = st.slider("Max Retry Attempts:", 0, 5, 3)
        timeout_minutes = st.slider("Workflow Timeout (minutes):", 5, 300, 60)
    
    if st.button("🚀 Generate Step Functions Definition", use_container_width=True):
        
        # Calculate estimated metrics
        complexity_score = len(data_services) * 2 + len(error_handling)
        if parallel_execution:
            complexity_score += 2
        if conditional_logic:
            complexity_score += 3
            
        est_transitions = complexity_score * 5
        est_cost_standard = est_transitions * 0.000025
        est_cost_express = est_transitions * 0.000001
        
        workflow_is_standard = "Standard" in workflow_type
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Step Functions Workflow Generated!
        
        **Workflow Configuration:**
        - **Name**: {workflow_name}
        - **Type**: {workflow_type}
        - **Trigger**: {trigger_source}
        - **Services**: {', '.join(data_services)}
        - **Error Handling**: {', '.join(error_handling)}
        - **Parallel Processing**: {'Enabled' if parallel_execution else 'Disabled'}
        - **Max Retries**: {max_retries}
        - **Timeout**: {timeout_minutes} minutes
        
        **Estimated Metrics:**
        - **Complexity Score**: {complexity_score}/15
        - **Est. State Transitions**: {est_transitions}
        - **Est. Cost per Execution**: ${est_cost_standard:.4f} (Standard) / ${est_cost_express:.6f} (Express)
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Show sample definition
        st.markdown("### 📋 Sample State Machine Definition")
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code(f'''
{{
  "Comment": "{workflow_name} - Generated workflow",
  "StartAt": "StartProcessing",
  "States": {{
    "StartProcessing": {{
      "Type": "Pass",
      "Result": {{"status": "started"}},
      "Next": "{'ParallelProcessing' if parallel_execution else 'ProcessData'}"
    }},
    {"ParallelProcessing" if parallel_execution else "ProcessData"}: {{
      "Type": "{'Parallel' if parallel_execution else 'Task'}",
      {"Branches" if parallel_execution else "Resource"}: {
        f'''[
        {{
          "StartAt": "{data_services[0] if data_services else 'DefaultTask'}",
          "States": {{
            "{data_services[0] if data_services else 'DefaultTask'}": {{
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "End": true
            }}
          }}
        }}
      ]''' if parallel_execution else '"arn:aws:states:::glue:startJobRun.sync"'
      },
      {"" if parallel_execution else f'"Retry": [{{ "ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 30, "MaxAttempts": {max_retries} }}],'}
      "End": true
    }}
  }}
}}
        ''', language='json')
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Step Functions Features for Data Engineering
    st.markdown("## 🎯 Step Functions Features for Data Engineering")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔧 Service Integrations
        
        **Direct Integrations:**
        - AWS Glue (ETL Jobs)
        - Amazon EMR (Big Data)
        - AWS Batch (Compute Jobs)
        - Amazon Athena (Queries)
        - AWS Lambda (Functions)
        - Amazon SNS/SQS (Messaging)
        
        **Benefits:**
        - No glue code needed
        - Built-in error handling
        - Automatic retries
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎮 State Types
        
        **Task States:**
        - Execute work
        - Call AWS services
        - Run Lambda functions
        
        **Flow Control:**
        - Choice (conditional)
        - Parallel (concurrent)
        - Map (iterate)
        - Wait (delay)
        
        **Error Handling:**
        - Catch exceptions
        - Retry with backoff
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Monitoring & Debugging
        
        **Built-in Features:**
        - Visual execution history
        - State-by-state debugging
        - CloudWatch integration
        - X-Ray tracing
        
        **Alerting:**
        - Execution failures
        - Performance metrics
        - Custom notifications
        - Cost monitoring
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: ETL Pipeline with Step Functions")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Complete ETL pipeline orchestration with Step Functions
import boto3
import json

stepfunctions = boto3.client('stepfunctions')

# Define comprehensive ETL workflow
etl_state_machine = {
    "Comment": "Complete ETL Pipeline with Step Functions",
    "StartAt": "ValidateInputs",
    "States": {
        "ValidateInputs": {
            "Type": "Task", 
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": "validate-pipeline-inputs",
                "Payload": {
                    "inputBucket.$": "$.inputBucket",
                    "inputPrefix.$": "$.inputPrefix"
                }
            },
            "ResultPath": "$.validation",
            "Next": "CheckValidation"
        },
        
        "CheckValidation": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.validation.Payload.isValid",
                    "BooleanEquals": true,
                    "Next": "ParallelExtraction"
                }
            ],
            "Default": "ValidationFailed"
        },
        
        "ValidationFailed": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
                "TopicArn": "arn:aws:sns:us-east-1:123456789:pipeline-alerts",
                "Message": "Pipeline validation failed - check input data"
            },
            "End": True
        },
        
        "ParallelExtraction": {
            "Type": "Parallel",
            "Branches": [
                {
                    "StartAt": "ExtractSalesData",
                    "States": {
                        "ExtractSalesData": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "extract-sales-data",
                                "Arguments": {
                                    "--input-path.$": "$.inputBucket",
                                    "--output-path": "s3://data-lake/raw/sales/"
                                }
                            },
                            "Retry": [
                                {
                                    "ErrorEquals": [
                                        "Glue.AWSGlueException",
                                        "States.TaskFailed"
                                    ],
                                    "IntervalSeconds": 30,
                                    "MaxAttempts": 3,
                                    "BackoffRate": 2.0
                                }
                            ],
                            "Catch": [
                                {
                                    "ErrorEquals": ["States.ALL"],
                                    "Next": "HandleExtractionError",
                                    "ResultPath": "$.error"
                                }
                            ],
                            "End": True
                        },
                        "HandleExtractionError": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::sns:publish",
                            "Parameters": {
                                "TopicArn": "arn:aws:sns:us-east-1:123456789:pipeline-alerts",
                                "Message.$": "States.Format('Sales extraction failed: {}', $.error.Cause)"
                            },
                            "End": True
                        }
                    }
                },
                {
                    "StartAt": "ExtractCustomerData", 
                    "States": {
                        "ExtractCustomerData": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "extract-customer-data",
                                "Arguments": {
                                    "--database-connection": "customer-db-connection",
                                    "--output-path": "s3://data-lake/raw/customers/"
                                }
                            },
                            "Retry": [
                                {
                                    "ErrorEquals": ["States.TaskFailed"],
                                    "IntervalSeconds": 30,
                                    "MaxAttempts": 3,
                                    "BackoffRate": 2.0
                                }
                            ],
                            "End": True
                        }
                    }
                }
            ],
            "ResultPath": "$.extractionResults",
            "Next": "DataQualityCheck"
        },
        
        "DataQualityCheck": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync", 
            "Parameters": {
                "JobName": "data-quality-validation",
                "Arguments": {
                    "--input-path": "s3://data-lake/raw/",
                    "--quality-rules": "s3://config/quality-rules.json"
                }
            },
            "ResultPath": "$.qualityCheck",
            "Next": "CheckDataQuality"
        },
        
        "CheckDataQuality": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.qualityCheck.JobRunState",
                    "StringEquals": "SUCCEEDED",
                    "Next": "TransformData"
                }
            ],
            "Default": "QualityCheckFailed"
        },
        
        "QualityCheckFailed": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
                "TopicArn": "arn:aws:sns:us-east-1:123456789:pipeline-alerts", 
                "Subject": "Data Quality Check Failed",
                "Message": "Data quality validation failed - pipeline halted"
            },
            "End": True
        },
        
        "TransformData": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": "transform-and-join-data",
                "Arguments": {
                    "--input-path": "s3://data-lake/raw/",
                    "--output-path": "s3://data-lake/processed/",
                    "--transformation-script": "s3://scripts/transform-sales-data.py"
                }
            },
            "ResultPath": "$.transformResult",
            "Next": "LoadToWarehouse"
        },
        
        "LoadToWarehouse": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": "load-to-redshift",
                "Arguments": {
                    "--source-path": "s3://data-lake/processed/",
                    "--redshift-connection": "redshift-connection",
                    "--target-table": "sales_fact"
                }
            },
            "ResultPath": "$.loadResult",
            "Next": "UpdateAnalytics"
        },
        
        "UpdateAnalytics": {
            "Type": "Parallel",
            "Branches": [
                {
                    "StartAt": "RefreshDashboard",
                    "States": {
                        "RefreshDashboard": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "Parameters": {
                                "FunctionName": "refresh-quicksight-dashboard"
                            },
                            "End": True
                        }
                    }
                },
                {
                    "StartAt": "UpdateDataCatalog",
                    "States": {
                        "UpdateDataCatalog": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startCrawler",
                            "Parameters": {
                                "CrawlerName": "processed-data-crawler"
                            },
                            "End": True
                        }
                    }
                }
            ],
            "Next": "PipelineSuccess"
        },
        
        "PipelineSuccess": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
                "TopicArn": "arn:aws:sns:us-east-1:123456789:pipeline-notifications",
                "Subject": "ETL Pipeline Completed Successfully",
                "Message.$": "States.Format('Pipeline completed at {}. Processed {} records.', $$.State.EnteredTime, $.transformResult.Arguments.recordCount)"
            },
            "End": True
        }
    }
}

# Create state machine
def create_etl_state_machine():
    try:
        response = stepfunctions.create_state_machine(
            name='comprehensive-etl-pipeline',
            definition=json.dumps(etl_state_machine),
            roleArn='arn:aws:iam::123456789:role/StepFunctionsETLRole',
            type='STANDARD',  # Use STANDARD for long-running ETL
            tags=[
                {
                    'key': 'Environment',
                    'value': 'Production'
                },
                {
                    'key': 'Purpose', 
                    'value': 'ETL-Pipeline'
                }
            ]
        )
        
        print(f"State machine created successfully!")
        print(f"ARN: {response['stateMachineArn']}")
        return response['stateMachineArn']
        
    except Exception as e:
        print(f"Error creating state machine: {e}")
        return None

# Execute the pipeline
def start_etl_pipeline(state_machine_arn, input_data):
    try:
        response = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            name=f"etl-execution-{int(time.time())}",
            input=json.dumps(input_data)
        )
        
        print(f"Pipeline execution started!")
        print(f"Execution ARN: {response['executionArn']}")
        return response['executionArn']
        
    except Exception as e:
        print(f"Error starting execution: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Create the state machine
    state_machine_arn = create_etl_state_machine()
    
    if state_machine_arn:
        # Start pipeline execution with input parameters
        input_data = {
            "inputBucket": "s3://raw-data-bucket",
            "inputPrefix": "sales/2024/07/14/",
            "executionDate": "2024-07-14"
        }
        
        execution_arn = start_etl_pipeline(state_machine_arn, input_data)
        
        if execution_arn:
            print("✅ ETL pipeline is now running with Step Functions orchestration!")
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def etl_orchestration_tab():
    """Content for ETL Pipeline Orchestration tab"""
    st.markdown("# 🔄 ETL Pipeline Orchestration")
    st.markdown("*Automate and coordinate Extract, Transform, Load workflows at scale*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **ETL Pipeline Orchestration** automates the entire data processing workflow from extraction to loading,
    ensuring data flows smoothly through each transformation stage with proper error handling, monitoring, and recovery.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ETL Orchestration Architecture
    st.markdown("## 🏗️ ETL Orchestration Architecture")
    common.mermaid(create_etl_orchestration_mermaid(), height=450)
    
    # ETL Pipeline Components
    st.markdown("## 🧩 ETL Pipeline Components")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📥 Extract Phase
        
        **Data Sources:**
        - Relational databases (RDS, Aurora)
        - NoSQL databases (DynamoDB)
        - File systems (S3, EFS)
        - APIs and web services
        - Streaming data (Kinesis)
        
        **Challenges:**
        - Different data formats
        - Connection management
        - Rate limiting
        - Data volume handling
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Transform Phase
        
        **Operations:**
        - Data cleaning & validation
        - Format conversions
        - Data enrichment
        - Aggregations & calculations
        - Business rule application
        
        **Tools:**
        - AWS Glue (Apache Spark)
        - Amazon EMR
        - AWS Lambda
        - Custom applications
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📤 Load Phase
        
        **Destinations:**
        - Data warehouses (Redshift)
        - Data lakes (S3)
        - Analytics databases
        - BI tools (QuickSight)
        - Machine learning models
        
        **Patterns:**
        - Full load vs incremental
        - Batch vs streaming
        - Parallel loading
        - Error handling
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive ETL Builder
    st.markdown("## 🛠️ Interactive ETL Pipeline Builder")
    
    # Extract Configuration
    st.markdown("### 📥 Extract Configuration")
    col1, col2 = st.columns(2)
    
    with col1:
        source_systems = st.multiselect("Source Systems:", [
            "MySQL RDS Database", "PostgreSQL Aurora", "DynamoDB Table",
            "S3 Data Files", "REST API", "Kinesis Stream", "Redshift Table"
        ], default=["MySQL RDS Database", "S3 Data Files"])
        
        extraction_pattern = st.selectbox("Extraction Pattern:", [
            "Full Extract", "Incremental (CDC)", "Delta Load", "API Pagination"
        ])
    
    with col2:
        data_formats = st.multiselect("Data Formats:", [
            "CSV", "JSON", "Parquet", "Avro", "ORC", "XML", "Fixed-width"
        ], default=["CSV", "JSON"])
        
        schedule_frequency = st.selectbox("Schedule Frequency:", [
            "Real-time (Streaming)", "Every 15 minutes", "Hourly", "Daily", "Weekly"
        ])
    
    # Transform Configuration
    st.markdown("### 🔄 Transform Configuration")
    col3, col4 = st.columns(2)
    
    with col3:
        transformations = st.multiselect("Transformations:", [
            "Data Cleansing", "Data Validation", "Format Conversion", 
            "Data Enrichment", "Aggregation", "Join Operations", "Deduplication"
        ], default=["Data Cleansing", "Data Validation"])
        
        processing_engine = st.selectbox("Processing Engine:", [
            "AWS Glue (Spark)", "Amazon EMR", "AWS Lambda", "AWS Batch"
        ])
    
    with col4:
        data_quality_checks = st.multiselect("Data Quality Checks:", [
            "Null Value Detection", "Data Type Validation", "Range Checks",
            "Referential Integrity", "Duplicate Detection", "Schema Validation"
        ], default=["Null Value Detection", "Data Type Validation"])
        
        error_handling = st.selectbox("Error Handling:", [
            "Fail Fast", "Skip Invalid Records", "Quarantine Bad Data", "Retry with Backoff"
        ])
    
    # Load Configuration  
    st.markdown("### 📤 Load Configuration")
    col5, col6 = st.columns(2)
    
    with col5:
        target_systems = st.multiselect("Target Systems:", [
            "Amazon Redshift", "S3 Data Lake", "RDS Database", "DynamoDB", 
            "ElasticSearch", "QuickSight Dataset"
        ], default=["Amazon Redshift", "S3 Data Lake"])
        
        load_strategy = st.selectbox("Load Strategy:", [
            "Replace (Truncate & Load)", "Append Only", "Upsert (Merge)", "SCD Type 2"
        ])
    
    with col6:
        optimization_features = st.multiselect("Optimization Features:", [
            "Partitioning", "Compression", "Parallel Loading", "Indexing",
            "Columnar Storage", "Data Distribution"
        ], default=["Partitioning", "Compression"])
        
        monitoring_alerts = st.checkbox("Enable Monitoring & Alerts", value=True)
    
    if st.button("🚀 Generate ETL Pipeline Orchestration", use_container_width=True):
        
        # Calculate pipeline metrics
        complexity_score = (len(source_systems) * 2 + len(transformations) * 1.5 + 
                          len(target_systems) * 1.5 + len(data_quality_checks))
        
        estimated_duration = max(15, complexity_score * 5)
        if processing_engine == "AWS Glue (Spark)":
            estimated_cost = complexity_score * 0.44  # Glue DPU cost
        elif processing_engine == "Amazon EMR":
            estimated_cost = complexity_score * 0.96  # EMR instance cost  
        else:
            estimated_cost = complexity_score * 0.20  # Lambda/Batch cost
            
        sla_reliability = max(95, 99.5 - (complexity_score * 0.5))
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ ETL Pipeline Orchestration Generated!
        
        **Pipeline Configuration:**
        - **Sources**: {len(source_systems)} systems ({', '.join(source_systems[:2])}{'...' if len(source_systems) > 2 else ''})
        - **Transformations**: {len(transformations)} operations
        - **Targets**: {len(target_systems)} destinations  
        - **Processing Engine**: {processing_engine}
        - **Schedule**: {schedule_frequency}
        - **Error Handling**: {error_handling}
        
        **Performance Estimates:**
        - **Complexity Score**: {complexity_score:.1f}/20
        - **Estimated Duration**: {estimated_duration} minutes
        - **Estimated Cost**: ${estimated_cost:.2f} per run
        - **SLA Reliability**: {sla_reliability:.1f}%
        
        **Recommended Orchestrator**: AWS Step Functions (Standard Workflow)
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Show orchestration pattern
        st.markdown("### 🔗 Generated Orchestration Pattern")
        steps = ["Start"] + [f"Extract from {s.split()[0]}" for s in source_systems[:2]]
        steps += [f"Transform: {t}" for t in transformations[:2]]  
        steps += [f"Load to {t.split()[0]}" for t in target_systems[:2]]
        steps += ["Monitor & Alert", "Complete"]
        
        pattern_text = " → ".join(steps)
        st.info(f"**Execution Flow**: {pattern_text}")
    
    # ETL Best Practices
    st.markdown("## 💡 ETL Orchestration Best Practices")
    
    best_practices_data = {
        'Category': ['Data Quality', 'Performance', 'Reliability', 'Security', 'Monitoring'],
        'Best Practice': [
            'Implement comprehensive data validation',
            'Use parallel processing and optimize data formats',
            'Include retry logic and graceful error handling',
            'Encrypt data in transit and at rest',
            'Set up comprehensive logging and alerting'
        ],
        'Impact': ['High', 'High', 'Critical', 'Critical', 'Medium'],
        'Implementation': [
            'Data quality checks at each stage',
            'Partition data, use columnar formats',
            'Circuit breakers, dead letter queues',
            'IAM roles, encryption keys',
            'CloudWatch dashboards, SNS alerts'
        ]
    }
    
    df_practices = pd.DataFrame(best_practices_data)
    
    # Create best practices visualization
    fig = px.treemap(
        df_practices, 
        path=[px.Constant("ETL Best Practices"), 'Category', 'Best Practice'],
        values=[1]*len(df_practices),
        color='Impact',
        color_discrete_map={
            'Critical': AWS_COLORS['warning'],
            'High': AWS_COLORS['primary'], 
            'Medium': AWS_COLORS['light_blue']
        },
        title="ETL Best Practices Hierarchy"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Common ETL Patterns
    st.markdown("## 🔄 Common ETL Orchestration Patterns")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="architecture-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏭 Batch ETL Pattern
        
        **Characteristics:**
        - Scheduled execution (daily, weekly)
        - Large data volumes
        - Complex transformations
        - Full data refresh
        
        **AWS Services:**
        - Step Functions (Standard)
        - AWS Glue Jobs
        - Amazon EMR
        - Amazon Redshift
        
        **Use Cases:**
        - Daily sales reporting
        - Data warehouse updates
        - Compliance reporting
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="architecture-card">', unsafe_allow_html=True)
        st.markdown("""
        ### ⚡ Real-time ETL Pattern
        
        **Characteristics:**
        - Event-driven execution
        - Small data batches
        - Low latency requirements
        - Incremental processing
        
        **AWS Services:**
        - Step Functions (Express)
        - AWS Lambda
        - Kinesis Data Streams
        - DynamoDB Streams
        
        **Use Cases:**
        - Real-time analytics
        - Fraud detection
        - IoT data processing
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Complete ETL Orchestration")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Complete ETL pipeline with CSV to Parquet transformation
import boto3
import json
from datetime import datetime, timedelta

# Initialize AWS clients
stepfunctions = boto3.client('stepfunctions')
s3 = boto3.client('s3')
glue = boto3.client('glue')

def create_etl_orchestration_pipeline():
    """
    Create a comprehensive ETL pipeline that:
    1. Extracts CSV data from S3
    2. Transforms and validates data using Glue
    3. Converts to Parquet format
    4. Loads to data warehouse
    5. Updates data catalog
    6. Sends notifications
    """
    
    state_machine_definition = {
        "Comment": "CSV to Parquet ETL Pipeline with Data Quality Checks",
        "StartAt": "ValidateInputData",
        "States": {
            # Input validation
            "ValidateInputData": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "validate-csv-files",
                    "Payload": {
                        "sourceBucket.$": "$.sourceBucket",
                        "sourcePrefix.$": "$.sourcePrefix",
                        "expectedFileCount.$": "$.expectedFileCount"
                    }
                },
                "ResultPath": "$.validation",
                "Next": "CheckValidationResult"
            },
            
            # Conditional logic based on validation
            "CheckValidationResult": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.validation.Payload.isValid",
                        "BooleanEquals": True,
                        "Next": "StartDataProfiling"
                    }
                ],
                "Default": "NotifyValidationFailure"
            },
            
            # Data profiling before transformation
            "StartDataProfiling": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "data-profiling-job",
                    "Arguments": {
                        "--input-path.$": "States.Format('s3://{}/{}', $.sourceBucket, $.sourcePrefix)",
                        "--output-path": "s3://data-quality-reports/profiling/",
                        "--job-bookmark-option": "job-bookmark-enable"
                    }
                },
                "ResultPath": "$.profilingResult",
                "Retry": [
                    {
                        "ErrorEquals": ["Glue.AWSGlueException"],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 3,
                        "BackoffRate": 2.0
                    }
                ],
                "Next": "ParallelETLProcessing"
            },
            
            # Parallel processing for better performance
            "ParallelETLProcessing": {
                "Type": "Parallel",
                "Branches": [
                    {
                        "StartAt": "TransformSalesData",
                        "States": {
                            "TransformSalesData": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": "transform-sales-csv-to-parquet",
                                    "Arguments": {
                                        "--input-path.$": "States.Format('s3://{}/{}/sales/', $.sourceBucket, $.sourcePrefix)",
                                        "--output-path": "s3://data-lake/processed/sales/",
                                        "--enable-metrics": "true",
                                        "--enable-continuous-cloudwatch-log": "true"
                                    }
                                },
                                "Retry": [
                                    {
                                        "ErrorEquals": ["States.TaskFailed"],
                                        "IntervalSeconds": 60,
                                        "MaxAttempts": 2,
                                        "BackoffRate": 2.0
                                    }
                                ],
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
                                    "JobName": "transform-customer-csv-to-parquet",
                                    "Arguments": {
                                        "--input-path.$": "States.Format('s3://{}/{}/customers/', $.sourceBucket, $.sourcePrefix)",
                                        "--output-path": "s3://data-lake/processed/customers/",
                                        "--enable-partitioning": "true",
                                        "--partition-keys": "year,month"
                                    }
                                },
                                "End": True
                            }
                        }
                    },
                    {
                        "StartAt": "TransformProductData",
                        "States": {
                            "TransformProductData": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": "transform-product-csv-to-parquet",
                                    "Arguments": {
                                        "--input-path.$": "States.Format('s3://{}/{}/products/', $.sourceBucket, $.sourcePrefix)",
                                        "--output-path": "s3://data-lake/processed/products/",
                                        "--compression-type": "snappy"
                                    }
                                },
                                "End": True
                            }
                        }
                    }
                ],
                "ResultPath": "$.transformResults",
                "Next": "DataQualityValidation"
            },
            
            # Data quality validation after transformation
            "DataQualityValidation": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "data-quality-validation",
                    "Arguments": {
                        "--input-path": "s3://data-lake/processed/",
                        "--quality-rules-config": "s3://config/data-quality-rules.json",
                        "--validation-report-path": "s3://data-quality-reports/validation/"
                    }
                },
                "ResultPath": "$.qualityValidation",
                "Next": "CheckDataQuality"
            },
            
            # Check data quality results
            "CheckDataQuality": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.qualityValidation.JobRunState",
                        "StringEquals": "SUCCEEDED",
                        "Next": "UpdateDataCatalog"
                    }
                ],
                "Default": "HandleQualityFailure"
            },
            
            # Update Glue Data Catalog
            "UpdateDataCatalog": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startCrawler",
                "Parameters": {
                    "CrawlerName": "processed-data-crawler"
                },
                "ResultPath": "$.catalogUpdate",
                "Next": "WaitForCrawler"
            },
            
            # Wait for crawler to complete
            "WaitForCrawler": {
                "Type": "Wait",
                "Seconds": 60,
                "Next": "CheckCrawlerStatus"
            },
            
            "CheckCrawlerStatus": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
                "Parameters": {
                    "Name": "processed-data-crawler"
                },
                "ResultPath": "$.crawlerStatus",
                "Next": "IsCrawlerComplete"
            },
            
            "IsCrawlerComplete": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.crawlerStatus.Crawler.State",
                        "StringEquals": "READY",
                        "Next": "LoadToDataWarehouse"
                    }
                ],
                "Default": "WaitForCrawler"
            },
            
            # Load processed data to Redshift
            "LoadToDataWarehouse": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "load-to-redshift",
                    "Arguments": {
                        "--source-path": "s3://data-lake/processed/",
                        "--redshift-connection": "redshift-data-warehouse",
                        "--load-type": "UPSERT",
                        "--parallel-load": "true"
                    }
                },
                "ResultPath": "$.warehouseLoad",
                "Next": "PostProcessingTasks"
            },
            
            # Post-processing parallel tasks
            "PostProcessingTasks": {
                "Type": "Parallel",
                "Branches": [
                    {
                        "StartAt": "GenerateDataLineage",
                        "States": {
                            "GenerateDataLineage": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::lambda:invoke",
                                "Parameters": {
                                    "FunctionName": "generate-data-lineage-report"
                                },
                                "End": True
                            }
                        }
                    },
                    {
                        "StartAt": "UpdateBusinessMetrics",
                        "States": {
                            "UpdateBusinessMetrics": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
                                "Parameters": {
                                    "QueryString": "CALL update_business_metrics_procedure()",
                                    "WorkGroup": "data-analytics-workgroup"
                                },
                                "End": True
                            }
                        }
                    },
                    {
                        "StartAt": "RefreshDashboards",
                        "States": {
                            "RefreshDashboards": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::lambda:invoke",
                                "Parameters": {
                                    "FunctionName": "refresh-quicksight-dashboards"
                                },
                                "End": True
                            }
                        }
                    }
                ],
                "Next": "SendSuccessNotification"
            },
            
            # Success notification
            "SendSuccessNotification": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": "arn:aws:sns:us-east-1:123456789:etl-notifications",
                    "Subject": "ETL Pipeline Completed Successfully",
                    "Message.$": "States.Format('ETL pipeline completed successfully at {}.\\n\\nProcessing Summary:\\n- Files processed: {}\\n- Records transformed: {}\\n- Data quality score: {}%\\n- Execution time: {} minutes', $$.State.EnteredTime, $.validation.Payload.fileCount, $.transformResults[0].Arguments.recordCount, $.qualityValidation.Arguments.qualityScore, $$.Execution.ElapsedTime)"
                },
                "End": True
            },
            
            # Error handling states
            "NotifyValidationFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": "arn:aws:sns:us-east-1:123456789:etl-alerts",
                    "Subject": "ETL Pipeline - Input Validation Failed",
                    "Message.$": "States.Format('Input validation failed: {}', $.validation.Payload.errorMessage)"
                },
                "End": True
            },
            
            "HandleQualityFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": "arn:aws:sns:us-east-1:123456789:etl-alerts",
                    "Subject": "ETL Pipeline - Data Quality Check Failed",
                    "Message": "Data quality validation failed. Please check the quality report for details."
                },
                "Next": "QuarantineBadData"
            },
            
            "QuarantineBadData": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "quarantine-failed-data",
                    "Payload": {
                        "sourcePath": "s3://data-lake/processed/",
                        "quarantinePath": "s3://data-quarantine/"
                    }
                },
                "End": True
            }
        }
    }
    
    return state_machine_definition

# Deploy and execute the ETL pipeline
def deploy_etl_pipeline():
    """Deploy the ETL pipeline state machine"""
    
    definition = create_etl_orchestration_pipeline()
    
    try:
        # Create state machine
        response = stepfunctions.create_state_machine(
            name='comprehensive-csv-to-parquet-etl',
            definition=json.dumps(definition),
            roleArn='arn:aws:iam::123456789:role/StepFunctionsETLRole',
            type='STANDARD',
            loggingConfiguration={
                'level': 'ALL',
                'includeExecutionData': True,
                'destinations': [
                    {
                        'cloudWatchLogsLogGroup': {
                            'logGroupArn': 'arn:aws:logs:us-east-1:123456789:log-group:/aws/stepfunctions/etl-pipeline'
                        }
                    }
                ]
            },
            tags=[
                {'key': 'Environment', 'value': 'Production'},
                {'key': 'DataPipeline', 'value': 'CSV-to-Parquet-ETL'},
                {'key': 'Owner', 'value': 'DataEngineering'}
            ]
        )
        
        print(f"✅ ETL State Machine created successfully!")
        print(f"ARN: {response['stateMachineArn']}")
        
        return response['stateMachineArn']
        
    except Exception as e:
        print(f"❌ Error creating state machine: {e}")
        return None

def start_etl_execution(state_machine_arn):
    """Start ETL pipeline execution"""
    
    # Example input for the pipeline
    pipeline_input = {
        "sourceBucket": "raw-data-bucket",
        "sourcePrefix": "daily-extracts/2024/07/14",
        "expectedFileCount": 3,
        "executionDate": "2024-07-14",
        "dataQualityThreshold": 95.0
    }
    
    try:
        response = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            name=f"etl-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            input=json.dumps(pipeline_input)
        )
        
        print(f"✅ ETL Pipeline execution started!")
        print(f"Execution ARN: {response['executionArn']}")
        
        return response['executionArn']
        
    except Exception as e:
        print(f"❌ Error starting execution: {e}")
        return None

# Main execution
if __name__ == "__main__":
    print("🚀 Deploying comprehensive ETL orchestration pipeline...")
    
    # Deploy the pipeline
    state_machine_arn = deploy_etl_pipeline()
    
    if state_machine_arn:
        print("\\n📊 Starting pipeline execution...")
        execution_arn = start_etl_execution(state_machine_arn)
        
        if execution_arn:
            print("\\n✅ ETL pipeline is now orchestrating your data transformation!")
            print("\\n📈 Pipeline includes:")
            print("  - Input validation")
            print("  - Data profiling") 
            print("  - Parallel processing")
            print("  - Data quality checks")
            print("  - Catalog updates")
            print("  - Warehouse loading")
            print("  - Success notifications")
            print("  - Comprehensive error handling")
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def serverless_event_driven_tab():
    """Content for Serverless Event-driven Workflows tab"""
    st.markdown("# ⚡ Serverless Event-driven Workflows")
    st.markdown("*Build reactive data pipelines that respond to events automatically*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    **Serverless Event-driven Workflows** automatically trigger data processing in response to events, 
    eliminating the need for manual intervention or continuous polling. It's like having a smart assistant 
    that knows exactly when to start working on your data.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Event-driven Architecture
    st.markdown("## 🏗️ Serverless Event-driven Architecture")
    common.mermaid(create_serverless_event_mermaid(), height=300)
    
    # Event Sources and Triggers
    st.markdown("## 🔔 Event Sources and Triggers")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📁 Storage Events
        
        **Amazon S3:**
        - Object Created/Deleted
        - Object Restore Completed
        - Replication events
        
        **Use Cases:**
        - Auto-process uploaded files
        - Trigger ETL on data arrival
        - Archive lifecycle management
        
        **Example:**
        ```
        s3:ObjectCreated:*
        s3:ObjectRemoved:*
        ```
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗄️ Database Events
        
        **DynamoDB Streams:**
        - Item modifications
        - Insert/Update/Delete
        - Stream records
        
        **RDS Events:**
        - Database state changes
        - Backup completion
        - Performance insights
        
        **Example:**
        ```
        INSERT, MODIFY, REMOVE
        Events in DynamoDB Stream
        ```
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Application Events
        
        **Custom Events:**
        - Business logic triggers
        - User actions
        - System status changes
        
        **SNS/SQS:**
        - Message queue events
        - Topic notifications
        - Dead letter queues
        
        **Example:**
        ```
        Custom business events
        Published to EventBridge
        ```
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Event-driven Pipeline Builder
    st.markdown("## 🛠️ Interactive Event-driven Pipeline Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        event_source = st.selectbox("Event Source:", [
            "S3 PutObject", "DynamoDB Stream", "EventBridge Custom Event", 
            "SNS Topic", "SQS Queue", "CloudWatch Alarm", "API Gateway"
        ])
        
        trigger_condition = st.selectbox("Trigger Condition:", [
            "Single Event", "Batch of 5 Events", "Time Window (15 min)", 
            "File Size Threshold", "Custom Logic"
        ])
        
        processing_target = st.selectbox("Processing Target:", [
            "AWS Glue Workflow", "Step Functions", "Lambda Function", 
            "EMR Step", "Batch Job"
        ])
    
    with col2:
        event_filtering = st.multiselect("Event Filtering:", [
            "File Extension (.json, .csv)", "Object Size", "Metadata Tags",
            "Source System", "Time of Day", "Data Schema"
        ])
        
        downstream_actions = st.multiselect("Downstream Actions:", [
            "Data Validation", "Format Conversion", "Data Enrichment",
            "Send Notifications", "Update Dashboard", "Trigger ML Model"
        ], default=["Data Validation"])
        
        error_handling = st.selectbox("Error Handling:", [
            "Retry with Exponential Backoff", "Send to Dead Letter Queue", 
            "Send Alert and Stop", "Skip and Continue"
        ])
    
    # Advanced Configuration
    st.markdown("### ⚙️ Advanced Configuration")
    
    col3, col4 = st.columns(2)
    
    with col3:
        concurrency_control = st.checkbox("Enable Concurrency Control", value=True)
        if concurrency_control:
            max_concurrent = st.slider("Max Concurrent Executions:", 1, 100, 10)
        
        enable_monitoring = st.checkbox("Enable Detailed Monitoring", value=True)
    
    with col4:
        cost_optimization = st.checkbox("Enable Cost Optimization", value=True)
        if cost_optimization:
            idle_timeout = st.slider("Idle Timeout (minutes):", 5, 60, 15)
        
        enable_tracing = st.checkbox("Enable X-Ray Tracing", value=False)
    
    if st.button("🚀 Deploy Event-driven Pipeline", use_container_width=True):
        
        # Calculate pipeline characteristics
        latency_score = {
            "Single Event": 1, "Batch of 5 Events": 3, "Time Window (15 min)": 15
        }.get(trigger_condition, 5)
        
        complexity_score = len(downstream_actions) * 2 + len(event_filtering)
        cost_per_invocation = 0.0000002 if "Lambda" in processing_target else 0.025
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Event-driven Pipeline Deployed!
        
        **Pipeline Configuration:**
        - **Event Source**: {event_source}
        - **Trigger**: {trigger_condition}
        - **Processing**: {processing_target}
        - **Filtering**: {len(event_filtering)} filter(s) applied
        - **Actions**: {len(downstream_actions)} downstream actions
        - **Error Handling**: {error_handling}
        
        **Performance Characteristics:**
        - **Expected Latency**: {latency_score} {'second' if latency_score == 1 else 'seconds/minutes'}
        - **Complexity Score**: {complexity_score}/20
        - **Cost per Invocation**: ${cost_per_invocation:.7f}
        {'- **Max Concurrency**: ' + str(max_concurrent) if concurrency_control else ''}
        
        **✨ Pipeline is now reactive and will automatically process events!**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Event-driven Patterns
    st.markdown("## 🔄 Common Event-driven Patterns")
    
    patterns_data = {
        'Pattern': ['Fan-out', 'Fan-in', 'Saga Pattern', 'Event Sourcing', 'CQRS'],
        'Description': [
            'Single event triggers multiple workflows',
            'Multiple events combine into single action',
            'Coordinated transactions across services',
            'Store all changes as sequence of events',
            'Separate read and write data models'
        ],
        'Use Case': [
            'File upload → validate, transform, notify',
            'All daily files → start batch processing',
            'Order processing across microservices',
            'Audit trail, data replay capabilities',
            'Real-time dashboard + batch analytics'
        ],
        'AWS Implementation': [
            'EventBridge → Multiple targets',
            'Step Functions Map state',
            'Step Functions with compensations',
            'DynamoDB Streams + Kinesis',
            'DynamoDB + Kinesis Analytics'
        ]
    }
    
    df_patterns = pd.DataFrame(patterns_data)
    
    # Create interactive expandable cards for patterns
    for i, row in df_patterns.iterrows():
        with st.expander(f"🔍 {row['Pattern']} Pattern"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Description:** {row['Description']}")
                st.markdown(f"**Use Case:** {row['Use Case']}")
            with col2:
                st.markdown(f"**AWS Implementation:** {row['AWS Implementation']}")
    
    # Real-world Examples
    st.markdown("## 🌟 Real-world Event-driven Examples")
    
    example_tabs = st.tabs(["🛒 E-commerce", "🏭 IoT Manufacturing", "📊 Real-time Analytics"])
    
    with example_tabs[0]:
        st.markdown('<div class="architecture-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛒 E-commerce Order Processing
        
        **Event Flow:**
        1. **Order Placed** → API Gateway event
        2. **Inventory Check** → DynamoDB stream triggers Lambda
        3. **Payment Processing** → EventBridge rule triggers Step Function
        4. **Fulfillment** → Multiple parallel workflows:
           - Shipping label generation
           - Inventory update
           - Customer notification
           - Analytics update
        
        **Benefits:**
        - ⚡ Immediate response to customer actions
        - 🔄 Automatic retry and error handling
        - 📊 Real-time inventory and analytics
        - 💰 Pay only for actual order processing
        
        **AWS Services:** API Gateway + EventBridge + Step Functions + Lambda + DynamoDB
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with example_tabs[1]:
        st.markdown('<div class="architecture-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🏭 IoT Manufacturing Data Pipeline
        
        **Event Flow:**
        1. **Sensor Data** → IoT Core receives telemetry
        2. **Rule Engine** → Routes based on sensor type and values
        3. **Anomaly Detection** → Machine learning inference
        4. **Alerts & Actions** → Conditional processing:
           - Normal data → Kinesis → Data Lake
           - Anomaly detected → SNS alert + Step Function
           - Critical failure → Immediate shutdown workflow
        
        **Benefits:**
        - 🔍 Real-time monitoring and alerting
        - 🤖 Automated quality control
        - 📈 Predictive maintenance capabilities
        - ⚡ Sub-second response to critical events
        
        **AWS Services:** IoT Core + Kinesis + Lambda + Step Functions + SNS
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with example_tabs[2]:
        st.markdown('<div class="architecture-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Real-time Analytics Dashboard
        
        **Event Flow:**
        1. **User Activity** → Website/app events to Kinesis
        2. **Stream Processing** → Kinesis Analytics aggregates data
        3. **Dashboard Update** → Lambda updates DynamoDB
        4. **Alerting** → CloudWatch monitors KPIs:
           - Conversion rate drops → Alert marketing team
           - Traffic spike → Auto-scale infrastructure
           - Revenue milestone → Celebrate! 🎉
        
        **Benefits:**
        - 📈 Real-time business insights
        - 🚨 Proactive alerting on KPIs
        - 🔄 Automatic scaling based on load
        - 📊 Historical data preservation
        
        **AWS Services:** Kinesis + Lambda + DynamoDB + CloudWatch + SNS
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Complete Event-driven Workflow")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Complete serverless event-driven data pipeline
import boto3
import json
from datetime import datetime

# Initialize AWS clients
eventbridge = boto3.client('events')
stepfunctions = boto3.client('stepfunctions')
s3 = boto3.client('s3')
glue = boto3.client('glue')

def create_event_driven_data_pipeline():
    """
    Create a complete serverless event-driven pipeline that:
    1. Listens for S3 PutObject events via CloudTrail
    2. Uses EventBridge to route events
    3. Triggers Glue workflow when batch conditions are met
    4. Processes data with quality checks
    5. Sends notifications on completion/failure
    """
    
    # 1. Create EventBridge rule for S3 events
    eventbridge_rule = {
        "Name": "s3-data-ingestion-rule",
        "EventPattern": {
            "source": ["aws.s3"],
            "detail-type": ["Object Created"],
            "detail": {
                "eventSource": ["s3.amazonaws.com"],
                "eventName": ["PutObject"],
                "requestParameters": {
                    "bucketName": ["data-ingestion-bucket"],
                    "key": [{"prefix": "raw-data/"}]
                }
            }
        },
        "State": "ENABLED",
        "Description": "Trigger data pipeline on S3 file uploads"
    }
    
    # 2. Create Glue workflow with event-driven trigger
    glue_workflow_definition = {
        "name": "event-driven-etl-workflow",
        "description": "ETL workflow triggered by S3 events",
        "triggers": [
            {
                "name": "s3-event-trigger",
                "type": "EVENT",
                "actions": [
                    {
                        "jobName": "validate-and-process-data",
                        "arguments": {
                            "--TempDir": "s3://temp-glue-bucket/",
                            "--job-bookmark-option": "job-bookmark-enable"
                        }
                    }
                ],
                "batchCondition": {
                    "batchSize": 5,          # Process when 5 files arrive
                    "batchWindow": 900       # Or after 15 minutes
                }
            }
        ],
        "jobs": [
            {
                "name": "validate-and-process-data",
                "role": "arn:aws:iam::123456789:role/GlueServiceRole",
                "command": {
                    "name": "glueetl",
                    "scriptLocation": "s3://scripts-bucket/event-driven-etl.py"
                },
                "defaultArguments": {
                    "--enable-metrics": "",
                    "--enable-continuous-cloudwatch-log": "",
                    "--enable-auto-scaling": ""
                }
            }
        ]
    }
    
    # 3. Step Functions workflow for complex orchestration
    step_functions_definition = {
        "Comment": "Event-driven data processing workflow",
        "StartAt": "CheckEventDetails",
        "States": {
            "CheckEventDetails": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "process-s3-event",
                    "Payload": {
                        "eventDetails.$": "$",
                        "processingConfig": {
                            "dataQualityThreshold": 95,
                            "maxFileSize": "1GB",
                            "supportedFormats": ["csv", "json", "parquet"]
                        }
                    }
                },
                "ResultPath": "$.eventProcessing",
                "Next": "DecideProcessingPath"
            },
            
            "DecideProcessingPath": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.eventProcessing.Payload.fileType",
                        "StringEquals": "csv",
                        "Next": "ProcessCSVFile"
                    },
                    {
                        "Variable": "$.eventProcessing.Payload.fileType", 
                        "StringEquals": "json",
                        "Next": "ProcessJSONFile"
                    },
                    {
                        "Variable": "$.eventProcessing.Payload.fileSize",
                        "NumericGreaterThan": 100000000,  # 100MB
                        "Next": "ProcessLargeFile"
                    }
                ],
                "Default": "ProcessStandardFile"
            },
            
            "ProcessCSVFile": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "csv-to-parquet-converter",
                    "Arguments": {
                        "--input-path.$": "$.eventProcessing.Payload.s3Path",
                        "--output-path": "s3://processed-data/csv-converted/",
                        "--enable-partitioning": "true"
                    }
                },
                "ResultPath": "$.csvProcessing",
                "Next": "DataQualityCheck"
            },
            
            "ProcessJSONFile": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "json-flattener-and-validator",
                    "Arguments": {
                        "--input-path.$": "$.eventProcessing.Payload.s3Path",
                        "--output-path": "s3://processed-data/json-processed/",
                        "--flatten-nested": "true"
                    }
                },
                "ResultPath": "$.jsonProcessing",
                "Next": "DataQualityCheck"
            },
            
            "ProcessLargeFile": {
                "Type": "Task",
                "Resource": "arn:aws:states:::emr:addStep.sync",
                "Parameters": {
                    "ClusterId": "j-ABCDEFGHIJKLM",  # EMR cluster for large files
                    "Step": {
                        "Name": "Process Large File",
                        "ActionOnFailure": "CONTINUE",
                        "HadoopJarStep": {
                            "Jar": "command-runner.jar",
                            "Args": [
                                "spark-submit",
                                "--deploy-mode", "cluster",
                                "s3://scripts-bucket/large-file-processor.py",
                                "--input-path", "$.eventProcessing.Payload.s3Path",
                                "--output-path", "s3://processed-data/large-files/"
                            ]
                        }
                    }
                },
                "ResultPath": "$.largeFileProcessing",
                "Next": "DataQualityCheck"
            },
            
            "ProcessStandardFile": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "standard-file-processor",
                    "Payload": {
                        "inputPath.$": "$.eventProcessing.Payload.s3Path",
                        "outputPath": "s3://processed-data/standard/",
                        "processingOptions": {
                            "compressionType": "gzip",
                            "outputFormat": "parquet"
                        }
                    }
                },
                "ResultPath": "$.standardProcessing",
                "Next": "DataQualityCheck"
            },
            
            "DataQualityCheck": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "data-quality-validator",
                    "Payload": {
                        "processedDataPath.$": "States.Format('s3://processed-data/{}/{}', $.eventProcessing.Payload.fileType, $.eventProcessing.Payload.fileName)",
                        "qualityRules": [
                            {"type": "completeness", "threshold": 95},
                            {"type": "uniqueness", "columns": ["id"]},
                            {"type": "validity", "dataTypes": "inferred"}
                        ]
                    }
                },
                "ResultPath": "$.qualityCheck",
                "Next": "EvaluateDataQuality"
            },
            
            "EvaluateDataQuality": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.qualityCheck.Payload.overallScore",
                        "NumericGreaterThanEquals": 95,
                        "Next": "UpdateCatalogAndNotify"
                    },
                    {
                        "And": [
                            {
                                "Variable": "$.qualityCheck.Payload.overallScore",
                                "NumericGreaterThanEquals": 80
                            },
                            {
                                "Variable": "$.qualityCheck.Payload.overallScore",
                                "NumericLessThan": 95
                            }
                        ],
                        "Next": "QuarantineWithWarning"
                    }
                ],
                "Default": "QuarantineWithError"
            },
            
            "UpdateCatalogAndNotify": {
                "Type": "Parallel",
                "Branches": [
                    {
                        "StartAt": "UpdateGlueDataCatalog",
                        "States": {
                            "UpdateGlueDataCatalog": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startCrawler",
                                "Parameters": {
                                    "CrawlerName": "processed-data-crawler"
                                },
                                "End": True
                            }
                        }
                    },
                    {
                        "StartAt": "SendSuccessNotification",
                        "States": {
                            "SendSuccessNotification": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::sns:publish",
                                "Parameters": {
                                    "TopicArn": "arn:aws:sns:us-east-1:123456789:data-pipeline-success",
                                    "Subject": "Data Processing Completed Successfully",
                                    "Message.$": "States.Format('File {} processed successfully with quality score: {}%', $.eventProcessing.Payload.fileName, $.qualityCheck.Payload.overallScore)"
                                },
                                "End": True
                            }
                        }
                    },
                    {
                        "StartAt": "TriggerDownstreamProcesses",
                        "States": {
                            "TriggerDownstreamProcesses": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::events:putEvents",
                                "Parameters": {
                                    "Entries": [
                                        {
                                            "Source": "data.pipeline",
                                            "DetailType": "Data Processing Completed",
                                            "Detail": {
                                                "status": "success",
                                                "fileName.$": "$.eventProcessing.Payload.fileName",
                                                "qualityScore.$": "$.qualityCheck.Payload.overallScore",
                                                "outputPath.$": "States.Format('s3://processed-data/{}/{}', $.eventProcessing.Payload.fileType, $.eventProcessing.Payload.fileName)"
                                            }
                                        }
                                    ]
                                },
                                "End": True
                            }
                        }
                    }
                ],
                "End": True
            },
            
            "QuarantineWithWarning": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "quarantine-data",
                    "Payload": {
                        "sourcePath.$": "States.Format('s3://processed-data/{}/{}', $.eventProcessing.Payload.fileType, $.eventProcessing.Payload.fileName)",
                        "quarantinePath": "s3://data-quarantine/warning/",
                        "reason": "Data quality below threshold but above minimum"
                    }
                },
                "Next": "SendWarningNotification"
            },
            
            "QuarantineWithError": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "quarantine-data",
                    "Payload": {
                        "sourcePath.$": "States.Format('s3://processed-data/{}/{}', $.eventProcessing.Payload.fileType, $.eventProcessing.Payload.fileName)",
                        "quarantinePath": "s3://data-quarantine/error/",
                        "reason": "Data quality critically low"
                    }
                },
                "Next": "SendErrorNotification"
            },
            
            "SendWarningNotification": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": "arn:aws:sns:us-east-1:123456789:data-pipeline-warnings",
                    "Subject": "Data Quality Warning",
                    "Message.$": "States.Format('File {} has quality issues (Score: {}%). Data quarantined for review.', $.eventProcessing.Payload.fileName, $.qualityCheck.Payload.overallScore)"
                },
                "End": True
            },
            
            "SendErrorNotification": {
                "Type": "Task", 
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": "arn:aws:sns:us-east-1:123456789:data-pipeline-errors",
                    "Subject": "Critical Data Quality Error",
                    "Message.$": "States.Format('File {} failed quality checks (Score: {}%). Immediate attention required.', $.eventProcessing.Payload.fileName, $.qualityCheck.Payload.overallScore)"
                },
                "End": True
            }
        }
    }
    
    return {
        "eventbridge_rule": eventbridge_rule,
        "glue_workflow": glue_workflow_definition,
        "step_functions": step_functions_definition
    }

def deploy_event_driven_pipeline():
    """Deploy all components of the event-driven pipeline"""
    
    pipeline_config = create_event_driven_data_pipeline()
    
    try:
        # 1. Create EventBridge rule
        eventbridge.put_rule(
            Name=pipeline_config["eventbridge_rule"]["Name"],
            EventPattern=json.dumps(pipeline_config["eventbridge_rule"]["EventPattern"]),
            State=pipeline_config["eventbridge_rule"]["State"],
            Description=pipeline_config["eventbridge_rule"]["Description"]
        )
        print("✅ EventBridge rule created")
        
        # 2. Create Glue workflow
        glue.create_workflow(
            Name=pipeline_config["glue_workflow"]["name"],
            Description=pipeline_config["glue_workflow"]["description"]
        )
        print("✅ Glue workflow created")
        
        # 3. Create Step Functions state machine
        stepfunctions.create_state_machine(
            name='event-driven-data-processing',
            definition=json.dumps(pipeline_config["step_functions"]),
            roleArn='arn:aws:iam::123456789:role/StepFunctionsRole',
            type='EXPRESS'  # Use EXPRESS for event-driven workflows
        )
        print("✅ Step Functions state machine created")
        
        # 4. Configure EventBridge target
        eventbridge.put_targets(
            Rule=pipeline_config["eventbridge_rule"]["Name"],
            Targets=[
                {
                    'Id': '1',
                    'Arn': 'arn:aws:states:us-east-1:123456789:stateMachine:event-driven-data-processing',
                    'RoleArn': 'arn:aws:iam::123456789:role/EventBridgeRole'
                }
            ]
        )
        print("✅ EventBridge target configured")
        
        print("\\n🎉 Event-driven pipeline deployed successfully!")
        print("\\n📋 What happens now:")
        print("  1. Upload file to s3://data-ingestion-bucket/raw-data/")
        print("  2. S3 event triggers CloudTrail")
        print("  3. EventBridge receives event")
        print("  4. Step Functions workflow processes file")
        print("  5. Data quality checks run automatically") 
        print("  6. Notifications sent based on results")
        print("  7. Catalog updated for successful processing")
        
        return True
        
    except Exception as e:
        print(f"❌ Error deploying pipeline: {e}")
        return False

# Example lambda function for processing S3 events
def lambda_handler(event, context):
    """
    Lambda function to process S3 events and extract metadata
    """
    try:
        # Extract S3 event details
        s3_event = event['Records'][0]['s3']
        bucket_name = s3_event['bucket']['name']
        object_key = s3_event['object']['key']
        object_size = s3_event['object']['size']
        
        # Determine file type
        file_extension = object_key.split('.')[-1].lower()
        
        # Prepare response for Step Functions
        response = {
            'statusCode': 200,
            'body': {
                'fileName': object_key.split('/')[-1],
                'fileType': file_extension,
                'fileSize': object_size,
                's3Path': f's3://{bucket_name}/{object_key}',
                'bucketName': bucket_name,
                'timestamp': datetime.utcnow().isoformat()
            }
        }
        
        return response
        
    except Exception as e:
        print(f"Error processing S3 event: {e}")
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }

# Main execution
if __name__ == "__main__":
    print("🚀 Deploying serverless event-driven data pipeline...")
    
    success = deploy_event_driven_pipeline()
    
    if success:
        print("\\n✅ Your serverless event-driven data pipeline is now active!")
        print("\\n🔄 The pipeline will automatically:")
        print("  - React to data arriving in S3")
        print("  - Process files based on type and size")
        print("  - Validate data quality")
        print("  - Send appropriate notifications")
        print("  - Update data catalogs")
        print("  - Handle errors gracefully")
        print("\\n💡 No servers to manage, pay only for what you use!")
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def data_pipelines_comparison_tab():
    """Content for Data Pipelines on AWS comparison tab"""
    st.markdown("# 🏗️ Data Pipelines on AWS - Service Comparison")
    st.markdown("*Choose the right orchestration service for your data pipeline requirements*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Concept
    AWS offers multiple services for orchestrating data pipelines, each optimized for different use cases and complexity levels.
    Choosing the right service depends on your team's expertise, workflow complexity, and integration requirements.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Service Comparison Chart
    st.markdown("## 📊 Service Comparison Overview")
    st.plotly_chart(create_orchestration_services_comparison(), use_container_width=True)
    
    # Detailed Service Comparison
    st.markdown("## 🔍 Detailed Service Comparison")
    
    service_tabs = st.tabs(["🔧 Step Functions", "🐍 AWS Glue Workflows", "✈️ Amazon MWAA", "📨 EventBridge"])
    
    with service_tabs[0]:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="service-comparison">', unsafe_allow_html=True)
            st.markdown("""
            ### 🔧 AWS Step Functions
            
            **Best For:**
            - Complex multi-service workflows
            - Visual workflow management
            - Integration with multiple AWS services
            - Event-driven architectures
            
            **Strengths:**
            - ✅ Visual workflow designer
            - ✅ Built-in error handling and retry logic
            - ✅ JSON-based state language
            - ✅ Extensive AWS service integrations
            - ✅ Two workflow types (Standard/Express)
            
            **Considerations:**
            - ⚠️ Learning curve for ASL (Amazon States Language)
            - ⚠️ Cost can add up with many state transitions
            - ⚠️ JSON definitions can become complex
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="code-container">', unsafe_allow_html=True)
            st.code('''
# Step Functions Example
{
  "Comment": "Data pipeline with Step Functions",
  "StartAt": "ExtractData",
  "States": {
    "ExtractData": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "extract-job"
      },
      "Next": "TransformData"
    },
    "TransformData": {
      "Type": "Task", 
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "transform-job"
      },
      "Retry": [
        {
          "ErrorEquals": ["States.TaskFailed"],
          "IntervalSeconds": 30,
          "MaxAttempts": 3
        }
      ],
      "End": true
    }
  }
}
            ''', language='json')
            st.markdown('</div>', unsafe_allow_html=True)
    
    with service_tabs[1]:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="service-comparison">', unsafe_allow_html=True)
            st.markdown("""
            ### 🐍 AWS Glue Workflows
            
            **Best For:**
            - Glue-centric ETL pipelines
            - Simple to moderate complexity workflows
            - Teams already using AWS Glue extensively
            - Cost-sensitive projects
            
            **Strengths:**
            - ✅ Native integration with Glue jobs and crawlers
            - ✅ Lower cost for Glue-only workflows
            - ✅ Built-in job bookmarking
            - ✅ Simple trigger configuration
            - ✅ Good for ETL-focused pipelines
            
            **Considerations:**
            - ⚠️ Limited to Glue ecosystem
            - ⚠️ Less flexible than Step Functions
            - ⚠️ Basic error handling capabilities
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="code-container">', unsafe_allow_html=True)
            st.code('''
# AWS Glue Workflow Python API
import boto3

glue = boto3.client('glue')

# Create workflow
response = glue.create_workflow(
    Name='daily-etl-workflow',
    Description='Daily ETL processing',
    DefaultRunProperties={
        'glue:max.concurrent.runs': '3'
    }
)

# Create trigger
glue.create_trigger(
    Name='scheduled-trigger',
    WorkflowName='daily-etl-workflow',
    Type='SCHEDULED',
    Schedule='cron(0 6 * * ? *)',  # Daily at 6 AM
    Actions=[
        {
            'JobName': 'extract-daily-data',
            'Arguments': {
                '--TempDir': 's3://temp-bucket/',
                '--job-bookmark-option': 'job-bookmark-enable'
            }
        }
    ]
)
            ''', language='python')
            st.markdown('</div>', unsafe_allow_html=True)
    
    with service_tabs[2]:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="service-comparison">', unsafe_allow_html=True)
            st.markdown("""
            ### ✈️ Amazon MWAA (Managed Airflow)
            
            **Best For:**
            - Teams experienced with Apache Airflow
            - Complex DAG-based workflows
            - Migration from on-premises Airflow
            - Need for custom operators and plugins
            
            **Strengths:**
            - ✅ Full Apache Airflow compatibility
            - ✅ Python-based DAG definitions
            - ✅ Rich ecosystem of operators
            - ✅ Advanced scheduling capabilities
            - ✅ Extensive monitoring and logging
            
            **Considerations:**
            - ⚠️ Higher operational complexity
            - ⚠️ More expensive than other options
            - ⚠️ Requires Airflow expertise
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="code-container">', unsafe_allow_html=True)
            st.code('''
# Amazon MWAA (Airflow) DAG Example
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    description='Daily ETL processing',
    schedule_interval='@daily',
    catchup=False
)

# Wait for input file
wait_for_file = S3KeySensor(
    task_id='wait_for_input_file',
    bucket_name='input-bucket',
    bucket_key='daily-data/{{ ds }}/data.csv',
    timeout=3600,
    poke_interval=300,
    dag=dag
)

# Run Glue job
process_data = GlueJobOperator(
    task_id='process_daily_data',
    job_name='daily-data-processor',
    script_args={
        '--input-date': '{{ ds }}',
        '--s3-bucket': 'processed-data-bucket'
    },
    dag=dag
)

wait_for_file >> process_data
            ''', language='python')
            st.markdown('</div>', unsafe_allow_html=True)
    
    with service_tabs[3]:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="service-comparison">', unsafe_allow_html=True)
            st.markdown("""
            ### 📨 Amazon EventBridge
            
            **Best For:**
            - Event-driven architectures
            - Loose coupling between services
            - Real-time data processing
            - Microservices communication
            
            **Strengths:**
            - ✅ Serverless event routing
            - ✅ Schema registry and discovery
            - ✅ Content-based filtering
            - ✅ Multiple targets per rule
            - ✅ Built-in retry and DLQ support
            
            **Considerations:**
            - ⚠️ Not suitable for complex workflows
            - ⚠️ Limited orchestration capabilities
            - ⚠️ Best combined with other services
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="code-container">', unsafe_allow_html=True)
            st.code('''
# EventBridge Rule Example
import boto3

eventbridge = boto3.client('events')

# Create rule for S3 events
response = eventbridge.put_rule(
    Name='s3-data-processing-rule',
    EventPattern=json.dumps({
        "source": ["aws.s3"],
        "detail-type": ["Object Created"],
        "detail": {
            "eventSource": ["s3.amazonaws.com"],
            "eventName": ["PutObject"],
            "requestParameters": {
                "bucketName": ["data-lake-bucket"],
                "key": [{"prefix": "incoming/"}]
            }
        }
    }),
    State='ENABLED',
    Description='Trigger processing on S3 uploads'
)

# Add targets
eventbridge.put_targets(
    Rule='s3-data-processing-rule',
    Targets=[
        {
            'Id': '1',
            'Arn': 'arn:aws:states:us-east-1:123456789:stateMachine:data-processor',
            'RoleArn': 'arn:aws:iam::123456789:role/EventBridgeRole'
        },
        {
            'Id': '2', 
            'Arn': 'arn:aws:lambda:us-east-1:123456789:function:log-data-arrival'
        }
    ]
)
            ''', language='python')
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Decision Matrix
    st.markdown("## 🤔 Decision Matrix - Which Service to Choose?")
    
    decision_scenarios = [
        {
            "Scenario": "Simple Glue ETL with scheduling",
            "Recommendation": "AWS Glue Workflows",
            "Reason": "Native integration, cost-effective, sufficient for basic needs"
        },
        {
            "Scenario": "Complex multi-service data pipeline",
            "Recommendation": "AWS Step Functions",
            "Reason": "Visual workflows, extensive integrations, robust error handling"
        },
        {
            "Scenario": "Event-driven real-time processing", 
            "Recommendation": "EventBridge + Step Functions",
            "Reason": "Event routing + workflow orchestration combination"
        },
        {
            "Scenario": "Team with Airflow experience",
            "Recommendation": "Amazon MWAA",
            "Reason": "Leverage existing knowledge, Python DAGs, rich ecosystem"
        },
        {
            "Scenario": "High-volume, low-latency processing",
            "Recommendation": "Step Functions Express",
            "Reason": "High throughput, cost-effective, fast execution"
        },
        {
            "Scenario": "Budget-conscious small team",
            "Recommendation": "AWS Glue Workflows",
            "Reason": "Lower cost, simpler operations, good for ETL focus"
        }
    ]
    
    # Create decision matrix visualization
    df_scenarios = pd.DataFrame(decision_scenarios)
    
    fig = px.sunburst(
        df_scenarios,
        path=['Recommendation', 'Scenario'],
        title="Service Selection Decision Tree",
        color_discrete_sequence=[AWS_COLORS['primary'], AWS_COLORS['light_blue'], 
                                AWS_COLORS['success'], AWS_COLORS['warning']]
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Interactive Decision Helper
    st.markdown("## 🎯 Interactive Service Selector")
    
    col1, col2 = st.columns(2)
    
    with col1:
        team_experience = st.selectbox("Team Experience:", [
            "New to data orchestration", "AWS native services", 
            "Apache Airflow experience", "Mixed background"
        ])
        
        workflow_complexity = st.selectbox("Workflow Complexity:", [
            "Simple ETL jobs", "Multi-step pipelines", 
            "Complex business logic", "Event-driven processing"
        ])
        
        budget_priority = st.selectbox("Budget Priority:", [
            "Cost optimization critical", "Balanced cost/features", 
            "Feature-rich over cost", "Enterprise budget available"
        ])
    
    with col2:
        integration_needs = st.multiselect("Integration Requirements:", [
            "AWS Glue only", "Multiple AWS services", "Third-party APIs", 
            "On-premises systems", "Real-time streams"
        ])
        
        operational_preference = st.selectbox("Operational Preference:", [
            "Minimal maintenance", "Some configuration OK", 
            "Full control needed", "Managed service preferred"
        ])
        
        scale_requirements = st.selectbox("Scale Requirements:", [
            "Small/medium workloads", "Large batch processing", 
            "High-frequency events", "Enterprise scale"
        ])
    
    if st.button("🔍 Get Service Recommendation", use_container_width=True):
        
        # Simple recommendation logic
        score_sf = 0  # Step Functions
        score_glue = 0  # Glue Workflows  
        score_mwaa = 0  # Amazon MWAA
        score_eb = 0  # EventBridge
        
        # Team experience scoring
        if team_experience == "New to data orchestration":
            score_glue += 3
            score_sf += 2
        elif team_experience == "Apache Airflow experience":
            score_mwaa += 4
            score_sf += 1
        elif team_experience == "AWS native services":
            score_sf += 3
            score_glue += 2
        
        # Complexity scoring
        if workflow_complexity == "Simple ETL jobs":
            score_glue += 4
            score_sf += 2
        elif workflow_complexity == "Multi-step pipelines":
            score_sf += 4
            score_mwaa += 3
        elif workflow_complexity == "Complex business logic":
            score_mwaa += 4
            score_sf += 3
        elif workflow_complexity == "Event-driven processing":
            score_eb += 4
            score_sf += 3
        
        # Budget priority
        if budget_priority == "Cost optimization critical":
            score_glue += 3
            score_eb += 2
        elif budget_priority == "Enterprise budget available":
            score_mwaa += 2
            score_sf += 2
        
        # Integration needs
        if "AWS Glue only" in integration_needs:
            score_glue += 4
        if "Multiple AWS services" in integration_needs:
            score_sf += 4
            score_eb += 2
        if "Real-time streams" in integration_needs:
            score_eb += 3
            score_sf += 2
        
        # Determine recommendation
        scores = {
            "AWS Step Functions": score_sf,
            "AWS Glue Workflows": score_glue,
            "Amazon MWAA": score_mwaa,
            "EventBridge + Step Functions": score_eb
        }
        
        recommended_service = max(scores, key=scores.get)
        confidence = (scores[recommended_service] / sum(scores.values())) * 100 if sum(scores.values()) > 0 else 0
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 🎯 Service Recommendation
        
        **Recommended Service**: **{recommended_service}**
        **Confidence**: {confidence:.1f}%
        
        **Why this recommendation?**
        Based on your requirements:
        - Team Experience: {team_experience}
        - Workflow Complexity: {workflow_complexity}
        - Budget Priority: {budget_priority}
        - Integration Needs: {', '.join(integration_needs)}
        - Operational Preference: {operational_preference}
        - Scale Requirements: {scale_requirements}
        
        **Next Steps:**
        1. Review the detailed comparison above
        2. Start with a simple pilot project
        3. Consider hybrid approaches for complex scenarios
        4. Evaluate total cost of ownership
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Best Practices Summary
    st.markdown("## 💡 Best Practices Summary")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 Service Selection
        
        **Do:**
        - Start simple and evolve
        - Consider team expertise
        - Evaluate total cost of ownership
        - Plan for monitoring and operations
        
        **Don't:**
        - Over-engineer for simple use cases
        - Ignore operational complexity
        - Forget about error handling
        - Skip performance testing
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Pipeline Design
        
        **Key Principles:**
        - Idempotent operations
        - Comprehensive error handling
        - Proper resource sizing
        - Clear monitoring and alerting
        
        **Common Patterns:**
        - Retry with exponential backoff
        - Dead letter queues for failed items
        - Parallel processing where possible
        - Graceful degradation strategies
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Monitoring & Operations
        
        **Essential Metrics:**
        - Execution success/failure rates
        - Processing duration
        - Cost per execution
        - Data quality scores
        
        **Operational Tools:**
        - CloudWatch dashboards
        - SNS notifications
        - X-Ray tracing
        - Cost monitoring alerts
        """)
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
    # 🔗 AWS Data Pipeline Orchestration
    ### Master the art of coordinating complex data workflows at scale
    """)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🔗 Orchestration Fundamentals",
        "🛠️ Step Functions Orchestration", 
        "🔄 ETL Pipeline Orchestration",
        "⚡ Serverless Event-driven",
        "🏗️ Data Pipelines Comparison"
    ])
    
    with tab1:
        orchestration_fundamentals_tab()
    
    with tab2:
        step_functions_orchestration_tab()
    
    with tab3:
        etl_orchestration_tab()
    
    with tab4:
        serverless_event_driven_tab()
        
    with tab5:
        data_pipelines_comparison_tab()
    
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
