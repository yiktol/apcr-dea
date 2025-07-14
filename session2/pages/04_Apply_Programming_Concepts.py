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
    page_title="Programming Concepts",
    page_icon="💻",
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
        
        .code-container {{
            background-color: {AWS_COLORS['dark_blue']};
            color: white;
            padding: 20px;
            border-radius: 10px;
            border-left: 4px solid {AWS_COLORS['primary']};
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
        
        .metric-card {{
            background: linear-gradient(135deg, {AWS_COLORS['primary']} 0%, {AWS_COLORS['light_blue']} 100%);
            padding: 20px;
            border-radius: 15px;
            color: white;
            text-align: center;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
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
            - 🔄 CI/CD - Continuous Integration & Delivery
            - ⚡ AWS Lambda - Serverless compute optimization
            - 🏗️ Infrastructure as Code - CloudFormation & CDK
            - 🚀 AWS SAM - Serverless Application Model
            - 📊 Data Analytics Pipeline - Real-world example
            - ⚡ Query Optimization - Performance tuning
            
            **Learning Objectives:**
            - Master CI/CD pipelines for data engineering
            - Optimize Lambda functions for performance
            - Implement Infrastructure as Code
            - Build serverless applications with SAM
            - Design analytics pipelines
            - Optimize SQL queries for better performance
            """)

def create_cicd_pipeline_mermaid():
    """Create mermaid diagram for CI/CD pipeline"""
    return """
    graph TD
        A[👨‍💻 Developer] -->|commits code| B[📁 CodeCommit]
        B -->|triggers| C[🚀 CodePipeline]
        C --> D[🔨 CodeBuild]
        D --> E[🧪 Run Tests]
        E -->|tests pass| F[📦 Build Artifacts]
        F --> G[🏗️ Deploy to Staging]
        G --> H[✅ Integration Tests]
        H -->|approved| I[🚀 Deploy to Production]
        H -->|failed| J[❌ Rollback]
        
        style A fill:#4B9EDB,stroke:#232F3E,color:#fff
        style B fill:#FF9900,stroke:#232F3E,color:#fff
        style C fill:#FF9900,stroke:#232F3E,color:#fff
        style D fill:#3FB34F,stroke:#232F3E,color:#fff
        style E fill:#3FB34F,stroke:#232F3E,color:#fff
        style I fill:#3FB34F,stroke:#232F3E,color:#fff
        style J fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_lambda_performance_mermaid():
    """Create mermaid diagram for Lambda performance optimization"""
    return """
    graph TD
        A[🚀 Lambda Function] --> B[💾 Memory Configuration]
        A --> C[❄️ Cold Start Optimization]
        A --> D[🔄 Concurrency Management]
        A --> E[⏱️ Timeout Configuration]
        
        B --> B1[More Memory = More CPU]
        B --> B2[Balance Cost vs Performance]
        
        C --> C1[Provisioned Concurrency]
        C --> C2[Connection Pooling]
        C --> C3[Lazy Loading]
        
        D --> D1[Reserved Concurrency]
        D --> D2[Auto Scaling]
        
        E --> E1[Set Appropriate Timeouts]
        E --> E2[Monitor Function Duration]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#232F3E,stroke:#FF9900,color:#fff
        style E fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_iac_comparison_mermaid():
    """Create mermaid diagram comparing IaC options"""
    return """
    graph TD
        A[🏗️ Infrastructure as Code] --> B[☁️ CloudFormation]
        A --> C[⚙️ AWS CDK]
        A --> D[🚀 AWS SAM]
        
        B --> B1[JSON/YAML Templates]
        B --> B2[Declarative]
        B --> B3[AWS Native]
        
        C --> C1[Programming Languages]
        C --> C2[TypeScript, Python, Java]
        C --> C3[Higher Level Abstractions]
        
        D --> D1[Serverless Focus]
        D --> D2[Lambda + API Gateway]
        D --> D3[Built on CloudFormation]
        
        style A fill:#FF9900,stroke:#232F3E,color:#fff
        style B fill:#4B9EDB,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_analytics_pipeline_mermaid():
    """Create mermaid diagram for analytics pipeline"""
    return """
    graph TD
        A[📱 Data Sources] --> B[📥 Data Ingestion]
        B --> C[🔄 Data Processing]
        C --> D[🗄️ Data Storage]
        D --> E[📊 Analytics & Visualization]
        
        A --> A1[IoT Sensors]
        A --> A2[Web Applications]
        A --> A3[Mobile Apps]
        
        B --> B1[Kinesis Data Streams]
        B --> B2[Kinesis Firehose]
        
        C --> C1[AWS Glue ETL]
        C --> C2[Lambda Functions]
        C --> C3[EMR Spark Jobs]
        
        D --> D1[S3 Data Lake]
        D --> D2[Redshift DW]
        D --> D3[DynamoDB]
        
        E --> E1[QuickSight]
        E --> E2[Athena]
        E --> E3[CloudWatch]
        
        style A fill:#4B9EDB,stroke:#232F3E,color:#fff
        style B fill:#FF9900,stroke:#232F3E,color:#fff
        style C fill:#3FB34F,stroke:#232F3E,color:#fff
        style D fill:#232F3E,stroke:#FF9900,color:#fff
        style E fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_lambda_execution_comparison():
    """Create comparison chart for Lambda execution models"""
    data = {
        'Execution Model': ['Synchronous', 'Asynchronous', 'Stream'],
        'Response Time': ['Immediate', 'None', 'Real-time'],
        'Use Cases': ['API Gateway', 'S3 Events', 'Kinesis/DynamoDB'],
        'Error Handling': ['Return to caller', 'DLQ/Retry', 'Batch processing'],
        'Concurrency': ['Limited by caller', 'Auto-scaling', 'Per shard']
    }
    
    df = pd.DataFrame(data)
    
    fig = go.Figure(data=[go.Table(
        header=dict(values=list(df.columns),
                   fill_color=AWS_COLORS['primary'],
                   font_color='white',
                   font_size=14,
                   height=40),
        cells=dict(values=[df[col] for col in df.columns],
                  fill_color=AWS_COLORS['light_gray'],
                  font_size=12,
                  height=35))
    ])
    
    fig.update_layout(
        title="Lambda Execution Models Comparison",
        height=300
    )
    
    return fig

def programming_concepts_tab():
    """Content for Programming Concepts tab"""
    st.markdown("# 💻 Apply Programming Concepts")
    st.markdown("*Essential programming concepts for AWS Data Engineering*")
    
    # Key Concepts Overview
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Task Statement 1.4: Apply Programming Concepts
    
    This domain covers essential programming skills needed for data engineering on AWS, including:
    - **CI/CD pipelines** for automated deployment
    - **SQL optimization** for better query performance  
    - **Infrastructure as Code** for repeatable deployments
    - **Distributed computing** concepts
    - **Version control** with Git commands
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Programming Knowledge Assessment
    st.markdown("## 🧠 Knowledge Check: Programming Fundamentals")
    
    col1, col2 = st.columns(2)
    
    with col1:
        data_structure = st.selectbox("Best data structure for hierarchical data:", [
            "Array/List", "Tree", "Hash Table", "Queue"
        ])
        
        query_optimization = st.selectbox("Primary SQL optimization technique:", [
            "Use SELECT *", "Add proper indexes", "Avoid WHERE clauses", "Use nested loops"
        ])
    
    with col2:
        git_command = st.selectbox("Git command to create a new branch:", [
            "git branch new-feature", "git checkout -b new-feature", "git clone new-feature", "git merge new-feature"
        ])
        
        cicd_benefit = st.selectbox("Primary benefit of CI/CD:", [
            "Slower deployments", "Manual testing only", "Automated testing & deployment", "Larger team size"
        ])
    
    if st.button("🔍 Check Answers", use_container_width=True):
        score = 0
        feedback = []
        
        if data_structure == "Tree":
            score += 1
            feedback.append("✅ Correct! Trees are ideal for hierarchical data structures")
        else:
            feedback.append("❌ Trees are best for hierarchical data like file systems or organizational charts")
        
        if query_optimization == "Add proper indexes":
            score += 1
            feedback.append("✅ Correct! Proper indexing significantly improves query performance")
        else:
            feedback.append("❌ Adding proper indexes is crucial for SQL query optimization")
        
        if git_command == "git checkout -b new-feature":
            score += 1
            feedback.append("✅ Correct! This command creates and switches to a new branch")
        else:
            feedback.append("❌ 'git checkout -b' creates and switches to a new branch in one command")
        
        if cicd_benefit == "Automated testing & deployment":
            score += 1
            feedback.append("✅ Correct! CI/CD automates the entire development pipeline")
        else:
            feedback.append("❌ CI/CD's main benefit is automating testing and deployment processes")
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### 📊 Assessment Results
        **Score: {score}/4 ({score/4*100:.0f}%)**
        
        {chr(10).join(feedback)}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Key Skills Required
    st.markdown("## 🛠️ Key Skills & Knowledge Areas")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔗 Version Control
        - Git commands (clone, branch, merge)
        - Repository management
        - Collaboration workflows
        - **Example**: Feature branch workflow
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗃️ Data Structures
        - Arrays, Lists, Trees, Graphs
        - Algorithm complexity
        - Distributed computing concepts
        - **Example**: Tree for hierarchical data
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔧 Infrastructure as Code
        - CloudFormation templates
        - AWS CDK applications
        - Repeatable deployments
        - **Example**: Serverless applications
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example: Git Workflow
    st.markdown("## 💻 Code Example: Git Workflow for Data Pipelines")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Complete Git workflow for data engineering projects

# 1. Clone repository
git clone https://github.com/company/data-pipeline.git
cd data-pipeline

# 2. Create feature branch for new ETL job
git checkout -b feature/new-etl-job

# 3. Make changes to pipeline code
# Edit files: src/etl_jobs/user_analytics.py, tests/test_user_analytics.py

# 4. Stage and commit changes
git add src/etl_jobs/user_analytics.py
git add tests/test_user_analytics.py
git commit -m "feat: add user analytics ETL job

- Process user interaction data from Kinesis
- Transform and load to Redshift
- Add comprehensive unit tests
- Update documentation"

# 5. Push feature branch
git push origin feature/new-etl-job

# 6. Create pull request (via GitHub/GitLab UI)
# This triggers CI/CD pipeline:
# - Runs automated tests
# - Security scanning
# - Code quality checks

# 7. After approval, merge to main
git checkout main
git pull origin main
git merge feature/new-etl-job

# 8. Deploy to production (automated via CI/CD)
git push origin main

# 9. Clean up feature branch
git branch -d feature/new-etl-job
git push origin --delete feature/new-etl-job

# 10. Tag release
git tag -a v1.2.0 -m "Release v1.2.0: User analytics ETL"
git push origin v1.2.0
    ''', language='bash')
    st.markdown('</div>', unsafe_allow_html=True)

def cicd_tab():
    """Content for CI/CD tab"""
    st.markdown("# 🔄 Continuous Integration & Continuous Delivery")
    st.markdown("*Automate your data pipeline deployments*")
    
    # CI/CD Pipeline Visualization  
    st.markdown("## 🏗️ CI/CD Pipeline Architecture")
    common.mermaid(create_cicd_pipeline_mermaid(), height=400)
    
    # Interactive CI/CD Pipeline Builder
    st.markdown("## 🛠️ Interactive CI/CD Pipeline Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Source Configuration")
        repo_name = st.text_input("Repository Name:", "my-data-pipeline")
        trigger_branch = st.selectbox("Trigger Branch:", ["main", "develop", "master"])
        
        st.markdown("### Build Configuration")
        build_commands = st.multiselect("Build Steps:", [
            "Run unit tests", "Code quality scan", "Security scan", 
            "Package Lambda functions", "Update CloudFormation templates"
        ], default=["Run unit tests", "Package Lambda functions"])
    
    with col2:
        st.markdown("### Deployment Stages")
        stages = st.multiselect("Deployment Stages:", [
            "Development", "Staging", "Production"
        ], default=["Development", "Staging"])
        
        approval_required = st.checkbox("Manual approval for production", value=True)
        rollback_enabled = st.checkbox("Enable automatic rollback", value=True)
    
    if st.button("🚀 Create Pipeline (Simulation)", use_container_width=True):
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ CI/CD Pipeline Created Successfully!
        
        **Pipeline Configuration:**
        - **Repository**: {repo_name}
        - **Trigger**: Push to {trigger_branch} branch
        - **Build Steps**: {', '.join(build_commands)}
        - **Deployment Stages**: {' → '.join(stages)}
        - **Production Approval**: {'Required' if approval_required else 'Automatic'}
        - **Rollback**: {'Enabled' if rollback_enabled else 'Disabled'}
        
        🔄 Pipeline is now monitoring for changes to {trigger_branch} branch!
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Benefits of CI/CD
    st.markdown("## ✨ Benefits of CI/CD for Data Pipelines")
    
    benefits_data = {
        'Benefit': ['Faster Deployments', 'Reduced Errors', 'Better Testing', 'Team Collaboration', 'Rollback Speed'],
        'Without CI/CD': [3, 2, 2, 3, 1],
        'With CI/CD': [9, 9, 10, 8, 9]
    }
    
    df_benefits = pd.DataFrame(benefits_data)
    
    fig = go.Figure()
    fig.add_trace(go.Bar(name='Without CI/CD', x=df_benefits['Benefit'], y=df_benefits['Without CI/CD'], 
                        marker_color=AWS_COLORS['warning']))
    fig.add_trace(go.Bar(name='With CI/CD', x=df_benefits['Benefit'], y=df_benefits['With CI/CD'], 
                        marker_color=AWS_COLORS['primary']))
    
    fig.update_layout(barmode='group', title='CI/CD Impact on Development Process', 
                     yaxis_title='Effectiveness Score (1-10)')
    st.plotly_chart(fig, use_container_width=True)
    
    # AWS Glue CI/CD Example
    st.markdown("## 💻 Code Example: AWS Glue Job CI/CD with CodePipeline")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# buildspec.yml for AWS CodeBuild
version: 0.2

phases:
  install:
    runtime-versions:
      python: 3.9
    commands:
      - pip install pytest boto3 awscli
      
  pre_build:
    commands:
      - echo Logging in to Amazon ECR...
      - aws --version
      - echo Running pre-build checks...
      
  build:
    commands:
      - echo Build started on `date`
      - echo Running unit tests...
      - python -m pytest tests/ -v
      - echo Running code quality checks...
      - flake8 src/ --count --select=E9,F63,F7,F82 --show-source --statistics
      - echo Packaging Glue job scripts...
      - aws s3 cp src/glue_jobs/ s3://$ARTIFACT_BUCKET/glue-scripts/ --recursive
      
  post_build:
    commands:
      - echo Build completed on `date`
      - echo Deploying CloudFormation templates...
      - aws cloudformation deploy --template-file cfn-templates/glue-job.yaml 
        --stack-name $STACK_NAME --parameter-overrides 
        GlueScriptLocation=s3://$ARTIFACT_BUCKET/glue-scripts/
        
artifacts:
  files:
    - '**/*'
''', language='yaml')
    st.markdown('</div>', unsafe_allow_html=True)

def lambda_optimization_tab():
    """Content for Lambda Optimization tab"""
    st.markdown("# ⚡ AWS Lambda Optimization")
    st.markdown("*Optimize serverless functions for performance and cost*")
    
    # Lambda Performance Factors
    st.markdown("## 🏗️ Lambda Performance Optimization")
    common.mermaid(create_lambda_performance_mermaid(), height=350)
    
    # Interactive Lambda Performance Calculator
    st.markdown("## 🧮 Interactive Lambda Performance Calculator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Function Configuration")
        memory_mb = st.selectbox("Memory (MB):", [128, 256, 512, 1024, 2048, 3008])
        duration_ms = st.slider("Average Duration (ms):", 100, 15000, 1000)
        invocations_month = st.number_input("Monthly Invocations:", 1, 10000000, 100000)
    
    with col2:
        st.markdown("### Execution Model")
        execution_model = st.selectbox("Execution Model:", [
            "Synchronous (API Gateway)", "Asynchronous (S3 Events)", "Stream (Kinesis)"
        ])
        
        cold_starts = st.slider("% Cold Starts:", 0, 100, 10)
        
    # Calculate costs and performance metrics
    gb_seconds = (memory_mb / 1024) * (duration_ms / 1000) * invocations_month
    compute_cost = gb_seconds * 0.0000166667  # AWS Lambda pricing
    request_cost = invocations_month * 0.0000002  # Request pricing
    total_cost = compute_cost + request_cost
    
    # Cold start impact
    cold_start_delay = 200 if memory_mb < 512 else 100  # Simplified calculation
    avg_with_cold_start = duration_ms + (cold_starts / 100 * cold_start_delay)
    
    if st.button("📊 Calculate Performance & Cost", use_container_width=True):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            ### 💰 Monthly Cost
            **${total_cost:.2f}**
            Compute: ${compute_cost:.2f}
            Requests: ${request_cost:.2f}
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            ### ⚡ Avg Duration
            **{avg_with_cold_start:.0f}ms**
            Base: {duration_ms}ms
            + Cold Start Impact
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            ### 🧠 vCPU Power
            **{memory_mb/1024:.2f} vCPU**
            Linear scaling
            with memory
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col4:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            ### 🔥 Cold Starts
            **{cold_starts}%**
            {invocations_month * cold_starts // 100:,} per month
            """)
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Lambda Execution Models
    st.markdown("## 🔄 Lambda Execution Models")
    st.plotly_chart(create_lambda_execution_comparison(), use_container_width=True)
    
    # Best Practices
    st.markdown("## 💡 Lambda Optimization Best Practices")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🚀 Performance Optimization
        - **Right-size memory allocation** - Balance cost vs performance
        - **Use provisioned concurrency** for predictable workloads
        - **Connection pooling** - Reuse database connections
        - **Lazy loading** - Load dependencies when needed
        - **Optimize package size** - Smaller packages = faster cold starts
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🛡️ Reliability & Monitoring
        - **Set appropriate timeouts** - Prevent hanging functions
        - **Implement retry logic** - Handle transient failures
        - **Use dead letter queues** - Capture failed async invocations
        - **Monitor with CloudWatch** - Track performance metrics
        - **Error handling** - Graceful failure management
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Optimized Lambda Function")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
import json
import boto3
import os
from typing import Dict, Any
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients outside handler (connection reuse)
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Optimized Lambda function for processing S3 events
    """
    try:
        # Early validation
        if 'Records' not in event:
            raise ValueError("Invalid event format")
        
        results = []
        
        for record in event['Records']:
            # Extract S3 event details
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            logger.info(f"Processing file: s3://{bucket}/{key}")
            
            # Process file efficiently
            result = process_s3_file(bucket, key)
            results.append(result)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully processed {len(results)} files',
                'results': results
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        
        # For async invocations, raise to trigger retry
        if context.invoked_function_arn.endswith(':$LATEST'):
            raise
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def process_s3_file(bucket: str, key: str) -> Dict[str, Any]:
    """
    Process individual S3 file with error handling
    """
    try:
        # Stream large files instead of loading entirely into memory
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Process data in chunks for memory efficiency
        line_count = 0
        for line in response['Body'].iter_lines():
            line_count += 1
            # Process each line...
        
        # Update DynamoDB with batch operations for efficiency
        with table.batch_writer() as batch:
            batch.put_item(Item={
                'file_key': key,
                'line_count': line_count,
                'processed_at': int(time.time())
            })
        
        return {
            'file': key,
            'lines_processed': line_count,
            'status': 'success'
        }
        
    except Exception as e:
        logger.error(f"Error processing file {key}: {str(e)}")
        return {
            'file': key,
            'status': 'failed',
            'error': str(e)
        }

# Optimization techniques used:
# 1. Client initialization outside handler
# 2. Streaming file processing (memory efficient)
# 3. Batch DynamoDB operations
# 4. Proper error handling and logging
# 5. Early validation to fail fast
# 6. Environment variable configuration
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def infrastructure_code_tab():
    """Content for Infrastructure as Code tab"""
    st.markdown("# 🏗️ Infrastructure as Code")
    st.markdown("*Manage AWS resources with code for repeatable deployments*")
    
    # IaC Comparison
    st.markdown("## 🔧 Infrastructure as Code Options")
    common.mermaid(create_iac_comparison_mermaid(), height=350)
    
    # Interactive IaC Template Builder
    st.markdown("## 🛠️ Interactive CloudFormation Template Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Resources to Create")
        resources = st.multiselect("Select AWS Resources:", [
            "S3 Bucket", "Lambda Function", "DynamoDB Table", 
            "CloudWatch Log Group", "IAM Role", "API Gateway"
        ])
        
        environment = st.selectbox("Environment:", ["dev", "staging", "prod"])
    
    with col2:
        st.markdown("### Configuration")
        project_name = st.text_input("Project Name:", "my-data-pipeline")
        enable_encryption = st.checkbox("Enable encryption", value=True)
        enable_versioning = st.checkbox("Enable versioning", value=True)
    
    if st.button("🏗️ Generate CloudFormation Template", use_container_width=True):
        template = generate_cloudformation_template(resources, project_name, environment, 
                                                  enable_encryption, enable_versioning)
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ CloudFormation Template Generated!
        
        **Template includes:**
        - Resources: {', '.join(resources)}
        - Environment: {environment}
        - Encryption: {'Enabled' if enable_encryption else 'Disabled'}
        - Versioning: {'Enabled' if enable_versioning else 'Disabled'}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Display generated template
        st.markdown("### Generated CloudFormation Template:")
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code(template, language='yaml')
        st.markdown('</div>', unsafe_allow_html=True)
    
    # IaC Benefits
    st.markdown("## ✨ Benefits of Infrastructure as Code")
    
    iac_data = {
        'Aspect': ['Consistency', 'Repeatability', 'Version Control', 'Documentation', 'Rollback Speed'],
        'Manual Process': [3, 2, 1, 2, 2],
        'Infrastructure as Code': [10, 10, 10, 9, 9]
    }
    
    df_iac = pd.DataFrame(iac_data)
    
    fig = px.bar(df_iac, x='Aspect', y=['Manual Process', 'Infrastructure as Code'],
                 title='IaC vs Manual Infrastructure Management',
                 barmode='group',
                 color_discrete_sequence=[AWS_COLORS['warning'], AWS_COLORS['primary']])
    
    st.plotly_chart(fig, use_container_width=True)
    
    # AWS SAM Example
    st.markdown("## 🚀 AWS Serverless Application Model (SAM)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 What is AWS SAM?
        
        **AWS SAM** is an open-source framework for building serverless applications. It extends CloudFormation with:
        
        - **Simplified syntax** for serverless resources
        - **Local testing** capabilities
        - **Built-in best practices** for serverless
        - **Easy deployment** with SAM CLI
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔧 SAM CLI Commands
        
        ```bash
        # Initialize new SAM app
        sam init
        
        # Build application
        sam build
        
        # Test locally
        sam local start-api
        
        # Deploy to AWS
        sam deploy --guided
        ```
        """)
        st.markdown('</div>', unsafe_allow_html=True)

def generate_cloudformation_template(resources, project_name, environment, encryption, versioning):
    """Generate CloudFormation template based on selected resources"""
    template = f"""AWSTemplateFormatVersion: '2010-09-09'
Description: '{project_name} infrastructure for {environment} environment'

Parameters:
  ProjectName:
    Type: String
    Default: {project_name}
  Environment:
    Type: String
    Default: {environment}

Resources:"""
    
    if "S3 Bucket" in resources:
        template += f"""
  DataBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub '${{ProjectName}}-data-${{Environment}}-${{AWS::AccountId}}'
      {'BucketEncryption:' if encryption else ''}
      {'  ServerSideEncryptionConfiguration:' if encryption else ''}
      {'    - ServerSideEncryptionByDefault:' if encryption else ''}
      {'        SSEAlgorithm: AES256' if encryption else ''}
      {'VersioningConfiguration:' if versioning else ''}
      {'  Status: Enabled' if versioning else ''}"""
    
    if "Lambda Function" in resources:
        template += f"""
  DataProcessorFunction:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub '${{ProjectName}}-processor-${{Environment}}'
      Runtime: python3.9
      Handler: index.handler
      Code:
        ZipFile: |
          import json
          def handler(event, context):
              return {{'statusCode': 200, 'body': 'Hello from Lambda!'}}
      Role: !GetAtt LambdaExecutionRole.Arn"""
    
    if "DynamoDB Table" in resources:
        template += f"""
  DataTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: !Sub '${{ProjectName}}-data-${{Environment}}'
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: id
          AttributeType: S
      KeySchema:
        - AttributeName: id
          KeyType: HASH
      {'SSESpecification:' if encryption else ''}
      {'  SSEEnabled: true' if encryption else ''}"""
    
    if "IAM Role" in resources:
        template += """
  LambdaExecutionRole:
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
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"""
    
    template += """

Outputs:
  StackName:
    Description: 'Name of the CloudFormation stack'
    Value: !Ref AWS::StackName"""
    
    return template

def analytics_pipeline_tab():
    """Content for Analytics Pipeline tab"""
    st.markdown("# 📊 Game Analytics Pipeline Example")
    st.markdown("*Real-world data analytics pipeline architecture*")
    
    # Pipeline Architecture
    st.markdown("## 🏗️ Analytics Pipeline Architecture")
    common.mermaid(create_analytics_pipeline_mermaid(), height=400)
    
    # Interactive Pipeline Builder
    st.markdown("## 🎮 Game Analytics Pipeline Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Data Sources")
        game_events = st.multiselect("Game Events to Track:", [
            "Player Login/Logout", "Level Completion", "Item Purchases", 
            "Achievement Unlocks", "Social Interactions", "Crash Reports"
        ], default=["Player Login/Logout", "Level Completion"])
        
        data_volume = st.selectbox("Expected Data Volume:", [
            "Low (< 1GB/day)", "Medium (1-10GB/day)", 
            "High (10-100GB/day)", "Very High (> 100GB/day)"
        ])
    
    with col2:
        st.markdown("### Processing Requirements")
        real_time = st.checkbox("Real-time analytics", value=True)
        batch_processing = st.checkbox("Daily batch processing", value=True)
        
        analytics_features = st.multiselect("Analytics Features:", [
            "Player Retention Analysis", "Revenue Analytics", 
            "Performance Monitoring", "A/B Testing", "Fraud Detection"
        ])
    
    # Pipeline Components Selection
    st.markdown("### Select Pipeline Components")
    col3, col4, col5 = st.columns(3)
    
    with col3:
        st.markdown("**Data Ingestion**")
        ingestion = st.selectbox("Ingestion Service:", [
            "Kinesis Data Streams", "Kinesis Firehose", "MSK (Kafka)"
        ])
    
    with col4:
        st.markdown("**Data Processing**") 
        processing = st.selectbox("Processing Service:", [
            "AWS Glue", "Lambda Functions", "EMR Spark", "Kinesis Analytics"
        ])
    
    with col5:
        st.markdown("**Data Storage**")
        storage = st.selectbox("Storage Service:", [
            "S3 Data Lake", "Redshift", "DynamoDB", "Hybrid (S3 + Redshift)"
        ])
    
    if st.button("🚀 Generate Pipeline Architecture", use_container_width=True):
        # Calculate estimated costs
        base_cost = 100  # Base monthly cost
        volume_multiplier = {"Low (< 1GB/day)": 1, "Medium (1-10GB/day)": 3, 
                           "High (10-100GB/day)": 10, "Very High (> 100GB/day)": 30}
        
        estimated_cost = base_cost * volume_multiplier[data_volume]
        if real_time:
            estimated_cost *= 1.5
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        ### ✅ Analytics Pipeline Generated!
        
        **Pipeline Configuration:**
        - **Game Events**: {', '.join(game_events)}
        - **Data Volume**: {data_volume}
        - **Ingestion**: {ingestion}
        - **Processing**: {processing}
        - **Storage**: {storage}
        - **Real-time**: {'Enabled' if real_time else 'Disabled'}
        - **Analytics Features**: {', '.join(analytics_features)}
        
        💰 **Estimated Monthly Cost**: ${estimated_cost:,}
        ⚡ **Latency**: {'< 1 second' if real_time else '< 1 hour'}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Pipeline Benefits
    st.markdown("## 📈 Analytics Pipeline Benefits")
    
    benefit_data = {
        'Metric': ['Data Processing Speed', 'Insights Quality', 'Scalability', 'Cost Efficiency'],
        'Before Pipeline': [2, 3, 2, 3],
        'With Pipeline': [9, 9, 10, 8]
    }
    
    df_benefit = pd.DataFrame(benefit_data)
    
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=[2, 3, 2, 3, 2],
        theta=['Speed', 'Quality', 'Scalability', 'Cost', 'Speed'],
        fill='toself',
        name='Before Pipeline',
        line_color=AWS_COLORS['warning']
    ))
    fig.add_trace(go.Scatterpolar(
        r=[9, 9, 10, 8, 9],
        theta=['Speed', 'Quality', 'Scalability', 'Cost', 'Speed'],
        fill='toself',
        name='With Pipeline',
        line_color=AWS_COLORS['primary']
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 10]
            )),
        showlegend=True,
        title="Impact of Analytics Pipeline"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Game Analytics Pipeline")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
# Game Analytics Pipeline Implementation
import boto3
import json
from datetime import datetime
import uuid

class GameAnalyticsPipeline:
    def __init__(self):
        self.kinesis = boto3.client('kinesis')
        self.s3 = boto3.client('s3')
        self.glue = boto3.client('glue')
        
    def send_game_event(self, player_id: str, event_type: str, event_data: dict):
        """Send game event to Kinesis stream"""
        
        # Enrich event with metadata
        enriched_event = {
            'event_id': str(uuid.uuid4()),
            'player_id': player_id,
            'event_type': event_type,
            'timestamp': datetime.utcnow().isoformat(),
            'session_id': event_data.get('session_id'),
            'game_version': event_data.get('game_version', '1.0.0'),
            'platform': event_data.get('platform', 'unknown'),
            'event_data': event_data
        }
        
        # Partition by player_id for better distribution
        partition_key = f"player_{player_id}"
        
        try:
            response = self.kinesis.put_record(
                StreamName='game-events-stream',
                Data=json.dumps(enriched_event),
                PartitionKey=partition_key
            )
            
            print(f"Event sent successfully: {response['SequenceNumber']}")
            return response
            
        except Exception as e:
            print(f"Error sending event: {e}")
            raise
    
    def process_player_session(self, session_events: list):
        """Process complete player session for analytics"""
        
        analytics = {
            'session_duration': 0,
            'levels_completed': 0,
            'items_purchased': 0,
            'achievements_unlocked': 0,
            'revenue_generated': 0.0
        }
        
        session_start = None
        session_end = None
        
        for event in session_events:
            event_time = datetime.fromisoformat(event['timestamp'])
            
            if not session_start or event_time < session_start:
                session_start = event_time
            if not session_end or event_time > session_end:
                session_end = event_time
                
            # Process different event types
            if event['event_type'] == 'level_completed':
                analytics['levels_completed'] += 1
                
            elif event['event_type'] == 'item_purchased':
                analytics['items_purchased'] += 1
                analytics['revenue_generated'] += event['event_data'].get('price', 0)
                
            elif event['event_type'] == 'achievement_unlocked':
                analytics['achievements_unlocked'] += 1
        
        # Calculate session duration
        if session_start and session_end:
            analytics['session_duration'] = (session_end - session_start).seconds
        
        return analytics
    
    def trigger_etl_pipeline(self, date: str):
        """Trigger daily ETL job using AWS Glue"""
        
        job_name = 'game-analytics-etl'
        
        try:
            response = self.glue.start_job_run(
                JobName=job_name,
                Arguments={
                    '--processing_date': date,
                    '--source_bucket': 'game-events-raw',
                    '--target_bucket': 'game-analytics-processed',
                    '--enable-metrics': 'true'
                }
            )
            
            print(f"ETL job started: {response['JobRunId']}")
            return response
            
        except Exception as e:
            print(f"Error starting ETL job: {e}")
            raise

# Usage example
pipeline = GameAnalyticsPipeline()

# Send game events
pipeline.send_game_event(
    player_id='player_123',
    event_type='level_completed',
    event_data={
        'level': 5,
        'score': 8500,
        'duration': 120,
        'session_id': 'sess_456'
    }
)

pipeline.send_game_event(
    player_id='player_123', 
    event_type='item_purchased',
    event_data={
        'item_id': 'sword_legendary',
        'price': 4.99,
        'currency': 'USD'
    }
)

# Trigger daily processing
pipeline.trigger_etl_pipeline('2025-07-14')
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def query_optimization_tab():
    """Content for Query Optimization tab"""
    st.markdown("# ⚡ SQL Query Optimization")
    st.markdown("*Optimize queries for better performance on Amazon Redshift*")
    
    # Query Performance Factors
    st.markdown("## 🎯 Query Performance Factors")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗂️ Indexing Strategy
        - **Sort Keys** - Order data on disk
        - **Distribution Keys** - Distribute data across nodes  
        - **Column Store** - Compress similar data
        - **Zone Maps** - Skip irrelevant blocks
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📝 Query Structure
        - **Avoid SELECT \*** - Only select needed columns
        - **Filter Early** - Use WHERE clauses effectively
        - **Join Optimization** - Proper join order
        - **Subquery Strategy** - When to use vs JOINs
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🗃️ Data Format
        - **Columnar Format** - Parquet, ORC
        - **Compression** - GZIP, Snappy
        - **Partitioning** - By date, region, etc.
        - **File Size** - Optimal 64MB-1GB per file
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Query Optimizer
    st.markdown("## 🔧 Interactive Query Optimizer")
    
    # Sample query input
    sample_queries = {
        "Poor Performance Query": """SELECT * FROM sales s 
JOIN customers c ON s.customer_id = c.id 
WHERE s.order_date >= '2025-01-01' 
ORDER BY s.order_date;""",
        
        "Optimized Query": """SELECT s.order_id, s.total_amount, c.customer_name 
FROM sales s 
JOIN customers c ON s.customer_id = c.id 
WHERE s.order_date >= '2025-01-01' 
  AND s.order_date < '2025-08-01'
ORDER BY s.order_date;""",
        
        "Custom Query": ""
    }
    
    query_type = st.selectbox("Choose Query Template:", list(sample_queries.keys()))
    
    if query_type == "Custom Query":
        user_query = st.text_area("Enter your SQL query:", height=150)
    else:
        user_query = st.text_area("SQL Query:", sample_queries[query_type], height=150)
    
    col1, col2 = st.columns(2)
    
    with col1:
        table_size = st.selectbox("Table Size:", [
            "Small (< 1M rows)", "Medium (1M-10M rows)", 
            "Large (10M-100M rows)", "Very Large (> 100M rows)"
        ])
        
    with col2:
        data_format = st.selectbox("Data Format:", [
            "Row-based (JSON)", "Columnar (Parquet)", "Mixed Format"
        ])
    
    if st.button("🚀 Analyze Query Performance", use_container_width=True):
        # Simulate query analysis
        performance_score, recommendations = analyze_query_performance(
            user_query, table_size, data_format, query_type
        )
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            ### 📊 Performance Score
            **{performance_score}/100**
            {'🟢 Excellent' if performance_score >= 80 else '🟡 Good' if performance_score >= 60 else '🔴 Needs Work'}
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            est_time = calculate_estimated_time(performance_score, table_size)
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            ### ⏱️ Est. Runtime
            **{est_time}**
            Based on query
            complexity
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            cost_impact = "Low" if performance_score >= 70 else "Medium" if performance_score >= 50 else "High"
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f"""
            ### 💰 Cost Impact
            **{cost_impact}**
            Compute resource
            usage
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Show recommendations
        if recommendations:
            st.markdown('<div class="warning-box">', unsafe_allow_html=True)
            st.markdown("### 💡 Optimization Recommendations")
            for rec in recommendations:
                st.markdown(f"- {rec}")
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Format Comparison
    st.markdown("## 📈 Data Format Performance Comparison")
    
    format_data = {
        'Format': ['JSON (Row-based)', 'Parquet (Columnar)', 'ORC (Columnar)'],
        'Query Speed': [3, 9, 8],
        'Storage Efficiency': [2, 9, 8],
        'Compression Ratio': [3, 9, 9],
        'Read Performance': [3, 10, 9]
    }
    
    df_format = pd.DataFrame(format_data)
    
    fig = go.Figure()
    
    for format_name in df_format['Format']:
        row = df_format[df_format['Format'] == format_name].iloc[0]
        fig.add_trace(go.Scatterpolar(
            r=[row['Query Speed'], row['Storage Efficiency'], 
               row['Compression Ratio'], row['Read Performance']],
            theta=['Query Speed', 'Storage Efficiency', 'Compression Ratio', 'Read Performance'],
            fill='toself',
            name=format_name
        ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 10])
        ),
        showlegend=True,
        title="Data Format Performance Comparison"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Best Practices
    st.markdown("## 💻 Code Example: Redshift Query Optimization")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
-- BEFORE: Poorly optimized query
SELECT * 
FROM sales s, customers c, products p
WHERE s.customer_id = c.customer_id 
  AND s.product_id = p.product_id
  AND s.order_date > '2025-01-01'
ORDER BY s.order_date;

-- AFTER: Optimized query
SELECT s.order_id, 
       s.total_amount,
       c.customer_name,
       p.product_name
FROM sales s 
JOIN customers c ON s.customer_id = c.customer_id
JOIN products p ON s.product_id = p.product_id
WHERE s.order_date >= '2025-01-01' 
  AND s.order_date < '2025-08-01'
  AND s.total_amount > 0
ORDER BY s.order_date
LIMIT 1000;

-- Optimization techniques used:
-- 1. SELECT specific columns instead of *
-- 2. Use explicit JOINs instead of comma syntax
-- 3. Use range conditions for better performance
-- 4. Add LIMIT to restrict result set
-- 5. Use >= and < for date ranges (better than >)

-- Additional Redshift-specific optimizations:

-- Create table with proper DISTKEY and SORTKEY
CREATE TABLE sales_optimized (
    order_id BIGINT,
    customer_id INT,
    product_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2)
)
DISTKEY(customer_id)  -- Distribute by frequently joined column
SORTKEY(order_date);  -- Sort by frequently filtered column

-- Use COPY command for efficient data loading
COPY sales_optimized 
FROM 's3://my-bucket/sales-data/'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftRole'
FORMAT AS PARQUET;

-- Analyze table statistics for better query planning
ANALYZE sales_optimized;

-- Use EXPLAIN to understand query execution plan
EXPLAIN SELECT customer_id, SUM(total_amount)
FROM sales_optimized
WHERE order_date >= '2025-01-01'
GROUP BY customer_id;

-- Redshift-specific query hints
SELECT customer_id, SUM(total_amount)
FROM sales_optimized
WHERE order_date >= '2025-01-01'
GROUP BY customer_id
HAVING SUM(total_amount) > 1000
ORDER BY 2 DESC;
    ''', language='sql')
    st.markdown('</div>', unsafe_allow_html=True)

def analyze_query_performance(query, table_size, data_format, query_type):
    """Analyze query performance and provide recommendations"""
    score = 100
    recommendations = []
    
    # Check for SELECT *
    if "SELECT *" in query.upper():
        score -= 20
        recommendations.append("Avoid SELECT * - specify only needed columns")
    
    # Check for proper WHERE clauses
    if "WHERE" not in query.upper():
        score -= 15
        recommendations.append("Add WHERE clause to filter data early")
    
    # Check for ORDER BY without LIMIT
    if "ORDER BY" in query.upper() and "LIMIT" not in query.upper():
        score -= 10
        recommendations.append("Consider adding LIMIT when using ORDER BY")
    
    # Adjust for table size
    size_penalty = {"Small (< 1M rows)": 0, "Medium (1M-10M rows)": 5, 
                   "Large (10M-100M rows)": 15, "Very Large (> 100M rows)": 25}
    score -= size_penalty[table_size]
    
    # Adjust for data format
    if data_format == "Row-based (JSON)":
        score -= 20
        recommendations.append("Convert to columnar format (Parquet) for better performance")
    elif data_format == "Mixed Format":
        score -= 10
        recommendations.append("Standardize on columnar format for consistency")
    
    # Bonus for optimized queries
    if query_type == "Optimized Query":
        score += 10
    
    score = max(0, min(100, score))
    
    return score, recommendations

def calculate_estimated_time(score, table_size):
    """Calculate estimated query runtime based on performance score"""
    base_times = {
        "Small (< 1M rows)": 1,
        "Medium (1M-10M rows)": 10, 
        "Large (10M-100M rows)": 60,
        "Very Large (> 100M rows)": 300
    }
    
    base_time = base_times[table_size]
    multiplier = (100 - score) / 100 + 0.5
    
    estimated_seconds = base_time * multiplier
    
    if estimated_seconds < 60:
        return f"{estimated_seconds:.1f}s"
    elif estimated_seconds < 3600:
        return f"{estimated_seconds/60:.1f}m"
    else:
        return f"{estimated_seconds/3600:.1f}h"

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
    # 💻 Programming Concepts
    ### Master essential programming concepts for data engineering on AWS
    """)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "💻 Programming Concepts",
        "🔄 CI/CD Pipelines", 
        "⚡ Lambda Optimization",
        "🏗️ Infrastructure as Code",
        "📊 Analytics Pipeline",
        "⚡ Query Optimization"
    ])
    
    with tab1:
        programming_concepts_tab()
    
    with tab2:
        cicd_tab()
    
    with tab3:
        lambda_optimization_tab()
    
    with tab4:
        infrastructure_code_tab()
        
    with tab5:
        analytics_pipeline_tab()
    
    with tab6:
        query_optimization_tab()
    
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
