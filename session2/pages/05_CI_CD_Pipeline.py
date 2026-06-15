"""
Page 5: CI/CD & Programming Concepts

Covers Task 1.4: Apply programming concepts
- CI/CD for data pipelines (CodePipeline pattern)
- Lambda concurrency and memory optimization
- Infrastructure as Code (CDK/SAM/CloudFormation)
- Git workflows for data engineering
"""
import streamlit as st
import boto3
import json
from datetime import datetime, timezone

boto3.setup_default_session(region_name="ap-southeast-1")

st.set_page_config(page_title="CI/CD Pipeline", page_icon="🚀", layout="wide")

st.markdown("# 🚀 CI/CD & Programming Concepts (Task 1.4)")
st.markdown("""
Explore how CI/CD, Lambda optimization, and Infrastructure as Code
apply to data engineering pipelines on AWS.
""")

import utils.common as common


# ============================================================
# CI/CD Pipeline Architecture
# ============================================================
st.markdown("## 🔄 CI/CD for Data Pipelines")

cicd_mermaid = """
graph LR
    A[👨‍💻 Developer] -->|git push| B[CodeCommit]
    B -->|trigger| C[CodePipeline]
    C --> D[CodeBuild<br/>Unit Tests]
    D --> E[Deploy to QA<br/>CloudFormation]
    E --> F[Integration Tests]
    F --> G[Deploy to Prod<br/>CloudFormation]
    
    classDef dev fill:#FFE4B5,stroke:#FF9900,stroke-width:2px
    classDef pipeline fill:#E3F2FD,stroke:#2196F3,stroke-width:2px
    classDef deploy fill:#E8F5E8,stroke:#4CAF50,stroke-width:2px
    
    class A dev
    class B,C,D,F pipeline
    class E,G deploy
"""

common.mermaid(cicd_mermaid, height=200, show_controls=False)

st.markdown("""
**How this works for Glue ETL jobs:**
1. Developer modifies ETL script in CodeCommit
2. Push triggers CodePipeline
3. CodeBuild runs unit tests on the PySpark logic
4. Lambda uploads script to S3
5. Glue job is updated/created with the new script
""")

st.markdown("---")

# ============================================================
# Lambda Optimization
# ============================================================
st.markdown("## ⚡ Lambda Optimization")

st.markdown("""
Memory is the **primary lever** for Lambda performance. More memory = more CPU = faster execution.
The exam tests your understanding of this relationship.
""")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    ### Memory → CPU Relationship
    | Memory | vCPU | Use Case |
    |--------|------|----------|
    | 128 MB | 0.08 | Simple transforms |
    | 512 MB | 0.33 | JSON parsing |
    | 1024 MB | 0.58 | Data processing |
    | 1769 MB | 1.00 | Full vCPU |
    | 3008 MB | 1.75 | Compute-heavy |
    | 10240 MB | 6.00 | Max performance |
    """)

with col2:
    st.markdown("""
    ### Concurrency Settings
    | Type | Behavior |
    |------|----------|
    | **Unreserved** | Shared pool (default) |
    | **Reserved** | Guaranteed capacity |
    | **Provisioned** | Pre-warmed (no cold start) |
    
    **Max timeout:** 15 minutes  
    **Cold start mitigation:** Provisioned concurrency
    """)

# Lambda execution models
st.markdown("### Execution Models")

exec_mermaid = """
graph TD
    subgraph Synchronous
        A1[API Gateway] -->|Request/Response| L1[Lambda]
    end
    
    subgraph Asynchronous
        A2[S3 Event] -->|Fire and Forget| L2[Lambda]
        A3[SNS] -->|Fire and Forget| L2
    end
    
    subgraph Poll-based
        A4[Kinesis] -->|Event Source Mapping| L3[Lambda]
        A5[DynamoDB Streams] -->|Event Source Mapping| L3
    end
    
    classDef sync fill:#E3F2FD,stroke:#2196F3
    classDef async fill:#FFF3E0,stroke:#FF9800
    classDef poll fill:#E8F5E8,stroke:#4CAF50
    
    class A1,L1 sync
    class A2,A3,L2 async
    class A4,A5,L3 poll
"""

common.mermaid(exec_mermaid, height=400, show_controls=False)

st.markdown("---")

# ============================================================
# Infrastructure as Code
# ============================================================
st.markdown("## 🏗️ Infrastructure as Code (IaC)")

st.markdown("### This demo's CDK stack creates:")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    **Ingestion Layer**
    ```python
    kinesis.Stream(
        shard_count=2,
        retention_period=24h,
        stream_mode=PROVISIONED,
    )
    
    firehose.CfnDeliveryStream(
        buffer_interval=60s,
        compression=GZIP,
    )
    ```
    """)

with col2:
    st.markdown("""
    **Processing Layer**
    ```python
    lambda_.Function(
        runtime=PYTHON_3_12,
        memory_size=256,
        timeout=2min,
        event_source=KinesisESM(
            batch_size=100,
            parallelization_factor=2,
        )
    )
    ```
    """)

with col3:
    st.markdown("""
    **Orchestration Layer**
    ```python
    sfn.StateMachine(
        definition=
            Validate
            .next(GlueETL)
            .next(Notify),
        timeout=30min,
        tracing=True,
    )
    ```
    """)

st.markdown("---")

# ============================================================
# IaC Comparison
# ============================================================
st.markdown("### IaC Options on AWS")

st.markdown("""
| Tool | Language | Best For | Exam Focus |
|------|----------|----------|------------|
| **CloudFormation** | YAML/JSON | Declarative templates | Parameters, intrinsic functions |
| **AWS CDK** | Python/TS/Java | Programmatic infrastructure | Constructs, synthesis |
| **AWS SAM** | YAML (CFN extension) | Serverless apps | Lambda + API Gateway |
| **Terraform** | HCL | Multi-cloud | (Not on exam) |
""")

st.markdown("---")

# ============================================================
# SAM Example
# ============================================================
st.markdown("## 📦 AWS SAM — Serverless Deployment")

st.markdown("""
SAM simplifies deploying Lambda functions with API Gateway, event sources, etc.
""")

st.code("""
# template.yaml
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31

Resources:
  ProcessFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: index.handler
      Runtime: python3.12
      MemorySize: 512
      Timeout: 300
      Events:
        KinesisStream:
          Type: Kinesis
          Properties:
            Stream: !GetAtt DataStream.Arn
            StartingPosition: LATEST
            BatchSize: 100
""", language="yaml")

st.markdown("""
```bash
# SAM CLI commands
sam init                    # Create new project
sam build                   # Build artifacts
sam local invoke            # Test locally
sam deploy --guided         # Deploy to AWS
```
""")

# ============================================================
# Git for Data Engineering
# ============================================================
st.markdown("---")
st.markdown("## 🔀 Git for Data Engineering")

st.markdown("""
The exam expects knowledge of Git workflows for managing data pipeline code:

| Command | Use Case |
|---------|----------|
| `git clone` | Get a copy of the ETL repository |
| `git branch feature/new-transform` | Create a feature branch |
| `git commit -m "Add dedup step"` | Save changes |
| `git push origin feature/new-transform` | Push to remote |
| `git merge` | Merge approved changes to main |

**Best practice:** Use branching strategies (GitFlow, trunk-based) so ETL changes
go through code review before reaching production.
""")

# ============================================================
# Key Exam Points
# ============================================================
st.markdown("---")
st.markdown("## 💡 Exam Tips — Programming Concepts")

st.markdown("""
| Topic | Key Points |
|-------|-----------|
| **CI/CD** | CodePipeline + CodeBuild for automated Glue job deployment |
| **Lambda memory** | More memory = more CPU. 1769 MB = 1 full vCPU |
| **Lambda timeout** | Max 15 minutes. Use Step Functions for longer workflows |
| **Provisioned concurrency** | Eliminates cold starts. Costs more but guarantees latency |
| **IaC** | CDK synthesizes CloudFormation. SAM is a CFN transform for serverless |
| **SQL optimization** | Avoid SELECT *, use Parquet, partition data, use subqueries |
| **COPY vs UNLOAD** | COPY = S3→Redshift (inbound). UNLOAD = Redshift→S3 (outbound) |
""")
