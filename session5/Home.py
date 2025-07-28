import streamlit as st
import plotly.graph_objects as go
import utils.common as common
import utils.authenticate as authenticate


# Page configuration
st.set_page_config(
    page_title="Data Engineer Associate - Session 5",
    page_icon="🔒",
    layout="wide"
)

def main():

    # Custom CSS for AWS styling
    st.markdown("""
    <style>
    /* AWS Color Scheme */
    :root {
        --aws-orange: #FF9900;
        --aws-blue: #232F3E;
        --aws-light-blue: #4B9CD3;
        --aws-gray: #879196;
        --aws-white: #FFFFFF;
    }
    
    /* Main container styling */
    .main-header {
        background: linear-gradient(135deg, #232F3E 0%, #4B9CD3 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 1rem;
    }
    
    .program-card {
        background: white;
        padding: 1.5rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #FF9900;
        margin-bottom: 1rem;
    }
    
    .roadmap-item {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        border: 1px solid #e9ecef;
        transition: all 0.3s ease;
    }
    
    .roadmap-item:hover {
        background: #e3f2fd;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
    }
    
    .current-session {
        background: linear-gradient(135deg, #FF9900 0%, #FFB84D 100%);
        color: black;
        font-weight: bold;
    }
    
    .learning-outcome {
        background: #e8f5e8;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #4caf50;
        margin: 0.5rem 0;
    }
    
    .training-item {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #ddd;
        margin: 0.5rem 0;
        display: flex;
        align-items: center;
    }
    
    .training-icon {
        color: #FF9900;
        font-size: 1.5rem;
        margin-right: 1rem;
    }
    
    .service-card {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 1.5rem;
        border-radius: 8px;
        border-left: 4px solid #4B9CD3;
        margin-bottom: 1rem;
    }
    
    .security-feature {
        background: #d4edda;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #28a745;
        margin: 0.5rem 0;
    }
    
    .governance-card {
        background: #fff3cd;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #ffc107;
        margin: 0.5rem 0;
    }
    
    .comparison-table {
        background: white;
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 1rem 0;
    }
    
    .policy-example {
        background: #2d3748;
        color: #e2e8f0;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
        font-family: 'Courier New', monospace;
        border-left: 4px solid #FF9900;
    }
    
    .encryption-card {
        background: #e1f5fe;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #0288d1;
        margin: 0.5rem 0;
    }
    
    .audit-card {
        background: #fce4ec;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #e91e63;
        margin: 0.5rem 0;
    }
    
    .footer {
        text-align: center;
        padding: 1rem;
        background-color: #232F3E;
        color: white;
        margin-top: 1rem;
        border-radius: 8px;
    }
    
    /* Responsive design */
    @media (max-width: 768px) {
        .main-header {
            padding: 1rem;
        }
        .program-card {
            padding: 1rem;
        }
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Main header
    st.markdown("""
    <div class="main-header">
        <h1>AWS Partner Certification Readiness</h1>
        <h2>Data Engineer - Associate</h2>
        <h3>Content Review Session 5: Domain 4 - Data Security & Governance</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Program Check-in section
    st.markdown("## 📋 Program Check-in")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎉 Final Session - You're Almost Certified!</h4>
            <p>Welcome to the final content review session! This session focuses on Data Security 
            and Governance - the critical foundation for any data engineering solution. You'll master 
            authentication, authorization, encryption, auditing, and data governance principles. 
            After this session, you'll be ready for your certification exam!</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Progress indicator
        progress_value = 100  # 5th and final session
        st.metric("Program Progress", f"{progress_value}%", "Session 5 of 5 - Complete!")
        st.progress(progress_value / 100)
    
    # Program Roadmap
    st.markdown("## 🗺️ Program Roadmap")
    
    # Your Learning Journey - Mermaid Diagram
    st.markdown("### Your Certification Journey - Complete!")
    
    mermaid_code = """
    graph LR
        A[✅ Session 1<br/>AWS & Data<br/>Fundamentals] --> B[✅ Session 2<br/>Data Ingestion &<br/>Transformation]
        B --> C[✅ Session 3<br/>Data Store<br/>Management]
        C --> D[✅ Session 4<br/>Data Operations<br/>& Support]
        D --> E[🎯 Session 5<br/>Data Security &<br/>Governance]
        E --> F[🏆 Certification<br/>Exam Ready!]
        
        classDef completed fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef current fill:#FF9900,stroke:#232F3E,stroke-width:3px,color:#fff
        classDef target fill:#6f42c1,stroke:#232F3E,stroke-width:3px,color:#fff
        
        class A,B,C,D completed
        class E current
        class F target
    """
    
    common.mermaid(mermaid_code, height=150, show_controls=False)
    
    # Roadmap details
    roadmap_data = [
        {
            "session": "Content Review 1",
            "topic": "AWS & Data Fundamentals",
            "status": "completed",
            "description": "✅ Core AWS services, compute, networking, storage, and data fundamentals"
        },
        {
            "session": "Content Review 2", 
            "topic": "Domain 1: Data Ingestion and Transformation",
            "status": "completed",
            "description": "✅ Data ingestion patterns, transformation techniques, and processing frameworks"
        },
        {
            "session": "Content Review 3",
            "topic": "Domain 2: Data Store Management", 
            "status": "completed",
            "description": "✅ Data warehouses, data lakes, catalogs, and lifecycle management"
        },
        {
            "session": "Content Review 4",
            "topic": "Domain 3: Data Operations and Support",
            "status": "completed", 
            "description": "✅ Automation, analytics, monitoring, and data quality assurance"
        },
        {
            "session": "Content Review 5",
            "topic": "Domain 4: Data Security and Governance",
            "status": "current",
            "description": "🎯 Authentication, authorization, encryption, auditing, and governance"
        }
    ]
    
    for i, item in enumerate(roadmap_data):
        if item['status'] == 'current':
            status_class = "current-session"
            status_icon = "🎯"
        elif item['status'] == 'completed':
            status_class = "roadmap-item"
            status_icon = "✅"
        else:
            status_class = "roadmap-item"
            status_icon = "📅"
        
        st.markdown(f"""
        <div class="roadmap-item {status_class}">
            <h4>{status_icon} {item['session']}: {item['topic']}</h4>
            <p>{item['description']}</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Today's Learning Outcomes
    st.markdown("## 🎯 Today's Learning Outcomes")
    st.markdown("**During this final session, we will master:**")
    
    learning_outcomes = [
        "Task Statement 4.1: Apply authentication mechanisms",
        "Task Statement 4.2: Apply authorization mechanisms", 
        "Task Statement 4.3: Ensure data encryption and masking",
        "Task Statement 4.4: Prepare logs for audit",
        "Task Statement 4.5: Understand data privacy and governance"
    ]
    
    for i, outcome in enumerate(learning_outcomes):
        st.markdown(f"""
        <div class="learning-outcome">
            <strong>🛡️ {outcome}</strong>
        </div>
        """, unsafe_allow_html=True)
    
    # Task Statement 4.1: Apply authentication mechanisms
    st.markdown("## 🔐 Task Statement 4.1: Apply Authentication Mechanisms")
    
    # IAM Introduction
    st.markdown("### 👤 AWS Identity and Access Management (IAM)")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 What is AWS IAM?</h4>
            <p>AWS Identity and Access Management (IAM) is a web service that helps you securely 
            control access to AWS resources. With IAM, you can centrally manage permissions that 
            control which AWS resources users can access.</p>
            <h5>Core Components:</h5>
            <ul>
                <li><strong>Users:</strong> Individual identities with credentials</li>
                <li><strong>Groups:</strong> Collection of users with similar permissions</li>
                <li><strong>Roles:</strong> Temporary credentials for AWS services</li>
                <li><strong>Policies:</strong> Documents defining permissions</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### IAM Building Blocks")
        iam_mermaid = """
        graph TD
            A[IAM Root] --> B[Users]
            A --> C[Groups]
            A --> D[Roles]
            A --> E[Policies]
            
            B --> F[Access Keys]
            B --> G[Passwords]
            C --> H[Attached Policies]
            D --> I[Trust Policies]
            E --> J[Permissions]
            
            classDef root fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef identity fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef credential fill:#28a745,stroke:#232F3E,stroke-width:1px,color:#fff
            
            class A root
            class B,C,D,E identity
            class F,G,H,I,J credential
        """
        common.mermaid(iam_mermaid, height=200, show_controls=False)
    
    # Policy Interpretation Deep Dive
    st.markdown("### 📋 IAM Policy Interpretation Deep Dive")
    
    st.markdown("""
    <div class="program-card">
        <h4>🧠 Policy Evaluation Logic</h4>
        <p>Understanding how AWS evaluates policies is critical for both real-world implementation 
        and exam success. AWS uses a specific evaluation flow to determine access decisions.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Policy Example
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("### Sample IAM Policy")
        st.markdown("""
        <div class="policy-example">
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Deny",
      "Action": [
        "datapipeline:GetPipelineDefinition",
        "datapipeline:DescribePipelines"
      ],
      "Resource": "*",
      "Condition": {
        "StringNotEquals": {
          "datapipeline:PipelineCreator": "${aws:username}"
        }
      }
    }
  ]
}
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Policy Analysis")
        st.markdown("""
        <div class="security-feature">
            <h5>🔍 What does this policy do?</h5>
            <ul>
                <li><strong>Effect:</strong> Explicit Deny (strongest override)</li>
                <li><strong>Actions:</strong> Data Pipeline read operations</li>
                <li><strong>Resource:</strong> All pipeline resources (*)</li>
                <li><strong>Condition:</strong> Only if user is NOT the creator</li>
            </ul>
            <h6>Result:</h6>
            <p>Users can only view pipelines they created themselves. All other pipeline 
            access is explicitly denied, regardless of other Allow policies.</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Policy Evaluation Flow
    st.markdown("### 🔄 Policy Evaluation Flow")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("""
        <div class="security-feature">
            <h5>⚖️ Decision Logic</h5>
            <ol>
                <li><strong>Default:</strong> Implicit Deny</li>
                <li><strong>Check:</strong> Explicit Allow</li>
                <li><strong>Override:</strong> Permissions Boundaries</li>
                <li><strong>Override:</strong> SCPs (Organizations)</li>
                <li><strong>Override:</strong> Session Policies</li>
                <li><strong>Final Override:</strong> Explicit Deny</li>
            </ol>
            <p><strong>Remember:</strong> Explicit Deny always wins!</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Policy evaluation flowchart
        policy_flow_mermaid = """
        graph TD
            A[Request] --> B{Default: Implicit Deny}
            B --> C{Identity/Resource Policy Allow?}
            C -->|Yes| D{Permissions Boundary Allow?}
            C -->|No| Z[DENY]
            D -->|Yes| E{SCP Allow?}
            D -->|No| Z
            E -->|Yes| F{Session Policy Allow?}
            E -->|No| Z
            F -->|Yes| G{Explicit Deny?}
            F -->|No| Z
            G -->|Yes| Z
            G -->|No| Y[ALLOW]
            
            classDef start fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef decision fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef allow fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef deny fill:#dc3545,stroke:#232F3E,stroke-width:2px,color:#fff
            
            class A start
            class B,C,D,E,F,G decision
            class Y allow
            class Z deny
        """
        common.mermaid(policy_flow_mermaid, height=600, show_controls=True)
    
    # VPC Security Components
    st.markdown("### 🌐 VPC Security Components")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="service-card">
            <h5>🛡️ Security Groups</h5>
            <ul>
                <li><strong>Level:</strong> Instance/ENI level</li>
                <li><strong>Rules:</strong> Allow rules only</li>
                <li><strong>State:</strong> Stateful (return traffic automatic)</li>
                <li><strong>Evaluation:</strong> All rules before decision</li>
                <li><strong>Application:</strong> Must be explicitly assigned</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="service-card">
            <h5>🔒 Network ACLs</h5>
            <ul>
                <li><strong>Level:</strong> Subnet level</li>
                <li><strong>Rules:</strong> Allow and Deny rules</li>
                <li><strong>State:</strong> Stateless (explicit return rules)</li>
                <li><strong>Evaluation:</strong> Lowest number rule first</li>
                <li><strong>Application:</strong> Automatic for subnet resources</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # VPC Security Architecture
    st.markdown("### 🏗️ VPC Security Architecture")
    
    vpc_security_mermaid = """
    graph TB
        subgraph VPC["VPC - Virtual Private Cloud"]
            subgraph AZ1["Availability Zone 1"]
                subgraph PubSub["Public Subnet"]
                    EC2_1[EC2 Instance]
                    SG_A[Security Group A]
                end
                subgraph PrivSub["Private Subnet"]
                    EC2_2[EC2 Instance]
                    SG_B[Security Group B]
                end
            end
            
            NACL_1[Network ACL] --> PubSub
            NACL_2[Network ACL] --> PrivSub
        end
        
        IGW[Internet Gateway] --> PubSub
        VPE[VPC Endpoints] --> PrivSub
        
        classDef vpc fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef subnet fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef security fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef gateway fill:#6f42c1,stroke:#232F3E,stroke-width:2px,color:#fff
        
        class VPC vpc
        class PubSub,PrivSub subnet
        class SG_A,SG_B,NACL_1,NACL_2 security
        class IGW,VPE gateway
    """
    
    common.mermaid(vpc_security_mermaid, height=500, show_controls=False)
    
    # VPC Endpoints
    st.markdown("### 🔗 VPC Endpoints - Private AWS Service Access")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 VPC Endpoints Overview</h4>
            <p>VPC endpoints enable private connectivity between your VPC and supported AWS services 
            without requiring an Internet Gateway, NAT device, VPN connection, or AWS Direct Connect.</p>
            <h5>Key Benefits:</h5>
            <ul>
                <li><strong>Security:</strong> Traffic stays within AWS network</li>
                <li><strong>Performance:</strong> Lower latency connections</li>
                <li><strong>Compliance:</strong> Meet regulatory requirements</li>
                <li><strong>Cost:</strong> Avoid NAT Gateway data processing charges</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### VPC Endpoint Types")
        st.markdown("""
        <div class="service-card">
            <h5>🚪 Gateway Endpoints</h5>
            <ul>
                <li><strong>Services:</strong> S3 and DynamoDB only</li>
                <li><strong>Implementation:</strong> Route table entries</li>
                <li><strong>Cost:</strong> No additional charge</li>
            </ul>
            <h5>🔌 Interface Endpoints</h5>
            <ul>
                <li><strong>Services:</strong> Most AWS services</li>
                <li><strong>Implementation:</strong> ENI with private IP</li>
                <li><strong>Cost:</strong> Hourly and data processing charges</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Task Statement 4.2: Apply authorization mechanisms
    st.markdown("## 🔑 Task Statement 4.2: Apply Authorization Mechanisms")
    
    # Secrets Management
    st.markdown("### 🤐 AWS Secrets Manager vs Systems Manager Parameter Store")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="service-card">
            <h5>🤐 AWS Secrets Manager</h5>
            <ul>
                <li><strong>Purpose:</strong> Database credentials, API keys, passwords</li>
                <li><strong>Rotation:</strong> Automatic rotation with Lambda</li>
                <li><strong>Encryption:</strong> Always encrypted at rest</li>
                <li><strong>Pricing:</strong> Per secret per month + API calls</li>
                <li><strong>Integration:</strong> RDS, Redshift, DocumentDB</li>
                <li><strong>Cross-region:</strong> Automatic replication</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="service-card">
            <h5>⚙️ Systems Manager Parameter Store</h5>
            <ul>
                <li><strong>Purpose:</strong> Configuration data, app settings</li>
                <li><strong>Rotation:</strong> Manual or custom automation</li>
                <li><strong>Encryption:</strong> Optional with KMS</li>
                <li><strong>Pricing:</strong> Free tier available</li>
                <li><strong>Integration:</strong> EC2, Lambda, CloudFormation</li>
                <li><strong>Hierarchy:</strong> Hierarchical parameter organization</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # IAM Roles Deep Dive
    st.markdown("### 🎭 IAM Roles - The Power of Temporary Credentials")
    
    st.markdown("""
    <div class="program-card">
        <h4>🎯 Why Use IAM Roles?</h4>
        <p>Roles are a secure way to grant permissions without embedding long-term credentials 
        in your applications. They provide temporary credentials that automatically rotate.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Role Types
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="security-feature">
            <h5>🤖 Service Roles</h5>
            <ul>
                <li>For AWS services</li>
                <li>Lambda execution role</li>
                <li>EC2 instance role</li>
                <li>Redshift service role</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="security-feature">
            <h5>🔄 Cross-Account Roles</h5>
            <ul>
                <li>Access across AWS accounts</li>
                <li>Third-party access</li>
                <li>Service-to-service access</li>
                <li>Federated access</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="security-feature">
            <h5>👤 Identity Provider Roles</h5>
            <ul>
                <li>SAML 2.0 federation</li>
                <li>Web identity federation</li>
                <li>OpenID Connect</li>
                <li>Active Directory</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # AWS Lake Formation
    st.markdown("### 🏞️ AWS Lake Formation - Unified Data Lake Security")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 Lake Formation Security Model</h4>
            <p>AWS Lake Formation provides a unified approach to securing your data lake, combining 
            traditional IAM permissions with fine-grained, table and column-level access controls.</p>
            <h5>Key Features:</h5>
            <ul>
                <li><strong>Centralized permissions:</strong> Single pane of glass</li>
                <li><strong>Fine-grained access:</strong> Table and column level</li>
                <li><strong>Cross-service:</strong> Works with Athena, EMR, Redshift</li>
                <li><strong>Data filtering:</strong> Row and cell-level security</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Lake Formation Architecture")
        lf_mermaid = """
        graph TD
            A[Lake Formation] --> B[Data Catalog]
            A --> C[S3 Locations]
            A --> D[IAM Integration]
            
            B --> E[Table Permissions]
            C --> F[Data Location Permissions]
            D --> G[IAM Policies]
            
            E --> H[Athena]
            E --> I[EMR]
            E --> J[Redshift]
            
            classDef lf fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef perm fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef service fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
            
            class A lf
            class B,C,D,E,F,G perm
            class H,I,J service
        """
        common.mermaid(lf_mermaid, height=400, show_controls=False)
    
    # Lake Formation Permissions
    st.markdown("### 🔐 Lake Formation Permission Types")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="governance-card">
            <h5>📊 Metadata Access Permissions</h5>
            <ul>
                <li><strong>Scope:</strong> Data Catalog resources</li>
                <li><strong>Actions:</strong> CREATE, READ, UPDATE, DELETE</li>
                <li><strong>Targets:</strong> Databases and tables</li>
                <li><strong>Use case:</strong> Control catalog operations</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="governance-card">
            <h5>💾 Underlying Data Access</h5>
            <ul>
                <li><strong>Data lake permissions:</strong> READ/WRITE to S3 data</li>
                <li><strong>Data location permissions:</strong> CREATE/ALTER tables</li>
                <li><strong>Granularity:</strong> Column-level filtering</li>
                <li><strong>Use case:</strong> Control actual data access</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Task Statement 4.3: Ensure data encryption and masking
    st.markdown("## 🔒 Task Statement 4.3: Ensure Data Encryption and Masking")
    
    # AWS KMS Deep Dive
    st.markdown("### 🔐 AWS Key Management Service (KMS)")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 AWS KMS Overview</h4>
            <p>AWS KMS is a managed service that makes it easy for you to create and control 
            cryptographic keys across your applications and AWS services. All requests to use 
            keys are logged in CloudTrail for audit compliance.</p>
            <h5>Key Features:</h5>
            <ul>
                <li><strong>Centralized key management:</strong> Single control point</li>
                <li><strong>Integrated:</strong> Works with 100+ AWS services</li>
                <li><strong>Audit logging:</strong> All key usage logged</li>
                <li><strong>Compliance:</strong> FIPS 140-2 Level 2 validated</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### KMS Key Types")
        st.markdown("""
        <div class="encryption-card">
            <h5>🔑 Key Types</h5>
            <ul>
                <li><strong>AWS Managed:</strong> Created/managed by AWS</li>
                <li><strong>Customer Managed:</strong> Full customer control</li>
                <li><strong>AWS Owned:</strong> Used by AWS internally</li>
            </ul>
            <h5>💡 Best Practice</h5>
            <p>Use customer-managed keys for maximum control and flexibility</p>
        </div>
        """, unsafe_allow_html=True)
    
    # KMS Key Hierarchy
    kms_mermaid = """
    graph TD
        A[AWS KMS] --> B[Customer Managed Keys]
        A --> C[AWS Managed Keys]
        A --> D[AWS Owned Keys]
        
        B --> E[Key Policies]
        B --> F[Key Rotation]
        B --> G[Cross-Account Access]
        
        C --> H[Service Integration]
        C --> I[Automatic Rotation]
        
        D --> J[Internal AWS Use]
        
        classDef kms fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef customer fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef aws fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef owned fill:#6c757d,stroke:#232F3E,stroke-width:2px,color:#fff
        
        class A kms
        class B,E,F,G customer
        class C,H,I aws
        class D,J owned
    """
    
    common.mermaid(kms_mermaid, height=500, show_controls=False)
    
    # Service-Specific Encryption
    st.markdown("### 🛡️ Encryption Across AWS Analytics Services")
    
    # Redshift Encryption
    st.markdown("#### 🔴 Amazon Redshift Encryption")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="encryption-card">
            <h5>💾 Encryption at Rest</h5>
            <ul>
                <li><strong>Scope:</strong> Data blocks and system metadata</li>
                <li><strong>When:</strong> Cluster launch or modify existing</li>
                <li><strong>Keys:</strong> AWS-managed or customer-managed KMS keys</li>
                <li><strong>Migration:</strong> Automatic for encryption enable</li>
                <li><strong>RA3 nodes:</strong> Faster encryption process</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="encryption-card">
            <h5>🔄 Encryption in Transit</h5>
            <ul>
                <li><strong>SSL/TLS:</strong> Force SSL connections</li>
                <li><strong>Parameter groups:</strong> require_ssl = true</li>
                <li><strong>JDBC/ODBC:</strong> SSL parameter in connection string</li>
                <li><strong>Load operations:</strong> Encrypted COPY from S3</li>
                <li><strong>Backup:</strong> Encrypted snapshots</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # EMR Encryption
    st.markdown("#### 🐘 Amazon EMR Encryption")
    
    st.markdown("""
    <div class="program-card">
        <h4>🔐 EMR Security Configuration</h4>
        <p>EMR supports encryption at rest, in transit, and for local storage through security 
        configurations that are reusable across clusters.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="encryption-card">
            <h5>💾 At Rest</h5>
            <ul>
                <li><strong>S3:</strong> SSE-S3, SSE-KMS, CSE-KMS</li>
                <li><strong>EBS:</strong> EBS encryption with KMS</li>
                <li><strong>Local disks:</strong> LUKS encryption</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="encryption-card">
            <h5>🔄 In Transit</h5>
            <ul>
                <li><strong>TLS:</strong> Inter-node communication</li>
                <li><strong>HTTPS:</strong> Web interfaces</li>
                <li><strong>Certificate-based:</strong> Node authentication</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="encryption-card">
            <h5>⚙️ Configuration</h5>
            <ul>
                <li><strong>Security config:</strong> Reusable templates</li>
                <li><strong>KMS integration:</strong> Customer-managed keys</li>
                <li><strong>Certificate management:</strong> Automatic provisioning</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Glue Data Catalog Encryption
    st.markdown("#### 🕷️ AWS Glue Data Catalog Encryption")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="encryption-card">
            <h5>📋 Catalog Metadata Encryption</h5>
            <ul>
                <li><strong>Scope:</strong> All new catalog objects</li>
                <li><strong>Objects:</strong> Databases, tables, partitions, UDFs</li>
                <li><strong>Keys:</strong> AWS-managed or customer-managed</li>
                <li><strong>Control:</strong> Console, CLI, or API</li>
                <li><strong>Granular:</strong> Per-catalog encryption settings</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="encryption-card">
            <h5>🔧 Security Configurations</h5>
            <ul>
                <li><strong>Job encryption:</strong> SSE-S3 or SSE-KMS parameters</li>
                <li><strong>Development endpoints:</strong> Encrypted notebooks</li>
                <li><strong>CloudWatch logs:</strong> Encrypted job logs</li>
                <li><strong>Bookmarks:</strong> Encrypted job bookmarks</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Task Statement 4.4: Prepare logs for audit
    st.markdown("## 📊 Task Statement 4.4: Prepare Logs for Audit")
    
    # AWS CloudTrail
    st.markdown("### 🔍 AWS CloudTrail - Complete API Audit Trail")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 CloudTrail Overview</h4>
            <p>AWS CloudTrail logs, monitors, and retains account activity related to actions 
            across your AWS infrastructure. It provides the "who, what, when, and where" for AWS API calls.</p>
            <h5>Key Use Cases:</h5>
            <ul>
                <li><strong>Audit activity:</strong> Track all AWS API calls</li>
                <li><strong>Security incidents:</strong> Detect unauthorized access</li>
                <li><strong>Compliance:</strong> Meet regulatory requirements</li>
                <li><strong>Operational troubleshooting:</strong> Debug configuration changes</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### CloudTrail Data Flow")
        cloudtrail_mermaid = """
        graph TD
            A[AWS API Calls] --> B[CloudTrail]
            B --> C[Event History]
            B --> D[S3 Bucket]
            B --> E[CloudWatch Logs]
            D --> F[Athena Queries]
            E --> G[CloudWatch Insights]
            
            classDef api fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef trail fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef output fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
            
            class A api
            class B trail
            class C,D,E,F,G output
        """
        common.mermaid(cloudtrail_mermaid, height=400, show_controls=False)
    
    # CloudTrail Lake
    st.markdown("### 🏞️ AWS CloudTrail Lake - Advanced Log Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="audit-card">
            <h5>🎯 CloudTrail Lake Features</h5>
            <ul>
                <li><strong>SQL queries:</strong> Run SQL on event data</li>
                <li><strong>Multiple sources:</strong> CloudTrail, Config, Audit Manager</li>
                <li><strong>Event data stores:</strong> Immutable collections</li>
                <li><strong>Apache ORC format:</strong> Optimized storage</li>
                <li><strong>Federation:</strong> Glue Data Catalog integration</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="audit-card">
            <h5>📊 Analysis Capabilities</h5>
            <ul>
                <li><strong>Dashboards:</strong> Pre-built visualizations</li>
                <li><strong>Cross-account:</strong> Organization-wide events</li>
                <li><strong>Advanced selectors:</strong> Fine-grained filtering</li>
                <li><strong>Retention:</strong> Configurable data retention</li>
                <li><strong>Encryption:</strong> KMS encryption support</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Amazon CloudWatch
    st.markdown("### 📈 Amazon CloudWatch - Comprehensive Monitoring")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 CloudWatch Logs</h4>
            <p>Centralize logs from all systems, applications, and AWS services in a single, 
            highly scalable service with powerful query capabilities.</p>
            <h5>Key Features:</h5>
            <ul>
                <li><strong>Centralized logging:</strong> Single log destination</li>
                <li><strong>Log Insights:</strong> Interactive search and analysis</li>
                <li><strong>Live Tail:</strong> Real-time log streaming</li>
                <li><strong>Metric filters:</strong> Generate metrics from logs</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="program-card">
            <h4>📊 CloudWatch Metrics & Alarms</h4>
            <p>Monitor resource performance and operational health with automated responses 
            to threshold breaches.</p>
            <h5>Capabilities:</h5>
            <ul>
                <li><strong>Custom metrics:</strong> Application-specific metrics</li>
                <li><strong>Composite alarms:</strong> Multiple condition monitoring</li>
                <li><strong>Dashboards:</strong> Visual operational overview</li>
                <li><strong>Anomaly detection:</strong> ML-powered alerting</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Log Analysis Options
    st.markdown("### 🔍 Log Analysis Service Comparison")
    
    log_comparison = """
    <div class="comparison-table">
        <table style="width:100%; border-collapse: collapse;">
            <thead style="background-color: #232F3E; color: white;">
                <tr>
                    <th style="padding: 12px; text-align: left;">Service</th>
                    <th style="padding: 12px; text-align: left;">Best For</th>
                    <th style="padding: 12px; text-align: left;">Query Language</th>
                    <th style="padding: 12px; text-align: left;">Data Source</th>
                </tr>
            </thead>
            <tbody>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;"><strong>CloudWatch Logs Insights</strong></td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Real-time log analysis</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">CloudWatch query language</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">CloudWatch Log Groups</td>
                </tr>
                <tr>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;"><strong>Amazon Athena</strong></td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Ad-hoc analysis of large datasets</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Standard SQL</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">S3, CloudTrail logs</td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;"><strong>CloudTrail Lake</strong></td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">Audit and compliance queries</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">SQL</td>
                    <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">CloudTrail events</td>
                </tr>
                <tr>
                    <td style="padding: 12px;"><strong>Amazon EMR</strong></td>
                    <td style="padding: 12px;">Large-scale log processing</td>
                    <td style="padding: 12px;">Spark SQL, Hadoop</td>
                    <td style="padding: 12px;">Large log datasets</td>
                </tr>
            </tbody>
        </table>
    </div>
    """
    st.markdown(log_comparison, unsafe_allow_html=True)
    
    # Task Statement 4.5: Understand data privacy and governance
    st.markdown("## 🏛️ Task Statement 4.5: Understand Data Privacy and Governance")
    
    # AWS Config
    st.markdown("### ⚙️ AWS Config - Configuration Compliance")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 AWS Config Overview</h4>
            <p>AWS Config continually assesses, audits, and evaluates the configurations and 
            relationships of your resources across AWS, on-premises, and other clouds.</p>
            <h5>Key Capabilities:</h5>
            <ul>
                <li><strong>Configuration history:</strong> Track resource configuration changes</li>
                <li><strong>Compliance monitoring:</strong> Evaluate against rules</li>
                <li><strong>Relationship tracking:</strong> Map resource dependencies</li>
                <li><strong>Remediation:</strong> Automated compliance fixes</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Config Use Cases")
        st.markdown("""
        <div class="governance-card">
            <h5>🛡️ Security & Compliance</h5>
            <ul>
                <li>Security group rule changes</li>
                <li>S3 bucket policy violations</li>
                <li>Encryption compliance</li>
                <li>Network access control</li>
            </ul>
            <h5>🔧 Operational</h5>
            <ul>
                <li>Change impact analysis</li>
                <li>Troubleshooting</li>
                <li>Resource inventory</li>
                <li>Cost optimization</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Amazon Macie
    st.markdown("### 🔍 Amazon Macie - Intelligent Data Discovery")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 Amazon Macie Overview</h4>
            <p>Amazon Macie is a data security service that uses machine learning and pattern 
            matching to discover sensitive data, provides visibility into data security risks, 
            and enables automated protection.</p>
            <h5>Key Features:</h5>
            <ul>
                <li><strong>ML-powered discovery:</strong> Automatically detect sensitive data</li>
                <li><strong>PII identification:</strong> Names, addresses, credit cards, SSNs</li>
                <li><strong>Data classification:</strong> Automatic tagging and categorization</li>
                <li><strong>Risk assessment:</strong> S3 bucket security evaluation</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Macie Benefits")
        st.markdown("""
        <div class="security-feature">
            <h5>🤖 Machine Learning</h5>
            <ul>
                <li>Pattern recognition</li>
                <li>Large-scale detection</li>
                <li>Cost-efficient scanning</li>
                <li>Continuous monitoring</li>
            </ul>
            <h5>📊 Visibility</h5>
            <ul>
                <li>Data security dashboards</li>
                <li>Bucket-level insights</li>
                <li>Risk scores</li>
                <li>Finding remediation</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Data Sharing
    st.markdown("### 🤝 Amazon Redshift Data Sharing")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 Secure Data Sharing</h4>
            <p>Amazon Redshift data sharing enables secure access to live data across clusters, 
            workgroups, AWS accounts, and AWS Regions without copying data.</p>
            <h5>Use Cases:</h5>
            <ul>
                <li><strong>Workload isolation:</strong> Separate ETL and BI clusters</li>
                <li><strong>Cross-group collaboration:</strong> Team data sharing</li>
                <li><strong>Data as a service:</strong> Internal data products</li>
                <li><strong>Environment sharing:</strong> Dev/test/prod data access</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="program-card">
            <h4>🔒 Data Sharing Security</h4>
            <ul>
                <li><strong>Live data:</strong> Always current, no copying</li>
                <li><strong>Cross-account:</strong> Secure sharing between accounts</li>
                <li><strong>Cross-region:</strong> Global data access</li>
                <li><strong>Granular controls:</strong> Table and schema-level permissions</li>
                <li><strong>AWS Data Exchange:</strong> Monetize data assets</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Data Sharing Architecture
    data_sharing_mermaid = """
    graph TB
        subgraph Producer["Producer Cluster"]
            A[Data Owner]
            B[Create Share]
            C[Grant Access]
        end
        
        subgraph Consumer1["Consumer Cluster 1"]
            D[Business Intelligence]
            E[Dashboards]
        end
        
        subgraph Consumer2["Consumer Cluster 2"]
            F[Data Science]
            G[ML Models]
        end
        
        A --> B
        B --> C
        C --> D
        C --> F
        
        classDef producer fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef consumer fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
        
        class A,B,C producer
        class D,E,F,G consumer
    """
    
    common.mermaid(data_sharing_mermaid, height=500, show_controls=False)
    
    # Cross-Account Resource Sharing
    st.markdown("### 🌐 Cross-Account Resource Sharing Best Practices")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="governance-card">
            <h5>🔑 IAM Cross-Account Roles</h5>
            <ul>
                <li><strong>Trust policy:</strong> Define which accounts can assume</li>
                <li><strong>Permission policy:</strong> What actions are allowed</li>
                <li><strong>External ID:</strong> Additional security layer</li>
                <li><strong>Condition keys:</strong> Restrict access patterns</li>
                <li><strong>Temporary credentials:</strong> Time-limited access</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="governance-card">
            <h5>🔒 KMS Cross-Account Key Usage</h5>
            <ul>
                <li><strong>Key policy:</strong> Allow external account access</li>
                <li><strong>Grant tokens:</strong> Temporary key usage rights</li>
                <li><strong>ViaService condition:</strong> Service-specific access</li>
                <li><strong>Encryption context:</strong> Additional security layer</li>
                <li><strong>CloudTrail logging:</strong> Cross-account usage tracking</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Weekly Digital Training Curriculum
    st.markdown("## 📚 Final Week Digital Training Curriculum")
    st.markdown("**Complete your certification preparation with these final courses:**")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.markdown("### AWS Skill Builder Learning Plan Courses")
        
        required_courses = [
            "Amazon Simple Storage Service (Amazon S3) Troubleshooting",
            "Managing Amazon Simple Storage Service (Amazon S3)",
            "AWS Networking Basics",
            "Subnets, Gateways, and Route Tables Explained",
            "Understanding AWS Networking Gateways",
            "Official Practice Question Set: AWS Certified Data Engineer – Associate"
        ]
        
        for course in required_courses:
            st.markdown(f"""
            <div class="training-item">
                <div class="training-icon">📖</div>
                <div>
                    <strong>{course}</strong><br>
                    <small>Required Course</small>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Exam Preparation (Final Steps)")
        
        exam_prep = [
            "Complete – Official Pretest: AWS Certified Data Engineer – Associate",
            "Complete – Domain Practice Flashcards",
            "Consider – AWS Cloud Quest: Data Analytics Role"
        ]
        
        for item in exam_prep:
            st.markdown(f"""
            <div class="training-item">
                <div class="training-icon">🎯</div>
                <div>
                    <strong>{item}</strong><br>
                    <small>Exam Readiness</small>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    # AWS Skill Builder Subscription
    st.markdown("## 🎓 AWS Skill Builder Subscription - Final Recommendation")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🆓 Free Digital Training</h4>
            <ul>
                <li>600+ digital courses</li>
                <li>Learning plans</li>
                <li>10 Practice Question Sets</li>
                <li>AWS Cloud Quest (Foundational)</li>
                <li>AWS SimuLearn (Cloud Practitioner)</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="program-card">
            <h4>💰 Individual Subscription - Highly Recommended!</h4>
            <ul>
                <li>Everything in free, plus:</li>
                <li>AWS SimuLearn (200+ trainings)</li>
                <li>AWS Cloud Quest (Intermediate – Advanced)</li>
                <li><strong>Official Practice Exams</strong> 🎯</li>
                <li>Unlimited access to 1000+ hands-on labs</li>
                <li>AWS Jam Journeys (lab-based challenges)</li>
            </ul>
            <p><strong>$29 USD/month or $449 USD/year</strong></p>
        </div>
        """, unsafe_allow_html=True)
    
    # Exam Readiness Checklist
    st.markdown("## ✅ Certification Exam Readiness Checklist")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>📋 Knowledge Areas Mastered</h4>
            <ul>
                <li>✅ <strong>Domain 1:</strong> Data Ingestion and Transformation (34%)</li>
                <li>✅ <strong>Domain 2:</strong> Data Store Management (26%)</li>
                <li>✅ <strong>Domain 3:</strong> Data Operations and Support (22%)</li>
                <li>✅ <strong>Domain 4:</strong> Data Security and Governance (18%)</li>
            </ul>
            <h5>🎯 Total Coverage: 100%</h5>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="program-card">
            <h4>🚀 Next Steps to Certification</h4>
            <ol>
                <li>Complete all digital training assignments</li>
                <li>Take official practice exams</li>
                <li>Review weak areas with additional labs</li>
                <li>Schedule your certification exam</li>
                <li>Get certified! 🏆</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)
    
    # Key Resources
    st.markdown("## 📚 Key Resources and Documentation")
    
    with st.expander("🔗 Essential Documentation Links"):
        st.markdown("""
        **Security and Access Management:**
        - [IAM Policy Evaluation Logic](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic.html)
        - [Cross-Account IAM Roles Tutorial](https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html)
        - [AWS VPC Endpoints](https://docs.aws.amazon.com/whitepapers/latest/aws-privatelink/what-are-vpc-endpoints.html)
        
        **Data Lake Security:**
        - [Lake Formation Security Model](https://docs.aws.amazon.com/lake-formation/latest/dg/what-is-lake-formation.html)
        - [Lake Formation Permissions](https://docs.aws.amazon.com/lake-formation/latest/dg/lake-formation-permissions.html)
        
        **Encryption:**
        - [S3 Server-Side Encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingDSSEncryption.html)
        - [Glue Data Catalog Encryption](https://docs.aws.amazon.com/glue/latest/dg/encrypt-glue-data-catalog.html)
        - [Redshift Encryption](https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-db-encryption.html)
        - [EMR Encryption](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-data-encryption.html)
        
        **Auditing and Compliance:**
        - [CloudTrail Lake Concepts](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-lake-concepts.html)
        - [CloudWatch Logs User Guide](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/)
        - [AWS Config](https://docs.aws.amazon.com/config/latest/developerguide/WhatIsConfig.html)
        
        **Data Governance:**
        - [Amazon Macie User Guide](https://docs.aws.amazon.com/macie/latest/user/)
        - [Redshift Data Sharing](https://docs.aws.amazon.com/redshift/latest/dg/datashare-overview.html)
        - [KMS Cross-Account Access](https://docs.aws.amazon.com/kms/latest/developerguide/key-policy-modifying-external-accounts.html)
        """)
    
    # Final Encouragement
    st.markdown("## 🎉 Congratulations - You're Certification Ready!")
    
    st.markdown("""
    <div class="program-card">
        <h4>🏆 Amazing Achievement!</h4>
        <p>You've successfully completed all 5 content review sessions of the AWS Partner 
        Certification Readiness program for Data Engineer Associate! You now have comprehensive 
        knowledge across all exam domains:</p>
        <ul>
            <li>✅ <strong>Data fundamentals</strong> and core AWS services</li>
            <li>✅ <strong>Data ingestion and transformation</strong> techniques and tools</li>
            <li>✅ <strong>Data store management</strong> for warehouses and lakes</li>
            <li>✅ <strong>Data operations and support</strong> including monitoring and quality</li>
            <li>✅ <strong>Data security and governance</strong> best practices</li>
        </ul>
        <p><strong>You're now ready to take and pass the AWS Certified Data Engineer - Associate exam!</strong></p>
        <p>Remember: This certification validates your skills in implementing data pipelines, 
        monitoring data solutions, and ensuring data quality - essential capabilities for any 
        data engineering role in the cloud.</p>
        <p><strong>Best of luck on your certification journey! 🌟</strong></p>
    </div>
    """, unsafe_allow_html=True)
    
    # Footer
    st.markdown("""
    <div class="footer">
        <p>© 2025, Amazon Web Services, Inc. or its affiliates. All rights reserved.</p>
        <p><strong>Thank you for attending all sessions! You're now exam-ready! 🎯</strong></p>
        <p>Questions? Contact the AWS Partner Certification Readiness (APCR) team</p>
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
