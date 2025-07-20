import streamlit as st
import plotly.graph_objects as go
import utils.common as common
import utils.authenticate as authenticate

def main():
    # Page configuration
    st.set_page_config(
        page_title="Data Engineer Associate - Session 3",
        page_icon="🗄️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
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
    
    .storage-card {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 1.5rem;
        border-radius: 8px;
        border-left: 4px solid #4B9CD3;
        margin-bottom: 1rem;
    }
    
    .redshift-feature {
        background: #fff3cd;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #ffc107;
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
        <h3>Content Review Session 3: Domain 2 - Data Store Management</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Program Check-in section
    st.markdown("## 📋 Program Check-in")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>Welcome to Session 3!</h4>
            <p>We're deep into your certification journey! This session focuses on Data Store Management - 
            understanding how to choose, configure, and manage data storage solutions in AWS. 
            You'll learn about data warehouses, data lakes, catalogs, and lifecycle management.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Progress indicator
        progress_value = 60  # 3rd session out of 5
        st.metric("Program Progress", f"{progress_value}%", "Session 3 of 5")
        st.progress(progress_value / 100)
    
    # Program Roadmap
    st.markdown("## 🗺️ Program Roadmap")
    
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
            "status": "current",
            "description": "🎯 Data warehouses, data lakes, catalogs, and lifecycle management"
        },
        {
            "session": "Content Review 4",
            "topic": "Domain 3: Data Operations and Support",
            "status": "upcoming", 
            "description": "📅 Monitoring, troubleshooting, and operational best practices"
        },
        {
            "session": "Content Review 5",
            "topic": "Domain 4: Data Security and Governance",
            "status": "upcoming",
            "description": "📅 Security controls, compliance, and data governance frameworks"
        }
    ]
    
    # Your Learning Journey - Mermaid Diagram
    st.markdown("### Your Learning Journey")
    
    mermaid_code = """
    graph LR
        A[✅ Session 1<br/>AWS & Data<br/>Fundamentals] --> B[✅ Session 2<br/>Data Ingestion &<br/>Transformation]
        B --> C[🎯 Session 3<br/>Data Store<br/>Management]
        C --> D[📅 Session 4<br/>Data Operations<br/>& Support]
        D --> E[📅 Session 5<br/>Data Security &<br/>Governance]
        E --> F[🏆 Certification<br/>Exam Ready]
        
        classDef completed fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef current fill:#FF9900,stroke:#232F3E,stroke-width:3px,color:#fff
        classDef upcoming fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef target fill:#6f42c1,stroke:#232F3E,stroke-width:2px,color:#fff
        
        class A,B completed
        class C current
        class D,E upcoming
        class F target
    """
    
    common.mermaid(mermaid_code, height=150, show_controls=False)
    
    # Roadmap details
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
    st.markdown("**During this session, we will cover:**")
    
    learning_outcomes = [
        "Task Statement 2.1: Choosing a data store",
        "Task Statement 2.2: Understanding data cataloging systems", 
        "Task Statement 2.3: Managing the lifecycle of data",
        "Task Statement 2.4: Designing data models and schema evolution"
    ]
    
    for i, outcome in enumerate(learning_outcomes):
        st.markdown(f"""
        <div class="learning-outcome">
            <strong>✅ {outcome}</strong>
        </div>
        """, unsafe_allow_html=True)
    
    # Key AWS Storage Services Overview
    st.markdown("## 🗄️ AWS Storage Services Overview")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="storage-card">
            <h4>📦 Object Storage</h4>
            <strong>Amazon S3</strong><br>
            <strong>S3 Glacier</strong><br><br>
            <p>Stores and manages data as objects. Examples include documents, images, or data values.</p>
            <ul>
                <li>Infinitely scalable</li>
                <li>Multiple storage classes</li>
                <li>Lifecycle policies</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="storage-card">
            <h4>🧱 Block Storage</h4>
            <strong>Amazon EBS</strong><br><br>
            <p>Divides data into fixed-sized blocks. Works with EC2 instances for high-performance workloads.</p>
            <ul>
                <li>High IOPS performance</li>
                <li>Snapshot capabilities</li>
                <li>Multiple volume types</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="storage-card">
            <h4>📁 File Storage</h4>
            <strong>Amazon EFS</strong><br>
            <strong>Amazon FSx</strong><br><br>
            <p>Hierarchical storage system providing shared access to file data with metadata.</p>
            <ul>
                <li>Shared file systems</li>
                <li>POSIX compliance</li>
                <li>Auto-scaling</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Amazon Redshift Deep Dive
    st.markdown("## 🏬 Amazon Redshift - Data Warehouse Service")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 What is Amazon Redshift?</h4>
            <p>Amazon Redshift is a fully managed, petabyte-scale data warehouse service that provides 
            the best price-performance at any scale. It analyzes large volumes of data across data warehouses, 
            operational databases, and data lakes.</p>
            <h5>Key Benefits:</h5>
            <ul>
                <li><strong>Efficient storage</strong> - Columnar storage and compression</li>
                <li><strong>High performance</strong> - Query processing optimization</li>
                <li><strong>Agile scaling</strong> - Scale compute and storage independently</li>
                <li><strong>Price-performance</strong> - Best value at any scale</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Redshift cluster architecture diagram
        st.markdown("### Cluster Architecture")
        redshift_mermaid = """
        graph TD
            A[Leader Node] --> B[Compute Node 1]
            A --> C[Compute Node 2]
            A --> D[Compute Node N]
            B --> E[Slice 1]
            B --> F[Slice 2]
            C --> G[Slice 1]
            C --> H[Slice 2]
            D --> I[Slice 1]
            D --> J[Slice 2]
            
            classDef leader fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef compute fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
            classDef slice fill:#28a745,stroke:#232F3E,stroke-width:1px,color:#fff
            
            class A leader
            class B,C,D compute
            class E,F,G,H,I,J slice
        """
        common.mermaid(redshift_mermaid, height=300, show_controls=False)
    
    # Redshift Features
    st.markdown("### 🔧 Key Redshift Features")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="redshift-feature">
            <h5>🌐 Redshift Spectrum</h5>
            <p>Query structured and semi-structured data directly from S3 without loading into Redshift tables. 
            Massive parallelism for fast queries against large datasets.</p>
        </div>
        
        <div class="redshift-feature">
            <h5>🔗 Federated Queries</h5>
            <p>Query and analyze data across operational databases, data warehouses, and data lakes. 
            Works with RDS PostgreSQL, Aurora PostgreSQL, RDS MySQL, and Aurora MySQL.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="redshift-feature">
            <h5>🗂️ SUPER Data Type</h5>
            <p>Store and query semi-structured data like JSON directly in Redshift. 
            Use PartiQL for seamless querying of both structured and semi-structured data.</p>
        </div>
        
        <div class="redshift-feature">
            <h5>📊 Columnar Storage</h5>
            <p>Optimized for analytical queries with columnar data storage, compression, 
            and zone maps for efficient I/O operations.</p>
        </div>
        """, unsafe_allow_html=True)
    
    # AWS Glue and Data Catalog
    st.markdown("## 🕷️ AWS Glue - Data Integration Service")
    
    st.markdown("""
    <div class="program-card">
        <h4>What is AWS Glue?</h4>
        <p>AWS Glue is a serverless data integration service that makes it easier to discover, prepare, 
        and combine data for analytics, machine learning, and application development.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="storage-card">
            <h5>📚 Data Catalog</h5>
            <ul>
                <li>Central metadata repository</li>
                <li>Apache Hive Metastore compatible</li>
                <li>Integration with Athena, EMR, Redshift Spectrum</li>
                <li>Automatic schema discovery</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="storage-card">
            <h5>🕸️ Crawlers</h5>
            <ul>
                <li>Automatic schema detection</li>
                <li>Scheduled crawling</li>
                <li>Partition discovery</li>
                <li>Schema evolution tracking</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="storage-card">
            <h5>📋 Schema Registry</h5>
            <ul>
                <li>Avro and JSON Schema support</li>
                <li>Schema validation</li>
                <li>Compatibility checks</li>
                <li>Streaming integration</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Data Lifecycle Management
    st.markdown("## ♻️ Data Lifecycle Management")
    
    st.markdown("""
    <div class="program-card">
        <h4>S3 Lifecycle Policies</h4>
        <p>Define rules to transition objects between storage classes and expire objects to optimize costs:</p>
    </div>
    """, unsafe_allow_html=True)
    
    # S3 Storage Classes Flow
    lifecycle_mermaid = """
    graph LR
        A[S3 Standard] --> B[S3 Standard-IA]
        B --> C[S3 Glacier Flexible]
        C --> D[S3 Glacier Deep Archive]
        D --> E[Expire/Delete]
        
        A --> F[S3 Intelligent Tiering]
        F --> G[Auto-optimization]
        
        classDef standard fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef ia fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef glacier fill:#17a2b8,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef deep fill:#6c757d,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef expire fill:#dc3545,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef intelligent fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
        
        class A standard
        class B ia
        class C glacier
        class D deep
        class E expire
        class F,G intelligent
    """
    
    common.mermaid(lifecycle_mermaid, height=200, show_controls=False)
    
    # Schema Design and Migration
    st.markdown("## 🏗️ Schema Design and Migration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>📐 Redshift Schema Design Best Practices</h4>
            <ul>
                <li><strong>Distribution Keys:</strong> EVEN, KEY, ALL, AUTO</li>
                <li><strong>Sort Keys:</strong> Optimize query performance</li>
                <li><strong>Compression:</strong> ENCODE AUTO for best results</li>
                <li><strong>Column Sizing:</strong> Use smallest appropriate sizes</li>
                <li><strong>Data Types:</strong> Use appropriate types (DATE vs VARCHAR)</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="program-card">
            <h4>🔄 AWS Database Migration Service (DMS)</h4>
            <ul>
                <li><strong>Schema Conversion:</strong> DMS Schema Conversion & AWS SCT</li>
                <li><strong>Migration Types:</strong> Full load + CDC</li>
                <li><strong>Supported Sources:</strong> Oracle, SQL Server, MySQL, PostgreSQL</li>
                <li><strong>Minimal Downtime:</strong> Continuous replication</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Weekly Digital Training Curriculum
    st.markdown("## 📚 Weekly Digital Training Curriculum")
    st.markdown("**Get started early on next week's digital training assignments!**")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.markdown("### AWS Skill Builder Learning Plan Courses")
        
        required_courses = [
            "Introduction to Amazon Athena",
            "Amazon QuickSight – Getting Started", 
            "AWS Database Offerings",
            "Serverless Analytics",
            "Amazon Redshift - Best Practices for Data Warehousing in AWS"
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
        st.markdown("### Companion Learning Plan (Optional)")
        
        optional_labs = [
            "Complete Lab - Using Data Encryption in AWS",
            "Complete Lab – Introduction to AWS Database Migration Service"
        ]
        
        for lab in optional_labs:
            st.markdown(f"""
            <div class="training-item">
                <div class="training-icon">🧪</div>
                <div>
                    <strong>{lab}</strong><br>
                    <small>Hands-on Practice</small>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    # Action Items and Next Steps
    st.markdown("## 🚀 Action Items & Next Steps")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>📝 This Week's Focus</h4>
            <ul>
                <li>Deep dive into Redshift architecture and features</li>
                <li>Practice with AWS Glue crawlers and Data Catalog</li>
                <li>Understand S3 lifecycle policies</li>
                <li>Learn schema design best practices</li>
                <li>Explore data migration strategies</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 Key Study Areas</h4>
            <ul>
                <li>Redshift cluster architecture (RA3 vs DC2)</li>
                <li>Columnar storage vs row-based storage</li>
                <li>Parquet and ORC file formats</li>
                <li>AWS DMS and Schema Conversion Tool</li>
                <li>Data versioning and TTL concepts</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Helpful Resources
    with st.expander("📚 Additional Resources"):
        st.markdown("""
        **Documentation Links:**
        - [Amazon S3 Storage Classes](https://aws.amazon.com/s3/storage-classes/)
        - [Redshift Architecture Guide](https://docs.aws.amazon.com/redshift/latest/dg/c_high_level_system_architecture.html)
        - [AWS Glue Data Catalog and Crawlers](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
        - [S3 Lifecycle Configuration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/how-to-set-lifecycle-configuration-intro.html)
        - [Redshift Best Practices for Table Design](https://docs.aws.amazon.com/redshift/latest/dg/c_designing-tables-best-practices.html)
        - [AWS DMS User Guide](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Introduction.html)
        """)
    
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
