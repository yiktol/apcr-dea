
import streamlit as st
import plotly.graph_objects as go
import utils.common as common
import utils.authenticate as authenticate

def main():
    # Page configuration
    st.set_page_config(
        page_title="Data Engineer Associate - Domain 1",
        page_icon="🔄",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    common.initialize_session_state()
    
    with st.sidebar:
        common.render_sidebar()
    
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
        padding: 1.5rem;
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
        border-radius: 10px;
        margin: 1rem 0;
        border-left: 5px solid #FF9900;
        transition: all 0.3s ease;
    }
    
    .service-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 20px rgba(0,0,0,0.15);
    }
    
    .skill-badge {
        background: #4B9CD3;
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 15px;
        font-size: 0.8rem;
        display: inline-block;
        margin: 0.2rem;
    }
    
    .mermaid-container {
        background: white;
        padding: 2rem;
        border-radius: 10px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        margin: 1rem 0;
    }
    
    .footer {
        text-align: center;
        padding: 1rem;
        background-color: #232F3E;
        color: white;
        margin-top: 1rem;
        border-radius: 8px;
    }
    
    .task-statement {
        background: linear-gradient(135deg, #FFE4B5 0%, #FFF8DC 100%);
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 5px solid #FF9900;
        margin: 1rem 0;
    }
    
    .data-pipeline-stage {
        background: #e3f2fd;
        padding: 1rem;
        border-radius: 8px;
        border: 2px solid #2196f3;
        margin: 0.5rem 0;
        text-align: center;
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
        <h3>Content Review Session 2: Domain 1 - Data Ingestion and Transformation</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Program Check-in section
    st.markdown("## 📋 Program Check-in")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>Domain 1: Data Ingestion and Transformation Focus</h4>
            <p>This domain focuses on the process of collecting, ingesting, and transforming data from various sources, 
            which is a crucial step in building data-driven applications and services on AWS.</p>
            <p><strong>Real-world Example:</strong> Imagine a ride-sharing company collecting data from mobile apps, 
            GPS devices, and payment systems. They need to ingest and transform this data to gain insights into 
            ride patterns, customer behavior, and revenue streams.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Progress indicator
        progress_value = 40  # 2nd session out of 5
        st.metric("Program Progress", f"{progress_value}%", "Session 2 of 5")
        st.progress(progress_value / 100)
        
        st.markdown("""
        <div style="background: #e8f5e8; padding: 1rem; border-radius: 8px; margin-top: 1rem;">
            <strong>📊 Domain 1 Weight</strong><br>
            <strong>34%</strong> of exam content
        </div>
        """, unsafe_allow_html=True)
    
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
            "status": "current",
            "description": "🎯 Data ingestion patterns, transformation techniques, and processing frameworks"
        },
        {
            "session": "Content Review 3",
            "topic": "Domain 2: Data Store Management", 
            "status": "upcoming",
            "description": "📚 Data warehouses, data lakes, and database management strategies"
        },
        {
            "session": "Content Review 4",
            "topic": "Domain 3: Data Operations and Support",
            "status": "upcoming", 
            "description": "📚 Monitoring, troubleshooting, and operational best practices"
        },
        {
            "session": "Content Review 5",
            "topic": "Domain 4: Data Security and Governance",
            "status": "upcoming",
            "description": "📚 Security controls, compliance, and data governance frameworks"
        }
    ]
    
    # Your Learning Journey - Mermaid Diagram
    st.markdown("### Your Learning Journey")
    
    mermaid_code = """
    graph LR
        A[✅ Session 1<br/>AWS & Data<br/>Fundamentals] --> B[🎯 Session 2<br/>Data Ingestion &<br/>Transformation]
        B --> C[📚 Session 3<br/>Data Store<br/>Management]
        C --> D[📚 Session 4<br/>Data Operations<br/>& Support]
        D --> E[📚 Session 5<br/>Data Security &<br/>Governance]
        E --> F[🎯 Certification<br/>Exam Ready]
        
        classDef completed fill:#28a745,stroke:#155724,stroke-width:3px,color:#fff
        classDef current fill:#FF9900,stroke:#232F3E,stroke-width:3px,color:#fff
        classDef upcoming fill:#6c757d,stroke:#495057,stroke-width:2px,color:#fff
        classDef target fill:#dc3545,stroke:#721c24,stroke-width:2px,color:#fff
        
        class A completed
        class B current
        class C,D,E upcoming
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
            status_icon = "📚"
        
        st.markdown(f"""
        <div class="roadmap-item {status_class}">
            <h4>{status_icon} {item['session']}: {item['topic']}</h4>
            <p>{item['description']}</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Today's Learning Outcomes
    st.markdown("## 🎯 Today's Learning Outcomes")
    st.markdown("*During this session, we will cover the four key task statements for Domain 1:*")
    
    # Task Statements
    task_statements = [
        {
            "number": "1.1",
            "title": "Perform data ingestion",
            "description": "Understanding throughput, latency, streaming vs batch patterns, and replayability",
            "icon": "📥"
        },
        {
            "number": "1.2", 
            "title": "Transform and process data",
            "description": "ETL pipelines, Apache Spark processing, and data transformation services",
            "icon": "🔄"
        },
        {
            "number": "1.3",
            "title": "Orchestrate data pipelines", 
            "description": "Workflow automation, event-driven architecture, and serverless orchestration",
            "icon": "🎭"
        },
        {
            "number": "1.4",
            "title": "Apply programming concepts",
            "description": "CI/CD for data pipelines, SQL optimization, and Infrastructure as Code",
            "icon": "💻"
        }
    ]
    
    col1, col2 = st.columns(2)
    
    for i, task in enumerate(task_statements):
        with col1 if i < 2 else col2:
            st.markdown(f"""
            <div class="task-statement">
                <h4>{task['icon']} Task Statement {task['number']}: {task['title']}</h4>
                <p>{task['description']}</p>
            </div>
            """, unsafe_allow_html=True)
    
    # Data Pipeline Journey
    st.markdown("## 🔄 Data Pipeline Journey - Understanding the Flow")
    st.markdown("*Let's walk through the data journey from ingestion to insights:*")
    
    # Data Pipeline Stages
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div class="data-pipeline-stage">
            <h4>📥 INGEST</h4>
            <p><strong>Gather Data</strong></p>
            <p>Kinesis, MSK, IoT Core</p>
            <small>Like gathering ingredients for cooking</small>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="data-pipeline-stage">
            <h4>🔄 TRANSFORM</h4>
            <p><strong>Process & Clean</strong></p>
            <p>Glue, Lambda, EMR</p>
            <small>Like prepping ingredients</small>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="data-pipeline-stage">
            <h4>🎭 ORCHESTRATE</h4>
            <p><strong>Coordinate Flow</strong></p>
            <p>Step Functions, Airflow</p>
            <small>Like following a recipe</small>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div class="data-pipeline-stage">
            <h4>💻 OPTIMIZE</h4>
            <p><strong>Monitor & Improve</strong></p>
            <p>CloudWatch, CI/CD</p>
            <small>Like perfecting the dish</small>
        </div>
        """, unsafe_allow_html=True)
    
    # Key AWS Services Deep Dive
    st.markdown("## ☁️ Key AWS Services for Data Ingestion & Transformation")
    
    # Streaming Services
    st.markdown("### 🌊 Streaming Data Services")
    
    streaming_services = [
        {
            "service": "Amazon Kinesis Data Streams",
            "description": "Scalable real-time data streaming service that captures gigabytes of data per second",
            "use_case": "Real-time analytics, log processing, IoT data streams",
            "key_features": ["Millisecond latency", "Durable storage", "Multiple consumers"]
        },
        {
            "service": "Amazon Data Firehose", 
            "description": "Captures, transforms, and loads streaming data into AWS data stores",
            "use_case": "Near real-time analytics with existing BI tools",
            "key_features": ["Serverless", "Built-in transformations", "Automatic scaling"]
        },
        {
            "service": "Amazon MSK (Managed Kafka)",
            "description": "Fully managed Apache Kafka service for streaming data processing",
            "use_case": "High-throughput, fault-tolerant streaming applications",
            "key_features": ["Open source compatible", "Enterprise security", "Low latency"]
        }
    ]
    
    for service in streaming_services:
        st.markdown(f"""
        <div class="service-card">
            <h4>🔄 {service['service']}</h4>
            <p><strong>Description:</strong> {service['description']}</p>
            <p><strong>Best for:</strong> {service['use_case']}</p>
            <div>
                {''.join([f'<span class="skill-badge">{feature}</span>' for feature in service['key_features']])}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # ETL Services  
    st.markdown("### 🔄 ETL & Data Processing Services")
    
    etl_services = [
        {
            "service": "AWS Glue",
            "description": "Serverless ETL service for data preparation and integration",
            "use_case": "Data cataloging, ETL jobs, schema discovery",
            "key_features": ["Visual ETL", "Data Catalog", "Spark-based processing"]
        },
        {
            "service": "AWS Lambda",
            "description": "Run code without provisioning servers for data transformations", 
            "use_case": "Event-driven data processing, lightweight transformations",
            "key_features": ["Sub-second billing", "Auto scaling", "Event triggers"]
        },
        {
            "service": "Amazon EMR",
            "description": "Big data platform using Apache Spark, Hadoop, and other frameworks",
            "use_case": "Large-scale data processing, machine learning, analytics",
            "key_features": ["Managed clusters", "Multiple frameworks", "Cost optimization"]
        }
    ]
    
    for service in etl_services:
        st.markdown(f"""
        <div class="service-card">
            <h4>⚙️ {service['service']}</h4>
            <p><strong>Description:</strong> {service['description']}</p>
            <p><strong>Best for:</strong> {service['use_case']}</p>
            <div>
                {''.join([f'<span class="skill-badge">{feature}</span>' for feature in service['key_features']])}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Weekly Digital Training Curriculum
    st.markdown("## 📚 Weekly Digital Training Curriculum")
    st.markdown("**Plan to complete these trainings during this week to keep up with the program.**")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.markdown("### AWS Skill Builder Learning Plan Courses")
        
        required_courses = [
            {
                "name": "Data Engineering on AWS – Foundations",
                "duration": "2-3 hours",
                "type": "Core Concepts"
            },
            {
                "name": "AWS PartnerCast – Right Data Streaming Architecture",
                "duration": "45 minutes", 
                "type": "Technical Deep Dive"
            },
            {
                "name": "Amazon EMR Getting Started",
                "duration": "1 hour",
                "type": "Service Introduction"
            },
            {
                "name": "AWS Glue Getting Started", 
                "duration": "1 hour",
                "type": "Service Introduction"
            }
        ]
        
        for course in required_courses:
            st.markdown(f"""
            <div class="training-item">
                <div class="training-icon">📖</div>
                <div>
                    <strong>{course['name']}</strong><br>
                    <small>{course['type']} • {course['duration']}</small>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Companion Learning Plan (Optional)")
        
        optional_labs = [
            {
                "name": "Building BI Dashboards with Amazon QuickSight",
                "type": "Hands-on Lab",
                "focus": "Visualization"
            },
            {
                "name": "Migrating RDS MySQL to Aurora with Read Replica",
                "type": "Hands-on Lab", 
                "focus": "Database Migration"
            }
        ]
        
        for lab in optional_labs:
            st.markdown(f"""
            <div class="training-item">
                <div class="training-icon">🧪</div>
                <div>
                    <strong>Lab:</strong> {lab['name']}<br>
                    <small>{lab['type']} • Focus: {lab['focus']}</small>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
    
    # AWS Skill Builder Subscription Information
    st.markdown("## 💡 AWS Skill Builder Subscription Benefits")
    
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
                <li>Basic labs and simulations</li>
            </ul>
            <p><strong>Perfect for getting started!</strong></p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="program-card">
            <h4>⭐ Individual Subscription</h4>
            <ul>
                <li>AWS SimuLearn (200+ trainings)</li>
                <li>Official Practice Exams</li>
                <li>1000+ hands-on labs</li>
                <li>AWS Jam Journeys</li>
                <li>AWS Digital Classroom (Annual)</li>
            </ul>
            <p><strong>$29/month or $449/year</strong></p>
        </div>
        """, unsafe_allow_html=True)
    
    # Real-World Example Section
    st.markdown("## 🏢 Real-World Example: Ride-Sharing Data Pipeline")
    
    st.markdown("""
    <div class="program-card">
        <h4>🚗 Scenario: Building a Complete Data Pipeline</h4>
        <p><strong>Challenge:</strong> A ride-sharing company needs to process real-time data from mobile apps, GPS devices, and payment systems to gain insights into ride patterns, customer behavior, and revenue streams.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Pipeline Architecture
    pipeline_mermaid = """
    graph TD
        A[📱 Mobile Apps] --> D[🌊 Kinesis Data Streams]
        B[📍 GPS Devices] --> D
        C[💳 Payment Systems] --> D
        
        D --> E[🔄 Kinesis Data Firehose]
        E --> F[📊 Data Transformation]
        F --> G[🗄️ S3 Data Lake]
        
        G --> H[⚙️ AWS Glue ETL]
        H --> I[📈 Amazon Athena]
        I --> J[📊 QuickSight Dashboard]
        
        classDef source fill:#FFE4B5,stroke:#FF9900,stroke-width:2px
        classDef process fill:#E3F2FD,stroke:#2196F3,stroke-width:2px
        classDef storage fill:#E8F5E8,stroke:#4CAF50,stroke-width:2px
        classDef analytics fill:#FCE4EC,stroke:#E91E63,stroke-width:2px
        
        class A,B,C source
        class D,E,F,H process
        class G storage
        class I,J analytics
    """
    
    st.markdown("### 🏗️ Complete Pipeline Architecture")
    common.mermaid(pipeline_mermaid, height=700, show_controls=False)
    
    # Pipeline Steps Breakdown
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="service-card">
            <h4>📥 Data Ingestion</h4>
            <ul>
                <li><strong>Kinesis Data Streams:</strong> Real-time data capture</li>
                <li><strong>Kinesis Data Firehose:</strong> Reliable delivery to S3</li>
                <li><strong>Benefits:</strong> Handles millions of events per second</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="service-card">
            <h4>🔄 Data Transformation</h4>
            <ul>
                <li><strong>AWS Glue:</strong> ETL jobs for data cleansing</li>
                <li><strong>Amazon Athena:</strong> SQL queries on transformed data</li>
                <li><strong>Benefits:</strong> Serverless, pay-per-query model</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Action Items and Next Steps
    st.markdown("## 🚀 Action Items & Next Steps")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>📝 This Week's Action Items</h4>
            <ul>
                <li>✅ Complete Data Engineering Foundations course</li>
                <li>✅ Hands-on: Set up a Kinesis Data Stream</li>
                <li>✅ Practice: Create your first Glue ETL job</li>
                <li>✅ Review: Streaming vs Batch processing patterns</li>
                <li>✅ Explore: AWS Lambda for data transformations</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 Study Strategy Tips</h4>
            <ul>
                <li>🔬 Focus on hands-on labs for practical experience</li>
                <li>📊 Understand when to use each AWS service</li>
                <li>⚡ Practice with real-time vs batch scenarios</li>
                <li>🔄 Build a simple end-to-end data pipeline</li>
                <li>👥 Join AWS data engineering communities</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Key Takeaways
    st.markdown("## 💡 Key Takeaways from This Session")
    
    takeaways = [
        "Data ingestion patterns vary by use case: streaming for real-time, batch for periodic processing",
        "AWS provides multiple ingestion services - choose based on throughput, latency, and integration needs",
        "ETL transforms raw data into business value - cleaning, enriching, and optimizing for analysis", 
        "Orchestration services like Step Functions coordinate complex data workflows automatically",
        "Programming concepts like CI/CD and IaC make data pipelines reliable and maintainable"
    ]
    
    for i, takeaway in enumerate(takeaways, 1):
        st.markdown(f"""
        <div class="learning-outcome">
            <strong>🔑 Key Point {i}:</strong> {takeaway}
        </div>
        """, unsafe_allow_html=True)
    
    # Helpful Resources
    with st.expander("📚 Additional Resources & Documentation"):
        st.markdown("""
        **Essential Documentation:**
        - [Kinesis Data Streams Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html)
        - [AWS Lambda with Kinesis Integration](https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html)
        - [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)
        - [Step Functions Best Practices](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-standard-vs-express.html)
        - [ETL Best Practices on AWS](https://aws.amazon.com/what-is/etl/)
        
        **Video Resources:**
        - AWS re:Invent sessions on data engineering
        - AWS PartnerCast technical deep dives
        - Hands-on tutorials in AWS Skill Builder
        
        **Practice Resources:**
        - AWS Workshops for data engineering
        - Sample datasets for practice pipelines
        - Community forums and study groups
        """)
    
    # Preparation for Next Session
    st.markdown("## 🔮 Coming Up Next: Domain 2 - Data Store Management")
    
    st.markdown("""
    <div class="program-card">
        <h4>📅 Session 3 Preview: Data Store Management</h4>
        <p>In our next session, we'll dive into:</p>
        <ul>
            <li>🏗️ <strong>Data Warehousing:</strong> Amazon Redshift, design patterns, and optimization</li>
            <li>🗄️ <strong>Data Lakes:</strong> S3-based architectures, Lake Formation, and governance</li>
            <li>🔄 <strong>Database Selection:</strong> When to use RDS, DynamoDB, or specialized databases</li>
            <li>📊 <strong>Performance Tuning:</strong> Query optimization and cost management strategies</li>
        </ul>
        <p><em>Get ready to learn about choosing the right storage solution for your data needs!</em></p>
    </div>
    """, unsafe_allow_html=True)
    
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
