import streamlit as st
import plotly.graph_objects as go
import utils.common as common
import utils.authenticate as authenticate

def main():
    # Page configuration
    st.set_page_config(
        page_title="Data Engineer Associate",
        page_icon="☁️",
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
        <h3>Content Review Session 1: AWS & Data Fundamentals</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Program Check-in section
    st.markdown("## 📋 Program Check-in")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>Welcome to Your Certification Journey!</h4>
            <p>This is a multi-modal program combining digital learning with instructor-led sessions. 
            Each session is designed to supplement your digital training and provide interactive 
            experiences to enhance your exam readiness.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Progress indicator
        progress_value = 20  # 1st session out of 5
        st.metric("Program Progress", f"{progress_value}%", "Session 1 of 5")
        st.progress(progress_value / 100)
    
    # Program Roadmap
    st.markdown("## 🗺️ Program Roadmap")
    
    roadmap_data = [
        {
            "session": "Content Review 1",
            "topic": "AWS & Data Fundamentals",
            "status": "current",
            "description": "Core AWS services, compute, networking, storage, and data fundamentals"
        },
        {
            "session": "Content Review 2", 
            "topic": "Domain 1: Data Ingestion and Transformation",
            "status": "upcoming",
            "description": "Data ingestion patterns, transformation techniques, and processing frameworks"
        },
        {
            "session": "Content Review 3",
            "topic": "Domain 2: Data Store Management", 
            "status": "upcoming",
            "description": "Data warehouses, data lakes, and database management strategies"
        },
        {
            "session": "Content Review 4",
            "topic": "Domain 3: Data Operations and Support",
            "status": "upcoming", 
            "description": "Monitoring, troubleshooting, and operational best practices"
        },
        {
            "session": "Content Review 5",
            "topic": "Domain 4: Data Security and Governance",
            "status": "upcoming",
            "description": "Security controls, compliance, and data governance frameworks"
        }
    ]
    
    # Your Learning Journey - Mermaid Diagram
    st.markdown("### Your Learning Journey")
    
    mermaid_code = """
    graph LR
        A[📚 Session 1<br/>AWS & Data<br/>Fundamentals] --> B[🔄 Session 2<br/>Data Ingestion &<br/>Transformation]
        B --> C[🗄️ Session 3<br/>Data Store<br/>Management]
        C --> D[⚙️ Session 4<br/>Data Operations<br/>& Support]
        D --> E[🔒 Session 5<br/>Data Security &<br/>Governance]
        E --> F[🎯 Certification<br/>Exam Ready]
        
        classDef current fill:#FF9900,stroke:#232F3E,stroke-width:3px,color:#fff
        classDef upcoming fill:#4B9CD3,stroke:#232F3E,stroke-width:2px,color:#fff
        classDef target fill:#28a745,stroke:#232F3E,stroke-width:2px,color:#fff
        
        class A current
        class B,C,D,E upcoming
        class F target
    """
    
    common.mermaid(mermaid_code, height=150, show_controls=False)

    
    
    # Roadmap details
    for i, item in enumerate(roadmap_data):
        status_class = "current-session" if item['status'] == 'current' else "roadmap-item"
        status_icon = "🎯" if item['status'] == 'current' else "📚"
        
        st.markdown(f"""
        <div class="roadmap-item {status_class}">
            <h4>{status_icon} {item['session']}: {item['topic']}</h4>
            <p>{item['description']}</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Today's Learning Outcomes
    st.markdown("## 🎯 Today's Learning Outcomes")
    
    learning_outcomes = [
        "AWS Compute - EC2, Containers, Auto Scaling, Load Balancing",
        "AWS Networking - VPC, Security Groups, IAM fundamentals", 
        "AWS Storage - S3, EBS, EFS, and storage types",
        "AWS Databases - RDS, DynamoDB, and specialized databases",
        "Three V's of Data - Volume, Variety, Velocity concepts",
        "AWS Services per V - Matching services to data characteristics"
    ]
    
    col1, col2 = st.columns(2)
    
    for i, outcome in enumerate(learning_outcomes):
        with col1 if i < 3 else col2:
            st.markdown(f"""
            <div class="learning-outcome">
                <strong>✅ {outcome}</strong>
            </div>
            """, unsafe_allow_html=True)
    
    # Weekly Digital Training Curriculum
    st.markdown("## 📚 Weekly Digital Training Curriculum")
    st.markdown("**What to get started on this week! Do your best to complete this week's training content.**")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.markdown("### AWS Skill Builder Learning Plan Courses")
        
        required_courses = [
            "AWS Technical Essentials",
            "Fundamentals of Analytics on AWS – Part 1", 
            "Fundamentals of Analytics on AWS – Part 2"
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
            "A Day in the Life of a Data Engineer",
            "Building with Amazon Redshift Clusters",
            "Serverless Architectures with DynamoDB and Kinesis"
        ]
        
        for lab in optional_labs:
            st.markdown(f"""
            <div class="training-item">
                <div class="training-icon">🧪</div>
                <div>
                    <strong>Lab:</strong> {lab}<br>
                    <small>Hands-on Practice</small>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    # AWS Skill Builder Subscription Information
    st.markdown("## 💡 AWS Skill Builder Subscription (Optional)")
    
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
            </ul>
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
            </ul>
            <p><strong>$29/month or $449/year</strong></p>
        </div>
        """, unsafe_allow_html=True)
    
    # Action Items and Next Steps
    st.markdown("## 🚀 Action Items & Next Steps")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="program-card">
            <h4>📝 This Week's Tasks</h4>
            <ul>
                <li>Complete AWS Technical Essentials</li>
                <li>Start Fundamentals of Analytics Part 1</li>
                <li>Set up AWS Skill Builder account</li>
                <li>Review AWS Global Infrastructure concepts</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="program-card">
            <h4>🎯 Preparation Tips</h4>
            <ul>
                <li>Focus on hands-on labs for practical experience</li>
                <li>Take notes on key AWS services</li>
                <li>Join study groups or AWS communities</li>
                <li>Schedule regular study sessions</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Helpful Resources
    with st.expander("📚 Additional Resources"):
        st.markdown("""
        **Documentation Links:**
        - [AWS Global Infrastructure](https://aws.amazon.com/about-aws/global-infrastructure/)
        - [VPC Infrastructure Security](https://docs.aws.amazon.com/vpc/latest/userguide/infrastructure-security.html)
        - [Kinesis Data Streams FAQs](https://aws.amazon.com/kinesis/data-streams/faqs/)
        - [AWS Data Warehouse Guide](https://aws.amazon.com/what-is/data-warehouse/)
        - [Understanding Data Lakes](https://aws.amazon.com/what-is/data-lake/)
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

