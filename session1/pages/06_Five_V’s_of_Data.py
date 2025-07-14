
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import utils.common as common
import utils.authenticate as authenticate

# Page configuration
st.set_page_config(
    page_title="Five V's of Data Learning Hub",
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
        
        .v-card {{
            background: white;
            padding: 20px;
            border-radius: 12px;
            border: 3px solid {AWS_COLORS['primary']};
            margin: 15px 0;
            text-align: center;
        }}
        
        .data-source-card {{
            background: linear-gradient(135deg, {AWS_COLORS['secondary']} 0%, {AWS_COLORS['dark_blue']} 100%);
            padding: 20px;
            border-radius: 12px;
            color: white;
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
            - 📊 The 5 V's of Big Data - Volume, Variety, Velocity, Veracity, Value
            - 🗃️ Data Source Types - Structured, Semi-structured, Unstructured
            - 📦 Volume Solutions - S3, Lake Formation, Redshift
            - 🔄 Variety Solutions - RDS, DynamoDB, OpenSearch
            - ⚡ Velocity Solutions - EMR, MSK, Kinesis, Lambda
            
            **Learning Objectives:**
            - Understand the Five V's of Big Data
            - Explore different data source types
            - Learn AWS services for each data challenge
            - Practice with interactive examples
            """)

def create_five_vs_mermaid():
    """Create mermaid diagram for the Five V's of Data"""
    return """
    graph TD
        BD[Big Data] --> V1[📦 Volume]
        BD --> V2[🔄 Variety]
        BD --> V3[⚡ Velocity]
        BD --> V4[✓ Veracity]
        BD --> V5[💎 Value]
        
        V1 --> V1A[Petabytes to Exabytes]
        V1 --> V1B[Storage Challenges]
        V1 --> V1C[Processing Scale]
        
        V2 --> V2A[Structured Data]
        V2 --> V2B[Semi-structured Data]
        V2 --> V2C[Unstructured Data]
        
        V3 --> V3A[Real-time Processing]
        V3 --> V3B[Stream Processing]
        V3 --> V3C[Batch Processing]
        
        V4 --> V4A[Data Quality]
        V4 --> V4B[Data Accuracy]
        V4 --> V4C[Data Consistency]
        
        V5 --> V5A[Business Insights]
        V5 --> V5B[Decision Making]
        V5 --> V5C[Competitive Advantage]
        
        style BD fill:#FF9900,stroke:#232F3E,color:#fff
        style V1 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style V2 fill:#3FB34F,stroke:#232F3E,color:#fff
        style V3 fill:#FF6B35,stroke:#232F3E,color:#fff
        style V4 fill:#232F3E,stroke:#FF9900,color:#fff
        style V5 fill:#FF9900,stroke:#232F3E,color:#fff
    """

def create_data_sources_mermaid():
    """Create mermaid diagram for data source types"""
    return """
    graph TD
        DS[Data Sources] --> ST[📋 Structured]
        DS --> SS[📄 Semi-Structured]
        DS --> US[📁 Unstructured]
        
        ST --> ST1[Relational Databases]
        ST --> ST2[CSV Files]
        ST --> ST3[Excel Spreadsheets]
        ST --> ST4[Data Warehouses]
        
        SS --> SS1[JSON Documents]
        SS --> SS2[XML Files]
        SS --> SS3[Web APIs]
        SS --> SS4[Log Files]
        
        US --> US1[Images & Videos]
        US --> US2[Social Media Posts]
        US --> US3[Email Content]
        US --> US4[Audio Files]
        
        style DS fill:#FF9900,stroke:#232F3E,color:#fff
        style ST fill:#4B9EDB,stroke:#232F3E,color:#fff
        style SS fill:#3FB34F,stroke:#232F3E,color:#fff
        style US fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_volume_architecture_mermaid():
    """Create mermaid diagram for volume solutions"""
    return """
    graph LR
        VD[📊 Volume Data] --> S3[📦 Amazon S3]
        VD --> LF[🏗️ Lake Formation]
        VD --> RS[🏢 Redshift]
        
        S3 --> S3A[Object Storage]
        S3 --> S3B[Unlimited Capacity]
        S3 --> S3C[Multiple Storage Classes]
        
        LF --> LFA[Data Lake Management]
        LF --> LFB[Security & Governance]
        LF --> LFC[Data Cataloging]
        
        RS --> RSA[Data Warehouse]
        RS --> RSB[SQL Analytics]
        RS --> RSC[Columnar Storage]
        
        style VD fill:#FF9900,stroke:#232F3E,color:#fff
        style S3 fill:#4B9EDB,stroke:#232F3E,color:#fff
        style LF fill:#3FB34F,stroke:#232F3E,color:#fff
        style RS fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_variety_architecture_mermaid():
    """Create mermaid diagram for variety solutions"""
    return """
    graph LR
        VR[🔄 Variety Data] --> RDS[🗄️ Amazon RDS]
        VR --> DDB[⚡ DynamoDB]
        VR --> OS[🔍 OpenSearch]
        
        RDS --> RDSA[Relational Data]
        RDS --> RDSB[ACID Compliance]
        RDS --> RDSC[Multiple Engines]
        
        DDB --> DDBA[NoSQL Documents]
        DDB --> DDBB[Key-Value Store]
        DDB --> DDBC[Serverless]
        
        OS --> OSA[Full-Text Search]
        OS --> OSB[Log Analytics]
        OS --> OSC[Real-time Insights]
        
        style VR fill:#FF9900,stroke:#232F3E,color:#fff
        style RDS fill:#4B9EDB,stroke:#232F3E,color:#fff
        style DDB fill:#3FB34F,stroke:#232F3E,color:#fff
        style OS fill:#FF6B35,stroke:#232F3E,color:#fff
    """

def create_velocity_architecture_mermaid():
    """Create mermaid diagram for velocity solutions"""
    return """
    graph LR
        VL[⚡ Velocity Data] --> EMR[🔧 Amazon EMR]
        VL --> MSK[📨 Amazon MSK]
        VL --> KDS[🌊 Kinesis Streams]
        VL --> LMB[⚡ AWS Lambda]
        
        EMR --> EMRA[Big Data Processing]
        EMR --> EMRB[Hadoop & Spark]
        EMR --> EMRC[Batch Analytics]
        
        MSK --> MSKA[Apache Kafka]
        MSK --> MSKB[Event Streaming]
        MSK --> MSKC[Message Queuing]
        
        KDS --> KDSA[Real-time Streaming]
        KDS --> KDSB[Data Ingestion]
        KDS --> KDSC[Stream Processing]
        
        LMB --> LMBA[Event-driven Computing]
        LMB --> LMBB[Serverless Processing]
        LMB --> LMBC[Micro-batch Processing]
        
        style VL fill:#FF9900,stroke:#232F3E,color:#fff
        style EMR fill:#4B9EDB,stroke:#232F3E,color:#fff
        style MSK fill:#3FB34F,stroke:#232F3E,color:#fff
        style KDS fill:#FF6B35,stroke:#232F3E,color:#fff
        style LMB fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_data_growth_chart():
    """Create data growth visualization"""
    years = list(range(2015, 2026))
    data_created = [8.5, 12.5, 18.2, 26.8, 39.4, 59.0, 80.0, 97.0, 120.0, 147.0, 181.0]
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=years,
        y=data_created,
        mode='lines+markers',
        name='Global Data Created',
        line=dict(color=AWS_COLORS['primary'], width=4),
        marker=dict(size=8, color=AWS_COLORS['primary'])
    ))
    
    fig.update_layout(
        title='Global Data Creation Growth (Zettabytes)',
        xaxis_title='Year',
        yaxis_title='Data Created (ZB)',
        plot_bgcolor='white',
        height=400
    )
    
    return fig

def five_vs_tab():
    """Content for the Five V's of Big Data tab"""
    st.markdown("# 📊 The Five V's of Big Data")
    st.markdown("*Understanding the fundamental characteristics that define Big Data*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 What are the Five V's?
    The **Five V's of Big Data** represent the key characteristics that distinguish big data from traditional data:
    **Volume**, **Variety**, **Velocity**, **Veracity**, and **Value**. These dimensions help organizations 
    understand and approach their data challenges systematically.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Five V's Architecture
    st.markdown("## 🏗️ The Five V's Framework")
    common.mermaid(create_five_vs_mermaid(), height=200)
    
    # Interactive V's Explorer
    st.markdown("## 🔍 Interactive V's Explorer")
    
    selected_v = st.selectbox("Select a V to explore:", [
        "📦 Volume - Scale of Data",
        "🔄 Variety - Types of Data", 
        "⚡ Velocity - Speed of Data",
        "✓ Veracity - Quality of Data",
        "💎 Value - Worth of Data"
    ])
    
    if "Volume" in selected_v:
        st.markdown('<div class="v-card">', unsafe_allow_html=True)
        st.markdown("""
        ## 📦 Volume - Scale of Data
        
        **Definition**: The massive amount of data generated every second
        
        **Key Statistics**:
        - 2.5 quintillion bytes of data created daily
        - 90% of world's data created in last 2 years
        - By 2025: 175 zettabytes of data globally
        
        **AWS Solutions**: Amazon S3, AWS Lake Formation, Amazon Redshift
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Data growth chart
        st.plotly_chart(create_data_growth_chart(), use_container_width=True)
        
    elif "Variety" in selected_v:
        st.markdown('<div class="v-card">', unsafe_allow_html=True)
        st.markdown("""
        ## 🔄 Variety - Types of Data
        
        **Definition**: Different formats and types of data from various sources
        
        **Data Types**:
        - **Structured**: Databases, spreadsheets (20%)
        - **Semi-structured**: JSON, XML, logs (10%)
        - **Unstructured**: Images, videos, social media (70%)
        
        **AWS Solutions**: Amazon RDS, DynamoDB, OpenSearch Service
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Variety pie chart
        variety_data = pd.DataFrame({
            'Data Type': ['Structured', 'Semi-structured', 'Unstructured'],
            'Percentage': [20, 10, 70],
            'Examples': ['SQL Databases', 'JSON/XML Files', 'Images/Videos']
        })
        
        fig = px.pie(variety_data, values='Percentage', names='Data Type',
                    title='Distribution of Data Types',
                    color_discrete_sequence=[AWS_COLORS['primary'], 
                                           AWS_COLORS['light_blue'], 
                                           AWS_COLORS['success']])
        st.plotly_chart(fig, use_container_width=True)
        
    elif "Velocity" in selected_v:
        st.markdown('<div class="v-card">', unsafe_allow_html=True)
        st.markdown("""
        ## ⚡ Velocity - Speed of Data
        
        **Definition**: The speed at which data is generated, processed, and analyzed
        
        **Processing Types**:
        - **Real-time**: < 1 second (fraud detection)
        - **Near real-time**: 1-10 seconds (recommendations)
        - **Batch**: Minutes to hours (reporting)
        - **Stream**: Continuous processing (IoT sensors)
        
        **AWS Solutions**: Amazon EMR, MSK, Kinesis Data Streams, AWS Lambda
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Velocity comparison
        velocity_data = pd.DataFrame({
            'Processing Type': ['Real-time', 'Near Real-time', 'Batch', 'Stream'],
            'Latency (seconds)': [0.1, 5, 3600, 0.5],
            'Use Case': ['Fraud Detection', 'Recommendations', 'Daily Reports', 'IoT Monitoring']
        })
        
        fig = px.bar(velocity_data, x='Processing Type', y='Latency (seconds)',
                    title='Data Processing Velocity Comparison',
                    color='Processing Type',
                    color_discrete_sequence=[AWS_COLORS['warning'], 
                                           AWS_COLORS['primary'],
                                           AWS_COLORS['secondary'],
                                           AWS_COLORS['light_blue']])
        fig.update_layout(yaxis_type="log")  # Fixed: use update_layout instead of update_yaxis
        st.plotly_chart(fig, use_container_width=True)
        
    elif "Veracity" in selected_v:
        st.markdown('<div class="v-card">', unsafe_allow_html=True)
        st.markdown("""
        ## ✓ Veracity - Quality of Data
        
        **Definition**: The trustworthiness, accuracy, and quality of data
        
        **Quality Dimensions**:
        - **Accuracy**: How correct is the data?
        - **Completeness**: Are there missing values?
        - **Consistency**: Is data uniform across sources?
        - **Timeliness**: Is data current and up-to-date?
        
        **Impact**: Poor data quality costs organizations $3.1 trillion annually in the US
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Data quality metrics
        quality_metrics = pd.DataFrame({
            'Quality Dimension': ['Accuracy', 'Completeness', 'Consistency', 'Timeliness'],
            'Your Data Score': [85, 92, 78, 88],
            'Industry Average': [75, 80, 70, 75]
        })
        
        fig = go.Figure()
        fig.add_trace(go.Bar(name='Your Data', x=quality_metrics['Quality Dimension'], 
                           y=quality_metrics['Your Data Score'],
                           marker_color=AWS_COLORS['success']))
        fig.add_trace(go.Bar(name='Industry Average', x=quality_metrics['Quality Dimension'], 
                           y=quality_metrics['Industry Average'],
                           marker_color=AWS_COLORS['warning']))
        
        fig.update_layout(title='Data Quality Assessment', yaxis_title='Quality Score (%)')
        st.plotly_chart(fig, use_container_width=True)
        
    elif "Value" in selected_v:
        st.markdown('<div class="v-card">', unsafe_allow_html=True)
        st.markdown("""
        ## 💎 Value - Worth of Data
        
        **Definition**: The business value and insights that can be extracted from data
        
        **Value Creation Process**:
        1. **Data Collection**: Gather relevant data
        2. **Data Processing**: Clean and prepare data
        3. **Data Analysis**: Apply analytics and ML
        4. **Insights Generation**: Extract actionable insights
        5. **Decision Making**: Drive business decisions
        
        **ROI**: Companies using big data see 5-6% increase in productivity
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Value creation funnel
        funnel_data = pd.DataFrame({
            'Stage': ['Raw Data', 'Clean Data', 'Processed Data', 'Insights', 'Business Value'],
            'Data Volume': [100, 85, 70, 40, 20],
            'Business Value': [5, 15, 35, 70, 100]
        })
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(go.Bar(x=funnel_data['Stage'], y=funnel_data['Data Volume'],
                           name='Data Volume', marker_color=AWS_COLORS['light_blue']),
                     secondary_y=False)
        
        fig.add_trace(go.Scatter(x=funnel_data['Stage'], y=funnel_data['Business Value'],
                               mode='lines+markers', name='Business Value',
                               line=dict(color=AWS_COLORS['primary'], width=3)),
                     secondary_y=True)
        
        fig.update_layout(title='Data to Value Transformation')
        fig.update_yaxes(title_text="Data Volume %", secondary_y=False)  # Fixed: use update_yaxes
        fig.update_yaxes(title_text="Business Value %", secondary_y=True)  # Fixed: use update_yaxes
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Interactive Quiz
    st.markdown("## 🧠 Knowledge Check")
    
    quiz_question = st.selectbox("Test your understanding:", [
        "Select a question...",
        "Which V represents the amount of data?",
        "What percentage of data is unstructured?", 
        "Which V focuses on data accuracy?",
        "What's the main goal of the Value V?"
    ])
    
    if quiz_question != "Select a question...":
        if st.button("Show Answer"):
            answers = {
                "Which V represents the amount of data?": "📦 **Volume** - It represents the scale and amount of data being generated and stored.",
                "What percentage of data is unstructured?": "📊 **70%** - The majority of today's data is unstructured (images, videos, social media, etc.)",
                "Which V focuses on data accuracy?": "✓ **Veracity** - It deals with data quality, accuracy, and trustworthiness.",
                "What's the main goal of the Value V?": "💎 **Extract business insights** - Transform data into actionable business value and competitive advantage."
            }
            
            st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
            st.markdown(f"### Answer: {answers[quiz_question]}")
            st.markdown('</div>', unsafe_allow_html=True)

def data_sources_tab():
    """Content for Data Source Types tab"""
    st.markdown("# 🗃️ Data Source Types")
    st.markdown("*Understanding different formats and structures of data*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Data Classification
    Data comes in different formats and structures. Understanding these types is crucial for choosing 
    the right storage and processing solutions. The three main categories are **Structured**, 
    **Semi-structured**, and **Unstructured** data.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Sources Architecture
    st.markdown("## 🏗️ Data Source Types Overview")
    common.mermaid(create_data_sources_mermaid(), height=200)
    
    # Interactive Data Type Explorer
    st.markdown("## 🔍 Interactive Data Type Explorer")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="data-source-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📋 Structured Data
        
        **Characteristics**:
        - Fixed schema/format
        - Organized in rows & columns
        - Easily searchable
        - ACID compliance
        
        **Examples**:
        - SQL databases
        - CSV files
        - Excel spreadsheets
        - ERP systems
        
        **Volume**: ~20% of all data
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="data-source-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📄 Semi-Structured Data
        
        **Characteristics**:
        - Flexible schema
        - Self-describing structure
        - Tags and metadata
        - NoSQL friendly
        
        **Examples**:
        - JSON documents
        - XML files
        - Web APIs
        - Log files
        
        **Volume**: ~10% of all data
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="data-source-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📁 Unstructured Data
        
        **Characteristics**:
        - No predefined structure
        - Rich information content
        - Requires specialized tools
        - Machine learning ready
        
        **Examples**:
        - Images & videos
        - Social media posts
        - Email content
        - Audio files
        
        **Volume**: ~70% of all data
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Data Example Generator
    st.markdown("## 🎮 Data Example Generator")
    
    data_type = st.selectbox("Choose data type to see examples:", [
        "Structured Data",
        "Semi-Structured Data", 
        "Unstructured Data"
    ])
    
    if st.button("Generate Example"):
        if data_type == "Structured Data":
            # Generate sample structured data
            sample_data = pd.DataFrame({
                'CustomerID': [1001, 1002, 1003, 1004, 1005],
                'Name': ['John Smith', 'Sarah Johnson', 'Mike Wilson', 'Lisa Brown', 'Tom Davis'],
                'Age': [28, 34, 42, 29, 38],
                'City': ['Seattle', 'Portland', 'Denver', 'Austin', 'Phoenix'],
                'Purchase_Amount': [150.50, 89.99, 234.75, 67.25, 412.00]
            })
            
            st.markdown("### 📊 Sample Structured Data (Customer Database)")
            st.dataframe(sample_data, use_container_width=True)
            
            st.markdown('<div class="code-container">', unsafe_allow_html=True)
            st.code("""
# SQL Query Example
SELECT CustomerID, Name, Purchase_Amount 
FROM customers 
WHERE Age > 30 
ORDER BY Purchase_Amount DESC;
            """, language='sql')
            st.markdown('</div>', unsafe_allow_html=True)
            
        elif data_type == "Semi-Structured Data":
            st.markdown("### 📄 Sample Semi-Structured Data (JSON)")
            
            json_example = """{
  "user_id": "12345",
  "profile": {
    "name": "Alex Thompson",
    "location": {
      "city": "San Francisco",
      "state": "CA",
      "coordinates": [37.7749, -122.4194]
    },
    "preferences": ["technology", "travel", "photography"],
    "social_links": {
      "twitter": "@alextech",
      "linkedin": "/in/alexthompson"
    }
  },
  "activity": {
    "last_login": "2025-07-14T10:30:00Z",
    "page_views": 47,
    "session_duration": 1250
  }
}"""
            
            st.markdown('<div class="code-container">', unsafe_allow_html=True)
            st.code(json_example, language='json')
            st.markdown('</div>', unsafe_allow_html=True)
            
        else:  # Unstructured Data
            st.markdown("### 📁 Sample Unstructured Data Examples")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                **📧 Email Content:**
                ```
                Subject: Quarterly Sales Review Meeting
                
                Hi Team,
                
                Hope you're all doing well! I wanted to schedule 
                our quarterly sales review for next Tuesday at 
                2:00 PM. We'll be discussing:
                
                - Q3 performance metrics
                - Client feedback analysis  
                - Strategic initiatives for Q4
                
                Please review the attached reports before the meeting.
                
                Best regards,
                Sarah
                ```
                """)
            
            with col2:
                st.markdown("""
                **🐦 Social Media Post:**
                ```
                @TechStartupXYZ: "Excited to announce our latest 
                AI-powered feature! 🚀 After months of development, 
                we're finally ready to revolutionize how businesses 
                analyze customer sentiment. 
                
                #AI #MachineLearning #CustomerInsights #Innovation
                
                👍 247 likes, 🔄 89 retweets, 💬 43 comments
                ```
                """)
    
    # Data Processing Complexity Comparison
    st.markdown("## ⚖️ Processing Complexity Comparison")
    
    complexity_data = pd.DataFrame({
        'Data Type': ['Structured', 'Semi-Structured', 'Unstructured'],
        'Storage Complexity': [2, 5, 8],
        'Processing Complexity': [3, 6, 9],
        'Analysis Complexity': [2, 6, 9],
        'Cost per GB': [0.5, 1.2, 2.8]
    })
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(x=complexity_data['Data Type'], 
                           y=complexity_data['Storage Complexity'],
                           mode='lines+markers', name='Storage',
                           line=dict(color=AWS_COLORS['primary'], width=3),
                           marker=dict(size=10)))
    
    fig.add_trace(go.Scatter(x=complexity_data['Data Type'], 
                           y=complexity_data['Processing Complexity'],
                           mode='lines+markers', name='Processing',
                           line=dict(color=AWS_COLORS['light_blue'], width=3),
                           marker=dict(size=10)))
    
    fig.add_trace(go.Scatter(x=complexity_data['Data Type'], 
                           y=complexity_data['Analysis Complexity'],
                           mode='lines+markers', name='Analysis',
                           line=dict(color=AWS_COLORS['success'], width=3),
                           marker=dict(size=10)))
    
    fig.update_layout(
        title='Data Type Complexity Comparison',
        yaxis_title='Complexity Level (1-10)',
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)

def volume_solutions_tab():
    """Content for Volume Solutions tab"""
    st.markdown("# 📦 Volume Solutions")
    st.markdown("*AWS services for handling massive amounts of data*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Tackling Data Volume Challenges
    As data volumes grow exponentially, traditional storage and processing methods become inadequate. 
    AWS provides scalable solutions to store, manage, and analyze massive datasets efficiently and cost-effectively.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Volume Architecture
    st.markdown("## 🏗️ Volume Solutions Architecture")
    common.mermaid(create_volume_architecture_mermaid(), height=1000)
    
    # Service deep dives
    st.markdown("## 🔍 AWS Volume Services Deep Dive")
    
    # Amazon S3 Section
    st.markdown("### 📦 Amazon Simple Storage Service (S3)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        **Key Features:**
        - Virtually unlimited storage capacity
        - 99.999999999% (11 9's) durability
        - Multiple storage classes for different needs
        - Global accessibility with regional control
        - Integrated with 100+ AWS services
        
        **Use Cases:**
        - Data lakes and big data analytics
        - Backup and disaster recovery
        - Content distribution
        - Static website hosting
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        # S3 Storage Classes Comparison
        s3_classes = pd.DataFrame({
            'Storage Class': ['Standard', 'IA', 'One Zone-IA', 'Glacier', 'Deep Archive'],
            'Cost ($/GB/month)': [0.023, 0.0125, 0.01, 0.004, 0.00099],
            'Retrieval Time': ['Immediate', 'Immediate', 'Immediate', '1-5 min', '12 hours'],
            'Use Case': ['Frequent Access', 'Infrequent Access', 'Non-Critical', 'Archive', 'Long-term Archive']
        })
        
        fig = px.bar(s3_classes, x='Storage Class', y='Cost ($/GB/month)',
                    title='S3 Storage Classes Cost Comparison',
                    color='Storage Class',
                    color_discrete_sequence=px.colors.qualitative.Set3)
        st.plotly_chart(fig, use_container_width=True)
    
    # Interactive S3 Cost Calculator
    st.markdown("#### 💰 S3 Cost Calculator")
    
    col3, col4, col5 = st.columns(3)
    
    with col3:
        data_size = st.number_input("Data Size (TB):", 0.1, 1000.0, 10.0)
        storage_class = st.selectbox("Storage Class:", ['Standard', 'IA', 'Glacier', 'Deep Archive'])
    
    with col4:
        requests = st.number_input("Monthly Requests (thousands):", 0, 10000, 1000)
        data_transfer = st.number_input("Data Transfer (GB/month):", 0, 10000, 100)
    
    with col5:
        if st.button("Calculate Cost"):
            costs = {'Standard': 0.023, 'IA': 0.0125, 'Glacier': 0.004, 'Deep Archive': 0.00099}
            storage_cost = data_size * 1024 * costs[storage_class]  # Convert TB to GB
            request_cost = requests * 0.0004  # $0.40 per 1M requests
            transfer_cost = max(0, data_transfer - 100) * 0.09  # First 100GB free
            total_cost = storage_cost + request_cost + transfer_cost
            
            st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
            st.markdown(f"""
            **Monthly Cost Breakdown:**
            - Storage: ${storage_cost:.2f}
            - Requests: ${request_cost:.2f}
            - Data Transfer: ${transfer_cost:.2f}
            - **Total: ${total_cost:.2f}/month**
            """)
            st.markdown('</div>', unsafe_allow_html=True)
    
    # AWS Lake Formation Section
    st.markdown("### 🏗️ AWS Lake Formation")
    
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    **Purpose**: Simplifies building, securing, and managing data lakes
    
    **Key Capabilities:**
    - **Data Ingestion**: From databases, data warehouses, and streaming sources
    - **Data Cataloging**: Automatic discovery and classification of data
    - **Security & Governance**: Fine-grained access controls and audit trails
    - **Data Transformation**: Built-in ETL capabilities with AWS Glue
    
    **Benefits**:
    - Reduces data lake setup time from months to days
    - Centralizes security and governance policies
    - Integrates seamlessly with analytics services
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Amazon Redshift Section
    st.markdown("### 🏢 Amazon Redshift")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        **Architecture:**
        - **Columnar Storage**: Optimized for analytics queries
        - **Massively Parallel Processing (MPP)**: Distributes queries across nodes
        - **Compression**: Up to 75% storage savings
        - **Result Caching**: Improves query performance
        
        **Scaling Options:**
        - **RA3 Instances**: Separate compute and storage
        - **Serverless**: Pay-per-query pricing
        - **Concurrency Scaling**: Handle concurrent workloads
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        # Redshift Performance Comparison
        performance_data = pd.DataFrame({
            'Query Type': ['Simple Aggregation', 'Complex Join', 'Window Functions', 'Data Load'],
            'Traditional DB (minutes)': [15, 45, 30, 120],
            'Redshift (minutes)': [2, 8, 5, 15]
        })
        
        fig = go.Figure()
        fig.add_trace(go.Bar(name='Traditional DB', x=performance_data['Query Type'], 
                           y=performance_data['Traditional DB (minutes)'],
                           marker_color=AWS_COLORS['secondary']))
        fig.add_trace(go.Bar(name='Redshift', x=performance_data['Query Type'], 
                           y=performance_data['Redshift (minutes)'],
                           marker_color=AWS_COLORS['primary']))
        
        fig.update_layout(title='Query Performance Comparison', yaxis_title='Execution Time (minutes)')
        st.plotly_chart(fig, use_container_width=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Working with Volume Data")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
import boto3
import pandas as pd
from io import StringIO

# Initialize AWS clients
s3 = boto3.client('s3')
redshift = boto3.client('redshift-data')

# 1. Upload large dataset to S3
def upload_to_s3(dataframe, bucket, key):
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue(),
        StorageClass='STANDARD_IA'  # Cost-optimized for infrequent access
    )
    print(f"Data uploaded to s3://{bucket}/{key}")

# 2. Create data lake with Lake Formation
def setup_data_lake():
    lakeformation = boto3.client('lakeformation')
    
    # Register S3 location as data lake
    response = lakeformation.register_resource(
        ResourceArn='arn:aws:s3:::my-data-lake-bucket/raw-data/',
        UseServiceLinkedRole=True
    )
    
    # Grant permissions
    lakeformation.grant_permissions(
        Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789:role/DataAnalyst'},
        Resource={'Database': {'Name': 'sales_database'}},
        Permissions=['SELECT', 'DESCRIBE']
    )
    
    print("Data lake configured with Lake Formation")

# 3. Load data into Redshift for analytics
def load_to_redshift():
    # Create table in Redshift
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sales_data (
        order_id BIGINT,
        customer_id INT,
        product_name VARCHAR(255),
        quantity INT,
        price DECIMAL(10,2),
        order_date DATE
    );
    """
    
    # Execute table creation
    redshift.execute_statement(
        ClusterIdentifier='my-redshift-cluster',
        Database='sales_db',
        Sql=create_table_sql
    )
    
    # Load data from S3 to Redshift
    copy_sql = """
    COPY sales_data
    FROM 's3://my-data-lake-bucket/processed-data/sales/'
    IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftS3AccessRole'
    FORMAT AS CSV
    IGNOREHEADER 1;
    """
    
    redshift.execute_statement(
        ClusterIdentifier='my-redshift-cluster',
        Database='sales_db',
        Sql=copy_sql
    )
    
    print("Data loaded into Redshift for analytics")

# 4. Analyze large datasets
def analyze_sales_data():
    analysis_sql = """
    SELECT 
        DATE_TRUNC('month', order_date) as month,
        COUNT(*) as total_orders,
        SUM(quantity * price) as total_revenue,
        AVG(quantity * price) as avg_order_value
    FROM sales_data
    WHERE order_date >= '2025-01-01'
    GROUP BY DATE_TRUNC('month', order_date)
    ORDER BY month;
    """
    
    result = redshift.execute_statement(
        ClusterIdentifier='my-redshift-cluster',
        Database='sales_db',
        Sql=analysis_sql
    )
    
    return result

# Example usage
if __name__ == "__main__":
    # Handle petabyte-scale data processing
    setup_data_lake()
    load_to_redshift()
    results = analyze_sales_data()
    print("Volume data analysis completed!")
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def variety_solutions_tab():
    """Content for Variety Solutions tab"""
    st.markdown("# 🔄 Variety Solutions")
    st.markdown("*AWS services for handling different types and formats of data*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Managing Data Variety
    Modern applications generate data in multiple formats - from structured database records to 
    unstructured social media posts. AWS provides specialized database services optimized for 
    different data types and access patterns.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Variety Architecture
    st.markdown("## 🏗️ Variety Solutions Architecture")
    common.mermaid(create_variety_architecture_mermaid(), height=1000)
    
    # Service Comparison Matrix
    st.markdown("## 📊 Service Comparison Matrix")
    
    comparison_data = pd.DataFrame({
        'Service': ['Amazon RDS', 'Amazon DynamoDB', 'Amazon OpenSearch'],
        'Data Type': ['Structured', 'Semi-structured', 'Unstructured'],
        'Primary Use Case': ['Relational Data', 'NoSQL Documents', 'Search & Analytics'],
        'Scaling': ['Vertical', 'Horizontal', 'Horizontal'],
        'Query Language': ['SQL', 'NoSQL APIs', 'REST APIs'],
        'Consistency': ['ACID', 'Eventually Consistent', 'Near Real-time']
    })
    
    st.dataframe(comparison_data, use_container_width=True)
    
    # Amazon RDS Section
    st.markdown("### 🗄️ Amazon Relational Database Service (RDS)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        **Supported Engines:**
        - **MySQL**: Popular open-source database
        - **PostgreSQL**: Advanced open-source database
        - **MariaDB**: MySQL-compatible database
        - **Oracle**: Enterprise-grade database
        - **SQL Server**: Microsoft's relational database
        - **Aurora**: AWS's cloud-native database
        
        **Key Features:**
        - Automated backups and point-in-time recovery
        - Multi-AZ deployments for high availability
        - Read replicas for improved performance
        - Automated software patching
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        # RDS Instance Performance
        rds_performance = pd.DataFrame({
            'Instance Class': ['db.t3.micro', 'db.m5.large', 'db.r5.xlarge', 'db.r5.4xlarge'],
            'vCPUs': [2, 2, 4, 16],
            'Memory (GB)': [1, 8, 32, 128],
            'Network Performance': ['Low', 'Moderate', 'High', 'Very High'],
            'Monthly Cost ($)': [15, 140, 560, 2240]
        })
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(go.Bar(x=rds_performance['Instance Class'], 
                           y=rds_performance['Memory (GB)'],
                           name='Memory (GB)', marker_color=AWS_COLORS['primary']),
                     secondary_y=False)
        
        fig.add_trace(go.Scatter(x=rds_performance['Instance Class'], 
                               y=rds_performance['Monthly Cost ($)'],
                               mode='lines+markers', name='Monthly Cost ($)',
                               line=dict(color=AWS_COLORS['warning'], width=3)),
                     secondary_y=True)
        
        fig.update_layout(title='RDS Instance Performance vs Cost')
        # fig.update_yaxis(title_text="Memory (GB)", secondary_y=False)
        # fig.update_yaxis(title_text="Monthly Cost ($)", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Interactive RDS Configuration
    st.markdown("#### ⚙️ Interactive RDS Configuration")
    
    col3, col4, col5 = st.columns(3)
    
    with col3:
        db_engine = st.selectbox("Database Engine:", ['MySQL', 'PostgreSQL', 'Oracle', 'SQL Server'])
        instance_class = st.selectbox("Instance Class:", ['db.t3.micro', 'db.m5.large', 'db.r5.xlarge'])
    
    with col4:
        storage_type = st.selectbox("Storage Type:", ['gp2 (General Purpose)', 'io1 (Provisioned IOPS)', 'gp3 (Latest Generation)'])
        storage_size = st.slider("Storage Size (GB):", 20, 1000, 100)
    
    with col5:
        multi_az = st.checkbox("Multi-AZ Deployment", value=True)
        backup_retention = st.slider("Backup Retention (days):", 1, 35, 7)
    
    if st.button("Estimate RDS Cost"):
        base_costs = {'db.t3.micro': 15, 'db.m5.large': 140, 'db.r5.xlarge': 560}
        instance_cost = base_costs[instance_class]
        storage_cost = storage_size * 0.115  # $0.115 per GB for gp2
        if multi_az:
            instance_cost *= 2
        
        total_cost = instance_cost + storage_cost
        
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown(f"""
        **RDS Configuration Summary:**
        - **Engine**: {db_engine}
        - **Instance**: {instance_class}
        - **Storage**: {storage_size} GB ({storage_type})
        - **High Availability**: {'Yes' if multi_az else 'No'}
        - **Backup Retention**: {backup_retention} days
        
        **Estimated Monthly Cost**: ${total_cost:.2f}
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Amazon DynamoDB Section
    st.markdown("### ⚡ Amazon DynamoDB")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        **NoSQL Document Database:**
        - Key-value and document data models
        - Serverless with on-demand scaling
        - Single-digit millisecond latency
        - Global tables for multi-region replication
        
        **Use Cases:**
        - Mobile and web applications
        - Gaming leaderboards
        - IoT applications
        - Real-time personalization
        
        **Pricing Models:**
        - **On-Demand**: Pay per request
        - **Provisioned**: Reserve read/write capacity
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        # DynamoDB scaling demonstration
        time_series = pd.date_range(start='2025-07-01', end='2025-07-14', freq='H')
        traffic_pattern = np.sin(np.arange(len(time_series)) * 2 * np.pi / 24) * 500 + 1000
        traffic_pattern += np.random.normal(0, 100, len(time_series))
        
        scaling_data = pd.DataFrame({
            'Time': time_series,
            'Read Requests': traffic_pattern,
            'Provisioned Capacity': [1000] * len(time_series),
            'Auto Scaled Capacity': np.maximum(traffic_pattern * 1.2, 500)
        })
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=scaling_data['Time'], y=scaling_data['Read Requests'],
                               name='Actual Traffic', line=dict(color=AWS_COLORS['primary'])))
        fig.add_trace(go.Scatter(x=scaling_data['Time'], y=scaling_data['Provisioned Capacity'],
                               name='Fixed Provisioning', line=dict(color=AWS_COLORS['secondary'], dash='dash')))
        fig.add_trace(go.Scatter(x=scaling_data['Time'], y=scaling_data['Auto Scaled Capacity'],
                               name='Auto Scaling', line=dict(color=AWS_COLORS['success'])))
        
        fig.update_layout(title='DynamoDB Auto Scaling vs Fixed Provisioning', 
                         yaxis_title='Read Capacity Units')
        st.plotly_chart(fig, use_container_width=True)
    
    # Amazon OpenSearch Section
    st.markdown("### 🔍 Amazon OpenSearch Service")
    
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    **Full-Text Search and Analytics:**
    - **Search Capabilities**: Full-text search, autocomplete, faceted search
    - **Analytics**: Real-time dashboards with Kibana/OpenSearch Dashboards
    - **Log Analysis**: Centralized logging and monitoring
    - **Machine Learning**: Anomaly detection and forecasting
    
    **Common Use Cases:**
    - **Application Search**: E-commerce product search, content search
    - **Log Analytics**: Application logs, security logs, audit trails
    - **Real-time Analytics**: Business metrics, user behavior analysis
    - **Security Analytics**: Threat detection, compliance monitoring
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Search Example
    st.markdown("#### 🔍 Interactive Search Demo")
    
    sample_products = [
        {"name": "Wireless Bluetooth Headphones", "category": "Electronics", "price": 99.99, "rating": 4.5},
        {"name": "Organic Coffee Beans", "category": "Food", "price": 24.99, "rating": 4.8},
        {"name": "Yoga Exercise Mat", "category": "Sports", "price": 29.99, "rating": 4.3},
        {"name": "Smart Fitness Watch", "category": "Electronics", "price": 199.99, "rating": 4.6},
        {"name": "Organic Green Tea", "category": "Food", "price": 18.99, "rating": 4.7}
    ]
    
    search_term = st.text_input("Search products:", "wireless")
    
    if search_term:
        # Simple search simulation
        filtered_products = [p for p in sample_products 
                           if search_term.lower() in p["name"].lower() or 
                              search_term.lower() in p["category"].lower()]
        
        if filtered_products:
            st.markdown("**Search Results:**")
            for product in filtered_products:
                st.markdown(f"- **{product['name']}** ({product['category']}) - ${product['price']} ⭐{product['rating']}")
        else:
            st.markdown("No products found matching your search.")
    
    # Code Example
    st.markdown("## 💻 Code Example: Working with Variety Data")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
import boto3
import json
from decimal import Decimal

# Initialize AWS clients
rds = boto3.client('rds')
dynamodb = boto3.resource('dynamodb')
opensearch = boto3.client('opensearch')

# 1. Working with Structured Data in RDS
def setup_rds_database():
    import pymysql
    
    # Connect to RDS MySQL instance
    connection = pymysql.connect(
        host='my-rds-instance.cluster-xyz.us-east-1.rds.amazonaws.com',
        user='admin',
        password='mypassword',
        database='ecommerce',
        charset='utf8mb4'
    )
    
    cursor = connection.cursor()
    
    # Create structured table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INT AUTO_INCREMENT PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        registration_date DATE,
        total_orders INT DEFAULT 0,
        total_spent DECIMAL(10,2) DEFAULT 0.00
    );
    """
    
    cursor.execute(create_table_sql)
    
    # Insert structured data
    insert_sql = """
    INSERT INTO customers (first_name, last_name, email, registration_date)
    VALUES (%s, %s, %s, %s)
    """
    
    customer_data = [
        ('John', 'Smith', 'john.smith@email.com', '2025-01-15'),
        ('Sarah', 'Johnson', 'sarah.j@email.com', '2025-02-20'),
        ('Mike', 'Wilson', 'mike.w@email.com', '2025-03-10')
    ]
    
    cursor.executemany(insert_sql, customer_data)
    connection.commit()
    connection.close()
    
    print("Structured data stored in RDS")

# 2. Working with Semi-structured Data in DynamoDB
def setup_dynamodb_table():
    # Create DynamoDB table for semi-structured data
    table = dynamodb.create_table(
        TableName='user-profiles',
        KeySchema=[
            {'AttributeName': 'user_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'user_id', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Wait for table to be created
    table.wait_until_exists()
    
    # Insert semi-structured document
    user_profile = {
        'user_id': 'user123',
        'profile': {
            'name': 'Alex Thompson',
            'location': {
                'city': 'San Francisco',
                'state': 'CA',
                'coordinates': [Decimal('37.7749'), Decimal('-122.4194')]
            },
            'preferences': ['technology', 'travel', 'photography'],
            'social_links': {
                'twitter': '@alextech',
                'linkedin': '/in/alexthompson'
            }
        },
        'activity': {
            'last_login': '2025-07-14T10:30:00Z',
            'page_views': 47,
            'session_duration': 1250
        },
        'custom_attributes': {
            'subscription_tier': 'premium',
            'experiment_groups': ['A', 'C'],
            'feature_flags': {
                'new_ui': True,
                'beta_features': False
            }
        }
    }
    
    table.put_item(Item=user_profile)
    print("Semi-structured data stored in DynamoDB")

# 3. Working with Unstructured Data in OpenSearch
def setup_opensearch_index():
    # Create OpenSearch index for unstructured content
    index_body = {
        "mappings": {
            "properties": {
                "title": {"type": "text", "analyzer": "english"},
                "content": {"type": "text", "analyzer": "english"},
                "tags": {"type": "keyword"},
                "publish_date": {"type": "date"},
                "author": {"type": "keyword"},
                "sentiment": {"type": "float"}
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        }
    }
    
    # Note: This is a simplified example
    # In practice, you'd use the opensearch-py library
    print("Creating OpenSearch index for unstructured content...")
    
    # Sample unstructured documents
    documents = [
        {
            "title": "The Future of AI in Healthcare",
            "content": "Artificial intelligence is revolutionizing healthcare by enabling faster diagnosis, personalized treatment plans, and predictive analytics. Machine learning algorithms can analyze medical images with unprecedented accuracy...",
            "tags": ["AI", "healthcare", "machine-learning"],
            "publish_date": "2025-07-14",
            "author": "Dr. Sarah Chen",
            "sentiment": 0.85
        },
        {
            "title": "Customer Feedback Analysis",
            "content": "Our latest product received mixed reviews. Customers love the new features but are concerned about the learning curve. We should focus on improving user onboarding...",
            "tags": ["product", "feedback", "customer-experience"],
            "publish_date": "2025-07-13",
            "author": "Product Team",
            "sentiment": 0.65
        }
    ]
    
    print("Unstructured data indexed in OpenSearch")

# 4. Query across different data types
def analyze_customer_insights():
    # Combine structured data from RDS
    rds_query = """
    SELECT customer_id, email, total_orders, total_spent
    FROM customers
    WHERE total_spent > 100
    ORDER BY total_spent DESC;
    """
    
    # Query semi-structured data from DynamoDB
    table = dynamodb.Table('user-profiles')
    dynamodb_response = table.get_item(Key={'user_id': 'user123'})
    
    # Search unstructured content in OpenSearch
    search_query = {
        "query": {
            "multi_match": {
                "query": "customer feedback",
                "fields": ["title", "content"]
            }
        },
        "sort": [{"sentiment": {"order": "desc"}}]
    }
    
    print("Cross-platform data analysis completed!")

# Example usage
if __name__ == "__main__":
    setup_rds_database()         # Structured data
    setup_dynamodb_table()       # Semi-structured data  
    setup_opensearch_index()     # Unstructured data
    analyze_customer_insights()  # Cross-platform analytics
    ''', language='python')
    st.markdown('</div>', unsafe_allow_html=True)

def velocity_solutions_tab():
    """Content for Velocity Solutions tab"""
    st.markdown("# ⚡ Velocity Solutions")
    st.markdown("*AWS services for processing data at high speed and in real-time*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Handling Data Velocity
    Modern applications generate data continuously at high speeds. From IoT sensors to user interactions,
    businesses need to process, analyze, and act on data in real-time to remain competitive and responsive.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Velocity Architecture
    st.markdown("## 🏗️ Velocity Solutions Architecture")
    common.mermaid(create_velocity_architecture_mermaid(), height=1000)
    
    # Processing Types Comparison
    st.markdown("## ⚖️ Data Processing Types")
    
    processing_data = pd.DataFrame({
        'Processing Type': ['Batch', 'Micro-batch', 'Stream', 'Real-time'],
        'Latency': ['Hours-Days', 'Minutes', 'Seconds', 'Milliseconds'],
        'Throughput': ['Very High', 'High', 'Medium', 'Low-Medium'],
        'Complexity': ['Low', 'Medium', 'High', 'Very High'],
        'Cost': ['Low', 'Medium', 'High', 'Very High'],
        'AWS Service': ['EMR', 'Lambda', 'Kinesis', 'Lambda/Kinesis']
    })
    
    st.dataframe(processing_data, use_container_width=True)
    
    # Amazon EMR Section
    st.markdown("### 🔧 Amazon EMR (Elastic MapReduce)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        **Big Data Processing Platform:**
        - **Apache Spark**: In-memory data processing
        - **Apache Hadoop**: Distributed storage and processing
        - **Apache Hive**: Data warehouse software
        - **Apache HBase**: NoSQL database
        - **Presto**: Interactive SQL query engine
        
        **Use Cases:**
        - ETL data processing
        - Machine learning at scale
        - Log analysis and mining
        - Financial risk modeling
        - Bioinformatics research
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        # EMR Cluster Performance
        cluster_data = pd.DataFrame({
            'Instance Type': ['m5.xlarge', 'm5.2xlarge', 'c5.4xlarge', 'r5.4xlarge'],
            'vCPUs': [4, 8, 16, 16],
            'Memory (GB)': [16, 32, 32, 128],
            'Processing Speed (GB/hour)': [100, 200, 400, 350],
            'Cost per Hour ($)': [0.192, 0.384, 0.768, 1.008]
        })
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(go.Bar(x=cluster_data['Instance Type'], 
                           y=cluster_data['Processing Speed (GB/hour)'],
                           name='Processing Speed (GB/h)', marker_color=AWS_COLORS['primary']),
                     secondary_y=False)
        
        fig.add_trace(go.Scatter(x=cluster_data['Instance Type'], 
                               y=cluster_data['Cost per Hour ($)'],
                               mode='lines+markers', name='Cost per Hour ($)',
                               line=dict(color=AWS_COLORS['warning'], width=3)),
                     secondary_y=True)
        
        fig.update_layout(title='EMR Cluster Performance vs Cost')
        fig.update_yaxes(title_text="Processing Speed (GB/hour)", secondary_y=False)
        fig.update_yaxes(title_text="Cost per Hour ($)", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Amazon MSK Section
    st.markdown("### 📨 Amazon Managed Streaming for Apache Kafka (MSK)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        **Event Streaming Platform:**
        - **High Throughput**: Millions of events per second
        - **Durable Storage**: Configurable retention periods
        - **Fault Tolerant**: Multi-AZ replication
        - **Exactly-Once Delivery**: Ensures message reliability
        
        **Key Features:**
        - Fully managed Apache Kafka
        - Automatic scaling and patching
        - Integration with AWS services
        - Multiple security options
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        # Kafka throughput simulation
        time_points = np.arange(0, 60, 1)  # 60 seconds
        base_throughput = 10000  # messages per second
        throughput_variation = base_throughput + 5000 * np.sin(time_points / 10) + np.random.normal(0, 1000, len(time_points))
        throughput_variation = np.maximum(throughput_variation, 0)
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=time_points,
            y=throughput_variation,
            mode='lines',
            name='Message Throughput',
            line=dict(color=AWS_COLORS['primary'], width=2),
            fill='tonexty'
        ))
        
        fig.update_layout(
            title='Kafka Message Throughput Over Time',
            xaxis_title='Time (seconds)',
            yaxis_title='Messages per Second',
            height=300
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Amazon Kinesis Section
    st.markdown("### 🌊 Amazon Kinesis Data Streams")
    
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    **Real-time Data Streaming:**
    - **Kinesis Data Streams**: Real-time data ingestion and processing
    - **Kinesis Data Firehose**: Load streaming data into data stores
    - **Kinesis Data Analytics**: Real-time analytics with SQL
    - **Kinesis Video Streams**: Streaming video for analytics and ML
    
    **Capabilities:**
    - Sub-second processing latency
    - Automatic scaling with shards
    - Replay capability for up to 7 days
    - Integration with Lambda for serverless processing
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Interactive Kinesis Configuration
    st.markdown("#### ⚙️ Interactive Kinesis Stream Configuration")
    
    col3, col4, col5 = st.columns(3)
    
    with col3:
        shard_count = st.slider("Number of Shards:", 1, 100, 5)
        retention_hours = st.slider("Data Retention (hours):", 24, 168, 24)
    
    with col4:
        records_per_second = st.number_input("Records per Second:", 100, 100000, 1000)
        record_size_kb = st.number_input("Record Size (KB):", 1, 1000, 10)
    
    with col5:
        if st.button("Calculate Kinesis Capacity"):
            # Each shard can handle 1000 records/sec or 1MB/sec
            records_capacity = shard_count * 1000
            throughput_capacity_mb = shard_count * 1.0
            throughput_needed_mb = (records_per_second * record_size_kb) / 1024
            
            sufficient_shards = records_per_second <= records_capacity and throughput_needed_mb <= throughput_capacity_mb
            
            st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
            st.markdown(f"""
            **Kinesis Stream Capacity:**
            - **Shards**: {shard_count}
            - **Records Capacity**: {records_capacity:,}/sec
            - **Throughput Capacity**: {throughput_capacity_mb} MB/sec
            - **Your Requirements**: {records_per_second:,} records/sec, {throughput_needed_mb:.2f} MB/sec
            - **Status**: {'✅ Sufficient' if sufficient_shards else '❌ Need more shards'}
            
            **Monthly Cost**: ${shard_count * 15 + (retention_hours / 24) * shard_count * 2:.2f}
            """)
            st.markdown('</div>', unsafe_allow_html=True)
    
    # AWS Lambda Section
    st.markdown("### ⚡ AWS Lambda")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="concept-card">', unsafe_allow_html=True)
        st.markdown("""
        **Serverless Event Processing:**
        - **Event-driven**: Triggered by various AWS services
        - **Auto-scaling**: Handles from 1 to 1000s of concurrent executions
        - **Cost-effective**: Pay only for compute time used
        - **Multiple Languages**: Python, Node.js, Java, Go, .NET, Ruby
        
        **Common Triggers:**
        - Kinesis Data Streams
        - DynamoDB Streams
        - S3 events
        - API Gateway requests
        - CloudWatch Events
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        # Lambda execution metrics
        lambda_metrics = pd.DataFrame({
            'Memory (MB)': [128, 256, 512, 1024, 3008],
            'Max Duration (sec)': [15, 15, 15, 15, 15],
            'Cold Start (ms)': [400, 350, 300, 200, 100],
            'Cost per 100ms ($)': [0.0000002083, 0.0000004167, 0.0000008333, 0.0000016667, 0.0000048958]
        })
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(go.Bar(x=lambda_metrics['Memory (MB)'], 
                           y=lambda_metrics['Cold Start (ms)'],
                           name='Cold Start Time (ms)', marker_color=AWS_COLORS['warning']),
                     secondary_y=False)
        
        fig.add_trace(go.Scatter(x=lambda_metrics['Memory (MB)'], 
                               y=lambda_metrics['Cost per 100ms ($)'] * 1000000,
                               mode='lines+markers', name='Cost per 100ms (μ$)',
                               line=dict(color=AWS_COLORS['primary'], width=3)),
                     secondary_y=True)
        
        fig.update_layout(title='Lambda Memory vs Performance & Cost')
        fig.update_yaxes(title_text="Cold Start Time (ms)", secondary_y=False)
        fig.update_yaxes(title_text="Cost per 100ms (μ$)", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Real-time Processing Pipeline Demo
    st.markdown("## 🚀 Real-time Processing Pipeline Demo")
    
    if st.button("🎬 Simulate Real-time Data Processing"):
        # Create animated visualization of data flow
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        pipeline_steps = [
            "📊 Generating sample IoT sensor data...",
            "🌊 Streaming data to Kinesis...",
            "⚡ Lambda processing events...",
            "📈 Real-time analytics running...",
            "🎯 Triggering alerts and notifications...",
            "✅ Pipeline processing complete!"
        ]
        
        for i, step in enumerate(pipeline_steps):
            status_text.text(step)
            progress_bar.progress((i + 1) / len(pipeline_steps))
            # Simulate processing time
            import time
            time.sleep(1)
        
        # Show processing results
        st.markdown('<div class="highlight-box">', unsafe_allow_html=True)
        st.markdown("""
        ### 🎯 Processing Results
        
        **Data Processed:**
        - 10,000 IoT sensor readings
        - 500 user interaction events  
        - 50 system alerts generated
        - Average processing latency: 45ms
        
        **Actions Taken:**
        - 12 real-time recommendations sent
        - 3 anomaly alerts triggered
        - Dashboard updated in real-time
        - ML model predictions generated
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code Example
    st.markdown("## 💻 Code Example: Real-time Data Processing")
    st.markdown('<div class="code-container">', unsafe_allow_html=True)
    st.code('''
import boto3
import json
import base64
from datetime import datetime

# Initialize AWS clients
kinesis = boto3.client('kinesis')
lambda_client = boto3.client('lambda')
emr = boto3.client('emr')

# 1. Stream data to Kinesis
def stream_to_kinesis():
    stream_name = 'sensor-data-stream'
    
    # Sample IoT sensor data
    sensor_data = {
        'device_id': 'sensor_001',
        'timestamp': datetime.utcnow().isoformat(),
        'temperature': 23.5,
        'humidity': 65.2,
        'location': {'lat': 37.7749, 'lon': -122.4194},
        'alert_level': 'normal',
        'metadata': {
            'battery_level': 85,
            'signal_strength': -45
        }
    }
    
    # Put record to Kinesis stream
    response = kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(sensor_data),
        PartitionKey=sensor_data['device_id']
    )
    
    print(f"Data sent to Kinesis: {response['SequenceNumber']}")

# 2. Lambda function for real-time processing
def lambda_handler(event, context):
    \"\"\"
    Lambda function triggered by Kinesis stream
    Processes records in real-time
    \"\"\"
    
    processed_records = []
    
    for record in event['Records']:
        # Decode Kinesis data
        payload = base64.b64decode(record['kinesis']['data'])
        sensor_data = json.loads(payload)
        
        # Real-time processing logic
        if sensor_data['temperature'] > 30:
            # Trigger high temperature alert
            send_alert({
                'type': 'temperature_alert',
                'device_id': sensor_data['device_id'],
                'temperature': sensor_data['temperature'],
                'timestamp': sensor_data['timestamp']
            })
        
        # Enrich data with additional information
        enriched_data = {
            **sensor_data,
            'processed_at': datetime.utcnow().isoformat(),
            'region': 'us-west-2',
            'risk_score': calculate_risk_score(sensor_data)
        }
        
        # Store processed data
        store_processed_data(enriched_data)
        processed_records.append(enriched_data)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed_records': len(processed_records),
            'message': 'Real-time processing completed'
        })
    }

def calculate_risk_score(data):
    \"\"\"Calculate risk score based on sensor readings\"\"\"
    risk_score = 0
    
    if data['temperature'] > 25:
        risk_score += 30
    if data['humidity'] > 70:
        risk_score += 20
    if data.get('metadata', {}).get('battery_level', 100) < 20:
        risk_score += 50
    
    return min(risk_score, 100)

def send_alert(alert_data):
    \"\"\"Send real-time alert via SNS\"\"\"
    sns = boto3.client('sns')
    
    sns.publish(
        TopicArn='arn:aws:sns:us-east-1:123456789:sensor-alerts',
        Message=json.dumps(alert_data),
        Subject=f"Sensor Alert: {alert_data['type']}"
    )

def store_processed_data(data):
    \"\"\"Store processed data in DynamoDB for real-time queries\"\"\"
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('sensor-readings')
    
    table.put_item(Item=data)

# 3. EMR batch processing for historical analysis
def run_emr_analysis():
    \"\"\"Run Spark job on EMR for batch analytics\"\"\"
    
    # Submit Spark job to EMR cluster
    response = emr.add_job_flow_steps(
        JobFlowId='j-1234567890123',  # Your EMR cluster ID
        Steps=[
            {
                'Name': 'Sensor Data Analysis',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--class', 'SensorDataAnalysis',
                        's3://my-bucket/spark-jobs/sensor-analysis.py',
                        '--input-path', 's3://my-bucket/sensor-data/',
                        '--output-path', 's3://my-bucket/analysis-results/'
                    ]
                }
            }
        ]
    )
    
    print(f"EMR job submitted: {response['StepIds'][0]}")

# 4. MSK producer for high-throughput streaming
def produce_to_kafka():
    \"\"\"Send high-volume data to MSK/Kafka\"\"\"
    from kafka import KafkaProducer
    
    producer = KafkaProducer(
        bootstrap_servers=['b-1.msk-cluster.kafka.us-east-1.amazonaws.com:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=16384,  # Batch for higher throughput
        linger_ms=10,      # Small delay for batching
        compression_type='gzip'
    )
    
    # Send high-frequency events
    for i in range(10000):
        event_data = {
            'event_id': f'event_{i}',
            'timestamp': datetime.utcnow().isoformat(),
            'user_id': f'user_{i % 1000}',
            'action': 'page_view',
            'metadata': {'page': f'/product/{i % 100}'}
        }
        
        producer.send('user-events', value=event_data)
    
    producer.flush()  # Ensure all messages are sent
    print("High-throughput data sent to Kafka")

# Example usage for real-time data pipeline
if __name__ == "__main__":
    # Real-time streaming
    stream_to_kinesis()
    
    # High-throughput messaging
    produce_to_kafka()
    
    # Batch processing for historical analysis
    run_emr_analysis()
    
    print("Real-time data velocity pipeline activated!")
    ''', language='python')
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
    # 📊 Five V's of Data
    ### Master the fundamental characteristics of Big Data and AWS solutions
    """)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 The 5 V's of Big Data",
        "🗃️ Data Source Types", 
        "📦 Volume Solutions",
        "🔄 Variety Solutions",
        "⚡ Velocity Solutions"
    ])
    
    with tab1:
        five_vs_tab()
    
    with tab2:
        data_sources_tab()
    
    with tab3:
        volume_solutions_tab()
    
    with tab4:
        variety_solutions_tab()
        
    with tab5:
        velocity_solutions_tab()
    
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
