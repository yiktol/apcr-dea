
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
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="AWS Data Quality Tools",
    page_icon="🧹",
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
    'warning': '#FFA500',
    'error': '#FF6B6B'
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
        
        .footer {{
            text-align: center;
            padding: 1rem;
            background-color: {AWS_COLORS['secondary']};
            color: white;
            margin-top: 1rem;
            border-radius: 8px;
        }}

        .code-container {{
            background-color: {AWS_COLORS['dark_blue']};
            color: white;
            padding: 20px;
            border-radius: 10px;
            border-left: 4px solid {AWS_COLORS['primary']};
            margin: 15px 0;
        }}
        
        .feature-card {{
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
        
        .quality-metric {{
            background: white;
            padding: 15px;
            border-radius: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin: 10px 0;
            text-align: center;
        }}
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    common.initialize_session_state()
    if 'data_quality_session' not in st.session_state:
        st.session_state.data_quality_session = True
        st.session_state.recipes_created = []
        st.session_state.datasets_profiled = []
        st.session_state.transformations_applied = []

def create_sidebar():
    """Create sidebar with app information and controls"""
    with st.sidebar:
        common.render_sidebar()
        
        # About section (collapsed by default)
        with st.expander("📖 About this App", expanded=False):
            st.markdown("""
            **Topics Covered:**
            - 🧹 AWS Glue DataBrew - Visual data preparation with 250+ transformations
            - 🔬 Amazon SageMaker Data Wrangler - ML-focused data preparation and analysis
            
            **Learning Objectives:**
            - Master visual data preparation techniques
            - Understand data quality assessment methods
            - Learn automated data transformation workflows
            - Explore ML-focused data preparation tools
            """)

def create_databrew_architecture():
    """Create mermaid diagram for Glue DataBrew architecture"""
    return """
    graph TB
        subgraph "Data Sources"
            S3[Amazon S3<br/>CSV, JSON, Parquet]
            RDS[Amazon RDS<br/>PostgreSQL, MySQL]
            REDSHIFT[Amazon Redshift<br/>Data Warehouse]
            FILES[File Upload<br/>Excel, CSV]
        end
        
        subgraph "AWS Glue DataBrew"
            PROFILE[Data Profiling<br/>📊 Statistical Analysis]
            VISUAL[Visual Interface<br/>🎨 Recipe Builder]
            TRANSFORM[250+ Transformations<br/>🔄 Clean & Transform]
            SCHEDULE[Job Scheduling<br/>⏰ Automation]
        end
        
        subgraph "Outputs"
            S3_OUT[Amazon S3<br/>Clean Data]
            ANALYTICS[Analytics Services<br/>Athena, Redshift]
            ML[Machine Learning<br/>SageMaker]
        end
        
        S3 --> PROFILE
        RDS --> PROFILE
        REDSHIFT --> PROFILE
        FILES --> PROFILE
        
        PROFILE --> VISUAL
        VISUAL --> TRANSFORM
        TRANSFORM --> SCHEDULE
        
        SCHEDULE --> S3_OUT
        S3_OUT --> ANALYTICS
        S3_OUT --> ML
        
        style PROFILE fill:#FF9900,stroke:#232F3E,color:#fff
        style VISUAL fill:#4B9EDB,stroke:#232F3E,color:#fff
        style TRANSFORM fill:#3FB34F,stroke:#232F3E,color:#fff
        style SCHEDULE fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_data_wrangler_architecture():
    """Create mermaid diagram for SageMaker Data Wrangler architecture"""
    return """
    graph TB
        subgraph "Data Sources"
            S3_DW[Amazon S3<br/>Data Lake]
            ATHENA[Amazon Athena<br/>Query Engine]
            REDSHIFT_DW[Amazon Redshift<br/>Data Warehouse]
            FEATURE_STORE[SageMaker<br/>Feature Store]
        end
        
        subgraph "SageMaker Data Wrangler"
            IMPORT[Data Import<br/>📥 Multiple Sources]
            EXPLORE[Data Exploration<br/>🔍 Visual Analysis]
            PREP[Data Preparation<br/>🛠️ Advanced Transforms]
            INSIGHTS[Built-in Analysis<br/>📊 ML Insights]
        end
        
        subgraph "ML Integration"
            PIPELINE[SageMaker Pipeline<br/>🚀 MLOps]
            TRAINING[Model Training<br/>🤖 AutoML]
            FEATURE_ENG[Feature Engineering<br/>⚙️ Advanced Processing]
        end
        
        subgraph "Outputs"
            PROCESSED[Processed Data<br/>Ready for ML]
            FLOWS[Data Flows<br/>Reusable Pipelines]
            REPORTS[Analysis Reports<br/>📋 Insights]
        end
        
        S3_DW --> IMPORT
        ATHENA --> IMPORT
        REDSHIFT_DW --> IMPORT
        FEATURE_STORE --> IMPORT
        
        IMPORT --> EXPLORE
        EXPLORE --> PREP
        PREP --> INSIGHTS
        
        INSIGHTS --> PIPELINE
        PIPELINE --> TRAINING
        PIPELINE --> FEATURE_ENG
        
        PREP --> PROCESSED
        PIPELINE --> FLOWS
        INSIGHTS --> REPORTS
        
        style IMPORT fill:#FF9900,stroke:#232F3E,color:#fff
        style EXPLORE fill:#4B9EDB,stroke:#232F3E,color:#fff
        style PREP fill:#3FB34F,stroke:#232F3E,color:#fff
        style INSIGHTS fill:#232F3E,stroke:#FF9900,color:#fff
    """

def create_data_quality_workflow():
    """Create mermaid diagram for data quality workflow"""
    return """
    graph LR
        START[Raw Data] --> PROFILE[Data Profiling<br/>📊 Analyze Structure]
        PROFILE --> ASSESS[Quality Assessment<br/>🔍 Identify Issues]
        ASSESS --> CLEAN[Data Cleaning<br/>🧹 Fix Issues]
        CLEAN --> VALIDATE[Validation<br/>✅ Verify Quality]
        VALIDATE --> MONITOR[Monitoring<br/>📈 Ongoing Quality]
        
        ASSESS --> RULES[Quality Rules<br/>📋 Define Standards]
        RULES --> CLEAN
        
        VALIDATE --> REJECT{Quality<br/>Threshold?}
        REJECT -->|Pass| MONITOR
        REJECT -->|Fail| CLEAN
        
        MONITOR --> ALERT[Alerts<br/>🚨 Quality Issues]
        ALERT --> CLEAN
        
        style PROFILE fill:#FF9900,stroke:#232F3E,color:#fff
        style ASSESS fill:#4B9EDB,stroke:#232F3E,color:#fff
        style CLEAN fill:#3FB34F,stroke:#232F3E,color:#fff
        style VALIDATE fill:#232F3E,stroke:#FF9900,color:#fff
    """

def generate_sample_data():
    """Generate sample dataset with quality issues"""
    np.random.seed(42)
    
    # Generate base data
    n_rows = 1000
    dates = pd.date_range(start='2024-01-01', end='2024-12-31', periods=n_rows)
    
    # Customer data with quality issues
    customers = []
    for i in range(n_rows):
        # Introduce various data quality issues
        customer_id = f"CUST_{i:04d}" if random.random() > 0.02 else None  # 2% missing IDs
        
        # Email with quality issues
        if random.random() > 0.05:  # 95% valid emails
            email = f"customer{i}@{'company' if random.random() > 0.3 else 'gmail'}.com"
        else:
            email = f"invalid_email_{i}"  # Invalid format
            
        # Phone with inconsistent formats
        phone_formats = [
            f"(555) {random.randint(100,999)}-{random.randint(1000,9999)}",
            f"555-{random.randint(100,999)}-{random.randint(1000,9999)}",
            f"555{random.randint(1000000,9999999)}",
            f"+1-555-{random.randint(100,999)}-{random.randint(1000,9999)}"
        ]
        phone = random.choice(phone_formats) if random.random() > 0.03 else None
        
        # Age with outliers
        if random.random() > 0.98:  # 2% outliers
            age = random.choice([0, -5, 150, 200])
        else:
            age = random.randint(18, 85)
            
        # Salary with missing values and outliers
        if random.random() > 0.07:  # 93% have salary
            if random.random() > 0.99:  # 1% extreme outliers
                salary = random.choice([1, 10000000, -1000])
            else:
                salary = random.randint(30000, 150000)
        else:
            salary = None
            
        # State with inconsistent values
        states = ['CA', 'California', 'NY', 'New York', 'TX', 'Texas', 'FL', 'Florida', '']
        state = random.choice(states) if random.random() > 0.05 else None
        
        customers.append({
            'customer_id': customer_id,
            'first_name': f"Customer_{i}" if random.random() > 0.01 else None,
            'last_name': f"Last_{i}" if random.random() > 0.01 else "",
            'email': email,
            'phone': phone,
            'age': age,
            'salary': salary,
            'state': state,
            'registration_date': dates[i],
            'last_purchase_amount': round(random.uniform(10, 500), 2) if random.random() > 0.1 else None
        })
    
    return pd.DataFrame(customers)

def analyze_data_quality(df):
    """Analyze data quality issues in the dataset"""
    quality_report = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'quality_score': 0,
        'issues': {}
    }
    
    total_cells = len(df) * len(df.columns)
    quality_issues = 0
    
    for column in df.columns:
        col_issues = {}
        
        # Check for missing values
        missing_count = df[column].isnull().sum()
        if missing_count > 0:
            col_issues['missing_values'] = {
                'count': int(missing_count),
                'percentage': round((missing_count / len(df)) * 100, 2)
            }
            quality_issues += missing_count
        
        # Check for empty strings
        if df[column].dtype == 'object':
            empty_count = (df[column] == '').sum()
            if empty_count > 0:
                col_issues['empty_strings'] = {
                    'count': int(empty_count),
                    'percentage': round((empty_count / len(df)) * 100, 2)
                }
                quality_issues += empty_count
        
        # Check for outliers (numeric columns)
        if df[column].dtype in ['int64', 'float64']:
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
            if len(outliers) > 0:
                col_issues['outliers'] = {
                    'count': len(outliers),
                    'percentage': round((len(outliers) / len(df)) * 100, 2)
                }
                quality_issues += len(outliers)
        
        # Check for format issues (email example)
        if column == 'email':
            import re
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            invalid_emails = df[~df[column].astype(str).str.match(email_pattern, na=False)]
            if len(invalid_emails) > 0:
                col_issues['invalid_format'] = {
                    'count': len(invalid_emails),
                    'percentage': round((len(invalid_emails) / len(df)) * 100, 2)
                }
                quality_issues += len(invalid_emails)
        
        if col_issues:
            quality_report['issues'][column] = col_issues
    
    # Calculate overall quality score
    quality_report['quality_score'] = max(0, round(((total_cells - quality_issues) / total_cells) * 100, 2))
    
    return quality_report

def glue_databrew_tab():
    """Content for AWS Glue DataBrew tab"""
    st.markdown("## 🧹 AWS Glue DataBrew")
    st.markdown("*Visual data preparation tool for cleaning and transforming data with 250+ built-in transformations*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Capabilities
    AWS Glue DataBrew simplifies data preparation with visual, point-and-click interface:
    - **Visual Interface**: No coding required - build recipes through UI
    - **250+ Transformations**: Built-in functions for cleaning, normalizing, and enriching data
    - **Data Profiling**: Automated analysis to understand data quality and structure
    - **Recipe Management**: Save, share, and reuse transformation workflows
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ DataBrew Architecture")
    common.mermaid(create_databrew_architecture(), height=1100)
    
    # Interactive data quality analyzer
    st.markdown("#### 🔍 Interactive Data Quality Analyzer")
    
    # Generate sample data
    sample_data = generate_sample_data()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("##### Sample Dataset with Quality Issues")
        
        # Display options
        display_rows = st.selectbox("Rows to display", [10, 25, 50, 100], index=1)
        st.dataframe(sample_data.head(display_rows), use_container_width=True)
        
    with col2:
        st.markdown("##### Quality Metrics")
        
        # Analyze quality
        quality_report = analyze_data_quality(sample_data)
        
        st.markdown('<div class="quality-metric">', unsafe_allow_html=True)
        st.metric("Overall Quality Score", f"{quality_report['quality_score']}%")
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="quality-metric">', unsafe_allow_html=True)
        st.metric("Total Records", f"{quality_report['total_rows']:,}")
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="quality-metric">', unsafe_allow_html=True)
        st.metric("Columns Analyzed", quality_report['total_columns'])
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Quality issues breakdown
    st.markdown("#### 📊 Data Quality Issues Breakdown")
    
    if quality_report['issues']:
        issue_data = []
        for column, issues in quality_report['issues'].items():
            for issue_type, details in issues.items():
                issue_data.append({
                    'Column': column,
                    'Issue Type': issue_type.replace('_', ' ').title(),
                    'Count': details['count'],
                    'Percentage': f"{details['percentage']}%"
                })
        
        issue_df = pd.DataFrame(issue_data)
        st.dataframe(issue_df, use_container_width=True)
        
        # Visualize issues
        fig = px.bar(
            issue_df.groupby('Issue Type')['Count'].sum().reset_index(),
            x='Issue Type',
            y='Count',
            title="Data Quality Issues by Type",
            color='Count',
            color_continuous_scale='Reds'
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    # Recipe builder simulator
    st.markdown("#### 🍳 Recipe Builder Simulator")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("##### Build Your Transformation Recipe")
        
        # Select transformation steps
        available_transforms = [
            "Remove missing values",
            "Standardize phone numbers", 
            "Validate email addresses",
            "Remove outliers",
            "Normalize state names",
            "Fill missing ages with median",
            "Convert text to uppercase",
            "Extract domain from email",
            "Calculate age groups",
            "Remove duplicate records"
        ]
        
        selected_transforms = st.multiselect(
            "Select Transformations",
            available_transforms,
            default=["Remove missing values", "Standardize phone numbers", "Validate email addresses"]
        )
        
        # Recipe parameters
        outlier_method = st.selectbox("Outlier Detection Method", ["IQR", "Z-Score", "Custom"])
        missing_value_strategy = st.selectbox("Missing Value Strategy", ["Remove", "Fill with median", "Fill with mode", "Fill with constant"])
        
    with col2:
        st.markdown("##### Recipe Preview")
        
        if selected_transforms:
            st.markdown('<div class="code-container">', unsafe_allow_html=True)
            recipe_steps = []
            for i, transform in enumerate(selected_transforms, 1):
                recipe_steps.append(f"Step {i}: {transform}")
            
            st.code('\n'.join(recipe_steps), language='text')
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Simulate quality improvement
            estimated_improvement = len(selected_transforms) * 5 + random.randint(5, 15)
            new_quality_score = min(100, quality_report['quality_score'] + estimated_improvement)
            
            st.markdown('<div class="success-box">', unsafe_allow_html=True)
            st.markdown(f"""
            **Estimated Quality Improvement**
            - Current Score: {quality_report['quality_score']}%
            - Projected Score: {new_quality_score}%
            - Improvement: +{estimated_improvement}%
            """)
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Transformation categories
    st.markdown("#### 🔧 DataBrew Transformation Categories")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🧹 Data Cleansing
        - Remove duplicates
        - Handle missing values
        - Fix formatting issues
        - Standardize text case
        - Remove whitespace
        - Validate data types
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🔄 Data Transformation
        - Split/merge columns
        - Extract patterns
        - Date/time parsing
        - Math operations
        - Statistical functions
        - Conditional logic
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Data Enrichment
        - Calculate new columns
        - Aggregate data
        - Join datasets
        - Lookup values
        - Generate categories
        - Create derived metrics
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Code examples
    st.markdown("#### 💻 DataBrew Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Recipe Creation", "Job Execution", "Monitoring & Automation"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# AWS Glue DataBrew Recipe Management using Python SDK
import boto3
import json
from datetime import datetime

# Initialize DataBrew client
databrew_client = boto3.client('databrew')

def create_databrew_dataset(dataset_name, s3_location):
    """Create a DataBrew dataset from S3 data"""
    
    try:
        response = databrew_client.create_dataset(
            Name=dataset_name,
            Format='CSV',  # CSV, JSON, PARQUET, EXCEL
            FormatOptions={
                'Csv': {
                    'Delimiter': ',',
                    'HeaderRow': True
                }
            },
            Input={
                'S3InputDefinition': {
                    'Bucket': s3_location.split('/')[2],
                    'Key': '/'.join(s3_location.split('/')[3:])
                }
            },
            Tags={
                'Environment': 'Production',
                'Project': 'DataQuality',
                'CreatedBy': 'DataTeam'
            }
        )
        print(f"✅ Dataset created: {dataset_name}")
        return response['Name']
        
    except Exception as e:
        print(f"❌ Error creating dataset: {e}")
        return None

def create_data_profile(dataset_name, profile_name):
    """Create a data profile to analyze data quality"""
    
    try:
        response = databrew_client.create_profile_job(
            DatasetName=dataset_name,
            Name=profile_name,
            RoleArn='arn:aws:iam::123456789012:role/DataBrewServiceRole',
            OutputLocation={
                'Bucket': 'my-databrew-results',
                'Key': f'profiles/{profile_name}/'
            },
            Configuration={
                'DatasetStatisticsConfiguration': {
                    'IncludedStatistics': [
                        'MIN', 'MAX', 'MEAN', 'MEDIAN', 'MODE',
                        'STANDARD_DEVIATION', 'NULL_COUNT', 'DISTINCT_COUNT'
                    ]
                },
                'ProfileColumns': [
                    {
                        'Name': 'customer_id',
                        'StatisticsConfiguration': {
                            'IncludedStatistics': ['DISTINCT_COUNT', 'NULL_COUNT']
                        }
                    },
                    {
                        'Name': 'email',
                        'StatisticsConfiguration': {
                            'IncludedStatistics': ['DISTINCT_COUNT', 'NULL_COUNT']
                        }
                    },
                    {
                        'Name': 'age',
                        'StatisticsConfiguration': {
                            'IncludedStatistics': ['MIN', 'MAX', 'MEAN', 'MEDIAN', 'OUTLIERS']
                        }
                    }
                ]
            },
            Tags={
                'Purpose': 'DataQualityAnalysis',
                'Schedule': 'Daily'
            }
        )
        print(f"✅ Profile job created: {profile_name}")
        return response['Name']
        
    except Exception as e:
        print(f"❌ Error creating profile: {e}")
        return None

def create_recipe_with_transformations(recipe_name):
    """Create a comprehensive data cleaning recipe"""
    
    # Define transformation steps
    recipe_steps = [
        {
            'Action': {
                'Operation': 'DELETE_DUPLICATES',
                'Parameters': {
                    'groupByAggFunctionOptions': 'ALL_COLUMNS'
                }
            }
        },
        {
            'Action': {
                'Operation': 'FILL_WITH_VALUE',
                'Parameters': {
                    'sourceColumn': 'first_name',
                    'value': 'Unknown'
                }
            },
            'ConditionExpressions': [
                {
                    'Condition': 'ISNULL(first_name)',
                    'Value': 'first_name'
                }
            ]
        },
        {
            'Action': {
                'Operation': 'STANDARDIZE_PHONE_NUMBER',
                'Parameters': {
                    'sourceColumn': 'phone',
                    'targetColumn': 'phone_standardized',
                    'phoneNumberFormat': 'US'
                }
            }
        },
        {
            'Action': {
                'Operation': 'EXTRACT_PATTERN',
                'Parameters': {
                    'sourceColumn': 'email',
                    'targetColumn': 'email_domain',
                    'pattern': '@(.+)$'
                }
            }
        },
        {
            'Action': {
                'Operation': 'REPLACE_VALUE_OR_PATTERN',
                'Parameters': {
                    'sourceColumn': 'state',
                    'value': 'California',
                    'targetValue': 'CA'
                }
            }
        },
        {
            'Action': {
                'Operation': 'REPLACE_VALUE_OR_PATTERN',
                'Parameters': {
                    'sourceColumn': 'state',
                    'value': 'New York',
                    'targetValue': 'NY'
                }
            }
        },
        {
            'Action': {
                'Operation': 'DELETE_ROWS_BY_CONDITION',
                'Parameters': {
                    'conditionExpression': 'age < 0 OR age > 120'
                }
            }
        },
        {
            'Action': {
                'Operation': 'CREATE_COLUMN',
                'Parameters': {
                    'targetColumn': 'age_group',
                    'expression': """
                    CASE 
                        WHEN age < 25 THEN 'Young Adult'
                        WHEN age < 45 THEN 'Adult'
                        WHEN age < 65 THEN 'Middle Age'
                        ELSE 'Senior'
                    END
                    """
                }
            }
        },
        {
            'Action': {
                'Operation': 'UPPER_CASE',
                'Parameters': {
                    'sourceColumn': 'last_name'
                }
            }
        },
        {
            'Action': {
                'Operation': 'VALIDATE_EMAIL',
                'Parameters': {
                    'sourceColumn': 'email',
                    'targetColumn': 'email_valid'
                }
            }
        }
    ]
    
    try:
        response = databrew_client.create_recipe(
            Name=recipe_name,
            Description='Comprehensive data cleaning and transformation recipe',
            Steps=recipe_steps,
            Tags={
                'RecipeType': 'DataCleaning',
                'Version': '1.0',
                'LastUpdated': datetime.now().isoformat()
            }
        )
        print(f"✅ Recipe created: {recipe_name}")
        return response['Name']
        
    except Exception as e:
        print(f"❌ Error creating recipe: {e}")
        return None

def create_project_and_job(project_name, dataset_name, recipe_name):
    """Create a DataBrew project and recipe job"""
    
    # Create project
    try:
        project_response = databrew_client.create_project(
            DatasetName=dataset_name,
            Name=project_name,
            RecipeName=recipe_name,
            RoleArn='arn:aws:iam::123456789012:role/DataBrewServiceRole',
            Tags={
                'Environment': 'Production',
                'Owner': 'DataEngineering'
            }
        )
        print(f"✅ Project created: {project_name}")
        
        # Create recipe job
        job_response = databrew_client.create_recipe_job(
            DatasetName=dataset_name,
            Name=f"{project_name}-job",
            ProjectName=project_name,
            RecipeReference={
                'Name': recipe_name
            },
            RoleArn='arn:aws:iam::123456789012:role/DataBrewServiceRole',
            Outputs=[
                {
                    'Location': {
                        'Bucket': 'my-databrew-results',
                        'Key': f'cleaned-data/{project_name}/'
                    },
                    'Format': 'PARQUET',
                    'Overwrite': True,
                    'PartitionColumns': ['state', 'age_group']
                }
            ],
            MaxCapacity=10,
            MaxRetries=3,
            Timeout=480,  # 8 hours
            Tags={
                'JobType': 'DataCleaning',
                'Schedule': 'Daily'
            }
        )
        print(f"✅ Recipe job created: {project_name}-job")
        
        return {
            'project_name': project_response['Name'],
            'job_name': job_response['Name']
        }
        
    except Exception as e:
        print(f"❌ Error creating project/job: {e}")
        return None

# Example usage
def main_databrew_setup():
    """Complete DataBrew setup workflow"""
    
    print("🚀 Starting DataBrew Setup Workflow")
    print("=" * 50)
    
    # Step 1: Create dataset
    dataset_name = "customer-data-raw"
    s3_location = "s3://my-data-bucket/raw/customer_data.csv"
    
    print("Step 1: Creating dataset...")
    dataset = create_databrew_dataset(dataset_name, s3_location)
    
    if dataset:
        # Step 2: Create data profile
        print("\\nStep 2: Creating data profile...")
        profile_name = f"{dataset_name}-profile"
        profile = create_data_profile(dataset_name, profile_name)
        
        # Step 3: Create recipe
        print("\\nStep 3: Creating transformation recipe...")
        recipe_name = "customer-data-cleaning-recipe"  
        recipe = create_recipe_with_transformations(recipe_name)
        
        if recipe:
            # Step 4: Create project and job
            print("\\nStep 4: Creating project and job...")
            project_name = "customer-data-cleaning-project"
            result = create_project_and_job(project_name, dataset_name, recipe_name)
            
            if result:
                print(f"\\n✅ DataBrew setup completed successfully!")
                print(f"Project: {result['project_name']}")
                print(f"Job: {result['job_name']}")
            else:
                print("❌ Failed to create project and job")
        else:
            print("❌ Failed to create recipe")
    else:
        print("❌ Failed to create dataset")

# Run the setup
if __name__ == "__main__":
    main_databrew_setup()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# DataBrew Job Execution and Management
import boto3
import time
import json
from datetime import datetime, timedelta

databrew_client = boto3.client('databrew')

def start_recipe_job(job_name):
    """Start a DataBrew recipe job execution"""
    
    try:
        response = databrew_client.start_job_run(
            Name=job_name
        )
        
        run_id = response['RunId']
        print(f"✅ Job started successfully")
        print(f"Job Name: {job_name}")
        print(f"Run ID: {run_id}")
        
        return run_id
        
    except Exception as e:
        print(f"❌ Error starting job: {e}")
        return None

def monitor_job_execution(job_name, run_id):
    """Monitor DataBrew job execution progress"""
    
    print(f"🔄 Monitoring job execution: {job_name}")
    print(f"Run ID: {run_id}")
    print("-" * 50)
    
    while True:
        try:
            response = databrew_client.describe_job_run(
                Name=job_name,
                RunId=run_id
            )
            
            job_run = response
            state = job_run['State']
            started_on = job_run.get('StartedOn', datetime.now())
            
            print(f"Status: {state}")
            print(f"Started: {started_on}")
            
            if 'CompletedOn' in job_run:
                completed_on = job_run['CompletedOn']
                duration = completed_on - started_on
                print(f"Completed: {completed_on}")
                print(f"Duration: {duration}")
            
            # Display progress information
            if 'JobSample' in job_run:
                sample_info = job_run['JobSample']
                if 'Size' in sample_info:
                    print(f"Sample Size: {sample_info['Size']} rows")
            
            # Check if job is finished
            if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                break
            elif state == 'FAILED':
                if 'ErrorMessage' in job_run:
                    print(f"❌ Error: {job_run['ErrorMessage']}")
                break
                
            # Wait before next check
            print("Checking again in 30 seconds...")
            time.sleep(30)
            
        except Exception as e:
            print(f"❌ Error monitoring job: {e}")
            break
    
    return state

def get_job_statistics(job_name, run_id):
    """Get detailed job execution statistics"""
    
    try:
        response = databrew_client.describe_job_run(
            Name=job_name,
            RunId=run_id
        )
        
        job_run = response
        
        stats = {
            'job_name': job_name,
            'run_id': run_id,
            'state': job_run['State'],
            'started_on': job_run.get('StartedOn'),
            'completed_on': job_run.get('CompletedOn'),
            'execution_time': None,
            'data_catalog_outputs': job_run.get('DataCatalogOutputs', []),
            'database_outputs': job_run.get('DatabaseOutputs', []),
            'outputs': job_run.get('Outputs', [])
        }
        
        # Calculate execution time
        if stats['started_on'] and stats['completed_on']:
            stats['execution_time'] = stats['completed_on'] - stats['started_on']
        
        # Get output information
        output_info = []
        for output in stats['outputs']:
            output_detail = {
                'location': output.get('Location', {}),
                'format': output.get('Format'),
                'partition_columns': output.get('PartitionColumns', []),
                'overwrite': output.get('Overwrite', False)
            }
            output_info.append(output_detail)
        
        stats['output_details'] = output_info
        
        return stats
        
    except Exception as e:
        print(f"❌ Error getting job statistics: {e}")
        return None

def create_scheduled_job(job_name, cron_expression):
    """Create a scheduled DataBrew job"""
    
    try:
        response = databrew_client.create_schedule(
            JobNames=[job_name],
            Name=f"{job_name}-schedule",
            CronExpression=cron_expression,  # e.g., "0 9 * * MON-FRI" for weekdays at 9 AM
            Tags={
                'ScheduleType': 'Automated',
                'CreatedBy': 'DataEngineering'
            }
        )
        
        print(f"✅ Schedule created for job: {job_name}")
        print(f"Schedule Name: {response['Name']}")
        print(f"Cron Expression: {cron_expression}")
        
        return response['Name']
        
    except Exception as e:
        print(f"❌ Error creating schedule: {e}")
        return None

def list_job_runs(job_name, max_results=10):
    """List recent job runs for a DataBrew job"""
    
    try:
        response = databrew_client.list_job_runs(
            Name=job_name,
            MaxResults=max_results
        )
        
        job_runs = response['JobRuns']
        
        print(f"📋 Recent Job Runs for: {job_name}")
        print("-" * 60)
        
        for run in job_runs:
            run_id = run['RunId']
            state = run['State']
            started_on = run.get('StartedOn', 'Unknown')
            completed_on = run.get('CompletedOn', 'Still running')
            
            print(f"Run ID: {run_id}")
            print(f"  State: {state}")
            print(f"  Started: {started_on}")
            print(f"  Completed: {completed_on}")
            
            if 'ErrorMessage' in run:
                print(f"  Error: {run['ErrorMessage']}")
            
            print()
        
        return job_runs
        
    except Exception as e:
        print(f"❌ Error listing job runs: {e}")
        return []

def batch_job_management():
    """Manage multiple DataBrew jobs in batch"""
    
    # List of jobs to manage
    jobs = [
        "customer-data-cleaning-job",
        "product-data-transformation-job",
        "sales-data-enrichment-job"
    ]
    
    job_results = {}
    
    print("🚀 Starting batch job execution")
    print("=" * 40)
    
    # Start all jobs
    for job_name in jobs:
        print(f"\\nStarting job: {job_name}")
        run_id = start_recipe_job(job_name)
        
        if run_id:
            job_results[job_name] = {
                'run_id': run_id,
                'status': 'RUNNING',
                'started_at': datetime.now()
            }
    
    # Monitor all jobs
    all_completed = False
    while not all_completed:
        all_completed = True
        
        print(f"\\n🔄 Checking job status at {datetime.now()}")
        print("-" * 50)
        
        for job_name, job_info in job_results.items():
            if job_info['status'] not in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                try:
                    response = databrew_client.describe_job_run(
                        Name=job_name,
                        RunId=job_info['run_id']
                    )
                    
                    current_status = response['State']
                    job_results[job_name]['status'] = current_status
                    
                    print(f"{job_name}: {current_status}")
                    
                    if current_status not in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                        all_completed = False
                    elif current_status == 'SUCCEEDED':
                        job_results[job_name]['completed_at'] = datetime.now()
                        duration = job_results[job_name]['completed_at'] - job_results[job_name]['started_at']
                        print(f"  ✅ Completed in {duration}")
                    elif current_status == 'FAILED':
                        print(f"  ❌ Failed: {response.get('ErrorMessage', 'Unknown error')}")
                        
                except Exception as e:
                    print(f"  ❌ Error checking {job_name}: {e}")
        
        if not all_completed:
            print("\\nWaiting 60 seconds before next check...")
            time.sleep(60)
    
    # Final summary
    print("\\n📊 BATCH JOB EXECUTION SUMMARY")
    print("=" * 50)
    
    succeeded = 0
    failed = 0
    
    for job_name, job_info in job_results.items():
        status = job_info['status']
        duration = "Unknown"
        
        if 'completed_at' in job_info and 'started_at' in job_info:
            duration = job_info['completed_at'] - job_info['started_at']
        
        print(f"{job_name}:")
        print(f"  Status: {status}")
        print(f"  Duration: {duration}")
        
        if status == 'SUCCEEDED':
            succeeded += 1
        elif status == 'FAILED':
            failed += 1
    
    print(f"\\nResults: {succeeded} succeeded, {failed} failed out of {len(jobs)} total jobs")
    
    return job_results

# Example usage
if __name__ == "__main__":
    # Single job execution
    job_name = "customer-data-cleaning-job"
    run_id = start_recipe_job(job_name)
    
    if run_id:
        final_status = monitor_job_execution(job_name, run_id)
        
        if final_status == 'SUCCEEDED':
            stats = get_job_statistics(job_name, run_id)
            print(f"\\n📊 Job Statistics:")
            print(json.dumps(stats, indent=2, default=str))
            
            # Create schedule for future runs
            cron_expr = "0 6 * * *"  # Daily at 6 AM
            create_scheduled_job(job_name, cron_expr)
    
    # Batch job management
    batch_results = batch_job_management()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# DataBrew Monitoring, Automation, and Integration
import boto3
import json
from datetime import datetime, timedelta

databrew_client = boto3.client('databrew')
cloudwatch = boto3.client('cloudwatch')
sns = boto3.client('sns')

def setup_databrew_monitoring():
    """Set up comprehensive monitoring for DataBrew jobs"""
    
    # Create CloudWatch custom metrics
    def publish_job_metrics(job_name, run_id, status, duration_seconds):
        """Publish custom metrics to CloudWatch"""
        
        try:
            # Job execution duration
            cloudwatch.put_metric_data(
                Namespace='AWS/DataBrew/Custom',
                MetricData=[
                    {
                        'MetricName': 'JobExecutionDuration',
                        'Dimensions': [
                            {'Name': 'JobName', 'Value': job_name}
                        ],
                        'Value': duration_seconds,
                        'Unit': 'Seconds',
                        'Timestamp': datetime.now()
                    },
                    {
                        'MetricName': 'JobExecutionStatus',
                        'Dimensions': [
                            {'Name': 'JobName', 'Value': job_name},
                            {'Name': 'Status', 'Value': status}
                        ],
                        'Value': 1,
                        'Unit': 'Count',
                        'Timestamp': datetime.now()
                    }
                ]
            )
            print(f"✅ Metrics published for job: {job_name}")
            
        except Exception as e:
            print(f"❌ Error publishing metrics: {e}")
    
    return publish_job_metrics

def create_databrew_alarms():
    """Create CloudWatch alarms for DataBrew job monitoring"""
    
    alarms = [
        {
            'AlarmName': 'DataBrew-Job-Failure-Rate',
            'AlarmDescription': 'Alert when DataBrew job failure rate is high',
            'MetricName': 'JobExecutionStatus',
            'Namespace': 'AWS/DataBrew/Custom',
            'Statistic': 'Sum',
            'Period': 3600,  # 1 hour
            'EvaluationPeriods': 1,
            'Threshold': 2,  # More than 2 failures per hour
            'ComparisonOperator': 'GreaterThanThreshold',
            'Dimensions': [
                {'Name': 'Status', 'Value': 'FAILED'}
            ]
        },
        {
            'AlarmName': 'DataBrew-Job-Duration-High',
            'AlarmDescription': 'Alert when DataBrew job takes too long',
            'MetricName': 'JobExecutionDuration',
            'Namespace': 'AWS/DataBrew/Custom',
            'Statistic': 'Average',
            'Period': 900,  # 15 minutes
            'EvaluationPeriods': 2,
            'Threshold': 7200,  # 2 hours in seconds
            'ComparisonOperator': 'GreaterThanThreshold'
        }
    ]
    
    for alarm_config in alarms:
        try:
            cloudwatch.put_metric_alarm(**alarm_config)
            print(f"✅ Alarm created: {alarm_config['AlarmName']}")
        except Exception as e:
            print(f"❌ Error creating alarm {alarm_config['AlarmName']}: {e}")

def setup_sns_notifications(topic_arn):
    """Set up SNS notifications for DataBrew job events"""
    
    def send_job_notification(job_name, status, run_id, details=None):
        """Send notification about job status"""
        
        subject = f"DataBrew Job {status}: {job_name}"
        
        message_body = {
            "job_name": job_name,
            "status": status,
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(),
            "details": details or {}
        }
        
        message = f"""
DataBrew Job Status Update

Job Name: {job_name}
Status: {status}
Run ID: {run_id}
Timestamp: {datetime.now().isoformat()}

Details:
{json.dumps(details, indent=2, default=str) if details else 'No additional details'}

This is an automated notification from AWS DataBrew job monitoring.
        """
        
        try:
            sns.publish(
                TopicArn=topic_arn,
                Subject=subject,
                Message=message
            )
            print(f"📧 Notification sent for job: {job_name} ({status})")
            
        except Exception as e:
            print(f"❌ Error sending notification: {e}")
    
    return send_job_notification

def create_data_quality_dashboard():
    """Create CloudWatch dashboard for DataBrew data quality monitoring"""
    
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "x": 0, "y": 0,
                "width": 12, "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/DataBrew/Custom", "JobExecutionDuration", "JobName", "customer-data-cleaning-job"],
                        ["AWS/DataBrew/Custom", "JobExecutionDuration", "JobName", "product-data-transformation-job"],
                        ["AWS/DataBrew/Custom", "JobExecutionDuration", "JobName", "sales-data-enrichment-job"]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-west-2",
                    "title": "DataBrew Job Execution Duration",
                    "yAxis": {
                        "left": {
                            "min": 0
                        }
                    }
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 0,
                "width": 12, "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/DataBrew/Custom", "JobExecutionStatus", "Status", "SUCCEEDED"],
                        ["AWS/DataBrew/Custom", "JobExecutionStatus", "Status", "FAILED"]
                    ],
                    "period": 3600,
                    "stat": "Sum",
                    "region": "us-west-2",
                    "title": "DataBrew Job Success/Failure Rate"
                }
            },
            {
                "type": "log",
                "x": 0, "y": 6,
                "width": 24, "height": 6,
                "properties": {
                    "query": """SOURCE '/aws/databrew/jobs'
| fields @timestamp, @message
| filter @message like /ERROR/
| sort @timestamp desc
| limit 100""",
                    "region": "us-west-2",
                    "title": "Recent DataBrew Job Errors",
                    "view": "table"
                }
            }
        ]
    }
    
    try:
        cloudwatch.put_dashboard(
            DashboardName='DataBrew-DataQuality-Dashboard',
            DashboardBody=json.dumps(dashboard_body)
        )
        print("✅ CloudWatch dashboard created: DataBrew-DataQuality-Dashboard")
        
    except Exception as e:
        print(f"❌ Error creating dashboard: {e}")

def automated_data_quality_pipeline():
    """Create an automated data quality pipeline with monitoring"""
    
    class DataQualityPipeline:
        def __init__(self, config):
            self.config = config
            self.publish_metrics = setup_databrew_monitoring()
            self.send_notification = setup_sns_notifications(config['sns_topic_arn'])
            
        def execute_quality_pipeline(self, dataset_name):
            """Execute the complete data quality pipeline"""
            
            pipeline_steps = [
                {"name": "profile-job", "type": "profile"},
                {"name": "cleaning-job", "type": "recipe"},
                {"name": "validation-job", "type": "recipe"}
            ]
            
            results = {}
            
            for step in pipeline_steps:
                step_name = f"{dataset_name}-{step['name']}"
                
                print(f"🚀 Executing step: {step_name}")
                
                if step['type'] == 'profile':
                    # Start profile job
                    try:
                        response = databrew_client.start_job_run(Name=step_name)
                        run_id = response['RunId']
                        
                        # Monitor profile job  
                        status = self.monitor_job_with_notifications(step_name, run_id)
                        results[step_name] = {'status': status, 'run_id': run_id}
                        
                    except Exception as e:
                        print(f"❌ Error in profile step: {e}")
                        results[step_name] = {'status': 'ERROR', 'error': str(e)}
                        
                elif step['type'] == 'recipe':
                    # Start recipe job
                    try:
                        response = databrew_client.start_job_run(Name=step_name)
                        run_id = response['RunId']
                        
                        # Monitor recipe job
                        status = self.monitor_job_with_notifications(step_name, run_id)
                        results[step_name] = {'status': status, 'run_id': run_id}
                        
                    except Exception as e:
                        print(f"❌ Error in recipe step: {e}")
                        results[step_name] = {'status': 'ERROR', 'error': str(e)}
                
                # Stop pipeline if any step fails
                if results[step_name]['status'] in ['FAILED', 'ERROR']:
                    print(f"❌ Pipeline stopped due to failure in: {step_name}")
                    break
            
            return results
        
        def monitor_job_with_notifications(self, job_name, run_id):
            """Monitor job with automated notifications"""
            
            start_time = datetime.now()
            
            while True:
                try:
                    response = databrew_client.describe_job_run(
                        Name=job_name,
                        RunId=run_id
                    )
                    
                    status = response['State']
                    
                    if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                        end_time = datetime.now()
                        duration = (end_time - start_time).total_seconds()
                        
                        # Publish metrics
                        self.publish_metrics(job_name, run_id, status, duration)
                        
                        # Send notification
                        job_details = {
                            'duration_seconds': duration,
                            'start_time': start_time.isoformat(),
                            'end_time': end_time.isoformat()
                        }
                        
                        if status == 'FAILED':
                            job_details['error_message'] = response.get('ErrorMessage', 'Unknown error')
                        
                        self.send_notification(job_name, status, run_id, job_details)
                        
                        return status
                    
                    time.sleep(30)  # Check every 30 seconds
                    
                except Exception as e:
                    print(f"❌ Error monitoring job: {e}")
                    return 'ERROR'
        
        def generate_quality_report(self, pipeline_results):
            """Generate comprehensive quality report"""
            
            report = {
                'pipeline_execution_time': datetime.now().isoformat(),
                'overall_status': 'SUCCESS',
                'step_results': pipeline_results,
                'quality_metrics': {},
                'recommendations': []
            }
            
            # Determine overall status
            for step_name, result in pipeline_results.items():
                if result['status'] in ['FAILED', 'ERROR']:
                    report['overall_status'] = 'FAILED'
                    break
            
            # Add quality metrics (would be retrieved from actual profile results)
            report['quality_metrics'] = {
                'data_completeness': 95.5,
                'data_validity': 92.3,
                'data_consistency': 98.1,
                'data_accuracy': 94.7,
                'overall_quality_score': 95.2
            }
            
            # Generate recommendations
            if report['overall_status'] == 'SUCCESS':
                if report['quality_metrics']['data_validity'] < 95:
                    report['recommendations'].append("Consider adding more validation rules for email and phone formats")
                if report['quality_metrics']['data_completeness'] < 98:
                    report['recommendations'].append("Investigate sources of missing data and implement filling strategies")
            
            return report
    
    # Configuration
    config = {
        'sns_topic_arn': 'arn:aws:sns:us-west-2:123456789012:databrew-notifications',
        'cloudwatch_log_group': '/aws/databrew/jobs',
        'notification_email': 'data-team@company.com'
    }
    
    # Initialize pipeline
    pipeline = DataQualityPipeline(config)
    
    # Execute pipeline
    dataset_name = "customer-data"
    results = pipeline.execute_quality_pipeline(dataset_name)
    
    # Generate final report
    quality_report = pipeline.generate_quality_report(results)
    
    print("\\n📊 DATA QUALITY PIPELINE REPORT")
    print("=" * 50)
    print(json.dumps(quality_report, indent=2, default=str))
    
    return quality_report

# Main execution
if __name__ == "__main__":
    print("🔧 Setting up DataBrew monitoring and automation...")
    
    # Setup monitoring components
    create_databrew_alarms()
    create_data_quality_dashboard()
    
    # Execute automated pipeline
    report = automated_data_quality_pipeline()
    
    print("\\n✅ DataBrew monitoring and automation setup complete!")
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def sagemaker_data_wrangler_tab():
    """Content for SageMaker Data Wrangler tab"""
    st.markdown("## 🔬 Amazon SageMaker Data Wrangler")
    st.markdown("*ML-focused data preparation with advanced analytics and seamless SageMaker integration*")
    
    # Key concept
    st.markdown('<div class="concept-card">', unsafe_allow_html=True)
    st.markdown("""
    ### 🎯 Key Capabilities
    SageMaker Data Wrangler is designed specifically for machine learning workflows:
    - **ML-Focused Preparation**: Transformations optimized for ML feature engineering
    - **Advanced Analytics**: Built-in statistical analysis and ML insights  
    - **Visual Data Exploration**: Interactive charts and statistical summaries
    - **SageMaker Integration**: Seamless integration with training, pipelines, and Feature Store
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Architecture diagram
    st.markdown("#### 🏗️ Data Wrangler Architecture")
    common.mermaid(create_data_wrangler_architecture(), height=1100)
    
    # Data Wrangler vs DataBrew comparison
    st.markdown("#### ⚖️ Data Wrangler vs DataBrew Comparison")
    
    comparison_data = {
        'Feature': [
            'Primary Use Case',
            'Target Users',
            'Integration Focus',
            'Transformations',
            'Code Generation',
            'Built-in Analytics',
            'ML Insights',
            'Cost Model',
            'Scalability'
        ],
        'Data Wrangler': [
            'ML Feature Engineering',
            'Data Scientists, ML Engineers',
            'SageMaker Ecosystem',
            'ML-optimized transforms',
            'Python/PySpark code export',
            'Advanced statistical analysis',
            'Feature importance, bias detection',
            'Studio instance hours',
            'Scales with SageMaker'
        ],
        'DataBrew': [
            'General Data Preparation',
            'Data Analysts, Engineers',
            'AWS Analytics Services',
            '250+ general transforms',
            'Visual recipe builder',
            'Basic profiling',
            'Limited ML focus',
            'Job-based pricing',
            'Serverless scaling'
        ]
    }
    
    comparison_df = pd.DataFrame(comparison_data)
    st.dataframe(comparison_df, use_container_width=True)
    
    # Interactive feature engineering simulator
    st.markdown("#### 🛠️ Feature Engineering Simulator")
    
    # Generate ML-focused sample data
    ml_sample_data = create_ml_sample_data()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("##### ML Dataset Sample")
        st.dataframe(ml_sample_data.head(20), use_container_width=True)
    
    with col2:
        st.markdown("##### Feature Engineering Options")
        
        # Feature engineering selections
        encoding_options = st.multiselect(
            "Categorical Encoding",
            ["One-Hot Encoding", "Target Encoding", "Ordinal Encoding", "Binary Encoding"],
            default=["One-Hot Encoding"]
        )
        
        scaling_options = st.selectbox(
            "Numerical Scaling",
            ["None", "StandardScaler", "MinMaxScaler", "RobustScaler", "Normalizer"]
        )
        
        feature_creation = st.multiselect(
            "Feature Creation",
            ["Polynomial Features", "Date/Time Features", "Text Features", "Interaction Terms"],
            default=["Date/Time Features"]
        )
        
        # Simulate feature engineering results
        if st.button("Apply Feature Engineering"):
            engineered_features = apply_feature_engineering(
                ml_sample_data, encoding_options, scaling_options, feature_creation
            )
            
            st.markdown('<div class="success-box">', unsafe_allow_html=True)
            st.markdown(f"""
            **Feature Engineering Results:**
            - Original Features: {len(ml_sample_data.columns)}
            - New Features: {len(engineered_features.columns)}
            - Feature Increase: +{len(engineered_features.columns) - len(ml_sample_data.columns)}
            """)
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Show sample of engineered data
            st.markdown("##### Sample of Engineered Features")
            st.dataframe(engineered_features.head(10), use_container_width=True)
    
    # Data quality workflow
    st.markdown("#### 📊 Data Quality Workflow")
    common.mermaid(create_data_quality_workflow(), height=400)
    
    # Built-in analysis capabilities
    st.markdown("#### 📈 Built-in Analysis Capabilities")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📊 Statistical Analysis
        - **Descriptive Statistics**
        - **Distribution Analysis** 
        - **Correlation Matrix**
        - **Missing Value Analysis**
        - **Outlier Detection**
        - **Data Quality Report**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 🤖 ML Insights
        - **Feature Importance**
        - **Target Leakage Detection**
        - **Bias Detection**
        - **Model Explainability**
        - **Feature Correlation**
        - **Quick Model Assessment**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="feature-card">', unsafe_allow_html=True)
        st.markdown("""
        ### 📸 Visual Analysis
        - **Histogram Analysis**
        - **Scatter Plot Matrix**
        - **Box Plot Analysis**
        - **Time Series Plots**
        - **Correlation Heatmaps**
        - **Custom Visualizations**
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Create sample visualizations
    st.markdown("#### 📊 Sample Data Analysis")
    
    # Create sample analysis charts
    create_sample_analysis_charts(ml_sample_data)
    
    # Code examples
    st.markdown("#### 💻 Data Wrangler Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Data Import & Exploration", "Feature Engineering", "ML Pipeline Integration"])
    
    with tab1:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# SageMaker Data Wrangler: Data Import and Exploration
import pandas as pd
import numpy as np
import sagemaker
from sagemaker import get_execution_role
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.sklearn.processing import SKLearnProcessor
import boto3

# Initialize SageMaker session
sagemaker_session = sagemaker.Session()
role = get_execution_role()
region = boto3.Session().region_name

def setup_data_wrangler_flow():
    """Set up a Data Wrangler flow programmatically"""
    
    # This represents the configuration that would be set up in Studio
    flow_config = {
        "flow_name": "customer-churn-analysis",
        "flow_description": "Prepare data for customer churn prediction model",
        "data_sources": [
            {
                "name": "customer_data",
                "type": "s3",
                "location": "s3://my-ml-bucket/raw-data/customers.csv",
                "format": "csv"
            },
            {
                "name": "transaction_data", 
                "type": "redshift",
                "connection": {
                    "cluster_id": "ml-redshift-cluster",
                    "database": "analytics",
                    "query": "SELECT * FROM transactions WHERE date >= '2024-01-01'"
                }
            },
            {
                "name": "support_tickets",
                "type": "athena",
                "database": "support_db",
                "table": "tickets",
                "query": "SELECT customer_id, COUNT(*) as ticket_count FROM tickets GROUP BY customer_id"
            }
        ]
    }
    
    return flow_config

def create_data_exploration_analysis():
    """Create comprehensive data exploration analysis"""
    
    # Simulate loading data (in Data Wrangler, this would be visual)
    np.random.seed(42)
    n_customers = 5000
    
    # Create realistic customer dataset
    customer_data = pd.DataFrame({
        'customer_id': [f'CUST_{i:05d}' for i in range(n_customers)],
        'age': np.random.normal(35, 12, n_customers).clip(18, 80).astype(int),
        'tenure_months': np.random.exponential(24, n_customers).clip(1, 120).astype(int),
        'monthly_charges': np.random.normal(75, 25, n_customers).clip(20, 200),
        'total_charges': lambda x: x['monthly_charges'] * x['tenure_months'] + np.random.normal(0, 100, n_customers),
        'contract_type': np.random.choice(['Month-to-month', 'One year', 'Two year'], 
                                        n_customers, p=[0.5, 0.3, 0.2]),
        'payment_method': np.random.choice(['Electronic check', 'Mailed check', 'Bank transfer', 'Credit card'],
                                         n_customers, p=[0.4, 0.2, 0.2, 0.2]),
        'internet_service': np.random.choice(['DSL', 'Fiber optic', 'No'], 
                                           n_customers, p=[0.4, 0.4, 0.2]),
        'senior_citizen': np.random.choice([0, 1], n_customers, p=[0.84, 0.16]),
        'partner': np.random.choice(['Yes', 'No'], n_customers, p=[0.52, 0.48]),
        'dependents': np.random.choice(['Yes', 'No'], n_customers, p=[0.3, 0.7])
    })
    
    # Add churn target variable with realistic relationships
    churn_probability = (
        0.05 +  # Base churn rate
        (customer_data['contract_type'] == 'Month-to-month') * 0.15 +
        (customer_data['tenure_months'] < 12) * 0.1 +
        (customer_data['monthly_charges'] > 100) * 0.05 +
        (customer_data['payment_method'] == 'Electronic check') * 0.08 +
        (customer_data['senior_citizen'] == 1) * 0.03
    )
    customer_data['churn'] = np.random.binomial(1, churn_probability)
    
    return customer_data

def perform_data_quality_analysis(df):
    """Perform comprehensive data quality analysis"""
    
    quality_analysis = {
        'dataset_overview': {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'numeric_columns': len(df.select_dtypes(include=[np.number]).columns),
            'categorical_columns': len(df.select_dtypes(include=['object']).columns)
        },
        'missing_values': {},
        'data_types': {},
        'unique_values': {},
        'statistical_summary': {},
        'potential_issues': []
    }
    
    # Analyze each column
    for column in df.columns:
        col_data = df[column]
        
        # Missing values
        missing_count = col_data.isnull().sum()
        quality_analysis['missing_values'][column] = {
            'count': int(missing_count),
            'percentage': round((missing_count / len(df)) * 100, 2)
        }
        
        # Data types
        quality_analysis['data_types'][column] = str(col_data.dtype)
        
        # Unique values
        unique_count = col_data.nunique()
        quality_analysis['unique_values'][column] = {
            'count': int(unique_count),
            'percentage': round((unique_count / len(df)) * 100, 2)
        }
        
        # Statistical summary for numeric columns
        if col_data.dtype in ['int64', 'float64']:
            quality_analysis['statistical_summary'][column] = {
                'mean': round(col_data.mean(), 2),
                'median': round(col_data.median(), 2),
                'std': round(col_data.std(), 2),
                'min': round(col_data.min(), 2),
                'max': round(col_data.max(), 2),
                'q25': round(col_data.quantile(0.25), 2),
                'q75': round(col_data.quantile(0.75), 2),
                'skewness': round(col_data.skew(), 2),
                'kurtosis': round(col_data.kurtosis(), 2)
            }
            
            # Check for outliers
            Q1 = col_data.quantile(0.25)
            Q3 = col_data.quantile(0.75)
            IQR = Q3 - Q1
            outlier_count = len(col_data[(col_data < Q1 - 1.5*IQR) | (col_data > Q3 + 1.5*IQR)])
            
            if outlier_count > 0:
                quality_analysis['potential_issues'].append({
                    'column': column,
                    'issue': 'outliers_detected',
                    'count': outlier_count,
                    'description': f'{outlier_count} potential outliers detected using IQR method'
                })
        
        # Check for high cardinality categorical variables
        if col_data.dtype == 'object' and unique_count > len(df) * 0.1:
            quality_analysis['potential_issues'].append({
                'column': column,
                'issue': 'high_cardinality',
                'count': unique_count,
                'description': f'High cardinality categorical variable ({unique_count} unique values)'
            })
        
        # Check for potential ID columns
        if unique_count == len(df) and col_data.dtype == 'object':
            quality_analysis['potential_issues'].append({
                'column': column,
                'issue': 'potential_id_column',
                'description': 'Column has unique values for every row - likely an ID column'
            })
    
    return quality_analysis

def create_data_wrangler_transformations():
    """Define Data Wrangler transformations for ML preprocessing"""
    
    transformations = {
        "data_cleaning": [
            {
                "name": "Handle Missing Values",
                "type": "impute",
                "parameters": {
                    "strategy": "most_frequent",
                    "columns": ["contract_type", "payment_method", "internet_service"]
                }
            },
            {
                "name": "Remove Outliers",
                "type": "outlier_removal",
                "parameters": {
                    "method": "iqr",
                    "columns": ["monthly_charges", "total_charges"],
                    "multiplier": 1.5
                }
            }
        ],
        "feature_engineering": [
            {
                "name": "One-Hot Encode Categorical",
                "type": "encode_categorical",
                "parameters": {
                    "encoding_type": "one_hot",
                    "columns": ["contract_type", "payment_method", "internet_service"],
                    "drop_first": True
                }
            },
            {
                "name": "Create Tenure Groups",
                "type": "create_bins",
                "parameters": {
                    "column": "tenure_months",
                    "bins": [0, 12, 24, 48, 120],
                    "labels": ["New", "Short", "Medium", "Long"],
                    "new_column": "tenure_group"
                }
            },
            {
                "name": "Calculate Charges Ratio",
                "type": "calculate",
                "parameters": {
                    "formula": "total_charges / (monthly_charges * tenure_months)",
                    "new_column": "charges_efficiency"
                }
            }
        ],
        "feature_scaling": [
            {
                "name": "Standard Scale Numeric",
                "type": "scale",
                "parameters": {
                    "scaler_type": "standard",
                    "columns": ["age", "monthly_charges", "total_charges", "charges_efficiency"]
                }
            }
        ]
    }
    
    return transformations

def generate_bias_report(df, target_column, sensitive_features):
    """Generate bias analysis report for ML fairness"""
    
    bias_report = {
        'analysis_date': datetime.now().isoformat(),
        'dataset_size': len(df),
        'target_column': target_column,
        'sensitive_features': sensitive_features,
        'bias_metrics': {}
    }
    
    for feature in sensitive_features:
        if feature in df.columns:
            # Calculate group-level statistics
            group_stats = df.groupby(feature)[target_column].agg(['count', 'mean', 'std']).round(3)
            
            # Calculate disparate impact
            group_rates = df.groupby(feature)[target_column].mean()
            max_rate = group_rates.max()
            min_rate = group_rates.min()
            disparate_impact = min_rate / max_rate if max_rate > 0 else 0
            
            bias_report['bias_metrics'][feature] = {
                'group_statistics': group_stats.to_dict(),
                'disparate_impact_ratio': round(disparate_impact, 3),
                'bias_detected': disparate_impact < 0.8,  # 80% rule
                'recommendation': 'Monitor for bias' if disparate_impact < 0.8 else 'No immediate bias concerns'
            }
    
    return bias_report

# Example usage
def main_data_exploration():
    """Main function for data exploration workflow"""
    
    print("🔍 Starting Data Wrangler Exploration Workflow")
    print("=" * 60)
    
    # Setup flow
    flow_config = setup_data_wrangler_flow()
    print(f"✅ Flow configured: {flow_config['flow_name']}")
    
    # Create and analyze sample data
    print("\\n📊 Creating sample dataset...")
    sample_data = create_data_exploration_analysis()
    print(f"Dataset created: {len(sample_data)} rows, {len(sample_data.columns)} columns")
    
    # Perform quality analysis
    print("\\n🔍 Performing data quality analysis...")
    quality_report = perform_data_quality_analysis(sample_data)
    
    print(f"\\nDataset Overview:")
    for key, value in quality_report['dataset_overview'].items():
        print(f"  {key}: {value}")
    
    print(f"\\nPotential Issues Found: {len(quality_report['potential_issues'])}")
    for issue in quality_report['potential_issues'][:3]:  # Show first 3 issues
        print(f"  - {issue['column']}: {issue['description']}")
    
    # Generate bias report
    print("\\n⚖️ Generating bias analysis...")
    bias_report = generate_bias_report(
        sample_data, 
        'churn', 
        ['senior_citizen', 'contract_type', 'payment_method']
    )
    
    print("Bias Analysis Results:")
    for feature, metrics in bias_report['bias_metrics'].items():
        print(f"  {feature}: Disparate Impact = {metrics['disparate_impact_ratio']} "
              f"({'⚠️ Bias Detected' if metrics['bias_detected'] else '✅ No Bias'})")
    
    # Define transformations
    print("\\n🛠️ Defining transformations...")
    transformations = create_data_wrangler_transformations()
    
    total_transforms = sum(len(category) for category in transformations.values())
    print(f"Total transformations defined: {total_transforms}")
    for category, transforms in transformations.items():
        print(f"  {category}: {len(transforms)} transformations")
    
    print("\\n✅ Data exploration workflow completed!")
    
    return {
        'sample_data': sample_data,
        'quality_report': quality_report,
        'bias_report': bias_report,
        'transformations': transformations
    }

if __name__ == "__main__":
    results = main_data_exploration()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# SageMaker Data Wrangler: Advanced Feature Engineering
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
from sklearn.feature_selection import SelectKBest, chi2, f_classif
from datetime import datetime, timedelta
import boto3
import sagemaker
from sagemaker.feature_store.feature_group import FeatureGroup

def advanced_feature_engineering(df, target_column):
    """Perform advanced feature engineering optimized for ML"""
    
    print("🛠️ Starting Advanced Feature Engineering")
    print("=" * 50)
    
    # Create a copy for transformation
    df_transformed = df.copy()
    original_features = len(df_transformed.columns)
    
    # 1. Temporal Feature Engineering
    print("\\n📅 Creating temporal features...")
    if 'registration_date' in df_transformed.columns:
        df_transformed['registration_date'] = pd.to_datetime(df_transformed['registration_date'])
        df_transformed['days_since_registration'] = (datetime.now() - df_transformed['registration_date']).dt.days
        df_transformed['registration_year'] = df_transformed['registration_date'].dt.year
        df_transformed['registration_month'] = df_transformed['registration_date'].dt.month
        df_transformed['registration_quarter'] = df_transformed['registration_date'].dt.quarter
        df_transformed['registration_weekday'] = df_transformed['registration_date'].dt.weekday
        df_transformed['is_weekend_registration'] = (df_transformed['registration_weekday'] >= 5).astype(int)
    
    # 2. Numerical Feature Transformations
    print("🔢 Engineering numerical features...")
    
    # Log transformations for skewed data
    skewed_columns = ['monthly_charges', 'total_charges']
    for col in skewed_columns:
        if col in df_transformed.columns:
            df_transformed[f'{col}_log'] = np.log1p(df_transformed[col])
            df_transformed[f'{col}_sqrt'] = np.sqrt(df_transformed[col])
    
    # Create ratio features
    if 'total_charges' in df_transformed.columns and 'monthly_charges' in df_transformed.columns:
        df_transformed['avg_monthly_spend'] = df_transformed['total_charges'] / df_transformed['tenure_months']
        df_transformed['charges_growth_rate'] = (
            (df_transformed['monthly_charges'] - df_transformed['avg_monthly_spend']) / 
            df_transformed['avg_monthly_spend']
        ).fillna(0)
    
    # 3. Categorical Feature Engineering
    print("📊 Engineering categorical features...")
    
    # Create frequency encodings
    categorical_columns = df_transformed.select_dtypes(include=['object']).columns
    for col in categorical_columns:
        if col != target_column:
            freq_map = df_transformed[col].value_counts().to_dict()
            df_transformed[f'{col}_frequency'] = df_transformed[col].map(freq_map)
    
    # Create target encodings (for supervised learning)
    if target_column in df_transformed.columns:
        for col in categorical_columns:
            if col != target_column and df_transformed[col].nunique() > 2:
                target_mean = df_transformed.groupby(col)[target_column].mean()
                df_transformed[f'{col}_target_encoded'] = df_transformed[col].map(target_mean)
    
    # 4. Interaction Features
    print("🔗 Creating interaction features...")
    
    # Age-Tenure interaction
    if 'age' in df_transformed.columns and 'tenure_months' in df_transformed.columns:
        df_transformed['age_tenure_interaction'] = df_transformed['age'] * df_transformed['tenure_months']
        df_transformed['age_tenure_ratio'] = df_transformed['age'] / (df_transformed['tenure_months'] + 1)
    
    # Service complexity features
    service_columns = [col for col in df_transformed.columns if 'service' in col.lower()]
    if len(service_columns) > 1:
        df_transformed['total_services'] = df_transformed[service_columns].sum(axis=1)
    
    # 5. Binning and Discretization
    print("📦 Creating binned features...")
    
    # Age groups
    if 'age' in df_transformed.columns:
        df_transformed['age_group'] = pd.cut(
            df_transformed['age'], 
            bins=[0, 25, 35, 50, 65, 100], 
            labels=['Young', 'Young_Adult', 'Adult', 'Middle_Age', 'Senior']
        )
    
    # Tenure segments
    if 'tenure_months' in df_transformed.columns:
        df_transformed['tenure_segment'] = pd.cut(
            df_transformed['tenure_months'],
            bins=[0, 6, 12, 24, 48, 120],
            labels=['Very_New', 'New', 'Established', 'Loyal', 'Very_Loyal']
        )
    
    # 6. Feature Selection
    print("🎯 Performing feature selection...")
    
    # Separate numeric and categorical features
    numeric_features = df_transformed.select_dtypes(include=[np.number]).columns.tolist()
    if target_column in numeric_features:
        numeric_features.remove(target_column)
    
    # Feature importance using statistical tests
    if len(numeric_features) > 0 and target_column in df_transformed.columns:
        X_numeric = df_transformed[numeric_features].fillna(0)
        y = df_transformed[target_column]
        
        # Use appropriate test based on target type
        if df_transformed[target_column].nunique() == 2:  # Binary classification
            selector = SelectKBest(score_func=chi2, k=min(20, len(numeric_features)))
        else:
            selector = SelectKBest(score_func=f_classif, k=min(20, len(numeric_features)))
        
        X_selected = selector.fit_transform(X_numeric, y)
        selected_features = [numeric_features[i] for i in selector.get_support(indices=True)]
        feature_scores = selector.scores_
        
        # Create feature importance dataframe
        feature_importance = pd.DataFrame({
            'feature': numeric_features,
            'score': feature_scores,
            'selected': selector.get_support()
        }).sort_values('score', ascending=False)
        
        print(f"Selected {len(selected_features)} out of {len(numeric_features)} features")
    
    new_features = len(df_transformed.columns)
    print(f"\\n✅ Feature engineering completed!")
    print(f"Original features: {original_features}")
    print(f"New features: {new_features}")
    print(f"Features added: {new_features - original_features}")
    
    return df_transformed, feature_importance if 'feature_importance' in locals() else None

def create_ml_optimized_preprocessing_pipeline():
    """Create a comprehensive preprocessing pipeline for ML"""
    
    class MLPreprocessingPipeline:
        def __init__(self, target_column):
            self.target_column = target_column
            self.encoders = {}
            self.scalers = {}
            self.feature_engineering_steps = []
            
        def fit_transform(self, df):
            """Fit the pipeline and transform the data"""
            
            df_processed = df.copy()
            
            # Step 1: Handle missing values
            print("🔧 Handling missing values...")
            for column in df_processed.columns:
                if df_processed[column].isnull().sum() > 0:
                    if df_processed[column].dtype in ['int64', 'float64']:
                        # Fill numeric with median
                        median_value = df_processed[column].median()
                        df_processed[column].fillna(median_value, inplace=True)
                        self.feature_engineering_steps.append(f"Filled {column} with median: {median_value}")
                    else:
                        # Fill categorical with mode
                        mode_value = df_processed[column].mode()[0] if len(df_processed[column].mode()) > 0 else 'Unknown'
                        df_processed[column].fillna(mode_value, inplace=True)
                        self.feature_engineering_steps.append(f"Filled {column} with mode: {mode_value}")
            
            # Step 2: Encode categorical variables
            print("🏷️ Encoding categorical variables...")
            categorical_columns = df_processed.select_dtypes(include=['object']).columns.tolist()
            if self.target_column in categorical_columns:
                categorical_columns.remove(self.target_column)
            
            for column in categorical_columns:
                unique_vals = df_processed[column].nunique()
                
                if unique_vals == 2:
                    # Binary encoding
                    le = LabelEncoder()
                    df_processed[f'{column}_encoded'] = le.fit_transform(df_processed[column])
                    self.encoders[column] = le
                    self.feature_engineering_steps.append(f"Binary encoded {column}")
                    
                elif unique_vals <= 10:
                    # One-hot encoding for low cardinality
                    encoded_cols = pd.get_dummies(df_processed[column], prefix=column, drop_first=True)
                    df_processed = pd.concat([df_processed, encoded_cols], axis=1)
                    self.encoders[column] = encoded_cols.columns.tolist()
                    self.feature_engineering_steps.append(f"One-hot encoded {column} into {len(encoded_cols.columns)} features")
                    
                else:
                    # Target encoding for high cardinality
                    if self.target_column in df_processed.columns:
                        target_mean = df_processed.groupby(column)[self.target_column].mean()
                        df_processed[f'{column}_target_encoded'] = df_processed[column].map(target_mean)
                        self.encoders[column] = target_mean.to_dict()
                        self.feature_engineering_steps.append(f"Target encoded {column}")
            
            # Step 3: Scale numerical features
            print("📏 Scaling numerical features...")
            numerical_columns = df_processed.select_dtypes(include=[np.number]).columns.tolist()
            if self.target_column in numerical_columns:
                numerical_columns.remove(self.target_column)
            
            scaler = StandardScaler()
            df_processed[numerical_columns] = scaler.fit_transform(df_processed[numerical_columns])
            self.scalers['standard_scaler'] = scaler
            self.feature_engineering_steps.append(f"Standard scaled {len(numerical_columns)} numerical features")
            
            # Step 4: Feature selection based on correlation
            print("🎯 Performing correlation-based feature selection...")
            numeric_features = df_processed.select_dtypes(include=[np.number]).columns.tolist()
            if self.target_column in numeric_features:
                numeric_features.remove(self.target_column)
            
            if len(numeric_features) > 1:
                corr_matrix = df_processed[numeric_features].corr().abs()
                
                # Find highly correlated features
                high_corr_features = []
                for i in range(len(corr_matrix.columns)):
                    for j in range(i):
                        if corr_matrix.iloc[i, j] > 0.95:
                            high_corr_features.append(corr_matrix.columns[i])
                
                # Remove highly correlated features
                high_corr_features = list(set(high_corr_features))
                if high_corr_features:
                    df_processed.drop(columns=high_corr_features, inplace=True)
                    self.feature_engineering_steps.append(f"Removed {len(high_corr_features)} highly correlated features")
            
            return df_processed
            
        def get_feature_engineering_summary(self):
            """Get summary of all feature engineering steps"""
            return {
                'total_steps': len(self.feature_engineering_steps),
                'steps': self.feature_engineering_steps,
                'encoders_used': len(self.encoders),
                'scalers_used': len(self.scalers)
            }

def integrate_with_feature_store(df, feature_group_name):
    """Integrate processed features with SageMaker Feature Store"""
    
    print(f"🏪 Integrating with SageMaker Feature Store: {feature_group_name}")
    
    # Initialize SageMaker session
    sagemaker_session = sagemaker.Session()
    role = sagemaker.get_execution_role()
    
    # Create feature definitions
    feature_definitions = []
    for column in df.columns:
        if df[column].dtype in ['int64', 'int32']:
            feature_type = 'Integral'
        elif df[column].dtype in ['float64', 'float32']:
            feature_type = 'Fractional'
        else:
            feature_type = 'String'
            
        feature_definitions.append({
            'FeatureName': column,
            'FeatureType': feature_type
        })
    
    # Feature group configuration
    feature_group_config = {
        'FeatureGroupName': feature_group_name,
        'RecordIdentifierName': 'customer_id',  # Assuming customer_id exists
        'EventTimeFeatureName': 'event_time',
        'FeatureDefinitions': feature_definitions,
        'OnlineStoreConfig': {'EnableOnlineStore': True},
        'OfflineStoreConfig': {
            'S3StorageConfig': {
                'S3Uri': f's3://my-feature-store-bucket/{feature_group_name}/'
            }
        },
        'RoleArn': role,
        'Tags': [
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'DataSource', 'Value': 'DataWrangler'}
        ]
    }
    
    print("Feature Store integration configuration created")
    return feature_group_config

def export_to_sagemaker_pipeline(transformations, output_path):
    """Export Data Wrangler transformations to SageMaker Pipeline"""
    
    pipeline_code = f"""
# Generated from Data Wrangler Flow
import pandas as pd
import numpy as np
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.pipeline import Pipeline

def create_data_processing_step(role, instance_type="ml.m5.xlarge"):
    """Create data processing step for SageMaker Pipeline"""
    
    sklearn_processor = SKLearnProcessor(
        framework_version="0.23-1",
        role=role,
        instance_type=instance_type,
        instance_count=1,
        base_job_name="data-wrangler-processing"
    )
    
    processing_step = ProcessingStep(
        name="DataWranglerProcessing",
        processor=sklearn_processor,
        inputs=[
            ProcessingInput(
                source="{output_path}/input",
                destination="/opt/ml/processing/input"
            )
        ],
        outputs=[
            ProcessingOutput(
                output_name="processed_data",
                source="/opt/ml/processing/output",
                destination="{output_path}/output"
            )
        ],
        code="data_processing_script.py"
    )
    
    return processing_step

def create_ml_pipeline(role):
    """Create complete ML pipeline with data processing"""
    
    # Data processing step
    processing_step = create_data_processing_step(role)
    
    # Define pipeline
    pipeline = Pipeline(
        name="DataWranglerMLPipeline",
        steps=[processing_step],
        sagemaker_session=sagemaker.Session()
    )
    
    return pipeline

# Processing script template
processing_script = """
import pandas as pd
import numpy as np
import joblib
import argparse
import os

def process_data():
    # Load data
    input_path = "/opt/ml/processing/input"
    output_path = "/opt/ml/processing/output"
    
    df = pd.read_csv(os.path.join(input_path, "data.csv"))
    
    # Apply transformations from Data Wrangler
    {transformations}
    
    # Save processed data
    df.to_csv(os.path.join(output_path, "processed_data.csv"), index=False)
    print(f"Processed data saved: {{len(df)}} rows, {{len(df.columns)}} columns")

if __name__ == "__main__":
    process_data()
"""
    """
    
    # Save pipeline code
    with open(f"{output_path}/sagemaker_pipeline.py", "w") as f:
        f.write(pipeline_code)
    
    print(f"✅ SageMaker Pipeline code exported to {output_path}")
    return pipeline_code

# Example usage
def main_feature_engineering():
    """Main feature engineering workflow"""
    
    print("🚀 Starting Advanced Feature Engineering Workflow")
    print("=" * 60)
    
    # Load sample data (would be from Data Wrangler in practice)
    from create_data_exploration_analysis import create_data_exploration_analysis
    df = create_data_exploration_analysis()
    
    # Perform advanced feature engineering
    df_engineered, feature_importance = advanced_feature_engineering(df, 'churn')
    
    # Create preprocessing pipeline
    print("\\n🔄 Creating ML preprocessing pipeline...")
    pipeline = MLPreprocessingPipeline('churn')
    df_processed = pipeline.fit_transform(df_engineered)
    
    # Get pipeline summary
    summary = pipeline.get_feature_engineering_summary()
    print(f"\\nPipeline Summary:")
    print(f"  Total steps: {summary['total_steps']}")
    print(f"  Encoders used: {summary['encoders_used']}")
    print(f"  Scalers used: {summary['scalers_used']}")
    
    # Feature Store integration
    print("\\n🏪 Preparing Feature Store integration...")
    feature_store_config = integrate_with_feature_store(df_processed, "customer-churn-features")
    
    # Export to SageMaker Pipeline
    print("\\n📤 Exporting to SageMaker Pipeline...")
    pipeline_code = export_to_sagemaker_pipeline(summary['steps'], "/tmp/sagemaker_pipeline")
    
    print("\\n✅ Advanced feature engineering workflow completed!")
    
    return {
        'processed_data': df_processed,
        'feature_importance': feature_importance,
        'pipeline_summary': summary,
        'feature_store_config': feature_store_config
    }

if __name__ == "__main__":
    results = main_feature_engineering()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="code-container">', unsafe_allow_html=True)
        st.code('''
# SageMaker Data Wrangler: ML Pipeline Integration
import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep, TrainingStep
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.sklearn.estimator import SKLearn
from sagemaker.inputs import TrainingInput
from sagemaker.model_metrics import MetricsSource, ModelMetrics
import boto3
import json

def create_data_wrangler_pipeline_integration():
    """Create end-to-end ML pipeline with Data Wrangler integration"""
    
    # Initialize SageMaker components
    role = sagemaker.get_execution_role()
    session = sagemaker.Session()
    pipeline_session = PipelineSession()
    region = boto3.Session().region_name
    
    # Pipeline configuration
    pipeline_config = {
        'pipeline_name': 'customer-churn-ml-pipeline',
        'base_job_prefix': 'churn-prediction',
        'processing_instance_type': 'ml.m5.2xlarge',
        'training_instance_type': 'ml.m5.2xlarge',
        'model_approval_status': 'PendingManualApproval'
    }
    
    return pipeline_config, role, pipeline_session

def create_data_processing_step(role, pipeline_session):
    """Create data processing step using Data Wrangler output"""
    
    # SKLearn processor for data processing
    sklearn_processor = SKLearnProcessor(
        framework_version='1.0-1',
        role=role,
        instance_type='ml.m5.xlarge',
        instance_count=1,
        base_job_name='data-wrangler-processing',
        sagemaker_session=pipeline_session
    )
    
    # Processing step
    processing_step = ProcessingStep(
        name='DataWranglerProcessing',
        processor=sklearn_processor,
        inputs=[
            sagemaker.processing.ProcessingInput(
                source='s3://my-ml-bucket/data-wrangler-flows/customer-churn/output',
                destination='/opt/ml/processing/input',
                input_name='raw_data'
            )
        ],
        outputs=[
            sagemaker.processing.ProcessingOutput(
                output_name='train_data',
                source='/opt/ml/processing/train',
                destination='s3://my-ml-bucket/processed-data/train'
            ),
            sagemaker.processing.ProcessingOutput(
                output_name='validation_data', 
                source='/opt/ml/processing/validation',
                destination='s3://my-ml-bucket/processed-data/validation'
            ),
            sagemaker.processing.ProcessingOutput(
                output_name='test_data',
                source='/opt/ml/processing/test',
                destination='s3://my-ml-bucket/processed-data/test'
            ),
            sagemaker.processing.ProcessingOutput(
                output_name='feature_importance',
                source='/opt/ml/processing/analysis',
                destination='s3://my-ml-bucket/feature-analysis'
            )
        ],
        code='processing_script.py'
    )
    
    return processing_step

def create_model_training_step(role, pipeline_session, processing_step):
    """Create model training step with hyperparameter tuning"""
    
    # XGBoost estimator
    xgboost_estimator = sagemaker.estimator.Estimator(
        image_uri=sagemaker.image_uris.retrieve('xgboost', region_name=boto3.Session().region_name, version='1.5-1'),
        role=role,
        instance_count=1,
        instance_type='ml.m5.2xlarge',
        output_path='s3://my-ml-bucket/models/',
        base_job_name='churn-xgboost-training',
        sagemaker_session=pipeline_session,
        hyperparameters={
            'objective': 'binary:logistic',
            'num_round': 100,
            'max_depth': 6,
            'eta': 0.3,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'scale_pos_weight': 1,
            'eval_metric': 'auc'
        }
    )
    
    # Training step
    training_step = TrainingStep(
        name='ChurnModelTraining',
        estimator=xgboost_estimator,
        inputs={
            'train': TrainingInput(
                s3_data=processing_step.properties.ProcessingOutputs['train_data'].S3Output.S3Uri,
                content_type='text/csv'
            ),
            'validation': TrainingInput(
                s3_data=processing_step.properties.ProcessingOutputs['validation_data'].S3Output.S3Uri,
                content_type='text/csv'
            )
        }
    )
    
    return training_step

def create_model_evaluation_step(role, pipeline_session, training_step, processing_step):
    """Create model evaluation step"""
    
    # Evaluation processor
    evaluation_processor = SKLearnProcessor(
        framework_version='1.0-1',
        role=role,
        instance_type='ml.m5.xlarge',
        instance_count=1,
        base_job_name='churn-model-evaluation',
        sagemaker_session=pipeline_session
    )
    
    # Evaluation step
    evaluation_step = ProcessingStep(
        name='ModelEvaluation',
        processor=evaluation_processor,
        inputs=[
            sagemaker.processing.ProcessingInput(
                source=training_step.properties.ModelArtifacts.S3ModelArtifacts,
                destination='/opt/ml/processing/model',
                input_name='model'
            ),
            sagemaker.processing.ProcessingInput(
                source=processing_step.properties.ProcessingOutputs['test_data'].S3Output.S3Uri,
                destination='/opt/ml/processing/test',
                input_name='test_data'
            )
        ],
        outputs=[
            sagemaker.processing.ProcessingOutput(
                output_name='evaluation_report',
                source='/opt/ml/processing/evaluation',
                destination='s3://my-ml-bucket/evaluation-reports'
            )
        ],
        code='evaluation_script.py'
    )
    
    return evaluation_step

def create_model_registration_step(training_step, evaluation_step):
    """Create model registration step for Model Registry"""
    
    # Model metrics
    model_metrics = ModelMetrics(
        model_statistics=MetricsSource(
            s3_uri=f"{evaluation_step.properties.ProcessingOutputs['evaluation_report'].S3Output.S3Uri}/statistics.json",
            content_type="application/json"
        ),
        model_data_statistics=MetricsSource(
            s3_uri=f"{evaluation_step.properties.ProcessingOutputs['evaluation_report'].S3Output.S3Uri}/data_statistics.json",
            content_type="application/json"
        )
    )
    
    # Register model step
    register_step = sagemaker.workflow.model_step.ModelStep(
        name='RegisterChurnModel',
        step_args=sagemaker.model.Model(
            image_uri=sagemaker.image_uris.retrieve('xgboost', region_name=boto3.Session().region_name, version='1.5-1'),
            model_data=training_step.properties.ModelArtifacts.S3ModelArtifacts,
            role=sagemaker.get_execution_role()
        ).register(
            content_types=['text/csv'],
            response_types=['application/json'],
            inference_instances=['ml.t2.medium', 'ml.m5.large'],
            transform_instances=['ml.m5.large'],
            model_package_group_name='customer-churn-models',
            approval_status='PendingManualApproval',
            model_metrics=model_metrics
        )
    )
    
    return register_step

def create_processing_scripts():
    """Create the processing and evaluation scripts"""
    
    # Data processing script
    processing_script = """
import pandas as pd
import numpy as np
import os
import joblib
import argparse
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder

def process_data():
    """Process data using Data Wrangler transformations"""
    
    # Read processed data from Data Wrangler
    input_path = "/opt/ml/processing/input"
    
    # Data Wrangler outputs are typically in this format
    df = pd.read_csv(os.path.join(input_path, "data_wrangler_output.csv"))
    
    print(f"Loaded data: {len(df)} rows, {len(df.columns)} columns")
    
    # Assume Data Wrangler has already done most preprocessing
    # Final steps for ML pipeline
    
    # Separate features and target
    target_column = 'churn'
    X = df.drop(columns=[target_column])
    y = df[target_column]
    
    # Split data
    X_temp, X_test, y_temp, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    X_train, X_val, y_train, y_val = train_test_split(
        X_temp, y_temp, test_size=0.25, random_state=42, stratify=y_temp
    )
    
    # Combine features and target for XGBoost format
    train_data = pd.concat([y_train, X_train], axis=1)
    val_data = pd.concat([y_val, X_val], axis=1)
    test_data = pd.concat([y_test, X_test], axis=1)
    
    # Save processed data
    train_data.to_csv('/opt/ml/processing/train/train.csv', index=False, header=False)
    val_data.to_csv('/opt/ml/processing/validation/validation.csv', index=False, header=False)
    test_data.to_csv('/opt/ml/processing/test/test.csv', index=False, header=False)
    
    # Save feature names and statistics
    feature_info = {
        'feature_names': X.columns.tolist(),
        'num_features': len(X.columns),
        'train_samples': len(X_train),
        'val_samples': len(X_val),
        'test_samples': len(X_test),
        'target_distribution': y.value_counts().to_dict()
    }
    
    import json
    with open('/opt/ml/processing/analysis/feature_info.json', 'w') as f:
        json.dump(feature_info, f, indent=2)
    
    print("Data processing completed successfully!")
    print(f"Train: {len(X_train)} samples")
    print(f"Validation: {len(X_val)} samples") 
    print(f"Test: {len(X_test)} samples")

if __name__ == "__main__":
    process_data()
    """
    
    # Model evaluation script
    evaluation_script = """
import pandas as pd
import numpy as np
import os
import json
import joblib
import tarfile
import xgboost as xgb
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix

def evaluate_model():
    """Evaluate trained model on test data"""
    
    # Load model
    model_path = "/opt/ml/processing/model/model.tar.gz"
    with tarfile.open(model_path, 'r:gz') as tar:
        tar.extractall(path="/opt/ml/processing/model")
    
    model = xgb.Booster()
    model.load_model("/opt/ml/processing/model/xgboost-model")
    
    # Load test data
    test_data = pd.read_csv("/opt/ml/processing/test/test.csv", header=None)
    X_test = test_data.iloc[:, 1:]  # Features (excluding target)
    y_test = test_data.iloc[:, 0]   # Target
    
    # Make predictions
    dtest = xgb.DMatrix(X_test)
    y_pred_proba = model.predict(dtest)
    y_pred = (y_pred_proba > 0.5).astype(int)
    
    # Calculate metrics
    metrics = {
        'accuracy': float(accuracy_score(y_test, y_pred)),
        'precision': float(precision_score(y_test, y_pred)),
        'recall': float(recall_score(y_test, y_pred)),
        'f1_score': float(f1_score(y_test, y_pred)),
        'roc_auc': float(roc_auc_score(y_test, y_pred_proba)),
        'confusion_matrix': confusion_matrix(y_test, y_pred).tolist()
    }
    
    # Feature importance
    feature_importance = model.get_score(importance_type='weight')
    
    # Create evaluation report
    evaluation_report = {
        'model_metrics': metrics,
        'feature_importance': feature_importance,
        'test_samples': len(y_test),
        'prediction_distribution': {
            'positive_predictions': int(sum(y_pred)),
            'negative_predictions': int(len(y_pred) - sum(y_pred))
        }
    }
    
    # Save evaluation results
    output_dir = "/opt/ml/processing/evaluation"
    os.makedirs(output_dir, exist_ok=True)
    
    with open(f"{output_dir}/evaluation_report.json", 'w') as f:
        json.dump(evaluation_report, f, indent=2)
    
    # Save statistics for model registry
    statistics = {
        'binary_classification_metrics': {
            'accuracy': {'value': metrics['accuracy']},
            'precision': {'value': metrics['precision']},
            'recall': {'value': metrics['recall']},
            'f1': {'value': metrics['f1_score']},
            'auc': {'value': metrics['roc_auc']}
        }
    }
    
    with open(f"{output_dir}/statistics.json", 'w') as f:
        json.dump(statistics, f, indent=2)
    
    print("Model evaluation completed!")
    print(f"Accuracy: {metrics['accuracy']:.4f}")
    print(f"Precision: {metrics['precision']:.4f}")
    print(f"Recall: {metrics['recall']:.4f}")
    print(f"F1-Score: {metrics['f1_score']:.4f}")
    print(f"ROC-AUC: {metrics['roc_auc']:.4f}")

if __name__ == "__main__":
    evaluate_model()
    """
    
    return processing_script, evaluation_script

def create_complete_ml_pipeline():
    """Create the complete ML pipeline with Data Wrangler integration"""
    
    print("🚀 Creating Complete ML Pipeline with Data Wrangler Integration")
    print("=" * 70)
    
    # Initialize pipeline components
    config, role, pipeline_session = create_data_wrangler_pipeline_integration()
    
    # Create pipeline steps
    print("\\n📝 Creating processing step...")
    processing_step = create_data_processing_step(role, pipeline_session)
    
    print("🎯 Creating training step...")
    training_step = create_model_training_step(role, pipeline_session, processing_step)
    
    print("📊 Creating evaluation step...")
    evaluation_step = create_model_evaluation_step(role, pipeline_session, training_step, processing_step)
    
    print("📦 Creating model registration step...")
    registration_step = create_model_registration_step(training_step, evaluation_step)
    
    # Create complete pipeline
    pipeline = Pipeline(
        name=config['pipeline_name'],
        steps=[
            processing_step,
            training_step,
            evaluation_step,
            registration_step
        ],
        sagemaker_session=pipeline_session
    )
    
    # Generate processing scripts
    print("\\n💻 Generating processing scripts...")
    processing_script, evaluation_script = create_processing_scripts()
    
    # Save scripts
    os.makedirs('/tmp/pipeline_scripts', exist_ok=True)
    with open('/tmp/pipeline_scripts/processing_script.py', 'w') as f:
        f.write(processing_script)
    with open('/tmp/pipeline_scripts/evaluation_script.py', 'w') as f:
        f.write(evaluation_script)
    
    print("✅ Pipeline created successfully!")
    print(f"Pipeline name: {config['pipeline_name']}")
    print(f"Steps: {len(pipeline.steps)}")
    
    # Pipeline execution
    print("\\n🔄 Pipeline execution commands:")
    print(f"pipeline.upsert(role_arn='{role}')")
    print(f"execution = pipeline.start()")
    print(f"execution.describe()")
    
    return {
        'pipeline': pipeline,
        'config': config,
        'processing_script': processing_script,
        'evaluation_script': evaluation_script
    }

# Example usage
if __name__ == "__main__":
    pipeline_components = create_complete_ml_pipeline()
    
    # Optionally execute the pipeline
    # pipeline = pipeline_components['pipeline']
    # pipeline.upsert(role_arn=sagemaker.get_execution_role())
    # execution = pipeline.start()
        ''', language='python')
        st.markdown('</div>', unsafe_allow_html=True)

def create_ml_sample_data():
    """Create sample ML dataset with quality issues"""
    np.random.seed(42)
    n_samples = 500
    
    # Create realistic ML dataset
    data = {
        'customer_id': [f'CUST_{i:05d}' for i in range(n_samples)],
        'age': np.random.normal(40, 15, n_samples).clip(18, 80).astype(int),
        'income': np.random.lognormal(10.5, 0.5, n_samples).clip(20000, 200000).astype(int),
        'credit_score': np.random.normal(650, 100, n_samples).clip(300, 850).astype(int),
        'account_balance': np.random.normal(5000, 3000, n_samples),
        'num_products': np.random.poisson(2, n_samples) + 1,
        'tenure_years': np.random.exponential(5, n_samples).clip(0, 20),
        'transaction_frequency': np.random.gamma(2, 5, n_samples),
        'education_level': np.random.choice(['High School', 'Bachelor', 'Master', 'PhD'], n_samples, p=[0.3, 0.4, 0.25, 0.05]),
        'employment_status': np.random.choice(['Employed', 'Self-employed', 'Unemployed', 'Retired'], n_samples, p=[0.6, 0.2, 0.1, 0.1]),
        'marital_status': np.random.choice(['Single', 'Married', 'Divorced'], n_samples, p=[0.4, 0.5, 0.1]),
        'registration_date': pd.date_range(start='2020-01-01', end='2024-12-31', periods=n_samples)
    }
    
    df = pd.DataFrame(data)
    
    # Add target variable (churn) with realistic relationships
    churn_prob = (
        0.05 +  # Base rate
        (df['age'] < 25) * 0.1 +  # Young customers more likely to churn
        (df['tenure_years'] < 1) * 0.2 +  # New customers
        (df['account_balance'] < 1000) * 0.15 +  # Low balance
        (df['num_products'] == 1) * 0.1 +  # Single product
        (df['employment_status'] == 'Unemployed') * 0.2
    )
    
    df['churn'] = np.random.binomial(1, churn_prob)
    
    # Introduce some quality issues
    # Missing values
    missing_mask = np.random.random(n_samples) < 0.05
    df.loc[missing_mask, 'income'] = None
    
    # Outliers
    outlier_mask = np.random.random(n_samples) < 0.02
    df.loc[outlier_mask, 'account_balance'] = df.loc[outlier_mask, 'account_balance'] * 10
    
    return df

def apply_feature_engineering(df, encoding_options, scaling_option, feature_creation):
    """Apply selected feature engineering transformations"""
    df_processed = df.copy()
    
    # Apply encoding
    if 'One-Hot Encoding' in encoding_options:
        categorical_cols = ['education_level', 'employment_status', 'marital_status']
        for col in categorical_cols:
            if col in df_processed.columns:
                encoded = pd.get_dummies(df_processed[col], prefix=col, drop_first=True)
                df_processed = pd.concat([df_processed, encoded], axis=1)
    
    # Apply scaling
    if scaling_option != 'None':
        numeric_cols = df_processed.select_dtypes(include=[np.number]).columns
        # Simulate scaling (in reality would use sklearn scalers)
        for col in numeric_cols:
            if col not in ['churn', 'customer_id']:
                df_processed[f'{col}_scaled'] = (df_processed[col] - df_processed[col].mean()) / df_processed[col].std()
    
    # Create new features
    if 'Date/Time Features' in feature_creation:
        if 'registration_date' in df_processed.columns:
            df_processed['registration_year'] = df_processed['registration_date'].dt.year
            df_processed['registration_month'] = df_processed['registration_date'].dt.month
            df_processed['days_since_registration'] = (pd.Timestamp.now() - df_processed['registration_date']).dt.days
    
    if 'Interaction Terms' in feature_creation:
        if 'age' in df_processed.columns and 'income' in df_processed.columns:
            df_processed['age_income_interaction'] = df_processed['age'] * df_processed['income'].fillna(df_processed['income'].median())
    
    return df_processed

def create_sample_analysis_charts(df):
    """Create sample analysis visualizations"""
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Age distribution
        fig1 = px.histogram(
            df, x='age', nbins=20,
            title='Age Distribution',
            color_discrete_sequence=[AWS_COLORS['primary']]
        )
        fig1.update_layout(showlegend=False)
        st.plotly_chart(fig1, use_container_width=True)
        
        # Income vs Credit Score
        fig3 = px.scatter(
            df, x='income', y='credit_score', color='churn',
            title='Income vs Credit Score by Churn',
            color_discrete_map={0: AWS_COLORS['light_blue'], 1: AWS_COLORS['error']}
        )
        st.plotly_chart(fig3, use_container_width=True)
    
    with col2:
        # Churn by Education Level
        churn_education = df.groupby('education_level')['churn'].mean().reset_index()
        fig2 = px.bar(
            churn_education, x='education_level', y='churn',
            title='Churn Rate by Education Level',
            color='churn',
            color_continuous_scale='Reds'
        )
        fig2.update_layout(showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)
        
        # Correlation heatmap
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        corr_matrix = df[numeric_cols].corr()
        
        fig4 = px.imshow(
            corr_matrix,
            title='Feature Correlation Matrix',
            color_continuous_scale='RdBu',
            aspect='auto'
        )
        st.plotly_chart(fig4, use_container_width=True)

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
    # 🧹 AWS Data Quality Tools
    <div class='info-box'>
    Master visual data preparation and quality assurance with AWS Glue DataBrew and Amazon SageMaker Data Wrangler - powerful tools for cleaning, transforming, and preparing data for analytics and machine learning.
    </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2 = st.tabs([
        "🧹 AWS Glue DataBrew",
        "🔬 SageMaker Data Wrangler"
    ])
    
    with tab1:
        glue_databrew_tab()
    
    with tab2:
        sagemaker_data_wrangler_tab()
    
    # Footer
    st.markdown("""
    <div class="footer">
        <p>© 2025, Amazon Web Services, Inc. or its affiliates. All rights reserved.</p>
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
